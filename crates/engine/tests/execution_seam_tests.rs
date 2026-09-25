//! Scripted-adapter orchestration tests (§32, tasks 4.1-4.5, 5.1-5.2): the
//! controller drives the deterministic `ScriptedHttp` adapter through the
//! same code paths used with the production wire adapter — probe retry,
//! bounded authentication, probe notices, segmentation selection, retry
//! timing, durable-prefix retries, generation changes, and cancellation —
//! with no sockets and no wall-clock timeout waits.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kdown_engine::config::{DurabilityMode, EngineConfig, TransferPolicy};
use kdown_engine::control::auth::{provider_fn, Challenge, CredentialDecision};
use kdown_engine::control::CancellationToken;
use kdown_engine::error::ErrorCategory;
use kdown_engine::http::probe::ProbeMetadata;
use kdown_engine::http::scripted::{ProbeStep, ScriptedHttp, TransferOk, TransferStep};
use kdown_engine::http::{HttpExecution, HttpFailure};
use kdown_engine::job::controller::{
    DownloadController, DownloadHandle, DownloadRequest, ResultStatus,
};
use kdown_engine::DownloadError;
use support::fixtures::{assert_bytes_exact, deterministic_bytes};

#[path = "support/mod.rs"]
mod support;

fn cfg(threshold: u64) -> EngineConfig {
    let mut c = EngineConfig {
        transfer: TransferPolicy {
            segmentation_threshold: threshold,
            max_workers: 4,
            min_workers: 1,
            verify_range_support: true,
            ..TransferPolicy::default()
        },
        ..EngineConfig::default()
    };
    // Fast, deterministic retries.
    c.retry.base_delay = Duration::from_millis(5);
    c.retry.max_delay = Duration::from_millis(10);
    c.transfer.durability = DurabilityMode::Performance;
    c
}

fn scripted_controller(scripted: &ScriptedHttp, config: EngineConfig) -> DownloadController {
    DownloadController::with_execution(HttpExecution::from_adapter(scripted.clone()), config)
}

fn ok_probe(total: u64) -> ProbeStep {
    ProbeStep::new().ok_meta(ProbeMetadata {
        status: 200,
        total_size: Some(total),
        ..ProbeMetadata::default()
    })
}

/// Task 4.3: fresh sequential transfer through the seam produces exact
/// output bytes with no sockets.
#[tokio::test]
async fn scripted_sequential_success_exact_bytes() {
    let content = deterministic_bytes(300, 71);
    let scripted = ScriptedHttp::new()
        .expect_probe(ok_probe(300))
        .expect_transfer(
            TransferStep::new()
                .intent_full()
                .ok(TransferOk::new().total(300).chunk(content.clone())),
        );
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("out.bin");
    let c = scripted_controller(&scripted, cfg(u64::MAX));

    let result = c
        .run(DownloadRequest::new("https://scripted/f.bin", dest.clone()))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(result.total_size, Some(300));
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    assert!(!dir.path().join("out.bin.part").exists(), "no residue");
    scripted.assert_all_consumed();
    assert_eq!(scripted.request_log().len(), 2);
}

/// Task 4.2: probe failures retry through the shared policy and then
/// succeed; the request log proves the re-probe.
#[tokio::test]
async fn scripted_probe_retry_then_success() {
    let content = deterministic_bytes(64, 72);
    let scripted = ScriptedHttp::new()
        .expect_probe(
            ProbeStep::new().fail_error(DownloadError::Connection("connection reset".into())),
        )
        .expect_probe(ok_probe(64))
        .expect_transfer(
            TransferStep::new().ok(TransferOk::new().total(64).chunk(content.clone())),
        );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg(u64::MAX));

    let result = c
        .run(DownloadRequest::new(
            "https://scripted/retry.bin",
            dir.path().join("retry.bin"),
        ))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    let log = scripted.request_log();
    assert_eq!(log.len(), 3, "two probes + one transfer");
    scripted.assert_all_consumed();
}

/// Task 4.2: a probe authentication challenge consults the bounded
/// credential provider and re-probes with the credential header attached.
#[tokio::test]
async fn scripted_probe_challenge_consults_provider_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let provider = provider_fn(move |_ch: &Challenge| {
        provider_calls.fetch_add(1, Ordering::SeqCst);
        Ok(CredentialDecision::Headers(vec![(
            "Authorization".to_string(),
            "Bearer good-token".to_string(),
        )]))
    });
    let content = deterministic_bytes(64, 73);
    let challenge = || HttpFailure {
        error: DownloadError::AuthenticationRequired,
        retry_after: None,
        challenge: Some(Challenge {
            status: 401,
            authenticate: vec!["Bearer realm=\"x\"".to_string()],
            origin: "scripted".into(),
        }),
    };
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().fail(challenge()))
        .expect_probe(
            ProbeStep::new()
                .header("Authorization", "Bearer good-token")
                .ok_meta(ProbeMetadata {
                    status: 200,
                    total_size: Some(64),
                    ..ProbeMetadata::default()
                }),
        )
        .expect_transfer(
            TransferStep::new()
                .header("Authorization", "Bearer good-token")
                .ok(TransferOk::new().total(64).chunk(content.clone())),
        );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg(u64::MAX));
    let mut req = DownloadRequest::new("https://scripted/auth.bin", dir.path().join("auth.bin"));
    req.credential_provider = Some(Arc::from(provider));

    let result = c.run(req).await.expect("terminal");

    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "one provider stage");
    scripted.assert_all_consumed();
}

/// Task 4.2: advertised-but-broken range support arrives as a semantic
/// notice plus ineligible metadata; the job falls back to sequential
/// without a second eligibility formula.
#[tokio::test]
async fn scripted_broken_advertised_ranges_fall_back_sequential() {
    let content = deterministic_bytes(300, 74);
    // Size qualifies for segmentation, ranges advertised, verification
    // enabled — but the HTTP layer reports the advertised support unusable.
    let meta = ProbeMetadata {
        status: 200,
        total_size: Some(300),
        accept_ranges: true,
        range_verified: false,
        ..ProbeMetadata::default()
    };
    assert!(
        !meta.segment_eligible(128, true),
        "unverified ranges are ineligible under verify policy"
    );
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(meta))
        .expect_transfer(
            TransferStep::new()
                .intent_full()
                .ok(TransferOk::new().total(300).chunk(content.clone())),
        );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg(128));

    let result = c
        .run(DownloadRequest::new(
            "https://scripted/liar.bin",
            dir.path().join("liar.bin"),
        ))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(
        &std::fs::read(dir.path().join("liar.bin")).expect("read"),
        &content,
    );
    // Exactly one transfer: sequential fallback, no per-segment requests.
    let log = scripted.request_log();
    assert_eq!(log.len(), 2);
    scripted.assert_all_consumed();
}

/// Task 4.3: a retryable status with `Retry-After` timing is retried and
/// the download completes; the transfer is re-issued from zero.
#[tokio::test]
async fn scripted_retryable_status_with_retry_after() {
    let content = deterministic_bytes(100, 75);
    let scripted = ScriptedHttp::new()
        .expect_probe(ok_probe(100))
        .expect_transfer(TransferStep::new().fail_retry_after(
            DownloadError::RateLimited { status: 429 },
            Duration::from_millis(1),
        ))
        .expect_transfer(
            TransferStep::new()
                .intent_full()
                .ok(TransferOk::new().total(100).chunk(content.clone())),
        );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg(u64::MAX));

    let result = c
        .run(DownloadRequest::new(
            "https://scripted/limited.bin",
            dir.path().join("limited.bin"),
        ))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(
        &std::fs::read(dir.path().join("limited.bin")).expect("read"),
        &content,
    );
    scripted.assert_all_consumed();
}

/// Task 4.3: authentication challenges at transfer time are bounded by
/// MAX_AUTH_STAGES; the provider is never consulted a third time and the
/// job fails with the structured error.
#[tokio::test]
async fn scripted_auth_retry_limit_is_bounded() {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let provider = provider_fn(move |_ch: &Challenge| {
        provider_calls.fetch_add(1, Ordering::SeqCst);
        Ok(CredentialDecision::Headers(vec![(
            "Authorization".to_string(),
            "Bearer always-wrong".to_string(),
        )]))
    });
    let challenge = || HttpFailure {
        error: DownloadError::AuthenticationRequired,
        retry_after: None,
        challenge: Some(Challenge {
            status: 401,
            authenticate: vec!["Bearer realm=\"x\"".to_string()],
            origin: "scripted".into(),
        }),
    };
    let scripted = ScriptedHttp::new()
        .expect_probe(ok_probe(16))
        .expect_transfer(TransferStep::new().fail(challenge()))
        .expect_transfer(TransferStep::new().fail(challenge()))
        .expect_transfer(TransferStep::new().fail(challenge()));
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg(u64::MAX));
    let mut req = DownloadRequest::new("https://scripted/loop.bin", dir.path().join("loop.bin"));
    req.credential_provider = Some(Arc::from(provider));

    let result = c.run(req).await.expect("terminal");

    assert_eq!(result.status, ResultStatus::Failed);
    assert_eq!(
        result.error.as_ref().map(DownloadError::category),
        Some(ErrorCategory::AuthenticationRequired),
        "{result:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        kdown_engine::control::auth::MAX_AUTH_STAGES as usize,
        "provider bounded to MAX_AUTH_STAGES"
    );
    scripted.assert_all_consumed();
}

/// Task 4.3: a body fault after a delivered prefix retries from the
/// durable prefix (§17.3) with a ranged intent — never re-fetching the
/// committed bytes.
#[tokio::test]
async fn scripted_body_fault_after_prefix_retries_from_durable_prefix() {
    let content = deterministic_bytes(200, 76);
    let scripted = ScriptedHttp::new()
        .expect_probe(ok_probe(200))
        .expect_transfer(
            TransferStep::new().ok(TransferOk::new()
                .total(200)
                .chunk(content[..100].to_vec())
                .fault(DownloadError::Connection("connection reset".into()))),
        )
        .expect_transfer(
            TransferStep::new()
                .range((100, 199))
                .ok(TransferOk::new().total(200).chunk(content[100..].to_vec())),
        );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg(u64::MAX));

    let result = c
        .run(DownloadRequest::new(
            "https://scripted/flaky.bin",
            dir.path().join("flaky.bin"),
        ))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(
        &std::fs::read(dir.path().join("flaky.bin")).expect("read"),
        &content,
    );
    // The retry asked exactly for the missing tail.
    let log = scripted.request_log();
    assert_eq!(log.len(), 3);
    assert_eq!(log[2].range, Some((100, 199)));
    scripted.assert_all_consumed();
}

/// Task 4.3: a resource generation change (§26) fails the job before
/// bytes from different generations mix.
#[tokio::test]
async fn scripted_generation_change_fails_structured() {
    let scripted = ScriptedHttp::new()
        .expect_probe(ok_probe(200))
        .expect_transfer(TransferStep::new().ok(
            TransferOk::new().total(200).chunk(vec![1u8; 100]).fault(
                DownloadError::ResourceChanged(
                    "response validators disagree with the established generation".into(),
                ),
            ),
        ));
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg(u64::MAX));

    let result = c
        .run(DownloadRequest::new(
            "https://scripted/gen.bin",
            dir.path().join("gen.bin"),
        ))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Failed);
    assert_eq!(
        result.error.as_ref().map(|e| e.category()),
        Some(ErrorCategory::ResourceChanged),
        "{result:?}"
    );
    scripted.assert_all_consumed();
}

/// Task 4.4: cancellation interrupts a pending scripted body read
/// promptly — no wall-clock sleep, deterministic terminal state.
#[tokio::test]
async fn scripted_cancellation_interrupts_pending_body_read() {
    let scripted = ScriptedHttp::new()
        .expect_labeled_probe("probe", ok_probe(4096))
        .expect_labeled_transfer(
            "transfer",
            TransferStep::new().ok(TransferOk::new().total(4096).wait_for_cancellation()),
        );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg(u64::MAX));

    let started = std::time::Instant::now();
    let (handle, join) = {
        let req = DownloadRequest::new("https://scripted/slow.bin", dir.path().join("slow.bin"));
        c.start(req)
    };
    // Deterministic barrier: the body read is pending once the transfer
    // call was consumed. No sleep-based synchronization.
    scripted.wait_for_phase("transfer").await;
    cancel_and_assert(&handle, join, started).await;
    scripted.assert_all_consumed();
}

async fn cancel_and_assert(
    handle: &DownloadHandle,
    join: tokio::task::JoinHandle<
        Result<kdown_engine::job::controller::DownloadResult, DownloadError>,
    >,
    started: std::time::Instant,
) {
    handle.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), join)
        .await
        .expect("no hang: cancellation interrupts the pending read")
        .expect("join")
        .expect("terminal result");
    assert_eq!(result.status, ResultStatus::Cancelled, "{result:?}");
    assert!(matches!(result.error, Some(DownloadError::Cancelled)));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "cancellation converged promptly"
    );
}

/// Task 4.4: a pause observed across a pending body read converges, then
/// resumes the same source byte-exact (§9.3, §32).
#[tokio::test]
async fn scripted_pause_then_resume_delivers_byte_exact() {
    let content = deterministic_bytes(200, 77);
    let scripted = ScriptedHttp::new()
        .expect_labeled_probe("probe", ok_probe(200))
        .expect_labeled_transfer(
            "transfer",
            TransferStep::new().ok(TransferOk::new()
                .total(200)
                .chunk(content[..100].to_vec())
                .chunk(content[100..].to_vec())),
        );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg(u64::MAX));

    let req = DownloadRequest::new("https://scripted/pause.bin", dir.path().join("pause.bin"));
    let (handle, join) = c.start(req);
    // Pause before the body read: the seam surfaces Paused at the first
    // pending read and the controller settles (flush + checkpoint) before
    // waiting.
    handle.pause();
    scripted.wait_for_phase("transfer").await;
    // Give the controller a bounded moment to observe the pause, then
    // resume; delivery continues from the same owned source.
    handle.resume_now();
    let result = tokio::time::timeout(Duration::from_secs(5), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(
        &std::fs::read(dir.path().join("pause.bin")).expect("read"),
        &content,
    );
    scripted.assert_all_consumed();
}

/// Task 5.1: a scripted multi-worker segmented download covers the
/// complete file through the semantic seam — no `HttpTransport` anywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scripted_segmented_multi_worker_covers_file() {
    let content = deterministic_bytes(4000, 78);
    let mut cfg = cfg(1024); // size qualifies for segmentation
    cfg.transfer.max_segment_size = 1000; // exactly four segments
    cfg.transfer.min_segment_size = 1;
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(4000),
            accept_ranges: true,
            range_verified: true, // verified inside the HTTP layer (§10.2)
            ..ProbeMetadata::default()
        }))
        .expect_unordered_ranges(
            "segments",
            vec![
                TransferStep::new().range((0, 999)).ok(TransferOk::new()
                    .range(0, 999)
                    .total(4000)
                    .chunk(content[0..1000].to_vec())),
                TransferStep::new().range((1000, 1999)).ok(TransferOk::new()
                    .range(1000, 1999)
                    .total(4000)
                    .chunk(content[1000..2000].to_vec())),
                TransferStep::new().range((2000, 2999)).ok(TransferOk::new()
                    .range(2000, 2999)
                    .total(4000)
                    .chunk(content[2000..3000].to_vec())),
                TransferStep::new().range((3000, 3999)).ok(TransferOk::new()
                    .range(3000, 3999)
                    .total(4000)
                    .chunk(content[3000..4000].to_vec())),
            ],
        );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg);

    let result = c
        .run(DownloadRequest::new(
            "https://scripted/seg.bin",
            dir.path().join("seg.bin"),
        ))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(result.total_size, Some(4000));
    assert_bytes_exact(
        &std::fs::read(dir.path().join("seg.bin")).expect("read"),
        &content,
    );
    scripted.assert_all_consumed();
    // Probe + exactly one ranged call per segment.
    assert_eq!(scripted.request_log().len(), 5);
}

/// Task 5.2: 429 responses coordinate origin-wide backoff; every worker
/// retries after the gate and the job completes (§17.4).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scripted_segmented_429_coordinates_origin_backoff() {
    let content = deterministic_bytes(4000, 79);
    let mut cfg = cfg(1024);
    cfg.transfer.max_segment_size = 1000;
    cfg.transfer.min_segment_size = 1;
    let limited = || HttpFailure {
        error: DownloadError::RateLimited { status: 429 },
        retry_after: Some(Duration::from_millis(1)),
        challenge: None,
    };
    let seg = |s: usize| {
        TransferStep::new()
            .range(((s * 1000) as u64, (s * 1000 + 999) as u64))
            .ok(TransferOk::new()
                .range((s * 1000) as u64, (s * 1000 + 999) as u64)
                .total(4000)
                .chunk(content[s * 1000..(s + 1) * 1000].to_vec()))
    };
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(4000),
            accept_ranges: true,
            range_verified: true,
            ..ProbeMetadata::default()
        }))
        // Per range: one rate-limited attempt, then the successful retry
        // after the coordinated origin gate. One unordered phase keyed by
        // range keeps arrival order and retry timing irrelevant (§32).
        .expect_unordered_ranges(
            "limited-then-ok",
            vec![
                TransferStep::new().range((0, 999)).fail(limited()),
                TransferStep::new().range((1000, 1999)).fail(limited()),
                TransferStep::new().range((2000, 2999)).fail(limited()),
                TransferStep::new().range((3000, 3999)).fail(limited()),
                seg(0),
                seg(1),
                seg(2),
                seg(3),
            ],
        );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg);

    let result = c
        .run(DownloadRequest::new(
            "https://scripted/coord.bin",
            dir.path().join("coord.bin"),
        ))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(
        &std::fs::read(dir.path().join("coord.bin")).expect("read"),
        &content,
    );
    scripted.assert_all_consumed();
}

/// Task 5.2: retry exhaustion surfaces the structured exhaustion error
/// and stops the job (§14.5).
#[tokio::test]
async fn scripted_segmented_retry_exhaustion() {
    let mut cfg = cfg(1024);
    cfg.transfer.max_workers = 1;
    cfg.transfer.max_segment_size = 4000;
    cfg.transfer.min_segment_size = 1;
    cfg.retry.max_attempts_per_segment = 1;
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(4000),
            accept_ranges: true,
            range_verified: true,
            ..ProbeMetadata::default()
        }))
        .expect_transfer(
            TransferStep::new()
                .range((0, 3999))
                .fail_error(DownloadError::Connection("connection reset".into())),
        );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg);

    let result = c
        .run(DownloadRequest::new(
            "https://scripted/exhaust.bin",
            dir.path().join("exhaust.bin"),
        ))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Failed);
    assert!(
        matches!(result.error, Some(DownloadError::RetryExhausted { .. })),
        "structured exhaustion: {result:?}"
    );
    scripted.assert_all_consumed();
}

/// Task 5.2: a successful tail retry resumes from the durable prefix —
/// the re-issued range starts at the acknowledged offset, never refetching
/// committed bytes (§17.3 tail-only retry accounting).
#[tokio::test]
async fn scripted_segmented_tail_retry_resumes_from_durable_prefix() {
    let content = deterministic_bytes(4000, 80);
    let mut cfg = cfg(1024);
    cfg.transfer.max_workers = 1;
    cfg.transfer.max_segment_size = 4000;
    cfg.transfer.min_segment_size = 1;
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(4000),
            accept_ranges: true,
            range_verified: true,
            ..ProbeMetadata::default()
        }))
        .expect_transfer(
            TransferStep::new().range((0, 3999)).ok(TransferOk::new()
                .range(0, 3999)
                .total(4000)
                .chunk(content[..100].to_vec())
                .fault(DownloadError::Connection("connection reset".into()))),
        )
        .expect_transfer(
            // The ranged retry intent carries the authoritative total the
            // job established at probe time (§10.2).
            TransferStep::new()
                .range((100, 3999))
                .established_total(4000)
                .ok(TransferOk::new()
                    .range(100, 3999)
                    .total(4000)
                    .chunk(content[100..].to_vec())),
        );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg);

    let result = c
        .run(DownloadRequest::new(
            "https://scripted/tail.bin",
            dir.path().join("tail.bin"),
        ))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(
        &std::fs::read(dir.path().join("tail.bin")).expect("read"),
        &content,
    );
    // Tail-only retry: the second call starts at the durable prefix.
    let log = scripted.request_log();
    assert_eq!(log[2].range, Some((100, 3999)));
    scripted.assert_all_consumed();
}

/// Task 5.2: a validator change invalidates the whole job — segments are
/// never mixed across generations (§26).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scripted_segmented_validator_change_invalidates_whole_job() {
    let mut cfg = cfg(1024);
    cfg.transfer.max_workers = 1;
    cfg.transfer.max_segment_size = 4000;
    cfg.transfer.min_segment_size = 1;
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(4000),
            accept_ranges: true,
            range_verified: true,
            ..ProbeMetadata::default()
        }))
        .expect_transfer(TransferStep::new().range((0, 3999)).fail_error(
            DownloadError::ResourceChanged(
                "response validators disagree with the established generation".into(),
            ),
        ));
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted, cfg);

    let result = c
        .run(DownloadRequest::new(
            "https://scripted/genchange.bin",
            dir.path().join("genchange.bin"),
        ))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Failed);
    assert_eq!(
        result.error.as_ref().map(|e| e.category()),
        Some(ErrorCategory::ResourceChanged),
        "{result:?}"
    );
    // No output committed from mixed generations.
    assert!(!dir.path().join("genchange.bin").exists());
    scripted.assert_all_consumed();
}

/// Task 4.3: an invalid ranged resume response fails the job before any
/// byte is written (§11.2, §32: metadata validated before body delivery).
#[tokio::test]
async fn scripted_invalid_ranged_resume_fails_before_body() {
    use kdown_engine::resume::checkpoint_store::CheckpointStore;
    use kdown_engine::resume::{job_identity, Checkpoint, FileCheckpointStore};

    let dest_url = "https://scripted/resume.bin";
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("resume.bin");
    // Seed a resumable state: 100 bytes on disk, ranges and validators
    // recorded (§15.5).
    std::fs::write(dir.path().join("resume.bin.part"), vec![9u8; 100]).expect("temp");
    let identity = job_identity(dest_url, &dest);
    let store = FileCheckpointStore::new(
        dir.path(),
        kdown_engine::resume::DurabilityMode::Performance,
    )
    .expect("store");
    let stale_validators = kdown_engine::http::validators::ResourceValidators {
        etag: Some("\"stale-gen\"".into()),
        etag_is_weak: false,
        last_modified: None,
        total_size: Some(200),
    };
    let mut cp = Checkpoint::new(&identity, dest_url, "tmp-resume.bin");
    cp.total_size = Some(200);
    cp.validators = stale_validators.clone();
    cp.completed_ranges = vec![(0, 99)];
    store.save_atomic(&cp).expect("seed checkpoint");

    let scripted = ScriptedHttp::new()
        .expect_probe(ok_probe(200))
        .expect_transfer(
            // The resume request must be conditional on the checkpoint's
            // validators (If-Range, §11.3) for the missing tail only.
            TransferStep::new()
                .range((100, 199))
                .validators(&stale_validators)
                .fail_error(DownloadError::InvalidRangeResponse("start mismatch".into())),
        );
    let c = scripted_controller(&scripted, cfg(u64::MAX));

    let result = c
        .run(DownloadRequest::new(dest_url, dest.clone()))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Failed);
    assert_eq!(
        result.error.as_ref().map(DownloadError::category),
        Some(ErrorCategory::InvalidRangeResponse),
        "{result:?}"
    );
    // Only the tail was requested — the committed prefix is never
    // re-fetched (§17.3).
    let log = scripted.request_log();
    assert_eq!(log.len(), 2);
    assert_eq!(log[1].range, Some((100, 199)));
    scripted.assert_all_consumed();
    // Nothing committed.
    assert!(!dest.exists());
}

/// The unused-cancel token keeps clippy quiet about the unused import in
/// rare single-test builds.
#[test]
fn cancel_token_import_is_used() {
    let _ = CancellationToken::new();
}
