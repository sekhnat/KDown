//! End-to-end single-stream controller tests (task 3.7): byte-exact
//! download, correct final path, no temp residue, structured failures.

#[path = "support/mod.rs"]
mod support;

use std::time::Duration;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use kdown_engine::config::{EngineConfig, ExpectedHash, HashAlgorithm, IntegrityPolicy};
use kdown_engine::http::probe::ProbeMetadata;
use kdown_engine::http::scripted::{ProbeStep, ScriptedHttp, TransferOk, TransferStep};
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::http::HttpExecution;
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};
use kdown_engine::resume::{
    CheckpointError, CheckpointResolveContext, CheckpointStore, CheckpointStoreResolver,
    SidecarCheckpointResolver,
};
use support::fixtures::{assert_bytes_exact, deterministic_bytes, sha256_hex};
use support::test_server::{ScriptedResponse, TestServer};

fn controller() -> SingleStreamController {
    SingleStreamController::new(
        HttpTransport::new(kdown_engine::config::NetworkPolicy::default()).expect("transport"),
        EngineConfig::default(),
    )
}

/// Scripted orchestration controller (§32): network-neutral cases run
/// through the deterministic adapter — no sockets, no delays.
fn scripted_controller(scripted: &ScriptedHttp) -> SingleStreamController {
    SingleStreamController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        EngineConfig::default(),
    )
}

#[derive(Clone)]
struct CountingResolver(Arc<AtomicUsize>);

impl CheckpointStoreResolver for CountingResolver {
    fn resolve(
        &self,
        context: &CheckpointResolveContext,
    ) -> Result<Arc<dyn CheckpointStore>, CheckpointError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        SidecarCheckpointResolver.resolve(context)
    }
}

#[tokio::test]
async fn downloads_fixture_byte_exact_real() {
    let content = deterministic_bytes(1024 * 1024, 9001);
    let server = TestServer::new()
        .serve_static("/data.bin", content.clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("data.bin");

    let c = controller();
    let result = c
        .run(DownloadRequest::new(server.url("/data.bin"), dest.clone()))
        .await
        .expect("run terminal");

    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(result.final_path.as_deref(), Some(dest.as_path()));
    assert_eq!(result.total_size, Some(1024 * 1024));
    let got = std::fs::read(&dest).expect("final file");
    assert_bytes_exact(&got, &content);
    // No temp residue (§14.1).
    let part = dir.path().join("data.bin.part");
    assert!(!part.exists(), "temp file must be gone after commit");
}

#[tokio::test]
async fn hash_mismatch_prevents_commit() {
    let content = deterministic_bytes(4096, 5);
    let server = TestServer::new()
        .serve_static("/hash.bin", content)
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("hash.bin");

    let mut req = DownloadRequest::new(server.url("/hash.bin"), dest.clone());
    req.integrity = IntegrityPolicy {
        expected_hashes: vec![ExpectedHash {
            algorithm: HashAlgorithm::Sha256,
            hex: "00".repeat(32),
        }],
        apply_mtime: false,
    };

    let c = controller();
    let result = c.run(req).await.expect("terminal");
    assert_eq!(result.status, ResultStatus::Failed);
    assert!(matches!(
        result.error,
        Some(kdown_engine::DownloadError::IntegrityMismatch(_))
    ));
    // Destination untouched (integrity spec: no file created/replaced).
    assert!(!dest.exists(), "mismatch must not commit");
    assert!(
        !dir.path().join("hash.bin.part").exists(),
        "no temp residue"
    );
}

#[tokio::test]
async fn hash_match_commits() {
    let content = deterministic_bytes(8192, 42);
    let server = TestServer::new()
        .serve_static("/ok.bin", content.clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("ok.bin");

    let mut req = DownloadRequest::new(server.url("/ok.bin"), dest.clone());
    req.integrity = IntegrityPolicy {
        expected_hashes: vec![ExpectedHash {
            algorithm: HashAlgorithm::Sha256,
            hex: sha256_hex(&content),
        }],
        apply_mtime: false,
    };

    let c = controller();
    let result = c.run(req).await.expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
}

#[tokio::test]
async fn fail_if_exists_rejects_before_network() {
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("exists.bin");
    std::fs::write(&dest, b"original").expect("existing file");

    // The rejection is pre-network: the scripted log proves no probe or
    // transfer call was ever issued (§32, §14.6).
    let scripted = ScriptedHttp::new();
    let c = scripted_controller(&scripted);
    let result = c
        .run(DownloadRequest::new(
            "https://scripted/exists",
            dest.clone(),
        ))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Failed);
    assert!(matches!(
        result.error,
        Some(kdown_engine::DownloadError::DestinationConflict(_))
    ));
    // Existing file untouched.
    assert_eq!(std::fs::read(&dest).expect("read"), b"original");
    // No network requests were made.
    assert!(scripted.request_log().is_empty());
}

#[tokio::test]
async fn replace_policy_replaces_an_existing_real_file() {
    let dir = tempfile::tempdir().expect("tmp");
    let destination = dir.path().join("replace.bin");
    std::fs::write(&destination, b"old bytes").expect("old destination");
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(5),
            ..ProbeMetadata::default()
        }))
        .expect_transfer(
            TransferStep::new().ok(TransferOk::new().total(5).chunk(b"new!!".as_slice())),
        );
    let controller = scripted_controller(&scripted);
    let mut request = DownloadRequest::new("https://scripted/replace", destination.clone());
    request.overwrite = kdown_engine::config::OverwritePolicy::Replace;

    let result = controller.run(request).await.expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(
        std::fs::read(&destination).expect("new destination"),
        b"new!!"
    );
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn concurrent_different_urls_cannot_touch_the_same_partial_output() {
    let directory = tempfile::tempdir().expect("tmp");
    let destination = directory.path().join("shared.bin");
    let part = directory.path().join("shared.bin.part");
    let metadata = ProbeMetadata {
        status: 200,
        total_size: Some(5),
        ..ProbeMetadata::default()
    };
    let first_script = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(metadata))
        .gate("hold-before-transfer")
        .expect_transfer(
            TransferStep::new().ok(TransferOk::new().total(5).chunk(b"hello".as_slice())),
        );
    let mut config = EngineConfig::default();
    config.transfer.preallocate_output = false;
    let first_controller = SingleStreamController::with_execution(
        HttpExecution::from_adapter(first_script.clone()),
        config.clone(),
    );
    let first_destination = destination.clone();
    let first = tokio::spawn(async move {
        first_controller
            .run(DownloadRequest::new(
                "https://first.example.test/object",
                first_destination,
            ))
            .await
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        while !part.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first job prepares its shared partial output");
    std::fs::write(&part, b"owner").expect("seed observable partial bytes");

    let second_script = ScriptedHttp::new();
    let resolver_calls = Arc::new(AtomicUsize::new(0));
    let second_controller = SingleStreamController::with_execution(
        HttpExecution::from_adapter(second_script.clone()),
        config,
    )
    .with_checkpoint_resolver(Arc::new(CountingResolver(resolver_calls.clone())));
    let second = second_controller
        .run(DownloadRequest::new(
            "https://different.example.test/object",
            destination.clone(),
        ))
        .await
        .expect("second job returns a structured terminal result");

    assert_eq!(second.status, ResultStatus::Failed);
    assert!(matches!(
        second.error,
        Some(kdown_engine::DownloadError::DestinationConflict(_))
    ));
    assert_eq!(
        resolver_calls.load(Ordering::SeqCst),
        0,
        "rejected before checkpoint resolution"
    );
    assert!(
        second_script.request_log().is_empty(),
        "rejected before network activity"
    );
    assert_eq!(std::fs::read(&part).expect("read owned partial"), b"owner");

    first_script.open_gate("hold-before-transfer");
    let first_result = first
        .await
        .expect("join first job")
        .expect("first job terminal");
    assert_eq!(
        first_result.status,
        ResultStatus::Completed,
        "{first_result:?}"
    );
    assert_eq!(std::fs::read(&destination).expect("read final"), b"hello");
    first_script.assert_all_consumed();
}

#[tokio::test]
async fn non_retryable_404_fails_immediately() {
    // Orchestration-only (§32): the classified 404 failure arrives through
    // the seam; the job must not retry a non-retryable status.
    let scripted = ScriptedHttp::new().expect_probe(
        ProbeStep::new().fail_error(kdown_engine::DownloadError::NotFound { status: 404 }),
    );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted);
    let result = c
        .run(DownloadRequest::new(
            "https://scripted/missing",
            dir.path().join("x"),
        ))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Failed);
    assert!(matches!(
        result.error,
        Some(kdown_engine::DownloadError::NotFound { status: 404 })
    ));
    // HEAD is the only request; no retry storm (§17.1).
    assert_eq!(scripted.request_log().len(), 1);
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn truncated_body_retries_from_zero_and_completes() {
    // First attempt: truncated mid-body (reset). Second attempt: full body.
    let content = deterministic_bytes(256 * 1024, 31337);
    let server = TestServer::new()
        .serve_n(
            "/flaky",
            1,
            ScriptedResponse::ok(content.clone()).reset_after(4096),
        )
        .serve_static("/flaky", content.clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("flaky.bin");

    let c = controller();
    let result = c
        .run(DownloadRequest::new(server.url("/flaky"), dest.clone()))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    let got = std::fs::read(&dest).expect("final");
    assert_bytes_exact(&got, &content);
    assert!(
        result.bytes_downloaded_from_network >= content.len() as u64,
        "network bytes must cover the full content ({}): {}",
        content.len(),
        result.bytes_downloaded_from_network
    );
    // Wasted bytes counted when the failed attempt surfaced data through
    // frames (buffering may swallow them; then 0 wasted is acceptable).
    assert!(server.request_count("/flaky").await >= 2, "retry happened");
    assert!(!dir.path().join("flaky.bin.part").exists());
}

#[tokio::test]
async fn oversize_body_rejected() {
    // Server sends more than the expected size.
    let server = TestServer::new()
        .serve_static("/big", vec![7u8; 2048])
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let mut req = DownloadRequest::new(server.url("/big"), dir.path().join("big.bin"));
    req.expected_size = Some(1024); // smaller than actual
    let c = controller();
    let result = c.run(req).await.expect("terminal");
    assert_eq!(result.status, ResultStatus::Failed);
    assert!(matches!(
        result.error,
        Some(kdown_engine::DownloadError::Protocol(_))
    ));
    assert!(!dir.path().join("big.bin").exists());
}

#[tokio::test]
async fn under_size_body_fails_integrity() {
    // Server sends fewer bytes than content-length promised.
    let server = TestServer::new()
        .serve_handler("/short", move |_req| {
            ScriptedResponse::ok(vec![1u8; 100]).with_header("content-length", "1000")
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let c = controller();
    let result = c
        .run(DownloadRequest::new(
            server.url("/short"),
            dir.path().join("short.bin"),
        ))
        .await
        .expect("terminal");
    // The transport delivers a truncated body (connection close before
    // content-length satisfied); the engine must not commit 100 bytes as
    // a 1000-byte download (§16.3 via connection-close detection).
    assert_eq!(result.status, ResultStatus::Failed, "{result:?}");
    assert!(!dir.path().join("short.bin").exists());
}

#[tokio::test]
async fn rate_limited_with_retry_after_retries_then_completes() {
    // First request (HEAD probe): 429 with Retry-After: 1. Subsequent: 200.
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    let content = Arc::new(deterministic_bytes(64 * 1024, 555));
    let hits = Arc::new(AtomicU32::new(0));
    let hits_for_handler = hits.clone();
    let content_for_handler = content.clone();
    let server = TestServer::new()
        .serve_handler("/limited", move |_req| {
            let n = hits_for_handler.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                ScriptedResponse::new(429).with_header("retry-after", "1")
            } else {
                ScriptedResponse::ok((*content_for_handler).clone())
            }
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("limited.bin");
    let c = controller();
    let started = std::time::Instant::now();
    let result = c
        .run(DownloadRequest::new(server.url("/limited"), dest.clone()))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    // The Retry-After (1 s) delayed the retry.
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "Retry-After honored: {:?}",
        started.elapsed()
    );
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    assert!(hits.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn cancel_stops_download_promptly() {
    // Orchestration-only cancellation (§32, §9.2): cancellation interrupts
    // a pending body read deterministically — no wall-clock waits.
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(1_000_000),
            ..ProbeMetadata::default()
        }))
        .expect_labeled_transfer(
            "cancel-transfer",
            TransferStep::new().ok(TransferOk::new().total(1_000_000).wait_for_cancellation()),
        );
    let dir = tempfile::tempdir().expect("tmp");
    let c = scripted_controller(&scripted);
    let req = DownloadRequest::new("https://scripted/slow", dir.path().join("slow.bin"));
    let (handle, join) = c.start(req);
    // Deterministic barrier: the body read is pending once the transfer
    // call was consumed.
    scripted.wait_for_phase("cancel-transfer").await;
    handle.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), join)
        .await
        .expect("no hang: cancellation interrupts the pending read")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Cancelled, "{result:?}");
    assert!(matches!(
        result.error,
        Some(kdown_engine::DownloadError::Cancelled)
    ));
    scripted.assert_all_consumed();
}
