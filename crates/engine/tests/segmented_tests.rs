//! End-to-end segmented-mode tests (task 5.5): eligibility gate, worker
//! pool with bounded concurrency, byte-exact multi-worker download, and
//! randomized-failure behavior (§36.6). Single-stream fallbacks and
//! unknown-length handling live here too (task 5.7).

#[path = "support/mod.rs"]
mod support;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kdown_engine::config::{EngineConfig, TransferPolicy};
use kdown_engine::http::probe::ProbeMetadata;
use kdown_engine::http::scripted::{ProbeStep, ScriptedHttp, TransferOk, TransferStep};
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::http::HttpExecution;
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};
use kdown_engine::resume::checkpoint_store::CheckpointStore;
use support::fixtures::{assert_bytes_exact, deterministic_bytes};
use support::test_server::{RangeMode, ScriptedResponse, TestServer};

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
    // Faster tests: shorter retry delays.
    c.retry.base_delay = Duration::from_millis(20);
    c.retry.max_delay = Duration::from_millis(100);
    c
}

fn controller(cfg: EngineConfig) -> SingleStreamController {
    SingleStreamController::new(
        HttpTransport::new(cfg.network.clone()).expect("transport"),
        cfg,
    )
}

/// Scripted orchestration controller (§32): network-neutral cases run
/// through the deterministic adapter — no sockets, no delays.
fn scripted_controller(scripted: &ScriptedHttp, cfg: EngineConfig) -> SingleStreamController {
    SingleStreamController::with_execution(HttpExecution::from_adapter(scripted.clone()), cfg)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segmented_download_byte_exact() {
    let content = deterministic_bytes(2 * 1024 * 1024, 2001);
    let server = TestServer::new()
        .serve_static("/seg.bin", content.clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("seg.bin");

    let mut c_cfg = cfg(1024 * 1024);
    c_cfg.transfer.max_segment_size = 512 * 1024; // force several segments
    let c = controller(c_cfg);
    let result = c
        .run(DownloadRequest::new(server.url("/seg.bin"), dest.clone()))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(result.final_path.as_deref(), Some(dest.as_path()));
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    assert!(!dir.path().join("seg.bin.part").exists(), "no temp residue");
    // Multiple range requests prove segmented mode ran.
    let count = server.request_count("/seg.bin").await;
    assert!(
        count > 1,
        "segmented mode issues multiple requests: {count}"
    );
}

#[tokio::test]
async fn small_resource_uses_single_stream() {
    // Orchestration-only eligibility (§10.3, §32): below the threshold the
    // probe metadata's single eligibility decision selects sequential —
    // no per-segment requests.
    let content = deterministic_bytes(64 * 1024, 2002);
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(content.len() as u64),
            ..ProbeMetadata::default()
        }))
        .expect_transfer(
            TransferStep::new().ok(TransferOk::new()
                .total(content.len() as u64)
                .chunk(content.clone())),
        );
    let c = scripted_controller(&scripted, cfg(16 * 1024 * 1024));
    let result = c
        .run(DownloadRequest::new(
            "https://scripted/small.bin",
            tempfile::tempdir().expect("tmp").path().join("small.bin"),
        ))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    // HEAD probe + one sequential transfer; no per-segment requests.
    assert!(
        scripted.request_log().len() <= 2,
        "small resource must not segment: {:?}",
        scripted.request_log()
    );
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn lying_range_server_downgrades_to_single_stream() {
    // §11.2/§36.2: server advertises ranges but returns 200 full-body.
    // The engine must still produce byte-exact output via safe fallback.
    let content = deterministic_bytes(2 * 1024 * 1024, 2003);
    let server = TestServer::new()
        .serve_ranges("/liar.bin", content.clone(), RangeMode::Full200)
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("liar.bin");
    let c = controller(cfg(1024 * 1024));
    let result = c
        .run(DownloadRequest::new(server.url("/liar.bin"), dest.clone()))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    assert!(!dir.path().join("liar.bin.part").exists());
}

#[tokio::test]
async fn unknown_length_server_uses_sequential_mode() {
    // §25: chunked/unknown-length responses download sequentially and
    // still commit byte-exact; progress has no ETA but the job completes.
    let content = Arc::new(deterministic_bytes(512 * 1024, 2004));
    let content_for_handler = content.clone();
    let server = TestServer::new()
        .serve_handler("/unknown", move |_req| {
            ScriptedResponse::ok((*content_for_handler).clone()).unknown_length()
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("unknown.bin");
    let c = controller(cfg(16 * 1024 * 1024));
    let result = c
        .run(DownloadRequest::new(server.url("/unknown"), dest.clone()))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    assert_eq!(result.total_size, None, "unknown length stays unknown");
}

#[tokio::test]
async fn randomized_failures_segmented_still_exact() {
    let content = Arc::new(deterministic_bytes(2 * 1024 * 1024, 2004));
    let resets = Arc::new(AtomicU32::new(0));
    let server = {
        let content = content.clone();
        let resets = resets.clone();
        TestServer::new().serve_handler("/flaky-seg", move |req| {
            let total = content.len() as u64;
            if let Some((s, e)) = req.range {
                let n = resets.fetch_add(1, Ordering::SeqCst);
                let body = content[s as usize..=(e as usize).min(total as usize - 1)].to_vec();
                let mut r = ScriptedResponse::new(206)
                    .with_body(body)
                    .with_header("content-range", &format!("bytes {s}-{e}/{total}"))
                    .with_header("accept-ranges", "bytes");
                if n % 3 == 0 {
                    // Kill ~1/3 of range responses mid-body.
                    r.reset_after = Some(4096 + (n as usize % 5) * 8192);
                }
                r
            } else {
                ScriptedResponse::ok((*content).clone()).with_header("accept-ranges", "bytes")
            }
        })
    }
    .start()
    .await
    .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("flaky-seg.bin");
    let mut c_cfg = cfg(1024 * 1024);
    c_cfg.transfer.max_segment_size = 512 * 1024;
    c_cfg.retry.max_attempts_per_segment = 16;
    let c = controller(c_cfg);
    let result = c
        .run(DownloadRequest::new(server.url("/flaky-seg"), dest.clone()))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    assert!(
        resets.load(Ordering::Relaxed) > 0,
        "test must have induced resets"
    );
    assert!(!dir.path().join("flaky-seg.bin.part").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinated_503_backoff_completes() {
    // Orchestration-only 503 coordination (§17.4, §32): every segment's
    // first attempt is rate limited server-side, the coordinated origin
    // gate delays the retries, and the job completes byte-exact. One
    // unordered range-keyed phase keeps arrival order irrelevant.
    let content = deterministic_bytes(4000, 2005);
    let seg = |start: u64| {
        TransferStep::new()
            .range((start, start + 999))
            .ok(TransferOk::new()
                .range(start, start + 999)
                .total(4000)
                .chunk(content[start as usize..(start + 1000) as usize].to_vec()))
    };
    let server_error = || kdown_engine::http::HttpFailure {
        error: kdown_engine::DownloadError::Server { status: 503 },
        retry_after: Some(Duration::from_millis(1)),
        challenge: None,
    };
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(4000),
            accept_ranges: true,
            range_verified: true,
            ..ProbeMetadata::default()
        }))
        .expect_unordered_ranges(
            "503-then-ok",
            vec![
                TransferStep::new().range((0, 999)).fail(server_error()),
                TransferStep::new().range((1000, 1999)).fail(server_error()),
                TransferStep::new().range((2000, 2999)).fail(server_error()),
                TransferStep::new().range((3000, 3999)).fail(server_error()),
                seg(0),
                seg(1000),
                seg(2000),
                seg(3000),
            ],
        );
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("gate.bin");
    let mut c_cfg = cfg(1024);
    c_cfg.transfer.max_segment_size = 1000;
    c_cfg.transfer.min_segment_size = 1;
    let c = scripted_controller(&scripted, c_cfg);
    let result = c
        .run(DownloadRequest::new("https://scripted/gate", dest.clone()))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    // §17.4: coordinated delay — bounded requests rather than every
    // worker hammering independently: exactly one 503 + one success per
    // segment.
    assert_eq!(
        scripted.request_log().len(),
        9,
        "coordinated backoff must prevent a thundering herd: {:?}",
        scripted.request_log()
    );
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn induced_503_never_redownloads_completed_ranges() {
    // §17.3/§17.4: after a 503 burst, retried segments must resume at
    // their tail offsets — the server sees every range request starting
    // at or after its segment start, and no range is requested twice in
    // full once completed.
    let content = Arc::new(deterministic_bytes(2 * 1024 * 1024, 2006));
    let requested = Arc::new(std::sync::Mutex::new(Vec::<(u64, u64)>::new()));
    let hits = Arc::new(AtomicU32::new(0));
    let server = {
        let content = content.clone();
        let requested = requested.clone();
        let hits = hits.clone();
        TestServer::new().serve_handler("/tail", move |req| {
            let total = content.len() as u64;
            if req.method == "HEAD" {
                return ScriptedResponse::ok((*content).clone())
                    .with_header("accept-ranges", "bytes");
            }
            let n = hits.fetch_add(1, Ordering::SeqCst);
            if let Some((s, e)) = req.range {
                requested.lock().expect("requested").push((s, e));
                if n < 3 {
                    // Three initial 503s to engage the coordinated gate.
                    return ScriptedResponse::new(503)
                        .with_header("retry-after", "0")
                        .with_header("accept-ranges", "bytes");
                }
                let body = content[s as usize..=(e as usize).min(total as usize - 1)].to_vec();
                ScriptedResponse::new(206)
                    .with_body(body)
                    .with_header("content-range", &format!("bytes {s}-{e}/{total}"))
                    .with_header("accept-ranges", "bytes")
            } else {
                ScriptedResponse::ok((*content).clone()).with_header("accept-ranges", "bytes")
            }
        })
    }
    .start()
    .await
    .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("tail.bin");
    let mut c_cfg = cfg(1024 * 1024);
    c_cfg.transfer.max_segment_size = 1024 * 1024;
    let c = controller(c_cfg);
    let result = c
        .run(DownloadRequest::new(server.url("/tail"), dest.clone()))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    // No two range requests may cover the same completed range in full:
    // every requested range must be unique (§17.3 tail-only retry).
    let mut seen_start = std::collections::HashSet::new();
    for (s, _e) in requested.lock().expect("requested").iter() {
        assert!(
            seen_start.insert(*s),
            "range starting at {s} was re-requested after completion"
        );
    }
}

#[tokio::test]
async fn oversized_body_overrun_detected() {
    // A 206 whose body overshoots the accepted range is rejected (§11.2),
    // retried, and the final output remains byte-exact once a retry
    // returns a correct body.
    let content = Arc::new(deterministic_bytes(1024 * 1024, 2005));
    let hits = Arc::new(AtomicU32::new(0));
    let server = {
        let content = content.clone();
        let hits = hits.clone();
        TestServer::new().serve_handler("/overrun", move |req| {
            let total = content.len() as u64;
            if let Some((s, e)) = req.range {
                let n = hits.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    // Overshoot: send the whole tail, not the slice.
                    let body = content[s as usize..].to_vec();
                    return ScriptedResponse::new(206)
                        .with_body(body)
                        .with_header("content-range", &format!("bytes {s}-{}/{total}", total - 1))
                        .with_header("accept-ranges", "bytes");
                }
                let body = content[s as usize..=(e as usize).min(total as usize - 1)].to_vec();
                ScriptedResponse::new(206)
                    .with_body(body)
                    .with_header("content-range", &format!("bytes {s}-{e}/{total}"))
                    .with_header("accept-ranges", "bytes")
            } else {
                ScriptedResponse::ok((*content).clone()).with_header("accept-ranges", "bytes")
            }
        })
    }
    .start()
    .await
    .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("overrun.bin");
    let mut c_cfg = cfg(512 * 1024);
    c_cfg.transfer.max_segment_size = 256 * 1024;
    c_cfg.retry.max_attempts_per_segment = 8;
    let c = controller(c_cfg);
    let result = c
        .run(DownloadRequest::new(server.url("/overrun"), dest.clone()))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segmented_resume_reuses_all_completed_ranges() {
    // Admission-boundary evidence for segmented mode: disjoint admitted
    // ranges are all reused, only the remaining ranges are fetched, and
    // the output is byte-identical (§15.5, §12.1).
    let content = Arc::new(deterministic_bytes(2 * 1024 * 1024, 2010));
    let server = TestServer::new()
        .serve_static("/seg-resume.bin", (*content).clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("seg-resume.bin");
    let temp = dir.path().join("seg-resume.bin.part");
    // Partial local state: [0, 64 KiB) and [1 MiB, 1 MiB + 64 KiB).
    let prefix_len = 64 * 1024;
    let second_start = 1024 * 1024;
    let mut temp_bytes = content[..prefix_len].to_vec();
    temp_bytes.resize(second_start + prefix_len, 0);
    temp_bytes[second_start..second_start + prefix_len]
        .copy_from_slice(&content[second_start..second_start + prefix_len]);
    std::fs::write(&temp, &temp_bytes).expect("temp");
    let identity = kdown_engine::resume::job_identity(&server.url("/seg-resume.bin"), &dest);
    let store = kdown_engine::resume::FileCheckpointStore::new(
        dir.path(),
        kdown_engine::resume::DurabilityMode::Performance,
    )
    .expect("store");
    let mut cp =
        kdown_engine::resume::Checkpoint::new(&identity, server.url("/seg-resume.bin"), "tmp");
    cp.total_size = Some(content.len() as u64);
    cp.completed_ranges = vec![
        (0, prefix_len as u64 - 1),
        (
            second_start as u64,
            second_start as u64 + prefix_len as u64 - 1,
        ),
    ];
    store.save_atomic(&cp).expect("save");

    let c = controller(cfg(1024 * 1024));
    let result = c
        .run(DownloadRequest::new(
            server.url("/seg-resume.bin"),
            dest.clone(),
        ))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    // Both disjoint ranges counted once as reused.
    assert_eq!(result.bytes_reused_from_checkpoint, 2 * prefix_len as u64);
    // No residue after commit (§14.6 step 5).
    assert!(store.load(&identity).expect("load").is_none());
    assert!(!temp.exists());
}
