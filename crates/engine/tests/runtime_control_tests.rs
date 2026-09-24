//! Runtime control tests (task 5.8): concurrency reduction and rate-limit
//! changes mid-transfer converge without data loss (§7.3, §18.2).

#[path = "support/mod.rs"]
mod support;

use std::sync::Arc;
use std::time::Duration;

use kdown_engine::config::{EngineConfig, TransferPolicy};
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};
use support::fixtures::{assert_bytes_exact, deterministic_bytes};
use support::test_server::{ScriptedResponse, TestServer};

fn segmented_cfg() -> EngineConfig {
    let mut c = EngineConfig {
        transfer: TransferPolicy {
            segmentation_threshold: 1024 * 1024,
            max_workers: 4,
            min_workers: 1,
            verify_range_support: true,
            ..TransferPolicy::default()
        },
        ..EngineConfig::default()
    };
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

/// The rate-limit test server: ranges served from deterministic content.
fn rate_limit_server(
    content: Arc<Vec<u8>>,
) -> crate::support::test_server::TestServer {
    TestServer::new().serve_handler("/ratelimit", move |req| {
        let total = content.len() as u64;
        if req.method == "HEAD" {
            return ScriptedResponse::ok((*content).clone())
                .with_header("accept-ranges", "bytes");
        }
        if let Some((s, e)) = req.range {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrency_reduction_mid_transfer_no_data_loss() {
    // Slow-drip server so the transfer is in flight when the reduction
    // lands; output stays byte-exact (§7.3: excess workers settle leases
    // safely, no byte range is lost).
    let content = Arc::new(deterministic_bytes(3 * 1024 * 1024, 3001));
    let server = {
        let content = content.clone();
        TestServer::new().serve_handler("/reduce", move |req| {
            let total = content.len() as u64;
            if req.method == "HEAD" {
                return ScriptedResponse::ok((*content).clone())
                    .with_header("accept-ranges", "bytes");
            }
            if let Some((s, e)) = req.range {
                let body = content[s as usize..=(e as usize).min(total as usize - 1)].to_vec();
                ScriptedResponse::new(206)
                    .with_body(body)
                    .with_header("content-range", &format!("bytes {s}-{e}/{total}"))
                    .with_header("accept-ranges", "bytes")
                    .chunked(Duration::from_millis(5))
            } else {
                ScriptedResponse::ok((*content).clone()).with_header("accept-ranges", "bytes")
            }
        })
    }
    .start()
    .await
    .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("reduce.bin");
    let mut c_cfg = segmented_cfg();
    c_cfg.transfer.max_segment_size = 1024 * 1024;
    let c = controller(c_cfg);
    let req = DownloadRequest::new(server.url("/reduce"), dest.clone());
    let (handle, join) = c.start(req);
    // Give the workers time to fan out, then cut to one worker.
    tokio::time::sleep(Duration::from_millis(200)).await;
    handle.set_concurrency(1);
    let result = tokio::time::timeout(Duration::from_secs(60), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    assert!(!dir.path().join("reduce.bin.part").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn rate_limit_change_mid_transfer_converges() {
    // Lower the limit mid-transfer; the effective rate must converge to
    // the new limit without job restart or data loss (§18.2).
    let content = Arc::new(deterministic_bytes(2 * 1024 * 1024, 3002));
    let server = {
        let content = content.clone();
        TestServer::new().serve_handler("/ratelimit", move |req| {
            let total = content.len() as u64;
            if req.method == "HEAD" {
                return ScriptedResponse::ok((*content).clone())
                    .with_header("accept-ranges", "bytes");
            }
            if let Some((s, e)) = req.range {
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
    let dest = dir.path().join("ratelimit.bin");
    let mut c_cfg = segmented_cfg();
    c_cfg.transfer.max_segment_size = 1024 * 1024;
    let c = controller(c_cfg);
    let req = DownloadRequest::new(server.url("/ratelimit"), dest.clone());
    let (handle, join) = c.start(req);
    // Let the transfer start, then impose a small limit: 4 MiB/s.
    tokio::time::sleep(Duration::from_millis(150)).await;
    handle.set_rate_limit(4 * 1024 * 1024);
    assert_eq!(handle.rate_limit(), Some(4 * 1024 * 1024));
    let result = tokio::time::timeout(Duration::from_secs(60), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    assert!(!dir.path().join("ratelimit.bin.part").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn rate_limit_slow_limit_throttles_throughput() {
    // 4 MiB at line speed, then cut to 128 KiB/s with ~1 MiB remaining:
    // the tail must take >= ~6 s (burst 32 KiB ≈ 0.25 s of rate), proving
    // the bucket gates reads after the runtime change (§18.2).
    let content = Arc::new(deterministic_bytes(6 * 1024 * 1024, 3003));
    let server = {
        let content = content.clone();
        TestServer::new().serve_handler("/slow", move |req| {
            let total = content.len() as u64;
            if req.method == "HEAD" {
                return ScriptedResponse::ok((*content).clone())
                    .with_header("accept-ranges", "bytes");
            }
            if let Some((s, e)) = req.range {
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
    let dest = dir.path().join("slow.bin");
    let mut c_cfg = segmented_cfg();
    c_cfg.transfer.max_segment_size = 2 * 1024 * 1024;
    c_cfg.transfer.segmentation_threshold = 1024 * 1024;
    let c = controller(c_cfg);
    let req = DownloadRequest::new(server.url("/slow"), dest.clone());
    let (handle, join) = c.start(req);
    // Wait for the segmented job to register, then wait for real progress
    // (5 MiB done or 1.5 s), then impose the small limit with ~1 MiB left.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let done = handle.snapshot().completed_bytes;
        if done >= 5 * 1024 * 1024
            || handle.segmented_job().is_none()
            || std::time::Instant::now() > deadline
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.set_rate_limit(128 * 1024);
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(120), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    // Remaining ~1 MiB at 128 KiB/s minus burst: >= ~6 s; 1 s margin.
    assert!(
        started.elapsed() >= Duration::from_millis(4500),
        "rate limit must throttle the tail: {:?}",
        started.elapsed()
    );
}

/// Update BEFORE the job starts reaches the eventual job (task 5.3): the
/// stable per-job bucket carries the pre-start rate into the transfer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rate_limit_update_before_job_is_honored() {
    let content = Arc::new(deterministic_bytes(2 * 1024 * 1024, 3003));
    let server = rate_limit_server(content.clone()).start().await.expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("prestart.bin");
    let c_cfg = segmented_cfg();
    let c = controller(c_cfg);
    let (handle, join) = c.start(DownloadRequest::new(server.url("/ratelimit"), dest.clone()));
    // Update lands before the transfer begins (the job is still probing):
    // the STABLE bucket object carries it into the eventual transfer (task
    // 5.3) — no bucket swap, no worker restart.
    handle.set_rate_limit(1024 * 1024);
    // The getter reads the SAME stable bucket the job will use.
    assert_eq!(handle.rate_limit(), Some(1024 * 1024));
    // Unlimited ↔ limited cycle during the transfer (§18.2): switching to
    // unlimited must not require restarting workers or replacing the bucket.
    tokio::time::sleep(Duration::from_millis(120)).await;
    handle.set_rate_limit(0);
    assert_eq!(handle.rate_limit(), None, "0 = unlimited");
    let result = tokio::time::timeout(Duration::from_secs(60), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    assert!(!dir.path().join("prestart.bin.part").exists());
}

/// Retried ranges after a mid-transfer reset count retransmitted overhead:
/// wire bytes exceed unique completed bytes and the final file is exact
/// (task 5.4 — shard totals vs actual received and unique coverage).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retried_ranges_count_wasted_bytes_with_exact_coverage() {
    let content = Arc::new(deterministic_bytes(2 * 1024 * 1024, 3004));
    let server = {
        let content = content.clone();
        TestServer::new().serve_handler("/waste", move |req| {
            let total = content.len() as u64;
            if req.method == "HEAD" {
                return ScriptedResponse::ok((*content).clone())
                    .with_header("accept-ranges", "bytes");
            }
            if let Some((s, e)) = req.range {
                let body = content[s as usize..=(e as usize).min(total as usize - 1)].to_vec();
                // Every range response resets after 64 KiB: each lease
                // needs retries, losing received-but-unacked bytes.
                ScriptedResponse::new(206)
                    .with_body(body)
                    .with_header("content-range", &format!("bytes {s}-{e}/{total}"))
                    .with_header("accept-ranges", "bytes")
                    .reset_after(64 * 1024)
            } else {
                ScriptedResponse::ok((*content).clone()).with_header("accept-ranges", "bytes")
            }
        })
    }
    .start()
    .await
    .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("waste.bin");
    let mut c_cfg = segmented_cfg();
    c_cfg.transfer.max_segment_size = 512 * 1024;
    c_cfg.retry.base_delay = Duration::from_millis(10);
    let c = controller(c_cfg);
    let result = c
        .run(DownloadRequest::new(server.url("/waste"), dest.clone()))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    // Unique coverage across retries is EXACT (task 5.4): tail-only retry
    // requeues unwritten bytes as pending, so no byte is received twice —
    // wire bytes equal unique completed bytes equal the fixture.
    assert_eq!(
        result.bytes_downloaded_from_network, result.completed_bytes,
        "retried coverage must not duplicate delivery: {result:?}"
    );
    assert_eq!(result.bytes_downloaded_from_network, content.len() as u64);
    assert!(result.retries >= 1, "the resets must have been retried");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
}

/// Single-stream restarts after mid-body failure charge the discarded
/// stream prefix as wasted bytes attributed to the job (task 5.4).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_stream_restart_counts_wasted_bytes() {
    let content = Arc::new(deterministic_bytes(2 * 1024 * 1024, 3005));
    let server = {
        let content = content.clone();
        TestServer::new().serve_handler("/waste2", move |req| {
            let total = content.len() as u64;
            if req.method == "HEAD" {
                return ScriptedResponse::ok((*content).clone())
                    .with_header("accept-ranges", "bytes");
            }
            // Serve ranges correctly (the single-stream restart resumes with
            // a ranged request) and reset every response 512 KiB in: the
            // restart discards the delivered prefix's progress each time.
            if let Some((s, e)) = req.range {
                let body = content[s as usize..=(e as usize).min(total as usize - 1)].to_vec();
                ScriptedResponse::new(206)
                    .with_body(body)
                    .with_header("content-range", &format!("bytes {s}-{e}/{total}"))
                    .with_header("accept-ranges", "bytes")
                    .reset_after(512 * 1024)
            } else {
                ScriptedResponse::ok((*content).clone())
                    .with_header("accept-ranges", "bytes")
                    .reset_after(512 * 1024)
            }
        })
    }
    .start()
    .await
    .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("waste2.bin");
    let mut c_cfg = segmented_cfg();
    c_cfg.transfer.segmentation_threshold = u64::MAX; // single stream
    c_cfg.retry.base_delay = Duration::from_millis(10);
    let c = controller(c_cfg);
    let result = c
        .run(DownloadRequest::new(server.url("/waste2"), dest.clone()))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    // The discarded stream prefixes are charged as wasted bytes (task 5.4)
    // while unique delivered coverage stays exact.
    assert!(
        result.wasted_bytes > 0,
        "the discarded stream prefix is wasted: {result:?}"
    );
    assert!(result.retries >= 1, "{result:?}");
    assert_eq!(
        result.completed_bytes, content.len() as u64,
        "unique coverage is exact: {result:?}"
    );
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
}

/// Increase → decrease → increase (task 8.2): dormant workers reactivate on
/// runtime increase without rebuilding, and no work is lost across the
/// cycles (retry, pause and cancellation keep working).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrency_increase_decrease_increase_no_lost_work() {
    let content = Arc::new(deterministic_bytes(2 * 1024 * 1024, 3006));
    let server = rate_limit_server(content.clone()).start().await.expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("cycle.bin");
    let mut c_cfg = segmented_cfg();
    c_cfg.transfer.max_workers = 4;
    c_cfg.transfer.min_workers = 1;
    c_cfg.transfer.max_segment_size = 512 * 1024;
    let c = controller(c_cfg);
    let (handle, join) = c.start(DownloadRequest::new(server.url("/ratelimit"), dest.clone()));
    // Decrease: workers above the desired count settle and go dormant.
    tokio::time::sleep(Duration::from_millis(80)).await;
    handle.set_concurrency(1);
    tokio::time::sleep(Duration::from_millis(80)).await;
    // Increase: dormant workers reactivate without rebuilding the job.
    handle.set_concurrency(4);
    tokio::time::sleep(Duration::from_millis(40)).await;
    // Decrease again mid-transfer.
    handle.set_concurrency(2);
    tokio::time::sleep(Duration::from_millis(40)).await;
    // Final increase to the max.
    handle.set_concurrency(4);
    let result = tokio::time::timeout(Duration::from_secs(60), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(result.completed_bytes, content.len() as u64, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    assert!(!dir.path().join("cycle.bin.part").exists());
}

/// Manual concurrency control clamps to the configured bounds (task 8.2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_concurrency_clamps_to_configured_bounds() {
    let content = Arc::new(deterministic_bytes(1024 * 1024, 3007));
    let server = rate_limit_server(content.clone()).start().await.expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("clamp.bin");
    let mut c_cfg = segmented_cfg();
    c_cfg.transfer.max_workers = 3;
    c_cfg.transfer.min_workers = 2;
    let c = controller(c_cfg);
    let (handle, join) = c.start(DownloadRequest::new(server.url("/ratelimit"), dest.clone()));
    // Wait for the live job handle.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let job = handle.segmented_job().expect("live segmented job").clone();
    // Below min → clamped up; above max → clamped down.
    handle.set_concurrency(0);
    assert_eq!(job.desired_workers(), 2, "clamped to min_workers");
    handle.set_concurrency(99);
    assert_eq!(job.desired_workers(), 3, "clamped to max_workers");
    handle.set_concurrency(3);
    let result = tokio::time::timeout(Duration::from_secs(60), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
}

/// Opt-in adaptive concurrency (task 9.1): an adaptive job starts at
/// `min_workers`, stays within bounds while probing, and completes
/// byte-exact. Fixed mode (default config) starts at `max_workers`
/// (unchanged behavior). H2's connection policy is independent of the
/// adaptive stream count (design D6) — the stream worker count adjusts;
/// no extra physical connections are assumed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adaptive_concurrency_starts_at_min_and_stays_in_bounds() {
    let content = Arc::new(deterministic_bytes(2 * 1024 * 1024, 3008));
    let server = rate_limit_server(content.clone()).start().await.expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("adaptive.bin");

    let mut adaptive_cfg = segmented_cfg();
    adaptive_cfg.transfer.max_workers = 4;
    adaptive_cfg.transfer.min_workers = 2;
    adaptive_cfg.transfer.concurrency_mode =
        kdown_engine::config::ConcurrencyMode::Adaptive;
    adaptive_cfg.transfer.max_segment_size = 512 * 1024;
    let c = controller(adaptive_cfg);
    let (handle, join) = c.start(DownloadRequest::new(server.url("/ratelimit"), dest.clone()));
    // While the transfer runs, the desired count stays within [min, max]
    // and begins at min (the controller probes up from there).
    let mut observed = Vec::new();
    for _ in 0..20 {
        if let Some(job) = handle.segmented_job() {
            let desired = job.desired_workers();
            assert!(
                (2..=4).contains(&desired),
                "adaptive desired count out of bounds: {desired}"
            );
            observed.push(desired);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        observed.first() == Some(&2),
        "adaptive mode starts at min_workers: {observed:?}"
    );
    let result = tokio::time::timeout(Duration::from_secs(60), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
}

/// Fixed mode (default) is unchanged: the job starts at the configured
/// fixed concurrency (task 9.1: no silent behavior change).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_mode_starts_at_configured_max() {
    let content = Arc::new(deterministic_bytes(1024 * 1024, 3009));
    let server = rate_limit_server(content.clone()).start().await.expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("fixed.bin");
    let mut c_cfg = segmented_cfg();
    c_cfg.transfer.max_workers = 4;
    let c = controller(c_cfg);
    let (handle, join) = c.start(DownloadRequest::new(server.url("/ratelimit"), dest.clone()));
    tokio::time::sleep(Duration::from_millis(60)).await;
    let job = handle.segmented_job().expect("live segmented job").clone();
    assert_eq!(job.desired_workers(), 4, "fixed mode starts at max_workers");
    let result = tokio::time::timeout(Duration::from_secs(60), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
}
