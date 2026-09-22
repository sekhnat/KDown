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
    SingleStreamController::new(HttpTransport::new(cfg.network.clone()).expect("transport"), cfg)
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