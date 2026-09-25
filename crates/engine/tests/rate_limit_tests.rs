//! Task 7.1 end-to-end rate-limit coverage (§18): both transfer paths must
//! apply the configured per-job limiter AND the engine-global limiter,
//! runtime updates must reach running jobs, and waits must stop promptly on
//! cancellation. Unlimited paths keep their fast check (no lock).

use std::time::{Duration, Instant};

use kdown_engine::config::EngineConfig;
use kdown_engine::job::controller::{DownloadHandle, SingleStreamController};
use kdown_engine::{DownloadRequest, ResultStatus};

mod support;

use support::test_server::{RangeMode, RunningServer, TestServer};

fn cfg(segmentation_threshold: u64) -> EngineConfig {
    let mut c = EngineConfig::default();
    c.transfer.segmentation_threshold = segmentation_threshold;
    c
}

const MIB: u64 = 1024 * 1024;

async fn start_server(content: Vec<u8>) -> RunningServer {
    TestServer::new()
        .serve_ranges("/f.bin", content, RangeMode::Correct)
        .start()
        .await
        .expect("server")
}

async fn run_to_completion(
    _handle: &DownloadHandle,
    join: tokio::task::JoinHandle<
        Result<kdown_engine::DownloadResult, kdown_engine::DownloadError>,
    >,
) -> kdown_engine::DownloadResult {
    let result = tokio::time::timeout(Duration::from_secs(120), join)
        .await
        .expect("job must finish")
        .expect("join")
        .expect("terminal");
    assert_eq!(
        result.status,
        ResultStatus::Completed,
        "expected completion, got {:?} ({:?})",
        result.status,
        result.error
    );
    result
}

/// Payload-limited single-stream transfer: a job rate of 1 MiB/s over an
/// 8 MiB body must take at least ~7.75 s (burst ≈ 256 KiB is the only
/// headroom). Fails while the single-stream path ignores the bucket.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_stream_path_applies_job_rate_limit() {
    let size = 8 * MIB;
    let server = start_server(vec![0xAB; size as usize]).await;
    let mut c = cfg(64 * MIB); // below threshold: single-stream
    c.network.rate_limit = Some(MIB);
    let transport = kdown_engine::http::HttpTransport::from_config(&c).expect("transport");
    let controller = SingleStreamController::new(transport, c);

    let request = DownloadRequest::new(
        server.url("/f.bin"),
        std::env::temp_dir().join(format!("rl-single-{}", std::process::id())),
    );

    let (handle, _join) = controller.start(request);

    let started = Instant::now();
    let result = run_to_completion(&handle, _join).await;
    let wall = started.elapsed();

    assert!(
        wall >= Duration::from_secs(7),
        "single-stream transfer ignored the 1 MiB/s job limit: {wall:?}"
    );
    assert!(
        wall < Duration::from_secs(40),
        "unreasonably slow: {wall:?}"
    );
    assert_eq!(result.completed_bytes, size);
}

/// The engine-global limit binds alongside the job limit: 4 MiB/s job with a
/// 2 MiB/s global over 16 MiB must take ~7.75 s (the job limit alone would
/// allow ~3.9 s). Fails while the global bucket is never consulted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segmented_path_global_limit_binds_alongside_job_limit() {
    let size = 16 * MIB;
    let server = start_server(vec![0xCD; size as usize]).await;
    let mut c = cfg(1); // force segmented
    c.network.rate_limit = Some(4 * MIB);
    c.global_rate_limit = Some(2 * MIB);
    let transport = kdown_engine::http::HttpTransport::from_config(&c).expect("transport");
    let controller = SingleStreamController::new(transport, c);

    let request = DownloadRequest::new(
        server.url("/f.bin"),
        std::env::temp_dir().join(format!("rl-global-{}", std::process::id())),
    );

    let (handle, _join) = controller.start(request);

    let started = Instant::now();
    let result = run_to_completion(&handle, _join).await;
    let wall = started.elapsed();

    assert!(
        wall >= Duration::from_secs(7),
        "global limit did not bind alongside the job limit: {wall:?}"
    );
    assert!(
        wall < Duration::from_secs(60),
        "unreasonably slow: {wall:?}"
    );
    assert_eq!(result.completed_bytes, size);
}

/// One controller's global bucket is shared by its jobs: 2 × 8 MiB with a
/// 2 MiB/s global limit must take at least (16 MiB − burst)/2 MiB/s ≈ 7.9 s.
/// Fails while no global limiter exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_limit_is_shared_across_jobs() {
    let size = 8 * MIB;
    let server = start_server(vec![0xEF; size as usize]).await;
    let mut c = cfg(1); // segmented: both jobs in parallel
    c.global_rate_limit = Some(2 * MIB);
    let transport = kdown_engine::http::HttpTransport::from_config(&c).expect("transport");
    let controller = SingleStreamController::new(transport, c);

    let (h1, _j1) = controller.start(DownloadRequest::new(
        server.url("/f.bin"),
        std::env::temp_dir().join(format!("rl-shared-a-{}", std::process::id())),
    ));
    let (h2, _j2) = controller.start(DownloadRequest::new(
        server.url("/f.bin"),
        std::env::temp_dir().join(format!("rl-shared-b-{}", std::process::id())),
    ));

    let started = Instant::now();
    let r1 = run_to_completion(&h1, _j1).await;
    let r2 = run_to_completion(&h2, _j2).await;
    let combined = started.elapsed();

    assert_eq!(r1.completed_bytes + r2.completed_bytes, 2 * size);
    assert!(
        combined >= Duration::from_secs(7),
        "global limit not shared/enforced across jobs: {combined:?}"
    );
    assert!(
        combined < Duration::from_secs(60),
        "unreasonably slow: {combined:?}"
    );
}

/// A live `set_rate_limit` must reach a RUNNING single-stream job through
/// the same stable bucket object: raising the limit mid-transfer finishes
/// far sooner than the initial 1 MiB/s projection. Fails while the
/// single-stream path ignores the bucket.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rate_update_applies_live_on_running_single_stream_job() {
    let size = 8 * MIB;
    let server = start_server(vec![0x11; size as usize]).await;
    let mut c = cfg(64 * MIB);
    c.network.rate_limit = Some(MIB);
    let transport = kdown_engine::http::HttpTransport::from_config(&c).expect("transport");
    let controller = SingleStreamController::new(transport, c);

    let request = DownloadRequest::new(
        server.url("/f.bin"),
        std::env::temp_dir().join(format!("rl-live-{}", std::process::id())),
    );

    let started = Instant::now();
    let (handle, _join) = controller.start(request);

    // Let the initial 1 MiB/s phase run ~1.5 s (≈1.25 MiB transferred), then
    // raise the ceiling for the remainder. Total wall must show BOTH: the
    // limited phase ran (>= ~1.5 s) and the update took effect (an
    // un-updated 8 MiB at 1 MiB/s needs >= ~7.75 s).
    tokio::time::sleep(Duration::from_millis(1500)).await;
    handle.set_rate_limit(16 * MIB);

    let result = run_to_completion(&handle, _join).await;
    let wall = started.elapsed();

    assert!(
        wall >= Duration::from_millis(1400),
        "finished before the limited phase could run: {wall:?}"
    );
    assert!(
        wall <= Duration::from_secs(5),
        "live update did not speed the running job up (still limited?): {wall:?}"
    );
    assert_eq!(result.completed_bytes, size);
}

/// Waiting for tokens must stop promptly on cancellation (§18 / transfer-core
/// rate scenario): at 1 KiB/s a 128 KiB chunk implies a ~128 s wait; the job
/// must reach a terminal state well inside that, not sleep it out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_stops_rate_wait_promptly() {
    let size = 4 * MIB;
    let server = start_server(vec![0x22; size as usize]).await;
    let mut c = cfg(1); // segmented
    c.network.rate_limit = Some(1024);
    let transport = kdown_engine::http::HttpTransport::from_config(&c).expect("transport");
    let controller = SingleStreamController::new(transport, c);

    let request = DownloadRequest::new(
        server.url("/f.bin"),
        std::env::temp_dir().join(format!("rl-cancel-{}", std::process::id())),
    );

    let (handle, join) = controller.start(request);

    // First chunk is in its long rate wait by now.
    tokio::time::sleep(Duration::from_millis(400)).await;
    handle.cancel();

    let started = Instant::now();
    let outcome = tokio::time::timeout(Duration::from_secs(5), join).await;
    let elapsed = started.elapsed();
    assert!(
        outcome.is_ok(),
        "cancellation waited out the rate sleep instead of stopping promptly ({elapsed:?})"
    );
    assert!(
        elapsed <= Duration::from_secs(4),
        "terminal took too long after cancel: {elapsed:?}"
    );
}
