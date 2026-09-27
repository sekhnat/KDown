//! Handle-based pause/cancel tests (task 3.8): pause stops network
//! activity promptly (§9.3); cancel is deterministic (§9.4); the state
//! machine reaches terminal states correctly (§9.2).

mod support;

use std::time::Duration;

use kdown_engine::config::EngineConfig;
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::JobState;
use kdown_engine::{DownloadController, DownloadRequest};
use support::fixtures::deterministic_bytes;
use support::test_server::{ScriptedResponse, TestServer};

fn controller() -> DownloadController {
    DownloadController::new(
        HttpTransport::new(kdown_engine::config::NetworkPolicy::default()).expect("transport"),
        EngineConfig::default(),
    )
}

#[tokio::test]
async fn pause_stops_network_activity_promptly() {
    let content = vec![2u8; 8_000_000];
    let server = TestServer::new()
        .serve_handler("/pausable", move |_req| {
            ScriptedResponse::ok(content.clone()).chunked(Duration::from_millis(50))
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("paused.bin");

    let c = controller();
    let (handle, join) = c.start(DownloadRequest::new(server.url("/pausable"), dest.clone()));
    // Let it transfer for a bit, then pause.
    tokio::time::sleep(Duration::from_millis(300)).await;
    handle.pause();
    let before = handle.snapshot().network_bytes;
    assert!(before > 0, "transfer started before pause");
    // Wait well beyond the chunk cadence: at most one in-flight chunk may
    // settle (§9.3 stop at next safe chunk boundary); then reads freeze.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after = handle.snapshot().network_bytes;
    assert!(
        after - before <= 64 * 1024 + 1,
        "at most one in-flight 64 KiB chunk may settle after pause \
         (before {before}, after {after})"
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    let settled = handle.snapshot().network_bytes;
    assert_eq!(
        after, settled,
        "network bytes must be fully frozen after the in-flight chunk settles"
    );
    // Cancel from paused state: deterministic terminal.
    handle.cancel();
    let result = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("converges quickly")
        .expect("join")
        .expect_err("terminal result");
    assert!(matches!(
        result,
        kdown_engine::error::DownloadRunError::Cancelled(_)
    ));
    assert_eq!(handle.state(), JobState::Cancelled);
    // Temp cleaned up (DeletePartial default for cancel, §9.4).
    assert!(!dir.path().join("paused.bin.part").exists());
    assert!(!dest.exists());
}

#[tokio::test]
async fn resume_after_pause_completes() {
    let content = vec![5u8; 2_000_000];
    let expected = content.clone();
    let server = TestServer::new()
        .serve_handler("/resumable", move |_req| {
            ScriptedResponse::ok(content.clone()).chunked(Duration::from_millis(30))
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("resumed.bin");

    let c = controller();
    let (handle, join) = c.start(DownloadRequest::new(server.url("/resumable"), dest.clone()));
    tokio::time::sleep(Duration::from_millis(200)).await;
    handle.pause();
    tokio::time::sleep(Duration::from_millis(150)).await;
    handle.resume_now();
    let _ = tokio::time::timeout(Duration::from_secs(30), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    let got = std::fs::read(&dest).expect("final");
    assert_eq!(got, expected);
    assert_eq!(handle.state(), JobState::Completed);
}

#[tokio::test]
async fn cancel_midstream_deletes_temp() {
    let content = vec![3u8; 4_000_000];
    let server = TestServer::new()
        .serve_handler("/cancelme", move |_req| {
            ScriptedResponse::ok(content.clone()).chunked(Duration::from_millis(50))
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("cancelled.bin");

    let c = controller();
    let (handle, join) = c.start(DownloadRequest::new(server.url("/cancelme"), dest.clone()));
    tokio::time::sleep(Duration::from_millis(300)).await;
    handle.cancel();
    let result = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("prompt convergence")
        .expect("join")
        .expect_err("terminal");
    assert!(matches!(
        result,
        kdown_engine::error::DownloadRunError::Cancelled(_)
    ));
    // DeletePartial semantics: temp removed, no residue (§9.4).
    assert!(!dir.path().join("cancelled.bin.part").exists());
    assert!(!dest.exists());
    assert_eq!(handle.state(), JobState::Cancelled);
}

#[tokio::test]
async fn state_machine_terminal_after_normal_run() {
    let content = deterministic_bytes(4096, 4242);
    let server = TestServer::new()
        .serve_static("/state", content)
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let c = controller();
    let (handle, join) = c.start(DownloadRequest::new(
        server.url("/state"),
        dir.path().join("state.bin"),
    ));
    let _ = join.await.expect("join").expect("terminal");
    assert_eq!(handle.state(), JobState::Completed);
    assert!(handle.state().is_terminal());
    // Snapshot reflects full download.
    let snap = handle.snapshot();
    assert_eq!(snap.completed_bytes, 4096);
    assert_eq!(snap.network_bytes, 4096);
}

#[tokio::test]
async fn failed_job_leaves_no_temp_residue() {
    let server = TestServer::new()
        .serve_handler("/dead", |_req| ScriptedResponse::new(404))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let c = controller();
    let (handle, join) = c.start(DownloadRequest::new(
        server.url("/dead"),
        dir.path().join("dead.bin"),
    ));
    let _ = join.await.expect("join").expect_err("terminal");
    assert_eq!(handle.state(), JobState::Failed);
    assert!(!dir.path().join("dead.bin.part").exists());
}

/// `max_active_jobs` is enforced synchronously with an RAII permit: an
/// over-cap start is rejected immediately with a typed error and writes
/// no artifact, and the permit is released when a job finishes so a later
/// start is admitted (§9.5, design D5).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_job_cap_rejects_immediately_and_releases_on_completion() {
    let content = vec![3u8; 4_000_000];
    let server = TestServer::new()
        .serve_handler("/capped", move |_req| {
            ScriptedResponse::ok(content.clone()).chunked(Duration::from_millis(40))
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let first_dest = dir.path().join("first.bin");
    let second_dest = dir.path().join("second.bin");
    let cfg = EngineConfig {
        max_active_jobs: 1,
        ..EngineConfig::default()
    };
    let c = DownloadController::new(
        HttpTransport::new(cfg.network.clone()).expect("transport"),
        cfg,
    );

    let (first_handle, first_join) = c.start(DownloadRequest::new(
        server.url("/capped"),
        first_dest.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        first_handle.snapshot().network_bytes > 0,
        "the first job is admitted and transferring"
    );

    let (_second_handle, second_join) = c.start(DownloadRequest::new(
        server.url("/capped"),
        second_dest.clone(),
    ));
    let error = tokio::time::timeout(Duration::from_secs(10), second_join)
        .await
        .expect("no hang")
        .expect("task");
    let error = error.expect_err("an over-cap start must be rejected");
    assert_eq!(error.category(), kdown_engine::ErrorCategory::MemoryCap);
    assert!(
        matches!(
            error.as_engine_error(),
            Some(kdown_engine::DownloadError::AdmissionRejected { cap: 1, .. })
        ),
        "typed admission rejection: {error:?}"
    );
    assert!(!second_dest.exists(), "a rejected job publishes nothing");
    assert!(
        !dir.path().join("second.bin.part").exists(),
        "a rejected job writes no partial artifact"
    );

    // The first job's permit is released on its terminal path, so the cap
    // admits a later job.
    first_handle.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), first_join)
        .await
        .expect("no hang");
    let third_dest = dir.path().join("third.bin");
    let (_third_handle, third_join) = c.start(DownloadRequest::new(
        server.url("/capped"),
        third_dest.clone(),
    ));
    let completed = tokio::time::timeout(Duration::from_secs(60), third_join)
        .await
        .expect("no hang")
        .expect("task")
        .expect("a job started after a release must be admitted");
    assert!(completed.final_path.exists());
}

// ---- Job deadline (task 4.4) ----

fn deadline_controller(cfg: EngineConfig) -> DownloadController {
    DownloadController::new(
        HttpTransport::new(cfg.network.clone()).expect("transport"),
        cfg,
    )
}

/// A stalled response head must not outlive the deadline: the header wait
/// is interrupted by the same token.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_interrupts_stalled_header_wait_promptly() {
    let server = TestServer::new()
        .serve_handler("/stall", |_req| {
            ScriptedResponse::ok(vec![1, 2, 3]).delayed_headers(Duration::from_secs(30))
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("stall.bin");
    let mut cfg = EngineConfig::default();
    cfg.transfer.job_deadline = Some(Duration::from_millis(80));
    let c = deadline_controller(cfg);
    let started = std::time::Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        c.run(DownloadRequest::new(server.url("/stall"), dest.clone())),
    )
    .await
    .expect("bounded expiry")
    .expect_err("the deadline must fail the job");
    assert_eq!(
        error.category(),
        kdown_engine::ErrorCategory::DeadlineExceeded,
        "typed deadline outcome: {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "expiry latency must be bounded, took {:?}",
        started.elapsed()
    );
    assert!(!dest.exists(), "nothing may be published");
}

/// A server-directed `Retry-After` backoff longer than the deadline must be
/// interrupted instead of slept through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_interrupts_retry_after_backoff() {
    let server = TestServer::new()
        .serve_handler("/throttle", |_req| {
            ScriptedResponse::new(503).with_header("retry-after", "30")
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("throttle.bin");
    let mut cfg = EngineConfig::default();
    cfg.retry.honor_retry_after = true;
    cfg.transfer.job_deadline = Some(Duration::from_millis(100));
    let c = deadline_controller(cfg);
    let started = std::time::Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        c.run(DownloadRequest::new(server.url("/throttle"), dest.clone())),
    )
    .await
    .expect("bounded expiry")
    .expect_err("the deadline must fail the job");
    assert_eq!(
        error.category(),
        kdown_engine::ErrorCategory::DeadlineExceeded
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the 30s backoff must be interrupted, took {:?}",
        started.elapsed()
    );
    assert!(!dest.exists());
}

/// A job paused indefinitely still expires at its deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_expires_while_paused() {
    let content = vec![4u8; 4_000_000];
    let server = TestServer::new()
        .serve_handler("/paused", move |_req| {
            ScriptedResponse::ok(content.clone()).chunked(Duration::from_millis(30))
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("paused.bin");
    let mut cfg = EngineConfig::default();
    cfg.transfer.job_deadline = Some(Duration::from_millis(300));
    let c = deadline_controller(cfg);
    let (handle, join) = c.start(DownloadRequest::new(server.url("/paused"), dest.clone()));
    tokio::time::sleep(Duration::from_millis(60)).await;
    handle.pause();
    let error = tokio::time::timeout(Duration::from_secs(5), join)
        .await
        .expect("a paused job must still expire")
        .expect("task")
        .expect_err("the deadline must fail the paused job");
    assert_eq!(
        error.category(),
        kdown_engine::ErrorCategory::DeadlineExceeded
    );
    assert!(!dest.exists());
}

/// A slow body observes the deadline within a bounded number of chunks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_interrupts_slow_body() {
    let content = vec![5u8; 8_000_000];
    let server = TestServer::new()
        .serve_handler("/slow", move |_req| {
            ScriptedResponse::ok(content.clone()).chunked(Duration::from_millis(25))
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("slow.bin");
    let mut cfg = EngineConfig::default();
    cfg.transfer.job_deadline = Some(Duration::from_millis(120));
    let c = deadline_controller(cfg);
    let started = std::time::Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        c.run(DownloadRequest::new(server.url("/slow"), dest.clone())),
    )
    .await
    .expect("bounded expiry")
    .expect_err("the deadline must fail the job");
    assert_eq!(
        error.category(),
        kdown_engine::ErrorCategory::DeadlineExceeded
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "slow bodies must stop promptly, took {:?}",
        started.elapsed()
    );
    assert!(!dest.exists());
}
