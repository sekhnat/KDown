//! Handle-based pause/cancel tests (task 3.8): pause stops network
//! activity promptly (§9.3); cancel is deterministic (§9.4); the state
//! machine reaches terminal states correctly (§9.2).

#[path = "support/mod.rs"]
mod support;

use std::time::Duration;

use kdown_engine::config::EngineConfig;
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};
use kdown_engine::job::state::JobState;
use support::fixtures::deterministic_bytes;
use support::test_server::{ScriptedResponse, TestServer};

fn controller() -> SingleStreamController {
    SingleStreamController::new(
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
        .expect("terminal result");
    assert_eq!(result.status, ResultStatus::Cancelled);
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
    let result = tokio::time::timeout(Duration::from_secs(30), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
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
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Cancelled);
    assert!(
        matches!(result.error, Some(kdown_engine::DownloadError::Cancelled)),
        "structured cancellation (§20)"
    );
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
    let result = join.await.expect("join").expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed);
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
    let result = join.await.expect("join").expect("terminal");
    assert_eq!(result.status, ResultStatus::Failed);
    assert_eq!(handle.state(), JobState::Failed);
    assert!(!dir.path().join("dead.bin.part").exists());
}
