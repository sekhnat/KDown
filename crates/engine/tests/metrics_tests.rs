//! Engine metrics export integration tests (§19.5, task 7.5).

use std::sync::Arc;
use std::time::Duration;

use kdown_engine::config::{EngineConfig, ExpectedHash, HashAlgorithm};
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::metrics::EngineMetrics;
use kdown_engine::{DownloadController, DownloadHandle, DownloadRequest, Event, EventStream};

mod support;
use support::test_server::{ScriptedResponse, TestServer};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_export_jobs_bytes_status_and_integrity() {
    let content = b"metrics fixture bytes".to_vec();
    let server = TestServer::new()
        .serve_static("/ok.bin", content.clone())
        .serve_handler("/missing.bin", |_| ScriptedResponse::new(404))
        .start()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmpdir");
    let cfg = EngineConfig::default();
    let metrics = EngineMetrics::shared();
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::with_metrics(transport, cfg, metrics.clone());

    let ok = controller
        .run(DownloadRequest::new(
            server.url("/ok.bin"),
            dir.path().join("ok.bin"),
        ))
        .await
        .expect("ok result");
    drop(ok);

    let missing = controller
        .run(DownloadRequest::new(
            server.url("/missing.bin"),
            dir.path().join("missing.bin"),
        ))
        .await
        .expect_err("missing result");
    drop(missing);

    let mut bad_hash = DownloadRequest::new(server.url("/ok.bin"), dir.path().join("bad-hash.bin"));
    bad_hash.integrity.expected_hashes = vec![ExpectedHash {
        algorithm: HashAlgorithm::Sha256,
        hex: "00".repeat(32),
    }];
    let mismatch = controller.run(bad_hash).await.expect_err("hash failure");
    drop(mismatch);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.jobs_started, 3);
    assert_eq!(snapshot.jobs_completed, 1);
    assert_eq!(snapshot.jobs_failed, 2);
    assert_eq!(snapshot.active_jobs, 0);
    assert!(snapshot.network_bytes >= content.len() as u64);
    assert_eq!(snapshot.status_counts.get("404"), Some(&1));
    assert_eq!(snapshot.integrity_failures, 1);
    assert!(snapshot.latency_count >= 3);
    assert!(snapshot
        .retry_categories
        .keys()
        .any(|k| k.contains("NotFound")));

    // Snapshot is serializable for host telemetry/export adapters.
    let json = serde_json::to_string(&snapshot).expect("metrics JSON");
    assert!(json.contains("jobs_completed"));
    let _ = Arc::strong_count(&metrics);
}

/// Task 5.2: subscribe before the job finishes, keep the handle, and drain
/// afterwards — the stream must end instead of awaiting the live hub.
async fn drain_after_terminal(handle: &DownloadHandle, mut events: EventStream) -> Vec<Event> {
    let drained = tokio::time::timeout(Duration::from_secs(5), async {
        let mut seen = Vec::new();
        while let Some(event) = events.next().await {
            seen.push(event);
        }
        seen
    })
    .await
    .expect("a retained handle must not keep the stream pending");
    assert!(
        handle.state().is_terminal(),
        "the handle must report the terminal state"
    );
    assert!(events.is_finished(), "stream reports the terminal signal");
    drained
}

fn stream_controller(cfg: EngineConfig) -> DownloadController {
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    DownloadController::new(transport, cfg)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_handle_stream_ends_after_completed_job() {
    let content = b"terminal stream fixture".to_vec();
    let server = TestServer::new()
        .serve_static("/done.bin", content.clone())
        .start()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmpdir");
    let controller = stream_controller(EngineConfig::default());
    let (handle, join) = controller.start(DownloadRequest::new(
        server.url("/done.bin"),
        dir.path().join("done.bin"),
    ));
    let events = handle.events();
    let terminal = tokio::time::timeout(Duration::from_secs(30), join)
        .await
        .expect("no hang")
        .expect("join");
    assert!(terminal.is_ok(), "download completes: {terminal:?}");
    let seen = drain_after_terminal(&handle, events).await;
    assert!(
        seen.iter().any(|e| matches!(e, Event::Committed { .. })),
        "queued terminal events are drained before the stream ends: {seen:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_handle_stream_ends_after_failed_job() {
    let server = TestServer::new()
        .serve_handler("/missing.bin", |_| ScriptedResponse::new(404))
        .start()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmpdir");
    let controller = stream_controller(EngineConfig::default());
    let (handle, join) = controller.start(DownloadRequest::new(
        server.url("/missing.bin"),
        dir.path().join("missing.bin"),
    ));
    let events = handle.events();
    let terminal = tokio::time::timeout(Duration::from_secs(30), join)
        .await
        .expect("no hang")
        .expect("join");
    assert!(terminal.is_err(), "404 fails the job: {terminal:?}");
    let _ = drain_after_terminal(&handle, events).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_handle_stream_ends_after_cancelled_job() {
    let content = vec![7u8; 8 * 1024 * 1024];
    let server = TestServer::new()
        .serve_handler("/slow.bin", move |_| {
            ScriptedResponse::ok(content.clone()).chunked(Duration::from_millis(20))
        })
        .start()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmpdir");
    let mut cfg = EngineConfig::default();
    cfg.transfer.segmentation_threshold = u64::MAX;
    let controller = stream_controller(cfg);
    let (handle, join) = controller.start(DownloadRequest::new(
        server.url("/slow.bin"),
        dir.path().join("slow.bin"),
    ));
    let events = handle.events();
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.cancel();
    let terminal = tokio::time::timeout(Duration::from_secs(30), join)
        .await
        .expect("no hang")
        .expect("join");
    assert!(
        terminal.is_err(),
        "cancellation is not a success: {terminal:?}"
    );
    let _ = drain_after_terminal(&handle, events).await;
}
