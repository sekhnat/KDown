//! Engine metrics export integration tests (§19.5, task 7.5).

use std::sync::Arc;

use kdown_engine::config::{EngineConfig, ExpectedHash, HashAlgorithm};
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};
use kdown_engine::metrics::EngineMetrics;
use kdown_engine::http::transport::HttpTransport;

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
    let controller = SingleStreamController::with_metrics(transport, cfg, metrics.clone());

    let ok = controller
        .run(DownloadRequest::new(server.url("/ok.bin"), dir.path().join("ok.bin")))
        .await
        .expect("ok result");
    assert_eq!(ok.status, ResultStatus::Completed);

    let missing = controller
        .run(DownloadRequest::new(
            server.url("/missing.bin"),
            dir.path().join("missing.bin"),
        ))
        .await
        .expect("missing result");
    assert_eq!(missing.status, ResultStatus::Failed);

    let mut bad_hash = DownloadRequest::new(
        server.url("/ok.bin"),
        dir.path().join("bad-hash.bin"),
    );
    bad_hash.integrity.expected_hashes = vec![ExpectedHash {
        algorithm: HashAlgorithm::Sha256,
        hex: "00".repeat(32),
    }];
    let mismatch = controller.run(bad_hash).await.expect("hash result");
    assert_eq!(mismatch.status, ResultStatus::Failed);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.jobs_started, 3);
    assert_eq!(snapshot.jobs_completed, 1);
    assert_eq!(snapshot.jobs_failed, 2);
    assert_eq!(snapshot.active_jobs, 0);
    assert!(snapshot.network_bytes >= content.len() as u64);
    assert_eq!(snapshot.status_counts.get("404"), Some(&1));
    assert_eq!(snapshot.integrity_failures, 1);
    assert!(snapshot.latency_count >= 3);
    assert!(snapshot.retry_categories.keys().any(|k| k.contains("NotFound")));

    // Snapshot is serializable for host telemetry/export adapters.
    let json = serde_json::to_string(&snapshot).expect("metrics JSON");
    assert!(json.contains("jobs_completed"));
    let _ = Arc::strong_count(&metrics);
}
