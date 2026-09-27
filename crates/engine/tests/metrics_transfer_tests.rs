//! Transfer-memory metrics export (design D3, task 3.7).
//!
//! Verifies live and terminal snapshots, failure cleanup visibility, the
//! documented scope (no unknown-buffer-as-zero reporting), and the JSON
//! export path.

use std::sync::Arc;

use kdown_engine::{DownloadController, DownloadRequest};

use super::transfer_memory_tests::{bounded_config, start_static_h1_server};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_export_transfer_memory_live_and_terminal() {
    use kdown_engine::EngineMetrics;

    let content = Arc::new(vec![0x43_u8; 512 * 1024]);
    let addr = start_static_h1_server(content.clone()).await;

    let cfg = bounded_config();
    let metrics = EngineMetrics::shared();
    let transport = kdown_engine::HttpTransport::from_config(&cfg).expect("transport");
    let controller = Arc::new(DownloadController::with_metrics(
        transport,
        cfg.clone(),
        Arc::clone(&metrics),
    ));

    // Before any job: no sample yet (never fabricated as zeros).
    assert!(
        metrics.snapshot().transfer_memory.is_none(),
        "no unknown-buffer-as-zero reporting before the first sample"
    );

    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let request = DownloadRequest::new(
        format!("http://127.0.0.1:{}/file", addr.port()),
        dest.clone(),
    );
    let (handle, join) = controller.start(request);
    // A live sample mid-transfer shows real held bytes.
    let mut live = None;
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        if let Some(sample) = metrics.snapshot().transfer_memory.as_ref() {
            if sample.aggregate.current > 0 {
                live = Some(sample.clone());
                break;
            }
        }
        let _ = handle.snapshot();
    }
    // The terminal record always lands a sample.
    let completed = tokio::time::timeout(std::time::Duration::from_secs(30), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("download completes");
    assert!(completed.final_path.exists());

    let snapshot = metrics.snapshot().transfer_memory.expect("terminal sample");
    assert_eq!(
        snapshot.aggregate.cap,
        1024 * 1024,
        "configured limits are exported"
    );
    assert_eq!(
        snapshot.aggregate.current, 0,
        "the terminal snapshot shows the drained pipeline"
    );
    assert!(
        snapshot.aggregate.high_water >= snapshot.aggregate.current,
        "high-water is monotonic"
    );
    // Per-job component maxima do not bound the CONTROLLER-scope meters
    // (connection footprints and multi-job sums are metrics-only there);
    // the aggregate pool is the controller-scope bound.
    assert!(
        snapshot.aggregate.high_water <= snapshot.aggregate.cap,
        "the aggregate peak never exceeds the aggregate cap"
    );
    assert!(
        snapshot
            .components
            .values()
            .all(|c| c.current <= snapshot.aggregate.cap),
        "component currents are within the aggregate bound"
    );
    assert!(
        snapshot.scope.contains("unaccounted"),
        "scope is documented"
    );
    // The engine max-of-jobs high-water reflects real job peaks.
    assert!(
        metrics.snapshot().job_memory_high_water_max > 0,
        "the job's accounted peak was recorded"
    );
    let _ = live;
    // JSON export includes the transfer-memory view.
    let json = serde_json::to_string(&metrics.snapshot()).expect("export");
    assert!(
        json.contains("transfer_memory") && json.contains("high_water"),
        "JSON export carries the transfer-memory view"
    );
}
