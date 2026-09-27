//! Deterministic network-condition controls (task 4.1).
//!
//! The scripted test server applies latency, seeded jitter, seeded
//! per-response loss, bandwidth pacing, and scripted mid-body resets keyed
//! on a global response ordinal. These tests verify that replaying the
//! same seed yields identical error/output/accounting under both the
//! sequential and the segmented mode.

mod support;

use std::sync::Arc;
use std::time::Duration;

use kdown_engine::{
    CompletedDownload, DownloadController, DownloadRequest, DownloadRunError, EngineConfig,
    H2ConnectionPolicy, HttpTransport,
};

use support::fixtures;
use support::test_server::{NetworkConditions, TestServer};

/// The deterministic, timing-independent part of a terminal outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fingerprint {
    completed_bytes: u64,
    network_bytes: u64,
    reused_bytes: u64,
    wasted_bytes: u64,
    retries: u64,
    segment_requests: u64,
    /// SHA-256 of the published output file (empty when nothing published).
    output: String,
    /// Terminal classification: Ok, or the error's debug form.
    outcome: String,
}

fn fingerprint(
    result: &Result<CompletedDownload, DownloadRunError>,
    dest: &std::path::Path,
) -> Fingerprint {
    match result {
        Ok(completed) => Fingerprint {
            completed_bytes: completed.accounting.completed_bytes,
            network_bytes: completed.accounting.bytes_downloaded_from_network,
            reused_bytes: completed.accounting.bytes_reused_from_checkpoint,
            wasted_bytes: completed.accounting.wasted_bytes,
            retries: completed.accounting.retries,
            segment_requests: completed.accounting.segment_requests,
            output: fixtures::file_sha256(dest),
            outcome: "Ok".into(),
        },
        Err(error) => Fingerprint {
            completed_bytes: error.accounting().completed_bytes,
            network_bytes: error.accounting().bytes_downloaded_from_network,
            reused_bytes: error.accounting().bytes_reused_from_checkpoint,
            wasted_bytes: error.accounting().wasted_bytes,
            retries: error.accounting().retries,
            segment_requests: error.accounting().segment_requests,
            output: String::new(),
            outcome: format!("{error:?}"),
        },
    }
}

fn sequential_config() -> EngineConfig {
    EngineConfig::default()
}

/// Fixed-size segmented config: deterministic lease boundaries, so
/// replays exercise identical request patterns.
fn segmented_config() -> EngineConfig {
    let mut cfg = EngineConfig::default();
    cfg.transfer.segmentation_threshold = 16 * 1024;
    cfg.transfer.max_workers = 4;
    cfg.transfer.min_workers = 4;
    cfg.transfer.segment_sizing = kdown_engine::config::SegmentSizing::Explicit;
    cfg.transfer.initial_segment_size = 64 * 1024;
    cfg.transfer.min_segment_size = 64 * 1024;
    cfg.transfer.max_segment_size = 64 * 1024;
    cfg.h2_policy = H2ConnectionPolicy::Single;
    cfg
}

/// Run one download against a fresh server with `conditions` and return
/// the outcome fingerprint (the seed is recorded on stderr for replay).
async fn run_once(
    content: Arc<Vec<u8>>,
    conditions: NetworkConditions,
    cfg: EngineConfig,
) -> Fingerprint {
    let server = TestServer::new()
        .serve_static("/file.bin", (*content).clone())
        .network(conditions);
    let running = server.start().await.expect("server");
    eprintln!("[seed recorded] {}", conditions.describe());

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let request = DownloadRequest::new(running.url("/file.bin"), dest.clone());
    let result = tokio::time::timeout(Duration::from_secs(60), controller.run(request))
        .await
        .expect("no hang");
    fingerprint(&result, &dest)
}

const CONTENT: usize = 256 * 1024;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_latency_and_jitter_is_identical_sequential() {
    let content = Arc::new(fixtures::deterministic_bytes(CONTENT as u64, 0x4A));
    let conditions = NetworkConditions {
        seed: 0xC0FFEE,
        latency: Duration::from_millis(15),
        jitter_ms: 5,
        loss_permille: 0,
        bandwidth_bytes_per_s: 0,
        reset_first_responses: 0,
    };
    let first = run_once(content.clone(), conditions, sequential_config()).await;
    let second = run_once(content.clone(), conditions, sequential_config()).await;
    assert_eq!(first, second, "replay must be identical (sequential)");
    assert_eq!(first.outcome, "Ok");
    assert_eq!(first.completed_bytes, CONTENT as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replay_latency_and_jitter_is_identical_segmented() {
    let content = Arc::new(fixtures::deterministic_bytes(CONTENT as u64, 0x4B));
    let conditions = NetworkConditions {
        seed: 0xC0FFEE,
        latency: Duration::from_millis(10),
        jitter_ms: 4,
        loss_permille: 0,
        bandwidth_bytes_per_s: 0,
        reset_first_responses: 0,
    };
    let first = run_once(content.clone(), conditions, segmented_config()).await;
    let second = run_once(content.clone(), conditions, segmented_config()).await;
    assert_eq!(first, second, "replay must be identical (segmented)");
    assert_eq!(first.outcome, "Ok");
    assert_eq!(first.completed_bytes, CONTENT as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_loss_is_identical_across_modes() {
    // 30% per-response loss: deterministic retries; the outcome (and its
    // retry count) must replay exactly in both modes.
    let content = Arc::new(fixtures::deterministic_bytes(CONTENT as u64, 0x4C));
    let conditions = NetworkConditions {
        // Ordinal 1 (the second response) draws a loss for this seed.
        seed: 0xDEADBEEF,
        latency: Duration::from_millis(5),
        jitter_ms: 0,
        loss_permille: 300,
        bandwidth_bytes_per_s: 0,
        reset_first_responses: 0,
    };
    for (name, cfg) in [
        ("sequential", sequential_config()),
        ("segmented", segmented_config()),
    ] {
        let first = run_once(content.clone(), conditions, cfg.clone()).await;
        let second = run_once(content.clone(), conditions, cfg).await;
        assert_eq!(
            first, second,
            "replay must be identical under loss ({name})"
        );
        assert_eq!(first.outcome, "Ok", "30% loss must still recover ({name})");
        assert!(first.retries > 0, "loss must produce retries ({name})");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_mid_body_resets_are_identical_and_bounded() {
    // The first three responses die mid-body; the fourth completes. Both
    // replay runs must produce the same output and the same accounting.
    // Fast retry budget: the probe consumes some resets and the backoff
    // must not stretch the scenario.
    let content = Arc::new(fixtures::deterministic_bytes(CONTENT as u64, 0x4D));
    let mut cfg = sequential_config();
    cfg.retry.max_attempts_per_segment = 6;
    cfg.retry.base_delay = Duration::from_millis(5);
    cfg.retry.max_delay = Duration::from_millis(10);
    let conditions = NetworkConditions {
        seed: 0x1234,
        latency: Duration::from_millis(2),
        jitter_ms: 0,
        loss_permille: 0,
        bandwidth_bytes_per_s: 0,
        reset_first_responses: 3,
    };
    let first = run_once(content.clone(), conditions, cfg.clone()).await;
    let second = run_once(content.clone(), conditions, cfg).await;
    assert_eq!(first, second, "replay must be identical under resets");
    assert_eq!(
        first.outcome, "Ok",
        "resets recover within the retry budget"
    );
    assert!(
        first.retries >= 2,
        "mid-body resets produce retries: retries={}",
        first.retries
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bandwidth_pacing_is_honored_and_replays() {
    let content = Arc::new(fixtures::deterministic_bytes(CONTENT as u64, 0x4E));
    let conditions = NetworkConditions {
        seed: 7,
        latency: Duration::ZERO,
        jitter_ms: 0,
        loss_permille: 0,
        bandwidth_bytes_per_s: 256 * 1024, // 256 KiB/s: >= ~1 s for 256 KiB
        reset_first_responses: 0,
    };
    let started = std::time::Instant::now();
    let first = run_once(content.clone(), conditions, sequential_config()).await;
    let elapsed = started.elapsed();
    assert_eq!(first.outcome, "Ok");
    assert!(
        elapsed >= Duration::from_millis(900),
        "the server paces at 256 KiB/s: elapsed {elapsed:?}"
    );
    let second = run_once(content.clone(), conditions, sequential_config()).await;
    assert_eq!(first, second, "paced replay must be identical");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn total_loss_fails_deterministically_with_identical_accounting() {
    // 100% loss: every response dies; both replays fail with the same
    // typed error and the same partial accounting.
    let content = Arc::new(fixtures::deterministic_bytes(CONTENT as u64, 0x4F));
    let mut cfg = sequential_config();
    cfg.retry.max_attempts_per_segment = 3;
    cfg.retry.base_delay = Duration::from_millis(5);
    cfg.retry.max_delay = Duration::from_millis(5);
    let conditions = NetworkConditions {
        seed: 0xDEAD,
        latency: Duration::ZERO,
        jitter_ms: 0,
        loss_permille: 1000,
        bandwidth_bytes_per_s: 0,
        reset_first_responses: 0,
    };
    let first = run_once(content.clone(), conditions, cfg.clone()).await;
    let second = run_once(content, conditions, cfg).await;
    assert_eq!(first, second, "total-loss replay must be identical");
    assert_ne!(first.outcome, "Ok", "total loss must fail");
    assert_eq!(
        first.completed_bytes, 0,
        "nothing completes under total loss"
    );
}
