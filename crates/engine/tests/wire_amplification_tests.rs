//! Wire-amplification reproducer (optimize-transfer-engine-v2 task 0.1).
//!
//! The historical `transfer-core` delta claims a dynamic split "excludes
//! bytes already read or queued for write by the original worker". The
//! scheduler's `split_tail` actually splits at the lease's *acknowledged
//! write* frontier (`next_offset`), which excludes acknowledged bytes only —
//! bytes the original worker has received (or is still streaming) beyond
//! that frontier are re-delivered by the split lease's request while the
//! original request keeps streaming to its stale end. This test pins the
//! server-emitted payload against uniquely accepted bytes so that a live-
//! tail split can never double the wire payload (2× is an unconditional
//! failure; the phase-3 target is stricter, <1.10 on the clean split
//! fixture).

#[path = "support/mod.rs"]
mod support;

use std::sync::Arc;
use std::time::Duration;

use kdown_engine::config::{EngineConfig, TransferPolicy};
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};
use support::fixtures::{assert_bytes_exact, deterministic_bytes};
use support::test_server::{ScriptedResponse, TestServer};

/// One whole-file lease (explicit sizing) plus idle workers that must split
/// the live tail: the historical worst case for overlap re-delivery.
fn cfg(len: u64) -> EngineConfig {
    let mut c = EngineConfig {
        transfer: TransferPolicy {
            segmentation_threshold: 1024,
            min_workers: 1,
            max_workers: 4,
            initial_segment_size: len,
            max_segment_size: len,
            ..TransferPolicy::default()
        },
        ..EngineConfig::default()
    };
    c.retry.base_delay = Duration::from_millis(20);
    c.retry.max_delay = Duration::from_millis(100);
    c
}

/// Static range-correct content with a small per-chunk delay so the first
/// worker stays on the wire long enough for idle workers to observe and
/// split the live tail.
fn delayed_static(
    content: Arc<Vec<u8>>,
    delay: Duration,
) -> impl Fn(&support::test_server::RequestInfo) -> ScriptedResponse + Send + Sync + 'static {
    move |req| {
        let content = content.clone();
        let len = content.len() as u64;
        let base = match req.range {
            Some((s, e)) => {
                let end = e.min(len.saturating_sub(1));
                ScriptedResponse::new(206)
                    .with_body(content[s as usize..=(end as usize)].to_vec())
                    .with_header("content-range", &format!("bytes {s}-{end}/{len}"))
            }
            None => ScriptedResponse::ok((*content).clone()),
        };
        base.with_header("accept-ranges", "bytes")
            .chunked(delay)
    }
}

// Ignored until the phase-3 split-eligibility fix (task 3.6) removes the
// overlap re-delivery: the current engine measures exactly 2.000x on this
// scenario (2026-09-24 baseline), which is the documented gap this change
// closes. Unignore and tighten to <1.10 in task 3.6.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "known baseline gap: live-tail split re-delivers overlap (2.000x measured); task 3.6 unignores and tightens to <1.10"]
async fn live_tail_split_never_doubles_wire_payload() {
    let len = 8 * 1024 * 1024;
    let content = deterministic_bytes(len, 4242);
    let server = TestServer::new()
        .serve_handler(
            "/amp.bin",
            delayed_static(Arc::new(content.clone()), Duration::from_millis(2)),
        )
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("amp.bin");

    let c = SingleStreamController::new(
        HttpTransport::new(cfg(len).network).expect("transport"),
        cfg(len),
    );
    let result = c
        .run(DownloadRequest::new(server.url("/amp.bin"), dest.clone()))
        .await
        .expect("terminal");

    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    // Accepted unique coverage is exact regardless of overlap.
    assert_eq!(result.completed_bytes, len, "accepted bytes must equal file size");

    let emitted = server.payload_emitted().await;
    assert!(
        emitted >= len,
        "server must serve at least the full payload: {emitted}"
    );
    let amplification = emitted as f64 / len as f64;
    assert!(
        amplification < 2.0,
        "wire amplification {amplification:.3}x reached the unconditional 2x \
         failure bound (emitted {emitted} bytes for {len} accepted)"
    );
}
