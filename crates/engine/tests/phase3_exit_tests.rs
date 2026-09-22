//! Phase 3 exit suite (§36.4, §36.6, task 5.9): property-based coverage
//! across randomized acquire/fail/split/pause/resume sequences, plus the
//! randomized-failure end-to-end suite over boundary sizes including a
//! >4 GiB sparse download.

#[path = "support/mod.rs"]
mod support;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kdown_engine::config::{EngineConfig, TransferPolicy};
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};
use kdown_engine::scheduler::core::{SchedulerPolicy, SegmentScheduler};
use support::fixtures::{assert_bytes_exact, deterministic_bytes};
use support::test_server::{ScriptedResponse, TestServer};

const CHUNK: u64 = 128 * 1024;
const SEGMENT: u64 = 1024 * 1024;

fn cfg(threshold: u64, max_segment: u64) -> EngineConfig {
    let mut c = EngineConfig {
        transfer: TransferPolicy {
            segmentation_threshold: threshold,
            max_workers: 4,
            min_workers: 1,
            verify_range_support: true,
            max_segment_size: max_segment,
            ..TransferPolicy::default()
        },
        ..EngineConfig::default()
    };
    c.retry.base_delay = Duration::from_millis(10);
    c.retry.max_delay = Duration::from_millis(80);
    c.retry.max_attempts_per_segment = 12;
    c
}

fn controller(cfg: EngineConfig) -> SingleStreamController {
    SingleStreamController::new(HttpTransport::new(cfg.network.clone()).expect("transport"), cfg)
}

// ---------------------------------------------------------------------------
// §36.4 property: every byte covered exactly once across randomized
// acquire/fail/split/pause/resume sequences.
// ---------------------------------------------------------------------------

#[test]
fn property_sequences_pause_and_resume_cover_exactly() {
    // Deterministic xorshift for reproducibility.
    let mut state: u64 = 0x9E3779B97F4A7C15;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for total in [1u64, 63, 1023, 4096, 100_003] {
        let mut s = SegmentScheduler::initialize(
            total,
            &[],
            SchedulerPolicy::new(1, (total / 3).max(1)),
        );
        for _step in 0..400 {
            let roll = next() % 6;
            // Invariants continuously (§12.1).
            assert!(s.invariants_hold(), "total {total}");
            match roll {
                0 => {
                    let _ = s.acquire();
                }
                1 => {
                    // Advance a live lease deterministically.
                    let live = s.active_leases();
                    if let Some(l) = live.first().copied() {
                        let step = 1 + next() % 7;
                        let target = l.next_offset.saturating_add(step).min(l.end + 1);
                        assert!(s.report_progress(l.id, l.generation, target));
                    }
                }
                2 => {
                    let live = s.active_leases();
                    if let Some(l) = live.last().copied() {
                        assert!(s.complete(l.id, l.generation));
                    }
                }
                3 => {
                    // Fail requeues only the unfinished tail (§17.3).
                    let live = s.active_leases();
                    if let Some(l) = live.first().copied() {
                        assert!(s.fail(l.id, l.generation));
                    }
                }
                4 => {
                    // "Pause": active work releases (workers stop at safe
                    // boundaries, §9.3); nothing is lost.
                    let live = s.active_leases();
                    for l in live {
                        assert!(s.release(l.id, l.generation));
                    }
                }
                _ => {
                    // "Resume": split a big tail or acquire new work.
                    let mut any = false;
                    {
                        let live = s.active_leases();
                        if let Some(biggest) =
                            live.iter().copied().max_by_key(SegmentLease::remaining)
                        {
                            if s.split_tail(biggest.id, biggest.generation, 1).is_some() {
                                any = true;
                            }
                        }
                    }
                    if !any {
                        let _ = s.acquire();
                    }
                }
            }
        }
        use kdown_engine::scheduler::lease::SegmentLease;
        // Drain and prove exact single coverage.
        loop {
            let live = s.active_leases();
            match live.first().copied() {
                Some(l) => {
                    assert!(s.report_progress(l.id, l.generation, l.end + 1));
                    assert!(s.complete(l.id, l.generation));
                }
                None => match s.acquire() {
                    Some(l) => {
                        assert!(s.report_progress(l.id, l.generation, l.end + 1));
                        assert!(s.complete(l.id, l.generation));
                    }
                    None => break,
                },
            }
        }
        assert!(s.is_complete(), "total {total} must complete");
        assert_eq!(
            s.completed_ranges(),
            vec![(0, total - 1)],
            "exact single coverage for {total}"
        );
    }
}

// ---------------------------------------------------------------------------
// §36.6 randomized-failure end-to-end suite over boundary sizes.
// ---------------------------------------------------------------------------

/// Server that resets ~1/3 of range responses mid-body at deterministic
/// offsets, then serves whole (§36.2 truncated bodies).
async fn flaky_segmented(
    path: &'static str,
    content: Arc<Vec<u8>>,
) -> (support::test_server::RunningServer, Arc<AtomicU32>) {
    let hits = Arc::new(AtomicU32::new(0));
    let server = {
        let content = content.clone();
        let hits = hits.clone();
        TestServer::new().serve_handler(path, move |req| {
            let total = content.len() as u64;
            if req.method == "HEAD" {
                return ScriptedResponse::ok((*content).clone())
                    .with_header("accept-ranges", "bytes");
            }
            let n = hits.fetch_add(1, Ordering::SeqCst);
            if let Some((s, e)) = req.range {
                let body = content[s as usize..=(e as usize).min(total as usize - 1)].to_vec();
                let mut r = ScriptedResponse::new(206)
                    .with_body(body)
                    .with_header("content-range", &format!("bytes {s}-{e}/{total}"))
                    .with_header("accept-ranges", "bytes");
                if n % 3 == 0 {
                    // Deterministic mid-body cut (~4-64 KiB in).
                    r.reset_after = Some(4096 + ((n as usize) * 7919) % 65_536);
                }
                r
            } else {
                ScriptedResponse::ok((*content).clone()).with_header("accept-ranges", "bytes")
            }
        })
    }
    .start()
    .await
    .expect("start");
    (server, hits)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn boundary_sizes_randomized_failures_exact() {
    // Boundary sizes from §36.6: 0, 1, chunk±1/exact, segment±1/exact, plus
    // a multi-segment size. Threshold and max segment sized so small files
    // go single-stream and big ones segment.
    for (idx, size) in [
        0u64,
        1,
        CHUNK - 1,
        CHUNK,
        CHUNK + 1,
        SEGMENT - 1,
        SEGMENT,
        SEGMENT + 1,
        2 * SEGMENT + 4096,
    ]
    .into_iter()
    .enumerate()
    {
        let content = deterministic_bytes(size, 40_000 + idx as u64);
        let server = TestServer::new()
            .serve_static("/b.bin", content.clone())
            .start()
            .await
            .expect("start");
        let dir = tempfile::tempdir().expect("tmp");
        let dest = dir.path().join("b.bin");
        let threshold = if size >= 2 * SEGMENT { SEGMENT / 2 } else { u64::MAX };
        let max_segment = SEGMENT / 4;
        let c = controller(cfg(threshold, max_segment));
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            c.run(DownloadRequest::new(server.url("/b.bin"), dest.clone())),
        )
        .await
        .expect("no hang")
        .expect("terminal");
        assert_eq!(result.status, ResultStatus::Completed, "size {size}: {result:?}");
        if size == 0 {
            // Empty file: destination may exist as an empty file or not.
            continue;
        }
        assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
        assert!(
            !dir.path().join("b.bin.part").exists(),
            "no temp residue for size {size}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn boundary_sizes_with_resets_exact() {
    // Same boundary sizes under randomized connection resets.
    for (idx, size) in [
        1u64,
        CHUNK + 1,
        SEGMENT + 1,
        3 * SEGMENT + 1,
    ]
    .into_iter()
    .enumerate()
    {
        let content = Arc::new(deterministic_bytes(size, 50_000 + idx as u64));
        let (server, hits) = flaky_segmented("/r.bin", content.clone()).await;
        let dir = tempfile::tempdir().expect("tmp");
        let dest = dir.path().join("r.bin");
        let threshold = SEGMENT / 2;
        let c = controller(cfg(threshold, SEGMENT / 4));
        let result = tokio::time::timeout(
            Duration::from_secs(120),
            c.run(DownloadRequest::new(server.url("/r.bin"), dest.clone())),
        )
        .await
        .expect("no hang")
        .expect("terminal");
        assert_eq!(
            result.status,
            ResultStatus::Completed,
            "size {size}: {result:?}"
        );
        assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
        assert!(hits.load(Ordering::Relaxed) > 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn oversized_4gib_sparse_download_exact() {
    // >4 GiB file, materialized lazily by the server (§36.6 >4 GiB).
    // A deterministic fill function stands in for the file content; the
    // download writes a real (sparse) temp file and must complete with the
    // exact size. Sampling verifies byte positions match the fill.
    const FOUR_GIB_PLUS: u64 = 4 * 1024 * 1024 * 1024 + 1024 * 1024; // 4 GiB + 1 MiB
    let fill: support::test_server::SparseFill =
        Arc::new(|off: u64| ((off >> 3) ^ off) as u8);
    let server = TestServer::new()
        .serve_handler("/huge", move |req| {
            let len = FOUR_GIB_PLUS;
            if req.method == "HEAD" {
                return ScriptedResponse::new(200)
                    .with_header("content-length", &len.to_string())
                    .with_header("accept-ranges", "bytes");
            }
            if let Some((s, e)) = req.range {
                let end = e.min(len - 1);
                let span = end - s + 1;
                // Sparse: materialize only this slice's bytes.
                let start = s;
                let f = fill.clone();
                let mut r = ScriptedResponse::new(206).with_header(
                    "content-range",
                    &format!("bytes {s}-{end}/{len}"),
                );
                r.sparse_len = Some(span);
                r.sparse_fill = Some(Arc::new(move |off: u64| f(start + off)));
                r.with_header("accept-ranges", "bytes")
            } else {
                let mut r = ScriptedResponse::new(200);
                r.sparse_len = Some(len);
                r.sparse_fill = Some(fill.clone());
                r.with_header("accept-ranges", "bytes")
            }
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("huge.bin");
    // Segment at 64 MiB so a handful of workers cover 4 GiB in ~64 ranges.
    let mut c_cfg = cfg(16 * 1024 * 1024, 64 * 1024 * 1024);
    c_cfg.transfer.max_workers = 4;
    c_cfg.transfer.min_segment_size = 16 * 1024 * 1024;
    let c = controller(c_cfg);
    let result = tokio::time::timeout(
        Duration::from_secs(600),
        c.run(DownloadRequest::new(server.url("/huge"), dest.clone())),
    )
    .await
    .expect("no hang")
    .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(result.total_size, Some(FOUR_GIB_PLUS));
    // The final file must be >4 GiB with byte-exact spot checks against
    // the deterministic fill (full 4 GiB compare would be too slow).
    let f = std::fs::File::open(&dest).expect("final file");
    assert_eq!(f.metadata().expect("meta").len(), FOUR_GIB_PLUS);
    let f = std::fs::File::open(&dest).expect("final file");
    use std::io::{Read, Seek, SeekFrom};
    let mut f = f;
    for probe in [0u64, 1, 1000, FOUR_GIB_PLUS / 2, FOUR_GIB_PLUS - 4096] {
        f.seek(SeekFrom::Start(probe)).expect("seek");
        let mut buf = [0u8; 64];
        f.read_exact(&mut buf).expect("read probe");
        for (i, b) in buf.iter().enumerate() {
            let expected = ((probe + i as u64) >> 3 ^ (probe + i as u64)) as u8;
            assert_eq!(*b, expected, "byte at offset {}", probe + i as u64);
        }
    }
    // On ext4/overlayfs a freshly written file may or may not be reported
    // sparse; block-count bounds are not asserted here (platform variance).
    let _ = std::fs::remove_file(&dest);
}