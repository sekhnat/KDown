//! HTTP edge cases end-to-end (task 4.2): absent/ignored/mismatched/
//! changing ranges and validators, redirected cache/CDN paths, and
//! authenticated-proxy interplay. Every failure path must leave NO corrupt
//! published output; every success must be byte-exact and verified.

mod support;

use std::sync::Arc;

use kdown_engine::{
    config::{ExpectedHash, HashAlgorithm},
    DownloadController, DownloadRequest, DownloadRunError, EngineConfig, HttpTransport,
};

use support::fixtures;
use support::test_server::{ScriptedResponse, TestServer};

/// A segmented-triggering config so range behavior is exercised.
fn segmented_config() -> EngineConfig {
    let mut cfg = EngineConfig::default();
    cfg.transfer.segmentation_threshold = 16 * 1024;
    cfg.transfer.max_workers = 4;
    cfg.transfer.min_workers = 2;
    cfg
}

fn request(url: String, dest: &std::path::Path) -> DownloadRequest {
    DownloadRequest::new(url, dest.to_path_buf())
}

/// A download that must fail typed and leave NO published output.
async fn run_failing(cfg: EngineConfig, url: String, dest: &std::path::Path) -> DownloadRunError {
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        controller.run(request(url, dest)),
    )
    .await
    .expect("no hang");
    assert!(
        !dest.exists(),
        "a failed download must never publish output at {}",
        dest.display()
    );
    result.expect_err("expected failure")
}

/// A download that must complete byte-exact.
async fn run_completing(cfg: EngineConfig, url: String, dest: &std::path::Path) -> Vec<u8> {
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let completed = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        controller.run(request(url, dest)),
    )
    .await
    .expect("no hang")
    .expect("download completes");
    assert!(completed.final_path.exists());
    std::fs::read(dest).expect("read output")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absent_ranges_download_sequentially_byte_exact() {
    // No `accept-ranges` anywhere: the engine must not attempt segmented
    // transfers; the sequential download completes byte-exact.
    let content = Arc::new(fixtures::deterministic_bytes(256 * 1024, 0x60));
    let served = Arc::clone(&content);
    let server = TestServer::new()
        .serve_handler("/plain", move |_| {
            ScriptedResponse::ok((*served).clone()) // no accept-ranges header
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_completing(segmented_config(), server.url("/plain"), &dest).await;
    fixtures::assert_bytes_exact(&output, &content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ignored_ranges_200_full_never_publishes_corrupt_output() {
    // The server advertises `accept-ranges` but answers range requests
    // with a 200 FULL body: a segmented job sees a protocol violation and
    // must fail typed (or fall back clean) — never publish shifted bytes.
    let content = fixtures::deterministic_bytes(256 * 1024, 0x61);
    let server = TestServer::new()
        .serve_ranges(
            "/lying",
            content.clone(),
            support::test_server::RangeMode::Full200,
        )
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_completing(segmented_config(), server.url("/lying"), &dest).await;
    // Whatever path the engine took (fallback or retry), the published
    // bytes are the true bytes.
    fixtures::assert_bytes_exact(&output, &content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shifted_range_bytes_fail_integrity_with_no_publication() {
    // The server answers a range request with 206 and a CORRECT
    // Content-Range header but WRONG bytes (shifted by one position):
    // with a caller-provided digest the mismatch must fail typed and the
    // corrupted bytes must never be published.
    let content = Arc::new(fixtures::deterministic_bytes(256 * 1024, 0x62));
    let served = Arc::clone(&content);
    let server = TestServer::new()
        .serve_handler("/shifted", move |req| {
            let total = served.len() as u64;
            match req.range {
                Some((s, e)) => {
                    // Advertise the requested range but serve bytes shifted
                    // by +1 (wrap the last byte around).
                    let end = e.min(total - 1);
                    let start = s.min(total - 1);
                    let mut body = served[start as usize..=(end as usize)].to_vec();
                    body.rotate_left(1);
                    ScriptedResponse::new(206)
                        .with_body(body)
                        .with_header("content-range", &format!("bytes {s}-{end}/{total}"))
                        .with_header("accept-ranges", "bytes")
                }
                None => {
                    ScriptedResponse::ok((*served).clone()).with_header("accept-ranges", "bytes")
                }
            }
        })
        .start()
        .await
        .expect("start");
    let cfg = segmented_config();
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    // The caller pins the expected digest: a shifted payload cannot match.
    let mut req = request(server.url("/shifted"), &dest);
    req.integrity.expected_hashes = vec![ExpectedHash {
        algorithm: HashAlgorithm::Sha256,
        hex: fixtures::sha256_hex(&content),
    }];
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let result = tokio::time::timeout(std::time::Duration::from_secs(60), controller.run(req))
        .await
        .expect("no hang");
    assert!(
        !dest.exists(),
        "a failed download must never publish output at {}",
        dest.display()
    );
    let error = result.expect_err("expected failure");
    let message = format!("{error:?}");
    assert!(
        message.contains("Integrity")
            || message.contains("integrity")
            || message.contains("mismatch"),
        "the shifted bytes must fail integrity verification: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn etag_change_between_probe_and_transfer_is_typed_no_publication() {
    // The ETag flips after the probe: the transfer gate must reject with
    // ResourceChanged and no output may be published.
    let content = fixtures::deterministic_bytes(64 * 1024, 0x63);
    let flipped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let server = TestServer::new()
        .serve_handler("/flipping", move |req| {
            let n = flipped.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let etag = if n == 0 { "\"gen-1\"" } else { "\"gen-2\"" };
            let total = content.len() as u64;
            match req.range {
                Some((s, e)) => {
                    let end = e.min(total - 1);
                    ScriptedResponse::new(206)
                        .with_body(content[s as usize..=(end as usize)].to_vec())
                        .with_header("content-range", &format!("bytes {s}-{end}/{total}"))
                        .with_header("etag", etag)
                        .with_header("accept-ranges", "bytes")
                }
                None => ScriptedResponse::ok(content.clone())
                    .with_header("etag", etag)
                    .with_header("accept-ranges", "bytes"),
            }
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let error = run_failing(segmented_config(), server.url("/flipping"), &dest).await;
    let message = format!("{error:?}");
    assert!(
        message.contains("ResourceChanged") || message.contains("resource"),
        "a mid-transfer generation change must be typed ResourceChanged: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redirect_chain_to_cdn_serves_byte_exact_output() {
    // Probe on the origin redirects to a CDN path; the engine follows the
    // chain and the published output is byte-exact.
    let content = fixtures::deterministic_bytes(256 * 1024, 0x64);
    let server = TestServer::new()
        .serve_handler("/origin", move |_| {
            ScriptedResponse::new(302).with_header("location", "/cdn/file.bin")
        })
        .serve_static("/cdn/file.bin", content.clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_completing(segmented_config(), server.url("/origin"), &dest).await;
    fixtures::assert_bytes_exact(&output, &content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redirect_loop_fails_bounded_with_no_publication() {
    // A self-redirecting path must trip the redirect budget: a typed
    // failure with no published output (never an infinite loop).
    let server = TestServer::new()
        .serve_handler("/loop", move |_| {
            ScriptedResponse::new(302).with_header("location", "/loop")
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let error = run_failing(EngineConfig::default(), server.url("/loop"), &dest).await;
    let message = format!("{error:?}");
    assert!(
        message.contains("Redirect") || message.contains("redirect"),
        "a redirect loop must be typed: {message}"
    );
}
