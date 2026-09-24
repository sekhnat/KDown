//! Process-isolated fixture-server verification (task 1.4).
//!
//! The fixture server runs as its own process (`CARGO_BIN_EXE_fixture_server`),
//! so client-side resource measurement in the bench is client-only. These
//! tests verify generator parity with the in-process fixture and exercise the
//! controlled server behaviors (ranges, transient failures, resets, validator
//! changes, throttling) over real HTTP.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kdown_engine::config::EngineConfig;
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::job::controller::{
    DownloadRequest, ResultStatus, SingleStreamController,
};

mod support;
use support::fixtures;

struct IsolatedServer {
    child: Child,
    addr: String,
    sha256: String,
    size: u64,
}

impl Drop for IsolatedServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Spawn `fixture_server` and read its startup protocol.
fn spawn_server(args: &[&str]) -> IsolatedServer {
    let bin = env!("CARGO_BIN_EXE_fixture_server");
    let mut child = Command::new(bin)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn fixture_server");
    let stdout = child.stdout.take().expect("stdout");
    let mut addr = None;
    let mut sha = None;
    let mut size = None;
    let started = Instant::now();
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        if let Some(a) = line.strip_prefix("LISTENING ") {
            addr = Some(a.to_string());
        } else if let Some(s) = line.strip_prefix("SIZE ") {
            size = Some(s.parse().expect("size"));
        } else if let Some(h) = line.strip_prefix("SHA256 ") {
            sha = Some(h.to_string());
        } else if line == "READY" {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "fixture_server did not report READY in time"
        );
    }
    IsolatedServer {
        child,
        addr: addr.expect("LISTENING line"),
        sha256: sha.expect("SHA256 line"),
        size: size.expect("SIZE line"),
    }
}

/// Download from `url` with the given config overrides. Returns the result
/// and the surviving destination directory (the published file is hashed by
/// the caller, so the directory must outlive the assertion).
async fn download(
    url: &str,
    configure: impl FnOnce(&mut EngineConfig),
) -> (
    kdown_engine::job::controller::DownloadResult,
    tempfile::TempDir,
) {
    let mut cfg = EngineConfig::default();
    cfg.network.response_header_timeout = Duration::from_secs(30);
    cfg.network.read_idle_timeout = Duration::from_secs(30);
    configure(&mut cfg);
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg);
    let dir = tempfile::tempdir().expect("tmpdir");
    let result = controller
        .run(DownloadRequest::new(url, dir.path().join("out.bin")))
        .await
        .expect("download completes");
    (result, dir)
}

/// Generator parity + client download parity for a deterministic file:
/// the isolated server's declared digest must equal the in-process fixture
/// digest, and a real download must reproduce both.
#[tokio::test]
async fn isolated_server_matches_in_process_fixture() {
    let size = 1024u64 * 1024; // 1 MiB deterministic file
    let seed = 4242u64;
    let server = spawn_server(&["--size", "1MiB", "--seed", "4242"]);
    assert_eq!(server.size, size);

    // Generator parity: the standalone server's block-derived content must
    // hash identically to the shared synthetic fixture generator.
    let in_process_hash = fixtures::synthetic_expected_sha256(size, seed);
    assert_eq!(server.sha256, in_process_hash, "generator parity");
    // The whole file is reproducible block-wise in-process.
    let mut in_process = Vec::with_capacity(size as usize);
    for i in 0..size / fixtures::SYNTHETIC_BLOCK as u64 {
        in_process.extend_from_slice(&fixtures::synthetic_block_bytes(i, seed));
    }
    assert_eq!(
        fixtures::sha256_hex(&in_process),
        server.sha256,
        "full-file parity"
    );

    // Client download parity: single-stream download of the isolated file
    // reproduces the same digest.
    let url = format!("http://{}/f.bin", server.addr);
    let (result, _dir) = download(&url, |_| {}).await;
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    assert_eq!(result.bytes_reused_from_checkpoint, 0);
    let path = result.final_path.clone().expect("published");
    assert_eq!(
        fixtures::file_sha256(&path),
        server.sha256,
        "isolated download parity"
    );
}

/// Segmented download from the isolated server (range requests over real
/// HTTP) reproduces the same deterministic file.
#[tokio::test]
async fn isolated_server_segmented_download_matches() {
    let server = spawn_server(&["--size", "1MiB", "--seed", "4242"]);
    let url = format!("http://{}/f.bin", server.addr);
    let (result, _dir) = download(&url, |cfg| {
        cfg.transfer.segmentation_threshold = 1;
        cfg.transfer.max_workers = 4;
        cfg.transfer.initial_segment_size = 256 * 1024;
        cfg.transfer.min_segment_size = 64 * 1024;
        cfg.transfer.max_segment_size = 256 * 1024;
    })
    .await;
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256);
}

/// Transient 503s on the first range requests recover through the engine's
/// retry path (server behavior + client resilience over real HTTP).
#[tokio::test]
async fn isolated_transient_failures_recover_via_retry() {
    let server = spawn_server(&["--size", "512KiB", "--seed", "7", "--transient-fail", "2:503"]);
    let url = format!("http://{}/f.bin", server.addr);
    let (result, _dir) = download(&url, |cfg| {
        cfg.transfer.segmentation_threshold = 1;
        cfg.transfer.max_workers = 2;
        cfg.transfer.initial_segment_size = 128 * 1024;
        cfg.transfer.min_segment_size = 64 * 1024;
        cfg.transfer.max_segment_size = 128 * 1024;
        cfg.retry.base_delay = Duration::from_millis(10);
    })
    .await;
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    // The probe's validating bytes=0-0 GET may consume one injected failure;
    // at least one worker-visible retry must have happened and the job must
    // recover with the exact file.
    assert!(result.retries >= 1, "expected injected failures to be retried, retries={}", result.retries);
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256);
}

/// A mid-response connection reset recovers through retry (the reset kills
/// the keep-alive connection; the engine re-issues the range).
#[tokio::test]
async fn isolated_mid_transfer_reset_recovers() {
    // Reset each response after 32 KiB; with 128 KiB segments each range
    // needs several resets before one succeeds.
    let server = spawn_server(&[
        "--size",
        "128KiB",
        "--seed",
        "9",
        "--reset-after-bytes",
        "32768",
    ]);
    let url = format!("http://{}/f.bin", server.addr);
    let (result, _dir) = download(&url, |cfg| {
        cfg.transfer.segmentation_threshold = 1;
        cfg.transfer.max_workers = 1;
        cfg.retry.base_delay = Duration::from_millis(10);
    })
    .await;
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256);
}

/// The validator flip is observable over real HTTP: after the configured
/// number of responses the ETag changes (resource-change scenarios).
#[tokio::test]
async fn isolated_validator_flip_changes_etag() {
    let server = spawn_server(&["--size", "1KiB", "--seed", "3", "--change-etag-after", "1"]);
    use std::io::Write as _;
    let mut conn = std::net::TcpStream::connect(server.addr.clone()).expect("connect");
    conn.write_all(b"GET /f.bin HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .expect("req1");
    let mut response = Vec::new();
    std::io::Read::read_to_end(&mut conn, &mut response).expect("resp1");
    let head = String::from_utf8_lossy(&response);
    assert!(head.contains("etag: iso\r\n"), "first etag: {head}");

    let mut conn = std::net::TcpStream::connect(server.addr.clone()).expect("connect 2");
    conn.write_all(b"GET /f.bin HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .expect("req2");
    let mut response = Vec::new();
    std::io::Read::read_to_end(&mut conn, &mut response).expect("resp2");
    let head = String::from_utf8_lossy(&response);
    assert!(head.contains("etag: iso-v2\r\n"), "flipped etag: {head}");
}

/// The throttle paces the response body (server-side behavior smoke).
#[tokio::test]
async fn isolated_throttle_paces_body() {
    let server = spawn_server(&["--size", "1MiB", "--seed", "5", "--throttle-mib-s", "1"]);
    let started = Instant::now();
    let url = format!("http://{}/f.bin", server.addr);
    let (result, _dir) = download(&url, |_| {}).await;
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    // 1 MiB at 1 MiB/s must take at least ~0.9 s of body time.
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "throttled 1 MiB finished too fast: {:?}",
        started.elapsed()
    );
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256);
}

/// Ignoring ranges: a Range request receives a full 200 response (raw
/// server-behavior check; engine semantics are covered by the in-process
/// misbehaving-server suite).
#[tokio::test]
async fn isolated_ignore_ranges_serves_full_200() {
    let server = spawn_server(&["--size", "4KiB", "--seed", "11", "--ignore-ranges"]);
    use std::io::Write as _;
    let mut conn = std::net::TcpStream::connect(server.addr.clone()).expect("connect");
    conn.write_all(b"GET /f.bin HTTP/1.1\r\nHost: x\r\nRange: bytes=0-1\r\nConnection: close\r\n\r\n")
        .expect("req");
    let mut response = Vec::new();
    std::io::Read::read_to_end(&mut conn, &mut response).expect("resp");
    let head = String::from_utf8_lossy(&response).to_string();
    assert!(head.starts_with("HTTP/1.1 200 OK"), "head: {head}");
    // Body carries the whole file (4 KiB), not the 2-byte range.
    let body_len = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| response.len() - (i + 4))
        .expect("header terminator");
    assert_eq!(body_len, 4096);
}

/// The FailIfExists pre-check still rejects existing destinations with the
/// isolated URL form (client-side policy unaffected by server isolation).
#[tokio::test]
async fn isolated_client_fail_if_exists_unchanged() {
    let server = spawn_server(&["--size", "1KiB", "--seed", "13"]);
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    std::fs::write(&dest, b"existing").expect("seed destination");

    let mut cfg = EngineConfig::default();
    cfg.network.response_header_timeout = Duration::from_secs(30);
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg);
    let result = controller
        .run(DownloadRequest::new(
            format!("http://{}/f.bin", server.addr),
            dest.clone(),
        ))
        .await
        .expect("run result");
    assert_eq!(result.status, ResultStatus::Failed);
    assert!(result.bytes_downloaded_from_network == 0);
    assert_eq!(std::fs::read(&dest).expect("read"), b"existing");
}
