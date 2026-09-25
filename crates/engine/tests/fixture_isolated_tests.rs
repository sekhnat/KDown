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
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};

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
    let server = spawn_server(&[
        "--size",
        "512KiB",
        "--seed",
        "7",
        "--transient-fail",
        "2:503",
    ]);
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
    assert!(
        result.retries >= 1,
        "expected injected failures to be retried, retries={}",
        result.retries
    );
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
    conn.write_all(
        b"GET /f.bin HTTP/1.1\r\nHost: x\r\nRange: bytes=0-1\r\nConnection: close\r\n\r\n",
    )
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

// ---- TLS / H2 isolated coverage (optimize-transfer-engine-v2 task 0.2) ----

/// Spawn the fixture server in TLS mode with a fresh self-signed certificate
/// (rcgen in the test process; the server binary only needs rustls). Returns
/// the server, the CA PEM path (trust bundle for the client) and the cert
/// tempdir that must outlive the server.
fn spawn_tls_server(args: &[&str]) -> (IsolatedServer, std::path::PathBuf, tempfile::TempDir) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    let dir = tempfile::tempdir().expect("cert tmpdir");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).expect("write cert pem");
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).expect("write key pem");
    let mut all: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    all.push("--tls-cert".into());
    all.push(cert_path.to_string_lossy().into_owned());
    all.push("--tls-key".into());
    all.push(key_path.to_string_lossy().into_owned());
    let refs: Vec<&str> = all.iter().map(String::as_str).collect();
    (spawn_server(&refs), cert_path, dir)
}

/// HTTPS URL for the TLS server (cert SAN is `localhost`; the listener is on
/// 127.0.0.1, matching the h2_tests pattern).
fn tls_url(server: &IsolatedServer, path: &str) -> String {
    let port = server.addr.rsplit(':').next().expect("port");
    format!("https://localhost:{port}{path}")
}

fn configure_tls(cfg: &mut EngineConfig, ca: &std::path::Path) {
    cfg.tls.custom_ca_bundle = Some(ca.to_path_buf());
    cfg.network.response_header_timeout = Duration::from_secs(30);
    cfg.network.read_idle_timeout = Duration::from_secs(30);
}

/// Read the fixture server's `/__stats` counters through the engine with the
/// same TLS trust configuration as the job under test.
async fn read_tls_stats(cfg: &EngineConfig, server: &IsolatedServer) -> (u64, u64, u64) {
    let dir = tempfile::tempdir().expect("stats tmpdir");
    let transport = HttpTransport::from_config(cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg.clone());
    let result = controller
        .run(DownloadRequest::new(
            tls_url(server, "/__stats"),
            dir.path().join("stats.txt"),
        ))
        .await
        .expect("stats download");
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    let text = std::fs::read_to_string(dir.path().join("stats.txt")).expect("stats text");
    let parse = |key: &str| {
        text.lines()
            .find_map(|l| {
                l.strip_prefix(key)
                    .and_then(|v| v.trim().parse::<u64>().ok())
            })
            .expect(key)
    };
    (parse("emitted="), parse("connections="), parse("requests="))
}

/// Segmented HTTP/2 download over TLS from the isolated server: byte-exact
/// parity, multiple streams multiplexed on the connections the engine chose,
/// and server-emitted payload exactly the file size (no H2 duplication).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_tls_h2_segmented_parity() {
    let (server, ca, _cert_dir) = spawn_tls_server(&["--size", "1MiB", "--seed", "4242"]);
    let url = tls_url(&server, "/f.bin");
    let mut cfg = EngineConfig::default();
    configure_tls(&mut cfg, &ca);
    cfg.transfer.segmentation_threshold = 1;
    cfg.transfer.max_workers = 4;
    cfg.transfer.initial_segment_size = 256 * 1024;
    cfg.transfer.min_segment_size = 64 * 1024;
    cfg.transfer.max_segment_size = 256 * 1024;

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg.clone());
    let dir = tempfile::tempdir().expect("tmpdir");
    let result = controller
        .run(DownloadRequest::new(
            url.clone(),
            dir.path().join("out.bin"),
        ))
        .await
        .expect("download");
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256, "H2 TLS parity");
    assert_eq!(result.bytes_reused_from_checkpoint, 0);

    let (emitted, _conns, requests) = read_tls_stats(&cfg, &server).await;
    assert!(
        requests > 1,
        "segmented H2 must issue multiple requests: {requests}"
    );
    // The validating bytes=0-0 probe contributes <= a few payload bytes;
    // anything approaching 2x would be split-overlap re-delivery.
    assert!(
        emitted >= server.size && emitted <= server.size + 64,
        "H2 segmented coverage must not duplicate payload on the wire \
         (emitted {emitted} for {} bytes)",
        server.size
    );
}

/// Single-stream download over TLS (HTTP/1.1 via ALPN or H2): byte-exact.
#[tokio::test]
async fn isolated_tls_single_stream_parity() {
    let (server, ca, _cert_dir) = spawn_tls_server(&["--size", "512KiB", "--seed", "17"]);
    let url = tls_url(&server, "/f.bin");
    let mut cfg = EngineConfig::default();
    configure_tls(&mut cfg, &ca);
    cfg.transfer.segmentation_threshold = u64::MAX;

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg.clone());
    let dir = tempfile::tempdir().expect("tmpdir");
    let result = controller
        .run(DownloadRequest::new(url, dir.path().join("out.bin")))
        .await
        .expect("download");
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256);
}

/// A range-ignoring server over TLS still triggers the safe single-stream
/// fallback with byte-exact output (fallback semantics preserved on the TLS
/// path, task 0.2).
#[tokio::test]
async fn isolated_tls_ignore_ranges_falls_back() {
    let (server, ca, _cert_dir) =
        spawn_tls_server(&["--size", "256KiB", "--seed", "21", "--ignore-ranges"]);
    let url = tls_url(&server, "/f.bin");
    let mut cfg = EngineConfig::default();
    configure_tls(&mut cfg, &ca);
    cfg.transfer.segmentation_threshold = 1;
    cfg.transfer.max_workers = 4;

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg.clone());
    let dir = tempfile::tempdir().expect("tmpdir");
    let result = controller
        .run(DownloadRequest::new(url, dir.path().join("out.bin")))
        .await
        .expect("download");
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256);

    // Fallback proof: at most one full transfer plus the aborted validating
    // probe's partial body — no per-segment range re-downloads happened.
    let (emitted, _conns, requests) = read_tls_stats(&cfg, &server).await;
    assert!(
        emitted <= 2 * server.size + 64 * 1024,
        "range-ignoring server must downgrade to a single transfer \
         (emitted {emitted} for {} bytes)",
        server.size
    );
    assert!(
        requests <= 8,
        "range-ignoring server must not see per-segment requests: {requests}"
    );
}

/// A mid-response reset over TLS recovers through the retry path (hyper
/// transport semantics, not just the raw-H1 path).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_tls_mid_transfer_reset_recovers() {
    let (server, ca, _cert_dir) = spawn_tls_server(&[
        "--size",
        "128KiB",
        "--seed",
        "9",
        "--reset-after-bytes",
        "32768",
    ]);
    let url = tls_url(&server, "/f.bin");
    let mut cfg = EngineConfig::default();
    configure_tls(&mut cfg, &ca);
    cfg.transfer.segmentation_threshold = 1;
    cfg.transfer.max_workers = 1;
    cfg.retry.base_delay = Duration::from_millis(10);

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg.clone());
    let dir = tempfile::tempdir().expect("tmpdir");
    let result = controller
        .run(DownloadRequest::new(url, dir.path().join("out.bin")))
        .await
        .expect("download");
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    assert!(
        result.retries >= 1,
        "expected reset retries: {}",
        result.retries
    );
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256);
}

// ---- Controlled shaping modes (optimize-transfer-engine-v2 task 0.3) ----

/// `--rtt-ms` adds one RTT before each response's headers: a three-request
/// segmented transfer must take at least three RTTs (calibration smoke).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_rtt_shapes_per_request_latency() {
    let server = spawn_server(&["--size", "96KiB", "--seed", "11", "--rtt-ms", "40"]);
    let url = format!("http://{}/f.bin", server.addr);
    let started = Instant::now();
    let (result, _dir) = download(&url, |cfg| {
        cfg.transfer.segmentation_threshold = 1;
        cfg.transfer.max_workers = 1;
        cfg.transfer.initial_segment_size = 32 * 1024;
        cfg.transfer.min_segment_size = 32 * 1024;
        cfg.transfer.max_segment_size = 32 * 1024;
    })
    .await;
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256);
    // 96 KiB / 32 KiB = 3 segments (+probe) — at least 3 full RTTs.
    assert!(
        started.elapsed() >= Duration::from_millis(120),
        "rtt shaping must add >= 3 RTTs: {:?}",
        started.elapsed()
    );
}

/// `--loss-percent` truncates responses deterministically; the engine
/// recovers through retries with byte-exact output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_connection_loss_recovers_via_retry() {
    let server = spawn_server(&["--size", "256KiB", "--seed", "31", "--loss-percent", "40"]);
    let url = format!("http://{}/f.bin", server.addr);
    let (result, _dir) = download(&url, |cfg| {
        cfg.transfer.segmentation_threshold = 1;
        cfg.transfer.max_workers = 2;
        cfg.transfer.initial_segment_size = 64 * 1024;
        cfg.transfer.min_segment_size = 64 * 1024;
        cfg.transfer.max_segment_size = 64 * 1024;
        cfg.retry.base_delay = Duration::from_millis(10);
    })
    .await;
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    assert!(
        result.retries >= 1,
        "expected loss retries: {}",
        result.retries
    );
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256);
}

/// `--retry-after` controls the transient-fail `Retry-After` header value
/// (task 0.3): observable over raw HTTP, still recovering through retry.
#[tokio::test]
async fn isolated_transient_fail_carries_configured_retry_after() {
    let server = spawn_server(&[
        "--size",
        "64KiB",
        "--seed",
        "3",
        "--transient-fail",
        "1:503",
        "--retry-after",
        "1",
    ]);
    use std::io::Write as _;
    let mut conn = std::net::TcpStream::connect(server.addr.clone()).expect("connect");
    conn.write_all(b"GET /f.bin HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .expect("req");
    let mut response = Vec::new();
    std::io::Read::read_to_end(&mut conn, &mut response).expect("resp");
    let head = String::from_utf8_lossy(&response);
    assert!(
        head.contains("HTTP/1.1 503"),
        "transient status expected: {head}"
    );
    assert!(
        head.contains("retry-after: 1\r\n"),
        "configured Retry-After expected: {head}"
    );

    // The engine still recovers (1s Retry-After is honored/capped by policy).
    let url = format!("http://{}/f.bin", server.addr);
    let (result, _dir) = download(&url, |cfg| {
        cfg.retry.base_delay = Duration::from_millis(10);
    })
    .await;
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256);
}

// ---- Protocol instrumentation reconciliation (task 5.1) ----

/// Server-side counters read over a raw throwaway TCP connection
/// (`Connection: close`), so each read deterministically adds exactly one
/// accepted connection and one served request to the totals it reports.
fn raw_server_stats(addr: &str) -> (u64, u64, u64) {
    use std::io::{Read as _, Write as _};
    let mut conn = std::net::TcpStream::connect(addr).expect("stats connect");
    conn.write_all(b"GET /__stats HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .expect("stats request");
    // The /__stats handler keeps the connection open, so half-close the
    // write side: the server reads EOF, closes, and read_to_end returns.
    conn.shutdown(std::net::Shutdown::Write)
        .expect("stats half-close");
    let mut response = Vec::new();
    conn.read_to_end(&mut response).expect("stats response");
    let text = String::from_utf8_lossy(&response);
    let parse = |key: &str| {
        text.lines()
            .find_map(|l| {
                l.strip_prefix(key)
                    .and_then(|v| v.trim().parse::<u64>().ok())
            })
            .unwrap_or_else(|| panic!("stats key {key} missing: {text}"))
    };
    (parse("emitted="), parse("connections="), parse("requests="))
}

/// H1 reconciliation (task 5.1): the transport's physical-establishment and
/// request counters match the isolated server's accepted connections and
/// served requests exactly (raw stats reads account for their own +1/+1),
/// every establishment/request is labeled HTTP/1, and pooling keeps
/// establishments within the request count. H2 flow-control data stays
/// labeled unavailable instead of fabricated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h1_connection_and_request_counters_reconcile_with_server() {
    let server = spawn_server(&["--size", "1MiB", "--seed", "4242"]);
    let url = format!("http://{}/f.bin", server.addr);
    let mut cfg = EngineConfig::default();
    cfg.network.response_header_timeout = Duration::from_secs(30);
    cfg.network.read_idle_timeout = Duration::from_secs(30);
    cfg.transfer.segmentation_threshold = 1;
    cfg.transfer.max_workers = 4;
    cfg.transfer.initial_segment_size = 256 * 1024;
    cfg.transfer.min_segment_size = 64 * 1024;
    cfg.transfer.max_segment_size = 256 * 1024;

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let stats = transport.protocol_stats().clone();
    let controller = SingleStreamController::new(transport, cfg.clone());

    let (_, conns_before, reqs_before) = raw_server_stats(&server.addr);
    let dir = tempfile::tempdir().expect("tmpdir");
    let result = controller
        .run(DownloadRequest::new(url, dir.path().join("out.bin")))
        .await
        .expect("download");
    let (_, conns_after, reqs_after) = raw_server_stats(&server.addr);

    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256);

    // The after-read itself contributes exactly one connection and one
    // request (Connection: close); the before-read predates the delta.
    let server_conns = conns_after - conns_before - 1;
    let server_requests = reqs_after - reqs_before - 1;
    assert_eq!(
        stats.establishments_h1(),
        server_conns,
        "H1 establishments must reconcile with accepted server connections"
    );
    assert_eq!(
        stats.requests_h1(),
        server_requests,
        "H1 requests must reconcile with served server requests"
    );
    assert_eq!(stats.establishments_total(), stats.establishments_h1());
    assert_eq!(stats.requests_total(), stats.requests_h1());
    assert_eq!(
        (stats.establishments_h2(), stats.h2_streams()),
        (0, 0),
        "plain HTTP must label every connection/request HTTP/1"
    );
    assert!(stats.establishments_h1() >= 1, "at least one connection");
    assert!(
        stats.establishments_h1() <= stats.requests_h1(),
        "keep-alive reuse must not open more connections than requests"
    );
    // Unavailable H2 flow-control instrumentation is labeled, not guessed.
    assert!(
        stats.h2_flow_control_wait().is_none(),
        "flow-control data must stay labeled unavailable (instrumented={})",
        kdown_engine::http::H2_FLOW_CONTROL_INSTRUMENTED
    );
}

/// H2 reconciliation (task 5.1): segmented range requests arrive at the
/// server as multiplexed streams on the single default connection — stream
/// count matches served server requests exactly (engine stats reads add a
/// fixed HEAD+GET pair), physical establishments stay at one under the
/// default `H2ConnectionPolicy::Single`, and no request is labeled HTTP/1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_stream_counters_reconcile_with_server_side_requests() {
    let (server, ca, _cert_dir) = spawn_tls_server(&["--size", "1MiB", "--seed", "4242"]);
    let url = tls_url(&server, "/f.bin");
    let mut cfg = EngineConfig::default();
    configure_tls(&mut cfg, &ca);
    cfg.transfer.segmentation_threshold = 1;
    cfg.transfer.max_workers = 4;
    cfg.transfer.initial_segment_size = 256 * 1024;
    cfg.transfer.min_segment_size = 64 * 1024;
    cfg.transfer.max_segment_size = 256 * 1024;

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let stats = transport.protocol_stats().clone();
    let controller = SingleStreamController::new(transport, cfg.clone());

    let (_, conns_before, reqs_before) = read_tls_stats(&cfg, &server).await;
    let dir = tempfile::tempdir().expect("tmpdir");
    let result = controller
        .run(DownloadRequest::new(url, dir.path().join("out.bin")))
        .await
        .expect("download");
    let (_, conns_after, reqs_after) = read_tls_stats(&cfg, &server).await;

    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    let path = result.final_path.clone().expect("published");
    assert_eq!(fixtures::file_sha256(&path), server.sha256, "H2 parity");

    // Each stats read is one HEAD probe plus one GET on a tiny resource:
    // exactly two served requests, so the after-read adds 2 to the delta.
    let server_requests = reqs_after - reqs_before - 2;
    assert_eq!(
        stats.h2_streams(),
        server_requests,
        "H2 streams must reconcile with served server requests"
    );
    assert!(
        stats.h2_streams() > stats.establishments_h2(),
        "segmented H2 must multiplex streams over one connection"
    );
    // Default Single policy: the job's transport opens exactly one H2
    // connection; the server saw at least that one plus each stats read.
    assert_eq!(
        stats.establishments_h2(),
        1,
        "default H2ConnectionPolicy::Single must use one connection"
    );
    assert!(
        conns_after - conns_before >= stats.establishments_h2(),
        "server must have accepted at least the job's H2 connection"
    );
    assert_eq!(
        (stats.establishments_h1(), stats.requests_h1()),
        (0, 0),
        "TLS H2 transfer must not establish or label HTTP/1 traffic"
    );
    assert!(
        stats.h2_flow_control_wait().is_none(),
        "unavailable H2 flow-control data is labeled unavailable (instrumented={})",
        kdown_engine::http::H2_FLOW_CONTROL_INSTRUMENTED
    );
}
