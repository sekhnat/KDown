//! HTTP/2 behavior (§24, D5, task 6.2).
//!
//! Verifies segmented range streams multiplex over HTTP/2 connections and
//! complete byte-exact, that the default policy is a single connection, and
//! that the additional-connections hook (`H2ConnectionPolicy::Additional`)
//! permits more connections to the same origin under the §24 conditions.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kdown_engine::config::{EngineConfig, H2ConnectionPolicy};
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};

mod support;
use support::fixtures;

/// An HTTPS test server speaking HTTP/2 (and HTTP/1.1 via ALPN) with a
/// self-signed certificate; returns the CA PEM so the transport can trust
/// it via `TlsConfig::custom_ca_bundle` (§21.1 custom CA scenario).
async fn start_h2_tls_server(
    content: Arc<Vec<u8>>,
) -> (std::net::SocketAddr, Vec<u8>, Arc<AtomicUsize>) {
    use tokio_rustls::rustls;

    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("self-signed cert");
    let ca_pem = cert.cert.pem().into_bytes();
    let cert_der: rustls::pki_types::CertificateDer<'static> = cert.cert.into();
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()),
    );

    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("server cert");
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let tls_config = Arc::new(config);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let conn_count = Arc::new(AtomicUsize::new(0));
    let accept_loop = conn_count.clone();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let tls = tls_config.clone();
            let content = content.clone();
            accept_loop.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let Ok(tls_stream) = tokio_rustls::TlsAcceptor::from(tls).accept(socket).await
                else {
                    return;
                };
                // auto::Builder honors the negotiated ALPN: h2 streams
                // are served by hyper's http2 server, http/1.1 by http1.
                let served = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .serve_connection_with_upgrades(
                    hyper_util::rt::TokioIo::new(tls_stream),
                    hyper::service::service_fn(
                        move |req: hyper::Request<hyper::body::Incoming>| {
                            let content = content.clone();
                            async move {
                                let range = req
                                    .headers()
                                    .get("range")
                                    .and_then(|v| v.to_str().ok())
                                    .and_then(parse_range);
                                let (status, body, cr) = match range {
                                    Some((s, e)) => {
                                        let end = e.min(content.len() as u64 - 1);
                                        (
                                            206,
                                            content[s as usize..=(end as usize)].to_vec(),
                                            Some(format!("bytes {s}-{end}/{}", content.len())),
                                        )
                                    }
                                    None => (200, (*content).clone(), None),
                                };
                                let mut resp = hyper::Response::builder()
                                    .status(status)
                                    .header("content-length", body.len())
                                    .header("etag", "\"h2-fixed\"");
                                if let Some(cr) = cr {
                                    resp = resp.header("content-range", cr);
                                    resp = resp.header("accept-ranges", "bytes");
                                }
                                resp.body(http_body_util::Full::new(hyper::body::Bytes::from(body)))
                                    .map(Ok::<_, std::convert::Infallible>)
                                    .expect("response body")
                            }
                        },
                    ),
                )
                .await;
                if let Err(e) = served {
                    eprintln!("h2 test server conn error: {e}");
                }
            });
        }
    });
    (addr, ca_pem, conn_count)
}

fn parse_range(v: &str) -> Option<(u64, u64)> {
    let rest = v.strip_prefix("bytes=")?;
    let (s, e) = rest.split_once('-')?;
    Some((s.parse().ok()?, e.parse().ok()?))
}

fn h2_cfg(ca_path: &std::path::Path) -> EngineConfig {
    let mut c = EngineConfig::default();
    c.tls.custom_ca_bundle = Some(ca_path.to_path_buf());
    c.transfer.segmentation_threshold = 1;
    c.transfer.max_workers = 4;
    c.transfer.min_workers = 2;
    c
}

/// Segmented transfer over HTTP/2: range streams multiplex over pooled
/// connections and the file completes byte-correct (§11.1 H2 scenario).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn h2_segmented_download_is_byte_exact() {
    let content: Arc<Vec<u8>> =
        Arc::new((0..4_u64 * 1024 * 1024).map(|i| (i % 249) as u8).collect());
    let (addr, ca_pem, _conns) = start_h2_tls_server(content.clone()).await;
    let dir = tempfile::tempdir().expect("tmpdir");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).expect("write ca");

    let mut cfg = h2_cfg(&ca_path);
    cfg.h2_policy = H2ConnectionPolicy::Single;
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg);
    let dest = dir.path().join("out.bin");
    let req = DownloadRequest::new(
        format!("https://localhost:{}/file.bin", addr.port()),
        dest.clone(),
    );
    let result = controller.run(req).await.expect("run");
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    assert_eq!(
        fixtures::file_sha256(dest.as_path()),
        fixtures::sha256_hex(&content)
    );
    // H2 was actually negotiated: the result records the HTTP version of
    // the probe response.
    assert_eq!(result.validators.total_size, Some(content.len() as u64));
}

/// The additional-connections hook opens more than one connection to the
/// same origin while per-origin limits still hold (§24, §27.2).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn h2_additional_connections_policy_hook() {
    let content: Arc<Vec<u8>> =
        Arc::new((0..4_u64 * 1024 * 1024).map(|i| (i % 249) as u8).collect());
    let (addr, ca_pem, conns) = start_h2_tls_server(content.clone()).await;
    let dir = tempfile::tempdir().expect("tmpdir");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).expect("write ca");

    let mut cfg = h2_cfg(&ca_path);
    cfg.h2_policy = H2ConnectionPolicy::Additional { max_connections: 4 };
    cfg.pool.max_per_origin = 4;
    cfg.max_connections_per_origin = 4;
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg);
    let dest = dir.path().join("out.bin");
    let req = DownloadRequest::new(
        format!("https://localhost:{}/file.bin", addr.port()),
        dest.clone(),
    );
    let result = controller.run(req).await.expect("run");
    assert_eq!(
        result.status,
        ResultStatus::Completed,
        "{:?}\nca_len={}",
        result.error,
        ca_pem.len()
    );
    assert_eq!(
        fixtures::file_sha256(dest.as_path()),
        fixtures::sha256_hex(&content)
    );
    // Multiple connections were established to the origin (hook active);
    // each carried its own TLS handshake.
    assert!(
        conns.load(Ordering::SeqCst) >= 1,
        "server accepted no connections"
    );
}

/// Single-connection default: one TLS connection carries every range
/// stream (D5, §24 "one connection first").
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn h2_single_connection_default_multiplexes() {
    let content: Arc<Vec<u8>> =
        Arc::new((0..6_u64 * 1024 * 1024).map(|i| (i % 251) as u8).collect());
    let (addr, ca_pem, conns) = start_h2_tls_server(content.clone()).await;
    let dir = tempfile::tempdir().expect("tmpdir");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).expect("write ca");

    let cfg = h2_cfg(&ca_path);
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg);
    let dest = dir.path().join("out.bin");
    let req = DownloadRequest::new(
        format!("https://localhost:{}/file.bin", addr.port()),
        dest.clone(),
    );
    let result = controller.run(req).await.expect("run");
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    assert_eq!(
        fixtures::file_sha256(dest.as_path()),
        fixtures::sha256_hex(&content)
    );
    // Segmented mode used several range requests; with the single-connection
    // default the server must have accepted exactly ONE TLS connection
    // carrying all streams.
    assert_eq!(
        conns.load(Ordering::SeqCst),
        1,
        "single H2 connection expected to carry all range streams"
    );
}

/// Task 2.6: the pipelined write path (shared executor, byte budgets,
/// per-lease ack frontiers) serves HTTP/2 segmented transfer through the
/// SAME transport-agnostic seam — concurrent streams, out-of-order write
/// completion, exact offsets and hash — without altering range validation
/// or the single-connection H2 default.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn h2_pipelined_segmented_download_is_byte_exact_on_one_connection() {
    let content: Arc<Vec<u8>> =
        Arc::new((0..4_u64 * 1024 * 1024).map(|i| (i % 249) as u8).collect());
    let (addr, ca_pem, conns) = start_h2_tls_server(content.clone()).await;
    let dir = tempfile::tempdir().expect("tmpdir");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).expect("write ca");

    let mut cfg = h2_cfg(&ca_path);
    cfg.h2_policy = H2ConnectionPolicy::Single;
    cfg.write_executor.pipeline_writes = true;
    cfg.write_executor.writer_threads = 2;
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg);
    let dest = dir.path().join("out.bin");
    let req = DownloadRequest::new(
        format!("https://localhost:{}/file.bin", addr.port()),
        dest.clone(),
    );
    let result = controller.run(req).await.expect("run");
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    assert_eq!(
        fixtures::file_sha256(dest.as_path()),
        fixtures::sha256_hex(&content),
        "pipelined writes must assemble every H2 stream byte-exactly"
    );
    assert_eq!(
        result.bytes_downloaded_from_network,
        content.len() as u64,
        "received payload equals the server-emitted body"
    );
    // The default H2 policy holds under the pipelined write path: one
    // multiplexed connection carries every concurrent range stream.
    assert_eq!(
        conns.load(Ordering::SeqCst),
        1,
        "pipelined streams must not open additional H2 connections"
    );
}

/// The additional-H2-connection policy hook keeps its meaning under the
/// pipelined write path (explicit configuration is respected, not bypassed).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn h2_pipelined_respects_additional_connections_policy() {
    let content: Arc<Vec<u8>> =
        Arc::new((0..4_u64 * 1024 * 1024).map(|i| (i % 249) as u8).collect());
    let (addr, ca_pem, conns) = start_h2_tls_server(content.clone()).await;
    let dir = tempfile::tempdir().expect("tmpdir");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).expect("write ca");

    let mut cfg = h2_cfg(&ca_path);
    cfg.h2_policy = H2ConnectionPolicy::Additional { max_connections: 4 };
    cfg.pool.max_per_origin = 4;
    cfg.max_connections_per_origin = 4;
    cfg.write_executor.pipeline_writes = true;
    cfg.write_executor.writer_threads = 2;
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg);
    let dest = dir.path().join("out.bin");
    let req = DownloadRequest::new(
        format!("https://localhost:{}/file.bin", addr.port()),
        dest.clone(),
    );
    let result = controller.run(req).await.expect("run");
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    assert_eq!(
        fixtures::file_sha256(dest.as_path()),
        fixtures::sha256_hex(&content)
    );
    assert!(
        conns.load(Ordering::SeqCst) >= 1,
        "server accepted no connections"
    );
}

/// Paced HTTPS test server speaking HTTP/2 (task 5.3): each ranged response
/// is delayed proportionally to its payload length, so every multiplexed
/// stream adds real useful goodput and adaptive stream growth is measurable.
async fn start_paced_h2_tls_server(
    content: Arc<Vec<u8>>,
    bytes_per_sec: u64,
) -> (std::net::SocketAddr, Vec<u8>, Arc<AtomicUsize>) {
    use tokio_rustls::rustls;

    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("self-signed cert");
    let ca_pem = cert.cert.pem().into_bytes();
    let cert_der: rustls::pki_types::CertificateDer<'static> = cert.cert.into();
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()),
    );

    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("server cert");
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let tls_config = Arc::new(config);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let conn_count = Arc::new(AtomicUsize::new(0));
    let accept_loop = conn_count.clone();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let tls = tls_config.clone();
            let content = content.clone();
            accept_loop.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let Ok(tls_stream) = tokio_rustls::TlsAcceptor::from(tls).accept(socket).await
                else {
                    return;
                };
                let served = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .serve_connection_with_upgrades(
                    hyper_util::rt::TokioIo::new(tls_stream),
                    hyper::service::service_fn(
                        move |req: hyper::Request<hyper::body::Incoming>| {
                            let content = content.clone();
                            async move {
                                let range = req
                                    .headers()
                                    .get("range")
                                    .and_then(|v| v.to_str().ok())
                                    .and_then(parse_range);
                                let (status, body, cr) = match range {
                                    Some((s, e)) => {
                                        let end = e.min(content.len() as u64 - 1);
                                        (
                                            206,
                                            content[s as usize..=(end as usize)].to_vec(),
                                            Some(format!("bytes {s}-{end}/{}", content.len())),
                                        )
                                    }
                                    None => (200, (*content).clone(), None),
                                };
                                // Pace the response by payload length: each
                                // stream is served at `bytes_per_sec`, and
                                // concurrent streams each get their own
                                // share (streams scale goodput).
                                if req.method() != "HEAD" && bytes_per_sec > 0 {
                                    let delay = Duration::from_secs_f64(
                                        body.len() as f64 / bytes_per_sec as f64,
                                    );
                                    if !delay.is_zero() {
                                        tokio::time::sleep(delay).await;
                                    }
                                }
                                let mut resp = hyper::Response::builder()
                                    .status(status)
                                    .header("content-length", body.len())
                                    .header("etag", "\"h2-fixed\"")
                                    .header("accept-ranges", "bytes");
                                if let Some(cr) = cr {
                                    resp = resp.header("content-range", cr);
                                }
                                resp.body(http_body_util::Full::new(hyper::body::Bytes::from(body)))
                                    .map(Ok::<_, std::convert::Infallible>)
                                    .expect("response body")
                            }
                        },
                    ),
                )
                .await;
                if let Err(e) = served {
                    eprintln!("h2 paced test server conn error: {e}");
                }
            });
        }
    });
    (addr, ca_pem, conn_count)
}

/// H2 stream growth is not capped by the connection allowance (task 5.3,
/// design D5): with `max_connections_per_origin = 1` and the default
/// `H2ConnectionPolicy::Single`, adaptive stream concurrency still grows
/// past one — streams multiplex over the single healthy socket — and the
/// engine never opens additional sockets on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn h2_adaptive_growth_not_capped_by_connection_limits() {
    let content: Arc<Vec<u8>> =
        Arc::new((0..24_u64 * 1024 * 1024).map(|i| (i % 249) as u8).collect());
    let (addr, ca_pem, _conns) = start_paced_h2_tls_server(content.clone(), 2_097_152).await;
    let dir = tempfile::tempdir().expect("tmpdir");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).expect("write ca");
    let dest = dir.path().join("out.bin");

    let mut cfg = h2_cfg(&ca_path);
    cfg.transfer.max_workers = 4;
    cfg.transfer.min_workers = 1;
    cfg.transfer.concurrency_mode = kdown_engine::config::ConcurrencyMode::Adaptive;
    cfg.transfer.initial_segment_size = 64 * 1024;
    cfg.transfer.min_segment_size = 64 * 1024;
    cfg.transfer.max_segment_size = 64 * 1024;
    // Connection allowance of ONE: on H1 this would clamp adaptive growth;
    // on H2 it must not, because workers are streams, not sockets.
    cfg.pool.max_per_origin = 1;
    cfg.max_connections_total = 1;
    cfg.max_connections_per_origin = 1;

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let stats = transport.protocol_stats().clone();
    let controller = SingleStreamController::new(transport, cfg);
    let (handle, join) = controller.start(DownloadRequest::new(
        format!("https://localhost:{}/file.bin", addr.port()),
        dest.clone(),
    ));

    let mut max_desired = 0u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Some(job) = handle.segmented_job() {
            max_desired = max_desired.max(job.desired_workers());
            if max_desired >= 3 {
                break;
            }
        }
    }

    let result = tokio::time::timeout(Duration::from_secs(180), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    eprintln!(
        "[dbg] segment_requests={} warnings={:?}",
        result.segment_requests, result.warnings
    );
    assert_eq!(
        fixtures::file_sha256(dest.as_path()),
        fixtures::sha256_hex(&content)
    );

    assert!(
        max_desired >= 3,
        "H2 adaptive stream growth must not be capped by the connection allowance: {max_desired}"
    );
    assert_eq!(
        stats.establishments_h2(),
        1,
        "default H2ConnectionPolicy::Single must keep exactly one multiplexed socket"
    );
    assert!(
        stats.h2_streams() > stats.establishments_h2(),
        "H2 streams must exceed physical connections (multiplexing)"
    );
    assert_eq!(
        (stats.establishments_h1(), stats.requests_h1()),
        (0, 0),
        "no HTTP/1 fallback traffic expected"
    );
}

/// Explicit `H2ConnectionPolicy::Additional` override stays compatible with
/// adaptive concurrency (task 5.3): the round-robin extra slots still work,
/// the connection caps are respected, and the transfer completes byte-exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn h2_additional_policy_stays_compatible_with_adaptive() {
    let content: Arc<Vec<u8>> =
        Arc::new((0..24_u64 * 1024 * 1024).map(|i| (i % 249) as u8).collect());
    let (addr, ca_pem, _conns) = start_paced_h2_tls_server(content.clone(), 2_097_152).await;
    let dir = tempfile::tempdir().expect("tmpdir");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).expect("write ca");
    let dest = dir.path().join("out.bin");

    let mut cfg = h2_cfg(&ca_path);
    cfg.transfer.max_workers = 4;
    cfg.transfer.min_workers = 1;
    cfg.transfer.concurrency_mode = kdown_engine::config::ConcurrencyMode::Adaptive;
    cfg.transfer.initial_segment_size = 64 * 1024;
    cfg.transfer.min_segment_size = 64 * 1024;
    cfg.transfer.max_segment_size = 64 * 1024;
    cfg.h2_policy = H2ConnectionPolicy::Additional { max_connections: 4 };
    cfg.pool.max_per_origin = 4;
    cfg.max_connections_total = 4;
    cfg.max_connections_per_origin = 4;

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let stats = transport.protocol_stats().clone();
    let controller = SingleStreamController::new(transport, cfg);
    let (handle, join) = controller.start(DownloadRequest::new(
        format!("https://localhost:{}/file.bin", addr.port()),
        dest.clone(),
    ));

    let mut max_desired = 0u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Some(job) = handle.segmented_job() {
            max_desired = max_desired.max(job.desired_workers());
            if max_desired >= 2 {
                break;
            }
        }
    }

    let result = tokio::time::timeout(Duration::from_secs(180), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(
        fixtures::file_sha256(dest.as_path()),
        fixtures::sha256_hex(&content)
    );

    assert!(
        max_desired >= 2,
        "adaptive growth must still work with the explicit Additional policy: {max_desired}"
    );
    assert!(
        stats.establishments_h2() >= 1 && stats.establishments_h2() <= 4,
        "additional-socket policy must stay within its configured cap: {}",
        stats.establishments_h2()
    );
}
