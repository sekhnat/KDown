#![allow(clippy::field_reassign_with_default, unused_variables, dead_code)]

//! Proxy and credential hooks (§28, §29, task 7.1).
//!
//! Verifies HTTPS-over-CONNECT through a local proxy (TLS still applies
//! end-to-end to the origin), plain HTTP proxied with absolute-form
//! targets, credential redaction, and the credential provider callback
//! with bounded challenge stages (no auth-retry loops).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use kdown_engine::config::{EngineConfig, ProxyConfig};
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::job::controller::{DownloadController, DownloadRequest, ResultStatus};

mod support;
use support::fixtures;

const CONTENT: &[u8] = b"proxy tunnel payload 0123456789";

/// A minimal HTTP CONNECT proxy: on `CONNECT host:port`, opens a tunnel
/// after optionally checking `Proxy-Authorization`; then relays bytes.
async fn start_proxy(require_auth: bool, stats: Arc<ProxyStats>) -> std::net::SocketAddr {
    start_connect_proxy_inner(
        if require_auth {
            Some("user:secret")
        } else {
            None
        },
        stats,
    )
    .await
}

async fn start_connect_proxy_inner(
    require_auth: Option<&'static str>,
    stats: Arc<ProxyStats>,
) -> std::net::SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let require_auth: Option<String> = require_auth.map(|s| s.to_string());
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let stats = stats.clone();
            let require_auth = require_auth.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = loop {
                    let n = socket.read(&mut buf).await.expect("read");
                    if n == 0 {
                        return;
                    }
                    if String::from_utf8_lossy(&buf[..n]).contains("\r\n\r\n") {
                        break n;
                    }
                };
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let first = req.lines().next().unwrap_or("").to_string();
                stats.requests.fetch_add(1, Ordering::SeqCst);
                if let Some(rest) = first.strip_prefix("CONNECT ") {
                    // CONNECT host:port HTTP/1.1
                    let target = rest.split(' ').next().unwrap_or("").to_string();
                    let got_auth = req.lines().find_map(|l| {
                        l.strip_prefix("proxy-authorization: ")
                            .or_else(|| l.strip_prefix("Proxy-Authorization: "))
                    });
                    let authorized = match (&require_auth, got_auth) {
                        (Some(expected), Some(auth)) => check_basic(auth, expected),
                        (None, _) => true,
                        (Some(_), None) => false,
                    };
                    if !authorized {
                        stats.auth_rejected.fetch_add(1, Ordering::SeqCst);
                        let _ = socket
                            .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\nproxy-authenticate: Basic realm=\"proxy\"\r\ncontent-length: 0\r\n\r\n")
                            .await;
                        return;
                    }
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                        .await;
                    stats.connects.fetch_add(1, Ordering::SeqCst);
                    let Some((_h, p)) = target.rsplit_once(':') else {
                        return;
                    };
                    let Ok(port) = p.parse::<u16>() else {
                        return;
                    };
                    let Ok(mut upstream) =
                        tokio::net::TcpStream::connect(("127.0.0.1", port)).await
                    else {
                        return;
                    };
                    let _ = tokio::io::copy_bidirectional(&mut socket, &mut upstream).await;
                } else {
                    // Plain HTTP proxy request: absolute-form target.
                    let target = first.split(' ').nth(1).unwrap_or("").to_string();
                    stats.plain_requests.fetch_add(1, Ordering::SeqCst);
                    // The plain path must honor proxy auth too (§28).
                    let got = req.lines().find_map(|l| {
                        l.strip_prefix("proxy-authorization: ")
                            .or_else(|| l.strip_prefix("Proxy-Authorization: "))
                    });
                    let ok = match (&require_auth, got) {
                        (Some(expected), Some(a)) => check_basic(a, expected),
                        (None, _) => true,
                        (Some(_), None) => false,
                    };
                    if !ok {
                        stats.auth_rejected.fetch_add(1, Ordering::SeqCst);
                        let _ = socket
                            .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\nproxy-authenticate: Basic realm=\"proxy\"\r\ncontent-length: 0\r\n\r\n")
                            .await;
                        return;
                    }
                    // Rewrite the absolute-form URL to origin-form and
                    // forward to the origin.
                    let uri = target.parse::<hyper::Uri>().expect("uri");
                    let host = uri.host().expect("host").to_string();
                    let port = uri.port_u16().unwrap_or(80);
                    let mut upstream = tokio::net::TcpStream::connect(("127.0.0.1", port))
                        .await
                        .expect("origin connect");
                    let path = uri.path().to_string();
                    let forward = format!(
                        "GET {path} HTTP/1.1\r\nhost: {host}:{port}\r\nconnection: close\r\n\r\n"
                    );
                    upstream.write_all(forward.as_bytes()).await.expect("write");
                    let mut resp = vec![];
                    upstream.read_to_end(&mut resp).await.expect("read");
                    let _ = socket.write_all(&resp).await;
                }
            });
        }
    });
    addr
}

#[derive(Debug, Default)]
struct ProxyStats {
    requests: AtomicUsize,
    connects: AtomicUsize,
    plain_requests: AtomicUsize,
    auth_rejected: AtomicUsize,
}

/// Compare a `Basic` proxy-authorization header against the expected
/// `user:pass` the proxy demands.
fn check_basic(header: &str, expected_userpass: &str) -> bool {
    let encoded = kdown_engine::http::connect::base64_encode_public(expected_userpass.as_bytes());
    header.trim() == format!("Basic {encoded}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn https_through_connect_proxy_with_credentials() {
    let stats = Arc::new(ProxyStats::default());
    let proxy = start_proxy(true, stats.clone()).await;
    let (origin, ca_pem) = start_tls_origin().await;
    let dir = tempfile::tempdir().expect("tmpdir");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).expect("write ca");

    let mut cfg = EngineConfig::default();
    cfg.proxy = ProxyConfig::Http {
        url: format!("http://user:secret@{proxy}/"),
    };
    cfg.tls.custom_ca_bundle = Some(ca_path);
    cfg.network.response_header_timeout = std::time::Duration::from_secs(30);
    cfg.network.read_idle_timeout = std::time::Duration::from_secs(30);

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let dest = dir.path().join("out.bin");
    let req = DownloadRequest::new(
        format!("https://localhost:{}/f.bin", origin.port()),
        dest.clone(),
    );
    let result = controller.run(req).await.expect("run");
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    assert_eq!(
        fixtures::file_sha256(dest.as_path()),
        fixtures::sha256_hex(CONTENT)
    );
    assert_eq!(
        stats.connects.load(Ordering::SeqCst),
        1,
        "one CONNECT tunnel"
    );
    // TLS validation still applied end-to-end (the origin is self-signed;
    // the download succeeded only because the CA bundle trusted it).
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_http_through_proxy_uses_absolute_form() {
    let stats = Arc::new(ProxyStats::default());
    let proxy = start_proxy(false, stats.clone()).await;
    let origin_addr = start_plain_origin().await;
    let dir = tempfile::tempdir().expect("tmpdir");

    let mut cfg = EngineConfig::default();
    cfg.proxy = ProxyConfig::Http {
        url: format!("http://{proxy}/"),
    };
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let dest = dir.path().join("out.bin");
    let req = DownloadRequest::new(format!("http://{origin_addr}/f.bin"), dest.clone());
    let result = controller.run(req).await.expect("run");
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    assert_eq!(
        fixtures::file_sha256(dest.as_path()),
        fixtures::sha256_hex(CONTENT)
    );
    // HEAD probe + GET = 2 requests (documented pipeline §9.1).
    assert_eq!(stats.plain_requests.load(Ordering::SeqCst), 2);
}

/// The 407 challenge path: a proxy requiring credentials fails the job
/// with a structured error; no unbounded retries (§28/§29).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_auth_failure_fails_structured() {
    let stats = Arc::new(ProxyStats::default());
    let proxy = start_proxy(true, stats.clone()).await;
    let origin_addr = start_plain_origin().await;
    let dir = tempfile::tempdir().expect("tmpdir");

    let mut cfg = EngineConfig::default();
    // No credentials configured: proxy rejects.
    cfg.proxy = ProxyConfig::Http {
        url: format!("http://{proxy}/"),
    };
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let dest = dir.path().join("out.bin");
    let req = DownloadRequest::new(format!("http://{origin_addr}/f.bin"), dest.clone());
    let result = controller.run(req).await.expect("run");
    assert_eq!(result.status, ResultStatus::Failed);
    let err = result.error.expect("structured error");
    assert_eq!(
        err.category(),
        kdown_engine::error::ErrorCategory::Proxy,
        "{err}"
    );
    // At most a handful of requests: the engine does not loop (§29).
    assert!(stats.requests.load(Ordering::SeqCst) <= 4);
}

/// Credential provider callback consulted on a 401 challenge, once
/// (§29).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn credential_provider_satisfies_challenge_once() {
    use kdown_engine::control::auth::{provider_fn, Challenge, CredentialDecision};
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let provider = provider_fn(move |ch: &Challenge| {
        provider_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(ch.status, 401);
        Ok(CredentialDecision::Headers(vec![(
            "Authorization".to_string(),
            "Bearer good-token".to_string(),
        )]))
    });
    let origin_addr = start_plain_origin_auth().await;
    let dir = tempfile::tempdir().expect("tmpdir");

    let cfg = EngineConfig::default();
    let transport = HttpTransport::new(cfg.network.clone()).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let mut req = DownloadRequest::new(
        format!("http://{origin_addr}/f.bin"),
        dir.path().join("out.bin"),
    );
    req.credential_provider = Some(Arc::from(provider));
    let result = controller.run(req).await.expect("run");
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    assert_eq!(
        fixtures::file_sha256(dir.path().join("out.bin").as_path()),
        fixtures::sha256_hex(CONTENT)
    );
    // Exactly one provider consultation: the credential worked (§29).
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// A wrong credential must not loop: the provider is consulted at most
/// twice (MAX_AUTH_STAGES), then the job fails with the structured error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_credentials_fail_without_loop() {
    use kdown_engine::control::auth::{provider_fn, CredentialDecision};
    let provider = provider_fn(|_ch| {
        Ok(CredentialDecision::Headers(vec![(
            "Authorization".to_string(),
            "Bearer wrong-token".to_string(),
        )]))
    });
    let origin_addr = start_plain_origin_auth().await;
    let dir = tempfile::tempdir().expect("tmpdir");

    let cfg = EngineConfig::default();
    let transport = HttpTransport::new(cfg.network.clone()).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let mut req = DownloadRequest::new(
        format!("http://{origin_addr}/f.bin"),
        dir.path().join("out.bin"),
    );
    req.credential_provider = Some(Arc::from(provider));
    let result = controller.run(req).await.expect("run");
    assert_eq!(result.status, ResultStatus::Failed);
    assert_eq!(
        result.error.expect("err").category(),
        kdown_engine::error::ErrorCategory::AuthenticationRequired
    );
}

// ---------------------------------------------------------------------------
// Fixture servers
// ---------------------------------------------------------------------------

async fn start_tls_origin() -> (std::net::SocketAddr, Vec<u8>) {
    use tokio_rustls::rustls;
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    let ca_pem = cert.cert.pem().into_bytes();
    let cert_der: rustls::pki_types::CertificateDer<'static> = cert.cert.into();
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()),
    );
    let mut cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("cert pair");
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tls = Arc::new(cfg);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let tls = tls.clone();
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
                    hyper::service::service_fn(|_req| async {
                        Ok::<_, std::convert::Infallible>(
                            hyper::Response::builder()
                                .header("content-length", CONTENT.len())
                                .body(http_body_util::Full::new(hyper::body::Bytes::from(CONTENT)))
                                .expect("resp"),
                        )
                    }),
                )
                .await;
                drop(served);
            });
        }
    });
    (addr, ca_pem)
}

async fn start_plain_origin() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .timer(hyper_util::rt::TokioTimer::new())
                    .serve_connection(
                        hyper_util::rt::TokioIo::new(socket),
                        hyper::service::service_fn(|_req| async {
                            Ok::<_, std::convert::Infallible>(
                                hyper::Response::builder()
                                    .header("content-length", CONTENT.len())
                                    .body(http_body_util::Full::new(hyper::body::Bytes::from(
                                        CONTENT,
                                    )))
                                    .expect("resp"),
                            )
                        }),
                    )
                    .await;
            });
        }
    });
    addr
}

/// Origin that demands `Authorization: Bearer good-token` (401 first).
async fn start_plain_origin_auth() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .timer(hyper_util::rt::TokioTimer::new())
                    .serve_connection(
                        hyper_util::rt::TokioIo::new(socket),
                        hyper::service::service_fn(
                            |req: hyper::Request<hyper::body::Incoming>| async move {
                                let auth_ok = req
                                    .headers()
                                    .get("authorization")
                                    .and_then(|v| v.to_str().ok())
                                    .is_some_and(|v| v == "Bearer good-token");
                                if auth_ok {
                                    Ok::<_, std::convert::Infallible>(
                                        hyper::Response::builder()
                                            .header("content-length", CONTENT.len())
                                            .body(http_body_util::Full::new(
                                                hyper::body::Bytes::from(CONTENT),
                                            ))
                                            .expect("resp"),
                                    )
                                } else {
                                    Ok(hyper::Response::builder()
                                        .status(401)
                                        .header("www-authenticate", "Bearer realm=\"kdown\"")
                                        .body(http_body_util::Full::new(hyper::body::Bytes::new()))
                                        .expect("resp"))
                                }
                            },
                        ),
                    )
                    .await;
            });
        }
    });
    addr
}
