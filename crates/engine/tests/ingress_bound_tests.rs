//! Ingress bounding (design D3, task 3.3).
//!
//! Verifies the bounded HTTP/1 and HTTP/2 ingress shape: protocol knobs
//! derive from `transfer_memory.network_ingress_max_bytes`, every
//! connection reserves its worst-case buffered footprint from the
//! engine-wide ledger BEFORE dialing, oversized HTTP/1 response heads fail
//! the connection explicitly, and an HTTP/2 server pushing faster than the
//! consumer reads is flow-control throttled within the configured window.
//!
//! These tests are crate-internal: they observe the ledger and the
//! transport's ingress profile, which are not consumer surface.

use futures_core::Stream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use kdown_engine::{
    DownloadController, DownloadRequest, DownloadRunError, EngineConfig, H2ConnectionPolicy,
    HttpTransport,
};

#[tokio::test]
async fn ingress_profile_derives_from_transfer_memory_config() {
    let cfg = EngineConfig::default();
    let profile = crate::http::transport::IngressProfile::from_config(&cfg);
    // Defaults: 128 KiB frame quantum and windows, 64 KiB header ceiling,
    // footprint = window + header metadata.
    assert_eq!(profile.http1_read_buf_exact, 128 * 1024);
    assert_eq!(profile.http1_max_buf, 128 * 1024);
    assert_eq!(profile.http2_connection_window, 128 * 1024);
    assert_eq!(profile.http2_stream_window, 128 * 1024);
    assert_eq!(profile.http2_max_header_list, 64 * 1024);
    assert_eq!(
        profile.connection_footprint,
        128 * 1024 + 64 * 1024,
        "worst-case footprint: window/buffer + header allowance"
    );

    // A small config shrinks every knob and the footprint with it.
    let mut small = EngineConfig::default();
    small.transfer_memory.network_ingress_max_bytes = 64 * 1024;
    small.read_buffer_size = 16 * 1024;
    let profile = crate::http::transport::IngressProfile::from_config(&small);
    assert_eq!(profile.http1_read_buf_exact, 16 * 1024);
    assert_eq!(profile.http2_connection_window, 64 * 1024);
    assert_eq!(profile.http2_stream_window, 16 * 1024);
    assert_eq!(profile.http2_max_header_list, 64 * 1024);
    assert_eq!(profile.connection_footprint, 64 * 1024 + 64 * 1024);

    // An invalid budget fails transport construction before any network
    // activity (task 3.1 wiring through the transport path).
    let mut invalid = EngineConfig::default();
    invalid.transfer_memory.frames_max_bytes = 0;
    assert!(
        HttpTransport::from_config(&invalid).is_err(),
        "invalid budget must fail transport construction"
    );
}

/// An HTTP/1.1 server that paces its body in chunks so the test can sample
/// the ledger mid-transfer.
async fn start_paced_h1_server(
    chunk: usize,
    chunks: usize,
    pause_ms: u64,
) -> (std::net::SocketAddr, Arc<AtomicU64>) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let served = Arc::new(AtomicU64::new(0));
    let returned = Arc::clone(&served);
    tokio::spawn(async move {
        let served_task = served;
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let served_conn = served_task.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                // Read the request head to its terminator.
                let mut buf = [0_u8; 8192];
                loop {
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 || buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let body = vec![0xA5_u8; chunk];
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n",
                            chunk * chunks
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("write head");
                for _ in 0..chunks {
                    served_conn.fetch_add(chunk as u64, Ordering::SeqCst);
                    // The client may hang up as soon as it has the full
                    // body; a reset here is a normal teardown race.
                    if socket.write_all(&body).await.is_err() {
                        return;
                    }
                    if socket.flush().await.is_err() {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(pause_ms)).await;
                }
                // Hold the connection open a while before closing so the
                // reservation stays observable mid-transfer.
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            });
        }
    });
    (addr, returned)
}

fn component_ingress(controller: &DownloadController) -> u64 {
    controller
        .transfer_ledger()
        .component_outstanding(crate::io::transfer_ledger::Component::NetworkIngress)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_footprint_is_charged_while_connected_and_released_on_close() {
    // The paced server keeps one connection open across several chunks so
    // the reservation is observable mid-transfer.
    let (addr, _served) = start_paced_h1_server(64 * 1024, 8, 120).await;
    let cfg = EngineConfig::default();
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let ledger = transport.ledger();
    let footprint = transport.ingress_profile().connection_footprint;
    let controller = Arc::new(DownloadController::new(transport, cfg));

    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let request = DownloadRequest::new(format!("http://127.0.0.1:{}/file", addr.port()), dest);
    let (handle, join) = controller.start(request);

    // Sample until the connection's footprint reservation shows up.
    let mut observed = false;
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if component_ingress(&controller) >= footprint {
            observed = true;
            break;
        }
    }
    assert!(
        observed,
        "the connection must hold its ingress footprint reservation while open"
    );
    assert_eq!(
        component_ingress(&controller),
        footprint,
        "one connection charges exactly one footprint (metered)"
    );
    // The connection footprint is carved out of the aggregate cap at
    // construction (task 3.8): no dynamic pool charge is taken, so the
    // pipeline pool is untouched by connections and cannot be starved.
    assert_eq!(ledger.aggregate_outstanding(), 0);

    let completed = tokio::time::timeout(std::time::Duration::from_secs(30), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("download completes");
    assert!(completed.final_path.exists());
    // Completed transfers never claim unacknowledged bytes completed, and
    // the job's pipeline charges are all gone; the connection itself is
    // parked idle in the pool and holds its footprint until the client
    // drops.
    assert_eq!(
        component_ingress(&controller),
        footprint,
        "the idle pooled connection keeps holding its reservation"
    );
    // Dropping the controller closes the pool and every idle connection.
    drop(handle);
    drop(controller);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        ledger.component_outstanding(crate::io::transfer_ledger::Component::NetworkIngress),
        0,
        "closed connections release their reservation"
    );
    assert_eq!(ledger.aggregate_outstanding(), 0);
}

/// A raw HTTP/1.1 server that floods response HEADERS beyond the bounded
/// read buffer, then closes.
async fn start_oversized_head_server(header_bytes: usize) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: 1\r\nX-Flood: {}\r\n\r\n",
                    "A".repeat(header_bytes)
                );
                use tokio::io::AsyncWriteExt;
                socket.write_all(head.as_bytes()).await.ok();
                socket.write_all(b"x").await.ok();
                socket.shutdown().await.ok();
            });
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http1_oversized_response_head_fails_explicitly_without_memory_blowup() {
    // 4 MiB of headers against a 128 KiB exact read buffer: the connection
    // fails instead of growing the buffer.
    let addr = start_oversized_head_server(4 * 1024 * 1024).await;
    let mut cfg = EngineConfig::default();
    // Every retry re-fails on the flooded head; keep the retry budget
    // small so the explicit failure arrives promptly.
    cfg.retry.max_attempts_per_segment = 2;
    cfg.retry.base_delay = std::time::Duration::from_millis(10);
    cfg.retry.max_delay = std::time::Duration::from_millis(10);
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let ledger = transport.ledger();
    let controller = DownloadController::new(transport, cfg);
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let request = DownloadRequest::new(format!("http://127.0.0.1:{}/file", addr.port()), dest);
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), controller.run(request))
        .await
        .expect("no hang (the flooded head must fail the connection promptly)");
    assert!(
        result.is_err(),
        "an oversized response head must fail explicitly"
    );
    let error = result.expect_err("refusal");
    assert!(
        matches!(
            &error,
            DownloadRunError::Transfer(_) | DownloadRunError::Infrastructure(_)
        ),
        "the failure is typed: {error:?}"
    );
    // No connection survived: every reservation is released.
    assert_eq!(ledger.aggregate_outstanding(), 0);
}

/// Endless 64 KiB-chunk stream counting bytes handed to hyper's h2 send
/// path. hyper pulls from the stream only while the receiver's flow-control
/// window admits data, so the counter plateaus when the client stops
/// reading — the plateau is the observable window bound.
struct FloodStream {
    chunk: hyper::body::Bytes,
    handed: Arc<AtomicU64>,
}

impl Stream for FloodStream {
    type Item = Result<hyper::body::Frame<hyper::body::Bytes>, std::convert::Infallible>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.handed
            .fetch_add(self.chunk.len() as u64, Ordering::SeqCst);
        Poll::Ready(Some(Ok(hyper::body::Frame::data(self.chunk.clone()))))
    }
}

/// An HTTPS test server speaking HTTP/2, streaming an endless body while
/// counting bytes handed to the send path.
async fn start_h2_flood_server() -> (std::net::SocketAddr, Vec<u8>, Arc<AtomicU64>) {
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
    let handed = Arc::new(AtomicU64::new(0));
    let handed_task = handed.clone();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let tls = tls_config.clone();
            let handed = handed_task.clone();
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
                        move |_req: hyper::Request<hyper::body::Incoming>| {
                            let body = http_body_util::StreamBody::new(FloodStream {
                                chunk: hyper::body::Bytes::from(vec![0x5A_u8; 64 * 1024]),
                                handed: Arc::clone(&handed),
                            });
                            async move {
                                match hyper::Response::builder().status(200).body(body) {
                                    Ok(response) => Ok(response),
                                    Err(e) => {
                                        eprintln!("h2 flood response build: {e}");
                                        Err(e)
                                    }
                                }
                            }
                        },
                    ),
                )
                .await;
                if let Err(e) = served {
                    eprintln!("h2 flood server conn error: {e}");
                }
            });
        }
    });
    (addr, ca_pem, handed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn h2_flood_is_flow_control_throttled_within_the_ingress_window() {
    let (addr, ca_pem, handed) = start_h2_flood_server().await;
    let dir = tempfile::tempdir().expect("tmpdir");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).expect("write ca");

    let mut cfg = EngineConfig::default();
    cfg.tls.custom_ca_bundle = Some(ca_path);
    cfg.h2_policy = H2ConnectionPolicy::Single;
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let ledger = transport.ledger();
    let controller = Arc::new(DownloadController::new(transport, cfg));
    let dest = dir.path().join("out.bin");
    let request = DownloadRequest::new(
        format!("https://localhost:{}/file.bin", addr.port()),
        dest.clone(),
    );
    let (handle, join) = controller.start(request);

    // Let the transfer make progress, then pause the consumer.
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    handle.pause();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // While the consumer is paused, the h2 connection window bounds how
    // much the server can hand over: the flood stream plateaus instead of
    // running away. Two samples bound growth to a small multiple of the
    // window (slack covers server/kernel buffering and in-flight chunks).
    let window = 128 * 1024_u64;
    let a = handed.load(Ordering::SeqCst);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let b = handed.load(Ordering::SeqCst);
    assert!(
        b.saturating_sub(a) <= 8 * window,
        "paused client must throttle the server within ~the connection window: \
         handed {a} -> {b} (+{})",
        b - a
    );
    assert!(a > 0, "the flood must have made progress before the pause");

    // Terminate the endless transfer; the point is the bound, not the file.
    handle.cancel();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), join).await;
    // The cancelled job's connection parks idle in the pool until the
    // client drops; dropping closes it and releases the reservation.
    drop(handle);
    drop(controller);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        ledger.component_outstanding(crate::io::transfer_ledger::Component::NetworkIngress),
        0,
        "cancelled flood releases connection reservations"
    );
}
