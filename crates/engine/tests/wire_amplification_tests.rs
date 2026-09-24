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

use std::future::Future as _;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use kdown_engine::config::{EngineConfig, H2ConnectionPolicy, SegmentSizing, TransferPolicy};
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
        base.with_header("accept-ranges", "bytes").chunked(delay)
    }
}

// Task 3.2 closed the overlap re-delivery: the split boundary respects the
// original request's receipt high-watermark and the original worker stops
// consuming at its split-shrunk lease end (discard accounted as split
// waste). Measured 1.016-1.023x on this fixture across three runs
// (2026-09-24 baseline: 2.000x). The tolerance below is the phase-3 gate
// bound (<1.10); 2.0x remains an unconditional failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
    assert_eq!(
        result.completed_bytes, len,
        "accepted bytes must equal file size"
    );

    let emitted = server.payload_emitted().await;
    assert!(
        emitted >= len,
        "server must serve at least the full payload: {emitted}"
    );
    let amplification = emitted as f64 / len as f64;
    eprintln!(
        "[wire-amplification] emitted={emitted} accepted={len} amplification={amplification:.3}x"
    );
    assert!(
        amplification < 1.10,
        "clean split amplification {amplification:.3}x exceeded the 1.10 \
         tolerance (emitted {emitted} bytes for {len} accepted)"
    );
    assert!(
        amplification < 2.0,
        "wire amplification {amplification:.3}x reached the unconditional 2x \
         failure bound (emitted {emitted} bytes for {len} accepted)"
    );
}

/// Slow-streaming body: yields the payload in small frames with a delay
/// between frames (mirroring the H1 chunked fixture), so the server's
/// emission tracks the client's actual consumption instead of racing ahead
/// through HTTP/2 flow-control windows that a stream reset later discards.
struct SlowBody {
    data: hyper::body::Bytes,
    pos: usize,
    chunk: usize,
    delay: Duration,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl SlowBody {
    fn new(data: hyper::body::Bytes, chunk: usize, delay: Duration) -> Self {
        Self {
            data,
            pos: 0,
            chunk,
            delay,
            sleep: None,
        }
    }
}

impl hyper::body::Body for SlowBody {
    type Data = hyper::body::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        if self.pos >= self.data.len() {
            return std::task::Poll::Ready(None);
        }
        if let Some(sleep) = self.sleep.as_mut() {
            match sleep.as_mut().poll(cx) {
                std::task::Poll::Ready(()) => {
                    self.sleep = None;
                }
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
        let end = (self.pos + self.chunk).min(self.data.len());
        let frame = self.data.slice(self.pos..end);
        self.pos = end;
        self.sleep = Some(Box::pin(tokio::time::sleep(self.delay)));
        std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(frame))))
    }
}

/// Payload body that counts bytes as the server actually emits them
/// (polled frames), so a client-side stream reset stops the count —
/// requested bytes are not emitted bytes.
struct CountingBody<B> {
    inner: B,
    counter: Arc<std::sync::atomic::AtomicU64>,
}

impl<B> hyper::body::Body for CountingBody<B>
where
    B: hyper::body::Body<Data = hyper::body::Bytes> + Unpin,
    B::Error: std::fmt::Debug,
{
    type Data = hyper::body::Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match std::pin::Pin::new(&mut self.inner).poll_frame(cx) {
            std::task::Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    self.counter
                        .fetch_add(data.len() as u64, std::sync::atomic::Ordering::SeqCst);
                }
                std::task::Poll::Ready(Some(Ok(frame)))
            }
            other => other,
        }
    }
}

/// Phase-3 gate over HTTPS/2: the same clean live-tail split fixture must
/// hold the <1.10 amplification bound when the range streams multiplex over
/// one H2 connection (no additional sockets, streams validated as usual).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_tail_split_h2_never_doubles_wire_payload() {
    run_h2_split_case(4243, false, "h2").await;
}

/// Task 3.6/4.1 follow-up: the same H2 fixture over the pipelined write
/// path — a chunk that spans the split-shrunk lease boundary is truncated
/// (owned prefix written, remainder counted as split waste) instead of
/// failing the lease with "settled below the validated range".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_tail_split_h2_pipelined_never_doubles_wire_payload() {
    run_h2_split_case(4245, true, "h2+pipelined").await;
}

#[allow(clippy::too_many_lines)]
async fn run_h2_split_case(seed: u64, pipeline: bool, label: &str) {
    use tokio_rustls::rustls;

    let len = 8 * 1024 * 1024;
    let content = deterministic_bytes(len, seed);
    // H2 TLS server with the same delayed range handler.
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("self-signed cert");
    let ca_pem = cert.cert.pem().into_bytes();
    let cert_der: rustls::pki_types::CertificateDer<'static> = cert.cert.into();
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()),
    );
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("server cert");
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let tls_config = Arc::new(tls);

    let payload = Arc::new(content.clone());
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let emitted = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let emit_counter = emitted.clone();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let tls = tls_config.clone();
            let payload = payload.clone();
            let counter = emit_counter.clone();
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
                            let counter = counter.clone();
                            let payload = payload.clone();
                            async move {
                                let range = req
                                    .headers()
                                    .get("range")
                                    .and_then(|v| v.to_str().ok())
                                    .and_then(|v| {
                                        let rest = v.strip_prefix("bytes=")?;
                                        let (s, e) = rest.split_once('-')?;
                                        Some((s.parse::<u64>().ok()?, e.parse::<u64>().ok()?))
                                    });
                                let len = payload.len() as u64;
                                let (status, body, content_range) = match range {
                                    Some((s, e)) => {
                                        let end = e.min(len - 1);
                                        eprintln!("[h2-req] bytes={s}-{end}");
                                        // Slow the stream so the first worker
                                        // stays on the wire for idle workers
                                        // to split the live tail.
                                        tokio::time::sleep(Duration::from_millis(2)).await;
                                        (
                                            206,
                                            payload[s as usize..=(end as usize)].to_vec(),
                                            Some(format!("bytes {s}-{end}/{len}")),
                                        )
                                    }
                                    None => {
                                        tokio::time::sleep(Duration::from_millis(2)).await;
                                        (200, payload.as_ref().clone(), None)
                                    }
                                };
                                let mut resp = hyper::Response::builder()
                                    .status(status)
                                    .header("content-length", body.len())
                                    .header("accept-ranges", "bytes");
                                if let Some(cr) = content_range {
                                    resp = resp.header("content-range", cr);
                                }
                                // Count bytes as hyper actually polls them
                                // onto the socket: a client that stops
                                // reading (split stop) resets the stream and
                                // the count stops with it — requested bytes
                                // are not emitted bytes.
                                let counted = CountingBody {
                                    inner: SlowBody::new(
                                        hyper::body::Bytes::from(body),
                                        // A chunk size that does not divide
                                        // the range evenly, so chunks can
                                        // genuinely span a split boundary.
                                        40 * 1024,
                                        Duration::from_millis(2),
                                    ),
                                    counter: counter.clone(),
                                };
                                resp.body(counted)
                                    .map(Ok::<_, std::convert::Infallible>)
                                    .expect("response body")
                            }
                        },
                    ),
                )
                .await;
                if let Err(error) = served {
                    eprintln!("h2 amp server connection error: {error}");
                }
            });
        }
    });

    let dir = tempfile::tempdir().expect("tmp");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).expect("write ca");
    let dest = dir.path().join("amp-h2.bin");

    let mut config = cfg(len);
    config.tls.custom_ca_bundle = Some(ca_path);
    config.h2_policy = H2ConnectionPolicy::Single;
    if pipeline {
        config.write_executor.pipeline_writes = true;
        config.write_executor.writer_threads = 2;
    }
    let c = SingleStreamController::new(
        HttpTransport::from_config(&config).expect("transport"),
        config,
    );
    let result = c
        .run(DownloadRequest::new(
            format!("https://localhost:{}/amp-h2.bin", addr.port()),
            dest.clone(),
        ))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    assert_eq!(result.completed_bytes, len, "unique coverage exact");

    let emitted_bytes = emitted.load(std::sync::atomic::Ordering::SeqCst);
    assert!(emitted_bytes >= len, "server served at least the payload");
    let amplification = emitted_bytes as f64 / len as f64;
    eprintln!(
        "[wire-amplification][{label}] emitted={emitted_bytes} accepted={len} amplification={amplification:.3}x"
    );
    assert!(
        amplification < 1.10,
        "clean {label} split amplification {amplification:.3}x exceeded 1.10"
    );
    assert!(
        amplification < 2.0,
        "{label} amplification {amplification:.3}x reached the unconditional 2x bound"
    );
}

/// Phase-3 gate over the pipelined write path with duration sizing: the
/// shared executor + ready-work + duration allocation must hold the same
/// clean-split amplification bound (internal switch enabled).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_tail_split_pipelined_duration_holds_amplification_bound() {
    let len = 8 * 1024 * 1024;
    let content = deterministic_bytes(len, 4244);
    let server = TestServer::new()
        .serve_handler(
            "/amp-pipelined.bin",
            delayed_static(Arc::new(content.clone()), Duration::from_millis(2)),
        )
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("amp-pipelined.bin");

    let mut config = cfg(len);
    config.write_executor.pipeline_writes = true;
    config.write_executor.writer_threads = 2;
    config.transfer.segment_sizing = SegmentSizing::Duration { duration_ms: 1_000 };
    let c = SingleStreamController::new(
        HttpTransport::from_config(&config).expect("transport"),
        config,
    );
    let result = c
        .run(DownloadRequest::new(
            server.url("/amp-pipelined.bin"),
            dest.clone(),
        ))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    assert_eq!(result.completed_bytes, len, "unique coverage exact");

    let emitted = server.payload_emitted().await;
    let amplification = emitted as f64 / len as f64;
    eprintln!(
        "[wire-amplification][pipelined+duration] emitted={emitted} accepted={len} amplification={amplification:.3}x"
    );
    assert!(
        amplification < 1.10,
        "pipelined clean-split amplification {amplification:.3}x exceeded 1.10"
    );
    assert!(
        amplification < 2.0,
        "pipelined amplification {amplification:.3}x reached the unconditional 2x bound"
    );
}
