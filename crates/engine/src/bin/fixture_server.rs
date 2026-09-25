//! Process-isolated fixture server.
//!
//! A standalone HTTP/1.1 server for authoritative benchmark comparisons:
//! the download client's CPU/RSS/syscall measurements are client-only
//! because the server runs in this separate process. Content is a
//! deterministic synthetic file (block-derived xorshift bytes) so any size
//! is servable without whole-file memory, and the expected SHA-256 is
//! printed at startup for parity checks with the in-process fixture.
//!
//! Controlled behaviors (CLI flags):
//! - `--ignore-ranges`: answer every GET with a full 200 response.
//! - `--throttle-mib-s <F>`: pace each response body to at most F MiB/s.
//! - `--transient-fail <N>:<CODE>`: fail the first N body requests with the
//!   given status (e.g. `2:503`) plus `Retry-After: 0`, then serve normally.
//! - `--reset-after-bytes <N>`: abruptly close the connection after N body
//!   bytes of each response (mid-transfer reset).
//! - `--change-etag-after <N>`: flip the ETag validator after N served
//!   responses (resource-change scenarios).
//!
//! Startup protocol (stdout, one per line):
//!   LISTENING \<addr\>   — bind address to point clients at
//!   SIZE \<bytes\>       — synthetic file length
//!   SHA256 \<hex\>       — expected content digest (generator parity check)
//!   READY              — servers are accepting
//!
//! The server serves until killed. With `--tls-cert`/`--tls-key` it speaks
//! TLS with ALPN `h2` + `http/1.1` (hyper auto), so process-isolated
//! comparisons cover the H2 path; otherwise it is plaintext HTTP/1.1.
//!
//! `GET /__stats` returns `emitted=<n> connections=<n> requests=<n>` —
//! server-side wire accounting for amplification records (stats responses
//! do not count toward the validator flip or payload counters).

use std::fs::File;
use std::io::{BufReader, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
/// Synthetic block size: byte at offset `o` comes from the xorshift run
/// seeded with `seed ^ (o / 4096)`. Mirrors the bench/tests generator.
const SYNTHETIC_BLOCK: usize = 4096;

/// Body chunk used for streaming and throttle pacing.
const STREAM_CHUNK: usize = 64 * 1024;

struct ServerConfig {
    size: u64,
    seed: u64,
    ignore_ranges: bool,
    throttle_mib_s: Option<f64>,
    transient_fail: Option<(u32, u16)>,
    reset_after_bytes: Option<u64>,
    change_etag_after: Option<u64>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    /// Loopback RTT approximation: delay applied before each
    /// response's headers (one full RTT per request; the handshake and
    /// kernel queues add their own, so this is a lower bound).
    rtt: Option<Duration>,
    /// Deterministic per-response connection-loss probability (0-100).
    loss_percent: f64,
    /// `Retry-After` seconds for transient-fail responses (default 0).
    retry_after_secs: u64,
}

/// Server-side wire accounting shared by every connection.
#[derive(Debug, Default)]
struct ServerStats {
    /// Payload bytes actually written to sockets (excludes /__stats).
    emitted: AtomicU64,
    /// Accepted TCP connections.
    connections: AtomicU64,
    /// Served requests (including /__stats, excluding failed accepts).
    requests: AtomicU64,
}

impl ServerStats {
    /// Fixed-width summary: the byte length must not change between a
    /// HEAD probe and its GET (engine integrity compares the two), so
    /// every counter is zero-padded to u64's maximum digit count. Parsers
    /// read `key=<digits>` lines, which fixed width does not affect.
    fn summary(&self) -> String {
        format!(
            "emitted={:020}\nconnections={:020}\nrequests={:020}\n",
            self.emitted.load(Ordering::Relaxed),
            self.connections.load(Ordering::Relaxed),
            self.requests.load(Ordering::Relaxed),
        )
    }
}

/// Path of a request line / hyper URI (everything before the HTTP version
/// or query).
fn request_path(target: &str) -> &str {
    let no_query = target.split('?').next().unwrap_or(target);
    // Strip the HTTP version from a raw request line.
    no_query.split_whitespace().next().unwrap_or(no_query)
}

/// Block `i` is the canonical xorshift run seeded with `seed ^ i`, exactly
/// mirroring the shared `deterministic_bytes` generator (parity canary).
fn deterministic_block(block_index: u64, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(SYNTHETIC_BLOCK);
    let mut state = (seed ^ block_index)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0x517C_C1B7_2722_0A95);
    for _ in 0..SYNTHETIC_BLOCK {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push((state >> 24) as u8);
    }
    out
}

fn sha256_hex(content_sha: &sha2::Sha256) -> String {
    use sha2::Digest;
    content_sha
        .clone()
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn expected_sha256(size: u64, seed: u64) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    let mut block_index = 0u64;
    let mut remaining = size;
    while remaining > 0 {
        let block = deterministic_block(block_index, seed);
        let take = (block.len() as u64).min(remaining) as usize;
        h.update(&block[..take]);
        remaining -= take as u64;
        block_index += 1;
    }
    sha256_hex(&h)
}

/// Parse sizes like `1024`, `512KiB`, `32MiB`, `1GiB` (case-insensitive).
fn parse_size(v: &str) -> Option<u64> {
    let lower = v.trim().to_ascii_lowercase();
    let (num, unit): (&str, u64) = if let Some(n) = lower.strip_suffix("kib") {
        (n, 1024)
    } else if let Some(n) = lower.strip_suffix("mib") {
        (n, 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix("gib") {
        (n, 1024 * 1024 * 1024)
    } else {
        (lower.as_str(), 1)
    };
    num.trim().parse::<u64>().ok().map(|n| n * unit)
}

fn parse_u32(v: &str) -> Option<u32> {
    v.parse().ok()
}

fn parse_u64(v: &str) -> Option<u64> {
    // Accept 0x/0X hex prefixes (0xBEEF-style seeds).
    if let Some(hex) = v.strip_prefix("0x").or_else(|| v.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16).ok();
    }
    v.parse().ok()
}

fn usage() -> ! {
    eprintln!(
        "usage: fixture_server [--addr 127.0.0.1:0] [--size 1GiB] [--seed 0]\n\
         [--ignore-ranges] [--throttle-mib-s F] [--transient-fail N:CODE]\n\
         [--reset-after-bytes N] [--change-etag-after N]\n\
         [--tls-cert CERT.pem --tls-key KEY.pem]\n\
         [--rtt-ms F] [--loss-percent P] [--retry-after SECS]"
    );
    std::process::exit(2);
}

fn parse_args() -> (std::net::SocketAddr, ServerConfig) {
    let mut addr: Option<std::net::SocketAddr> = None;
    let mut size = 0u64;
    let mut seed = 0u64;
    let mut ignore_ranges = false;
    let mut throttle = None;
    let mut transient_fail = None;
    let mut reset_after = None;
    let mut change_etag_after = None;
    let mut tls_cert: Option<PathBuf> = None;
    let mut tls_key: Option<PathBuf> = None;
    let mut rtt = None;
    let mut loss_percent = 0.0f64;
    let mut retry_after_secs = 0u64;
    // Materialize (flag, value) pairs first: flags take exactly one value,
    // boolean flags none. No closure borrow conflicts, no arg-parsing deps.
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut it = raw.into_iter();
    while let Some(arg) = it.next() {
        let takes_value = !matches!(arg.as_str(), "--ignore-ranges");
        let value = if takes_value {
            match it.next() {
                Some(v) => v,
                None => usage(),
            }
        } else {
            String::new()
        };
        match arg.as_str() {
            "--addr" => addr = Some(value.parse().unwrap_or_else(|_| usage())),
            "--size" => size = parse_size(&value).unwrap_or_else(|| usage()),
            "--seed" => seed = parse_u64(&value).unwrap_or_else(|| usage()),
            "--ignore-ranges" => ignore_ranges = true,
            "--throttle-mib-s" => throttle = value.parse().ok(),
            "--transient-fail" => {
                let (n, code) = value
                    .split_once(':')
                    .and_then(|(a, b)| Some((parse_u32(a)?, parse_u16(b)?)))
                    .unwrap_or_else(|| usage());
                transient_fail = Some((n, code));
            }
            "--reset-after-bytes" => reset_after = parse_u64(&value),
            "--change-etag-after" => change_etag_after = parse_u64(&value),
            "--tls-cert" => tls_cert = Some(PathBuf::from(&value)),
            "--tls-key" => tls_key = Some(PathBuf::from(&value)),
            "--rtt-ms" => {
                rtt = value
                    .parse::<f64>()
                    .ok()
                    .filter(|ms| *ms >= 0.0)
                    .map(|ms| Duration::from_secs_f64(ms / 1000.0));
            }
            "--loss-percent" => loss_percent = value.parse().unwrap_or(0.0),
            "--retry-after" => retry_after_secs = parse_u64(&value).unwrap_or_else(|| usage()),
            _ => usage(),
        }
    }
    if tls_cert.is_some() != tls_key.is_some() {
        usage();
    }
    let cfg = ServerConfig {
        size,
        seed,
        ignore_ranges,
        throttle_mib_s: throttle,
        transient_fail,
        reset_after_bytes: reset_after,
        change_etag_after,
        tls_cert,
        tls_key,
        rtt,
        loss_percent,
        retry_after_secs,
    };
    (
        addr.unwrap_or_else(|| "127.0.0.1:0".parse().expect("default addr")),
        cfg,
    )
}

fn parse_u16(v: &str) -> Option<u16> {
    v.parse().ok()
}

/// Shared per-server mutable behavior state.
#[derive(Debug)]
struct ServerState {
    /// Remaining count of requests to answer with the transient status.
    remaining_failures: u32,
    transient_status: u16,
    /// Served responses so far (for the validator flip).
    served: u64,
    /// Deterministic loss-roll sequence.
    loss_seq: u64,
    etag: String,
}

impl ServerState {
    fn etag_header(&self) -> String {
        format!("etag: {}\r\n", self.etag)
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let (bind, cfg) = parse_args();
    let listener = TcpListener::bind(bind).await.expect("bind");
    let local = listener.local_addr().expect("local addr");

    // Startup protocol for the spawning process (flushed before serving).
    println!("LISTENING {local}");
    println!("SIZE {}", cfg.size);
    println!("SHA256 {}", expected_sha256(cfg.size, cfg.seed));
    println!("READY");
    std::io::stdout().flush().expect("flush startup protocol");

    let state = std::sync::Arc::new(tokio::sync::Mutex::new(ServerState {
        remaining_failures: cfg.transient_fail.map_or(0, |(n, _)| n),
        transient_status: cfg.transient_fail.map_or(503, |(_, c)| c),
        served: 0,
        loss_seq: 0,
        etag: "iso".to_string(),
    }));
    let cfg = Arc::new(cfg);
    let stats = Arc::new(ServerStats::default());

    if let (Some(cert), Some(key)) = (cfg.tls_cert.clone(), cfg.tls_key.clone()) {
        let tls = tls_server_config(&cert, &key);
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            stats.connections.fetch_add(1, Ordering::Relaxed);
            let acceptor = acceptor.clone();
            let state = state.clone();
            let cfg = cfg.clone();
            let stats = stats.clone();
            tokio::spawn(async move {
                if let Ok(stream) = acceptor.accept(socket).await {
                    serve_tls_connection(stream, state, cfg, stats).await;
                }
            });
        }
    }

    loop {
        let Ok((socket, _)) = listener.accept().await else {
            return;
        };
        stats.connections.fetch_add(1, Ordering::Relaxed);
        let state = state.clone();
        let cfg = cfg.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            serve_connection(socket, state, cfg, stats).await;
        });
    }
}

/// Build a rustls server config from PEM files, advertising both ALPN
/// protocols so hyper's auto builder picks h2 or http/1.1 per connection.
fn tls_server_config(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> tokio_rustls::rustls::ServerConfig {
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut BufReader::new(
        File::open(cert_path).expect("open cert"),
    ))
    .collect::<Result<_, _>>()
    .expect("parse cert pem");
    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut BufReader::new(File::open(key_path).expect("open key")))
            .expect("parse key pem")
            .expect("key present");
    let mut config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("server cert");
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    config
}

/// Serve one TLS connection over HTTP/1.1 or HTTP/2 (ALPN-negotiated) via
/// hyper's auto connection builder, mirroring the raw-H1 behaviors.
async fn serve_tls_connection(
    stream: tokio_rustls::server::TlsStream<TcpStream>,
    state: Arc<tokio::sync::Mutex<ServerState>>,
    cfg: Arc<ServerConfig>,
    stats: Arc<ServerStats>,
) {
    use hyper_util::rt::{TokioExecutor, TokioIo};

    let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    let service = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
        let state = state.clone();
        let cfg = cfg.clone();
        let stats = stats.clone();
        async move { Ok::<_, std::convert::Infallible>(handle_request(req, state, cfg, stats).await) }
    });
    if let Err(e) = builder
        .serve_connection_with_upgrades(TokioIo::new(stream), service)
        .await
    {
        if std::env::var("FIXTURE_SERVER_TRACE").is_ok() {
            eprintln!("[fixture_server] tls connection error: {e}");
        }
    }
}

type BoxBody = http_body_util::combinators::BoxBody<hyper::body::Bytes, std::convert::Infallible>;

fn empty_body() -> BoxBody {
    BoxBody::new(http_body_util::Empty::<hyper::body::Bytes>::new())
}

fn stats_body(stats: &ServerStats) -> BoxBody {
    BoxBody::new(http_body_util::Full::new(hyper::body::Bytes::from(
        stats.summary(),
    )))
}

/// Streaming synthetic body: block-derived chunks on an mpsc channel,
/// honoring throttle pacing and the reset-after boundary.
fn stream_body_box(
    cfg: &ServerConfig,
    start: u64,
    end: u64,
    stats: Arc<ServerStats>,
    truncate: bool,
) -> BoxBody {
    let (tx, rx) = tokio::sync::mpsc::channel::<
        Result<hyper::body::Frame<hyper::body::Bytes>, std::convert::Infallible>,
    >(4);
    let seed = cfg.seed;
    let throttle_mib_s = cfg.throttle_mib_s;
    let reset_after_bytes = cfg.reset_after_bytes;
    tokio::spawn(async move {
        let bytes_per_sec = throttle_mib_s.unwrap_or(f64::INFINITY) * 1024.0 * 1024.0;
        let mut off = start;
        let mut sent_this_response: u64 = 0;
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        while off <= end {
            let take = (STREAM_CHUNK as u64).min(end - off + 1);
            let block_off = off / SYNTHETIC_BLOCK as u64;
            let in_block = (off % SYNTHETIC_BLOCK as u64) as usize;
            let block = deterministic_block(block_off, seed);
            let take_in_block = (SYNTHETIC_BLOCK - in_block).min(take as usize);
            chunks.push(block[in_block..in_block + take_in_block].to_vec());
            if chunks.len() * STREAM_CHUNK >= 256 * 1024 {
                for chunk in chunks.drain(..) {
                    if tx
                        .send(Ok(hyper::body::Frame::data(chunk.into())))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            off += take_in_block as u64;
            sent_this_response += take_in_block as u64;
            stats
                .emitted
                .fetch_add(take_in_block as u64, Ordering::Relaxed);
            if truncate {
                // Deterministic connection loss: premature end after the
                // first chunk (incomplete body on h1 and h2 alike).
                return;
            }
            if let Some(limit) = reset_after_bytes {
                if sent_this_response >= limit {
                    // Premature end: hyper closes/truncates the response.
                    return;
                }
            }
            if bytes_per_sec.is_finite() {
                tokio::time::sleep(Duration::from_secs_f64(
                    take_in_block as f64 / bytes_per_sec.max(f64::EPSILON),
                ))
                .await;
            }
        }
        for chunk in chunks.drain(..) {
            let _ = tx.send(Ok(hyper::body::Frame::data(chunk.into()))).await;
        }
    });
    BoxBody::new(ChanBody { rx })
}

/// hyper body over a chunk channel (tokio-stream is a dev-dependency, so
/// the stream wrapper is implemented directly).
struct ChanBody {
    rx: tokio::sync::mpsc::Receiver<
        Result<hyper::body::Frame<hyper::body::Bytes>, std::convert::Infallible>,
    >,
}

impl hyper::body::Body for ChanBody {
    type Data = hyper::body::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        std::pin::Pin::new(&mut self.rx).poll_recv(cx)
    }
}

/// Shared request handling for the hyper (TLS h2/h1) path.
async fn handle_request(
    req: hyper::Request<hyper::body::Incoming>,
    state: Arc<tokio::sync::Mutex<ServerState>>,
    cfg: Arc<ServerConfig>,
    stats: Arc<ServerStats>,
) -> hyper::Response<BoxBody> {
    use hyper::header;

    stats.requests.fetch_add(1, Ordering::Relaxed);
    let path = request_path(req.uri().path());
    if path == "/__stats" {
        return hyper::Response::builder()
            .status(200)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(stats_body(&stats))
            .expect("stats response");
    }
    let range = req
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("bytes="))
        .and_then(|v| {
            let (s, e) = v.split_once('-')?;
            Some((s.trim().parse::<u64>().ok()?, e.trim().parse::<u64>().ok()?))
        });
    let is_head = req.method() == hyper::Method::HEAD;
    let decision = decide_response(&state, &cfg, is_head, range).await;
    let (status, range, truncate) = match decision {
        Decision::Fail(status) => {
            return hyper::Response::builder()
                .status(status)
                .header(header::RETRY_AFTER, cfg.retry_after_secs.to_string())
                .header(header::ETAG, current_etag_value(&state).await)
                .body(empty_body())
                .expect("fail response");
        }
        Decision::Serve {
            status,
            range,
            truncate,
        } => (status, range, truncate),
    };
    // Loopback RTT approximation: one RTT before the response headers.
    if let Some(rtt) = cfg.rtt {
        tokio::time::sleep(rtt).await;
    }
    let (start, end) = range.unwrap_or((0, cfg.size.wrapping_sub(1)));
    let body_len = end.saturating_sub(start) + 1;
    let mut builder = hyper::Response::builder()
        .status(status)
        .header(header::ETAG, current_etag_value(&state).await)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, body_len);
    if status == 206 {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{}", cfg.size),
        );
    }
    if is_head {
        return builder.body(empty_body()).expect("head response");
    }
    builder
        .body(stream_body_box(&cfg, start, end, stats, truncate))
        .expect("streaming response")
}

/// Outcome of the shared per-request behavior decision (raw and TLS paths).
enum Decision {
    /// Transient status with the configured `Retry-After`.
    Fail(u16),
    /// Normal response: 200 full or 206 range (end inclusive).
    /// `truncate` marks a deterministic connection-loss cut: the body
    /// aborts after its first chunk (the client sees an incomplete body).
    Serve {
        status: u16,
        range: Option<(u64, u64)>,
        truncate: bool,
    },
}

async fn current_etag(state: &tokio::sync::Mutex<ServerState>) -> String {
    state.lock().await.etag_header()
}

/// Bare validator value for header builders (hyper path).
async fn current_etag_value(state: &tokio::sync::Mutex<ServerState>) -> String {
    state.lock().await.etag.clone()
}

/// Shared validator-flip + transient/ignore/range decision, identical for
/// the raw-H1 and TLS hyper paths.
async fn decide_response(
    state: &tokio::sync::Mutex<ServerState>,
    cfg: &ServerConfig,
    is_head: bool,
    range: Option<(u64, u64)>,
) -> Decision {
    // Validator flip: applied after the configured number of served
    // responses. The flip happens between responses, so requests after
    // the boundary observe a changed ETag.
    {
        let mut st = state.lock().await;
        if let Some(after) = cfg.change_etag_after {
            if st.served >= after && st.etag == "iso" {
                st.etag = "iso-v2".to_string();
            }
        }
    }

    // Transient failure window: the first N body requests receive the
    // configured status (Range/GET only; HEAD always succeeds).
    let (status, range) = {
        let mut st = state.lock().await;
        if !is_head && st.remaining_failures > 0 {
            st.remaining_failures -= 1;
            (st.transient_status, None)
        } else if cfg.ignore_ranges {
            (200, None)
        } else {
            match range {
                Some((s, e)) => (206, Some((s, e.min(cfg.size - 1)))),
                None => (200, Some((0, cfg.size.wrapping_sub(1)))),
            }
        }
    };
    // A transient failure consumes the "served" slot too.
    state.lock().await.served += 1;
    if status == 200 || status == 206 {
        // Deterministic per-response connection loss: the roll
        // sequence is derived from the server seed so failures reproduce.
        let truncate = cfg.loss_percent > 0.0 && {
            let mut st = state.lock().await;
            st.loss_seq = st.loss_seq.wrapping_add(1);
            let mut rng = st.loss_seq ^ cfg.seed;
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            (rng % 10_000) as f64 / 100.0 < cfg.loss_percent
        };
        Decision::Serve {
            status,
            range,
            truncate,
        }
    } else {
        Decision::Fail(status)
    }
}

/// Read one HTTP/1.1 request head (headers until CRLFCRLF), returning the
/// parsed Range and the request line for logging/diagnostics.
async fn read_request(socket: &mut TcpStream) -> Option<(Option<(u64, u64)>, String, bool)> {
    let mut head = Vec::with_capacity(1024);
    let mut one = [0u8; 1];
    // Byte-at-a-time header read: loopback latency is irrelevant here and
    // this avoids buffering the body of pipelined requests.
    loop {
        let n = socket.read(&mut one).await.ok()?;
        if n == 0 {
            return None;
        }
        head.push(one[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
        if head.len() > 64 * 1024 {
            return None;
        }
    }
    let text = String::from_utf8_lossy(&head).to_string();
    let request_line = text.lines().next().unwrap_or_default().to_string();
    let close_after = text.to_ascii_lowercase().contains("connection: close");
    // Header names are case-insensitive (RFC 9110 §5.1); hyper writes
    // lowercase names on the wire.
    let range = text
        .lines()
        .find_map(|l| {
            let (name, value) = l.split_once(':')?;
            if !name.eq_ignore_ascii_case("range") {
                return None;
            }
            let value = value.trim().strip_prefix("bytes=")?;
            value.split_once('-')
        })
        .and_then(|(s, e)| Some((s.trim().parse().ok()?, e.trim().parse().ok()?)));
    Some((range, request_line, close_after))
}

async fn serve_connection(
    mut socket: TcpStream,
    state: Arc<tokio::sync::Mutex<ServerState>>,
    cfg: Arc<ServerConfig>,
    stats: Arc<ServerStats>,
) {
    loop {
        let Some((range, request_line, close_after)) = read_request(&mut socket).await else {
            return;
        };
        let is_head = request_line.starts_with("HEAD");
        if std::env::var("FIXTURE_SERVER_TRACE").is_ok() {
            eprintln!("[fixture_server] {} range={:?}", request_line, range);
        }
        stats.requests.fetch_add(1, Ordering::Relaxed);

        // Server-side accounting endpoint (not counted as served content).
        // The raw request line is `METHOD /path HTTP/1.1` — extract the target.
        let raw_target = request_line.split_whitespace().nth(1).unwrap_or("");
        if raw_target.split('?').next().unwrap_or("") == "/__stats" {
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\n\r\n",
                stats.summary().len()
            );
            if socket.write_all(head.as_bytes()).await.is_err()
                || socket.write_all(stats.summary().as_bytes()).await.is_err()
            {
                return;
            }
            continue;
        }

        let decision = decide_response(&state, &cfg, is_head, range).await;
        let etag = current_etag(&state).await;
        let (status, range, truncate) = match decision {
            Decision::Fail(status) => (status, None, false),
            Decision::Serve {
                status,
                range,
                truncate,
            } => (status, range, truncate),
        };
        // Loopback RTT approximation: one RTT before the response headers.
        if let Some(rtt) = cfg.rtt {
            tokio::time::sleep(rtt).await;
        }

        if status == 200 || status == 206 {
            let (start, end) = range.unwrap_or((0, cfg.size.wrapping_sub(1)));
            let body_len = end.saturating_sub(start) + 1;
            let content_range = if status == 206 {
                format!("content-range: bytes {start}-{end}/{}\r\n", cfg.size)
            } else {
                String::new()
            };
            let head = format!(
                "HTTP/1.1 {status} {}\r\n{content_range}{etag}accept-ranges: bytes\r\ncontent-length: {body_len}\r\n\r\n",
                if status == 206 { "Partial Content" } else { "OK" },
            );
            if socket.write_all(head.as_bytes()).await.is_err() {
                return;
            }
            if is_head {
                continue;
            }
            if stream_body(&mut socket, &cfg, start, end, &stats, truncate)
                .await
                .is_err()
            {
                return;
            }
        } else {
            // Transient failure response: structured status + Retry-After.
            let head = format!(
                "HTTP/1.1 {status} Transient\r\n{etag}retry-after: {}\r\ncontent-length: 0\r\n\r\n",
                cfg.retry_after_secs
            );
            if socket.write_all(head.as_bytes()).await.is_err() {
                return;
            }
        }
        if close_after {
            // Honor HTTP/1.1 Connection: close (raw behavior probes wait for
            // EOF to delimit the response).
            socket.shutdown().await.ok();
            return;
        }
    }
}

/// Stream the inclusive range `[start, end]` in bounded chunks with optional
/// throttling, honoring a per-response reset boundary.
async fn stream_body(
    socket: &mut TcpStream,
    cfg: &ServerConfig,
    start: u64,
    end: u64,
    stats: &ServerStats,
    truncate: bool,
) -> std::io::Result<()> {
    let bytes_per_sec = cfg.throttle_mib_s.unwrap_or(f64::INFINITY) * 1024.0 * 1024.0;
    let mut off = start;
    let mut sent_this_response: u64 = 0;
    while off <= end {
        let take = (STREAM_CHUNK as u64).min(end - off + 1);
        let block_off = off / SYNTHETIC_BLOCK as u64;
        let in_block = (off % SYNTHETIC_BLOCK as u64) as usize;
        let block = deterministic_block(block_off, cfg.seed);
        let take_in_block = (SYNTHETIC_BLOCK - in_block).min(take as usize);
        socket
            .write_all(&block[in_block..in_block + take_in_block])
            .await?;
        stats
            .emitted
            .fetch_add(take_in_block as u64, Ordering::Relaxed);
        if truncate {
            // Deterministic connection loss: abort right after the first
            // chunk (the client observes an incomplete body).
            socket.shutdown().await.ok();
            return Ok(());
        }
        off += take_in_block as u64;
        sent_this_response += take_in_block as u64;
        if let Some(limit) = cfg.reset_after_bytes {
            if sent_this_response >= limit {
                // Abrupt mid-transfer reset: drop the connection without a
                // graceful shutdown.
                socket.shutdown().await.ok();
                return Ok(());
            }
        }
        if bytes_per_sec.is_finite() {
            let delay =
                Duration::from_secs_f64(take_in_block as f64 / bytes_per_sec.max(f64::EPSILON));
            tokio::time::sleep(delay).await;
        }
    }
    Ok(())
}
