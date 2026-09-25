//! Benchmark harness (§37, task 6.4).
//!
//! Scenario set (§37.1-§37.3): localhost HTTP/1.1, localhost HTTP/2
//! (ALPN over self-signed TLS, trusted via a custom CA bundle), varying
//! worker counts, preallocation on/off.
//!
//! Every scenario emits a [`ResourceRecord`] built from the engine's real
//! counters (task 1.1): unique completed bytes (useful goodput numerator),
//! network wire bytes, reused checkpoint bytes and wasted/retransmitted
//! bytes. Retransferred bytes are never derived from warning counts. The
//! record verifies final size, content hash and atomic publication per run
//! (task 1.2) and reports useful goodput and wire throughput separately.
//!
//! `criterion` provides the timing loop for the small end (throughput
//! stability) and the harness also emits a §37.3 record per scenario run.
//! Local regression checks compare `benches/results/baseline.md` using
//! `scripts/bench_check.sh` on matching hardware.
//! GitHub-hosted runners execute these scenarios smoke-only because their hardware
//! and load are not comparable to the recorded same-host baseline.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{black_box, Criterion, Throughput};

use kdown_engine::config::{EngineConfig, H2ConnectionPolicy, ProxyConfig, TlsConfig};
use kdown_engine::control::origin::OriginRegistry;
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::job::controller::{
    DownloadRequest, DownloadResult, ResultStatus, SingleStreamController,
};

/// Fixture size per benchmark iteration: big enough that per-request setup
/// is amortized, small enough that one iteration stays sub-second on a
/// developer box (§37: repeatable, noise-tolerant).
const FIXTURE_BYTES: u64 = 32 * 1024 * 1024;

pub mod fixtures {
    // Fixture helpers for the bench harness (duplicated from tests/support
    // so benches stay independent of the tests tree).
    use std::path::Path;

    /// Synthetic block size: content bytes are a pure function of the
    /// block index, so arbitrary ranges (including multi-GiB matrices and a
    /// process-isolated server) are servable without whole-file memory.
    pub const SYNTHETIC_BLOCK: usize = 4096;

    /// Deterministic fixture bytes shared with integration tests
    /// (xorshift generator duplicated from tests/support so benches stay
    /// independent of the tests tree).
    #[must_use]
    pub fn deterministic_bytes(len: u64, seed: u64) -> Vec<u8> {
        let len = len as usize;
        let mut out = Vec::with_capacity(len);
        let mut state = seed
            .wrapping_mul(0x9E3779B97F4A7C15)
            .wrapping_add(0x517CC1B727220A95);
        for _ in 0..len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            out.push((state >> 24) as u8);
        }
        out
    }

    /// One synthetic block (task 1.3): a xorshift run seeded by the block
    /// index, deterministic across processes and runs.
    #[must_use]
    pub fn synthetic_block_bytes(block_index: u64, seed: u64) -> Vec<u8> {
        deterministic_bytes(SYNTHETIC_BLOCK as u64, seed ^ block_index)
    }

    /// SHA-256 hex of a completed file (baseline verification).
    #[must_use]
    pub fn file_sha256(path: &Path) -> String {
        use sha2::{Digest, Sha256};
        let f = std::fs::File::open(path).expect("open");
        let mut r = std::io::BufReader::with_capacity(256 * 1024, f);
        let mut h = Sha256::new();
        std::io::copy(&mut r, &mut h).expect("hash");
        h.finalize().iter().map(|b| format!("{b:02x}")).collect()
    }

    /// SHA-256 hex of in-memory bytes (expected-hash computation for the
    /// deterministic fixture).
    #[must_use]
    pub fn bytes_sha256(content: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(content);
        h.finalize().iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Fixture content behind the bench servers: in-memory (small smoke
    /// fixtures) or synthetic (block-derived; serves any range of any size
    /// without allocating the whole file).
    #[derive(Clone)]
    pub enum ContentSource {
        InMemory(std::sync::Arc<Vec<u8>>),
        /// `len` bytes derived from per-block xorshift runs (`seed`).
        Synthetic {
            len: u64,
            seed: u64,
        },
    }

    impl ContentSource {
        /// Content length in bytes.
        #[allow(clippy::len_without_is_empty)]
        #[must_use]
        pub fn len(&self) -> u64 {
            match self {
                Self::InMemory(v) => v.len() as u64,
                Self::Synthetic { len, .. } => *len,
            }
        }

        /// Read the inclusive byte range `[start, end]`.
        #[must_use]
        pub fn read_range(&self, start: u64, end: u64) -> Vec<u8> {
            debug_assert!(end >= start);
            match self {
                Self::InMemory(v) => v[start as usize..=(end as usize)].to_vec(),
                Self::Synthetic { seed, .. } => {
                    let mut out = Vec::with_capacity((end - start + 1) as usize);
                    let mut off = start;
                    while off <= end {
                        let block_index = off / SYNTHETIC_BLOCK as u64;
                        let block = synthetic_block_bytes(block_index, *seed);
                        let in_block = (off % SYNTHETIC_BLOCK as u64) as usize;
                        let take = (SYNTHETIC_BLOCK - in_block).min((end - off + 1) as usize);
                        out.extend_from_slice(&block[in_block..in_block + take]);
                        off += take as u64;
                    }
                    out
                }
            }
        }

        /// Expected SHA-256 of the full content, computed block-wise for
        /// synthetic sources (no whole-file allocation).
        #[must_use]
        pub fn sha256(&self) -> String {
            use sha2::{Digest, Sha256};
            match self {
                Self::InMemory(v) => bytes_sha256(v),
                Self::Synthetic { len, seed } => {
                    let mut h = Sha256::new();
                    let mut block_index = 0u64;
                    let mut remaining = *len;
                    while remaining > 0 {
                        let block = synthetic_block_bytes(block_index, *seed);
                        let take = (block.len() as u64).min(remaining) as usize;
                        h.update(&block[..take]);
                        remaining -= take as u64;
                        block_index += 1;
                    }
                    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
                }
            }
        }
    }
}

use fixtures::ContentSource;

/// Deterministic in-memory fixture generator shared with integration tests.
fn fixture() -> ContentSource {
    ContentSource::InMemory(std::sync::Arc::new(fixtures::deterministic_bytes(
        FIXTURE_BYTES,
        0xBEEF,
    )))
}

/// Deterministic synthetic fixture: any size, tiny resident cost (task 1.3).
fn synthetic_fixture(len: u64) -> ContentSource {
    ContentSource::Synthetic { len, seed: 0xBEEF }
}

// ---------------------------------------------------------------------------
// Benchmark HTTP servers
// ---------------------------------------------------------------------------

/// HTTP/1.1 plaintext fixture server: serves `/f.bin` with correct ranges.
async fn start_h1_server(content: ContentSource) -> std::net::SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let content = content.clone();
            tokio::spawn(async move {
                // Minimal keep-alive HTTP/1.1 responder over raw TCP.
                let mut buf = vec![0u8; 8192];
                loop {
                    let n = match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    let req = String::from_utf8_lossy(&buf[..n]).to_string();
                    // A HEAD response has no body (RFC 9110 §9.3.2): writing
                    // one desyncs keep-alive connections and poisons pool
                    // reuse for the next request.
                    let is_head = req.starts_with("HEAD");
                    // Header names are case-insensitive (RFC 9110 §5.1).
                    let range = req
                        .lines()
                        .find_map(|l| {
                            let (name, value) = l.split_once(':')?;
                            if !name.eq_ignore_ascii_case("range") {
                                return None;
                            }
                            let value = value.trim().strip_prefix("bytes=")?;
                            value.split_once('-')
                        })
                        .and_then(|(s, e)| {
                            Some((s.trim().parse::<u64>().ok()?, e.trim().parse::<u64>().ok()?))
                        });
                    let (status, extra, range) = match range {
                        Some((s, e)) => {
                            let end = e.min(content.len() - 1);
                            (
                                "206 Partial Content",
                                format!("content-range: bytes {s}-{end}/{}\r\n", content.len()),
                                Some((s, end)),
                            )
                        }
                        None => ("200 OK", String::new(), Some((0, content.len() - 1))),
                    };
                    let body_len = range.map_or(0, |(s, e)| e - s + 1);
                    let head = format!(
                        "HTTP/1.1 {status}\r\n{extra}etag: \"bench\"\r\naccept-ranges: bytes\r\ncontent-length: {body_len}\r\n\r\n",
                    );
                    if socket.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    // HEAD: headers only; keep the keep-alive loop going.
                    if is_head {
                        continue;
                    }
                    // Stream in bounded chunks (task 1.3): never materialize
                    // the whole range server-side, whatever its size.
                    if let Some((s, e)) = range {
                        let mut off = s;
                        while off <= e {
                            let take = (e - off + 1).min(64 * 1024);
                            if socket
                                .write_all(&content.read_range(off, off + take - 1))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            off += take;
                        }
                    }
                }
            });
        }
    });
    addr
}

fn parse_range(v: &str) -> Option<(u64, u64)> {
    // Range headers arrive as "bytes=S-E" (RFC 9110 §14.1.2); the
    // bytes= prefix must be stripped before parsing (task 5.4: the
    // unprefixed parse silently rejected every real Range header, so H2
    // bench fixtures answered range requests with full 200 responses and
    // the engine's safe fallback downgraded those cells to single stream).
    let rest = v.trim().strip_prefix("bytes=")?;
    let (s, e) = rest.split_once('-')?;
    Some((s.trim().parse().ok()?, e.trim().parse().ok()?))
}

/// HTTPS/2 (or HTTP/1.1 via ALPN) fixture server with a self-signed
/// certificate; returns `(addr, ca_pem)`.
async fn start_h2_tls_server(content: ContentSource) -> (std::net::SocketAddr, Vec<u8>) {
    start_h2_tls_server_paced(content, None, false).await
}

/// Paced variant: optional per-64KiB-chunk delay (shaped comparisons,
/// task 9.4).
async fn start_h2_tls_server_paced(
    content: ContentSource,
    pacing: Option<Duration>,
    advertise_ranges: bool,
) -> (std::net::SocketAddr, Vec<u8>) {
    use tokio_rustls::rustls;

    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("self-signed cert");
    let ca_pem = cert.cert.pem().into_bytes();
    let cert_der: rustls::pki_types::CertificateDer<'static> = cert.cert.into();
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()),
    );
    let mut server_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("server cert");
    server_cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let tls_config = Arc::new(server_cfg);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let tls = tls_config.clone();
            let content = content.clone();
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
                                let (status, range, cr) = match range {
                                    Some((s, e)) => {
                                        let end = e.min(content.len() - 1);
                                        (
                                            206,
                                            Some((s, end)),
                                            Some(format!("bytes {s}-{end}/{}", content.len())),
                                        )
                                    }
                                    None => (200, Some((0, content.len() - 1)), None),
                                };
                                let body_len = range.map_or(0, |(s, e)| e - s + 1);
                                let mut resp = hyper::Response::builder()
                                    .status(status)
                                    .header("content-length", body_len)
                                    .header("etag", "\"bench-h2\"");
                                // Task 5.4 protocol-compare fixture: advertise
                                // ranges so the HEAD probe marks the resource
                                // segmentable (segmented H2 comparison).
                                if advertise_ranges {
                                    resp = resp.header("accept-ranges", "bytes");
                                }
                                if let Some(cr) = cr {
                                    resp = resp.header("content-range", cr);
                                }
                                // Stream in bounded chunks (task 1.3): never
                                // materialize the whole range server-side.
                                let (tx, rx) = tokio::sync::mpsc::channel::<
                                    Result<
                                        hyper::body::Frame<hyper::body::Bytes>,
                                        std::convert::Infallible,
                                    >,
                                >(4);
                                tokio::spawn(async move {
                                    if let Some((s, e)) = range {
                                        let mut off = s;
                                        while off <= e {
                                            let take = (e - off + 1).min(64 * 1024);
                                            if let Some(pacing) = pacing {
                                                tokio::time::sleep(pacing).await;
                                            }
                                            let chunk = content.read_range(off, off + take - 1);
                                            if tx
                                                .send(Ok(hyper::body::Frame::data(
                                                    hyper::body::Bytes::from(chunk),
                                                )))
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                            off += take;
                                        }
                                    }
                                });
                                let body = http_body_util::StreamBody::new(
                                    tokio_stream::wrappers::ReceiverStream::new(rx),
                                );
                                resp.body(body)
                                    .map(Ok::<_, std::convert::Infallible>)
                                    .expect("response body")
                            }
                        },
                    ),
                )
                .await;
                drop(served);
            });
        }
    });
    (addr, ca_pem)
}

// ---------------------------------------------------------------------------
// Resource measurement (§37.3) — real counter accounting (task 1.1)
// ---------------------------------------------------------------------------

/// Per-scenario resource record. Byte-labeled fields come from the engine's
/// real `JobCounters` counters via [`DownloadResult`]; the warning count is
/// never reported as bytes.
#[derive(Debug, Clone)]
struct ResourceRecord {
    wall: Duration,
    /// Unique newly completed file bytes — the useful-goodput numerator.
    completed_bytes: u64,
    /// Wire bytes received from the network (retries inflate this).
    network_bytes: u64,
    /// Bytes reused from a checkpoint (never counted as network bytes).
    reused_bytes: u64,
    /// Retransmitted/wasted network bytes (real counter, not warnings.len()).
    retransferred_bytes: u64,
    retries: u64,
    cpu_percent: f64,
    peak_rss_kib: u64,
    /// Context switches during the run (voluntary + nonvoluntary delta).
    context_switches: u64,
    connections: Option<u64>,
    /// Server-side wire accounting from the isolated fixture's `/__stats`
    /// (task 0.2): payload emitted, connections accepted, requests served.
    /// `None` when the scenario has no isolated fixture endpoint.
    server_emitted: Option<u64>,
    server_connections: Option<u64>,
    server_requests: Option<u64>,
    /// Verification (task 1.2): atomic publication happened.
    published: bool,
    /// Verification: final size matches the fixture.
    size_ok: bool,
    /// Verification: final content hash matches the fixture.
    hash_ok: bool,
    /// Process thread-count delta around the run (write-path gate, task
    /// 2.8): legacy lanes shut down (≈0), the pipelined executor keeps its
    /// configured pool (+writer_threads). `None` when not sampled.
    threads_delta: Option<i64>,
}

impl ResourceRecord {
    /// Useful goodput: unique completed bytes per second (MiB/s).
    #[must_use]
    pub fn useful_goodput_mib_s(&self) -> f64 {
        self.completed_bytes as f64 / self.wall.as_secs_f64().max(f64::EPSILON) / (1024.0 * 1024.0)
    }

    /// Wire throughput: network bytes per second (MiB/s), including
    /// retransmit overhead.
    #[must_use]
    pub fn wire_throughput_mib_s(&self) -> f64 {
        self.network_bytes as f64 / self.wall.as_secs_f64().max(f64::EPSILON) / (1024.0 * 1024.0)
    }

    /// Verification status string: `ok` when published, size and hash all
    /// match; otherwise lists what failed.
    #[must_use]
    pub fn verification(&self) -> String {
        if self.published && self.size_ok && self.hash_ok {
            "ok".to_string()
        } else {
            format!(
                "published={} size_ok={} hash_ok={}",
                self.published, self.size_ok, self.hash_ok
            )
        }
    }
}

fn cpu_time() -> Duration {
    // getrusage user+sys via /proc self stat (utime+stime ticks).
    let ticks = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            let fields: Vec<&str> = s.split_whitespace().collect();
            let utime: u64 = fields.get(13)?.parse().ok()?;
            let stime: u64 = fields.get(14)?.parse().ok()?;
            Some(utime + stime)
        })
        .unwrap_or(0);
    Duration::from_millis(ticks * 10) // assume 100 Hz
}

/// Cumulative voluntary + nonvoluntary context switches of this process
/// (/proc/self/status). Callers take before/after deltas around a run.
fn context_switches() -> u64 {
    let text = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let voluntary = text
        .lines()
        .find_map(|l| l.strip_prefix("voluntary_ctxt_switches:"))
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let nonvoluntary = text
        .lines()
        .find_map(|l| l.strip_prefix("nonvoluntary_ctxt_switches:"))
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0);
    voluntary + nonvoluntary
}

fn peak_rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find_map(|l| {
                l.strip_prefix("VmHWM:")
                    .and_then(|v| v.split_whitespace().next()?.parse().ok())
            })
        })
        .unwrap_or(0)
}

fn clock_ticks_per_sec() -> f64 {
    // Linux _SC_CLK_TCK is 100 for the /proc stat fields.
    100.0
}

/// Verify a completed download's size, hash and publication against the
/// fixture (task 1.2).
fn verify_output(
    result: &DownloadResult,
    expected_size: u64,
    expected_hash: &str,
) -> (bool, bool, bool) {
    let published = result.status == ResultStatus::Completed && result.final_path.is_some();
    let Some(path) = result.final_path.as_ref() else {
        return (published, false, false);
    };
    let size_ok = std::fs::metadata(path)
        .map(|m| m.len() == expected_size)
        .unwrap_or(false);
    let hash_ok = size_ok && fixtures::file_sha256(path) == expected_hash;
    (published, size_ok, hash_ok)
}

/// Run one download and return the §37.3 record with real-counter byte
/// accounting (task 1.1) and hash/size/publication verification (task 1.2).
async fn measure_download(
    name: &str,
    cfg: &EngineConfig,
    url: String,
    dest_dir: &std::path::Path,
    expected_size: u64,
    expected_hash: &str,
    conn_probe: Option<&Arc<AtomicUsize>>,
) -> (DownloadResult, ResourceRecord) {
    let transport = HttpTransport::from_config(cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg.clone());
    let dest = dest_dir.join(format!("{name}.bin"));
    let cpu0 = cpu_time();
    let ctx0 = context_switches();
    let wall0 = Instant::now();
    let result = controller
        .run(DownloadRequest::new(url, dest))
        .await
        .expect("download completes");
    let wall = wall0.elapsed();
    let cpu = cpu_time().saturating_sub(cpu0);
    let hz = clock_ticks_per_sec();
    let _ = hz;
    let (published, size_ok, hash_ok) = verify_output(&result, expected_size, expected_hash);
    let record = ResourceRecord {
        wall,
        // Real counters (task 1.1): unique completed, wire network, reused,
        // wasted/retransferred bytes and retries. Never warnings.len().
        completed_bytes: result.completed_bytes,
        network_bytes: result.bytes_downloaded_from_network,
        reused_bytes: result.bytes_reused_from_checkpoint,
        retransferred_bytes: result.wasted_bytes,
        retries: result.retries,
        cpu_percent: if wall.as_secs_f64() > 0.0 {
            (cpu.as_secs_f64() / wall.as_secs_f64()) * 100.0
        } else {
            0.0
        },
        peak_rss_kib: peak_rss_kib(),
        context_switches: context_switches().saturating_sub(ctx0),
        connections: conn_probe.map(|c| c.load(Ordering::SeqCst) as u64),
        server_emitted: None,
        server_connections: None,
        server_requests: None,
        published,
        size_ok,
        hash_ok,
        threads_delta: None,
    };
    (result, record)
}

fn fmt_record(name: &str, r: &ResourceRecord) -> String {
    format!(
        "| {name} | {:.2} | {:.2} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {}  | {} |",
        r.useful_goodput_mib_s(),
        r.wire_throughput_mib_s(),
        r.completed_bytes,
        r.network_bytes,
        r.reused_bytes,
        r.retransferred_bytes,
        r.retries,
        r.wall.as_secs_f64(),
        r.cpu_percent,
        r.peak_rss_kib,
        r.context_switches,
        r.connections
            .map_or_else(|| "n/r".into(), |c| c.to_string()),
        r.verification(),
        r.server_emitted.map_or_else(
            || "n/r".into(),
            |e| format!(
                "{e}/{} /{}",
                r.server_connections.unwrap_or(0),
                r.server_requests.unwrap_or(0)
            ),
        ),
        r.threads_delta
            .map_or_else(|| "n/r".into(), |t| format!("{t:+}")),
    )
}

const RECORD_HEADER: &str = "| Scenario | Goodput (MiB/s) | Wire (MiB/s) | Completed | Network | Reused | Retransferred | Retries | Wall | CPU | Peak RSS | Ctx Switches | Connections | Verify | Server E/C/R |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|";

/// Emit the scenario report to stderr (criterion captures stdout) and the
/// results file so before/after comparisons (task 2.4) have an artifact.
fn emit_report(group: &str, records: &[(String, ResourceRecord)]) {
    let mut out = format!("# {group} resource records (task 1.1/1.2)\n\n{RECORD_HEADER}\n");
    for (name, r) in records {
        out.push_str(&fmt_record(name, r));
        out.push('\n');
    }
    eprintln!("{out}");
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("benches/results")
        .join(group);
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::write(dir.join("records.md"), &out);
}

/// Run one measured scenario download and return its record (task 1.1: real
/// counters; task 1.2: hash/size/publication verification).
fn record_scenario(
    name: &str,
    rt: &tokio::runtime::Runtime,
    cfg: &EngineConfig,
    url: &str,
    expected_hash: &str,
    conn_probe: Option<&Arc<AtomicUsize>>,
) -> ResourceRecord {
    let dir = tempfile::tempdir().expect("tmpdir");
    let (result, record) = rt.block_on(async {
        measure_download(
            name,
            cfg,
            url.to_string(),
            dir.path(),
            FIXTURE_BYTES,
            expected_hash,
            conn_probe,
        )
        .await
    });
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    record
}
// ---------------------------------------------------------------------------
// Criterion benchmarks
// ---------------------------------------------------------------------------

fn bench_h1_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let content = rt.block_on(async { fixture() });
    let expected_hash = content.sha256();
    let addr = rt.block_on(start_h1_server(content.clone()));
    let url = format!("http://{addr}/f.bin");

    let mut group = c.benchmark_group("h1");
    group.throughput(Throughput::Bytes(FIXTURE_BYTES));
    group.sample_size(10);

    let mut records: Vec<(String, ResourceRecord)> = Vec::new();
    for workers in [1u32, 4] {
        let mut cfg = EngineConfig::default();
        cfg.transfer.segmentation_threshold = if workers > 1 { 1 } else { u64::MAX };
        cfg.transfer.max_workers = workers;
        cfg.transfer.min_workers = workers.min(2);
        cfg.network.response_header_timeout = Duration::from_secs(30);
        cfg.network.read_idle_timeout = Duration::from_secs(30);
        let cfg_for_bench = cfg.clone();
        group.bench_function(format!("workers_{workers}"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let dir = tempfile::tempdir().expect("tmpdir");
                    let transport = HttpTransport::from_config(&cfg_for_bench).expect("transport");
                    let controller = SingleStreamController::new(transport, cfg_for_bench.clone());
                    let r = controller
                        .run(DownloadRequest::new(
                            black_box(url.clone()),
                            dir.path().join("out.bin"),
                        ))
                        .await
                        .expect("download");
                    black_box(assert_completed(&r));
                })
            });
        });
        records.push((
            format!("h1/workers_{workers}"),
            record_scenario(
                &format!("workers_{workers}"),
                &rt,
                &cfg,
                &url,
                &expected_hash,
                None,
            ),
        ));
    }
    group.finish();
    emit_report("h1", &records);
}

fn bench_h2_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let content = rt.block_on(async { fixture() });
    let expected_hash = content.sha256();
    let (addr, ca_pem) = rt.block_on(start_h2_tls_server(content.clone()));
    let url = format!("https://localhost:{}/f.bin", addr.port());

    let mut group = c.benchmark_group("h2");
    group.throughput(Throughput::Bytes(FIXTURE_BYTES));
    group.sample_size(10);

    let dir = tempfile::tempdir().expect("tmpdir for ca");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).expect("write ca");

    let mut records: Vec<(String, ResourceRecord)> = Vec::new();
    for workers in [1u32, 4] {
        let mut cfg = EngineConfig::default();
        cfg.tls.custom_ca_bundle = Some(ca_path.clone());
        cfg.transfer.segmentation_threshold = if workers > 1 { 1 } else { u64::MAX };
        cfg.transfer.max_workers = workers;
        cfg.transfer.min_workers = workers.min(2);
        cfg.h2_policy = H2ConnectionPolicy::Single;
        cfg.network.response_header_timeout = Duration::from_secs(30);
        cfg.network.read_idle_timeout = Duration::from_secs(30);
        let cfg_for_bench = cfg.clone();
        group.bench_function(format!("workers_{workers}"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let out = tempfile::tempdir().expect("tmpdir");
                    let transport = HttpTransport::from_config(&cfg_for_bench).expect("transport");
                    let controller = SingleStreamController::new(transport, cfg_for_bench.clone());
                    let r = controller
                        .run(DownloadRequest::new(
                            black_box(url.clone()),
                            out.path().join("out.bin"),
                        ))
                        .await
                        .expect("download");
                    black_box(assert_completed(&r));
                })
            });
        });
        records.push((
            format!("h2/workers_{workers}"),
            record_scenario(
                &format!("workers_{workers}"),
                &rt,
                &cfg,
                &url,
                &expected_hash,
                None,
            ),
        ));
    }
    group.finish();
    emit_report("h2", &records);
}

fn bench_prealloc(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let content = rt.block_on(async { fixture() });
    let expected_hash = content.sha256();
    let addr = rt.block_on(start_h1_server(content.clone()));
    let url = format!("http://{addr}/f.bin");

    let mut group = c.benchmark_group("prealloc");
    group.throughput(Throughput::Bytes(FIXTURE_BYTES));
    group.sample_size(10);

    let mut records: Vec<(String, ResourceRecord)> = Vec::new();
    for prealloc in [true, false] {
        let mut cfg = EngineConfig::default();
        cfg.transfer.preallocate_output = prealloc;
        cfg.network.response_header_timeout = Duration::from_secs(30);
        cfg.network.read_idle_timeout = Duration::from_secs(30);
        let cfg_for_bench = cfg.clone();
        group.bench_function(format!("prealloc_{prealloc}"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let out = tempfile::tempdir().expect("tmpdir");
                    let transport = HttpTransport::from_config(&cfg_for_bench).expect("transport");
                    let controller = SingleStreamController::new(transport, cfg_for_bench.clone());
                    let r = controller
                        .run(DownloadRequest::new(
                            black_box(url.clone()),
                            out.path().join("out.bin"),
                        ))
                        .await
                        .expect("download");
                    black_box(assert_completed(&r));
                })
            });
        });
        records.push((
            format!("prealloc_{prealloc}"),
            record_scenario(
                &format!("prealloc_{prealloc}"),
                &rt,
                &cfg,
                &url,
                &expected_hash,
                None,
            ),
        ));
    }
    group.finish();
    emit_report("prealloc", &records);
}

fn assert_completed(r: &DownloadResult) -> &DownloadResult {
    assert_eq!(r.status, ResultStatus::Completed, "{:?}", r.error);
    r
}

// ---------------------------------------------------------------------------
// Smoke vs manual scenario matrices (task 1.3)
// ---------------------------------------------------------------------------

/// Matrix protocol axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Protocol {
    H1,
    H2,
}

impl Protocol {
    fn label(self) -> &'static str {
        match self {
            Self::H1 => "h1",
            Self::H2 => "h2",
        }
    }
}

/// Matrix mode: the smoke grid runs in CI-sized time (one measured run per
/// scenario, synthetic fixture — no huge allocations); the manual grid adds
/// multi-GiB sizes and 16-worker runs for workstation use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatrixMode {
    Smoke,
    Manual,
}

impl MatrixMode {
    fn label(self) -> &'static str {
        match self {
            Self::Smoke => "matrix-smoke",
            Self::Manual => "matrix-manual",
        }
    }
}

struct MatrixScenario {
    size: u64,
    protocol: Protocol,
    workers: u32,
}

fn size_label(size: u64) -> String {
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    if size % GIB == 0 {
        format!("{}GiB", size / GIB)
    } else {
        format!("{}MiB", size / MIB)
    }
}

/// Smoke grid: 32 MiB/256 MiB/1 GiB × H1/H2 × 1/2/4/8 workers.
/// Manual grid: multi-GiB (2/4 GiB) × H1/H2 × 1/2/4/8/16 workers.
fn matrix_scenarios(mode: MatrixMode) -> Vec<MatrixScenario> {
    let mib = 1024u64 * 1024;
    let gib = 1024 * mib;
    let (sizes, workers): (&[u64], &[u32]) = match mode {
        MatrixMode::Smoke => (&[32 * mib, 256 * mib, gib], &[1, 2, 4, 8]),
        MatrixMode::Manual => (&[2 * gib, 4 * gib], &[1, 2, 4, 8, 16]),
    };
    let mut out = Vec::new();
    for &size in sizes {
        for &protocol in &[Protocol::H1, Protocol::H2] {
            for &workers in workers {
                out.push(MatrixScenario {
                    size,
                    protocol,
                    workers,
                });
            }
        }
    }
    out
}

/// Run one matrix end-to-end with a single measured (non-criterion) run per
/// scenario, verifying size/hash/publication, and record the results file.
fn run_matrix(mode: MatrixMode) {
    // Enough executor threads for the largest worker count plus I/O lanes.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(12)
        .enable_all()
        .build()
        .expect("runtime");
    let mut records: Vec<(String, ResourceRecord)> = Vec::new();
    let scenario_list: Vec<MatrixScenario> = matrix_scenarios(mode);
    for s in scenario_list {
        let label = format!(
            "{}/{}/{}/workers_{}",
            mode.label(),
            s.protocol.label(),
            size_label(s.size),
            s.workers,
        );
        records.push((
            label.clone(),
            rt.block_on(async {
                // Synthetic content: block-derived bytes, no whole-file
                // allocation in either the server or the client.
                let content = synthetic_fixture(s.size);
                let expected_hash = content.sha256();
                let mut cfg = EngineConfig::default();
                if s.workers > 1 {
                    cfg.transfer.segmentation_threshold = 1;
                } else {
                    cfg.transfer.segmentation_threshold = u64::MAX;
                }
                cfg.transfer.max_workers = s.workers;
                cfg.transfer.min_workers = s.workers.min(2);
                cfg.network.response_header_timeout = Duration::from_secs(30);
                cfg.network.read_idle_timeout = Duration::from_secs(30);
                let (url, _ca_dir) = if s.protocol == Protocol::H1 {
                    let addr = start_h1_server(content.clone()).await;
                    (format!("http://{addr}/f.bin"), None)
                } else {
                    let (addr, ca_pem) = start_h2_tls_server(content.clone()).await;
                    let dir = tempfile::tempdir().expect("ca tmpdir");
                    let ca_path = dir.path().join("ca.pem");
                    std::fs::write(&ca_path, &ca_pem).expect("write ca");
                    cfg.tls.custom_ca_bundle = Some(ca_path);
                    cfg.h2_policy = H2ConnectionPolicy::Single;
                    (
                        format!("https://localhost:{}/f.bin", addr.port()),
                        Some(dir),
                    )
                };
                let (result, record) = measure_download(
                    "matrix",
                    &cfg,
                    url,
                    // Destination on the default temp filesystem (tmpfs in CI).
                    tempfile::tempdir().expect("dest tmpdir").path(),
                    s.size,
                    &expected_hash,
                    None,
                )
                .await;
                assert_eq!(
                    result.status,
                    ResultStatus::Completed,
                    "{label}: {:?}",
                    result.error
                );
                assert!(
                    record.published && record.size_ok && record.hash_ok,
                    "{label}: verification failed: {}",
                    record.verification()
                );
                record
            }),
        ));
    }
    emit_report(mode.label(), &records);
}

fn main() {
    // Matrix modes (task 1.3) intercept the criterion CLI: opt-in via
    // `--matrix-smoke` (CI-bounded grid) or `--matrix-manual` (multi-GiB and
    // 16-worker runs). Default remains the criterion smoke scenarios.
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--matrix-smoke") {
        run_matrix(MatrixMode::Smoke);
        return;
    }
    if args.iter().any(|a| a == "--matrix-manual") {
        run_matrix(MatrixMode::Manual);
        return;
    }
    // Phase-4 gate (task 4.4): focused storage-axis comparison with many
    // repetitions (the full matrix on a rotational disk is noisy), H1/H2
    // unshaped, fixed-4 vs adaptive-1-4:
    //   throughput --storage-compare [--dest-dir /mnt/HDD/kdown-bench]
    if args.iter().any(|a| a == "--storage-compare") {
        let dest_root = args
            .iter()
            .position(|a| a == "--dest-dir")
            .and_then(|i| args.get(i + 1))
            .map(std::path::PathBuf::from);
        run_storage_compare(dest_root);
        return;
    }
    // Fixed vs opt-in adaptive comparison (task 9.4): shaped and
    // unconstrained server cases on H1/H2. Phase-4 gate (task 4.4) adds
    // repetitions and an optional destination root so the storage axis can
    // be a real constrained device:
    //   throughput --adaptive-compare [--dest-dir /mnt/HDD/kdown-bench]
    if args.iter().any(|a| a == "--adaptive-compare") {
        let dest_root = args
            .iter()
            .position(|a| a == "--dest-dir")
            .and_then(|i| args.get(i + 1))
            .map(std::path::PathBuf::from);
        run_adaptive_compare(dest_root);
        return;
    }
    // Phase-5 gate (task 5.4): protocol-aware concurrency comparison —
    // shaped H1/H2, single-job and two same-origin jobs, fixed-4 vs
    // adaptive-1-4, recording stream/socket counts, desired/active decision
    // traces, fairness and fallback:
    //   throughput --protocol-compare
    if args.iter().any(|a| a == "--protocol-compare") {
        run_protocol_compare();
        return;
    }
    // Phase-6 gate (task 6.5): shared-origin coordination comparison —
    // shared registry vs per-job backoff fallback, same-origin and
    // mixed-origin job pairs under server throttling:
    //   throughput --origin-compare
    if args.iter().any(|a| a == "--origin-compare") {
        run_origin_compare();
        return;
    }
    // Phase-7 gate (task 7.4): read_buffer_size (write-pipelining frame
    // quantum) sweep — H1/H2 x LAN/WAN x 64/128/256/512 KiB, goodput,
    // CPU/RSS/context-switch cost; 128 KiB stays default unless a size wins
    // beyond dispersion and resource cost.
    //   throughput --buffer-sweep
    if args.iter().any(|a| a == "--buffer-sweep") {
        run_buffer_sweep();
        return;
    }
    // Phase-8 gate (task 8.1): logical-only vs opt-in physical
    // preallocation across tmpfs (fallocate-unsupported fallback) and the
    // project filesystem (NVMe-class), recording startup latency
    // (start -> first credited byte), throughput and resource cost.
    //   throughput --alloc-compare
    if args.iter().any(|a| a == "--alloc-compare") {
        run_alloc_compare();
        return;
    }
    // Phase-2 gate (task 2.8): legacy writer lanes vs the pipelined shared
    // executor across H1/H2 and shaped/unshaped shapes.
    if args.iter().any(|a| a == "--write-path-compare") {
        run_write_path_compare();
        return;
    }
    // Task 3.5: duration-sizing + ready-work sweep on H1/H2 WAN-shaped
    // fixtures against the explicit/automatic baselines.
    if args.iter().any(|a| a == "--duration-sweep") {
        run_duration_sweep();
        return;
    }
    // Target-sizing sweep (task 6.3): explicit initial sizes × automatic
    // oversubscription factors × H1/H2, recording useful goodput and
    // request/retry overhead to benches/results/sweep/records.md.
    if args.iter().any(|a| a == "--sweep") {
        run_sweep();
        return;
    }
    // Multi-job contention harness (task 0.3): one-job baseline,
    // same-origin contention and multi-origin isolation, in-process.
    if args.iter().any(|a| a == "--jobs-smoke") {
        run_jobs_matrix(false);
        return;
    }
    if args.iter().any(|a| a == "--jobs-pipeline") {
        run_jobs_matrix(true);
        return;
    }
    // Client-only mode against a process-isolated fixture server (task 1.4):
    // the server runs as a separate process, so CPU/RSS/connections recorded
    // here are the client's alone. Usage:
    //   throughput --isolated 127.0.0.1:PORT --isolated-size 1GiB \
    //     [--isolated-seed 0] [--isolated-workers 4] [--isolated-label name]
    if let Some(addr) = args
        .iter()
        .position(|a| a == "--isolated")
        .map(|i| args[i + 1].clone())
    {
        run_isolated_client(
            addr,
            arg_value(&args, "--isolated-size"),
            arg_value(&args, "--isolated-seed")
                .and_then(|v| parse_u64_arg(&v))
                .unwrap_or(0),
            arg_value(&args, "--isolated-workers")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1),
            arg_value(&args, "--isolated-label").unwrap_or_else(|| "isolated".to_string()),
            arg_value(&args, "--isolated-ca"),
            arg_value(&args, "--isolated-dest"),
            args.iter().any(|a| a == "--isolated-pipeline"),
        );
        return;
    }
    let mut criterion = criterion::Criterion::default().configure_from_args();
    bench_h1_throughput(&mut criterion);
    bench_h2_throughput(&mut criterion);
    bench_prealloc(&mut criterion);
    criterion.final_summary();
}

/// Parse a u64 argument with optional `0x`/`0X` hex prefix (matches the
/// fixture server's `--seed` parsing so `0xBEEF`-style seeds round-trip).
fn parse_u64_arg(v: &str) -> Option<u64> {
    if let Some(hex) = v.strip_prefix("0x").or_else(|| v.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16).ok();
    }
    v.parse().ok()
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1).cloned())
}

/// Run one client download against a running isolated fixture server and
/// record client-only resources (task 1.4). With `ca` (PEM path) the URL
/// becomes https and the CA bundle is trusted — the isolated fixture then
/// speaks TLS/H2 via `--tls-cert`/`--tls-key` (task 0.2).
#[allow(clippy::too_many_arguments)]
fn run_isolated_client(
    addr: String,
    size: Option<String>,
    seed: u64,
    workers: u32,
    label: String,
    ca: Option<String>,
    dest_dir: Option<String>,
    pipeline: bool,
) {
    let Some(size_str) = size else {
        eprintln!("--isolated requires --isolated-size (e.g. 1GiB)");
        std::process::exit(2);
    };
    let Some(size) = parse_size_arg(&size_str) else {
        eprintln!("--isolated-size must parse (e.g. 32MiB, 1GiB): {size_str}");
        std::process::exit(2);
    };
    let threads_before = process_thread_count();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime");
    let record = rt.block_on(async {
        // Same generator as the isolated server: the client derives the
        // expected digest locally and verifies the received file.
        let expected_hash = fixtures::ContentSource::Synthetic { len: size, seed }.sha256();
        let mut cfg = EngineConfig::default();
        if workers > 1 {
            cfg.transfer.segmentation_threshold = 1;
        } else {
            cfg.transfer.segmentation_threshold = u64::MAX;
        }
        cfg.transfer.max_workers = workers;
        cfg.transfer.min_workers = workers.min(2);
        cfg.network.response_header_timeout = Duration::from_secs(30);
        cfg.network.read_idle_timeout = Duration::from_secs(30);
        cfg.write_executor.pipeline_writes = pipeline;
        let scheme = if let Some(ca) = &ca {
            cfg.tls.custom_ca_bundle = Some(std::path::PathBuf::from(ca));
            "https"
        } else {
            "http"
        };
        // Destination override (task 0.3): point the output at a real
        // storage target (tmpfs/NVMe/HDD) instead of the default tmpdir.
        let dir = if let Some(dest) = &dest_dir {
            std::fs::create_dir_all(dest).expect("create dest dir");
            None
        } else {
            Some(tempfile::tempdir().expect("dest tmpdir"))
        };
        let dest_root: std::path::PathBuf = dest_dir.as_ref().map_or_else(
            || dir.as_ref().expect("tmpdir").path().to_path_buf(),
            std::path::PathBuf::from,
        );
        let _scratch = match dir {
            Some(d) => d,
            None => tempfile::tempdir().expect("scratch tmpdir"),
        };
        let (result, mut record) = measure_download(
            "isolated",
            &cfg,
            format!("{scheme}://{addr}/f.bin"),
            &dest_root,
            size,
            &expected_hash,
            None,
        )
        .await;
        assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
        assert!(
            record.published && record.size_ok && record.hash_ok,
            "isolated client verification failed: {}",
            record.verification()
        );
        // Server-side wire accounting (task 0.2): download the /__stats
        // document through the same engine stack (works for h1 and h2/TLS).
        if let Some(stats) = fetch_isolated_stats(&cfg, &format!("{scheme}://{addr}")).await {
            eprintln!(
                "[isolated] server emitted={} connections={} requests={}",
                stats.0, stats.1, stats.2
            );
            record.server_emitted = Some(stats.0);
            record.server_connections = Some(stats.1);
            record.server_requests = Some(stats.2);
        }
        record
    });
    let record = ResourceRecord {
        threads_delta: Some(process_thread_count() as i64 - threads_before as i64),
        ..record
    };
    let label = if pipeline {
        format!("{label}-pipelined")
    } else {
        label
    };
    emit_report(
        &format!("isolated/{label}"),
        &[(format!("isolated/{label}"), record)],
    );
}

/// Multi-job contention harness (task 0.3): a one-job baseline, same-origin
/// contention (N jobs on one server) and multi-origin isolation (N jobs on N
/// servers), on H1 and H2. One measured run per configuration; per-job rows
/// plus an aggregate row (total bytes / batch wall). CPU/RSS are process-wide
/// and therefore shared across concurrent jobs (noted in the report).
fn run_jobs_matrix(pipeline: bool) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(16)
        .enable_all()
        .build()
        .expect("runtime");
    let mib = 1024u64 * 1024;
    let size = 64 * mib;
    let workers = 4u32;
    let jobs = 4u32;
    let mut records: Vec<(String, ResourceRecord)> = Vec::new();
    for protocol in ["h1", "h2"] {
        for (mode, servers, concurrent) in [
            ("one-job", 1u32, 1u32),
            ("same-origin", 1, jobs),
            ("multi-origin", jobs, jobs),
        ] {
            let label = format!("jobs/{protocol}/{mode}");
            let (batch_wall, aggregate_bytes, per_job) = rt.block_on(async {
                // Start `servers` fixture servers; jobs are distributed
                // round-robin across them (1 server for one-job/same-origin).
                let mut contents = Vec::new();
                let mut urls = Vec::new();
                let mut _ca_dirs = Vec::new();
                let mut cfgs = Vec::new();
                for i in 0..servers {
                    let content = synthetic_fixture(size);
                    let expected = content.sha256();
                    let mut cfg = EngineConfig::default();
                    cfg.transfer.segmentation_threshold = 1;
                    cfg.transfer.max_workers = workers;
                    cfg.transfer.min_workers = workers.min(2);
                    cfg.write_executor.pipeline_writes = pipeline;
                    cfg.network.response_header_timeout = Duration::from_secs(30);
                    cfg.network.read_idle_timeout = Duration::from_secs(30);
                    let url = if protocol == "h1" {
                        let addr = start_h1_server(content.clone()).await;
                        format!("http://{addr}/f{i}.bin")
                    } else {
                        let (addr, ca_pem) = start_h2_tls_server(content.clone()).await;
                        let dir = tempfile::tempdir().expect("ca tmpdir");
                        let ca_path = dir.path().join("ca.pem");
                        std::fs::write(&ca_path, &ca_pem).expect("write ca");
                        cfg.tls.custom_ca_bundle = Some(ca_path);
                        cfg.h2_policy = H2ConnectionPolicy::Single;
                        _ca_dirs.push(dir);
                        format!("https://localhost:{}/f{i}.bin", addr.port())
                    };
                    contents.push((content, expected));
                    urls.push(url);
                    cfgs.push(cfg);
                }
                let started = std::time::Instant::now();
                let mut set = tokio::task::JoinSet::new();
                for j in 0..concurrent {
                    let cfg = cfgs[j as usize % cfgs.len()].clone();
                    let url = urls[j as usize % urls.len()].clone();
                    let expected = contents[j as usize % contents.len()].1.clone();
                    set.spawn(async move {
                        measure_download(
                            &format!("job{j}"),
                            &cfg,
                            url,
                            tempfile::tempdir().expect("dest tmpdir").path(),
                            size,
                            &expected,
                            None,
                        )
                        .await
                    });
                }
                let mut per_job = Vec::new();
                let mut total_bytes = 0u64;
                let mut all_ok = true;
                while let Some(joined) = set.join_next().await {
                    let (result, record) = joined.expect("job join");
                    all_ok &= result.status == ResultStatus::Completed
                        && record.published
                        && record.size_ok
                        && record.hash_ok;
                    total_bytes += result.completed_bytes;
                    per_job.push(record);
                }
                let wall = started.elapsed();
                assert!(all_ok, "{label}: a concurrent job failed verification");
                (wall, total_bytes, per_job)
            });
            for (idx, record) in per_job.iter().enumerate() {
                records.push((format!("{label}/job{idx}"), record.clone()));
            }
            // Aggregate row: batch wall and total completed bytes; the
            // aggregate goodput is the honest contention metric.
            let mut aggregate = per_job.first().cloned().expect("jobs present");
            aggregate.wall = batch_wall;
            aggregate.completed_bytes = aggregate_bytes;
            aggregate.network_bytes = per_job.iter().map(|r| r.network_bytes).sum();
            aggregate.cpu_percent =
                per_job.iter().map(|r| r.cpu_percent).sum::<f64>() / per_job.len().max(1) as f64;
            aggregate.peak_rss_kib = per_job
                .iter()
                .map(|r| r.peak_rss_kib)
                .max()
                .expect("jobs present");
            records.push((format!("{label}/AGGREGATE"), aggregate));
        }
    }
    emit_report("jobs-smoke", &records);
}

/// Fetch the isolated fixture's `/__stats` counters by downloading the
/// tiny document through the engine (same TLS/CA semantics as the job).
async fn fetch_isolated_stats(cfg: &EngineConfig, base: &str) -> Option<(u64, u64, u64)> {
    let dir = tempfile::tempdir().ok()?;
    let transport = HttpTransport::from_config(cfg).ok()?;
    let controller = SingleStreamController::new(transport, cfg.clone());
    let result = controller
        .run(DownloadRequest::new(
            format!("{base}/__stats"),
            dir.path().join("stats.txt"),
        ))
        .await
        .ok()?;
    if result.status != ResultStatus::Completed {
        return None;
    }
    let text = std::fs::read_to_string(dir.path().join("stats.txt")).ok()?;
    let parse = |key: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(key).map(|v| v.trim().parse::<u64>().ok()))
            .flatten()
    };
    Some((
        parse("emitted=")?,
        parse("connections=")?,
        parse("requests=")?,
    ))
}

/// Target-sizing sweep (task 6.3): one measured run per configuration over
/// explicit initial sizes {4, 8, 16} MiB and automatic oversubscription
/// {2, 3, 4} at 4 workers, on H1 and H2, 256 MiB fixture. Records useful
/// goodput, wire bytes (duplication overhead proxy) and retries.
fn run_sweep() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime");
    let mib = 1024u64 * 1024;
    let fixture_size = 256 * mib;
    let workers = 4u32;
    let mut records: Vec<(String, ResourceRecord)> = Vec::new();
    for protocol in ["h1", "h2"] {
        for &target_mib in &[4u64, 8, 16] {
            records.push((
                format!("sweep/{protocol}/explicit-{target_mib}MiB"),
                sweep_run(
                    &rt,
                    protocol,
                    fixture_size,
                    workers,
                    Some(target_mib * mib),
                    0,
                    None,
                    0,
                    false,
                ),
            ));
        }
        for &factor in &[2u64, 3, 4] {
            records.push((
                format!("sweep/{protocol}/auto-x{factor}"),
                sweep_run(
                    &rt,
                    protocol,
                    fixture_size,
                    workers,
                    None,
                    factor,
                    None,
                    0,
                    false,
                ),
            ));
        }
    }
    emit_report("sweep", &records);
}

/// Duration-sizing sweep (task 3.5): target durations 0.5/1/1.5 s ×
/// ready-work factors 2/3/4 on H1/H2 over {unshaped, shaped} in-process
/// fixtures, against the explicit-8MiB and automatic baselines. Records
/// segment/split counts, wire amplification, goodput and wall time.
fn run_duration_sweep() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime");
    let mib = 1024u64 * 1024;
    let reps = 2;
    let mut records: Vec<(String, ResourceRecord)> = Vec::new();
    for protocol in ["h1", "h2"] {
        for shaped in [false, true] {
            let shape_label = if shaped { "shaped" } else { "unshaped" };
            let size = if shaped { 64 * mib } else { 256 * mib };
            // Reference baselines: explicit 8 MiB and automatic x3.
            records.push((
                format!("duration-sweep/{protocol}/{shape_label}/explicit-8MiB"),
                sweep_run(&rt, protocol, size, 4, Some(8 * mib), 0, None, 0, shaped),
            ));
            records.push((
                format!("duration-sweep/{protocol}/{shape_label}/auto-x3"),
                sweep_run(&rt, protocol, size, 4, None, 3, None, 0, shaped),
            ));
            for duration_ms in [500u64, 1_000, 1_500] {
                for ready in [2u64, 3, 4] {
                    for rep in 0..reps {
                        records.push((
                            format!(
                                "duration-sweep/{protocol}/{shape_label}/dur{duration_ms}ms-ready{ready}/rep{rep}"
                            ),
                            sweep_run(&rt, protocol, size, 4, None, 0, Some(duration_ms), ready, shaped),
                        ));
                    }
                }
            }
        }
    }
    emit_report("duration-sweep", &records);
}

/// Fixed vs opt-in adaptive concurrency (task 9.4): one measured run per
/// configuration over {unconstrained, shaped (per-connection 16 MiB/s
/// pacing)} × {h1, h2} × {fixed-4, adaptive(1-4)}. Records useful goodput,
/// wire overhead and worker stability to benches/results/adaptive-compare/.
/// Phase-2 gate (task 2.8): legacy writer lanes vs the pipelined shared
/// executor across H1/H2, shaped/unshaped, with repetitions and process
/// thread-count deltas (writer-thread scaling evidence).
/// Server-side accounting for the throttled origin fixture (task 6.5).
#[derive(Debug, Default)]
struct ThrottledServerStats {
    arrivals: std::sync::Mutex<Vec<Instant>>,
    failures: std::sync::atomic::AtomicU64,
    accepts: std::sync::atomic::AtomicU64,
}

impl ThrottledServerStats {
    fn record_arrival(&self) {
        self.arrivals.lock().expect("arrivals").push(Instant::now());
    }

    fn arrival_count(&self) -> usize {
        self.arrivals.lock().expect("arrivals").len()
    }

    fn failure_count(&self) -> u64 {
        self.failures.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn accept_count(&self) -> u64 {
        self.accepts.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// HTTP/1.1 fixture with first-N throttling: the first `fail_first` requests
/// receive `503` + `Retry-After: <secs>`, then normal ranged service.
async fn start_throttled_h1_server(
    content: ContentSource,
    fail_first: u32,
    retry_after_secs: u64,
    stats: Arc<ThrottledServerStats>,
) -> std::net::SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    // The throttle budget is GLOBAL across connections: exactly `fail_first`
    // failures total, not per connection (per-connection re-arming would
    // multiply the injected failures and blur the comparison).
    let global_remaining = Arc::new(std::sync::atomic::AtomicU32::new(fail_first));
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            stats
                .accepts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let content = content.clone();
            let stats = stats.clone();
            let remaining = global_remaining.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                loop {
                    let n = match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    let req = String::from_utf8_lossy(&buf[..n]).to_string();
                    stats.record_arrival();
                    let is_head = req.starts_with("HEAD");
                    let range = req
                        .lines()
                        .find_map(|l| {
                            let (name, value) = l.split_once(':')?;
                            if !name.eq_ignore_ascii_case("range") {
                                return None;
                            }
                            let value = value.trim().strip_prefix("bytes=")?;
                            value.split_once('-')
                        })
                        .and_then(|(s, e)| {
                            Some((s.trim().parse::<u64>().ok()?, e.trim().parse::<u64>().ok()?))
                        });
                    if remaining.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                        remaining.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        stats
                            .failures
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let head = format!(
                            "HTTP/1.1 503 Service Unavailable\r\nretry-after: {retry_after_secs}\r\ncontent-length: 0\r\n\r\n"
                        );
                        if socket.write_all(head.as_bytes()).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    let (status, extra, range) = match range {
                        Some((s, e)) => {
                            let end = e.min(content.len() - 1);
                            (
                                "206 Partial Content",
                                format!("content-range: bytes {s}-{end}/{}\r\n", content.len()),
                                Some((s, end)),
                            )
                        }
                        None => ("200 OK", String::new(), Some((0, content.len() - 1))),
                    };
                    let body_len = range.map_or(0, |(s, e)| e - s + 1);
                    let head = format!(
                        "HTTP/1.1 {status}\r\n{extra}etag: \"throttled\"\r\naccept-ranges: bytes\r\ncontent-length: {body_len}\r\n\r\n",
                    );
                    if socket.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    if is_head {
                        continue;
                    }
                    if let Some((s, e)) = range {
                        let mut off = s;
                        while off <= e {
                            let take = (e - off + 1).min(64 * 1024);
                            if socket
                                .write_all(&content.read_range(off, off + take - 1))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            off += take;
                        }
                    }
                }
            });
        }
    });
    addr
}

/// One origin-compare cell (task 6.5).
struct OriginCellRecord {
    label: String,
    job_goodput_mib_s: Vec<f64>,
    job_wall_secs: Vec<f64>,
    aggregate_goodput_mib_s: f64,
    server_failures: u64,
    server_requests: usize,
    server_connections: u64,
    registry_throttles: u64,
    registry_successes: u64,
    verify: Vec<String>,
}

/// Phase-6 gate (task 6.5): shared registry vs per-job backoff fallback
/// under server throttling, on one shared origin and on separate origins.
/// The shared registry is injected explicitly so the cell owns its counters.
fn run_origin_compare() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime");
    let mib = 1024u64 * 1024;
    let size = 64 * mib;
    let reps = 3;
    let mut cells: Vec<OriginCellRecord> = Vec::new();
    rt.block_on(async {
        for variant in ["shared", "fallback"] {
            for topology in ["same", "mixed"] {
                for job_count in [1usize, 2] {
                    if topology == "mixed" && job_count == 1 {
                        continue; // mixed needs a peer to demonstrate isolation
                    }
                    for rep in 0..reps {
                        let label = format!("origin/{variant}/{topology}/{job_count}job/rep{rep}");
                        cells.push(
                            origin_compare_cell(&label, variant, topology, job_count, size).await,
                        );
                    }
                }
            }
        }
    });
    emit_origin_report("phase-6/origin-compare", &cells);
}

/// One origin-compare cell: throttle each involved origin once (503 +
/// `Retry-After: 1`), run 1-2 jobs, and record coordination outcomes.
async fn origin_compare_cell(
    label: &str,
    variant: &str,
    topology: &str,
    job_count: usize,
    size: u64,
) -> OriginCellRecord {
    let content = synthetic_fixture(size);
    let expected_hash = content.sha256();
    let stats_a = Arc::new(ThrottledServerStats::default());
    let addr_a = start_throttled_h1_server(content.clone(), 1, 1, stats_a.clone()).await;
    let url_a = format!("http://{addr_a}/f.bin");
    let (stats_b, url_b) = if topology == "mixed" {
        let stats_b = Arc::new(ThrottledServerStats::default());
        let addr_b = start_throttled_h1_server(content.clone(), 1, 1, stats_b.clone()).await;
        (Some(stats_b), Some(format!("http://{addr_b}/f.bin")))
    } else {
        (None, None)
    };

    let mut cfg = EngineConfig::default();
    cfg.transfer.segmentation_threshold = 1;
    cfg.transfer.max_workers = 4;
    cfg.transfer.min_workers = 1;
    cfg.transfer.max_segment_size = 8 * 1024 * 1024;
    cfg.network.response_header_timeout = Duration::from_secs(30);
    cfg.network.read_idle_timeout = Duration::from_secs(30);
    cfg.retry.base_delay = Duration::from_millis(20);
    cfg.retry.max_delay = Duration::from_millis(100);

    let registry = if variant == "shared" {
        OriginRegistry::new()
    } else {
        OriginRegistry::disabled()
    };
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller =
        SingleStreamController::new(transport, cfg.clone()).with_origin_registry(registry.clone());

    let dir = tempfile::tempdir().expect("cell tmpdir");
    let urls = match (topology, job_count) {
        ("same", 1) => vec![url_a.clone()],
        ("same", _) => vec![url_a.clone(), url_a.clone()],
        ("mixed", _) => vec![url_a.clone(), url_b.clone().expect("mixed url b")],
        _ => unreachable!("topology"),
    };

    let started = Instant::now();
    let mut handles = Vec::new();
    let mut joins = Vec::new();
    // Per-job wall measured start->join: `DownloadResult.elapsed` begins
    // after the probe, which would hide probe-phase throttle latency.
    for (j, url) in urls.iter().enumerate() {
        let job_started = Instant::now();
        let req = DownloadRequest::new(url.clone(), dir.path().join(format!("job{j}.bin")));
        let (handle, join) = controller.start(req);
        handles.push(handle);
        joins.push(tokio::spawn(async move {
            let result = join.await;
            (job_started.elapsed(), result)
        }));
    }
    let mut job_goodput = Vec::new();
    let mut job_wall = Vec::new();
    let mut verify = Vec::new();
    for join in joins.into_iter() {
        let (wall, outcome) = tokio::time::timeout(Duration::from_secs(120), join)
            .await
            .expect("no hang")
            .expect("job task");
        let result = outcome.expect("join").expect("terminal");
        let (published, size_ok, hash_ok) = verify_output(&result, size, &expected_hash);
        let error_note = result
            .error
            .as_ref()
            .map(|e| format!(" error={e}"))
            .unwrap_or_default();
        verify.push(format!(
            "published={published} size_ok={size_ok} hash_ok={hash_ok}{error_note}"
        ));
        job_wall.push(wall.as_secs_f64());
        job_goodput.push(
            result.completed_bytes as f64
                / wall.as_secs_f64().max(f64::EPSILON)
                / (1024.0 * 1024.0),
        );
    }
    let cell_wall = started.elapsed().as_secs_f64().max(f64::EPSILON);
    let aggregate = (size * job_count as u64) as f64 / cell_wall / (1024.0 * 1024.0);

    // Registry traces (task 6.5): throttle/success events per involved
    // origin, aggregated over the cell.
    let (mut throttles, mut successes) = (0u64, 0u64);
    for url in &urls {
        if let Some(key) = kdown_engine::control::origin::normalized_origin(url) {
            let (t, s) = registry.throttle_trace(&key);
            throttles += t;
            successes += s;
        }
    }

    let stats = match topology {
        "same" => stats_a,
        _ => {
            // Mixed: report both origins' load summed.
            let b = stats_b.expect("mixed stats b");
            let merged_failures = stats_a.failure_count() + b.failure_count();
            let merged_accepts = stats_a.accept_count() + b.accept_count();
            let mut merged_arrivals = stats_a.arrivals.lock().expect("arrivals").clone();
            merged_arrivals.extend(b.arrivals.lock().expect("arrivals").iter().copied());
            Arc::new(ThrottledServerStats {
                arrivals: std::sync::Mutex::new(merged_arrivals),
                failures: std::sync::atomic::AtomicU64::new(merged_failures),
                accepts: std::sync::atomic::AtomicU64::new(merged_accepts),
            })
        }
    };
    OriginCellRecord {
        label: label.to_string(),
        job_goodput_mib_s: job_goodput,
        job_wall_secs: job_wall,
        aggregate_goodput_mib_s: aggregate,
        server_failures: stats.failure_count(),
        server_requests: stats.arrival_count(),
        server_connections: stats.accept_count(),
        registry_throttles: throttles,
        registry_successes: successes,
        verify,
    }
}

/// Report emitter for origin-compare cells (task 6.5).
fn emit_origin_report(group: &str, cells: &[OriginCellRecord]) {
    let mut out = String::new();
    out.push_str("# Origin-compare cells (task 6.5)\n\n");
    out.push_str("| Cell | Job goodput (MiB/s) | Job wall (s) | Aggregate (MiB/s) | 503s | Requests | Connections | Registry thr/ok | Verify |\n|---|---|---|---|---|---|---|---|---|\n");
    for c in cells {
        let goods = c
            .job_goodput_mib_s
            .iter()
            .map(|g| format!("{g:.2}"))
            .collect::<Vec<_>>()
            .join("/");
        let walls = c
            .job_wall_secs
            .iter()
            .map(|w| format!("{w:.2}"))
            .collect::<Vec<_>>()
            .join("/");
        let fairness = if c.job_goodput_mib_s.len() == 2 {
            let a = c.job_goodput_mib_s[0];
            let b = c.job_goodput_mib_s[1];
            format!(" {:.3}", a.min(b) / a.max(b).max(f64::EPSILON))
        } else {
            String::new()
        };
        out.push_str(&format!(
            "| {} | {goods} | {walls} | {:.2} | {} | {} | {} | {}/{} | {} |{}\n",
            c.label,
            c.aggregate_goodput_mib_s,
            c.server_failures,
            c.server_requests,
            c.server_connections,
            c.registry_throttles,
            c.registry_successes,
            c.verify.join(" ; "),
            fairness
        ));
    }
    eprintln!("{out}");
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("benches/results")
        .join(group);
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::write(dir.join("records.md"), &out);
}

/// One job inside a protocol-compare cell (task 5.4).
struct ProtocolJobRecord {
    label: String,
    goodput_mib_s: f64,
    wall_secs: f64,
    completed_bytes: u64,
    network_bytes: u64,
    retries: u64,
    segment_requests: u64,
    verify: String,
    max_desired: u64,
    max_active: u64,
    desired_trace: String,
    fallback: String,
}

/// One protocol-compare cell: per-cell transport protocol counters plus the
/// per-job records sharing that transport.
struct ProtocolCellRecord {
    label: String,
    establishments_h1: u64,
    establishments_h2: u64,
    requests_h1: u64,
    h2_streams: u64,
    cpu_percent: f64,
    peak_rss_kib: u64,
    jobs: Vec<ProtocolJobRecord>,
}

/// Protocol-aware concurrency gate (task 5.4): shaped H1/H2 x single/dual
/// same-origin jobs x fixed-4/adaptive-1-4. Each cell uses a fresh transport
/// so the task-5.1 protocol counters (physical establishments vs
/// requests/streams) describe exactly that cell; jobs within a cell share
/// one transport (one engine), which is the same-origin fairness scenario.
fn run_protocol_compare() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime");
    let mib = 1024u64 * 1024;
    let size = 128 * mib;
    let reps = 3;
    let mut cells: Vec<ProtocolCellRecord> = Vec::new();
    rt.block_on(async {
        for protocol in ["h1", "h2"] {
            let content = synthetic_fixture(size);
            let expected_hash = content.sha256();
            // 16 MiB/s per connection (H1) / per socket chunk (H2) shaping.
            let pacing = Some(Duration::from_micros(4_000));
            let (url, ca_dir) = if protocol == "h1" {
                let addr = start_h1_server_paced(content.clone(), pacing).await;
                (format!("http://{addr}/f.bin"), None)
            } else {
                let (addr, ca_pem) = start_h2_tls_server_paced(content.clone(), pacing, true).await;
                let dir = tempfile::tempdir().expect("ca tmpdir");
                let ca_path = dir.path().join("ca.pem");
                std::fs::write(&ca_path, &ca_pem).expect("write ca");
                (
                    format!("https://localhost:{}/f.bin", addr.port()),
                    Some(dir),
                )
            };
            for job_count in [1usize, 2] {
                for (mode_label, adaptive) in [("fixed-4", false), ("adaptive-1-4", true)] {
                    for rep in 0..reps {
                        let label =
                            format!("protocol/{protocol}/{}job/{mode_label}/rep{rep}", job_count);
                        let cell = protocol_compare_cell(
                            &url,
                            ca_dir.as_ref(),
                            job_count,
                            adaptive,
                            size,
                            &expected_hash,
                            &label,
                        )
                        .await;
                        cells.push(cell);
                    }
                }
            }
        }
    });
    emit_protocol_report("phase-5/protocol-compare", &cells);
}

/// One protocol-compare cell (task 5.4): start 1..2 jobs on one shared
/// transport, sample desired/active decision traces while they run, then
/// verify and record goodput, fairness and protocol counters.
async fn protocol_compare_cell(
    url: &str,
    ca_dir: Option<&tempfile::TempDir>,
    job_count: usize,
    adaptive: bool,
    size: u64,
    expected_hash: &str,
    label: &str,
) -> ProtocolCellRecord {
    let mut cfg = EngineConfig::default();
    cfg.transfer.segmentation_threshold = 1;
    cfg.transfer.max_workers = 4;
    cfg.transfer.min_workers = 1;
    cfg.transfer.max_segment_size = 8 * 1024 * 1024;
    if adaptive {
        cfg.transfer.concurrency_mode = kdown_engine::config::ConcurrencyMode::Adaptive;
    }
    cfg.network.response_header_timeout = Duration::from_secs(30);
    cfg.network.read_idle_timeout = Duration::from_secs(30);
    if let Some(ca) = ca_dir {
        cfg.tls.custom_ca_bundle = Some(ca.path().join("ca.pem"));
        cfg.h2_policy = H2ConnectionPolicy::Single;
    }

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let stats = transport.protocol_stats().clone();
    let controller = SingleStreamController::new(transport, cfg.clone());

    let dir = tempfile::tempdir().expect("cell tmpdir");
    let cpu0 = cpu_time();
    let cell_started = Instant::now();

    let mut handles = Vec::new();
    let mut joins = Vec::new();
    // Join-completion counter: the job handle stays published after a job
    // finishes, so the sampler below stops on completions, not on the
    // handle disappearing.
    let completed_jobs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for j in 0..job_count {
        let req = DownloadRequest::new(url.to_string(), dir.path().join(format!("job{j}.bin")));
        let (handle, join) = controller.start(req);
        handles.push(handle);
        let completed_jobs = completed_jobs.clone();
        let started = Instant::now();
        joins.push(tokio::spawn(async move {
            let outcome = join.await;
            completed_jobs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            (started.elapsed(), outcome)
        }));
    }

    // Decision-trace sampling (task 5.4): desired/active transitions per job.
    let mut max_desired = vec![0u64; job_count];
    let mut max_active = vec![0u64; job_count];
    let mut traces = vec![Vec::<String>::new(); job_count];
    let mut last_desired = vec![0u64; job_count];
    let sample_started = Instant::now();
    loop {
        for (j, handle) in handles.iter().enumerate() {
            if let Some(job) = handle.segmented_job() {
                let desired = job.desired_workers();
                let active = job.active_workers();
                max_desired[j] = max_desired[j].max(desired);
                max_active[j] = max_active[j].max(active);
                if desired != last_desired[j] {
                    traces[j].push(format!(
                        "{}@{:.2}s",
                        desired,
                        sample_started.elapsed().as_secs_f64()
                    ));
                    last_desired[j] = desired;
                }
            }
        }
        if completed_jobs.load(std::sync::atomic::Ordering::Relaxed) >= job_count
            || sample_started.elapsed() > Duration::from_secs(300)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let mut jobs = Vec::new();
    for (j, join) in joins.into_iter().enumerate() {
        let (wall, outcome) = join.await.expect("job task");
        let result = outcome.expect("join").expect("terminal result");
        let (published, size_ok, hash_ok) = verify_output(&result, size, expected_hash);
        let verify = format!("published={published} size_ok={size_ok} hash_ok={hash_ok}");
        let fallback = if result.warnings.is_empty() {
            "none".to_string()
        } else {
            result.warnings.join("; ")
        };
        jobs.push(ProtocolJobRecord {
            label: format!("{label}/job{j}"),
            goodput_mib_s: result.completed_bytes as f64
                / wall.as_secs_f64().max(f64::EPSILON)
                / (1024.0 * 1024.0),
            wall_secs: wall.as_secs_f64(),
            completed_bytes: result.completed_bytes,
            network_bytes: result.bytes_downloaded_from_network,
            retries: result.retries,
            segment_requests: result.segment_requests,
            verify,
            max_desired: max_desired[j],
            max_active: max_active[j],
            desired_trace: if traces[j].is_empty() {
                format!("{}@start", last_desired[j])
            } else {
                traces[j].join("→")
            },
            fallback,
        });
    }

    let cell_wall = cell_started.elapsed();
    let cpu = cpu_time().saturating_sub(cpu0);
    let cpu_percent = if cell_wall.as_secs_f64() > 0.0 {
        (cpu.as_secs_f64() / cell_wall.as_secs_f64()) * 100.0
    } else {
        0.0
    };
    ProtocolCellRecord {
        label: label.to_string(),
        establishments_h1: stats.establishments_h1(),
        establishments_h2: stats.establishments_h2(),
        requests_h1: stats.requests_h1(),
        h2_streams: stats.h2_streams(),
        cpu_percent,
        peak_rss_kib: peak_rss_kib(),
        jobs,
    }
}

/// Task 7.4: sweep the write-pipelining frame quantum (`read_buffer_size`)
/// across H1/H2 and LAN/WAN shapes; single job, fixed-4 workers.
fn run_buffer_sweep() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime");
    let mib = 1024u64 * 1024;
    let size = 64 * mib;
    let reps = 3;
    let mut rows: Vec<BufferSweepRow> = Vec::new();
    rt.block_on(async {
        for protocol in ["h1", "h2"] {
            let content = synthetic_fixture(size);
            let expected_hash = content.sha256();
            for (shape_label, pacing) in [
                ("lan", None),
                ("wan", Some(Duration::from_micros(4_000))), // ~16 MiB/s
            ] {
                let (url, ca_dir) = if protocol == "h1" {
                    let addr = start_h1_server_paced(content.clone(), pacing).await;
                    (format!("http://{addr}/f.bin"), None)
                } else {
                    let (addr, ca_pem) =
                        start_h2_tls_server_paced(content.clone(), pacing, true).await;
                    let dir = tempfile::tempdir().expect("ca tmpdir");
                    let ca_path = dir.path().join("ca.pem");
                    std::fs::write(&ca_path, &ca_pem).expect("write ca");
                    (
                        format!("https://localhost:{}/f.bin", addr.port()),
                        Some(dir),
                    )
                };
                for quantum_kib in [64u64, 128, 256, 512] {
                    for rep in 0..reps {
                        let label =
                            format!("buffer/{protocol}/{shape_label}/{}k/rep{rep}", quantum_kib);
                        let row = buffer_sweep_cell(
                            &url,
                            ca_dir.as_ref(),
                            quantum_kib * 1024,
                            size,
                            &expected_hash,
                            &label,
                        )
                        .await;
                        rows.push(row);
                    }
                }
            }
        }
    });
    emit_buffer_report(&rows);
}

/// One buffer-sweep cell: single fixed-4 job at a given frame quantum.
async fn buffer_sweep_cell(
    url: &str,
    ca_dir: Option<&tempfile::TempDir>,
    read_buffer_size: u64,
    size: u64,
    expected_hash: &str,
    label: &str,
) -> BufferSweepRow {
    let mut cfg = EngineConfig::default();
    cfg.transfer.segmentation_threshold = 1;
    cfg.transfer.max_workers = 4;
    cfg.transfer.min_workers = 4;
    cfg.transfer.max_segment_size = 8 * 1024 * 1024;
    cfg.read_buffer_size = u32::try_from(read_buffer_size).expect("quantum fits u32");
    cfg.network.response_header_timeout = Duration::from_secs(30);
    cfg.network.read_idle_timeout = Duration::from_secs(30);
    if let Some(ca) = ca_dir {
        cfg.tls.custom_ca_bundle = Some(ca.path().join("ca.pem"));
        cfg.h2_policy = H2ConnectionPolicy::Single;
    }

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg.clone());
    let dir = tempfile::tempdir().expect("cell tmpdir");
    let cpu0 = cpu_time();
    let ctx0 = context_switches();
    let rss0 = peak_rss_kib();
    let started = Instant::now();

    let req = DownloadRequest::new(url.to_string(), dir.path().join("job0.bin"));
    let (_handle, join) = controller.start(req);
    let result = tokio::time::timeout(Duration::from_secs(300), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    let wall = started.elapsed();
    let (published, size_ok, hash_ok) = verify_output(&result, size, expected_hash);

    BufferSweepRow {
        label: label.to_string(),
        read_buffer_kib: read_buffer_size / 1024,
        goodput_mib_s: result.completed_bytes as f64
            / wall.as_secs_f64().max(f64::EPSILON)
            / (1024.0 * 1024.0),
        wall_secs: wall.as_secs_f64(),
        completed_bytes: result.completed_bytes,
        retries: result.retries,
        segment_requests: result.segment_requests,
        cpu_percent: if wall.as_secs_f64() > 0.0 {
            (cpu_time().saturating_sub(cpu0).as_secs_f64() / wall.as_secs_f64()) * 100.0
        } else {
            0.0
        },
        peak_rss_kib: peak_rss_kib(),
        rss0_kib: rss0,
        context_switches: context_switches().saturating_sub(ctx0),
        verify: format!("published={published} size_ok={size_ok} hash_ok={hash_ok}"),
    }
}

/// Report emitter for the buffer sweep (task 7.4).
fn emit_buffer_report(rows: &[BufferSweepRow]) {
    let mut out = String::new();
    out.push_str("# Buffer-sweep cells (task 7.4)\n\n");
    out.push_str(
        "| Cell | read_buffer KiB | Goodput (MiB/s) | Wall (s) | CPU % | ctx/s | Peak RSS KiB | Retries | SegReqs | Verify |\n|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for r in rows {
        let wall = if r.wall_secs > 0.0 { r.wall_secs } else { 1.0 };
        out.push_str(&format!(
            "| {} | {} | {:.2} | {:.2} | {:.1} | {} | {} | {} | {} | {} |\n",
            r.label,
            r.read_buffer_kib,
            r.goodput_mib_s,
            r.wall_secs,
            r.cpu_percent,
            (r.context_switches as f64 / wall) as u64,
            r.peak_rss_kib,
            r.retries,
            r.segment_requests,
            r.verify
        ));
    }
    out.push_str(
        "\nUnavailable axes (labeled, not fabricated): syscall counts and allocation counts are not\ninstrumented; context switches (in+vol, client process) stand in as scheduling-pressure proxy;\npeak RSS is the monotonic process VmHWM (coarse per-cell caveat).\n",
    );
    eprintln!("{out}");
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("benches/results/phase-7/buffer-sweep");
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::write(dir.join("records.md"), &out);
}

/// Task 8.1: logical-only vs opt-in physical preallocation comparison.
fn run_alloc_compare() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime");
    let mib = 1024u64 * 1024;
    let size = 256 * mib;
    let reps = 3;
    let mut rows: Vec<AllocCompareRow> = Vec::new();
    rt.block_on(async {
        let content = synthetic_fixture(size);
        let expected_hash = content.sha256();
        // tmpfs (/dev/shm): fallocate-unsupported -> silent logical fallback.
        // project fs: the repo's own filesystem (NVMe-class on this host).
        for (storage_label, root) in [
            ("tmpfs", Some(std::path::Path::new("/dev/shm"))),
            ("project-fs", None),
        ] {
            if let Some(root) = root {
                if !root.is_dir() {
                    eprintln!("[alloc-compare] {root:?} unavailable; axis labeled not run");
                    continue;
                }
            }
            let addr = start_h1_server_paced(content.clone(), None).await;
            let url = format!("http://{addr}/f.bin");
            for (policy_label, physical) in [("logical", false), ("physical", true)] {
                for rep in 0..reps {
                    let label = format!("alloc/{storage_label}/{policy_label}/rep{rep}");
                    let row =
                        alloc_compare_cell(&url, root, physical, size, &expected_hash, &label)
                            .await;
                    rows.push(row);
                }
            }
        }
    });
    emit_alloc_report(&rows);
}

/// One alloc-compare cell: single fixed-4 job under one prealloc policy,
/// measuring start -> first credited byte (where the reservation cost lands)
/// plus wall, goodput and CPU.
async fn alloc_compare_cell(
    url: &str,
    storage_root: Option<&std::path::Path>,
    physical: bool,
    size: u64,
    expected_hash: &str,
    label: &str,
) -> AllocCompareRow {
    let mut cfg = EngineConfig::default();
    cfg.transfer.segmentation_threshold = 1;
    cfg.transfer.max_workers = 4;
    cfg.transfer.min_workers = 4;
    cfg.transfer.max_segment_size = 8 * 1024 * 1024;
    cfg.transfer.preallocate_output = true; // logical sizing in both arms
    cfg.transfer.preallocate_physical = physical;
    cfg.network.response_header_timeout = Duration::from_secs(30);
    cfg.network.read_idle_timeout = Duration::from_secs(30);

    // Destination under the storage axis root (tempdir inside it).
    let dir = match storage_root {
        Some(root) => tempfile::Builder::new()
            .prefix("alloc-compare-")
            .tempdir_in(root)
            .expect("storage tmpdir"),
        None => tempfile::tempdir().expect("cell tmpdir"),
    };

    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = SingleStreamController::new(transport, cfg.clone());
    let cpu0 = cpu_time();
    let started = Instant::now();

    let req = DownloadRequest::new(url.to_string(), dir.path().join("job0.bin"));
    let (handle, join) = controller.start(req);
    // The join moves into a helper task so the poll loop can timeout on it
    // without consuming it; the task's handle is polled with a 2 ms timeout.
    let mut join_task = tokio::spawn(async move { join.await.expect("join").expect("terminal") });
    // Startup latency: first poll tick where any byte is credited (the
    // reservation cost lands before the first byte). A 2 ms poll overshoots
    // by < 2 ms — far below the effects being measured.
    let mut first_byte: Option<Duration> = None;
    let mut result: Option<DownloadResult> = None;
    loop {
        if first_byte.is_none() && handle.snapshot().completed_bytes > 0 {
            first_byte = Some(started.elapsed());
        }
        if let Ok(outcome) = tokio::time::timeout(Duration::from_millis(2), &mut join_task).await {
            first_byte.get_or_insert(started.elapsed());
            result = Some(outcome.expect("job task"));
            break;
        }
        if started.elapsed() > Duration::from_secs(300) {
            break;
        }
    }
    let result = result.expect("job completed");
    let wall = started.elapsed();
    let (published, size_ok, hash_ok) = verify_output(&result, size, expected_hash);

    AllocCompareRow {
        label: label.to_string(),
        physical,
        first_byte_ms: first_byte.map(|d| d.as_millis() as u64).unwrap_or(u64::MAX),
        goodput_mib_s: result.completed_bytes as f64
            / wall.as_secs_f64().max(f64::EPSILON)
            / (1024.0 * 1024.0),
        wall_secs: wall.as_secs_f64(),
        cpu_percent: if wall.as_secs_f64() > 0.0 {
            (cpu_time().saturating_sub(cpu0).as_secs_f64() / wall.as_secs_f64()) * 100.0
        } else {
            0.0
        },
        verify: format!("published={published} size_ok={size_ok} hash_ok={hash_ok}"),
    }
}

/// Report emitter for the alloc comparison (task 8.1).
fn emit_alloc_report(rows: &[AllocCompareRow]) {
    let mut out = String::new();
    out.push_str("# Alloc-compare cells (task 8.1)\n\n");
    out.push_str(
        "| Cell | physical | first byte (ms) | Goodput (MiB/s) | Wall (s) | CPU % | Verify |\n|---|---|---|---|---|---|---|\n",
    );
    for r in rows {
        out.push_str(&format!(
            "| {} | {} | {} | {:.2} | {:.2} | {:.1} | {} |\n",
            r.label,
            r.physical,
            r.first_byte_ms,
            r.goodput_mib_s,
            r.wall_secs,
            r.cpu_percent,
            r.verify
        ));
    }
    out.push_str(
        "\nUnavailable axes (labeled, not fabricated): fragmentation (FIEMAP extents) and device-level\nENOSPC injection are not plumbed; ENOSPC/permission surfacing is covered by sink error-path tests.\nCancellation cleanup is covered by the suites re-run in task 8.2, not re-measured here.\n",
    );
    eprintln!("{out}");
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("benches/results/phase-8/alloc-compare");
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::write(dir.join("records.md"), &out);
}

/// One alloc-compare measurement row (task 8.1).
struct AllocCompareRow {
    label: String,
    physical: bool,
    first_byte_ms: u64,
    goodput_mib_s: f64,
    wall_secs: f64,
    cpu_percent: f64,
    verify: String,
}

/// One buffer-sweep measurement row (task 7.4).
struct BufferSweepRow {
    label: String,
    read_buffer_kib: u64,
    goodput_mib_s: f64,
    wall_secs: f64,
    #[allow(dead_code)]
    completed_bytes: u64,
    retries: u64,
    segment_requests: u64,
    cpu_percent: f64,
    peak_rss_kib: u64,
    #[allow(dead_code)]
    rss0_kib: u64,
    context_switches: u64,
    verify: String,
}

/// Report emitter for protocol-compare cells (task 5.4).
fn emit_protocol_report(group: &str, cells: &[ProtocolCellRecord]) {
    let mut out = String::new();
    out.push_str("# Protocol-compare cells (task 5.4)\n\n");
    out.push_str("| Cell | estab H1 | estab H2 | req H1 | H2 streams | CPU % | Peak RSS KiB |\n|---|---|---|---|---|---|---|\n");
    for c in cells {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {:.1} | {} |\n",
            c.label,
            c.establishments_h1,
            c.establishments_h2,
            c.requests_h1,
            c.h2_streams,
            c.cpu_percent,
            c.peak_rss_kib
        ));
    }
    out.push_str("\n## Per-job records\n\n");
    out.push_str("| Job | Goodput (MiB/s) | Wall (s) | Completed | Network | Retries | SegReqs | Verify | maxDesired | maxActive | Desired trace | Fallback |\n|---|---|---|---|---|---|---|---|---|---|---|\n");
    for c in cells {
        for j in &c.jobs {
            out.push_str(&format!(
                "| {} | {:.2} | {:.2} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                j.label,
                j.goodput_mib_s,
                j.wall_secs,
                j.completed_bytes,
                j.network_bytes,
                j.retries,
                j.segment_requests,
                j.verify,
                j.max_desired,
                j.max_active,
                j.desired_trace,
                j.fallback
            ));
        }
    }
    eprintln!("{out}");
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("benches/results")
        .join(group);
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::write(dir.join("records.md"), &out);
}

fn run_write_path_compare() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime");
    let mib = 1024u64 * 1024;
    let reps = 3;
    let mut records: Vec<(String, ResourceRecord)> = Vec::new();
    for protocol in ["h1", "h2"] {
        for shaped in [false, true] {
            let shape_label = if shaped { "shaped" } else { "unshaped" };
            // Shaped runs are slow; use a smaller fixture there.
            let size = if shaped { 64 * mib } else { 256 * mib };
            for pipeline in [false, true] {
                let path_label = if pipeline { "pipelined" } else { "legacy" };
                for rep in 0..reps {
                    records.push((
                        format!("write-path/{protocol}/{shape_label}/{path_label}/rep{rep}"),
                        compare_run(&rt, protocol, size, shaped, false, pipeline, None, rep),
                    ));
                }
            }
        }
    }
    emit_report("write-path-compare", &records);
}

/// Focused storage-axis comparison (task 4.4): unshaped H1/H2 with many
/// repetitions so a constrained destination (rotational disk) yields a
/// distribution instead of one noisy pass.
fn run_storage_compare(dest_root: Option<std::path::PathBuf>) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime");
    let mib = 1024u64 * 1024;
    let size = 128 * mib;
    let reps = 8;
    let mut records: Vec<(String, ResourceRecord)> = Vec::new();
    for protocol in ["h1", "h2"] {
        for (mode_label, adaptive) in [("fixed-4", false), ("adaptive-1-4", true)] {
            for rep in 0..reps {
                records.push((
                    format!("storage/{protocol}/unshaped/{mode_label}/rep{rep}"),
                    compare_run(
                        &rt,
                        protocol,
                        size,
                        false,
                        adaptive,
                        false,
                        dest_root.as_deref(),
                        rep,
                    ),
                ));
            }
        }
    }
    emit_report("storage-compare", &records);
}

/// Fixed vs opt-in adaptive (v2) comparison with repetitions (task 4.4):
/// H1/H2 x shaped/unshaped x fixed-4/adaptive-1-4, optionally on a
/// constrained destination root so the storage axis is a real device.
fn run_adaptive_compare(dest_root: Option<std::path::PathBuf>) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime");
    let mib = 1024u64 * 1024;
    let reps = 3;
    let mut records: Vec<(String, ResourceRecord)> = Vec::new();
    for protocol in ["h1", "h2"] {
        for shaped in [false, true] {
            let shape_label = if shaped { "shaped" } else { "unshaped" };
            // Shaped runs are paced; use a smaller fixture there.
            let size = if shaped { 128 * mib } else { 256 * mib };
            for (mode_label, adaptive) in [("fixed-4", false), ("adaptive-1-4", true)] {
                for rep in 0..reps {
                    records.push((
                        format!("compare/{protocol}/{shape_label}/{mode_label}/rep{rep}"),
                        compare_run(
                            &rt,
                            protocol,
                            size,
                            shaped,
                            adaptive,
                            false,
                            dest_root.as_deref(),
                            rep,
                        ),
                    ));
                }
            }
        }
    }
    emit_report("adaptive-compare", &records);
}

/// One comparison run; adaptive mode probes 1..=4 workers on useful goodput.
#[allow(clippy::too_many_arguments)]
fn compare_run(
    rt: &tokio::runtime::Runtime,
    protocol: &str,
    size: u64,
    shaped: bool,
    adaptive: bool,
    pipeline: bool,
    dest_root: Option<&std::path::Path>,
    rep: usize,
) -> ResourceRecord {
    let threads_before = process_thread_count();
    let record = rt.block_on(async {
        let content = synthetic_fixture(size);
        let expected_hash = content.sha256();
        let mut cfg = EngineConfig::default();
        cfg.transfer.segmentation_threshold = 1;
        cfg.transfer.max_workers = 4;
        cfg.transfer.min_workers = 1;
        cfg.transfer.max_segment_size = 8 * 1024 * 1024;
        if adaptive {
            cfg.transfer.concurrency_mode = kdown_engine::config::ConcurrencyMode::Adaptive;
        }
        cfg.write_executor.pipeline_writes = pipeline;
        cfg.network.response_header_timeout = Duration::from_secs(30);
        cfg.network.read_idle_timeout = Duration::from_secs(30);
        // Per-connection pacing shapes the server (16 MiB/s per 64 KiB chunk
        // delay ≈ the isolated server's throttle behavior).
        let pacing = if shaped {
            Some(Duration::from_micros(4_000))
        } else {
            None
        };
        let (url, _ca) = if protocol == "h1" {
            let addr = start_h1_server_paced(content.clone(), pacing).await;
            (format!("http://{addr}/f.bin"), None)
        } else {
            let (addr, ca_pem) = start_h2_tls_server_paced(content.clone(), pacing, false).await;
            let dir = tempfile::tempdir().expect("ca tmpdir");
            let ca_path = dir.path().join("ca.pem");
            std::fs::write(&ca_path, &ca_pem).expect("write ca");
            cfg.tls.custom_ca_bundle = Some(ca_path);
            cfg.h2_policy = H2ConnectionPolicy::Single;
            (
                format!("https://localhost:{}/f.bin", addr.port()),
                Some(dir),
            )
        };
        // Destination: a fresh tempdir, or a wiped unique subdirectory of the
        // configured root (constrained-storage axis).
        let owned_dir = dest_root
            .is_none()
            .then(|| tempfile::tempdir().expect("dest tmpdir"));
        let dir_path = match (dest_root, &owned_dir) {
            (Some(root), _) => {
                let path = root.join(format!(
                    "kdown-{protocol}-{}-{}-{rep}",
                    if shaped { "shaped" } else { "unshaped" },
                    if adaptive { "adaptive" } else { "fixed" }
                ));
                let _ = std::fs::remove_dir_all(&path);
                std::fs::create_dir_all(&path).expect("dest dir");
                path
            }
            (None, Some(temp)) => temp.path().to_path_buf(),
            (None, None) => unreachable!("tempdir or root"),
        };
        let (result, record) =
            measure_download("compare", &cfg, url, &dir_path, size, &expected_hash, None).await;
        assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
        assert!(
            record.published && record.size_ok && record.hash_ok,
            "compare verification failed: {}",
            record.verification()
        );
        record
    });
    ResourceRecord {
        threads_delta: Some(process_thread_count() as i64 - threads_before as i64),
        ..record
    }
}

/// Process thread count from /proc/self/status (`Threads:`).
fn process_thread_count() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("Threads:"))
                .and_then(|n| n.trim().parse::<u64>().ok())
        })
        .unwrap_or(0)
}

/// HTTP/1.1 fixture server with optional per-chunk pacing (shaped cases).
async fn start_h1_server_paced(
    content: ContentSource,
    pacing: Option<Duration>,
) -> std::net::SocketAddr {
    // Reuse the unpaced server when no shaping is requested.
    let Some(pacing) = pacing else {
        return start_h1_server(content).await;
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let content = content.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                loop {
                    let n = match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    let req = String::from_utf8_lossy(&buf[..n]).to_string();
                    let is_head = req.starts_with("HEAD");
                    let range = req
                        .lines()
                        .find_map(|l| {
                            let (name, value) = l.split_once(':')?;
                            if !name.eq_ignore_ascii_case("range") {
                                return None;
                            }
                            let value = value.trim().strip_prefix("bytes=")?;
                            value.split_once('-')
                        })
                        .and_then(|(s, e)| {
                            Some((s.trim().parse::<u64>().ok()?, e.trim().parse::<u64>().ok()?))
                        });
                    let (status, extra, range) = match range {
                        Some((s, e)) => {
                            let end = e.min(content.len() - 1);
                            (
                                "206 Partial Content",
                                format!("content-range: bytes {s}-{end}/{}\r\n", content.len()),
                                Some((s, end)),
                            )
                        }
                        None => ("200 OK", String::new(), Some((0, content.len() - 1))),
                    };
                    let body_len = range.map_or(0, |(s, e)| e - s + 1);
                    let head = format!(
                        "HTTP/1.1 {status}\r\n{extra}etag: \"bench-paced\"\r\naccept-ranges: bytes\r\ncontent-length: {body_len}\r\n\r\n",
                    );
                    if socket.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    if is_head {
                        continue;
                    }
                    if let Some((s, e)) = range {
                        let mut off = s;
                        while off <= e {
                            let take = (e - off + 1).min(64 * 1024);
                            tokio::time::sleep(pacing).await;
                            if socket
                                .write_all(&content.read_range(off, off + take - 1))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            off += take;
                        }
                    }
                }
            });
        }
    });
    addr
}

/// One sweep run; `explicit_target_bytes` set → Explicit sizing, otherwise
/// `oversubscription` drives Automatic sizing.
#[allow(clippy::too_many_arguments)]
fn sweep_run(
    rt: &tokio::runtime::Runtime,
    protocol: &str,
    size: u64,
    workers: u32,
    explicit_target_bytes: Option<u64>,
    oversubscription: u64,
    duration_ms: Option<u64>,
    ready_factor: u64,
    shaped: bool,
) -> ResourceRecord {
    rt.block_on(async {
        let content = synthetic_fixture(size);
        let expected_hash = content.sha256();
        let mut cfg = EngineConfig::default();
        cfg.transfer.segmentation_threshold = 1;
        cfg.transfer.max_workers = workers;
        cfg.transfer.min_workers = workers.min(2);
        if let Some(duration_ms) = duration_ms {
            cfg.transfer.segment_sizing =
                kdown_engine::config::SegmentSizing::Duration { duration_ms };
            // The ready-work divisor is auto_oversubscription x desired
            // (task 3.3); the sweep axis varies the factor directly.
            cfg.transfer.auto_oversubscription = ready_factor.max(1);
        } else if let Some(target) = explicit_target_bytes {
            cfg.transfer.segment_sizing = kdown_engine::config::SegmentSizing::Explicit;
            cfg.transfer.initial_segment_size =
                target.clamp(cfg.transfer.min_segment_size, cfg.transfer.max_segment_size);
        } else {
            cfg.transfer.segment_sizing = kdown_engine::config::SegmentSizing::Automatic;
            cfg.transfer.auto_oversubscription = oversubscription;
        }
        cfg.network.response_header_timeout = Duration::from_secs(30);
        cfg.network.read_idle_timeout = Duration::from_secs(30);
        // WAN shape (task 3.5): per-connection pacing (~16 MiB/s) on the
        // in-process fixture, matching the phase-1 shaped axis.
        let pacing = if shaped {
            Some(Duration::from_micros(4_000))
        } else {
            None
        };
        let (url, _ca) = if protocol == "h1" {
            let addr = start_h1_server_paced(content.clone(), pacing).await;
            (format!("http://{addr}/f.bin"), None)
        } else {
            let (addr, ca_pem) = start_h2_tls_server_paced(content.clone(), pacing, false).await;
            let dir = tempfile::tempdir().expect("ca tmpdir");
            let ca_path = dir.path().join("ca.pem");
            std::fs::write(&ca_path, &ca_pem).expect("write ca");
            cfg.tls.custom_ca_bundle = Some(ca_path);
            cfg.h2_policy = H2ConnectionPolicy::Single;
            (
                format!("https://localhost:{}/f.bin", addr.port()),
                Some(dir),
            )
        };
        let dir = tempfile::tempdir().expect("dest tmpdir");
        let (result, record) =
            measure_download("sweep", &cfg, url, dir.path(), size, &expected_hash, None).await;
        assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
        assert!(
            record.published && record.size_ok && record.hash_ok,
            "sweep verification failed: {}",
            record.verification()
        );
        record
    })
}

/// Size parsing for the client mode (mirrors the server binary).
fn parse_size_arg(v: &str) -> Option<u64> {
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

// Keep unused-import warnings away: ProxyConfig and TlsConfig are part of
// the scenario matrix documented for §37 (proxy/TLS scenarios land with
// their hardening tasks).
#[allow(unused)]
fn _scenario_docs(_p: ProxyConfig, _t: TlsConfig) {}
