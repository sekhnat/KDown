//! Process-isolated fixture server (task 1.4).
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
//!   LISTENING <addr>   — bind address to point clients at
//!   SIZE <bytes>       — synthetic file length
//!   SHA256 <hex>       — expected content digest (generator parity check)
//!   READY              — servers are accepting
//!
//! The server serves until killed. Plaintext HTTP/1.1 only: isolated
//! comparisons target architecture (output path, scheduling, storage), not
//! TLS/H2 handshakes; H2 stays covered by the in-process smoke scenarios.

use std::io::Write as _;
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
         [--reset-after-bytes N] [--change-etag-after N]"
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
    // Materialize (flag, value) pairs first: flags take exactly one value,
    // boolean flags none. No closure borrow conflicts, no arg-parsing deps.
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut it = raw.into_iter();
    while let Some(arg) = it.next() {
        let takes_value = !matches!(
            arg.as_str(),
            "--ignore-ranges"
        );
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
            _ => usage(),
        }
    }
    let cfg = ServerConfig {
        size,
        seed,
        ignore_ranges,
        throttle_mib_s: throttle,
        transient_fail,
        reset_after_bytes: reset_after,
        change_etag_after,
    };
    (addr.unwrap_or_else(|| "127.0.0.1:0".parse().expect("default addr")), cfg)
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
        etag: "iso".to_string(),
    }));
    let cfg = std::sync::Arc::new(cfg);

    loop {
        let Ok((socket, _)) = listener.accept().await else {
            return;
        };
        let state = state.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            serve_connection(socket, state, cfg).await;
        });
    }
}

/// Read one HTTP/1.1 request head (headers until CRLFCRLF), returning the
/// parsed Range and the request line for logging/diagnostics.
async fn read_request(
    socket: &mut TcpStream,
) -> Option<(Option<(u64, u64)>, String, bool)> {
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
    state: std::sync::Arc<tokio::sync::Mutex<ServerState>>,
    cfg: std::sync::Arc<ServerConfig>,
) {
    loop {
        let Some((range, request_line, close_after)) = read_request(&mut socket).await else {
            return;
        };
        let is_head = request_line.starts_with("HEAD");
        if std::env::var("FIXTURE_SERVER_TRACE").is_ok() {
            eprintln!("[fixture_server] {} range={:?}", request_line, range);
        }

        // Validator flip: applied after the configured number of served
        // responses. The flip happens between responses, so requests after
        // the boundary observe a changed ETag.
        let etag = {
            let mut st = state.lock().await;
            if let Some(after) = cfg.change_etag_after {
                if st.served >= after && st.etag == "iso" {
                    st.etag = "iso-v2".to_string();
                }
            }
            st.etag_header()
        };

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
            if stream_body(&mut socket, &cfg, start, end).await.is_err() {
                return;
            }
        } else {
            // Transient failure response: structured status + Retry-After: 0.
            let head = format!(
                "HTTP/1.1 {status} Transient\r\n{etag}retry-after: 0\r\ncontent-length: 0\r\n\r\n"
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
            let delay = Duration::from_secs_f64(
                take_in_block as f64 / bytes_per_sec.max(f64::EPSILON),
            );
            tokio::time::sleep(delay).await;
        }
    }
    Ok(())
}

