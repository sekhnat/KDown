//! Deterministic misbehaving-HTTP test server (`KDownSpec.md` §36.2).
//!
//! Supports three registration styles:
//! 1. Static content with correct or lying range behavior (`serve_static` /
//!    `serve_with_ranges`).
//! 2. Scripted fixed responses counted down (`serve_n`) before falling
//!    through to a fallback.
//! 3. Arbitrary handler closures over the parsed request (`serve_handler`),
//!    for stateful behaviors such as mid-download ETag flips.
//!
//! The server speaks raw HTTP/1.1 on an ephemeral 127.0.0.1 port, which
//! gives it full control over protocol-level misbehavior (truncated bodies,
//! abrupt resets, malformed headers) that a normal framework would not
//! allow sending.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// A scripted response.
#[derive(Clone)]
pub struct ScriptedResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Sparse virtual body: `body` stays empty and the sender materializes
    /// deterministic bytes lazily per chunk from `fill` (used for the
    /// >4 GiB sparse test, §36.6, avoiding multi-GiB fixtures).
    pub sparse_len: Option<u64>,
    pub sparse_fill: Option<SparseFill>,
    /// Send the body in 64 KiB chunks with this delay between them.
    pub chunk_delay: Option<Duration>,
    /// Delay before sending the response head.
    pub header_delay: Option<Duration>,
    /// Abruptly close the connection after this many body bytes
    /// (simulates mid-transfer disconnect).
    pub reset_after: Option<usize>,
    /// Omit Content-Length and stream with `Transfer-Encoding: chunked`
    /// (unknown total length, §25/§36.2).
    pub omit_content_length: bool,
}

impl std::fmt::Debug for ScriptedResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptedResponse")
            .field("status", &self.status)
            .field("body_len", &self.logical_len())
            .field("sparse", &self.sparse_len.is_some())
            .field("reset_after", &self.reset_after)
            .field("omit_content_length", &self.omit_content_length)
            .finish()
    }
}

/// Deterministic filler for sparse bodies: `f(global_offset) -> byte`.
pub type SparseFill = Arc<dyn Fn(u64) -> u8 + Send + Sync>;

impl ScriptedResponse {
    #[must_use]
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: vec![],
            body: vec![],
            sparse_len: None,
            sparse_fill: None,
            chunk_delay: None,
            header_delay: None,
            reset_after: None,
            omit_content_length: false,
        }
    }

    #[must_use]
    pub fn ok(body: Vec<u8>) -> Self {
        Self::new(200).with_body(body)
    }

    #[must_use]
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }

    /// Sparse virtual body of `len` bytes materialized lazily by `fill`
    /// (§36.6 >4 GiB test). Inclusive range responses slice it by offset.
    #[must_use]
    pub fn sparse(mut self, len: u64, fill: SparseFill) -> Self {
        self.sparse_len = Some(len);
        self.sparse_fill = Some(fill);
        self
    }

    /// The logical length of the response body (sparse or materialized).
    #[must_use]
    pub fn logical_len(&self) -> u64 {
        self.sparse_len.unwrap_or(self.body.len() as u64)
    }

    #[must_use]
    pub fn with_header(mut self, k: &str, v: &str) -> Self {
        self.headers.push((k.to_ascii_lowercase(), v.to_string()));
        self
    }

    #[must_use]
    pub fn reset_after(mut self, n: u64) -> Self {
        self.reset_after = Some(n as usize);
        self
    }

    #[must_use]
    pub fn chunked(mut self, delay: Duration) -> Self {
        self.chunk_delay = Some(delay);
        self
    }

    #[must_use]
    pub fn delayed_headers(mut self, delay: Duration) -> Self {
        self.header_delay = Some(delay);
        self
    }

    /// Unknown length: omit Content-Length and stream chunked (§25).
    #[must_use]
    pub fn unknown_length(mut self) -> Self {
        self.omit_content_length = true;
        self
    }
}

/// Parsed subset of the request the scripts need.
#[derive(Debug, Clone, Default)]
pub struct RequestInfo {
    pub method: String,
    pub path: String,
    /// Parsed `Range: bytes=S-E` inclusive.
    pub range: Option<(u64, u64)>,
    pub if_range: Option<String>,
    /// Header names lowercased.
    pub headers: Vec<(String, String)>,
}

impl RequestInfo {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }
}

type Handler = Arc<dyn Fn(&RequestInfo) -> ScriptedResponse + Send + Sync>;
type ExtraHeaders = Arc<dyn Fn(&mut Vec<(String, String)>) + Send + Sync>;

#[derive(Default)]
struct ServerState {
    /// Path -> handler. Handlers take priority over everything else.
    handlers: Mutex<HashMap<String, Handler>>,
    /// Path prefix -> (remaining fires, response template).
    scripts: Mutex<Vec<(String, u32, ScriptedResponse)>>,
    /// Path -> response for unlimited repeats.
    fallbacks: Mutex<HashMap<String, Handler>>,
    /// Path -> extra response headers.
    default_headers: Mutex<HashMap<String, ExtraHeaders>>,
    requests: Mutex<Vec<RequestInfo>>,
}

/// Builder for a deterministic scripted HTTP server.
#[derive(Default)]
pub struct TestServer {
    handlers: HashMap<String, Handler>,
    scripts: Vec<(String, u32, ScriptedResponse)>,
    default_headers: HashMap<String, ExtraHeaders>,
}

impl TestServer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Script `times` responses for `path` (prefix match), then fall back.
    #[must_use]
    pub fn serve_n(mut self, path: &str, times: u32, response: ScriptedResponse) -> Self {
        self.scripts.push((path.to_string(), times, response));
        self
    }

    /// Serve `handler` for `path` (exact match) indefinitely.
    #[must_use]
    pub fn serve_handler(
        mut self,
        path: &str,
        handler: impl Fn(&RequestInfo) -> ScriptedResponse + Send + Sync + 'static,
    ) -> Self {
        self.handlers.insert(path.to_string(), Arc::new(handler));
        self
    }

    /// Serve static bytes with correct range support at `path`.
    #[must_use]
    pub fn serve_static(self, path: &str, content: Vec<u8>) -> Self {
        self.serve_ranges(path, content, RangeMode::Correct)
    }

    /// Serve static bytes with a range-behavior mode (§36.2).
    #[must_use]
    pub fn serve_ranges(mut self, path: &str, content: Vec<u8>, mode: RangeMode) -> Self {
        let owned = Arc::new(content);
        let mode = Arc::new(Mutex::new(mode));
        let mode_for_handler = mode.clone();
        self = self.serve_handler(path, move |req| {
            let mode = *mode_for_handler.lock().expect("mode lock");
            let len = owned.len() as u64;
            match (req.range, mode) {
                (Some((s, e)), RangeMode::Correct) => {
                    let end = e.min(len.saturating_sub(1));
                    let body = owned[s as usize..=(end as usize)].to_vec();
                    ScriptedResponse::new(206)
                        .with_body(body)
                        .with_header("content-range", &format!("bytes {s}-{end}/{len}"))
                }
                // Lying server: advertises ranges but answers 200 with the
                // FULL body, ignoring the range.
                (Some(_), RangeMode::Full200) => ScriptedResponse::ok((*owned).clone()),
                // Advertises ranges but returns a malformed Content-Range.
                (Some((s, _)), RangeMode::MalformedContentRange) => ScriptedResponse::new(206)
                    .with_body(vec![b'X'])
                    .with_header("content-range", &format!("bytes {}-{}/bogus", s + 1, s + 5)),
                // Range requested but never honored: full body either way.
                (Some(_), RangeMode::NoRanges) | (None, _) => {
                    ScriptedResponse::ok((*owned).clone())
                }
            }
        });
        self = self.with_default_headers(path, move |headers| {
            let mode = *mode.lock().expect("mode lock");
            if mode != RangeMode::NoRanges {
                headers.push(("accept-ranges".to_string(), "bytes".to_string()));
            }
        });
        self
    }

    /// Extra headers always attached to responses for `path`.
    #[must_use]
    pub fn with_default_headers(
        mut self,
        path: &str,
        add: impl Fn(&mut Vec<(String, String)>) + Send + Sync + 'static,
    ) -> Self {
        let add = Arc::new(add);
        self.default_headers
            .insert(path.to_string(), add);
        self
    }

    /// Bind and spawn; returns the running handle.
    pub async fn start(self) -> Result<RunningServer, std::io::Error> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let state = Arc::new(ServerState {
            handlers: Mutex::new(self.handlers),
            scripts: Mutex::new(self.scripts),
            fallbacks: Mutex::new(HashMap::new()),
            default_headers: Mutex::new(self.default_headers),
            requests: Mutex::new(Vec::new()),
        });
        let loop_state = state.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((socket, _)) => {
                        let st = loop_state.clone();
                        tokio::spawn(async move {
                            let _ = serve_conn(socket, st).await;
                        });
                    }
                    Err(_) => return,
                }
            }
        });
        Ok(RunningServer {
            addr,
            state,
            _shutdown: Arc::new(tokio::sync::Notify::new()),
        })
    }
}

/// Range behavior of `serve_ranges` (§36.2 scenarios).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RangeMode {
    /// Correct 206 + Content-Range.
    #[default]
    Correct,
    /// Ignore Range; return 200 with the full body.
    Full200,
    /// Return 206 with a malformed Content-Range.
    MalformedContentRange,
    /// Never advertise ranges.
    NoRanges,
}

/// A live scripted server.
pub struct RunningServer {
    addr: SocketAddr,
    state: Arc<ServerState>,
    _shutdown: Arc<tokio::sync::Notify>,
}

impl RunningServer {
    /// Base URL pointing at this server.
    #[must_use]
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    /// The bound address (for HTTPS-variant tests that rewrite the scheme).
    #[must_use]
    pub fn socket_addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    /// All requests seen so far, in order.
    pub async fn requests(&self) -> Vec<RequestInfo> {
        self.state.requests.lock().expect("requests lock").clone()
    }

    /// Number of requests seen for `path`.
    pub async fn request_count(&self, path: &str) -> usize {
        self.state
            .requests
            .lock()
            .expect("requests lock")
            .iter()
            .filter(|r| r.path == path)
            .count()
    }
}

impl std::fmt::Debug for RunningServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningServer")
            .field("addr", &self.addr)
            .finish()
    }
}

fn parse_range_header(v: &str) -> Option<(u64, u64)> {
    let rest = v.strip_prefix("bytes=")?;
    let (s, e) = rest.split_once('-')?;
    Some((s.parse().ok()?, e.parse().ok()?))
}

fn resolve_response(state: &ServerState, info: &RequestInfo) -> ScriptedResponse {
    // 1. Explicit handler wins.
    if let Some(h) = state.handlers.lock().expect("handlers lock").get(&info.path) {
        return h(info);
    }
    // 2. Scripted behaviors (fire `times` each, prefix match).
    {
        let mut scripts = state.scripts.lock().expect("scripts lock");
        if let Some(i) = scripts
            .iter()
            .position(|(p, times, _)| *times > 0 && info.path.starts_with(p))
        {
            scripts[i].1 -= 1;
            return scripts[i].2.clone();
        }
    }
    // 3. Fallbacks, else 404.
    state
        .fallbacks
        .lock()
        .expect("fallbacks lock")
        .get(&info.path)
        .map(|h| h(info))
        .unwrap_or_else(|| ScriptedResponse::new(404))
}

async fn serve_conn(
    socket: tokio::net::TcpStream,
    state: Arc<ServerState>,
) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(()); // client closed
        }
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let target = parts.next().unwrap_or("").to_string();
        if method.is_empty() || target.is_empty() {
            return Ok(());
        }

        let mut headers = Vec::new();
        loop {
            let mut h = String::new();
            reader.read_line(&mut h).await?;
            let trimmed = h.trim_end();
            if trimmed.is_empty() {
                break;
            }
            if let Some((k, v)) = trimmed.split_once(':') {
                headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
            }
        }

        let path = target.split('?').next().unwrap_or("").to_string();
        let info = RequestInfo {
            method: method.clone(),
            path: path.clone(),
            range: headers
                .iter()
                .find(|(k, _)| k == "range")
                .and_then(|(_, v)| parse_range_header(v)),
            if_range: headers
                .iter()
                .find(|(k, _)| k == "if-range")
                .map(|(_, v)| v.clone()),
            headers,
        };
        state.requests.lock().expect("requests lock").push(info.clone());

        let mut resp = resolve_response(&state, &info);
        {
            let extras = state.default_headers.lock().expect("extras lock");
            if let Some(add) = extras.get(&path) {
                add(&mut resp.headers);
            }
        }

        if let Some(delay) = resp.header_delay {
            tokio::time::sleep(delay).await;
        }
        write_response(&mut writer, &resp, &method).await?;
        if resp.reset_after.is_some() {
            // Abrupt reset: close without clean shutdown.
            return Ok(());
        }
    }
}

async fn write_response(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    resp: &ScriptedResponse,
    method: &str,
) -> std::io::Result<()> {
    let reason = match resp.status {
        200 => "OK",
        206 => "Partial Content",
        302 => "Found",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        408 => "Request Timeout",
        429 => "Too Many Requests",
        500 | 502 | 503 | 504 => "Server Error",
        _ => "OK",
    };
    let mut head = format!("HTTP/1.1 {} {reason}\r\n", resp.status);
    for (k, v) in &resp.headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    // Always declare content-length: truncated responses keep the declared
    // length and die mid-body, which is exactly how real servers fail
    // (§36.2 short/truncated bodies -> hyper reports an incomplete body).
    if resp.omit_content_length {
        head.push_str("transfer-encoding: chunked\r\n");
        head.push_str("\r\n");
        writer.write_all(head.as_bytes()).await?;
        if method == "HEAD" {
            return writer.flush().await;
        }
        // Chunked framing: <hex-size>\r\n<data>\r\n ... 0\r\n\r\n.
        if let (Some(len), Some(fill)) = (resp.sparse_len, &resp.sparse_fill) {
            // Sparse chunked: materialize 16 KiB at a time; the reset cut
            // applies to the logical byte count.
            let limit = match resp.reset_after {
                Some(n) => (n as u64).min(len),
                None => len,
            };
            let mut off = 0u64;
            while off < limit {
                let n = 16 * 1024u64.min(limit - off);
                let chunk: Vec<u8> = (off..off + n).map(|i| fill(i)).collect();
                writer
                    .write_all(format!("{n:x}\r\n").as_bytes())
                    .await?;
                writer.write_all(&chunk).await?;
                writer.write_all(b"\r\n").await?;
                off += n;
            }
        } else {
            let bytes: &[u8] = match resp.reset_after {
                Some(n) => &resp.body[..n.min(resp.body.len())],
                None => &resp.body,
            };
            for chunk in bytes.chunks(16 * 1024) {
                writer
                    .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                    .await?;
                writer.write_all(chunk).await?;
                writer.write_all(b"\r\n").await?;
            }
        }
        writer.write_all(b"0\r\n\r\n").await?;
        return writer.flush().await;
    }
    // Content-Length comes from the logical body (sparse or materialized).
    let logical_len = resp.logical_len();
    head.push_str(&format!("content-length: {logical_len}\r\n"));
    head.push_str("\r\n");
    writer.write_all(head.as_bytes()).await?;

    if method == "HEAD" {
        return writer.flush().await;
    }

    // Sparse bodies: stream deterministic materialized chunks; a reset cut
    // truncates the stream (§36.2 truncated bodies).
    if let (Some(len), Some(fill)) = (resp.sparse_len, &resp.sparse_fill) {
        let limit = match resp.reset_after {
            Some(n) => (n as u64).min(len),
            None => len,
        };
        let mut off = 0u64;
        while off < limit {
            let n = 64 * 1024u64.min(limit - off);
            let chunk: Vec<u8> = (off..off + n).map(|i| fill(i)).collect();
            writer.write_all(&chunk).await?;
            if let Some(delay) = resp.chunk_delay {
                writer.flush().await?;
                tokio::time::sleep(delay).await;
            }
            off += n;
        }
        return writer.flush().await;
    }

    let bytes: &[u8] = match resp.reset_after {
        Some(n) => &resp.body[..n.min(resp.body.len())],
        None => &resp.body,
    };

    match resp.chunk_delay {
        Some(delay) if !bytes.is_empty() => {
            for chunk in bytes.chunks(64 * 1024) {
                writer.write_all(chunk).await?;
                writer.flush().await?;
                tokio::time::sleep(delay).await;
            }
        }
        _ => writer.write_all(bytes).await?,
    }
    writer.flush().await
}