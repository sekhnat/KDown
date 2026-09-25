//! Connection layer: TLS, proxying, per-origin/global connection limits,
//! and the SSRF restriction hook (§27, §28, §21.5; tasks 6.1, 7.1).
//!
//! The connector is a `tower::Service<Uri>` usable directly by
//! hyper-util's legacy client. Every connection holds one global and one
//! per-origin permit for its whole lifetime (pooled idle included, §27.2),
//! passes the embedding layer's [`crate::config::AddressFilter`] before
//! connecting (§21.5), and applies the proxy policy (§28):
//! - HTTP proxy + http target: TCP to the proxy; hyper writes absolute-form
//!   request targets because the connector reports `Connected::proxy(true)`.
//! - HTTP proxy + https target: CONNECT tunnel, then TLS (§28).
//! - SOCKS5: hook extension via hyper-util's `SocksV5` (§28).
//! - Direct otherwise; TLS via rustls with validation on and no plaintext
//!   fallback (§21.1).

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use hyper_util::client::legacy::connect::Connected;
use hyper_util::rt::TokioIo;
use tower_service::Service;
use tracing::debug;

use crate::config::{ConnectionTarget, ProxyConfig, TlsConfig};
use crate::error::DownloadError;
use crate::redact::Redactor;

/// A TCP or TLS stream with its connection-limit permits attached: the
/// permits live until the stream (and any pool idle time) is dropped, so
/// concurrent connection counts include pooled idle connections (§27.2).
/// `Connection::connected()` rebuilds the metadata hyper-util's pool keys
/// on (H2 multiplexing, absolute-form proxy targets) from stored flags.
pub struct LimitedConn {
    io: ConnIo,
    /// Connector reported a proxy path (§28): absolute-form targets.
    proxied: bool,
    /// h2 requested via ALPN (§24): report `negotiated_h2` when the TLS
    /// handshake actually negotiated it.
    alpn_h2_requested: bool,
    _permits: LimitPermits,
}

enum ConnIo {
    Plain(tokio::net::TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>),
}

impl LimitedConn {
    fn new(io: ConnIo, proxied: bool, alpn_h2_requested: bool, permits: LimitPermits) -> Self {
        Self {
            io,
            proxied,
            alpn_h2_requested,
            _permits: permits,
        }
    }

    fn plain(tcp: tokio::net::TcpStream, proxied: bool, permits: LimitPermits) -> Self {
        Self::new(ConnIo::Plain(tcp), proxied, false, permits)
    }

    fn tls(
        tls: tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
        proxied: bool,
        alpn_h2_requested: bool,
        permits: LimitPermits,
    ) -> Self {
        Self::new(
            ConnIo::Tls(Box::new(tls)),
            proxied,
            alpn_h2_requested,
            permits,
        )
    }
}

impl tokio::io::AsyncRead for LimitedConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut self.io {
            ConnIo::Plain(s) => Pin::new(s).poll_read(cx, buf),
            ConnIo::Tls(s) => {
                // Many HTTP servers close TCP without TLS `close_notify`
                // (RFC 8446 §6.5.1: "not required"). rustls surfaces that
                // EOF as UnexpectedEof once buffered plaintext is drained;
                // a download treats it as clean end-of-body (§13: the body
                // length was already validated against the request, §11.2,
                // and integrity is caller-verified §16). This is teardown
                // tolerance, not a certificate/trust bypass (§21.1).
                match Pin::new(s).poll_read(cx, buf) {
                    Poll::Ready(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        Poll::Ready(Ok(()))
                    }
                    other => other,
                }
            }
        }
    }
}

impl tokio::io::AsyncWrite for LimitedConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut self.io {
            ConnIo::Plain(s) => Pin::new(s).poll_write(cx, buf),
            ConnIo::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut self.io {
            ConnIo::Plain(s) => Pin::new(s).poll_flush(cx),
            ConnIo::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut self.io {
            ConnIo::Plain(s) => Pin::new(s).poll_shutdown(cx),
            ConnIo::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl hyper_util::client::legacy::connect::Connection for LimitedConn {
    fn connected(&self) -> Connected {
        let mut c = Connected::new();
        if self.proxied {
            c = c.proxy(true);
        }
        if self.alpn_h2_requested {
            if let ConnIo::Tls(t) = &self.io {
                if t.get_ref().1.alpn_protocol() == Some(b"h2") {
                    c = c.negotiated_h2();
                }
            }
        }
        c
    }
}

/// Connector failure carrying the structured category.
#[derive(Debug)]
pub struct ConnectError(pub DownloadError);

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "connect error: {}", self.0)
    }
}
impl std::error::Error for ConnectError {}

impl From<std::io::Error> for ConnectError {
    fn from(e: std::io::Error) -> Self {
        Self(DownloadError::from_io(&e))
    }
}

impl From<ConnectError> for DownloadError {
    fn from(e: ConnectError) -> Self {
        e.0
    }
}

/// TLS trust configuration resolved once per transport (§21.1).
#[derive(Debug)]
pub(crate) struct TlsSettings {
    config: Arc<rustls::ClientConfig>,
}

impl TlsSettings {
    /// Build TLS client config: platform roots by default, optional custom
    /// CA bundle, hostname validation always on (§21.1).
    ///
    /// # Errors
    /// [`DownloadError::Tls`] when the trust set cannot be loaded; the
    /// engine fails closed and never falls back to plaintext (§21.1).
    pub fn new(tls: &TlsConfig, alpn_h2: bool) -> Result<Self, DownloadError> {
        let mut roots = rustls::RootCertStore::empty();
        match &tls.custom_ca_bundle {
            Some(path) => {
                let pem = std::fs::read(path).map_err(|e| {
                    DownloadError::Tls(format!("read CA bundle {}: {e}", path.display()))
                })?;
                let mut ok = 0usize;
                for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
                    let cert =
                        cert.map_err(|e| DownloadError::Tls(format!("parse CA bundle: {e}")))?;
                    roots
                        .add(cert)
                        .map_err(|e| DownloadError::Tls(format!("add CA cert: {e}")))?;
                    ok += 1;
                }
                if ok == 0 {
                    return Err(DownloadError::Tls(
                        "CA bundle contained no certificates".into(),
                    ));
                }
            }
            None => {
                let loaded = rustls_native_certs::load_native_certs();
                for e in &loaded.errors {
                    tracing::warn!("platform root load warning: {e}");
                }
                for cert in loaded.certs {
                    let _ = roots.add(cert);
                }
                if roots.is_empty() {
                    return Err(DownloadError::Tls(
                        "no platform root certificates found".into(),
                    ));
                }
            }
        }
        let provider = rustls::crypto::ring::default_provider();
        let config = rustls::ClientConfig::builder_with_provider(provider.into())
            .with_safe_default_protocol_versions()
            .map_err(|e| DownloadError::Tls(format!("protocol versions: {e}")))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let mut config = config;
        // ALPN prefers h2 only when the caller opts in (§24: multiplexing
        // is a negotiated capability, never an assumption).
        config.alpn_protocols = if alpn_h2 {
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        } else {
            vec![b"http/1.1".to_vec()]
        };
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

/// Connection-limits registry: one global semaphore and per-origin
/// semaphores (§27.2). Keys include the proxy address so proxied and
/// direct connections never share a bucket (§27.1 pool keys).
#[derive(Debug)]
pub struct ConnectionLimits {
    global: Arc<tokio::sync::Semaphore>,
    per_origin: u32,
    origins: Mutex<std::collections::HashMap<String, Arc<tokio::sync::Semaphore>>>,
}

impl ConnectionLimits {
    #[must_use]
    pub fn new(max_total: u32, max_per_origin: u32) -> Self {
        Self {
            global: Arc::new(tokio::sync::Semaphore::new(max_total.max(1) as usize)),
            per_origin: max_per_origin.max(1),
            origins: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Acquire one global and one per-origin permit; both release on drop.
    pub(crate) async fn acquire(&self, origin_key: &str) -> LimitPermits {
        // Global first, then origin: an origin can never hold more global
        // permits than origin permits.
        let global = self
            .global
            .clone()
            .acquire_owned()
            .await
            .expect("global semaphore open");
        let origin = {
            let mut map = self.origins.lock().expect("origin map");
            map.entry(origin_key.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(self.per_origin as usize)))
                .clone()
        };
        let permit = origin
            .clone()
            .acquire_owned()
            .await
            .expect("origin semaphore open");
        LimitPermits {
            _global: global,
            _origin: permit,
        }
    }

    /// Live connection count for an origin key (used by tests/observability).
    #[must_use]
    pub fn origin_in_use(&self, origin_key: &str) -> u32 {
        let map = self.origins.lock().expect("origin map");
        let cap = self.per_origin;
        map.get(origin_key)
            .map(|s| cap - s.available_permits() as u32)
            .unwrap_or(0)
    }
}

/// RAII permits held for the life of one connection.
#[derive(Debug)]
pub(crate) struct LimitPermits {
    _global: tokio::sync::OwnedSemaphorePermit,
    _origin: tokio::sync::OwnedSemaphorePermit,
}

/// Negotiated wire protocol of one HTTP connection (§24): `Http2` only
/// when actually negotiated (TLS ALPN `h2`), never assumed from
/// configuration. Instrumentation labels, not connection policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpProtocol {
    /// HTTP/1.x over its own connection (one request at a time).
    Http1,
    /// HTTP/2: many multiplexed streams over one connection.
    Http2,
}

/// Protocol-level transport instrumentation (task 5.1): logical HTTP
/// requests/H2 streams counted separately from physical TCP/TLS
/// establishments, each labeled with its negotiated protocol.
///
/// Counters are monotonically increasing process-wide for one transport
/// stack (connector + clients share one `Arc`). Physical establishments
/// increment exactly when the connector dials a new socket — pooled reuse
/// never re-enters the connector — so `establishments_*` reconcile with a
/// server's accepted-connection count, while `requests_h1`/`h2_streams`
/// reconcile with a server's served-request count (H2 requests arrive as
/// multiplexed streams).
///
/// H2 flow-control stall/window data is **not** observable through
/// hyper-util's legacy client: [`Self::h2_flow_control_wait`] reports
/// `None` and [`H2_FLOW_CONTROL_INSTRUMENTED`] is `false`, so reports must
/// label that axis unavailable rather than fabricate values (observability
/// contract).
#[derive(Debug, Default)]
pub struct HttpProtocolStats {
    establishments_h1: std::sync::atomic::AtomicU64,
    establishments_h2: std::sync::atomic::AtomicU64,
    requests_h1: std::sync::atomic::AtomicU64,
    requests_h2: std::sync::atomic::AtomicU64,
}

/// Whether H2 flow-control windows/stall waits AND peer stream limits
/// (SETTINGS_MAX_CONCURRENT_STREAMS) are instrumented. The hyper-util
/// legacy client exposes neither, so this is `false` and dependent reports
/// are labeled unavailable (task 5.1/5.3: automatic extra H2 sockets stay
/// off without measured flow-control/stream-limit evidence; peer stream
/// limits cannot be probed where not exposed).
pub const H2_FLOW_CONTROL_INSTRUMENTED: bool = false;

impl HttpProtocolStats {
    /// Record one physical TCP/TLS establishment with its negotiated
    /// protocol. Called only from a successful connector dial.
    pub(crate) fn record_establishment(&self, protocol: HttpProtocol) {
        match protocol {
            HttpProtocol::Http1 => {
                self.establishments_h1
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            HttpProtocol::Http2 => {
                self.establishments_h2
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Record one completed HTTP request. On HTTP/2 every request is one
    /// multiplexed stream; on HTTP/1.x it occupies its connection.
    pub(crate) fn record_request(&self, protocol: HttpProtocol) {
        match protocol {
            HttpProtocol::Http1 => {
                self.requests_h1
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            HttpProtocol::Http2 => {
                self.requests_h2
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Physical TCP/TLS establishments negotiated to HTTP/1.x.
    #[must_use]
    pub fn establishments_h1(&self) -> u64 {
        self.establishments_h1
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Physical TCP/TLS establishments negotiated to HTTP/2.
    #[must_use]
    pub fn establishments_h2(&self) -> u64 {
        self.establishments_h2
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// All physical establishments regardless of negotiated protocol.
    #[must_use]
    pub fn establishments_total(&self) -> u64 {
        self.establishments_h1()
            .saturating_add(self.establishments_h2())
    }

    /// HTTP/1.x requests served (each occupies its connection).
    #[must_use]
    pub fn requests_h1(&self) -> u64 {
        self.requests_h1.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// HTTP/2 requests served — each is one multiplexed stream on a shared
    /// connection, so this counts streams, not sockets.
    #[must_use]
    pub fn h2_streams(&self) -> u64 {
        self.requests_h2.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// All logical requests across protocols.
    #[must_use]
    pub fn requests_total(&self) -> u64 {
        self.requests_h1().saturating_add(self.h2_streams())
    }

    /// Observed H2 flow-control stall wait of the last blocked stream, when
    /// the transport can observe it. Always `None` today: hyper-util's
    /// legacy client does not expose per-stream flow-control windows, so
    /// this axis is labeled unavailable instead of fabricated.
    #[must_use]
    pub fn h2_flow_control_wait(&self) -> Option<Duration> {
        None
    }
}

/// Origin key for limits and pooling (§27.1).
pub(crate) fn origin_key(scheme: &str, host: &str, port: u16, proxy: Option<&str>) -> String {
    match proxy {
        Some(p) => format!("{scheme}://{host}:{port}@via:{p}"),
        None => format!("{scheme}://{host}:{port}"),
    }
}

/// Full connector stack: limits + SSRF hook + proxy + TLS. Implements
/// `tower::Service<Uri>` so hyper-util's legacy `Client` can drive it.
pub(crate) struct EngineConnector {
    limits: Arc<ConnectionLimits>,
    /// Shared protocol instrumentation (task 5.1): requests/streams vs
    /// physical establishments, labeled by negotiated protocol.
    stats: Arc<HttpProtocolStats>,
    proxy: ProxyConfig,
    tls: Option<Arc<TlsSettings>>,
    address_filter: Option<Arc<dyn crate::config::AddressFilter>>,
    /// Proxy credentials as a sensitive basic-auth header (§28).
    proxy_auth: Option<hyper::header::HeaderValue>,
    connect_timeout: Duration,
    alpn_h2: bool,
}

impl std::fmt::Debug for EngineConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineConnector")
            .field("proxy", &self.proxy)
            .field("tls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

impl Clone for EngineConnector {
    fn clone(&self) -> Self {
        Self {
            stats: self.stats.clone(),
            limits: self.limits.clone(),
            proxy: self.proxy.clone(),
            tls: self.tls.clone(),
            address_filter: self.address_filter.clone(),
            proxy_auth: self.proxy_auth.clone(),
            connect_timeout: self.connect_timeout,
            alpn_h2: self.alpn_h2,
        }
    }
}

impl EngineConnector {
    /// Assemble the stack from engine configuration.
    ///
    /// # Errors
    /// TLS trust-set load failures (§21.1: fail closed).
    pub fn new(cfg: &crate::config::EngineConfig) -> Result<Self, DownloadError> {
        let tls = Some(Arc::new(TlsSettings::new(&cfg.tls, cfg.prefer_http2)?));
        let proxy_auth = match &cfg.proxy {
            ProxyConfig::Http { url } => {
                let uri: hyper::Uri = url
                    .parse()
                    .map_err(|e| DownloadError::Proxy(format!("bad proxy url: {e}")))?;
                // Userinfo rides inside the authority ("user:pass@host").
                uri.authority()
                    .and_then(|a| a.as_str().split_once('@'))
                    .map(|(ui, _)| {
                        let mut v = hyper::header::HeaderValue::from_str(&format!(
                            "Basic {}",
                            base64_encode(ui.as_bytes())
                        ))
                        .expect("static basic auth");
                        v.set_sensitive(true);
                        v
                    })
            }
            _ => None,
        };
        Ok(Self {
            stats: Arc::new(HttpProtocolStats::default()),
            limits: Arc::new(ConnectionLimits::new(
                cfg.max_connections_total,
                cfg.max_connections_per_origin,
            )),
            proxy: cfg.proxy.clone(),
            tls,
            address_filter: cfg.address_filter.clone(),
            proxy_auth,
            connect_timeout: cfg.network.connect_timeout,
            alpn_h2: cfg.prefer_http2,
        })
    }

    /// The shared connection limits (exposed for benchmark instrumentation).
    #[must_use]
    pub fn limits(&self) -> &Arc<ConnectionLimits> {
        &self.limits
    }

    /// The shared protocol instrumentation (task 5.1): logical
    /// requests/H2 streams vs physical TCP/TLS establishments and their
    /// negotiated protocol. Shared by every client slot of one transport.
    #[must_use]
    pub fn protocol_stats(&self) -> &Arc<HttpProtocolStats> {
        &self.stats
    }

    /// Negotiated wire protocol of a completed TLS handshake (§24):
    /// `Http2` only when ALPN actually negotiated `h2`, never assumed.
    fn tls_protocol(tls: &tokio_rustls::client::TlsStream<tokio::net::TcpStream>) -> HttpProtocol {
        if tls.get_ref().1.alpn_protocol() == Some(b"h2") {
            HttpProtocol::Http2
        } else {
            HttpProtocol::Http1
        }
    }

    /// Connect to `dst`, honoring limits, the SSRF hook, and the proxy
    /// policy (§27.2, §28, §21.5).
    ///
    /// # Errors
    /// Structured connection/TLS/proxy/DNS errors (§20).
    pub async fn connect(&self, dst: &hyper::Uri) -> Result<LimitedConn, ConnectError> {
        let scheme = dst.scheme_str().unwrap_or("http").to_string();
        if scheme != "http" && scheme != "https" {
            return Err(ConnectError(DownloadError::UnsupportedScheme(scheme)));
        }
        let host = dst
            .host()
            .ok_or_else(|| ConnectError(DownloadError::InvalidUrl("missing host".into())))?
            .to_string();
        let port = dst
            .port_u16()
            .unwrap_or(if scheme == "https" { 443 } else { 80 });
        let proxy_addr = match &self.proxy {
            ProxyConfig::Http { url } | ProxyConfig::Socks5 { url } => Some(url.clone()),
            ProxyConfig::None => None,
        };

        // SSRF hook: the redirect-target pre-check (§21.5).
        if let Some(f) = &self.address_filter {
            f.check(&ConnectionTarget::Unresolved {
                host: host.clone(),
                port,
            })
            .map_err(ConnectError)?;
        }

        let key = origin_key(&scheme, &host, port, proxy_addr.as_deref());
        let permits = self.limits.acquire(&key).await;

        match (&self.proxy, scheme.as_str()) {
            (ProxyConfig::None, "http") => {
                let tcp = self.connect_tcp(&host, port, true).await?;
                self.stats.record_establishment(HttpProtocol::Http1);
                let conn = LimitedConn::plain(tcp, false, permits);
                Ok(conn)
            }
            (ProxyConfig::None, "https") => {
                let tcp = self.connect_tcp(&host, port, true).await?;
                let tls = self.tls_handshake(tcp, &host).await?;
                let protocol = Self::tls_protocol(&tls);
                self.stats.record_establishment(protocol);
                let conn = LimitedConn::tls(tls, false, self.alpn_h2, permits);
                Ok(conn)
            }
            (ProxyConfig::Http { .. }, "https") => {
                // CONNECT tunnel to the origin through the proxy (§28);
                // TLS still applies end-to-end to the origin.
                let (phost, pport) = proxy_target(&self.proxy)?;
                if let Some(f) = &self.address_filter {
                    f.check(&ConnectionTarget::Unresolved {
                        host: phost.clone(),
                        port: pport,
                    })
                    .map_err(ConnectError)?;
                }
                // Tunnel::call dials the proxy through the inner connector.
                let proxy_uri: hyper::Uri = proxy_addr
                    .and_then(|u| u.parse().ok())
                    .ok_or_else(|| ConnectError(DownloadError::Proxy("bad proxy url".into())))?;
                let mut tunnel = hyper_util::client::legacy::connect::proxy::Tunnel::new(
                    proxy_uri,
                    RawTcpConnector,
                );
                if let Some(auth) = &self.proxy_auth {
                    tunnel = tunnel.with_auth(auth.clone());
                }
                let tunneled = tunnel.call(dst.clone()).await.map_err(|e| {
                    ConnectError(DownloadError::Proxy(format!("connect tunnel: {e}")))
                })?;
                let raw = tunneled.into_inner();
                let tls = self.tls_handshake(raw, &host).await?;
                let protocol = Self::tls_protocol(&tls);
                self.stats.record_establishment(protocol);
                let conn = LimitedConn::tls(tls, true, self.alpn_h2, permits);
                Ok(conn)
            }
            (ProxyConfig::Socks5 { .. }, _) => {
                // SOCKS5 hook (§28 extension): the SocksV5 service performs
                // the handshake and dials the origin through the proxy; the
                // SSRF hook gates the proxy target and the unresolved
                // origin before any socket is opened.
                let proxy_uri: hyper::Uri = proxy_addr
                    .and_then(|u| u.parse().ok())
                    .ok_or_else(|| ConnectError(DownloadError::Proxy("bad socks url".into())))?;
                let (phost, pport) = proxy_target(&self.proxy)?;
                if let Some(f) = &self.address_filter {
                    f.check(&ConnectionTarget::Unresolved {
                        host: phost.clone(),
                        port: pport,
                    })
                    .map_err(ConnectError)?;
                }
                let mut s = hyper_util::client::legacy::connect::proxy::SocksV5::new(
                    proxy_uri,
                    RawTcpConnector,
                );
                let conn = s
                    .call(dst.clone())
                    .await
                    .map_err(|e| ConnectError(DownloadError::Proxy(format!("socks5: {e}"))))?;
                let raw = conn.into_inner();
                if scheme == "https" {
                    let tls = self.tls_handshake(raw, &host).await?;
                    let protocol = Self::tls_protocol(&tls);
                    self.stats.record_establishment(protocol);
                    let conn = LimitedConn::tls(tls, true, self.alpn_h2, permits);
                    Ok(conn)
                } else {
                    self.stats.record_establishment(HttpProtocol::Http1);
                    let conn = LimitedConn::plain(raw, true, permits);
                    Ok(conn)
                }
            }
            // Plain HTTP through an HTTP proxy: TCP to the *proxy*, and
            // hyper writes absolute-form targets because of proxy(true)
            // (§28). The proxy resolves the origin; the SSRF hook gated the
            // unresolved origin above and the proxy target below.
            (ProxyConfig::Http { .. }, "http") => {
                let (phost, pport) = proxy_target(&self.proxy)?;
                if let Some(f) = &self.address_filter {
                    f.check(&ConnectionTarget::Unresolved {
                        host: phost.clone(),
                        port: pport,
                    })
                    .map_err(ConnectError)?;
                }
                let tcp = self.connect_tcp(&phost, pport, false).await?;
                self.stats.record_establishment(HttpProtocol::Http1);
                let conn = LimitedConn::plain(tcp, true, permits);
                Ok(conn)
            }
            _ => Err(ConnectError(DownloadError::UnsupportedScheme(scheme))),
        }
    }

    async fn connect_tcp(
        &self,
        host: &str,
        port: u16,
        check_resolved: bool,
    ) -> Result<tokio::net::TcpStream, ConnectError> {
        let resolved =
            tokio::time::timeout(self.connect_timeout, tokio::net::lookup_host((host, port)))
                .await
                .map_err(|_| ConnectError(DownloadError::ConnectTimeout))?
                .map_err(|e| ConnectError(DownloadError::Dns(e.to_string())))?;
        let addrs: Vec<std::net::SocketAddr> = resolved.collect();
        if addrs.is_empty() {
            return Err(ConnectError(DownloadError::Dns("no addresses".into())));
        }
        // Resolved-address check (§21.5): the engine never connects before
        // the embedding layer's filter accepts the IP.
        if check_resolved {
            if let Some(f) = &self.address_filter {
                for a in &addrs {
                    f.check(&ConnectionTarget::Resolved {
                        host: host.to_string(),
                        ip: a.ip(),
                        port: a.port(),
                    })
                    .map_err(ConnectError)?;
                }
            }
        }
        // Try each resolved address in order (resolver preference): a host
        // with both AAAA and A records must not fail when one family is
        // unroutable (§23: DNS caching/runtime behavior).
        let mut last_err: Option<ConnectError> = None;
        for addr in addrs {
            match tokio::time::timeout(self.connect_timeout, tokio::net::TcpStream::connect(addr))
                .await
            {
                Ok(Ok(tcp)) => {
                    tcp.set_nodelay(true).map_err(ConnectError::from)?;
                    debug!(
                        target = %Redactor::new().redact_url(&format!("http://{host}:{port}")),
                        "tcp connected"
                    );
                    return Ok(tcp);
                }
                Ok(Err(e)) => {
                    last_err = Some(ConnectError(DownloadError::Connection(e.to_string())));
                }
                Err(_) => {
                    last_err = Some(ConnectError(DownloadError::ConnectTimeout));
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            ConnectError(DownloadError::Connection("no dialable address".into()))
        }))
    }

    async fn tls_handshake(
        &self,
        tcp: tokio::net::TcpStream,
        host: &str,
    ) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, ConnectError> {
        let settings = self
            .tls
            .as_ref()
            .ok_or_else(|| ConnectError(DownloadError::Tls("tls unavailable".into())))?;
        let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
            .map_err(|e| ConnectError(DownloadError::Tls(format!("server name: {e}"))))?;
        let connector = tokio_rustls::TlsConnector::from(settings.config.clone());
        let tls = tokio::time::timeout(self.connect_timeout, connector.connect(server_name, tcp))
            .await
            .map_err(|_| ConnectError(DownloadError::Tls("handshake timeout".into())))?
            .map_err(|e| ConnectError(DownloadError::Tls(e.to_string())))?;
        Ok(tls)
    }
}

fn proxy_target(p: &ProxyConfig) -> Result<(String, u16), ConnectError> {
    let url = match p {
        ProxyConfig::Http { url } | ProxyConfig::Socks5 { url } => url.clone(),
        ProxyConfig::None => return Err(ConnectError(DownloadError::Proxy("no proxy".into()))),
    };
    let uri: hyper::Uri = url
        .parse()
        .map_err(|e| ConnectError(DownloadError::Proxy(format!("bad proxy url: {e}"))))?;
    let host = uri
        .host()
        .ok_or_else(|| ConnectError(DownloadError::Proxy("proxy missing host".into())))?
        .to_string();
    let port = uri.port_u16().unwrap_or(80);
    Ok((host, port))
}

impl Service<hyper::Uri> for EngineConnector {
    type Response = TokioIo<LimitedConn>;
    type Error = ConnectError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: hyper::Uri) -> Self::Future {
        let this = self.clone();
        Box::pin(async move {
            let conn = this.connect(&dst).await?;
            Ok(TokioIo::new(conn))
        })
    }
}

/// Raw TCP connector handed to the proxy connectors (Tunnel / SocksV5);
/// limits and SSRF checks already ran on the outer stack.
#[derive(Debug, Clone, Copy)]
struct RawTcpConnector;

impl Service<hyper::Uri> for RawTcpConnector {
    type Response = TokioIo<tokio::net::TcpStream>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, dst: hyper::Uri) -> Self::Future {
        Box::pin(async move {
            let host = dst
                .host()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "no host"))?;
            let port = dst.port_u16().unwrap_or(80);
            let addr = tokio::net::lookup_host((host, port))
                .await
                .map_err(std::io::Error::other)?
                .next()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "no addr"))?;
            Ok(TokioIo::new(tokio::net::TcpStream::connect(addr).await?))
        })
    }
}

/// RFC 4648 base64 (public for the credential-provider helpers; §29).
#[must_use]
pub fn base64_encode_public(input: &[u8]) -> String {
    base64_encode(input)
}

/// RFC 4648 base64 (small dependency-free helper for proxy auth only).
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = u32::from_be_bytes([0, b[0], b[1], b[2]]);
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_rfc4648_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn origin_keys_isolate_proxy_and_tls() {
        assert_ne!(
            origin_key("https", "a.example", 443, None),
            origin_key("https", "a.example", 443, Some("proxy:3128"))
        );
        assert_ne!(
            origin_key("http", "a.example", 80, None),
            origin_key("https", "a.example", 443, None)
        );
    }

    #[tokio::test]
    async fn limits_count_live_and_pooled_connections() {
        let limits = ConnectionLimits::new(64, 2);
        let key = origin_key("http", "x.example", 80, None);
        let p1 = limits.acquire(&key).await;
        assert_eq!(limits.origin_in_use(&key), 1);
        let p2 = limits.acquire(&key).await;
        assert_eq!(limits.origin_in_use(&key), 2);
        // Global permits track every live origin permit.
        assert_eq!(limits.global.available_permits(), 64 - 2);
        drop((p1, p2));
        assert_eq!(limits.origin_in_use(&key), 0);
    }
}
