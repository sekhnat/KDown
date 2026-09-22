//! HTTP transport: hyper-backed request execution and response streaming
//! (§7.1, §32).

use std::sync::Arc;
use std::time::Duration;

use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{ACCEPT_ENCODING, AUTHORIZATION, COOKIE, IF_RANGE, RANGE, USER_AGENT};
use hyper::http::response::Parts;
use hyper::{Method, Request, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use crate::config::{EngineConfig, NetworkPolicy, ProxyConfig};
use crate::error::DownloadError;
use crate::http::connect::{ConnectError, ConnectionLimits, EngineConnector};
use crate::http::redirect::{RedirectAction, RedirectPolicy, RedirectTracker};
use crate::http::validators::{if_range_value, ContentRange, ResourceValidators};

/// Metadata + streaming body returned before any body byte is accepted
/// (§32: transport returns enough metadata for job-level validation).
pub struct RangeResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub content_range: Option<ContentRange>,
    pub validators: ResourceValidators,
    pub total_size: Option<u64>,
    pub http_version: &'static str,
    body: Option<Incoming>,
}

impl RangeResponse {
    /// Access the body stream exactly once; caller validates metadata
    /// first (§32).
    pub fn body(&mut self) -> Option<Incoming> {
        self.body.take()
    }

    /// Test-only constructor: metadata-only response with no body.
    #[cfg(test)]
    pub(crate) fn for_test(
        status: u16,
        headers: Vec<(String, String)>,
        content_range: Option<ContentRange>,
        validators: ResourceValidators,
        total_size: Option<u64>,
    ) -> Self {
        Self {
            status,
            headers,
            content_range,
            validators,
            total_size,
            http_version: "HTTP/1.1",
            body: None,
        }
    }

    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

impl std::fmt::Debug for RangeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeResponse")
            .field("status", &self.status)
            .field("content_range", &self.content_range)
            .field("total_size", &self.total_size)
            .finish_non_exhaustive()
    }
}

/// Raw response metadata without body (probe path).
pub struct HeadResponse {
    pub parts: Parts,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
pub struct RequestSpec {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub range: Option<(u64, u64)>,
    pub validators: Option<ResourceValidators>,
    /// Send `Accept-Encoding: identity` for byte-addressable semantics
    /// (§11.4).
    pub identity_encoding: bool,
    /// Credentials attached as Authorization header by the caller.
    pub sensitive: bool,
}

/// Hyper-based HTTP transport (D2, §24, §27, §28).
#[derive(Clone)]
pub struct HttpTransport {
    /// One pooled client per H2 connection slot (D5): the first
    /// multiplexes all streams over a single connection; the hook
    /// (`H2ConnectionPolicy::Additional`) adds more connection slots that
    /// range requests round-robin across when the single connection
    /// underutilizes the path (§24).
    clients: Vec<Arc<Client<EngineConnector, Full<bytes::Bytes>>>>,
    next_client: Arc<std::sync::atomic::AtomicUsize>,
    connector: Arc<EngineConnector>,
    network: NetworkPolicy,
    redirect: RedirectPolicy,
    user_agent: String,
    proxy: ProxyConfig,
}

impl std::fmt::Debug for HttpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTransport")
            .field("network", &self.network)
            .field("proxy", &self.proxy)
            .field("h2_connection_slots", &self.clients.len())
            .finish()
    }
}

impl HttpTransport {
    /// Build a transport from engine configuration: TLS (validation on,
    /// §21.1), pooling limits (§27), proxy selection (§28), H2 ALPN (§24).
    ///
    /// With [`crate::config::H2ConnectionPolicy::Single`] (default) one
    /// client handles everything: hyper-util's pool opens exactly one
    /// connecting task per H2 origin, so segmented range streams multiplex
    /// over a single TCP/TLS connection (D5).
    ///
    /// With [`crate::config::H2ConnectionPolicy::Additional`] the transport
    /// carries N pool slots and round-robins range requests across them,
    /// allowing up to N connections per origin under the §24 conditions.
    ///
    /// # Errors
    /// TLS trust-set load failure (fails closed; no plaintext fallback).
    pub fn from_config(cfg: &EngineConfig) -> Result<Self, DownloadError> {
        let connector = EngineConnector::new(cfg)?;
        let slots = match &cfg.h2_policy {
            crate::config::H2ConnectionPolicy::Single => 1,
            crate::config::H2ConnectionPolicy::Additional { max_connections } => {
                (*max_connections).max(1) as usize
            }
        };
        let mut clients = Vec::with_capacity(slots);
        for _ in 0..slots {
            clients.push(Arc::new(Self::build_client(&connector, cfg)));
        }
        Ok(Self {
            clients,
            next_client: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            connector: Arc::new(connector),
            redirect: RedirectPolicy {
                max_redirects: cfg.network.max_redirects,
                deny_downgrade: cfg.network.deny_https_downgrade,
                forward_cross_origin_credentials: cfg.network.forward_credentials_cross_origin,
            },
            network: cfg.network.clone(),
            user_agent: "kdown-engine/0.1".to_string(),
            proxy: cfg.proxy.clone(),
        })
    }

    fn build_client(
        connector: &EngineConnector,
        cfg: &EngineConfig,
    ) -> Client<EngineConnector, Full<bytes::Bytes>> {
        Client::builder(TokioExecutor::new())
            .pool_idle_timeout(cfg.pool.idle_timeout)
            // Idle H2 conns are cheap; keep a healthy idle cache so pooled
            // reuse is active (§27.1-§27.3).
            .pool_max_idle_per_host(cfg.pool.max_per_origin as usize)
            .retry_canceled_requests(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .timer(hyper_util::rt::TokioTimer::new())
            .build(connector.clone())
    }

    /// Build a transport from network policy only (pool/TLS/proxy defaults).
    ///
    /// # Errors
    /// TLS trust-set load failure.
    pub fn new(network: NetworkPolicy) -> Result<Self, DownloadError> {
        let cfg = EngineConfig {
            network,
            ..EngineConfig::default()
        };
        Self::from_config(&cfg)
    }

    #[must_use]
    pub fn with_user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = ua.into();
        self
    }

    /// Live connection-limit instrumentation (§27.2).
    #[must_use]
    pub fn connection_limits(&self) -> &Arc<ConnectionLimits> {
        self.connector.limits()
    }

    fn uri(&self, url: &str) -> Result<Uri, DownloadError> {
        url.parse::<Uri>()
            .map_err(|e| DownloadError::InvalidUrl(format!("{url}: {e}")))
    }

    /// HEAD probe (§10.2 step 1).
    ///
    /// # Errors
    /// Structured transport errors.
    pub async fn head(
        &self,
        spec: &RequestSpec,
        cancel: &crate::control::CancellationToken,
    ) -> Result<HeadResponse, DownloadError> {
        self.run_headless(Method::HEAD, spec, cancel).await
    }

    /// GET with headers; returns metadata and the untouched body.
    ///
    /// # Errors
    /// Structured transport errors.
    pub async fn get(
        &self,
        spec: &RequestSpec,
        cancel: &crate::control::CancellationToken,
    ) -> Result<RangeResponse, DownloadError> {
        // The spec's range (if any) drives request framing; the explicit
        // `range` parameter of get_range takes precedence.
        self.run_get(spec, spec.range, cancel).await
    }

    /// GET a byte range, conditionally validated (§11.2-§11.3).
    pub async fn get_range(
        &self,
        spec: &RequestSpec,
        range: (u64, u64),
        cancel: &crate::control::CancellationToken,
    ) -> Result<RangeResponse, DownloadError> {
        self.run_get(spec, Some(range), cancel).await
    }

    async fn run_headless(
        &self,
        method: Method,
        spec: &RequestSpec,
        cancel: &crate::control::CancellationToken,
    ) -> Result<HeadResponse, DownloadError> {
        let resp = self
            .request_following_redirects(method.clone(), spec, None, cancel)
            .await?;
        let (parts, _body) = resp.into_parts();
        let headers: Vec<(String, String)> = parts
            .headers
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        Ok(HeadResponse { parts, headers })
    }

    async fn run_get(
        &self,
        spec: &RequestSpec,
        range: Option<(u64, u64)>,
        cancel: &crate::control::CancellationToken,
    ) -> Result<RangeResponse, DownloadError> {
        let resp = self
            .request_following_redirects(Method::GET, spec, range, cancel)
            .await?;
        let (parts, body) = resp.into_parts();
        let headers: Vec<(String, String)> = parts
            .headers
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let content_range = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("content-range"))
            .and_then(|(_, v)| crate::http::validators::parse_content_range(v).ok());
        // For 206 responses the resource total is Content-Range's total,
        // not the (shorter) Content-Length of the range body (§11.2).
        let total_size = if content_range.is_some() {
            content_range.and_then(|cr| cr.total)
        } else {
            headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, v)| v.parse().ok())
        };
        let validators = ResourceValidators::from_headers(
            headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("etag"))
                .map(|(_, v)| v.as_str()),
            headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("last-modified"))
                .map(|(_, v)| v.as_str()),
            total_size,
        );
        Ok(RangeResponse {
            status: parts.status.as_u16(),
            headers,
            content_range,
            validators,
            total_size,
            http_version: if parts.version == hyper::Version::HTTP_2 {
                "HTTP/2.0"
            } else {
                "HTTP/1.1"
            },
            body: Some(body),
        })
    }

    async fn request_following_redirects(
        &self,
        method: Method,
        spec: &RequestSpec,
        range: Option<(u64, u64)>,
        cancel: &crate::control::CancellationToken,
    ) -> Result<hyper::Response<Incoming>, DownloadError> {
        let mut tracker = RedirectTracker::new(self.redirect.clone());
        let mut url = spec.url.clone();
        let mut strip_credentials = false;
        loop {
            if cancel.is_cancelled() {
                return Err(DownloadError::Cancelled);
            }
            let resp = self
                .single_request(&method, &url, spec, range, strip_credentials)
                .await?;
            let status = resp.status().as_u16();
            let location = resp
                .headers()
                .get(hyper::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let current_headers: Vec<(String, String)> = resp
                .headers()
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();
            let decision = tracker.decide(status, location.as_deref(), &url, &current_headers);
            match decision.action {
                RedirectAction::Final => return Ok(resp),
                RedirectAction::Follow { location: next } => {
                    if strip_credentials || decision.strip_credentials {
                        strip_credentials = true;
                    }
                    url = resolve_redirect(&url, &next)?;
                }
                RedirectAction::Reject(e) => return Err(e),
            }
        }
    }

    async fn single_request(
        &self,
        method: &Method,
        url: &str,
        spec: &RequestSpec,
        range: Option<(u64, u64)>,
        strip_credentials: bool,
    ) -> Result<hyper::Response<Incoming>, DownloadError> {
        let uri = self.uri(url)?;
        let scheme_ok = matches!(
            uri.scheme().map(|s| s.as_str()),
            Some("http") | Some("https")
        );
        if !scheme_ok {
            return Err(DownloadError::UnsupportedScheme(
                uri.scheme_str().unwrap_or("").to_string(),
            ));
        }
        let mut builder = Request::builder().method(method).uri(uri);
        {
            let hm = builder.headers_mut().expect("builder headers");
            hm.insert(USER_AGENT, self.user_agent.parse().expect("static ua"));
            // Byte-addressable semantics (§11.4): request identity for
            // segmented/range requests.
            if spec.identity_encoding || range.is_some() {
                hm.insert(ACCEPT_ENCODING, "identity".parse().expect("static"));
            }
            if let Some((s, e)) = range {
                hm.insert(RANGE, format!("bytes={s}-{e}").parse().expect("range"));
            }
            if let (Some(v), _) = (spec.validators.as_ref(), range.is_some()) {
                if range.is_some() {
                    if let Some(ir) = if_range_value(v) {
                        hm.insert(IF_RANGE, ir.value.parse().expect("if-range value"));
                    }
                }
            }
            for (k, v) in &spec.headers {
                let lower = k.to_ascii_lowercase();
                if strip_credentials && (lower == "authorization" || lower == "cookie") {
                    continue; // stripped by redirect policy (§21.2)
                }
                hm.insert(
                    hyper::header::HeaderName::from_bytes(k.as_bytes())
                        .map_err(|e| DownloadError::InvalidUrl(format!("bad header {k}: {e}")))?,
                    v.parse().map_err(|e| {
                        DownloadError::InvalidUrl(format!("bad header value {k}: {e}"))
                    })?,
                );
            }
        }
        let req = builder
            .body(Full::new(bytes::Bytes::new()))
            .map_err(|e| DownloadError::Protocol(format!("request build: {e}")))?;

        // Range requests spread across H2 connection slots (D5 hook);
        // everything else uses slot 0. Range requests round-robin so
        // `Additional { max_connections: N }` opens up to N connections
        // while slot 0 alone keeps the single-multiplexed default.
        let client = if range.is_some() && self.clients.len() > 1 {
            let i = self
                .next_client
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            &self.clients[i % self.clients.len()]
        } else {
            &self.clients[0]
        };
        let fut = client.request(req);
        let resp = tokio::time::timeout(self.network.response_header_timeout, fut)
            .await
            .map_err(|_| DownloadError::ConnectTimeout)?
            .map_err(|e| classify_transport_error(&e))?;
        Ok(resp)
    }
}

/// Resolve a redirect Location against the current URL (RFC 7231 §7.1.2).
pub(crate) fn resolve_redirect(current: &str, location: &str) -> Result<String, DownloadError> {
    if location.contains("://") {
        return Ok(location.to_string());
    }
    let base =
        hyper::Uri::try_from(current).map_err(|e| DownloadError::InvalidUrl(e.to_string()))?;
    if let Some(path_and_query) = base.path_and_query() {
        let pq = path_and_query.as_str();
        if let Some(rest) = pq.strip_prefix('/') {
            let _ = rest;
        }
    }
    let authority = base
        .authority()
        .ok_or_else(|| DownloadError::InvalidUrl(current.to_string()))?;
    let scheme = base.scheme_str().unwrap_or("http");
    if location.starts_with('/') {
        Ok(format!("{scheme}://{}{location}", authority.as_str()))
    } else {
        // Relative path: resolve against the current directory portion.
        let base_path = base.path();
        let dir = base_path.rsplit_once('/').map_or("/", |(d, _)| d);
        Ok(format!("{scheme}://{authority}{dir}/{location}"))
    }
}

/// Map hyper client errors onto the taxonomy (§17.1).
pub(crate) fn classify_transport_error(e: &hyper_util::client::legacy::Error) -> DownloadError {
    // Connect-phase failures already carry a structured `DownloadError`
    // as their source (the connector's taxonomy, §20). `DownloadError`
    // is not Clone, so map its category onto a message-preserving
    // reconstruction of the same family.
    let mut src: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(e);
    while let Some(s) = src {
        if let Some(de) = s.downcast_ref::<ConnectError>() {
            return reconstruct_error(&de.0);
        }
        src = s.source();
    }
    let mut msg = e.to_string();
    let mut src: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(e);
    while let Some(s) = src {
        msg.push_str(&format!(": {s}"));
        src = s.source();
    }
    if msg.contains("dns error") || msg.contains("failed to lookup") {
        DownloadError::Dns(msg)
    } else if msg.contains("timed out") {
        DownloadError::ConnectTimeout
    } else {
        DownloadError::Connection(msg)
    }
}

/// Rebuild a [`DownloadError`] in the same taxonomy family from a
/// connect-phase error's category (§20): the message carries the full
/// source chain for debuggability.
fn reconstruct_error(de: &DownloadError) -> DownloadError {
    let msg = de.to_string();
    match de.category() {
        crate::error::ErrorCategory::Proxy => DownloadError::Proxy(msg),
        crate::error::ErrorCategory::Tls => DownloadError::Tls(msg),
        crate::error::ErrorCategory::Dns => DownloadError::Dns(msg),
        crate::error::ErrorCategory::UnsupportedScheme => DownloadError::UnsupportedScheme(msg),
        crate::error::ErrorCategory::InvalidUrl => DownloadError::InvalidUrl(msg),
        _ => DownloadError::Connection(msg),
    }
}

/// Header names stripped by redirect policy — used by tests and callers.
pub const CREDENTIAL_HEADERS: &[&str] = &["authorization", "cookie"];

// Re-exported for callers building requests manually (§21.2 tests).
pub use hyper::header::{AUTHORIZATION as AUTHORIZATION_NAME, COOKIE as COOKIE_NAME};

#[allow(unused_imports)]
use AUTHORIZATION as _AUTH;

#[allow(unused_imports)]
use COOKIE as _COOKIE;
