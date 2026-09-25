//! HTTP transport: hyper-backed request execution and response streaming
//! (§7.1, §32).

use std::future::Future;
use std::pin::Pin;
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
use crate::control::CancellationToken;
use crate::error::DownloadError;
use crate::http::connect::{
    ConnectError, ConnectionLimits, EngineConnector, HttpProtocol, HttpProtocolStats,
};
use crate::http::execution::{
    challenge, classify_hyper_body_error, range_overrun_error, retry_after, status_to_error,
    FullResponsePolicy, HttpBody, HttpBodySource, HttpExecutor, HttpFailure, ProbeOutcome,
    ProbeRequest, ResponseMetadata, TransferIntent, TransferRequest, TransferResponse,
};
use crate::http::range::{validate_range_response, ResponseHead};
use crate::http::redirect::{RedirectAction, RedirectPolicy, RedirectTracker};
use crate::http::validators::{if_range_value, ContentRange, ResourceValidators};

/// HTTP-private final response (§32): the raw Hyper response plus the
/// redirect-resolved final URL and HTTP version. Raw values never leave
/// the `http` module; adapters map them onto semantic outcomes.
pub(crate) struct FinalResponse {
    pub response: hyper::Response<Incoming>,
    pub final_url: String,
    pub http_version: &'static str,
}

impl FinalResponse {
    /// Flattened header list from a `HeaderMap`.
    pub(crate) fn header_list_from(headers: &hyper::HeaderMap) -> Vec<(String, String)> {
        headers
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect()
    }
}

/// Metadata + streaming body returned before any body byte is accepted.
/// HTTP-private production-adapter detail (§32): the semantic seam maps
/// this onto `TransferResponse`; job code never sees it.
pub(crate) struct RangeResponse {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) content_range: Option<ContentRange>,
    pub(crate) validators: ResourceValidators,
    pub(crate) total_size: Option<u64>,
}

impl RangeResponse {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

impl ResponseHead for RangeResponse {
    fn status(&self) -> u16 {
        self.status
    }

    fn content_range(&self) -> Option<ContentRange> {
        self.content_range
    }

    fn validators(&self) -> &ResourceValidators {
        &self.validators
    }

    fn total_size(&self) -> Option<u64> {
        self.total_size
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.header(name)
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

/// Raw response metadata without body (probe path). HTTP-private
/// production-adapter detail (§32).
pub(crate) struct HeadResponse {
    pub(crate) parts: Parts,
    pub(crate) headers: Vec<(String, String)>,
    /// Redirect-resolved final URL (§10.1), retained by the execution layer.
    pub(crate) final_url: String,
    /// Wire protocol version of the final response.
    pub(crate) http_version: &'static str,
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
    /// Protocol instrumentation shared with the connector (task 5.1):
    /// logical requests/H2 streams counted separately from physical
    /// TCP/TLS establishments, labeled by negotiated protocol.
    stats: Arc<HttpProtocolStats>,
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
            .field("protocol_stats", &self.stats)
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
        let stats = connector.protocol_stats().clone();
        Ok(Self {
            clients,
            next_client: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            connector: Arc::new(connector),
            stats,
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

    /// Protocol instrumentation shared with the connector (task 5.1):
    /// logical requests/H2 streams vs physical TCP/TLS establishments,
    /// each labeled with its negotiated protocol.
    #[must_use]
    pub fn protocol_stats(&self) -> &Arc<HttpProtocolStats> {
        &self.stats
    }

    fn uri(&self, url: &str) -> Result<Uri, DownloadError> {
        url.parse::<Uri>()
            .map_err(|e| DownloadError::InvalidUrl(format!("{url}: {e}")))
    }

    /// HEAD probe (§10.2 step 1). HTTP-private: the semantic seam's
    /// `probe` owns interpretation.
    ///
    /// # Errors
    /// Structured transport errors.
    pub(crate) async fn head(
        &self,
        spec: &RequestSpec,
        cancel: &crate::control::CancellationToken,
    ) -> Result<HeadResponse, DownloadError> {
        self.run_headless(Method::HEAD, spec, cancel).await
    }

    /// GET a byte range, conditionally validated (§11.2-§11.3).
    /// HTTP-private: the semantic seam's `transfer` owns validation.
    pub(crate) async fn get_range(
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
        let final_resp = self
            .request_following_redirects(method, spec, None, cancel)
            .await?;
        let (parts, _body) = final_resp.response.into_parts();
        let headers: Vec<(String, String)> = parts
            .headers
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        Ok(HeadResponse {
            parts,
            headers,
            final_url: final_resp.final_url,
            http_version: final_resp.http_version,
        })
    }

    async fn run_get(
        &self,
        spec: &RequestSpec,
        range: Option<(u64, u64)>,
        cancel: &crate::control::CancellationToken,
    ) -> Result<RangeResponse, DownloadError> {
        let final_resp = self
            .request_following_redirects(Method::GET, spec, range, cancel)
            .await?;
        // Metadata-only: the validating probe never consumes this body
        // (the semantic transfer path streams its own response body).
        let (parts, _body) = final_resp.response.into_parts();
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
        })
    }

    async fn request_following_redirects(
        &self,
        method: Method,
        spec: &RequestSpec,
        range: Option<(u64, u64)>,
        cancel: &crate::control::CancellationToken,
    ) -> Result<FinalResponse, DownloadError> {
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
                RedirectAction::Final => {
                    // Retain the post-redirect URL and wire version for
                    // probe metadata (§10.1); raw values stay inside `http`.
                    let http_version = if resp.version() == hyper::Version::HTTP_2 {
                        "HTTP/2.0"
                    } else {
                        "HTTP/1.1"
                    };
                    return Ok(FinalResponse {
                        response: resp,
                        final_url: url,
                        http_version,
                    });
                }
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
        // Logical request accounting (task 5.1): one completed request is
        // one H2 stream when the connection negotiated HTTP/2, otherwise
        // one HTTP/1.x request on its own connection.
        let protocol = if resp.version() == hyper::Version::HTTP_2 {
            HttpProtocol::Http2
        } else {
            HttpProtocol::Http1
        };
        self.stats.record_request(protocol);
        Ok(resp)
    }
}

/// Hyper frame source for [`HttpBody`] (§32): polls `Incoming` frames
/// internally, discards no data frames, rejects unexpected non-data frames
/// per the existing protocol policy, and maps Hyper body errors through
/// the one shared classifier. Hyper's `Bytes` chunks pass through
/// zero-copy. A range limit rejects an overrun before returning the
/// offending chunk (§11.2); EOF and underflow continue into the engine's
/// exact final size/coverage checks.
struct HyperBodySource {
    inner: Incoming,
    /// Range limiting: `(range_start, range_end, delivered)` when the
    /// intent is ranged; `None` for full bodies.
    limit: Option<(u64, u64, u64)>,
}

impl HyperBodySource {
    fn new(inner: Incoming, limit: Option<(u64, u64, u64)>) -> Self {
        Self { inner, limit }
    }
}

impl HttpBodySource for HyperBodySource {
    fn poll_chunk(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<bytes::Bytes>, DownloadError>> {
        use http_body_util::BodyExt;
        // `Incoming` is Unpin: safe to project to &mut inside the box. The
        // frame future is a wrapper struct, no allocation per poll.
        let this = self.get_mut();
        match std::pin::pin!(this.inner.frame()).poll(cx) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(None) => std::task::Poll::Ready(Ok(None)),
            std::task::Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(data) => {
                    if let Some((start, end, delivered)) = &mut this.limit {
                        let (start, end) = (*start, *end);
                        let len = data.len() as u64;
                        let accepted_len = end - start + 1;
                        if delivered.saturating_add(len) > accepted_len {
                            // Overrun: reject before returning the chunk
                            // (§11.2).
                            return std::task::Poll::Ready(Err(range_overrun_error(start, end)));
                        }
                        *delivered += len;
                    }
                    std::task::Poll::Ready(Ok(Some(data)))
                }
                Err(_trailer) => std::task::Poll::Ready(Err(DownloadError::Protocol(
                    "unexpected trailer frame".into(),
                ))),
            },
            std::task::Poll::Ready(Some(Err(e))) => {
                std::task::Poll::Ready(Err(classify_hyper_body_error(&e)))
            }
        }
    }
}

impl HttpExecutor for HttpTransport {
    /// Semantic probe (§10): HEAD interpretation plus, when policy requires,
    /// the validating `bytes=0-0` request — all inside the HTTP layer.
    fn probe<'a>(
        &'a self,
        request: ProbeRequest,
        cancel: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<ProbeOutcome, HttpFailure>> + Send + 'a>> {
        Box::pin(async move {
            let head = self
                .head(&request.spec, cancel)
                .await
                .map_err(HttpFailure::from_error)?;
            let status = head.parts.status.as_u16();
            let retry_after = retry_after(&head.headers);
            let challenge_data = challenge(status, &request.spec.url, &head.headers);
            let mut meta = crate::http::probe::interpret(
                status,
                &head.final_url,
                &head.headers,
                None,
                head.http_version,
            )
            .map_err(|error| HttpFailure {
                error,
                retry_after,
                challenge: challenge_data,
            })?;

            // Policy-controlled validating range request (§10.2): only when
            // the size qualifies for segmentation, ranges are advertised,
            // verification is enabled, and nothing verified it yet.
            let mut notices: Vec<String> = Vec::new();
            let size_qualifies = meta
                .total_size
                .is_some_and(|s| s >= request.segmentation_threshold);
            if size_qualifies
                && meta.accept_ranges
                && request.verify_range_support
                && !meta.range_verified
            {
                let mut vspec = RequestSpec {
                    url: head.final_url.clone(),
                    headers: request.spec.headers.clone(),
                    identity_encoding: true,
                    ..RequestSpec::default()
                };
                vspec.range = Some((0, 0));
                match self.get_range(&vspec, (0, 0), cancel).await {
                    Ok(vresp) => {
                        let ok = vresp.status == 206
                            && vresp
                                .content_range
                                .is_some_and(|cr| cr.start == 0 && cr.total.is_some());
                        if ok {
                            meta.range_verified = true;
                            if let Some(cr) = vresp.content_range {
                                meta.content_range_total = cr.total;
                                if cr.total != meta.total_size {
                                    // HEAD lied about the size; trust the
                                    // Content-Range total (§10.2).
                                    meta.total_size = cr.total;
                                }
                            }
                        } else {
                            // Advertised-but-broken range support: capability
                            // failure for segmentation (§10.2); fall back to
                            // single stream with verified=false.
                            meta.accept_ranges = false;
                            meta.range_verified = false;
                            notices.push(
                                "server advertises ranges but ranged GET failed; \
                                 falling back to single stream"
                                    .into(),
                            );
                        }
                    }
                    // Ordinary transport failures do not prove ranges unusable:
                    // retain the structured failure so the job's retry policy
                    // decides (design decision 5) instead of silently
                    // downgrading every failure.
                    Err(error) => return Err(HttpFailure::from_error(error)),
                }
            }
            Ok(ProbeOutcome {
                metadata: meta,
                notices,
            })
        })
    }

    /// Semantic transfer (§11.2): status/range/generation validation all
    /// complete before the body is returned; no raw response crosses.
    fn transfer<'a>(
        &'a self,
        request: TransferRequest,
        cancel: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<TransferResponse, HttpFailure>> + Send + 'a>> {
        Box::pin(async move {
            let requested_range = request.intent.range();
            let mut spec = request.spec.clone();
            spec.range = requested_range;
            // Conditional request for ranged transfers with expected
            // validators (§11.3): the strongest validator protects against
            // generation mixing.
            if let (TransferIntent::Range(ri), Some(_)) = (&request.intent, requested_range) {
                spec.validators = ri.expected_validators.clone();
            }

            let final_resp = self
                .request_following_redirects(Method::GET, &spec, requested_range, cancel)
                .await
                .map_err(HttpFailure::from_error)?;
            let FinalResponse {
                response,
                final_url: _,
                http_version,
            } = final_resp;
            let (parts, incoming) = response.into_parts();
            let headers = FinalResponse::header_list_from(&parts.headers);
            let status = parts.status.as_u16();
            let meta = ResponseMetadata::from_head(status, headers.clone(), http_version);

            // Status mapping (§17.1) with retry timing and challenge data,
            // before any body byte is available.
            if status != 200 && status != 206 {
                return Err(HttpFailure {
                    error: status_to_error(status),
                    retry_after: retry_after(&headers),
                    challenge: challenge(status, &request.spec.url, &headers),
                });
            }

            // Intent validation before body delivery (§32).
            let (start, end, total_size, validators) = match &request.intent {
                TransferIntent::Full => {
                    // A 206 to a full request is accepted as-is (existing
                    // behavior); the body covers the representation from 0.
                    let total = meta.total_size;
                    (
                        0u64,
                        total.map_or(u64::MAX, |t| t.saturating_sub(1)),
                        total,
                        meta.validators.clone(),
                    )
                }
                TransferIntent::Range(ri) => {
                    // Full response to a ranged request: classified from the
                    // intent's conditional policy (§26 vs §11.2) before body
                    // delivery.
                    if status == 200 && ri.full_response == FullResponsePolicy::ResourceChanged {
                        return Err(HttpFailure::from_error(DownloadError::ResourceChanged(
                            "server ignored If-Range; resource changed".into(),
                        )));
                    }
                    let validated = validate_range_response(
                        ri.range,
                        &meta,
                        ri.established_total,
                        ri.expected_validators.as_ref(),
                    )
                    .map_err(|rej| {
                        // Generation changes report resource change; the
                        // remaining rejections report invalid range response.
                        HttpFailure::from_error(rej.into_error())
                    })?;
                    (
                        validated.start,
                        validated.end,
                        validated.total_size,
                        meta.validators.clone(),
                    )
                }
            };

            // Range-limited bounded body (§32): the limit rejects an overrun
            // before returning the offending chunk.
            let limit = match (&request.intent, requested_range) {
                (TransferIntent::Range(_), Some((s, e))) => Some((s, e, 0u64)),
                _ => None,
            };
            let body = HttpBody::new(
                HyperBodySource::new(incoming, limit),
                self.network.read_idle_timeout,
            );
            Ok(TransferResponse {
                start,
                end,
                total_size,
                validators,
                body,
            })
        })
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
