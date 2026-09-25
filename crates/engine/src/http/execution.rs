//! Semantic HTTP execution seam (§32).
//!
//! This module defines the transport-independent HTTP behavior boundary:
//! job orchestration issues semantic [`HttpExecutor::probe`] and
//! [`HttpExecutor::transfer`] operations and receives HTTP-owned outcomes
//! (`ProbeMetadata`, accepted range/total/validator data, and a bounded
//! [`HttpBody`]) or a structured [`HttpFailure`] — never a concrete client
//! response, raw status, header list, or framing type.
//!
//! Two adapters implement the port:
//!
//! - [`crate::http::HttpTransport`]: the production Hyper wire adapter.
//! - [`crate::http::scripted::ScriptedHttp`]: a deterministic scripted
//!   adapter for orchestration tests (no sockets, no wall-clock delays).
//!
//! Job code keeps retry budgets, backoff scheduling, credential-provider
//! decisions, state transitions, sink writes, and checkpointing (§17, §29);
//! the seam removes protocol interpretation from it.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::control::auth::Challenge;
use crate::control::CancellationToken;
use crate::error::DownloadError;
use crate::http::probe::ProbeMetadata;
use crate::http::range::ResponseHead;
use crate::http::transport::RequestSpec;
use crate::http::validators::{parse_content_range, ContentRange, ResourceValidators};

/// Probe request (§10): request specification plus the segmentation and
/// range-verification policy the HTTP layer needs to own probe sequencing.
#[derive(Debug, Clone)]
pub struct ProbeRequest {
    /// Request specification (URL, headers, credentials).
    pub spec: RequestSpec,
    /// Sizes at or above this threshold may qualify for segmented transfer
    /// (§10.3) and therefore warrant a validating range request (§10.2).
    pub segmentation_threshold: u64,
    /// Whether advertised range support must be verified with a real
    /// ranged request before segmented selection (§10.2).
    pub verify_range_support: bool,
}

/// Semantic probe outcome (§10.1): final metadata plus HTTP-level notices
/// orchestration should surface (e.g., advertised-but-unusable ranges).
#[derive(Debug, Clone)]
pub struct ProbeOutcome {
    /// Final probe metadata (the single segmentation-decision input, §10.3).
    pub metadata: ProbeMetadata,
    /// Semantic notices: human-readable facts the HTTP layer classified
    /// while interpreting the probe sequence.
    pub notices: Vec<String>,
}

/// What a ranged transfer response must satisfy to be accepted (§11.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FullResponsePolicy {
    /// A full (200) response to a nonzero range request is an invalid range
    /// response: the body is never delivered (§11.2).
    #[default]
    InvalidRange,
    /// A full (200) response means the `If-Range` precondition failed and
    /// the resource generation changed (§26): report resource change.
    ResourceChanged,
}

/// Ranged transfer intent (§11.2-§11.3): the inclusive requested range with
/// the generation context needed to validate the response before any body
/// byte is accepted.
#[derive(Debug, Clone)]
pub struct RangeIntent {
    /// Inclusive requested byte range `(start, end)`.
    pub range: (u64, u64),
    /// Total size the job already established (probe or earlier responses);
    /// a conflicting response total rejects the transfer (§11.2).
    pub established_total: Option<u64>,
    /// Expected validators (generation identity, §5.2). When present, the
    /// request is issued conditionally (`If-Range`, §11.3).
    pub expected_validators: Option<ResourceValidators>,
    /// How to classify a full (200) response to this ranged request.
    pub full_response: FullResponsePolicy,
}

/// Transfer intent (§32): only two shapes exist — a fresh full
/// representation or a validated byte range.
#[derive(Debug, Clone)]
pub enum TransferIntent {
    /// Fresh sequential representation (full GET).
    Full,
    /// Resumed/retried byte-range representation (§11.2, §17.3).
    Range(RangeIntent),
}

impl TransferIntent {
    /// The requested range when this is a ranged intent.
    #[must_use]
    pub fn range(&self) -> Option<(u64, u64)> {
        match self {
            TransferIntent::Full => None,
            TransferIntent::Range(r) => Some(r.range),
        }
    }
}

/// Transfer request (§32): request specification plus the intent.
#[derive(Debug, Clone)]
pub struct TransferRequest {
    /// Request specification (URL, headers, credentials).
    pub spec: RequestSpec,
    /// Full or ranged intent (§11.2).
    pub intent: TransferIntent,
}

/// Semantic transfer outcome (§32): accepted range/total/validators and a
/// bounded transport-neutral body. Returned only after status, range,
/// total, and generation validation passed — no body chunk exists before
/// that (§32: metadata validated before body).
pub struct TransferResponse {
    /// Inclusive accepted start offset. Full responses accept from 0;
    /// ranged responses start exactly at the requested start.
    pub start: u64,
    /// Inclusive accepted end offset; the body never exceeds it (§11.2).
    pub end: u64,
    /// Authoritative total size when known.
    pub total_size: Option<u64>,
    /// Response validators captured for generation tracking (§5.2).
    pub validators: ResourceValidators,
    /// Bounded demand-driven body (§32): zero-copy chunks, one-chunk
    /// demand, read-idle timeout, cancellation, and pause/resume.
    pub body: HttpBody,
}

impl std::fmt::Debug for TransferResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransferResponse")
            .field("start", &self.start)
            .field("end", &self.end)
            .field("total_size", &self.total_size)
            .field("validators", &self.validators)
            .finish_non_exhaustive()
    }
}

/// Structured HTTP failure (§20): the classified download error, the
/// server-provided retry timing when present (§17.2), and the
/// authentication challenge data when the failure is a challenge (§29).
#[derive(Debug)]
pub struct HttpFailure {
    /// Classified failure ready for the job's retry policy (§17.1).
    pub error: DownloadError,
    /// Server-provided retry timing (Retry-After), already parsed.
    pub retry_after: Option<Duration>,
    /// Authentication challenge (401/407) for the bounded credential
    /// provider flow (§29).
    pub challenge: Option<Challenge>,
}

impl HttpFailure {
    /// Wrap an error with no retry timing and no challenge.
    #[must_use]
    pub fn from_error(error: DownloadError) -> Self {
        Self {
            error,
            retry_after: None,
            challenge: None,
        }
    }
}

impl From<DownloadError> for HttpFailure {
    fn from(error: DownloadError) -> Self {
        Self::from_error(error)
    }
}

/// HTTP-private response metadata (§32): raw status/header values plus the
/// parsed transport-neutral fields validation needs. Confined to the `http`
/// module — job orchestration never sees it; adapters hand job code only
/// semantic outcomes.
#[allow(dead_code)] // wired by the production adapter migration
pub(crate) struct ResponseMetadata {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub content_range: Option<ContentRange>,
    pub validators: ResourceValidators,
    /// Total from Content-Range for 206, else Content-Length (§11.2).
    pub total_size: Option<u64>,
    pub http_version: &'static str,
}

#[allow(dead_code)] // wired by the production adapter migration
impl ResponseMetadata {
    /// Case-insensitive header lookup.
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Build from a raw status/header list, parsing Content-Range,
    /// validators, and the authoritative total (206: Content-Range total,
    /// not the shorter range Content-Length, §11.2).
    pub(crate) fn from_head(
        status: u16,
        headers: Vec<(String, String)>,
        http_version: &'static str,
    ) -> Self {
        let content_range = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("content-range"))
            .and_then(|(_, v)| parse_content_range(v).ok());
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
        Self {
            status,
            headers,
            content_range,
            validators,
            total_size,
            http_version,
        }
    }

    /// Test helper: metadata-only response with no body.
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
        }
    }
}

impl ResponseHead for ResponseMetadata {
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

/// Centralized status-to-error mapping (§17.1, §20): the single table both
/// transfer modes and the probe path use. Previously the sequential path
/// mapped 407 to `Protocol` while the probe path mapped it to `Proxy`;
/// this table makes them agree (allowed compatibility fix).
#[allow(dead_code)] // wired by the seam migration
pub(crate) fn status_to_error(status: u16) -> DownloadError {
    match status {
        404 | 410 => DownloadError::NotFound { status },
        401 => DownloadError::AuthenticationRequired,
        403 => DownloadError::AuthorizationFailed,
        407 => DownloadError::Proxy("proxy authentication required (407)".into()),
        408 => DownloadError::Protocol("408 request timeout".into()),
        429 => DownloadError::RateLimited { status },
        s if (500..=599).contains(&s) => DownloadError::Server { status: s },
        s => DownloadError::Protocol(format!("unexpected status {s}")),
    }
}

/// Retry-After extraction (§17.2): seconds form; HTTP-date unsupported in
/// v1 and yields `None`.
#[allow(dead_code)] // wired by the seam migration
pub(crate) fn retry_after(headers: &[(String, String)]) -> Option<Duration> {
    let value = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("retry-after"))
        .map(|(_, v)| v.as_str());
    crate::control::retry::parse_retry_after(value)
}

/// Authentication challenge extraction (§29): 401/407 with
/// WWW-Authenticate / Proxy-Authenticate headers.
#[allow(dead_code)] // wired by the seam migration
pub(crate) fn challenge(status: u16, url: &str, headers: &[(String, String)]) -> Option<Challenge> {
    crate::control::auth::challenge_from_headers(status, url, headers)
}

/// Centralized body-failure classification (§17.1): resets, truncation
/// (`incomplete message`), connection closes, and read timeouts all land
/// in the Connection family so the shared retry classifier sees identical
/// categories from both adapters and both transfer modes.
#[allow(dead_code)] // wired by the seam migration
pub(crate) fn classify_body_failure(msg: &str, is_timeout: bool) -> DownloadError {
    if msg.contains("incomplete") || msg.contains("connection closed") || msg.contains("reset") {
        DownloadError::Connection(msg.to_string())
    } else if is_timeout || msg.contains("timed out") {
        DownloadError::Connection("read timeout".into())
    } else {
        DownloadError::Connection(msg.to_string())
    }
}

/// Hyper-specific body error classification, delegating to the shared
/// table (production adapter only; Hyper never crosses the seam).
#[allow(dead_code)] // wired by the seam migration
pub(crate) fn classify_hyper_body_error(e: &hyper::Error) -> DownloadError {
    classify_body_failure(&e.to_string(), e.is_timeout())
}

/// Range-overrun classification (§11.2): the body exceeded the accepted
/// inclusive range; structured `InvalidRangeResponse` before delivery.
#[allow(dead_code)] // wired by the seam migration
pub(crate) fn range_overrun_error(start: u64, end: u64) -> DownloadError {
    DownloadError::InvalidRangeResponse(format!(
        "response body exceeds accepted range [{start}, {end}]"
    ))
}

/// The substitutable HTTP execution port (§32).
///
/// Object safe on purpose: adapters are shared as
/// `Arc<dyn HttpExecutor>` behind the cloneable [`HttpExecution`] handle.
/// Methods return explicitly boxed futures so no adapter-specific types
/// leak and no `async-trait` dependency is needed; the allocation happens
/// once per HTTP operation, not once per body chunk.
pub trait HttpExecutor: Send + Sync + 'static {
    /// Execute the probe sequence (§10): HEAD interpretation plus, when
    /// policy requires, the validating range request. The outcome carries
    /// the segmentation decision input; failures carry retry timing and
    /// challenge data.
    fn probe<'a>(
        &'a self,
        request: ProbeRequest,
        cancel: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<ProbeOutcome, HttpFailure>> + Send + 'a>>;

    /// Execute one transfer operation (§11.2): full or ranged. The response
    /// body is returned only after status/range/generation validation
    /// passes; validation failures arrive as structured [`HttpFailure`]s
    /// before any byte is delivered.
    fn transfer<'a>(
        &'a self,
        request: TransferRequest,
        cancel: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<TransferResponse, HttpFailure>> + Send + 'a>>;
}

/// Cheap cloneable handle to one HTTP executor (§32): sequential and
/// segmented job paths both hold this instead of a concrete adapter.
#[derive(Clone)]
pub struct HttpExecution {
    executor: Arc<dyn HttpExecutor>,
}

impl HttpExecution {
    /// Wrap any executor in the shared handle.
    #[must_use]
    pub fn new(executor: Arc<dyn HttpExecutor>) -> Self {
        Self { executor }
    }

    /// Wrap a concrete adapter value.
    #[must_use]
    pub fn from_adapter<E: HttpExecutor>(adapter: E) -> Self {
        Self::new(Arc::new(adapter))
    }

    /// Semantic probe operation (§10).
    ///
    /// # Errors
    /// Structured [`HttpFailure`] with retry timing and challenge data.
    pub async fn probe(
        &self,
        request: ProbeRequest,
        cancel: &CancellationToken,
    ) -> Result<ProbeOutcome, HttpFailure> {
        self.executor.probe(request, cancel).await
    }

    /// Semantic transfer operation (§11.2).
    ///
    /// # Errors
    /// Structured [`HttpFailure`]; body validation completed before the
    /// response is returned.
    pub async fn transfer(
        &self,
        request: TransferRequest,
        cancel: &CancellationToken,
    ) -> Result<TransferResponse, HttpFailure> {
        self.executor.transfer(request, cancel).await
    }
}

impl std::fmt::Debug for HttpExecution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpExecution").finish_non_exhaustive()
    }
}

/// Demand-driven, transport-neutral body source (§32).
///
/// Implemented by adapters (Hyper frames, scripted events); polled only
/// through [`HttpBody`], which adds the configured read-idle timeout and
/// the cancellation/pause signal. The trait is object safe so one pinned
/// box per response suffices.
pub trait HttpBodySource: Send + 'static {
    /// Poll for the next payload chunk. `Ok(None)` is a clean end of body.
    fn poll_chunk(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<bytes::Bytes>, DownloadError>>;
}

/// Bounded transport-neutral body (§32).
///
/// Owns one pinned boxed [`HttpBodySource`] (allocated once per response).
/// Each [`HttpBody::next_chunk`] call is one-chunk demand: no further chunk
/// is fetched until the consumer asks again (§32 backpressure scenario).
/// The read-idle deadline resets per requested chunk and uses the engine's
/// `network.read_idle_timeout` in both transfer modes.
pub struct HttpBody {
    source: Pin<Box<dyn HttpBodySource>>,
    /// Lazily created, reused idle timer: reset for each requested chunk
    /// (§32), never re-created per chunk.
    idle: Option<Pin<Box<tokio::time::Sleep>>>,
    read_idle_timeout: Duration,
}

impl HttpBody {
    /// Wrap a source with the configured read-idle timeout.
    #[must_use]
    pub fn new(source: impl HttpBodySource, read_idle_timeout: Duration) -> Self {
        Self {
            source: Box::pin(source),
            idle: None,
            read_idle_timeout,
        }
    }

    /// Wrap an already-boxed source with the configured read-idle timeout.
    #[must_use]
    pub fn from_boxed(source: Box<dyn HttpBodySource>, read_idle_timeout: Duration) -> Self {
        Self {
            source: Pin::from(source),
            idle: None,
            read_idle_timeout,
        }
    }

    /// The configured read-idle timeout (§32: one policy for both modes).
    #[must_use]
    pub fn read_idle_timeout(&self) -> Duration {
        self.read_idle_timeout
    }

    /// Await exactly one chunk (§32 one-chunk demand).
    ///
    /// Cancellation wins a `select` against a pending read and terminates
    /// with `DownloadError::Cancelled`. A pause also interrupts the pending
    /// read (returning [`BodyEvent::Paused`]) while the source stays owned:
    /// the next call resumes polling that same source, so no data is lost.
    /// No chunk arriving within the read-idle timeout classifies as the
    /// shared connection error (§17.1).
    ///
    /// # Errors
    /// Cancellation, read-idle timeout, and body faults surface as the
    /// structured [`DownloadError`] taxonomy.
    pub async fn next_chunk(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<BodyEvent, DownloadError> {
        // The idle deadline resets for each requested chunk (§32).
        let deadline = tokio::time::Instant::now() + self.read_idle_timeout;
        std::future::poll_fn(|cx| self.poll_step(cx, cancel, deadline)).await
    }

    /// One poll step: cancellation/pause win a select against the pending
    /// source poll; the idle timer arms only while the source is pending.
    fn poll_step(
        &mut self,
        cx: &mut std::task::Context<'_>,
        cancel: &CancellationToken,
        deadline: tokio::time::Instant,
    ) -> std::task::Poll<Result<BodyEvent, DownloadError>> {
        // Cancellation wins against a pending read (§9.2 invariant 8).
        if cancel.is_cancelled() {
            return std::task::Poll::Ready(Err(DownloadError::Cancelled));
        }
        // Pause also interrupts a pending read (§9.3); the source stays
        // owned so the next read resumes the same stream, byte-exact.
        if cancel.is_paused() {
            return std::task::Poll::Ready(Ok(BodyEvent::Paused));
        }
        match self.source.as_mut().poll_chunk(cx) {
            std::task::Poll::Ready(Ok(Some(data))) => {
                std::task::Poll::Ready(Ok(BodyEvent::Data(data)))
            }
            std::task::Poll::Ready(Ok(None)) => std::task::Poll::Ready(Ok(BodyEvent::End)),
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
            std::task::Poll::Pending => {
                // Arm (or re-arm) the reused idle timer; expiry classifies
                // as the shared read-idle connection error (§17.1).
                let sleep = self
                    .idle
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
                sleep.as_mut().reset(deadline);
                match sleep.as_mut().poll(cx) {
                    std::task::Poll::Ready(()) => std::task::Poll::Ready(Err(
                        DownloadError::Connection("read idle timeout".into()),
                    )),
                    std::task::Poll::Pending => std::task::Poll::Pending,
                }
            }
        }
    }
}

/// One awaited step of bounded body delivery (§32).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyEvent {
    /// One payload chunk, zero-copy (`bytes::Bytes` passed through).
    Data(bytes::Bytes),
    /// Clean end of body.
    End,
    /// A pause was requested while waiting; the source is untouched and
    /// the next read resumes it.
    Paused,
}

impl BodyEvent {
    /// The payload when this event carries data.
    #[must_use]
    pub fn data(&self) -> Option<&bytes::Bytes> {
        match self {
            BodyEvent::Data(d) => Some(d),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll};

    /// Minimal executor used to prove the port is object safe (§32): a
    /// `dyn HttpExecutor` value must be constructible and callable.
    struct NoopExecutor;

    impl HttpExecutor for NoopExecutor {
        fn probe<'a>(
            &'a self,
            _request: ProbeRequest,
            _cancel: &'a CancellationToken,
        ) -> Pin<Box<dyn Future<Output = Result<ProbeOutcome, HttpFailure>> + Send + 'a>> {
            Box::pin(async { Err(HttpFailure::from_error(DownloadError::Cancelled)) })
        }

        fn transfer<'a>(
            &'a self,
            _request: TransferRequest,
            _cancel: &'a CancellationToken,
        ) -> Pin<Box<dyn Future<Output = Result<TransferResponse, HttpFailure>> + Send + 'a>>
        {
            Box::pin(async { Err(HttpFailure::from_error(DownloadError::Cancelled)) })
        }
    }

    /// A body source stub proving [`HttpBodySource`] is object safe behind
    /// the one-response box.
    struct EmptySource;

    impl HttpBodySource for EmptySource {
        fn poll_chunk(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<Option<bytes::Bytes>, DownloadError>> {
            Poll::Ready(Ok(None))
        }
    }

    #[test]
    fn executor_is_object_safe() {
        let execution = HttpExecution::from_adapter(NoopExecutor);
        // A second handle clones cheaply and shares the same adapter.
        let clone = execution.clone();
        assert!(Arc::ptr_eq(&execution.executor, &clone.executor));
    }

    #[test]
    fn body_source_is_object_safe() {
        let body = HttpBody::new(EmptySource, Duration::from_secs(1));
        let _ = body;
    }

    #[test]
    fn range_intent_reports_requested_range() {
        let full = TransferIntent::Full;
        let ranged = TransferIntent::Range(RangeIntent {
            range: (10, 19),
            established_total: Some(100),
            expected_validators: None,
            full_response: FullResponsePolicy::InvalidRange,
        });
        assert!(full.range().is_none());
        assert_eq!(ranged.range(), Some((10, 19)));
    }

    // ---- HttpBody semantics ----

    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    /// Source that yields the given chunks in order; reports poll count
    /// through a shared counter the test can read.
    struct CountingSource {
        chunks: Vec<bytes::Bytes>,
        polls: Arc<AtomicUsize>,
    }

    impl HttpBodySource for CountingSource {
        fn poll_chunk(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<Option<bytes::Bytes>, DownloadError>> {
            let this = self.get_mut();
            this.polls.fetch_add(1, AtomicOrdering::SeqCst);
            match this.chunks.is_empty() {
                true => std::task::Poll::Ready(Ok(None)),
                false => std::task::Poll::Ready(Ok(Some(this.chunks.remove(0)))),
            }
        }
    }

    /// Source parked until a flag flips: models a stalled wire read.
    struct ParkedSource {
        go: Arc<std::sync::atomic::AtomicBool>,
        chunk: bytes::Bytes,
        polls: Arc<AtomicUsize>,
    }

    impl HttpBodySource for ParkedSource {
        fn poll_chunk(
            self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<Option<bytes::Bytes>, DownloadError>> {
            let this = self.get_mut();
            this.polls.fetch_add(1, AtomicOrdering::SeqCst);
            if this.go.load(AtomicOrdering::SeqCst) {
                std::task::Poll::Ready(Ok(Some(this.chunk.clone())))
            } else {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }

    #[tokio::test]
    async fn one_chunk_demand_never_prefetches() {
        let a = bytes::Bytes::from_static(b"aa");
        let b = bytes::Bytes::from_static(b"bbb");
        let polls = Arc::new(AtomicUsize::new(0));
        let src = CountingSource {
            chunks: vec![a.clone(), b.clone()],
            polls: polls.clone(),
        };
        let mut body = HttpBody::new(src, Duration::from_secs(5));
        let cancel = CancellationToken::new();

        let first = body.next_chunk(&cancel).await.expect("chunk a");
        assert_eq!(first.data(), Some(&a));
        // Consumer holds the chunk: the source must not be polled again.
        assert_eq!(
            1,
            polls.load(AtomicOrdering::SeqCst),
            "§32: no prefetch while the consumer processes one chunk"
        );

        let second = body.next_chunk(&cancel).await.expect("chunk b");
        assert_eq!(second.data(), Some(&b));
        let third = body.next_chunk(&cancel).await.expect("eof");
        assert_eq!(third, BodyEvent::End);
        assert_eq!(3, polls.load(AtomicOrdering::SeqCst));
    }

    #[tokio::test]
    async fn delivered_bytes_share_source_storage() {
        // Zero-copy ownership (§32): the delivered chunk references the same
        // allocation the source produced, not a copy.
        let stored = bytes::Bytes::from(vec![7u8; 300]);
        let ptr = stored.as_ptr();
        let polls = Arc::new(AtomicUsize::new(0));
        let src = CountingSource {
            chunks: vec![stored.clone()],
            polls,
        };
        let mut body = HttpBody::new(src, Duration::from_secs(5));
        let event = body
            .next_chunk(&CancellationToken::new())
            .await
            .expect("chunk");
        match event {
            BodyEvent::Data(d) => {
                assert_eq!(d.as_ptr(), ptr, "§32: Bytes must pass through uncopied");
                assert_eq!(d.len(), 300);
            }
            other => panic!("expected data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancellation_interrupts_pending_read() {
        let go = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let src = ParkedSource {
            go,
            chunk: bytes::Bytes::from_static(b"x"),
            polls: Arc::new(AtomicUsize::new(0)),
        };
        let mut body = HttpBody::new(src, Duration::from_secs(30));
        let cancel = CancellationToken::new();
        let cancel2 = cancel.clone();
        let waiter = tokio::spawn(async move { body.next_chunk(&cancel2).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        let started = std::time::Instant::now();
        let err = waiter.await.expect("join").expect_err("cancelled");
        assert!(matches!(err, DownloadError::Cancelled));
        assert!(started.elapsed() < Duration::from_secs(5), "prompt");
    }

    #[tokio::test]
    async fn pause_interrupts_pending_read_then_resumes_same_source() {
        let go = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let polls = Arc::new(AtomicUsize::new(0));
        let src = ParkedSource {
            go: go.clone(),
            chunk: bytes::Bytes::from_static(b"resume-me"),
            polls: polls.clone(),
        };
        let mut body = HttpBody::new(src, Duration::from_secs(30));
        let cancel = CancellationToken::new();

        cancel.pause();
        // Pause wins even when issued before the read starts (§9.3).
        let event = body.next_chunk(&cancel).await.expect("paused");
        assert_eq!(event, BodyEvent::Paused);

        // Lift the pause and signal the source: the SAME body (and its
        // source state) delivers the chunk.
        cancel.unpause();
        go.store(true, AtomicOrdering::SeqCst);
        let event = body.next_chunk(&cancel).await.expect("chunk after resume");
        assert_eq!(event.data().map(|d| d.as_ref()), Some(&b"resume-me"[..]));
        assert!(
            polls.load(AtomicOrdering::SeqCst) > 0,
            "resumed polling the same owned source"
        );
    }

    #[tokio::test]
    async fn read_idle_timeout_is_classified() {
        struct StalledSource;
        impl HttpBodySource for StalledSource {
            fn poll_chunk(
                self: Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<Option<bytes::Bytes>, DownloadError>> {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
        let mut body = HttpBody::new(StalledSource, Duration::from_millis(20));
        let err = body
            .next_chunk(&CancellationToken::new())
            .await
            .expect_err("idle timeout");
        assert!(
            matches!(err, DownloadError::Connection(ref m) if m.contains("read idle timeout")),
            "§32: idle timeout classifies as the shared connection error: {err:?}"
        );
        // Category is retryable through the shared classifier (§17.1).
        assert_eq!(err.category(), crate::error::ErrorCategory::Connection);
    }

    #[tokio::test]
    async fn source_fault_surfaces_unchanged() {
        struct FaultySource;
        impl HttpBodySource for FaultySource {
            fn poll_chunk(
                self: Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<Option<bytes::Bytes>, DownloadError>> {
                std::task::Poll::Ready(Err(DownloadError::Connection("reset by peer".into())))
            }
        }
        let mut body = HttpBody::new(FaultySource, Duration::from_secs(5));
        let err = body
            .next_chunk(&CancellationToken::new())
            .await
            .expect_err("fault");
        assert!(matches!(err, DownloadError::Connection(_)));
    }

    #[test]
    fn body_event_helpers() {
        let d = BodyEvent::Data(bytes::Bytes::from_static(b"z"));
        assert_eq!(d.data().map(|b| b.len()), Some(1));
        assert!(BodyEvent::End.data().is_none());
        assert!(BodyEvent::Paused.data().is_none());
    }

    // ---- Centralized classification ----

    #[test]
    fn status_table_covers_error_families() {
        use crate::error::ErrorCategory as C;
        let cases: [(u16, C); 10] = [
            (404, C::NotFound),
            (410, C::NotFound),
            (401, C::AuthenticationRequired),
            (403, C::AuthorizationFailed),
            (407, C::Proxy),
            (408, C::Protocol),
            (429, C::RateLimited),
            (500, C::Server),
            (503, C::Server),
            (599, C::Server),
        ];
        for (status, want) in cases {
            let err = super::status_to_error(status);
            assert_eq!(err.category(), want, "status {status}");
        }
        // Only the status-carrying variants expose http_status (§20).
        for status in [404u16, 410, 429, 500, 503, 599] {
            assert_eq!(super::status_to_error(status).http_status(), Some(status));
        }
        // Unexpected statuses stay Protocol with the status in the message.
        assert!(matches!(
            super::status_to_error(302),
            DownloadError::Protocol(ref m) if m.contains("302")
        ));
        // 200/206 are not errors (intent rules decide acceptance below).
        assert_eq!(super::status_to_error(200).category(), C::Protocol);
        assert_eq!(super::status_to_error(206).category(), C::Protocol);
    }

    #[test]
    fn retry_after_forms() {
        let h = |v: &str| vec![("retry-after".into(), v.into())];
        assert_eq!(
            super::retry_after(&h("120")),
            Some(Duration::from_secs(120))
        );
        // HTTP-date unsupported in v1 (§17.2): None.
        assert_eq!(
            super::retry_after(&h("Mon, 22 Sep 2026 00:00:00 GMT")),
            None
        );
        assert_eq!(super::retry_after(&h("bogus")), None);
        assert_eq!(super::retry_after(&[]), None);
        // Case-insensitive header lookup.
        assert_eq!(
            super::retry_after(&[("Retry-After".into(), "5".into())]),
            Some(Duration::from_secs(5))
        );
    }

    #[test]
    fn challenge_extraction_401_407_only() {
        let headers = vec![("WWW-Authenticate".into(), "Bearer realm=\"x\"".into())];
        let ch = super::challenge(401, "https://x/f", &headers).expect("401 challenge");
        assert_eq!(ch.status, 401);
        assert_eq!(ch.scheme().as_deref(), Some("bearer"));
        assert!(super::challenge(200, "https://x/f", &headers).is_none());
        assert!(super::challenge(404, "https://x/f", &headers).is_none());
    }

    #[test]
    fn response_metadata_parses_totals_and_validators() {
        let meta = ResponseMetadata::from_head(
            206,
            vec![
                ("content-range".into(), "bytes 100-199/1234".into()),
                ("etag".into(), "\"v1\"".into()),
                ("content-length".into(), "100".into()),
            ],
            "HTTP/1.1",
        );
        // 206 total comes from Content-Range, not the range Content-Length.
        assert_eq!(meta.total_size, Some(1234));
        assert_eq!(meta.content_range.map(|cr| cr.start), Some(100));
        assert_eq!(meta.validators.etag.as_deref(), Some("\"v1\""));
        assert_eq!(meta.header("ETAG"), Some("\"v1\""));

        let full = ResponseMetadata::from_head(
            200,
            vec![("content-length".into(), "500".into())],
            "HTTP/1.1",
        );
        assert_eq!(full.total_size, Some(500));
        assert!(full.content_range.is_none());
    }

    /// Table: ranged intent validation over the private metadata view.
    #[test]
    fn range_intent_rules_table() {
        use crate::http::range::{validate_range_response, RejectionKind};
        let validators = || ResourceValidators::from_headers(Some("\"v1\""), None, Some(1000));
        let cr = |s, e, t| ContentRange {
            start: s,
            end: e,
            total: t,
        };
        type Case = (
            &'static str,
            u16,
            Option<ContentRange>,
            (u64, u64),
            Option<u64>,
            Option<RejectionKind>,
        );
        let cases: Vec<Case> = vec![
            (
                "valid 206",
                206,
                Some(cr(100, 199, Some(1000))),
                (100, 199),
                Some(1000),
                None,
            ),
            (
                "malformed/missing content-range",
                206,
                None,
                (100, 199),
                Some(1000),
                Some(RejectionKind::StartMismatch),
            ),
            (
                "mismatched start",
                206,
                Some(cr(101, 199, Some(1000))),
                (100, 199),
                Some(1000),
                Some(RejectionKind::StartMismatch),
            ),
            (
                "end overshoot",
                206,
                Some(cr(100, 250, Some(1000))),
                (100, 199),
                Some(1000),
                Some(RejectionKind::EndOvershoot),
            ),
            (
                "total conflict",
                206,
                Some(cr(100, 199, Some(999))),
                (100, 199),
                Some(1000),
                Some(RejectionKind::TotalConflict),
            ),
            (
                "200 to nonzero range",
                200,
                None,
                (100, 199),
                Some(1000),
                Some(RejectionKind::FullResponseToNonzeroRange),
            ),
            (
                "unexpected status",
                503,
                None,
                (0, 99),
                Some(1000),
                Some(RejectionKind::UnexpectedStatus),
            ),
        ];
        for (name, status, content_range, requested, established, want_rejection) in cases {
            let meta = ResponseMetadata::for_test(
                status,
                vec![],
                content_range,
                validators(),
                content_range.and_then(|c| c.total),
            );
            let result = validate_range_response(requested, &meta, established, None);
            match want_rejection {
                None => assert!(result.is_ok(), "{name}: expected acceptance"),
                Some(kind) => {
                    let err = result.expect_err(name);
                    assert_eq!(err.kind, kind, "{name}");
                }
            }
        }
        // 200 to bytes=0-E stays the acceptable full-body case.
        let meta = ResponseMetadata::for_test(200, vec![], None, validators(), Some(1000));
        let v = validate_range_response((0, 999), &meta, Some(1000), None).expect("zero range");
        assert_eq!(v.start, 0);
    }

    #[test]
    fn generation_conflict_rejected_before_body() {
        use crate::http::range::{validate_range_response, RejectionKind};
        let old = ResourceValidators::from_headers(Some("\"v1\""), None, Some(1000));
        let new = ResourceValidators::from_headers(Some("\"v2\""), None, Some(1000));
        let meta = ResponseMetadata::for_test(
            206,
            vec![],
            Some(ContentRange {
                start: 0,
                end: 99,
                total: Some(1000),
            }),
            new.clone(),
            Some(1000),
        );
        let err = validate_range_response((0, 99), &meta, Some(1000), Some(&old))
            .expect_err("generation change");
        assert_eq!(err.kind, RejectionKind::GenerationChanged);
        assert!(matches!(
            err.into_error(),
            DownloadError::ResourceChanged(_)
        ));
        // Same generation passes.
        let same = ResponseMetadata::for_test(
            206,
            vec![],
            Some(ContentRange {
                start: 0,
                end: 99,
                total: Some(1000),
            }),
            old.clone(),
            Some(1000),
        );
        assert!(validate_range_response((0, 99), &same, Some(1000), Some(&old)).is_ok());
    }

    /// Table: body failures (resets, truncation, timeouts) classify into the
    /// Connection family identically for both transfer modes (§17.1).
    #[test]
    fn body_failure_classification_table() {
        let cases = [
            ("incomplete message", false, "incomplete message"),
            (
                "connection closed before message",
                false,
                "connection closed before message",
            ),
            ("connection reset", false, "connection reset"),
            ("hypertext body timed out", false, "read timeout"),
            ("anything else", false, "anything else"),
        ];
        for (msg, is_timeout, want) in cases {
            let err = super::classify_body_failure(msg, is_timeout);
            assert!(
                matches!(err, DownloadError::Connection(ref m) if m == want),
                "msg={msg}: {err:?}"
            );
            assert_eq!(err.category(), crate::error::ErrorCategory::Connection);
        }
        // The timeout flag alone forces the read-timeout classification.
        let err = super::classify_body_failure("idle", true);
        assert!(matches!(err, DownloadError::Connection(ref m) if m == "read timeout"));
    }

    #[test]
    fn range_overrun_is_invalid_range_response() {
        let err = super::range_overrun_error(100, 199);
        assert!(matches!(
            err,
            DownloadError::InvalidRangeResponse(ref m)
                if m.contains("[100, 199]"),
        ));
        // Overrun predicate (§11.2): shared by both transfer modes.
        assert!(crate::http::range::body_overrun(50, 100, 100));
        assert!(!crate::http::range::body_overrun(0, 100, 100));
    }
}
