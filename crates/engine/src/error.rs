//! Structured error taxonomy (§20).
//!
//! Every error carries a stable [`ErrorCategory`], a retryability hint, and
//! optional origin/status/segment context. Sensitive data is redacted at
//! the formatting boundary by [`crate::redact::Redactor`].

use std::path::PathBuf;
use std::time::Duration;
/// Stable machine-readable category of an error (§20).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCategory {
    Configuration,
    InvalidUrl,
    UnsupportedScheme,
    Dns,
    ConnectTimeout,
    Connection,
    Tls,
    Proxy,
    AuthenticationRequired,
    AuthorizationFailed,
    NotFound,
    Server,
    RateLimited,
    Redirect,
    Protocol,
    RangeUnsupported,
    InvalidRangeResponse,
    ResourceChanged,
    UnknownLengthUnsupportedForMode,
    SinkOpen,
    SinkWrite,
    DiskFull,
    PermissionDenied,
    Checkpoint,
    IntegrityMismatch,
    Commit,
    DestinationConflict,
    Cancelled,
    DeadlineExceeded,
    RetryExhausted,
    /// A transfer-memory admission refusal (design D3): an atomic
    /// allocation exceeded a configured cap and can never fit.
    MemoryCap,
}

impl ErrorCategory {
    /// Retryability hint (§20): whether the caller may retry an identical
    /// operation after this error without external change.
    pub fn retryable(self) -> Retryability {
        use ErrorCategory::*;
        match self {
            Dns | ConnectTimeout | Connection | Server | RateLimited | Protocol
            | RetryExhausted => Retryability::Transient,
            Configuration
            | InvalidUrl
            | UnsupportedScheme
            | MemoryCap
            | Tls
            | Proxy
            | AuthenticationRequired
            | AuthorizationFailed
            | NotFound
            | RangeUnsupported
            | InvalidRangeResponse
            | ResourceChanged
            | UnknownLengthUnsupportedForMode
            | SinkOpen
            | SinkWrite
            | DiskFull
            | PermissionDenied
            | Checkpoint
            | IntegrityMismatch
            | DestinationConflict
            | Commit
            | DeadlineExceeded => Retryability::Never,
            Redirect => Retryability::Never,
            Cancelled => Retryability::Never,
        }
    }
}

/// Retryability hint attached to every error (§20).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Retryability {
    /// Retry may succeed; use the configured backoff policy.
    Transient,
    /// Retry without external change will fail again.
    Permanent,
    /// The operation was deliberately abandoned; never retry.
    Never,
}

/// Segment/range context attached to an error when applicable (§20).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentContext {
    pub lease_id: u64,
    pub start: u64,
    pub end: u64,
}

/// The single structured error type for the whole engine (§20, D13).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DownloadError {
    #[error("configuration invalid: {0}")]
    Configuration(String),
    #[error("invalid URL: {0}")]
    InvalidUrl(String),
    #[error("unsupported URL scheme: {0}")]
    UnsupportedScheme(String),
    #[error("DNS failure: {0}")]
    Dns(String),
    #[error("connect timeout")]
    ConnectTimeout,
    #[error("connection error: {0}")]
    Connection(String),
    #[error("TLS error: {0}")]
    Tls(String),
    #[error("proxy error: {0}")]
    Proxy(String),
    #[error("authentication required")]
    AuthenticationRequired,
    #[error("authorization failed")]
    AuthorizationFailed,
    #[error("resource not found (HTTP {status})")]
    NotFound { status: u16 },
    #[error("server error (HTTP {status})")]
    Server { status: u16 },
    #[error("rate limited (HTTP {status})")]
    RateLimited { status: u16 },
    #[error("redirect error: {0}")]
    Redirect(String),
    #[error("protocol violation: {0}")]
    Protocol(String),
    #[error("range requests unsupported")]
    RangeUnsupported,
    #[error("invalid range response: {0}")]
    InvalidRangeResponse(String),
    #[error("resource changed: {0}")]
    ResourceChanged(String),
    #[error("unknown content length unsupported for requested mode")]
    UnknownLengthUnsupportedForMode,
    #[error("cannot open sink: {0}")]
    SinkOpen(String),
    #[error("cannot write sink: {0}")]
    SinkWrite(String),
    #[error("disk full: {0}")]
    DiskFull(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("checkpoint error: {0}")]
    Checkpoint(String),
    #[error("integrity mismatch: {0}")]
    IntegrityMismatch(String),
    #[error("commit failed: {0}")]
    Commit(String),
    #[error("destination conflict: {0}")]
    DestinationConflict(String),
    #[error("cancelled")]
    Cancelled,
    #[error("deadline exceeded")]
    DeadlineExceeded,
    #[error("retries exhausted: {source}")]
    RetryExhausted { source: Box<DownloadError> },
    #[error("transfer-memory cap exceeded: {component} allocation of {requested} bytes exceeds cap {cap}")]
    MemoryCapExceeded {
        /// Accounted component whose cap bound the allocation.
        component: &'static str,
        /// Atomic allocation size in bytes.
        requested: u64,
        /// Binding cap in bytes.
        cap: u64,
    },
    /// Concurrent admitted-job cap (`max_active_jobs`) rejected this start
    /// immediately: no transfer begins and no artifact is written. The
    /// caller may retry after an active job finishes.
    #[error("active-job admission rejected: {active} of {cap} jobs admitted")]
    AdmissionRejected {
        /// Jobs admitted when the start was refused.
        active: u32,
        /// Configured `max_active_jobs` cap.
        cap: u32,
    },
}

impl DownloadError {
    /// Stable category for this error instance.
    pub fn category(&self) -> ErrorCategory {
        use DownloadError::*;
        match self {
            Configuration(_) => ErrorCategory::Configuration,
            MemoryCapExceeded { .. } => ErrorCategory::MemoryCap,
            // A job-admission refusal is the same capacity family: fail
            // immediately, never retryable.
            AdmissionRejected { .. } => ErrorCategory::MemoryCap,
            InvalidUrl(_) => ErrorCategory::InvalidUrl,
            UnsupportedScheme(_) => ErrorCategory::UnsupportedScheme,
            Dns(_) => ErrorCategory::Dns,
            ConnectTimeout => ErrorCategory::ConnectTimeout,
            Connection(_) => ErrorCategory::Connection,
            Tls(_) => ErrorCategory::Tls,
            Proxy(_) => ErrorCategory::Proxy,
            AuthenticationRequired => ErrorCategory::AuthenticationRequired,
            AuthorizationFailed => ErrorCategory::AuthorizationFailed,
            NotFound { .. } => ErrorCategory::NotFound,
            Server { .. } => ErrorCategory::Server,
            RateLimited { .. } => ErrorCategory::RateLimited,
            Redirect(_) => ErrorCategory::Redirect,
            Protocol(_) => ErrorCategory::Protocol,
            RangeUnsupported => ErrorCategory::RangeUnsupported,
            InvalidRangeResponse(_) => ErrorCategory::InvalidRangeResponse,
            ResourceChanged(_) => ErrorCategory::ResourceChanged,
            UnknownLengthUnsupportedForMode => ErrorCategory::UnknownLengthUnsupportedForMode,
            SinkOpen(_) => ErrorCategory::SinkOpen,
            SinkWrite(_) => ErrorCategory::SinkWrite,
            DiskFull(_) => ErrorCategory::DiskFull,
            PermissionDenied(_) => ErrorCategory::PermissionDenied,
            Checkpoint(_) => ErrorCategory::Checkpoint,
            IntegrityMismatch(_) => ErrorCategory::IntegrityMismatch,
            Commit(_) => ErrorCategory::Commit,
            DestinationConflict(_) => ErrorCategory::DestinationConflict,
            Cancelled => ErrorCategory::Cancelled,
            DeadlineExceeded => ErrorCategory::DeadlineExceeded,
            RetryExhausted { .. } => ErrorCategory::RetryExhausted,
        }
    }

    /// Retryability hint (§20).
    pub fn retryability(&self) -> Retryability {
        self.category().retryable()
    }

    /// HTTP status carried by this error, when applicable.
    pub fn http_status(&self) -> Option<u16> {
        match self {
            DownloadError::NotFound { status }
            | DownloadError::Server { status }
            | DownloadError::RateLimited { status } => Some(*status),
            _ => None,
        }
    }

    /// Map a filesystem I/O error into the structured taxonomy (§14.5).
    pub fn from_io(err: &std::io::Error) -> DownloadError {
        match err.kind() {
            std::io::ErrorKind::NotFound => DownloadError::SinkOpen(err.to_string()),
            std::io::ErrorKind::PermissionDenied => {
                DownloadError::PermissionDenied(err.to_string())
            }
            _ => {
                // errno-based detection for ENOSPC/EDQUOT where libc surfaces them.
                match err.raw_os_error() {
                    Some(28) | Some(122) => DownloadError::DiskFull(err.to_string()), // ENOSPC, EDQUOT
                    Some(13) => DownloadError::PermissionDenied(err.to_string()),     // EACCES
                    Some(30) => DownloadError::PermissionDenied(err.to_string()),     // EROFS
                    _ => DownloadError::SinkWrite(err.to_string()),
                }
            }
        }
    }
}

/// Which failure domain a terminal outcome belongs to (§20, design D1).
///
/// Transfer failures concern the remote exchange (network, protocol,
/// integrity, retry exhaustion); infrastructure failures concern the
/// engine or the local environment (configuration, admission, filesystem,
/// checkpoint persistence, commit); cancellation is the caller's own
/// request. The three domains are distinct typed terminal outcomes, never
/// values that must be told apart by parsing a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FailureDomain {
    Transfer,
    Infrastructure,
    Cancelled,
}

impl DownloadError {
    /// The terminal failure domain this error belongs to.
    ///
    /// Network, transport, HTTP semantics, protocol violations, integrity
    /// and retry exhaustion are transfer failures. Caller configuration,
    /// URL admission, sink/filesystem operations, checkpoint persistence,
    /// commit and destination conflicts are infrastructure failures.
    /// [`DownloadError::Cancelled`] is the cancellation domain.
    #[must_use]
    pub fn domain(&self) -> FailureDomain {
        use DownloadError::*;
        match self {
            Configuration(_)
            | InvalidUrl(_)
            | UnsupportedScheme(_)
            | SinkOpen(_)
            | SinkWrite(_)
            | DiskFull(_)
            | PermissionDenied(_)
            | Checkpoint(_)
            | Commit(_)
            | DestinationConflict(_)
            | AdmissionRejected { .. } => FailureDomain::Infrastructure,
            Cancelled => FailureDomain::Cancelled,
            Dns(_)
            | ConnectTimeout
            | Connection(_)
            | Tls(_)
            | Proxy(_)
            | AuthenticationRequired
            | AuthorizationFailed
            | NotFound { .. }
            | Server { .. }
            | RateLimited { .. }
            | Redirect(_)
            | Protocol(_)
            | RangeUnsupported
            | InvalidRangeResponse(_)
            | ResourceChanged(_)
            | UnknownLengthUnsupportedForMode
            | IntegrityMismatch(_)
            | DeadlineExceeded
            | MemoryCapExceeded { .. }
            | RetryExhausted { .. } => FailureDomain::Transfer,
        }
    }
}

/// Byte/time accounting shared by every terminal outcome (§19.1).
///
/// On a completed download these are the final totals; on a failure or
/// cancellation they are the partial state reached before the terminal
/// transition. Every field is a real counter — never derived from
/// warnings or fabricated on failure paths.
#[derive(Debug, Clone, Default)]
pub struct TransferAccounting {
    /// Payload bytes read from the network (retries inflate this).
    pub bytes_downloaded_from_network: u64,
    /// Bytes skipped because a checkpoint already had them.
    pub bytes_reused_from_checkpoint: u64,
    /// Unique newly completed file bytes (excludes reuse and retransmits).
    pub completed_bytes: u64,
    /// Wasted/retransmitted network bytes.
    pub wasted_bytes: u64,
    /// Retry attempts charged by the transfer paths.
    pub retries: u64,
    /// Range requests issued by the segmented transfer (`0` for
    /// single-stream transfers).
    pub segment_requests: u64,
    /// Live-tail splits performed by the segmented scheduler (`0` for
    /// single-stream transfers).
    pub live_splits: u64,
    /// Total transfer size when known.
    pub total_size: Option<u64>,
    /// Time from transfer start to the terminal transition.
    pub elapsed: Duration,
    /// Resource validators observed for this transfer.
    pub validators: crate::http::validators::ResourceValidators,
    /// Actionable, redacted warnings accumulated to the terminal point.
    pub warnings: Vec<String>,
}

impl TransferAccounting {
    /// Wire amplification: payload bytes received from the network divided
    /// by the unique output bytes they produced (§19.1, task 5.1).
    ///
    /// The numerator is [`TransferAccounting::bytes_downloaded_from_network`]:
    /// every wire payload byte counted exactly once. Retries that re-deliver
    /// data therefore inflate it. [`TransferAccounting::wasted_bytes`] is
    /// informational only — it marks received bytes a stream did not keep,
    /// which the network counter already includes, so adding it would count
    /// those bytes a second time.
    ///
    /// The denominator is the unique output coverage:
    /// [`TransferAccounting::completed_bytes`] plus
    /// [`TransferAccounting::bytes_reused_from_checkpoint`], because
    /// checkpoint-reused bytes are legitimately part of the output. A
    /// mostly-reused resume can therefore report a ratio below `1.0`.
    ///
    /// `None` when the denominator is zero — the metric is undefined there,
    /// never fabricated. Benchmark amplification measured from
    /// server-emitted bytes is a separate axis (see
    /// `docs/benchmark-profiling.md`).
    #[must_use]
    pub fn wire_amplification(&self) -> Option<f64> {
        let unique_output = self
            .completed_bytes
            .saturating_add(self.bytes_reused_from_checkpoint);
        if unique_output == 0 {
            None
        } else {
            Some(self.bytes_downloaded_from_network as f64 / unique_output as f64)
        }
    }
}

/// Disposition of retained artifacts (temporary output, checkpoint) at
/// terminal time (§9.4, §15.4).
///
/// Both flags describe the engine's own artifacts as left by the terminal
/// path: a resumable checkpoint from a resumed prior run that the terminal
/// path did not delete still counts as retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ArtifactDisposition {
    /// The temporary output file was retained on disk.
    pub temp_retained: bool,
    /// A resumable checkpoint was retained on disk.
    pub checkpoint_retained: bool,
}

/// A verified, published completion (§7.4).
///
/// Constructed only after size/integrity verification passed and the
/// final destination was committed, so a successful terminal result
/// always means a real completed download.
#[derive(Debug, Clone)]
pub struct CompletedDownload {
    /// Final published destination path. Never absent: success means the
    /// destination was committed.
    pub final_path: PathBuf,
    /// Final transfer accounting.
    pub accounting: TransferAccounting,
}

/// Typed transfer failure (§20): the remote exchange failed.
///
/// Carries the classified error, the partial accounting reached before
/// failure, and the disposition of retained artifacts.
#[derive(Debug)]
pub struct TransferFailure {
    /// Classified transfer error. Display/Debug output is redacted at
    /// formatting boundaries; the category is machine-readable.
    pub error: DownloadError,
    /// Partial accounting of the work done before the failure.
    pub partial: TransferAccounting,
    /// Disposition of the temporary output and checkpoint at failure time.
    pub artifacts: ArtifactDisposition,
}

/// Typed engine/infrastructure failure (§20): an internal engine or local
/// environment fault independent of remote transfer semantics.
#[derive(Debug)]
pub struct EngineFailure {
    /// Classified infrastructure error.
    pub error: DownloadError,
    /// Partial accounting of the work done before the failure.
    pub partial: TransferAccounting,
    /// Disposition of the temporary output and checkpoint at failure time.
    pub artifacts: ArtifactDisposition,
}

/// Typed cancellation summary (§9.4): the caller cancelled the job.
#[derive(Debug)]
pub struct CancellationSummary {
    /// The artifact policy applied at cancellation.
    pub mode: crate::job::controller::CancelMode,
    /// Partial accounting of the work done before cancellation.
    pub partial: TransferAccounting,
    /// Disposition of the temporary output and checkpoint at cancellation.
    pub artifacts: ArtifactDisposition,
}

/// The terminal error of a download run (§7.4, design D1).
///
/// Every non-success terminal outcome is one of these typed variants — a
/// failed transfer is never reported through a successful `Result`. A
/// spawned job's task-join failure stays a distinct outer await error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DownloadRunError {
    /// The remote transfer failed (network, protocol, integrity, retries).
    #[error("transfer failed: {0}")]
    Transfer(Box<TransferFailure>),
    /// The engine or local environment failed (configuration, admission,
    /// filesystem, checkpoint persistence, commit).
    #[error("engine infrastructure failed: {0}")]
    Infrastructure(Box<EngineFailure>),
    /// The caller cancelled the running transfer.
    #[error("cancelled")]
    Cancelled(Box<CancellationSummary>),
}

impl DownloadRunError {
    /// Stable machine-readable category of this terminal error.
    #[must_use]
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Transfer(failure) => failure.error.category(),
            Self::Infrastructure(failure) => failure.error.category(),
            Self::Cancelled(_) => ErrorCategory::Cancelled,
        }
    }

    /// The failure domain of this terminal error.
    #[must_use]
    pub fn domain(&self) -> FailureDomain {
        match self {
            Self::Transfer(_) => FailureDomain::Transfer,
            Self::Infrastructure(_) => FailureDomain::Infrastructure,
            Self::Cancelled(_) => FailureDomain::Cancelled,
        }
    }

    /// The classified engine error behind a transfer or infrastructure
    /// failure; `None` for cancellation.
    #[must_use]
    pub fn as_engine_error(&self) -> Option<&DownloadError> {
        match self {
            Self::Transfer(failure) => Some(&failure.error),
            Self::Infrastructure(failure) => Some(&failure.error),
            Self::Cancelled(_) => None,
        }
    }

    /// Partial accounting carried by this terminal error.
    #[must_use]
    pub fn accounting(&self) -> &TransferAccounting {
        match self {
            Self::Transfer(failure) => &failure.partial,
            Self::Infrastructure(failure) => &failure.partial,
            Self::Cancelled(summary) => &summary.partial,
        }
    }

    /// Disposition of retained artifacts at terminal time.
    #[must_use]
    pub fn artifacts(&self) -> ArtifactDisposition {
        match self {
            Self::Transfer(failure) => failure.artifacts,
            Self::Infrastructure(failure) => failure.artifacts,
            Self::Cancelled(summary) => summary.artifacts,
        }
    }
}

impl TransferFailure {
    /// Classify an engine error as a transfer failure.
    ///
    /// # Panics
    /// In debug builds when the error does not belong to the transfer
    /// domain; release builds accept it unchanged (classification is
    /// advisory for constructors, authoritative in [`Self::error`]).
    #[must_use]
    pub fn new(
        error: DownloadError,
        partial: TransferAccounting,
        artifacts: ArtifactDisposition,
    ) -> Self {
        debug_assert_eq!(
            error.domain(),
            FailureDomain::Transfer,
            "transfer failure constructed from a non-transfer error"
        );
        Self {
            error,
            partial,
            artifacts,
        }
    }
}

impl EngineFailure {
    /// Classify an engine error as an infrastructure failure.
    ///
    /// # Panics
    /// In debug builds when the error does not belong to the
    /// infrastructure domain; release builds accept it unchanged.
    #[must_use]
    pub fn new(
        error: DownloadError,
        partial: TransferAccounting,
        artifacts: ArtifactDisposition,
    ) -> Self {
        debug_assert_eq!(
            error.domain(),
            FailureDomain::Infrastructure,
            "infrastructure failure constructed from a non-infrastructure error"
        );
        Self {
            error,
            partial,
            artifacts,
        }
    }
}

impl std::fmt::Display for TransferFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for TransferFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl std::fmt::Display for EngineFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for EngineFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl std::fmt::Display for CancellationSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cancelled")
    }
}

impl std::error::Error for CancellationSummary {}

#[cfg(test)]
mod terminal_outcome_tests {
    use super::*;

    fn empty_accounting() -> TransferAccounting {
        TransferAccounting {
            elapsed: Duration::from_secs(1),
            ..TransferAccounting::default()
        }
    }

    /// Task 5.1: the received payload is counted once. The historical
    /// `network + wasted` numerator double-counted bytes that both crossed the
    /// wire and were marked redundant, reporting 2.0 for this input.
    #[test]
    fn wire_amplification_counts_received_payload_once() {
        let accounting = TransferAccounting {
            bytes_downloaded_from_network: 150,
            completed_bytes: 100,
            wasted_bytes: 50,
            ..empty_accounting()
        };
        assert_eq!(accounting.wire_amplification(), Some(1.5));
    }

    /// Reused checkpoint bytes are real output coverage, so they sit in the
    /// denominator: a mostly-reused resume reports below 1.0.
    #[test]
    fn wire_amplification_denominator_includes_reused_coverage() {
        let accounting = TransferAccounting {
            bytes_downloaded_from_network: 150,
            completed_bytes: 50,
            bytes_reused_from_checkpoint: 100,
            wasted_bytes: 50,
            ..empty_accounting()
        };
        assert_eq!(accounting.wire_amplification(), Some(1.0));

        let mostly_reused = TransferAccounting {
            bytes_downloaded_from_network: 50,
            completed_bytes: 50,
            bytes_reused_from_checkpoint: 950,
            ..empty_accounting()
        };
        assert_eq!(mostly_reused.wire_amplification(), Some(0.05));
    }

    /// No unique output coverage means the ratio is undefined, even when
    /// bytes crossed the wire.
    #[test]
    fn wire_amplification_is_undefined_without_unique_coverage() {
        let no_coverage = TransferAccounting {
            bytes_downloaded_from_network: 100,
            wasted_bytes: 100,
            ..empty_accounting()
        };
        assert_eq!(no_coverage.wire_amplification(), None);
        assert_eq!(empty_accounting().wire_amplification(), None);
    }

    /// Every error variant lands in exactly one terminal failure domain,
    /// and the domain assignment matches the typed outcome constructors.
    #[test]
    fn domains_are_exhaustive_and_stable() {
        use DownloadError::*;
        let cases: Vec<(DownloadError, FailureDomain)> = vec![
            (Configuration("c".into()), FailureDomain::Infrastructure),
            (InvalidUrl("u".into()), FailureDomain::Infrastructure),
            (UnsupportedScheme("s".into()), FailureDomain::Infrastructure),
            (Dns("d".into()), FailureDomain::Transfer),
            (ConnectTimeout, FailureDomain::Transfer),
            (Connection("c".into()), FailureDomain::Transfer),
            (Tls("t".into()), FailureDomain::Transfer),
            (Proxy("p".into()), FailureDomain::Transfer),
            (AuthenticationRequired, FailureDomain::Transfer),
            (AuthorizationFailed, FailureDomain::Transfer),
            (NotFound { status: 404 }, FailureDomain::Transfer),
            (Server { status: 500 }, FailureDomain::Transfer),
            (RateLimited { status: 429 }, FailureDomain::Transfer),
            (Redirect("r".into()), FailureDomain::Transfer),
            (Protocol("p".into()), FailureDomain::Transfer),
            (RangeUnsupported, FailureDomain::Transfer),
            (InvalidRangeResponse("i".into()), FailureDomain::Transfer),
            (ResourceChanged("r".into()), FailureDomain::Transfer),
            (UnknownLengthUnsupportedForMode, FailureDomain::Transfer),
            (SinkOpen("s".into()), FailureDomain::Infrastructure),
            (SinkWrite("s".into()), FailureDomain::Infrastructure),
            (DiskFull("d".into()), FailureDomain::Infrastructure),
            (PermissionDenied("p".into()), FailureDomain::Infrastructure),
            (Checkpoint("c".into()), FailureDomain::Infrastructure),
            (IntegrityMismatch("i".into()), FailureDomain::Transfer),
            (Commit("c".into()), FailureDomain::Infrastructure),
            (
                DestinationConflict("d".into()),
                FailureDomain::Infrastructure,
            ),
            (Cancelled, FailureDomain::Cancelled),
            (DeadlineExceeded, FailureDomain::Transfer),
            (
                RetryExhausted {
                    source: Box::new(Connection("c".into())),
                },
                FailureDomain::Transfer,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.domain(), expected, "{error:?}");
        }
    }

    /// The three terminal branches stay distinguishable without parsing
    /// messages: domain, category, accounting, and artifact disposition
    /// are all typed accessors.
    #[test]
    fn terminal_branches_are_distinguishable() {
        let transfer = DownloadRunError::Transfer(Box::new(TransferFailure::new(
            DownloadError::IntegrityMismatch("digest".into()),
            TransferAccounting {
                completed_bytes: 10,
                ..empty_accounting()
            },
            ArtifactDisposition {
                temp_retained: true,
                checkpoint_retained: false,
            },
        )));
        let infrastructure = DownloadRunError::Infrastructure(Box::new(EngineFailure::new(
            DownloadError::Checkpoint("store".into()),
            empty_accounting(),
            ArtifactDisposition {
                temp_retained: false,
                checkpoint_retained: true,
            },
        )));
        let cancelled = DownloadRunError::Cancelled(Box::new(CancellationSummary {
            mode: crate::job::controller::CancelMode::KeepPartial,
            partial: empty_accounting(),
            artifacts: ArtifactDisposition {
                temp_retained: true,
                checkpoint_retained: true,
            },
        }));

        assert_eq!(transfer.domain(), FailureDomain::Transfer);
        assert_eq!(transfer.category(), ErrorCategory::IntegrityMismatch);
        assert_eq!(transfer.accounting().completed_bytes, 10);
        assert_eq!(
            transfer.artifacts(),
            ArtifactDisposition {
                temp_retained: true,
                checkpoint_retained: false
            }
        );

        assert_eq!(infrastructure.domain(), FailureDomain::Infrastructure);
        assert_eq!(infrastructure.category(), ErrorCategory::Checkpoint);
        assert_eq!(
            infrastructure.artifacts(),
            ArtifactDisposition {
                temp_retained: false,
                checkpoint_retained: true
            }
        );

        assert_eq!(cancelled.domain(), FailureDomain::Cancelled);
        assert_eq!(cancelled.category(), ErrorCategory::Cancelled);
        assert!(cancelled.as_engine_error().is_none());
        assert_eq!(
            cancelled.artifacts(),
            ArtifactDisposition {
                temp_retained: true,
                checkpoint_retained: true
            }
        );

        // Transfer and infrastructure failures surface their engine error
        // for callers that need the structured variant.
        assert_eq!(
            transfer.as_engine_error().map(DownloadError::category),
            Some(ErrorCategory::IntegrityMismatch)
        );
        assert_eq!(
            infrastructure
                .as_engine_error()
                .map(DownloadError::category),
            Some(ErrorCategory::Checkpoint)
        );
    }

    /// Constructors reject misclassified errors in debug builds: a
    /// filesystem fault must never be reported as a transfer failure.
    #[test]
    #[should_panic(expected = "transfer failure constructed from a non-transfer error")]
    fn transfer_constructor_rejects_infrastructure_errors() {
        let _ = TransferFailure::new(
            DownloadError::DiskFull("disk".into()),
            empty_accounting(),
            ArtifactDisposition::default(),
        );
    }

    #[test]
    #[should_panic(expected = "infrastructure failure constructed from a non-infrastructure error")]
    fn infrastructure_constructor_rejects_transfer_errors() {
        let _ = EngineFailure::new(
            DownloadError::Server { status: 500 },
            empty_accounting(),
            ArtifactDisposition::default(),
        );
    }

    /// Diagnostics stay redaction-safe: the terminal error exposes the
    /// engine error's own (boundary-redacted) message and never invents
    /// new unredacted text, while preserving category, retryability, and
    /// the accumulated warnings verbatim.
    #[test]
    fn diagnostics_are_preserved_and_redaction_safe() {
        let mut partial = empty_accounting();
        partial.warnings = vec!["checkpoint cleanup incomplete (x)".into()];
        let failure = TransferFailure::new(
            DownloadError::Proxy("connect refused".into()),
            partial,
            ArtifactDisposition::default(),
        );
        let terminal = DownloadRunError::Transfer(Box::new(failure));

        // Display delegates to the engine error's redaction-safe message.
        assert_eq!(
            terminal.to_string(),
            "transfer failed: proxy error: connect refused"
        );
        assert_eq!(terminal.category(), ErrorCategory::Proxy);
        assert_eq!(terminal.accounting().retries, 0);
        assert_eq!(
            terminal.accounting().warnings,
            vec!["checkpoint cleanup incomplete (x)".to_string()],
            "partial diagnostics survive the terminal error"
        );
        // Retryability remains derivable from the typed category.
        assert_eq!(terminal.category().retryable(), Retryability::Never);
    }
}
