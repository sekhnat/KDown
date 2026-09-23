//! Structured error taxonomy per `KDownSpec.md` §20.
//!
//! Every error carries a stable [`ErrorCategory`], a retryability hint, and
//! optional origin/status/segment context. Sensitive data is redacted at
//! the formatting boundary by [`crate::redact::Redactor`].

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
}

impl DownloadError {
    /// Stable category for this error instance.
    pub fn category(&self) -> ErrorCategory {
        use DownloadError::*;
        match self {
            Configuration(_) => ErrorCategory::Configuration,
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
