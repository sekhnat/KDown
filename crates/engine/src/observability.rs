//! Structured logging with correlation fields and redaction (§35).
//!
//! The engine emits `tracing` events carrying correlation fields
//! (§35.2): engine instance id, job id, worker id, lease id, origin,
//! request attempt, error category. All values pass through the
//! redaction layer (§35.3) at the formatting boundary — credentials
//! never reach a log record.
//!
//! Log levels (§35.1):
//! - ERROR: terminal failures
//! - WARN:  retries nearing exhaustion, recoverable protocol oddities
//! - INFO:  job start/completion, selected mode, major state changes
//! - DEBUG: segment assignment, retry classification, connection decisions
//! - TRACE: detailed request lifecycle with sensitive data redacted

use std::fmt;

use tracing::{debug, error, info, warn};

use crate::error::ErrorCategory;

/// Correlation fields for structured log events (§35.2). All optional:
/// events carry what applies to their stage.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct Correlation {
    /// Engine instance id (§35.2).
    pub engine: Option<u64>,
    /// Job id (§35.2).
    pub job: Option<u64>,
    /// Worker id when applicable (§35.2).
    pub worker: Option<usize>,
    /// Segment lease id when applicable (§35.2).
    pub lease: Option<u64>,
    /// Origin (scheme + authority), userinfo-redacted.
    pub origin: Option<String>,
    /// Request attempt number when applicable.
    pub attempt: Option<u32>,
    /// Error category on failure events.
    pub category: Option<ErrorCategory>,
}

impl Correlation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn engine(mut self, id: u64) -> Self {
        self.engine = Some(id);
        self
    }

    #[must_use]
    pub fn job(mut self, id: u64) -> Self {
        self.job = Some(id);
        self
    }

    #[must_use]
    pub fn worker(mut self, id: usize) -> Self {
        self.worker = Some(id);
        self
    }

    #[must_use]
    pub fn lease(mut self, id: u64) -> Self {
        self.lease = Some(id);
        self
    }

    #[must_use]
    pub fn origin(mut self, o: impl Into<String>) -> Self {
        self.origin = Some(crate::redact::Redactor::new().redact_url(&o.into()));
        self
    }

    #[must_use]
    pub fn attempt(mut self, a: u32) -> Self {
        self.attempt = Some(a);
        self
    }

    #[must_use]
    pub fn category(mut self, c: ErrorCategory) -> Self {
        self.category = Some(c);
        self
    }
}

/// Terminal failure: ERROR (§35.1) with category and origin context.
pub fn log_terminal_error(correlation: &Correlation, error: &crate::error::DownloadError) {
    error!(
        engine = correlation.engine,
        job = correlation.job,
        worker = correlation.worker,
        lease = correlation.lease,
        origin = correlation.origin,
        attempt = correlation.attempt,
        category = ?error.category(),
        "terminal failure: {error}"
    );
}

/// Recoverable oddity / nearing-exhaustion retry: WARN (§35.1).
pub fn log_warning(correlation: &Correlation, detail: &str) {
    warn!(
        engine = correlation.engine,
        job = correlation.job,
        worker = correlation.worker,
        lease = correlation.lease,
        origin = correlation.origin,
        attempt = correlation.attempt,
        category = ?correlation.category,
        "{detail}"
    );
}

/// Major state change / mode selection: INFO (§35.1).
pub fn log_info(correlation: &Correlation, detail: &str) {
    info!(
        engine = correlation.engine,
        job = correlation.job,
        worker = correlation.worker,
        lease = correlation.lease,
        origin = correlation.origin,
        attempt = correlation.attempt,
        category = ?correlation.category,
        "{detail}"
    );
}

/// Segment assignment / retry classification / connection decisions:
/// DEBUG (§35.1).
pub fn log_debug(correlation: &Correlation, detail: &str) {
    debug!(
        engine = correlation.engine,
        job = correlation.job,
        worker = correlation.worker,
        lease = correlation.lease,
        origin = correlation.origin,
        attempt = correlation.attempt,
        category = ?correlation.category,
        "{detail}"
    );
}

/// Request lifecycle detail: TRACE (§35.1) — sensitive data is redacted
/// by [`crate::redact::Redactor`] before it reaches this layer.
pub fn log_trace(correlation: &Correlation, detail: &str) {
    tracing::trace!(
        engine = correlation.engine,
        job = correlation.job,
        worker = correlation.worker,
        lease = correlation.lease,
        origin = correlation.origin,
        attempt = correlation.attempt,
        category = ?correlation.category,
        "{detail}"
    );
}

/// A redacting formatter wrapper for host-provided header pairs: values
/// of sensitive header names never render (§35.3).
pub struct RedactedHeaders<'a> {
    headers: &'a [(String, String)],
}

impl<'a> RedactedHeaders<'a> {
    #[must_use]
    pub fn new(headers: &'a [(String, String)]) -> Self {
        Self { headers }
    }
}

impl fmt::Display for RedactedHeaders<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (k, v) in self.headers {
            let redacted =
                crate::redact::SENSITIVE_HEADER_NAMES.contains(&k.to_ascii_lowercase().as_str());
            if redacted {
                writeln!(f, "{k}: <redacted>")?;
            } else {
                writeln!(f, "{k}: {v}")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlation_redacts_origin_userinfo() {
        let c = Correlation::new().origin("https://alice:secret@cdn.example/f?token=x");
        assert_eq!(c.origin.as_deref(), Some("https://cdn.example/f?token=x"));
    }

    #[test]
    fn redacted_headers_display() {
        let headers = vec![
            (
                "Authorization".to_string(),
                "Bearer secret-token".to_string(),
            ),
            ("cookie".to_string(), "session=abc".to_string()),
            ("set-cookie".to_string(), "sid=xyz".to_string()),
            ("proxy-authorization".to_string(), "Basic xyz".to_string()),
            ("etag".to_string(), "\"v1\"".to_string()),
        ];
        let out = RedactedHeaders::new(&headers).to_string();
        assert!(out.contains("Authorization: <redacted>"));
        assert!(out.contains("cookie: <redacted>"));
        assert!(out.contains("set-cookie: <redacted>"));
        assert!(out.contains("proxy-authorization: <redacted>"));
        assert!(out.contains("etag: \"v1\""));
        assert!(!out.contains("secret-token"));
        assert!(!out.contains("session=abc"));
        assert!(!out.contains("sid=xyz"));
    }
}
