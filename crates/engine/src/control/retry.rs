//! Retry classification and backoff (§17, D10).

use std::time::Duration;

use rand::Rng;

use crate::config::RetryPolicy;
use crate::error::{DownloadError, ErrorCategory};

/// Decision after classifying one failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Retry after waiting `delay` (full jitter, §17.2, or Retry-After).
    Retry { attempt: u32, delay: Duration },
    /// Permanent: no further attempts for this operation.
    GiveUp,
}

/// Classifies errors against policy (§17.1).
#[derive(Debug, Clone)]
pub struct RetryClassifier {
    policy: RetryPolicy,
}

impl RetryClassifier {
    #[must_use]
    pub fn new(policy: RetryPolicy) -> Self {
        Self { policy }
    }

    #[must_use]
    pub fn policy(&self) -> &RetryPolicy {
        &self.policy
    }

    /// Whether this error may be retried (§17.1 tables).
    #[must_use]
    pub fn retryable(&self, err: &DownloadError) -> bool {
        use ErrorCategory as C;
        let status = err.http_status();
        match err.category() {
            C::Dns | C::ConnectTimeout | C::Connection | C::RetryExhausted => true,
            C::Server => self.policy.retry_5xx,
            C::RateLimited => self.policy.retry_429,
            C::Protocol => match status {
                Some(408) => self.policy.retry_408,
                _ => true, // protocol resets (§17.1: HTTP/2 stream reset)
            },
            C::NotFound
            | C::AuthorizationFailed
            | C::AuthenticationRequired
            | C::InvalidUrl
            | C::UnsupportedScheme
            | C::Tls
            | C::Proxy
            | C::RangeUnsupported
            | C::InvalidRangeResponse
            | C::ResourceChanged
            | C::UnknownLengthUnsupportedForMode
            | C::SinkOpen
            | C::SinkWrite
            | C::DiskFull
            | C::PermissionDenied
            | C::Checkpoint
            | C::IntegrityMismatch
            | C::Commit
            | C::DestinationConflict
            | C::Cancelled
            | C::DeadlineExceeded
            | C::Configuration
            | C::Redirect => false,
        }
    }

    /// Backoff delay for `attempt` (0-based) with full jitter (§17.2):
    /// `delay = random(0, min(max_delay, base * multiplier^attempt))`.
    #[must_use]
    pub fn backoff_delay(&self, attempt: u32) -> Duration {
        let base = self.policy.base_delay.as_secs_f64();
        let cap = (base
            * self
                .policy
                .multiplier
                .powi(i32::try_from(attempt).unwrap_or(i32::MAX)))
        .min(self.policy.max_delay.as_secs_f64());
        let jittered = rand::rng().random_range(0.0..=cap);
        Duration::from_secs_f64(jittered.max(0.0))
    }

    /// Cap a server-provided Retry-After duration to policy max (§17.2).
    #[must_use]
    pub fn honor_retry_after(&self, server_value: Option<Duration>) -> Option<Duration> {
        if !self.policy.honor_retry_after {
            return None;
        }
        server_value
            .map(|d| d.min(self.policy.retry_after_max))
            .filter(|d| !d.is_zero())
    }

    /// Classify and produce the full decision for attempt `attempt`
    /// (0-based).
    #[must_use]
    pub fn decide(
        &self,
        err: &DownloadError,
        attempt: u32,
        retry_after: Option<Duration>,
    ) -> RetryDecision {
        if err.category() == ErrorCategory::Cancelled {
            return RetryDecision::GiveUp;
        }
        if !self.retryable(err) {
            return RetryDecision::GiveUp;
        }
        if attempt >= self.policy.max_attempts_per_segment {
            return RetryDecision::GiveUp;
        }
        // Retry-After takes precedence when honored and longer (§17.2).
        let delay = if let Some(ra) = self.honor_retry_after(retry_after) {
            ra.max(self.backoff_delay(attempt))
        } else {
            self.backoff_delay(attempt)
        };
        RetryDecision::Retry {
            attempt: attempt + 1,
            delay,
        }
    }
}

/// Parse a `Retry-After` header value: seconds form (delta-seconds) or
/// HTTP-date (not supported in v1 — returns None).
#[must_use]
pub fn parse_retry_after(value: Option<&str>) -> Option<Duration> {
    value?.trim().parse::<u64>().ok().map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classifier() -> RetryClassifier {
        RetryClassifier::new(RetryPolicy {
            max_attempts_per_segment: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            multiplier: 2.0,
            retry_408: true,
            retry_429: true,
            retry_5xx: true,
            honor_retry_after: true,
            retry_after_max: Duration::from_secs(10),
        })
    }

    #[test]
    fn transient_errors_retryable() {
        let c = classifier();
        for e in [
            DownloadError::Connection("reset".into()),
            DownloadError::Dns("temp".into()),
            DownloadError::ConnectTimeout,
            DownloadError::Server { status: 503 },
            DownloadError::RateLimited { status: 429 },
        ] {
            assert!(c.retryable(&e), "{e:?} must be retryable");
        }
    }

    #[test]
    fn permanent_errors_not_retryable() {
        let c = classifier();
        for e in [
            DownloadError::NotFound { status: 404 },
            DownloadError::AuthorizationFailed,
            DownloadError::Tls("bad cert".into()),
            DownloadError::DiskFull("full".into()),
            DownloadError::IntegrityMismatch("sha256".into()),
            DownloadError::Cancelled,
        ] {
            assert!(!c.retryable(&e), "{e:?} must not be retryable");
        }
    }

    #[test]
    fn backoff_bounds_and_full_jitter() {
        let c = classifier();
        for attempt in 0..10u32 {
            let d = c.backoff_delay(attempt);
            // Full jitter: delay in [0, cap]; cap = min(max, base*2^n).
            let cap = (0.1 * 2f64.powi(attempt.min(8) as i32)).min(30.0);
            assert!(d.as_secs_f64() >= 0.0);
            assert!(
                d.as_secs_f64() <= cap * 1.01 + 0.01,
                "attempt {attempt}: {d:?} exceeds cap {cap}"
            );
        }
    }

    #[test]
    fn jitter_actually_varies() {
        let c = classifier();
        let samples: std::collections::HashSet<u64> = (0..50)
            .map(|_| c.backoff_delay(3).as_millis() as u64)
            .collect();
        assert!(samples.len() > 10, "full jitter must vary: {samples:?}");
    }

    #[test]
    fn attempts_exhaustion_gives_up() {
        let c = classifier();
        let err = DownloadError::Connection("reset".into());
        assert!(
            matches!(c.decide(&err, 3, None), RetryDecision::GiveUp,),
            "attempt 3 of max 3 must give up"
        );
        assert!(matches!(
            c.decide(&err, 2, None),
            RetryDecision::Retry { attempt: 3, .. }
        ));
    }

    #[test]
    fn retry_after_honored_within_cap() {
        let c = classifier();
        let d = c
            .honor_retry_after(Some(Duration::from_secs(5)))
            .expect("valid retry-after");
        assert_eq!(d, Duration::from_secs(5));
        // Capped at retry_after_max (10s here).
        assert_eq!(
            c.honor_retry_after(Some(Duration::from_secs(3600))),
            Some(Duration::from_secs(10))
        );
        assert_eq!(c.honor_retry_after(None), None);
    }

    #[test]
    fn retry_after_disabled_by_policy() {
        let c = RetryClassifier::new(RetryPolicy {
            honor_retry_after: false,
            ..RetryPolicy::default()
        });
        assert_eq!(c.honor_retry_after(Some(Duration::from_secs(5))), None);
    }

    #[test]
    fn parse_retry_after_forms() {
        assert_eq!(
            parse_retry_after(Some("120")),
            Some(Duration::from_secs(120))
        );
        assert_eq!(
            parse_retry_after(Some("Mon, 22 Sep 2026 00:00:00 GMT")),
            None
        );
        assert_eq!(parse_retry_after(None), None);
        assert_eq!(parse_retry_after(Some("bogus")), None);
    }

    #[test]
    fn cancelled_never_retries() {
        let c = classifier();
        assert!(matches!(
            c.decide(&DownloadError::Cancelled, 0, None),
            RetryDecision::GiveUp
        ));
    }
}
