//! Credential provider hooks (§29) and challenge handling.
//!
//! The engine never owns long-term secrets: callers supply either fixed
//! credentials (already used as `Authorization` headers) or a
//! [`CredentialProvider`] callback consulted when a 401/407 challenge
//! arrives. The engine applies one credential per challenge stage and
//! never retries authentication in an unbounded loop (§29: avoid loops and
//! account locks): at most `max_attempts` rounds, then the structured
//! auth error surfaces.
//!
//! Redaction: provider-supplied header values are marked sensitive and
//! never logged (§35.3); the challenge header itself is fine to log.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::error::DownloadError;

/// What a challenge response looked like (§29 scope/challenge input).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Challenge {
    /// HTTP status that carried the challenge (401 or 407).
    pub status: u16,
    /// Raw `WWW-Authenticate` / `Proxy-Authenticate` values, in order.
    pub authenticate: Vec<String>,
    /// The origin host the challenge came from.
    pub origin: String,
}

impl Challenge {
    /// The authentication scheme of the first challenge line (Basic,
    /// Bearer, Digest, Negotiate, ...) lowercased.
    #[must_use]
    pub fn scheme(&self) -> Option<String> {
        self.authenticate
            .first()
            .and_then(|a| a.split_whitespace().next())
            .map(|s| s.to_ascii_lowercase())
    }
}

/// Decision returned by a credential provider (§29).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CredentialDecision {
    /// Attach these headers to the next request (e.g., Authorization).
    Headers(Vec<(String, String)>),
    /// No credentials available: fail with the structured auth error.
    Decline,
}

/// Credential provider callback (§29):
/// `request(scope, challenge) -> headers/token`.
/// The engine does not store returned secrets beyond the request lifetime.
pub trait CredentialProvider: Send + Sync {
    /// Consulted once per challenge stage.
    fn request(&self, challenge: &Challenge) -> Result<CredentialDecision, DownloadError>;
}

/// Adapter for a plain function/closure.
#[derive(Debug)]
pub struct ProviderFn<F>
where
    F: Fn(&Challenge) -> Result<CredentialDecision, DownloadError> + Send + Sync,
{
    f: F,
}

impl<F> CredentialProvider for ProviderFn<F>
where
    F: Fn(&Challenge) -> Result<CredentialDecision, DownloadError> + Send + Sync,
{
    fn request(&self, challenge: &Challenge) -> Result<CredentialDecision, DownloadError> {
        (self.f)(challenge)
    }
}

/// Shorthand constructor mirroring `CredentialProvider.request(scope,
/// challenge)` semantics (§29 pseudocode).
#[must_use]
pub fn provider_fn<F>(f: F) -> ProviderFn<F>
where
    F: Fn(&Challenge) -> Result<CredentialDecision, DownloadError> + Send + Sync,
{
    ProviderFn { f }
}

/// Authentication-retry guard (§29): bounds how many times the engine
/// consults the provider per job. Two stages are enough for every standard
/// scheme exchange (server challenge -> credentials; repeated 401s mean
/// the credentials are wrong — stop rather than lock accounts).
pub const MAX_AUTH_STAGES: u32 = 2;

/// Tracks one job's credential stages.
#[derive(Debug)]
pub(crate) struct AuthStageGuard {
    stages_used: u32,
}

impl AuthStageGuard {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self { stages_used: 0 }
    }

    /// Whether another credential stage is allowed (§29: no loops).
    #[must_use]
    pub(crate) fn can_provide(&self) -> bool {
        self.stages_used < MAX_AUTH_STAGES
    }

    pub(crate) fn record(&mut self) {
        self.stages_used += 1;
    }
}

/// Extract a challenge from response headers (§29).
#[must_use]
pub fn challenge_from_headers(
    status: u16,
    origin: &str,
    headers: &[(String, String)],
) -> Option<Challenge> {
    if status != 401 && status != 407 {
        return None;
    }
    let name = if status == 407 {
        "proxy-authenticate"
    } else {
        "www-authenticate"
    };
    let authenticate: Vec<String> = headers
        .iter()
        .filter(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
        .collect();
    if authenticate.is_empty() {
        return None;
    }
    Some(Challenge {
        status,
        authenticate,
        origin: origin.to_string(),
    })
}

/// Basic credentials convenience (caller supplies user/password; the
/// engine encodes and marks the value sensitive).
#[must_use]
pub fn basic_credentials(user: &str, password: &str) -> String {
    use std::fmt::Write as _;
    let raw = format!("{user}:{password}");
    let encoded = crate::http::connect::base64_encode_public(raw.as_bytes());
    let mut out = String::new();
    let _ = write!(out, "Basic {encoded}");
    out
}

/// Shared, bounded origin-credential context for one job's requests
/// (§21.2, §29).
///
/// Every transfer path that talks to the same resolved origin reads the
/// current header set from one instance; a 401/407 challenge consults the
/// configured provider at most [`MAX_AUTH_STAGES`] times per job and
/// latches the returned headers for later attempts. Header values are
/// never logged.
#[derive(Clone)]
pub(crate) struct SharedCredentials {
    headers: Arc<RwLock<Vec<(String, String)>>>,
    provider: Option<Arc<dyn CredentialProvider>>,
    stages_used: Arc<AtomicU32>,
    /// Serializes challenge-stage decisions so concurrent workers cannot
    /// consume stages twice or interleave provider calls.
    stage_lock: Arc<Mutex<()>>,
    /// The challenge the provider already answered: concurrent workers
    /// observing the same challenge reuse the latched credentials instead
    /// of each consuming a bounded authentication stage.
    answered_challenge: Arc<Mutex<Option<Challenge>>>,
    /// Caller/provider-supplied credential headers may live under
    /// arbitrary names; when this is set, a cross-origin hop drops every
    /// caller-supplied header rather than guessing which are secrets.
    sensitive: bool,
}

impl std::fmt::Debug for SharedCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedCredentials")
            .field(
                "headers",
                &crate::redact::RedactedHeaders(
                    &self.headers.read().unwrap_or_else(|e| e.into_inner()),
                ),
            )
            .field("has_provider", &self.provider.is_some())
            .field("stages_used", &self.stages_used.load(Ordering::SeqCst))
            .field("sensitive", &self.sensitive)
            .finish()
    }
}

impl SharedCredentials {
    #[must_use]
    pub(crate) fn new(
        headers: Vec<(String, String)>,
        provider: Option<Arc<dyn CredentialProvider>>,
        sensitive: bool,
    ) -> Self {
        Self {
            headers: Arc::new(RwLock::new(headers)),
            provider,
            stages_used: Arc::new(AtomicU32::new(0)),
            stage_lock: Arc::new(Mutex::new(())),
            answered_challenge: Arc::new(Mutex::new(None)),
            sensitive,
        }
    }

    /// Current header set for this job's resolved origin.
    #[must_use]
    pub(crate) fn headers(&self) -> Vec<(String, String)> {
        self.headers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Whether this context's values may include credentials under
    /// arbitrary names.
    #[must_use]
    pub(crate) fn sensitive(&self) -> bool {
        self.sensitive
    }

    /// Consult the provider for one bounded challenge stage.
    ///
    /// Returns `Ok(true)` when new credentials were latched (the caller
    /// should retry the request), `Ok(false)` when no stage remains or the
    /// provider declined (the caller fails closed), and `Err` when the
    /// provider itself failed.
    pub(crate) fn provide(&self, challenge: &Challenge) -> Result<bool, DownloadError> {
        let Some(provider) = &self.provider else {
            return Ok(false);
        };
        let _stage = self.stage_lock.lock().unwrap_or_else(|e| e.into_inner());
        // The same challenge has already been answered: the latched headers
        // are what the retry must use, and no new stage is consumed.
        if self
            .answered_challenge
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|answered| answered == challenge)
        {
            return Ok(true);
        }
        if self.stages_used.load(Ordering::SeqCst) >= MAX_AUTH_STAGES {
            return Ok(false);
        }
        self.stages_used.fetch_add(1, Ordering::SeqCst);
        match provider.request(challenge)? {
            CredentialDecision::Headers(extra) => {
                let mut headers = self.headers.write().unwrap_or_else(|e| e.into_inner());
                for (name, value) in extra {
                    upsert_header(&mut headers, &name, value);
                }
                *self
                    .answered_challenge
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(challenge.clone());
                Ok(true)
            }
            CredentialDecision::Decline => Ok(false),
        }
    }
}

/// Insert or replace a header (case-insensitive name match).
pub(crate) fn upsert_header(headers: &mut Vec<(String, String)>, name: &str, value: String) {
    if let Some(existing) = headers
        .iter_mut()
        .find(|(seen, _)| seen.eq_ignore_ascii_case(name))
    {
        existing.1 = value;
    } else {
        headers.push((name.to_string(), value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn challenge_extracted_from_headers() {
        let headers = vec![
            ("content-type".to_string(), "text/html".to_string()),
            (
                "WWW-Authenticate".to_string(),
                "Bearer realm=\"x\"".to_string(),
            ),
            (
                "Www-Authenticate".to_string(),
                "Basic realm=\"y\"".to_string(),
            ),
        ];
        let ch = challenge_from_headers(401, "https://x.example", &headers).expect("challenge");
        assert_eq!(ch.status, 401);
        assert_eq!(ch.authenticate.len(), 2);
        assert_eq!(ch.scheme(), Some("bearer".to_string()));
        assert!(challenge_from_headers(200, "x", &headers).is_none());
        assert!(challenge_from_headers(404, "x", &headers).is_none());
    }

    #[test]
    fn stage_guard_bounds_retries() {
        let mut g = AuthStageGuard::new();
        assert!(g.can_provide());
        g.record();
        assert!(g.can_provide());
        g.record();
        assert!(!g.can_provide(), "§29: no unbounded auth retries");
    }

    #[test]
    fn basic_credentials_encode() {
        let v = basic_credentials("user", "pass");
        assert_eq!(v, "Basic dXNlcjpwYXNz");
    }
}
