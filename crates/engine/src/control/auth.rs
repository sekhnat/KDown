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
