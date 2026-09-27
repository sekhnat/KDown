//! Sensitive-data redaction (§20, §35.3, D13).
//!
//! Applied at the formatting/logging boundary. The engine never embeds
//! credentials into `Display` output; this module is the enforcement point
//! for log records and for error messages that must quote URLs.

/// Redacts sensitive URL components for diagnostics.
///
/// The default policy is the safe one (§35.3, task 5.3): userinfo is
/// removed and **every** query value is masked, because the engine cannot
/// know which unfamiliar query key carries a secret (signed URLs put
/// credentials under arbitrary names). Callers that know their query keys
/// are not secret can opt down with
/// [`Redactor::with_marked_query_params_only`];
/// [`Redactor::with_sensitive_query_params`] marks extra names and stays
/// supported.
#[derive(Debug, Clone)]
pub struct Redactor {
    /// Case-insensitive query parameter names to treat as secrets.
    sensitive_query_params: Vec<String>,
    /// Mask every query value (the default); when `false`, only
    /// `sensitive_query_params` values are masked.
    mask_all_query_values: bool,
}

impl Default for Redactor {
    fn default() -> Self {
        Self {
            sensitive_query_params: Vec::new(),
            mask_all_query_values: true,
        }
    }
}

/// Query/header names that are always redacted (§35.3).
pub const SENSITIVE_HEADER_NAMES: &[&str] = &[
    "authorization",
    "cookie",
    "set-cookie",
    "proxy-authorization",
];

/// Fixed placeholder used wherever a credential-bearing value would
/// otherwise be formatted.
pub(crate) const REDACTED_VALUE: &str = "<redacted>";

/// Render a URL for diagnostics under the default policy: userinfo is
/// removed and every query value is masked (§35.3, task 5.3). `Debug`
/// impls and error messages that must quote a request target use this so
/// signed-URL secrets never reach logs; the URL sent on the wire is never
/// changed.
#[must_use]
pub(crate) fn redacted_url(url: &str) -> String {
    Redactor::new().redact_url(url)
}

/// Debug formatter for request-header collections.
///
/// Header NAMES stay visible so support and debugging can see what was
/// sent; every VALUE is replaced by a fixed placeholder. Callers can carry
/// credentials under any header name (not only the well-known
/// authorization ones), so no name-based list is consulted here: values
/// are simply never formatted.
pub(crate) struct RedactedHeaders<'a>(pub &'a [(String, String)]);

impl std::fmt::Debug for RedactedHeaders<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.0
                    .iter()
                    .map(|(name, _)| (name.as_str(), REDACTED_VALUE)),
            )
            .finish()
    }
}

impl Redactor {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an additional query parameter name as sensitive
    /// (e.g., signed-URL tokens). Matching is case-insensitive. Under the
    /// default policy every value is already masked, so this is additive
    /// and retained for callers that opt down with
    /// [`Redactor::with_marked_query_params_only`].
    #[must_use]
    pub fn with_sensitive_query_params(mut self, params: &[&str]) -> Self {
        self.sensitive_query_params
            .extend(params.iter().map(|p| p.to_ascii_lowercase()));
        self
    }

    /// Opt down to masking only userinfo and the marked query parameters.
    ///
    /// Every other query value is left readable, so use this only when the
    /// caller knows none of them are secret. The default masks every
    /// query value because an unfamiliar key's secrecy cannot be
    /// determined safely (task 5.3).
    #[must_use]
    pub fn with_marked_query_params_only(mut self) -> Self {
        self.mask_all_query_values = false;
        self
    }

    /// Redact userinfo and query values from a URL string for diagnostics.
    ///
    /// Under the default policy
    /// `https://user:secret@example.com/x?token=abc&ok=1` becomes
    /// `https://example.com/x?token=REDACTED&ok=REDACTED`.
    #[must_use]
    pub fn redact_url(&self, url: &str) -> String {
        let (scheme, rest) = match url.split_once("://") {
            Some((s, r)) => (s, r),
            None => return self.redact_query_only(url),
        };
        // Split authority from path+query at the first '/'.
        let (authority, path_query) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        // Strip userinfo from the authority.
        let authority = match authority.rfind('@') {
            Some(at) if !authority[..at].is_empty() => &authority[at + 1..],
            _ => authority,
        };
        let cleaned = format!("{scheme}://{authority}{path_query}");
        self.redact_query_only(&cleaned)
    }

    fn redact_query_only(&self, s: &str) -> String {
        let (base, query) = match s.split_once('?') {
            Some((b, q)) => (b, q),
            None => return s.to_string(),
        };
        let parts: Vec<String> = query
            .split('&')
            .map(|kv| match kv.split_once('=') {
                Some((k, _)) if self.masks_query_value(k) => format!("{k}=REDACTED"),
                _ => kv.to_string(),
            })
            .collect();
        format!("{base}?{}", parts.join("&"))
    }

    fn masks_query_value(&self, name: &str) -> bool {
        self.mask_all_query_values
            || self
                .sensitive_query_params
                .iter()
                .any(|p| name.eq_ignore_ascii_case(p))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_userinfo_and_every_query_value_by_default() {
        let r = Redactor::new();
        assert_eq!(
            r.redact_url("https://alice:hunter2@example.com/file.bin?x=1&token=abc"),
            "https://example.com/file.bin?x=REDACTED&token=REDACTED"
        );
    }

    #[test]
    fn redacts_marked_query_params_case_insensitively() {
        let r = Redactor::new().with_sensitive_query_params(&["token", "Signature"]);
        assert_eq!(
            r.redact_url("https://cdn.example.com/f?Token=abc&Signature=xyz&keep=1"),
            "https://cdn.example.com/f?Token=REDACTED&Signature=REDACTED&keep=REDACTED"
        );
    }

    #[test]
    fn marked_only_policy_keeps_unmarked_values() {
        let r = Redactor::new()
            .with_sensitive_query_params(&["signature"])
            .with_marked_query_params_only();
        assert_eq!(
            r.redact_url("https://cdn.example.com/f?Signature=xyz&page=2"),
            "https://cdn.example.com/f?Signature=REDACTED&page=2"
        );
    }

    #[test]
    fn unknown_keys_are_masked_without_any_configuration() {
        // The default policy cannot know whether `X-Amz-Signature` or an
        // unfamiliar key is secret, so every value is masked (task 5.3).
        let r = Redactor::new();
        assert_eq!(
            r.redact_url("https://cdn.example.com/f?X-Amz-Signature=secret&next=/x?y"),
            "https://cdn.example.com/f?X-Amz-Signature=REDACTED&next=REDACTED"
        );
    }

    #[test]
    fn leaves_clean_urls_untouched() {
        let r = Redactor::new();
        assert_eq!(
            r.redact_url("https://example.com/plain"),
            "https://example.com/plain"
        );
        assert_eq!(
            r.redact_url("https://example.com/f?a=1&b=2"),
            "https://example.com/f?a=REDACTED&b=REDACTED"
        );
    }

    #[test]
    fn header_names_include_sensitive_set() {
        for name in SENSITIVE_HEADER_NAMES {
            assert!([
                "authorization",
                "cookie",
                "set-cookie",
                "proxy-authorization"
            ]
            .contains(name));
        }
    }
}

#[test]
fn redacted_headers_show_names_never_values() {
    let headers = vec![
        ("authorization".to_string(), "Bearer sentinel-a".to_string()),
        ("x-api-key".to_string(), "sentinel-b".to_string()),
    ];
    let rendered = format!("{:?}", RedactedHeaders(&headers));
    assert!(!rendered.contains("sentinel-a"));
    assert!(!rendered.contains("sentinel-b"));
    assert!(rendered.contains("authorization"));
    assert!(rendered.contains("x-api-key"));
}

#[test]
fn redact_url_strips_proxy_userinfo() {
    // Proxy URLs can embed credentials; debug formatting of proxy
    // configuration must not print them.
    let r = Redactor::new();
    let rendered = r.redact_url("http://user:proxy-secret@proxy.example:8080");
    assert!(!rendered.contains("proxy-secret"));
    assert!(rendered.contains("proxy.example:8080"));
}
