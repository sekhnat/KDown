//! Sensitive-data redaction (§20, §35.3, D13).
//!
//! Applied at the formatting/logging boundary. The engine never embeds
//! credentials into `Display` output; this module is the enforcement point
//! for log records and for error messages that must quote URLs.

/// Redacts sensitive URL components and caller-marked query parameters.
#[derive(Debug, Clone, Default)]
pub struct Redactor {
    /// Case-insensitive query parameter names to treat as secrets.
    sensitive_query_params: Vec<String>,
}

/// Query/header names that are always redacted (§35.3).
pub const SENSITIVE_HEADER_NAMES: &[&str] = &[
    "authorization",
    "cookie",
    "set-cookie",
    "proxy-authorization",
];

impl Redactor {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an additional query parameter name as sensitive
    /// (e.g., signed-URL tokens). Matching is case-insensitive.
    #[must_use]
    pub fn with_sensitive_query_params(mut self, params: &[&str]) -> Self {
        self.sensitive_query_params
            .extend(params.iter().map(|p| p.to_ascii_lowercase()));
        self
    }

    /// Redact userinfo and sensitive query parameters from a URL string.
    ///
    /// `https://user:secret@example.com/x?token=abc&ok=1` becomes
    /// `https://example.com/x?token=REDACTED&ok=1`.
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
        if self.sensitive_query_params.is_empty() {
            return s.to_string();
        }
        let (base, query) = match s.split_once('?') {
            Some((b, q)) => (b, q),
            None => return s.to_string(),
        };
        let parts: Vec<String> = query
            .split('&')
            .map(|kv| match kv.split_once('=') {
                Some((k, _))
                    if self
                        .sensitive_query_params
                        .iter()
                        .any(|p| k.eq_ignore_ascii_case(p)) =>
                {
                    format!("{k}=REDACTED")
                }
                _ => kv.to_string(),
            })
            .collect();
        format!("{base}?{}", parts.join("&"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_userinfo() {
        let r = Redactor::new();
        assert_eq!(
            r.redact_url("https://alice:hunter2@example.com/file.bin?x=1"),
            "https://example.com/file.bin?x=1"
        );
    }

    #[test]
    fn redacts_marked_query_params_case_insensitively() {
        let r = Redactor::new().with_sensitive_query_params(&["token", "Signature"]);
        assert_eq!(
            r.redact_url("https://cdn.example.com/f?Token=abc&Signature=xyz&keep=1"),
            "https://cdn.example.com/f?Token=REDACTED&Signature=REDACTED&keep=1"
        );
    }

    #[test]
    fn leaves_clean_urls_untouched() {
        let r = Redactor::new().with_sensitive_query_params(&["token"]);
        assert_eq!(
            r.redact_url("https://example.com/plain"),
            "https://example.com/plain"
        );
        assert_eq!(
            r.redact_url("https://example.com/f?a=1&b=2"),
            "https://example.com/f?a=1&b=2"
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
