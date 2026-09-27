//! Redirect handling with safety policy (§11.1, §21.2).
//!
//! Every hop is resolved against the URL that produced the response before
//! any policy decision, so loop detection, downgrade protection and
//! credential scoping all compare real, normalized target origins instead
//! of the raw `Location` text. Credential stripping is a property of the
//! outgoing request's credential scope: it never depends on response
//! headers, and once an origin boundary is crossed the strip stays latched
//! for the remainder of the chain (including a later return to the
//! original origin) unless the caller explicitly opted into cross-origin
//! forwarding.

use std::collections::HashSet;

use hyper::Uri;

use crate::error::DownloadError;
use crate::redact::Redactor;

/// Policy knobs for redirect following (§11.1).
#[derive(Debug, Clone)]
pub struct RedirectPolicy {
    pub max_redirects: u32,
    /// Deny HTTPS -> HTTP redirects (default true, §11.1).
    pub deny_downgrade: bool,
    /// Forward Authorization/Cookie across origins (default false, §21.2).
    pub forward_cross_origin_credentials: bool,
}

impl Default for RedirectPolicy {
    fn default() -> Self {
        Self {
            max_redirects: 10,
            deny_downgrade: true,
            forward_cross_origin_credentials: false,
        }
    }
}

/// Outcome of evaluating one redirect hop.
#[derive(Debug, PartialEq)]
pub struct RedirectDecision {
    pub action: RedirectAction,
    /// Headers stripped from the redirected request, per policy (§21.2).
    ///
    /// True only when the chain has actually crossed an origin boundary and
    /// the outgoing request carries origin credentials. Callers with
    /// caller-marked sensitive header sets should consult
    /// [`RedirectTracker::cross_origin_seen`] so they can drop those too.
    pub strip_credentials: bool,
}

/// Redirect decision compares actions structurally; `Reject` errors are
/// compared only by category.
#[derive(Debug)]
#[non_exhaustive]
pub enum RedirectAction {
    /// Follow the redirect. `location` is the *resolved absolute URL* of the
    /// next hop (relative, protocol-relative and dot-segment locations are
    /// resolved against the current request URL before comparison).
    Follow {
        location: String,
    },
    /// Not a redirect response; treat as final.
    Final,
    /// Redirect chain is invalid; the engine must fail.
    Reject(DownloadError),
}

impl PartialEq for RedirectAction {
    fn eq(&self, other: &Self) -> bool {
        use RedirectAction::*;
        match (self, other) {
            (Follow { location: a }, Follow { location: b }) => a == b,
            (Final, Final) => true,
            (Reject(a), Reject(b)) => a.category() == b.category(),
            _ => false,
        }
    }
}

/// Header names that carry origin credentials and must never be forwarded
/// to a different origin by default (§21.2).
///
/// `proxy-authorization` is included because proxy credentials must only
/// ever reach the configured proxy; the transport drops it from origin
/// requests entirely.
#[must_use]
pub fn is_origin_credential_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("authorization")
        || name.eq_ignore_ascii_case("cookie")
        || name.eq_ignore_ascii_case("proxy-authorization")
}

/// Header set to send to `target_url` for a request whose credentials were
/// configured for `original_url` (§21.2).
///
/// Same-origin targets keep every header. Cross-origin targets drop the
/// classified origin credentials, and when the request is caller-marked
/// sensitive they drop every caller-supplied header (values may all be
/// credentials under arbitrary names). `forward_cross_origin_credentials`
/// is the documented opt-in that disables the strip.
#[must_use]
pub fn scoped_request_headers(
    original_url: &str,
    target_url: &str,
    headers: &[(String, String)],
    sensitive: bool,
    forward_cross_origin_credentials: bool,
) -> Vec<(String, String)> {
    let cross_origin = match (origin_of(original_url), origin_of(target_url)) {
        (Some(from), Some(to)) => from != to,
        // Unparsable origins cannot be proven same-origin: fail closed.
        _ => true,
    };
    if !cross_origin || forward_cross_origin_credentials {
        return headers.to_vec();
    }
    headers
        .iter()
        .filter(|(name, _)| {
            if is_origin_credential_header(name) {
                return false;
            }
            !sensitive
        })
        .cloned()
        .collect()
}

/// Tracks redirect chains across hops (loop detection, §11.1) and latches
/// the best-effort credential strip once an origin boundary is crossed
/// (§21.2).
#[derive(Debug, Default)]
pub struct RedirectTracker {
    hops: u32,
    visited: HashSet<String>,
    policy: RedirectPolicy,
    cross_origin_seen: bool,
}

impl RedirectTracker {
    #[must_use]
    pub fn new(policy: RedirectPolicy) -> Self {
        Self {
            hops: 0,
            visited: HashSet::new(),
            policy,
            cross_origin_seen: false,
        }
    }

    /// Evaluate a response's `Location` against policy.
    ///
    /// `current_url` is the URL of the request that produced the response;
    /// `outgoing_headers` are the headers that were sent with *that request*
    /// (never the response headers). The stripping decision depends only on
    /// the resolved target origin and the outgoing request's credential
    /// scope. Errors are structured (§20 RedirectError).
    #[must_use]
    pub fn decide(
        &mut self,
        status: u16,
        location: Option<&str>,
        current_url: &str,
        outgoing_headers: &[(String, String)],
    ) -> RedirectDecision {
        let is_redirect = matches!(status, 301 | 302 | 303 | 307 | 308);
        if !is_redirect {
            return RedirectDecision {
                action: RedirectAction::Final,
                strip_credentials: false,
            };
        }
        let Some(loc) = location else {
            return RedirectDecision {
                action: RedirectAction::Reject(DownloadError::Redirect(
                    "redirect status without Location header".into(),
                )),
                strip_credentials: false,
            };
        };
        // Resolve before anything else: relative locations, protocol-relative
        // locations and dot segments all become absolute target URLs, so
        // loop detection and origin comparison see the real hop.
        let target = match resolve_redirect(current_url, loc) {
            Ok(target) => target,
            Err(error) => {
                return RedirectDecision {
                    action: RedirectAction::Reject(error),
                    strip_credentials: false,
                };
            }
        };
        self.hops += 1;
        if self.hops > self.policy.max_redirects {
            return RedirectDecision {
                action: RedirectAction::Reject(DownloadError::Redirect(format!(
                    "exceeded {} redirects",
                    self.policy.max_redirects
                ))),
                strip_credentials: false,
            };
        }
        if !self.visited.insert(target.clone()) {
            return RedirectDecision {
                action: RedirectAction::Reject(DownloadError::Redirect(
                    "redirect loop detected".into(),
                )),
                strip_credentials: false,
            };
        }
        // Downgrade protection (§11.1) on the resolved target.
        if self.policy.deny_downgrade
            && scheme_of(current_url).as_deref() == Some("https")
            && scheme_of(&target).as_deref() == Some("http")
        {
            return RedirectDecision {
                action: RedirectAction::Reject(DownloadError::Redirect(
                    "HTTPS to HTTP downgrade denied".into(),
                )),
                strip_credentials: false,
            };
        }
        // Cross-origin credential scoping (§21.2): the boundary is decided by
        // normalized scheme/host/effective-port comparison of the resolved
        // target, and the strip latches for every later hop.
        let cross_origin = match (origin_of(current_url), origin_of(&target)) {
            (Some(from), Some(to)) => from != to,
            // An unparsable origin cannot be proven same-origin: fail closed
            // by treating the hop as a boundary.
            _ => true,
        };
        if cross_origin && !self.policy.forward_cross_origin_credentials {
            self.cross_origin_seen = true;
        }
        let has_credentials = outgoing_headers
            .iter()
            .any(|(name, _)| is_origin_credential_header(name));

        RedirectDecision {
            action: RedirectAction::Follow { location: target },
            strip_credentials: self.cross_origin_seen && has_credentials,
        }
    }

    /// Whether this chain has crossed an origin boundary and therefore must
    /// keep dropping origin credentials on every later hop.
    #[must_use]
    pub fn cross_origin_seen(&self) -> bool {
        self.cross_origin_seen
    }
}

/// Scheme of an absolute URL, lowercased, when present.
fn scheme_of(url: &str) -> Option<String> {
    Uri::try_from(url)
        .ok()
        .and_then(|uri| uri.scheme_str().map(str::to_ascii_lowercase))
}

/// Default port for a URL scheme.
fn default_port(scheme: &str) -> u16 {
    if scheme.eq_ignore_ascii_case("https") {
        443
    } else {
        80
    }
}

/// Normalized `scheme://host:effective-port` origin for comparison.
///
/// The host is lowercased and a missing port is normalized to the scheme
/// default, so `http://example.com`, `http://EXAMPLE.com:80` and
/// `http://example.com/` compare equal. Userinfo is ignored (it is not part
/// of the origin). Returns `None` when the URL cannot be parsed as an
/// absolute URL with a host.
fn origin_of(url: &str) -> Option<String> {
    let uri = Uri::try_from(url).ok()?;
    let scheme = uri.scheme_str()?.to_ascii_lowercase();
    let host = uri.host()?.to_ascii_lowercase();
    let port = uri.port_u16().unwrap_or_else(|| default_port(&scheme));
    Some(format!("{scheme}://{host}:{port}"))
}

/// Resolve a `Location` value against the current request URL (RFC 7231
/// §7.1.2) and return an absolute target URL.
///
/// Absolute URLs pass through; protocol-relative (`//host/x`), absolute-path
/// (`/x`), query-only (`?q`) and relative-path (`x`) locations are resolved
/// against the current URL, including `.`/`..` segment collapsing. Fragments
/// are dropped: they are never sent with a request.
pub(crate) fn resolve_redirect(current: &str, location: &str) -> Result<String, DownloadError> {
    let location = location.trim();
    if location.is_empty() {
        return Err(DownloadError::Redirect("empty Location header".into()));
    }
    if location.starts_with("//") {
        let base = Uri::try_from(current)
            .map_err(|e| DownloadError::InvalidUrl(format!("{current}: {e}")))?;
        let scheme = base.scheme_str().unwrap_or("http");
        return Ok(format!("{scheme}:{location}"));
    }
    if let Some((scheme, _rest)) = location.split_once("://") {
        let valid_scheme = !scheme.is_empty()
            && scheme
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic())
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
        if valid_scheme {
            return Ok(location.to_string());
        }
        return Err(DownloadError::InvalidUrl(format!(
            "invalid redirect scheme in {location}"
        )));
    }
    let base = Uri::try_from(current)
        .map_err(|e| DownloadError::InvalidUrl(format!("{current}: {e}")))?;
    let authority = base
        .authority()
        .ok_or_else(|| DownloadError::InvalidUrl(current.to_string()))?
        .as_str();
    let scheme = base.scheme_str().unwrap_or("http");
    let without_fragment = location.split('#').next().unwrap_or(location);
    let (loc_path, loc_query) = match without_fragment.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (without_fragment, None),
    };
    let path = if loc_path.is_empty() {
        // Query-only (or fragment-only) redirect: keep the current path.
        base.path().to_string()
    } else if loc_path.starts_with('/') {
        normalize_dot_segments(loc_path)
    } else {
        // Relative path: merge with the current path's directory portion.
        let base_path = base.path();
        let dir = base_path.rfind('/').map_or("/", |i| &base_path[..=i]);
        normalize_dot_segments(&format!("{dir}{loc_path}"))
    };
    let mut target = format!("{scheme}://{authority}{path}");
    if let Some(query) = loc_query {
        target.push('?');
        target.push_str(query);
    }
    Ok(target)
}

/// Collapse `.` and `..` segments in an absolute path.
fn normalize_dot_segments(path: &str) -> String {
    let trailing_slash = path.ends_with('/');
    let mut segments: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            other => segments.push(other),
        }
    }
    let mut normalized = String::from("/");
    normalized.push_str(&segments.join("/"));
    if trailing_slash && !normalized.ends_with('/') {
        normalized.push('/');
    }
    normalized
}

/// Redact a redirect chain for logging (§35.3): never log signed URLs or
/// credentials. Uses the shared Redactor.
#[must_use]
pub fn redact_redirect_chain(chain: &[String], redactor: &Redactor) -> Vec<String> {
    chain.iter().map(|u| redactor.redact_url(u)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn follow_chain(tracker: &mut RedirectTracker, from: &str, loc: &str) -> RedirectDecision {
        tracker.decide(302, Some(loc), from, &[])
    }

    #[test]
    fn follows_bounded_chain() {
        let mut t = RedirectTracker::new(RedirectPolicy::default());
        for hop in 0..10u32 {
            let d = follow_chain(
                &mut t,
                "http://a.example/x",
                &format!("http://b.example/h{hop}"),
            );
            assert!(
                matches!(d.action, RedirectAction::Follow { .. }),
                "hop {hop}"
            );
        }
        let d = follow_chain(&mut t, "http://b.example/x", "http://c.example/z");
        assert!(
            matches!(d.action, RedirectAction::Reject(DownloadError::Redirect(_))),
            "hop 11 must exceed max_redirects"
        );
    }

    #[test]
    fn loop_detected() {
        let mut t = RedirectTracker::new(RedirectPolicy::default());
        let d1 = follow_chain(&mut t, "http://a/x", "http://a/y");
        assert!(matches!(d1.action, RedirectAction::Follow { .. }));
        let d2 = follow_chain(&mut t, "http://a/y", "http://a/y");
        assert!(
            matches!(
                d2.action,
                RedirectAction::Reject(DownloadError::Redirect(_))
            ),
            "repeated location must be detected as a loop"
        );
    }

    #[test]
    fn loop_detected_across_relative_and_absolute_forms() {
        let mut t = RedirectTracker::new(RedirectPolicy::default());
        // Relative then equivalent absolute form of the same target must be
        // recognized as a loop, not two distinct hops.
        let d1 = follow_chain(&mut t, "http://a/x", "/same");
        assert!(matches!(d1.action, RedirectAction::Follow { .. }));
        let d2 = follow_chain(&mut t, "http://a/same", "http://a/same");
        assert!(
            matches!(
                d2.action,
                RedirectAction::Reject(DownloadError::Redirect(_))
            ),
            "equivalent targets must be detected as a loop"
        );
    }

    #[test]
    fn https_downgrade_denied_by_default() {
        let mut t = RedirectTracker::new(RedirectPolicy::default());
        let d = follow_chain(&mut t, "https://secure.example/f", "http://plain.example/f");
        assert!(matches!(
            d.action,
            RedirectAction::Reject(DownloadError::Redirect(_))
        ));
    }

    #[test]
    fn downgrade_allowed_when_policy_allows() {
        let mut t = RedirectTracker::new(RedirectPolicy {
            deny_downgrade: false,
            ..RedirectPolicy::default()
        });
        let d = follow_chain(&mut t, "https://secure.example/f", "http://plain.example/f");
        assert!(matches!(d.action, RedirectAction::Follow { .. }));
    }

    #[test]
    fn relative_location_resolves_against_current_url() {
        let mut t = RedirectTracker::new(RedirectPolicy::default());
        let d = t.decide(302, Some("../other/file.bin"), "http://a.example/dir/one.bin", &[]);
        assert_eq!(
            d.action,
            RedirectAction::Follow {
                location: "http://a.example/other/file.bin".to_string()
            }
        );
        let mut t2 = RedirectTracker::new(RedirectPolicy::default());
        let d2 = t2.decide(302, Some("/root.bin"), "http://a.example/dir/one.bin", &[]);
        assert_eq!(
            d2.action,
            RedirectAction::Follow {
                location: "http://a.example/root.bin".to_string()
            }
        );
        let mut t3 = RedirectTracker::new(RedirectPolicy::default());
        let d3 = t3.decide(302, Some("?token=1"), "http://a.example/dir/one.bin?old=2", &[]);
        assert_eq!(
            d3.action,
            RedirectAction::Follow {
                location: "http://a.example/dir/one.bin?token=1".to_string()
            }
        );
    }

    #[test]
    fn credentials_stripped_cross_origin() {
        let mut t = RedirectTracker::new(RedirectPolicy::default());
        let headers = vec![("authorization".to_string(), "Bearer x".to_string())];
        let d = t.decide(
            302,
            Some("http://other.example/f"),
            "http://origin.example/f",
            &headers,
        );
        assert!(d.strip_credentials, "cross-origin hop strips credentials");
        assert!(t.cross_origin_seen());

        let mut t2 = RedirectTracker::new(RedirectPolicy::default());
        let d2 = t2.decide(
            302,
            Some("http://origin.example/g"),
            "http://origin.example/f",
            &headers,
        );
        assert!(!d2.strip_credentials, "same-origin hop keeps credentials");
        assert!(!t2.cross_origin_seen());

        // Policy may explicitly allow forwarding.
        let mut t3 = RedirectTracker::new(RedirectPolicy {
            forward_cross_origin_credentials: true,
            ..RedirectPolicy::default()
        });
        let d3 = t3.decide(
            302,
            Some("http://other.example/f"),
            "http://origin.example/f",
            &headers,
        );
        assert!(!d3.strip_credentials, "explicit policy allows forwarding");
        assert!(!t3.cross_origin_seen());
    }

    #[test]
    fn strip_does_not_depend_on_response_headers() {
        // The old implementation only stripped when the *response* carried
        // credential headers; a cross-origin hop must strip even when the
        // redirect response is completely plain (the caller passes the
        // outgoing request headers here).
        let mut t = RedirectTracker::new(RedirectPolicy::default());
        let outgoing = vec![("Cookie".to_string(), "session=1".to_string())];
        let d = t.decide(302, Some("http://other.example/f"), "http://origin.example/f", &outgoing);
        assert!(d.strip_credentials);
    }

    #[test]
    fn sensitive_custom_headers_latch_across_hops() {
        // Relative hop first (same origin), then a cross-origin hop: the
        // latch survives, and the tracker reports it for caller-marked
        // sensitive header sets even when no known credential name matches.
        let mut t = RedirectTracker::new(RedirectPolicy::default());
        let d1 = t.decide(302, Some("/next"), "http://origin.example/f", &[]);
        assert!(!d1.strip_credentials);
        let d2 = t.decide(
            302,
            Some("http://other.example/f"),
            "http://origin.example/f/next",
            &[],
        );
        assert!(!d2.strip_credentials, "no known credentials to strip");
        assert!(t.cross_origin_seen(), "boundary is still latched");
        let d3 = t.decide(
            302,
            Some("http://origin.example/back"),
            "http://other.example/f",
            &[],
        );
        assert!(t.cross_origin_seen(), "returning home keeps the latch");
        assert!(!d3.strip_credentials);
    }

    #[test]
    fn ipv6_and_default_ports_compare_normalized() {
        let mut t = RedirectTracker::new(RedirectPolicy::default());
        let headers = vec![("authorization".to_string(), "Bearer x".to_string())];
        // Same origin written with an explicit default port.
        let d = t.decide(
            302,
            Some("http://origin.example:80/f"),
            "http://origin.example/f",
            &headers,
        );
        assert!(!d.strip_credentials, "default port must normalize to 80");

        let mut t2 = RedirectTracker::new(RedirectPolicy::default());
        let d2 = t2.decide(
            302,
            Some("https://origin.example:443/f"),
            "https://origin.example/f",
            &headers,
        );
        assert!(!d2.strip_credentials, "default port must normalize to 443");
    }

    #[test]
    fn non_redirect_status_is_final() {
        let mut t = RedirectTracker::new(RedirectPolicy::default());
        let d = t.decide(200, Some("http://x/y"), "http://a/b", &[]);
        assert_eq!(d.action, RedirectAction::Final);
    }

    #[test]
    fn missing_location_is_structured_error() {
        let mut t = RedirectTracker::new(RedirectPolicy::default());
        let d = t.decide(302, None, "http://a/b", &[]);
        assert!(matches!(
            d.action,
            RedirectAction::Reject(DownloadError::Redirect(_))
        ));
    }

    #[test]
    fn empty_location_is_structured_error() {
        let mut t = RedirectTracker::new(RedirectPolicy::default());
        let d = t.decide(302, Some("   "), "http://a/b", &[]);
        assert!(matches!(
            d.action,
            RedirectAction::Reject(DownloadError::Redirect(_))
        ));
    }
}
