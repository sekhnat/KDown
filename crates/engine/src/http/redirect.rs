//! Redirect handling with safety policy (§11.1, §21.2).

use std::collections::HashSet;

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
    pub strip_credentials: bool,
}

/// Redirect decision compares actions structurally; `Reject` errors are
/// compared only by category.
#[derive(Debug)]
#[non_exhaustive]
pub enum RedirectAction {
    Follow { location: String },
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

/// Tracks redirect chains across hops (loop detection, §11.1).
#[derive(Debug, Default)]
pub struct RedirectTracker {
    hops: u32,
    visited: HashSet<String>,
    policy: RedirectPolicy,
}

impl RedirectTracker {
    #[must_use]
    pub fn new(policy: RedirectPolicy) -> Self {
        Self {
            hops: 0,
            visited: HashSet::new(),
            policy,
        }
    }

    /// Evaluate a response's `Location` against policy.
    ///
    /// `current_url` and `credentials_header` inform downgrade checks and
    /// cross-origin stripping. Errors are structured (§20 RedirectError).
    #[must_use]
    pub fn decide(
        &mut self,
        status: u16,
        location: Option<&str>,
        current_url: &str,
        current_headers: &[(String, String)],
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
        if !self.visited.insert(loc.to_string()) {
            return RedirectDecision {
                action: RedirectAction::Reject(DownloadError::Redirect(
                    "redirect loop detected".into(),
                )),
                strip_credentials: false,
            };
        }
        // Downgrade protection (§11.1).
        if self.policy.deny_downgrade
            && current_url.starts_with("https://")
            && loc.starts_with("http://")
        {
            return RedirectDecision {
                action: RedirectAction::Reject(DownloadError::Redirect(
                    "HTTPS to HTTP downgrade denied".into(),
                )),
                strip_credentials: false,
            };
        }
        // Cross-origin credential stripping (§21.2).
        let strip = !self.policy.forward_cross_origin_credentials
            && origin_of(current_url) != origin_of(loc)
            && current_headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("authorization") || k.eq_ignore_ascii_case("cookie"));

        RedirectDecision {
            action: RedirectAction::Follow {
                location: loc.to_string(),
            },
            strip_credentials: strip,
        }
    }
}

/// Extract `scheme://authority` for origin comparison.
fn origin_of(url: &str) -> String {
    let rest = url.split("://").nth(1).unwrap_or("");
    format!(
        "{}://{}",
        url.split("://").next().unwrap_or(""),
        rest.split(['/', '?', '#']).next().unwrap_or("")
    )
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
            let d = follow_chain(&mut t, "http://a.example/x", &format!("http://b.example/h{hop}"));
            assert!(matches!(d.action, RedirectAction::Follow { .. }), "hop {hop}");
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
            matches!(d2.action, RedirectAction::Reject(DownloadError::Redirect(_))),
            "repeated location must be detected as a loop"
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

        let mut t2 = RedirectTracker::new(RedirectPolicy::default());
        let d2 = t2.decide(
            302,
            Some("http://origin.example/g"),
            "http://origin.example/f",
            &headers,
        );
        assert!(!d2.strip_credentials, "same-origin hop keeps credentials");

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
}