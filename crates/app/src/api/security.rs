//! Security primitives for the local loopback API.
//!
//! Loopback binding alone is not treated as sufficient protection: every
//! mutation must arrive as JSON, carry the per-process CSRF token, present
//! a same-origin `Origin`, and pass Fetch Metadata checks. Session data is
//! only released to accepted loopback `Host` values.

use axum::http::{header, HeaderMap};

/// Per-process security material: one random 256-bit CSRF token plus the
/// canonical origin the SPA is served from.
#[derive(Clone, Debug)]
pub struct SecurityContext {
    csrf_token: String,
    origin: String,
}

impl SecurityContext {
    /// Generates the process-lifetime token. 256 bits from the OS CSPRNG.
    pub fn generate(origin: String) -> Self {
        use rand::TryRngCore;
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng
            .try_fill_bytes(&mut bytes)
            .expect("OS CSPRNG must be available");
        let csrf_token = bytes.iter().map(|b| format!("{b:02x}")).collect();
        Self { csrf_token, origin }
    }

    /// The per-process CSRF token; never logged.
    pub fn csrf_token(&self) -> &str {
        &self.csrf_token
    }

    /// The canonical origin of the UI/API surface.
    pub fn origin(&self) -> &str {
        &self.origin
    }
}

/// Hosts the service accepts. Only loopback names, optionally with a port.
pub fn host_is_accepted(host: &str) -> bool {
    let authority = host.rsplit_once(':').map_or(host, |(h, _)| h);
    matches!(authority, "127.0.0.1" | "localhost" | "[::1]" | "::1") && host.starts_with(authority)
}

/// The mutation contract from the design: JSON body, process CSRF token,
/// same-origin `Origin`, and acceptable Fetch Metadata.
pub fn mutation_is_allowed(headers: &HeaderMap, security: &SecurityContext) -> bool {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    if content_type.is_some_and(|value| value != "application/json") {
        // Present but wrong content type is a media-type problem.
        return false;
    }
    let csrf_ok =
        headers.get("x-kdown-csrf").and_then(|v| v.to_str().ok()) == Some(security.csrf_token());
    let origin_ok = origin_matches_host(headers);
    let fetch_metadata_ok = headers
        .get("sec-fetch-site")
        .and_then(|v| v.to_str().ok())
        .is_none_or(|v| v == "same-origin");
    csrf_ok && origin_ok && fetch_metadata_ok
}

/// Whether the mutation's `Origin` authority equals the request's `Host`:
/// the same-origin invariant the browser enforces for us.
pub fn origin_matches_host(headers: &HeaderMap) -> bool {
    let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok());
    let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
    match (origin, host) {
        (Some(origin), Some(host)) => {
            origin.strip_prefix("http://") == Some(host)
                || origin.strip_prefix("https://") == Some(host)
        }
        _ => false,
    }
}
