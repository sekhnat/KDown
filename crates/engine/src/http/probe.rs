//! HTTP probe: metadata discovery and range validation (§10).

use crate::error::DownloadError;
use crate::http::validators::ResourceValidators;

/// Probe results (§10.1).
#[derive(Debug, Clone, Default)]
pub struct ProbeMetadata {
    pub final_url: String,
    pub status: u16,
    pub total_size: Option<u64>,
    pub media_type: Option<String>,
    pub filename_hint: Option<String>,
    pub validators: ResourceValidators,
    pub accept_ranges: bool,
    /// Range behavior verified by an actual ranged request (§10.2).
    pub range_verified: bool,
    pub content_encoding: Option<String>,
    pub http_version: &'static str,
    pub auth_required: bool,
    /// Full Content-Range from the validating ranged request when present.
    pub content_range_total: Option<u64>,
}

impl ProbeMetadata {
    /// Segmentation eligibility gate (§10.3).
    #[must_use]
    pub fn segment_eligible(&self, threshold: u64, verify_range: bool) -> bool {
        let size_ok = self.total_size.is_some_and(|s| s >= threshold);
        let range_ok = if verify_range {
            self.range_verified
        } else {
            self.accept_ranges
        };
        size_ok && range_ok && self.status == 200
    }
}

/// Filename hint from Content-Disposition (§10.1) — sanitized separately
/// by the caller (§21.3); this only extracts the raw hint.
#[must_use]
pub fn filename_from_disposition(cd: Option<&str>) -> Option<String> {
    let cd = cd?;
    let after = cd.split_once(';')?.1;
    for part in after.split(';') {
        let part = part.trim();
        if let Some((k, v)) = part.split_once('=') {
            if k.trim().eq_ignore_ascii_case("filename") {
                let v = v.trim().trim_matches('"');
                if !v.is_empty() {
                    return Some(v.to_string());
                }
            }
        }
    }
    None
}

/// Interpret a probe response into metadata (shared by HEAD and ranged
/// GET paths).
#[allow(dead_code)] // wired by job controller in task 3.7
pub(crate) fn interpret(
    status: u16,
    final_url: &str,
    headers: &[(String, String)],
    body_size: Option<u64>,
    http_version: &'static str,
) -> Result<ProbeMetadata, DownloadError> {
    let header = |name: &str| -> Option<&str> {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    };
    // Centralized status table (§17.1): identical classification for probe
    // and transfer in both modes; 200/206 pass through as success.
    if status != 200 && status != 206 {
        return Err(crate::http::execution::status_to_error(status));
    }
    let content_length = header("content-length")
        .and_then(|v| v.parse().ok())
        .or(body_size);
    let validators =
        ResourceValidators::from_headers(header("etag"), header("last-modified"), content_length);
    Ok(ProbeMetadata {
        final_url: final_url.to_string(),
        status,
        total_size: content_length,
        media_type: header("content-type").map(str::to_string),
        filename_hint: filename_from_disposition(header("content-disposition")),
        validators,
        accept_ranges: header("accept-ranges")
            .map(|v| v.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false),
        range_verified: false,
        content_encoding: header("content-encoding").map(str::to_string),
        http_version,
        auth_required: status == 401,
        content_range_total: header("content-range").and_then(|v| {
            crate::http::validators::parse_content_range(v)
                .ok()
                .and_then(|cr| cr.total)
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers() -> Vec<(String, String)> {
        vec![
            ("etag".into(), "\"v1\"".into()),
            ("accept-ranges".into(), "bytes".into()),
            ("content-length".into(), "1024".into()),
            (
                "content-disposition".into(),
                "attachment; filename=\"report.pdf\"".into(),
            ),
        ]
    }

    #[test]
    fn interprets_full_response() {
        let md = interpret(200, "http://x/f", &headers(), None, "HTTP/1.1").expect("ok");
        assert_eq!(md.total_size, Some(1024));
        assert!(md.accept_ranges);
        assert!(!md.range_verified, "ranged GET has not run yet");
        assert_eq!(md.filename_hint.as_deref(), Some("report.pdf"));
        assert!(!md.segment_eligible(512, true));
    }

    #[test]
    fn eligibility_requires_verified_range() {
        let mut md = interpret(200, "http://x/f", &headers(), None, "HTTP/1.1").unwrap();
        md.range_verified = true;
        assert!(md.segment_eligible(512, true));
        assert!(!md.segment_eligible(2048, true), "below threshold");
    }

    #[test]
    fn error_statuses_map() {
        assert!(matches!(
            interpret(404, "http://x", &[], None, "HTTP/1.1"),
            Err(DownloadError::NotFound { status: 404 })
        ));
        assert!(matches!(
            interpret(503, "http://x", &[], None, "HTTP/1.1"),
            Err(DownloadError::Server { status: 503 })
        ));
        assert!(matches!(
            interpret(429, "http://x", &[], None, "HTTP/1.1"),
            Err(DownloadError::RateLimited { .. })
        ));
    }

    #[test]
    fn disposition_variants() {
        assert_eq!(
            filename_from_disposition(Some("attachment; filename=\"a b.bin\"")),
            Some("a b.bin".to_string())
        );
        assert_eq!(filename_from_disposition(Some("inline")), None);
        assert_eq!(filename_from_disposition(None), None);
    }
}
