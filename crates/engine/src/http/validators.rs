//! Resource validators: capture and comparison (§5.2, §11.3).

use serde::{Deserialize, Serialize};

/// The strongest available identity of the remote representation (§5.2).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceValidators {
    pub etag: Option<String>,
    pub etag_is_weak: bool,
    pub last_modified: Option<String>,
    /// `None` when unknown (§25).
    pub total_size: Option<u64>,
}

impl ResourceValidators {
    /// Capture from HTTP response headers (§10.1).
    #[must_use]
    pub fn from_headers(
        etag: Option<&str>,
        last_modified: Option<&str>,
        content_length: Option<u64>,
    ) -> Self {
        let (etag, weak) = etag
            .map(parse_etag)
            .unwrap_or((None, false));
        Self {
            etag,
            etag_is_weak: weak,
            last_modified: last_modified.map(str::to_string),
            total_size: content_length,
        }
    }

    /// Whether identity is strong enough for safe resume (§5.2): a strong
    /// ETag, or an eligible Last-Modified. A weak ETag is treated
    /// conservatively (not sufficient alone).
    #[must_use]
    pub fn resume_capable(&self) -> bool {
        if let Some(_etag) = &self.etag {
            if !self.etag_is_weak {
                return true;
            }
        }
        self.last_modified.is_some()
    }

    /// Compare with a freshly probed validator set (§26).
    ///
    /// Returns `Ok` when the generation plausibly matches; `Err(reason)`
    /// on detected change. `None` fields are not proof of change — they
    /// are simply absent evidence.
    pub fn same_generation(&self, other: &ResourceValidators) -> Result<(), String> {
        if let (Some(a), Some(b)) = (&self.etag, &other.etag) {
            if a != b {
                return Err(format!("ETag changed: {a} -> {b}"));
            }
        }
        if let (Some(a), Some(b)) = (&self.last_modified, &other.last_modified) {
            if a != b {
                return Err(format!("Last-Modified changed: {a} -> {b}"));
            }
        }
        if let (Some(a), Some(b)) = (self.total_size, other.total_size) {
            if a != b {
                return Err(format!("total size changed: {a} -> {b}"));
            }
        }
        Ok(())
    }
}

/// Split an ETag header value into (opaque-tag, weak-flag).
fn parse_etag(v: &str) -> (Option<String>, bool) {
    let v = v.trim();
    if let Some(rest) = v.strip_prefix("W/") {
        (Some(rest.trim().to_string()), true)
    } else {
        (Some(v.to_string()), false)
    }
}

/// A single resume validator header value pair (§11.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IfRangeHeader {
    pub value: String,
}

/// Build the `If-Range` header for a resume request (§11.3): the strong
/// ETag when available, otherwise Last-Modified.
#[must_use]
pub fn if_range_value(v: &ResourceValidators) -> Option<IfRangeHeader> {
    if let Some(etag) = &v.etag {
        if !v.etag_is_weak {
            return Some(IfRangeHeader {
                value: etag.clone(),
            });
        }
    }
    v.last_modified
        .as_ref()
        .map(|lm| IfRangeHeader {
            value: lm.clone(),
        })
}

/// Parse `Content-Range: bytes S-E/TOTAL` (§11.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentRange {
    pub start: u64,
    pub end: u64,
    /// `None` when the server sends `bytes S-E/*`.
    pub total: Option<u64>,
}

/// Parse failures carry the offending input for error context.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("malformed Content-Range: {0:?}")]
pub struct ContentRangeParseError(pub String);

pub fn parse_content_range(v: &str) -> Result<ContentRange, ContentRangeParseError> {
    let rest = v
        .trim()
        .strip_prefix("bytes ")
        .ok_or_else(|| ContentRangeParseError(v.to_string()))?;
    let (range_part, total_part) = rest
        .split_once('/')
        .ok_or_else(|| ContentRangeParseError(v.to_string()))?;
    let (start_s, end_s) = range_part
        .split_once('-')
        .ok_or_else(|| ContentRangeParseError(v.to_string()))?;
    let start: u64 = start_s
        .trim()
        .parse()
        .map_err(|_| ContentRangeParseError(v.to_string()))?;
    let end: u64 = end_s
        .trim()
        .parse()
        .map_err(|_| ContentRangeParseError(v.to_string()))?;
    let total = match total_part.trim() {
        "*" => None,
        t => Some(t.parse().map_err(|_| ContentRangeParseError(v.to_string()))?),
    };
    if end < start {
        return Err(ContentRangeParseError(v.to_string()));
    }
    Ok(ContentRange { start, end, total })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_strong_etag() {
        let v = ResourceValidators::from_headers(Some("\"abc123\""), Some("Mon, 22 Sep 2026 00:00:00 GMT"), Some(100));
        assert_eq!(v.etag.as_deref(), Some("\"abc123\""));
        assert!(!v.etag_is_weak);
        assert!(v.resume_capable());
    }

    #[test]
    fn weak_etag_is_conservative() {
        let v = ResourceValidators::from_headers(Some("W/\"abc\""), None, None);
        assert!(v.etag_is_weak);
        assert!(!v.resume_capable(), "weak ETag alone insufficient (§5.2)");
    }

    #[test]
    fn last_modified_only_is_capable() {
        let v = ResourceValidators::from_headers(None, Some("Mon, 22 Sep 2026 00:00:00 GMT"), None);
        assert!(v.resume_capable());
    }

    #[test]
    fn nothing_is_not_capable() {
        let v = ResourceValidators::from_headers(None, None, Some(1));
        assert!(!v.resume_capable());
    }

    #[test]
    fn if_range_prefers_strong_etag() {
        let v = ResourceValidators::from_headers(
            Some("\"e1\""),
            Some("date"),
            None,
        );
        assert_eq!(if_range_value(&v).map(|h| h.value), Some("\"e1\"".to_string()));
        // Weak ETag falls through to Last-Modified.
        let v = ResourceValidators::from_headers(Some("W/\"e\""), Some("date"), None);
        assert_eq!(if_range_value(&v).map(|h| h.value), Some("date".to_string()));
    }

    #[test]
    fn generation_change_detection() {
        let a = ResourceValidators::from_headers(Some("\"e1\""), None, Some(100));
        let b = ResourceValidators::from_headers(Some("\"e2\""), None, Some(100));
        assert!(a.same_generation(&b).is_err(), "ETag change detected");
        let size_b = ResourceValidators::from_headers(Some("\"e1\""), None, Some(101));
        assert!(a.same_generation(&size_b).is_err(), "size change detected");
        let same = ResourceValidators::from_headers(Some("\"e1\""), None, Some(100));
        assert!(a.same_generation(&same).is_ok());
    }

    #[test]
    fn content_range_parses() {
        let cr = parse_content_range("bytes 100-199/1234").expect("parse");
        assert_eq!(cr, ContentRange { start: 100, end: 199, total: Some(1234) });
        let star = parse_content_range("bytes 0-0/*").expect("parse star");
        assert_eq!(star.total, None);
        assert!(parse_content_range("bogus 1-2/3").is_err());
        assert!(parse_content_range("bytes 5-2/10").is_err(), "end < start");
    }
}