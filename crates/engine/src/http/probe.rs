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
    /// Ordinary (non-extended) `filename` parameter of the first
    /// Content-Disposition header, when one parsed. Ordered fallback for
    /// automatic filename resolution when the extended hint sanitizes away
    /// (§10.1); never populated from the validating ranged GET.
    pub plain_filename_hint: Option<String>,
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

/// Ordered filename candidates from the FIRST Content-Disposition header:
/// `(extended_or_plain, plain)` where `extended_or_plain` is the first
/// well-formed `filename*` (RFC 5987/8187 ext-value) or, when none parsed,
/// the first well-formed ordinary `filename`; `plain` is always the first
/// well-formed ordinary `filename` so a syntactically valid `filename*`
/// that sanitizes away cannot lose the plain fallback (§10.1, §21.3).
/// Raw values are returned; sanitization happens in the caller.
///
/// Duplicate parameter occurrences use the first well-formed one of each
/// name. Malformed parameters are skipped without failing the download.
/// Parsing is linear in the (header-limit-bounded) input and never panics
/// on hostile values (§21.4).
#[must_use]
pub fn disposition_filename_candidates(cd: Option<&str>) -> (Option<String>, Option<String>) {
    let Some(cd) = cd else {
        return (None, None);
    };
    // Disposition type is advisory: parameters start after the first ';'.
    let Some((_, parameters)) = cd.split_once(';') else {
        return (None, None);
    };
    let mut extended: Option<String> = None;
    let mut plain: Option<String> = None;
    for parameter in split_disposition_parameters(parameters) {
        let Some((raw_name, raw_value)) = parameter.split_once('=') else {
            continue;
        };
        let name = raw_name.trim();
        if name.eq_ignore_ascii_case("filename*") && extended.is_none() {
            if let Some(value) = parse_extended_value(raw_value.trim()) {
                if !value.is_empty() {
                    extended = Some(value);
                }
            }
        } else if name.eq_ignore_ascii_case("filename") && plain.is_none() {
            if let Some(value) = parse_plain_value(raw_value.trim()) {
                if !value.is_empty() {
                    plain = Some(value);
                }
            }
        }
        if extended.is_some() && plain.is_some() {
            break;
        }
    }
    (extended.or_else(|| plain.clone()), plain)
}

/// Filename hint from Content-Disposition (§10.1) — the first ordered
/// candidate ([`disposition_filename_candidates`]); sanitized separately
/// by the caller (§21.3); this only extracts the raw hint.
#[must_use]
pub fn filename_from_disposition(cd: Option<&str>) -> Option<String> {
    disposition_filename_candidates(cd).0
}

/// Split a parameter list on ';', respecting quoted strings and quoted-pair
/// escapes so semicolons inside `filename="a;b"` do not split parameters.
/// The escape backslash is preserved for the value parser.
fn split_disposition_parameters(parameters: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escaped = false;
    for c in parameters.chars() {
        if escaped {
            current.push('\\');
            current.push(c);
            escaped = false;
            continue;
        }
        match c {
            '\\' if in_quotes => escaped = true,
            '"' if in_quotes => {
                in_quotes = false;
                current.push(c);
            }
            '"' => {
                in_quotes = true;
                current.push(c);
            }
            ';' if !in_quotes => parts.push(std::mem::take(&mut current)),
            _ => current.push(c),
        }
    }
    parts.push(current);
    parts
}

/// Ordinary `filename` value: a quoted string with quoted-pair unescaping
/// (`\;` → `;`, `\"` → `"`), or a bare token. Trailing garbage after the
/// closing quote makes the occurrence malformed.
fn parse_plain_value(value: &str) -> Option<String> {
    let mut chars = value.chars();
    if chars.next() == Some('"') {
        let rest: String = chars.collect();
        let mut out = String::with_capacity(rest.len());
        let mut iter = rest.chars();
        loop {
            match iter.next() {
                // RFC 9110 quoted-pair: only `\"` and `\\` unescape; any
                // other escape stays literal so backslash path text keeps
                // reaching the sanitizer's separator handling.
                Some('\\') => match iter.next() {
                    Some(escaped @ ('"' | '\\')) => out.push(escaped),
                    Some(other) => {
                        out.push('\\');
                        out.push(other);
                    }
                    None => return None, // trailing escape
                },
                Some('"') => {
                    // Only trailing whitespace may follow the closing quote.
                    if !iter.all(|c| c.is_whitespace()) {
                        return None;
                    }
                    break;
                }
                Some(c) => out.push(c),
                None => return None, // unterminated quoted string
            }
        }
        Some(out)
    } else {
        // Bare token: taken literally; the caller sanitizes it.
        Some(value.to_string())
    }
}

/// `filename*` ext-value (RFC 5987/8187): `charset'language'value`. Only
/// UTF-8 charsets are supported; percent decoding is strict (malformed
/// escapes reject the occurrence) and the decoded bytes must be valid UTF-8.
fn parse_extended_value(value: &str) -> Option<String> {
    let mut parts = value.splitn(3, '\'');
    let charset = parts.next()?;
    let _language = parts.next()?;
    let encoded = parts.next()?;
    if !(charset.eq_ignore_ascii_case("utf-8") || charset.eq_ignore_ascii_case("utf8")) {
        return None;
    }
    let bytes = percent_decode_strict(encoded)?;
    String::from_utf8(bytes).ok()
}

/// Strict percent decoding: any malformed escape (`%` without two hex
/// digits) rejects the whole input; valid escapes decode byte-exactly.
fn percent_decode_strict(value: &str) -> Option<Vec<u8>> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let high = bytes.get(i + 1).and_then(|b| (*b as char).to_digit(16))?;
            let low = bytes.get(i + 2).and_then(|b| (*b as char).to_digit(16))?;
            out.push(((high << 4) | low) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    Some(out)
}

/// Interpret a probe response into metadata (shared by HEAD and ranged
/// GET paths).
#[allow(dead_code)] // wired by job controller in
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
    let (filename_hint, plain_filename_hint) =
        disposition_filename_candidates(header("content-disposition"));
    Ok(ProbeMetadata {
        final_url: final_url.to_string(),
        status,
        total_size: content_length,
        media_type: header("content-type").map(str::to_string),
        filename_hint,
        plain_filename_hint,
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

    #[test]
    fn extended_value_wins_and_plain_fallback_is_kept() {
        let (hint, plain) = disposition_filename_candidates(Some(
            "attachment; filename*=UTF-8''r%C3%A9sum%C3%A9.pdf; filename=\"back.pdf\"",
        ));
        assert_eq!(hint.as_deref(), Some("résumé.pdf"));
        assert_eq!(plain.as_deref(), Some("back.pdf"));
    }

    #[test]
    fn malformed_extended_falls_back_to_plain() {
        // Unsupported charset and invalid decoded UTF-8 reject the extended
        // occurrence; the ordinary filename still supplies the hint.
        for bad in [
            "attachment; filename*=ISO-8859-1''a.zip; filename=\"b.zip\"",
            "attachment; filename*=UTF-8''%FF%FE; filename=\"b.zip\"",
            "attachment; filename*=UTF-8''%2;z; filename=\"b.zip\"",
            "attachment; filename*=UTF-8'lang; filename=\"b.zip\"",
        ] {
            let (hint, plain) = disposition_filename_candidates(Some(bad));
            assert_eq!(hint.as_deref(), Some("b.zip"), "{bad}");
            assert_eq!(plain.as_deref(), Some("b.zip"), "{bad}");
        }
    }

    #[test]
    fn quoted_semicolons_and_quoted_pairs_stay_one_parameter() {
        let (hint, _) =
            disposition_filename_candidates(Some("attachment; filename=\"a;b\\\\c\\\"d.pdf\""));
        assert_eq!(hint.as_deref(), Some("a;b\\c\"d.pdf"));
    }

    #[test]
    fn duplicate_parameters_use_first_well_formed_occurrence() {
        let (hint, plain) = disposition_filename_candidates(Some(
            "attachment; filename*=UTF-8''%2; filename=\"first.zip\"; filename=\"second.zip\"",
        ));
        assert_eq!(hint.as_deref(), Some("first.zip"));
        assert_eq!(plain.as_deref(), Some("first.zip"));
        // A malformed first plain occurrence (trailing garbage after the
        // closing quote) yields to a later well-formed one.
        let (hint, _) = disposition_filename_candidates(Some(
            "attachment; filename=\"ab\"garbage; filename=\"later.zip\"",
        ));
        assert_eq!(hint.as_deref(), Some("later.zip"));
    }

    #[test]
    fn first_disposition_header_and_garbage_type() {
        // interpret reads only the FIRST Content-Disposition header.
        let headers = vec![
            (
                "content-disposition".into(),
                "attachment; filename=\"a.zip\"".into(),
            ),
            (
                "Content-Disposition".into(),
                "attachment; filename=\"b.zip\"".into(),
            ),
        ];
        let md = interpret(200, "http://x/f", &headers, None, "HTTP/1.1").expect("ok");
        assert_eq!(md.filename_hint.as_deref(), Some("a.zip"));
        assert_eq!(md.plain_filename_hint.as_deref(), Some("a.zip"));
        // A disposition type without parameters supplies no hint.
        assert_eq!(
            disposition_filename_candidates(Some("form-data")),
            (None, None)
        );
    }

    #[test]
    fn hostile_disposition_inputs_do_not_panic() {
        for evil in [
            "attachment; filename=\"unterminated",
            "attachment; filename*=UTF-8''",
            "attachment; filename*=\u{0}; filename=\"x\"",
            "attachment; filename*=UTF-8''%ZZ%2",
        ] {
            let _ = disposition_filename_candidates(Some(evil)); // must not panic
        }
        let long_quoted = format!("attachment; filename=\"{}", "x".repeat(100_000));
        let long_encoded = format!("attachment; filename*=UTF-8''{}", "%41".repeat(50_000));
        for evil in [&long_quoted, &long_encoded] {
            let _ = disposition_filename_candidates(Some(evil)); // must not panic
        }
    }
}
