//! Fuzz entry points (§36.5) shared by `cargo fuzz` targets and
//! the corpus smoke test.
//!
//! Malformed metadata must fail safely: no panics, no memory corruption,
//! no path traversal (§36.5). Each `fuzz_*` function takes raw bytes and
//! must handle arbitrary input.

use crate::http::filename_from_disposition;
use crate::http::probe::{disposition_filename_candidates, ProbeMetadata};
use crate::http::validators::parse_content_range;
use crate::io::sanitize_filename;
use crate::job::naming::{resolve_directory_basename, url_path_basename, DirectoryOptions};
use crate::redact::Redactor;
use crate::resume::checkpoint::Checkpoint;

/// Fuzz target: URL handling (§36.5).
pub fn fuzz_url(data: &[u8]) {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    let _ = s.parse::<hyper::Uri>();
    let red = Redactor::new().with_sensitive_query_params(&["token", "sig"]);
    let _ = red.redact_url(s);
}

/// Fuzz target: Content-Range parsing (§36.5).
pub fn fuzz_content_range(data: &[u8]) {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    let _ = parse_content_range(s);
}

/// Fuzz target: ETag capture/comparison (§36.5).
pub fn fuzz_etag(data: &[u8]) {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    let _ = crate::http::validators::ResourceValidators::from_headers(Some(s), Some(s), Some(1024));
}

/// Fuzz target: Content-Disposition parsing + filename sanitization +
/// automatic name resolution (§36.5, automatic-filename-resolution). The
/// ordered disposition parsing, the portable sanitizer and the URL-candidate
/// path containment must all hold for hostile input: no panics, one normal
/// path component, byte limits honored.
pub fn fuzz_content_disposition(data: &[u8]) {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    // Structured parse: both hint slots stay sanitized-safe when resolved.
    let (hint, plain) = disposition_filename_candidates(Some(s));
    for raw in [hint.as_deref(), plain.as_deref()].into_iter().flatten() {
        let safe = sanitize_filename(raw);
        assert!(!safe.contains('\0'));
        assert!(!safe.contains('/'));
        assert!(!safe.contains('\\'));
        // `..` can occur within an ordinary basename (e.g. `..*name`);
        // the safety invariant is one normal path component, not absence
        // of those characters anywhere in the basename.
        let mut components = std::path::Path::new(&safe).components();
        assert!(matches!(
            components.next(),
            Some(std::path::Component::Normal(_))
        ));
        assert!(components.next().is_none());
        assert!(safe.len() <= crate::io::sanitize::MAX_FILENAME_LEN);
        // The fallible sanitizer either rejects or produces the same
        // containment invariants under any byte limit.
        if let Some(fallible) = crate::io::sanitize::try_sanitize_filename(raw, safe.len()) {
            let mut components = std::path::Path::new(&fallible).components();
            assert!(matches!(
                components.next(),
                Some(std::path::Component::Normal(_))
            ));
            assert!(components.next().is_none());
        }
    }
    // Legacy single-hint extraction keeps working on the ordered parser.
    if filename_from_disposition(Some(s)).is_some() {
        assert!(hint.is_some() || plain.is_some());
    }
    // URL-candidate containment: the basename of a hostile URL must be
    // separator-free and single-component after sanitization, and a
    // resolved candidate (hints -> URLs -> fallback) is always safe under
    // the directory cap.
    let url = format!("http://host/{s}");
    if let Some(basename) = url_path_basename(&url) {
        assert!(!basename.contains("%2F"));
        let sanitized = sanitize_filename(&basename);
        assert!(!sanitized.is_empty());
    }
    let meta = ProbeMetadata {
        filename_hint: hint,
        plain_filename_hint: plain,
        final_url: url.clone(),
        ..ProbeMetadata::default()
    };
    let resolved = resolve_directory_basename(
        &meta,
        &url,
        &DirectoryOptions {
            fallback: "download".to_string(),
            max_filename_bytes: crate::job::naming::MAX_FINAL_BASENAME_BYTES,
        },
    );
    let mut components = std::path::Path::new(&resolved).components();
    assert!(matches!(
        components.next(),
        Some(std::path::Component::Normal(_))
    ));
    assert!(components.next().is_none());
    assert!(resolved.len() <= crate::job::naming::MAX_FINAL_BASENAME_BYTES);
}

/// Fuzz target: checkpoint files (§36.5).
pub fn fuzz_checkpoint(data: &[u8]) {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    let _ = Checkpoint::from_json(s);
    if let Ok(mut cp) = serde_json::from_str::<Checkpoint>(s) {
        cp.completed_ranges = vec![(u64::MAX, 0), (0, u64::MAX)];
        let _ = cp.validate();
    }
}

#[cfg(test)]
mod smoke {
    use super::*;

    /// Stable fallback when `cargo-fuzz`/nightly is unavailable: exercises
    /// the same entry points over an adversarial corpus without panics.
    #[test]
    fn corpus_smoke_no_panics() {
        let utf8 = [0xC3u8, 0xA9].repeat(1000);
        let large = vec![0x41u8; 8192];
        let corpus: Vec<&[u8]> = vec![
            b"",
            b"\x00",
            b"garbage",
            b"bytes 0-999999999999999999999999/999999999999999999999999",
            b"bytes -1-/99999999999999999999",
            b"bytes */*",
            b"bytes 18446744073709551615-18446744073709551615/18446744073709551615",
            b"*/18446744073709551615",
            b"\xff\xfe\xfd",
            b"https://[invalid: :::1]/x?token=\x00&sig=abc",
            b"http://user:pass@host/",
            b"\x00\x01\x02...",
            utf8.as_slice(),
            b"../../etc/passwd\x00\\..",
            b"attachment; filename=\"\\\"\\\"\\\"\"",
            b"attachment; filename*=UTF-8''%FF%FE",
            b"attachment; filename=\"../../../../../etc/shadow\"",
            b"CON\x00",
            b"\"{:wtf}\"",
            b"{ \"format_version\": 999999, \"job_id\": \"x\" }",
            b"{ \"format_version\": 1, \"job_id\": \"\", \"completed_ranges\": [[18446744073709551615, 0]] }",
            b"[[[[[[",
            large.as_slice(),
        ];
        for c in corpus {
            fuzz_url(c);
            fuzz_content_range(c);
            fuzz_etag(c);
            fuzz_content_disposition(c);
            fuzz_checkpoint(c);
        }
    }
}
