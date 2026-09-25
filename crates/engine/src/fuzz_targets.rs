//! Fuzz entry points (§36.5) shared by `cargo fuzz` targets and
//! the corpus smoke test.
//!
//! Malformed metadata must fail safely: no panics, no memory corruption,
//! no path traversal (§36.5). Each `fuzz_*` function takes raw bytes and
//! must handle arbitrary input.

use crate::http::filename_from_disposition;
use crate::http::validators::parse_content_range;
use crate::io::sanitize_filename;
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

/// Fuzz target: Content-Disposition parsing + filename sanitization (§36.5).
pub fn fuzz_content_disposition(data: &[u8]) {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    if let Some(name) = filename_from_disposition(Some(s)) {
        let safe = sanitize_filename(&name);
        assert!(!safe.contains('\0'));
        assert!(!safe.contains('/'));
        assert!(!safe.contains('\\'));
        assert!(!safe.contains(".."));
        assert!(safe.len() <= crate::io::sanitize::MAX_FILENAME_LEN);
    }
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
