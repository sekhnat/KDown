//! Local path safety (§21.3): filename sanitization for server-supplied
//! names.
//!
//! The engine receives a resolved destination path from the caller; it
//! never derives filesystem paths from unsanitized server filenames. This
//! utility is for optional filename extraction (e.g., `Content-Disposition`
//! hints in an embedding layer) and sanitizes:
//! - path separators (`/`, `\`) and Windows drive/UNC prefixes;
//! - `..` traversal segments;
//! - NUL and control characters;
//! - reserved platform names (CON, PRN, AUX, NUL, COM1-9, LPT1-9);
//! - excessive length (§21.3 "excessive filename length").
//!
//! Parsing hostile metadata must fail safely without panics (§21.4).

/// Maximum filename length kept by [`sanitize_filename`] (bytes). Windows'
/// per-component limit is 255; other platforms allow more — the strictest
/// common bound wins.
pub const MAX_FILENAME_LEN: usize = 255;

/// Reserved Windows device names, case-insensitive (§21.3).
const RESERVED_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5",
    "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5",
    "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Sanitize a server-supplied filename into a safe single-component name.
///
/// Returns a candidate name containing no separators, traversal, control
/// characters, or reserved names. Falls back to `"download"` when nothing
/// survives sanitization. Never panics on arbitrary input (§21.4).
#[must_use]
pub fn sanitize_filename(input: &str) -> String {
    // 1. Take only the final path component (strips any prefix traversal
    //    or separators, `/` and `\` alike).
    let mut candidate = input
        .rsplit(['/', '\\'])
        .find(|c| !c.is_empty())
        .unwrap_or("")
        .to_string();

    // 2. Strip a Windows drive prefix if the component still carries one
    //    (e.g., "C:evil.txt" after backslash stripping).
    if let Some(idx) = candidate.find(':') {
        candidate = candidate[idx + 1..].to_string();
    }

    // 3. Remove control characters (including NUL) and trim whitespace
    //    and trailing dots (Windows treats them specially).
    candidate = candidate
        .chars()
        .filter(|c| !c.is_control())
        .collect::<String>()
        .trim()
        .trim_end_matches('.')
        .to_string();

    // 4. Neutralize traversal leftovers after component split (".." as a
    //    whole name or a name made only of dots).
    if candidate.is_empty() || candidate.chars().all(|c| c == '.') {
        return "download".to_string();
    }

    // 5. Reserved device names (with or without extension, §21.3).
    let stem = candidate.split('.').next().unwrap_or("");
    if RESERVED_NAMES.iter().any(|r| r.eq_ignore_ascii_case(stem)) {
        candidate = format!("_{candidate}");
    }

    // 6. Bound length: prefer keeping the extension when truncating.
    if candidate.len() > MAX_FILENAME_LEN {
        let ext = std::path::Path::new(&candidate)
            .extension()
            .map(|e| format!(".{}", e.to_string_lossy()))
            .unwrap_or_default();
        let keep = MAX_FILENAME_LEN.saturating_sub(ext.len());
        // Truncate to a char boundary first, then to the byte bound.
        let mut stem_end = candidate.len().min(keep);
        while !candidate.is_char_boundary(stem_end) {
            stem_end -= 1;
        }
        let stem = &candidate[..stem_end];
        candidate = format!("{stem}{ext}");
        // Extension could itself push over the bound in pathological
        // inputs; hard clamp to a char boundary defensively.
        if candidate.len() > MAX_FILENAME_LEN {
            let mut end = MAX_FILENAME_LEN;
            while !candidate.is_char_boundary(end) {
                end -= 1;
            }
            candidate.truncate(end);
        }
    }

    if candidate.is_empty() {
        "download".to_string()
    } else {
        candidate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_traversal_and_separators() {
        assert_eq!(sanitize_filename("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_filename("..\\..\\windows\\system32"), "system32");
        assert_eq!(sanitize_filename("/etc/passwd"), "passwd");
        assert_eq!(sanitize_filename("a/b/c.txt"), "c.txt");
    }

    #[test]
    fn removes_control_chars_and_nul() {
        let evil = "bad\0name\u{7f}.bin";
        assert_eq!(sanitize_filename(evil), "badname.bin");
        assert!(!sanitize_filename("x\u{1}y").contains('\u{1}'));
    }

    #[test]
    fn neutralizes_reserved_names() {
        assert_eq!(sanitize_filename("CON"), "_CON");
        assert_eq!(sanitize_filename("com1.txt"), "_com1.txt");
        assert_eq!(sanitize_filename("nul"), "_nul");
        // Non-reserved names untouched.
        assert_eq!(sanitize_filename("console.txt"), "console.txt");
    }

    #[test]
    fn bounds_length_safely() {
        let long = "a".repeat(600);
        let out = sanitize_filename(&long);
        assert!(out.len() <= MAX_FILENAME_LEN);
        assert!(out.starts_with('a'));
        // UTF-8 safety under truncation.
        let mut utf8 = "é".repeat(200);
        utf8.push_str(".bin");
        let out2 = sanitize_filename(&utf8);
        assert!(out2.len() <= MAX_FILENAME_LEN);
    }

    #[test]
    fn falls_back_on_garbage_without_panicking() {
        assert_eq!(sanitize_filename(""), "download");
        assert_eq!(sanitize_filename(".."), "download");
        assert_eq!(sanitize_filename("."), "download");
        assert_eq!(sanitize_filename("///"), "download");
        assert_eq!(sanitize_filename(""), "download");
        // Hostile fuzz-ish inputs must not panic (§21.4).
        for evil in [
            "\0", "../../", "C:\\..\\..\\", "\u{0}\u{0}", "....", "a//..//b",
            "\\\\", "COM0", "con.", "..a..", "é\u{0}.txt",
        ] {
            let _ = sanitize_filename(evil); // must not panic
        }
    }
}