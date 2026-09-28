//! Local path safety (§21.3): filename sanitization for server-supplied
//! names.
//!
//! The engine receives a resolved destination path from the caller; it never
//! derives filesystem paths from unsanitized server filenames. Directory-
//! target downloads (`automatic-filename-resolution`) additionally resolve a
//! basename from remote metadata, so the sanitizer is fallible there: a
//! candidate that cannot be represented safely is rejected so the next
//! candidate source can be tried, instead of silently winning as `download`.
//!
//! Sanitization is portable: the union of Windows/Unix-invalid characters is
//! processed on every host, so a name accepted here is a single normal path
//! component everywhere. Parsing hostile metadata must fail safely without
//! panics (§21.4).

/// Maximum filename length kept by [`sanitize_filename`] (bytes). Windows'
/// per-component limit is 255; other platforms allow more — the strictest
/// common bound wins.
pub const MAX_FILENAME_LEN: usize = 255;

/// Reserved Windows device basenames, case-insensitive (§21.3). The
/// superscript-digit spellings (`COM¹`–`COM³`, `LPT¹`–`LPT³`) are reserved
/// with or without extensions just like their ASCII digits.
const RESERVED_BASENAMES: &[&str] = &[
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
    "COM\u{00B9}",
    "COM\u{00B2}",
    "COM\u{00B3}",
    "LPT\u{00B9}",
    "LPT\u{00B2}",
    "LPT\u{00B3}",
];

/// Sanitize a server-supplied filename into a safe single-component name.
///
/// Returns a candidate name containing no separators, traversal, control
/// characters, or reserved names. Falls back to `"download"` when nothing
/// survives sanitization. Never panics on arbitrary input (§21.4).
#[must_use]
pub fn sanitize_filename(input: &str) -> String {
    try_sanitize_filename(input, MAX_FILENAME_LEN).unwrap_or_else(|| "download".to_string())
}

/// Fallible sanitizer for automatic filename resolution: `None` when the
/// input cannot become a safe single component within `byte_limit`, so the
/// caller can fall through to the next candidate source.
///
/// Rules (portable — applied on every host):
/// - separators (`/`, `\\`) in untrusted text select the last nonempty
///   component instead of being copied through;
/// - control characters (including NUL) are removed;
/// - Windows-illegal punctuation `< > : " | ? *` is replaced per character
///   (a drive-like prefix is NOT stripped);
/// - Windows-invalid trailing spaces/periods are trimmed;
/// - empty, all-dot and reserved device basenames (including superscript
///   `COM¹`–`COM³`/`LPT¹`–`LPT³`) are rejected or prefixed;
/// - over-limit names are truncated on UTF-8 boundaries, preserving an
///   extension when possible, then re-validated.
///
/// # Errors
/// Returns `None` instead of panicking for any unusable input (§21.4).
#[must_use]
pub fn try_sanitize_filename(input: &str, byte_limit: usize) -> Option<String> {
    if byte_limit == 0 {
        return None;
    }

    // 1. Untrusted path text: separators select the final nonempty component
    //    (`/` and `\\` alike), so prefixes and traversal cannot escape.
    let mut candidate = input
        .rsplit(['/', '\\'])
        .find(|c| !c.is_empty())?
        .to_string();

    // 2. Remove control characters (including NUL).
    candidate = candidate.chars().filter(|c| !c.is_control()).collect();

    // 3. Replace Windows-illegal punctuation per character. `:` is replaced
    //    like any other illegal character; text before it is preserved.
    candidate = candidate
        .chars()
        .map(|c| {
            if matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*') {
                '_'
            } else {
                c
            }
        })
        .collect();

    // 4. Trim Windows-invalid trailing spaces/periods.
    let trimmed_len = candidate
        .char_indices()
        .rev()
        .find(|(_, c)| !matches!(c, ' ' | '.'))
        .map_or(0, |(i, c)| i + c.len_utf8());
    candidate.truncate(trimmed_len);

    // 5. Reject empty and all-dot remnants before any further work.
    if candidate.is_empty() || candidate.chars().all(|c| c == '.') {
        return None;
    }

    // 6. Reserved Windows device basenames are prefixed, case-insensitively.
    if is_reserved_device_basename(&candidate) {
        candidate.insert(0, '_');
    }

    // 7. Bound length on a UTF-8 boundary, preserving an extension when it
    //    fits alongside at least part of the stem.
    if candidate.len() > byte_limit {
        candidate = truncate_preserving_extension(&candidate, byte_limit)?;
    }

    // 8. Re-check the invariants after truncation: cutting the stem can
    //    expose a reserved device basename. Prefix it; a candidate that then
    //    no longer fits the limit is rejected so the next source is tried.
    if candidate.is_empty() || candidate.chars().all(|c| c == '.') {
        return None;
    }
    if is_reserved_device_basename(&candidate) {
        candidate.insert(0, '_');
        if candidate.len() > byte_limit {
            return None;
        }
    }

    // 9. The result must be exactly one normal path component.
    if !is_single_normal_component(&candidate) {
        return None;
    }
    Some(candidate)
}

/// Whether the basename's extension-less stem is a reserved Windows device
/// name (`CON`, `COM¹.txt`, …), matched case-insensitively.
fn is_reserved_device_basename(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or("");
    RESERVED_BASENAMES
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(stem))
}

/// Truncate to `byte_limit` bytes on a UTF-8 boundary. When a final extension
/// fits within the limit, the stem is cut first so the extension survives.
fn truncate_preserving_extension(candidate: &str, byte_limit: usize) -> Option<String> {
    if candidate.len() <= byte_limit {
        return Some(candidate.to_owned());
    }
    let ext_len = std::path::Path::new(candidate)
        .extension()
        .map_or(0, |e| e.len() + 1); // the separating dot
    let (stem_part, suffix) = if ext_len > 0 && ext_len < byte_limit {
        let stem_end = candidate.len() - ext_len;
        (&candidate[..stem_end], &candidate[stem_end..])
    } else {
        (candidate, "")
    };
    let mut end = byte_limit.saturating_sub(suffix.len()).min(stem_part.len());
    while end > 0 && !stem_part.is_char_boundary(end) {
        end -= 1;
    }
    if end == 0 && suffix.is_empty() {
        return None;
    }
    Some(format!("{}{}", &stem_part[..end], suffix))
}

/// Exactly one `Normal` component: no prefixes, root, `.`/`..`, or separators.
fn is_single_normal_component(name: &str) -> bool {
    let mut components = std::path::Path::new(name).components();
    matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none()
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
    fn replaces_windows_punctuation_per_character() {
        assert_eq!(sanitize_filename("a<b>c:d\"e|f?g*h"), "a_b_c_d_e_f_g_h");
        // A drive-like prefix is replaced, not stripped: nothing before ':'
        // is silently discarded.
        assert_eq!(sanitize_filename("C:evil.txt"), "C_evil.txt");
        assert_eq!(sanitize_filename("quote\"name"), "quote_name");
    }

    #[test]
    fn trims_trailing_windows_invalid_characters() {
        assert_eq!(sanitize_filename("report.pdf. . "), "report.pdf");
        assert_eq!(sanitize_filename("name "), "name");
        // Leading spaces are portable; they stay.
        assert_eq!(sanitize_filename(" leading.txt"), " leading.txt");
    }

    #[test]
    fn neutralizes_reserved_names() {
        assert_eq!(sanitize_filename("CON"), "_CON");
        assert_eq!(sanitize_filename("com1.txt"), "_com1.txt");
        assert_eq!(sanitize_filename("nul"), "_nul");
        // Superscript device digits are reserved like their ASCII forms.
        assert_eq!(sanitize_filename("COM\u{00B9}"), "_COM\u{00B9}");
        assert_eq!(sanitize_filename("lpt\u{00B3}.bin"), "_lpt\u{00B3}.bin");
        // Non-reserved names untouched.
        assert_eq!(sanitize_filename("console.txt"), "console.txt");
        assert_eq!(sanitize_filename("COM0"), "COM0");
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
        assert!(out2.ends_with(".bin"), "extension preserved: {out2}");
        assert!(out2.is_char_boundary(out2.len()));
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
            "\0",
            "../../",
            "C:\\..\\..\\",
            "\u{0}\u{0}",
            "....",
            "a//..//b",
            "\\\\",
            "COM0",
            "con.",
            "..a..",
            "é\u{0}.txt",
        ] {
            let _ = sanitize_filename(evil); // must not panic
        }
    }

    #[test]
    fn fallible_sanitizer_rejects_instead_of_falling_back() {
        // Rejected candidates let directory resolution try the next source.
        assert_eq!(try_sanitize_filename("", 250), None);
        assert_eq!(try_sanitize_filename("..", 250), None);
        assert_eq!(try_sanitize_filename("....", 250), None);
        assert_eq!(try_sanitize_filename("///", 250), None);
        assert_eq!(try_sanitize_filename("a/b", 250), Some("b".to_string()));
        // Byte limit is honored exactly.
        assert_eq!(try_sanitize_filename("abcdef", 4), Some("abcd".to_string()));
        assert_eq!(try_sanitize_filename("abcdef", 0), None);
        // Truncation preserves a fitting extension and stays valid UTF-8.
        let multibyte = "é".repeat(200);
        let out = try_sanitize_filename(&format!("{multibyte}.bin"), 30).expect("fits");
        assert!(out.len() <= 30);
        assert!(out.ends_with(".bin"));
        assert!(out.is_char_boundary(out.len()));
    }

    #[test]
    fn truncation_recheck_rejects_exposed_reserved_names() {
        // Truncation takes the leading bytes, so this fits exactly.
        let out = try_sanitize_filename("AUXILIARY.bin", 8).expect("representable");
        assert_eq!(out, "AUXI.bin");
        // Truncation can expose a reserved device stem ("AUX"); prefixing
        // would exceed the limit, so the candidate is rejected and the
        // caller falls through to the next candidate source.
        assert_eq!(try_sanitize_filename("AUXILIARY", 3), None);
        assert_eq!(
            try_sanitize_filename("AUXILIARY", 4),
            Some("AUXI".to_string())
        );
    }
    #[test]
    fn sanitizer_output_is_one_normal_component() {
        for input in [
            "../../etc/passwd",
            "a\\b\\c",
            "C:evil.txt",
            "COM1",
            "..a..",
            "é\u{0}.txt",
            "nul.txt",
        ] {
            if let Some(out) = try_sanitize_filename(input, 250) {
                let mut components = std::path::Path::new(&out).components();
                assert!(matches!(
                    components.next(),
                    Some(std::path::Component::Normal(_))
                ));
                assert!(components.next().is_none(), "{input} -> {out}");
                assert!(!out.contains('/'));
                assert!(!out.contains('\\'));
                assert!(!out.contains('\0'));
            }
        }
    }
}
