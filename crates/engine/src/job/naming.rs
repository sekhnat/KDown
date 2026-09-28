//! Automatic filename resolution (automatic-filename-resolution).
//!
//! Directory targets resolve an immutable, safe basename from the final HEAD
//! metadata, the redirected/original URLs, or a validated fallback, then
//! lease the resulting destination and rejoin the ordinary
//! admission/transfer/commit path. `OverwritePolicy::Rename` shares the same
//! candidate machinery for explicit-file and directory targets: resume-first
//! sibling discovery, then bounded fresh selection under destination leases.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::config::{DurabilityMode, OverwritePolicy, ResumePolicy};
use crate::error::DownloadError;
use crate::http::probe::ProbeMetadata;
use crate::io::destination_lease::DestinationLease;
use crate::io::sanitize::try_sanitize_filename;
use crate::resume::checkpoint::Checkpoint;
use crate::resume::checkpoint_store::{
    CheckpointResolveContext, CheckpointStore, CheckpointStoreResolver,
};
use crate::resume::flow::{discover_admission, AdmissionDiscovery};

/// Default fallback basename for directory targets.
pub(crate) const DEFAULT_FALLBACK_FILENAME: &str = "download";

/// Hard final-basename cap for directory targets: the strictest common
/// 255-byte component limit minus the five-byte `.part` sibling suffix, so
/// the temp output of any accepted name stays a valid component.
pub(crate) const MAX_FINAL_BASENAME_BYTES: usize = 250;

/// Minimum configured byte cap for `Rename`: one stem byte plus the longest
/// generated suffix ` (999)` must always be representable.
pub(crate) const RENAME_MIN_CAP_BYTES: usize = 7;

/// Highest generated Rename sibling suffix (`stem (999).ext`); the base plus
/// these 999 siblings make 1,000 candidates.
const RENAME_MAX_SUFFIX: u32 = 999;

/// Private directory-target options carried beside the ordinary request.
#[derive(Debug, Clone)]
pub(crate) struct DirectoryOptions {
    pub(crate) fallback: String,
    pub(crate) max_filename_bytes: usize,
}

/// Internal target mode for one job: an explicit file path chosen by the
/// caller, or an opt-in directory target whose filename is resolved after
/// the probe. An explicit path that happens to name a directory stays a
/// file target — directory mode is only reached through the directory API.
#[derive(Debug, Clone)]
pub(crate) enum JobTarget {
    File,
    Directory {
        directory: PathBuf,
        options: DirectoryOptions,
    },
}

/// Validate a directory target and its naming options before any network
/// activity. Determinable invalid input fails in the Configuration category
/// and creates no output artifacts.
///
/// # Errors
/// [`DownloadError::Configuration`] for a missing/non-directory target, a
/// fallback that is not already an unchanged-by-sanitization single normal
/// component, or a byte cap outside `[fallback_len, 250]` (and below 7 for
/// `Rename`).
pub(crate) fn validate_directory_target(
    directory: &Path,
    options: &DirectoryOptions,
    overwrite: OverwritePolicy,
) -> Result<(), DownloadError> {
    // The directory is caller-selected, not server-selected: it must already
    // exist and be a directory. Symlinked directories are accepted; the
    // trusted-directory assumption covers their entries.
    let metadata = std::fs::metadata(directory).map_err(|error| {
        DownloadError::Configuration(format!(
            "directory target {} is not an existing directory: {error}",
            directory.display()
        ))
    })?;
    if !metadata.is_dir() {
        return Err(DownloadError::Configuration(format!(
            "directory target {} is not a directory",
            directory.display()
        )));
    }
    if options.fallback.is_empty()
        || try_sanitize_filename(&options.fallback, options.max_filename_bytes).as_deref()
            != Some(options.fallback.as_str())
    {
        return Err(DownloadError::Configuration(format!(
            "fallback filename {:?} is not an unchanged single normal component within the byte cap",
            options.fallback
        )));
    }
    if options.max_filename_bytes > MAX_FINAL_BASENAME_BYTES
        || options.max_filename_bytes < options.fallback.len()
        || (overwrite == OverwritePolicy::Rename
            && options.max_filename_bytes < RENAME_MIN_CAP_BYTES)
    {
        return Err(DownloadError::Configuration(format!(
            "max_filename_bytes {} must be within [{}, {}] for this request",
            options.max_filename_bytes,
            options.max_filename_bytes.max(options.fallback.len()).max(
                if overwrite == OverwritePolicy::Rename {
                    RENAME_MIN_CAP_BYTES
                } else {
                    options.fallback.len()
                }
            ),
            MAX_FINAL_BASENAME_BYTES
        )));
    }
    Ok(())
}

/// The final path segment of a URL, parsed through the existing URI
/// representation so query and fragment never contribute to a basename.
/// A trailing `/` yields no basename (the previous segment is not taken).
/// The segment is percent-decoded exactly once: malformed escapes skip the
/// candidate, valid escapes producing invalid UTF-8 decode lossily, and a
/// decoded `/` or `\` rejects the whole candidate so it cannot change
/// directories.
#[must_use]
pub(crate) fn url_path_basename(url: &str) -> Option<String> {
    let uri: hyper::Uri = url.parse().ok()?;
    let path = uri.path();
    if path.ends_with('/') {
        return None;
    }
    let segment = path.rsplit_once('/').map_or(path, |(_, last)| last);
    if segment.is_empty() {
        return None;
    }
    let decoded = percent_decode_once(segment)?;
    if decoded.contains('/') || decoded.contains('\\') {
        return None;
    }
    Some(decoded)
}

/// Percent-decode one URL path segment. Malformed escapes reject the
/// candidate; well-formed escapes producing invalid UTF-8 decode lossily.
fn percent_decode_once(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let high = bytes.get(i + 1).and_then(|b| (*b as char).to_digit(16))?;
            let low = bytes.get(i + 2).and_then(|b| (*b as char).to_digit(16))?;
            decoded.push(((high << 4) | low) as u8);
            i += 3;
        } else {
            decoded.push(bytes[i]);
            i += 1;
        }
    }
    Some(String::from_utf8_lossy(&decoded).into_owned())
}

/// Resolve the directory-target basename: the first surviving candidate in
/// the fixed precedence (final HEAD `filename*`, final HEAD `filename`,
/// final HEAD URL segment, original URL segment, validated fallback). Every
/// untrusted candidate is sanitized under the byte cap; a malformed or
/// rejected candidate yields to the next source. The fallback was validated
/// unchanged at request time, so resolution always succeeds.
#[must_use]
pub(crate) fn resolve_directory_basename(
    meta: &ProbeMetadata,
    original_url: &str,
    options: &DirectoryOptions,
) -> String {
    let cap = options.max_filename_bytes;
    // Header hints first: the ordered hint is the valid `filename*` when one
    // parsed, else the ordinary `filename`; the plain hint is the ordinary
    // `filename` fallback for a valid `filename*` that sanitizes away.
    for raw in [
        meta.filename_hint.as_deref(),
        meta.plain_filename_hint.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(name) = try_sanitize_filename(raw, cap) {
            return name;
        }
    }
    for url in [&meta.final_url, original_url] {
        if let Some(raw) = url_path_basename(url) {
            if let Some(name) = try_sanitize_filename(&raw, cap) {
                return name;
            }
        }
    }
    options.fallback.clone()
}

/// The `.part` sibling path of a destination (the engine's default temp
/// convention).
pub(crate) fn part_sibling_path(destination: &Path) -> PathBuf {
    crate::io::sink::TempFileSpec::default().temp_path_for(destination)
}

/// One Rename candidate: base first, then `stem (1).ext` … `stem (999).ext`.
/// Generated siblings reserve the suffix bytes before extension-preserving
/// truncation so different suffixes never collide by truncating away their
/// distinguishing number; an unrepresentable candidate is `None` (the base
/// candidate is never truncated here: explicit-file bases stay verbatim and
/// directory bases were already sanitized under the cap).
#[must_use]
pub(crate) fn rename_candidate_names(base_file_name: &OsStr, cap: usize) -> Vec<Option<OsString>> {
    let mut candidates = Vec::with_capacity(RENAME_MAX_SUFFIX as usize + 1);
    candidates.push(Some(base_file_name.to_os_string()));
    let base = base_file_name.to_string_lossy().into_owned();
    let stem = std::path::Path::new(&base).file_stem().map_or_else(
        || base.clone(),
        |s: &OsStr| s.to_string_lossy().into_owned(),
    );
    let ext = std::path::Path::new(&base)
        .extension()
        .map_or(String::new(), |e| format!(".{}", e.to_string_lossy()));
    for n in 1..=RENAME_MAX_SUFFIX {
        let suffix = format!(" ({n})");
        let stem_budget = cap.saturating_sub(suffix.len()).saturating_sub(ext.len());
        let generated = if stem_budget == 0 {
            None
        } else {
            let mut end = stem_budget.min(stem.len());
            while end > 0 && !stem.is_char_boundary(end) {
                end -= 1;
            }
            if end == 0 {
                None
            } else {
                let raw = format!("{}{}{}", &stem[..end], suffix, ext);
                // Generated siblings obey the same portable invariants as
                // resolved names; the sanitizer is idempotent for already
                // clean names and rejects a truncated stem that exposes a
                // reserved device basename it cannot prefix within the cap.
                try_sanitize_filename(&raw, cap).map(OsString::from)
            }
        };
        candidates.push(generated);
    }
    candidates
}

/// The pieces of one selected Rename candidate, retained together so the
/// selected store, lease and loaded checkpoint are neither reloaded nor
/// readmitted.
pub(crate) struct SelectedDestination {
    pub(crate) destination: PathBuf,
    pub(crate) lease: DestinationLease,
    pub(crate) store: Arc<dyn CheckpointStore>,
    pub(crate) identity: String,
    pub(crate) checkpoint: Option<Checkpoint>,
    /// Corrupt-restart warnings discovered with the selected checkpoint
    /// (empty for a cleanly loaded one); they join the admission plan.
    pub(crate) admission_warnings: Vec<String>,
}

/// Outcome of Rename candidate selection.
pub(crate) enum RenameSelection {
    /// A candidate was leased and its checkpoint (when any) discovered.
    Selected(Box<SelectedDestination>),
    /// Every candidate was occupied, conflicted, or unrepresentable.
    /// `required_checkpoint_missing` preserves the existing `Checkpoint`
    /// failure for `ResumePolicy::Required` even when no candidate is free.
    Exhausted { required_checkpoint_missing: bool },
}

/// Inputs for Rename candidate selection.
pub(crate) struct RenameSelectionContext<'a> {
    /// Parent directory of every candidate (the caller directory for
    /// directory targets, the destination parent for explicit files).
    pub(crate) parent: &'a Path,
    /// Base candidate file name: verbatim for explicit files, the resolved
    /// sanitized basename for directory targets.
    pub(crate) base_name: &'a OsStr,
    /// Byte cap for generated siblings (the hard 250-byte `.part` headroom
    /// bound for explicit files; the validated configured cap for
    /// directories).
    pub(crate) cap: usize,
    pub(crate) resume: ResumePolicy,
    /// Original request URL: checkpoint identities bind URL + destination.
    pub(crate) url: &'a str,
    pub(crate) durability: DurabilityMode,
    pub(crate) resolver: &'a dyn CheckpointStoreResolver,
}

/// Select a Rename destination under destination leases: resume-first
/// discovery of checkpointed partial siblings in numeric order, then fresh
/// selection of the first candidate whose final entry and `.part` sibling
/// are absent. Only `DestinationConflict` lease errors are skipped; other
/// lease, resolver or checkpoint failures propagate. Selection never
/// creates a partial, checkpoint or final output: the lease is acquired
/// before anything is written and re-checks happen under the lock.
///
/// # Errors
/// [`DownloadError`] for propagated lease/resolver/checkpoint failures;
/// exhaustion is reported through [`RenameSelection::Exhausted`].
pub(crate) fn select_rename_candidate(
    ctx: &RenameSelectionContext<'_>,
) -> Result<RenameSelection, DownloadError> {
    let candidates = rename_candidate_names(ctx.base_name, ctx.cap);
    let required_checkpoint_missing = ctx.resume == ResumePolicy::Required;
    // Phase 1: resume-first scan. Candidates without partial output are
    // skipped without a lease (no resumable sibling is possible there).
    if ctx.resume != ResumePolicy::Never {
        for name in candidates.iter().flatten() {
            let destination = ctx.parent.join(name);
            let part = part_sibling_path(&destination);
            if std::fs::symlink_metadata(&part).is_err() {
                continue;
            }
            let lease = match DestinationLease::acquire(&destination) {
                Ok(lease) => lease,
                Err(DownloadError::DestinationConflict(_)) => continue,
                Err(error) => return Err(error),
            };
            // Under the lease: a published final means this partial belongs
            // to an already completed download — skip, never resume over it.
            if std::fs::symlink_metadata(&destination).is_ok() {
                drop(lease);
                continue;
            }
            let identity = crate::resume::flow::job_identity(ctx.url, &destination);
            let resolve_context = CheckpointResolveContext::new(
                identity.clone(),
                destination.clone(),
                map_durability(ctx.durability),
            );
            let resolved = ctx.resolver.resolve(&resolve_context).map_err(|e| {
                DownloadError::Checkpoint(format!("checkpoint adapter unavailable: {e}"))
            })?;
            let store: Arc<dyn CheckpointStore> = Arc::new(
                crate::resume::coordinated_store::CoordinatedCheckpointStore::new(resolved),
            );
            match discover_admission(ctx.resume, &identity, store.as_ref()) {
                Ok(AdmissionDiscovery::Found(checkpoint)) => {
                    return Ok(RenameSelection::Selected(Box::new(SelectedDestination {
                        destination,
                        lease,
                        store,
                        identity,
                        checkpoint: Some(*checkpoint),
                        admission_warnings: vec![],
                    })));
                }
                // A stale `.part` without a usable checkpoint is occupied:
                // it is never opened or truncated as a fresh transfer. Its
                // corrupt-restart warnings (when any) are dropped with the
                // skipped candidate.
                Ok(AdmissionDiscovery::Absent { warnings: _ }) => {
                    drop(lease);
                    continue;
                }
                Err(failure) => return Err(failure.error),
            }
        }
    }

    // Phase 2: fresh selection — first candidate whose final entry and
    // `.part` sibling are both absent, re-checked under the lease.
    for name in candidates.iter().flatten() {
        let destination = ctx.parent.join(name);
        let part = part_sibling_path(&destination);
        let occupied = |path: &Path| std::fs::symlink_metadata(path).is_ok();
        if occupied(&destination) || occupied(&part) {
            continue;
        }
        let lease = match DestinationLease::acquire(&destination) {
            Ok(lease) => lease,
            Err(DownloadError::DestinationConflict(_)) => continue,
            Err(error) => return Err(error),
        };
        if occupied(&destination) || occupied(&part) {
            drop(lease);
            continue;
        }
        let identity = crate::resume::flow::job_identity(ctx.url, &destination);
        let resolve_context = CheckpointResolveContext::new(
            identity.clone(),
            destination.clone(),
            map_durability(ctx.durability),
        );
        let resolved = ctx.resolver.resolve(&resolve_context).map_err(|e| {
            DownloadError::Checkpoint(format!("checkpoint adapter unavailable: {e}"))
        })?;
        let store: Arc<dyn CheckpointStore> =
            Arc::new(crate::resume::coordinated_store::CoordinatedCheckpointStore::new(resolved));
        // A checkpoint may exist for this identity without a `.part`
        // (externally removed temp); admission still discovers it so the
        // existing restart policy applies unchanged.
        let (checkpoint, admission_warnings) =
            match discover_admission(ctx.resume, &identity, store.as_ref()) {
                Ok(AdmissionDiscovery::Found(checkpoint)) => (Some(*checkpoint), vec![]),
                Ok(AdmissionDiscovery::Absent { warnings }) => (None, warnings),
                Err(failure) => return Err(failure.error),
            };
        return Ok(RenameSelection::Selected(Box::new(SelectedDestination {
            destination,
            lease,
            store,
            identity,
            checkpoint,
            admission_warnings,
        })));
    }

    Ok(RenameSelection::Exhausted {
        required_checkpoint_missing,
    })
}

fn map_durability(durability: DurabilityMode) -> crate::resume::checkpoint_store::DurabilityMode {
    match durability {
        DurabilityMode::Performance => crate::resume::checkpoint_store::DurabilityMode::Performance,
        DurabilityMode::Durable => crate::resume::checkpoint_store::DurabilityMode::Durable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(hint: Option<&str>, plain: Option<&str>, final_url: &str) -> ProbeMetadata {
        ProbeMetadata {
            filename_hint: hint.map(str::to_string),
            plain_filename_hint: plain.map(str::to_string),
            final_url: final_url.to_string(),
            ..ProbeMetadata::default()
        }
    }

    fn options(fallback: &str, cap: usize) -> DirectoryOptions {
        DirectoryOptions {
            fallback: fallback.to_string(),
            max_filename_bytes: cap,
        }
    }

    #[test]
    fn precedence_follows_fixed_order() {
        let opts = options("fallback.bin", 250);
        // Extended hint wins over everything.
        assert_eq!(
            resolve_directory_basename(
                &meta(Some("a.zip"), Some("b.zip"), "http://x/c.bin"),
                "http://o/d.bin",
                &opts
            ),
            "a.zip"
        );
        // Extended sanitizing away yields to the plain hint.
        assert_eq!(
            resolve_directory_basename(
                &meta(Some("/"), Some("b.zip"), "http://x/c.bin"),
                "http://o/d.bin",
                &opts
            ),
            "b.zip"
        );
        // No hints: final HEAD URL segment beats the original URL segment.
        assert_eq!(
            resolve_directory_basename(
                &meta(None, None, "http://x/path/c.bin"),
                "http://o/d.bin",
                &opts
            ),
            "c.bin"
        );
        // Original URL segment before the fallback.
        assert_eq!(
            resolve_directory_basename(&meta(None, None, "http://x/"), "http://o/d.bin", &opts),
            "d.bin"
        );
        // Validated fallback last.
        assert_eq!(
            resolve_directory_basename(&meta(None, None, "http://x/"), "http://o/", &opts),
            "fallback.bin"
        );
    }

    #[test]
    fn url_basenames_exclude_query_fragment_and_trailing_slash() {
        assert_eq!(
            url_path_basename("http://x/a/b.bin?q=1#f"),
            Some("b.bin".into())
        );
        assert_eq!(url_path_basename("http://x/a/b/"), None);
        assert_eq!(url_path_basename("http://x/"), None);
        assert_eq!(url_path_basename("http://x"), None);
        assert_eq!(url_path_basename("not a url at all"), None);
    }

    #[test]
    fn url_decoding_is_single_and_separator_safe() {
        assert_eq!(
            url_path_basename("http://x/a%20b.bin"),
            Some("a b.bin".into())
        );
        // A decoded separator rejects the whole candidate.
        assert_eq!(url_path_basename("http://x/a%2Fb.bin"), None);
        assert_eq!(url_path_basename("http://x/a%5Cb.bin"), None);
        // Malformed escapes skip the candidate.
        assert_eq!(url_path_basename("http://x/a%2.bin"), None);
        assert_eq!(url_path_basename("http://x/a%ZZ.bin"), None);
        // Invalid UTF-8 decodes lossily instead of rejecting.
        assert_eq!(
            url_path_basename("http://x/a%FF.bin"),
            Some("a\u{FFFD}.bin".into())
        );
        // A double-encoded separator stays one decode (literal "%2F" text).
        assert_eq!(
            url_path_basename("http://x/a%252Fb.bin"),
            Some("a%2Fb.bin".into())
        );
    }

    #[test]
    fn header_candidates_are_sanitized_or_skipped() {
        let opts = options("download", 250);
        // Traversal in header text collapses to the final component.
        assert_eq!(
            resolve_directory_basename(
                &meta(Some("../../etc/passwd"), None, "http://x/"),
                "http://o/",
                &opts
            ),
            "passwd"
        );
        // A reserved device basename is prefixed, not rejected.
        assert_eq!(
            resolve_directory_basename(
                &meta(Some("COM1.txt"), None, "http://x/"),
                "http://o/",
                &opts
            ),
            "_COM1.txt"
        );
        // Windows-illegal punctuation is replaced per character.
        assert_eq!(
            resolve_directory_basename(
                &meta(Some("a<b>.zip"), None, "http://x/"),
                "http://o/",
                &opts
            ),
            "a_b_.zip"
        );
    }

    #[test]
    fn fallback_survives_when_nothing_else_resolves() {
        let opts = options("download", 250);
        assert_eq!(
            resolve_directory_basename(&meta(Some(""), Some(""), "http://x/"), "http://o/", &opts),
            "download"
        );
    }

    #[test]
    fn candidates_cover_base_and_numbered_siblings() {
        let candidates = rename_candidate_names(OsStr::new("archive.bin"), 250);
        assert_eq!(candidates.len(), 1_000);
        assert_eq!(candidates[0].as_deref(), Some(OsStr::new("archive.bin")));
        assert_eq!(
            candidates[1].as_deref(),
            Some(OsStr::new("archive (1).bin"))
        );
        assert_eq!(
            candidates[999].as_deref(),
            Some(OsStr::new("archive (999).bin"))
        );
        // Extension-less bases get a bare numeric suffix.
        let plain = rename_candidate_names(OsStr::new("README"), 250);
        assert_eq!(plain[1].as_deref(), Some(OsStr::new("README (1)")));
        // Multi-dot extensions keep the final extension.
        let nested = rename_candidate_names(OsStr::new("a.tar.gz"), 250);
        assert_eq!(nested[2].as_deref(), Some(OsStr::new("a.tar (2).gz")));
    }

    #[test]
    fn suffix_bytes_are_reserved_before_truncation() {
        // A stem that only fits with the short suffix must shrink further
        // for the long suffix, so ` (1)` and ` (999)` never collide.
        let stem = "s".repeat(8);
        let base = format!("{stem}.bin");
        let candidates = rename_candidate_names(OsStr::new(&base), 16);
        // cap 16: " (1)" = 4, ".bin" = 4 -> stem budget 8 (base fits exactly)
        assert_eq!(
            candidates[1].as_deref(),
            Some(OsStr::new("ssssssss (1).bin"))
        );
        // " (999)" = 6 -> stem budget 6
        assert_eq!(
            candidates[999].as_deref(),
            Some(OsStr::new("ssssss (999).bin"))
        );
        // Distinguishing suffixes survive: no two candidates are equal.
        let mut seen = std::collections::HashSet::new();
        for name in candidates.iter().flatten() {
            assert!(seen.insert(name.to_string_lossy().into_owned()));
        }
    }

    #[test]
    fn unrepresentable_candidates_are_none() {
        // cap 7 is the Rename minimum: one stem byte plus " (999)" and no
        // extension.
        let candidates = rename_candidate_names(OsStr::new("nameless"), 7);
        assert_eq!(candidates[0].as_deref(), Some(OsStr::new("nameless")));
        for generated in &candidates[1..] {
            assert!(generated.is_some(), "1 stem byte + ' (n)' fits cap 7");
        }
        // A four-byte extension leaves no room for a stem at cap 7.
        // A six-byte extension leaves no room for a stem at cap 7.
        let candidates = rename_candidate_names(OsStr::new("x.abcde"), 7);
        assert!(candidates[1].is_none(), "'.abcde' cannot fit a stem");
        // The base itself is never truncated or rejected here.
        assert_eq!(candidates[0].as_deref(), Some(OsStr::new("x.abcde")));
    }

    #[test]
    fn truncated_siblings_stay_portable_components() {
        let candidates = rename_candidate_names(OsStr::new("AUXILIARY.bin"), 12);
        // The base is exempt from the cap (already sanitized upstream for
        // directory targets, verbatim for explicit files); generated
        // siblings must fit it.
        for name in candidates.iter().skip(1).flatten() {
            let as_str = name.to_string_lossy();
            let mut components = Path::new(as_str.as_ref()).components();
            assert!(matches!(
                components.next(),
                Some(std::path::Component::Normal(_))
            ));
            assert!(components.next().is_none());
            assert!(as_str.len() <= 12, "{as_str}");
        }
        // With cap 12 the first sibling is "AUXI (1).bin": a truncated stem
        // that would expose reserved "AUX" (cap 11 leaves no room for the
        // '_' prefix) is rejected as unrepresentable instead.
        assert_eq!(
            rename_candidate_names(OsStr::new("AUXILIARY.bin"), 12)[1].as_deref(),
            Some(OsStr::new("AUXI (1).bin"))
        );
        // A shorter cap truncates the stem but keeps the distinguishing
        // suffix ahead of the extension (the stem is then "AUX (1)", which
        // is not a reserved device basename).
        assert_eq!(
            rename_candidate_names(OsStr::new("AUXILIARY.bin"), 11)[1].as_deref(),
            Some(OsStr::new("AUX (1).bin"))
        );
    }

    #[test]
    fn validation_rejects_invalid_directories_and_options() {
        let dir = tempfile::tempdir().expect("tmp");
        let missing = dir.path().join("missing");
        let opts = options("download", 250);
        assert!(matches!(
            validate_directory_target(&missing, &opts, OverwritePolicy::FailIfExists),
            Err(DownloadError::Configuration(_))
        ));
        let file = dir.path().join("file.txt");
        std::fs::write(&file, b"x").expect("file");
        assert!(matches!(
            validate_directory_target(&file, &opts, OverwritePolicy::FailIfExists),
            Err(DownloadError::Configuration(_))
        ));
        // Traversal fallback: sanitization changes it.
        for bad_fallback in ["", "../escape", "a/b", "CON", "a<b"] {
            assert!(
                matches!(
                    validate_directory_target(
                        dir.path(),
                        &options(bad_fallback, 250),
                        OverwritePolicy::FailIfExists
                    ),
                    Err(DownloadError::Configuration(_))
                ),
                "{bad_fallback}"
            );
        }
        // Cap bounds.
        for (fallback, cap, policy) in [
            ("download", 251, OverwritePolicy::FailIfExists),
            ("download", 7, OverwritePolicy::FailIfExists), // below fallback len
            ("download", 6, OverwritePolicy::Rename),       // below Rename minimum
            ("longer-name", 10, OverwritePolicy::FailIfExists),
        ] {
            assert!(
                matches!(
                    validate_directory_target(dir.path(), &options(fallback, cap), policy),
                    Err(DownloadError::Configuration(_))
                ),
                "{fallback} {cap} {policy:?}"
            );
        }
        // Valid boundaries pass.
        assert!(
            validate_directory_target(dir.path(), &opts, OverwritePolicy::FailIfExists).is_ok()
        );
        assert!(
            validate_directory_target(dir.path(), &options("d", 7), OverwritePolicy::Rename)
                .is_ok()
        );
    }
}
