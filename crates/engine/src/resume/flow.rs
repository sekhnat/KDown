//! Resume orchestration helpers (§15.5, §26, tasks 4.4-4.6).
//!
//! The controller consumes these helpers; generation mismatches apply the
//! configured policy (fail or restart from zero) and never mix
//! generations (§26).

use std::path::Path;

use sha2::{Digest, Sha256, Sha512};

use crate::config::ResumePolicy;
use crate::error::DownloadError;
use crate::http::validators::ResourceValidators;
use crate::resume::checkpoint::{ByteRange, Checkpoint};
use crate::resume::checkpoint_store::CheckpointStore;

/// Policy for detected generation changes (§26: safety default is Fail).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GenerationChangePolicy {
    /// Fail with `ResourceChanged` (default; §11.3 core safety).
    #[default]
    Fail,
    /// Restart from byte zero, truncating/recreating temp state.
    RestartFromZero,
}

/// Stable, redaction-safe job identity for the checkpoint store
/// (hash of URL + destination; no secrets in filenames).
#[must_use]
pub fn job_identity(url: &str, destination: &Path) -> String {
    let mut h = Sha256::new();
    h.update(url.as_bytes());
    h.update(destination.as_os_str().as_encoded_bytes());
    hex8(&h.finalize())
}

fn hex8(bytes: &[u8]) -> String {
    bytes.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// Resume-flow helpers over the shared pipeline (§15.5).
pub struct ResumeSupport;

// Helpers are wired into the controller incrementally (tasks 4.4-4.6).
#[allow(dead_code)]
impl ResumeSupport {
    /// Prepare resume state honoring [`ResumePolicy`] (§7.2).
    ///
    /// §15.1/§38: a corrupt checkpoint is never trusted — under `Allowed`
    /// it degrades to a fresh start; under `Required` it is a hard error.
    pub(crate) fn check_resume_policy(
        resume: ResumePolicy,
        store: &dyn CheckpointStore,
        identity: &str,
    ) -> Result<Option<Checkpoint>, DownloadError> {
        match resume {
            ResumePolicy::Never => Ok(None),
            // Corrupt: fail-safe restart (§38 "Checkpoint corrupt").
            ResumePolicy::Allowed => Ok(store.load(identity).ok().flatten()),
            ResumePolicy::Required => store
                .load(identity)?
                .map(Some)
                .ok_or_else(|| {
                    DownloadError::Checkpoint("resume required but no checkpoint".into())
                }),
        }
    }

    /// Validate a checkpoint against the local temp file (§15.5 step 2):
    /// the temp file must exist and plausibly cover completed ranges.
    pub(crate) fn validate_temp_file(
        cp: &Checkpoint,
        temp_path: &Path,
    ) -> Result<(), DownloadError> {
        let meta = std::fs::metadata(temp_path).map_err(|e| {
            DownloadError::Checkpoint(format!(
                "temp file {} missing: {e}",
                temp_path.display()
            ))
        })?;
        let needed = cp
            .completed_ranges
            .last()
            .map(|(_, e)| e.saturating_add(1))
            .unwrap_or(0);
        if meta.len() < needed {
            return Err(DownloadError::Checkpoint(format!(
                "temp file {} too small: {} < {} required by completed ranges",
                temp_path.display(),
                meta.len(),
                needed
            )));
        }
        Ok(())
    }

    /// Compare remote validators with checkpoint (§15.5 step 4, §26).
    pub(crate) fn compare_generations(
        cp_validators: &ResourceValidators,
        remote: &ResourceValidators,
    ) -> Result<(), String> {
        cp_validators.same_generation(remote)
    }

    /// Compute remaining ranges after resume (§15.5 step 6): the
    /// complement of completed ranges within [0, total).
    #[must_use]
    pub fn remaining_ranges(total: u64, completed: &[ByteRange]) -> Vec<ByteRange> {
        let mut out = Vec::new();
        let mut next = 0u64;
        for &(s, e) in completed {
            if s > next {
                out.push((next, s - 1));
            }
            next = next.max(e + 1);
        }
        if next < total {
            out.push((next, total - 1));
        }
        out
    }
}

/// Sequential hash verification of a file against expected digests
/// (§16.2; used by both fresh and resumed verification).
#[allow(dead_code)] // consumed in task 4.4 controller wiring
pub(crate) fn verify_digests(
    path: &Path,
    algorithms: &[(String, String)],
) -> Result<(), DownloadError> {
    for (algo, expected_hex) in algorithms {
        let file = std::fs::File::open(path).map_err(|e| DownloadError::from_io(&e))?;
        let mut reader = std::io::BufReader::with_capacity(256 * 1024, file);
        let computed = match algo.as_str() {
            "sha256" => {
                let mut h = Sha256::new();
                std::io::copy(&mut reader, &mut h)
                    .map_err(|e| DownloadError::SinkWrite(e.to_string()))?;
                hex_digest(&h.finalize())
            }
            "sha512" => {
                let mut h = Sha512::new();
                std::io::copy(&mut reader, &mut h)
                    .map_err(|e| DownloadError::SinkWrite(e.to_string()))?;
                hex_digest(&h.finalize())
            }
            other => {
                return Err(DownloadError::IntegrityMismatch(format!(
                    "unsupported persisted algorithm {other}"
                )))
            }
        };
        if computed != expected_hex.to_ascii_lowercase() {
            return Err(DownloadError::IntegrityMismatch(format!(
                "{algo} mismatch: expected {expected_hex}, computed {computed}"
            )));
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remaining_ranges_complement() {
        // Completed [0,99] and [400,499] of 1000: pending [100,399], [500,999].
        let remaining = ResumeSupport::remaining_ranges(1000, &[(0, 99), (400, 499)]);
        assert_eq!(remaining, vec![(100, 399), (500, 999)]);
    }

    #[test]
    fn empty_completed_is_full_range() {
        assert_eq!(ResumeSupport::remaining_ranges(10, &[]), vec![(0, 9)]);
    }

    #[test]
    fn fully_completed_has_no_remaining() {
        assert_eq!(ResumeSupport::remaining_ranges(10, &[(0, 9)]), vec![]);
    }

    #[test]
    fn identity_is_stable_and_secret_free() {
        let a = job_identity("https://x/f", Path::new("/tmp/a"));
        let b = job_identity("https://x/f", Path::new("/tmp/a"));
        let c = job_identity("https://x/g", Path::new("/tmp/a"));
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(!a.contains("https"), "no URL leakage in identity");
    }

    #[test]
    fn generation_comparison_detects_change() {
        let cp_v = ResourceValidators {
            etag: Some("\"v1\"".into()),
            etag_is_weak: false,
            last_modified: None,
            total_size: Some(100),
        };
        let same = cp_v.clone();
        let changed = ResourceValidators {
            etag: Some("\"v2\"".into()),
            ..cp_v.clone()
        };
        assert!(ResumeSupport::compare_generations(&cp_v, &same).is_ok());
        assert!(ResumeSupport::compare_generations(&cp_v, &changed).is_err());
    }

    #[test]
    fn missing_temp_file_fails_validation() {
        let cp = Checkpoint::new("j", "u", "t");
        let err = ResumeSupport::validate_temp_file(&cp, Path::new("/nonexistent/x"))
            .expect_err("missing temp");
        assert!(matches!(err, DownloadError::Checkpoint(_)));
    }
}