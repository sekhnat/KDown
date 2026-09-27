//! Versioned checkpoint model (§15.1-§15.2, D3).
//!
//! Canonical JSON per §15.2 logical schema. Validated on load; unknown
//! fields are tolerated for forward compatibility (§15.1 extensible
//! across minor releases); version gating rejects newer formats.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::DownloadError;
use crate::http::validators::ResourceValidators;

/// Current checkpoint format version.
///
/// v2 binds persisted ranges to the owned temp file object (identity)
/// and covered bytes (bounded digest); v1 files are rejected so legacy
/// resume state restarts conservatively instead of being reinterpreted.
pub const CHECKPOINT_FORMAT_VERSION: u32 = 2;

/// A completed byte range `[start, end]` inclusive (§3 Range).
pub type ByteRange = (u64, u64);

/// Errors surfaced by checkpoint validation (§15.1: validated before use).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CheckpointError {
    #[error("checkpoint format version {found} not supported (known up to {known})")]
    UnsupportedVersion { found: u32, known: u32 },
    #[error("checkpoint corrupt: {0}")]
    Corrupt(String),
    #[error("checkpoint inconsistent: {0}")]
    Inconsistent(String),
    #[error("checkpoint size {size} exceeds the configured budget {cap}")]
    TooLarge { size: u64, cap: u64 },
    #[error("checkpoint size estimate overflowed")]
    Overflow,
}

impl From<CheckpointError> for DownloadError {
    fn from(e: CheckpointError) -> Self {
        DownloadError::Checkpoint(e.to_string())
    }
}

/// Persisted resumable state (§15.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub format_version: u32,
    pub job_id: String,
    /// In-memory only: the original request URL. Never serialized and never
    /// loaded from a store — the default sidecar must not retain raw URL
    /// credentials (task 5.4) and admission does not read it back (the
    /// opaque [`Self::job_id`] plus validators and the local binding
    /// identify the resource). A legacy sidecar carrying URL fields loads
    /// without reviving them.
    #[serde(skip)]
    pub original_url: String,
    /// In-memory only: the probe's redirect-resolved URL. Never serialized,
    /// for the same reason as [`Self::original_url`].
    #[serde(skip)]
    pub final_url: String,
    /// Opaque identity of the temp file for cross-restart validation.
    pub temp_path_identity: String,
    /// v2: owned identity of the temp file object the ranges describe
    /// (`<path>|<device>:<inode>` on Unix). Verified on admission when
    /// present.
    #[serde(default)]
    pub owned_temp_identity: Option<String>,
    /// v2: bounded SHA-256 over the covered bytes (see
    /// [`Self::set_covered_digest`]). Verified on admission when present;
    /// `None` for covered sets larger than the digest window.
    #[serde(default)]
    pub covered_digest: Option<String>,
    /// `None` for unknown-length downloads (§25).
    pub total_size: Option<u64>,
    pub validators: ResourceValidators,
    /// Completed inclusive ranges, normalized (sorted, non-overlapping).
    pub completed_ranges: Vec<ByteRange>,
    pub expected_hashes: Vec<ExpectedDigest>,
    pub created_at: String,
    pub updated_at: String,
}

/// Caller-provided digest persisted for resume verification (§15.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpectedDigest {
    pub algorithm: String,
    pub hex: String,
}

impl Checkpoint {
    #[must_use]
    pub fn new(
        job_id: impl Into<String>,
        original_url: impl Into<String>,
        temp_path_identity: impl Into<String>,
    ) -> Self {
        let now = iso_now();
        Self {
            format_version: CHECKPOINT_FORMAT_VERSION,
            job_id: job_id.into(),
            original_url: original_url.into(),
            final_url: String::new(),
            temp_path_identity: temp_path_identity.into(),
            owned_temp_identity: None,
            covered_digest: None,
            total_size: None,
            validators: ResourceValidators::default(),
            completed_ranges: vec![],
            expected_hashes: vec![],
            created_at: now.clone(),
            updated_at: now,
        }
    }

    /// Serialize to canonical JSON (§15.2).
    ///
    /// # Errors
    /// Serialization failure (should not happen for this plain model).
    pub fn to_json(&self) -> Result<String, CheckpointError> {
        serde_json::to_string(self).map_err(|e| CheckpointError::Corrupt(e.to_string()))
    }

    /// Checked UPPER-BOUND of the serialized size in bytes, computed
    /// WITHOUT allocating the serialization (task 4.2).
    ///
    /// Every JSON field is counted, including validator strings at their
    /// worst-case escaping cost (up to six bytes per input byte: `\u00XX`
    /// plus quotes), so remotely supplied ETag/Last-Modified text can never
    /// undercount the real serialization. Arithmetic is checked: an
    /// overflowing bound fails closed instead of wrapping.
    ///
    /// # Errors
    /// [`CheckpointError::Overflow`] when a count or length cannot be
    /// represented.
    pub fn checked_serialized_size(&self) -> Result<u64, CheckpointError> {
        // Worst-case JSON string cost: 6x the UTF-8 length (each char can
        // become a `\uXXXX` escape) plus two quotes; `null` otherwise.
        fn json_string(bytes: usize) -> Result<u64, CheckpointError> {
            let len = u64::try_from(bytes).map_err(|_| CheckpointError::Overflow)?;
            len.checked_mul(6)
                .and_then(|v| v.checked_add(2))
                .ok_or(CheckpointError::Overflow)
        }
        fn json_optional_string(value: Option<&str>) -> Result<u64, CheckpointError> {
            match value {
                Some(v) => json_string(v.len()),
                None => Ok(4), // `null`
            }
        }
        fn count(len: usize) -> Result<u64, CheckpointError> {
            u64::try_from(len).map_err(|_| CheckpointError::Overflow)
        }

        const FIXED_OVERHEAD: u64 = 512; // keys, version, timestamps, nesting
        const RANGE_ENTRY_COST: u64 = 48; // [start,end] with 20-digit values
        const NUMBER_COST: u64 = 20; // u64 decimal digits
        const BOOL_COST: u64 = 5; // `false`
        const DIGEST_FIXED_COST: u64 = 64; // keys and quoting around a digest

        let mut total = FIXED_OVERHEAD;
        let mut add = |value: u64| -> Result<(), CheckpointError> {
            total = total.checked_add(value).ok_or(CheckpointError::Overflow)?;
            Ok(())
        };

        // URLs are memory-only (`skip_serializing`): they are absent from
        // the persisted JSON and must not be charged here.
        for field in [&self.temp_path_identity, &self.created_at, &self.updated_at] {
            add(json_string(field.len())?)?;
        }
        add(json_optional_string(self.owned_temp_identity.as_deref())?)?;
        add(json_optional_string(self.covered_digest.as_deref())?)?;
        add(json_optional_string(self.validators.etag.as_deref())?)?;
        add(json_optional_string(
            self.validators.last_modified.as_deref(),
        )?)?;
        add(if self.validators.etag_is_weak {
            4 // `true`
        } else {
            BOOL_COST
        })?;
        add(NUMBER_COST)?; // total_size
        add(count(self.completed_ranges.len())?
            .checked_mul(RANGE_ENTRY_COST)
            .ok_or(CheckpointError::Overflow)?)?;
        for digest in &self.expected_hashes {
            add(json_string(digest.algorithm.len())?)?;
            add(json_string(digest.hex.len())?)?;
            add(DIGEST_FIXED_COST)?;
        }
        Ok(total)
    }

    /// The bounded serialized-size policy (task 3.6): refuse a checkpoint
    /// whose estimated serialization exceeds `cap` BEFORE any allocation.
    ///
    /// # Errors
    /// [`CheckpointError::TooLarge`] when the estimate exceeds the budget.
    pub fn check_serialized_within(&self, cap: u64) -> Result<u64, CheckpointError> {
        let estimate = self.checked_serialized_size()?;
        if estimate > cap {
            return Err(CheckpointError::TooLarge {
                size: estimate,
                cap,
            });
        }
        Ok(estimate)
    }

    /// Parse and validate from JSON (§15.5 step 1).
    ///
    /// # Errors
    /// [`CheckpointError`] on malformed JSON, unknown newer version, or
    /// structural inconsistency.
    pub fn from_json(json: &str) -> Result<Self, CheckpointError> {
        // Unknown-field tolerant: serde default behavior ignores extras
        // with serde_json::from_str unless deny_unknown_fields is set.
        let cp: Checkpoint =
            serde_json::from_str(json).map_err(|e| CheckpointError::Corrupt(e.to_string()))?;
        cp.validate()?;
        Ok(cp)
    }

    /// Structural validation (§15.5 step 1).
    ///
    /// # Errors
    /// Inconsistent fields (end < start, overlapping/unordered ranges,
    /// unsupported version).
    pub fn validate(&self) -> Result<(), CheckpointError> {
        if self.format_version != CHECKPOINT_FORMAT_VERSION {
            return Err(CheckpointError::UnsupportedVersion {
                found: self.format_version,
                known: CHECKPOINT_FORMAT_VERSION,
            });
        }
        if self.job_id.is_empty() {
            return Err(CheckpointError::Inconsistent("empty job_id".into()));
        }
        // Normalized-range invariants (§12.1).
        let mut prev_end: Option<u64> = None;
        for &(start, end) in &self.completed_ranges {
            if end < start {
                return Err(CheckpointError::Inconsistent(format!(
                    "range [{start}, {end}] has end < start"
                )));
            }
            if let Some(pe) = prev_end {
                if start <= pe {
                    return Err(CheckpointError::Inconsistent(format!(
                        "range [{start}, {end}] overlaps or precedes previous end {pe}"
                    )));
                }
            }
            prev_end = Some(end);
        }
        if let (Some(total), Some((_, last_end))) = (self.total_size, self.completed_ranges.last())
        {
            if *last_end >= total {
                return Err(CheckpointError::Inconsistent(format!(
                    "range end {last_end} exceeds total size {total}"
                )));
            }
        }
        Ok(())
    }

    /// Insert a completed range and renormalize (merge adjacent/overlapping).
    pub fn record_completed(&mut self, start: u64, end: u64) {
        let mut ranges: Vec<ByteRange> = self
            .completed_ranges
            .iter()
            .copied()
            .chain(std::iter::once((start, end)))
            .collect();
        ranges.sort();
        let mut merged: Vec<ByteRange> = vec![];
        for (s, e) in ranges {
            match merged.last_mut() {
                Some((_, pe)) if s <= pe.saturating_add(1) => {
                    *pe = (*pe).max(e);
                }
                _ => merged.push((s, e)),
            }
        }
        self.completed_ranges = merged;
        self.updated_at = iso_now();
    }

    /// Bytes covered by completed ranges (unique, §19.1).
    #[must_use]
    pub fn completed_bytes(&self) -> u64 {
        self.completed_ranges.iter().map(|(s, e)| e - s + 1).sum()
    }

    /// Stamp the v2 owned temp identity of the file object backing this
    /// checkpoint's ranges. The identity is the resolved temp path plus
    /// the object's device/inode (on Unix); it changes whenever the entry
    /// is replaced, so a swapped file can never satisfy admission.
    pub fn set_owned_temp_identity(&mut self, temp_path: &Path) {
        self.owned_temp_identity = owned_temp_identity(temp_path);
    }

    /// Stamp the v2 bounded covered-byte digest: SHA-256 over up to
    /// `COVERED_DIGEST_WINDOW` bytes of the covered ranges, in order.
    ///
    /// Large covered sets leave the digest `None`; identity and remote
    /// validators still carry the binding. A failed read also leaves it
    /// `None` (the save proceeds; admission then has less evidence).
    pub fn set_covered_digest(&mut self, temp_path: &Path) {
        self.covered_digest = covered_digest(temp_path, &self.completed_ranges);
    }

    /// Verify the v2 local binding against the file at `temp_path`.
    ///
    /// Missing evidence (fields absent, or a digest outside the bounded
    /// window) is not an error here; mismatched evidence is.
    ///
    /// # Errors
    /// Human-readable reason when the recorded identity or digest does not
    /// describe the file now at `temp_path`.
    pub fn verify_local_binding(&self, temp_path: &Path) -> Result<(), String> {
        if let Some(expected) = &self.owned_temp_identity {
            let actual = owned_temp_identity(temp_path)
                .ok_or_else(|| "temp identity unavailable".to_string())?;
            if &actual != expected {
                return Err("owned temp identity mismatch".into());
            }
        }
        if let Some(expected) = &self.covered_digest {
            let actual = covered_digest(temp_path, &self.completed_ranges)
                .ok_or_else(|| "covered digest unavailable".to_string())?;
            if &actual != expected {
                return Err("covered-byte digest mismatch".into());
            }
        }
        Ok(())
    }
}

/// Bounded window hashed into [`Checkpoint::covered_digest`].
pub const COVERED_DIGEST_WINDOW: u64 = 1024 * 1024;

/// Owned identity of a temp file object: resolved path plus device/inode
/// (Unix) or length (other platforms). `None` when the entry is missing or
/// not a regular file.
#[must_use]
pub fn owned_temp_identity(temp_path: &Path) -> Option<String> {
    let meta = std::fs::symlink_metadata(temp_path).ok()?;
    if !meta.file_type().is_file() {
        return None;
    }
    #[cfg(unix)]
    let object = {
        use std::os::unix::fs::MetadataExt as _;
        format!("{}:{}", meta.dev(), meta.ino())
    };
    #[cfg(not(unix))]
    let object = format!("{}", meta.len());
    Some(format!("{}|{}", temp_path.display(), object))
}

/// SHA-256 over up to [`COVERED_DIGEST_WINDOW`] covered bytes, in range
/// order. `None` when nothing was covered or a read failed.
fn covered_digest(temp_path: &Path, ranges: &[ByteRange]) -> Option<String> {
    use sha2::{Digest, Sha256};
    use std::io::{Read, Seek, SeekFrom};

    if ranges.is_empty() {
        return None;
    }
    let mut file = std::fs::File::open(temp_path).ok()?;
    let mut hasher = Sha256::new();
    let mut budget = COVERED_DIGEST_WINDOW;
    for &(start, end) in ranges {
        let mut offset = start;
        while offset <= end && budget > 0 {
            let len = (end - offset + 1).min(budget);
            let mut buffer = vec![0u8; usize::try_from(len).ok()?];
            file.seek(SeekFrom::Start(offset)).ok()?;
            file.read_exact(&mut buffer).ok()?;
            hasher.update(&buffer);
            offset += len;
            budget -= len;
        }
        if budget == 0 {
            break;
        }
    }
    let digest = hasher.finalize();
    Some(digest.iter().map(|b| format!("{b:02x}")).collect())
}

/// ISO-8601-ish timestamp; ordering matters only for diagnostics (§15.2).
fn iso_now() -> String {
    // Wall-clock seconds since epoch; sufficient for created/updated
    // ordering in v1 (no external chrono dependency).
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{secs}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Checkpoint {
        let mut cp = Checkpoint::new("job-1", "https://example/file", "temp-identity");
        cp.total_size = Some(1000);
        cp.validators = ResourceValidators {
            etag: Some("\"v1\"".into()),
            etag_is_weak: false,
            last_modified: Some("Mon, 22 Sep 2026 00:00:00 GMT".into()),
            total_size: Some(1000),
        };
        cp
    }

    #[test]
    fn serialize_roundtrip() {
        let mut cp = sample();
        cp.record_completed(0, 99);
        cp.record_completed(200, 299);
        let json = cp.to_json().expect("serialize");
        let mut back = Checkpoint::from_json(&json).expect("deserialize");
        // URLs are memory-only (task 5.4): a round trip does not revive them.
        assert!(back.original_url.is_empty());
        assert!(back.final_url.is_empty());
        back.original_url = cp.original_url.clone();
        back.final_url = cp.final_url.clone();
        assert_eq!(back, cp);
        assert_eq!(back.completed_ranges, vec![(0, 99), (200, 299)]);
        assert_eq!(back.completed_bytes(), 200);
    }

    #[test]
    fn unknown_fields_tolerated() {
        let mut cp = sample();
        cp.record_completed(0, 9);
        let mut json = cp.to_json().expect("json");
        // Inject an unknown field.
        json = json.replace(
            &format!("\"format_version\":{CHECKPOINT_FORMAT_VERSION}"),
            &format!("\"format_version\":{CHECKPOINT_FORMAT_VERSION},\"future_field\":42"),
        );
        let back = Checkpoint::from_json(&json).expect("unknown field tolerated");
        assert_eq!(back.format_version, CHECKPOINT_FORMAT_VERSION);
    }

    /// Task 5.4: the default sidecar representation contains no URL text,
    /// so a signed URL's userinfo and query values cannot leak to disk.
    #[test]
    fn serialized_json_never_contains_urls() {
        let mut cp = Checkpoint::new(
            "job-1",
            "https://user:URL-USERINFO-SECRET@cdn.test/file?token=URL-QUERY-SECRET",
            "temp-identity",
        );
        cp.final_url = "https://cdn.test/file?token=FINAL-URL-SECRET".to_string();
        let json = cp.to_json().expect("serialize");
        for secret in [
            "URL-USERINFO-SECRET",
            "URL-QUERY-SECRET",
            "FINAL-URL-SECRET",
        ] {
            assert!(
                !json.contains(secret),
                "URL secret `{secret}` persisted: {json}"
            );
        }
        assert!(!json.contains("original_url"), "field persisted: {json}");
        assert!(!json.contains("final_url"), "field persisted: {json}");
        // The checked bound still covers the URL-free serialization.
        assert!(cp.checked_serialized_size().expect("bound") >= json.len() as u64);
    }

    /// A sidecar written before the URL fields were dropped still loads
    /// (unknown-field tolerant) without reviving the URL text.
    #[test]
    fn legacy_url_fields_load_without_being_revived() {
        let cp = sample();
        let mut value: serde_json::Value =
            serde_json::from_str(&cp.to_json().expect("json")).expect("value");
        value["original_url"] =
            serde_json::json!("https://user:OLD-SECRET@cdn.test/f?token=OLD-QUERY");
        value["final_url"] = serde_json::json!("https://cdn.test/f?token=OLD-FINAL");
        let json = serde_json::to_string(&value).expect("json");
        let loaded = Checkpoint::from_json(&json).expect("legacy sidecar still loads");
        assert!(loaded.original_url.is_empty());
        assert!(loaded.final_url.is_empty());
        assert_eq!(loaded.format_version, CHECKPOINT_FORMAT_VERSION);
    }

    #[test]
    fn corrupt_json_rejected() {
        let err = Checkpoint::from_json("{not json").expect_err("must reject");
        assert!(matches!(err, CheckpointError::Corrupt(_)));
        // Valid JSON, wrong shape.
        assert!(Checkpoint::from_json("[1,2,3]").is_err());
    }

    #[test]
    fn version_gating() {
        let mut cp = sample();
        cp.format_version = CHECKPOINT_FORMAT_VERSION + 1;
        let json = cp.to_json().expect("serialize");
        let err = Checkpoint::from_json(&json).expect_err("newer version must reject");
        assert!(matches!(err, CheckpointError::UnsupportedVersion { .. }));
    }

    #[test]
    fn legacy_version_restarts_conservatively() {
        // v1 state is rejected, so admission discards it and restarts
        // instead of reinterpreting old ranges without the v2 binding.
        let mut cp = sample();
        cp.format_version = 1;
        let json = cp.to_json().expect("serialize");
        let err = Checkpoint::from_json(&json).expect_err("legacy version must reject");
        assert!(matches!(
            err,
            CheckpointError::UnsupportedVersion { found: 1, known: 2 }
        ));
    }

    #[test]
    fn owned_binding_detects_replacement_and_edits() {
        let dir = tempfile::tempdir().expect("tmp");
        let temp = dir.path().join("out.part");
        std::fs::write(&temp, b"verified-bytes").expect("temp");
        let mut cp = Checkpoint::new("job", "https://example/f", "tmp");
        cp.record_completed(0, 13);
        cp.set_owned_temp_identity(&temp);
        cp.set_covered_digest(&temp);
        assert!(cp.owned_temp_identity.is_some());
        assert!(cp.covered_digest.is_some());
        assert!(cp.verify_local_binding(&temp).is_ok());

        // Same inode, different bytes: the digest catches it.
        std::fs::write(&temp, b"tampered-bytes").expect("tamper");
        let err = cp.verify_local_binding(&temp).expect_err("digest mismatch");
        assert!(err.contains("digest"), "{err}");

        // A genuinely different object is caught by the stored identity.
        // The check cannot rely on a same-directory delete/recreate handing
        // out a new inode: filesystems may legally reuse the freed inode, so
        // a fabricated different identity proves the identity branch
        // deterministically (real replacement races are covered by the
        // publication integration fixtures).
        let mut replaced = cp.clone();
        replaced.owned_temp_identity = Some(format!(
            "{}|different",
            cp.owned_temp_identity.as_deref().expect("identity")
        ));
        let err = replaced
            .verify_local_binding(&temp)
            .expect_err("identity mismatch");
        assert!(err.contains("identity"), "{err}");
    }

    #[test]
    fn checked_bound_covers_escaping_and_validators() {
        // A remotely supplied validator can require JSON escaping; the
        // bound must never be below the real serialization.
        let mut cp = sample();
        cp.validators.etag = Some("\"\\\\\"\n\t\u{7f}quote\"".into());
        cp.validators.last_modified = Some("<\"&>\\".into());
        cp.record_completed(0, 9);
        let bound = cp.checked_serialized_size().expect("bound");
        let actual = cp.to_json().expect("json").len() as u64;
        assert!(
            bound >= actual,
            "the bound ({bound}) must cover the serialized size ({actual})"
        );
    }

    #[test]
    fn large_validator_is_rejected_by_the_cap_before_allocation() {
        let mut cp = sample();
        cp.validators.etag = Some(format!("\"{}\"", "E".repeat(4096)));
        let err = cp
            .check_serialized_within(1024)
            .expect_err("an oversized validator must fail the cap");
        assert!(matches!(err, CheckpointError::TooLarge { .. }), "{err:?}");
    }

    #[test]
    fn bound_is_a_result_so_overflow_fails_closed() {
        // The estimator uses checked arithmetic end to end: its contract
        // is a `Result`, and every additive step maps an impossible count
        // to the structured overflow error instead of wrapping.
        let cp = sample();
        let bound = cp.checked_serialized_size().expect("bound");
        assert!(bound > 0);
        assert!(matches!(
            cp.check_serialized_within(bound).expect("within"),
            value if value == bound
        ));
    }

    #[test]
    fn inconsistent_ranges_rejected() {
        let mut cp = sample();
        cp.completed_ranges = vec![(500, 100)]; // end < start
        assert!(cp.validate().is_err());
        cp.completed_ranges = vec![(0, 99), (50, 99)]; // overlap
        assert!(cp.validate().is_err());
        cp.completed_ranges = vec![(200, 99)]; // end < start
        assert!(cp.validate().is_err());
        // Total size violation: end beyond total.
        let mut cp2 = sample();
        cp2.completed_ranges = vec![(0, 1000)]; // total is 1000 -> end >= total
        assert!(cp2.validate().is_err(), "range end >= total must fail");
    }

    #[test]
    fn record_completed_normalizes() {
        let mut cp = sample();
        cp.record_completed(400, 499);
        cp.record_completed(0, 99);
        cp.record_completed(100, 199); // adjacent -> merge
        cp.record_completed(490, 599); // overlap -> merge
        assert_eq!(cp.completed_ranges, vec![(0, 199), (400, 599)]);
        assert_eq!(cp.completed_bytes(), 400);
    }
}
