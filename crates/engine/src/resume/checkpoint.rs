//! Versioned checkpoint model (§15.1-§15.2, D3).
//!
//! Canonical JSON per §15.2 logical schema. Validated on load; unknown
//! fields are tolerated for forward compatibility (§15.1 extensible
//! across minor releases); version gating rejects newer formats.

use serde::{Deserialize, Serialize};

use crate::error::DownloadError;
use crate::http::validators::ResourceValidators;

/// Current checkpoint format version.
pub const CHECKPOINT_FORMAT_VERSION: u32 = 1;

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
    pub original_url: String,
    pub final_url: String,
    /// Opaque identity of the temp file for cross-restart validation.
    pub temp_path_identity: String,
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
        if self.format_version > CHECKPOINT_FORMAT_VERSION {
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
        let back = Checkpoint::from_json(&json).expect("deserialize");
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
