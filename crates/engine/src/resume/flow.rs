//! Resume admission protocol (§15.5, §26).
//!
//! Admission is one deep decision owned by this module: the controller
//! begins admission (policy-aware checkpoint loading) before probing and
//! finalizes it with probed remote metadata. The protocol orders
//! generation comparison before temp-output plausibility, selects the
//! continue/fail/restart behavior, and performs the checkpoint cleanup a
//! restart requires. Job state transitions and event emission stay with
//! the controller, which publishes returned notification facts; the
//! resume module never emits events itself.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256, Sha512};

use crate::config::ResumePolicy;
use crate::error::DownloadError;
use crate::http::validators::ResourceValidators;
use crate::resume::checkpoint::{ByteRange, Checkpoint, CheckpointError};
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

/// A fact the controller must publish as an event. Admission decides;
/// the observable effect stays job-owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResumeNotice {
    /// The probed resource no longer matches the checkpoint generation (§26).
    ResourceChanged { detail: String },
}

/// A rejected admission: the job fails with this error after the
/// controller publishes any notification facts.
#[derive(Debug)]
pub(crate) struct ResumeFailure {
    pub(crate) error: DownloadError,
    pub(crate) notices: Vec<ResumeNotice>,
}

impl From<CheckpointError> for ResumeFailure {
    fn from(e: CheckpointError) -> Self {
        Self {
            error: DownloadError::Checkpoint(e.to_string()),
            notices: vec![],
        }
    }
}

/// Segmented-transfer resume inputs: every admitted range is reusable.
#[derive(Debug)]
pub(crate) struct SegmentedResume<'a> {
    pub(crate) ranges: &'a [ByteRange],
    pub(crate) reused_bytes: u64,
}

/// Sequential-transfer resume inputs: continue at the end of the
/// contiguous prefix and count only that prefix as reused.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SequentialResume {
    pub(crate) offset: u64,
    pub(crate) reused_bytes: u64,
}

/// The admitted transfer plan. Checkpoint contents stay private; callers
/// consume admitted facts and mode-specific views only.
#[derive(Debug)]
pub(crate) struct ResumePlan {
    /// Admitted checkpoint, present only when resuming.
    checkpoint: Option<Checkpoint>,
    /// Validated completed ranges available for reuse.
    completed_ranges: Vec<ByteRange>,
    /// Length of the completed range starting at byte zero, if any.
    /// Sequential transfer reuses only this prefix: the stream rewrites
    /// everything after it, so later disjoint ranges are not reusable.
    contiguous_prefix: u64,
    warnings: Vec<String>,
    notices: Vec<ResumeNotice>,
}

/// Final admission decision (§15.5 steps 2-7).
#[derive(Debug)]
#[must_use]
pub(crate) enum AdmissionDecision {
    /// Safe to begin transfer setup with this plan.
    Proceed(Box<ResumePlan>),
    /// Fail the job with this error, publishing its notices first.
    Reject(ResumeFailure),
}

impl ResumePlan {
    fn fresh(warnings: Vec<String>, notices: Vec<ResumeNotice>) -> Self {
        Self {
            checkpoint: None,
            completed_ranges: vec![],
            contiguous_prefix: 0,
            warnings,
            notices,
        }
    }

    fn resuming(cp: Checkpoint) -> Self {
        let ranges = cp.completed_ranges.clone();
        // Only a range starting at byte zero forms a sequential prefix.
        let contiguous_prefix = ranges
            .first()
            .filter(|(s, _)| *s == 0)
            .map_or(0, |(_, e)| e.saturating_add(1));
        Self {
            checkpoint: Some(cp),
            completed_ranges: ranges,
            contiguous_prefix,
            warnings: vec![],
            notices: vec![],
        }
    }

    /// Whether admitted state exists for the transfer to continue.
    #[must_use]
    pub(crate) fn is_resuming(&self) -> bool {
        !self.completed_ranges.is_empty()
    }

    /// Checkpoint validators for an `If-Range` resume request (§11.3).
    #[must_use]
    pub(crate) fn validators(&self) -> Option<ResourceValidators> {
        self.checkpoint.as_ref().map(|cp| cp.validators.clone())
    }

    /// The admitted checkpoint, when one remains reusable.
    #[must_use]
    pub(crate) fn checkpoint(&self) -> Option<&Checkpoint> {
        self.checkpoint.as_ref()
    }

    /// Warnings accumulated by admission (unusable-state restarts).
    #[must_use]
    pub(crate) fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// Notification facts the controller must publish as events.
    #[must_use]
    pub(crate) fn notices(&self) -> &[ResumeNotice] {
        &self.notices
    }

    /// View for a segmented transfer: every admitted range is reusable.
    #[must_use]
    pub(crate) fn segmented(&self) -> SegmentedResume<'_> {
        SegmentedResume {
            ranges: &self.completed_ranges,
            reused_bytes: completed_bytes(&self.completed_ranges),
        }
    }

    /// View for a sequential transfer: continue at the contiguous
    /// prefix's end; disjoint ranges are overwritten, not skipped.
    #[must_use]
    pub(crate) fn sequential(&self) -> SequentialResume {
        SequentialResume {
            offset: self.contiguous_prefix,
            reused_bytes: self.contiguous_prefix,
        }
    }
}

/// Unique bytes covered by normalized, non-overlapping ranges (§19.1).
fn completed_bytes(ranges: &[ByteRange]) -> u64 {
    ranges.iter().map(|(s, e)| e.saturating_sub(*s) + 1).sum()
}

/// Phase 1 of admission (§15.5 step 1, §7.2): policy-aware checkpoint
/// loading before any network activity. Returns the pending admission to
/// finalize after the probe, or an early rejection. `temp_path` is the
/// resolved local partial output admission will later validate.
///
/// # Errors
/// [`ResumeFailure`] when `ResumePolicy::Required` finds no usable
/// checkpoint, or when unusable state cannot be cleaned up (fail closed).
pub(crate) fn begin_admission<'a>(
    resume: ResumePolicy,
    identity: &str,
    temp_path: PathBuf,
    store: &'a dyn CheckpointStore,
) -> Result<PendingAdmission<'a>, ResumeFailure> {
    let checkpoint = match resume {
        // Resume disabled: never read or disturb persisted state (§7.2).
        ResumePolicy::Never => None,
        ResumePolicy::Allowed => match store.load(identity) {
            Ok(cp) => cp,
            // Corrupt checkpoint: never trusted (§38). Remove it so a
            // later process restart cannot trust it either; cleanup
            // failure is fatal (fail closed).
            Err(e) => {
                delete_restart_state(store, identity)?;
                return Ok(PendingAdmission::fresh(
                    store,
                    identity,
                    temp_path,
                    vec![format!("checkpoint unusable ({e}); restarting from zero")],
                ));
            }
        },
        ResumePolicy::Required => match store.load(identity)? {
            Some(cp) => Some(cp),
            None => {
                return Err(ResumeFailure {
                    error: DownloadError::Checkpoint("resume required but no checkpoint".into()),
                    notices: vec![],
                });
            }
        },
    };
    Ok(match checkpoint {
        Some(cp) => PendingAdmission {
            store,
            identity: identity.to_string(),
            temp_path,
            warnings: vec![],
            checkpoint: Some(cp),
        },
        None => PendingAdmission::fresh(store, identity, temp_path, vec![]),
    })
}

/// Phase 2 of admission: owns the loaded checkpoint (opaque to the
/// controller) plus the context needed to finalize the decision or clean
/// up on restart.
pub(crate) struct PendingAdmission<'a> {
    store: &'a dyn CheckpointStore,
    identity: String,
    temp_path: PathBuf,
    warnings: Vec<String>,
    checkpoint: Option<Checkpoint>,
}

impl std::fmt::Debug for PendingAdmission<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingAdmission")
            .field("identity", &self.identity)
            .field("temp_path", &self.temp_path)
            .field("has_checkpoint", &self.checkpoint.is_some())
            .finish_non_exhaustive()
    }
}

impl<'a> PendingAdmission<'a> {
    fn fresh(
        store: &'a dyn CheckpointStore,
        identity: &str,
        temp_path: PathBuf,
        warnings: Vec<String>,
    ) -> Self {
        Self {
            store,
            identity: identity.to_string(),
            temp_path,
            warnings,
            checkpoint: None,
        }
    }

    /// Finalize admission against probed remote metadata (§15.5 steps
    /// 2-7): generation comparison first — a mismatch never mixes
    /// generations (§26) — then temp-output plausibility, then the
    /// continue/restart decision and any cleanup it requires.
    pub(crate) fn finalize(self, remote: &ResourceValidators) -> AdmissionDecision {
        let Some(cp) = self.checkpoint else {
            return AdmissionDecision::Proceed(Box::new(ResumePlan::fresh(self.warnings, vec![])));
        };
        if cp.validators.same_generation(remote).is_err() {
            // Preserve checkpoint and temp state for the configured
            // generation policy (fail-only in v1).
            return AdmissionDecision::Reject(ResumeFailure {
                error: DownloadError::ResourceChanged("resource changed since checkpoint".into()),
                notices: vec![ResumeNotice::ResourceChanged {
                    detail: "validators differ from checkpoint".into(),
                }],
            });
        }
        if let Err(e) = validate_temp_file(&cp, &self.temp_path) {
            // Unusable local output: conservative restart (§15.5/§38).
            if let Err(failure) = delete_restart_state(self.store, &self.identity) {
                return AdmissionDecision::Reject(failure);
            }
            let mut warnings = self.warnings;
            warnings.push(format!("checkpoint unusable ({e}); restarting from zero"));
            return AdmissionDecision::Proceed(Box::new(ResumePlan::fresh(warnings, vec![])));
        }
        AdmissionDecision::Proceed(Box::new(ResumePlan::resuming(cp)))
    }
}

/// Delete stale checkpoint state before a restart. Fail closed: stale
/// ranges surviving beside restarted output could make a later resume
/// trust the wrong local state (§15.5, §38).
///
/// # Errors
/// [`ResumeFailure`] when the store cannot remove the stale checkpoint.
fn delete_restart_state(store: &dyn CheckpointStore, identity: &str) -> Result<(), ResumeFailure> {
    store.delete(identity).map_err(|e| ResumeFailure {
        error: DownloadError::Checkpoint(format!("checkpoint cleanup failed: {e}")),
        notices: vec![],
    })
}

/// Validate the checkpoint against the local temp output (§15.5 step 2):
/// the temp file must exist and plausibly cover completed ranges.
///
/// # Errors
/// [`DownloadError::Checkpoint`] when the temp file is missing or too
/// small for the recorded ranges.
fn validate_temp_file(cp: &Checkpoint, temp_path: &Path) -> Result<(), DownloadError> {
    let meta = std::fs::metadata(temp_path).map_err(|e| {
        DownloadError::Checkpoint(format!("temp file {} missing: {e}", temp_path.display()))
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

/// Sequential hash verification of a file against expected digests
/// (§16.2; used by both fresh and resumed verification).
///
/// # Errors
/// [`DownloadError`] on I/O failure, unsupported algorithm, or digest
/// mismatch.
#[allow(dead_code)] // consumed in  controller wiring
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

    fn validators_for(etag: &str) -> ResourceValidators {
        ResourceValidators {
            etag: Some(etag.into()),
            etag_is_weak: false,
            last_modified: None,
            total_size: Some(1000),
        }
    }

    fn sample_cp(etag: &str, ranges: &[ByteRange]) -> Checkpoint {
        let mut cp = Checkpoint::new("job", "https://example/f", "tmp");
        cp.validators = validators_for(etag);
        for &(s, e) in ranges {
            cp.record_completed(s, e);
        }
        cp
    }

    /// Scripted in-memory store for admission tests: deterministic
    /// load/delete outcomes with observed call order (no filesystem).
    struct FakeStore {
        load: Result<Option<Checkpoint>, CheckpointError>,
        delete: Result<(), CheckpointError>,
        loads: std::sync::atomic::AtomicU32,
        deletes: std::sync::Mutex<Vec<String>>,
    }

    impl FakeStore {
        fn absent() -> Self {
            Self {
                load: Ok(None),
                delete: Ok(()),
                loads: std::sync::atomic::AtomicU32::new(0),
                deletes: std::sync::Mutex::new(vec![]),
            }
        }

        fn holding(cp: Checkpoint) -> Self {
            Self {
                load: Ok(Some(cp)),
                delete: Ok(()),
                loads: std::sync::atomic::AtomicU32::new(0),
                deletes: std::sync::Mutex::new(vec![]),
            }
        }

        fn corrupt() -> Self {
            Self {
                load: Err(CheckpointError::Corrupt("bad json".into())),
                delete: Ok(()),
                loads: std::sync::atomic::AtomicU32::new(0),
                deletes: std::sync::Mutex::new(vec![]),
            }
        }

        fn corrupt_and_delete_fails() -> Self {
            Self {
                load: Err(CheckpointError::Corrupt("bad json".into())),
                delete: Err(CheckpointError::Corrupt("disk error".into())),
                loads: std::sync::atomic::AtomicU32::new(0),
                deletes: std::sync::Mutex::new(vec![]),
            }
        }

        fn holding_but_delete_fails(cp: Checkpoint) -> Self {
            Self {
                load: Ok(Some(cp)),
                delete: Err(CheckpointError::Corrupt("disk error".into())),
                loads: std::sync::atomic::AtomicU32::new(0),
                deletes: std::sync::Mutex::new(vec![]),
            }
        }

        fn deleted_identities(&self) -> Vec<String> {
            self.deletes.lock().expect("deletes").clone()
        }
    }

    impl CheckpointStore for FakeStore {
        fn load(&self, _job_identity: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            self.loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.load.clone()
        }

        fn save_atomic(&self, _checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
            panic!("admission never saves checkpoints");
        }

        fn delete(&self, job_identity: &str) -> Result<(), CheckpointError> {
            self.deletes
                .lock()
                .expect("deletes")
                .push(job_identity.to_string());
            self.delete.clone()
        }
    }

    fn begin<'a>(
        resume: ResumePolicy,
        store: &'a FakeStore,
        temp: &Path,
    ) -> Result<PendingAdmission<'a>, ResumeFailure> {
        begin_admission(resume, "identity", temp.to_path_buf(), store)
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
    fn never_policy_skips_checkpoint_loading() {
        let store = FakeStore::holding(sample_cp("\"v\"", &[(0, 499)]));
        let pending = begin(ResumePolicy::Never, &store, Path::new("/x")).expect("pending");
        let AdmissionDecision::Proceed(plan) = pending.finalize(&validators_for("\"v\"")) else {
            panic!("resume-disabled must proceed fresh");
        };
        assert!(!plan.is_resuming());
        assert!(plan.validators().is_none());
        assert!(plan.warnings().is_empty());
        assert_eq!(plan.sequential().offset, 0);
        assert_eq!(plan.sequential().reused_bytes, 0);
        assert_eq!(
            store.loads.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "Never never loads"
        );
        assert!(store.deleted_identities().is_empty(), "Never never deletes");
    }

    #[test]
    fn allowed_absent_proceeds_fresh() {
        let store = FakeStore::absent();
        let pending = begin(ResumePolicy::Allowed, &store, Path::new("/x")).expect("pending");
        let AdmissionDecision::Proceed(plan) = pending.finalize(&validators_for("\"v\"")) else {
            panic!("absent checkpoint proceeds fresh");
        };
        assert!(!plan.is_resuming());
        assert!(plan.warnings().is_empty());
        assert_eq!(store.loads.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(store.deleted_identities().is_empty());
    }

    #[test]
    fn required_absent_rejects_before_probe() {
        let store = FakeStore::absent();
        let failure = begin(ResumePolicy::Required, &store, Path::new("/x"))
            .expect_err("required without checkpoint must reject");
        assert!(matches!(failure.error, DownloadError::Checkpoint(_)));
        assert!(failure.notices.is_empty());
        assert_eq!(store.loads.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(store.deleted_identities().is_empty());
    }

    #[test]
    fn required_corrupt_rejects_with_load_error() {
        let store = FakeStore::corrupt();
        let failure = begin(ResumePolicy::Required, &store, Path::new("/x"))
            .expect_err("required with corrupt checkpoint must reject");
        assert!(matches!(failure.error, DownloadError::Checkpoint(_)));
        assert!(
            store.deleted_identities().is_empty(),
            "required keeps state for inspection"
        );
    }

    #[test]
    fn allowed_corrupt_deletes_and_restarts_with_warning() {
        let store = FakeStore::corrupt();
        let pending = begin(ResumePolicy::Allowed, &store, Path::new("/x")).expect("pending");
        assert_eq!(store.deleted_identities(), vec!["identity".to_string()]);
        let AdmissionDecision::Proceed(plan) = pending.finalize(&validators_for("\"v\"")) else {
            panic!("corrupt optional state restarts conservatively");
        };
        assert!(!plan.is_resuming());
        assert_eq!(
            plan.warnings(),
            &[
                "checkpoint unusable (checkpoint corrupt: bad json); restarting from zero"
                    .to_string()
            ]
        );
    }

    #[test]
    fn allowed_corrupt_deletion_failure_fails_closed() {
        let store = FakeStore::corrupt_and_delete_fails();
        let failure = begin(ResumePolicy::Allowed, &store, Path::new("/x"))
            .expect_err("unremovable corrupt state must fail closed");
        assert!(matches!(failure.error, DownloadError::Checkpoint(_)));
        assert_eq!(store.deleted_identities().len(), 1);
    }

    #[test]
    fn stale_generation_rejects_and_preserves_state() {
        let store = FakeStore::holding(sample_cp("\"v1\"", &[(0, 499)]));
        let pending = begin(ResumePolicy::Allowed, &store, Path::new("/x")).expect("pending");
        let AdmissionDecision::Reject(failure) = pending.finalize(&validators_for("\"v2\"")) else {
            panic!("generation mismatch must reject (§26)");
        };
        assert!(matches!(failure.error, DownloadError::ResourceChanged(_)));
        assert_eq!(
            failure.notices,
            vec![ResumeNotice::ResourceChanged {
                detail: "validators differ from checkpoint".into(),
            }]
        );
        assert!(
            store.deleted_identities().is_empty(),
            "fail policy preserves checkpoint and temp state"
        );
    }

    #[test]
    fn valid_resume_admits_ranges_and_prefix() {
        let store = FakeStore::holding(sample_cp("\"v\"", &[(0, 499)]));
        let dir = tempfile::tempdir().expect("tmp");
        let temp = dir.path().join("out.part");
        std::fs::write(&temp, vec![0u8; 500]).expect("temp");
        let pending = begin(ResumePolicy::Allowed, &store, &temp).expect("pending");
        let AdmissionDecision::Proceed(plan) = pending.finalize(&validators_for("\"v\"")) else {
            panic!("valid state must proceed");
        };
        assert!(plan.is_resuming());
        assert_eq!(
            plan.validators().and_then(|v| v.etag),
            Some("\"v\"".to_string())
        );
        // Segmented view: the whole admitted range is reusable.
        let seg = plan.segmented();
        assert_eq!(seg.ranges, &[(0, 499)]);
        assert_eq!(seg.reused_bytes, 500);
        // Sequential view: continue at the prefix end.
        let seq = plan.sequential();
        assert_eq!(seq.offset, 500);
        assert_eq!(seq.reused_bytes, 500);
    }

    #[test]
    fn missing_temp_restarts_and_deletes_checkpoint() {
        let store = FakeStore::holding(sample_cp("\"v\"", &[(0, 499)]));
        let pending = begin(
            ResumePolicy::Allowed,
            &store,
            Path::new("/nonexistent/x.part"),
        )
        .expect("pending");
        let AdmissionDecision::Proceed(plan) = pending.finalize(&validators_for("\"v\"")) else {
            panic!("missing temp restarts conservatively");
        };
        assert!(!plan.is_resuming());
        assert!(plan.validators().is_none());
        assert_eq!(store.deleted_identities(), vec!["identity".to_string()]);
        let warning = plan.warnings().first().expect("warning");
        assert!(warning.starts_with("checkpoint unusable ("), "{warning}");
        assert!(warning.ends_with("restarting from zero"), "{warning}");
    }

    #[test]
    fn short_temp_restarts_and_deletes_checkpoint() {
        let store = FakeStore::holding(sample_cp("\"v\"", &[(0, 499)]));
        let dir = tempfile::tempdir().expect("tmp");
        let temp = dir.path().join("out.part");
        std::fs::write(&temp, vec![0u8; 100]).expect("short temp");
        let pending = begin(ResumePolicy::Allowed, &store, &temp).expect("pending");
        let AdmissionDecision::Proceed(plan) = pending.finalize(&validators_for("\"v\"")) else {
            panic!("short temp restarts conservatively");
        };
        assert!(!plan.is_resuming());
        assert_eq!(store.deleted_identities(), vec!["identity".to_string()]);
    }

    #[test]
    fn restart_deletion_failure_fails_closed() {
        let store = FakeStore::holding_but_delete_fails(sample_cp("\"v\"", &[(0, 499)]));
        let pending = begin(
            ResumePolicy::Allowed,
            &store,
            Path::new("/nonexistent/x.part"),
        )
        .expect("pending");
        let AdmissionDecision::Reject(failure) = pending.finalize(&validators_for("\"v\"")) else {
            panic!("unremovable stale state must fail closed");
        };
        assert!(matches!(failure.error, DownloadError::Checkpoint(_)));
        assert_eq!(store.deleted_identities().len(), 1);
    }

    #[test]
    fn disjoint_ranges_reuse_only_prefix_for_sequential() {
        let plan = ResumePlan::resuming(sample_cp("\"v\"", &[(0, 99), (400, 499)]));
        let seq = plan.sequential();
        assert_eq!(seq.offset, 100, "sequential continues at the prefix end");
        assert_eq!(seq.reused_bytes, 100, "holes are rewritten, not reused");
        let seg = plan.segmented();
        assert_eq!(seg.ranges, &[(0, 99), (400, 499)]);
        assert_eq!(
            seg.reused_bytes, 200,
            "segmented reuses every admitted range"
        );
    }

    #[test]
    fn empty_admitted_checkpoint_reuses_nothing() {
        let plan = ResumePlan::resuming(sample_cp("\"v\"", &[]));
        assert!(!plan.is_resuming());
        assert_eq!(plan.sequential().offset, 0);
        assert_eq!(plan.sequential().reused_bytes, 0);
        assert_eq!(plan.segmented().reused_bytes, 0);
    }

    #[test]
    fn fully_complete_checkpoint_resumes_at_total() {
        let plan = ResumePlan::resuming(sample_cp("\"v\"", &[(0, 999)]));
        assert!(plan.is_resuming());
        assert_eq!(plan.sequential().offset, 1000);
        assert_eq!(plan.sequential().reused_bytes, 1000);
        assert_eq!(plan.segmented().reused_bytes, 1000);
    }
}
