//! Checkpoint persistence: trait + atomic sidecar store (§15.3, §34, D3).

use crate::resume::checkpoint::{Checkpoint, CheckpointError};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Durability level for checkpoint persistence (§15.4, D4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DurabilityMode {
    /// Checkpoint represents page-cache-acknowledged writes (§15.4
    /// performance mode). After power loss some recorded bytes may need
    /// redownload.
    #[default]
    Performance,
    /// Data flushed before completed intervals are recorded; the
    /// checkpoint fsyncs before rename.
    Durable,
}

/// Storage abstraction (§34): pluggable (sidecar, SQLite, app state).
pub trait CheckpointStore: Send + Sync {
    /// Load a checkpoint; `Ok(None)` when absent.
    ///
    /// # Errors
    /// [`CheckpointError`] when present but unreadable/corrupt.
    fn load(&self, job_identity: &str) -> Result<Option<Checkpoint>, CheckpointError>;

    /// Atomically replace the checkpoint (§15.3: write-temp → fsync →
    /// rename → optional dir fsync).
    ///
    /// # Errors
    /// I/O failures map to [`CheckpointError`].
    fn save_atomic(&self, checkpoint: &Checkpoint) -> Result<(), CheckpointError>;

    /// Remove persisted state (§14.6 step 5).
    ///
    /// # Errors
    /// I/O failure; missing file is not an error.
    fn delete(&self, job_identity: &str) -> Result<(), CheckpointError>;
}

/// What a resolver needs to select one checkpoint adapter for a job (§34).
///
/// Redaction-safe by construction: the identity is a secret-free hash token
/// and the destination is an already-public filesystem path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointResolveContext {
    /// Stable, secret-free checkpoint key for the job (§15.2 job_id).
    pub job_identity: String,
    /// Final local destination the job commits to; adapters that are
    /// directory-scoped derive their storage location from this.
    pub destination: PathBuf,
    /// Configured checkpoint durability (§15.4): adapters preserve the
    /// caller's guarantee when persisting.
    pub durability: DurabilityMode,
}

impl CheckpointResolveContext {
    #[must_use]
    pub fn new(
        job_identity: impl Into<String>,
        destination: PathBuf,
        durability: DurabilityMode,
    ) -> Self {
        Self {
            job_identity: job_identity.into(),
            destination,
            durability,
        }
    }

    /// The destination's containing directory (§34 default sidecar scope);
    /// `.` when the destination has no parent component.
    #[must_use]
    pub fn destination_parent(&self) -> PathBuf {
        match self.destination.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        }
    }
}

/// Adapter-selection port (§34): a thread-safe factory that chooses one
/// shareable [`CheckpointStore`] for a job from its resolve context.
///
/// Resolution happens once per job before any checkpoint operation; the
/// returned adapter is retained by that job for its whole lifecycle.
pub trait CheckpointStoreResolver: Send + Sync {
    /// Select the checkpoint adapter for one job.
    ///
    /// # Errors
    /// [`CheckpointError`] when no usable adapter can be provided; the job
    /// fails with a checkpoint-category error before probing.
    fn resolve(
        &self,
        context: &CheckpointResolveContext,
    ) -> Result<Arc<dyn CheckpointStore>, CheckpointError>;
}

/// Default resolver (§34): destination-relative file sidecar.
///
/// Places `<destination_parent>/<job_identity>.kdown` with the configured
/// durability, so one resolver serves controllers whose jobs have unrelated
/// destination parents.
#[derive(Debug, Clone, Copy, Default)]
pub struct SidecarCheckpointResolver;

impl CheckpointStoreResolver for SidecarCheckpointResolver {
    fn resolve(
        &self,
        context: &CheckpointResolveContext,
    ) -> Result<Arc<dyn CheckpointStore>, CheckpointError> {
        let dir = context.destination_parent();
        let store = FileCheckpointStore::new(&dir, context.durability)?;
        Ok(Arc::new(store))
    }
}

/// Sidecar-file store (§34 default): `<dir>/<job_identity>.kdown`.
#[derive(Debug, Clone)]
pub struct FileCheckpointStore {
    dir: PathBuf,
    durability: DurabilityMode,
}

impl FileCheckpointStore {
    /// Store checkpoints in `dir` with the given durability mode.
    ///
    /// # Errors
    /// Returns an error when `dir` cannot be created.
    pub fn new(dir: &Path, durability: DurabilityMode) -> Result<Self, CheckpointError> {
        std::fs::create_dir_all(dir)
            .map_err(|e| CheckpointError::Corrupt(format!("mkdir {}: {e}", dir.display())))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            durability,
        })
    }

    fn path_for(&self, job_identity: &str) -> PathBuf {
        // Identity is a filesystem-safe token generated by the engine.
        self.dir.join(format!("{job_identity}.kdown"))
    }

    /// Process-wide temp-file sequence: worker sharing means concurrent
    /// saves must never contend for one PID-only temp path (§12).
    fn next_temp_suffix() -> u64 {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        SEQ.fetch_add(1, Ordering::Relaxed)
    }
}

impl CheckpointStore for FileCheckpointStore {
    fn load(&self, job_identity: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let path = self.path_for(job_identity);
        match std::fs::read(&path) {
            Ok(bytes) => {
                let json = String::from_utf8(bytes)
                    .map_err(|e| CheckpointError::Corrupt(format!("checkpoint not utf-8: {e}")))?;
                Checkpoint::from_json(&json).map(Some)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CheckpointError::Corrupt(format!(
                "read {}: {e}",
                path.display()
            ))),
        }
    }

    fn save_atomic(&self, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        let json = checkpoint.to_json()?;
        let path = self.path_for(&checkpoint.job_id);
        // Collision-resistant temp name: PID alone would collide across
        // concurrent workers of one process; the sequence suffix makes
        // every in-flight write distinct.
        let tmp_path = self.dir.join(format!(
            "{}.{}.{}.tmp",
            checkpoint.job_id,
            std::process::id(),
            Self::next_temp_suffix()
        ));
        let result = (|| -> Result<(), CheckpointError> {
            {
                let mut f = std::fs::File::create(&tmp_path).map_err(|e| {
                    CheckpointError::Corrupt(format!("create {}: {e}", tmp_path.display()))
                })?;
                f.write_all(json.as_bytes())
                    .map_err(|e| CheckpointError::Corrupt(format!("write: {e}")))?;
                f.flush()
                    .map_err(|e| CheckpointError::Corrupt(format!("flush: {e}")))?;
                if self.durability == DurabilityMode::Durable {
                    f.sync_all()
                        .map_err(|e| CheckpointError::Corrupt(format!("fsync checkpoint: {e}")))?;
                }
            }
            std::fs::rename(&tmp_path, &path).map_err(|e| {
                CheckpointError::Corrupt(format!(
                    "rename {} -> {}: {e}",
                    tmp_path.display(),
                    path.display()
                ))
            })?;
            if self.durability == DurabilityMode::Durable {
                // Directory fsync so the rename itself survives power loss
                // (§15.3 step 4). Supported-platform failures are reported:
                // a save is never reported successful after an ignored
                // durability failure.
                if let Some(parent) = path.parent() {
                    sync_parent_durable(parent)?;
                }
            }
            Ok(())
        })();
        if result.is_err() {
            // Best-effort residue cleanup: a failed save must not leave a
            // temp file behind (harmless no-op once the rename succeeded).
            let _ = std::fs::remove_file(&tmp_path);
        }
        result
    }

    fn delete(&self, job_identity: &str) -> Result<(), CheckpointError> {
        let path = self.path_for(job_identity);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CheckpointError::Corrupt(format!("delete: {e}"))),
        }
    }
}

/// Durable parent-directory synchronization (§15.3 step 4): make the
/// rename itself survive a crash where the platform supports directory
/// synchronization. On unix a directory file descriptor can be opened and
/// fsynced, so failures are reported — a save is never reported successful
/// after an ignored durability failure. Platforms without directory-fd
/// support skip the step explicitly (the checkpoint file itself is still
/// fsynced before rename); arbitrary I/O errors are never silently
/// swallowed on supported paths.
pub(crate) fn sync_parent_durable(parent: &Path) -> Result<(), CheckpointError> {
    #[cfg(unix)]
    {
        let dir_file = std::fs::File::open(parent).map_err(|e| {
            CheckpointError::Corrupt(format!(
                "open checkpoint directory {}: {e}",
                parent.display()
            ))
        })?;
        dir_file.sync_all().map_err(|e| {
            CheckpointError::Corrupt(format!(
                "fsync checkpoint directory {}: {e}",
                parent.display()
            ))
        })
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::validators::ResourceValidators;

    fn sample(job: &str) -> Checkpoint {
        let mut cp = Checkpoint::new(job, "https://example/f", "tmp-1");
        cp.validators = ResourceValidators {
            etag: Some("\"v\"".into()),
            etag_is_weak: false,
            last_modified: None,
            total_size: Some(100),
        };
        cp.record_completed(0, 49);
        cp
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = tempfile::tempdir().expect("tmp");
        let store =
            FileCheckpointStore::new(dir.path(), DurabilityMode::Performance).expect("store");
        store.save_atomic(&sample("job-a")).expect("save");
        let loaded = store.load("job-a").expect("load");
        assert!(loaded.is_some());
        assert_eq!(loaded.expect("some").completed_ranges, vec![(0, 49)]);
    }

    #[test]
    fn missing_checkpoint_is_none_not_error() {
        let dir = tempfile::tempdir().expect("tmp");
        let store =
            FileCheckpointStore::new(dir.path(), DurabilityMode::Performance).expect("store");
        assert!(store.load("absent").expect("none").is_none());
    }

    #[test]
    fn atomic_replace_never_leaves_unreadable() {
        // Simulate the write-temp/rename cycle: an interrupted save leaves
        // either the old checkpoint or the new one, never garbage (§15.3).
        let dir = tempfile::tempdir().expect("tmp");
        let store =
            FileCheckpointStore::new(dir.path(), DurabilityMode::Performance).expect("store");
        store.save_atomic(&sample("job-b")).expect("save 1");
        let mut cp = sample("job-b");
        cp.record_completed(50, 99);
        store.save_atomic(&cp).expect("save 2");
        let loaded = store.load("job-b").expect("load");
        assert_eq!(loaded.expect("some").completed_ranges, vec![(0, 99)]);
        // No .tmp residue after a successful save.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "tmp files must be renamed away");
    }

    #[test]
    fn delete_removes_and_missing_ok() {
        let dir = tempfile::tempdir().expect("tmp");
        let store = FileCheckpointStore::new(dir.path(), DurabilityMode::Durable).expect("store");
        store.save_atomic(&sample("job-c")).expect("save");
        store.delete("job-b").expect("missing delete is ok");
        store.delete("job-a").expect("delete");
        assert!(store.load("job-a").expect("gone").is_none());
    }

    // ---- Resolver/context contract (§34) ----

    #[test]
    fn resolve_context_carries_values_and_parent_fallback() {
        let ctx = CheckpointResolveContext::new(
            "job-x",
            PathBuf::from("/srv/data/out.bin"),
            DurabilityMode::Durable,
        );
        assert_eq!(ctx.job_identity, "job-x");
        assert_eq!(ctx.destination, PathBuf::from("/srv/data/out.bin"));
        assert_eq!(ctx.durability, DurabilityMode::Durable);
        assert_eq!(ctx.destination_parent(), PathBuf::from("/srv/data"));
        // A bare filename has no parent component: the sidecar scope is `.`.
        let bare = CheckpointResolveContext::new(
            "job-y",
            PathBuf::from("out.bin"),
            DurabilityMode::Performance,
        );
        assert_eq!(bare.destination_parent(), PathBuf::from("."));
    }

    #[test]
    fn default_resolver_resolves_per_destination() {
        // One resolver serves distinct destination parents: each resolved
        // adapter persists beside its own destination (§34).
        let dir = tempfile::tempdir().expect("tmp");
        let parent_a = dir.path().join("a");
        let parent_b = dir.path().join("b");
        std::fs::create_dir_all(&parent_a).expect("mkdir a");
        std::fs::create_dir_all(&parent_b).expect("mkdir b");
        let resolver = SidecarCheckpointResolver;

        let dest_a = parent_a.join("out.bin");
        let dest_b = parent_b.join("out.bin");
        let store_a = resolver
            .resolve(&CheckpointResolveContext::new(
                "job-a",
                dest_a.clone(),
                DurabilityMode::Performance,
            ))
            .expect("resolve a");
        let store_b = resolver
            .resolve(&CheckpointResolveContext::new(
                "job-b",
                dest_b.clone(),
                DurabilityMode::Performance,
            ))
            .expect("resolve b");
        // The two contexts resolved independent adapters.
        assert_eq!(store_a.load("job-a").expect("load a"), None);
        assert_eq!(store_b.load("job-b").expect("load b"), None);

        let cp_a = sample("job-a");
        store_a.save_atomic(&cp_a).expect("save a");
        let cp_b = sample("job-b");
        store_b.save_atomic(&cp_b).expect("save b");
        // Sidecars landed beside each destination, not in a shared scope.
        assert!(parent_a.join("job-a.kdown").exists());
        assert!(parent_b.join("job-b.kdown").exists());
        assert!(!parent_a.join("job-b.kdown").exists());
        assert!(!parent_b.join("job-a.kdown").exists());
    }

    #[test]
    fn default_resolver_reports_unusable_location() {
        // A parent path occupied by a file cannot host the sidecar
        // directory: resolution fails with a checkpoint error instead of
        // silently mis-placing state.
        let dir = tempfile::tempdir().expect("tmp");
        let blocked = dir.path().join("occupied");
        std::fs::write(&blocked, b"not a directory").expect("file");
        let resolver = SidecarCheckpointResolver;
        let resolved = resolver.resolve(&CheckpointResolveContext::new(
            "job-z",
            blocked.join("out.bin"),
            DurabilityMode::Performance,
        ));
        let err = match resolved {
            Ok(_) => panic!("unusable location must fail resolution"),
            Err(e) => e,
        };
        assert!(matches!(err, CheckpointError::Corrupt(_)), "{err:?}");
    }

    #[test]
    fn concurrent_saves_never_collide_or_leave_residue() {
        // Worker sharing: concurrent saves through one adapter instance
        // must not contend for a single temp path, and every successful
        // save leaves a complete checkpoint (never torn) with no temp
        // residue (§15.3).
        let dir = tempfile::tempdir().expect("tmp");
        let store = Arc::new(
            FileCheckpointStore::new(dir.path(), DurabilityMode::Performance).expect("store"),
        );
        let cp_a = {
            let mut cp = sample("job-share");
            cp.record_completed(50, 99);
            cp
        };
        let cp_b = sample("job-share");
        let s1 = store.clone();
        let s2 = store.clone();
        let (h1, h2) = (
            std::thread::spawn(move || s1.save_atomic(&cp_a)),
            std::thread::spawn(move || s2.save_atomic(&cp_b)),
        );
        h1.join().expect("join a").expect("save a ok");
        h2.join().expect("join b").expect("save b ok");
        // Final state is one complete snapshot (last rename wins).
        let loaded = store.load("job-share").expect("load").expect("present");
        assert!(
            loaded.completed_ranges == vec![(0, 49)] || loaded.completed_ranges == vec![(0, 99)],
            "final checkpoint is a complete snapshot, never torn: {:?}",
            loaded.completed_ranges
        );
        // No temp residue after successful concurrent saves.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "temp files must be renamed away");
    }

    #[test]
    fn durable_save_roundtrips_with_directory_sync() {
        // Durable mode: file fsync before rename plus supported parent-
        // directory synchronization; a successful save round-trips and
        // leaves no residue.
        let dir = tempfile::tempdir().expect("tmp");
        let store = FileCheckpointStore::new(dir.path(), DurabilityMode::Durable).expect("store");
        store
            .save_atomic(&sample("job-durable"))
            .expect("durable save");
        let loaded = store.load("job-durable").expect("load").expect("present");
        assert_eq!(loaded.completed_ranges, vec![(0, 49)]);
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "no temp residue after durable save");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn durable_directory_sync_failures_are_reported() {
        // Supported-platform failure reporting: procfs directories open
        // but do not support fsync — the sync failure must surface as a
        // checkpoint error, never be swallowed (§15.3 step 4).
        let err = sync_parent_durable(Path::new("/proc/self"));
        assert!(
            matches!(err, Err(CheckpointError::Corrupt(_))),
            "procfs dir sync failure must be reported: {err:?}"
        );
        // A regular directory synchronizes successfully.
        let dir = tempfile::tempdir().expect("tmp");
        sync_parent_durable(dir.path()).expect("regular dir sync ok");
    }
}
