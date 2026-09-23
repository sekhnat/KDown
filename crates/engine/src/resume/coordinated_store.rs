//! Per-job checkpoint mutation coordination (§12, design §2).
//!
//! Decorates the one resolver-selected adapter with a per-job mutation
//! lock: at most one checkpoint operation is active at a time, later saves
//! never publish progress regression within the same resource generation,
//! and successful deletion clears remembered state so a fresh admission can
//! create a clean checkpoint afterwards.
//!
//! The inner mutex is short-lived and guards one synchronous store call
//! plus mutation bookkeeping; store methods are synchronous, so the lock is
//! never held across an `.await` point. Orchestration invokes terminal
//! deletion only after sequential activity stopped or segmented workers
//! joined, so no save can be issued after cleanup.

use std::collections::HashMap;
use std::sync::Arc;

use crate::resume::checkpoint::{ByteRange, Checkpoint, CheckpointError};
use crate::resume::checkpoint_store::CheckpointStore;

/// Normalized union of two completed-range sets (sorted, adjacent/overlapping
/// merged — §12.1 normalized-range invariants).
fn union_ranges(a: &[ByteRange], b: &[ByteRange]) -> Vec<ByteRange> {
    let mut all: Vec<ByteRange> = a.iter().copied().chain(b.iter().copied()).collect();
    all.sort_unstable();
    let mut merged: Vec<ByteRange> = Vec::with_capacity(all.len());
    for (start, end) in all {
        match merged.last_mut() {
            Some((_, pe)) if start <= pe.saturating_add(1) => {
                *pe = (*pe).max(end);
            }
            _ => merged.push((start, end)),
        }
    }
    merged
}

/// Per-identity coordination state.
#[derive(Default)]
struct JobCoordination {
    /// Last accepted checkpoint snapshot for this identity, when one has
    /// been saved and not deleted since.
    last_accepted: Option<Checkpoint>,
}

/// Crate-private decorator over the job's selected adapter: serializes
/// `load`/`save_atomic`/`delete` and keeps saves monotonic within one
/// resource generation. Neither job orchestration module names this type;
/// they consume the plain [`CheckpointStore`] interface.
pub(crate) struct CoordinatedCheckpointStore {
    inner: Arc<dyn CheckpointStore>,
    state: std::sync::Mutex<HashMap<String, JobCoordination>>,
}

impl std::fmt::Debug for CoordinatedCheckpointStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatedCheckpointStore")
            .finish_non_exhaustive()
    }
}

impl CoordinatedCheckpointStore {
    pub(crate) fn new(inner: Arc<dyn CheckpointStore>) -> Self {
        Self {
            inner,
            state: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<String, JobCoordination>>, CheckpointError> {
        self.state.lock().map_err(|_| {
            CheckpointError::Inconsistent("checkpoint coordination lock poisoned".into())
        })
    }
}

impl CheckpointStore for CoordinatedCheckpointStore {
    fn load(&self, job_identity: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let _state = self.lock_state()?;
        self.inner.load(job_identity)
    }

    fn save_atomic(&self, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        let mut state = self.lock_state()?;
        let job = state.entry(checkpoint.job_id.clone()).or_default();
        if let Some(prev) = &job.last_accepted {
            // Same-job lineage: a snapshot from a different resource
            // generation is an inconsistency, never mixed state (§26).
            if prev.validators != checkpoint.validators || prev.total_size != checkpoint.total_size
            {
                return Err(CheckpointError::Inconsistent(format!(
                    "checkpoint generation changed mid-job for {}",
                    checkpoint.job_id
                )));
            }
            // Monotonic ranges: fold forward recorded progress so a stale
            // snapshot can never overwrite newer state.
            let merged = union_ranges(&prev.completed_ranges, &checkpoint.completed_ranges);
            if merged == prev.completed_ranges {
                // Strictly superseded (or identical) snapshot: suppress the
                // redundant adapter write.
                return Ok(());
            }
            let mut forwarded = checkpoint.clone();
            forwarded.completed_ranges = merged;
            self.inner.save_atomic(&forwarded)?;
            job.last_accepted = Some(forwarded);
        } else {
            self.inner.save_atomic(checkpoint)?;
            job.last_accepted = Some(checkpoint.clone());
        }
        Ok(())
    }

    fn delete(&self, job_identity: &str) -> Result<(), CheckpointError> {
        let mut state = self.lock_state()?;
        // The adapter delete happens under the same lock as saves: a
        // pending save completes (or is suppressed) before deletion starts.
        self.inner.delete(job_identity)?;
        // Successful deletion clears remembered state so admission may
        // remove unusable state and a later job can create a fresh
        // checkpoint without a false lineage conflict.
        if let Some(job) = state.get_mut(job_identity) {
            job.last_accepted = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::validators::ResourceValidators;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Operation kinds recorded by the scripted inner store.
    #[derive(Debug, Clone, PartialEq)]
    enum Op {
        Load(String),
        Save(String, Vec<ByteRange>),
        Delete(String),
    }

    /// Gate handle: a release flag plus its condition variable.
    type GateFlag = Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>;

    /// Deterministic scripted store for coordination tests: records the
    /// exact operation order, detects overlapping operations via an
    /// active-counter (no wall-clock races), and can hold one save until a
    /// gate is released.
    struct ScriptedStore {
        ops: std::sync::Mutex<Vec<Op>>,
        active: AtomicUsize,
        overlaps: AtomicUsize,
        /// When set, `save_atomic` blocks until the gate is released
        /// (after recording entry).
        save_gate: std::sync::Mutex<Option<GateFlag>>,
        save_entered: std::sync::atomic::AtomicBool,
        load_result: std::sync::Mutex<Result<Option<Checkpoint>, CheckpointError>>,
        delete_result: std::sync::Mutex<Result<(), CheckpointError>>,
    }

    impl ScriptedStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                ops: std::sync::Mutex::new(vec![]),
                active: AtomicUsize::new(0),
                overlaps: AtomicUsize::new(0),
                save_gate: std::sync::Mutex::new(None),
                save_entered: std::sync::atomic::AtomicBool::new(false),
                load_result: std::sync::Mutex::new(Ok(None)),
                delete_result: std::sync::Mutex::new(Ok(())),
            })
        }

        fn ops(&self) -> Vec<Op> {
            self.ops.lock().expect("ops").clone()
        }

        fn overlaps(&self) -> usize {
            self.overlaps.load(Ordering::SeqCst)
        }

        fn set_save_gate(&self, gate: GateFlag) {
            *self.save_gate.lock().expect("gate") = Some(gate);
        }
    }

    impl CheckpointStore for ScriptedStore {
        fn load(&self, job_identity: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            let n = self.active.fetch_add(1, Ordering::SeqCst);
            if n > 0 {
                self.overlaps.fetch_add(1, Ordering::SeqCst);
            }
            self.ops
                .lock()
                .expect("ops")
                .push(Op::Load(job_identity.to_string()));
            let r = self.load_result.lock().expect("load").clone();
            self.active.fetch_sub(1, Ordering::SeqCst);
            r
        }

        fn save_atomic(&self, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
            let n = self.active.fetch_add(1, Ordering::SeqCst);
            if n > 0 {
                self.overlaps.fetch_add(1, Ordering::SeqCst);
            }
            self.ops.lock().expect("ops").push(Op::Save(
                checkpoint.job_id.clone(),
                checkpoint.completed_ranges.clone(),
            ));
            self.save_entered.store(true, Ordering::SeqCst);
            // Hold inside the critical section when a gate is armed.
            let gate = self.save_gate.lock().expect("gate").clone();
            if let Some(gate) = gate {
                let (done, cv) = &*gate;
                let mut d = done.lock().expect("gate flag");
                while !*d {
                    d = cv.wait(d).expect("gate cv");
                }
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }

        fn delete(&self, job_identity: &str) -> Result<(), CheckpointError> {
            let n = self.active.fetch_add(1, Ordering::SeqCst);
            if n > 0 {
                self.overlaps.fetch_add(1, Ordering::SeqCst);
            }
            self.ops
                .lock()
                .expect("ops")
                .push(Op::Delete(job_identity.to_string()));
            let r = self.delete_result.lock().expect("delete").clone();
            self.active.fetch_sub(1, Ordering::SeqCst);
            r
        }
    }

    fn validators_for(etag: &str) -> ResourceValidators {
        ResourceValidators {
            etag: Some(etag.into()),
            etag_is_weak: false,
            last_modified: None,
            total_size: Some(1000),
        }
    }

    fn cp(job: &str, etag: &str, ranges: &[ByteRange]) -> Checkpoint {
        let mut cp = Checkpoint::new(job, "https://example/f", "tmp-1");
        cp.validators = validators_for(etag);
        cp.total_size = Some(1000);
        for &(s, e) in ranges {
            cp.record_completed(s, e);
        }
        cp
    }

    #[test]
    fn concurrent_saves_never_overlap_and_both_progress() {
        // Two workers saving disjoint progress concurrently observe a
        // serialized adapter: no overlap, both snapshots forwarded.
        let inner = ScriptedStore::new();
        let store = Arc::new(CoordinatedCheckpointStore::new(inner.clone()));
        let a = cp("job", "\"v\"", &[(0, 99)]);
        let b = cp("job", "\"v\"", &[(100, 199)]);
        let s1 = store.clone();
        let s2 = store.clone();
        let (h1, h2) = (
            std::thread::spawn(move || s1.save_atomic(&a)),
            std::thread::spawn(move || s2.save_atomic(&b)),
        );
        h1.join().expect("join save 1").expect("save 1 ok");
        h2.join().expect("join save 2").expect("save 2 ok");
        assert_eq!(inner.overlaps(), 0, "mutations never overlap");
        let ops = inner.ops();
        assert_eq!(ops.len(), 2, "both disjoint snapshots forwarded: {ops:?}");
        // Ranges never regress: the final adapter state is the union.
        let loaded = store.load("job").expect("load");
        assert!(loaded.is_none(), "scripted load has no stored state");
    }

    #[test]
    fn stale_snapshot_is_suppressed() {
        // A later call with an older snapshot must not overwrite newer
        // recorded progress: the stale write never reaches the adapter.
        let inner = ScriptedStore::new();
        let store = CoordinatedCheckpointStore::new(inner.clone());
        store
            .save_atomic(&cp("job", "\"v\"", &[(0, 99)]))
            .expect("save newer");
        store
            .save_atomic(&cp("job", "\"v\"", &[(0, 49)]))
            .expect("stale snapshot accepted without regression");
        let ops = inner.ops();
        assert_eq!(
            ops,
            vec![Op::Save("job".into(), vec![(0, 99)])],
            "stale snapshot suppressed, newer state retained"
        );
    }

    #[test]
    fn fold_forward_merges_disjoint_and_overlapping_ranges() {
        let inner = ScriptedStore::new();
        let store = CoordinatedCheckpointStore::new(inner.clone());
        store
            .save_atomic(&cp("job", "\"v\"", &[(0, 99)]))
            .expect("save 1");
        store
            .save_atomic(&cp("job", "\"v\"", &[(50, 150)]))
            .expect("save 2 overlapping");
        store
            .save_atomic(&cp("job", "\"v\"", &[(0, 49)]))
            .expect("save 3 subsumed");
        store
            .save_atomic(&cp("job", "\"v\"", &[(200, 299)]))
            .expect("save 4 disjoint");
        let ops = inner.ops();
        assert_eq!(ops.len(), 3, "subsumed snapshot suppressed, rest forwarded");
        let Op::Save(_, overlapping) = &ops[1] else {
            panic!("expected save: {ops:?}");
        };
        assert_eq!(
            overlapping,
            &vec![(0, 150)],
            "fold-forward merges overlapping progress"
        );
        let Op::Save(_, disjoint) = &ops[2] else {
            panic!("expected save: {ops:?}");
        };
        assert_eq!(
            disjoint,
            &vec![(0, 150), (200, 299)],
            "normalized union across disjoint workers"
        );
    }

    #[test]
    fn generation_change_is_rejected_not_mixed() {
        let inner = ScriptedStore::new();
        let store = CoordinatedCheckpointStore::new(inner.clone());
        store
            .save_atomic(&cp("job", "\"v1\"", &[(0, 99)]))
            .expect("save generation 1");
        let conflicting = store.save_atomic(&cp("job", "\"v2\"", &[(0, 99)]));
        assert!(
            matches!(conflicting, Err(CheckpointError::Inconsistent(_))),
            "lineage change must be an inconsistency: {conflicting:?}"
        );
        // A total-size change is equally incompatible.
        let mut resized = cp("job", "\"v1\"", &[(0, 99)]);
        resized.total_size = Some(2000);
        let conflicting = store.save_atomic(&resized);
        assert!(
            matches!(conflicting, Err(CheckpointError::Inconsistent(_))),
            "total-size change must be an inconsistency: {conflicting:?}"
        );
        // Neither conflicting snapshot reached the adapter.
        assert_eq!(inner.ops().len(), 1, "conflicting saves never forwarded");
    }

    #[test]
    fn successful_delete_clears_lineage_memory() {
        let inner = ScriptedStore::new();
        let store = CoordinatedCheckpointStore::new(inner.clone());
        store
            .save_atomic(&cp("job", "\"v1\"", &[(0, 99)]))
            .expect("save");
        store.delete("job").expect("delete");
        // After deletion a fresh admission may create a new generation.
        store
            .save_atomic(&cp("job", "\"v2\"", &[(0, 49)]))
            .expect("fresh checkpoint after delete");
        let ops = inner.ops();
        assert_eq!(ops.len(), 3, "save, delete, fresh save all forwarded");
        assert!(matches!(ops[1], Op::Delete(_)), "{ops:?}");
        // Deleting a missing file stays successful.
        store.delete("absent").expect("missing delete ok");
    }

    #[test]
    fn delete_waits_for_pending_save() {
        // Save-before-delete ordering: the delete cannot start inside the
        // adapter while a save is still active; coordination settles the
        // order deterministically via the gate (no wall-clock race).
        let inner = ScriptedStore::new();
        let store = Arc::new(CoordinatedCheckpointStore::new(inner.clone()));
        let gate: GateFlag = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        inner.set_save_gate(gate.clone());
        let saver = {
            let store = store.clone();
            let cp = cp("job", "\"v\"", &[(0, 99)]);
            std::thread::spawn(move || store.save_atomic(&cp))
        };
        // Wait until the save is inside the adapter's critical section.
        while !inner.save_entered.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        let deleter = {
            let store = store.clone();
            std::thread::spawn(move || store.delete("job"))
        };
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert_eq!(
            inner.ops().len(),
            1,
            "delete must not enter the adapter while the save is active"
        );
        *gate.0.lock().expect("gate flag") = true;
        gate.1.notify_all();
        saver.join().expect("join save").expect("save ok");
        deleter.join().expect("join delete").expect("delete ok");
        assert_eq!(inner.overlaps(), 0, "save/delete never overlap");
        let ops = inner.ops();
        assert_eq!(ops.len(), 2);
        assert!(matches!(ops[0], Op::Save(_, _)), "{ops:?}");
        assert!(matches!(ops[1], Op::Delete(_)), "{ops:?}");
    }
}
