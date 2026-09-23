//! Shared scripted checkpoint-store test support (§34, design §7):
//! an in-memory, failure-scripted adapter implementing the public
//! [`CheckpointStore`] trait with an ordered operation log, queued
//! load/save/delete failure injection, and deterministic concurrency
//! gates (hold an operation inside the adapter until released; overlap
//! detection via an active counter — no wall-clock races).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use kdown_engine::resume::checkpoint::{ByteRange, Checkpoint, CheckpointError};
use kdown_engine::resume::checkpoint_store::CheckpointStore;

/// One recorded adapter operation (entry recorded when the operation
/// enters the adapter; `ok` is the outcome it will report).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptedOp {
    Load {
        identity: String,
        outcome_ok: bool,
    },
    Save {
        job_id: String,
        ranges: Vec<ByteRange>,
        outcome_ok: bool,
    },
    Delete {
        identity: String,
        outcome_ok: bool,
    },
}

impl ScriptedOp {
    #[must_use]
    pub fn is_save(&self) -> bool {
        matches!(self, ScriptedOp::Save { .. })
    }

    #[must_use]
    pub fn is_delete(&self) -> bool {
        matches!(self, ScriptedOp::Delete { .. })
    }

    #[must_use]
    pub fn is_load(&self) -> bool {
        matches!(self, ScriptedOp::Load { .. })
    }

    #[must_use]
    pub fn outcome_ok(&self) -> bool {
        match self {
            ScriptedOp::Load { outcome_ok, .. }
            | ScriptedOp::Save { outcome_ok, .. }
            | ScriptedOp::Delete { outcome_ok, .. } => *outcome_ok,
        }
    }

    #[must_use]
    pub fn save_ranges(&self) -> Option<&[ByteRange]> {
        match self {
            ScriptedOp::Save { ranges, .. } => Some(ranges),
            _ => None,
        }
    }
}

/// A deterministic gate: release unblocks the held adapter operation.
#[derive(Clone)]
pub struct OpGate {
    shared: Arc<(Mutex<bool>, Condvar)>,
}

impl OpGate {
    /// Release the held operation.
    pub fn release(&self) {
        let (done, cv) = &*self.shared;
        *done.lock().expect("gate flag") = true;
        cv.notify_all();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OpKind {
    Load,
    Save,
    Delete,
}

struct HoldSpec {
    kind: OpKind,
    occurrence: u64,
    gate: Arc<(Mutex<bool>, Condvar)>,
    consumed: AtomicBool,
}

#[derive(Default)]
struct Inner {
    checkpoints: Mutex<HashMap<String, Checkpoint>>,
    ops: Mutex<Vec<ScriptedOp>>,
    load_failures: Mutex<VecDeque<CheckpointError>>,
    save_failures: Mutex<VecDeque<CheckpointError>>,
    delete_failures: Mutex<VecDeque<CheckpointError>>,
    /// Operations currently inside the adapter (overlap detector).
    active: AtomicU64,
    /// Entries that observed another operation already active.
    overlaps: AtomicU64,
    /// Per-kind 1-based occurrence counters (for hold matching).
    load_occurrence: AtomicU64,
    save_occurrence: AtomicU64,
    delete_occurrence: AtomicU64,
    holds: Mutex<Vec<HoldSpec>>,
}

/// In-memory scripted checkpoint adapter for orchestration tests: inject
/// failures, record exact operation order, and gate operations
/// deterministically.
#[derive(Clone, Default)]
pub struct ScriptedCheckpointStore {
    inner: Arc<Inner>,
}

impl ScriptedCheckpointStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue one failure for the next load (FIFO; later loads succeed).
    pub fn fail_next_load(&self, err: CheckpointError) {
        self.inner
            .load_failures
            .lock()
            .expect("load q")
            .push_back(err);
    }

    /// Queue one failure for the next save (FIFO; later saves succeed).
    pub fn fail_next_save(&self, err: CheckpointError) {
        self.inner
            .save_failures
            .lock()
            .expect("save q")
            .push_back(err);
    }

    /// Queue one failure for the next delete (FIFO; later deletes succeed).
    pub fn fail_next_delete(&self, err: CheckpointError) {
        self.inner
            .delete_failures
            .lock()
            .expect("delete q")
            .push_back(err);
    }

    /// Arm a gate: the `occurrence`-th load (1-based) blocks inside the
    /// adapter after recording its log entry, until the gate is released.
    pub fn hold_load(&self, occurrence: u64) -> OpGate {
        self.hold(OpKind::Load, occurrence)
    }

    /// Arm a gate: the `occurrence`-th save (1-based) blocks inside the
    /// adapter after recording its log entry, until the gate is released.
    pub fn hold_save(&self, occurrence: u64) -> OpGate {
        self.hold(OpKind::Save, occurrence)
    }

    /// Arm a gate: the `occurrence`-th delete (1-based) blocks inside the
    /// adapter after recording its log entry, until the gate is released.
    pub fn hold_delete(&self, occurrence: u64) -> OpGate {
        self.hold(OpKind::Delete, occurrence)
    }

    fn hold(&self, kind: OpKind, occurrence: u64) -> OpGate {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        self.inner.holds.lock().expect("holds").push(HoldSpec {
            kind,
            occurrence,
            gate: gate.clone(),
            consumed: AtomicBool::new(false),
        });
        OpGate { shared: gate }
    }

    /// The recorded operations, in the order they entered the adapter.
    #[must_use]
    pub fn ops(&self) -> Vec<ScriptedOp> {
        self.inner.ops.lock().expect("ops").clone()
    }

    /// Number of operations that entered while another was active.
    #[must_use]
    pub fn overlaps(&self) -> u64 {
        self.inner.overlaps.load(Ordering::SeqCst)
    }

    /// Operations currently inside the adapter.
    #[must_use]
    pub fn active_ops(&self) -> u64 {
        self.inner.active.load(Ordering::SeqCst)
    }

    /// The in-memory checkpoint for `identity`, when one was saved.
    #[must_use]
    pub fn stored(&self, identity: &str) -> Option<Checkpoint> {
        self.inner
            .checkpoints
            .lock()
            .expect("checkpoints")
            .get(identity)
            .cloned()
    }

    /// Number of recorded operations of each kind.
    #[must_use]
    pub fn counts(&self) -> (usize, usize, usize) {
        let ops = self.ops();
        (
            ops.iter().filter(|o| o.is_load()).count(),
            ops.iter().filter(|o| o.is_save()).count(),
            ops.iter().filter(|o| o.is_delete()).count(),
        )
    }
}

impl std::fmt::Debug for ScriptedCheckpointStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptedCheckpointStore")
            .field("ops", &self.ops().len())
            .finish_non_exhaustive()
    }
}

fn pop_failure(queue: &Mutex<VecDeque<CheckpointError>>) -> Option<CheckpointError> {
    queue.lock().expect("failure queue").pop_front()
}

fn take_hold(holds: &Mutex<Vec<HoldSpec>>, kind: OpKind, occurrence: u64) -> Option<OpGate> {
    let holds = holds.lock().expect("holds");
    let idx = holds.iter().position(|h| {
        h.kind == kind && h.occurrence == occurrence && !h.consumed.load(Ordering::SeqCst)
    })?;
    holds[idx].consumed.store(true, Ordering::SeqCst);
    Some(OpGate {
        shared: holds[idx].gate.clone(),
    })
}

fn enter_active(inner: &Inner) {
    let prev = inner.active.fetch_add(1, Ordering::SeqCst);
    if prev > 0 {
        inner.overlaps.fetch_add(1, Ordering::SeqCst);
    }
}

fn exit_active(inner: &Inner) {
    inner.active.fetch_sub(1, Ordering::SeqCst);
}

fn wait_gate(gate: &OpGate) {
    let (done, cv) = &*gate.shared;
    let mut d = done.lock().expect("gate flag");
    while !*d {
        d = cv.wait(d).expect("gate cv");
    }
}

impl CheckpointStore for ScriptedCheckpointStore {
    fn load(&self, job_identity: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let inner = &self.inner;
        let n = inner.load_occurrence.fetch_add(1, Ordering::SeqCst) + 1;
        let hold = take_hold(&inner.holds, OpKind::Load, n);
        let failure = pop_failure(&inner.load_failures);
        let outcome_ok = failure.is_none();
        enter_active(inner);
        inner.ops.lock().expect("ops").push(ScriptedOp::Load {
            identity: job_identity.to_string(),
            outcome_ok,
        });
        if let Some(gate) = &hold {
            wait_gate(gate);
        }
        let result = match failure {
            Some(err) => Err(err),
            None => {
                let stored = inner.checkpoints.lock().expect("checkpoints");
                Ok(stored.get(job_identity).cloned())
            }
        };
        exit_active(inner);
        result
    }

    fn save_atomic(&self, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        let inner = &self.inner;
        let n = inner.save_occurrence.fetch_add(1, Ordering::SeqCst) + 1;
        let hold = take_hold(&inner.holds, OpKind::Save, n);
        let failure = pop_failure(&inner.save_failures);
        let outcome_ok = failure.is_none();
        enter_active(inner);
        inner.ops.lock().expect("ops").push(ScriptedOp::Save {
            job_id: checkpoint.job_id.clone(),
            ranges: checkpoint.completed_ranges.clone(),
            outcome_ok,
        });
        if let Some(gate) = &hold {
            wait_gate(gate);
        }
        let result = match failure {
            Some(err) => Err(err),
            None => {
                inner
                    .checkpoints
                    .lock()
                    .expect("checkpoints")
                    .insert(checkpoint.job_id.clone(), checkpoint.clone());
                Ok(())
            }
        };
        exit_active(inner);
        result
    }

    fn delete(&self, job_identity: &str) -> Result<(), CheckpointError> {
        let inner = &self.inner;
        let n = inner.delete_occurrence.fetch_add(1, Ordering::SeqCst) + 1;
        let hold = take_hold(&inner.holds, OpKind::Delete, n);
        let failure = pop_failure(&inner.delete_failures);
        let outcome_ok = failure.is_none();
        enter_active(inner);
        inner.ops.lock().expect("ops").push(ScriptedOp::Delete {
            identity: job_identity.to_string(),
            outcome_ok,
        });
        if let Some(gate) = &hold {
            wait_gate(gate);
        }
        let result = match failure {
            Some(err) => Err(err),
            None => {
                inner
                    .checkpoints
                    .lock()
                    .expect("checkpoints")
                    .remove(job_identity);
                Ok(())
            }
        };
        exit_active(inner);
        result
    }
}
