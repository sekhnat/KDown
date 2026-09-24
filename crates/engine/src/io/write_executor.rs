// The seam is defined and verified here; the segmented transfer paths
// consume it with the byte budgets (task 2.2), shared pool (task 2.3),
// ack frontiers (task 2.4) and H1/H2 integration (tasks 2.5-2.7).
#![allow(dead_code)]
//! Internal write-executor boundary (design D2, tasks 2.1-2.3).
//!
//! Decouples network workers from blocking filesystem work: a worker
//! submits a positional write (job, lease id/generation, absolute offset,
//! payload) and keeps receiving the next bounded payload while the write
//! executes on a blocking thread. Completion is reported back
//! asynchronously, so *acknowledgement* — not submission — defines
//! published progress (task 2.4).
//!
//! Every submitted write is executed through [`WriteBackend::write_blocking`],
//! i.e. the existing checked positional adapter (`write_all_at`: short
//! writes completed, interruption retried, zero progress rejected, checked
//! offset arithmetic) over one write-only [`OutputWriteHandle`] capability.
//! Output fault scripts therefore apply at this boundary exactly as they do
//! for the legacy writer lanes, and the platform cfg selection
//! (Unix `write_at` / Windows `seek_write`) is inherited unchanged.
//!
//! Execution core (task 2.3): a small fixed-size pool of blocking writer
//! threads is shared by every attached job — blocking filesystem threads
//! never scale with `jobs × workers`. Admission is fair per job: a shared
//! round-robin cursor pops from the next job's queue every dispatch, so
//! one job's heavy queue cannot starve another. Queued+executing payload
//! is bounded by an executor-wide byte cap, acquired before enqueueing
//! (cancellation-aware) and released after execution or discard.
//!
//! Contract honored by the executor:
//! - every write executes positionally at its absolute offset — concurrent
//!   writes never share or move a cursor;
//! - an accepted submission is executed at most once and yields exactly
//!   one completion (`Completed`, `Failed` or `Discarded`); a *completed*
//!   outcome means the whole checked positional operation succeeded (never
//!   a durability claim);
//! - no Tokio runtime worker is held while sync file I/O runs (blocking
//!   execution only);
//! - a write failing through a panic yields a `Failed` completion — never
//!   a silently lost acknowledgement — and the pool thread survives;
//! - [`WriteExecutor::shutdown`] stops admissions and joins every worker
//!   after all accepted writes settled, so the output owner can reclaim
//!   exclusive access afterwards.

use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc;

use super::sink::SinkError;
use super::write_budget::OutstandingByteBudget;
use crate::config::WriteExecutorConfig;
use crate::error::DownloadError;

/// Identifies the submitting job (fair per-job admission).
pub(crate) type JobId = u64;

/// The blocking positional write primitive the executor drives (design D2).
///
/// The production backend is [`super::output_session::OutputWriteHandle`]
/// through its existing `write_blocking`; tests may script short,
/// zero-progress or interrupted writes to verify that the executor
/// preserves the checked positional semantics.
pub(crate) trait WriteBackend: Send + Sync + 'static {
    /// Synchronous positional write of the complete buffer at `offset`.
    ///
    /// # Errors
    /// Structured sink error on write failure.
    fn write_blocking(&self, offset: u64, data: &[u8]) -> Result<(), SinkError>;
}

impl WriteBackend for super::output_session::OutputWriteHandle {
    fn write_blocking(&self, offset: u64, data: &[u8]) -> Result<(), SinkError> {
        // Delegates to the capability's checked positional adapter, keeping
        // the output fault-script boundary (cfg(test)) authoritative.
        self.write_blocking(offset, data)
    }
}

/// One positional write submitted for execution (design D2).
#[derive(Debug)]
pub(crate) struct WriteSubmission {
    /// Submitting job; the shared executor uses this for fair admission.
    pub job: JobId,
    /// Lease the payload belongs to (generation-tagged frontier, task 2.4).
    pub lease_id: u64,
    /// Lease generation at submission; completions for invalidated
    /// generations are discardable (tasks 2.4/2.7).
    pub generation: u64,
    /// Absolute output offset; positional, no shared cursor is touched.
    pub offset: u64,
    /// Payload, moved into the executor and retained until completion.
    pub data: Bytes,
}

/// Terminal status of one submitted write (design D2/D3).
#[derive(Debug)]
pub(crate) enum WriteOutcome {
    /// The whole checked positional operation returned successfully
    /// (`write_all_at` completed every byte at the offset). This is
    /// OS-write acknowledgement, **not** a durability claim.
    Completed,
    /// Structured sink error; the write did not fully reach the output.
    Failed(SinkError),
    /// Dropped before executing (detach discard disposition, task 2.7);
    /// no bytes reached the output.
    Discarded,
}

/// One completed write, tagged for per-lease frontier accounting (task 2.4).
#[derive(Debug)]
pub(crate) struct WriteCompletion {
    /// Lease the payload belonged to.
    pub lease_id: u64,
    /// Lease generation at submission.
    pub generation: u64,
    /// Absolute offset the write targeted.
    pub offset: u64,
    /// Payload length in bytes.
    pub len: u64,
    /// Terminal status.
    pub outcome: WriteOutcome,
}

/// Why a submission was not accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteExecutorError {
    /// The executor (or the submitting session) shut down; the write was
    /// not accepted and no completion will arrive for it.
    Closed,
}

/// What a detached session does with its queued-but-unstarted writes
/// (task 2.7 maps pause/retry to drain and cancellation to discard).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionDisposition {
    /// Execute every queued write before the session settles.
    Drain,
    /// Drop queued writes without executing; each yields a
    /// [`WriteOutcome::Discarded`] completion. In-flight writes finish.
    Discard,
}

/// Submit end of one job's executor attachment (design D2): workers submit
/// positional writes here and keep receiving payload. Detaching the session
/// (or dropping it without detaching, which drains) releases its output
/// capability; the executor stays usable by other sessions until it is shut
/// down.
/// Submit end of one job's executor attachment (design D2): workers submit
/// positional writes here and keep receiving payload. Cloning shares the
/// same session (same job, same completion stream). Detaching the session
/// (or dropping every handle without detaching, which drains) releases its
/// output capability; the executor stays usable by other sessions until it
/// is shut down.
#[derive(Clone)]
pub(crate) struct WriteSession {
    core: Arc<ExecutorCore>,
    /// Registry identity of this attachment (stable across queue changes).
    session_id: u64,
    job: JobId,
    /// Settle signal shared with the registry entry.
    settled: Arc<tokio::sync::Notify>,
    completions_tx: mpsc::UnboundedSender<WriteCompletion>,
}

/// Shared write executor (design D2, tasks 2.1-2.3).
///
/// Cloning the executor shares one core; each job attaches a session with
/// its own write-only capability and completion stream.
#[derive(Clone)]
pub(crate) struct WriteExecutor {
    core: Arc<ExecutorCore>,
}

/// One attached job's queue and lifecycle state, owned by the executor's
/// state mutex (every field is accessed under that lock; no interior
/// locking). Identified by `id` for post-execution bookkeeping.
struct SessionEntry {
    id: u64,
    job: JobId,
    capability: Arc<dyn WriteBackend>,
    /// Set when the session is closed (detached or executor shutdown):
    /// further submissions are rejected.
    closed: bool,
    /// Set when the session was explicitly detached; once its queue and
    /// in-flight work settle, it may leave the registry.
    detached: bool,
    queue: VecDeque<QueuedWrite>,
    executing: usize,
    /// Fired when a detached session has no queued and no executing writes.
    settled: Arc<tokio::sync::Notify>,
}

/// One queued positional write plus its completion channel and the
/// executor-wide byte charge it holds.
struct QueuedWrite {
    submission: WriteSubmission,
    completions: mpsc::UnboundedSender<WriteCompletion>,
    /// Bytes charged against the executor-wide queued+executing bound.
    bytes: u64,
}

#[derive(Default)]
struct ExecutorState {
    /// Attached sessions in attachment order; the round-robin cursor walks
    /// this vec for fair admission.
    sessions: Vec<SessionEntry>,
    cursor: usize,
    /// Identity counter for stable session lookup.
    next_session_id: u64,
}

struct ExecutorCore {
    state: Mutex<ExecutorState>,
    /// Blocking workers park here while no queued work exists.
    work_available: Condvar,
    /// Executor-wide bound on queued+executing payload bytes (design D2,
    /// task 2.3) — defense in depth beyond the caller-held reservations.
    queued_bytes: OutstandingByteBudget,
    /// Whole-executor close: no new submissions; workers drain and exit.
    closed: AtomicBool,
    /// Blocking worker thread identities (diagnostics, task 0.4: writer
    /// thread count must never scale with jobs × workers).
    writer_ids: Mutex<Vec<std::thread::ThreadId>>,
    /// Per-worker exit signals for deterministic shutdown: a worker sends
    /// on its oneshot after its loop returns; a dropped sender means the
    /// thread panicked.
    worker_exits: Mutex<Vec<tokio::sync::oneshot::Receiver<()>>>,
}

impl WriteExecutor {
    /// Create the executor with the configured small blocking pool. The
    /// threads start lazily on the Tokio blocking pool and register
    /// themselves for the writer-thread diagnostics.
    #[must_use]
    pub fn new(config: &WriteExecutorConfig) -> Self {
        let writer_threads = usize::try_from(config.writer_threads.max(1)).unwrap_or(1);
        let core = Arc::new(ExecutorCore {
            state: Mutex::new(ExecutorState::default()),
            work_available: Condvar::new(),
            queued_bytes: OutstandingByteBudget::new(config.max_queued_bytes),
            closed: AtomicBool::new(false),
            writer_ids: Mutex::new(Vec::new()),
            worker_exits: Mutex::new(Vec::new()),
        });
        for _ in 0..writer_threads {
            let worker_core = Arc::clone(&core);
            let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();
            std::thread::Builder::new()
                .name("kdown-write-executor".into())
                .spawn(move || worker_loop(worker_core, exit_tx))
                .expect("spawn write executor thread");
            core.worker_exits
                .lock()
                .expect("executor exits")
                .push(exit_rx);
        }
        Self { core }
    }

    /// Attach one job: the capability moves into the session and stays
    /// there until the session detaches (or the executor shuts down),
    /// which is what keeps `reclaim_exclusive` fail-closed while writes
    /// are outstanding.
    ///
    /// Returns the submit end and the session's completion stream.
    /// Completions arrive in execution order, which may differ from
    /// submission order — positional writes are independent.
    #[must_use]
    pub fn session(
        &self,
        job: JobId,
        capability: Arc<dyn WriteBackend>,
    ) -> (WriteSession, mpsc::UnboundedReceiver<WriteCompletion>) {
        // Unbounded: completions are tiny fixed-size records and their
        // count is bounded by accepted submissions (the executor byte cap
        // and the caller-held reservations bound queued work). A bounded
        // stream here could deadlock a pipelined worker awaiting
        // submission space while completions are undrained.
        let (completions_tx, completions_rx) = mpsc::unbounded_channel();
        let settled = Arc::new(tokio::sync::Notify::new());
        let session_id = {
            let mut state = self.core.state.lock().expect("executor state");
            let id = state.next_session_id;
            state.next_session_id += 1;
            state.sessions.push(SessionEntry {
                id,
                job,
                capability,
                closed: false,
                detached: false,
                queue: VecDeque::new(),
                executing: 0,
                settled: Arc::clone(&settled),
            });
            id
        };
        let session = WriteSession {
            core: Arc::clone(&self.core),
            session_id,
            job,
            settled,
            completions_tx,
        };
        (session, completions_rx)
    }

    /// Distinct blocking writer threads of this pool (diagnostics, task
    /// 0.4): bounded by the configured thread count regardless of jobs and
    /// network workers.
    #[must_use]
    pub fn writer_thread_count(&self) -> usize {
        self.core.writer_ids.lock().expect("writer ids").len()
    }

    /// Payload bytes currently queued or executing inside the executor
    /// (diagnostics, task 0.4).
    #[must_use]
    pub fn queued_bytes_outstanding(&self) -> u64 {
        self.core.queued_bytes.outstanding()
    }

    /// Stop accepting writes and join every pool worker after all accepted
    /// submissions settled (drain semantics; per-session discard of queued
    /// work is [`SessionDisposition::Discard`]). Resolves only after every
    /// write completed, failed or was discarded, so the caller may reclaim
    /// the output owner afterwards (session capabilities held only by the
    /// registry are released here as well).
    ///
    /// # Errors
    /// Sink error when a pool worker task panicked.
    pub async fn shutdown(self) -> Result<(), SinkError> {
        self.core.closed.store(true, Ordering::Release);
        self.core.work_available.notify_all();
        let exits = std::mem::take(&mut *self.core.worker_exits.lock().expect("executor exits"));
        let mut first_error: Option<SinkError> = None;
        for exit in exits {
            if exit.await.is_err() {
                first_error.get_or_insert_with(|| {
                    SinkError(DownloadError::SinkWrite(
                        "write executor worker thread panicked".into(),
                    ))
                });
            }
        }
        // Release capabilities held only by the registry so a caller that
        // dropped its session handles can reclaim the output.
        self.core
            .state
            .lock()
            .expect("executor state")
            .sessions
            .clear();
        first_error.map_or(Ok(()), Err)
    }
}

impl Default for WriteExecutor {
    fn default() -> Self {
        Self::new(&WriteExecutorConfig::default())
    }
}

impl WriteSession {
    /// The job this session submits for (fair admission identity).
    #[must_use]
    pub fn job(&self) -> JobId {
        self.job
    }

    /// Submit one positional write for execution. The payload moves into
    /// the executor; exactly one completion arrives on this session's
    /// stream when the write settles. Submission does NOT acknowledge the
    /// write — callers advance progress only on completions (task 2.4).
    ///
    /// The executor-wide queued+executing byte cap is acquired before
    /// enqueueing (cancellation-aware), so admission backpressures the
    /// submitting worker instead of buffering unboundedly.
    ///
    /// # Errors
    /// [`WriteExecutorError::Closed`] when the executor or this session
    /// shut down; the write was not accepted and no completion will arrive.
    ///
    /// # Panics
    /// When the submission's job id differs from this session's job — an
    /// internal routing bug, never a caller-recoverable condition.
    pub async fn submit(&self, write: WriteSubmission) -> Result<(), WriteExecutorError> {
        if self.core.closed.load(Ordering::Acquire) {
            return Err(WriteExecutorError::Closed);
        }
        assert!(
            write.job == self.job,
            "submission for job {} routed through the session of job {}",
            write.job,
            self.job
        );
        let bytes = write.data.len() as u64;
        self.core.queued_bytes.acquire(bytes).await;
        {
            let mut state = self.core.state.lock().expect("executor state");
            let session = state.sessions.iter_mut().find(|s| s.id == self.session_id);
            let Some(session) = session.filter(|s| !s.closed) else {
                drop(state);
                self.core.queued_bytes.release(bytes);
                return Err(WriteExecutorError::Closed);
            };
            session.queue.push_back(QueuedWrite {
                submission: write,
                completions: self.completions_tx.clone(),
                bytes,
            });
        }
        self.core.work_available.notify_one();
        Ok(())
    }

    /// Detach the session: stop accepting writes, settle queued work
    /// according to `disposition`, wait for in-flight writes, release the
    /// output capability and leave the executor registry. The completion
    /// stream closes once the final write settles.
    pub async fn detach(self, disposition: SessionDisposition) {
        {
            let mut state = self.core.state.lock().expect("executor state");
            if let Some(session) = state.sessions.iter_mut().find(|s| s.id == self.session_id) {
                session.closed = true;
                session.detached = true;
                if disposition == SessionDisposition::Discard {
                    while let Some(item) = session.queue.pop_front() {
                        self.core.queued_bytes.release(item.bytes);
                        let _ = item.completions.send(WriteCompletion {
                            lease_id: item.submission.lease_id,
                            generation: item.submission.generation,
                            offset: item.submission.offset,
                            len: item.submission.data.len() as u64,
                            outcome: WriteOutcome::Discarded,
                        });
                    }
                }
                if session.queue.is_empty() && session.executing == 0 {
                    session.settled.notify_waiters();
                }
            }
        }
        // Wait until the session has no queued and no executing writes
        // (register interest before checking — no lost wakeup), then leave
        // the registry so its capability can drop.
        loop {
            let notified = self.settled.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.core.state.lock().expect("executor state");
                match state.sessions.iter().find(|s| s.id == self.session_id) {
                    // Removed by the executor shutdown path: settled.
                    None => break,
                    Some(session) => {
                        if session.queue.is_empty() && session.executing == 0 {
                            break;
                        }
                    }
                }
            }
            notified.await;
        }
        self.core
            .state
            .lock()
            .expect("executor state")
            .sessions
            .retain(|s| s.id != self.session_id);
    }
}

impl std::fmt::Debug for WriteExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteExecutor")
            .field("writer_threads", &self.writer_thread_count())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for WriteSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSession")
            .field("job", &self.job)
            .finish_non_exhaustive()
    }
}

impl Drop for ExecutorCore {
    fn drop(&mut self) {
        // Release parked workers even when the executor is dropped without
        // an explicit `shutdown` (error paths, test teardown): workers see
        // `closed` and exit as soon as everything settled. Dropping a Tokio
        // runtime must never block on a parked blocking worker.
        self.closed.store(true, Ordering::SeqCst);
        self.work_available.notify_all();
    }
}

/// Blocking pool worker: pop the next write fairly across sessions and
/// execute it until the executor closes and everything drained.
fn worker_loop(core: Arc<ExecutorCore>, exit_tx: tokio::sync::oneshot::Sender<()>) {
    core.writer_ids
        .lock()
        .expect("writer ids")
        .push(std::thread::current().id());
    while let Some((session_id, capability, item)) = core.take_next() {
        let outcome = execute_write(&capability, &item.submission);
        core.settle(session_id, item, outcome);
    }
    // Signal the (async) shutdown that this worker settled and exited; a
    // dropped sender (thread panicked) surfaces as a sink error instead.
    let _ = exit_tx.send(());
}

/// Execute one write through the capability, converting a panic into a
/// structured `Failed` completion so every accepted submission settles and
/// its byte charge is always released (fail closed, never lost).
fn execute_write(capability: &Arc<dyn WriteBackend>, submission: &WriteSubmission) -> WriteOutcome {
    let result = catch_unwind(AssertUnwindSafe(|| {
        capability.write_blocking(submission.offset, &submission.data)
    }));
    match result {
        Ok(Ok(())) => WriteOutcome::Completed,
        Ok(Err(error)) => WriteOutcome::Failed(error),
        Err(panic) => WriteOutcome::Failed(SinkError(DownloadError::SinkWrite(format!(
            "write executor task panicked: {}",
            panic_message(panic.as_ref())
        )))),
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

impl ExecutorCore {
    /// Pop the next write, fair round-robin across sessions (task 2.3):
    /// every dispatch advances the cursor past the served session, so a
    /// heavy job cannot starve others. Parks on the condvar while no work
    /// exists; exits only after the executor closed AND everything settled.
    fn take_next(&self) -> Option<(u64, Arc<dyn WriteBackend>, QueuedWrite)> {
        let mut state = self.state.lock().expect("executor state");
        loop {
            let count = state.sessions.len();
            if count > 0 {
                let start = state.cursor % count;
                for offset in 0..count {
                    let index = (start + offset) % count;
                    if state.sessions[index].queue.is_empty() {
                        continue;
                    }
                    state.cursor = (index + 1) % count;
                    let item = state.sessions[index]
                        .queue
                        .pop_front()
                        .expect("checked non-empty");
                    state.sessions[index].executing += 1;
                    let session = &state.sessions[index];
                    return Some((session.id, Arc::clone(&session.capability), item));
                }
            }
            if self.closed.load(Ordering::SeqCst)
                && state
                    .sessions
                    .iter()
                    .all(|s| s.queue.is_empty() && s.executing == 0)
            {
                return None;
            }
            state = self.work_available.wait(state).expect("executor state");
        }
    }

    /// Deliver the completion, release the byte charge and update the
    /// session/executor settle state.
    fn settle(&self, session_id: u64, item: QueuedWrite, outcome: WriteOutcome) {
        let completion = WriteCompletion {
            lease_id: item.submission.lease_id,
            generation: item.submission.generation,
            offset: item.submission.offset,
            len: item.submission.data.len() as u64,
            outcome,
        };
        let _ = item.completions.send(completion);
        self.queued_bytes.release(item.bytes);
        drop(item);
        let mut state = self.state.lock().expect("executor state");
        if let Some(session) = state.sessions.iter_mut().find(|s| s.id == session_id) {
            session.executing -= 1;
            if session.detached && session.queue.is_empty() && session.executing == 0 {
                session.settled.notify_waiters();
            }
        }
        if self.closed.load(Ordering::SeqCst) {
            // A parked worker may now be able to exit (all drained).
            self.work_available.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::output_session::OutputSession;
    use crate::io::positional::{write_all_at, PositionalWriter};
    use crate::io::sink::TempFileSpec;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    fn executor(writer_threads: u32, max_queued_bytes: u64) -> WriteExecutor {
        WriteExecutor::new(&WriteExecutorConfig {
            writer_threads,
            max_queued_bytes,
            pipeline_writes: false,
        })
    }

    fn session(directory: &tempfile::TempDir) -> OutputSession {
        OutputSession::create(
            &directory.path().join("out.bin"),
            &TempFileSpec::default(),
            false,
            false,
            None,
        )
        .expect("session")
    }

    fn capability(session: &mut OutputSession) -> Arc<dyn WriteBackend> {
        Arc::new(
            session
                .share_write_handles(1)
                .expect("share")
                .pop()
                .expect("one capability"),
        )
    }

    fn submission(
        job: JobId,
        lease: u64,
        generation: u64,
        offset: u64,
        data: &[u8],
    ) -> WriteSubmission {
        WriteSubmission {
            job,
            lease_id: lease,
            generation,
            offset,
            data: Bytes::copy_from_slice(data),
        }
    }

    async fn recv(rx: &mut mpsc::UnboundedReceiver<WriteCompletion>) -> WriteCompletion {
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("completion arrives in time")
            .expect("completion stream open")
    }

    fn assert_completed(completion: &WriteCompletion) {
        assert!(
            matches!(completion.outcome, WriteOutcome::Completed),
            "expected Completed, got {:?}",
            completion.outcome
        );
    }

    /// Deterministic scripted positional file mirroring the positional
    /// adapter's test double: short writes, interruption, zero progress,
    /// scripted failures and an optional slow-sink delay — executed
    /// through the REAL `write_all_at` loop so the executor is verified
    /// against production semantics. Every invocation logs its offset so
    /// tests can observe execution order.
    struct ScriptedFile {
        state: Mutex<ScriptState>,
        /// Milliseconds to sleep per write (slow-sink simulation).
        delay_ms: AtomicU64,
        /// Offsets of executed writes, in execution order.
        invocations: Mutex<Vec<u64>>,
    }

    struct ScriptState {
        writes: Vec<(u64, Vec<u8>)>,
        /// Maximum bytes per call (short-write simulation).
        limit: Option<usize>,
        /// Fail with this error from call N (1-based) onward.
        error_from: Option<(usize, std::io::ErrorKind)>,
        /// Report Ok(0) on the first N calls.
        zero_first: usize,
        /// Report Interrupted on the first N calls.
        interrupted_first: usize,
        calls: usize,
    }

    impl ScriptedFile {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                state: Mutex::new(ScriptState {
                    writes: vec![],
                    limit: None,
                    error_from: None,
                    zero_first: 0,
                    interrupted_first: 0,
                    calls: 0,
                }),
                delay_ms: AtomicU64::new(0),
                invocations: Mutex::new(Vec::new()),
            })
        }

        fn set_delay_ms(&self, delay_ms: u64) {
            self.delay_ms.store(delay_ms, Ordering::SeqCst);
        }

        fn invocation_offsets(&self) -> Vec<u64> {
            self.invocations.lock().expect("invocations").clone()
        }
    }

    impl PositionalWriter for ScriptedFile {
        fn pos_write(&self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
            let mut state = self.state.lock().expect("scripted file");
            state.calls += 1;
            if let Some((n, kind)) = &state.error_from {
                if state.calls >= *n {
                    return Err(std::io::Error::new(*kind, "scripted"));
                }
            }
            if state.calls <= state.interrupted_first {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "scripted interrupt",
                ));
            }
            if state.calls <= state.zero_first {
                return Ok(0);
            }
            let n = state.limit.map_or(buf.len(), |l| l.min(buf.len()));
            state.writes.push((offset, buf[..n].to_vec()));
            Ok(n)
        }
    }

    impl WriteBackend for ScriptedFile {
        fn write_blocking(&self, offset: u64, data: &[u8]) -> Result<(), SinkError> {
            self.invocations.lock().expect("invocations").push(offset);
            let delay = self.delay_ms.load(Ordering::SeqCst);
            if delay > 0 {
                std::thread::sleep(Duration::from_millis(delay));
            }
            write_all_at(self, offset, data).map_err(SinkError::from)
        }
    }

    fn reconstructed(file: &ScriptedFile) -> Vec<u8> {
        let state = file.state.lock().expect("scripted file");
        let mut out = vec![];
        for (offset, bytes) in &state.writes {
            let end = *offset as usize + bytes.len();
            if out.len() < end {
                out.resize(end, 0);
            }
            out[*offset as usize..end].copy_from_slice(bytes);
        }
        out
    }

    #[tokio::test]
    async fn writes_execute_at_exact_offsets_out_of_submission_order() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let mut output = session(&directory);
        let executor = executor(4, 32 * 1024 * 1024);
        let (session_a, mut rx) = executor.session(7, capability(&mut output));

        // Submit out of order and unaligned; completions may arrive in any
        // execution order, but every byte must land at its exact offset.
        session_a
            .submit(submission(7, 1, 0, 11, b"segment-c"))
            .await
            .expect("submit c");
        session_a
            .submit(submission(7, 1, 0, 5, b"-seg-b"))
            .await
            .expect("submit b");
        session_a
            .submit(submission(7, 1, 0, 0, b"seg-a"))
            .await
            .expect("submit a");

        let mut completions = [
            recv(&mut rx).await,
            recv(&mut rx).await,
            recv(&mut rx).await,
        ];
        completions.sort_by_key(|c| c.offset);
        let expected = [(0, 5, 1), (5, 6, 1), (11, 9, 1)];
        for (completion, (offset, len, lease)) in completions.iter().zip(expected) {
            assert_completed(completion);
            assert_eq!(completion.offset, offset);
            assert_eq!(completion.len, len);
            assert_eq!(completion.lease_id, lease);
            assert_eq!(completion.generation, 0);
        }

        drop(session_a);
        executor.shutdown().await.expect("shutdown");
        output.reclaim_exclusive().expect("reclaim after shutdown");
        assert_eq!(
            std::fs::read(output.temp_path()).expect("content"),
            b"seg-a-seg-bsegment-c"
        );
    }

    #[tokio::test]
    async fn concurrent_sessions_write_disjoint_offsets_exactly() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let mut output = session(&directory);
        let executor = executor(4, 32 * 1024 * 1024);
        let mut handles = output.share_write_handles(2).expect("share two");
        let capability_b: Arc<dyn WriteBackend> = Arc::new(handles.pop().expect("b"));
        let capability_a: Arc<dyn WriteBackend> = Arc::new(handles.pop().expect("a"));
        let (session_a, mut rxa) = executor.session(1, capability_a);
        let (session_b, mut rxb) = executor.session(2, capability_b);

        let (ra, rb) = tokio::join!(
            session_a.submit(submission(1, 1, 3, 0, b"first")),
            session_b.submit(submission(2, 9, 3, 5, b"-last")),
        );
        ra.expect("submit a");
        rb.expect("submit b");
        assert_completed(&recv(&mut rxa).await);
        assert_completed(&recv(&mut rxb).await);

        drop(session_a);
        drop(session_b);
        executor.shutdown().await.expect("shutdown");
        output.reclaim_exclusive().expect("reclaim");
        assert_eq!(
            std::fs::read(output.temp_path()).expect("content"),
            b"first-last"
        );
    }

    #[tokio::test]
    async fn zero_length_write_completes_without_touching_the_output() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let mut output = session(&directory);
        let executor = executor(2, 32 * 1024 * 1024);
        let (session_a, mut rx) = executor.session(1, capability(&mut output));

        session_a
            .submit(submission(1, 1, 0, 7, b""))
            .await
            .expect("submit empty");
        let completion = recv(&mut rx).await;
        assert_completed(&completion);
        assert_eq!(completion.len, 0);

        drop(session_a);
        executor.shutdown().await.expect("shutdown");
        output.reclaim_exclusive().expect("reclaim");
        assert!(
            std::fs::read(output.temp_path())
                .expect("content")
                .is_empty(),
            "empty write must not touch the output"
        );
    }

    #[tokio::test]
    async fn large_offsets_are_preserved_through_the_executor() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let mut output = session(&directory);
        let executor = executor(2, 32 * 1024 * 1024);
        let (session_a, mut rx) = executor.session(1, capability(&mut output));

        const FAR: u64 = 4 * 1024 * 1024;
        session_a
            .submit(submission(1, 1, 0, FAR, b"tail"))
            .await
            .expect("submit far");
        let completion = recv(&mut rx).await;
        assert_completed(&completion);
        assert_eq!(completion.offset, FAR);

        drop(session_a);
        executor.shutdown().await.expect("shutdown");
        output.reclaim_exclusive().expect("reclaim");
        let content = std::fs::read(output.temp_path()).expect("content");
        assert_eq!(content.len(), FAR as usize + 4);
        assert_eq!(&content[FAR as usize..], b"tail");
    }

    #[tokio::test]
    async fn short_writes_complete_exactly_through_the_executor() {
        let executor = executor(2, 32 * 1024 * 1024);
        let file = ScriptedFile::new();
        file.state.lock().expect("scripted file").limit = Some(3);
        let (session_a, mut rx) = executor.session(1, file.clone() as Arc<dyn WriteBackend>);

        session_a
            .submit(submission(1, 1, 0, 10, b"abcdefghij"))
            .await
            .expect("submit");
        let completion = recv(&mut rx).await;
        assert_completed(&completion);
        assert_eq!(completion.len, 10);
        assert_eq!(completion.offset, 10);

        let mut expected = vec![0u8; 20];
        expected[10..20].copy_from_slice(b"abcdefghij");
        assert_eq!(
            reconstructed(&file),
            expected,
            "remainder at advanced offset"
        );
    }

    #[tokio::test]
    async fn interrupted_writes_retry_and_complete_through_the_executor() {
        let executor = executor(2, 32 * 1024 * 1024);
        let file = ScriptedFile::new();
        file.state.lock().expect("scripted file").interrupted_first = 2;
        let (session_a, mut rx) = executor.session(1, file.clone() as Arc<dyn WriteBackend>);

        session_a
            .submit(submission(1, 1, 0, 0, b"data"))
            .await
            .expect("submit");
        let completion = recv(&mut rx).await;
        assert_completed(&completion);
        assert_eq!(reconstructed(&file), b"data");
    }

    #[tokio::test]
    async fn zero_progress_reports_a_failed_outcome_never_success() {
        let executor = executor(2, 32 * 1024 * 1024);
        let file = ScriptedFile::new();
        file.state.lock().expect("scripted file").zero_first = 5;
        let (session_a, mut rx) = executor.session(1, file.clone() as Arc<dyn WriteBackend>);

        session_a
            .submit(submission(1, 1, 0, 0, b"never"))
            .await
            .expect("submit");
        let completion = recv(&mut rx).await;
        match &completion.outcome {
            WriteOutcome::Failed(error) => {
                assert!(
                    matches!(error.0, DownloadError::SinkWrite(_)),
                    "structured sink error expected, got {:?}",
                    error.0
                );
            }
            other => panic!("zero progress must fail, got {other:?}"),
        }
        assert!(reconstructed(&file).is_empty(), "no bytes reported written");
    }

    #[tokio::test]
    async fn fault_script_failure_surfaces_as_a_failed_completion() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let destination = directory.path().join("out.bin");
        let registration = crate::io::fault_script::OutputFaultScript::register(&destination);
        registration.script().fail_next(
            crate::io::fault_script::OutputOperation::Write,
            DownloadError::DiskFull("scripted ENOSPC".into()),
        );

        let mut output = session(&directory);
        let executor = executor(2, 32 * 1024 * 1024);
        let (session_a, mut rx) = executor.session(1, capability(&mut output));

        session_a
            .submit(submission(1, 1, 0, 0, b"payload"))
            .await
            .expect("submit");
        let failed = recv(&mut rx).await;
        match &failed.outcome {
            WriteOutcome::Failed(error) => {
                assert!(
                    matches!(error.0, DownloadError::DiskFull(_)),
                    "scripted fault must surface, got {:?}",
                    error.0
                );
            }
            other => panic!("scripted fault must fail the write, got {other:?}"),
        }
        assert_eq!(failed.len, 7);
        assert_eq!(failed.offset, 0);

        // The capability is not poisoned: the next write completes.
        session_a
            .submit(submission(1, 1, 0, 0, b"payload"))
            .await
            .expect("submit retry");
        assert_completed(&recv(&mut rx).await);

        drop(session_a);
        executor.shutdown().await.expect("shutdown");
        output.reclaim_exclusive().expect("reclaim");
        assert_eq!(
            std::fs::read(output.temp_path()).expect("content"),
            b"payload"
        );
    }

    #[tokio::test]
    async fn shutdown_joins_inflight_writes_then_rejects_new_submissions() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let destination = directory.path().join("out.bin");
        let registration = crate::io::fault_script::OutputFaultScript::register(&destination);
        let gate = registration
            .script()
            .hold_next(crate::io::fault_script::OutputOperation::Write);

        let mut output = session(&directory);
        let executor = executor(2, 32 * 1024 * 1024);
        let (session_a, mut rx) = executor.session(1, capability(&mut output));

        session_a
            .submit(submission(1, 1, 0, 0, b"held"))
            .await
            .expect("submit");
        gate.wait_until_entered();

        // Shutdown must not resolve while the write is still executing.
        let shutdown = tokio::spawn(executor.shutdown());
        tokio::time::timeout(Duration::from_millis(50), async {
            loop {
                tokio::task::yield_now().await;
                if shutdown.is_finished() {
                    panic!("shutdown returned while a write was in flight");
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect_err("shutdown must stay pending while the write executes");

        gate.release();
        shutdown.await.expect("shutdown task").expect("clean join");
        assert_completed(&recv(&mut rx).await);

        // Submissions after shutdown fail closed; no completion follows.
        let rejected = session_a
            .submit(submission(1, 1, 0, 0, b"late"))
            .await
            .expect_err("post-shutdown submission");
        assert_eq!(rejected, WriteExecutorError::Closed);

        drop(session_a);
        output.reclaim_exclusive().expect("reclaim after join");
        assert_eq!(std::fs::read(output.temp_path()).expect("content"), b"held");
    }

    #[tokio::test]
    async fn session_drop_closes_the_completion_stream_without_blocking_others() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let mut output = session(&directory);
        let executor = executor(2, 32 * 1024 * 1024);
        let mut handles = output.share_write_handles(2).expect("share two");
        let capability_b: Arc<dyn WriteBackend> = Arc::new(handles.pop().expect("b"));
        let capability_a: Arc<dyn WriteBackend> = Arc::new(handles.pop().expect("a"));
        let (session_a, mut rxa) = executor.session(1, capability_a);
        let (session_b, mut rxb) = executor.session(2, capability_b);

        session_a
            .submit(submission(1, 1, 0, 0, b"a"))
            .await
            .expect("submit a");
        drop(session_a);
        // The dropped session's queued write still executes (drain
        // semantics); its stream closes after delivering it.
        assert_completed(&recv(&mut rxa).await);
        assert!(
            tokio::time::timeout(Duration::from_secs(10), rxa.recv())
                .await
                .expect("recv resolves")
                .is_none(),
            "closed session stream must end without fabricating completions"
        );

        session_b
            .submit(submission(2, 2, 0, 1, b"b"))
            .await
            .expect("submit b still works");
        assert_completed(&recv(&mut rxb).await);

        drop(session_b);
        executor.shutdown().await.expect("shutdown");
        output.reclaim_exclusive().expect("reclaim");
        assert_eq!(std::fs::read(output.temp_path()).expect("content"), b"ab");
    }

    /// Task 2.3 verification: many jobs (16 "async workers" worth of
    /// sessions) hammering one executor must be served by exactly the
    /// configured number of blocking writer threads — never one per
    /// worker — and every job must make byte-exact progress.
    #[tokio::test]
    async fn many_jobs_are_served_by_only_the_configured_writer_threads() {
        const JOBS: usize = 16;
        const WRITES_PER_JOB: usize = 4;
        const CHUNK: usize = 512;

        let directory = tempfile::tempdir().expect("tmpdir");
        let mut output = session(&directory);
        let executor = executor(2, 32 * 1024 * 1024);
        let sink = ScriptedFile::new();
        sink.set_delay_ms(5); // slow sink: overlapping executions

        let mut handles = output.share_write_handles(JOBS).expect("share 16");
        let mut sessions = Vec::new();
        let mut receivers = Vec::new();
        for job in 0..JOBS {
            let capability: Arc<dyn WriteBackend> = Arc::new(handles.pop().expect("one per job"));
            let (session, rx) = executor.session(job as JobId, capability);
            sessions.push(session);
            receivers.push(rx);
        }

        // Every job's async "worker" pipelines its writes through the
        // shared pool.
        for (job, session) in sessions.iter().enumerate() {
            let base = (job * WRITES_PER_JOB * CHUNK) as u64;
            for chunk in 0..WRITES_PER_JOB {
                let payload =
                    vec![u8::try_from(job * WRITES_PER_JOB + chunk + 1).unwrap_or(1); CHUNK];
                session
                    .submit(submission(
                        job as JobId,
                        1,
                        0,
                        base + (chunk * CHUNK) as u64,
                        &payload,
                    ))
                    .await
                    .expect("submit");
            }
        }
        // Await every completion; all jobs must progress despite the
        // two-thread pool.
        for rx in receivers.iter_mut() {
            for _ in 0..WRITES_PER_JOB {
                assert_completed(&recv(rx).await);
            }
        }

        assert_eq!(
            executor.writer_thread_count(),
            2,
            "only the configured writer threads may exist"
        );

        // Byte-exact assembly across all jobs (checked while the session
        // handles are still alive; they must drop before reclaim).
        let content = std::fs::read(output.temp_path()).expect("content");
        assert_eq!(content.len(), JOBS * WRITES_PER_JOB * CHUNK);
        for (job, _) in sessions.iter().enumerate() {
            for chunk in 0..WRITES_PER_JOB {
                let start = (job * WRITES_PER_JOB + chunk) * CHUNK;
                let expected_byte = u8::try_from(job * WRITES_PER_JOB + chunk + 1).unwrap_or(1);
                assert!(
                    content[start..start + CHUNK]
                        .iter()
                        .all(|&b| b == expected_byte),
                    "job {job} chunk {chunk} bytes must land exactly"
                );
            }
        }
        drop(sessions);
        executor.shutdown().await.expect("shutdown");
        output.reclaim_exclusive().expect("reclaim");
    }

    /// Fair per-job admission (task 2.3): with one writer thread and a
    /// slow sink, a queued interleave of two jobs must be served
    /// round-robin — job B's first write precedes job A's second.
    #[tokio::test]
    async fn admission_is_fair_round_robin_across_jobs() {
        let executor = executor(1, 32 * 1024 * 1024);
        let sink = ScriptedFile::new();
        sink.set_delay_ms(10);
        let (session_a, mut rxa) = executor.session(1, sink.clone() as Arc<dyn WriteBackend>);
        let (session_b, mut rxb) = executor.session(2, sink.clone() as Arc<dyn WriteBackend>);

        // Enqueue A1, A2, then B1, B2. Offsets identify each write in the
        // backend's execution log.
        session_a
            .submit(submission(1, 1, 0, 0, b"A1"))
            .await
            .expect("A1");
        session_a
            .submit(submission(1, 1, 0, 100, b"A2"))
            .await
            .expect("A2");
        session_b
            .submit(submission(2, 1, 0, 50, b"B1"))
            .await
            .expect("B1");
        session_b
            .submit(submission(2, 1, 0, 150, b"B2"))
            .await
            .expect("B2");

        assert_completed(&recv(&mut rxa).await);
        assert_completed(&recv(&mut rxa).await);
        assert_completed(&recv(&mut rxb).await);
        assert_completed(&recv(&mut rxb).await);

        assert_eq!(
            sink.invocation_offsets(),
            vec![0, 50, 100, 150],
            "round-robin must interleave A1,B1,A2,B2 — not drain A first"
        );

        drop(session_a);
        drop(session_b);
        executor.shutdown().await.expect("shutdown");
    }

    /// The executor-wide queued+executing byte cap bounds admitted payload
    /// even while the sink is completely blocked.
    #[tokio::test]
    async fn queued_payload_stays_within_the_executor_byte_bound() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let destination = directory.path().join("out.bin");
        let registration = crate::io::fault_script::OutputFaultScript::register(&destination);
        let gate = registration
            .script()
            .hold_next(crate::io::fault_script::OutputOperation::Write);

        let mut output = session(&directory);
        // Cap admits: 1 executing + 1 queued 100-byte write; a third 100
        // (total 260 over the 250 cap) must pend.
        let executor = executor(1, 250);
        let (session_a, mut rx) = executor.session(1, capability(&mut output));

        session_a
            .submit(submission(1, 1, 0, 0, &[b'x'; 100]))
            .await
            .expect("first write (executing under the gate)");
        gate.wait_until_entered();
        session_a
            .submit(submission(1, 1, 0, 100, &[b'y'; 100]))
            .await
            .expect("second write (queued)");
        assert_eq!(executor.queued_bytes_outstanding(), 200);

        let session_for_blocked = session_a.clone();
        let blocked = tokio::spawn(async move {
            session_for_blocked
                .submit(submission(1, 1, 0, 200, &[b'z'; 100]))
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !blocked.is_finished(),
            "submission beyond the byte cap must backpressure"
        );
        assert_eq!(executor.queued_bytes_outstanding(), 200);

        // Releasing the sink drains everything and admits the third write.
        gate.release();
        assert_completed(&recv(&mut rx).await);
        assert_completed(&recv(&mut rx).await);
        let third = tokio::time::timeout(Duration::from_secs(5), blocked)
            .await
            .expect("third write admitted after drain")
            .expect("submit ok");
        assert!(third.is_ok());
        assert_completed(&recv(&mut rx).await);
        assert_eq!(executor.queued_bytes_outstanding(), 0);

        drop(session_a);
        executor.shutdown().await.expect("shutdown");
        output.reclaim_exclusive().expect("reclaim");
    }

    /// Detach with `Discard` drops queued writes as `Discarded`
    /// completions, lets in-flight writes finish, releases the byte
    /// charge and leaves the executor usable for other sessions.
    #[tokio::test]
    async fn discard_disposition_drops_queued_writes_and_releases_capacity() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let destination = directory.path().join("out.bin");
        let registration = crate::io::fault_script::OutputFaultScript::register(&destination);
        let gate = registration
            .script()
            .hold_next(crate::io::fault_script::OutputOperation::Write);

        let mut output = session(&directory);
        let executor = executor(1, 32 * 1024 * 1024);
        let mut handles = output.share_write_handles(2).expect("share two");
        let capability_b: Arc<dyn WriteBackend> = Arc::new(handles.pop().expect("b"));
        let capability_a: Arc<dyn WriteBackend> = Arc::new(handles.pop().expect("a"));
        let (session_a, mut rxa) = executor.session(1, capability_a);
        let (session_b, mut rxb) = executor.session(2, capability_b);

        session_a
            .submit(submission(1, 1, 0, 0, b"inflight"))
            .await
            .expect("executing");
        gate.wait_until_entered();
        session_a
            .submit(submission(1, 1, 0, 100, b"queued"))
            .await
            .expect("queued");

        let detach = tokio::spawn(session_a.detach(SessionDisposition::Discard));
        let discarded = recv(&mut rxa).await;
        assert!(
            matches!(discarded.outcome, WriteOutcome::Discarded),
            "queued write must be discarded, got {:?}",
            discarded.outcome
        );
        assert_eq!(discarded.offset, 100);
        assert_eq!(discarded.len, 6);
        assert_eq!(
            executor.queued_bytes_outstanding(),
            8,
            "only the in-flight write's bytes remain charged"
        );

        // The in-flight write completes; then the detach resolves.
        gate.release();
        assert_completed(&recv(&mut rxa).await);
        tokio::time::timeout(Duration::from_secs(5), detach)
            .await
            .expect("detach settles")
            .expect("detach task");
        assert_eq!(executor.queued_bytes_outstanding(), 0);

        // Other sessions keep working; the discarded bytes never landed.
        session_b
            .submit(submission(2, 1, 0, 200, b"B"))
            .await
            .expect("submit b");
        assert_completed(&recv(&mut rxb).await);
        drop(session_b);
        executor.shutdown().await.expect("shutdown");
        output.reclaim_exclusive().expect("reclaim");
        // The in-flight write landed; the discarded write did not.
        let content = std::fs::read(output.temp_path()).expect("content");
        assert_eq!(&content[..8], b"inflight");
        assert_eq!(&content[200..201], b"B");
        assert_eq!(content.len(), 201, "no bytes from the discarded write");
    }

    /// Detach with `Drain` executes every queued write before the session
    /// settles (pause semantics, task 2.7).
    #[tokio::test]
    async fn drain_disposition_executes_queued_writes_before_settling() {
        let directory = tempfile::tempdir().expect("tmpdir");
        let destination = directory.path().join("out.bin");
        let registration = crate::io::fault_script::OutputFaultScript::register(&destination);
        let gate = registration
            .script()
            .hold_next(crate::io::fault_script::OutputOperation::Write);

        let mut output = session(&directory);
        let executor = executor(1, 32 * 1024 * 1024);
        let (session_a, mut rx) = executor.session(1, capability(&mut output));

        session_a
            .submit(submission(1, 1, 0, 0, b"first"))
            .await
            .expect("executing");
        gate.wait_until_entered();
        session_a
            .submit(submission(1, 1, 0, 5, b"second"))
            .await
            .expect("queued");

        let detach = tokio::spawn(session_a.detach(SessionDisposition::Drain));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !detach.is_finished(),
            "drain must wait for queued writes to execute"
        );

        gate.release();
        tokio::time::timeout(Duration::from_secs(5), detach)
            .await
            .expect("drain settles after the sink unblocks")
            .expect("detach task");
        assert_completed(&recv(&mut rx).await);
        assert_completed(&recv(&mut rx).await);
        assert_eq!(executor.queued_bytes_outstanding(), 0);

        executor.shutdown().await.expect("shutdown");
        output.reclaim_exclusive().expect("reclaim");
        assert_eq!(
            std::fs::read(output.temp_path()).expect("content"),
            b"firstsecond"
        );
    }

    /// A panicking write backend yields a structured `Failed` completion
    /// (never a lost acknowledgement) and the pool keeps serving.
    #[tokio::test]
    async fn panicking_write_yields_a_failed_completion_and_the_pool_survives() {
        struct PanickingBackend;
        impl WriteBackend for PanickingBackend {
            fn write_blocking(&self, _offset: u64, _data: &[u8]) -> Result<(), SinkError> {
                panic!("scripted backend panic");
            }
        }

        let executor = executor(2, 32 * 1024 * 1024);
        let (session_a, mut rxa) =
            executor.session(1, Arc::new(PanickingBackend) as Arc<dyn WriteBackend>);
        session_a
            .submit(submission(1, 1, 0, 0, b"boom"))
            .await
            .expect("submit");
        let failed = recv(&mut rxa).await;
        assert!(
            matches!(&failed.outcome, WriteOutcome::Failed(error)
                if matches!(error.0, DownloadError::SinkWrite(_))),
            "panic must surface as a structured Failed completion"
        );

        // The pool survives: a healthy session still completes.
        let sink = ScriptedFile::new();
        let (session_b, mut rxb) = executor.session(2, sink as Arc<dyn WriteBackend>);
        session_b
            .submit(submission(2, 1, 0, 0, b"ok"))
            .await
            .expect("submit b");
        assert_completed(&recv(&mut rxb).await);

        drop(session_a);
        drop(session_b);
        executor.shutdown().await.expect("shutdown");
    }
}
