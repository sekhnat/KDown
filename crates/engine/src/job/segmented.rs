//! Segmented download orchestration (§12, §13).
//!
//! eligibility gate (§10.3, caller) -> scheduler -> bounded worker pool:
//! acquire -> validated range request -> positional write -> report ->
//! complete/fail -> repeat. Terminal sink errors propagate and stop the
//! job (§14.5). Coordinated origin backoff gates all workers on 503/429
//! (§17.4). Runtime concurrency reduction settles excess workers (task
//! 5.8).

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex as AsyncMutex;

use crate::config::{DurabilityMode, EngineConfig};
use crate::control::origin::OriginRegistry;
use crate::control::retry::{RetryClassifier, RetryDecision};
use crate::control::CancellationToken;
use crate::error::DownloadError;
use crate::http::probe::ProbeMetadata;
use crate::http::{
    BodyEvent, FullResponsePolicy, HttpExecution, HttpFailure, RangeIntent, RequestSpec,
    TransferIntent, TransferRequest,
};
use crate::io::output_session::OutputSession;
use crate::io::write_budget::{ByteReservation, JobWriteBudget, WriteBudgets};
use crate::io::write_executor::{
    SessionDisposition, WriteCompletion, WriteExecutor, WriteOutcome, WriteSession, WriteSubmission,
};
use crate::io::write_frontier::{CompletionStatus, LeaseFrontier};
use crate::job::controller::{DownloadRequest, ResultStatus};
use crate::job::state::{JobState, StateMachine};
use crate::metrics::counters::JobCounters;
use crate::metrics::events::SharedHub;
use crate::resume::checkpoint::Checkpoint;
use crate::resume::checkpoint_store::CheckpointStore;
use crate::scheduler::core::{SchedulerPolicy, SegmentScheduler, TargetSelector};
use crate::scheduler::lease::SegmentLease;

/// Shared worker↔controller state for one segmented job.
pub struct SegmentedJob {
    scheduler: AsyncMutex<SegmentScheduler>,
    /// Coordinated origin backoff gate (§17.4): when set, all workers wait
    /// until this instant before their next request.
    origin_backoff_until: AsyncMutex<Option<Instant>>,
    /// Terminal error state (§14.5): a cheap atomic flag for the
    /// chunk path plus a small mutex owning the first-wins detailed error.
    fatal: FatalState,
    cancel: CancellationToken,
    hub: SharedHub,
    counters: Arc<JobCounters>,
    /// Stable per-job token bucket (§18): one `Arc<TokenBucket>`
    /// for the job's lifetime; rate `0` = unlimited. Live rate updates
    /// mutate the bucket in place — active workers never see the object
    /// replaced, and unlimited chunks take no outer lock.
    rate_bucket: Arc<crate::control::rate_limit::TokenBucket>,
    /// Engine-global payload bucket above the job bucket (§18).
    /// Unlimited by default; `acquire` early-returns without the lock.
    global_rate_bucket: Arc<crate::control::rate_limit::TokenBucket>,
    total_size: u64,
    validators: crate::http::validators::ResourceValidators,
    /// Durable-mode data-sync capability over the output file:
    /// used by the shared save path to synchronize before persisting.
    sync: Option<crate::io::output_session::OutputSyncCapability>,
    /// Selected checkpoint durability: drives the save path's
    /// sync-before-persist ordering.
    durability: DurabilityMode,
    /// Versioned scheduler-state signal: bumped AFTER every
    /// state transition (work added/removed, split eligibility, progress
    /// reconciliation, desired-worker changes, fatal, resume). Parked
    /// workers register before sleeping and recheck on change — no lost
    /// work, no fixed polling.
    revision_tx: Arc<tokio::sync::watch::Sender<u64>>,
    /// The job-level checkpoint coordinator's command channel:
    /// workers wake the coordinator for pause-boundary saves. The task's
    /// join handle stays with `run_segmented`, which stops the coordinator
    /// before reclaim/verify/publish/cleanup.
    save_now_tx: mpsc::Sender<CoordinatorCmd>,
    /// Requested worker concurrency (handle API). Manual updates
    /// clamp to `[min_workers, max_workers]` and suspend the
    /// adaptive controller (manual precedence).
    desired_workers: AtomicU64,
    /// A manual concurrency override happened: the adaptive
    /// controller suspends for the job's remainder.
    manual_override: AtomicBool,
    /// Throttle events (429/503-style responses) observed by any worker
    ///.
    throttle_events: AtomicU64,
    /// Writer acknowledgement-latency histogram: per-window
    /// p50/p95 for the adaptive controller instead of a last-value sample.
    ack_latency: crate::metrics::histogram::LatencyHistogram,
    /// Outstanding-write depth histogram sampled at submit time.
    queue_depth: crate::metrics::histogram::DepthHistogram,
    /// Worker time blocked waiting for write-byte budget, in microseconds
    ///.
    budget_wait_us: AtomicU64,
    /// Test-only capture of adaptive controller window samples.
    #[cfg(test)]
    adaptive_samples: std::sync::Mutex<Vec<crate::control::adaptive::WindowSample>>,
    /// Live-tail split events observed by this job:
    /// every successful `split_tail` increments this counter.
    splits: AtomicU64,
    /// Range requests issued by any worker.
    segment_requests: AtomicU64,
    /// Per-worker provisioning state keyed by the stable worker index
    ///: `0` = not provisioned, `1` = parked/idle (no lease),
    /// `2` = actively holding a lease. Written only by the owning worker;
    /// read by the gauges below. Sized to `max_workers`.
    worker_states: Vec<AtomicU8>,
    /// Per-worker live writer-lane flag: the owning worker sets
    /// it when it spawns its blocking lane and clears it after the lane's
    /// shutdown join returns, so `writer_lanes_alive` tracks real blocking
    /// writer threads.
    worker_lane_live: Vec<AtomicBool>,
    /// Duration of the last coordinator checkpoint save, in microseconds
    ///. `0` means no save has completed yet.
    last_checkpoint_save_us: AtomicU64,
    /// Configured bounds for manual concurrency control.
    min_workers: u64,
    max_workers: u64,
    /// Negotiated wire protocol of the probe response:
    /// `true` when HTTP/2 was actually negotiated — additional active
    /// workers are streams multiplexed over one connection; `false` for
    /// HTTP/1, where each additional active worker means an additional
    /// physical connection subject to the connector's permits.
    protocol_is_h2: bool,
    /// Per-origin physical connection allowance: the connector
    /// enforces it; on HTTP/1 the adaptive controller never probes desired
    /// beyond it — a growth probe into blocked permits is wasted.
    per_origin_connection_cap: u32,
    /// Engine-global physical connection allowance: also gates
    /// HTTP/1 growth probes alongside the per-origin cap.
    global_connection_cap: u32,
    /// Controller-shared origin registry: request
    /// admission and shared throttle feedback, keyed by the normalized
    /// FINAL origin of the probe-resolved URL. A `None` key (unparseable
    /// origin) skips admission; the per-job backoff gate remains.
    origin_registry: Arc<OriginRegistry>,
    origin_key: Option<String>,
    /// Ready-work sizing: the opt-in Automatic selector keeps
    /// `ready_work_factor × desired` unclaimed leases pending; disabled for
    /// Explicit sizing (which keeps its configured meaning).
    ready_work_sizing: bool,
    ready_work_factor: u64,
    /// The divisor last applied to the scheduler policy (avoids re-locking
    /// writes on every acquire; workers apply changes under their existing
    /// scheduler lock).
    applied_ready_divisor: AtomicU64,
    /// Hot-path lease progress (§13.3): one atomic cell per
    /// worker. A worker publishing durable-through offsets for its current
    /// lease writes `(lease_id, generation, durable_through)` into its own
    /// cell with Relaxed atomics — no scheduler lock on the chunk path.
    /// The scheduler reconciles at lease boundaries (complete/fail/split)
    /// and at the checkpoint cadence.
    worker_progress: Vec<Arc<LeaseProgress>>,
}

/// One worker's published progress record (§13.3): read as ONE coherent
/// observation — lease id, generation and written-through offset always come
/// from the same publication, never mixed across generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseRecord {
    /// Current lease id (`0` = none/cleared).
    pub lease_id: u64,
    /// Generation the lease was acquired under.
    pub generation: u64,
    /// Lease start offset (for reconciliation).
    pub lease_start: u64,
    /// Written-through offset (exclusive) acknowledged by the output path.
    /// Written means OS-acknowledged page-cache writes; this is NOT a
    /// durability claim (checkpoint saves synchronize separately).
    pub written_through: u64,
    /// Receipt high-watermark (exclusive): how far the worker has
    /// pulled payload for this lease from the network, regardless of write
    /// acknowledgement. The live-tail split uses it as the boundary so the
    /// split lease never re-requests received/queued bytes.
    pub received_through: u64,
}

/// One worker's in-flight lease progress (§13.3).
///
/// Single-writer sequence-counter cell: the writer (one worker task owns
/// this cell) stores an odd sequence, then the fields, then an even
/// sequence; readers retry when the two sequence reads differ or are odd.
/// **Every access uses `SeqCst`**: one global order prevents a reader from
/// treating mixed publications as one record, even across clear/reuse, and
/// write acknowledgment happens before publication via the worker's task
/// continuation. Sequence wrap is unreachable in practice (u64, +2 per
/// publish); a wrap would force readers to retry until a fresh publication
/// lands — the writer never resets while readers exist.
#[derive(Debug, Default)]
pub struct LeaseProgress {
    sequence: AtomicU64,
    lease_id: AtomicU64,
    generation: AtomicU64,
    lease_start: AtomicU64,
    written_through: AtomicU64,
    received_through: AtomicU64,
}

impl LeaseProgress {
    /// Single-writer publication: odd sequence → fields → even
    /// sequence, all `SeqCst`.
    fn publish(&self, record: LeaseRecord) {
        let seq = self.sequence.load(Ordering::SeqCst);
        self.sequence.store(seq | 1, Ordering::SeqCst);
        self.lease_id.store(record.lease_id, Ordering::SeqCst);
        self.generation.store(record.generation, Ordering::SeqCst);
        self.lease_start.store(record.lease_start, Ordering::SeqCst);
        self.written_through
            .store(record.written_through, Ordering::SeqCst);
        self.received_through
            .store(record.received_through, Ordering::SeqCst);
        self.sequence.store((seq | 1) + 1, Ordering::SeqCst);
    }

    /// Test hook mirroring [`Self::publish`] for scheduler reconciliation
    /// tests (the real publish is chunk-path-internal).
    #[cfg(test)]
    pub(crate) fn test_publish(
        &self,
        lease_id: crate::scheduler::LeaseId,
        generation: u64,
        lease_start: u64,
        written_through: u64,
    ) {
        self.publish(LeaseRecord {
            lease_id,
            generation,
            lease_start,
            written_through,
            received_through: written_through,
        });
    }

    fn clear(&self) {
        self.publish(LeaseRecord {
            lease_id: 0,
            generation: 0,
            lease_start: 0,
            written_through: 0,
            received_through: 0,
        });
    }

    /// One coherent snapshot: retry when the two sequence reads
    /// differ or are odd; `(0, .., ..)` records read as idle.
    pub(crate) fn snapshot(&self) -> Option<LeaseRecord> {
        loop {
            let seq1 = self.sequence.load(Ordering::SeqCst);
            if seq1 % 2 == 1 {
                // Mid-publication: the writer is between stores.
                std::hint::spin_loop();
                continue;
            }
            let record = LeaseRecord {
                lease_id: self.lease_id.load(Ordering::SeqCst),
                generation: self.generation.load(Ordering::SeqCst),
                lease_start: self.lease_start.load(Ordering::SeqCst),
                written_through: self.written_through.load(Ordering::SeqCst),
                received_through: self.received_through.load(Ordering::SeqCst),
            };
            let seq2 = self.sequence.load(Ordering::SeqCst);
            if seq1 == seq2 {
                return if record.lease_id == 0 {
                    None
                } else {
                    Some(record)
                };
            }
            // Torn or concurrent publication: retry.
            std::hint::spin_loop();
        }
    }
}

/// Terminal failure state: a separate atomic fatal
/// flag gives workers a cheap `Acquire` check on the chunk path — no
/// asynchronous error lock per chunk. Installing a terminal failure takes
/// the small mutex, sets the error only if absent (first-wins: a racing
/// failure cannot replace the first recorded error), then publishes the
/// flag with `Release`. The controller reads the error only after workers
/// and the coordinator have joined.
#[derive(Debug, Default)]
struct FatalState {
    flag: AtomicBool,
    error: std::sync::Mutex<Option<DownloadError>>,
}

impl FatalState {
    /// Cheap chunk-path check (`Acquire`).
    fn is_fatal(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    /// Install the terminal error; returns true when THIS call recorded it
    /// (first-wins ownership).
    fn install(&self, error: DownloadError) -> bool {
        let mut guard = self.error.lock().expect("fatal error lock");
        if guard.is_some() {
            return false;
        }
        *guard = Some(error);
        drop(guard);
        // Never publish the flag before the error object is in place.
        self.flag.store(true, Ordering::Release);
        true
    }

    /// The authoritative terminal error (read once, after joins).
    fn take(&self) -> Option<DownloadError> {
        self.error.lock().expect("fatal error lock").take()
    }
}

impl SegmentedJob {
    /// Whether no leases are active (all work done or in pending) — a
    /// direct, allocation-free scheduler query.
    async fn active_leases_empty(&self) -> bool {
        !self.scheduler.lock().await.has_active()
    }

    /// The receipt high-watermark published for `lease_id` by any worker
    /// cell: the live-tail split boundary must not re-request
    /// bytes the original request already pulled. `0` when no cell tracks
    /// the lease (the split then falls back to the acknowledged frontier).
    fn cell_received_through(&self, lease_id: u64) -> u64 {
        self.worker_progress
            .iter()
            .filter_map(|cell| cell.snapshot())
            .filter(|record| record.lease_id == lease_id)
            .map(|record| record.received_through)
            .max()
            .unwrap_or(0)
    }

    /// Range requests issued so far.
    pub fn segment_requests(&self) -> u64 {
        self.segment_requests.load(Ordering::Relaxed)
    }

    /// Live-tail splits performed so far.
    pub fn live_splits(&self) -> u64 {
        self.splits.load(Ordering::Relaxed)
    }

    /// Set the desired worker count at runtime. Excess workers settle their
    /// leases and exit on the next loop. Returns the APPLIED count after
    /// clamping to the configured worker bounds, so callers report what
    /// actually took effect rather than what was requested.
    pub fn set_desired_workers(&self, n: u64) -> u64 {
        // Manual control is clamped to the configured bounds; a request
        // outside them is partially honored rather than rejected.
        let clamped = n.clamp(self.min_workers, self.max_workers);
        self.desired_workers.store(clamped, Ordering::Relaxed);
        // A manual override suspends the adaptive controller for the job's
        // remainder: the operator's count must not be second-guessed by
        // later automatic probes.
        self.manual_override.store(true, Ordering::Relaxed);
        // A concurrency change is a scheduler transition: parked workers
        // above or below the new desired count must wake so provisioning
        // converges promptly.
        self.notify_transition();
        clamped
    }

    /// The adaptive controller's internal adjustment: does NOT
    /// mark a manual override.
    fn adjust_desired_workers(&self, n: u64) {
        let clamped = n.clamp(self.min_workers, self.max_workers);
        self.desired_workers.store(clamped, Ordering::Relaxed);
        self.notify_transition();
    }

    /// The ready-work divisor for a desired count:
    /// `ready_work_factor × desired`; `0` when the opt-in Automatic sizing
    /// is not active (Explicit keeps its configured meaning).
    fn ready_work_divisor_for(&self, desired: u64) -> u64 {
        if !self.ready_work_sizing {
            return 0;
        }
        self.ready_work_factor.max(1).saturating_mul(desired.max(1))
    }

    /// Actual active/idle worker counts: active workers hold a
    /// lease right now; idle workers are provisioned and parked without one.
    /// Dormant workers above the desired count hold no capacity and are not
    /// counted. The controller weights these counts by interval so a
    /// momentary sample cannot stand in for a whole window.
    fn worker_activity_counts(&self) -> (u64, u64) {
        let mut active = 0u64;
        let mut idle = 0u64;
        for state in &self.worker_states {
            match state.load(Ordering::Relaxed) {
                2 => active += 1,
                1 => idle += 1,
                _ => {}
            }
        }
        (active, idle)
    }

    /// Record one writer acknowledgement-latency sample.
    fn record_ack_latency(&self, latency: Duration) {
        self.ack_latency
            .record_us(u64::try_from(latency.as_micros()).unwrap_or(u64::MAX));
    }

    /// Record the outstanding-write depth observed at submit time.
    fn record_queue_depth(&self, depth: u64) {
        self.queue_depth.record(depth);
    }

    /// Accumulate worker time blocked on write-byte budget.
    fn add_budget_wait(&self, wait: Duration) {
        let us = u64::try_from(wait.as_micros()).unwrap_or(u64::MAX);
        self.budget_wait_us.fetch_add(us, Ordering::Relaxed);
    }

    /// Total worker microseconds blocked on write-byte budget.
    #[must_use]
    pub fn budget_wait_us(&self) -> u64 {
        self.budget_wait_us.load(Ordering::Relaxed)
    }

    /// Writer acknowledgement-latency samples recorded so far.
    #[must_use]
    pub fn ack_latency_samples(&self) -> u64 {
        self.ack_latency.samples()
    }

    /// Outstanding-write depth samples recorded so far.
    #[must_use]
    pub fn queue_depth_samples(&self) -> u64 {
        self.queue_depth.samples()
    }

    /// Test-only capture of the controller's window samples: lets
    /// in-module tests assert the inputs the controller actually saw.
    #[cfg(test)]
    pub(crate) fn adaptive_sample_log(&self) -> Vec<crate::control::adaptive::WindowSample> {
        self.adaptive_samples
            .lock()
            .map(|samples| samples.clone())
            .unwrap_or_default()
    }

    /// Cumulative acknowledgement-latency bucket snapshot.
    fn ack_latency_snapshot(&self) -> crate::metrics::histogram::BucketSnapshot {
        self.ack_latency.snapshot()
    }

    /// Cumulative outstanding-depth bucket snapshot.
    fn queue_depth_snapshot(&self) -> crate::metrics::histogram::BucketSnapshot {
        self.queue_depth.snapshot()
    }

    /// Whether a manual override is active.
    fn is_manual_override(&self) -> bool {
        self.manual_override.load(Ordering::Relaxed)
    }

    /// Throttle events since job start.
    fn throttle_events(&self) -> u64 {
        self.throttle_events.load(Ordering::Relaxed)
    }

    /// Publish a scheduler-state transition: called AFTER the
    /// mutating mutation completed while holding (or having held) the
    /// scheduler serialization — parked workers recheck state on wake.
    /// Publish a scheduler-state transition: called AFTER the
    /// state mutation completed. Public for the runtime-control handle.
    pub fn notify_transition(&self) {
        // `send_modify` takes `&self` (tokio watch has interior mutability)
        // and always marks every receiver changed.
        self.revision_tx.send_modify(|r| {
            *r = r.wrapping_add(1);
        });
    }

    /// Subscribe to scheduler-state transitions (one receiver per worker).
    fn subscribe_revisions(&self) -> tokio::sync::watch::Receiver<u64> {
        self.revision_tx.as_ref().subscribe()
    }

    /// Update the job rate limit at runtime (§18.2): mutates the
    /// stable bucket in place; takes effect on the next acquire. `0` =
    /// unlimited.
    pub fn set_rate(&self, bytes_per_second: u64) {
        self.rate_bucket.set_rate(bytes_per_second);
    }

    /// The job's configured rate (`None` = unlimited).
    #[must_use]
    pub fn rate_limit(&self) -> Option<u64> {
        let rate = self.rate_bucket.rate();
        (rate != 0).then_some(rate)
    }

    /// Acquire tokens for `len` payload bytes, sleeping when the bucket
    /// gates (§18.2: payload bytes only, no busy wait, no outer lock —
    /// the bucket checks its atomic limit before the internal state lock).
    async fn acquire_rate(&self, len: u64) {
        // Aggregate job and engine-global levels (§18): the slowest level's
        // wait governs, computed by the one shared combiner so this path
        // cannot diverge from the hierarchical limiter or the sequential
        // path. Both `acquire` calls early-return without touching their
        // mutex while unlimited. Waiting stops on cancellation (prompt
        // pause/cancel interruption); the worker loop observes the cancel
        // on its next check.
        let job_wait = self.rate_bucket.acquire(len).wait;
        let global_wait = self.global_rate_bucket.acquire(len).wait;
        let wait = crate::control::rate_limit::dominant_wait([job_wait, global_wait]);
        if let Some(wait) = wait {
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = self.cancel.cancelled() => {}
            }
        }
    }

    #[must_use]
    pub fn desired_workers(&self) -> u64 {
        self.desired_workers.load(Ordering::Relaxed)
    }

    /// Split events observed so far.
    #[must_use]
    pub fn split_count(&self) -> u64 {
        self.splits.load(Ordering::Relaxed)
    }

    /// Configured worker capacity: the number of provisioned
    /// async tasks; the desired count floats within `[min_workers, this]`.
    #[must_use]
    pub fn max_workers(&self) -> u64 {
        self.max_workers
    }

    /// Workers currently holding a lease (the *actual* active
    /// gauge, distinct from `desired_workers`). Dormant or parked workers
    /// publish no lease record and are not counted.
    #[must_use]
    pub fn active_workers(&self) -> u64 {
        self.worker_progress
            .iter()
            .filter(|cell| cell.snapshot().is_some())
            .count() as u64
    }

    /// Duration of the last completed checkpoint save in microseconds, or
    /// `None` before the first save. Values below
    /// one microsecond report as one microsecond.
    #[must_use]
    pub fn last_checkpoint_save_us(&self) -> Option<u64> {
        match self.last_checkpoint_save_us.load(Ordering::Relaxed) {
            0 => None,
            v => Some(v),
        }
    }

    /// Workers with a provisioned task: the *actual* capacity,
    /// which may trail `desired_workers` until provisioning matches it.
    #[must_use]
    pub fn provisioned_workers(&self) -> u64 {
        self.worker_states
            .iter()
            .filter(|st| st.load(Ordering::Relaxed) != 0)
            .count() as u64
    }

    /// Workers parked/idle without a lease: provisioned minus
    /// active — includes dormant workers above the desired count.
    #[must_use]
    pub fn parked_workers(&self) -> u64 {
        self.worker_states
            .iter()
            .filter(|st| st.load(Ordering::Relaxed) == 1)
            .count() as u64
    }

    /// Per-worker state setter used by `worker_loop`: `0` not
    /// provisioned, `1` parked/idle, `2` active lease. Index-stable: the
    /// state array position is the worker index.
    pub(crate) fn set_worker_state(&self, worker_idx: usize, state: u8) {
        if let Some(st) = self.worker_states.get(worker_idx) {
            st.store(state, Ordering::Relaxed);
        }
    }

    /// Live blocking writer lanes: threads actually serving
    /// positional writes right now.
    #[must_use]
    pub fn writer_lanes_alive(&self) -> u64 {
        self.worker_lane_live
            .iter()
            .filter(|f| f.load(Ordering::Relaxed))
            .count() as u64
    }

    pub(crate) fn set_worker_lane_live(&self, worker_idx: usize, live: bool) {
        if let Some(f) = self.worker_lane_live.get(worker_idx) {
            f.store(live, Ordering::Relaxed);
        }
    }

    fn take_fatal(&self) -> Option<DownloadError> {
        self.fatal.take()
    }

    async fn completed_ranges(&self) -> Vec<(u64, u64)> {
        self.scheduler.lock().await.completed_ranges()
    }

    async fn is_complete(&self) -> bool {
        self.scheduler.lock().await.is_complete()
    }

    /// Whether the scheduler is finished (no pending, no active) — the
    /// persistent pool's job-done condition.
    async fn is_finished(&self) -> bool {
        self.scheduler.lock().await.is_finished()
    }
}

/// A completed segmented transfer's accounting (before verify/commit).
pub struct SegmentedOutcome {
    pub status: ResultStatus,
    pub error: Option<DownloadError>,
    pub network_bytes: u64,
    pub reused_bytes: u64,
    /// Unique newly completed file bytes (real counter).
    pub completed_bytes: u64,
    /// Wasted/retransmitted network bytes (real counter).
    pub wasted_bytes: u64,
    /// Retry attempts charged (real counter).
    pub retries: u64,
    /// Range requests issued.
    pub segment_requests: u64,
    /// Live-tail splits performed.
    pub live_splits: u64,
    pub total_size: u64,
    pub elapsed: Duration,
    pub validators: crate::http::validators::ResourceValidators,
    pub warnings: Vec<String>,
    /// Completed ranges at terminal time (for pause/keep persistence).
    pub completed_ranges: Vec<(u64, u64)>,
}

/// Run a segmented download over a validated probe result.
///
/// The caller has already checked eligibility (§10.3), opened the sink,
/// and validated resume state. This drives the worker pool to a terminal
/// condition; verification and commit stay with the caller (§16/§14.6).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_segmented(
    execution: HttpExecution,
    config: &EngineConfig,
    request: &DownloadRequest,
    state: &Arc<StateMachine>,
    session: &mut OutputSession,
    store: &Arc<dyn CheckpointStore>,
    identity: &str,
    meta: &ProbeMetadata,
    total_size: u64,
    counters: Arc<JobCounters>,
    hub: &SharedHub,
    cancel: CancellationToken,
    cancel_mode: Arc<std::sync::atomic::AtomicU8>,
    start_offset_ranges: Vec<(u64, u64)>,
    started: Instant,
    handle_cell: Option<Arc<std::sync::OnceLock<Arc<SegmentedJob>>>>,
    initial_rate_bucket: Arc<crate::control::rate_limit::TokenBucket>,
    origin_registry: Arc<OriginRegistry>,
    global_rate_bucket: Arc<crate::control::rate_limit::TokenBucket>,
) -> SegmentedOutcome {
    let mut warnings: Vec<String> = vec![];
    // Lease sizing per configuration: the explicit
    // `initial_segment_size` is honored (previously ignored in favor of
    // `max_segment_size`); the opt-in automatic selector derives the target
    // from remaining coverage and the initial active workers.
    let target = match config.transfer.segment_sizing {
        crate::config::SegmentSizing::Explicit => {
            TargetSelector::Explicit(config.transfer.initial_segment_size)
        }
        crate::config::SegmentSizing::Automatic => TargetSelector::Automatic {
            initial_workers: u64::from(config.transfer.max_workers.max(1)),
            oversubscription: config.transfer.auto_oversubscription,
        },
        crate::config::SegmentSizing::Duration { duration_ms } => TargetSelector::Duration {
            duration_ms,
            seed_size: config.transfer.initial_segment_size,
        },
    };
    let mut policy = SchedulerPolicy::with_target(
        config.transfer.min_segment_size,
        config.transfer.max_segment_size,
        target,
        256 * 1024,
    );
    // Adaptive mode starts at `min_workers` and probes
    // upward; fixed mode keeps the configured fixed concurrency.
    let adaptive = config.transfer.concurrency_mode == crate::config::ConcurrencyMode::Adaptive;
    let desired = if adaptive {
        u64::from(config.transfer.min_workers.max(1))
    } else {
        u64::from(config.transfer.max_workers.max(1))
    };
    // Ready-work target: with the opt-in Automatic
    // sizing, keep roughly `oversubscription × desired` unclaimed leases
    // pending so workers acquire unclaimed ranges instead of splitting live
    // tails. Explicit sizing keeps its configured meaning (no ready-work
    // cap). The initial divisor seeds the policy; workers refresh it under
    // their acquire lock as the desired count changes.
    if matches!(
        config.transfer.segment_sizing,
        crate::config::SegmentSizing::Automatic | crate::config::SegmentSizing::Duration { .. }
    ) {
        policy.ready_work_divisor = config
            .transfer
            .auto_oversubscription
            .max(1)
            .saturating_mul(desired.max(1));
    }
    let scheduler = SegmentScheduler::initialize(total_size, &start_offset_ranges, policy);
    // Progress cells cover the full capacity (all provisioned workers),
    // not just the initial desired count.
    let worker_progress: Vec<Arc<LeaseProgress>> =
        (0..u64::from(config.transfer.max_workers.max(1)).max(16))
            .map(|_| Arc::new(LeaseProgress::default()))
            .collect();
    let sync_capability = match session.sync_capability() {
        Ok(cap) => Some(cap),
        Err(error) => {
            let snapshot = counters.fold();
            return SegmentedOutcome {
                status: ResultStatus::Failed,
                error: Some(error.0),
                network_bytes: snapshot.network_bytes,
                reused_bytes: snapshot.reused_bytes,
                completed_bytes: snapshot.completed_bytes,
                wasted_bytes: snapshot.wasted_bytes,
                retries: snapshot.retries,
                segment_requests: 0,
                live_splits: 0,
                total_size,
                elapsed: started.elapsed(),
                validators: meta.validators.clone(),
                warnings,
                completed_ranges: start_offset_ranges,
            };
        }
    };
    // The checkpoint coordinator channel exists before the job so workers
    // can wake it; the join handle stays with run_segmented.
    let (save_now_tx, save_now_rx) = mpsc::channel(8);
    // The versioned scheduler-state signal.
    let (revision_tx, _revision_rx_init) = tokio::sync::watch::channel(0u64);
    let job = Arc::new(SegmentedJob {
        scheduler: AsyncMutex::new(scheduler),
        origin_backoff_until: AsyncMutex::new(None),
        fatal: FatalState::default(),
        cancel: cancel.clone(),
        hub: hub.clone(),
        counters: counters.clone(),
        rate_bucket: initial_rate_bucket,
        global_rate_bucket,
        total_size,
        validators: meta.validators.clone(),
        sync: sync_capability,
        durability: config.transfer.durability,
        revision_tx: Arc::new(revision_tx),
        save_now_tx,
        desired_workers: AtomicU64::new(desired),
        manual_override: AtomicBool::new(false),
        throttle_events: AtomicU64::new(0),
        ack_latency: crate::metrics::histogram::LatencyHistogram::new(),
        queue_depth: crate::metrics::histogram::DepthHistogram::new(),
        budget_wait_us: AtomicU64::new(0),
        #[cfg(test)]
        adaptive_samples: std::sync::Mutex::new(Vec::new()),
        splits: AtomicU64::new(0),
        segment_requests: AtomicU64::new(0),
        last_checkpoint_save_us: AtomicU64::new(0),
        worker_states: (0..config.transfer.max_workers.max(1) as usize)
            .map(|_| AtomicU8::new(0))
            .collect(),
        worker_lane_live: (0..config.transfer.max_workers.max(1) as usize)
            .map(|_| AtomicBool::new(false))
            .collect(),
        min_workers: u64::from(config.transfer.min_workers.max(1)),
        max_workers: u64::from(config.transfer.max_workers.max(1)),
        protocol_is_h2: meta.http_version == "HTTP/2.0",
        per_origin_connection_cap: config.max_connections_per_origin,
        origin_registry,
        origin_key: crate::control::origin::normalized_origin(&meta.final_url),
        global_connection_cap: config.max_connections_total,
        ready_work_sizing: matches!(
            config.transfer.segment_sizing,
            crate::config::SegmentSizing::Automatic | crate::config::SegmentSizing::Duration { .. }
        ),
        ready_work_factor: config.transfer.auto_oversubscription.max(1),
        applied_ready_divisor: AtomicU64::new(u64::MAX),
        worker_progress,
    });
    // One job-level checkpoint coordinator: owns interval timing,
    // reconciliation and persistence; workers never save on the chunk path.
    let coordinator_join = tokio::spawn(coordinator_loop(
        job.clone(),
        store.clone(),
        identity.to_string(),
        config.checkpoint_flush_interval,
        save_now_rx,
    ));
    // Yield once so the coordinator's immediate save (admitted coverage on
    // resumed jobs, §15.5) runs before the transfer's chunk loop monopolizes
    // the worker.
    tokio::time::sleep(Duration::from_millis(1)).await;
    // Publish the live job for the handle's runtime controls.
    if let Some(cell) = &handle_cell {
        let _ = cell.set(job.clone());
    }

    // Provision the FULL capacity (`max_workers`) of lightweight
    // async tasks up front; workers above the desired count park dormant on
    // the revision signal and reactivate without rebuilding when the
    // desired count rises.
    // Fixed mode starts with desired == max, so its visible behavior is
    // unchanged; adaptive mode can now actually grow.
    let worker_count = job.max_workers() as usize;
    let mut handles = Vec::with_capacity(worker_count);
    let mut writers = match session.share_write_handles(worker_count) {
        Ok(writers) => writers,
        Err(error) => {
            let snapshot = counters.fold();
            return SegmentedOutcome {
                status: ResultStatus::Failed,
                error: Some(error.0),
                network_bytes: snapshot.network_bytes,
                reused_bytes: snapshot.reused_bytes,
                completed_bytes: snapshot.completed_bytes,
                wasted_bytes: snapshot.wasted_bytes,
                retries: snapshot.retries,
                segment_requests: job.segment_requests(),
                live_splits: job.live_splits(),
                total_size,
                elapsed: started.elapsed(),
                validators: meta.validators.clone(),
                warnings,
                completed_ranges: start_offset_ranges,
            };
        }
    };
    // Internal legacy/new write-path switch. The
    // pipelined path routes worker writes through the shared bounded
    // executor with byte budgets and per-lease acknowledged frontiers; the
    // default remains the legacy writer lanes until the phase 2 gate
    // proves parity.
    let pipelined = config.write_executor.pipeline_writes;
    let write_executor = pipelined.then(|| WriteExecutor::new(&config.write_executor));
    let write_budgets = pipelined.then(|| {
        WriteBudgets::new(
            config.write_budget.global_max_bytes,
            config.write_budget.job_max_bytes,
            config.write_budget.worker_read_ahead_bytes,
        )
    });
    // Workers own their writer-lane lifecycle — each receives one
    // write-only capability and spawns its blocking lane on first activation,
    // releasing it on dormancy or exit. `worker join` therefore implies every
    // lane is shut down and every capability clone dropped before reclaim.
    // (Pipelined workers lend the same capability to their executor session
    // instead; the pool bounds blocking threads, so no lazy-lane dance.)
    for worker_idx in 0..worker_count {
        let execution = execution.clone();
        let classifier = RetryClassifier::new(config.retry.clone());
        let job = job.clone();
        let output = writers.pop().expect("one output handle per worker");
        let writer = match (&write_executor, &write_budgets) {
            (Some(executor), Some(budgets)) => {
                let (session, completions) = executor.session(0, Arc::new(output));
                WorkerWriter::Pipelined(Box::new(PipelinedWriter {
                    session,
                    completions,
                    budget: budgets.job(0),
                    frame_quantum: u64::from(config.read_buffer_size),
                    reservations: std::collections::HashMap::new(),
                }))
            }
            _ => WorkerWriter::Legacy { lane: None, output },
        };
        let req_spec = WorkerRequestSpec {
            url: meta.final_url.clone(),
            headers: request.headers.clone(),
            identity_encoding: true,
        };
        let counters_w = counters.clone();
        let identity_owned = identity.to_string();
        handles.push(tokio::spawn(async move {
            worker_loop(
                execution,
                classifier,
                job,
                writer,
                identity_owned,
                req_spec,
                counters_w,
                worker_idx,
                started,
            )
            .await
        }));
    }

    // Adaptive controller task: evaluates windowed
    // useful-goodput deltas and probes +1 conservatively. Manual overrides
    // (handle `set_concurrency`) suspend it for the job's remainder.
    // Adaptive controller. Its join handle is kept and awaited after the
    // workers exit — no late decision can race the terminal outcome, and
    // the task never leaks.
    let mut controller_join: Option<tokio::task::JoinHandle<()>> = None;
    let controller_stop_tx;
    if adaptive {
        let controller_job = job.clone();
        let controller_counters = counters.clone();
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        controller_stop_tx = Some(stop_tx);
        controller_join = Some(tokio::spawn(adaptive_controller_loop(
            controller_job,
            controller_counters,
            stop_rx,
        )));
    } else {
        controller_stop_tx = None;
    }

    let mut outcome_error: Option<DownloadError> = None;
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if outcome_error.is_none() {
                    outcome_error = Some(e);
                }
            }
            Err(join_err) => {
                if outcome_error.is_none() {
                    outcome_error = Some(DownloadError::Protocol(format!(
                        "worker task failed: {join_err}"
                    )));
                }
            }
        }
    }
    drop(writers);
    // Shut the pipelined executor down after every worker
    // detached its session (each worker exit drains and releases its
    // capability), so pool threads are gone before the output is reclaimed.
    if let Some(executor) = write_executor {
        if let Err(error) = executor.shutdown().await {
            outcome_error.get_or_insert(error.0);
        }
    }
    // Stop and join the adaptive controller before the outcome is
    // computed — no decision can fire after the workers settled.
    if let Some(tx) = controller_stop_tx {
        let _ = tx.send(true);
    }
    if let Some(join) = controller_join {
        let _ = join.await;
    }
    // No separate lane join is needed — every worker shut its lane down
    // (and dropped its capability clone) before returning, so the joins
    // above already drained all blocking writer threads at this same
    // boundary.
    // Stop the checkpoint coordinator before reclaim/verify/publish/cleanup
    //: no save can race post-commit cleanup or a
    // stale checkpoint. All workers have joined, so no boundary save can
    // arrive afterwards; the stop acks after any in-flight save drained.
    let (stop_ack_tx, stop_ack_rx) = oneshot::channel();
    if job
        .save_now_tx
        .send(CoordinatorCmd::Stop { ack: stop_ack_tx })
        .await
        .is_ok()
    {
        let _ = stop_ack_rx.await;
    }
    let _ = coordinator_join.await;
    let ownership_reclaimed = match session.reclaim_exclusive() {
        Ok(()) => true,
        Err(error) => {
            outcome_error.get_or_insert(error.0);
            false
        }
    };

    // Cancelled: report cancelled with the scheduler's completed set.
    // All workers have joined, so terminal cleanup cannot race with saves:
    // the selected cancellation mode settles artifacts (§9.4) and delete
    // failures become warnings without rewriting the outcome (§9.2).
    if job.cancel.is_cancelled() {
        let completed = job.completed_ranges().await;
        let cleanup_warnings = if ownership_reclaimed {
            crate::job::controller::DownloadController::cleanup_cancelled(
                crate::job::controller::CancelMode::from_u8(
                    cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                ),
                session,
                store.as_ref(),
                identity,
            )
        } else {
            vec![]
        };
        warnings.extend(cleanup_warnings);
        // One consistent fold for the whole terminal record.
        let snap = counters.fold();
        return SegmentedOutcome {
            status: if ownership_reclaimed {
                ResultStatus::Cancelled
            } else {
                ResultStatus::Failed
            },
            error: outcome_error.or(Some(DownloadError::Cancelled)),
            network_bytes: snap.network_bytes,
            reused_bytes: snap.reused_bytes,
            completed_bytes: snap.completed_bytes,
            wasted_bytes: snap.wasted_bytes,
            retries: snap.retries,
            segment_requests: job.segment_requests(),
            live_splits: job.live_splits(),
            total_size,
            elapsed: started.elapsed(),
            validators: job.validators.clone(),
            warnings,
            completed_ranges: completed,
        };
    }
    if let Some(f) = job.take_fatal() {
        outcome_error.get_or_insert(f);
    }
    let completed = job.completed_ranges().await;
    let complete = job.is_complete().await;
    let snap = counters.fold();
    if let Some(e) = outcome_error {
        let _ = state.transition(JobState::Failing);
        let _ = state.transition(JobState::Failed);
        return SegmentedOutcome {
            status: ResultStatus::Failed,
            error: Some(e),
            network_bytes: snap.network_bytes,
            reused_bytes: snap.reused_bytes,
            completed_bytes: snap.completed_bytes,
            wasted_bytes: snap.wasted_bytes,
            retries: snap.retries,
            segment_requests: job.segment_requests(),
            live_splits: job.live_splits(),
            total_size,
            elapsed: started.elapsed(),
            validators: job.validators.clone(),
            warnings,
            completed_ranges: completed,
        };
    }
    if !complete {
        let _ = state.transition(JobState::Failing);
        let _ = state.transition(JobState::Failed);
        return SegmentedOutcome {
            status: ResultStatus::Failed,
            error: Some(DownloadError::Protocol(
                "segmented transfer ended with incomplete coverage".into(),
            )),
            network_bytes: snap.network_bytes,
            reused_bytes: snap.reused_bytes,
            completed_bytes: snap.completed_bytes,
            wasted_bytes: snap.wasted_bytes,
            retries: snap.retries,
            segment_requests: job.segment_requests(),
            live_splits: job.live_splits(),
            total_size,
            elapsed: started.elapsed(),
            validators: job.validators.clone(),
            warnings,
            completed_ranges: completed,
        };
    }
    SegmentedOutcome {
        status: ResultStatus::Completed,
        error: None,
        network_bytes: snap.network_bytes,
        reused_bytes: snap.reused_bytes,
        completed_bytes: snap.completed_bytes,
        wasted_bytes: snap.wasted_bytes,
        retries: snap.retries,
        segment_requests: job.segment_requests(),
        live_splits: job.live_splits(),
        total_size,
        elapsed: started.elapsed(),
        validators: job.validators.clone(),
        warnings,
        completed_ranges: completed,
    }
}

/// Worker request material shared by all workers of one job.
#[derive(Debug, Clone)]
struct WorkerRequestSpec {
    url: String,
    headers: Vec<(String, String)>,
    identity_encoding: bool,
}

/// The per-worker write path . The legacy path keeps
/// one blocking lane per worker and awaits every write before reading the
/// next chunk; the pipelined path submits writes through the shared
/// bounded executor and keeps receiving under the byte budgets, with
/// per-lease acknowledged frontiers driving progress publication.
enum WorkerWriter {
    Legacy {
        lane: Option<crate::io::writer_lane::WriterLane>,
        output: crate::io::output_session::OutputWriteHandle,
    },
    Pipelined(Box<PipelinedWriter>),
}

/// One worker's pipelined write path: an executor session (its
/// own completion stream), the job write budget, the pre-read frame
/// quantum and the reservations for submitted-but-unsettled writes.
struct PipelinedWriter {
    session: WriteSession,
    completions: mpsc::UnboundedReceiver<WriteCompletion>,
    budget: JobWriteBudget,
    /// Bytes to reserve before polling the next body chunk (one frame
    /// quantum, design D2).
    frame_quantum: u64,
    /// Reservations covering queued+executing payload, keyed by
    /// (lease id, offset); released when the write settles.
    reservations: std::collections::HashMap<(u64, u64), (ByteReservation, std::time::Instant)>,
}

/// Errors inside one lease attempt.
enum WorkerError {
    Retryable {
        error: DownloadError,
        retry_after: Option<Duration>,
        /// Bytes received past the acknowledged written-through frontier:
        /// retransmitted overhead counted at the lease boundary.
        wasted: u64,
    },
    Fatal(DownloadError),
    GenerationChanged(DownloadError),
}

/// Map one classified HTTP failure (§32) onto the worker error taxonomy:
/// generation changes invalidate the whole job (§26); non-retryable
/// failures abort it; everything else retries the absorbed tail (§17.3)
/// with the server-provided timing when present (§17.2).
fn worker_error_from_failure(
    error: DownloadError,
    retry_after: Option<Duration>,
    classifier: &RetryClassifier,
    wasted: u64,
) -> WorkerError {
    match error.category() {
        crate::error::ErrorCategory::ResourceChanged => WorkerError::GenerationChanged(error),
        _ if classifier.retryable(&error) => WorkerError::Retryable {
            error,
            retry_after,
            wasted,
        },
        _ => WorkerError::Fatal(error),
    }
}

/// The worker loop (§13 steps 1-10).
#[allow(clippy::too_many_arguments)]
/// One provisioned worker: owns the lazy writer lane lifecycle —
/// the blocking lane is created on first activation and released on dormancy
/// or exit, so blocking writer threads track the desired count, not the
/// configured maximum.
async fn worker_loop(
    execution: HttpExecution,
    classifier: RetryClassifier,
    job: Arc<SegmentedJob>,
    mut writer: WorkerWriter,
    identity: String,
    req_spec: WorkerRequestSpec,
    counters: Arc<JobCounters>,
    worker_idx: usize,
    started: Instant,
) -> Result<(), DownloadError> {
    job.set_worker_state(worker_idx, 1);
    let result = worker_cycle(
        execution,
        classifier,
        &job,
        &mut writer,
        identity,
        req_spec,
        counters,
        worker_idx,
        started,
    )
    .await;
    // Release the write path on exit (legacy or pipelined): blocking
    // threads end and capability clones drop before
    // this worker joins, so run_segmented's reclaim observes no live
    // writers. A shutdown failure means the blocking thread panicked —
    // surfaced via the first-wins fatal state, matching the old
    // outcome_error path.
    match writer {
        WorkerWriter::Legacy { lane, .. } => {
            if let Some(l) = lane {
                job.set_worker_lane_live(worker_idx, false);
                if let Err(error) = l.shutdown().await {
                    job.fatal.install(error.0);
                }
            }
        }
        WorkerWriter::Pipelined(pipelined) => {
            // Detach with discard: queued writes are
            // dropped, in-flight writes finish; draining the completion
            // stream releases every outstanding reservation so the job
            // budget never leaks bytes across worker lifecycles (the
            // pause/retry dispositions below rely on that).
            let PipelinedWriter {
                session,
                mut completions,
                mut reservations,
                ..
            } = *pipelined;
            session.detach(SessionDisposition::Discard).await;
            while let Some(completion) = completions.recv().await {
                reservations.remove(&(completion.lease_id, completion.offset));
            }
            drop(reservations);
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn worker_cycle(
    execution: HttpExecution,
    classifier: RetryClassifier,
    job: &Arc<SegmentedJob>,
    writer: &mut WorkerWriter,
    identity: String,
    req_spec: WorkerRequestSpec,
    counters: Arc<JobCounters>,
    worker_idx: usize,
    started: Instant,
) -> Result<(), DownloadError> {
    let _ = started;
    // Provisioning gauge: this task exists — mark it parked/idle
    // until it holds a lease. Index-stable per worker.
    job.set_worker_state(worker_idx, 1);
    let mut attempt: u32 = 0;
    // Versioned scheduler-state signal: parked workers wake
    // on transitions instead of polling on a fixed timer.
    let mut revisions = job.subscribe_revisions();

    loop {
        // Register the current revision BEFORE checking for work:
        // any transition after this mark re-notifies and wakes the park
        // below, so no transition is ever slept through. The guard drops
        // immediately — only the seen-version matters.
        let _seen_revision = {
            let seen = revisions.borrow_and_update();
            *seen
        };

        // Terminal conditions.
        if job.cancel.is_cancelled() {
            return Ok(());
        }
        if job.fatal.is_fatal() {
            return Ok(());
        }
        // All work settled: the job is done — persistent workers exit
        // together.
        if job.is_finished().await {
            return Ok(());
        }
        // Concurrency reduction: a worker above the desired
        // count deactivates — it holds no lease here (any in-flight lease
        // was settled by completing/failing before the next loop), so it
        // parks DORMANT instead of exiting (a later increase
        // reactivates it via the revision signal, without rebuilding).
        if job.desired_workers() <= u64::from(u32::try_from(worker_idx).unwrap_or(u32::MAX)) {
            // Dormant: release the blocking writer lane while
            // parked so writer threads track the desired count.
            if let WorkerWriter::Legacy { lane, .. } = writer {
                if let Some(l) = lane.take() {
                    job.set_worker_lane_live(worker_idx, false);
                    if let Err(error) = l.shutdown().await {
                        job.fatal.install(error.0);
                    }
                }
            }
            tokio::select! {
                changed = revisions.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                }
                _ = job.cancel.cancelled() => return Ok(()),
            }
            continue;
        }

        // Acquire a lease (short lock, §13.3).
        let lease = {
            let mut sched = job.scheduler.lock().await;
            // Ready-work divisor: track desired-count changes
            // under the lock the worker already holds for the acquire.
            let desired_now = job.desired_workers.load(Ordering::Relaxed);
            let want_divisor = job.ready_work_divisor_for(desired_now);
            if job
                .applied_ready_divisor
                .swap(want_divisor, Ordering::Relaxed)
                != want_divisor
            {
                sched.set_ready_work_divisor(want_divisor);
            }
            // No pending work: wait for tails/failures of live leases
            // instead of manufacturing splits every tick (§12.3 splits
            // serve a worker that would otherwise idle; a single split
            // attempt per idle pass is enough).
            sched.acquire()
        };
        let lease = match lease {
            Some(l) => l,
            None => {
                // No pending work; if nothing is active either, the job is
                // finished — exit on the next loop-top check.
                if job.active_leases_empty().await {
                    return Ok(());
                }
                // Other workers still hold work; opportunistically split a
                // big live tail once, then wait (§12.3). The split threshold
                // is scheduler policy, not a worker constant.
                let split = {
                    let mut sched = job.scheduler.lock().await;
                    let threshold = sched.policy().split_threshold;
                    let split = match sched.largest_splittable(threshold) {
                        Some(b) => {
                            let received = job.cell_received_through(b.id);
                            sched.split_tail(b.id, b.generation, threshold, received)
                        }
                        None => None,
                    };
                    if split.is_some() {
                        // New lease available for parked workers.
                        job.splits.fetch_add(1, Ordering::Relaxed);
                        job.notify_transition();
                    }
                    split
                };
                match split {
                    Some(tail) => tail,
                    None => {
                        // Park on the versioned state signal:
                        // wake on any transition (new pending, requeue,
                        // split eligibility via progress, desired-worker
                        // changes, fatal, resume, completion) or on
                        // cancellation. Recheck on wake — never spin.
                        tokio::select! {
                            changed = revisions.changed() => {
                                if changed.is_err() {
                                    // The signal channel closed: the job is
                                    // going away.
                                    return Ok(());
                                }
                            }
                            _ = job.cancel.cancelled() => return Ok(()),
                        }
                        let _ = _seen_revision;
                        continue;
                    }
                }
            }
        };

        job.hub
            .emit(crate::metrics::events::Event::SegmentStarted {
                worker: worker_idx,
                start: lease.start,
                end: lease.end,
            })
            .await;

        // Holding a lease (acquired or split): mark this worker active
        // right before the transfer so both acquisition paths
        // report identically.
        job.set_worker_state(worker_idx, 2);
        // Lazy writer lane: created on this worker's first
        // activation; the blocking thread lives until dormancy or exit.
        // Lazy writer lane: created on this worker's first
        // activation; the blocking thread lives until dormancy or exit.
        // Pipelined workers need no activation step (their session holds
        // the capability from attach; the shared pool bounds threads).
        if let WorkerWriter::Legacy { lane, output } = writer {
            if lane.is_none() {
                *lane = Some(crate::io::writer_lane::WriterLane::spawn(
                    output.clone_capability(),
                ));
                job.set_worker_lane_live(worker_idx, true);
            }
        }
        let result = transfer_lease(
            &execution,
            &classifier,
            job,
            &lease,
            writer,
            &identity,
            &req_spec,
            worker_idx,
            &mut revisions,
        )
        .await;

        match result {
            Ok(()) => {
                attempt = 0; // reset backoff on success
                             // Reconcile (crediting accepted coverage) then
                             // complete the lease.
                reconcile_and_credit(job).await;
                let mut sched = job.scheduler.lock().await;
                let _ = sched.complete(lease.id, lease.generation);
                job.worker_progress[worker_idx].clear();
                // The worker is idle again: state flips exactly
                // where the lease cell clears so the gauges never disagree.
                job.set_worker_state(worker_idx, 1);
                // Completion frees bytes / finishes the job.
                job.notify_transition();
            }
            Err(WorkerError::Retryable {
                error,
                retry_after,
                wasted,
            }) => {
                // Retry accounting at the lease boundary: charge
                // the received-but-lost gap to THIS worker's shard before
                // the tail-only requeue.
                if wasted > 0 {
                    if let Some(w) = counters.worker(worker_idx) {
                        w.add_wasted(wasted);
                    }
                }
                // Reconcile (credit accepted coverage) then
                // requeue: the acknowledged prefix completes, the tail
                // returns to pending (§17.3 tail-only retry) — one lock at
                // the retry boundary.
                reconcile_and_credit(job).await;
                {
                    let mut sched = job.scheduler.lock().await;
                    let _ = sched.fail(lease.id, lease.generation);
                    // Requeue adds pending work.
                    job.notify_transition();
                }
                job.worker_progress[worker_idx].clear();
                // Idle again after the retry requeue.
                job.set_worker_state(worker_idx, 1);
                if let Some(w) = counters.worker(worker_idx) {
                    w.add_retries(1);
                }
                // Coordinated origin backoff (§17.4): gate ALL workers.
                if matches!(
                    error.category(),
                    crate::error::ErrorCategory::Server | crate::error::ErrorCategory::RateLimited
                ) {
                    // Throttle signal for the adaptive controller.
                    job.throttle_events.fetch_add(1, Ordering::Relaxed);
                    // RetryClassifier-capped Retry-After: the
                    // coordinated window — per job AND shared across
                    // same-origin peers — never exceeds policy.
                    let capped = classifier.honor_retry_after(retry_after);
                    let earliest = capped.unwrap_or_else(|| classifier.backoff_delay(attempt));
                    let until = Instant::now() + earliest;
                    let mut gate = job.origin_backoff_until.lock().await;
                    if gate.is_none_or(|g| until > g) {
                        *gate = Some(until);
                    }
                    drop(gate);
                    if let (registry, Some(key)) = (&job.origin_registry, &job.origin_key) {
                        registry.report_throttle(key, capped, classifier.backoff_delay(attempt));
                    }
                }
                // Tail-only retry (§17.3): the lease was already failed
                // above with the absorbed prefix.
                job.hub
                    .emit(crate::metrics::events::Event::SegmentRetried {
                        worker: worker_idx,
                        start: lease.start,
                        end: lease.end,
                        error: error.to_string(),
                    })
                    .await;
                match classifier.decide(&error, attempt, retry_after) {
                    RetryDecision::Retry {
                        attempt: next,
                        delay,
                    } => {
                        attempt = next;
                        tokio::time::sleep(delay).await;
                    }
                    RetryDecision::GiveUp => {
                        job.fatal.install(DownloadError::RetryExhausted {
                            source: Box::new(error),
                        });
                        return Ok(());
                    }
                }
            }
            Err(WorkerError::Fatal(e)) => {
                reconcile_and_credit(job).await;
                {
                    let mut sched = job.scheduler.lock().await;
                    let _ = sched.fail(lease.id, lease.generation);
                    job.notify_transition();
                }
                job.worker_progress[worker_idx].clear();
                // Terminal failure wakes every parked worker.
                job.fatal.install(e);
                job.notify_transition();
                return Ok(());
            }
            Err(WorkerError::GenerationChanged(e)) => {
                // Resource changed mid-transfer (§26): invalidate all
                // leases; never mix generations.
                job.hub
                    .emit(crate::metrics::events::Event::ResourceChanged {
                        detail: e.to_string(),
                    })
                    .await;
                {
                    let mut sched = job.scheduler.lock().await;
                    let _ = sched.bump_generation();
                    // Generation invalidation is a transition.
                    job.notify_transition();
                }
                // Terminal failure wakes every parked worker.
                job.fatal.install(e);
                job.notify_transition();
                return Ok(());
            }
        }
    }
}

/// Reconcile worker cells into the scheduler and credit each cell's
/// accepted-delta to the OWNING worker's completed shard:
/// only scheduler-accepted bytes count as unique completed coverage, so
/// writes beyond a split-shrunk lease end are never double-counted. The
/// operation is idempotent (a second reconcile of the same progress credits
/// nothing).
async fn reconcile_and_credit(job: &Arc<SegmentedJob>) {
    let mut sched = job.scheduler.lock().await;
    let deltas = sched.absorb_worker_progress(&job.worker_progress);
    for (worker_idx, delta) in deltas.into_iter().enumerate() {
        if delta > 0 {
            if let Some(w) = job.counters.worker(worker_idx) {
                w.add_completed(delta);
            }
        }
    }
}

/// Transfer one lease: semantic ranged transfer (§32) -> validated body ->
/// positional writes -> progress reporting. The HTTP layer validates
/// status/range/generation before the body is returned; the worker only
/// consumes semantic chunks and decides retry/coordination policy.
#[allow(clippy::too_many_arguments)]
async fn transfer_lease(
    execution: &HttpExecution,
    classifier: &RetryClassifier,
    job: &Arc<SegmentedJob>,
    lease: &SegmentLease,
    writer: &mut WorkerWriter,
    identity: &str,
    req_spec: &WorkerRequestSpec,
    worker_idx: usize,
    revisions: &mut tokio::sync::watch::Receiver<u64>,
) -> Result<(), WorkerError> {
    let _ = (identity, req_spec);
    // Coordinated backoff gate (§17.4): wait until the earliest acceptable
    // retry time before issuing the request.
    loop {
        let wait = {
            let gate = job.origin_backoff_until.lock().await;
            gate.map(|until| until.saturating_duration_since(Instant::now()))
        };
        if let Some(d) = wait {
            if d > Duration::ZERO {
                tokio::time::sleep(d).await;
                continue;
            }
        }
        break;
    }

    // Shared-origin request admission: wait out the
    // origin's coordinated throttle deadline (shared with every peer job)
    // and take one fair FIFO request slot; the RAII permit releases on
    // success, failure and cancellation alike. The per-job gate above stays
    // as the compatibility fallback.
    let _origin_permit = match (&job.origin_registry, &job.origin_key) {
        (registry, Some(key)) => match registry.admit(key, &job.cancel).await {
            Ok(permit) => Some(permit),
            Err(e) => {
                return Err(if matches!(e, DownloadError::Cancelled) {
                    WorkerError::Fatal(e)
                } else {
                    WorkerError::Retryable {
                        error: e,
                        retry_after: None,
                        wasted: 0,
                    }
                });
            }
        },
        _ => None,
    };

    // Range-request diagnostic: one counter per issued request
    // (attempts included; retries are visible separately).
    job.segment_requests.fetch_add(1, Ordering::Relaxed);
    // Resume offset for tail retries (§17.3): the worker's own progress
    // cell holds the durable-through offset — read it lock-free (§13.3).
    // Fall back to the lease's scheduler-side next_offset when the cell is
    // empty (e.g., first attempt after acquire).
    let cell = &job.worker_progress[worker_idx];
    let start_from = match cell.snapshot() {
        // The worker's own progress record holds the acknowledged
        // written-through offset (one coherent record).
        Some(record) if record.lease_id == lease.id => {
            record.written_through.max(lease.next_offset)
        }
        _ => lease.next_offset,
    };
    // Semantic ranged transfer (§32): the intent carries the requested
    // range, established total, and expected validators (generation
    // identity, §5.2); HTTP validates the response before any body byte.
    let spec = RequestSpec {
        url: req_spec.url.clone(),
        headers: req_spec.headers.clone(),
        identity_encoding: req_spec.identity_encoding,
        ..RequestSpec::default()
    };
    let intent = TransferIntent::Range(RangeIntent {
        range: (start_from, lease.end),
        established_total: Some(job.total_size),
        // Expected validators issue the request conditionally (If-Range,
        // §11.3) and reject generation mixing (§26).
        expected_validators: Some(job.validators.clone()),
        full_response: FullResponsePolicy::InvalidRange,
    });
    let cancel = job.cancel.clone();
    let response = match execution
        .transfer(TransferRequest { spec, intent }, &cancel)
        .await
    {
        Ok(r) => r,
        Err(HttpFailure {
            error, retry_after, ..
        }) => {
            // No body bytes were received for this attempt: zero waste.
            return Err(worker_error_from_failure(error, retry_after, classifier, 0));
        }
    };
    let validated_start = response.start;
    let validated_end = response.end;
    let accepted_len = validated_end - validated_start + 1;
    // The write path diverges here: the legacy lane blocks the
    // worker per chunk; the pipelined path submits through the shared
    // executor. Both consume the SAME validated response body and share
    // the reconciliation/complete tail below.
    let mut body = response.body;
    let lane_handle = match writer {
        WorkerWriter::Legacy { lane, .. } => {
            let lane = lane.as_ref().expect("lane activated before transfer");
            Some(lane.handle())
        }
        WorkerWriter::Pipelined(_) => None,
    };
    let consume = match writer {
        WorkerWriter::Legacy { .. } => {
            consume_legacy_body(
                job,
                lease,
                &mut body,
                lane_handle.as_ref().expect("legacy lane"),
                worker_idx,
                revisions,
                validated_start,
                validated_end,
                classifier,
            )
            .await
        }
        WorkerWriter::Pipelined(pipelined) => {
            consume_pipelined_body(
                job,
                lease,
                &mut body,
                pipelined,
                worker_idx,
                revisions,
                validated_start,
                validated_end,
                classifier,
            )
            .await
        }
    };
    consume?;
    // Final acknowledgment: everything accepted is written. Reconcile this
    // worker's cell (crediting accepted coverage) before
    // completing the lease (§31).
    reconcile_and_credit(job).await;
    {
        let mut sched = job.scheduler.lock().await;
        let _ = sched.report_progress(lease.id, lease.generation, validated_start + accepted_len);
    }
    job.worker_progress[worker_idx].clear();
    job.hub
        .emit(crate::metrics::events::Event::SegmentCompleted {
            worker: lease.id as usize,
            start: validated_start,
            end: validated_end,
        })
        .await;
    // One completed origin request: the
    // post-cooldown probe signal for the shared registry.
    if let (registry, Some(key)) = (&job.origin_registry, &job.origin_key) {
        registry.report_success(key);
    }
    Ok(())
}

/// Legacy write path: one blocking lane per worker; the worker
/// awaits each write before reading the next chunk (one outstanding
/// payload per worker). Behavior is unchanged from the pre-executor
/// transfer loop.
#[allow(clippy::too_many_arguments)]
async fn consume_legacy_body(
    job: &Arc<SegmentedJob>,
    lease: &SegmentLease,
    body: &mut crate::http::execution::HttpBody,
    lane: &crate::io::writer_lane::LaneHandle,
    worker_idx: usize,
    revisions: &mut tokio::sync::watch::Receiver<u64>,
    validated_start: u64,
    validated_end: u64,
    classifier: &RetryClassifier,
) -> Result<(), WorkerError> {
    let cell = &job.worker_progress[worker_idx];
    let mut in_range_offset: u64 = 0;
    // Live-tail split safety: the lease end may SHRINK when an
    // idle worker splits this request's tail. The worker refreshes the end
    // on revision wakes and stops consuming at the shrunken boundary — the
    // discarded response tail is accounted as split waste, never written or
    // credited to the split lease.
    let mut effective_end = validated_end;
    {
        let sched = job.scheduler.lock().await;
        if let Some(end) = sched.lease_end(lease.id, lease.generation) {
            effective_end = effective_end.min(end);
        }
    }
    // Bounded body (§32): the configured read-idle policy and overrun
    // rejection live inside the body; the worker consumes one chunk at a
    // time and never sees frame types.
    loop {
        // Register the current revision BEFORE the read: a fatal
        // installed while this worker is parked mid-body publishes a
        // transition and wakes the select below — no lost convergence.
        let _seen_revision = {
            let seen = revisions.borrow_and_update();
            *seen
        };
        if job.cancel.is_cancelled() {
            return Err(WorkerError::Fatal(DownloadError::Cancelled));
        }
        if job.fatal.is_fatal() {
            return Err(WorkerError::Fatal(DownloadError::Cancelled));
        }
        let event = tokio::select! {
            event = body.next_chunk(&job.cancel) => event,
            changed = revisions.changed() => {
                // A transition while parked: fatal must converge this
                // worker; other transitions (progress reconciliation,
                // saves, live-tail splits) just re-poll the body. A split
                // may have shrunk this lease: refresh the stop boundary.
                if changed.is_err() || job.fatal.is_fatal() {
                    return Err(WorkerError::Fatal(DownloadError::Cancelled));
                }
                let sched = job.scheduler.lock().await;
                if let Some(end) = sched.lease_end(lease.id, lease.generation) {
                    effective_end = effective_end.min(end);
                }
                drop(sched);
                let _ = _seen_revision;
                continue;
            }
        };
        match event {
            Ok(BodyEvent::Data(mut data)) => {
                // Live-tail split boundary: stop consuming at the
                // shrunken lease end — the response tail beyond it belongs
                // to the split lease and is discarded as split waste.
                // The lease end is INCLUSIVE: bytes up to and including
                // effective_end belong to this worker. A chunk may SPAN the
                // boundary (a server delivering a whole body as one frame):
                // the owned prefix is written and the remainder is split
                // waste, so the overlap is bounded regardless of chunk size.
                let abs_offset = validated_start + in_range_offset;
                let original_len = data.len() as u64;
                // inclusive end: abs_offset == effective_end is still owned.
                let owned_len = if abs_offset > effective_end {
                    0
                } else {
                    (effective_end + 1 - abs_offset).min(data.len() as u64)
                };
                let wasted = data.len() as u64 - owned_len;
                if wasted > 0 {
                    if let Some(w) = job.counters.worker(worker_idx) {
                        w.add_wasted(wasted);
                    }
                }
                if owned_len == 0 {
                    break;
                }
                // Wire bytes count at RECEIPT: even if
                // a later write fails, the payload crossed the network and
                // must show in wire throughput.
                if let Some(w) = job.counters.worker(worker_idx) {
                    w.add_network(data.len() as u64);
                }
                // Write at the absolute offset (positional, §14.2) through
                // this worker's blocking writer lane. Rate tokens first:
                // payload bytes only (§18.2). The lane acknowledges before
                // the worker publishes progress or completed counters (one
                // outstanding payload per worker); no per-chunk
                // flush.
                let chunk_was_truncated = owned_len < original_len;
                let owned = data.split_to(owned_len as usize);
                job.acquire_rate(owned.len() as u64).await;
                let write_started = Instant::now();
                lane.write(abs_offset, owned.clone())
                    .await
                    .map_err(|se| WorkerError::Fatal(se.0))?;
                // Write-ack latency and submit-time queue depth for the
                // controller's window sampling. The legacy lane
                // holds exactly one outstanding payload per worker, so its
                // observed depth is 1 by construction.
                job.record_ack_latency(write_started.elapsed());
                job.record_queue_depth(1);
                in_range_offset += owned.len() as u64;

                // Hot-path progress (§13.3): publish the
                // acknowledged written-through offset as one coherent record
                // — no scheduler lock on the chunk path. The
                // publication happens BEFORE the boundary break so the owned
                // prefix of a truncated chunk is published and credited.
                let durable_through = validated_start + in_range_offset;
                job.worker_progress[worker_idx].publish(LeaseRecord {
                    lease_id: lease.id,
                    generation: lease.generation,
                    lease_start: lease.start,
                    written_through: durable_through,
                    received_through: durable_through,
                });
                if chunk_was_truncated {
                    // The chunk was truncated at the inclusive lease end:
                    // this worker is done with the lease.
                    break;
                }
            }
            Ok(BodyEvent::End) => break, // clean EOF
            Ok(BodyEvent::Paused) => {
                // §9.3: pause converges at a safe boundary. The coordinator
                // settles the acknowledged snapshot and the
                // worker waits for the save result before reporting
                // resumability. A save failure is fatal: the observing
                // worker installs the shared error and all workers converge
                // before the job fails.
                let (ack_tx, ack_rx) = oneshot::channel();
                job.save_now_tx
                    .send(CoordinatorCmd::SaveNow { ack: ack_tx })
                    .await
                    .map_err(|_| {
                        WorkerError::Fatal(DownloadError::Protocol(
                            "checkpoint coordinator stopped before the pause save".into(),
                        ))
                    })?;
                ack_rx
                    .await
                    .map_err(|_| {
                        WorkerError::Fatal(DownloadError::Protocol(
                            "checkpoint coordinator dropped the pause save".into(),
                        ))
                    })?
                    .map_err(WorkerError::Fatal)?;
                while job.cancel.is_paused() && !job.cancel.is_cancelled() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                if job.cancel.is_cancelled() {
                    return Err(WorkerError::Fatal(DownloadError::Cancelled));
                }
            }
            Err(e) => {
                // Body faults (reset, truncation, idle timeout, overrun)
                // arrive classified (§32); the worker maps them onto the
                // shared retry/coordination policy. Bytes received past the
                // acknowledged written-through frontier are lost with the
                // attempt: count them as wasted at this lease boundary
                //.
                let written_frontier = cell
                    .snapshot()
                    .filter(|record| record.lease_id == lease.id)
                    .map(|record| record.written_through)
                    .unwrap_or(lease.next_offset)
                    .max(lease.next_offset);
                let received_frontier = validated_start + in_range_offset;
                let wasted = received_frontier.saturating_sub(written_frontier);
                return Err(worker_error_from_failure(e, None, classifier, wasted));
            }
        }
    }
    Ok(())
}

/// Handle one completion: release its reservation, fold it into the
/// frontier and publish one coherent record when the contiguous
/// acknowledged frontier advanced. Stale generations are ignored — they can
/// never credit the live lease.
async fn settle_completion(
    job: &Arc<SegmentedJob>,
    worker_idx: usize,
    writer: &mut PipelinedWriter,
    frontier: &mut LeaseFrontier,
    completion: WriteCompletion,
) -> Result<(), WorkerError> {
    let latency = writer
        .reservations
        .remove(&(completion.lease_id, completion.offset))
        .map(|(reservation, submitted_at)| {
            drop(reservation); // release queued+executing budget bytes
            submitted_at.elapsed()
        });
    let success = matches!(completion.outcome, WriteOutcome::Completed);
    match frontier.record_completion(
        completion.lease_id,
        completion.generation,
        completion.offset,
        completion.len,
        success,
    ) {
        CompletionStatus::Acknowledged { through } => {
            // Write-ack latency for the controller's window sampling
            //: every acknowledged write contributes one sample.
            if let Some(elapsed) = latency {
                job.record_ack_latency(elapsed);
            }
            // Hot-path progress (§13.3): publish the acknowledged
            // contiguous frontier as one coherent record —
            // no scheduler lock on the completion path.
            job.worker_progress[worker_idx].publish(LeaseRecord {
                lease_id: frontier.lease_id(),
                generation: frontier.generation(),
                lease_start: frontier.start(),
                written_through: through,
                received_through: frontier.received_high_water(),
            });
            Ok(())
        }
        CompletionStatus::Failed => {
            // A failed write blocks the frontier and is fatal for the
            // job, matching the legacy lane-error semantics (§14.5).
            match completion.outcome {
                WriteOutcome::Failed(error) => Err(WorkerError::Fatal(error.0)),
                _ => Err(WorkerError::Fatal(DownloadError::SinkWrite(
                    "write was discarded before execution".into(),
                ))),
            }
        }
        CompletionStatus::StaleGeneration => Ok(()),
    }
}

/// Drain every outstanding write of the frontier: completions
/// settle (or fail fatally) until nothing is queued or executing, leaving
/// the published frontier at the contiguous acknowledged prefix. Used at
/// end-of-body, pause boundaries and before retryable-error requeue so a
/// retry resumes from settled coverage only.
async fn drain_outstanding_writes(
    job: &Arc<SegmentedJob>,
    worker_idx: usize,
    writer: &mut PipelinedWriter,
    frontier: &mut LeaseFrontier,
    revisions: &mut tokio::sync::watch::Receiver<u64>,
) -> Result<(), WorkerError> {
    while frontier.has_outstanding() {
        tokio::select! {
            completion = writer.completions.recv() => {
                match completion {
                    Some(completion) => {
                        settle_completion(job, worker_idx, writer, frontier, completion).await?;
                    }
                    None => {
                        return Err(WorkerError::Fatal(DownloadError::SinkWrite(
                            "write executor shut down mid-transfer".into(),
                        )));
                    }
                }
            }
            changed = revisions.changed() => {
                if changed.is_err() || job.fatal.is_fatal() {
                    return Err(WorkerError::Fatal(DownloadError::Cancelled));
                }
            }
            _ = job.cancel.cancelled() => {
                return Err(WorkerError::Fatal(DownloadError::Cancelled));
            }
        }
    }
    Ok(())
}

/// Pipelined write path (design D2/D3): the worker reserves byte
/// budget BEFORE polling the next chunk, submits writes to the shared
/// executor without awaiting them, and keeps receiving while earlier writes
/// execute. Progress publication is driven by completions through the
/// per-lease frontier: the published written-through offset only advances
/// across contiguous acknowledgements — a missing or failed earlier write
/// blocks every later publication (never publishes past a gap).
#[allow(clippy::too_many_arguments)]
async fn consume_pipelined_body(
    job: &Arc<SegmentedJob>,
    lease: &SegmentLease,
    body: &mut crate::http::execution::HttpBody,
    writer: &mut PipelinedWriter,
    worker_idx: usize,
    revisions: &mut tokio::sync::watch::Receiver<u64>,
    validated_start: u64,
    validated_end: u64,
    classifier: &RetryClassifier,
) -> Result<(), WorkerError> {
    let mut frontier =
        LeaseFrontier::new(lease.id, lease.generation, validated_start, validated_end);
    let mut in_range_offset: u64 = 0;
    // Live-tail split safety: refresh the stop boundary on
    // revision wakes; chunks at/after the shrunken lease end are discarded
    // as split waste (never written or credited to the split lease).
    let mut effective_end = validated_end;
    {
        let sched = job.scheduler.lock().await;
        if let Some(end) = sched.lease_end(lease.id, lease.generation) {
            effective_end = effective_end.min(end);
        }
    }
    // The pre-read reservation: held across the body poll and
    // reconciled to the actual frame size after receipt.
    let mut reservation: Option<ByteReservation> = None;

    loop {
        // Register the current revision BEFORE the read: a fatal
        // installed while this worker is parked mid-body publishes a
        // transition and wakes the select below — no lost convergence.
        let _seen_revision = {
            let seen = revisions.borrow_and_update();
            *seen
        };
        if job.cancel.is_cancelled() {
            return Err(WorkerError::Fatal(DownloadError::Cancelled));
        }
        if job.fatal.is_fatal() {
            return Err(WorkerError::Fatal(DownloadError::Cancelled));
        }

        // Pre-read reservation : hold byte budget for
        // the next frame BEFORE polling the body, bounded by the worker
        // read-ahead so a fast connection cannot outrun a slow sink.
        if reservation.is_none() {
            // Byte-budget wait: the read-ahead wait loop plus the
            // reservation acquisition, both of which are storage backpressure.
            let wait_started = Instant::now();
            let quantum = writer.frame_quantum;
            loop {
                let held: u64 = writer
                    .reservations
                    .values()
                    .map(|(reservation, _)| reservation.held())
                    .sum();
                if held + quantum <= writer.budget.worker_read_ahead_bytes() {
                    break;
                }
                // Read-ahead exhausted: wait for a completion to release
                // capacity (or termination).
                tokio::select! {
                    completion = writer.completions.recv() => {
                        match completion {
                            Some(completion) => {
                                settle_completion(job, worker_idx, writer, &mut frontier, completion).await?;
                            }
                            None => {
                                return Err(WorkerError::Fatal(DownloadError::SinkWrite(
                                    "write executor shut down mid-transfer".into(),
                                )));
                            }
                        }
                    }
                    changed = revisions.changed() => {
                        if changed.is_err() || job.fatal.is_fatal() {
                            return Err(WorkerError::Fatal(DownloadError::Cancelled));
                        }
                        let _ = _seen_revision;
                    }
                    _ = job.cancel.cancelled() => {
                        return Err(WorkerError::Fatal(DownloadError::Cancelled));
                    }
                }
            }
            reservation = Some(writer.budget.reserve(quantum).await);
            job.add_budget_wait(wait_started.elapsed());
        }

        let event = tokio::select! {
            event = body.next_chunk(&job.cancel) => event,
            completion = writer.completions.recv() => {
                match completion {
                    Some(completion) => {
                        settle_completion(job, worker_idx, writer, &mut frontier, completion).await?;
                    }
                    None => {
                        return Err(WorkerError::Fatal(DownloadError::SinkWrite(
                            "write executor shut down mid-transfer".into(),
                        )));
                    }
                }
                continue;
            }
            changed = revisions.changed() => {
                // A transition while parked: fatal must converge this
                // worker; other transitions (progress reconciliation,
                // saves, live-tail splits) just re-poll the body. A split
                // may have shrunk this lease: refresh the stop boundary.
                if changed.is_err() || job.fatal.is_fatal() {
                    return Err(WorkerError::Fatal(DownloadError::Cancelled));
                }
                let sched = job.scheduler.lock().await;
                if let Some(end) = sched.lease_end(lease.id, lease.generation) {
                    effective_end = effective_end.min(end);
                }
                drop(sched);
                let _ = _seen_revision;
                continue;
            }
        };
        match event {
            Ok(BodyEvent::Data(mut data)) => {
                // Live-tail split boundary: stop consuming at
                // the shrunken lease end — the response tail beyond it
                // belongs to the split lease. The lease end is INCLUSIVE
                // (the byte at effective_end is still owned), so a chunk
                // that SPANS the boundary is truncated: the owned prefix is
                // submitted and the remainder is split waste. This bounds
                // overlap regardless of chunk size (a server delivering a
                // whole body as one frame cannot overshoot the shrunken
                // lease).
                let abs_offset = validated_start + in_range_offset;
                let chunk_len = data.len() as u64;
                let owned_len = if abs_offset > effective_end {
                    0
                } else {
                    (effective_end + 1 - abs_offset).min(chunk_len)
                };
                let wasted = chunk_len - owned_len;
                if wasted > 0 {
                    if let Some(w) = job.counters.worker(worker_idx) {
                        w.add_wasted(wasted);
                    }
                }
                if owned_len == 0 {
                    // Fully past the boundary: settle outstanding writes,
                    // then complete at the shrunk boundary (the drain keeps
                    // publications coherent; the final check uses the
                    // shrunken end).
                    drop(reservation.take());
                    drain_outstanding_writes(job, worker_idx, writer, &mut frontier, revisions)
                        .await?;
                    if frontier.acknowledged_through() != effective_end + 1 {
                        return Err(WorkerError::Fatal(DownloadError::SinkWrite(
                            "write pipeline settled below the validated range".into(),
                        )));
                    }
                    break;
                }
                // Wire bytes count at RECEIPT: even if
                // a later write fails, the payload crossed the network and
                // must show in wire throughput. The full received chunk is
                // counted (the truncated remainder was received too).
                if let Some(w) = job.counters.worker(worker_idx) {
                    w.add_network(chunk_len);
                }
                let spanned_boundary = owned_len < chunk_len;
                let data = data.split_to(owned_len as usize);
                let chunk_len = owned_len;
                // Rate tokens first: payload bytes only (§18.2).
                job.acquire_rate(chunk_len).await;
                let mut frame = reservation.take().expect("pre-read reservation held");
                if chunk_len > frame.held() {
                    // Oversize frame: grow the reservation (design D2
                    // oversize-frame reconciliation).
                    frame.grow(chunk_len - frame.held()).await;
                }
                frame.reconcile(chunk_len);
                frontier.record_received(abs_offset, chunk_len);
                frontier.record_submission(abs_offset, chunk_len);
                // Submit WITHOUT awaiting the write: the worker keeps
                // receiving the next bounded chunk while this write
                // executes on the shared pool (design D2 overlap).
                writer
                    .session
                    .submit(WriteSubmission {
                        job: writer.session.job(),
                        lease_id: lease.id,
                        generation: lease.generation,
                        offset: abs_offset,
                        data,
                    })
                    .await
                    .map_err(|_| {
                        WorkerError::Fatal(DownloadError::SinkWrite(
                            "write executor shut down mid-transfer".into(),
                        ))
                    })?;
                writer
                    .reservations
                    .insert((lease.id, abs_offset), (frame, std::time::Instant::now()));
                // Outstanding depth at submit time.
                job.record_queue_depth(writer.reservations.len() as u64);
                in_range_offset += chunk_len;
                if spanned_boundary {
                    // The chunk was truncated at the inclusive lease end:
                    // this worker is done with the lease.
                    drop(reservation.take());
                    drain_outstanding_writes(job, worker_idx, writer, &mut frontier, revisions)
                        .await?;
                    if frontier.acknowledged_through() != effective_end + 1 {
                        return Err(WorkerError::Fatal(DownloadError::SinkWrite(
                            "write pipeline settled below the validated range".into(),
                        )));
                    }
                    break;
                }
            }
            Ok(BodyEvent::End) => {
                // Unused pre-read reservation: release it (no chunk came).
                drop(reservation.take());
                // Settle every outstanding write before completing the
                // lease: completions may arrive out of order, and the
                // frontier publishes only the contiguous acknowledged
                // prefix.
                drain_outstanding_writes(job, worker_idx, writer, &mut frontier, revisions).await?;
                if frontier.acknowledged_through() != effective_end + 1 {
                    return Err(WorkerError::Fatal(DownloadError::SinkWrite(
                        "write pipeline settled below the validated range".into(),
                    )));
                }
                break;
            }
            Ok(BodyEvent::Paused) => {
                // §9.3: pause converges at a safe boundary. Drain the
                // outstanding writes first so the acknowledged snapshot the
                // coordinator settles covers everything already received
                //.
                drop(reservation.take());
                // Settle outstanding writes BEFORE the pause save so the
                // checkpoint captures exactly the acknowledged prefix
                // (drain before coordinator_loop save).
                drain_outstanding_writes(job, worker_idx, writer, &mut frontier, revisions).await?;
                // The coordinator settles the acknowledged snapshot
                // and the worker waits for the save result
                // before reporting resumability. A save failure is fatal:
                // the observing worker installs the shared error and all
                // workers converge before the job fails.
                let (ack_tx, ack_rx) = oneshot::channel();
                job.save_now_tx
                    .send(CoordinatorCmd::SaveNow { ack: ack_tx })
                    .await
                    .map_err(|_| {
                        WorkerError::Fatal(DownloadError::Protocol(
                            "checkpoint coordinator stopped before the pause save".into(),
                        ))
                    })?;
                ack_rx
                    .await
                    .map_err(|_| {
                        WorkerError::Fatal(DownloadError::Protocol(
                            "checkpoint coordinator dropped the pause save".into(),
                        ))
                    })?
                    .map_err(WorkerError::Fatal)?;
                while job.cancel.is_paused() && !job.cancel.is_cancelled() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                if job.cancel.is_cancelled() {
                    return Err(WorkerError::Fatal(DownloadError::Cancelled));
                }
            }
            Err(e) => {
                // Body faults (reset, truncation, idle timeout, overrun)
                // arrive classified (§32); the worker maps them onto the
                // shared retry/coordination policy. Retryable
                // errors DRAIN the outstanding writes first so the retry
                // requeues from the settled acknowledged prefix — never
                // past a gap. Fatal and generation-change errors return
                // immediately; worker exit hygiene (detach + discard)
                // settles their queued work before reclaim.
                drop(reservation.take());
                match worker_error_from_failure(e, None, classifier, 0) {
                    WorkerError::Retryable {
                        error, retry_after, ..
                    } => {
                        drain_outstanding_writes(job, worker_idx, writer, &mut frontier, revisions)
                            .await?;
                        let wasted = frontier
                            .received_high_water()
                            .saturating_sub(frontier.acknowledged_through());
                        return Err(WorkerError::Retryable {
                            error,
                            retry_after,
                            wasted,
                        });
                    }
                    other => return Err(other),
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Job-level checkpoint coordinator
// ---------------------------------------------------------------------------

/// Commands into the coordinator.
enum CoordinatorCmd {
    /// Save at a pause boundary; the ack carries the save result so the
    /// pausing worker reports resumability only after the save settled.
    SaveNow {
        ack: oneshot::Sender<Result<(), DownloadError>>,
    },
    /// Stop the coordinator after draining in-flight work (no
    /// post-cleanup saves).
    Stop { ack: oneshot::Sender<()> },
}

/// What the last successful save recorded; unchanged candidates are skipped
/// (coalescing compares intervals plus identity/validator metadata,
/// not a chunk counter).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SavedRevision {
    ranges: Vec<(u64, u64)>,
    generation: u64,
    validators: crate::http::validators::ResourceValidators,
}

/// The coordinator loop: wakes on the configured interval and on boundary
/// commands; every save runs the shared mode-aware path.
async fn coordinator_loop(
    job: Arc<SegmentedJob>,
    store: Arc<dyn CheckpointStore>,
    identity: String,
    interval: Duration,
    mut cmd_rx: mpsc::Receiver<CoordinatorCmd>,
) {
    let mut last_saved: Option<SavedRevision> = None;
    // Immediate first save (§15.5): a resumed job persists its admitted
    // coverage with current validators before any transfer work, so a crash
    // immediately after admission still resumes. Fresh jobs snapshot empty
    // and skip the store entirely.
    if let Err(error) = attempt_coordinator_save(&job, &store, &identity, &mut last_saved).await {
        job.fatal.install(error);
        return;
    }
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(CoordinatorCmd::SaveNow { ack }) => {
                        let result = attempt_coordinator_save(
                            &job, &store, &identity, &mut last_saved,
                        )
                        .await;
                        let _ = ack.send(result);
                    }
                    Some(CoordinatorCmd::Stop { ack }) => {
                        // Drained: any in-flight save completed before this
                        // point (the loop body is sequential).
                        let _ = ack.send(());
                        break;
                    }
                    None => break,
                }
            }
            _ = tick.tick() => {
                // Interval save: skip-unchanged snapshots make a no-progress
                // tick a no-op. A persistence failure is fatal
                //: install the shared error — first wins — and
                // stop saving; workers converge on the fatal flag and the
                // job reports exactly one terminal failure.
                if let Err(error) = attempt_coordinator_save(
                    &job, &store, &identity, &mut last_saved,
                )
                .await
                {
                    job.fatal.install(error);
                    break;
                }
            }
        }
    }
}

/// Counter deltas for one adaptive window: unique newly
/// completed bytes (checkpoint-reused bytes are never part of
/// `completed_bytes`), wire receipts, waste and retries. Duplicate write
/// submissions are not wire bytes — the network delta counts receipts only —
/// so re-submitting a write cannot inflate it.
fn window_counter_deltas(
    previous: &crate::metrics::ProgressSnapshot,
    current: &crate::metrics::ProgressSnapshot,
) -> (u64, u64, u64, u64) {
    (
        current
            .completed_bytes
            .saturating_sub(previous.completed_bytes),
        current.network_bytes.saturating_sub(previous.network_bytes),
        current.wasted_bytes.saturating_sub(previous.wasted_bytes),
        current.retries.saturating_sub(previous.retries),
    )
}

/// One window's inputs folded into the controller's sample: the
/// conversion computes the interval-weighted idle share, so the controller
/// never sees a raw instantaneous count.
struct WindowInputs {
    completed_bytes: u64,
    network_bytes: u64,
    wasted_bytes: u64,
    retries: u64,
    throttled: u64,
    active_worker_ms: u64,
    idle_worker_ms: u64,
    ack_p50_ms: Option<f64>,
    ack_p95_ms: Option<f64>,
    queue_p50: Option<f64>,
    queue_p95: Option<f64>,
    budget_wait_ms: u64,
    rss_bytes: Option<u64>,
    cpu_percent: Option<f64>,
    elapsed: Duration,
}

impl WindowInputs {
    fn into_sample(self) -> crate::control::adaptive::WindowSample {
        crate::control::adaptive::WindowSample {
            completed_bytes: self.completed_bytes,
            network_bytes: self.network_bytes,
            wasted_bytes: self.wasted_bytes,
            retries: self.retries,
            throttled: self.throttled,
            active_worker_ms: self.active_worker_ms,
            idle_worker_ms: self.idle_worker_ms,
            worker_idle_ratio: crate::control::adaptive::WorkerActivity::idle_ratio(
                self.active_worker_ms,
                self.idle_worker_ms,
            ),
            writer_ack_p50_ms: self.ack_p50_ms,
            writer_ack_p95_ms: self.ack_p95_ms,
            writer_queue_p50: self.queue_p50,
            writer_queue_p95: self.queue_p95,
            budget_wait_ms: self.budget_wait_ms,
            rss_bytes: self.rss_bytes,
            cpu_percent: self.cpu_percent,
            elapsed: self.elapsed,
        }
    }
}

/// The adaptive range-concurrency controller loop: sample the actual
/// worker activity interval-weighted (never one point sample) at
/// sub-intervals, and every `window` fold the counter deltas (unique
/// completed = useful goodput — resumed bytes excluded — plus network/wasted,
/// retries, throttle events, interval-weighted active/idle worker-time and
/// writer queue/ack percentiles plus byte-budget wait), decide, and apply
/// within strict bounds. Manual override suspends the loop for the job's
/// remainder.
async fn adaptive_controller_loop(
    job: Arc<SegmentedJob>,
    counters: Arc<JobCounters>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let config = crate::control::adaptive::AdaptiveConfig::default();
    // Protocol-aware growth gate: on HTTP/1 an
    // additional active worker means an additional physical connection, so
    // growth probes never target more concurrent workers than the
    // per-origin connection allowance the connector enforces — probing
    // into blocked permits wastes the probe and the cooldown. On HTTP/2
    // workers are streams multiplexed over one connection: the connection
    // allowance does not cap stream growth, and automatic additional
    // sockets stay off (flow-control evidence is unavailable).
    let effective_max = if job.protocol_is_h2 {
        job.max_workers
    } else {
        job.max_workers.min(u64::from(
            job.per_origin_connection_cap
                .min(job.global_connection_cap)
                .max(1),
        ))
    };
    let mut controller =
        crate::control::adaptive::AdaptiveController::new(config, job.min_workers, effective_max);
    // Sub-interval activity sampling: the window's active/idle
    // inputs are interval-weighted worker-time, not one instantaneous count.
    let sample_interval = (config.window / 8).max(Duration::from_millis(1));
    let mut activity = crate::control::adaptive::WorkerActivity::default();
    let mut previous = counters.fold();
    let mut previous_throttled = job.throttle_events();
    let mut previous_ack = job.ack_latency_snapshot();
    let mut previous_queue = job.queue_depth_snapshot();
    let mut previous_budget_us = job.budget_wait_us();
    // Process RSS/CPU for the storage/resource veto; unavailable
    // measurements stay None and never trigger a veto.
    let mut resources = crate::metrics::resources::ResourceSampler::new();
    let mut last = Instant::now();
    loop {
        // The controller exits promptly on the job shutdown
        // signal as well as its own cancel/fatal/manual-override checks —
        // run_segmented joins it before computing the outcome, so no late
        // decision can race the terminal record.
        tokio::select! {
            _ = tokio::time::sleep(sample_interval) => {}
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return;
                }
                continue;
            }
        }
        if job.cancel.is_cancelled() || job.fatal.is_fatal() {
            return;
        }
        // Manual override: suspend for the remainder.
        if job.is_manual_override() {
            controller.manual_override();
            return;
        }
        let now = Instant::now();
        // Weight the actual active/idle counts by the interval they were in
        // effect for before deciding whether a window ended.
        let (active, idle) = job.worker_activity_counts();
        activity.observe(active, idle, now);
        if now.saturating_duration_since(last) < config.window {
            continue;
        }
        let fold = counters.fold();
        let now_throttled = job.throttle_events();
        let ack = job.ack_latency_snapshot();
        let queue = job.queue_depth_snapshot();
        let budget_us = job.budget_wait_us();
        let (active_ms, idle_ms) = activity.take();
        let window = now.saturating_duration_since(last);
        let (rss_bytes, cpu_percent) = resources.sample(window);
        let (completed, network, wasted, retries) = window_counter_deltas(&previous, &fold);
        let ack_delta = previous_ack.delta(&ack);
        let queue_delta = previous_queue.delta(&queue);
        let sample = WindowInputs {
            completed_bytes: completed,
            network_bytes: network,
            wasted_bytes: wasted,
            retries,
            throttled: now_throttled.saturating_sub(previous_throttled),
            active_worker_ms: active_ms,
            idle_worker_ms: idle_ms,
            ack_p50_ms: ack_delta.latency_percentile_us(0.50).map(|us| us / 1000.0),
            ack_p95_ms: ack_delta.latency_percentile_us(0.95).map(|us| us / 1000.0),
            queue_p50: queue_delta.depth_percentile(0.50),
            queue_p95: queue_delta.depth_percentile(0.95),
            budget_wait_ms: budget_us.saturating_sub(previous_budget_us) / 1000,
            rss_bytes,
            cpu_percent,
            elapsed: window,
        }
        .into_sample();
        previous = fold;
        previous_throttled = now_throttled;
        previous_ack = ack;
        previous_queue = queue;
        previous_budget_us = budget_us;
        last = now;
        #[cfg(test)]
        if let Ok(mut samples) = job.adaptive_samples.lock() {
            samples.push(sample);
        }
        let current = job.desired_workers();
        let decision = controller.decide(sample, current);
        if decision != crate::control::adaptive::Decision::Hold {
            let next = controller.apply(decision, current);
            if next != current {
                job.adjust_desired_workers(next);
            }
        }
    }
}

/// One coordinator save attempt — the shared mode-aware save path (tasks
/// 3.3/4.1, design D2):
///
/// 1. snapshot acknowledged, generation-validated ranges under one short
///    scheduler lock (never data whose write acknowledgment has not
///    returned), then release the lock before filesystem operations;
/// 2. skip unchanged snapshots (same ranges, generation and validators);
/// 3. **Durable** mode synchronizes the output file data *before* persisting
///    the corresponding ranges — a failed sync prevents any save of new
///    ranges; **Performance** mode saves after written acknowledgment
///    without a sync;
/// 4. on failed persistence the in-memory revision does not advance.
///
/// The sidecar format is unchanged.
async fn attempt_coordinator_save(
    job: &Arc<SegmentedJob>,
    store: &Arc<dyn CheckpointStore>,
    identity: &str,
    last_saved: &mut Option<SavedRevision>,
) -> Result<(), DownloadError> {
    // The save latency covers snapshot through the
    // store write (the whole coordinator save), recorded only on success.
    let save_started = std::time::Instant::now();
    // 1. Coherent snapshot under the scheduler lock (reconcile + credit the
    // accepted deltas), then settle.
    let (ranges, generation) = {
        let mut sched = job.scheduler.lock().await;
        let deltas = sched.absorb_worker_progress(&job.worker_progress);
        for (worker_idx, delta) in deltas.into_iter().enumerate() {
            if delta > 0 {
                if let Some(w) = job.counters.worker(worker_idx) {
                    w.add_completed(delta);
                }
            }
        }
        let result = (sched.settled_ranges(), sched.generation());
        // Progress reconciliation can make a live tail splittable — parked
        // workers recheck.
        job.notify_transition();
        result
    };
    if ranges.is_empty() {
        return Ok(());
    }
    let candidates = SavedRevision {
        ranges,
        generation,
        validators: job.validators.clone(),
    };
    // 2. Skip unchanged snapshots (no rewrite without new coverage).
    if last_saved.as_ref() == Some(&candidates) {
        return Ok(());
    }

    // Generation fence: a generation rollover between the
    // snapshot and this check invalidates the snapshot — stale-generation
    // coverage is never persisted. Residual exposure (a bump between this
    // check and the store write) is safe: the ranges are genuinely written
    // bytes and the sidecar carries the old validators, which resume
    // validation rejects; the job is already fatal after a generation bump.
    {
        let sched = job.scheduler.lock().await;
        if sched.generation() != candidates.generation {
            return Ok(());
        }
    }

    // 3. Durability ordering: data sync strictly before metadata save.
    if job.durability == DurabilityMode::Durable {
        let sync = job.sync.as_ref().ok_or_else(|| {
            DownloadError::SinkWrite("durable checkpoint save has no sync capability".into())
        })?;
        // Blocking fs work off the network tasks: one
        // spawn_blocking per infrequent checkpoint, never per chunk.
        let sync = sync.clone();
        tokio::task::spawn_blocking(move || sync.sync_data())
            .await
            .map_err(|join_err| {
                DownloadError::SinkWrite(format!("checkpoint sync task failed: {join_err}"))
            })??;
    }

    // 4. Persist off the latency-sensitive path.
    let mut cp = Checkpoint::new(
        identity,
        String::new(), // original URL is set by the caller's checkpoint; identity hash suffices here
        format!("tmp-{identity}"),
    );
    cp.total_size = Some(job.total_size);
    cp.validators = candidates.validators.clone();
    cp.completed_ranges = candidates.ranges.clone();
    let store_result = {
        let checkpoint = cp;
        let store = store.clone();
        tokio::task::spawn_blocking(move || {
            store
                .save_atomic(&checkpoint)
                .map_err(|e| DownloadError::Checkpoint(e.to_string()))
        })
        .await
        .map_err(|join_err| {
            DownloadError::Checkpoint(format!("checkpoint store task failed: {join_err}"))
        })?
    };
    match store_result {
        Ok(()) => {
            // Revision advances only on a successful save.
            *last_saved = Some(candidates);
            job.last_checkpoint_save_us.store(
                save_started.elapsed().as_micros().max(1) as u64,
                Ordering::Relaxed,
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod durability_tests {
    use super::*;
    use crate::io::sink::TempFileSpec;
    use crate::resume::checkpoint::Checkpoint;
    use crate::resume::CheckpointError;
    use std::sync::Mutex;

    /// Injectable recording store: counts save attempts vs successes.
    struct RecordingStore {
        state: Mutex<(usize, usize)>, // (attempts, successful)
        fail_saves: bool,
    }

    impl crate::resume::checkpoint_store::CheckpointStore for RecordingStore {
        fn load(&self, _job_identity: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            Ok(None)
        }

        fn save_atomic(&self, _checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
            let mut state = self.state.lock().expect("recording store");
            state.0 += 1;
            if self.fail_saves {
                return Err(CheckpointError::Corrupt("injected save failure".into()));
            }
            state.1 += 1;
            Ok(())
        }

        fn delete(&self, _job_identity: &str) -> Result<(), CheckpointError> {
            Ok(())
        }
    }

    /// A segmented job over one real temp output with the given durability.
    fn durable_job(
        directory: &tempfile::TempDir,
        durability: DurabilityMode,
        fail_saves: bool,
    ) -> (
        Arc<SegmentedJob>,
        OutputSession,
        Arc<dyn crate::resume::checkpoint_store::CheckpointStore>,
        Arc<RecordingStore>,
    ) {
        let destination = directory.path().join("out.bin");
        let mut session =
            OutputSession::create(&destination, &TempFileSpec::default(), false, false, None)
                .expect("session");
        crate::io::sink::Sink::write_at(&mut session, 0, b"durable-payload-bytes").expect("write");
        let sync = session.sync_capability().expect("sync capability");
        let (hub, _events) = crate::metrics::events::EventHub::new(16, Duration::from_secs(1));
        let store_impl = Arc::new(RecordingStore {
            state: Mutex::new((0, 0)),
            fail_saves,
        });
        let store: Arc<dyn crate::resume::checkpoint_store::CheckpointStore> = store_impl.clone();
        let store_counts = store_impl;
        let (save_now_tx, _save_rx) = mpsc::channel(8);
        let (revision_tx, _revision_rx) = tokio::sync::watch::channel(0u64);
        let job = SegmentedJob {
            scheduler: AsyncMutex::new(SegmentScheduler::initialize(
                100,
                &[],
                SchedulerPolicy::new(1, 100),
            )),
            origin_backoff_until: AsyncMutex::new(None),
            fatal: FatalState::default(),
            cancel: crate::control::CancellationToken::new(),
            hub: std::sync::Arc::new(hub),
            counters: Arc::new(JobCounters::new(1)),
            rate_bucket: Arc::new(crate::control::rate_limit::TokenBucket::new(0)),
            global_rate_bucket: Arc::new(crate::control::rate_limit::TokenBucket::new(0)),
            total_size: 100,
            validators: crate::http::validators::ResourceValidators::default(),
            sync: Some(sync),
            durability,
            revision_tx: Arc::new(revision_tx),
            save_now_tx,
            desired_workers: AtomicU64::new(1),
            manual_override: AtomicBool::new(false),
            throttle_events: AtomicU64::new(0),
            ack_latency: crate::metrics::histogram::LatencyHistogram::new(),
            queue_depth: crate::metrics::histogram::DepthHistogram::new(),
            budget_wait_us: AtomicU64::new(0),
            #[cfg(test)]
            adaptive_samples: std::sync::Mutex::new(Vec::new()),
            splits: AtomicU64::new(0),
            segment_requests: AtomicU64::new(0),
            last_checkpoint_save_us: AtomicU64::new(0),
            worker_states: vec![AtomicU8::new(0)],
            worker_lane_live: vec![AtomicBool::new(false)],
            min_workers: 1,
            max_workers: 1,
            ready_work_sizing: false,
            ready_work_factor: 1,
            applied_ready_divisor: AtomicU64::new(u64::MAX),
            protocol_is_h2: false,
            per_origin_connection_cap: 1,
            origin_registry: crate::control::origin::OriginRegistry::disabled(),
            origin_key: None,
            global_connection_cap: 1,
            worker_progress: vec![Arc::new(LeaseProgress::default())],
        };
        let job = Arc::new(job);
        (job, session, store, store_counts)
    }

    /// Write→sync boundary (durable mode): a failed sync must
    /// prevent ANY save — the store sees zero save attempts, and the
    /// revision does not advance.
    #[tokio::test]
    async fn failed_sync_prevents_checkpoint_save() {
        let directory = tempfile::tempdir().expect("tempdir");
        // Fail the sync by scripting a Flush-op failure for this destination.
        let script =
            crate::io::fault_script::OutputFaultScript::register(&directory.path().join("out.bin"));
        script.script().fail_next(
            crate::io::fault_script::OutputOperation::Flush,
            DownloadError::SinkWrite("injected sync failure".into()),
        );
        let (job, _session, store, counts) =
            durable_job(&directory, DurabilityMode::Durable, false);

        // Seed one acknowledged written range so the save has candidates.
        let lease = job.scheduler.lock().await.acquire().expect("lease");
        job.worker_progress[0].test_publish(
            lease.id,
            lease.generation,
            lease.start,
            lease.start + 20,
        );
        let mut last_saved = None;
        let error = attempt_coordinator_save(&job, &store, "identity", &mut last_saved)
            .await
            .expect_err("sync failure is fatal");
        assert!(matches!(error, DownloadError::SinkWrite(_)), "{error}");
        let (attempts, successful) = *counts.state.lock().expect("state");
        assert_eq!(attempts, 0, "no save attempt after a failed sync");
        assert_eq!(successful, 0);
        assert!(last_saved.is_none(), "revision never advances on failure");
    }

    /// Sync→save boundary (durable mode): the sync succeeds but
    /// persistence fails — the save is attempted (sync happened first) but
    /// no coverage is durably recorded and the revision stays put.
    #[tokio::test]
    async fn failed_save_after_sync_is_reported_not_silenced() {
        let directory = tempfile::tempdir().expect("tempdir");
        let (job, _session, store, counts) = durable_job(&directory, DurabilityMode::Durable, true);

        let lease = job.scheduler.lock().await.acquire().expect("lease");
        job.worker_progress[0].test_publish(
            lease.id,
            lease.generation,
            lease.start,
            lease.start + 20,
        );
        let mut last_saved = None;
        let error = attempt_coordinator_save(&job, &store, "identity", &mut last_saved)
            .await
            .expect_err("save failure is fatal");
        assert!(matches!(error, DownloadError::Checkpoint(_)), "{error}");
        let (attempts, successful) = *counts.state.lock().expect("state");
        assert_eq!(attempts, 1, "exactly one save attempt after a good sync");
        assert_eq!(successful, 0);
        assert!(last_saved.is_none());
    }

    /// Performance mode: the shared save path persists
    /// WITHOUT a data sync — the Flush script op is never touched by saves.
    #[tokio::test]
    async fn performance_mode_saves_without_sync() {
        let directory = tempfile::tempdir().expect("tempdir");
        let script = crate::io::fault_script::OutputFaultScript::register(directory.path());
        script.script().fail_next(
            crate::io::fault_script::OutputOperation::Flush,
            DownloadError::SinkWrite("sync must not run in performance mode".into()),
        );
        let (job, _session, store, counts) =
            durable_job(&directory, DurabilityMode::Performance, false);

        let lease = job.scheduler.lock().await.acquire().expect("lease");
        job.worker_progress[0].test_publish(
            lease.id,
            lease.generation,
            lease.start,
            lease.start + 20,
        );
        let mut last_saved = None;
        attempt_coordinator_save(&job, &store, "identity", &mut last_saved)
            .await
            .expect("performance save needs no sync");
        let (attempts, successful) = *counts.state.lock().expect("state");
        assert_eq!((attempts, successful), (1, 1));
        assert!(
            last_saved.is_some(),
            "a successful save advances the revision"
        );
        // A completed save records its latency.
        assert!(
            job.last_checkpoint_save_us().is_some(),
            "save latency must be recorded after a successful save"
        );
        // The Flush op was never fired by the save path.
        assert!(!script
            .script()
            .operations()
            .contains(&crate::io::fault_script::OutputOperation::Flush));
    }

    /// Empty snapshots never touch the store (skip-unchanged semantics at
    /// the save boundary).
    #[tokio::test]
    async fn empty_ranges_skip_the_save() {
        let directory = tempfile::tempdir().expect("tempdir");
        let (job, _session, store, counts) =
            durable_job(&directory, DurabilityMode::Durable, false);
        let mut last_saved = None;
        attempt_coordinator_save(&job, &store, "identity", &mut last_saved)
            .await
            .expect("empty save is a no-op");
        let (attempts, _) = *counts.state.lock().expect("state");
        assert_eq!(attempts, 0);
    }

    /// Unchanged snapshots are skipped: a second save
    /// with identical ranges/generation/validators does not reach the store;
    /// new progress persists again.
    #[tokio::test]
    async fn unchanged_snapshots_are_skipped() {
        let directory = tempfile::tempdir().expect("tempdir");
        let (job, _session, store, counts) =
            durable_job(&directory, DurabilityMode::Performance, false);
        let lease = job.scheduler.lock().await.acquire().expect("lease");
        job.worker_progress[0].test_publish(
            lease.id,
            lease.generation,
            lease.start,
            lease.start + 20,
        );
        let mut last_saved = None;
        attempt_coordinator_save(&job, &store, "identity", &mut last_saved)
            .await
            .expect("first save");
        // Same snapshot again (no new progress): skipped.
        attempt_coordinator_save(&job, &store, "identity", &mut last_saved)
            .await
            .expect("second save skipped");
        let (attempts, _) = *counts.state.lock().expect("state");
        assert_eq!(attempts, 1, "unchanged snapshot must not rewrite the store");

        // New progress → the next save persists again.
        job.worker_progress[0].test_publish(
            lease.id,
            lease.generation,
            lease.start,
            lease.start + 60,
        );
        attempt_coordinator_save(&job, &store, "identity", &mut last_saved)
            .await
            .expect("third save persists new coverage");
        let (attempts, _) = *counts.state.lock().expect("state");
        assert_eq!(attempts, 2);
    }
}

#[cfg(test)]
mod sync_tests {
    use super::*;
    use std::sync::Arc;

    /// Stress: one writer publishes records where the
    /// written-through offset is a pure function of (lease_id, generation);
    /// readers must NEVER observe a mixed record. Includes clear/reuse
    /// cycles (lease_id 0) and stale-generation records.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sequence_cell_never_mixes_publications_under_stress() {
        let cell = Arc::new(LeaseProgress::default());
        let writer_cell = cell.clone();
        let writer = tokio::spawn(async move {
            for lease_id in 1..=20_000u64 {
                if lease_id % 5 == 0 {
                    // Clear/reuse: the cell returns to idle between leases.
                    writer_cell.clear();
                } else {
                    let generation = lease_id / 7;
                    writer_cell.publish(LeaseRecord {
                        lease_id,
                        generation,
                        lease_start: lease_id * 1000,
                        // Derivable invariant: start < written_through <= start + 1000.
                        written_through: lease_id * 1000 + (lease_id % 1000),
                        received_through: lease_id * 1000 + (lease_id % 1000),
                    });
                }
            }
            writer_cell.clear();
        });

        let mut observed = 0u64;
        while !writer.is_finished() {
            if let Some(record) = cell.snapshot() {
                observed += 1;
                // Coherence invariants (never mixed across publications):
                assert_eq!(
                    record.written_through,
                    record.lease_id * 1000 + (record.lease_id % 1000),
                    "mixed publication observed: {record:?}"
                );
                assert_eq!(
                    record.generation,
                    record.lease_id / 7,
                    "mixed generation: {record:?}"
                );
                assert!(record.lease_start < record.written_through);
            }
            std::hint::spin_loop();
        }
        writer.await.expect("writer task");
        // Final cleared state reads idle.
        assert!(cell.snapshot().is_none());
        assert!(observed > 0, "stress readers observed publications");
    }

    /// Stale-generation progress: a stale record read by the
    /// scheduler's reconciliation is rejected (existing invariant, now over
    /// the sequence cell).
    #[test]
    fn stale_generation_records_are_rejected_by_reconciliation() {
        let mut s = SegmentScheduler::initialize(10_000, &[], SchedulerPolicy::new(1, 100));
        let lease = s.acquire().expect("lease");
        let cell = LeaseProgress::default();
        // Stale generation publication.
        cell.test_publish(lease.id, lease.generation + 7, lease.start, 5_000);
        s.absorb_worker_progress(&[Arc::new(cell)]);
        assert_eq!(
            s.active_leases()[0].next_offset,
            lease.start,
            "stale generation never moves the lease"
        );
    }

    /// First-wins fatal ownership: simultaneous failures retain
    /// exactly one authoritative error.
    #[test]
    fn simultaneous_failures_retain_exactly_one_error() {
        let fatal = FatalState::default();
        let first = fatal.install(DownloadError::Protocol("first failure".into()));
        let second = fatal.install(DownloadError::SinkWrite("second failure".into()));
        assert!(first, "the first installer owns the error");
        assert!(!second, "a racing failure cannot replace the first error");
        assert!(fatal.is_fatal());
        let taken = fatal.take().expect("one error");
        assert!(
            matches!(taken, DownloadError::Protocol(_)),
            "the FIRST error is retained: {taken}"
        );
    }

    /// The fatal flag publishes only after the error object is in place:
    /// a thread observing `is_fatal` must always find an error to take.
    #[test]
    fn fatal_flag_implies_error_is_readable() {
        let fatal = FatalState::default();
        fatal.install(DownloadError::Cancelled);
        assert!(fatal.is_fatal());
        assert!(fatal.take().is_some(), "flag set but no error readable");
    }
}

/// Pipelined write path under the internal
/// switch — byte-exact H1 segmented transfer, network/storage overlap with
/// reverse completion through the ack frontier, and read-ahead-bounded
/// receipt under a blocked sink.
#[cfg(test)]
mod write_pipeline_tests {
    use super::*;
    use crate::http::scripted::{ProbeStep, ScriptedHttp, TransferOk, TransferStep};
    use crate::io::fault_script::{OutputFaultScript, OutputOperation};
    use crate::job::controller::{DownloadController, DownloadRequest, ResultStatus};
    use std::time::Duration;

    const TOTAL: u64 = 2000;

    fn content() -> Vec<u8> {
        (0..TOTAL)
            .map(|index| ((index * 31 + 7) % 251) as u8)
            .collect()
    }

    /// One segmented lease [0, 1999] whose body arrives as two chunks.
    fn pipeline_http(content: &[u8]) -> ScriptedHttp {
        ScriptedHttp::new()
            .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
                status: 200,
                total_size: Some(TOTAL),
                accept_ranges: true,
                range_verified: true,
                ..ProbeMetadata::default()
            }))
            .expect_transfer(
                TransferStep::new()
                    .range((0, TOTAL - 1))
                    .ok(TransferOk::new()
                        .range(0, TOTAL - 1)
                        .total(TOTAL)
                        .chunk(content[..1000].to_vec())
                        .chunk(content[1000..].to_vec())),
            )
    }

    fn pipeline_config(pipeline_writes: bool, read_ahead_frames: u64) -> EngineConfig {
        let mut config = EngineConfig::default();
        config.transfer.preallocate_output = false;
        config.transfer.segmentation_threshold = 1024;
        config.transfer.max_workers = 1;
        config.transfer.min_workers = 1;
        config.transfer.max_segment_size = TOTAL;
        config.transfer.min_segment_size = 1;
        config.transfer.verify_range_support = false;
        config.checkpoint_flush_interval = Duration::from_secs(60);
        config.read_buffer_size = 4096;
        config.write_executor.pipeline_writes = pipeline_writes;
        config.write_executor.writer_threads = 2;
        // Read-ahead in whole frame quanta (the reservation unit).
        config.write_budget.worker_read_ahead_bytes = read_ahead_frames * 4096;
        config
    }

    /// Window deltas exclude checkpoint-resumed bytes and count
    /// wire receipts once, so neither resumed coverage nor a re-submitted
    /// write can inflate useful goodput. The weighted activity converts to
    /// the interval idle share.
    #[test]
    fn window_deltas_exclude_resumed_bytes_and_duplicate_submissions() {
        let previous = crate::metrics::ProgressSnapshot {
            network_bytes: 1_000,
            completed_bytes: 1_000,
            reused_bytes: 4_000,
            retries: 0,
            wasted_bytes: 0,
            elapsed: Duration::from_millis(10),
        };
        // One window later: 500 newly completed bytes and 500 further wire
        // bytes (one receipt) although two writes were submitted for the
        // same payload, plus 5_000 more reused checkpoint bytes.
        let current = crate::metrics::ProgressSnapshot {
            network_bytes: 1_500,
            completed_bytes: 1_500,
            reused_bytes: 9_000,
            retries: 0,
            wasted_bytes: 0,
            elapsed: Duration::from_millis(20),
        };
        let (completed, network, wasted, retries) = window_counter_deltas(&previous, &current);
        assert_eq!(
            completed, 500,
            "reused checkpoint bytes never inflate useful goodput"
        );
        assert_eq!(
            network, 500,
            "wire receipts are counted once, not once per submission"
        );
        assert_eq!((wasted, retries), (0, 0));
        let sample = WindowInputs {
            completed_bytes: completed,
            network_bytes: network,
            wasted_bytes: wasted,
            retries,
            throttled: 0,
            active_worker_ms: 300,
            idle_worker_ms: 100,
            ack_p50_ms: Some(2.0),
            ack_p95_ms: Some(8.0),
            queue_p50: Some(1.0),
            queue_p95: Some(2.0),
            budget_wait_ms: 5,
            rss_bytes: None,
            cpu_percent: None,
            elapsed: Duration::from_millis(20),
        }
        .into_sample();
        assert_eq!(
            sample.worker_idle_ratio, 0.25,
            "interval-weighted idle share"
        );
        assert_eq!(sample.active_worker_ms, 300);
        assert_eq!(sample.idle_worker_ms, 100);
        assert_eq!(sample.writer_ack_p50_ms, Some(2.0));
        assert_eq!(sample.writer_queue_p95, Some(2.0));
        assert_eq!(sample.budget_wait_ms, 5);
    }

    /// The live controller records interval-weighted activity plus
    /// writer queue/ack percentiles and never lets one receipt inflate the
    /// wire delta (two chunks received, two writes submitted).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn adaptive_window_carries_weighted_activity_and_writer_instrumentation() {
        let content = content();
        let scripted = pipeline_http(&content);
        let dir = tempfile::tempdir().expect("tmp");
        let dest = dir.path().join("adaptive-window.bin");
        let mut config = pipeline_config(true, 4);
        config.transfer.concurrency_mode = crate::config::ConcurrencyMode::Adaptive;
        config.transfer.min_workers = 1;
        config.transfer.max_workers = 1;
        config.write_executor.writer_threads = 2;
        // Hold the FIRST write: the job stays alive across whole controller
        // windows while the second write still acknowledges.
        let script = OutputFaultScript::register(&dest);
        let gate = script.script().hold_next(OutputOperation::Write);
        let controller = DownloadController::with_execution(
            crate::http::execution::HttpExecution::from_adapter(scripted),
            config,
        );
        let (handle, task) = controller.start(DownloadRequest::new(
            "https://example.test/adaptive-window.bin",
            dest.clone(),
        ));
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while handle
            .segmented_job()
            .is_none_or(|job| job.adaptive_sample_log().len() < 2)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the controller recorded fewer than two windows"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let samples = handle
            .segmented_job()
            .expect("live job")
            .adaptive_sample_log();
        for sample in &samples {
            assert!(
                sample.active_worker_ms + sample.idle_worker_ms > 0,
                "interval-weighted worker time observed: {sample:?}"
            );
            let expected = crate::control::adaptive::WorkerActivity::idle_ratio(
                sample.active_worker_ms,
                sample.idle_worker_ms,
            );
            assert!((sample.worker_idle_ratio - expected).abs() < 1e-9);
            if let (Some(p50), Some(p95)) = (sample.writer_ack_p50_ms, sample.writer_ack_p95_ms) {
                assert!(p50 <= p95, "ack p50 <= p95: {sample:?}");
            }
            if let (Some(p50), Some(p95)) = (sample.writer_queue_p50, sample.writer_queue_p95) {
                assert!(p50 <= p95, "queue p50 <= p95: {sample:?}");
            }
            assert!(
                sample.completed_bytes <= sample.network_bytes,
                "unique completion never exceeds wire bytes: {sample:?}"
            );
        }
        assert!(
            samples.iter().any(|s| s.writer_ack_p50_ms.is_some()),
            "an acknowledged write must appear as a latency percentile: {samples:?}"
        );
        assert!(
            samples.iter().any(|s| s.writer_queue_p50.is_some()),
            "submitted writes must appear as queue-depth percentiles: {samples:?}"
        );
        // Process resource signals reach the controller where the
        // platform reports them; elsewhere they stay labeled unavailable.
        #[cfg(target_os = "linux")]
        {
            assert!(
                samples.iter().all(|s| s.rss_bytes.is_some()),
                "linux windows carry RSS: {samples:?}"
            );
            assert!(
                samples.iter().all(|s| s.cpu_percent.is_some()),
                "linux windows carry CPU: {samples:?}"
            );
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert!(
                samples
                    .iter()
                    .all(|s| s.rss_bytes.is_none() && s.cpu_percent.is_none()),
                "unavailable resource measurements stay None: {samples:?}"
            );
        }
        // Window deltas partition the receipts exactly once: two chunks
        // received and two writes submitted add up to the payload total.
        let total_network: u64 = samples.iter().map(|s| s.network_bytes).sum();
        assert_eq!(
            total_network, TOTAL,
            "wire receipts counted once across windows: {samples:?}"
        );
        gate.release();
        let result = tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .expect("no hang")
            .expect("join")
            .expect("terminal");
        assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
        assert_eq!(result.completed_bytes, TOTAL);
        assert_eq!(
            result.bytes_downloaded_from_network, TOTAL,
            "two submitted writes, two receipts — no duplicate-wire inflation"
        );
        assert_eq!(std::fs::read(&dest).expect("content"), content);
    }

    fn wait_for(predicate: impl Fn() -> bool, timeout: Duration, label: &'static str) {
        let deadline = std::time::Instant::now() + timeout;
        while !predicate() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {label}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pipelined_segmented_transfer_is_byte_exact() {
        let content = content();
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");
        let controller = DownloadController::with_execution(
            HttpExecution::from_adapter(pipeline_http(&content)),
            pipeline_config(true, 4),
        );
        let result = controller
            .run(DownloadRequest::new(
                "https://scripted/pipelined",
                destination.clone(),
            ))
            .await
            .expect("terminal");
        assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
        // Server-emitted == client-received (no over-send on the scripted
        // path), and the useful completion equals the wire payload.
        assert_eq!(result.bytes_downloaded_from_network, TOTAL);
        assert_eq!(result.completed_bytes, TOTAL);
        assert_eq!(
            std::fs::read(&destination).expect("content"),
            content,
            "byte-exact through the shared executor"
        );
    }

    /// Overlap: while the FIRST write is blocked on a slow
    /// sink, the worker keeps receiving the next chunk and submits it; the
    /// second write completes first (reverse completion) and the frontier
    /// publishes nothing until the first write settles. Releasing the
    /// first gate then completes the lease byte-exactly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pipelined_worker_receives_while_writes_execute() {
        let content = content();
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");
        let registration = OutputFaultScript::register(&destination);
        let gate = registration.script().hold_next(OutputOperation::Write);

        let controller = DownloadController::with_execution(
            HttpExecution::from_adapter(pipeline_http(&content)),
            pipeline_config(true, 2),
        );
        let (handle, task) = controller.start(DownloadRequest::new(
            "https://scripted/overlap",
            destination.clone(),
        ));

        // Wait until the first write is executing (blocked in the gate).
        let gate = tokio::task::spawn_blocking(move || {
            gate.wait_until_entered();
            gate
        })
        .await
        .expect("gate waiter");

        // Overlap proof: BOTH chunks were received from the network while
        // the first write is still blocked — the legacy path could not
        // have read chunk 2 before chunk 1's write acknowledged.
        wait_for(
            || handle.snapshot().network_bytes == TOTAL,
            Duration::from_secs(5),
            "network receipt to overlap the blocked write",
        );
        assert_eq!(
            handle.snapshot().completed_bytes,
            0,
            "no publication may pass the blocked first write"
        );

        // Release the SECOND write first (it was never gated): the
        // frontier still cannot publish across the missing first write.
        // (The gate only holds the first Write operation.)
        gate.release();
        let result = task.await.expect("job task").expect("job completes");
        assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
        assert_eq!(
            std::fs::read(&destination).expect("content"),
            content,
            "reverse completion must still produce byte-exact output"
        );
        assert_eq!(result.bytes_downloaded_from_network, TOTAL);
        assert_eq!(result.completed_bytes, TOTAL);
    }

    /// Bounded bytes: with a one-frame read-ahead, a blocked
    /// sink stops network receipt — the worker cannot outrun storage and
    /// engine-owned payload stays within the configured read-ahead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pipelined_read_ahead_bounds_receipt_under_slow_storage() {
        let content = content();
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");
        let registration = OutputFaultScript::register(&destination);
        let gate = registration.script().hold_next(OutputOperation::Write);

        let controller = DownloadController::with_execution(
            HttpExecution::from_adapter(pipeline_http(&content)),
            pipeline_config(true, 1),
        );
        let (handle, task) = controller.start(DownloadRequest::new(
            "https://scripted/bounded",
            destination.clone(),
        ));
        let gate = tokio::task::spawn_blocking(move || {
            gate.wait_until_entered();
            gate
        })
        .await
        .expect("gate waiter");

        // The first chunk was received and submitted; the read-ahead cap
        // (one frame) stops further receipt while the write is blocked.
        wait_for(
            || handle.snapshot().network_bytes >= 1000,
            Duration::from_secs(5),
            "first chunk receipt",
        );
        // The receipt plateau is stable: no further chunk arrives while
        // the sink is blocked (the state cannot advance on its own).
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            handle.snapshot().network_bytes,
            1000,
            "read-ahead must bound receipt while the sink is blocked"
        );

        // Releasing the sink completes the transfer byte-exactly.
        gate.release();
        let result = task.await.expect("job task").expect("job completes");
        assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
        assert_eq!(
            std::fs::read(&destination).expect("content"),
            content,
            "backpressured pipeline still produces byte-exact output"
        );
    }

    /// Retryable body fault: DRAINS the outstanding
    /// writes before the lease requeues, so the retry resumes from the
    /// settled acknowledged prefix — here exactly [1000, 1999] — and the
    /// union stays byte-exact with no double-credited coverage.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pipelined_retry_after_body_fault_resumes_from_the_settled_prefix() {
        let content = content();
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");
        let scripted = ScriptedHttp::new()
            .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
                status: 200,
                total_size: Some(TOTAL),
                accept_ranges: true,
                range_verified: true,
                ..ProbeMetadata::default()
            }))
            .expect_unordered_ranges(
                "pipelined-retry",
                vec![
                    // First attempt: chunk 1 delivered, then the connection
                    // dies (retryable). Chunk 1's write settles during the
                    // drain before the retry.
                    TransferStep::new()
                        .range((0, TOTAL - 1))
                        .ok(TransferOk::new()
                            .range(0, TOTAL - 1)
                            .total(TOTAL)
                            .chunk(content[..1000].to_vec())
                            .fault(DownloadError::Connection("scripted reset".into()))),
                    // The retry must request exactly the unsettled tail.
                    TransferStep::new()
                        .range((1000, TOTAL - 1))
                        .ok(TransferOk::new()
                            .range(1000, TOTAL - 1)
                            .total(TOTAL)
                            .chunk(content[1000..].to_vec())),
                ],
            );
        let mut config = pipeline_config(true, 2);
        config.retry.base_delay = Duration::from_millis(5);
        config.retry.max_delay = Duration::from_millis(10);
        let controller =
            DownloadController::with_execution(HttpExecution::from_adapter(scripted), config);
        let result = controller
            .run(DownloadRequest::new(
                "https://scripted/retry",
                destination.clone(),
            ))
            .await
            .expect("terminal");
        assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
        assert_eq!(
            std::fs::read(&destination).expect("content"),
            content,
            "retry from the settled prefix must complete byte-exactly"
        );
        assert_eq!(result.completed_bytes, TOTAL, "unique coverage once");
        assert!(
            result.bytes_downloaded_from_network >= TOTAL,
            "wire bytes cover the payload (any waste is accounted separately)"
        );
    }

    /// Pause with durable sync-before-save: pausing mid-transfer
    /// with queued writes drains them first; the coordinator persists the
    /// acknowledged prefix (durable mode synchronizes first) and the pause
    /// cannot settle while a write is still blocked on the slow sink.
    /// After resume the unsettled tail is re-requested exactly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pipelined_pause_drains_queued_writes_before_the_checkpoint_save() {
        use crate::config::DurabilityMode;
        use crate::resume::job_identity;

        let content = content();
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");
        let registration = OutputFaultScript::register(&destination);
        let gate = registration.script().hold_next(OutputOperation::Write);

        // One lease [0, 1999]: chunk 1, then a body that only wakes on
        // cancellation (pause is observed by the engine's body wrapper).
        // The resumed tail arrives through a second scripted range step,
        // which the paused-and-drained attempt re-requests deterministically.
        let scripted = ScriptedHttp::new()
            .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
                status: 200,
                total_size: Some(TOTAL),
                accept_ranges: true,
                range_verified: true,
                ..ProbeMetadata::default()
            }))
            .expect_unordered_ranges(
                "pipelined-pause",
                vec![
                    TransferStep::new()
                        .range((0, TOTAL - 1))
                        .ok(TransferOk::new()
                            .range(0, TOTAL - 1)
                            .total(TOTAL)
                            .chunk(content[..1000].to_vec())
                            .wait_for_cancellation()),
                    TransferStep::new()
                        .range((1000, TOTAL - 1))
                        .ok(TransferOk::new()
                            .range(1000, TOTAL - 1)
                            .total(TOTAL)
                            .chunk(content[1000..].to_vec())),
                ],
            );

        let mut config = pipeline_config(true, 2);
        config.transfer.durability = DurabilityMode::Durable;
        // The parked body read times out quickly after resume, ending the
        // attempt with its acknowledged prefix; the retry then fetches the
        // exact tail (engine semantics for a stalled connection).
        let scripted = scripted.with_read_idle_timeout(Duration::from_millis(500));
        config.retry.base_delay = Duration::from_millis(5);
        config.retry.max_delay = Duration::from_millis(10);
        let controller =
            DownloadController::with_execution(HttpExecution::from_adapter(scripted), config);
        let (handle, task) = controller.start(DownloadRequest::new(
            "https://scripted/pause-drain",
            destination.clone(),
        ));
        let gate = tokio::task::spawn_blocking(move || {
            gate.wait_until_entered();
            gate
        })
        .await
        .expect("gate waiter");

        // Chunk 1 was received and its write is blocked on the slow sink.
        wait_for(
            || handle.snapshot().network_bytes >= 1000,
            Duration::from_secs(5),
            "first chunk received and write gated",
        );

        handle.pause();
        // The pause must NOT settle (save) while the drain is blocked on
        // the slow sink: the job stays operational and nothing publishes
        // past the blocked write.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !handle.state().is_terminal(),
            "drain must gate the pause boundary"
        );
        assert_eq!(
            handle.snapshot().completed_bytes,
            0,
            "no acknowledged coverage exists while the write is blocked"
        );

        // Releasing the sink lets the drain finish; the coordinator then
        // runs the durable sync-before-save for the acknowledged prefix.
        gate.release();
        let identity = job_identity("https://scripted/pause-drain", &destination);
        let sidecar = directory.path().join(format!("{identity}.kdown"));
        wait_for(
            || sidecar.exists(),
            Duration::from_secs(10),
            "durable checkpoint saved after the drained pause",
        );

        // Resume: the stalled attempt ends at its acknowledged prefix and
        // the tail is re-fetched; the job completes byte-exactly.
        handle.resume_now();
        let result = task.await.expect("job task").expect("job completes");
        assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
        assert_eq!(
            std::fs::read(&destination).expect("content"),
            content,
            "resume after drained pause completes byte-exactly"
        );
        assert_eq!(result.completed_bytes, TOTAL, "unique coverage once");
    }

    /// Cancellation: cancelling with queued writes converges
    /// deterministically — delete-partial removes the artifact, keep-partial
    /// preserves it; neither hangs on queued writes or leaks budget permits
    /// (a leak would deadlock the drain and trip the timeout).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pipelined_cancellation_with_queued_writes_converges_without_leaks() {
        use crate::job::controller::CancelMode;
        for mode in [CancelMode::DeletePartial, CancelMode::KeepPartial] {
            let content = content();
            let directory = tempfile::tempdir().expect("tempdir");
            let destination = directory.path().join("output.bin");
            let registration = OutputFaultScript::register(&destination);
            let gate = registration.script().hold_next(OutputOperation::Write);

            let controller = DownloadController::with_execution(
                HttpExecution::from_adapter(pipeline_http(&content)),
                pipeline_config(true, 2),
            );
            let (handle, task) = controller.start(DownloadRequest::new(
                "https://scripted/cancel",
                destination.clone(),
            ));
            let gate = tokio::task::spawn_blocking(move || {
                gate.wait_until_entered();
                gate
            })
            .await
            .expect("gate waiter");
            wait_for(
                || handle.snapshot().network_bytes == TOTAL,
                Duration::from_secs(5),
                "queued writes exist at cancel time",
            );

            handle.cancel_with(mode);
            // The in-flight write always completes (a real sink never
            // blocks forever); release the test gate so it settles.
            gate.release();
            let result = tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .expect("cancellation must converge without deadlock")
                .expect("terminal")
                .expect("job task");
            assert_eq!(result.status, ResultStatus::Cancelled, "{result:?}");

            let partial = directory.path().join("output.bin.part");
            match mode {
                CancelMode::DeletePartial => {
                    assert!(!partial.exists(), "delete-partial removes the artifact");
                    assert!(!destination.exists(), "no published output on cancel");
                }
                CancelMode::KeepPartial => {
                    assert!(partial.exists(), "keep-partial preserves the artifact");
                    assert!(!destination.exists(), "cancel never publishes");
                }
                CancelMode::KeepFileDiscardCheckpoint => unreachable!(),
            }
        }
    }

    /// Fatal write errors: ENOSPC and permission-denied surface
    /// as structured failures, publish nothing, and the job terminates
    /// promptly — no permit leak can deadlock the pipeline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pipelined_write_failures_fail_closed_without_leaks() {
        for (kind, reason) in [
            ("disk-full", "scripted ENOSPC"),
            ("permission", "scripted EACCES"),
        ] {
            let expected = if kind == "disk-full" {
                DownloadError::DiskFull(reason.into())
            } else {
                DownloadError::PermissionDenied(reason.into())
            };
            let content = content();
            let directory = tempfile::tempdir().expect("tempdir");
            let destination = directory.path().join("output.bin");
            let registration = OutputFaultScript::register(&destination);
            let expected_category = expected.category();
            registration
                .script()
                .fail_next(OutputOperation::Write, expected);

            let controller = DownloadController::with_execution(
                HttpExecution::from_adapter(pipeline_http(&content)),
                pipeline_config(true, 2),
            );
            let result = tokio::time::timeout(
                Duration::from_secs(10),
                controller.run(DownloadRequest::new(
                    "https://scripted/fail",
                    destination.clone(),
                )),
            )
            .await
            .expect("write failure must terminate without deadlock")
            .expect("terminal");
            assert_eq!(result.status, ResultStatus::Failed, "{result:?}");
            assert_eq!(
                result.error.as_ref().map(DownloadError::category),
                Some(expected_category),
                "structured sink error surfaces"
            );
            assert!(
                !destination.exists(),
                "a failed job never publishes unverified bytes"
            );
        }
    }

    /// The legacy path (default) is unaffected by the switch: identical
    /// scripted transfer completes byte-exactly with lanes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn legacy_path_remains_the_default_and_byte_exact() {
        let content = content();
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");
        let controller = DownloadController::with_execution(
            HttpExecution::from_adapter(pipeline_http(&content)),
            pipeline_config(false, 4),
        );
        let result = controller
            .run(DownloadRequest::new(
                "https://scripted/legacy",
                destination.clone(),
            ))
            .await
            .expect("terminal");
        assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
        assert_eq!(std::fs::read(&destination).expect("content"), content);
    }
}
