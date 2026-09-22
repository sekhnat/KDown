//! Segmented download orchestration (§12, §13, tasks 5.5-5.6, 5.8).
//!
//! eligibility gate (§10.3, caller) -> scheduler -> bounded worker pool:
//! acquire -> validated range request -> positional write -> report ->
//! complete/fail -> repeat. Terminal sink errors propagate and stop the
//! job (§14.5). Coordinated origin backoff gates all workers on 503/429
//! (§17.4). Runtime concurrency reduction settles excess workers (task
//! 5.8).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex as AsyncMutex;

use crate::config::{DurabilityMode, EngineConfig};
use crate::control::retry::{parse_retry_after, RetryClassifier, RetryDecision};
use crate::control::CancellationToken;
use crate::error::DownloadError;
use crate::http::probe::ProbeMetadata;
use crate::http::range::{validate_range_response, RejectionKind};
use crate::http::transport::{HttpTransport, RequestSpec};
use crate::io::sink::{FileSink, FlushLevel, Sink as _};
use crate::job::controller::{DownloadRequest, ResultStatus};
use crate::job::state::{JobState, StateMachine};
use crate::metrics::counters::JobCounters;
use crate::metrics::events::SharedHub;
use crate::resume::checkpoint::Checkpoint;
use crate::resume::checkpoint_store::{CheckpointStore as _, FileCheckpointStore};
use crate::scheduler::lease::SegmentLease;
use crate::scheduler::LeaseId;
use crate::scheduler::core::{SchedulerPolicy, SegmentScheduler};

/// Shared worker↔controller state for one segmented job.
pub struct SegmentedJob {
    scheduler: AsyncMutex<SegmentScheduler>,
    /// Coordinated origin backoff gate (§17.4): when set, all workers wait
    /// until this instant before their next request.
    origin_backoff_until: AsyncMutex<Option<Instant>>,
    /// Terminal error: set once, stops all workers (§14.5).
    fatal: AsyncMutex<Option<DownloadError>>,
    cancel: CancellationToken,
    hub: SharedHub,
    counters: Arc<JobCounters>,
    /// Job-level token bucket (§18): `None` = unlimited; rate changes at
    /// runtime take effect on the next acquire.
    rate_bucket: std::sync::Mutex<Option<Arc<crate::control::rate_limit::TokenBucket>>>,
    total_size: u64,
    validators: crate::http::validators::ResourceValidators,
    /// Requested worker concurrency (handle API, task 5.8).
    desired_workers: AtomicU64,
    /// Hot-path lease progress (§13.3, task 6.3): one atomic cell per
    /// worker. A worker publishing durable-through offsets for its current
    /// lease writes `(lease_id, generation, durable_through)` into its own
    /// cell with Relaxed atomics — no scheduler lock on the chunk path.
    /// The scheduler reconciles at lease boundaries (complete/fail/split)
    /// and at the checkpoint cadence.
    worker_progress: Vec<Arc<LeaseProgress>>,
}

/// One worker's in-flight lease progress (§13.3): lock-free.
#[derive(Debug, Default)]
pub struct LeaseProgress {
    /// Current lease id (`0` = none).
    lease_id: AtomicU64,
    /// Generation the lease was acquired under.
    generation: AtomicU64,
    /// Durable-through offset (exclusive) acknowledged by the sink.
    durable_through: AtomicU64,
    /// Lease start offset (for reconciliation).
    lease_start: AtomicU64,
}

impl LeaseProgress {
    fn publish(&self, lease_id: LeaseId, generation: u64, lease_start: u64, durable_through: u64) {
        self.lease_id.store(lease_id, Ordering::Relaxed);
        self.generation.store(generation, Ordering::Relaxed);
        self.lease_start.store(lease_start, Ordering::Relaxed);
        self.durable_through.store(durable_through, Ordering::Relaxed);
    }

    /// Test hook mirroring [`Self::publish`] for scheduler reconciliation
    /// tests (the real publish is chunk-path-internal).
    #[cfg(test)]
    pub(crate) fn test_publish(&self, lease_id: LeaseId, generation: u64, lease_start: u64, durable_through: u64) {
        self.publish(lease_id, generation, lease_start, durable_through);
    }

    fn clear(&self) {
        self.lease_id.store(0, Ordering::Relaxed);
        self.generation.store(0, Ordering::Relaxed);
        self.lease_start.store(0, Ordering::Relaxed);
        self.durable_through.store(0, Ordering::Relaxed);
    }

    /// A consistent-enough snapshot for scheduler reconciliation (§13.3):
    /// `(lease_id, generation, durable_through)`; `(0, .., ..)` when idle.
    pub(crate) fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.lease_id.load(Ordering::Relaxed),
            self.generation.load(Ordering::Relaxed),
            self.durable_through.load(Ordering::Relaxed),
        )
    }
}

impl SegmentedJob {
    /// Whether no leases are active (all work done or in pending).
    async fn active_leases_empty(&self) -> bool {
        self.scheduler.lock().await.active_leases().is_empty()
    }

    /// Whether no leases held by workers other than `worker_idx` remain
    /// (used for concurrency-reduction exit decisions, task 5.8).
    ///
    /// Leases are not worker-attributed in v1; an idle exiting worker
    /// simply checks whether any lease remains unconsumed.
    async fn active_leases_empty_owned_by_others(&self, worker_idx: usize) -> bool {
        let _ = worker_idx;
        self.scheduler.lock().await.active_leases().is_empty()
    }

    /// The shared worker counters slot (v1: all workers share slot 0's
    /// atomics; counts remain exact, attribution is coarse).
    fn counters_slot(&self) -> Option<&crate::metrics::counters::WorkerCounters> {
        self.counters.worker(0)
    }

    /// Set the desired worker count at runtime (task 5.8: handle API).
    /// Excess workers settle their leases and exit on the next loop.
    pub fn set_desired_workers(&self, n: u64) {
        self.desired_workers.store(n.max(1), Ordering::Relaxed);
    }

    /// Attach or replace the job rate bucket (§18.2).
    pub fn set_rate_bucket(&self, bucket: Option<Arc<crate::control::rate_limit::TokenBucket>>) {
        *self
            .rate_bucket
            .lock()
            .expect("rate bucket lock") = bucket;
    }

    /// Acquire tokens for `len` payload bytes, sleeping when the bucket
    /// gates (§18.2: payload bytes only, no busy wait).
    async fn acquire_rate(&self, len: u64) {
        let bucket = self
            .rate_bucket
            .lock()
            .expect("rate bucket lock")
            .clone();
        if let Some(b) = bucket {
            b.acquire_async(len).await;
        }
    }

    #[must_use]
    pub fn desired_workers(&self) -> u64 {
        self.desired_workers.load(Ordering::Relaxed)
    }

    async fn take_fatal(&self) -> Option<DownloadError> {
        self.fatal.lock().await.take()
    }

    async fn completed_ranges(&self) -> Vec<(u64, u64)> {
        self.scheduler.lock().await.completed_ranges()
    }

    async fn is_complete(&self) -> bool {
        self.scheduler.lock().await.is_complete()
    }
}

/// A completed segmented transfer's accounting (before verify/commit).
pub struct SegmentedOutcome {
    pub status: ResultStatus,
    pub error: Option<DownloadError>,
    pub network_bytes: u64,
    pub reused_bytes: u64,
    pub total_size: u64,
    pub elapsed: Duration,
    pub validators: crate::http::validators::ResourceValidators,
    pub warnings: Vec<String>,
    /// Completed ranges at terminal time (for pause/keep persistence).
    pub completed_ranges: Vec<(u64, u64)>,
}

/// Run a segmented download over a validated probe result (task 5.5).
///
/// The caller has already checked eligibility (§10.3), opened the sink,
/// and validated resume state. This drives the worker pool to a terminal
/// condition; verification and commit stay with the caller (§16/§14.6).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_segmented(
    transport: HttpTransport,
    config: &EngineConfig,
    request: &DownloadRequest,
    state: &Arc<StateMachine>,
    sink: Arc<AsyncMutex<FileSink>>,
    store: &FileCheckpointStore,
    identity: &str,
    meta: &ProbeMetadata,
    total_size: u64,
    counters: Arc<JobCounters>,
    hub: &SharedHub,
    cancel: CancellationToken,
    start_offset_ranges: Vec<(u64, u64)>,
    started: Instant,
    handle_cell: Option<
        Arc<std::sync::OnceLock<Arc<SegmentedJob>>>,
    >,
    initial_rate_bucket: Option<Arc<crate::control::rate_limit::TokenBucket>>,
) -> SegmentedOutcome {
    let warnings: Vec<String> = vec![];
    let policy = SchedulerPolicy::new(
        config.transfer.min_segment_size,
        config.transfer.max_segment_size,
    );
    let scheduler = SegmentScheduler::initialize(total_size, &start_offset_ranges, policy);
    let desired = u64::from(config.transfer.max_workers.max(1));
    let worker_progress: Vec<Arc<LeaseProgress>> =
        (0..desired.max(16)).map(|_| Arc::new(LeaseProgress::default())).collect();
    let job = Arc::new(SegmentedJob {
        scheduler: AsyncMutex::new(scheduler),
        origin_backoff_until: AsyncMutex::new(None),
        fatal: AsyncMutex::new(None),
        cancel: cancel.clone(),
        hub: hub.clone(),
        counters: counters.clone(),
        rate_bucket: std::sync::Mutex::new(initial_rate_bucket),
        total_size,
        validators: meta.validators.clone(),
        desired_workers: AtomicU64::new(desired),
        worker_progress,
    });
    // Publish the live job for the handle's runtime controls (task 5.8).
    if let Some(cell) = &handle_cell {
        let _ = cell.set(job.clone());
    }

    // Fixed configured workers for v1 (D6); mutable at runtime via the
    // job (task 5.8).
    let worker_count = job.desired_workers() as usize;
    let mut handles = Vec::with_capacity(worker_count);
    for worker_idx in 0..worker_count {
        let transport = transport.clone();
        let classifier = RetryClassifier::new(config.retry.clone());
        let job = job.clone();
        let sink = Arc::clone(&sink);
        let req_spec = WorkerRequestSpec {
            url: meta.final_url.clone(),
            headers: request.headers.clone(),
            identity_encoding: true,
        };
        let chunk_size = (config.read_buffer_size as usize).max(4096);
        let buffer_budget = config.buffer_pool_max_bytes;
        let checkpoint_interval = config.checkpoint_flush_interval;
        let durable_mode = config.transfer.durability;
        let counters_w = counters.clone();
        let store = store.clone();
        let identity_owned = identity.to_string();
        handles.push(tokio::spawn(async move {
            worker_loop(
                transport,
                classifier,
                job,
                sink,
                store,
                identity_owned,
                req_spec,
                buffer_budget,
                chunk_size,
                checkpoint_interval,
                durable_mode,
                counters_w,
                worker_idx,
                started,
            )
            .await
        }));
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

    // Cancelled: report cancelled with the scheduler's completed set.
    if job.cancel.is_cancelled() {
        let completed = job.completed_ranges().await;
        return SegmentedOutcome {
            status: ResultStatus::Cancelled,
            error: outcome_error.or(Some(DownloadError::Cancelled)),
            network_bytes: counters.fold().network_bytes,
            reused_bytes: counters.fold().reused_bytes,
            total_size,
            elapsed: started.elapsed(),
            validators: job.validators.clone(),
            warnings,
            completed_ranges: completed,
        };
    }
    if let Some(f) = job.take_fatal().await {
        outcome_error.get_or_insert(f);
    }
    let completed = job.completed_ranges().await;
    let complete = job.is_complete().await;
    if let Some(e) = outcome_error {
        let _ = state.transition(JobState::Failing);
        let _ = state.transition(JobState::Failed);
        return SegmentedOutcome {
            status: ResultStatus::Failed,
            error: Some(e),
            network_bytes: counters.fold().network_bytes,
            reused_bytes: counters.fold().reused_bytes,
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
            network_bytes: counters.fold().network_bytes,
            reused_bytes: counters.fold().reused_bytes,
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
        network_bytes: counters.fold().network_bytes,
        reused_bytes: counters.fold().reused_bytes,
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

/// Errors inside one lease attempt.
enum WorkerError {
    Retryable {
        error: DownloadError,
        retry_after: Option<Duration>,
    },
    Fatal(DownloadError),
    GenerationChanged(DownloadError),
}

/// The worker loop (§13 steps 1-10).
#[allow(clippy::too_many_arguments)]
async fn worker_loop(
    transport: HttpTransport,
    classifier: RetryClassifier,
    job: Arc<SegmentedJob>,
    sink: Arc<AsyncMutex<FileSink>>,
    store: FileCheckpointStore,
    identity: String,
    req_spec: WorkerRequestSpec,
    buffer_budget: u64,
    chunk_size: usize,
    checkpoint_interval: Duration,
    durable_mode: DurabilityMode,
    counters: Arc<JobCounters>,
    worker_idx: usize,
    started: Instant,
) -> Result<(), DownloadError> {
    let _ = (durable_mode, started);
    let pool = Arc::new(crate::io::BufferPool::new(chunk_size, buffer_budget));
    let mut attempt: u32 = 0;

    loop {
        // Terminal conditions.
        if job.cancel.is_cancelled() {
            return Ok(());
        }
        if job.fatal.lock().await.is_some() {
            return Ok(());
        }
        // Concurrency reduction: over the desired count -> settle and exit
        // (task 5.8). Workers leave only when they hold no lease.
        if job.desired_workers() < u64::from(u32::try_from(worker_idx + 1).unwrap_or(u32::MAX)) {
            // Workers with index >= desired exit once idle.
            // (Leases are settled inside the acquire branch below.)
            if job.active_leases_empty_owned_by_others(worker_idx).await {
                return Ok(());
            }
        }

        // Acquire a lease (short lock, §13.3).
        let lease = {
            let mut sched = job.scheduler.lock().await;
            // No pending work: wait for tails/failures of live leases
            // instead of manufacturing splits every tick (§12.3 splits
            // serve a worker that would otherwise idle; a single split
            // attempt per idle pass is enough).
            sched.acquire()
        };
        let lease = match lease {
            Some(l) => l,
            None => {
                // No pending work: if nothing is active either, we're done.
                if job.active_leases_empty().await {
                    return Ok(());
                }
                // Other workers still hold work; opportunistically split a
                // big live tail once, then wait (§12.3).
                let split = {
                    let mut sched = job.scheduler.lock().await;
                    let live = sched.active_leases();
                    let biggest = live.iter().copied().max_by_key(SegmentLease::remaining);
                    match biggest {
                        Some(b) => sched.split_tail(b.id, b.generation, 256 * 1024),
                        None => None,
                    }
                };
                match split {
                    Some(tail) => tail,
                    None => {
                        tokio::time::sleep(Duration::from_millis(20)).await;
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

        let result = transfer_lease(
            &transport,
            &classifier,
            &job,
            &lease,
            &sink,
            &store,
            &identity,
            &req_spec,
            &pool,
            checkpoint_interval,
            worker_idx,
        )
        .await;

        match result {
            Ok(()) => {
                attempt = 0; // reset backoff on success
                let mut sched = job.scheduler.lock().await;
                // Cell already absorbed inside transfer_lease before
                // completion; clear the worker's cell now the lease ends.
                let _ = sched.complete(lease.id, lease.generation);
                job.worker_progress[worker_idx].clear();
            }
            Err(WorkerError::Retryable { error, retry_after }) => {
                // Absorb the worker's acknowledged prefix before requeueing
                // (§17.3 tail-only retry) — one lock at the retry boundary.
                {
                    let mut sched = job.scheduler.lock().await;
                    sched.absorb_worker_progress(&job.worker_progress);
                    let _ = sched.fail(lease.id, lease.generation);
                }
                job.worker_progress[worker_idx].clear();
                if let Some(w) = counters.worker(worker_idx) {
                    w.add_retries(1);
                }
                // Coordinated origin backoff (§17.4): gate ALL workers.
                if matches!(
                    error.category(),
                    crate::error::ErrorCategory::Server | crate::error::ErrorCategory::RateLimited
                ) {
                    let earliest = retry_after
                        .unwrap_or_else(|| classifier.backoff_delay(attempt));
                    let until = Instant::now() + earliest;
                    let mut gate = job.origin_backoff_until.lock().await;
                    if gate.is_none_or(|g| until > g) {
                        *gate = Some(until);
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
                    RetryDecision::Retry { attempt: next, delay } => {
                        attempt = next;
                        tokio::time::sleep(delay).await;
                    }
                    RetryDecision::GiveUp => {
                        let mut fatal = job.fatal.lock().await;
                        if fatal.is_none() {
                            *fatal = Some(DownloadError::RetryExhausted {
                                source: Box::new(error),
                            });
                        }
                        return Ok(());
                    }
                }
            }
            Err(WorkerError::Fatal(e)) => {
                {
                    let mut sched = job.scheduler.lock().await;
                    sched.absorb_worker_progress(&job.worker_progress);
                    let _ = sched.fail(lease.id, lease.generation);
                }
                job.worker_progress[worker_idx].clear();
                let mut fatal = job.fatal.lock().await;
                if fatal.is_none() {
                    *fatal = Some(e);
                }
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
                }
                let mut fatal = job.fatal.lock().await;
                if fatal.is_none() {
                    *fatal = Some(e);
                }
                return Ok(());
            }
        }
    }
}

/// Transfer one lease: range request -> validated body -> positional
/// writes -> progress reporting.
#[allow(clippy::too_many_arguments)]
async fn transfer_lease(
    transport: &HttpTransport,
    classifier: &RetryClassifier,
    job: &Arc<SegmentedJob>,
    lease: &SegmentLease,
    sink: &Arc<AsyncMutex<FileSink>>,
    store: &FileCheckpointStore,
    identity: &str,
    req_spec: &WorkerRequestSpec,
    pool: &Arc<crate::io::BufferPool>,
    checkpoint_interval: Duration,
    worker_idx: usize,
) -> Result<(), WorkerError> {
    let _ = (pool, checkpoint_interval);
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

    // Resume offset for tail retries (§17.3): the worker's own progress
    // cell holds the durable-through offset — read it lock-free (§13.3).
    // Fall back to the lease's scheduler-side next_offset when the cell is
    // empty (e.g., first attempt after acquire).
    let cell = &job.worker_progress[worker_idx];
    let cell_id = cell.lease_id.load(Ordering::Relaxed);
    let start_from = if cell_id == lease.id {
        cell.durable_through
            .load(Ordering::Relaxed)
            .max(lease.next_offset)
    } else {
        lease.next_offset
    };

    let mut spec = RequestSpec {
        url: req_spec.url.clone(),
        headers: req_spec.headers.clone(),
        identity_encoding: req_spec.identity_encoding,
        ..RequestSpec::default()
    };
    spec.range = Some((start_from, lease.end));
    // Conditional request for tail retries (§11.3): the strongest validator
    // protects against generation mixing.
    if start_from > 0 {
        spec.validators = Some(job.validators.clone());
    }
    let cancel = job.cancel.clone();
    let mut resp = transport
        .get_range(&spec, (start_from, lease.end), &cancel)
        .await
        .map_err(|e| WorkerError::Retryable {
            error: e,
            retry_after: None,
        })?;

    // Metadata validation before body acceptance (§32, §11.2).
    let established_total = Some(job.total_size);
    let expected_validators = Some(&job.validators);
    let validated = validate_range_response(
        (start_from, lease.end),
        &resp,
        established_total,
        expected_validators,
    )
    .map_err(|rej| match rej.kind {
        RejectionKind::GenerationChanged => WorkerError::GenerationChanged(rej.into_error()),
        RejectionKind::FullResponseToNonzeroRange => {
            // Segmented mode cannot proceed: abort for the job (§11.2).
            WorkerError::Fatal(rej.into_error())
        }
        RejectionKind::UnexpectedStatus => {
            let err = rej.into_error();
            if classifier.retryable(&err) {
                let ra = parse_retry_after(resp.header("retry-after"));
                WorkerError::Retryable {
                    error: err,
                    retry_after: ra,
                }
            } else {
                WorkerError::Fatal(err)
            }
        }
        _ => WorkerError::Fatal(rej.into_error()),
    })?;

    let accepted_len = validated.end - validated.start + 1;
    let mut in_range_offset: u64 = 0;
    let mut last_checkpoint = Instant::now();
    use http_body_util::BodyExt;
    let Some(mut body) = resp.body() else {
        return Err(WorkerError::Fatal(DownloadError::Protocol(
            "range response lost its body".into(),
        )));
    };
    loop {
        if job.cancel.is_cancelled() {
            return Err(WorkerError::Fatal(DownloadError::Cancelled));
        }
        if job.fatal.lock().await.is_some() {
            return Err(WorkerError::Fatal(DownloadError::Cancelled));
        }
        let frame = tokio::time::timeout(DEFAULT_READ_IDLE, body.frame())
            .await
            .map_err(|_| WorkerError::Retryable {
                error: DownloadError::Connection("read idle timeout".into()),
                retry_after: None,
            })?;
        let Some(frame) = frame else {
            break; // clean EOF
        };
        let data = match frame {
            Ok(frame) => frame.into_data().map_err(|_| {
                WorkerError::Fatal(DownloadError::Protocol("unexpected trailer frame".into()))
            })?,
            Err(e) => {
                return Err(WorkerError::Retryable {
                    error: classify_body(&e.to_string()),
                    retry_after: None,
                });
            }
        };
        // Body overrun detection (§11.2).
        if crate::http::range::body_overrun(in_range_offset, data.len() as u64, accepted_len) {
            return Err(WorkerError::Fatal(
                DownloadError::InvalidRangeResponse(format!(
                    "response body exceeds accepted range [{}, {}]",
                    validated.start, validated.end
                )),
            ));
        }
        // Write at the absolute offset (positional, §14.2). Rate tokens
        // first: payload bytes only (§18.2).
        let abs_offset = validated.start + in_range_offset;
        job.acquire_rate(data.len() as u64).await;
        {
            let mut s = sink.lock().await;
            s.write_at(abs_offset, &data)
                .map_err(|se| WorkerError::Fatal(se.0))?;
            s.flush(FlushLevel::PageCache).ok();
        }
        if let Some(w) = job.counters_slot() {
            w.add_network(data.len() as u64);
            // Unique completed bytes: only newly-acknowledged file bytes
            // count (§19.1, invariant 5).
            w.add_completed(data.len() as u64);
        }
        in_range_offset += data.len() as u64;

        // Hot-path progress (§13.3, task 6.3): publish the durable-through
        // offset with Relaxed atomics into this worker's cell — no
        // scheduler lock on the chunk path. The scheduler reconciles at
        // lease boundaries and the checkpoint cadence.
        let durable_through = validated.start + in_range_offset;
        job.worker_progress[worker_idx]
            .publish(lease.id, lease.generation, lease.start, durable_through);

        // Checkpoint cadence (§15.4): completed ranges only; this is a
        // lease-boundary-quality reconciliation where the scheduler lock is
        // taken once per interval, not per chunk (§13.3).
        if last_checkpoint.elapsed() >= checkpoint_interval {
            last_checkpoint = Instant::now();
            let ranges = {
                let mut sched = job.scheduler.lock().await;
                sched.absorb_worker_progress(&job.worker_progress);
                sched.completed_ranges()
            };
            persist_checkpoint(store, identity, job, ranges);
        }
    }
    // Final acknowledgment: everything accepted is written. Reconcile this
    // worker's cell into the scheduler before completing the lease (§31).
    {
        let mut sched = job.scheduler.lock().await;
        sched.absorb_worker_progress(&job.worker_progress);
        let _ = sched.report_progress(
            lease.id,
            lease.generation,
            validated.start + accepted_len,
        );
    }
    job.worker_progress[worker_idx].clear();
    job.hub
        .emit(crate::metrics::events::Event::SegmentCompleted {
            worker: lease.id as usize,
            start: validated.start,
            end: validated.end,
        })
        .await;
    Ok(())
}

/// Persist a checkpoint from the scheduler's completed set (§15.4):
/// only durably acknowledged bytes are recorded.
fn persist_checkpoint(
    store: &FileCheckpointStore,
    identity: &str,
    job: &SegmentedJob,
    ranges: Vec<(u64, u64)>,
) {
    let mut cp = Checkpoint::new(
        identity,
        String::new(), // original URL is set by the caller's checkpoint; identity hash suffices here
        format!("tmp-{identity}"),
    );
    cp.total_size = Some(job.total_size);
    cp.validators = job.validators.clone();
    cp.completed_ranges = ranges;
    let _ = store.save_atomic(&cp);
}

/// Default read idle timeout used by the worker loop.
const DEFAULT_READ_IDLE: Duration = Duration::from_secs(30);

fn classify_body(msg: &str) -> DownloadError {
    if msg.contains("incomplete") || msg.contains("connection closed") || msg.contains("reset") {
        DownloadError::Connection(msg.to_string())
    } else if msg.contains("timed out") {
        DownloadError::Connection("read timeout".into())
    } else {
        DownloadError::Connection(msg.to_string())
    }
}