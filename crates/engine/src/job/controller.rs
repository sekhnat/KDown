//! Job controller: one orchestrator for sequential and segmented
//! transfers (§9, §47).
//!
//! Sequential pipeline: probe -> prepare -> sequential GET -> chunked
//! positional writes with backpressure -> verify size/hash -> atomic
//! commit -> Completed. Segmenting transfers reuse the same lifecycle
//! with the segmented scheduler underneath the same controller API.
//!
//! Pause/cancel are cooperative via [`CancellationToken`]; retry uses
//! [`RetryClassifier`] with structured classification (§17).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use sha2::{Sha256, Sha512};

use crate::config::{EngineConfig, HashAlgorithm, IntegrityPolicy, OverwritePolicy, ResumePolicy};
use crate::control::origin::{normalized_origin, OriginRegistry};
use crate::control::retry::{RetryClassifier, RetryDecision};
use crate::control::CancellationToken;
use crate::error::{
    ArtifactDisposition, CancellationSummary, CompletedDownload, DownloadError, DownloadRunError,
    EngineFailure, FailureDomain, TransferAccounting, TransferFailure,
};
use crate::http::probe::ProbeMetadata;
use crate::http::transport::{HttpTransport, RequestSpec};
use crate::http::validators::ResourceValidators;
use crate::http::{
    BodyEvent, FullResponsePolicy, HttpExecution, HttpFailure, ProbeRequest, RangeIntent,
    TransferIntent, TransferRequest,
};
use crate::io::destination_lease::DestinationLease;
use crate::io::output_session::{OutputSession, PartialArtifactOwner};
use crate::io::publish::PublishMode;
use crate::io::sink::{FlushLevel, Sink, TempFileSpec};
use crate::job::state::{JobState, StateMachine};
use crate::metrics::counters::JobCounters;
use crate::metrics::events::{Event, EventHub, SharedHub};
use crate::metrics::export::EngineMetrics;
use crate::resume::checkpoint_store::{
    CheckpointResolveContext, CheckpointStore, CheckpointStoreResolver,
    DurabilityMode as StoreDurability, SidecarCheckpointResolver,
};
use crate::resume::durable_ranges::DurableRangeTracker;
fn publication_mode(policy: OverwritePolicy) -> PublishMode {
    match policy {
        OverwritePolicy::FailIfExists => PublishMode::NoReplace,
        OverwritePolicy::Replace => PublishMode::Replace,
        OverwritePolicy::ResumeIfMatching => PublishMode::Replace,
    }
}

/// What a job downloads (§7.2 subset for v1 single-stream).
#[derive(Clone)]
pub struct DownloadRequest {
    pub url: String,
    pub destination: PathBuf,
    pub headers: Vec<(String, String)>,
    pub expected_size: Option<u64>,
    pub integrity: IntegrityPolicy,
    pub overwrite: OverwritePolicy,
    pub resume: ResumePolicy,
    /// Bearer-style Authorization header convenience.
    pub authorization: Option<String>,
    /// Credential provider callback (§29): consulted on 401/407 challenges,
    /// at most [`crate::control::auth::MAX_AUTH_STAGES`] times per job.
    pub credential_provider: Option<Arc<dyn crate::control::auth::CredentialProvider>>,
}

impl std::fmt::Debug for DownloadRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Sensitive fields (authorization, credential provider) never print
        // their values, and custom header values never appear at all (§35.3):
        // credentials may travel under arbitrary header names.
        // Only header NAMES are diagnostic and stay visible.
        f.debug_struct("DownloadRequest")
            .field("url", &self.url)
            .field("destination", &self.destination)
            // Header values can carry credentials under any name; only the
            // names are diagnostic, so values are never formatted.
            .field("headers", &crate::redact::RedactedHeaders(&self.headers))
            .field("expected_size", &self.expected_size)
            .field("integrity", &self.integrity)
            .field("overwrite", &self.overwrite)
            .field("resume", &self.resume)
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "credential_provider",
                &self.credential_provider.as_ref().map(|_| "<provider>"),
            )
            .finish()
    }
}

impl DownloadRequest {
    #[must_use]
    pub fn new(url: impl Into<String>, destination: PathBuf) -> Self {
        Self {
            url: url.into(),
            destination,
            headers: vec![],
            expected_size: None,
            integrity: IntegrityPolicy::default(),
            overwrite: OverwritePolicy::default(),
            resume: ResumePolicy::default(),
            authorization: None,
            credential_provider: None,
        }
    }
}

/// Internal terminal classification of the segmented transfer phase before
/// verification/commit (§7.4). Crate plumbing only: the public terminal
/// API is [`crate::error::CompletedDownload`] and
/// [`crate::error::DownloadRunError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResultStatus {
    Completed,
    Cancelled,
    Failed,
}

/// Handle for observing/controlling one running job (§7.3).
// hub/total_size are consumed by the event wiring task (3.8).
/// Cancellation artifact policy (§9.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum CancelMode {
    /// Preserve temp file and checkpoint (default-safe for resume).
    KeepPartial = 1,
    /// Delete temp output and checkpoint after workers stop.
    #[default]
    DeletePartial = 0,
    /// Advanced: preserved file is not guaranteed resumable.
    KeepFileDiscardCheckpoint = 2,
}

impl CancelMode {
    pub(crate) fn from_u8(v: u8) -> Self {
        match v {
            1 => CancelMode::KeepPartial,
            2 => CancelMode::KeepFileDiscardCheckpoint,
            _ => CancelMode::DeletePartial,
        }
    }
}

/// Warnings and the resulting artifact disposition produced by
/// [`DownloadController::cleanup_cancelled`] (§9.4).
#[derive(Debug)]
pub(crate) struct CancelCleanup {
    pub warnings: Vec<String>,
    pub artifacts: ArtifactDisposition,
}

// hub/total_size are consumed by event-wiring refinements.
#[allow(dead_code)]
pub struct DownloadHandle {
    pub id: u64,
    state: Arc<StateMachine>,
    cancel: CancellationToken,
    cancel_mode: Arc<std::sync::atomic::AtomicU8>,
    hub: SharedHub,
    counters: Arc<JobCounters>,
    total_size: Arc<std::sync::atomic::AtomicU64>,
    total_known: Arc<std::sync::atomic::AtomicBool>,
    /// Resolved once the run task creates the live SegmentedJob:
    /// worker-concurrency reduction; `None` for single-stream jobs.
    segmented_cell: Arc<std::sync::OnceLock<Arc<crate::job::segmented::SegmentedJob>>>,
    /// Shared rate-limit bucket for the job (§18: runtime changeable).
    /// Stable job-wide bucket: rate 0 = unlimited; runtime
    /// updates mutate it in place so the same object reaches the eventual
    /// job and its active workers.
    rate_bucket: Arc<crate::control::rate_limit::TokenBucket>,
    /// Serializes runtime-control update+event pairs (see the field comment
    /// at construction).
    runtime_control: std::sync::Mutex<()>,
}

impl DownloadHandle {
    #[must_use]
    pub fn state(&self) -> JobState {
        self.state.get()
    }

    /// Live progress snapshot (§19.1).
    #[must_use]
    pub fn snapshot(&self) -> crate::metrics::counters::ProgressSnapshot {
        let s = self.counters.fold();
        if self.total_known.load(std::sync::atomic::Ordering::Relaxed) {
            // total_size carried separately by events; snapshot stays
            // byte-only for v1.
        }
        s
    }

    /// Request cooperative pause (§9.3).
    pub fn pause(&self) {
        self.cancel.pause();
    }

    /// Lift a pause.
    pub fn resume_now(&self) {
        self.cancel.unpause();
        // Wake parked segmented workers: resume is a scheduler-
        // relevant transition for lease-less parked workers.
        if let Some(job) = self.segmented_cell.get() {
            job.notify_transition();
        }
    }

    /// Request cancellation (§9.4) with DeletePartial cleanup.
    pub fn cancel(&self) {
        self.cancel_with(CancelMode::DeletePartial);
    }

    /// Request cancellation with an explicit artifact policy (§9.4):
    /// `KeepPartial` preserves temp+checkpoint for resume; `DeletePartial`
    /// removes them; `KeepFileDiscardCheckpoint` keeps the file only.
    pub fn cancel_with(&self, mode: CancelMode) {
        self.cancel_mode
            .store(mode as u8, std::sync::atomic::Ordering::SeqCst);
        self.cancel.cancel();
    }

    /// Internal accessor for the run task.
    fn cancel_mode_cell(&self) -> Arc<std::sync::atomic::AtomicU8> {
        self.cancel_mode.clone()
    }

    /// The cancellation mode in effect.
    #[must_use]
    pub fn cancel_mode(&self) -> CancelMode {
        match self.cancel_mode.load(std::sync::atomic::Ordering::SeqCst) {
            1 => CancelMode::KeepPartial,
            2 => CancelMode::KeepFileDiscardCheckpoint,
            _ => CancelMode::DeletePartial,
        }
    }

    /// Request a concurrency change for a segmented job: excess workers
    /// settle their leases safely and exit; no byte range is lost. The
    /// request is clamped to the job's configured worker bounds and the
    /// APPLIED count is reported through exactly one
    /// [`Event::ConcurrencyChanged`]; a concurrency-only change never
    /// emits [`Event::RateLimitChanged`]. No-op and silent for
    /// single-stream jobs or before a segmented job exists.
    pub fn set_concurrency(&self, workers: u64) {
        if let Some(job) = self.segmented_cell.get() {
            // The lock pairs the state update with its event emission so
            // concurrent callers' events cannot invert against the applied
            // order; it guards only this control path.
            let _guard = self.runtime_control.lock().expect("runtime control lock");
            let applied = job.set_desired_workers(workers);
            self.hub
                .emit_try(crate::metrics::events::Event::ConcurrencyChanged { workers: applied });
        }
    }
    /// The live segmented job, when the run task created one.
    #[must_use]
    pub fn segmented_job(&self) -> Option<&Arc<crate::job::segmented::SegmentedJob>> {
        self.segmented_cell.get()
    }

    /// Change the job's rate limit at runtime: takes effect on the next
    /// acquire without restarting workers. `0` = unlimited. The stable
    /// bucket is updated BEFORE the event is emitted, so a subscriber that
    /// observes [`Event::RateLimitChanged`] already sees the new limit; a
    /// rate-only change never emits a concurrency event.
    pub fn set_rate_limit(&self, bytes_per_second: u64) {
        // Same update+event pairing as `set_concurrency`.
        let _guard = self.runtime_control.lock().expect("runtime control lock");
        // One stable bucket: pre-start updates mutate it in place so the
        // SAME object reaches the eventual job; live updates mutate the
        // running job's bucket through the same object.
        self.rate_bucket.set_rate(bytes_per_second);
        if let Some(job) = self.segmented_cell.get() {
            job.set_rate(bytes_per_second);
        }
        self.hub
            .emit_try(crate::metrics::events::Event::RateLimitChanged {
                bytes_per_second: if bytes_per_second == 0 {
                    None
                } else {
                    Some(bytes_per_second)
                },
            });
    }

    /// The active rate limit (`None` = unlimited).
    #[must_use]
    pub fn rate_limit(&self) -> Option<u64> {
        let rate = self.rate_bucket.rate();
        (rate != 0).then_some(rate)
    }

    /// Subscribe to this job's event stream (§7.3, §19.4). Multiple
    /// subscribers are supported; slow subscribers may skip lagged events,
    /// while snapshot() remains authoritative for progress.
    pub fn events(&self) -> crate::metrics::events::EventStream {
        self.hub.subscribe()
    }
}

/// The download job controller: starts and supervises both sequential and
/// segmented range transfers behind one lifecycle and handle API (§47
/// `run_job`, both branches).
pub struct DownloadController {
    /// The substitutable HTTP execution seam (§32): semantic probe and
    /// transfer operations without concrete client response types. The
    /// production adapter is injected by [`new`]/[`with_metrics`]; tests and
    /// alternate adapters inject via [`with_execution`].
    execution: HttpExecution,
    config: EngineConfig,
    classifier: RetryClassifier,
    /// The substitutable checkpoint-store resolver (§34): resolved once
    /// per job before admission; the default creates destination-relative
    /// file sidecars so existing construction is unchanged.
    checkpoint_resolver: Arc<dyn CheckpointStoreResolver>,
    /// Controller-shared origin registry: fair
    /// cancellable request admission plus shared throttle feedback, keyed by
    /// the normalized FINAL origin; one instance serves every job this
    /// controller starts.
    origin_registry: Arc<OriginRegistry>,
    next_id: std::sync::atomic::AtomicU64,
    metrics: Arc<EngineMetrics>,
    /// Engine-wide transfer-memory ledger (design D3): shared with the
    /// transport so jobs and connections draw from one budget. The
    /// production construction adopts the transport's ledger; the scripted
    /// test path builds one from configuration.
    transfer_ledger: Arc<crate::io::transfer_ledger::TransferLedger>,
    /// Engine-global payload rate bucket (§18): shared by every job of this
    /// controller, above the per-job bucket. Created from
    /// `config.global_rate_limit`; replaceable for tests via
    /// [`Self::with_global_rate_bucket`].
    global_rate_bucket: Arc<crate::control::rate_limit::TokenBucket>,
    /// Concurrent-admitted-job permits (`max_active_jobs`, §9.5): one
    /// RAII permit per running job, acquired synchronously in [`Self::start`]
    /// and released on success, failure, cancellation or task abort.
    active_jobs: Arc<tokio::sync::Semaphore>,
}

/// Deprecated alias of [`DownloadController`], kept for source
/// compatibility: the controller has always orchestrated BOTH sequential
/// and segmented transfers, so the old name described only one of its two
/// modes. New code must use [`DownloadController`]; this alias is a
/// one-line mechanical rename for existing callers and adds nothing.
#[deprecated(
    note = "`SingleStreamController` starts both sequential and segmented downloads; rename to `DownloadController`"
)]
pub type SingleStreamController = DownloadController;

impl DownloadController {
    /// Build a controller over the production Hyper wire adapter (§32:
    /// compatible construction — existing callers compile unchanged).
    #[must_use]
    pub fn new(transport: HttpTransport, config: EngineConfig) -> Self {
        let ledger = transport.ledger();
        Self::with_execution_and_metrics(
            HttpExecution::from_adapter(transport),
            config,
            EngineMetrics::shared(),
            ledger,
        )
    }

    /// Inject an explicit HTTP execution handle (§32): scripted or alternate
    /// adapters substitute here without adapter-specific branches.
    /// Crate-internal: the external injection seam is retired (task 2.3);
    /// consumers construct the production transport with [`new`](Self::new).
    #[allow(dead_code)] // used by relocated internal tests only
    #[must_use]
    pub(crate) fn with_execution(execution: HttpExecution, config: EngineConfig) -> Self {
        let ledger = Arc::new(crate::io::transfer_ledger::TransferLedger::new(
            &config.transfer_memory,
            config
                .transfer_memory
                .connection_ingress_reserve(config.read_buffer_size, config.max_connections_total),
        ));
        Self::with_execution_and_metrics(execution, config, EngineMetrics::shared(), ledger)
    }

    /// Execution injection with a shared metrics registry (§19.5).
    #[must_use]
    pub(crate) fn with_execution_and_metrics(
        execution: HttpExecution,
        config: EngineConfig,
        metrics: Arc<EngineMetrics>,
        transfer_ledger: Arc<crate::io::transfer_ledger::TransferLedger>,
    ) -> Self {
        let active_jobs = Arc::new(tokio::sync::Semaphore::new(config.max_active_jobs as usize));
        let classifier = RetryClassifier::new(config.retry.clone());
        let global_rate_bucket = Arc::new(crate::control::rate_limit::TokenBucket::new(
            config.global_rate_limit.unwrap_or(0),
        ));
        let checkpoint_resolver = Arc::new(
            SidecarCheckpointResolver::default()
                .with_checkpoint_budget(config.transfer_memory.checkpoint_max_bytes),
        );
        Self {
            execution,
            config,
            classifier,
            checkpoint_resolver,
            origin_registry: OriginRegistry::new(),
            next_id: std::sync::atomic::AtomicU64::new(1),
            metrics,
            transfer_ledger,
            global_rate_bucket,
            active_jobs,
        }
    }

    /// Replace the shared global rate bucket (§18): opt-in sharing across
    /// separately-constructed controllers (tests/benches); the default is
    /// one bucket per controller from `config.global_rate_limit`.
    #[must_use]
    pub fn with_global_rate_bucket(
        mut self,
        bucket: Arc<crate::control::rate_limit::TokenBucket>,
    ) -> Self {
        self.global_rate_bucket = bucket;
        self
    }

    /// Change the engine-global rate limit at runtime (§18.2): takes
    /// effect on the next acquire of every job sharing this controller.
    /// `0` = unlimited.
    pub fn set_global_rate_limit(&self, bytes_per_second: u64) {
        self.global_rate_bucket.set_rate(bytes_per_second);
    }

    /// The active engine-global rate limit (`None` = unlimited).
    #[must_use]
    pub fn global_rate_limit(&self) -> Option<u64> {
        let rate = self.global_rate_bucket.rate();
        (rate != 0).then_some(rate)
    }

    /// Replace the checkpoint-store resolver (§34): one consuming
    /// injection — `controller.with_checkpoint_resolver(resolver)` — so
    /// every existing construction path can opt into another adapter
    /// without a constructor matrix.
    #[must_use]
    pub fn with_checkpoint_resolver(mut self, resolver: Arc<dyn CheckpointStoreResolver>) -> Self {
        self.checkpoint_resolver = resolver;
        self
    }

    /// Replace the shared origin registry: the
    /// default is the enabled engine-shared registry; a
    /// [`OriginRegistry::disabled`] instance restores the per-job backoff
    /// fallback (no shared admission, no shared throttle feedback).
    #[must_use]
    pub fn with_origin_registry(mut self, registry: Arc<OriginRegistry>) -> Self {
        self.origin_registry = registry;
        self
    }

    /// Build a controller sharing an engine-wide metrics registry (§19.5).
    #[must_use]
    pub fn with_metrics(
        transport: HttpTransport,
        config: EngineConfig,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        let ledger = transport.ledger();
        Self::with_execution_and_metrics(
            HttpExecution::from_adapter(transport),
            config,
            metrics,
            ledger,
        )
    }

    /// The engine-wide transfer-memory ledger this controller accounts job
    /// pipeline memory against (design D3): the same ledger the transport
    /// charges connection ingress to. Consumed by the pipeline wiring of
    /// task 3.4 and by relocated internal tests.
    #[cfg_attr(not(test), allow(dead_code))]
    #[must_use]
    pub(crate) fn transfer_ledger(&self) -> Arc<crate::io::transfer_ledger::TransferLedger> {
        Arc::clone(&self.transfer_ledger)
    }

    /// Admit one request to `origin`: waits out the origin's
    /// shared throttle deadline and takes one fair request slot; the RAII
    /// permit releases on drop. `origin: None` (or a disabled registry)
    /// admits immediately.
    async fn admit_origin(
        &self,
        origin: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Option<crate::control::origin::OriginPermit>, DownloadError> {
        match origin {
            Some(key) => self.origin_registry.admit(key, cancel).await.map(Some),
            None => Ok(None),
        }
    }

    /// Shared throttle feedback: extends the origin's coordinated
    /// deadline with the `RetryClassifier`-capped Retry-After (or the
    /// classifier's backoff delay when the server sent none).
    fn report_throttle_origin(
        &self,
        origin: Option<&str>,
        retry_after: Option<Duration>,
        attempt: u32,
    ) {
        if let Some(key) = origin {
            self.origin_registry.report_throttle(
                key,
                self.classifier.honor_retry_after(retry_after),
                self.classifier.backoff_delay(attempt),
            );
        }
    }

    /// Shared recovery accounting: one completed request against
    /// the origin — the post-cooldown probe signal.
    fn report_success_origin(&self, origin: Option<&str>) {
        if let Some(key) = origin {
            self.origin_registry.report_success(key);
        }
    }

    /// Export a consistent metrics snapshot (§19.5).
    #[must_use]
    pub fn metrics(&self) -> Arc<EngineMetrics> {
        self.metrics.clone()
    }

    /// Run to a terminal state without exposing the handle (fire-and-forget
    /// use case).
    ///
    /// # Errors
    /// # Errors
    /// Returns [`DownloadRunError`] for every non-success terminal outcome:
    /// a typed transfer failure, an engine/infrastructure failure, or
    /// cancellation. `Ok` is produced only for a verified, published
    /// completion.
    pub async fn run(
        &self,
        request: DownloadRequest,
    ) -> Result<CompletedDownload, DownloadRunError> {
        self.run_with_handle(request).await.map(|(r, _)| r)
    }

    /// Start a download and return immediately with a control handle
    /// (§7.2 start -> §7.3 handle). The job runs on a spawned task; the
    /// caller awaits the returned join handle for the terminal result.
    ///
    /// Admission (§9.5): at most `max_active_jobs` jobs run concurrently
    /// per controller. When the cap is exhausted, `start` still returns
    /// its handle/join pair, but the task resolves immediately with
    /// [`DownloadError::AdmissionRejected`] (category `MemoryCap`): no
    /// transfer begins and no artifact is written. The permit is released
    /// on success, failure, cancellation, or task abort.
    pub fn start(
        &self,
        request: DownloadRequest,
    ) -> (
        DownloadHandle,
        tokio::task::JoinHandle<Result<CompletedDownload, DownloadRunError>>,
    ) {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.metrics.job_started();
        let metrics_for_run = self.metrics.clone();
        let state = StateMachine::new();
        let cancel = CancellationToken::new();
        // One counter shard per potential worker (per-worker
        // attribution); mirrors the segmented job's worker-progress cells.
        let counters = Arc::new(JobCounters::new(self.config.transfer.max_workers.max(16)));
        let (hub, _stream) = EventHub::new(256, self.config.metrics_interval);
        let hub: SharedHub = Arc::new(hub);
        let handle = DownloadHandle {
            id,
            state: state.clone(),
            cancel: cancel.clone(),
            cancel_mode: Arc::new(std::sync::atomic::AtomicU8::new(
                CancelMode::DeletePartial as u8,
            )),
            hub: hub.clone(),
            counters: counters.clone(),
            total_size: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            total_known: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            segmented_cell: Arc::new(std::sync::OnceLock::new()),
            // Serializes each runtime-control update+event pair so two
            // concurrent callers cannot invert the reported order against
            // the applied order. Guards only these control paths — never
            // scheduler or sink locks, and event emission is nonblocking.
            runtime_control: std::sync::Mutex::new(()),
            // The configured per-job limit (§18) seeds the stable bucket so
            // a pre-start configured limit and later live updates share one
            // object (the config value was previously never read).
            rate_bucket: Arc::new(crate::control::rate_limit::TokenBucket::new(
                self.config.network.rate_limit.unwrap_or(0),
            )),
        };
        // The run task publishes the live SegmentedJob into the same cell
        // the handle reads.
        let handle_cell = handle.segmented_cell.clone();
        let rate_bucket_for_run = handle.rate_bucket.clone();
        let global_bucket_for_run = self.global_rate_bucket.clone();
        let inner_state = state;
        let inner_cancel = cancel;
        let handle_cancel_mode = handle.cancel_mode_cell();
        let inner_counters = counters;
        let inner_hub = hub;
        let execution = self.execution.clone();
        let config = self.config.clone();
        let checkpoint_resolver = self.checkpoint_resolver.clone();
        let classifier = RetryClassifier::new(self.config.retry.clone());
        let origin_registry = self.origin_registry.clone();
        let transfer_ledger = self.transfer_ledger.clone();
        let job_memory_high_water = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let job_memory_cell = Arc::clone(&job_memory_high_water);
        // Synchronous admission (§9.5): one RAII permit per admitted job.
        // An exhausted cap rejects immediately with a typed error and
        // writes nothing; the permit is held by the run task and released
        // on success, failure, cancellation, or task abort.
        let admission_cap = self.config.max_active_jobs;
        let admission_permit = Arc::clone(&self.active_jobs).try_acquire_owned().ok();
        let admission_active =
            admission_cap.saturating_sub(self.active_jobs.available_permits() as u32);
        let active_jobs_for_run = Arc::clone(&self.active_jobs);
        // Admission timestamp for the one monotonic job deadline (§9.5):
        // the watchdog is spawned only for admitted jobs.
        let job_deadline_at = self
            .config
            .transfer
            .job_deadline
            .map(|budget| std::time::Instant::now() + budget);
        let join = tokio::spawn(async move {
            let Some(_admission_permit) = admission_permit else {
                // Immediate typed rejection: no transfer begins and no
                // artifact is written for a job that was never admitted.
                let _ = inner_state.transition(JobState::Failing);
                let _ = inner_state.transition(JobState::Failed);
                return Err(DownloadRunError::Infrastructure(Box::new(EngineFailure {
                    error: DownloadError::AdmissionRejected {
                        active: admission_active,
                        cap: admission_cap,
                    },
                    partial: TransferAccounting::default(),
                    artifacts: ArtifactDisposition::default(),
                })));
            };
            // One monotonic deadline from admission: the watchdog cancels
            // the same token every wait already honors, so probe,
            // header/body waits, retries, workers and verification stop
            // promptly. It is aborted at terminal; a deadline that fires
            // during the job turns a Cancelled outcome into the typed
            // DeadlineExceeded failure below.
            let deadline_cancel = inner_cancel.clone();
            let deadline_done = Arc::new(tokio::sync::Notify::new());
            let deadline_watchdog = job_deadline_at.map(|deadline| {
                let cancel = deadline_cancel.clone();
                let done = Arc::clone(&deadline_done);
                tokio::spawn(async move {
                    tokio::select! {
                        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                            cancel.cancel_with_deadline();
                        }
                        _ = done.notified() => {}
                    }
                })
            });
            let this = Self {
                execution,
                config,
                classifier,
                checkpoint_resolver,
                transfer_ledger,
                origin_registry,
                next_id: std::sync::atomic::AtomicU64::new(0),
                metrics: metrics_for_run.clone(),
                global_rate_bucket: global_bucket_for_run.clone(),
                active_jobs: active_jobs_for_run,
            };
            let terminal = this
                .run_inner(
                    request,
                    inner_state,
                    inner_cancel,
                    handle_cancel_mode,
                    inner_counters,
                    inner_hub,
                    handle_cell.clone(),
                    rate_bucket_for_run,
                    global_bucket_for_run,
                    job_memory_cell,
                )
                .await;
            deadline_done.notify_waiters();
            if let Some(watchdog) = deadline_watchdog {
                watchdog.abort();
            }
            let terminal = match terminal {
                Err(DownloadRunError::Cancelled(summary))
                    if deadline_cancel.deadline_exceeded() =>
                {
                    // The job stopped because its deadline expired, not
                    // because the caller cancelled: report the typed
                    // deadline outcome with the same truthful partial
                    // accounting and artifact disposition.
                    Err(DownloadRunError::Infrastructure(Box::new(EngineFailure {
                        error: DownloadError::DeadlineExceeded,
                        partial: summary.partial,
                        artifacts: summary.artifacts,
                    })))
                }
                other => other,
            };
            match &terminal {
                Ok(result) => metrics_for_run.record_completed(result),
                Err(error) => metrics_for_run.record_run_error(error),
            }
            // Transfer-memory metrics at terminal (task 3.7): the engine
            // ledger's live sample (failure cleanup visible: residual
            // charges are connection footprints only) plus this job's
            // accounted-pipeline peak.
            metrics_for_run.record_transfer_memory(this.transfer_ledger.snapshot());
            metrics_for_run.record_job_memory_high_water(
                job_memory_high_water.load(std::sync::atomic::Ordering::SeqCst),
            );
            terminal
        });
        (handle, join)
    }

    /// Run with a control handle: blocks until terminal.
    ///
    /// # Errors
    /// Same contract as [`DownloadController::run`].
    pub async fn run_with_handle(
        &self,
        request: DownloadRequest,
    ) -> Result<(CompletedDownload, DownloadHandle), DownloadRunError> {
        let (handle, join) = self.start(request);
        // A task-join failure (the spawned job panicked or was aborted)
        // stays distinct from the job's own terminal outcome: it maps to
        // the engine infrastructure domain and never masks a transfer
        // error reported through the join result.
        let terminal = join.await.map_err(|e| {
            DownloadRunError::Infrastructure(Box::new(EngineFailure {
                error: DownloadError::Protocol(format!("job task panicked: {e}")),
                partial: TransferAccounting::default(),
                artifacts: ArtifactDisposition::default(),
            }))
        })??;
        Ok((terminal, handle))
    }

    /// Body of the job pipeline, shared by both entry points.
    #[allow(clippy::too_many_arguments)]
    async fn run_inner(
        &self,
        request: DownloadRequest,
        state: Arc<StateMachine>,
        cancel: CancellationToken,
        cancel_mode: Arc<std::sync::atomic::AtomicU8>,
        counters: Arc<JobCounters>,
        hub: SharedHub,
        segmented_cell: Arc<std::sync::OnceLock<Arc<crate::job::segmented::SegmentedJob>>>,
        rate_bucket_shared: Arc<crate::control::rate_limit::TokenBucket>,
        global_rate_bucket: Arc<crate::control::rate_limit::TokenBucket>,
        job_memory_high_water: Arc<std::sync::atomic::AtomicU64>,
    ) -> Result<CompletedDownload, DownloadRunError> {
        // The job's transfer-memory ledger (design D3): one instance per
        // run, sharing the controller/engine aggregate pool. Every frame
        // the pipeline holds is admitted through it (task 3.4). The guard
        // publishes the job's accounted-pipeline peak on EVERY exit path.
        let job_ledger =
            std::sync::Arc::new(self.transfer_ledger.job(&self.config.transfer_memory));
        let _memory_guard = MemoryHighWaterGuard {
            ledger: Arc::clone(&job_ledger),
            cell: job_memory_high_water,
        };
        // Overwrite policy pre-check (§14.6): FailIfExists rejects before
        // any network activity.
        if request.overwrite == OverwritePolicy::FailIfExists && request.destination.exists() {
            let error = DownloadError::DestinationConflict(format!(
                "destination exists: {}",
                request.destination.display()
            ));
            return Err(self.terminal_error(
                &state,
                &request,
                error,
                Self::sequential_accounting(
                    &counters,
                    None,
                    Duration::ZERO,
                    ResourceValidators::default(),
                    vec![],
                ),
                ArtifactDisposition::default(),
                CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
            ));
        }

        // Hold destination ownership before any checkpoint resolution/admission
        // or temp output open. The binding lives through worker joins and
        // terminal cleanup, including the awaited segmented completion path.
        let _destination_lease = match DestinationLease::acquire(&request.destination) {
            Ok(lease) => lease,
            Err(error) => {
                return Err(self.terminal_error(
                    &state,
                    &request,
                    error,
                    Self::sequential_accounting(
                        &counters,
                        None,
                        Duration::ZERO,
                        ResourceValidators::default(),
                        vec![],
                    ),
                    ArtifactDisposition::default(),
                    CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                ));
            }
        };

        // ---- Resume admission, phase 1 (§15.5, §7.2): policy-aware
        // checkpoint loading before any network activity. Required-state
        // failures reject before the job enters Probing.
        let identity = crate::resume::flow::job_identity(&request.url, &request.destination);
        // ---- Checkpoint adapter selection (§34): one resolution per job,
        // before any checkpoint operation and before probing. Resolution
        // failure is a checkpoint-category terminal failure.
        let resolve_context = CheckpointResolveContext::new(
            identity.clone(),
            request.destination.clone(),
            match self.config.transfer.durability {
                crate::config::DurabilityMode::Performance => StoreDurability::Performance,
                crate::config::DurabilityMode::Durable => StoreDurability::Durable,
            },
        );
        let selected = match self.checkpoint_resolver.resolve(&resolve_context) {
            Ok(store) => store,
            Err(e) => {
                return Err(self.terminal_error(
                    &state,
                    &request,
                    DownloadError::Checkpoint(e.to_string()),
                    Self::sequential_accounting(
                        &counters,
                        None,
                        Duration::ZERO,
                        ResourceValidators::default(),
                        vec![],
                    ),
                    ArtifactDisposition::default(),
                    CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                ));
            }
        };
        // Per-job mutation coordination: every operation reaches the one
        // selected adapter through a single serialized order (§12).
        let store: Arc<dyn CheckpointStore> =
            Arc::new(crate::resume::coordinated_store::CoordinatedCheckpointStore::new(selected));
        let pending_admission = match crate::resume::flow::begin_admission(
            request.resume,
            &identity,
            TempFileSpec::default().temp_path_for(&request.destination),
            store.as_ref(),
        ) {
            Ok(pending) => pending,
            Err(failure) => {
                Self::emit_resume_notices(&hub, &failure.notices).await;
                return Err(self.terminal_error(
                    &state,
                    &request,
                    failure.error,
                    Self::sequential_accounting(
                        &counters,
                        None,
                        Duration::ZERO,
                        ResourceValidators::default(),
                        vec![],
                    ),
                    ArtifactDisposition::default(),
                    CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                ));
            }
        };

        // ---- Probe (§9.1 Probing) ----
        let _ = state.transition(JobState::Probing);
        hub.emit(Event::StateChanged {
            from: JobState::Created,
            to: JobState::Probing,
        })
        .await;
        crate::observability::log_info(
            &crate::observability::Correlation::new().origin(request.url.clone()),
            "job probing",
        );
        let mut spec = RequestSpec {
            url: request.url.clone(),
            headers: request.headers.clone(),
            identity_encoding: false,
            ..RequestSpec::default()
        };
        if let Some(auth) = &request.authorization {
            spec.headers
                .push(("Authorization".to_string(), auth.clone()));
        }
        let meta: ProbeMetadata;
        let probe_notices: Vec<String>;
        let mut attempt: u32 = 0;
        let mut probe_auth_guard = crate::control::auth::AuthStageGuard::new();
        // Names supplied by the credential provider while probing: their
        // values are credentials under arbitrary names, so the transfer
        // context treats them as sensitive (§21.2, §29).
        let mut provider_header_names: Vec<String> = Vec::new();
        loop {
            // Rebuilt per attempt: credential-provider headers (§29) merge
            // into `spec` before each re-probe.
            let probe_request = ProbeRequest {
                spec: spec.clone(),
                segmentation_threshold: self.config.transfer.segmentation_threshold,
                verify_range_support: self.config.transfer.verify_range_support,
            };
            if cancel.is_cancelled() {
                return Err(self
                    .terminal_cancelled(
                        &state,
                        counters,
                        Duration::ZERO,
                        &hub,
                        CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                        vec![],
                        ArtifactDisposition::default(),
                    )
                    .await);
            }
            // Shared-origin admission: one fair request slot for
            // the probe's dispatch, keyed by the request URL's origin — the
            // FINAL origin is only known after the probe resolves redirects.
            let probe_origin = normalized_origin(&request.url);
            let _probe_permit = match self.admit_origin(probe_origin.as_deref(), &cancel).await {
                Ok(permit) => permit,
                Err(e) => {
                    if matches!(e, DownloadError::Cancelled) || cancel.is_cancelled() {
                        return Err(self
                            .terminal_cancelled(
                                &state,
                                counters,
                                Duration::ZERO,
                                &hub,
                                CancelMode::from_u8(
                                    cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                ),
                                vec![],
                                ArtifactDisposition::default(),
                            )
                            .await);
                    }
                    return Err(self.terminal_error(
                        &state,
                        &request,
                        e,
                        Self::sequential_accounting(
                            &counters,
                            None,
                            Duration::ZERO,
                            ResourceValidators::default(),
                            vec![],
                        ),
                        ArtifactDisposition::default(),
                        CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                    ));
                }
            };
            // Semantic probe (§32): the HTTP layer owns HEAD interpretation
            // and any configured validating range request; the controller
            // never sees raw statuses or headers.
            match self.execution.probe(probe_request.clone(), &cancel).await {
                Ok(outcome) => {
                    self.report_success_origin(probe_origin.as_deref());
                    meta = outcome.metadata;
                    probe_notices = outcome.notices;
                    break;
                }
                Err(HttpFailure {
                    error: e,
                    retry_after,
                    challenge,
                }) => {
                    // Credential challenge at probe time (§29): consult the
                    // provider (bounded stages), attach headers, re-probe.
                    if let (Some(ch), Some(provider)) = (&challenge, &request.credential_provider) {
                        if probe_auth_guard.can_provide() {
                            probe_auth_guard.record();
                            if let Ok(crate::control::auth::CredentialDecision::Headers(hdrs)) =
                                provider.request(ch)
                            {
                                for (k, v) in hdrs {
                                    provider_header_names.push(k.clone());
                                    if let Some(existing) = spec
                                        .headers
                                        .iter_mut()
                                        .find(|(hk, _)| hk.eq_ignore_ascii_case(&k))
                                    {
                                        existing.1 = v;
                                    } else {
                                        spec.headers.push((k, v));
                                    }
                                }
                                continue; // re-probe with credentials
                            }
                        }
                    }
                    // Shared-origin throttle feedback: a 429/503
                    // seen by this job delays every same-origin peer's next
                    // request, with the Retry-After capped by the classifier.
                    if matches!(
                        e.category(),
                        crate::error::ErrorCategory::Server
                            | crate::error::ErrorCategory::RateLimited
                    ) {
                        self.report_throttle_origin(probe_origin.as_deref(), retry_after, attempt);
                    }
                    match self.classifier.decide(&e, attempt, retry_after) {
                        RetryDecision::Retry {
                            attempt: next,
                            delay,
                        } => {
                            counters.worker(0).expect("worker slot").add_retries(1);
                            attempt = next;
                            // Cancellation/deadline interrupts the backoff
                            // instead of sleeping it out (§9.5).
                            tokio::select! {
                                _ = tokio::time::sleep(delay) => {}
                                _ = cancel.cancelled() => {}
                            }
                        }
                        RetryDecision::GiveUp => {
                            return Err(self.terminal_error(
                                &state,
                                &request,
                                e,
                                Self::sequential_accounting(
                                    &counters,
                                    None,
                                    Duration::ZERO,
                                    ResourceValidators::default(),
                                    vec![],
                                ),
                                ArtifactDisposition::default(),
                                CancelMode::from_u8(
                                    cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                ),
                            ));
                        }
                    }
                }
            }
        }
        hub.emit(Event::ProbeCompleted {
            total_size: meta.total_size,
            range_support: meta.accept_ranges,
        })
        .await;
        // HTTP-classified probe notices (§32): e.g. advertised-but-unusable
        // range support.
        for detail in &probe_notices {
            hub.emit(Event::Warning {
                detail: detail.clone(),
            })
            .await;
        }

        // ---- Resume admission, phase 2 (§15.5 steps 2-7) ----
        // One decision: generation safety first (never mixing, §26),
        // then temp-output plausibility, then continue/restart selection
        // with the checkpoint cleanup a restart requires.
        let plan = match pending_admission.finalize(&meta.validators) {
            crate::resume::flow::AdmissionDecision::Proceed(plan) => *plan,
            crate::resume::flow::AdmissionDecision::Reject(failure) => {
                Self::emit_resume_notices(&hub, &failure.notices).await;
                return Err(self.terminal_error(
                    &state,
                    &request,
                    failure.error,
                    Self::sequential_accounting(
                        &counters,
                        None,
                        Duration::ZERO,
                        meta.validators.clone(),
                        vec![],
                    ),
                    ArtifactDisposition::default(),
                    CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                ));
            }
        };
        Self::emit_resume_notices(&hub, plan.notices()).await;
        let warnings: Vec<String> = plan.warnings().to_vec();
        let resume_validators: Option<ResourceValidators> = plan.validators();

        // Size expectation check (§4.1): caller-provided size must match.
        if let (Some(expected), Some(actual)) = (request.expected_size, meta.total_size) {
            if expected != actual {
                return Err(self.terminal_error(
                    &state,
                    &request,
                    DownloadError::Protocol(format!(
                        "expected size {expected} but server reports {actual}"
                    )),
                    Self::sequential_accounting(
                        &counters,
                        None,
                        Duration::ZERO,
                        meta.validators.clone(),
                        vec![],
                    ),
                    ArtifactDisposition::default(),
                    CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                ));
            }
        }

        // ---- Segmentation eligibility (§10.3, §32): one decision, owned by
        // the returned probe metadata. The validating range request (§10.2)
        // already ran inside the HTTP layer; the controller never reconstructs
        // the size/status/advertised/verified formula.
        let eligible = meta.segment_eligible(
            self.config.transfer.segmentation_threshold,
            self.config.transfer.verify_range_support,
        );

        // One authenticated, redirect-sanitized credential context shared
        // by every transfer path (§21.2, §29): the caller's headers plus the
        // authorization convenience plus any headers the credential
        // provider supplied while probing. It is scoped to the resolved
        // final origin, so segmented workers — which request that URL
        // directly — authenticate there while a cross-origin redirect never
        // receives the credentials.
        let shared_credentials = Arc::new(crate::control::auth::SharedCredentials::new(
            crate::http::redirect::scoped_request_headers(
                &request.url,
                &meta.final_url,
                &spec.headers,
                !provider_header_names.is_empty(),
                self.config.network.forward_credentials_cross_origin,
            ),
            request.credential_provider.clone(),
            !provider_header_names.is_empty(),
        ));

        // ---- Prepare (§9.1 Preparing, §14) ----
        let _ = state.transition(JobState::Preparing);
        let resuming = plan.is_resuming();
        // Artifact tracking for terminal diagnostics (§9.4): whether a
        // resumable checkpoint is on disk and whether the temporary output
        // will survive this session at terminal time.
        let mut checkpoint_retained = plan.checkpoint().is_some();
        let mut sink = if resuming {
            // Reuse the admitted temp file without truncating it; the session
            // preserves these bytes unless the caller explicitly aborts.
            match OutputSession::reopen(&request.destination, &TempFileSpec::default()) {
                Ok(sink) => sink,
                Err(error) => {
                    return Err(self.terminal_error(
                        &state,
                        &request,
                        error.0,
                        Self::sequential_accounting(
                            &counters,
                            None,
                            Duration::ZERO,
                            ResourceValidators::default(),
                            vec![],
                        ),
                        // A failed reopen leaves the resumed temp file in place.
                        ArtifactDisposition {
                            temp_retained: resuming,
                            checkpoint_retained,
                        },
                        CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                    ));
                }
            }
        } else {
            match OutputSession::create(
                &request.destination,
                &TempFileSpec::default(),
                self.config.transfer.preallocate_output,
                self.config.transfer.preallocate_physical,
                meta.total_size,
            ) {
                Ok(sink) => sink,
                Err(error) => {
                    return Err(self.terminal_error(
                        &state,
                        &request,
                        error.0,
                        Self::sequential_accounting(
                            &counters,
                            None,
                            Duration::ZERO,
                            ResourceValidators::default(),
                            vec![],
                        ),
                        ArtifactDisposition::default(),
                        CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                    ));
                }
            }
        };
        // ---- Segmented mode dispatch (§12) ----
        if eligible {
            let _ = state.transition(JobState::Running);
            hub.emit(Event::StateChanged {
                from: JobState::Preparing,
                to: JobState::Running,
            })
            .await;
            let started = std::time::Instant::now();
            let total = meta.total_size.expect("eligible requires known size");
            // Preserve the session while segmented workers borrow bounded
            // write handles; ownership is reclaimed only after they join.
            sink.preserve_partial();
            // Pre-set rate limit (set before the probe completed) carries
            // into the segmented job: the SAME stable bucket object (task
            // 5.3), so pre-start and live updates share one bucket.
            let initial_bucket = rate_bucket_shared.clone();
            // Segmented view: every admitted range is reusable (§12.1).
            let resumed = plan.segmented();
            counters
                .worker(0)
                .expect("w")
                .add_reused(resumed.reused_bytes);
            let outcome = crate::job::segmented::run_segmented(
                self.execution.clone(),
                &self.config,
                Arc::clone(&shared_credentials),
                &state,
                &mut sink,
                &store,
                &identity,
                &meta,
                total,
                counters.clone(),
                &hub,
                cancel.clone(),
                cancel_mode.clone(),
                resumed.ranges.to_vec(),
                started,
                checkpoint_retained,
                Some(segmented_cell.clone()),
                initial_bucket,
                self.origin_registry.clone(),
                global_rate_bucket,
                job_ledger,
            )
            .await;
            return self
                .finish_segmented(
                    &request,
                    state,
                    counters,
                    hub,
                    store.as_ref(),
                    &identity,
                    outcome,
                    sink,
                    warnings,
                    checkpoint_retained,
                    &cancel,
                )
                .await;
        }

        // ---- Running: sequential GET with chunked writes (§13) ----
        let _ = state.transition(JobState::Running);
        hub.emit(Event::StateChanged {
            from: JobState::Preparing,
            to: JobState::Running,
        })
        .await;
        let started = std::time::Instant::now();
        // Final-origin identity: every transfer — and its
        // throttle feedback — keys on the probe-resolved final URL's origin,
        // so a redirected resource coordinates with its ACTUAL serving
        // origin rather than the pre-redirect address.
        let transfer_origin = normalized_origin(&meta.final_url);
        // Sequential view: only the contiguous [0, n) prefix is reused;
        // continue at n. Disjoint admitted ranges are rewritten by the
        // sequential stream, not skipped.
        let resumed = plan.sequential();
        counters
            .worker(0)
            .expect("w")
            .add_reused(resumed.reused_bytes);
        let mut offset: u64 = resumed.offset;
        let mut validators = meta.validators.clone();
        let mut attempt: u32 = 0;
        // Checkpoint cadence state (§8.1 checkpoint_flush_interval).
        let mut last_checkpoint = std::time::Instant::now();
        let mut durable = DurableRangeTracker::from_config(self.config.transfer.durability);
        durable.page_cache_ack(offset);

        // Persist an initial checkpoint when resuming (§15.5: carry
        // validators forward so a later resume still validates).
        if resuming {
            if let Some(cp) = plan.checkpoint() {
                let mut fresh = cp.clone();
                fresh.final_url = meta.final_url.clone();
                fresh.set_owned_temp_identity(sink.temp_path());
                fresh.set_covered_digest(sink.temp_path());
                if let Err(e) = self
                    .persist_checkpoint(&job_ledger, &store, &sink, &fresh)
                    .await
                {
                    // A failed refresh cannot promise resumability: stop
                    // with the previous checkpoint untouched (§15.4).
                    return Err(self.checkpoint_save_failed(
                        &state,
                        &request,
                        counters,
                        started.elapsed(),
                        &mut sink,
                        validators.clone(),
                        warnings,
                        e,
                        CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                    ));
                }
            }
        }

        // Request intent (§32): ranged resume with If-Range (§11.3), or full
        // GET when fresh. The HTTP layer validates the response (status,
        // range, generation) before any body byte reaches the sink.
        let conditional_resume = resuming && resume_validators.is_some();
        // Single-stream restarts from zero after a mid-body failure when
        // the body has not been committed (§41 phase 1 exit criterion).
        // When resuming, retries may continue from the durable checkpoint
        // rather than restarting from zero (§17.3, §15.4).
        let mut auth_guard = crate::control::auth::AuthStageGuard::new();
        let mut challenge_headers: Vec<(String, String)> = vec![];
        'download: loop {
            if cancel.is_cancelled() {
                let elapsed = started.elapsed();
                let mode =
                    CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst));
                let cleanup = Self::cleanup_cancelled(mode, &mut sink, store.as_ref(), &identity);
                return Err(self
                    .terminal_cancelled(
                        &state,
                        counters,
                        elapsed,
                        &hub,
                        mode,
                        cleanup.warnings,
                        cleanup.artifacts,
                    )
                    .await);
            }
            // Fast path: nothing left to fetch (resume covered everything).
            if meta.total_size.is_some_and(|t| offset >= t) {
                break 'download;
            }
            // Ranged when continuing at a nonzero offset (§17.3: retry only
            // the unfinished portion) or resuming; full GET when fresh.
            // Credential-provider headers (§29) attach to every re-issued
            // request after a challenge.
            let request_was_ranged = resuming || offset > 0;
            let mut this_spec = if conditional_resume {
                RequestSpec {
                    url: spec.url.clone(),
                    headers: spec.headers.clone(),
                    identity_encoding: true,
                    ..RequestSpec::default()
                }
            } else {
                spec.clone()
            };
            if !challenge_headers.is_empty() {
                for (k, v) in &challenge_headers {
                    if let Some(existing) = this_spec
                        .headers
                        .iter_mut()
                        .find(|(hk, _)| hk.eq_ignore_ascii_case(k))
                    {
                        existing.1 = v.clone();
                    } else {
                        this_spec.headers.push((k.clone(), v.clone()));
                    }
                }
            }
            let intent = if request_was_ranged {
                TransferIntent::Range(RangeIntent {
                    range: (
                        offset,
                        meta.total_size.unwrap_or(u64::MAX).saturating_sub(1),
                    ),
                    established_total: meta.total_size,
                    expected_validators: if resume_validators.is_some() {
                        resume_validators.clone()
                    } else {
                        Some(validators.clone())
                    },
                    full_response: if conditional_resume {
                        // Full representation to an If-Range request: the
                        // resource changed mid-resume (§26).
                        FullResponsePolicy::ResourceChanged
                    } else {
                        // A 200 to a nonzero range is never written as the
                        // requested range (§11.2).
                        FullResponsePolicy::InvalidRange
                    },
                })
            } else {
                TransferIntent::Full
            };
            // Shared-origin admission: one fair request slot held
            // from dispatch until the body is fully consumed (or the attempt
            // fails/cancels); the RAII permit releases it on every exit path.
            let _origin_permit = match self.admit_origin(transfer_origin.as_deref(), &cancel).await
            {
                Ok(permit) => permit,
                Err(e) => {
                    if matches!(e, DownloadError::Cancelled) || cancel.is_cancelled() {
                        let elapsed = started.elapsed();
                        let mode = CancelMode::from_u8(
                            cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                        );
                        let cleanup =
                            Self::cleanup_cancelled(mode, &mut sink, store.as_ref(), &identity);
                        return Err(self
                            .terminal_cancelled(
                                &state,
                                counters,
                                elapsed,
                                &hub,
                                mode,
                                cleanup.warnings,
                                cleanup.artifacts,
                            )
                            .await);
                    }
                    let temp_retained = sink.abort().is_err();
                    return Err(self.terminal_error(
                        &state,
                        &request,
                        e,
                        Self::sequential_accounting(
                            &counters,
                            None,
                            started.elapsed(),
                            validators,
                            warnings,
                        ),
                        ArtifactDisposition {
                            temp_retained,
                            checkpoint_retained,
                        },
                        CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                    ));
                }
            };
            // Semantic transfer (§32): failures carry classified errors,
            // server retry timing, and challenge data — no raw response.
            let response = match self
                .execution
                .transfer(
                    TransferRequest {
                        spec: this_spec,
                        intent,
                    },
                    &cancel,
                )
                .await
            {
                Ok(r) => r,
                Err(HttpFailure {
                    error: e,
                    retry_after,
                    challenge,
                }) => {
                    // Credential challenge (§29): consult the provider at most
                    // MAX_AUTH_STAGES times; never loop authentication.
                    if let (Some(ch), Some(provider)) = (&challenge, &request.credential_provider) {
                        if !auth_guard.can_provide() {
                            let temp_retained = sink.abort().is_err();
                            return Err(self.terminal_error(
                                &state,
                                &request,
                                DownloadError::AuthenticationRequired,
                                Self::sequential_accounting(
                                    &counters,
                                    None,
                                    started.elapsed(),
                                    validators,
                                    warnings,
                                ),
                                ArtifactDisposition {
                                    temp_retained,
                                    checkpoint_retained,
                                },
                                CancelMode::from_u8(
                                    cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                ),
                            ));
                        }
                        auth_guard.record();
                        match provider.request(ch) {
                            Ok(crate::control::auth::CredentialDecision::Headers(hdrs)) => {
                                challenge_headers.clear();
                                for (k, v) in hdrs {
                                    challenge_headers.push((k, v));
                                }
                            }
                            _ => {
                                let temp_retained = sink.abort().is_err();
                                return Err(self.terminal_error(
                                    &state,
                                    &request,
                                    DownloadError::AuthenticationRequired,
                                    Self::sequential_accounting(
                                        &counters,
                                        None,
                                        started.elapsed(),
                                        validators,
                                        warnings,
                                    ),
                                    ArtifactDisposition {
                                        temp_retained,
                                        checkpoint_retained,
                                    },
                                    CancelMode::from_u8(
                                        cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                    ),
                                ));
                            }
                        }
                        continue 'download; // re-issue with provider headers
                    }
                    // Shared-origin throttle feedback: 429/503
                    // seen by this job delays every same-origin peer's next
                    // request, with the Retry-After capped by the classifier.
                    if matches!(
                        e.category(),
                        crate::error::ErrorCategory::Server
                            | crate::error::ErrorCategory::RateLimited
                    ) {
                        self.report_throttle_origin(
                            transfer_origin.as_deref(),
                            retry_after,
                            attempt,
                        );
                    }
                    // Retry classification (§17.1-§17.2) with server-provided
                    // retry timing; single-stream status/transport failures
                    // restart from zero (no committed prefix yet, §41).
                    let temp_retained = sink.abort().is_err();
                    match self.classifier.decide(&e, attempt, retry_after) {
                        RetryDecision::Retry {
                            attempt: next,
                            delay,
                        } => {
                            counters.worker(0).expect("w").add_retries(1);
                            attempt = next;
                            offset = 0;
                            sink = match OutputSession::create(
                                &request.destination,
                                &TempFileSpec::default(),
                                self.config.transfer.preallocate_output,
                                self.config.transfer.preallocate_physical,
                                meta.total_size,
                            ) {
                                Ok(sink) => sink,
                                Err(error) => {
                                    return Err(self.terminal_error(
                                        &state,
                                        &request,
                                        error.0,
                                        Self::sequential_accounting(
                                            &counters,
                                            None,
                                            started.elapsed(),
                                            validators,
                                            warnings,
                                        ),
                                        ArtifactDisposition {
                                            temp_retained,
                                            checkpoint_retained,
                                        },
                                        CancelMode::from_u8(
                                            cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                        ),
                                    ));
                                }
                            };
                            if let Some(status) = e.http_status() {
                                hub.emit(Event::Warning {
                                    detail: format!("status {status} retrying from zero ({e})"),
                                })
                                .await;
                            }
                            tokio::select! {
                                _ = tokio::time::sleep(delay) => {}
                                _ = cancel.cancelled() => {}
                            }
                            continue 'download;
                        }
                        RetryDecision::GiveUp => {
                            // Retryable-but-given-up means the retry budget
                            // exhausted (§17): the structured exhaustion error
                            // keeps sequential and segmented classification in
                            // agreement (§32 parity). Non-retryable failures
                            // surface as themselves.
                            let err = if self.classifier.retryable(&e) {
                                DownloadError::RetryExhausted {
                                    source: Box::new(e),
                                }
                            } else {
                                e
                            };
                            let temp_retained = sink.abort().is_err();
                            return Err(self.terminal_error(
                                &state,
                                &request,
                                err,
                                Self::sequential_accounting(
                                    &counters,
                                    None,
                                    started.elapsed(),
                                    validators,
                                    warnings,
                                ),
                                ArtifactDisposition {
                                    temp_retained,
                                    checkpoint_retained,
                                },
                                CancelMode::from_u8(
                                    cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                ),
                            ));
                        }
                    }
                }
            };
            validators = response.validators.clone();
            // Bounded body delivery (§32): one-chunk demand. The HTTP layer
            // validated status/range/generation before returning the
            // response; chunks arrive zero-copy and no further chunk is
            // fetched until the sink finished the current one (§13.1).
            // Cancellation and pause win a select against a pending read;
            // the source stays owned so a pause resumes byte-exact (§9.3).
            let mut body = response.body;
            let mut written_this_stream: u64 = 0;
            loop {
                // Chunk read with cancellation checks (§9.2 invariant 8).
                if cancel.is_cancelled() {
                    let elapsed = started.elapsed();
                    let mode =
                        CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst));
                    let cleanup =
                        Self::cleanup_cancelled(mode, &mut sink, store.as_ref(), &identity);
                    return Err(self
                        .terminal_cancelled(
                            &state,
                            counters,
                            elapsed,
                            &hub,
                            mode,
                            cleanup.warnings,
                            cleanup.artifacts,
                        )
                        .await);
                }
                match body.next_chunk(&cancel).await {
                    Ok(BodyEvent::Data(data)) => {
                        let len = data.len() as u64;
                        if let Some(max) = request.expected_size {
                            if offset + len > max {
                                // Overshoot is a protocol violation (§11.2).
                                let temp_retained = sink.abort().is_err();
                                return Err(self.terminal_error(
                                    &state,
                                    &request,
                                    DownloadError::Protocol(format!(
                                        "body exceeds expected size {max}"
                                    )),
                                    Self::sequential_accounting(
                                        &counters,
                                        None,
                                        started.elapsed(),
                                        validators,
                                        warnings,
                                    ),
                                    ArtifactDisposition {
                                        temp_retained,
                                        checkpoint_retained,
                                    },
                                    CancelMode::from_u8(
                                        cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                    ),
                                ));
                            }
                        }

                        // Transfer-memory admission (design D3, task 3.4):
                        // the owned chunk is charged to the job ledger from
                        // receipt until its write acknowledges; the RAII
                        // reservation releases on every error/cancel path.
                        // An atomic chunk larger than a cap can never fit:
                        // typed refusal, no wait.
                        let mut reservation = match job_ledger
                            .reserve(crate::io::transfer_ledger::Component::Frames, len)
                            .await
                        {
                            Ok(reservation) => reservation,
                            Err(crate::io::transfer_ledger::LedgerRefusal::Oversize(
                                crate::io::transfer_ledger::OversizeRefusal {
                                    component,
                                    requested,
                                    cap,
                                },
                            )) => {
                                let temp_retained = sink.abort().is_err();
                                return Err(self.terminal_error(
                                    &state,
                                    &request,
                                    DownloadError::MemoryCapExceeded {
                                        component: component.name(),
                                        requested,
                                        cap,
                                    },
                                    Self::sequential_accounting(
                                        &counters,
                                        None,
                                        started.elapsed(),
                                        validators,
                                        warnings,
                                    ),
                                    ArtifactDisposition {
                                        temp_retained,
                                        checkpoint_retained,
                                    },
                                    CancelMode::from_u8(
                                        cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                    ),
                                ));
                            }
                            Err(crate::io::transfer_ledger::LedgerRefusal::NoCapacity(_)) => {
                                unreachable!("reserve waits fairly for capacity")
                            }
                        };

                        // Rate tokens first: payload bytes only (§18.2).
                        // The hierarchical limiter aggregates job and
                        // engine-global levels through the one shared
                        // slowest-wait combiner, so this path cannot diverge
                        // from the segmented path. Waiting stops on
                        // cancellation (prompt pause/cancel interruption);
                        // the next loop pass performs cleanup.
                        {
                            let job_wait = rate_bucket_shared.acquire(len).wait;
                            let global_wait = global_rate_bucket.acquire(len).wait;
                            let wait =
                                crate::control::rate_limit::dominant_wait([job_wait, global_wait]);
                            if let Some(wait) = wait {
                                tokio::select! {
                                    _ = tokio::time::sleep(wait) => {}
                                    _ = cancel.cancelled() => {}
                                }
                            }
                        }

                        // Backpressure: single in-flight chunk; write then
                        // read (§13.1). The chunk retags Frames -> Writer
                        // for the write (single charge; the writer
                        // component cap bounds it).
                        if let Err(refusal) = reservation
                            .retag(crate::io::transfer_ledger::Component::Writer)
                            .await
                        {
                            let (component, requested, cap) = match refusal {
                                crate::io::transfer_ledger::LedgerRefusal::Oversize(
                                    crate::io::transfer_ledger::OversizeRefusal {
                                        component,
                                        requested,
                                        cap,
                                    },
                                ) => (component, requested, cap),
                                crate::io::transfer_ledger::LedgerRefusal::NoCapacity(_) => {
                                    unreachable!("retag waits fairly for capacity")
                                }
                            };
                            let temp_retained = sink.abort().is_err();
                            return Err(self.terminal_error(
                                &state,
                                &request,
                                DownloadError::MemoryCapExceeded {
                                    component: component.name(),
                                    requested,
                                    cap,
                                },
                                Self::sequential_accounting(
                                    &counters,
                                    None,
                                    started.elapsed(),
                                    validators,
                                    warnings,
                                ),
                                ArtifactDisposition {
                                    temp_retained,
                                    checkpoint_retained,
                                },
                                CancelMode::from_u8(
                                    cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                ),
                            ));
                        }
                        if let Err(se) = sink.write_at(offset, &data) {
                            let error = se.0;
                            // A failed write is terminal: route it through
                            // the typed terminal path instead of an unchecked
                            // `?` escape (§13.1, task 1.3).
                            if matches!(error, DownloadError::Cancelled) || cancel.is_cancelled() {
                                let mode = CancelMode::from_u8(
                                    cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                );
                                let cleanup = Self::cleanup_cancelled(
                                    mode,
                                    &mut sink,
                                    store.as_ref(),
                                    &identity,
                                );
                                return Err(self
                                    .terminal_cancelled(
                                        &state,
                                        counters,
                                        started.elapsed(),
                                        &hub,
                                        mode,
                                        cleanup.warnings,
                                        cleanup.artifacts,
                                    )
                                    .await);
                            }
                            let temp_retained = sink.abort().is_err();
                            return Err(self.terminal_error(
                                &state,
                                &request,
                                error,
                                Self::sequential_accounting(
                                    &counters,
                                    None,
                                    started.elapsed(),
                                    validators,
                                    warnings,
                                ),
                                ArtifactDisposition {
                                    temp_retained,
                                    checkpoint_retained,
                                },
                                CancelMode::from_u8(
                                    cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                ),
                            ));
                        }
                        counters.worker(0).expect("w").add_network(len);
                        counters.worker(0).expect("w").add_completed(len);
                        offset += len;
                        durable.page_cache_ack(offset);
                        written_this_stream += len;
                        // The write acknowledged: the chunk is no longer
                        // held (design D3 single charge, released on ack).
                        drop(reservation);

                        // Checkpoint cadence (§8.1): record progress on the
                        // configured interval (§15.4 performance mode).
                        if last_checkpoint.elapsed() >= self.config.checkpoint_flush_interval
                            && offset > 0
                        {
                            last_checkpoint = std::time::Instant::now();
                            let mut cp = plan.checkpoint().cloned().unwrap_or_else(|| {
                                crate::resume::checkpoint::Checkpoint::new(
                                    identity.clone(),
                                    request.url.clone(),
                                    format!("tmp-{}", identity),
                                )
                            });
                            cp.total_size = meta.total_size;
                            cp.validators = validators.clone();
                            cp.final_url = meta.final_url.clone();
                            cp.completed_ranges = vec![(0, offset.saturating_sub(1))];
                            cp.set_owned_temp_identity(sink.temp_path());
                            cp.set_covered_digest(sink.temp_path());
                            if let Err(e) =
                                self.persist_checkpoint(&job_ledger, &store, &sink, &cp)
                                    .await
                            {
                                // A failed cadence save must stop the job:
                                // transfer continues would claim resumable
                                // state that was never persisted (§15.4).
                                return Err(self.checkpoint_save_failed(
                                    &state,
                                    &request,
                                    counters,
                                    started.elapsed(),
                                    &mut sink,
                                    validators.clone(),
                                    warnings,
                                    e,
                                    CancelMode::from_u8(
                                        cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                    ),
                                ));
                            }
                            checkpoint_retained = true;
                            sink.preserve_partial();
                        }
                    }
                    Ok(BodyEvent::End) => break, // clean EOF
                    Ok(BodyEvent::Paused) => {
                        // §9.3: pause converges quickly. Single-stream v1
                        // keeps the temp file, settles writes, and persists a
                        // checkpoint (§9.3 steps 3-5). The body source stays
                        // owned: the next read resumes byte-exact (§32).
                        let _ = sink.flush(FlushLevel::PageCache);
                        durable.page_cache_ack(offset);
                        if meta.total_size.is_some() && offset > 0 {
                            let mut cp = plan.checkpoint().cloned().unwrap_or_else(|| {
                                crate::resume::checkpoint::Checkpoint::new(
                                    identity.clone(),
                                    request.url.clone(),
                                    format!("tmp-{}", identity),
                                )
                            });
                            cp.total_size = meta.total_size;
                            cp.validators = validators.clone();
                            cp.final_url = meta.final_url.clone();
                            cp.completed_ranges = vec![(0, offset.saturating_sub(1))];
                            cp.set_owned_temp_identity(sink.temp_path());
                            cp.set_covered_digest(sink.temp_path());
                            if let Err(e) =
                                self.persist_checkpoint(&job_ledger, &store, &sink, &cp)
                                    .await
                            {
                                // Pause must not claim a resumable state
                                // that failed to persist: stop with the
                                // partial output preserved (§9.3, §15.4).
                                return Err(self.checkpoint_save_failed(
                                    &state,
                                    &request,
                                    counters,
                                    started.elapsed(),
                                    &mut sink,
                                    validators.clone(),
                                    warnings,
                                    e,
                                    CancelMode::from_u8(
                                        cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                    ),
                                ));
                            }
                            checkpoint_retained = true;
                            sink.preserve_partial();
                        }
                        // Wait while paused; the token resolves on resume,
                        // cancellation, or deadline expiry — no fixed polling.
                        let _ = cancel.wait_for_resume().await;
                        if cancel.is_cancelled() {
                            let mode = CancelMode::from_u8(
                                cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                            );
                            let cleanup =
                                Self::cleanup_cancelled(mode, &mut sink, store.as_ref(), &identity);
                            return Err(self
                                .terminal_cancelled(
                                    &state,
                                    counters,
                                    started.elapsed(),
                                    &hub,
                                    mode,
                                    cleanup.warnings,
                                    cleanup.artifacts,
                                )
                                .await);
                        }
                    }
                    Err(DownloadError::Cancelled) => {
                        // Cancellation interrupts a pending read (§32): same
                        // terminal path as an observed cancel.
                        let elapsed = started.elapsed();
                        let mode = CancelMode::from_u8(
                            cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                        );
                        let cleanup =
                            Self::cleanup_cancelled(mode, &mut sink, store.as_ref(), &identity);
                        return Err(self
                            .terminal_cancelled(
                                &state,
                                counters,
                                elapsed,
                                &hub,
                                mode,
                                cleanup.warnings,
                                cleanup.artifacts,
                            )
                            .await);
                    }
                    Err(err) => {
                        // Body fault or read-idle timeout, already classified
                        // by the HTTP layer (§17.1): decide retry from the
                        // durable prefix (§17.3) or give up.
                        match self.classifier.decide(&err, attempt, None) {
                            RetryDecision::Retry {
                                attempt: next,
                                delay,
                            } => {
                                counters.worker(0).expect("w").add_retries(1);
                                counters
                                    .worker(0)
                                    .expect("w")
                                    .add_wasted(written_this_stream);
                                attempt = next;
                                // Retry from the durable prefix (§17.3):
                                // reuse checkpointed offset when a checkpoint
                                // exists, else restart from zero.
                                let durable_through = durable.admissible_through();
                                if durable_through > 0 {
                                    offset = durable_through;
                                    // Keep the temp file; reopen without
                                    // truncate to preserve the prefix.
                                    let _ = sink.flush(FlushLevel::PageCache);
                                    // Prevent the old sink's Drop from
                                    // deleting the temp file we are
                                    // preserving.
                                    sink.preserve_partial();
                                    sink = match OutputSession::reopen(
                                        &request.destination,
                                        &TempFileSpec::default(),
                                    ) {
                                        Ok(sink) => sink,
                                        Err(error) => {
                                            return Err(self.terminal_error(
                                                &state,
                                                &request,
                                                error.0,
                                                Self::sequential_accounting(
                                                    &counters,
                                                    None,
                                                    started.elapsed(),
                                                    validators,
                                                    warnings,
                                                ),
                                                ArtifactDisposition {
                                                    temp_retained: true,
                                                    checkpoint_retained,
                                                },
                                                CancelMode::from_u8(
                                                    cancel_mode
                                                        .load(std::sync::atomic::Ordering::SeqCst),
                                                ),
                                            ));
                                        }
                                    };
                                } else {
                                    offset = 0;
                                    let temp_retained = sink.abort().is_err();
                                    sink = match OutputSession::create(
                                        &request.destination,
                                        &TempFileSpec::default(),
                                        self.config.transfer.preallocate_output,
                                        self.config.transfer.preallocate_physical,
                                        meta.total_size,
                                    ) {
                                        Ok(sink) => sink,
                                        Err(error) => {
                                            return Err(self.terminal_error(
                                                &state,
                                                &request,
                                                error.0,
                                                Self::sequential_accounting(
                                                    &counters,
                                                    None,
                                                    started.elapsed(),
                                                    validators,
                                                    warnings,
                                                ),
                                                ArtifactDisposition {
                                                    temp_retained,
                                                    checkpoint_retained,
                                                },
                                                CancelMode::from_u8(
                                                    cancel_mode
                                                        .load(std::sync::atomic::Ordering::SeqCst),
                                                ),
                                            ));
                                        }
                                    };
                                }
                                hub.emit(Event::Warning {
                                    detail: format!(
                                        "stream reset; retrying from offset {offset} ({err})"
                                    ),
                                })
                                .await;
                                tokio::select! {
                                    _ = tokio::time::sleep(delay) => {}
                                    _ = cancel.cancelled() => {}
                                }
                                continue 'download;
                            }
                            RetryDecision::GiveUp => {
                                let temp_retained = sink.abort().is_err();
                                // Same exhaustion shaping as the transfer path
                                // (§32 parity): retryable failures that exhausted
                                // the budget report RetryExhausted.
                                let err = if self.classifier.retryable(&err) {
                                    DownloadError::RetryExhausted {
                                        source: Box::new(err),
                                    }
                                } else {
                                    err
                                };
                                return Err(self.terminal_error(
                                    &state,
                                    &request,
                                    err,
                                    Self::sequential_accounting(
                                        &counters,
                                        None,
                                        started.elapsed(),
                                        validators,
                                        warnings,
                                    ),
                                    ArtifactDisposition {
                                        temp_retained,
                                        checkpoint_retained,
                                    },
                                    CancelMode::from_u8(
                                        cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                    ),
                                ));
                            }
                        }
                    }
                }
            }

            // Stream finished cleanly: record one successful origin request
            //, then proceed to verification.
            self.report_success_origin(transfer_origin.as_deref());
            break 'download;
        }

        let total_size = meta.total_size.or(request.expected_size);
        let accounting = Self::sequential_accounting(
            &counters,
            total_size,
            started.elapsed(),
            validators,
            warnings,
        );
        self.complete_verified_output(
            &request,
            &state,
            &hub,
            store.as_ref(),
            &identity,
            sink,
            accounting,
            checkpoint_retained,
            offset,
            &cancel,
        )
        .await
    }

    /// Cleanup for a cancelled job per [`CancelMode`] (§9.4):
    /// DeletePartial removes temp+checkpoint; KeepPartial keeps both;
    /// KeepFileDiscardCheckpoint removes only the checkpoint.
    ///
    /// Returns the cleanup warnings and the resulting artifact
    /// disposition: deletion happens after the cancellation outcome is
    /// already determined, so a delete failure is surfaced as an
    /// actionable warning (and a retained artifact) instead of rewriting
    /// the result (§9.2: `Cancelled` is defined by the caller's request).
    pub(crate) fn cleanup_cancelled<S: Sink + PartialArtifactOwner>(
        mode: CancelMode,
        sink: &mut S,
        store: &dyn CheckpointStore,
        identity: &str,
    ) -> CancelCleanup {
        let mut warnings = Vec::new();
        let mut artifacts = ArtifactDisposition::default();
        match mode {
            CancelMode::DeletePartial => {
                artifacts.temp_retained = sink.abort().is_err();
                if let Err(e) = store.delete(identity) {
                    artifacts.checkpoint_retained = true;
                    warnings.push(Self::checkpoint_cleanup_warning(e));
                }
            }
            CancelMode::KeepPartial => {
                let _ = sink.flush(FlushLevel::PageCache);
                sink.preserve_partial();
                artifacts = ArtifactDisposition {
                    temp_retained: true,
                    checkpoint_retained: true,
                };
            }
            CancelMode::KeepFileDiscardCheckpoint => {
                let _ = sink.flush(FlushLevel::PageCache);
                sink.preserve_partial();
                artifacts.temp_retained = true;
                if let Err(e) = store.delete(identity) {
                    artifacts.checkpoint_retained = true;
                    warnings.push(Self::checkpoint_cleanup_warning(e));
                }
            }
        }
        CancelCleanup {
            warnings,
            artifacts,
        }
    }

    /// The actionable warning for incomplete checkpoint cleanup after an
    /// irreversible terminal decision (§9.2, §14.6).
    fn checkpoint_cleanup_warning(error: crate::resume::CheckpointError) -> String {
        format!("checkpoint cleanup incomplete ({error}); manual cleanup may be required")
    }

    /// Publish admission notification facts as events. The resume module
    /// decides; the job owns the observable effect (§26: ResourceChanged
    /// precedes any terminal transition).
    async fn emit_resume_notices(hub: &SharedHub, notices: &[crate::resume::flow::ResumeNotice]) {
        for notice in notices {
            match notice {
                crate::resume::flow::ResumeNotice::ResourceChanged { detail } => {
                    hub.emit(Event::ResourceChanged {
                        detail: detail.clone(),
                    })
                    .await;
                }
            }
        }
    }

    /// Post-segmented-transfer completion path: verify size + hashes over
    /// the assembled temp file (§16.2 sequential read), then commit (§14.6).
    #[allow(clippy::too_many_arguments)]
    async fn finish_segmented(
        &self,
        request: &DownloadRequest,
        state: Arc<StateMachine>,
        _counters: Arc<JobCounters>,
        hub: SharedHub,
        store: &dyn CheckpointStore,
        identity: &str,
        outcome: crate::job::segmented::SegmentedOutcome,
        sink: OutputSession,
        mut warnings: Vec<String>,
        checkpoint_retained: bool,
        cancel: &crate::control::CancellationToken,
    ) -> Result<CompletedDownload, DownloadRunError> {
        if outcome.status == ResultStatus::Cancelled {
            let _ = state.transition(JobState::Cancelling);
            let _ = state.transition(JobState::Cancelled);
            // Segmented cleanup warnings (delete failures) join the
            // admission warnings; the outcome remains Cancelled (§9.2).
            warnings.extend(outcome.warnings.clone());
            return Err(DownloadRunError::Cancelled(Box::new(CancellationSummary {
                mode: outcome.cancel_mode,
                partial: Self::segmented_accounting(&outcome, warnings),
                artifacts: outcome.artifacts,
            })));
        }
        warnings.extend(outcome.warnings.clone());
        if outcome.status == ResultStatus::Failed {
            let accounting = Self::segmented_accounting(&outcome, warnings);
            let error = outcome
                .error
                .expect("failed segmented outcome carries its error");
            return Err(self.terminal_error(
                &state,
                request,
                error,
                accounting,
                outcome.artifacts,
                outcome.cancel_mode,
            ));
        }
        let accounting = Self::segmented_accounting(&outcome, warnings);
        let accepted_bytes: u64 = outcome
            .completed_ranges
            .iter()
            .map(|(start, end)| end.saturating_sub(*start).saturating_add(1))
            .sum();
        self.complete_verified_output(
            request,
            &state,
            &hub,
            store,
            identity,
            sink,
            accounting,
            checkpoint_retained,
            accepted_bytes,
            cancel,
        )
        .await
    }

    /// Sequence verification, durable finalization, publication, checkpoint
    /// cleanup, and the terminal result for a successful transfer.
    #[allow(clippy::too_many_arguments)]
    async fn complete_verified_output(
        &self,
        request: &DownloadRequest,
        state: &Arc<StateMachine>,
        hub: &SharedHub,
        store: &dyn CheckpointStore,
        identity: &str,
        mut sink: OutputSession,
        mut accounting: TransferAccounting,
        checkpoint_retained: bool,
        // Acknowledged accepted coverage for this job (admitted
        // checkpoint ranges plus bytes written under this session).
        accepted_bytes: u64,
        cancel: &crate::control::CancellationToken,
    ) -> Result<CompletedDownload, DownloadRunError> {
        // On a verification/commit failure the temp file survives exactly
        // when the session preserves it on drop (segmented runs preserve
        // the assembled file for diagnosis); the checkpoint state is
        // unchanged because deletion happens only after a successful
        // commit.
        let artifacts_on_failure = ArtifactDisposition {
            temp_retained: sink.preserves_partial_on_drop(),
            checkpoint_retained,
        };
        // Verification obeys the same deadline/cancellation token as the
        // transfer (§9.5): an expiry before the commit boundary fails
        // safely and publishes nothing.
        if cancel.is_cancelled() {
            return Err(self.terminal_error(
                state,
                request,
                cancellation_error(cancel),
                accounting,
                artifacts_on_failure,
                CancelMode::DeletePartial,
            ));
        }
        let _ = state.transition(JobState::Verifying);
        hub.emit(Event::IntegrityCheckStarted).await;
        let sink_size = match sink.size() {
            Ok(size) => size,
            Err(error) => {
                let error = error.0;
                hub.emit(Event::IntegrityCheckFailed {
                    detail: error.to_string(),
                })
                .await;
                return Err(self.terminal_error(
                    state,
                    request,
                    error,
                    accounting,
                    artifacts_on_failure,
                    CancelMode::DeletePartial,
                ));
            }
        };
        if let Some(expected) = accounting.total_size {
            if accepted_bytes != expected || sink_size != expected {
                let error = DownloadError::IntegrityMismatch(format!(
                    "size mismatch: acknowledged {accepted_bytes}, got {sink_size}, expected {expected}"
                ));
                hub.emit(Event::IntegrityCheckFailed {
                    detail: error.to_string(),
                })
                .await;
                return Err(self.terminal_error(
                    state,
                    request,
                    error,
                    accounting,
                    artifacts_on_failure,
                    CancelMode::DeletePartial,
                ));
            }
        } else if sink_size != accepted_bytes {
            // Unknown length: the successful stream's acknowledged end
            // defines the output; drop any preallocated or stale tail (§25).
            if let Err(error) = sink.truncate_to(accepted_bytes) {
                hub.emit(Event::IntegrityCheckFailed {
                    detail: error.to_string(),
                })
                .await;
                return Err(self.terminal_error(
                    state,
                    request,
                    error.0,
                    accounting,
                    artifacts_on_failure,
                    CancelMode::DeletePartial,
                ));
            }
        }
        if !request.integrity.expected_hashes.is_empty() {
            if let Err(error) = sink.verification_read() {
                hub.emit(Event::IntegrityCheckFailed {
                    detail: error.to_string(),
                })
                .await;
                return Err(self.terminal_error(
                    state,
                    request,
                    error,
                    accounting,
                    artifacts_on_failure,
                    CancelMode::DeletePartial,
                ));
            }
            match verify_hashes(&request.integrity, sink.temp_path(), cancel) {
                Ok(()) => hub.emit(Event::IntegrityCheckPassed).await,
                Err(error) => {
                    hub.emit(Event::IntegrityCheckFailed {
                        detail: error.to_string(),
                    })
                    .await;
                    return Err(self.terminal_error(
                        state,
                        request,
                        error,
                        accounting,
                        artifacts_on_failure,
                        CancelMode::DeletePartial,
                    ));
                }
            }
        }
        // Publication commit boundary (§9.5, design D5): once the atomic
        // publication succeeds the outcome is success, truthfully — but an
        // expiry that arrives before it fails closed without publishing.
        if cancel.is_cancelled() {
            return Err(self.terminal_error(
                state,
                request,
                cancellation_error(cancel),
                accounting,
                artifacts_on_failure,
                CancelMode::DeletePartial,
            ));
        }
        let _ = state.transition(JobState::Committing);
        if let Err(error) = sink.finalize() {
            return Err(self.terminal_error(
                state,
                request,
                error.0,
                accounting,
                artifacts_on_failure,
                CancelMode::DeletePartial,
            ));
        }
        let (final_path, publication_warning) =
            match sink.commit_with_policy(publication_mode(request.overwrite)) {
                Ok(result) => result,
                Err(error) => {
                    return Err(self.terminal_error(
                        state,
                        request,
                        error.0,
                        accounting,
                        artifacts_on_failure,
                        CancelMode::DeletePartial,
                    ));
                }
            };
        if let Some(warning) = publication_warning {
            hub.emit(Event::Warning {
                detail: warning.clone(),
            })
            .await;
            accounting.warnings.push(warning);
        }
        let cleanup_warnings = match store.delete(identity) {
            Ok(()) => Vec::new(),
            Err(error) => vec![Self::checkpoint_cleanup_warning(error)],
        };
        for warning in &cleanup_warnings {
            hub.emit(Event::Warning {
                detail: warning.clone(),
            })
            .await;
        }
        accounting.warnings.extend(cleanup_warnings);
        let _ = state.transition(JobState::Completed);
        hub.emit(Event::Committed {
            path: final_path.display().to_string(),
        })
        .await;
        Ok(CompletedDownload {
            final_path,
            accounting,
        })
    }

    /// Fold job counters into transfer accounting for a single-stream run.
    fn sequential_accounting(
        counters: &JobCounters,
        total_size: Option<u64>,
        elapsed: Duration,
        validators: ResourceValidators,
        warnings: Vec<String>,
    ) -> TransferAccounting {
        let snap = counters.fold();
        TransferAccounting {
            bytes_downloaded_from_network: snap.network_bytes,
            bytes_reused_from_checkpoint: snap.reused_bytes,
            completed_bytes: snap.completed_bytes,
            wasted_bytes: snap.wasted_bytes,
            retries: snap.retries,
            segment_requests: 0,
            live_splits: 0,
            total_size,
            elapsed,
            validators,
            warnings,
        }
    }

    /// Fold a segmented outcome into transfer accounting.
    fn segmented_accounting(
        outcome: &crate::job::segmented::SegmentedOutcome,
        warnings: Vec<String>,
    ) -> TransferAccounting {
        TransferAccounting {
            bytes_downloaded_from_network: outcome.network_bytes,
            bytes_reused_from_checkpoint: outcome.reused_bytes,
            completed_bytes: outcome.completed_bytes,
            wasted_bytes: outcome.wasted_bytes,
            retries: outcome.retries,
            segment_requests: outcome.segment_requests,
            live_splits: outcome.live_splits,
            total_size: Some(outcome.total_size),
            elapsed: outcome.elapsed,
            validators: outcome.validators.clone(),
            warnings,
        }
    }

    /// Persist one checkpoint under transfer-memory admission (task 3.6):
    /// the checkpoint's estimated serialized size is charged to the
    /// Checkpoint component BEFORE the store allocates; a refusal (an
    /// oversized/fragmented checkpoint) is a typed failure through the
    /// same safe path as any save failure — never an overclaim.
    async fn save_checkpoint_bounded(
        &self,
        job_ledger: &crate::io::transfer_ledger::JobLedger,
        store: &Arc<dyn CheckpointStore>,
        checkpoint: &crate::resume::checkpoint::Checkpoint,
    ) -> Result<(), crate::resume::checkpoint::CheckpointError> {
        let estimate = checkpoint.checked_serialized_size()?;
        let reservation = job_ledger
            .reserve(crate::io::transfer_ledger::Component::Checkpoint, estimate)
            .await
            .map_err(|refusal| match refusal {
                crate::io::transfer_ledger::LedgerRefusal::Oversize(
                    crate::io::transfer_ledger::OversizeRefusal {
                        requested: size,
                        cap,
                        ..
                    },
                ) => crate::resume::checkpoint::CheckpointError::TooLarge { size, cap },
                crate::io::transfer_ledger::LedgerRefusal::NoCapacity(_) => {
                    unreachable!("reserve waits fairly for capacity")
                }
            })?;
        let result = store.save_atomic(checkpoint);
        drop(reservation);
        result
    }

    /// Persist one checkpoint through the shared mode-aware durability
    /// ordering: in Durable mode the output data is synchronized before
    /// the checkpoint metadata is allowed to persist (§15.4). Failure of
    /// either step surfaces as a checkpoint-category error, so the job
    /// stops without advertising unsynced ranges.
    async fn persist_checkpoint(
        &self,
        job_ledger: &crate::io::transfer_ledger::JobLedger,
        store: &Arc<dyn CheckpointStore>,
        sink: &OutputSession,
        checkpoint: &crate::resume::checkpoint::Checkpoint,
    ) -> Result<(), crate::resume::checkpoint::CheckpointError> {
        if self.config.transfer.durability == crate::config::DurabilityMode::Durable {
            let sync = sink.sync_capability().map_err(|e| {
                crate::resume::checkpoint::CheckpointError::Corrupt(format!(
                    "output sync capability unavailable: {}",
                    e.0
                ))
            })?;
            crate::io::output_session::sync_data_before_checkpoint(true, sync)
                .await
                .map_err(|e| {
                    crate::resume::checkpoint::CheckpointError::Corrupt(format!(
                        "data sync before checkpoint failed: {}",
                        e.0
                    ))
                })?;
        }
        self.save_checkpoint_bounded(job_ledger, store, checkpoint)
            .await
    }

    /// Route one terminal engine error through the once-only terminal
    /// transition (Failing -> Failed), terminal logging, and typed
    /// failure classification (§20, design D1). Every non-success,
    /// non-cancellation terminal branch funnels here exactly once; the
    /// failure domain of the classified error selects the typed variant.
    #[allow(clippy::too_many_arguments)]
    fn terminal_error(
        &self,
        state: &Arc<StateMachine>,
        request: &DownloadRequest,
        error: DownloadError,
        accounting: TransferAccounting,
        artifacts: ArtifactDisposition,
        cancel_mode: CancelMode,
    ) -> DownloadRunError {
        let _ = state.transition(JobState::Failing);
        let _ = state.transition(JobState::Failed);
        crate::observability::log_terminal_error(
            &crate::observability::Correlation::new().origin(request.url.clone()),
            &error,
        );
        match error.domain() {
            FailureDomain::Transfer => DownloadRunError::Transfer(Box::new(TransferFailure {
                error,
                partial: accounting,
                artifacts,
            })),
            FailureDomain::Infrastructure => {
                DownloadRunError::Infrastructure(Box::new(EngineFailure {
                    error,
                    partial: accounting,
                    artifacts,
                }))
            }
            // Defensive: a cancellation-classified error that reached a
            // failure path is still a caller cancellation — report the
            // typed cancellation summary rather than a failure.
            FailureDomain::Cancelled => {
                DownloadRunError::Cancelled(Box::new(CancellationSummary {
                    mode: cancel_mode,
                    partial: accounting,
                    artifacts,
                }))
            }
        }
    }

    /// Map a checkpoint save failure to the structured fatal path
    /// (§15.4, design §4): the job stops with a checkpoint-category
    /// error, keeps consistent partial output for diagnosis or recovery,
    /// and never deletes the previous checkpoint (atomic replacement
    /// leaves the last complete state usable).
    #[allow(clippy::too_many_arguments)]
    fn checkpoint_save_failed(
        &self,
        state: &Arc<StateMachine>,
        request: &DownloadRequest,
        counters: Arc<JobCounters>,
        elapsed: Duration,
        sink: &mut (impl Sink + PartialArtifactOwner),
        validators: ResourceValidators,
        warnings: Vec<String>,
        error: crate::resume::CheckpointError,
        cancel_mode: CancelMode,
    ) -> DownloadRunError {
        // Preserve the partial output beyond the last good checkpoint.
        let _ = sink.flush(FlushLevel::PageCache);
        sink.preserve_partial();
        self.terminal_error(
            state,
            request,
            DownloadError::Checkpoint(error.to_string()),
            Self::sequential_accounting(&counters, None, elapsed, validators, warnings),
            // The preserved output and the last complete checkpoint state
            // both remain available.
            ArtifactDisposition {
                temp_retained: true,
                checkpoint_retained: true,
            },
            cancel_mode,
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn terminal_cancelled(
        &self,
        state: &Arc<StateMachine>,
        counters: Arc<JobCounters>,
        elapsed: Duration,
        hub: &SharedHub,
        mode: CancelMode,
        warnings: Vec<String>,
        artifacts: ArtifactDisposition,
    ) -> DownloadRunError {
        // Cleanup warnings are published before the terminal transition:
        // the outcome stays Cancelled, the observation is not hidden.
        for warning in &warnings {
            hub.emit(Event::Warning {
                detail: warning.clone(),
            })
            .await;
        }
        let _ = state.transition(JobState::Cancelling);
        let _ = state.transition(JobState::Cancelled);
        let accounting = Self::sequential_accounting(
            &counters,
            None,
            elapsed,
            ResourceValidators::default(),
            warnings,
        );
        DownloadRunError::Cancelled(Box::new(CancellationSummary {
            mode,
            partial: accounting,
            artifacts,
        }))
    }
}

/// Chunked SHA-256/SHA-512 verification of the completed temporary file.
///
/// Cancellation and deadline expiry are checked between bounded chunks so
/// an expiry during verification fails promptly without publishing.
fn verify_hashes(
    integrity: &IntegrityPolicy,
    path: &Path,
    cancel: &crate::control::CancellationToken,
) -> Result<(), DownloadError> {
    if cancel.is_cancelled() {
        return Err(cancellation_error(cancel));
    }
    for expected in &integrity.expected_hashes {
        let computed = match expected.algorithm {
            HashAlgorithm::Sha256 => hash_file_bounded::<Sha256>(path, cancel)?,
            HashAlgorithm::Sha512 => hash_file_bounded::<Sha512>(path, cancel)?,
        };
        if computed != expected.hex.to_ascii_lowercase() {
            return Err(DownloadError::IntegrityMismatch(format!(
                "{:?} digest mismatch: expected {}, computed {}",
                expected.algorithm, expected.hex, computed
            )));
        }
    }
    Ok(())
}

/// One bounded read pass feeding `D`; cancellation is honored between
/// 256 KiB chunks.
fn hash_file_bounded<D: sha2::Digest>(
    path: &Path,
    cancel: &crate::control::CancellationToken,
) -> Result<String, DownloadError> {
    use std::io::Read as _;

    let file = std::fs::File::open(path).map_err(|error| DownloadError::from_io(&error))?;
    let mut reader = std::io::BufReader::with_capacity(256 * 1024, file);
    let mut buffer = vec![0u8; 256 * 1024];
    let mut hasher = D::new();
    loop {
        if cancel.is_cancelled() {
            return Err(cancellation_error(cancel));
        }
        let read = reader
            .read(&mut buffer)
            .map_err(|error| DownloadError::SinkWrite(error.to_string()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex(&hasher.finalize()))
}

/// Typed terminal error for a cancelled or expired wait: deadline expiry
/// maps to `DeadlineExceeded`, caller cancellation to `Cancelled`.
fn cancellation_error(cancel: &crate::control::CancellationToken) -> DownloadError {
    if cancel.deadline_exceeded() {
        DownloadError::DeadlineExceeded
    } else {
        DownloadError::Cancelled
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Publishes the job's accounted-pipeline high-water into the run's metrics
/// cell on every exit path (task 3.7): drop runs on success, failure,
/// cancellation and panic alike.
struct MemoryHighWaterGuard {
    ledger: std::sync::Arc<crate::io::transfer_ledger::JobLedger>,
    cell: Arc<std::sync::atomic::AtomicU64>,
}

impl Drop for MemoryHighWaterGuard {
    fn drop(&mut self) {
        self.cell.store(
            self.ledger.job_high_water(),
            std::sync::atomic::Ordering::SeqCst,
        );
    }
}

#[cfg(test)]
mod completion_tests {
    use super::*;
    use crate::config::{ExpectedHash, HashAlgorithm, IntegrityPolicy};
    use crate::error::ErrorCategory;
    use crate::http::scripted::{ProbeStep, ScriptedHttp, TransferOk, TransferStep};
    use crate::io::fault_script::{OutputFaultScript, OutputOperation};
    use crate::resume::checkpoint_store::FileCheckpointStore;
    use sha2::{Digest, Sha256};

    #[tokio::test]
    async fn finalization_failure_is_structured_and_preserves_old_destination() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");
        std::fs::write(&destination, b"previous destination").expect("seed old destination");
        let mut sink = OutputSession::create(
            &destination,
            &TempFileSpec::default(),
            false,
            false,
            Some(3),
        )
        .expect("open output session");
        sink.write_at(0, b"new").expect("write temp output");
        sink.fail_next_flush();
        let controller = DownloadController::with_execution(
            HttpExecution::from_adapter(ScriptedHttp::new()),
            EngineConfig::default(),
        );
        let mut request =
            DownloadRequest::new("https://completion.test/output", destination.clone());
        request.overwrite = OverwritePolicy::Replace;
        let state = StateMachine::new();
        state
            .transition(JobState::Probing)
            .expect("created to probing");
        state
            .transition(JobState::Preparing)
            .expect("probing to preparing");
        state
            .transition(JobState::Running)
            .expect("preparing to running");
        let (hub, mut events) = EventHub::new(16, Duration::from_secs(1));
        let hub = Arc::new(hub);
        let store = FileCheckpointStore::new(directory.path(), StoreDurability::Performance)
            .expect("checkpoint store");
        let result = controller
            .complete_verified_output(
                &request,
                &state,
                &hub,
                &store,
                "test-identity",
                sink,
                TransferAccounting {
                    bytes_downloaded_from_network: 3,
                    bytes_reused_from_checkpoint: 0,
                    completed_bytes: 3,
                    wasted_bytes: 0,
                    retries: 0,
                    segment_requests: 0,
                    live_splits: 0,
                    total_size: Some(3),
                    elapsed: Duration::ZERO,
                    validators: ResourceValidators::default(),
                    warnings: Vec::new(),
                },
                false,
                3,
                &crate::control::CancellationToken::new(),
            )
            .await;

        let error = result.expect_err("finalize failure is terminal");
        assert_eq!(error.category(), crate::error::ErrorCategory::SinkWrite);
        assert_eq!(state.get(), JobState::Failed);
        assert_eq!(
            std::fs::read(&destination).expect("old destination remains"),
            b"previous destination"
        );
        assert!(matches!(
            events.try_next(),
            Some(Event::IntegrityCheckStarted)
        ));
        assert!(
            events.try_next().is_none(),
            "finalize failure cannot commit"
        );
    }

    const FAULT_TOTAL: u64 = 4000;

    struct FaultObservation {
        completed: bool,
        category: Option<ErrorCategory>,
        operations: Vec<OutputOperation>,
        committed: bool,
        destination: Vec<u8>,
        part_exists: bool,
        destination_at_publish: Option<Vec<u8>>,
        state_at_publish: Option<JobState>,
    }

    fn fault_config(segmented: bool) -> EngineConfig {
        let mut config = EngineConfig::default();
        config.transfer.preallocate_output = false;
        config.transfer.segmentation_threshold = if segmented { 1024 } else { u64::MAX };
        config.transfer.max_workers = 4;
        config.transfer.min_workers = 1;
        config.transfer.max_segment_size = 1000;
        config.transfer.min_segment_size = 1;
        config.transfer.verify_range_support = false;
        config.checkpoint_flush_interval = Duration::from_secs(60);
        config
    }

    fn fault_http(segmented: bool, content: &[u8]) -> ScriptedHttp {
        let scripted = ScriptedHttp::new().expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(FAULT_TOTAL),
            accept_ranges: true,
            range_verified: true,
            ..ProbeMetadata::default()
        }));
        if segmented {
            let ranges = (0..FAULT_TOTAL)
                .step_by(1000)
                .map(|start| {
                    let end = (start + 999).min(FAULT_TOTAL - 1);
                    TransferStep::new().range((start, end)).ok(TransferOk::new()
                        .range(start, end)
                        .total(FAULT_TOTAL)
                        .chunk(content[start as usize..=end as usize].to_vec()))
                })
                .collect();
            scripted.expect_unordered_ranges("output-faults", ranges)
        } else {
            scripted.expect_transfer(
                TransferStep::new()
                    .ok(TransferOk::new().total(FAULT_TOTAL).chunk(content.to_vec())),
            )
        }
    }

    fn digest_hex(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn injected_error(operation: OutputOperation) -> DownloadError {
        match operation {
            OutputOperation::Open => DownloadError::SinkOpen("scripted open fault".into()),
            OutputOperation::Publish => DownloadError::Commit("scripted publish fault".into()),
            OutputOperation::Write
            | OutputOperation::Flush
            | OutputOperation::VerificationRead
            | OutputOperation::Finalize
            | OutputOperation::Cleanup => {
                DownloadError::SinkWrite(format!("scripted {operation:?} fault"))
            }
        }
    }

    async fn run_output_fault(segmented: bool, operation: OutputOperation) -> FaultObservation {
        let content: Vec<u8> = (0..FAULT_TOTAL)
            .map(|index| ((index * 17 + 3) % 251) as u8)
            .collect();
        let scripted = fault_http(segmented, &content);
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");
        std::fs::write(&destination, b"previous destination").expect("seed old destination");
        let registration = OutputFaultScript::register(&destination);
        registration
            .script()
            .fail_next(operation, injected_error(operation));
        let gate = (operation == OutputOperation::Publish)
            .then(|| registration.script().hold_next(OutputOperation::Publish));
        let controller = DownloadController::with_execution(
            HttpExecution::from_adapter(scripted),
            fault_config(segmented),
        );
        let mut request = DownloadRequest::new(
            "https://completion.test/faulted-output",
            destination.clone(),
        );
        request.overwrite = OverwritePolicy::Replace;
        request.expected_size = Some(FAULT_TOTAL);
        request.integrity = IntegrityPolicy {
            expected_hashes: vec![ExpectedHash {
                algorithm: HashAlgorithm::Sha256,
                hex: digest_hex(&content),
            }],
            ..IntegrityPolicy::default()
        };
        let (handle, task) = controller.start(request);
        let mut events = handle.events();
        let (destination_at_publish, state_at_publish) = if let Some(gate) = gate {
            let gate = tokio::task::spawn_blocking(move || {
                gate.wait_until_entered();
                gate
            })
            .await
            .expect("publish gate waiter");
            let bytes = std::fs::read(&destination).expect("old destination during publish");
            let state = handle.state();
            gate.release();
            (Some(bytes), Some(state))
        } else {
            (None, None)
        };
        let terminal = task.await.expect("job task");
        let (completed, category) = match terminal {
            Ok(_) => (true, None),
            Err(error) => (false, Some(error.category())),
        };
        let committed = std::iter::from_fn(|| events.try_next())
            .any(|event| matches!(event, Event::Committed { .. }));
        let part = TempFileSpec::default().temp_path_for(&destination);
        FaultObservation {
            completed,
            category,
            operations: registration.script().operations(),
            committed,
            destination: std::fs::read(&destination).expect("old destination remains"),
            part_exists: part.exists(),
            destination_at_publish,
            state_at_publish,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scripted_output_faults_are_ordered_and_fail_closed_in_both_modes() {
        let cases = [
            OutputOperation::Open,
            OutputOperation::Write,
            OutputOperation::Flush,
            OutputOperation::VerificationRead,
            OutputOperation::Finalize,
            OutputOperation::Publish,
        ];
        for operation in cases {
            for segmented in [false, true] {
                let observed = run_output_fault(segmented, operation).await;
                assert_eq!(
                    observed.category,
                    Some(injected_error(operation).category()),
                    "{operation:?}, segmented={segmented}: structured category"
                );
                assert!(!observed.completed);
                assert!(!observed.committed, "{operation:?}: no false commit");
                assert_eq!(observed.destination, b"previous destination");
                assert_eq!(observed.operations.first(), Some(&OutputOperation::Open));
                assert!(
                    observed.operations.contains(&operation),
                    "{:?}",
                    observed.operations
                );
                let expected_partial = match operation {
                    OutputOperation::Open => false,
                    OutputOperation::Write
                    | OutputOperation::Flush
                    | OutputOperation::VerificationRead
                    | OutputOperation::Finalize => segmented,
                    OutputOperation::Publish => true,
                    OutputOperation::Cleanup => unreachable!(),
                };
                assert_eq!(
                    observed.part_exists, expected_partial,
                    "{operation:?}, segmented={segmented}: partial artifact disposition {:?}",
                    observed.operations
                );
                if operation == OutputOperation::Open {
                    assert!(!observed.completed);
                    assert_eq!(observed.operations, [OutputOperation::Open]);
                }
                if operation == OutputOperation::Publish {
                    assert_eq!(
                        observed.destination_at_publish.as_deref(),
                        Some(&b"previous destination"[..])
                    );
                    assert_eq!(observed.state_at_publish, Some(JobState::Committing));
                    assert_eq!(observed.operations.last(), Some(&OutputOperation::Publish));
                }
                if operation == OutputOperation::Finalize {
                    let flush = observed
                        .operations
                        .iter()
                        .rposition(|op| *op == OutputOperation::Flush)
                        .expect("flush before finalize");
                    let finalize = observed
                        .operations
                        .iter()
                        .position(|op| *op == OutputOperation::Finalize)
                        .expect("finalize operation");
                    assert!(flush < finalize, "{:?}", observed.operations);
                }
            }
        }
    }

    #[test]
    fn scripted_cleanup_fault_preserves_the_partial_artifact() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("cleanup.bin");
        let temporary = TempFileSpec::default().temp_path_for(&destination);
        let registration = OutputFaultScript::register(&destination);
        registration.script().fail_next(
            OutputOperation::Cleanup,
            injected_error(OutputOperation::Cleanup),
        );
        let mut session =
            OutputSession::create(&destination, &TempFileSpec::default(), false, false, None)
                .expect("output session");
        session.write_at(0, b"partial").expect("write");

        let error = session.abort().expect_err("scripted cleanup failure");
        assert_eq!(error.0.category(), ErrorCategory::SinkWrite);
        assert!(
            temporary.exists(),
            "failed cleanup leaves the partial for recovery"
        );
        assert_eq!(
            registration.script().operations(),
            [
                OutputOperation::Open,
                OutputOperation::Write,
                OutputOperation::Cleanup
            ]
        );
    }
    /// Ordinary segmented chunks never flush: the flush operation
    /// fires exactly once — the owner's finalization — not once per chunk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn segmented_chunk_path_never_flushes() {
        let directory = tempfile::tempdir().expect("tempdir");
        let destination = directory.path().join("output.bin");
        // Observe-only script: records operations, injects nothing.
        let registration = OutputFaultScript::register(&destination);

        let mut config = EngineConfig::default();
        config.transfer.preallocate_output = false;
        config.transfer.segmentation_threshold = 1024;
        config.transfer.max_workers = 2;
        config.transfer.max_segment_size = 1000;
        config.transfer.min_segment_size = 1;
        config.transfer.verify_range_support = false;

        let controller = DownloadController::with_execution(
            HttpExecution::from_adapter(fault_http(
                true,
                &(0..FAULT_TOTAL)
                    .map(|i| ((i * 17 + 3) % 251) as u8)
                    .collect::<Vec<u8>>(),
            )),
            config,
        );
        let result = controller
            .run(DownloadRequest::new(
                "https://completion.test/no-chunk-flush",
                destination.clone(),
            ))
            .await
            .expect("terminal");
        assert!(result.final_path.exists(), "{result:?}");

        let flushes = registration
            .script()
            .operations()
            .iter()
            .filter(|op| **op == OutputOperation::Flush)
            .count();
        assert_eq!(
            flushes, 1,
            "ordinary segmented chunks must not flush; only the owner finalizes (flush ops: {flushes})"
        );
    }
}
