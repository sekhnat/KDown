//! Job controller: the single-stream pipeline (§9, §47, tasks 3.7-3.9).
//!
//! probe -> prepare -> sequential GET -> chunked positional writes with
//! backpressure -> verify size/hash -> atomic commit -> Completed.
//!
//! Pause/cancel are cooperative via [`CancellationToken`]; retry uses
//! [`RetryClassifier`] with structured classification (§17).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256, Sha512};

use crate::config::{EngineConfig, HashAlgorithm, IntegrityPolicy, OverwritePolicy, ResumePolicy};
use crate::control::retry::{RetryClassifier, RetryDecision};
use crate::control::CancellationToken;
use crate::error::DownloadError;
use crate::http::probe::ProbeMetadata;
use crate::http::transport::{HttpTransport, RequestSpec};
use crate::http::validators::ResourceValidators;
use crate::http::{
    BodyEvent, FullResponsePolicy, HttpExecution, HttpFailure, ProbeRequest, RangeIntent,
    TransferIntent, TransferRequest,
};
use crate::io::sink::{FileSink, FlushLevel, Sink, TempFileSpec};
use crate::job::state::{JobState, StateMachine};
use crate::metrics::counters::JobCounters;
use crate::metrics::events::{Event, EventHub, SharedHub};
use crate::metrics::export::EngineMetrics;
use crate::resume::checkpoint_store::{
    CheckpointStore as _, DurabilityMode as StoreDurability, FileCheckpointStore,
};
use crate::resume::durable_ranges::DurableRangeTracker;

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
        // Sensitive fields (authorization, credential provider) never
        // print their values (§35.3).
        f.debug_struct("DownloadRequest")
            .field("url", &self.url)
            .field("destination", &self.destination)
            .field("headers", &self.headers)
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

/// Terminal outcome (§7.4).
#[derive(Debug)]
pub struct DownloadResult {
    pub status: ResultStatus,
    pub final_path: Option<PathBuf>,
    pub bytes_downloaded_from_network: u64,
    pub bytes_reused_from_checkpoint: u64,
    pub total_size: Option<u64>,
    pub elapsed: Duration,
    pub validators: ResourceValidators,
    pub warnings: Vec<String>,
    pub error: Option<DownloadError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResultStatus {
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
    fn from_u8(v: u8) -> Self {
        match v {
            1 => CancelMode::KeepPartial,
            2 => CancelMode::KeepFileDiscardCheckpoint,
            _ => CancelMode::DeletePartial,
        }
    }
}

// hub/total_size are consumed by event-wiring refinements (task 3.8+).
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
    /// Resolved once the run task creates the live SegmentedJob (task 5.8):
    /// worker-concurrency reduction; `None` for single-stream jobs.
    segmented_cell: Arc<std::sync::OnceLock<Arc<crate::job::segmented::SegmentedJob>>>,
    /// Shared rate-limit bucket for the job (§18: runtime changeable).
    rate_bucket: Arc<std::sync::Mutex<Option<Arc<crate::control::rate_limit::TokenBucket>>>>,
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

    /// Request a concurrency reduction for a segmented job (task 5.8, §7.3):
    /// excess workers settle their leases safely and exit; no byte range
    /// is lost. No-op for single-stream jobs.
    pub fn set_concurrency(&self, workers: u64) {
        if let Some(job) = self.segmented_cell.get() {
            job.set_desired_workers(workers);
            self.hub
                .emit_try(crate::metrics::events::Event::RateLimitChanged {
                    bytes_per_second: None,
                });
        }
    }

    /// The live segmented job, when the run task created one (task 5.8).
    #[must_use]
    pub fn segmented_job(&self) -> Option<&Arc<crate::job::segmented::SegmentedJob>> {
        self.segmented_cell.get()
    }

    /// Change the job's rate limit at runtime (§18.2): takes effect on the
    /// next acquire without restarting workers. `0` = unlimited.
    pub fn set_rate_limit(&self, bytes_per_second: u64) {
        self.hub
            .emit_try(crate::metrics::events::Event::RateLimitChanged {
                bytes_per_second: if bytes_per_second == 0 {
                    None
                } else {
                    Some(bytes_per_second)
                },
            });
        // Replace the segmented job's bucket (or drop it for unlimited).
        if let Some(job) = self.segmented_cell.get() {
            let bucket = if bytes_per_second == 0 {
                None
            } else {
                Some(Arc::new(crate::control::rate_limit::TokenBucket::new(
                    bytes_per_second,
                )))
            };
            job.set_rate_bucket(bucket);
        }
        let mut bucket = self.rate_bucket.lock().expect("rate bucket lock");
        *bucket = if bytes_per_second == 0 {
            None
        } else {
            Some(Arc::new(crate::control::rate_limit::TokenBucket::new(
                bytes_per_second,
            )))
        };
    }

    /// The active rate limit (`None` = unlimited).
    #[must_use]
    pub fn rate_limit(&self) -> Option<u64> {
        let bucket = self.rate_bucket.lock().expect("rate bucket lock");
        bucket.as_ref().map(|b| b.rate()).filter(|r| *r != 0)
    }

    /// Subscribe to this job's event stream (§7.3, §19.4). Multiple
    /// subscribers are supported; slow subscribers may skip lagged events,
    /// while snapshot() remains authoritative for progress.
    pub fn events(&self) -> crate::metrics::events::EventStream {
        self.hub.subscribe()
    }
}

/// The single-stream job controller (§47 run_job, sequential branch).
pub struct SingleStreamController {
    /// The substitutable HTTP execution seam (§32): semantic probe and
    /// transfer operations without concrete client response types. The
    /// production adapter is injected by [`new`]/[`with_metrics`]; tests and
    /// alternate adapters inject via [`with_execution`].
    execution: HttpExecution,
    config: EngineConfig,
    classifier: RetryClassifier,
    next_id: std::sync::atomic::AtomicU64,
    metrics: Arc<EngineMetrics>,
}

impl SingleStreamController {
    /// Build a controller over the production Hyper wire adapter (§32:
    /// compatible construction — existing callers compile unchanged).
    #[must_use]
    pub fn new(transport: HttpTransport, config: EngineConfig) -> Self {
        Self::with_execution(HttpExecution::from_adapter(transport), config)
    }

    /// Inject an explicit HTTP execution handle (§32): scripted or alternate
    /// adapters substitute here without adapter-specific branches.
    #[must_use]
    pub fn with_execution(execution: HttpExecution, config: EngineConfig) -> Self {
        Self::with_execution_and_metrics(execution, config, EngineMetrics::shared())
    }

    /// Execution injection with a shared metrics registry (§19.5).
    #[must_use]
    pub fn with_execution_and_metrics(
        execution: HttpExecution,
        config: EngineConfig,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        let classifier = RetryClassifier::new(config.retry.clone());
        Self {
            execution,
            config,
            classifier,
            next_id: std::sync::atomic::AtomicU64::new(1),
            metrics,
        }
    }

    /// Build a controller sharing an engine-wide metrics registry (§19.5).
    #[must_use]
    pub fn with_metrics(
        transport: HttpTransport,
        config: EngineConfig,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        Self::with_execution_and_metrics(HttpExecution::from_adapter(transport), config, metrics)
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
    /// Terminal errors surface in [`DownloadResult::error`]; the function
    /// itself returns `Ok` for all three terminal statuses.
    pub async fn run(&self, request: DownloadRequest) -> Result<DownloadResult, DownloadError> {
        self.run_with_handle(request).await.map(|(r, _)| r)
    }

    /// Start a download and return immediately with a control handle
    /// (§7.2 start -> §7.3 handle). The job runs on a spawned task; the
    /// caller awaits the returned join handle for the terminal result.
    pub fn start(
        &self,
        request: DownloadRequest,
    ) -> (
        DownloadHandle,
        tokio::task::JoinHandle<Result<DownloadResult, DownloadError>>,
    ) {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.metrics.job_started();
        let metrics_for_run = self.metrics.clone();
        let state = StateMachine::new();
        let cancel = CancellationToken::new();
        let counters = Arc::new(JobCounters::new(1));
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
            rate_bucket: Arc::new(std::sync::Mutex::new(None)),
        };
        // The run task publishes the live SegmentedJob into the same cell
        // the handle reads (task 5.8).
        let handle_cell = handle.segmented_cell.clone();
        let rate_bucket_for_run = handle.rate_bucket.clone();
        let inner_state = state;
        let inner_cancel = cancel;
        let handle_cancel_mode = handle.cancel_mode_cell();
        let inner_counters = counters;
        let inner_hub = hub;
        let execution = self.execution.clone();
        let config = self.config.clone();
        let classifier = RetryClassifier::new(self.config.retry.clone());
        let join = tokio::spawn(async move {
            let this = Self {
                execution,
                config,
                classifier,
                next_id: std::sync::atomic::AtomicU64::new(0),
                metrics: metrics_for_run.clone(),
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
                )
                .await;
            match &terminal {
                Ok(result) => metrics_for_run.record_result(result),
                Err(error) => metrics_for_run.record_task_error(error),
            }
            terminal
        });
        (handle, join)
    }

    /// Run with a control handle: blocks until terminal.
    ///
    /// # Errors
    /// Same contract as [`SingleStreamController::run`].
    pub async fn run_with_handle(
        &self,
        request: DownloadRequest,
    ) -> Result<(DownloadResult, DownloadHandle), DownloadError> {
        let (handle, join) = self.start(request);
        let result = join
            .await
            .map_err(|e| DownloadError::Protocol(format!("job task panicked: {e}")))??;
        Ok((result, handle))
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
        rate_bucket_shared: Arc<
            std::sync::Mutex<Option<Arc<crate::control::rate_limit::TokenBucket>>>,
        >,
    ) -> Result<DownloadResult, DownloadError> {
        // Overwrite policy pre-check (§14.6): FailIfExists rejects before
        // any network activity.
        if request.overwrite == OverwritePolicy::FailIfExists && request.destination.exists() {
            let _ = state.transition(JobState::Failing);
            let _ = state.transition(JobState::Failed);
            return Ok(DownloadResult {
                status: ResultStatus::Failed,
                final_path: None,
                bytes_downloaded_from_network: 0,
                bytes_reused_from_checkpoint: 0,
                total_size: None,
                elapsed: Duration::ZERO,
                validators: ResourceValidators::default(),
                warnings: vec![],
                error: Some(DownloadError::Commit(format!(
                    "destination exists: {}",
                    request.destination.display()
                ))),
            });
        }

        // ---- Resume admission, phase 1 (§15.5, §7.2): policy-aware
        // checkpoint loading before any network activity. Required-state
        // failures reject before the job enters Probing.
        let identity = crate::resume::flow::job_identity(&request.url, &request.destination);
        let store = FileCheckpointStore::new(
            &request
                .destination
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(".")),
            match self.config.transfer.durability {
                crate::config::DurabilityMode::Performance => StoreDurability::Performance,
                crate::config::DurabilityMode::Durable => StoreDurability::Durable,
            },
        )
        .map_err(|e| DownloadError::Checkpoint(e.to_string()))?;
        let pending_admission = match crate::resume::flow::begin_admission(
            request.resume,
            &identity,
            TempFileSpec::default().temp_path_for(&request.destination),
            &store,
        ) {
            Ok(pending) => pending,
            Err(failure) => {
                Self::emit_resume_notices(&hub, &failure.notices).await;
                let _ = state.transition(JobState::Failing);
                let _ = state.transition(JobState::Failed);
                return Ok(self.failed_result(
                    request,
                    counters,
                    Duration::ZERO,
                    failure.error,
                    ResourceValidators::default(),
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
        loop {
            // Rebuilt per attempt: credential-provider headers (§29) merge
            // into `spec` before each re-probe.
            let probe_request = ProbeRequest {
                spec: spec.clone(),
                segmentation_threshold: self.config.transfer.segmentation_threshold,
                verify_range_support: self.config.transfer.verify_range_support,
            };
            if cancel.is_cancelled() {
                return self.terminal_cancelled(&state, request, counters, Duration::ZERO);
            }
            // Semantic probe (§32): the HTTP layer owns HEAD interpretation
            // and any configured validating range request; the controller
            // never sees raw statuses or headers.
            match self.execution.probe(probe_request.clone(), &cancel).await {
                Ok(outcome) => {
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
                    match self.classifier.decide(&e, attempt, retry_after) {
                        RetryDecision::Retry {
                            attempt: next,
                            delay,
                        } => {
                            counters.worker(0).expect("worker slot").add_retries(1);
                            attempt = next;
                            tokio::time::sleep(delay).await;
                        }
                        RetryDecision::GiveUp => {
                            let _ = state.transition(JobState::Failing);
                            let _ = state.transition(JobState::Failed);
                            return Ok(DownloadResult {
                                status: ResultStatus::Failed,
                                final_path: None,
                                bytes_downloaded_from_network: 0,
                                bytes_reused_from_checkpoint: 0,
                                total_size: None,
                                elapsed: Duration::ZERO,
                                validators: ResourceValidators::default(),
                                warnings: vec![],
                                error: Some(e),
                            });
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
                let _ = state.transition(JobState::Failing);
                let _ = state.transition(JobState::Failed);
                return Ok(self.failed_result(
                    request,
                    counters,
                    Duration::ZERO,
                    failure.error,
                    meta.validators.clone(),
                ));
            }
        };
        Self::emit_resume_notices(&hub, plan.notices()).await;
        let warnings: Vec<String> = plan.warnings().to_vec();
        let resume_validators: Option<ResourceValidators> = plan.validators();

        // Size expectation check (§4.1): caller-provided size must match.
        if let (Some(expected), Some(actual)) = (request.expected_size, meta.total_size) {
            if expected != actual {
                let _ = state.transition(JobState::Failing);
                let _ = state.transition(JobState::Failed);
                return Ok(self.failed_result(
                    request,
                    counters,
                    Duration::ZERO,
                    DownloadError::Protocol(format!(
                        "expected size {expected} but server reports {actual}"
                    )),
                    meta.validators.clone(),
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

        // ---- Prepare (§9.1 Preparing, §14) ----
        let _ = state.transition(JobState::Preparing);
        let resuming = plan.is_resuming();
        let mut sink = if resuming {
            // Reuse the existing temp file (§15.5: continue, never
            // truncate when ranges validate). Preserve across drops.
            let mut s = FileSink::open(&request.destination, &TempFileSpec::default(), false)
                .map_err(|e| e.0)?;
            s.set_keep_on_drop(true);
            s
        } else {
            FileSink::open(
                &request.destination,
                &TempFileSpec::default(),
                self.config.transfer.preallocate_output,
            )
            .map_err(|e| e.0)?
        };
        if !resuming {
            sink.prepare(meta.total_size).map_err(|e| e.0)?;
        }

        // ---- Segmented mode dispatch (§12, task 5.5) ----
        if eligible {
            let _ = state.transition(JobState::Running);
            hub.emit(Event::StateChanged {
                from: JobState::Preparing,
                to: JobState::Running,
            })
            .await;
            let started = std::time::Instant::now();
            let total = meta.total_size.expect("eligible requires known size");
            // The sink must survive worker drops (workers hold clones of
            // the Arc; the last Drop would delete the temp file otherwise).
            sink.set_keep_on_drop(true);
            let sink_shared = Arc::new(tokio::sync::Mutex::new(sink));
            // Pre-set rate limit (set before the probe completed) carries
            // into the segmented job (task 5.8).
            let initial_bucket = rate_bucket_shared.lock().expect("rate bucket lock").clone();
            // Segmented view: every admitted range is reusable (§12.1).
            let resumed = plan.segmented();
            counters
                .worker(0)
                .expect("w")
                .add_reused(resumed.reused_bytes);
            let outcome = crate::job::segmented::run_segmented(
                self.execution.clone(),
                &self.config,
                &request,
                &state,
                sink_shared,
                &store,
                &identity,
                &meta,
                total,
                counters.clone(),
                &hub,
                cancel.clone(),
                resumed.ranges.to_vec(),
                started,
                Some(segmented_cell.clone()),
                initial_bucket,
            )
            .await;
            return self
                .finish_segmented(
                    &request,
                    state,
                    counters,
                    hub,
                    &store,
                    &identity,
                    &meta,
                    outcome,
                    request.destination.clone(),
                    warnings,
                    started,
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
                let _ = store.save_atomic(&fresh);
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
                self.cleanup_cancelled(
                    CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                    &mut sink,
                    &store,
                    &identity,
                );
                return self.terminal_cancelled(&state, request, counters, elapsed);
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
                            let _ = sink.abort();
                            return self
                                .terminal_failed(
                                    &state,
                                    request,
                                    counters,
                                    started.elapsed(),
                                    DownloadError::AuthenticationRequired,
                                    validators,
                                    warnings,
                                )
                                .map(|mut r| {
                                    r.final_path = None;
                                    r
                                });
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
                                let _ = sink.abort();
                                return self
                                    .terminal_failed(
                                        &state,
                                        request,
                                        counters,
                                        started.elapsed(),
                                        DownloadError::AuthenticationRequired,
                                        validators,
                                        warnings,
                                    )
                                    .map(|mut r| {
                                        r.final_path = None;
                                        r
                                    });
                            }
                        }
                        let _ = sink.abort();
                        continue 'download; // re-issue with provider headers
                    }
                    // Retry classification (§17.1-§17.2) with server-provided
                    // retry timing; single-stream status/transport failures
                    // restart from zero (no committed prefix yet, §41).
                    let _ = sink.abort();
                    match self.classifier.decide(&e, attempt, retry_after) {
                        RetryDecision::Retry {
                            attempt: next,
                            delay,
                        } => {
                            counters.worker(0).expect("w").add_retries(1);
                            attempt = next;
                            offset = 0;
                            sink = FileSink::open(
                                &request.destination,
                                &TempFileSpec::default(),
                                self.config.transfer.preallocate_output,
                            )
                            .map_err(|se| se.0)?;
                            sink.prepare(meta.total_size).map_err(|se| se.0)?;
                            if let Some(status) = e.http_status() {
                                hub.emit(Event::Warning {
                                    detail: format!("status {status} retrying from zero ({e})"),
                                })
                                .await;
                            }
                            tokio::time::sleep(delay).await;
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
                            return self
                                .terminal_failed(
                                    &state,
                                    request,
                                    counters,
                                    started.elapsed(),
                                    err,
                                    validators,
                                    warnings,
                                )
                                .map(|mut r| {
                                    r.final_path = None;
                                    r
                                });
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
                    self.cleanup_cancelled(
                        CancelMode::from_u8(cancel_mode.load(std::sync::atomic::Ordering::SeqCst)),
                        &mut sink,
                        &store,
                        &identity,
                    );
                    return self.terminal_cancelled(&state, request, counters, elapsed);
                }
                match body.next_chunk(&cancel).await {
                    Ok(BodyEvent::Data(data)) => {
                        let len = data.len() as u64;
                        if let Some(max) = request.expected_size {
                            if offset + len > max {
                                // Overshoot is a protocol violation (§11.2).
                                let _ = sink.abort();
                                return self.terminal_failed(
                                    &state,
                                    request,
                                    counters,
                                    started.elapsed(),
                                    DownloadError::Protocol(format!(
                                        "body exceeds expected size {max}"
                                    )),
                                    validators,
                                    warnings,
                                );
                            }
                        }

                        // Backpressure: single in-flight chunk; write then
                        // read (§13.1).
                        sink.write_at(offset, &data).map_err(|se| {
                            let _ = sink.abort();
                            se.0
                        })?;
                        counters.worker(0).expect("w").add_network(len);
                        counters.worker(0).expect("w").add_completed(len);
                        offset += len;
                        durable.page_cache_ack(offset);
                        written_this_stream += len;

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
                            let _ = store.save_atomic(&cp);
                            sink.set_keep_on_drop(true);
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
                            let _ = store.save_atomic(&cp);
                            sink.set_keep_on_drop(true);
                        }
                        // Wait while paused, then continue or cancel.
                        while cancel.is_paused() && !cancel.is_cancelled() {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        if cancel.is_cancelled() {
                            self.cleanup_cancelled(
                                CancelMode::from_u8(
                                    cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                                ),
                                &mut sink,
                                &store,
                                &identity,
                            );
                            return self.terminal_cancelled(
                                &state,
                                request,
                                counters,
                                started.elapsed(),
                            );
                        }
                    }
                    Err(DownloadError::Cancelled) => {
                        // Cancellation interrupts a pending read (§32): same
                        // terminal path as an observed cancel.
                        let elapsed = started.elapsed();
                        self.cleanup_cancelled(
                            CancelMode::from_u8(
                                cancel_mode.load(std::sync::atomic::Ordering::SeqCst),
                            ),
                            &mut sink,
                            &store,
                            &identity,
                        );
                        return self.terminal_cancelled(&state, request, counters, elapsed);
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
                                    sink.set_keep_on_drop(true);
                                    sink = FileSink::open(
                                        &request.destination,
                                        &TempFileSpec::default(),
                                        false,
                                    )
                                    .map_err(|se| se.0)?;
                                } else {
                                    offset = 0;
                                    let _ = sink.abort();
                                    sink = FileSink::open(
                                        &request.destination,
                                        &TempFileSpec::default(),
                                        self.config.transfer.preallocate_output,
                                    )
                                    .map_err(|se| se.0)?;
                                    sink.prepare(meta.total_size).map_err(|se| se.0)?;
                                }
                                hub.emit(Event::Warning {
                                    detail: format!(
                                        "stream reset; retrying from offset {offset} ({err})"
                                    ),
                                })
                                .await;
                                tokio::time::sleep(delay).await;
                                continue 'download;
                            }
                            RetryDecision::GiveUp => {
                                let _ = sink.abort();
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
                                return self.terminal_failed(
                                    &state,
                                    request,
                                    counters,
                                    started.elapsed(),
                                    err,
                                    validators,
                                    warnings,
                                );
                            }
                        }
                    }
                }
            }

            // Stream finished cleanly: proceed to verification.
            break 'download;
        }

        // ---- Verifying (§16) ----
        let _ = state.transition(JobState::Verifying);
        hub.emit(Event::IntegrityCheckStarted).await;
        let total = if meta.total_size.is_some() {
            meta.total_size
        } else {
            None
        };
        // Exact size verification (§16.3).
        let sink_size = sink.size().map_err(|se| se.0)?;
        if let Some(expected) = meta.total_size {
            if sink_size != expected {
                return self
                    .terminal_failed(
                        &state,
                        request,
                        counters,
                        started.elapsed(),
                        DownloadError::IntegrityMismatch(format!(
                            "size mismatch: got {sink_size}, expected {expected}"
                        )),
                        validators,
                        warnings,
                    )
                    .map(|mut r| {
                        r.final_path = None;
                        r
                    });
            }
        }
        // Hash verification: sequential read of the completed file (§16.2).
        if !request.integrity.expected_hashes.is_empty() {
            match verify_hashes(&request, sink.temp_path()) {
                Ok(()) => {
                    hub.emit(Event::IntegrityCheckPassed).await;
                }
                Err(e) => {
                    hub.emit(Event::IntegrityCheckFailed {
                        detail: e.to_string(),
                    })
                    .await;
                    return self
                        .terminal_failed(
                            &state,
                            request,
                            counters,
                            started.elapsed(),
                            e,
                            validators,
                            warnings,
                        )
                        .map(|mut r| {
                            r.final_path = None;
                            r
                        });
                }
            }
        }

        // ---- Committing (§14.6) ----
        let _ = state.transition(JobState::Committing);
        sink.finalize().map_err(|se| se.0)?;
        match sink.commit() {
            Ok(final_path) => {
                let _ = state.transition(JobState::Completed);
                // Checkpoint removal (§14.6 step 5): committed downloads
                // leave no resumable state.
                let _ = store.delete(&identity);
                hub.emit(Event::Committed {
                    path: final_path.display().to_string(),
                })
                .await;
                let snap = counters.fold();
                Ok(DownloadResult {
                    status: ResultStatus::Completed,
                    final_path: Some(final_path),
                    bytes_downloaded_from_network: snap.network_bytes,
                    bytes_reused_from_checkpoint: snap.reused_bytes,
                    total_size: total,
                    elapsed: started.elapsed(),
                    validators,
                    warnings,
                    error: None,
                })
            }
            Err(e) => {
                let _ = state.transition(JobState::Failing);
                let _ = state.transition(JobState::Failed);
                Ok(DownloadResult {
                    status: ResultStatus::Failed,
                    final_path: None,
                    bytes_downloaded_from_network: counters.fold().network_bytes,
                    bytes_reused_from_checkpoint: 0,
                    total_size: total,
                    elapsed: started.elapsed(),
                    validators,
                    warnings,
                    error: Some(e.0),
                })
            }
        }
    }

    /// Cleanup for a cancelled job per [`CancelMode`] (§9.4):
    /// DeletePartial removes temp+checkpoint; KeepPartial keeps both;
    /// KeepFileDiscardCheckpoint removes only the checkpoint.
    #[allow(clippy::too_many_arguments)]
    fn cleanup_cancelled(
        &self,
        mode: CancelMode,
        sink: &mut FileSink,
        store: &FileCheckpointStore,
        identity: &str,
    ) {
        match mode {
            CancelMode::DeletePartial => {
                let _ = sink.abort();
                let _ = store.delete(identity);
            }
            CancelMode::KeepPartial => {
                let _ = sink.flush(FlushLevel::PageCache);
            }
            CancelMode::KeepFileDiscardCheckpoint => {
                let _ = sink.flush(FlushLevel::PageCache);
                let _ = store.delete(identity);
            }
        }
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
        counters: Arc<JobCounters>,
        hub: SharedHub,
        store: &FileCheckpointStore,
        identity: &str,
        _meta: &ProbeMetadata,
        outcome: crate::job::segmented::SegmentedOutcome,
        destination: PathBuf,
        mut warnings: Vec<String>,
        started: std::time::Instant,
    ) -> Result<DownloadResult, DownloadError> {
        let total = Some(outcome.total_size);
        if outcome.status == ResultStatus::Cancelled {
            let _ = state.transition(JobState::Cancelling);
            let _ = state.transition(JobState::Cancelled);
            let snap = counters.fold();
            return Ok(DownloadResult {
                status: ResultStatus::Cancelled,
                final_path: None,
                bytes_downloaded_from_network: snap.network_bytes,
                bytes_reused_from_checkpoint: snap.reused_bytes,
                total_size: None,
                elapsed: outcome.elapsed,
                validators: outcome.validators,
                warnings: warnings.clone(),
                error: outcome.error,
            });
        }
        if outcome.status == ResultStatus::Failed {
            let _ = state.transition(JobState::Failing);
            let _ = state.transition(JobState::Failed);
            return Ok(DownloadResult {
                status: ResultStatus::Failed,
                final_path: None,
                bytes_downloaded_from_network: outcome.network_bytes,
                bytes_reused_from_checkpoint: outcome.reused_bytes,
                total_size: outcome.total_size.into(),
                elapsed: outcome.elapsed,
                validators: outcome.validators,
                warnings,
                error: outcome.error,
            });
        }
        warnings.extend(outcome.warnings);

        // ---- Verifying (§16) ----
        let _ = state.transition(JobState::Verifying);
        hub.emit(Event::IntegrityCheckStarted).await;
        // Exact size verification (§16.3) over the assembled temp file.
        let temp_path = TempFileSpec::default().temp_path_for(&destination);
        let sink_size = std::fs::metadata(&temp_path)
            .map(|m| m.len())
            .map_err(|e| DownloadError::from_io(&e))?;
        if sink_size != outcome.total_size {
            hub.emit(Event::IntegrityCheckFailed {
                detail: format!(
                    "size mismatch: got {sink_size}, expected {}",
                    outcome.total_size
                ),
            })
            .await;
            let _ = state.transition(JobState::Failing);
            let _ = state.transition(JobState::Failed);
            return Ok(DownloadResult {
                status: ResultStatus::Failed,
                final_path: None,
                bytes_downloaded_from_network: outcome.network_bytes,
                bytes_reused_from_checkpoint: outcome.reused_bytes,
                total_size: Some(outcome.total_size),
                elapsed: outcome.elapsed,
                validators: outcome.validators,
                warnings,
                error: Some(DownloadError::IntegrityMismatch(format!(
                    "size mismatch: got {sink_size}, expected {}",
                    outcome.total_size
                ))),
            });
        }
        // Whole-file hash verification via sequential read of the
        // assembled temp file (§16.2, integrity spec).
        if !request.integrity.expected_hashes.is_empty() {
            match verify_hashes_path(&request.integrity, &temp_path) {
                Ok(()) => {
                    hub.emit(Event::IntegrityCheckPassed).await;
                }
                Err(e) => {
                    hub.emit(Event::IntegrityCheckFailed {
                        detail: e.to_string(),
                    })
                    .await;
                    let _ = state.transition(JobState::Failing);
                    let _ = state.transition(JobState::Failed);
                    return Ok(DownloadResult {
                        status: ResultStatus::Failed,
                        final_path: None,
                        bytes_downloaded_from_network: outcome.network_bytes,
                        bytes_reused_from_checkpoint: outcome.reused_bytes,
                        total_size: Some(outcome.total_size),
                        elapsed: outcome.elapsed,
                        validators: outcome.validators,
                        warnings,
                        error: Some(e),
                    });
                }
            }
        }

        // ---- Committing (§14.6) ----
        let _ = state.transition(JobState::Committing);
        let _ = identity;
        let commit = {
            // Reopen a sink over the temp file strictly for the commit:
            // the worker-shared sink stays locked inside run_segmented's
            // scope; FileSink::commit performs the atomic rename.
            let mut commit_sink =
                FileSink::open(&request.destination, &TempFileSpec::default(), false)
                    .map_err(|e| e.0)?;
            commit_sink.set_keep_on_drop(true);
            commit_sink.finalize().map_err(|e| e.0)?;
            commit_sink.commit()
        };
        match commit {
            Ok(final_path) => {
                let _ = state.transition(JobState::Completed);
                // Checkpoint removal (§14.6 step 5).
                let _ = store.delete(identity);
                hub.emit(Event::Committed {
                    path: final_path.display().to_string(),
                })
                .await;
                Ok(DownloadResult {
                    status: ResultStatus::Completed,
                    final_path: Some(final_path),
                    bytes_downloaded_from_network: outcome.network_bytes,
                    bytes_reused_from_checkpoint: outcome.reused_bytes,
                    total_size: total,
                    elapsed: outcome.elapsed,
                    validators: outcome.validators,
                    warnings,
                    error: None,
                })
            }
            Err(e) => {
                let _ = state.transition(JobState::Failing);
                let _ = state.transition(JobState::Failed);
                Ok(DownloadResult {
                    status: ResultStatus::Failed,
                    final_path: None,
                    bytes_downloaded_from_network: outcome.network_bytes,
                    bytes_reused_from_checkpoint: outcome.reused_bytes,
                    total_size: total,
                    elapsed: started.elapsed(),
                    validators: outcome.validators,
                    warnings,
                    error: Some(e.0),
                })
            }
        }
    }

    fn failed_result(
        &self,
        _request: DownloadRequest,
        counters: Arc<JobCounters>,
        elapsed: Duration,
        error: DownloadError,
        validators: ResourceValidators,
    ) -> DownloadResult {
        let snap = counters.fold();
        DownloadResult {
            status: ResultStatus::Failed,
            final_path: None,
            bytes_downloaded_from_network: snap.network_bytes,
            bytes_reused_from_checkpoint: snap.reused_bytes,
            total_size: None,
            elapsed,
            validators,
            warnings: vec![],
            error: Some(error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn terminal_failed(
        &self,
        state: &Arc<StateMachine>,
        request: DownloadRequest,
        counters: Arc<JobCounters>,
        elapsed: Duration,
        error: DownloadError,
        validators: ResourceValidators,
        warnings: Vec<String>,
    ) -> Result<DownloadResult, DownloadError> {
        let _ = state.transition(JobState::Failing);
        let _ = state.transition(JobState::Failed);
        crate::observability::log_terminal_error(
            &crate::observability::Correlation::new().origin(request.url.clone()),
            &error,
        );
        let mut r = self.failed_result(request, counters, elapsed, error, validators);
        r.warnings = warnings;
        Ok(r)
    }

    fn terminal_cancelled(
        &self,
        state: &Arc<StateMachine>,
        _request: DownloadRequest,
        counters: Arc<JobCounters>,
        elapsed: Duration,
    ) -> Result<DownloadResult, DownloadError> {
        let _ = state.transition(JobState::Cancelling);
        let _ = state.transition(JobState::Cancelled);
        let snap = counters.fold();
        Ok(DownloadResult {
            status: ResultStatus::Cancelled,
            final_path: None,
            bytes_downloaded_from_network: snap.network_bytes,
            bytes_reused_from_checkpoint: snap.reused_bytes,
            total_size: None,
            elapsed,
            validators: ResourceValidators::default(),
            warnings: vec![],
            error: Some(DownloadError::Cancelled),
        })
    }
}

/// Sequential hash verification of a file against the expected digests
/// (§16.2): used by the segmented completion path over the assembled
/// temp file.
fn verify_hashes_path(integrity: &IntegrityPolicy, path: &Path) -> Result<(), DownloadError> {
    for expected in &integrity.expected_hashes {
        let file = std::fs::File::open(path).map_err(|e| DownloadError::from_io(&e))?;
        let mut reader = std::io::BufReader::with_capacity(256 * 1024, file);
        let computed = match expected.algorithm {
            HashAlgorithm::Sha256 => {
                let mut h = Sha256::new();
                std::io::copy(&mut reader, &mut h)
                    .map_err(|e| DownloadError::SinkWrite(e.to_string()))?;
                hex(&h.finalize())
            }
            HashAlgorithm::Sha512 => {
                let mut h = Sha512::new();
                std::io::copy(&mut reader, &mut h)
                    .map_err(|e| DownloadError::SinkWrite(e.to_string()))?;
                hex(&h.finalize())
            }
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

/// Sequential hash verification of the completed temp file (§16.2).
fn verify_hashes(request: &DownloadRequest, temp: &Path) -> Result<(), DownloadError> {
    for expected in &request.integrity.expected_hashes {
        let file = std::fs::File::open(temp).map_err(|e| DownloadError::from_io(&e))?;
        let mut reader = std::io::BufReader::with_capacity(256 * 1024, file);
        let computed = match expected.algorithm {
            crate::config::HashAlgorithm::Sha256 => {
                let mut h = Sha256::new();
                std::io::copy(&mut reader, &mut h)
                    .map_err(|e| DownloadError::SinkWrite(e.to_string()))?;
                hex(&h.finalize())
            }
            crate::config::HashAlgorithm::Sha512 => {
                let mut h = Sha512::new();
                std::io::copy(&mut reader, &mut h)
                    .map_err(|e| DownloadError::SinkWrite(e.to_string()))?;
                hex(&h.finalize())
            }
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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
