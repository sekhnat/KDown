//! The single-owner supervisor actor.
//!
//! The actor owns the queue, the live engine handles, the completion
//! futures, and the 250 ms sample cadence. All lifecycle commands flow
//! through one bounded mpsc channel; the actor persists intent, attempts,
//! and status before resolving destinations and launching the engine, so a
//! crash between persistence and launch is always recoverable (Task 5).
//! No database transaction is ever held across filesystem or engine work.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::domain::{
    AttemptId, AttemptOutcome, AttemptReason, CancelArtifactPolicy, ControlVersion, DesiredState,
    DurableJobStatus, JobId, JobIntent, JobRecord, JobView, RootId,
};
use crate::engine_adapter::{
    EngineControl, EngineLaunch, EngineLauncher, EngineOutcome, EngineSnapshotView,
};
use crate::error::AppError;
use crate::events::EventBroker;
use crate::path_policy::{validate_filename, ResolvedDestination};
use crate::registry::Registry;

pub mod command;
pub mod recovery;

pub use crate::events::SupervisorEvent;
pub use command::SupervisorCommand;
pub use recovery::{classify, RecoveryAction, RecoveryPlanner};

/// Completion futures tracked by the actor, tagged with their job/attempt.
type CompletionStream = FuturesUnordered<
    std::pin::Pin<Box<dyn Future<Output = (JobId, AttemptId, EngineOutcome)> + Send>>,
>;

/// Resolves launch destinations. Implemented by [`PathPolicy`]; tests may
/// wrap it to observe or gate resolution.
pub trait LaunchResolver: Send + Sync + 'static {
    fn resolve_for_launch(
        &self,
        root_id: RootId,
        relative: &str,
    ) -> impl Future<Output = Result<ResolvedDestination, AppError>> + Send;
}

impl LaunchResolver for crate::path_policy::PathPolicy {
    fn resolve_for_launch(
        &self,
        root_id: RootId,
        relative: &str,
    ) -> impl Future<Output = Result<ResolvedDestination, AppError>> + Send {
        crate::path_policy::PathPolicy::resolve_for_launch(self, root_id, relative)
    }
}

/// Initial supervisor limits; settings can change them at runtime.
#[derive(Clone, Copy, Debug)]
pub struct SupervisorLimits {
    pub max_active: usize,
    pub rate_limit_bytes_per_second: Option<u64>,
}

/// Cloneable handle to the supervisor actor.
#[derive(Clone, Debug)]
pub struct SupervisorHandle {
    tx: mpsc::Sender<SupervisorCommand>,
    broker: EventBroker,
}

impl SupervisorHandle {
    /// Subscribes to revisioned job snapshot events.
    pub fn subscribe(&self) -> broadcast::Receiver<SupervisorEvent> {
        self.broker.subscribe()
    }

    async fn request<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, AppError>>) -> SupervisorCommand,
    ) -> Result<T, AppError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(make(tx))
            .await
            .map_err(|_| AppError::ServiceDegraded)?;
        rx.await.map_err(|_| AppError::ServiceDegraded)?
    }

    /// Admits a persisted job for queueing/launch.
    pub async fn enqueue(&self, job_id: JobId) -> Result<JobView, AppError> {
        self.request(|reply| SupervisorCommand::Enqueue {
            job_id,
            attempt: None,
            reply,
        })
        .await
    }

    /// Admits a job whose recovery attempt already exists; the supervisor
    /// launches against that attempt instead of creating a new one.
    pub(crate) async fn enqueue_recovered(
        &self,
        job_id: JobId,
        attempt_id: AttemptId,
    ) -> Result<JobView, AppError> {
        self.request(|reply| SupervisorCommand::Enqueue {
            job_id,
            attempt: Some(attempt_id),
            reply,
        })
        .await
    }

    /// Requests pause; stale duplicates conflict.
    pub async fn pause(
        &self,
        job_id: JobId,
        expected: ControlVersion,
    ) -> Result<JobView, AppError> {
        self.request(|reply| SupervisorCommand::Pause {
            job_id,
            expected,
            reply,
        })
        .await
    }

    /// Requests resume; stale duplicates conflict.
    pub async fn resume(
        &self,
        job_id: JobId,
        expected: ControlVersion,
    ) -> Result<JobView, AppError> {
        self.request(|reply| SupervisorCommand::Resume {
            job_id,
            expected,
            reply,
        })
        .await
    }

    /// Requests cancellation with an explicit artifact policy.
    pub async fn cancel(
        &self,
        job_id: JobId,
        expected: ControlVersion,
        policy: CancelArtifactPolicy,
    ) -> Result<JobView, AppError> {
        self.request(|reply| SupervisorCommand::Cancel {
            job_id,
            expected,
            policy,
            reply,
        })
        .await
    }

    /// Requests a new attempt on a terminal job.
    pub async fn retry(
        &self,
        job_id: JobId,
        expected: ControlVersion,
    ) -> Result<JobView, AppError> {
        self.request(|reply| SupervisorCommand::Retry {
            job_id,
            expected,
            reply,
        })
        .await
    }

    /// Applies new global limits.
    pub async fn update_limits(
        &self,
        active_downloads: usize,
        bytes_per_second: Option<u64>,
    ) -> Result<(), AppError> {
        self.request(|reply| SupervisorCommand::UpdateLimits {
            active_downloads,
            bytes_per_second,
            reply,
        })
        .await
    }

    /// Graceful shutdown preserving resumable artifacts (Task 5 refines).
    pub async fn shutdown(&self) -> Result<(), AppError> {
        self.request(|reply| SupervisorCommand::Shutdown { reply })
            .await
    }
}

struct ActiveRun<H: EngineControl> {
    attempt_id: AttemptId,
    handle: H,
    seq: u64,
    job: JobRecord,
}

/// Spawns the supervisor actor and returns its handle.
pub fn spawn_supervisor<L, R>(
    registry: Registry,
    resolver: R,
    launcher: L,
    limits: SupervisorLimits,
) -> SupervisorHandle
where
    L: EngineLauncher,
    R: LaunchResolver,
{
    spawn_supervisor_with_broker(EventBroker::new(256), registry, resolver, launcher, limits)
}

/// Spawns the supervisor actor publishing through the given broker, so the
/// API layer subscribes to the same event source the supervisor writes.
pub fn spawn_supervisor_with_broker<L, R>(
    broker: EventBroker,
    registry: Registry,
    resolver: R,
    launcher: L,
    limits: SupervisorLimits,
) -> SupervisorHandle
where
    L: EngineLauncher,
    R: LaunchResolver,
{
    let (tx, rx) = mpsc::channel(64);
    let handle = SupervisorHandle {
        tx,
        broker: broker.clone(),
    };
    tokio::spawn(async move {
        let actor = Actor {
            registry,
            resolver,
            launcher,
            broker,
            active: HashMap::new(),
            queue: VecDeque::new(),
            max_active: limits.max_active,
            rate_limit: limits.rate_limit_bytes_per_second,
            shutting_down: false,
            completions: FuturesUnordered::new(),
        };
        actor.run(rx).await;
    });
    handle
}

struct Actor<L: EngineLauncher, R: LaunchResolver> {
    registry: Registry,
    resolver: R,
    launcher: L,
    broker: EventBroker,
    active: HashMap<JobId, ActiveRun<L::Handle>>,
    queue: VecDeque<JobId>,
    max_active: usize,
    rate_limit: Option<u64>,
    shutting_down: bool,
    completions: CompletionStream,
}

impl<L: EngineLauncher, R: LaunchResolver> Actor<L, R> {
    async fn run(mut self, mut rx: mpsc::Receiver<SupervisorCommand>) {
        let mut sample_tick = tokio::time::interval(Duration::from_millis(250));
        sample_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                command = rx.recv() => {
                    match command {
                        Some(command) => self.handle_command(command).await,
                        None => break,
                    }
                }
                completed = self.completions.next(), if !self.completions.is_empty() => {
                    if let Some((job_id, attempt_id, outcome)) = completed {
                        self.finalize(job_id, attempt_id, outcome).await;
                    }
                }
                _ = sample_tick.tick() => {
                    self.sample().await;
                }
            }
        }
    }

    async fn handle_command(&mut self, command: SupervisorCommand) {
        match command {
            SupervisorCommand::Enqueue {
                job_id,
                attempt,
                reply,
            } => {
                let _ = reply.send(self.enqueue(job_id, attempt).await);
            }
            SupervisorCommand::Pause {
                job_id,
                expected,
                reply,
            } => {
                let _ = reply.send(self.pause(job_id, expected).await);
            }
            SupervisorCommand::Resume {
                job_id,
                expected,
                reply,
            } => {
                let _ = reply.send(self.resume(job_id, expected).await);
            }
            SupervisorCommand::Cancel {
                job_id,
                expected,
                policy,
                reply,
            } => {
                let _ = reply.send(self.cancel(job_id, expected, policy).await);
            }
            SupervisorCommand::Retry {
                job_id,
                expected,
                reply,
            } => {
                let _ = reply.send(self.retry(job_id, expected).await);
            }
            SupervisorCommand::UpdateLimits {
                active_downloads,
                bytes_per_second,
                reply,
            } => {
                self.max_active = active_downloads;
                self.rate_limit = bytes_per_second;
                let result = self.launcher.set_global_rate_limit(bytes_per_second);
                let _ = reply.send(result);
            }
            SupervisorCommand::AttemptFinished {
                job_id,
                attempt_id,
                outcome,
            } => {
                self.finalize(job_id, attempt_id, outcome).await;
            }
            SupervisorCommand::Shutdown { reply } => {
                self.shutting_down = true;
                // Stop admission and preserve resumable artifacts. The
                // resulting completions record `Interrupted`, never a user
                // cancellation, and desired state stays untouched.
                for run in self.active.values() {
                    let _ = run
                        .handle
                        .cancel_with(CancelArtifactPolicy::PreservePartial);
                }
                let _ = reply.send(Ok(()));
            }
        }
    }

    /// Admits a persisted job: launch when a slot is open, otherwise queue.
    /// A prebound attempt (recovery) is launched against directly; a queued
    /// prebound job keeps its attempt for the later promotion.
    async fn enqueue(
        &mut self,
        job_id: JobId,
        prebound: Option<AttemptId>,
    ) -> Result<JobView, AppError> {
        let job = self.registry.load_job(job_id).await?;
        if job.status.is_terminal() {
            return Err(AppError::InvalidTransition);
        }
        if self.shutting_down || self.active.len() >= self.max_active {
            self.queue.push_back(job_id);
            return Ok(job_view(&job, job.current_attempt_id, 0, None));
        }
        self.launch_job(&job, AttemptReason::Initial, prebound)
            .await?;
        let job = self.registry.load_job(job_id).await?;
        let attempt_id = job.current_attempt_id;
        Ok(job_view(&job, attempt_id, 0, None))
    }

    /// Persists the attempt and status, resolves the destination, and
    /// launches the engine — in that order, with no open transaction.
    async fn launch_job(
        &mut self,
        job: &JobRecord,
        reason: AttemptReason,
        prebound: Option<AttemptId>,
    ) -> Result<(), AppError> {
        let attempt = match prebound {
            Some(attempt_id) => self.registry.load_attempt(attempt_id).await?,
            None => {
                let launch_key = crate::domain::LaunchKey::new();
                self.registry
                    .begin_attempt_once(job.id, reason, launch_key)
                    .await?
            }
        };
        self.registry
            .mark_status(job.id, DurableJobStatus::Active)
            .await?;

        let failed_launch = |code: String, detail: Option<String>| AttemptOutcome::Failed {
            code,
            detail,
            metrics: None,
        };

        // Validate a filename override defensively even though the API also
        // validates it: the destination must never escape the root.
        if let Some(name) = job.intent.filename_override.as_deref() {
            if let Err(error) = validate_filename(name) {
                self.registry
                    .finish_attempt(attempt.id, failed_launch(error.code().to_string(), None))
                    .await
                    .ok();
                return Err(error);
            }
        }

        let relative = compose_relative(&job.intent);
        let resolved = match self
            .resolver
            .resolve_for_launch(job.intent.root_id, &relative)
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => {
                self.registry
                    .finish_attempt(attempt.id, failed_launch(error.code().to_string(), None))
                    .await
                    .ok();
                self.broadcast_record(job.id).await;
                return Err(error);
            }
        };

        let destination = match job.intent.filename_override.as_deref() {
            Some(_override) => resolved.destination.clone(),
            None => match ensure_destination_directory(&resolved).await {
                Ok(destination) => destination,
                Err(error) => {
                    self.registry
                        .finish_attempt(attempt.id, failed_launch(error.code().to_string(), None))
                        .await
                        .ok();
                    self.broadcast_record(job.id).await;
                    return Err(error);
                }
            },
        };

        let engine_launch = EngineLaunch {
            job_id: job.id,
            attempt_id: attempt.id,
            source: job.intent.source.clone(),
            destination,
            filename_override: job.intent.filename_override.clone(),
            conflict_policy: job.intent.conflict_policy,
        };
        let run = match self.launcher.launch(engine_launch).await {
            Ok(run) => run,
            Err(error) => {
                self.registry
                    .finish_attempt(attempt.id, failed_launch(error.code().to_string(), None))
                    .await
                    .ok();
                self.broadcast_record(job.id).await;
                return Err(error);
            }
        };

        let job = self.registry.load_job(job.id).await?;
        let job_id = job.id;
        let attempt_id = attempt.id;
        self.active.insert(
            job_id,
            ActiveRun {
                attempt_id,
                handle: run.handle,
                seq: 0,
                job,
            },
        );
        let completion = run.completion;
        self.completions.push(Box::pin(async move {
            let outcome = completion.await;
            (job_id, attempt_id, outcome)
        }));
        self.broadcast_record(job_id).await;
        Ok(())
    }

    async fn pause(
        &mut self,
        job_id: JobId,
        expected: ControlVersion,
    ) -> Result<JobView, AppError> {
        self.registry
            .compare_and_set_desired(job_id, expected, DesiredState::Paused)
            .await?;
        if let Some(run) = self.active.get_mut(&job_id) {
            run.handle.pause()?;
            self.registry
                .mark_status(job_id, DurableJobStatus::Paused)
                .await?;
            // The sampler broadcasts from this record; a stale launch-time
            // capture would flip the UI back to a pre-pause state.
            if let Ok(fresh) = self.registry.load_job(job_id).await {
                run.job = fresh;
            }
        }
        self.broadcast_record(job_id).await;
        let job = self.registry.load_job(job_id).await?;
        Ok(job_view(
            &job,
            job.current_attempt_id,
            self.seq_of(job_id),
            None,
        ))
    }

    async fn resume(
        &mut self,
        job_id: JobId,
        expected: ControlVersion,
    ) -> Result<JobView, AppError> {
        self.registry
            .compare_and_set_desired(job_id, expected, DesiredState::Running)
            .await?;
        if let Some(run) = self.active.get_mut(&job_id) {
            run.handle.resume_now()?;
            self.registry
                .mark_status(job_id, DurableJobStatus::Active)
                .await?;
            if let Ok(fresh) = self.registry.load_job(job_id).await {
                run.job = fresh;
            }
        }
        self.broadcast_record(job_id).await;
        let job = self.registry.load_job(job_id).await?;
        Ok(job_view(
            &job,
            job.current_attempt_id,
            self.seq_of(job_id),
            None,
        ))
    }

    async fn cancel(
        &mut self,
        job_id: JobId,
        expected: ControlVersion,
        policy: CancelArtifactPolicy,
    ) -> Result<JobView, AppError> {
        let job = self.registry.load_job(job_id).await?;
        if job.status.is_terminal() {
            return Err(AppError::InvalidTransition);
        }
        let record = self
            .registry
            .compare_and_set_desired(job_id, expected, DesiredState::Cancelled)
            .await?;
        if let Some(run) = self.active.get(&job_id) {
            run.handle.cancel_with(policy)?;
        } else {
            // Queued job: no engine run exists; cancel durably now.
            self.queue.retain(|id| *id != job_id);
            self.registry
                .mark_status(job_id, DurableJobStatus::Cancelled)
                .await?;
        }
        self.broadcast_record(job_id).await;
        let job = self.registry.load_job(job_id).await?;
        Ok(job_view(
            &record,
            job.current_attempt_id,
            self.seq_of(job_id),
            None,
        ))
    }

    async fn retry(
        &mut self,
        job_id: JobId,
        expected: ControlVersion,
    ) -> Result<JobView, AppError> {
        let job = self.registry.load_job(job_id).await?;
        if !job.status.is_terminal() {
            return Err(AppError::InvalidTransition);
        }
        self.registry
            .compare_and_set_desired(job_id, expected, DesiredState::Running)
            .await?;
        if self.shutting_down || self.active.len() >= self.max_active {
            self.registry
                .mark_status(job_id, DurableJobStatus::Queued)
                .await?;
            self.queue.push_back(job_id);
        } else {
            let job = self.registry.load_job(job_id).await?;
            self.launch_job(&job, AttemptReason::Retry, None).await?;
        }
        self.broadcast_record(job_id).await;
        let job = self.registry.load_job(job_id).await?;
        Ok(job_view(&job, job.current_attempt_id, 0, None))
    }

    /// Records a terminal attempt outcome and promotes queued jobs.
    async fn finalize(&mut self, job_id: JobId, attempt_id: AttemptId, outcome: EngineOutcome) {
        // Capture the resolved destination while the handle still exists.
        let final_path = self
            .active
            .get(&job_id)
            .and_then(|run| run.handle.resolved_destination());
        let attempt_outcome = match outcome {
            EngineOutcome::Completed => {
                // Metrics come from the final handle snapshot, which the
                // engine keeps authoritative to the end.
                AttemptOutcome::Completed(metrics_from_active(self, job_id))
            }
            EngineOutcome::Failed { code, detail } => AttemptOutcome::Failed {
                code,
                detail,
                metrics: None,
            },
            // A cancellation that shutdown itself requested is an
            // interruption of host-owned work, not user intent.
            EngineOutcome::Cancelled if self.shutting_down => AttemptOutcome::Interrupted,
            EngineOutcome::Cancelled => AttemptOutcome::Cancelled,
        };
        if self
            .registry
            .finish_attempt_with_path(attempt_id, attempt_outcome, final_path)
            .await
            .is_err()
        {
            // Already finished (duplicate completion): keep current state.
        }
        if let Some(run) = self.active.get_mut(&job_id) {
            run.seq = run.seq.saturating_add(1);
        }
        self.broadcast_record(job_id).await;
        self.active.remove(&job_id);
        self.promote_queued().await;
    }

    /// Launches queued jobs while slots remain open. Paused and cancelled
    /// desired states are honored without launching.
    async fn promote_queued(&mut self) {
        let mut queue = std::mem::take(&mut self.queue);
        let mut still_queued = VecDeque::new();
        while let Some(job_id) = queue.pop_front() {
            if self.shutting_down || self.active.len() >= self.max_active {
                still_queued.push_back(job_id);
                continue;
            }
            let Ok(job) = self.registry.load_job(job_id).await else {
                continue;
            };
            if job.status.is_terminal() {
                continue;
            }
            match job.desired_state {
                DesiredState::Paused => still_queued.push_back(job_id),
                DesiredState::Cancelled => {
                    self.registry
                        .mark_status(job_id, DurableJobStatus::Cancelled)
                        .await
                        .ok();
                    self.broadcast_record(job_id).await;
                }
                DesiredState::Running => {
                    // A queued job can already carry an unfinished attempt
                    // (recovery); reuse it instead of creating a second one.
                    if let Err(error) = self
                        .launch_job(&job, AttemptReason::Initial, job.current_attempt_id)
                        .await
                    {
                        // Launch failure already recorded the durable failure.
                        let _ = error;
                    }
                }
            }
        }
        self.queue = still_queued;
    }

    /// Samples every active run at the bounded cadence.
    async fn sample(&mut self) {
        let ids: Vec<JobId> = self.active.keys().copied().collect();
        for job_id in ids {
            let Some(run) = self.active.get_mut(&job_id) else {
                continue;
            };
            run.seq += 1;
            let snapshot = run.handle.snapshot();
            let view = job_view(&run.job, Some(run.attempt_id), run.seq, Some(snapshot));
            self.broker.publish(SupervisorEvent::JobSnapshot(view));
        }
    }

    fn seq_of(&self, job_id: JobId) -> u64 {
        self.active.get(&job_id).map_or(0, |run| run.seq)
    }

    /// Broadcasts the current durable record as a snapshot milestone.
    async fn broadcast_record(&mut self, job_id: JobId) {
        let Ok(job) = self.registry.load_job(job_id).await else {
            return;
        };
        let seq = self.seq_of(job_id);
        let snapshot = self.active.get(&job_id).map(|run| run.handle.snapshot());
        let view = job_view(&job, job.current_attempt_id, seq, snapshot);
        self.broker.publish(SupervisorEvent::JobSnapshot(view));
    }
}

fn metrics_from_active<L: EngineLauncher, R: LaunchResolver>(
    actor: &Actor<L, R>,
    job_id: JobId,
) -> crate::domain::AttemptMetrics {
    actor
        .active
        .get(&job_id)
        .map(|run| {
            let snapshot = run.handle.snapshot();
            crate::engine_adapter::metrics_from_accounting(
                snapshot.bytes_received,
                snapshot.network_bytes,
                Duration::from_millis(snapshot.elapsed_ms),
            )
        })
        .unwrap_or_default()
}

/// Joins the intent's relative directory and filename override into the
/// relative destination the path policy resolves.
fn compose_relative(intent: &JobIntent) -> String {
    match (
        intent.relative_directory.as_deref(),
        intent.filename_override.as_deref(),
    ) {
        (Some(directory), Some(name)) => format!("{directory}/{name}"),
        (Some(directory), None) => directory.to_string(),
        (None, Some(name)) => name.to_string(),
        (None, None) => String::new(),
    }
}

/// Creates the directory-target destination and re-verifies containment.
/// The final component is created here — after resolution validated the
/// whole parent chain — immediately before the engine starts.
async fn ensure_destination_directory(
    resolved: &ResolvedDestination,
) -> Result<std::path::PathBuf, AppError> {
    let destination = &resolved.destination;
    if !destination.exists() {
        tokio::fs::create_dir_all(destination)
            .await
            .map_err(|_| AppError::DestinationUnavailable)?;
    }
    let canonical = tokio::fs::canonicalize(destination)
        .await
        .map_err(|_| AppError::DestinationUnavailable)?;
    if !canonical.starts_with(&resolved.canonical_root) {
        return Err(AppError::DestinationOutsideRoot);
    }
    Ok(canonical)
}

fn job_view(
    record: &JobRecord,
    attempt_id: Option<AttemptId>,
    sample_seq: u64,
    snapshot: Option<EngineSnapshotView>,
) -> JobView {
    JobView {
        id: record.id,
        status: record.status,
        desired_state: record.desired_state,
        control_version: record.control_version,
        attempt_id,
        sample_seq,
        source_display: record.intent.source.redacted(),
        root_id: record.intent.root_id,
        relative_directory: record.intent.relative_directory.clone(),
        filename_override: record.intent.filename_override.clone(),
        created_at: record.created_at,
        updated_at: record.updated_at,
        snapshot,
    }
}
