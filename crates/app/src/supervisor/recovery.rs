//! Startup and graceful-shutdown recovery policy.
//!
//! On startup the planner classifies every nonterminal job by its desired
//! state and durable status, then resumes exactly once per job: the
//! recovery attempt carries this process's `startup_key`, so concurrent
//! or repeated recovery passes cannot create a second attempt. Destination
//! revalidation happens at launch time inside the supervisor's launch
//! pipeline; a failed revalidation finishes the recovery attempt as
//! `Failed` and preserves the artifact.

use crate::domain::{DesiredState, DurableJobStatus, JobId, JobRecord, LaunchKey};
use crate::error::AppError;
use crate::registry::Registry;
use crate::supervisor::SupervisorHandle;

/// What startup should do with one recovered job.
#[derive(Clone, Debug)]
pub enum RecoveryAction {
    /// Desired running: create one recovery attempt and launch.
    Resume(JobId),
    /// User-paused: leave paused.
    KeepPaused(JobId),
    /// Terminal or user-cancelled: leave alone.
    KeepTerminal(JobId),
    /// Resume preparation failed: the attempt is finished as failed and
    /// the artifact is preserved.
    Fail(JobId, AppError),
}

/// Classifies one durable job for startup recovery.
pub fn classify(job: &JobRecord) -> RecoveryAction {
    match (job.desired_state, job.status) {
        (DesiredState::Running, DurableJobStatus::Completed | DurableJobStatus::Cancelled) => {
            RecoveryAction::KeepTerminal(job.id)
        }
        (DesiredState::Running, _) => RecoveryAction::Resume(job.id),
        (DesiredState::Paused, _) => RecoveryAction::KeepPaused(job.id),
        (DesiredState::Cancelled, _) => RecoveryAction::KeepTerminal(job.id),
    }
}

/// Plans and executes startup recovery.
#[derive(Clone, Debug)]
pub struct RecoveryPlanner {
    registry: Registry,
    supervisor: SupervisorHandle,
    startup_key: LaunchKey,
    /// Serializes concurrent recovery passes in-process without blocking a
    /// thread on SQLite write locks.
    gate: std::sync::Arc<tokio::sync::Mutex<()>>,
}

impl RecoveryPlanner {
    pub fn new(registry: Registry, supervisor: SupervisorHandle, startup_key: LaunchKey) -> Self {
        Self {
            registry,
            supervisor,
            startup_key,
            gate: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Recovers every nonterminal job. Safe to run concurrently and
    /// repeatedly within one process: only the caller that created the
    /// startup's recovery attempt enqueues the job.
    pub async fn recover_startup(&self) -> Result<Vec<RecoveryAction>, AppError> {
        let _gate = self.gate.lock().await;
        let jobs = self.registry.list_recoverable().await?;
        let mut actions = Vec::with_capacity(jobs.len());
        for job in &jobs {
            let action = classify(job);
            match action {
                RecoveryAction::Resume(id) => {
                    if job.status != DurableJobStatus::Recovering {
                        self.registry
                            .mark_status(id, DurableJobStatus::Recovering)
                            .await?;
                    }
                    let (attempt, created) = self
                        .registry
                        .begin_recovery_attempt_once(id, self.startup_key)
                        .await?;
                    if created {
                        self.supervisor.enqueue_recovered(id, attempt.id).await.ok();
                    }
                }
                RecoveryAction::KeepPaused(id) => {
                    if job.status != DurableJobStatus::Paused {
                        self.registry
                            .mark_status(id, DurableJobStatus::Paused)
                            .await?;
                    }
                }
                RecoveryAction::KeepTerminal(_) | RecoveryAction::Fail(_, _) => {}
            }
            actions.push(action);
        }
        Ok(actions)
    }
}
