//! Commands the supervisor accepts from handles.

use tokio::sync::oneshot;

use crate::domain::{AttemptId, CancelArtifactPolicy, ControlVersion, JobId, JobView};
use crate::engine_adapter::EngineOutcome;
use crate::error::AppError;

/// One request to the single-owner supervisor actor. Lifecycle commands
/// carry the caller's last observed `control_version` so stale duplicates
/// lose the race deterministically.
pub enum SupervisorCommand {
    Enqueue {
        job_id: JobId,
        /// Recovery reuses its already-created attempt.
        attempt: Option<AttemptId>,
        reply: oneshot::Sender<Result<JobView, AppError>>,
    },
    Pause {
        job_id: JobId,
        expected: ControlVersion,
        reply: oneshot::Sender<Result<JobView, AppError>>,
    },
    Resume {
        job_id: JobId,
        expected: ControlVersion,
        reply: oneshot::Sender<Result<JobView, AppError>>,
    },
    Cancel {
        job_id: JobId,
        expected: ControlVersion,
        policy: CancelArtifactPolicy,
        reply: oneshot::Sender<Result<JobView, AppError>>,
    },
    Retry {
        job_id: JobId,
        expected: ControlVersion,
        reply: oneshot::Sender<Result<JobView, AppError>>,
    },
    UpdateLimits {
        active_downloads: usize,
        bytes_per_second: Option<u64>,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    AttemptFinished {
        job_id: JobId,
        attempt_id: AttemptId,
        outcome: EngineOutcome,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), AppError>>,
    },
}

impl std::fmt::Debug for SupervisorCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Reply channels and outcomes are deliberately not printed.
        match self {
            Self::Enqueue { job_id, .. } => f.debug_tuple("Enqueue").field(job_id).finish(),
            Self::Pause { job_id, .. } => f.debug_tuple("Pause").field(job_id).finish(),
            Self::Resume { job_id, .. } => f.debug_tuple("Resume").field(job_id).finish(),
            Self::Cancel { job_id, .. } => f.debug_tuple("Cancel").field(job_id).finish(),
            Self::Retry { job_id, .. } => f.debug_tuple("Retry").field(job_id).finish(),
            Self::UpdateLimits {
                active_downloads, ..
            } => f
                .debug_tuple("UpdateLimits")
                .field(active_downloads)
                .finish(),
            Self::AttemptFinished {
                job_id, attempt_id, ..
            } => f
                .debug_tuple("AttemptFinished")
                .field(job_id)
                .field(attempt_id)
                .finish(),
            Self::Shutdown { .. } => f.debug_tuple("Shutdown").finish(),
        }
    }
}
