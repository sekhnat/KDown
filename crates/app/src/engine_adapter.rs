//! Translation between application launches/commands and `kdown-engine`.
//!
//! The launcher boundary is generic and static: the supervisor is generic
//! over [`EngineLauncher`], so the production engine and test doubles use
//! the same call shape with no trait objects on the launcher itself.

use std::future::Future;
use std::path::PathBuf;

use kdown_engine::{
    CancelMode, CompletedDownload, DirectoryDownloadRequest, DownloadController, DownloadHandle,
    DownloadRequest, DownloadRunError, OverwritePolicy, ResumePolicy,
};
use tokio::task::JoinHandle;

use crate::domain::{
    AttemptId, AttemptMetrics, CancelArtifactPolicy, ConflictPolicy, JobId, SourceUrl,
};
use crate::error::AppError;

/// Everything an engine run needs, already validated by the host.
#[derive(Clone, Debug)]
pub struct EngineLaunch {
    pub job_id: JobId,
    pub attempt_id: AttemptId,
    pub source: SourceUrl,
    /// Directory when no filename override exists, full file path otherwise.
    pub destination: PathBuf,
    pub filename_override: Option<String>,
    pub conflict_policy: ConflictPolicy,
}

/// Terminal result of one engine run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineOutcome {
    Completed,
    Failed {
        code: String,
        detail: Option<String>,
    },
    Cancelled,
}

/// A launched run: a live control handle plus the completion future.
pub struct EngineRun<H, C> {
    pub handle: H,
    pub completion: C,
}

impl<H, C> std::fmt::Debug for EngineRun<H, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineRun").finish_non_exhaustive()
    }
}

/// Display-safe engine lifecycle label.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineStateView {
    pub label: String,
    pub terminal: bool,
}

/// Display-safe telemetry snapshot; contains no URLs or credentials.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct EngineSnapshotView {
    pub state_label: String,
    pub bytes_received: u64,
    pub network_bytes: u64,
    pub reused_bytes: u64,
    pub retries: u64,
    pub elapsed_ms: u64,
}

/// Control surface the supervisor uses on live runs.
pub trait EngineControl: Send + 'static {
    fn state(&self) -> EngineStateView;
    fn snapshot(&self) -> EngineSnapshotView;
    fn pause(&self) -> Result<(), AppError>;
    fn resume_now(&self) -> Result<(), AppError>;
    fn cancel_with(&self, policy: CancelArtifactPolicy) -> Result<(), AppError>;
    fn resolved_destination(&self) -> Option<PathBuf>;
}

/// Launches engine runs. Static dispatch only: the supervisor is generic
/// over the launcher, never over a `dyn` trait object.
pub trait EngineLauncher: Send + Sync + 'static {
    type Handle: EngineControl;
    type Completion: Future<Output = EngineOutcome> + Send + 'static;

    fn launch(
        &self,
        launch: EngineLaunch,
    ) -> impl Future<Output = Result<EngineRun<Self::Handle, Self::Completion>, AppError>> + Send;

    /// Applies the global rate limit; `None` means unlimited.
    fn set_global_rate_limit(&self, bytes_per_second: Option<u64>) -> Result<(), AppError>;
}

/// Maps the user-facing conflict choice onto engine policies.
pub fn overwrite_policy_for(policy: ConflictPolicy) -> (OverwritePolicy, ResumePolicy) {
    match policy {
        ConflictPolicy::FailIfExists => (OverwritePolicy::FailIfExists, ResumePolicy::Allowed),
        ConflictPolicy::Overwrite => (OverwritePolicy::Replace, ResumePolicy::Allowed),
        ConflictPolicy::Rename => (OverwritePolicy::Rename, ResumePolicy::Allowed),
        ConflictPolicy::Resume => (OverwritePolicy::ResumeIfMatching, ResumePolicy::Allowed),
    }
}

/// Maps cancellation policy onto the engine's cancel mode, one to one.
pub fn cancel_mode_for(policy: CancelArtifactPolicy) -> CancelMode {
    match policy {
        CancelArtifactPolicy::PreservePartial => CancelMode::KeepPartial,
        CancelArtifactPolicy::DeletePartial => CancelMode::DeletePartial,
        CancelArtifactPolicy::KeepFileDiscardCheckpoint => CancelMode::KeepFileDiscardCheckpoint,
    }
}

/// Metrics derived from an engine's final accounting.
pub fn metrics_from_accounting(
    bytes_received: u64,
    network_bytes: u64,
    elapsed: std::time::Duration,
) -> AttemptMetrics {
    AttemptMetrics {
        bytes_received,
        network_bytes,
        duration_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
    }
}

fn outcome_from_join(
    result: Result<Result<CompletedDownload, DownloadRunError>, tokio::task::JoinError>,
) -> EngineOutcome {
    match result {
        Ok(Ok(_completed)) => EngineOutcome::Completed,
        Ok(Err(DownloadRunError::Cancelled(_))) => EngineOutcome::Cancelled,
        Ok(Err(DownloadRunError::Transfer(failure))) => EngineOutcome::Failed {
            code: "transfer_failed".to_string(),
            // The engine redacts its own Display output at formatting
            // boundaries; truncate to keep browser error detail bounded.
            detail: Some(truncate_detail(&failure.to_string())),
        },
        Ok(Err(DownloadRunError::Infrastructure(failure))) => EngineOutcome::Failed {
            code: "engine_infrastructure_failed".to_string(),
            detail: Some(truncate_detail(&failure.to_string())),
        },
        Err(join_error) => EngineOutcome::Failed {
            code: "engine_task_failed".to_string(),
            detail: Some(truncate_detail(&join_error.to_string())),
        },
        Ok(Err(other)) => EngineOutcome::Failed {
            code: "engine_run_failed".to_string(),
            detail: Some(truncate_detail(&other.to_string())),
        },
    }
}

fn truncate_detail(detail: &str) -> String {
    const LIMIT: usize = 200;
    let mut end = LIMIT;
    while end > 0 && !detail.is_char_boundary(end) {
        end -= 1;
    }
    detail[..end].to_string()
}

/// The production launcher: owns one `DownloadController` and constructs
/// engine requests from validated application intent.
pub struct KdownEngineLauncher {
    controller: DownloadController,
}

impl std::fmt::Debug for KdownEngineLauncher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KdownEngineLauncher")
            .finish_non_exhaustive()
    }
}

impl KdownEngineLauncher {
    pub fn new(controller: DownloadController) -> Self {
        Self { controller }
    }
}

/// Concrete completion: an async wrapper around the engine's join handle
/// that adapts the join result into [`EngineOutcome`].
pub type KdownEngineCompletion = std::pin::Pin<Box<dyn Future<Output = EngineOutcome> + Send>>;

impl EngineLauncher for KdownEngineLauncher {
    type Handle = DownloadHandle;
    type Completion = KdownEngineCompletion;

    async fn launch(
        &self,
        launch: EngineLaunch,
    ) -> Result<EngineRun<Self::Handle, Self::Completion>, AppError> {
        let url = launch.source.persisted().to_string();
        let (overwrite, resume) = overwrite_policy_for(launch.conflict_policy);
        let (handle, task): (
            DownloadHandle,
            JoinHandle<Result<CompletedDownload, DownloadRunError>>,
        ) = match launch.filename_override.as_deref() {
            Some(_override) => {
                let mut request = DownloadRequest::new(url, launch.destination.clone());
                request.overwrite = overwrite;
                request.resume = resume;
                self.controller.start(request)
            }
            None => {
                let mut directory_request =
                    DirectoryDownloadRequest::new(url, launch.destination.clone());
                directory_request.request_mut().overwrite = overwrite;
                directory_request.request_mut().resume = resume;
                self.controller.start_to_directory(directory_request)
            }
        };
        let completion: KdownEngineCompletion =
            Box::pin(async move { outcome_from_join(task.await) });
        Ok(EngineRun { handle, completion })
    }

    fn set_global_rate_limit(&self, bytes_per_second: Option<u64>) -> Result<(), AppError> {
        // The engine treats 0 as unlimited.
        self.controller
            .set_global_rate_limit(bytes_per_second.unwrap_or(0));
        Ok(())
    }
}

impl EngineControl for DownloadHandle {
    fn state(&self) -> EngineStateView {
        let state = self.state();
        EngineStateView {
            label: state.to_string(),
            terminal: state.is_terminal(),
        }
    }

    fn snapshot(&self) -> EngineSnapshotView {
        let snapshot = self.snapshot();
        EngineSnapshotView {
            state_label: self.state().to_string(),
            bytes_received: snapshot.completed_bytes,
            network_bytes: snapshot.network_bytes,
            reused_bytes: snapshot.reused_bytes,
            retries: snapshot.retries,
            elapsed_ms: u64::try_from(snapshot.elapsed.as_millis()).unwrap_or(u64::MAX),
        }
    }

    fn pause(&self) -> Result<(), AppError> {
        self.pause();
        Ok(())
    }

    fn resume_now(&self) -> Result<(), AppError> {
        self.resume_now();
        Ok(())
    }

    fn cancel_with(&self, policy: CancelArtifactPolicy) -> Result<(), AppError> {
        self.cancel_with(cancel_mode_for(policy));
        Ok(())
    }

    fn resolved_destination(&self) -> Option<PathBuf> {
        self.resolved_destination().map(PathBuf::from)
    }
}
