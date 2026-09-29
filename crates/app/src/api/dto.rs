//! Wire DTOs for the versioned JSON API. The Rust contract is the source
//! of truth; frontend types are generated from the OpenAPI document.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::domain::{DesiredState, DurableJobStatus, JobView};

/// User-facing desired state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum DesiredStateDto {
    Running,
    Paused,
    Cancelled,
}

impl From<DesiredState> for DesiredStateDto {
    fn from(value: DesiredState) -> Self {
        match value {
            DesiredState::Running => Self::Running,
            DesiredState::Paused => Self::Paused,
            DesiredState::Cancelled => Self::Cancelled,
        }
    }
}

/// Durable lifecycle status as the UI presents it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum DurableJobStatusDto {
    Queued,
    Recovering,
    Active,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl From<DurableJobStatus> for DurableJobStatusDto {
    fn from(value: DurableJobStatus) -> Self {
        match value {
            DurableJobStatus::Queued => Self::Queued,
            DurableJobStatus::Recovering => Self::Recovering,
            DurableJobStatus::Active => Self::Active,
            DurableJobStatus::Paused => Self::Paused,
            DurableJobStatus::Completed => Self::Completed,
            DurableJobStatus::Failed => Self::Failed,
            DurableJobStatus::Cancelled => Self::Cancelled,
        }
    }
}

/// Display-safe telemetry snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EngineSnapshotViewDto {
    pub state_label: String,
    pub bytes_received: u64,
    pub network_bytes: u64,
    pub reused_bytes: u64,
    pub retries: u64,
    pub elapsed_ms: u64,
}

impl From<crate::engine_adapter::EngineSnapshotView> for EngineSnapshotViewDto {
    fn from(value: crate::engine_adapter::EngineSnapshotView) -> Self {
        Self {
            state_label: value.state_label,
            bytes_received: value.bytes_received,
            network_bytes: value.network_bytes,
            reused_bytes: value.reused_bytes,
            retries: value.retries,
            elapsed_ms: value.elapsed_ms,
        }
    }
}

/// Display-safe job view. Source URLs are redacted; filesystem paths are
/// reduced to relative display form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct JobViewDto {
    pub id: uuid::Uuid,
    pub status: DurableJobStatusDto,
    pub desired_state: DesiredStateDto,
    pub control_version: u64,
    pub attempt_id: Option<uuid::Uuid>,
    pub sample_seq: u64,
    pub source_display: String,
    pub root_id: uuid::Uuid,
    /// Root label; ordinary job views never carry absolute paths.
    pub root_label: String,
    pub relative_directory: Option<String>,
    pub filename_override: Option<String>,
    /// Final display destination relative to the root, once completed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_display: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<EngineSnapshotViewDto>,
}

impl From<JobView> for JobViewDto {
    fn from(value: JobView) -> Self {
        Self {
            id: value.id.0,
            status: value.status.into(),
            desired_state: value.desired_state.into(),
            control_version: value.control_version.get(),
            attempt_id: value.attempt_id.map(|id| id.0),
            sample_seq: value.sample_seq,
            source_display: value.source_display,
            root_id: value.root_id.0,
            root_label: String::new(),
            relative_directory: value.relative_directory,
            filename_override: value.filename_override,
            destination_display: None,
            created_at: value.created_at,
            updated_at: value.updated_at,
            snapshot: value.snapshot.map(Into::into),
        }
    }
}

impl From<crate::domain::JobRecord> for JobViewDto {
    fn from(record: crate::domain::JobRecord) -> Self {
        JobViewDto::from(JobView {
            id: record.id,
            status: record.status,
            desired_state: record.desired_state,
            control_version: record.control_version,
            attempt_id: record.current_attempt_id,
            sample_seq: 0,
            source_display: record.intent.source.redacted(),
            root_id: record.intent.root_id,
            relative_directory: record.intent.relative_directory,
            filename_override: record.intent.filename_override,
            created_at: record.created_at,
            updated_at: record.updated_at,
            snapshot: None,
        })
    }
}

/// Session/bootstrap data: the CSRF token plus service identity.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BootstrapDto {
    pub csrf_token: String,
    pub origin: String,
    pub build: String,
    pub stream_epoch: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_download_root: Option<String>,
}

/// Request body for creating a job. Ordinary creation never carries
/// absolute destinations: an opaque root ID plus relative paths only.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateJobRequest {
    pub source_url: String,
    pub root_id: uuid::Uuid,
    #[serde(default)]
    pub relative_directory: Option<String>,
    #[serde(default)]
    pub filename_override: Option<String>,
    #[serde(default)]
    pub conflict_policy: Option<ConflictPolicyDto>,
}

/// What happens when the destination already exists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicyDto {
    #[default]
    FailIfExists,
    Overwrite,
    Rename,
    Resume,
}

impl From<ConflictPolicyDto> for crate::domain::ConflictPolicy {
    fn from(value: ConflictPolicyDto) -> Self {
        match value {
            ConflictPolicyDto::FailIfExists => Self::FailIfExists,
            ConflictPolicyDto::Overwrite => Self::Overwrite,
            ConflictPolicyDto::Rename => Self::Rename,
            ConflictPolicyDto::Resume => Self::Resume,
        }
    }
}

/// Terminal result of one attempt, display-safe.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct AttemptOutcomeDto {
    /// One of `completed`, `failed`, `cancelled`, `interrupted`.
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<AttemptMetricsDto>,
}

/// Final byte counts and wall time of an attempt.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AttemptMetricsDto {
    pub bytes_received: u64,
    pub network_bytes: u64,
    pub duration_ms: u64,
}

/// One attempt in a job's durable history. Absolute paths are never
/// exposed here; the job view carries relative display destinations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct AttemptDto {
    pub id: uuid::Uuid,
    /// One of `initial`, `retry`, `recovery`.
    pub reason: String,
    pub started_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<AttemptOutcomeDto>,
}

impl From<crate::domain::AttemptRecord> for AttemptDto {
    fn from(value: crate::domain::AttemptRecord) -> Self {
        Self {
            id: value.id.0,
            reason: value.reason.as_db().to_string(),
            started_at: value.started_at,
            finished_at: value.finished_at,
            outcome: value.outcome.map(|outcome| match outcome {
                crate::domain::AttemptOutcome::Completed(metrics) => AttemptOutcomeDto {
                    kind: "completed".to_string(),
                    code: None,
                    detail: None,
                    metrics: Some(AttemptMetricsDto {
                        bytes_received: metrics.bytes_received,
                        network_bytes: metrics.network_bytes,
                        duration_ms: metrics.duration_ms,
                    }),
                },
                crate::domain::AttemptOutcome::Failed {
                    code,
                    detail,
                    metrics,
                } => AttemptOutcomeDto {
                    kind: "failed".to_string(),
                    code: Some(code),
                    detail,
                    metrics: metrics.map(|metrics| AttemptMetricsDto {
                        bytes_received: metrics.bytes_received,
                        network_bytes: metrics.network_bytes,
                        duration_ms: metrics.duration_ms,
                    }),
                },
                crate::domain::AttemptOutcome::Cancelled => AttemptOutcomeDto {
                    kind: "cancelled".to_string(),
                    code: None,
                    detail: None,
                    metrics: None,
                },
                crate::domain::AttemptOutcome::Interrupted => AttemptOutcomeDto {
                    kind: "interrupted".to_string(),
                    code: None,
                    detail: None,
                    metrics: None,
                },
            }),
        }
    }
}

/// One job with its durable attempt history.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobDetailDto {
    pub job: JobViewDto,
    pub attempts: Vec<AttemptDto>,
}

/// One page of the jobs collection.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobPageDto {
    pub jobs: Vec<JobViewDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Lifecycle command preconditions: the caller's last observed version.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PauseJobCommand {
    pub expected_control_version: u64,
}

/// Resume preconditions.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct ResumeJobCommand {
    pub expected_control_version: u64,
}

/// Retry preconditions.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct RetryJobCommand {
    pub expected_control_version: u64,
}

/// What cancellation should do with partial artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPolicyDto {
    PreservePartial,
    DeletePartial,
    KeepFileDiscardCheckpoint,
}

impl From<ArtifactPolicyDto> for crate::domain::CancelArtifactPolicy {
    fn from(value: ArtifactPolicyDto) -> Self {
        match value {
            ArtifactPolicyDto::PreservePartial => Self::PreservePartial,
            ArtifactPolicyDto::DeletePartial => Self::DeletePartial,
            ArtifactPolicyDto::KeepFileDiscardCheckpoint => Self::KeepFileDiscardCheckpoint,
        }
    }
}

/// Cancel preconditions plus the explicit artifact choice.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CancelJobCommand {
    pub expected_control_version: u64,
    pub artifact_policy: ArtifactPolicyDto,
}

/// A configured root as the administration surface presents it. This is
/// the only DTO family that may carry absolute filesystem paths.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RootDto {
    pub id: uuid::Uuid,
    pub label: String,
    pub canonical_path: String,
    pub enabled: bool,
    pub is_default: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

impl From<crate::domain::RootRecord> for RootDto {
    fn from(value: crate::domain::RootRecord) -> Self {
        Self {
            id: value.id.0,
            label: value.label,
            canonical_path: value.canonical_path.to_string_lossy().into_owned(),
            enabled: value.enabled,
            is_default: value.is_default,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

/// Root identity without filesystem paths, safe for bootstrap.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RootSummaryDto {
    pub id: uuid::Uuid,
    pub label: String,
    pub enabled: bool,
    pub is_default: bool,
}

/// Request to add an existing directory as a configured root.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateRootRequest {
    pub label: String,
    pub absolute_path: String,
    #[serde(default)]
    pub make_default: bool,
}

/// Partial root update.
#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
pub struct PatchRootRequest {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub make_default: Option<bool>,
}

/// Typed global settings as stored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct AppSettingsDto {
    pub active_concurrency: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_bytes_per_second: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_root_id: Option<uuid::Uuid>,
    pub notifications_enabled: bool,
    /// One of `manual`, `service`.
    pub startup_mode: String,
}

impl From<crate::registry::AppSettings> for AppSettingsDto {
    fn from(value: crate::registry::AppSettings) -> Self {
        Self {
            active_concurrency: value.active_concurrency,
            rate_limit_bytes_per_second: value.rate_limit_bytes_per_second,
            default_root_id: value.default_root_id.map(|id| id.0),
            notifications_enabled: value.notifications_enabled,
            startup_mode: value.startup_mode.as_db().to_string(),
        }
    }
}

impl TryFrom<AppSettingsDto> for crate::registry::AppSettings {
    type Error = crate::error::AppError;

    fn try_from(value: AppSettingsDto) -> Result<Self, Self::Error> {
        let startup_mode = crate::registry::StartupMode::from_db(&value.startup_mode)
            .ok_or(crate::error::AppError::InvalidSettings)?;
        Ok(Self {
            active_concurrency: value.active_concurrency,
            rate_limit_bytes_per_second: value.rate_limit_bytes_per_second,
            default_root_id: value.default_root_id.map(crate::domain::RootId::from_uuid),
            notifications_enabled: value.notifications_enabled,
            startup_mode,
        })
    }
}

/// Full settings update (PUT semantics: every field is required).
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct UpdateSettingsRequest {
    pub active_concurrency: u32,
    #[serde(default)]
    pub rate_limit_bytes_per_second: Option<u64>,
    #[serde(default)]
    pub default_root_id: Option<uuid::Uuid>,
    pub notifications_enabled: bool,
    pub startup_mode: String,
}

/// One SSE stream event. `job` carries the complete display-safe view for
/// `job.snapshot`; `hello` carries the stream epoch and build identity.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EventEnvelopeDto {
    /// One of `hello`, `job.snapshot`, `job.removed`, `settings.changed`,
    /// `service.degraded`.
    pub kind: String,
    /// Process stream epoch: changes across service restarts.
    pub stream_epoch: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job: Option<JobViewDto>,
}

impl EventEnvelopeDto {
    pub fn hello(stream_epoch: String, build: String) -> Self {
        Self {
            kind: "hello".to_string(),
            stream_epoch,
            build: Some(build),
            job: None,
        }
    }

    pub fn job_snapshot(view: crate::domain::JobView) -> Self {
        Self {
            kind: "job.snapshot".to_string(),
            // The epoch is filled by the stream layer before serialization.
            stream_epoch: String::new(),
            build: None,
            job: Some(JobViewDto::from(view)),
        }
    }

    pub fn job_removed(job_id: crate::domain::JobId, stream_epoch: String) -> Self {
        let _ = job_id;
        Self {
            kind: "job.removed".to_string(),
            stream_epoch,
            build: None,
            job: None,
        }
    }

    pub fn service_degraded() -> Self {
        Self {
            kind: "service.degraded".to_string(),
            stream_epoch: String::new(),
            build: None,
            job: None,
        }
    }
}
