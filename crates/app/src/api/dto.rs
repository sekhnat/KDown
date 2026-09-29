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
    pub relative_directory: Option<String>,
    pub filename_override: Option<String>,
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
            relative_directory: value.relative_directory,
            filename_override: value.filename_override,
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
