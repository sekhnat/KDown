//! Deterministic OpenAPI document for the versioned API.

use utoipa::OpenApi;

use crate::api::dto::{
    AppSettingsDto, ArtifactPolicyDto, AttemptDto, AttemptMetricsDto, AttemptOutcomeDto,
    BootstrapDto, CancelJobCommand, ConflictPolicyDto, CreateJobRequest, CreateRootRequest,
    DesiredStateDto, DurableJobStatusDto, EngineSnapshotViewDto, EventEnvelopeDto, JobDetailDto,
    JobPageDto, JobViewDto, PatchRootRequest, PauseJobCommand, ResumeJobCommand, RetryJobCommand,
    RootDto, RootSummaryDto, UpdateSettingsRequest,
};
use crate::api::error::ApiErrorEnvelope;

#[derive(Debug, OpenApi)]
#[openapi(
    info(title = "KDown Local API", version = "1.0.0"),
    paths(
        crate::api::bootstrap,
        crate::api::jobs::create_job,
        crate::api::jobs::list_jobs,
        crate::api::jobs::get_job,
        crate::api::jobs::pause_job,
        crate::api::jobs::resume_job,
        crate::api::jobs::cancel_job,
        crate::api::jobs::retry_job,
        crate::api::jobs::reveal_job,
        crate::api::jobs::delete_job,
        crate::api::roots::list_roots,
        crate::api::roots::create_root,
        crate::api::roots::get_root,
        crate::api::roots::patch_root,
        crate::api::settings::get_settings,
        crate::api::settings::put_settings,
    ),
    components(schemas(
        BootstrapDto,
        CreateJobRequest,
        ConflictPolicyDto,
        DesiredStateDto,
        DurableJobStatusDto,
        EngineSnapshotViewDto,
        EventEnvelopeDto,
        JobViewDto,
        JobDetailDto,
        JobPageDto,
        AttemptDto,
        AttemptOutcomeDto,
        AttemptMetricsDto,
        PauseJobCommand,
        ResumeJobCommand,
        RetryJobCommand,
        CancelJobCommand,
        ArtifactPolicyDto,
        RootDto,
        RootSummaryDto,
        CreateRootRequest,
        PatchRootRequest,
        AppSettingsDto,
        UpdateSettingsRequest,
        ApiErrorEnvelope,
    )),
    tags(
        (name = "kdown", description = "Local download management API")
    )
)]
pub struct ApiDoc;

/// Returns the OpenAPI document. Serialization is deterministic.
pub fn document() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

/// Writes the OpenAPI document as pretty JSON to `path`.
pub fn write_openapi(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = serde_json::to_vec_pretty(&document())?;
    std::fs::write(path, bytes)?;
    Ok(())
}
