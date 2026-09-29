//! Deterministic OpenAPI document for the versioned API.

use utoipa::OpenApi;

use crate::api::dto::{
    BootstrapDto, ConflictPolicyDto, CreateJobRequest, DesiredStateDto, DurableJobStatusDto,
    EngineSnapshotViewDto, JobViewDto,
};
use crate::api::error::ApiErrorEnvelope;

#[derive(Debug, OpenApi)]
#[openapi(
    info(title = "KDown Local API", version = "1.0.0"),
    paths(
        crate::api::bootstrap,
        crate::api::create_job,
    ),
    components(schemas(
        BootstrapDto,
        CreateJobRequest,
        ConflictPolicyDto,
        DesiredStateDto,
        DurableJobStatusDto,
        EngineSnapshotViewDto,
        JobViewDto,
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
