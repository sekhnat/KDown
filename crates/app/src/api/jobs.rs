//! Jobs collection, job detail, and explicit lifecycle command routes.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use uuid::Uuid;

use crate::api::dto::{
    AttemptDto, CancelJobCommand, JobDetailDto, JobPageDto, JobViewDto, PauseJobCommand,
    ResumeJobCommand, RetryJobCommand,
};
use crate::api::error::ApiError;
use crate::domain::{JobId, JobRecord, JobView};

/// Routes nested under `/api/v1`.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/jobs", get(list_jobs).post(create_job))
        .route("/jobs/{id}", get(get_job).delete(delete_job))
        .route("/jobs/{id}/pause", post(pause_job))
        .route("/jobs/{id}/resume", post(resume_job))
        .route("/jobs/{id}/cancel", post(cancel_job))
        .route("/jobs/{id}/retry", post(retry_job))
        .route("/jobs/{id}/reveal", post(reveal_job))
}

use super::AppState;
use crate::registry::JobPageQuery;

/// Query parameters for the jobs collection.
#[derive(Debug, Default, Deserialize)]
pub struct JobListParams {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    pub status: Option<String>,
    pub source: Option<String>,
    pub from: Option<i64>,
    pub to: Option<i64>,
}

fn parse_status(value: &str) -> Option<crate::domain::DurableJobStatus> {
    crate::domain::DurableJobStatus::from_db(value)
}

/// Enriches a job view with the root label and the final display
/// destination (relative to the root) once an attempt completed.
async fn enrich(registry: &crate::registry::Registry, view: JobView) -> JobViewDto {
    let mut dto = JobViewDto::from(view.clone());
    if let Ok(root) = registry.load_root(view.root_id).await {
        dto.root_label = root.label;
    }
    if dto.status == crate::api::dto::DurableJobStatusDto::Completed {
        if let Some(attempt_id) = view.attempt_id {
            if let Ok(attempt) = registry.load_attempt(attempt_id).await {
                if let Some(final_path) = attempt.final_path {
                    if let Ok(root) = registry.load_root(view.root_id).await {
                        if let Ok(relative) = final_path.strip_prefix(&root.canonical_path) {
                            dto.destination_display = Some(relative.to_string_lossy().into_owned());
                        }
                    }
                }
            }
        }
    }
    dto
}

/// Creates a job: validate, persist durable intent, then enqueue. The
/// response returns the current durable job view.
#[utoipa::path(
    post,
    path = "/api/v1/jobs",
    tag = "kdown",
    request_body = super::dto::CreateJobRequest,
    responses(
        (status = 201, description = "Job created", body = JobViewDto),
        (status = 422, description = "Validation failure", body = super::error::ApiErrorEnvelope),
        (status = 503, description = "Service degraded", body = super::error::ApiErrorEnvelope)
    )
)]
pub async fn create_job(
    State(state): State<AppState>,
    Json(request): Json<super::dto::CreateJobRequest>,
) -> Result<(StatusCode, Json<JobViewDto>), ApiError> {
    let intent = crate::domain::JobIntent {
        source: crate::domain::SourceUrl::parse(&request.source_url)?,
        root_id: crate::domain::RootId(request.root_id),
        relative_directory: request.relative_directory,
        filename_override: request.filename_override,
        conflict_policy: request.conflict_policy.map(Into::into).unwrap_or_default(),
    };
    // Root must exist and be enabled before intent is persisted.
    state.registry.load_enabled_root(intent.root_id).await?;
    let record = state.registry.insert_job(intent).await?;
    state.supervisor.enqueue(record.id).await?;
    let fresh = state.registry.load_job(record.id).await?;
    let dto = enrich(&state.registry, job_view_of(&fresh)).await;
    Ok((StatusCode::CREATED, Json(dto)))
}

fn job_view_of(record: &JobRecord) -> JobView {
    JobView {
        id: record.id,
        status: record.status,
        desired_state: record.desired_state,
        control_version: record.control_version,
        attempt_id: record.current_attempt_id,
        sample_seq: 0,
        source_display: record.intent.source.redacted(),
        root_id: record.intent.root_id,
        relative_directory: record.intent.relative_directory.clone(),
        filename_override: record.intent.filename_override.clone(),
        created_at: record.created_at,
        updated_at: record.updated_at,
        snapshot: None,
    }
}

/// Lists jobs with stable cursor pagination, newest first.
#[utoipa::path(
    get,
    path = "/api/v1/jobs",
    tag = "kdown",
    params(
        ("cursor" = Option<String>, Query, description = "Opaque continuation cursor"),
        ("limit" = Option<u32>, Query, description = "Page size (1-200)"),
        ("status" = Option<String>, Query, description = "Durable status filter"),
        ("source" = Option<String>, Query, description = "Source URL substring filter"),
        ("from" = Option<i64>, Query, description = "Created-at lower bound (epoch ms)"),
        ("to" = Option<i64>, Query, description = "Created-at upper bound (epoch ms)")
    ),
    responses(
        (status = 200, description = "Job page", body = JobPageDto)
    )
)]
pub async fn list_jobs(
    State(state): State<AppState>,
    Query(params): Query<JobListParams>,
) -> Result<Json<JobPageDto>, ApiError> {
    let cursor = params
        .cursor
        .as_deref()
        .map(parse_cursor)
        .transpose()
        .map_err(|_| ApiError::from_app(crate::error::AppError::InvalidSettings))?;
    let (records, next) = state
        .registry
        .list_jobs_page(JobPageQuery {
            status: params.status.as_deref().and_then(parse_status),
            source_contains: params.source,
            created_after: params.from,
            created_before: params.to,
            cursor,
            limit: params.limit.unwrap_or(50),
        })
        .await?;
    let mut jobs = Vec::with_capacity(records.len());
    for record in &records {
        jobs.push(enrich(&state.registry, job_view_of(record)).await);
    }
    Ok(Json(JobPageDto {
        jobs,
        next_cursor: next.map(|(updated, id)| format!("{updated}:{id}")),
    }))
}

fn parse_cursor(raw: &str) -> Result<(i64, String), ()> {
    let (updated, id) = raw.split_once(':').ok_or(())?;
    let updated: i64 = updated.parse().map_err(|_| ())?;
    Ok((updated, id.to_string()))
}

/// One job with its durable attempt history.
#[utoipa::path(
    get,
    path = "/api/v1/jobs/{id}",
    tag = "kdown",
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, description = "Job detail", body = JobDetailDto),
        (status = 404, description = "Not found", body = super::error::ApiErrorEnvelope)
    )
)]
pub async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JobDetailDto>, ApiError> {
    let record = state.registry.load_job(JobId(id)).await?;
    let attempts = state
        .registry
        .list_attempts(record.id)
        .await?
        .into_iter()
        .map(AttemptDto::from)
        .collect();
    let dto = enrich(&state.registry, job_view_of(&record)).await;
    Ok(Json(JobDetailDto { job: dto, attempts }))
}

/// Pauses the job when the caller's version is current.
#[utoipa::path(
    post,
    path = "/api/v1/jobs/{id}/pause",
    tag = "kdown",
    request_body = PauseJobCommand,
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, description = "Paused", body = JobViewDto),
        (status = 409, description = "Stale version or illegal transition", body = super::error::ApiErrorEnvelope)
    )
)]
pub async fn pause_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(command): Json<PauseJobCommand>,
) -> Result<Json<JobViewDto>, ApiError> {
    let view = state
        .supervisor
        .pause(
            JobId(id),
            crate::domain::ControlVersion::new(command.expected_control_version),
        )
        .await?;
    let dto = enrich(&state.registry, view).await;
    Ok(Json(dto))
}

/// Resumes the job when the caller's version is current.
#[utoipa::path(
    post,
    path = "/api/v1/jobs/{id}/resume",
    tag = "kdown",
    request_body = ResumeJobCommand,
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, description = "Resumed", body = JobViewDto),
        (status = 409, description = "Stale version or illegal transition", body = super::error::ApiErrorEnvelope)
    )
)]
pub async fn resume_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(command): Json<ResumeJobCommand>,
) -> Result<Json<JobViewDto>, ApiError> {
    let view = state
        .supervisor
        .resume(
            JobId(id),
            crate::domain::ControlVersion::new(command.expected_control_version),
        )
        .await?;
    let dto = enrich(&state.registry, view).await;
    Ok(Json(dto))
}

/// Cancels the job with an explicit artifact policy.
#[utoipa::path(
    post,
    path = "/api/v1/jobs/{id}/cancel",
    tag = "kdown",
    request_body = CancelJobCommand,
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, description = "Cancel accepted", body = JobViewDto),
        (status = 409, description = "Stale version or illegal transition", body = super::error::ApiErrorEnvelope)
    )
)]
pub async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(command): Json<CancelJobCommand>,
) -> Result<Json<JobViewDto>, ApiError> {
    let view = state
        .supervisor
        .cancel(
            JobId(id),
            crate::domain::ControlVersion::new(command.expected_control_version),
            command.artifact_policy.into(),
        )
        .await?;
    let dto = enrich(&state.registry, view).await;
    Ok(Json(dto))
}

/// Retries a terminal job under the same history.
#[utoipa::path(
    post,
    path = "/api/v1/jobs/{id}/retry",
    tag = "kdown",
    request_body = RetryJobCommand,
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, description = "Retry accepted", body = JobViewDto),
        (status = 409, description = "Stale version or illegal transition", body = super::error::ApiErrorEnvelope)
    )
)]
pub async fn retry_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(command): Json<RetryJobCommand>,
) -> Result<Json<JobViewDto>, ApiError> {
    let view = state
        .supervisor
        .retry(
            JobId(id),
            crate::domain::ControlVersion::new(command.expected_control_version),
        )
        .await?;
    let dto = enrich(&state.registry, view).await;
    Ok(Json(dto))
}

/// Reveals a completed artifact's parent directory via the desktop
/// integration. Only completed jobs with a recorded resolved destination
/// qualify.
#[utoipa::path(
    post,
    path = "/api/v1/jobs/{id}/reveal",
    tag = "kdown",
    params(("id" = Uuid, Path)),
    responses(
        (status = 204, description = "Revealed"),
        (status = 409, description = "Job is not completable for reveal", body = super::error::ApiErrorEnvelope)
    )
)]
pub async fn reveal_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let record = state.registry.load_job(JobId(id)).await?;
    if record.status != crate::domain::DurableJobStatus::Completed {
        return Err(ApiError::from_app(
            crate::error::AppError::InvalidTransition,
        ));
    }
    let attempt_id = record.current_attempt_id.ok_or(ApiError::from_app(
        crate::error::AppError::InvalidTransition,
    ))?;
    let attempt = state.registry.load_attempt(attempt_id).await?;
    let final_path = attempt.final_path.ok_or(ApiError::from_app(
        crate::error::AppError::InvalidTransition,
    ))?;
    let parent = final_path
        .parent()
        .ok_or(ApiError::from_app(
            crate::error::AppError::InvalidTransition,
        ))?
        .to_path_buf();
    state.desktop.reveal_parent(&parent).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Removes a terminal job's durable history. Never touches files.
#[utoipa::path(
    delete,
    path = "/api/v1/jobs/{id}",
    tag = "kdown",
    params(("id" = Uuid, Path)),
    responses(
        (status = 204, description = "History removed"),
        (status = 409, description = "Job is not terminal", body = super::error::ApiErrorEnvelope)
    )
)]
pub async fn delete_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let record = state.registry.load_job(JobId(id)).await?;
    if !record.status.is_terminal() {
        return Err(ApiError::from_app(
            crate::error::AppError::InvalidTransition,
        ));
    }
    state.registry.remove_job_history(record.id).await?;
    Ok(StatusCode::NO_CONTENT)
}
