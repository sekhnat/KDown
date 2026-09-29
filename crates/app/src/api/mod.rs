//! The secure Axum shell: one origin, loopback hosts only, JSON mutations
//! guarded by a per-process CSRF token, and a stable error envelope.

pub mod dto;
pub mod error;
pub mod openapi;
pub mod security;

use axum::extract::State;
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::registry::Registry;
use crate::supervisor::SupervisorHandle;
use dto::{BootstrapDto, CreateJobRequest, JobViewDto};

/// Shared application state for handlers.
#[derive(Clone, Debug)]
pub struct AppState {
    pub registry: Registry,
    pub supervisor: SupervisorHandle,
    pub security: std::sync::Arc<security::SecurityContext>,
    /// Per-process stream epoch; a restart produces a new value (Task 8).
    pub stream_epoch: String,
    pub build: String,
    /// The XDG Downloads directory suggestion for first-run setup.
    pub suggested_download_root: Option<String>,
}

impl AppState {
    pub fn new(registry: Registry, supervisor: SupervisorHandle) -> Self {
        Self::with_suggestion(registry, supervisor, None)
    }

    pub fn with_suggestion(
        registry: Registry,
        supervisor: SupervisorHandle,
        suggested_download_root: Option<String>,
    ) -> Self {
        let origin = "http://127.0.0.1".to_string();
        let security = std::sync::Arc::new(security::SecurityContext::generate(origin));
        let stream_epoch = uuid::Uuid::new_v4().to_string();
        let build = env!("CARGO_PKG_VERSION").to_string();
        Self {
            registry,
            supervisor,
            security,
            stream_epoch,
            build,
            suggested_download_root,
        }
    }
}

/// Session bootstrap: CSRF token and service identity, released only to
/// accepted loopback hosts, never cached.
#[utoipa::path(
    get,
    path = "/api/v1/bootstrap",
    tag = "kdown",
    responses(
        (status = 200, description = "Session bootstrap", body = BootstrapDto),
        (status = 403, description = "Unexpected host", body = error::ApiErrorEnvelope)
    )
)]
async fn bootstrap(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, error::ApiError> {
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !security::host_is_accepted(host) {
        return Err(error::ApiError::forbidden(
            "this service only accepts loopback hosts",
        ));
    }
    let body = Json(BootstrapDto {
        csrf_token: state.security.csrf_token().to_string(),
        origin: state.security.origin().to_string(),
        build: state.build.clone(),
        stream_epoch: state.stream_epoch.clone(),
        suggested_download_root: state.suggested_download_root.clone(),
    });
    Ok(([(header::CACHE_CONTROL, "no-store")], body).into_response())
}

/// Creates a job: validate, persist durable intent, then enqueue. The
/// response returns the current durable job view.
#[utoipa::path(
    post,
    path = "/api/v1/jobs",
    tag = "kdown",
    request_body = CreateJobRequest,
    responses(
        (status = 201, description = "Job created", body = JobViewDto),
        (status = 422, description = "Validation failure", body = error::ApiErrorEnvelope),
        (status = 503, description = "Service degraded", body = error::ApiErrorEnvelope)
    )
)]
async fn create_job(
    State(state): State<AppState>,
    Json(request): Json<CreateJobRequest>,
) -> Result<(StatusCode, Json<JobViewDto>), error::ApiError> {
    let intent = crate::domain::JobIntent {
        source: crate::domain::SourceUrl::parse(&request.source_url)?,
        root_id: crate::domain::RootId::from_uuid(request.root_id),
        relative_directory: request.relative_directory,
        filename_override: request.filename_override,
        conflict_policy: request.conflict_policy.map(Into::into).unwrap_or_default(),
    };
    // Root must exist and be enabled before intent is persisted.
    state.registry.load_enabled_root(intent.root_id).await?;
    let record = state.registry.insert_job(intent).await?;
    state.supervisor.enqueue(record.id).await?;
    let fresh = state.registry.load_job(record.id).await?;
    Ok((StatusCode::CREATED, Json(JobViewDto::from(fresh))))
}

/// JSON 404 for every unmatched API path; the SPA fallback (Task 13) never
/// masks these.
async fn api_not_found() -> error::ApiError {
    error::ApiError::not_found()
}

/// Restrictive hardening headers on every response.
async fn security_headers(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        header::HeaderValue::from_static(
            "default-src 'self'; script-src 'self'; style-src 'self'; \
             img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; \
             base-uri 'none'; form-action 'self'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        header::HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        header::X_FRAME_OPTIONS,
        header::HeaderValue::from_static("DENY"),
    );
    response
}

/// Guards mutations: JSON content type, per-process CSRF token, same-origin
/// Origin, and acceptable Fetch Metadata. Wrong content type is a 415; any
/// other violation is a 403. Safe methods pass through.
async fn mutation_guard(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) {
        return next.run(request).await;
    }
    let headers = request.headers();
    let wrong_content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value != "application/json");
    if wrong_content_type {
        return error::ApiError::unsupported_media_type().into_response();
    }
    if !security::mutation_is_allowed(headers, &state.security) {
        return error::ApiError::forbidden(
            "this action requires the session token and a same-origin request",
        )
        .into_response();
    }
    next.run(request).await
}

/// Builds the API router. CORS is deliberately not enabled: UI and API
/// share one origin, and no permissive CORS header is ever emitted.
pub fn build_router(state: AppState) -> Router {
    let api = Router::new()
        .route("/bootstrap", get(bootstrap))
        .route("/jobs", post(create_job))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            mutation_guard,
        ))
        .with_state(state);
    Router::new()
        .nest("/api/v1", api)
        .fallback(api_not_found)
        .layer(axum::middleware::from_fn(security_headers))
}
