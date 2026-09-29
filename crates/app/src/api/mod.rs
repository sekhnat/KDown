//! The secure Axum shell: one origin, loopback hosts only, JSON mutations
//! guarded by a per-process CSRF token, and a stable error envelope.

pub mod assets;
pub mod dto;
pub mod error;
pub mod jobs;
pub mod openapi;
pub mod roots;
pub mod security;
pub mod settings;
pub mod sse;

use axum::extract::State;
use axum::http::{header, HeaderMap, Method};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use crate::registry::Registry;
use crate::supervisor::SupervisorHandle;
use dto::BootstrapDto;

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
    /// Desktop integration for reveal actions; tests inject a no-op.
    pub desktop: std::sync::Arc<crate::platform::DesktopIntegration>,
    /// Bounded event broker shared with the supervisor.
    pub events: crate::events::EventBroker,
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
            desktop: std::sync::Arc::new(crate::platform::DesktopIntegration::native()),
            events: crate::events::EventBroker::new(256),
        }
    }

    /// Overrides the event broker so the API subscribes to the same broker
    /// the supervisor publishes through.
    pub fn with_events(mut self, events: crate::events::EventBroker) -> Self {
        self.events = events;
        self
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
    let roots = state
        .registry
        .list_roots()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|root| crate::api::dto::RootSummaryDto {
            id: root.id.0,
            label: root.label,
            enabled: root.enabled,
            is_default: root.is_default,
        })
        .collect();
    let body = Json(BootstrapDto {
        csrf_token: state.security.csrf_token().to_string(),
        origin: state.security.origin().to_string(),
        build: state.build.clone(),
        stream_epoch: state.stream_epoch.clone(),
        suggested_download_root: state.suggested_download_root.clone(),
        roots,
    });
    Ok(([(header::CACHE_CONTROL, "no-store")], body).into_response())
}

/// JSON 404 for unmatched API paths inside the nested scope; the outer
/// asset fallback also emits this shape for `/api/*` misses.
#[allow(dead_code)]
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
    build_router_with_web_dir(state, None)
}

/// Builds the full router: the API plus, when a web directory is
/// configured, static assets with SPA fallback. The API's JSON 404 stays
/// scoped under `/api/v1`; missing `/assets/*` never serves HTML.
pub fn build_router_with_web_dir(state: AppState, web_dir: Option<std::path::PathBuf>) -> Router {
    let api = Router::new()
        .route("/bootstrap", get(bootstrap))
        .route("/events", get(sse::stream))
        .merge(jobs::router())
        .merge(roots::router())
        .merge(settings::router())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            mutation_guard,
        ))
        .with_state(state);
    let assets = assets::router(web_dir);
    Router::new()
        .nest("/api/v1", api)
        .merge(assets)
        .layer(axum::middleware::from_fn(security_headers))
}
