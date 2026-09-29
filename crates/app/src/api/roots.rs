//! Configured-root administration routes. These are the only responses
//! that may carry absolute filesystem paths.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use uuid::Uuid;

use super::AppState;
use crate::api::dto::{CreateRootRequest, PatchRootRequest, RootDto};
use crate::api::error::ApiError;
use crate::domain::RootId;

/// Routes nested under `/api/v1`.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/roots", get(list_roots).post(create_root))
        .route("/roots/{id}", get(get_root).patch(patch_root))
}

/// Lists configured roots with canonical paths.
#[utoipa::path(
    get,
    path = "/api/v1/roots",
    tag = "kdown",
    responses(
        (status = 200, description = "Root list", body = Vec<RootDto>)
    )
)]
pub async fn list_roots(State(state): State<AppState>) -> Result<Json<Vec<RootDto>>, ApiError> {
    let roots = state.registry.list_roots().await?;
    Ok(Json(roots.into_iter().map(RootDto::from).collect()))
}

/// One configured root.
#[utoipa::path(
    get,
    path = "/api/v1/roots/{id}",
    tag = "kdown",
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, description = "Root", body = RootDto),
        (status = 404, description = "Not found", body = super::error::ApiErrorEnvelope)
    )
)]
pub async fn get_root(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<RootDto>, ApiError> {
    let root = state.registry.load_root(RootId(id)).await?;
    Ok(Json(RootDto::from(root)))
}

/// Adds an existing directory as a configured root. This is the explicit
/// authorization step, so an absolute path is expected here.
#[utoipa::path(
    post,
    path = "/api/v1/roots",
    tag = "kdown",
    request_body = CreateRootRequest,
    responses(
        (status = 201, description = "Root added", body = RootDto),
        (status = 422, description = "Path is not an existing directory", body = super::error::ApiErrorEnvelope)
    )
)]
pub async fn create_root(
    State(state): State<AppState>,
    Json(request): Json<CreateRootRequest>,
) -> Result<(StatusCode, Json<RootDto>), ApiError> {
    let policy = crate::path_policy::PathPolicy::new(state.registry.clone());
    let root = policy
        .add_root(
            &request.label,
            std::path::Path::new(&request.absolute_path),
            request.make_default,
        )
        .await?;
    Ok((StatusCode::CREATED, Json(RootDto::from(root))))
}

/// Updates label, enabled state, or default flag of a root.
#[utoipa::path(
    patch,
    path = "/api/v1/roots/{id}",
    tag = "kdown",
    request_body = PatchRootRequest,
    params(("id" = Uuid, Path)),
    responses(
        (status = 200, description = "Root updated", body = RootDto),
        (status = 409, description = "Root in use", body = super::error::ApiErrorEnvelope),
        (status = 422, description = "Invalid update", body = super::error::ApiErrorEnvelope)
    )
)]
pub async fn patch_root(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<PatchRootRequest>,
) -> Result<Json<RootDto>, ApiError> {
    let root_id = RootId(id);
    if let Some(label) = request.label {
        state.registry.rename_root(root_id, &label).await?;
    }
    let mut root = match request.enabled {
        Some(false) => Some(state.registry.disable_root(root_id).await?),
        Some(true) => Some(state.registry.enable_root(root_id).await?),
        None => None,
    };
    if request.make_default == Some(true) {
        state.registry.set_default_root(root_id).await?;
        root = Some(state.registry.load_root(root_id).await?);
    }
    let root = match root {
        Some(root) => root,
        None => state.registry.load_root(root_id).await?,
    };
    Ok(Json(RootDto::from(root)))
}
