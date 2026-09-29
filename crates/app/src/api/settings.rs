//! Settings routes: read and update typed global settings.

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};

use super::AppState;
use crate::api::dto::{AppSettingsDto, UpdateSettingsRequest};
use crate::api::error::ApiError;

/// Routes nested under `/api/v1`.
pub fn router() -> Router<AppState> {
    Router::new().route("/settings", get(get_settings).put(put_settings))
}

/// Returns the typed global settings.
#[utoipa::path(
    get,
    path = "/api/v1/settings",
    tag = "kdown",
    responses(
        (status = 200, description = "Settings", body = AppSettingsDto)
    )
)]
pub async fn get_settings(State(state): State<AppState>) -> Result<Json<AppSettingsDto>, ApiError> {
    let settings = state.registry.load_settings().await?;
    Ok(Json(AppSettingsDto::from(settings)))
}

/// Validates and persists settings, then applies the transfer limits to
/// the supervisor. Lowering active concurrency never cancels running work;
/// it only prevents the next launch until the count falls below the limit.
#[utoipa::path(
    put,
    path = "/api/v1/settings",
    tag = "kdown",
    request_body = UpdateSettingsRequest,
    responses(
        (status = 200, description = "Settings saved", body = AppSettingsDto),
        (status = 422, description = "Invalid values", body = super::error::ApiErrorEnvelope)
    )
)]
pub async fn put_settings(
    State(state): State<AppState>,
    Json(request): Json<UpdateSettingsRequest>,
) -> Result<Json<AppSettingsDto>, ApiError> {
    let settings: crate::registry::AppSettings = AppSettingsDto {
        active_concurrency: request.active_concurrency,
        rate_limit_bytes_per_second: request.rate_limit_bytes_per_second,
        default_root_id: request.default_root_id,
        notifications_enabled: request.notifications_enabled,
        startup_mode: request.startup_mode,
    }
    .try_into()?;
    let saved = state.registry.update_settings(settings).await?;
    state
        .supervisor
        .update_limits(
            usize::try_from(saved.active_concurrency).unwrap_or(1),
            saved.rate_limit_bytes_per_second,
        )
        .await
        .map_err(ApiError::from_app)?;
    Ok(Json(AppSettingsDto::from(saved)))
}
