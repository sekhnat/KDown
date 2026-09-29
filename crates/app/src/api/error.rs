//! The stable error envelope every non-success API response uses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::AppError;

/// Machine-stable error envelope: `code` identifies the failure class,
/// `message` is a safe user-facing summary, and optional fields carry
/// structured context. Secrets, URL query values, and credentials never
/// appear in `message` or `detail`.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ApiErrorEnvelope {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_errors: Option<std::collections::BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_job: Option<crate::api::dto::JobViewDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// HTTP mapping of [`AppError`], producing the stable envelope. The
/// envelope is boxed so `Result<T, ApiError>` handlers stay small.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub envelope: Box<ApiErrorEnvelope>,
}

impl ApiError {
    pub fn new(status: StatusCode, envelope: ApiErrorEnvelope) -> Self {
        Self {
            status,
            envelope: Box::new(envelope),
        }
    }

    pub fn not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            ApiErrorEnvelope {
                code: "not_found".to_string(),
                message: "the requested resource was not found".to_string(),
                retryable: false,
                field_errors: None,
                current_job: None,
                detail: None,
            },
        )
    }

    pub fn forbidden(message: &str) -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            ApiErrorEnvelope {
                code: "forbidden".to_string(),
                message: message.to_string(),
                retryable: false,
                field_errors: None,
                current_job: None,
                detail: None,
            },
        )
    }

    pub fn unsupported_media_type() -> Self {
        Self::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ApiErrorEnvelope {
                code: "unsupported_media_type".to_string(),
                message: "mutations must be sent as application/json".to_string(),
                retryable: false,
                field_errors: None,
                current_job: None,
                detail: None,
            },
        )
    }

    /// Maps an application error onto status code plus envelope.
    pub fn from_app(error: AppError) -> Self {
        let status = match &error {
            AppError::NotFound => StatusCode::NOT_FOUND,
            AppError::Conflict { .. } | AppError::InvalidTransition => StatusCode::CONFLICT,
            AppError::Persistence | AppError::ServiceDegraded => StatusCode::SERVICE_UNAVAILABLE,
            AppError::InvalidSourceUrl(_)
            | AppError::UnsupportedSourceScheme
            | AppError::SourceCredentialsUnsupported
            | AppError::DestinationOutsideRoot
            | AppError::RootUnavailable
            | AppError::RootInUse
            | AppError::DestinationUnavailable
            | AppError::InvalidSettings => StatusCode::UNPROCESSABLE_ENTITY,
            AppError::VersionExhausted => StatusCode::CONFLICT,
            AppError::EngineLaunch(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let field_errors = match &error {
            AppError::InvalidSourceUrl(_)
            | AppError::UnsupportedSourceScheme
            | AppError::SourceCredentialsUnsupported => {
                let mut map = std::collections::BTreeMap::new();
                map.insert("source_url".to_string(), error.to_string());
                Some(map)
            }
            AppError::DestinationOutsideRoot | AppError::DestinationUnavailable => {
                let mut map = std::collections::BTreeMap::new();
                map.insert("relative_directory".to_string(), error.to_string());
                Some(map)
            }
            AppError::RootUnavailable | AppError::RootInUse => {
                let mut map = std::collections::BTreeMap::new();
                map.insert("root_id".to_string(), error.to_string());
                Some(map)
            }
            _ => None,
        };
        let code = error.code().to_string();
        let message = error.to_string();
        let retryable = error.retryable();
        let current_job = match error {
            AppError::Conflict { current } => Some(crate::api::dto::JobViewDto::from(*current)),
            _ => None,
        };
        Self::new(
            status,
            ApiErrorEnvelope {
                code,
                message,
                retryable,
                field_errors,
                current_job,
                detail: None,
            },
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.envelope)).into_response()
    }
}

impl From<AppError> for ApiError {
    fn from(error: AppError) -> Self {
        Self::from_app(error)
    }
}
