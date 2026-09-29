//! Application error taxonomy, independent of HTTP transport.
//!
//! Every variant carries a stable machine-readable `code` and a safe
//! user-facing message. Secrets, URL query values, and credentials never
//! appear in messages or debug output.

/// Application-level failure. HTTP layers map this onto the stable error
/// envelope without inventing new meanings.
#[derive(Debug, Clone, thiserror::Error)]
pub enum AppError {
    /// The source URL did not parse.
    #[error("source URL is invalid")]
    InvalidSourceUrl(url::ParseError),
    /// Only HTTP and HTTPS sources are supported in this release.
    #[error("only HTTP and HTTPS source URLs are supported")]
    UnsupportedSourceScheme,
    /// Embedded URL credentials are rejected before persistence.
    #[error("source URLs with embedded credentials are not supported")]
    SourceCredentialsUnsupported,
    /// The durable mutation version space is exhausted.
    #[error("control version space is exhausted")]
    VersionExhausted,
    /// The requested lifecycle transition is not legal for the job state.
    #[error("the requested action is not valid for this job's current state")]
    InvalidTransition,
    /// The referenced durable record does not exist.
    #[error("the requested job was not found")]
    NotFound,
    /// A concurrent mutation changed the job first; the current state is
    /// A concurrent mutation changed the job first; the current state is
    /// reported back to the caller.
    #[error("the job changed concurrently; the shown state is current")]
    Conflict {
        current: Box<crate::domain::JobRecord>,
    },
    /// Durable persistence failed; no new engine work may start.
    #[error("the service could not record the change durably")]
    Persistence,
    /// The destination escaped the configured root or was otherwise invalid.
    #[error("the destination must stay inside the configured download root")]
    DestinationOutsideRoot,
    /// A configured root was not found, is disabled, or is invalid.
    #[error("the download root is unavailable")]
    RootUnavailable,
    /// Engine construction or launch failed before any transfer began.
    #[error("the download engine could not be started")]
    EngineLaunch(String),
    /// The service is degraded; mutations are rejected until it recovers.
    #[error("the service is degraded and cannot accept this action right now")]
    ServiceDegraded,
}

impl AppError {
    /// Stable machine identifier used by the API error envelope.
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidSourceUrl(_) => "source_url_invalid",
            Self::UnsupportedSourceScheme => "source_scheme_unsupported",
            Self::SourceCredentialsUnsupported => "source_credentials_unsupported",
            Self::VersionExhausted => "version_exhausted",
            Self::InvalidTransition => "invalid_transition",
            Self::NotFound => "not_found",
            Self::Conflict { .. } => "conflict",
            Self::Persistence => "persistence_failed",
            Self::DestinationOutsideRoot => "destination_outside_root",
            Self::RootUnavailable => "root_unavailable",
            Self::EngineLaunch(_) => "engine_launch_failed",
            Self::ServiceDegraded => "service_degraded",
        }
    }

    /// Returns the rejected mutation together with the current durable
    /// state when this error is a mutation conflict, so stale commands
    /// can be answered with live state.
    pub fn into_conflict(self) -> Option<ConflictError> {
        match self {
            Self::Conflict { current } => Some(ConflictError { current: *current }),
            _ => None,
        }
    }

    /// Whether repeating the operation unchanged can reasonably succeed.
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Persistence | Self::ServiceDegraded)
    }
}

/// A rejected stale mutation plus the current durable job record.
/// The loser of a command race receives this so it can show the real state.
#[derive(Debug, Clone)]
pub struct ConflictError {
    pub current: crate::domain::JobRecord,
}
