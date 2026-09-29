//! Durable domain types owned by the application host.
//!
//! These types are the persistence-facing contract between the registry,
//! supervisor, and API layers. Live transfer state is owned by
//! `kdown-engine`; application types here never wrap engine internals
//! except where a display view is explicitly constructed from
//! [`kdown_engine::JobState`].

use crate::error::AppError;

/// Durable job identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JobId(uuid::Uuid);

impl JobId {
    /// Rebuilds an identifier from its persisted string form.
    pub(crate) fn from_uuid(value: uuid::Uuid) -> Self {
        Self(value)
    }
}

impl JobId {
    #[must_use]
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Durable attempt identifier; a new attempt resets per-attempt sequencing.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AttemptId(uuid::Uuid);

impl AttemptId {
    /// Rebuilds an identifier from its persisted string form.
    pub(crate) fn from_uuid(value: uuid::Uuid) -> Self {
        Self(value)
    }
}

impl AttemptId {
    #[must_use]
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for AttemptId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for AttemptId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Opaque configured-root identifier used by all job views and commands.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RootId(uuid::Uuid);

impl RootId {
    /// Rebuilds an identifier from its persisted string form.
    pub(crate) fn from_uuid(value: uuid::Uuid) -> Self {
        Self(value)
    }
}

impl RootId {
    #[must_use]
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for RootId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RootId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One-shot launch identity: retries and recovery attempts use a fresh key,
/// while repeated work within one accepted command or startup reuses it.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LaunchKey(uuid::Uuid);

impl LaunchKey {
    /// Rebuilds an identifier from its persisted string form.
    pub(crate) fn from_uuid(value: uuid::Uuid) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for LaunchKey {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for LaunchKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Durable mutation version used for compare-and-set conflict detection.
/// Bounded to the JSON safe-integer range so browser clients compare it
/// exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ControlVersion(u64);

impl ControlVersion {
    /// Largest value representable exactly as a JSON/JavaScript integer.
    pub const MAX_JSON_INTEGER: u64 = 9_007_199_254_740_991;

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Advances by one; fails closed instead of wrapping.
    pub fn next(self) -> Result<Self, AppError> {
        if self.0 >= Self::MAX_JSON_INTEGER {
            return Err(AppError::VersionExhausted);
        }
        Ok(Self(self.0 + 1))
    }
}

impl std::fmt::Display for ControlVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Full source URL. The complete value, including signed query strings,
/// survives persistence and recovery; every log/display-safe representation
/// hides query values, and embedded userinfo is rejected at parse time.
#[derive(Clone)]
pub struct SourceUrl(url::Url);

impl SourceUrl {
    pub fn parse(input: &str) -> Result<Self, AppError> {
        let parsed = url::Url::parse(input).map_err(AppError::InvalidSourceUrl)?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(AppError::UnsupportedSourceScheme);
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(AppError::SourceCredentialsUnsupported);
        }
        Ok(Self(parsed))
    }

    /// Byte-for-byte URL used for persistence and engine launch.
    pub fn persisted(&self) -> &str {
        self.0.as_str()
    }

    /// Display-safe representation: origin plus path, with any query
    /// reduced to a marker and the fragment removed.
    pub fn redacted(&self) -> String {
        let mut safe = self.0.clone();
        let had_query = safe.query().is_some();
        safe.set_query(None);
        safe.set_fragment(None);
        let mut display = safe.to_string();
        if had_query {
            display.push_str("?…");
        }
        display
    }
}

impl std::fmt::Debug for SourceUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SourceUrl").field(&self.redacted()).finish()
    }
}

impl std::fmt::Display for SourceUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.redacted())
    }
}

/// What the user wants the job to do next; persisted intent, distinct from
/// the engine's live lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DesiredState {
    Running,
    Paused,
    Cancelled,
}

impl DesiredState {
    /// Storage representation used by the SQLite registry.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Cancelled => "cancelled",
        }
    }

    /// Parses the storage representation.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "running" => Some(Self::Running),
            "paused" => Some(Self::Paused),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// Durable lifecycle status recorded by the host. `Queued` and `Recovering`
/// are application states that wrap the engine's own lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DurableJobStatus {
    Queued,
    Recovering,
    Active,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl DurableJobStatus {
    /// Terminal statuses keep their history and artifacts but accept no
    /// further lifecycle mutation except removal from history.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl DurableJobStatus {
    /// Storage representation used by the SQLite registry.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Recovering => "recovering",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Parses the storage representation.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "recovering" => Some(Self::Recovering),
            "active" => Some(Self::Active),
            "paused" => Some(Self::Paused),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// What cancellation should do to partial artifacts, mapped one-to-one onto
/// the engine's `CancelMode` by the adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum CancelArtifactPolicy {
    PreservePartial,
    DeletePartial,
    KeepFileDiscardCheckpoint,
}

/// What happens when the destination already exists, expressed in user
/// terms and mapped onto engine overwrite/resume policy by the adapter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ConflictPolicy {
    /// Reject the job before network activity.
    #[default]
    FailIfExists,
    /// Replace the existing destination atomically.
    Overwrite,
    /// Never replace; resolve a free sibling name instead.
    Rename,
    /// Resume matching existing partial output.
    Resume,
}

impl ConflictPolicy {
    /// Storage representation used by the SQLite registry.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::FailIfExists => "fail_if_exists",
            Self::Overwrite => "overwrite",
            Self::Rename => "rename",
            Self::Resume => "resume",
        }
    }

    /// Parses the storage representation.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "fail_if_exists" => Some(Self::FailIfExists),
            "overwrite" => Some(Self::Overwrite),
            "rename" => Some(Self::Rename),
            "resume" => Some(Self::Resume),
            _ => None,
        }
    }
}

/// Validated, durable creation intent for one job. Browser input never
/// carries absolute destinations: jobs reference an opaque root plus a
/// relative destination.
#[derive(Clone, Debug)]
pub struct JobIntent {
    pub source: SourceUrl,
    pub root_id: RootId,
    /// Optional safe subfolder inside the root; empty means the root itself.
    pub relative_directory: Option<String>,
    /// Optional explicit filename; empty means engine-resolved naming.
    pub filename_override: Option<String>,
    pub conflict_policy: ConflictPolicy,
}

/// Durable job record as stored in the registry.
#[derive(Clone, Debug)]
pub struct JobRecord {
    pub id: JobId,
    pub intent: JobIntent,
    pub desired_state: DesiredState,
    pub status: DurableJobStatus,
    pub control_version: ControlVersion,
    pub current_attempt_id: Option<AttemptId>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Why an attempt exists: the first launch, an explicit user retry, or
/// service-startup recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptReason {
    Initial,
    Retry,
    Recovery,
}

impl AttemptReason {
    /// Storage representation used by the SQLite registry.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Retry => "retry",
            Self::Recovery => "recovery",
        }
    }

    /// Parses the storage representation.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "initial" => Some(Self::Initial),
            "retry" => Some(Self::Retry),
            "recovery" => Some(Self::Recovery),
            _ => None,
        }
    }
}

/// Final byte counts and wall time recorded once per terminal attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttemptMetrics {
    pub bytes_received: u64,
    pub network_bytes: u64,
    pub duration_ms: u64,
}

/// Terminal result of one attempt. `Interrupted` marks an attempt that a
/// prior process owned when the service restarted; it does not change the
/// job's durable status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptOutcome {
    Completed(AttemptMetrics),
    Failed {
        code: String,
        detail: Option<String>,
        metrics: Option<AttemptMetrics>,
    },
    Cancelled,
    Interrupted,
}

impl AttemptOutcome {
    /// Storage representation used by the SQLite registry.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Completed(_) => "completed",
            Self::Failed { .. } => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }
}

/// One durable attempt as stored in the registry.
#[derive(Clone, Debug)]
pub struct AttemptRecord {
    pub id: AttemptId,
    pub job_id: JobId,
    pub reason: AttemptReason,
    pub launch_key: LaunchKey,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub outcome: Option<AttemptOutcome>,
}
