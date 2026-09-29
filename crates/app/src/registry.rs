//! SQLx-backed durable registry for jobs and attempts.
//!
//! SQLite is authoritative for durable intent, attempts, settings, and
//! terminal history. Mutations that user commands depend on use atomic
//! compare-and-set updates so exactly one of several stale duplicate
//! commands wins. Telemetry samples are never written here.

use crate::domain::{
    AttemptId, AttemptMetrics, AttemptOutcome, AttemptReason, AttemptRecord, ConflictPolicy,
    ControlVersion, DesiredState, DurableJobStatus, JobId, JobIntent, JobRecord, LaunchKey, RootId,
    RootRecord, SourceUrl,
};
use crate::error::AppError;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow};
use sqlx::{Pool, Row, Sqlite};
use std::path::{Path, PathBuf};

/// Durable registry. Cheap to clone; the pool owns connections.
#[derive(Clone)]
pub struct Registry {
    pool: Pool<Sqlite>,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry").finish_non_exhaustive()
    }
}

fn db_err(error: sqlx::Error) -> AppError {
    match error {
        sqlx::Error::RowNotFound => AppError::NotFound,
        _ => AppError::Persistence,
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Largest value representable exactly as a JSON/JavaScript integer; the
/// compare-and-set guards refuse to advance past it.
const MAX_CONTROL_VERSION: i64 = 9_007_199_254_740_991;

impl Registry {
    /// Opens (creating if needed) the SQLite database at `path` in WAL mode
    /// with foreign keys enforced.
    pub async fn connect(path: impl AsRef<std::path::Path>) -> Result<Self, AppError> {
        let options = SqliteConnectOptions::new()
            .filename(path.as_ref())
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(db_err)?;
        Ok(Self { pool })
    }

    /// Applies embedded migrations. Safe to run repeatedly.
    pub async fn migrate(&self) -> Result<(), AppError> {
        sqlx::migrate!()
            .run(&self.pool)
            .await
            .map_err(|_| AppError::Persistence)?;
        Ok(())
    }

    /// Persists new durable intent; the job starts `Queued` wanting `Running`.
    pub async fn insert_job(&self, intent: JobIntent) -> Result<JobRecord, AppError> {
        let id = JobId::new();
        let now = now_millis();
        sqlx::query(
            "INSERT INTO jobs (id, source_url, root_id, relative_directory, filename_override, \
             conflict_policy, desired_state, status, control_version, current_attempt_id, \
             created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
        )
        .bind(id.to_string())
        .bind(intent.source.persisted())
        .bind(intent.root_id.to_string())
        .bind(intent.relative_directory.clone())
        .bind(intent.filename_override.clone())
        .bind(intent.conflict_policy.as_db())
        .bind(DesiredState::Running.as_db())
        .bind(DurableJobStatus::Queued.as_db())
        .bind(1i64)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        self.load_job(id).await
    }

    /// Loads one durable job record.
    pub async fn load_job(&self, id: JobId) -> Result<JobRecord, AppError> {
        let row = sqlx::query("SELECT * FROM jobs WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
        job_from_row(&row)
    }

    /// Lists durable jobs, newest first.
    pub async fn list_jobs(&self) -> Result<Vec<JobRecord>, AppError> {
        let rows = sqlx::query("SELECT * FROM jobs ORDER BY created_at DESC, id DESC")
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
        rows.iter().map(job_from_row).collect()
    }

    /// Atomically moves desired state when the caller observed `expected`.
    /// One of several stale duplicate commands wins; the losers receive a
    /// conflict carrying the current record.
    pub async fn compare_and_set_desired(
        &self,
        id: JobId,
        expected: ControlVersion,
        desired: DesiredState,
    ) -> Result<JobRecord, AppError> {
        let next = expected.next()?;
        let result = sqlx::query(
            "UPDATE jobs SET desired_state = ?, control_version = ?, updated_at = ? \
             WHERE id = ? AND control_version = ?",
        )
        .bind(desired.as_db())
        .bind(next.get() as i64)
        .bind(now_millis())
        .bind(id.to_string())
        .bind(expected.get() as i64)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        if result.rows_affected() == 0 {
            let current = self.load_job(id).await?;
            return Err(AppError::Conflict {
                current: Box::new(current),
            });
        }
        self.load_job(id).await
    }

    /// Records a durable status milestone and advances the control version.
    pub async fn mark_status(
        &self,
        id: JobId,
        status: DurableJobStatus,
    ) -> Result<JobRecord, AppError> {
        let result = sqlx::query(
            "UPDATE jobs SET status = ?, control_version = control_version + 1, updated_at = ? \
             WHERE id = ? AND control_version < ?",
        )
        .bind(status.as_db())
        .bind(now_millis())
        .bind(id.to_string())
        .bind(MAX_CONTROL_VERSION)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        if result.rows_affected() == 0 {
            // Distinguish a missing job from an exhausted version space.
            self.load_job(id).await?;
            return Err(AppError::VersionExhausted);
        }
        self.load_job(id).await
    }

    /// Begins the attempt carrying `launch_key`, or returns the attempt
    /// already carrying it. A retry or a new service recovery uses a new
    /// key; repeated work within one accepted command or startup reuses
    /// its key.
    pub async fn begin_attempt_once(
        &self,
        job_id: JobId,
        reason: AttemptReason,
        launch_key: LaunchKey,
    ) -> Result<AttemptRecord, AppError> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        if let Some(existing) = attempt_by_launch_key(&mut tx, job_id, launch_key).await? {
            return Ok(existing);
        }
        self.load_job(job_id).await?;
        let id = AttemptId::new();
        let now = now_millis();
        sqlx::query(
            "INSERT INTO attempts (id, job_id, launch_key, reason, started_at) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(job_id.to_string())
        .bind(launch_key.to_string())
        .bind(reason.as_db())
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        sqlx::query("UPDATE jobs SET current_attempt_id = ? WHERE id = ?")
            .bind(id.to_string())
            .bind(job_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        self.load_attempt(id).await
    }

    /// Records a terminal attempt result. `Completed`, `Failed`, and
    /// `Cancelled` outcomes advance the job's durable status as a milestone;
    /// `Interrupted` leaves the job untouched for recovery classification.
    pub async fn finish_attempt(
        &self,
        attempt_id: AttemptId,
        outcome: AttemptOutcome,
    ) -> Result<AttemptRecord, AppError> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        let (job_id, started_at) = {
            let row =
                sqlx::query("SELECT job_id, started_at, finished_at FROM attempts WHERE id = ?")
                    .bind(attempt_id.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(db_err)?;
            let finished: Option<i64> = row.try_get("finished_at").map_err(db_err)?;
            if finished.is_some() {
                return Err(AppError::InvalidTransition);
            }
            let job_id: String = row.try_get("job_id").map_err(db_err)?;
            let started_at: i64 = row.try_get("started_at").map_err(db_err)?;
            (job_id, started_at)
        };

        let now = now_millis();
        let (outcome_db, failure_code, failure_detail, metrics_json) = match &outcome {
            AttemptOutcome::Completed(metrics) => (
                AttemptOutcome::as_db(outcome.clone()),
                None,
                None,
                Some(metrics_json(metrics)),
            ),
            AttemptOutcome::Failed {
                code,
                detail,
                metrics,
            } => (
                AttemptOutcome::as_db(outcome.clone()),
                Some(code.clone()),
                detail.clone(),
                metrics.map(|m| metrics_json(&m)),
            ),
            other => (AttemptOutcome::as_db(other.clone()), None, None, None),
        };
        sqlx::query(
            "UPDATE attempts SET finished_at = ?, outcome = ?, failure_code = ?, \
             failure_detail = ?, metrics_json = ? WHERE id = ?",
        )
        .bind(now)
        .bind(outcome_db)
        .bind(failure_code)
        .bind(failure_detail)
        .bind(metrics_json)
        .bind(attempt_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        match &outcome {
            AttemptOutcome::Completed(_) => {
                bump_job_status(&mut tx, &job_id, DurableJobStatus::Completed, now).await?;
            }
            AttemptOutcome::Failed { .. } => {
                bump_job_status(&mut tx, &job_id, DurableJobStatus::Failed, now).await?;
            }
            AttemptOutcome::Cancelled => {
                bump_job_status(&mut tx, &job_id, DurableJobStatus::Cancelled, now).await?;
            }
            AttemptOutcome::Interrupted => {}
        }
        tx.commit().await.map_err(db_err)?;

        let _ = started_at;
        self.load_attempt(attempt_id).await
    }

    /// Loads one attempt by id.
    pub async fn load_attempt(&self, id: AttemptId) -> Result<AttemptRecord, AppError> {
        let row = sqlx::query("SELECT * FROM attempts WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
        attempt_from_row(&row)
    }

    /// Lists a job's attempts in creation order.
    pub async fn list_attempts(&self, job_id: JobId) -> Result<Vec<AttemptRecord>, AppError> {
        let rows = sqlx::query(
            "SELECT * FROM attempts WHERE job_id = ? ORDER BY started_at ASC, rowid ASC",
        )
        .bind(job_id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        rows.iter().map(attempt_from_row).collect()
    }
}

async fn attempt_by_launch_key(
    tx: &mut sqlx::SqliteConnection,
    job_id: JobId,
    launch_key: LaunchKey,
) -> Result<Option<AttemptRecord>, AppError> {
    let row = sqlx::query("SELECT * FROM attempts WHERE job_id = ? AND launch_key = ?")
        .bind(job_id.to_string())
        .bind(launch_key.to_string())
        .fetch_optional(tx)
        .await
        .map_err(db_err)?;
    match row {
        Some(row) => Ok(Some(attempt_from_row(&row)?)),
        None => Ok(None),
    }
}

async fn bump_job_status(
    tx: &mut sqlx::SqliteConnection,
    job_id: &str,
    status: DurableJobStatus,
    now: i64,
) -> Result<(), AppError> {
    sqlx::query(
        "UPDATE jobs SET status = ?, control_version = control_version + 1, updated_at = ? \
         WHERE id = ? AND control_version < ?",
    )
    .bind(status.as_db())
    .bind(now)
    .bind(job_id)
    .bind(MAX_CONTROL_VERSION)
    .execute(tx)
    .await
    .map_err(db_err)?;
    Ok(())
}

fn metrics_json(metrics: &AttemptMetrics) -> String {
    serde_json::json!({
        "bytes_received": metrics.bytes_received,
        "network_bytes": metrics.network_bytes,
        "duration_ms": metrics.duration_ms,
    })
    .to_string()
}

fn parse_metrics(json: &str) -> Option<AttemptMetrics> {
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    Some(AttemptMetrics {
        bytes_received: value.get("bytes_received")?.as_u64()?,
        network_bytes: value.get("network_bytes")?.as_u64()?,
        duration_ms: value.get("duration_ms")?.as_u64()?,
    })
}

fn uuid_db(value: &str) -> Result<uuid::Uuid, AppError> {
    uuid::Uuid::parse_str(value).map_err(|_| AppError::Persistence)
}

fn job_from_row(row: &SqliteRow) -> Result<JobRecord, AppError> {
    let id: String = row.try_get("id").map_err(db_err)?;
    let source_url: String = row.try_get("source_url").map_err(db_err)?;
    let root_id: String = row.try_get("root_id").map_err(db_err)?;
    let relative_directory: Option<String> = row.try_get("relative_directory").map_err(db_err)?;
    let filename_override: Option<String> = row.try_get("filename_override").map_err(db_err)?;
    let conflict_policy: String = row.try_get("conflict_policy").map_err(db_err)?;
    let desired_state: String = row.try_get("desired_state").map_err(db_err)?;
    let status: String = row.try_get("status").map_err(db_err)?;
    let control_version: i64 = row.try_get("control_version").map_err(db_err)?;
    let current_attempt_id: Option<String> = row.try_get("current_attempt_id").map_err(db_err)?;
    let created_at: i64 = row.try_get("created_at").map_err(db_err)?;
    let updated_at: i64 = row.try_get("updated_at").map_err(db_err)?;

    let root_id = RootId::from_uuid(uuid_db(&root_id)?);
    Ok(JobRecord {
        id: JobId::from_uuid(uuid_db(&id)?),
        root_id,
        intent: JobIntent {
            source: SourceUrl::parse(&source_url).map_err(|_| AppError::Persistence)?,
            root_id,
            relative_directory,
            filename_override,
            conflict_policy: ConflictPolicy::from_db(&conflict_policy)
                .ok_or(AppError::Persistence)?,
        },
        desired_state: DesiredState::from_db(&desired_state).ok_or(AppError::Persistence)?,
        status: DurableJobStatus::from_db(&status).ok_or(AppError::Persistence)?,
        control_version: ControlVersion::new(control_version.max(0) as u64),
        current_attempt_id: current_attempt_id
            .map(|v| uuid_db(&v).map(AttemptId::from_uuid))
            .transpose()?,
        created_at,
        updated_at,
    })
}

fn attempt_from_row(row: &SqliteRow) -> Result<AttemptRecord, AppError> {
    let id: String = row.try_get("id").map_err(db_err)?;
    let job_id: String = row.try_get("job_id").map_err(db_err)?;
    let launch_key: String = row.try_get("launch_key").map_err(db_err)?;
    let reason: String = row.try_get("reason").map_err(db_err)?;
    let started_at: i64 = row.try_get("started_at").map_err(db_err)?;
    let finished_at: Option<i64> = row.try_get("finished_at").map_err(db_err)?;
    let outcome: Option<String> = row.try_get("outcome").map_err(db_err)?;
    let failure_code: Option<String> = row.try_get("failure_code").map_err(db_err)?;
    let failure_detail: Option<String> = row.try_get("failure_detail").map_err(db_err)?;
    let metrics_json: Option<String> = row.try_get("metrics_json").map_err(db_err)?;

    let parsed_outcome = match (outcome.as_deref(), finished_at) {
        (Some(kind), Some(_)) => Some(match kind {
            "completed" => AttemptMetrics::parse_from_db(&metrics_json)
                .map(AttemptOutcome::Completed)
                .ok_or(AppError::Persistence)?,
            "failed" => AttemptOutcome::Failed {
                code: failure_code
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                detail: failure_detail.clone(),
                metrics: metrics_json.as_deref().and_then(parse_metrics),
            },
            "cancelled" => AttemptOutcome::Cancelled,
            "interrupted" => AttemptOutcome::Interrupted,
            _ => return Err(AppError::Persistence),
        }),
        (None, None) => None,
        _ => return Err(AppError::Persistence),
    };

    Ok(AttemptRecord {
        id: AttemptId::from_uuid(uuid_db(&id)?),
        job_id: JobId::from_uuid(uuid_db(&job_id)?),
        reason: AttemptReason::from_db(&reason).ok_or(AppError::Persistence)?,
        launch_key: LaunchKey::from_uuid(uuid_db(&launch_key)?),
        started_at,
        finished_at,
        outcome: parsed_outcome,
    })
}

impl AttemptMetrics {
    fn parse_from_db(json: &Option<String>) -> Option<Self> {
        json.as_deref().and_then(parse_metrics)
    }
}

/// Typed global settings. `rate_limit_bytes_per_second` is `None` when the
/// transfer rate is unlimited.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppSettings {
    pub active_concurrency: u32,
    pub rate_limit_bytes_per_second: Option<u64>,
    pub default_root_id: Option<RootId>,
    pub notifications_enabled: bool,
    pub startup_mode: StartupMode,
}

/// How the service was started; interactive manual launches may open the
/// browser while service units never do.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupMode {
    Manual,
    Service,
}

impl StartupMode {
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Service => "service",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "manual" => Some(Self::Manual),
            "service" => Some(Self::Service),
            _ => None,
        }
    }
}

fn root_from_row(row: &SqliteRow) -> Result<RootRecord, AppError> {
    let id: String = row.try_get("id").map_err(db_err)?;
    let label: String = row.try_get("label").map_err(db_err)?;
    let canonical_path: String = row.try_get("canonical_path").map_err(db_err)?;
    let enabled: i64 = row.try_get("enabled").map_err(db_err)?;
    let is_default: i64 = row.try_get("is_default").map_err(db_err)?;
    let created_at: i64 = row.try_get("created_at").map_err(db_err)?;
    let updated_at: i64 = row.try_get("updated_at").map_err(db_err)?;
    Ok(RootRecord {
        id: RootId::from_uuid(uuid_db(&id)?),
        label,
        canonical_path: PathBuf::from(canonical_path),
        enabled: enabled != 0,
        is_default: is_default != 0,
        created_at,
        updated_at,
    })
}

impl Registry {
    /// Adds an existing directory as a configured download root. The path
    /// must exist and be a directory; the stored value is its canonical
    /// absolute form. Re-adding an already-configured directory returns the
    /// existing record.
    pub async fn add_root(
        &self,
        label: &str,
        path: &Path,
        make_default: bool,
    ) -> Result<RootRecord, AppError> {
        let canonical = tokio::fs::canonicalize(path)
            .await
            .map_err(|_| AppError::RootUnavailable)?;
        let metadata = tokio::fs::metadata(&canonical)
            .await
            .map_err(|_| AppError::RootUnavailable)?;
        if !metadata.is_dir() {
            return Err(AppError::RootUnavailable);
        }
        let canonical = canonical.to_string_lossy().into_owned();

        if let Some(existing) = sqlx::query("SELECT * FROM roots WHERE canonical_path = ?")
            .bind(&canonical)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?
        {
            let record = root_from_row(&existing)?;
            if make_default {
                self.set_default_root(record.id).await?;
                return self.load_root(record.id).await;
            }
            return Ok(record);
        }

        let id = RootId::new();
        let now = now_millis();
        sqlx::query(
            "INSERT INTO roots (id, label, canonical_path, enabled, is_default, created_at, updated_at) \
             VALUES (?, ?, ?, 1, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(label)
        .bind(&canonical)
        .bind(i64::from(make_default))
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        if make_default {
            self.set_default_root(id).await?;
        }
        self.load_root(id).await
    }

    /// Loads one configured root regardless of enabled state.
    pub async fn load_root(&self, id: RootId) -> Result<RootRecord, AppError> {
        let row = sqlx::query("SELECT * FROM roots WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
        root_from_row(&row)
    }

    /// Loads a root that must be enabled for new launches.
    pub async fn load_enabled_root(&self, id: RootId) -> Result<RootRecord, AppError> {
        let root = self.load_root(id).await?;
        if !root.enabled {
            return Err(AppError::RootUnavailable);
        }
        Ok(root)
    }

    /// Lists configured roots, default first.
    pub async fn list_roots(&self) -> Result<Vec<RootRecord>, AppError> {
        let rows = sqlx::query("SELECT * FROM roots ORDER BY is_default DESC, created_at ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
        rows.iter().map(root_from_row).collect()
    }

    /// Enables a disabled root.
    pub async fn enable_root(&self, id: RootId) -> Result<RootRecord, AppError> {
        sqlx::query("UPDATE roots SET enabled = 1, updated_at = ? WHERE id = ?")
            .bind(now_millis())
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        self.load_root(id).await
    }

    /// Disables a root. Rejected while any nonterminal job references it;
    /// a disabled default root stops being the default.
    pub async fn disable_root(&self, id: RootId) -> Result<RootRecord, AppError> {
        let in_use: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM jobs WHERE root_id = ? \
             AND status NOT IN ('completed', 'failed', 'cancelled')",
        )
        .bind(id.to_string())
        .fetch_one(&self.pool)
        .await
        .map_err(db_err)?;
        if in_use > 0 {
            return Err(AppError::RootInUse);
        }
        let result = sqlx::query(
            "UPDATE roots SET enabled = 0, is_default = 0, updated_at = ? WHERE id = ?",
        )
        .bind(now_millis())
        .bind(id.to_string())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        if result.rows_affected() == 0 {
            return Err(AppError::NotFound);
        }
        sqlx::query("UPDATE settings SET default_root_id = NULL WHERE default_root_id = ?")
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        self.load_root(id).await
    }

    /// Records the default root.
    pub async fn set_default_root(&self, id: RootId) -> Result<(), AppError> {
        self.load_root(id).await?;
        sqlx::query("UPDATE settings SET default_root_id = ?, updated_at = ? WHERE id = 1")
            .bind(id.to_string())
            .bind(now_millis())
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    /// Loads the typed global settings.
    pub async fn load_settings(&self) -> Result<AppSettings, AppError> {
        let row = sqlx::query(
            "SELECT active_concurrency, rate_limit_bytes_per_second, default_root_id, \
             notifications_enabled, startup_mode FROM settings WHERE id = 1",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db_err)?;
        let active_concurrency: i64 = row.try_get("active_concurrency").map_err(db_err)?;
        let rate_limit: Option<i64> = row.try_get("rate_limit_bytes_per_second").map_err(db_err)?;
        let default_root_id: Option<String> = row.try_get("default_root_id").map_err(db_err)?;
        let notifications_enabled: i64 = row.try_get("notifications_enabled").map_err(db_err)?;
        let startup_mode: String = row.try_get("startup_mode").map_err(db_err)?;
        Ok(AppSettings {
            active_concurrency: active_concurrency.max(0) as u32,
            rate_limit_bytes_per_second: rate_limit.map(|v| v.max(0) as u64),
            default_root_id: default_root_id
                .map(|v| uuid_db(&v).map(RootId::from_uuid))
                .transpose()?,
            notifications_enabled: notifications_enabled != 0,
            startup_mode: StartupMode::from_db(&startup_mode).ok_or(AppError::Persistence)?,
        })
    }

    /// Persists typed global settings after validation.
    pub async fn update_settings(&self, settings: AppSettings) -> Result<AppSettings, AppError> {
        if settings.active_concurrency == 0 {
            return Err(AppError::InvalidSettings);
        }
        if settings.rate_limit_bytes_per_second.is_some_and(|v| v == 0) {
            return Err(AppError::InvalidSettings);
        }
        if let Some(root) = settings.default_root_id {
            self.load_root(root).await?;
        }
        sqlx::query(
            "UPDATE settings SET active_concurrency = ?, rate_limit_bytes_per_second = ?, \
             default_root_id = ?, notifications_enabled = ?, startup_mode = ?, updated_at = ? \
             WHERE id = 1",
        )
        .bind(i64::from(settings.active_concurrency))
        .bind(settings.rate_limit_bytes_per_second.map(|v| v as i64))
        .bind(settings.default_root_id.map(|v| v.to_string()))
        .bind(i64::from(settings.notifications_enabled))
        .bind(settings.startup_mode.as_db())
        .bind(now_millis())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        self.load_settings().await
    }
}
