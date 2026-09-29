-- KDown registry: durable jobs and attempts.
-- The complete source URL (including signed query strings) is persisted in
-- source_url; redaction happens only in display/log views.

PRAGMA foreign_keys = ON;

CREATE TABLE jobs (
    id                 TEXT PRIMARY KEY,
    source_url         TEXT NOT NULL,
    root_id            TEXT NOT NULL,
    relative_directory TEXT,
    filename_override  TEXT,
    conflict_policy    TEXT NOT NULL
        CHECK (conflict_policy IN ('fail_if_exists', 'overwrite', 'rename', 'resume')),
    desired_state      TEXT NOT NULL
        CHECK (desired_state IN ('running', 'paused', 'cancelled')),
    status             TEXT NOT NULL
        CHECK (status IN ('queued', 'recovering', 'active', 'paused', 'completed', 'failed', 'cancelled')),
    control_version    INTEGER NOT NULL CHECK (control_version >= 0),
    current_attempt_id TEXT,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL
);

CREATE TABLE attempts (
    id            TEXT PRIMARY KEY,
    job_id        TEXT NOT NULL REFERENCES jobs (id),
    launch_key    TEXT NOT NULL,
    reason        TEXT NOT NULL CHECK (reason IN ('initial', 'retry', 'recovery')),
    started_at    INTEGER NOT NULL,
    finished_at   INTEGER,
    outcome       TEXT CHECK (outcome IN ('completed', 'failed', 'cancelled', 'interrupted')),
    failure_code  TEXT,
    failure_detail TEXT,
    metrics_json  TEXT,
    CHECK ((outcome IS NULL) = (finished_at IS NULL))
);

-- One attempt per launch key: retries and recovery use fresh keys, while
-- repeated work within one accepted command or startup reuses its key.
CREATE UNIQUE INDEX idx_attempts_job_launch ON attempts (job_id, launch_key);

-- Status/created-time pagination for the jobs collection and history.
CREATE INDEX idx_jobs_status_created ON jobs (status, created_at DESC, id DESC);
CREATE INDEX idx_jobs_updated ON jobs (updated_at DESC, id DESC);
