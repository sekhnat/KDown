-- Configured download roots and typed global settings.
-- Roots store the canonical absolute directory; ordinary job requests never
-- carry absolute paths, only root IDs plus relative destinations.

CREATE TABLE roots (
    id             TEXT PRIMARY KEY,
    label          TEXT NOT NULL,
    canonical_path TEXT NOT NULL UNIQUE,
    enabled        INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    is_default     INTEGER NOT NULL CHECK (is_default IN (0, 1)),
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);

CREATE TABLE settings (
    id                           INTEGER PRIMARY KEY CHECK (id = 1),
    active_concurrency           INTEGER NOT NULL CHECK (active_concurrency > 0),
    rate_limit_bytes_per_second  INTEGER
        CHECK (rate_limit_bytes_per_second IS NULL OR rate_limit_bytes_per_second > 0),
    default_root_id              TEXT REFERENCES roots (id),
    notifications_enabled        INTEGER NOT NULL CHECK (notifications_enabled IN (0, 1)),
    startup_mode                 TEXT NOT NULL CHECK (startup_mode IN ('manual', 'service')),
    updated_at                   INTEGER NOT NULL
);

-- Typed defaults: concurrency 3, unlimited rate (NULL), no default root,
-- notifications disabled, manual startup.
INSERT INTO settings (id, active_concurrency, rate_limit_bytes_per_second, default_root_id,
                      notifications_enabled, startup_mode, updated_at)
VALUES (1, 3, NULL, NULL, 0, 'manual', 0);
