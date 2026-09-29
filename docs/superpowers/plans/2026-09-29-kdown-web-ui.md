# KDown Local Web UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a durable Linux-first local KDown service and Ocean Precision web UI for creating, observing, controlling, recovering, and reviewing downloads.

**Architecture:** Add a `kdown-app` Rust crate around the existing `kdown-engine` library and a React/TypeScript SPA under `web/`. The host owns SQLite state, configured-root policy, queueing, engine handles, recovery, security, and a versioned JSON/SSE API; the browser owns disposable presentation state only.

**Tech Stack:** Rust 1.85/2021, Tokio, Axum, SQLx SQLite, Utoipa/OpenAPI, tracing, React, TypeScript, Vite, React Router, TanStack Query, Radix UI primitives, Vitest/Testing Library, Playwright.

**Spec:** `docs/superpowers/specs/2026-09-29-kdown-web-ui-design.md`

## Global Constraints

- Keep `crates/engine` library-only and free of application, database, HTTP API, and UI concerns.
- Rust workspace builds, tests, clippy, docs, and MSRV 1.85 checks MUST work without Node.js and without prebuilt frontend assets.
- The first release is Linux-first, local single-user, loopback-only, and unsupported for LAN or Internet exposure.
- The production host MUST serve the UI and API from one origin, disable CORS, validate loopback hosts, and require same-origin JSON plus a per-process CSRF token for mutations.
- Job artifacts MUST remain under enabled configured roots. Ordinary job requests use opaque root IDs and relative destinations.
- Full source URLs, including signed query strings, MUST survive persistence and recovery; embedded URL userinfo MUST be rejected, and logs/display-safe fields MUST hide query values.
- The app queues before calling the engine because engine max-active admission rejects rather than waits.
- Engine snapshots are authoritative for live transfers. SQLite is authoritative for durable intent, attempts, settings, and terminal history.
- Active-job UI snapshots are limited to 4 Hz per job except immediate lifecycle milestones.
- Browser closure MUST NOT affect downloads. Service restart MUST recover safe resumable jobs exactly once.
- History removal MUST NOT delete completed artifacts. Partial deletion requires an explicit destructive cancellation choice.
- Ocean Precision is dark-only in this release and MUST meet the accessibility requirements in the spec.
- Permanent tests MUST verify consumer-visible behavior and invariants; avoid forwarding, copied-text, mock-echo, and bare not-throw tests.

## Review Focus

1. **Signed URL queries:** persist and launch the byte-for-byte URL query while every log/display-safe representation hides query values and URL userinfo is rejected — covered in Tasks 1, 2, and 4.
2. **Root symlink swap:** re-resolve the destination immediately before launch and reject a path whose existing parent now escapes the configured root — covered in Tasks 3 and 4.
3. **Stale duplicate command:** two pause/resume/cancel requests with one `control_version` allow exactly one mutation and return the current job for the loser — covered in Tasks 2 and 7.
4. **Crash between persistence and launch:** a durable Running job with no attempt is recovered into exactly one attempt after restart — covered in Tasks 4 and 5.
5. **SSE bootstrap event gap:** events arriving after stream subscription but before collection fetch completion are buffered and applied after the authoritative replacement — covered in Tasks 8 and 9.

---

## File Structure

### Rust application

- `crates/app/src/domain.rs`: durable IDs, lifecycle enums, URL handling, command preconditions, and display-safe views.
- `crates/app/src/error.rs`: application error taxonomy independent of HTTP.
- `crates/app/src/registry.rs`: SQLx persistence and atomic compare-and-set operations.
- `crates/app/src/path_policy.rs`: configured-root canonicalization and launch-time destination validation.
- `crates/app/src/engine_adapter.rs`: concrete translation from application launches/commands to `kdown-engine`.
- `crates/app/src/supervisor/mod.rs`: generic single-owner actor for queueing, handles, sampling, and completion.
- `crates/app/src/supervisor/recovery.rs`: startup and graceful-shutdown recovery policy.
- `crates/app/src/events.rs`: bounded supervisor event broadcast and job snapshot envelope.
- `crates/app/src/api/*`: Axum security, DTOs, handlers, SSE, OpenAPI, and static assets.
- `crates/app/src/cli.rs` and `main.rs`: process configuration, startup, browser-open option, and shutdown.
- `crates/app/migrations/*`: ordered SQLite migrations.
- `crates/app/tests/*`: behavior-focused host/API contracts using temporary state and the engine HTTP fixture.

### Frontend

- `web/src/api/*`: generated contract types, fetch client, query keys, and race-free SSE reconciliation.
- `web/src/app/*`: router, shell, providers, navigation, and global error boundaries.
- `web/src/features/downloads/*`: new-download, dashboard, job cards/detail, and controls.
- `web/src/features/history/*`: paginated filters and attempt history.
- `web/src/features/settings/*`: first-run root setup, roots, limits, startup, and notifications.
- `web/src/components/*`: accessible shared primitives and Ocean Precision presentation.
- `web/src/styles/*`: tokens, reset, layout, typography, focus, responsive, and reduced-motion rules.
- `web/src/**/*.test.tsx`: focused behavioral tests.
- `web/e2e/*`: packaged real-browser acceptance scenarios.

### Delivery

- `scripts/build_app.sh`: deterministic frontend build followed by bundled Rust release build.
- `scripts/e2e_web.sh`: fixture, host, and Playwright lifecycle.
- `packaging/systemd/kdown-app.service`: optional user service unit.
- `.github/workflows/ci.yml`: separate Node/frontend/E2E job; existing Rust matrix remains Node-free.

---

### Task 1: Application Crate and Domain Contract

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/app/Cargo.toml`
- Create: `crates/app/src/lib.rs`
- Create: `crates/app/src/domain.rs`
- Create: `crates/app/src/error.rs`
- Create: `crates/app/tests/domain_contract.rs`

**Interfaces:**
- Consumes: `kdown_engine::JobState` only when constructing display views; domain persistence types remain application-owned.
- Produces: `JobId`, `AttemptId`, `RootId`, `LaunchKey`, `ControlVersion`, `SourceUrl`, `DesiredState`, `DurableJobStatus`, `CancelArtifactPolicy`, `JobIntent`, and `AppError`.

- [ ] **Step 1: Write the domain contract tests**

```rust
use kdown_app::domain::{ControlVersion, SourceUrl};

#[test]
fn signed_query_survives_while_display_is_redacted() {
    let source = SourceUrl::parse(
        "https://example.test/file.iso?X-Amz-Signature=abc123&part=7",
    )
    .unwrap();

    assert_eq!(
        source.persisted(),
        "https://example.test/file.iso?X-Amz-Signature=abc123&part=7"
    );
    assert_eq!(source.redacted(), "https://example.test/file.iso?…");
    assert!(!format!("{source:?}").contains("abc123"));
}

#[test]
fn source_url_rejects_embedded_credentials() {
    let error = SourceUrl::parse("https://user:secret@example.test/file.iso").unwrap_err();
    assert_eq!(error.code(), "source_credentials_unsupported");
}

#[test]
fn control_versions_advance_without_wrapping() {
    assert_eq!(ControlVersion::new(4).next().unwrap().get(), 5);
    assert!(ControlVersion::new(ControlVersion::MAX_JSON_INTEGER).next().is_err());
}
```

- [ ] **Step 2: Run the tests and observe the missing crate failure**

Run: `cargo test -p kdown-app --test domain_contract`

Expected: FAIL because workspace package `kdown-app` does not exist.

- [ ] **Step 3: Add the crate and implement the domain types**

Add `crates/app` to the workspace and create this initial manifest:

```toml
[package]
name = "kdown-app"
description = "Local KDown download manager service"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[lints]
workspace = true

[dependencies]
kdown-engine = { path = "../engine" }
serde = { version = "1", features = ["derive"] }
thiserror = "2"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal", "sync", "time", "fs", "process"] }
url = { version = "2", features = ["serde"] }
uuid = { version = "1", features = ["v4", "serde"] }

[dev-dependencies]
tempfile = "3"
```

Use this shape in `domain.rs`:

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JobId(uuid::Uuid);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AttemptId(uuid::Uuid);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RootId(uuid::Uuid);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LaunchKey(uuid::Uuid);

impl LaunchKey {
    pub fn new() -> Self { Self(uuid::Uuid::new_v4()) }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ControlVersion(u64);

impl ControlVersion {
    pub const MAX_JSON_INTEGER: u64 = 9_007_199_254_740_991;
    pub const fn new(value: u64) -> Self { Self(value) }
    pub const fn get(self) -> u64 { self.0 }
    pub fn next(self) -> Result<Self, crate::error::AppError> {
        if self.0 >= Self::MAX_JSON_INTEGER {
            return Err(crate::error::AppError::VersionExhausted);
        }
        Ok(Self(self.0 + 1))
    }
}

#[derive(Clone)]
pub struct SourceUrl(url::Url);

impl SourceUrl {
    pub fn parse(input: &str) -> Result<Self, crate::error::AppError> {
        let parsed = url::Url::parse(input).map_err(crate::error::AppError::InvalidSourceUrl)?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(crate::error::AppError::UnsupportedSourceScheme);
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(crate::error::AppError::SourceCredentialsUnsupported);
        }
        Ok(Self(parsed))
    }
    pub fn persisted(&self) -> &str { self.0.as_str() }
    pub fn redacted(&self) -> String {
        let mut safe = self.0.clone();
        let had_query = safe.query().is_some();
        safe.set_query(None);
        safe.set_fragment(None);
        let mut display = safe.to_string();
        if had_query { display.push_str("?…"); }
        display
    }
}

impl std::fmt::Debug for SourceUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SourceUrl").field(&self.redacted()).finish()
    }
}
```

Define the application lifecycle explicitly:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DesiredState { Running, Paused, Cancelled }

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DurableJobStatus { Queued, Recovering, Active, Paused, Completed, Failed, Cancelled }

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum CancelArtifactPolicy { PreservePartial, DeletePartial, KeepFileDiscardCheckpoint }
```

`AppError` must include invalid URL, unsupported scheme, source credentials unsupported, version exhaustion, invalid transition, not found, conflict, persistence, path policy, engine, and service-degraded variants with stable codes and safe messages.

- [ ] **Step 4: Run formatting and domain tests**

Run: `cargo fmt --all -- --check && cargo test -p kdown-app --test domain_contract`

Expected: PASS; signed query bytes survive in `persisted()`, debug output hides query values, and URL userinfo is rejected.

- [ ] **Step 5: Commit the domain foundation**

```bash
git add Cargo.toml Cargo.lock crates/app
git commit -m "feat(app): add durable download domain"
```

---

### Task 2: SQLite Registry and Atomic Versions

**Files:**
- Create: `crates/app/migrations/0001_registry.sql`
- Create: `crates/app/src/registry.rs`
- Modify: `crates/app/Cargo.toml`
- Modify: `crates/app/src/lib.rs`
- Modify: `crates/app/src/domain.rs`
- Modify: `crates/app/src/error.rs`
- Create: `crates/app/tests/registry_contract.rs`

**Interfaces:**
- Consumes: `JobId`, `AttemptId`, `LaunchKey`, `SourceUrl`, `JobIntent`, `DesiredState`, `DurableJobStatus`, `ControlVersion`.
- Produces: `Registry::connect`, `Registry::migrate`, `Registry::insert_job`, `Registry::load_job`, `Registry::list_jobs`, `Registry::compare_and_set_desired`, `Registry::mark_status`, `Registry::begin_attempt_once(job_id, reason, launch_key)`, and `Registry::finish_attempt`.

- [ ] **Step 1: Write failing persistence and conflict tests**

```rust
#[tokio::test]
async fn signed_query_and_terminal_attempt_survive_reopen() {
    let db = TestDb::new().await;
    let registry = db.registry().await;
    let job = registry.insert_job(fixture_intent_with_signed_url()).await.unwrap();
    let attempt = registry
        .begin_attempt_once(job.id, AttemptReason::Initial, LaunchKey::new())
        .await
        .unwrap();
    registry.finish_attempt(attempt.id, AttemptOutcome::Completed(fixture_metrics())).await.unwrap();
    drop(registry);

    let reopened = db.registry().await;
    let loaded = reopened.load_job(job.id).await.unwrap();
    assert_eq!(loaded.intent.source.persisted(), fixture_signed_url());
    assert_eq!(loaded.status, DurableJobStatus::Completed);
    assert_eq!(reopened.list_attempts(job.id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn stale_duplicate_command_changes_state_once() {
    let db = TestDb::new().await;
    let registry = db.registry().await;
    let job = registry.insert_job(fixture_intent()).await.unwrap();
    let expected = job.control_version;

    let first = registry.compare_and_set_desired(job.id, expected, DesiredState::Paused).await;
    let second = registry.compare_and_set_desired(job.id, expected, DesiredState::Cancelled).await;

    assert!(first.is_ok());
    let conflict = second.unwrap_err().into_conflict().unwrap();
    assert_eq!(conflict.current.desired_state, DesiredState::Paused);
}
```

- [ ] **Step 2: Run the registry tests and verify failure**

Run: `cargo test -p kdown-app --test registry_contract`

Expected: FAIL because `Registry` and migration tables are absent.

- [ ] **Step 3: Add the schema and SQLx registry**

Add the async SQLite dependency:

```toml
sqlx = { version = "0.8", default-features = false, features = ["runtime-tokio", "sqlite", "migrate"] }
```

The migration must create `jobs` and `attempts` with foreign keys, durable status/desired-state checks, unique attempt IDs, a required `launch_key`, a unique `(job_id, launch_key)` index, and an index for status/created-time pagination. Preserve the complete source URL in `source_url`; never store `SourceUrl::redacted()`.

Use compare-and-set SQL for mutations:

```rust
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
    .await?;

    if result.rows_affected() == 0 {
        let current = self.load_job(id).await?;
        return Err(AppError::Conflict { current });
    }
    self.load_job(id).await
}
```

Make `begin_attempt_once(job_id, reason, launch_key)` transactional: return the attempt already carrying that launch key, otherwise insert exactly one and update `current_attempt_id`. A retry or a new service recovery uses a new key; repeated work within one accepted command/startup reuses its key.

- [ ] **Step 4: Verify registry behavior and migration idempotence**

Run: `cargo test -p kdown-app --test registry_contract && cargo test -p kdown-app --lib`

Expected: PASS; reopening the database preserves the signed query, attempt, and terminal status; duplicate migration runs succeed.

- [ ] **Step 5: Commit the registry**

```bash
git add crates/app Cargo.lock
git commit -m "feat(app): persist jobs and attempts in sqlite"
```

---

### Task 3: Configured Roots, Settings, and Launch-Time Path Policy

**Files:**
- Create: `crates/app/migrations/0002_roots_settings.sql`
- Create: `crates/app/src/path_policy.rs`
- Modify: `crates/app/src/registry.rs`
- Modify: `crates/app/src/domain.rs`
- Modify: `crates/app/src/lib.rs`
- Create: `crates/app/tests/path_policy_contract.rs`
- Create: `crates/app/tests/settings_contract.rs`

**Interfaces:**
- Consumes: `Registry`, `RootId`, and job-relative destination data.
- Produces: `RootRecord`, `AppSettings`, `PathPolicy::add_root`, and `PathPolicy::resolve_for_launch(root, relative) -> ResolvedDestination`.

- [ ] **Step 1: Write root and symlink-swap tests**

```rust
#[tokio::test]
#[cfg(unix)]
async fn launch_recheck_rejects_parent_symlink_swapped_outside_root() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("downloads");
    let outside = temp.path().join("outside");
    std::fs::create_dir_all(root.join("safe")).unwrap();
    std::fs::create_dir_all(&outside).unwrap();

    let policy = fixture_policy_with_root(&root).await;
    assert!(policy.resolve_for_launch(root_id(), "safe/file.iso").await.is_ok());

    std::fs::remove_dir(root.join("safe")).unwrap();
    std::os::unix::fs::symlink(&outside, root.join("safe")).unwrap();

    let error = policy.resolve_for_launch(root_id(), "safe/file.iso").await.unwrap_err();
    assert_eq!(error.code(), "destination_outside_root");
}

#[tokio::test]
async fn disabling_root_with_nonterminal_job_is_rejected() {
    let fixture = SettingsFixture::new().await;
    let job = fixture.insert_running_job().await;
    let error = fixture.registry.disable_root(job.root_id).await.unwrap_err();
    assert_eq!(error.code(), "root_in_use");
}
```

- [ ] **Step 2: Run the path/settings tests and verify failure**

Run: `cargo test -p kdown-app --test path_policy_contract --test settings_contract`

Expected: FAIL because roots, settings, and `PathPolicy` do not exist.

- [ ] **Step 3: Implement roots, settings, and destination resolution**

Add `dirs = "6"` to app dependencies and use `dirs::download_dir()` only to suggest a first-run root; the user still confirms it before insertion.

Represent a validated launch destination without exposing arbitrary browser paths:

```rust
#[derive(Clone, Debug)]
pub struct ResolvedDestination {
    pub root_id: RootId,
    pub canonical_root: std::path::PathBuf,
    pub destination: std::path::PathBuf,
    pub filename_override: Option<String>,
}

impl PathPolicy {
    pub async fn resolve_for_launch(
        &self,
        root_id: RootId,
        relative: &str,
    ) -> Result<ResolvedDestination, AppError> {
        let root = self.registry.load_enabled_root(root_id).await?;
        let canonical_root = tokio::fs::canonicalize(&root.canonical_path).await?;
        let relative = validate_relative_components(relative)?;
        let destination = canonicalize_existing_parent(&canonical_root, &relative).await?;
        if !destination.starts_with(&canonical_root) {
            return Err(AppError::DestinationOutsideRoot);
        }
        Ok(ResolvedDestination { root_id, canonical_root, destination, filename_override: None })
    }
}
```

`validate_relative_components` rejects root/prefix/parent components, NUL, and empty filename overrides. `canonicalize_existing_parent` walks to the nearest existing parent, canonicalizes it, then appends only validated missing components. Root creation accepts only existing directories and stores the canonical absolute path.

Persist typed settings with defaults: active concurrency `3`, unlimited rate represented by `NULL`, default root optional, notifications disabled, and startup preference `Manual`.

- [ ] **Step 4: Run cross-platform checks**

Run: `cargo test -p kdown-app --test path_policy_contract --test settings_contract && cargo clippy -p kdown-app --all-targets -- -D warnings`

Expected: PASS. The Unix-only symlink case runs on Linux/macOS; all modules compile on Windows.

- [ ] **Step 5: Commit roots and settings**

```bash
git add crates/app
git commit -m "feat(app): enforce configured download roots"
```

---

### Task 4: Generic Supervisor and Production Engine Adapter

**Files:**
- Create: `crates/app/src/engine_adapter.rs`
- Create: `crates/app/src/events.rs`
- Create: `crates/app/src/supervisor/mod.rs`
- Create: `crates/app/src/supervisor/command.rs`
- Modify: `crates/app/src/lib.rs`
- Modify: `crates/app/src/domain.rs`
- Modify: `crates/app/src/registry.rs`
- Create: `crates/app/tests/supervisor_contract.rs`
- Create: `crates/app/tests/support/mod.rs`
- Create: `crates/app/tests/support/gate_launcher.rs`

**Interfaces:**
- Consumes: `Registry`, `PathPolicy`, `JobIntent`, `kdown_engine::{DownloadController, DownloadHandle, DownloadRequest, DirectoryDownloadRequest, CancelMode}`.
- Produces: generic `EngineLauncher`, `EngineRun`, `SupervisorHandle`, `SupervisorCommand`, `SupervisorEvent`, and concrete `KdownEngineLauncher`.

- [ ] **Step 1: Write queue, persistence-order, redaction, and path-recheck tests**

```rust
#[tokio::test]
async fn job_is_persisted_before_launcher_observes_it() {
    let fixture = SupervisorFixture::<GateLauncher>::new(1).await;
    let job = fixture.enqueue(fixture_intent()).await.unwrap();
    let observed = fixture.launcher.next_launch().await;

    assert_eq!(observed.job_id, job.id);
    assert!(fixture.registry.load_job(job.id).await.is_ok());
    fixture.launcher.complete(observed, EngineOutcome::Completed).await;
}

#[tokio::test]
async fn supervisor_queues_before_engine_admission() {
    let fixture = SupervisorFixture::<GateLauncher>::new(1).await;
    let first = fixture.enqueue(fixture_intent()).await.unwrap();
    let second = fixture.enqueue(fixture_intent()).await.unwrap();

    assert_eq!(fixture.launcher.launch_count(), 1);
    assert_eq!(fixture.registry.load_job(second.id).await.unwrap().status, DurableJobStatus::Queued);

    fixture.launcher.complete_job(first.id, EngineOutcome::Completed).await;
    fixture.launcher.wait_for_launch(second.id).await;
    assert_eq!(fixture.launcher.launch_count(), 2);
}

#[tokio::test]
async fn launch_receives_full_signed_url_but_events_are_redacted() {
    let fixture = SupervisorFixture::<GateLauncher>::new(1).await;
    let job = fixture.enqueue(fixture_intent_with_signed_url()).await.unwrap();
    let launch = fixture.launcher.next_launch().await;
    assert_eq!(launch.source.persisted(), fixture_signed_url());
    assert!(!format!("{:?}", fixture.events_for(job.id)).contains("abc123"));
}
```

Add the Task 3 symlink-swap scenario around a queued job: mutate the parent after enqueue but before the gate allows launch; assert no engine launch and durable Failed status with `destination_outside_root`.

- [ ] **Step 2: Run the supervisor tests and verify failure**

Run: `cargo test -p kdown-app --test supervisor_contract`

Expected: FAIL because launcher/supervisor types are absent.

- [ ] **Step 3: Define the generic no-trait-object launcher boundary**

```rust
pub struct EngineRun<H, C> {
    pub handle: H,
    pub completion: C,
}

pub trait EngineLauncher: Send + Sync + 'static {
    type Handle: EngineControl;
    type Completion: std::future::Future<Output = EngineOutcome> + Send + 'static;

    fn launch(
        &self,
        launch: EngineLaunch,
    ) -> impl std::future::Future<Output = Result<EngineRun<Self::Handle, Self::Completion>, AppError>> + Send;

    fn set_global_rate_limit(&self, bytes_per_second: Option<u64>) -> Result<(), AppError>;
}

pub trait EngineControl: Send + 'static {
    fn state(&self) -> EngineStateView;
    fn snapshot(&self) -> EngineSnapshotView;
    fn pause(&self) -> Result<(), AppError>;
    fn resume_now(&self) -> Result<(), AppError>;
    fn cancel_with(&self, policy: CancelArtifactPolicy) -> Result<(), AppError>;
    fn resolved_destination(&self) -> Option<std::path::PathBuf>;
}
```

`KdownEngineLauncher` owns one `DownloadController`. It constructs `DirectoryDownloadRequest::new(url, directory)` when no filename override exists and `DownloadRequest` for an explicit destination. Its concrete run is `EngineRun<DownloadHandle, JoinHandle<Result<CompletedDownload, DownloadRunError>>>`; adapt the join result into `EngineOutcome` inside the actor.

Map cancellation exactly:

```rust
match policy {
    CancelArtifactPolicy::PreservePartial => CancelMode::KeepPartial,
    CancelArtifactPolicy::DeletePartial => CancelMode::DeletePartial,
    CancelArtifactPolicy::KeepFileDiscardCheckpoint => CancelMode::KeepFileDiscardCheckpoint,
}
```

- [ ] **Step 4: Implement the single-owner supervisor actor**

`spawn_supervisor<L: EngineLauncher>` owns the `HashMap<JobId, ActiveRun<L::Handle>>`, queue, completion futures, and 250 ms sample interval. `SupervisorHandle` sends commands over bounded `mpsc` and receives replies over `oneshot`:

```rust
pub enum SupervisorCommand {
    Enqueue { job_id: JobId, reply: oneshot::Sender<Result<JobView, AppError>> },
    Pause { job_id: JobId, expected: ControlVersion, reply: oneshot::Sender<Result<JobView, AppError>> },
    Resume { job_id: JobId, expected: ControlVersion, reply: oneshot::Sender<Result<JobView, AppError>> },
    Cancel { job_id: JobId, expected: ControlVersion, policy: CancelArtifactPolicy, reply: oneshot::Sender<Result<JobView, AppError>> },
    Retry { job_id: JobId, expected: ControlVersion, reply: oneshot::Sender<Result<JobView, AppError>> },
    UpdateLimits { active_downloads: usize, bytes_per_second: Option<u64>, reply: oneshot::Sender<Result<(), AppError>> },
    AttemptFinished { job_id: JobId, attempt_id: AttemptId, outcome: EngineOutcome },
    Shutdown { reply: oneshot::Sender<Result<(), AppError>> },
}
```

For each accepted initial start or retry, generate one `LaunchKey`, call `begin_attempt_once(job_id, reason, launch_key)`, and retain that key while the command is in flight. Persist intent/status and the attempt before `PathPolicy::resolve_for_launch` and `EngineLauncher::launch`. Never hold a database transaction while awaiting filesystem or engine work. Broadcast `SupervisorEvent::JobSnapshot` through a bounded Tokio broadcast channel; snapshots contain `attempt_id`, `control_version`, and per-attempt `sample_seq`.

- [ ] **Step 5: Verify the actor and production adapter compile**

Run: `cargo test -p kdown-app --test supervisor_contract && cargo clippy -p kdown-app --all-targets -- -D warnings`

Expected: PASS; maximum concurrent launches equals the configured limit; queued jobs start only after a slot opens.

- [ ] **Step 6: Commit the supervisor**

```bash
git add crates/app
git commit -m "feat(app): supervise queued engine downloads"
```

---

### Task 5: Restart Recovery and Graceful Shutdown

**Files:**
- Create: `crates/app/src/supervisor/recovery.rs`
- Modify: `crates/app/src/supervisor/mod.rs`
- Modify: `crates/app/src/registry.rs`
- Modify: `crates/app/src/domain.rs`
- Create: `crates/app/tests/recovery_contract.rs`

**Interfaces:**
- Consumes: `Registry::list_recoverable`, `PathPolicy`, and `SupervisorHandle`.
- Produces: `Registry::begin_recovery_attempt_once(job_id, startup_key)`, `RecoveryPlanner::recover_startup()`, and `SupervisorHandle::shutdown()` with preserve-partial semantics.

- [ ] **Step 1: Write crash-window and desired-state recovery tests**

```rust
#[tokio::test]
async fn running_job_without_attempt_recovers_exactly_once() {
    let fixture = RecoveryFixture::with_persisted_running_job_without_attempt().await;
    let planner = fixture.recovery_planner();

    let (first, second) = tokio::join!(planner.recover_startup(), planner.recover_startup());
    first.unwrap();
    second.unwrap();
    fixture.wait_for_launches(1).await;

    assert_eq!(fixture.registry.list_attempts(fixture.job_id).await.unwrap().len(), 1);
    assert_eq!(fixture.launcher.total_launches_for(fixture.job_id), 1);
}

#[tokio::test]
async fn prior_process_attempt_is_interrupted_before_one_recovery_attempt() {
    let fixture = RecoveryFixture::with_persisted_running_job_and_active_attempt().await;
    fixture.recovery_planner().recover_startup().await.unwrap();

    let attempts = fixture.registry.list_attempts(fixture.job_id).await.unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].outcome, Some(AttemptOutcome::Interrupted));
    assert_eq!(attempts[1].reason, AttemptReason::Recovery);
}

#[tokio::test]
async fn paused_job_stays_paused_and_cancelled_job_stays_terminal() {
    let fixture = RecoveryFixture::with_paused_and_cancelled_jobs().await;
    fixture.start_supervisor().await.unwrap();

    assert_eq!(fixture.launcher.launch_count(), 0);
    assert_eq!(fixture.job_status(fixture.paused_id).await, DurableJobStatus::Paused);
    assert_eq!(fixture.job_status(fixture.cancelled_id).await, DurableJobStatus::Cancelled);
}

#[tokio::test]
async fn graceful_shutdown_preserves_partial_without_recording_user_cancel() {
    let fixture = RecoveryFixture::with_active_job().await;
    fixture.supervisor.shutdown().await.unwrap();

    assert_eq!(fixture.launcher.cancel_policy(), Some(CancelArtifactPolicy::PreservePartial));
    let job = fixture.registry.load_job(fixture.job_id).await.unwrap();
    assert_eq!(job.desired_state, DesiredState::Running);
}
```

- [ ] **Step 2: Run recovery tests and verify failure**

Run: `cargo test -p kdown-app --test recovery_contract`

Expected: FAIL because startup recovery and shutdown policy are absent.

- [ ] **Step 3: Implement explicit recovery classification**

```rust
pub enum RecoveryAction {
    Resume(JobId),
    KeepPaused(JobId),
    KeepTerminal(JobId),
    Fail(JobId, AppError),
}

pub fn classify(job: &JobRecord) -> RecoveryAction {
    match (job.desired_state, job.status) {
        (DesiredState::Running, DurableJobStatus::Completed | DurableJobStatus::Cancelled) => RecoveryAction::KeepTerminal(job.id),
        (DesiredState::Running, _) => RecoveryAction::Resume(job.id),
        (DesiredState::Paused, _) => RecoveryAction::KeepPaused(job.id),
        (DesiredState::Cancelled, _) => RecoveryAction::KeepTerminal(job.id),
    }
}
```

`RecoveryPlanner` owns one `startup_key: LaunchKey` generated when the process starts. For `Resume`, mark Recovering and call `begin_recovery_attempt_once(job_id, startup_key)`. That transaction returns the attempt already created by this startup, or marks a prior process's nonterminal attempt Interrupted and inserts one Recovery attempt with the startup key. Revalidate the path before enqueueing it. If validation/resume preparation fails, finish the recovery attempt as Failed and preserve the artifact.

Shutdown stops admission, sends `KeepPartial` to active handles, waits for bounded completion/persistence draining, and leaves desired Running unchanged. Repeated shutdown calls return the same successful terminal state.

- [ ] **Step 4: Run recovery and supervisor suites**

Run: `cargo test -p kdown-app --test recovery_contract --test supervisor_contract`

Expected: PASS; the crash-window job creates one attempt and one launch, never two.

- [ ] **Step 5: Commit recovery behavior**

```bash
git add crates/app
git commit -m "feat(app): recover interrupted downloads safely"
```

---

### Task 6: Secure Axum Shell, Error Contract, and OpenAPI Export

**Files:**
- Create: `crates/app/src/api/mod.rs`
- Create: `crates/app/src/api/security.rs`
- Create: `crates/app/src/api/error.rs`
- Create: `crates/app/src/api/dto.rs`
- Create: `crates/app/src/api/openapi.rs`
- Modify: `crates/app/src/lib.rs`
- Modify: `crates/app/Cargo.toml`
- Create: `crates/app/tests/api_security_contract.rs`

**Interfaces:**
- Consumes: `Registry`, `SupervisorHandle`, domain views/errors.
- Produces: `AppState`, `SecurityContext`, `ApiErrorEnvelope`, `api::build_router(AppState) -> axum::Router`, and `api::write_openapi(path)`.

- [ ] **Step 1: Write security and error-envelope tests against the real router**

```rust
#[tokio::test]
async fn mutation_requires_loopback_host_same_origin_json_and_csrf() {
    let app = fixture_router().await;
    let token = bootstrap_token(&app).await;

    assert_eq!(post_job(&app, RequestHeaders::none()).await.status(), StatusCode::FORBIDDEN);
    assert_eq!(post_job(&app, RequestHeaders::cross_origin(&token)).await.status(), StatusCode::FORBIDDEN);
    assert_eq!(post_job(&app, RequestHeaders::form(&token)).await.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(post_job(&app, RequestHeaders::same_origin_json(&token)).await.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn api_404_is_json_and_never_spa_html() {
    let response = request(fixture_router().await, "/api/v1/missing").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
    let body: ApiErrorEnvelope = json_body(response).await;
    assert_eq!(body.code, "not_found");
}
```

Also assert CSP, `Cache-Control: no-store` on bootstrap, rejected unexpected Host, and no permissive CORS header.

- [ ] **Step 2: Run the API shell tests and verify failure**

Run: `cargo test -p kdown-app --test api_security_contract`

Expected: FAIL because `api::build_router` does not exist.

- [ ] **Step 3: Implement security middleware and stable errors**

Add the HTTP/schema dependencies:

```toml
axum = { version = "0.8", features = ["json", "tokio"] }
http = "1"
rand = "0.9"
tower = { version = "0.5", features = ["util"] }
tower-http = { version = "0.6", features = ["set-header", "trace"] }
utoipa = { version = "5", features = ["uuid"] }
```

Generate one 256-bit random token per process. `/api/v1/bootstrap` returns it only on an accepted loopback Host. Mutation middleware requires:

```rust
fn mutation_is_allowed(headers: &HeaderMap, security: &SecurityContext) -> bool {
    headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()) == Some("application/json")
        && headers.get("x-kdown-csrf").and_then(|v| v.to_str().ok()) == Some(security.csrf_token())
        && headers.get(ORIGIN).and_then(|v| v.to_str().ok()) == Some(security.origin())
        && headers.get("sec-fetch-site").and_then(|v| v.to_str().ok()).is_none_or(|v| v == "same-origin")
}
```

Return this Utoipa schema for every error:

```rust
#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ApiErrorEnvelope {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_errors: Option<std::collections::BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_job: Option<JobViewDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}
```

Add CSP, `X-Content-Type-Options`, `Referrer-Policy`, and frame-denial headers. Do not log request bodies or CSRF values.

- [ ] **Step 4: Add deterministic OpenAPI export**

`api::openapi::document()` returns the Utoipa document. Add `crates/app/examples/export_openapi.rs`:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os().nth(1).ok_or("missing output path")?;
    let bytes = serde_json::to_vec_pretty(&kdown_app::api::openapi::document())?;
    std::fs::write(path, bytes)?;
    Ok(())
}
```

Running it twice must produce identical bytes.

Run: `cargo test -p kdown-app --test api_security_contract && cargo run -p kdown-app --example export_openapi -- /tmp/kdown-openapi.json`

Expected: PASS and a valid OpenAPI JSON document containing `/api/v1/bootstrap`.

- [ ] **Step 5: Commit the secure API shell**

```bash
git add crates/app Cargo.lock
git commit -m "feat(app): add secure local api shell"
```

---

### Task 7: Jobs, Roots, Settings, and Commands API

**Files:**
- Create: `crates/app/src/api/jobs.rs`
- Create: `crates/app/src/api/roots.rs`
- Create: `crates/app/src/api/settings.rs`
- Create: `crates/app/src/platform.rs`
- Modify: `crates/app/src/api/mod.rs`
- Modify: `crates/app/src/api/dto.rs`
- Modify: `crates/app/src/api/openapi.rs`
- Create: `crates/app/tests/api_jobs_contract.rs`
- Create: `crates/app/tests/api_settings_contract.rs`

**Interfaces:**
- Consumes: `Registry`, `PathPolicy`, `SupervisorHandle`, `ControlVersion`.
- Produces: `/api/v1/jobs`, `/jobs/{id}`, explicit action routes including reveal, `/roots`, and `/settings` with generated DTO schemas.

- [ ] **Step 1: Write end-to-end router tests for create and stale commands**

```rust
#[tokio::test]
async fn create_persists_then_enqueues_and_returns_created_view() {
    let fixture = ApiFixture::new().await;
    let response = fixture.post_json("/api/v1/jobs", json!({
        "source_url": fixture.signed_url(),
        "root_id": fixture.root_id(),
        "relative_directory": "isos",
        "filename_override": null,
        "conflict_policy": "resume"
    })).await;

    assert_eq!(response.status(), StatusCode::CREATED);
    let job: JobViewDto = json_body(response).await;
    assert_eq!(fixture.registry.load_job(job.id).await.unwrap().intent.source.persisted(), fixture.signed_url());
    fixture.launcher.wait_for_launch(job.id).await;
}

#[tokio::test]
async fn duplicate_pause_with_stale_version_returns_current_job() {
    let fixture = ApiFixture::with_running_job().await;
    let version = fixture.current_job().await.control_version;
    assert_eq!(fixture.pause(version).await.status(), StatusCode::OK);

    let response = fixture.pause(version).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let error: ApiErrorEnvelope = json_body(response).await;
    assert_eq!(error.code, "stale_control_version");
    assert_eq!(error.current_job.unwrap().desired_state, DesiredStateDto::Paused);
}

#[tokio::test]
async fn reveal_requires_completed_job_and_uses_its_resolved_parent() {
    let fixture = ApiFixture::with_noop_desktop_integration().await;
    let running = fixture.insert_running_job().await;
    assert_eq!(fixture.reveal(running.id).await.status(), StatusCode::CONFLICT);

    let completed = fixture.insert_completed_job("downloads/isos/file.iso").await;
    assert_eq!(fixture.reveal(completed.id).await.status(), StatusCode::NO_CONTENT);
}
```

Settings tests must cover first root creation, default root, root-in-use disable conflict, global limit update, and invalid absolute/nonexistent root.

- [ ] **Step 2: Run API resource tests and verify failure**

Run: `cargo test -p kdown-app --test api_jobs_contract --test api_settings_contract`

Expected: FAIL because resource routes are missing.

- [ ] **Step 3: Implement typed DTOs and explicit routes**

Create these mutation routes:

```text
POST   /api/v1/jobs
POST   /api/v1/jobs/{id}/pause
POST   /api/v1/jobs/{id}/resume
POST   /api/v1/jobs/{id}/cancel
POST   /api/v1/jobs/{id}/retry
POST   /api/v1/jobs/{id}/reveal
DELETE /api/v1/jobs/{id}
POST   /api/v1/roots
PATCH  /api/v1/roots/{id}
PUT    /api/v1/settings
```

Every lifecycle command body contains `expected_control_version`; cancel additionally contains `artifact_policy`. Reveal accepts only a completed job with a recorded resolved destination, then calls `DesktopIntegration::reveal_parent`, whose Linux implementation executes `xdg-open` with the parent path as one argument and never through a shell. Tests inject a no-op function pointer. `DELETE /jobs/{id}` accepts only terminal jobs and removes registry history, never filesystem paths. List jobs with stable cursor pagination ordered by `(updated_at DESC, id DESC)` and status/source/date filters.

`PUT /settings` validates and persists the values, then sends `SupervisorCommand::UpdateLimits`; the supervisor calls `EngineLauncher::set_global_rate_limit` and updates future queue admission. Lowering active concurrency below the current active count does not cancel work—it prevents another launch until the count falls below the new limit.

Map field errors to stable names (`source_url`, `root_id`, `relative_directory`, `filename_override`). Return absolute filesystem paths only from settings/root administration DTOs; ordinary job DTOs return root labels and relative/final display destinations.

- [ ] **Step 4: Regenerate and inspect the OpenAPI contract through tests**

Add an assertion that the OpenAPI document includes each explicit action and that mutation request schemas require `expected_control_version`.

Run: `cargo test -p kdown-app --test api_jobs_contract --test api_settings_contract --test api_security_contract`

Expected: PASS; stale commands return one current view and do not invoke the launcher twice.

- [ ] **Step 5: Commit API resources**

```bash
git add crates/app
git commit -m "feat(app): expose download management api"
```

---

### Task 8: Bounded SSE Stream and Revision Protocol

**Files:**
- Create: `crates/app/src/api/sse.rs`
- Modify: `crates/app/src/events.rs`
- Modify: `crates/app/src/api/mod.rs`
- Modify: `crates/app/src/api/dto.rs`
- Modify: `crates/app/src/api/openapi.rs`
- Create: `crates/app/tests/sse_contract.rs`

**Interfaces:**
- Consumes: `SupervisorHandle::subscribe()`, `SupervisorEvent`, job collection handlers.
- Produces: `/api/v1/events`, `EventEnvelopeDto`, process `stream_epoch`, and hello-first subscription semantics.

- [ ] **Step 1: Write bounded-cadence and bootstrap-gap tests**

```rust
#[tokio::test]
async fn stream_subscribes_before_hello_and_does_not_drop_intervening_event() {
    let fixture = SseFixture::new().await;
    let mut stream = fixture.connect().await;
    let hello = stream.next_json().await;
    assert_eq!(hello.kind, "hello");

    fixture.publish_snapshot(sample_seq(7)).await;
    let collection = fixture.fetch_delayed_collection().await;
    let event = stream.next_json().await;

    assert_eq!(collection.jobs[0].sample_seq, sample_seq(6));
    assert_eq!(event.job.unwrap().sample_seq, sample_seq(7));
}

#[tokio::test]
async fn telemetry_is_coalesced_but_milestones_are_immediate() {
    let fixture = SseFixture::paused_time().await;
    fixture.publish_telemetry_samples(20).await;
    fixture.advance(std::time::Duration::from_secs(1)).await;
    assert!(fixture.emitted_job_snapshots() <= 4);

    fixture.publish_milestone(DurableJobStatus::Completed).await;
    assert_eq!(fixture.last_event().status, DurableJobStatus::Completed);
}
```

- [ ] **Step 2: Run SSE tests and verify failure**

Run: `cargo test -p kdown-app --test sse_contract`

Expected: FAIL because `/api/v1/events` is absent.

- [ ] **Step 3: Implement subscribe-before-hello and bounded broadcast**

Add stream support without an unbounded adapter:

```toml
futures-util = "0.3"
tokio-stream = { version = "0.1", features = ["sync"] }
```

The handler must call `events.subscribe()` before creating the first event:

```rust
pub async fn stream(State(state): State<AppState>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let receiver = state.events.subscribe();
    let hello = EventEnvelopeDto::hello(state.stream_epoch, state.build.clone());
    let stream = futures_util::stream::once(async move { Ok(to_sse(hello)) })
        .chain(BroadcastStream::new(receiver).filter_map(map_event));
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}
```

Use a bounded broadcast channel. Lag becomes a `service.degraded`/reconcile-required event rather than silent omission. Include `stream_epoch`, `attempt_id`, `control_version`, and `sample_seq` in job snapshots. Do not keep a durable replay log.

- [ ] **Step 4: Verify stream behavior**

Run: `cargo test -p kdown-app --test sse_contract --test api_jobs_contract`

Expected: PASS; an event emitted during collection fetch remains available after the hello.

- [ ] **Step 5: Commit SSE support**

```bash
git add crates/app
git commit -m "feat(app): stream revisioned job snapshots"
```

---

### Task 9: Frontend Toolchain, Ocean Precision Shell, and Live Reconciliation

**Files:**
- Create: `web/package.json`
- Create: `web/package-lock.json`
- Create: `web/tsconfig.json`
- Create: `web/vite.config.ts`
- Create: `web/vitest.config.ts`
- Create: `web/eslint.config.js`
- Create: `web/index.html`
- Create: `web/openapi.json`
- Create: `web/src/main.tsx`
- Create: `web/src/app/App.tsx`
- Create: `web/src/app/router.tsx`
- Create: `web/src/app/AppShell.tsx`
- Create: `web/src/api/client.ts`
- Create: `web/src/api/liveSync.ts`
- Create: `web/src/api/queryKeys.ts`
- Create: `web/src/api/schema.d.ts`
- Create: `web/src/styles/tokens.css`
- Create: `web/src/styles/global.css`
- Create: `web/src/api/liveSync.test.ts`
- Create: `web/src/app/AppShell.test.tsx`

**Interfaces:**
- Consumes: generated OpenAPI document and `/api/v1/bootstrap`, `/jobs`, `/events`.
- Produces: `ApiClient`, `LiveSync`, TanStack Query keys, responsive application shell, and generated TypeScript API types.

- [ ] **Step 1: Export the OpenAPI document and write live-gap tests**

Run once to create the contract input:

```bash
cargo run -p kdown-app --example export_openapi -- web/openapi.json
```

Write the key race test:

```ts
it('buffers stream snapshots until collection replacement completes', async () => {
  const harness = createLiveSyncHarness()
  harness.open({ streamEpoch: 'epoch-a' })
  harness.receive(jobSnapshot({ attemptId: 'a1', sampleSeq: 7 }))

  await harness.resolveCollection([jobView({ attemptId: 'a1', sampleSeq: 6 })])

  expect(harness.job('job-1')?.sampleSeq).toBe(7)
  expect(harness.phase()).toBe('live')
})

it('marks data stale and disables mutations after disconnect', () => {
  const harness = createLiveSyncHarness({ connected: true })
  harness.disconnect()
  expect(harness.phase()).toBe('stale')
  expect(harness.canMutate()).toBe(false)
})
```

Add a shell test for Downloads/History/Settings landmarks and an accessible mobile navigation name.

- [ ] **Step 2: Create the npm manifest, pin dependencies, and verify tests fail**

```bash
cd web
npm init -y
npm pkg set type=module
npm pkg set private=true --json
npm pkg set scripts.dev=vite
npm pkg set 'scripts.build=npm run generate:api && tsc -b && vite build'
npm pkg set 'scripts.generate:api=openapi-typescript openapi.json -o src/api/schema.d.ts'
npm pkg set scripts.test=vitest
npm pkg set 'scripts.lint=eslint .'
npm pkg set 'scripts.e2e=playwright test'
npm install --save-exact react react-dom react-router-dom @tanstack/react-query @tanstack/react-virtual @radix-ui/react-dialog @radix-ui/react-dropdown-menu @radix-ui/react-switch @radix-ui/react-tooltip lucide-react
npm install --save-dev --save-exact typescript vite @vitejs/plugin-react vitest jsdom @types/node @types/react @types/react-dom @testing-library/react @testing-library/user-event @testing-library/jest-dom msw eslint typescript-eslint eslint-plugin-react-hooks openapi-typescript @playwright/test @axe-core/playwright
npm test -- --run
```

Expected: FAIL because app/live-sync modules are absent; exact versions are recorded in `package.json` and `package-lock.json` and both are committed.

- [ ] **Step 3: Generate API types and implement the client**

Run `npm run generate:api`, then implement `ApiClient`. It fetches bootstrap first, retains CSRF only in memory, sends `Origin` naturally from the browser, uses JSON for mutations, and parses `ApiErrorEnvelope`. It never writes CSRF or full source URLs to console output.

`LiveSync` implements this exact order: open SSE, receive `hello`, buffer subsequent events, fetch active and visible history collections, replace query caches, apply only buffered views newer by `(stream_epoch, attempt_id, sample_seq)`, then enter live mode. Disconnect marks caches stale and invalidates mutation eligibility.

- [ ] **Step 4: Implement the Ocean Precision shell**

Define semantic CSS tokens from the spec and consume them from the shell:

```css
:root {
  color-scheme: dark;
  --canvas: #07111f;
  --surface: #101f32;
  --border: #1b304a;
  --accent: #3be0c5;
  --data: #68a7ff;
  --text: #e9f2ff;
}
:focus-visible { outline: 3px solid var(--accent); outline-offset: 3px; }
@media (prefers-reduced-motion: reduce) { *, *::before, *::after { scroll-behavior: auto; transition: none !important; } }
```

Build persistent rail, compact rail, and bottom navigation breakpoints. `AppShell` renders a skip link, named navigation, connection status, error boundary, and `<main id="main-content">`.

- [ ] **Step 5: Run frontend checks**

Run: `cd web && npm run generate:api && npm run lint && npm test -- --run && npm run build`

Expected: PASS; Vite emits content-hashed assets; live-gap test ends at sample 7.

- [ ] **Step 6: Commit frontend foundation**

```bash
git add web
git commit -m "feat(web): add ocean precision application shell"
```

---

### Task 10: First-Run Setup and New Download Flow

**Files:**
- Create: `web/src/features/settings/FirstRunSetup.tsx`
- Create: `web/src/features/settings/rootQueries.ts`
- Create: `web/src/features/downloads/NewDownloadDrawer.tsx`
- Create: `web/src/features/downloads/downloadMutations.ts`
- Create: `web/src/components/FieldError.tsx`
- Modify: `web/src/app/router.tsx`
- Create: `web/src/features/settings/FirstRunSetup.test.tsx`
- Create: `web/src/features/downloads/NewDownloadDrawer.test.tsx`

**Interfaces:**
- Consumes: root/settings/job endpoints and `ApiErrorEnvelope.field_errors`.
- Produces: blocking first-run setup, root confirmation, and accessible job creation drawer.

- [ ] **Step 1: Write first-run and creation behavior tests**

```tsx
it('blocks download routes until an allowed root is confirmed', async () => {
  server.use(noRootsBootstrap())
  renderApp('/downloads')
  expect(await screen.findByRole('heading', { name: /choose a download folder/i })).toBeVisible()
  expect(screen.queryByRole('button', { name: /new download/i })).not.toBeInTheDocument()
})

it('maps server field errors and keeps the drawer open', async () => {
  server.use(createJobError({ field_errors: { source_url: 'Only HTTP and HTTPS URLs are supported.' } }))
  renderNewDownload()
  await user.type(screen.getByLabelText(/url/i), 'file:///tmp/a')
  await user.click(screen.getByRole('button', { name: /start download/i }))
  expect(await screen.findByText(/only http and https/i)).toBeVisible()
  expect(screen.getByRole('dialog')).toBeVisible()
})
```

Also test focus restoration, Escape with dirty form confirmation, selected root ID plus relative directory payload, and successful navigation to the created job.

- [ ] **Step 2: Run focused UI tests and verify failure**

Run: `cd web && npm test -- --run src/features/settings/FirstRunSetup.test.tsx src/features/downloads/NewDownloadDrawer.test.tsx`

Expected: FAIL because these surfaces are absent.

- [ ] **Step 3: Implement first-run root confirmation**

Render the host's suggested XDG Downloads path when present. Require explicit confirmation or an existing absolute path. Display server validation in an alert associated with the field. On success, invalidate bootstrap/roots/settings and route to Downloads:

```tsx
if (bootstrap.roots.length === 0) {
  return <FirstRunSetup suggestedPath={bootstrap.suggestedDownloadRoot} />
}
return <Outlet />
```

`FirstRunSetup` submits `{ label, absolute_path, make_default: true }` and associates `field_errors.absolute_path` with the input using `aria-describedby`.

- [ ] **Step 4: Implement the new-download drawer**

Use an accessible Radix dialog/sheet. Fields are URL, allowed root, optional relative directory, optional filename, and conflict behavior. Client validation only catches empty/malformed basics; the host remains authoritative:

```tsx
const mutation = useMutation({
  mutationFn: (input: CreateJobRequest) => api.createJob(input),
  onSuccess: async (job) => {
    await queryClient.invalidateQueries({ queryKey: jobKeys.lists() })
    setOpen(false)
    navigate(`/downloads/${job.id}`)
  },
})
```

Submission shows pending affordance, does not fabricate Running state, closes only after `201 Created`, and maps server field errors without discarding the form.

- [ ] **Step 5: Run tests and production build**

Run: `cd web && npm test -- --run src/features/settings/FirstRunSetup.test.tsx src/features/downloads/NewDownloadDrawer.test.tsx && npm run build`

Expected: PASS; keyboard focus returns to New Download after a cancelled drawer.

- [ ] **Step 6: Commit setup and creation flows**

```bash
git add web
git commit -m "feat(web): add first-run and new download flows"
```

---

### Task 11: Dashboard, Job Detail, and Lifecycle Controls

**Files:**
- Create: `web/src/features/downloads/DashboardPage.tsx`
- Create: `web/src/features/downloads/JobCard.tsx`
- Create: `web/src/features/downloads/JobDetailPage.tsx`
- Create: `web/src/features/downloads/JobControls.tsx`
- Create: `web/src/features/downloads/CancelDialog.tsx`
- Create: `web/src/features/downloads/RateSparkline.tsx`
- Create: `web/src/features/downloads/jobState.ts`
- Modify: `web/src/app/router.tsx`
- Create: `web/src/features/downloads/DashboardPage.test.tsx`
- Create: `web/src/features/downloads/JobControls.test.tsx`
- Create: `web/src/features/downloads/CancelDialog.test.tsx`

**Interfaces:**
- Consumes: job list/detail views, revisioned commands, and LiveSync stale/live state.
- Produces: active dashboard, detail telemetry, legal state controls, and explicit partial-artifact cancellation choice.

- [ ] **Step 1: Write lifecycle and stale-state tests**

```tsx
it.each([
  ['Running', ['Pause', 'Cancel']],
  ['Paused', ['Resume', 'Cancel']],
  ['Failed', ['Retry', 'Remove from history']],
  ['Completed', ['Reveal in folder', 'Remove from history']],
])('shows only legal controls for %s', (status, labels) => {
  renderControls(jobView({ status }))
  for (const label of labels) expect(screen.getByRole('button', { name: label })).toBeVisible()
})

it('disables all mutations while the connection is stale', () => {
  renderControls(jobView({ status: 'Running' }), { livePhase: 'stale' })
  expect(screen.getByRole('button', { name: 'Pause' })).toBeDisabled()
  expect(screen.getByText(/reconnecting/i)).toBeVisible()
})

it('defaults cancellation to preserving resumable data', async () => {
  renderCancelDialog()
  expect(screen.getByRole('radio', { name: /keep partial/i })).toBeChecked()
  expect(screen.getByRole('radio', { name: /delete partial/i })).not.toBeChecked()
})
```

- [ ] **Step 2: Run focused dashboard/control tests and verify failure**

Run: `cd web && npm test -- --run src/features/downloads/DashboardPage.test.tsx src/features/downloads/JobControls.test.tsx src/features/downloads/CancelDialog.test.tsx`

Expected: FAIL because dashboard/detail/control components are absent.

- [ ] **Step 3: Implement dashboard and state projection**

Derive legal actions from one exhaustive `jobState.ts` map, not scattered string checks:

```ts
export const legalActions: Record<JobStatus, readonly JobAction[]> = {
  Queued: ['cancel'], Recovering: ['cancel'], Created: ['cancel'], Probing: ['cancel'],
  Preparing: ['cancel'], Running: ['pause', 'cancel'], Pausing: ['cancel'], Paused: ['resume', 'cancel'],
  Resuming: ['cancel'], Verifying: ['cancel'], Committing: [], Cancelling: [],
  Cancelled: ['retry', 'remove'], Failing: [], Failed: ['retry', 'remove'], Completed: ['reveal', 'remove'],
}
```

Dashboard shows aggregate effective rate, active/queued counts, completed bytes today, active/queued cards, recent terminal jobs, and New Download. Unknown totals render indeterminate progress; unavailable ETA renders an em dash rather than zero.

- [ ] **Step 4: Implement job detail and controls**

Render lifecycle, bytes, effective/wire rate, ETA, retries, warnings, destination, protocol, connection, concurrency, and expandable redacted error detail. Keep a bounded 120-sample sparkline:

```ts
setSamples((current) => [...current, nextRateSample].slice(-120))
await api.pauseJob(job.id, { expected_control_version: job.control_version })
```

Commands submit the displayed `control_version`; conflicts replace the cached job with `current_job` and explain that state changed elsewhere.

Cancel dialog exposes Preserve partial, Delete partial, and Keep file/discard checkpoint using explicit artifact consequences. History removal wording states that the downloaded file remains. Announce lifecycle changes in a polite live region, never each telemetry tick.

- [ ] **Step 5: Run component checks**

Run: `cd web && npm test -- --run src/features/downloads && npm run lint && npm run build`

Expected: PASS; stale mode disables controls and every status has text/icon independent of color.

- [ ] **Step 6: Commit dashboard and controls**

```bash
git add web
git commit -m "feat(web): manage live download lifecycles"
```

---

### Task 12: History, Settings, Notifications, and Responsive Accessibility

**Files:**
- Create: `web/src/features/history/HistoryPage.tsx`
- Create: `web/src/features/history/HistoryFilters.tsx`
- Create: `web/src/features/settings/SettingsPage.tsx`
- Create: `web/src/features/settings/RootSettings.tsx`
- Create: `web/src/features/settings/TransferSettings.tsx`
- Create: `web/src/features/settings/NotificationSettings.tsx`
- Create: `web/src/components/VirtualJobList.tsx`
- Modify: `web/src/app/router.tsx`
- Modify: `web/src/styles/global.css`
- Create: `web/src/features/history/HistoryPage.test.tsx`
- Create: `web/src/features/settings/SettingsPage.test.tsx`
- Create: `web/src/app/responsiveAccessibility.test.tsx`

**Interfaces:**
- Consumes: paginated jobs, attempts, roots/settings endpoints, browser Notification API, and responsive shell.
- Produces: durable history/filtering, root/limit settings, foreground notifications, and narrow-layout behavior.

- [ ] **Step 1: Write pagination, root conflict, and notification tests**

```tsx
it('keeps stable filters while loading the next history cursor', async () => {
  renderHistory('?status=failed&query=iso')
  await user.click(await screen.findByRole('button', { name: /load more/i }))
  expect(lastRequest()).toMatchObject({ status: 'failed', query: 'iso', cursor: 'next-2' })
})

it('shows root-in-use conflict without removing the root', async () => {
  server.use(disableRootConflict())
  renderSettings()
  await user.click(await screen.findByRole('button', { name: /disable downloads/i }))
  expect(await screen.findByText(/active download uses this folder/i)).toBeVisible()
  expect(screen.getByText('/home/user/Downloads')).toBeVisible()
})

it('requests notification permission only from an explicit user action', async () => {
  const requestPermission = vi.spyOn(Notification, 'requestPermission')
  renderNotificationSettings()
  expect(requestPermission).not.toHaveBeenCalled()
  await user.click(screen.getByRole('button', { name: /enable notifications/i }))
  expect(requestPermission).toHaveBeenCalledOnce()
})

it('keeps the rendered history window bounded for ten thousand rows', () => {
  renderHistoryWithJobs(buildJobs(10_000))
  expect(screen.getAllByRole('row').length).toBeLessThanOrEqual(80)
})
```

- [ ] **Step 2: Run focused tests and verify failure**

Run: `cd web && npm test -- --run src/features/history src/features/settings src/app/responsiveAccessibility.test.tsx`

Expected: FAIL because history/settings surfaces are absent.

- [ ] **Step 3: Implement paginated history and bounded rendering**

Keep status/source/date filters in URL search parameters. Render completed, failed, and cancelled jobs with attempt counts and durable error summaries. Use `@tanstack/react-virtual` so 10,000 records do not become 10,000 DOM rows:

```tsx
const rowVirtualizer = useVirtualizer({ count: jobs.length, getScrollElement: () => parentRef.current, estimateSize: () => 58, overscan: 8 })
return rowVirtualizer.getVirtualItems().map((row) => <HistoryRow key={jobs[row.index].id} job={jobs[row.index]} />)
```

Retry routes back to the existing job detail with its new attempt.

- [ ] **Step 4: Implement settings and foreground notifications**

Roots accept existing absolute directories and show canonical paths only on the administration surface. Settings update active concurrency, optional global bytes/second limit, default root, browser notification preference, and manual/systemd startup guidance. Permission remains click-bound:

```ts
async function enableNotifications() {
  const permission = await Notification.requestPermission()
  await api.updateSettings({ browser_notifications: permission === 'granted' })
}
```

Emit completion/failure notifications only while a browser client is connected and permission is granted.

- [ ] **Step 5: Verify responsive and accessibility behavior**

Test 1024 px rail, 640–1023 px compact rail, and sub-640 px bottom navigation/full-screen sheets. Assert named landmarks, visible focus classes, 44 px narrow controls, dialog focus restoration, reduced-motion CSS, and progress ARIA values.

Run: `cd web && npm test -- --run && npm run lint && npm run build`

Expected: PASS with no accessibility-test violations in the rendered representative surfaces.

- [ ] **Step 6: Commit history and settings**

```bash
git add web
git commit -m "feat(web): add history and application settings"
```

---

### Task 13: Embedded Assets, CLI, Browser Open, and systemd Unit

**Files:**
- Create: `crates/app/src/api/assets.rs`
- Create: `crates/app/src/cli.rs`
- Create: `crates/app/src/main.rs`
- Modify: `crates/app/src/api/mod.rs`
- Modify: `crates/app/src/lib.rs`
- Modify: `crates/app/Cargo.toml`
- Create: `crates/app/tests/assets_contract.rs`
- Create: `crates/app/tests/cli_contract.rs`
- Create: `scripts/build_app.sh`
- Create: `packaging/systemd/kdown-app.service`

**Interfaces:**
- Consumes: API router, SQLx registry, supervisor, `web/dist`.
- Produces: `kdown-app serve`, optional `bundled-web` feature, asset fallback rules, readiness output, and optional user service metadata.

- [ ] **Step 1: Write asset-routing and CLI tests**

```rust
#[tokio::test]
async fn known_spa_route_falls_back_but_api_and_missing_asset_do_not() {
    let app = fixture_router_with_assets();
    assert_eq!(get(&app, "/downloads/job-1").await.status(), StatusCode::OK);
    assert_eq!(get(&app, "/api/v1/missing").await.status(), StatusCode::NOT_FOUND);
    assert_eq!(get(&app, "/assets/missing.js").await.status(), StatusCode::NOT_FOUND);
}

#[test]
fn serve_defaults_to_loopback_and_manual_browser_start() {
    let cli = Cli::try_parse_from(["kdown-app", "serve"]).unwrap();
    assert_eq!(cli.listen, "127.0.0.1:8734".parse().unwrap());
    assert!(!cli.open);
}
```

Also assert hashed assets use immutable caching while `index.html` and bootstrap use no-cache/no-store as appropriate.

- [ ] **Step 2: Run default Rust checks without a frontend build**

Run: `rm -rf web/dist && cargo test -p kdown-app --test assets_contract --test cli_contract`

Expected: FAIL because CLI/assets are absent, not because `web/dist` is missing.

- [ ] **Step 3: Implement default and bundled asset modes**

Add optional packaging dependencies and feature wiring:

```toml
clap = { version = "4", features = ["derive"] }
mime_guess = "2"
rust-embed = { version = "8", optional = true }

[features]
default = []
bundled-web = ["dep:rust-embed"]
```

`kdown-app` compiles by default with API/dev asset support and no Node requirement. Under `bundled-web`, derive `RustEmbed` on `../../web/dist`; without the feature, `--web-dir` serves a caller-provided directory. Route fallback accepts only extensionless known SPA paths; `/api/*` and missing `/assets/*` remain real 404s.

- [ ] **Step 4: Implement process startup and shutdown**

`kdown-app serve` defaults to the stable `127.0.0.1:8734` endpoint and accepts `--listen`, `--state-dir`, `--web-dir` when unbundled, `--open`, and repeatable initial `--root` for controlled test/first-run setup. Port `0` is accepted only when explicitly requested, as in E2E. Refuse non-loopback listen addresses. After binding and migrations, print exactly one readiness line:

```text
READY http://127.0.0.1:<port>
```

When `--open` is present, invoke `xdg-open` with the URL as one argument after readiness; never use a shell:

```rust
if cli.open {
    tokio::process::Command::new("xdg-open").arg(&ready_url).spawn()?.wait().await?;
}
tokio::signal::ctrl_c().await?;
supervisor.shutdown().await?;
```

The production signal branch also handles SIGTERM on Unix. SIGINT/SIGTERM calls `SupervisorHandle::shutdown`, drains persistence, and exits nonzero on failed shutdown.

- [ ] **Step 5: Add deterministic build script and user unit**

```bash
#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root/web"
npm ci
npm run build
cd "$repo_root"
cargo build --release -p kdown-app --features bundled-web
```

The user unit runs `kdown-app serve` without `--open`, restarts on failure, and includes `NoNewPrivileges=true`. Documentation, not package installation, tells the user how to enable it.

- [ ] **Step 6: Verify both build modes**

Run: `rm -rf web/dist && cargo check --workspace && scripts/build_app.sh && cargo test -p kdown-app --features bundled-web --test assets_contract --test cli_contract`

Expected: PASS; the first command proves Node-free workspace compilation and the bundled test serves hashed assets.

- [ ] **Step 7: Commit packaging**

```bash
git add crates/app scripts/build_app.sh packaging/systemd web/package-lock.json
git commit -m "feat(app): package the local web service"
```

---

### Task 14: Real-Browser Acceptance, CI, and User Documentation

**Files:**
- Create: `web/playwright.config.ts`
- Create: `web/e2e/downloads.spec.ts`
- Create: `web/e2e/accessibility.spec.ts`
- Create: `web/e2e/support/fixtures.ts`
- Create: `web/e2e/support/processes.ts`
- Create: `scripts/e2e_web.sh`
- Modify: `.github/workflows/ci.yml`
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Create: `docs/web-ui.md`
- Create: `docs/security-local-ui.md`

**Interfaces:**
- Consumes: packaged `kdown-app`, `kdown-engine` fixture server startup protocol (`LISTENING`, `SIZE`, `SHA256`, `READY`), Playwright, and the complete UI/API.
- Produces: executable end-to-end proof, dedicated CI job, and operating/security documentation.

- [ ] **Step 1: Write Playwright acceptance scenarios**

```ts
test('download, pause, restart service, recover, and complete', async ({ page, processes }) => {
  await page.goto(processes.kdownUrl)
  await configureRoot(page, processes.downloadRoot)
  await createDownload(page, `${processes.fixtureUrl}/throttled.bin`)
  await expect(page.getByText('Running')).toBeVisible()
  await page.getByRole('button', { name: 'Pause' }).click()
  await expect(page.getByText('Paused')).toBeVisible()
  await page.getByRole('button', { name: 'Resume' }).click()

  await processes.restartApp()
  await expect(page.getByText(/reconnecting/i)).toBeVisible()
  await expect(page.getByText('Recovering')).toBeVisible()
  await expect(page.getByText('Completed')).toBeVisible({ timeout: 60_000 })
  await processes.expectDownloadedSha256()
})
```

Add scenarios for small completion/history, preserve/delete partial cancellation, outside-root rejection, typed failure/retry, browser reload during active work, completed-history removal retaining the file, narrow layout keyboard operation, and axe accessibility audit.

- [ ] **Step 2: Create the E2E process harness and observe the initial failure**

`scripts/e2e_web.sh` builds binaries and delegates process ownership to a Playwright worker fixture:

```bash
#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$repo_root/scripts/build_app.sh"
cargo build --manifest-path "$repo_root/Cargo.toml" -p kdown-engine --bin fixture_server
export KDOWN_BIN="$repo_root/target/release/kdown-app"
export FIXTURE_BIN="$repo_root/target/debug/fixture_server"
cd "$repo_root/web"
npx playwright test
```

`web/e2e/support/processes.ts` owns temporary state/download directories and both child processes. It parses the fixture's `LISTENING`, `SIZE`, `SHA256`, and `READY` lines, starts the app with `--listen 127.0.0.1:0`, parses the app `READY` URL, and exposes:

```ts
export interface TestProcesses {
  readonly kdownUrl: string
  readonly fixtureUrl: string
  readonly downloadRoot: string
  restartApp(): Promise<void>
  expectDownloadedSha256(): Promise<void>
  stop(): Promise<void>
}
```

`web/e2e/support/fixtures.ts` extends Playwright's worker fixtures, calls `startTestProcesses()` before `use(processes)`, and calls `processes.stop()` in `finally`; `restartApp()` terminates and respawns only the app with the same state directory. This makes cleanup and restart callable from the test process instead of an unreachable shell child.

Run: `scripts/e2e_web.sh`

Expected before completing helpers: FAIL at the first unmet UI assertion; the worker fixture still terminates both child processes and deletes temporary state.

- [ ] **Step 3: Complete E2E helpers and run the actual application**

Reuse the existing engine fixture through its binary protocol; do not replace it with mocked network responses. Browser assertions must observe real host/API/SSE behavior. Verify SHA-256 on disk after completion and exact partial-file presence/absence after each cancellation policy.

Run: `scripts/build_app.sh && (cd web && npx playwright install --with-deps chromium) && scripts/e2e_web.sh`

Expected: PASS with zero uncaught page errors and no unexpected failed requests.

- [ ] **Step 4: Add a separate frontend/E2E CI job**

Extend `.github/workflows/ci.yml` with Ubuntu, Node 22, `npm ci`, API generation drift check, lint, Vitest, frontend build, bundled Rust build, Playwright Chromium install, and `scripts/e2e_web.sh`. Keep the existing correctness/durability matrix and MSRV job Node-free.

The API drift command must fail when regeneration changes tracked files:

```bash
cargo run -p kdown-app --example export_openapi -- web/openapi.json
cd web && npm run generate:api && cd ..
git diff --exit-code -- web/openapi.json web/src/api/schema.d.ts
```

- [ ] **Step 5: Write user and security documentation**

`README.md` links the app quick start. `docs/web-ui.md` covers build/run, first-run root setup, dashboard, cancellation artifact choices, recovery, systemd enablement, troubleshooting, and the dark-only/responsive UI. `docs/security-local-ui.md` states loopback-only support, no remote exposure, root policy, CSRF/same-origin controls, redaction, and filesystem trust boundary. `CHANGELOG.md` records the new host/UI without claiming macOS/Windows support.

- [ ] **Step 6: Run the full release proof**

Run:

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
cd web && npm run lint && npm test -- --run && npm run build && cd ..
scripts/build_app.sh
scripts/e2e_web.sh
```

Expected: every command PASS; real-browser run covers restart recovery; no unexpected request failure or uncaught browser error is reported.

- [ ] **Step 7: Commit acceptance and documentation**

```bash
git add .github/workflows/ci.yml web scripts/e2e_web.sh README.md CHANGELOG.md docs/web-ui.md docs/security-local-ui.md
git commit -m "test: verify the local web download manager"
```

---

## Plan Completion Check

Before declaring implementation complete, compare every acceptance criterion in the spec against the Task 14 real-browser run and the owning task's focused tests. Remove temporary databases, download artifacts, browser traces, and fixture logs. Keep generated OpenAPI/type artifacts only when `git diff --exit-code` proves they match the Rust contract.
