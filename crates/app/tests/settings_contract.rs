use kdown_app::domain::{
    AttemptOutcome, AttemptReason, ConflictPolicy, DurableJobStatus, JobIntent, LaunchKey, RootId,
    SourceUrl,
};
use kdown_app::registry::{AppSettings, Registry, StartupMode};

struct TestDb {
    dir: tempfile::TempDir,
}

impl TestDb {
    async fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }

    async fn registry(&self) -> Registry {
        let registry = Registry::connect(self.dir.path().join("kdown.db"))
            .await
            .unwrap();
        registry.migrate().await.unwrap();
        registry
    }
}

struct SettingsFixture {
    _db: TestDb,
    registry: Registry,
    root_id: RootId,
}

impl SettingsFixture {
    async fn new() -> Self {
        let db = TestDb::new().await;
        let registry = db.registry().await;
        let root_dir = db.dir.path().join("downloads");
        std::fs::create_dir_all(&root_dir).unwrap();
        let root = registry
            .add_root("Downloads", &root_dir, true)
            .await
            .unwrap();
        Self {
            _db: db,
            registry,
            root_id: root.id,
        }
    }

    async fn insert_running_job(&self) -> kdown_app::domain::JobRecord {
        let intent = JobIntent {
            source: SourceUrl::parse("https://example.test/file.iso").unwrap(),
            root_id: self.root_id,
            relative_directory: None,
            filename_override: None,
            conflict_policy: ConflictPolicy::FailIfExists,
        };
        let job = self.registry.insert_job(intent).await.unwrap();
        let attempt = self
            .registry
            .begin_attempt_once(job.id, AttemptReason::Initial, LaunchKey::new())
            .await
            .unwrap();
        self.registry
            .finish_attempt(attempt.id, AttemptOutcome::Interrupted)
            .await
            .unwrap();
        self.registry
            .mark_status(job.id, DurableJobStatus::Active)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn disabling_root_with_nonterminal_job_is_rejected() {
    let fixture = SettingsFixture::new().await;
    let job = fixture.insert_running_job().await;
    let error = fixture
        .registry
        .disable_root(job.root_id)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "root_in_use");
}

#[tokio::test]
async fn first_root_defaults_when_requested() {
    let db = TestDb::new().await;
    let registry = db.registry().await;
    let root_dir = db.dir.path().join("downloads");
    std::fs::create_dir_all(&root_dir).unwrap();

    let root = registry
        .add_root("Downloads", &root_dir, true)
        .await
        .unwrap();
    let settings = registry.load_settings().await.unwrap();
    assert_eq!(settings.default_root_id, Some(root.id));
    assert!(root.enabled);
    assert_eq!(root.canonical_path, root_dir.canonicalize().unwrap());
}

#[tokio::test]
async fn nonexistent_root_path_is_rejected() {
    let db = TestDb::new().await;
    let registry = db.registry().await;
    let missing = db.dir.path().join("nope");
    let error = registry
        .add_root("Missing", &missing, false)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "root_unavailable");
}

#[tokio::test]
async fn settings_defaults_are_typed() {
    let db = TestDb::new().await;
    let registry = db.registry().await;
    let settings = registry.load_settings().await.unwrap();
    assert_eq!(settings.active_concurrency, 3);
    assert_eq!(settings.rate_limit_bytes_per_second, None);
    assert_eq!(settings.default_root_id, None);
    assert!(!settings.notifications_enabled);
    assert_eq!(settings.startup_mode, StartupMode::Manual);
}

#[tokio::test]
async fn global_limit_update_persists() {
    let db = TestDb::new().await;
    let registry = db.registry().await;

    let updated = registry
        .update_settings(AppSettings {
            active_concurrency: 5,
            rate_limit_bytes_per_second: Some(1_048_576),
            default_root_id: None,
            notifications_enabled: true,
            startup_mode: StartupMode::Manual,
        })
        .await
        .unwrap();

    assert_eq!(updated.active_concurrency, 5);
    assert_eq!(updated.rate_limit_bytes_per_second, Some(1_048_576));
    assert!(updated.notifications_enabled);

    let reloaded = registry.load_settings().await.unwrap();
    assert_eq!(reloaded.active_concurrency, 5);
    assert_eq!(reloaded.rate_limit_bytes_per_second, Some(1_048_576));
}

#[tokio::test]
async fn disabling_terminal_referenced_root_is_allowed() {
    let fixture = SettingsFixture::new().await;
    let job = fixture.insert_running_job().await;
    // Terminal completion releases the root.
    fixture
        .registry
        .mark_status(job.id, DurableJobStatus::Completed)
        .await
        .unwrap();
    let disabled = fixture.registry.disable_root(job.root_id).await.unwrap();
    assert!(!disabled.enabled);
}
