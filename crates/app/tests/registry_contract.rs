use kdown_app::domain::{
    AttemptOutcome, AttemptReason, ConflictPolicy, DesiredState, DurableJobStatus, JobIntent,
    LaunchKey, RootId, SourceUrl,
};
use kdown_app::registry::Registry;

const SIGNED_URL: &str = "https://example.test/file.iso?X-Amz-Signature=abc123&part=7";

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

fn fixture_intent() -> JobIntent {
    JobIntent {
        source: SourceUrl::parse("https://example.test/file.iso").unwrap(),
        root_id: RootId::new(),
        relative_directory: None,
        filename_override: None,
        conflict_policy: ConflictPolicy::FailIfExists,
    }
}

fn fixture_intent_with_signed_url() -> JobIntent {
    JobIntent {
        source: SourceUrl::parse(SIGNED_URL).unwrap(),
        ..fixture_intent()
    }
}

fn fixture_signed_url() -> &'static str {
    SIGNED_URL
}

fn fixture_metrics() -> kdown_app::domain::AttemptMetrics {
    kdown_app::domain::AttemptMetrics {
        bytes_received: 1024,
        network_bytes: 1100,
        duration_ms: 50,
    }
}

#[tokio::test]
async fn signed_query_and_terminal_attempt_survive_reopen() {
    let db = TestDb::new().await;
    let registry = db.registry().await;
    let job = registry
        .insert_job(fixture_intent_with_signed_url())
        .await
        .unwrap();
    let attempt = registry
        .begin_attempt_once(job.id, AttemptReason::Initial, LaunchKey::new())
        .await
        .unwrap();
    registry
        .finish_attempt(attempt.id, AttemptOutcome::Completed(fixture_metrics()))
        .await
        .unwrap();
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

    let first = registry
        .compare_and_set_desired(job.id, expected, DesiredState::Paused)
        .await;
    let second = registry
        .compare_and_set_desired(job.id, expected, DesiredState::Cancelled)
        .await;

    assert!(first.is_ok());
    let conflict = second.unwrap_err().into_conflict().unwrap();
    assert_eq!(conflict.current.desired_state, DesiredState::Paused);
}

#[tokio::test]
async fn duplicate_migration_runs_succeed() {
    let db = TestDb::new().await;
    let first = db.registry().await;
    drop(first);
    let second = db.registry().await;
    assert!(second
        .load_job(kdown_app::domain::JobId::new())
        .await
        .is_err());
}
