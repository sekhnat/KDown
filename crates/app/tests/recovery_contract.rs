mod support;

use support::gate_launcher::GateLauncher;

use kdown_app::domain::{
    AttemptOutcome, AttemptReason, CancelArtifactPolicy, ConflictPolicy, DesiredState,
    DurableJobStatus, JobId, JobIntent, LaunchKey, RootId, SourceUrl,
};
use kdown_app::error::AppError;
use kdown_app::registry::Registry;
use kdown_app::supervisor::{RecoveryPlanner, SupervisorHandle};

use std::time::Duration;

struct RecoveryFixture {
    _db_dir: tempfile::TempDir,
    pub registry: Registry,
    pub launcher: GateLauncher,
    pub supervisor: SupervisorHandle,
    pub startup_key: LaunchKey,
    pub job_id: JobId,
    pub paused_id: JobId,
    pub cancelled_id: JobId,
}

fn intent_for_root(root_id: RootId) -> JobIntent {
    JobIntent {
        source: SourceUrl::parse("https://example.test/file.iso").unwrap(),
        root_id,
        relative_directory: None,
        filename_override: None,
        conflict_policy: ConflictPolicy::FailIfExists,
    }
}

impl RecoveryFixture {
    async fn base() -> (
        tempfile::TempDir,
        Registry,
        RootId,
        std::path::PathBuf,
        SupervisorHandle,
        GateLauncher,
        LaunchKey,
    ) {
        let db_dir = tempfile::tempdir().unwrap();
        let registry = {
            let registry = Registry::connect(db_dir.path().join("kdown.db"))
                .await
                .unwrap();
            registry.migrate().await.unwrap();
            registry
        };
        let root_dir = db_dir.path().join("downloads");
        std::fs::create_dir_all(&root_dir).unwrap();
        let root = registry
            .add_root("Downloads", &root_dir, true)
            .await
            .unwrap();
        let launcher = GateLauncher::default();
        let policy = kdown_app::path_policy::PathPolicy::new(registry.clone());
        let supervisor = kdown_app::supervisor::spawn_supervisor(
            registry.clone(),
            policy,
            launcher.clone(),
            kdown_app::supervisor::SupervisorLimits {
                max_active: 1,
                rate_limit_bytes_per_second: None,
            },
        );
        (
            db_dir,
            registry,
            root.id,
            root_dir,
            supervisor,
            launcher,
            LaunchKey::new(),
        )
    }

    async fn with_persisted_running_job_without_attempt() -> Self {
        let (db_dir, registry, root_id, _root_dir, supervisor, launcher, startup_key) =
            Self::base().await;
        let job = registry.insert_job(intent_for_root(root_id)).await.unwrap();
        registry
            .mark_status(job.id, DurableJobStatus::Active)
            .await
            .unwrap();
        Self {
            _db_dir: db_dir,
            registry,
            launcher,
            supervisor,
            startup_key,
            job_id: job.id,
            paused_id: JobId::new(),
            cancelled_id: JobId::new(),
        }
    }

    async fn with_persisted_running_job_and_active_attempt() -> Self {
        let (db_dir, registry, root_id, _root_dir, supervisor, launcher, startup_key) =
            Self::base().await;
        let job = registry.insert_job(intent_for_root(root_id)).await.unwrap();
        registry
            .begin_attempt_once(job.id, AttemptReason::Initial, LaunchKey::new())
            .await
            .unwrap();
        registry
            .mark_status(job.id, DurableJobStatus::Active)
            .await
            .unwrap();
        Self {
            _db_dir: db_dir,
            registry,
            launcher,
            supervisor,
            startup_key,
            job_id: job.id,
            paused_id: JobId::new(),
            cancelled_id: JobId::new(),
        }
    }

    async fn with_paused_and_cancelled_jobs() -> Self {
        let (db_dir, registry, root_id, _root_dir, supervisor, launcher, startup_key) =
            Self::base().await;
        let paused = registry.insert_job(intent_for_root(root_id)).await.unwrap();
        registry
            .compare_and_set_desired(paused.id, paused.control_version, DesiredState::Paused)
            .await
            .unwrap();
        registry
            .mark_status(paused.id, DurableJobStatus::Paused)
            .await
            .unwrap();

        let cancelled = registry.insert_job(intent_for_root(root_id)).await.unwrap();
        registry
            .compare_and_set_desired(
                cancelled.id,
                cancelled.control_version,
                DesiredState::Cancelled,
            )
            .await
            .unwrap();
        registry
            .mark_status(cancelled.id, DurableJobStatus::Cancelled)
            .await
            .unwrap();

        Self {
            _db_dir: db_dir,
            registry,
            launcher,
            supervisor,
            startup_key,
            job_id: JobId::new(),
            paused_id: paused.id,
            cancelled_id: cancelled.id,
        }
    }

    async fn with_active_job() -> Self {
        let (db_dir, registry, root_id, _root_dir, supervisor, launcher, startup_key) =
            Self::base().await;
        let job = registry.insert_job(intent_for_root(root_id)).await.unwrap();
        supervisor.enqueue(job.id).await.unwrap();
        Self {
            _db_dir: db_dir,
            registry,
            launcher,
            supervisor,
            startup_key,
            job_id: job.id,
            paused_id: JobId::new(),
            cancelled_id: JobId::new(),
        }
    }

    fn recovery_planner(&self) -> RecoveryPlanner {
        RecoveryPlanner::new(
            self.registry.clone(),
            self.supervisor.clone(),
            self.startup_key,
        )
    }

    /// The startup sequence: spawn the supervisor and run recovery.
    async fn start_supervisor(
        &self,
    ) -> Result<Vec<kdown_app::supervisor::RecoveryAction>, AppError> {
        self.recovery_planner().recover_startup().await
    }

    async fn job_status(&self, job_id: JobId) -> DurableJobStatus {
        self.registry
            .load_job(job_id)
            .await
            .map(|job| job.status)
            .unwrap_or(DurableJobStatus::Failed)
    }

    async fn wait_for_launches(&self, count: u64) {
        for _ in 0..600 {
            if self.launcher.launch_count() >= count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("launcher never reached {count} launches");
    }
}

#[tokio::test]
async fn running_job_without_attempt_recovers_exactly_once() {
    let fixture = RecoveryFixture::with_persisted_running_job_without_attempt().await;
    let planner = fixture.recovery_planner();

    let (first, second) = tokio::join!(planner.recover_startup(), planner.recover_startup());
    first.unwrap();
    second.unwrap();
    fixture.wait_for_launches(1).await;

    assert_eq!(
        fixture
            .registry
            .list_attempts(fixture.job_id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(fixture.launcher.total_launches_for(fixture.job_id), 1);
}

#[tokio::test]
async fn prior_process_attempt_is_interrupted_before_one_recovery_attempt() {
    let fixture = RecoveryFixture::with_persisted_running_job_and_active_attempt().await;
    fixture.recovery_planner().recover_startup().await.unwrap();

    let attempts = fixture
        .registry
        .list_attempts(fixture.job_id)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].outcome, Some(AttemptOutcome::Interrupted));
    assert_eq!(attempts[1].reason, AttemptReason::Recovery);
}

#[tokio::test]
async fn paused_job_stays_paused_and_cancelled_job_stays_terminal() {
    let fixture = RecoveryFixture::with_paused_and_cancelled_jobs().await;
    fixture.start_supervisor().await.unwrap();

    assert_eq!(fixture.launcher.launch_count(), 0);
    assert_eq!(
        fixture.job_status(fixture.paused_id).await,
        DurableJobStatus::Paused
    );
    assert_eq!(
        fixture.job_status(fixture.cancelled_id).await,
        DurableJobStatus::Cancelled
    );
}

#[tokio::test]
async fn graceful_shutdown_preserves_partial_without_recording_user_cancel() {
    let fixture = RecoveryFixture::with_active_job().await;
    fixture.supervisor.shutdown().await.unwrap();

    assert_eq!(
        fixture.launcher.cancel_policy(),
        Some(CancelArtifactPolicy::PreservePartial)
    );
    let job = fixture.registry.load_job(fixture.job_id).await.unwrap();
    assert_eq!(job.desired_state, DesiredState::Running);
}
