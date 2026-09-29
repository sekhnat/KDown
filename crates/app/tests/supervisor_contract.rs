mod support;

use std::time::Duration;

use support::gate_launcher::GateLauncher;

use kdown_app::domain::{
    AttemptOutcome, ConflictPolicy, DurableJobStatus, JobIntent, RootId, SourceUrl,
};
use kdown_app::engine_adapter::EngineOutcome;
use kdown_app::error::AppError;

const SIGNED_URL: &str = "https://example.test/file.iso?X-Amz-Signature=abc123&part=7";

struct SupervisorFixture {
    _db_dir: tempfile::TempDir,
    pub registry: kdown_app::registry::Registry,
    pub launcher: GateLauncher,
    pub handle: kdown_app::supervisor::SupervisorHandle,
    pub root_id: RootId,
    pub root_dir: std::path::PathBuf,
    events: std::sync::Arc<std::sync::Mutex<Vec<kdown_app::supervisor::SupervisorEvent>>>,
}

impl SupervisorFixture {
    async fn new(max_active: usize) -> Self {
        let db_dir = tempfile::tempdir().unwrap();
        let registry = {
            let registry = kdown_app::registry::Registry::connect(db_dir.path().join("kdown.db"))
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

        let policy = kdown_app::path_policy::PathPolicy::new(registry.clone());
        let launcher = GateLauncher::default();
        let handle = kdown_app::supervisor::spawn_supervisor(
            registry.clone(),
            policy,
            launcher.clone(),
            kdown_app::supervisor::SupervisorLimits {
                max_active,
                rate_limit_bytes_per_second: None,
            },
        );

        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let collector = {
            let events = std::sync::Arc::clone(&events);
            let mut rx = handle.subscribe();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => events.lock().unwrap().push(event),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            })
        };
        // Keep the collector handle alive without awaiting it.
        std::mem::forget(collector);

        Self {
            _db_dir: db_dir,
            registry,
            launcher,
            handle,
            root_id: root.id,
            root_dir,
            events,
        }
    }

    fn intent(&self) -> JobIntent {
        JobIntent {
            source: SourceUrl::parse("https://example.test/file.iso").unwrap(),
            root_id: self.root_id,
            relative_directory: None,
            filename_override: None,
            conflict_policy: ConflictPolicy::FailIfExists,
        }
    }

    fn intent_with_relative(&self, relative: &str) -> JobIntent {
        JobIntent {
            relative_directory: Some(relative.to_string()),
            ..self.intent()
        }
    }

    fn intent_with_signed_url(&self) -> JobIntent {
        JobIntent {
            source: SourceUrl::parse(SIGNED_URL).unwrap(),
            ..self.intent()
        }
    }

    #[allow(dead_code)] // consumed by the Task 7 API fixture
    fn signed_url(&self) -> &'static str {
        SIGNED_URL
    }

    async fn enqueue(&self, intent: JobIntent) -> Result<JobRecord, AppError> {
        let job = self.registry.insert_job(intent).await?;
        self.handle.enqueue(job.id).await?;
        Ok(job)
    }

    fn events_for(&self, job_id: JobId) -> Vec<kdown_app::domain::JobView> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                kdown_app::supervisor::SupervisorEvent::JobSnapshot(view) if view.id == job_id => {
                    Some(view.clone())
                }
                _ => None,
            })
            .collect()
    }

    async fn wait_for_status(&self, job_id: JobId, status: DurableJobStatus) {
        for _ in 0..600 {
            if let Ok(job) = self.registry.load_job(job_id).await {
                if job.status == status {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("job {job_id} never reached {status:?}");
    }
}

use kdown_app::domain::JobId;
use kdown_app::domain::JobRecord;

#[tokio::test]
async fn job_is_persisted_before_launcher_observes_it() {
    let fixture = SupervisorFixture::new(1).await;
    let job = fixture.enqueue(fixture.intent()).await.unwrap();
    let observed = fixture.launcher.next_launch().await;

    assert_eq!(observed.job_id, job.id);
    assert!(fixture.registry.load_job(job.id).await.is_ok());
    fixture
        .launcher
        .complete(observed, EngineOutcome::Completed)
        .await;
}

#[tokio::test]
async fn supervisor_queues_before_engine_admission() {
    let fixture = SupervisorFixture::new(1).await;
    let first = fixture.enqueue(fixture.intent()).await.unwrap();
    let second = fixture.enqueue(fixture.intent()).await.unwrap();

    assert_eq!(fixture.launcher.launch_count(), 1);
    assert_eq!(
        fixture.registry.load_job(second.id).await.unwrap().status,
        DurableJobStatus::Queued
    );

    fixture
        .launcher
        .complete_job(first.id, EngineOutcome::Completed)
        .await;
    fixture.launcher.wait_for_launch(second.id).await;
    assert_eq!(fixture.launcher.launch_count(), 2);
}

#[tokio::test]
async fn launch_receives_full_signed_url_but_events_are_redacted() {
    let fixture = SupervisorFixture::new(1).await;
    let job = fixture
        .enqueue(fixture.intent_with_signed_url())
        .await
        .unwrap();
    let launch = fixture.launcher.next_launch().await;
    assert_eq!(launch.source.persisted(), fixture_signed_url());
    assert!(!format!("{:?}", fixture.events_for(job.id)).contains("abc123"));
}

#[tokio::test]
async fn symlink_swap_after_enqueue_fails_launch_closed() {
    let fixture = SupervisorFixture::new(1).await;
    let first = fixture.enqueue(fixture.intent()).await.unwrap();

    // Second job targets a real subdirectory while the only slot is held.
    std::fs::create_dir_all(fixture.root_dir.join("safe")).unwrap();
    let second = fixture
        .enqueue(fixture.intent_with_relative("safe"))
        .await
        .unwrap();
    assert_eq!(
        fixture.registry.load_job(second.id).await.unwrap().status,
        DurableJobStatus::Queued
    );

    // Swap the parent to a symlink outside the root while the job is queued.
    std::fs::remove_dir(fixture.root_dir.join("safe")).unwrap();
    let outside = fixture._db_dir.path().join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, fixture.root_dir.join("safe")).unwrap();
    #[cfg(windows)]
    {
        let _ = &outside;
        panic!("symlink scenario requires unix");
    }

    fixture
        .launcher
        .complete_job(first.id, EngineOutcome::Completed)
        .await;

    fixture
        .wait_for_status(second.id, DurableJobStatus::Failed)
        .await;
    assert_eq!(fixture.launcher.total_launches_for(second.id), 0);
    let attempts = fixture.registry.list_attempts(second.id).await.unwrap();
    assert_eq!(attempts.len(), 1);
    match attempts[0].outcome.as_ref().unwrap() {
        AttemptOutcome::Failed { code, .. } => {
            assert_eq!(code, "destination_outside_root");
        }
        other => panic!("expected failed attempt, got {other:?}"),
    }
}

#[tokio::test]
async fn terminal_event_advances_past_last_active_snapshot() {
    let fixture = SupervisorFixture::new(1).await;
    let job = fixture.enqueue(fixture.intent()).await.unwrap();
    let observed = fixture.launcher.next_launch().await;

    let active_seq = {
        let mut found = None;
        for _ in 0..200 {
            found = fixture
                .events_for(job.id)
                .into_iter()
                .filter(|view| view.status == DurableJobStatus::Active && view.sample_seq > 0)
                .map(|view| view.sample_seq)
                .max();
            if found.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        found.expect("active snapshot with a positive sequence")
    };

    fixture
        .launcher
        .complete(observed, EngineOutcome::Cancelled)
        .await;
    fixture
        .wait_for_status(job.id, DurableJobStatus::Cancelled)
        .await;

    let terminal = {
        let mut found = None;
        for _ in 0..200 {
            found = fixture
                .events_for(job.id)
                .into_iter()
                .find(|view| view.status == DurableJobStatus::Cancelled);
            if found.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        found.expect("cancelled terminal snapshot")
    };

    assert!(
        terminal.sample_seq > active_seq,
        "terminal sequence {} did not advance past active sequence {active_seq}",
        terminal.sample_seq
    );
}

fn fixture_signed_url() -> &'static str {
    SIGNED_URL
}

/// A pause must keep flowing through subsequent samples: the sampler
/// broadcasts the CURRENT durable record, not the launch-time capture, so
/// the UI never flaps back to a pre-pause state.
#[tokio::test]
async fn paused_samples_carry_the_paused_durable_status() {
    let fixture = SupervisorFixture::new(1).await;
    let job = fixture.enqueue(fixture.intent()).await.unwrap();

    let pre_pause_max_seq = {
        let mut found = 0u64;
        for _ in 0..200 {
            found = fixture
                .events_for(job.id)
                .into_iter()
                .filter(|view| view.status == DurableJobStatus::Active && view.sample_seq > 0)
                .map(|view| view.sample_seq)
                .max()
                .unwrap_or(found);
            if found > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        found
    };
    assert!(pre_pause_max_seq > 0, "no active sample observed");

    let current = fixture.registry.load_job(job.id).await.unwrap();
    let view = fixture
        .handle
        .pause(job.id, current.control_version)
        .await
        .unwrap();
    assert_eq!(view.status, DurableJobStatus::Paused);

    // Give the sampler at least two 250ms ticks after the pause.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let paused_samples = fixture
        .events_for(job.id)
        .into_iter()
        .filter(|view| {
            view.status == DurableJobStatus::Paused && view.sample_seq > pre_pause_max_seq
        })
        .count();
    assert!(
        paused_samples >= 1,
        "no post-pause sample carried the Paused status (pre_pause_max_seq={pre_pause_max_seq})"
    );
}
