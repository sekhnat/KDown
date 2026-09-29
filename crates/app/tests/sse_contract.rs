mod support;

use std::sync::Arc;
use std::time::Duration;

use support::api_fixture::HOST;

use kdown_app::domain::{DurableJobStatus, JobId, RootId};
use kdown_app::events::{EventBroker, SupervisorEvent};
use kdown_app::supervisor::SupervisorHandle;

struct SseFixture {
    _dir: tempfile::TempDir,
    broker: EventBroker,
    _supervisor: SupervisorHandle,
    router: axum::Router,
    job_id: JobId,
    received: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

fn sample_seq(value: u64) -> u64 {
    value
}

impl SseFixture {
    async fn base(paused: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let registry = {
            let registry = kdown_app::registry::Registry::connect(dir.path().join("kdown.db"))
                .await
                .unwrap();
            registry.migrate().await.unwrap();
            registry
        };
        let root_dir = dir.path().join("downloads");
        std::fs::create_dir_all(&root_dir).unwrap();
        registry
            .add_root("Downloads", &root_dir, true)
            .await
            .unwrap();

        let broker = EventBroker::new(256);
        let launcher = support::gate_launcher::GateLauncher::default();
        let policy = kdown_app::path_policy::PathPolicy::new(registry.clone());
        let supervisor = kdown_app::supervisor::spawn_supervisor_with_broker(
            broker.clone(),
            registry.clone(),
            policy,
            launcher,
            kdown_app::supervisor::SupervisorLimits {
                max_active: 1,
                rate_limit_bytes_per_second: None,
            },
        );
        let state =
            kdown_app::api::AppState::new(registry, supervisor.clone()).with_events(broker.clone());
        let router = kdown_app::api::build_router(state);
        // Paused time applies only to the sampling/coalescing phase; the
        // database and supervisor must start under real time.
        if paused {
            tokio::time::pause();
        }
        Self {
            _dir: dir,
            broker,
            _supervisor: supervisor,
            router,
            job_id: JobId::new(),
            received: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    async fn new() -> Self {
        Self::base(false).await
    }

    async fn paused_time() -> Self {
        Self::base(true).await
    }

    fn job_view(&self, seq: u64, status: DurableJobStatus) -> kdown_app::domain::JobView {
        kdown_app::domain::JobView {
            id: self.job_id,
            status,
            desired_state: kdown_app::domain::DesiredState::Running,
            control_version: kdown_app::domain::ControlVersion::new(1),
            attempt_id: Some(kdown_app::domain::AttemptId::new()),
            sample_seq: seq,
            source_display: "https://example.test/file.iso?…".to_string(),
            root_id: RootId::new(),
            relative_directory: None,
            filename_override: None,
            created_at: 0,
            updated_at: 0,
            snapshot: None,
        }
    }

    /// Publishes one telemetry-style snapshot event.
    async fn publish_snapshot(&self, seq: u64) {
        self.broker.publish(SupervisorEvent::JobSnapshot(
            self.job_view(seq, DurableJobStatus::Active),
        ));
    }

    /// Publishes `count` telemetry snapshots for one job back to back.
    async fn publish_telemetry_samples(&self, count: usize) {
        for index in 0..count {
            self.publish_snapshot(sample_seq(index as u64)).await;
        }
    }

    /// Publishes a lifecycle milestone for the tracked job.
    async fn publish_milestone(&self, status: DurableJobStatus) {
        self.broker.publish(SupervisorEvent::JobSnapshot(
            self.job_view(sample_seq(99), status),
        ));
    }

    /// Simulates the client's authoritative collection fetch: the durable
    /// collection view carries the last sampled sequence the record had.
    async fn fetch_delayed_collection(&self) -> serde_json::Value {
        serde_json::json!({
            "jobs": [ { "sample_seq": sample_seq(6) } ],
            "next_cursor": null
        })
    }

    /// Connects to /api/v1/events and starts a background reader that
    /// records every `data:` payload.
    async fn connect(&self) {
        use tower::ServiceExt;
        let request = http::Request::builder()
            .method("GET")
            .uri("/api/v1/events")
            .header("host", HOST)
            .header("accept", "text/event-stream")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = self.router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "text/event-stream"
        );
        let received = Arc::clone(&self.received);
        let mut data_stream = response.into_body().into_data_stream();
        let reader = tokio::spawn(async move {
            let mut buffer: Vec<u8> = Vec::new();
            use futures_util::StreamExt;
            while let Some(chunk) = data_stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        buffer.extend_from_slice(&bytes);
                        while let Some(position) = find_event_end(&buffer) {
                            let event: Vec<u8> = buffer.drain(..position).collect();
                            if let Some(payload) = data_payload(&event) {
                                if let Ok(value) =
                                    serde_json::from_slice::<serde_json::Value>(payload)
                                {
                                    received.lock().unwrap().push(value);
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        // The reader lives as long as the fixture; keep the JoinHandle.
        std::mem::forget(reader);
    }

    /// Next JSON payload from the stream, in order.
    async fn next_json(&self) -> serde_json::Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            {
                let mut received = self.received.lock().unwrap();
                if !received.is_empty() {
                    return received.remove(0);
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("no SSE event arrived in time");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Number of job.snapshot events delivered so far.
    fn emitted_job_snapshots(&self) -> usize {
        self.received
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event["kind"] == "job.snapshot")
            .count()
    }

    /// The most recent job.snapshot event's status.
    fn last_event_status(&self) -> DurableJobStatus {
        let received = self.received.lock().unwrap();
        let last = received
            .iter()
            .rev()
            .find(|event| event["kind"] == "job.snapshot")
            .expect("milestone event must arrive");
        serde_json::from_value::<kdown_app::api::dto::DurableJobStatusDto>(
            last["job"]["status"].clone(),
        )
        .map(|dto| match dto {
            kdown_app::api::dto::DurableJobStatusDto::Queued => DurableJobStatus::Queued,
            kdown_app::api::dto::DurableJobStatusDto::Recovering => DurableJobStatus::Recovering,
            kdown_app::api::dto::DurableJobStatusDto::Active => DurableJobStatus::Active,
            kdown_app::api::dto::DurableJobStatusDto::Paused => DurableJobStatus::Paused,
            kdown_app::api::dto::DurableJobStatusDto::Completed => DurableJobStatus::Completed,
            kdown_app::api::dto::DurableJobStatusDto::Failed => DurableJobStatus::Failed,
            kdown_app::api::dto::DurableJobStatusDto::Cancelled => DurableJobStatus::Cancelled,
        })
        .unwrap()
    }
}

fn find_event_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|p| p + 4)
        .or_else(|| {
            buffer
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|p| p + 2)
        })
}

fn data_payload(event: &[u8]) -> Option<&[u8]> {
    let text = std::str::from_utf8(event).ok()?;
    for line in text.lines() {
        if let Some(data) = line.strip_prefix("data:") {
            return Some(data.trim_start().as_bytes());
        }
    }
    None
}

#[tokio::test]
async fn stream_subscribes_before_hello_and_does_not_drop_intervening_event() {
    let fixture = SseFixture::new().await;
    fixture.connect().await;
    let hello = fixture.next_json().await;
    assert_eq!(hello["kind"], "hello");
    assert!(hello["stream_epoch"].as_str().is_some());

    fixture.publish_snapshot(sample_seq(7)).await;
    let collection = fixture.fetch_delayed_collection().await;
    let event = fixture.next_json().await;

    assert_eq!(collection["jobs"][0]["sample_seq"], sample_seq(6));
    assert_eq!(event["kind"], "job.snapshot");
    assert_eq!(event["job"]["sample_seq"], sample_seq(7));
    assert_eq!(event["stream_epoch"], hello["stream_epoch"]);
}

#[tokio::test]
async fn telemetry_is_coalesced_but_milestones_are_immediate() {
    let fixture = SseFixture::paused_time().await;
    fixture.connect().await;
    let _hello = fixture.next_json().await;

    fixture.publish_telemetry_samples(20).await;
    fixture.advance(std::time::Duration::from_secs(1)).await;
    assert!(fixture.emitted_job_snapshots() <= 4);

    fixture.publish_milestone(DurableJobStatus::Completed).await;
    // The milestone must propagate without any timer advance: yield the
    // runtime until the reader observes it.
    for _ in 0..200 {
        if fixture.last_event_status() == DurableJobStatus::Completed {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("milestone did not arrive immediately");
}

impl SseFixture {
    async fn advance(&self, duration: std::time::Duration) {
        tokio::time::advance(duration).await;
    }
}
