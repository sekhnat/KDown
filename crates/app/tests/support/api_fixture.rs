//! Shared end-to-end API fixture: a real router over a real registry and
//! supervisor with the gated launcher, plus same-origin request helpers.
#![allow(dead_code)] // each test binary uses a different subset

use axum::Router;
use serde_json::{json, Value};
use tower::ServiceExt;

use super::gate_launcher::GateLauncher;

use kdown_app::domain::{
    AttemptOutcome, AttemptReason, ConflictPolicy, DurableJobStatus, JobId, JobIntent, JobRecord,
    LaunchKey, RootId, SourceUrl,
};

pub const HOST: &str = "127.0.0.1:8734";
pub const SIGNED_URL: &str = "https://example.test/file.iso?X-Amz-Signature=abc123&part=7";

pub struct ApiFixture {
    pub _dir: tempfile::TempDir,
    pub registry: kdown_app::registry::Registry,
    pub launcher: GateLauncher,
    pub router: Router,
    pub root_id: RootId,
    root_dir: std::path::PathBuf,
    csrf: String,
}

impl ApiFixture {
    pub async fn new() -> Self {
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
                max_active: 4,
                rate_limit_bytes_per_second: None,
            },
        );
        let mut state = kdown_app::api::AppState::with_suggestion(
            registry.clone(),
            supervisor,
            Some(root_dir.to_string_lossy().into_owned()),
        );
        state.desktop = std::sync::Arc::new(kdown_app::platform::DesktopIntegration::no_op());
        let router = kdown_app::api::build_router(state);

        let csrf = {
            let response = Self::send(router.clone(), "GET", "/api/v1/bootstrap", None, None).await;
            assert_eq!(response.status(), axum::http::StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            value["csrf_token"].as_str().unwrap().to_string()
        };

        Self {
            _dir: dir,
            registry,
            launcher,
            router,
            root_id: root.id,
            root_dir,
            csrf,
        }
    }

    pub fn signed_url(&self) -> &'static str {
        SIGNED_URL
    }

    pub fn root_id(&self) -> RootId {
        self.root_id
    }

    pub fn root_dir(&self) -> &std::path::Path {
        &self.root_dir
    }

    async fn send(
        router: Router,
        method: &str,
        path: &str,
        body: Option<Value>,
        csrf: Option<&str>,
    ) -> axum::response::Response {
        use axum::body::Body;
        let mut builder = http::Request::builder()
            .method(method)
            .uri(path)
            .header("host", HOST)
            .header("origin", format!("http://{HOST}"))
            .header("content-type", "application/json");
        if let Some(csrf) = csrf {
            builder = builder.header("x-kdown-csrf", csrf);
        }
        let request = builder
            .body(Body::from(body.map(|b| b.to_string()).unwrap_or_default()))
            .unwrap();
        router.oneshot(request).await.unwrap()
    }

    pub async fn get(&self, path: &str) -> axum::response::Response {
        Self::send(self.router.clone(), "GET", path, None, None).await
    }

    pub async fn delete(&self, path: &str) -> axum::response::Response {
        Self::send(self.router.clone(), "DELETE", path, None, Some(&self.csrf)).await
    }

    pub async fn post_json(&self, path: &str, body: Value) -> axum::response::Response {
        let request = {
            use axum::body::Body;
            http::Request::builder()
                .method("POST")
                .uri(path)
                .header("host", HOST)
                .header("origin", format!("http://{HOST}"))
                .header("content-type", "application/json")
                .header("x-kdown-csrf", &self.csrf)
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        self.router.clone().oneshot(request).await.unwrap()
    }

    pub async fn patch_json(&self, path: &str, body: Value) -> axum::response::Response {
        let request = {
            use axum::body::Body;
            http::Request::builder()
                .method("PATCH")
                .uri(path)
                .header("host", HOST)
                .header("origin", format!("http://{HOST}"))
                .header("content-type", "application/json")
                .header("x-kdown-csrf", &self.csrf)
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        self.router.clone().oneshot(request).await.unwrap()
    }

    pub async fn put_json(&self, path: &str, body: Value) -> axum::response::Response {
        let request = {
            use axum::body::Body;
            http::Request::builder()
                .method("PUT")
                .uri(path)
                .header("host", HOST)
                .header("origin", format!("http://{HOST}"))
                .header("content-type", "application/json")
                .header("x-kdown-csrf", &self.csrf)
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        self.router.clone().oneshot(request).await.unwrap()
    }

    pub fn create_job_body(&self) -> Value {
        json!({
            "source_url": self.signed_url(),
            "root_id": self.root_id.0,
            "relative_directory": "isos",
            "filename_override": null,
            "conflict_policy": "resume"
        })
    }

    pub fn intent(&self) -> JobIntent {
        JobIntent {
            source: SourceUrl::parse("https://example.test/file.iso").unwrap(),
            root_id: self.root_id,
            relative_directory: None,
            filename_override: None,
            conflict_policy: ConflictPolicy::FailIfExists,
        }
    }

    /// A job with an unfinished attempt: durable status Active.
    pub async fn insert_running_job(&self) -> JobRecord {
        let job = self.registry.insert_job(self.intent()).await.unwrap();
        self.registry
            .begin_attempt_once(job.id, AttemptReason::Initial, LaunchKey::new())
            .await
            .unwrap();
        self.registry
            .mark_status(job.id, DurableJobStatus::Active)
            .await
            .unwrap()
    }

    /// A terminal job whose attempt records a resolved final path.
    pub async fn insert_completed_job(&self, relative: &str) -> JobRecord {
        let job = self.registry.insert_job(self.intent()).await.unwrap();
        let attempt = self
            .registry
            .begin_attempt_once(job.id, AttemptReason::Initial, LaunchKey::new())
            .await
            .unwrap();
        let final_path = self.root_dir.join(relative);
        if let Some(parent) = final_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&final_path, b"completed").unwrap();
        self.registry
            .finish_attempt_with_path(
                attempt.id,
                AttemptOutcome::Completed(kdown_app::domain::AttemptMetrics {
                    bytes_received: 10,
                    network_bytes: 10,
                    duration_ms: 1,
                }),
                Some(final_path),
            )
            .await
            .unwrap();
        self.registry.load_job(job.id).await.unwrap()
    }

    /// A job created through the real API flow, now launched.
    pub async fn with_running_job() -> Self {
        let fixture = Self::new().await;
        let response = fixture
            .post_json("/api/v1/jobs", fixture.create_job_body())
            .await;
        assert_eq!(response.status(), axum::http::StatusCode::CREATED);
        fixture
    }

    pub async fn current_job(&self, job_id: JobId) -> kdown_app::api::dto::JobViewDto {
        let record = self.registry.load_job(job_id).await.unwrap();
        kdown_app::api::dto::JobViewDto::from(record)
    }

    pub async fn pause(&self, job_id: JobId, version: u64) -> axum::response::Response {
        self.post_json(
            &format!("/api/v1/jobs/{job_id}/pause"),
            json!({ "expected_control_version": version }),
        )
        .await
    }

    pub async fn reveal(&self, job_id: JobId) -> axum::response::Response {
        self.post_json(&format!("/api/v1/jobs/{job_id}/reveal"), json!({}))
            .await
    }
}

pub async fn json_body(response: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
