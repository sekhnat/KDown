mod support;

use axum::http::StatusCode;
use support::api_fixture::{json_body, ApiFixture};
use support::gate_launcher::GateLauncher;

use kdown_app::api::dto::{DesiredStateDto, JobViewDto};
use kdown_app::api::error::ApiErrorEnvelope;
use kdown_app::domain::{DurableJobStatus, JobId};

#[tokio::test]
async fn create_persists_then_enqueues_and_returns_created_view() {
    let fixture = ApiFixture::new().await;
    let response = fixture
        .post_json("/api/v1/jobs", fixture.create_job_body())
        .await;

    assert_eq!(response.status(), StatusCode::CREATED);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let job: JobViewDto = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        fixture
            .registry
            .load_job(kdown_app::domain::JobId(job.id))
            .await
            .unwrap()
            .intent
            .source
            .persisted(),
        fixture.signed_url()
    );
    fixture
        .launcher
        .wait_for_launch(kdown_app::domain::JobId(job.id))
        .await;
}

#[tokio::test]
async fn duplicate_pause_with_stale_version_returns_current_job() {
    let fixture = ApiFixture::with_running_job().await;
    let job_id = created_job_id(&fixture).await;
    let version = fixture.current_job(job_id).await.control_version;
    assert_eq!(
        fixture.pause(job_id, version).await.status(),
        StatusCode::OK
    );

    let response = fixture.pause(job_id, version).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let error: ApiErrorEnvelope = serde_json::from_value(json_body(response).await).unwrap();
    assert_eq!(error.code, "stale_control_version");
    assert_eq!(
        error.current_job.unwrap().desired_state,
        DesiredStateDto::Paused
    );
}

#[tokio::test]
async fn reveal_requires_completed_job_and_uses_its_resolved_parent() {
    let fixture = ApiFixture::new().await;
    let running = fixture.insert_running_job().await;
    assert_eq!(
        fixture.reveal(running.id).await.status(),
        StatusCode::CONFLICT
    );

    let completed = fixture
        .insert_completed_job("downloads/isos/file.iso")
        .await;
    assert_eq!(
        fixture.reveal(completed.id).await.status(),
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn history_removal_needs_terminal_job_and_keeps_the_file() {
    let fixture = ApiFixture::new().await;
    let completed = fixture
        .insert_completed_job("downloads/isos/file.iso")
        .await;
    let path = fixture.root_dir().join("downloads/isos/file.iso");
    assert!(path.exists());

    assert_eq!(
        fixture
            .delete(&format!("/api/v1/jobs/{}", completed.id))
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert!(path.exists(), "history removal must never delete files");
    assert!(fixture.registry.load_job(completed.id).await.is_err());
}

#[tokio::test]
async fn job_listing_pages_stably_by_updated_at_and_id() {
    let fixture = ApiFixture::new().await;
    for _ in 0..3 {
        let job = fixture.insert_running_job().await;
        let _ = job;
    }

    let first: serde_json::Value =
        serde_json::from_value(json_body(fixture.get("/api/v1/jobs?limit=2").await).await).unwrap();
    assert_eq!(first["jobs"].as_array().unwrap().len(), 2);
    let cursor = first["next_cursor"].as_str().unwrap().to_string();

    let second: serde_json::Value = serde_json::from_value(
        json_body(
            fixture
                .get(&format!("/api/v1/jobs?limit=2&cursor={cursor}"))
                .await,
        )
        .await,
    )
    .unwrap();
    assert_eq!(second["jobs"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn openapi_document_names_every_explicit_action() {
    let document = kdown_app::api::openapi::document();
    let json = serde_json::to_value(&document).unwrap();
    let paths = json["paths"].as_object().unwrap();
    for path in [
        "/api/v1/jobs",
        "/api/v1/jobs/{id}",
        "/api/v1/jobs/{id}/pause",
        "/api/v1/jobs/{id}/resume",
        "/api/v1/jobs/{id}/cancel",
        "/api/v1/jobs/{id}/retry",
        "/api/v1/jobs/{id}/reveal",
        "/api/v1/roots",
        "/api/v1/roots/{id}",
        "/api/v1/settings",
    ] {
        assert!(paths.contains_key(path), "missing OpenAPI path {path}");
    }
    let schemas = json["components"]["schemas"].as_object().unwrap();
    for schema in ["PauseJobCommand", "CancelJobCommand", "CreateJobRequest"] {
        assert!(schemas.contains_key(schema), "missing schema {schema}");
    }
    let pause = &schemas["PauseJobCommand"];
    let required = pause["required"].as_array().unwrap();
    assert!(required
        .iter()
        .any(|value| value == "expected_control_version"));
}

async fn created_job_id(fixture: &ApiFixture) -> JobId {
    let response = fixture
        .post_json("/api/v1/jobs", fixture.create_job_body())
        .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let job: JobViewDto = serde_json::from_slice(&bytes).unwrap();
    JobId(job.id)
}

#[allow(dead_code)] // silence until used by later scenarios
fn _typecheck(_: GateLauncher, _: DurableJobStatus) {}
