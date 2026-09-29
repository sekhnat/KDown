mod support;

use std::path::Path;

use axum::http::StatusCode;
use serde_json::json;
use support::api_fixture::{json_body, ApiFixture};

use kdown_app::domain::{AttemptOutcome, DurableJobStatus};

#[tokio::test]
async fn first_root_creation_succeeds_with_canonical_path() {
    let fixture = ApiFixture::new().await;
    let new_dir = fixture._dir.path().join("second-root");
    std::fs::create_dir_all(&new_dir).unwrap();

    let response = fixture
        .post_json(
            "/api/v1/roots",
            json!({
                "label": "Second",
                "absolute_path": new_dir.to_string_lossy(),
                "make_default": false
            }),
        )
        .await;

    assert_eq!(response.status(), StatusCode::CREATED);
    let body = json_body(response).await;
    assert_eq!(
        body["canonical_path"],
        new_dir.canonicalize().unwrap().to_string_lossy().as_ref()
    );
}

#[tokio::test]
async fn nonexistent_or_non_directory_root_is_rejected() {
    let fixture = ApiFixture::new().await;

    let missing = fixture._dir.path().join("missing-dir");
    let response = fixture
        .post_json(
            "/api/v1/roots",
            json!({
                "label": "Missing",
                "absolute_path": missing.to_string_lossy(),
                "make_default": false
            }),
        )
        .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json_body(response).await["code"], "root_unavailable");

    // A file is not a directory root.
    let file = fixture._dir.path().join("plain.txt");
    std::fs::write(&file, b"x").unwrap();
    let response = fixture
        .post_json(
            "/api/v1/roots",
            json!({
                "label": "File",
                "absolute_path": file.to_string_lossy(),
                "make_default": false
            }),
        )
        .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn making_a_root_default_updates_settings() {
    let fixture = ApiFixture::new().await;
    let new_dir = fixture._dir.path().join("default-candidate");
    std::fs::create_dir_all(&new_dir).unwrap();

    let response = fixture
        .post_json(
            "/api/v1/roots",
            json!({
                "label": "Default candidate",
                "absolute_path": new_dir.to_string_lossy(),
                "make_default": true
            }),
        )
        .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let new_root = json_body(response).await["id"]
        .as_str()
        .unwrap()
        .to_string();

    let settings = json_body(fixture.get("/api/v1/settings").await).await;
    assert_eq!(
        settings["default_root_id"].as_str(),
        Some(new_root.as_str())
    );
}

#[tokio::test]
async fn disabling_root_in_use_is_conflicted() {
    let fixture = ApiFixture::new().await;
    let running = fixture.insert_running_job().await;

    let response = fixture
        .patch_json(
            &format!("/api/v1/roots/{}", running.root_id.0),
            json!({ "enabled": false }),
        )
        .await;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(json_body(response).await["code"], "root_in_use");
}

#[tokio::test]
async fn global_limit_update_persists_and_is_reported() {
    let fixture = ApiFixture::new().await;

    let response = fixture
        .put_json(
            "/api/v1/settings",
            json!({
                "active_concurrency": 5,
                "rate_limit_bytes_per_second": 1_048_576,
                "default_root_id": null,
                "notifications_enabled": true,
                "startup_mode": "manual"
            }),
        )
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    let persisted = json_body(fixture.get("/api/v1/settings").await).await;
    assert_eq!(persisted["active_concurrency"], 5);
    assert_eq!(persisted["rate_limit_bytes_per_second"], 1_048_576);
    assert_eq!(persisted["notifications_enabled"], true);
}

#[tokio::test]
async fn invalid_settings_values_are_rejected() {
    let fixture = ApiFixture::new().await;

    let response = fixture
        .put_json(
            "/api/v1/settings",
            json!({
                "active_concurrency": 0,
                "rate_limit_bytes_per_second": null,
                "default_root_id": null,
                "notifications_enabled": false,
                "startup_mode": "manual"
            }),
        )
        .await;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json_body(response).await["code"], "invalid_settings");
}

#[tokio::test]
async fn job_detail_includes_attempt_history() {
    let fixture = ApiFixture::new().await;
    let job = fixture.insert_running_job().await;
    let attempt_id = job.current_attempt_id.unwrap();
    fixture
        .registry
        .finish_attempt(attempt_id, AttemptOutcome::Cancelled)
        .await
        .unwrap();
    fixture
        .registry
        .mark_status(job.id, DurableJobStatus::Cancelled)
        .await
        .unwrap();

    let body = json_body(fixture.get(&format!("/api/v1/jobs/{}", job.id)).await).await;
    assert_eq!(body["job"]["status"], "Cancelled");
    assert_eq!(body["attempts"].as_array().unwrap().len(), 1);
}

#[allow(dead_code)] // path import used above
fn _uses_path(_: &Path) {}

#[tokio::test]
async fn bootstrap_includes_root_summaries() {
    let fixture = ApiFixture::new().await;
    let body = json_body(fixture.get("/api/v1/bootstrap").await).await;
    let roots = body["roots"].as_array().expect("roots array");
    assert_eq!(roots.len(), 1);
    assert!(roots[0]["id"].as_str().is_some());
    assert_eq!(roots[0]["label"], "Downloads");
    assert_eq!(roots[0]["is_default"], true);
    // Bootstrap is not the administration surface: no absolute paths.
    assert!(roots[0].get("canonical_path").is_none());
}
