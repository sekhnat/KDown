use axum::body::Body;
use axum::http::{Request, StatusCode};
use kdown_app::api::AppState;
use kdown_app::registry::Registry;
use tower::ServiceExt;

struct TestDb {
    _dir: tempfile::TempDir,
    registry: Registry,
}

async fn test_db() -> TestDb {
    let dir = tempfile::tempdir().unwrap();
    let registry = {
        let registry = Registry::connect(dir.path().join("kdown.db"))
            .await
            .unwrap();
        registry.migrate().await.unwrap();
        registry
    };
    TestDb {
        _dir: dir,
        registry,
    }
}

async fn fixture_router_with_assets(web_dir: std::path::PathBuf) -> axum::Router {
    let db = test_db().await;
    let supervisor = kdown_app::supervisor::spawn_supervisor(
        db.registry.clone(),
        kdown_app::path_policy::PathPolicy::new(db.registry.clone()),
        kdown_app::engine_adapter::KdownEngineLauncher::new(kdown_engine_stub_controller()),
        kdown_app::supervisor::SupervisorLimits {
            max_active: 1,
            rate_limit_bytes_per_second: None,
        },
    );
    let state = AppState::new(db.registry.clone(), supervisor);
    kdown_app::api::build_router_with_web_dir(state, Some(web_dir))
}

fn kdown_engine_stub_controller() -> kdown_engine::DownloadController {
    let config = kdown_engine::EngineConfig::default();
    let transport = kdown_engine::HttpTransport::from_config(&config)
        .expect("default transport config must be valid");
    kdown_engine::DownloadController::new(transport, config)
}

async fn get(app: axum::Router, path: &str) -> axum::response::Response {
    app.oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn known_spa_route_falls_back_but_api_and_missing_asset_do_not() {
    let dir = tempfile::tempdir().unwrap();
    let assets = dir.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(dir.path().join("index.html"), "<html>kdown</html>").unwrap();
    std::fs::write(assets.join("index-abc123.js"), "console.log(1)").unwrap();

    let app = fixture_router_with_assets(dir.path().to_path_buf()).await;
    assert_eq!(
        get(app.clone(), "/downloads/job-1").await.status(),
        StatusCode::OK
    );
    assert_eq!(
        get(app.clone(), "/api/v1/missing").await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(app.clone(), "/assets/missing.js").await.status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn hashed_assets_are_immutable_while_html_is_no_cache() {
    let dir = tempfile::tempdir().unwrap();
    let assets = dir.path().join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(dir.path().join("index.html"), "<html>kdown</html>").unwrap();
    std::fs::write(assets.join("index-abc123.js"), "console.log(1)").unwrap();

    let app = fixture_router_with_assets(dir.path().to_path_buf()).await;

    let asset = get(app.clone(), "/assets/index-abc123.js").await;
    assert_eq!(asset.status(), StatusCode::OK);
    assert_eq!(
        asset.headers()["cache-control"],
        "public, max-age=31536000, immutable"
    );

    let index = get(app.clone(), "/").await;
    assert_eq!(index.status(), StatusCode::OK);
    assert_eq!(index.headers()["cache-control"], "no-cache");
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_extensionless_spa_paths_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("assets")).unwrap();
    std::fs::write(dir.path().join("index.html"), "<html>kdown</html>").unwrap();

    let app = fixture_router_with_assets(dir.path().to_path_buf()).await;
    assert_eq!(
        get(app, "/definitely/not/a/route").await.status(),
        StatusCode::NOT_FOUND
    );
}
