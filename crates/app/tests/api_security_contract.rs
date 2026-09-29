mod support;

use std::collections::HashMap;

use support::gate_launcher::GateLauncher;

use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::Router;
use serde_json::json;
use tower::ServiceExt; // `oneshot`

use kdown_app::api::error::ApiErrorEnvelope;

const HOST: &str = "127.0.0.1:8734";

struct ApiFixture {
    router: Router,
    _dir: tempfile::TempDir,
    root_id: uuid::Uuid,
}

impl ApiFixture {
    async fn new() -> Self {
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
            launcher,
            kdown_app::supervisor::SupervisorLimits {
                max_active: 2,
                rate_limit_bytes_per_second: None,
            },
        );
        let state = kdown_app::api::AppState::new(registry, supervisor);
        Self {
            router: kdown_app::api::build_router(state),
            _dir: dir,
            root_id: root.id.0,
        }
    }

    fn router(&self) -> Router {
        self.router.clone()
    }
}

/// Header sets the security contract distinguishes.
struct RequestHeaders {
    map: HashMap<&'static str, String>,
}

impl RequestHeaders {
    fn none() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    fn cross_origin(token: &str) -> Self {
        Self {
            map: HashMap::from([
                ("host", HOST.to_string()),
                ("origin", "http://evil.example:8734".to_string()),
                ("content-type", "application/json".to_string()),
                ("x-kdown-csrf", token.to_string()),
            ]),
        }
    }

    fn form(token: &str) -> Self {
        Self {
            map: HashMap::from([
                ("host", HOST.to_string()),
                ("origin", format!("http://{HOST}")),
                (
                    "content-type",
                    "application/x-www-form-urlencoded".to_string(),
                ),
                ("x-kdown-csrf", token.to_string()),
            ]),
        }
    }

    fn same_origin_json(token: &str) -> Self {
        Self {
            map: HashMap::from([
                ("host", HOST.to_string()),
                ("origin", format!("http://{HOST}")),
                ("content-type", "application/json".to_string()),
                ("x-kdown-csrf", token.to_string()),
            ]),
        }
    }

    fn into_header_map(self) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in self.map {
            map.insert(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(&value).unwrap(),
            );
        }
        map
    }
}

async fn request(
    app: Router,
    method: &str,
    path: &str,
    headers: HeaderMap,
    body: Option<serde_json::Value>,
) -> axum::response::Response {
    use axum::body::Body;
    use http::Request;
    let mut builder = Request::builder().method(method).uri(path);
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    let request = builder
        .body(Body::from(body.map(|b| b.to_string()).unwrap_or_default()))
        .unwrap();
    app.oneshot(request).await.unwrap()
}

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

impl ApiFixture {
    async fn bootstrap_token(&self) -> String {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static(HOST));
        let response = request(self.router(), "GET", "/api/v1/bootstrap", headers, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        body["csrf_token"].as_str().unwrap().to_string()
    }

    async fn post_job(&self, headers: RequestHeaders) -> axum::response::Response {
        let body = json!({
            "source_url": "https://example.test/file.iso",
            "root_id": self.root_id,
            "relative_directory": "isos",
            "filename_override": null,
            "conflict_policy": "resume"
        });
        request(
            self.router(),
            "POST",
            "/api/v1/jobs",
            headers.into_header_map(),
            Some(body),
        )
        .await
    }

    async fn get(&self, path: &str, host: &str) -> axum::response::Response {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_str(host).unwrap());
        request(self.router(), "GET", path, headers, None).await
    }
}

#[tokio::test]
async fn mutation_requires_loopback_host_same_origin_json_and_csrf() {
    let fixture = ApiFixture::new().await;
    let token = fixture.bootstrap_token().await;

    assert_eq!(
        fixture.post_job(RequestHeaders::none()).await.status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        fixture
            .post_job(RequestHeaders::cross_origin(&token))
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        fixture
            .post_job(RequestHeaders::form(&token))
            .await
            .status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert_eq!(
        fixture
            .post_job(RequestHeaders::same_origin_json(&token))
            .await
            .status(),
        StatusCode::CREATED
    );
}

#[tokio::test]
async fn api_404_is_json_and_never_spa_html() {
    let fixture = ApiFixture::new().await;
    let response = fixture.get("/api/v1/missing", HOST).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response.headers()[axum::http::header::CONTENT_TYPE],
        "application/json"
    );
    let body: ApiErrorEnvelope = serde_json::from_value(json_body(response).await).unwrap();
    assert_eq!(body.code, "not_found");
}

#[tokio::test]
async fn bootstrap_requires_loopback_host_and_sets_no_store() {
    let fixture = ApiFixture::new().await;

    let rejected = fixture.get("/api/v1/bootstrap", "example.com").await;
    assert_eq!(rejected.status(), StatusCode::FORBIDDEN);

    let accepted = fixture.get("/api/v1/bootstrap", HOST).await;
    assert_eq!(accepted.status(), StatusCode::OK);
    assert_eq!(
        accepted.headers()[axum::http::header::CACHE_CONTROL],
        "no-store"
    );
}

#[tokio::test]
async fn responses_carry_security_headers_and_no_cors() {
    let fixture = ApiFixture::new().await;
    let response = fixture.get("/api/v1/bootstrap", HOST).await;
    let headers = response.headers();
    let csp = headers
        .get(axum::http::header::CONTENT_SECURITY_POLICY)
        .expect("CSP header")
        .to_str()
        .unwrap();
    assert!(csp.contains("default-src 'self'"), "CSP: {csp}");
    assert!(csp.contains("frame-ancestors 'none'"), "CSP: {csp}");
    assert_eq!(
        headers[axum::http::header::X_CONTENT_TYPE_OPTIONS],
        "nosniff"
    );
    assert_eq!(headers[axum::http::header::REFERRER_POLICY], "no-referrer");
    assert_eq!(headers[axum::http::header::X_FRAME_OPTIONS], "DENY");
    assert!(headers
        .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .is_none());
}
