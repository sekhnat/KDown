//! Request-diagnostic redaction regression tests: custom header values and
//! dedicated credential fields must never reach `Debug` output, scripted
//! mismatch messages, or proxy-configuration diagnostics, while header
//! names stay identifiable.

use kdown_engine::config::{EngineConfig, ProxyConfig};
use kdown_engine::control::CancellationToken;
use kdown_engine::http::execution::{HttpExecution, ProbeRequest};
use kdown_engine::http::scripted::{CallKind, CallRecord, ProbeStep, ScriptedHttp};
use kdown_engine::http::transport::RequestSpec;
use kdown_engine::job::controller::DownloadRequest;

/// Distinct secret sentinels: each must be absent from every formatted
/// diagnostic under test. The exact strings are asserted against directly,
/// rather than relying on any particular replacement text.
const SENTINELS: [&str; 5] = [
    "sentinel-bearer-token",
    "sentinel-proxy-basic",
    "sentinel-cookie-value",
    "sentinel-api-key",
    "sentinel-obscure-header",
];

fn sentinel_headers() -> Vec<(String, String)> {
    vec![
        ("Authorization".to_string(), SENTINELS[0].to_string()),
        ("Proxy-Authorization".to_string(), SENTINELS[1].to_string()),
        ("Cookie".to_string(), SENTINELS[2].to_string()),
        ("X-Api-Key".to_string(), SENTINELS[3].to_string()),
        ("X-Obscure-Credential".to_string(), SENTINELS[4].to_string()),
    ]
}

fn assert_names_visible(rendered: &str) {
    for name in [
        "Authorization",
        "Proxy-Authorization",
        "Cookie",
        "X-Api-Key",
        "X-Obscure-Credential",
    ] {
        assert!(
            rendered.contains(name),
            "header name `{name}` must stay identifiable in {rendered}"
        );
    }
}

#[test]
fn download_request_debug_never_contains_header_values() {
    let mut request = DownloadRequest::new("https://example.test/file", "file".into());
    request.headers = sentinel_headers();
    request.authorization = Some(SENTINELS[0].to_string());
    let rendered = format!("{request:?}");

    for sentinel in SENTINELS {
        assert!(
            !rendered.contains(sentinel),
            "sentinel `{sentinel}` leaked through DownloadRequest debug: {rendered}"
        );
    }
    assert_names_visible(&rendered);
}

#[test]
fn request_spec_debug_never_contains_header_values() {
    let spec = RequestSpec {
        url: "https://example.test/file".to_string(),
        headers: sentinel_headers(),
        range: None,
        validators: None,
        identity_encoding: true,
        sensitive: true,
    };
    let rendered = format!("{spec:?}");

    for sentinel in SENTINELS {
        assert!(
            !rendered.contains(sentinel),
            "sentinel `{sentinel}` leaked through RequestSpec debug: {rendered}"
        );
    }
    assert_names_visible(&rendered);
}

#[test]
fn call_record_debug_never_contains_header_values() {
    let record = CallRecord {
        kind: CallKind::Probe,
        url: "https://example.test/file".to_string(),
        range: None,
        headers: sentinel_headers(),
    };
    let rendered = format!("{record:?}");

    for sentinel in SENTINELS {
        assert!(
            !rendered.contains(sentinel),
            "sentinel `{sentinel}` leaked through CallRecord debug: {rendered}"
        );
    }
    assert_names_visible(&rendered);
}

#[tokio::test]
async fn scripted_mismatch_diagnostics_never_contain_expected_header_values() {
    // The fixture expectation carries a credential value; a mismatch must
    // name the header without printing the expected value.
    let scripted =
        ScriptedHttp::new().expect_probe(ProbeStep::new().header("authorization", SENTINELS[0]));
    let execution = HttpExecution::from_adapter(scripted);
    let failure = execution
        .probe(
            ProbeRequest {
                spec: RequestSpec {
                    url: "https://example.test/file".to_string(),
                    headers: vec![("accept".to_string(), "*/*".to_string())],
                    range: None,
                    validators: None,
                    identity_encoding: true,
                    sensitive: false,
                },
                segmentation_threshold: 0,
                verify_range_support: false,
            },
            &CancellationToken::new(),
        )
        .await
        .expect_err("header expectation must mismatch");
    let rendered = format!("{failure:?}");

    assert!(
        !rendered.contains(SENTINELS[0]),
        "expected header value leaked through mismatch diagnostics: {rendered}"
    );
    assert!(
        rendered.contains("authorization"),
        "mismatch must still identify the header name: {rendered}"
    );
}

#[test]
fn proxy_config_debug_never_contains_userinfo_credentials() {
    let config = EngineConfig {
        proxy: ProxyConfig::Http {
            url: "http://proxy-user:proxy-sentinel@proxy.example:8080".to_string(),
        },
        ..EngineConfig::default()
    };
    let rendered = format!("{config:?}");

    assert!(
        !rendered.contains("proxy-sentinel"),
        "proxy credential leaked through EngineConfig debug: {rendered}"
    );
    assert!(
        rendered.contains("proxy.example:8080"),
        "proxy location must stay identifiable: {rendered}"
    );

    let direct = format!("{:?}", config.proxy);
    assert!(!direct.contains("proxy-sentinel"));
}

/// URL secrets are masked in diagnostics while the non-secret shape of
/// the target stays identifiable (task 5.3).
const URL_SENTINELS: [&str; 2] = ["sentinel-userinfo-value", "sentinel-query-value"];

fn assert_no_url_sentinels(rendered: &str) {
    for sentinel in URL_SENTINELS {
        assert!(
            !rendered.contains(sentinel),
            "URL secret `{sentinel}` leaked: {rendered}"
        );
    }
}

#[test]
fn download_request_debug_redacts_url_userinfo_and_query_values() {
    let request = DownloadRequest::new(
        "https://user:sentinel-userinfo-value@example.test/file?token=sentinel-query-value",
        "file".into(),
    );
    let rendered = format!("{request:?}");
    assert_no_url_sentinels(&rendered);
    assert!(
        rendered.contains("example.test/file"),
        "the target must stay identifiable: {rendered}"
    );
}

#[test]
fn request_spec_debug_redacts_url_userinfo_and_query_values() {
    let spec = RequestSpec {
        url: "https://user:sentinel-userinfo-value@example.test/file?token=sentinel-query-value"
            .to_string(),
        headers: sentinel_headers(),
        range: None,
        validators: None,
        identity_encoding: true,
        sensitive: true,
    };
    let rendered = format!("{spec:?}");
    assert_no_url_sentinels(&rendered);
    assert!(
        rendered.contains("example.test/file"),
        "the target must stay identifiable: {rendered}"
    );
}

#[test]
fn redirect_resolution_errors_redact_url_secrets() {
    // Invalid-scheme Location carrying a query secret.
    let error = kdown_engine::http::redirect::resolve_redirect(
        "https://example.test/file",
        "1http://cdn.test/f?token=sentinel-query-value",
    )
    .expect_err("the invalid scheme must be rejected");
    assert_no_url_sentinels(&format!("{error} {error:?}"));

    // Unparseable current URL with userinfo and a query secret: the
    // InvalidUrl message must not echo either.
    let error = kdown_engine::http::redirect::resolve_redirect(
        "https://user:sentinel-userinfo-value@exa mple.test/f?token=sentinel-query-value",
        "/next",
    )
    .expect_err("the current URL must fail to parse");
    assert_no_url_sentinels(&format!("{error} {error:?}"));
}

#[tokio::test]
async fn transport_parse_failure_never_echoes_url_secrets() {
    let cfg = EngineConfig::default();
    let transport =
        kdown_engine::http::transport::HttpTransport::from_config(&cfg).expect("transport");
    let controller = kdown_engine::job::controller::DownloadController::new(transport, cfg);
    let dir = tempfile::tempdir().expect("tmpdir");
    let request = DownloadRequest::new(
        "https://user:sentinel-userinfo-value@exa mple.test/file?token=sentinel-query-value",
        dir.path().join("out.bin"),
    );
    let error = controller
        .run(request)
        .await
        .expect_err("the URL must fail to parse");
    assert_no_url_sentinels(&format!("{error} {error:?}"));
}
