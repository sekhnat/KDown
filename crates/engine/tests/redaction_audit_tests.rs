//! End-to-end redaction audit (§35.3, task 7.2) and correlation logging.
//!
//! Runs jobs with bearer credentials and cookies against the fixture
//! server while capturing everything the engine logs at default levels,
//! then asserts no secret value appears in any record. Also verifies
//! correlation fields are present so operators can attribute events.

use std::sync::{Arc, Mutex};
use tracing_subscriber::layer::SubscriberExt;

use kdown_engine::config::EngineConfig;
use kdown_engine::job::controller::{DownloadController, DownloadRequest};

mod support;
use support::test_server::{ScriptedResponse, TestServer};

/// A tracing layer that records every event's full formatted output
/// (including fields) at the default level filter (§35.1: no TRACE).
#[derive(Clone)]
struct CapturingLayer {
    records: Arc<Mutex<Vec<String>>>,
}

struct CapturingVisitor {
    msg: String,
    fields: Vec<(String, String)>,
}

impl CapturingVisitor {
    fn render(&self) -> String {
        let mut out = self.msg.clone();
        for (k, v) in &self.fields {
            out.push_str(&format!(" {k}={v}"));
        }
        out
    }
}

impl tracing::field::Visit for CapturingVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.msg = format!("{value:?}");
        } else {
            self.fields
                .push((field.name().to_string(), format!("{value:?}")));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.msg = value.to_string();
        } else {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturingLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut v = CapturingVisitor {
            msg: String::new(),
            fields: vec![],
        };
        event.record(&mut v);
        self.records.lock().expect("records").push(v.render());
    }
}

/// The shared capture sink; tests install a global default subscriber
/// once (set_global_default may only be called once per process), so all
/// records from all tests in this binary land here, tagged by job URL.
fn capture_sink() -> Arc<Mutex<Vec<String>>> {
    static SINK: std::sync::OnceLock<Arc<Mutex<Vec<String>>>> = std::sync::OnceLock::new();
    SINK.get_or_init(|| {
        let records: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
        let subscriber = tracing_subscriber::registry()
            .with(CapturingLayer {
                records: records.clone(),
            })
            // Default levels: everything except TRACE (§35.1 defaults).
            .with(tracing_subscriber::filter::LevelFilter::DEBUG);
        tracing::subscriber::set_global_default(subscriber)
            .expect("one global subscriber per test binary");
        records
    })
    .clone()
}

/// Run a download and return all records captured during the run window
/// (from `before` to after the terminal state). The sink is shared across
/// tests; callers pass the pre-run length for a parallel-safe window.
fn records_since(captured: &Arc<Mutex<Vec<String>>>, before: usize) -> Vec<String> {
    captured.lock().expect("records")[before..].to_vec()
}

async fn run_with_capture(
    _server_url: String,
    request: DownloadRequest,
) -> (
    usize,
    Vec<String>,
    kdown_engine::job::controller::DownloadResult,
) {
    let captured = capture_sink();
    let before = captured.lock().expect("records").len();

    let cfg = EngineConfig::default();
    let transport =
        kdown_engine::http::transport::HttpTransport::new(cfg.network.clone()).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let result = controller.run(request).await.expect("run");
    let records = records_since(&captured, before);
    (before, records, result)
}

fn test_request(url: String, dest: std::path::PathBuf) -> DownloadRequest {
    let mut r = DownloadRequest::new(url, dest);
    r.headers.push((
        "Authorization".to_string(),
        "Bearer super-secret-bearer-token".to_string(),
    ));
    r.headers.push((
        "Cookie".to_string(),
        "session=TOP-SECRET-COOKIE".to_string(),
    ));
    r.authorization = Some("Bearer another-secret-value".to_string());
    r
}

/// No authorization/cookie values appear in any log record at default
/// levels, even when the job fails (§35.3 audit).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn credentials_never_appear_in_logs() {
    // 403 target: the job fails, exercising error paths with credentials
    // attached.
    let server = TestServer::new()
        .serve_handler("/secret.bin", |_| {
            ScriptedResponse::new(403).with_body(b"forbidden".to_vec())
        })
        .start()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmpdir");
    let req = test_request(server.url("/secret.bin"), dir.path().join("out.bin"));
    let (before, records, result) = run_with_capture(server.url("/"), req).await;
    let _ = before;
    assert_eq!(
        result.status,
        kdown_engine::job::controller::ResultStatus::Failed
    );
    assert!(
        !records.is_empty(),
        "the failing job must have produced log records"
    );
    for r in &records {
        assert!(!r.contains("TOP-SECRET-COOKIE"), "cookie value leaked: {r}");
        assert!(
            !r.contains("secret-bearer-token"),
            "bearer value leaked: {r}"
        );
        assert!(
            !r.contains("another-secret-value"),
            "authorization value leaked: {r}"
        );
    }
}

/// Successful downloads likewise never leak credentials, and events carry
/// correlation fields for attribution (§35.2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn successful_job_logs_correlated_without_secrets() {
    let content = b"correlated logging fixture".to_vec();
    let server = TestServer::new()
        .serve_static("/f.bin", content.clone())
        .start()
        .await
        .expect("server");
    let dir = tempfile::tempdir().expect("tmpdir");
    let req = test_request(server.url("/f.bin"), dir.path().join("out.bin"));
    let (_before, records, result) = run_with_capture(server.url("/"), req).await;
    assert_eq!(
        result.status,
        kdown_engine::job::controller::ResultStatus::Completed
    );
    for r in &records {
        assert!(!r.contains("super-secret-bearer-token"), "bearer leak: {r}");
        assert!(!r.contains("TOP-SECRET-COOKIE"), "cookie leak: {r}");
        assert!(
            !r.contains("another-secret-value"),
            "authorization leak: {r}"
        );
    }
    // Correlation: at least one record carries the (redacted) origin.
    let origin_field = records.iter().any(|r| r.contains("origin=http"));
    assert!(origin_field, "no origin correlation field in {records:?}");
}
