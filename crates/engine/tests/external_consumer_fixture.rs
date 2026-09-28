//! External-consumer compile fixture (consumer-api spec, task 2.1).
//!
//! Simulates a downstream embedding application: it imports ONLY the
//! documented supported surface (`docs/api-surface.md`) and drives a full
//! download lifecycle — start, observe, control, await, and distinguish
//! every terminal outcome. It must compile before and after the visibility
//! narrowing; a build failure here means the supported surface regressed.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

// Supported crate-root re-exports.
use kdown_engine::{
    CancelMode, Checkpoint, CompletedDownload, DownloadController, DownloadError, DownloadHandle,
    DownloadRequest, DownloadRunError, EngineConfig, EngineMetrics, FailureDomain, HttpTransport,
    ProgressSnapshot, Redactor, TransferAccounting,
};

/// Directory-target API proofs (automatic-filename-resolution): the
/// explicit-file request stays exhaustively constructible, `Rename` is a
/// matchable non-exhaustive variant, and the directory request/entry
/// points are nameable with their builder options.
#[test]
fn directory_api_signature_proofs() {
    // Unchanged explicit-file struct literal: adding no fields keeps
    // exhaustive construction compiling (source compatibility).
    let literal = DownloadRequest {
        url: "https://example.test/f".to_string(),
        destination: PathBuf::from("file.bin"),
        headers: vec![],
        expected_size: None,
        integrity: kdown_engine::IntegrityPolicy::default(),
        overwrite: OverwritePolicy::default(),
        resume: ResumePolicy::default(),
        authorization: None,
        credential_provider: None,
    };
    let _ = literal;

    // `OverwritePolicy::Rename` is matchable under the non_exhaustive enum.
    match OverwritePolicy::Rename {
        OverwritePolicy::Rename => {}
        other => panic!("unexpected variant: {other:?}"),
    }

    // Directory request construction, mutation and builders.
    let mut request =
        kdown_engine::DirectoryDownloadRequest::new("https://example.test/f", PathBuf::from("."));
    request
        .request_mut()
        .headers
        .push(("accept".to_string(), "*/*".to_string()));
    let built =
        kdown_engine::DirectoryDownloadRequest::new("https://example.test/f", PathBuf::from("."))
            .with_fallback_filename("fallback.bin")
            .with_max_filename_bytes(128);
    let _ = built;

    // Entry points exist with the file-API shapes (nameable without a
    // running server).
    fn entry_points(controller: &DownloadController, url: String, directory: PathBuf) {
        let request = kdown_engine::DirectoryDownloadRequest::new(url, directory);
        let _ = controller.start_to_directory(request);
    }
    let _ = entry_points as fn(&DownloadController, String, PathBuf);
    fn futures(controller: &DownloadController, url: String, directory: PathBuf) {
        // Nameable future types (the async entry points return concrete
        // futures; drop them explicitly without awaiting).
        let request = kdown_engine::DirectoryDownloadRequest::new(url.clone(), directory.clone());
        drop(controller.run_to_directory(request));
        let request = kdown_engine::DirectoryDownloadRequest::new(url, directory);
        drop(controller.run_to_directory_with_handle(request));
    }
    let _ = futures as fn(&DownloadController, String, PathBuf);
}
// Observation type (pre-narrowing path; re-exported at the root after the
// visibility change).
use kdown_engine::JobState;

// Supported module paths.
use kdown_engine::config::{OverwritePolicy, ResumePolicy};
use kdown_engine::control::rate_limit::TokenBucket;
use kdown_engine::error::ErrorCategory;
use kdown_engine::metrics::EventStream;

// Checkpoint-store injection surface for resolver implementors
// (re-exported at the root; docs/api-surface.md).
use kdown_engine::{CheckpointStore, CheckpointStoreResolver};

/// A minimal custom resolver: proves the injection trait is nameable and
/// implementable from outside the crate.
struct PassthroughResolver;

impl CheckpointStoreResolver for PassthroughResolver {
    fn resolve(
        &self,
        context: &kdown_engine::CheckpointResolveContext,
    ) -> Result<Arc<dyn CheckpointStore>, kdown_engine::CheckpointError> {
        kdown_engine::SidecarCheckpointResolver::default().resolve(context)
    }
}

/// Nameability proof for every whitelisted root re-export.
#[allow(unused)]
fn nameability() {
    let _ = std::path::PathBuf::new();
    let _: Option<PathBuf> = None;
    let _: OverwritePolicy = OverwritePolicy::default();
    let _: ResumePolicy = ResumePolicy::default();
    let _bucket: Arc<TokenBucket> = Arc::new(TokenBucket::new(0));
    let _redactor = Redactor::new();
    let _: Option<ErrorCategory> = None;
    let _: Option<FailureDomain> = None;
    let _: Option<Duration> = None;
}

/// The exact future shape of `EventStream::next()` (an alias keeps the
/// signature proof readable and clippy-clean).
type NextFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Option<kdown_engine::Event>> + 'a>>;

/// Exact-signature proofs for the supported surface (task 6.3): a change to
/// any parameter or return type here fails the fixture build, not only the
/// inventory script. Covers the controls, diagnostics, checkpoint and event
/// surfaces reinforced by the remediation change.
#[test]
fn supported_signature_proofs() {
    let _: fn() -> Redactor = Redactor::new;
    let _: fn(Redactor, &[&str]) -> Redactor = Redactor::with_sensitive_query_params;
    let _: fn(Redactor) -> Redactor = Redactor::with_marked_query_params_only;
    let _: fn(&Redactor, &str) -> String = Redactor::redact_url;
    let _: fn(&TransferAccounting) -> Option<f64> = TransferAccounting::wire_amplification;
    let _: fn(&DownloadHandle) -> ProgressSnapshot = DownloadHandle::snapshot;
    let _: fn(&DownloadHandle) -> JobState = DownloadHandle::state;
    let _: fn(&DownloadHandle) -> EventStream = DownloadHandle::events;
    let _: fn(&DownloadHandle, CancelMode) = DownloadHandle::cancel_with;
    let _: fn(&DownloadHandle) -> Option<u64> = DownloadHandle::rate_limit;
    let _: fn(&DownloadHandle, u64) = DownloadHandle::set_rate_limit;
    let _: fn(&EventStream) -> bool = EventStream::is_finished;
    // `next()` is async: prove the exact future output type.
    let _: for<'a> fn(&'a mut EventStream) -> NextFuture<'a> = |stream| Box::pin(stream.next());
    let _: fn(String, PathBuf) -> DownloadRequest = |url, dest| DownloadRequest::new(url, dest);
    let _: fn() -> EngineConfig = EngineConfig::default;
    let _: fn(&EngineConfig) -> Result<HttpTransport, DownloadError> = HttpTransport::from_config;

    // Lifetime controls shipped with the remediation change.
    let mut config = EngineConfig::default();
    config.transfer.job_deadline = Some(Duration::from_secs(30));
    config.max_active_jobs = 8;
    let _: Option<Duration> = config.transfer.job_deadline;
    assert_eq!(config.max_active_jobs, 8);

    // Admission refusal is a matchable, non-retryable typed variant.
    let admission = DownloadError::AdmissionRejected { active: 1, cap: 2 };
    match admission {
        DownloadError::AdmissionRejected { active, cap } => {
            let _: u32 = active;
            let _: u32 = cap;
        }
        other => panic!("unexpected variant: {other:?}"),
    }

    // v2 checkpoint constructor and local-binding fields.
    let mut checkpoint =
        Checkpoint::new("identity", "https://example.test/f?token=sentinel", "tmp");
    checkpoint.owned_temp_identity = Some("path|0:0".to_string());
    checkpoint.covered_digest = Some("digest".to_string());
    let _: Option<String> = checkpoint.owned_temp_identity;
    let _: Option<String> = checkpoint.covered_digest;
}

async fn drive(url: String, destination: PathBuf) -> Result<CompletedDownload, DownloadRunError> {
    let config = EngineConfig::default();
    let transport = HttpTransport::from_config(&config).expect("transport");
    let metrics = EngineMetrics::shared();
    let controller = DownloadController::new(transport, config)
        .with_checkpoint_resolver(Arc::new(PassthroughResolver));

    let request = DownloadRequest::new(url, destination);
    let (handle, task) = controller.start(request);

    // Observation APIs: snapshots, state, and event streams are all
    // available while the job runs.
    let snapshot: ProgressSnapshot = handle.snapshot();
    let _ = snapshot.completed_bytes;
    let _state: JobState = handle.state();
    let mut events: EventStream = handle.events();
    let _ = events.try_next();

    // Terminal outcome: success is a verified publication; every other
    // outcome is a typed error distinguishable without parsing strings.
    let terminal = task.await.expect("job task");
    match &terminal {
        Ok(completed) => {
            let _ = completed.final_path.display();
            let _ = completed.accounting.completed_bytes;
        }
        Err(error) => {
            match error.domain() {
                FailureDomain::Transfer => {}
                FailureDomain::Infrastructure => {}
                FailureDomain::Cancelled => {}
                // `#[non_exhaustive]`: forward-compatible match.
                _ => {}
            }
            let _ = error.category();
            let _ = error.accounting().wasted_bytes;
            let _ = error.artifacts();
            let _ = metrics.snapshot().jobs_completed;
        }
    }
    terminal
}

/// Runtime-control APIs take effect on a running job; cancelling a job
/// reports a typed cancellation, never a success.
#[tokio::test]
async fn runtime_controls_and_cancellation_are_typed() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        use std::io::{Read as _, Write as _};
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            loop {
                match stream.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        request.push(byte[0]);
                        if request.ends_with(b"\r\n\r\n") {
                            break;
                        }
                    }
                }
            }
            let request = String::from_utf8_lossy(&request).to_string();
            let is_head = request.starts_with("HEAD");
            let body = b"consumer fixture payload";
            let mut response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            if !is_head {
                response.push_str(std::str::from_utf8(body).expect("ascii body"));
            }
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });
    let config = EngineConfig::default();
    let transport = HttpTransport::from_config(&config).expect("transport");
    let controller = DownloadController::new(transport, config);
    let dir = tempfile::tempdir().expect("tmpdir");
    let (handle, task) = controller.start(DownloadRequest::new(
        format!("http://{addr}/slow.bin"),
        dir.path().join("cancelled.bin"),
    ));
    handle.pause();
    handle.resume_now();
    handle.set_rate_limit(1024);
    let _ = handle.rate_limit();
    handle.cancel_with(CancelMode::KeepPartial);
    let outcome = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("cancellation converges")
        .expect("job task");
    assert!(
        matches!(outcome, Err(DownloadRunError::Cancelled(_))),
        "cancellation is a typed error: {outcome:?}"
    );
}

/// The engine metrics surface is directly constructible by hosts.
#[test]
fn metrics_surface_is_nameable() {
    let metrics = EngineMetrics::new();
    metrics.job_started();
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.jobs_started, 1);
}

// `drive` is exercised by the consumer smoke test below against a local
// HTTP endpoint so the fixture stays a real compile+run proof.
#[tokio::test]
async fn consumer_lifecycle_compiles_and_runs() {
    // A trivial HTTP endpoint on localhost: answers the HEAD probe and the
    // GET (keep-alive: both may arrive on one connection) with the same
    // fixed resource.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        use std::io::{Read as _, Write as _};
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            // One request per connection; responses declare
            // `Connection: close` so the client reconnects instead of
            // pipelining into a socket nobody reads.
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            loop {
                match stream.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        request.push(byte[0]);
                        if request.ends_with(b"\r\n\r\n") {
                            break;
                        }
                    }
                }
            }
            let request = String::from_utf8_lossy(&request).to_string();
            eprintln!(
                "[fixture-server] request: {} bytes, head={}",
                request.len(),
                request.starts_with("HEAD")
            );
            let is_head = request.starts_with("HEAD");
            let body = b"consumer fixture payload";
            let mut response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            if !is_head {
                response.push_str(std::str::from_utf8(body).expect("ascii body"));
            }
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
            // Closing after Content-Length bytes were sent is safe.
        }
    });

    let dir = tempfile::tempdir().expect("tmpdir");
    let destination = dir.path().join("out.bin");
    let completed = drive(format!("http://{addr}/fixture.bin"), destination.clone())
        .await
        .expect("download completes");
    assert_eq!(completed.final_path, destination);
    assert_eq!(
        std::fs::read(&destination).expect("published bytes"),
        b"consumer fixture payload"
    );
}
