//! Directory-target and Rename integration tests (automatic-filename-
//! resolution): real loopback HTTP through the shared test server, covering
//! option validation before networking, name resolution precedence, policy
//! outcomes, Rename collision/resume selection, and the resolved-destination
//! observation surface.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::support::test_server::{RequestInfo, ScriptedResponse, TestServer};
use kdown_engine::config::{OverwritePolicy, ResumePolicy};
use kdown_engine::{
    CompletedDownload, DirectoryDownloadRequest, DownloadController, DownloadError,
    DownloadRunError, Event, HttpTransport,
};

/// Body served by the download fixtures.
const BODY: &[u8] = b"automatic-filename-resolution-body";

/// A controller over the production transport against the loopback server.
fn controller() -> DownloadController {
    let config = kdown_engine::EngineConfig::default();
    let transport = HttpTransport::from_config(&config).expect("transport");
    DownloadController::new(transport, config)
}

/// Bounded run so a regression cannot hang CI.
async fn run(request: DirectoryDownloadRequest) -> Result<CompletedDownload, DownloadRunError> {
    let c = controller();
    tokio::time::timeout(Duration::from_secs(60), c.run_to_directory(request))
        .await
        .expect("no hang")
}

fn directory_request(url: String, directory: &Path) -> DirectoryDownloadRequest {
    DirectoryDownloadRequest::new(url, directory.to_path_buf())
}

/// HEAD that supplies a Content-Disposition name, followed by a GET body.
fn disposition_response(name: &str) -> ScriptedResponse {
    ScriptedResponse::ok(BODY.to_vec()).with_header(
        "content-disposition",
        &format!("attachment; filename=\"{name}\""),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_directory_targets_fail_before_network() {
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&requests);
    let server = TestServer::new()
        .serve_handler("/file", move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            ScriptedResponse::ok(BODY.to_vec())
        })
        .start()
        .await
        .expect("start");
    let root = tempfile::tempdir().expect("tmpdir");
    let missing = root.path().join("missing-directory");
    let file_target = root.path().join("plain-file.txt");
    std::fs::write(&file_target, b"existing").expect("file");

    #[derive(Debug)]
    struct Case {
        directory: PathBuf,
        fallback: String,
        cap: usize,
        rename: bool,
    }
    let cases = [
        // Missing directory target.
        Case {
            directory: missing.clone(),
            fallback: "download".into(),
            cap: 250,
            rename: false,
        },
        // A plain file is not a directory target.
        Case {
            directory: file_target.clone(),
            fallback: "download".into(),
            cap: 250,
            rename: false,
        },
        // Traversal fallback: sanitization changes it.
        Case {
            directory: root.path().to_path_buf(),
            fallback: "../escape".into(),
            cap: 250,
            rename: false,
        },
        // Cap above the hard 250-byte bound.
        Case {
            directory: root.path().to_path_buf(),
            fallback: "download".into(),
            cap: 251,
            rename: false,
        },
        // Cap below the fallback's byte length.
        Case {
            directory: root.path().to_path_buf(),
            fallback: "download".into(),
            cap: 7,
            rename: false,
        },
        // Rename requires at least one stem byte plus " (999)".
        Case {
            directory: root.path().to_path_buf(),
            fallback: "download".into(),
            cap: 6,
            rename: true,
        },
    ];
    for case in &cases {
        let mut request = directory_request(server.url("/file"), &case.directory);
        request = request
            .with_fallback_filename(case.fallback.clone())
            .with_max_filename_bytes(case.cap);
        request.request_mut().overwrite = if case.rename {
            OverwritePolicy::Rename
        } else {
            OverwritePolicy::FailIfExists
        };
        let result = run(request).await.expect_err("terminal failure");
        assert!(
            matches!(
                result.as_engine_error(),
                Some(DownloadError::Configuration(_))
            ),
            "expected Configuration for {case:?}, got {result:?}"
        );
    }
    // Zero network activity for every rejected request.
    assert_eq!(
        requests.load(Ordering::SeqCst),
        0,
        "no requests may be made"
    );
    // And zero output artifacts: only the plain file exists in the root.
    let entries: Vec<_> = std::fs::read_dir(root.path())
        .expect("readdir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(entries, vec!["plain-file.txt".to_string()], "no artifacts");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directory_download_resolves_disposition_name_with_events() {
    let server = TestServer::new()
        .serve_handler("/doc", |_| disposition_response("server name.bin"))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");

    let c = controller();
    let (handle, join) = c.start_to_directory(directory_request(server.url("/doc"), dir.path()));
    let mut events = handle.events();
    let mut resolved_index: Option<usize> = None;
    let mut first_progress_index: Option<usize> = None;
    let mut resolved_paths: Vec<String> = vec![];
    let mut index = 0usize;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .ok()
        .flatten()
    {
        match event {
            Event::DestinationResolved { path } => {
                resolved_index.get_or_insert(index);
                resolved_paths.push(path);
            }
            Event::Progress(_) => {
                first_progress_index.get_or_insert(index);
            }
            Event::StateChanged { .. } => {}
            Event::Committed { .. } => break,
            _ => {}
        }
        index += 1;
    }
    let completed = join.await.expect("job task").expect("download");
    // Exactly one DestinationResolved, before meaningful transfer progress,
    // agreeing with the handle and the terminal path.
    assert_eq!(resolved_paths.len(), 1, "exactly one resolved event");
    assert!(
        resolved_index.unwrap() < first_progress_index.unwrap_or(index),
        "resolved precedes progress"
    );
    assert_eq!(
        handle.resolved_destination(),
        Some(completed.final_path.as_path())
    );
    let expected = dir.path().join("server name.bin");
    assert_eq!(completed.final_path, expected);
    assert_eq!(std::fs::read(&expected).expect("read"), BODY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directory_url_basename_uses_final_head_url_over_original() {
    // No Content-Disposition: the final redirected HEAD URL segment wins
    // over the original URL segment; query strings never contribute.
    let server = TestServer::new()
        .serve_handler("/original-name.bin", |_| {
            ScriptedResponse::ok(BODY.to_vec())
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let completed = run(directory_request(
        format!("{}?q=value", server.url("/original-name.bin")),
        dir.path(),
    ))
    .await
    .expect("download");
    assert_eq!(
        completed.final_path,
        dir.path().join("original-name.bin"),
        "final HEAD URL segment wins; the query is excluded"
    );
    assert_eq!(std::fs::read(completed.final_path).expect("read"), BODY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directory_fail_if_exists_conflicts_after_probe() {
    let server = TestServer::new()
        .serve_handler("/doc", |_| disposition_response("server name.bin"))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let existing = dir.path().join("server name.bin");
    std::fs::write(&existing, b"untouched").expect("existing");

    let result = run(directory_request(server.url("/doc"), dir.path()))
        .await
        .expect_err("conflict");
    match result.as_engine_error() {
        Some(DownloadError::DestinationConflict(_)) => {}
        other => panic!("expected DestinationConflict, got {other:?}"),
    }
    // The existing entry is never clobbered.
    assert_eq!(std::fs::read(&existing).expect("read"), b"untouched");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directory_rename_selects_first_free_sibling() {
    let server = TestServer::new()
        .serve_handler("/doc", |_| disposition_response("file.bin"))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let occupied = dir.path().join("file.bin");
    std::fs::write(&occupied, b"occupied").expect("occupied base");

    let mut request = directory_request(server.url("/doc"), dir.path());
    request.request_mut().overwrite = OverwritePolicy::Rename;
    let completed = run(request).await.expect("download");
    assert_eq!(completed.final_path, dir.path().join("file (1).bin"));
    assert_eq!(std::fs::read(completed.final_path).expect("read"), BODY);
    // The occupied base stays untouched.
    assert_eq!(std::fs::read(&occupied).expect("read"), b"occupied");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rename_resume_prefers_checkpointed_sibling_over_free_names() {
    let server = TestServer::new()
        .serve_handler("/doc", |req: &RequestInfo| {
            // The disposition pins the resolved base name to "file.bin", so
            // the candidate list is file.bin + file (n).bin. Ranged GETs
            // (the resumed transfer) answer 206 for the requested slice.
            let mut response = ScriptedResponse::ok(BODY.to_vec())
                .with_header("etag", "\"gen-1\"")
                .with_header("content-disposition", "attachment; filename=\"file.bin\"");
            if let Some((start, end)) = req.range {
                response = ScriptedResponse::new(206)
                    .with_body(
                        BODY[start as usize..=end.min(BODY.len() as u64 - 1) as usize].to_vec(),
                    )
                    .with_header("etag", "\"gen-1\"")
                    .with_header(
                        "content-range",
                        &format!("bytes {start}-{end}/{}", BODY.len()),
                    );
            }
            response
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let url = server.url("/doc");
    // Occupied base; the resumable state lives at sibling (1).
    std::fs::write(dir.path().join("file.bin"), b"occupied").expect("base");
    let sibling = dir.path().join("file (1).bin");
    let part = dir.path().join("file (1).bin.part");
    let prefix_len = 8usize;
    std::fs::write(&part, &BODY[..prefix_len]).expect("partial");

    // Craft a structurally valid checkpoint for the sibling candidate: the
    // v2 local binding is stamped from the crafted `.part` file.
    let identity = kdown_engine::resume::job_identity(&url, &sibling);
    let mut checkpoint = kdown_engine::Checkpoint::new(&identity, &url, "tmp");
    checkpoint.total_size = Some(BODY.len() as u64);
    checkpoint.validators = kdown_engine::http::validators::ResourceValidators::from_headers(
        Some("\"gen-1\""),
        None,
        Some(BODY.len() as u64),
    );
    checkpoint.final_url = url.clone();
    checkpoint.record_completed(0, prefix_len as u64 - 1);
    checkpoint.set_owned_temp_identity(&part);
    checkpoint.set_covered_digest(&part);
    std::fs::write(
        dir.path().join(format!("{identity}.kdown")),
        checkpoint.to_json().expect("serialize"),
    )
    .expect("sidecar");

    let mut request = directory_request(url, dir.path());
    request.request_mut().overwrite = OverwritePolicy::Rename;
    request.request_mut().resume = ResumePolicy::Allowed;
    let completed = run(request).await.expect("resumed download");
    // The structurally admitted sibling beats any fresh free name.
    assert_eq!(completed.final_path, sibling);
    assert_eq!(std::fs::read(&sibling).expect("read"), BODY);
    assert!(
        completed.accounting.bytes_reused_from_checkpoint >= prefix_len as u64,
        "resume reused the crafted prefix: {:?}",
        completed.accounting
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_directory_rename_requests_choose_distinct_names() {
    let server = TestServer::new()
        .serve_handler("/doc", |_| disposition_response("file.bin"))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let c = controller();
    let make = || {
        let mut request = directory_request(server.url("/doc"), dir.path());
        request.request_mut().overwrite = OverwritePolicy::Rename;
        request
    };
    let (first, second) = tokio::join!(c.run_to_directory(make()), c.run_to_directory(make()));
    let first = first.expect("first download");
    let second = second.expect("second download");
    let paths = [first.final_path.clone(), second.final_path.clone()];
    assert_ne!(
        paths[0], paths[1],
        "concurrent contenders must not collide: {paths:?}"
    );
    for path in &paths {
        assert_eq!(std::fs::read(path).expect("read"), BODY);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directory_rename_skips_stale_partial_without_truncation() {
    let server = TestServer::new()
        .serve_handler("/doc", |_| disposition_response("file.bin"))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    std::fs::write(dir.path().join("file.bin"), b"occupied").expect("base");
    // A stale `.part` at the first sibling: occupied, never truncated.
    let stale_part = dir.path().join("file (1).bin.part");
    std::fs::write(&stale_part, b"STALE-STATE").expect("stale part");

    let mut request = directory_request(server.url("/doc"), dir.path());
    request.request_mut().overwrite = OverwritePolicy::Rename;
    request.request_mut().resume = ResumePolicy::Allowed;
    let completed = run(request).await.expect("download");
    assert_eq!(completed.final_path, dir.path().join("file (2).bin"));
    assert_eq!(std::fs::read(&stale_part).expect("read"), b"STALE-STATE");
    assert_eq!(
        std::fs::read(dir.path().join("file.bin")).expect("read"),
        b"occupied"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directory_rename_treats_dangling_symlinks_as_occupied() {
    let server = TestServer::new()
        .serve_handler("/doc", |_| disposition_response("file.bin"))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    std::fs::write(dir.path().join("file.bin"), b"occupied").expect("base");
    // A dangling symlink at the first sibling is an occupied entry.
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(dir.path().join("nowhere"), dir.path().join("file (1).bin"))
            .expect("symlink");
    }

    let mut request = directory_request(server.url("/doc"), dir.path());
    request.request_mut().overwrite = OverwritePolicy::Rename;
    let completed = run(request).await.expect("download");
    #[cfg(unix)]
    assert_eq!(completed.final_path, dir.path().join("file (2).bin"));
    #[cfg(not(unix))]
    assert_eq!(completed.final_path, dir.path().join("file (1).bin"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directory_replace_policy_replaces_resolved_destination() {
    let server = TestServer::new()
        .serve_handler("/doc", |_| disposition_response("server name.bin"))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let existing = dir.path().join("server name.bin");
    std::fs::write(&existing, b"stale-bytes").expect("existing");

    let mut request = directory_request(server.url("/doc"), dir.path());
    request.request_mut().overwrite = OverwritePolicy::Replace;
    let completed = run(request).await.expect("download");
    assert_eq!(completed.final_path, existing);
    // Replace semantics at the resolved path.
    assert_eq!(std::fs::read(&existing).expect("read"), BODY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directory_download_probes_once_and_transfers_once() {
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&requests);
    let server = TestServer::new()
        .serve_handler("/doc", move |req: &RequestInfo| {
            counter.fetch_add(1, Ordering::SeqCst);
            let _ = req;
            disposition_response("server name.bin")
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    run(directory_request(server.url("/doc"), dir.path()))
        .await
        .expect("download");
    // One HEAD probe plus one transfer GET: the body-less validating
    // ranged probe only runs for segmentation-sized resources.
    assert_eq!(requests.load(Ordering::SeqCst), 2, "no extra probe");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_checkpoint_resolver_sees_the_final_destination() {
    struct RecordingResolver {
        seen: std::sync::Mutex<Vec<PathBuf>>,
    }
    impl kdown_engine::CheckpointStoreResolver for RecordingResolver {
        fn resolve(
            &self,
            context: &kdown_engine::CheckpointResolveContext,
        ) -> Result<Arc<dyn kdown_engine::CheckpointStore>, kdown_engine::CheckpointError> {
            self.seen
                .lock()
                .expect("seen")
                .push(context.destination.clone());
            kdown_engine::SidecarCheckpointResolver::default().resolve(context)
        }
    }

    let server = TestServer::new()
        .serve_handler("/doc", |_| disposition_response("server name.bin"))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let recorder = Arc::new(RecordingResolver {
        seen: std::sync::Mutex::new(vec![]),
    });
    let config = kdown_engine::EngineConfig::default();
    let transport = HttpTransport::from_config(&config).expect("transport");
    let c = DownloadController::new(transport, config)
        .with_checkpoint_resolver(
            Arc::clone(&recorder) as Arc<dyn kdown_engine::CheckpointStoreResolver>
        );
    let request = directory_request(server.url("/doc"), dir.path());
    let completed = tokio::time::timeout(Duration::from_secs(60), c.run_to_directory(request))
        .await
        .expect("no hang")
        .expect("download");
    // The resolver receives the RESOLVED final destination, never the
    // bare directory.
    let seen = recorder.seen.lock().expect("seen").clone();
    assert!(!seen.is_empty());
    assert!(!seen.contains(&dir.path().to_path_buf()));
    assert_eq!(
        seen.last().expect("at least one resolution"),
        &completed.final_path
    );
}
