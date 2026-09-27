//! Byte-exact publication regressions (task 2.1).
//!
//! These exercise the real controller against loopback servers and assert the
//! integrity contract directly: a successful publication contains exactly the
//! bytes acknowledged by the transfer, and a failed transfer never publishes
//! anything. Preallocation, stale `.part` tails and interrupted bodies must
//! not be able to turn unverified filesystem content into success.

mod support;

use std::sync::Arc;
use std::time::Duration;

use kdown_engine::{
    Checkpoint, CheckpointError, CheckpointStore, CheckpointStoreResolver, CompletedDownload,
    DownloadController, DownloadRequest, DownloadRunError, EngineConfig, HttpTransport,
};
use support::fixtures;
use support::test_server::{ScriptedResponse, TestServer};

/// A default request for `url` writing to `dest`.
fn request(url: String, dest: &std::path::Path) -> DownloadRequest {
    DownloadRequest::new(url, dest.to_path_buf())
}

/// Run a request with a bounded wall clock so a regression cannot hang CI.
async fn run(
    cfg: EngineConfig,
    req: DownloadRequest,
) -> Result<CompletedDownload, DownloadRunError> {
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    tokio::time::timeout(Duration::from_secs(60), controller.run(req))
        .await
        .expect("no hang")
}

/// A fast-retry configuration so failure-path tests stay quick.
fn impatient_retry(mut cfg: EngineConfig) -> EngineConfig {
    cfg.retry.max_attempts_per_segment = 2;
    cfg.retry.base_delay = Duration::from_millis(10);
    cfg.retry.max_delay = Duration::from_millis(50);
    cfg
}

/// Known length: HEAD advertises six bytes but the actual body provides only
/// three. With preallocation enabled (the default) the preallocated hole must
/// not count as received bytes: the job fails and publishes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn short_known_body_with_preallocation_fails_without_publishing() {
    let server = TestServer::new()
        .serve_handler("/short", |req| {
            if req.method == "HEAD" {
                // Advertise six bytes to the probe...
                ScriptedResponse::ok(vec![b'n', b'e', b'w', 0, 0, 0])
            } else {
                // ...but deliver only three.
                ScriptedResponse::ok(b"new".to_vec())
            }
        })
        .start()
        .await
        .expect("start");
    let cfg = EngineConfig::default();
    assert!(
        cfg.transfer.preallocate_output,
        "the regression specifically covers preallocated output"
    );
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let result = run(cfg, request(server.url("/short"), &dest)).await;
    assert!(
        result.is_err(),
        "a body shorter than the advertised length must fail: {result:?}"
    );
    assert!(
        !dest.exists(),
        "a failed download must never publish output at {}",
        dest.display()
    );
}

/// Unknown length: a stale `.part` contains `STALE-TAIL` and the server
/// streams the three-byte chunked body `new`. Success must publish
/// exactly `new`, never the stale tail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_length_body_over_stale_part_publishes_only_acknowledged_bytes() {
    let server = TestServer::new()
        .serve_handler("/new", |_| {
            ScriptedResponse::ok(b"new".to_vec()).unknown_length()
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    std::fs::write(dir.path().join("out.bin.part"), b"STALE-TAIL").expect("stale part");

    let completed = run(EngineConfig::default(), request(server.url("/new"), &dest))
        .await
        .expect("download completes");
    assert!(completed.final_path.exists());
    let output = std::fs::read(&dest).expect("read output");
    assert_eq!(
        output, b"new",
        "the published output must be exactly the acknowledged stream"
    );
}

/// Known length with preallocation: a stale `.part` longer than the resource
/// must not survive into the published output either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_length_body_over_stale_part_publishes_only_the_resource() {
    let content = fixtures::deterministic_bytes(6, 0x71);
    let served = content.clone();
    let server = TestServer::new()
        .serve_handler("/six", move |_| ScriptedResponse::ok(served.clone()))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    std::fs::write(dir.path().join("out.bin.part"), b"STALE-TAIL-LONGER").expect("stale part");

    let completed = run(EngineConfig::default(), request(server.url("/six"), &dest))
        .await
        .expect("download completes");
    assert!(completed.final_path.exists());
    let output = std::fs::read(&dest).expect("read output");
    assert_eq!(
        output, content,
        "no stale byte may reach the published file"
    );
}

/// Interrupted unknown-length body: the stream dies before EOF, so no
/// successful publication may appear regardless of any existing partial.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_unknown_length_body_never_publishes() {
    let server = TestServer::new()
        .serve_handler("/flaky", |_| {
            ScriptedResponse::ok(b"new-and-never-completing".to_vec())
                .unknown_length()
                .reset_after(3)
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    std::fs::write(dir.path().join("out.bin.part"), b"STALE-TAIL").expect("stale part");

    let result = run(
        impatient_retry(EngineConfig::default()),
        request(server.url("/flaky"), &dest),
    )
    .await;
    assert!(result.is_err(), "an interrupted body must fail: {result:?}");
    assert!(
        !dest.exists(),
        "an interrupted body must never publish output at {}",
        dest.display()
    );
}

/// Interrupted known-length body with a retry: the first attempt dies after
/// three of six bytes, the retry completes, and the published file is exactly
/// the six acknowledged bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_known_length_body_retries_to_a_byte_exact_publication() {
    let content = fixtures::deterministic_bytes(6, 0x72);
    let server = TestServer::new()
        .serve_n(
            "/flaky-six",
            1,
            ScriptedResponse::ok(content.clone()).reset_after(3),
        )
        .serve_n("/flaky-six", 16, ScriptedResponse::ok(content.clone()))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let mut cfg = impatient_retry(EngineConfig::default());
    cfg.retry.max_attempts_per_segment = 4;

    let completed = run(cfg, request(server.url("/flaky-six"), &dest))
        .await
        .expect("the retry must complete the download");
    assert!(completed.final_path.exists());
    let output = std::fs::read(&dest).expect("read output");
    assert_eq!(
        output, content,
        "the retried publication must contain exactly the resource bytes"
    );
}

/// A truncated physical file left behind by an earlier failure cannot make a
/// later short body succeed: the accepted coverage still has to be complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preallocated_hole_is_not_received_coverage() {
    // Advertise six, deliver three, but preallocate to six: the file length
    // already matches the advertised total before verification, so only
    // accepted-byte coverage can reject it.
    let server = TestServer::new()
        .serve_handler("/hole", |req| {
            if req.method == "HEAD" {
                ScriptedResponse::ok(vec![1, 2, 3, 4, 5, 6])
            } else {
                ScriptedResponse::ok(vec![1, 2, 3])
            }
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let mut cfg = EngineConfig::default();
    cfg.transfer.preallocate_output = true;

    let result = run(cfg, request(server.url("/hole"), &dest)).await;
    assert!(result.is_err(), "a preallocated hole is not coverage");
    assert!(!dest.exists());
}

/// Sanity: an honestly complete short body publishes byte-exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clean_eof_publishes_exactly_the_acknowledged_bytes() {
    let content = Arc::new(b"new-bytes-exactly".to_vec());
    let served = Arc::clone(&content);
    let server = TestServer::new()
        .serve_handler("/clean", move |_| ScriptedResponse::ok((*served).clone()))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let completed = run(
        EngineConfig::default(),
        request(server.url("/clean"), &dest),
    )
    .await
    .expect("download completes");
    assert!(completed.final_path.exists());
    let output = std::fs::read(&dest).expect("read output");
    assert_eq!(output, *content);
}

/// A symlink planted at the temp entry must never be followed: the fresh
/// job unlinks the entry, writes its own file, and the unrelated target
/// keeps its bytes (design D2, §14.1).
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn symlink_at_part_entry_never_touches_unrelated_file() {
    let content = fixtures::deterministic_bytes(64 * 1024, 0x74);
    let server = TestServer::new()
        .serve_static("/file.bin", content.clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let scratch = dir.path().join("unrelated.scratch");
    std::fs::write(&scratch, b"UNRELATED-SCRATCH").expect("scratch");
    std::os::unix::fs::symlink(&scratch, dir.path().join("out.bin.part")).expect("plant symlink");

    let completed = run(
        EngineConfig::default(),
        request(server.url("/file.bin"), &dest),
    )
    .await
    .expect("download completes despite the impostor entry");
    assert!(completed.final_path.exists());
    assert_eq!(std::fs::read(&dest).expect("read output"), content);
    assert_eq!(
        std::fs::read(&scratch).expect("read scratch"),
        b"UNRELATED-SCRATCH",
        "the symlink target must never be written through"
    );
}

/// A hard-linked temp entry is unlinked rather than written through; the
/// unrelated link keeps its bytes and the job publishes its own output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hardlinked_part_entry_never_touches_unrelated_file() {
    let content = fixtures::deterministic_bytes(64 * 1024, 0x75);
    let server = TestServer::new()
        .serve_static("/file.bin", content.clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let scratch = dir.path().join("unrelated.scratch");
    std::fs::write(&scratch, b"UNRELATED-SCRATCH").expect("scratch");
    std::fs::hard_link(&scratch, dir.path().join("out.bin.part")).expect("hard link");

    let completed = run(
        EngineConfig::default(),
        request(server.url("/file.bin"), &dest),
    )
    .await
    .expect("download completes despite the linked entry");
    assert!(completed.final_path.exists());
    assert_eq!(std::fs::read(&dest).expect("read output"), content);
    assert_eq!(
        std::fs::read(&scratch).expect("read scratch"),
        b"UNRELATED-SCRATCH",
        "the other hard link must keep its bytes"
    );
}

/// Resume over a swapped-in symlink fails closed or restarts cleanly: the
/// unrelated target is never modified and no wrong bytes are published.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_over_swapped_part_symlink_is_safe() {
    use kdown_engine::config::ResumePolicy;
    use kdown_engine::CancelMode;

    let content = fixtures::deterministic_bytes(1024 * 1024, 0x76);
    let server = TestServer::new()
        .serve_static("/file.bin", content.clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let cfg = EngineConfig {
        checkpoint_flush_interval: Duration::from_millis(50),
        ..EngineConfig::default()
    };
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let (handle, join) =
        controller.start(DownloadRequest::new(server.url("/file.bin"), dest.clone()));
    handle.set_rate_limit(128 * 1024);
    tokio::time::sleep(Duration::from_millis(400)).await;
    handle.cancel_with(CancelMode::KeepPartial);
    let _ = tokio::time::timeout(Duration::from_secs(20), join).await;
    assert!(!dest.exists(), "nothing published before the restart");
    drop(controller);

    let part = dir.path().join("out.bin.part");
    assert!(part.exists(), "KeepPartial retains the partial output");
    let scratch = dir.path().join("unrelated.scratch");
    std::fs::write(&scratch, b"UNRELATED-SCRATCH").expect("scratch");
    std::fs::remove_file(&part).expect("remove the retained partial");
    std::os::unix::fs::symlink(&scratch, &part).expect("plant symlink");

    let transport = HttpTransport::from_config(&EngineConfig::default()).expect("transport");
    let controller = DownloadController::new(transport, EngineConfig::default());
    let mut retry = DownloadRequest::new(server.url("/file.bin"), dest.clone());
    retry.resume = ResumePolicy::Allowed;
    match tokio::time::timeout(Duration::from_secs(30), controller.run(retry))
        .await
        .expect("no hang")
    {
        Ok(completed) => {
            assert!(completed.final_path.exists());
            assert_eq!(
                std::fs::read(&dest).expect("read output"),
                content,
                "a restart publishes the true bytes"
            );
        }
        Err(error) => {
            assert!(!dest.exists(), "a typed failure never publishes: {error:?}");
        }
    }
    assert_eq!(
        std::fs::read(&scratch).expect("read scratch"),
        b"UNRELATED-SCRATCH",
        "the swapped symlink target must never be modified"
    );
}

/// Task 5.4: the default sidecar stores no raw URL text and is owner-only,
/// and a signed-URL resume stays safe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_url_sidecar_is_secret_free_and_owner_only() {
    use kdown_engine::config::ResumePolicy;
    use kdown_engine::CancelMode;

    let content = fixtures::deterministic_bytes(1024 * 1024, 0x77);
    let server = TestServer::new()
        .serve_static("/file.bin", content.clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("signed.bin");
    let cfg = EngineConfig {
        checkpoint_flush_interval: Duration::from_millis(50),
        ..EngineConfig::default()
    };
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let url = server.url("/file.bin?X-Amz-Signature=SIDECAR-URL-SECRET&token=SIDECAR-OTHER-SECRET");
    let (handle, join) = controller.start(DownloadRequest::new(url.clone(), dest.clone()));
    handle.set_rate_limit(128 * 1024);
    tokio::time::sleep(Duration::from_millis(400)).await;
    handle.cancel_with(CancelMode::KeepPartial);
    let _ = tokio::time::timeout(Duration::from_secs(20), join).await;
    assert!(!dest.exists(), "nothing published before the restart");
    drop(controller);

    // The default resolver names the sidecar by an opaque job identity
    // (private to the crate): locate it by extension instead.
    let sidecar = std::fs::read_dir(dir.path())
        .expect("read dir")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| path.extension().is_some_and(|ext| ext == "kdown"))
        .expect("a checkpoint sidecar must have been persisted");
    let bytes = std::fs::read(&sidecar).expect("read sidecar");
    let text = String::from_utf8_lossy(&bytes);
    for secret in ["SIDECAR-URL-SECRET", "SIDECAR-OTHER-SECRET"] {
        assert!(
            !text.contains(secret),
            "URL secret `{secret}` persisted: {text}"
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&sidecar)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "sidecar must not be group/world readable");
    }

    // Safe restart: resume (or conservatively restart) and publish exactly.
    let transport = HttpTransport::from_config(&EngineConfig::default()).expect("transport");
    let controller = DownloadController::new(transport, EngineConfig::default());
    let mut retry = DownloadRequest::new(url, dest.clone());
    retry.resume = ResumePolicy::Allowed;
    let completed = tokio::time::timeout(Duration::from_secs(30), controller.run(retry))
        .await
        .expect("no hang")
        .expect("resume completes");
    assert_eq!(completed.final_path, dest);
    assert_eq!(std::fs::read(&dest).expect("read output"), content);
}

// ---- Validator-comparable resume admission (task 3.2) ----

/// A checkpoint store returning one fixed checkpoint for any job identity
/// and recording deletions, so admission decisions are observable without
/// depending on the private sidecar filename.
struct FixedCheckpointStore {
    checkpoint: std::sync::Mutex<Option<Checkpoint>>,
    deletes: std::sync::atomic::AtomicUsize,
}

impl CheckpointStore for FixedCheckpointStore {
    fn load(&self, _job_identity: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        Ok(self.checkpoint.lock().expect("checkpoint lock").clone())
    }

    fn save_atomic(&self, _checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        Ok(())
    }

    fn delete(&self, _job_identity: &str) -> Result<(), CheckpointError> {
        self.deletes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self.checkpoint.lock().expect("checkpoint lock") = None;
        Ok(())
    }
}

struct FixedResolver {
    store: Arc<FixedCheckpointStore>,
}

impl CheckpointStoreResolver for FixedResolver {
    fn resolve(
        &self,
        _context: &kdown_engine::CheckpointResolveContext,
    ) -> Result<Arc<dyn CheckpointStore>, CheckpointError> {
        let store: Arc<dyn CheckpointStore> = self.store.clone();
        Ok(store)
    }
}

fn fixed_store(checkpoint: Checkpoint) -> (Arc<FixedCheckpointStore>, Arc<FixedResolver>) {
    let store = Arc::new(FixedCheckpointStore {
        checkpoint: std::sync::Mutex::new(Some(checkpoint)),
        deletes: std::sync::atomic::AtomicUsize::new(0),
    });
    let resolver = Arc::new(FixedResolver {
        store: Arc::clone(&store),
    });
    (store, resolver)
}

/// A complete checkpoint with no validator evidence must not publish its
/// bytes without re-fetching: the reproduced P0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn complete_checkpoint_without_validators_refetches_instead_of_publishing() {
    use kdown_engine::config::ResumePolicy;

    let stale = fixtures::deterministic_bytes(6, 0x80);
    let current = fixtures::deterministic_bytes(6, 0x81);
    let served = current.clone();
    let server = TestServer::new()
        .serve_handler("/file.bin", move |_| ScriptedResponse::ok(served.clone()))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    std::fs::write(dir.path().join("out.bin.part"), &stale).expect("stale partial");
    let mut cp = Checkpoint::new("job", server.url("/file.bin"), "tmp");
    cp.total_size = Some(6);
    cp.validators.total_size = Some(6);
    cp.completed_ranges = vec![(0, 5)];
    let (store, resolver) = fixed_store(cp);

    let cfg = EngineConfig::default();
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg).with_checkpoint_resolver(resolver);
    let mut req = request(server.url("/file.bin"), &dest);
    req.resume = ResumePolicy::Allowed;
    let completed = tokio::time::timeout(Duration::from_secs(60), controller.run(req))
        .await
        .expect("no hang")
        .expect("download completes");
    assert!(completed.final_path.exists());
    assert_eq!(
        std::fs::read(&dest).expect("read output"),
        current,
        "the current representation must be fetched, never the stale partial"
    );
    assert!(
        server.requests().await.iter().any(|r| r.method == "GET"),
        "a GET must be issued after the non-comparable checkpoint is discarded"
    );
    assert!(
        store.deletes.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "the insufficient checkpoint must be deleted"
    );
}

/// A saved strong ETag that disappears from the current response is
/// insufficient evidence: the checkpoint is discarded and the resource is
/// re-fetched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disappearing_etag_discards_the_checkpoint_and_refetches() {
    use kdown_engine::config::ResumePolicy;

    let stale = fixtures::deterministic_bytes(6, 0x82);
    let current = fixtures::deterministic_bytes(6, 0x83);
    let served = current.clone();
    let server = TestServer::new()
        .serve_handler("/file.bin", move |_| ScriptedResponse::ok(served.clone()))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    std::fs::write(dir.path().join("out.bin.part"), &stale).expect("stale partial");
    let mut cp = Checkpoint::new("job", server.url("/file.bin"), "tmp");
    cp.total_size = Some(6);
    cp.validators.etag = Some("\"gen-1\"".into());
    cp.validators.total_size = Some(6);
    cp.completed_ranges = vec![(0, 5)];
    let (store, resolver) = fixed_store(cp);

    let cfg = EngineConfig::default();
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg).with_checkpoint_resolver(resolver);
    let mut req = request(server.url("/file.bin"), &dest);
    req.resume = ResumePolicy::Allowed;
    tokio::time::timeout(Duration::from_secs(60), controller.run(req))
        .await
        .expect("no hang")
        .expect("download completes");
    assert_eq!(std::fs::read(&dest).expect("read output"), current);
    assert!(server.requests().await.iter().any(|r| r.method == "GET"));
    assert!(store.deletes.load(std::sync::atomic::Ordering::SeqCst) >= 1);
}

/// A fully covered checkpoint with a matching strong ETag may skip the GET
/// only after admission accepts the comparable evidence (design D3).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn complete_checkpoint_with_matching_strong_etag_may_skip_the_get() {
    use kdown_engine::config::ResumePolicy;

    let admitted = fixtures::deterministic_bytes(6, 0x84);
    let remote = fixtures::deterministic_bytes(6, 0x85);
    let served = remote.clone();
    let server = TestServer::new()
        .serve_handler("/file.bin", move |_| {
            ScriptedResponse::ok(served.clone()).with_header("etag", "\"gen-1\"")
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    std::fs::write(dir.path().join("out.bin.part"), &admitted).expect("partial");
    let mut cp = Checkpoint::new("job", server.url("/file.bin"), "tmp");
    cp.total_size = Some(6);
    cp.validators.etag = Some("\"gen-1\"".into());
    cp.validators.total_size = Some(6);
    cp.completed_ranges = vec![(0, 5)];
    let (_store, resolver) = fixed_store(cp);

    let cfg = EngineConfig::default();
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg).with_checkpoint_resolver(resolver);
    let mut req = request(server.url("/file.bin"), &dest);
    req.resume = ResumePolicy::Allowed;
    tokio::time::timeout(Duration::from_secs(60), controller.run(req))
        .await
        .expect("no hang")
        .expect("download completes");
    assert_eq!(
        std::fs::read(&dest).expect("read output"),
        admitted,
        "an admitted complete checkpoint publishes its covered bytes"
    );
    assert!(
        !server.requests().await.iter().any(|r| r.method == "GET"),
        "a fully covered, comparable checkpoint may skip the GET"
    );
    // (A post-commit cleanup delete is expected; admission itself deleted
    // nothing.)
}
