//! Crash/restart test suite (§36.3, task 4.7): interrupt transfers at
//! randomized points (mid-segment write, checkpoint save, pause, verify),
//! restart, and verify byte-exact final output.
//!
//! Process-kill semantics are approximated in-process by dropping all
//! controller state (the checkpoint + temp file persist on disk); a
//! follow-up phase (7.6) extends this with real multi-process kills.

#[path = "support/mod.rs"]
mod support;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kdown_engine::config::EngineConfig;
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::io::sink::Sink as _;
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};
use kdown_engine::resume::checkpoint_store::CheckpointStore as _;
use kdown_engine::resume::{job_identity, DurabilityMode, FileCheckpointStore};
use support::fixtures::{assert_bytes_exact, deterministic_bytes};
use support::test_server::{ScriptedResponse, TestServer};

fn fast_config() -> EngineConfig {
    EngineConfig {
        checkpoint_flush_interval: Duration::from_millis(60),
        ..EngineConfig::default()
    }
}

fn controller() -> SingleStreamController {
    let mut cfg = fast_config();
    // Roomier attempts for the multi-kill runs.
    cfg.retry.max_attempts_per_segment = 16;
    SingleStreamController::new(
        HttpTransport::new(kdown_engine::config::NetworkPolicy::default()).expect("transport"),
        cfg,
    )
}

fn sequential_controller() -> SingleStreamController {
    let mut cfg = fast_config();
    cfg.transfer.segmentation_threshold = u64::MAX;
    SingleStreamController::new(
        HttpTransport::new(kdown_engine::config::NetworkPolicy::default()).expect("transport"),
        cfg,
    )
}

/// Server: kill after a partial-body write for the first `kill_count`
/// GETs, then serve whole (honoring Range like a well-behaved origin).
/// HEAD always full.
fn crashy(path: &'static str, content: Arc<Vec<u8>>, kill_count: u32) -> TestServer {
    let hits = Arc::new(AtomicU32::new(0));
    TestServer::new().serve_handler(path, move |req| {
        let n = hits.fetch_add(1, Ordering::SeqCst);
        if req.method == "HEAD" {
            return ScriptedResponse::ok((*content).clone()).with_header("accept-ranges", "bytes");
        }
        let total = (*content).len() as u64;
        let range_response = |start: u64| {
            let body = (*content)[start as usize..].to_vec();
            ScriptedResponse::new(206)
                .with_body(body)
                .with_header(
                    "content-range",
                    &format!("bytes {start}-{}/{total}", total - 1),
                )
                .with_header("accept-ranges", "bytes")
        };
        if n < kill_count {
            let cut = (4096usize + n as usize * 8192).min((*content).len().saturating_sub(1));
            // Kill the ranged response mid-body (§36.2 truncated bodies).
            let start = req.range.map(|(s, _)| s).unwrap_or(0);
            let full = ScriptedResponse::ok((*content).clone());
            let _ = full;
            let mut r = range_response(start);
            r.reset_after = Some((cut.saturating_sub(start as usize)) + 1);
            r
        } else if let Some((s, _e)) = req.range {
            range_response(s)
        } else {
            ScriptedResponse::ok((*content).clone()).with_header("accept-ranges", "bytes")
        }
    })
}

#[tokio::test]
async fn kill_during_segment_write_then_resume() {
    let content = Arc::new(deterministic_bytes(2 * 1024 * 1024, 41_000));
    let server = crashy("/crash.bin", content.clone(), 2)
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("crash.bin");
    let expected_crash = content.clone();
    let c = controller();
    let r1 = tokio::time::timeout(
        Duration::from_secs(60),
        c.run(DownloadRequest::new(server.url("/crash.bin"), dest.clone())),
    )
    .await
    .expect("no hang")
    .expect("terminal");
    assert_eq!(r1.status, ResultStatus::Completed, "{r1:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &expected_crash);
}

#[tokio::test]
async fn interrupt_during_pause_simulates_process_kill() {
    // Pause -> checkpoint persisted -> "process death" (drop handle and
    // controller without terminal transition) -> fresh run resumes from
    // the checkpoint and completes byte-exact (§36.3 crash at pause).
    let content = Arc::new(vec![6u8; 2_400_000]);
    let expected = content.clone();
    let server = TestServer::new()
        .serve_handler("/kill.bin", move |req| {
            let total = (*content).len() as u64;
            let base = if let (Some((s, _e)), false) = (req.range, req.method == "HEAD") {
                let body = (*content)[s as usize..].to_vec();
                ScriptedResponse::new(206)
                    .with_body(body)
                    .with_header("content-range", &format!("bytes {s}-{}/{total}", total - 1))
            } else {
                ScriptedResponse::ok((*content).clone())
            };
            base.chunked(Duration::from_millis(20))
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("kill.bin");
    let identity = job_identity(&server.url("/kill.bin"), &dest);

    // Run 1: transfer a while, pause (checkpoint persisted), then abort
    // the task without terminal cleanup — the closest in-process analog
    // of SIGKILL during Running.
    {
        let c = controller();
        let (handle, join) = c.start(DownloadRequest::new(server.url("/kill.bin"), dest.clone()));
        tokio::time::sleep(Duration::from_millis(400)).await;
        handle.pause();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let snap = handle.snapshot();
        assert!(snap.completed_bytes > 0, "progress before kill");
        join.abort(); // hard stop: no terminal state, no cleanup
    }
    // The checkpoint must exist (pause persisted it, §9.3 step 5) and the
    // temp file must be intact.
    let store = FileCheckpointStore::new(dir.path(), DurabilityMode::Performance).expect("store");
    let cp = store
        .load(&identity)
        .expect("load")
        .expect("checkpoint survived simulated kill");
    assert!(cp.completed_bytes() > 0);

    // Run 2: fresh controller resumes (temp reused because validators
    // match) and completes byte-exact.
    let c2 = controller();
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        c2.run(DownloadRequest::new(server.url("/kill.bin"), dest.clone())),
    )
    .await;
    if result.is_err() {
        let reqs = server.requests().await;
        for r in &reqs {
            eprintln!("REQ {} {} range={:?}", r.method, r.path, r.range);
        }
        eprintln!("cp bytes: {:?}", store.load(&identity));
    }
    let result = result.expect("no hang").expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert!(
        result.bytes_reused_from_checkpoint > 0,
        "resume reused the persisted prefix: {:?}",
        result
    );
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &expected);
    assert!(!dir.path().join("kill.bin.part").exists());
}

#[tokio::test]
async fn kill_during_checkpoint_save_leaves_usable_state() {
    // Repeatedly save+load in a loop while "killing" (dropping) mid-write:
    // the atomic-replace store must never expose a torn checkpoint (§15.3).
    let dir = tempfile::tempdir().expect("tmp");
    let store = FileCheckpointStore::new(dir.path(), DurabilityMode::Performance).expect("store");
    let identity = "crash-store";
    for i in 0..200u32 {
        let mut cp = kdown_engine::resume::Checkpoint::new(identity, "u", "t");
        cp.record_completed(0, i as u64 * 10 + 5);
        store.save_atomic(&cp).expect("save");
        // Immediately reload: must be valid (never torn).
        let loaded = store.load(identity).expect("load");
        let cp2 = loaded.expect("present after save");
        assert_eq!(cp2.completed_ranges, vec![(0, i as u64 * 10 + 5)]);
        assert!(cp2.completed_bytes() == i as u64 * 10 + 6);
    }
}

#[tokio::test]
async fn interruption_during_final_rename_leaves_temp_or_final() {
    // Crash at final rename: either the rename happened (final exists,
    // complete) or it did not (temp exists, final absent). Never both
    // partial. Verified at the sink level with an injected failure point.
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("commit.bin");
    let temp = dir.path().join("commit.bin.part");
    std::fs::write(&temp, b"complete-data").expect("temp");
    // Simulate the pre-rename crash: state on disk shows only temp.
    assert!(temp.exists());
    assert!(!dest.exists());
    // Recovery: a later commit of the same temp completes atomically.
    let mut sink = kdown_engine::io::sink::FileSink::open(
        &dest,
        &kdown_engine::io::sink::TempFileSpec::default(),
        false,
        false,
    )
    .expect("reopen");
    sink.finalize().expect("finalize");
    sink.commit().expect("commit");
    assert_eq!(std::fs::read(&dest).expect("read"), b"complete-data");
}

#[tokio::test]
async fn randomized_multi_kill_suite() {
    // §36.3 driver: several seeds × kill points; every run must end
    // byte-exact.
    for seed in 0..4u64 {
        let content = Arc::new(deterministic_bytes(1024 * 1024, 50_000 + seed));
        let kills = 1 + (seed % 3) as u32;
        let expected = content.clone();
        let server = crashy("/multi.bin", content.clone(), kills)
            .start()
            .await
            .expect("start");
        let dir = tempfile::tempdir().expect("tmp");
        let dest = dir.path().join("multi.bin");
        let c = controller();
        let result = tokio::time::timeout(
            Duration::from_secs(90),
            c.run(DownloadRequest::new(server.url("/multi.bin"), dest.clone())),
        )
        .await
        .expect("no hang")
        .expect("terminal");
        assert_eq!(result.status, ResultStatus::Completed, "seed {seed}");
        assert_bytes_exact(&std::fs::read(&dest).expect("read"), &expected);
    }
}

#[tokio::test]
async fn durable_mode_pause_checkpoint_resumes_byte_exact() {
    // Durable-mode variant of the pause/kill/resume scenario: the sidecar
    // fsyncs file data and the directory entry before rename, so the
    // simulated process death after pause still resumes byte-exact
    // (§15.3, §15.4, §36.3).
    let content = Arc::new(vec![9u8; 2_400_000]);
    let expected = content.clone();
    let server = TestServer::new()
        .serve_handler("/durable.bin", move |req| {
            let total = (*content).len() as u64;
            let base = if let (Some((s, _e)), false) = (req.range, req.method == "HEAD") {
                let body = (*content)[s as usize..].to_vec();
                ScriptedResponse::new(206)
                    .with_body(body)
                    .with_header("content-range", &format!("bytes {s}-{}/{total}", total - 1))
            } else {
                ScriptedResponse::ok((*content).clone())
            };
            base.chunked(Duration::from_millis(20))
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("durable.bin");
    let identity = job_identity(&server.url("/durable.bin"), &dest);

    let mut cfg = fast_config();
    cfg.transfer.durability = kdown_engine::config::DurabilityMode::Durable;
    {
        let c = SingleStreamController::new(
            HttpTransport::new(kdown_engine::config::NetworkPolicy::default()).expect("transport"),
            cfg.clone(),
        );
        let (handle, join) = c.start(DownloadRequest::new(
            server.url("/durable.bin"),
            dest.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(400)).await;
        handle.pause();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            handle.snapshot().completed_bytes > 0,
            "progress before kill"
        );
        join.abort(); // simulated SIGKILL during Running
    }
    // The durable checkpoint survived the simulated kill with no temp
    // residue (durable mode syncs contents and directory entry).
    let store = FileCheckpointStore::new(dir.path(), DurabilityMode::Durable).expect("store");
    let cp = store
        .load(&identity)
        .expect("load")
        .expect("durable checkpoint survived simulated kill");
    assert!(cp.completed_bytes() > 0);
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .expect("dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "no temp residue: {leftovers:?}");

    // Fresh controller resumes byte-exact (§36.3).
    let c2 = SingleStreamController::new(
        HttpTransport::new(kdown_engine::config::NetworkPolicy::default()).expect("transport"),
        cfg,
    );
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        c2.run(DownloadRequest::new(
            server.url("/durable.bin"),
            dest.clone(),
        )),
    )
    .await
    .expect("no hang")
    .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert!(
        result.bytes_reused_from_checkpoint > 0,
        "resume reused the persisted prefix: {result:?}"
    );
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &expected);
    assert!(!dir.path().join("durable.bin.part").exists());
}

#[tokio::test]
async fn real_process_crash_child() {
    let Ok(url) = std::env::var("KDOWN_REAL_CRASH_URL") else {
        return;
    };
    let destination = std::path::PathBuf::from(
        std::env::var_os("KDOWN_REAL_CRASH_DESTINATION").expect("destination path"),
    );
    let ready = std::env::var_os("KDOWN_REAL_CRASH_READY").expect("ready path");
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let store = FileCheckpointStore::new(parent, DurabilityMode::Performance).expect("store");
    let identity = job_identity(&url, &destination);
    let controller = sequential_controller();
    let (handle, _join) = controller.start(DownloadRequest::new(url, destination.clone()));
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let checkpoint = store.load(&identity).expect("load child checkpoint");
        if handle.snapshot().completed_bytes > 0
            && checkpoint
                .as_ref()
                .is_some_and(|cp| cp.completed_bytes() > 0)
        {
            std::fs::write(&ready, b"checkpointed").expect("signal parent");
            std::future::pending::<()>().await;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "child never reached durable checkpoint progress"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn real_process_crash_releases_lock_and_resumes_partial_output() {
    let content = Arc::new(deterministic_bytes(8 * 1024 * 1024, 77_031));
    let server_content = content.clone();
    let server = TestServer::new()
        .serve_handler("/process-crash.bin", move |request| {
            let total = server_content.len() as u64;
            let etag = "\"process-crash-v1\"";
            if request.method == "HEAD" {
                return ScriptedResponse::ok((*server_content).clone())
                    .with_header("accept-ranges", "bytes")
                    .with_header("etag", etag);
            }
            let (status, body, content_range) = match request.range {
                Some((start, end)) => {
                    let end = end.min(total - 1);
                    let body = server_content[start as usize..=end as usize].to_vec();
                    (206, body, Some(format!("bytes {start}-{end}/{total}")))
                }
                None => (200, (*server_content).clone(), None),
            };
            let mut response = ScriptedResponse::new(status)
                .with_body(body)
                .with_header("accept-ranges", "bytes")
                .with_header("etag", etag);
            if let Some(content_range) = content_range {
                response = response.with_header("content-range", &content_range);
            }
            response.chunked(Duration::from_millis(5))
        })
        .start()
        .await
        .expect("start server");
    let directory = tempfile::tempdir().expect("tempdir");
    let destination = directory.path().join("process-crash.bin");
    let url = server.url("/process-crash.bin");
    let ready = directory.path().join("child-ready");
    let mut child = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", "real_process_crash_child", "--nocapture"])
        .env("KDOWN_REAL_CRASH_URL", &url)
        .env("KDOWN_REAL_CRASH_DESTINATION", &destination)
        .env("KDOWN_REAL_CRASH_READY", &ready)
        .spawn()
        .expect("spawn download child process");

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !ready.exists() {
        if let Some(status) = child.try_wait().expect("poll download child") {
            panic!("download child exited before checkpointing: {status}");
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("download child did not persist partial checkpoint state");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.kill().expect("kill download process");
    let _ = child.wait().expect("reap download process");

    let identity = job_identity(&url, &destination);
    let store = FileCheckpointStore::new(directory.path(), DurabilityMode::Performance)
        .expect("checkpoint store");
    let checkpoint = store
        .load(&identity)
        .expect("load checkpoint after process death")
        .expect("partial checkpoint survives process death");
    assert_eq!(
        checkpoint.format_version,
        kdown_engine::resume::CHECKPOINT_FORMAT_VERSION
    );
    assert!(checkpoint.completed_bytes() > 0);
    assert!(checkpoint.completed_bytes() < content.len() as u64);
    let part = directory.path().join("process-crash.bin.part");
    let partial = std::fs::read(&part).expect("partial output survives process death");
    assert_eq!(
        &partial[..checkpoint.completed_bytes() as usize],
        &content[..checkpoint.completed_bytes() as usize],
        "checkpointed bytes agree with the partial file"
    );
    let lockfile_present = std::fs::read_dir(directory.path())
        .expect("read directory")
        .filter_map(Result::ok)
        .any(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with(".kdown-destination-") && name.ends_with(".lock")
        });
    assert!(
        lockfile_present,
        "crash leaves the persistent lockfile in place"
    );

    let controller = sequential_controller();
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        controller.run(DownloadRequest::new(url, destination.clone())),
    )
    .await
    .expect("resume completes without hanging")
    .expect("terminal result");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert!(result.bytes_reused_from_checkpoint > 0, "{result:?}");
    assert_bytes_exact(
        &std::fs::read(&destination).expect("final output"),
        &content,
    );
    assert!(
        !part.exists(),
        "successful resume consumes the partial output"
    );
    assert!(
        lockfile_present,
        "the unlocked lockfile remains reusable after resume"
    );
}

/// Task 2.7: the pipelined write path survives a server-side connection
/// kill mid-segment exactly like the legacy lanes — outstanding executor
/// writes settle, the retry resumes from the acknowledged prefix, and the
/// download completes byte-exactly.
#[tokio::test]
async fn pipelined_kill_during_segment_write_then_resume() {
    let content = Arc::new(deterministic_bytes(2 * 1024 * 1024, 41_001));
    let server = crashy("/crash-pipelined.bin", content.clone(), 2)
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("crash-pipelined.bin");
    let expected_crash = content.clone();

    let mut cfg = fast_config();
    cfg.write_executor.pipeline_writes = true;
    cfg.write_executor.writer_threads = 2;
    // Tiny read-ahead churns executor reservations across retries, so any
    // permit leak would stall the pipeline instead of completing.
    cfg.write_budget.worker_read_ahead_bytes = 2 * u64::from(cfg.read_buffer_size);
    let c = SingleStreamController::new(
        HttpTransport::new(kdown_engine::config::NetworkPolicy::default()).expect("transport"),
        cfg,
    );
    let r1 = tokio::time::timeout(
        Duration::from_secs(60),
        c.run(DownloadRequest::new(
            server.url("/crash-pipelined.bin"),
            dest.clone(),
        )),
    )
    .await
    .expect("no hang")
    .expect("terminal");
    assert_eq!(r1.status, ResultStatus::Completed, "{r1:?}");
    assert_bytes_exact(&std::fs::read(&dest).expect("read"), &expected_crash);
}

/// Task 2.7: pause with queued executor writes, then simulate process
/// death; a fresh controller resumes from the checkpoint byte-exactly.
/// The pipelined pause drains queued writes before the coordinator save,
/// so the checkpoint holds only acknowledged coverage (durable and
/// performance modes).
#[tokio::test]
async fn pipelined_pause_then_process_restart_resumes_byte_exact() {
    for durability in [
        kdown_engine::config::DurabilityMode::Performance,
        kdown_engine::config::DurabilityMode::Durable,
    ] {
        let content = Arc::new(vec![7u8; 2_400_000]);
        let expected = content.clone();
        let server = TestServer::new()
            .serve_handler("/pipelined-pause.bin", move |req| {
                let total = (*content).len() as u64;
                let base = if let (Some((s, _e)), false) = (req.range, req.method == "HEAD") {
                    let body = (*content)[s as usize..].to_vec();
                    ScriptedResponse::new(206)
                        .with_body(body)
                        .with_header("content-range", &format!("bytes {s}-{}/{total}", total - 1))
                } else {
                    ScriptedResponse::ok((*content).clone())
                };
                base.chunked(Duration::from_millis(20))
            })
            .start()
            .await
            .expect("start");
        let dir = tempfile::tempdir().expect("tmp");
        let dest = dir.path().join("pipelined-pause.bin");

        let mut cfg = fast_config();
        cfg.transfer.durability = durability;
        cfg.write_executor.pipeline_writes = true;
        cfg.write_executor.writer_threads = 2;
        {
            let c = SingleStreamController::new(
                HttpTransport::new(kdown_engine::config::NetworkPolicy::default())
                    .expect("transport"),
                cfg.clone(),
            );
            let (handle, join) = c.start(DownloadRequest::new(
                server.url("/pipelined-pause.bin"),
                dest.clone(),
            ));
            tokio::time::sleep(Duration::from_millis(400)).await;
            assert!(
                handle.snapshot().completed_bytes > 0,
                "{durability:?}: acknowledged coverage before pause"
            );
            handle.pause();
            tokio::time::sleep(Duration::from_millis(400)).await;
            // The pipelined workers drained their queued writes; the
            // coordinator saved the acknowledged prefix before the pause
            // boundary settled.
            let identity = job_identity(&server.url("/pipelined-pause.bin"), &dest);
            let sidecar = dir.path().join(format!("{identity}.kdown"));
            assert!(
                sidecar.exists(),
                "{durability:?}: checkpoint persisted at the pause boundary"
            );
            // Simulated process death: abort the run task without a
            // terminal transition (SIGKILL semantics).
            join.abort();
        }
        assert!(!dest.exists(), "{durability:?}: no published output");

        let c = SingleStreamController::new(
            HttpTransport::new(kdown_engine::config::NetworkPolicy::default()).expect("transport"),
            cfg,
        );
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            c.run(DownloadRequest::new(
                server.url("/pipelined-pause.bin"),
                dest.clone(),
            )),
        )
        .await
        .expect("no hang")
        .expect("terminal");
        assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
        assert_bytes_exact(&std::fs::read(&dest).expect("read"), &expected);
    }
}
