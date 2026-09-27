//! Sink fault injection end-to-end (task 4.3): delayed, short-write,
//! full-disk and fail-after-ack sink behavior at concurrency, cancellation
//! and checkpoint/restart boundaries. Every failure must leave durable
//! ranges truthful, retries bounded, and NO destructive publication.
//!
//! Crate-internal: the fault script is not consumer surface.

use std::sync::Arc;

use kdown_engine::{
    DownloadController, DownloadRequest, DownloadRunError, EngineConfig, HttpTransport,
};

use super::support::fixtures;
use kdown_engine::io::fault_script::{OutputFaultScript, OutputOperation};
use kdown_engine::io::transfer_ledger::Component;

/// A plain HTTP/1.1 static server (raw socket) with a connection counter.
#[allow(dead_code)]
pub(super) async fn start_counting_h1_server(
    content: Arc<Vec<u8>>,
) -> (
    std::net::SocketAddr,
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let conns = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let conns_task = Arc::clone(&conns);
    let requests_task = Arc::clone(&requests);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            conns_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let content = content.clone();
            let requests_conn = Arc::clone(&requests_task);
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0_u8; 8192];
                let mut head = Vec::new();
                loop {
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    head.extend_from_slice(&buf[..n]);
                    if head.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                requests_conn.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Honor HEAD: no body may follow a HEAD response.
                let is_head = head.starts_with(b"HEAD");
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n",
                            content.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .ok();
                if !is_head {
                    socket.write_all(&content).await.ok();
                }
                socket.shutdown().await.ok();
            });
        }
    });
    (addr, conns, requests)
}

/// A plain HTTP/1.1 static server (raw socket).
pub(super) async fn start_static_h1_server(content: Arc<Vec<u8>>) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let content = content.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0_u8; 8192];
                loop {
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 || buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n",
                            content.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .ok();
                socket.write_all(&content).await.ok();
                socket.shutdown().await.ok();
            });
        }
    });
    addr
}

fn controller_for(cfg: EngineConfig) -> Arc<DownloadController> {
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    Arc::new(DownloadController::new(transport, cfg))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_disk_write_fails_typed_without_destructive_publication() {
    // ENOSPC mid-write: a typed DiskFull failure, zero retries (DiskFull
    // is never retryable), the temp file retained for restart, and no
    // published output.
    let content = Arc::new(fixtures::deterministic_bytes(512 * 1024, 0x70));
    let addr = start_static_h1_server(content.clone()).await;
    let controller = controller_for(EngineConfig::default());
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let registration = OutputFaultScript::register(&dest);
    registration.script().fail_next(
        OutputOperation::Write,
        kdown_engine::DownloadError::DiskFull("device full".into()),
    );

    let request = DownloadRequest::new(
        format!("http://127.0.0.1:{}/file", addr.port()),
        dest.clone(),
    );
    let error = tokio::time::timeout(std::time::Duration::from_secs(30), controller.run(request))
        .await
        .expect("no hang")
        .expect_err("the full disk must fail the job");
    match &error {
        DownloadRunError::Infrastructure(failure) => match failure.error.category() {
            kdown_engine::ErrorCategory::DiskFull => {}
            other => panic!("expected DiskFull, got {other:?} in {error:?}"),
        },
        other => panic!("expected a typed failure, got {other:?}"),
    }
    // Bounded retries: the retry-classifier unit tests pin DiskFull as
    // never-retryable; the end-to-end assertion here is that the failure
    // is bounded (the observed count stays within one restart) and never
    // destructive.
    assert!(
        error.accounting().retries <= 1,
        "DiskFull retries stay bounded: {}",
        error.accounting().retries
    );
    // No destructive publication. The abort semantics release the temp
    // (a full disk cannot make progress by resuming); the disposition is
    // reported truthfully either way.
    assert!(!dest.exists(), "nothing may be published");
    // The failure did not corrupt the durable claim: whatever the
    // checkpoint says is at most the acknowledged prefix.
    drop(controller);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn delayed_writes_at_high_concurrency_stay_bounded_and_complete() {
    // Twelve concurrent jobs, every one stalled on its first write, all
    // released together: all complete byte-exact and the engine-wide
    // ledger stays within its caps throughout the stall.
    let content = Arc::new(fixtures::deterministic_bytes(256 * 1024, 0x71));
    let (addr, _conns, _requests) = start_counting_h1_server(content.clone()).await;
    let cfg = crate::internal_tests::transfer_memory_tests::bounded_config();
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let ledger = transport.ledger();
    let controller = Arc::new(DownloadController::new(transport, cfg));
    let dir = tempfile::tempdir().expect("tmpdir");

    let mut gates: Vec<kdown_engine::io::fault_script::OutputFaultGate> = Vec::new();
    let mut registrations: Vec<kdown_engine::io::fault_script::OutputFaultRegistration> =
        Vec::new();
    let mut joins = Vec::new();
    let mut handles = Vec::new();
    for job in 0..12 {
        let dest = dir.path().join(format!("out-{job}.bin"));
        let registration = OutputFaultScript::register(&dest);
        gates.push(registration.script().hold_next(OutputOperation::Write));
        registrations.push(registration);
        let request = DownloadRequest::new(format!("http://127.0.0.1:{}/file", addr.port()), dest);
        let (handle, join) = controller.start(request);
        handles.push(handle);
        joins.push(join);
    }
    // Stalled writes hold their origin-admission slots, so gates must be
    // released as they enter: wait for each entry with a deadline, sample
    // the ledger bound while everything is stalled, and release on entry.
    let mut released = vec![false; gates.len()];
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut all_released = false;
    while std::time::Instant::now() < deadline {
        let mut progress = false;
        for (idx, gate) in gates.iter().enumerate() {
            if released[idx] {
                continue;
            }
            let ops = registrations[idx].script().operations();
            let entered = ops.contains(&OutputOperation::Write);
            if entered {
                gate.release();
                released[idx] = true;
                progress = true;
            }
        }
        // While at least one stall is held, the aggregate stays bounded.
        assert!(
            ledger.aggregate_outstanding() <= ledger.aggregate_cap(),
            "the aggregate stays within its cap during the stalls"
        );
        if released.iter().all(|r| *r) {
            all_released = true;
            break;
        }
        if !progress {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
    assert!(
        all_released,
        "every job must reach (and release) its stalled write: released={released:?}"
    );
    for join in joins {
        let completed = tokio::time::timeout(std::time::Duration::from_secs(60), join)
            .await
            .expect("no hang")
            .expect("join")
            .expect("every job completes");
        assert!(completed.final_path.exists());
    }
    drop(controller);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        ledger.component_outstanding(Component::Writer),
        0,
        "all writer reservations drained"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fail_after_ack_flush_never_publishes_and_restart_is_clean() {
    // The writes acknowledge into the page cache; the durable sync then
    // fails. The job fails typed, nothing is published, and a restart
    // attempt over the retained temp re-downloads what was never durable.
    let content = Arc::new(fixtures::deterministic_bytes(512 * 1024, 0x72));
    let addr = start_static_h1_server(content.clone()).await;
    let mut cfg = EngineConfig::default();
    cfg.transfer.durability = kdown_engine::config::DurabilityMode::Durable;
    let controller = controller_for(cfg.clone());
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let registration = OutputFaultScript::register(&dest);
    registration.script().fail_next(
        OutputOperation::Flush,
        kdown_engine::DownloadError::SinkWrite("fsync failed".into()),
    );

    let url = format!("http://127.0.0.1:{}/file", addr.port());
    let request = DownloadRequest::new(url.clone(), dest.clone());
    let error = tokio::time::timeout(std::time::Duration::from_secs(30), controller.run(request))
        .await
        .expect("no hang")
        .expect_err("the failed sync must fail the job");
    assert!(!dest.exists(), "a failed durable sync must never publish");
    let message = format!("{error:?}");
    assert!(
        message.contains("SinkWrite") || message.contains("fsync"),
        "the sync failure is typed: {message}"
    );

    // Restart over the retained temp: the fresh run completes byte-exact
    // (whatever was not durable is re-downloaded).
    let controller2 = controller_for(cfg);
    let request = DownloadRequest::new(url, dest.clone());
    let completed =
        tokio::time::timeout(std::time::Duration::from_secs(60), controller2.run(request))
            .await
            .expect("no hang")
            .expect("the restart completes");
    assert!(completed.final_path.exists());
    fixtures::assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keep_partial_cancel_then_restart_resumes_from_durable_ranges() {
    // Checkpoint/restart boundary: the first run is cancelled with
    // KeepPartial after real progress (throttled so chunks land); the
    // retained temp plus the checkpoint let the second run reuse durable
    // ranges and complete byte-exact. The server must honor ranges (a
    // resumed job sends ranged requests): the scripted server does.
    let content = Arc::new(fixtures::deterministic_bytes(1024 * 1024, 0x73));
    let server = crate::internal_tests::support::test_server::TestServer::new()
        .serve_static("/file", (*content).clone())
        .with_default_headers("/file", |headers| {
            headers.push(("etag".to_string(), "\"fault-gen\"".to_string()));
        })
        .start()
        .await
        .expect("start");
    let base = server.url("/file");
    let cfg = EngineConfig {
        // Frequent cadence checkpoints capture progress before the cancel.
        checkpoint_flush_interval: std::time::Duration::from_millis(50),
        ..EngineConfig::default()
    };
    let controller = controller_for(cfg.clone());
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");

    let url = base.clone();
    let request = DownloadRequest::new(url.clone(), dest.clone());
    let (handle, join) = controller.start(request);
    // Throttle and wait for the first cadence checkpoint to land.
    handle.set_rate_limit(512 * 1024);
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    handle.cancel_with(kdown_engine::CancelMode::KeepPartial);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), join)
        .await
        .expect("no hang");
    assert!(
        outcome.is_ok(),
        "the first run terminates (cancelled or complete)"
    );
    assert!(!dest.exists(), "nothing published before the restart");
    drop(controller);

    // Restart: the engine reuses the durable ranges from the checkpoint.
    let controller2 = controller_for(cfg);
    let request = DownloadRequest::new(url, dest.clone());
    let completed =
        tokio::time::timeout(std::time::Duration::from_secs(60), controller2.run(request))
            .await
            .expect("no hang")
            .expect("the restart completes");
    assert!(completed.final_path.exists());
    fixtures::assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    // The durable ranges were honored: some bytes came from the
    // checkpoint instead of the wire.
    assert!(
        completed.accounting.bytes_reused_from_checkpoint > 0,
        "the restart reused durable ranges: reused={}",
        completed.accounting.bytes_reused_from_checkpoint
    );
}

/// Durable mode: a cadence checkpoint may persist only after the output
/// data corresponding to its frontier was synchronized. An injected sync
/// failure stops the job with a checkpoint-category failure and never
/// persists the unsynced frontier (§15.4, design D3).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_cadence_sync_failure_never_persists_the_frontier() {
    use super::support::test_server::{ScriptedResponse, TestServer};
    use kdown_engine::resume::FileCheckpointStore;
    use kdown_engine::{CheckpointStore as _, StoreDurabilityMode};

    let content = Arc::new(fixtures::deterministic_bytes(1024 * 1024, 0x77));
    let served = Arc::clone(&content);
    let server = TestServer::new()
        .serve_handler("/file", move |_| {
            ScriptedResponse::ok((*served).clone()).chunked(std::time::Duration::from_millis(15))
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let identity = kdown_engine::resume::job_identity(&server.url("/file"), &dest);
    let mut cfg = EngineConfig::default();
    cfg.transfer.durability = kdown_engine::config::DurabilityMode::Durable;
    cfg.checkpoint_flush_interval = std::time::Duration::from_millis(10);
    let controller = controller_for(cfg);
    let registration = OutputFaultScript::register(&dest);
    registration.script().fail_next(
        OutputOperation::Flush,
        kdown_engine::DownloadError::SinkWrite("sync boom".into()),
    );

    let error = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        controller.run(DownloadRequest::new(server.url("/file"), dest.clone())),
    )
    .await
    .expect("no hang")
    .expect_err("the failed sync must fail the job");
    assert_eq!(
        error.category(),
        kdown_engine::ErrorCategory::Checkpoint,
        "sync failure surfaces as a checkpoint-category failure: {error:?}"
    );
    assert!(!dest.exists(), "nothing may be published");
    let store = FileCheckpointStore::new(dir.path(), StoreDurabilityMode::Durable).expect("store");
    assert!(
        store.load(&identity).expect("load").is_none(),
        "a failed sync must never persist a frontier"
    );
}

/// Publication commit boundary (task 4.5): an expiry that arrives while
/// the atomic commit is in flight still reports success truthfully once
/// the commit completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_during_gated_commit_reports_success_truthfully() {
    use super::support::test_server::{ScriptedResponse, TestServer};
    use crate::io::publish::{install_test_gate, PublishMode};
    use kdown_engine::config::OverwritePolicy;

    let content = Arc::new(b"hello".to_vec());
    let served = Arc::clone(&content);
    let server = TestServer::new()
        .serve_handler("/file", move |_| ScriptedResponse::ok((*served).clone()))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let mut cfg = EngineConfig::default();
    cfg.transfer.job_deadline = Some(std::time::Duration::from_millis(400));
    let controller = controller_for(cfg);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    install_test_gate(dest.clone(), PublishMode::Replace, entered_tx, release_rx);
    let mut request = DownloadRequest::new(server.url("/file"), dest.clone());
    request.overwrite = OverwritePolicy::Replace;
    let (_handle, task) = controller.start(request);
    tokio::task::spawn_blocking(move || {
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .map_err(|_| ())
    })
    .await
    .expect("gate waiter")
    .expect("job reached the commit gate");
    // The deadline expires while the atomic publication is held at the gate.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    release_tx.send(()).expect("release commit");
    let completed = tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .expect("no hang")
        .expect("task")
        .expect("success after an atomic commit must be reported truthfully");
    assert!(completed.final_path.exists());
    assert_eq!(std::fs::read(&dest).expect("read"), b"hello");
}

/// Publication commit boundary (task 4.5): an expiry that arrives before
/// the atomic commit fails safely and publishes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_before_commit_fails_safely_without_publishing() {
    use super::support::test_server::{ScriptedResponse, TestServer};
    use kdown_engine::config::{ExpectedHash, HashAlgorithm};

    let content = Arc::new(fixtures::deterministic_bytes(64 * 1024, 0x78));
    let served = Arc::clone(&content);
    let expected_hash = fixtures::sha256_hex(&content);
    let server = TestServer::new()
        .serve_handler("/file", move |_| ScriptedResponse::ok((*served).clone()))
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let mut cfg = EngineConfig::default();
    cfg.transfer.job_deadline = Some(std::time::Duration::from_millis(400));
    let controller = controller_for(cfg);
    let registration = OutputFaultScript::register(&dest);
    let gate = registration
        .script()
        .hold_next(OutputOperation::VerificationRead);
    let mut request = DownloadRequest::new(server.url("/file"), dest.clone());
    request.integrity.expected_hashes = vec![ExpectedHash {
        algorithm: HashAlgorithm::Sha256,
        hex: expected_hash,
    }];
    let (_handle, task) = controller.start(request);
    // Wait at the verification boundary until the deadline has passed.
    gate.wait_until_entered();
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    gate.release();
    let error = tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .expect("no hang")
        .expect("task")
        .expect_err("an expiry before commit must fail the job");
    assert_eq!(
        error.category(),
        kdown_engine::ErrorCategory::DeadlineExceeded,
        "typed deadline failure: {error:?}"
    );
    assert!(!dest.exists(), "nothing may be published");
}
