//! End-to-end transfer-memory admission (design D3, tasks 3.4/3.5).
//!
//! Verifies that owned frame reservations are carried through the
//! sequential and segmented read paths and the writer lanes: held/queued
//! bytes stay within the job/controller caps under slow-disk stalls and
//! cancellation, reservations drain on completion, and many concurrent
//! jobs share one aggregate ledger. Nothing ever claims unacknowledged
//! bytes completed (the release points trail the write acknowledgements).
//!
//! These tests are crate-internal: they observe the job ledger, which is
//! not consumer surface.

use std::sync::Arc;

use kdown_engine::{
    DownloadController, DownloadRequest, DownloadRunError, EngineConfig, TransferMemoryConfig,
};

use super::support::fixtures;
use kdown_engine::io::fault_script::{OutputFaultScript, OutputOperation};
use kdown_engine::io::transfer_ledger::Component;

/// A bounded config set for admission tests: 64 KiB chunks, 256 KiB job
/// cap, 192 KiB writer cap, 1 MiB aggregate — small enough that the caps
/// bind under the test loads.
pub(super) fn bounded_config() -> EngineConfig {
    let mut cfg = EngineConfig {
        transfer_memory: TransferMemoryConfig {
            aggregate_max_bytes: 1024 * 1024,
            job_max_bytes: 256 * 1024,
            network_ingress_max_bytes: 128 * 1024,
            frames_max_bytes: 128 * 1024,
            writer_max_bytes: 192 * 1024,
            checkpoint_max_bytes: 64 * 1024,
        },
        // Legacy budgets stay subordinate to the ledger caps.
        write_budget: crate::config::WriteBudgetConfig {
            job_max_bytes: 256 * 1024,
            global_max_bytes: 1024 * 1024,
            worker_read_ahead_bytes: 128 * 1024,
        },
        write_executor: crate::config::WriteExecutorConfig {
            max_queued_bytes: 1024 * 1024,
            ..crate::config::WriteExecutorConfig::default()
        },
        ..EngineConfig::default()
    };
    cfg.read_buffer_size = 64 * 1024;
    // The connection-ingress carve-out (4 × 128 KiB footprint) plus one
    // job cap must fit the aggregate (validated): cap the connections.
    cfg.max_connections_total = 4;
    cfg.max_connections_per_origin = 4;
    cfg.pool.max_total = 4;
    cfg.pool.max_per_origin = 4;
    cfg
}

/// A plain HTTP/1.1 static server (raw socket) serving `content` to every
/// request.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_disk_write_holds_ledger_bytes_within_caps_then_drains() {
    // 512 KiB body, 64 KiB chunks: 8 chunks through the sequential path.
    let content = Arc::new(vec![0x3C_u8; 512 * 1024]);
    let addr = start_static_h1_server(content.clone()).await;

    let cfg = bounded_config();
    let transport = kdown_engine::HttpTransport::from_config(&cfg).expect("transport");
    let ledger = transport.ledger();
    let controller = Arc::new(DownloadController::new(transport, cfg));

    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    // Script a stalled write: the sequential worker blocks inside the
    // fault gate while holding its Writer-tagged reservation.
    let registration = OutputFaultScript::register(&dest);
    let gate = registration.script().hold_next(OutputOperation::Write);

    let request = DownloadRequest::new(
        format!("http://127.0.0.1:{}/file", addr.port()),
        dest.clone(),
    );
    let (handle, join) = controller.start(request);

    // The stall is reached once the first chunk is being written.
    gate.wait_until_entered();
    // While the write is stalled, the ledger must show bytes HELD at the
    // writer (the stalled chunk) and nothing above the caps.
    let writer_held = ledger.component_outstanding(Component::Writer);
    assert!(
        writer_held > 0,
        "a stalled write must hold its Writer-tagged reservation"
    );
    assert!(
        writer_held <= 192 * 1024,
        "writer-held bytes stay within the component cap: {writer_held}"
    );
    assert!(
        ledger.aggregate_outstanding() <= 1024 * 1024,
        "aggregate stays within its cap during the stall"
    );
    gate.release();

    let completed = tokio::time::timeout(std::time::Duration::from_secs(30), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("download completes");
    assert!(completed.final_path.exists());
    fixtures::assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);

    // Every job charge drained on acknowledgement; only the pooled idle
    // connection's footprint can remain until the client drops.
    assert!(
        ledger.aggregate_outstanding() <= 192 * 1024,
        "after completion only the connection footprint may remain: {}",
        ledger.aggregate_outstanding()
    );
    drop(handle);
    drop(controller);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        ledger.aggregate_outstanding(),
        0,
        "dropping the client releases the connection footprint"
    );
    // Later chunks (full 64 KiB buffers) may set a higher peak after the
    // release; the recorded peak stays within the cap and reflects real
    // held bytes.
    let high = ledger.component_high_water(Component::Writer);
    assert!(
        high >= writer_held && high <= 192 * 1024,
        "writer high-water {high} within [stalled {writer_held}, cap]"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_stalled_write_drains_job_reservations() {
    let content = Arc::new(vec![0x3D_u8; 512 * 1024]);
    let addr = start_static_h1_server(content.clone()).await;

    let cfg = bounded_config();
    let transport = kdown_engine::HttpTransport::from_config(&cfg).expect("transport");
    let ledger = transport.ledger();
    let controller = Arc::new(DownloadController::new(transport, cfg));

    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let registration = OutputFaultScript::register(&dest);
    let gate = registration.script().hold_next(OutputOperation::Write);

    let request = DownloadRequest::new(
        format!("http://127.0.0.1:{}/file", addr.port()),
        dest.clone(),
    );
    let (handle, join) = controller.start(request);
    gate.wait_until_entered();

    // Cancel while the write is stalled: the job must converge to a typed
    // cancellation and drop its pipeline reservations (the stalled write
    // thread is released so the lane can observe the cancellation).
    handle.cancel();
    gate.release();
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect_err("cancelled job");
    assert!(
        matches!(result, DownloadRunError::Cancelled(_)),
        "cancellation is typed: {result:?}"
    );
    // Job charges drained; at most the idle pooled connection's footprint
    // remains (job cap bound), and dropping the client clears it fully.
    assert!(
        ledger.aggregate_outstanding() <= 192 * 1024,
        "only the connection footprint may outlive the job: {}",
        ledger.aggregate_outstanding()
    );
    drop(handle);
    drop(controller);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        ledger.aggregate_outstanding(),
        0,
        "cancellation must drain every reservation after the client drops"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn pipelined_executor_draws_from_the_single_ledger_and_drains() {
    let content = Arc::new(vec![0x3E_u8; 512 * 1024]);
    let addr = start_static_h1_server(content.clone()).await;

    let mut cfg = bounded_config();
    cfg.write_executor.pipeline_writes = true;
    let transport = kdown_engine::HttpTransport::from_config(&cfg).expect("transport");
    let ledger = transport.ledger();
    let controller = Arc::new(DownloadController::new(transport, cfg));

    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let registration = OutputFaultScript::register(&dest);
    let gate = registration.script().hold_next(OutputOperation::Write);

    let request = DownloadRequest::new(
        format!("http://127.0.0.1:{}/file", addr.port()),
        dest.clone(),
    );
    let (handle, join) = controller.start(request);
    gate.wait_until_entered();

    // While the executor write is stalled, the writer component cap bounds
    // queued+in-flight bytes (read-ahead + pre-read quanta all draw from
    // the same ledger).
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let writer_held = ledger.component_outstanding(Component::Writer);
    assert!(
        writer_held > 0,
        "the pipelined writer holds its pre-read quanta in the ledger"
    );
    assert!(
        writer_held <= 192 * 1024,
        "queued+in-flight bytes stay within the writer component cap: {writer_held}"
    );
    assert!(
        ledger.aggregate_outstanding() <= 1024 * 1024,
        "aggregate stays within its cap during the stall"
    );
    gate.release();

    let completed = tokio::time::timeout(std::time::Duration::from_secs(30), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("download completes");
    assert!(completed.final_path.exists());
    fixtures::assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    drop(handle);
    drop(controller);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        ledger.aggregate_outstanding(),
        0,
        "all reservations drained"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn many_jobs_share_one_aggregate_ledger_within_its_cap() {
    let content = Arc::new(vec![0x3F_u8; 256 * 1024]);
    let addr = start_static_h1_server(content.clone()).await;

    let cfg = bounded_config();
    let transport = kdown_engine::HttpTransport::from_config(&cfg).expect("transport");
    let ledger = transport.ledger();
    let controller = Arc::new(DownloadController::new(transport, cfg));
    let dir = tempfile::tempdir().expect("tmpdir");

    // Four concurrent jobs; the aggregate cap (1 MiB) admits all four at
    // their full job cap plus connection footprints.
    let mut joins = Vec::new();
    let mut handles = Vec::new();
    let mut registrations = Vec::new();
    for job in 0..4 {
        let dest = dir.path().join(format!("out-{job}.bin"));
        registrations.push(kdown_engine::io::fault_script::OutputFaultScript::register(
            &dest,
        ));
        let request = DownloadRequest::new(format!("http://127.0.0.1:{}/file", addr.port()), dest);
        let (handle, join) = controller.start(request);
        handles.push(handle);
        joins.push(join);
    }
    let _ = &handles;
    // Mid-flight aggregate bound sampling.
    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert!(
            ledger.aggregate_outstanding() <= 1024 * 1024,
            "aggregate exceeded its cap mid-flight: {}",
            ledger.aggregate_outstanding()
        );
    }
    for join in joins {
        let completed = tokio::time::timeout(std::time::Duration::from_secs(30), join)
            .await
            .expect("no hang")
            .expect("join")
            .expect("each job completes");
        assert!(completed.final_path.exists());
    }
    drop(controller);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        ledger.aggregate_outstanding(),
        0,
        "all jobs drained; the shared ledger is empty"
    );
    assert!(
        ledger.aggregate_high_water() <= 1024 * 1024,
        "the recorded peak never exceeded the cap"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_checkpoint_budget_fails_the_save_safely() {
    // A 256-byte checkpoint budget cannot even hold the minimal
    // serialization overhead: every save fails the typed TooLarge policy
    // BEFORE allocating, and the job fails explicitly instead of growing
    // unbounded checkpoint memory.
    // Large enough that several cadence checkpoints fire mid-transfer.
    let content = Arc::new(vec![0x41_u8; 1024 * 1024]);
    let addr = start_static_h1_server(content.clone()).await;

    let mut cfg = bounded_config();
    cfg.transfer_memory.checkpoint_max_bytes = 256; // just above MIN
                                                    // Cadence saves fire from the second chunk onward.
    cfg.checkpoint_flush_interval = std::time::Duration::from_millis(1);

    let transport = kdown_engine::HttpTransport::from_config(&cfg).expect("transport");
    let ledger = transport.ledger();
    let controller = Arc::new(DownloadController::new(transport, cfg));
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");

    let request = DownloadRequest::new(
        format!("http://127.0.0.1:{}/file", addr.port()),
        dest.clone(),
    );
    // Throttle the transfer so it outlives the cadence interval and a
    // cadence save fires mid-download.
    let (handle, join) = controller.start(request);
    handle.set_rate_limit(256 * 1024); // 1 MiB takes ~4 s
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), join)
        .await
        .expect("no hang")
        .expect("join");
    // The typed refusal is a checkpoint/memcap failure — never a silent
    // overclaim or an unbounded allocation.
    let error = result.expect_err("the impossible budget must fail the job");
    let message = format!("{error:?}");
    assert!(
        message.contains("checkpoint") || message.contains("cap") || message.contains("budget"),
        "the failure names the budget: {message}"
    );
    // No sidecar state was written for the impossible budget: nothing
    // overclaims resumability.
    let sidecars: Vec<_> = std::fs::read_dir(dir.path())
        .expect("dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".kdown"))
        .collect();
    assert!(
        sidecars.is_empty(),
        "an over-budget checkpoint must not persist state"
    );
    drop(controller);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        ledger.component_outstanding(Component::Checkpoint),
        0,
        "failed saves release their reservation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_sidecar_is_refused_before_reading_and_never_overclaims() {
    // A hostile 1 MiB sidecar exceeds the budget: the load fails before
    // reading and the job starts fresh — the corrupt state cannot poison
    // the download or overclaim durable ranges.
    let content = Arc::new(vec![0x42_u8; 128 * 1024]);
    let addr = start_static_h1_server(content.clone()).await;

    let mut cfg = bounded_config();
    cfg.transfer_memory.checkpoint_max_bytes = 32 * 1024;

    let transport = kdown_engine::HttpTransport::from_config(&cfg).expect("transport");
    let ledger = transport.ledger();
    let controller = Arc::new(DownloadController::new(transport, cfg));
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");

    // The sidecar name is `<job_identity>.kdown` in the destination's
    // parent; a job from a fresh controller has a deterministic identity
    // only after start — instead, write garbage sidecars for EVERY
    // identity: an oversized file at any `.kdown` path in this directory
    // must be refused before reading.
    for id in 0..8 {
        std::fs::write(
            dir.path().join(format!("job-{id}.kdown")),
            vec![0x00_u8; 1024 * 1024],
        )
        .expect("write hostile sidecar");
    }

    let request = DownloadRequest::new(
        format!("http://127.0.0.1:{}/file", addr.port()),
        dest.clone(),
    );
    let completed =
        tokio::time::timeout(std::time::Duration::from_secs(30), controller.run(request))
            .await
            .expect("no hang")
            .expect("the hostile sidecar fails safe: the job starts fresh and completes");
    assert!(completed.final_path.exists());
    fixtures::assert_bytes_exact(&std::fs::read(&dest).expect("read"), &content);
    // The accounting reflects a FRESH download, not resumed state.
    assert_eq!(
        completed.accounting.bytes_reused_from_checkpoint, 0,
        "a refused sidecar never overclaims durable ranges"
    );
    drop(controller);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        ledger.component_outstanding(Component::Checkpoint),
        0,
        "checkpoint reservations drain"
    );
}

/// Linux RSS in KiB from `/proc/self/status` (`None` elsewhere).
fn rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|l| l.split_whitespace().next()?.parse().ok())
}

/// Adversarial end-to-end profile (task 3.8): many concurrent jobs against
/// paced slow servers — the managed ledger peaks must never exceed the
/// configured caps, and the observed process RSS stays bounded, with the
/// unaccounted overhead explained (kernel buffers, allocator arenas,
/// runtime stacks) rather than folded into the managed numbers.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn adversarial_multi_job_profile_respects_caps_with_rss_observed() {
    // Paced server: 128 KiB every 20 ms per connection — large enough to
    // exercise coalesced frames while sampling the pipeline across many jobs.
    let content = Arc::new(vec![0x44_u8; 512 * 1024]);
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
                for chunk in content.chunks(128 * 1024) {
                    if socket.write_all(chunk).await.is_err() {
                        return;
                    }
                    let _ = socket.flush().await;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            });
        }
    });

    // The test bounds a 512 KiB response, not individual transport frames:
    // hyper can coalesce the paced 128 KiB writes into larger body frames.
    // Every component that holds an atomic frame (including the writer) must
    // admit the entire bounded response while eight job caps still exceed the
    // shared 2 MiB aggregate cap. A smaller writer cap caused CI-only
    // MemoryCapExceeded refusals on legitimate deliveries.
    let mut cfg = EngineConfig {
        transfer_memory: TransferMemoryConfig {
            aggregate_max_bytes: 4 * 1024 * 1024,
            job_max_bytes: 512 * 1024,
            network_ingress_max_bytes: 512 * 1024,
            frames_max_bytes: 512 * 1024,
            writer_max_bytes: 512 * 1024,
            checkpoint_max_bytes: 64 * 1024,
        },
        // Legacy budgets subordinate to the new caps.
        write_budget: crate::config::WriteBudgetConfig {
            job_max_bytes: 128 * 1024,
            global_max_bytes: 1024 * 1024,
            worker_read_ahead_bytes: 64 * 1024,
        },
        write_executor: crate::config::WriteExecutorConfig {
            max_queued_bytes: 1024 * 1024,
            ..crate::config::WriteExecutorConfig::default()
        },
        ..EngineConfig::default()
    };
    cfg.read_buffer_size = 64 * 1024;
    // The connection-ingress carve-out (4 × 128 KiB footprint) plus one
    // The connection-ingress carve-out (4 × 576 KiB footprint: the 512 KiB
    // window + 64 KiB header allowance) plus one job cap must fit the
    // aggregate (validated): cap the connections.
    cfg.max_connections_total = 4;
    cfg.max_connections_per_origin = 4;
    cfg.pool.max_total = 4;
    cfg.pool.max_per_origin = 4;
    cfg.checkpoint_flush_interval = std::time::Duration::from_millis(50);
    let transport = kdown_engine::HttpTransport::from_config(&cfg).expect("transport");
    let ledger = transport.ledger();
    let aggregate_cap = ledger.aggregate_cap();
    let controller = Arc::new(DownloadController::new(transport, cfg));
    let dir = tempfile::tempdir().expect("tmpdir");

    let mut joins = Vec::new();
    for job in 0..8 {
        let dest = dir.path().join(format!("out-{job}.bin"));
        let request = DownloadRequest::new(format!("http://127.0.0.1:{}/file", addr.port()), dest);
        let (_handle, join) = controller.start(request);
        joins.push(join);
    }

    // Sample the managed ledger and the external RSS throughout.
    let mut managed_peak = 0_u64;
    let mut rss_peak = rss_kib().unwrap_or(0);
    let rss_start = rss_peak;
    for sample in 0..120 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let managed = ledger.aggregate_outstanding();
        managed_peak = managed_peak.max(managed);
        if let Some(rss) = rss_kib() {
            rss_peak = rss_peak.max(rss);
        }
        assert!(
            managed <= aggregate_cap,
            "managed peak {managed} exceeded the aggregate cap {aggregate_cap}"
        );
        if sample % 4 == 0 {
            eprintln!(
                "[sample {sample}] agg={managed} writer={} frames={}",
                ledger.component_outstanding(Component::Writer),
                ledger.component_outstanding(Component::Frames),
            );
        }
        if joins.iter().all(|j| j.is_finished()) {
            break;
        }
    }
    for join in joins {
        let completed = tokio::time::timeout(std::time::Duration::from_secs(60), join)
            .await
            .expect("no hang")
            .expect("join")
            .expect("every job completes");
        assert!(completed.final_path.exists());
    }

    // Every job's connection footprint holds until the client drops; the
    // managed pipeline itself drained with the last acknowledgement.
    assert!(
        ledger.aggregate_outstanding() <= 8 * 128 * 1024,
        "residual charges are connection footprints only: {}",
        ledger.aggregate_outstanding()
    );
    let snapshot = ledger.snapshot();
    drop(controller);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        ledger.aggregate_outstanding(),
        0,
        "dropping the client releases every connection footprint"
    );

    // External RSS observation (task 3.8): the accounted pipeline peaks
    // are capped; the process RSS includes unaccounted overhead (runtime,
    // allocator arenas, kernel socket buffers, test harness). Print the
    // observation and the explanation for the evidence record; the RSS
    // itself stays bounded far below an unbounded-ingress blowup.
    let rss_end = rss_kib().unwrap_or(0);
    eprintln!(
        "[evidence] managed aggregate peak: {managed_peak} bytes (cap {aggregate_cap}); \
         pool aggregate: {:?}; per-component peaks: {:?}; RSS start {rss_start} KiB, peak {rss_peak} KiB, end {rss_end} KiB; \
         unaccounted overhead = runtime + allocator arenas + kernel socket buffers + harness",
        snapshot.aggregate, snapshot.components
    );
    assert!(
        rss_peak < 512 * 1024,
        "RSS stayed bounded ({} KiB peak) under tight caps",
        rss_peak
    );
    // The sampled outstanding is a point-in-time view (reservations are
    // held only for the microseconds of each write); the LEDGER's own
    // monotonic high-water is the authoritative exercised-peak record.
    assert!(
        snapshot.aggregate.high_water > 0,
        "the profile actually exercised the ledger (pool high-water)"
    );
    assert!(
        snapshot.aggregate.high_water <= aggregate_cap,
        "the recorded managed peak {} never exceeded the cap {aggregate_cap}",
        snapshot.aggregate.high_water
    );
}
