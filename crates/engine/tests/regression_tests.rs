//! Named regression tests (task 4.5, `docs/regression-triage.md`).
//!
//! Every test here replays a discovered production bug deterministically
//! and carries its full triage record: observed, expected, reproduction
//! command, and the fixing change. See the process document for the
//! severity triage and seed/fixture retention rules.

use std::sync::Arc;

use kdown_engine::{
    DownloadController, DownloadRequest, DownloadRunError, EngineConfig, HttpTransport,
};

use super::support::fixtures;
use kdown_engine::io::fault_script::{OutputFaultScript, OutputOperation};

/// Plain HTTP/1.1 static server (raw socket), HEAD-honoring.
async fn start_static_h1_server(content: Arc<Vec<u8>>) -> std::net::SocketAddr {
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
    addr
}

/// Regression: a mid-transfer sink-write failure used to return to the
/// caller WITHOUT the terminal state transitions, leaving the job state
/// machine in `Running` forever (consumer-observable: a nonterminal job
/// despite a returned failure).
///
/// - **Observed** (pre-fix): `sink.write_at` errors escaped the
///   sequential loop as a raw `?`-propagated failure; the state stayed
///   `Running` instead of reaching the terminal `Failed` state.
/// - **Expected** (§9, §14.5, design D1): every terminal branch funnels
///   through `terminal_error`, performing the once-only
///   `Running -> Failing -> Failed` transition and returning a typed
///   failure classification.
/// - **Reproduction**: `cargo test -p kdown-engine --lib
///   regression_2026_09_27_sink_write_skips_terminal_transitions` — the
///   fault script injects one `DiskFull` write failure; the test asserts
///   both the typed error and the observable terminal state.
/// - **Fix**: establish-production-stability task 1.2 (every failure site
///   funnels through `terminal_error`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regression_2026_09_27_sink_write_skips_terminal_transitions() {
    let content = Arc::new(fixtures::deterministic_bytes(512 * 1024, 0x90));
    let addr = start_static_h1_server(content.clone()).await;
    let cfg = EngineConfig::default();
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let registration = OutputFaultScript::register(&dest);
    registration.script().fail_next(
        OutputOperation::Write,
        kdown_engine::DownloadError::DiskFull("regression: device full".into()),
    );

    let request = DownloadRequest::new(
        format!("http://127.0.0.1:{}/file", addr.port()),
        dest.clone(),
    );
    let (handle, join) = controller.start(request);

    let error = tokio::time::timeout(std::time::Duration::from_secs(30), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect_err("the injected write failure must fail the job");
    assert!(
        matches!(error, DownloadRunError::Infrastructure(ref f)
            if f.error.category() == kdown_engine::ErrorCategory::DiskFull),
        "typed DiskFull failure: {error:?}"
    );
    assert!(!dest.exists(), "nothing published");
    assert_eq!(
        handle.state(),
        kdown_engine::JobState::Failed,
        "a terminal write failure must not leave the job in Running"
    );
}

/// Regression: a seeded loss replay must produce identical outcomes.
/// This pins the task 4.1 replay contract for a discovered flake class:
/// network-condition replays that drifted because loss decisions were
/// keyed per-connection instead of per-response-ordinal.
///
/// - **Observed** (pre-fix): two replays of the same seed produced
///   different retry counts (loss decisions tied to connection ids, which
///   pooling varies between runs).
/// - **Expected** (task 4.1, design §36.2): identical fingerprints.
/// - **Reproduction**: `cargo test -p kdown-engine --lib
///   regression_2026_09_27_seeded_loss_replay_drift` with
///   `SEED = 0xDEAD_BEEF`. Each run starts a fresh scripted server so
///   response ordinals reset; this seed at 300 permille deterministically
///   loses the second response and causes at least one retry.
/// - **Fix**: `NetworkConditions` draws key off the global response
///   ordinal, never the connection id (task 4.1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regression_2026_09_27_seeded_loss_replay_drift() {
    // The seed is retained per `docs/regression-triage.md` §2: a named
    // constant, echoed in the failure output.
    const SEED: u64 = 0xDEAD_BEEF;
    let content = Arc::new(fixtures::deterministic_bytes(256 * 1024, 0x91));
    let conditions = super::support::test_server::NetworkConditions {
        seed: SEED,
        latency: std::time::Duration::from_millis(5),
        jitter_ms: 0,
        loss_permille: 300,
        bandwidth_bytes_per_s: 0,
        reset_first_responses: 0,
    };
    eprintln!("[regression seed] {SEED:#x}");

    let fingerprint_of =
        |dest: &std::path::Path,
         result: &Result<kdown_engine::CompletedDownload, DownloadRunError>| {
            match result {
                Ok(completed) => format!(
                    "ok {} {} {}",
                    completed.accounting.completed_bytes,
                    completed.accounting.retries,
                    fixtures::file_sha256(dest)
                ),
                Err(error) => format!(
                    "err {} {}",
                    error.accounting().retries,
                    error.accounting().completed_bytes
                ),
            }
        };

    let mut first = None;
    for run in 0..2 {
        // A new server resets the global response ordinal to zero,
        // reproducing the same seeded loss decisions for each attempt.
        let server = super::support::test_server::TestServer::new()
            .serve_static("/file", (*content).clone())
            .network(conditions)
            .start()
            .await
            .expect("start replay server");
        let dir = tempfile::tempdir().expect("tmpdir");
        let dest = dir.path().join("out.bin");
        let cfg = EngineConfig::default();
        let transport = HttpTransport::from_config(&cfg).expect("transport");
        let controller = DownloadController::new(transport, cfg);
        let request = DownloadRequest::new(server.url("/file"), dest.clone());
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(30), controller.run(request))
                .await
                .expect("no hang");
        let completed = result.as_ref().expect("the 30% loss profile recovers");
        assert!(
            completed.accounting.retries > 0,
            "seed {SEED:#x} must exercise loss"
        );
        let fingerprint = fingerprint_of(&dest, &result);
        if let Some(previous) = &first {
            assert_eq!(
                previous, &fingerprint,
                "replay {run} must match the first run (seed {SEED:#x})"
            );
        } else {
            first = Some(fingerprint);
        }
    }
}
