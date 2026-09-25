//! Phase 1 exit test (§41): randomized disconnects during sequential
//! downloads must produce byte-exact final output via restart-from-zero
//! retries.

#[path = "support/mod.rs"]
mod support;

use std::time::Duration;

use kdown_engine::config::{EngineConfig, RetryPolicy};
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::job::controller::{DownloadController, DownloadRequest, ResultStatus};
use support::fixtures::{assert_bytes_exact, deterministic_bytes};
use support::test_server::TestServer;

/// Server that kills the connection at a deterministic pseudo-random
/// offset for the first N requests, then serves the full body.
fn flaky_server(
    path: &'static str,
    content: Vec<u8>,
    failures: u32,
) -> support::test_server::TestServer {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    let hits = Arc::new(AtomicU32::new(0));
    let content = std::sync::Arc::new(content);
    TestServer::new().serve_handler(path, move |req| {
        let n = hits.fetch_add(1, Ordering::SeqCst);
        let total = (*content).len() as u64;
        let range_response = |start: u64| {
            let body = (*content)[start as usize..].to_vec();
            support::test_server::ScriptedResponse::new(206)
                .with_body(body)
                .with_header(
                    "content-range",
                    &format!("bytes {start}-{}/{total}", total - 1),
                )
                .with_header("accept-ranges", "bytes")
        };
        if req.method == "HEAD" {
            return support::test_server::ScriptedResponse::ok((*content).clone())
                .with_header("accept-ranges", "bytes");
        }
        if n < failures {
            // Kill the ranged response mid-body, always incomplete
            // relative to its declared content-length.
            let start = req.range.map(|(s, _)| s).unwrap_or(0);
            let cut = (4097usize * (n + 1) as usize)
                .saturating_sub(start as usize)
                .clamp(1, (*content).len() - start as usize);
            let mut r = range_response(start);
            r.reset_after = Some(cut);
            r
        } else if let Some((s, _e)) = req.range {
            range_response(s)
        } else {
            support::test_server::ScriptedResponse::ok((*content).clone())
                .with_header("accept-ranges", "bytes")
        }
    })
}

/// Always kills the ranged/full GET after `cut` bytes (strictly partial).
fn flaky_server_fixed_cut(
    path: &'static str,
    content: Vec<u8>,
    cut: usize,
) -> support::test_server::TestServer {
    use std::sync::Arc;
    let content = Arc::new(content);
    TestServer::new().serve_handler(path, move |req| {
        let total = (*content).len() as u64;
        let start = req.range.map(|(s, _)| s).unwrap_or(0);
        let body = (*content)[start as usize..].to_vec();
        let mut r = support::test_server::ScriptedResponse::new(206)
            .with_body(body)
            .with_header(
                "content-range",
                &format!("bytes {start}-{}/{total}", total - 1),
            )
            .with_header("accept-ranges", "bytes");
        r.reset_after = Some(cut.min((*content).len() - start as usize - 1));
        r
    })
}

fn controller() -> DownloadController {
    let cfg = EngineConfig {
        // Tight backoff so the randomized suite stays fast; enough attempts
        // for up to 3 induced kills plus slack.
        retry: RetryPolicy {
            max_attempts_per_segment: 16,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            multiplier: 1.5,
            ..RetryPolicy::default()
        },
        ..EngineConfig::default()
    };
    DownloadController::new(
        HttpTransport::new(kdown_engine::config::NetworkPolicy::default()).expect("transport"),
        cfg,
    )
}

#[tokio::test]
async fn randomized_disconnects_produce_exact_output() {
    for seed in 0..5u64 {
        let content = deterministic_bytes(512 * 1024, 10_000 + seed);
        let failure_count = 1 + (seed % 3) as u32; // 1..3 kills
        let server = flaky_server("/flaky.bin", content.clone(), failure_count)
            .start()
            .await
            .expect("start");
        let dir = tempfile::tempdir().expect("tmp");
        let dest = dir.path().join("out.bin");
        let c = controller();
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            c.run(DownloadRequest::new(server.url("/flaky.bin"), dest.clone())),
        )
        .await
        .expect("no hang")
        .expect("terminal");
        assert_eq!(
            result.status,
            ResultStatus::Completed,
            "seed {seed}: {result:?}"
        );
        let got = std::fs::read(&dest).expect("final output");
        assert_bytes_exact(&got, &content);
        assert_eq!(
            server.request_count("/flaky.bin").await,
            (failure_count + 1) as usize,
            "one request per attempt"
        );
        // No temp residue (§14.1).
        assert!(!dir.path().join("out.bin.part").exists());
    }
}

#[tokio::test]
async fn exhausted_retries_fail_structured() {
    let content = vec![1u8; 4096];
    // Kills at a fixed quarter-body offset forever: every attempt fails;
    // engine must give up with a structured error after max attempts.
    let server = flaky_server_fixed_cut("/always", content, 1024)
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmp");
    let c = controller();
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        c.run(DownloadRequest::new(
            server.url("/always"),
            dir.path().join("x"),
        )),
    )
    .await
    .expect("no hang")
    .expect("terminal");
    assert_eq!(result.status, ResultStatus::Failed);
    assert!(
        matches!(
            result.error,
            Some(kdown_engine::DownloadError::RetryExhausted { .. })
                | Some(kdown_engine::DownloadError::Connection(_))
        ),
        "structured retry-exhaustion error, got {:?}",
        result.error
    );
    assert!(!dir.path().join("x.part").exists());
}
