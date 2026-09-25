//! Cross-job origin coordination (tasks 6.2/6.3, design D6): the
//! controller-shared origin registry coordinates request admission and
//! 429/503/Retry-After feedback across jobs that share a normalized final
//! origin, while unrelated origins stay independent.

#[path = "support/mod.rs"]
mod support;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kdown_engine::config::EngineConfig;
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};
use support::fixtures::{assert_bytes_exact, deterministic_bytes};
use support::test_server::{ScriptedResponse, TestServer};

fn single_stream_cfg() -> EngineConfig {
    let mut c = EngineConfig::default();
    c.network.response_header_timeout = Duration::from_secs(30);
    c.network.read_idle_timeout = Duration::from_secs(30);
    c.retry.base_delay = Duration::from_millis(20);
    c.retry.max_delay = Duration::from_millis(100);
    c
}

fn controller(cfg: EngineConfig) -> SingleStreamController {
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    SingleStreamController::new(transport, cfg)
}

/// A throttling fixture: the first `fail_first` requests receive `status`
/// with `Retry-After: <secs>`; with `spare_heads`, HEAD probes always pass so
/// the throttle lands on the transfer request (whose feedback is keyed on the
/// final origin). Every request's arrival instant is logged for timeline
/// assertions.
struct ThrottleFixture {
    server: support::test_server::RunningServer,
    arrivals: Arc<std::sync::Mutex<Vec<Instant>>>,
}

impl ThrottleFixture {
    async fn start(
        content: Arc<Vec<u8>>,
        path: &'static str,
        fail_first: u32,
        status: u16,
        retry_after_secs: u64,
        spare_heads: bool,
    ) -> Self {
        let arrivals = Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = arrivals.clone();
        let remaining = Arc::new(AtomicU32::new(fail_first));
        let server = TestServer::new()
            .serve_handler(path, move |req| {
                log.lock().expect("arrival log").push(Instant::now());
                let total = content.len() as u64;
                let failing =
                    remaining.load(Ordering::SeqCst) > 0 && !(spare_heads && req.method == "HEAD");
                if failing {
                    remaining.fetch_sub(1, Ordering::SeqCst);
                    return ScriptedResponse::new(status)
                        .with_header("retry-after", &retry_after_secs.to_string());
                }
                if req.method == "HEAD" {
                    return ScriptedResponse::ok((*content).clone())
                        .with_header("accept-ranges", "bytes");
                }
                if let Some((s, e)) = req.range {
                    let end = e.min(total - 1);
                    ScriptedResponse::new(206)
                        .with_body(content[s as usize..=(end as usize)].to_vec())
                        .with_header("content-range", &format!("bytes {s}-{end}/{total}"))
                        .with_header("accept-ranges", "bytes")
                } else {
                    ScriptedResponse::ok((*content).clone()).with_header("accept-ranges", "bytes")
                }
            })
            .start()
            .await
            .expect("throttle fixture");
        Self { server, arrivals }
    }

    fn arrival_count(&self) -> usize {
        self.arrivals.lock().expect("arrival log").len()
    }

    /// Wait until at least `n` requests arrived (bounded poll).
    async fn wait_arrivals(&self, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.arrival_count() < n && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(self.arrival_count() >= n, "expected {n} arrivals");
    }

    fn arrival(&self, index: usize) -> Instant {
        self.arrivals.lock().expect("arrival log")[index]
    }
}

/// A 503 observed by one job delays every same-origin peer's next request:
/// the peer job starts while the coordinated window is open and its first
/// dispatch waits it out; recovery after the cooldown completes both jobs.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn throttle_on_one_job_delays_same_origin_peer() {
    let content = Arc::new(deterministic_bytes(512 * 1024, 6001));
    let fixture = ThrottleFixture::start(content.clone(), "/shared", 1, 503, 3, false).await;
    let url = fixture.server.url("/shared");
    let dir = tempfile::tempdir().expect("tmpdir");

    let c = controller(single_stream_cfg());
    let (ha, ja) = c.start(DownloadRequest::new(url.clone(), dir.path().join("a.bin")));

    // Wait until job A's probe was throttled (arrival 1), then give the
    // controller a moment to record the shared deadline before starting the
    // peer on the same origin.
    fixture.wait_arrivals(1).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let throttled_at = fixture.arrival(0);
    let (hb, jb) = c.start(DownloadRequest::new(url, dir.path().join("b.bin")));

    let ra = tokio::time::timeout(Duration::from_secs(60), ja)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal a");
    let rb = tokio::time::timeout(Duration::from_secs(60), jb)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal b");
    assert_eq!(ra.status, ResultStatus::Completed, "{:?}", ra.error);
    assert_eq!(rb.status, ResultStatus::Completed, "{:?}", rb.error);
    assert_bytes_exact(
        &std::fs::read(dir.path().join("a.bin")).expect("read a"),
        &content,
    );
    assert_bytes_exact(
        &std::fs::read(dir.path().join("b.bin")).expect("read b"),
        &content,
    );

    // Every request after the 503 — A's own retry AND the peer's first
    // dispatch — waited out the shared Retry-After window.
    let all = fixture.arrivals.lock().expect("arrival log").clone();
    assert!(
        all.len() >= 3,
        "both jobs must have made requests: {}",
        all.len()
    );
    for (i, at) in all.iter().enumerate().skip(1) {
        assert!(
            at.duration_since(throttled_at) >= Duration::from_millis(2_400),
            "request {i} after the 503 must wait out the shared window \
             (503 at {throttled_at:?}, this at {at:?})"
        );
    }
    let _ = (ha, hb);
}

/// Throttled origin A does not delay an unrelated origin B; A recovers after
/// its cooldown (recovery probe) and still completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn unrelated_origin_is_not_delayed_by_shared_backoff() {
    let content = Arc::new(deterministic_bytes(256 * 1024, 6002));
    let a = ThrottleFixture::start(content.clone(), "/slow", 1, 503, 5, false).await;
    let b = ThrottleFixture::start(content.clone(), "/fast", 0, 503, 0, false).await;

    let dir = tempfile::tempdir().expect("tmpdir");
    let c = controller(single_stream_cfg());
    let (ha, ja) = c.start(DownloadRequest::new(
        a.server.url("/slow"),
        dir.path().join("a.bin"),
    ));
    // Start B only after A's throttle was recorded, so B would inherit the
    // cooldown if origin isolation were broken.
    a.wait_arrivals(1).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let b_started = Instant::now();
    let (hb, jb) = c.start(DownloadRequest::new(
        b.server.url("/fast"),
        dir.path().join("b.bin"),
    ));

    let rb = tokio::time::timeout(Duration::from_secs(30), jb)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal b");
    let b_elapsed = b_started.elapsed();
    assert_eq!(rb.status, ResultStatus::Completed, "{:?}", rb.error);
    assert!(
        b_elapsed < Duration::from_secs(4),
        "unrelated origin must not inherit A's 5 s cooldown: {b_elapsed:?}"
    );
    // A still completes after its own cooldown (no starvation).
    let ra = tokio::time::timeout(Duration::from_secs(60), ja)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal a");
    assert_eq!(ra.status, ResultStatus::Completed, "{:?}", ra.error);
    let _ = (ha, hb);
}

/// The shared window honors the RetryClassifier cap: a server demanding an
/// hour with `retry_after_max = 1 s` coordinates for about one second, not
/// an hour.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn retry_after_is_capped_by_policy_for_the_shared_window() {
    let content = Arc::new(deterministic_bytes(256 * 1024, 6003));
    let fixture = ThrottleFixture::start(content.clone(), "/capped", 1, 503, 3600, false).await;
    let mut cfg = single_stream_cfg();
    cfg.retry.retry_after_max = Duration::from_secs(1);
    let dir = tempfile::tempdir().expect("tmpdir");

    let c = controller(cfg);
    let started = Instant::now();
    let result = c
        .run(DownloadRequest::new(
            fixture.server.url("/capped"),
            dir.path().join("out.bin"),
        ))
        .await
        .expect("run");
    let elapsed = started.elapsed();
    assert_eq!(result.status, ResultStatus::Completed, "{:?}", result.error);
    assert_bytes_exact(
        &std::fs::read(dir.path().join("out.bin")).expect("read"),
        &content,
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "Retry-After must be capped by policy for the shared window: {elapsed:?}"
    );
}

/// A persistently throttling origin exhausts the per-job retry policy with a
/// structured failure and a bounded number of requests — no unbounded
/// hammering, with or without the shared window.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn repeated_throttling_exhausts_structured_without_unbounded_requests() {
    let content = Arc::new(deterministic_bytes(64 * 1024, 6004));
    let fixture =
        ThrottleFixture::start(content.clone(), "/exhaust", u32::MAX, 429, 0, false).await;
    let mut cfg = single_stream_cfg();
    cfg.retry.max_attempts_per_segment = 3;
    let dir = tempfile::tempdir().expect("tmpdir");

    let c = controller(cfg);
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        c.run(DownloadRequest::new(
            fixture.server.url("/exhaust"),
            dir.path().join("out.bin"),
        )),
    )
    .await
    .expect("no hang")
    .expect("run");
    assert_eq!(result.status, ResultStatus::Failed, "{:?}", result.error);
    assert!(result.error.is_some(), "structured failure expected");
    // Bounded attempts (probe retries), not an infinite hammer.
    let arrivals = fixture.arrival_count();
    assert!(
        arrivals <= 12,
        "retry policy must bound attempts against a throttled origin: {arrivals}"
    );
}

/// Congestion feedback follows the FINAL redirect origin: a job throttled at
/// the redirect target coordinates a peer job that targets that origin
/// directly (never touching the redirecting host) — proof that the feedback
/// key is the final origin, not the pre-redirect address.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn throttle_feedback_follows_the_final_redirect_origin() {
    let content = Arc::new(deterministic_bytes(256 * 1024, 6005));
    // Origin B (the redirect target) throttles only non-HEAD requests, so
    // the probe passes and the throttle lands on the transfer — whose
    // feedback keys on the probe-resolved final origin.
    let b = ThrottleFixture::start(content.clone(), "/target", 1, 503, 3, true).await;
    let target_url = b.server.url("/target");
    let a = TestServer::new()
        .serve_handler("/redir", move |_req| {
            ScriptedResponse::new(302).with_header("location", &target_url)
        })
        .start()
        .await
        .expect("redirector");

    let dir = tempfile::tempdir().expect("tmpdir");
    let c = controller(single_stream_cfg());

    // Job 1 reaches B only through A's redirect. B's arrivals: [HEAD,
    // throttled GET, ...]. As soon as the throttled GET is observed, start
    // job 2 DIRECTLY against B: if the feedback were misattributed to the
    // redirecting host A, job 2 would dispatch immediately instead of
    // waiting out B's coordinated window.
    let (h1, j1) = c.start(DownloadRequest::new(
        a.url("/redir"),
        dir.path().join("one.bin"),
    ));
    b.wait_arrivals(2).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let throttled_at = b.arrival(1);
    let (h2, j2) = c.start(DownloadRequest::new(
        b.server.url("/target"),
        dir.path().join("two.bin"),
    ));

    let r1 = tokio::time::timeout(Duration::from_secs(60), j1)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal 1");
    let r2 = tokio::time::timeout(Duration::from_secs(60), j2)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal 2");
    assert_eq!(r1.status, ResultStatus::Completed, "{:?}", r1.error);
    assert_eq!(r2.status, ResultStatus::Completed, "{:?}", r2.error);
    assert_bytes_exact(
        &std::fs::read(dir.path().join("one.bin")).expect("read"),
        &content,
    );
    assert_bytes_exact(
        &std::fs::read(dir.path().join("two.bin")).expect("read"),
        &content,
    );

    // Every B arrival after the throttled GET — job 1's own retry AND job
    // 2's first dispatch — waited out the shared window.
    let all = b.arrivals.lock().expect("arrival log").clone();
    assert!(
        all.len() >= 4,
        "both jobs must have made requests: {}",
        all.len()
    );
    for (i, at) in all.iter().enumerate().skip(2) {
        assert!(
            at.duration_since(throttled_at) >= Duration::from_millis(2_400),
            "arrival {i} must wait out the shared window keyed on the FINAL origin              (503 at {throttled_at:?}, this at {at:?})"
        );
    }
    let _ = (h1, h2, a);
}
