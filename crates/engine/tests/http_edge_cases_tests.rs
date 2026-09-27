//! HTTP edge cases end-to-end (task 4.2): absent/ignored/mismatched/
//! changing ranges and validators, redirected cache/CDN paths, and
//! authenticated-proxy interplay. Every failure path must leave NO corrupt
//! published output; every success must be byte-exact and verified.

mod support;

use std::sync::Arc;

use kdown_engine::{
    config::{ExpectedHash, HashAlgorithm},
    DownloadController, DownloadRequest, DownloadRunError, EngineConfig, HttpTransport,
};

use support::fixtures;
use support::test_server::{RequestInfo, ScriptedResponse, TestServer};

/// A segmented-triggering config so range behavior is exercised.
fn segmented_config() -> EngineConfig {
    let mut cfg = EngineConfig::default();
    cfg.transfer.segmentation_threshold = 16 * 1024;
    cfg.transfer.max_workers = 4;
    cfg.transfer.min_workers = 2;
    cfg
}

fn request(url: String, dest: &std::path::Path) -> DownloadRequest {
    DownloadRequest::new(url, dest.to_path_buf())
}

/// A download that must fail typed and leave NO published output.
async fn run_failing(cfg: EngineConfig, url: String, dest: &std::path::Path) -> DownloadRunError {
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        controller.run(request(url, dest)),
    )
    .await
    .expect("no hang");
    assert!(
        !dest.exists(),
        "a failed download must never publish output at {}",
        dest.display()
    );
    result.expect_err("expected failure")
}

/// A download that must complete byte-exact.
async fn run_completing(cfg: EngineConfig, url: String, dest: &std::path::Path) -> Vec<u8> {
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let completed = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        controller.run(request(url, dest)),
    )
    .await
    .expect("no hang")
    .expect("download completes");
    assert!(completed.final_path.exists());
    std::fs::read(dest).expect("read output")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absent_ranges_download_sequentially_byte_exact() {
    // No `accept-ranges` anywhere: the engine must not attempt segmented
    // transfers; the sequential download completes byte-exact.
    let content = Arc::new(fixtures::deterministic_bytes(256 * 1024, 0x60));
    let served = Arc::clone(&content);
    let server = TestServer::new()
        .serve_handler("/plain", move |_| {
            ScriptedResponse::ok((*served).clone()) // no accept-ranges header
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_completing(segmented_config(), server.url("/plain"), &dest).await;
    fixtures::assert_bytes_exact(&output, &content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ignored_ranges_200_full_never_publishes_corrupt_output() {
    // The server advertises `accept-ranges` but answers range requests
    // with a 200 FULL body: a segmented job sees a protocol violation and
    // must fail typed (or fall back clean) — never publish shifted bytes.
    let content = fixtures::deterministic_bytes(256 * 1024, 0x61);
    let server = TestServer::new()
        .serve_ranges(
            "/lying",
            content.clone(),
            support::test_server::RangeMode::Full200,
        )
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_completing(segmented_config(), server.url("/lying"), &dest).await;
    // Whatever path the engine took (fallback or retry), the published
    // bytes are the true bytes.
    fixtures::assert_bytes_exact(&output, &content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shifted_range_bytes_fail_integrity_with_no_publication() {
    // The server answers a range request with 206 and a CORRECT
    // Content-Range header but WRONG bytes (shifted by one position):
    // with a caller-provided digest the mismatch must fail typed and the
    // corrupted bytes must never be published.
    let content = Arc::new(fixtures::deterministic_bytes(256 * 1024, 0x62));
    let served = Arc::clone(&content);
    let server = TestServer::new()
        .serve_handler("/shifted", move |req| {
            let total = served.len() as u64;
            match req.range {
                Some((s, e)) => {
                    // Advertise the requested range but serve bytes shifted
                    // by +1 (wrap the last byte around).
                    let end = e.min(total - 1);
                    let start = s.min(total - 1);
                    let mut body = served[start as usize..=(end as usize)].to_vec();
                    body.rotate_left(1);
                    ScriptedResponse::new(206)
                        .with_body(body)
                        .with_header("content-range", &format!("bytes {s}-{end}/{total}"))
                        .with_header("accept-ranges", "bytes")
                }
                None => {
                    ScriptedResponse::ok((*served).clone()).with_header("accept-ranges", "bytes")
                }
            }
        })
        .start()
        .await
        .expect("start");
    let cfg = segmented_config();
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    // The caller pins the expected digest: a shifted payload cannot match.
    let mut req = request(server.url("/shifted"), &dest);
    req.integrity.expected_hashes = vec![ExpectedHash {
        algorithm: HashAlgorithm::Sha256,
        hex: fixtures::sha256_hex(&content),
    }];
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let result = tokio::time::timeout(std::time::Duration::from_secs(60), controller.run(req))
        .await
        .expect("no hang");
    assert!(
        !dest.exists(),
        "a failed download must never publish output at {}",
        dest.display()
    );
    let error = result.expect_err("expected failure");
    let message = format!("{error:?}");
    assert!(
        message.contains("Integrity")
            || message.contains("integrity")
            || message.contains("mismatch"),
        "the shifted bytes must fail integrity verification: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn etag_change_between_probe_and_transfer_is_typed_no_publication() {
    // The ETag flips after the probe: the transfer gate must reject with
    // ResourceChanged and no output may be published.
    let content = fixtures::deterministic_bytes(64 * 1024, 0x63);
    let flipped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let server = TestServer::new()
        .serve_handler("/flipping", move |req| {
            let n = flipped.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let etag = if n == 0 { "\"gen-1\"" } else { "\"gen-2\"" };
            let total = content.len() as u64;
            match req.range {
                Some((s, e)) => {
                    let end = e.min(total - 1);
                    ScriptedResponse::new(206)
                        .with_body(content[s as usize..=(end as usize)].to_vec())
                        .with_header("content-range", &format!("bytes {s}-{end}/{total}"))
                        .with_header("etag", etag)
                        .with_header("accept-ranges", "bytes")
                }
                None => ScriptedResponse::ok(content.clone())
                    .with_header("etag", etag)
                    .with_header("accept-ranges", "bytes"),
            }
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let error = run_failing(segmented_config(), server.url("/flipping"), &dest).await;
    let message = format!("{error:?}");
    assert!(
        message.contains("ResourceChanged") || message.contains("resource"),
        "a mid-transfer generation change must be typed ResourceChanged: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redirect_chain_to_cdn_serves_byte_exact_output() {
    // Probe on the origin redirects to a CDN path; the engine follows the
    // chain and the published output is byte-exact.
    let content = fixtures::deterministic_bytes(256 * 1024, 0x64);
    let server = TestServer::new()
        .serve_handler("/origin", move |_| {
            ScriptedResponse::new(302).with_header("location", "/cdn/file.bin")
        })
        .serve_static("/cdn/file.bin", content.clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_completing(segmented_config(), server.url("/origin"), &dest).await;
    fixtures::assert_bytes_exact(&output, &content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redirect_loop_fails_bounded_with_no_publication() {
    // A self-redirecting path must trip the redirect budget: a typed
    // failure with no published output (never an infinite loop).
    let server = TestServer::new()
        .serve_handler("/loop", move |_| {
            ScriptedResponse::new(302).with_header("location", "/loop")
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let error = run_failing(EngineConfig::default(), server.url("/loop"), &dest).await;
    let message = format!("{error:?}");
    assert!(
        message.contains("Redirect") || message.contains("redirect"),
        "a redirect loop must be typed: {message}"
    );
}

// ---- Redirect credential isolation (task 1.1): two-listener regressions ----

/// Dummy credential for redirect regressions: never a real secret; it is
/// only ever offered to loopback listeners started by these tests.
const DUMMY_AUTHORIZATION: &str = "Bearer dummy-redirect-secret";

/// True when the recorded request carried an Authorization header.
fn saw_authorization(req: &RequestInfo) -> bool {
    req.headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("authorization"))
}

/// A short human-readable request log for assertion messages.
fn describe(requests: &[RequestInfo]) -> String {
    requests
        .iter()
        .map(|r| {
            format!(
                "{} {} authorization={}",
                r.method,
                r.path,
                saw_authorization(r)
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Run an authorized download to completion, returning the published bytes.
async fn run_authorized(cfg: EngineConfig, url: String, dest: &std::path::Path) -> Vec<u8> {
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let mut req = request(url, dest);
    req.authorization = Some(DUMMY_AUTHORIZATION.to_string());
    let completed = tokio::time::timeout(std::time::Duration::from_secs(60), controller.run(req))
        .await
        .expect("no hang")
        .expect("download completes");
    assert!(completed.final_path.exists());
    std::fs::read(dest).expect("read output")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absolute_cross_origin_redirect_strips_authorization() {
    // Two loopback listeners with one dummy credential: the origin receives
    // it, while the absolute redirect target must never see it on the HEAD
    // probe or the following GET, even though the redirect response carries
    // no credential headers.
    let content = fixtures::deterministic_bytes(64 * 1024, 0x65);
    let peer = TestServer::new()
        .serve_static("/file.bin", content.clone())
        .start()
        .await
        .expect("start");
    let peer_target = peer.url("/file.bin");
    let origin = TestServer::new()
        .serve_handler("/redirect", move |_| {
            ScriptedResponse::new(302).with_header("location", &peer_target)
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_authorized(EngineConfig::default(), origin.url("/redirect"), &dest).await;
    fixtures::assert_bytes_exact(&output, &content);

    let origin_requests = origin.requests().await;
    assert!(
        origin_requests.iter().any(saw_authorization),
        "the origin must receive the caller credentials: {}",
        describe(&origin_requests)
    );
    let peer_requests = peer.requests().await;
    assert!(!peer_requests.is_empty(), "redirect target must be reached");
    assert!(
        peer_requests.iter().all(|r| !saw_authorization(r)),
        "a cross-origin redirect target must never receive caller credentials: {}",
        describe(&peer_requests)
    );
    assert!(
        peer_requests.iter().any(|r| r.method == "HEAD"),
        "the probe HEAD must reach the redirect target: {}",
        describe(&peer_requests)
    );
    assert!(
        peer_requests.iter().any(|r| r.method == "GET"),
        "the transfer GET must reach the redirect target: {}",
        describe(&peer_requests)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relative_same_origin_redirect_keeps_authorization() {
    // A relative Location that resolves to the same scheme/host/port must
    // keep the caller credentials available.
    let content = fixtures::deterministic_bytes(64 * 1024, 0x66);
    let server = TestServer::new()
        .serve_handler("/move", move |_| {
            ScriptedResponse::new(302).with_header("location", "/moved.bin")
        })
        .serve_static("/moved.bin", content.clone())
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_authorized(EngineConfig::default(), server.url("/move"), &dest).await;
    fixtures::assert_bytes_exact(&output, &content);

    let requests = server.requests().await;
    let moved: Vec<_> = requests.iter().filter(|r| r.path == "/moved.bin").collect();
    assert!(!moved.is_empty(), "the relative target must be fetched");
    assert!(
        moved.iter().all(|r| saw_authorization(r)),
        "a same-origin relative redirect keeps credentials: {}",
        describe(&requests)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_hop_return_to_origin_does_not_reintroduce_credentials() {
    // Origin -> peer -> origin: the default policy latches the cross-origin
    // strip, so the credentials never come back on the return hop.
    let content = fixtures::deterministic_bytes(64 * 1024, 0x67);
    let origin_base: Arc<std::sync::OnceLock<String>> = Arc::new(std::sync::OnceLock::new());
    let peer_base: Arc<std::sync::OnceLock<String>> = Arc::new(std::sync::OnceLock::new());
    let origin_for_peer = Arc::clone(&origin_base);
    let peer = TestServer::new()
        .serve_handler("/hop", move |_| {
            let base = origin_for_peer.get().expect("origin base set").clone();
            ScriptedResponse::new(302).with_header("location", &format!("{base}/final.bin"))
        })
        .start()
        .await
        .expect("start");
    let peer_for_origin = Arc::clone(&peer_base);
    let origin = TestServer::new()
        .serve_handler("/start", move |_| {
            let base = peer_for_origin.get().expect("peer base set").clone();
            ScriptedResponse::new(302).with_header("location", &format!("{base}/hop"))
        })
        .serve_static("/final.bin", content.clone())
        .start()
        .await
        .expect("start");
    origin_base.set(origin.url("")).expect("set origin base");
    peer_base.set(peer.url("")).expect("set peer base");

    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_authorized(EngineConfig::default(), origin.url("/start"), &dest).await;
    fixtures::assert_bytes_exact(&output, &content);

    let peer_requests = peer.requests().await;
    assert!(!peer_requests.is_empty(), "the peer hop must be reached");
    assert!(
        peer_requests.iter().all(|r| !saw_authorization(r)),
        "the first cross-origin hop must be credential-free: {}",
        describe(&peer_requests)
    );
    let final_requests: Vec<_> = origin
        .requests()
        .await
        .into_iter()
        .filter(|r| r.path == "/final.bin")
        .collect();
    assert!(!final_requests.is_empty(), "the return hop must be fetched");
    assert!(
        final_requests.iter().all(|r| !saw_authorization(r)),
        "returning to the original origin must not reintroduce stripped credentials: {}",
        describe(&final_requests)
    );
    let start_requests: Vec<_> = origin
        .requests()
        .await
        .into_iter()
        .filter(|r| r.path == "/start")
        .collect();
    assert!(
        start_requests.iter().any(saw_authorization),
        "the initial same-origin request keeps credentials: {}",
        describe(&start_requests)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_origin_get_redirect_without_probe_redirect_strips_authorization() {
    // The probe is same-origin and succeeds; only the transfer GET is
    // redirected cross-origin. The strip must follow the outgoing request
    // credential scope, not the probe's redirect history.
    let content = fixtures::deterministic_bytes(64 * 1024, 0x68);
    let peer = TestServer::new()
        .serve_static("/file.bin", content.clone())
        .start()
        .await
        .expect("start");
    let peer_target = peer.url("/file.bin");
    let prober_body = content.clone();
    let origin = TestServer::new()
        .serve_handler("/split", move |req| {
            if req.method == "HEAD" {
                ScriptedResponse::ok(prober_body.clone())
            } else {
                ScriptedResponse::new(302).with_header("location", &peer_target)
            }
        })
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_authorized(EngineConfig::default(), origin.url("/split"), &dest).await;
    fixtures::assert_bytes_exact(&output, &content);

    let peer_requests = peer.requests().await;
    assert!(!peer_requests.is_empty(), "the GET redirect target must be reached");
    assert!(
        peer_requests.iter().all(|r| r.method == "GET"),
        "only the transfer GET is redirected: {}",
        describe(&peer_requests)
    );
    assert!(
        peer_requests.iter().all(|r| !saw_authorization(r)),
        "a GET-only cross-origin redirect must not leak credentials: {}",
        describe(&peer_requests)
    );
    let probe_requests: Vec<_> = origin
        .requests()
        .await
        .into_iter()
        .filter(|r| r.method == "HEAD")
        .collect();
    assert!(
        probe_requests.iter().any(saw_authorization),
        "the same-origin probe keeps credentials: {}",
        describe(&probe_requests)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_opt_in_forwards_credentials_cross_origin() {
    // `forward_credentials_cross_origin` is an explicit, documented opt-in:
    // with it enabled the redirect target receives the credentials.
    let content = fixtures::deterministic_bytes(64 * 1024, 0x69);
    let peer = TestServer::new()
        .serve_static("/file.bin", content.clone())
        .start()
        .await
        .expect("start");
    let peer_target = peer.url("/file.bin");
    let origin = TestServer::new()
        .serve_handler("/redirect", move |_| {
            ScriptedResponse::new(302).with_header("location", &peer_target)
        })
        .start()
        .await
        .expect("start");
    let mut cfg = EngineConfig::default();
    cfg.network.forward_credentials_cross_origin = true;
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_authorized(cfg, origin.url("/redirect"), &dest).await;
    fixtures::assert_bytes_exact(&output, &content);
    let peer_requests = peer.requests().await;
    assert!(
        peer_requests.iter().any(saw_authorization),
        "explicit opt-in forwards credentials to the redirect target: {}",
        describe(&peer_requests)
    );
}

// ---- Protected-resource authentication parity (task 1.3) ----

/// Range-capable handler requiring `value` in the `header` header;
/// answers a 401 challenge otherwise.
fn token_protected_handler(
    content: Arc<Vec<u8>>,
    header: &str,
    value: &str,
) -> impl Fn(&RequestInfo) -> ScriptedResponse + Send + Sync + 'static {
    let header = header.to_ascii_lowercase();
    let value = value.to_string();
    move |req: &RequestInfo| {
        let authorized = req.header(&header).is_some_and(|seen| seen == value);
        if !authorized {
            return ScriptedResponse::new(401)
                .with_header("www-authenticate", "Bearer realm=\"regression\"");
        }
        let total = content.len() as u64;
        match req.range {
            Some((s, e)) => {
                let end = e.min(total - 1);
                ScriptedResponse::new(206)
                    .with_body(content[s as usize..=(end as usize)].to_vec())
                    .with_header("content-range", &format!("bytes {s}-{end}/{total}"))
            }
            None => ScriptedResponse::ok((*content).clone()),
        }
    }
}

fn accept_ranges_header(headers: &mut Vec<(String, String)>) {
    headers.push(("accept-ranges".to_string(), "bytes".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_protected_resource_authenticates_every_request() {
    let content = Arc::new(fixtures::deterministic_bytes(128 * 1024, 0x6a));
    let server = TestServer::new()
        .serve_handler(
            "/protected.bin",
            token_protected_handler(content.clone(), "authorization", DUMMY_AUTHORIZATION),
        )
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_authorized(EngineConfig::default(), server.url("/protected.bin"), &dest).await;
    fixtures::assert_bytes_exact(&output, &content);
    let requests = server.requests().await;
    assert!(!requests.is_empty());
    assert!(
        requests.iter().all(saw_authorization),
        "a sequential protected download must authenticate every request: {}",
        describe(&requests)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segmented_protected_resource_authenticates_every_range_get() {
    let content = Arc::new(fixtures::deterministic_bytes(256 * 1024, 0x6b));
    let server = TestServer::new()
        .serve_handler(
            "/protected.bin",
            token_protected_handler(content.clone(), "authorization", DUMMY_AUTHORIZATION),
        )
        .with_default_headers("/protected.bin", accept_ranges_header)
        .start()
        .await
        .expect("start");
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let output = run_authorized(segmented_config(), server.url("/protected.bin"), &dest).await;
    fixtures::assert_bytes_exact(&output, &content);
    let requests = server.requests().await;
    let gets: Vec<_> = requests.iter().filter(|r| r.method == "GET").collect();
    assert!(!gets.is_empty(), "segmented GETs must run: {}", describe(&requests));
    assert!(
        gets.iter().all(|r| saw_authorization(r)),
        "every segmented range GET must carry the configured authorization: {}",
        describe(&requests)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn credential_provider_token_reaches_segmented_workers() {
    // The probe is challenged, the provider supplies a token, and every
    // ranged worker GET must then authenticate with it (bounded stages).
    use kdown_engine::control::auth::{provider_fn, Challenge, CredentialDecision};

    const PROVIDER_TOKEN: &str = "sentinel-provider-token";
    let content = Arc::new(fixtures::deterministic_bytes(256 * 1024, 0x6c));
    let server = TestServer::new()
        .serve_handler(
            "/provider.bin",
            token_protected_handler(content.clone(), "x-api-key", PROVIDER_TOKEN),
        )
        .with_default_headers("/provider.bin", accept_ranges_header)
        .start()
        .await
        .expect("start");
    let cfg = segmented_config();
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let mut req = request(server.url("/provider.bin"), &dest);
    req.credential_provider = Some(Arc::from(provider_fn(|_ch: &Challenge| {
        Ok(CredentialDecision::Headers(vec![(
            "x-api-key".to_string(),
            PROVIDER_TOKEN.to_string(),
        )]))
    })));
    tokio::time::timeout(std::time::Duration::from_secs(60), controller.run(req))
        .await
        .expect("no hang")
        .expect("download completes with provider credentials");
    let output = std::fs::read(&dest).expect("read output");
    fixtures::assert_bytes_exact(&output, &content);

    let requests = server.requests().await;
    assert!(
        requests.iter().any(|r| r.method == "HEAD"),
        "the probe must be attempted: {}",
        describe(&requests)
    );
    let gets: Vec<_> = requests.iter().filter(|r| r.method == "GET").collect();
    assert!(!gets.is_empty(), "segmented GETs must run: {}", describe(&requests));
    assert!(
        gets
            .iter()
            .all(|r| r.header("x-api-key") == Some(PROVIDER_TOKEN)),
        "provider credentials must reach every segmented GET: {}",
        describe(&requests)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_challenge_after_probe_reaches_segmented_workers() {
    // The probe and the `bytes=0-0` validation are open, so the challenge
    // first appears on a worker's nonzero ranged GET: the segmented path
    // itself must consult the provider (bounded, at most MAX_AUTH_STAGES)
    // and retry the request with the latched credentials.
    use kdown_engine::control::auth::{provider_fn, Challenge, CredentialDecision};

    const PROVIDER_TOKEN: &str = "sentinel-late-provider-token";
    let content = Arc::new(fixtures::deterministic_bytes(1024 * 1024, 0x6e));
    let served = Arc::clone(&content);
    let server = TestServer::new()
        .serve_handler("/late.bin", move |req: &RequestInfo| {
            let challenged = req.range.is_some_and(|(start, _)| start > 0);
            if challenged && req.header("x-api-key") != Some(PROVIDER_TOKEN) {
                return ScriptedResponse::new(401)
                    .with_header("www-authenticate", "Bearer realm=\"late\"");
            }
            let total = served.len() as u64;
            match req.range {
                Some((s, e)) => {
                    let end = e.min(total - 1);
                    ScriptedResponse::new(206)
                        .with_body(served[s as usize..=(end as usize)].to_vec())
                        .with_header("content-range", &format!("bytes {s}-{end}/{total}"))
                }
                None => ScriptedResponse::ok((*served).clone()),
            }
        })
        .with_default_headers("/late.bin", accept_ranges_header)
        .start()
        .await
        .expect("start");
    let mut cfg = segmented_config();
    cfg.transfer.min_segment_size = 64 * 1024;
    cfg.transfer.initial_segment_size = 64 * 1024;
    cfg.transfer.max_segment_size = 64 * 1024;
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let provider_calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let calls = Arc::clone(&provider_calls);
    let mut req = request(server.url("/late.bin"), &dest);
    req.credential_provider = Some(Arc::from(provider_fn(move |_ch: &Challenge| {
        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(CredentialDecision::Headers(vec![(
            "x-api-key".to_string(),
            PROVIDER_TOKEN.to_string(),
        )]))
    })));
    tokio::time::timeout(std::time::Duration::from_secs(60), controller.run(req))
        .await
        .expect("no hang")
        .expect("download completes after the worker challenge");
    let output = std::fs::read(&dest).expect("read output");
    fixtures::assert_bytes_exact(&output, &content);
    let calls = provider_calls.load(std::sync::atomic::Ordering::SeqCst);
    assert!(calls >= 1, "the worker path must consult the provider");
    assert!(
        calls <= kdown_engine::control::auth::MAX_AUTH_STAGES,
        "provider consultations must stay bounded: {calls}"
    );

    let requests = server.requests().await;
    let challenged_then_authorized = requests
        .iter()
        .filter(|r| r.range.is_some_and(|(start, _)| start > 0))
        .filter(|r| r.header("x-api-key") == Some(PROVIDER_TOKEN))
        .count();
    assert!(
        challenged_then_authorized > 0,
        "retried nonzero range GETs must carry the provider token: {}",
        describe(&requests)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caller_proxy_authorization_never_reaches_origin() {
    // Proxy credentials belong to the configured proxy only: a caller
    // header named Proxy-Authorization must never travel to an origin,
    // while the origin authorization still does.
    let content = fixtures::deterministic_bytes(64 * 1024, 0x6d);
    let server = TestServer::new()
        .serve_static("/file.bin", content.clone())
        .start()
        .await
        .expect("start");
    let cfg = EngineConfig::default();
    let transport = HttpTransport::from_config(&cfg).expect("transport");
    let controller = DownloadController::new(transport, cfg);
    let dir = tempfile::tempdir().expect("tmpdir");
    let dest = dir.path().join("out.bin");
    let mut req = request(server.url("/file.bin"), &dest);
    req.authorization = Some(DUMMY_AUTHORIZATION.to_string());
    req.headers.push((
        "Proxy-Authorization".to_string(),
        "Basic sentinel-proxy-secret".to_string(),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(60), controller.run(req))
        .await
        .expect("no hang")
        .expect("download completes");
    let output = std::fs::read(&dest).expect("read output");
    fixtures::assert_bytes_exact(&output, &content);

    let requests = server.requests().await;
    assert!(!requests.is_empty());
    assert!(
        requests.iter().all(|r| r.header("proxy-authorization").is_none()),
        "caller proxy credentials must never reach an origin: {}",
        describe(&requests)
    );
    assert!(
        requests.iter().all(saw_authorization),
        "origin authorization still reaches the origin: {}",
        describe(&requests)
    );
}
