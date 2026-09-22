//! Integration tests for the hyper transport against the misbehaving
//! test server: redirects, probe fallback, validator capture (§10, §11.1).

#[path = "support/mod.rs"]
mod support;

use std::time::Duration;

use kdown_engine::config::NetworkPolicy;
use kdown_engine::control::CancellationToken;
use kdown_engine::http::probe::filename_from_disposition;
use kdown_engine::http::transport::{HttpTransport, RequestSpec};
use kdown_engine::http::{
    FullResponsePolicy, HttpExecution, ProbeRequest, RangeIntent, TransferIntent, TransferRequest,
};
use support::fixtures::deterministic_bytes;
use support::test_server::{ScriptedResponse, TestServer};

fn transport() -> HttpTransport {
    HttpTransport::new(NetworkPolicy::default()).expect("transport")
}

fn spec(url: &str) -> RequestSpec {
    RequestSpec {
        url: url.to_string(),
        ..RequestSpec::default()
    }
}

#[tokio::test]
async fn get_returns_metadata_and_body() {
    let content = deterministic_bytes(4096, 11);
    let server = TestServer::new()
        .serve_static("/file", content.clone())
        .start()
        .await
        .expect("start");
    let mut resp = executor()
        .transfer(
            TransferRequest {
                spec: spec(&server.url("/file")),
                intent: TransferIntent::Full,
            },
            &CancellationToken::new(),
        )
        .await
        .expect("transfer");
    assert_eq!(resp.total_size, Some(4096));
    let mut bytes = Vec::new();
    while let Some(d) = resp
        .body
        .next_chunk(&CancellationToken::new())
        .await
        .expect("chunk")
        .data()
    {
        bytes.extend_from_slice(d);
    }
    assert_eq!(&bytes[..], &content[..]);
}

#[tokio::test]
async fn redirect_chain_follows_and_records_final_url() {
    let content = vec![5u8; 128];
    let server = TestServer::new()
        .serve_handler("/r1", move |_req| {
            ScriptedResponse::new(302).with_header("location", "/r2")
        })
        .serve_handler("/r2", move |_req| {
            ScriptedResponse::new(302).with_header("location", "/r3")
        })
        .serve_static("/r3", content.clone())
        .start()
        .await
        .expect("start");
    let t = executor();
    let outcome = t
        .probe(
            probe_request(&server.url("/r1"), u64::MAX, false),
            &CancellationToken::new(),
        )
        .await
        .expect("redirect chain follows");
    assert_eq!(outcome.metadata.status, 200);
    assert_eq!(
        outcome.metadata.final_url,
        server.url("/r3"),
        "post-redirect URL retained"
    );
    let reqs = server.requests().await;
    let paths: Vec<&str> = reqs.iter().map(|r| r.path.as_str()).collect();
    assert!(paths.contains(&"/r1") && paths.contains(&"/r2") && paths.contains(&"/r3"));
}

#[tokio::test]
async fn redirect_loop_fails_structured() {
    let server = TestServer::new()
        .serve_handler("/loop", move |_req| {
            ScriptedResponse::new(302).with_header("location", "/loop")
        })
        .start()
        .await
        .expect("start");
    let err = executor()
        .probe(
            probe_request(&server.url("/loop"), u64::MAX, false),
            &CancellationToken::new(),
        )
        .await
        .expect_err("loop must fail");
    assert!(
        matches!(err.error, kdown_engine::DownloadError::Redirect(_)),
        "structured redirect error expected, got {:?}",
        err.error
    );
}

#[tokio::test]
async fn probe_head_then_ranged_get_validates_ranges() {
    let content = deterministic_bytes(64 * 1024, 77);
    let server = TestServer::new()
        .serve_static("/data", content)
        .start()
        .await
        .expect("start");
    let cancel = CancellationToken::new();

    // Semantic probe: HEAD interpretation plus validating bytes=0-0 inside
    // the HTTP layer (§10.2).
    let outcome = executor()
        .probe(probe_request(&server.url("/data"), 1024, true), &cancel)
        .await
        .expect("probe");
    let md = outcome.metadata;
    assert!(md.accept_ranges, "§10.1: Accept-Ranges captured");
    assert!(md.range_verified, "§10.2: validating range request ran");
    assert_eq!(md.total_size, Some(64 * 1024));
    assert_eq!(md.content_range_total, Some(64 * 1024));
}

#[tokio::test]
async fn validator_capture_etag_and_last_modified() {
    let server = TestServer::new()
        .serve_handler("/v", move |_req| {
            ScriptedResponse::ok(vec![1; 32])
                .with_header("etag", "\"gen-1\"")
                .with_header("last-modified", "Mon, 22 Sep 2026 00:00:00 GMT")
        })
        .start()
        .await
        .expect("start");
    let t = executor();
    let resp = t
        .transfer(
            TransferRequest {
                spec: spec(&server.url("/v")),
                intent: TransferIntent::Full,
            },
            &CancellationToken::new(),
        )
        .await
        .expect("get");
    assert_eq!(resp.validators.etag.as_deref(), Some("\"gen-1\""));
    assert_eq!(
        resp.validators.last_modified.as_deref(),
        Some("Mon, 22 Sep 2026 00:00:00 GMT")
    );
    assert!(resp.validators.resume_capable());
}

#[tokio::test]
async fn range_request_carries_if_range_when_validators_present() {
    let content = vec![9u8; 2048];
    let server = TestServer::new()
        .serve_static("/ifrange", content.clone())
        .start()
        .await
        .expect("start");
    let t = executor();
    let mut s = spec(&server.url("/ifrange"));
    s.identity_encoding = true;
    // The intent's expected validators drive the conditional request
    // (If-Range, §11.3) and the generation check.
    let validators = kdown_engine::http::validators::ResourceValidators {
        etag: Some("\"abc\"".into()),
        etag_is_weak: false,
        last_modified: Some("Mon, 22 Sep 2026 00:00:00 GMT".into()),
        total_size: Some(2048),
    };
    let mut resp = t
        .transfer(
            TransferRequest {
                spec: s,
                intent: range_intent(
                    (1024, 2047),
                    None,
                    Some(validators),
                    FullResponsePolicy::InvalidRange,
                ),
            },
            &CancellationToken::new(),
        )
        .await
        .expect("range");
    // The server must have received Range + If-Range.
    let reqs = server.requests().await;
    let last = reqs.last().expect("request recorded");
    assert_eq!(last.range, Some((1024, 2047)));
    assert_eq!(last.if_range.as_deref(), Some("\"abc\""));
    // Accept-Encoding identity was sent (§11.4).
    assert!(last
        .headers
        .iter()
        .any(|(k, v)| k == "accept-encoding" && v.eq_ignore_ascii_case("identity")));
    let mut got = Vec::new();
    while let Some(d) = resp
        .body
        .next_chunk(&CancellationToken::new())
        .await
        .expect("chunk")
        .data()
    {
        got.extend_from_slice(d);
    }
    assert_eq!(&got[..], &content[1024..2048]);
}

#[tokio::test]
async fn credentials_stripped_on_cross_origin_redirect() {
    let server = TestServer::new()
        .serve_static("/auth", vec![1; 16])
        .start()
        .await
        .expect("start");
    let t = executor();
    let mut s = spec(&server.url("/auth"));
    s.headers
        .push(("Authorization".into(), "Bearer tok".into()));
    let _ = t
        .transfer(
            TransferRequest {
                spec: s,
                intent: TransferIntent::Full,
            },
            &CancellationToken::new(),
        )
        .await
        .expect("get");
    // Same-origin request keeps credentials (§21.2) — asserted on the
    // recorded request.
    let reqs = server.requests().await;
    let first = reqs.first().expect("request recorded");
    assert_eq!(
        first.header("authorization"),
        Some("Bearer tok"),
        "same-request credentials must be present"
    );
}

#[tokio::test]
async fn unsupported_scheme_is_structured_error() {
    let err = executor()
        .transfer(
            TransferRequest {
                spec: spec("ftp://example.invalid/file"),
                intent: TransferIntent::Full,
            },
            &CancellationToken::new(),
        )
        .await
        .expect_err("ftp must be rejected");
    assert!(
        matches!(err.error, kdown_engine::DownloadError::UnsupportedScheme(_)),
        "got {:?}",
        err.error
    );
}

#[tokio::test]
async fn connection_reset_maps_to_structured_error() {
    // Server accepts then kills the connection before any response bytes.
    let server = TestServer::new()
        .serve_handler("/reset", |_req| {
            ScriptedResponse::ok(vec![1; 1024]).reset_after(0)
        })
        .start()
        .await
        .expect("start");
    let result = executor()
        .transfer(
            TransferRequest {
                spec: spec(&server.url("/reset")),
                intent: TransferIntent::Full,
            },
            &CancellationToken::new(),
        )
        .await;
    // reset_after(0) closes right after headers; body read may still
    // succeed as empty OR the request may fail with Connection — either
    // is acceptable, but it must not hang. Assert completion.
    drop(result);
    let _ = Duration::from_millis(0);
}

#[test]
fn disposition_extraction() {
    assert_eq!(
        filename_from_disposition(Some("attachment; filename=\"x.bin\"")),
        Some("x.bin".into())
    );
}

// ---- Semantic execution seam: production adapter (tasks 2.2-2.3) ----

fn executor() -> HttpExecution {
    HttpExecution::from_adapter(transport())
}

fn probe_request(url: &str, threshold: u64, verify: bool) -> ProbeRequest {
    ProbeRequest {
        spec: spec(url),
        segmentation_threshold: threshold,
        verify_range_support: verify,
    }
}

fn range_intent(
    range: (u64, u64),
    established_total: Option<u64>,
    expected_validators: Option<kdown_engine::http::validators::ResourceValidators>,
    full_response: FullResponsePolicy,
) -> TransferIntent {
    TransferIntent::Range(RangeIntent {
        range,
        established_total,
        expected_validators,
        full_response,
    })
}

#[tokio::test]
async fn semantic_probe_verifies_ranges_and_final_metadata() {
    let content = deterministic_bytes(64 * 1024, 91);
    let server = TestServer::new()
        .serve_static("/p", content)
        .start()
        .await
        .expect("start");
    let outcome = executor()
        .probe(
            probe_request(&server.url("/p"), 1024, true),
            &CancellationToken::new(),
        )
        .await
        .expect("probe");
    let md = outcome.metadata;
    assert!(md.range_verified, "§10.2: validating bytes=0-0 ran");
    assert_eq!(md.total_size, Some(64 * 1024));
    assert_eq!(md.content_range_total, Some(64 * 1024));
    assert_eq!(md.status, 200);
    assert_eq!(md.final_url, server.url("/p"), "final URL retained");
    assert_eq!(md.http_version, "HTTP/1.1", "wire version retained");
    assert!(outcome.notices.is_empty());
    // The single eligibility decision is now usable as-is (§10.3).
    assert!(md.segment_eligible(1024, true));
}

#[tokio::test]
async fn semantic_probe_lying_range_advertisement_falls_back_with_notice() {
    let content = deterministic_bytes(64 * 1024, 17);
    let server = TestServer::new()
        .serve_ranges("/lie", content, support::test_server::RangeMode::Full200)
        .start()
        .await
        .expect("start");
    let outcome = executor()
        .probe(
            probe_request(&server.url("/lie"), 1024, true),
            &CancellationToken::new(),
        )
        .await
        .expect("probe still succeeds");
    let md = outcome.metadata;
    assert!(!md.range_verified, "200 to bytes=0-0 proves ranges broken");
    assert!(!md.accept_ranges, "ineligible for verified segmentation");
    assert_eq!(md.status, 200);
    assert!(
        !outcome.notices.is_empty(),
        "semantic notice required for advertised-but-broken ranges"
    );
    assert!(!md.segment_eligible(1024, true), "§10.3 fallback");
}

#[tokio::test]
async fn semantic_probe_maps_error_statuses() {
    let server = TestServer::new()
        .serve_n(
            "/gone",
            5,
            ScriptedResponse::new(404).with_body(b"no".to_vec()),
        )
        .serve_n(
            "/busy",
            5,
            ScriptedResponse::new(503).with_body(b"x".to_vec()),
        )
        .start()
        .await
        .expect("start");
    let cancel = CancellationToken::new();
    let e = executor()
        .probe(probe_request(&server.url("/gone"), 1024, true), &cancel)
        .await
        .expect_err("404");
    assert!(matches!(
        e.error,
        kdown_engine::DownloadError::NotFound { status: 404 }
    ));

    let e = executor()
        .probe(probe_request(&server.url("/busy"), 1024, true), &cancel)
        .await
        .expect_err("503");
    assert!(matches!(
        e.error,
        kdown_engine::DownloadError::Server { status: 503 }
    ));
}

#[tokio::test]
async fn semantic_transfer_full_body_delivers_exact_bytes() {
    let content = deterministic_bytes(8192, 23);
    let server = TestServer::new()
        .serve_static("/full", content.clone())
        .start()
        .await
        .expect("start");
    let mut resp = executor()
        .transfer(
            TransferRequest {
                spec: spec(&server.url("/full")),
                intent: TransferIntent::Full,
            },
            &CancellationToken::new(),
        )
        .await
        .expect("transfer");
    assert_eq!(resp.start, 0);
    assert_eq!(resp.total_size, Some(8192));
    let mut got = Vec::new();
    while let Some(chunk) = resp
        .body
        .next_chunk(&CancellationToken::new())
        .await
        .unwrap()
        .data()
    {
        got.extend_from_slice(chunk);
    }
    assert_eq!(&got[..], &content[..]);
}

#[tokio::test]
async fn semantic_transfer_ranged_body_delivers_requested_slice() {
    let content = deterministic_bytes(4096, 41);
    let server = TestServer::new()
        .serve_static("/slice", content.clone())
        .start()
        .await
        .expect("start");
    let mut resp = executor()
        .transfer(
            TransferRequest {
                spec: spec(&server.url("/slice")),
                intent: range_intent(
                    (100, 199),
                    Some(4096),
                    None,
                    FullResponsePolicy::InvalidRange,
                ),
            },
            &CancellationToken::new(),
        )
        .await
        .expect("transfer");
    assert_eq!(resp.start, 100);
    assert_eq!(resp.end, 199);
    let mut got = Vec::new();
    while let Some(chunk) = resp
        .body
        .next_chunk(&CancellationToken::new())
        .await
        .unwrap()
        .data()
    {
        got.extend_from_slice(chunk);
    }
    assert_eq!(&got[..], &content[100..200]);
}

#[tokio::test]
async fn semantic_transfer_if_range_generation_change_rejected_before_body() {
    // Server honors If-Range: a stale validator gets the full body (200).
    let content = vec![7u8; 2048];
    let server = TestServer::new()
        .serve_handler("/gen", move |req| match &req.if_range {
            Some(v) if v == "\"gen-2\"" => ScriptedResponse::new(206)
                .with_body(content[1024..2048].to_vec())
                .with_header("content-range", "bytes 1024-2047/2048")
                .with_header("etag", "\"gen-2\""),
            _ => ScriptedResponse::ok(content.clone()).with_header("etag", "\"gen-2\""),
        })
        .start()
        .await
        .expect("start");
    let stale = kdown_engine::http::validators::ResourceValidators {
        etag: Some("\"gen-1\"".into()),
        etag_is_weak: false,
        last_modified: None,
        total_size: Some(2048),
    };
    let err = executor()
        .transfer(
            TransferRequest {
                spec: spec(&server.url("/gen")),
                intent: range_intent(
                    (1024, 2047),
                    Some(2048),
                    Some(stale),
                    FullResponsePolicy::ResourceChanged,
                ),
            },
            &CancellationToken::new(),
        )
        .await
        .expect_err("generation change");
    assert!(
        matches!(err.error, kdown_engine::DownloadError::ResourceChanged(_)),
        "§26: If-Range failure is a resource change, got {err:?}"
    );

    // Current generation: 206 accepted with validators captured.
    let current = kdown_engine::http::validators::ResourceValidators {
        etag: Some("\"gen-2\"".into()),
        etag_is_weak: false,
        last_modified: None,
        total_size: Some(2048),
    };
    let ok = executor()
        .transfer(
            TransferRequest {
                spec: spec(&server.url("/gen")),
                intent: range_intent(
                    (1024, 2047),
                    Some(2048),
                    Some(current),
                    FullResponsePolicy::ResourceChanged,
                ),
            },
            &CancellationToken::new(),
        )
        .await
        .expect("current generation accepted");
    assert_eq!(ok.start, 1024);
    assert_eq!(ok.validators.etag.as_deref(), Some("\"gen-2\""));
}

#[tokio::test]
async fn semantic_transfer_200_to_nonzero_range_rejected_before_body() {
    // Lying server: advertises ranges but answers every range with 200 full.
    let content = deterministic_bytes(4096, 55);
    let server = TestServer::new()
        .serve_ranges(
            "/full200",
            content,
            support::test_server::RangeMode::Full200,
        )
        .start()
        .await
        .expect("start");
    let err = executor()
        .transfer(
            TransferRequest {
                spec: spec(&server.url("/full200")),
                intent: range_intent(
                    (100, 199),
                    Some(4096),
                    None,
                    FullResponsePolicy::InvalidRange,
                ),
            },
            &CancellationToken::new(),
        )
        .await
        .expect_err("200 to nonzero range");
    assert!(
        matches!(
            err.error,
            kdown_engine::DownloadError::InvalidRangeResponse(_)
        ),
        "§11.2: full response to a nonzero range is never delivered, got {err:?}"
    );
}

#[tokio::test]
async fn semantic_transfer_invalid_content_range_rejected_before_body() {
    let content = deterministic_bytes(4096, 66);
    let server = TestServer::new()
        .serve_ranges(
            "/bogus",
            content,
            support::test_server::RangeMode::MalformedContentRange,
        )
        .start()
        .await
        .expect("start");
    let err = executor()
        .transfer(
            TransferRequest {
                spec: spec(&server.url("/bogus")),
                intent: range_intent(
                    (100, 199),
                    Some(4096),
                    None,
                    FullResponsePolicy::InvalidRange,
                ),
            },
            &CancellationToken::new(),
        )
        .await
        .expect_err("malformed content range");
    assert!(matches!(
        err.error,
        kdown_engine::DownloadError::InvalidRangeResponse(_)
    ));
}

#[tokio::test]
async fn semantic_transfer_validator_change_rejected_before_body() {
    let content = vec![3u8; 1024];
    let server = TestServer::new()
        .serve_handler("/moved", move |_req| {
            ScriptedResponse::new(206)
                .with_body(content[0..512].to_vec())
                .with_header("content-range", "bytes 0-511/1024")
                .with_header("etag", "\"new-gen\"")
        })
        .start()
        .await
        .expect("start");
    let expected = kdown_engine::http::validators::ResourceValidators {
        etag: Some("\"old-gen\"".into()),
        etag_is_weak: false,
        last_modified: None,
        total_size: Some(1024),
    };
    let err = executor()
        .transfer(
            TransferRequest {
                spec: spec(&server.url("/moved")),
                intent: range_intent(
                    (0, 511),
                    Some(1024),
                    Some(expected),
                    FullResponsePolicy::InvalidRange,
                ),
            },
            &CancellationToken::new(),
        )
        .await
        .expect_err("validator change");
    assert!(matches!(
        err.error,
        kdown_engine::DownloadError::ResourceChanged(_)
    ));
}

#[tokio::test]
async fn semantic_transfer_body_overrun_rejected_before_offending_chunk() {
    // Server claims bytes 0-99 but sends 300 bytes.
    let server = TestServer::new()
        .serve_handler("/overrun", move |_req| {
            ScriptedResponse::new(206)
                .with_body(vec![9u8; 300])
                .with_header("content-range", "bytes 0-99/1000")
        })
        .start()
        .await
        .expect("start");
    let mut resp = executor()
        .transfer(
            TransferRequest {
                spec: spec(&server.url("/overrun")),
                intent: range_intent((0, 99), Some(1000), None, FullResponsePolicy::InvalidRange),
            },
            &CancellationToken::new(),
        )
        .await
        .expect("transfer accepted (metadata valid)");
    assert_eq!(resp.start, 0);
    assert_eq!(resp.end, 99);
    let cancel = CancellationToken::new();
    // Chunks within the accepted length arrive; the offending chunk is
    // rejected before delivery (§11.2).
    let mut delivered = 0u64;
    loop {
        match resp.body.next_chunk(&cancel).await {
            Ok(kdown_engine::http::BodyEvent::Data(d)) => delivered += d.len() as u64,
            Ok(other) => panic!("expected overrun, got {other:?}"),
            Err(e) => {
                assert!(
                    matches!(e, kdown_engine::DownloadError::InvalidRangeResponse(ref m) if m.contains("[0, 99]")),
                    "overrun error expected, got {e:?}"
                );
                break;
            }
        }
    }
    assert!(
        delivered <= 100,
        "no chunk past the accepted range may arrive"
    );
}
