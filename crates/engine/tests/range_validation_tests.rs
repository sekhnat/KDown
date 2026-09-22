//! Integration tests for range request building and response validation
//! (§11.2, task 5.4): the validation gate rejects lying/broken range
//! server behaviors before any body byte is accepted, and well-behaved
//! 206 responses validate through with correct offsets.

#[path = "support/mod.rs"]
mod support;

use kdown_engine::config::NetworkPolicy;
use kdown_engine::control::CancellationToken;
use kdown_engine::http::range::{validate_range_response, RejectionKind};
use kdown_engine::http::transport::{HttpTransport, RequestSpec};
use kdown_engine::http::validators::ResourceValidators;
use support::fixtures::deterministic_bytes;
use support::test_server::{RangeMode, TestServer};

fn transport() -> HttpTransport {
    HttpTransport::new(NetworkPolicy::default()).expect("transport")
}

fn spec(url: &str) -> RequestSpec {
    RequestSpec {
        url: url.to_string(),
        identity_encoding: true,
        ..RequestSpec::default()
    }
}

/// Fetch a range and hand the metadata to the validation gate (no body
/// read before validation — §32).
async fn fetch_and_validate(
    t: &HttpTransport,
    spec: &RequestSpec,
    range: (u64, u64),
    established_total: Option<u64>,
) -> Result<kdown_engine::http::range::ValidatedRange, RejectionKind> {
    let cancel = CancellationToken::new();
    let resp = t
        .get_range(spec, range, &cancel)
        .await
        .expect("transport ok");
    validate_range_response(range, &resp, established_total, None).map_err(|e| e.kind)
}

#[tokio::test]
async fn correct_range_server_validates() {
    let content = deterministic_bytes(8192, 101);
    let server = TestServer::new()
        .serve_static("/ok", content)
        .start()
        .await
        .expect("start");
    let t = transport();
    let s = spec(&server.url("/ok"));
    let v = fetch_and_validate(&t, &s, (100, 199), Some(8192))
        .await
        .expect("correct range validates");
    assert_eq!(v.start, 100);
    assert_eq!(v.end, 199);
    assert_eq!(v.total_size, Some(8192));
}

#[tokio::test]
async fn lying_200_server_rejected() {
    // §36.2 Full200: advertises Accept-Ranges but answers 200 + full body.
    let content = deterministic_bytes(4096, 102);
    let server = TestServer::new()
        .serve_ranges("/liar", content, RangeMode::Full200)
        .start()
        .await
        .expect("start");
    let t = transport();
    let s = spec(&server.url("/liar"));
    let err = fetch_and_validate(&t, &s, (100, 199), Some(4096))
        .await
        .expect_err("200-on-range must reject");
    assert_eq!(err, RejectionKind::FullResponseToNonzeroRange);
}

#[tokio::test]
async fn malformed_content_range_rejected() {
    // §36.2 MalformedContentRange: 206 with wrong start/end and a bogus
    // total — the parse fails, so the gate rejects as StartMismatch.
    let content = deterministic_bytes(4096, 103);
    let server = TestServer::new()
        .serve_ranges("/malformed", content, RangeMode::MalformedContentRange)
        .start()
        .await
        .expect("start");
    let t = transport();
    let s = spec(&server.url("/malformed"));
    let err = fetch_and_validate(&t, &s, (0, 99), None)
        .await
        .expect_err("malformed Content-Range must reject");
    assert_eq!(err, RejectionKind::StartMismatch);
}

#[tokio::test]
async fn no_range_server_200_rejected_for_nonzero_range() {
    // NoRanges mode never honors ranges: a nonzero range request gets 200.
    let content = deterministic_bytes(2048, 104);
    let server = TestServer::new()
        .serve_ranges("/noranges", content, RangeMode::NoRanges)
        .start()
        .await
        .expect("start");
    let t = transport();
    let s = spec(&server.url("/noranges"));
    let err = fetch_and_validate(&t, &s, (500, 999), Some(2048))
        .await
        .expect_err("200 to nonzero range rejected");
    assert_eq!(err, RejectionKind::FullResponseToNonzeroRange);
}

#[tokio::test]
async fn range_request_carries_identity_encoding() {
    // §11.4: segmented requests send Accept-Encoding: identity.
    let server = TestServer::new()
        .serve_handler("/enc", |req| {
            let ae = req.header("accept-encoding").unwrap_or("").to_string();
            let body = if ae.eq_ignore_ascii_case("identity") {
                b"identity-ok".to_vec()
            } else {
                b"wrong".to_vec()
            };
            let ae_hdr = ae.clone();
            support::test_server::ScriptedResponse::ok(body)
                .with_header("x-observed-accept-encoding", &ae_hdr)
        })
        .start()
        .await
        .expect("start");
    let t = transport();
    let s = spec(&server.url("/enc"));
    let resp = t
        .get_range(&s, (0, 7), &CancellationToken::new())
        .await
        .expect("get");
    assert_eq!(
        resp.header("x-observed-accept-encoding"),
        Some("identity"),
        "range requests must carry Accept-Encoding: identity (§11.4)"
    );
}

#[tokio::test]
async fn well_behaved_range_end_to_end() {
    // Full loop: request a range from a correct server, validate, then
    // read the body and assert it matches the requested slice exactly.
    let content = deterministic_bytes(64 * 1024, 105);
    let server = TestServer::new()
        .serve_static("/slice", content.clone())
        .start()
        .await
        .expect("start");
    let t = transport();
    let s = spec(&server.url("/slice"));
    let range = (17_345u64, 18_943u64);
    let cancel = CancellationToken::new();
    let mut resp = t.get_range(&s, range, &cancel).await.expect("get");
    let v = validate_range_response(range, &resp, Some(content.len() as u64), None)
        .expect("valid slice");
    assert_eq!(v.start, range.0);
    use http_body_util::BodyExt;
    let body = resp.body().expect("body");
    let bytes = body.collect().await.expect("collect").to_bytes();
    assert_eq!(
        bytes.len() as u64,
        v.end - v.start + 1,
        "body length matches validated slice"
    );
    assert_eq!(
        &bytes[..],
        &content[range.0 as usize..=(range.1 as usize)],
        "byte-exact slice"
    );
}

#[tokio::test]
async fn established_total_conflict_rejected() {
    // A server whose Content-Range total contradicts the established size
    // (e.g., resource replaced between probe and segment request, §26).
    let content = deterministic_bytes(3000, 106);
    let server = TestServer::new()
        .serve_handler("/mutated", move |req| {
            let total = content.len() as u64;
            if let Some((s, e)) = req.range {
                let body = content[s as usize..=(e as usize).min(content.len() - 1)].to_vec();
                support::test_server::ScriptedResponse::new(206)
                    .with_body(body)
                    .with_header("content-range", &format!("bytes {s}-{e}/{total}"))
            } else {
                support::test_server::ScriptedResponse::ok(content.clone())
                    .with_header("accept-ranges", "bytes")
            }
        })
        .start()
        .await
        .expect("start");
    let t = transport();
    let s = spec(&server.url("/mutated"));
    // Established total from the probe was 4096, but the segment response
    // claims 3000: TotalConflict.
    let cancel = CancellationToken::new();
    let resp = t.get_range(&s, (0, 99), &cancel).await.expect("get");
    let err = validate_range_response((0, 99), &resp, Some(4096), None).unwrap_err();
    assert_eq!(err.kind, RejectionKind::TotalConflict);
    assert!(!matches!(err.kind, RejectionKind::StartMismatch));
}

#[tokio::test]
async fn validators_passed_to_gate_detect_generation_change() {
    // The response carries a different ETag than the job's established
    // validators: the gate rejects with GenerationChanged.
    let server = TestServer::new()
        .serve_handler("/gen", |req| {
            let _ = req;
            support::test_server::ScriptedResponse::new(206)
                .with_body(vec![1u8; 100])
                .with_header("etag", "\"generation-2\"")
                .with_header("content-range", "bytes 0-99/1000")
                .with_header("accept-ranges", "bytes")
        })
        .start()
        .await
        .expect("start");
    let t = transport();
    let s = spec(&server.url("/gen"));
    let cancel = CancellationToken::new();
    let resp = t.get_range(&s, (0, 99), &cancel).await.expect("get");
    let expected = ResourceValidators::from_headers(Some("\"generation-1\""), None, Some(1000));
    let err = kdown_engine::http::range::validate_range_response(
        (0, 99),
        &resp,
        Some(1000),
        Some(&expected),
    )
    .unwrap_err();
    assert_eq!(err.kind, RejectionKind::GenerationChanged);
}
