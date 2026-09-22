//! Integration tests for range request building and response validation
//! (§11.2, task 5.4) through the semantic execution seam: the production
//! Hyper adapter's parsing is the subject, exercised via `HttpExecutor` —
//! the validation gate rejects lying/broken range server behaviors before
//! any body byte is accepted, and well-behaved 206 responses validate
//! through with correct offsets.

#[path = "support/mod.rs"]
mod support;

use kdown_engine::config::NetworkPolicy;
use kdown_engine::control::CancellationToken;
use kdown_engine::http::transport::{HttpTransport, RequestSpec};
use kdown_engine::http::{
    FullResponsePolicy, HttpExecution, HttpFailure, RangeIntent, TransferIntent, TransferRequest,
};
use kdown_engine::DownloadError;
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

fn range_intent(
    range: (u64, u64),
    established_total: Option<u64>,
    expected_validators: Option<kdown_engine::http::validators::ResourceValidators>,
) -> TransferIntent {
    TransferIntent::Range(RangeIntent {
        range,
        established_total,
        expected_validators,
        full_response: FullResponsePolicy::InvalidRange,
    })
}

/// Ranged transfer through the semantic seam; validation happens inside
/// the HTTP layer before the response (and its body) exists (§32).
async fn transfer_range(
    t: &HttpExecution,
    url: &str,
    range: (u64, u64),
    established_total: Option<u64>,
    expected_validators: Option<kdown_engine::http::validators::ResourceValidators>,
) -> Result<kdown_engine::http::TransferResponse, HttpFailure> {
    t.transfer(
        TransferRequest {
            spec: spec(url),
            intent: range_intent(range, established_total, expected_validators),
        },
        &CancellationToken::new(),
    )
    .await
}

#[tokio::test]
async fn correct_range_server_validates() {
    let content = deterministic_bytes(8192, 101);
    let server = TestServer::new()
        .serve_static("/ok", content.clone())
        .start()
        .await
        .expect("start");
    let t = HttpExecution::from_adapter(transport());
    let mut resp = transfer_range(&t, &server.url("/ok"), (100, 199), Some(8192), None)
        .await
        .expect("correct range validates");
    assert_eq!(resp.start, 100);
    assert_eq!(resp.end, 199);
    assert_eq!(resp.total_size, Some(8192));
    let mut got = Vec::new();
    while let Some(d) = resp
        .body
        .next_chunk(&CancellationToken::new())
        .await
        .unwrap()
        .data()
    {
        got.extend_from_slice(d);
    }
    assert_eq!(&got[..], &content[100..200], "byte-exact slice");
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
    let t = HttpExecution::from_adapter(transport());
    let err = transfer_range(&t, &server.url("/liar"), (100, 199), Some(4096), None)
        .await
        .expect_err("200-on-range must reject");
    assert!(
        matches!(err.error, DownloadError::InvalidRangeResponse(ref m) if m.contains("full response to nonzero range")),
        "got {err:?}"
    );
}

#[tokio::test]
async fn malformed_content_range_rejected() {
    // §36.2 MalformedContentRange: 206 with wrong start/end and a bogus
    // total — the parse fails, so the gate rejects as start mismatch.
    let content = deterministic_bytes(4096, 103);
    let server = TestServer::new()
        .serve_ranges("/malformed", content, RangeMode::MalformedContentRange)
        .start()
        .await
        .expect("start");
    let t = HttpExecution::from_adapter(transport());
    let err = transfer_range(&t, &server.url("/malformed"), (0, 99), None, None)
        .await
        .expect_err("malformed Content-Range must reject");
    assert!(
        matches!(err.error, DownloadError::InvalidRangeResponse(ref m) if m.contains("start mismatch")),
        "got {err:?}"
    );
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
    let t = HttpExecution::from_adapter(transport());
    let err = transfer_range(&t, &server.url("/noranges"), (500, 999), Some(2048), None)
        .await
        .expect_err("200 to nonzero range rejected");
    assert!(matches!(
        err.error,
        DownloadError::InvalidRangeResponse(ref m) if m.contains("full response to nonzero range")
    ));
}

#[tokio::test]
async fn range_request_carries_identity_encoding() {
    // §11.4: ranged requests send Accept-Encoding: identity.
    let server = TestServer::new()
        .serve_handler("/enc", |_req| {
            support::test_server::ScriptedResponse::ok(b"identity-ok".to_vec())
        })
        .start()
        .await
        .expect("start");
    let t = HttpExecution::from_adapter(transport());
    let _ = transfer_range(&t, &server.url("/enc"), (0, 7), None, None)
        .await
        .expect("get");
    let reqs = server.requests().await;
    let last = reqs.last().expect("request recorded");
    assert_eq!(
        last.header("accept-encoding"),
        Some("identity"),
        "ranged requests must carry Accept-Encoding: identity (§11.4)"
    );
}

#[tokio::test]
async fn well_behaved_range_end_to_end() {
    // Full loop: request a range from a correct server and read the body,
    // asserting it matches the requested slice exactly.
    let content = deterministic_bytes(64 * 1024, 105);
    let server = TestServer::new()
        .serve_static("/slice", content.clone())
        .start()
        .await
        .expect("start");
    let t = HttpExecution::from_adapter(transport());
    let range = (17_345u64, 18_943u64);
    let mut resp = transfer_range(
        &t,
        &server.url("/slice"),
        range,
        Some(content.len() as u64),
        None,
    )
    .await
    .expect("valid slice");
    assert_eq!(resp.start, range.0);
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
    assert_eq!(
        bytes.len() as u64,
        resp.end - resp.start + 1,
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
    let t = HttpExecution::from_adapter(transport());
    // Established total from the probe was 4096, but the segment response
    // claims 3000: total conflict.
    let err = transfer_range(&t, &server.url("/mutated"), (0, 99), Some(4096), None)
        .await
        .expect_err("total conflict");
    assert!(
        matches!(err.error, DownloadError::InvalidRangeResponse(ref m) if m.contains("total conflict")),
        "got {err:?}"
    );
}

#[tokio::test]
async fn validators_passed_to_gate_detect_generation_change() {
    // The response carries a different ETag than the job's established
    // validators: the gate rejects with a resource-change error (§26).
    let server = TestServer::new()
        .serve_handler("/gen", |_req| {
            support::test_server::ScriptedResponse::new(206)
                .with_body(vec![1u8; 100])
                .with_header("etag", "\"generation-2\"")
                .with_header("content-range", "bytes 0-99/1000")
                .with_header("accept-ranges", "bytes")
        })
        .start()
        .await
        .expect("start");
    let t = HttpExecution::from_adapter(transport());
    let expected = kdown_engine::http::validators::ResourceValidators::from_headers(
        Some("\"generation-1\""),
        None,
        Some(1000),
    );
    let err = transfer_range(&t, &server.url("/gen"), (0, 99), Some(1000), Some(expected))
        .await
        .expect_err("generation change");
    assert!(matches!(err.error, DownloadError::ResourceChanged(_)));
}
