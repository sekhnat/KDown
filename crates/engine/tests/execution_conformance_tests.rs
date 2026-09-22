//! Shared semantic conformance cases (§32, task 3.3): the same scenario
//! table runs against the production Hyper adapter (real `TestServer`)
//! and the deterministic `ScriptedHttp` adapter, and both variants must
//! produce matching `DownloadError` categories, retry timing, and
//! accepted transfer metadata. This is what makes the scripted adapter a
//! faithful stand-in for orchestration tests.
//!
//! The scenario tables below are the independent oracle: they state the
//! wire behavior (production harness) and the equivalent semantic
//! outcome (scripted harness) without calling into the production
//! mapping code.

#[path = "support/mod.rs"]
mod support;

use std::time::Duration;

use kdown_engine::config::NetworkPolicy;
use kdown_engine::control::CancellationToken;
use kdown_engine::error::ErrorCategory;
use kdown_engine::http::scripted::{ScriptedHttp, TransferOk, TransferStep};
use kdown_engine::http::transport::{HttpTransport, RequestSpec};
use kdown_engine::http::{
    FullResponsePolicy, HttpExecution, HttpFailure, RangeIntent, ResourceValidators,
    TransferIntent, TransferRequest,
};
use kdown_engine::DownloadError;
use support::fixtures::deterministic_bytes;
use support::test_server::TestServer;

const CONTENT_SEED: u64 = 3311;
const CONTENT_LEN: u64 = 4096;

fn content() -> Vec<u8> {
    deterministic_bytes(CONTENT_LEN, CONTENT_SEED)
}

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
    expected_validators: Option<ResourceValidators>,
    full_response: FullResponsePolicy,
) -> TransferIntent {
    TransferIntent::Range(RangeIntent {
        range,
        established_total,
        expected_validators,
        full_response,
    })
}

/// Issue one ranged transfer through the seam.
async fn transfer_range(
    exec: &HttpExecution,
    url: &str,
    intent: TransferIntent,
) -> Result<kdown_engine::http::TransferResponse, HttpFailure> {
    exec.transfer(
        TransferRequest {
            spec: spec(url),
            intent,
        },
        &CancellationToken::new(),
    )
    .await
}

/// Consume a successful body to the end (byte-exact collect).
async fn collect(body: &mut kdown_engine::http::HttpBody) -> Vec<u8> {
    let cancel = CancellationToken::new();
    let mut out = Vec::new();
    loop {
        match body.next_chunk(&cancel).await.expect("body event") {
            kdown_engine::http::BodyEvent::Data(d) => out.extend_from_slice(&d),
            kdown_engine::http::BodyEvent::End => break,
            kdown_engine::http::BodyEvent::Paused => panic!("no pause expected in conformance"),
        }
    }
    out
}

/// The independent conformance oracle for status-to-error mapping
/// (§17.1/§20). (status, wire Retry-After, expected category, expected
/// parsed retry timing).
const STATUS_CASES: &[(u16, ErrorCategory, Option<Duration>)] = &[
    (404, ErrorCategory::NotFound, None),
    (401, ErrorCategory::AuthenticationRequired, None),
    (403, ErrorCategory::AuthorizationFailed, None),
    (408, ErrorCategory::Protocol, None),
    (503, ErrorCategory::Server, None),
];

#[tokio::test]
async fn status_mapping_conformance() {
    for (status, want_category, want_retry) in STATUS_CASES {
        // Production harness: a real server answering the raw status.
        let server = TestServer::new()
            .serve_handler("/status", move |_| {
                support::test_server::ScriptedResponse::new(*status)
            })
            .start()
            .await
            .expect("start");
        let prod = HttpExecution::from_adapter(transport());
        let failure = transfer_range(&prod, &server.url("/status"), TransferIntent::Full)
            .await
            .expect_err("status case fails");
        assert_eq!(
            failure.error.category(),
            *want_category,
            "production status {status}: {:?}",
            failure.error
        );
        assert_eq!(
            failure.retry_after, *want_retry,
            "production status {status}"
        );

        // Scripted harness: the same wire behavior expressed as the
        // table's semantic outcome; the step still proves the request
        // shape by matching it.
        let scripted_error = match want_category {
            ErrorCategory::NotFound => DownloadError::NotFound { status: *status },
            ErrorCategory::AuthenticationRequired => DownloadError::AuthenticationRequired,
            ErrorCategory::AuthorizationFailed => DownloadError::AuthorizationFailed,
            ErrorCategory::Protocol => DownloadError::Protocol("408 request timeout".into()),
            ErrorCategory::Server => DownloadError::Server { status: *status },
            other => panic!("unexpected oracle category {other:?}"),
        };
        let scripted = ScriptedHttp::new().expect_transfer(
            TransferStep::new()
                .url(server.url("/status"))
                .fail_error(scripted_error),
        );
        let exec = HttpExecution::from_adapter(scripted.clone());
        let failure = transfer_range(&exec, &server.url("/status"), TransferIntent::Full)
            .await
            .expect_err("scripted status case fails");
        assert_eq!(
            failure.error.category(),
            *want_category,
            "scripted status {status}: {:?}",
            failure.error
        );
        assert_eq!(failure.retry_after, *want_retry, "scripted status {status}");
        scripted.assert_all_consumed();
    }
}

#[tokio::test]
async fn rate_limited_retry_timing_conformance() {
    // Wire behavior: 429 with `Retry-After: 5` (§17.2).
    let server = TestServer::new()
        .serve_handler("/limited", |_| {
            support::test_server::ScriptedResponse::new(429).with_header("retry-after", "5")
        })
        .start()
        .await
        .expect("start");
    let url = server.url("/limited");

    // Production: the adapter must extract the timing.
    let prod = HttpExecution::from_adapter(transport());
    let failure = transfer_range(&prod, &url, TransferIntent::Full)
        .await
        .expect_err("429 fails");
    assert_eq!(failure.error.category(), ErrorCategory::RateLimited);
    assert_eq!(failure.retry_after, Some(Duration::from_secs(5)));

    // Scripted: the same semantic outcome, timing included.
    let scripted =
        ScriptedHttp::new().expect_transfer(TransferStep::new().url(url.clone()).fail_retry_after(
            DownloadError::RateLimited { status: 429 },
            Duration::from_secs(5),
        ));
    let exec = HttpExecution::from_adapter(scripted.clone());
    let failure = transfer_range(&exec, &url, TransferIntent::Full)
        .await
        .expect_err("scripted 429 fails");
    assert_eq!(failure.error.category(), ErrorCategory::RateLimited);
    assert_eq!(failure.retry_after, Some(Duration::from_secs(5)));
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn lying_200_range_rejection_conformance() {
    // Wire behavior: advertised ranges answered with 200 + full body
    // (§36.2 Full200). Rejection happens before any body byte.
    let server = TestServer::new()
        .serve_ranges("/liar", content(), support::test_server::RangeMode::Full200)
        .start()
        .await
        .expect("start");
    let url = server.url("/liar");
    let intent = range_intent(
        (100, 199),
        Some(CONTENT_LEN),
        None,
        FullResponsePolicy::InvalidRange,
    );

    let prod = HttpExecution::from_adapter(transport());
    let failure = transfer_range(&prod, &url, intent.clone())
        .await
        .expect_err("lying 200 rejects");
    assert!(
        matches!(failure.error, DownloadError::InvalidRangeResponse(ref m) if m.contains("full response to nonzero range")),
        "production: {failure:?}"
    );

    let scripted = ScriptedHttp::new().expect_transfer(
        TransferStep::new()
            .url(url.clone())
            .range((100, 199))
            .established_total(CONTENT_LEN)
            .fail_error(DownloadError::InvalidRangeResponse(
                "full response to nonzero range".into(),
            )),
    );
    let exec = HttpExecution::from_adapter(scripted.clone());
    let failure = transfer_range(&exec, &url, intent)
        .await
        .expect_err("scripted lying 200 rejects");
    assert!(
        matches!(failure.error, DownloadError::InvalidRangeResponse(_)),
        "scripted: {failure:?}"
    );
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn mismatched_content_range_conformance() {
    // Wire behavior: 206 whose Content-Range start disagrees with the
    // request (§36.2 MalformedContentRange shape).
    let server = TestServer::new()
        .serve_ranges(
            "/malformed",
            content(),
            support::test_server::RangeMode::MalformedContentRange,
        )
        .start()
        .await
        .expect("start");
    let url = server.url("/malformed");
    let intent = range_intent((0, 99), None, None, FullResponsePolicy::InvalidRange);

    let prod = HttpExecution::from_adapter(transport());
    let failure = transfer_range(&prod, &url, intent.clone())
        .await
        .expect_err("mismatched range rejects");
    assert!(
        matches!(failure.error, DownloadError::InvalidRangeResponse(ref m) if m.contains("start mismatch")),
        "production: {failure:?}"
    );

    let scripted = ScriptedHttp::new().expect_transfer(
        TransferStep::new()
            .url(url.clone())
            .range((0, 99))
            .fail_error(DownloadError::InvalidRangeResponse("start mismatch".into())),
    );
    let exec = HttpExecution::from_adapter(scripted.clone());
    let failure = transfer_range(&exec, &url, intent)
        .await
        .expect_err("scripted mismatched range rejects");
    assert!(
        matches!(failure.error, DownloadError::InvalidRangeResponse(_)),
        "scripted: {failure:?}"
    );
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn total_conflict_conformance() {
    // Wire behavior: Content-Range total contradicts the established
    // size (resource replaced, §26).
    let content = content();
    let server = TestServer::new()
        .serve_handler("/mutated", move |req| {
            let owned = content.clone();
            let total = owned.len() as u64;
            if let Some((s, e)) = req.range {
                support::test_server::ScriptedResponse::new(206)
                    .with_body(owned[s as usize..=(e as usize).min(owned.len() - 1)].to_vec())
                    .with_header("content-range", &format!("bytes {s}-{e}/{total}"))
            } else {
                support::test_server::ScriptedResponse::ok(owned)
                    .with_header("accept-ranges", "bytes")
            }
        })
        .start()
        .await
        .expect("start");
    let url = server.url("/mutated");
    // Established total from probe was 4096; the response claims 3000.
    let intent = range_intent((0, 99), Some(3000), None, FullResponsePolicy::InvalidRange);

    let prod = HttpExecution::from_adapter(transport());
    let failure = transfer_range(&prod, &url, intent.clone())
        .await
        .expect_err("total conflict rejects");
    assert!(
        matches!(failure.error, DownloadError::InvalidRangeResponse(ref m) if m.contains("total conflict")),
        "production: {failure:?}"
    );

    let scripted = ScriptedHttp::new().expect_transfer(
        TransferStep::new()
            .url(url.clone())
            .range((0, 99))
            .established_total(3000)
            .fail_error(DownloadError::InvalidRangeResponse("total conflict".into())),
    );
    let exec = HttpExecution::from_adapter(scripted.clone());
    let failure = transfer_range(&exec, &url, intent)
        .await
        .expect_err("scripted total conflict rejects");
    assert!(
        matches!(failure.error, DownloadError::InvalidRangeResponse(_)),
        "scripted: {failure:?}"
    );
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn generation_change_conformance() {
    // Wire behavior: the response validators disagree with the job's
    // established generation (§26) — via ETag comparison.
    let server = TestServer::new()
        .serve_handler("/gen", |_| {
            support::test_server::ScriptedResponse::new(206)
                .with_body(vec![1u8; 100])
                .with_header("etag", "\"generation-2\"")
                .with_header("content-range", "bytes 0-99/1000")
        })
        .start()
        .await
        .expect("start");
    let url = server.url("/gen");
    let gen1 = ResourceValidators::from_headers(Some("\"generation-1\""), None, Some(1000));
    let intent = range_intent(
        (0, 99),
        Some(1000),
        Some(gen1.clone()),
        FullResponsePolicy::InvalidRange,
    );

    let prod = HttpExecution::from_adapter(transport());
    let failure = transfer_range(&prod, &url, intent.clone())
        .await
        .expect_err("generation change");
    assert!(
        matches!(failure.error, DownloadError::ResourceChanged(_)),
        "production: {failure:?}"
    );

    // Scripted: the step matches the request's If-Range validators and
    // reports the same semantic resource-change outcome.
    let scripted = ScriptedHttp::new().expect_transfer(
        TransferStep::new()
            .url(url.clone())
            .range((0, 99))
            .established_total(1000)
            .validators(&gen1)
            .fail_error(DownloadError::ResourceChanged(
                "response validators disagree with the established generation".into(),
            )),
    );
    let exec = HttpExecution::from_adapter(scripted.clone());
    let failure = transfer_range(&exec, &url, intent)
        .await
        .expect_err("scripted generation change");
    assert!(
        matches!(failure.error, DownloadError::ResourceChanged(_)),
        "scripted: {failure:?}"
    );
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn if_range_ignored_full_response_conformance() {
    // Wire behavior: the server ignores If-Range and answers 200; the
    // conditional intent classifies this as a generation change (§26),
    // not an invalid range response.
    let server = TestServer::new()
        .serve_ranges(
            "/full200",
            content(),
            support::test_server::RangeMode::Full200,
        )
        .start()
        .await
        .expect("start");
    let url = server.url("/full200");
    let gen1 = ResourceValidators::from_headers(Some("\"generation-1\""), None, Some(CONTENT_LEN));
    let intent = range_intent(
        (100, 199),
        Some(CONTENT_LEN),
        Some(gen1.clone()),
        FullResponsePolicy::ResourceChanged,
    );

    let prod = HttpExecution::from_adapter(transport());
    let failure = transfer_range(&prod, &url, intent.clone())
        .await
        .expect_err("ignored If-Range");
    assert!(
        matches!(failure.error, DownloadError::ResourceChanged(ref m) if m.contains("If-Range")),
        "production: {failure:?}"
    );

    let scripted = ScriptedHttp::new().expect_transfer(
        TransferStep::new()
            .url(url.clone())
            .range((100, 199))
            .established_total(CONTENT_LEN)
            .validators(&gen1)
            .full_response_policy(FullResponsePolicy::ResourceChanged)
            .fail_error(DownloadError::ResourceChanged(
                "server ignored If-Range; resource changed".into(),
            )),
    );
    let exec = HttpExecution::from_adapter(scripted.clone());
    let failure = transfer_range(&exec, &url, intent)
        .await
        .expect_err("scripted ignored If-Range");
    assert!(
        matches!(failure.error, DownloadError::ResourceChanged(_)),
        "scripted: {failure:?}"
    );
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn accepted_ranged_transfer_metadata_conformance() {
    // Happy path: a correct 206 accepts exactly the requested slice with
    // the authoritative total — identical metadata and body bytes on
    // both adapters.
    let content = content();
    let range_bytes: Vec<u8> = content[100..200].to_vec();
    let server = TestServer::new()
        .serve_static("/ok", content.clone())
        .start()
        .await
        .expect("start");
    let url = server.url("/ok");
    let intent = range_intent(
        (100, 199),
        Some(CONTENT_LEN),
        None,
        FullResponsePolicy::InvalidRange,
    );

    let prod = HttpExecution::from_adapter(transport());
    let mut response = transfer_range(&prod, &url, intent.clone())
        .await
        .expect("production accepts");
    assert_eq!(response.start, 100);
    assert_eq!(response.end, 199);
    assert_eq!(response.total_size, Some(CONTENT_LEN));
    let prod_bytes = collect(&mut response.body).await;
    assert_eq!(&prod_bytes[..], &range_bytes[..]);

    // Scripted: same accepted metadata, same bytes.
    let validators = ResourceValidators::default();
    let scripted = ScriptedHttp::new().expect_transfer(
        TransferStep::new()
            .url(url.clone())
            .range((100, 199))
            .established_total(CONTENT_LEN)
            .ok(TransferOk::new()
                .range(100, 199)
                .total(CONTENT_LEN)
                .validators(validators)
                .chunk(range_bytes.clone())),
    );
    let exec = HttpExecution::from_adapter(scripted.clone());
    let mut response = transfer_range(&exec, &url, intent)
        .await
        .expect("scripted accepts");
    assert_eq!(response.start, 100);
    assert_eq!(response.end, 199);
    assert_eq!(response.total_size, Some(CONTENT_LEN));
    let scripted_bytes = collect(&mut response.body).await;
    assert_eq!(&scripted_bytes[..], &range_bytes[..]);
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn body_fault_after_prefix_conformance() {
    // Wire behavior: the server starts the body, then resets/truncates
    // the connection (§36.2). Both adapters deliver the same category
    // (Connection) after the accepted prefix — no adapter-specific
    // inspection in job orchestration.
    let server = TestServer::new()
        .serve_handler("/reset-mid", |_| {
            support::test_server::ScriptedResponse::ok(content()[..8].to_vec()).reset_after(2)
        })
        .start()
        .await
        .expect("start");
    let url = server.url("/reset-mid");
    let intent = TransferIntent::Full;
    let cancel = CancellationToken::new();

    // Production: the body faults mid-stream after a delivered prefix.
    let prod = HttpExecution::from_adapter(transport());
    let mut response = transfer_range(&prod, &url, intent.clone())
        .await
        .expect("headers validate before body");
    let mut prefix = Vec::new();
    let fault = loop {
        match response.body.next_chunk(&cancel).await {
            Ok(kdown_engine::http::BodyEvent::Data(d)) => prefix.extend_from_slice(&d),
            Ok(kdown_engine::http::BodyEvent::End) => panic!("expected a mid-body fault"),
            Ok(kdown_engine::http::BodyEvent::Paused) => panic!("no pause expected"),
            Err(e) => break e,
        }
    };
    assert_eq!(
        fault.category(),
        ErrorCategory::Connection,
        "production: {fault:?}"
    );
    assert!(
        !prefix.is_empty(),
        "a prefix was delivered before the fault"
    );

    // Scripted: chunk prefix, then the same classified fault.
    let scripted = ScriptedHttp::new().expect_transfer(
        TransferStep::new().url(url.clone()).ok(TransferOk::new()
            .chunk(b"ab".as_slice())
            .fault(DownloadError::Connection("connection reset".into()))),
    );
    let exec = HttpExecution::from_adapter(scripted.clone());
    let mut response = transfer_range(&exec, &url, intent)
        .await
        .expect("scripted accepts");
    let mut prefix = Vec::new();
    let fault = loop {
        match response.body.next_chunk(&cancel).await {
            Ok(kdown_engine::http::BodyEvent::Data(d)) => prefix.extend_from_slice(&d),
            Ok(kdown_engine::http::BodyEvent::End) => panic!("expected a mid-body fault"),
            Ok(kdown_engine::http::BodyEvent::Paused) => panic!("no pause expected"),
            Err(e) => break e,
        }
    };
    assert_eq!(
        fault.category(),
        ErrorCategory::Connection,
        "scripted: {fault:?}"
    );
    assert_eq!(&prefix[..], b"ab");
    scripted.assert_all_consumed();
}
