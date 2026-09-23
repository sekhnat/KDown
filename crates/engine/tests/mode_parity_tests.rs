//! Sequential/segmented classification parity (§32, task 5.4): the same
//! scripted HTTP failure scenarios run through both transfer modes must
//! report the same structured `DownloadError` categories and retry
//! behavior. The scenario table is the shared oracle; the mode is the
//! only variable.

mod support;

use std::time::Duration;

use kdown_engine::config::{
    EngineConfig, ExpectedHash, HashAlgorithm, IntegrityPolicy, OverwritePolicy, TransferPolicy,
};
use kdown_engine::error::ErrorCategory;
use kdown_engine::http::probe::ProbeMetadata;
use kdown_engine::http::scripted::{ProbeStep, ScriptedHttp, TransferOk, TransferStep};
use kdown_engine::http::HttpExecution;
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};
use kdown_engine::DownloadError;

use kdown_engine::metrics::events::{Event, EventStream};
use sha2::{Digest, Sha256, Sha512};
const TOTAL: u64 = 4000;

/// One failure scenario: the scripted transfer steps (after the probe)
/// and the expected terminal error category.
struct Scenario {
    name: &'static str,
    /// Fresh scripted transfer steps per run (steps own non-Clone
    /// outcomes); each entry produces one transfer call.
    steps: fn() -> Vec<TransferStep>,
    expected: ErrorCategory,
}

fn scenario(
    name: &'static str,
    steps: fn() -> Vec<TransferStep>,
    expected: ErrorCategory,
) -> Scenario {
    Scenario {
        name,
        steps,
        expected,
    }
}

fn gen_changed() -> DownloadError {
    DownloadError::ResourceChanged(
        "response validators disagree with the established generation".into(),
    )
}

/// The shared failure table (§32 parity oracle). Body resets and idle
/// timeouts exhaust the budget (max_attempts_per_segment = 1) so both
/// modes terminate deterministically with `RetryExhausted`.
fn scenarios() -> Vec<Scenario> {
    fn not_found() -> Vec<TransferStep> {
        vec![TransferStep::new().fail_error(DownloadError::NotFound { status: 404 })]
    }
    fn forbidden() -> Vec<TransferStep> {
        vec![TransferStep::new().fail_error(DownloadError::AuthorizationFailed)]
    }
    fn rate_limited() -> Vec<TransferStep> {
        vec![TransferStep::new().fail_retry_after(
            DownloadError::RateLimited { status: 429 },
            Duration::from_millis(1),
        )]
    }
    fn server_503() -> Vec<TransferStep> {
        vec![TransferStep::new().fail_error(DownloadError::Server { status: 503 })]
    }
    fn malformed_range() -> Vec<TransferStep> {
        vec![TransferStep::new()
            .fail_error(DownloadError::InvalidRangeResponse("start mismatch".into()))]
    }
    fn validator_conflict() -> Vec<TransferStep> {
        vec![TransferStep::new().fail_error(gen_changed())]
    }
    fn body_reset() -> Vec<TransferStep> {
        vec![TransferStep::new().fail_error(DownloadError::Connection("connection reset".into()))]
    }
    fn idle_timeout() -> Vec<TransferStep> {
        vec![TransferStep::new().ok(TransferOk::new().total(TOTAL).idle_timeout())]
    }
    vec![
        scenario("http status 404", not_found, ErrorCategory::NotFound),
        scenario(
            "http status 403",
            forbidden,
            ErrorCategory::AuthorizationFailed,
        ),
        scenario(
            "rate limited exhausts budget",
            rate_limited,
            ErrorCategory::RetryExhausted,
        ),
        scenario(
            "server 503 exhausts budget",
            server_503,
            ErrorCategory::RetryExhausted,
        ),
        scenario(
            "malformed range start",
            malformed_range,
            ErrorCategory::InvalidRangeResponse,
        ),
        scenario(
            "validator conflict",
            validator_conflict,
            ErrorCategory::ResourceChanged,
        ),
        scenario(
            "body reset exhausts budget",
            body_reset,
            ErrorCategory::RetryExhausted,
        ),
        scenario(
            "read idle timeout exhausts budget",
            idle_timeout,
            ErrorCategory::RetryExhausted,
        ),
    ]
}

fn mode_cfg(segmented: bool) -> EngineConfig {
    let mut c = EngineConfig {
        transfer: TransferPolicy {
            segmentation_threshold: if segmented { 1024 } else { u64::MAX },
            max_workers: 4,
            min_workers: 1,
            verify_range_support: false,
            ..TransferPolicy::default()
        },
        ..EngineConfig::default()
    };
    c.retry.base_delay = Duration::from_millis(1);
    c.retry.max_delay = Duration::from_millis(2);
    c.retry.max_attempts_per_segment = 1;
    c
}

async fn run_mode(segmented: bool, scenario: &Scenario) -> (ResultStatus, Option<ErrorCategory>) {
    let mut scripted = ScriptedHttp::new().expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
        status: 200,
        total_size: Some(TOTAL),
        accept_ranges: true,
        ..ProbeMetadata::default()
    }));
    for step in (scenario.steps)() {
        scripted = scripted.expect_transfer(step);
    }
    let dir = tempfile::tempdir().expect("tmp");
    let c = SingleStreamController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        mode_cfg(segmented),
    );
    let result = c
        .run(DownloadRequest::new(
            format!("https://parity/{}.bin", scenario.name.replace(' ', "-")),
            dir.path().join("out.bin"),
        ))
        .await
        .expect("terminal");
    (result.status, result.error.map(|e| e.category()))
}

/// The parity table: each scenario produces the same terminal status and
/// structured error category in both transfer modes (§32).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_and_segmented_report_the_same_categories() {
    for s in scenarios() {
        let (seq_status, seq_cat) = run_mode(false, &s).await;
        let (seg_status, seg_cat) = run_mode(true, &s).await;
        assert_eq!(
            seq_status,
            ResultStatus::Failed,
            "{}: {seq_status:?}",
            s.name
        );
        assert_eq!(seq_status, seg_status, "{}: terminal status", s.name);
        assert_eq!(
            seq_cat, seg_cat,
            "{}: structured error category must agree across transfer modes",
            s.name
        );
        assert_eq!(seq_cat, Some(s.expected), "{}: expected category", s.name);
    }
}

fn integrity_event_names(events: &mut EventStream) -> Vec<&'static str> {
    let mut names = Vec::new();
    while let Some(event) = events.try_next() {
        match event {
            Event::IntegrityCheckStarted => names.push("started"),
            Event::IntegrityCheckPassed => names.push("passed"),
            Event::IntegrityCheckFailed { .. } => names.push("failed"),
            Event::Warning { .. } => names.push("warning"),
            Event::Committed { .. } => names.push("committed"),
            _ => {}
        }
    }
    names
}

fn digest_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

async fn run_completion_mode(
    segmented: bool,
    hashes: Vec<ExpectedHash>,
    content: &[u8],
) -> (ResultStatus, Option<ErrorCategory>, Vec<&'static str>) {
    let mut config = mode_cfg(segmented);
    config.transfer.preallocate_output = false;
    if segmented {
        config.transfer.max_segment_size = 1000;
        config.transfer.min_segment_size = 1;
    }
    let scripted = ScriptedHttp::new().expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
        status: 200,
        total_size: Some(TOTAL),
        accept_ranges: true,
        range_verified: true,
        ..ProbeMetadata::default()
    }));
    let scripted = if segmented {
        let ranges = (0..TOTAL)
            .step_by(1000)
            .map(|start| {
                let end = (start + 999).min(TOTAL - 1);
                let chunk_end = (end as usize + 1).min(content.len());
                let chunk = if (start as usize) < content.len() {
                    content[start as usize..chunk_end].to_vec()
                } else {
                    Vec::new()
                };
                TransferStep::new().range((start, end)).ok(TransferOk::new()
                    .range(start, end)
                    .total(TOTAL)
                    .chunk(chunk))
            })
            .collect();
        scripted.expect_unordered_ranges("completion", ranges)
    } else {
        scripted.expect_transfer(
            TransferStep::new().ok(TransferOk::new().total(TOTAL).chunk(content.to_vec())),
        )
    };
    let dir = tempfile::tempdir().expect("tmp");
    let destination = dir.path().join("completion.bin");
    std::fs::write(&destination, b"previous destination").expect("seed old destination");
    let controller = SingleStreamController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        config,
    );
    let mut request = DownloadRequest::new("https://parity/completion.bin", destination.clone());
    request.expected_size = Some(TOTAL);
    request.overwrite = OverwritePolicy::Replace;
    request.integrity = IntegrityPolicy {
        expected_hashes: hashes,
        ..IntegrityPolicy::default()
    };
    let (handle, task) = controller.start(request);
    let mut events = handle.events();
    let result = task
        .await
        .expect("job task")
        .expect("structured terminal result");
    let published = std::fs::read(&destination).expect("read destination after completion");
    if result.status == ResultStatus::Completed {
        assert_eq!(
            published, content,
            "successful verification publishes new bytes"
        );
    } else {
        assert_eq!(
            published, b"previous destination",
            "failure preserves old destination"
        );
    }
    let completion_events = integrity_event_names(&mut events);
    scripted.assert_all_consumed();
    let requests = scripted.request_log();
    if segmented {
        assert_eq!(
            requests.len(),
            5,
            "probe plus four range requests: {requests:?}"
        );
        assert!(requests[1..].iter().all(|request| request.range.is_some()));
    } else {
        assert_eq!(
            requests.len(),
            2,
            "probe plus one sequential request: {requests:?}"
        );
        assert!(requests[1].range.is_none());
    }
    (
        result.status,
        result.error.as_ref().map(DownloadError::category),
        completion_events,
    )
}

async fn assert_completion_parity(
    scenario: &str,
    hashes: Vec<ExpectedHash>,
    expected_status: ResultStatus,
    expected_category: Option<ErrorCategory>,
    expected_events: &[&str],
    content: &[u8],
) {
    let sequential = run_completion_mode(false, hashes.clone(), content).await;
    let segmented = run_completion_mode(true, hashes, content).await;
    assert_eq!(
        sequential.0, expected_status,
        "{scenario}: sequential status"
    );
    assert_eq!(segmented.0, expected_status, "{scenario}: segmented status");
    assert_eq!(
        sequential.1, expected_category,
        "{scenario}: sequential category"
    );
    assert_eq!(
        segmented.1, expected_category,
        "{scenario}: segmented category"
    );
    assert_eq!(
        sequential.1, segmented.1,
        "{scenario}: error category parity"
    );
    assert_eq!(
        sequential.2, expected_events,
        "{scenario}: sequential events"
    );
    assert_eq!(segmented.2, expected_events, "{scenario}: segmented events");
    assert_eq!(
        sequential.2, segmented.2,
        "{scenario}: completion event ordering parity"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completion_verification_order_and_categories_match_across_modes() {
    let content: Vec<u8> = (0..TOTAL)
        .map(|index| ((index * 31 + 7) % 251) as u8)
        .collect();
    let sha256 = digest_hex(&Sha256::digest(&content));
    let sha512 = digest_hex(&Sha512::digest(&content));
    assert_completion_parity(
        "both supported hashes match",
        vec![
            ExpectedHash {
                algorithm: HashAlgorithm::Sha256,
                hex: sha256.clone(),
            },
            ExpectedHash {
                algorithm: HashAlgorithm::Sha512,
                hex: sha512.clone(),
            },
        ],
        ResultStatus::Completed,
        None,
        &["started", "passed", "committed"],
        &content,
    )
    .await;
    assert_completion_parity(
        "SHA-256 mismatch",
        vec![ExpectedHash {
            algorithm: HashAlgorithm::Sha256,
            hex: "0".repeat(64),
        }],
        ResultStatus::Failed,
        Some(ErrorCategory::IntegrityMismatch),
        &["started", "failed"],
        &content,
    )
    .await;
    assert_completion_parity(
        "SHA-512 mismatch after a matching SHA-256",
        vec![
            ExpectedHash {
                algorithm: HashAlgorithm::Sha256,
                hex: sha256,
            },
            ExpectedHash {
                algorithm: HashAlgorithm::Sha512,
                hex: "0".repeat(128),
            },
        ],
        ResultStatus::Failed,
        Some(ErrorCategory::IntegrityMismatch),
        &["started", "failed"],
        &content,
    )
    .await;
    assert_completion_parity(
        "known-size mismatch",
        vec![],
        ResultStatus::Failed,
        Some(ErrorCategory::IntegrityMismatch),
        &["started", "failed"],
        &content[..content.len() - 1],
    )
    .await;
}
