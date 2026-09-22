//! Sequential/segmented classification parity (§32, task 5.4): the same
//! scripted HTTP failure scenarios run through both transfer modes must
//! report the same structured `DownloadError` categories and retry
//! behavior. The scenario table is the shared oracle; the mode is the
//! only variable.

mod support;

use std::time::Duration;

use kdown_engine::config::{EngineConfig, TransferPolicy};
use kdown_engine::error::ErrorCategory;
use kdown_engine::http::probe::ProbeMetadata;
use kdown_engine::http::scripted::{ProbeStep, ScriptedHttp, TransferOk, TransferStep};
use kdown_engine::http::HttpExecution;
use kdown_engine::job::controller::{DownloadRequest, ResultStatus, SingleStreamController};
use kdown_engine::DownloadError;

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
