//! Checkpoint seam tests (§34, checkpoint-store-seam): focused coverage of
//! the shared scripted checkpoint adapter, then end-to-end orchestration
//! evidence for resolver selection, lifecycle-wide adapter use, mutation
//! ordering, and observable save/delete failure semantics.

#[path = "support/mod.rs"]
mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use kdown_engine::config::EngineConfig;
use kdown_engine::http::probe::ProbeMetadata;
use kdown_engine::http::scripted::{ProbeStep, ScriptedHttp, TransferOk, TransferStep};
use kdown_engine::http::transport::HttpTransport;
use kdown_engine::http::validators::ResourceValidators;
use kdown_engine::http::HttpExecution;
use kdown_engine::job::controller::{
    CancelMode, DownloadController, DownloadRequest, ResultStatus,
};
use kdown_engine::job::JobState;
use kdown_engine::metrics::events::Event;
use kdown_engine::resume::checkpoint::Checkpoint;
use kdown_engine::resume::checkpoint_store::{
    CheckpointResolveContext, CheckpointStore, CheckpointStoreResolver,
};
use kdown_engine::resume::{Checkpoint as EngineCheckpoint, CheckpointError, DurabilityMode};
use kdown_engine::DownloadError;
use support::checkpoint::ScriptedCheckpointStore;
use support::fixtures::deterministic_bytes;

fn sample(job: &str, end: u64) -> Checkpoint {
    let mut cp = Checkpoint::new(job, "https://example/f", "tmp-1");
    cp.total_size = Some(1000);
    cp.record_completed(0, end);
    cp
}

async fn wait_for_delete(store: &ScriptedCheckpointStore) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if store.ops().iter().any(|operation| operation.is_delete()) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("checkpoint delete reached its synchronization gate");
}

/// Wait until the store records at least one save operation. Unlike the
/// delete gate (which the completion path always reaches), a cadence save
/// only happens while the job is still transferring — tests that need the
/// delete-after-save ordering must keep the job alive (for example with a
/// scripted gate) until this observes a save.
async fn wait_for_save(store: &ScriptedCheckpointStore) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if store.ops().iter().any(|operation| operation.is_save()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("cadence save recorded while the job was still running");
}

// ---- Scripted adapter focused tests ----

#[test]
fn in_memory_roundtrip_and_missing_load() {
    let store = ScriptedCheckpointStore::new();
    assert_eq!(store.load("job").expect("load"), None, "absent");
    let mut cp = sample("job", 49);
    cp.record_completed(100, 149);
    store.save_atomic(&cp).expect("save");
    let loaded = store.load("job").expect("load").expect("stored");
    assert_eq!(loaded.completed_ranges, vec![(0, 49), (100, 149)]);
    store.delete("job").expect("delete");
    assert_eq!(store.load("job").expect("load"), None, "removed");
    store.delete("absent").expect("missing delete ok");
    assert_eq!(
        store.counts(),
        (3, 1, 2),
        "3 loads, 1 save, 2 deletes recorded"
    );
}

#[test]
fn queued_failures_inject_per_operation_fifo() {
    let store = ScriptedCheckpointStore::new();
    // FIFO queues: first op of each kind fails, later ops succeed.
    store.fail_next_load(CheckpointError::Corrupt("load boom".into()));
    store.fail_next_save(CheckpointError::Corrupt("save boom".into()));
    store.fail_next_delete(CheckpointError::Corrupt("delete boom".into()));
    assert!(store.load("job").is_err(), "queued load failure");
    assert!(store.load("job").is_ok(), "next load succeeds");
    assert!(
        store.save_atomic(&sample("job", 9)).is_err(),
        "queued save failure"
    );
    store
        .save_atomic(&sample("job", 49))
        .expect("later save ok");
    assert!(store.delete("job").is_err(), "queued delete failure");
    assert!(
        store.stored("job").is_some(),
        "failed delete does not remove state"
    );
    store.delete("job").expect("later delete ok");
    assert!(store.stored("job").is_none(), "successful delete removes");
    let ops = store.ops();
    assert!(
        !ops[0].outcome_ok() && ops[0].is_load(),
        "first op is the failing load: {ops:?}"
    );
    let saves: Vec<_> = ops.iter().filter(|o| o.is_save()).collect();
    assert_eq!(saves.len(), 2, "failed + successful save");
    assert!(!saves[0].outcome_ok(), "first save failed: {saves:?}");
    assert!(saves[1].outcome_ok(), "second save succeeded: {saves:?}");
    let deletes: Vec<_> = ops.iter().filter(|o| o.is_delete()).collect();
    assert_eq!(deletes.len(), 2, "failed + successful delete");
    assert!(!deletes[0].outcome_ok(), "first delete failed: {deletes:?}");
    assert!(
        deletes[1].outcome_ok(),
        "second delete succeeded: {deletes:?}"
    );
}

#[test]
fn held_save_blocks_inside_adapter_and_overlap_is_detected() {
    // Deterministic gate: the first save enters and holds; a concurrent
    // save cannot enter the adapter — the overlap counter records it
    // without any wall-clock race — until the gate releases.
    let store = ScriptedCheckpointStore::new();
    let gate = store.hold_save(1);
    let s1 = store.clone();
    let s2 = store.clone();
    let holder = {
        let s = s1.clone();
        std::thread::spawn(move || s.save_atomic(&sample("job", 49)))
    };
    // Wait until the first save is inside the adapter (log entry visible).
    while store.ops().is_empty() {
        std::thread::yield_now();
    }
    let second = {
        let s = s2.clone();
        std::thread::spawn(move || s.save_atomic(&sample("job", 99)))
    };
    // The raw adapter does not serialize: the second save enters while
    // the first is held, and the overlap counter records that concurrent
    // presence without any wall-clock race.
    while store.ops().len() < 2 {
        std::thread::yield_now();
    }
    assert_eq!(store.overlaps(), 1, "concurrent entry detected");
    // The ungated save completes on its own; only the held save remains.
    while store.active_ops() > 1 {
        std::thread::yield_now();
    }
    assert_eq!(store.active_ops(), 1, "held save still in flight");
    gate.release();
    holder.join().expect("join holder").expect("save 1 ok");
    second.join().expect("join second").expect("save 2 ok");
    assert_eq!(store.overlaps(), 1, "exactly the one concurrent entry");
    // Stored state reflects whichever save completed last.
    assert!(store.stored("job").is_some());
}

#[test]
fn operation_log_records_exact_order() {
    let store = ScriptedCheckpointStore::new();
    store.save_atomic(&sample("job", 9)).expect("save 1");
    store.save_atomic(&sample("job", 19)).expect("save 2");
    store.delete("job").expect("delete");
    let ops = store.ops();
    assert_eq!(ops.len(), 3);
    assert!(ops[0].is_save(), "{ops:?}");
    assert_eq!(ops[0].save_ranges(), Some(&[(0, 9)][..]));
    assert!(ops[1].is_save(), "{ops:?}");
    assert_eq!(ops[1].save_ranges(), Some(&[(0, 19)][..]));
    assert!(ops[2].is_delete() && ops[2].outcome_ok(), "{ops:?}");
    // Save-before-delete ordering is directly observable.
    assert!(
        ops.iter().position(|o| o.is_save()).unwrap()
            < ops.iter().position(|o| o.is_delete()).unwrap(),
        "save precedes delete"
    );
}

// ---- Controller resolver selection (task 2.1, §34) ----

/// Test resolver wrapping the scripted adapter and recording every
/// resolution context (one entry per job).
struct RecordingResolver {
    store: ScriptedCheckpointStore,
    contexts: Mutex<Vec<CheckpointResolveContext>>,
    /// When set, resolution fails for every job (pre-probe failure case).
    fail_with: Option<CheckpointError>,
}

impl RecordingResolver {
    fn new(store: ScriptedCheckpointStore) -> Self {
        Self {
            store,
            contexts: Mutex::new(vec![]),
            fail_with: None,
        }
    }

    fn failing(err: CheckpointError) -> Self {
        Self {
            store: ScriptedCheckpointStore::new(),
            contexts: Mutex::new(vec![]),
            fail_with: Some(err),
        }
    }

    fn contexts(&self) -> Vec<CheckpointResolveContext> {
        self.contexts.lock().expect("contexts").clone()
    }
}

impl CheckpointStoreResolver for RecordingResolver {
    fn resolve(
        &self,
        context: &CheckpointResolveContext,
    ) -> Result<Arc<dyn CheckpointStore>, CheckpointError> {
        self.contexts
            .lock()
            .expect("contexts")
            .push(context.clone());
        if let Some(err) = &self.fail_with {
            return Err(err.clone());
        }
        let store: Arc<dyn CheckpointStore> = Arc::new(self.store.clone());
        Ok(store)
    }
}

fn fresh_download_script(content: &[u8]) -> ScriptedHttp {
    ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(content.len() as u64),
            ..ProbeMetadata::default()
        }))
        .expect_transfer(
            TransferStep::new().ok(TransferOk::new()
                .total(content.len() as u64)
                .chunk(content.to_vec())),
        )
}

#[tokio::test]
async fn resolver_resolves_once_per_job_with_destination_context() {
    let scripted = fresh_download_script(b"hello")
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(5),
            ..ProbeMetadata::default()
        }))
        .expect_transfer(
            TransferStep::new().ok(TransferOk::new().total(5).chunk(b"world".to_vec())),
        );
    let store = ScriptedCheckpointStore::new();
    let resolver = Arc::new(RecordingResolver::new(store.clone()));
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted),
        EngineConfig::default(),
    )
    .with_checkpoint_resolver(resolver.clone());

    let dir = tempfile::tempdir().expect("tmp");
    let dest_a = dir.path().join("a").join("out.bin");
    let dest_b = dir.path().join("b").join("out.bin");
    std::fs::create_dir_all(dest_a.parent().expect("parent a")).expect("mkdir a");
    std::fs::create_dir_all(dest_b.parent().expect("parent b")).expect("mkdir b");

    let result_a = controller
        .run(DownloadRequest::new(
            "https://scripted/one.bin",
            dest_a.clone(),
        ))
        .await
        .expect("terminal a");
    assert_eq!(result_a.status, ResultStatus::Completed, "{result_a:?}");
    let result_b = controller
        .run(DownloadRequest::new(
            "https://scripted/two.bin",
            dest_b.clone(),
        ))
        .await
        .expect("terminal b");
    assert_eq!(result_b.status, ResultStatus::Completed, "{result_b:?}");

    // Exactly one resolution per job.
    let contexts = resolver.contexts();
    assert_eq!(contexts.len(), 2, "one resolution per job: {contexts:?}");
    assert_eq!(contexts[0].destination, dest_a, "job A destination context");
    assert_eq!(contexts[1].destination, dest_b, "job B destination context");
    assert_ne!(
        contexts[0].job_identity, contexts[1].job_identity,
        "distinct jobs carry distinct identities"
    );
    assert_eq!(
        contexts[0].durability,
        DurabilityMode::Performance,
        "configured durability carried into the context"
    );
    // Every checkpoint operation crossed the selected adapter: one load per
    // job during admission and one delete per job after commit (§14.6).
    assert_eq!(store.counts(), (2, 0, 2), "adapter ops: {:?}", store.ops());
    // No file sidecar was created: the alternate adapter served the jobs.
    assert!(!dir.path().join("a").join("out.bin.kdown").exists());
    assert!(!dir.path().join("b").join("out.bin.kdown").exists());
}

#[tokio::test]
async fn resolution_failure_fails_job_before_probing() {
    let scripted = fresh_download_script(b"hello");
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        EngineConfig::default(),
    )
    .with_checkpoint_resolver(Arc::new(RecordingResolver::failing(
        CheckpointError::Corrupt("no storage for this job".into()),
    )));

    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("out.bin");
    let result = controller
        .run(DownloadRequest::new("https://scripted/x.bin", dest))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Failed, "{result:?}");
    assert!(
        matches!(
            result.error.as_ref().expect("error"),
            DownloadError::Checkpoint(_)
        ),
        "checkpoint-category failure: {result:?}"
    );
    // Pre-probe: the job failed before any network activity.
    assert!(
        scripted.request_log().is_empty(),
        "resolution failure must fail before probing: {:?}",
        scripted.request_log()
    );
}

#[tokio::test]
async fn existing_construction_paths_remain_compatible() {
    // All four production constructors still build, and the consuming
    // injection chains onto each of them (§34: no constructor matrix).
    let transport = HttpTransport::new(EngineConfig::default().network).expect("transport");
    let config = EngineConfig::default();
    let _c1 = DownloadController::new(transport.clone(), config.clone());
    let _c2 = DownloadController::with_metrics(transport.clone(), config.clone(), {
        use kdown_engine::EngineMetrics;
        EngineMetrics::shared()
    });
    let _c3 = DownloadController::with_execution(
        HttpExecution::from_adapter(fresh_download_script(b"hi")),
        config.clone(),
    );
    let scripted = fresh_download_script(b"hi");
    let _c4 = DownloadController::with_execution_and_metrics(
        HttpExecution::from_adapter(scripted),
        config.clone(),
        kdown_engine::EngineMetrics::shared(),
    );

    // Injection chains from an existing constructor and the job completes
    // through the injected adapter (sidecar-free).
    let scripted = fresh_download_script(b"hi");
    let store = ScriptedCheckpointStore::new();
    let resolver = Arc::new(RecordingResolver::new(store.clone()));
    let controller =
        DownloadController::with_execution(HttpExecution::from_adapter(scripted), config)
            .with_checkpoint_resolver(resolver.clone());
    let dir = tempfile::tempdir().expect("tmp");
    let dest: PathBuf = dir.path().join("out.bin");
    let result = controller
        .run(DownloadRequest::new("https://scripted/compat.bin", dest))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(store.counts(), (1, 0, 1), "adapter served the job");
    assert!(!dir.path().join("out.bin.kdown").exists(), "no sidecar");
}

// ---- Sequential lifecycle through the selected adapter (tasks 2.2-2.4) ----

async fn wait_until<F: Fn() -> bool>(cond: F, timeout: Duration, what: &str) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn pause_mid_body_script() -> ScriptedHttp {
    // Probe (known size) + transfer that delivers one chunk then parks the
    // body until cancellation: pausing mid-body is fully deterministic.
    ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(10),
            ..ProbeMetadata::default()
        }))
        .expect_transfer(
            TransferStep::new().ok(TransferOk::new()
                .total(10)
                .chunk(b"hello".to_vec())
                .wait_for_cancellation()),
        )
}

#[tokio::test]
async fn custom_adapter_serves_load_save_delete_without_sidecar() {
    let scripted = pause_mid_body_script();
    let store = ScriptedCheckpointStore::new();
    let resolver = Arc::new(RecordingResolver::new(store.clone()));
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        EngineConfig::default(),
    )
    .with_checkpoint_resolver(resolver.clone());
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("out.bin");
    let (handle, join) = controller.start(DownloadRequest::new(
        "https://scripted/lifecycle.bin",
        dest.clone(),
    ));
    // First chunk written; the body now parks deterministically.
    wait_until(
        || handle.snapshot().network_bytes >= 5,
        Duration::from_secs(10),
        "first chunk",
    )
    .await;
    handle.pause();
    // The pause save crosses the selected adapter (§9.3 step 5).
    wait_until(
        || store.ops().iter().any(|o| o.is_save()),
        Duration::from_secs(10),
        "pause save",
    )
    .await;
    handle.resume_now();
    handle.cancel(); // DeletePartial: checkpoint cleanup requested
    let result = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Cancelled, "{result:?}");
    // Exact custom-adapter ordering: admission load, pause save, cleanup
    // delete — no file-sidecar fallback anywhere in the lifecycle.
    let ops = store.ops();
    assert_eq!(store.counts(), (1, 1, 1), "ops: {ops:?}");
    let load_idx = ops.iter().position(|o| o.is_load()).expect("load");
    let save_idx = ops.iter().position(|o| o.is_save()).expect("save");
    let delete_idx = ops.iter().position(|o| o.is_delete()).expect("delete");
    assert!(
        load_idx < save_idx && save_idx < delete_idx,
        "order: {ops:?}"
    );
    assert_eq!(
        ops[save_idx].save_ranges(),
        Some(&[(0, 4)][..]),
        "pause save recorded the written prefix"
    );
    assert!(
        !dir.path().join("out.bin.kdown").exists(),
        "no sidecar created for the alternate adapter"
    );
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn pause_save_failure_fails_job_and_preserves_partial_output() {
    let scripted = pause_mid_body_script();
    let store = ScriptedCheckpointStore::new();
    store.fail_next_save(CheckpointError::Corrupt("save boom".into()));
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted),
        EngineConfig::default(),
    )
    .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("out.bin");
    let (handle, join) = controller.start(DownloadRequest::new(
        "https://scripted/pausefail.bin",
        dest.clone(),
    ));
    wait_until(
        || handle.snapshot().network_bytes >= 5,
        Duration::from_secs(10),
        "first chunk",
    )
    .await;
    handle.pause();
    let result = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    // Structured checkpoint failure: no Paused success, no commit.
    assert_eq!(result.status, ResultStatus::Failed, "{result:?}");
    assert!(
        matches!(
            result.error.as_ref().expect("error"),
            DownloadError::Checkpoint(_)
        ),
        "checkpoint-category failure: {result:?}"
    );
    assert!(result.final_path.is_none(), "no committed output");
    // Consistent partial output preserved for diagnosis/recovery (§14.5).
    let temp = dir.path().join("out.bin.part");
    assert!(temp.exists(), "partial output preserved");
    assert_eq!(std::fs::metadata(&temp).expect("meta").len(), 10);
    assert!(!dest.exists(), "no final file");
}

#[tokio::test]
async fn cadence_save_failure_fails_job_and_preserves_partial_output() {
    let scripted = pause_mid_body_script();
    let store = ScriptedCheckpointStore::new();
    store.fail_next_save(CheckpointError::Corrupt("cadence boom".into()));
    // 1ns cadence: the first written chunk deterministically triggers the
    // cadence save (the init-to-check path spans awaited scheduling).
    let config = EngineConfig {
        checkpoint_flush_interval: Duration::from_nanos(1),
        ..EngineConfig::default()
    };
    let controller =
        DownloadController::with_execution(HttpExecution::from_adapter(scripted), config)
            .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("out.bin");
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        controller.run(DownloadRequest::new(
            "https://scripted/cadencefail.bin",
            dest.clone(),
        )),
    )
    .await
    .expect("no hang")
    .expect("terminal");
    assert_eq!(result.status, ResultStatus::Failed, "{result:?}");
    assert!(
        matches!(
            result.error.as_ref().expect("error"),
            DownloadError::Checkpoint(_)
        ),
        "checkpoint-category failure: {result:?}"
    );
    assert!(result.final_path.is_none());
    let temp = dir.path().join("out.bin.part");
    assert!(temp.exists(), "partial output preserved");
    assert!(!dest.exists(), "no commit after cadence save failure");
    // The failing cadence save reached the adapter and was recorded.
    assert!(
        store.ops().iter().any(|o| o.is_save() && !o.outcome_ok()),
        "failing save crossed the adapter: {:?}",
        store.ops()
    );
}

#[tokio::test]
async fn resume_refresh_save_failure_preserves_previous_checkpoint() {
    let scripted = pause_mid_body_script();
    let store = ScriptedCheckpointStore::new();
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("out.bin");
    let identity = kdown_engine::resume::job_identity("https://scripted/refresh.bin", &dest);
    // Prior usable state: checkpoint + temp file covering its ranges.
    let mut prior = EngineCheckpoint::new(&identity, "https://scripted/refresh.bin", "tmp");
    prior.total_size = Some(10);
    prior.record_completed(0, 4);
    store.save_atomic(&prior).expect("seed checkpoint");
    std::fs::write(dir.path().join("out.bin.part"), vec![0u8; 5]).expect("temp");
    // The resume-refresh save fails.
    store.fail_next_save(CheckpointError::Corrupt("refresh boom".into()));
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted),
        EngineConfig::default(),
    )
    .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let result = controller
        .run(DownloadRequest::new(
            "https://scripted/refresh.bin",
            dest.clone(),
        ))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Failed, "{result:?}");
    assert!(
        matches!(
            result.error.as_ref().expect("error"),
            DownloadError::Checkpoint(_)
        ),
        "checkpoint-category failure: {result:?}"
    );
    assert!(result.final_path.is_none(), "no commit");
    // The previous checkpoint is untouched (no delete, no overwrite).
    let kept = store.stored(&identity).expect("previous checkpoint kept");
    assert_eq!(kept.completed_ranges, vec![(0, 4)]);
    let ops = store.ops();
    assert!(
        ops.iter().all(|o| !o.is_delete()),
        "save failure never deletes prior state: {ops:?}"
    );
    // The partial temp output is preserved.
    assert!(dir.path().join("out.bin.part").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_commit_delete_failure_retains_completed_with_warning() {
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(5),
            ..ProbeMetadata::default()
        }))
        .expect_transfer(
            TransferStep::new().ok(TransferOk::new().total(5).chunk(b"hello".to_vec())),
        );
    let store = ScriptedCheckpointStore::new();
    store.fail_next_delete(CheckpointError::Corrupt("delete boom".into()));
    let delete_gate = store.hold_delete(1);
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        EngineConfig::default(),
    )
    .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("out.bin");
    let (handle, join) = controller.start(DownloadRequest::new(
        "https://scripted/commitfail.bin",
        dest.clone(),
    ));
    let mut events = handle.events();
    wait_for_delete(&store).await;
    let published_before_delete = std::fs::read(&dest);
    let state_before_delete = handle.state();
    delete_gate.release();
    let result = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(
        published_before_delete.expect("published before delete"),
        b"hello"
    );
    assert_eq!(
        state_before_delete,
        JobState::Committing,
        "delete precedes terminal state"
    );
    // The committed outcome is preserved (§9.2, §14.6 exception).
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(handle.state(), JobState::Completed);
    assert_eq!(result.final_path.as_deref(), Some(dest.as_path()));
    assert!(
        result
            .warnings
            .iter()
            .any(|w| w.contains("checkpoint cleanup incomplete")),
        "warning in terminal result: {:?}",
        result.warnings
    );
    // The warning is visible as an event too.
    let mut completion_events = Vec::new();
    while let Some(event) = events.try_next() {
        match event {
            Event::Warning { detail } if detail.contains("checkpoint cleanup incomplete") => {
                completion_events.push("warning");
            }
            Event::Committed { .. } => completion_events.push("committed"),
            _ => {}
        }
    }
    assert_eq!(completion_events, ["warning", "committed"]);
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn cancellation_delete_failure_retains_cancelled_with_warning() {
    let scripted = pause_mid_body_script();
    let store = ScriptedCheckpointStore::new();
    store.fail_next_delete(CheckpointError::Corrupt("cancel delete boom".into()));
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        EngineConfig::default(),
    )
    .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("out.bin");
    let (handle, join) = controller.start(DownloadRequest::new(
        "https://scripted/cancelfail.bin",
        dest.clone(),
    ));
    wait_until(
        || handle.snapshot().network_bytes >= 5,
        Duration::from_secs(10),
        "first chunk",
    )
    .await;
    let mut events = handle.events();
    handle.cancel(); // DeletePartial
    let result = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Cancelled, "{result:?}");
    assert!(
        result
            .warnings
            .iter()
            .any(|w| w.contains("checkpoint cleanup incomplete")),
        "warning in terminal result: {:?}",
        result.warnings
    );
    let mut saw_warning = false;
    while let Some(event) = events.try_next() {
        if let Event::Warning { detail } = event {
            if detail.contains("checkpoint cleanup incomplete") {
                saw_warning = true;
            }
        }
    }
    assert!(saw_warning, "warning event published");
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn keep_partial_cancellation_does_not_delete_checkpoint() {
    let scripted = pause_mid_body_script();
    let store = ScriptedCheckpointStore::new();
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        EngineConfig::default(),
    )
    .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("out.bin");
    let (handle, join) = controller.start(DownloadRequest::new(
        "https://scripted/keep.bin",
        dest.clone(),
    ));
    wait_until(
        || handle.snapshot().network_bytes >= 5,
        Duration::from_secs(10),
        "first chunk",
    )
    .await;
    handle.pause();
    wait_until(
        || store.ops().iter().any(|o| o.is_save()),
        Duration::from_secs(10),
        "pause save",
    )
    .await;
    handle.cancel_with(CancelMode::KeepPartial);
    let result = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Cancelled, "{result:?}");
    assert!(
        result.warnings.is_empty(),
        "no cleanup warnings without deletion: {:?}",
        result.warnings
    );
    // KeepPartial preserves the checkpoint: no delete crosses the adapter.
    assert_eq!(store.counts(), (1, 1, 0), "ops: {:?}", store.ops());
    let identity = kdown_engine::resume::job_identity("https://scripted/keep.bin", &dest);
    let kept = store.stored(&identity).expect("checkpoint preserved");
    assert_eq!(kept.completed_ranges, vec![(0, 4)]);
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn keep_file_discard_checkpoint_delete_failure_warns() {
    let scripted = pause_mid_body_script();
    let store = ScriptedCheckpointStore::new();
    store.fail_next_delete(CheckpointError::Corrupt("keep-file boom".into()));
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        EngineConfig::default(),
    )
    .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("out.bin");
    let (handle, join) = controller.start(DownloadRequest::new(
        "https://scripted/keepfile.bin",
        dest.clone(),
    ));
    wait_until(
        || handle.snapshot().network_bytes >= 5,
        Duration::from_secs(10),
        "first chunk",
    )
    .await;
    handle.cancel_with(CancelMode::KeepFileDiscardCheckpoint);
    let result = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Cancelled, "{result:?}");
    assert!(
        result
            .warnings
            .iter()
            .any(|w| w.contains("checkpoint cleanup incomplete")),
        "warning: {:?}",
        result.warnings
    );
    assert_eq!(store.counts(), (1, 0, 1), "delete attempted once");
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn admission_delete_failure_remains_fail_closed() {
    // Corrupt load triggers admission cleanup; a failing delete must keep
    // failing the job before any transfer (safety boundary, §15.5).
    let scripted = pause_mid_body_script();
    let store = ScriptedCheckpointStore::new();
    store.fail_next_load(CheckpointError::Corrupt("corrupt state".into()));
    store.fail_next_delete(CheckpointError::Corrupt("admission delete boom".into()));
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        EngineConfig::default(),
    )
    .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("out.bin");
    let result = controller
        .run(DownloadRequest::new("https://scripted/admission.bin", dest))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Failed, "{result:?}");
    assert!(
        matches!(
            result.error.as_ref().expect("error"),
            DownloadError::Checkpoint(_)
        ),
        "fail-closed admission cleanup: {result:?}"
    );
    assert!(
        scripted.request_log().is_empty(),
        "failed before any network activity: {:?}",
        scripted.request_log()
    );
    assert_eq!(store.counts(), (1, 0, 1), "load then failed delete");
}

// ---- Segmented lifecycle through the selected adapter (tasks 3.1-3.3) ----

fn segmented_cfg() -> EngineConfig {
    let mut c = EngineConfig::default();
    c.transfer.segmentation_threshold = 1024;
    c.transfer.max_workers = 2;
    c.transfer.min_workers = 1;
    c.transfer.max_segment_size = 1000;
    c.transfer.min_segment_size = 1;
    c
}

const SEG_TOTAL: u64 = 4000;

/// Four deterministic 1000-byte leases; steps optionally park the body at
/// wait_for_cancellation so pause lands deterministically mid-transfer.
fn segmented_script(park: bool) -> (ScriptedHttp, Arc<Vec<u8>>) {
    let content = Arc::new(deterministic_bytes(SEG_TOTAL, 77_001));
    let steps = (0..4u64)
        .map(|i| {
            let start = i * 1000;
            let end = start + 999;
            let slice = content[start as usize..(end + 1) as usize].to_vec();
            let ok = TransferOk::new()
                .range(start, end)
                .total(SEG_TOTAL)
                .chunk(slice);
            let ok = if park { ok.wait_for_cancellation() } else { ok };
            TransferStep::new().range((start, end)).ok(ok)
        })
        .collect();
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(SEG_TOTAL),
            accept_ranges: true,
            range_verified: true,
            ..ProbeMetadata::default()
        }))
        .expect_unordered_ranges("segments", steps);
    (scripted, content)
}

fn recorded_saves(store: &ScriptedCheckpointStore) -> Vec<Vec<(u64, u64)>> {
    store
        .ops()
        .iter()
        .filter_map(|o| o.save_ranges().map(|r| r.to_vec()))
        .collect()
}

#[tokio::test]
async fn segmented_pause_persists_absorbed_snapshot_monotonically() {
    let (scripted, _content) = segmented_script(true);
    let store = ScriptedCheckpointStore::new();
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        segmented_cfg(),
    )
    .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("seg.bin");
    let (handle, join) = controller.start(DownloadRequest::new(
        "https://scripted/seg-pause.bin",
        dest.clone(),
    ));
    // Both active workers wrote their first lease chunk.
    wait_until(
        || handle.snapshot().network_bytes >= 2000,
        Duration::from_secs(10),
        "first lease chunks",
    )
    .await;
    handle.pause();
    // The pause persists the absorbed scheduler snapshot through the
    // selected adapter (§9.3 step 5, §15.4).
    wait_until(
        || !recorded_saves(&store).is_empty() && store.active_ops() == 0,
        Duration::from_secs(10),
        "pause persistence",
    )
    .await;
    handle.resume_now();
    handle.cancel_with(CancelMode::KeepPartial);
    let result = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Cancelled, "{result:?}");
    let saves = recorded_saves(&store);
    assert!(!saves.is_empty(), "pause persisted a snapshot: {saves:?}");
    // Accepted progress never regresses: each recorded save covers at
    // least everything the previous one did (§12 mutation ordering).
    for pair in saves.windows(2) {
        assert!(
            covers(&pair[1], &pair[0]),
            "regressed save: {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }
    // KeepPartial: no checkpoint deletion crossed the adapter.
    assert!(
        store.ops().iter().all(|o| !o.is_delete()),
        "{:?}",
        store.ops()
    );
    assert_eq!(store.overlaps(), 0, "coordinated saves never overlap");
    assert!(
        !dir.path().join("seg.bin.kdown").exists(),
        "no sidecar for the alternate adapter"
    );
    // Partial output preserved (KeepPartial).
    assert!(dir.path().join("seg.bin.part").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segmented_save_failure_converges_workers_to_failed() {
    let (scripted, _content) = segmented_script(false);
    let store = ScriptedCheckpointStore::new();
    store.fail_next_save(CheckpointError::Corrupt("segmented boom".into()));
    // 1ns cadence: the first written chunk deterministically triggers the
    // failing save; the observing worker installs the shared fatal error
    // and every worker converges before run_segmented returns failure.
    let config = EngineConfig {
        checkpoint_flush_interval: Duration::from_nanos(1),
        ..segmented_cfg()
    };
    let controller =
        DownloadController::with_execution(HttpExecution::from_adapter(scripted), config)
            .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("seg.bin");
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        controller.run(DownloadRequest::new(
            "https://scripted/seg-fail.bin",
            dest.clone(),
        )),
    )
    .await
    .expect("no hang (workers converged)")
    .expect("terminal");
    assert_eq!(result.status, ResultStatus::Failed, "{result:?}");
    assert!(
        matches!(
            result.error.as_ref().expect("error"),
            DownloadError::Checkpoint(_)
        ),
        "checkpoint-category failure: {result:?}"
    );
    assert!(result.final_path.is_none(), "no commit");
    assert!(
        dir.path().join("seg.bin.part").exists(),
        "partial output preserved"
    );
    assert!(!dest.exists());
    assert!(
        store.ops().iter().any(|o| o.is_save() && !o.outcome_ok()),
        "the failing save crossed the adapter: {:?}",
        store.ops()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segmented_post_commit_delete_failure_retains_completed() {
    let content = deterministic_bytes(SEG_TOTAL, 77_001);
    // Three ranges transfer freely; the LAST range parks behind a gate so
    // the job cannot complete before the coordinator's cadence save has
    // provably recorded the absorbed progress. A zero-delay scripted job
    // can otherwise outrun the first timer tick entirely — observed on
    // Windows, where the transfer finished between ticks and the
    // delete-after-save premise had no saves to order against.
    let head = (0..3u64)
        .map(|i| {
            let start = i * 1000;
            let end = start + 999;
            TransferStep::new().range((start, end)).ok(TransferOk::new()
                .range(start, end)
                .total(SEG_TOTAL)
                .chunk(content[start as usize..(end + 1) as usize].to_vec()))
        })
        .collect();
    let last_start = 3 * 1000;
    let last_end = SEG_TOTAL - 1;
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(SEG_TOTAL),
            accept_ranges: true,
            range_verified: true,
            ..ProbeMetadata::default()
        }))
        .expect_unordered_ranges("head", head)
        .gate("final")
        .expect_unordered_ranges(
            "final",
            vec![TransferStep::new()
                .range((last_start, last_end))
                .ok(TransferOk::new()
                    .range(last_start, last_end)
                    .total(SEG_TOTAL)
                    .chunk(content[last_start as usize..=last_end as usize].to_vec()))],
        );
    let store = ScriptedCheckpointStore::new();
    store.fail_next_delete(CheckpointError::Corrupt("seg delete boom".into()));
    let delete_gate = store.hold_delete(1);
    // 1ns cadence: cadence saves record in-flight progress; the gate above
    // guarantees at least one such save exists before the job can finish.
    let config = EngineConfig {
        checkpoint_flush_interval: Duration::from_nanos(1),
        ..segmented_cfg()
    };
    let controller =
        DownloadController::with_execution(HttpExecution::from_adapter(scripted.clone()), config)
            .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("seg.bin");
    let (handle, join) = controller.start(DownloadRequest::new(
        "https://scripted/seg-commit.bin",
        dest.clone(),
    ));
    let mut events = handle.events();
    // Deterministic ordering premise: the parked final range keeps the job
    // alive until the coordinator records at least one save of the head.
    wait_for_save(&store).await;
    scripted.open_gate("final");
    wait_for_delete(&store).await;
    let published_before_delete = std::fs::read(&dest);
    let state_before_delete = handle.state();
    delete_gate.release();
    let result = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(
        published_before_delete
            .expect("published before delete")
            .as_slice(),
        content.as_slice()
    );
    assert_eq!(
        state_before_delete,
        JobState::Committing,
        "delete precedes terminal state"
    );
    // Completed is retained after the irreversible commit (§9.2, §14.6).
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(handle.state(), JobState::Completed);
    assert_eq!(result.final_path.as_deref(), Some(dest.as_path()));
    assert!(
        result
            .warnings
            .iter()
            .any(|w| w.contains("checkpoint cleanup incomplete")),
        "warning: {:?}",
        result.warnings
    );
    let mut completion_events = Vec::new();
    while let Some(event) = events.try_next() {
        match event {
            Event::Warning { detail } if detail.contains("checkpoint cleanup incomplete") => {
                completion_events.push("warning");
            }
            Event::Committed { .. } => completion_events.push("committed"),
            _ => {}
        }
    }
    assert_eq!(completion_events, ["warning", "committed"]);
    // Delete-after-save order: the terminal delete came after every save.
    let ops = store.ops();
    let last_save = ops.iter().rposition(|o| o.is_save()).expect("saves");
    let delete_idx = ops.iter().position(|o| o.is_delete()).expect("delete");
    assert!(last_save < delete_idx, "delete after saves: {ops:?}");
    assert!(!ops[delete_idx].outcome_ok(), "delete failure recorded");
    assert_eq!(store.overlaps(), 0);
    scripted.assert_all_consumed();
}

#[tokio::test]
async fn segmented_cancellation_delete_failure_retains_cancelled() {
    let (scripted, _content) = segmented_script(true);
    let store = ScriptedCheckpointStore::new();
    store.fail_next_delete(CheckpointError::Corrupt("seg cancel boom".into()));
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        segmented_cfg(),
    )
    .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("seg.bin");
    let (handle, join) = controller.start(DownloadRequest::new(
        "https://scripted/seg-cancel.bin",
        dest.clone(),
    ));
    wait_until(
        || handle.snapshot().network_bytes >= 2000,
        Duration::from_secs(10),
        "first lease chunks",
    )
    .await;
    handle.cancel(); // DeletePartial: discard temp + checkpoint
    let result = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    // Cancelled is preserved; the failed delete surfaces as a warning.
    assert_eq!(result.status, ResultStatus::Cancelled, "{result:?}");
    assert!(
        result
            .warnings
            .iter()
            .any(|w| w.contains("checkpoint cleanup incomplete")),
        "warning: {:?}",
        result.warnings
    );
    let ops = store.ops();
    assert!(
        ops.iter().any(|o| o.is_delete() && !o.outcome_ok()),
        "delete attempted after workers joined: {ops:?}"
    );
    // Delete-after-save ordering holds in cancellation too.
    if let (Some(last_save), Some(delete_idx)) = (
        ops.iter().rposition(|o| o.is_save()),
        ops.iter().position(|o| o.is_delete()),
    ) {
        assert!(last_save < delete_idx, "{ops:?}");
    }
    assert_eq!(store.overlaps(), 0);
}

#[tokio::test]
async fn segmented_keep_partial_cancellation_preserves_checkpoint() {
    let (scripted, _content) = segmented_script(true);
    let store = ScriptedCheckpointStore::new();
    let controller = DownloadController::with_execution(
        HttpExecution::from_adapter(scripted.clone()),
        segmented_cfg(),
    )
    .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("seg.bin");
    let (handle, join) = controller.start(DownloadRequest::new(
        "https://scripted/seg-keep.bin",
        dest.clone(),
    ));
    wait_until(
        || handle.snapshot().network_bytes >= 2000,
        Duration::from_secs(10),
        "first lease chunks",
    )
    .await;
    handle.cancel_with(CancelMode::KeepPartial);
    let result = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("no hang")
        .expect("join")
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Cancelled, "{result:?}");
    assert!(result.warnings.is_empty(), "{:?}", result.warnings);
    // KeepPartial: temp file and checkpoint both preserved, no delete.
    assert!(dir.path().join("seg.bin.part").exists());
    assert!(
        store.ops().iter().all(|o| !o.is_delete()),
        "{:?}",
        store.ops()
    );
}

// ---- End-to-end alternate-adapter lifecycle (task 4.1) ----

/// Byte-coverage: every byte of `earlier` is inside some range of `later`.
fn covers(later: &[(u64, u64)], earlier: &[(u64, u64)]) -> bool {
    earlier
        .iter()
        .all(|&(s, e)| later.iter().any(|&(ls, le)| s >= ls && e <= le))
}

fn no_sidecar_files(dir: &std::path::Path) {
    let sidecars: Vec<_> = std::fs::read_dir(dir)
        .expect("dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".kdown"))
        .collect();
    assert!(
        sidecars.is_empty(),
        "no real checkpoint sidecar may exist for an alternate adapter: {sidecars:?}"
    );
}

fn etag_validators(etag: &str, total: u64) -> ResourceValidators {
    ResourceValidators {
        etag: Some(etag.into()),
        etag_is_weak: false,
        last_modified: None,
        total_size: Some(total),
    }
}

#[tokio::test]
async fn sequential_resume_cadence_and_commit_use_one_adapter() {
    // One completed job whose resume admission, cadence saves, and commit
    // cleanup all cross the single resolver-selected adapter (§34), with
    // no file sidecar anywhere.
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("out.bin");
    let identity = kdown_engine::resume::job_identity("https://scripted/e2e-seq.bin", &dest);
    let store = ScriptedCheckpointStore::new();
    // Prior usable state: checkpoint + temp covering its range.
    let mut prior = EngineCheckpoint::new(&identity, "https://scripted/e2e-seq.bin", "tmp");
    prior.total_size = Some(10);
    prior.validators = etag_validators("\"v\"", 10);
    prior.record_completed(0, 4);
    store.save_atomic(&prior).expect("seed");
    let seeded_ops = store.ops().len();
    std::fs::write(dir.path().join("out.bin.part"), vec![0u8; 5]).expect("temp");

    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(10),
            validators: etag_validators("\"v\"", 10),
            ..ProbeMetadata::default()
        }))
        .expect_transfer(
            TransferStep::new().range((5, 9)).ok(TransferOk::new()
                .range(5, 9)
                .total(10)
                .validators(etag_validators("\"v\"", 10))
                .chunk(b"world".to_vec())),
        );
    let config = EngineConfig {
        checkpoint_flush_interval: Duration::from_nanos(1),
        ..EngineConfig::default()
    };
    let controller =
        DownloadController::with_execution(HttpExecution::from_adapter(scripted.clone()), config)
            .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let result = controller
        .run(DownloadRequest::new(
            "https://scripted/e2e-seq.bin",
            dest.clone(),
        ))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(result.final_path.as_deref(), Some(dest.as_path()));
    assert!(
        result.bytes_reused_from_checkpoint > 0,
        "resume admission reused the persisted prefix: {result:?}"
    );
    // Every checkpoint operation crossed the one adapter, in order:
    // admission load, resume-refresh save, cadence saves, commit delete.
    let ops = store.ops();
    assert!(
        ops[seeded_ops].is_load(),
        "the job's first adapter op is the admission load: {ops:?}"
    );
    assert!(
        ops.iter().skip(seeded_ops).filter(|o| o.is_save()).count() >= 2,
        "refresh + cadence saves crossed the adapter: {ops:?}"
    );
    let last_save = ops.iter().rposition(|o| o.is_save()).expect("saves");
    let delete_idx = ops.iter().position(|o| o.is_delete()).expect("delete");
    assert!(last_save < delete_idx, "commit delete after saves: {ops:?}");
    assert!(ops[delete_idx].outcome_ok(), "commit cleanup succeeded");
    assert_eq!(store.overlaps(), 0);
    scripted.assert_all_consumed();
    no_sidecar_files(dir.path());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segmented_resume_cadence_and_commit_use_one_adapter() {
    // A segmented job with admitted prior state: admission load, worker
    // cadence saves, and commit cleanup all cross the one adapter with no
    // sidecar and no adapter-specific worker branches.
    let dir = tempfile::tempdir().expect("tmp");
    let dest = dir.path().join("seg.bin");
    let identity = kdown_engine::resume::job_identity("https://scripted/e2e-seg.bin", &dest);
    let store = ScriptedCheckpointStore::new();
    let mut prior = EngineCheckpoint::new(&identity, "https://scripted/e2e-seg.bin", "tmp");
    prior.total_size = Some(SEG_TOTAL);
    prior.validators = etag_validators("\"v\"", SEG_TOTAL);
    prior.record_completed(0, 999);
    store.save_atomic(&prior).expect("seed");
    let seeded_ops = store.ops().len();
    std::fs::write(dir.path().join("seg.bin.part"), vec![0u8; 1000]).expect("temp");

    let content = Arc::new(deterministic_bytes(SEG_TOTAL, 77_002));
    let steps = (1..4u64)
        .map(|i| {
            let start = i * 1000;
            let end = start + 999;
            let slice = content[start as usize..(end + 1) as usize].to_vec();
            TransferStep::new().range((start, end)).ok(TransferOk::new()
                .range(start, end)
                .total(SEG_TOTAL)
                .validators(etag_validators("\"v\"", SEG_TOTAL))
                .chunk(slice))
        })
        .collect();
    let scripted = ScriptedHttp::new()
        .expect_probe(ProbeStep::new().ok_meta(ProbeMetadata {
            status: 200,
            total_size: Some(SEG_TOTAL),
            validators: etag_validators("\"v\"", SEG_TOTAL),
            accept_ranges: true,
            range_verified: true,
            ..ProbeMetadata::default()
        }))
        .expect_unordered_ranges("remaining", steps);
    let config = EngineConfig {
        checkpoint_flush_interval: Duration::from_nanos(1),
        ..segmented_cfg()
    };
    let controller =
        DownloadController::with_execution(HttpExecution::from_adapter(scripted.clone()), config)
            .with_checkpoint_resolver(Arc::new(RecordingResolver::new(store.clone())));
    let result = controller
        .run(DownloadRequest::new(
            "https://scripted/e2e-seg.bin",
            dest.clone(),
        ))
        .await
        .expect("terminal");
    assert_eq!(result.status, ResultStatus::Completed, "{result:?}");
    assert_eq!(result.final_path.as_deref(), Some(dest.as_path()));
    assert_eq!(
        result.bytes_reused_from_checkpoint, 1000,
        "segmented admission reused every admitted range"
    );
    let ops = store.ops();
    assert!(
        ops[seeded_ops].is_load(),
        "the job's first adapter op is the admission load: {ops:?}"
    );
    assert!(
        ops.iter().skip(seeded_ops).filter(|o| o.is_save()).count() >= 1,
        "worker cadence saves crossed the adapter: {ops:?}"
    );
    // Accepted progress never regresses within the job.
    let saves: Vec<Vec<(u64, u64)>> = ops
        .iter()
        .filter_map(|o| o.save_ranges().map(|r| r.to_vec()))
        .collect();
    for pair in saves.windows(2) {
        assert!(
            covers(&pair[1], &pair[0]),
            "regressed: {:?} -> {:?}",
            pair[0],
            pair[1]
        );
    }
    let last_save = ops.iter().rposition(|o| o.is_save()).expect("saves");
    let delete_idx = ops.iter().position(|o| o.is_delete()).expect("delete");
    assert!(last_save < delete_idx, "commit delete after saves: {ops:?}");
    assert_eq!(
        store.overlaps(),
        0,
        "coordinated worker saves never overlap"
    );
    scripted.assert_all_consumed();
    no_sidecar_files(dir.path());
}
