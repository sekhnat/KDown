# Migration Guide: kdown-engine 0.1 → current

The 0.1 → current release is a **breaking** release. This guide covers every
breaking change and shows the supported replacement for each removed or changed
item.

## 1. Terminal outcome API

### Old result checks

| Old 0.1 usage | Current replacement |
|---|---|
| `controller.run(req)` returning `Ok(DownloadResult { status: Failed, error: Some(e) })` | `controller.run(req)` returns `Err(DownloadRunError::Transfer/Infrastructure/Cancelled)` for all non-success outcomes; `Ok` only for verified publication |
| `result.status == ResultStatus::Completed` | `match run(...) { Ok(c) => ... }` — `is_ok()` means verified publication |
| `result.status == ResultStatus::Failed` | `Err(DownloadRunError::Transfer(f))` or `Err(DownloadRunError::Infrastructure(f))`; classify via `error.domain()` |
| `result.status == ResultStatus::Cancelled` | `Err(DownloadRunError::Cancelled(summary))` |
| `result.error` / `result.error.as_ref().map(DownloadError::category)` | `error.as_engine_error()` / `error.category()` |
| `result.final_path: Option<PathBuf>` | `CompletedDownload.final_path: PathBuf` (non-optional on success) |
| `result.completed_bytes`, `.wasted_bytes`, `.retries`, `.bytes_downloaded_from_network`, `.bytes_reused_from_checkpoint`, `.segment_requests`, `.live_splits`, `.total_size`, `.elapsed`, `.validators`, `.warnings` | `CompletedDownload.accounting.<field>` (field) / `error.accounting().<field>` (method on error) |
| `result.wire_amplification()` | `completed.accounting.wire_amplification()` |

### Import paths

| Old 0.1 import | Current import |
|---|---|
| `use kdown_engine::job::controller::{DownloadController, DownloadRequest, DownloadHandle, CancelMode, ...}` | `use kdown_engine::{DownloadController, DownloadRequest, DownloadHandle, CancelMode, ...}` |
| `use kdown_engine::job::state::JobState` | `use kdown_engine::JobState` |
| `use kdown_engine::{DownloadResult, ResultStatus}` | removed from the public API — see the terminal outcome table above (the crate keeps a private internal status type of the same name) |

## Removed injection hooks

| Old 0.1 seam | Disposition |
|---|---|
| `use kdown_engine::http::scripted::{ScriptedHttp, ProbeStep, TransferStep, TransferOk}` | No supported replacement — injection seam deliberately retired (task 2.3) |
| `use kdown_engine::http::{HttpExecution, HttpExecutor, HttpBodySource}` | No supported replacement — seam deliberately retired |
| `use kdown_engine::http::probe::ProbeMetadata` | No supported replacement — seam deliberately retired |
| `DownloadController::with_execution(execution, config)` | No supported replacement — use `DownloadController::new(transport, config)` with a real `HttpTransport` |
| `DownloadController::with_execution_and_metrics(...)` | No supported replacement — use `DownloadController::with_metrics(transport, config, metrics)` |
| `metrics.record_result(&DownloadResult)` / `record_task_error(&DownloadError)` | `EngineMetrics::record_completed(&CompletedDownload)` / `record_run_error(&DownloadRunError)` |

## Behavioral changes (checkpoint, privacy, lifetime)

| Area | Change |
|---|---|
| Checkpoint format | New checkpoints are v2: they bind persisted ranges to the owned temp-file identity and a bounded digest of covered bytes, and the persisted JSON contains no request/final URL. v1 files fail validation and restart conservatively; a sidecar written by an older v2 build still loads (its URL fields are ignored, never revived). Delete old `.part`/sidecar pairs if you do not want the conservative restart. |
| Redactor defaults | `Redactor::new()` masks userinfo and **every** query value; `with_sensitive_query_params` is retained, and `with_marked_query_params_only` restores the previous opt-in-only behavior. Request URLs sent on the wire are never changed. |
| Event streams | `EventStream::next()` drains queued events and then returns `None` once the job is terminal, even while a `DownloadHandle` (and its hub) is retained; previously it could wait forever. |
| Job lifetime | `transfer.job_deadline` is enforced end to end; `max_active_jobs` rejects an over-cap `start` with `DownloadError::AdmissionRejected` (category `MemoryCap`) before any artifact is written; atomic publication is the commit boundary, so a post-commit expiry reports success truthfully. |
| File permissions | Engine-created `.part` files and checkpoint sidecars are owner-only (`0600`) on Unix; a resumed partial is tightened on open. |


## Verification

- **External consumer examples compile:** `crates/engine/examples/download.rs`
  and `tests/external_consumer_fixture.rs` compile and run using only
  supported imports.
- **Signature drift detection:** the external-consumer fixture is the
  compile-level compatibility check — any change to a supported type or
  signature visible in the whitelist fails the fixture build and must be
  reviewed under the compatibility policy before release.
