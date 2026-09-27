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
| `use kdown_engine::{DownloadResult, ResultStatus}` | removed — see terminal outcome table above |

## Removed injection hooks

| Old 0.1 seam | Disposition |
|---|---|
| `use kdown_engine::http::scripted::{ScriptedHttp, ProbeStep, TransferStep, TransferOk}` | No supported replacement — injection seam deliberately retired (task 2.3) |
| `use kdown_engine::http::{HttpExecution, HttpExecutor, HttpBodySource}` | No supported replacement — seam deliberately retired |
| `use kdown_engine::http::probe::ProbeMetadata` | No supported replacement — seam deliberately retired |
| `DownloadController::with_execution(execution, config)` | No supported replacement — use `DownloadController::new(transport, config)` with a real `HttpTransport` |
| `DownloadController::with_execution_and_metrics(...)` | No supported replacement — use `DownloadController::with_metrics(transport, config, metrics)` |
| `metrics.record_result(&DownloadResult)` / `record_task_error(&DownloadError)` | `EngineMetrics::record_completed(&CompletedDownload)` / `record_run_error(&DownloadRunError)` |

## Verification

- **External consumer examples compile:** `crates/engine/examples/download.rs`
  and `tests/external_consumer_fixture.rs` compile and run using only
  supported imports.
- **Signature drift detection:** the external-consumer fixture is the
  compile-level compatibility check — any change to a supported type or
  signature visible in the whitelist fails the fixture build and must be
  reviewed under the compatibility policy before release.
