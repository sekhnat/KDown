# Supported API surface (0.1 breaking cleanup)

This file is the concrete whitelist of the externally supported `kdown-engine`
Rust surface, the public type signatures that depend on it, and the disposition
of the previously advertised test seams. It is the baseline for the API
compatibility check (`scripts/api_surface_check.sh`) and the migration guide
(`docs/migration-0.1.md`).

## Supported import paths

Consumers may rely on exactly the following paths.

### Crate root re-exports

- `DownloadRequest`, `DownloadController`, `DownloadHandle`, `CancelMode`,
  `SingleStreamController` (deprecated alias)
- `CompletedDownload`, `DownloadRunError`, `TransferFailure`, `EngineFailure`,
  `CancellationSummary`, `TransferAccounting`, `ArtifactDisposition`
- `DownloadError`, `ErrorCategory`, `Retryability`, `FailureDomain`
- `EngineConfig`, `DurabilityMode`, `ExpectedHash`, `H2ConnectionPolicy`,
  `HashAlgorithm`, `IntegrityPolicy`, `NetworkPolicy`, `OverwritePolicy`,
  `PoolConfig`, `ProxyConfig`, `ResumePolicy`, `TlsConfig`, `TransferPolicy`
- `HttpTransport`
- `EngineMetrics`, `Event`, `EventHub`, `EventStream`, `MetricsSnapshot`,
  `ProgressSnapshot`
- `JobState` (observation only: `DownloadHandle::state()`)
- `Redactor`

### Public modules

- `kdown_engine::config` — configuration types and validation.
- `kdown_engine::control` — runtime-control surface: `CancellationToken`,
  rate limiting (`rate_limit::TokenBucket`), origin registry
  (`origin::OriginRegistry`), credential providers (`auth::{Challenge,
  CredentialDecision, CredentialProvider}`), retry classification
  (`retry::RetryClassifier`, `RetryDecision`).
- `kdown_engine::error` — the structured error taxonomy and terminal outcome
  types.
- `kdown_engine::http` — transport construction and policy:
  `transport::{HttpTransport, RequestSpec}`, `redirect::{RedirectPolicy,
  RedirectTracker, RedirectDecision, RedirectAction}`, `connect` (connection
  limits and protocol statistics), `validators::{ResourceValidators,
  ContentRange}`.
- `kdown_engine::metrics` — metrics/events surface.
- `kdown_engine::redact` — `Redactor`.

## Signature dependencies (types nameable through supported paths)

These types appear in supported public signatures and are therefore reachable
through the paths above:

- `DownloadRequest::credential_provider: Option<Arc<dyn CredentialProvider>>`
  → `control::auth::CredentialProvider` and its associated types.
- `DownloadController::with_global_rate_bucket(Arc<TokenBucket>)`
  → `control::rate_limit::TokenBucket`.
- `DownloadController::with_origin_registry(Arc<OriginRegistry>)`
  → `control::origin::OriginRegistry`.
- `DownloadController::with_checkpoint_resolver(Arc<dyn CheckpointStoreResolver>)`
  → `resume::checkpoint_store::{CheckpointStore, CheckpointStoreResolver}`
  (re-exported at the crate root as `CheckpointStoreResolver` /
  `CheckpointStore` together with `FileCheckpointStore`, `Checkpoint`,
  `ByteRange`, `DurabilityMode`, and `job_identity` for resolver implementors).
- `CompletedDownload::accounting` / `DownloadRunError::accounting()`
  → `http::validators::ResourceValidators` inside `TransferAccounting`.

## Implementation modules (not public)

`scheduler`, `job` (orchestration internals beyond the re-exported
controller/handle types), `io`, `resume` internals beyond the checkpoint-store
injection surface above, `observability`, and `fuzz_targets` are not part of
the public API. `fuzz_targets` is compiled only behind the non-default
`fuzz-entry` feature for the fuzzing harness; it is never a consumer API.

## Retired seams (0.1 removals)

- `http::HttpExecution`, `http::HttpExecutor`, `http::scripted::*`
  (`ScriptedHttp`, `ProbeStep`, `TransferStep`, `TransferOk`, `CallKind`,
  `CallRecord`, `ScriptedBodyEvent`), `http::probe::ProbeMetadata`,
  `http::probe::filename_from_disposition`, `http::range::*`,
  `http::execution::*`, and
  `DownloadController::{with_execution, with_execution_and_metrics}` are
  retired as external injection seams. Deterministic scripted adapters remain
  in the crate for internal conformance tests. See `docs/migration-0.1.md`.
- `DownloadResult` / `ResultStatus` are removed from the public API (a
  crate-private status type of the same name remains internal); terminal
  outcomes are `Result<CompletedDownload, DownloadRunError>`.

## Compatibility policy

See `docs/api-compatibility.md` for the semver/MSRV policy and the drift
checks that guard this whitelist.
