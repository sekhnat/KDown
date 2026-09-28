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
- `DirectoryDownloadRequest` (opt-in directory-target request; see also
  `DownloadHandle::resolved_destination` and `Event::DestinationResolved`)
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

### Directory-target downloads and `Rename` (additive)

`DirectoryDownloadRequest` wraps an ordinary `DownloadRequest` (mutate it via
`request_mut()`), targets an existing directory, and resolves the final
basename from the final HEAD `Content-Disposition` (`filename*` then
`filename`), the final/original URL segment, or the validated fallback
(default `download`, default byte cap 250). Controllers expose
`start_to_directory`, `run_to_directory`, and `run_to_directory_with_handle`;
`OverwritePolicy::Rename` opts into automatic collision handling (base name
then `stem (1).ext` … `stem (999).ext`, always atomic no-replace) for both
target forms. Directory targets probe first and lease/admit the resolved
destination afterward; explicit-file targets — including `Rename` — keep
lease/admission before any networking. `Event::DestinationResolved` is
emitted once for directory and `Rename` jobs after lease acquisition;
`DownloadHandle::resolved_destination()` is the lag-safe lookup.

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

## Behavior notes for supported types

- `Redactor::new()` masks URL userinfo and every query value by default.
  `Redactor::with_sensitive_query_params` marks extra names (additive), and
  `Redactor::with_marked_query_params_only` opts down to userinfo plus
  marked names. `DownloadRequest`/`RequestSpec` `Debug` output uses the
  default policy; the wire URL is never rewritten.
- `EventStream::next()` returns `None` after the job's terminal outcome
  once queued events are drained, even while the `DownloadHandle` (and its
  `EventHub`) is retained. `EventStream::is_finished()` exposes the signal.
- `DownloadError::AdmissionRejected { active, cap }` is returned when
  `max_active_jobs` is exceeded; it is non-retryable, category `MemoryCap`,
  and is delivered before any transfer or artifact write.
- `TransferAccounting::wire_amplification()` counts received payload once
  over unique output coverage (`completed_bytes` plus
  `bytes_reused_from_checkpoint`); it is `None` when that coverage is zero.
- `Checkpoint` is a v2 model: persisted JSON omits request/final URLs and
  carries owned-temp identity plus a bounded covered-byte digest. A custom
  `CheckpointStore` must protect its own storage and must not add URL or
  credential fields of its own.
