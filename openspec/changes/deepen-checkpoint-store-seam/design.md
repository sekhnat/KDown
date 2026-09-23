# Design

## Context

See `proposal.md` for motivation and `specs/checkpoint-store-seam/spec.md` for the behavior contract. The current controller constructs `FileCheckpointStore` inside `run_inner`, passes that concrete type into segmented transfer and completion, and reconstructs the controller inside the spawned job task. Sequential cadence, pause, cancellation, segmented cadence, and commit cleanup currently discard some `save_atomic` or `delete` results.

`CheckpointStore` is already a synchronous, object-safe `Send + Sync` public trait with `load`, `save_atomic`, and `delete`. Resume admission consumes `&dyn CheckpointStore`, and `DownloadError::Checkpoint` already provides the stable failure category. `FileCheckpointStore` fixes one directory at construction, while a controller may run jobs with unrelated destination parents. Segmented workers are spawned tasks and currently clone the concrete file adapter; its PID-only temporary filename also assumes saves do not overlap.

Two existing contracts shape the exceptional cleanup path. `KDownSpec.md` §14.6 gives the normal commit order as rename, checkpoint removal, then `Completed`, while §9.2 defines `Completed` primarily by successful integrity verification and destination commit. The selected behavior resolves delete failure after the irreversible rename by retaining `Completed` and reporting incomplete cleanup, while preserving the normal ordering when deletion succeeds.

## Goals / Non-Goals

**Goals:**

- Keep the existing store operations and checkpoint format while making adapter selection a real controller-level port.
- Resolve one adapter per job with enough context to preserve destination-relative sidecars, then share it safely through every checkpoint path.
- Prevent overlapping or regressive saves from concurrent workers without holding a lock across an async suspension point.
- Give save and delete failures one explicit mapping to job results, worker convergence, warnings, and artifact preservation.
- Keep real filesystem evidence for sidecar atomicity and crash/restart behavior while moving orchestration failure cases to a scripted adapter.

**Non-Goals:**

- Changing checkpoint JSON, job identity, validator comparison, durability policy selection, final sidecar naming, or temp-output identity.
- Converting `CheckpointStore` into an async trait or adding a new runtime/dependency.
- Reworking resume admission decisions established by `deepen-resume-admission`.
- Adding SQLite or application-state production adapters; this change makes such adapters injectable but ships only the sidecar production resolver.
- Generalizing sink/output injection or changing HTTP execution, retry, or scheduling policy.

## Decisions

### 1. Inject a destination-aware resolver into the controller

Add a public, object-safe `CheckpointStoreResolver` port and a redaction-safe resolution context containing the job identity, destination path, and checkpoint durability mode. Resolution returns `Result<Arc<dyn CheckpointStore>, CheckpointError>`. The resolver is held by `SingleStreamController`, cloned into its spawned run task, called once before resume admission, and the returned adapter remains owned by that job through terminal cleanup.

Existing constructors (`new`, `with_metrics`, `with_execution`, and `with_execution_and_metrics`) install a default resolver that creates `FileCheckpointStore` in the destination parent. Add one consuming controller builder/injection method for replacing the resolver so every existing construction combination can opt into another adapter without a constructor matrix. Re-export the resolver/context beside the existing checkpoint-store API. Resolver failure becomes a checkpoint-category terminal failure before probing.

A resolver, rather than one controller-wide store, is necessary because the file adapter is directory-scoped and one controller can serve many destination parents. Putting the adapter on `DownloadRequest` was rejected because persistence selection is an application/controller dependency, not download input, and would mix infrastructure with request data. Redesigning `CheckpointStore::load(job_identity)` to accept destination context was rejected because it would break the existing store interface merely to accommodate the default adapter.

### 2. Decorate the selected adapter with per-job mutation coordination

Immediately after resolution, wrap the selected adapter in a crate-private coordinated store that itself implements `CheckpointStore`, and expose it to the rest of the job as `Arc<dyn CheckpointStore>`. Controller helpers and segmented functions therefore depend only on the existing interface; neither job module imports or names `FileCheckpointStore`. Every operation still reaches the one resolver-selected adapter under the decorator.

The decorator uses a short-lived standard mutex because the public store methods are synchronous already. It never holds that mutex across `.await`; it serializes only one existing synchronous store call plus mutation bookkeeping. `load`, `save_atomic`, and `delete` all participate in the same per-job order.

For successful saves, the coordinator remembers the last accepted checkpoint. Before forwarding another snapshot from the same job and resource generation, it prevents progress regression by folding forward already-recorded completed ranges (or discarding a strictly superseded snapshot) and retaining a normalized range set. An unexpected identity or generation conflict is a checkpoint inconsistency rather than an opportunity to mix state. Successful deletion clears the remembered snapshot so admission may delete unusable state and later create a fresh checkpoint. Terminal deletion is invoked only after sequential activity has stopped or segmented workers have joined, so no save can be issued after cleanup.

Requiring every adapter to implement its own locking was rejected: `Send + Sync` guarantees shareability, not ordered semantics, and it would leave the current sidecar implementation's shared temporary path vulnerable. Serializing only in `FileCheckpointStore` was rejected because alternate adapters would observe different job semantics. Changing checkpoint schema to add a sequence number was rejected because ordering is process-local and does not justify a data migration.

### 3. Carry `Arc<dyn CheckpointStore>` through both transfer modes

Resolve and coordinate the store once in `run_inner`, then pass cloneable `Arc<dyn CheckpointStore>` handles through resume admission, sequential helpers, `run_segmented`, `worker_loop`, `transfer_lease`, checkpoint persistence, cancellation cleanup, and segmented completion. Borrow `&dyn CheckpointStore` where a call cannot outlive its owner; clone the `Arc` only for spawned workers.

`persist_checkpoint` returns `Result<(), DownloadError>` instead of discarding the store result. Segmented pause handling first absorbs worker progress into the scheduler, creates the durable-range snapshot, and persists it before treating the pause as resumable. Common terminal cleanup is used by sequential and segmented paths after worker convergence, including the selected cancellation mode.

A new parallel checkpoint API was rejected because it would leave the old trait shallow. Passing a generic store type through all functions was rejected because it would monomorphize the orchestration path, complicate spawned-task ownership, and make the controller's stored dependency harder to replace dynamically.

### 4. Treat all active save failures as fatal checkpoint errors

Map `CheckpointError` from resume-state refresh, cadence, or pause to `DownloadError::Checkpoint`. In sequential mode, mark the sink to keep consistent partial output and return through the normal `Failing`/`Failed` result path immediately. In segmented mode, return the failure from `persist_checkpoint` through `transfer_lease`; the observing worker installs it as the shared fatal error, and the existing worker fatal checks make peers stop at safe boundaries before `run_segmented` returns a failed outcome.

Do not delete the previous checkpoint after a save failure. Atomic replacement leaves the previous complete state usable, and preserved partial bytes beyond that checkpoint can safely be ignored on a later admission. Checkpoint failures remain non-retryable under the existing error taxonomy.

Continuing with a warning was rejected because pause would claim resumability without persisted state and callers could not rely on checkpoint policy. Making only `ResumePolicy::Required` fatal was rejected because that policy governs admission of prior state, not whether current persistence silently fails.

### 5. Distinguish safety deletion from terminal cleanup deletion

Resume admission continues to own deletion required before restarting from zero. That delete remains fail-closed because continuing could leave stale checkpoint ranges beside new temp output.

Cancellation cleanup returns accumulated warnings instead of discarding delete results. For `DeletePartial` and `KeepFileDiscardCheckpoint`, cleanup waits until transfer work is stopped, attempts the selected adapter's delete, and appends an actionable warning on failure; `KeepPartial` does not delete. Both sequential and segmented terminal-result builders retain `Cancelled` and include those warnings.

After successful destination commit, attempt checkpoint deletion before transitioning/reporting `Completed`. If deletion succeeds, the normal §14.6 sequence is unchanged. If it fails, the destination rename is not rolled back: append a checkpoint-cleanup warning (and publish the existing warning event), retain `Completed` with the committed `final_path`, then publish commit completion. A later admission still validates temp/checkpoint state and remains fail-closed if stale state cannot be removed.

Turning a committed download into `Failed` was rejected because the final artifact is already valid and externally visible. Treating all deletes as warnings was rejected because pre-transfer stale-state deletion is a safety boundary. Silently ignoring terminal cleanup was rejected because callers need to know manual cleanup may be required.

### 6. Preserve and tighten the default sidecar adapter's durability contract

Keep the current destination-parent sidecar name and write-temp, optional file sync, rename, and durable parent-directory sync sequence. Continue treating a missing file as successful deletion. Use a collision-resistant temporary filename or rely on the coordinated per-job writer so spawned workers cannot contend for one PID-only temp path.

In durable mode, an attempted file or supported parent-directory synchronization failure is returned as `CheckpointError`; a save is not reported successful after an ignored durability failure. Platform-specific lack of directory-sync support is handled explicitly rather than by silently swallowing arbitrary I/O errors. Atomic replacement and temp-residue tests remain attached to the real file adapter.

Moving sidecar behavior into the resolver was rejected because resolution chooses an adapter; atomic persistence remains adapter-owned. Relaxing directory synchronization was rejected because pluggability must not weaken the default durability guarantee.

### 7. Use a shared in-memory scripted adapter for orchestration evidence

Add test support implementing the public store trait with an in-memory checkpoint map, ordered operation log, queued load/save/delete failures, and deterministic gates/overlap detection. Tests inject it through the controller resolver and assert both returned outcomes and exact store operations. The scripted HTTP adapter supplies deterministic transfer behavior where wire protocol is irrelevant.

Cover at minimum resolver context/one-resolution-per-job, sequential save failure, segmented save failure and worker convergence, pause save failure, admission delete failure, post-commit delete warning, cancellation delete warning, non-overlapping/monotonic segmented saves, and delete-after-save ordering. Keep real sidecar tests for corrupt JSON, save/load round trips, atomic replacement, missing deletion, process interruption, and crash/restart resume.

A production-exported scripted store was rejected because it is test instrumentation rather than an engine capability. File-only orchestration tests were rejected because permissions and timing cannot deterministically drive every ordering and failure branch.

## Risks / Trade-offs

- [Synchronous persistence under a coordinator mutex can briefly block an executor worker] → checkpoint calls are already synchronous and cadence-limited; keep the critical section free of awaits and treat an async store redesign as a separate compatibility change.
- [Merging monotonic ranges could conceal a caller regression] → require matching identity/generation, normalize explicitly, record scripted operations, and fail on incompatible lineage rather than guessing.
- [A checkpoint save failure now fails downloads that previously could complete] → this is the selected behavior; preserve the old atomic checkpoint and partial output and test the structured failure in both transfer modes.
- [Completed with a stale checkpoint is an exception to the normal §14.6 sequence] → attempt deletion before the terminal transition, emit a visible warning, keep admission fail-closed, and update the conceptual contract to document the irreversible-commit exception.
- [Resolver additions can produce a constructor combinator explosion] → use one consuming injection method shared by all existing constructor paths and keep default construction unchanged.
- [Segmented save failures could race with successful worker completion] → latch the first shared fatal checkpoint error, check it before declaring coverage complete, and join every worker before terminal cleanup.
- [Platform directory-sync behavior differs] → isolate support detection in the file adapter and keep platform-specific tests/guards without weakening supported durable paths.

## Migration Plan

1. Add the resolver/context API, default sidecar resolver, coordinated decorator, and focused unit tests without changing current constructors.
2. Store the resolver on the controller, clone it into spawned jobs, resolve one adapter per request, and migrate resume admission and sequential checkpoint calls to the trait-object handle.
3. Migrate segmented signatures and spawned workers, return persistence errors, and add pause/save and shared-fatal convergence behavior.
4. Unify sequential and segmented cancellation/commit cleanup warning handling and document the post-commit delete-failure exception to the normal cleanup sequence.
5. Add scripted orchestration tests, retain and strengthen real sidecar/crash tests, then run formatting, linting, focused suites, and the full engine test suite.

No checkpoint data migration or deployment sequencing is required. Rollback is code-only: restore concrete controller wiring and prior failure handling; checkpoints written by either version remain format-compatible. A rollback may return to ignoring persistence failures, so it should be treated as a loss of the new reliability guarantees rather than a data conversion.
