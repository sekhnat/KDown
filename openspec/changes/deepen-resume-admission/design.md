# Design

## Context

See `proposal.md` for motivation. Today `SingleStreamController::run_inner` loads a checkpoint before probing, then directly orders generation comparison, temp-file validation, checkpoint deletion, range reuse, validator propagation, counter updates, and offset derivation. `resume::flow::ResumeSupport` supplies only isolated checks, while the controller owns the safety protocol.

The ordering is externally relevant. A missing or corrupt checkpoint under `ResumePolicy::Required` currently fails before the job enters `Probing`; a remote generation mismatch emits `ResourceChanged` before terminal failure; and an unusable temp file becomes a fresh transfer with a warning. The refactor must preserve those state/event relationships while leaving HTTP probing and job orchestration outside the resume module.

The admission path also sits between existing abstractions: `CheckpointStore` is the D3 persistence seam, `ResourceValidators` defines generation comparison, and `TempFileSpec` identifies the local partial output. D4 durability controls checkpoint writes elsewhere; admission only loads and, when restarting, deletes checkpoint state.

## Goals / Non-Goals

**Goals:**

- Represent resume admission as one named, invariant-preserving protocol rather than a collection of controller-owned conditionals.
- Keep checkpoint contents opaque to the controller until admission returns a complete decision.
- Produce mode-neutral admitted state from which sequential and segmented transfer inputs are derived without repeating safety logic.
- Preserve pre-probe required-checkpoint failures and post-probe resource-change event ordering.
- Make every admission branch testable with a scripted `CheckpointStore`, validator values, and local temp files, without HTTP.
- Fail closed if stale checkpoint state cannot be deleted before a restart.

**Non-Goals:**

- Adding a caller-selectable generation-change restart policy or changing `DownloadRequest`.
- Moving probe retries, authentication, job state transitions, event emission, sink construction, checkpoint saving, or transfer execution into the resume module.
- Changing checkpoint JSON, validator comparison semantics, durability acknowledgement, temp-file naming, or sink lifecycle rules.
- Completing the separate checkpoint-store injection/factory refactor; this change consumes `dyn CheckpointStore` but leaves concrete store selection where it is.
- Publishing the new admission protocol as supported external API.

## Decisions

### 1. Use a two-phase admission object, not more stateless helpers

Introduce a crate-private admission object in `resume::flow` with two protocol phases:

1. A begin phase receives `ResumePolicy`, checkpoint identity, resolved temp path, and `&dyn CheckpointStore`. It performs policy-aware loading and returns either an early rejection or an opaque pending admission.
2. After the controller completes its existing probe sequence, the pending admission receives the probed validators/size and returns one final admission decision.

The pending value owns the loaded checkpoint and references needed for cleanup. Its finalization orders remote-generation comparison before temp-output plausibility, matching the current safety sequence. The controller cannot call the comparison and validation pieces independently or inspect a checkpoint and accidentally reorder them.

Two phases are necessary because required missing/corrupt state must still reject before network activity, while remote metadata does not exist until the controller has emitted the probing transition and run its retry/authentication loop. Passing an async probe closure into a single resume function was rejected: it would make the resume module generic over transport orchestration and obscure ownership of job events. Moving all loading after probe was rejected because it would change observable state and network ordering.

### 2. Return a closed decision model with notification facts

The final result is a crate-private enum with two top-level outcomes:

- `Proceed(ResumePlan)`: safe to start transfer setup.
- `Reject(ResumeFailure)`: fail the job with a structured `DownloadError`.

`ResumePlan` uses private fields and exposes mode-specific views rather than allowing callers to assemble combinations. It carries:

- the admitted checkpoint, when one remains reusable;
- validated completed ranges for segmented transfer;
- the reusable contiguous prefix and next offset for sequential transfer;
- mode-appropriate reused-byte accounting;
- checkpoint validators needed for an `If-Range` request;
- existing warning text; and
- notification facts that the controller must turn into events.

`ResumeFailure` carries both the error and notification facts. This allows a generation mismatch to request `Event::ResourceChanged` before the controller performs the existing `Failing`/`Failed` transitions, without letting the resume module emit events itself.

For a segmented transfer, all admitted completed ranges and their unique byte count are reusable. For a sequential transfer, only the normalized range beginning at byte zero is reusable; later disjoint ranges are overwritten by the sequential stream and are not counted as reused. Empty state produces offset zero. A checkpoint covering the full resource produces an offset equal to total size, preserving the existing no-network fast path. Deriving both views in the resume module prevents transfer-mode fallback from treating holes as a completed prefix.

A bag of optional fields was rejected because combinations such as nonzero reused bytes with no admitted checkpoint or validators would be representable. Returning raw `Checkpoint` and asking the controller to derive ranges, offsets, and counters was rejected because it recreates the current shallow boundary.

### 3. Keep policy behavior explicit in one outcome matrix

The admission protocol implements these branches:

| Policy and state | Admission result |
| --- | --- |
| `Never` | Proceed fresh without loading or deleting the existing checkpoint. |
| `Allowed`, checkpoint absent | Proceed fresh. |
| `Allowed`, checkpoint corrupt/unreadable | Delete the unusable checkpoint, then proceed fresh; reject if deletion fails. |
| `Required`, checkpoint absent | Reject before probe with the current structured checkpoint error. |
| `Required`, checkpoint corrupt/unreadable | Reject before probe with the load error. |
| Loaded checkpoint, remote generation differs | Reject with `ResourceChanged`, preserve checkpoint/temp state, and request the existing resource-change event. |
| Loaded checkpoint, generation matches, temp missing or too short | Delete the checkpoint, then proceed fresh with the existing unusable-checkpoint warning; reject if deletion fails. |
| Loaded checkpoint, generation and temp valid | Proceed with admitted resume state. |

Generation equality continues to delegate to `ResourceValidators::same_generation`; this refactor does not strengthen or loosen validator acceptance rules. The currently unselectable generation-change behavior remains fail-only. The controller's private `change_policy` branch is removed rather than promoted into request configuration. The existing public `GenerationChangePolicy` symbol is not expanded into a working policy surface by this change.

A deletion required for restart is no longer best-effort. Continuing after deletion failure could leave a stale checkpoint beside newly restarted output and make a later process restart trust ranges from the wrong local state. Admission therefore returns a structured checkpoint failure and the controller terminates normally. This is the one intentional runtime tightening in an otherwise behavior-preserving refactor.

### 4. Preserve D3 and D4 boundaries

Admission accepts `&dyn CheckpointStore` and uses only `load` and `delete`. It does not construct `FileCheckpointStore`, choose its directory, or know its durability mode. This preserves the D3 seam and avoids coupling the new deep module to the default sidecar adapter before the separate store-pluggability change.

Admission never saves checkpoints or marks ranges durable. `DurableRangeTracker`, transfer-time `save_atomic`, and performance/durable flush ordering remain unchanged, preserving D4. Focused tests use an in-memory/failure-scripted store to prove load/delete ordering, while existing file-store tests continue to cover atomic persistence.

### 5. Keep orchestration and effects at the current boundaries

The controller remains responsible for:

- deriving identity and constructing the currently selected store;
- state transitions and event emission;
- probing and range-capability verification;
- choosing sequential versus segmented transfer;
- applying the selected mode view to counters and transfer arguments;
- opening the sink and running/finishing the transfer.

The resume module owns the decision and the checkpoint deletion required by that decision. It validates the resolved temp path but does not open, truncate, or delete the temp output; those lifecycle effects remain with output/transfer code. This keeps the refactor from absorbing the separate temp-output lifecycle opportunity.

### 6. Replace shallow tests with an admission matrix and retain integration evidence

Unit tests in the resume module use a deterministic fake `CheckpointStore` capable of returning absent, valid, corrupt, and delete-failure outcomes. Temp directories supply missing, short, plausible, partial, and complete files. Table-driven cases assert the decision variant, deletion calls, notices, warnings, admitted ranges, sequential offset, validators, and reused-byte values.

Controller/integration tests continue to cover real sidecar files, real sink behavior, real HTTP probing, generation mismatch events/results, interrupted resume, corrupt-checkpoint recovery, and both transfer modes. The old `ResumeSupport::remaining_ranges` integration test is removed or rewritten against the new decision view; range/offset assertions belong at the admission boundary.

### 7. Remove only the agreed shallow public facade

`ResumeSupport` and its re-export from `resume::mod` are removed after controller migration. The new admission types remain `pub(crate)` so the refactor does not accidentally create another public API before it stabilizes. `job_identity`, checkpoint types/store traits, and the existing job request/result surface remain available. No compatibility wrapper is retained, per the proposal's explicit breaking-change decision.

## Risks / Trade-offs

- [A two-phase protocol may look more complex than one function] → encode the phases as distinct types so only the valid next operation is available, and keep all checkpoint interpretation inside those types.
- [Mode-specific reuse accounting could drift from transfer arguments] → derive both from the same immutable `ResumePlan` view and apply the counter exactly once after mode selection.
- [Cleanup failure now terminates a job that previously continued] → document the intentional fail-closed behavior and cover it with a failure-scripted store test.
- [Controller event ordering could change while conditionals move] → return explicit notification facts and retain controller tests asserting `ResourceChanged` precedes terminal failure and required-load failure occurs before probing.
- [The new module could become coupled to `FileCheckpointStore` or `FileSink`] → accept `dyn CheckpointStore` and a resolved path; keep store construction and sink lifecycle outside.
- [Public removal breaks downstream `ResumeSupport` imports] → call out the breaking change, remove the re-export in one step, and avoid offering a facade that preserves the shallow abstraction.

## Migration Plan

1. Add the crate-private admission phase, decision, plan, and fake-store test support alongside the existing helpers.
2. Build the complete focused outcome matrix, including delete failure and sequential-versus-segmented views.
3. Migrate `SingleStreamController::run_inner` to begin admission before probe, finalize it after probe, emit returned notices, and consume one mode-specific plan.
4. Run existing resume and controller integration tests to verify real-file behavior and observable state/event ordering.
5. Remove `ResumeSupport`, its re-export, the private controller `change_policy` helper, and tests that target the obsolete fragment APIs.
6. Run formatting, linting, and the full engine test suite. No checkpoint-data migration or deployment sequencing is required because persisted format and job-facing behavior remain compatible.

Rollback is code-only: restore the prior controller/helper wiring. Existing checkpoints remain readable in either direction because their format is unchanged.
