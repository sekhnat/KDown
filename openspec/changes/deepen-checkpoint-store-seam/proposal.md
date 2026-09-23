# Proposal

## Why

`CheckpointStore` is public but job orchestration uses it only during resume admission before narrowing back to `FileCheckpointStore` for transfer, pause, cancellation, and completion. Carrying one selected adapter through the full lifecycle makes checkpoint persistence genuinely substitutable and makes save/delete failure behavior deterministic to test.

## What Changes

- Add a thread-safe checkpoint-store resolver at controller construction so each job receives one request-scoped `Arc<dyn CheckpointStore>` selected from its destination and durability configuration; preserve all existing controller constructors by installing the file-sidecar resolver by default.
- Pass the resolved checkpoint-store interface, never `FileCheckpointStore`, through resume admission, sequential transfer, segmented workers, pause persistence, cancellation cleanup, and successful commit cleanup.
- Serialize checkpoint operations for a job so concurrent workers cannot publish stale state out of order, while keeping the selected adapter safe to share across spawned tasks.
- Make every checkpoint save failure stop the job with a structured checkpoint error, converge segmented workers, and preserve partial output rather than silently continuing without the promised resumable state.
- Keep restart/admission deletion fail-closed. For cleanup after an already-determined terminal outcome, preserve `Completed` or `Cancelled` and surface checkpoint deletion failure as a warning instead of hiding it or rewriting the primary outcome.
- Retain `FileCheckpointStore` as the default destination-relative sidecar adapter, including atomic replacement and durable directory synchronization behavior.
- Add a deterministic in-memory, failure-scripted test adapter that records operation ordering and can inject load, save, and delete failures. Retain real-file tests for corruption, atomic replacement, durability, and crash/restart behavior.

## Capabilities

### New Capabilities

- `checkpoint-store-seam`: Defines end-to-end checkpoint adapter selection, lifecycle-wide use, operation ordering, and observable save/delete failure semantics.

### Modified Capabilities

_None._

## Impact

- Primary code: `crates/engine/src/resume/checkpoint_store.rs`, `crates/engine/src/job/controller.rs`, and `crates/engine/src/job/segmented.rs`.
- Public API: an additive controller construction/injection path for a checkpoint-store resolver; existing `SingleStreamController` construction and the `CheckpointStore` load/save/delete contract remain supported.
- Runtime behavior: previously ignored save failures become terminal checkpoint failures; terminal cleanup delete failures become warnings, while admission cleanup remains fail-closed.
- Tests: `crates/engine/tests/resume_tests.rs`, `crates/engine/tests/crash_restart_tests.rs`, segmented transfer coverage, and focused store/resolver tests.
- Dependencies and data: no new dependency or checkpoint-format migration is planned; the default sidecar location, atomic replacement, and configured durability guarantees remain compatible.
