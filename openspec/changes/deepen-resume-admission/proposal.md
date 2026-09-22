# Proposal

## Why

Resume admission is safety-critical, but `SingleStreamController::run_inner` currently assembles checkpoint policy, remote-generation checks, temp-output validation, restart cleanup, reusable ranges, offsets, warnings, and accounting across several modules. Moving that ordered decision behind one resume-owned interface makes the never-mix-generations invariant locally explainable and directly testable without requiring the full controller and an HTTP server.

## What Changes

- Replace the shallow `ResumeSupport` helper collection with one resume-admission operation that owns checkpoint loading interpretation, generation comparison, temp-output plausibility, fail/restart/continue selection, and restart checkpoint deletion.
- Return a decision object containing the completed ranges, sequential resume offset, reusable-byte count, checkpoint validators/state needed by later transfer setup, warnings, and any resource-change fact the controller must publish.
- Make both sequential and segmented setup consume the same admission decision while keeping job state transitions, event emission, probe execution, counters, sink creation, and transfer orchestration in the controller.
- Preserve the `CheckpointStore` abstraction and current performance/durable checkpoint guarantees; admission depends on the trait rather than constructing a concrete storage backend.
- Preserve current policy behavior: generation changes fail because no selectable restart-on-change policy exists; corrupt or locally unusable optional state restarts conservatively; required-resume failures remain errors.
- Fail closed with a structured checkpoint error when restart cannot delete stale checkpoint state; do not continue with cleanup failure hidden.
- Add focused admission tests for resume-disabled, missing, corrupt, required, stale-generation, missing/short temp output, empty, partial, and complete checkpoint states, including deletion success/failure behavior where relevant. Retain real-file and controller-level resume tests as integration evidence.
- **BREAKING**: remove the publicly re-exported `ResumeSupport` shallow helper API rather than retaining a compatibility facade. No replacement admission API is promised as public engine API unless its implementation naturally belongs there.

## Capabilities

### New Capabilities

None. This change reorganizes the implementation of existing resume behavior and sets `skip_specs: true`.

### Modified Capabilities

None. The existing resume, storage, and job-facing requirements remain unchanged; in particular, the change adds no selectable generation-restart policy.

## Impact

- Primary code: `crates/engine/src/resume/flow.rs`, `crates/engine/src/resume/mod.rs`, and `crates/engine/src/job/controller.rs`.
- Supporting contracts: `crates/engine/src/resume/checkpoint_store.rs`, `crates/engine/src/http/validators.rs`, and `crates/engine/src/io/sink.rs` remain the sources of storage, validator, and temp-path behavior consumed by admission.
- Tests: focused resume-module tests expand, while `crates/engine/tests/resume_tests.rs` continues to verify real filesystem, HTTP, and controller-observable behavior.
- Public API: downstream imports of `kdown_engine::resume::ResumeSupport` will no longer compile; job request/result behavior and event/state ordering are intended to remain compatible.
- Dependencies and checkpoint format: no new dependency or persisted-format change is planned.
