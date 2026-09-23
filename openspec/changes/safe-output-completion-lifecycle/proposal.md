# Proposal

## Why

The engine's documented output guarantees can be violated between its initial destination check and final rename, by concurrent jobs sharing `<destination>.part`, and by the Windows delete-before-rename replacement fallback. The strict rustdoc build also currently fails, while separate sequential and segmented completion paths make these safety rules difficult to enforce and test once.

## What Changes

- Restore the warnings-denied rustdoc gate by resolving the HTTP-execution API links, without suppressing documentation warnings.
- Enforce `FailIfExists` atomically at publication, preserve the old destination on failed or unsupported `Replace`, and isolate simultaneous jobs targeting one destination across controllers/processes. Preserve safe crash/resume recovery of prior partial output.
- Give both transfer modes one output lifecycle and verified completion path: prepare/resume, positional writes, durability, pause/cancel/retry disposition, size/hash verification, policy-aware publish, checkpoint cleanup, and terminal events/results. Remove the segmented reopen-for-commit path and controller-owned `keep_on_drop` handling.
- Add deterministic fault/concurrency tests and retain real-filesystem, cross-platform, and benchmark evidence. Preserve existing public APIs and checkpoint format unless a safety requirement cannot be met without a narrowly documented compatible extension. No new protocol, manager feature, or speculative optimization is included.

## Capabilities

### New Capabilities

The main `openspec/specs/` inventory is currently empty. These paths reuse the capability names already established by the unarchived `download-engine-v1` change, adding only targeted requirements rather than redefining its v1 baseline.

- `engine-api`: Commit-time no-clobber and structured destination-conflict outcomes, including conflicts introduced while a job is running.
- `resume-and-storage`: Exclusive ownership or isolation of temporary output/checkpoint artifacts across concurrent jobs, safe replacement/failure behavior, and one observable verified-publish lifecycle.

### Modified Capabilities

None in the current main-spec inventory. The existing unarchived v1 deltas for `engine-api`, `resume-and-storage`, and `integrity` are compatibility constraints; the change must not weaken their intended guarantees.

## Impact

- `crates/engine/src/io/sink.rs`, `job/controller.rs`, `job/segmented.rs`, and output/commit support; coordination with the existing checkpoint-store resolver and state/event contracts.
- `crates/engine/src/lib.rs` rustdoc links; output, crash/restart, concurrency, and integrity tests; platform CI and performance evidence.
- No checkpoint schema migration, no replacement of the HTTP/resume/checkpoint seams, and no planned public API break. Publication behavior becomes stricter where current code fails to honor the v1 overwrite/atomicity contract.
