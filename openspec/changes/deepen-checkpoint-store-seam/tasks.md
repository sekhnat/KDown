# Tasks

## 1. Checkpoint Port and Coordination

- [x] 1.1 Add the public checkpoint resolver/context contract, the destination-relative default sidecar resolver, and resume-module re-exports; verify unit tests cover context values, per-destination resolution, and resolver errors.
- [x] 1.2 Add the crate-private coordinated checkpoint-store decorator over `Arc<dyn CheckpointStore>` with serialized load/save/delete operations and monotonic same-generation range handling; verify deterministic unit tests cover non-overlap, stale snapshot suppression/merge, lineage rejection, and save-before-delete ordering.
- [x] 1.3 Tighten `FileCheckpointStore` atomic replacement so worker sharing cannot collide and supported durable parent-directory sync failures are reported; verify real-filesystem tests cover round trips, atomic replacement, no successful torn/temp residue, durable sync handling, and missing-file deletion.
- [x] 1.4 Add shared in-memory/failure-scripted checkpoint test support with queued outcomes, operation logs, and deterministic concurrency gates; verify its focused tests can inject load/save/delete failures and detect overlapping operations without wall-clock races.

## 2. Controller Selection and Sequential Lifecycle

- [x] 2.1 Store the resolver on `SingleStreamController`, add the consuming resolver-injection API, preserve every existing constructor with the default resolver, and clone the dependency into spawned job controllers; verify controller tests prove one resolution per job, distinct destination contexts, existing construction compatibility, and pre-probe checkpoint failure on resolution error.

- [x] 2.2 Resolve and coordinate one store in `run_inner`, pass it to resume admission, and replace every sequential concrete-store call with the trait-object handle; verify the controller and resume tests exercise custom-adapter load/save/delete ordering without a sidecar fallback.
- [x] 2.3 Propagate resume-refresh, cadence, and pause save failures through the structured `Failing`/`Failed` path while preserving consistent partial output and the previous checkpoint; verify scripted sequential tests cover all three save sites and assert `DownloadError::Checkpoint`, no commit, and preserved artifacts.
- [x] 2.4 Refactor terminal cleanup to return checkpoint warnings, attempt deletion before the terminal transition, and preserve `Completed`/`Cancelled` after post-commit or cancellation delete failure; verify sequential tests assert final path/status, warning and warning-event visibility, each cancellation mode, and admission deletion remaining fatal.

## 3. Segmented Lifecycle

- [x] 3.1 Change `run_segmented`, `worker_loop`, `transfer_lease`, `persist_checkpoint`, and segmented completion to accept/clone `Arc<dyn CheckpointStore>` and return persistence errors instead of discarding them; verify `cargo check -p kdown-engine --all-targets` passes and neither job orchestration module imports or requires `FileCheckpointStore`.
- [x] 3.2 Persist an absorbed scheduler snapshot when segmented transfer pauses and route a save failure into the shared fatal path so all workers converge before failure; verify scripted segmented tests cover pause persistence, checkpoint-category failure, worker shutdown, monotonic ranges, and preserved partial output.
- [x] 3.3 Apply the common cancellation and successful-commit cleanup behavior after segmented workers join, including delete warnings without outcome replacement; verify segmented tests cover discard/keep cancellation modes, delete-after-save order, and post-commit delete failure retaining `Completed`.

## 4. Contract and Regression Evidence

- [x] 4.1 Add end-to-end alternate-adapter tests for sequential and segmented jobs that assert resume admission, cadence, pause, cancellation, and commit cleanup all use the one resolver-selected adapter; verify no real checkpoint sidecar is created in those tests.
- [x] 4.2 Update `KDownSpec.md` to document controller-level checkpoint resolver selection, per-job mutation ordering, fatal save failures, and the warning-bearing exception when deletion fails after irreversible commit/cancellation; verify the documented state/order matches the delta spec and implemented tests.
- [x] 4.3 Retain and extend `resume_tests.rs` and `crash_restart_tests.rs` real-sidecar coverage for corrupt state, atomic replacement, pause/process interruption, and byte-exact restart; verify both integration test binaries pass with the default resolver.
- [x] 4.4 Run `cargo fmt --all -- --check`, `cargo clippy -p kdown-engine --all-targets -- -D warnings`, `cargo test -p kdown-engine`, and `openspec validate deepen-checkpoint-store-seam --strict`; verify all checks pass and every completion criterion has direct test evidence.
