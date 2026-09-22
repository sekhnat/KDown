# Tasks

## 1. Build the resume-admission protocol

- [x] 1.1 Replace the `ResumeSupport` helper shape in `resume/flow.rs` with crate-private begin/pending/decision/plan types that keep loaded checkpoint state opaque, and verify unit tests cover `Never`, `Allowed` absent, and `Required` absent/corrupt pre-probe outcomes.
- [x] 1.2 Implement policy-aware corrupt-checkpoint recovery and restart cleanup through `dyn CheckpointStore`, failing with a structured checkpoint error when deletion fails, and verify a failure-scripted in-memory store asserts load/delete calls and fail-closed behavior.
- [x] 1.3 Implement final admission ordering—generation comparison before temp-file plausibility—and verify focused tests cover matching, stale generation, missing temp, short temp, partial, and complete checkpoint states, including resource-change notices and preserved warning text.
- [x] 1.4 Add mode-specific immutable views from one `ResumePlan` for segmented completed ranges and sequential contiguous-prefix offset/accounting, and verify unit tests cover empty, contiguous, disjoint, and fully complete ranges without counting sequential holes as reused.

## 2. Integrate admission into job orchestration

- [x] 2.1 Start admission before the existing probe loop and route early rejection through the current terminal-failure path, and verify a controller test with `ResumePolicy::Required` and no checkpoint fails without entering or emitting `Probing`.
- [x] 2.2 Finalize admission immediately after probe, emit returned resource-change notices before failure transitions, and propagate plan warnings/validators without controller-side generation or temp validation; verify a controller event test observes `ResourceChanged` before `Failed` for a stale checkpoint.
- [x] 2.3 Select the sequential or segmented view only after range verification and transfer-mode eligibility are known, apply its reused-byte count exactly once, and feed its offset/ranges/checkpoint state into existing transfer and checkpoint-save paths; verify targeted tests show both modes resume byte-identically with correct reused-byte accounting.
- [x] 2.4 Preserve D4 transfer-time durability and checkpoint-save ordering while replacing `loaded_cp` plumbing with admitted plan state, and verify existing durable-range/checkpoint tests plus `cargo test -p kdown-engine --test resume_tests` pass.

## 3. Remove shallow paths and retain integration evidence

- [x] 3.1 Remove `ResumeSupport`, its `resume::mod` re-export, the controller's private `change_policy` helper, and obsolete fragment tests while leaving `job_identity`, checkpoint/store APIs, and the unexpanded `GenerationChangePolicy` symbol intact; verify `cargo check -p kdown-engine --all-targets` succeeds and no production/test reference to `ResumeSupport` or `change_policy` remains.
- [x] 3.2 Replace the external `ResumeSupport::remaining_ranges` test with admission-boundary assertions and retain real-file corrupt, interrupted, and generation-mismatch scenarios; verify `cargo test -p kdown-engine --test resume_tests` and `cargo test -p kdown-engine --test segmented_tests` pass.

## 4. Verify the completed refactor

- [x] 4.1 Run `cargo fmt --all --check` and `cargo clippy -p kdown-engine --all-targets --all-features -- -D warnings`, resolving all formatting and lint failures.
- [x] 4.2 Run `cargo test -p kdown-engine --all-targets` and confirm the full engine suite passes with unchanged checkpoint-format fixtures and no new dependency or public admission export.
