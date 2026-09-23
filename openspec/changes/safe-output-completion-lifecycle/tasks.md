# Tasks

## 1. Documentation Gate and Baseline

- [x] 1.1 Resolve the HTTP execution intra-doc links in `crates/engine/src/lib.rs` without warning suppression; verify `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` succeeds and the generated links point to the public items.
- [x] 1.2 Record a same-host pre-change benchmark for four-worker HTTP/1.1 and HTTP/2 throughput and memory; verify results and machine/command details are saved for a comparable post-change run.

## 2. Exclusive Destination Ownership

- [x] 2.1 Add a destination key based on the resolved parent and filename plus a process-local nonblocking ownership registry; verify two controllers targeting the same path or parent-directory aliases cannot both acquire it, while distinct destinations can.
- [x] 2.2 Add an OS-managed cross-process lock on a stable destination lockfile, with safe failure when locking is unavailable and no unlink-on-release; verify separate-process contention, release on exit/crash, and harmless reuse of a persistent unlocked lockfile.
- [x] 2.3 Acquire and retain ownership before checkpoint resolution, resume admission, and temp opening through joined-worker terminal cleanup; verify competing jobs fail with a structured result before mutating shared `.part`/checkpoint artifacts, including jobs with different URLs.
- [x] 2.4 Add process-crash and subsequent resume tests for partial output and a stale lockfile; verify checkpoint-format fixtures remain unchanged and a new job can validate/recover state without damaging a live owner's artifacts.

## 3. Policy-Aware Publication

- [x] 3.1 Introduce a filesystem publication operation with atomic no-replace and safe replace variants on each supported OS, and remove the Windows delete-before-rename fallback; verify adapter tests cover success, existing/dangling destination entries, unsupported operations, and failed replacement preserving the old bytes.
- [x] 3.2 Thread overwrite policy into final publication while retaining the early `FailIfExists` shortcut; verify a gate-driven test creates the destination immediately before publication and observes a structured conflict, unchanged destination, and no committed event.
- [ ] 3.3 Exercise `Replace` and `FailIfExists` against real files on Linux, macOS, and Windows; verify successful replacement is atomic to observers and failed/unsupported publication neither deletes the previous destination nor returns `Completed`.

## 4. One Temporary-Output Owner

- [x] 4.1 Add a crate-private output session owning prepare/reopen, positional write, flush, and partial-artifact disposition while preserving the public sink surface; verify direct session tests cover fresh and resumed output, write/flush failures, and drop behavior.
- [x] 4.2 Route sequential fresh, resume, retry, pause, and cancellation paths through the session instead of controller-owned `keep_on_drop`; verify `single_stream_tests`, `resume_tests`, and scripted retry/cancel cases preserve bytes, checkpoints, and selected partial-output policy.
- [x] 4.3 Give segmented workers a bounded shared write handle and return exclusive ownership to the session after all workers join; verify segmented output is byte-exact, bounded, and cannot be finalized while workers still write.
- [x] 4.4 Remove the segmented reopen-for-commit and related duplicate output cleanup; verify segmented pause, cancellation, failure, and crash/restart suites retain correct artifacts and no job path creates a second sink solely to publish.

## 5. One Verified Completion Path

- [x] 5.1 Implement one completion sequencer for both modes with exact-size, sequential whole-file SHA-256/SHA-512 verification, durability finalization, policy-aware publish, checkpoint cleanup, and terminal events/results; verify parity cases assert identical ordering and error categories for both modes.
- [x] 5.2 Route sequential and segmented completion through the sequencer and remove duplicate digest/result construction; verify SHA-256/SHA-512 matches and mismatches, size mismatches, finalization failure, and publish failure leave prior destinations intact and never emit a false committed event.
- [x] 5.3 Preserve post-publish checkpoint-delete warning semantics and fatal pre-transfer safety deletion; verify scripted-store tests assert delete ordering, warning event/result visibility, `Completed` after successful publish, and failure on unsafe admission cleanup.
- [x] 5.4 Add a crate-private fault-scripted output adapter with ordered operations and synchronization gates for open, write, flush, verification read, finalize, publish, and cleanup; verify each injected fault has a deterministic structured outcome and artifact disposition in each applicable transfer mode.

## 6. Compatibility and Release Evidence

- [x] 6.1 Update output/overwrite and ownership documentation to describe no-clobber publication, safe unsupported-platform failure, lockfile persistence, and unchanged resume/checkpoint semantics; verify examples, rustdoc, and existing v1 acceptance text agree with the implementation.
- [ ] 6.2 Run format, workspace all-target tests, strict Clippy, warnings-denied rustdoc, real-file crash/restart, and the supported OS CI matrix; verify every gate passes with no change to public construction patterns or checkpoint fixtures.
- [x] 6.3 Run the same-host post-change four-worker HTTP/1.1 and HTTP/2 benchmark with memory measurements; verify a recorded before/after comparison and investigate any material throughput or memory regression before marking the change complete.
