# Design

## Context

See `proposal.md` for motivation and the two delta specs for observable behavior. Today `run_inner` rejects an existing destination only before probing; `FileSink::commit` receives no overwrite policy and performs an unconditional rename, with a delete-before-rename path on Windows. Jobs open the same destination-derived `.part` path, while checkpoint sidecars are keyed by job identity (which includes the URL), so checkpoint-store coordination does not protect two different URLs writing one temp file. Sequential and segmented completion independently verify and publish; segmented completion opens a second `FileSink` over the worker output. The docs job already denies rustdoc warnings; only the broken links need correcting.

The default file store, resume admission, HTTP execution, and state/event interfaces should remain intact. The existing v1 delta specs (not yet synchronized into main specs) define the baseline conflict, integrity, resume, and durability guarantees. This change tightens enforcement, not those baseline promises.

## Goals / Non-Goals

**Goals:**
- Give one job exclusive, crash-releasable ownership of destination-associated mutable artifacts before any resume/checkpoint/output operation.
- Make policy-aware publish atomic where supported and non-destructive on failure, including a destination created during transfer.
- Keep one output owner and one completion sequencer for both transfer modes, with deterministic failure injection and parity tests.
- Pass the existing strict rustdoc job and preserve public constructors/request/result shape and checkpoint JSON.

**Non-Goals:**
- Generalize HTTP, resume admission, or the checkpoint port again; add public output injection, new download policies, or promise improved throughput.
- Permit concurrent jobs for the same destination. Rejecting the second job is the chosen valid outcome; it can retry after ownership is released.
- Silently fall back to non-atomic replacement on a platform where no safe replacement primitive is available.

## Decisions

### 1. Hold an exclusive destination lease across the entire job

Acquire a destination-scoped ownership guard before checkpoint resolution/admission and before output preparation, and retain it through transfer, worker join, terminal cleanup, and result construction. Key it by an existing/canonicalized parent directory plus destination filename so separate controllers and path aliases to that parent contend. A process-local registry prevents two controllers in one process from bypassing the guard; an OS-managed exclusive lock on a stable, destination-derived lockfile coordinates processes. Never unlink the lockfile while another process could be waiting: the file may persist, but a crashed process releases its OS lock, allowing later resume admission to validate the existing `.part` and checkpoint. On an unsupported filesystem or lock error, fail closed before touching shared output/checkpoint state. The guard is released on normal completion, cancellation, error, and task unwind; do not hold an async mutex over the network.

A directory-existence sentinel or PID-only lockfile was rejected because stale files can permanently block recovery and existence checks cannot prove a former process died. Per-job unique temp paths without a lease were rejected because the current resume sidecar/`.part` identity and cleanup semantics would need a migration, and concurrent publication decisions would still need coordination. The early `FailIfExists` check remains for cheap rejection but cannot be the final arbiter; even a dangling symlink or late-created entry must be rejected by the publication operation itself.

### 2. Make publication an explicit policy-aware filesystem operation

Move final publish behind the output boundary with distinct no-replace and replace operations. `FailIfExists` uses an atomic no-clobber filesystem operation on the destination directory entry; never use `exists()` followed by ordinary rename as the correctness boundary. `Replace` uses a platform-verified atomic replacement primitive where available, after all file handles required by that platform are settled. Eliminate the Windows delete-before-rename fallback. If the selected filesystem cannot provide the required safe operation, return a structured commit/conflict error with the old destination untouched. Preserve same-directory temp placement, file/parent durability ordering, and existing final-path reporting. Validate Linux, macOS, and Windows behavior in CI; unsupported filesystem behavior is an explicit safe failure rather than an emulated non-atomic success.

Using a second destination check was rejected as time-of-check/time-of-use unsafe. Copying into or deleting the old destination before rename was rejected because observers could see missing or partial content and a failed publish could destroy valid prior data. Introduce only the smallest platform adapter/dependency needed to access and test no-clobber/atomic-replace operations; record platform support and failure classifications in its tests before wiring orchestration.

### 3. Introduce one output session and one completion sequencer

Create a crate-private output session around the file implementation. It owns temp preparation/resume reopening, positional writes, flush acknowledgements, retry disposition, pause/cancel artifact policy, verification access, and final policy-aware publication. The existing public `Sink`/`FileSink` surface may stay compatible, but job code no longer toggles `keep_on_drop` or constructs a second file sink to commit. Sequential mode holds the session directly; segmented workers borrow/share a bounded write handle, and only after every worker has joined does the completion owner regain exclusive access to the original session. Keep the current mutex/seek serialization initially; changing write mechanics is a separate measured decision.

A single job-level completion sequencer consumes the stopped transfer's result, session, validators, counters, checkpoint handle, and event/state handles. It owns exact-size and sequential whole-file SHA-256/SHA-512 verification, transition/event ordering, session finalization and publish, checkpoint delete, and terminal result. The I/O session owns the actual publish; no second component may commit independently. Before publish, faults flow into one structured `Failed` path and preserve the old destination. After publish, delete failure produces a warning while retaining `Completed` and the final path. Admission delete failures remain fatal; pause/cancel still converge after workers settle. This separates job-state/event responsibility from filesystem mechanics without duplicating either completion sequence.

Extracting phase functions into separate controller files was rejected because it would retain two commit owners. Moving events and checkpoint store selection into the filesystem adapter was rejected because it would couple output mechanics to job policy and break existing seams.

### 4. Test the boundary at two levels and measure the migration

Add a crate-private fault-scripted output adapter/session covering open, write, flush, verification read, finalize, publish, and cleanup errors with exact operation logs and synchronization gates. Scripted HTTP already supplies deterministic body faults and transfer ordering; pair both to test sequential/segmented parity, no-clobber races, cancellation, checkpoint warnings, and event ordering without wall-clock timing. Keep real-filesystem, process-crash/resume, Unix/macOS/Windows replace, and real-HTTP suites for adapter fidelity. Capture pre/post four-worker HTTP/1.1 and HTTP/2 throughput and memory on the same machine and investigate any material loss before accepting the refactor. Fix only the three unresolved intra-doc links and re-run the strict docs gate at the beginning of implementation.

Pure real-filesystem fault tests were rejected as the only evidence because permission, disk-full, and commit timing faults are not deterministic. Pure scripts were rejected as the only evidence because filesystem atomicity and OS locking are platform behaviors.

## Risks / Trade-offs

- [Cross-process locks on some filesystems may not be reliable] → Use OS-managed locks with a process-local registry, test separate processes, and fail closed when the guarantee cannot be established; document any unsupported filesystem/primitive rather than silently claiming safety.
- [Destination path aliases or a replaced parent directory could evade a naive path key] → Resolve an existing parent to a stable directory identity before lease acquisition, test common relative/symlink aliases, and bind filesystem operations to that identity where the platform allows; do not treat a string-only registry as sufficient.
- [A lockfile persists after jobs] → Never unlink it as a lock-release mechanism; document that its unlocked presence is harmless and verify crash recovery.
- [Windows replacement may not be atomic on every filesystem] → Remove delete-before-rename and fail non-destructively if the vetted replacement operation is unavailable or errors.
- [Refactoring output ownership can alter drop/partial-state behavior] → Test every cancel, pause, retry, write/flush failure, hash failure, and process-restart disposition in both modes before removing old paths.
- [The existing global write mutex can cap segmented throughput] → Preserve it during safety migration, then measure before considering positional-write changes; reject unmeasured performance claims.
- [A publish succeeds but checkpoint delete fails] → Preserve the existing warning-bearing `Completed` contract; never roll back the visible file or report a false `Failed` outcome.

## Migration Plan

1. Repair rustdoc links and establish the CI/documentation baseline. Record benchmark results on one host before output changes.
2. Implement/test destination lease and policy-aware publication under the existing controller; prove competing jobs and commit-time conflicts fail safely on supported platforms before restructuring completion.
3. Introduce the output session and scripted faults, migrate sequential writes/retries and segmented write ownership, then route both through the single completion sequencer. Remove obsolete reopen-for-commit, duplicate hashing/result construction, and controller drop-flag paths only after parity tests pass.
4. Run the full format/test/Clippy/rustdoc gates, real-file crash tests, platform-specific CI matrix, and same-host post-change benchmarks. Update public output/overwrite documentation to match behavior.

Existing checkpoints and `.part` files remain readable; no data migration is planned. A code rollback after deploying the safer publication path would reintroduce the old overwrite/Windows risks, so do not roll back into a safety-sensitive release without an explicit operator decision.
