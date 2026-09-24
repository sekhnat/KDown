# Tasks

## 1. Benchmark correctness and baseline

- [x] 1.1 Fix `benches/throughput.rs` byte-labeled fields (especially warning-count-as-retransferred-bytes); derive unique completed, network and wasted/retried bytes from real counters and verify synthetic accounting/unit tests, including resume and failures.
- [x] 1.2 Report useful goodput and wire throughput separately with elapsed time and hash/size/publication checks; verify formulas for reused, retried and successful transfers.
- [x] 1.3 Add smoke versus manual matrices for 32 MiB/256 MiB/1 GiB, H1/H2 and 1/2/4/8 workers (manual multi-GiB/16); verify a bounded smoke run and the manual selection work without huge CI allocations.
- [x] 1.4 Add process-isolated fixture-server mode with controlled range/throttle/failure/reset/validator behaviors and record client-only resources; verify parity with in-process fixture for a deterministic file.
- [x] 1.5 Document repeatable profiling commands and baseline hardware/protocol/storage/network settings; capture baseline results using the existing `scripts/bench_check.sh` methodology, confirming CPU, RSS, context-switch/syscall and checkpoint/write timings or documented tool limitations.

## 2. Concurrent positional output

- [x] 2.1 Introduce safe cfg-gated Unix `write_at`/Windows `seek_write` adapter and checked write-all-at loop (short/zero/interrupted/error/overflow); verify platform build and deterministic partial-write tests.
- [x] 2.2 Separate `OutputSession` lifecycle/sync ownership from clonable write-only capabilities; retain scripted `OutputFaultScript` behavior and exclusive reclaim; verify fresh/reopened partial disposition and cannot-publish-while-writers-live tests.
- [x] 2.3 Replace segmented mutex/seek writes with independent offset writes through bounded long-lived blocking writer lanes (one outstanding `Bytes`/worker, acknowledged before progress); verify disjoint/out-of-order/unaligned/small/large-offset writes, failure propagation and final checksum/publication on Unix and Windows.
- [x] 2.4 Run the same baseline matrix before/after positional output and document output-lock removal, goodput scaling and writer-lane CPU/context-switch effects; verify comparison file exists.

## 3. Remove chunk flush and enforce durability

- [x] 3.1 Remove `FlushLevel::PageCache` from ordinary segmented chunks and distinguish written, synchronized and persisted checkpoint coverage without changing the sidecar format; verify no chunk calls flush and the existing resume suite passes.
- [x] 3.2 Implement safe interim coherent progress publication (e.g., a worker-record mutex) until milestone 5's tested atomic primitive replaces it; verify snapshots under concurrent publish/clear never mix generations and offsets.
- [x] 3.3 Route *all* segmented checkpoint saves, including pause/terminal, through a shared mode-aware save path: snapshot acknowledged/generation-validated ranges, durable data sync before metadata save, performance mode after written acknowledgment; verify injected sync failure prevents any save of new ranges.
- [x] 3.4 Add crash-boundary tests (write→sync, sync→save, save→crash), failed sync/store including post-rename failure, stale-generation progress and resume after interruption; verify checkpoints never lead the durability promised by the mode.

## 4. Single job checkpoint coordinator

- [x] 4.1 Move interval timing and reconciliation from `transfer_lease` to one job coordinator; feed it coherent progress and metadata revisions, skip unchanged snapshots, and verify one save per changed interval instead of one per worker.
- [x] 4.2 Move checkpoint serialization/store and file sync off network tasks using bounded blocking execution (not a task per chunk); verify chunk latency/checkpoint work and injected persistence failures converge on one terminal outcome.
- [x] 4.3 Coordinate pause, resume, keep/delete cancellation, job failure, generation invalidation and final join-before-verify/publish/cleanup with coordinator lifecycle; reuse/extend `checkpoint_seam_tests.rs` and output lifecycle tests to verify no post-cleanup saves or stale checkpoint.

## 5. Hot-path synchronization and worker metrics

- [x] 5.1 Replace interim progress publication with a single-writer coherent sequence-counter cell using the design's `SeqCst` order and scheduler lease/generation/bounds validation; verify stress and isolated Loom (where practical) tests including clear/reuse and stale generation.
- [x] 5.2 Split atomic fatal check from first-wins detailed error ownership; verify simultaneous failures retain exactly one error and idle/body workers converge without per-chunk async error locking.
- [x] 5.3 Reuse one stable per-job token bucket through startup and live limit/unlimited updates (including handle getter); verify update-before-job and update-during-transfer tests and no outer lock on unlimited chunks.
- [x] 5.4 Attribute network, unique completed, retries and wasted bytes to `worker_idx`, correcting retry accounting at lease boundaries; verify shard totals against actual received and unique coverage across retries.
- [x] 5.5 Reprofile chunk CPU, locks, write/checkpoint latencies and shard contention versus milestones 1/2; verify report and only add cache padding if evidence supports it.

## 6. Scheduler target policy

- [x] 6.1 Extend `SchedulerPolicy` with min/target/max and split threshold; use existing explicit `initial_segment_size`, plus backward-compatible opt-in automatic target derived from remaining ranges/(initial workers × configurable oversubscription), clamped safely; verify config validation and lease-size tests for small/large/resumed jobs.
- [x] 6.2 Replace worker-specific 256 KiB split constant and allocating `active_leases()` idle queries with policy and direct scheduler-state queries; verify split/failed-tail/stale-generation/unique-coverage tests.
- [x] 6.3 Benchmark multiple target sizes and oversubscription factors on H1/H2, comparing goodput and request/retry overhead; document why the initial tuning is selected and verify results are recorded.

## 7. Event-driven scheduler

- [x] 7.1 Add versioned scheduler-state signaling for new pending, requeue, split eligibility, concurrency changes, terminal failure/cancellation/completion; verify deterministic registration/transition race tests without lost work.
- [x] 7.2 Replace idle fixed 20 ms sleeps and pause/resume polling with predicate rechecks and notifications, while preserving retry/backoff timers as necessary; verify parked workers wake on requeue/split/resume/cancel and do not spin.

## 8. Persistent concurrency pool

- [x] 8.1 Keep up to `max_workers` tasks and writer lanes alive for the job; gate acquisitions by bounded desired worker count and settle leases on deactivation; verify runtime decrease without incomplete or duplicate byte coverage.
- [x] 8.2 Allow dormant tasks to reactivate on runtime increase without rebuilding, clamp manual control to `[min_workers,max_workers]`, preserve fixed-mode start; verify increase→decrease→increase and no lost work across retry, pause and cancellation.

## 9. Conservative adaptive controller

- [x] 9.1 Add explicit opt-in adaptive range-concurrency configuration starting at `min_workers`, leaving default fixed mode and H2 physical connection policy unchanged; verify configuration and H1/H2 stream-vs-connection tests.
- [x] 9.2 Collect windowed unique-byte goodput, network/wasted bytes, retry/throttle signals, worker idle and write latency; verify synthetic sample arithmetic, empty windows and no resumed-byte inflation.
- [x] 9.3 Implement additive +1 probing, revert/hold/reduce under pressure, smoothing, cooldown/hysteresis and manual-override precedence; verify deterministic controller traces for gains, no gain, 429/503 and storage pressure with strict bounds.
- [x] 9.4 Benchmark fixed versus opt-in adaptive across shaped and unconstrained server/network/storage cases; document empirically chosen windows/thresholds, worker stability, connection behavior and regressions.

## 10. Memory-path cleanup

- [x] 10.1 Remove unused per-worker `BufferPool` allocation and keep Hyper `Bytes` through rate limit and positional write without a mandatory copy; verify byte-pointer/copy-path tests and RSS/worker-count scaling.
- [x] 10.2 Make surviving `BufferPool::try_acquire` genuinely nonblocking on exhaustion and `acquire` async-safe, document exact job-versus-worker budget scope and validate too-small budgets; verify exhaustion/concurrent-lease tests.
- [x] 10.3 If the new bounded writer lanes need explicit buffering/backpressure, account retained bytes with one job-wide budget (including oversized frames via zero-copy slices) rather than per-worker full budgets; verify aggregate staged bytes never exceed configured allowance, and document Hyper ingress/RSS limits. If no staging is needed, document/test the one-frame-per-worker direct path instead.

## 11. Optional physical allocation

- [x] 11.1 Add portable logical `set_len` fallback plus optional supported physical reservation only where safe/profitable; verify unsupported platform/filesystem paths fall back without correctness changes.
- [x] 11.2 Test allocation/write out-of-space, permission failure, partial-file cleanup and retry/resume for both supported and fallback paths; verify real errors never publish incomplete output.

## 12. End-to-end validation and performance review

- [x] 12.1 Run full unit/integration suites and Unix/Windows build/test checks covering retry after partial progress, stale validators, interrupted resume, cancellation modes, injected sync/store failures, runtime rate/concurrency updates, final checksum and atomic no-replace/replace publication; verify all pass.
- [x] 12.2 Run isolated-server benchmark matrix against baseline and document useful/wire throughput, CPU/instructions/cycles per useful byte, RSS, syscalls/context switches, checkpoint/sync cost, worker scaling and H1/H2 differences by storage/network conditions; verify no ordinary global output mutex, per-chunk flush, redundant worker saves or multiplied explicit memory budget, and record any material regression/trade-off.
- [x] 12.3 Update user-facing docs for durability boundaries, checkpoint cadence, explicit vs auto sizing, fixed vs opt-in adaptive concurrency, manual override and memory-budget scope; verify examples/defaults match config and all OpenSpec artifacts validate with `openspec validate optimize-segmented-download-pipeline --strict`.
