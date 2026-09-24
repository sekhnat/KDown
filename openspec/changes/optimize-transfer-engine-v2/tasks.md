# Tasks

Every phase is independently verifiable. Before a behavior change, record its baseline and enable an internal legacy fallback; if evidence contradicts the design hypothesis, update the change artifacts and report the measurement instead of forcing the optimization. Use `cargo test -p kdown-engine`, `cargo clippy -p kdown-engine --all-targets -- -D warnings`, and the phase-specific checks below. Do not change the checkpoint sidecar or existing public defaults implicitly.

## 0. Discovery, fixture and trustworthy baseline

- [x] 0.1 Record the current commit, config defaults and invariants in `docs/benchmark-profiling.md`; reconcile historical `transfer-core` split/read-queued claim with `SegmentScheduler::split_tail` and write a focused reproducer asserting server-emitted versus accepted bytes (test fails if amplification reaches 2×).
- [x] 0.2 Extend `benches/throughput.rs` and `src/bin/fixture_server.rs` with isolated H1 **and H2 TLS** fixture parity, server-emitted payload/connection/request counters and size/hash/publication assertions; verify deterministic H1/H2 tests including fallback/validator changes and preserve `bench_check.sh` group/function identifiers.
- [x] 0.3 Add controlled bandwidth/RTT/loss/throttle/Retry-After/reset and slow-storage fixture modes, plus one-job/same-origin/different-origin harness; verify shaping calibration, isolated server/client CPU separation and hash parity with smoke tests. Mark unavailable netem axes explicitly.
- [x] 0.4 Expose or collect accurate unique completion, received/wasted bytes, amplification (undefined on zero denominator), retries, desired/actual active workers, connections/H2 streams, writer threads/bytes/queue/ack wait, split events, checkpoint latency, CPU/GiB and RSS; verify counter arithmetic under resume/retry and label unavailable scheduler contention/H2 stall/syscall/allocation fields.
- [x] 0.5 Run the repeatable baseline core (H1/H2, clean, local/10 ms, below-threshold/32 MiB/1 GiB, 1/2/4/8/16 workers where permitted) with ≥5 measured repetitions; record median/dispersion, correctness, commit and environment in a curated report.
- [x] 0.6 Run controlled WAN/manual baseline axes (100 Mbps/1 Gbps/max reliable, 50/150 ms, loss/throttle/reset+resume, multi-GiB when feasible); verify shaping calibration and mark unsupported netem/hardware combinations not run.
- [x] 0.7 Run storage and contention baseline axes (tmpfs/NVMe/slow sink, single/same-origin/different-origin jobs); verify client/server resource isolation and record CPU/GiB, RSS, writer threads and completion-time dispersion.
- [x] 0.8 Gate phase 0: run existing full tests, benchmark smoke and isolated parity; record exact result and baseline variance before altering transfer behavior.

## 1. Repair actual adaptive worker provisioning

- [x] 1.1 Write a deterministic failing `runtime_control_tests.rs` test: adaptive min=1/max≥4, held large ranges and a forced beneficial probe must produce >1 simultaneously active validated requests; assert current desired-only behavior fails before the fix.
- [x] 1.2 Add actual-active request/lease and parked worker gauges keyed by stable worker index in `SegmentedJob`, with tests that desired=4 but active=1 is not misreported as growth.
- [x] 1.3 Provision `max_workers` lightweight async tasks and matching progress/counter cells in `run_segmented`, maintaining register-before-check revision-wake behavior; verify growth, decrease, reactivation and bounded idle CPU with paused deterministic fixtures.
- [x] 1.4 In the temporary legacy writer branch, allocate `WriterLane` only on worker activation and release it on dormancy (or use the shared executor after phase 2), so max async capacity does not spawn max permanent blocking writers; verify writer count as desired rises/falls and owner reclaim waits for all handles.
- [x] 1.5 Join the adaptive-controller task on every exit and test cancellation/fatal/pause while workers are parked or holding leases; assert no stranded range, no late decision and no lost revision wakeup.
- [x] 1.6 Gate phase 1: run full tests, fixed/adaptive H1/H2 correctness and same-host baseline comparison, recording actual-vs-desired curves, CPU/RSS and thread count; retain fixed default and ability to disable new provisioning if regressions appear.

## 2. Bounded shared writer and acknowledgement semantics

- [x] 2.1 Define internal `WriteExecutor` submit/completion interface in `io/` using existing `OutputWriteHandle::write_blocking`; verify fault-script, short/zero/Interrupted positional writes, offsets and Unix/Windows cfg compilation (CI for Windows if target absent locally).
- [x] 2.2 Implement configurable engine-global and per-job outstanding-write byte budgets (plus bounded per-worker read-ahead) with pre-read reservation and oversize-frame split/reconciliation; validate minimum frame/executor limits at construction and test exact budget, cancellation and failure permit release.
- [x] 2.3 Implement a small controller-shared blocking positional executor with fair per-job admission and bounded queued+executing payload, no permanent worker-to-thread coupling; verify many jobs × 16 async workers create only configured writer threads and each job makes progress under a slow sink.
- [x] 2.4 Add generation-tagged per-lease received/submitted/acknowledged contiguous frontiers and deterministic reverse-completion tests; assert missing/failed earlier write blocks later publication and scheduler unique-credit never doubles after retries/splits.
- [x] 2.5 Integrate the executor into **H1 segmented** `transfer_lease` under an internal legacy/new switch; verify overlap (network receives next bounded chunk while previous write executes), bounded bytes, slow-disk backpressure and server/client emitted-vs-received counters.
- [ ] 2.6 Migrate **H2 segmented** through the same write abstraction; test concurrent streams, out-of-order writes, one default H2 connection, exact offsets and hash without altering range validation or H2 connection policy.
- [ ] 2.7 Drain or invalidate queued writes on pause, keep/delete cancellation, retry, validator change and fatal write error before `coordinator_loop` save/reclaim; test durable sync-before-save, performance-mode prefix, process-restart resume, ENOSPC, permission error, no permit leaks and shutdown timeout/no deadlock.
- [ ] 2.8 Gate phase 2: full suites plus single/multi-job H1/H2 slow/fast storage benchmarks; record thread/job scaling, queued bytes, acknowledgement latency, goodput, CPU/GiB and RSS vs legacy; retain `WriterLane` rollback until parity and defensible benefit.

## 3. Segment duration and duplicate-payload control

- [ ] 3.1 Add deterministic scheduler property tests for normalized pending/active/completed union, stale generations, inclusive `[S,E]`/exclusive frontier math, exact final partial range, retry/split/concurrency cycles and resumed intervals; run randomized seeds and record reproducible failures.
- [ ] 3.2 Measure requested/received/submitted high-watermarks separately from `lease.next_offset`; change `split_tail` eligibility to avoid bytes received/queued for an original request (or stop/drain it safely) and verify split timing with server-emitted-byte fixture.
- [ ] 3.3 Prefer pending unclaimed ranges and add measured ready-work target (~2–3× desired candidate) with straggler-only live split; verify no lease leak, starvation or overlapping logical ownership under 1→4→1 worker changes.
- [ ] 3.4 Implement opt-in duration-informed `TargetSelector` using smoothed per-worker *unique* goodput, RTT/request-cost guard and bounded size-step changes; verify fast/slow scripted samples, short job fallback to explicit size, min/max bounds and checkpoint resume gaps.
- [ ] 3.5 Sweep target duration 0.5/1/1.5 s and ready-work factors on H1/H2 WAN-shaped fixtures; record segment/split counts, amplification, completion latency and variance before choosing a documented opt-in default. Keep existing Explicit/Automatic behavior unchanged.
- [ ] 3.6 Gate phase 3: deterministic clean H1 partial-range/split test checks server-emitted and client payload, exact hash, unique coverage, amplification <1.10 and unconditional 2× regression failure; run H2 and baseline comparisons; record any exception with evidence, not a relaxed silent threshold.

## 4. Adaptive controller v2 and storage pressure

- [ ] 4.1 Replace instantaneous worker-idle/last-write-latency inputs with interval-weighted actual active/idle counts, writer queue/ack p50-p95, byte-budget wait and useful-goodput deltas in `WindowSample`; test no resumed-byte or duplicate-wire inflation.
- [ ] 4.2 Add storage-pressure veto and marginal-gain/cost probes to `AdaptiveController`; use deterministic synthetic windows for benefit, no-gain, high RSS/CPU, writer saturation, retries and 429/503; test cooldown, bound clamping and re-probe after recovery.
- [ ] 4.3 Integrate decisions into active workers while preserving manual `DownloadHandle::set_concurrency` override; test actual growth/reduction (not only desired value), pause/retry/checkpoint interactions and no oscillation on noisy samples.
- [ ] 4.4 Gate phase 4: H1/H2 shaped WAN and constrained disk fixed-vs-v1-vs-v2 runs with median/dispersion; document chosen thresholds and reason codes; keep legacy adaptive selector switch until beneficial outside noise and no fixed-mode regressions.

## 5. Protocol-aware concurrency

- [ ] 5.1 Instrument request/stream count separately from physical TCP/TLS establishments and negotiated protocol in `http/{transport,connect}.rs`; tests reconcile H1 connections and H2 streams with server-side counts and mark unavailable H2 flow-control data.
- [ ] 5.2 Gate H1 additional connections by marginal *useful* benefit, retry/throttle and existing `ConnectionLimits`; test many H1 requests never exceed per-origin/global permits and revert when marginal gain disappears.
- [ ] 5.3 Keep default H2 on one multiplexed socket while probing stream concurrency; test peer stream limits where exposed and `H2ConnectionPolicy::Additional` explicit override stays compatible. Permit automatic additional sockets only behind proven bottleneck and configured policy; otherwise record unsupported/unused state.
- [ ] 5.4 Gate phase 5: compare shaped H1/H2 single/multi-job throughput, fairness, stream/socket counts, CPU/RSS with prior phase and record protocol-specific decision traces and fallback.

## 6. Cross-job origin coordination

- [ ] 6.1 Implement normalized final-origin identity (scheme/IDNA host/effective port) distinct from connector proxy/TLS pool key; test redirects, mixed-case/default ports, proxy and credentials redaction.
- [ ] 6.2 Add controller-shared origin registry with fair cancellable request admission and RAII release, retaining connector `ConnectionLimits` for physical sockets; test two jobs/one origin fairness, one job starvation avoidance and separate-origin independence.
- [ ] 6.3 Propagate `RetryClassifier`-capped 429/503/Retry-After to same-origin peers, with cooldown recovery/probe and no unbounded retries; test simultaneous jobs, 429, 503, Retry-After, retry exhaustion, cancel/failure release and unrelated origin progress.
- [ ] 6.4 Bound registry lifetime with inactive TTL/size cap without evicting live permit holders; stress many ephemeral origins and assert bounded retained entries and no leaked/negative capacity.
- [ ] 6.5 Gate phase 6: compare same-origin, mixed-origin and no-origin-feedback variants under server throttling; report latency/fairness, aggregate goodput, connections and cooldown traces. Retain per-job backoff fallback if the shared policy regresses.

## 7. Evidence-gated hot-path tuning

- [ ] 7.1 Verify whether segmented and single-stream paths actually apply the configured global limiter alongside per-job limits; add failing tests if not, repair without changing unlimited-path fast check and verify live updates, fairness and bounded burst.
- [ ] 7.2 Profile limited-mode `TokenBucket` lock contention under many workers/jobs; only if material, prototype bounded local token leasing with runtime invalidation and aggregate-limit tests, compare CPU/GiB and burst before keeping it; otherwise record no-change decision.
- [ ] 7.3 Profile `WorkerCounters` cache-line interference and `LeaseProgress` publish frequency; only if material, test padding or byte/time batching with documented maximum crash-lag and forced pause/checkpoint flush. Any atomic-order relaxation needs a synchronization proof, Loom/equivalent model and stress tests; otherwise preserve SeqCst.
- [ ] 7.4 Sweep `read_buffer_size` 64/128/256/512 KiB after writer pipelining (H1/H2, WAN/TLS, CPU/GiB, syscalls/allocations when available, RSS, queue pressure); retain 128 KiB unless improvement exceeds dispersion and resource costs.
- [ ] 7.5 Gate phase 7: run full regression suites and publish a table of retained/rejected optimizations, variance and unchanged defaults; do not claim unused `BufferPool` reuse.

## 8. Physical allocation/storage evaluation

- [ ] 8.1 Compare logical-only versus opt-in physical preallocation on large tmpfs/NVMe and constrained/slow storage where available; record startup latency, throughput, fragmentation if measurable, sparse behavior, ENOSPC, cancellation and filesystem fallback with repetitions.
- [ ] 8.2 Re-run `allocation_tests.rs` and resume/retry/collision tests on both policies; document unsupported/error behavior and retain physical-off default unless a separately justified compatibility decision is approved.

## 9. Final correctness and benchmark acceptance

- [ ] 9.1 Run matrix for ordinary single-stream/small file, segmented H1/H2, exact boundary/final tail, Range-ignore/malformed `Content-Range`/200-to-range/unexpected status/EOF, retryable and nonretryable errors/exhaustion, changed validators and resume from process restart; assert hash/size and byte-exact coverage in each relevant case.
- [ ] 9.2 Run queued-write pause/cancel/retry and durable/performance checkpoint tests, write failure/ENOSPC/permission, checksum success/failure, destination collision/atomic replace/no-replace, partial cleanup and multi-job independent destination tests; assert no write or save after publication.
- [ ] 9.3 Run randomized scheduler/ack-generation properties and concurrency stress (including lost wakeups, writer permit release, shutdown timeouts, origin fairness and capacity accounting); use Loom/equivalent only if atomic ordering changes, plus sanitizer tests where available.
- [ ] 9.4 Run the full representative D0 matrix on baseline and optimized commits with the same host/shaping/repetitions; report median+dispersion, useful/wire throughput, amplification, CPU/GiB, RSS, writer threads/latency, completion time, correctness and any unavailable axes. Gate on <1.10 clean H1 split amplification, actual adaptive up/down, bounded bytes, no material single-stream or CPU regression and no statistically unsupported speedup claims.
- [ ] 9.5 Update `README.md`, `docs/benchmark-profiling.md`, config reference and curated results with stable vs advanced vs internal knobs, limitations, rollback switches and known future options (HTTP/3, io_uring/IOCP, PGO). Remove obsolete `WriterLane` and temporary switches only after new path parity, benchmark acceptance and user-visible compatibility checks; otherwise keep the fallback and document it.
- [ ] 9.6 Run `cargo test --workspace --all-targets`, `cargo clippy --workspace --all-targets -- -D warnings`, doc build and supported target checks; record results/host limitations and reconcile this change's capability deltas with prior unsynced changes before any OpenSpec sync/archive.
