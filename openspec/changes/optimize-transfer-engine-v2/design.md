# Design

## Context

See [proposal.md](proposal.md) for motivation and the five delta specs for behavioral contracts. **Verified against current code** (not inferred from older planning):

- `job/segmented.rs::run_segmented` initializes adaptive `desired` at `min_workers`, then sets `worker_count = job.desired_workers()`, lends exactly that many output handles, spawns that many `WriterLane`s and async worker tasks. `adjust_desired_workers` may raise the number but cannot spawn missing workers. `worker_progress` allocates `desired.max(16)` cells; these are *not* tasks. `worker_loop` already has revision-watch register-before-check dormancy, so wakeup need not be redesigned.
- `io/writer_lane.rs::WriterLane::spawn` creates one long-lived `spawn_blocking` consumer per worker with `mpsc::channel(1)`; `LaneHandle::write` waits for a per-chunk oneshot acknowledgement before `transfer_lease` reads another chunk. `OutputWriteHandle::write_blocking` calls checked `write_all_at` on an `Arc<File>` using Unix `write_at` / Windows `seek_write`. `OutputSession::reclaim_exclusive` refuses live writers. The existing `buffer_pool_max_bytes` bounds the public pool, **not** these Hyper ingress buffers.
- `scheduler/core.rs::SegmentScheduler` owns nonoverlapping pending/completed interval sets and active leases with generation checks. `SchedulerPolicy::resolve_target` chooses an initial size once (`Explicit` default 8 MiB; `Automatic` initial remaining/(workers × oversubscription)); `worker_loop` tries `split_tail` whenever pending is empty. `split_tail` uses acknowledged `lease.next_offset`, not bytes already received or queued. The historical `download-engine-v1` transfer-core delta says a split excludes read/queued bytes: **the implementation does not currently establish that**, so treat this as a correctness/efficiency gap, not an already-solved invariant. Existing sweep data shows low overhead at some explicit sizes, not a WAN-wide guarantee.
- `LeaseProgress` publishes `(lease_id,generation,lease_start,written_through)` via a single-writer SeqCst sequence cell; `reconcile_and_credit` folds *scheduler-accepted* deltas into `JobCounters`, not raw repeated chunk writes. `coordinator_loop` is job-local and persists only acknowledged settled ranges: Performance mode after page-cache write acknowledgement, Durable after successful `OutputSyncCapability::sync_data()` and before atomic checkpoint save. `run_segmented` joins workers/lanes then stops the coordinator before reclaim, verify and publish. `job/controller.rs` owns resume admission, hash/size verification, cancellation policy and atomic commit; `http/range.rs`, `http/transport.rs` validate range responses/validators and choose safe fallback. Preserve all these contracts and sidecar format.
- `control/adaptive.rs` samples unique completed bytes in 500 ms windows with EWMA and +1 probes, but the worker-idle ratio is an instantaneous cell sample and `write_latency_us` stores only the last ack. `origin_backoff_until` is a mutex **per SegmentedJob**, so peers do not share 429/503 feedback. `http/connect.rs::ConnectionLimits` already has RAII global/per-origin physical connection semaphores keyed by scheme/host/port/proxy; it has no bounded eviction. `http/transport.rs` uses one H2 client slot by default and round-robins configured additional slots; connection count and stream count are different. `TokenBucket` bypasses its mutex when unlimited; limited mode locks per acquisition. `RateLimiter` supports global+job composition but `SegmentedJob::acquire_rate` currently uses the job bucket directly; establish actual global use before claiming aggregate enforcement.
- `benches/throughput.rs` already offers in-process H1/H2 smoke/matrix, size sweep and an **H1-only** process-isolated fixture server (`src/bin/fixture_server.rs`). `docs/benchmark-profiling.md` and `benches/results/report-milestone-12.md` record local measurements and missing profiling tools; no controlled H2 WAN matrix or multi-job resource study exists. Tests already cover pause/checkpoint seam, retry/range fallback, positional writes, rate updates and fixed-worker cycles, but `runtime_control_tests.rs::adaptive_concurrency_starts_at_min_and_stays_in_bounds` does not prove scale-up.

### Before: current data flow

```text
Hyper H1 connections / H2 streams → fixed number of async workers (= initial desired)
    → each owns lease + coherent progress cell → await per-chunk rate + WriterLane.write
    → 1 channel(1) + 1 permanent spawn_blocking consumer *per worker*
    → positional OutputWriteHandle → page-cache acknowledgement → LeaseProgress
    → scheduler reconciliation/unique counters → job checkpoint coordinator
    → join lanes → OutputSession owner → verify size/hash → atomic publish
             [per-job backoff only; adaptive desired may exceed task count]
```

## Goals / Non-Goals

**Goals:** true elastic *actual* async worker capacity; few bounded blocking writer executors shared by network workers/jobs; overlap within a provable byte cap; contiguous ack and unique-credit correctness; measured segment/stream/origin decisions; compatible defaults and reversible phase gates. **Non-goals:** new file formats, rewriting single-stream output, auto-enabling physical preallocation, default adaptive policy without evidence, HTTP/3, io_uring/IOCP, unsafe atomic weakening, UI/CLI redesign.

**Unchanged invariants:** integer range requests inclusive `[S,E]`; write and lease frontiers exclusive `[S,through)`; validated total/ETag/Last-Modified/identity encoding; no stale-generation or duplicate *logical* credit; bounded engine-owned payload independent of file size; write acknowledgement before progress; durable sync before checkpoint; pause/retry/cancel cleanup; worker and writer join before reclaim; exact size/hash before atomic no-replace/replace publication. Maintain existing `CheckpointStore` resolver and output fault hooks. Ordinary single-stream and below-threshold jobs retain behavior.

## Decisions

### D0 — Measure before optimizing; treat earlier numbers as hypotheses

Extend the current fixture, not a new benchmark framework: process-isolated H1 **and** H2 TLS fixtures, explicit server-side emitted-payload/connection counters, controlled latency/bandwidth/loss (netem where available; proxy/shaped fixture otherwise, with limitations recorded), response throttling, reset/validator changes, and a deliberate slow sink. Measure one job/one origin, many jobs/same origin, many jobs/separate origins. Matrix axes: 100 Mbps, 1 Gbps, highest repeatable local rate (10 Gbps only if credible); 0/10/50/150 ms RTT; clean/modest loss/429-503+Retry-After/reset+resume; below 16 MiB threshold/32 MiB/1 GiB/manual multi-GiB; 1/2/4/8/16 workers where allowed; tmpfs/NVMe/constrained storage. Smoke CI is a small deterministic subset; large/network shaping tests are explicit manual/nightly. Record *client-only* CPU/GiB, RSS, writer blocking thread count, metrics and hash/size/publication checks. Alternate baseline/candidate order on same host; ≥5 measured repetitions after warmup, report median, p10/p90 or MAD and confidence limits. Never call noise a gain. Report unavailable perf/syscall/flow-control probes honestly. Keep `h1`/`h2`/`prealloc` Criterion identifiers for `bench_check.sh` compatibility. Baseline SHA/commit and environment are part of every report; phase gates compare with the baseline and prior phase. Alternative: in-process-only microbenchmarks confound server/client CPU; retain only as smoke.

### D1 — Provision async capacity independently of blocking writers

Spawn up to `max_workers` *async* tasks, stable worker indices and progress/counter cells sized for max at job startup. Initially only `desired` can acquire leases; others use the existing watch `borrow_and_update`-before-check park and cancel-only wake. Decrease lets a current request reach a safe boundary (or explicitly settle it) then parks; increase wakes tasks and records actual active leases/requests, not just desired. Guard against premature `is_finished()` while writes remain unsettled. Join the adaptive controller task instead of detaching it. **Phase dependency:** fixing the adaptive allocation must not create max long-lived blocking lanes. In the temporary legacy-writer branch, lazily create a lane only for an activated worker and release/park it when inactive, with a bounded cap; the new shared writer path is the production target. An alternative is dynamic task spawning on every probe, but that complicates stable indices and joins. A pure increase of `desired_workers` is already known broken.

### D2 — Shared portable executor and byte credits

Expose an internal `WriteExecutor` boundary: submit `(job, lease id/generation, absolute offset, Bytes, length, completion)` and an output write-only capability; use a small fixed-size blocking pool shared by one `SingleStreamController`/engine context (not per worker), with fair per-job queues or round-robin admission. Do not hold a Tokio runtime worker while doing sync file writes. Keep a per-job outstanding-byte semaphore and an engine-global byte semaphore; configurable caps validated against `read_buffer_size`, and optionally a per-worker sublimit. Reserve capacity **before** polling the next payload (at least one configured frame quantum); reconcile actual frame size, split a larger frame into bounded slices or fail safely on an unsupported over-cap frame. Permits cover queued+executing `Bytes` and are released by RAII on completed/failed/discarded writes. Bounded queue slots alone are insufficient. Avoid holding a global permit while waiting for a job permit or vice versa if this creates head-of-line deadlock; use a consistent acquisition order with cancellation-aware waits. Set conservative **provisional** limits (e.g. one to two `read_buffer_size` frames per active worker, 2–4 shared blocking writers) only after D0 measures; internal flag chooses legacy/new path during rollout. A worker may continue receiving while earlier writes are pending up to its byte cap; cap worker read-ahead too, not just the job total. One `OutputWriteHandle` per active job/executor lease, not per network worker; shut down/drain executor jobs and drop all capabilities before `reclaim_exclusive`. Preserve `OutputFaultScript` at handle boundary. Portable `write_all_at` first; io_uring/overlapped APIs are possible future backends behind this boundary.

**Memory accounting:** engine-owned in-flight payload ≤ global outstanding cap; each job ≤ job cap; each worker ≤ worker read-ahead cap + at most one just-received bounded frame until reservation reconciliation. Metadata for queued writes/ack intervals scales with capped number of chunks, not file length (scheduler metadata still scales with segment count; cap ready work). Hyper internal ingress/socket/kernel buffers and hashing buffers remain explicitly outside this cap; measure peak RSS independently. A global shared pool avoids permanent thread growth with `jobs × workers`; completed jobs evict their handles/queues. Alternative per-job small executor simplifies ownership but scales with job count; benchmark it only if fair global sharing proves too costly.

### D3 — Per-lease ordered acknowledgement before any published frontier

Write completion only means the *whole* checked positional operation returned successfully (short/interrupted writes handled by `write_all_at`); it does not imply fsync. Track separate per-lease/generation frontiers `received_through`, `submitted_through`, `acknowledged_through` (interval set or bounded ordered completion map) and `published_through`. For out-of-order completions, insert success intervals, advance the contiguous exclusive frontier from lease start only while the next interval touches it, and publish one coherent `LeaseRecord` after advancement. `SegmentScheduler::absorb_worker_progress` credits only its accepted delta; never count a completion twice, and discard completions for invalidated generations. A failed earlier write blocks frontier advance; settle/cancel remaining submitted work before retrying from the acknowledged prefix, preventing a late old write from overwriting a new-generation range. Track received-but-not-admitted and failed/discarded payload as waste without conflating server-emitted vs client-received bytes. `coordinator_loop::attempt_coordinator_save` reads only this frontier and retains the sync-before-save ordering. At pause: stop ingest, drain or cancel queued writes, reconcile, save eligible coverage, then acknowledge pause. At cancellation: drain/abort deterministically then honor `CancelMode`; stop coordinator before reclaim. At finish: join workers, drain executor, stop coordinator, reclaim, verify, publish. Alternative last-completed-offset tracking is unsafe under out-of-order writes.

### D4 — Duration-aware scheduling and safe splitting

Retain `IntervalSet` + generation semantics in `scheduler/core.rs`; extend `TargetSelector` with opt-in duration mode. For each new lease, use clamped `EWMA(per-worker unique acknowledged goodput) × target duration`, initialized from existing explicit target until sufficiently stable samples; include RTT/request setup cost, max 2× step change per allocation, and minimum request quantum. Sweep candidate durations 0.5/1/1.5 s **before** choosing a default, not as a spec threshold. Prefer pending work and maintain approximately 2–3× desired workers' worth of unclaimed requests where remaining bytes permit; lazily carve from pending (not pre-owning active ranges). Split only on measured straggler conditions or insufficient pending work. **Before splitting**, reconcile progress AND the received/submitted high-watermark; only hand off an unreceived tail or cancel/drain original request before handing off the overlapping tail. If an uncooperative H1 server sends beyond a shrunk lease, account bytes received as waste and never credit/write them to the new lease. Record split-related emitted payload; clean split fixture must stay <1.10 amplification; 2× is an unconditional failure. Alternative always splitting at `next_offset` repeats the observed overlap exposure. Keep explicit/legacy automatic selectors untouched.

### D5 — Useful-goodput controller with protocol/storage gates

Build on `control/adaptive.rs::AdaptiveController` and `WindowSample` rather than replace it. Use unique accepted completion deltas, smoothed marginal gain after +1 probes, retries/waste/throttle, interval-weighted idle and actual active requests, writer queue occupancy/ack latency and budget-blocked time. Suppress probes on storage saturation even if network bytes rise. Negative pressure reduces promptly; bounded cooldown and periodic re-probe prevent permanent low-concurrency lock-in. Do not select fixed percentages before D0; capture chosen thresholds in phase report. Manual `set_concurrency` suspends auto decisions for the job (existing contract). Protocol policy reads actual negotiated H1/H2: H1 probes connections subject to permits and marginal gain; H2 probes streams on one healthy socket, respecting peer constraints where exposed; any automatic extra H2 connection is off until measured flow-control bottleneck and explicit policy permit it. `HttpTransport` already has configurable `H2ConnectionPolicy::Additional`; never interpret worker count as connection count. Missing H2 flow-control instrumentation means report unavailable and hold one socket, not guess.

### D6 — Shared-origin feedback above, not instead of, transport permits

Create an engine/controller-shared `OriginRegistry` Arc propagated through `SingleStreamController::start/run_inner` and `HttpTransport` final-URL dispatch. Canonical key is lowercased IDNA-normalized scheme + host + effective port of final URL (respect redirect); isolate proxy/TLS credential pool keys for actual physical connections using existing `http/connect.rs::origin_key`. The registry owns bounded-size weak/TTL-evicted per-origin state: request slots (fair FIFO/round robin across jobs with per-job quotas), Retry-After deadline, throttle EWMA, dynamic ceiling and recovery timer. Keep `ConnectionLimits` authoritative for physical sockets: acquire origin request slot before network dispatch, then connector's physical permit only when creating a connection; never hold a global connection permit while awaiting the origin request gate. Existing `RetryClassifier` classifies/limits attempts; 429/503 update shared deadline with policy-capped Retry-After, cancellation-aware sleep and a gradual post-cooldown probe. RAII request/connection permits release on success/error/cancel. Coordinate *all* retries of one origin without serializing unrelated origins. Bound idle entries via TTL + cap and evict only inactive entries; clear sensitive URL/userinfo. Alternative global backoff for all origins punishes innocent hosts; per-job `origin_backoff_until` remains as compatibility fallback until shared behavior passes tests.

### D7 — Secondary changes require isolated evidence

`TokenBucket` already skips its internal mutex when unlimited: **no unlimited-path rewrite**. First verify end-to-end global+job rate limiting (current segmented path only calls its job bucket); if limited-mode lock contention is measurable, evaluate local bounded token quanta without violating aggregate burst or live updates, and keep old limiter as fallback. `LeaseProgress` is SeqCst single-writer; prefer reduced *safe* publication frequency with bounded crash-lag and pause/checkpoint flush over weakening atomics. Any ordering change requires proof + Loom/equivalent and stress tests. Cache-line padding of `WorkerCounters` needs contention profile, not speculation. Sweep 64/128/256/512 KiB receive buffers after pipelining; retain 128 KiB absent material net gain (CPU/syscalls/allocations/RSS/TLS included). `BufferPool` is not on the current receive path: do not claim reuse or add copies to justify it. Compare logical versus opt-in physical allocation on tmpfs/NVMe/slow FS for latency, throughput, ENOSPC, cleanup and compatibility; keep physical off by default absent clear evidence.

### Proposed data flow

```text
H1 sockets or multiplexed H2 streams ← fair origin request gate ← shared throttle/retry state
  → up to max async workers (only desired eligible; others watch-park)
  → validated lease / rate tokens / pre-read byte credit
  → bounded received Bytes → per-job + engine byte admission → fair shared write queue
  → small shared blocking positional executor → per-write completion + permit release
  → per-lease contiguous ack frontier → coherent LeaseProgress → unique scheduler credit
  → job checkpoint coordinator (Performance ack / Durable sync then save)
  → drained writers + stopped coordinator → owner verify hash/size → atomic publish
```

### Adaptive state flow

```text
observe actual requests + unique goodput + writer pressure + origin/protocol signals
  → stable EWMA window → Hold / +1 probe / fast reduce → cooldown → re-probe
  → desired count → revision wake/park → actual active count → next observation
                        manual override ───────────────┘ (pins count)
```

### Origin coordination

```text
Job A ─┐             ┌─ final-origin A: fair slots + Retry-After + cooldown
Job B ─┴─ registry ──┤  (H1 request count; H2 stream count; socket permits separate)
Job C ───────────────└─ final-origin B: independent slots/backoff
     idle entries TTL/cap; RAII permits returned on every exit path
```

## Risks / Trade-offs

- [Out-of-order ack corrupts checkpoint] → generation-tagged interval frontier; deterministic reverse-completion, pause, crash/resume and fault-script tests; never publish later bytes across a hole.
- [Permits deadlock or leak on cancel/write error] → RAII releases, consistent admission order, bounded queue, timeout-based shutdown tests across many jobs; drain before reclaim.
- [Default change breaks callers] → keep Fixed/Explicit/Single H2/Performance/physical-off; internal feature gates for new writer, adaptive v2, duration sizing and shared-origin feedback; public additions only when needed.
- [Adaptive overfits noisy short jobs] → warmup, hysteresis, protocol gates and minimum viable observation; skip probes when job nearly done; compare fixed baseline in WAN matrix.
- [Origin state scopes redirects/proxies incorrectly or grows forever] → final-origin identity, separate physical pool keys, TTL/capped idle eviction and redirect/cancel tests.
- [Shared executor starves small jobs] → per-job fair dispatch and timed multi-job progress checks; measure aggregate latency as well as total throughput.
- [Endpoint overlap despite logically unique scheduler coverage] → track received/submitted high watermark separately; test server-emitted bytes; do not assume accepted-completed counters detect redundant network traffic.
- [CI cannot run netem/perf or fast disks] → deterministic shaped fixtures in CI, explicit manual/nightly matrix and transparent unavailable markers, no fabricated success claim.

## Migration Plan

1. Add metrics/fixture parity and pin baseline commit. Build and test legacy/new paths side by side with internal, temporary switches; establish both isolated correctness and same-host variance.
2. Fix real adaptive provisioning with a temporary lazily-allocated legacy writer branch so max async tasks do not imply max blocking lanes. Gate on observed active growth/shrink and full regression suites.
3. Introduce executor/byte admission, ack frontier, controlled H1 then H2 migration. Fail closed on write errors; retain `WriterLane` until pause/resume/retry/finalization and resource profiles match or improve. Roll back to legacy path for regressions.
4. Enable opt-in duration sizing/ready work, adaptive v2, protocol gates and shared-origin state one at a time with independent comparison reports. Legacy behavior remains selectable.
5. Test secondary improvements only where profiling justifies them. Document defaults/limitations; remove temporary switches and obsolete lanes **only after** parity, new tests and stable benchmark acceptance. Reconcile unsynced historical capability deltas before any spec sync/archive (no main specs currently exist).

## Open Questions

- Exact byte caps, writer count, duration, controller bands and origin TTL/ceilings are **measurement outputs** of D0/D2/D4/D5/D6; initial candidates are not committed defaults. The phase tasks contain the required sweeps and decision records, so these do not change the approach or acceptance contract.
