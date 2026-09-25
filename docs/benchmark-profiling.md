# Benchmarking and profiling guide

Repeatable procedures for measuring the segmented download pipeline, plus the
recorded pre-optimization baseline. Every command is reproducible from this
repository; results land in `crates/engine/benches/results/`.

**What these numbers mean (and do not mean):** all recorded throughput in
this guide and in `benches/results/` is loopback/synthetic regression
evidence for the exact host, storage, and fixture listed below. It measures
the engine's efficiency, not Internet download performance. It is NOT a
promise of WAN speedup, and it does not imply segmented downloading
outperforms sequential downloading on arbitrary remote servers: real-world
outcomes depend on the server's `Range` implementation and correctness,
server/CDN throttling, round-trip latency (RTT), available bandwidth, HTTP
version (1.1 vs 2) and per-origin connection limits, the engine's own
connection caps, local disk/storage behavior, CPU capacity, and general
environment load. Treat cross-host comparisons as invalid by construction.
GitHub-hosted runners run smoke-only by design (no absolute threshold,
no cross-host comparison).

| Setting | Value |
|---|---|
| CPU | AMD Ryzen 7 9700X (8C/16T), `nproc` = 16 |
| RAM | 32 GiB (zram swap) |
| OS | CachyOS, Linux 6.2.6 kernel series |
| Storage | NVMe (`nvme0n1`, `nvme1n1`) + rotational HDD; bench destinations default to tmpfs-backed tempdirs |
| Network | Loopback (127.0.0.1) fixture servers; no RTT/loss shaping in CI runs |
| Protocol axis | HTTP/1.1 plaintext (raw TCP responder) and HTTP/2 over ALPN TLS (self-signed CA bundle) |
| Build | `cargo bench` → release profile (`lto = thin`, `codegen-units = 1`) |
| Durability | `DurabilityMode::Performance` (default) unless stated |
| Fixtures | Deterministic xorshift content (`seed 0xBEEF`); synthetic block-derived for ≥256 MiB |

Throughput values from different hosts, containers, or CPU-frequency states
are not comparable. GitHub-hosted runners run smoke-only by design (no
cross-host comparison).

## Repeatable commands

All commands run from the repository root.

### Criterion smoke scenarios (default, CI-bounded)

```sh
scripts/bench_check.sh --smoke        # scenarios only, no baseline comparison
scripts/bench_check.sh                # compare against benches/results/baseline.md
```

Runs the criterion groups `h1`, `h2`, `prealloc` (32 MiB fixture,
workers 1/4, preallocation on/off) with a 0.5 s warm-up and 1.5 s
measurement budget. Raw criterion medians are the regression-check input.

### Smoke matrix (32 MiB / 256 MiB / 1 GiB × H1/H2 × 1/2/4/8 workers)

```sh
cargo bench --bench throughput -- --matrix-smoke
```

One measured (non-criterion) run per scenario: 24 scenarios, single-digit
seconds total, no multi-GiB allocations (synthetic block-derived content).
Writes `benches/results/matrix-smoke/records.md` and prints the same table to
stderr. Every row is verified for final size, SHA-256, and atomic publication.

### Manual matrix (multi-GiB / 16 workers — workstation only)

```sh
cargo bench --bench throughput -- --matrix-manual
```

2 GiB and 4 GiB × H1/H2 × 1/2/4/8/16 workers. Never run in CI (allocation
and time). Writes `benches/results/matrix-manual/records.md`.

### Process-isolated authoritative runs (client-only resources)

The fixture server is a separate process, so client CPU/RSS/context-switch
records exclude server cost:

```sh
cargo build --release -p kdown-engine --bin fixture_server
target/release/fixture_server --size 1GiB --seed 0xBEEF --addr 127.0.0.1:0 &
# read LISTENING <addr> from its stdout, then:
cargo bench --bench throughput -- --isolated 127.0.0.1:<port> \
    --isolated-size 1GiB --isolated-seed 0xBEEF \
    [--isolated-workers 4] [--isolated-label mylabel]
# → benches/results/isolated/mylabel/records.md
```

Server behavior controls: `--ignore-ranges`, `--throttle-mib-s F`,
`--transient-fail N:CODE` (e.g. `2:503`), `--reset-after-bytes N`,
`--change-etag-after N`. Startup protocol: `LISTENING`, `SIZE`, `SHA256`,
`READY`. The isolated server is plaintext HTTP/1.1; H2 multiplexing stays
covered by the in-process smoke scenarios.

## Profiling procedures

### Always available (no extra tooling; used by the harness)

- **CPU time / CPU %** — `/proc/self/stat` utime+stime delta around each run
  (100 Hz tick granularity).
- **Peak RSS** — `/proc/self/status` `VmHWM`.
- **Context switches** — `/proc/self/status`
  `voluntary_ctxt_switches + nonvoluntary_ctxt_switches` delta around each
  run. LIMITATION: these counters track the calling (main) thread only, not
  the process or its worker threads — multithreaded runs report near-zero
  deltas. Use `vmstat 1` (system-wide `cs`) or `pidstat -t` (when
  installed) for real switch visibility; recorded per-thread switch counts
  are therefore marked n/r where meaningless.
- **Wire vs useful bytes** — engine `JobCounters` folded into
  `DownloadResult` (`bytes_downloaded_from_network`, `completed_bytes`,
  `wasted_bytes`, `retries`); goodput and wire throughput are reported
  separately per scenario.
- **System-wide context-switch pressure** — `vmstat 1` in a second terminal
  during manual runs (`cs` column) when per-process deltas are not enough.

### Requires tooling not installed on the baseline host (documented limitation)

`perf`, `pidstat`, and `strace` are **not installed** on the baseline host,
so cycles/instructions per useful byte, per-syscall counts, and per-thread
CPU breakdowns are **not part of the recorded baseline**. When available,
the following reproduce the missing views:

```sh
# CPU cycles/instructions per useful byte (isolated server strongly advised)
perf stat -e cycles,instructions,context-switches,cs \
    -p "$(pgrep -f 'throughput --isolated')" -- sleep 30
# Per-thread CPU and paging during a run
pidstat -t -p "$(pgrep -f 'throughput --isolated')" 1
# Syscall counts and write sizes on the output path
strace -c -f -e trace=write,pwrite64,pwritev2,fsync,fdatasync,ftruncate \
    target/release/deps/throughput-* --isolated 127.0.0.1:PORT --isolated-size 1GiB
```

Until those tools are installed, syscall counts are reported as
"unavailable" and CPU cost per useful byte is approximated by the recorded
CPU% per goodput (record rows), which is sufficient to detect order-of-
magnitude regressions.

### Checkpoint / write latency status

Dedicated checkpoint-save and fsync latency instrumentation is **not yet in
place**; it lands with the job-level checkpoint coordinator (milestone 4 of
the optimization change). Until then:

- Durable-mode runs pay `sync_all` per checkpoint interval inside worker
  chunk paths; the cost is visible indirectly as goodput loss vs the
  performance-mode record under identical conditions.
- Post-coordinator, per-save latency and per-interval write latency will be
  reported through job events and added to this document.

## Recorded pre-optimization baseline (2026-09-24)

Harness corrections included (task 1.1-1.4): real counter accounting,
case-insensitive Range parsing in the raw H1 responder, size/hash/publication
verification per run.

### Criterion medians (32 MiB, 0.5 s warm-up, 1.5 s measurement)

| Scenario | Median | Throughput |
|---|---|---|
| h1/workers_1 | 8.87 ms | ~3.6 GiB/s |
| h1/workers_4 | 8.89 ms | ~3.6 GiB/s |
| h2/workers_1 | 15.64 ms | ~2.0 GiB/s |
| h2/workers_4 | 16.13 ms | ~2.0 GiB/s |
| prealloc_true | 18.19 ms | ~1.8 GiB/s |
| prealloc_false | 17.59 ms | ~1.8 GiB/s |

### Smoke-matrix resource records (highlights)

Full tables: `benches/results/matrix-smoke/records.md`.

- **h1 workers_1** is a clean single stream: network == completed == fixture
  size at every size (32 MiB → 798 MiB/s, 1 GiB → 811 MiB/s; loopback-bound).
- **h1 segmented (workers ≥ 2) re-downloads overlapping ranges**: network
  bytes exceed the fixture (32 MiB × 2 workers → 50 MiB; × 4 → 67 MiB;
  256 MiB × 8 → 432 MiB = 1.6×). Cause: idle workers split a live lease's
  tail while the split lease's worker keeps streaming its original request;
  the pre-change counters also re-count re-acknowledged prefix bytes as
  unique completed (retry accounting is corrected in milestone 5.4). This
  waste is visible now because byte fields come from real counters.
- **h2 segmented stays exact** (network == fixture) at every size/worker
  count: multiplexed streams finish fast enough that idle-split duplication
  does not trigger, but goodput plateaus at ~820-845 MiB/s regardless of
  workers — the H2 single-connection path does not scale with range workers
  on loopback.
- CPU% scales with duplicated work (h1/256MiB/workers_8: 611% CPU for
  1.6× the file).
- Peak RSS grows with worker count (9.4 → 29 MiB across the matrix) — the
  pre-change per-worker `BufferPool` allocation contributes; removed in
  milestone 10.

### Pre-optimization findings this baseline pins

1. Segmented output serializes on one mutex-protected sink (see proposal);
   h1 goodput plateaus despite parallel workers.
2. Idle-worker tail splits duplicate network traffic on H1 (up to 1.6× wire
   bytes at 8 workers) without inflating `retries`/`wasted` counters.
3. Per-chunk `PageCache` flush + synchronous checkpoint saves sit on the
   chunk path (measured via goodput delta between durability modes in
   milestone-2.4 comparisons).
4. Worker-count scaling on H2 is flat (~820-845 MiB/s from 1 to 8 workers).

Milestone 2.4 re-runs this exact matrix after the positional-output change
and records the comparison in `benches/results/comparison-milestone-2.md`.

## Memory-path scope (milestone 10)

- **One frame per worker, no staging** (task 10.3, direct path): each
  segmented worker holds at most ONE received Hyper `Bytes` frame in flight
  (submitted to its writer lane, acknowledged before the next read).
  There is no staging queue, no pooled-buffer copy, and no per-worker full
  budget: retained payload = one frame per active worker (≤
  `read_buffer_size` each, typically 128 KiB × `max_workers`).
- **`buffer_pool_max_bytes` bounds the pool, not Hyper**: the engine's
  transfer path no longer constructs `BufferPool`s (removed, task 10.1);
  the pool remains public for embedding callers and bounds ONLY its own
  buffers. Hyper's internal ingress buffers and socket receive windows are
  outside any explicit engine budget — peak RSS may transiently exceed
  `read_buffer_size × workers` because of them (documented; no absolute
  process-RSS cap is promised).
- **RSS/worker scaling** (measured, h1/32 MiB matrix, this host):
  workers_1 → workers_8 peak RSS went 9.7 → 20.1 MiB after the per-worker
  pool removal (was 9.4 → 29 MiB before) — roughly +1.3 MiB per extra
  worker (task + lane + buffers), scaling linearly with the worker count.

## Optimize-transfer-engine-v2 phase-0 baseline record

Recorded 2026-09-24 at commit `cc6b4b6` (HEAD when the change started), before
any behavior change from this second optimization change. All comparisons for
`optimize-transfer-engine-v2` phases run against this record on the same host.

### Configuration defaults in force (verified against `config.rs`)

| Field | Default |
|---|---|
| `transfer.max_workers` / `min_workers` | 8 / 1 |
| `transfer.initial_segment_size` | 8 MiB (`SegmentSizing::Explicit` honors it) |
| `transfer.min_segment_size` / `max_segment_size` | 1 MiB / 64 MiB |
| `transfer.segment_sizing` | `Explicit` |
| `transfer.auto_oversubscription` | 3 |
| `transfer.concurrency_mode` | `Fixed` (adaptive is opt-in, starts at `min_workers`) |
| `transfer.segmentation_threshold` | 16 MiB |
| `transfer.preallocate_output` / `preallocate_physical` | true / false |
| `transfer.durability` | `Performance` |
| `transfer.verify_range_support` | true |
| `retry.max_attempts_per_segment` | 8 (base 250 ms, ×2, honor `Retry-After` capped 120 s) |
| `buffer_pool_max_bytes` | bounds the pool only (see memory-path scope above) |

### Invariants this change must preserve (authoritative today)

- Scheduler interval ownership: normalized, non-overlapping
  pending/active/completed coverage; stale generations rejected
  (`scheduler/core.rs`).
- Write acknowledgement boundary: progress/counters advance only after a
  positional write acks (legacy `WriterLane` per worker, or the shared
  `WriteExecutor` completion when `write_executor.pipeline_writes` is on).
- Checkpoint coordinator owns all saves; durable mode syncs data before
  `store.save_atomic`; failed sync prevents any save; generation-fence recheck.
- Output lifecycle: exclusive reclaim before verify/publish; atomic
  publication; owner-only integrity verification.
- Accounting: `completed` = scheduler-accepted unique deltas; `network`
  counted at receipt; `wasted` = received-but-unacked gap on retry.
- HTTP validation: `Content-Range`/length/generation/identity checks,
  retry classification, redirect/credential rules unchanged.

### Reconciliation: split exclusion claim vs `split_tail` reality

The historical `download-engine-v1` `transfer-core` delta requires a dynamic
split to exclude "bytes already read or queued for write by the original
worker". The implemented `SegmentScheduler::split_tail` splits at the lease's
acknowledged **write** frontier (`next_offset`, advanced only by lane write
acks). Bytes the original worker has **received but not yet written**, and
everything it continues to stream toward its stale lease end, are not
excluded: the split lease re-requests them while the original request keeps
downloading them. Writes stay positionally correct (the file is exact), but
the wire payload is duplicated.

Measured by the focused reproducer
(`crates/engine/tests/wire_amplification_tests.rs`, whole-file lease +
4 workers + paced server, 8 MiB): server-emitted payload 16,777,217 bytes for
8,388,608 accepted bytes — **2.000× wire amplification**. The reproducer is
`#[ignore]`d while this is the current behavior; task 3.6 (split-eligibility
fix) unignores it and tightens the bound to <1.10× with 2× remaining an
unconditional failure. Default-size leases (8 MiB target) keep live-tail
duplication under ~1% on loopback; the pathological case above is the guard
for the scheduler rework.

### Controlled shaping modes (task 0.3) and axis availability

The isolated fixture server now supports `--rtt-ms F` (one RTT before each
response's headers — a lower bound on real RTT), `--loss-percent P`
(deterministic, seed-derived per-response connection truncation), and
`--retry-after SECS` for transient-fail responses. Calibration is verified by
`fixture_isolated_tests` (3 RTTs ≥ 3×40 ms across a three-segment transfer;
loss recovers through retries; Retry-After value observed on the wire).

The bench `--jobs-smoke` mode measures one-job / same-origin / multi-origin
contention in-process (H1+H2, per-job and aggregate rows). The isolated
client accepts `--isolated-dest <dir>` to point the output at real storage
(tmpfs, NVMe, HDD) — storage axes are exercised by choosing destinations, not
simulated.

Explicitly unavailable on this host: kernel-level netem (no `tc`/root netem
configuration), so RTT/loss emulation is server-side approximation only;
`perf`/`pidstat`/`strace` remain unavailable (see limitations above). WAN
numbers from the shaping approximation are comparative (same-host, same
approximation on both sides of an A/B), not absolute network truth.

### Engine-side gauges (task 0.4)

- `DownloadResult::wire_amplification()` — received payload (network +
  re-received waste) / unique completed bytes; `None` when nothing uniquely
  completed (undefined, never fabricated). This engine counts each wire byte
  once in `bytes_downloaded_from_network` and charges duplicates to
  `wasted_bytes`, so their sum is what crossed the wire.
- `SegmentedJob::split_count()` — live-tail split events.
- `SegmentedJob::active_workers()` — workers actually holding a lease
  (distinct from `desired_workers`).
- `SegmentedJob::last_checkpoint_save_us()` — last coordinator save latency.
- Bench harness records CPU%/CPU-per-GiB, peak RSS, context switches and the
  isolated server's emitted/connections/requests via `/__stats`.

Explicitly unavailable (labeled, not fabricated): H2 per-stream counts and
flow-control stall timing (phase 5), writer queue depth/ack-wait percentiles
(phase 2's executor), scheduler lock-wait timing, syscall counts and
allocation profiles (no `perf`/`strace` on this host). Single-stream
checkpoints do not yet record save latency (segmented coordinator only).

### Phase-0 gate (optimize-transfer-engine-v2, recorded 2026-09-24)

- `cargo test -p kdown-engine`: 30 suites, all `test result: ok`, 0 failures
  (includes the new wire-amplification reproducer (ignored, task 3.6),
  12 fixture-isolated tests, 4 shaping tests, 11 runtime-control tests).
- `cargo clippy -p kdown-engine --all-targets`: clean.
- `scripts/bench_check.sh --smoke`: exit 0; all scenario verifications ok.
- Baseline variance: recorded in `benches/results/baseline-v2-core.md`
  (1 GiB rows 1–11% spread; 32 MiB rows noise-dominated) and
  `benches/results/baseline-v2-wan.md`. The criterion prealloc group still
  shows 1.6–1.7× wire amplification (whole-file-lease split pattern) — the
  phase-3 gate (<1.10 on the clean split fixture, 2× unconditional failure)
  applies to both the reproducer and these groups.

## Knob taxonomy, rollback switches and future options (task 9.5)

**Stable (documented for ordinary callers):** everything under
`EngineConfig::transfer` documented in the README table, `network.*`
timeouts and per-job `rate_limit`, `global_rate_limit`, TLS/proxy/SSRF
settings, retry policy, and the public result/metrics types.

**Advanced (opt-in, measured):** `write_executor.pipeline_writes` (shared
bounded executor: flat +8 writer threads vs +24 at 16 workers, at a small
single-job H1@4 cost), `transfer.concurrency_mode = Adaptive`,
`segment_sizing = Automatic | Duration`, `h2_policy = Additional`,
`preallocate_physical`, `OriginRegistry` sharing.

**Internal (not part of the compatibility surface):** the legacy
`WriterLane` vs pipelined switch and its test aliases, registry
`with_limits`, controller `with_origin_registry` /
`with_global_rate_bucket`, `H2_FLOW_CONTROL_INSTRUMENTED`, checkpoint
format internals.

**Rollback switches (each gate keeps its fallback selectable):**
- Write path: `write_executor.pipeline_writes = false` restores per-worker
  lanes (phase-2 gate decision; `WriterLane` is retained deliberately as the
  rollback path, not dead code).
- Adaptive concurrency: `ConcurrencyMode::Fixed`.
- Segment sizing: `SegmentSizing::Explicit`.
- H2 extra sockets: `H2ConnectionPolicy::Single` (default).
- Shared-origin feedback:
  `DownloadController::with_origin_registry(OriginRegistry::disabled())`
  restores per-job backoff only (phase-6 gate).
- Physical preallocation: `preallocate_physical = false` (default, phase-8).

**Known future options (not started, recorded for planning):** HTTP/3
(QUIC) transport behind the same `HttpExecution` seam; io_uring (Linux) /
IOCP (Windows) positional write backends behind `WriteExecutor`; PGO/BOLT
profiles for the release CLI; dynamic per-origin request ceilings in
`OriginRegistry` (phase-6 report); flow-control instrumentation if a hyper
upgrade exposes it.
