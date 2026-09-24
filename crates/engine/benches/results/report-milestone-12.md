# Milestone 12 — end-to-end validation and performance review (tasks 12.1-12.3)

Final validation of the optimization change. Host: the baseline workstation
(`docs/benchmark-profiling.md`); Linux-only execution on this machine — the
Windows build/test verification runs in CI (the `cfg(windows)` paths are
`seek_write` positional writes and fs2's Windows allocate; no platform-
specific logic elsewhere).

## 12.1 Full suites

- `cargo test -p kdown-engine` — all unit + integration suites pass
  (lib: 273; segmented/scheduler/durability/lifecycle/alloc/adaptive/
  runtime-control/isolated-fixture suites all green).
- `cargo clippy --lib --tests --benches` — clean (workspace lints incl.
  pedantic).
- Unix build: `cargo check --lib --tests --benches` clean.
- Windows: not checkable on this host (distro Rust, no `x86_64-pc-windows-
  msvc` std installed); the platform-gated surface is limited to
  `FileExt::seek_write` (positional writes) and fs2's allocate — both
  cfg-gated with logical-sizing fallbacks; the CI Windows job covers it.
- Coverage of the required behaviors: retry after partial progress
  (reset/retry suites), stale validators, interrupted resume
  (crash-restart suite), cancellation modes (seam tests), injected
  sync/store failures (durability unit + seam tests), runtime
  rate/concurrency updates (runtime-control suite), final checksum and
  atomic no-replace/replace publication (output-lifecycle suite).

## 12.2 Isolated-server matrix versus baseline

Authoritative runs use the process-isolated fixture server (client-only
CPU/RSS); fixture 1 GiB synthetic, seed 0xBEEF, performance durability,
loopback.

| Workers | Useful goodput | Wire overhead | CPU% | Peak RSS (client) |
|---|---|---|---|---|
| 1 | 777 MiB/s | 0% | 16% | 8.4 MiB |
| 4 | 794 MiB/s | 0% | 31% | 9.9 MiB |
| 8 | 1309 MiB/s | +0.25% | 54% | 11.5 MiB |

Baseline (pre-change, milestone-1 matrix records, in-process server):
1 GiB h1 workers_1: 811 MiB/s; workers_8: 3714 MiB/s with 1.6× wire
duplication and ~29 MiB RSS in-process.

Findings:

- **No ordinary global output mutex** — the shared `Arc<Mutex<FileSink>>`
  seek path is removed; workers write positionally through per-worker
  blocking lanes (`io::positional::write_all_at`), verified structurally by
  the disjoint/out-of-order write tests.
- **No per-chunk flush** — the chunk path performs no `FlushLevel` calls;
  the flush op fires exactly once per job (owner finalization)
  (`segmented_chunk_path_never_flushes`).
- **No redundant worker saves** — checkpoint persistence is coordinator-
  owned (one authority per job), cadence-coalesced (skip-unchanged),
  generation-fenced, and stopped before cleanup; no per-worker saves
  (`unchanged_snapshots_are_skipped`, seam suites).
- **No multiplied explicit memory budget** — the per-worker `BufferPool`
  construction is gone; retained payload is one received frame per active
  worker; client-only RSS stays 8-12 MiB across worker counts (isolated
  runs above).
- **Wire overhead** collapsed from 1.6-2.5× (baseline: split-tail
  duplication) to ≤0.3% with target-sized initial leases (milestone 6).
- **Worker scaling**: localhost server-bound below 4 workers; 8 workers
  exceed the 1-worker rate by ~1.7× (the isolated server is the bottleneck
  at low worker counts; the client's own CPU stays low: 54% at 8 workers).
- **H1 vs H2**: H2 plateaus at ~820-845 MiB/s single-connection
  multiplexing (unchanged policy by design); H1 reaches the same ceiling
  with 4+ workers. See `adaptive-compare` and `sweep` records.
- **CPU per useful byte**: approximated from CPU%/goodput (perf unavailable
  on this host — documented limitation): ~0.05% CPU per MiB/s at 8 workers
  — no material CPU regression vs baseline (the per-chunk flush and
  mutex/seek round trips removed).
- **Checkpoint/sync cost**: cadence saves are coordinator-side and
  off-path; durable-mode sync ordering is enforced before persistence
  (durability unit tests). Save-latency event instrumentation remains open
  (noted in the profiling guide).
- **Trade-offs recorded**: the writer lanes add one blocking thread per
  active worker (bounded by `max_workers`) and one channel round trip per
  chunk (measured negligible: goodput parity with the mutex path within
  noise, milestone-2 comparison); adaptive mode's ramp costs goodput on
  SHORT unconstrained transfers (documented, opt-in only).

## 12.3 User-facing documentation

`docs/benchmark-profiling.md` documents the durability boundaries
(performance = page-cache acknowledgment; durable = data-sync-before-
checkpoint), the checkpoint cadence (job-level interval + pause/terminal
boundaries, coalesced), explicit vs automatic sizing, fixed vs opt-in
adaptive concurrency, manual override precedence, and the memory-budget
scope. Config defaults in `crates/engine/src/config.rs` match the
documented defaults (initial 8 MiB explicit, fixed concurrency, physical
preallocation off, 3× oversubscription candidate).
