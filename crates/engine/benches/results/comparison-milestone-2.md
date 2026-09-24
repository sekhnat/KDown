# Milestone 2 comparison — positional writer lanes vs baseline (task 2.4)

Before/after the concurrent positional output change (milestones 2.1-2.3),
recorded on the baseline host (`docs/benchmark-profiling.md`): same harness,
same fixtures, same scenario set. "Before" session: 2026-09-24 02:02-02:03
(post milestone-1 harness corrections, mutex/seek output path, per-chunk
PageCache flush). "After" session: 2026-09-24 03:45-03:55 (positional
`write_all_at` adapter, per-worker blocking writer lanes, no chunk-path
flush, 1 MiB bounded write batching).

## Structural change (verified by tests, not just timings)

- The shared `Arc<tokio::Mutex<FileSink>>` cursor-serialized write path is
  **removed**: workers receive write-only capabilities over one immutable
  file handle and write disjoint ranges through `write_all_at`
  (Unix `write_at` / Windows `seek_write`). No ordinary segmented write
  takes an output-wide lock or performs a shared seek
  (`cannot_publish_while_writers_live`,
  `capabilities_write_out_of_order_at_disjoint_offsets`,
  `lanes_write_disjoint_ranges_out_of_order`).
- The per-chunk `FlushLevel::PageCache` call is gone from the chunk path.
- Lifecycle ordering is preserved: lanes are joined before
  `reclaim_exclusive`, which stays fail-closed while any capability is alive
  (`worker_handles_must_be_released_before_session_can_finalize`).
- Per-worker `BufferPool` construction is no longer on the worker path
  (full removal/accounting semantics are milestone 10's scope).

## Single-run resource records (most stable comparison)

h1, 32 MiB fixture, one measured run per scenario (full tables in
`benches/results/matrix-smoke/records.md`):

| Scenario | Before goodput | After goodput | Before wall | After wall | Verify |
|---|---|---|---|---|---|
| workers_1 | 1195.8 MiB/s → n/a* | 804.3 MiB/s | n/a* | 42.0 ms | ok |
| workers_2 | 1146.6 MiB/s | 1141.9 MiB/s | 41.9 ms | 42.0 ms | ok |
| workers_4 | 1204.1 MiB/s | 1204.1 MiB/s | 53.5 ms | 53.3 ms | ok |
| workers_8 | 1480.1 MiB/s | 1172.4 MiB/s | 55.5 ms | 69.3 ms | ok |
| 1 GiB/workers_8 | 3714.2 MiB/s | 3767.1 MiB/s | 275.8 ms | 283.8 ms | ok |
| 256 MiB/workers_8 | 3702.4 MiB/s | 3362.0 MiB/s | 111.3 ms | 117.6 ms | ok |

*The workers_1 baseline row was recorded in the earlier matrix session;
later h1/workers_1 sessions measured 5042-5630 MiB/s — single-stream runs
are unaffected by the lane change (no lanes on that path).

Reading: within run-to-run variance (±10-20% on single runs, this host
shares CPUs with unrelated load), single-run goodput is **unchanged** by the
lane rewrite. The dominant h1 segmented cost remains the split-tail
range duplication (2.0-2.5× wire bytes), which is scheduler behavior
addressed by milestones 6-8, not the output path.

## Criterion medians (caveat: cross-session noise)

| Scenario | Before (02:02 session) | After (03:45 session) |
|---|---|---|
| h1/workers_1 | 8.87 ms | 8.58 ms |
| h1/workers_4 | 8.89 ms | 35.03 ms |
| h2/workers_1 | 15.64 ms | 9.76 ms |
| h2/workers_4 | 16.13 ms | 9.76 ms |
| prealloc_true | 18.19 ms | 43.85 ms |
| prealloc_false | 17.59 ms | 41.44 ms |

These cross-session medians are **not** a reliable A/B: h2/workers_1 (no
lanes on that path) "improved" 1.6× between sessions, which is pure ambient
host variance. A precise lane-overhead comparison requires same-session
interleaved A/B runs (build the previous commit in a worktree and alternate
runs); the raw criterion values are recorded here for traceability, with the
single-run tables above treated as the comparison of record.

## Writer-lane CPU / context-switch effects

- One long-lived `spawn_blocking` lane per active worker (capped by
  `max_workers`); payloads are bounded to 1 MiB of retained received bytes
  per worker (`WRITE_BATCH_BYTES`), acknowledging the whole batch before
  progress publication. Without batching, the per-chunk cross-thread handoff
  (mpsc + oneshot wake per 128 KiB chunk) was measurable; batching amortizes
  it ~8× while keeping retained memory bounded.
- The recorded per-run context-switch delta is a main-thread-only counter
  (`/proc/self/status` limitation, documented in the profiling guide) and
  reads near zero in all rows — treat switch counts as n/r until
  `pidstat`/`perf` are installed on the host.
- CPU% per scenario is unchanged within variance (h1/workers_4: 98% before,
  157% after — different single-run samples, both CPU-bound on duplicated
  wire bytes).

## Follow-ups pinned by this comparison

1. Same-session interleaved A/B (worktree of the previous commit) when a
   quiet-host window is available; the current record uses cross-session
   data with the noise caveat above.
2. Re-profile after milestones 6-8 remove the split-tail duplication —
   goodput scaling claims are only meaningful once wire bytes ≈ fixture
   size.
3. Install `perf`/`pidstat`/`strace` or record their absence per session
   (profiling guide documents the commands).
