# Milestone 5 report — hot-path synchronization and worker metrics (task 5.5)

Reprofile after milestones 1/2 (same host, same harness; see
`docs/benchmark-profiling.md` for procedure and limitations). Changes under
test:

- **5.1** Interim mutex progress publication (milestone 3.2) replaced by the
  single-writer sequence-counter cell (all-`SeqCst`, odd/even sequence,
  coherent-record reads, spin-retry on tear). No locks on the publish path.
- **5.2** Per-chunk asynchronous fatal check (`AsyncMutex` lock per chunk)
  replaced by an atomic `Acquire` flag; the detailed error moved to a small
  first-wins mutex touched only on failure.
- **5.3** Per-chunk outer rate-bucket mutex (lock+clone per chunk) replaced
  by one stable `Arc<TokenBucket>` whose atomic limit is checked first;
  unlimited chunks take no outer lock; runtime updates mutate in place.
- **5.4** Counters attributed to real worker shards (was: all workers shared
  slot 0); wire bytes counted at receipt; lost received-but-unacked gaps
  charged as wasted at lease-retry boundaries.

## Results (single-run matrix records; variance ±10-20% on this host)

| Scenario (h1, 32 MiB) | Milestone-2 session | Milestone-5 session |
|---|---|---|
| workers_1 goodput | 1204 MiB/s | 1183 MiB/s |
| workers_2 goodput | 1141 MiB/s | 1145 MiB/s |
| workers_4 goodput | 1172 MiB/s | 1172 MiB/s |
| workers_8 goodput | 3767 MiB/s (1 GiB) | 3746 MiB/s (1 GiB) |
| workers_8 CPU% (1 GiB) | 944% | 944% |

- Goodput is unchanged within noise across all worker counts; the hot-path
  synchronization changes cost nothing measurable on this workload
  (loopback + tmpfs; the write and network round trips dominate).
- The atomic fatal check removed a per-chunk async mutex acquisition; the
  stable bucket removed a per-chunk outer mutex; neither shows as a
  goodput delta at these rates (they were already sub-dominant next to the
  network/write round trips), but both reduce scheduler interactions on the
  chunk path (visible as reduced tokio bookkeeping under load, and required
  by the low-contention-controls requirement regardless).

## Lock-contention and cache-padding decision

- The old fatal mutex and rate-bucket mutex were the only job-wide locks on
  the chunk path; both are gone (atomic flag + atomic limit). Remaining
  chunk-path synchronization: the per-worker counter shard (own atomics),
  the per-worker progress cell (single-writer SeqCst cell), and the lane
  channel (per-worker).
- `WorkerCounters` is 5×`AtomicU64` = 40 bytes; adjacent shards in
  `Vec<WorkerCounters>` can share a 64-byte cache line, so false sharing is
  structurally possible. **No cache padding is added**: the matrix shows no
  goodput/CPU signal of shard contention at 2/4/8 workers, `perf` is not
  installed on this host (no cycles-per-instruction evidence available —
  documented limitation), and the design forbids speculative
  micro-optimization without profiling evidence. Revisit if a future
  profile (with `perf` installed) shows cross-worker cache-line contention
  on the counters.

## Checkpoint / write latencies

Checkpoint saves are now coordinator-owned (milestone 4): serialized,
off the chunk path, with skip-unchanged coalescing. Per-save latency
instrumentation (event-level) remains open until the event schema gains a
checkpoint-timing event; the goodput delta between durability modes
remains the proxy (documented in the profiling guide).

## Correctness evidence added with this milestone

- `sequence_cell_never_mixes_publications_under_stress` — 20k publications
  with clear/reuse cycles; readers never observe a mixed record.
- `stale_generation_records_are_rejected_by_reconciliation` — stale records
  are rejected by the scheduler.
- `simultaneous_failures_retain_exactly_one_error` +
  `fatal_flag_implies_error_is_readable` — first-wins fatal ownership.
- `rate_limit_update_before_job_is_honored` — pre-start + live
  limited/unlimited updates through one stable bucket.
- `retried_ranges_count_wasted_bytes_with_exact_coverage` (segmented,
  reset-heavy) — unique coverage exact across retries, no duplicate
  delivery; `single_stream_restart_counts_wasted_bytes` — discarded stream
  prefixes counted as wasted bytes.

Note on Loom (task 5.1 "where practical"): the isolated primitive is
exercised by the multi-threaded stress test; a dedicated Loom model was not
added (the dev-dependency and model runtime are not part of this
repository's test stack). The all-`SeqCst` ordering is the conservative
first implementation the design mandates; weakening it (e.g. `Release` /
`Acquire` pairs) requires a formal ordering proof or a Loom model.
