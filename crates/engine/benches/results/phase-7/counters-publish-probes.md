# Task 7.3 — WorkerCounters false-sharing and LeaseProgress publish probes (no-change evidence)

Run 2026-09-25, release build, 16 logical cores. Probes kept as `#[ignore]`
tests for re-measurement:
`metrics::counters::tests::probe_worker_counters_false_sharing`.

## WorkerCounters false sharing

Method: 16 threads × 225k counter cycles each (1 `add_network` + 1/8
`add_completed` + 1 load — the real worker pattern), one counter cell per
thread. Storage leaked (`Box::leak`) so LLVM cannot elide the atomics;
`black_box` on a sink of the loads. Harness sanity: forcing all 16 threads
onto ONE cell shows 10 ns/op (5–20× slowdown) — the probe detects sharing
when it exists.

| Layout | ns/op | Total (3.6M ops) |
|---|---|---|
| Adjacent `Vec<WorkerCounters>` (current, ~40 B stride) | 1.0 | 4.48 ms |
| `#[repr(align(64))]` padded cells | 0.7 | 2.60 ms |

The real pattern (one writer thread per cell) shows ~0.3–0.5 ns/op of
interference over perfect padding — versus a ≥ 50 µs chunk cycle, ≲ 0.001 %.
(Cross-check: a deliberately shared cell costs 10 ns/op, so the probe would
see material interference if present.)

## LeaseProgress publish frequency

`publish` runs once per received chunk (segmented.rs ~1865/~1967, before the
boundary break so owned prefixes are credited) = 6 SeqCst stores ≈ ~60 ns
per chunk, ~0.1 % of a chunk cycle. Reads are occasional (coordinator
sampling, pause/checkpoint flush), single-writer per lease (D7).

## Decision: no-change

- No padding: the measured win (~0.5 ns/counter-op) is four orders of
  magnitude below the chunk cycle; `WorkerCounters` cells stay plain.
- No publish batching: would introduce crash-lag (published offset behind
  the durable write) and force documented flush semantics for a ~0.1 %
  cycle-time saving; SeqCst single-writer publication is preserved.
- No atomic-order relaxation was considered (D7: any such change requires a
  synchronization proof + Loom model; there is no evidence to justify
  starting that work).
