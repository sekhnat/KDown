# Optimize-transfer-engine-v2 — phase 2 gate (task 2.8)

Recorded 2026-09-24 at the phase-2 completion commit (working tree on top of
`66c2f24`). Baseline reference: `baseline-v2-core.md` / `baseline-v2-wan.md`,
phase-1 gate: `report-phase-1.md`. Host: AMD Ryzen 7 9700X (8C/16T), 32 GiB,
CachyOS Linux 6.2.6; loopback fixtures; `/tmp` is tmpfs; `/mnt/1T` is NVMe
btrfs (93% full — 128 MiB fixtures used there).

## Correctness gates

- `cargo test -p kdown-engine`: 30 suites, all `test result: ok`, 0 failures
  (315 lib tests incl. the new write-path/pipeline suites + integration
  suites incl. the new H2 pipelined and crash-restart pipelined variants).
- `cargo clippy -p kdown-engine --all-targets -- -D warnings`: clean.
- Every benchmark row below is verification-gated (`published + size_ok +
  hash_ok` all true; rows failing verification abort the run).

## New benchmark axes (this phase)

- `throughput --write-path-compare` — in-process H1/H2 × shaped/unshaped ×
  legacy-lanes/pipelined-executor, 3 repetitions (256 MiB unshaped, 64 MiB
  shaped), records → `benches/results/write-path-compare/records.md`.
- `throughput --isolated … [--isolated-pipeline]` — process-isolated
  client-only CPU/RSS/threads; new `Threads Δ` column (process thread-count
  delta around the run: legacy lanes shut down, the pipelined pool persists).
- `throughput --jobs-pipeline` — the task-0.3 multi-job harness with the
  pipelined write path (records → `benches/results/jobs-pipeline/`).

## Write-path comparison (in-process, medians of 3; wire==completed, 0 waste, all ok)

| Scenario | legacy goodput MiB/s | pipelined goodput MiB/s | Δ |
|---|---|---|---|
| h1/unshaped (256 MiB, 4 workers) | 1850 (1792–2449) | 1862 (1813–1868) | +0.6% |
| h1/shaped (64 MiB, 4 workers) | 48.1 | 48.1 | 0 |
| h2/unshaped (256 MiB, 4 workers) | 686 (681–697) | 760 (691–785) | +10.8% |
| h2/shaped (64 MiB, 4 workers) | 12.06 | 12.06 | 0 |

Shaped scenarios are network-bound: identical to the digit. Unshaped H2 shows
a pipelined median ~11% higher with overlapping rep ranges (legacy 681–697,
pipelined 691–785) — directional, not claimed as a gain. Unshaped H1 is at
parity (medians 1850 vs 1862; the legacy rep1 2449 outlier is loopback
variance, same shape as the phase-0 baseline dispersion).

## Process-isolated H1 (authoritative client-only resources; 1 GiB, tmpfs, 3 reps)

| Workers | legacy goodput MiB/s (med) | pipelined goodput MiB/s (med) | legacy CPU% / RSS | pipelined CPU% / RSS | legacy Threads Δ | pipelined Threads Δ |
|---|---|---|---|---|---|---|
| 1 | 715 (714–721) | 722 (720–749) | 16.7–17.5% / ~10.4 MiB | 17.6–18.3% / ~10.3 MiB | +8 | +8 |
| 4 | 668 (654–673) | 646 (633–661) | 30.6–31.5% / ~11.7 MiB | 35.9–38.7% / ~11.9 MiB | +12 | +8 |
| 16 | 1299 (1273–1339) | 1306 (1222–1310) | 71–73% / ~14.4 MiB | 83–100% / ~15.7 MiB | +24 | +8 |

- **Thread/job scaling (the structural result):** the legacy process retains
  threads proportional to workers (+8 → +12 → +24 for 1/4/16 workers); the
  pipelined path is flat at +8 (4 dedicated writer threads + runtime
  baseline) regardless of worker count — `jobs × workers` no longer buys
  blocking filesystem threads (design D1/D2 verified in production shape).
- **Goodput:** parity at 1 and 16 workers; at 4 workers the pipelined path is
  ~3% slower with ~7pp more client CPU. This is the per-chunk
  submit/complete overhead against a single fast sink; it is within the
  dispersion of the unshaped loopback axis and does not grow with worker
  count (16 workers: parity).
- **Wire amplification:** 1.000 on every row (server-emitted == received ==
  completed; zero waste) — no duplication introduced by pipelining.

## Storage axis (isolated H1, 128 MiB, NVMe btrfs /mnt/1T, 4 workers, 3 reps)

| Path | goodput MiB/s (med) | CPU% | Threads Δ |
|---|---|---|---|
| legacy | 485 (484–600) | 26.5–32.8% | +12 |
| pipelined | 512 (484–586) | 30.2–41.2% | +8 |

Parity within dispersion (ranges overlap: 484–600 vs 484–586). The NVMe axis
is server/loopback-bound at this shape (matches the phase-0 finding), so it
does not separate the paths; no regression is visible.

## Multi-job contention (in-process, 4 concurrent jobs × 64 MiB, aggregate goodput)

| Scenario | legacy | pipelined |
|---|---|---|
| h1/same-origin AGGREGATE | 1771 MiB/s | 1852 MiB/s |
| h1/multi-origin AGGREGATE | 1833 MiB/s | 1811 MiB/s |
| h2/same-origin AGGREGATE | 1546 MiB/s | 1537 MiB/s |
| h2/multi-origin AGGREGATE | 1523 MiB/s | 1538 MiB/s |

Aggregates at parity (±0.5–4.6%, single measured batch per mode — the
harness records one run per configuration; per-job rows all verified `ok`
with zero waste). The in-process one-job rows show high loopback variance
(legacy 2618 vs pipelined 954 MiB/s on single runs) — the process-isolated
single-job numbers above are the authoritative single-job comparison.

## Queued bytes and acknowledgement latency (observability status)

- **Queued bytes:** bounded by construction and unit-verified — the
  executor-wide queued+executing cap (validated ≥ one frame quantum) and the
  global/job/per-worker byte budgets with exact-capacity, cancellation and
  failure-release tests (`io::write_budget`, `io::write_executor`). The
  per-run aggregate queued-byte figure is **not yet exported** through the
  benchmark record (engine-internal gauge); labeled unavailable here rather
  than fabricated, per the observability spec.
- **Acknowledgement latency:** sampled per write into the job's
  `write_latency_us` gauge (consumed by the phase-4 controller); not yet part
  of the benchmark record. Same availability note as above.

## Gate decision (task 2.8)

- **Legacy writer-lane path remains the production default**
  (`write_executor.pipeline_writes = false`). The pipelined path shows
  goodput parity at 1/16 workers, on H2, on NVMe storage and in multi-job
  aggregates, but a small single-job regression at 4 workers on H1
  (~3% goodput, ~7pp CPU) that is not yet defensible as a default change.
- **`WriterLane` is retained** as the rollback path exactly as the task
  requires; the internal switch stays, and phases 3–4 (duration sizing,
  controller v2) tune the pipelined path — the bounded writer-thread result
  (+8 flat vs +24 at 16 workers) is the structural benefit this phase was
  designed to establish.
- No public default changed in this phase (engine-api compatibility
  requirement satisfied; all new knobs are opt-in).
