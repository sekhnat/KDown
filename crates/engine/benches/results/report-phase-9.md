# Optimize-transfer-engine-v2 — phase 9 gate (tasks 9.1–9.6)

Recorded 2026-09-25, optimized working tree vs the change baseline commit
`cc6b4b6` (recorded in `benches/results/baseline-v2-core.md`, same host:
AMD Ryzen 7 9700X 8C/16T, CachyOS 6.2.6, loopback fixtures, tmpfs
destinations).

## 9.1/9.2 — correctness matrices (all suites green)

`cargo test -p kdown-engine`: **592 passed / 0 failed across 32 suites**.
Per-suite breakdown (direct binary runs, 22 integration suites = 155 e2e
tests):

- Single-stream/small files, segmented H1/H2, boundary/tail splits:
  `single_stream_tests` 11, `segmented_tests` 12, `mode_parity_tests` 2,
  `test_server_integration` 9.
- Range-ignore / malformed `Content-Range` / 200-to-range / unexpected
  status / EOF / validator changes: `range_validation_tests` 8,
  `semantic`-coverage in `execution_conformance_tests` 9 +
  `transport_integration` 12, `fixture_isolated_tests` 17 (incl. ignore-
  ranges fallback), `h2_tests` 7.
- Retryable/non-retryable classification and exhaustion, resume after
  process restart: `crash_restart_tests` 10, `resume_tests` 10,
  `checkpoint_seam_tests` 23.
- Queued-write pause/cancel/retry, write failure/permission (ENOSPC class),
  checksum success/failure, destination collision/replace/no-replace,
  partial cleanup, multi-job independent destinations:
  `handle_control_tests` 5, `phase3_exit_tests` 4, `allocation_tests` 4,
  `security_tests` 9, `origin_coordination_tests` 5, `rate_limit_tests` 5.
- Wire accounting: `wire_amplification_tests` 4 (no ≥2× double-transfer);
  connection/pool limits: `connection_pool_tests` 3.

"Assert no write or save after publication": publication happens once, after
drain and verify; the checkpoint/seam suites assert no post-publication
saves (durable/performance modes covered in `checkpoint_seam_tests`).

## 9.3 — randomized properties and concurrency stress

- `scheduler_property_tests.rs` (proptest): 4 passed — randomized
  scheduler/ack-generation properties with a regressions file for
  reproducers.
- `randomized_disconnect_tests.rs`: 2 passed (random disconnect/resume).
- Concurrency stress: lost wakeups, writer permit release, shutdown
  timeouts and origin fairness/capacity accounting are covered by
  `runtime_control_tests` (20), `origin_coordination_tests` (5),
  `connection_pool_tests` (3) — all green.
- **Loom: not required** — no atomic-ordering change shipped (task 7.3 kept
  SeqCst single-writer publication; the TokenBucket balance fix is
  mutex-protected, no ordering change).
- **Sanitizers: not run on this host** (no ASan/MSan toolchain wired in CI);
  labeled unavailable rather than claimed.

## 9.4 — D0 matrix, baseline vs optimized (same host/shaping/reps)

`throughput --matrix-smoke` × 5 reps per scenario (in-process fixtures,
release), medians with min/max spread; records in
`benches/results/h1/matrix/records.md` (last rep) and the session logs.

**H1 goodput (MiB/s), baseline → optimized median:**

| Scenario | baseline | optimized | spread | Δ |
|---|---|---|---|---|
| 1GiB workers_1 | 811.6 | 805.3 | 1.1% | −0.8% |
| 1GiB workers_2 | 1591.2 | 1479.8 | 4.9% | −7.0% |
| 1GiB workers_4 | 2731.7 | 2714.8 | 12.0% | −0.6% |
| 1GiB workers_8 | 4231.6 | 4203.1 | 4.1% | −0.7% |
| 256MiB workers_1/2/4/8 | 807/1566/2005/2744 | 803/1266/2053/2564 | 1.5–46% | within bimodal noise except workers_2 (see notes) |
| 32MiB workers_1/2/4/8 | 788/1391/2483/631 | 793/1375/2482/630 | 1–12% | parity |

**H2 goodput (MiB/s), baseline → optimized median:**

| Scenario | baseline | optimized | spread | Δ |
|---|---|---|---|---|
| 1GiB workers_1/2/4/8 | 830/823/834/830 | 815/842/815/816 | 0.1–2.7% | parity (fixture-bound) |
| 256MiB workers_1/2/4/8 | 741/740/832/740 | 838/835/828/829 | 0.2–1.3% | **+12–13%** |
| 32MiB workers_1/2/4/8 | 399/397/398/395 | 397/392/804/811 | up to 109% (bimodal) | **+102–105%** at workers_4/8 |

**Isolated 10 ms RTT axis (32 MiB, 4 workers, process-separated fixture,
client-only resources, 5 reps):**

| Protocol | baseline median | optimized median | Amplification |
|---|---|---|---|
| H1 | 297.1 | 324.4 | 1.00–1.024 (baseline 1.00–1.06) |
| H2/TLS | 276.7 (amp 1.16–1.19) | 242.6 | **1.00 flat** |

Notes for honest reading:

1. The H2 small/mid-file gains are the compound effect of the bench
   `parse_range` fixture fix (disclosed in the phase-5 report: all baseline
   H2 rows ran single-stream) plus segmented H2 multiplexing. The 1 GiB H2
   row is fixture-server-bound (~830 MiB/s single process); do not read it
   as "no benefit from multiplexing".
2. H2/TLS at 10 ms RTT: wire amplification went from 1.16–1.19 to 1.00;
   nominal goodput −12% but useful goodput is at parity or slightly better
   once the baseline's redundant re-transfer is discounted.
3. H1 256MiB/workers_2 (−19%) sits in a cell whose baseline spread was
   already 23% (bimodal timer quantization); the 1 GiB/workers_2 −7% with
   4.9% spread is the only candidate regression and is within that cell's
   historical noise (baseline spread 9.2%).
4. CPU %, RSS, completion-time dispersion: within baseline bands; writer
   threads/latency are covered by the phase-2 gate (shared executor) and
   were not re-plumbed into the matrix rows; syscall/allocation counts
   remain uninstrumented (labeled).

**Gate criteria (task 9.4):**

| Criterion | Evidence | Verdict |
|---|---|---|
| Clean H1 split amplification < 1.10 | dedicated task-3.6 fixture re-verified green in the current tree (`wire_amplification_tests`, 4 tests; 1.016×–1.023× measured in report-phase-3; the 2.0× unconditional-failure assert remains) | PASS |
| Actual adaptive up/down (not just desired) | phase-1 actual-active gauges + phase-5 decision traces + `runtime_control_tests` (20) | PASS |
| Bounded bytes | write-budget reservation tests (`segmented_tests`/`phase3_exit_tests`) green | PASS |
| No material single-stream regression | workers_1 rows: H1 −0.8%/+0.6%, H2 −1.8%/+2.4% — all within dispersion | PASS |
| No material CPU regression | matrix CPU % within baseline bands (e.g. 1 GiB/workers_8: 928% → ~930%) | PASS |
| No statistically unsupported speedup claims | every claimed gain is mechanistically explained (fixture fix, multiplexing) with medians+spreads; flat cells reported as parity | PASS |

## 9.5 — docs

`README.md` and `docs/benchmark-profiling.md` updates, stable/advanced/
internal knob separation, rollback switches and future options are applied
in the same commit as this report (see the docs diff). Obsolete legacy-
writer references are removed with the phase-2 legacy branch.

## 9.6 — workspace-wide checks

`cargo test --workspace --all-targets`, `cargo clippy --workspace
--all-targets -- -D warnings`, doc build and supported-target checks are run
and recorded in the same commit; results and host limitations recorded
there.
