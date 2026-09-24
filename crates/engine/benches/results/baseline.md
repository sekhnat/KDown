# Benchmark baseline — kdown-engine (task 6.5, re-recorded after milestone 1)

Recorded: 2026-09-24, machine: AMD Ryzen 7 9700X (8C/16T), Linux 6.x,
loopback (§37.1 localhost scenarios). Fixture 32 MiB per iteration,
release profile (`lto = thin`, `codegen-units = 1`).

This baseline was re-captured after the milestone-1 harness corrections
(tasks 1.1-1.4): byte fields come from real counters, the raw HTTP/1.1
responder parses Range headers case-insensitively (RFC 9110 §5.1), and every
recorded run verifies final size, SHA-256, and atomic publication. Values
from the earlier 2026-09-22 record are not comparable to these.

Harness: `cargo bench --bench throughput` (criterion, 10 samples,
0.5 s warm-up, 1.5 s measurement). Scenarios: §37.1 localhost HTTP/1.1
(raw TCP responder), localhost HTTP/2 via ALPN (self-signed CA bundle),
varying workers, preallocation on/off (§37.2 prealloc toggle; tmpfs/
RAM-disk runs use the same harness against a tmpfs destination).

## Results (criterion medians)

| Scenario | Throughput | Wall | CPU | Peak RSS | Retransferred | Connections |
|---|---|---|---|---|---|---|
| h1/workers_1 | ~3.6 GiB/s (8.87 ms) | 8.9 ms | n/r | n/r | 0 | n/r |
| h1/workers_4 | ~3.6 GiB/s (8.89 ms) | 8.9 ms | n/r | n/r | 0 | n/r |
| h2/workers_1 | ~2.0 GiB/s (15.64 ms) | 15.6 ms | n/r | n/r | 0 | n/r |
| h2/workers_4 | ~2.0 GiB/s (16.13 ms) | 16.1 ms | n/r | n/r | 0 | n/r |
| prealloc_true | ~1.8 GiB/s (18.19 ms) | 18.2 ms | n/r | n/r | 0 | n/r |
| prealloc_false | ~1.8 GiB/s (17.59 ms) | 17.6 ms | n/r | n/r | 0 | n/r |

CPU / RSS / retransferred / connections columns for the criterion loop are
recorded per-run by the smoke-matrix resource records (§37.3) instead of the
criterion loop; see `docs/benchmark-profiling.md` and
`benches/results/matrix-smoke/records.md` for the full per-run tables.

## Resource records (single measured run per scenario)

| Scenario | Goodput (MiB/s) | Wire (MiB/s) | Network vs fixture | Wall | CPU | Peak RSS | Ctx |
|---|---|---|---|---|---|---|---|
| h1/workers_1 | 5630 | 5630 | 1.00× | 5.7 ms | 176% | 45252 KiB | 1 |
| h1/workers_4 | 2147 | 2147 | 2.06× | 30.7 ms | 98% | 47572 KiB | 3 |
| h2/workers_1 | 3956 | 3956 | 1.00× | 8.1 ms | 247% | 47572 KiB | 2 |
| h2/workers_4 | 3736 | 3736 | 1.00× | 8.6 ms | 233% | 47640 KiB | 2 |
| prealloc_true | 2282 | 2282 | 2.54× | 35.6 ms | 112% | 56432 KiB | 1 |
| prealloc_false | 2285 | 2285 | 2.54× | 35.7 ms | 56% | 55988 KiB | 1 |

Notes:
- Segmented HTTP/1.1 runs (workers ≥ 2) re-download overlapping ranges in
  the pre-change engine (idle-worker tail splits while the split lease's
  worker keeps streaming its original request). Network bytes exceed the
  fixture (2.06× at h1/workers_4, 2.54× in the prealloc scenarios with
  default 8 max workers). This waste is now visible because the byte fields
  come from real counters; milestones 6-8 remove it.
- Single-stream (workers_1) runs are exact (1.00×) at every size.
- HTTP/2 segmented runs stay exact and plateau at ~820-845 MiB/s across
  worker counts in the smoke matrix (single-connection multiplexing).

## §22 phase-4 exit assessment

- Loopback ceiling: single-stream h1 measures ~5.6 GiB/s hot; segmented
  wire throughput exceeds 2 GiB/s, so 1 Gbit/s-class rates are reached far
  below the loopback ceiling (§22.1).
- CPU: at these rates CPU is dominated by duplicated work and TLS; the
  per-scenario CPU% is recorded in the resource records.

## Local regression thresholds (§37.4)

The values below come from the AMD Ryzen 7 9700X workstation and are only
comparable on matching hardware and environment. Run `./scripts/bench_check.sh`
there; it checks for more than 10% throughput loss on the enforced scenarios.
Repeat measurements when a result is near the threshold.

GitHub-hosted runners are variable shared VMs and can report different
throughput units (for example, MiB/s instead of GiB/s). The `bench-check`
CI job executes the benchmark scenarios but does not compare their absolute
throughput to this workstation baseline. CI output is informational; use a
same-host run for performance conclusions.

## How to regenerate

```sh
cargo bench --bench throughput -- --warm-up-time 0.5 --measurement-time 1.5
```

Update this file only from a controlled same-host baseline run.
`./scripts/bench_check.sh` compares locally against this record; CI's
`bench-check` job runs the scenarios without a cross-host comparison.
