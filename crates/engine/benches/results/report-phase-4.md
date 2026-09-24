# Optimize-transfer-engine-v2 — phase 4 gate (task 4.4)

Recorded 2026-09-25 on the phase-4 completion working tree (tasks 4.1–4.3
committed) against the pre-phase-4 tree at `8889051` (the phase-3 gate
commit). Baselines: `report-phase-3.md`, `report-phase-2.md`,
`baseline-v2-core.md`, `baseline-v2-wan.md`. Raw records:
`benches/results/phase-4/`.

## What phase 4 changed

- **Task 4.1** — the adaptive window's inputs are interval-weighted actual
  active/idle worker-time (not an instantaneous cell sample), writer
  acknowledgement-latency and submit-time queue-depth percentiles, byte-budget
  wait, and counter deltas that exclude resumed bytes and count wire receipts
  once.
- **Task 4.2** — storage-pressure veto and process-resource veto with
  sustained-window hysteresis, plus `DecisionReason` codes; thresholds are
  provisional config defaults.
- **Task 4.3** — decisions verified on real leases (growth, reduction, manual
  override, pause, retry) and bounded on noisy windows.

## Chosen thresholds (provisional, from `AdaptiveConfig::default`)

| Signal | Ceiling | Rationale |
|---|---|---|
| Writer queue-depth p95 | 8 outstanding writes | above a healthy pipeline depth on either write path |
| Writer acknowledgement p95 | 250 ms | slow acknowledgements mean the sink is the bottleneck |
| Byte-budget wait | 250 ms per window | workers blocked on write bytes, not network |
| Process RSS | 1 GiB | memory ceiling for a download engine |
| Window CPU | 400 % (4 cores) | generous multi-core ceiling |
| Sustained pressure windows | 2 | one saturated window vetoes growth; two shrink |

Reason codes (`DecisionReason`): `ManualOverride`, `EmptyWindow`,
`Cooldown`, `Baseline`, `GainKept`, `NoGainReverted`, `Probe`,
`RetryPressure`, `StoragePressure`, `ResourcePressure`, `AtMaximum`.

## Method

- Harness: `benches/throughput.rs` in-process paced H1/H2 servers,
  `--adaptive-compare` (H1/H2 x shaped/unshaped x fixed-4/adaptive-1-4,
  3 reps, 256 MiB unshaped / 128 MiB shaped) and `--storage-compare`
  (H1/H2 unshaped, fixed-4 vs adaptive-1-4, 8 reps, 128 MiB, optional
  `--dest-dir` for a real constrained device).
- Axes: `/tmp` tmpfs (fast) and `/mnt/HDD` (rotational, 98 % full — the
  constrained-disk axis). Shaping is 16 MiB/s per connection; H2 uses one
  multiplexed TLS socket.
- Every run verifies published size and SHA-256; records include goodput,
  wire amplification, retries, wall time, CPU, peak RSS and server counts.
- Environment note: the HDD axis is **bimodal across passes** (btrfs on a
  nearly full rotational disk). One full-matrix pass measured fixed-4 at
  57–73 MiB/s while a repeat measured 128–166 MiB/s for the same cell, so
  the storage axis is reported from the 8-repetition focused mode, not from a
  single pass. The first HDD figures were discarded as a cold/unlucky pass,
  not reported as a result.

## Results — fixed-4 vs adaptive (v2)

Medians with [min,max] dispersion.

| Axis | Cell | fixed-4 | adaptive-1-4 | Δ |
|---|---|---|---|---|
| tmpfs | h1 unshaped | 2945 [1514,3000] | 777 [762,787] | adaptive −74 % |
| tmpfs | h1 shaped | 48.2 [48.1,48.3] | 30.6 [30.4,30.6] | adaptive −37 % |
| tmpfs | h2 unshaped | 661 [652,838] | 662 [654,834] | parity |
| tmpfs | h2 shaped | 12.0 | 12.0 | parity (network-bound) |
| HDD | h1 unshaped | 164.9 [128,174] | 145.3 [118,173] | −12 % (ranges overlap) |
| HDD | h2 unshaped | 145.7 [119,164] | 152.1 [113,167] | +4 % (ranges overlap) |
| HDD | h1 shaped | 43.0 [42.5,44.0] | 28.1 [27.5,28.3] | adaptive −35 % |
| HDD | h2 shaped | 11.6 | 11.6 | parity (network-bound) |

Adaptive starts at one worker and probes with a 500 ms window plus a 1 s
cooldown, so on transfers that finish in well under a second (tmpfs
unshaped, 128–256 MiB) it never reaches the fixed worker count and loses
goodput. Where the link is the bottleneck (H2 shaped, H2 unshaped with
overlapping ranges, HDD with overlapping ranges) it is at parity. No cell
shows adaptive beneficial outside dispersion.

## Results — v1 vs v2 (pre-phase-4 tree `8889051` vs this tree)

| Axis | Cell | v1 | v2 | Δ |
|---|---|---|---|---|
| tmpfs | h1 unshaped adaptive | 775.8 | 777.2 | +0.2 % |
| tmpfs | h2 unshaped adaptive | 661.2 | 661.7 | +0.1 % |
| tmpfs | h1 shaped adaptive | 30.55 | 30.55 | 0.0 % |
| tmpfs | h2 shaped adaptive | 11.97 | 11.98 | +0.1 % |
| HDD | h1 unshaped adaptive | 140.3 | 145.3 | +3.6 % (overlap) |
| HDD | h2 unshaped adaptive | 142.3 | 152.1 | +6.9 % (overlap) |
| HDD | h1 shaped adaptive | 28.1 | 28.1 | 0.0 % |
| HDD | h2 shaped adaptive | 11.59 | 11.57 | −0.2 % |

v2 is at parity with v1 on every measured axis; the small HDD deltas are
inside overlapping dispersion and are not claimed as gains. Wire
amplification was 1.000–1.007 with zero retries in every cell of both
trees.

## Fixed-mode regression check

Fixed mode does not run the controller, and the measurements agree:

| Axis | Cell | v1 | v2 | Δ |
|---|---|---|---|---|
| tmpfs | h1 unshaped | 1526.8 | 2944.9 | both bimodal (see note) |
| tmpfs | h2 unshaped | 662.9 | 661.3 | −0.2 % |
| tmpfs | h1 shaped | 48.39 | 48.22 | −0.4 % |
| tmpfs | h2 shaped | 11.98 | 12.00 | +0.2 % |
| HDD | h1 unshaped | 162.2 | 164.9 | +1.7 % |
| HDD | h2 unshaped | 144.9 | 145.7 | +0.6 % |
| HDD | h1 shaped | 42.61 | 42.95 | +0.8 % |
| HDD | h2 shaped | 11.59 | 11.59 | −0.1 % |

No fixed-mode regression: every shaped/HDD cell is within ±2 %, and the
tmpfs h1 unshaped cell is bimodal in *both* trees (1.5 GiB/s and 2.9 GiB/s
modes, loopback variance already documented in the phase-2 gate), with both
modes present in each tree.

## Storage-pressure evidence

No measured cell produced writer backlog or acknowledgement latency above
the ceilings, so the veto never fired during the gate runs: with the legacy
writer lane the outstanding depth is 1 by construction and tmpfs/HDD
acknowledgements stayed far below 250 ms. The veto is therefore verified by
deterministic synthetic windows (task 4.2 tests: writer saturation,
acknowledgement latency, byte-budget wait, RSS, CPU, sustained hysteresis,
recovery re-probe) and the live adaptive-window test (task 4.1), not by the
benchmark matrix. The HDD is network-limited at 16 MiB/s when shaped and
bounded by the ~150–165 MiB/s device when unshaped, which is why neither
mode is storage-saturated here.

## Gate decision

1. **Adaptive concurrency stays opt-in** (`ConcurrencyMode::Adaptive`); the
default remains fixed concurrency. The measured axes show adaptive is not
beneficial outside dispersion (and is clearly worse on fast storage with
short transfers), so no default changes and the legacy adaptive selector
switch stays as the only way to opt in.
2. **v2 is retained as the opt-in adaptive behavior** with parity against v1
on every measured axis, and with the storage/resource veto and reason codes
that the deterministic tests exercise.
3. **No fixed-mode regressions** against the pre-phase-4 tree.
4. Thresholds above are provisional defaults recorded here; a future default
change would need evidence on a storage-saturated axis, which this machine
cannot produce with the current fixtures.

## Unavailable axes (labeled, not fabricated)

- No storage-saturated device was available: `/tmp` is tmpfs, `/mnt/1T` is
  NVMe, and `/mnt/HDD` reaches ~165 MiB/s unshaped, so the write path never
  became the bottleneck. The veto is covered by deterministic tests instead.
- H2 flow-control wait is still not instrumented (unchanged from phase 2).
- RTT/loss shaping beyond the per-connection pacing was not run.
