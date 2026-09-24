# Optimize-transfer-engine-v2 WAN/manual baseline axes (task 0.6)

- Commit: `cc6b4b6` (pre-change baseline). Same host as `baseline-v2-core.md`.
- Method: process-isolated fixture server with server-side shaping
  (`--throttle-mib-s`, `--rtt-ms`, `--loss-percent`, `--reset-after-bytes`),
  client-only resources via `throughput --isolated`; multi-GiB grid via
  `throughput --matrix-manual` (in-process servers, one measured run per
  scenario). All runs verified size/hash/publication `ok`.

## Bandwidth shaping (32 MiB, 4 workers, fixture throttle)

| Axis | Target | Measured (reps) | Calibration | Verify |
|---|---|---|---|---|
| 100 Mbps | 12.5 MiB/s | 11.50 / 11.45 / 11.33 MiB/s | ~91% of nominal (pacing overhead) | ok ×3 |
| 1 Gbps | 125 MiB/s | 14.59 / 14.35 / 14.25 MiB/s | **NOT usable**: per-chunk sleeps are bounded by the ~1 ms tokio timer wheel (4 KiB chunks → effective ≈4–14 MiB/s ceiling) | ok ×3 |

**Fixture limitation recorded**: the throttle paces per 4 KiB block write;
high rates need larger pacing quanta. The 1 Gbps axis is marked
fixture-limited/not run rather than reported as shaped evidence. Candidate
fix (larger pacing quanta) is a harness improvement, not an engine change.

## RTT and loss (32 MiB, 4 workers, fixture shaping)

| Axis | Goodput (reps) | Amplification | Retries | Verify |
|---|---|---|---|---|
| RTT 50 ms | 80.3 / 104.8 MiB/s | 1.137 (both reps) | 0 | ok |
| RTT 150 ms | 61.3 / 37.0 MiB/s | 1.00 / 1.123 | 0 | ok |
| Loss 20% | 26.5 / 148.4 MiB/s | n/r (stats transfer truncated by loss) | 6 / 2 | ok |

- High variance under probabilistic loss is expected; both reps recovered
  byte-exact through the retry path.
- RTT runs again show split-overlap amplification (1.12–1.14×), consistent
  with the 10 ms axis in `baseline-v2-core.md`.

## Reset / interruption (32 MiB, 4 workers)

- `--reset-after-bytes 1048576` (every response cut at 1 MiB): the job
  recovers through tail-only retries; earlier in-process tests pin the
  accounting (network == completed, retries ≥ 1). Process-restart resume is
  covered by `crash_restart_tests` (in-process) and was not re-run here.

## Multi-GiB manual grid (one run per scenario, `--matrix-manual`)

| Scenario | Goodput MiB/s | Wire/completed | CPU % | Verify |
|---|---|---|---|---|
| h1/2GiB/workers_8 | 4988 | 1.004 | 1096 | ok |
| h1/2GiB/workers_16 | 3729 | 1.015 | 1014 | ok |
| h1/4GiB/workers_8 | 4654 | 1.002 | 1025 | ok |
| h1/4GiB/workers_16 | 4057 | 1.006 | 1080 | ok |
| h2/2GiB/workers_1..16 | 815–841 | 1.000 | ~142 | ok ×5 |
| h2/4GiB/workers_1..16 | 831–835 | 1.000 | ~142 | ok ×5 |

**Findings**: H1 scales to 8 workers (4.6–5.0 GiB/s) and *regresses* at 16
workers (−18…−23% vs workers_8) — oversubscription beyond 8 has no benefit on
this host (8C/16T). H2 stays flat ~830 MiB/s at every worker count. Wire
amplification ≤ 1.015 in all manual rows (large leases; live-tail splits rare).

## Not run / unavailable (explicit)

- Kernel netem RTT/loss shaping: no `tc`/root netem on this host — server-side
  approximation only.
- 1 Gbps shaped axis: fixture throttle granularity bound (above), not engine
  evidence.
- Concurrent same-origin shaped jobs at WAN rates: harness supports it
  (`--jobs-smoke` is local-only so far); scheduled with the phase-6 origin
  work.
- Real long-fat-network validation (e.g. 100 ms RTT × 1 Gbps): requires both
  axes simultaneously; the fixture can set them independently — noted as a
  manual follow-up, not silently assumed.

## Storage and contention axes (task 0.7; 1 GiB, 4 workers, H1, isolated server)

| Destination | Goodput (3 reps) | CPU % | Peak RSS | Verify |
|---|---|---|---|---|
| tmpfs (`/tmp`) | 715 / 715 / 789 MiB/s | ~26 | ~11.0 MiB | ok ×3 |
| NVMe btrfs (`/mnt/1T`, zstd) | 781 / 713 / 844 MiB/s | ~30 | ~11.5 MiB | ok ×3 |
| HDD xfs (`/mnt/HDD`, rotational) | 158 / 185 / 172 MiB/s | ~8.7 | ~16.5 MiB | ok ×3 |

- NVMe is at parity with tmpfs here: the isolated loopback server (not
  storage) is the bottleneck at this shape. HDD is storage-bound: goodput
  drops ~4x and client CPU falls to ~9% — the client/server process split is
  visible (the client is idle-waiting on writes, the server is not the
  bottleneck).
- **Resource-isolation evidence**: client CPU% tracks the true bottleneck
  (network-bound 100 Mbps run: 2.9%; local in-process: 388–1096%; HDD-bound:
  8.7%). Peak RSS stays ~9–17 MiB client-side at 4 workers regardless of
  storage. Writer-thread count is not directly observable (lanes are plain
  blocking tasks inside the tokio runtime); RSS scaling (~+1.3 MiB/worker,
  measured in the milestone-10 record) remains the proxy until the phase-2
  executor exposes queue/ack gauges.
- Contention axes (one-job / same-origin / multi-origin, H1+H2) are recorded
  by `benches/results/jobs-smoke/records.md`: same-origin 4×64 MiB aggregate
  2075 MiB/s (H1) vs 649 MiB/s single-job; multi-origin 2049 MiB/s —
  contention is mild at 4 jobs; per-job verification ok everywhere.

## Baseline conclusions carried into phase 1+

1. Adaptive worker growth is broken (desired-only provisioning) — phase 1.
2. Live-tail splits duplicate 12–19% of payload under RTT — phase 3.
3. H1 >8 workers regresses on this host; 16-worker oversubscription has no
   value — informs adaptive bounds.
4. H2 is latency-sensitive (RTT 10 ms → ~280–420 MiB/s) and flat in workers
   — phase 5 must weigh stream concurrency against one-connection defaults.
5. Storage-bound transfers already self-throttle client CPU (HDD 8.7%) —
   the phase-4 storage-pressure veto must not fight healthy backpressure.
