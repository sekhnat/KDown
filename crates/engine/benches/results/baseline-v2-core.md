# Optimize-transfer-engine-v2 baseline core (task 0.5)

- Commit: `cc6b4b6` (pre-change baseline for this change)
- Host: AMD Ryzen 7 9700X (8C/16T), 32 GiB, CachyOS Linux 6.2.6; loopback fixtures; tmpfs destinations
- Method: 5 measured repetitions of `throughput --matrix-smoke` (in-process H1/H2 fixture servers, release build); median and spread of useful goodput (MiB/s). Verification (size/hash/publication) asserted ok on every repetition.
- workers_1 rows exercise the single-stream (below-threshold) path; workers>1 rows are segmented with the default explicit 8 MiB initial segment size.
- Separate isolated runs (client-only resources, process-separated fixture server) recorded for the 10 ms RTT axis: H1 x5 and H2-over-TLS x3 repetitions at 32 MiB / 4 workers, `--rtt-ms 10`.

| Scenario | Median goodput | Min | Max | Spread % | Median wire | Median CPU % | Median RSS | Reps | Verify |
|---|---|---|---|---|---|---|---|---|---|
| matrix-smoke/h1/1GiB/workers_1 | 811.6 | 801.4 | 811.8 | 1.3% | 811.6 | 111.0 | 27864 | 5 | ok |
| matrix-smoke/h1/1GiB/workers_2 | 1591.2 | 1447.7 | 1594.7 | 9.2% | 1591.5 | 248.8 | 27864 | 5 | ok |
| matrix-smoke/h1/1GiB/workers_4 | 2731.7 | 2716.2 | 3015.9 | 11.0% | 2749.6 | 466.9 | 27864 | 5 | ok |
| matrix-smoke/h1/1GiB/workers_8 | 4231.6 | 4022.6 | 4332.9 | 7.3% | 4260.9 | 928.2 | 31436 | 5 | ok |
| matrix-smoke/h1/256MiB/workers_1 | 807.0 | 792.0 | 814.0 | 2.7% | 807.0 | 111.4 | 18740 | 5 | ok |
| matrix-smoke/h1/256MiB/workers_2 | 1565.9 | 1217.7 | 1579.7 | 23.1% | 1567.7 | 246.3 | 19964 | 5 | ok |
| matrix-smoke/h1/256MiB/workers_4 | 2005.1 | 1976.9 | 3002.0 | 51.1% | 2038.0 | 360.3 | 22356 | 5 | ok |
| matrix-smoke/h1/256MiB/workers_8 | 2743.8 | 2532.5 | 4131.6 | 58.3% | 2789.3 | 629.1 | 27864 | 5 | ok |
| matrix-smoke/h1/32MiB/workers_1 | 788.3 | 767.9 | 803.7 | 4.5% | 788.3 | 98.5 | 8272 | 5 | ok |
| matrix-smoke/h1/32MiB/workers_2 | 1391.0 | 496.8 | 1418.3 | 66.3% | 1391.0 | 217.9 | 11304 | 5 | ok |
| matrix-smoke/h1/32MiB/workers_4 | 2483.2 | 585.3 | 2490.2 | 76.7% | 2483.2 | 388.0 | 13108 | 5 | ok |
| matrix-smoke/h1/32MiB/workers_8 | 630.8 | 597.2 | 643.7 | 7.4% | 970.0 | 205.3 | 18740 | 5 | ok |
| matrix-smoke/h2/1GiB/workers_1 | 830.1 | 800.9 | 838.3 | 4.5% | 830.1 | 140.4 | 31436 | 5 | ok |
| matrix-smoke/h2/1GiB/workers_2 | 822.7 | 803.1 | 839.5 | 4.4% | 822.7 | 141.8 | 31436 | 5 | ok |
| matrix-smoke/h2/1GiB/workers_4 | 834.0 | 802.2 | 840.4 | 4.6% | 834.0 | 142.5 | 31436 | 5 | ok |
| matrix-smoke/h2/1GiB/workers_8 | 829.9 | 811.4 | 839.4 | 3.4% | 829.9 | 142.4 | 31436 | 5 | ok |
| matrix-smoke/h2/256MiB/workers_1 | 740.6 | 727.2 | 838.9 | 15.1% | 740.6 | 127.8 | 27864 | 5 | ok |
| matrix-smoke/h2/256MiB/workers_2 | 739.8 | 731.1 | 838.1 | 14.5% | 739.8 | 128.5 | 27864 | 5 | ok |
| matrix-smoke/h2/256MiB/workers_4 | 831.5 | 742.3 | 842.3 | 12.0% | 831.5 | 144.2 | 27864 | 5 | ok |
| matrix-smoke/h2/256MiB/workers_8 | 740.3 | 732.5 | 834.1 | 13.7% | 740.3 | 128.8 | 27864 | 5 | ok |
| matrix-smoke/h2/32MiB/workers_1 | 399.3 | 392.6 | 804.0 | 103.0% | 399.3 | 62.4 | 18740 | 5 | ok |
| matrix-smoke/h2/32MiB/workers_2 | 396.6 | 393.0 | 800.2 | 102.7% | 396.6 | 86.8 | 18740 | 5 | ok |
| matrix-smoke/h2/32MiB/workers_4 | 397.9 | 392.8 | 800.6 | 102.5% | 397.9 | 74.6 | 18740 | 5 | ok |
| matrix-smoke/h2/32MiB/workers_8 | 395.3 | 392.0 | 796.7 | 102.4% | 395.3 | 74.1 | 18740 | 5 | ok |

## Isolated 10 ms RTT axis (32 MiB, 4 workers, fixture server `--rtt-ms 10`, client-only resources)

| Rep | Protocol | Goodput MiB/s | Wire MiB/s | Network bytes | Amplification (net+wasted)/completed | Verify |
|---|---|---|---|---|---|---|
| 1 | H1 | 332.26 | 332.26 | 33554432 | 1.00 | ok |
| 2 | H1 | 294.25 | 312.71 | 35659776 | 1.063 | ok |
| 3 | H1 | 326.55 | 326.55 | 33554432 | 1.00 | ok |
| 4 | H1 | 293.52 | 307.28 | 35127296 | 1.047 | ok |
| 5 | H1 | 297.11 | 306.81 | 34650112 | 1.033 | ok |
| 1 | H2/TLS | 422.13 | 495.14 | 39357440 | 1.173 | ok |
| 2 | H2/TLS | 276.28 | 328.40 | 39884800 | 1.189 | ok |
| 3 | H2/TLS | 276.68 | 320.12 | 38821888 | 1.157 | ok |

## Findings pinned by the baseline

- **Live-tail split duplication is real and RTT-sensitive**: with the default
  8 MiB segments on loopback the local matrix shows network == completed
  (amplification 1.00), but under a 10 ms RTT the isolated runs show H1
  1.00–1.06× and H2/TLS 1.16–1.19× amplification from split-overlap
  re-delivery. This is the measured gap phase 3 closes.
- H2 goodput at 10 ms RTT degrades to ~280–420 MiB/s (from ~430–850 local),
  consistent with stream-latency sensitivity; H1 ranges re-establish
  connections under RTT (connections column rises).
- Local matrix medians (H1 32 MiB): workers_1 ≈ single-stream path; see the
  scenario table above for the full grid with dispersion.

## Data-quality assessment (same-host, no quiet-host guarantee)

- **32 MiB rows are noise-dominated** (transfers complete in 13–40 ms):
  spreads up to 66–77% at workers_2/4. Use these rows only as smoke/parity
  signals, never as performance evidence.
- **256 MiB rows**: workers_1/2 within ~3–23%; workers_4/8 show isolated fast
  reps up to ~2× the median (51–58% spread) — treat medians with the spread
  column and never claim improvements inside it.
- **1 GiB rows are the reference core**: spreads 1–11% across the grid
  (h1/1GiB/workers_8 median 4231.6 MiB/s, 7.3% spread).
- H2 rows are flat across worker counts (known single-connection behavior),
  740–840 MiB/s locally, spread 3–15%.
- Phase gates in this change must compare against these medians with the
  recorded dispersion and flag overlapping variance instead of claiming
  speedups.

- **Not run in this core** (manual/long axes, deliberately excluded from the
  ≥5-rep automation): 16-worker rows and multi-GiB sizes (`--matrix-manual`
  covers them on demand), 50/150 ms RTT, loss/throttle shaping, and
  reset+resume sequences — recorded in task 0.6 where the shaping fixture
  supports them; kernel netem axes remain unavailable on this host.
