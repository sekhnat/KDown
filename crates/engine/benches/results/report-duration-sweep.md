# Duration-sizing sweep (task 3.5)

Recorded 2026-09-24 on the phase-3 working tree (task 3.4 implementation).
Harness: `throughput --duration-sweep` — H1/H2 × {unshaped, shaped
(per-connection ~16 MiB/s pacing)} × target duration {0.5, 1, 1.5 s} ×
ready-work factor {2, 3, 4}, 2 repetitions per configuration, against the
`explicit-8MiB` and `automatic-x3` baselines. Fixture 256 MiB unshaped /
64 MiB shaped, 4 workers, tmpfs destination. Every row is
verification-gated (published + size + hash). Raw records:
`benches/results/duration-sweep/records.md`.

## Medians (goodput MiB/s; amplification = network/completed)

| Config | H1 unshaped | H1 shaped | H2 unshaped | H2 shaped | max amp |
|---|---|---|---|---|---|
| explicit-8MiB (baseline) | 2010 | 48.1 | 737 | 12.0 | 1.000 |
| automatic-x3 (baseline) | 1493 | 45.2 | 830 | 12.0 | 1.021 |
| dur500ms-ready2 | 2524 | 45.7 | 779 | 12.0 | 1.001 |
| dur500ms-ready3 | 2020 | 46.6 | 772 | 12.1 | 1.001 |
| dur500ms-ready4 | 2026 | 46.4 | 826 | 12.0 | 1.001 |
| dur1000ms-ready2 | 2032 | 47.5 | 731 | 12.0 | 1.001 |
| **dur1000ms-ready3** | **2458** | **47.5** | **832** | **12.0** | 1.021 |
| dur1000ms-ready4 | 2015 | 46.3 | 782 | 12.0 | 1.000 |
| dur1500ms-ready2 | 2509 | 46.9 | 736 | 12.1 | 1.000 |
| dur1500ms-ready3 | 2498 | 48.2 | 787 | 12.0 | 1.000 |
| dur1500ms-ready4 | 2020 | 46.8 | 788 | 12.1 | 1.015 |

- **Retries: 0 on every row.** Amplification ≤ 1.037 across all 80 rows —
  the ready-work + receipt-watermark split machinery does not duplicate
  payload at any sizing (the split-stopped worker's discarded tail is the
  only overhead and it is bounded: amp ≤ 1.037 worst case, ≤ 1.021 for the
  recommended config).
- **H1 shaped:** duration configs 45.7–48.2 vs baselines 48.1/45.2 — parity
  within dispersion; the sizing barely matters on a paced connection
  because request latency is a fixed share of each lease's service time.
- **H1 unshaped:** every duration configuration is at or above the explicit
  baseline (2010); dur1000ms-ready3 reaches 2458 (+22%) and dur1500ms-ready2
  2509 (+25%). The automatic-x3 baseline is the weakest (1493) — the
  ready-work divisor plus duration sizing together outperform the
  one-shot automatic target on loopback.
- **H2 shaped:** 12.0–12.1 everywhere — completely network-bound; sizing is
  invisible, as expected. **H2 unshaped:** 731–832 vs 737/830 — parity
  within dispersion; dur1000ms-ready3 ties the best row (832).
- Completion latency: walls track goodput inversely at fixed size (0.106–
  0.128 s unshaped H1; 1.33–1.41 s shaped H1; ~5.33 s shaped H2).
- Variance: 2 repetitions per configuration; the loopback unshaped axis
  shows ~±10% run-to-run spread (same shape as the phase-0 baseline
  dispersion), so single-row deltas under ~10% are not claimed as gains.
  The shaped axes are almost perfectly stable (pacing quantized).

## Documented opt-in default (task 3.5 decision)

**Recommended opt-in values: `duration_ms = 1000`, `auto_oversubscription`
(ready-work factor) = 3** — recorded on `SegmentSizing::Duration`.

Rationale: the 1000 ms / ready-3 combination is at or above both baselines
on every axis that is not network-bound (H1 unshaped +22% median, H2
unshaped +13% median, H1 shaped within 1.2% of the explicit baseline, H2
shaped identical), with amplification ≤ 1.021 and zero retries. Shorter
durations (500 ms) trade request overhead for marginally less adaptivity;
longer durations (1500 ms) show no additional benefit on any axis.

**No existing default changes**: `SegmentSizing::Explicit` remains the
default sizing and `Duration` is strictly opt-in (engine-api compatibility
requirement); the values above are tuning guidance for callers who opt in,
verified against the 3.6 gate bounds.
