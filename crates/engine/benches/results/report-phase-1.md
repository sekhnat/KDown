# Optimize-transfer-engine-v2 — phase 1 gate (task 1.6)

Recorded 2026-09-24 at the phase-1 completion commit (working tree on top of
`cc6b4b6`). Baseline reference: `baseline-v2-core.md` / `baseline-v2-wan.md`.

## Correctness gates

- `cargo test -p kdown-engine`: 30 suites, all `test result: ok`, 0 failures
  (283 lib tests + 15 fixture-isolated + 17 runtime-control + …).
- `cargo clippy -p kdown-engine --all-targets`: clean.
- `--adaptive-compare` (256 MiB, one measured run per config, all verified
  ok — size/hash/publication):

| Scenario | Goodput MiB/s | Wire/completed | CPU % | Peak RSS |
|---|---|---|---|---|
| h1/unshaped/fixed-4 | 2019 | 1.002 | 355 | 14.9 MiB |
| h1/unshaped/adaptive-1-4 | 784 | 1.000 | 119 | 14.9 MiB |
| h1/shaped/fixed-4 | 48.5 | 1.000 | 10.6 | 14.9 MiB |
| h1/shaped/adaptive-1-4 | 36.8 | 1.016 | 8.1 | 14.9 MiB |
| h2/unshaped/fixed-4 | 741 | 1.000 | 127 | 16.1 MiB |
| h2/unshaped/adaptive-1-4 | 831 | 1.000 | 146 | 16.1 MiB |
| h2/shaped/fixed-4 | 12.1 | 1.000 | 3.4 | 16.2 MiB |
| h2/shaped/adaptive-1-4 | 12.1 | 1.000 | 3.5 | 16.2 MiB |

- Fixed-mode behavior is unchanged (starts at `max_workers`, same defaults);
  adaptive remains opt-in (`ConcurrencyMode::Fixed` default).

## Actual-vs-desired behavior (new instrumentation, tests as evidence)

- `adaptive_probe_activates_additional_workers`: desired probes 1→2 and the
  observed **active** workers reach 2 (was impossible before this phase —
  the deterministic failing test now passes).
- `worker_gauges_distinguish_desired_provisioned_active`: desired=4,
  provisioned=4, active=1, parked=3 are reported as distinct values.
- `writer_lanes_track_desired_concurrency`: blocking writer lanes are 1 at
  min, grow to 2 with the probe, return to 1 after a manual decrease —
  writer threads track the desired count instead of `max_workers`.
- `adaptive_pause_resume_with_parked_and_active_workers` and
  `adaptive_cancel_keep_partial_with_parked_workers`: pause/resume and
  keep-partial cancel with parked dormant workers and a lease-holding worker
  complete without stranded ranges or hangs.

## Resource notes

- Provisioning `max_workers` async tasks costs only parked tokio tasks
  (watch-based park, no polling); blocking writer threads are now dynamic
  (task 1.4), so an adaptive 1-worker job holds one writer thread, not
  `max_workers`.
- CPU/RSS in the compare table are process-wide (in-process servers) and
  match the pre-phase-1 ranges; no regression signal.
- Thread-count instrumentation: writer lanes are observable via
  `SegmentedJob::writer_lanes_alive()`; total process threads are not
  directly counted (tokio runtime + blocking pool); the lane gauge plus RSS
  are the recorded proxies.

## Rollback posture

- Fixed remains the default; adaptive is opt-in and unchanged at the config
  surface.
- The provisioning change is contained in `run_segmented`/`worker_loop`
  (spawn `max_workers` tasks; lazy lanes). No new configuration switch was
  added because the gates show no regression; reverting the phase-1 commits
  restores the previous behavior if a future regression appears.
