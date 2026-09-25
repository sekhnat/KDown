# Task 7.2 — TokenBucket limited-mode contention probe (no-change evidence)

Run 2026-09-25, release build, `cargo test -p kdown-engine --lib control::rate_limit -- --ignored --nocapture`.

Method: 16 threads × 40k acquires each, 64 KiB per acquire (the segmented
worker pattern: one acquire per received chunk), shared `TokenBucket`.
Limited mode exercises the mutex + refill arithmetic on every acquire
(rate high enough that no deficit sleep occurs — the critical section is
fully exercised); unlimited mode exercises the atomic fast path.

Results:

| Mode | ns/acquire | Total (640k acquires) |
|---|---|---|
| unlimited (rate 0) | ~0 | 278.5 µs |
| limited (64 GiB/s ceiling) | 156 | 100.3 ms |

Assessment:

- 156 ns per 64 KiB chunk ≈ **2.6 ms CPU per GiB** of payload.
- A worker's chunk cycle (network read + positional write) is ≥ ~50 µs even
  on fast loopback, so at 16 contending workers the lock adds ≲ 0.3 % to the
  cycle; the mutex is never held across sleeps (waits happen in the caller).
- CPU/GiB and burst behavior at these levels leave no measurable margin to
  win: local token leasing would trade runtime invalidation complexity
  (live `set_rate_limit`/`set_global_rate_limit` must reach leased workers)
  and burst-fairness risk for ≤ 0.3 % cycle time.

**Decision: no-change.** The unlimited fast path is untouched (0 ns), and
limited-mode contention is immaterial at realistic chunk sizes. Revisit only
if a future chunk size shrinks by ≥ 10× (sub-µs chunks) or workers scale past
the current per-origin bounds (~16-256). Probe kept as
`probe_limited_mode_lock_contention` (#[ignore]) for re-measurement.
