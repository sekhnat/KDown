# Optimize-transfer-engine-v2 — phase 6 gate (task 6.5)

Recorded 2026-09-25 on the phase-6 completion working tree (tasks 6.1–6.4
implemented). Raw records: `benches/results/phase-6/origin-compare/records.md`.

## What phase 6 changed

- **Task 6.1** — normalized final-origin identity
  (`control::origin::normalized_origin`): lowercased scheme + case-folded
  host (trailing dot stripped) + effective port (scheme default included),
  userinfo stripped. Deliberately distinct from the connector's physical
  pool key (`http::connect::origin_key`, which embeds the proxy). Non-ASCII
  (IDN) hosts are case-folded but not punycoded — matching what the engine
  actually passes to TLS/DNS today; documented limitation.
- **Task 6.2** — controller-shared `OriginRegistry`: per-origin fair FIFO
  request slots (cancellation-aware admission, RAII release on
  success/failure/cancel), wired into both the segmented worker path and the
  single-stream controller path. The connector's `ConnectionLimits` remain
  authoritative for physical sockets; origin admission sits above them and
  never holds a physical permit while waiting.
- **Task 6.3** — 429/503 throttle feedback is `RetryClassifier`-capped
  (`retry_after_max`) and propagated to the SHARED origin deadline: any
  job's throttle delays every same-origin peer's next request. Recovery
  after cooldown is a plain probe; per-job retry bounds remain the limit on
  retries. The per-job `origin_backoff_until` gate stays as the fallback and
  now also uses the capped value.
- **Task 6.4** — registry state is bounded: inactive-TTL eviction plus a
  hard size cap that never evicts entries with live permit holders (least
  recently touched idle entry goes first).

## Method

- Harness: new `throughput --origin-compare` mode — H1 fixture that answers
  the first request(s) with `503` + `Retry-After: 1` (budget GLOBAL across
  connections), 64 MiB per job, 3 repetitions per cell.
- Variants: `shared` (engine-shared registry, default behavior) vs
  `fallback` (`OriginRegistry::disabled()` — per-job backoff only, injected
  via the new opt-in `SingleStreamController::with_origin_registry`).
- Topologies: `same` (both jobs on one origin) and `mixed` (each job on its
  own throttled origin, to prove no cross-origin penalization).
- Every run verifies published size and SHA-256; records include per-job
  wall (measured start→join — `DownloadResult.elapsed` excludes the probe
  phase and would hide probe-phase throttle latency), aggregate goodput,
  server-side 503/request/connection counts, and the registry's
  throttle/success event traces.

## Results (medians over 3 reps; walls in seconds)

| Cell | Job walls | Aggregate (MiB/s) | 503s | Requests | Fairness | Registry thr/ok |
|---|---|---|---|---|---|---|
| shared/same/1job | 1.06 | 58.5 | 1 | ~12 | — | 1/10 |
| shared/same/2job | 1.05 / 1.07 | 113.4 | 1 | 28 | 0.96–1.00 | 2/50 |
| shared/mixed/2job | 1.05 / 1.07 | 113.1 | 2 | 29 | 0.96 | 2/25 |
| fallback/same/1job | 1.03 | 60.7 | 1 | ~12 | — | n/a (disabled) |
| fallback/same/2job | 0.02–0.06 / 1.02–1.07 | 116.5–121.5 | 1 | ~23 | 0.02–0.06 | n/a |
| fallback/mixed/2job | 1.03 / 1.07 | 113.0 | 2 | 29 | 1.00 | n/a |

Server-side arrival timelines (shared, 1 job) confirm the coordination:
`0.000s` (the 503), then every subsequent request at `≥ 1.001s` — nothing
touches the origin during its cooldown window.

## Findings

1. **The shared policy does what the spec requires**: after any job's 429/503,
   the origin receives ZERO further requests until the coordinated window
   passes (fallback: the non-throttled peer keeps requesting through the
   window). Both variants eventually complete byte-exact.
2. **No cross-origin penalization**: mixed-origin cells behave identically
   under shared and fallback (one window per origin, jobs in parallel) — the
   shared registry never delays an unrelated origin.
3. **Cost of sharing**: in the same-origin race, the peer job also waits out
   the window, so aggregate goodput is ~3–7 % lower in these micro-cells
   where a 1 s throttle dominates a 0.06 s transfer. The peer's outcome
   fairness improves sharply (0.96–1.00 shared vs 0.02–0.06 fallback) because
   both jobs observe the same constraint instead of one racing ahead.
4. **No unbounded retries**: failures stay at 1 per origin per window
   (fallback identical); per-job retry bounds cap everything else. A
   persistently-throttling origin exhausts structurally (e2e test).
5. One transient failed cell (fallback/same/2job, both jobs unpublished) was
   observed once during fixture bring-up and did not reproduce across the
   final 3-rep matrix or the deterministic e2e suite; it is recorded here for
   honesty, not excluded silently. The deterministic tests
   (`origin_coordination_tests.rs`) cover the same paths with exact timing
   assertions and pass repeatedly.

## Gate decision

1. **Retain the shared registry as the default.** The measured cost is the
   intended coordination delay; the origin is protected during its cooldown
   window and job outcomes are symmetric. No regression outside the intended
   behavior was observed.
2. **Keep the per-job backoff fallback selectable** via
   `with_origin_registry(OriginRegistry::disabled())` (design D6's
   compatibility switch). It remains the behavior when the registry is
   absent/unparseable-origin jobs.
3. Provisional registry defaults (256 request slots per origin, 1024
   retained entries, 5 min idle TTL) are recorded here; they never bind
   below the legal `max_active_jobs × max_workers` request population.
4. The dynamic-ceiling refinement (lowering per-origin request slots on
   repeated throttles) is recorded as a future option, not implemented in
   this phase; the deadline mechanism covers the spec scenarios.

## Unavailable axes (labeled, not fabricated)

- H2 flow-control wait and peer stream limits remain uninstrumented
  (unchanged from phase 5); the gate ran on H1 fixtures.
- Server-side per-connection congestion state (e.g., real 429-budget
  middleware) was not simulated beyond the first-N 503 fixture; the
  `Retry-After` handling is covered deterministically by the e2e suite.
