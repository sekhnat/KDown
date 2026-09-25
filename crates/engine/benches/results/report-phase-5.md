# Optimize-transfer-engine-v2 — phase 5 gate (task 5.4)

Recorded 2026-09-25 on the phase-5 completion working tree (tasks 5.1–5.3
implemented) against the phase-4 gate tree (`report-phase-4.md`, commit
recorded there). Raw records: `benches/results/phase-5/protocol-compare/records.md`.

## What phase 5 changed

- **Task 5.1** — protocol instrumentation (`http::HttpProtocolStats`, shared by
  connector and transport): physical TCP/TLS establishments counted separately
  from logical requests (HTTP/1.x requests vs HTTP/2 streams), each labeled
  with its actually negotiated protocol (TLS ALPN / response version). H2
  flow-control stall data and peer stream limits are **not exposed** by
  hyper-util's legacy client; `H2_FLOW_CONTROL_INSTRUMENTED = false` and
  `h2_flow_control_wait() -> None` label that axis unavailable instead of
  fabricating values. Tests reconcile H1 connections/requests and H2
  streams/requests against the isolated fixture server's accepted-connection
  and served-request counters (exact deltas on both axes).
- **Task 5.2** — HTTP/1 adaptive growth is gated by marginal useful benefit
  (existing controller gain/hysteresis), retry/throttle pressure, and the
  existing `ConnectionLimits`: the controller's effective maximum never
  exceeds the per-origin/global connection allowance, so growth probes are
  never wasted on workers that could not obtain a connection permit. Tests:
  permits are never exceeded under many concurrent H1 requests, and desired
  reverts to the minimum once an additional connection stops helping.
- **Task 5.3** — HTTP/2 keeps the single multiplexed socket by default while
  stream concurrency grows: the connection allowance does **not** cap H2
  adaptive growth. Automatic additional H2 sockets stay off (no measured
  flow-control bottleneck evidence and no exposed peer stream limit — the
  recorded unsupported state); the explicit `H2ConnectionPolicy::Additional`
  override remains compatible and stays inside its configured cap.

## Fixture fix disclosed (affects interpretation of earlier gates)

The bench harness's H2 fixture server mis-parsed every `Range` header
(`parse_range` did not strip the `bytes=` prefix), so **all earlier H2 bench
cells silently ran single-stream**: ranged requests were answered with full
200 responses and the engine's safe fallback downgraded them. Phase-5 numbers
below are the first segmented-H2 bench measurements; H2 cells in
`report-phase-2/3/4.md` should be read as single-stream comparisons. The
engine's fallback behavior itself was correct in every case (no unvalidated
bytes, exact hashes throughout).

## Method

- Harness: new `throughput --protocol-compare` mode — shaped (16 MiB/s per
  connection/socket chunk) H1/H2 × single-job / two same-origin jobs ×
  fixed-4 / adaptive-1-4, 3 repetitions, 128 MiB per job, fresh transport per
  cell so protocol counters describe exactly that cell; jobs within a cell
  share one transport (one engine, same origin).
- Every run verifies published size and SHA-256; records include goodput,
  wall time, retries, segment requests, desired/active decision traces,
  fallback notices, CPU, peak RSS, and per-cell stream/socket counts.
- Environment: loopback, tmpfs destination, `/proc` CPU and VmHWM sampling;
  dual-job CPU is process-wide (shared across the two jobs).

## Results — shaped 128 MiB per job (medians, [min,max] over 3 reps)

| Cell | Goodput/job (MiB/s) | Sockets | Streams/requests | maxDesired/maxActive | Fairness (dual) |
|---|---|---|---|---|---|
| h1 1job fixed-4 | 48.24 [48.18,48.39] | 5 | 18 H1 req | 4/4 | — |
| h1 1job adaptive | 30.64 [30.43,30.69] | 5 | 20 H1 req | 4/4 | — |
| h1 2job fixed-4 | 48.30 [48.23,48.36] | 10 | 36 H1 req | 4/4 | ≥0.998 |
| h1 2job adaptive | 30.43 [30.26,30.62] | 10 | 40 H1 req | 4/4 | ≥0.994 |
| h2 1job fixed-4 | 47.64 [47.61,48.42] | **1** | 18 streams | 4/4 | — |
| h2 1job adaptive | 30.51 [30.49,30.67] | **1** | 20 streams | 4/4 | — |
| h2 2job fixed-4 | 47.95 [47.46,48.41] | **2** | 36 streams | 4/4 | ≥0.985 |
| h2 2job adaptive | 30.49 [29.36,30.69] | **2** | 40 streams | 4/4 | ≥0.957 |

- **Protocol-aware socket behavior is now measurable and correct**: HTTP/1
  opens one socket per concurrent worker (5 for one fixed-4 job, 10 for two),
  while HTTP/2 serves the same 16 range requests as multiplexed streams over
  **one** socket per job — including while adaptive concurrency grows to 4.
- **Segmented H2 reaches H1 parity** once the fixture actually honors ranges:
  47.6–48.4 MiB/s H2 vs 48.2–48.4 MiB/s H1 (fixed-4), and 30.5 vs 30.6
  MiB/s adaptive. The previously recorded 12.0 MiB/s "H2 shaped" cell was the
  single-stream fallback, not segmented H2.
- **Fairness** between two same-origin jobs is ≥ 0.985 in fixed cells and
  ≥ 0.957 in adaptive cells (the worst adaptive rep showed one job receiving
  ~1.9 MB of extra wire payload — amplification ≈ 1.014, within tolerance and
  not a claimed gain).
- **CPU** scales with active streams, not workers: H1 fixed 12.5 % (1 job) /
  23.5 % (2 jobs) of one core, H2 fixed 14.8 % / 27.7 %; adaptive cells use
  roughly half during the ramp. Peak RSS stayed 11–16 MiB (process high-water
  mark; bounded by the write budget, not by file size).
- No protocol fallback occurred in any cell (`fallback: none`); wire
  amplification ≈ 1.000–1.014 with zero retries everywhere.

## Protocol-specific decision traces (sampled every 50 ms)

- Adaptive cells ramp `1→2→3→4` on the controller cadence (~1 s per step) on
  both protocols, e.g. `1@0.05s→2@1.02s→3@2.04s→4@3.06s`, with
  maxDesired = maxActive = 4 — growth is real leases, not desired-only.
- Fixed cells stay at `4@0.05s` throughout.
- H1 socket counts follow desired exactly (5 = 4 workers + probe connection);
  H2 socket counts do not grow with desired (1 socket serves 18–20 streams).

## Comparison with the phase-4 gate

- Fixed-mode shaped cells: H1 48.2 vs 48.2 (parity); H2 47.6–48.4 vs the
  previously recorded 12.0 — the difference is the fixture fix disclosed
  above (the phase-4 H2 cell measured single-stream throughput), not an
  engine change. No fixed-mode regression is attributable to phase 5.
- Adaptive shaped cells: H1 30.6/30.4 (1 job) matches phase 4's 30.6;
  H2 30.5 (1 job) likewise corresponds to the same single-stream-fallback
  number now produced by a segmented transfer.
- Phase 5 adds no new defaults: fixed mode, `H2ConnectionPolicy::Single`, and
  the connection caps are unchanged; the H1 growth gate and H2 stream growth
  apply to the opt-in adaptive mode only.

## Gate decision

1. **Keep the protocol-aware gates**: the H1 connection-permit clamp and the
   H2 stream-not-socket growth policy are retained for adaptive mode. The
   instrumentation is permanent (observability contract), not a switch.
2. **Automatic additional H2 sockets stay unavailable/off**: without exposed
   flow-control or peer-stream-limit data there is no measured bottleneck
   evidence; the explicit `Additional` override remains the only way to get
   more H2 sockets, and it stays within its configured cap (verified ≤ 4 in
   the compatibility cell).
3. **No fixed-mode or single-stream regression** on any measured axis; the
   dual-job fairness cells show no starvation.
4. The H2 fixture bug is fixed at the source (`parse_range`), so future gate
   reruns of `--adaptive-compare`/`--storage-compare` will produce segmented
   H2 numbers and must not be compared 1:1 with pre-phase-5 H2 records.

## Unavailable axes (labeled, not fabricated)

- H2 flow-control stall/window data: not exposed by hyper-util's legacy
  client — `H2_FLOW_CONTROL_INSTRUMENTED = false`, `h2_flow_control_wait()`
  returns `None` (asserted in tests).
- Peer H2 stream limits (`SETTINGS_MAX_CONCURRENT_STREAMS`): not exposed
  through the same client; cannot be probed, so the one-socket default stands
  on multiplexing behavior rather than negotiated limits.
- Scheduler lock wait, syscall and allocation profiling remain unavailable on
  this host (unchanged from earlier phases).
