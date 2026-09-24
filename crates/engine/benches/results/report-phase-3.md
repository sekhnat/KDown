# Optimize-transfer-engine-v2 — phase 3 gate (task 3.6)

Recorded 2026-09-24 on the phase-3 completion working tree (tasks 3.1–3.5
committed; `git log` head `4894e26` + gate tests). Baselines:
`baseline-v2-core.md` / `baseline-v2-wan.md`; phase-2 gate:
`report-phase-2.md`; sizing sweep: `report-duration-sweep.md`.

## Correctness gates

- `cargo test -p kdown-engine`: 30 suites, all `test result: ok`, 0 failures
  (including the extended randomized scheduler property suite and the three
  amplification gate tests below).
- `cargo clippy -p kdown-engine --all-targets -- -D warnings`: clean.

## Clean split amplification gate (task 3.6 bounds)

Fixture: 8 MiB whole-file lease under `initial_segment_size = max =
8 MiB` with 4 workers and a 2 ms/chunk delayed origin — idle workers split
the live tail while the first worker streams. Server-emitted payload is
measured at the server (real emission, not requested ranges: a client that
stops reading resets its stream and the count stops with it).

| Variant | Amplification (measured) | Bound |
|---|---|---|
| H1, legacy write path | 1.016× | < 1.10 ✓ |
| H2 (single multiplexed TLS connection) | 1.023× | < 1.10 ✓ |
| H1, pipelined executor + duration sizing (opt-in) | 1.016× | < 1.10 ✓ |

Every variant also asserts **exact hash**, **exact size**, **unique
coverage** (`completed_bytes == file size`), and carries the unconditional
**2.0× regression failure** assert. Baseline for this exact scenario before
phase 3: **2.000×** (documented in the task-0.1 reproducer, which was
`#[ignore]`d until this phase).

### What closed the gap (task 3.2, verified by these tests)

1. **Split boundary respects the receipt high-watermark**:
   `split_tail(…, received_through)` bounds the new lease at
   `max(next_offset, received_through)` — bytes the original request already
   received/queued are never re-requested. The worker publishes
   `received_through` in its progress cell per chunk.
2. **Safe stop at the split-shrunk lease end**: the original worker
   refreshes its (possibly shrunk) lease end on revision wakes
   (`SegmentScheduler::lease_end`) and stops consuming at the inclusive
   boundary; a chunk that SPANS the boundary is truncated — the owned prefix
   is written, the remainder is counted as split waste. This bounds overlap
   regardless of chunk size (a server delivering a whole body as one frame
   cannot overshoot the shrunken lease).
3. The H2 fixture initially exposed a real hazard (single-frame full-body
   responses defeating the chunk-boundary check, measured 3.3×) which the
   truncation fix closes; the fixture itself streams slowly so server
   emission tracks actual client consumption (requested bytes are not
   emitted bytes — a stream reset stops the count).

## Property coverage (task 3.1)

`tests/scheduler_property_tests.rs` — randomized over proptest seeds with
persisted regressions:

- interval normalization and exact subtraction;
- full-drain exact coverage under randomized acquire/report/fail/split/
  release sequences (union == `[0, N)` exactly, invariants after every op);
- **resumed intervals**: any admitted completed prefix keeps
  completed + active-remainder + pending exactly the domain and drains
  exactly;
- **stale generations**: every old-generation operation is rejected and
  mutates nothing (prefix promoted, remainder requeued, post-invalidation
  drain exact);
- **boundary math**: inclusive `[start, end]` leases, exclusive
  `next_offset` frontier, exact final partial range;
- **no-double-lease**: splits never include consumed bytes; live leases
  never overlap;
- **ready-work target** (task 3.3): carving reserves unclaimed pending
  leases while bytes permit; union exactness and drain completeness hold
  under randomized 1→4→1 concurrency changes (no lease leak, no overlap).

## Duration sizing (tasks 3.4–3.5)

Opt-in `SegmentSizing::Duration { duration_ms }` implemented and swept
(`report-duration-sweep.md`): at or above the explicit/automatic baselines
on every non-network-bound axis, amplification ≤ 1.021 for the recommended
config (`1000 ms`, ready factor 3), zero retries across all 80 sweep rows.
Explicit/Automatic behavior unchanged; no default changed.

## Exceptions and evidence

- None against the 1.10 bound: all three measured variants sit at 1.016×–
  1.023× with margin, and the 2.0× unconditional failure assert remains.
- The H2 full-body hazard found during gate hardening is recorded above
  with its fix (chunk-spanning truncation) and its pre-fix measurement
  (3.3× with a naive single-frame fixture) — not silently relaxed.
- Remaining known gap (unchanged scope): the H1 `1.016×` residual is the
  in-flight window between a split and the original worker's revision wake;
  bounded by one chunk and counted as split waste (`wasted_bytes`).
