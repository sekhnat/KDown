# Optimize-transfer-engine-v2 — phase 8 gate (tasks 8.1–8.2)

Recorded 2026-09-25. Evidence:
`benches/results/phase-8/alloc-compare/records.md`.

## Task 8.1 — logical-only vs opt-in physical preallocation

Bench: new `throughput --alloc-compare` mode — 256 MiB H1 download, fixed-4
workers, `preallocate_output` (logical sizing) in BOTH arms, arms differing
only in `preallocate_physical`; 3 reps per cell; startup latency measured as
start → first credited byte (2 ms poll granularity, labeled); tmpfs axis
targets `/dev/shm`, project-fs axis the repo filesystem (NVMe-class here).

| Storage | Policy | first byte (ms) | Wall (s) | Verify |
|---|---|---|---|---|
| tmpfs | logical | 14 | 0.08–0.12 | hash ok |
| tmpfs | physical | 43–46 | 0.11–0.15 | hash ok |
| project-fs | logical | 14–15 | 0.08–0.13 | hash ok |
| project-fs | physical | 41–44 | 0.12–0.15 | hash ok |

Findings:

1. **Physical reservation costs ~30 ms of startup latency per 256 MiB**
   (~120 µs per 100 MiB, linear in `fallocate`) and buys **no measured
   throughput gain** — post-start goodput is identical within dispersion
   (walls at this size are dominated by the same startup/teardown).
2. This host's tmpfs accepts `fallocate` (kernel ≥ 5.x behavior), so the
   EOPNOTSUPP fallback was NOT exercised by the bench cells; the silent
   fallback path is instead covered deterministically by
   `allocation_tests.rs::unsupported_physical_allocation_falls_back_and_completes`.
   The fallback semantics are unchanged: allocation is never a correctness
   dependency, real errors (ENOSPC, EPERM) surface, unsupported ops fall
   back to logical sizing.
3. Sparse behavior is inherent to logical sizing (set_len) and unchanged;
   completed outputs are fully materialized and hash-verified in every cell.

**Decision: retain `preallocate_physical = false` default.** The opt-in
remains available for constrained-filesystem scenarios where callers want
the space reserved up front; no compatibility migration is warranted on
this evidence.

## Task 8.2 — both policies across storage/resume/retry/collision suites

Re-run on this working tree (both policies where parametrized):

- `allocation_tests.rs`: 4 passed (fallback + logical parity + permission
  failure surfacing + both-policy completion loop).
- `checkpoint_seam_tests.rs`: 23 passed (resume/retry seams).
- `crash_restart_tests.rs`: 10 passed (crash/resume + collision paths).

Documented error behavior (unchanged): ENOSPC/permission failures surface as
sink errors and never publish incomplete output; unsupported filesystems
silently fall back to logical sizing; cancellation cleanup is covered by the
cancel suites re-run in the phase-7 gate (592 passed / 0 failed full suite).

**Default retention confirmed:** physical off by default unless a separately
justified compatibility decision is approved (design D7 / §14).

## Unavailable axes (labeled, not fabricated)

- Fragmentation not measured (no FIEMAP extent plumbing in the bench).
- Device-level ENOSPC not injected in the bench (no safe device-level fault
  injection); surfacing is covered by the sink error-path tests.
- Slow-storage axis (network fs / HDD) not available on this host; the
  startup-latency conclusion is proportional (fallocate is linear in size)
  and the throughput-neutrality conclusion applies to fast local storage.
