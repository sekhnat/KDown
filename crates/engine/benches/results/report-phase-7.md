# Optimize-transfer-engine-v2 — phase 7 gate (task 7.5)

Recorded 2026-09-25. Evidence files:
`benches/results/phase-7/rate-limit-contention.md`,
`benches/results/phase-7/counters-publish-probes.md`,
`benches/results/phase-7/buffer-sweep/records.md`.

## What phase 7 changed (and rejected)

| Task | Candidate optimization | Verdict |
|---|---|---|
| 7.1 | Apply configured rate limits end-to-end (global + job) on both transfer paths | **RETAINED (bug repair)** — see below |
| 7.2 | Bounded local token leasing to cut limited-mode lock contention | **REJECTED (no-change)** — 156 ns/acquire contended, ~2.6 ms CPU/GiB, ≲ 0.3 % of a chunk cycle |
| 7.3 | Cache-line padding of `WorkerCounters`; `LeaseProgress` publish batching / ordering relaxation | **REJECTED (no-change)** — padding wins ~0.5 ns/op (≲ 0.001 % of cycle); batching adds crash-lag for ~0.1 %; SeqCst kept |
| 7.4 | `read_buffer_size` (frame quantum) 64/256/512 KiB | **REJECTED (no-change)** — WAN flat within dispersion, LAN bimodal; **128 KiB retained** |

## Task 7.1 — the retained repair (three real defects)

1. **The single-stream transfer path never rate-limited at all.** The
   controller's `rate_bucket` was created, mutated by `set_rate_limit`, and
   cloned — but never consumed on the single-stream body path; a below-
   threshold job with a configured limit downloaded unthrottled (8 MiB at
   "1 MiB/s" finished in ~0.02 s). Payload bytes now pass both buckets
   (job + engine-global) before the positional write, with waits stopping on
   cancellation.
2. **`NetworkPolicy::rate_limit` was never read by production code.** All
   buckets started unlimited; only runtime `set_rate_limit` calls had any
   effect. The configured per-job limit now seeds the stable job bucket, and
   a new `EngineConfig::global_rate_limit` seeds the engine-global bucket
   shared by every job of a controller (`with_global_rate_bucket` for
   explicit cross-controller sharing in tests/benches).
3. **`TokenBucket` deficit accounting double-spent tokens.** A deficit
   acquire zeroed the balance and let the tokens accrued during the wait be
   consumed by the *next* acquire for free — the bucket delivered ~2× the
   configured rate in the deficit regime (measured: 8 MiB at 1 MiB/s in
   3.90 s ≈ exactly (size − burst)/2). Fixed with negative-balance
   accounting: the debit stays in the balance and refill climbs back; the
   rate is now exact.

**Verification:** new `tests/rate_limit_tests.rs` (5 tests) — job limit on
the single-stream path, global binding alongside the job limit on the
segmented path, the global bucket shared across two jobs, a live
`set_rate_limit` reaching a running single-stream job, and cancellation
stopping a ~128 s rate wait within 5 s. All four discriminating tests were
verified FAILING before the repair (unlimited path untouched: ~0 ns/acquire
in the 7.2 probe, unit tests unchanged).

## Rejected candidates — measured evidence and unchanged defaults

- **7.2 token leasing:** 16 threads × 40k × 64 KiB acquires against a shared
  limited bucket: 156 ns/acquire (unlimited fast path: ~0). At 16 K
  acquires/GiB that is ~2.6 ms CPU/GiB and ≲ 0.3 % of a ≥ 50 µs chunk
  cycle. Local leasing would complicate live rate invalidation and burst
  fairness for less than the measurement noise. Probe kept (`#[ignore]`).
- **7.3 counters padding:** 16 threads × 225k counter cycles: adjacent
  layout 1.0 ns/op vs padded 0.7 ns/op — a ~0.3–0.5 ns/op win, four orders
  of magnitude below the chunk cycle (a deliberately shared cell shows
  10 ns/op, proving the probe sees real sharing). Publish batching would
  trade crash-lag and flush semantics for ~0.1 %. SeqCst publication
  unchanged; no ordering relaxation attempted.
- **7.4 buffer sweep:** 48 cells (h1/h2 × LAN/WAN × 64/128/256/512 KiB ×
  3 reps, 64 MiB, fixed-4): WAN goodput flat across quanta (H1 ~48.1–48.4
  MiB/s at every size; H2 45.4–48.3 with a non-monotonic 256 KiB blip inside
  shaped-fixture dispersion); LAN bimodal from timer quantization; CPU %,
  context switches and RSS show no quantum-specific signal; zero retries;
  all cells hash-verified. **Default stays 128 KiB.**

## Unchanged defaults (unchanged behavior guarantees)

Unlimited-mode fast check (no lock), Fixed/Adaptive concurrency semantics,
`SegmentSizing`, H2ConnectionPolicy, write budgets, physical prealloc off,
and all prior phase defaults are untouched. The only behavioral change is
that *configured* rate limits (per-job and the new global) now actually
apply — a caller setting them before was silently unlimited.

## Unavailable axes (labeled, not fabricated)

- Syscall and allocation counts are not instrumented; context switches
  (client process, in+vol) stand in as the scheduling-pressure proxy in the
  buffer sweep.
- Peak RSS is the monotonic process VmHWM — coarse per-cell caveat as in
  phases 4–6.
- The 7.2/7.3 probes ran on this host (16 logical cores); numbers are
  host-relative, the conclusions rest on orders-of-magnitude margins.

## Suite status at the gate

`cargo test -p kdown-engine`: 592 passed / 0 failed across 32 suites
(573 pre-phase-6 + 5 origin-coordination + 7 registry unit + 5 rate-limit
+ 2 ignored probes); `cargo fmt --check` clean;
`cargo clippy --all-targets -- -D warnings` clean.
