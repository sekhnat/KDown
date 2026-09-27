# Changelog

All notable changes to KDown Engine are documented here.

## [Unreleased]

### Added
- Release-evidence gate: `release/evidence-manifest.json` declares the gates a
  production-stable verdict needs (three-OS correctness/durability, resource
  bound, interoperability, scheduled fuzz/stress, dependency audit, targeted
  dynamic checks, loopback/low-latency/WAN performance profiles).
  `scripts/evidence_io.py` records per-lane JSON fragments and
  `scripts/release_gate.py` refuses the verdict for missing, failed, stale,
  non-approving, fingerprint-mismatched or unavailable evidence and for
  untriaged high-severity defects; `self-test` proves each blocking rule
  (including that the PR benchmark smoke can never approve a release).
- Dynamic-analysis lanes: targeted Miri and address/thread sanitizer jobs over
  the parser, scheduler and transfer-ledger state machines
  (`scripts/dynamic_checks.sh`), with explicit unavailable-tool handling —
  an unrunnable checker records `unavailable` and fails its job instead of
  reporting a pass, and only a reviewed, expiring exception can waive it
  (`docs/regression-triage.md` §5).
- Multi-axis release performance suite: `throughput --suite
  loopback|low-latency|wan` emits machine-readable throughput, network
  amplification, CPU per byte, engine-accounted transfer-memory high-water and
  job/worker scaling for fixed data on H1 and H2, with pinned RTT/jitter/loss/
  bandwidth profiles, >=5 repetitions, median/spread recording and a stable
  host/config fingerprint. Versioned matched-host baselines and per-axis
  thresholds live in `crates/engine/benches/results/baselines/`; `scripts/bench_gate.py`
  never compares across fingerprints, refuses stale baselines, treats
  unavailable axes as errors, and reports a noise disposition (never a pass) when
  a run's spread exceeds the limit. `scripts/bench_check.sh --release|--baseline|--smoke|--self-test`
  drives it; numbers and blockers are in `docs/performance-report-v1.md`.
- Category verification lanes (`scripts/ci_lane.sh correctness|durability|resource-bound|interoperability`)
  that run the commands and record the release-evidence fragment, so a green run
  on one platform cannot stand in for the three-OS matrix.

### Fixed
- Request `Debug` output (and adjacent scripted/proxy diagnostics) never exposes custom
  header values or proxy-credential URL userinfo; header names remain visible.
  Regression-tested with exact secret sentinels.
- Runtime control events: `set_concurrency` now emits a dedicated `Event::ConcurrencyChanged`
  with the applied (clamped) worker count instead of a spurious `RateLimitChanged`; a
  concurrency request that cannot be applied emits nothing. `set_rate_limit` still emits
  `RateLimitChanged`, now after the limit is applied.
- Hierarchical rate limiting: when both global and per-job buckets gate a payload, the
  slowest (most restrictive) wait now always governs — evaluation order can no longer pick
  the faster bucket's wait. Both transfer paths share one arbitration implementation.
- Windows builds: the positional-write unit tests used a Unix-only read API and failed to
  compile; they now verify positional writes portably, including disjoint writes and
  read-back beyond 4 GiB.
- Adaptive concurrency: a probe is now kept only when its level beats the best observed
  stable-level goodput, and the reference is anchored on converged steady-state windows.
  This prevents a startup-dipped opening window (slow or loaded runners) from locking a job
  at an unhelpful concurrency level for the whole transfer.

### Changed
- The fixture server's `--throttle-mib-s` now emulates one shared link (a single
  slot schedule across connections) with virtual-clock deadline pacing in 64 KiB
  quanta. Per-response pacing let each parallel range response use the full rate
  and the ~1 ms timer wheel capped high rates near 4 MiB/s; a 12.5 MiB/s target
  now measures 12.49 MiB/s and a 4 x 4 MiB concurrent batch takes the 1282 ms
  the shared link allows. `--jitter-ms` adds deterministic seeded per-response
  jitter.
- CI is layered: PR jobs run the three-OS matrix, locked dependency audit,
  bounded property/fuzz-corpus smoke, benchmark smoke and the gate self-tests;
  scheduled/release jobs add targeted Miri and sanitizers, per-target fuzzing,
  long state-machine stress, the loopback+WAN profile gates and a release
  evidence verdict.
- **BREAKING (compatibility-preserving):** `DownloadController` is now the canonical name of
  the job controller, which starts both sequential and segmented transfers.
  `SingleStreamController` remains as a deprecated type alias with a migration message;
  migrate imports to `DownloadController`.
- CI now verifies the declared MSRV (Rust 1.85) on the exact tracked dependency set
  (`Cargo.lock` is tracked; workspace builds run `--locked`), adds a `cargo fmt --check` job,
  and keeps the Linux/macOS/Windows build/test/clippy matrix and benchmark smoke checks.
  Development/test dependency `rcgen` is pinned to 0.14.0 (with `time` 0.3.41) so the whole
  workspace builds on the declared MSRV.
- Documentation now states explicitly that recorded loopback benchmark throughput is
  environment-specific regression data, not an Internet-performance guarantee; hosted CI
  never enforces workstation throughput numbers.

## [0.1.0] — 2026-09-22

Initial library release:

- Validated Rust/Tokio download engine with HTTP/1.1 and HTTP/2 transport.
- Single-stream and segmented range transfers with tail-only retry,
  coordinated origin backoff, and bounded buffer/concurrency policies.
- Atomic temp-file commit, preallocation, positional writes, versioned
  checkpoints, pause/resume, cancellation cleanup, and generation checks.
- SHA-256/SHA-512 verification before commit with exact-size validation.
- TLS validation, custom CA support, downgrade protection, redirect
  credential stripping, HTTP/CONNECT and SOCKS5 proxy hooks, SSRF filters,
  and server-filename sanitization.
- Structured errors, progress snapshots, cadence-batched events, correlation
  logging, sensitive-data redaction, and engine metrics export.
- Deterministic failure server, property/crash/resume suites, cargo-fuzz
  targets, and criterion loopback benchmarks.
