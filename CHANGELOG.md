# Changelog

All notable changes to KDown Engine are documented here.

## [Unreleased]

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
