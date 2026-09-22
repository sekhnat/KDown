# Changelog

All notable changes to KDown Engine are documented here.

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
