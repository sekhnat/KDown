# Proposal

## Why

The current `main` CI run (0050ebb, [run 36172416772](https://github.com/sekhnat/KDown/actions/runs/36172416772)) fails on Windows during test compilation and on macOS in an adaptive-concurrency integration test, while Linux, docs, and benchmark smoke checks pass. Separately, request `Debug` output exposes custom header values, concurrency updates report the wrong event, and the shared rate limiter can choose the less restrictive wait. These correctness, security, API, and release-claim gaps should be closed together before the next release without redesigning transfers.

## What Changes

- Fix the observed Windows compile failure: `crates/engine/src/io/positional.rs` uses Unix-only `read_exact_at` in a test without a Windows equivalent; preserve real positional-I/O coverage and add a platform regression. Investigate and repair the repeatedly failing macOS `h1_adaptive_reverts_when_marginal_gain_disappears` integration assertion without suppressing the adaptive-concurrency guarantee. Keep meaningful Linux/macOS/Windows build, test, lint, docs, and benchmark smoke checks and expose actionable failures.
- Test the exact declared Rust MSRV (`1.85` in workspace metadata) in a separate CI job, independently of the normal pinned `1.98.1` toolchain. Preserve `1.85` if the intended dependency set and all relevant targets support it; otherwise establish and document the actual minimum with reproducible dependency resolution.
- Redact **all** custom header values from `DownloadRequest::Debug`, preserving header names, and audit adjacent request/debug/log paths (notably `RequestSpec`, semantic request wrappers, and scripted diagnostics) for the same leak. Continue masking dedicated credential fields.
- Introduce a documented concurrency-specific event emitted only after a manual concurrency update takes effect (report the clamped/applied count); stop using `RateLimitChanged` for concurrency changes and preserve the rate-limit event for actual limit changes.
- Make global/job token-bucket wait arbitration choose the maximum applicable delay in one shared semantic implementation used by both segmented and sequential paths, with no double debit or loss of cancellation responsiveness.
- Make `DownloadController` the canonical name for the existing single/segmented orchestrator. Retain `SingleStreamController` as a deprecated compatibility alias where feasible; update public imports, examples, and documentation. The new event variant is a narrowly scoped public API addition, not a transfer redesign.
- Replace obsolete task-number-only comments in touched and core orchestration code with enduring rationale. Clarify that local synthetic benchmark throughput is environment-specific regression evidence, not a WAN speed guarantee; keep hosted CI free of absolute workstation thresholds. Update README, rustdoc, changelog, and regression coverage.

## Capabilities

### New Capabilities

- `ci-release-hardening`: Supported-platform matrix, reproducible MSRV verification, and actionable CI failures.
- `request-secret-redaction`: Safe-by-default request/header formatting and adjacent diagnostic surfaces.
- `runtime-control-events`: Correct post-update concurrency and rate-limit event semantics.
- `rate-limit-arbitration`: Most-restrictive global/job bucket gating with shared accounting and cancellation.
- `download-controller-api`: Canonical public controller naming and compatibility migration.
- `engineering-documentation`: Durable implementation rationale and appropriately scoped benchmark/release claims.

### Modified Capabilities

None: `openspec list --specs` reports no main capability specs; earlier change-local delta specs are not main specs to modify.

## Impact

Primarily `crates/engine/src/{io/positional.rs,job/controller.rs,job/segmented.rs,control/rate_limit.rs,http,metrics}`, corresponding unit/integration tests, public re-exports and examples, `.github/workflows/ci.yml`, workspace Rust metadata/dependency reproducibility if needed, `README.md`, `docs/benchmark-profiling.md`, and `CHANGELOG.md`. No new protocols, checkpoint changes, `unsafe`, or unrelated public API changes. The macOS failure's mechanism and lowest viable dependency-resolved MSRV remain verification questions; neither is assumed fixed by this proposal.
