# Proposal

## Why

KDown's current terminal API can return `Ok(DownloadResult { status: Failed, error: Some(_) })`, and its broadly public modules and pool-only memory budget do not support a defensible production-stability claim. Existing tests and loopback benchmarks provide a base, but end-to-end resource bounds, adverse-network/OS coverage, security and stress gates, and realistic performance regression controls are not yet enforced together.

## What Changes

- **BREAKING**: Make `run` and the awaited `start` task return success only for a verified completed download; return typed cancellation, transfer failure, and infrastructure failure through the error path, retaining diagnostic accounting on failures. Update examples, metrics, consumers, and migration notes.
- **BREAKING**: Narrow the supported public API to documented consumer types; make scheduler, job implementation, I/O, resume, and fuzzing internals private or crate-visible. Review currently documented injection seams explicitly before removal and publish versioned compatibility guarantees and migration guidance for the 0.1 API.
- Introduce a validated, end-to-end transfer-memory budget covering network ingress/client buffering, queued chunks, writer work, and checkpoint state across concurrent jobs, with configured and observed per-component and aggregate high-water telemetry; reject or backpressure workloads that cannot honor the bound.
- Expand deterministic fault/network/HTTP interoperability and cross-platform filesystem regression suites; turn each discovered high-severity production failure into a reproducible test.
- Add dependency auditing, bounded PR property/fuzz/dynamic checks, and scheduled longer fuzz/stress/concurrency checks. Maintain evidence and block production release unless all required gates pass.
- Track loopback and WAN-profile throughput, network amplification, high-water memory, CPU, and concurrency scaling against explicit, versioned same-environment baselines; use fast PR smoke checks and release-blocking scheduled/performance validation.

## Capabilities

### New Capabilities

- `download-outcomes`: Unambiguous success/error/cancellation contracts and typed transfer versus engine failures.
- `consumer-api`: Supported external surface, stability policy, and breaking migration contract.
- `transfer-resource-bounds`: Complete pipeline resource admission, enforcement, and telemetry.
- `reliability-verification`: Real-world failure coverage, regression reproduction, and CI security/dynamic/stress verification.
- `performance-release-gates`: Controlled and WAN-profile measurements, baseline-based regression thresholds, and production readiness decision.

### Modified Capabilities

None. `openspec list --specs` currently reports no main capability specs; prior change artifacts and `KDownSpec.md` are background, not existing main specs.

## Impact

`crates/engine/src/{lib.rs,job,config,http,io,resume,metrics}`, integration and property tests, fuzz targets, benchmarks, `.github/workflows/ci.yml`, `scripts/bench_check.sh`, `README.md`, `KDownSpec.md`, and acceptance/performance documentation. Existing examples and consumers using `DownloadResult.status/error` or public internal modules will require migration. Current `docs/acceptance-v1.md` labels pool tests as bounded-transfer-memory evidence, while `README.md` and `docs/benchmark-profiling.md` explicitly say the pool does not bound Hyper or the transfer path: the new end-to-end guarantee supersedes that narrower claim only after implementation and verification.
