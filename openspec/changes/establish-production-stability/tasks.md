# Tasks

## 1. Typed terminal outcomes

- [x] 1.1 Add completed-download, typed transfer/infrastructure failures, cancellation summary, and shared partial accounting in `job/controller.rs`/`error.rs`; verify unit tests distinguish all terminal branches and preserve redacted diagnostics.
- [x] 1.2 Convert `DownloadController::run` and `start` task results to success-only-on-verified-publication, keeping task-join failure distinct; verify direct and spawned controller tests assert `Err` for range exhaustion, integrity failure, disk failure, and cancellation.
- [x] 1.3 Route every terminal branch through once-only metrics/event updates, including infrastructure errors; verify `metrics_tests` and lifecycle tests report exactly one terminal count with no `Ok(Failed)` path.
- [x] 1.4 Update the example downloader and user-facing result documentation to match the new error types; verify examples build and doctests pass.

## 2. Consumer API and migration

- [x] 2.1 Record a concrete whitelist of externally supported imports, signature dependencies, and existing advertised scripted/checkpoint seams; verify an external fixture using the whitelist compiles against the current crate before visibility changes.
- [x] 2.2 Re-export necessary consumer-facing types from `lib.rs` and make scheduler, job internals, I/O, resume, and fuzz modules inaccessible as implementation modules; relocate internal integration/property tests and verify workspace tests and external facade tests compile.
- [x] 2.3 Retire the advertised `HttpExecution`/`ScriptedHttp` external injection seam and keep deterministic scripted adapters internal; verify internal conformance tests still run and a downstream fixture cannot import the retired seam.
- [x] 2.4 Publish supported API/semver/MSRV policy and 0.1 migration table (old result checks, import paths, and removed injection hooks); verify external consumer examples compile and a documented API compatibility check detects signature drift.

## 3. End-to-end transfer-memory admission

- [x] 3.1 Specify/validate `EngineConfig` per-job and aggregate pipeline budgets plus component caps and minimum feasible frame/checkpoint sizes; verify zero, overflow, and contradictory-limit config tests fail before network activity.
- [x] 3.2 Implement a shared per-job/controller reservation ledger with fair cancellation-aware admission and typed oversize refusal; verify concurrency, peak accuracy, no double-charge on ownership transfer, and drop/abort release tests.
- [x] 3.3 Bound HTTP/1 and HTTP/2 ingress/client buffers, header metadata, windows, stream/connection counts and frame admission; verify adversarial oversized frames and slow-reader H1/H2 tests either remain in-budget or fail explicitly. If client internals cannot be bounded, restrict/replace affected transport mode and verify the fallback.
- [x] 3.4 Carry owned frame reservations through sequential/segmented reads and the default per-worker writer lanes; verify slow-disk and cancel tests keep held/queued bytes within job/controller caps and never claim unacknowledged bytes completed.
- [x] 3.5 Integrate the optional shared write executor queue/in-flight cap into the single ledger without charging moved `Bytes` twice; verify many-job slow/failing-writer tests enforce aggregate bounds and drain reservations.
- [x] 3.6 Bound in-memory checkpoint ranges, parsing/serialization and queued save state before allocation; verify oversized/corrupt checkpoint tests fail safely without durable-range overclaim or memory-cap breach.
- [x] 3.7 Expose configured/effective limits, current bytes and per-component/total high-water in job and engine metrics with documented lifetime/scope; verify live/terminal snapshots, failure cleanup, JSON export and no unknown-buffer-as-zero reporting.
- [x] 3.8 Run adversarial end-to-end ingress/writer/checkpoint/multi-job profiles with external RSS observations; verify managed peaks never exceed configured caps, explain RSS overhead, and update the former pool-only claim only after proof holds.

## 4. Reliability and platform coverage

- [x] 4.1 Add deterministic latency/jitter/loss/bandwidth/reset controls to `tests/support/test_server.rs` with recorded seeds; verify replay yields identical error/output/accounting under both sequential and segmented modes.
- [x] 4.2 Extend HTTP tests for absent, ignored, mismatched and changing Range/validators plus redirected cache/CDN and authenticated proxy cases; verify byte-exact verified publication or safe typed failure with no corrupt final output.
- [x] 4.3 Add delayed, short-write, full-disk and fail-after-ack sink tests at high concurrent-job counts, cancellation and checkpoint/restart boundaries; verify durable ranges, bounded retries/memory, and no destructive publication.
- [x] 4.4 Extend filesystem matrix cases for Linux/macOS/Windows path, sharing/lock, sparse/preallocation, rename/no-replace and crash/restart differences; verify targeted tests run on all three CI OS jobs and unsupported behaviors fail safely with documented evidence.
- [x] 4.5 Add a seed/fixture retention and severity-triage process for discovered production bugs; verify a deliberately replayed failure becomes a named regression test with deterministic reproduction instructions.

## 5. Verification CI and release evidence

- [ ] 5.1 Add locked dependency audit and bounded property/fuzz-corpus jobs to PR CI, preserving the three-OS correctness matrix; verify CI config and run the audit/property/corpus commands successfully or record actionable advisory blockers.
- [ ] 5.2 Add scheduled/release per-target real fuzz jobs with time limits and retained corpora/seeds plus long-running scheduler/checkpoint/commit/concurrent-job stress; verify a failed seed makes its job red and produces a replay artifact.
- [ ] 5.3 Add applicable targeted Miri and sanitizer/equivalent concurrency/dynamic jobs on supported runners with explicit unavailable-tool handling; verify checker failures or missing mandatory evidence block release rather than silently pass.
- [ ] 5.4 Assemble a machine-checkable release evidence manifest covering correctness, durability, resource bound, interoperability, stress, security and known high-severity defect triage; verify missing/failed/stale mandatory evidence prevents a production-stable verdict.

## 6. Performance and production gate

- [ ] 6.1 Extend `benches/throughput.rs` and benchmark support to emit machine-readable throughput, amplification, CPU/byte, managed-memory high-water and job/worker scaling for fixed data and H1/H2; verify each axis has a non-placeholder result in loopback runs.
- [ ] 6.2 Add pinned low-latency and WAN emulation profiles (RTT/jitter/loss/bandwidth), controlled disk conditions and >=5-run median/spread recording; verify profile inputs, host/config fingerprint and results can be replayed on a matched runner.
- [ ] 6.3 Capture and review versioned matched-environment per-profile baselines and numeric thresholds, extending `scripts/bench_check.sh` without comparing unrelated shared runners; verify injected throughput, amplification, CPU and memory regressions independently fail comparison.
- [ ] 6.4 Wire PR benchmark smoke and scheduled/release loopback+WAN comparisons into the evidence manifest; verify missing WAN data or a violated threshold prevents a production-stable verdict while PR smoke alone cannot approve release.
- [ ] 6.5 Run the complete cross-platform and release verification suite, document the actual results and unresolved blockers in acceptance/performance reports, and verify a production-stable claim is made only if every required gate passes with no known unmitigated high-severity defects.
