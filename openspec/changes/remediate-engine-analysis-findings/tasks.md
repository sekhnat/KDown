# Tasks

## 1. P0: Credential Isolation and Auth Parity

- [x] 1.1 Add two-listener dummy-credential HEAD/GET redirect regressions (absolute/relative, same/cross-origin, multi-hop, H1/H2, default/opt-in) in `http_edge_cases_tests.rs` and verify that they expose the current leak without external traffic.
- [x] 1.2 Replace `http/redirect.rs` string origins and response-header-dependent stripping with resolved normalized target origins and latched request-credential policy in `http/transport.rs`; verify new redirect regressions pass and downgrade/loop tests stay green.
- [x] 1.3 Share authenticated redirect-sanitized request context between `job/controller.rs` and `job/segmented.rs`, including `DownloadRequest.authorization`, challenge-provider credentials and proxy separation; verify sequential/segmented protected-resource and redirect tests pass over H1/H2.

## 2. P0: Owned Output and Byte-Exact Publication

- [x] 2.1 Add real-controller tests for six-byte HEAD/three-byte GET with preallocation and unknown-length `new` over stale `STALE-TAIL`, plus interrupted/retry cases; verify failures never publish and clean EOF publishes only acknowledged bytes.
- [x] 2.2 Rework `io/sink.rs` and `io/output_session.rs` so fresh output is exclusive, regular and uniquely owned, and resuming opens only a validated existing output; verify stale `.part` and symlink fixtures cannot alter unrelated scratch files.
- [x] 2.3 Track accepted written coverage (and normal unknown-length EOF) across sequential and segmented jobs, seed only with admitted checkpoint ranges, and require exact coverage/truncation in shared pre-publication verification; verify gap, sparse/preallocation, retry and digest tests under both transfer modes.
- [x] 2.4 Bind `io/publish.rs` to the verified output identity and enforce or document a trusted-directory precondition where descriptor-bound publication cannot be guaranteed; verify entry-swap, hardlink/symlink, no-clobber and replacement fixtures on supported platforms.

## 3. P0: Trustworthy Resume and Storage

- [x] 3.1 Extend `resume/checkpoint.rs` with versioned owned temp identity and covered-byte verification data; implement conservative legacy restart/failure under the destination lease and verify checkpoint-format, migration and crash/restart tests.
- [x] 3.2 Tighten `resume/flow.rs` and `http/validators.rs` so admission requires paired comparable strong ETag or eligible Last-Modified plus verified opened-file identity/content; verify ETag disappearance, weak/no validators, changed Last-Modified and fully covered checkpoint cases never publish stale bytes without re-fetching.
- [x] 3.3 Harden `resume/checkpoint_store.rs` sidecar/temp creation, permissions and safe path operations against symlink or replacement, with explicit filesystem trust rules; verify unrelated scratch files and secret-bearing sidecars are not exposed by default-store save/load/cleanup tests.

## 4. P1: Durability, Resource Budgets, and Job Controls

- [x] 4.1 Route sequential cadence, pause and segmented checkpoint saves through shared mode-aware data-sync-then-store ordering; verify injected sync/save failure, pause and restart tests for Durable vs Performance mode.
- [x] 4.2 Replace `Checkpoint::serialized_size_estimate` with a checked bound for every JSON field including escaped validators, and enforce cap/reservation before both file and injected-store saves; verify large ETag, escaping, overflow and ledger high-water tests.
- [x] 4.3 Introduce RAII active-job permits at `DownloadController::start` using immediate typed rejection at `max_active_jobs`, with no pre-admission artifact writes; verify cap under concurrent jobs and permit release after success, failure, cancellation and abort.
- [x] 4.4 Thread one monotonic `job_deadline` and cancellation signal through probe, HTTP header/body waits, retry/backoff, worker waits and chunked verification; verify bounded expiry/cancel latency under a stalled header, `Retry-After`, paused workers and slow bodies.
- [x] 4.5 Define and test the publication commit boundary so an expiry before commit fails safely while success after an atomic commit is reported truthfully; verify fault-gated commit/deadline races and artifact disposition.

## 5. P2: Metrics, Events, and Secret-safe Diagnostics

- [x] 5.1 Correct `TransferAccounting::wire_amplification` to count received payload once and document reused-byte denominator separately from benchmark server-emitted amplification; verify 150/100/50 = 1.5 and real retry/resume tests in both modes.
- [x] 5.2 Add a per-job terminal signal to `EventStream` so retained handles do not keep `next()` pending forever; verify completed, failed, cancelled, lagged and handle-retained subscriber tests.
- [x] 5.3 Redact URL userinfo and all query values in `DownloadRequest`, `RequestSpec`, error and event diagnostics without changing transport URLs; verify sentinel-secret unit/integration tests for parse failures, redirects and caller-marked keys.
- [x] 5.4 Remove raw original/final URLs from new default checkpoint persistence where not required, restrict sidecar/temp permissions, and document custom-store trust; verify signed-URL disk-content/permissions tests and safe migration/restart behavior.

## 6. Release Evidence and Documentation

- [x] 6.1 In `scripts/release_gate.py`, check every prerequisite's timestamp and candidate commit before satisfaction and require a revision for production verdicts; verify `self-test` includes old/wrong-commit smoke, missing commit, future evidence, equivalence and reviewed-exception cases.
- [x] 6.2 Update `docs/acceptance-v1.md`, `README.md`, migration/API notes and `docs/performance-report-v1.md` to describe corrected guarantees, checkpoint incompatibility, trusted-directory boundary, deadline/admission/event behavior and limits of historical performance/RSS evidence; verify examples and doc links resolve.
- [x] 6.3 Repair `lib.rs` reference to missing `KDownSpec.md`, enable `[lints] workspace = true` for `crates/engine`, and strengthen supported-signature external consumer checks; verify `cargo check --locked --workspace --all-targets`, rustdoc, lint and consumer fixture pass without disabling relevant lints.

## 7. Cross-platform Acceptance Before Stability Claim

- [ ] 7.1 Add isolated failure fixtures and targeted integration/fault/property suites to CI for all P0/P1 cases with recorded seeds, dummy credentials and scratch-only files; verify the affected Linux/macOS/Windows jobs run or surface explicit blockers.
- [ ] 7.2 Run and record full locked workspace tests plus scheduled fuzz, dependency audit, dynamic/concurrency and crash/restart checks on supported runners; verify each required candidate-linked release fragment exists or is explicitly blocked with a reviewed exception.
- [ ] 7.3 Re-run controlled matched-host WAN/loopback benchmark and process-RSS observation for the exact candidate, inspect noise and per-axis budgets, and run `release_gate.py check --commit <candidate>`; verify no production-stable claim is issued unless all corrected prerequisites and approvals pass.
