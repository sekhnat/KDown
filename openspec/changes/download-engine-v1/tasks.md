# Tasks

Design decisions D1–D15 and spec phases (§41) drive this order: workspace → test server (needed by everything) → single-stream core → resume → segmentation → performance → hardening. Each group ends at the phase exit criterion from `KDownSpec.md` §41.

## 1. Workspace and foundations (Phase 1 start)

- [x] 1.1 Create Cargo workspace, `crates/engine` library crate with `rust-toolchain.toml`, `deny.toml`-free minimal lint config, CI skeleton; verify `cargo build` and `cargo test` run green on an empty lib
- [x] 1.2 Add core dependencies per design D15 (tokio, hyper 1.x stack, hyper-rustls/rustls, serde, sha2, thiserror, tracing, bytes, proptest, criterion, axum, tempfile) and verify `cargo build` resolves and `cargo test` passes with a smoke test
- [x] 1.3 Define `config` module: `EngineConfig`, `TransferPolicy`, `RetryPolicy`, `NetworkPolicy`, `IntegrityPolicy`, `ResumePolicy`, `OverwritePolicy` with validation (§8) returning structured `ConfigurationError`; verify unit tests for valid/invalid combinations (min>max workers, zero timeouts) fail validation
- [x] 1.4 Define `error` module: `DownloadError` enum per spec taxonomy with category, retryability hint, origin/status/segment context, and `Redactor` for headers/query/userinfo (D13); verify unit tests assert redaction of Authorization/Cookie/userinfo in Display and log formatting

## 2. Test infrastructure (Phase 1)

- [x] 2.1 Build `tests/support/test_server` deterministic misbehaving HTTP server on axum/hyper with scripted behaviors: correct ranges, no-range, lying Accept-Ranges, 200-on-range, malformed Content-Range, truncated bodies, delayed headers/chunks, resets, 429+Retry-After, redirect chains/loops, mid-download ETag change, unknown length, auth challenge, encoding edge cases (§36.2); verify each behavior via its own integration test hitting it
- [x] 2.2 Add test fixture generators (random files, boundary sizes 0/1/chunk±1/segment±1) and a byte-exact comparison helper; verify a round-trip test uses them

## 3. Single-stream core (Phase 1 exit: reliable sequential downloads under induced disconnects)

- [x] 3.1 Implement `io/sink` trait + `io/file_sink`: temp file naming (`<dest>.part`), prepare/write_at/flush/size/finalize/abort, preallocation on known size, disk-error mapping to structured errors (§14); verify unit tests for write/flush/commit/abort and an out-of-space mapped error
- [x] 3.2 Implement `io/buffer_pool` (bounded pooled 128 KiB buffers, budget by bytes) and verify a test asserts the pool never exceeds `buffer_pool_max_bytes`
- [x] 3.3 Implement `control/cancellation` tokens and `control/rate_limit` hierarchical token bucket (global→job→worker, payload bytes only, burst ≈250 ms, unlimited bypass, runtime limit change) (D11); verify unit tests for token accounting, burst bounds, and mid-flight limit changes
- [x] 3.4 Implement `job/state` state machine (§9.1) with transition validation, monotonicity, and terminal semantics; verify unit tests reject invalid transitions and pause/cancel paths
- [x] 3.5 Implement `metrics/counters` (atomic per-worker counters, fold task) and `metrics/events` (bounded channel, EWMA speed, ETA gating, cadence-batched Progress events) (D12); verify a test asserts completed_bytes uniqueness under simulated re-reads and ETA omission when size unknown
- [x] 3.6 Implement hyper-based `http/transport` `open_full` + probe metadata extraction, `http/redirect` (max 10, loop detection, downgrade denial, cross-origin credential stripping), `http/validators` (ETag/Last-Modified capture and comparison), `http/probe` (HEAD→ranged GET fallback per §10); verify integration tests against test server for redirect policies, probe fallback, and validator capture
- [x] 3.7 Implement `job/controller` single-stream pipeline: probe → prepare → sequential GET → chunked positional writes with backpressure → verify size/hash → atomic rename commit (§14.6) → Completed; verify end-to-end test downloads a fixture from test server byte-exact with correct final path and no temp residue
- [x] 3.8 Implement pause/cancel handling for single-stream (KeepPartial/DeletePartial cleanup) and progress/event wiring; verify pause stops network activity promptly and cancel deletes temp+checkpoint
- [x] 3.9 Implement `control/retry` classification and exponential backoff with full jitter + Retry-After honoring (D10) applied to the single-stream loop; verify induced disconnect test retries from zero without duplication and 404 fails non-retryable
- [x] 3.10 Phase 1 exit: randomized disconnect integration test (server kills connections at random offsets) passes byte-exact final output; run full `cargo test`

## 4. Safe resume (Phase 2 exit: interrupted downloads resume without corruption)

- [x] 4.1 Implement `resume/checkpoint` versioned JSON model (§15.2 fields) with validate-on-load and unknown-field tolerance; verify unit tests for serialize round-trip, corrupt-file rejection, and version gating
- [x] 4.2 Implement `resume/checkpoint_store` trait + sidecar file store with atomic replace (write-temp → rename, optional fsync, per D3/D4 durability modes); verify a test kills the store mid-save loop and asserts the checkpoint is never unreadable
- [x] 4.3 Implement durable-interval tracking: ranges enter the checkpoint only after the selected durability level acknowledges writes; verify ordering test asserts checkpoint never claims unflushed bytes
- [x] 4.4 Implement resume flow: load checkpoint → validate temp file → probe remote → compare validators/size → reconstruct remaining ranges → continue or restart per policy (§15.5); verify integration test interrupts mid-download, restarts, resumes, and produces byte-identical output
- [x] 4.5 Implement generation-change handling on resume and mid-transfer (ResourceChanged events, fail-or-restart policy, never mixing generations §26); verify tests for ETag flip mid-download and validator-mismatch resume
- [x] 4.6 Implement pause checkpointing (§9.3: settle writes, update intervals, persist) and process-restart resume; verify pause→kill→restart→resume integration test yields byte-exact output
- [x] 4.7 Phase 2 exit: crash/restart test suite (§36.3) automates kills at segment writes, checkpoint save, checkpoint rename, verification, final rename; run and pass

## 5. Segmented downloading (Phase 3 exit: exact output across randomized worker failures and range edge cases)

- [x] 5.1 Implement `scheduler/interval_set` (BTreeMap normalized insert/merge/subtract/query per D7); verify unit tests plus property tests for normalization, disjointness, and coverage invariants
- [x] 5.2 Implement `scheduler/lease` (id + generation) and `scheduler/scheduler` (initialize from completed ranges, acquire, report_progress, complete, fail, pending/active/completed queries); verify lease uniqueness test and stale-generation callback rejection test
- [x] 5.3 Implement initial segmentation (oversubscribed clamp formula §12.2) and dynamic splitting (idle worker takes unconsumed tail, excludes read/queued bytes §12.3); verify unit tests for split boundaries and a no-double-lease property
- [x] 5.4 Implement `http/range` request building and response validation (§11.2 rejects: start mismatch, end overshoot, total conflict, body overrun, 200-on-nonzero-range) and `Accept-Encoding: identity` for segmented requests (§11.4); verify integration tests against lying/broken range server behaviors
- [x] 5.5 Implement segmented mode in `job/controller`: eligibility gate (§10.3), worker pool with bounded concurrency, per-worker acquire→range-request→positional-write→report loop, terminal sink error propagation (§14.5); verify multi-worker download of a fixture is byte-exact
- [x] 5.6 Wire coordinated origin backoff into segmented retries (§17.4: 503/429 gate all workers of a job) and per-segment tail-only retry (§17.3); verify induced-503 test shows coordinated delay and no completed-range re-download
- [x] 5.7 Implement single-stream fallback paths (range violation downgrade, unknown-length sequential mode with max-size guard §25); verify integration tests for 200-on-range downgrade and unknown-length download
- [x] 5.8 Implement runtime concurrency reduction (handle API: excess workers settle leases safely) and rate-limit convergence; verify tests adjust both mid-transfer without data loss
- [x] 5.9 Phase 3 exit: property test suite (§36.4 — every byte covered exactly once across randomized acquire/fail/split/retry/pause/resume sequences) plus randomized-failure end-to-end suite (§36.6 boundary sizes incl. >4 GiB sparse test) pass; run `cargo test`

## 6. Performance engineering (Phase 4 exit: saturate target link without excessive CPU/memory)

- [ ] 6.1 Implement connection pooling config (engine-global + per-origin limits, idle expiry, safe broken-connection retry §27) over hyper-util client; verify per-origin limit test with two concurrent segmented jobs
- [ ] 6.2 Tune HTTP/2 behavior: single-connection multiplexing default with additional-connections policy hook (D5); verify H2 segmented test multiplexes and byte-exact
- [ ] 6.3 Reduce hot-path overhead: lock-free counters on chunk path, scheduler lock only at lease boundaries, event batching, no per-byte callbacks (§13.3, §23); verify bench harness shows no hot global lock and memory stays O(workers×buffers) with file size growth
- [ ] 6.4 Build `criterion` benchmark harness with local fixture-server scenarios (localhost H1/H2, throttled, varying workers, prealloc on/off, tmpfs) recording throughput/CPU/wall/RSS/retransferred bytes (§37); verify harness runs and records a baseline
- [ ] 6.5 Phase 4 exit: benchmark on loopback demonstrates near-link saturation at 1 Gbit/s-class throughput with single-digit-percent CPU per §22 targets; record results and set CI regression thresholds (§37.4)

## 7. Hardening (Phase 5)

- [ ] 7.1 Implement proxy support (HTTP/CONNECT, optional SOCKS hook, caller selection, credential redaction) and credential provider callback with challenge handling and no auth-retry loops (§28, §29); verify integration tests through a local CONNECT proxy and credential-provider unit tests
- [ ] 7.2 Implement structured log levels + correlation fields (engine/job/worker/lease/origin/attempt/category) and end-to-end redaction audit; verify a test asserting no secret values appear anywhere in logs at default levels
- [ ] 7.3 Implement security tests: TLS-failure/no-downgrade (mock TLS), redirect credential stripping, path-sanitization utility for Content-Disposition (§21.3), resource-exhaustion bounds (header size, redirect loops, oversized metadata §21.4), SSRF restriction hooks (§21.5); verify each via integration tests
- [ ] 7.4 Set up `cargo-fuzz` targets for URL, Content-Range, ETag, Content-Disposition, checkpoint parsers (§36.5); verify short fuzz runs complete without panics and malformed inputs fail safely
- [ ] 7.5 Implement engine metrics export (§19.5 counters/gauges: jobs by outcome, bytes, retries by category, status counts, range violations, integrity failures, latencies); verify metrics assertions in integration tests
- [ ] 7.6 Platform matrix: run full test suite incl. positional-write, rename-over-existing, and preallocation tests on Linux, macOS, Windows CI; verify green matrix
- [ ] 7.7 Phase 5 exit: acceptance review against §42 v1 criteria (correctness, reliability, performance, API quality, security) with all suites green; run `cargo test` full, `cargo clippy -- -D warnings`, fuzz smoke, bench baseline

## 8. Documentation and delivery

- [ ] 8.1 Write crate docs: public API examples (start/pause/resume/cancel/observe), engine/job config reference, durability mode explanation, event/callback concurrency guarantees (§30), and redaction behavior; verify `cargo doc` builds without warnings
- [ ] 8.2 Add README quickstart (embedding the engine), CHANGELOG entry for 0.1.0, and example program downloading a URL with progress display; verify example runs against the local fixture server
- [ ] 8.3 Final verification: full `cargo test`, clippy clean, docs clean, bench baseline committed; confirm §42 acceptance checklist items each have a passing test or documented benchmark