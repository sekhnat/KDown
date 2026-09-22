# Tasks

## 1. Define the HTTP Execution Port

- [x] 1.1 Add `http::execution` with `HttpExecutor`, cloneable `HttpExecution`, `ProbeRequest`/`ProbeOutcome`, `TransferRequest`/`TransferIntent`/`TransferResponse`, `HttpFailure`, and public exports; verify `cargo check -p kdown-engine --all-targets` succeeds and object-safety is covered by a trait-object construction test.
- [x] 1.2 Implement the poll-based `HttpBody`/`HttpBodySource` abstraction with one-response boxing, direct `bytes::Bytes` delivery, one-chunk demand, read-idle timeout, cancellation, and pause/resume; verify focused unit tests prove no prefetch, preserve the same `Bytes` storage, interrupt pending reads, resume the same source after pause, and classify idle timeout.
- [x] 1.3 Add HTTP-private response metadata and centralized status, retry timing, authentication challenge, range, validator, total-size, body-error, and range-overrun classification; verify table-driven HTTP unit tests cover 200/206 intent rules, 401/403/404/408/429/5xx, `Retry-After`, malformed/mismatched ranges, generation conflicts, resets, truncation, and timeout errors.

## 2. Adapt the Production HTTP Transport

- [x] 2.1 Refactor redirect-following execution to retain final URL and HTTP version while keeping raw Hyper response/header/body values inside `http`; verify redirect-chain, credential-stripping, downgrade-rejection, and loop tests in `transport_integration` and `security_tests` pass.
- [x] 2.2 Implement `HttpExecutor::probe` for `HttpTransport`, including HEAD interpretation, policy-controlled `bytes=0-0` validation, authoritative total updates, semantic notices, challenge data, and retry timing; verify real-server tests cover verified ranges, lying range advertisements, probe status mapping, and final metadata.
- [x] 2.3 Implement `HttpExecutor::transfer` for full and ranged intents, returning `HttpBody` only after status/range/generation validation; verify production-adapter tests cover full bodies, `If-Range`, nonzero-range 200 responses, invalid `Content-Range`, validator changes, and body overruns before sink delivery.
- [x] 2.4 Update direct transport/range integration tests to exercise the semantic execution API, then make `HeadResponse`, `RangeResponse`, and Hyper body access private or remove them; verify `cargo test -p kdown-engine --test transport_integration --test range_validation_tests` passes and public rustdoc exposes no Hyper response-body type.

## 3. Build the Scripted Adapter

- [x] 3.1 Add `http::scripted::ScriptedHttp` with FIFO expected calls, relevant request matching, a request log, actionable mismatch failures, and an all-steps-consumed assertion; verify unit tests cover probe/transfer ordering plus mismatches in URL, headers, policy, range, totals, and validators.
- [x] 3.2 Add scripted success/failure outcomes, `Chunk`, `Fault`, `IdleTimeout`, `WaitForCancellation`, `End`, synchronization gates, and bounded unordered range-keyed phases; verify unit tests deliver events in order without sockets or delay, unblock cancellation deterministically, and allow concurrent range calls without arrival-order flakiness.
- [x] 3.3 Create shared semantic conformance cases that run against the scripted and production adapters for status/error mapping, retry timing, range validation, generation changes, and body faults; verify both adapter test variants produce matching `DownloadError` categories and accepted transfer metadata.

## 4. Migrate Controller Probe and Sequential Transfer

- [x] 4.1 Change `SingleStreamController` to store `HttpExecution`, keep existing `new(HttpTransport, EngineConfig)` and `with_metrics` call patterns, and add an explicit execution-injection constructor; verify crate doctests, `examples/download.rs`, benches, and existing construction call sites compile unchanged.
- [x] 4.2 Replace controller-owned HEAD/range probe sequencing with the semantic probe operation, preserve bounded credential-provider and retry policy handling, emit semantic notices, and call `ProbeMetadata::segment_eligible` exactly once; verify scripted tests cover probe retry, auth challenge, advertised-but-broken ranges, authoritative total changes, segmented selection, and sequential fallback.
- [x] 4.3 Migrate fresh, resumed, and retrying sequential transfers to `TransferIntent` and `HttpBody`, while preserving sink/checkpoint/counter/event behavior and durable-prefix retries; verify scripted sequential tests cover success, retryable status with `Retry-After`, auth retry limits, body fault after a prefix, invalid ranged resume, generation change, and exact output bytes.
- [x] 4.4 Preserve cancellation and pause behavior across a pending sequential body read, including checkpoint persistence and resume from the correct offset; verify scripted cancellation tests require no wall-clock sleep and a delayed-Hyper pause/resume integration test passes.
- [x] 4.5 Remove controller-local `probe_with_retry_after`, raw status/header inspection, duplicated range checks, `status_error`, `classify_body_error`, and Hyper `BodyExt` usage; verify `rg -n 'hyper::body::Incoming|http_body_util::BodyExt|RangeResponse|HeadResponse|status_error|classify_body_error' crates/engine/src/job/controller.rs` returns no matches and controller tests pass.

## 5. Migrate Segmented Transfer

- [x] 5.1 Change `run_segmented`, worker loops, and lease transfer to receive cloned `HttpExecution` handles and issue semantic ranged `TransferRequest`s; verify a scripted multi-worker download covers the complete file with no direct `HttpTransport` dependency.
- [x] 5.2 Consume `HttpFailure` for segmented retryability, `Retry-After`, coordinated origin backoff, and generation-change handling while preserving tail-only retry accounting; verify deterministic tests cover 429/503 coordination, retry exhaustion, successful tail retry, and whole-job invalidation on validator change.
- [x] 5.3 Replace Hyper frame reads, local range/body validation, body-error classification, and `DEFAULT_READ_IDLE` with `HttpBody` and configured read-idle policy; verify scripted tests cover backpressure, body fault, overrun, timeout, and cancellation, and confirm no chunk is read before the previous sink write completes.
- [x] 5.4 Add sequential/segmented parity tests for identical HTTP statuses, malformed ranges, validator conflicts, body resets, and idle timeouts; verify both modes report the same structured error categories and retry metadata.
- [x] 5.5 Remove segmented imports/helpers for concrete transport responses, `parse_retry_after`, `validate_range_response`, `classify_body`, Hyper `BodyExt`, and the hard-coded timeout; verify `rg -n 'HttpTransport|RangeResponse|http_body_util::BodyExt|parse_retry_after|validate_range_response|classify_body|DEFAULT_READ_IDLE' crates/engine/src/job/segmented.rs` returns no matches and segmented tests pass.

## 6. Rebalance Orchestration and Protocol Tests

- [x] 6.1 Convert network-neutral retry, response-ordering, cancellation, timeout, and generation-change cases in `single_stream_tests.rs`, `segmented_tests.rs`, and `phase3_exit_tests.rs` to `ScriptedHttp`; verify those test targets pass without starting `TestServer` for the converted cases.
- [x] 6.2 Convert network-neutral bounded-authentication and selected resume orchestration cases to `ScriptedHttp` while retaining socket-dependent credential forwarding and crash/restart cases; verify the scripted cases assert consumed request sequences and `cargo test -p kdown-engine --test resume_tests --test proxy_tests` passes.
- [x] 6.3 Retain real-network coverage for TLS, redirect safety, proxying, connection limits/reuse, HTTP/2 multiplexing, malformed wire behavior, randomized disconnects, crash/restart, and test-server behavior; verify the corresponding `security_tests`, `proxy_tests`, `connection_pool_tests`, `h2_tests`, `transport_integration`, `range_validation_tests`, `randomized_disconnect_tests`, `crash_restart_tests`, and `test_server_integration` targets pass with `HttpTransport`.

## 7. Documentation, Performance, and Final Verification

- [x] 7.1 Update `KDownSpec.md` §32 and crate rustdoc to distinguish the production wire adapter from the semantic execution seam, document pre-body validation and bounded body delivery, and show both compatible production construction and scripted injection; verify `cargo test -p kdown-engine --doc` passes.
- [x] 7.2 Run the existing throughput benchmark before and after the seam migration, confirm the hot path has only per-operation/per-response boxing and no per-chunk copy or queue, and investigate any regression beyond benchmark variance; verify `cargo bench -p kdown-engine --bench throughput` completes with recorded comparison results.
- [x] 7.3 Run `cargo fmt --all --check`, `cargo clippy -p kdown-engine --all-targets -- -D warnings`, and `cargo test -p kdown-engine --all-targets`; verify all commands pass and a final source search shows job modules contain no Hyper body/frame types or concrete transport response types.
