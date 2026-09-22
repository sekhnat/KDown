# Design

## Context

See `proposal.md` for motivation and `specs/http-execution-seam/spec.md` for the behavior contract.

`SingleStreamController` currently stores and clones `HttpTransport`. It performs the HEAD probe interpretation, optional `bytes=0-0` verification, authentication-stage handling, segmentation calculation, sequential response validation, status classification, `Retry-After` parsing, Hyper frame reads, timeout handling, and body-error classification. `run_segmented` and each worker also receive `HttpTransport`; workers call the shared range validator but repeat retry timing, Hyper frame consumption, timeout handling, and body-error classification. `RangeResponse::body()` is the direct leak because it returns `hyper::body::Incoming`.

The existing HTTP implementation already owns TLS, proxies, redirects, request framing, connection pooling, HTTP/2 policy, validators, and range parsing. The change therefore adds a semantic execution layer above those mechanics rather than replacing the production transport. The existing one-read/one-write flow, `bytes::Bytes` payloads, error taxonomy, cancellation token, credential-provider stage guard, retry classifier, and real misbehaving server remain constraints.

`KDownSpec.md` §32 already calls for a transport abstraction, but its conceptual interface exposes a `ByteStreamWithResponseMetadata` and says job-level validation consumes that metadata. This change refines the document to separate the production wire adapter from the HTTP execution seam: HTTP-owned validation happens before a transport-neutral body reaches job orchestration.

## Goals / Non-Goals

**Goals:**

- Give controller and worker code one cloneable, transport-neutral HTTP execution handle.
- Make probe and transfer results semantic enough that job code never reads raw HTTP status, headers, frames, or Hyper errors.
- Validate statuses, ranges, validators, and body length in HTTP code before unsafe bytes reach a sink.
- Preserve one-chunk-at-a-time backpressure and pass `bytes::Bytes` through without copying.
- Support deterministic orchestration tests with a real second adapter while preserving production-adapter integration evidence.
- Keep existing `SingleStreamController::new(HttpTransport, EngineConfig)` and `with_metrics` call sites source-compatible.

**Non-Goals:**

- Replacing Hyper/rustls, changing redirect/proxy/TLS/H2 policy, or redesigning connection pools.
- Moving retry budgets, backoff scheduling, credential-provider decisions, job state transitions, sink writes, checkpointing, or scheduler policy into HTTP.
- Replacing real-server tests for protocol and wire behavior.
- Introducing a general virtual clock. Scripted timeout outcomes are immediate; retry tests can use zero-duration retry policy where elapsed time is not under test.
- Changing the public download result, error categories, checkpoint format, or event model.

## Decisions

### 1. Add an object-safe HTTP execution port and cloneable handle

Add an `http::execution` module containing:

- `HttpExecutor`: a `Send + Sync + 'static` port with semantic `probe` and `transfer` operations.
- `HttpExecution`: a cheap cloneable handle holding `Arc<dyn HttpExecutor>` and forwarding those operations.
- Operation request/result types described below.

The trait methods return explicit `Pin<Box<dyn Future<...> + Send + '_>>` futures. This keeps the port object-safe on the supported Rust toolchain without adding `async-trait`. The allocation occurs once per HTTP operation, not once per body chunk.

`HttpTransport` remains the production adapter and implements `HttpExecutor`. `SingleStreamController` stores `HttpExecution`; its existing constructors accept `HttpTransport` and wrap it. An additive constructor accepts `HttpExecution` for scripted or future adapters. `run_segmented`, `worker_loop`, and `transfer_lease` receive clones of `HttpExecution`, so neither transfer path is generic over a concrete adapter and job modules no longer import transport response types.

**Why this over a generic controller:** a generic controller would propagate a type parameter through spawned tasks, segmented workers, examples, and public types. A trait object confines substitution to the boundary and its per-request dispatch cost is negligible beside network I/O.

**Why this over an enum of known adapters:** an enum would make every new adapter an edit to production dispatch and would not be a real port.

### 2. Model probe and transfer as semantic operations

The port accepts request values rather than exposing HTTP responses:

- `ProbeRequest` carries the request specification plus segmentation threshold and range-verification policy.
- `ProbeOutcome` carries final `ProbeMetadata` and HTTP notices needed by orchestration, such as advertised-but-unusable range support.
- `TransferRequest` carries the request specification and a `TransferIntent`.
- `TransferIntent::Full` describes a fresh sequential representation.
- `TransferIntent::Range` carries the inclusive requested range, established total, expected validators, and the conditional/full-response policy needed to distinguish invalid range handling from an `If-Range` generation change.
- `TransferResponse` carries only accepted start/end, total size, validators, and a transport-neutral body.
- `HttpFailure` carries `DownloadError`, optional `Retry-After`, and optional authentication challenge data.

The production adapter performs the HEAD request and, when policy requires it, the validating `bytes=0-0` request inside `probe`. It resolves the final URL, updates range-verification and authoritative-total metadata, and produces a notice when advertised support is unusable. The controller keeps the credential-provider loop: on a semantic challenge it updates request headers and calls `probe` or `transfer` again. It also keeps the retry classifier and sleep because those are job policy, not HTTP mechanics.

For transfer, both sequential and segmented callers invoke `transfer`; only `TransferIntent` differs. HTTP code maps non-success status and retry metadata, validates content range/total/validators, and returns a body only after validation passes. A full response to a nonzero range is classified from the intent as either `InvalidRangeResponse` or `ResourceChanged` before body delivery.

**Why two operations rather than raw `execute`:** probe is a protocol sequence and transfer has stronger pre-body invariants. A generic request/response method would simply recreate the existing leak under new names.

**Why not move retries/authentication entirely into the adapter:** retry budget, backoff, credential-provider calls, counters, warnings, and state transitions are orchestration policy. The seam removes protocol interpretation while preserving those decisions at the job layer.

### 3. Replace Hyper bodies with a demand-driven chunk body

Add `HttpBody`, which owns one pinned boxed `HttpBodySource`. The source exposes a poll-based `poll_chunk` operation yielding `Result<Option<bytes::Bytes>, DownloadError>`. `HttpBody::next_chunk` uses `poll_fn` plus the configured idle timer and cancellation/pause signal. The box is allocated once per response; polling a chunk does not allocate a boxed future, channel node, or replacement buffer.

The Hyper source polls `Incoming` frames internally, discards no data frames, rejects unexpected non-data frames according to the existing protocol policy, and maps Hyper body errors through one classifier. Hyper's `Bytes` is returned directly. A range-limited wrapper tracks delivered length and rejects an overrun before returning the offending chunk. EOF and underflow continue into the engine's exact final size/coverage checks.

The caller awaits one chunk, writes it, updates durable progress, and only then requests the next chunk. This preserves current one-read/one-write backpressure. Cancellation or pause wins a `select` against a pending read; the body remains owned so a pause can resume polling, while cancellation terminates with `DownloadError::Cancelled`. The idle deadline resets for each requested chunk and uses `EngineConfig.network.read_idle_timeout` in both modes, removing the segmented worker's separate 30-second constant.

**Why a poll source over `Stream` or `mpsc`:** a local poll abstraction avoids a new stream dependency, per-chunk boxed futures, a feeder task, and hidden queue capacity. It also lets the HTTP layer normalize Hyper and scripted bodies without exposing frame types.

### 4. Centralize protocol classification in the HTTP module

Move or consolidate these responsibilities behind `HttpExecutor`:

- probe status mapping and challenge extraction;
- transfer status mapping, `Retry-After`, and challenge extraction;
- range response validation, including 200/206 intent rules;
- validator and established-total conflicts;
- range body overrun;
- body reset/truncation/timeout classification;
- final URL and HTTP version capture needed by probe metadata.

`http::range` will validate an HTTP-private metadata view rather than public `RangeResponse`. `RangeResponse`, `HeadResponse`, raw headers, and body access become private production-adapter details. The controller removes `probe_with_retry_after`, `status_error`, `classify_body_error`, duplicated range checks, and the local segmentation formula. Segmented workers remove `parse_retry_after`, direct range validation, `classify_body`, Hyper imports, and `DEFAULT_READ_IDLE`.

Job code still decides whether an `HttpFailure` is retryable, coordinates origin-wide backoff, limits auth stages, emits events, accounts wasted bytes, and chooses the durable retry offset.

**Why keep `DownloadError` as the semantic error:** the retry classifier, result API, metrics, and event records already use it. Adding an intermediate duplicate taxonomy would create another mapping without improving isolation.

### 5. Use one HTTP-owned segmentation eligibility path

The production probe adapter updates `ProbeMetadata.range_verified`, `content_range_total`, `total_size`, and `accept_ranges` after validation. The controller then calls `ProbeMetadata::segment_eligible(threshold, verify_range_support)` exactly once and does not reconstruct its formula.

An unusable validation response produces metadata with verified support disabled plus a semantic notice, preserving sequential fallback. A transport failure during validation is represented consistently by probe policy: capability failures that prove ranges unusable fall back; ordinary transport failures retain their structured failure so the controller's retry policy can decide rather than silently downgrading every failure.

**Alternative considered:** leave the validating request in the controller and only abstract body streaming. That would retain protocol sequencing and the duplicate eligibility bug, so it does not create the intended deep seam.

### 6. Add a deterministic scripted adapter as a first-class test utility

Add `http::scripted` with a small, dependency-free `ScriptedHttp` implementation of `HttpExecutor`. It is intended for engine and downstream orchestration tests and is available without a network listener. A script contains expected semantic calls and consumed outcomes:

- expected probe policy, URL, relevant headers, transfer intent, range, total, and validators;
- successful probe or transfer metadata;
- `HttpFailure` with retry timing or authentication challenge;
- body events: `Chunk(Bytes)`, `Fault(DownloadError)`, `IdleTimeout`, `WaitForCancellation`, and `End`;
- optional synchronization gates for concurrent worker tests.

A mutex protects a FIFO script and request log. Each call atomically matches and consumes its next step; mismatches return an explicit protocol-style test failure containing expected and observed call summaries. Segmented tests that do not care which worker arrives first can use a bounded unordered phase keyed by requested range, while phase boundaries and each matched response remain deterministic. The adapter exposes an assertion that all required steps were consumed.

`IdleTimeout` immediately returns the same semantic timeout error as the production body wrapper. `WaitForCancellation` resolves only when the supplied token is cancelled, so cancellation tests require no sleep. Chunks are stored and yielded as `Bytes`, preserving the production ownership model.

**Why keep the adapter in the engine rather than only in individual test files:** it establishes a real second adapter, prevents each suite from inventing a different mock contract, and lets downstream embedders test their orchestration against the same seam. It performs no work and opens no sockets unless constructed.

### 7. Split orchestration evidence from protocol evidence

Refactor network-neutral cases in `single_stream_tests.rs`, `segmented_tests.rs`, `phase3_exit_tests.rs`, selected resume/auth cases, and controller unit tests to use `ScriptedHttp`. Prioritize retry exhaustion and ordering, body fault after a prefix, retry offsets, cancellation during a pending read, bounded auth stages, timeout classification, generation changes, and sequential/segmented classification parity.

Keep the real test server for:

- `transport_integration.rs` and malformed response framing;
- `range_validation_tests.rs` where the Hyper adapter's parsing is the subject;
- TLS and redirect credential safety in `security_tests.rs`;
- proxy behavior, connection limits/reuse, and HTTP/2 multiplexing;
- randomized disconnect, crash/restart, and end-to-end byte correctness where socket/process behavior is evidence;
- direct tests of the test server itself.

This preserves D14 rather than replacing it. Shared conformance cases should run against both adapters where they assert semantic parity, while wire-only cases remain production-only.

## Risks / Trade-offs

- **[Risk] The semantic request types omit metadata later needed by job policy.** → Define transfer intent from every current controller/worker use before deleting raw access; add compile-time-private raw responses and parity tests before removing old helpers.
- **[Risk] Type erasure adds allocations or copies on the hot path.** → Limit boxing to executor futures and one body source per response; pass `Bytes` unchanged; add an allocation/backpressure-focused unit test and compare existing throughput benchmarks before completion.
- **[Risk] Interrupting a pending body poll could regress pause/resume behavior.** → Keep the body source owned when the per-call poll future is dropped, resume by polling that same source, and require focused scripted and delayed-Hyper pause/resume tests before removing the old path.
- **[Risk] Centralizing validation changes an existing edge-case classification.** → Build a table-driven conformance suite from current probe/range/status/body cases and run it for sequential and segmented intents before deleting duplicated code.
- **[Risk] A scripted adapter creates false confidence about wire behavior.** → Enforce the test split above and retain all D14 production-adapter suites for TLS, redirects, H2, pooling, proxying, malformed framing, and resets.
- **[Risk] Strict FIFO scripts become flaky with concurrent workers.** → Provide unordered range-keyed phases and explicit gates; never use scheduler arrival order as an assertion unless ordering itself is the behavior under test.
- **[Risk] Exposing a test adapter increases public API surface.** → Keep its types in `http::scripted`, document them as deterministic testing support, and keep production constructors unchanged.

## Migration Plan

1. Introduce semantic request/result/failure types, `HttpExecutor`, `HttpExecution`, and `HttpBody` with unit tests; leave existing job calls intact.
2. Implement the port for `HttpTransport` by adapting existing redirect, probe, validator, range, and body logic. Add conformance tests while raw response APIs still exist.
3. Change controller storage to `HttpExecution`, preserve existing constructors, add the explicit injection constructor, and migrate probe/eligibility handling.
4. Migrate sequential transfer to semantic `transfer` and `HttpBody`; then migrate segmented workers to the same operation.
5. Remove raw HTTP imports and duplicate classification/validation helpers from job modules after parity tests pass.
6. Add `ScriptedHttp`, migrate orchestration-only tests, and retain/label the real-network suites described above.
7. Run formatting, linting, unit/integration tests, and throughput benchmarks; verify no job module mentions Hyper body/frame types and both transfer modes pass adapter conformance tests.

No persisted data or deployment migration is required. Rollback is code-only: until step 5, the old raw methods can remain private compatibility shims; after completion, reverting the change restores the previous controller/transport coupling without checkpoint or output-format conversion.
