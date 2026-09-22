# Proposal

## Why

HTTP behavior is internally cohesive, but its boundary with job orchestration leaks `hyper` response bodies, raw protocol metadata, and duplicated status/range/body classification. Deepening that boundary now will keep protocol mechanics local while making retry, authentication, cancellation, timeout, and resource-change orchestration deterministic to test without weakening the existing real-network coverage.

## What Changes

- Introduce a substitutable HTTP execution port owned by the `http` module and make both sequential and segmented job paths depend on it instead of directly on `HttpTransport` or `hyper::body::Incoming`.
- Return HTTP-owned semantic probe and transfer outcomes, including retry timing and authentication challenges, rather than requiring job code to interpret raw statuses and headers.
- Provide an HTTP-owned bounded body/chunk abstraction that preserves backpressure, cancellation responsiveness, read-idle timeouts, and zero-copy `bytes::Bytes` delivery without exposing Hyper types.
- Consolidate range validation, status-to-error mapping, body-error classification, and resource-generation checks so sequential and segmented transfers use the same rules.
- Use `ProbeMetadata::segment_eligible` as the single segmentation decision after the HTTP layer performs any configured validating range request.
- Keep `HttpTransport` as the production Hyper adapter and add a deterministic scripted adapter for orchestration tests, with ordered requests/responses, authentication challenges, body faults, timeouts, and generation changes.
- Move orchestration-only cases from local-server timing tests to the scripted adapter while retaining real-network tests for TLS, redirects, HTTP/2 multiplexing, connection pooling, malformed wire behavior, and production-adapter integration.

## Capabilities

### New Capabilities

- `http-execution-seam`: Defines the transport-independent HTTP execution contract, shared sequential/segmented semantics, deterministic scripted behavior, and the boundary between orchestration tests and real-network adapter tests.

### Modified Capabilities

_None._

## Impact

- Primary code: `crates/engine/src/http/transport.rs`, `probe.rs`, `range.rs`, `validators.rs`, and `mod.rs`; new HTTP execution/scripted-adapter modules; `crates/engine/src/job/controller.rs` and `segmented.rs`; and the conceptual transport boundary in `KDownSpec.md` §32.
- Tests: transfer/orchestration integration tests under `crates/engine/tests/` and their support modules; real-server transport, TLS, redirect, proxy, pool, HTTP/2, and malformed-wire tests remain.
- Public API: existing `HttpTransport` construction remains supported; controller internals change to a substitutable executor, with an injection path suitable for tests and alternate adapters.
- Dependencies and performance: no per-chunk copy or unbounded buffering is introduced; the seam may require object-safe async plumbing but should reuse existing `bytes`/Tokio facilities where possible.
