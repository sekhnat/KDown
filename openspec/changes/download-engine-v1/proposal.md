# Proposal

## Why

KDown currently contains only a design specification (`KDownSpec.md`) and an empty OpenSpec scaffold — there is no implementation. The project needs its core component: a standalone download engine that transfers a remote object to local storage correctly under cancellation, process interruption, network failure, and partial completion, while saturating available bandwidth. This change bootstraps that engine as a reusable Rust library a future download manager can embed.

## What Changes

- Create a new Rust workspace and the `kdown-engine` crate (library-first, no UI, no global singletons) implementing the engine described in `KDownSpec.md` end to end.
- Implement the public programmatic API (`DownloadEngine` / `DownloadRequest` / `DownloadHandle` / `DownloadResult`) with validated configuration, policy types, and a structured error taxonomy.
- Implement the transfer core: probe and capability detection, single-stream and segmented (range-parallel) transfers, an interval-set segment scheduler with leases and dynamic splitting, bounded workers, retry with coordinated backoff, and token-bucket rate limiting.
- Implement resumability: versioned crash-tolerant checkpoints with atomic replacement, validator-based resume (ETag/Last-Modified, `If-Range`), pause/cancel semantics, and resource-generation consistency.
- Implement the file I/O layer: temp-file sink, positional writes, preallocation, sequential hashing verification, atomic final commit with overwrite policies.
- Implement HTTP transport semantics (redirects, ranges, conditional requests, content-encoding handling, HTTP/1.1 + HTTP/2, connection pooling, proxy and credential hooks) behind a transport abstraction prepared for HTTP/3 and non-HTTP transports.
- Implement observability: progress snapshots (unique vs network bytes), EWMA speed/ETA, event stream, counters/metrics, leveled logging with secret redaction.
- Implement security defaults: TLS validation on by default, no silent HTTPS→HTTP downgrade, credential-safety across redirects, path sanitization for server filenames, bounded resource usage against hostile servers, SSRF-restriction hooks.
- Build the verification infrastructure: deterministic misbehaving-HTTP test server, crash/restart tests, property-based scheduler tests, fuzz targets for parsers, and a repeatable benchmark harness with regression thresholds.

Scope covers the full v1 plan (spec phases 1–5). Decisions that `KDownSpec.md` §46 requires to be explicit — Rust/Tokio runtime, HTTP stack, checkpoint sidecar storage, durability mode, HTTP/2 segmentation default, adaptive concurrency timing — are locked in `design.md`.

## Capabilities

### New Capabilities

- `engine-api`: The public programmatic surface — engine/handle/request/result types, engine and job configuration with validation, job lifecycle state machine, and concurrency/limit control entry points.
- `transfer-core`: The transfer execution core — probe and capability detection, segmentation eligibility, interval-set scheduling with worker leases and dynamic splitting, the worker execution loop, retry classification and coordinated backoff, rate limiting, unknown-length handling, and resource-generation-change handling.
- `resume-and-storage`: Persistent progress and local storage — sink abstraction, temp-file layout, positional writes and preallocation, versioned checkpoint format with atomic durable replacement, checkpoint/resume validation, pause semantics, and atomic final commit with overwrite policies.
- `integrity`: Correctness verification — caller-supplied hash verification (SHA-256/SHA-512 at minimum), exact size verification, verification timing relative to commit, and mismatch handling that prevents committing wrong data.
- `http-transport`: Protocol behavior — HTTP/1.1 and HTTP/2 range/conditional/redirect semantics with content-encoding rules, response validation, connection pooling and limits, proxy and authentication hooks, and the transport abstraction boundary that keeps HTTP specifics out of scheduling.
- `observability`: Structured errors, progress snapshots (unique vs network bytes), speed/ETA estimation, event stream, metrics, leveled logging with correlation fields, and sensitive-data redaction.
- `security`: Safety defaults — TLS validation, redirect credential safety, local path safety, resource-exhaustion bounds, and SSRF restriction hooks.

### Modified Capabilities

None — the project has no existing specs.

## Impact

- **Code**: New Rust workspace at the repo root; engine code under `crates/engine/src` (module layout per `KDownSpec.md` §43); test/bench infrastructure under `crates/engine/tests` and `crates/engine/bench`. Nothing existing is modified or removed.
- **APIs**: Introduces the first public API of the project (library crate). No compatibility constraints yet; semver starts at 0.x.
- **Dependencies**: Adds the core Rust stack — Tokio (async runtime), an HTTP client layer (reqwest or hyper, decided in `design.md`), TLS (rustls preferred), serde (checkpoint serialization), SHA-2 hashing, property-testing/fuzzing/benchmark tooling. Exact versions pinned in `design.md`.
- **Systems**: Local filesystem (temp files, checkpoints, atomic rename semantics per platform); network origins (concurrency and retry coordination must not overwhelm servers).