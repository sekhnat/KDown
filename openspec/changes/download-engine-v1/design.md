# Design

## Context

Greenfield project: `KDownSpec.md` is the authoritative technical specification (language-neutral); the repo has no code yet. The user selected **Rust** as the implementation language and **full v1 scope (spec phases 1–5)**. `KDownSpec.md` §46 lists ten decisions that must be explicit before implementation; this document locks them down. Requirements live in the delta specs; this file covers the how.

## Goals / Non-Goals

**Goals:**

- A reusable Rust library crate (`kdown-engine`) with a stable programmatic API, no UI dependencies, and module layout per `KDownSpec.md` §43.
- Correctness invariants (§39) enforced structurally — by types and tested invariants, not by convention.
- Bounded memory (O(workers × buffers), not O(file size)); saturate a 1 Gbit/s link on commodity hardware without excessive CPU (§22).
- Deterministic, testable failure behavior for every scenario in §38 and the spec deltas.

**Non-Goals:**

- No download-manager features (queues, UI, scheduling, browser integration, catalogs) — engine is a library only (§2).
- No BitTorrent/FTP/HLS/DRM/credential vaults (§2).
- No HTTP/3 implementation in this change — architecture must merely not preclude it (§4.2).
- No global adaptive concurrency controller in v1 — fixed configured concurrency with scheduler hooks; adaptive control is post-v1 (§12.4).

## Decisions

### D1. Language, runtime, and crate layout — Rust + Tokio, single library crate

**Decision:** Rust (edition 2021+, MSRV latest stable at project start, pinned in `rust-toolchain.toml`). Async runtime: **Tokio**. Workspace with one library crate `kdown-engine` at `crates/engine`; a thin `kdown-cli` example binary may follow in a later change but is not built here.

Module tree inside `crates/engine/src/` follows §43: `engine`, `config`, `error`, `job/{controller,state,progress}`, `http/{transport,probe,range,validators,redirect}`, `scheduler/{interval_set,scheduler,lease}`, `io/{sink,file_sink,buffer_pool}`, `resume/{checkpoint,checkpoint_store}`, `control/{cancellation,retry,rate_limit}`, `integrity/verifier`, `metrics/{events,counters}`.

**Rationale:** §45 names Rust a strong fit for memory safety and predictable performance; async tasks give worker-per-segment without thread-per-segment (§30); Tokio's ecosystem (hyper, rustls) is mature.
**Alternatives:** Go (faster to build, weaker bounded-buffer guarantees, GC on the hot path); C++ (more control, far more risk); smol/async-std (smaller ecosystems). Tokio over raw hyper-only: task/time utilities (timeouts, select, CancellationToken-equivalent) are needed regardless.

### D2. HTTP stack — hyper 1.x + hyper-util client, not reqwest

**Decision:** Build the `http` module directly on **hyper 1.x** (`hyper`, `hyper-util`, `http-body-util`) with **rustls** (`tokio-rustls`/`rustls` via `hyper-rustls`), not on reqwest.

**Rationale:** The engine needs to classify protocol violations precisely (range/`Content-Range` mismatches, `If-Range` behavior, redirect credential stripping per hop, per-origin connection limits). reqwest hides per-hop decisions and its redirect layer fights the engine's own; hyper exposes exactly the request/response boundary the `Transport` trait (§32) needs. `reqwest` remains the fallback if build-time estimates blow up.
**Alternatives:** reqwest (fewer lines, less control); raw `hyper` without hyper-util (hand-rolled pooling/connector plumbing hyper-util already provides).

### D3. Checkpoint storage — sidecar file default behind a `CheckpointStore` trait

**Decision:** Default store writes a sidecar `<dest>.kdown` checkpoint file next to the temp file, using a **versioned JSON** format (`format_version` field) per §15.2 logical schema. The `CheckpointStore` trait (§34) keeps SQLite/other backends pluggable later.

**Rationale:** Zero new dependencies, human-inspectable during development, matches spec default; trait keeps the future manager free to substitute a database.
**Alternatives:** SQLite from day one (heavier, premature); bincode/CBOR (less debuggable; can be added as an encoding behind the trait without format churn — format_version permits it).

### D4. Durability — performance mode default, durable mode opt-in per job

**Decision:** `fsync_policy`/checkpoint durability defaults to **performance mode**: checkpoint records ranges after the OS acknowledges writes (page cache); a `Durable` mode flushes data (and optionally directory entries) before recording. The active guarantee is visible in config and surfaced in results/warnings.

**Rationale:** §15.4 allows either if the guarantee is explicit; performance mode matches typical desktop use and the 2s checkpoint cadence. Durable mode exists for callers who need power-loss certainty.
**Alternatives:** Durable-by-default (safe but slower; correct choice remains available).

### D5. HTTP/2 segmented streams — enabled by default, one connection first

**Decision:** HTTP/2 is used by default (`prefer_http2: true` per §8.1); segmented jobs start with a single connection multiplexing range streams and may open additional connections to an origin only under the §24 conditions (server stream limits, flow-control bottleneck, measured throughput gain) via an engine-level policy hook.

**Rationale:** Multiplexing avoids TLS/connection setup cost and respects per-origin limits by default; the escape hatch keeps the "one H2 connection is not always optimal" failure mode (§24) out of the architecture.

### D6. Concurrency policy — fixed configured workers in v1, adaptive post-v1

**Decision:** Worker count is fixed from `TransferPolicy` (start `min(4, max_workers)`, scale up per §40 note); the scheduler exposes the interface needed for a future adaptive controller but v1 ships no adaptive logic.

**Rationale:** §12.4 and §40 recommend staged complexity; adaptive control needs benchmark data that does not exist yet.

### D7. Scheduling and correctness core — `BTreeMap`-backed interval set + lease generation counters

**Decision:** `IntervalSet` implemented over `BTreeMap<u64, Range>` (start → inclusive end) with normalized insert/merge/subtract operations. `SegmentScheduler` hands out `SegmentLease`s carrying `lease_id` + monotonically increasing `generation`; all progress reporting is `(lease_id, generation, durable_through_offset)` and stale callbacks are rejected by comparing generation (§31).

**Rationale:** O(log n) operations, trivially testable, property-test friendly (§36.4); avoids per-chunk records (§12.1). Lease generation directly implements invariant §39.6.
**Alternatives:** Interval tree crates (extra dependency, no behavioral gain at expected segment counts); lock-free structures (complexity not justified — scheduler lock is taken only on acquire/report/complete/fail, per §13.3).

### D8. Write path — pooled buffers, positional writes, explicit flush coordination

**Decision:** Chunk size 128 KiB from a shared `BufferPool` (bounded by `buffer_pool_max_bytes`, default 128 MiB); workers write via positional I/O (`std::fs::File` + seek-free `write_at`-style calls — on Unix `pwrite` via `std::os::unix::fs::FileExt::write_at`, on Windows `seek_write`-equivalent semantics through a dedicated handle per job, never a shared seek pointer). Backpressure: a worker has at most one in-flight network read and one pending write; it cannot read the next chunk until the previous write is acknowledged. A small `Semaphore`-bounded pipeline (default 2 chunks) is the tunable if throughput demands.

**Rationale:** Bounded memory trivially; backpressure falls out of the one-read-one-write pipeline; `write_at` avoids pointer sharing (§14.2). Cross-platform: Unix `write_at`, Windows overlapped or `seek_write` on a job-owned handle; abstraction in `io/file_sink`.
**Alternatives:** vectored writes (premature); mmap sink (post-v1 extension point §4.2).

### D9. Hashing — sequential verification pass for segmented mode

**Decision:** Segmented jobs verify expected whole-file hashes by a streaming sequential read of the temp file during `Verifying` (§16.2). Single-stream jobs may hash on the fly. Only SHA-256/SHA-512 ship in v1 (MD5/SHA-1 deferred unless requested); size verification is exact.

**Rationale:** Disk-bound, simple, strong; per-segment tree hashing adds protocol complexity that only pays off with a compatible tree construction (post-v1).

### D10. Retry and backoff — classified errors, full jitter, per-origin circuit state

**Decision:** A `RetryClassifier` maps error kinds to retryable/non-retryable per `RetryPolicy` (§8.3 defaults). Backoff: `delay = random(0, min(max_delay, base × multiplier^attempt))` with `Retry-After` honored within `max_delay`. A per-origin shared backoff state (per job, coordinated via the job controller) prevents thundering herds on 503/429 (§17.4): when multiple workers fail with an origin-wide error, a job-level backoff gate holds all workers until the earliest acceptable retry time.

**Rationale:** §17.2/§17.4; full jitter is the standard anti-herd choice. Job-level coordination is sufficient in v1 because an engine-wide gate would need cross-job plumbing with no near-term caller.

### D11. Rate limiting — hierarchical token buckets on payload bytes

**Decision:** `RateLimiter` (token bucket, burst ≈ 250 ms of configured rate) at global (engine-owned, shared across jobs) and per-job levels; workers acquire tokens for payload bytes only (headers/overhead excluded). Unlimited mode bypasses the bucket entirely (no wakeup overhead). Runtime changes take effect on the next `acquire` call.

**Rationale:** §18; payload-only accounting keeps progress honest and limiter cost near zero in unlimited mode.

### D12. Progress and events — atomic counters, folded snapshots, bounded event channel

**Decision:** Per-worker counters updated with atomics; the metrics task folds them into `ProgressSnapshot` every 500 ms (rate via EWMA over payload throughput; ETA only when size known and smoothed rate above threshold). Events flow through a bounded mpsc channel with documented loss-free delivery for lifecycle events and cadence-batched progress events; callbacks/events never run while scheduler locks are held (documented: events are delivered concurrently by the stream consumer).

**Rationale:** §19; bounded channel prevents a slow consumer from stalling workers (progress events are dropped-safe by design; lifecycle events are small).

### D13. Errors and redaction — single `DownloadError` enum with category + redaction at the logging boundary

**Decision:** One non-exhaustive `DownloadError` enum per §20 taxonomy, each variant carrying structured context (origin, status, segment range, source). `Redactor` applied at the log formatting layer (never inside hot paths) strips `Authorization`, `Cookie`, `Set-Cookie`, proxy auth, URL userinfo, and caller-marked query params; error Display paths apply the same redaction.

**Rationale:** One enum keeps `match` exhaustiveness for retry classification and host error handling; redaction centralized so new call sites can't forget it.

### D14. Test/bench infrastructure — custom deterministic misbehaving server, property + crash + fuzz + bench

**Decision:** `crates/engine/tests/support/test_server`: an axum/hyper-based server with scripted fault behaviors (§36.2 list: lying Accept-Ranges, 200-on-range, malformed Content-Range, truncated bodies, delayed headers/chunks, resets, redirect loops, mid-download ETag flips, unknown length, auth challenges, encoding edge cases). Property tests: `proptest` over the interval set and scheduler operation sequences (the §36.4 coverage property). Crash tests: `tokio::process`-driven kill-and-restart loops at randomized checkpoints. Fuzz: `cargo-fuzz` targets for URL/Content-Range/ETag/Content-Disposition/checkpoint parsers. Bench: `criterion` harness + local fixture server scenarios (§37), with regression thresholds enforced in CI as warnings.

**Rationale:** All four are spec-mandated (§36, §37); the custom server is the only way to get deterministic faults from a real HTTP stack.

### D15. Workspace and dependency set

**Decision:** Single-crate workspace. Core dependencies (pinned at implementation start, current stable majors):

- `tokio` (full), `hyper` 1.x, `hyper-util`, `http-body-util`, `hyper-rustls`, `rustls`, `tokio-rustls`, `rustls-pemfile` — HTTP/TLS
- `serde`, `serde_json` — checkpoint encoding
- `sha2` (+ `md-5`/`sha1` only if legacy digests are requested)
- `thiserror`, `tracing` (+ `tracing-subscriber` in tests/examples)
- `bytes` — buffer types
- Dev: `proptest`, `criterion`, `axum` (test server), `tempfile`, `cargo-fuzz` tooling
- `libc`/`winapi`-tier direct OS calls avoided — std `FileExt`/`SeekFrom` cover positional writes; anything missing becomes a small vendored wrapper, not a new heavyweight dep.

**Rationale:** Boring, mature, permissively licensed. No C-level transitive surprises beyond TLS stack (ring/aws-lc-rs backend choice at implementation time; aws-lc-rs default with `ring` fallback noted as a build-compat risk).

## Risks / Trade-offs

- [rustls backend (aws-lc-rs) build friction on some platforms] → select backend via feature flag; `ring` fallback documented; CI matrix covers Linux/macOS/Windows.
- [hyper 1.x manual plumbing vs reqwest productivity] → the `Transport` trait isolates the HTTP layer; if schedule slips, a reqwest-backed transport can coexist behind the same trait without touching scheduler/sink.
- [Windows positional-write and rename semantics (sharing/locking, rename-over-existing)] → dedicated job handle + `ReplaceFile`-equivalent handling wrapped in `io/file_sink` with platform tests in the matrix; Linux remains the primary dev target.
- [Performance-mode checkpoint vs power loss] → guarantee is explicit in config/results (D4); durable mode available; spec scenarios only require safety, not zero-loss.
- [Bounded event channel may drop progress events under slow consumers] → lifecycle events are loss-free and small; progress snapshots are recoverable via `snapshot()`; documented in observability spec behavior.
- [Custom test server fidelity] → behaviors are scripted at the HTTP layer with the same stack as production transport, so protocol-edge behavior (chunking, H2 resets) is exercised through real code paths.
- [Full v1 scope is large] → tasks.md is phased (§41 phases 1–5); each phase has exit criteria so apply can proceed incrementally and stop safely at any phase boundary.

## Migration Plan

Not applicable — greenfield. The change creates the workspace and crate from scratch; no deployment or rollback surface beyond source control. Library API starts at 0.1.0 with no stability guarantees until 1.0.

## Open Questions

None that block tasks. Deferred-by-design items (adaptive concurrency controller, HTTP/3 transport, tree hashing, mmap sink) are post-v1 extension points already recorded in the spec (§4.2, §12.4, §16.2) and do not change this change's specs or breakdown.