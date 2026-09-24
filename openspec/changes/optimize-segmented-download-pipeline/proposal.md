# Proposal

## Why

KDown's segmented workers validate ranges and maintain resumable, generation-aware coverage, but their output handles share one mutex-protected `FileSink` that seeks a shared file cursor; each body chunk also calls a page-cache flush. This serializes independent ranges and lets per-worker checkpoint work compete with transfers. Establish a measurable baseline, remove internal serialization, then tune scheduling and concurrency without weakening correctness or publication safety.

## What Changes

- Add concurrent offset-based file writes with lifecycle ownership restricted to the output session; remove the segmented seek/write mutex and ordinary per-chunk `PageCache` flush. Keep safe portable fallback for optional physical preallocation and existing atomic publication/cleanup behavior.
- Make checkpointing job-owned and coalesced: performance mode checkpoints acknowledged written ranges (page-cache acknowledgment, not power-loss durability); durable mode synchronizes the output file before persisting the corresponding ranges. Failed sync/save must not advance acknowledged checkpoint state. Preserve pause, cancellation and retry semantics.
- Publish coherent lease/generation/written-through snapshots; reduce chunk-path fatal-state and rate-limiter locks, and use actual worker metric shards.
- Honor configured initial segment sizing; add a bounded scheduler target-size policy, event notifications instead of idle polling, a persistent worker pool that can shrink and reactivate, and conservative bounded adaptive range concurrency. Manual runtime concurrency controls remain supported.
- Treat Hyper `Bytes` as the primary payload path; remove unused per-worker pools, make any explicit transfer memory cap job-wide, and clarify surviving `BufferPool` exhaustion semantics without adding copies. Separate stream-worker policy from physical connection policy.
- Correct benchmark byte accounting; compare useful goodput against wire throughput across representative sizes, protocols, worker counts, storage/network/server conditions, using an isolated-server mode for authoritative results. Profile before adding micro-optimizations.
- Behavioral/configuration changes: segmented checkpoint timing moves from worker-chunk opportunities to a job interval plus pause/terminal boundaries; durable mode gains explicit data-sync-before-metadata ordering (it must not persist unsafe ranges); `set_concurrency` becomes bidirectional within configured bounds, with adaptive jobs initially using `min_workers`. The existing non-optional `initial_segment_size` is honored rather than silently replaced by `max_segment_size`; any auto-target selection requires an explicit new option or documented compatibility migration. No automatic HTTP/2 connection increase. Public configuration/API changes, if needed for adaptive opt-in or auto sizing, must retain current defaults and be documented.

## Capabilities

`openspec/specs/` currently contains no installed specs. The paths below are **new** to the main inventory but follow the established paths in prior change deltas (`download-engine-v1`); this avoids parallel names for the same behavior.

### New Capabilities
- `transfer-core`: scheduler sizing, event-driven persistent workers, adaptive concurrency, coherent progress, retry/generation safety, rate limiting and job-wide transfer memory behavior.
- `resume-and-storage`: independent positional output, durability and centralized checkpoint ordering, preallocation fallback, and preservation of temporary-file/publication lifecycle.
- `observability`: correct per-worker counters and useful-vs-wire benchmark/profiling requirements.

### Modified Capabilities
- None: no main specs currently exist to modify.

## Impact

Primary code: `crates/engine/src/io/{output_session,sink,buffer_pool}.rs`, `job/{segmented,controller}.rs`, `scheduler/core.rs`, `resume/{checkpoint_store,durable_ranges}.rs`, `control/rate_limit.rs`, `metrics/counters.rs`, and `config.rs`; tests in `crates/engine/tests/` and the existing unit tests. Benchmark work in `crates/engine/benches/throughput.rs`, `scripts/bench_check.sh`, and benchmark documentation/results. Preserve existing `CheckpointStore` injection, `OutputSession` fault hooks, destination lease, checksum verification, atomic no-replace/replace publication, and supported Unix/Windows behavior. Avoid io_uring, IOCP-specific engines, HTTP/3, extra H2 connections, unsafe buffers, or a new hashing/allocator architecture absent profiling evidence.
