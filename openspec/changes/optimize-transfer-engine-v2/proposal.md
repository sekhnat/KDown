# Proposal

## Why

KDown's previous optimization established correct positional writes and unique-byte accounting, but its opt-in adaptive mode currently allocates only the **initial** number of worker tasks (`run_segmented` uses `worker_count = desired_workers()`), so later increases cannot activate additional workers. It also retains one long-lived blocking `WriterLane` per network worker, with each worker awaiting each write before reading again. We need measurable, WAN-representative gains in **useful** goodput, not additional connections or duplicated bytes at the expense of correctness.

## What Changes

- Establish a reproducible, process-isolated H1/H2 baseline and low-overhead metrics before changing behavior; compare medians and dispersion across bandwidth, RTT, loss, storage and concurrent-job conditions. No throughput gain is assumed before measurement.
- Repair actual adaptive worker growth: provision async network workers up to configured maximum, park dormant ones on the existing revision signal, and decouple writer resources from that capacity. Preserve fixed mode and manual override semantics.
- Introduce an internally switchable bounded positional-write executor with a byte budget, out-of-order completion tracking and contiguous acknowledged frontier. Retain the current writer path until correctness and performance parity; owner-only sync, verification and atomic publication remain unchanged.
- Add opt-in duration-informed segment sizing and a ready-work policy; reserve live-tail splitting mainly for stragglers and measure wire amplification, including a targeted H1 split regression.
- Improve the adaptive controller with verified active-worker feedback, storage-pressure and protocol signals; distinguish H1 connections from H2 streams and maintain one H2 connection by default unless measured flow-control evidence supports otherwise.
- Share normalized-origin throttle/backoff and fair request admission across jobs within one engine; isolate unrelated origins and bound registry lifetime. Build on existing connection permits and per-job backoff rather than replacing HTTP validation or retry classification.
- Evaluate (do not assume benefits from) token leasing, counter layout/publication batching, read-buffer tuning and physical preallocation after structural phases. Keep existing defaults until benchmark evidence and compatibility review justify changes.

No deliberate breaking API or sidecar-format changes are proposed. New optional configuration and diagnostic fields must be additive; legacy fixed/explicit settings retain their meanings. HTTP/3, io_uring, UI and unrelated CLI work are out of scope.

## Capabilities

`openspec list --specs` currently reports **no main specs**: earlier, still-active change deltas define the project's established paths. Accordingly these are **new main-spec paths, not duplicate names**; they reuse exactly the prior change paths (`download-engine-v1` and `optimize-segmented-download-pipeline`). Before archive/sync, reconcile their requirements with the earlier completed but unsynced changes; do not overwrite those deltas. Notably the old `transfer-core` split requirement says read/queued bytes are excluded, whereas current `split_tail` uses acknowledged `next_offset`; this change measures and closes that gap.

### New Capabilities

- `transfer-core`: actual dynamic concurrency, bounded writer backpressure, useful-goodput control, duration-based scheduling and low-duplication splits.
- `resume-and-storage`: ordered write acknowledgement, safe checkpoint/finalization boundaries and storage backpressure.
- `http-transport`: protocol-aware stream/connection policy and fair, shared-origin throttle admission without weakening range/validator checks.
- `observability`: trustworthy cross-job metrics and repeatable before/after benchmark evidence.
- `engine-api`: compatible opt-in tuning and runtime controls across manual/fixed/adaptive modes.

### Modified Capabilities

- None: there are no synced main specs in `openspec/specs/`; paths above follow the existing historical capability organization.

## Impact

Affected: `crates/engine/src/job/segmented.rs`, `job/controller.rs`, `control/adaptive.rs`, `control/rate_limit.rs`, `scheduler/core.rs`, `io/{writer_lane,output_session,positional}.rs`, `http/{connect,transport}.rs`, `config.rs`, `metrics/*`, the isolated fixture and throughput harness, tests and profiling docs. Existing `SegmentScheduler` interval ownership, `LeaseProgress` coherence, `OutputSession` exclusive publication, checkpoint-store injection, `HttpExecution` range checks, integrity verification and `CancelMode` cleanup remain authoritative. Risks: additional queued-write state, leaked permits/deadlocks, origin fairness, longer checkpoint lag, and ambient benchmark noise. Each major phase has a legacy-path rollback switch and a correctness plus benchmark gate; a phase with no defensible gain is not retained merely to satisfy an optimization hypothesis.
