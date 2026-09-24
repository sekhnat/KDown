# Spec Delta

## Purpose

Defines trustworthy worker-level accounting and repeatable performance evidence for optimized segmented downloads without weakening correctness verification.

## ADDED Requirements

### Requirement: Correct worker and byte accounting
Network bytes, unique completed bytes, retries, and retransferred/retry overhead SHALL be attributed to the actual worker where applicable. A metric labeled as bytes MUST be derived from a byte count, not a warning count or request count. Useful completed-byte goodput and network-byte wire throughput SHALL be reported separately with elapsed time; reused checkpoint bytes SHALL not inflate newly completed or network-byte throughput.

#### Scenario: Retry and resumed coverage
- **WHEN** a segmented job reuses a checkpoint and retransfers a failed range
- **THEN** network bytes include received payload, unique newly completed bytes exclude reused and duplicate ranges, and retry overhead is counted in bytes with correct worker attribution

#### Scenario: Failed transfer fixture
- **WHEN** a benchmark fixture emits warnings without retransferring any bytes
- **THEN** retransferred bytes remain zero and warning count is not misreported as bytes

### Requirement: Representative comparative benchmark suite
The suite SHALL validate benchmark metric arithmetic and correctness outcomes (final size/hash and publication), include HTTP/1.1 and HTTP/2, representative 32 MiB/256 MiB/1 GiB fixtures and worker counts 1/2/4/8, and offer optional manual multi-GiB/16-worker runs. It SHALL offer an isolated fixture-server mode for authoritative architectural comparisons; in-process scenarios MAY remain as smoke tests. A repeatable profiling procedure SHALL report or explain availability of CPU cost per useful byte, context switches, syscalls, peak memory, worker idle time, write and checkpoint/sync latency, and throughput by protocol and storage/network/server behavior. Comparison SHALL use same-host baseline conditions rather than a universal throughput threshold, and MUST flag material CPU or correctness regressions.

#### Scenario: Baseline versus positional output
- **WHEN** equivalent pre- and post-change runs execute at multiple worker counts with isolated server resources
- **THEN** the report distinguishes useful goodput from wire throughput, identifies whether worker scaling is limited by network/server/CPU/storage rather than a global output mutex, and documents CPU and memory changes

#### Scenario: Variable environments
- **WHEN** manual runs use tmpfs and disk-backed storage, bandwidth/RTT/loss controls, and servers honoring/ignoring ranges, throttling, resetting, failing transiently or changing validators
- **THEN** the report identifies the conditions and captures retries, overhead, throughput, and final correctness rather than attributing fixture-server cost to the client
