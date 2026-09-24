# Spec Delta

## Purpose

Provides truthful, low-overhead transfer accounting and reproducible benchmark evidence for throughput, network efficiency, resource use and adaptive decisions across protocols and simultaneous jobs.

## ADDED Requirements

### Requirement: Useful versus wire accounting
The engine SHALL separately report newly unique completed bytes, reused checkpoint bytes, network payload bytes, retry/wasted bytes, elapsed time and retry/throttle events. Useful goodput SHALL exclude resumed bytes; wire throughput SHALL use received network payload. Wire amplification SHALL be defined as total received payload (network bytes including re-received waste, which this engine accounts separately) divided by newly unique completed bytes when the denominator is nonzero, and undefined or explicitly unavailable otherwise. A metric labeled bytes MUST be derived from bytes, not warnings or request counts.

#### Scenario: Clean, resumed and retried transfers
- **GIVEN** a clean range transfer, a resumed transfer and a retry with duplicate payload
- **WHEN** metrics are emitted
- **THEN** clean amplification is near 1, resumed bytes do not inflate useful goodput, and redundant network payload is exposed without double-crediting completed coverage

### Requirement: Resource and decision visibility
When available, diagnostics SHALL distinguish desired/active/idle workers, active H1 connections/H2 streams, establishment count, writer queue depth and outstanding bytes, acknowledgement and queue wait latency, retries/429/503/Retry-After, split count and split-related waste, segment counts/sizes, checkpoint cadence/cost, CPU/GiB, peak RSS and total completion time. If scheduler lock wait, allocation, syscalls or H2 flow-control wait cannot be observed on a platform, reports MUST label the gap rather than fabricate values. Expensive per-operation tracing SHALL remain disabled or sampled by default; normal logs MUST NOT include credentials or noisy per-chunk records.

#### Scenario: Bottleneck identification
- **GIVEN** a slow sink and a congested origin in otherwise identical runs
- **WHEN** diagnostic output is enabled
- **THEN** it explains why concurrency was held/reduced, exposes storage versus origin signals and distinguishes physical connections from logical requests

#### Scenario: Unsupported instrumentation
- **GIVEN** a platform without H2 flow-stall hooks or syscall profiling
- **WHEN** a comparison report is generated
- **THEN** unavailable measurements are marked as unavailable, with methodology and limitations documented

### Requirement: Reproducible comparison before optimization claims
A process-isolated benchmark suite SHALL verify final size, hash and publication and report for each comparison baseline/optimized commits, environment, protocol, bandwidth, RTT, impairment, file size, worker/stream setting, storage target, job pattern, useful/wire throughput, amplification, CPU per GiB, peak RSS, writer latency, runtime and repetitions with median plus dispersion. It SHALL exercise H1/H2, 100 Mbps/1 Gbps/highest reliable local rate, local/10/50/150 ms RTT, clean/loss/throttle/interruption, below-threshold/32 MiB/approximately 1 GiB/multi-GiB where feasible, 1/2/4/8/16 workers where permitted, fast and constrained storage, and single/same-origin/multi-origin jobs. Unavailable shaping or hardware axes MUST be marked manual/not run. Conclusions MUST flag variance overlap and regressions rather than assume speedup.

#### Scenario: Cross-environment comparison
- **GIVEN** a baseline and candidate run under identical controlled conditions with multiple repetitions
- **WHEN** the report is produced
- **THEN** it shows goodput and resource tradeoffs, correctness checks and dispersion; overlapping variance does not become a claimed improvement

#### Scenario: Multi-job congestion
- **GIVEN** same-origin and separate-origin job mixes
- **WHEN** the suite compares them to the isolated single-job case
- **THEN** fairness, aggregate memory and writer-thread count are recorded in addition to individual completion times
