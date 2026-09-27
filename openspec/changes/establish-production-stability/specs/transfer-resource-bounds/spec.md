# Spec Delta

## Purpose

Bounds memory retained by the full active transfer pipeline and makes effective limits and actual peak usage observable to operators and embedding applications.

## ADDED Requirements

### Requirement: Validated end-to-end transfer budget
The engine SHALL provide configurable, validated per-job and aggregate limits for transfer-pipeline retained memory, including network ingress, HTTP-client buffering, queued/held chunks, writer queues and in-flight writes, and checkpoint state. Across concurrent jobs all covered allocations MUST be accounted within the aggregate budget without double-counting shared payloads. Backpressure, bounded streaming, or a typed refusal MUST prevent a job from exceeding the limits; unsupported or unbounded client buffering MUST NOT be silently omitted from the guarantee. The documented contract SHALL distinguish this bound from total process RSS and operating-system socket/kernel memory.

#### Scenario: Malicious large body frames
- **WHEN** a server delivers unexpectedly large or rapid body frames under a small configured budget
- **THEN** admitted transfer-pipeline memory remains within its effective budget or the operation fails explicitly before a bound is violated

#### Scenario: Multiple jobs and slow writer
- **WHEN** many jobs compete for network buffers and a slow disk retains queued writes
- **THEN** aggregate covered memory remains within the configured limit, with backpressure or typed rejection rather than unchecked queue growth

#### Scenario: Oversized checkpoint
- **WHEN** checkpoint data or range metadata grows toward its memory allowance
- **THEN** it is bounded or the job fails safely without saving an invalid checkpoint or declaring incomplete bytes durable

### Requirement: Resource telemetry and release of reservations
The observable metrics SHALL report configured and effective per-job/aggregate budget, current usage, and observed high-water usage for each covered component and the total, with units, scope, and reset semantics documented. Reservations MUST be released after cancellation, failure, and completion; reporting MUST not claim unmeasured client buffers are zero.

#### Scenario: Peak and limit inspection
- **WHEN** an application reads metrics during and after a concurrent transfer
- **THEN** it sees effective limits and high-water totals and components, and each recorded total peak does not exceed its configured cap

#### Scenario: Terminal cleanup
- **WHEN** a job fails or is cancelled while a write and a checkpoint operation are pending
- **THEN** its outstanding reservations eventually return to zero while the completed high-water record remains available
