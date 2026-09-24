# Spec Delta

## Purpose

Defines compatible opt-in transfer tuning, truthful runtime status and safe lifecycle controls for embedding applications using KDown across single and concurrent download jobs.

## ADDED Requirements

### Requirement: Compatible tuning and defaults
Existing fixed worker mode, explicit segment size, configured H2 connection policy, logical output allocation, durability mode, checkpoint format, result fields, and default network behavior SHALL retain their meanings unless a separately justified migration is documented. Adaptive concurrency/sizing, bounded writer capacity and origin feedback SHALL be independently testable, with advanced/internal diagnostic controls not imposed on ordinary callers. Invalid byte budgets or worker/executor bounds MUST fail validation before network activity. A future default-policy switch requires measured correctness, performance and compatibility evidence; it is not implicit in this change.

#### Scenario: Existing configuration
- **GIVEN** a client using the current default configuration and fixed explicit sizing
- **WHEN** it upgrades to the optimized engine
- **THEN** requests, output and resume/publication outcomes retain the prior public contract without requiring new tuning fields at call sites

#### Scenario: Invalid capacity
- **GIVEN** an engine configured with an insufficient positive write-byte cap for one permitted payload or invalid executor bounds
- **WHEN** a job is started
- **THEN** a structured configuration error is returned before network activity rather than a deadlock or unbounded allocation

### Requirement: Authoritative controls and terminal outcomes
Runtime rate and concurrency updates SHALL apply to active jobs within bounds; manual concurrency updates SHALL override adaptive decisions for that job without abandoning leases. Pause and keep-partial cancellation SHALL never claim unacknowledged writes as resumable. Failure, cancellation and verification SHALL preserve existing structured outcomes and destination collision policy for single-stream and segmented transfers alike.

#### Scenario: Manual override during adaptive job
- **GIVEN** an adaptive job with live queued writes
- **WHEN** the caller sets a permitted fixed concurrency and later pauses
- **THEN** the desired and active worker counts converge to the manual value, in-flight work settles correctly and the checkpoint reflects only eligible acknowledged coverage

#### Scenario: Completion and failure
- **GIVEN** a verified job and a separate job with write, checksum or publication failure
- **WHEN** each reaches a terminal outcome
- **THEN** only the verified and successfully published destination reports Completed; the failure leaves no falsely published output or unacknowledged resume ranges
