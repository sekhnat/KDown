# Spec Delta

## Purpose

Defines enforceable lifetime and concurrency controls for download jobs, including timely cancellation of blocked operations.

## ADDED Requirements

### Requirement: End-to-end job deadline and cancellation
A configured job deadline SHALL be measured from admission on a monotonic clock and SHALL bound probe, transfer, backoff, pending HTTP headers and bodies, verification, and the decision to begin publication. On expiry the job SHALL terminate with a typed non-success outcome, without publishing unverified output. Cancellation SHALL interrupt pending network/header waits, retry backoff, rate/resource admission waits, and paused workers promptly; cleanup SHALL preserve the configured artifact disposition. Once an atomic publication has succeeded, the engine SHALL NOT report a failure that falsely implies that no file was committed.

#### Scenario: Expired deadline during slow transfer
- **WHEN** a job has a 10 ms deadline and the server stalls well beyond that deadline
- **THEN** it returns a typed non-success outcome in bounded time and does not publish partial output

#### Scenario: Cancel during server-directed backoff
- **WHEN** the caller cancels while a long `Retry-After` delay or header wait is pending
- **THEN** the job exits promptly with cancellation and honors its selected cleanup policy rather than sleeping the full delay

#### Scenario: Deadline at publication boundary
- **WHEN** the deadline expires before the verified file begins publication, or while atomic publication completes
- **THEN** the outcome and committed events accurately describe whether the destination was published

### Requirement: Maximum active job admission
The configured `max_active_jobs` SHALL cap concurrently admitted jobs per controller. Permit ownership SHALL cover the job lifetime and be released on success, error, cancellation, or aborted task. Excess starts SHALL use a documented cancellation-aware bounded admission behavior, without creating unbounded per-job work or mutable output artifacts.

#### Scenario: Capacity exhausted
- **WHEN** jobs up to the configured maximum are active and another caller starts one
- **THEN** the extra job does not begin transfer or mutate an output until admitted, or returns a documented admission error

#### Scenario: Capacity released
- **WHEN** an admitted job terminates or is cancelled
- **THEN** another waiting job can acquire capacity without leaked permits
