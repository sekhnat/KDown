# Spec Delta

## Purpose

Makes network-efficiency metrics faithful to bytes actually received and gives subscribers a reliable end-of-job signal.

## ADDED Requirements

### Requirement: Non-duplicated wire amplification
Wire amplification SHALL be total payload bytes received from the network divided by uniquely completed bytes, without counting already-received wasted bytes a second time. The denominator SHALL have a documented policy for checkpoint-reused bytes; zero completed bytes SHALL yield an undefined metric rather than a fabricated finite value. Benchmark server-emitted bytes SHALL remain a separate labeled measurement.

#### Scenario: Retry wastes received bytes
- **WHEN** a job has received 150 network bytes, completed 100 unique bytes, and classified 50 received bytes as wasted
- **THEN** its wire amplification is 1.5, not 2.0

#### Scenario: No completed bytes
- **WHEN** a job fails before completing any unique bytes
- **THEN** the job reports amplification as undefined

### Requirement: Terminal event stream completion
A subscription to a job's events SHALL be able to observe a terminal outcome or an explicit end of stream after the job finishes, even while the caller retains a download handle. Lagged subscribers SHALL still be able to use the snapshot or terminal state to learn the final outcome; an event stream SHALL NOT wait forever after a completed, failed, or cancelled job.

#### Scenario: Handle outlives completed job
- **WHEN** a caller retains the handle and drains all events after completion
- **THEN** its next event operation terminates instead of blocking indefinitely

#### Scenario: Subscriber lags terminal event
- **WHEN** the terminal event was skipped due to bounded broadcast lag
- **THEN** the subscriber can still detect termination and inspect the terminal snapshot
