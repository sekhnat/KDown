# Spec Delta

## Purpose

Defines a terminal download contract in which a successful return always means verified publication, and all other outcomes are unmistakable to callers.

## ADDED Requirements

### Requirement: Success means verified completion
The public synchronous-await result of a started job and the direct run operation SHALL report success only after the requested object passes applicable size/integrity checks and the final destination is committed. Failure and cancellation MUST NOT be represented by a successful outer `Result` with a status or optional error field. Awaiting a spawned job MAY produce a distinct task-join failure, which MUST NOT mask transfer or engine errors.

#### Scenario: Successful verified transfer
- **WHEN** all bytes are verified and the destination is published
- **THEN** awaiting the job yields a successful completion containing the final path and accounting data

#### Scenario: Failed transfer is not success
- **WHEN** retries are exhausted or integrity validation fails
- **THEN** the direct run and awaited job return an error with transfer-failure detail, even if partial bytes were written

#### Scenario: Cancellation is not success
- **WHEN** the caller cancels a running transfer
- **THEN** the terminal result is a typed cancellation error, not a successful completion

### Requirement: Separate failure domains and preserve diagnostics
The public terminal error SHALL distinguish transfer failure from engine/infrastructure failure and cancellation with distinct typed variants or types; errors SHALL preserve redacted diagnostic category, available partial accounting, and retained-artifact disposition. Neither a completed value nor a task-join error SHALL silently carry an unreported failed transfer.

#### Scenario: Transfer protocol failure
- **WHEN** a remote server repeatedly violates a validated range contract
- **THEN** the caller receives a typed transfer failure with an appropriate category and retry/accounting details

#### Scenario: Infrastructure failure
- **WHEN** an internal engine operation fails independently of remote transfer semantics
- **THEN** the caller receives a typed engine/infrastructure failure, distinguishable without parsing an error string

#### Scenario: Partial outcome inspection
- **WHEN** a failed or cancelled transfer has received bytes and retained resumable artifacts
- **THEN** the caller can inspect partial accounting and artifact disposition on the error without mistaking it for a completed download
