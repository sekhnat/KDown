# Spec Delta

## Purpose

Defines the caller-visible download request, destination conflict policy, and terminal outcome contract so clients can trust that publication respects the selected overwrite policy even when the filesystem changes during a transfer.

## ADDED Requirements

### Requirement: Conflict-safe publication
For a job using `FailIfExists`, the engine SHALL ensure that the final publication does not replace a destination that exists at the moment of publication. The check and publication SHALL be one race-safe operation, not a separate check followed by an unconditional replacement. A conflict SHALL yield a structured failed result, preserve the conflicting destination byte-for-byte, and SHALL NOT report completion or emit a committed event. An existing destination at admission SHALL still be rejected before network activity. Existing `Replace` and validator-gated resume policies SHALL retain their documented meanings.

#### Scenario: Destination already exists at admission
- **WHEN** a `FailIfExists` job starts with an existing destination
- **THEN** the engine rejects it without network activity and leaves the destination unchanged

#### Scenario: Destination appears while downloading
- **WHEN** another actor creates a destination after admission but before a `FailIfExists` job publishes
- **THEN** the job fails with a structured conflict outcome, preserves that actor's bytes, and emits no committed event

#### Scenario: No competing destination
- **WHEN** a `FailIfExists` job finishes verification and the destination remains absent through publication
- **THEN** it publishes the complete verified file and reports `Completed`

### Requirement: Predictable outcome for simultaneous destination jobs
The engine SHALL prevent simultaneously active jobs, including jobs started by separate controllers or processes on the same filesystem, from corrupting one another's output or checkpoint state. It SHALL either safely isolate their artifacts and publication decisions or reject a conflicting job with a structured terminal error before that job mutates shared artifacts. A later job SHALL be able to resume safely after the earlier process exits, subject to the existing resume policy.

#### Scenario: Competing jobs for one destination
- **WHEN** two jobs target the same destination concurrently
- **THEN** no output contains interleaved bytes, no checkpoint claims another job's data, and any rejected job reports a clear failed outcome

#### Scenario: Jobs for different destinations
- **WHEN** two jobs target different destinations in one directory
- **THEN** they can proceed without sharing mutable output or checkpoint state

#### Scenario: Process exits with partial output
- **WHEN** a process ends while owning partial output and a later job targets that destination
- **THEN** the later job may safely resume or reject per policy without permanent ownership lockout or damage to a live job's artifacts
