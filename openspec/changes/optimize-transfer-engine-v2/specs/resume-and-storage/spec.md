# Spec Delta

## Purpose

Defines bounded, positional output and acknowledged resume boundaries so overlapping network and storage work cannot compromise safe checkpointing, integrity verification or atomic publication.

## ADDED Requirements

### Requirement: Bounded shared positional writing
Independent segmented requests SHALL be able to submit positional writes without dedicating a blocking filesystem thread to each possible network worker. Total unacknowledged payload retained by the engine MUST stay within a configured byte budget, with fair admission across active jobs where shared. A network reader MUST stop ingesting further body payload before exceeding its admitted budget; write capacity SHALL be released on success, failure and cancellation. The limit applies to engine-owned pending writes, not an undocumented promise to bound transport/kernel buffers.

#### Scenario: Slow storage and multiple jobs
- **GIVEN** multiple jobs with faster network reception than disk writes
- **WHEN** writes queue and budget is exhausted
- **THEN** readers backpressure, engine-owned outstanding bytes stay under configured limits, and other jobs eventually receive writer service

#### Scenario: Cancel or disk full with queued writes
- **GIVEN** queued and executing writes with all byte capacity consumed
- **WHEN** a job cancels or a write fails with ENOSPC/permission denied
- **THEN** unacknowledged bytes are not checkpointed, all capacity is eventually released, a structured sink error is reported where appropriate and no incomplete destination is published

### Requirement: Safe ordered acknowledgement and checkpointing
A write is acknowledged only when its complete positional operation returns successfully at the selected write boundary. Performance-mode checkpoint coverage SHALL contain only contiguous acknowledged bytes; durable-mode coverage MUST additionally follow successful data synchronization before atomic checkpoint persistence. Out-of-order writes, a missing completion notification, a short write and a failed synchronization MUST NOT advance the eligible frontier across a gap. A save cannot race cleanup after cancellation or commit.

#### Scenario: Out-of-order write and pause
- **GIVEN** a later chunk is acknowledged while an earlier chunk remains queued
- **WHEN** the job pauses and saves a checkpoint
- **THEN** the checkpoint includes only the contiguous acknowledged prefix (synchronized first in durable mode) and remains safe to resume after restart

#### Scenario: Failed sync or checkpoint store
- **GIVEN** valid acknowledged data
- **WHEN** durable sync fails or the atomic checkpoint save fails
- **THEN** the snapshot is not reported as durably persisted, failure is surfaced and the prior valid checkpoint remains the resume baseline

### Requirement: Safe draining and final publication
Pause, cancellation, retry, generation invalidation and job completion SHALL settle or explicitly invalidate queued work before a file is reclaimed or ownership transferred. Only the output owner SHALL verify final exact length and requested SHA-256/SHA-512, synchronize as configured, and atomically publish according to the selected collision policy. Final state MUST NOT be Completed before verification and publication succeed.

#### Scenario: Cancellation during queued output
- **GIVEN** active downloads with queued writes
- **WHEN** cancellation chooses keep-partial or delete-partial
- **THEN** network and writes converge without deadlock, no late write or checkpoint races cleanup, and the selected artifact policy is honored

#### Scenario: Hash mismatch or destination collision
- **GIVEN** fully received output whose hash mismatches, or a destination appears before a no-replace commit
- **WHEN** finalization runs
- **THEN** the job fails without publishing unverified bytes or overwriting the competing destination; pre-existing replacement targets remain safe

#### Scenario: Restart after partial output
- **GIVEN** a process stops with a valid partial file and checkpoint
- **WHEN** resume validates identity, ranges, length and remote validators
- **THEN** only accepted ranges are reused and unfinished ranges are fetched without mixing representations

### Requirement: Portable preallocation does not weaken output safety
Logical preallocation SHALL remain distinct from optional physical reservation. Unsupported physical allocation SHALL fall back to logical sizing; genuine space or permission failures SHALL be surfaced without publishing incomplete output. A default-policy change requires comparative filesystem measurements and compatibility review.

#### Scenario: Unsupported allocation and real failure
- **GIVEN** one filesystem without physical reservation and one returning real ENOSPC
- **WHEN** physical reservation is requested
- **THEN** the first continues safely with logical sizing and the second fails visibly, with no incomplete published destination
