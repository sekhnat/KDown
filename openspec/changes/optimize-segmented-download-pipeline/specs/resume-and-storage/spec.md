# Spec Delta

## Purpose

Defines how segmented output is written and persisted for safe resume and final publication, including failure boundaries and portable storage behavior.

## ADDED Requirements

### Requirement: Concurrent positional segmented output
Segmented workers SHALL write complete buffers at their assigned absolute offsets without sharing or modifying a common file cursor or acquiring a job-global output lock on ordinary writes. Disjoint writes SHALL yield identical final contents irrespective of scheduling order. Short writes SHALL be retried until complete or reported as a structured sink failure. Only the output owner SHALL synchronize, abort, finalize and publish the file; all writers MUST finish before finalization.

#### Scenario: Interleaved writes
- **WHEN** multiple workers write disjoint small or unaligned ranges out of order, including ranges at large offsets
- **THEN** every successful range is present at its exact assigned position, independent of completion order

#### Scenario: Partial and failed writes
- **WHEN** an underlying positional operation writes only a prefix or returns an error
- **THEN** the remainder is written at the advanced offset or a sink error is reported, and unwritten bytes are never marked complete

#### Scenario: Concurrent completion and publication
- **WHEN** workers finish all validated ranges and verification succeeds
- **THEN** the owner alone completes final synchronization and atomic publication after all writes end; destination replacement/no-replace policy and final checksum remain correct

### Requirement: Checkpoint coverage reflects selected durability
Written progress, durably synchronized progress and persisted checkpoint coverage SHALL be distinguishable. In performance mode, a checkpoint MAY contain ranges whose writes have completed in the OS cache, without claiming power-loss durability. In durable mode, the output file MUST be synchronized successfully before corresponding coverage is passed to atomic checkpoint persistence; a failed sync MUST NOT advance checkpoint coverage. Checkpoint coverage MAY lag valid output but MUST NOT include bytes not acknowledged under the selected mode. Inclusive completed intervals SHALL never contain duplicate committed byte coverage or stale-generation data.

#### Scenario: Failure between write and sync
- **WHEN** a durable job writes range data and fails before successful file synchronization
- **THEN** the checkpoint does not advance to include those bytes

#### Scenario: Failure between sync and save
- **WHEN** file synchronization succeeds but the process fails before checkpoint persistence
- **THEN** the previous checkpoint remains a safe resume baseline, even though the file contains additional durable data

#### Scenario: Failure after checkpoint save
- **WHEN** durable data synchronization and durable checkpoint persistence both succeed before interruption
- **THEN** resume can reuse the recorded ranges after normal identity, validator, file and range checks

#### Scenario: Data sync or checkpoint error
- **WHEN** file synchronization or checkpoint persistence fails
- **THEN** the job reports the appropriate failure and does not treat a newer snapshot as safely persisted; on restart it revalidates whichever atomic checkpoint is visible against output, identity and validators, retaining prior valid state where the backend can guarantee it

#### Scenario: Stale generation
- **WHEN** progress from a superseded lease or validator generation arrives during reconciliation
- **THEN** it is excluded from checkpoint coverage and retry/resume remain generation-safe

### Requirement: Job-level checkpoint coordination
A segmented job SHALL schedule checkpoints at the configured interval and at required pause and terminal boundaries, reconcile coherent written ranges once per checkpoint attempt, skip unchanged snapshots, and persist from one job-level authority rather than from independent network workers. Checkpoint serialization and blocking filesystem operations MUST NOT run on the latency-sensitive chunk path. Cancellation SHALL preserve or delete partial data and checkpoints in accordance with the existing cancellation mode; no checkpoint task may race post-commit cleanup.

#### Scenario: Pause or keep-partial cancellation
- **WHEN** the job pauses or is cancelled with keep-partial policy while ranges have been written
- **THEN** the coordinator settles eligible progress according to durability mode before reporting a resumable state, and cleanup cannot race a late save

#### Scenario: No new progress
- **WHEN** a checkpoint interval elapses with no new eligible coverage
- **THEN** the coordinator does not rewrite the checkpoint

#### Scenario: Persistence fails
- **WHEN** a scheduled or pause-boundary save fails
- **THEN** the job converges on one terminal failure without reporting a successfully persisted newer snapshot

### Requirement: Portable output allocation
When configured, output preparation SHALL retain portable logical sizing and MAY reserve physical storage on a supported platform. Unsupported physical allocation SHALL fall back safely; genuine permission or out-of-space failures SHALL surface as sink errors. Allocation is never necessary for correctness.

#### Scenario: Unsupported allocation
- **WHEN** physical reservation is unavailable for the output filesystem
- **THEN** the download continues with portable sizing and correct positional writes

#### Scenario: Space exhaustion
- **WHEN** physical allocation or a later write fails due to insufficient space
- **THEN** the job reports a storage failure without publishing an incomplete destination
