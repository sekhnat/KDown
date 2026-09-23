# Spec Delta

## Purpose

Defines how a download job selects and uses a substitutable checkpoint store across its full lifecycle, including ordered persistence and observable failure behavior.

## ADDED Requirements

### Requirement: Per-job checkpoint adapter selection
The engine SHALL allow a thread-safe checkpoint-store resolver to be supplied when a controller is constructed. The resolver SHALL receive the job context needed to select storage, including the destination and configured durability, and SHALL return one shareable checkpoint-store adapter that the job retains for its entire lifecycle. Existing production controller construction SHALL remain supported and SHALL resolve to the destination-relative file-sidecar adapter by default.

#### Scenario: Existing construction uses destination sidecar
- **WHEN** a caller uses an existing production controller constructor and starts a download
- **THEN** the job stores checkpoints beside the destination with the configured durability without requiring caller migration

#### Scenario: Alternate adapter is selected once
- **WHEN** a caller constructs a controller with an alternate checkpoint-store resolver and starts a job
- **THEN** the resolver selects one adapter for that job and every checkpoint operation for that job is sent to that adapter

#### Scenario: Controller serves different destinations
- **WHEN** one controller starts jobs whose destinations have different parent directories
- **THEN** the resolver receives each job's destination context and can select an appropriate request-scoped adapter independently

#### Scenario: Adapter resolution fails
- **WHEN** the configured resolver cannot provide a checkpoint store for a job
- **THEN** the job fails before probing with a checkpoint-category error

### Requirement: Selected adapter spans the complete job lifecycle
Resume admission, resume-state refresh, transfer checkpoint cadence, pause persistence, cancellation cleanup, segmented-worker persistence, and successful-commit cleanup SHALL use the selected checkpoint-store adapter. Job orchestration MUST NOT require a concrete checkpoint-store implementation or switch adapters during a job.

#### Scenario: Sequential lifecycle uses alternate storage
- **WHEN** a sequential job uses a conforming alternate adapter, resumes, persists progress, pauses, and completes
- **THEN** its load, save, and terminal delete operations cross the selected adapter without file-sidecar-specific behavior in orchestration

#### Scenario: Segmented workers use alternate storage
- **WHEN** a segmented job runs multiple workers with a conforming alternate adapter
- **THEN** worker checkpoint saves and terminal cleanup use that same selected adapter without requiring adapter-specific worker branches

#### Scenario: Cancellation uses alternate storage
- **WHEN** a job using an alternate adapter is cancelled with a mode that discards checkpoint state
- **THEN** checkpoint cleanup is requested from that adapter after transfer activity has stopped

### Requirement: Checkpoint mutations are ordered per job
Checkpoint save and delete operations for one job SHALL be coordinated so that at most one mutation is active at a time. A later progress snapshot MUST NOT be overwritten by an older snapshot from the same resource generation, and terminal deletion SHALL wait for or supersede outstanding saves so no save can recreate state after cleanup.

#### Scenario: Concurrent workers reach checkpoint cadence
- **WHEN** multiple segmented workers request checkpoint persistence concurrently
- **THEN** the adapter observes non-overlapping saves whose accepted progress does not regress within the active generation

#### Scenario: Cleanup races with persistence
- **WHEN** cancellation or successful commit requests checkpoint deletion while a save is pending
- **THEN** mutation coordination settles the save/delete order and no later save recreates the deleted checkpoint

### Requirement: Checkpoint save failures fail the active job
A failure to save a checkpoint during resume-state refresh, transfer cadence, or pause SHALL become a structured checkpoint failure. The engine SHALL stop further transfer work, SHALL NOT report the job as completed, and SHALL preserve consistent partial output for diagnosis or recovery rather than silently continuing without the promised checkpoint state.

#### Scenario: Sequential cadence save fails
- **WHEN** checkpoint persistence fails during sequential transfer cadence
- **THEN** the job terminates as Failed with a checkpoint-category error and preserves its consistent partial output

#### Scenario: Pause save fails
- **WHEN** checkpoint persistence fails while a job is converging to a resumable paused state
- **THEN** the job does not enter or remain successfully Paused and instead terminates as Failed with a checkpoint-category error

#### Scenario: Segmented save fails
- **WHEN** a segmented worker observes a checkpoint save failure
- **THEN** the failure is propagated to shared job coordination, all workers converge, and the job terminates as Failed with that checkpoint error

### Requirement: Checkpoint delete failures preserve the appropriate primary outcome
A checkpoint deletion required to discard unsafe state before transfer SHALL remain fail-closed: failure SHALL stop the job with a structured checkpoint error. If deletion fails only after the destination has committed successfully or after cancellation has already determined the terminal outcome, the engine SHALL preserve Completed or Cancelled respectively and SHALL include an actionable checkpoint-cleanup warning in the terminal result.

#### Scenario: Admission cleanup delete fails
- **WHEN** stale, corrupt, or locally unusable checkpoint state must be deleted before a fresh transfer and the adapter rejects deletion
- **THEN** the job fails with a checkpoint-category error before transfer begins

#### Scenario: Delete fails after successful commit
- **WHEN** the destination commits successfully but checkpoint cleanup fails
- **THEN** the result remains Completed with the committed final path and includes a warning that checkpoint state may remain

#### Scenario: Delete fails during cancellation cleanup
- **WHEN** cancellation selects a mode that discards checkpoint state and deletion fails after workers stop
- **THEN** the result remains Cancelled and includes a warning that checkpoint cleanup was incomplete

### Requirement: Default sidecar durability is preserved
The default file-sidecar adapter SHALL continue to replace checkpoints atomically so an interrupted save does not expose a torn checkpoint. Under durable mode it SHALL synchronize checkpoint contents before replacement and synchronize the containing directory where supported so the replacement itself survives a crash. Missing checkpoint deletion SHALL remain successful.

#### Scenario: Atomic replacement is interrupted
- **WHEN** a file-sidecar checkpoint replacement is interrupted at any point
- **THEN** a subsequent load observes either the previous complete checkpoint or the new complete checkpoint, never a torn representation

#### Scenario: Durable save succeeds
- **WHEN** durable mode reports a checkpoint save as successful on a platform supporting directory synchronization
- **THEN** checkpoint contents and the directory entry replacement have both been synchronized

#### Scenario: Missing sidecar is deleted
- **WHEN** cleanup requests deletion of a sidecar that does not exist
- **THEN** deletion succeeds without changing the job outcome
