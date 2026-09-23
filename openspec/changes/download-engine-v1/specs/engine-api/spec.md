# Spec Delta

## Purpose

Defines the public programmatic surface of the download engine: how a host application creates, configures, controls, and observes a download job, independent of any UI or application framework.

## ADDED Requirements

### Requirement: Engine creation and shared resources
The engine SHALL expose a constructor that accepts a validated engine configuration and SHALL own all shared transfer infrastructure (connection pooling, DNS/TLS settings, buffer pools, rate-limit hooks, metrics sink). The engine MUST NOT depend on GUI frameworks, application-global singletons, or environment-specific state.

#### Scenario: Engine constructed from configuration
- **WHEN** a host creates an engine with an engine configuration
- **THEN** the engine is ready to accept probe, start, resume, and shutdown calls, and no network activity has begun

#### Scenario: Engine shutdown
- **WHEN** the host calls shutdown with a graceful mode while jobs are active
- **THEN** the engine stops issuing new work, allows in-flight jobs to settle per mode, and returns once all owned resources are released

### Requirement: Job start, resume, and handles
The engine SHALL accept a download request describing the URL, destination, headers, optional credentials provider, expected size, expected hashes, and policy selections, and SHALL return a handle that is safe to call concurrently from multiple tasks/threads.

#### Scenario: Start returns a handle
- **WHEN** a caller submits a valid download request
- **THEN** the engine returns a handle exposing the job id, progress snapshot, pause, cancel, limit adjustments, an event stream, and a terminal result

#### Scenario: Resume from persisted state
- **WHEN** a caller submits a resume request for a job with a valid checkpoint
- **THEN** the engine validates the checkpoint and remote resource, reconstructs remaining ranges, and continues the transfer without re-downloading completed ranges

#### Scenario: Resume of invalid checkpoint state
- **WHEN** a resume request references missing, corrupt, or generation-incompatible state
- **THEN** the engine fails safely with a structured error or restarts from zero according to the configured resume policy, and never appends bytes to mismatched data

### Requirement: Job lifecycle state machine
Each job SHALL progress through an explicit state machine (Created → Probing → Preparing → Running → … → Verifying → Committing → Completed) where Completed, Cancelled, and Failed are terminal, no worker writes after a terminal transition, Paused guarantees all network workers have stopped and a valid checkpoint exists when resume is enabled, failure to commit is never reported as Completed, and the public state is monotonic except Paused → Running.

#### Scenario: Normal completion ordering
- **WHEN** a job finishes transferring all bytes
- **THEN** observed states include Verifying then Committing before Completed, and Completed is emitted only after integrity checks and the final commit succeed

#### Scenario: Pause convergence
- **WHEN** a caller pauses a running job
- **THEN** network activity stops promptly, pending writes settle, the completed interval set is updated and persisted, and the job reports Paused

#### Scenario: Cancel from any operational state
- **WHEN** a caller cancels a job from any non-terminal state
- **THEN** the job reaches Cancelled terminal state and no further network reads or sink writes occur afterward

### Requirement: Runtime limit adjustment
The handle SHALL allow changing the per-job rate limit and worker concurrency while the job is running, taking effect without restarting workers or re-downloading data.

#### Scenario: Rate limit changed mid-transfer
- **WHEN** a caller sets a lower rate limit while a segmented download is running
- **THEN** the network read rate converges to the new limit without job restart and without losing completed progress

#### Scenario: Concurrency reduced mid-transfer
- **WHEN** a caller reduces the concurrency below the current active worker count
- **THEN** excess workers finish or abandon their current leases safely, no byte range is lost, and the job continues with fewer workers

### Requirement: Configuration validation
The engine SHALL validate configuration values (concurrency bounds, timeouts, buffer sizes, segment size bounds, retry policy) at construction/request time and reject invalid combinations with a structured configuration error before any network activity.

#### Scenario: Invalid configuration rejected
- **WHEN** a configuration specifies contradictory or out-of-range values (e.g., min workers > max workers, zero timeout)
- **THEN** the engine returns a structured configuration error and does not start the job

### Requirement: Destination conflict policy
The engine SHALL honor the selected overwrite policy on the destination. `FailIfExists` SHALL reject a destination that already exists before network activity and SHALL use an atomic no-replace publication operation so a destination created during transfer is never overwritten; if that operation is unsupported, the job SHALL fail closed without publishing. `Replace` SHALL use safe atomic replacement where the platform and filesystem support it; if replacement fails or is unsupported, the engine SHALL return a structured failure without deleting or modifying the prior destination. Automatic renaming is delegated to the embedding layer. `ResumeIfMatching` SHALL resume only when validator identity matches and SHALL use replacement semantics for final publication.

#### Scenario: Destination exists with FailIfExists
- **WHEN** the destination exists at admission or is created before publication while a `FailIfExists` job is running
- **THEN** the job fails with a structured conflict, leaves the existing entry and its bytes untouched, and does not report `Completed` or emit a committed event

#### Scenario: Destination exists with Replace
- **WHEN** a job completes verification with `Replace` selected and the platform supports safe atomic replacement
- **THEN** the destination is replaced atomically so observers see either the complete old file or the complete new file

#### Scenario: Replace cannot proceed safely
- **WHEN** atomic replacement is unsupported or fails
- **THEN** the job reports a structured commit failure and preserves the old destination bytes