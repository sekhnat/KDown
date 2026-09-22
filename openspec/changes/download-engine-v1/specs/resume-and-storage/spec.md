# Spec Delta

## Purpose

Defines how the engine stores downloaded bytes locally and persists resumable progress: the sink contract, temp-file handling, positional writes, checkpoint format and durability, resume validation, pause semantics, and atomic final commit.

## ADDED Requirements

### Requirement: Temp-file sink and positional writes
The engine SHALL write all downloaded bytes to a temp file distinct from the final destination path (in the same directory when possible so final rename is atomic), SHALL write every byte at an explicit absolute offset using positional writes in segmented mode, and SHALL NOT share a mutable seek pointer between workers.

#### Scenario: Temp file never equals destination
- **WHEN** a download is in progress
- **THEN** the destination path either does not exist or still contains its previous content, and all partial data lives in a separate temp artifact in the same directory

#### Scenario: Concurrent positional writes are isolated
- **WHEN** multiple workers write to disjoint offsets of the same temp file concurrently
- **THEN** each byte lands at its requested offset with no interleaving corruption

### Requirement: Preallocation and disk errors
When total size is known, the engine SHOULD preallocate the temp file where the platform supports it, treating unsupported preallocation as non-fatal and insufficient disk space as fatal. Disk-full, quota, permission, read-only filesystem, and I/O errors SHALL be terminal: network workers SHALL stop promptly and a structured sink error SHALL surface.

#### Scenario: Disk fills mid-download
- **WHEN** the filesystem runs out of space during a transfer
- **THEN** workers stop promptly, the job fails with a structured disk-full error, and any consistent partial state is preserved for possible resume

### Requirement: Checkpoint format and atomic durability
Checkpoints SHALL be versioned, validated before use, independent of in-memory layout, and extensible across minor releases, recording at minimum job identity, URLs, temp-file identity, total size, validators, expected hashes, completed byte ranges, and timestamps. Checkpoints SHALL be replaced atomically (write-temp-then-rename with optional fsync) so a torn write never destroys the only copy, and SHALL never claim durability beyond the active policy (page-cache-acknowledged performance mode vs flushed-before-record durable mode); the active guarantee SHALL be discoverable in configuration or output.

#### Scenario: Checkpoint survives process kill
- **WHEN** the process is terminated at any point during a transfer and restarted
- **THEN** the last atomic checkpoint is intact, and resume validates it and continues from recorded completed ranges without corrupting the output

#### Scenario: Corrupt checkpoint fails safely
- **WHEN** a checkpoint file is truncated or corrupted
- **THEN** the engine does not trust it and either recovers conservatively or restarts per policy, surfacing a structured checkpoint error

#### Scenario: Checkpoint never overclaims durability
- **WHEN** the durability policy requires flushed data
- **THEN** completed ranges are recorded in the checkpoint only after the corresponding bytes are flushed, so a power loss never leaves checkpoint-claimed bytes missing

### Requirement: Resume validation
Before resuming, the engine SHALL validate the checkpoint format, verify the temp file exists with a plausible size, probe the remote resource, compare validators and total size, reconstruct remaining intervals, and resume only when the generation identity is acceptable.

#### Scenario: Resume with matching generation
- **WHEN** a resume request finds the checkpoint valid and remote validators unchanged
- **THEN** the transfer continues from recorded completed ranges and finishes without re-downloading them

#### Scenario: Resume with missing temp file
- **WHEN** the checkpoint exists but the temp file was deleted
- **THEN** the engine treats prior progress as unusable and restarts from zero per policy rather than writing to a fresh file at stale offsets

### Requirement: Pause and cancellation cleanup semantics
Pause SHALL converge quickly: stop segment assignment, stop workers at safe chunk boundaries, settle pending writes, update and persist the checkpoint, and enter Paused with resumable state when resume is enabled. Cancellation SHALL support keep-partial, delete-partial, and advanced keep-file-discard-checkpoint modes, cleaning up artifacts deterministically after workers stop.

#### Scenario: Pause then process restart
- **WHEN** a caller pauses a job and the process later restarts
- **THEN** the caller can resume the same job from the persisted checkpoint and the output is byte-identical to an uninterrupted download

#### Scenario: Cancel with delete-partial
- **WHEN** a caller cancels with delete-partial mode
- **THEN** workers stop, then the temp file and checkpoint are removed, leaving no orphaned artifacts

### Requirement: Atomic final commit
On successful verification the engine SHALL flush per durability policy, settle file handles, apply optional caller-requested metadata, atomically rename the temp file to the destination where the platform supports it, remove the checkpoint, and only then report Completed. Failure to commit SHALL never be reported as Completed even when all bytes were fetched.

#### Scenario: Commit failure is not completion
- **WHEN** the final rename fails (e.g., cross-device link or permission error)
- **THEN** the job reports a structured commit error with the partial artifacts preserved per policy, and never reports Completed

#### Scenario: Commit is atomic to observers
- **WHEN** another process reads the destination path while the final rename occurs
- **THEN** it observes either the previous file or the complete new file, never a partially written one