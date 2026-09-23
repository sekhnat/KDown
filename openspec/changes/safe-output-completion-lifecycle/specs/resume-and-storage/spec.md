# Spec Delta

## Purpose

Defines safe ownership of temporary output and checkpoint state and the durable, verified publication lifecycle shared by sequential and segmented downloads, including preservation of a prior destination on failure.

## ADDED Requirements

### Requirement: Exclusive mutable-artifact ownership
The engine SHALL keep a job's temporary output and checkpoint mutations isolated from other active jobs targeting the same destination, including across processes sharing a filesystem. An inability to establish safe ownership SHALL fail closed before shared artifacts are written. A failed or cancelled job SHALL NOT remove another job's artifacts. Ownership left behind by an interrupted process SHALL NOT prevent safe, policy-compliant recovery indefinitely.

#### Scenario: Concurrent output preparation
- **WHEN** a second live job attempts to prepare output for a destination already targeted by another live job
- **THEN** neither job can write into or delete the other's partial output or checkpoint

#### Scenario: Interrupted owner and later resume
- **WHEN** a job is killed after writing a partial file and a later job seeks to resume
- **THEN** the later job validates prior output and checkpoint state under the existing resume policy and does not blindly discard or trust it

### Requirement: Non-destructive replacement
For `Replace`, the engine SHALL publish a fully verified new file atomically to observers wherever the filesystem offers a safe atomic replacement operation. If such a replacement is unavailable or fails, the engine SHALL fail without first deleting or modifying the pre-existing destination. For `FailIfExists`, publication SHALL not overwrite an existing destination even if it appeared after transfer began. No verification, finalization, or publication failure SHALL be reported as `Completed`.

#### Scenario: Successful replacement
- **WHEN** a verified job replaces an existing destination using `Replace`
- **THEN** an observer sees either the complete old file or the complete new file, not a missing or partially written destination

#### Scenario: Replacement cannot proceed safely
- **WHEN** the platform cannot safely replace the old destination or publication fails
- **THEN** the job reports a structured failure and the old destination retains its bytes

#### Scenario: Verification failure preserves old destination
- **WHEN** exact-size or caller-required hash verification fails before publication
- **THEN** the old destination is unchanged and no committed event is emitted

### Requirement: Shared verified completion contract
Both sequential and segmented jobs SHALL check exact size when known, verify caller-required SHA-256/SHA-512 hashes over assembled output, settle data according to the configured durability level, publish according to overwrite policy, attempt checkpoint cleanup, and only then report the terminal outcome. Segmented whole-file hashes SHALL reflect the assembled file's bytes in sequential order. The existing integrity, state, warning, and committed-event ordering SHALL be preserved. A post-publication checkpoint-delete failure SHALL leave the successful outcome and final path intact while surfacing an actionable warning in the result and event stream; a pre-transfer deletion required for safe resume SHALL remain fatal on failure.

#### Scenario: Both modes complete successfully
- **WHEN** either transfer mode finishes with an expected size and matching digest
- **THEN** size and digest verification precede durable finalization and publication, checkpoint cleanup is attempted before `Completed`, and the result identifies the verified final path

#### Scenario: Failure before publication
- **WHEN** either mode encounters a size mismatch, digest mismatch, output flush/finalize error, or publication error
- **THEN** the job fails with a structured error, leaves any old destination unchanged, and never emits a committed event

#### Scenario: Cleanup fails after publication
- **WHEN** checkpoint deletion fails after the verified file was successfully published
- **THEN** the job remains `Completed` with the final path and reports a cleanup warning both in its result and event stream

#### Scenario: Pause and cancellation preserve policy
- **WHEN** either transfer mode is paused or cancelled while work is active
- **THEN** workers settle before artifact disposition, and partial output and checkpoint state follow the selected pause/cancel and durability policies
