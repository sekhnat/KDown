# Spec Delta

## Purpose

Defines safe ownership of partial files and checkpoints, sound representation identity on resume, and durability of persisted progress.

## ADDED Requirements

### Requirement: Bound and safe partial-file ownership
A fresh job SHALL NOT write to or publish a pre-existing partial file, symbolic link, or hard link outside its owned output. On resume the engine SHALL validate a regular partial file's identity against its checkpoint and retain a safe binding between the write target and the file published. Unsafe path replacement SHALL fail closed without modifying unrelated files; document any filesystem or directory-trust preconditions where this cannot be guaranteed. Checkpoint sidecar open/write/rename operations SHALL follow equivalent ownership rules.

#### Scenario: Symlink at partial path
- **WHEN** another local actor places a symlink from the prospective `.part` entry to an unrelated file
- **THEN** the unrelated file remains unchanged and the engine either safely creates its own output or rejects the job

#### Scenario: Entry replaced before publication
- **WHEN** the partial-file directory entry is replaced between write and commit
- **THEN** the engine does not publish the replacement or corrupt a file it did not open and verify

### Requirement: Resume requires comparable representation and local evidence
Checkpoint ranges SHALL be reused only when saved and current representations share a comparable strong ETag, or an eligible, matching Last-Modified validator according to the documented policy, and the partial-file identity/integrity is established. Size equality or a HEAD response without comparable identity SHALL NOT suffice, even for a fully covered checkpoint. Unsupported or legacy checkpoint formats SHALL follow a documented conservative restart-or-fail policy with no mixed-generation publication.

#### Scenario: Saved ETag disappears
- **WHEN** a complete checkpoint's old ETag is absent in the current response and content has changed without changing size
- **THEN** the old output is not published without re-fetching or verifying the current representation

#### Scenario: Local partial replaced or truncated
- **WHEN** checkpoint metadata describes completed ranges but the partial file has been replaced or cannot be verified
- **THEN** the engine refuses those ranges and safely restarts or fails per configured policy

#### Scenario: Weak and absent validators
- **WHEN** both representations have no comparable eligible validator or only a weak ETag
- **THEN** the engine does not trust previously completed bytes solely on that evidence

### Requirement: Durable checkpoint ordering
In Durable mode, for both sequential and segmented transfers, output data corresponding to a checkpoint frontier SHALL be successfully synced before that frontier is persisted; a failed sync SHALL NOT advance a durable checkpoint. Performance mode SHALL retain its explicitly weaker guarantee. Pause, cancellation, and crash recovery SHALL not advertise ranges more durable than the corresponding output bytes.

#### Scenario: Sequential sync fails
- **WHEN** output data sync fails before a sequential checkpoint save in Durable mode
- **THEN** the checkpoint does not claim those unsynced bytes and the job reports a structured failure

#### Scenario: Save fails after sync
- **WHEN** data sync succeeds but checkpoint persistence fails
- **THEN** no later resume trusts an unpersisted frontier and the job reports the persistence failure
