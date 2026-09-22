# Spec Delta

## Purpose

Defines the integrity guarantees of the engine: what is verified, when, and what happens on failure, so a completed file is byte-correct or explicitly not produced.

## ADDED Requirements

### Requirement: Caller-supplied hash verification
The engine SHALL verify caller-provided expected hashes for at least SHA-256 and SHA-512 before commit, MAY accept MD5/SHA-1 for compatibility while not presenting them as collision-resistant guarantees, and SHALL give caller-provided hashes precedence over any server-provided digest headers. A mismatch SHALL fail the job with a structured integrity error and prevent commit.

#### Scenario: SHA-256 match commits
- **WHEN** a download completes and the computed SHA-256 equals the expected digest
- **THEN** verification passes and the commit proceeds

#### Scenario: SHA-512 mismatch fails without commit
- **WHEN** the computed SHA-512 differs from the expected digest
- **THEN** the job fails with a structured integrity error identifying the algorithm, and no destination file is created or replaced

#### Scenario: Legacy digest not treated as strong guarantee
- **WHEN** only an MD5 or SHA-1 expectation is provided
- **THEN** verification proceeds but results/warnings indicate the weaker guarantee rather than claiming collision-resistant integrity

### Requirement: Exact size verification
For downloads with a known total size, the engine SHALL verify the final byte length equals the established total before commit and SHALL fail with a structured error otherwise.

#### Scenario: Size mismatch fails
- **WHEN** a known-size download finishes with fewer or more bytes than the established total
- **THEN** the job fails with a structured error and does not commit

### Requirement: Verification precedes commit
All integrity requirements (size and caller-required hashes) SHALL pass before the job may enter the commit phase, and no Completed result SHALL be emitted before verification and commit both succeed.

#### Scenario: Verification runs before rename
- **WHEN** a job with an expected hash finishes transferring all bytes
- **THEN** the integrity check completes (pass or fail) before the temp file is renamed to the destination, and a failure leaves the destination untouched

### Requirement: Whole-file hashing strategy for segmented transfers
For segmented random-access downloads, the engine SHALL verify whole-file hashes with a sequential read of the completed file (or an equivalent correct method), rather than deriving a whole-file digest from unrelated per-segment hashes; single-stream transfers MAY hash during the transfer.

#### Scenario: Segmented download hash is computed over final content
- **WHEN** a segmented download with an expected hash completes
- **THEN** the digest is computed from a full sequential read of the assembled temp file, guaranteeing the hash reflects the exact final content

### Requirement: Server-provided digests subordinate
Server-provided digest headers SHALL be recorded and validated only when trustworthy and supported, and caller-provided expected hashes SHALL take precedence when both exist.

#### Scenario: Caller hash overrides server digest
- **WHEN** both a server digest header and a caller-provided expected hash are available and they disagree
- **THEN** the caller's expectation governs the verification outcome and any discrepancy is surfaced as a warning or failure per policy