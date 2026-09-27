# Spec Delta

## Purpose

Defines when an output contains exactly the accepted transfer and safely reusable checkpoint bytes, independent of filesystem length or sparse preallocation.

## ADDED Requirements

### Requirement: Complete accepted-byte coverage before success
For a resource with known total size the engine SHALL publish success only if verified accepted bytes, including safely admitted checkpoint ranges, cover every byte of `[0,total)` exactly as intended. File length, preallocation, sparse holes, and a successful flush SHALL NOT substitute for coverage. Expected digests, when supplied, SHALL still be checked over the assembled output before publication.

#### Scenario: Short response with preallocation
- **WHEN** metadata advertises six bytes but the actual sequential body provides only three, with output preallocation enabled
- **THEN** the job returns a typed failure, does not publish a file, and does not count the missing three bytes as complete

#### Scenario: Segmented completion with a gap
- **WHEN** accepted ranges leave a hole despite a full-length output file
- **THEN** verification fails before any committed event or publication

### Requirement: Exact unknown-length output
For a resource with no trustworthy declared length the engine SHALL publish only bytes up to the acknowledged end of the successful stream, with no stale tail, and SHALL NOT treat an interrupted or unverified stream as a successful EOF.

#### Scenario: Existing stale partial followed by short unknown-length body
- **WHEN** an unknown-length transfer yields `new` and an old `.part` contains a longer tail
- **THEN** a successful output, if published, is exactly `new`, never the old tail

#### Scenario: Interrupted body
- **WHEN** the stream fails before normal EOF
- **THEN** the job fails without publishing, regardless of the size of an existing or preallocated file
