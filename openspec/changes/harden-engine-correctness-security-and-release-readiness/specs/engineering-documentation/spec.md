# Spec Delta

## Purpose

Keeps engineering rationale, benchmark interpretation, and release notes accurate so maintainers and users can understand invariants and avoid treating synthetic measurements as WAN promises.

## ADDED Requirements

### Requirement: Durable comments on touched orchestration code
Comments in files touched by this change and core job/control orchestration SHALL explain enduring invariants and non-obvious ordering, security, and durability rationale without depending on temporary implementation-task or bare spec-section identifiers. Comment cleanup alone SHALL NOT change behavior.

#### Scenario: Reader without task list
- **WHEN** a maintainer reviews the touched orchestration source without access to previous implementation task lists
- **THEN** comments covering race prevention, checkpoint ordering, cancellation, and security give a self-contained explanation; any durable normative cross-reference also includes plain-language rationale

### Requirement: Honest benchmark claims and CI policy
Benchmark documentation SHALL identify recorded loopback and synthetic throughput as environment-specific regression data, not general Internet-performance guarantees. It SHALL explain how server range behavior, CDN/server throttling, RTT, bandwidth, HTTP version, connection limits, storage, CPU, and environment can affect results. Hosted CI SHALL NOT enforce absolute workstation-throughput numbers; it SHALL retain functional or smoke coverage and existing local benchmark evidence.

#### Scenario: Reader interprets a local baseline
- **WHEN** a user reads the README or benchmark guide alongside local throughput records
- **THEN** they can tell the hardware/network context of those records and are not promised a WAN speedup or universal segmented-over-sequential advantage

#### Scenario: Hosted runner performance varies
- **WHEN** a hosted CI runner has lower absolute throughput than a developer workstation but benchmark smoke checks remain functionally correct
- **THEN** CI does not fail solely for missing a workstation throughput threshold

### Requirement: Release notes match verified changes
The changelog and public examples SHALL document header redaction, concurrency events, rate-limit arbitration, controller migration/deprecation or breaking rename, MSRV/CI status, and any relevant supported-platform fix, and SHALL claim fixed behaviors only when reasonably test-covered.

#### Scenario: Release migration review
- **WHEN** a user reads the next release notes and copies its controller or event example
- **THEN** the example uses the current public API and identifies how existing callers migrate
