# Spec Delta

## Purpose

Makes the security and correctness guarantees independently reproducible across supported protocols, transfer modes, and platforms.

## ADDED Requirements

### Requirement: Isolated regression fixtures for reported defects
The verification suite SHALL exercise credential redirects, short/preallocated and stale-tail outputs, fully complete/missing-validator checkpoints, partial-file symlink or entry races, durable save ordering, segmented authentication, deadline/cancellation waits, checkpoint budget violations, amplification, terminal subscriptions, URL-secret leaks, and stale release evidence using dummy URLs, secrets, and scratch files. Where applicable it SHALL compare H1/H2 and sequential/segmented outcomes, require byte-exact success or no publication on failure, and preserve failing seeds/fixtures.

#### Scenario: P0 regression reintroduced
- **WHEN** a loopback server redirects credentials to an untrusted origin, supplies a short body, or presents a missing validator on a complete checkpoint
- **THEN** the targeted automated test fails on the unsafe behavior before a release can proceed

#### Scenario: Hostile local partial path
- **WHEN** a fixture replaces an output entry or installs a symlink to an unrelated scratch file
- **THEN** the test asserts that the unrelated bytes remain unchanged and an unverified file is never published

### Requirement: Auditable project-facing verification
Supported-platform CI SHALL run relevant locked suites and safe filesystem/admission regressions; release verification SHALL publish candidate-linked evidence for required platform, fuzz, dynamic, dependency, crash/restart, and matched-environment performance checks or state an explicit blocker. Public API documentation and compile checks SHALL reference existing files and validate the supported exported signatures; workspace lint configuration SHALL actually apply to the engine member. Passing targeted regressions SHALL NOT be represented as a full-workspace or all-platform audit.

#### Scenario: Missing evidence on a supported platform
- **WHEN** the release lacks a mandatory platform or dynamic-check result
- **THEN** its checklist reports the missing evidence and the stability gate stays blocked

#### Scenario: Broken public reference or lint wiring
- **WHEN** a root rustdoc link points to an absent file or the engine has not opted into workspace lints
- **THEN** automated project checks flag the drift rather than silently accepting it
