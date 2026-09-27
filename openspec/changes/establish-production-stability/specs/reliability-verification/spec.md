# Spec Delta

## Purpose

Provides reproducible cross-platform, adverse-condition, security, and state-machine evidence required before claiming the download engine production-stable.

## ADDED Requirements

### Requirement: Reproducible real-world failure coverage
Automated verification SHALL cover controlled latency, jitter, packet loss, bandwidth limits, dropped/reset connections, missing/incorrect/inconsistent HTTP Range responses, proxy and CDN behaviors, slow/failing disks, and high concurrent-job counts. It SHALL verify byte-exact output, no incorrect publication, consistent checkpoints, bounded retries/resources, and actionable errors. Linux, macOS, and Windows filesystem suites SHALL exercise relevant platform-specific path, rename/overwrite, sharing/locking, sparse-file, and crash/restart edge cases, with unsupported cases documented instead of silently skipped. A confirmed production correctness or durability defect MUST gain a reproducible automated regression case before closure.

#### Scenario: Unreliable CDN and proxy path
- **WHEN** a proxied segmented transfer encounters inconsistent range metadata, loss, and a reset connection
- **THEN** the suite establishes that the engine either safely resumes/retries to exact verified output or fails without publishing corrupted output

#### Scenario: Disk stalls across many jobs
- **WHEN** a delayed or failing sink operates with high job concurrency
- **THEN** the suite verifies safe termination, checkpoint correctness, and aggregate resource limits

#### Scenario: Cross-platform publication
- **WHEN** platform-specific filesystem publication and recovery tests run on each supported OS
- **THEN** observed unsupported operations fail safely and existing destination data is not silently destroyed

### Requirement: Layered CI and auditable release evidence
PR CI SHALL run cross-platform correctness, dependency/vulnerability audit, and bounded deterministic property/fuzz-corpus checks. Applicable sanitizer or equivalent dynamic checks, targeted Miri/concurrency validation, time-budgeted real fuzz runs, and long-duration critical-state-machine stress SHALL run in separate scheduled/release jobs on supported environments. A failure, missing required job, unavailable tool without an approved equivalent, or untriaged high-severity finding MUST block the production release; results and justified platform exceptions SHALL be recorded in a release evidence report.

#### Scenario: Vulnerable dependency
- **WHEN** dependency auditing reports an untriaged high-severity vulnerability affecting supported configurations
- **THEN** the production release gate fails until remediated or formally reviewed and an applicable exception is documented

#### Scenario: Stress failure replay
- **WHEN** a scheduled concurrency/fuzz/stress run discovers a state-machine failure
- **THEN** its seed, environment, and reproduction steps are retained as a regression test and release is blocked pending resolution

#### Scenario: Unsupported dynamic checker
- **WHEN** a sanitizer or Miri cannot execute a platform-specific test
- **THEN** evidence identifies the limitation and runs an applicable equivalent check or explicitly blocks release pending review rather than reporting that test as passing
