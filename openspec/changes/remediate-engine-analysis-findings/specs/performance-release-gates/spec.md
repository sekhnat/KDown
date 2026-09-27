# Spec Delta

## Purpose

Requires complete, current, candidate-revision-bound release evidence so smoke checks cannot be mistaken for production approval.

## ADDED Requirements

### Requirement: All prerequisite evidence is fresh and revision-bound
Every required gate, including non-approving PR/smoke prerequisites, SHALL be checked for passing status, a valid non-future timestamp within its configured freshness window, and a match to the supplied release revision before being counted satisfied. A production-stability check with no release revision SHALL fail closed; non-approving evidence SHALL NOT count as a release-approving category. Documented, time-bounded reviewed exceptions SHALL remain explicit and visible, never implicit passes.

#### Scenario: Old passing smoke from a different commit
- **WHEN** a non-approving smoke fragment passes but is stale or names another commit
- **THEN** the production release verdict is blocked and identifies that prerequisite

#### Scenario: Missing candidate commit
- **WHEN** production-stability evaluation runs without an explicit candidate revision
- **THEN** it cannot report production stable

#### Scenario: Passing fresh PR smoke alone
- **WHEN** smoke evidence is current and bound to the candidate but release-approving evidence is missing
- **THEN** the prerequisite can be satisfied but the production verdict stays blocked

### Requirement: Production stability requires verified candidate evidence
The release report SHALL not declare production stability while P0/P1 correctness, security, durability, or admission defects remain open, or required cross-platform, fuzz, dynamic, dependency, stress, and controlled multi-axis performance evidence is missing, stale, noisy, or tied to a different candidate revision. Metrics SHALL distinguish managed-memory caps from total RSS and report unsupported checks and reviewed exceptions transparently.

#### Scenario: Green synthetic benchmark but unresolved security regression
- **WHEN** benchmark profiles pass while a reproduced cross-origin credential leak remains open
- **THEN** the release report does not claim production stability

#### Scenario: Unsupported dynamic checker
- **WHEN** a required checker cannot run on a candidate platform and no reviewed equivalent or exception exists
- **THEN** the corresponding release gate remains blocked
