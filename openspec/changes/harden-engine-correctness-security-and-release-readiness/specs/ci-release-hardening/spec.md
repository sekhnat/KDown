# Spec Delta

## Purpose

Ensures the engine's supported-platform and supported-Rust-version claims are exercised by reproducible, diagnostically useful CI rather than inferred from a single host.

## ADDED Requirements

### Requirement: Supported-platform verification
The repository SHALL build and run the full applicable workspace test and lint suites on Linux, macOS, and Windows; failures SHALL identify the failing check and test. A platform difference SHALL be isolated without suppressing tests or weakening correctness, storage, network, or security guarantees.

#### Scenario: Windows positional I/O regression
- **WHEN** workspace targets compile and tests run on Windows
- **THEN** the positional-file test compiles using a supported Windows read strategy and verifies disjoint absolute writes and exact read-back, including an offset beyond 4 GiB where feasible

#### Scenario: macOS adaptive-concurrency regression
- **WHEN** the macOS test matrix runs the H1 marginal-benefit scenario
- **THEN** the test deterministically checks that unhelpful additional connections cause the adaptive desired concurrency to revert while the transfer finishes byte-exactly; a failure exposes the measured state or unmet invariant

#### Scenario: Other platform jobs
- **WHEN** CI runs on Linux and Windows as well as macOS
- **THEN** each platform's full applicable build, tests, and clippy checks pass without disabling a failing test solely to green the matrix

### Requirement: Verified minimum supported Rust version
The minimum Rust version advertised in Cargo metadata and project documentation SHALL be the exact version exercised by a dedicated CI job. The job SHALL check all relevant workspace crates and reasonably checkable targets with an intended, reproducible dependency set while normal CI continues to test the separately pinned current toolchain. If the current minimum cannot support the intended secure and functional dependencies, the declared version and documentation SHALL be updated to the lowest supportable version before claiming support.

#### Scenario: Future use of a newer compiler feature
- **WHEN** source or dependency resolution requires a Rust version above the declared minimum
- **THEN** the MSRV CI job fails rather than passing due to the normal toolchain's newer compiler

#### Scenario: Minimum version remains viable
- **WHEN** the currently declared Rust 1.85 builds the workspace with the intended dependency set
- **THEN** CI exercises 1.85 and release documentation continues to advertise 1.85

#### Scenario: Minimum version is not viable
- **WHEN** the intended dependency set or needed functionality cannot build with Rust 1.85
- **THEN** the actual minimum is verified in CI and Cargo metadata, README, and changelog agree on the updated version
