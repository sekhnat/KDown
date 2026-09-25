# Spec Delta

## Purpose

Gives callers an accurately named public controller for both sequential and segmented jobs while preserving a documented migration path for existing library users.

## ADDED Requirements

### Requirement: Canonical download controller
`DownloadController` SHALL be the documented public controller for starting both sequential and segmented downloads and SHALL expose the existing construction and control behavior without a second implementation. Public imports, examples, and rustdoc SHALL use the canonical name.

#### Scenario: New public import
- **WHEN** a caller imports `DownloadController` from the crate root and starts a sequential or segmented job
- **THEN** the code compiles and the controller starts the appropriate existing transfer path

### Requirement: Compatibility for former controller name
The former public `SingleStreamController` name SHALL remain available as a deprecated alias of `DownloadController` when Rust compatibility permits, with a deprecation message identifying the replacement. If retaining an alias proves materially incompatible, the release notes SHALL instead explicitly label the rename as breaking and supply a migration example.

#### Scenario: Compatibility alias retained
- **WHEN** existing caller code constructs `SingleStreamController` from its previously public path
- **THEN** it still resolves to the same implementation, with a deprecation diagnostic directing callers to `DownloadController`

#### Scenario: Breaking alternative required
- **WHEN** the compatibility alias cannot reasonably preserve existing construction or methods
- **THEN** release notes and public documentation clearly identify the breaking change and the new import/construction path before release
