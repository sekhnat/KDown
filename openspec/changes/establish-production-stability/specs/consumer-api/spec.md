# Spec Delta

## Purpose

Defines the intentionally supported external Rust library surface and its documented compatibility and migration commitments for embedding applications.

## ADDED Requirements

### Requirement: Deliberate consumer surface
The crate SHALL expose only documented consumer-facing requests, configuration, transport construction, controller/handle operations, terminal results/errors, and observation APIs. Scheduler, job orchestration, I/O, resume, and fuzzing implementation modules MUST NOT be public unless an individual interface is explicitly designated and documented as supported; necessary public type signatures SHALL remain nameable through supported paths. Test injection interfaces, if retained, MUST be individually declared stable or kept internal.

#### Scenario: External application compiles against supported API
- **WHEN** an external crate starts, observes, controls, and awaits a download using documented public imports
- **THEN** it compiles without referring to implementation modules and distinguishes successful completion from all failure outcomes

#### Scenario: Implementation internals are not accessible
- **WHEN** an external crate tries to import a non-designated scheduler, resume, I/O, job-internal, or fuzzing module
- **THEN** the import is not part of the supported public API

### Requirement: Versioned compatibility and migration
The release SHALL document which public types, methods, enum variants, fields, and trait/injection seams are supported, and SHALL state the compatibility policy for changes to those items (including exhaustiveness and minimum Rust version). Removal or signature changes from the current 0.1 API MUST be identified as breaking with a migration guide covering outcomes, changed import paths, and removed injection seams; consumer-facing examples MUST use the new API.

#### Scenario: Existing 0.1 consumer migrates
- **WHEN** a consumer previously checked `DownloadResult.status/error` or used public implementation modules
- **THEN** published migration guidance shows how to handle typed errors and which supported replacements exist or states that a seam was deliberately retired

#### Scenario: Future compatibility check
- **WHEN** a proposed release changes a supported type or signature
- **THEN** an external-consumer compile check and the documented compatibility policy identify whether the change is compatible or needs an explicitly breaking release
