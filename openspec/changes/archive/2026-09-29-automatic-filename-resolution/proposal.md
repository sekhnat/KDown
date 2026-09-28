# Proposal

## Why

KDown currently requires callers to choose an output filename before probing the server. Callers who want the final redirected URL or `Content-Disposition` name must implement their own unsafe or inconsistent filename handling, collision policy and resume coordination. Provide an opt-in, repository-aligned directory target without changing the behavior or source compatibility of existing explicit-file requests.

## What Changes

- Add `DirectoryDownloadRequest` and symmetric controller entry points to resolve an immutable, safe filename from the final HEAD metadata, redirected/original URL, or a validated fallback. Preserve existing `DownloadRequest` fields and explicit-file entry points.
- Extend the existing `OverwritePolicy` with opt-in `Rename` for file and directory targets: checkpoint-first resumable sibling discovery, then bounded collision-free name selection under destination leases and atomic no-replace publication.
- Add resolved-destination observation through `DownloadHandle::resolved_destination()` and a single `Event::DestinationResolved` for directory and Rename jobs, without changing the event sequence for existing file policies.
- Preserve existing transfer, authentication, retry, cancellation, checkpoint and commit machinery; adapt ordering so directory targets probe before lease/admission, while all explicit-file targets select/admit before networking.
- Extend public API documentation, compatibility inventory, fixture, fuzz/property and integration coverage for naming, security, storage and outcome behavior.

## Capabilities

### New Capabilities

- `download-destination-resolution`: Opt-in directory downloads, safe metadata-based filename selection, Rename collisions/resume, ordering, observability and outcomes.

### Modified Capabilities

None: `openspec list --specs` currently reports no main capabilities. Existing explicit-file behavior is a compatibility constraint of the new capability, not a main-spec delta.

## Impact

- Rust library: `crates/engine/src/job/controller.rs`, `config.rs`, `http/probe.rs`, `http/transport.rs`, `io/sanitize.rs`, destination lease/publication integration, `resume/flow.rs`, `metrics/events.rs`, `lib.rs` and fuzz targets.
- External callers gain additive root re-exports/methods/variant/event; old request struct literals and method signatures remain unchanged. No new naming error category or parallel transfer implementation.
- Test coverage in engine internal/integration/external-consumer suites; `docs/api-surface.md`, `docs/api-compatibility.md`, `CHANGELOG.md`, `scripts/api_surface_check.sh` and platform CI.
- Assumes the caller's selected directory and its entries are trusted against non-cooperating filesystem mutation; lexical containment and atomic publication protect against hostile metadata and destination races, not directory replacement by another actor.
