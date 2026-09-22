# Spec Delta

## Purpose

Defines how the engine reports progress, exposes structured failures, emits events and metrics, and logs safely — giving host applications and operators actionable, honest information about transfers.

## ADDED Requirements

### Requirement: Progress snapshot semantics
The engine SHALL expose a thread-safe progress snapshot reporting state, total size when known, unique completed bytes, network bytes received, reused bytes, active workers, instantaneous and smoothed rates, ETA when meaningful, retry count, and elapsed time. `completed_bytes` SHALL mean unique file bytes completed — retries and re-reads MUST NOT inflate it — and SHALL never exceed the known total size.

#### Scenario: Unique bytes vs network bytes
- **WHEN** a worker fails after transferring 10 MiB and the tail is re-downloaded
- **THEN** completed_bytes reflects only uniquely completed data while network bytes include the retransmission, and completed_bytes never exceeds total size

#### Scenario: ETA only when meaningful
- **WHEN** total size is unknown or the smoothed rate is below the meaningful threshold during startup
- **THEN** the snapshot omits ETA and percentage instead of showing unstable values

### Requirement: Speed estimation and events
The engine SHALL estimate user-visible speed with a smoothed estimator over recent payload throughput (excluding probe/pause periods from distorting the average) and SHALL expose a documented event stream covering state changes, probe completion, segment lifecycle, progress, rate-limit changes, resource changes, integrity checks, commit, warnings, and failures — batched at a documented cadence without high-frequency chunk events by default.

#### Scenario: Event stream covers job lifecycle
- **WHEN** a job runs from start to completion with a pause and a retry
- **THEN** observers receive state-change, segment, progress, and warning events in order, with no per-chunk events by default

#### Scenario: Callbacks never run under internal locks
- **WHEN** an event or callback is delivered to the host
- **THEN** it is not executed while internal scheduler locks are held, and the delivery concurrency guarantees are documented

### Requirement: Structured error taxonomy
The engine SHALL surface every terminal failure as a structured error carrying a stable category, human-readable message, retryability hint, source cause where supported, origin context, HTTP status when applicable, and segment/range context when applicable — covering configuration, URL/scheme, DNS, connect/timeout, connection, TLS, proxy, authentication/authorization, not-found, server errors, rate limiting, redirects, protocol violations, range support/invalid responses, resource-changed, unknown-length mode conflicts, sink open/write errors, disk-full, permission, checkpoint, integrity mismatch, commit, cancellation, deadline, and retry exhaustion.

#### Scenario: Terminal errors carry stable categories
- **WHEN** jobs fail due to DNS failure, disk-full, validator mismatch, and a 404 respectively
- **THEN** each result exposes a distinct stable error category with actionable context (origin, HTTP status, or filesystem path as applicable), not a string-only error

### Requirement: Sensitive data redaction
Default logging and error output SHALL redact authorization headers, cookie values, proxy authentication, URL userinfo, and caller-marked sensitive query parameters or signed URLs; structured log events SHALL support correlation fields (engine instance, job id, worker id, lease id, origin, attempt, error category) and leveled logging from ERROR (terminal failures) through TRACE (redacted request lifecycle).

#### Scenario: Credentials never appear in logs
- **WHEN** a job with bearer credentials and cookies logs at default levels
- **THEN** no authorization, cookie, proxy-auth, or userinfo secret values appear in any log record or error message

#### Scenario: Correlation across log events
- **WHEN** an operator filters logs by job id during a multi-job transfer
- **THEN** all events for that job — including worker retries and segment failures — are attributable via correlation fields