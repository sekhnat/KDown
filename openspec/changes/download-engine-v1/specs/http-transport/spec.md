# Spec Delta

## Purpose

Defines the HTTP protocol behavior of the engine — redirects, ranges, conditional requests, content encoding, connection management, proxying, authentication hooks — and the transport abstraction boundary that keeps protocol specifics out of scheduling and storage layers.

## ADDED Requirements

### Requirement: HTTP and HTTPS support with range semantics
The engine SHALL support HTTP and HTTPS URLs using HTTP/1.1 and HTTP/2, issuing byte-range requests as `Range: bytes=S-E` for segments and requiring `206 Partial Content` with a consistent `Content-Range` response; a `200 OK` response to a nonzero range request SHALL NOT be treated as the requested range and SHALL trigger safe downgrade or failure.

#### Scenario: Segmented transfer over HTTP/2
- **WHEN** an eligible resource is transferred in segmented mode from an HTTP/2 origin
- **THEN** range streams are multiplexed over pooled connections without violating per-origin connection limits, and the file completes byte-correct

#### Scenario: Range ignored by server
- **WHEN** a segment request returns 200 with the full body instead of a 206 range response
- **THEN** the engine does not write the body at the requested offset, aborts segmented mode for the job, and restarts or downgrades safely per policy

### Requirement: Redirect handling with safety policy
The engine SHALL follow redirects with a configurable maximum (default 10), loop detection, final-URL recording, downgrade protection denying HTTPS→HTTP redirects by default, and cross-origin credential forwarding disabled unless explicitly allowed by caller policy.

#### Scenario: Redirect chain resolves and is recorded
- **WHEN** an origin responds with a bounded chain of same-or-upgraded-scheme redirects
- **THEN** the transfer proceeds from the final URL, which is recorded in the result metadata

#### Scenario: HTTPS to HTTP downgrade denied
- **WHEN** a redirect moves from HTTPS to HTTP and no override is configured
- **THEN** the job fails with a structured redirect error instead of silently downgrading

#### Scenario: Authorization header does not cross origins
- **WHEN** a redirect crosses to a different origin while carrying caller credentials
- **THEN** authorization headers and cookies are stripped unless policy explicitly permits forwarding

### Requirement: Conditional requests and generation validators
The engine SHALL use the strongest available validator on resume — strong ETag via `If-Range`, falling back to Last-Modified when eligible — and when the origin indicates the resource changed, SHALL NOT append new bytes to old-generation data, applying the configured change policy (restart from zero, structured failure, or caller decision callback where supported). Segmented range requests SHALL normally send `Accept-Encoding: identity` unless verified otherwise, keeping byte offsets unambiguous.

#### Scenario: If-Range protects resume
- **WHEN** a resume request carries If-Range and the resource has changed since the checkpoint
- **THEN** the server's response is handled as a generation change and old checkpoint ranges are never mixed with new-generation bytes

#### Scenario: Encoded responses keep offsets unambiguous
- **WHEN** a server would return gzip-encoded content for range requests
- **THEN** segmented requests request identity encoding so byte offsets refer to the stored representation the engine writes

### Requirement: Connection management and limits
The engine SHALL pool connections across compatible jobs keyed by scheme, origin, proxy, and TLS configuration; SHALL enforce both an engine-global connection limit and a per-origin limit; SHALL expire idle connections after a configurable timeout; and SHALL transparently retry on broken pooled connections when safe.

#### Scenario: Per-origin limit enforced
- **WHEN** multiple segmented jobs target the same origin
- **THEN** concurrent connections to that origin never exceed the configured per-origin limit while other origins are unaffected

#### Scenario: Stale pooled connection retried safely
- **WHEN** a pooled keep-alive connection is closed by the server before a request
- **THEN** the request is retried transparently without failing the job when no bytes of the response were consumed

### Requirement: Proxy and credential hooks
The engine SHALL support no-proxy, HTTP proxy, and HTTPS-over-CONNECT configurations, with optional SOCKS as an extension and caller-defined proxy selection; the engine SHALL accept credentials or a credential provider callback without owning long-term secret storage, SHALL avoid automatic repeated authentication retries that can loop or lock accounts, and SHALL redact proxy credentials from logs.

#### Scenario: HTTPS through CONNECT proxy
- **WHEN** a transfer is configured with an HTTPS CONNECT proxy
- **THEN** the tunnel is established with the proxy credentials and TLS validation still applies end-to-end to the origin

#### Scenario: Credential provider callback
- **WHEN** a challenge requires credentials and a provider is configured
- **THEN** the engine consults the provider for the scope/challenge without storing secrets itself, and does not retry authentication in an unbounded loop

### Requirement: Transport abstraction boundary
Protocol-specific logic (header parsing, URL handling, status interpretation, validator headers) SHALL live behind the transport interface providing probe, open-range, and open-full operations returning metadata validated before body bytes are accepted; scheduling and storage layers SHALL NOT contain HTTP-specific parsing, and additional transports (e.g., HTTP/3) SHALL be addable without changing scheduler or sink semantics.

#### Scenario: Scheduler remains protocol-agnostic
- **WHEN** a new transport is implemented for another protocol
- **THEN** the scheduler and sink operate unchanged on that transport's validated metadata and byte streams

#### Scenario: Response metadata validated before body
- **WHEN** a range response arrives whose metadata (status, Content-Range, validators) fails job-level validation
- **THEN** the transport consumer rejects the response before accepting any body bytes