# Spec Delta

## Purpose

Defines protocol-aware connection and stream concurrency plus fair cross-job origin feedback while retaining strict HTTP response validation, redirects and retry safety.

## ADDED Requirements

### Requirement: Protocol-aware request capacity
The engine SHALL distinguish network worker count, concurrent requests/H2 streams and physical connections. H1 may use additional permitted connections only while they improve useful work and do not worsen origin/storage signals. Additional H2 range workers MUST NOT implicitly require additional physical TCP/TLS connections; the default SHALL favor one healthy multiplexed H2 connection. Explicit existing additional-H2-connection configuration MUST retain its meaning; any adaptive expansion beyond one connection requires measured limiting evidence and configured caps.

#### Scenario: H2 growth without socket growth
- **GIVEN** an H2 origin with one healthy connection and multiple range streams
- **WHEN** adaptive range concurrency grows
- **THEN** the observed stream count may grow while physical connections stay at one by default and every range is validated

#### Scenario: H1 marginal benefit and limits
- **GIVEN** an H1 origin whose additional connections stop improving unique-byte goodput
- **WHEN** concurrency is probed within configured connection limits
- **THEN** further connection growth stops or reverses, without exceeding engine or origin connection caps

### Requirement: Shared-origin fair throttling and recovery
Jobs sharing a normalized final origin within one engine SHALL coordinate request admission and 429/503/Retry-After feedback. A throttle or Retry-After seen by one job SHALL reduce aggressiveness of peers to that origin in accord with existing retry policy, but SHALL NOT delay unrelated origins. Active jobs SHALL receive fair eventual access; failed and cancelled jobs MUST release their admission capacity. Idle origin state MUST be evicted or otherwise bounded, and throttled origins SHALL be able to probe recovery after cooldown.

#### Scenario: Two jobs on one origin
- **GIVEN** two concurrent jobs on the same origin and one returns 503 with Retry-After
- **WHEN** either job attempts a new request
- **THEN** both respect shared backoff within policy bounds, neither job permanently starves and requests later recover after cooldown

#### Scenario: Independent origins and cancellation
- **GIVEN** a throttled origin A, an unrelated origin B and a waiting job on A
- **WHEN** the waiting job is cancelled
- **THEN** B remains unaffected, A's request capacity is released and origin-state cardinality does not grow unbounded with completed jobs

#### Scenario: Redirect or proxy isolation
- **GIVEN** a redirected request or proxied connection
- **WHEN** origin throttling is reported
- **THEN** congestion feedback follows the actual final normalized origin, while transport connection limits continue to distinguish proxy/TLS pool identity and no credentials cross origins against policy

### Requirement: HTTP safety under optimized admission
Request and stream admission SHALL NOT bypass validated `Content-Range`, exact length, resource generation, identity encoding, retry classification or redirect/credential rules. Range-ignoring or inconsistent responses SHALL still trigger safe fallback or structured failure before bytes are credited.

#### Scenario: Malformed range and validator mismatch
- **GIVEN** a server sends `200` to a nonzero range, malformed `Content-Range`, or changed ETag during concurrent H1 or H2 requests
- **WHEN** optimized policy dispatches a request
- **THEN** the response is not accepted as that range, generations are not mixed and existing fallback/failure semantics apply

#### Scenario: Backoff exhaustion
- **GIVEN** repeated retryable 429/503 responses and an exhausted retry policy
- **WHEN** the job reaches its configured limit
- **THEN** it fails with the appropriate structured error rather than retrying indefinitely or retaining origin capacity
