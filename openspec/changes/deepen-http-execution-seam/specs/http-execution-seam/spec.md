# Spec Delta

## Purpose

Define a transport-independent HTTP execution boundary that gives download orchestration consistent semantic outcomes while preserving bounded streaming and protocol-fidelity testing.

## ADDED Requirements

### Requirement: Substitutable HTTP execution
The engine SHALL execute probe, sequential-transfer, and segmented-transfer HTTP work through a substitutable HTTP behavior boundary. Job orchestration MUST NOT require a concrete HTTP client, connection, response-body, or framing type, and both transfer modes SHALL use the same semantic transfer operation.

#### Scenario: Sequential transfer uses the seam
- **WHEN** a job selects sequential transfer
- **THEN** its probe and body transfer cross the HTTP behavior boundary without exposing production-client response types to job orchestration

#### Scenario: Segmented transfer uses the same seam
- **WHEN** workers execute concurrent byte-range transfers
- **THEN** each worker uses the same HTTP behavior boundary and semantic transfer outcome used by sequential transfer

#### Scenario: Adapter substitution preserves orchestration
- **WHEN** the production HTTP adapter is replaced by a conforming scripted adapter
- **THEN** job state, retry, cancellation, authentication, and resource-generation decisions operate without adapter-specific branches

### Requirement: HTTP-owned probe sequencing
The HTTP boundary SHALL own probe response interpretation and any configured validating range request. It SHALL return semantic probe metadata, retry timing, and authentication challenge information without requiring job orchestration to inspect raw response status or headers.

#### Scenario: Range support is verified
- **WHEN** the resource size qualifies for segmentation, range support is advertised, and verification is enabled
- **THEN** the HTTP boundary performs the validating range request and returns metadata that records whether byte ranges were verified and the authoritative total size

#### Scenario: Broken advertised range support
- **WHEN** a resource advertises byte ranges but the validating range response is unusable
- **THEN** the returned probe metadata marks the resource ineligible for verified segmented transfer without exposing the raw validating response to job orchestration

#### Scenario: Probe challenge is surfaced semantically
- **WHEN** a probe receives an authentication challenge
- **THEN** the caller receives the classified authentication failure and challenge data needed for the bounded credential-provider flow

### Requirement: Single segmentation decision
Job orchestration SHALL select segmented mode from the eligibility decision owned by the returned probe metadata and MUST NOT independently reconstruct the size, status, advertised-range, and verified-range calculation.

#### Scenario: Verified eligibility is reused
- **WHEN** probe metadata reports a known qualifying size and verified range support under the active policy
- **THEN** the job uses that metadata's eligibility decision to select segmented transfer

#### Scenario: Ineligible metadata falls back
- **WHEN** the HTTP-owned eligibility decision is false
- **THEN** the job selects sequential transfer without applying a second eligibility formula

### Requirement: Unified response classification and range validation
The HTTP boundary SHALL apply one status-to-error mapping, retry-timing extraction, authentication-challenge extraction, range metadata validation, and resource-generation validation for both sequential and segmented transfers. For ranged transfers, metadata validation MUST complete before any body chunk is made available to job orchestration.

#### Scenario: Retryable status is classified consistently
- **WHEN** either transfer mode receives a retryable HTTP status with retry timing
- **THEN** it receives the same structured error category and retry timing regardless of transfer mode

#### Scenario: Invalid content range is rejected before data
- **WHEN** a ranged response has a missing or mismatched content range, overshoots the requested end, or conflicts with the established total
- **THEN** the transfer fails with the corresponding structured range error before any body chunk is delivered

#### Scenario: Full response to nonzero range is rejected
- **WHEN** a nonzero byte-range request receives a full response
- **THEN** the response body is not delivered and the transfer reports either an invalid range response or a resource-generation change according to the conditional-request context

#### Scenario: Resource generation changes
- **WHEN** response validators conflict with the established resource generation
- **THEN** the transfer reports a resource-change error before mixing bytes from different generations

### Requirement: Bounded transport-neutral body delivery
The HTTP boundary SHALL deliver payload chunks without exposing production-client body or frame types. Delivery MUST preserve zero-copy payload ownership where the adapter supports it, MUST keep buffering explicitly bounded by demand, and MUST not fetch an additional chunk until the consumer requests one.

#### Scenario: Consumer backpressure bounds reads
- **WHEN** the sink has not finished processing the current payload chunk
- **THEN** the HTTP boundary does not deliver another chunk to that consumer

#### Scenario: Cancellation interrupts a pending read
- **WHEN** cancellation occurs while body delivery is waiting for data
- **THEN** the pending delivery completes promptly with the structured cancellation outcome

#### Scenario: Read idle timeout is classified consistently
- **WHEN** no payload arrives within the configured read-idle interval
- **THEN** either transfer mode receives the same retry-classifiable connection error

#### Scenario: Body fault is classified consistently
- **WHEN** the adapter reports a truncated body, reset, timeout, or other body-read failure
- **THEN** either transfer mode receives the same structured body error without adapter-specific inspection in job orchestration

### Requirement: Deterministic scripted adapter
A scripted HTTP adapter SHALL support an ordered sequence of expected requests and semantic responses. It MUST be able to produce successful metadata and chunks, retryable failures with retry timing, authentication challenges, explicit read-idle timeouts, body faults after any delivered prefix, cancellation waits, and validator or total-size changes without opening a network socket or depending on wall-clock delays.

#### Scenario: Ordered requests and responses
- **WHEN** a test scripts probe, validation, and transfer responses in order
- **THEN** the adapter returns those responses in order and reports an actionable mismatch if the observed request kind, range, validators, or relevant headers differ from the expectation

#### Scenario: Fault after a body prefix
- **WHEN** a script contains payload chunks followed by a body fault
- **THEN** the consumer receives the chunks in order and then the scripted classified fault

#### Scenario: Deterministic timeout and challenge
- **WHEN** a script contains an idle-timeout outcome or authentication challenge
- **THEN** orchestration observes that outcome deterministically without sleeping for a real timeout or running a local server

#### Scenario: Scripted resource change
- **WHEN** a later scripted response changes validators or the established total
- **THEN** orchestration follows the same resource-change path used with the production adapter

### Requirement: Production protocol coverage remains real
The production HTTP adapter SHALL retain responsibility for TLS, redirects and credential forwarding, proxy behavior, connection pooling, HTTP/2 multiplexing, request framing, and wire parsing. Tests whose evidence depends on those behaviors or malformed wire input MUST continue to exercise a real network server and the production adapter.

#### Scenario: Orchestration-only test uses scripts
- **WHEN** a test verifies retries, cancellation, bounded authentication stages, response ordering, or resource-change orchestration without relying on wire behavior
- **THEN** it may use the scripted adapter for deterministic evidence

#### Scenario: Protocol integration test uses the production adapter
- **WHEN** a test verifies TLS validation, redirect safety, proxying, connection reuse or limits, HTTP/2 behavior, malformed framing, or actual client error classification
- **THEN** it exercises the production adapter against a real server

### Requirement: Existing production construction remains compatible
Existing callers that construct the production HTTP adapter and pass it to the download controller SHALL continue to compile and receive the same download-result and error contracts, except for bug fixes that make previously inconsistent sequential and segmented classification agree.

#### Scenario: Existing caller supplies the production adapter
- **WHEN** a caller constructs the controller with the production HTTP adapter and engine configuration using the existing construction pattern
- **THEN** no adapter-specific migration is required
