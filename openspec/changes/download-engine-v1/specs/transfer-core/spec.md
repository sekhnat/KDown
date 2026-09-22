# Spec Delta

## Purpose

Defines the transfer execution behavior of the engine: probing remote resources, planning and scheduling byte-range work, executing transfers with bounded workers, retrying failures safely, and handling unknown lengths and resource changes.

## ADDED Requirements

### Requirement: Probe and capability detection
The engine SHALL determine, before segmented transfer, the resource's final URL, status, content length, media type, filename hints, validators (ETag, Last-Modified), range support, content encoding, and authentication requirements. Because HEAD is not authoritative, the engine SHALL validate range capability with a ranged GET when metadata is incomplete or suspicious, and MUST treat advertised-but-broken range support as a capability failure for the job.

#### Scenario: HEAD metadata incomplete
- **WHEN** a HEAD probe returns incomplete or suspicious metadata
- **THEN** the engine issues a ranged GET (e.g., bytes=0-0) and validates the 206 response, Content-Range, and total size before choosing a transfer mode

#### Scenario: Server lies about range support
- **WHEN** a server advertises Accept-Ranges but a range request returns a non-conforming response (e.g., 200 for a nonzero range)
- **THEN** the engine disables segmentation for that job and downgrades safely (single stream or restart) rather than writing misaligned data

### Requirement: Segmentation eligibility
The engine SHALL enable segmented parallel transfer only when all of the following hold: total size is known, size meets the configured threshold, range behavior is verified or explicitly trusted, the representation is stable across range requests, content/transfer encoding does not make byte offsets ambiguous, and resume validators satisfy the configured safety policy. Otherwise it MUST fall back to single-stream transfer.

#### Scenario: Small resource uses single stream
- **WHEN** a resource's size is below the segmentation threshold
- **THEN** the engine transfers it with a single stream and does not issue parallel range requests

#### Scenario: Unknown length forces sequential mode
- **WHEN** the total content length is unknown
- **THEN** the engine uses sequential transfer, reports progress without percent/ETA, honors an optional caller-configured maximum size, and treats chunked transfer encoding as normal

### Requirement: Interval-set scheduling correctness
The engine SHALL represent remaining work as a normalized, non-overlapping interval set and SHALL guarantee: no byte belongs to two active leases; completed ranges never overlap pending ranges; the union of completed, active, and pending intervals equals the target byte domain; and logical completion covers every byte exactly once regardless of retries, splits, or worker failures.

#### Scenario: Coverage after randomized failures
- **WHEN** workers fail, retry, split, and resume across a randomized sequence of operations
- **THEN** every byte of the target range is covered exactly once logically and the final file contains no gaps or duplicated regions

#### Scenario: Dynamic split excludes consumed bytes
- **WHEN** an idle worker takes over the unconsumed tail of another worker's segment
- **THEN** the split excludes bytes already read or queued for write by the original worker, and both workers' writes remain non-overlapping

### Requirement: Worker execution and backpressure
Workers SHALL transfer data in bounded reusable buffers from network to explicit offsets, SHALL apply backpressure when disk writes fall behind network reads, and SHALL never buffer an entire segment in memory. Memory usage SHALL be bounded by active workers times per-worker buffers plus scheduler metadata, independent of file size.

#### Scenario: Slow disk applies backpressure
- **WHEN** sink writes fall behind network reads during a transfer
- **THEN** workers pause reading rather than accumulating unbounded in-flight bytes, and memory usage stays within the configured buffer budget

#### Scenario: Retry does not inflate progress
- **WHEN** a worker fails partway through a segment and retries
- **THEN** unique completed bytes do not increase from re-transferred data, and network byte counters reflect the retransmission separately

### Requirement: Retry classification and backoff
The engine SHALL classify failures into retryable and non-retryable categories per policy (connection resets, temporary DNS failure, timeouts, 408/429, selected 5xx, truncated bodies, protocol resets retryable; 400/401/403/404/410, invalid URL, TLS validation failure, validator mismatch, local permission and disk-full errors non-retryable without external change), SHALL retry with exponential backoff and full jitter capped by policy, SHALL honor valid Retry-After headers within policy limits, and SHALL retry only the unfinished portion of failed work.

#### Scenario: Transient failure requeues the tail
- **WHEN** a connection drops mid-segment
- **THEN** only the unfinished tail is requeued and retried with capped exponential backoff, and previously completed ranges are not re-downloaded

#### Scenario: Non-retryable error fails the job
- **WHEN** the origin returns 404
- **THEN** the job fails with a structured not-found error without retry attempts

#### Scenario: Coordinated origin backoff
- **WHEN** many workers receive 503 or 429 responses from the same origin simultaneously
- **THEN** retries are coordinated into a shared origin backoff state rather than each worker retrying at full concurrency

### Requirement: Resource generation consistency
The engine SHALL capture the strongest available resource validators and SHALL treat any detected generation change (ETag change, Last-Modified change, size change, mismatched Content-Range total, full response to an If-Range request) as invalidating current partial data: stop new segment assignment, settle active requests, emit a resource-changed signal, and apply the configured policy (fail or restart from zero) without ever mixing generations into one file.

#### Scenario: ETag changes mid-download
- **WHEN** the remote resource's ETag changes while segments are being transferred
- **THEN** the engine stops assignment, invalidates current partial data, reports the generation change, and either fails or restarts per policy — never appending new-generation bytes to old-generation data

#### Scenario: Resume with changed validator
- **WHEN** a resume attempt finds the remote validator differs from the checkpoint
- **THEN** the engine refuses to continue from old ranges and applies the configured generation-change policy

### Requirement: Range response validation
The engine SHALL validate every accepted range response against the request: the returned start offset, end bound, total size, generation validators, and body length MUST be consistent with the request, and a 200 response to a nonzero range request MUST NOT be written as the requested range.

#### Scenario: Mismatched Content-Range rejected
- **WHEN** a segment request for [S, E] receives a 206 response whose Content-Range start differs from S or whose total conflicts with the established size
- **THEN** the engine rejects the response body, does not write the bytes, and retries or downgrades safely

### Requirement: Bandwidth limiting
The engine SHALL support hierarchical token-bucket limiting (global → per-job → worker consumption) applied to payload bytes read from the network, with runtime-changeable job limits taking effect without restart, non-busy-wait enforcement, fairness across workers, and bounded bursts.

#### Scenario: Global limit shared across jobs
- **WHEN** multiple jobs run concurrently under a global rate limit
- **THEN** combined network consumption converges to the global limit while each job also respects its own limit

### Requirement: Integrity verification timing
The engine SHALL verify known-size downloads for exact final byte length and SHALL compute caller-required hashes (SHA-256 and SHA-512 at minimum, with legacy MD5/SHA-1 not presented as collision-resistant) via a verification pass completed before commit; a hash mismatch MUST prevent commit and surface a structured integrity error.

#### Scenario: Hash mismatch prevents commit
- **WHEN** the computed hash of the completed file differs from the caller-provided expected hash
- **THEN** the job fails with a structured integrity error and the destination is not created or overwritten

#### Scenario: Sequential verification for segmented downloads
- **WHEN** a segmented download completes and an expected whole-file hash is configured
- **THEN** the engine verifies it with a sequential read of the completed file before entering Committing

#### Scenario: Exact size verification
- **WHEN** a known-size download completes with a byte count different from the established total size
- **THEN** the job fails with a structured error instead of committing