# Download Engine — Technical Specification

**Document:** `SPEC.md`  
**Component:** High-performance download engine  
**Status:** Design specification  
**Intended use:** Core transfer component for a larger download manager  
**Primary scope:** Efficient, reliable retrieval of remote byte streams to local storage  
**Initial protocol scope:** HTTP/1.1 and HTTP/2 over HTTPS, with architecture prepared for HTTP/3 and additional transports

---

## 1. Purpose

This document specifies a standalone **download engine** responsible for transferring one remote object to a local destination as efficiently and reliably as practical while preserving correctness under cancellation, process interruption, network failure, server inconsistency, and partial completion.

The engine is intentionally narrower than a full download manager. It must not own user-interface concerns, download queues spanning unrelated jobs, account management, browser integration, media extraction, torrent logic, scheduling by wall-clock time, library/catalog management, or long-term user preferences. Those belong to higher layers.

The engine must expose a stable programmatic API that a future download manager can use to:

- inspect a remote resource;
- start a new download;
- resume an interrupted download;
- pause or cancel an active transfer;
- observe progress and transfer metrics;
- set bandwidth and concurrency limits;
- verify integrity;
- commit a completed file atomically;
- obtain structured failure information.

The design prioritizes:

1. **Correctness** — never silently produce a corrupt or misassembled file.
2. **Throughput** — efficiently saturate available network bandwidth when the origin and storage permit it.
3. **Low overhead** — bounded memory use, low lock contention, minimal copying, limited syscall pressure.
4. **Resilience** — recover from transient failures without restarting completed work.
5. **Deterministic behavior** — explicit state transitions and retry semantics.
6. **Composability** — no dependency on a specific UI, database, or application framework.
7. **Observability** — enough structured telemetry to diagnose poor performance and failures.

---

## 2. Non-goals

The first version of the engine does **not** need to implement:

- BitTorrent, magnet links, ed2k, IPFS, or peer-to-peer protocols;
- FTP/SFTP unless added as a later transport plugin;
- website crawling;
- HLS/DASH media playlist resolution;
- DRM circumvention;
- CAPTCHA solving;
- browser cookie extraction;
- credential vaults;
- archive extraction;
- post-download media processing;
- global multi-download scheduling policy;
- cross-device synchronization;
- distributed downloads across multiple machines;
- upload support.

The architecture should avoid preventing these future additions, but they must not complicate the core implementation.

---

## 3. Terminology

| Term | Meaning |
|---|---|
| **Job** | One logical download operation for one remote object. |
| **Resource** | The remote object being downloaded. |
| **Probe** | Initial request(s) used to determine metadata and server capabilities. |
| **Range** | Inclusive byte interval `[start, end]` requested via HTTP Range semantics. |
| **Segment** | A planned range assigned to a worker. |
| **Chunk** | A smaller in-memory unit read from the network and written to disk. |
| **Worker** | Logical execution unit transferring one segment at a time. |
| **Checkpoint** | Persisted resumable state describing verified/completed ranges. |
| **Generation** | Identity version of the remote resource, typically constrained by ETag and/or Last-Modified. |
| **Commit** | Final transition from temporary output to the requested destination. |
| **Sink** | Abstraction that accepts downloaded bytes at explicit offsets. |
| **Transport** | Protocol-specific mechanism that retrieves metadata and byte ranges. |

---

## 4. Functional requirements

### 4.1 Required capabilities

The engine MUST support:

- HTTP and HTTPS URLs;
- redirects subject to policy;
- HTTP Range requests when supported;
- single-stream fallback when ranges are unsupported or unsafe;
- segmented parallel downloading for suitable resources;
- dynamic segment scheduling;
- configurable connection and worker concurrency;
- pause with resumable state preservation;
- cancellation with deterministic cleanup policy;
- restart after process termination;
- retry with exponential backoff and jitter;
- per-request timeouts;
- total job deadline as an optional policy;
- integrity verification using expected hashes when supplied;
- validator-based resume using ETag and/or Last-Modified;
- bounded in-memory buffering;
- direct positional writes to the destination temp file;
- atomic finalization whenever supported by the local filesystem;
- progress and metrics callbacks/events;
- global-to-job bandwidth limiting hooks;
- proxy configuration hooks;
- custom headers;
- cookies or bearer credentials supplied by the caller without owning credential storage;
- TLS certificate validation by default;
- configurable user-agent;
- IPv4/IPv6 support delegated to the networking stack;
- graceful handling of unknown content length.

### 4.2 Optional capabilities for v1.x

The architecture SHOULD support later addition of:

- HTTP/3 / QUIC;
- alternate mirrors for the same object;
- multi-source segment fetching;
- adaptive concurrency using bandwidth-delay measurements;
- checksum trees / per-segment hashes;
- content decoding plugins;
- memory-mapped file sinks;
- network interface binding;
- DNS resolver customization;
- transport plugins beyond HTTP.

---

## 5. Core design principles

### 5.1 Separate policy from mechanism

The engine should separate:

- **mechanism:** HTTP requests, range reads, writes, retries, checkpoint persistence;
- **policy:** how many workers to use, segment sizing, retry limits, bandwidth allocation.

Policy must be configurable without modifying transport code.

### 5.2 Explicit resource identity

A resumed or segmented download must never assume the remote object is unchanged merely because the URL is identical.

The engine must capture the strongest available resource validators:

1. strong ETag;
2. weak ETag, treated conservatively;
3. Last-Modified;
4. content length;
5. caller-provided expected checksum.

Resume requests should use conditional range semantics such as `If-Range` where applicable.

If identity cannot be established safely, the engine must either restart or require caller policy to permit unsafe resume.

### 5.3 Bounded resource usage

Memory, open files, active sockets, queued chunks, and pending write operations must all be bounded.

No code path may allow unbounded accumulation of downloaded-but-unwritten bytes.

### 5.4 Offset-based correctness

Every byte written for a segmented download must be associated with an explicit absolute offset. Completion is represented by a normalized interval set, not merely by counting bytes received.

---

## 6. High-level architecture

```text
+-------------------------------------------------------------+
|                        Engine API                           |
+----------------------+--------------------------------------+
                       |
                       v
+-------------------------------------------------------------+
| Job Controller                                              |
| - lifecycle/state machine                                   |
| - probe                                                     |
| - resource identity                                         |
| - policy selection                                          |
| - checkpoint coordination                                   |
+----------+-------------------+------------------------------+
           |                   |
           v                   v
+-------------------+   +-----------------------+
| Segment Scheduler |   | Progress / Metrics    |
| - pending ranges  |   | - counters            |
| - dynamic splits  |   | - speed estimation    |
| - worker leases   |   | - events              |
+---------+---------+   +-----------------------+
          |
          v
+-------------------------------------------------------------+
| Transfer Workers                                            |
| - acquire segment                                           |
| - issue range request                                       |
| - stream chunks                                             |
| - retry/requeue                                             |
+----------+--------------------------------------------------+
           |
     +-----+------+
     |            |
     v            v
+----------+  +-----------------------------------------------+
|Transport |  | Sink / File Writer                            |
|HTTP 1/2  |  | - positional writes                           |
|future H3 |  | - preallocation                               |
+----------+  | - flush/checkpoint coordination               |
              +-----------------------------------------------+
```

### 6.1 Main modules

The implementation should contain the following logical modules:

- `engine` — public API and engine-wide shared resources;
- `job` — one transfer lifecycle;
- `probe` — metadata discovery and range capability validation;
- `transport` — HTTP request execution and response streaming;
- `scheduler` — interval planning and segment assignment;
- `worker` — network-to-sink transfer loop;
- `sink` — random-access output storage;
- `checkpoint` — resumable state serialization;
- `rate_limit` — token-bucket or equivalent bandwidth control;
- `retry` — retry classification and backoff;
- `integrity` — hashes and final verification;
- `metrics` — counters, latency, rates, and events;
- `error` — structured error taxonomy;
- `config` — validated engine and job configuration.

No module should rely on a GUI or application-global singleton.

---

## 7. Public API

The exact syntax is language-dependent. Semantically, the engine should expose equivalents of the following.

### 7.1 Engine

```text
DownloadEngine
  new(EngineConfig) -> Engine
  probe(DownloadRequest) -> ResourceMetadata
  start(DownloadRequest) -> DownloadHandle
  resume(ResumeRequest) -> DownloadHandle
  shutdown(mode) -> Result
```

The engine owns shared infrastructure such as:

- connection pools;
- DNS/TLS client configuration;
- optional global rate limiter;
- worker executor/runtime integration;
- metrics sink;
- buffer pool.

### 7.2 Download request

```text
DownloadRequest {
    url: URL,
    destination: Path,
    headers: HeaderMap,
    credentials: optional CredentialProvider,
    expected_size: optional u64,
    expected_hashes: list<HashExpectation>,
    overwrite_policy: OverwritePolicy,
    resume_policy: ResumePolicy,
    transfer_policy: TransferPolicy,
    network_policy: NetworkPolicy,
    integrity_policy: IntegrityPolicy,
    metadata: opaque caller metadata
}
```

### 7.3 Download handle

```text
DownloadHandle
  id() -> JobId
  snapshot() -> ProgressSnapshot
  pause() -> Result
  resume() -> Result
  cancel(CancelMode) -> Result
  set_rate_limit(optional BytesPerSecond) -> Result
  set_concurrency(u32) -> Result
  events() -> EventStream
  wait() -> DownloadResult
```

Calls must be thread-safe or task-safe according to the host language.

### 7.4 Result

```text
DownloadResult {
    status: Completed | Cancelled | Failed,
    final_path: optional Path,
    bytes_downloaded_from_network: u64,
    bytes_reused_from_checkpoint: u64,
    total_size: optional u64,
    elapsed: Duration,
    effective_average_rate: f64,
    validators: ResourceValidators,
    verified_hashes: map<HashAlgorithm, Digest>,
    warnings: list<Warning>,
    error: optional DownloadError
}
```

---

## 8. Configuration

### 8.1 EngineConfig

Recommended defaults:

```text
EngineConfig {
    max_active_jobs: 64,
    max_connections_total: 256,
    max_connections_per_origin: 16,
    max_idle_connections_per_origin: 8,
    idle_connection_timeout: 90s,

    read_buffer_size: 128 KiB,
    buffer_pool_max_bytes: 128 MiB,

    connect_timeout: 10s,
    tls_handshake_timeout: 10s,
    response_header_timeout: 20s,
    read_idle_timeout: 30s,

    global_rate_limit: unlimited,
    checkpoint_flush_interval: 2s,
    metrics_interval: 500ms,

    prefer_http2: true,
    enable_http3_when_available: false
}
```

Defaults are starting points, not normative performance constants. Implementations should make them configurable and benchmark platform-specific values.

### 8.2 TransferPolicy

```text
TransferPolicy {
    mode: Auto | SingleStream | Segmented,
    max_workers: 8,
    min_workers: 1,
    initial_segment_size: 8 MiB,
    min_segment_size: 1 MiB,
    max_segment_size: 64 MiB,
    dynamic_segment_splitting: true,
    segmentation_threshold: 16 MiB,
    preallocate_output: true,
    fsync_policy: OnCheckpoint | OnCompletion | Never,
    verify_range_support: true
}
```

### 8.3 RetryPolicy

```text
RetryPolicy {
    max_attempts_per_segment: 8,
    base_delay: 250ms,
    max_delay: 30s,
    multiplier: 2.0,
    jitter: Full,
    retry_408: true,
    retry_429: true,
    retry_5xx: selected,
    honor_retry_after: true,
    retry_connection_reset: true,
    retry_dns_temporary_failure: true
}
```

Retries must be classified by error type, not by a generic catch-all loop.

---

## 9. Job lifecycle and state machine

### 9.1 States

```text
Created
  -> Probing
  -> Preparing
  -> Running
  -> Pausing
  -> Paused
  -> Running
  -> Verifying
  -> Committing
  -> Completed

Any non-terminal operational state may transition to:
  -> Cancelling -> Cancelled
  -> Failing -> Failed
```

### 9.2 State invariants

- `Completed`, `Cancelled`, and `Failed` are terminal.
- No worker may write after transition to terminal state.
- `Paused` means all network workers have stopped issuing new reads, in-flight writes are settled according to policy, and a valid checkpoint exists if resume is enabled.
- `Completed` means final integrity requirements passed and the final destination path is committed.
- Failure to commit must not be reported as completed even if all bytes were fetched.
- The public state must be monotonic except `Paused -> Running`.

### 9.3 Pause semantics

Pause is cooperative but must converge quickly:

1. stop segment assignment;
2. signal workers to stop at the next safe chunk boundary;
3. settle pending writes;
4. update completed interval set;
5. persist checkpoint;
6. optionally flush file metadata/data according to durability policy;
7. enter `Paused`.

Partially transferred segments may be retained at chunk granularity only if the checkpoint format can describe the exact durable interval. Otherwise the partial segment tail is discarded logically and redownloaded.

### 9.4 Cancellation modes

`CancelMode` should include:

- `KeepPartial` — preserve temp file and checkpoint;
- `DeletePartial` — delete temp output and checkpoint after workers stop;
- `KeepFileDiscardCheckpoint` — for advanced callers; preserved file is not guaranteed resumable.

---

## 10. Probe and capability detection

The engine must probe the resource before selecting segmented mode unless the caller provides trusted metadata that allows the probe to be skipped.

### 10.1 Metadata to discover

Probe should determine where possible:

- final URL after redirects;
- HTTP status;
- content length;
- media type;
- filename hints from Content-Disposition;
- ETag;
- Last-Modified;
- Accept-Ranges;
- content encoding;
- selected protocol version;
- server range correctness;
- whether authentication is required.

### 10.2 HEAD is not authoritative

Do not assume `HEAD` is implemented correctly by every origin. The probe strategy should be:

1. optionally issue `HEAD`;
2. if metadata is incomplete, suspicious, or range support needs validation, issue `GET` with `Range: bytes=0-0`;
3. validate `206 Partial Content`, `Content-Range`, and total size;
4. fall back to a normal GET when the origin does not support ranges.

### 10.3 Segmentation eligibility

Segmented mode is allowed only when all required conditions hold:

- total size is known;
- size exceeds the configured threshold;
- byte range behavior has been verified or explicitly trusted;
- content representation is stable across range requests;
- transfer encoding/content encoding does not make requested byte offsets ambiguous;
- resume validators are sufficient for configured safety policy.

If a server advertises ranges but returns invalid range responses, segmentation must be disabled for that job and, optionally, cached as an origin capability observation.

---

## 11. HTTP semantics

### 11.1 Redirects

Support standard redirects with:

- maximum redirect count, default 10;
- loop detection;
- configurable cross-origin credential forwarding policy;
- downgrade protection: HTTPS -> HTTP redirect denied by default;
- final URL recorded in metadata.

### 11.2 Range requests

For segment `[S, E]`:

```http
Range: bytes=S-E
```

Expected response:

```http
206 Partial Content
Content-Range: bytes S-E/TOTAL
```

The engine MUST reject a response when:

- status is inconsistent with the request and cannot be safely recovered;
- returned start offset differs from `S`;
- returned end exceeds the requested range;
- total size conflicts with established resource size;
- validators indicate a different generation;
- body exceeds the accepted range length.

A `200 OK` response to a nonzero range request is not safe to treat as the requested range. The engine should abort segmented mode and restart safely if policy allows.

### 11.3 Conditional requests

When resuming, use the strongest validator possible.

Examples:

```http
Range: bytes=S-E
If-Range: "strong-etag"
```

or an eligible Last-Modified date when a strong ETag is unavailable.

If the origin indicates that the resource changed, the engine must not append new bytes to old-generation data. It must either:

- restart from byte 0;
- fail with `ResourceChanged`;
- ask higher-level policy via a callback if the embedding API supports decisions.

The core engine should default to safety, not optimistic continuation.

### 11.4 Content encoding

To preserve byte-addressable range semantics, segmented requests should normally send:

```http
Accept-Encoding: identity
```

unless the implementation has verified that the server's ranges operate over the selected encoded representation and the output is intentionally that representation.

---

## 12. Segment scheduler

### 12.1 Interval representation

Represent remaining work as a normalized, non-overlapping set of byte intervals.

Example:

```text
Total:      [0 ........................................ 999]
Completed:  [0..199], [400..699]
Pending:    [200..399], [700..999]
```

The scheduler must maintain these invariants:

- no byte belongs to two active worker leases;
- completed ranges do not overlap pending ranges;
- union of completed + active + pending equals the target byte domain;
- ranges remain normalized after inserts/merges.

Recommended internal structures:

- ordered interval tree;
- B-tree/map keyed by start offset;
- specialized interval set.

Avoid one record per chunk for large files unless checkpoint compression makes it acceptable.

### 12.2 Initial segmentation

For a file of size `N` and target worker count `W`, prefer ranges large enough to avoid excessive requests.

Conceptually:

```text
segment_size = clamp(
    ceil(N / (W * oversubscription_factor)),
    min_segment_size,
    max_segment_size
)
```

Use an oversubscription factor around 2–4 so faster workers can consume additional work instead of waiting for one slow long-lived segment.

### 12.3 Dynamic splitting

When a worker becomes idle and another worker owns a large unfinished segment, the scheduler may split the unconsumed tail and assign it to the idle worker.

A split must never include bytes already read or queued for write by the original worker.

Dynamic splitting is especially important when connections have uneven throughput.

### 12.4 Adaptive concurrency

Initial v1 behavior may use fixed configured concurrency, but the scheduler should expose hooks for adaptive logic.

A future adaptive controller may consider:

- per-worker throughput;
- aggregate throughput gain from the last worker added;
- RTT;
- server response latency;
- retransmission/timeout signals if available;
- HTTP/2 stream behavior;
- local disk write latency;
- CPU load;
- configured connection limits.

Concurrency must never increase merely because more workers are permitted; it should increase when doing so improves throughput materially.

---

## 13. Worker execution model

Each worker repeatedly:

1. acquires a segment lease from the scheduler;
2. waits for rate-limit tokens if needed;
3. opens or reuses a transport stream;
4. issues a validated range request;
5. reads data into a bounded reusable buffer;
6. performs an offset-based write;
7. updates durable/completed progress at safe granularity;
8. emits metrics;
9. marks the lease complete or requeues the remainder after failure;
10. requests another segment.

Workers should be logical asynchronous tasks rather than permanently dedicated OS threads unless the target runtime requires otherwise.

### 13.1 Buffering

Recommended strategy:

- reusable pooled buffers;
- default chunk size 64–256 KiB;
- bounded number of buffers per active worker;
- at most a small pipeline of network-read -> disk-write operations per worker;
- no full-segment buffering.

The system must apply backpressure when disk writes fall behind network reads.

### 13.2 Copy minimization

Where the platform permits, avoid unnecessary copies between:

- transport receive buffer;
- application buffer;
- sink write buffer.

However, correctness and portability are more important than forcing zero-copy APIs. A single application-level copy is acceptable when it materially simplifies implementation.

### 13.3 Locking

Avoid a global mutex on every chunk.

Prefer:

- atomic counters for aggregate byte counts;
- per-worker local metrics folded periodically;
- scheduler locking only when acquiring/updating leases;
- independent positional file writes;
- event batching.

---

## 14. Sink and file I/O

### 14.1 Temporary file

Downloads should target a temp file distinct from the final path, for example:

```text
<destination>.part
```

or a collision-resistant hidden temp filename in the same directory.

Using the same filesystem/directory is preferred so final rename can be atomic.

### 14.2 Positional writes

Segmented mode requires random-access positional writes such as:

- `pwrite` / `pwritev`;
- overlapped I/O on Windows;
- equivalent async positional APIs.

Workers should not share a mutable seek pointer.

### 14.3 Preallocation

When total size is known, the engine SHOULD preallocate the file when supported.

Goals:

- fail early on insufficient disk space when possible;
- reduce fragmentation;
- avoid repeated filesystem metadata growth.

Preallocation failure due to unsupported filesystem operation may be non-fatal; lack of disk space is fatal.

### 14.4 Sparse files

The engine may use sparse allocation where appropriate, but the resulting semantics must be explicit because apparent file size and allocated blocks can differ.

### 14.5 Disk error handling

Disk-full, quota, permission, read-only filesystem, path disappearance, and I/O errors are terminal unless the embedding application provides a recovery mechanism.

Network workers must stop promptly after a terminal sink failure.

### 14.6 Final commit

On successful verification:

1. flush according to durability policy;
2. close/settle file handles as required by the platform;
3. apply optional metadata such as modification time if desired by caller policy;
4. atomically rename temp file to final path when possible;
5. remove checkpoint;
6. enter `Completed`.

Overwrite behavior must follow `OverwritePolicy`:

- `FailIfExists`;
- `Replace`;
- `AutoRename` handled by higher-level caller or optional utility;
- `ResumeIfMatching`.

---

## 15. Checkpoint and resume format

### 15.1 Requirements

Checkpoint data must be:

- versioned;
- crash-tolerant;
- atomically replaceable;
- validated before use;
- independent of in-memory pointer layout;
- extensible across minor engine releases.

A compact binary format is acceptable, but a canonical JSON/CBOR-like model is shown below.

### 15.2 Logical schema

```json
{
  "format_version": 1,
  "job_id": "...",
  "original_url": "https://example/file",
  "final_url": "https://cdn.example/file",
  "temp_path_identity": "opaque-local-id",
  "total_size": 123456789,
  "validators": {
    "etag": "\"abc123\"",
    "etag_is_weak": false,
    "last_modified": "..."
  },
  "completed_ranges": [
    [0, 8388607],
    [16777216, 25165823]
  ],
  "expected_hashes": [],
  "created_at": "...",
  "updated_at": "..."
}
```

### 15.3 Checkpoint durability

Never update a checkpoint in place if a torn write could make the only copy unreadable.

Use an atomic replace pattern:

1. write new checkpoint to temporary metadata file;
2. optionally fsync metadata file;
3. rename over old checkpoint atomically where supported;
4. optionally fsync parent directory when strong crash durability is required.

### 15.4 Ordering between data and checkpoint

A checkpoint must never claim a range is durable if the underlying bytes are not sufficiently persisted for the selected durability policy.

Two valid policies:

- **Performance mode:** checkpoint represents writes successfully acknowledged by the OS page cache; after power loss some supposedly completed bytes may need validation/redownload.
- **Durable mode:** data is flushed before corresponding completed intervals are committed to checkpoint.

The API/configuration must state which guarantee is active.

### 15.5 Resume validation

Before resuming:

1. validate checkpoint format;
2. verify temp file exists and size is plausible;
3. probe remote resource;
4. compare validators and total size;
5. optionally sample or hash persisted data if stronger validation is needed;
6. reconstruct remaining intervals;
7. resume only if generation identity is acceptable.

---

## 16. Integrity verification

### 16.1 Caller-provided hashes

Support at minimum:

- SHA-256;
- SHA-512.

MD5/SHA-1 may be supported for compatibility but must not be presented as collision-resistant integrity guarantees.

### 16.2 Hashing strategy

For segmented random-access downloads, a normal whole-file cryptographic hash cannot generally be finalized from segment hashes unless using a compatible tree construction.

Therefore v1 should use one of these methods:

- perform a sequential read of the completed file during `Verifying`;
- compute hash during write only in single-stream mode;
- later add a tree-hash scheme when the expected digest format supports it.

A final sequential verification pass is acceptable because it is disk-bound and provides strong correctness assurance.

### 16.3 Size verification

Known-size downloads must verify exact final byte length before commit.

### 16.4 Server-provided digests

Server digest headers may be recorded and validated when trustworthy and supported, but caller-provided expected hashes have precedence.

---

## 17. Retry behavior

### 17.1 Retryable conditions

Typically retryable:

- connection reset;
- temporary DNS failure;
- connect timeout;
- read timeout;
- selected TLS transport interruptions after a valid handshake;
- HTTP 408;
- HTTP 429 subject to `Retry-After`;
- HTTP 500, 502, 503, 504;
- truncated range body;
- HTTP/2 stream reset when not caused by a permanent request error.

Typically non-retryable without external change:

- 400;
- 401/403 unless credentials may refresh;
- 404/410;
- invalid URL;
- unsupported scheme;
- certificate validation failure;
- consistent validator mismatch;
- local permission error;
- disk full.

### 17.2 Backoff

Use exponential backoff with full jitter:

```text
cap = min(max_delay, base_delay * multiplier^attempt)
delay = random(0, cap)
```

Honor valid `Retry-After` when it requests a longer reasonable delay, subject to configured maximums.

### 17.3 Retry scope

Retry only the unfinished portion of the failed segment.

Do not redownload already committed ranges unless integrity validation requires it.

### 17.4 Circuit behavior

If many workers simultaneously receive server-wide failures such as 503 or 429, retries should be coordinated to avoid a thundering herd.

The job should enter a shared origin backoff state rather than allowing every worker to retry independently at full concurrency.

---

## 18. Bandwidth limiting

### 18.1 Hierarchical limits

The engine should support:

```text
Global limiter
  -> Per-job limiter
     -> worker consumption
```

A token bucket is recommended.

### 18.2 Semantics

- limits apply to payload bytes read from the network;
- limiter must not busy-wait;
- fairness between active workers is desirable;
- changing the job limit at runtime must take effect without restarting workers;
- unlimited mode should impose near-zero overhead.

### 18.3 Burst size

Permit limited bursts, typically around 100–500 ms worth of configured bandwidth, to avoid excessive timer wakeups while maintaining responsive limiting.

---

## 19. Progress and metrics

### 19.1 Progress snapshot

```text
ProgressSnapshot {
    state,
    total_size: optional u64,
    completed_bytes: u64,
    network_bytes: u64,
    reused_bytes: u64,
    active_workers: u32,
    instantaneous_rate: f64,
    smoothed_rate: f64,
    eta: optional Duration,
    retries: u64,
    elapsed: Duration
}
```

`completed_bytes` must mean unique file bytes completed, not sum of network traffic. Retries must not inflate logical progress.

### 19.2 Speed estimation

Use a smoothed estimator such as EWMA over recent payload throughput.

Avoid computing user-visible speed as total bytes / total elapsed time because startup/probe/pause periods distort it.

### 19.3 ETA

ETA is meaningful only when total size is known and smoothed rate exceeds a minimum threshold.

```text
eta = remaining_unique_bytes / smoothed_rate
```

ETA should be omitted rather than showing extreme unstable values during startup.

### 19.4 Events

Recommended events:

```text
StateChanged
ProbeCompleted
SegmentStarted
SegmentRetried
SegmentCompleted
Progress
RateLimitChanged
ResourceChanged
IntegrityCheckStarted
IntegrityCheckPassed
IntegrityCheckFailed
Committed
Warning
Failed
```

High-frequency chunk events should not be exposed by default.

### 19.5 Metrics for instrumentation

Counters/gauges/histograms should include:

- jobs started/completed/failed/cancelled;
- active jobs;
- active workers;
- active/reused connections if exposed by transport;
- bytes received;
- unique bytes completed;
- bytes retried/wasted;
- request latency;
- DNS/connect/TLS timing if available;
- response-header latency;
- write latency;
- checkpoint latency;
- retry counts by category;
- HTTP status counts;
- range protocol violations;
- integrity failures.

---

## 20. Error model

Use a structured error taxonomy rather than string-only errors.

```text
DownloadError
  ConfigurationError
  InvalidUrl
  UnsupportedScheme
  DnsError
  ConnectTimeout
  ConnectionError
  TlsError
  ProxyError
  AuthenticationRequired
  AuthorizationFailed
  NotFound
  ServerError
  RateLimited
  RedirectError
  ProtocolError
  RangeUnsupported
  InvalidRangeResponse
  ResourceChanged
  UnknownLengthUnsupportedForMode
  SinkOpenError
  SinkWriteError
  DiskFull
  PermissionDenied
  CheckpointError
  IntegrityMismatch
  CommitError
  Cancelled
  DeadlineExceeded
  RetryExhausted { last_error }
```

Each error should carry:

- stable category/code;
- human-readable message;
- retryability hint;
- source error/cause where supported;
- URL/origin context with sensitive data redacted;
- HTTP status when applicable;
- segment/range context when applicable.

Do not leak authorization headers, cookies, proxy credentials, signed query parameters, or bearer tokens into logs by default.

---

## 21. Security requirements

### 21.1 TLS

- certificate validation enabled by default;
- hostname validation enabled;
- no silent fallback from HTTPS to plaintext HTTP;
- custom CA bundle allowed by explicit configuration;
- insecure certificate bypass, if exposed at all, must require explicit opt-in.

### 21.2 Redirect credential safety

Authorization headers and cookies must not automatically cross origins unless caller policy explicitly allows it.

### 21.3 Local path safety

The engine receives a resolved destination path. It should not derive arbitrary filesystem paths directly from unsanitized server filenames.

Any optional filename extraction utility must sanitize:

- path separators;
- `..` traversal;
- NUL/control characters;
- reserved names where relevant;
- excessive filename length.

### 21.4 Resource exhaustion

Protect against malicious or broken servers by bounding:

- response header size;
- redirect count;
- retry count;
- concurrent streams;
- in-memory buffers;
- checkpoint size;
- metadata field lengths.

### 21.5 SSRF boundary

If the future application accepts untrusted URLs, the embedding layer may need SSRF controls. The engine should expose hooks to restrict resolved address ranges and redirect targets, but should not hard-code internet-only assumptions.

---

## 22. Performance requirements

These are engineering targets, not universal guarantees.

### 22.1 Throughput

On a modern desktop/server with a local SSD and a sufficiently fast remote origin, the engine should be capable of saturating at least a 1 Gbit/s link without excessive CPU usage.

Architecture should not prevent multi-gigabit operation.

### 22.2 CPU

At 1 Gbit/s HTTPS transfer on suitable hardware, target low single-digit to modest CPU core utilization depending on TLS library and runtime. Avoid avoidable per-byte work.

### 22.3 Memory

Memory must remain approximately:

```text
O(active_workers * per_worker_buffers + scheduler_metadata)
```

not `O(file_size)`.

Suggested default budget for one high-concurrency job: under ~32 MiB of transfer buffers, excluding HTTP/TLS library internals.

### 22.4 Syscalls

Favor moderately large reads/writes. Avoid 4 KiB application-level chunks for high-throughput network transfer unless the platform proves otherwise.

### 22.5 Scalability

The engine should support many concurrent jobs while preserving configured global bounds. Hundreds of idle/paused handles should not imply hundreds of threads.

---

## 23. Efficiency strategies

The implementation SHOULD adopt the following strategies where supported:

- async nonblocking networking;
- HTTP keep-alive and connection pooling;
- HTTP/2 multiplexing, while allowing multiple connections when one connection becomes a bottleneck;
- pooled reusable buffers;
- positional file writes;
- preallocation;
- batched progress updates;
- atomic counters for hot metrics;
- reduced scheduler lock frequency;
- no per-byte callbacks;
- avoidance of repeated allocation/parsing inside read loops;
- caching of validated origin capabilities for a bounded time;
- DNS caching according to runtime/network library behavior;
- cooperative cancellation tokens rather than forceful thread termination.

Optimization must be benchmark-driven. Any optimization that weakens correctness requires explicit rejection unless guarded by an opt-in unsafe mode.

---

## 24. HTTP/2 considerations

HTTP/2 changes the relationship between logical workers and TCP connections.

The transport layer should distinguish:

- concurrent range **streams**;
- underlying origin **connections**.

A job with 8 segment workers does not necessarily need 8 TCP/TLS connections.

However, implementations should allow multiple HTTP/2 connections to one origin if:

- the server limits concurrent streams;
- flow control becomes a bottleneck;
- one TCP connection underutilizes the path;
- empirical adaptive policy shows a material throughput gain.

Do not assume “one HTTP/2 connection is always optimal.”

---

## 25. Unknown-length resources

When total size is unknown:

- segmented mode is disabled;
- output is written sequentially;
- resume may be unavailable unless transport-specific semantics safely support it;
- progress reports bytes completed without percent/ETA;
- the engine must enforce optional caller-configured maximum size to guard against unexpectedly large responses.

Chunked transfer encoding itself is not an error.

---

## 26. Resource changes during download

Signals of possible resource mutation include:

- ETag change;
- Last-Modified change;
- total size change;
- mismatched `Content-Range` total;
- server returning full representation to an `If-Range` request;
- caller checksum mismatch after completion.

On detected generation change:

1. stop assigning new segments;
2. cancel/settle active segment requests;
3. mark current partial data generation-invalid;
4. emit `ResourceChanged`;
5. apply configured policy:
   - fail;
   - restart from zero after truncating/recreating temp state.

Never mix generations into one final file.

---

## 27. Connection management

### 27.1 Pooling

Connection pooling should be shared across jobs when compatible security/proxy settings permit.

Pool keys should include at least:

- scheme;
- origin host/port;
- proxy configuration;
- TLS trust/client identity configuration;
- protocol compatibility constraints.

### 27.2 Limits

Enforce both:

- engine-global connection limit;
- per-origin limit.

### 27.3 Keepalive

Idle connections should expire after a configurable timeout. Broken pooled connections must be retried transparently when safe.

---

## 28. Proxy support

The networking abstraction should allow:

- no proxy;
- HTTP proxy;
- HTTPS via CONNECT;
- SOCKS support as an optional extension;
- caller-defined proxy selection.

Proxy credentials are sensitive and must be redacted from logs.

---

## 29. Authentication hooks

The engine should accept credentials or a credential callback/provider but should not own long-term secrets.

Possible provider behavior:

```text
CredentialProvider.request(scope, challenge) -> headers/token
```

This permits future support for refreshable bearer tokens without coupling the engine to OAuth implementation details.

Avoid automatic repeated authentication retries that can lock accounts or create request loops.

---

## 30. Threading and async-runtime model

The design should work with:

- event-loop runtimes;
- async/await runtimes;
- completion-port based I/O;
- thread pools where async networking is unavailable.

Recommended model:

- network workers are async tasks;
- blocking filesystem operations, if truly blocking on the target platform, execute on a bounded I/O pool;
- CPU-heavy hashing may use a bounded CPU pool or streaming sequential verifier;
- public callbacks/events must not run while internal scheduler locks are held.

The engine must document whether callbacks are serialized or may occur concurrently.

---

## 31. Internal scheduler interface

Conceptual interface:

```text
SegmentScheduler {
    initialize(total_size, completed_ranges, policy)

    acquire(worker_id) -> optional SegmentLease
    report_progress(lease_id, durable_through_offset)
    complete(lease_id)
    fail(lease_id, completed_prefix, error)
    split_if_beneficial()

    pending_bytes() -> u64
    active_ranges() -> list<Range>
    completed_ranges() -> IntervalSet
}
```

`SegmentLease`:

```text
SegmentLease {
    id,
    generation,
    start,
    end,
    next_offset
}
```

Lease IDs/generation numbers prevent stale worker callbacks from mutating newly reassigned ranges after cancellation or retry.

---

## 32. Transport abstraction

Conceptual interface:

```text
Transport {
    probe(request, cancellation) -> ResourceMetadata

    open_range(
        request,
        range,
        validators,
        cancellation
    ) -> ByteStreamWithResponseMetadata

    open_full(
        request,
        validators,
        cancellation
    ) -> ByteStreamWithResponseMetadata
}
```

The scheduler must not contain HTTP-specific header parsing.

Transport must return enough metadata for job-level validation before body bytes are accepted.

---

## 33. Sink abstraction

```text
Sink {
    prepare(optional_total_size) -> Result
    write_at(offset, bytes) -> Result
    flush(mode) -> Result
    size() -> Result<u64>
    finalize() -> Result
    abort(cleanup_policy) -> Result
}
```

An optional sequential sink can be optimized for unknown-length downloads.

The default implementation is a local filesystem sink.

---

## 34. Checkpoint interface

```text
CheckpointStore {
    load(job_identity) -> optional Checkpoint
    save_atomic(checkpoint) -> Result
    delete(job_identity) -> Result
}
```

This permits the future download manager to store checkpoints in:

- sidecar files;
- SQLite;
- another transactional database;
- application state storage.

The initial default can be a sidecar metadata file.

---

## 35. Observability and logging

### 35.1 Logging levels

- `ERROR` — terminal failures;
- `WARN` — recoverable protocol oddities, retries nearing exhaustion, unsafe server behavior;
- `INFO` — job start/completion, selected mode, major state changes;
- `DEBUG` — segment assignment, retry classification, connection decisions;
- `TRACE` — detailed request lifecycle with sensitive data redacted.

### 35.2 Correlation fields

Every structured log event should support:

- engine instance id;
- job id;
- worker id when applicable;
- segment lease id;
- origin;
- request attempt;
- error category.

### 35.3 Sensitive data

Default logging must redact:

- Authorization;
- Cookie / Set-Cookie values;
- proxy authentication;
- URL userinfo;
- configurable sensitive query parameters;
- signed URLs when caller marks them sensitive.

---

## 36. Testing strategy

The engine requires significantly more than happy-path unit tests.

### 36.1 Unit tests

Cover:

- interval set insertion/merge/subtraction;
- scheduler lease uniqueness;
- dynamic segment splitting;
- retry classification;
- backoff bounds;
- rate limiter accounting;
- state machine transition validity;
- checkpoint serialization/versioning;
- validator comparison;
- Content-Range parsing;
- filename/path sanitization utilities;
- progress arithmetic;
- ETA/speed smoothing;
- error redaction.

### 36.2 Integration test server

Build a deterministic local HTTP test server capable of simulating:

- correct range support;
- no range support;
- lying `Accept-Ranges` header;
- ignored ranges returning 200;
- malformed `Content-Range`;
- short/truncated bodies;
- delayed headers;
- delayed chunks;
- random connection resets;
- specific HTTP error sequences;
- 429 with Retry-After;
- redirects;
- redirect loops;
- resource mutation mid-download;
- ETag changes;
- unknown content length;
- HTTP/2 stream resets if test stack supports them;
- authentication challenge;
- content encoding edge cases.

### 36.3 Crash/restart tests

Automated tests should repeatedly terminate the process at random points during a transfer, restart it, and verify final output correctness.

Test interruption during:

- segment writes;
- checkpoint save;
- checkpoint rename;
- final verification;
- final file rename.

### 36.4 Property-based tests

Use property testing for interval/scheduler logic.

Key property:

> For any sequence of segment acquisitions, partial completions, failures, splits, retries, pauses, and resumes, successful completion must cover every byte in `[0, N-1]` exactly once logically, regardless of how many times bytes were transferred physically.

### 36.5 Fuzzing

Fuzz parsers for:

- URL handling;
- Content-Range;
- ETag;
- Content-Disposition if implemented;
- checkpoint files;
- HTTP metadata adapters.

Malformed metadata must fail safely without panics, memory corruption, or path traversal.

### 36.6 End-to-end correctness tests

Generate random source files, download under induced failures, then compare exact content and cryptographic hash.

Include sizes around boundary values:

- 0 bytes;
- 1 byte;
- chunk size - 1 / exact / +1;
- segment size - 1 / exact / +1;
- large sparse-like files;
- >4 GiB to catch 32-bit offset bugs;
- sizes near platform API limits where feasible.

---

## 37. Benchmark suite

A repeatable benchmark harness is mandatory before labeling the engine “high performance.”

### 37.1 Network benchmarks

Measure:

- localhost HTTP/1.1;
- localhost HTTP/2;
- controlled LAN;
- high-bandwidth/high-latency simulated path;
- throttled origin;
- multiple connections vs one connection;
- varying worker counts.

### 37.2 Storage benchmarks

Test:

- fast NVMe;
- SATA SSD;
- slower HDD if relevant;
- tmpfs/RAM disk to isolate network/CPU bottlenecks;
- filesystem preallocation on/off.

### 37.3 Metrics

Record:

- throughput;
- CPU time;
- wall time;
- peak RSS;
- allocations if runtime supports profiling;
- context switches;
- syscalls where measurable;
- connection count;
- retransferred bytes;
- disk write latency.

### 37.4 Regression thresholds

CI performance tests should flag significant regressions on stable benchmark hosts, for example:

- >5–10% throughput loss;
- >15% CPU increase at equal throughput;
- unbounded or major memory increase;
- increased retry amplification.

Exact thresholds should be tuned for benchmark noise.

---

## 38. Failure scenarios and required behavior

| Scenario | Required behavior |
|---|---|
| Connection drops mid-segment | Requeue unfinished tail and retry with backoff. |
| Server returns 503 | Coordinated backoff; retry according to policy. |
| Server returns 429 | Honor `Retry-After` within policy limits. |
| Range request returns 200 | Do not write as requested range; safely downgrade/restart or fail. |
| ETag changes on resume | Treat as resource generation change; never mix bytes. |
| Disk fills | Stop workers, surface `DiskFull`, preserve consistent partial state if possible. |
| Process killed | On restart, validate checkpoint/temp file and resume safe ranges. |
| Checkpoint corrupt | Do not trust it; recover conservatively or restart according to policy. |
| User pauses | Stop network activity, settle writes, persist resume state, enter `Paused`. |
| User cancels/delete | Stop workers, then remove partial artifacts according to mode. |
| Hash mismatch | Never commit as successful; surface `IntegrityMismatch`. |
| Final destination exists | Follow explicit overwrite policy. |
| Unknown content length | Use single stream and indeterminate progress. |
| Proxy/TLS auth fails | Return structured error; no silent bypass. |

---

## 39. Correctness invariants

The implementation must preserve all of these:

1. A byte range is never considered complete merely because it was requested; completion follows successful sink write at minimum.
2. No final `Completed` result is emitted before integrity checks and commit succeed.
3. Segments from different detected resource generations are never combined.
4. Logical completed bytes never exceed known total size.
5. A retry may increase network bytes but must not incorrectly increase unique completed bytes.
6. A stale worker lease cannot report completion for a range reassigned to another generation/lease.
7. Checkpoint claims never intentionally exceed data durability promised by the selected checkpoint policy.
8. Cancellation eventually prevents further network reads and sink writes.
9. Any accepted `206` response is validated against requested offsets before body data is committed.
10. Every terminal error is observable to the caller with a stable category.

---

## 40. Suggested initial defaults

These defaults are conservative and should be tuned by benchmark data:

| Setting | Default |
|---|---:|
| Segmentation threshold | 16 MiB |
| Max workers/job | 8 |
| Initial workers | min(4, max workers) |
| Initial segment size | 8 MiB |
| Min segment size | 1 MiB |
| Max segment size | 64 MiB |
| Chunk/buffer size | 128 KiB |
| Per-origin connections | 8–16 max |
| Connect timeout | 10 s |
| Header timeout | 20 s |
| Read idle timeout | 30 s |
| Retry attempts/segment | 8 |
| Backoff base | 250 ms |
| Backoff max | 30 s |
| Progress event cadence | 250–500 ms |
| Checkpoint cadence | 1–2 s plus state transitions |

The engine should start with fewer workers and allow policy to scale up rather than immediately opening the maximum number of connections.

---

## 41. Suggested implementation phases

### Phase 1 — Correct single-stream core

Implement:

- engine/job API;
- HTTP(S) full GET;
- temp file sink;
- cancellation;
- progress;
- structured errors;
- final rename;
- expected size/hash verification;
- integration test server.

Exit criterion: reliable sequential downloads under induced disconnects with restart-from-zero retry.

### Phase 2 — Safe resume

Implement:

- probe;
- ETag/Last-Modified capture;
- checkpoint store;
- persisted byte interval state;
- conditional range resume;
- crash/restart tests.

Exit criterion: interrupted downloads resume without corruption.

### Phase 3 — Segmented downloading

Implement:

- interval scheduler;
- range validation;
- positional writes;
- bounded workers;
- dynamic work queue;
- single-stream fallback.

Exit criterion: exact output across randomized worker failures and range edge cases.

### Phase 4 — Performance engineering

Implement/tune:

- connection reuse;
- HTTP/2 behavior;
- buffer pool;
- preallocation;
- lock reduction;
- batched events;
- benchmark harness;
- adaptive segment splitting.

Exit criterion: saturate target network link without excessive CPU/memory.

### Phase 5 — Production hardening

Implement:

- proxy/auth hooks;
- hierarchical rate limits;
- robust redaction;
- fuzzing;
- telemetry;
- origin-wide retry coordination;
- extensive platform-specific filesystem tests.

---

## 42. Acceptance criteria for v1

The download engine is considered v1-ready when all of the following hold:

### Correctness

- downloads exact files from compliant HTTP/1.1 and HTTP/2 origins;
- segmented downloads validate every range response;
- final output matches source across randomized failure tests;
- resume never knowingly mixes resource generations;
- checksum mismatch prevents commit;
- >4 GiB files are handled correctly on supported 64-bit platforms.

### Reliability

- transient network failures retry without restarting completed ranges;
- pause/resume survives process restart;
- corrupted checkpoint files fail safely;
- terminal disk errors stop network workers promptly;
- cancellation does not leave active worker tasks indefinitely.

### Performance

- memory remains bounded with file size;
- no thread-per-segment requirement;
- connection pooling is active;
- high-speed local benchmark demonstrates near-link saturation on target hardware;
- no hot global lock on the chunk transfer path.

### API quality

- higher layers can start, pause, resume, cancel, and observe a job;
- progress distinguishes network bytes from unique completed bytes;
- errors are structured and actionable;
- transport, sink, and checkpoint layers are replaceable behind interfaces.

### Security

- TLS validation is on by default;
- HTTPS downgrade redirects are rejected by default;
- credentials are not leaked across origins by default;
- sensitive headers and URL secrets are redacted from logs;
- untrusted server metadata cannot create arbitrary local paths.

---

## 43. Recommended source tree

Language-neutral example:

```text
/download-engine
  /src
    engine.*
    config.*
    error.*

    /job
      controller.*
      state.*
      progress.*

    /http
      transport.*
      probe.*
      range.*
      validators.*
      redirect.*

    /scheduler
      interval_set.*
      scheduler.*
      lease.*

    /io
      sink.*
      file_sink.*
      buffer_pool.*

    /resume
      checkpoint.*
      checkpoint_store.*

    /control
      cancellation.*
      retry.*
      rate_limit.*

    /integrity
      verifier.*

    /metrics
      events.*
      counters.*

  /tests
    unit/
    integration/
    crash/
    property/
    fuzz/

  /bench
    server/
    scenarios/
```

---

## 44. Future extension points

The initial interfaces should leave room for:

### Mirror-aware downloading

A future `SourceSet` may represent several URLs known to contain the same object. Resource identity must be proven before ranges from multiple sources are mixed.

### HTTP/3

Add a new transport implementation without changing scheduler/sink semantics.

### Download-manager orchestration

The future manager should wrap this engine with:

- global queue and priorities;
- persistent job database;
- UI/API layer;
- schedules;
- categories/tags;
- user settings;
- browser integration;
- duplicate detection;
- post-download actions.

The engine should remain unaware of those concerns.

### Pluggable storage

Possible future sinks:

- encrypted local file;
- object storage;
- in-memory buffer for small resources;
- streaming consumer;
- content-addressed storage.

---

## 45. Implementation guidance by language family

This specification is language-neutral. Suitable mappings include:

### Rust

- Tokio or equivalent async runtime;
- Hyper/Reqwest-style HTTP stack depending desired control;
- `pread`/`pwrite` equivalents or platform async file APIs;
- channels/cancellation tokens;
- interval structure using `BTreeMap` or specialized crate.

Rust is a strong fit when memory safety, predictable performance, and a reusable native core are priorities.

### Go

- goroutines for workers;
- shared `http.Transport` with explicit connection limits;
- `WriteAt` for positional output;
- contexts for cancellation;
- mutex/atomic discipline around scheduler and counters.

Go is a strong fit when implementation speed and operational simplicity are priorities.

### C++

- Asio/Boost.Asio or another mature async networking stack;
- platform positional I/O;
- careful ownership/lifetime design;
- sanitizers and fuzzing strongly recommended.

### Java/Kotlin

- modern async HTTP client or Netty;
- `FileChannel.write(buffer, position)`;
- structured executor management;
- avoid allocating new byte arrays in the hot path.

---

## 46. Design decisions that must remain explicit

Before implementation begins, the project should lock down these choices in an Architecture Decision Record (ADR), because they materially affect behavior:

1. **Primary implementation language/runtime.**
2. **HTTP stack** and degree of low-level control required.
3. **Default checkpoint storage**: sidecar file vs database adapter supplied externally.
4. **Default durability guarantee** for checkpoints.
5. **Whether v1 enables HTTP/2 segmented streams by default.**
6. **Whether adaptive worker count is v1 or post-v1.**
7. **Filesystem support baseline** across Windows/macOS/Linux.
8. **How destination conflicts are owned** by engine vs download-manager layer.
9. **Whether server-provided filenames are handled by engine utilities or exclusively by higher layers.**
10. **Whether credentials can refresh through callbacks during an active job.**

These are implementation decisions, not reasons to weaken the core invariants in this specification.

---

## 47. Minimal pseudocode flow

```text
function run_job(request):
    transition(Probing)
    metadata = transport.probe(request)
    validate_probe(metadata)

    resume_state = checkpoint_store.load(job_identity)
    plan = build_transfer_plan(request, metadata, resume_state)

    transition(Preparing)
    sink.prepare(metadata.total_size)
    scheduler.initialize(metadata.total_size, plan.completed_ranges, policy)

    transition(Running)

    spawn bounded workers:
        while not cancelled:
            lease = scheduler.acquire(worker)
            if none:
                break

            try:
                response = transport.open_range_or_full(...)
                validate_response(response, lease, metadata.validators)

                while chunk = response.read_bounded():
                    rate_limiter.acquire(chunk.length)
                    sink.write_at(lease.next_offset, chunk)
                    lease.advance(chunk.length)
                    scheduler.report_progress(lease, lease.next_offset)
                    metrics.add_unique_and_network_bytes(...)

                scheduler.complete(lease)

            catch retryable error:
                scheduler.fail(lease, durable_prefix, error)
                coordinated_backoff(error)

            catch terminal error:
                fail_job(error)

    if cancelled:
        settle_and_checkpoint()
        transition(Cancelled or Paused)
        return

    assert scheduler.pending_bytes() == 0

    transition(Verifying)
    verify_size()
    verify_hashes_if_required()

    transition(Committing)
    sink.finalize()
    checkpoint_store.delete(job_identity)

    transition(Completed)
```

This pseudocode is illustrative. A production implementation must ensure that logical completion reflects successful writes and selected durability semantics, not merely bytes read from the network.

---

## 48. Final architectural rule

The download engine must optimize for **recoverable progress** rather than raw parallelism alone.

A fast engine that occasionally corrupts files, restarts large downloads unnecessarily, overwhelms origins, or cannot reliably recover after interruption is not a high-quality download engine. Throughput, correctness, and resumability must be designed together.
