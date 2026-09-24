# Spec Delta

## Purpose

Defines byte-correct, efficient range scheduling and adaptive worker behavior under changing network, storage and concurrency conditions without sacrificing retry or fallback safety.

## ADDED Requirements

### Requirement: Actual elastic range concurrency
In adaptive mode the engine SHALL increase and decrease *actual* simultaneously active range-request capacity within configured minimum and maximum bounds; changing the desired count alone SHALL NOT count as growth. Dormant workers MUST hold no network request, range lease or dedicated blocking filesystem thread and MUST wait without busy polling. A reduction SHALL settle or safely return owned work; fixed/manual control SHALL remain available.

#### Scenario: Beneficial growth
- **GIVEN** an eligible range resource with `min_workers = 1`, `max_workers >= 4` and controlled measurements showing benefit from multiple requests
- **WHEN** adaptive policy requests growth
- **THEN** at least two workers can hold simultaneous validated range requests, desired and observed active counts reflect that, and output completes byte-exactly

#### Scenario: Shrink and reactivation
- **GIVEN** multiple workers holding disjoint leases
- **WHEN** desired concurrency falls and later rises
- **THEN** excess workers finish or return leases safely, park without polling or blocking-thread retention, later awaken, and no range is lost or credited twice

### Requirement: Contiguous acknowledged transfer progress
Network receipt, write submission, write acknowledgement, unique scheduler-accepted completion and checkpoint eligibility SHALL be distinct. If later writes finish first, published completion for an in-flight range MUST NOT advance past a gap. Stale-generation or abandoned write completions MUST NOT credit the new lease; retries SHALL only requeue uncovered coverage.

#### Scenario: Reverse completion
- **GIVEN** two consecutive writes for one range complete in reverse order
- **WHEN** the later completion arrives before the earlier one
- **THEN** acknowledged range progress remains at the earlier gap until it is acknowledged, then advances across both exactly once

#### Scenario: Retry and generation change
- **GIVEN** a partial lease with queued writes
- **WHEN** the request fails or its validator generation changes
- **THEN** all in-flight completions settle or are invalidated before ownership changes, unique completed intervals remain non-overlapping and only eligible uncovered bytes are retried

### Requirement: Adaptive duration-informed range allocation
When opt-in adaptive sizing is selected, new ranges SHALL use smoothed observed *useful* progress and a measured target-duration policy within configured minimum/maximum sizes. The policy MUST avoid pathological small requests, account for request/RTT cost, bound step-to-step size changes and preserve deterministic inclusive request endpoints and exclusive write frontiers. Explicit sizing SHALL retain its configured meaning.

#### Scenario: Fast versus slow observed service
- **GIVEN** two otherwise comparable eligible jobs with substantially different sustained per-worker useful goodput
- **WHEN** new work is allocated after stable samples
- **THEN** the target sizes differ in the appropriate direction, stay within bounds and do not oscillate on one outlier

#### Scenario: Final partial range and resume
- **GIVEN** a short final gap or a checkpoint containing verified completed ranges
- **WHEN** new segments are allocated
- **THEN** every request covers only pending bytes once, including the final partial range, regardless of new target size

### Requirement: Pending work before disruptive live splitting
The scheduler SHALL normally use unclaimed pending work to balance workers. A live-tail split MUST NOT give another worker bytes already received or queued for write by the original worker unless the original request is safely stopped or its excess payload is explicitly tracked as wasted. For a deterministic clean H1 fixture, wire amplification (payload received / newly unique completed bytes) MUST remain below 1.10 in the split/rebalance regression; 2.0 or greater MUST fail. This tolerance covers limited in-flight buffered data and is measured at the server as well as in client counters.

#### Scenario: Idle worker after partial H1 range
- **GIVEN** one large H1 range already streaming and another worker becomes available
- **WHEN** balancing creates additional work
- **THEN** the final hash and coverage are exact, server-emitted and client-received bytes are reconciled, and measured amplification remains below 1.10

#### Scenario: Retry and many ready ranges
- **GIVEN** pending segments and a retried tail
- **WHEN** workers become idle or concurrency changes
- **THEN** pending work is preferred and the union of pending, active and completed coverage stays exactly equal to the target domain

### Requirement: Useful-goodput adaptive decisions
Adaptive concurrency SHALL optimize unique newly completed bytes per unit time subject to bounded wire amplification, CPU, memory, storage pressure, retry/throttle signals and origin policy. Decisions SHALL use stable observations and hysteresis/cooldown rather than reacting to one spike; manual concurrency overrides SHALL take precedence. A storage-saturated job MUST NOT grow network concurrency merely because raw network throughput increases.

#### Scenario: Storage saturation
- **GIVEN** persistent writer backlog or high acknowledgement latency and no marginal useful-goodput gain
- **WHEN** adaptive policy evaluates another worker
- **THEN** it holds or reduces actual request concurrency instead of hiding the bottleneck in queued payload

#### Scenario: Noisy probe and throttling
- **GIVEN** an unhelpful probe or repeated 429/503 responses
- **WHEN** an observation window ends
- **THEN** concurrency does not oscillate rapidly; negative pressure prevents further immediate probes and later recovery remains possible

### Requirement: Compatible rate and range failure behavior
Rate limiting SHALL honor runtime job-limit updates and aggregate configured global and job limits with bounded burst and prompt pause/cancel interruption; optimization SHALL NOT bypass either limit. Range-unfriendly and inconsistent responses MUST retain safe single-stream fallback or structured failure, never write unvalidated bytes at a range offset.

#### Scenario: Invalid range response
- **GIVEN** an origin ignoring Range, returning malformed `Content-Range`, premature EOF, inconsistent length, unexpected status or changed validator
- **WHEN** a segmented request is opened or consumed
- **THEN** existing validation/fallback or failure applies before unsafe credit or publication, regardless of concurrency policy

#### Scenario: Rate limit and cancellation
- **GIVEN** simultaneous jobs with global and per-job limits
- **WHEN** a rate changes while requests run and one job is cancelled
- **THEN** each applicable aggregate limit remains enforced, waiting stops on cancellation, and no range/limiter capacity leaks
