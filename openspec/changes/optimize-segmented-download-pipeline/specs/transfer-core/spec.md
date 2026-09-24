# Spec Delta

## Purpose

Defines safe, efficient segmented transfer scheduling and worker controls while retaining validated ranges, unique coverage, retry, resume and generation invariants.

## ADDED Requirements

### Requirement: Small ordinary chunk path and coherent progress
An ordinary segmented body chunk SHALL be handled without output-wide locking, shared seek operations, filesystem synchronization, checkpoint persistence or scheduler-wide reconciliation. Published worker progress SHALL identify its lease, generation and written-through position as one coherent observation. Only bytes successfully written within the validated lease SHALL advance eligible progress.

#### Scenario: Concurrent chunk completion
- **WHEN** two workers finish disjoint chunks in either order while a checkpoint snapshot is read
- **THEN** the snapshot never combines a lease identity from one publication with an offset or generation from another

#### Scenario: Retry after partial lease
- **WHEN** a range fails after a successfully written prefix
- **THEN** the scheduler recognizes the unique prefix, requeues only the unwritten tail and does not count or commit duplicate coverage

#### Scenario: Representation changes
- **WHEN** a response validator or lease generation becomes stale during segmented transfer
- **THEN** stale bytes cannot enter completed coverage and existing generation restart/rejection semantics remain in force

### Requirement: Scheduler target sizing
Initial leases SHALL use a target size within the configured minimum and maximum. An explicitly configured initial segment size SHALL be honored; a separately selected automatic mode SHALL derive its target from remaining bytes, initial active workers and a tunable oversubscription factor (initial candidate: ceiling of remaining bytes / (workers × 3), clamped to bounds). Tail splitting SHALL follow scheduler-owned policy rather than a worker-specific constant. Existing default configuration MUST remain valid and retain its documented meaning.

#### Scenario: Explicit size
- **WHEN** a user configures an initial size between minimum and maximum
- **THEN** initial leases use that target except for shorter remaining gaps

#### Scenario: Automatic size
- **WHEN** the user explicitly selects automatic sizing for a small or large job
- **THEN** the scheduler computes and clamps a target from remaining coverage and worker count, enabling multiple work units per active worker where the file size permits

#### Scenario: Final tail
- **WHEN** the remaining range is smaller than the minimum target or a live lease is split
- **THEN** exact coverage is retained with no gap or overlap and split thresholds obey the same scheduler policy

### Requirement: Event-driven persistent concurrency
A job SHALL retain worker capacity up to the configured maximum and allow its desired active range-worker count to increase or decrease within configured bounds without rebuilding the job. Deactivated workers SHALL settle current leases safely and remain available for reactivation. Idle workers SHALL wait for relevant scheduler-state transitions without periodic fixed-duration polling; notifications MUST NOT lose work if transitions race waiter registration. Fixed concurrency SHALL remain available, and manual runtime updates SHALL affect an active segmented job without disrupting retry, cancellation, pause, or terminal completion.

#### Scenario: Requeue or split
- **WHEN** a failed lease is requeued or a live lease becomes splittable while workers are idle
- **THEN** eligible workers awaken and acquire work without a fixed polling interval

#### Scenario: Decrease and reactivation
- **WHEN** desired concurrency decreases during transfer and later rises
- **THEN** excess workers settle safely, become inactive, and later resume without losing work or exceeding the maximum

#### Scenario: Notify race
- **WHEN** state changes immediately before or during an idle worker's wait registration
- **THEN** it observes the change or is awakened; completion and cancellation always unblock parked workers

### Requirement: Conservative adaptive range concurrency
When explicitly enabled, active range concurrency SHALL start at the configured minimum, remain within the configured minimum and maximum, and probe increases conservatively based primarily on useful completed-byte goodput. The controller SHALL consider retry/retransfer and throttling or storage pressure, use observation windows and hysteresis/cooldown, revert unhelpful probes, and avoid rapid oscillation. Explicit manual concurrency settings SHALL remain usable; adaptive range-stream concurrency SHALL NOT implicitly change the physical HTTP connection policy. Fixed mode SHALL preserve configured fixed-concurrency behavior.

#### Scenario: Beneficial probe
- **WHEN** a stable measurement window shows materially better useful goodput without unacceptable pressure after one additional active worker
- **THEN** the controller may retain the increase within bounds

#### Scenario: Harmful probe
- **WHEN** increased retries, throttling, storage pressure or no material goodput gain follows a probe
- **THEN** the controller holds or reverts and respects cooldown before probing again

#### Scenario: Multiplexed HTTP/2
- **WHEN** more range workers are activated over HTTP/2
- **THEN** connection-count policy remains independent; extra physical connections are not assumed or required

### Requirement: Low-contention controls and job-wide memory accounting
An ordinary worker SHALL detect terminal failure without acquiring an asynchronous error lock; only one authoritative terminal error SHALL be retained. Disabled rate limiting SHALL not require a per-chunk outer mutex, and runtime changes SHALL take effect without replacing the object used by active workers. Transfer payloads SHALL be written from received byte chunks without mandatory additional copying. Any configured explicit transfer-memory budget SHALL be accounted at job scope, not independently multiplied per worker; any reusable buffer-pool acquisition contract SHALL distinguish immediate exhaustion from waiting.

#### Scenario: Concurrent failures
- **WHEN** workers report terminal errors simultaneously
- **THEN** exactly one authoritative error is retained and other workers stop safely

#### Scenario: Live rate update
- **WHEN** the job rate switches between unlimited and limited while workers are active
- **THEN** subsequent payload acquisitions follow the updated limit without restarting workers or a lock merely to detect unlimited mode

#### Scenario: Multiple large chunks
- **WHEN** many workers receive body chunks concurrently
- **THEN** job-wide explicitly retained or staged transfer memory obeys the configured budget and no extra pooled-buffer copy is required solely for accounting
