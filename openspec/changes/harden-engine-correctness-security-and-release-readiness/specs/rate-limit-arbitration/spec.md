# Spec Delta

## Purpose

Ensures shared global and per-job rate limits jointly govern payload delivery without letting evaluation order select a less restrictive delay or charge tokens twice.

## ADDED Requirements

### Requirement: Most restrictive applicable wait governs
For each payload acquisition, the effective delay SHALL equal the maximum of the waits required by every active applicable global and job-level bucket, treating unlimited/absent buckets as zero delay. Bucket order SHALL NOT change the chosen delay; each applicable bucket SHALL account for those bytes once. Sequential and segmented transfer paths SHALL share these semantics.

#### Scenario: Global bucket slower
- **WHEN** the global bucket requires a longer wait than the job bucket for the same payload
- **THEN** the effective delay equals the global wait, irrespective of evaluation order

#### Scenario: Job bucket slower
- **WHEN** the job bucket requires a longer wait than the global bucket
- **THEN** the effective delay equals the job wait, irrespective of evaluation order

#### Scenario: Equal waits
- **WHEN** global and job buckets require equal waits
- **THEN** the effective delay equals that common wait and neither bucket debits the bytes more than once

#### Scenario: Only global bucket active
- **WHEN** only the global limit is active
- **THEN** the effective delay is the global bucket's wait

#### Scenario: Only job bucket active
- **WHEN** only the job limit is active
- **THEN** the effective delay is the job bucket's wait

#### Scenario: Both buckets unlimited
- **WHEN** neither limit is active or both are unlimited
- **THEN** acquisition returns without a rate-limit wait

### Requirement: Cancellation remains prompt during rate gating
A transfer SHALL remain cancellable while awaiting a combined rate-limit delay, without bypassing token accounting or deadlocking an active bucket.

#### Scenario: Cancel while waiting
- **WHEN** cancellation is requested while a sequential or segmented transfer awaits a required rate-limit delay
- **THEN** the wait ends promptly and the transfer follows its normal cancellation/cleanup semantics
