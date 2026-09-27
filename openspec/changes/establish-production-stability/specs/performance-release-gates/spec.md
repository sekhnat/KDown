# Spec Delta

## Purpose

Defines repeatable performance and production-readiness gates that prevent regressions from being hidden by loopback-only throughput numbers or incomplete test evidence.

## ADDED Requirements

### Requirement: Multi-axis representative performance measurements
The benchmark suite SHALL measure throughput, network-byte amplification, transfer-pipeline memory high-water, CPU overhead, and concurrency scaling in both controlled loopback and versioned realistic WAN/network-condition profiles (latency, jitter, loss, and bandwidth). Each profile SHALL record dataset, protocol, worker/job counts, environment, repetitions, and machine-readable results. Unmeasurable metrics MUST be marked unavailable and MUST NOT be treated as zero or passing.

#### Scenario: WAN throughput improvement with regressions elsewhere
- **WHEN** a change improves WAN throughput but increases retransmitted bytes or memory usage
- **THEN** the report shows each axis independently and applies its own regression threshold

#### Scenario: Scaling comparison
- **WHEN** concurrent-job and per-job worker counts increase across prescribed profiles
- **THEN** the report captures throughput, CPU cost, byte amplification, and high-water memory at every configured scale point

### Requirement: Baseline-driven release decision
Performance limits and permitted regressions SHALL be explicit, numeric, versioned per profile and axis, established on matched or calibrated environments before a production release, and reviewed when changed. Fast PR smoke checks SHALL detect broken benchmark scenarios; scheduled/release runs SHALL compare representative loopback and WAN measurements to approved baselines and fail on a threshold breach or missing data. The production-stable designation SHALL require passing correctness, durability, resource-bound, interoperability, stress, vulnerability, and performance gates with no known unmitigated high-severity defects; an absent or stale required report SHALL prevent the designation.

#### Scenario: Same-host throughput regression
- **WHEN** a scheduled run exceeds the approved loss threshold against a matched loopback baseline
- **THEN** the production gate fails, regardless of a passing PR smoke check

#### Scenario: WAN profile missing
- **WHEN** loopback benchmarks pass but the required realistic WAN profile has no valid recent result
- **THEN** production stability is not declared

#### Scenario: High-severity defect
- **WHEN** all benchmarks pass but an unmitigated high-severity correctness or security defect is known
- **THEN** production stability is not declared
