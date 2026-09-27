# Spec Delta

## Purpose

Ensures checkpoint-memory and serialization limits are enforceable before allocation, including remotely supplied validator text.

## ADDED Requirements

### Requirement: Sound bounded checkpoint estimate
Before serializing, retaining, or passing a checkpoint to any configured store, the engine SHALL enforce the configured checkpoint cap using a checked upper bound that includes all serialized fields and escaping costs, notably ETag and Last-Modified values; overflow SHALL fail closed. The job and controller memory reservations SHALL cover actual held checkpoint bytes and SHALL never report a high-water below a retained allocation.

#### Scenario: Large validator header
- **WHEN** an ETag contains enough characters to exceed a 1 KiB checkpoint cap after serialization
- **THEN** pre-allocation validation rejects it before an oversized checkpoint allocation or injected-store save

#### Scenario: Escaping and overflow
- **WHEN** validator strings require JSON escaping, or lengths/counts overflow an estimate
- **THEN** the bound does not undercount serialization and the job fails within the configured limits

#### Scenario: Configured checkpoint resolver
- **WHEN** a caller supplies a checkpoint store instead of the default file store
- **THEN** the same pre-allocation limit and memory reservation apply before calling that store
