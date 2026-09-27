# Proposal

## Why

`Analysis.md` (source review at `1cf85ac`, including isolated loopback reproductions) identifies four P0 failures: cross-origin credential disclosure, successful publication of bytes not received, unsafe checkpoint reuse, and writes through an attacker-placed `.part` symlink. It also identifies broken durability/control/resource contracts and fail-open release evidence; the completed earlier stability change and existing acceptance documentation do not make these counterexamples safe. Remediate these before any production-stability claim.

## What Changes

- Fix redirect origin resolution and default credential stripping across all hops, including segmented requests and provider-supplied credentials; preserve explicit, scoped opt-in only where safe.
- Make fresh output exclusive and byte-clean; require exact acknowledged coverage (or acknowledged EOF) before publication, independently of file length/preallocation. Prevent symlink and directory-entry substitution from changing the file written or published.
- **BREAKING:** refuse reuse of checkpoints without comparable validators and a trustworthy binding to the partial output; incompatible legacy checkpoint/partial pairs are discarded or fail closed according to policy, rather than trusted by size alone.
- Make Durable sequential checkpoints sync data before committing range metadata; enforce job deadlines, prompt cancellation of waits, and `max_active_jobs` admission; bound checkpoint serialization including untrusted validator text.
- Correct network amplification accounting, finish event streams at job termination, and make URL diagnostics and checkpoint sidecars secret-safe by default without breaking request URL identity or authorized transfers.
- Make all required release evidence (including non-approving prerequisites) fresh and commit-bound; extend deterministic regressions and platform verification, repair broken API-documentation references and workspace lint wiring, and withhold the production-stable designation until the full gate passes for the candidate commit.

## Capabilities

### New Capabilities

There are no main specs under `openspec/specs/`; these names follow the established change-local capability organization. Prior deltas describe intended safety, but the source and reproductions show gaps; the new deltas strengthen rather than silently override those intentions.

- `http-transport`: Resolved redirect origin, credential scope and segmented authentication parity.
- `integrity`: Exact accepted-byte coverage and acknowledged-EOF publication, including preallocated and resumed output.
- `resume-and-storage`: Safe partial-file ownership/publication, validator-based resume, durable checkpoint ordering.
- `engine-api`: Enforced deadlines, cancellation latency, and concurrent-job admission.
- `transfer-resource-bounds`: Sound pre-allocation checkpoint size and reservation accounting.
- `observability`: Accurate amplification and finite terminal event streams.
- `request-secret-redaction`: Safe URL diagnostics and protected checkpoint persistence.
- `performance-release-gates`: Revision-bound, fresh prerequisite and approval evidence; honest stability verdicts.
- `reliability-verification`: Repeatable regression/platform checks and maintainable API verification/documentation.

### Modified Capabilities

None: `openspec list --specs` reports no main capability specs to modify. Earlier change-local specs and `docs/acceptance-v1.md` describe guarantees that the reproduced findings contradict; update the latter and reconcile earlier expectations during implementation.

## Impact

Rust engine `crates/engine/src/{http,io,resume,job,control,metrics,error.rs,redact.rs,config.rs}`, facade/config/documentation and their integration/fault/property tests; release tooling `scripts/release_gate.py`, CI, `release/evidence-manifest.json`, `Cargo.toml`, `crates/engine/Cargo.toml`, `README.md` and `docs/{acceptance-v1,performance-report-v1,api-surface}.md`. Resume state compatibility and possibly consumer-visible terminal/event semantics change; no production-stability claim is made by writing these artifacts or by PR smoke alone.
