# Regression triage: seeds, fixtures, and named regression tests

This document defines the process for turning a discovered production bug
into a permanently named, deterministically replayable regression test.
It satisfies the establish-production-stability requirement that "a
deliberately replayed failure becomes a named regression test with
deterministic reproduction instructions" (task 4.5).

## 1. Severity triage

Every discovered production bug is triaged before the fix lands:

| Severity | Meaning                                            | Required response                                            |
| -------- | -------------------------------------------------- | ------------------------------------------------------------ |
| S0       | Corrupt published output, data loss, or a crash    | Block release. Fix + regression test before any next tag.    |
| S1       | Wrong accounting/state that a consumer can observe | Fix in the current change set; regression test mandatory.    |
| S2       | Resource-bound violation without corruption        | Fix or gate with a documented limit; regression test recommended. |
| S3       | Cosmetic / diagnostics-only                        | Fix opportunistically; regression test optional. |

Severity and fix status are recorded in the change's `tasks.md` notes or
the bug tracker entry; S0/S1 defects without a named regression test block
the release-evidence manifest (see the establish-production-stability
spec, group 5).

## 2. Seed and fixture retention

Deterministic inputs are the reproduction. Two retention forms:

1. **Seeds.** When the trigger is a seeded decision (network-condition
   draws, scheduler interleavings, property-test shrinking), the seed is
   recorded as a named constant inside the regression test and echoed to
   test output. Seeds live in the test source — never in a scratch file.
2. **Fixtures.** When the trigger needs captured bytes (a malformed
   response, a hostile header flood, a corrupt sidecar), the raw bytes are
   stored under `crates/engine/tests/fixtures/` with a README entry
   describing origin and shape. Fixtures are committed; they are never
   regenerated at test time from network inputs.

The scripted test server (`tests/support/test_server.rs`) and the
network-condition controls (`NetworkConditions`, task 4.1) replay both
forms deterministically: same seed plus same server script yields the same
wire behavior, independent of how many sockets pooling opens.

## 3. Naming and structure

Regression tests are named
`regression_<YYYY_MM_DD>_<slug>` (for example
`regression_2026_09_27_sink_write_skips_terminal_transitions`) and live in
`crates/engine/tests/regression_tests.rs` (crate-internal when the
reproduction needs internal seams such as the fault script). Each test
carries, in its doc comment:

- **Observed**: what the bug looked like (the wrong observable).
- **Expected**: the correct observable, with the spec/design reference.
- **Reproduction**: the exact command (`cargo test -p kdown-engine
  --lib regression_...`) and any seed/fixture constants involved.
- **Fix**: the change or commit that fixed it.

## 4. Verification rule

A regression test must be written against the FIXED code and must be
demonstrably red on the unfixed code path. When the unfixed path can no
longer be built (the fix is structural), the test documents the pre-fix
observable from the bug report instead, and the triage entry links the
failing pre-fix run.
