# API Compatibility Policy — kdown-engine

## Supported surface

The supported surface is exactly the whitelist in `docs/api-surface.md`. Items
not on that list may change or disappear at any time without notice. Items on
the list follow the policy below.

## Semver policy

From this cleanup forward, the crate follows SemVer:

- **MAJOR**: removing or renaming a supported type, method, enum variant, or
  field; changing a supported signature; removing a supported injection seam.
- **MINOR**: adding supported types, methods, enum variants, or fields;
  non-breaking additions.
- **PATCH**: bug fixes, performance work, documentation.

### Exhaustiveness

The expansion-prone public enums are `#[non_exhaustive]`: `DownloadRunError`,
`ErrorCategory`, `FailureDomain`, `Retryability` and `Event`. Callers must
handle an unknown-variant arm for these, so adding a variant is a MINOR change.

`CancelMode` and `JobState` are exhaustive as shipped: adding a variant to
either is a MAJOR (breaking) change unless it first gains `#[non_exhaustive]`
in a reviewed change. This gap is recorded here deliberately rather than
silently implied.

## MSRV policy

Workspace `rust-version` is **1.85**. A supported build must compile on the
MSRV toolchain; an MSRV bump is a breaking (MAJOR) change.

## Signature drift detection

- **Positive check:** `crates/engine/tests/external_consumer_fixture.rs`
  compiles and runs using only supported imports. Any change to a supported
  type or signature visible in the whitelist fails the fixture build and must
  be reviewed under the compatibility policy before release.
- **Negative check:** `crates/engine/tests/retired_seam_negative.rs` (trybuild)
  asserts that retired seams (`http::scripted`, `http::HttpExecution`,
  `job::controller::DownloadResult`) cannot be imported from outside the
  crate; a retired seam becoming importable again fails this test.
- **Inventory drift:** `scripts/api_surface_check.sh` compares the crate's
  public module inventory (`pub mod` declarations in `src/lib.rs`), its root
  re-exports and the implementation-module exclusions against the whitelist in
  `docs/api-surface.md`; an unlisted public module or a missing whitelisted
  re-export fails the check. Public paths *inside* those modules
  (`control::auth`, `http::transport`, `metrics::events`, ...) are covered by
  the external-consumer fixture above.

## Release rule

A release that changes a supported type, method, variant, field, or signature
must be reviewed against this policy and, if incompatible, must ship as an
explicitly breaking (MAJOR) release with updated migration guidance before
release.
