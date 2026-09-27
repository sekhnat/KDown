# Retained libFuzzer seed corpus

These small committed inputs are starting points for the corresponding parser
fuzz targets. Scheduled/release runs evolve a working corpus and retain it with
any crash/reproducer files as a GitHub Actions artifact (90-day retention).

A discovered failure must be minimized, copied into a named regression test,
and include the artifact's seed and replay command per
`docs/regression-triage.md`; CI artifacts are evidence, not a substitute for
a checked-in regression test.

Targets: `fuzz_url`, `fuzz_content_range`, `fuzz_etag`,
`fuzz_content_disposition`, and `fuzz_checkpoint`.
