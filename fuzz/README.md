# KDown parser fuzzing

LibFuzzer targets:

- `fuzz_url`
- `fuzz_content_range`
- `fuzz_etag`
- `fuzz_content_disposition`
- `fuzz_checkpoint`

Each target has a small checked-in starting corpus under
`fuzz/corpus/<target>/`; see `fuzz/corpus/README.md`. With a nightly
Rust toolchain and `cargo-fuzz` installed, run a bounded, replayable target
from the repository root:

```sh
cargo fuzz run fuzz_url -- -seed=2026092701 -max_total_time=60 -timeout=10 -max_len=65536
```

Replace `fuzz_url` with any target above. To replay a crash artifact, use the
seed and command recorded alongside the scheduled/release workflow artifact:

```sh
cargo fuzz run fuzz_url path/to/crash-artifact -- -runs=1 -seed=2026092701
```

`.github/workflows/fuzz-stress.yml` runs each target separately every week and
on version tags. It uses 120 seconds per target on scheduled/manual runs and
600 seconds on release tags, with an overall 30-minute job timeout. A fuzzer
failure uploads the target corpus, crash artifacts, seed, and log before an
explicit failing step marks that matrix job red. Artifacts are retained for 90
days. The same workflow runs repeated scheduler, checkpoint, atomic-publication,
crash/restart, and high-concurrency sink-fault suites with recorded Proptest
seeds and uploads replay logs/regression seeds.

The stable PR fallback exercises the same parser entry points over a fixed
adversarial corpus without requiring nightly or `cargo-fuzz`:

```sh
cargo test --locked -p kdown-engine --lib fuzz_targets::smoke::corpus_smoke_no_panics -- --exact
```

It is deterministic and fail-closed: malformed input must return an error or
sanitized value, never panic or produce traversal components.
