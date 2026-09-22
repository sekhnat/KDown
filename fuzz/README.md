# KDown parser fuzzing

Targets:

- `fuzz_url`
- `fuzz_content_range`
- `fuzz_etag`
- `fuzz_content_disposition`
- `fuzz_checkpoint`

With a nightly toolchain and `cargo-fuzz` installed:

```sh
cargo fuzz run fuzz_url -- -max_total_time=30
cargo fuzz run fuzz_content_range -- -max_total_time=30
cargo fuzz run fuzz_etag -- -max_total_time=30
cargo fuzz run fuzz_content_disposition -- -max_total_time=30
cargo fuzz run fuzz_checkpoint -- -max_total_time=30
```

The current workstation uses the distribution stable toolchain without
`rustup`/nightly, so the stable fallback runs the same entry points over an
adversarial corpus:

```sh
cargo test -p kdown-engine fuzz_targets::smoke::corpus_smoke_no_panics
```

The fallback is deterministic and fail-closed: malformed input must return
an error or a sanitized value, never panic or produce traversal components.
