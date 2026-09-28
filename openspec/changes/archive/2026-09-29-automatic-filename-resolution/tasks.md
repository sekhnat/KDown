# Tasks

## 1. Public opt-in API and guards

- [x] 1.1 Add `DirectoryDownloadRequest` with `new(url, directory)`, `request_mut()`, fallback and byte-cap builders without altering `DownloadRequest`/`EngineConfig`; verify external-consumer fixture still compiles unchanged request literals and compiles new request construction.
- [x] 1.2 Validate directory existence/type and fallback/cap invariants (default `download`/250 bytes, Rename minimum 7) before networking; verify parameterized invalid-directory/fallback/cap tests assert Configuration and zero network/output artifacts.
- [x] 1.3 Add `OverwritePolicy::Rename` on the existing non-exhaustive enum, public root export of the new request, and `start_to_directory`, `run_to_directory`, `run_to_directory_with_handle` delegating to a shared runner; verify signature proofs in `external_consumer_fixture.rs` and `cargo test -p kdown-engine --test external_consumer_fixture --locked`.

## 2. Filename metadata and portable safety

- [x] 2.1 Replace simplistic Content-Disposition extraction in `http/probe.rs` with bounded ordered `filename*`/`filename` parsing (quoted semicolons/escapes, duplicates, UTF-8 ext-values, fallback after sanitization) while preserving existing hint tests; verify unit cases for malformed, unsupported, long and duplicate parameters/headers.
- [x] 2.2 Decode only HEAD Content-Disposition values lossily in `http/transport.rs`, leaving other header handling unchanged; verify a transport test with raw non-UTF-8 ordinary filename and malformed extended form resolves the plain hint without header logging.
- [x] 2.3 Extend `io/sanitize.rs` with a fallible byte-limited sanitizer, portable punctuation/device-name rules, post-truncation validation and unchanged fallback-returning wrapper; verify sanitizer unit/property tests for traversal, superscript device names, invalid input, multibyte truncation, extension and single-component containment.
- [x] 2.4 Resolve directory candidates from final HEAD hints, final HEAD URL, original URL and validated fallback using parsed URI path/one percent decode; verify resolution tests for HEAD-vs-GET redirects, trailing slash, query/fragment, malformed escapes, invalid UTF-8 and encoded separator fallthrough.

## 3. Ownership and checkpoint-aware Rename

- [x] 3.1 Build a shared base/` (1)`…` (999)` candidate generator with reserved suffix bytes and `.part` headroom for directory names and explicit-file Rename siblings; verify tests for 250-byte and custom caps, multibyte suffix/extension truncation, verbatim free explicit-file base and unrepresentable sibling conflicts.
- [x] 3.2 Add a `resume::flow` discovery admission result that distinguishes absent checkpoints from existing Required-state failure without parsing error strings or changing corrupt-state handling; verify unit tests for absent/valid/corrupt checkpoints with Allowed and Required.
- [x] 3.3 Implement resume-first Rename candidate search: skip occupied finals/conflicting leases, derive candidate-specific identity/resolver, retain one leased selected store and pending admission without a second load; verify tests for checkpoint-first priority, validator mismatch, custom resolver path, Required absence and metadata-name changes.
- [x] 3.4 Implement fresh Rename selection with `symlink_metadata`, pre-/post-lease final and `.part` checks, bounded exhaustion, nonblocking conflict skip and atomic `NoReplace`; verify filesystem tests for stale partial, dangling symlink, 1,000 candidates, non-cooperating writer race and no overwrite.
- [x] 3.5 Exercise process-local and cross-process lease contention and two concurrent identical directory requests; verify selected paths differ where possible, `.part`/checkpoint/final output is never created pre-lease and selected lease survives commit/cleanup.

## 4. Pipeline ordering, outcomes and observation

- [x] 4.1 Refactor controller preparation to share the probe/transfer/commit path while retaining explicit-file pre-probe lease/admission and doing directory validation → one probe → resolution/lease/admission; verify existing pre-network FailIfExists/Required tests plus directory single-probe, optional metadata-only ranged GET and cancellation/deadline tests.
- [x] 4.2 Pass final destination into job identity, checkpoint resolver, sink/cleanup and publication; preserve Replace/ResumeIfMatching mapping, enforce directory FailIfExists after probe, use NoReplace for Rename; verify integration tests for each policy, atomically published bytes, custom resolver destination and correct Configuration/Commit/PermissionDenied/DestinationConflict/Checkpoint categories.
- [x] 4.3 Add `DownloadHandle::resolved_destination()` via shared OnceLock and emit one DestinationResolved only for directory/Rename after leasing and before progress; verify event ordering, legacy file event parity, lagged-handle lookup and matching committed/terminal paths in `handle_control_tests.rs` and integration tests.

## 5. Compatibility, security and verification

- [x] 5.1 Extend the external-consumer fixture with the new APIs while retaining the existing literal and method-reference proofs; update `docs/api-surface.md`, `docs/api-compatibility.md`, `CHANGELOG.md` and `scripts/api_surface_check.sh`; verify `scripts/api_surface_check.sh` and the fixture test pass.
- [x] 5.2 Extend `crates/engine/src/fuzz_targets.rs::fuzz_content_disposition` and its corpus smoke test with URL resolution/path containment, plus redaction assertions for raw headers/URLs while documenting paths as potentially sensitive; verify targeted fuzz smoke/redaction tests pass.
- [x] 5.3 Ensure platform filesystem tests cover portable filename publication and Rename on macOS/Windows through the existing `.github/workflows/ci.yml` matrix and `scripts/ci_lane.sh` durability lane; verify CI test filters actually include the new cases and obtain green matrix results.
- [x] 5.4 Run `cargo fmt --all -- --check`, `scripts/api_surface_check.sh`, `cargo test --workspace --all-targets --locked`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, targeted filesystem/resume/event suites, `scripts/ci_lane.sh all` (records evidence), and `openspec validate automatic-filename-resolution --strict`; verify each completes successfully and review the generated evidence.
