# download-destination-resolution Specification

## Purpose
Allow callers to download into an existing directory using a safe, stable filename chosen from server metadata or URLs, while retaining explicit-file compatibility and collision-safe resume and publication.

## Requirements

### Requirement: Opt-in directory request and compatible file API
The engine SHALL provide a separate directory-target request with an existing-directory argument, access to the existing request's headers/integrity/overwrite/resume settings, a configurable fallback basename (default `download`) and a configurable final-basename UTF-8 byte cap (default 250). It SHALL provide start, run, and run-with-handle entry points for that request. Existing public explicit-file request fields, constructor, controller method signatures, policies and observable event order SHALL remain source-compatible and unchanged unless callers opt into `Rename`. A path that happens to be a directory in an explicit-file request SHALL NOT implicitly activate directory mode.

#### Scenario: Existing consumer
- **WHEN** a client constructs the unchanged explicit-file request using a struct literal or calls its existing controller entry points
- **THEN** it compiles without adding fields and retains explicit-file semantics and existing event sequence.

#### Scenario: Directory consumer
- **WHEN** a client supplies an existing directory, custom headers/integrity and a valid fallback through the directory API
- **THEN** the download uses those request settings and publishes a file beneath that directory.

### Requirement: Validate directory and naming options before network
The engine SHALL reject a missing or non-directory target, an empty fallback or one that is not already an unchanged-by-sanitization single normal component, and a byte cap above 250, below the fallback's UTF-8 byte length, or below 7 when `Rename` is selected. It SHALL report determinable invalid input in the existing Configuration category before networking or creating output artifacts. The caller-selected directory SHALL NOT be replaced by any server-provided path.

#### Scenario: Invalid input
- **WHEN** a directory request supplies a nonexistent directory, a traversal fallback, or an invalid cap
- **THEN** the job fails with Configuration before making a request or creating a partial/checkpoint/final output.

#### Scenario: Valid boundary
- **WHEN** a caller selects `Rename` with a valid fallback and a 7-byte cap
- **THEN** the request passes option validation, subject to whether a unique candidate can actually be represented and leased.

### Requirement: Fixed metadata source and precedence
For directory targets the engine SHALL choose the first usable candidate in this order: the final HEAD response's valid UTF-8 `Content-Disposition` `filename*`, that response's `filename`, the final segment of the final redirected HEAD URL path if nonempty, the final segment of the original URL path if nonempty, then the validated fallback. It SHALL use the first `Content-Disposition` header and the first well-formed occurrence of each parameter name. A malformed or unresolvable candidate, including one rejected after sanitization, SHALL yield to the next source. The validating ranged GET SHALL not consume a response body or contribute a filename; intermediate redirect or later transfer headers SHALL NOT rename the job. The final choice SHALL remain immutable after destination selection.

#### Scenario: Extended header falls through
- **WHEN** `filename*` is syntactically valid but sanitizes to no name, and `filename` has a usable value
- **THEN** the ordinary `filename` wins instead of the default fallback.

#### Scenario: Redirect and transfer differ
- **WHEN** the original URL, final HEAD URL and transfer response present different names
- **THEN** the resolved name follows the final HEAD candidate precedence and does not change after transfer begins.

#### Scenario: Trailing path delimiter
- **WHEN** a URL path ends in `/` and no higher-priority source yields a name
- **THEN** that URL contributes no basename; the engine tries the next source instead of using the previous segment.

### Requirement: Robust header and URL decoding
The engine SHALL parse `filename*` only as a valid `UTF-8'language'value` with valid percent escapes and valid decoded UTF-8. Ordinary `filename` SHALL accept quoted-string (including semicolons and quoted-pair escapes) or token values; only this HEAD header MAY be decoded lossily from non-UTF-8 transport bytes. Unsupported charset, malformed parameters and invalid extended UTF-8 SHALL fall through without aborting the download. URL candidates SHALL be derived from parsed URL path segments, excluding query and fragment, percent-decoded exactly once; malformed escapes SHALL be skipped, valid escapes yielding invalid UTF-8 SHALL decode lossily, and percent-decoded `/` or `\` SHALL reject the entire URL candidate. Parser work SHALL remain bounded by HTTP header limits and SHALL not panic on hostile input.

#### Scenario: Escaped quoted value and duplicates
- **WHEN** the first HEAD disposition contains quoted semicolons/escapes or duplicate filename parameters
- **THEN** the first well-formed occurrence of each name is considered with correct quoted-string decoding.

#### Scenario: Invalid extended value and raw ordinary value
- **WHEN** `filename*` uses an unsupported charset or invalid UTF-8 and the ordinary filename has non-UTF-8 bytes
- **THEN** the extended value is skipped and the ordinary value is considered after lossy header decoding.

#### Scenario: Unsafe encoded URL basename
- **WHEN** a URL path basename contains an encoded separator or malformed percent escape
- **THEN** that candidate is skipped, without elevating a substring into a basename or interpreting query/fragment as a name.

### Requirement: Portable single-component filename safety
Each accepted directory basename SHALL be exactly one normal path component joined lexically beneath the caller's directory. For untrusted header text, the engine SHALL take the last nonempty `/` or `\` component; reject empty, `.`, `..`, or all-dots results; remove control/NUL characters; replace `< > : " | ? *` character by character; trim Windows-invalid trailing spaces/periods; and prefix reserved Windows device basenames case-insensitively, including COM¹–COM³ and LPT¹–LPT³ with or without extensions. These rules SHALL apply on all host platforms. It SHALL truncate on UTF-8 boundaries, preserve an extension when possible, then recheck all normal-component and reserved-name constraints; a rejected candidate SHALL fall through. The existing sanitizer's fallback-returning behavior SHALL remain available for existing callers. Every directory basename, including generated siblings, SHALL obey the configured cap and a hard 250-byte cap to leave room for `.part`.

#### Scenario: Traversal and device name
- **WHEN** the HEAD name contains path traversal, Windows-illegal punctuation or a device basename with a superscript numeral
- **THEN** the chosen output is a portable, sanitized single component beneath the selected directory, or the next usable source wins.

#### Scenario: Multibyte long extension
- **WHEN** a candidate with a multibyte stem and extension exceeds the cap
- **THEN** its accepted truncation is valid UTF-8, retains the extension where possible and leaves `.part` headroom.

### Requirement: Preserve explicit-file admission and permit directory post-probe selection
Existing file-target policies SHALL continue to perform conflict checks, lease acquisition and checkpoint admission before network probing; a file-target `Rename` SHALL select/lease a final candidate and begin admission before probing as well. A directory target SHALL validate directory/options before network, complete the current HEAD probe and optional metadata-only validating ranged GET, then resolve/lease the name, derive checkpoint identity from original URL plus final destination, admit/finalize against that probe, and transfer/publish without another name-resolution probe. Directory mode explicitly relaxes pre-network destination lease and checkpoint-admission guarantees. All modes SHALL use the existing authentication, redirect, retry, origin coordination, cancellation/deadline and transfer/commit behavior.

#### Scenario: Pre-network explicit-file rejection
- **WHEN** an explicit-file FailIfExists destination is occupied or an explicit-file Required resume has no checkpoint
- **THEN** it rejects before networking as before; a file `Rename` chooses its leased name before its probe.

#### Scenario: Directory probe failure or cancellation
- **WHEN** the directory job fails or is cancelled during its pre-lease probe
- **THEN** it has created no partial output or checkpoint and has not selected/admitted a destination.

#### Scenario: Directory required state absent
- **WHEN** a directory job with Required resume successfully probes but has no usable checkpoint for its resolved path
- **THEN** it fails in the Checkpoint category, even though the probe already occurred.

### Requirement: Policy-specific publication and renamed collision selection
Directory FailIfExists SHALL test its resolved final entry after probing and publish with atomic no-replace; Replace and ResumeIfMatching SHALL retain their existing publication and resume semantics at the resolved path. `Rename` SHALL never replace a published final. For file and directory Rename, it SHALL try the base and numbered siblings `stem (1).ext` through `stem (999).ext` (1,000 candidates); generated siblings SHALL reserve suffix bytes before extension-preserving truncation so different suffixes remain distinguishable. A free explicit-file base SHALL remain verbatim; if a safe unique explicit-file sibling cannot fit `.part` headroom, selection SHALL fail DestinationConflict. It SHALL treat dangling symlinks and other entries, including `.part`, as occupied, skip lease conflicts without waiting, and hold the selected lease through commit/cleanup. It SHALL select/recheck a fresh candidate under the lease without clobbering existing output; no partial, checkpoint or final output SHALL be created before leasing. On exhaustion or an atomic publication race it SHALL fail DestinationConflict rather than overwrite.

#### Scenario: Existing final and stale partial
- **WHEN** Rename sees an occupied final at the base and a stale `.part` at `stem (1).ext`
- **THEN** it selects the first available later safe sibling under a lease, never truncates the stale partial and never overwrites an occupied final.

#### Scenario: Concurrent contenders
- **WHEN** two jobs or processes request the same basename simultaneously
- **THEN** contenders skip conflicting leases and choose different names where possible; an uncooperative late writer cannot be clobbered at atomic publication.

#### Scenario: Candidate limit
- **WHEN** every representable base/sibling through suffix 999 is unavailable
- **THEN** Rename fails DestinationConflict (unless Required has no usable checkpoint, which fails Checkpoint).

### Requirement: Resume-aware Rename and destination-bound identity
When Rename has resume enabled, it SHALL scan candidate checkpointed partial siblings in numeric order before selecting a fresh free candidate, skipping occupied finals/lease conflicts and absent checkpoints. A structurally admitted checkpoint SHALL take priority over a free lower-numbered basename. Each candidate SHALL use its own original-URL-plus-final-destination identity, checkpoint resolver and admission; the selected store, lease and pending admission SHALL be retained and finalized against the probe validators without reloading. Existing corrupt-state and validator-mismatch policy SHALL apply; non-absence checkpoint failures remain Checkpoint failures. Required with no usable checkpoint SHALL fail Checkpoint even if no candidate is free. A retry with a changed server name MAY form a new identity and fresh download; default sidecars stay destination-parent scoped and custom resolvers SHALL see the chosen final path.

#### Scenario: Checkpoint beats fresh name
- **WHEN** an earlier-numbered candidate is free but a later one has a structurally admitted checkpoint
- **THEN** Rename retains the leased later candidate and resumes/finalizes its state rather than taking the free name.

#### Scenario: Resolver sees final destination
- **WHEN** a directory or renamed job selects a candidate and uses a custom checkpoint resolver
- **THEN** the resolver receives the final selected destination and a matching identity; it does not receive the unresolved directory as a file path.

#### Scenario: Metadata name changes
- **WHEN** a retry to the same directory obtains a different metadata-derived filename
- **THEN** it uses a different destination identity rather than mixing old partial bytes into the new name.

### Requirement: Observable resolved destination and existing error taxonomy
For directory and file-Rename jobs the engine SHALL publish exactly one DestinationResolved event after selecting and leasing a final choice and before meaningful transfer progress; no such event SHALL be added to old explicit-file modes. A handle SHALL offer a reliable optional immutable resolved path: known explicit-file non-Rename input MAY be available immediately; directory and Rename paths SHALL appear only after lease. The event, handle and successful terminal final path SHALL agree; committed events and completion SHALL retain their existing meaning. Broadcast lag MAY lose the event; a live handle lookup after resolution SHALL remain reliable. Invalid options use Configuration; parent/lease failures use existing Commit/PermissionDenied categories; exhausted/racing names use DestinationConflict; missing Required state uses Checkpoint. No new naming error category SHALL be introduced. URL/redaction boundaries SHALL not log raw headers or unredacted URLs; chosen paths remain intentionally visible through events, handle, terminal output and filesystem, and SHALL not be described as intrinsically non-secret.

#### Scenario: Late event subscriber
- **WHEN** an observer misses DestinationResolved because its broadcast receiver lagged
- **THEN** the handle still returns the selected path once resolved, matching the committed/terminal destination on success.

#### Scenario: Legacy event stream
- **WHEN** a non-Rename explicit-file job runs
- **THEN** no new destination event alters its previous observable event sequence.
