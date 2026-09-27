# Spec Delta

## Purpose

Prevents credentials embedded in download URLs from leaking through ordinary diagnostics or lower-trust checkpoint storage.

## ADDED Requirements

### Requirement: Secret-safe URL diagnostics by default
Debug, Display, errors, events, and engine-authored logs SHALL NOT render URL userinfo or values of known credential-bearing query keys in plaintext by default; caller-marked additional keys SHALL also be redacted. Legitimate request URLs SHALL remain intact for transport and resume identity. No default diagnostic formatting path SHALL emit arbitrary URL query values if their secrecy cannot be determined safely.

#### Scenario: URL with userinfo and token
- **WHEN** a request uses a URL with `user:password@` and `?token=secret`
- **THEN** request and transport debug/error formatting contains neither secret value while the request still reaches the correct URL

#### Scenario: Custom signed parameter
- **WHEN** the caller marks a nonstandard query key sensitive
- **THEN** engine diagnostic output masks that key's value

### Requirement: Protected checkpoint URL storage
Default checkpoint persistence SHALL NOT expose raw URL credentials to other directory users through engine-created sidecar files, temporary files, retained old versions, or error text. If raw URL identity is retained for compatibility, the engine SHALL restrict its storage to the owning user; where it cannot ensure secure persistence, it SHALL fail or require a documented trusted store. Changing the checkpoint representation SHALL preserve safe admission and migration behavior.

#### Scenario: Signed URL and shared destination directory
- **WHEN** a checkpoint is stored for a signed URL in a directory readable by another user
- **THEN** raw query credentials are not readable from engine-created sidecars or their temp files by that user, or checkpoint creation fails safely

#### Scenario: Custom checkpoint store
- **WHEN** a caller supplies its own store
- **THEN** the storage trust boundary and responsibility for protecting raw URL identity are explicitly documented
