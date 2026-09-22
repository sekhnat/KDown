# Spec Delta

## Purpose

Defines the security defaults the engine enforces on its own authority: TLS validation, credential-safety across origins, local path safety, bounds against hostile servers, and hooks for embedding layers to add SSRF restrictions.

## ADDED Requirements

### Requirement: TLS validation by default
The engine SHALL enable certificate and hostname validation by default for HTTPS transfers, SHALL NOT silently fall back from HTTPS to plaintext HTTP, SHALL allow a custom CA bundle only by explicit configuration, and SHALL expose insecure-certificate bypass only behind explicit opt-in that is distinguishable in configuration and results.

#### Scenario: Invalid certificate fails by default
- **WHEN** an origin presents an invalid or mismatched TLS certificate and no bypass is configured
- **THEN** the job fails with a structured TLS error and no bytes are transferred

#### Scenario: No silent downgrade to HTTP
- **WHEN** an HTTPS URL is requested and the engine cannot establish TLS
- **THEN** the job fails with a structured TLS error rather than retrying over plaintext HTTP

#### Scenario: Custom CA bundle honored
- **WHEN** a caller configures a custom CA bundle explicitly
- **THEN** validation uses that trust set while hostname validation remains enforced

### Requirement: Credential safety across redirects and origins
The engine SHALL NOT forward authorization headers, cookies, or proxy credentials to a different origin on redirect unless the caller policy explicitly allows it, and SHALL NOT leak credentials into logs or error messages.

#### Scenario: Redirect to another host strips credentials
- **WHEN** a transfer with caller credentials follows a redirect to a different origin
- **THEN** the redirected request carries no authorization headers or cookies from the original origin unless policy explicitly permits it

### Requirement: Local path safety
The engine SHALL treat the caller-provided destination as the resolved target and SHALL NOT derive arbitrary filesystem paths from unsanitized server-supplied filenames; any optional filename-extraction utility SHALL sanitize path separators, `..` traversal, control characters (including NUL), reserved platform names, and excessive length before returning a candidate name.

#### Scenario: Server filename cannot traverse
- **WHEN** a Content-Disposition filename contains `../../etc/passwd` or path separators
- **THEN** the sanitization utility yields a safe single-component filename and the engine never writes outside the caller's destination directory

#### Scenario: Malicious metadata cannot escape destination
- **WHEN** a hostile server supplies extreme or control-character-laden metadata fields
- **THEN** parsing fails safely without panics, and no filesystem path is derived from the unsanitized value

### Requirement: Resource-exhaustion bounds
The engine SHALL bound response header size, redirect count, retry count, concurrent streams, in-memory buffers, checkpoint size, and metadata field lengths so that a malicious or broken server cannot exhaust memory, file descriptors, or request loops; exceeding a bound SHALL fail safely with a structured error.

#### Scenario: Hostile server cannot exhaust memory
- **WHEN** a server sends endless headers, an unbounded redirect loop, or oversized metadata fields
- **THEN** the corresponding bound triggers and the job fails with a structured error rather than consuming unbounded memory or looping indefinitely

### Requirement: SSRF restriction hooks
The engine SHALL expose hooks permitting an embedding layer to restrict resolved address ranges and redirect targets (for example, blocking link-local or private address ranges for untrusted URLs), without hard-coding internet-only assumptions into the engine core.

#### Scenario: Embedding layer blocks private addresses
- **WHEN** the embedding layer configures an address-range restriction and a redirect resolves to a blocked range
- **THEN** the engine aborts the request with a structured error before connecting