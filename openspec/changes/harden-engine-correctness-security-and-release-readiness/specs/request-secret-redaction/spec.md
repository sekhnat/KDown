# Spec Delta

## Purpose

Prevents request-supplied credentials and arbitrary header values from reaching formatted diagnostic output while keeping header-name-level troubleshooting useful.

## ADDED Requirements

### Requirement: Safe-by-default request diagnostics
Formatting a download request or adjacent request/configuration diagnostics with `Debug` SHALL NOT disclose any caller-supplied custom header value, regardless of header name or case. Header names MAY remain visible; dedicated authorization and proxy-authorization credential values SHALL remain redacted. Any new diagnostics or logs touched by this change SHALL obey the same policy for request headers.

#### Scenario: Common credential headers
- **WHEN** a request contains distinct secret sentinels in `Authorization`, `Proxy-Authorization`, and `Cookie` custom header values and dedicated credential fields
- **THEN** formatted debug output contains none of the exact sentinel strings and the custom header names remain identifiable

#### Scenario: Arbitrary credential header names
- **WHEN** a request contains unique secret values in `X-Api-Key` and an otherwise unrecognized custom header
- **THEN** formatted debug output contains neither exact secret string but still identifies both header names

#### Scenario: Request propagated into lower-level diagnostics
- **WHEN** caller-supplied headers are present in the transport or semantic request representations, or appear in scripted request diagnostics
- **THEN** default debug/diagnostic formatting does not expose their values, including on error paths
