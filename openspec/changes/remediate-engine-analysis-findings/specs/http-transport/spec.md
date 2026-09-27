# Spec Delta

## Purpose

Defines origin-safe redirect behavior and consistent authorization across probing, sequential transfer, and segmented workers.

## ADDED Requirements

### Requirement: Origin-bound credential forwarding
The engine SHALL resolve redirect targets against the current request URL before comparing normalized scheme, host, and effective port. By default it SHALL NOT send caller-supplied or provider-supplied origin credentials, cookies, or origin-scoped custom credential headers to a different origin, regardless of response headers. Stripping SHALL remain in force over later hops; explicit cross-origin credential forwarding SHALL require caller opt-in. Proxy credentials SHALL only be sent to the configured proxy and SHALL never be forwarded as origin credentials. Redirect loops and HTTPS downgrade checks SHALL use resolved URLs.

#### Scenario: Absolute redirect to another origin
- **WHEN** a credential-bearing probe or GET receives a redirect to a different origin and forwarding is not enabled
- **THEN** the new origin receives no scoped credentials, even when the redirect response contains no credential headers

#### Scenario: Same-origin relative redirect
- **WHEN** a relative redirect resolves to the same scheme, host, and effective port
- **THEN** allowed credentials remain available to that origin without weakening the cross-origin default

#### Scenario: Multi-hop and explicit opt-in
- **WHEN** a request crosses origins and later redirects back, with or without the explicit cross-origin forwarding opt-in
- **THEN** the default never reintroduces stripped credentials, while a permitted opt-in applies only within its documented scope; proxy credentials never reach an origin

### Requirement: Authenticated transfer parity
Authorization configured for a download SHALL apply to its eligible probe and data requests in both sequential and segmented modes, subject to the same redirect policy; challenge-provider credentials SHALL have the same bounded challenge and scope behavior in both modes.

#### Scenario: Protected segmented resource
- **WHEN** a resource requires the request's configured bearer token on every ranged GET
- **THEN** workers authenticate just as a sequential GET would, without leaking the token on a cross-origin redirect

#### Scenario: Provider challenge after probe
- **WHEN** a segmented worker receives a supported authentication challenge
- **THEN** it uses the configured provider only for the permitted origin/scope and never retries indefinitely
