# Security model of the local web UI

The web UI is a **local-only** application. This document states exactly
what it trusts, what it refuses, and what it never does.

## Loopback-only support

The HTTP listener accepts loopback addresses only. `kdown-app serve`
defaults to `127.0.0.1:8734` and rejects any non-loopback `--listen` with a
`listen_not_loopback` error before binding. There is no flag to listen on a
LAN or public interface, and no remote-exposure mode. Anyone who wants the
UI from another device must provide their own authenticated tunnel, which
is outside the supported configuration.

## Same-origin and CSRF controls

Every state-changing request (`POST`/`PATCH`/`PUT`/`DELETE`) must satisfy
all of:

- `Content-Type: application/json` (anything else gets `415`),
- the `x-kdown-csrf` header with the token issued in the bootstrap payload,
- a same-origin `Origin` (or none) whose authority matches the request's
  `Host`, plus a `same-origin` fetch-metadata site where the browser sends
  one.

Violations return `403`. `GET`/`HEAD`/`OPTIONS` are exempt because they are
safe methods. The CSRF token lives in the frontend's memory only — never in
storage — so a stolen `localStorage` dump does not carry it. No CORS
headers are emitted, so browsers deny cross-origin reads outright.

## Response headers

Every response carries `Content-Security-Policy` with `script-src 'self'`
(no inline or eval'd scripts), `frame-ancestors 'none'`, `base-uri 'none'`,
`form-action 'self'`, and `connect-src 'self'`; plus `X-Content-Type-Options:
nosniff`, `X-Frame-Options: DENY`, and `Referrer-Policy: no-referrer`.
Inline styles are permitted (`style-src 'self' 'unsafe-inline'`) because the
SPA sets presentation styles through React's style attribute — a styling
vector, not a script-injection vector. Hashed `/assets/*` files are served
`immutable`; `index.html` is served `no-cache` so updates land immediately.
API responses are `no-store`.

## Download root policy

Downloads are written only inside configured roots. Before each launch the
host resolves the destination (subfolder + filename) inside the root,
rejecting `..` components, absolute escapes, and symlink swaps that would
resolve outside the root (`destination_outside_root`). The final filename
is validated and length-capped. Adding a root is an explicit authorization
act performed in Settings or at first-run setup; a root cannot be disabled
or removed while non-terminal jobs reference it.

## Redaction and data exposure

- Ordinary job payloads carry display strings only: a redacted source
  (scheme + host + path, **no query string**), the root label, and a display
  destination. Signed-URL credentials, query parameters, and absolute
  filesystem paths never reach the UI for regular jobs.
- Absolute paths appear only where they are the point: root administration
  surfaces (Settings) and the reveal action.
- Error envelopes truncate failure details to a bounded length and never
  include URLs or job payloads. The frontend's error boundary never logs
  job payloads or URLs to the console.
- The API never serves filesystem listings or arbitrary paths.

## Filesystem trust boundary

The host writes only inside configured roots (download artifacts, `.part`
files, checkpoints, destination locks) and its state directory (SQLite
state). It reads remote content only over the download's HTTP(S) source and
never executes it. The reveal action invokes `xdg-open` with the download's
parent directory as a single argument — never through a shell — and the
`--open` browser launch spawns the process without shell interpolation.

The embedded web bundle is compiled into the binary; the asset router
rejects traversal paths and serves only files it embedded at build time.

## What this does NOT protect against

- A malicious local process running as the same user can do anything the
  user can, including reading the state directory and downloaded files.
- A compromised browser on the same machine could read the page (the UI
  holds no secrets beyond what the page already shows) but cannot mutate
  anything without the in-memory CSRF token and same-origin context.
- Downloads themselves are untrusted content: the engine verifies integrity
  when a checksum is available, but the UI never executes downloaded files.
