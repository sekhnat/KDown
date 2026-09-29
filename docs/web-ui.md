# The local web UI

KDown ships a browser download manager that runs entirely on your machine:
a Rust host process (`kdown-app serve`) serves a bundled single-page app and
a local HTTP API on a loopback port. Nothing leaves your machine except the
downloads you start.

## Build and run

Prerequisites: Node 22 (frontend build) and a Rust toolchain.

```sh
./scripts/build_app.sh          # builds web/dist and the bundled release binary
./target/release/kdown-app serve --open
```

`--open` opens the UI in your default browser. The service listens on
`127.0.0.1:8734` by default; `--listen` accepts another loopback address and
refuses anything non-loopback. `--state-dir` overrides the state directory
(default: `$XDG_STATE_HOME/kdown`), and repeatable `--root <dir>` flags
register download roots at startup.

Without the `bundled-web` build feature, pass `--web-dir web/dist` to serve
a separately built frontend instead of the embedded one.

## First-run setup

The first launch shows a one-screen setup: confirm the suggested download
folder (your user's downloads directory) or enter another absolute path.
That folder becomes the default download root. Additional roots can be
added later in **Settings → Download folders**.

## Dashboard

Active and recently finished downloads appear as cards with live telemetry
(received bytes, wire rate, elapsed time, retries) sampled four times per
second over a server-sent events stream. The connection badge in the header
shows **Live** while the stream is healthy and **Reconnecting…** while the
UI catches up; controls disable during reconnects so commands never act on
stale state.

**New download** opens a drawer: paste the URL, optionally choose a
subfolder inside the root, a filename, the download folder, and what to do
when the destination already exists (`fail if exists`, `overwrite`,
`rename`, `resume`). Destinations that resolve outside a configured root
are rejected before anything is written.

## Lifecycle controls

- **Pause** freezes the transfer; the partial file and checkpoint stay on
  disk and Resume continues from the checkpoint.
- **Cancel** offers three artifact choices:
  - *Keep partial data* (default): keeps the `.part` file and checkpoint so
    a later retry resumes.
  - *Delete partial data*: removes the partial file and checkpoint.
  - *Keep file, discard checkpoint*: keeps the finished-looking file but
    discards resume state; a retry starts from zero.
- **Retry** is available for failed and cancelled downloads.
- **Reveal in folder** opens the download's folder in the file manager.
- **Remove from history** forgets the job's history; it never deletes the
  downloaded file.

## Recovery

If the service exits while a download was running, the next start marks
interrupted jobs as **Recovering** and resumes them from their checkpoints
automatically. Downloads you had paused stay paused.

## History and settings

**History** lists every terminal download with a stable cursor pagination,
status/source/date filters, and expandable failure detail (error code and
message) for failed attempts. **Settings** manages download folders (add,
rename, enable/disable, choose the default), transfer limits (active
downloads and the global rate limit), and desktop notifications for
completed and failed downloads.

## systemd enablement

`packaging/systemd/kdown-app.service` runs the host as a user unit:

```sh
mkdir -p ~/.local/bin && cp target/release/kdown-app ~/.local/bin/
cp packaging/systemd/kdown-app.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now kdown-app.service
journalctl --user -u kdown-app -f
```

Edit the unit to add `--root` flags for your download folders. Stopping the
unit preserves partial artifacts exactly like a Ctrl+C.

## Troubleshooting

- **The page says "service unreachable"** — the host process is not
  running, or it was started with a different `--listen` port than the page
  expects. Restart the host and reopen the printed `READY` URL.
- **"Reconnecting…" stays visible** — the SSE stream dropped. If it does
  not recover within a few seconds, reload the page; caches stay visible
  while reconnecting and no command is sent until the connection is live.
- **"That state changed elsewhere"** — another tab or window changed the
  job first. The job card refreshes automatically; reapply your action.
- **A download fails with `destination_outside_root`** — the URL's filename
  or a subfolder escaped the root (`../`). Use a plain filename or a
  subfolder that stays inside the configured folder.
- **Reveal does nothing** — desktop integration uses `xdg-open` on Linux;
  make sure `xdg-open` is installed.

## Interface notes

The UI is dark-only (the Ocean Precision palette), responsive from narrow
phones (bottom navigation, 44px touch targets) to wide desktops (side
rail), and fully keyboard operable with visible focus. Job payloads and
URLs are never logged to the browser console.
