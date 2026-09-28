#!/usr/bin/env bash
# Download a URL with the kdown-engine download_link example.
# Shows live transfer speed (decimal MB/s) and estimated time remaining.
#
# Usage: download.sh [OPTIONS] URL [DIRECTORY]
#   URL         http(s) link to download (required)
#   DIRECTORY   existing destination directory (default: current directory)
#
# Options (forwarded to the engine):
#   -s, --segments N      parallel range workers (1+); also lowers the
#                         segmentation threshold so N-way splitting applies
#                         (server range support still required)
#   -S, --segment-size N  initial range size, e.g. 4M (K/M/G decimal suffixes)
#   -r, --rate N          per-job rate limit in bytes/s, e.g. 10M
#       --retries N       max attempts per segment (engine default 8)
#       --resume MODE     allowed | never | required (default allowed)
#       --overwrite MODE  rename | fail | replace (default rename)
#   -c, --connections N   parallel HTTP/2 connection slots per origin
#                         (default 8; each adds a bounded 2 MiB flow-control
#                         window — raise for high-bandwidth × high-RTT paths)
#   -h, --help            show this help
#
# The output filename is resolved automatically and an existing file is
# never replaced unless --overwrite says otherwise.
set -euo pipefail

usage() {
  # Print the leading comment header (lines 2..first non-comment).
  awk 'NR == 1 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "${BASH_SOURCE[0]}"
}

case "${1:-}" in
  -h | --help)
    usage
    exit 0
    ;;
esac

if (($# < 1)); then
  usage >&2
  echo "download.sh: expected at least a URL" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
MANIFEST="$ROOT/Cargo.toml"

if [[ ! -f "$MANIFEST" ]]; then
  echo "download.sh: cannot find Cargo.toml at $MANIFEST" >&2
  exit 2
fi
if ! command -v cargo >/dev/null 2>&1; then
  echo "download.sh: cargo is not available in PATH" >&2
  exit 2
fi

# The example runs in the caller's working directory, so a relative
# DIRECTORY argument stays relative to the caller. All options are parsed
# and validated by the example itself.
exec cargo run --release --locked \
  --manifest-path "$MANIFEST" \
  -p kdown-engine \
  --example download_link \
  -- "$@"
