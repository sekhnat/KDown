#!/usr/bin/env bash
# API surface drift check (consumer-api task 2.4).
#
# Compares the crate's public module inventory (pub mod / pub use in
# src/lib.rs) against the whitelist in docs/api-surface.md.
# - An unlisted public module fails the check.
# - A whitelisted root re-export that is missing from lib.rs fails the check.
# - The implementation modules (scheduler, job, io, resume, observability,
#   fuzz_targets without fuzz-entry) must NOT be public.
#
# Usage: scripts/api_surface_check.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LIB="$ROOT/crates/engine/src/lib.rs"
WHITELIST="$ROOT/docs/api-surface.md"

fail=0

# --- Implementation modules must NOT be public -----------------------------
for modname in scheduler job io resume observability; do
  if grep -Eq "^pub mod ${modname};" "$LIB"; then
    echo "FAIL: implementation module '${modname}' is public in src/lib.rs"
    fail=1
  fi
done

# fuzz_targets must be feature-gated, not unconditionally public:
# every `pub mod fuzz_targets;` must be preceded (within 3 lines) by
# `#[cfg(feature = "fuzz-entry")]`.
if grep -Eq '^pub mod fuzz_targets;' "$LIB"; then
  line_no=$(grep -nE '^pub mod fuzz_targets;' "$LIB" | head -1 | cut -d: -f1)
  context_start=$((line_no > 3 ? line_no - 3 : 1))
  if ! sed -n "${context_start},${line_no}p" "$LIB" | grep -q 'cfg(feature = "fuzz-entry")'; then
    echo "FAIL: fuzz_targets is unconditionally public in src/lib.rs"
    fail=1
  fi
fi

# --- Whitelisted public modules must exist ---------------------------------
for modname in config control error http metrics redact; do
  if ! grep -Eq "^pub mod ${modname};" "$LIB"; then
    echo "FAIL: whitelisted module '${modname}' missing from src/lib.rs"
    fail=1
  fi
done

# --- Whitelisted root re-exports must exist ---------------------------------
# Extract the full text of every `pub use … ;` statement (possibly
# multi-line, semicolon-terminated) and search within it for the type name.
python3 - "$LIB" <<'PYEOF'
import re, sys

lib = open(sys.argv[1]).read()
# Extract every `pub use … ;` statement (possibly multi-line).
blocks = re.findall(r'pub use\b.*?;', lib, re.S)
text = '\n'.join(blocks)

TYPES = [
    "DownloadRequest", "DownloadController", "DownloadHandle", "CancelMode",
    "SingleStreamController",
    "CompletedDownload", "DownloadRunError", "TransferFailure", "EngineFailure",
    "CancellationSummary", "TransferAccounting", "ArtifactDisposition",
    "DownloadError", "ErrorCategory", "Retryability", "FailureDomain",
    "EngineConfig", "DurabilityMode", "ExpectedHash", "H2ConnectionPolicy",
    "HashAlgorithm", "IntegrityPolicy", "NetworkPolicy", "OverwritePolicy",
    "PoolConfig", "ProxyConfig", "ResumePolicy", "TlsConfig", "TransferPolicy",
    "HttpTransport", "EngineMetrics", "Event", "EventHub", "EventStream",
    "MetricsSnapshot", "ProgressSnapshot", "JobState", "Redactor",
    "CheckpointStore", "CheckpointStoreResolver", "CheckpointResolveContext",
    "FileCheckpointStore", "SidecarCheckpointResolver", "Checkpoint",
    "ByteRange", "CheckpointError", "StoreDurabilityMode",
]
missing = [t for t in TYPES if not re.search(r'\b' + re.escape(t) + r'\b', text)]
for t in missing:
    print(f"FAIL: whitelisted re-export '{t}' missing from src/lib.rs")
sys.exit(1 if missing else 0)
PYEOF
fail=$((fail + $?))

# --- Unlisted public modules fail the check ---------------------------------
# Every `pub mod` in lib.rs must be one of the whitelisted public modules.
while read -r modname; do
  [ -z "$modname" ] && continue
  case "$modname" in
    config|control|error|http|metrics|redact|fuzz_targets) ;;  # whitelisted
    *) echo "FAIL: unlisted public module '${modname}' in src/lib.rs"; fail=1 ;;
  esac
done < <(grep -oE '^pub mod [a-z_]+;' "$LIB" | sed 's/^pub mod //; s/;$//')

if [ "$fail" -eq 0 ]; then
  echo "API surface check PASSED"
fi
exit "$fail"
