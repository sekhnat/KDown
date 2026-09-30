#!/usr/bin/env bash
# Category verification lanes (production-stability task 5.4).
#
# Each lane runs the commands that verify one evidence category and records a
# release-evidence fragment naming exactly what ran. CI runs the OS-specific
# correctness/durability lanes inside the three-OS matrix; the resource-bound
# and interoperability lanes run on Linux. The same script runs locally, so a
# local run and the CI run produce comparable evidence.
#
# A lane that fails records `failed` (never `passed`) and exits non-zero: the
# release gate treats a failed gate as blocking and never waivable.
#
# Usage: scripts/ci_lane.sh {correctness|durability|resource-bound|interoperability|all}
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/artifacts/evidence}"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 || command -v python)}"
mkdir -p "$OUT_DIR"

LANE="${1:-all}"

host_os() {
  if [[ -n "${RUNNER_OS:-}" ]]; then
    printf '%s' "$RUNNER_OS" | tr '[:upper:]' '[:lower:]'
    return
  fi
  case "$(uname -s)" in
    Darwin) printf 'macos' ;;
    Linux) printf 'linux' ;;
    MINGW*|MSYS*|CYGWIN*) printf 'windows' ;;
    *) printf 'unknown' ;;
  esac
}

toolchain() {
  printf 'rustc %s; cargo %s' \
    "$(rustc --version 2>/dev/null | awk '{print $2}')" \
    "$(cargo --version 2>/dev/null | awk '{print $2}')"
}

git_commit() {
  if [[ -n "${GITHUB_SHA:-}" ]]; then printf '%s' "$GITHUB_SHA"; else git -C "$ROOT" rev-parse HEAD 2>/dev/null || printf 'unknown'; fi
}

run_lane() {
  # run_lane <id> <category> <max-age-days> <source> <detail> <command>...
  local id="$1" category="$2" max_age="$3" source="$4" detail="$5"
  shift 5
  local log="$OUT_DIR/$id.log"
  : > "$log"
  local failed=0
  for cmd in "$@"; do
    printf '=== %s\n' "$cmd" >> "$log"
    local start rc=0
    start=$(date +%s)
    eval "$cmd" >> "$log" 2>&1 || rc=$?
    printf -- '--- exit=%s elapsed=%ss\n' "$rc" "$(( $(date +%s) - start ))" >> "$log"
    [ "$rc" -eq 0 ] || failed=1
  done

  local status=passed
  local extra=()
  if [ "$failed" -ne 0 ]; then
    status=failed
    detail="$detail (at least one command failed; see $log)"
  else
    extra=(--approves-release)
  fi

  local cmd_args=()
  for cmd in "$@"; do
    cmd_args+=(--command "$cmd")
  done

  "$PYTHON_BIN" "$ROOT/scripts/evidence_io.py" record \
    --out "$OUT_DIR/$id.json" \
    --id "$id" \
    --category "$category" \
    --status "$status" \
    --source "$source" \
    --detail "$detail" \
    --toolchain "$(toolchain)" \
    --commit "$(git_commit)" \
    --artifact "$log" \
    --max-age-days "$max_age" \
    "${cmd_args[@]}" \
    ${extra[@]+"${extra[@]}"}
  return "$failed"
}

lane_correctness() {
  local os_lc
  os_lc="$(host_os)"
  run_lane "correctness_workspace_tests_${os_lc}" correctness 30 "ci-lane:correctness" \
    "Locked workspace build, full test matrix and clippy on ${os_lc}, plus the targeted P1 control/fault and secret-diagnostic regressions." \
    "cargo build --workspace --all-targets --locked" \
    "cargo test --workspace --all-targets --locked" \
    "cargo clippy --workspace --all-targets --locked -- -D warnings" \
    "cargo test --locked -p kdown-engine --test handle_control_tests" \
    "cargo test --locked -p kdown-engine --test metrics_tests" \
    "cargo test --locked -p kdown-engine --test redaction_audit_tests" \
    "cargo test --locked -p kdown-engine --lib resume::checkpoint"
}

lane_durability() {
  local os_lc
  os_lc="$(host_os)"
  run_lane "durability_platform_matrix_${os_lc}" durability 30 "ci-lane:durability" \
    "Platform filesystem/lease/publication matrix, checkpoint crash/restart recovery, byte-exact publication and durable save ordering on ${os_lc}." \
    "cargo test --locked -p kdown-engine --test platform_fs_tests" \
    "cargo test --locked -p kdown-engine --test publication_tests" \
    "cargo test --locked -p kdown-engine --lib internal_tests::crash_restart_tests -- --test-threads=1" \
    "cargo test --locked -p kdown-engine --lib internal_tests::sink_fault_tests -- --test-threads=1" \
    "cargo test --locked -p kdown-engine --lib internal_tests::resume_tests -- --test-threads=1" \
    "cargo test --locked -p kdown-engine --lib internal_tests::directory_resolution_tests -- --test-threads=1" \
    "cargo test --locked -p kdown-engine --lib internal_tests::checkpoint_seam_tests -- --test-threads=1"
}

lane_resource_bound() {
  run_lane "resource_bound_adversarial" resource_bound 30 "ci-lane:resource-bound" \
    "Transfer-memory admission, HTTP ingress bounds and budget/high-water telemetry under adversarial load." \
    "cargo test --locked -p kdown-engine --lib internal_tests::transfer_memory_tests -- --test-threads=1" \
    "cargo test --locked -p kdown-engine --lib internal_tests::ingress_bound_tests -- --test-threads=1" \
    "cargo test --locked -p kdown-engine --lib internal_tests::metrics_transfer_tests -- --test-threads=1"
}

lane_interoperability() {
  run_lane "interoperability_http_matrix" interoperability 30 "ci-lane:interoperability" \
    "Range/validator/CDN/proxy/network-condition interoperability and wire-amplification suite on H1 and H2." \
    "cargo test --locked -p kdown-engine --test http_edge_cases_tests" \
    "cargo test --locked -p kdown-engine --test network_conditions_tests" \
    "cargo test --locked -p kdown-engine --test proxy_tests" \
    "cargo test --locked -p kdown-engine --test h2_tests" \
    "cargo test --locked -p kdown-engine --test wire_amplification_tests"
}

status=0
case "$LANE" in
  correctness) lane_correctness || status=1 ;;
  durability) lane_durability || status=1 ;;
  resource-bound) lane_resource_bound || status=1 ;;
  interoperability) lane_interoperability || status=1 ;;
  all)
    lane_correctness || status=1
    lane_durability || status=1
    lane_resource_bound || status=1
    lane_interoperability || status=1
    ;;
  *) echo "usage: $0 {correctness|durability|resource-bound|interoperability|all}" >&2; exit 2 ;;
esac
exit "$status"
