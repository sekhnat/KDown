#!/usr/bin/env bash
# Benchmark driver: PR smoke, same-host legacy check, and the release-gate
# profile suite (tasks 6.1-6.4).
#
# Modes:
#   --smoke                  PR scenario smoke on shared CI hardware: runs the
#                            criterion scenarios with a tight budget, records a
#                            non-approving `performance_pr_smoke` fragment, and
#                            guards the gate semantics. Never approves a release.
#   check (default)          Same-host comparison of the criterion scenarios
#                            against results/baseline.md (historical loopback axis).
#   --release <profile|all>  Run the pinned release suite (loopback | low-latency |
#                            wan | all) with >=5 repetitions and gate every axis
#                            against the versioned matched-host baseline, then
#                            record release-approving evidence for the profile(s).
#   --baseline <profile>     Run the suite and (re)record the reviewed baseline.
#   --no-run                 Gate an existing --suite-out report without running.
#   --self-test              Gate-semantics self-test, no benchmarks run.
#
# Options: --suite-out DIR, --dest-dir DIR, --disk-label NAME, --reps N,
#          --reviewer NAME.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 || command -v python)}"
EVIDENCE_DIR="${EVIDENCE_DIR:-$ROOT/artifacts/evidence}"
SUITE_ROOT="${SUITE_ROOT:-$ROOT/crates/engine/benches/results/suite}"

MODE="check"
PROFILE=""
SUITE_OUT=""
DEST_DIR=""
DISK_LABEL=""
REPS=""
REVIEWER=""
NO_RUN=0

usage() {
  # Print the header comment block (everything before the first command line).
  awk 'NR == 1 { next } /^set / { exit } NR > 1 { sub(/^# ?/, ""); print }' "$0" >&2
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --smoke) MODE="smoke"; shift ;;
    check) MODE="check"; shift ;;
    --release) MODE="release"; PROFILE="${2:-}"; shift 2 ;;
    --baseline) MODE="baseline"; PROFILE="${2:-}"; shift 2 ;;
    --no-run) NO_RUN=1; shift ;;
    --self-test) MODE="self-test"; shift ;;
    --suite-out) SUITE_OUT="${2:-}"; shift 2 ;;
    --dest-dir) DEST_DIR="${2:-}"; shift 2 ;;
    --disk-label) DISK_LABEL="${2:-}"; shift 2 ;;
    --reps) REPS="${2:-}"; shift 2 ;;
    --reviewer) REVIEWER="${2:-}"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

record_evidence() {
  # record_evidence <id> <status> <detail> [extra evidence_io args...]
  # A passing gate approves release unless the call passes --advisory (PR
  # smoke must never approve a release).
  local id="$1" status="$2" detail="$3"
  shift 3
  mkdir -p "$EVIDENCE_DIR"
  local extra=()
  if [ "$status" = "passed" ]; then
    case " $* " in
      *" --advisory "*) ;;
      *) extra=(--approves-release) ;;
    esac
  fi
  "$PYTHON_BIN" "$ROOT/scripts/evidence_io.py" record \
    --out "$EVIDENCE_DIR/$id.json" \
    --id "$id" \
    --category performance \
    --status "$status" \
    --source "scripts/bench_check.sh:$MODE" \
    --detail "$detail" \
    --toolchain "$(rustc --version 2>/dev/null || echo unknown)" \
    --max-age-days 45 \
    "$@" \
    "${extra[@]}"
}

run_suite_for() {
  # run_suite_for <profile> [record|gate-only] [out_dir]
  local profile="$1" action="${2:-record}" out_override="${3:-}"
  local out="${out_override:-${SUITE_OUT:-$SUITE_ROOT/$profile}}"
  local evidence_id="performance_${profile//-/_}"
  local -a cmd=(cargo bench --locked -p kdown-engine --bench throughput -- --suite "$profile" --suite-out "$out")
  [ -n "$REPS" ] && cmd+=(--reps "$REPS")
  [ -n "$DEST_DIR" ] && cmd+=(--dest-dir "$DEST_DIR")
  [ -n "$DISK_LABEL" ] && cmd+=(--disk-label "$DISK_LABEL")
  local run_command
  run_command="${cmd[*]}"

  if [ "$NO_RUN" -eq 0 ]; then
    echo "== running release suite profile '$profile' (repetitions >= 5) =="
    if ! "${cmd[@]}"; then
      # A run that cannot complete is a failed gate, never a silent skip.
      record_evidence "$evidence_id" failed \
        "The pinned '$profile' profile suite did not complete; release is blocked." \
        --command "$run_command"
      return 1
    fi
  else
    echo "== using existing report at $out (--no-run) =="
  fi

  local gate_status=0
  local gate_json
  gate_json=$("$PYTHON_BIN" "$ROOT/scripts/bench_gate.py" check \
    --metrics "$out/metrics.json" --profile "$profile" --json) || gate_status=1

  if ! "$PYTHON_BIN" - "$gate_json" <<'PYEOF'
import json
import sys
result = json.loads(sys.argv[1])
print(result["verdict"])
for failure in result["failures"]:
    print("  [{}] {}: {}".format(failure["axis"], failure["scenario"], failure["detail"]))
PYEOF
  then
    echo "failed to summarize the gate result" >&2
    return 1
  fi
  printf '%s\n' "$gate_json" > "$out/gate.json"

  if [ "$action" = "gate-only" ]; then
    return "$gate_status"
  fi

  local gate_command
  gate_command="python3 scripts/bench_gate.py check --metrics $out/metrics.json --profile $profile"
  if [ "$gate_status" -eq 0 ]; then
    record_evidence "$evidence_id" passed \
      "Pinned '$profile' profile suite met every per-axis threshold against the matched-host baseline." \
      --command "$run_command" --command "$gate_command" \
      --artifact "$out/metrics.json" --artifact "$out/gate.json"
  else
    local env_only limitation
    env_only=$(printf '%s' "$gate_json" | "$PYTHON_BIN" -c 'import json,sys; r=json.load(sys.stdin); print("1" if r["affected_axes"] == ["environment"] else "0")')
    limitation=$(printf '%s' "$gate_json" | "$PYTHON_BIN" -c 'import json,sys; r=json.load(sys.stdin); print(r["failures"][0]["detail"] if r["failures"] else "no baseline comparison available")')
    if [ "$env_only" = "1" ]; then
      # No matched baseline for this host: the report is retained, but a
      # production-stable verdict needs a matched-host run or a reviewed
      # exception (design D7 forbids comparing unrelated runners).
      record_evidence "$evidence_id" unavailable "$limitation" \
        --command "$run_command" --command "$gate_command" \
        --limitation "$limitation" \
        --artifact "$out/metrics.json" --artifact "$out/gate.json"
    else
      record_evidence "$evidence_id" failed \
        "Pinned '$profile' profile suite breached at least one per-axis threshold; release is blocked." \
        --command "$run_command" --command "$gate_command" \
        --artifact "$out/metrics.json" --artifact "$out/gate.json"
    fi
  fi
  return "$gate_status"
}

case "$MODE" in
  self-test)
    "$PYTHON_BIN" "$ROOT/scripts/bench_gate.py" self-test
    ;;

  smoke)
    # GitHub-hosted hardware/load differs from any recorded baseline, so this
    # is a scenario-integrity smoke: it proves the benchmark still runs and that
    # the gate semantics hold, and it explicitly cannot approve a release.
    out=$(cargo bench --bench throughput -- --warm-up-time 0.5 --measurement-time 1.5 2>&1)
    echo "$out"
    suite_smoke=$(cargo bench --locked -p kdown-engine --bench throughput -- --suite loopback --reps 5 --dataset 4MiB --suite-scenario "h1/workers_1/jobs_1" --suite-out "${SUITE_OUT:-$SUITE_ROOT/pr-smoke}" 2>&1)
    echo "$suite_smoke"
    "$PYTHON_BIN" "$ROOT/scripts/bench_gate.py" self-test
    record_evidence performance_pr_smoke passed \
      "PR benchmark smoke: criterion scenarios ran, the pinned suite executed one loopback scenario with all axes, and the per-axis gate self-test passed. Shared CI hardware is not comparable to any baseline, so this evidence cannot approve a release." \
      --command "cargo bench --bench throughput -- --warm-up-time 0.5 --measurement-time 1.5" \
      --command "cargo bench --locked -p kdown-engine --bench throughput -- --suite loopback --reps 5 --dataset 4MiB --suite-scenario h1/workers_1/jobs_1" \
      --advisory \
      --artifact "${SUITE_ROOT}/pr-smoke/metrics.json"
    echo "Smoke-only: release comparisons require a matched-host run (scripts/bench_check.sh --release <profile>)."
    ;;

  release)
    [ -n "$PROFILE" ] || usage
    status=0
    if [ "$PROFILE" = "all" ]; then
      for profile in loopback low-latency wan; do
        # Keep per-profile reports separate when the caller pinned --suite-out.
        run_suite_for "$profile" record "${SUITE_OUT:+$SUITE_OUT/$profile}" || status=1
      done
    else
      run_suite_for "$PROFILE" record || status=1
    fi
    exit "$status"
    ;;

  baseline)
    [ -n "$PROFILE" ] || usage
    [ "$PROFILE" = "all" ] && { echo "--baseline takes one profile, not all" >&2; exit 2; }
    out="${SUITE_OUT:-$SUITE_ROOT/$PROFILE}"
    cmd=(cargo bench --locked -p kdown-engine --bench throughput -- --suite "$PROFILE" --suite-out "$out")
    [ -n "$REPS" ] && cmd+=(--reps "$REPS")
    [ -n "$DEST_DIR" ] && cmd+=(--dest-dir "$DEST_DIR")
    [ -n "$DISK_LABEL" ] && cmd+=(--disk-label "$DISK_LABEL")
    echo "== recording a reviewed baseline for '$PROFILE' =="
    "${cmd[@]}"
    "$PYTHON_BIN" "$ROOT/scripts/bench_gate.py" record \
      --metrics "$out/metrics.json" --profile "$PROFILE" \
      ${REVIEWER:+--reviewer "$REVIEWER"}
    echo "Review the baseline diff and commit it with the profile version it belongs to."
    ;;

  check)
    # Historical mode: compare the recorded same-host criterion medians.
    out=$(cargo bench --bench throughput -- --warm-up-time 0.5 --measurement-time 1.5 2>&1)
    echo "$out"

    BASELINE="$ROOT/crates/engine/benches/results/baseline.md"
    [[ -f "$BASELINE" ]] || { echo "baseline missing: $BASELINE" >&2; exit 2; }

    base_h1_w4=$(grep -oP 'h1/workers_4 \| ~\K[0-9.]+(?= GiB/s)' "$BASELINE")
    base_h2_w4=$(grep -oP 'h2/workers_4 \| ~\K[0-9.]+(?= GiB/s)' "$BASELINE")
    base_prealloc=$(grep -oP 'prealloc_true \| ~\K[0-9.]+(?= GiB/s)' "$BASELINE")

    fail=0
    check() { # name got base
        local name="$1" got="$2" base="$3"
        "$PYTHON_BIN" - "$name" "$got" "$base" <<'PY'
import sys
name, got, base = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
loss = (base - got) / base * 100
status = "OK" if loss <= 10 else "REGRESSION"
print(f"{name}: {got:.2f} GiB/s vs baseline {base:.2f} GiB/s -> {loss:+.1f}% [{status}]")
sys.exit(1 if loss > 10 else 0)
PY
    }

    got_h1_w4=$(echo "$out" | grep -A1 "^h1/workers_4 " | grep thrpt | grep -oP '[0-9.]+ GiB/s' | head -1 | cut -d' ' -f1)
    got_h2_w4=$(echo "$out" | grep -A1 "^h2/workers_4 " | grep thrpt | grep -oP '[0-9.]+ GiB/s' | head -1 | cut -d' ' -f1)
    got_prealloc=$(echo "$out" | grep -A1 "^prealloc/prealloc_true" | grep thrpt | grep -oP '[0-9.]+ GiB/s' | head -1 | cut -d' ' -f1)

    check "h1/workers_4" "$got_h1_w4" "$base_h1_w4" || fail=1
    check "h2/workers_4" "$got_h2_w4" "$base_h2_w4" || fail=1
    check "prealloc_true" "$got_prealloc" "$base_prealloc" || fail=1

    exit $fail
    ;;

  *) usage ;;
esac
