#!/usr/bin/env bash
# Dynamic-analysis checks (reliability-verification task 5.3).
#
# Targeted Miri and sanitizer/equivalent jobs run the isolated state-machine
# tests that actually exercise memory-unsafe-adjacent code paths (parser
# metadata, interval/lease scheduling, transfer-ledger admission) rather than
# the whole suite: these checkers are orders of magnitude slower and the full
# matrix already runs on three operating systems.
#
# Unavailable-tool policy (spec: "Unsupported dynamic checker"):
#   * `unavailable` is NEVER recorded as passing and the command exits non-zero,
#     so a missing checker makes its job red and blocks release.
#   * `--fallback-equivalent` runs the documented applicable equivalent
#     (address sanitizer over the same targeted tests, which is available
#     wherever nightly is) and records `passed_equivalent` naming both the
#     limitation and the substitute. The release gate accepts that only when
#     the equivalent is named in the evidence.
#   * A sanitizer that cannot execute on the runner (for example ThreadSanitizer
#     failing on a kernel/ASLR configuration) is recorded as `unavailable` with
#     the observed limitation, which the release gate treats as blocking unless
#     a reviewed exception exists in the evidence manifest.
#
# Usage:
#   scripts/dynamic_checks.sh probe
#   scripts/dynamic_checks.sh miri [--fallback-equivalent]
#   scripts/dynamic_checks.sh address
#   scripts/dynamic_checks.sh thread
#   scripts/dynamic_checks.sh self-test
#
# Evidence fragments are written to $OUT_DIR (default artifacts/dynamic).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/artifacts/dynamic}"
mkdir -p "$OUT_DIR"

MODE="${1:-probe}"
FALLBACK_EQUIVALENT=0
if [[ "${2:-}" == "--fallback-equivalent" ]]; then
  FALLBACK_EQUIVALENT=1
fi

# Targeted test filters: pure or thread-concurrency state machines that the
# dynamic checkers can actually execute at useful depth.
PARSER_FILTER="fuzz_targets::smoke::corpus_smoke_no_panics"
SCHEDULER_FILTER="internal_tests::scheduler_property_tests"
LEDGER_FILTER="internal_tests::transfer_memory_tests"

# Bounded, deterministic property runs: Miri/sanitizers interpret or instrument
# every instruction, so the full nightly case count is not affordable here.
PROPTEST_ENV="PROPTEST_CASES=8 PROPTEST_RNG_SEED=2026092702"
MIRIFLAGS_DEFAULT="-Zmiri-disable-isolation"
MIRIFLAGS="${MIRIFLAGS:-$MIRIFLAGS_DEFAULT}"

# Sanitizer builds instrument the standard library as well; this requires the
# nightly `rust-src` component and an explicit target triple.
SANITIZER_TARGET="${SANITIZER_TARGET:-x86_64-unknown-linux-gnu}"

# Unavailable-simulation switch used by `self-test` so the unavailable-tool
# semantics stay verifiable even on a runner that has nightly installed.
FORCE_UNAVAILABLE="${DYNAMIC_FORCE_UNAVAILABLE:-}"

have_nightly() {
  [ -n "$FORCE_UNAVAILABLE" ] && return 1
  # `cargo +nightly` requires rustup; the version string must actually be a
  # nightly, because a plain `rustc` may ignore the `+toolchain` argument.
  cargo +nightly --version >/dev/null 2>&1 || return 1
  case "$(rustc +nightly --version 2>/dev/null)" in
    *nightly*) return 0 ;;
    *) return 1 ;;
  esac
}
have_miri() { have_nightly && cargo +nightly miri --version >/dev/null 2>&1; }
have_rust_src() {
  have_nightly && rustc +nightly --print sysroot 2>/dev/null | xargs -r -I{} test -d "{}/lib/rustlib/src/rust/library"
}

toolchain_line() {
  if have_nightly; then
    printf 'nightly: %s' "$(rustc +nightly --version 2>/dev/null)"; printf '; cargo: %s' "$(cargo +nightly --version 2>/dev/null)"
  else
    printf 'nightly: unavailable'
  fi
}

git_commit() {
  if [[ -n "${GITHUB_SHA:-}" ]]; then
    printf '%s' "$GITHUB_SHA"
  else
    git -C "$ROOT" rev-parse HEAD 2>/dev/null || printf 'unknown'
  fi
}

record() {
  # record <id> <status> <detail> [extra evidence_io args...]
  local id="$1" status="$2" detail="$3"
  shift 3
  python3 "$ROOT/scripts/evidence_io.py" record \
    --out "$OUT_DIR/$id.json" \
    --id "$id" \
    --category security_dynamic \
    --status "$status" \
    --source "scripts/dynamic_checks.sh:$MODE" \
    --detail "$detail" \
    --toolchain "$(toolchain_line)" \
    --commit "$(git_commit)" \
    --artifact "$OUT_DIR/$id.log" \
    "$@"
}

run_commands() {
  # run_commands <log> <command>...
  local log="$1"
  shift
  : > "$log"
  local failed=0
  for cmd in "$@"; do
    printf '=== %s\n' "$cmd" >> "$log"
    start=$(date +%s)
    rc=0
    eval "$cmd" >> "$log" 2>&1 || rc=$?
    printf '%s\n' "--- exit=$rc elapsed=$(( $(date +%s) - start ))s" >> "$log"
    [ "$rc" -eq 0 ] || failed=1
  done
  return "$failed"
}

probe() {
  printf 'nightly: %s\n' "$(have_nightly && echo available || echo unavailable)"
  printf 'miri: %s\n' "$(have_miri && echo available || echo unavailable)"
  printf 'rust-src (sanitizer build-std): %s\n' "$(have_rust_src && echo available || echo unavailable)"
  printf 'toolchain: %s\n' "$(toolchain_line)"
}

miri_commands() {
  printf '%s\n' \
    "MIRIFLAGS='$MIRIFLAGS' cargo +nightly miri test --locked -p kdown-engine --lib $PARSER_FILTER -- --exact" \
    "MIRIFLAGS='$MIRIFLAGS' $PROPTEST_ENV cargo +nightly miri test --locked -p kdown-engine --lib $SCHEDULER_FILTER -- --test-threads=1"
}

asan_commands() {
  local flags="-Zsanitizer=address -Cdebug-assertions=on"
  printf '%s\n' \
    "RUSTFLAGS='$flags' RUSTDOCFLAGS='-Zsanitizer=address' cargo +nightly test -Zbuild-std --target $SANITIZER_TARGET --locked -p kdown-engine --lib $PARSER_FILTER -- --exact" \
    "RUSTFLAGS='$flags' RUSTDOCFLAGS='-Zsanitizer=address' $PROPTEST_ENV cargo +nightly test -Zbuild-std --target $SANITIZER_TARGET --locked -p kdown-engine --lib $SCHEDULER_FILTER -- --test-threads=1" \
    "RUSTFLAGS='$flags' RUSTDOCFLAGS='-Zsanitizer=address' cargo +nightly test -Zbuild-std --target $SANITIZER_TARGET --locked -p kdown-engine --lib $LEDGER_FILTER -- --test-threads=1"
}

tsan_commands() {
  local flags="-Zsanitizer=thread -Cdebug-assertions=on"
  printf '%s\n' \
    "RUSTFLAGS='$flags' RUSTDOCFLAGS='-Zsanitizer=thread' cargo +nightly test -Zbuild-std --target $SANITIZER_TARGET --locked -p kdown-engine --lib $LEDGER_FILTER -- --test-threads=1"
}

run_miri() {
  local log="$OUT_DIR/security_dynamic_miri.log"
  if have_miri; then
    mapfile -t cmds < <(miri_commands)
    if run_commands "$log" "${cmds[@]}"; then
      record security_dynamic_miri passed "Targeted Miri run over parser metadata and scheduler state machines." \
        --command "${cmds[0]}" --command "${cmds[1]}" --approves-release --max-age-days 30
      return 0
    fi
    record security_dynamic_miri failed "Miri reported a failure in a targeted state machine; release is blocked." \
      --command "${cmds[0]}" --command "${cmds[1]}" --approves-release --max-age-days 30
    return 1
  fi

  local limitation="Miri is not installed for the nightly toolchain on this runner."
  if [[ "$FALLBACK_EQUIVALENT" -eq 1 ]]; then
    # The substitute lane records its own evidence; when even the substitute
    # cannot execute, the Miri gate is recorded as unavailable so release
    # stays blocked instead of silently lacking an entry.
    if run_address "miri-unavailable: $limitation"; then
      return 0
    fi
    record security_dynamic_miri unavailable "$limitation No applicable equivalent could execute on this runner." \
      --limitation "$limitation" --max-age-days 30
    return 1
  fi
  printf 'limitation: %s\n' "$limitation" > "$log"
  record security_dynamic_miri unavailable "$limitation" \
    --limitation "$limitation" --max-age-days 30
  return 1
}

run_address() {
  local equivalent_reason="${1:-}"
  local log="$OUT_DIR/security_dynamic_asan.log"
  local status=passed
  local detail="Address-sanitizer run over parser metadata, scheduler and transfer-ledger state machines."
  if ! have_rust_src; then
    local limitation="Nightly rust-src is unavailable, so -Zbuild-std sanitizer runs cannot execute here."
    printf 'limitation: %s\n' "$limitation" > "$log"
    record security_dynamic_asan unavailable "$limitation" \
      --limitation "$limitation" --max-age-days 30
    return 1
  fi
  mapfile -t cmds < <(asan_commands)
  if ! run_commands "$log" "${cmds[@]}"; then
    status=failed
    detail="Address sanitizer reported a failure; release is blocked."
  fi
  if [[ "$status" == "passed" ]]; then
    record security_dynamic_asan passed "$detail" \
      --command "${cmds[0]}" --command "${cmds[1]}" --command "${cmds[2]}" \
      --approves-release --max-age-days 30
  else
    record security_dynamic_asan failed "$detail" \
      --command "${cmds[0]}" --command "${cmds[1]}" --command "${cmds[2]}" \
      --max-age-days 30
  fi
  if [[ -n "$equivalent_reason" && "$status" == "passed" ]]; then
    record security_dynamic_miri passed_equivalent "$equivalent_reason" \
      --limitation "Miri is unavailable on this runner." \
      --equivalent "address sanitizer over the same targeted tests (scripts/dynamic_checks.sh address)" \
      --command "${cmds[0]}" --command "${cmds[1]}" --command "${cmds[2]}" \
      --approves-release --max-age-days 30
    return 0
  fi
  [[ "$status" == "passed" ]]
}

run_thread() {
  local log="$OUT_DIR/security_dynamic_tsan.log"
  if ! have_rust_src; then
    local limitation="Nightly rust-src is unavailable, so ThreadSanitizer (-Zbuild-std) cannot execute here."
    printf 'limitation: %s\n' "$limitation" > "$log"
    record security_dynamic_tsan unavailable "$limitation" \
      --limitation "$limitation" --max-age-days 30
    return 1
  fi
  mapfile -t cmds < <(tsan_commands)
  local failed=0
  run_commands "$log" "${cmds[@]}" || failed=1
  if [[ "$failed" -eq 0 ]]; then
    record security_dynamic_tsan passed "ThreadSanitizer run over the transfer-ledger admission state machine." \
      --command "${cmds[0]}" --approves-release --max-age-days 30
    return 0
  fi
  # A known-unrunnable environment is reported as a limitation, never as a
  # passing check: the release gate then requires a reviewed exception.
  if grep -qE "unexpected memory mapping|FATAL: ThreadSanitizer: (unexpected|failed to)" "$log"; then
    local limitation="ThreadSanitizer cannot map its shadow memory on this runner (kernel ASLR/mapping limitation); see $log."
    record security_dynamic_tsan unavailable "$limitation" \
      --limitation "$limitation" --command "${cmds[0]}" --max-age-days 30
    return 1
  fi
  record security_dynamic_tsan failed "ThreadSanitizer reported a data race or an unexpected failure; release is blocked." \
    --command "${cmds[0]}" --approves-release --max-age-days 30
  return 1
}

self_test() {
  # Verifies the unavailable/failure semantics that keep a green release from
  # being declared without real dynamic evidence. Runs without nightly.
  local tmp
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN
  local fail=0
  local status

  # 1. Unavailable Miri without a fallback is recorded as unavailable and
  #    exits non-zero (a red job, not a silent pass).
  status=$(
    set +e
    DYNAMIC_FORCE_UNAVAILABLE=1 OUT_DIR="$tmp" bash "$0" miri >/dev/null 2>&1
    echo $?
  )
  if [[ "$status" -eq 0 ]]; then
    echo "FAIL: miri-unavailable did not exit non-zero"; fail=1
  fi
  if ! grep -q '"status": "unavailable"' "$tmp/security_dynamic_miri.json" 2>/dev/null; then
    echo "FAIL: miri-unavailable was not recorded as unavailable"; fail=1
  fi
  if grep -q '"status": "passed"' "$tmp/security_dynamic_miri.json" 2>/dev/null; then
    echo "FAIL: miri-unavailable was recorded as passing"; fail=1
  fi

  # 2. Unavailable evidence never names an equivalent that did not run, and
  #    the address-sanitizer lane cannot claim a pass when it was never run.
  if grep -q '"equivalent": "' "$tmp/security_dynamic_miri.json" 2>/dev/null; then
    echo "FAIL: unavailable evidence named an equivalent that did not run"; fail=1
  fi
  if grep -q '"approves_release": true' "$tmp/security_dynamic_miri.json" 2>/dev/null; then
    echo "FAIL: unavailable evidence claimed release approval"; fail=1
  fi

  # 3. With --fallback-equivalent but no nightly, the ASan substitute is also
  #    unavailable: the Miri gate stays unavailable (never passed) and the
  #    command exits non-zero.
  rm -f "$tmp/security_dynamic_miri.json" "$tmp/security_dynamic_asan.json"
  status=$(
    set +e
    DYNAMIC_FORCE_UNAVAILABLE=1 OUT_DIR="$tmp" bash "$0" miri --fallback-equivalent >/dev/null 2>&1
    echo $?
  )
  if [[ "$status" -eq 0 ]]; then
    echo "FAIL: unavailable fallback did not exit non-zero"; fail=1
  fi
  if ! grep -q '"status": "unavailable"' "$tmp/security_dynamic_miri.json" 2>/dev/null; then
    echo "FAIL: fallback without nightly did not leave the Miri gate unavailable"; fail=1
  fi
  if ! grep -q '"status": "unavailable"' "$tmp/security_dynamic_asan.json" 2>/dev/null; then
    echo "FAIL: unavailable substitute was not recorded as unavailable"; fail=1
  fi

  # 4. A real checker failure keeps its own named status: the evidence must
  #    distinguish failure (blocks release) from unavailability.
  python3 "$ROOT/scripts/evidence_io.py" record --out "$tmp/forced_fail.json" \
    --id security_dynamic_miri --category security_dynamic --status failed \
    --command "cargo +nightly miri test (simulated failure)" \
    --approves-release --source self-test >/dev/null
  if ! grep -q '"status": "failed"' "$tmp/forced_fail.json"; then
    echo "FAIL: a checker failure is not recorded as failed"; fail=1
  fi

  if [[ "$fail" -eq 0 ]]; then
    echo "dynamic_checks self-test PASSED"
  fi
  return "$fail"
}

case "$MODE" in
  probe) probe ;;
  miri) run_miri ;;
  address) run_address ;;
  thread) run_thread ;;
  self-test) self_test ;;
  *) echo "usage: $0 {probe|miri|address|thread|self-test} [--fallback-equivalent]" >&2; exit 2 ;;
esac
