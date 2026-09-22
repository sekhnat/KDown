#!/usr/bin/env bash
# §37.4 regression thresholds: compare current bench throughput against the
# baseline recorded in crates/engine/benches/results/baseline.md.
# Advisory: exits 1 when a threshold trips so CI can surface it.
set -euo pipefail

BASELINE="crates/engine/benches/results/baseline.md"
: "${BASELINE:?baseline missing}"

# Extract baseline throughput medians (the "~X" column) per scenario.
base_h1_w4=$(grep -oP 'h1/workers_4 \| ~\K[0-9.]+(?= GiB/s)' "$BASELINE")
base_h2_w4=$(grep -oP 'h2/workers_4 \| ~\K[0-9.]+(?= GiB/s)' "$BASELINE")
base_prealloc=$(grep -oP 'prealloc_true \| ~\K[0-9.]+(?= GiB/s)' "$BASELINE")

# Run the same scenarios with a tight budget.
out=$(cargo bench --bench throughput -- --warm-up-time 0.5 --measurement-time 1.5 2>&1)
echo "$out"

fail=0
check() { # name got base
    local name="$1" got="$2" base="$3"
    python3 - "$name" "$got" "$base" <<'PY'
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
