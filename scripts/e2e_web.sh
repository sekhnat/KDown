#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$repo_root/scripts/build_app.sh"
cargo build --manifest-path "$repo_root/Cargo.toml" -p kdown-engine --bin fixture_server
export KDOWN_BIN="$repo_root/target/release/kdown-app"
export FIXTURE_BIN="$repo_root/target/debug/fixture_server"
cd "$repo_root/web"
npx playwright test
