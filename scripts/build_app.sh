#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root/web"
npm ci
npm run build
cd "$repo_root"
# Force rust-embed to re-embed a changed web/dist: the proc macro does not
# track the folder on its own across dist-only changes.
touch crates/app/src/api/assets.rs
cargo build --release -p kdown-app --features bundled-web
