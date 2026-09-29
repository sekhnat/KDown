#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root/web"
npm ci
npm run build
cd "$repo_root"
cargo build --release -p kdown-app --features bundled-web
