#!/usr/bin/env bash
# git pull code updates (no backup — same as manual: git pull origin main).
set -euo pipefail

ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT"

echo "[pull] git pull origin main"
git pull origin main
echo "[pull] done"
