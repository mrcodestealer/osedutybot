#!/usr/bin/env bash
# Compare command-routing latency for all Ollama models on this host.
set -euo pipefail

REPO_DIR="${REPO_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$REPO_DIR"

PHRASE="${1:-who is covering fpms shift tonight}"
RUNS="${BENCH_RUNS:-3}"

echo "[bench-all-models] phrase=$PHRASE runs=$RUNS"
exec python3 commandagent.py bench-all "$PHRASE" -n "$RUNS" --from-ollama
