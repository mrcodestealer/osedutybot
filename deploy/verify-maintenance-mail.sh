#!/usr/bin/env bash
# Quick check that maintenance email settings + state exist on the server.
set -euo pipefail

ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT"

ENV_FILE="${ENV_FILE:-.env}"
ok=0

check_env() {
  local key="$1"
  if [[ -f "$ENV_FILE" ]] && grep -q "^${key}=" "$ENV_FILE" 2>/dev/null; then
    local val
    val="$(grep "^${key}=" "$ENV_FILE" | head -1 | cut -d= -f2- | tr -d ' \"')"
    if [[ -n "$val" ]]; then
      echo "OK  $key"
      return 0
    fi
  fi
  echo "MISSING/EMPTY  $key"
  ok=1
  return 1
}

echo "=== Maintenance mail (.env) ==="
check_env MAINTENANCE_MAIL_USER || true
check_env MAINTENANCE_MAIL_PASSWORD || true
check_env MAINTENANCE_MAIL_IMAP_HOST || true
check_env MAINTENANCE_MAIL_IMAP_PORT || true
check_env MAINTENANCE_MAIL_TARGET_CHAT_ID || true

echo ""
echo "=== State file ==="
if [[ -f maintenance.json ]]; then
  python3 - <<'PY'
import json, pathlib
p = pathlib.Path("maintenance.json")
try:
    d = json.loads(p.read_text(encoding="utf-8"))
    launched = len(d.get("launched_names") or {})
    uids = len(d.get("handled_uids") or [])
    print(f"OK  maintenance.json ({p.stat().st_size} bytes, launched_names={launched}, handled_uids={uids})")
except Exception as e:
    print(f"WARN  maintenance.json unreadable: {e}")
PY
else
  echo "MISSING  maintenance.json (empty state until watcher runs)"
  ok=1
fi

echo ""
if [[ "$ok" -eq 0 ]]; then
  echo "All maintenance mail checks passed."
else
  echo "Fix missing items above — do NOT git pull over .env; edit .env on server only."
  exit 1
fi
