#!/usr/bin/env bash
# git pull code updates WITHOUT overwriting server runtime data or .env secrets.
# Preserves maintenance email state (maintenance.json) and MAINTENANCE_MAIL_* in .env.
set -euo pipefail

ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT"

BACKUP="${BACKUP_DIR:-/tmp/osedutybot-json-backup-$(date +%Y%m%d%H%M%S)}"
mkdir -p "$BACKUP"

# Critical: maintenance.json = IMAP watcher state (launched games, handled UIDs).
# .env MAINTENANCE_MAIL_* = IMAP password, user, SMTP — never in git; do not delete .env.
KEEP=(
  maintenance.json
  holiday.csv
  allduty.json
  machineencoder.json
  webmachine_data.json
  cpms_igo_uat_services.json
  offset_approver_notified.json
  offset_peer_approver_approval_notified.json
  offset_requester_approval_notified.json
  offset_requester_open_id.json
  offset_rows_snapshot.json
  offset_shift_sheet_applied.json
  offset_delete_actors.json
  offset_deletion_notified.json
  leave_shift_sheet_applied.json
  restart_pending.json
)

echo "[pull] backup dir: $BACKUP"
if [[ -f .env ]]; then
  cp -a .env "$BACKUP/.env.server.backup"
  echo "  saved .env (MAINTENANCE_MAIL_* and all secrets)"
fi

for f in "${KEEP[@]}"; do
  if [[ -f "$f" ]]; then
    cp -a "$f" "$BACKUP/"
    echo "  saved $f"
  fi
done

echo "[pull] clear git tracking diffs on data files (backup safe)..."
while IFS= read -r f; do
  [[ -n "$f" ]] && git checkout HEAD -- "$f" 2>/dev/null || true
done < <(git ls-files '*.json' 'holiday.csv' 2>/dev/null || true)

TRACKED=()
for f in "${KEEP[@]}"; do
  if git ls-files --error-unmatch "$f" >/dev/null 2>&1; then
    TRACKED+=("$f")
  fi
done
if ((${#TRACKED[@]})); then
  git stash push -m "osedutybot local data $(date -Iseconds)" -- "${TRACKED[@]}" 2>/dev/null || true
fi

echo "[pull] git pull origin main"
git pull origin main

echo "[pull] restore server data (maintenance.json first)..."
if [[ -f "$BACKUP/maintenance.json" ]]; then
  cp -a "$BACKUP/maintenance.json" "$ROOT/maintenance.json"
  echo "  restored maintenance.json (email watcher state)"
fi
for f in "${KEEP[@]}"; do
  [[ "$f" == "maintenance.json" ]] && continue
  if [[ -f "$BACKUP/$f" ]]; then
    cp -a "$BACKUP/$f" "$ROOT/$f"
    echo "  restored $f"
  fi
done

if [[ -f "$BACKUP/.env.server.backup" ]] && [[ ! -f .env ]]; then
  cp -a "$BACKUP/.env.server.backup" .env
  echo "  restored .env (was missing)"
fi

echo ""
echo "[pull] verify maintenance email config:"
if [[ -f .env ]] && grep -q '^MAINTENANCE_MAIL_PASSWORD=' .env; then
  echo "  .env MAINTENANCE_MAIL_* OK"
else
  echo "  WARNING: .env missing MAINTENANCE_MAIL_PASSWORD — fix before restart"
fi
if [[ -f maintenance.json ]]; then
  echo "  maintenance.json OK ($(wc -c < maintenance.json) bytes)"
else
  echo "  NOTE: maintenance.json missing (watcher will recreate empty state)"
fi
echo "[pull] done. backup kept at: $BACKUP"
