#!/usr/bin/env bash
# git pull without losing local *.json / holiday.csv (server runtime data).
set -euo pipefail

ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT"

BACKUP="${BACKUP_DIR:-/tmp/osedutybot-json-backup-$(date +%Y%m%d%H%M%S)}"
mkdir -p "$BACKUP"

# Files that must stay on server but are no longer in git
KEEP=(
  holiday.csv
  allduty.json
  machineencoder.json
  webmachine_data.json
  maintenance.json
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

echo "[pull] backing up local data files to $BACKUP"
for f in "${KEEP[@]}"; do
  if [[ -f "$f" ]]; then
    cp -a "$f" "$BACKUP/"
    echo "  saved $f"
  fi
done

echo "[pull] stashing local changes to data files (if tracked)..."
TRACKED=()
for f in "${KEEP[@]}"; do
  if git ls-files --error-unmatch "$f" >/dev/null 2>&1; then
    TRACKED+=("$f")
  fi
done

if ((${#TRACKED[@]})); then
  git stash push -m "osedutybot local json $(date -Iseconds)" -- "${TRACKED[@]}" || true
fi

echo "[pull] git pull origin main"
git pull origin main

echo "[pull] restoring local data files"
for f in "${KEEP[@]}"; do
  if [[ -f "$BACKUP/$f" ]]; then
    cp -a "$BACKUP/$f" "$ROOT/$f"
    echo "  restored $f"
  fi
done

echo "[pull] done — json/csv are local-only (.gitignore). backup: $BACKUP"
