#!/usr/bin/env bash
# Preload chat + command models so the first Lark message is not a cold start.
set -euo pipefail

REPO_DIR="${REPO_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
ENV_FILE="${ENV_FILE:-$REPO_DIR/.env}"
OLLAMA_HOST="${OLLAMA_HOST:-http://127.0.0.1:11434}"
CHAT_MODEL="${BOT_CHAT_MODEL:-qwen3.6:35b-a3b}"
CMD_MODEL="${BOT_COMMANDAGENT_LLM_MODEL:-qwen3.5:4b}"
MAX_WAIT="${OLLAMA_WARMUP_WAIT_SEC:-600}"

# Read only the keys we need — do not `source` the whole .env (may have bash syntax issues).
_read_env_key() {
  local key="$1"
  local file="$2"
  [[ -f "$file" ]] || return 1
  local line
  line="$(grep -E "^[[:space:]]*(export[[:space:]]+)?${key}=" "$file" 2>/dev/null | tail -1 || true)"
  [[ -n "$line" ]] || return 1
  local val="${line#*=}"
  val="${val//$'\r'/}"
  val="${val#"${val%%[![:space:]]*}"}"
  val="${val%"${val##*[![:space:]]}"}"
  case "$val" in
    \"*\") val="${val:1:${#val}-2}" ;;
    \'*\') val="${val:1:${#val}-2}" ;;
  esac
  printf '%s' "$val"
}

if [[ -f "$ENV_FILE" ]]; then
  if v="$(_read_env_key BOT_CHAT_MODEL "$ENV_FILE")" && [[ -n "$v" ]]; then
    CHAT_MODEL="$v"
  fi
  if v="$(_read_env_key BOT_COMMANDAGENT_LLM_MODEL "$ENV_FILE")" && [[ -n "$v" ]]; then
    CMD_MODEL="$v"
  fi
  if ! bash -n "$ENV_FILE" 2>/dev/null; then
    echo "[warmup-ollama] WARN: $ENV_FILE has bash syntax errors — using parsed model keys only." >&2
    echo "[warmup-ollama] WARN: fix with: bash -n $ENV_FILE" >&2
  fi
else
  echo "[warmup-ollama] WARN: no $ENV_FILE — using defaults." >&2
fi

warm_model() {
  local model="$1"
  echo "[warmup-ollama] loading model: $model"
  curl -sf "${OLLAMA_HOST}/api/generate" -d "{
    \"model\": \"${model}\",
    \"prompt\": \"hi\",
    \"stream\": false,
    \"keep_alive\": -1,
    \"options\": {\"num_predict\": 8}
  }" >/dev/null
}

echo "[warmup-ollama] waiting for $OLLAMA_HOST ..."
deadline=$((SECONDS + MAX_WAIT))
until curl -sf "${OLLAMA_HOST}/api/tags" >/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "[warmup-ollama] ERROR: Ollama not ready after ${MAX_WAIT}s" >&2
    exit 1
  fi
  sleep 2
done

warm_model "$CMD_MODEL"
if [[ "$CMD_MODEL" != "$CHAT_MODEL" ]]; then
  warm_model "$CHAT_MODEL"
fi

echo "[warmup-ollama] OK — models should stay loaded (keep_alive=-1)"
echo "[warmup-ollama]   command: $CMD_MODEL"
echo "[warmup-ollama]   chat:    $CHAT_MODEL"
