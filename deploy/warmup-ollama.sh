#!/usr/bin/env bash
# Preload the chat model so the first Lark message is not a cold start.
set -euo pipefail

OLLAMA_HOST="${OLLAMA_HOST:-http://127.0.0.1:11434}"
MODEL="${BOT_CHAT_MODEL:-qwen3.5:35b-a3b}"
MAX_WAIT="${OLLAMA_WARMUP_WAIT_SEC:-600}"

if [[ -f /root/osedutybot/.env ]]; then
  # shellcheck disable=SC1091
  set -a
  source /root/osedutybot/.env
  set +a
  MODEL="${BOT_CHAT_MODEL:-$MODEL}"
fi

echo "[warmup-ollama] waiting for $OLLAMA_HOST ..."
deadline=$((SECONDS + MAX_WAIT))
until curl -sf "${OLLAMA_HOST}/api/tags" >/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "[warmup-ollama] ERROR: Ollama not ready after ${MAX_WAIT}s" >&2
    exit 1
  fi
  sleep 2
done

echo "[warmup-ollama] loading model: $MODEL"
curl -sf "${OLLAMA_HOST}/api/generate" -d "{
  \"model\": \"${MODEL}\",
  \"prompt\": \"hi\",
  \"stream\": false,
  \"keep_alive\": -1,
  \"options\": {\"num_predict\": 8}
}" >/dev/null

echo "[warmup-ollama] OK — model should stay loaded (keep_alive=-1)"
