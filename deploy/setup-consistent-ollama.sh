#!/usr/bin/env bash
# Install stable Ollama + warmup for osedutybot on Linux (run as root on OSE-Tools).
set -euo pipefail

REPO_DIR="${REPO_DIR:-/root/osedutybot}"
AMX_LIB="/usr/local/lib/ollama/libggml-cpu-sapphirerapids.so"
AMX_DISABLED="${AMX_DISABLED:-/usr/local/lib/ollama/disabled-amx/libggml-cpu-sapphirerapids.so}"

echo "=== 1) Ollama systemd override ==="
mkdir -p /etc/systemd/system/ollama.service.d
cp -f "${REPO_DIR}/deploy/ollama.service.d/override.conf" \
  /etc/systemd/system/ollama.service.d/override.conf

echo "=== 2) Disable AMX backend (Xeon 6982P qwen3.5 segfault fix) ==="
if [[ -f "$AMX_LIB" ]]; then
  mkdir -p /usr/local/lib/ollama/disabled-amx
  mv -f "$AMX_LIB" "$AMX_DISABLED"
  echo "Moved sapphirerapids.so -> disabled-amx/"
elif [[ -f "$AMX_DISABLED" ]]; then
  echo "AMX lib already disabled."
else
  echo "No sapphirerapids.so found (skip)."
fi

echo "=== 3) Optional swap (helps model pull / load) ==="
if ! swapon --show | grep -q .; then
  if [[ ! -f /swapfile ]]; then
    fallocate -l 16G /swapfile || dd if=/dev/zero of=/swapfile bs=1M count=16384
    chmod 600 /swapfile
    mkswap /swapfile
  fi
  swapon /swapfile 2>/dev/null || true
  grep -q '/swapfile' /etc/fstab 2>/dev/null || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

echo "=== 4) Restart Ollama ==="
systemctl daemon-reload
systemctl enable ollama
systemctl restart ollama
sleep 3
systemctl is-active ollama

echo "=== 5) Install warmup on boot ==="
chmod +x "${REPO_DIR}/deploy/warmup-ollama.sh"
sed "s|/root/osedutybot|${REPO_DIR}|g" "${REPO_DIR}/deploy/ollama-warmup.service" \
  > /etc/systemd/system/ollama-warmup.service
systemctl daemon-reload
systemctl enable ollama-warmup.service

echo "=== 6) Warmup now ==="
REPO_DIR="$REPO_DIR" bash "${REPO_DIR}/deploy/warmup-ollama.sh"

echo ""
echo "=== Done ==="
echo "Check:  ollama ps"
echo "Check:  curl -s http://127.0.0.1:11434/api/tags | head"
echo "Bot .env must include:"
echo "  BOT_CHAT_API_BASE=http://127.0.0.1:11434/v1"
echo "  BOT_CHATAGENT_BACKEND=llm"
echo "  BOT_CHAT_MODEL=qwen3.6:35b-a3b"
echo "  BOT_COMMANDAGENT_LLM_MODEL=qwen2.5:0.5b"
echo "Then:   ollama pull qwen2.5:0.5b && ollama pull qwen3.6:35b-a3b"
echo "Then:   bash deploy/warmup-ollama.sh && systemctl restart larkbot"
