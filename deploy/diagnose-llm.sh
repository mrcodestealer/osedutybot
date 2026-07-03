#!/usr/bin/env bash
# Diagnose why osedutybot LLM replies are slow. Run as root on the osedutybot server:
#   bash /root/osedutybot/deploy/diagnose-llm.sh
set -uo pipefail

OLLAMA_HOST="${OLLAMA_HOST:-http://127.0.0.1:11434}"
MODEL="${BOT_CHAT_MODEL:-qwen3.6:35b-a3b}"

hr() { echo "----------------------------------------------------------------"; }

hr; echo "== 1) Hardware =="
echo "CPU cores (logical): $(nproc)"
lscpu | grep -E '^(Model name|Socket|Core|Thread)' || true
free -h
echo "Swap in use:"; swapon --show || echo "  (no swap)"

hr; echo "== 2) Ollama service =="
ollama --version 2>/dev/null || echo "ollama CLI not found"
systemctl is-active ollama
echo "-- systemd override installed? --"
if [[ -f /etc/systemd/system/ollama.service.d/override.conf ]]; then
  cat /etc/systemd/system/ollama.service.d/override.conf
else
  echo "!! NO OVERRIDE INSTALLED -> keep_alive defaults to 5m, model unloads when idle !!"
fi
echo "-- effective env of running ollama --"
OLLAMA_PID=$(pgrep -x ollama | head -1 || true)
if [[ -n "${OLLAMA_PID:-}" ]]; then
  tr '\0' '\n' < "/proc/${OLLAMA_PID}/environ" | grep -E 'OLLAMA_|LLAMA_' || echo "(no OLLAMA_ vars set)"
  echo "-- is the model paged out to swap? --"
  grep -E 'VmRSS|VmSwap' "/proc/${OLLAMA_PID}/status"
fi

hr; echo "== 3) Loaded models (ollama ps) =="
ollama ps 2>/dev/null || curl -s "${OLLAMA_HOST}/api/ps"

hr; echo "== 4) Timed generation test (${MODEL}) =="
echo "Sending a short prompt, measuring speed..."
RESP=$(curl -s "${OLLAMA_HOST}/api/generate" -d "{
  \"model\": \"${MODEL}\",
  \"prompt\": \"Say hello in one short sentence.\",
  \"stream\": false,
  \"keep_alive\": -1
}")
python3 - "$RESP" <<'PY'
import json, sys
try:
    d = json.loads(sys.argv[1])
except Exception:
    print("Could not parse response:", sys.argv[1][:300]); sys.exit(0)
ns = 1e9
load = d.get("load_duration", 0) / ns
prompt_n = d.get("prompt_eval_count", 0)
prompt_s = d.get("prompt_eval_duration", 0) / ns
gen_n = d.get("eval_count", 0)
gen_s = d.get("eval_duration", 0) / ns
total = d.get("total_duration", 0) / ns
print(f"model load time : {load:8.2f}s   (should be ~0 if model already resident)")
if prompt_s: print(f"prompt speed    : {prompt_n/prompt_s:8.1f} tok/s ({prompt_n} tokens)")
if gen_s:    print(f"generation speed: {gen_n/gen_s:8.1f} tok/s ({gen_n} tokens)")
print(f"total           : {total:8.2f}s")
if load > 5: print(">> PROBLEM: model was NOT resident in RAM (keep_alive not working)")
if gen_s and gen_n/gen_s < 5: print(">> PROBLEM: generation very slow -> check swap / CPU contention")
PY

hr; echo "== 5) Top CPU consumers right now =="
ps -eo pid,comm,%cpu,%mem --sort=-%cpu | head -8

hr; echo "== 6) Recent ollama unload/load events =="
journalctl -u ollama --since "24 hours ago" 2>/dev/null | grep -iE 'unload|loading|evict' | tail -10 || true

hr; echo "Done. If section 2 shows NO OVERRIDE INSTALLED, run:"
echo "  bash /root/osedutybot/deploy/setup-consistent-ollama.sh"
