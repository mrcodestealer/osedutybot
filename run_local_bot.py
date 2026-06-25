"""
Local PC Duty Bot: Flask handlers + Lark WebSocket long connection.

  python run_local_bot.py

No OSE-Tools server, no public webhook URL, no ngrok.
"""
from __future__ import annotations

import os
import sys
import threading
import time

from dotenv import load_dotenv

_ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_ROOT, ".env"))


def _apply_local_playwright_defaults() -> None:
    """Local PC: headless Chromium + skip startup browser pre-warm (override in .env)."""
    headless = (os.getenv("BOT_PLAYWRIGHT_HEADLESS") or "1").strip().lower()
    if headless in ("0", "false", "no", "off"):
        return
    for key, val in (
        ("JENKINSUPDATE_BOT_HEADLESS", "1"),
        ("SMACHINE_HEADLESS", "1"),
        ("CHECKCREDIT_HEADLESS", "1"),
        ("NP_BACKEND_HEADLESS", "1"),
        ("PROD_WARM_POOL", "0"),
        ("JU_WARM_POOL", "0"),
        ("VPN_WARM_BROWSER", "0"),
    ):
        os.environ[key] = val
    print("[local] Playwright headless mode (no visible browser windows)", flush=True)


_apply_local_playwright_defaults()


def _log_gpu_setup() -> None:
    """Print GPU hints for Ollama (LLM) and PyTorch (local commandagent models)."""
    try:
        import torch

        if torch.cuda.is_available():
            name = torch.cuda.get_device_name(0)
            vram = torch.cuda.get_device_properties(0).total_memory // (1024**2)
            print(
                f"[local] PyTorch CUDA: {name} ({vram} MiB) — local AI models use GPU",
                flush=True,
            )
        else:
            print(
                "[local] PyTorch CUDA: not available — local commandagent/chat models use CPU",
                flush=True,
            )
    except ImportError:
        print(
            "[local] PyTorch not installed — only Ollama LLM chat (install torch for local models)",
            flush=True,
        )
    except Exception as exc:
        print(f"[local] PyTorch GPU check skipped: {exc!r}", flush=True)

    backend = (os.getenv("BOT_CHATAGENT_BACKEND") or "auto").strip().lower()
    base = (os.getenv("BOT_CHAT_API_BASE") or "").strip().rstrip("/")
    if backend in ("llm", "auto") and ("11434" in base or "ollama" in base.lower()):
        model = (os.getenv("BOT_CHAT_MODEL") or "qwen3.5:9b").strip()
        print(
            f"[local] Ollama LLM: {model} @ {base or 'http://127.0.0.1:11434/v1'}",
            flush=True,
        )
        print(
            "[local] Ollama GPU: controlled by the Ollama app (not this bot). "
            "While a model is loaded, run `ollama ps` — PROCESSOR should show `gpu`, not `100% CPU`.",
            flush=True,
        )
        print(
            "[local] If Ollama is CPU-only: update Ollama, keep NVIDIA drivers current, "
            "then set OLLAMA_NUM_GPU=1 in Windows user env and restart Ollama.",
            flush=True,
        )
        try:
            import json
            import urllib.request

            root = base.replace("/v1", "").rstrip("/") or "http://127.0.0.1:11434"
            with urllib.request.urlopen(f"{root}/api/ps", timeout=2) as resp:
                rows = json.loads(resp.read().decode("utf-8")).get("models") or []
            for row in rows:
                proc = row.get("processor") or row.get("size_vram") or "?"
                print(
                    f"[local] Ollama loaded: {row.get('name', '?')} processor={proc!r}",
                    flush=True,
                )
        except Exception:
            print(
                "[local] Tip: chat once, then `ollama ps` to confirm GPU usage.",
                flush=True,
            )


_log_gpu_setup()


def _start_flask() -> None:
    root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(root)
    if root not in sys.path:
        sys.path.insert(0, root)
    import main

    main._run_main_entry()


def main() -> int:
    mode = (os.getenv("LARK_EVENT_MODE") or "websocket").strip().lower()
    if mode != "websocket":
        print("[local] LARK_EVENT_MODE is not websocket — use python main.py", flush=True)
        return 1

    t = threading.Thread(target=_start_flask, daemon=True, name="larkbot-flask")
    t.start()
    time.sleep(2)
    print("[local] Flask starting in background; opening Lark long connection…", flush=True)

    from lark_longconn import run_forever

    run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
