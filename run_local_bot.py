"""
Local PC Duty Bot — thin launcher for persistent-connection mode.

  python run_local_bot.py

Same as ``LARK_EVENT_MODE=websocket python main.py``.
"""
from __future__ import annotations

import os
import sys

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


def main() -> int:
    os.environ.setdefault("LARK_EVENT_MODE", "websocket")
    _apply_local_playwright_defaults()
    root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(root)
    if root not in sys.path:
        sys.path.insert(0, root)
    import main as larkbot_main

    return larkbot_main._run_main_entry()


if __name__ == "__main__":
    raise SystemExit(main())
