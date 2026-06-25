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
