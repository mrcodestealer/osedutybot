"""
Lark WebSocket long connection → local larkbot webhook.

Use on a PC with no public IP. Set LARK_EVENT_MODE=websocket in .env and run
``python run_local_bot.py`` (or start main.py + this module separately).

Requires: pip install lark-oapi
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid

import requests
from dotenv import load_dotenv

_ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_ROOT, ".env"))

APP_ID = (os.getenv("APP_ID") or "").strip()
APP_SECRET = (os.getenv("APP_SECRET") or "").strip()
VERIFICATION_TOKEN = (os.getenv("VERIFICATION_TOKEN") or "").strip()
LOCAL_WEBHOOK = (
    os.getenv("LARK_LOCAL_WEBHOOK_URL") or "http://127.0.0.1:5000/webhook/event"
).strip()


def _wait_for_webhook(timeout_sec: float = 60.0) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            r = requests.get(LOCAL_WEBHOOK, timeout=2)
            if r.status_code < 500:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.5)
    return False


def _to_webhook_payload(data) -> dict:
    import lark_oapi as lark

    raw = json.loads(lark.JSON.marshal(data))
    if isinstance(raw, dict) and "header" in raw and "event" in raw:
        return raw
    inner = raw.get("event", raw) if isinstance(raw, dict) else raw
    return {
        "schema": "2.0",
        "header": {
            "event_id": str(uuid.uuid4()),
            "event_type": "im.message.receive_v1",
            "create_time": str(int(time.time() * 1000)),
            "token": VERIFICATION_TOKEN,
        },
        "event": inner,
    }


def _on_message(data) -> None:
    try:
        payload = _to_webhook_payload(data)
        r = requests.post(LOCAL_WEBHOOK, json=payload, timeout=300)
        print(f"[lark-ws] forwarded → {LOCAL_WEBHOOK} status={r.status_code}", flush=True)
    except Exception as exc:
        print(f"[lark-ws] forward failed: {exc!r}", flush=True)


def run_forever() -> None:
    import lark_oapi as lark

    if not APP_ID or not APP_SECRET:
        print("[lark-ws] Set APP_ID and APP_SECRET in .env", file=sys.stderr)
        sys.exit(1)

    if not _wait_for_webhook():
        print(
            f"[lark-ws] Local webhook not ready at {LOCAL_WEBHOOK} — start main.py first",
            file=sys.stderr,
        )
        sys.exit(1)

    handler = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(_on_message)
        .build()
    )
    domain_name = (os.getenv("LARK_DOMAIN") or "lark").strip().lower()
    domain = lark.FEISHU_DOMAIN if domain_name == "feishu" else lark.LARK_DOMAIN

    cli = lark.ws.Client(
        APP_ID,
        APP_SECRET,
        event_handler=handler,
        log_level=lark.LogLevel.INFO,
        domain=domain,
    )
    print(f"[lark-ws] Long connection active → {LOCAL_WEBHOOK}", flush=True)
    cli.start()


if __name__ == "__main__":
    run_forever()
