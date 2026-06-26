"""
Lark WebSocket long connection → local larkbot webhook.

Use on a PC with no public IP. Set LARK_EVENT_MODE=websocket in .env and run
``python run_local_bot.py`` (or start main.py + this module separately).

Requires: pip install lark-oapi

**Card buttons (checkcredit, Jenkins, reminders, …):**
Subscribe **Card callback interaction** ``card.action.trigger`` in the developer
console when using **persistent connection**. This module registers
``register_p2_card_action_trigger`` and patches a known ``lark-oapi`` bug where
``MessageType.CARD`` frames were dropped (Lark client shows ``code: undefined``).
"""
from __future__ import annotations

import base64
import http
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
CARD_CALLBACK_TIMEOUT_SEC = float(os.getenv("LARK_CARD_CALLBACK_TIMEOUT_SEC", "2.8"))


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


def _ensure_inbound_message_id(payload: dict) -> dict:
    """WebSocket SDK payloads should carry ``event.message.message_id`` for quoted replies."""
    if not isinstance(payload, dict):
        return payload
    ev = payload.get("event")
    if not isinstance(ev, dict):
        return payload
    msg = ev.get("message")
    if not isinstance(msg, dict):
        return payload
    if (msg.get("message_id") or "").strip():
        return payload
    for alt in (
        ev.get("message_id"),
        (ev.get("message") or {}).get("message_id") if isinstance(ev.get("message"), dict) else None,
    ):
        mid = str(alt or "").strip()
        if mid:
            msg["message_id"] = mid
            break
    return payload


def _ensure_card_webhook_payload(payload: dict) -> dict:
    """Schema 2.0 card callback shape for ``main.lark_webhook`` (token + open_chat_id)."""
    out = dict(payload)
    out.setdefault("schema", "2.0")
    hdr = dict(out.get("header") or {})
    hdr.setdefault("event_type", "card.action.trigger")
    hdr.setdefault("event_id", hdr.get("event_id") or str(uuid.uuid4()))
    if VERIFICATION_TOKEN and not str(hdr.get("token") or "").strip():
        hdr["token"] = VERIFICATION_TOKEN
    out["header"] = hdr
    ev = out.get("event")
    if isinstance(ev, dict):
        ctx = ev.get("context") if isinstance(ev.get("context"), dict) else {}
        if not ev.get("open_chat_id") and ctx.get("open_chat_id"):
            ev["open_chat_id"] = str(ctx["open_chat_id"]).strip()
        if not ev.get("chat_id") and ctx.get("chat_id"):
            ev["chat_id"] = str(ctx["chat_id"]).strip()
        out["event"] = ev
    return out


def _to_webhook_payload(data) -> dict:
    import lark_oapi as lark

    raw = json.loads(lark.JSON.marshal(data))
    if isinstance(raw, dict) and "header" in raw and "event" in raw:
        payload = dict(raw)
        hdr = dict(payload.get("header") or {})
        payload["header"] = hdr
    else:
        inner = raw.get("event", raw) if isinstance(raw, dict) else raw
        payload = {
            "schema": "2.0",
            "header": {
                "event_id": str(uuid.uuid4()),
                "event_type": "im.message.receive_v1",
                "create_time": str(int(time.time() * 1000)),
            },
            "event": inner,
        }

    if VERIFICATION_TOKEN:
        hdr = payload.setdefault("header", {})
        if not str(hdr.get("token") or "").strip():
            hdr["token"] = VERIFICATION_TOKEN
    payload = _ensure_inbound_message_id(payload)
    mid = (
        ((payload.get("event") or {}).get("message") or {}).get("message_id")
        if isinstance(payload.get("event"), dict)
        else None
    )
    if not str(mid or "").strip():
        print("[lark-ws] warning: forwarded payload missing event.message.message_id", flush=True)
    return payload


def _post_webhook(payload: dict, *, timeout_sec: float) -> tuple[int, dict]:
    r = requests.post(LOCAL_WEBHOOK, json=payload, timeout=timeout_sec)
    body: dict = {}
    if r.content:
        try:
            parsed = r.json()
            if isinstance(parsed, dict):
                body = parsed
        except ValueError:
            body = {}
    return r.status_code, body


def _on_message(data) -> None:
    try:
        payload = _to_webhook_payload(data)
        r = requests.post(LOCAL_WEBHOOK, json=payload, timeout=300)
        print(f"[lark-ws] im.message → {LOCAL_WEBHOOK} status={r.status_code}", flush=True)
    except Exception as exc:
        print(f"[lark-ws] im forward failed: {exc!r}", flush=True)


def _on_card_action(data):
    """
    ``card.action.trigger`` over WebSocket — must return within ~3s or Lark shows ``code: undefined``.
    Forwards to local Flask webhook (same handlers as HTTPS mode) and returns its JSON body.
    """
    from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTriggerResponse

    import lark_oapi as lark

    try:
        payload = _ensure_card_webhook_payload(json.loads(lark.JSON.marshal(data)))
        status, body = _post_webhook(payload, timeout_sec=CARD_CALLBACK_TIMEOUT_SEC)
        print(
            f"[lark-ws] card.action.trigger → {LOCAL_WEBHOOK} status={status} "
            f"resp_keys={list(body.keys())!r}",
            flush=True,
        )
        if status == 200 and isinstance(body, dict):
            return P2CardActionTriggerResponse(body)
        if status == 403:
            print(
                "[lark-ws] card callback 403 — check VERIFICATION_TOKEN matches developer console",
                flush=True,
            )
    except Exception as exc:
        print(f"[lark-ws] card callback failed: {exc!r}", flush=True)
    return P2CardActionTriggerResponse({})


def _apply_lark_ws_card_frame_patch() -> None:
    """
    ``lark-oapi`` ws client (through 1.6.x) returns early on ``MessageType.CARD`` without ACK.
    That breaks every interactive card button in persistent-connection mode.
    """
    try:
        from lark_oapi.core.const import UTF_8
        from lark_oapi.core.json import JSON
        from lark_oapi.ws.client import Client, _get_by_key
        from lark_oapi.ws.const import (
            HEADER_BIZ_RT,
            HEADER_MESSAGE_ID,
            HEADER_SEQ,
            HEADER_SUM,
            HEADER_TRACE_ID,
            HEADER_TYPE,
        )
        from lark_oapi.ws.enum import MessageType
        from lark_oapi.ws.model import Response
    except ImportError:
        print("[lark-ws] lark-oapi ws imports missing — card patch skipped", flush=True)
        return

    if getattr(Client, "_osedutybot_card_patch", False):
        return

    async def _handle_data_frame_patched(self, frame):
        hs = frame.headers
        msg_id = _get_by_key(hs, HEADER_MESSAGE_ID)
        trace_id = _get_by_key(hs, HEADER_TRACE_ID)
        sum_ = _get_by_key(hs, HEADER_SUM)
        seq = _get_by_key(hs, HEADER_SEQ)
        type_ = _get_by_key(hs, HEADER_TYPE)

        pl = frame.payload
        if int(sum_) > 1:
            pl = self._combine(msg_id, int(sum_), int(seq), pl)
            if pl is None:
                return

        message_type = MessageType(type_)
        resp = Response(code=http.HTTPStatus.OK)
        try:
            start = int(round(time.time() * 1000))
            if message_type in (MessageType.EVENT, MessageType.CARD):
                result = self._event_handler._do_without_validation(pl)
            else:
                return
            end = int(round(time.time() * 1000))
            header = hs.add()
            header.key = HEADER_BIZ_RT
            header.value = str(end - start)
            if result is not None:
                resp.data = base64.b64encode(JSON.marshal(result).encode(UTF_8))
        except Exception as e:
            from lark_oapi.core.log import logger

            logger.error(
                self._fmt_log(
                    "handle message failed, message_type: {}, message_id: {}, trace_id: {}, err: {}",
                    message_type.value,
                    msg_id,
                    trace_id,
                    e,
                )
            )
            resp = Response(code=http.HTTPStatus.INTERNAL_SERVER_ERROR)

        frame.payload = JSON.marshal(resp).encode(UTF_8)
        await self._write_message(frame.SerializeToString())

    Client._handle_data_frame = _handle_data_frame_patched
    Client._osedutybot_card_patch = True
    print("[lark-ws] patched ws Client._handle_data_frame for CARD callbacks", flush=True)


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

    _apply_lark_ws_card_frame_patch()

    handler = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(_on_message)
        .register_p2_card_action_trigger(_on_card_action)
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
    print(
        f"[lark-ws] Long connection active → {LOCAL_WEBHOOK} "
        "(im.message + card.action.trigger)",
        flush=True,
    )
    cli.start()


if __name__ == "__main__":
    run_forever()
