#!/usr/bin/env python3
"""Lark ephemeral group forms for OSE leave / offset (visible only to the requester)."""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any, Callable, Optional

import requests

import ose_Duty as od

_OFFSET_SUBMIT_KEY = "offsetleave_offset_submit"
_LEAVE_SUBMIT_KEY = "offsetleave_leave_submit"


def _wants_offset(text: str) -> bool:
    return bool(re.search(r"\boffset\b", text or "", re.I))


def _wants_leave(text: str) -> bool:
    return bool(re.search(r"\bleave\b", text or "", re.I))


def _fetch_user_display_name(open_id: str, token: str) -> str:
    oid = (open_id or "").strip()
    if not oid:
        return ""
    url = f"https://open.larksuite.com/open-apis/contact/v3/users/{oid}"
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers, params={"user_id_type": "open_id"}, timeout=20).json()
    if res.get("code") != 0:
        return ""
    user = (res.get("data") or {}).get("user") or {}
    for key in ("name", "en_name", "nickname"):
        val = str(user.get(key) or "").strip()
        if val:
            return val
    return ""


def _match_roster_name(display: str) -> Optional[str]:
    raw = (display or "").strip()
    if not raw:
        return None
    titled = od._title_name(raw)
    for roster in od.OSE_LEAVE_FORM_NAMES:
        if titled == od._title_name(roster) or od._names_same_person(roster, raw):
            return od._title_name(roster)
    return None


def resolve_request_person(open_id: str, token: str) -> str:
    display = _fetch_user_display_name(open_id, token)
    roster = _match_roster_name(display)
    if not roster:
        raise ValueError(
            f"Could not match your Lark name {display!r} to an OSE roster name. "
            "Ask admin to align your Lark profile with the duty roster."
        )
    return roster


def _select_options(values: tuple[str, ...] | list[str]) -> list[dict[str, Any]]:
    return [
        {"text": {"tag": "plain_text", "content": str(v)}, "value": str(v)}
        for v in values
    ]


def _callback_payload(kind: str, *, owner_open_id: str, request_person: str) -> dict[str, str]:
    return {
        "k": kind,
        "owner": (owner_open_id or "").strip(),
        "request_person": (request_person or "").strip(),
    }


def build_offset_form_card(*, owner_open_id: str, request_person: str) -> dict[str, Any]:
    exchange_names = [n for n in od.OSE_LEAVE_FORM_NAMES if od._title_name(n) != od._title_name(request_person)]
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "OSE offset request"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**Request person:** {request_person}\n"
                            "Fill the fields below, then tap **Submit**."
                        ),
                    },
                },
                {
                    "tag": "form",
                    "name": "ose_offset_form",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Exchange person"},
                        },
                        {
                            "tag": "select_static",
                            "name": "exchange_person",
                            "placeholder": {"tag": "plain_text", "content": "Select exchange person"},
                            "options": _select_options(exchange_names),
                            "required": True,
                        },
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Shift"},
                        },
                        {
                            "tag": "select_static",
                            "name": "shift_type",
                            "placeholder": {"tag": "plain_text", "content": "N or D"},
                            "options": _select_options(od.OSE_SHIFT_TYPES),
                            "required": True,
                        },
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Original date"},
                        },
                        {
                            "tag": "date_picker",
                            "name": "original_date",
                            "placeholder": {"tag": "plain_text", "content": "Pick original date"},
                            "required": True,
                        },
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Exchange date"},
                        },
                        {
                            "tag": "date_picker",
                            "name": "exchange_date",
                            "placeholder": {"tag": "plain_text", "content": "Pick exchange date"},
                            "required": True,
                        },
                        {
                            "tag": "input",
                            "name": "reason",
                            "input_type": "multiline_text",
                            "rows": 4,
                            "auto_resize": True,
                            "width": "fill",
                            "label": {"tag": "plain_text", "content": "Reason"},
                            "label_position": "top",
                            "placeholder": {"tag": "plain_text", "content": "Reason for offset"},
                            "required": True,
                            "max_length": 1000,
                        },
                        {
                            "tag": "button",
                            "name": "submit_ose_offset",
                            "text": {"tag": "plain_text", "content": "Submit"},
                            "type": "primary",
                            "form_action_type": "submit",
                            "behaviors": [
                                {
                                    "type": "callback",
                                    "value": _callback_payload(
                                        _OFFSET_SUBMIT_KEY,
                                        owner_open_id=owner_open_id,
                                        request_person=request_person,
                                    ),
                                }
                            ],
                        },
                    ],
                },
            ]
        },
    }


def build_leave_form_card(*, owner_open_id: str, request_person: str) -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "OSE leave request"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**Name:** {request_person}\n"
                            "Fill the fields below, then tap **Submit**."
                        ),
                    },
                },
                {
                    "tag": "form",
                    "name": "ose_leave_form",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Leave type"},
                        },
                        {
                            "tag": "select_static",
                            "name": "leave_type",
                            "placeholder": {"tag": "plain_text", "content": "Select leave type"},
                            "options": _select_options(od.OSE_LEAVE_TYPES),
                            "required": True,
                        },
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Start date"},
                        },
                        {
                            "tag": "date_picker",
                            "name": "start_date",
                            "placeholder": {"tag": "plain_text", "content": "Pick start date"},
                            "required": True,
                        },
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "End date"},
                        },
                        {
                            "tag": "date_picker",
                            "name": "end_date",
                            "placeholder": {"tag": "plain_text", "content": "Pick end date"},
                            "required": True,
                        },
                        {
                            "tag": "input",
                            "name": "reason",
                            "input_type": "multiline_text",
                            "rows": 4,
                            "auto_resize": True,
                            "width": "fill",
                            "label": {"tag": "plain_text", "content": "Reason"},
                            "label_position": "top",
                            "placeholder": {"tag": "plain_text", "content": "Reason for leave"},
                            "required": True,
                            "max_length": 1000,
                        },
                        {
                            "tag": "button",
                            "name": "submit_ose_leave",
                            "text": {"tag": "plain_text", "content": "Submit"},
                            "type": "primary",
                            "form_action_type": "submit",
                            "behaviors": [
                                {
                                    "type": "callback",
                                    "value": _callback_payload(
                                        _LEAVE_SUBMIT_KEY,
                                        owner_open_id=owner_open_id,
                                        request_person=request_person,
                                    ),
                                }
                            ],
                        },
                    ],
                },
            ]
        },
    }


def _send_ephemeral_card(
    chat_id: str,
    open_id: str,
    card: dict[str, Any],
    token: str,
) -> None:
    cid = (chat_id or "").strip()
    oid = (open_id or "").strip()
    if not cid or not oid:
        raise ValueError("chat_id and open_id are required for a group-only form")
    url = "https://open.larksuite.com/open-apis/ephemeral/v1/send"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    body = {
        "chat_id": cid,
        "open_id": oid,
        "msg_type": "interactive",
        "card": card,
    }
    res = requests.post(url, headers=headers, json=body, timeout=20).json()
    if res.get("code") != 0:
        raise RuntimeError(f"Failed to send group-only form: {res}")


def _deliver_private_card(
    *,
    owner_open_id: str,
    group_chat_id: str,
    chat_type: Optional[str],
    card: dict[str, Any],
    send_message: Callable[..., dict[str, Any]],
    token: str,
) -> None:
    if (chat_type or "").strip().lower() == "group":
        _send_ephemeral_card(group_chat_id, owner_open_id, card, token)
        return
    card_json = json.dumps(card, ensure_ascii=False)
    resp = send_message(group_chat_id, card_json, msg_type="interactive")
    if isinstance(resp, dict) and int(resp.get("code", -1)) != 0:
        raise RuntimeError(f"Failed to send form: {resp}")


def handle_mention(
    clean_text: str,
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    text = (clean_text or "").strip()
    want_offset = _wants_offset(text)
    want_leave = _wants_leave(text)
    if not want_offset and not want_leave:
        return False
    oid = (sender_open_id or "").strip()
    if not oid:
        send_message(chat_id, "❌ Could not identify your Lark user for a private form.")
        return True
    try:
        token = get_token_func()
        request_person = resolve_request_person(oid, token)
    except Exception as e:
        send_message(chat_id, f"❌ {e}")
        return True
    try:
        if want_offset:
            _deliver_private_card(
                owner_open_id=oid,
                group_chat_id=chat_id,
                chat_type=chat_type,
                card=build_offset_form_card(owner_open_id=oid, request_person=request_person),
                send_message=send_message,
                token=token,
            )
        if want_leave:
            _deliver_private_card(
                owner_open_id=oid,
                group_chat_id=chat_id,
                chat_type=chat_type,
                card=build_leave_form_card(owner_open_id=oid, request_person=request_person),
                send_message=send_message,
                token=token,
            )
    except Exception as e:
        send_message(chat_id, f"❌ Could not open form: {e}")
    return True


def _form_field_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (int, float)):
        return str(v).strip()
    if isinstance(v, list):
        parts = [_form_field_text(x) for x in v]
        return " ".join(p for p in parts if p).strip()
    if isinstance(v, dict):
        for key in ("value", "text", "content", "date"):
            t = _form_field_text(v.get(key))
            if t:
                return t
        for vv in v.values():
            t = _form_field_text(vv)
            if t:
                return t
    return ""


def _get_form_field(action_obj: Any, parsed: Any, event_obj: Any, name: str) -> str:
    if isinstance(action_obj, dict):
        fv = action_obj.get("form_value")
        if isinstance(fv, dict):
            t = _form_field_text(fv.get(name))
            if t:
                return t
    if isinstance(parsed, dict):
        fv = parsed.get("form_value")
        if isinstance(fv, dict):
            t = _form_field_text(fv.get(name))
            if t:
                return t
        t = _form_field_text(parsed.get(name))
        if t:
            return t
    return _find_field_deep(event_obj, name)


def _find_field_deep(obj: Any, name: str) -> str:
    if isinstance(obj, dict):
        if name in obj:
            t = _form_field_text(obj.get(name))
            if t:
                return t
        for vv in obj.values():
            t = _find_field_deep(vv, name)
            if t:
                return t
    elif isinstance(obj, list):
        for it in obj:
            t = _find_field_deep(it, name)
            if t:
                return t
    return ""


def _parse_date_iso(raw: Optional[str]) -> date:
    s = str(raw or "").strip()
    if not s:
        raise ValueError("date is required")
    if re.match(r"^\d{10,13}$", s):
        try:
            ts = int(s)
            if ts > 10**12:
                ts //= 1000
            return datetime.fromtimestamp(ts).date()
        except Exception as e:
            raise ValueError(f"invalid date timestamp {raw!r}") from e
    m = re.match(r"^\s*(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m2 = re.match(r"^\s*(\d{4})/(\d{2})/(\d{2})", s)
    if m2:
        return date(int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
    try:
        return date.fromisoformat(s[:10])
    except ValueError as e:
        raise ValueError(f"invalid date {raw!r} (use YYYY-MM-DD)") from e


def _assert_owner(parsed: dict[str, Any], sender_open_id: str) -> tuple[str, str]:
    owner = str(parsed.get("owner") or "").strip()
    request_person = str(parsed.get("request_person") or "").strip()
    sender = (sender_open_id or "").strip()
    if not owner or owner != sender:
        raise ValueError("This form can only be submitted by the user who opened it.")
    if not request_person:
        raise ValueError("Request person is missing from the form session.")
    return owner, request_person


def handle_card_callback(
    parsed: dict[str, Any],
    event_obj: dict[str, Any],
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., dict[str, Any]],
) -> bool:
    key = str(parsed.get("k") or "").strip().lower()
    if key not in (_OFFSET_SUBMIT_KEY, _LEAVE_SUBMIT_KEY):
        return False
    try:
        _, request_person = _assert_owner(parsed, sender_open_id)
        action = event_obj.get("action") if isinstance(event_obj.get("action"), dict) else {}
        reason = _get_form_field(action, parsed, event_obj, "reason")
        if not reason:
            send_message(chat_id, "❌ Reason is required.")
            return True
        if key == _OFFSET_SUBMIT_KEY:
            exchange_person = _get_form_field(action, parsed, event_obj, "exchange_person")
            shift_type = _get_form_field(action, parsed, event_obj, "shift_type")
            original_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "original_date"))
            exchange_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "exchange_date"))
            if not exchange_person or not shift_type:
                send_message(chat_id, "❌ Please fill Exchange person and Shift.")
                return True
            out = od.submit_ose_offset(
                request_person=request_person,
                exchange_person=exchange_person,
                shift_type=shift_type,
                original_date=original_date,
                exchange_date=exchange_date,
                reason=reason,
            )
            rid = str((out or {}).get("record_id") or "").strip()
            send_message(
                chat_id,
                f"✅ Offset submitted for {request_person} (record {rid or 'saved'}).",
            )
            return True
        leave_type = _get_form_field(action, parsed, event_obj, "leave_type")
        start_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "start_date"))
        end_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "end_date"))
        if not leave_type:
            send_message(chat_id, "❌ Please choose a leave type.")
            return True
        out = od.submit_ose_leave(
            name=request_person,
            leave_type=leave_type,
            start_date=start_date,
            end_date=end_date,
            reason=reason,
        )
        rid = str((out or {}).get("record_id") or "").strip()
        send_message(
            chat_id,
            f"✅ Leave submitted for {request_person} (record {rid or 'saved'}).",
        )
    except Exception as e:
        send_message(chat_id, f"❌ Submit failed: {e}")
    return True
