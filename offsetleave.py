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
_OFFSET_APPR_PICK_KEY = "offsetleave_offset_appr_pick"
_OFFSET_APPR_CONFIRM_KEY = "offsetleave_offset_appr_confirm"

# Lark open_id — receives an interactive **message card** to approve / reject offset requests.
OFFSET_APPROVER_OPEN_ID = "ou_5f660c0fb0769d184aca635d02209272"
OFFSET_APPROVAL_CALLBACK_KEYS = frozenset({_OFFSET_APPR_PICK_KEY, _OFFSET_APPR_CONFIRM_KEY})


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


def wants_offset_request(text: str) -> bool:
    return _wants_offset(text)


def handle_showoffset(
    clean_text: str,
    *,
    chat_id: str,
    send_message: Callable[..., dict[str, Any]],
) -> bool:
    try:
        target = od.parse_showoffset_command(clean_text)
    except ValueError as exc:
        send_message(chat_id, f"❌ {exc}")
        return True
    if target is None:
        return False
    year, month = target
    try:
        card = od.build_ose_showoffset_card(year, month)
        send_message(chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive")
    except Exception as exc:
        send_message(chat_id, f"❌ showoffset failed: {exc}")
    return True


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


def _event_message_id(event_obj: Any, webhook_data: Any = None) -> str:
    for source in (event_obj, webhook_data):
        if not isinstance(source, dict):
            continue
        ctx = source.get("context")
        if isinstance(ctx, dict):
            for key in ("open_message_id", "message_id"):
                mid = str(ctx.get(key) or "").strip()
                if mid:
                    return mid
        act = source.get("action")
        if isinstance(act, dict):
            for key in ("open_message_id", "message_id"):
                mid = str(act.get(key) or "").strip()
                if mid:
                    return mid
            act_ctx = act.get("context")
            if isinstance(act_ctx, dict):
                for key in ("open_message_id", "message_id"):
                    mid = str(act_ctx.get(key) or "").strip()
                    if mid:
                        return mid
        for key in ("open_message_id", "message_id"):
            mid = str(source.get(key) or "").strip()
            if mid:
                return mid
    if isinstance(webhook_data, dict):
        ev = webhook_data.get("event")
        if isinstance(ev, dict) and ev is not event_obj:
            return _event_message_id(ev)
    return ""


def _dismiss_ephemeral_form(message_id: str) -> None:
    mid = (message_id or "").strip()
    if not mid:
        print("[offsetleave] dismiss skipped: missing ephemeral message_id", flush=True)
        return
    try:
        token = od.get_tenant_access_token()
    except Exception as exc:
        print(f"[offsetleave] dismiss ephemeral form token error: {exc!r}", flush=True)
        return
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    try:
        res = requests.post(
            "https://open.larksuite.com/open-apis/ephemeral/v1/delete",
            headers=headers,
            json={"message_id": mid},
            timeout=20,
        ).json()
        code = int(res.get("code", -1))
        if code == 0:
            print(f"[offsetleave] dismissed ephemeral form {mid}", flush=True)
            return
        if code == 18051:
            return
        print(f"[offsetleave] ephemeral delete failed: {res!r}", flush=True)
        fallback = requests.delete(
            f"https://open.larksuite.com/open-apis/im/v1/messages/{mid}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=20,
        ).json()
        if int(fallback.get("code", -1)) != 0:
            print(f"[offsetleave] im delete fallback failed: {fallback!r}", flush=True)
    except Exception as exc:
        print(f"[offsetleave] dismiss ephemeral form error: {exc!r}", flush=True)


def _operator_open_id(event_obj: dict[str, Any], fallback: str) -> str:
    op = event_obj.get("operator") if isinstance(event_obj.get("operator"), dict) else {}
    oid = (op.get("open_id") or "").strip()
    if oid:
        return oid
    return (fallback or "").strip()


def _lark_md_cell(s: Any) -> str:
    t = str(s if s is not None else "").replace("\n", " ").replace("|", "/").strip()
    if len(t) > 900:
        return t[:900] + "…"
    return t or "—"


def _offset_approval_table_md(row: dict[str, Any], *, status: str) -> str:
    rd = _lark_md_cell(row.get("request_date"))
    rp = _lark_md_cell(row.get("request_person"))
    ex = _lark_md_cell(row.get("exchange_person"))
    sh = _lark_md_cell(row.get("shift_type"))
    od_ = _lark_md_cell(row.get("original_date"))
    xd = _lark_md_cell(row.get("exchange_date"))
    rs = _lark_md_cell(row.get("reason"))
    st = _lark_md_cell(status)
    lines = [
        "**Someone submitted an offset record.** Review the table, tap **Approve** or **Reject**, "
        "then optional **Remarks**, then **Confirm**.",
        "",
        "| REQ. DATE | REQUEST PERSON | EXCHANGE PERSON | SHIFT | ORIGINAL DATE | EXCHANGE DATE | REASON | STATUS |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
        f"| {rd} | {rp} | {ex} | {sh} | {od_} | {xd} | {rs} | {st} |",
        "",
    ]
    return "\n".join(lines)


def _offset_admin_row_by_id(record_id: str) -> dict[str, Any]:
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("missing record_id")
    data = od.get_ose_offset_records_admin()
    for it in (data or {}).get("items") or []:
        if str(it.get("record_id") or "").strip() == rid:
            return dict(it)
    raise KeyError(f"offset record {rid!r}")


def _local_pending_offset_row(
    *,
    record_id: str,
    request_person: str,
    exchange_person: str,
    shift_type: str,
    original_date: date,
    exchange_date: date,
    reason: str,
) -> dict[str, Any]:
    today = date.today()
    return {
        "record_id": record_id,
        "request_id": "",
        "request_date": od._format_yyyymmdd(today),
        "request_person": od._title_name(request_person),
        "exchange_person": od._title_name(exchange_person),
        "shift_type": (shift_type or "").strip().upper(),
        "original_date": od._format_yyyymmdd(original_date),
        "exchange_date": od._format_yyyymmdd(exchange_date),
        "reason": (reason or "").strip(),
        "approval_status": "Pending",
    }


def _patch_interactive_card_message(message_id: str, card: dict[str, Any]) -> None:
    mid = (message_id or "").strip()
    if not mid:
        raise ValueError("message_id required to patch card")
    token = od.get_tenant_access_token()
    url = f"https://open.larksuite.com/open-apis/im/v1/messages/{mid}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    payload = {"msg_type": "interactive", "content": json.dumps(card, ensure_ascii=False)}
    res = requests.patch(
        url,
        headers=headers,
        params={"message_id_type": "open_message_id"},
        json=payload,
        timeout=25,
    ).json()
    if int(res.get("code", -1)) != 0:
        raise RuntimeError(f"patch card failed: {res}")


def _approval_pick_button_row(approve_val: dict[str, Any], reject_val: dict[str, Any]) -> dict[str, Any]:
    def _btn(label: str, typ: str, val: dict[str, Any]) -> dict[str, Any]:
        return {
            "tag": "button",
            "text": {"tag": "plain_text", "content": label},
            "type": typ,
            "behaviors": [{"type": "callback", "value": val}],
        }

    return {
        "tag": "column_set",
        "flex_mode": "flow",
        "background_style": "default",
        "horizontal_spacing": "8px",
        "columns": [
            {
                "tag": "column",
                "width": "auto",
                "weight": 1,
                "vertical_align": "top",
                "elements": [_btn("Approve", "primary", approve_val)],
            },
            {
                "tag": "column",
                "width": "auto",
                "weight": 1,
                "vertical_align": "top",
                "elements": [_btn("Reject", "danger", reject_val)],
            },
        ],
    }


def build_offset_approver_initial_card(row: dict[str, Any]) -> dict[str, Any]:
    rid = str(row.get("record_id") or "").strip()
    st = (str(row.get("approval_status") or "").strip() or "Pending")
    md = _offset_approval_table_md(row, status=st)
    approve_val = {"k": _OFFSET_APPR_PICK_KEY, "record_id": rid, "decision": "Approved"}
    reject_val = {"k": _OFFSET_APPR_PICK_KEY, "record_id": rid, "decision": "Rejected"}
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "wathet",
            "title": {"tag": "plain_text", "content": "OSE offset — pending approval"},
        },
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}, _approval_pick_button_row(approve_val, reject_val)]},
    }


def build_offset_approver_confirm_card(row: dict[str, Any], decision: str) -> dict[str, Any]:
    rid = str(row.get("record_id") or "").strip()
    dec = (decision or "").strip().title()
    st_show = f"{dec} (pending Confirm)"
    md = _offset_approval_table_md(row, status=st_show)
    confirm_val = {"k": _OFFSET_APPR_CONFIRM_KEY, "record_id": rid, "decision": dec}
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "wathet",
            "title": {"tag": "plain_text", "content": "OSE offset — confirm approval"},
        },
        "body": {
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": md}},
                {
                    "tag": "form",
                    "name": "ose_offset_approval_confirm",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": (
                                    f"You selected **{dec}**. Add optional **Remarks** below, then tap **Confirm**."
                                ),
                            },
                        },
                        {
                            "tag": "input",
                            "name": "approval_remarks",
                            "input_type": "multiline_text",
                            "rows": 2,
                            "auto_resize": True,
                            "width": "fill",
                            "label": {"tag": "plain_text", "content": "Remarks (optional)"},
                            "label_position": "top",
                            "placeholder": {"tag": "plain_text", "content": "Optional remarks"},
                            "required": False,
                            "max_length": 1000,
                        },
                        {
                            "tag": "button",
                            "name": "confirm_offset_approval",
                            "text": {"tag": "plain_text", "content": "Confirm"},
                            "type": "primary",
                            "form_action_type": "submit",
                            "behaviors": [{"type": "callback", "value": confirm_val}],
                        },
                    ],
                },
            ]
        },
    }


def build_offset_approver_done_card(row: dict[str, Any], decision: str, remarks: str) -> dict[str, Any]:
    dec = (decision or "").strip().title()
    rr = (remarks or "").strip()
    extra = f"\n**Remarks:** {_lark_md_cell(rr) if rr else '—'}"
    md = _offset_approval_table_md(row, status=dec) + extra
    tpl = "green" if dec == "Approved" else "red"
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": tpl, "title": {"tag": "plain_text", "content": f"OSE offset — {dec}"}},
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}]},
    }


def _build_offset_approval_denied_card() -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "red", "title": {"tag": "plain_text", "content": "Offset approval"}},
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {"tag": "plain_text", "content": "Not authorized — only the assigned approver can use this card."},
                }
            ]
        },
    }


def _build_offset_approval_error_card(message: str) -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "red", "title": {"tag": "plain_text", "content": "Offset approval error"}},
        "body": {"elements": [{"tag": "div", "text": {"tag": "plain_text", "content": _lark_md_cell(message)}}]},
    }


def _norm_offset_decision(raw: Any) -> str:
    t = str(raw or "").strip().lower()
    if t in ("approved", "approve"):
        return "Approved"
    if t in ("rejected", "reject"):
        return "Rejected"
    raise ValueError(f"invalid decision {raw!r}")


def _approver_display_for_bitable(operator_open_id: str) -> str:
    token = od.get_tenant_access_token()
    display = _fetch_user_display_name(operator_open_id, token)
    roster = _match_roster_name(display) if display else None
    if roster:
        return roster
    if display:
        return od._title_name(display)
    return "Approver"


def _notify_offset_approver_pending(send_message: Callable[..., Any], row: dict[str, Any]) -> None:
    oid = (OFFSET_APPROVER_OPEN_ID or "").strip()
    if not oid:
        return
    card = build_offset_approver_initial_card(row)
    body = json.dumps(card, ensure_ascii=False)
    r = send_message(oid, body, msg_type="interactive", receive_id_type="open_id")
    if isinstance(r, dict) and int(r.get("code", -1)) != 0:
        raise RuntimeError(str(r))


def _toast_approval_problem(send_message: Callable[..., Any], chat_id: str, text: str) -> None:
    cid = (chat_id or "").strip()
    if cid:
        try:
            send_message(cid, text)
        except Exception:
            print(f"[offsetleave] approval notify failed: {text}", flush=True)
    else:
        print(f"[offsetleave] approval: {text}", flush=True)


def _requester_open_id_for_offset_row(request_person: str) -> str:
    nm = (request_person or "").strip()
    if not nm:
        return ""
    token = od.get_tenant_access_token()
    idx = od._get_ose_person_open_id_index(token)
    return (od._lookup_person_open_id(nm, idx) or "").strip()


def _format_requester_offset_responded_text(
    *,
    approver_name: str,
    decision: str,
    row: dict[str, Any],
    remarks: str,
) -> str:
    dec = (decision or "").strip().title()
    rmk = (remarks or "").strip()
    lines = [
        f"{approver_name} already responded to your offset request ({dec}).",
        "",
        "Details:",
        f"• REQ. DATE: {row.get('request_date') or '—'}",
        f"• REQUEST PERSON: {row.get('request_person') or '—'}",
        f"• EXCHANGE PERSON: {row.get('exchange_person') or '—'}",
        f"• SHIFT: {row.get('shift_type') or '—'}",
        f"• ORIGINAL DATE: {row.get('original_date') or '—'}",
        f"• EXCHANGE DATE: {row.get('exchange_date') or '—'}",
        f"• REASON: {(row.get('reason') or '').strip() or '—'}",
        f"• STATUS: {(row.get('approval_status') or dec).strip() or dec}",
    ]
    if rmk:
        lines.append(f"• APPROVER REMARKS: {rmk}")
    return "\n".join(lines)


def _notify_requester_offset_responded(
    send_message: Callable[..., Any],
    row: dict[str, Any],
    *,
    approver_name: str,
    decision: str,
    remarks: str,
) -> None:
    request_person = str(row.get("request_person") or "").strip()
    if not request_person:
        return
    oid = _requester_open_id_for_offset_row(request_person)
    if not oid:
        print(f"[offsetleave] could not resolve Lark open_id for requester {request_person!r}", flush=True)
        return
    text = _format_requester_offset_responded_text(
        approver_name=approver_name,
        decision=decision,
        row=row,
        remarks=remarks,
    )
    r = send_message(oid, text, receive_id_type="open_id")
    if isinstance(r, dict) and int(r.get("code", -1)) != 0:
        print(f"[offsetleave] requester DM failed: {r!r}", flush=True)


def _handle_offset_approval_callback(
    parsed: dict[str, Any],
    event_obj: dict[str, Any],
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., Any],
    webhook_data: Optional[dict[str, Any]],
) -> bool:
    key = str(parsed.get("k") or "").strip().lower()
    operator = _operator_open_id(event_obj, sender_open_id)
    approver_oid = (OFFSET_APPROVER_OPEN_ID or "").strip()
    mid = _event_message_id(event_obj, webhook_data)
    if not approver_oid or operator != approver_oid:
        try:
            if mid:
                _patch_interactive_card_message(mid, _build_offset_approval_denied_card())
        except Exception:
            pass
        _toast_approval_problem(send_message, chat_id, "❌ Only the assigned approver can act on this card.")
        return True
    try:
        if key == _OFFSET_APPR_PICK_KEY:
            rid = str(parsed.get("record_id") or "").strip()
            dec = _norm_offset_decision(parsed.get("decision"))
            if not rid:
                raise ValueError("missing record_id")
            row = _offset_admin_row_by_id(rid)
            if not mid:
                raise ValueError("missing message id for card update")
            _patch_interactive_card_message(mid, build_offset_approver_confirm_card(row, dec))
            return True
        if key == _OFFSET_APPR_CONFIRM_KEY:
            rid = str(parsed.get("record_id") or "").strip()
            dec = _norm_offset_decision(parsed.get("decision"))
            action = event_obj.get("action") if isinstance(event_obj.get("action"), dict) else {}
            remarks = _get_form_field(action, parsed, event_obj, "approval_remarks")
            if not rid:
                raise ValueError("missing record_id")
            _offset_admin_row_by_id(rid)
            approver_name = _approver_display_for_bitable(operator)
            od.update_ose_offset_approval(
                record_id=rid,
                status=dec,
                approver=approver_name,
                remarks=remarks,
                approver_open_id=operator,
            )
            fresh = _offset_admin_row_by_id(rid)
            if mid:
                _patch_interactive_card_message(mid, build_offset_approver_done_card(fresh, dec, remarks))
            try:
                _notify_requester_offset_responded(
                    send_message,
                    fresh,
                    approver_name=approver_name,
                    decision=dec,
                    remarks=remarks,
                )
            except Exception as exc:
                print(f"[offsetleave] requester notify failed: {exc!r}", flush=True)
            return True
    except Exception as exc:
        try:
            if mid:
                _patch_interactive_card_message(mid, _build_offset_approval_error_card(str(exc)))
        except Exception:
            pass
        _toast_approval_problem(send_message, chat_id, f"❌ Offset approval failed: {exc}")
    return True


def handle_card_callback(
    parsed: dict[str, Any],
    event_obj: dict[str, Any],
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., dict[str, Any]],
    webhook_data: Optional[dict[str, Any]] = None,
) -> bool:
    key = str(parsed.get("k") or "").strip().lower()
    if key in OFFSET_APPROVAL_CALLBACK_KEYS:
        return _handle_offset_approval_callback(
            parsed,
            event_obj,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            send_message=send_message,
            webhook_data=webhook_data,
        )
    if key not in (_OFFSET_SUBMIT_KEY, _LEAVE_SUBMIT_KEY):
        return False
    cid = (chat_id or "").strip()
    try:
        _, request_person = _assert_owner(parsed, sender_open_id)
        action = event_obj.get("action") if isinstance(event_obj.get("action"), dict) else {}
        reason = _get_form_field(action, parsed, event_obj, "reason")
        if not reason:
            if cid:
                send_message(chat_id, "❌ Reason is required.")
            return True
        if key == _OFFSET_SUBMIT_KEY:
            exchange_person = _get_form_field(action, parsed, event_obj, "exchange_person")
            shift_type = _get_form_field(action, parsed, event_obj, "shift_type")
            original_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "original_date"))
            exchange_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "exchange_date"))
            if not exchange_person or not shift_type:
                if cid:
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
            _dismiss_ephemeral_form(_event_message_id(event_obj, webhook_data))
            if cid:
                send_message(
                    chat_id,
                    f"✅ Offset submitted for {request_person} (record {rid or 'saved'}).",
                )
            if rid:
                try:
                    try:
                        row = _offset_admin_row_by_id(rid)
                    except Exception:
                        row = _local_pending_offset_row(
                            record_id=rid,
                            request_person=request_person,
                            exchange_person=exchange_person,
                            shift_type=shift_type,
                            original_date=original_date,
                            exchange_date=exchange_date,
                            reason=reason,
                        )
                    _notify_offset_approver_pending(send_message, row)
                except Exception as exc:
                    print(f"[offsetleave] approver DM failed: {exc!r}", flush=True)
            return True
        leave_type = _get_form_field(action, parsed, event_obj, "leave_type")
        start_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "start_date"))
        end_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "end_date"))
        if not leave_type:
            if cid:
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
        _dismiss_ephemeral_form(_event_message_id(event_obj, webhook_data))
        if cid:
            send_message(
                chat_id,
                f"✅ Leave submitted for {request_person} (record {rid or 'saved'}).",
            )
    except Exception as e:
        if cid:
            send_message(chat_id, f"❌ Submit failed: {e}")
        else:
            print(f"[offsetleave] submit failed (no chat_id): {e!r}", flush=True)
    return True
