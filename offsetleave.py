#!/usr/bin/env python3
"""Lark ephemeral group forms for OSE leave / offset (visible only to the requester)."""

from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import date, datetime
from typing import Any, Callable, Optional

import requests

import ose_Duty as od

_CHBOX_DIR = os.path.dirname(os.path.abspath(__file__))
_OFFSET_APPROVER_NOTIFIED_PATH = os.path.join(_CHBOX_DIR, "offset_approver_notified.json")
_OFFSET_APPROVER_NOTIFIED_LOCK = threading.Lock()

# Prevent opening multiple edit forms for the same pending record (double-tap Edit).
_OFFSET_EDIT_OPEN_LOCK = threading.Lock()
_OFFSET_EDIT_OPEN: dict[str, str] = {}

# Prevent double-tap Delete on the same row (duplicate callbacks / impatient clicks).
_OFFSET_DELETE_LOCK = threading.Lock()
_OFFSET_DELETE_IN_FLIGHT: set[str] = set()

# Prevent double-tap Submit creating duplicate Bitable rows (Lark + web).
_OFFSET_SUBMIT_LOCK = threading.Lock()
_OFFSET_SUBMIT_IN_FLIGHT: set[str] = set()
_OFFSET_SUBMIT_RECENT: dict[str, float] = {}
_OFFSET_SUBMIT_DEDUPE_SEC = int(os.getenv("OSE_OFFSET_SUBMIT_DEDUPE_SEC", "60"))

_OFFSET_SUBMIT_KEY = "offsetleave_offset_submit"
_LEAVE_SUBMIT_KEY = "offsetleave_leave_submit"
_OFFSET_APPR_PICK_KEY = "offsetleave_offset_appr_pick"
_OFFSET_APPR_CONFIRM_KEY = "offsetleave_offset_appr_confirm"

# Lark open_ids — each receives the interactive approval card for new offset submissions.
# Keep in sync with ``main.OFFSET_APPROVER_OPEN_IDS``.
OFFSET_APPROVER_OPEN_IDS: frozenset[str] = frozenset(
    {
        "ou_540944d83349cda961ec6124425cdfb4",
        "ou_c4346ace5927c14f51a89b2394b55338",
    }
)

OFFSET_APPROVAL_CALLBACK_KEYS = frozenset({_OFFSET_APPR_PICK_KEY, _OFFSET_APPR_CONFIRM_KEY})

_OFFSET_EDIT_PICK_KEY = "offsetleave_offset_edit_pick"
_OFFSET_EDIT_SUBMIT_KEY = "offsetleave_offset_edit_submit"
_OFFSET_DELETE_KEY = "offsetleave_offset_delete"

OFFSETLEAVE_CARD_CALLBACK_KEYS = frozenset(
    set(OFFSET_APPROVAL_CALLBACK_KEYS)
    | {_OFFSET_EDIT_PICK_KEY, _OFFSET_EDIT_SUBMIT_KEY, _OFFSET_DELETE_KEY}
)


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


def wants_editoffset(text: str) -> bool:
    return bool(re.match(r"^\s*editoffset\s*$", (text or "").strip(), re.I))


def _edit_form_session_key(owner_open_id: str, record_id: str) -> str:
    return f"{(owner_open_id or '').strip()}:{(record_id or '').strip()}"


def _clear_edit_forms_for_owner(owner_open_id: str) -> None:
    oid = (owner_open_id or "").strip()
    if not oid:
        return
    prefix = f"{oid}:"
    with _OFFSET_EDIT_OPEN_LOCK:
        for key in list(_OFFSET_EDIT_OPEN.keys()):
            if key.startswith(prefix):
                _OFFSET_EDIT_OPEN.pop(key, None)


def _is_edit_form_open(owner_open_id: str, record_id: str) -> bool:
    return _edit_form_session_key(owner_open_id, record_id) in _OFFSET_EDIT_OPEN


def _mark_edit_form_open(owner_open_id: str, record_id: str, message_id: str = "") -> None:
    key = _edit_form_session_key(owner_open_id, record_id)
    if not key or key == ":":
        return
    with _OFFSET_EDIT_OPEN_LOCK:
        _OFFSET_EDIT_OPEN[key] = (message_id or "open").strip() or "open"


def _clear_edit_form_open(owner_open_id: str, record_id: str) -> None:
    key = _edit_form_session_key(owner_open_id, record_id)
    with _OFFSET_EDIT_OPEN_LOCK:
        _OFFSET_EDIT_OPEN.pop(key, None)


def _delete_session_key(owner_open_id: str, record_id: str) -> str:
    return f"{(owner_open_id or '').strip()}:{(record_id or '').strip()}"


def _try_begin_offset_delete(owner_open_id: str, record_id: str) -> Optional[str]:
    key = _delete_session_key(owner_open_id, record_id)
    if not key or key == ":":
        return "missing record"
    with _OFFSET_DELETE_LOCK:
        if key in _OFFSET_DELETE_IN_FLIGHT:
            return "Delete already in progress for this request — please wait."
        _OFFSET_DELETE_IN_FLIGHT.add(key)
    return None


def _end_offset_delete(owner_open_id: str, record_id: str) -> None:
    key = _delete_session_key(owner_open_id, record_id)
    with _OFFSET_DELETE_LOCK:
        _OFFSET_DELETE_IN_FLIGHT.discard(key)


def _deliver_requester_offset_edit_menu(
    *,
    owner_open_id: str,
    request_person: str,
    group_chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    token: str,
) -> None:
    """editoffset in group/DM: only this user's pending rows (never approver admin list)."""
    rows = _pending_offsets_for_request_person(request_person)
    if not rows:
        send_message(
            group_chat_id,
            f"No pending offset found for **{request_person}**. "
            "Approved or rejected requests cannot be edited with editoffset.",
        )
        return
    if len(rows) == 1:
        card = build_offset_edit_form_card(
            owner_open_id=owner_open_id,
            request_person=request_person,
            row=rows[0],
            is_admin=False,
        )
        _deliver_private_card(
            owner_open_id=owner_open_id,
            group_chat_id=group_chat_id,
            chat_type=chat_type,
            card=card,
            send_message=send_message,
            token=token,
        )
        rid = str(rows[0].get("record_id") or "").strip()
        if rid:
            _mark_edit_form_open(owner_open_id, rid)
        return
    card = build_offset_edit_list_card(owner_open_id, request_person, rows, is_admin=False)
    _deliver_private_card(
        owner_open_id=owner_open_id,
        group_chat_id=group_chat_id,
        chat_type=chat_type,
        card=card,
        send_message=send_message,
        token=token,
    )


def wants_deleteoffset(text: str) -> bool:
    return bool(re.match(r"^\s*deleteoffset\s*$", (text or "").strip(), re.I))


def wants_pendingoffset(text: str) -> bool:
    return bool(re.match(r"^\s*pendingoffset\s*$", (text or "").strip(), re.I))


def _pending_offsets_for_request_person(request_person: str) -> list[dict[str, Any]]:
    rp = od._title_name(request_person)
    if not rp:
        return []
    data = od.get_ose_offset_records_admin()
    out: list[dict[str, Any]] = []
    for it in (data or {}).get("items") or []:
        if not bool(it.get("pending")):
            continue
        if od._title_name(str(it.get("request_person") or "")) != rp:
            continue
        out.append(dict(it))
    return out


def _non_pending_offsets_all() -> list[dict[str, Any]]:
    """Approved / rejected rows (for approver edit/delete lists)."""
    data = od.get_ose_offset_records_admin()
    out: list[dict[str, Any]] = []
    for it in (data or {}).get("items") or []:
        if bool(it.get("pending")):
            continue
        out.append(dict(it))
    out.sort(
        key=lambda r: (r.get("request_date") or "", r.get("record_id") or ""),
        reverse=True,
    )
    return out


def _all_pending_offsets() -> list[dict[str, Any]]:
    """All pending offset rows (for approver pendingoffset command)."""
    od.invalidate_ose_bitable_cache()
    data = od.get_ose_offset_records_admin()
    out: list[dict[str, Any]] = []
    for it in (data or {}).get("items") or []:
        if bool(it.get("pending")):
            out.append(dict(it))
    out.sort(
        key=lambda r: (
            r.get("request_date") or "",
            r.get("original_date") or "",
            r.get("request_person") or "",
        ),
        reverse=True,
    )
    return out


def _parsed_admin_flag(parsed: dict[str, Any]) -> bool:
    a = parsed.get("admin")
    if a is True:
        return True
    if isinstance(a, (int, float)) and int(a) == 1:
        return True
    return str(a or "").strip().lower() in ("1", "true", "yes")


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
    exchange_names = list(od.ose_offset_form_exchange_names(exclude_person=request_person))
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


def offset_submit_fingerprint(
    *,
    request_person: str,
    exchange_person: str,
    shift_type: str,
    original_date: date,
    exchange_date: date,
    reason: str,
) -> str:
    parts = [
        od._title_name(request_person),
        od._title_name(exchange_person),
        (shift_type or "").strip().upper(),
        original_date.isoformat(),
        exchange_date.isoformat(),
        (reason or "").strip().lower(),
    ]
    return "|".join(parts)


def try_begin_offset_submit(actor_key: str, fingerprint: str) -> Optional[str]:
    """
    Reserve a submit slot for ``actor_key`` (Lark open_id or ``web:{name}``).
    Returns an error message when duplicate / in-flight; ``None`` if OK to proceed.
    """
    actor = (actor_key or "").strip()
    if not actor:
        actor = "unknown"
    fp = (fingerprint or "").strip()
    dedupe_key = f"{actor}:{fp}"
    now = time.monotonic()
    with _OFFSET_SUBMIT_LOCK:
        if actor in _OFFSET_SUBMIT_IN_FLIGHT:
            return "Your offset submit is already processing. Please wait."
        last = _OFFSET_SUBMIT_RECENT.get(dedupe_key, 0.0)
        if fp and now - last < _OFFSET_SUBMIT_DEDUPE_SEC:
            return (
                "Duplicate submit ignored — the same offset was just saved. "
                "Check your pending list or wait a minute before submitting again."
            )
        _OFFSET_SUBMIT_IN_FLIGHT.add(actor)
    return None


def release_offset_submit(actor_key: str, fingerprint: str, *, success: bool) -> None:
    actor = (actor_key or "").strip() or "unknown"
    fp = (fingerprint or "").strip()
    dedupe_key = f"{actor}:{fp}"
    with _OFFSET_SUBMIT_LOCK:
        _OFFSET_SUBMIT_IN_FLIGHT.discard(actor)
        if success and fp:
            _OFFSET_SUBMIT_RECENT[dedupe_key] = time.monotonic()
            stale_before = time.monotonic() - max(_OFFSET_SUBMIT_DEDUPE_SEC * 4, 120)
            for k, ts in list(_OFFSET_SUBMIT_RECENT.items()):
                if ts < stale_before:
                    _OFFSET_SUBMIT_RECENT.pop(k, None)


def build_offset_edit_saved_card(
    *,
    request_person: str,
    row: dict[str, Any],
) -> dict[str, Any]:
    md = _offset_approval_table_md(
        row,
        status=str(row.get("approval_status") or "Pending"),
        intro=f"**{_lark_md_cell(request_person)}** — your pending offset was **saved**.",
    )
    return {
        "schema": "2.0",
        "config": {"width_mode": "fill"},
        "header": {
            "template": "green",
            "title": {"tag": "plain_text", "content": "OSE offset — saved"},
        },
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}]},
    }


def _finish_ephemeral_card_ui(
    *,
    owner_open_id: str,
    chat_id: str,
    card: dict[str, Any],
    message_id: str,
    send_message: Callable[..., Any],
    token: str,
    fallback_text: str,
) -> None:
    """Show result after save/submit — group ephemeral cards often cannot be PATCHed."""
    mid = (message_id or "").strip()
    if _try_patch_interactive_card_message(mid, card):
        return
    _dismiss_ephemeral_form(mid)
    cid = (chat_id or "").strip()
    oid = (owner_open_id or "").strip()
    if cid and oid:
        try:
            _send_ephemeral_card(cid, oid, card, token)
            return
        except Exception as exc:
            print(f"[offsetleave] ephemeral result card failed: {exc!r}", flush=True)
    if cid and fallback_text:
        send_message(chat_id, fallback_text)


def _finish_offset_edit_saved_ui(
    *,
    owner_open_id: str,
    request_person: str,
    row: dict[str, Any],
    message_id: str,
    chat_id: str,
    send_message: Callable[..., Any],
    token: str,
) -> None:
    card = build_offset_edit_saved_card(request_person=request_person, row=row)
    _finish_ephemeral_card_ui(
        owner_open_id=owner_open_id,
        chat_id=chat_id,
        card=card,
        message_id=message_id,
        send_message=send_message,
        token=token,
        fallback_text=f"✅ Offset updated for {request_person}.",
    )


def build_offset_submit_done_card(
    *,
    request_person: str,
    record_id: str,
    message: str,
) -> dict[str, Any]:
    rid = (record_id or "").strip() or "—"
    body = (message or "Offset submitted.").strip()
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "green",
            "title": {"tag": "plain_text", "content": "OSE offset — submitted"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**Request person:** {_lark_md_cell(request_person)}\n\n{body}\n\n**Record:** `{_lark_md_cell(rid)}`",
                    },
                }
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


def _short_cell(s: Any) -> str:
    t = str(s or "").replace("\n", " ").replace("|", "/").strip()
    if len(t) > 240:
        return t[:240] + "…"
    return t or "—"


def _callback_payload_edit_submit(
    *,
    owner_open_id: str,
    record_id: str,
    request_person: str = "",
    admin: bool = False,
) -> dict[str, Any]:
    d: dict[str, Any] = {
        "k": _OFFSET_EDIT_SUBMIT_KEY,
        "owner": (owner_open_id or "").strip(),
        "record_id": (record_id or "").strip(),
    }
    rp = (request_person or "").strip()
    if rp:
        d["request_person"] = rp
    if admin:
        d["admin"] = 1
    return d


def _callback_payload_row_action(
    kind: str,
    *,
    owner_open_id: str,
    request_person: str,
    record_id: str,
    admin: bool = False,
) -> dict[str, Any]:
    d: dict[str, Any] = {
        "k": kind,
        "owner": (owner_open_id or "").strip(),
        "request_person": (request_person or "").strip(),
        "record_id": (record_id or "").strip(),
    }
    if admin:
        d["admin"] = 1
    return d


def _row_datepicker_initial(cell: Any) -> str:
    raw = str(cell or "").strip()
    if not raw:
        return ""
    try:
        return _parse_date_iso(raw).isoformat()
    except Exception:
        return ""


def build_offset_edit_list_card(
    owner_open_id: str,
    request_person: str,
    rows: list[dict[str, Any]],
    *,
    is_admin: bool = False,
) -> dict[str, Any]:
    cap = 15
    sliced = rows[:cap]
    if is_admin:
        intro = "**Approver** — edit **approved** or **rejected** offsets (all requesters)."
        cap_note = "non-pending"
    else:
        intro = (
            f"**{request_person}** — pending offset requests you can **edit**.\n"
            "Approved / rejected rows are not listed."
        )
        cap_note = "pending"
    elements: list[dict[str, Any]] = [{"tag": "div", "text": {"tag": "lark_md", "content": intro}}]
    if len(rows) > cap:
        elements.append(
            {
                "tag": "div",
                "text": {"tag": "plain_text", "content": f"(Showing first {cap} of {len(rows)} {cap_note}.)"},
            }
        )
    for i, r in enumerate(sliced, start=1):
        rid = str(r.get("record_id") or "").strip()
        if not rid:
            continue
        extra = ""
        if is_admin:
            extra = f"\n**Requester:** {_short_cell(r.get('request_person'))} · **Status:** {_short_cell(r.get('approval_status'))}"
        summary = (
            f"**{i}.** {_short_cell(r.get('exchange_person'))} · **{_short_cell(r.get('shift_type'))}** · "
            f"{_short_cell(r.get('original_date'))} → {_short_cell(r.get('exchange_date'))}\n"
            f"**Reason:** {_short_cell(r.get('reason'))}{extra}"
        )
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": summary}})
        elements.append(
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "Edit"},
                "type": "primary",
                "behaviors": [
                    {
                        "type": "callback",
                        "value": _callback_payload_row_action(
                            _OFFSET_EDIT_PICK_KEY,
                            owner_open_id=owner_open_id,
                            request_person=request_person,
                            record_id=rid,
                            admin=is_admin,
                        ),
                    }
                ],
            }
        )
    title = "OSE offset — edit (approver)" if is_admin else "OSE offset — edit"
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "blue", "title": {"tag": "plain_text", "content": title}},
        "body": {"elements": elements},
    }


def build_offset_delete_list_card(
    owner_open_id: str,
    request_person: str,
    rows: list[dict[str, Any]],
    *,
    is_admin: bool = False,
) -> dict[str, Any]:
    cap = 15
    sliced = rows[:cap]
    if is_admin:
        intro = "**Approver** — delete **approved** or **rejected** offsets (removes the Bitable row)."
        cap_note = "non-pending"
    else:
        intro = (
            f"**{request_person}** — pending offset requests you can **delete**.\n"
            "Approved / rejected rows are not listed."
        )
        cap_note = "pending"
    elements: list[dict[str, Any]] = [{"tag": "div", "text": {"tag": "lark_md", "content": intro}}]
    if len(rows) > cap:
        elements.append(
            {
                "tag": "div",
                "text": {"tag": "plain_text", "content": f"(Showing first {cap} of {len(rows)} {cap_note}.)"},
            }
        )
    for i, r in enumerate(sliced, start=1):
        rid = str(r.get("record_id") or "").strip()
        if not rid:
            continue
        extra = ""
        if is_admin:
            extra = f"\n**Requester:** {_short_cell(r.get('request_person'))} · **Status:** {_short_cell(r.get('approval_status'))}"
        summary = (
            f"**{i}.** {_short_cell(r.get('exchange_person'))} · **{_short_cell(r.get('shift_type'))}** · "
            f"{_short_cell(r.get('original_date'))} → {_short_cell(r.get('exchange_date'))}\n"
            f"**Reason:** {_short_cell(r.get('reason'))}{extra}"
        )
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": summary}})
        elements.append(
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "Delete"},
                "type": "danger",
                "behaviors": [
                    {
                        "type": "callback",
                        "value": _callback_payload_row_action(
                            _OFFSET_DELETE_KEY,
                            owner_open_id=owner_open_id,
                            request_person=request_person,
                            record_id=rid,
                            admin=is_admin,
                        ),
                    }
                ],
            }
        )
    title = "OSE offset — delete (approver)" if is_admin else "OSE offset — delete"
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "orange", "title": {"tag": "plain_text", "content": title}},
        "body": {"elements": elements},
    }


def build_offset_pending_list_card(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Approver view of all pending offsets (read + Approve/Reject per row)."""
    cap = 12
    total = len(rows)
    sliced = rows[:cap]
    intro = (
        f"**{total} pending offset request(s)** awaiting approval.\n"
        "Tap **Approve** or **Reject** on a row, add optional **Remarks**, then **Confirm**."
    )
    elements: list[dict[str, Any]] = [{"tag": "div", "text": {"tag": "lark_md", "content": intro}}]
    if total > cap:
        elements.append(
            {
                "tag": "motion",
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "plain_text",
                            "content": f"(Showing first {cap} of {total} pending — run pendingoffset again after clearing some.)",
                        },
                    }
                ],
            }
        )
    if not sliced:
        elements.append(
            {
                "tag": "div",
                "text": {"tag": "plain_text", "content": "No pending offset requests right now."},
            }
        )
    for i, r in enumerate(sliced, start=1):
        rid = str(r.get("record_id") or "").strip()
        if not rid:
            continue
        rp = _short_cell(r.get("request_person"))
        summary = (
            f"**{i}. {rp}** · {_short_cell(r.get('exchange_person'))} · "
            f"**{_short_cell(r.get('shift_type'))}** · "
            f"{_short_cell(r.get('original_date'))} → {_short_cell(r.get('exchange_date'))}\n"
            f"**Req. date:** {_short_cell(r.get('request_date'))} · **Reason:** {_short_cell(r.get('reason'))}"
        )
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": summary}})
        approve_val = {"k": _OFFSET_APPR_PICK_KEY, "record_id": rid, "decision": "Approved"}
        reject_val = {"k": _OFFSET_APPR_PICK_KEY, "record_id": rid, "decision": "Rejected"}
        elements.append(_approval_pick_button_row(approve_val, reject_val))
        elements.append({"tag": "hr"})
    if elements and elements[-1].get("tag") == "hr":
        elements.pop()
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "wathet",
            "title": {"tag": "plain_text", "content": "OSE offset — pending queue"},
        },
        "body": {"elements": elements},
    }


def build_offset_edit_form_card(
    *,
    owner_open_id: str,
    request_person: str,
    row: dict[str, Any],
    is_admin: bool = False,
) -> dict[str, Any]:
    rid = str(row.get("record_id") or "").strip()
    req_on_row = str(row.get("request_person") or request_person or "").strip()
    exchange_names = list(od.ose_offset_form_exchange_names(exclude_person=req_on_row))
    o_ini = _row_datepicker_initial(row.get("original_date"))
    x_ini = _row_datepicker_initial(row.get("exchange_date"))
    original_dp: dict[str, Any] = {
        "tag": "date_picker",
        "name": "original_date",
        "placeholder": {"tag": "plain_text", "content": "Pick original date"},
        "required": True,
    }
    if o_ini:
        original_dp["initial_date"] = o_ini
    exchange_dp: dict[str, Any] = {
        "tag": "date_picker",
        "name": "exchange_date",
        "placeholder": {"tag": "plain_text", "content": "Pick exchange date"},
        "required": True,
    }
    if x_ini:
        exchange_dp["initial_date"] = x_ini
    status_line = ""
    if is_admin:
        status_line = f"\n**Status:** {_short_cell(row.get('approval_status'))}"
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "OSE offset — edit request (approver)" if is_admin else "OSE offset — edit request"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**Request person:** {req_on_row}\n"
                            f"**Record:** `{_short_cell(rid)}`{status_line}\n"
                            "Update the fields below, then tap **Save**."
                        ),
                    },
                },
                {
                    "tag": "form",
                    "name": "ose_offset_edit_form",
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
                        original_dp,
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Exchange date"},
                        },
                        exchange_dp,
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
                            "name": "submit_ose_offset_edit",
                            "text": {"tag": "plain_text", "content": "Save"},
                            "type": "primary",
                            "form_action_type": "submit",
                            "behaviors": [
                                {
                                    "type": "callback",
                                    "value": _callback_payload_edit_submit(
                                        owner_open_id=owner_open_id,
                                        record_id=rid,
                                        request_person=req_on_row,
                                        admin=is_admin,
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


def handle_editoffset_command(
    clean_text: str,
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    if not wants_editoffset(clean_text):
        return False
    oid = (sender_open_id or "").strip()
    if not oid:
        send_message(chat_id, "❌ Could not identify your Lark user.")
        return True
    try:
        token = get_token_func()
        _clear_edit_forms_for_owner(oid)
        request_person = resolve_request_person(oid, token)
        _deliver_requester_offset_edit_menu(
            owner_open_id=oid,
            request_person=request_person,
            group_chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            token=token,
        )
    except Exception as e:
        send_message(chat_id, f"❌ editoffset: {e}")
    return True


def handle_deleteoffset_command(
    clean_text: str,
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    if not wants_deleteoffset(clean_text):
        return False
    oid = (sender_open_id or "").strip()
    if not oid:
        send_message(chat_id, "❌ Could not identify your Lark user.")
        return True
    try:
        token = get_token_func()
        if _is_offset_approver_open_id(oid):
            request_person = resolve_request_person(oid, token)
            own_pending = _pending_offsets_for_request_person(request_person)
            if own_pending:
                card = build_offset_delete_list_card(oid, request_person, own_pending, is_admin=False)
            else:
                rows = _non_pending_offsets_all()
                if not rows:
                    send_message(
                        chat_id,
                        "No approved or rejected offset records found to delete, and you have no pending requests as requester.",
                    )
                    return True
                card = build_offset_delete_list_card(oid, "", rows, is_admin=True)
        else:
            request_person = resolve_request_person(oid, token)
            rows = _pending_offsets_for_request_person(request_person)
            if not rows:
                send_message(
                    chat_id,
                    "No offset found that you requested (no pending rows). "
                    "Already approved or rejected requests are removed with the approver deleteoffset list.",
                )
                return True
            card = build_offset_delete_list_card(oid, request_person, rows, is_admin=False)
        _deliver_private_card(
            owner_open_id=oid,
            group_chat_id=chat_id,
            chat_type=chat_type,
            card=card,
            send_message=send_message,
            token=token,
        )
    except Exception as e:
        send_message(chat_id, f"❌ deleteoffset: {e}")
    return True


def handle_pendingoffset_command(
    clean_text: str,
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    if not wants_pendingoffset(clean_text):
        return False
    oid = (sender_open_id or "").strip()
    if not oid:
        send_message(chat_id, "❌ Could not identify your Lark user.")
        return True
    if not _is_offset_approver_open_id(oid):
        send_message(chat_id, "❌ **pendingoffset** is for configured offset approvers only.")
        return True
    try:
        token = get_token_func()
        rows = _all_pending_offsets()
        if not rows:
            send_message(chat_id, "✅ No pending offset requests — the queue is empty.")
            return True
        card = build_offset_pending_list_card(rows)
        _deliver_private_card(
            owner_open_id=oid,
            group_chat_id=chat_id,
            chat_type=chat_type,
            card=card,
            send_message=send_message,
            token=token,
        )
    except Exception as e:
        send_message(chat_id, f"❌ pendingoffset: {e}")
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


def _offset_approval_table_md(
    row: dict[str, Any],
    *,
    status: str,
    intro: Optional[str] = None,
) -> str:
    rd = _lark_md_cell(row.get("request_date"))
    rp = _lark_md_cell(row.get("request_person"))
    ex = _lark_md_cell(row.get("exchange_person"))
    sh = _lark_md_cell(row.get("shift_type"))
    od_ = _lark_md_cell(row.get("original_date"))
    xd = _lark_md_cell(row.get("exchange_date"))
    rs = _lark_md_cell(row.get("reason"))
    st = _lark_md_cell(status)
    intro_line = intro or (
        "**Someone submitted an offset record.** Review the table, tap **Approve** or **Reject**, "
        "then optional **Remarks**, then **Confirm**."
    )
    lines = [
        intro_line,
        "",
        "| REQ. DATE | REQUEST PERSON | EXCHANGE PERSON | SHIFT | ORIGINAL DATE | EXCHANGE DATE | REASON | STATUS |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
        f"| {rd} | {rp} | {ex} | {sh} | {od_} | {xd} | {rs} | {st} |",
        "",
    ]
    return "\n".join(lines)


def _lookup_offset_row(record_id: str, *, bust_cache: bool = False) -> Optional[dict[str, Any]]:
    """Find offset row by Bitable record_id; optionally force a fresh Bitable read."""
    rid = (record_id or "").strip()
    if not rid:
        return None

    def _scan() -> Optional[dict[str, Any]]:
        for it in (od.get_ose_offset_records_admin().get("items") or []):
            if str(it.get("record_id") or "").strip() == rid:
                return dict(it)
        return None

    if bust_cache:
        od.invalidate_ose_bitable_cache()
    row = _scan()
    if row is not None:
        return row
    if not bust_cache:
        od.invalidate_ose_bitable_cache()
        return _scan()
    return None


def _offset_admin_row_by_id(record_id: str) -> dict[str, Any]:
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("missing record_id")
    row = _lookup_offset_row(rid, bust_cache=True)
    if row is None:
        raise KeyError(f"offset record {rid!r}")
    return row


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


def _try_patch_interactive_card_message(message_id: str, card: dict[str, Any]) -> bool:
    """PATCH card in place. Ephemeral group cards often cannot be patched (returns False)."""
    mid = (message_id or "").strip()
    if not mid:
        return False
    token = od.get_tenant_access_token()
    url = f"https://open.larksuite.com/open-apis/im/v1/messages/{mid}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    payload = {"msg_type": "interactive", "content": json.dumps(card, ensure_ascii=False)}
    last_res: dict[str, Any] = {}
    for id_type in ("open_message_id", ""):
        params = {"message_id_type": id_type} if id_type else {}
        last_res = requests.patch(
            url,
            headers=headers,
            params=params,
            json=payload,
            timeout=25,
        ).json()
        if int(last_res.get("code", -1)) == 0:
            return True
    print(f"[offsetleave] patch card skipped for {mid!r}: {last_res!r}", flush=True)
    return False


def _patch_interactive_card_message(message_id: str, card: dict[str, Any]) -> None:
    if not _try_patch_interactive_card_message(message_id, card):
        raise RuntimeError(f"patch card failed for message_id={message_id!r}")


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


def build_offset_requester_responded_card(
    row: dict[str, Any],
    *,
    approver_name: str,
    decision: str,
    remarks: str,
) -> dict[str, Any]:
    """Read-only message card for the requester (same layout style as approver done card, picture 2)."""
    dec = (decision or "").strip().title()
    an = _lark_md_cell(approver_name)
    rd = _lark_md_cell(row.get("request_date"))
    rp = _lark_md_cell(row.get("request_person"))
    ex = _lark_md_cell(row.get("exchange_person"))
    sh = _lark_md_cell(row.get("shift_type"))
    od_ = _lark_md_cell(row.get("original_date"))
    xd = _lark_md_cell(row.get("exchange_date"))
    rs = _lark_md_cell(row.get("reason"))
    st = _lark_md_cell((row.get("approval_status") or dec or "").strip() or dec)
    rr = (remarks or "").strip()
    remark_line = f"**Remarks:** {_lark_md_cell(rr) if rr else '—'}"
    md = "\n".join(
        [
            f"**{an}** already responded to your offset request (**{dec}**).",
            "",
            "| REQ. DATE | REQUEST PERSON | EXCHANGE PERSON | SHIFT | ORIGINAL DATE | EXCHANGE DATE | REASON | STATUS |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
            f"| {rd} | {rp} | {ex} | {sh} | {od_} | {xd} | {rs} | {st} |",
            "",
            remark_line,
        ]
    )
    tpl = "green" if dec == "Approved" else "red"
    return {
        "schema": "2.0",
        "config": {"width_mode": "fill"},
        "header": {"template": tpl, "title": {"tag": "plain_text", "content": f"OSE offset — {dec}"}},
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}]},
    }


def build_offset_requester_deleted_notify_card(
    row: dict[str, Any],
    *,
    requester_name: str,
) -> dict[str, Any]:
    """Read-only card for approvers when a requester deletes a pending offset."""
    rn = _lark_md_cell(requester_name)
    intro = (
        f"**{rn}** **deleted** a **pending** offset request. "
        "No approval action is needed — the request was withdrawn and removed from the table."
    )
    md = _offset_approval_table_md(row, status="Withdrawn", intro=intro)
    return {
        "schema": "2.0",
        "config": {"width_mode": "fill"},
        "header": {
            "template": "grey",
            "title": {"tag": "plain_text", "content": "OSE offset — pending request deleted"},
        },
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}]},
    }


def build_offset_requester_edited_notify_card(
    row: dict[str, Any],
    *,
    requester_name: str,
) -> dict[str, Any]:
    """Interactive card for approvers when a requester updates a pending offset (re-approve/reject)."""
    rid = str(row.get("record_id") or "").strip()
    rn = _lark_md_cell(requester_name)
    intro = (
        f"**{rn}** updated a **pending** offset request. Please **review again** — "
        "tap **Approve** or **Reject**, then optional **Remarks**, then **Confirm**."
    )
    md = _offset_approval_table_md(row, status="Pending", intro=intro)
    approve_val = {"k": _OFFSET_APPR_PICK_KEY, "record_id": rid, "decision": "Approved"}
    reject_val = {"k": _OFFSET_APPR_PICK_KEY, "record_id": rid, "decision": "Rejected"}
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "wathet",
            "title": {"tag": "plain_text", "content": "OSE offset — request updated (re-review)"},
        },
        "body": {
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": md}},
                _approval_pick_button_row(approve_val, reject_val),
            ]
        },
    }


def build_offset_other_approver_responded_card(
    row: dict[str, Any],
    *,
    approver_name: str,
    decision: str,
    remarks: str,
) -> dict[str, Any]:
    """Read-only card for the other approver(s) after one peer has confirmed."""
    dec = (decision or "").strip().title()
    verb = "approved" if dec == "Approved" else "rejected"
    an = _lark_md_cell(approver_name)
    rd = _lark_md_cell(row.get("request_date"))
    rp = _lark_md_cell(row.get("request_person"))
    ex = _lark_md_cell(row.get("exchange_person"))
    sh = _lark_md_cell(row.get("shift_type"))
    od_ = _lark_md_cell(row.get("original_date"))
    xd = _lark_md_cell(row.get("exchange_date"))
    rs = _lark_md_cell(row.get("reason"))
    st = _lark_md_cell((row.get("approval_status") or dec or "").strip() or dec)
    rr = (remarks or "").strip()
    remark_line = f"**Remarks:** {_lark_md_cell(rr) if rr else '—'}"
    md = "\n".join(
        [
            f"**{an}** already **{verb}** this offset request. No further action is needed on your pending card.",
            "",
            "| REQ. DATE | REQUEST PERSON | EXCHANGE PERSON | SHIFT | ORIGINAL DATE | EXCHANGE DATE | REASON | STATUS |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
            f"| {rd} | {rp} | {ex} | {sh} | {od_} | {xd} | {rs} | {st} |",
            "",
            remark_line,
        ]
    )
    tpl = "green" if dec == "Approved" else "red"
    return {
        "schema": "2.0",
        "config": {"width_mode": "fill"},
        "header": {"template": tpl, "title": {"tag": "plain_text", "content": f"OSE offset — {dec} (by {approver_name})"}},
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


def _is_offset_approver_open_id(open_id: str) -> bool:
    return (open_id or "").strip() in OFFSET_APPROVER_OPEN_IDS


def _lark_im_send_message(
    receive_id: str,
    text: str,
    msg_type: str = "text",
    mentions: Any = None,
    receive_id_type: str = "chat_id",
) -> dict[str, Any]:
    """Send Lark IM when ``main.send_message`` is unavailable (e.g. web offset submit)."""
    token = od.get_tenant_access_token()
    url = "https://open.larksuite.com/open-apis/im/v1/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if msg_type == "interactive":
        content = text if isinstance(text, str) else json.dumps(text)
    else:
        content = json.dumps({"text": text})
    body: dict[str, Any] = {
        "receive_id": receive_id,
        "msg_type": msg_type,
        "content": content,
    }
    if mentions:
        body["mentions"] = mentions
    rid_type = (receive_id_type or "chat_id").strip() or "chat_id"
    response = requests.post(
        url,
        headers=headers,
        params={"receive_id_type": rid_type},
        json=body,
        timeout=30,
    )
    return response.json()


def _notify_offset_approver_pending(send_message: Callable[..., Any], row: dict[str, Any]) -> None:
    if not OFFSET_APPROVER_OPEN_IDS:
        return
    card = build_offset_approver_initial_card(row)
    body = json.dumps(card, ensure_ascii=False)
    for oid in OFFSET_APPROVER_OPEN_IDS:
        aid = (oid or "").strip()
        if not aid:
            continue
        r = send_message(aid, body, msg_type="interactive", receive_id_type="open_id")
        if isinstance(r, dict) and int(r.get("code", -1)) != 0:
            print(f"[offsetleave] approver DM failed for {aid!r}: {r!r}", flush=True)


def notify_offset_approvers_for_record(
    record_id: str,
    *,
    send_message: Optional[Callable[..., Any]] = None,
    row: Optional[dict[str, Any]] = None,
    fallback_row: Optional[dict[str, Any]] = None,
) -> None:
    """DM pending-approval cards to all configured approvers (Lark bot or web submit)."""
    rid = (record_id or "").strip()
    if not rid:
        return
    send = send_message or _lark_im_send_message
    resolved = row
    if resolved is None:
        try:
            resolved = _offset_admin_row_by_id(rid)
        except Exception:
            if fallback_row:
                resolved = fallback_row
            else:
                try:
                    resolved = od.get_ose_offset_record_admin_row(rid)
                except Exception as exc:
                    print(f"[offsetleave] approver notify: no row for {rid!r}: {exc!r}", flush=True)
                    return
    _notify_offset_approver_pending(send, resolved)
    _mark_offset_record_notified(rid)


def _load_notified_offset_record_ids_unlocked() -> set[str]:
    try:
        with open(_OFFSET_APPROVER_NOTIFIED_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return set()
    except Exception:
        return set()
    ids = data.get("record_ids") if isinstance(data, dict) else data
    if not isinstance(ids, list):
        return set()
    return {str(x).strip() for x in ids if str(x).strip()}


def _load_notified_offset_record_ids() -> set[str]:
    with _OFFSET_APPROVER_NOTIFIED_LOCK:
        return _load_notified_offset_record_ids_unlocked()


def _mark_offset_record_notified(record_id: str) -> None:
    rid = (record_id or "").strip()
    if not rid:
        return
    with _OFFSET_APPROVER_NOTIFIED_LOCK:
        known = _load_notified_offset_record_ids_unlocked()
        if rid in known:
            return
        known.add(rid)
        tmp = f"{_OFFSET_APPROVER_NOTIFIED_PATH}.tmp"
        payload = {"record_ids": sorted(known)}
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, _OFFSET_APPROVER_NOTIFIED_PATH)


def _unmark_offset_record_notified(record_id: str) -> None:
    """Drop record from poll dedupe file after requester deletes a pending row."""
    rid = (record_id or "").strip()
    if not rid:
        return
    with _OFFSET_APPROVER_NOTIFIED_LOCK:
        known = _load_notified_offset_record_ids_unlocked()
        if rid not in known:
            return
        known.discard(rid)
        tmp = f"{_OFFSET_APPROVER_NOTIFIED_PATH}.tmp"
        payload = {"record_ids": sorted(known)}
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, _OFFSET_APPROVER_NOTIFIED_PATH)


def _offset_row_ready_for_approver_notify(row: dict[str, Any]) -> bool:
    if not bool(row.get("pending")):
        return False
    if not str(row.get("request_person") or "").strip():
        return False
    if not str(row.get("exchange_person") or "").strip():
        return False
    return True


def scan_bitable_pending_offsets_for_approver_notify() -> dict[str, int]:
    """
    Find pending offset rows added directly in Bitable (not via submit_ose_offset)
    and DM approvers. Uses ``offset_approver_notified.json`` to avoid duplicate cards.
    """
    od.invalidate_ose_bitable_cache()
    items = (od.get_ose_offset_records_admin() or {}).get("items") or []
    notified_ids = _load_notified_offset_record_ids()
    sent = 0
    for row in items:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("record_id") or "").strip()
        if not rid or rid in notified_ids:
            continue
        if not _offset_row_ready_for_approver_notify(row):
            continue
        try:
            notify_offset_approvers_for_record(rid, row=dict(row))
            sent += 1
            notified_ids.add(rid)
        except Exception as exc:
            print(f"[offsetleave] bitable scan notify failed for {rid!r}: {exc!r}", flush=True)
    return {"scanned": len(items), "notified": sent}


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
    card = build_offset_requester_responded_card(
        row,
        approver_name=approver_name,
        decision=decision,
        remarks=remarks,
    )
    body = json.dumps(card, ensure_ascii=False)
    r = send_message(oid, body, msg_type="interactive", receive_id_type="open_id")
    if isinstance(r, dict) and int(r.get("code", -1)) != 0:
        print(f"[offsetleave] requester DM failed: {r!r}", flush=True)


def _notify_offset_approvers_requester_edited(
    send_message: Callable[..., Any],
    row: dict[str, Any],
    *,
    requester_name: str,
) -> None:
    card = build_offset_requester_edited_notify_card(row, requester_name=requester_name)
    body = json.dumps(card, ensure_ascii=False)
    for oid in OFFSET_APPROVER_OPEN_IDS:
        aid = (oid or "").strip()
        if not aid:
            continue
        r = send_message(aid, body, msg_type="interactive", receive_id_type="open_id")
        if isinstance(r, dict) and int(r.get("code", -1)) != 0:
            print(f"[offsetleave] requester-edit notify failed for {aid!r}: {r!r}", flush=True)


def _notify_offset_approvers_requester_deleted(
    send_message: Callable[..., Any],
    row: dict[str, Any],
    *,
    requester_name: str,
) -> None:
    if not OFFSET_APPROVER_OPEN_IDS:
        return
    card = build_offset_requester_deleted_notify_card(row, requester_name=requester_name)
    body = json.dumps(card, ensure_ascii=False)
    for oid in OFFSET_APPROVER_OPEN_IDS:
        aid = (oid or "").strip()
        if not aid:
            continue
        r = send_message(aid, body, msg_type="interactive", receive_id_type="open_id")
        if isinstance(r, dict) and int(r.get("code", -1)) != 0:
            print(f"[offsetleave] requester-delete notify failed for {aid!r}: {r!r}", flush=True)


def _notify_other_offset_approvers_responded(
    send_message: Callable[..., Any],
    row: dict[str, Any],
    *,
    acting_approver_open_id: str,
    approver_name: str,
    decision: str,
    remarks: str,
) -> None:
    actor = (acting_approver_open_id or "").strip()
    card = build_offset_other_approver_responded_card(
        row,
        approver_name=approver_name,
        decision=decision,
        remarks=remarks,
    )
    body = json.dumps(card, ensure_ascii=False)
    for oid in OFFSET_APPROVER_OPEN_IDS:
        aid = (oid or "").strip()
        if not aid or aid == actor:
            continue
        r = send_message(aid, body, msg_type="interactive", receive_id_type="open_id")
        if isinstance(r, dict) and int(r.get("code", -1)) != 0:
            print(f"[offsetleave] peer approver DM failed for {aid!r}: {r!r}", flush=True)


def _assert_offset_card_actor(
    parsed: dict[str, Any],
    sender_open_id: str,
    token: str,
) -> tuple[str, Optional[str], bool]:
    """Return (owner_open_id, request_person_roster_or_None, is_approver_admin_flow)."""
    owner = str(parsed.get("owner") or "").strip()
    sender = (sender_open_id or "").strip()
    if not owner or owner != sender:
        raise ValueError("This action is only for the user who opened the menu.")
    if _parsed_admin_flag(parsed):
        if not _is_offset_approver_open_id(sender):
            raise ValueError("Only configured offset approvers can use this admin action.")
        return owner, None, True
    rp_val = str(parsed.get("request_person") or "").strip()
    if not rp_val:
        raise ValueError("Missing session.")
    rp_live = resolve_request_person(sender, token)
    if od._title_name(rp_live) != od._title_name(rp_val):
        raise ValueError("Identity mismatch for this action.")
    return owner, rp_live, False


def _build_offset_edit_approver_empty_patch_card() -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "grey", "title": {"tag": "plain_text", "content": "OSE offset — edit (approver)"}},
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "plain_text",
                        "content": "No approved or rejected offset records left to edit.",
                    },
                }
            ]
        },
    }


def _build_offset_delete_approver_empty_patch_card() -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "grey", "title": {"tag": "plain_text", "content": "OSE offset — delete (approver)"}},
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "plain_text",
                        "content": "No approved or rejected offset records left to delete.",
                    },
                }
            ]
        },
    }


def _build_offset_edit_empty_patch_card(request_person: str) -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "grey", "title": {"tag": "plain_text", "content": "OSE offset — edit"}},
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "plain_text",
                        "content": f"No pending offset found that you requested ({request_person}).",
                    },
                }
            ]
        },
    }


def _build_offset_delete_empty_patch_card(request_person: str) -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "grey", "title": {"tag": "plain_text", "content": "OSE offset — delete"}},
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "plain_text",
                        "content": f"No pending offset requests left to delete ({request_person}).",
                    },
                }
            ]
        },
    }


def _patch_my_offset_list_after_change(
    *,
    message_id: str,
    mode: str,
    owner_open_id: str,
    request_person: str,
) -> None:
    mid = (message_id or "").strip()
    if not mid:
        return
    m = (mode or "").strip().lower()
    if m == "edit_admin":
        rows = _non_pending_offsets_all()
        card = (
            build_offset_edit_list_card(owner_open_id, "", rows, is_admin=True)
            if rows
            else _build_offset_edit_approver_empty_patch_card()
        )
    elif m == "delete_admin":
        rows = _non_pending_offsets_all()
        card = (
            build_offset_delete_list_card(owner_open_id, "", rows, is_admin=True)
            if rows
            else _build_offset_delete_approver_empty_patch_card()
        )
    elif m == "edit":
        rows = _pending_offsets_for_request_person(request_person)
        card = (
            build_offset_edit_list_card(owner_open_id, request_person, rows, is_admin=False)
            if rows
            else _build_offset_edit_empty_patch_card(request_person)
        )
    elif m == "delete":
        rows = _pending_offsets_for_request_person(request_person)
        card = (
            build_offset_delete_list_card(owner_open_id, request_person, rows, is_admin=False)
            if rows
            else _build_offset_delete_empty_patch_card(request_person)
        )
    else:
        return
    if not _try_patch_interactive_card_message(mid, card):
        print(f"[offsetleave] list refresh patch skipped ({m})", flush=True)


def _handle_offset_edit_pick(
    parsed: dict[str, Any],
    event_obj: dict[str, Any],
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., Any],
    webhook_data: Optional[dict[str, Any]],
) -> bool:
    cid = (chat_id or "").strip()
    mid = _event_message_id(event_obj, webhook_data)
    try:
        token = od.get_tenant_access_token()
        owner, rp_live, is_admin = _assert_offset_card_actor(parsed, sender_open_id, token)
        rid = str(parsed.get("record_id") or "").strip()
        if not rid:
            raise ValueError("missing record id")
        if not is_admin and _is_edit_form_open(owner, rid):
            _toast_approval_problem(
                send_message,
                cid,
                "You already opened the edit form for this request. "
                "Finish **Save** on that form, or run **editoffset** again to start over.",
            )
            return True
        row = _offset_admin_row_by_id(rid)
        if is_admin:
            if not mid:
                raise ValueError("missing message id")
            if bool(row.get("pending")):
                raise ValueError(
                    "This record is still pending. Approver editoffset is for approved/rejected rows only; "
                    "use your normal editoffset as the requester for pending items."
                )
            req_disp = str(row.get("request_person") or "").strip()
            form_card = build_offset_edit_form_card(
                owner_open_id=owner,
                request_person=req_disp,
                row=row,
                is_admin=True,
            )
            if not _try_patch_interactive_card_message(mid, form_card):
                if cid:
                    _send_ephemeral_card(cid, owner, form_card, token)
                else:
                    raise ValueError("Could not open edit form — run editoffset again.")
        else:
            if not bool(row.get("pending")):
                raise ValueError("That request is no longer pending (already approved or rejected).")
            if od._title_name(str(row.get("request_person") or "")) != od._title_name(rp_live or ""):
                raise ValueError("That offset is not yours to edit.")
            form_card = build_offset_edit_form_card(
                owner_open_id=owner,
                request_person=rp_live or "",
                row=row,
                is_admin=False,
            )
            opened = False
            if mid and _try_patch_interactive_card_message(mid, form_card):
                _mark_edit_form_open(owner, rid, mid)
                opened = True
            elif cid:
                _send_ephemeral_card(cid, owner, form_card, token)
                _mark_edit_form_open(owner, rid)
                opened = True
            if not opened:
                raise ValueError("Could not open edit form — run editoffset again.")
    except Exception as e:
        if cid:
            send_message(chat_id, f"❌ {e}")
        else:
            print(f"[offsetleave] edit pick: {e!r}", flush=True)
    return True


def _handle_offset_edit_submit(
    parsed: dict[str, Any],
    event_obj: dict[str, Any],
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., Any],
    webhook_data: Optional[dict[str, Any]],
) -> bool:
    cid = (chat_id or "").strip()
    mid = _event_message_id(event_obj, webhook_data)
    try:
        token = od.get_tenant_access_token()
        owner, rp_live, is_admin = _assert_offset_card_actor(parsed, sender_open_id, token)
        rid = str(parsed.get("record_id") or "").strip()
        if not rid:
            raise ValueError("missing record id")
        action = event_obj.get("action") if isinstance(event_obj.get("action"), dict) else {}
        exchange_person = _get_form_field(action, parsed, event_obj, "exchange_person")
        shift_type = _get_form_field(action, parsed, event_obj, "shift_type")
        original_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "original_date"))
        exchange_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "exchange_date"))
        reason = _get_form_field(action, parsed, event_obj, "reason")
        if not reason or not exchange_person or not shift_type:
            raise ValueError("Please fill Exchange person, Shift, dates, and Reason.")
        if is_admin:
            row_chk = _offset_admin_row_by_id(rid)
            if bool(row_chk.get("pending")):
                raise ValueError("Cannot use approver save on a pending row.")
            od.update_ose_offset_record_fields(
                record_id=rid,
                exchange_person=exchange_person,
                shift_type=shift_type,
                original_date=original_date,
                exchange_date=exchange_date,
                reason=reason,
            )
            _clear_edit_form_open(owner, rid)
            fresh_admin = _offset_admin_row_by_id(rid)
            _finish_offset_edit_saved_ui(
                owner_open_id=owner,
                request_person=str(fresh_admin.get("request_person") or ""),
                row=fresh_admin,
                message_id=mid,
                chat_id=cid,
                send_message=send_message,
                token=token,
            )
        else:
            row_chk = _offset_admin_row_by_id(rid)
            if not bool(row_chk.get("pending")):
                raise ValueError(
                    "This request is no longer pending (already approved or rejected). "
                    "Run editoffset again to refresh the list."
                )
            if od._title_name(str(row_chk.get("request_person") or "")) != od._title_name(rp_live or ""):
                raise ValueError("Not your request to edit.")
            od.update_ose_offset_request(
                record_id=rid,
                request_person=rp_live or "",
                exchange_person=exchange_person,
                shift_type=shift_type,
                original_date=original_date,
                exchange_date=exchange_date,
                reason=reason,
            )
            _clear_edit_form_open(owner, rid)
            fresh = _offset_admin_row_by_id(rid)
            try:
                _notify_offset_approvers_requester_edited(
                    send_message,
                    fresh,
                    requester_name=rp_live or str(fresh.get("request_person") or ""),
                )
            except Exception as exc:
                print(f"[offsetleave] requester-edit approver notify failed: {exc!r}", flush=True)
            _finish_offset_edit_saved_ui(
                owner_open_id=owner,
                request_person=rp_live or str(fresh.get("request_person") or ""),
                row=fresh,
                message_id=mid,
                chat_id=cid,
                send_message=send_message,
                token=token,
            )
    except Exception as e:
        if cid:
            send_message(chat_id, f"❌ Save failed: {e}")
        else:
            print(f"[offsetleave] edit submit: {e!r}", flush=True)
    return True


def _handle_offset_delete_row(
    parsed: dict[str, Any],
    event_obj: dict[str, Any],
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., Any],
    webhook_data: Optional[dict[str, Any]],
) -> bool:
    cid = (chat_id or "").strip()
    mid = _event_message_id(event_obj, webhook_data)
    owner = ""
    rid = ""
    try:
        token = od.get_tenant_access_token()
        owner, rp_live, is_admin = _assert_offset_card_actor(parsed, sender_open_id, token)
        rid = str(parsed.get("record_id") or "").strip()
        if not rid:
            raise ValueError("missing record id")
        busy = _try_begin_offset_delete(owner, rid)
        if busy:
            _toast_approval_problem(send_message, cid, f"⏳ {busy}")
            return True
        row_chk = _lookup_offset_row(rid, bust_cache=True)
        if row_chk is None:
            raise ValueError(
                "This record was already deleted or is no longer in the table. "
                "Run **deleteoffset** to refresh the list."
            )
        if is_admin:
            if bool(row_chk.get("pending")):
                raise ValueError(
                    "Cannot delete a pending row from the approver list. "
                    "The requester should use deleteoffset for pending requests."
                )
        else:
            if not bool(row_chk.get("pending")):
                raise ValueError(
                    "This request is no longer pending (already approved or rejected). "
                    "Run deleteoffset again to refresh the list."
                )
            if od._title_name(str(row_chk.get("request_person") or "")) != od._title_name(rp_live or ""):
                raise ValueError("Not your request to delete.")
        try:
            od.delete_ose_offset_record(record_id=rid)
        except RuntimeError as exc:
            err = str(exc).lower()
            if "not found" in err or "record not exist" in err or "125404" in err:
                od.invalidate_ose_bitable_cache()
            else:
                raise
        od.invalidate_ose_bitable_cache()
        if is_admin:
            rows = _non_pending_offsets_all()
            card = (
                build_offset_delete_list_card(owner, "", rows, is_admin=True)
                if rows
                else _build_offset_delete_approver_empty_patch_card()
            )
            fallback = "✅ Offset record deleted."
        else:
            rp = rp_live or str(row_chk.get("request_person") or "")
            deleted_snapshot = dict(row_chk)
            try:
                _notify_offset_approvers_requester_deleted(
                    send_message,
                    deleted_snapshot,
                    requester_name=rp,
                )
                _unmark_offset_record_notified(rid)
            except Exception as exc:
                print(f"[offsetleave] requester-delete approver notify failed: {exc!r}", flush=True)
            rows = _pending_offsets_for_request_person(rp)
            card = (
                build_offset_delete_list_card(owner, rp, rows, is_admin=False)
                if rows
                else _build_offset_delete_empty_patch_card(rp)
            )
            fallback = f"✅ Deleted pending offset for {rp}."
        _finish_ephemeral_card_ui(
            owner_open_id=owner,
            chat_id=cid,
            card=card,
            message_id=mid,
            send_message=send_message,
            token=token,
            fallback_text=fallback,
        )
    except KeyError:
        _toast_approval_problem(
            send_message,
            cid,
            "❌ This record is no longer available (may already be deleted). Run **deleteoffset** again.",
        )
    except Exception as e:
        if cid:
            send_message(chat_id, f"❌ Delete failed: {e}")
        else:
            print(f"[offsetleave] delete: {e!r}", flush=True)
    finally:
        if owner and rid:
            _end_offset_delete(owner, rid)
    return True


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
    mid = _event_message_id(event_obj, webhook_data)
    if not OFFSET_APPROVER_OPEN_IDS or not _is_offset_approver_open_id(operator):
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
            try:
                _notify_other_offset_approvers_responded(
                    send_message,
                    fresh,
                    acting_approver_open_id=operator,
                    approver_name=approver_name,
                    decision=dec,
                    remarks=remarks,
                )
            except Exception as exc:
                print(f"[offsetleave] peer approver notify failed: {exc!r}", flush=True)
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
    if key == _OFFSET_EDIT_PICK_KEY:
        return _handle_offset_edit_pick(
            parsed,
            event_obj,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            send_message=send_message,
            webhook_data=webhook_data,
        )
    if key == _OFFSET_EDIT_SUBMIT_KEY:
        return _handle_offset_edit_submit(
            parsed,
            event_obj,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            send_message=send_message,
            webhook_data=webhook_data,
        )
    if key == _OFFSET_DELETE_KEY:
        return _handle_offset_delete_row(
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
        owner, request_person = _assert_owner(parsed, sender_open_id)
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
            fp = offset_submit_fingerprint(
                request_person=request_person,
                exchange_person=exchange_person,
                shift_type=shift_type,
                original_date=original_date,
                exchange_date=exchange_date,
                reason=reason,
            )
            dup_err = try_begin_offset_submit(owner, fp)
            mid = _event_message_id(event_obj, webhook_data)
            if dup_err:
                _toast_approval_problem(send_message, cid, f"❌ {dup_err}")
                if mid:
                    _try_patch_interactive_card_message(
                        mid,
                        build_offset_submit_done_card(
                            request_person=request_person,
                            record_id="",
                            message=dup_err,
                        ),
                    )
                return True
            _dismiss_ephemeral_form(mid)
            try:
                out = od.submit_ose_offset(
                    request_person=request_person,
                    exchange_person=exchange_person,
                    shift_type=shift_type,
                    original_date=original_date,
                    exchange_date=exchange_date,
                    reason=reason,
                )
                rid = str((out or {}).get("record_id") or "").strip()
                release_offset_submit(owner, fp, success=True)
                done_msg = f"✅ Offset submitted for {request_person} (record {rid or 'saved'})."
                done_card = build_offset_submit_done_card(
                    request_person=request_person,
                    record_id=rid,
                    message=done_msg,
                )
                _finish_ephemeral_card_ui(
                    owner_open_id=owner,
                    chat_id=cid,
                    card=done_card,
                    message_id=mid,
                    send_message=send_message,
                    token=od.get_tenant_access_token(),
                    fallback_text=done_msg,
                )
            except Exception as submit_exc:
                release_offset_submit(owner, fp, success=False)
                raise submit_exc
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
