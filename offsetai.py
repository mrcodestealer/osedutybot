"""
offsetai — LLM agent for OSE offset (shift swap) with **tool calling only**.

No intent rules / regex routing. The model reads the user message + sheet context,
chooses tools, and the code executes them (read sheet, send reply, open cards, submit).

Requires LLM API (``BOT_USE_AI=1`` + API key). Without LLM, returns False so other
handlers can run.

Toggle: ``BOT_USE_OFFSETAI=0`` disables entirely.

CLI:
    python offsetai.py "show me man chung offset which is approved"
    python offsetai.py --dry "i want to delete one of those"
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Optional

import ose_Duty as od

# Minimal gate: might be offset (not intent). Slash commands stay in offsetleave.
_OFFSET_TOPIC_RE = re.compile(
    r"(?i)\b("
    r"offset|swap\s+(?:my\s+)?(?:duty|shift|roster)|exchange\s+(?:my\s+)?(?:duty|shift)|"
    r"调休|换班|duty\s+swap|shift\s+swap"
    r")\b"
)
_SLASH_OFFSET_RE = re.compile(
    r"(?i)^(?:/)?(?:showoffset|deleteoffset|editoffset|pendingoffset)\b"
)
_MAX_ROWS_FOR_LLM = 80
_AGENT_MAX_TURNS = int(os.getenv("OSE_OFFSET_AGENT_MAX_TURNS", "6"))
_AGENT_SESSION_TTL_SEC = int(os.getenv("OSE_OFFSET_AGENT_TTL_SEC", "1800"))
_AGENT_LOCK = threading.Lock()
_AGENT_SESSIONS: dict[str, list[dict[str, Any]]] = {}

_AGENT_SYSTEM = (
    "You are the OSE Duty Bot offset assistant on Lark/Feishu.\n"
    "Users talk naturally about duty shift swaps (offset): lookups, submit, delete, edit.\n"
    "You MUST use the provided tools — do not invent sheet data.\n"
    "Today's date and a sheet snapshot are in the user context.\n\n"
    "How to work:\n"
    "1. Understand what the user wants (even messy / unexpected phrasing).\n"
    "2. Call tools to read data or perform actions.\n"
    "3. For lookups: call list_offset_records with filters YOU infer, then send_chat_reply "
    "with a clear summary (only matching rows).\n"
    "4. To let user pick a row to delete: call show_delete_picker with the SAME filters "
    "(person, status, year, month) — never show the whole sheet unless they asked.\n"
    "5. To submit a new offset: gather fields from conversation; if anything missing, "
    "send_chat_reply asking ONE question; when complete call submit_offset_record.\n"
    "6. If the message is clearly NOT about OSE offset/swap, call pass_not_offset.\n"
    "7. Approvers can delete any status; requesters only delete their own pending rows.\n"
    "8. inferred_filters in context are AI-extracted from the user message — "
    "you MUST pass them to list_offset_records / show_delete_picker "
    "(person, status, person_role, year, month).\n"
    "9. When user names someone (e.g. Man Chung) use person_role=requester unless they "
    "ask about swap partner.\n"
    "Roster names: use exact names from exchange_roster when submitting.\n"
    "Shift type: D (day) or N (night).\n"
    "Dates: YYYY-MM-DD.\n"
)

_TOOL_SPECS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "pass_not_offset",
            "description": "User message is NOT about OSE duty offset/swap. Hand off to other bot handlers.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_offset_records",
            "description": (
                "Read offset sheet rows with optional filters. "
                "Returns JSON {count, rows} for you to summarize or plan next action."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "person": {
                        "type": "string",
                        "description": "Roster name, e.g. Man Chung",
                    },
                    "person_role": {
                        "type": "string",
                        "enum": ["requester", "any"],
                        "description": "requester=only their requests; any=also when they are exchange person",
                    },
                    "status": {
                        "type": "string",
                        "enum": ["approved", "pending", "rejected"],
                    },
                    "year": {"type": "integer"},
                    "month": {"type": "integer", "minimum": 1, "maximum": 12},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "send_chat_reply",
            "description": "Send the final markdown reply to the user in Lark chat.",
            "parameters": {
                "type": "object",
                "properties": {
                    "text": {"type": "string", "description": "Markdown message for the user"},
                },
                "required": ["text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "show_delete_picker",
            "description": (
                "Open interactive card so user can pick one offset row to delete. "
                "Apply filters so the list matches what the user asked for."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "person": {"type": "string"},
                    "person_role": {
                        "type": "string",
                        "enum": ["requester", "any"],
                    },
                    "status": {
                        "type": "string",
                        "enum": ["approved", "pending", "rejected"],
                    },
                    "year": {"type": "integer"},
                    "month": {"type": "integer", "minimum": 1, "maximum": 12},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "show_offset_calendar",
            "description": "Show monthly offset calendar card.",
            "parameters": {
                "type": "object",
                "properties": {
                    "year": {"type": "integer"},
                    "month": {"type": "integer", "minimum": 1, "maximum": 12},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "open_offset_form",
            "description": "Open the full offset submit form card (when user prefers UI).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "submit_offset_record",
            "description": (
                "Submit a new offset request to Bitable when all fields are known. "
                "Validate with user first if unsure."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "exchange_person": {"type": "string"},
                    "original_date": {
                        "type": "string",
                        "description": "YYYY-MM-DD — user's duty day to swap out",
                    },
                    "exchange_date": {
                        "type": "string",
                        "description": "YYYY-MM-DD — day swapping to",
                    },
                    "shift_type": {"type": "string", "enum": ["D", "N"]},
                    "reason": {"type": "string"},
                },
                "required": [
                    "exchange_person",
                    "original_date",
                    "exchange_date",
                    "shift_type",
                    "reason",
                ],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "show_edit_picker",
            "description": "Open card to edit pending offset rows.",
            "parameters": {
                "type": "object",
                "properties": {
                    "person": {"type": "string"},
                    "year": {"type": "integer"},
                    "month": {"type": "integer", "minimum": 1, "maximum": 12},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "show_pending_queue",
            "description": "Approver: show all pending offset requests awaiting approval.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
]


def is_enabled() -> bool:
    explicit = (os.getenv("BOT_USE_OFFSETAI") or "").strip().lower()
    if explicit in ("0", "false", "no", "off"):
        return False
    if explicit in ("1", "true", "yes", "on"):
        return True
    inherited = (os.getenv("BOT_USE_AI") or "").strip().lower()
    return inherited in ("1", "true", "yes", "on")


def _llm_available() -> bool:
    try:
        import chatagent as ca

        return ca.llm_available()
    except Exception:
        return False


def looks_like_offset_topic(text: str) -> bool:
    """Cheap pre-filter only — NOT intent routing."""
    raw = (text or "").strip()
    if not raw or raw.lstrip().startswith("/"):
        return False
    if _SLASH_OFFSET_RE.match(raw):
        return False
    if not _OFFSET_TOPIC_RE.search(raw):
        return False
    try:
        import offsetleave as ol

        if ol._LEAVE_QUERY_RE.search(raw) and not re.search(
            r"(?i)\boffset\b", raw
        ):
            return False
    except Exception:
        pass
    return True


def _agent_session_key(session_key: str) -> str:
    return f"offset_agent:{(session_key or '').strip()}"


def _agent_history_get(session_key: str) -> list[dict[str, Any]]:
    key = _agent_session_key(session_key)
    now = time.monotonic()
    with _AGENT_LOCK:
        ent = _AGENT_SESSIONS.get(key)
        if not ent:
            return []
        if now - float(ent.get("ts") or 0) > _AGENT_SESSION_TTL_SEC:
            _AGENT_SESSIONS.pop(key, None)
            return []
        return list(ent.get("messages") or [])


def _agent_history_append(session_key: str, user_text: str, assistant_note: str) -> None:
    key = _agent_session_key(session_key)
    with _AGENT_LOCK:
        ent = _AGENT_SESSIONS.get(key) or {"messages": [], "ts": time.monotonic()}
        msgs = list(ent.get("messages") or [])
        msgs.append({"role": "user", "content": user_text})
        if assistant_note:
            msgs.append({"role": "assistant", "content": assistant_note})
        while len(msgs) > 20:
            msgs.pop(0)
        _AGENT_SESSIONS[key] = {"messages": msgs, "ts": time.monotonic()}


def _row_status_bucket(row: dict[str, Any]) -> str:
    if bool(row.get("pending")):
        return "pending"
    st = str(row.get("approval_status") or row.get("status") or "").lower()
    if "reject" in st:
        return "rejected"
    if "approv" in st:
        return "approved"
    return st or "unknown"


def _admin_rows() -> list[dict[str, Any]]:
    try:
        data = od.get_ose_offset_records_admin() or {}
        return list((data.get("items") or []))[:_MAX_ROWS_FOR_LLM]
    except Exception:
        return []


def _resolve_person_filter(person: Optional[str]) -> list[str]:
    person = (person or "").strip()
    if not person:
        return []
    key = od._resolve_ose_roster_key(person)
    return [key or od._title_name(person)]


def _same_person(a: str, b: str) -> bool:
    a = (a or "").strip()
    b = (b or "").strip()
    if not a or not b:
        return False
    if od._names_same_person(a, b):
        return True
    ak = od._resolve_ose_roster_key(a) or od._title_name(a)
    bk = od._resolve_ose_roster_key(b) or od._title_name(b)
    return bool(ak and bk and od._names_same_person(ak, bk))


def _row_matches_person(
    row: dict[str, Any],
    person_filters: list[str],
    *,
    person_role: str = "any",
) -> bool:
    if not person_filters:
        return True
    req = str(row.get("request_person") or "")
    exc = str(row.get("exchange_person") or "")
    role = (person_role or "any").strip().lower()
    if role == "requester":
        return any(_same_person(req, pf) for pf in person_filters)
    return any(
        _same_person(req, pf) or _same_person(exc, pf) for pf in person_filters
    )


def filter_rows_by_args(
    rows: list[dict[str, Any]],
    *,
    person: Optional[str] = None,
    status: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    person_role: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Filter sheet rows using explicit arguments (from LLM tools, not regex)."""
    status_f = (status or "").strip().lower() or None
    if status_f not in ("approved", "rejected", "pending"):
        status_f = None
    person_filters = _resolve_person_filter(person)
    role = (person_role or "any").strip().lower()
    if role not in ("requester", "any"):
        role = "any"
    filtered: list[dict[str, Any]] = []
    for row in rows:
        if status_f and _row_status_bucket(row) != status_f:
            continue
        if year is not None and month is not None:
            if not any(
                (d := od._parse_date_value(row.get(key)))
                and d.year == int(year)
                and d.month == int(month)
                for key in ("original_date", "exchange_date", "request_date")
            ):
                continue
        if person_filters and not _row_matches_person(
            row, person_filters, person_role=role
        ):
            continue
        filtered.append(row)
    return filtered


# Back-compat for offsetleave delete picker
def filter_offset_rows(
    rows: list[dict[str, Any]],
    text: str,
    *,
    person_filter: Optional[str] = None,
    status_filter: Optional[str] = None,
    month_target: Optional[tuple[int, int]] = None,
) -> list[dict[str, Any]]:
    y, m = (month_target[0], month_target[1]) if month_target else (None, None)
    return filter_rows_by_args(
        rows,
        person=person_filter,
        status=status_filter,
        year=y,
        month=m,
    )


def _slim_rows(rows: list[dict[str, Any]], *, limit: int = 30) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows[:limit]:
        out.append(
            {
                "record_id": str(r.get("record_id") or ""),
                "request_person": str(r.get("request_person") or ""),
                "exchange_person": str(r.get("exchange_person") or ""),
                "original_date": str(r.get("original_date") or ""),
                "exchange_date": str(r.get("exchange_date") or ""),
                "shift_type": str(r.get("shift_type") or ""),
                "status": _row_status_bucket(r),
                "reason": str(r.get("reason") or "")[:120],
            }
        )
    return out


def _format_rows_markdown(rows: list[dict[str, Any]], *, title: str = "OSE offset") -> str:
    if not rows:
        return "No offset records found."
    lines = [f"**{title}**\n"]
    for r in rows[:20]:
        st = _row_status_bucket(r)
        lines.append(
            f"• **{r.get('request_person') or '?'}** → {r.get('exchange_person') or '?'} "
            f"({r.get('shift_type') or ''}): "
            f"{r.get('original_date') or '?'} → {r.get('exchange_date') or '?'} "
            f"[{st}]"
        )
    if len(rows) > 20:
        lines.append(f"\n_(Showing 20 of {len(rows)} rows.)_")
    return "\n".join(lines)


def build_query_reply(
    text: str,
    *,
    rows: Optional[list[dict[str, Any]]] = None,
    person_filter: Optional[str] = None,
    status_filter: Optional[str] = None,
    month_target: Optional[tuple[int, int]] = None,
) -> str:
    """Legacy helper — prefer agent tools."""
    rows = rows if rows is not None else _admin_rows()
    y, m = (month_target[0], month_target[1]) if month_target else (None, None)
    filtered = filter_rows_by_args(
        rows,
        person=person_filter,
        status=status_filter,
        year=y,
        month=m,
        person_role="requester" if person_filter else "any",
    )
    return _format_rows_markdown(filtered)


_FILTER_INFER_SYSTEM = (
    "Extract offset lookup filters from the user message.\n"
    "Use exact roster names when possible.\n"
    "Return ONE JSON object only:\n"
    '{"person":"roster name or null","status":"approved|pending|rejected|null",'
    '"year":int|null,"month":1-12|null,"person_role":"requester|any"}\n'
    "person_role=requester when user asks about someone's offsets (default for 'X offset').\n"
    "person_role=any only when user asks who swapped with X or exchange side.\n"
)


def _llm_infer_filters(user_text: str) -> dict[str, Any]:
    """AI reads the user message and returns filter args for tools (not regex routing)."""
    try:
        import chatagent as ca
    except Exception:
        return {}
    if not ca.llm_available():
        return {}
    api_key = ca._llm_api_key()
    if not api_key:
        return {}
    today = date.today()
    roster = list(od.OSE_LEAVE_FORM_NAMES)[:60]
    user = (
        f"today: {today.isoformat()}\n"
        f"roster: {json.dumps(roster, ensure_ascii=False)}\n"
        f"user_message: {user_text.strip()}"
    )
    payload = {
        "model": ca._llm_model_for_request(images=False),
        "messages": [
            {"role": "system", "content": _FILTER_INFER_SYSTEM},
            {"role": "user", "content": user},
        ],
        "max_tokens": 200,
        "temperature": 0.0,
    }
    url = f"{ca._llm_base_url()}/chat/completions"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=ca._llm_timeout_sec()) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        content = ((body.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        obj = _parse_llm_json(content) or {}
        out: dict[str, Any] = {}
        p = str(obj.get("person") or "").strip()
        if p:
            key = od._resolve_ose_roster_key(p)
            out["person"] = key or od._title_name(p)
        st = str(obj.get("status") or "").strip().lower()
        if st in ("approved", "pending", "rejected"):
            out["status"] = st
        pr = str(obj.get("person_role") or "requester").strip().lower()
        out["person_role"] = pr if pr in ("requester", "any") else "requester"
        try:
            y, m = obj.get("year"), obj.get("month")
            if y is not None and m is not None:
                out["year"] = int(y)
                out["month"] = int(m)
        except (TypeError, ValueError):
            pass
        print(f"[offsetai] inferred_filters={out!r}", flush=True)
        return out
    except Exception as exc:
        print(f"[offsetai] filter infer failed: {exc!r}", flush=True)
        return {}


def _parse_llm_json(content: str) -> Optional[dict[str, Any]]:
    s = (content or "").strip()
    if not s:
        return None
    s = re.sub(r"^```(?:json)?\s*", "", s)
    s = re.sub(r"\s*```$", "", s).strip()
    try:
        obj = json.loads(s)
    except Exception:
        m = re.search(r"\{.*\}", s, re.S)
        if not m:
            return None
        try:
            obj = json.loads(m.group(0))
        except Exception:
            return None
    return obj if isinstance(obj, dict) else None


def _parse_date_arg(raw: Optional[str]) -> Optional[date]:
    if not raw:
        return None
    try:
        return od._parse_date_value(raw)
    except Exception:
        return None


@dataclass
class _AgentCtx:
    user_text: str
    sender_open_id: str
    chat_id: str
    chat_type: Optional[str]
    session_key: str
    send_message: Callable[..., Any]
    get_token_func: Callable[[], str]
    request_person: str = ""
    is_approver: bool = False
    exchange_roster: list[str] = field(default_factory=list)
    inferred_filters: dict[str, Any] = field(default_factory=dict)
    dry_run: bool = False
    tool_trace: list[str] = field(default_factory=list)


def _month_target_from_args(args: dict[str, Any]) -> Optional[tuple[int, int]]:
    try:
        y, m = args.get("year"), args.get("month")
        if y is not None and m is not None:
            return int(y), int(m)
    except (TypeError, ValueError):
        pass
    return None


def _merge_tool_args(ctx: _AgentCtx, args: dict[str, Any]) -> dict[str, Any]:
    """Fill missing tool args from AI-inferred filters (not regex)."""
    out = dict(args)
    inf = ctx.inferred_filters or {}
    for key in ("person", "status", "year", "month", "person_role"):
        if out.get(key) in (None, "") and inf.get(key) not in (None, ""):
            out[key] = inf[key]
    if not out.get("person_role"):
        out["person_role"] = "requester" if out.get("person") else "any"
    return out


def _filter_label_from_args(args: dict[str, Any]) -> str:
    bits: list[str] = []
    if args.get("person"):
        bits.append(str(args["person"]))
    if args.get("status"):
        bits.append(str(args["status"]))
    if args.get("year") and args.get("month"):
        bits.append(f"{args['year']}-{int(args['month']):02d}")
    return " · ".join(bits)


def _filter_kwargs(args: dict[str, Any]) -> dict[str, Any]:
    return {
        "person": args.get("person"),
        "status": args.get("status"),
        "year": args.get("year"),
        "month": args.get("month"),
        "person_role": args.get("person_role") or "requester",
    }


def _execute_tool(ctx: _AgentCtx, name: str, args: dict[str, Any]) -> tuple[str, Optional[bool]]:
    """
    Run one tool. Returns (json_result_for_llm, terminal_handle_result).
    terminal: True=handled, False=pass_not_offset, None=continue agent loop.
    """
    import offsetleave as ol

    name = (name or "").strip()
    args = _merge_tool_args(ctx, args if isinstance(args, dict) else {})
    ctx.tool_trace.append(f"{name}({json.dumps(args, ensure_ascii=False)[:240]})")
    fk = _filter_kwargs(args)

    if name == "pass_not_offset":
        return json.dumps({"ok": True, "pass": True}), False

    if name == "list_offset_records":
        rows = filter_rows_by_args(_admin_rows(), **fk)
        payload = {"count": len(rows), "rows": _slim_rows(rows), "filters_applied": fk}
        return json.dumps(payload, ensure_ascii=False), None

    if name == "send_chat_reply":
        text = str(args.get("text") or "").strip()
        if not text:
            return json.dumps({"ok": False, "error": "text required"}), None
        if not ctx.dry_run:
            ctx.send_message(ctx.chat_id, text)
        return json.dumps({"ok": True, "sent": True}), True

    if name == "show_delete_picker":
        mt = _month_target_from_args(args)
        month_label = ol._month_filter_label(mt[0], mt[1]) if mt else None
        filter_label = _filter_label_from_args(args)
        oid = ctx.sender_open_id

        if not fk.get("person") and not fk.get("status"):
            return json.dumps({
                "ok": False,
                "error": (
                    "Refusing to show full delete list — call show_delete_picker with "
                    "person and/or status from inferred_filters or user message."
                ),
                "inferred_filters": ctx.inferred_filters,
            }), None

        if ol._is_offset_approver_open_id(oid):
            rows = ol._all_offsets_for_approver_delete()
            if mt:
                rows = ol._filter_offsets_by_month(rows, *mt)
            rows = filter_rows_by_args(rows, **fk)
            if not rows:
                msg = f"No offset records match filter: **{filter_label or 'none'}**."
                if not ctx.dry_run:
                    ctx.send_message(ctx.chat_id, msg)
                return json.dumps({"ok": False, "error": msg}), True
            card = ol.build_offset_delete_list_card(
                oid,
                "",
                rows,
                is_admin=True,
                month_label=month_label,
                filter_label=filter_label,
            )
        else:
            rp = ctx.request_person or ol.resolve_request_person(
                oid, ctx.get_token_func()
            )
            rows = ol._pending_offsets_for_request_person(rp)
            if mt:
                rows = ol._filter_offsets_by_month(rows, *mt)
            rows = filter_rows_by_args(rows, **fk)
            if not rows:
                msg = f"No pending rows match filter: **{filter_label or 'none'}**."
                if not ctx.dry_run:
                    ctx.send_message(ctx.chat_id, msg)
                return json.dumps({"ok": False, "error": msg}), True
            card = ol.build_offset_delete_list_card(
                oid,
                rp,
                rows,
                is_admin=False,
                month_label=month_label,
                filter_label=filter_label,
            )
        if not ctx.dry_run:
            ol._deliver_private_card(
                owner_open_id=oid,
                group_chat_id=ctx.chat_id,
                chat_type=ctx.chat_type,
                card=card,
                send_message=ctx.send_message,
                token=ctx.get_token_func(),
            )
        return json.dumps({"ok": True, "picker": True, "count": len(rows), "filters": fk}), True

    if name == "show_offset_calendar":
        today = date.today()
        try:
            y = int(args.get("year") if args.get("year") is not None else today.year)
            m = int(args.get("month") if args.get("month") is not None else today.month)
        except (TypeError, ValueError):
            y, m = today.year, today.month
        if not ctx.dry_run:
            card = od.build_ose_showoffset_card(y, m)
            ctx.send_message(
                ctx.chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive"
            )
        return json.dumps({"ok": True, "calendar": f"{y}-{m:02d}"}), True

    if name == "open_offset_form":
        oid = ctx.sender_open_id
        if not ctx.dry_run:
            rp = ctx.request_person or ol.resolve_request_person(
                oid, ctx.get_token_func()
            )
            ol._deliver_private_card(
                owner_open_id=oid,
                group_chat_id=ctx.chat_id,
                chat_type=ctx.chat_type,
                card=ol.build_offset_form_card(owner_open_id=oid, request_person=rp),
                send_message=ctx.send_message,
                token=ctx.get_token_func(),
            )
        return json.dumps({"ok": True, "form": True}), True

    if name == "submit_offset_record":
        oid = ctx.sender_open_id
        rp = ctx.request_person or ol.resolve_request_person(oid, ctx.get_token_func())
        exchange_person = str(args.get("exchange_person") or "").strip()
        shift_type = str(args.get("shift_type") or "").strip().upper()
        reason = str(args.get("reason") or "").strip()
        original_date = _parse_date_arg(args.get("original_date"))
        exchange_date = _parse_date_arg(args.get("exchange_date"))
        if not all([exchange_person, shift_type, reason, original_date, exchange_date]):
            return json.dumps({"ok": False, "error": "missing required fields"}), None
        fp = ol.offset_submit_fingerprint(
            request_person=rp,
            exchange_person=exchange_person,
            shift_type=shift_type,
            original_date=original_date,
            exchange_date=exchange_date,
            reason=reason,
        )
        if not ctx.dry_run:
            dup_err = ol.try_begin_offset_submit(oid, fp)
            if dup_err:
                ctx.send_message(ctx.chat_id, f"❌ {dup_err}")
                return json.dumps({"ok": False, "error": dup_err}), True
            try:
                out = od.submit_ose_offset(
                    request_person=rp,
                    exchange_person=exchange_person,
                    shift_type=shift_type,
                    original_date=original_date,
                    exchange_date=exchange_date,
                    reason=reason,
                )
                rid = str((out or {}).get("record_id") or "").strip()
                if rid:
                    ol.remember_offset_requester_open_id(rid, oid)
                ol.release_offset_submit(oid, fp, success=True)
                msg = (
                    f"✅ **Offset submitted** for **{rp}**.\n"
                    f"• Swap with **{exchange_person}** ({shift_type})\n"
                    f"• {original_date.isoformat()} → {exchange_date.isoformat()}\n"
                    f"• Reason: {reason}\n"
                    f"Record: `{rid or 'saved'}`"
                )
                ctx.send_message(ctx.chat_id, msg)
            except Exception as exc:
                ol.release_offset_submit(oid, fp, success=False)
                ctx.send_message(ctx.chat_id, f"❌ Submit failed: {exc}")
                return json.dumps({"ok": False, "error": str(exc)}), True
        return json.dumps({"ok": True, "submitted": True}), True

    if name == "show_edit_picker":
        oid = ctx.sender_open_id
        mt = _month_target_from_args(args)
        if ol._is_offset_approver_open_id(oid):
            rows = ol._all_offsets_for_approver_delete()
            if mt:
                rows = ol._filter_offsets_by_month(rows, *mt)
            rows = [r for r in rows if bool(r.get("pending"))]
            rows = filter_rows_by_args(
                rows,
                person=args.get("person"),
                year=args.get("year"),
                month=args.get("month"),
            )
            card = ol.build_offset_edit_list_card(oid, "", rows, is_admin=True)
        else:
            rp = ctx.request_person or ol.resolve_request_person(
                oid, ctx.get_token_func()
            )
            rows = ol._pending_offsets_for_request_person(rp)
            if mt:
                rows = ol._filter_offsets_by_month(rows, *mt)
            card = ol.build_offset_edit_list_card(oid, rp, rows, is_admin=False)
        if not ctx.dry_run:
            ol._deliver_private_card(
                owner_open_id=oid,
                group_chat_id=ctx.chat_id,
                chat_type=ctx.chat_type,
                card=card,
                send_message=ctx.send_message,
                token=ctx.get_token_func(),
            )
        return json.dumps({"ok": True, "edit_picker": True}), True

    if name == "show_pending_queue":
        oid = ctx.sender_open_id
        if not ol._is_offset_approver_open_id(oid):
            return json.dumps({"ok": False, "error": "approvers only"}), None
        rows = ol._all_pending_offsets()
        if not ctx.dry_run:
            if not rows:
                ctx.send_message(ctx.chat_id, "No pending offset requests.")
            else:
                card = ol.build_offset_pending_list_card(rows)
                ol._deliver_private_card(
                    owner_open_id=oid,
                    group_chat_id=ctx.chat_id,
                    chat_type=ctx.chat_type,
                    card=card,
                    send_message=ctx.send_message,
                    token=ctx.get_token_func(),
                )
        return json.dumps({"ok": True, "pending_count": len(rows)}), True

    return json.dumps({"ok": False, "error": f"unknown tool {name!r}"}), None


def _llm_chat_with_tools(messages: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    try:
        import chatagent as ca
    except Exception:
        return None
    if not ca.llm_available():
        return None
    api_key = ca._llm_api_key()
    if not api_key:
        return None
    payload: dict[str, Any] = {
        "model": ca._llm_model_for_request(images=False),
        "messages": messages,
        "tools": _TOOL_SPECS,
        "tool_choice": "auto",
        "max_tokens": int(os.getenv("OSE_OFFSET_AGENT_MAX_TOKENS", "900")),
        "temperature": 0.0,
    }
    try:
        if ca._is_ollama_base():
            payload["think"] = False
    except Exception:
        pass
    url = f"{ca._llm_base_url()}/chat/completions"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=ca._llm_timeout_sec()) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        print(f"[offsetai] agent LLM failed: {exc!r}", flush=True)
        return None


def _fallback_tool_from_content(content: str) -> Optional[tuple[str, dict[str, Any]]]:
    """JSON tool call when the model does not return native tool_calls."""
    obj = _parse_llm_json(content)
    if not obj:
        return None
    tool = str(obj.get("tool") or obj.get("name") or "").strip()
    if not tool:
        return None
    args = obj.get("arguments") or obj.get("args") or {}
    if not isinstance(args, dict):
        args = {}
    return tool, args


def _build_context_block(ctx: _AgentCtx) -> str:
    today = date.today()
    rows = _slim_rows(_admin_rows(), limit=40)
    parts = [
        f"today: {today.isoformat()} ({today.strftime('%A')})",
        f"request_person: {ctx.request_person or 'unknown'}",
        f"is_approver: {ctx.is_approver}",
        f"inferred_filters: {json.dumps(ctx.inferred_filters, ensure_ascii=False)}",
        f"exchange_roster: {json.dumps(ctx.exchange_roster[:40], ensure_ascii=False)}",
        f"offset_sheet_snapshot ({len(rows)} rows):",
        json.dumps(rows, ensure_ascii=False),
        f"user_message: {ctx.user_text.strip()}",
    ]
    return "\n".join(parts)


def _run_agent(ctx: _AgentCtx) -> bool:
    import offsetleave as ol

    ctx.inferred_filters = _llm_infer_filters(ctx.user_text)

    try:
        token = ctx.get_token_func()
        ctx.request_person = ol.try_resolve_request_person(ctx.sender_open_id, token) or ""
        ctx.is_approver = ol._is_offset_approver_open_id(ctx.sender_open_id)
        if ctx.request_person:
            ctx.exchange_roster = list(
                od.ose_offset_form_exchange_names(exclude_person=ctx.request_person)
            )
    except Exception:
        pass

    messages: list[dict[str, Any]] = [{"role": "system", "content": _AGENT_SYSTEM}]
    for h in _agent_history_get(ctx.session_key):
        messages.append(h)
    messages.append({"role": "user", "content": _build_context_block(ctx)})

    assistant_note = ""
    for turn in range(_AGENT_MAX_TURNS):
        body = _llm_chat_with_tools(messages)
        if not body:
            return False
        choice = (body.get("choices") or [{}])[0]
        msg = choice.get("message") or {}
        tool_calls = msg.get("tool_calls") or []
        content = (msg.get("content") or "").strip()

        if not tool_calls and content:
            fb = _fallback_tool_from_content(content)
            if fb:
                tool_calls = [
                    {
                        "id": "fallback0",
                        "type": "function",
                        "function": {
                            "name": fb[0],
                            "arguments": json.dumps(fb[1], ensure_ascii=False),
                        },
                    }
                ]
                content = ""

        if tool_calls:
            messages.append(msg)
            for tc in tool_calls:
                fn = tc.get("function") or {}
                tname = str(fn.get("name") or "").strip()
                try:
                    targs = json.loads(fn.get("arguments") or "{}")
                except json.JSONDecodeError:
                    targs = {}
                if not isinstance(targs, dict):
                    targs = {}
                result, terminal = _execute_tool(ctx, tname, targs)
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.get("id") or tname,
                        "content": result,
                    }
                )
                if terminal is not None:
                    assistant_note = "; ".join(ctx.tool_trace)
                    _agent_history_append(ctx.session_key, ctx.user_text, assistant_note)
                    return terminal
            continue

        if content:
            if not ctx.dry_run:
                ctx.send_message(ctx.chat_id, content)
            assistant_note = content[:200]
            _agent_history_append(ctx.session_key, ctx.user_text, assistant_note)
            return True

        break

    if not ctx.dry_run:
        ctx.send_message(
            ctx.chat_id,
            "Sorry, I could not complete that offset request. Try rephrasing or use **open form**.",
        )
    return True


def handle(
    text: str,
    *,
    sender_open_id: str = "",
    chat_id: str = "",
    chat_type: Optional[str] = None,
    send_message: Optional[Callable[..., Any]] = None,
    get_token_func: Optional[Callable[[], str]] = None,
    session_key: Optional[str] = None,
) -> bool:
    """LLM agent with tools. Returns True if handled. Never raises."""
    if not is_enabled():
        return False
    if not _llm_available():
        return False
    raw = (text or "").strip()
    if not raw or send_message is None or get_token_func is None:
        return False
    sk = (session_key or "").strip() or f"{chat_id}:{sender_open_id}"
    # Active agent session or loose topic gate
    if not looks_like_offset_topic(raw) and not _agent_history_get(sk):
        return False

    ctx = _AgentCtx(
        user_text=raw,
        sender_open_id=(sender_open_id or "").strip(),
        chat_id=chat_id,
        chat_type=chat_type,
        session_key=sk,
        send_message=send_message,
        get_token_func=get_token_func,
    )
    print(f"[offsetai] agent start {raw[:100]!r}", flush=True)
    try:
        handled = _run_agent(ctx)
        print(
            f"[offsetai] agent done handled={handled} tools={ctx.tool_trace}",
            flush=True,
        )
        if not handled:
            if any(t.startswith("pass_not_offset") for t in ctx.tool_trace):
                return False
            send_message(
                chat_id,
                "⚠️ Offset assistant could not reach the AI. "
                "Check API key / model, or use slash commands like **deleteoffset**.",
            )
            return True
        return handled
    except Exception as exc:
        print(f"[offsetai] agent error: {exc!r}", flush=True)
        send_message(chat_id, f"❌ Offset assistant error: {exc}")
        return True


def parse_request(text: str) -> dict[str, Any]:
    """CLI dry-run: run agent without sending messages."""
    raw = (text or "").strip()
    out: dict[str, Any] = {
        "intent": "agent",
        "source": "llm_tools",
        "tools": [],
        "handled": False,
    }
    if not raw or not looks_like_offset_topic(raw):
        out["intent"] = "none"
        return out
    ctx = _AgentCtx(
        user_text=raw,
        sender_open_id="cli",
        chat_id="cli",
        chat_type="p2p",
        session_key="cli",
        send_message=lambda *a, **k: None,
        get_token_func=lambda: "",
        dry_run=True,
    )
    out["handled"] = _run_agent(ctx)
    out["tools"] = ctx.tool_trace
    return out


def startup_status() -> None:
    print(
        f"[offsetai] BOT_USE_OFFSETAI={os.getenv('BOT_USE_OFFSETAI')!r} "
        f"enabled={is_enabled()} llm={'yes' if _llm_available() else 'no'} "
        f"mode=agent_tools",
        flush=True,
    )
    if is_enabled() and _llm_available():
        print(
            "[offsetai] Ready — LLM agent + tools (no rule routing).",
            flush=True,
        )
    elif is_enabled():
        print(
            "[offsetai] LLM required — set API key; no rule fallback.",
            flush=True,
        )


def _cli(args: list[str]) -> None:
    dry = False
    if args and args[0] in ("--dry", "--parse", "-p"):
        dry = True
        args = args[1:]
    text = " ".join(args)
    if not text:
        print('Usage: python offsetai.py [--dry] "show me man chung offset approved"')
        return
    print(f"topic:  {looks_like_offset_topic(text)}")
    print(f"llm:    {_llm_available()}")
    if dry or not _llm_available():
        parsed = parse_request(text)
        print(f"handled: {parsed.get('handled')}")
        print(f"tools:   {parsed.get('tools')}")
        return
    print(f"rows:   {len(_admin_rows())} loaded from sheet")


if __name__ == "__main__":
    import sys

    _cli(sys.argv[1:])
