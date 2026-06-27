"""
offsetai — LLM-driven OSE offset assistant (reads the live Bitable sheet).

Natural-language offset questions no longer go through command routing
(``showoffset`` / ``deleteoffset`` / action classifiers). Instead:

1. Cheap keyword gate — is this about OSE duty **offset** (swap), not leave queries?
2. Load current offset rows from the Bitable sheet (same source as the calendar).
3. One LLM call with the sheet snapshot + user message → intent + optional reply.
4. Execute: answer in chat, show month card, or open add/delete/edit/pending UI.

Toggle: ``BOT_USE_OFFSETAI=0`` disables (falls back to ``offsetleave`` NL rules).

CLI:
    python offsetai.py "who has offset this month"
    python offsetai.py --parse "i want to delete offset for this month"
"""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from datetime import date
from typing import Any, Callable, Optional

import ose_Duty as od

# Offset swap / 调休 — not monthly leave attendance queries.
_OFFSET_TOPIC_RE = re.compile(
    r"(?i)\b("
    r"offset|swap\s+(?:my\s+)?(?:duty|shift|roster)|exchange\s+(?:my\s+)?(?:duty|shift)|"
    r"调休|换班|duty\s+swap|shift\s+swap"
    r")\b"
)
_OFFSET_QUERY_RE = re.compile(
    r"(?i)\b("
    r"show\s+me|show|who|which|what|list|check|view|see|get|tell\s+me|find|"
    r"approved|rejected|pending|status|"
    r"has\s+offset|have\s+offset|any\s+offset|whose\s+offset"
    r")\b"
)
_OFFSET_ADD_RE = re.compile(
    r"(?i)\b("
    r"i\s+want\s+to\s+(?:add|submit|request|apply)|"
    r"want\s+to\s+(?:add|submit|request|swap|exchange)|"
    r"need\s+to\s+swap|submit\s+(?:an?\s+)?offset|offset\s+request|offset\s+form|"
    r"swap\s+(?:my\s+)?(?:duty|shift)|exchange\s+(?:my\s+)?(?:duty|shift)|"
    r"request\s+(?:a\s+)?offset|调休|换班"
    r")\b"
)
# Explicit glued admin commands still handled by offsetleave slash parsers.
_SLASH_OFFSET_RE = re.compile(
    r"(?i)^(?:/)?(?:showoffset|deleteoffset|editoffset|pendingoffset)\b"
)
_MAX_ROWS_FOR_LLM = 80

_PARSE_SYSTEM = (
    "You are the OSE Duty Bot offset assistant on Lark/Feishu.\n"
    "Users ask about duty shift swaps (offset) — who swapped which days, pending "
    "requests, or they want to add / delete / edit / approve.\n"
    "You receive a JSON snapshot of the current offset sheet rows (read-only).\n"
    "Today's date is provided in the user message.\n\n"
    "Reply with ONE JSON object only:\n"
    '{\n'
    '  "intent": "query"|"show_calendar"|"add"|"delete"|"edit"|"pending"|"none",\n'
    '  "year": <int or null>,\n'
    "  \"month\": <1-12 or null>,\n"
    '  "reply": "<text for intent=query only; summarize sheet data; empty otherwise>"\n'
    "}\n\n"
    "Intent rules:\n"
    "- query: user is ASKING / LOOKING UP data (show me, who, which, approved, rejected, "
    "a person's name + offset). ALWAYS use query — never add.\n"
    "- show_calendar: monthly calendar card only when they want the whole month grid\n"
    "- add: ONLY when user explicitly wants to SUBMIT / SWAP / REQUEST a NEW offset\n"
    "- delete: cancel/remove an offset request\n"
    "- edit: change an existing offset request\n"
    "- pending: approver views pending approval queue\n"
    "- none: not about OSE offset\n"
    "- Filter by person name and approval status (approved/rejected/pending) in your reply\n"
    "- 'this month' / 'for this month' → current year+month\n"
    "Examples:\n"
    '"show me man chung offset which is approved" -> '
    '{"intent":"query","year":null,"month":null,"reply":"Man Chung approved: ..."}\n'
    '"who has offset in June" -> {"intent":"query","year":2026,"month":6,"reply":"..."}\n'
    '"show offset calendar for this month" -> {"intent":"show_calendar","year":2026,"month":6,"reply":""}\n'
    '"i want to delete offset for this month" -> {"intent":"delete","year":2026,"month":6,"reply":""}\n'
    '"i want to swap my duty" -> {"intent":"add","year":null,"month":null,"reply":""}\n'
)


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


def looks_like_offset_query(text: str) -> bool:
    raw = (text or "").strip()
    if not raw:
        return False
    if _OFFSET_QUERY_RE.search(raw):
        return True
    # "man chung offset" — name + offset without submit verbs is a lookup
    if re.search(r"(?i)\boffset\b", raw) and not _OFFSET_ADD_RE.search(raw):
        if re.search(r"(?i)\b(delete|remove|cancel|edit|change|swap|submit|request|add)\b", raw):
            return False
        return True
    return False


def looks_like_offset_add(text: str) -> bool:
    return bool(_OFFSET_ADD_RE.search((text or "").strip()))


def _status_filter_from_text(text: str) -> Optional[str]:
    low = (text or "").lower()
    if re.search(r"\bapproved\b", low):
        return "approved"
    if re.search(r"\brejected\b", low):
        return "rejected"
    if re.search(r"\bpending\b", low) and not re.search(r"\bpending\s+offset", low):
        return "pending"
    return None


def _person_matches_text(person: str, text: str) -> bool:
    person = (person or "").strip()
    if not person:
        return False
    low = text.lower()
    titled = od._title_name(person)
    if titled.lower() in low:
        return True
    tokens = [t.lower() for t in od._word_tokens(person) if len(t) >= 2]
    if len(tokens) >= 2 and all(t in low for t in tokens):
        return True
    for roster in od.OSE_LEAVE_FORM_NAMES:
        if od._names_same_person(roster, person) and roster.lower() in low:
            return True
        rt = [t.lower() for t in od._word_tokens(roster) if len(t) >= 2]
        if len(rt) >= 2 and all(t in low for t in rt):
            return True
    return False


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


def _format_row_line(row: dict[str, Any]) -> str:
    st = _row_status_bucket(row)
    return (
        f"• **{row.get('request_person') or '?'}** <-> {row.get('exchange_person') or '?'} "
        f"({row.get('shift_type') or ''}): "
        f"{row.get('original_date') or '?'} → {row.get('exchange_date') or '?'} "
        f"[{st}]"
    )


def build_query_reply(text: str, *, rows: Optional[list[dict[str, Any]]] = None) -> str:
    """Answer an offset lookup from sheet rows (LLM fallback / safety net)."""
    raw = (text or "").strip()
    rows = rows if rows is not None else _admin_rows()
    status_f = _status_filter_from_text(raw)
    month_target = None
    try:
        import offsetleave as ol

        month_target = ol._parse_offset_month_filter(raw)
    except Exception:
        pass

    person_named = any(
        _person_matches_text(str(r.get("request_person") or ""), raw)
        or _person_matches_text(str(r.get("exchange_person") or ""), raw)
        for r in rows
    )
    filtered: list[dict[str, Any]] = []
    for row in rows:
        if status_f and _row_status_bucket(row) != status_f:
            continue
        if month_target:
            y, m = month_target
            if not any(
                (d := od._parse_date_value(row.get(key)))
                and d.year == y
                and d.month == m
                for key in ("original_date", "exchange_date", "request_date")
            ):
                continue
        if person_named:
            req = str(row.get("request_person") or "")
            exc = str(row.get("exchange_person") or "")
            if not (_person_matches_text(req, raw) or _person_matches_text(exc, raw)):
                continue
        filtered.append(row)

    if not filtered:
        bits = []
        if person_named:
            bits.append("matching that person")
        if status_f:
            bits.append(status_f)
        if month_target:
            try:
                import offsetleave as ol

                bits.append(ol._month_filter_label(*month_target))
            except Exception:
                pass
        hint = " (" + ", ".join(bits) + ")" if bits else ""
        return f"No offset records found{hint}."

    lines = ["**OSE offset lookup**\n"]
    lines.extend(_format_row_line(r) for r in filtered[:20])
    if len(filtered) > 20:
        lines.append(f"\n_(Showing 20 of {len(filtered)} rows.)_")
    return "\n".join(lines)


def _rows_for_llm() -> list[dict[str, str]]:
    try:
        data = od.get_ose_offset_records_admin() or {}
        items = list((data.get("items") or []))[:_MAX_ROWS_FOR_LLM]
    except Exception as exc:
        print(f"[offsetai] sheet load failed: {exc!r}", flush=True)
        return []
    out: list[dict[str, str]] = []
    for it in items:
        out.append(
            {
                "request_person": str(it.get("request_person") or ""),
                "exchange_person": str(it.get("exchange_person") or ""),
                "original_date": str(it.get("original_date") or ""),
                "exchange_date": str(it.get("exchange_date") or ""),
                "shift_type": str(it.get("shift_type") or ""),
                "status": str(it.get("approval_status") or ("Pending" if it.get("pending") else "")),
                "reason": str(it.get("reason") or "")[:120],
            }
        )
    return out


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


def _llm_decide(text: str, rows: list[dict[str, str]]) -> Optional[dict[str, Any]]:
    try:
        import chatagent as ca
    except Exception:
        return None
    if not ca.llm_available():
        return None
    api_key = ca._llm_api_key()
    if not api_key:
        return None
    today = date.today()
    user = (
        f"today is {today.isoformat()} ({today.strftime('%A')}).\n"
        f"offset_sheet_rows ({len(rows)} shown):\n"
        f"{json.dumps(rows, ensure_ascii=False)}\n\n"
        f"user message: {text.strip()}"
    )
    payload = {
        "model": ca._llm_model_for_request(images=False),
        "messages": [
            {"role": "system", "content": _PARSE_SYSTEM},
            {"role": "user", "content": user},
        ],
        "max_tokens": 600,
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
            body = json.loads(resp.read().decode("utf-8"))
        content = ((body.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        return _parse_llm_json(content)
    except Exception as exc:
        print(f"[offsetai] LLM failed: {exc!r}", flush=True)
        return None


def _rule_fallback_intent(text: str) -> Optional[str]:
    """When LLM is down — never default bare 'offset' to add."""
    raw = (text or "").strip()
    if not raw:
        return None
    try:
        import offsetleave as ol

        if ol._PENDING_OFFSET_RULE_RE.match(raw):
            return "pending"
        if ol._DELETE_OFFSET_RULE_RE.match(raw) or re.search(
            r"(?i)\b(delete|remove|cancel|drop|withdraw)\b", raw
        ):
            return "delete"
        if ol._EDIT_OFFSET_RULE_RE.match(raw) or re.search(
            r"(?i)\b(edit|change|update|modify|amend)\b", raw
        ):
            return "edit"
        if od.parse_showoffset_command(raw) is not None:
            return "show_calendar"
    except Exception:
        pass
    if looks_like_offset_query(raw):
        return "query"
    if looks_like_offset_add(raw):
        return "add"
    return None


def parse_request(text: str) -> dict[str, Any]:
    """Return ``{intent, year, month, reply, source}`` — LLM first, smart rules fallback."""
    out: dict[str, Any] = {
        "intent": "none",
        "year": None,
        "month": None,
        "reply": "",
        "source": None,
    }
    raw = (text or "").strip()
    if not raw or not looks_like_offset_topic(raw):
        return out
    rows = _rows_for_llm()
    admin_rows = _admin_rows()

    obj = _llm_decide(raw, rows) if _llm_available() else None
    if obj:
        intent = str(obj.get("intent") or "none").strip().lower()
        if intent not in (
            "query",
            "show_calendar",
            "add",
            "delete",
            "edit",
            "pending",
            "none",
        ):
            intent = "none"
        # LLM said add but user is clearly asking — override to query
        if intent == "add" and looks_like_offset_query(raw) and not looks_like_offset_add(raw):
            intent = "query"
        out["intent"] = intent
        out["source"] = "llm"
        out["reply"] = str(obj.get("reply") or "").strip()
        try:
            y = obj.get("year")
            m = obj.get("month")
            if y is not None and m is not None:
                out["year"] = int(y)
                out["month"] = int(m)
        except (TypeError, ValueError):
            pass
        if intent == "query" and not out["reply"]:
            out["reply"] = build_query_reply(raw, rows=admin_rows)
        if out["intent"] != "none":
            return out

    fb = _rule_fallback_intent(raw)
    if fb:
        out["intent"] = fb
        out["source"] = "rules"
        if fb == "query":
            out["reply"] = build_query_reply(raw, rows=admin_rows)
        try:
            import offsetleave as ol

            mt = ol._parse_offset_month_filter(raw)
            if mt:
                out["year"], out["month"] = mt
        except Exception:
            pass
    return out


def handle(
    text: str,
    *,
    sender_open_id: str = "",
    chat_id: str = "",
    chat_type: Optional[str] = None,
    send_message: Optional[Callable[..., Any]] = None,
    get_token_func: Optional[Callable[[], str]] = None,
) -> bool:
    """
    Handle a natural-language offset message. Returns True if handled.
    Never raises.
    """
    if not is_enabled():
        return False
    raw = (text or "").strip()
    if not raw or not looks_like_offset_topic(raw):
        return False
    if send_message is None or get_token_func is None:
        return False

    parsed = parse_request(raw)
    intent = parsed.get("intent") or "none"
    if intent == "none":
        return False

    # Safety: lookups must never open the add form
    if intent == "add" and looks_like_offset_query(raw) and not looks_like_offset_add(raw):
        intent = "query"
        parsed["intent"] = "query"
        if not parsed.get("reply"):
            parsed["reply"] = build_query_reply(raw)

    month_target = None
    y, m = parsed.get("year"), parsed.get("month")
    if y is not None and m is not None:
        try:
            month_target = (int(y), int(m))
        except (TypeError, ValueError):
            month_target = None
    if month_target is None:
        try:
            import offsetleave as ol

            month_target = ol._parse_offset_month_filter(raw)
        except Exception:
            pass

    print(
        f"[offsetai] {raw[:100]!r} -> intent={intent} source={parsed.get('source')} "
        f"month={month_target}",
        flush=True,
    )

    try:
        import offsetleave as ol

        return ol.execute_offset_action(
            intent,
            clean_text=raw,
            month_target=month_target,
            llm_reply=str(parsed.get("reply") or "").strip() or None,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_token_func,
        )
    except Exception as exc:
        print(f"[offsetai] handle error: {exc!r}", flush=True)
        send_message(chat_id, f"❌ Offset assistant error: {exc}")
        return True


def startup_status() -> None:
    print(
        f"[offsetai] BOT_USE_OFFSETAI={os.getenv('BOT_USE_OFFSETAI')!r} "
        f"enabled={is_enabled()} llm={'yes' if _llm_available() else 'no'}",
        flush=True,
    )
    if is_enabled() and _llm_available():
        print("[offsetai] Ready — reads offset sheet + LLM (no command routing).", flush=True)
    elif is_enabled():
        print("[offsetai] Ready (rule fallback; no LLM API key).", flush=True)


def _cli(args: list[str]) -> None:
    parse_only = False
    if args and args[0] in ("--parse", "-p"):
        parse_only = True
        args = args[1:]
    text = " ".join(args)
    if not text:
        print('Usage: python offsetai.py [--parse] "who has offset this month"')
        return
    print(f"topic:  {looks_like_offset_topic(text)}")
    parsed = parse_request(text)
    print(f"intent: {parsed.get('intent')} (source={parsed.get('source')})")
    print(f"month:  {parsed.get('year')}-{parsed.get('month')}")
    print(f"reply:  {(parsed.get('reply') or '')[:200]}")
    if parse_only:
        return
    print(f"rows:   {len(_rows_for_llm())} loaded from sheet")


if __name__ == "__main__":
    import sys

    _cli(sys.argv[1:])
