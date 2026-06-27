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
    "- query: explain / list / who has offset — answer in reply using the sheet rows\n"
    "- show_calendar: monthly calendar card (like showoffset); set year/month\n"
    "- add: open form to submit a NEW offset request\n"
    "- delete: user wants to cancel/remove their (or admin: any) offset\n"
    "- edit: change an existing offset request\n"
    "- pending: approver views pending approval queue\n"
    "- none: not about OSE offset\n"
    "- 'this month' / 'for this month' → current year+month; 'next month' / 'last month' likewise\n"
    "- For delete/edit, still set year/month when the user names a month\n"
    "Examples:\n"
    '"who has offset in June" -> {"intent":"query","year":2026,"month":6,"reply":"..."}\n'
    '"show offset for this month" -> {"intent":"show_calendar","year":2026,"month":6,"reply":""}\n'
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


def _rule_fallback_action(text: str) -> Optional[str]:
    """Map to offsetleave action names when LLM is unavailable."""
    try:
        import offsetleave as ol

        action = ol._parse_offset_leave_action_rules(text)
        if not action:
            return None
        mapping = {
            "offset_form": "add",
            "show_offset": "show_calendar",
            "delete_offset": "delete",
            "edit_offset": "edit",
            "pending_offset": "pending",
        }
        return mapping.get(action)
    except Exception:
        return None


def parse_request(text: str) -> dict[str, Any]:
    """Return ``{intent, year, month, reply}`` — LLM first, rules fallback."""
    out: dict[str, Any] = {"intent": "none", "year": None, "month": None, "reply": ""}
    raw = (text or "").strip()
    if not raw or not looks_like_offset_topic(raw):
        return out
    rows = _rows_for_llm()
    obj = _llm_decide(raw, rows)
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
        out["intent"] = intent
        out["reply"] = str(obj.get("reply") or "").strip()
        try:
            y = obj.get("year")
            m = obj.get("month")
            if y is not None and m is not None:
                out["year"] = int(y)
                out["month"] = int(m)
        except (TypeError, ValueError):
            pass
        if out["intent"] != "none":
            return out
    fb = _rule_fallback_action(raw)
    if fb:
        out["intent"] = fb
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
        f"[offsetai] {raw[:100]!r} -> intent={intent} month={month_target}",
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
    print(f"intent: {parsed.get('intent')}")
    print(f"month:  {parsed.get('year')}-{parsed.get('month')}")
    print(f"reply:  {(parsed.get('reply') or '')[:200]}")
    if parse_only:
        return
    print(f"rows:   {len(_rows_for_llm())} loaded from sheet")


if __name__ == "__main__":
    import sys

    _cli(sys.argv[1:])
