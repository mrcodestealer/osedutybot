"""
dutyai — AI-driven duty + leave assistant that replies with message **cards**.

What it does
------------
Lets users ask for duty/on-call rosters in *free form* natural language for one
or more departments on one or more (relative) dates, and replies with a nicely
formatted Lark **message card** (with emoji) per result. Examples it handles::

    "i want ose duty tmmr"                 -> OSE card for tomorrow
    "fpms after two days"                  -> FPMS card for today+2
    "fpms and cpms today"                  -> FPMS + CPMS cards for today
    "hi @duty bot i want ose next month 16" -> OSE card for the 16th next month
    "who is on sre duty this week"          -> SRE week card
    "this week who on leave"                -> read-only leave card for the week

How it decides (cheap → smart)
------------------------------
1. ``parse_request`` first does a cheap keyword check (department / leave / date
   words). If nothing duty-or-leave-ish is present, it returns ``kind="none"``
   instantly (no LLM call) so normal chat/command routing is untouched.
2. When the message *is* duty/leave-ish and an LLM is configured (same config as
   ``chatagent``), it asks the model for a single JSON object resolving every
   relative date to an absolute ``YYYY-MM-DD`` and listing the departments.
3. If there is no LLM (or it fails), a deterministic regex parser handles the
   common cases (today / tomorrow / tmr / after N days / next month DD /
   this week / explicit dates).

Everything is **read-only** — this module never writes to any sheet/Bitable.
Every adapter is wrapped so it can never raise into the bot's hot path.

Toggle: ``BOT_USE_DUTYAI=0`` disables it (``handle`` always returns ``None``).

CLI:
    python dutyai.py "i want ose and fpms duty tomorrow"
    python dutyai.py --parse "fpms after two days"
"""

from __future__ import annotations

import calendar
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from typing import Any, Optional

# --------------------------------------------------------------------------- #
# Department registry: canonical key -> (display name, emoji, card colour)
# --------------------------------------------------------------------------- #
_DEPT_META: dict[str, tuple[str, str, str]] = {
    "ose": ("OSE", "🛡️", "green"),
    "fpms": ("FPMS", "🟦", "blue"),
    "pms": ("PMS", "🟪", "purple"),
    "cpms": ("CPMS", "🟧", "orange"),
    "bi": ("BI", "📊", "carmine"),
    "fe": ("FE", "🎨", "turquoise"),
    "sre": ("SRE", "🛠️", "indigo"),
    "db": ("DB", "🗄️", "wathet"),
    "liveslot": ("Liveslot", "🎰", "violet"),
    "ote": ("OTE", "🎯", "yellow"),
    "ft": ("FT", "⚙️", "grey"),
}

# Department aliases (word -> canonical key). Longer phrases first.
_DEPT_ALIASES: list[tuple[str, str]] = [
    ("live slot", "liveslot"),
    ("liveslot", "liveslot"),
    ("database", "db"),
    ("dba", "db"),
    ("ose", "ose"),
    ("fpms", "fpms"),
    ("cpms", "cpms"),
    ("pms", "pms"),
    ("bi", "bi"),
    ("fe", "fe"),
    ("sre", "sre"),
    ("db", "db"),
    ("ote", "ote"),
    ("ft", "ft"),
]

# Departments whose underlying module only exposes a "today / next few days"
# roster (no arbitrary-date lookup). For those we show the latest available
# roster and add a small note when a specific other date was requested.
_TODAY_ONLY_DEPTS = {"bi", "fe", "ft"}

# Duty intent signals — any of these + (a date OR a department) means "duty".
_DUTY_SIGNAL_RE = re.compile(
    r"(?i)\b(duty|on[\s-]?call|oncall|roster|shift|schedule|who\s+is\s+on|"
    r"who'?s\s+on|cover(?:ing|s)?|standby|stand\s*by)\b"
    r"|值班|當班|当班|排班|轮值|輪值|当值|當值|谁\s*值班|誰\s*值班|on\s*duty"
)
# Leave intent signals.
_LEAVE_SIGNAL_RE = re.compile(
    r"(?i)\b(on\s+leave|who.*leave|leave\s+(?:today|tomorrow|this\s+week|"
    r"next\s+week|this\s+month)|day\s+off|who.*off\b|annual\s+leave|"
    r"vacation|holiday\s+leave|休假|请假|谁.*请假)\b"
)
# Things that must NOT be treated as a duty/leave card request (other flows own them).
# NB: no bare "/" here — a date like 30/06 must NOT be skipped (slash *commands*
# are already excluded by the caller via ``startswith("/")``).
_SKIP_RE = re.compile(
    r"(?i)\b(check|missing|jenkins|deploy|build|update|maintenance|maint|"
    r"credit|cctv|sms|otp|provider\s*id|pid|setmaintenance|unset|"
    r"reminder|cashout|amount\s*loss)\b"
)
# A monthly leave/WFH question is already served by the richer ``/leave`` month
# card — defer those (we only own today / tomorrow / week / specific-date leave).
_LEAVE_MONTH_RE = re.compile(r"(?i)\b(this|next|last)\s+month\b|\bmonthly\b")
_WFH_RE = re.compile(r"(?i)\b(wfh|work\s+from\s+home|remote\s+work)\b")

# Relative / absolute date tokens that hint the message references a specific date.
_DATE_HINT_RE = re.compile(
    r"(?i)\b(today|tonight|now|tmr|tmrw|tmmr|tomo|tomorrow|"
    r"yesterday|day\s+after\s+tomorrow|"
    r"this\s+week|next\s+week|this\s+month|next\s+month|"
    r"in\s+\d+\s+days?|after\s+\d+\s+days?|\d+\s+days?\s+later|"
    r"mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|"
    r"sat(?:urday)?|sun(?:day)?|"
    r"\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?|\d{4}-\d{2}-\d{2})\b"
)

_CJK_RE = re.compile(r"[\u4e00-\u9fff]")

# Hard cap so a "this week" × many-departments request can't spam the chat.
_MAX_DATES = 10
_MAX_CARDS = 12


# --------------------------------------------------------------------------- #
# Enable / config
# --------------------------------------------------------------------------- #
def is_enabled() -> bool:
    return (os.getenv("BOT_USE_DUTYAI") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _today() -> date:
    return datetime.now().date()


# --------------------------------------------------------------------------- #
# Cheap keyword detection
# --------------------------------------------------------------------------- #
def _find_departments(text: str) -> list[str]:
    """Return canonical department keys found in ``text`` (stable order, deduped)."""
    low = (text or "").lower()
    found: list[str] = []
    for word, canon in _DEPT_ALIASES:
        if re.search(rf"\b{re.escape(word)}\b", low):
            if canon not in found:
                found.append(canon)
    # Stable display order matching the registry.
    order = list(_DEPT_META.keys())
    found.sort(key=lambda d: order.index(d) if d in order else 999)
    return found


_CJK_DATE_HINT_RE = re.compile(
    r"今天|今日|今晚|明天|明日|后天|昨天|昨日|"
    r"这(?:个|一)?(?:星期|礼拜|周)|本(?:星期|周)|"
    r"下(?:个|一)?(?:星期|礼拜|周)|上(?:个|一)?(?:星期|礼拜|周)|"
    r"(?:星期|周|週|礼拜|禮拜)\s*[一二三四五六日天]"
)


def _has_date_hint(text: str) -> bool:
    t = text or ""
    return bool(_DATE_HINT_RE.search(t) or _CJK_DATE_HINT_RE.search(t))


# --------------------------------------------------------------------------- #
# Deterministic date parsing (fallback when no LLM)
# --------------------------------------------------------------------------- #
_WEEKDAYS = {
    "monday": 0, "mon": 0,
    "tuesday": 1, "tue": 1, "tues": 1,
    "wednesday": 2, "wed": 2,
    "thursday": 3, "thu": 3, "thurs": 3,
    "friday": 4, "fri": 4,
    "saturday": 5, "sat": 5,
    "sunday": 6, "sun": 6,
}

_NUM_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "a": 1, "an": 1, "couple": 2, "few": 3,
}


def _week_range(anchor: date) -> list[date]:
    monday = anchor - timedelta(days=anchor.weekday())
    return [monday + timedelta(days=i) for i in range(7)]


# Relative-week tokens in English AND Chinese (下个星期 / 下周 / 这周 / 上星期 …).
_NEXT_WEEK_RE = re.compile(r"(?i)\bnext\s+week\b|下(?:个|一)?(?:星期|礼拜|周)")
_THIS_WEEK_RE = re.compile(r"(?i)\bthis\s+week\b|这(?:个|一)?(?:星期|礼拜|周)|本(?:星期|周)")
_LAST_WEEK_RE = re.compile(r"(?i)\blast\s+week\b|上(?:个|一)?(?:星期|礼拜|周)")


def _week_offset(text: str) -> Optional[int]:
    """Return -1/0/1 for last/this/next week (English or Chinese), else None."""
    raw = text or ""
    if _NEXT_WEEK_RE.search(raw):
        return 1
    if _LAST_WEEK_RE.search(raw):
        return -1
    if _THIS_WEEK_RE.search(raw):
        return 0
    return None


def _safe_day(year: int, month: int, day: int) -> Optional[date]:
    try:
        last = calendar.monthrange(year, month)[1]
        if 1 <= day <= last:
            return date(year, month, day)
    except Exception:
        pass
    return None


def _add_months(d: date, months: int) -> tuple[int, int]:
    """Return (year, month) of ``d`` shifted by ``months``."""
    idx = (d.year * 12 + (d.month - 1)) + months
    return idx // 12, (idx % 12) + 1


def regex_parse_dates(text: str, *, today: Optional[date] = None) -> list[date]:
    """Best-effort extraction of dates from free text. Never raises."""
    today = today or _today()
    raw = (text or "").lower()
    dates: list[date] = []

    def push(d: Optional[date]) -> None:
        if d and d not in dates:
            dates.append(d)

    # this/next/last week → 7-day range (English or Chinese)
    wk = _week_offset(raw)
    if wk is not None:
        for d in _week_range(today + timedelta(days=7 * wk)):
            push(d)

    # "after N days" / "in N days" / "N days later" (numeric or word)
    for m in re.finditer(
        r"\b(?:after|in)\s+(\d+|[a-z]+)\s+days?\b|\b(\d+|[a-z]+)\s+days?\s+later\b",
        raw,
    ):
        token = (m.group(1) or m.group(2) or "").strip()
        n = None
        if token.isdigit():
            n = int(token)
        elif token in _NUM_WORDS:
            n = _NUM_WORDS[token]
        if n is not None and 0 < n < 400:
            push(today + timedelta(days=n))

    # day after tomorrow / 后天
    if re.search(r"\bday\s+after\s+tomorrow\b", raw) or "后天" in raw:
        push(today + timedelta(days=2))
    # tomorrow / tmr / tmrw / tmmr / tomo / 明天 / 明日
    if re.search(r"\b(tomorrow|tmr|tmrw|tmmr|tomo|tmw)\b", raw) or re.search(r"明天|明日", raw):
        push(today + timedelta(days=1))
    # yesterday / 昨天 / 昨日
    if re.search(r"\byesterday\b", raw) or re.search(r"昨天|昨日", raw):
        push(today - timedelta(days=1))
    # today / tonight / now / 今天 / 今日 / 今晚
    if re.search(r"\b(today|tonight|now)\b", raw) or re.search(r"今天|今日|今晚", raw):
        push(today)

    # "next month 16" / "16 next month" / "16th of next month"
    nm_year, nm_month = _add_months(today, 1)
    for m in re.finditer(
        r"\bnext\s+month\s+(?:the\s+)?(\d{1,2})(?:st|nd|rd|th)?\b"
        r"|\b(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?next\s+month\b",
        raw,
    ):
        day = int(m.group(1) or m.group(2))
        push(_safe_day(nm_year, nm_month, day))
    if re.search(r"\bnext\s+month\b", raw) and not any(
        d.month == nm_month and d.year == nm_year for d in dates
    ) and not re.search(r"\d", raw):
        # bare "next month" → same day next month
        push(_safe_day(nm_year, nm_month, min(today.day, 28)))

    # "this month 16" / "on the 16th"
    for m in re.finditer(
        r"\bthis\s+month\s+(?:the\s+)?(\d{1,2})(?:st|nd|rd|th)?\b"
        r"|\bon\s+the\s+(\d{1,2})(?:st|nd|rd|th)?\b",
        raw,
    ):
        day = int(m.group(1) or m.group(2))
        push(_safe_day(today.year, today.month, day))

    # ISO date YYYY-MM-DD
    for m in re.finditer(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", raw):
        push(_safe_day(int(m.group(1)), int(m.group(2)), int(m.group(3))))

    # DD/MM or DD/MM/YYYY or DD-MM (day-first, matches the bot's other commands)
    for m in re.finditer(r"\b(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?\b", raw):
        d_, mo_, y_ = m.group(1), m.group(2), m.group(3)
        year = today.year
        if y_:
            year = int(y_)
            if year < 100:
                year += 2000
        push(_safe_day(year, int(mo_), int(d_)))

    # Weekday names → the upcoming occurrence (today counts).
    for name, wd in _WEEKDAYS.items():
        if re.search(rf"\b{name}\b", raw):
            delta = (wd - today.weekday()) % 7
            push(today + timedelta(days=delta))

    # Chinese weekdays: 星期一/周一/礼拜一 … 星期日|天/周日|天 (一..六 → 0..5, 日|天 → 6).
    for m in re.finditer(r"(?:星期|周|週|礼拜|禮拜)\s*([一二三四五六日天])", raw):
        ch = m.group(1)
        wd = {"一": 0, "二": 1, "三": 2, "四": 3, "五": 4, "六": 5, "日": 6, "天": 6}.get(ch)
        if wd is not None:
            delta = (wd - today.weekday()) % 7
            push(today + timedelta(days=delta))

    # Dedup + sort + cap.
    uniq = sorted(set(dates))
    return uniq[:_MAX_DATES]


# --------------------------------------------------------------------------- #
# LLM-based parsing (preferred)
# --------------------------------------------------------------------------- #
def _llm_available() -> bool:
    try:
        import chatagent as ca

        return ca.llm_available()
    except Exception:
        return False


_PARSE_SYSTEM_PROMPT = (
    "You parse messages for a workplace Duty Bot. The user asks for on-call / duty "
    "rosters for one or more departments on one or more dates, OR asks who is on "
    "leave. Resolve EVERY relative date to an absolute date using the provided "
    "'today'.\n"
    "Known departments (use these exact lowercase keys): ose, fpms, pms, cpms, bi, "
    "fe, sre, db, liveslot, ote, ft. Map 'dba'/'database' -> db, 'live slot' -> liveslot.\n"
    "Reply with ONE JSON object, no prose, exactly these keys:\n"
    '  "kind": one of "duty", "leave", "none".\n'
    '  "departments": array of department keys (empty for leave/none).\n'
    '  "dates": array of "YYYY-MM-DD" strings.\n'
    "Rules:\n"
    "- 'this week' / 这周 / 这个星期 / 本周 -> the 7 dates Monday..Sunday of this week.\n"
    "- 'next week' / 下周 / 下个星期 / 下星期 / 下礼拜 -> the 7 dates Monday..Sunday of NEXT week.\n"
    "- 'last week' / 上周 / 上个星期 -> Monday..Sunday of last week.\n"
    "- 'tomorrow'/'tmr'/'tmmr'/明天 -> today + 1 day. 'after two days'/后天 -> +2 days. 今天 -> today.\n"
    "- Chinese weekdays: 星期一/周一 -> Monday … 星期日/周日/星期天 -> Sunday (upcoming occurrence).\n"
    "- 'next month 16' -> the 16th of next month.\n"
    "- If it asks about leave / who is off / vacation, kind='leave' (default dates = [today]).\n"
    "- If it is NOT about duty or leave, kind='none' with empty arrays.\n"
    "- If duty is requested without any date, use [today].\n"
    'Example: {"kind": "duty", "departments": ["ose", "fpms"], "dates": ["2026-06-27"]}'
)


def _llm_parse(text: str, *, today: date, session_key: Optional[str] = None) -> Optional[dict]:
    try:
        import chatagent as ca
    except Exception:
        return None
    if not ca.llm_available():
        return None
    api_key = ca._llm_api_key()
    if not api_key:
        return None

    user = (
        f"today is {today.isoformat()} ({today.strftime('%A')}).\n"
        f"message: {text.strip()}"
    )
    payload = {
        "model": ca._llm_model_for_request(images=False),
        "messages": [
            {"role": "system", "content": _PARSE_SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ],
        "max_tokens": 400,
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
        print(f"[dutyai] LLM parse start model={payload['model']!r} text={text[:80]!r}", flush=True)
        t0 = time.perf_counter()
        with urllib.request.urlopen(req, timeout=ca._llm_timeout_sec()) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        choices = body.get("choices") or []
        if not choices:
            return None
        content = (choices[0].get("message") or {}).get("content") or ""
        parsed = _parse_json_obj(content)
        print(
            f"[dutyai] LLM parse done: {(time.perf_counter() - t0) * 1000:.0f}ms",
            flush=True,
        )
        return parsed
    except Exception as exc:
        print(f"⚠️ dutyai LLM parse failed: {exc!r}", flush=True)
        return None


def _parse_json_obj(raw: str) -> Optional[dict]:
    s = (raw or "").strip()
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


# --------------------------------------------------------------------------- #
# Request parsing (public)
# --------------------------------------------------------------------------- #
def parse_request(text: str, *, session_key: Optional[str] = None) -> dict:
    """
    Return ``{"kind": "duty"|"leave"|"none", "departments": [...], "dates": [date,...]}``.
    Cheap keyword gate first → LLM (if available) → regex fallback. Never raises.
    """
    out = {"kind": "none", "departments": [], "dates": []}
    raw = (text or "").strip()
    if not raw or raw.lstrip().startswith("/"):
        return out

    # Hard skip: maintenance / jenkins / credit / check etc. own these messages.
    if _SKIP_RE.search(raw):
        return out
    # WFH questions belong to the existing ``/wfh`` flow.
    if _WFH_RE.search(raw):
        return out

    has_dept = bool(_find_departments(raw))
    has_leave = bool(_LEAVE_SIGNAL_RE.search(raw))
    has_duty_word = bool(_DUTY_SIGNAL_RE.search(raw))
    has_date = _has_date_hint(raw)

    # Monthly leave / WFH questions belong to the existing ``/leave`` & ``/wfh``
    # month cards — don't intercept those here.
    if has_leave and (_LEAVE_MONTH_RE.search(raw) or _WFH_RE.search(raw)):
        return out

    # Nothing duty/leave-ish → let normal routing handle it (no LLM call).
    if not (has_leave or ((has_dept or has_duty_word) and (has_date or has_duty_word or has_dept))):
        return out
    if not (has_dept or has_leave):
        # A bare "duty today" with no department isn't actionable here.
        return out

    today = _today()

    # 1) Deterministic path first — avoids a 35b LLM round-trip for common phrasing.
    depts = _find_departments(raw)
    dates = regex_parse_dates(raw, today=today)
    if has_leave and not has_dept:
        if dates or _LEAVE_SIGNAL_RE.search(raw):
            return {"kind": "leave", "departments": depts, "dates": dates or [today]}
    if depts and (dates or has_duty_word or has_date):
        print(f"[dutyai] regex parse (no LLM): {raw[:80]!r} → {depts} {len(dates)} date(s)", flush=True)
        return {"kind": "duty", "departments": depts, "dates": dates or [today]}

    # 2) LLM only when regex/keywords could not resolve dept + date.
    if _llm_available() and (os.getenv("BOT_DUTYAI_USE_LLM") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    ):
        obj = _llm_parse(raw, today=today, session_key=session_key)
    else:
        obj = None
    if obj:
        kind = str(obj.get("kind") or "none").strip().lower()
        if kind not in ("duty", "leave", "none"):
            kind = "none"
        depts = _normalise_depts(obj.get("departments"))
        dates = _normalise_dates(obj.get("dates"))
        # A this/next/last week request (English OR Chinese e.g. 下个星期) ALWAYS means
        # the full 7-day range — override the LLM if it collapsed it to a single day.
        wk = _week_offset(raw)
        if wk is not None and kind in ("duty", "leave"):
            dates = _week_range(today + timedelta(days=7 * wk))
        if kind == "duty" and not dates:
            dates = [today]
        if kind == "leave" and not dates:
            dates = [today]
        if kind == "duty" and not depts:
            # LLM said duty but gave no department — fall back to keyword scan.
            depts = _find_departments(raw)
        if kind != "none" and (depts or kind == "leave"):
            return {"kind": kind, "departments": depts, "dates": dates[:_MAX_DATES]}

    # 3) Last-resort keyword scan (partial phrases the LLM might also miss).
    depts = _find_departments(raw)
    dates = regex_parse_dates(raw, today=today)
    if has_leave:
        return {"kind": "leave", "departments": depts, "dates": dates or [today]}
    if depts:
        return {"kind": "duty", "departments": depts, "dates": dates or [today]}
    return out


def message_needs_dutyai_cards(text: str) -> bool:
    """True when dutyai adds value beyond a bare ``/fpms``-style slash mapping."""
    raw = (text or "").strip()
    if not raw:
        return False
    if _week_offset(raw) is not None:
        return True
    if len(_find_departments(raw)) > 1:
        return True
    if re.search(r"(?i)\b(after|in)\s+\d+\s+days?\b|\b\d+\s+days?\s+later\b", raw):
        return True
    if re.search(r"(?i)\b(tmmr|tmrw|tomo|tomorrow|yesterday|day\s+after\s+tomorrow)\b", raw):
        return True
    if re.search(r"明天|后天|昨天|今日|今天", raw):
        return True
    return False


def _normalise_depts(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    alias = dict(_DEPT_ALIASES)
    for item in value:
        key = str(item or "").strip().lower()
        key = alias.get(key, key)
        if key in _DEPT_META and key not in out:
            out.append(key)
    order = list(_DEPT_META.keys())
    out.sort(key=lambda d: order.index(d))
    return out


def _normalise_dates(value: Any) -> list[date]:
    if not isinstance(value, list):
        return []
    out: list[date] = []
    for item in value:
        s = str(item or "").strip()
        d = None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                d = datetime.strptime(s, fmt).date()
                break
            except ValueError:
                continue
        if d and d not in out:
            out.append(d)
    return sorted(out)


# --------------------------------------------------------------------------- #
# Per-department, per-date roster text adapters (read-only)
# --------------------------------------------------------------------------- #
def _clean_body(text: Optional[str]) -> str:
    """Normalise a department's text for card display (drop redundant date header)."""
    s = (text or "").strip()
    if not s:
        return ""
    lines = s.splitlines()
    # Drop a leading "📅 ... – date" / "Today Date:" header (the card shows the date).
    if lines and (lines[0].lstrip().startswith("📅") or lines[0].lower().startswith("today date")):
        lines = lines[1:]
    cleaned = "\n".join(lines).strip()
    # lark_md uses **bold**, not <b>.
    cleaned = cleaned.replace("<b>", "**").replace("</b>", "**")
    cleaned = re.sub(r"</?[a-zA-Z]+[^>]*>", "", cleaned)
    return cleaned.strip()


def _fpms_text_for_date(d: date) -> Optional[str]:
    import fpms_duty

    duty_map = fpms_duty.get_month_duty_map(d.year, d.month)
    names = (duty_map or {}).get(d.day, [])
    if not names:
        return None
    lines = []
    for n in names:
        try:
            phone = fpms_duty.get_phone(n)
        except Exception:
            phone = ""
        lines.append(f"• {n}  📞 {phone}" if phone else f"• {n}")
    return "\n".join(lines)


def _cpms_text_for_date(d: date) -> Optional[str]:
    import cpms_duty

    _, main_name, main_phone, backup_name, backup_phone = cpms_duty.get_cpms_duty_for_date(d)
    sections = []
    if main_name:
        sections.append(f"🟢 **Main**\n• {main_name}  📞 {main_phone}".rstrip())
    if backup_name:
        sections.append(f"🔵 **Backup**\n• {backup_name}  📞 {backup_phone}".rstrip())
    return "\n\n".join(sections) or None


def _pms_text_for_date(d: date) -> Optional[str]:
    import pms_duty

    values, target_year = pms_duty._fetch_sheet_data(d.year)
    start, end, first, second, final_names = pms_duty._find_duty_for_date(values, target_year, d)
    if not (first or second or final_names):
        return None
    return _clean_body(pms_duty._format_week_duty(start, end, first, second, final_names))


def _today_only_text(dept: str, d: date) -> Optional[str]:
    today = _today()
    note = ""
    if d != today:
        note = (
            f"\n\n_ℹ️ {_DEPT_META[dept][0]} only publishes the current roster; "
            f"showing the latest available (asked for {d.strftime('%d/%m/%Y')})._"
        )
    if dept == "bi":
        import bi_duty

        body = _clean_body(bi_duty.get_bi_today_duty())
    elif dept == "fe":
        import fe_duty

        body = _clean_body(fe_duty.get_fe_today_duty())
    else:  # ft
        import ft

        body = _clean_body(ft.get_ft_three_days())
    return (body + note).strip() if body else None


def _dept_text_for_date(dept: str, d: date) -> Optional[str]:
    """Return roster body text for a department + date, or ``None`` if no duty.

    Wrapped by callers; may raise on transient API errors (handled upstream).
    """
    if dept == "ose":
        import ose_Duty

        return _clean_body(ose_Duty.get_ose_duty_for_date(d))
    if dept == "fpms":
        return _fpms_text_for_date(d)
    if dept == "cpms":
        return _cpms_text_for_date(d)
    if dept == "pms":
        return _pms_text_for_date(d)
    if dept == "sre":
        import sre_Duty

        return _clean_body(sre_Duty.get_sre_duty(d))
    if dept == "db":
        import db_duty

        return _clean_body(db_duty.get_db_day_duty(d))
    if dept == "liveslot":
        import liveslot_duty

        return _clean_body(liveslot_duty.get_day_duty(d))
    if dept == "ote":
        import ote_duty

        return _clean_body(ote_duty.get_day_duty(d))
    if dept in _TODAY_ONLY_DEPTS:
        return _today_only_text(dept, d)
    return None


# --------------------------------------------------------------------------- #
# Card rendering
# --------------------------------------------------------------------------- #
def _md_div(content: str) -> dict:
    return {"tag": "div", "text": {"tag": "lark_md", "content": content}}


def _date_range_label(start: date, end: date) -> str:
    """A human label for one day or a day-span (used as a section header)."""
    if start == end:
        return start.strftime("%A, %d %b %Y")
    if start.year == end.year and start.month == end.month:
        return f"{start.strftime('%a %d')} – {end.strftime('%a %d %b %Y')}"
    return f"{start.strftime('%a, %d %b')} – {end.strftime('%a, %d %b %Y')}"


def _group_consecutive_same(
    day_bodies: list[tuple[date, str]]
) -> list[tuple[date, date, str]]:
    """Merge consecutive calendar days that share the same roster body.

    e.g. a Mon–Fri week where it's the same person every day collapses into a
    single ``(Mon, Fri, body)`` group instead of five identical rows.
    """
    groups: list[tuple[date, date, str]] = []
    for d, body in day_bodies:
        if (
            groups
            and groups[-1][2] == body
            and groups[-1][1] + timedelta(days=1) == d
        ):
            s, _e, b = groups[-1]
            groups[-1] = (s, d, b)
        else:
            groups.append((d, d, body))
    return groups


def _build_card(*, title: str, colour: str, elements: list[dict]) -> dict:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": colour,
            "title": {"tag": "plain_text", "content": title},
        },
        "body": {"elements": elements},
    }


def _dept_card_payload(dept: str, dates: list[date]) -> dict:
    """Build a single card covering one department across one or more dates."""
    display, emoji, colour = _DEPT_META[dept]

    # OSE single date → reuse its rich native card (offset / leave sections).
    if dept == "ose" and len(dates) == 1:
        try:
            import ose_Duty

            payload = ose_Duty.get_ose_payload_for_date(dates[0], mode="date")
            if isinstance(payload, dict) and payload.get("lark_card"):
                return {"text": payload.get("text") or "", "lark_card": payload["lark_card"]}
        except Exception as exc:
            print(f"⚠️ dutyai OSE native card failed: {exc!r}", flush=True)

    # Look up each day's roster, then collapse consecutive days with the SAME
    # roster into one range (so a Mon–Fri week with one person shows once).
    day_bodies: list[tuple[date, str]] = []
    sre_bulk: dict[date, str] | None = None
    if dept == "sre" and len(dates) >= 1:
        try:
            import sre_Duty

            sre_bulk = sre_Duty.get_sre_duty_bulk(dates)
        except Exception as exc:
            print(f"⚠️ dutyai SRE bulk fetch failed: {exc!r}", flush=True)
            sre_bulk = None
    for d in dates:
        try:
            if sre_bulk is not None:
                raw_body = sre_bulk.get(d) or "• No duty assigned"
                body = _clean_body(raw_body)
            else:
                body = _dept_text_for_date(dept, d)
        except Exception as exc:
            body = None
            print(f"⚠️ dutyai {dept} {d} lookup failed: {exc!r}", flush=True)
        day_bodies.append((d, body or "• No duty assigned"))

    elements: list[dict] = []
    text_lines: list[str] = []
    for i, (start, end, body) in enumerate(_group_consecutive_same(day_bodies)):
        if i > 0:
            elements.append({"tag": "hr"})
        label = _date_range_label(start, end)
        elements.append(_md_div(f"📅 **{label}**\n{body}"))
        text_lines.append(f"{label}\n{body}")

    if len(dates) == 1:
        title = f"{emoji} {display} DUTY · {dates[0].strftime('%d/%m/%Y')}"
    else:
        title = (
            f"{emoji} {display} DUTY · {dates[0].strftime('%d/%m')}"
            f"–{dates[-1].strftime('%d/%m/%Y')}"
        )

    card = _build_card(title=title, colour=colour, elements=elements)
    text = f"{display} DUTY\n" + "\n\n".join(text_lines)
    return {"text": text, "lark_card": card}


# --------------------------------------------------------------------------- #
# Leave (read-only)
# --------------------------------------------------------------------------- #
def _leave_payload(dates: list[date]) -> Optional[dict]:
    import leavewfh as lw

    dates = sorted(set(dates)) or [_today()]

    # Single day → reuse the existing "who is on leave today" card.
    if len(dates) == 1:
        try:
            payload = lw.get_wholeave_today_payload(dates[0])
            if isinstance(payload, dict) and payload.get("lark_card"):
                return {"text": payload.get("text") or "", "lark_card": payload["lark_card"]}
            return {"text": payload.get("text") or "No leave data.", "lark_card": None}
        except Exception as exc:
            print(f"⚠️ dutyai leave (single) failed: {exc!r}", flush=True)
            return {"text": f"❌ Could not load leave data: {exc}", "lark_card": None}

    # Range → fetch each spanned month once, then list per day.
    try:
        token = lw.get_tenant_access_token()
        months = sorted({(d.year, d.month) for d in dates})
        rows: list[dict] = []
        seen = set()
        for (y, m) in months:
            try:
                month_rows = lw.fetch_approved_leaves_for_month(
                    token,
                    y,
                    m,
                    app_token=lw._OSE_BITABLE_BASE,
                    table_id=lw.od.OSE_HRMS_LEAVE_TABLE_ID,
                    require_approved=False,
                )
            except Exception:
                month_rows = []
            for r in month_rows:
                key = (str(r.get("name")), str(r.get("start")), str(r.get("end")))
                if key not in seen:
                    seen.add(key)
                    rows.append(r)

        any_leave = False
        day_bodies: list[tuple[date, str]] = []
        for d in dates:
            try:
                day_rows = lw.rows_on_leave_date(rows, d)
            except Exception:
                day_rows = []
            if day_rows:
                any_leave = True
                body = "\n".join(
                    f"• {r.get('name')} ({r.get('leave_type') or 'Leave'})" for r in day_rows
                )
            else:
                body = "• Nobody on leave"
            day_bodies.append((d, body))

        # Collapse consecutive days with the identical leave list into one range.
        elements: list[dict] = []
        text_lines: list[str] = []
        for i, (start, end, body) in enumerate(_group_consecutive_same(day_bodies)):
            if i > 0:
                elements.append({"tag": "hr"})
            label = _date_range_label(start, end)
            elements.append(_md_div(f"📅 **{label}**\n{body}"))
            text_lines.append(f"{label}\n{body}")

        if not any_leave:
            elements.append(_md_div("🎉 Nobody is on leave during this period."))
        title = (
            f"🏖️ ON LEAVE · {dates[0].strftime('%d/%m')}–{dates[-1].strftime('%d/%m/%Y')}"
        )
        card = _build_card(title=title, colour="turquoise", elements=elements)
        return {"text": "On leave\n" + "\n\n".join(text_lines), "lark_card": card}
    except Exception as exc:
        print(f"⚠️ dutyai leave (range) failed: {exc!r}", flush=True)
        return {"text": f"❌ Could not load leave data: {exc}", "lark_card": None}


# --------------------------------------------------------------------------- #
# Top-level handler
# --------------------------------------------------------------------------- #
def handle(text: str, *, session_key: Optional[str] = None) -> Optional[list[dict]]:
    """
    Parse a free-form duty/leave request and return a list of card payloads
    (``[{"text", "lark_card"}, ...]``), or ``None`` to let normal routing run.
    Never raises.
    """
    if not is_enabled():
        return None
    try:
        parsed = parse_request(text, session_key=session_key)
    except Exception as exc:
        print(f"⚠️ dutyai parse error: {exc!r}", flush=True)
        return None

    kind = parsed.get("kind")
    if kind == "leave":
        dates = parsed.get("dates") or [_today()]
        payload = _leave_payload(dates)
        return [payload] if payload else None

    if kind != "duty":
        return None

    depts = parsed.get("departments") or []
    dates = parsed.get("dates") or [_today()]
    if not depts:
        return None
    dates = sorted(set(dates))[:_MAX_DATES]

    payloads: list[dict] = []
    for dept in depts:
        if dept not in _DEPT_META:
            continue
        try:
            payloads.append(_dept_card_payload(dept, dates))
        except Exception as exc:
            print(f"⚠️ dutyai card build failed for {dept}: {exc!r}", flush=True)
        if len(payloads) >= _MAX_CARDS:
            break
    return payloads or None


def startup_status() -> None:
    print(
        f"[dutyai] BOT_USE_DUTYAI={os.getenv('BOT_USE_DUTYAI')!r} enabled={is_enabled()} "
        f"llm={'yes' if _llm_available() else 'no'} depts={len(_DEPT_META)}",
        flush=True,
    )
    if not is_enabled():
        print("[dutyai] OFF — duty/leave NL → cards disabled.", flush=True)
    elif _llm_available():
        print("[dutyai] ✅ Ready — AI parses random duty/leave requests into cards.", flush=True)
    else:
        print("[dutyai] ✅ Ready (regex date parser; no LLM configured).", flush=True)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _cli(args: list[str]) -> None:
    parse_only = False
    if args and args[0] in ("--parse", "-p"):
        parse_only = True
        args = args[1:]
    text = " ".join(args)
    if not text:
        print('Usage: python dutyai.py [--parse] "i want ose and fpms duty tomorrow"')
        return
    parsed = parse_request(text)
    dates = parsed.get("dates") or []
    print(f"Input:       {text!r}")
    print(f"Kind:        {parsed.get('kind')}")
    print(f"Departments: {parsed.get('departments')}")
    print(f"Dates:       {[d.isoformat() for d in dates]}")
    if parse_only:
        return
    print("-" * 50)
    payloads = handle(text)
    if not payloads:
        print("(no duty/leave payloads — normal routing would handle this)")
        return
    for i, p in enumerate(payloads, 1):
        print(f"\n=== Payload {i} ===")
        print(p.get("text") or "(no text)")
        print(f"[card: {'yes' if p.get('lark_card') else 'no'}]")


if __name__ == "__main__":
    import sys

    _cli(sys.argv[1:])
