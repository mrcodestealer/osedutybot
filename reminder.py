from __future__ import annotations

import re
from datetime import datetime, timedelta
import functools
import json
import os
from datetime import date
import requests
from dotenv import load_dotenv
load_dotenv()
def parse_duration(duration_str):
    pattern = re.compile(r'^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$', re.IGNORECASE)
    match = pattern.match(duration_str.strip())
    if not match:
        raise ValueError(f"Invalid duration format: {duration_str}. Use e.g., 1h30m, 45s, 2h5s")
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    total = hours * 3600 + minutes * 60 + seconds
    if total <= 0:
        raise ValueError("Duration must be positive")
    return total

def parse_absolute_time(time_str):
    """Convert a time string like '8:39PM', '2039', '8pm' into a datetime (today or tomorrow)."""
    time_str = time_str.strip().lower()
    now = datetime.now()
    today = now.date()

    # Pattern 1: HH:MMam/pm
    match = re.match(r'^(\d{1,2}):(\d{2})\s*(am|pm)$', time_str)
    if match:
        hour = int(match.group(1))
        minute = int(match.group(2))
        meridian = match.group(3)
        if meridian == 'pm' and hour != 12:
            hour += 12
        elif meridian == 'am' and hour == 12:
            hour = 0
        dt = datetime.combine(today, datetime.min.time().replace(hour=hour, minute=minute))
        if dt < now:
            dt += timedelta(days=1)
        return dt

    # Pattern 2: HHam/pm (no colon)
    match = re.match(r'^(\d{1,2})\s*(am|pm)$', time_str)
    if match:
        hour = int(match.group(1))
        minute = 0
        meridian = match.group(2)
        if meridian == 'pm' and hour != 12:
            hour += 12
        elif meridian == 'am' and hour == 12:
            hour = 0
        dt = datetime.combine(today, datetime.min.time().replace(hour=hour, minute=minute))
        if dt < now:
            dt += timedelta(days=1)
        return dt

    # Pattern 3: HHMM (24-hour)
    if re.match(r'^\d{4}$', time_str):
        hour = int(time_str[:2])
        minute = int(time_str[2:])
        if hour > 23 or minute > 59:
            raise ValueError("Invalid time: hours must be 00-23, minutes 00-59")
        dt = datetime.combine(today, datetime.min.time().replace(hour=hour, minute=minute))
        if dt < now:
            dt += timedelta(days=1)
        return dt

    # Pattern 4: HH:MM (24-hour)
    match = re.match(r'^(\d{1,2}):(\d{2})$', time_str)
    if match:
        hour = int(match.group(1))
        minute = int(match.group(2))
        if hour > 23 or minute > 59:
            raise ValueError("Invalid time: hours must be 00-23, minutes 00-59")
        dt = datetime.combine(today, datetime.min.time().replace(hour=hour, minute=minute))
        if dt < now:
            dt += timedelta(days=1)
        return dt

    raise ValueError(f"Unsupported time format: {time_str}. Use e.g., 8:39PM, 2039, 8pm, 20:39")

def schedule_reminder(chat_id, user_id, duration_str, message, scheduler, send_func):
    try:
        delay_seconds = parse_duration(duration_str)
    except ValueError as e:
        return str(e)

    run_time = datetime.now() + timedelta(seconds=delay_seconds)
    reminder_text = f'<at user_id="{user_id}">you</at> ⏰ Reminder: {message}'
    job = scheduler.add_job(func=send_func, trigger='date', run_date=run_time, args=[chat_id, reminder_text])

    # 返回 job 对象，以便外部取消
    return job

def schedule_reminder_absolute(chat_id, user_id, time_str, message, scheduler, send_func):
    try:
        run_time = parse_absolute_time(time_str)
    except ValueError as e:
        return str(e)

    reminder_text = f'<at user_id="{user_id}">you</at> ⏰ Reminder: {message}'
    scheduler.add_job(func=send_func, trigger='date', run_date=run_time, args=[chat_id, reminder_text])

    # Format the time for user feedback (e.g., 08:39 PM)
    time_str_display = run_time.strftime("%I:%M %p").lstrip('0')
    return f"✅ Reminder set for {time_str_display}. I'll remind you about: {message}"


# ================= Sheet-based daily reminders =================

REMINDER_BASE_TOKEN = os.getenv("REMINDERSHEETTOKEN", "").strip()
REMINDER_TABLE_ID = os.getenv("REMINDERSHEETID", "").strip()
REMINDER_FIELD_ID = os.getenv("REMINDER_FIELD_ID", "ID").strip() or "ID"
REMINDER_FIELD_START = os.getenv("REMINDER_FIELD_START", "Start Time").strip() or "Start Time"
REMINDER_FIELD_END = os.getenv("REMINDER_FIELD_END", "End Time").strip() or "End Time"
REMINDER_FIELD_TIME = os.getenv("REMINDER_FIELD_TIME", "Time").strip() or "Time"
REMINDER_FIELD_REASON = os.getenv("REMINDER_FIELD_REASON", "Reason").strip() or "Reason"
# Schedule multi-select column in Bitable: title **when** (lowercase). API writes this key; reads try ``when`` then ``When``.
# Options include **One time** → fires only on the row's Start Time date (see ``when_matches_schedule``).
REMINDER_FIELD_WHEN_WRITE = "when"
REMINDER_FIELD_WHEN_READ_KEYS = ("when", "When")
_WHEN_LABEL_DEFAULT = "Every day"
_SHEET_JOB_PREFIX = "sheet_daily_reminder::"

# Canonical tokens for ``When`` matching (multi-select on Bitable + form).
_WHEN_TOKEN_DAILY = "DAILY"
_WHEN_TOKEN_MONTHLY = "MONTHLY"
_WHEN_TOKEN_ONCE = "ONCE"
_WEEKDAY_TOKENS_ORDER = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")


def lark_card_at_open_id(open_id: str) -> str:
    """
    Mention inside Lark **interactive card** ``lark_md`` / rich text.

    Cards require ``<at id=open_id></at>`` (open_id is usually ``ou_…``).
    Plain chat **text** messages often still use ``<at user_id=\"…\">display</at>`` — do not mix.
    """
    oid = (open_id or "").strip()
    if not oid:
        return ""
    return f"<at id={oid}></at>"


def _resolve_sheet_reminder_mention_id(explicit_user_id: str | None) -> str | None:
    """
    Resolve **open_id** (``ou_…``) for @ in reminder cards.
    Prefer ``omduty`` / ``OMDUTY`` from ``.env`` so sheet reminders tag the duty account
    without hardcoding in callers.
    """
    env_id = (os.getenv("omduty", "").strip() or os.getenv("OMDUTY", "").strip())
    if env_id:
        return env_id
    ex = (explicit_user_id or "").strip()
    return ex if ex else None


def _reminder_sheet_enabled() -> bool:
    return bool(REMINDER_BASE_TOKEN and REMINDER_TABLE_ID)


def _parse_sheet_date(raw: str) -> date:
    s = (raw or "").strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            d = datetime.strptime(s, fmt).date()
            return d
        except ValueError:
            continue
    m = re.match(r"^(\d{1,2})/(\d{1,2})$", s)
    if m:
        a = int(m.group(1))
        b = int(m.group(2))
        y = date.today().year
        # Prefer MM/DD when unambiguous by command docs, but accept DD/MM too.
        if a > 12 and 1 <= b <= 12:
            return date(y, b, a)  # DD/MM
        if b > 12 and 1 <= a <= 12:
            return date(y, a, b)  # MM/DD
        if 1 <= a <= 12 and 1 <= b <= 12:
            return date(y, a, b)  # default MM/DD when ambiguous
    raise ValueError(f"Invalid date `{raw}`. Use YYYY/MM/DD (or MM/DD for current year).")


def _normalize_sheet_date(raw: str) -> str:
    return _parse_sheet_date(raw).strftime("%Y/%m/%d")


def _parse_sheet_date_field(raw) -> date:
    """
    Parse date from sheet field values.
    Supports:
    - int/float milliseconds timestamp (Datetime field)
    - date-like strings (YYYY/MM/DD, YYYY-MM-DD, MM/DD, DD/MM)
    """
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw) / 1000.0).date()
        except Exception as e:
            raise ValueError(f"Invalid datetime timestamp `{raw}`: {e}") from e
    return _parse_sheet_date(str(raw or "").strip())


def _sheet_date_to_timestamp_ms(d: date) -> int:
    """Lark Bitable Datetime field value (milliseconds)."""
    dt = datetime.combine(d, datetime.min.time())
    return int(dt.timestamp() * 1000)


def resolve_add_reminder_form_time(
    time_preset: str | None,
    time_override: str | None,
    *,
    legacy_time: str | None = None,
) -> str:
    """
    Add-reminder card: optional **Exact time** overrides **Time (quick pick)**.
    ``legacy_time`` supports older cards that used a single ``time`` field only.
    """
    o = (time_override or "").strip()
    if o:
        return o
    p = (time_preset or "").strip()
    if p:
        return p
    return (legacy_time or "").strip()


def _normalize_sheet_time(raw: str) -> str:
    """
    Normalize to ``H:MMAPM`` (12-hour) for storage + cron.

    Accepts:
    - 12-hour: ``9:55AM``, ``9:55 AM``, ``6:30pm``
    - hour only: ``9pm`` → ``9:00PM``
    - 24-hour: ``14:30``, ``09:05`` (also used when Lark returns HH:MM)
    """
    s_compact = (raw or "").strip().upper().replace(" ", "")
    # 24-hour H:MM or HH:MM
    m24 = re.match(r"^(\d{1,2}):(\d{2})$", s_compact)
    if m24:
        hh24 = int(m24.group(1))
        mm = int(m24.group(2))
        if not (0 <= hh24 <= 23 and 0 <= mm <= 59):
            raise ValueError(f"Invalid time `{raw}`. Use 0:00–23:59 (24h) or e.g. 9:55AM.")
        ap = "AM" if hh24 < 12 else "PM"
        hh12 = hh24 % 12
        if hh12 == 0:
            hh12 = 12
        return f"{hh12}:{mm:02d}{ap}"
    m = re.match(r"^(\d{1,2}):(\d{2})(AM|PM)$", s_compact)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        ap = m.group(3)
        if not (1 <= hh <= 12 and 0 <= mm <= 59):
            raise ValueError(f"Invalid time `{raw}`. Use HH:MMPM/AM, e.g. 9:55AM.")
        return f"{hh}:{mm:02d}{ap}"
    m = re.match(r"^(\d{1,2})(AM|PM)$", s_compact)
    if m:
        hh = int(m.group(1))
        ap = m.group(2)
        if not (1 <= hh <= 12):
            raise ValueError(f"Invalid time `{raw}`. Use HH:MMPM/AM, e.g. 6:30PM.")
        return f"{hh}:00{ap}"
    raise ValueError(
        f"Invalid time `{raw}`. Examples: 9:55AM, 2:05pm, 14:30 (24-hour), 9pm."
    )


def _time_to_hour_minute(raw: str) -> tuple[int, int]:
    s = _normalize_sheet_time(raw)
    m = re.match(r"^(\d{1,2}):(\d{2})(AM|PM)$", s)
    assert m
    hh = int(m.group(1))
    mm = int(m.group(2))
    ap = m.group(3)
    if ap == "AM":
        hh24 = 0 if hh == 12 else hh
    else:
        hh24 = 12 if hh == 12 else hh + 12
    return hh24, mm


def _bitable_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _bitable_records_url() -> str:
    return f"https://open.larksuite.com/open-apis/bitable/v1/apps/{REMINDER_BASE_TOKEN}/tables/{REMINDER_TABLE_ID}/records"


def _bitable_get_all_records(get_token_func) -> list[dict]:
    if not _reminder_sheet_enabled():
        raise RuntimeError("REMINDERSHEETTOKEN / REMINDERSHEETID is not set in environment.")
    token = get_token_func()
    if not token:
        raise RuntimeError("Failed to get tenant access token.")
    url = _bitable_records_url()
    out: list[dict] = []
    page_token = None
    while True:
        params = {"page_size": 200}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(url, headers=_bitable_headers(token), params=params, timeout=30)
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Fetch reminder sheet failed: {data}")
        dd = data.get("data", {})
        out.extend(dd.get("items") or [])
        if not dd.get("has_more"):
            break
        page_token = dd.get("page_token")
    return out


def _field_text(v) -> str:
    """
    Flatten common Bitable field payloads to plain text.
    Handles plain strings and rich-text arrays like:
    [{"text":"abc","type":"text"}]
    """
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (int, float)):
        return str(v).strip()
    if isinstance(v, list):
        parts: list[str] = []
        for item in v:
            if isinstance(item, str):
                t = item.strip()
                if t:
                    parts.append(t)
            elif isinstance(item, dict):
                t = str(item.get("text") or "").strip()
                if t:
                    parts.append(t)
        return "".join(parts).strip()
    if isinstance(v, dict):
        t = str(v.get("text") or "").strip()
        if t:
            return t
    return str(v).strip()


def _field_when_list(v) -> list[str]:
    """Bitable multi-select / text → list of option labels."""
    if v is None:
        return []
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        return [p.strip() for p in re.split(r"[,，、]", s) if p.strip()]
    if isinstance(v, list):
        out: list[str] = []
        for item in v:
            if isinstance(item, str):
                t = item.strip()
                if t:
                    out.append(t)
            elif isinstance(item, dict):
                t = (
                    str(item.get("name") or item.get("text") or item.get("option_name") or "").strip()
                    or str(item.get("value") or "").strip()
                )
                if t:
                    out.append(t)
        return out
    if isinstance(v, dict):
        t = _field_text(v)
        return [t] if t else []
    return [str(v).strip()] if str(v).strip() else []


def parse_when_form_value(v) -> list[str]:
    """Lark card ``form_value["when"]`` may be list of strings / dicts or a single string."""
    return _field_when_list(v)


def _normalize_when_label(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _label_to_when_tokens(label: str) -> set[str]:
    """Map one multi-select label (any language / wording) to canonical tokens."""
    x = _normalize_when_label(label)
    if not x:
        return set()
    out: set[str] = set()
    if x in (
        "every day",
        "everyday",
        "daily",
        "每天",
        "每日",
        "all days",
    ) or re.search(r"\b(every\s+day|daily)\b", x):
        out.add(_WHEN_TOKEN_DAILY)
    if x in ("every month", "monthly", "每月", "每个月") or re.search(
        r"\b(every\s+month|monthly)\b", x
    ):
        out.add(_WHEN_TOKEN_MONTHLY)
    if x in (
        "one time",
        "onetime",
        "once",
        "once only",
        "single time",
        "单次",
        "仅一次",
        "只提醒一次",
    ) or re.search(r"\b(one[\s-]?time|once\s+only)\b", x):
        out.add(_WHEN_TOKEN_ONCE)

    wd_specs: list[tuple[str, str]] = [
        ("MON", r"\b(every\s+)?monday\b|星期一|周一"),
        ("TUE", r"\b(every\s+)?tuesday\b|星期二|周二"),
        ("WED", r"\b(every\s+)?wednesday\b|星期三|周三"),
        ("THU", r"\b(every\s+)?thursday\b|星期四|周四"),
        ("FRI", r"\b(every\s+)?friday\b|星期五|周五"),
        ("SAT", r"\b(every\s+)?saturday\b|星期六|周六"),
        ("SUN", r"\b(every\s+)?sunday\b|星期日|周日|星期天"),
    ]
    for tok, pat in wd_specs:
        if re.search(pat, x):
            out.add(tok)
    return out


def _when_tokens_from_labels(labels: list[str]) -> tuple[frozenset[str], str]:
    """
    Merge all labels into token set + human display string.
    Empty labels → treat as **every day** (backward compatible).
    """
    labels_n = [str(l).strip() for l in labels if str(l).strip()]
    tokens: set[str] = set()
    for lab in labels_n:
        tokens |= _label_to_when_tokens(lab)
    if not labels_n:
        tokens = {_WHEN_TOKEN_DAILY}
        disp = _WHEN_LABEL_DEFAULT
    elif not tokens:
        tokens.add(_WHEN_TOKEN_DAILY)
        disp = ", ".join(labels_n)
    else:
        disp = ", ".join(labels_n)
    return frozenset(tokens), disp


def _bitable_raw_when_field(fields: dict) -> object | None:
    """Return first non-missing When column payload (table may use ``when`` or legacy ``When``)."""
    if not isinstance(fields, dict):
        return None
    for k in REMINDER_FIELD_WHEN_READ_KEYS:
        if k not in fields:
            continue
        return fields[k]
    return None


def _py_weekday_to_token(wd: int) -> str:
    """Monday=0 … Sunday=6 → MON…SUN."""
    return _WEEKDAY_TOKENS_ORDER[int(wd) % 7]


def when_matches_schedule(
    when_tokens: frozenset[str],
    today: date,
    *,
    row_start_date: date,
) -> bool:
    """
    Whether ``today`` should fire for this row's **When** multi-select.

    - **DAILY**: any day in [start, end].
    - **MON–SUN**: weekday matches one selected day.
    - **MONTHLY**: same calendar day-of-month as **Start Time** (within range).
    Multiple selections are OR'd (e.g. Monday OR monthly on the 15th).
    **ONCE** (One time): fires only on **Start Time** calendar day (still must fall in [start, end]; checked by caller).
    If **ONCE** is selected with other options, **ONCE wins** (single trigger on start date only).
    """
    if _WHEN_TOKEN_ONCE in when_tokens:
        return today == row_start_date
    if _WHEN_TOKEN_DAILY in when_tokens:
        return True
    matched = False
    if _WHEN_TOKEN_MONTHLY in when_tokens and today.day == row_start_date.day:
        matched = True
    wd_sel = set(when_tokens) & set(_WEEKDAY_TOKENS_ORDER)
    if wd_sel and _py_weekday_to_token(today.weekday()) in wd_sel:
        matched = True
    return matched


def _normalize_sheet_rows(records: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for rec in records:
        fields = rec.get("fields") or {}
        rid = str(rec.get("record_id") or "").strip()
        sid = _field_text(fields.get(REMINDER_FIELD_ID))
        start_raw = fields.get(REMINDER_FIELD_START)
        end_raw = fields.get(REMINDER_FIELD_END)
        time_raw = _field_text(fields.get(REMINDER_FIELD_TIME))
        reason = _field_text(fields.get(REMINDER_FIELD_REASON))
        if not (rid and sid and start_raw is not None and end_raw is not None and time_raw and reason):
            continue
        try:
            start_d = _parse_sheet_date_field(start_raw)
            end_d = _parse_sheet_date_field(end_raw)
            time_n = _normalize_sheet_time(time_raw)
        except Exception:
            continue
        when_labels = _field_when_list(_bitable_raw_when_field(fields))
        when_tokens, when_display = _when_tokens_from_labels(when_labels)
        rows.append(
            {
                "record_id": rid,
                "id": sid,
                "start_date": start_d,
                "end_date": end_d,
                "time": time_n,
                "reason": reason,
                "when_tokens": when_tokens,
                "when_display": when_display,
            }
        )
    rows.sort(key=lambda x: (x["start_date"], x["time"], x["id"]))
    return rows


def _sheet_rows_card(
    rows: list[dict],
    *,
    title: str,
    target_user_id: str | None = None,
    include_id: bool = False,
) -> dict:
    lines = []
    mention_id = _resolve_sheet_reminder_mention_id(target_user_id)
    if mention_id:
        lines.append(lark_card_at_open_id(mention_id))
        lines.append("")
    if not rows:
        lines.append("No reminder records found.")
    else:
        for idx, r in enumerate(rows):
            id_line = f"🆔 **ID:** `{r['id']}`\n" if include_id else ""
            lines.append(
                f"{id_line}"
                f"📅 **Start Time:** `{r['start_date'].strftime('%Y/%m/%d')}`\n"
                f"📅 **End Time:** `{r['end_date'].strftime('%Y/%m/%d')}`\n"
                f"📆 **When:** `{r.get('when_display') or _WHEN_LABEL_DEFAULT}`\n"
                f"⏰ **Time:** `{r['time']}`\n"
                f"📝 **Reason:** {r['reason']}"
            )
            if idx < len(rows) - 1:
                lines.append("")
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": "blue", "title": {"tag": "plain_text", "content": title}},
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}}],
    }


def _reminder_v2_callback_value(payload: dict) -> dict:
    out = {}
    for k, v in payload.items():
        ks = str(k)
        if isinstance(v, (dict, list)):
            out[ks] = v
        elif v is None:
            out[ks] = ""
        else:
            out[ks] = str(v)
    return out


def _reminder_v2_callback_button(
    label: str,
    payload: dict,
    *,
    btn_type: str = "default",
    element_id: str = "",
) -> dict:
    btn = {
        "tag": "button",
        "text": {"tag": "plain_text", "content": label},
        "type": btn_type,
        "behaviors": [{"type": "callback", "value": _reminder_v2_callback_value(payload)}],
    }
    eid = (element_id or "").strip()[:20]
    if eid:
        btn["element_id"] = eid
    return btn


def _sheet_delete_picker_card(rows: list[dict]) -> dict:
    def _button_row(btn: dict) -> dict:
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
                    "elements": [btn],
                }
            ],
        }

    elems = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "Tap an ID button to delete one reminder.",
            },
        }
    ]
    if not rows:
        elems.append({"tag": "div", "text": {"tag": "plain_text", "content": "No reminder records found."}})
    else:
        for i, r in enumerate(rows):
            rid = str(r.get("id") or "").strip()
            elems.append(
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"🆔 **ID:** `{rid}`\n"
                            f"📅 `{r['start_date'].strftime('%Y/%m/%d')}` → `{r['end_date'].strftime('%Y/%m/%d')}`\n"
                            f"📆 `{r.get('when_display') or _WHEN_LABEL_DEFAULT}`\n"
                            f"⏰ `{r['time']}`\n"
                            f"📝 {r['reason']}"
                        ),
                    },
                }
            )
            btn = _reminder_v2_callback_button(
                f"🆔 ID: `{rid}`",
                {"k": "rem_del", "id": rid},
                btn_type="danger",
                element_id=f"remdel_{i}"[:20],
            )
            elems.append(_button_row(btn))
            if i < len(rows) - 1:
                # visual gap between records
                elems.append({"tag": "div", "text": {"tag": "plain_text", "content": " "}})
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "📋 Reminder List"},
        },
        "body": {"elements": elems},
    }


def _send_daily_sheet_reminder(
    send_func,
    *,
    chat_id: str,
    target_user_id: str,
    row: dict,
) -> None:
    today = date.today()
    if not (row["start_date"] <= today <= row["end_date"]):
        return
    wt = row.get("when_tokens")
    if not isinstance(wt, frozenset):
        wt = frozenset({_WHEN_TOKEN_DAILY})
    if not when_matches_schedule(wt, today, row_start_date=row["start_date"]):
        return
    mention_id = _resolve_sheet_reminder_mention_id(target_user_id)
    at_line = (f"{lark_card_at_open_id(mention_id)}\n\n" if mention_id else "")
    card = {
        "config": {"wide_screen_mode": True},
        "header": {"template": "orange", "title": {"tag": "plain_text", "content": "⏰ Reminder"}},
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"{at_line}"
                        f"📅 **Start Time:** `{row['start_date'].strftime('%Y/%m/%d')}`\n"
                        f"📅 **End Time:** `{row['end_date'].strftime('%Y/%m/%d')}`\n"
                        f"📆 **When:** `{row.get('when_display') or _WHEN_LABEL_DEFAULT}`\n"
                        f"⏰ **Time:** `{row['time']}`\n"
                        f"📝 **Reason:** {row['reason']}"
                    ),
                },
            }
        ],
    }
    send_func(chat_id, json.dumps(card), msg_type="interactive")


def sync_sheet_daily_reminders(
    *,
    scheduler,
    send_func,
    get_token_func,
    chat_id: str,
    target_user_id: str,
) -> tuple[int, int]:
    """
    Reload reminder jobs from Lark Sheet.
    Returns (scheduled_count, total_valid_rows).
    """
    if not _reminder_sheet_enabled():
        return 0, 0

    for j in scheduler.get_jobs():
        if str(j.id).startswith(_SHEET_JOB_PREFIX):
            scheduler.remove_job(j.id)

    rows = _normalize_sheet_rows(_bitable_get_all_records(get_token_func))
    scheduled = 0
    for row in rows:
        hh, mm = _time_to_hour_minute(row["time"])
        jid = f"{_SHEET_JOB_PREFIX}{row['record_id']}"
        scheduler.add_job(
            func=_send_daily_sheet_reminder,
            trigger="cron",
            hour=hh,
            minute=mm,
            id=jid,
            replace_existing=True,
            kwargs={
                "send_func": send_func,
                "chat_id": chat_id,
                "target_user_id": target_user_id,
                "row": row,
            },
        )
        scheduled += 1
    return scheduled, len(rows)


def list_sheet_reminders(*, get_token_func) -> list[dict]:
    if not _reminder_sheet_enabled():
        raise RuntimeError("REMINDERSHEETTOKEN / REMINDERSHEETID is not set in environment.")
    return _normalize_sheet_rows(_bitable_get_all_records(get_token_func))


def add_sheet_reminder(
    *,
    start_raw: str,
    end_raw: str,
    time_raw: str,
    reason: str,
    get_token_func,
    scheduler,
    send_func,
    chat_id: str,
    target_user_id: str,
    schedule_chat_id: str | None = None,
    when_labels: list[str] | None = None,
) -> str:
    if not _reminder_sheet_enabled():
        return "❌ REMINDERSHEETTOKEN / REMINDERSHEETID is not set."
    reason_n = (reason or "").strip()
    if not reason_n:
        return "❌ Reason cannot be empty."
    try:
        start_s = _normalize_sheet_date(start_raw)
        end_s = _normalize_sheet_date(end_raw)
        time_s = _normalize_sheet_time(time_raw)
        start_d = _parse_sheet_date(start_s)
        end_d = _parse_sheet_date(end_s)
    except ValueError as e:
        return f"❌ {e}"
    if end_d < start_d:
        return "❌ End Time cannot be earlier than Start Time."

    token = get_token_func()
    if not token:
        return "❌ Failed to get tenant access token."

    rows = _normalize_sheet_rows(_bitable_get_all_records(get_token_func))
    max_id = 0
    for r in rows:
        m = re.search(r"\d+", str(r["id"]))
        if m:
            max_id = max(max_id, int(m.group(0)))
    new_id = str(max_id + 1)

    if when_labels:
        wl = [str(x).strip() for x in when_labels if str(x).strip()]
    else:
        wl = [_WHEN_LABEL_DEFAULT]
    when_tokens, when_display = _when_tokens_from_labels(wl)

    fields = {
        REMINDER_FIELD_ID: new_id,
        REMINDER_FIELD_START: _sheet_date_to_timestamp_ms(start_d),
        REMINDER_FIELD_END: _sheet_date_to_timestamp_ms(end_d),
        REMINDER_FIELD_TIME: time_s,
        REMINDER_FIELD_REASON: reason_n,
        REMINDER_FIELD_WHEN_WRITE: wl,
    }
    resp = requests.post(
        _bitable_records_url(),
        headers=_bitable_headers(token),
        json={"fields": fields},
        timeout=30,
    )
    out = resp.json()
    if out.get("code") != 0:
        return f"❌ Add reminder to sheet failed: {out}"

    sync_sheet_daily_reminders(
        scheduler=scheduler,
        send_func=send_func,
        get_token_func=get_token_func,
        chat_id=(schedule_chat_id or chat_id),
        target_user_id=target_user_id,
    )
    card = _sheet_rows_card(
        [
            {
                "record_id": out.get("data", {}).get("record", {}).get("record_id", ""),
                "id": new_id,
                "start_date": start_d,
                "end_date": end_d,
                "time": time_s,
                "reason": reason_n,
                "when_tokens": when_tokens,
                "when_display": when_display,
            }
        ],
        title="✅ Reminder Added",
    )
    send_func(chat_id, json.dumps(card), msg_type="interactive")
    return ""


def delete_sheet_reminders(
    *,
    ids: list[str],
    get_token_func,
    scheduler,
    send_func,
    chat_id: str,
    target_user_id: str,
    schedule_chat_id: str | None = None,
) -> str:
    if not _reminder_sheet_enabled():
        return "❌ REMINDERSHEETTOKEN / REMINDERSHEETID is not set."
    targets = [str(x).strip() for x in ids if str(x).strip()]
    if not targets:
        return "❌ No ID provided."

    rows = list_sheet_reminders(get_token_func=get_token_func)
    by_id = {str(r["id"]).strip(): r for r in rows}
    missing = [x for x in targets if x not in by_id]
    to_del = [by_id[x] for x in targets if x in by_id]
    if not to_del:
        return f"❌ ID not found: {', '.join(missing)}"

    token = get_token_func()
    if not token:
        return "❌ Failed to get tenant access token."
    base_url = _bitable_records_url().rstrip("/")
    deleted_ids: list[str] = []
    for row in to_del:
        rid = row["record_id"]
        resp = requests.delete(
            f"{base_url}/{rid}",
            headers=_bitable_headers(token),
            timeout=30,
        )
        out = resp.json()
        if out.get("code") == 0:
            deleted_ids.append(str(row["id"]))

    sync_sheet_daily_reminders(
        scheduler=scheduler,
        send_func=send_func,
        get_token_func=get_token_func,
        chat_id=(schedule_chat_id or chat_id),
        target_user_id=target_user_id,
    )
    msg = f"✅ Deleted reminder ID(s): {', '.join(deleted_ids)}"
    if missing:
        msg += f"\n⚠️ Not found: {', '.join(missing)}"
    return msg


def send_sheet_reminder_list_card(*, send_func, chat_id: str, get_token_func) -> None:
    rows = list_sheet_reminders(get_token_func=get_token_func)
    card = _sheet_delete_picker_card(rows)
    resp = send_func(chat_id, json.dumps(card), msg_type="interactive")
    if isinstance(resp, dict) and int(resp.get("code", -1)) != 0:
        # Fallback message to surface card-delivery problems quickly.
        send_func(chat_id, f"❌ Reminder button card failed: {resp}")


def build_add_reminder_form_card() -> dict:
    time_options: list[dict] = []
    for hh in range(24):
        for mm in (0, 30):
            ap = "AM" if hh < 12 else "PM"
            hh12 = hh % 12
            if hh12 == 0:
                hh12 = 12
            v = f"{hh12}:{mm:02d}{ap}"
            time_options.append({"text": {"tag": "plain_text", "content": v}, "value": v})
    default_quick = "9:30AM"
    # Lark ``initial_index`` is 1-based (1 = first option); 0 = none selected.
    initial_index_1based = next(
        (i + 1 for i, o in enumerate(time_options) if o.get("value") == default_quick),
        1,
    )

    intro_lines = [
        "Fill all fields, then tap **Submit** once.",
        "Date can be picked from UI date picker.",
        "**Time:** pick **30-minute slots** below, or type an **exact time** to override (e.g. `9:55AM`, `14:25`).",
        "**When** (optional): weekdays / **Every day** / **Every month** / **One time** (only on Start date) — same labels as Bitable **when**.",
    ]

    form_elements: list[dict] = [
        {"tag": "div", "text": {"tag": "plain_text", "content": "Start Date"}},
        {
            "tag": "date_picker",
            "name": "start_date",
            "placeholder": {"tag": "plain_text", "content": "Pick start date"},
            "required": True,
        },
        {"tag": "div", "text": {"tag": "plain_text", "content": "End Date"}},
        {
            "tag": "date_picker",
            "name": "end_date",
            "placeholder": {"tag": "plain_text", "content": "Pick end date"},
            "required": True,
        },
        {"tag": "div", "text": {"tag": "plain_text", "content": "Time (quick pick, every 30 min)"}},
        {
            "tag": "select_static",
            "name": "time_preset",
            "placeholder": {"tag": "plain_text", "content": "e.g. 9:30AM, 10:00AM"},
            "options": time_options,
            "required": True,
            "initial_index": initial_index_1based,
        },
        {
            "tag": "div",
            "text": {
                "tag": "plain_text",
                "content": "Exact time (optional — overrides quick pick if you fill this)",
            },
        },
        {
            "tag": "input",
            "name": "time_override",
            "placeholder": {
                "tag": "plain_text",
                "content": "e.g. 9:55AM — leave empty to use dropdown",
            },
            "required": False,
        },
        {"tag": "div", "text": {"tag": "plain_text", "content": "When (multi-select)"}},
        {
            "tag": "multi_select_static",
            "name": "when",
            "placeholder": {
                "tag": "plain_text",
                "content": "Every day / weekdays / Every month / One time",
            },
            "required": False,
            "width": "fill",
            "selected_values": ["Every day"],
            "options": [
                {"text": {"tag": "plain_text", "content": lab}, "value": lab}
                for lab in (
                    "Every Monday",
                    "Every Tuesday",
                    "Every Wednesday",
                    "Every Thursday",
                    "Every Friday",
                    "Every Saturday",
                    "Every Sunday",
                    "Every day",
                    "Every month",
                    "One time",
                )
            ],
        },
    ]
    form_elements.extend(
        [
            {"tag": "div", "text": {"tag": "plain_text", "content": "Reason"}},
            {
                "tag": "input",
                "name": "reason",
                "placeholder": {"tag": "plain_text", "content": "Kindly send graph"},
                "required": True,
            },
            {
                "tag": "button",
                "name": "submit_rem_add",
                "text": {"tag": "plain_text", "content": "Submit"},
                "type": "primary",
                "form_action_type": "submit",
                "behaviors": [{"type": "callback", "value": {"k": "rem_add_submit"}}],
            },
        ]
    )

    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "➕ Add Reminder"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": "\n".join(intro_lines)},
                },
                {"tag": "form", "name": "rem_add_form", "elements": form_elements},
            ]
        },
    }


def send_add_reminder_form_card(*, send_func, chat_id: str) -> None:
    card = build_add_reminder_form_card()
    resp = send_func(chat_id, json.dumps(card), msg_type="interactive")
    if isinstance(resp, dict) and int(resp.get("code", -1)) != 0:
        send_func(chat_id, f"❌ Reminder add form card failed: {resp}")