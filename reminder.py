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
_SHEET_JOB_PREFIX = "sheet_daily_reminder::"


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


def _normalize_sheet_time(raw: str) -> str:
    s = (raw or "").strip().upper().replace(" ", "")
    m = re.match(r"^(\d{1,2}):(\d{2})(AM|PM)$", s)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        ap = m.group(3)
        if not (1 <= hh <= 12 and 0 <= mm <= 59):
            raise ValueError(f"Invalid time `{raw}`. Use HH:MMPM/AM, e.g. 6:30PM.")
        return f"{hh}:{mm:02d}{ap}"
    m = re.match(r"^(\d{1,2})(AM|PM)$", s)
    if m:
        hh = int(m.group(1))
        ap = m.group(2)
        if not (1 <= hh <= 12):
            raise ValueError(f"Invalid time `{raw}`. Use HH:MMPM/AM, e.g. 6:30PM.")
        return f"{hh}:00{ap}"
    raise ValueError(f"Invalid time `{raw}`. Use HH:MMPM/AM, e.g. 6:30PM.")


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
        rows.append(
            {
                "record_id": rid,
                "id": sid,
                "start_date": start_d,
                "end_date": end_d,
                "time": time_n,
                "reason": reason,
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
    if target_user_id:
        lines.append(f'<at user_id="{target_user_id}">User</at>')
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
    # Ensure real mention highlight in chat before the card.
    send_func(chat_id, f'<at user_id="{target_user_id}">User</at> ⏰ Daily Reminder')
    card = {
        "config": {"wide_screen_mode": True},
        "header": {"template": "orange", "title": {"tag": "plain_text", "content": "⏰ Daily Reminder"}},
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"📅 **Start Time:** `{row['start_date'].strftime('%Y/%m/%d')}`\n"
                        f"📅 **End Time:** `{row['end_date'].strftime('%Y/%m/%d')}`\n"
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

    fields = {
        REMINDER_FIELD_ID: new_id,
        REMINDER_FIELD_START: _sheet_date_to_timestamp_ms(start_d),
        REMINDER_FIELD_END: _sheet_date_to_timestamp_ms(end_d),
        REMINDER_FIELD_TIME: time_s,
        REMINDER_FIELD_REASON: reason_n,
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
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "Fill all fields, then tap **Submit** once.\n"
                            "Date can be picked from UI date picker.\n"
                            "Time: `HH:MMPM/AM` (e.g. `6:30PM`)"
                        ),
                    },
                },
                {
                    "tag": "div",
                    "text": {"tag": "plain_text", "content": "Start Date"},
                },
                {
                    "tag": "date_picker",
                    "name": "start_date",
                    "placeholder": {"tag": "plain_text", "content": "Pick start date"},
                },
                {
                    "tag": "div",
                    "text": {"tag": "plain_text", "content": "End Date"},
                },
                {
                    "tag": "date_picker",
                    "name": "end_date",
                    "placeholder": {"tag": "plain_text", "content": "Pick end date"},
                },
                {
                    "tag": "input",
                    "name": "time",
                    "label": {"tag": "plain_text", "content": "Time"},
                    "placeholder": {"tag": "plain_text", "content": "6:30PM"},
                },
                {
                    "tag": "input",
                    "name": "reason",
                    "label": {"tag": "plain_text", "content": "Reason"},
                    "placeholder": {"tag": "plain_text", "content": "Kindly send graph"},
                },
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "Submit"},
                    "type": "primary",
                    "behaviors": [{"type": "callback", "value": {"k": "rem_add_submit"}}],
                },
            ]
        },
    }


def send_add_reminder_form_card(*, send_func, chat_id: str) -> None:
    card = build_add_reminder_form_card()
    resp = send_func(chat_id, json.dumps(card), msg_type="interactive")
    if isinstance(resp, dict) and int(resp.get("code", -1)) != 0:
        send_func(chat_id, f"❌ Reminder add form card failed: {resp}")