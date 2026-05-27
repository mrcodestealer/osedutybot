#!/usr/bin/env python3
"""
Fetch approved leave records from Lark Base table.
Columns: Name, Leave Type, Start Date, End Date, Reason
Only shows records with Status = Approved.

Monthly leave calendar sync (OSE tracking Bitable):
  - Primary: company shared calendar **HRMS SNSoft Leave Calendar** (``AL - Name`` events for all staff).
  - Also: per-user primary calendars, profile status text, Attendance approvals.
  - Fallback: Leave2026 spreadsheet / OSE sheet markers, and leave Bitable rows.
  - Build who is on leave this calendar month
  - Write to TRACK_LEAVE_BASE_ID / TRACK_LEAVE_TABLE_ID (defaults = OSE leave table URL)
  - On a new calendar month: delete **all** rows in the tracking table, then refill that month only
  - During the month: **add** new leave rows when they appear; remove auto-synced rows that were cancelled
  - Tracking table: OSE leave Bitable (``TRACK_LEAVE_*`` / URL tblmHJHe12BCJRD8)
  - Annual Leave rows are always included when they overlap the month

Usage:
    ./leave.py                     # Show approved leaves as table
    ./leave.py --csv               # Output CSV
    ./leave.py --calendar          # Print this month's leave calendar (names per day)
    ./leave.py --calendar 2026-05  # Calendar for a specific month
    ./leave.py --sync-month        # Sync this month to tracking Bitable
    ./leave.py --sync-month 2026-05
    ./leave.py --resync-month 2026-05   # rewrite auto-synced rows (e.g. after Name → Text)
    ./leave.py --calendar-wfh 2026-05    # Who is WFH this month (console)
    ./leave.py --sync-wfh-month         # Sync WFH → tblWBI5BxrtFiJul
    ./leave.py --resync-wfh-month 2026-05
    ./leave.py --leave-today       # Print today's leave (console)
    ./leave.py --debug             # Show raw API responses
"""

from __future__ import annotations

import calendar
import json
import os
import re
import sys
import csv
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

import ose_Duty as od

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")

_OSE_BITABLE_BASE = (os.getenv("OSE_BASE_TOKEN") or "CpdEbEofwaYyyEsSjlElKNxzgec").strip()
_OSE_BITABLE_LEAVE_TABLE = (os.getenv("OSE_LEAVE_TABLE_ID") or "tblmHJHe12BCJRD8").strip()
_OSE_SPREADSHEET = (os.getenv("OSE_SPREADSHEET_TOKEN") or "").strip()


def _leave_source_ids() -> tuple[str, str]:
    """
  Resolve source Bitable for leave reads.

  ``.env`` often sets ``LEAVE_BASE_ID`` to the OSE *spreadsheet* token (not Bitable);
  in that case fall back to ``OSE_BASE_TOKEN`` / ``OSE_LEAVE_TABLE_ID``.
    """
    base = (os.getenv("LEAVE_BASE_ID") or "").strip()
    table = (os.getenv("LEAVE_TABLE_ID") or "").strip()
    if not base or (_OSE_SPREADSHEET and base == _OSE_SPREADSHEET):
        base = _OSE_BITABLE_BASE
    if not table or table == "tblfC3XoBP3as3Ci":
        table = _OSE_BITABLE_LEAVE_TABLE
    return base, table


BASE_ID, TABLE_ID = _leave_source_ids()

# Destination: https://casinoplus.sg.larksuite.com/base/CpdEbEofwaYyyEsSjlElKNxzgec?table=tblmHJHe12BCJRD8
TRACK_BASE_ID = os.getenv(
    "TRACK_LEAVE_BASE_ID",
    os.getenv("OSE_BASE_TOKEN", "CpdEbEofwaYyyEsSjlElKNxzgec"),
)
TRACK_TABLE_ID = os.getenv(
    "TRACK_LEAVE_TABLE_ID",
    os.getenv("OSE_LEAVE_TABLE_ID", "tblmHJHe12BCJRD8"),
)

# WFH tracking: https://casinoplus.sg.larksuite.com/base/.../table=tblWBI5BxrtFiJul
TRACK_WFH_TABLE_ID = os.getenv("TRACK_WFH_TABLE_ID", os.getenv("OSE_WFH_TABLE_ID", "tblWBI5BxrtFiJul"))

SYNC_STATE_FILE = Path(
    os.getenv("LEAVE_CALENDAR_SYNC_STATE", ".leave_calendar_sync_state.json")
)
WFH_SYNC_STATE_FILE = Path(
    os.getenv("WFH_CALENDAR_SYNC_STATE", ".wfh_calendar_sync_state.json")
)
SYNC_REASON_TAG = "[leave-calendar-sync]"
ANNUAL_LEAVE_TYPE = "Annual Leave"
WFH_TYPE = "Work From Home"

# Spreadsheet cell text that means the person is on leave (not a shift code).
_LEAVE_CELL_RE = re.compile(
    r"(?i)(on\s*leave|annual\s*leave|\bAL\b|sick\s*leave|\bSL\b|hospital|"
    r"compassionate|maternity|marriage|non\s*pay|replacement|\bMC\b|\bleave\b)"
)
_SHIFT_CODES = frozenset({"D", "N", "*"})

# Lark Calendar event titles that are leave / time-off (not regular meetings).
_LEAVE_EVENT_TITLE_RE = re.compile(
    r"(?i)(on\s*leave|out\s*of\s*office|\booo\b|annual\s*leave|sick\s*leave|"
    r"请假|休假|年假|病假|事假|调休|compassionate|maternity|marriage\s*leave|"
    r"hospital|time\s*off|non\s*pay|replacement\s*leave|\bAL\b|\bSL\b|\bMC\b)"
)
_MEETING_TITLE_RE = re.compile(
    r"(?i)(meeting|weekly|1-on-1|maintenance|okr|例会|会议|migration|checking of pending)"
)
# HRMS shared leave calendar titles: ``AL - Bk``, ``SL - Name``, …
_SHARED_LEAVE_TITLE_RE = re.compile(
    r"^(?P<code>AL|SL|MC|EL|ML|PL|HL|OB|WFH)\s*[-–—]\s*(?P<name>.+)$",
    re.IGNORECASE,
)
_LEAVE_CODE_TO_TYPE: dict[str, str] = {
    "AL": ANNUAL_LEAVE_TYPE,
    "SL": "Sick Leave",
    "MC": "Medical Leave",
    "EL": "Emergency Leave",
    "ML": "Maternity Leave",
    "PL": "Paternity Leave",
    "HL": "Hospitalisation Leave",
    "OB": "Out of Office",
    "WFH": "Work From Home",
}

DEBUG = False


def _open_api_base() -> str:
    return (os.getenv("LARK_OPEN_API_BASE") or "https://open.larksuite.com/open-apis").rstrip("/")


def debug_print(*args, **kwargs):
    if DEBUG:
        print("[DEBUG]", *args, file=sys.stderr, **kwargs)


def get_tenant_access_token() -> str:
    return od.get_tenant_access_token()


def get_all_records(token: str, app_token: str, table_id: str) -> list[dict[str, Any]]:
    return od._bitable_get_all_records(token, app_token, table_id)


def delete_record(token: str, app_token: str, table_id: str, record_id: str) -> None:
    rid = (record_id or "").strip()
    if not rid:
        return
    url = (
        "https://open.larksuite.com/open-apis/bitable/v1/apps/"
        f"{app_token}/tables/{table_id}/records/{rid}"
    )
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.delete(
        url, headers=headers, params={"user_id_type": "open_id"}, timeout=30
    ).json()
    if res.get("code") != 0:
        raise RuntimeError(f"Bitable delete failed: {res}")


def create_record(token: str, app_token: str, table_id: str, fields: dict[str, Any]) -> str:
    if app_token == od.OSE_BASE_TOKEN and table_id in (
        od.OSE_LEAVE_TABLE_ID,
        TRACK_WFH_TABLE_ID,
    ):
        res = od._bitable_create_record(token, table_id, fields)
    else:
        url = (
            "https://open.larksuite.com/open-apis/bitable/v1/apps/"
            f"{app_token}/tables/{table_id}/records"
        )
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        res = requests.post(
            url,
            headers=headers,
            params={"user_id_type": "open_id"},
            json={"fields": fields},
            timeout=30,
        ).json()
        if res.get("code") != 0:
            raise RuntimeError(f"Bitable create failed: {res}")
    return str((res.get("data") or {}).get("record", {}).get("record_id") or "")


def _month_range(year: int, month: int) -> tuple[date, date]:
    _, last = calendar.monthrange(year, month)
    return date(year, month, 1), date(year, month, last)


def _overlaps_month(start: date, end: date, year: int, month: int) -> bool:
    month_start, month_end = _month_range(year, month)
    return not (end < month_start or start > month_end)


def _normalize_sheet_name(raw: str) -> str:
    """``Jewel [Platform]`` → ``Jewel``; keep full string when no roster token match."""
    s = od._field_text(raw).strip()
    if not s:
        return ""
    bracket = re.match(r"^(.+?)\s*\[[^\]]+\]\s*$", s)
    core = bracket.group(1).strip() if bracket else s
    titled = od._title_name(core)
    for roster in od.OSE_LEAVE_FORM_NAMES:
        if od._names_same_person(roster, titled) or od._names_same_person(roster, core):
            return od._title_name(roster)
    return titled


def _cell_is_on_leave(code: str) -> bool:
    s = (code or "").strip()
    if not s or s.upper() in _SHIFT_CODES:
        return False
    return bool(_LEAVE_CELL_RE.search(s))


def _leave_type_from_cell(cell: str) -> str:
    s = (cell or "").strip().lower()
    if "annual" in s or re.search(r"\bal\b", s):
        return ANNUAL_LEAVE_TYPE
    if "sick" in s or re.search(r"\bsl\b", s):
        return "Sick Leave"
    if "hospital" in s:
        return "Hospitalisation Leave"
    if "compassionate" in s:
        return "Compassionate Leave"
    if "maternity" in s:
        return "Maternity Leave"
    if "marriage" in s:
        return "Marriage Leave"
    if "replacement" in s:
        return "Replacement Leave"
    if "non pay" in s:
        return "Non Pay Leave"
    if "on leave" in s or s == "leave":
        return "Leave"
    return (cell or "").strip() or "Leave"


def _date_column_for_day(values: list[list[Any]], year: int, month: int, day: int) -> Optional[int]:
    """Column index for ``year``/``month``/``day`` (same header rules as OSE duty sheet)."""
    for row_idx in range(1, min(15, len(values))):
        row = values[row_idx] if row_idx < len(values) else []
        for col in range(len(row)):
            try:
                day_num = int(str(row[col]).strip())
            except Exception:
                continue
            if day_num != day:
                continue
            header = ""
            for hcol in range(col, -1, -1):
                if hcol < len(values[0]) and values[0][hcol]:
                    header = od._field_text(values[0][hcol])
                    break
            mon_num, hdr_year = od.parse_month_year(header)
            if mon_num == month and hdr_year == year:
                return col
    return None


def _leave_rows_from_sheet_matrix(
    values: list[list[Any]],
    year: int,
    month: int,
    *,
    source: str,
) -> list[dict[str, Any]]:
    """One row per person-day with a leave marker in the spreadsheet grid."""
    _, last = calendar.monthrange(year, month)
    out: list[dict[str, Any]] = []
    for day in range(1, last + 1):
        col = _date_column_for_day(values, year, month, day)
        if col is None:
            continue
        on_date = date(year, month, day)
        for row_idx in range(2, len(values)):
            row = values[row_idx]
            if not row:
                continue
            name_raw = od._field_text(row[0] if len(row) > 0 else "")
            if not name_raw:
                continue
            low = name_raw.lower()
            if low in ("date", "day", "remark") or name_raw.upper().startswith("TEAM"):
                continue
            if col >= len(row):
                continue
            code = od._field_text(row[col])
            if not _cell_is_on_leave(code):
                continue
            name = _normalize_sheet_name(name_raw)
            if not name:
                continue
            out.append(
                {
                    "name": name,
                    "leave_type": _leave_type_from_cell(code),
                    "start": on_date,
                    "end": on_date,
                    "reason": f"from {source}",
                    "source": source,
                }
            )
    return out


def fetch_leave_rows_from_spreadsheets(
    token: str,
    year: int,
    month: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Read Leave2026 tab first; if unreadable, scan OSE2026 duty sheet for leave markers.
    """
    warnings: list[str] = []
    rows: list[dict[str, Any]] = []
    leave_ss = (os.getenv("OSE_LEAVE_SPREADSHEET_TOKEN") or _OSE_SPREADSHEET or "").strip()
    leave_sid = (os.getenv("OSE_LEAVE_SHEET_ID") or "65p5cn").strip()
    duty_ss = (os.getenv("OSE_SPREADSHEET_TOKEN") or leave_ss).strip()
    duty_sid = (os.getenv("OSE_SHEET_ID") or "3RIBRL").strip().replace(" ", "")

    if leave_ss and leave_sid:
        vals = od.get_range_values(token, leave_ss, leave_sid, "A1:BN80")
        if vals:
            rows.extend(_leave_rows_from_sheet_matrix(vals, year, month, source="Leave2026"))
        else:
            warnings.append(
                "Leave2026 sheet ("
                f"{leave_sid}) not readable by the bot (90215). "
                "In Lark: share the spreadsheet with the Duty Bot app, or move leave data to the Bitable."
            )

    if not rows and duty_ss and duty_sid:
        vals = od.get_range_values(token, duty_ss, duty_sid, "A1:BN80")
        if vals:
            rows.extend(_leave_rows_from_sheet_matrix(vals, year, month, source="OSE2026"))
        elif not warnings:
            warnings.append(f"OSE sheet ({duty_sid}) could not be read.")

    return rows, warnings


def _consolidate_leave_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge consecutive days for the same person + leave type into one date range."""
    if not rows:
        return []
    keyed: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in rows:
        key = (r["name"], r["leave_type"])
        keyed.setdefault(key, []).append(r)
    out: list[dict[str, Any]] = []
    for (name, lt), group in keyed.items():
        group.sort(key=lambda x: x["start"])
        cur_start = group[0]["start"]
        cur_end = group[0]["end"]
        reason = group[0].get("reason") or ""
        source = group[0].get("source") or ""
        for item in group[1:]:
            st, ed = item["start"], item["end"]
            if (st - cur_end).days <= 1:
                cur_end = max(cur_end, ed)
            else:
                out.append(
                    {
                        "name": name,
                        "leave_type": lt,
                        "start": cur_start,
                        "end": cur_end,
                        "reason": reason,
                        "source": source,
                    }
                )
                cur_start, cur_end, reason, source = st, ed, item.get("reason") or "", item.get("source") or ""
        out.append(
            {
                "name": name,
                "leave_type": lt,
                "start": cur_start,
                "end": cur_end,
                "reason": reason,
                "source": source,
            }
        )
    out.sort(key=lambda r: (r["start"], r["end"], r["name"].lower()))
    return out


def _calendar_person_label(name_or_key: str) -> str:
    """Display name for calendar roster (keep short uppercase aliases like BK)."""
    k = str(name_or_key or "").replace("_", " ").strip()
    if k.lower() == "bk":
        return "BK"
    if len(k) <= 4 and k.isalpha() and k.isupper():
        return k
    return od._title_name(k)


def _env_key_matches_roster(env_key: str, roster: str) -> bool:
    key = (env_key or "").strip().lower().replace("_", " ")
    if not key:
        return False
    return od._names_same_person(roster, key) or od._name_key(roster) == od._name_key(key)


def resolve_roster_open_ids(token: str) -> dict[str, str]:
    """
    Map OSE roster name → Lark ``open_id`` for calendar APIs.

    Sources: ``LEAVE_CALENDAR_OPEN_IDS`` JSON, ``.env`` ``ou_…`` keys, leave Bitable person cache.
    """
    out: dict[str, str] = {}
    raw = (os.getenv("LEAVE_CALENDAR_OPEN_IDS") or "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                for k, v in parsed.items():
                    nm = _calendar_person_label(str(k))
                    oid = str(v or "").strip()
                    if nm and oid.startswith("ou_"):
                        out[nm] = oid
        except json.JSONDecodeError:
            pass
    for key, val in os.environ.items():
        oid = (val or "").strip()
        if not oid.startswith("ou_"):
            continue
        for roster in od.OSE_LEAVE_FORM_NAMES:
            if _env_key_matches_roster(key, roster):
                out[od._title_name(roster)] = oid
    # .env aliases (yuxuan=ou_…) for calendar even if not on OSE roster
    _skip_env_keys = frozenset(
        k.lower()
        for k in (
            "BOT_OPEN_ID",
            "DUTY_BOT_OPEN_ID",
            "JENKINS_BOT_OPEN_ID",
            "APP_ID",
            "APP_SECRET",
        )
    )
    for key, val in os.environ.items():
        oid = (val or "").strip()
        if not oid.startswith("ou_"):
            continue
        if key.lower() in _skip_env_keys or "open_id" in key.lower():
            continue
        if not re.fullmatch(r"[a-z][a-z0-9_]{1,24}", key.lower()):
            continue
        label = _calendar_person_label(key)
        if label and oid not in out.values():
            out.setdefault(label, oid)
    try:
        idx = od._get_ose_person_open_id_index(token)
    except Exception:
        idx = {}
    for roster in od.OSE_LEAVE_FORM_NAMES:
        nm = od._title_name(roster)
        if nm in out:
            continue
        oid = idx.get(nm) or idx.get(od._name_key(nm))
        if oid:
            out[nm] = oid
    return out


def _contact_user_for_open_id(token: str, open_id: str) -> dict[str, Any]:
    url = f"{_open_api_base()}/contact/v3/users/{open_id}"
    res = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params={"user_id_type": "open_id"},
        timeout=20,
    ).json()
    if res.get("code") != 0:
        return {}
    return (res.get("data") or {}).get("user") or {}


def _employee_no_for_open_id(token: str, open_id: str) -> str:
    user = _contact_user_for_open_id(token, open_id)
    return str(user.get("employee_no") or user.get("user_id") or "").strip()


_PROFILE_LEAVE_RE = re.compile(r"(?i)(休假|请假|on\s*leave|annual\s*leave|年假|time\s*off)")


def _parse_leave_dates_from_profile_text(
    text: str,
    default_year: int,
) -> Optional[tuple[date, date]]:
    """
    Parse leave date range from Lark profile ``description`` (名片状态).

    Examples: ``5月27至6月7日休假``, ``2026-05-27至2026-06-07``.
    """
    if not (text or "").strip() or not _PROFILE_LEAVE_RE.search(text):
        return None
    t = text.strip()
    m = re.search(
        r"(?:(\d{4})年)?\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日?\s*(?:至|到|-|~)\s*"
        r"(?:(\d{4})年)?\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日",
        t,
    )
    if m:
        y1 = int(m.group(1) or default_year)
        m1, d1 = int(m.group(2)), int(m.group(3))
        y2 = int(m.group(4) or y1)
        m2, d2 = int(m.group(5)), int(m.group(6))
        if (m2, d2) < (m1, d1) and y2 == y1:
            y2 = y1 + 1
        return date(y1, m1, d1), date(y2, m2, d2)
    m = re.search(
        r"(\d{4})-(\d{2})-(\d{2})\s*(?:至|到|-|~)\s*(\d{4})-(\d{2})-(\d{2})",
        t,
    )
    if m:
        return (
            date(int(m.group(1)), int(m.group(2)), int(m.group(3))),
            date(int(m.group(4)), int(m.group(5)), int(m.group(6))),
        )
    return None


def _leave_type_from_profile_text(text: str) -> str:
    if re.search(r"(?i)annual|年假", text or ""):
        return ANNUAL_LEAVE_TYPE
    return "Leave"


def _month_unix_range(year: int, month: int) -> tuple[int, int]:
    _, last = calendar.monthrange(year, month)
    start = int(datetime(year, month, 1).timestamp())
    end = int(datetime(year, month, last, 23, 59, 59).timestamp())
    return start, end


def _parse_event_time_block(block: Any) -> Optional[datetime]:
    if not isinstance(block, dict):
        return None
    if block.get("date"):
        try:
            return datetime.strptime(str(block["date"])[:10], "%Y-%m-%d")
        except ValueError:
            return None
    ts = block.get("timestamp")
    if ts is not None and str(ts).strip():
        try:
            return datetime.fromtimestamp(int(ts))
        except (TypeError, ValueError):
            return None
    return None


def _event_date_range(event: dict[str, Any]) -> tuple[Optional[date], Optional[date]]:
    st = _parse_event_time_block(event.get("start_time"))
    ed = _parse_event_time_block(event.get("end_time"))
    if not st:
        return None, None
    if not ed:
        ed = st
    start_d, end_d = st.date(), ed.date()
    if isinstance(event.get("start_time"), dict) and event["start_time"].get("date"):
        if end_d <= start_d:
            end_d = start_d
        else:
            end_d = end_d - timedelta(days=1)
    if end_d < start_d:
        start_d, end_d = end_d, start_d
    return start_d, end_d


def _leave_type_from_event_title(title: str) -> str:
    return _leave_type_from_cell(title)


def _is_lark_leave_event(event: dict[str, Any]) -> bool:
    if (event.get("status") or "").strip().lower() == "cancelled":
        return False
    title = (event.get("summary") or "").strip()
    desc = (event.get("description") or "").strip()
    hay = f"{title} {desc}"
    if _MEETING_TITLE_RE.search(hay) and not _LEAVE_EVENT_TITLE_RE.search(hay):
        return False
    return bool(_LEAVE_EVENT_TITLE_RE.search(hay))


def _primary_calendars_batch(token: str, open_ids: list[str]) -> dict[str, dict[str, Any]]:
    """``open_id`` → {calendar_id, calendar_name, display_name}."""
    out: dict[str, dict[str, Any]] = {}
    if not open_ids:
        return out
    url = f"{_open_api_base()}/calendar/v4/calendars/primarys"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    chunk_size = 50
    for i in range(0, len(open_ids), chunk_size):
        chunk = open_ids[i : i + chunk_size]
        res = requests.post(
            url,
            headers=headers,
            params={"user_id_type": "open_id"},
            json={"user_ids": chunk},
            timeout=60,
        ).json()
        if res.get("code") != 0:
            debug_print("primarys error:", res)
            continue
        for item in (res.get("data") or {}).get("calendars") or []:
            uid = str(item.get("user_id") or "").strip()
            cal = item.get("calendar") or {}
            cid = str(cal.get("calendar_id") or "").strip()
            if not uid or not cid:
                continue
            out[uid] = {
                "calendar_id": cid,
                "calendar_name": str(cal.get("summary") or "").strip(),
                "permissions": str(cal.get("permissions") or "").strip(),
            }
    return out


def _calendar_events_for_month(
    token: str,
    calendar_id: str,
    year: int,
    month: int,
    *,
    page_size: int = 100,
) -> list[dict[str, Any]]:
    start_ts, end_ts = _month_unix_range(year, month)
    url = f"{_open_api_base()}/calendar/v4/calendars/{calendar_id}/events"
    items: list[dict[str, Any]] = []
    page_token = ""
    while True:
        params: dict[str, str] = {
            "start_time": str(start_ts),
            "end_time": str(end_ts),
            "page_size": str(page_size),
        }
        if page_token:
            params["page_token"] = page_token
        res = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=90,
        ).json()
        if res.get("code") != 0:
            debug_print("events list error:", res)
            break
        data = res.get("data") or {}
        items.extend(data.get("items") or [])
        if not data.get("has_more"):
            break
        page_token = str(data.get("page_token") or "").strip()
        if not page_token:
            break
    return items


def _resolve_company_leave_calendar_id(token: str) -> tuple[str, str]:
    """
    Return (calendar_id, calendar_title).

    Set ``OSE_HRMS_LEAVE_CALENDAR_ID`` in ``.env``, or we search by
    ``LEAVE_SHARED_CALENDAR_QUERY`` (default: HRMS SNSoft Leave Calendar).
    """
    cid = (
        os.getenv("OSE_HRMS_LEAVE_CALENDAR_ID")
        or os.getenv("LEAVE_SHARED_CALENDAR_ID")
        or ""
    ).strip()
    title = (os.getenv("LEAVE_SHARED_CALENDAR_QUERY") or "HRMS SNSoft Leave Calendar").strip()
    if cid:
        return cid, title
    url = f"{_open_api_base()}/calendar/v4/calendars/search"
    res = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"query": title},
        timeout=30,
    ).json()
    if res.get("code") != 0:
        return "", title
    for item in (res.get("data") or {}).get("items") or []:
        cal = item.get("calendar") or item
        summary = str(cal.get("summary") or "").strip()
        cal_id = str(cal.get("calendar_id") or "").strip()
        if cal_id and summary.lower() == title.lower():
            return cal_id, summary
    for item in (res.get("data") or {}).get("items") or []:
        cal = item.get("calendar") or item
        summary = str(cal.get("summary") or "").strip()
        cal_id = str(cal.get("calendar_id") or "").strip()
        if cal_id and title.lower() in summary.lower() and "leave" in summary.lower():
            return cal_id, summary
    return "", title


def _parse_shared_leave_event(summary: str) -> Optional[tuple[str, str]]:
    """``AL - Bk`` → (``BK``, ``Annual Leave``)."""
    m = _SHARED_LEAVE_TITLE_RE.match((summary or "").strip())
    if not m:
        return None
    code = m.group("code").upper()
    name = _calendar_person_label(m.group("name").strip())
    if not name:
        return None
    lt = _LEAVE_CODE_TO_TYPE.get(code, "Leave")
    return name, lt


def fetch_leave_from_company_leave_calendar(
    token: str,
    year: int,
    month: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    All staff leave from the company shared calendar (e.g. HRMS SNSoft Leave Calendar).

    Events look like ``AL - Michelle Chen`` (all-day). This is what the Lark UI shows under
    Following → HRMS SNSoft Leave Calendar — no per-user ``open_id`` list required.
    """
    warnings: list[str] = []
    cal_id, cal_title = _resolve_company_leave_calendar_id(token)
    if not cal_id:
        warnings.append(
            f"Company leave calendar not found (search: {cal_title!r}). "
            "Set OSE_HRMS_LEAVE_CALENDAR_ID in .env to the calendar_id."
        )
        return [], warnings

    events = _calendar_events_for_month(token, cal_id, year, month, page_size=500)
    if not events:
        warnings.append(f"Company leave calendar {cal_title!r} returned 0 events for {year}-{month:02d}.")
        return [], warnings

    rows: list[dict[str, Any]] = []
    for ev in events:
        if (ev.get("status") or "").strip().lower() == "cancelled":
            continue
        parsed = _parse_shared_leave_event(ev.get("summary") or "")
        if not parsed:
            continue
        name, leave_type = parsed
        start_d, end_d = _event_date_range(ev)
        if not start_d or not end_d:
            continue
        if not _overlaps_month(start_d, end_d, year, month):
            continue
        rows.append(
            {
                "name": name,
                "leave_type": leave_type,
                "start": start_d,
                "end": end_d,
                "reason": f"{cal_title}: {ev.get('summary') or ''}",
                "source": "company_leave_calendar",
            }
        )
    rows = _consolidate_leave_rows(rows)
    return rows, warnings


_SHARED_WFH_TITLE_RE = re.compile(
    r"^WFH(?:\s*\([^)]+\))?\s*[-–—]\s*(?P<name>.+)$",
    re.IGNORECASE,
)


def _resolve_wfh_calendar_id(token: str) -> tuple[str, str]:
    cid = (os.getenv("OSE_HRMS_WFH_CALENDAR_ID") or os.getenv("WFH_SHARED_CALENDAR_ID") or "").strip()
    title = (os.getenv("WFH_SHARED_CALENDAR_QUERY") or "HRMS SNSoft WFH Calendar").strip()
    if cid:
        return cid, title
    url = f"{_open_api_base()}/calendar/v4/calendars/search"
    res = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"query": title},
        timeout=30,
    ).json()
    if res.get("code") != 0:
        return "", title
    for item in (res.get("data") or {}).get("items") or []:
        cal = item.get("calendar") or item
        summary = str(cal.get("summary") or "").strip()
        cal_id = str(cal.get("calendar_id") or "").strip()
        if cal_id and summary.lower() == title.lower():
            return cal_id, summary
    for item in (res.get("data") or {}).get("items") or []:
        cal = item.get("calendar") or item
        summary = str(cal.get("summary") or "").strip()
        cal_id = str(cal.get("calendar_id") or "").strip()
        if cal_id and "wfh" in summary.lower():
            return cal_id, summary
    return "", title


def _parse_wfh_event(summary: str) -> Optional[str]:
    m = _SHARED_WFH_TITLE_RE.match((summary or "").strip())
    if not m:
        return None
    return _calendar_person_label(m.group("name").strip()) or None


def fetch_wfh_from_company_calendar(
    token: str,
    year: int,
    month: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    """All staff WFH from HRMS SNSoft WFH Calendar (``WFH - Name``, ``WFH (PM) - Name``)."""
    warnings: list[str] = []
    cal_id, cal_title = _resolve_wfh_calendar_id(token)
    if not cal_id:
        warnings.append(
            f"WFH calendar not found (search: {cal_title!r}). Set OSE_HRMS_WFH_CALENDAR_ID in .env."
        )
        return [], warnings

    events = _calendar_events_for_month(token, cal_id, year, month, page_size=500)
    rows: list[dict[str, Any]] = []
    for ev in events:
        if (ev.get("status") or "").strip().lower() == "cancelled":
            continue
        name = _parse_wfh_event(ev.get("summary") or "")
        if not name:
            continue
        start_d, end_d = _event_date_range(ev)
        if not start_d or not end_d:
            continue
        if not _overlaps_month(start_d, end_d, year, month):
            continue
        rows.append(
            {
                "name": name,
                "leave_type": WFH_TYPE,
                "start": start_d,
                "end": end_d,
                "reason": f"{cal_title}: {ev.get('summary') or ''}",
                "source": "company_wfh_calendar",
            }
        )
    return _consolidate_leave_rows(rows), warnings


def _build_wfh_tracking_fields(row: dict[str, Any]) -> dict[str, Any]:
    """WFH Bitable has Name (text), Leave Type, dates — no Reason column."""
    return {
        "Name": str(row.get("name") or "").strip(),
        "Leave Type": WFH_TYPE,
        "Start Date": _bitable_date_ms(row["start"]),
        "End Date": _bitable_date_ms(row["end"]),
    }


def _parse_wfh_bitable_row(rec: dict[str, Any]) -> Optional[dict[str, Any]]:
    f = rec.get("fields") or {}
    name = od._title_name(od._field_text(od._get_field_by_aliases(f, ["Name", "Employee Name"])))
    if not name:
        return None
    leave_type = od._field_text(od._get_field_by_aliases(f, ["Leave Type", "Type"])).strip()
    start_d = od._parse_date_value(od._get_field_by_aliases(f, ["Start Date", "From"]))
    end_d = od._parse_date_value(od._get_field_by_aliases(f, ["End Date", "To"]))
    if not start_d or not end_d:
        return None
    if start_d > end_d:
        start_d, end_d = end_d, start_d
    return {
        "name": name,
        "leave_type": leave_type or WFH_TYPE,
        "start": start_d,
        "end": end_d,
    }


def _existing_wfh_rows_by_key(records: list[dict[str, Any]]) -> dict[tuple[str, str, str, str], str]:
    out: dict[tuple[str, str, str, str], str] = {}
    for rec in records:
        parsed = _parse_wfh_bitable_row(rec)
        rid = str(rec.get("record_id") or "").strip()
        if not parsed or not rid:
            continue
        out[_leave_row_key(parsed)] = rid
    return out


def get_wfh_calendar(year: int, month: int) -> dict[str, Any]:
    if month < 1 or month > 12:
        raise ValueError("month must be 1–12")
    token = get_tenant_access_token()
    rows, warnings = fetch_wfh_from_company_calendar(token, year, month)
    days, _ = _rows_to_calendar_days(rows, year, month)
    return {
        "ok": True,
        "year": year,
        "month": month,
        "days": days,
        "total_records": len(rows),
        "wfh_rows": len(rows),
        "warnings": warnings,
    }


def sync_wfh_calendar_to_bitable(
    *,
    year: Optional[int] = None,
    month: Optional[int] = None,
    ref_date: Optional[date] = None,
    force_resync: bool = False,
) -> dict[str, Any]:
    """
    Sync WFH for ``year``/``month`` into tblWBI5BxrtFiJul from HRMS WFH Calendar.

    New month: clear the whole WFH table and refill. Same month: add new WFH rows only;
    remove rows no longer on the calendar. ``--resync-wfh-month`` rewrites the month.
    """
    ref = ref_date or date.today()
    year = int(year if year is not None else ref.year)
    month = int(month if month is not None else ref.month)
    if not TRACK_BASE_ID or not TRACK_WFH_TABLE_ID:
        raise ValueError("TRACK_WFH_TABLE_ID / OSE_BASE_TOKEN must be set")

    token = get_tenant_access_token()
    state = _load_sync_state(WFH_SYNC_STATE_FILE)
    month_changed = state.get("year") != year or state.get("month") != month

    source_rows, warnings = fetch_wfh_from_company_calendar(token, year, month)
    if not source_rows:
        return {
            "ok": False,
            "skipped": True,
            "year": year,
            "month": month,
            "message": "No WFH events found on HRMS WFH Calendar for this month.",
            "warnings": warnings,
        }

    existing = get_all_records(token, TRACK_BASE_ID, TRACK_WFH_TABLE_ID)
    source_by_key = {_leave_row_key(r): r for r in source_rows}
    synced_by_key = _existing_wfh_rows_by_key(existing)

    deleted = 0
    delete_errors: list[str] = []
    if month_changed or force_resync:
        for rec in existing:
            rid = str(rec.get("record_id") or "").strip()
            if not rid:
                continue
            try:
                delete_record(token, TRACK_BASE_ID, TRACK_WFH_TABLE_ID, rid)
                deleted += 1
            except Exception as exc:
                delete_errors.append(f"{rid}: {exc}")
        rows_to_add = list(source_rows)
    else:
        for key, rid in synced_by_key.items():
            if key in source_by_key:
                continue
            try:
                delete_record(token, TRACK_BASE_ID, TRACK_WFH_TABLE_ID, rid)
                deleted += 1
            except Exception as exc:
                delete_errors.append(f"{rid}: {exc}")
        rows_to_add = [row for key, row in source_by_key.items() if key not in synced_by_key]

    created_ids: list[str] = []
    create_errors: list[str] = []
    for row in rows_to_add:
        try:
            fields = _build_wfh_tracking_fields(row)
            rid = create_record(token, TRACK_BASE_ID, TRACK_WFH_TABLE_ID, fields)
            if rid:
                created_ids.append(rid)
        except Exception as exc:
            create_errors.append(f"{row.get('name')}: {exc}")

    _save_sync_state(
        {"year": year, "month": month, "synced_at": datetime.now().isoformat(timespec="seconds")},
        WFH_SYNC_STATE_FILE,
    )
    od.invalidate_ose_bitable_cache()

    return {
        "ok": not create_errors,
        "year": year,
        "month": month,
        "month_changed": month_changed,
        "deleted": deleted,
        "created": len(created_ids),
        "added": len(created_ids),
        "already_synced": len(source_rows) - len(rows_to_add),
        "source_rows": len(source_rows),
        "warnings": warnings,
        "delete_errors": delete_errors,
        "create_errors": create_errors,
        "tracking_base": TRACK_BASE_ID,
        "tracking_table": TRACK_WFH_TABLE_ID,
    }


def fetch_leave_from_lark_calendar(
    token: str,
    year: int,
    month: int,
    open_map: Optional[dict[str, str]] = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Read leave/time-off from each employee's Lark primary calendar (company calendar).
    Requires app scope ``calendar:calendar:readonly`` and calendar sharing (public or
    show_only_free_busy) for each user.
    """
    warnings: list[str] = []
    open_map = open_map or resolve_roster_open_ids(token)
    if not open_map:
        warnings.append(
            "No Lark open_id mapped for OSE roster. Set LEAVE_CALENDAR_OPEN_IDS in .env "
            '(JSON: {"Jun Chen":"ou_…", ...}) or add names as env keys with ou_ values.'
        )
        return [], warnings

    primaries = _primary_calendars_batch(token, list(open_map.values()))
    oid_to_name = {oid: nm for nm, oid in open_map.items()}
    visible = [
        oid_to_name.get(oid, meta.get("calendar_name") or oid)
        for oid, meta in primaries.items()
    ]
    missing_calendar = [nm for nm, oid in open_map.items() if oid not in primaries]
    if visible:
        perms = ", ".join(
            f"{oid_to_name.get(oid, oid)} ({meta.get('permissions') or '?'})"
            for oid, meta in list(primaries.items())[:6]
        )
        warnings.append(f"Lark calendars readable by bot: {perms}.")
    if missing_calendar:
        warnings.append(
            "Primary calendar NOT readable (sharing the Leave2026 spreadsheet or a chat "
            "does not apply): "
            + ", ".join(missing_calendar[:8])
            + (" …" if len(missing_calendar) > 8 else "")
            + ". Fix: Lark → Calendar → ⚙️ Settings → your primary calendar → "
            "Sharing / visibility → set to Public or Show only free/busy (not Private)."
        )

    rows: list[dict[str, Any]] = []
    name_by_oid = {oid: nm for nm, oid in open_map.items()}
    for oid, meta in primaries.items():
        roster_name = name_by_oid.get(oid) or od._title_name(meta.get("calendar_name", ""))
        if not roster_name:
            continue
        for ev in _calendar_events_for_month(token, meta["calendar_id"], year, month):
            if not _is_lark_leave_event(ev):
                continue
            start_d, end_d = _event_date_range(ev)
            if not start_d or not end_d:
                continue
            if not _overlaps_month(start_d, end_d, year, month):
                continue
            title = (ev.get("summary") or "").strip() or "Leave"
            rows.append(
                {
                    "name": roster_name,
                    "leave_type": _leave_type_from_event_title(title),
                    "start": start_d,
                    "end": end_d,
                    "reason": f"Lark calendar: {title}",
                    "source": "lark_calendar",
                }
            )
    rows = _consolidate_leave_rows(rows)
    return rows, warnings


def fetch_leave_from_lark_contact_profile(
    token: str,
    year: int,
    month: int,
    open_map: Optional[dict[str, str]] = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Leave from Lark profile status (``contact/v3/users`` → ``description``).

    Many employees set「Annual Leave」on the name card (e.g. ``5月27至6月7日休假``)
    without creating a calendar event — the events API will not see that leave.
    """
    warnings: list[str] = []
    open_map = open_map or resolve_roster_open_ids(token)
    rows: list[dict[str, Any]] = []
    for roster_name, oid in open_map.items():
        user = _contact_user_for_open_id(token, oid)
        desc = str(user.get("description") or "").strip()
        if not desc:
            continue
        rng = _parse_leave_dates_from_profile_text(desc, year)
        if not rng:
            continue
        start_d, end_d = rng
        if not _overlaps_month(start_d, end_d, year, month):
            continue
        rows.append(
            {
                "name": roster_name,
                "leave_type": _leave_type_from_profile_text(desc),
                "start": start_d,
                "end": end_d,
                "reason": f"Lark profile: {desc[:120]}",
                "source": "lark_profile",
            }
        )
    return _consolidate_leave_rows(rows), warnings


def _attendance_date_chunks(year: int, month: int) -> list[tuple[int, int]]:
    """Attendance API allows at most 30 days per request; do not query future dates."""
    _, last = calendar.monthrange(year, month)
    today = date.today()
    if (year, month) > (today.year, today.month):
        return []
    if (year, month) == (today.year, today.month):
        last = today.day
    chunks: list[tuple[int, int]] = []
    day = 1
    while day <= last:
        chunk_end = min(day + 29, last)
        chunks.append(
            (int(f"{year}{month:02d}{day:02d}"), int(f"{year}{month:02d}{chunk_end:02d}"))
        )
        day = chunk_end + 1
    return chunks


def fetch_leave_from_lark_attendance(
    token: str,
    year: int,
    month: int,
    open_map: Optional[dict[str, str]] = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Approved leave from Lark Attendance (feeds calendar leave status)."""
    warnings: list[str] = []
    open_map = open_map or resolve_roster_open_ids(token)
    employee_nos: list[str] = []
    eno_by_no: dict[str, str] = {}
    for name, oid in open_map.items():
        eno = _employee_no_for_open_id(token, oid)
        if eno:
            employee_nos.append(eno)
            eno_by_no[eno] = name
    if not employee_nos:
        return [], warnings

    url = f"{_open_api_base()}/attendance/v1/user_approvals/query"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    rows: list[dict[str, Any]] = []
    for date_from, date_to in _attendance_date_chunks(year, month):
        body = {
            "user_ids": employee_nos,
            "check_date_from": date_from,
            "check_date_to": date_to,
            "status": 2,
        }
        res = requests.post(
            url,
            headers=headers,
            params={"employee_type": "employee_no"},
            json=body,
            timeout=60,
        ).json()
        if res.get("code") != 0:
            warnings.append(f"Lark Attendance query failed: {res.get('msg') or res}")
            break
        for ua in (res.get("data") or {}).get("user_approvals") or []:
            eno = str(ua.get("user_id") or "").strip()
            roster = eno_by_no.get(eno) or eno
            for leave in ua.get("leaves") or []:
                st = od._parse_date_value(leave.get("start_time"))
                ed = od._parse_date_value(leave.get("end_time"))
                if not st or not ed:
                    continue
                if not _overlaps_month(st, ed, year, month):
                    continue
                lt = "Leave"
                i18n = leave.get("i18n_names") or {}
                if isinstance(i18n, dict):
                    lt = (
                        str(i18n.get("en_us") or i18n.get("zh_cn") or i18n.get("ch") or "").strip()
                        or lt
                    )
                rows.append(
                    {
                        "name": od._title_name(roster),
                        "leave_type": lt,
                        "start": st,
                        "end": ed,
                        "reason": "Lark Attendance approval",
                        "source": "lark_attendance",
                    }
                )
    return _consolidate_leave_rows(rows), warnings


def _merge_leave_rows_unique(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for row in rows:
        if not any(
            r["name"] == row["name"]
            and r["leave_type"] == row["leave_type"]
            and r["start"] == row["start"]
            and r["end"] == row["end"]
            for r in merged
        ):
            merged.append(row)
    merged.sort(key=lambda r: (r["start"], r["end"], r["name"].lower()))
    return merged


def collect_leave_source_rows(
    token: str,
    year: int,
    month: int,
    *,
    include_bitable: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Company HRMS leave calendar + Leave2026 sheet (no per-user Lark calendars)."""
    warnings: list[str] = []
    company_rows, w_co = fetch_leave_from_company_leave_calendar(token, year, month)
    warnings.extend(w_co)
    profile_rows: list[dict[str, Any]] = []
    lark_rows: list[dict[str, Any]] = []
    att_rows: list[dict[str, Any]] = []
    open_map: dict[str, str] = {}
    if os.getenv("LEAVE_WFH_USE_PER_USER_SOURCES", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        open_map = resolve_roster_open_ids(token)
        profile_rows, w0 = fetch_leave_from_lark_contact_profile(
            token, year, month, open_map=open_map
        )
        warnings.extend(w0)
        lark_rows, w1 = fetch_leave_from_lark_calendar(token, year, month, open_map=open_map)
        warnings.extend(w1)
        att_rows, w2 = fetch_leave_from_lark_attendance(token, year, month, open_map=open_map)
        warnings.extend(w2)
    bitable_rows = (
        fetch_approved_leaves_for_month(token, year, month, require_approved=False)
        if include_bitable
        else []
    )
    sheet_days, w3 = fetch_leave_rows_from_spreadsheets(token, year, month)
    warnings.extend(w3)
    sheet_rows = _consolidate_leave_rows(sheet_days)
    merged = _merge_leave_rows_unique(
        company_rows + profile_rows + lark_rows + att_rows + bitable_rows + sheet_rows
    )
    if not merged and not company_rows:
        warnings.append(
            f"No leave found for {year}-{month:02d} from company leave calendar or sheet."
        )
    meta = {
        "company_leave_calendar_rows": len(company_rows),
        "lark_profile_rows": len(profile_rows),
        "lark_calendar_rows": len(lark_rows),
        "lark_attendance_rows": len(att_rows),
        "bitable_rows": len(bitable_rows),
        "sheet_day_marks": len(sheet_days),
        "sheet_rows": len(sheet_rows),
        "open_ids_mapped": len(open_map),
        "warnings": warnings,
    }
    return merged, meta


def _rows_to_calendar_days(
    rows: list[dict[str, Any]],
    year: int,
    month: int,
) -> tuple[dict[str, list[dict[str, str]]], list[dict[str, str]]]:
    month_start, month_end = _month_range(year, month)
    _, last = calendar.monthrange(year, month)
    days: dict[str, list[dict[str, str]]] = {str(d): [] for d in range(1, last + 1)}
    annual_leave: list[dict[str, str]] = []
    seen_annual: set[tuple[str, str, str]] = set()
    for row in rows:
        name = row["name"]
        lt = row["leave_type"]
        st: date = row["start"]
        ed: date = row["end"]
        if lt.strip().lower() == ANNUAL_LEAVE_TYPE.lower():
            key = (name, st.isoformat(), ed.isoformat())
            if key not in seen_annual:
                seen_annual.add(key)
                annual_leave.append({"name": name, "start": st.isoformat(), "end": ed.isoformat()})
        cur = max(st, month_start)
        end = min(ed, month_end)
        entry = {
            "name": name,
            "leave_type": lt,
            "start": st.isoformat(),
            "end": ed.isoformat(),
        }
        while cur <= end:
            key = str(cur.day)
            if not any(x["name"] == name and x["leave_type"] == lt for x in days[key]):
                days[key].append(dict(entry))
            cur += timedelta(days=1)
    for key in days:
        days[key].sort(key=lambda x: (x["name"].lower(), x["leave_type"]))
    annual_leave.sort(key=lambda x: x["name"].lower())
    return days, annual_leave


def _parse_leave_row(
    rec: dict[str, Any],
    *,
    require_approved: bool = False,
) -> Optional[dict[str, Any]]:
    f = rec.get("fields") or {}
    status_v = od._get_field_by_aliases(f, ["Status", "Approval Status"])
    status_text = od._field_text(status_v).strip().lower()
    if require_approved:
        if status_text and status_text != "approved":
            return None
    elif status_text in ("rejected",):
        return None
    name_raw = od._get_field_by_aliases(f, ["Name", "Employee Name", "Person"])
    name = od._title_name(od._field_text(name_raw))
    if not name and isinstance(name_raw, list) and name_raw:
        first = name_raw[0]
        if isinstance(first, dict):
            name = od._title_name(
                str(first.get("name") or first.get("en_name") or "").strip()
            )
    reason = od._field_text(od._get_field_by_aliases(f, ["Reason"])).strip()
    if not name and _record_reason_tagged(rec):
        name = _name_from_sync_reason(reason)
    if not name:
        return None
    leave_type = (
        od._field_text(od._get_field_by_aliases(f, ["Leave Type", "Type"])).strip() or "Leave"
    )
    start_d = od._parse_date_value(
        od._get_field_by_aliases(f, ["Start Date", "Leave Start Date", "From"])
    )
    end_d = od._parse_date_value(
        od._get_field_by_aliases(f, ["End Date", "Leave End Date", "To"])
    )
    if not start_d or not end_d:
        return None
    if start_d > end_d:
        start_d, end_d = end_d, start_d
    return {
        "name": name,
        "leave_type": leave_type,
        "start": start_d,
        "end": end_d,
        "reason": reason,
        "status": status_text or "approved",
    }


def fetch_approved_leaves_for_month(
    token: str,
    year: int,
    month: int,
    *,
    app_token: Optional[str] = None,
    table_id: Optional[str] = None,
    require_approved: bool = True,
) -> list[dict[str, Any]]:
    """Leave rows from the source Bitable that overlap ``year``/``month``."""
    app_token = (app_token or BASE_ID or "").strip()
    table_id = (table_id or TABLE_ID or "").strip()
    if not app_token or not table_id:
        raise ValueError(
            "Leave Bitable not configured. Set OSE_BASE_TOKEN + OSE_LEAVE_TABLE_ID "
            "(or LEAVE_BASE_ID + LEAVE_TABLE_ID to a real Bitable app token)."
        )
    records = get_all_records(token, app_token, table_id)
    out: list[dict[str, Any]] = []
    for rec in records:
        if _record_reason_tagged(rec):
            continue
        row = _parse_leave_row(rec, require_approved=require_approved)
        if not row:
            continue
        if _overlaps_month(row["start"], row["end"], year, month):
            out.append(row)
    out.sort(key=lambda r: (r["start"], r["end"], r["name"].lower()))
    return out


def _leave_row_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("name") or "").strip().lower(),
        str(row.get("leave_type") or "").strip().lower(),
        row["start"].isoformat() if isinstance(row.get("start"), date) else str(row.get("start")),
        row["end"].isoformat() if isinstance(row.get("end"), date) else str(row.get("end")),
    )


def _existing_synced_rows_by_key(
    records: list[dict[str, Any]],
) -> dict[tuple[str, str, str, str], str]:
    """Map leave row key → record_id for auto-synced Bitable rows."""
    out: dict[tuple[str, str, str, str], str] = {}
    for rec in records:
        if not _record_reason_tagged(rec):
            continue
        parsed = _parse_leave_row(rec, require_approved=False)
        rid = str(rec.get("record_id") or "").strip()
        if not parsed or not rid:
            continue
        out[_leave_row_key(parsed)] = rid
    return out


def get_leave_calendar(year: int, month: int) -> dict[str, Any]:
    """
    Who is on leave each day this month (Lark company calendar + other sources).
    Annual leave appears in ``days`` and under ``annual_leave``.
    """
    if month < 1 or month > 12:
        raise ValueError("month must be 1–12")
    token = get_tenant_access_token()
    rows, meta = collect_leave_source_rows(token, year, month)
    days, annual_leave = _rows_to_calendar_days(rows, year, month)
    return {
        "ok": True,
        "year": year,
        "month": month,
        "days": days,
        "annual_leave": annual_leave,
        "total_records": len(rows),
        "company_leave_calendar_rows": meta["company_leave_calendar_rows"],
        "lark_profile_rows": meta["lark_profile_rows"],
        "lark_calendar_rows": meta["lark_calendar_rows"],
        "lark_attendance_rows": meta["lark_attendance_rows"],
        "open_ids_mapped": meta["open_ids_mapped"],
        "bitable_rows": meta["bitable_rows"],
        "sheet_day_marks": meta["sheet_day_marks"],
        "warnings": meta["warnings"],
    }


def _load_sync_state(path: Path = SYNC_STATE_FILE) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_sync_state(state: dict[str, Any], path: Path = SYNC_STATE_FILE) -> None:
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _record_reason_tagged(record: dict[str, Any]) -> bool:
    f = record.get("fields") or {}
    reason = od._field_text(od._get_field_by_aliases(f, ["Reason"]))
    return SYNC_REASON_TAG in (reason or "")


def _bitable_date_ms(d: date) -> int:
    return od._bitable_date_ms(d)


def _name_from_sync_reason(reason: str) -> str:
    if SYNC_REASON_TAG not in (reason or ""):
        return ""
    rest = (reason or "").split(SYNC_REASON_TAG, 1)[1].strip()
    if " | " in rest:
        return rest.split(" | ", 1)[0].strip()
    return rest.strip()


def _build_tracking_fields(
    token: str,
    row: dict[str, Any],
    *,
    open_map: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    del token, open_map  # Name column is plain text; no open_id required.
    name = str(row.get("name") or "").strip()
    detail = (row.get("reason") or "").strip()
    tagged_reason = f"{SYNC_REASON_TAG} {name} | {detail}".strip()
    st: date = row["start"]
    ed: date = row["end"]
    return {
        "Name": name,
        "Leave Type": row["leave_type"],
        "Start Date": _bitable_date_ms(st),
        "End Date": _bitable_date_ms(ed),
        "Reason": tagged_reason,
    }


def sync_leave_calendar_to_bitable(
    *,
    year: Optional[int] = None,
    month: Optional[int] = None,
    ref_date: Optional[date] = None,
    force_resync: bool = False,
) -> dict[str, Any]:
    """
    Sync leave for ``year``/``month`` into the OSE tracking Bitable (tblmHJHe12BCJRD8).

    - **New month** (vs last sync): delete every row in the table, insert this month only.
    - **Same month**: keep existing rows; **add** new leave not already present; remove auto-synced
      rows that disappeared from Lark (cancelled leave). Manual rows (no sync tag) are kept.
    - **force_resync** (``--resync-month``): delete all auto-synced rows for this month and rewrite
      (use after changing the Name column to Text).
    """
    ref = ref_date or date.today()
    year = int(year if year is not None else ref.year)
    month = int(month if month is not None else ref.month)
    if month < 1 or month > 12:
        raise ValueError("month must be 1–12")
    if not TRACK_BASE_ID or not TRACK_TABLE_ID:
        raise ValueError("TRACK_LEAVE_BASE_ID and TRACK_LEAVE_TABLE_ID must be set")

    token = get_tenant_access_token()
    state = _load_sync_state()
    prev_year = state.get("year")
    prev_month = state.get("month")
    month_changed = prev_year != year or prev_month != month

    open_map = resolve_roster_open_ids(token)
    source_rows, meta = collect_leave_source_rows(
        token, year, month, include_bitable=False
    )
    if not source_rows:
        return {
            "ok": False,
            "skipped": True,
            "year": year,
            "month": month,
            "month_changed": month_changed,
            "deleted": 0,
            "created": 0,
            "annual_leave_rows": 0,
            "source_rows": 0,
            "warnings": meta.get("warnings") or [],
            "message": (
                "No leave data found (Bitable empty and Leave2026 sheet not readable). "
                "Tracking table was not modified."
            ),
        }

    existing = get_all_records(token, TRACK_BASE_ID, TRACK_TABLE_ID)
    source_by_key = {_leave_row_key(r): r for r in source_rows}
    synced_by_key = _existing_synced_rows_by_key(existing)

    deleted = 0
    delete_errors: list[str] = []
    if month_changed:
        for rec in existing:
            rid = str(rec.get("record_id") or "").strip()
            if not rid:
                continue
            try:
                delete_record(token, TRACK_BASE_ID, TRACK_TABLE_ID, rid)
                deleted += 1
            except Exception as exc:
                delete_errors.append(f"{rid}: {exc}")
        rows_to_add = list(source_rows)
    elif force_resync:
        for rec in existing:
            if not _record_reason_tagged(rec):
                continue
            rid = str(rec.get("record_id") or "").strip()
            if not rid:
                continue
            try:
                delete_record(token, TRACK_BASE_ID, TRACK_TABLE_ID, rid)
                deleted += 1
            except Exception as exc:
                delete_errors.append(f"{rid}: {exc}")
        rows_to_add = list(source_rows)
    else:
        for key, rid in synced_by_key.items():
            if key in source_by_key:
                continue
            try:
                delete_record(token, TRACK_BASE_ID, TRACK_TABLE_ID, rid)
                deleted += 1
            except Exception as exc:
                delete_errors.append(f"{rid}: {exc}")
        rows_to_add = [
            row for key, row in source_by_key.items() if key not in synced_by_key
        ]

    created_ids: list[str] = []
    create_errors: list[str] = []
    annual_count = sum(
        1
        for row in source_rows
        if row["leave_type"].strip().lower() == ANNUAL_LEAVE_TYPE.lower()
    )
    for row in rows_to_add:
        try:
            fields = _build_tracking_fields(token, row, open_map=open_map)
            rid = create_record(token, TRACK_BASE_ID, TRACK_TABLE_ID, fields)
            if rid:
                created_ids.append(rid)
        except Exception as exc:
            create_errors.append(f"{row.get('name')}: {exc}")

    _save_sync_state(
        {
            "year": year,
            "month": month,
            "synced_at": datetime.now().isoformat(timespec="seconds"),
            "synced_record_ids": created_ids,
            "month_changed": month_changed,
        }
    )

    od.invalidate_ose_bitable_cache()

    return {
        "ok": not create_errors,
        "year": year,
        "month": month,
        "month_changed": month_changed,
        "deleted": deleted,
        "created": len(created_ids),
        "added": len(created_ids),
        "already_synced": len(source_rows) - len(rows_to_add),
        "annual_leave_rows": annual_count,
        "source_rows": len(source_rows),
        "company_leave_calendar_rows": meta.get("company_leave_calendar_rows", 0),
        "lark_profile_rows": meta.get("lark_profile_rows", 0),
        "lark_calendar_rows": meta.get("lark_calendar_rows", 0),
        "lark_attendance_rows": meta.get("lark_attendance_rows", 0),
        "open_ids_mapped": len(open_map),
        "bitable_rows": meta.get("bitable_rows", 0),
        "sheet_day_marks": meta.get("sheet_day_marks", 0),
        "warnings": meta.get("warnings") or [],
        "delete_errors": delete_errors,
        "create_errors": create_errors,
        "tracking_base": TRACK_BASE_ID,
        "tracking_table": TRACK_TABLE_ID,
    }


def _leave_type_emoji(leave_type: str) -> str:
    lt = (leave_type or "").strip().lower()
    if "annual" in lt or lt == "al":
        return "🏖️"
    if "sick" in lt or lt == "sl":
        return "🤒"
    if "medical" in lt or "hospital" in lt or lt == "mc":
        return "🏥"
    if "maternity" in lt:
        return "👶"
    if "emergency" in lt:
        return "🚨"
    if "work from home" in lt or lt == "wfh":
        return "🏠"
    return "📋"


def _format_leave_day(d: date) -> str:
    return d.strftime("%d %b %Y")


def rows_on_leave_date(rows: list[dict[str, Any]], on_date: date) -> list[dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    for r in rows:
        if not (r["start"] <= on_date <= r["end"]):
            continue
        if (r.get("leave_type") or "").strip().lower() == WFH_TYPE.lower():
            continue
        key = r["name"].strip().lower()
        prev = by_name.get(key)
        if not prev:
            by_name[key] = r
            continue
        if "annual" in (r.get("leave_type") or "").lower():
            by_name[key] = r
    out = list(by_name.values())
    out.sort(key=lambda r: (r["leave_type"].lower(), r["name"].lower()))
    return out


def format_leave_today_text(
    on_date: date,
    today_rows: list[dict[str, Any]],
    *,
    source: str = "HRMS Leave Calendar",
) -> str:
    header = f"🏖️ On leave — {on_date.strftime('%A, %d %b %Y')}"
    if not today_rows:
        return f"{header}\n\n✅ No one on leave today ({source})."
    lines = [header, ""]
    cur_type = ""
    for row in today_rows:
        lt = row["leave_type"]
        if lt != cur_type:
            cur_type = lt
            lines.append(f"\n{_leave_type_emoji(lt)} **{lt}**")
        st, ed = row["start"], row["end"]
        if st == ed:
            span = _format_leave_day(st)
        else:
            span = f"{_format_leave_day(st)} → {_format_leave_day(ed)}"
        lines.append(f"• **{row['name']}** — {span}")
    lines.append(f"\n👥 **{len(today_rows)}** person(s) on leave today")
    return "\n".join(lines).strip()


def build_leave_today_lark_card(
    on_date: date,
    today_rows: list[dict[str, Any]],
    warnings: Optional[list[str]] = None,
    *,
    source: str = "HRMS Leave Calendar",
) -> dict[str, Any]:
    """Interactive card for ``/leave`` / ``/wholeave`` — who is on leave today."""
    date_label = on_date.strftime("%A · %d %b %Y")
    if not today_rows:
        body_md = (
            f"📅 **{date_label}**\n\n"
            "✅ **All hands on deck!**\n\n"
            f"No one is on leave today according to the **{source}**."
        )
        header_title = "✅ No Leave Today"
        template = "green"
    else:
        parts = [f"📅 **{date_label}**\n"]
        by_type: dict[str, list[dict[str, Any]]] = {}
        for row in today_rows:
            by_type.setdefault(row["leave_type"], []).append(row)
        for lt in sorted(by_type.keys(), key=str.lower):
            emoji = _leave_type_emoji(lt)
            parts.append(f"\n{emoji} **{lt}** ({len(by_type[lt])})")
            for row in by_type[lt]:
                st, ed = row["start"], row["end"]
                if st == ed:
                    span = _format_leave_day(st)
                elif st <= on_date <= ed and st != ed:
                    if st == on_date:
                        span = f"starts today · until {_format_leave_day(ed)}"
                    elif ed == on_date:
                        span = f"since {_format_leave_day(st)} · **last day**"
                    else:
                        span = f"{_format_leave_day(st)} → {_format_leave_day(ed)}"
                else:
                    span = f"{_format_leave_day(st)} → {_format_leave_day(ed)}"
                parts.append(f"• **{row['name']}** — {span}")
        parts.append(f"\n---\n👥 **{len(today_rows)}** colleague(s) on leave")
        body_md = "\n".join(parts)
        header_title = f"🏖️ On Leave Today ({len(today_rows)})"
        template = "orange"

    elements: list[dict[str, Any]] = [
        {"tag": "div", "text": {"tag": "lark_md", "content": body_md}},
    ]
    for w in (warnings or [])[:2]:
        elements.append(
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"⚠️ {w}"},
            }
        )

    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": header_title},
        },
        "body": {"elements": elements},
    }


_WHOLEAVE_BITABLE_SOURCE = "OSE leave Bitable (approved records)"


def get_wholeave_today_payload(ref_date: Optional[date] = None) -> dict[str, Any]:
    """
    Text + Lark card for who is on leave today — from the OSE leave Bitable
    (``OSE_BASE_TOKEN`` / ``OSE_LEAVE_TABLE_ID``, same as /ose duty leave list).
    """
    on_date = ref_date or date.today()
    try:
        token = get_tenant_access_token()
        rows = fetch_approved_leaves_for_month(
            token,
            on_date.year,
            on_date.month,
            app_token=_OSE_BITABLE_BASE,
            table_id=_OSE_BITABLE_LEAVE_TABLE,
            require_approved=True,
        )
        today_rows = rows_on_leave_date(rows, on_date)
        warnings: list[str] = []
        if not rows:
            warnings.append(
                f"No approved leave rows in OSE Bitable for {on_date.year}-{on_date.month:02d}."
            )
        return {
            "text": format_leave_today_text(
                on_date, today_rows, source=_WHOLEAVE_BITABLE_SOURCE
            ),
            "lark_card": build_leave_today_lark_card(
                on_date,
                today_rows,
                warnings,
                source=_WHOLEAVE_BITABLE_SOURCE,
            ),
            "count": len(today_rows),
            "date": on_date.isoformat(),
            "source": "bitable",
        }
    except Exception as exc:
        return {
            "text": f"❌ Could not load OSE leave Bitable: {exc}",
            "lark_card": None,
            "count": 0,
            "date": on_date.isoformat(),
            "source": "bitable",
        }


def get_leave_today_payload(ref_date: Optional[date] = None) -> dict[str, Any]:
    """Text + Lark card for who is on leave on ``ref_date`` (default: today)."""
    on_date = ref_date or date.today()
    try:
        token = get_tenant_access_token()
        rows, meta = collect_leave_source_rows(
            token, on_date.year, on_date.month, include_bitable=False
        )
        warnings = list(meta.get("warnings") or [])
        today_rows = rows_on_leave_date(rows, on_date)
        return {
            "text": format_leave_today_text(on_date, today_rows),
            "lark_card": build_leave_today_lark_card(on_date, today_rows, warnings),
            "count": len(today_rows),
            "date": on_date.isoformat(),
            "source": "hrms",
        }
    except Exception as exc:
        return {
            "text": f"❌ Could not load leave data: {exc}",
            "lark_card": None,
            "count": 0,
            "date": on_date.isoformat(),
            "source": "hrms",
        }


def _print_leave_calendar(cal: dict[str, Any]) -> None:
    y, m = cal["year"], cal["month"]
    print(f"Leave calendar — {y}-{m:02d}")
    print(
        f"Sources: {cal.get('total_records', 0)} row(s) "
        f"(company cal {cal.get('company_leave_calendar_rows', 0)}, "
        f"profile {cal.get('lark_profile_rows', 0)}, "
        f"personal cal {cal.get('lark_calendar_rows', 0)}, "
        f"attendance {cal.get('lark_attendance_rows', 0)}, "
        f"open_ids {cal.get('open_ids_mapped', 0)}, "
        f"Bitable {cal.get('bitable_rows', 0)}, sheet {cal.get('sheet_day_marks', 0)})"
    )
    for w in cal.get("warnings") or []:
        print(f"⚠️  {w}")
    annual = cal.get("annual_leave") or []
    if annual:
        print(f"\nAnnual Leave ({len(annual)}):")
        for row in annual:
            print(f"  • {row['name']}: {row['start']} → {row['end']}")
    print()
    any_day = False
    for day in sorted(cal.get("days", {}).keys(), key=int):
        entries = cal["days"][day]
        if not entries:
            continue
        any_day = True
        names = ", ".join(f"{e['name']} ({e['leave_type']})" for e in entries)
        print(f"  {int(day):2d}: {names}")
    if not any_day and not annual:
        print(
            "  (no leave this month in synced sources — calendars are readable but May has no "
            "leave-titled events; fix Leave2026 sheet 90215 or add rows to the leave Bitable)"
        )


def _print_wfh_calendar(cal: dict[str, Any]) -> None:
    y, m = cal["year"], cal["month"]
    print(f"WFH calendar — {y}-{m:02d}")
    print(f"Sources: {cal.get('total_records', 0)} WFH row(s)")
    for w in cal.get("warnings") or []:
        print(f"⚠️  {w}")
    print()
    any_day = False
    for day in sorted(cal.get("days", {}).keys(), key=int):
        entries = cal["days"][day]
        if not entries:
            continue
        any_day = True
        names = ", ".join(e["name"] for e in entries)
        print(f"  {int(day):2d}: {names}")
    if not any_day:
        print("  (no WFH entries this month)")


def main():
    global DEBUG
    args = sys.argv[1:]
    output_csv = False
    show_calendar = False
    sync_month = False
    resync_month = False
    show_wfh_calendar = False
    sync_wfh_month = False
    resync_wfh_month = False
    cal_year: Optional[int] = None
    cal_month: Optional[int] = None

    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--debug":
            DEBUG = True
        elif arg == "--csv":
            output_csv = True
        elif arg == "--leave-today":
            payload = get_leave_today_payload()
            print(payload.get("text") or "")
            return
        elif arg == "--calendar":
            show_calendar = True
            if i + 1 < len(args) and re.match(r"^\d{4}-\d{1,2}$", args[i + 1]):
                i += 1
                y_s, m_s = args[i].split("-", 1)
                cal_year, cal_month = int(y_s), int(m_s)
        elif arg == "--sync-month":
            sync_month = True
            if i + 1 < len(args) and re.match(r"^\d{4}-\d{1,2}$", args[i + 1]):
                i += 1
                y_s, m_s = args[i].split("-", 1)
                cal_year, cal_month = int(y_s), int(m_s)
        elif arg == "--resync-month":
            sync_month = True
            resync_month = True
            if i + 1 < len(args) and re.match(r"^\d{4}-\d{1,2}$", args[i + 1]):
                i += 1
                y_s, m_s = args[i].split("-", 1)
                cal_year, cal_month = int(y_s), int(m_s)
        elif arg == "--calendar-wfh":
            show_wfh_calendar = True
            if i + 1 < len(args) and re.match(r"^\d{4}-\d{1,2}$", args[i + 1]):
                i += 1
                y_s, m_s = args[i].split("-", 1)
                cal_year, cal_month = int(y_s), int(m_s)
        elif arg == "--sync-wfh-month":
            sync_wfh_month = True
            if i + 1 < len(args) and re.match(r"^\d{4}-\d{1,2}$", args[i + 1]):
                i += 1
                y_s, m_s = args[i].split("-", 1)
                cal_year, cal_month = int(y_s), int(m_s)
        elif arg == "--resync-wfh-month":
            sync_wfh_month = True
            resync_wfh_month = True
            if i + 1 < len(args) and re.match(r"^\d{4}-\d{1,2}$", args[i + 1]):
                i += 1
                y_s, m_s = args[i].split("-", 1)
                cal_year, cal_month = int(y_s), int(m_s)
        else:
            print(f"Unknown argument: {arg}", file=sys.stderr)
            sys.exit(1)
        i += 1

    try:
        if sync_month:
            today = date.today()
            y = cal_year if cal_year is not None else today.year
            m = cal_month if cal_month is not None else today.month
            result = sync_leave_calendar_to_bitable(
                year=y, month=m, force_resync=resync_month
            )
            if result.get("skipped"):
                print(f"Sync skipped: {result.get('message', 'no data')}")
                for w in result.get("warnings") or []:
                    print(f"  ⚠️  {w}")
                sys.exit(1)
            print(
                f"Sync {y}-{m:02d}: deleted {result['deleted']} row(s), "
                f"added {result.get('added', result['created'])} new, "
                f"{result.get('already_synced', 0)} unchanged "
                f"(annual leave: {result['annual_leave_rows']}), "
                f"month_changed={result['month_changed']}"
            )
            print(f"  Table: {result.get('tracking_base')} / {result.get('tracking_table')}")
            for w in result.get("warnings") or []:
                print(f"  ⚠️  {w}")
            if result.get("delete_errors"):
                print("Delete errors:", result["delete_errors"], file=sys.stderr)
            if result.get("create_errors"):
                print("Create errors:", result["create_errors"], file=sys.stderr)
                sys.exit(1)
            return

        if sync_wfh_month:
            today = date.today()
            y = cal_year if cal_year is not None else today.year
            m = cal_month if cal_month is not None else today.month
            result = sync_wfh_calendar_to_bitable(
                year=y, month=m, force_resync=resync_wfh_month
            )
            if result.get("skipped"):
                print(f"WFH sync skipped: {result.get('message', 'no data')}")
                for w in result.get("warnings") or []:
                    print(f"  ⚠️  {w}")
                sys.exit(1)
            print(
                f"WFH sync {y}-{m:02d}: deleted {result['deleted']} row(s), "
                f"added {result.get('added', 0)} new, "
                f"{result.get('already_synced', 0)} unchanged, "
                f"month_changed={result['month_changed']}"
            )
            print(f"  Table: {result.get('tracking_base')} / {result.get('tracking_table')}")
            for w in result.get("warnings") or []:
                print(f"  ⚠️  {w}")
            if result.get("create_errors"):
                print("Create errors:", result["create_errors"], file=sys.stderr)
                sys.exit(1)
            return

        if show_wfh_calendar:
            today = date.today()
            y = cal_year if cal_year is not None else today.year
            m = cal_month if cal_month is not None else today.month
            _print_wfh_calendar(get_wfh_calendar(y, m))
            return

        if show_calendar:
            today = date.today()
            y = cal_year if cal_year is not None else today.year
            m = cal_month if cal_month is not None else today.month
            cal = get_leave_calendar(y, m)
            _print_leave_calendar(cal)
            return

        token = get_tenant_access_token()
        debug_print("Token obtained successfully", f"base={BASE_ID} table={TABLE_ID}")

        records = get_all_records(token, BASE_ID, TABLE_ID)
        rows = []
        for rec in records:
            row = _parse_leave_row(rec, require_approved=True)
            if not row:
                continue
            rows.append(
                [
                    row["name"],
                    row["leave_type"],
                    row["start"].strftime("%Y-%m-%d"),
                    row["end"].strftime("%Y-%m-%d"),
                    row["reason"],
                ]
            )

        debug_print(f"Approved records: {len(rows)}")

        if not rows:
            print("No approved leave records found.")
            return

        columns = ["Name", "Leave Type", "Start Date", "End Date", "Reason"]

        if output_csv:
            writer = csv.writer(sys.stdout)
            writer.writerow(columns)
            writer.writerows(rows)
        else:
            col_widths = [len(col) for col in columns]
            for row in rows:
                for i, cell in enumerate(row):
                    col_widths[i] = max(col_widths[i], len(cell))

            def print_sep():
                print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")

            print_sep()
            header = "| " + " | ".join(c.ljust(w) for c, w in zip(columns, col_widths)) + " |"
            print(header)
            print_sep()
            for row in rows:
                line = "| " + " | ".join(cell.ljust(w) for cell, w in zip(row, col_widths)) + " |"
                print(line)
            print_sep()
            print(f"Total approved leaves: {len(approved)}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
