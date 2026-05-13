#!/usr/bin/env python3
"""
OSE Duty + Leave + Offset

- Shift source: OSE sheet (`D`=day, `N`=night)
- Leave source: Lark Bitable (Approved + date range)
- Offset source: Lark Bitable (Approved + Original/Exchange date)
"""

from __future__ import annotations

import calendar
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")

SPREADSHEET_TOKEN = os.getenv("OSE_SPREADSHEET_TOKEN")
SHEET_ID = os.getenv("OSE_SHEET_ID")

# Leave / Offset Bitable (defaults from user-provided URLs).
OSE_BASE_TOKEN = os.getenv("OSE_BASE_TOKEN", "CpdEbEofwaYyyEsSjlElKNxzgec")
OSE_LEAVE_TABLE_ID = os.getenv("OSE_LEAVE_TABLE_ID", "tblmHJHe12BCJRD8")
OSE_OFFSET_TABLE_ID = os.getenv("OSE_OFFSET_TABLE_ID", "tblC5T2MAydwT42j")

TARGET_NAMES = [
    "Louie",
    "Bryan Peh",
    "Eduard James",
    "Chrisjames",
    "Augustine Si yew",
    "Man Chung",
    "Jan Rei",
    "Katleen",
    "Lynette",
    "Chun Chee",
    "Renzel",
    "Jun Chen",
    "Kenneth",
    "Jewel",
    "Justine Miguel",
    "Kheng Kwan",
    "Kris Ng",
]

MONTH_MAP = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}

DEBUG = False

# In-memory OSE shift sheet (avoids one full-sheet fetch per day for calendar / repeated /ose).
_OSE_SHEET_CACHE_TTL_SEC = int(os.getenv("OSE_SHEET_CACHE_SEC", "120"))
_OSE_SHEET_CACHE: dict[str, Any] = {"mono": 0.0, "values": None}


def debug_print(*args, **kwargs) -> None:
    if DEBUG:
        print("[DEBUG]", *args, file=sys.stderr, **kwargs)


TARGET_USER_OPEN_ID = (
    os.getenv("omduty", "").strip()
    or os.getenv("OMDUTY", "").strip()
    or "ou_d7bc33724e2d6ced4050c944c2ca5650"
)


def _name_key(name: str) -> str:
    # "Augustine (Si Yew)" and "Augustine Si Yew" should match.
    return re.sub(r"[^a-z0-9]+", "", str(name or "").lower())


def _word_tokens(name: str) -> list[str]:
    """Lowercase word tokens for roster short name vs leave-sheet full name."""
    return [t.lower() for t in re.findall(r"[A-Za-z0-9]+", str(name or "")) if t]


def _token_prefix_matches_roster_to_leave(roster_tokens: list[str], leave_tokens: list[str]) -> bool:
    """
    True if roster name refers to the same person as leave full name.
    Examples: Lynette ↔ Lynette Enriquez; Jun Chen ↔ Jun Chen Wong.
    First token may match by equality or (if roster is a single word) by prefix with min length 3
    to avoid matching unrelated one-letter names.
    """
    if not roster_tokens or not leave_tokens:
        return False
    if len(roster_tokens) > len(leave_tokens):
        return False
    for i, rw in enumerate(roster_tokens):
        lw = leave_tokens[i]
        if i == 0 and len(roster_tokens) == 1:
            if rw == lw:
                return True
            if len(rw) >= 3 and lw.startswith(rw):
                return True
            return False
        if rw != lw:
            return False
    return True


def _names_same_person(roster_name: str, leave_sheet_name: str) -> bool:
    """Roster / shift label vs leave Bitable full name (may differ in length)."""
    if not roster_name or not leave_sheet_name:
        return False
    rk, lk = _name_key(roster_name), _name_key(leave_sheet_name)
    if rk and rk == lk:
        return True
    rt, lt = _word_tokens(roster_name), _word_tokens(leave_sheet_name)
    return _token_prefix_matches_roster_to_leave(rt, lt) or _token_prefix_matches_roster_to_leave(lt, rt)


def _title_name(name: str) -> str:
    return re.sub(r"\s+", " ", str(name or "").strip()).title()


def _field_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (int, float)):
        return str(v).strip()
    if isinstance(v, dict):
        t = str(v.get("name") or v.get("text") or v.get("value") or "").strip()
        if t:
            return t
        return str(v).strip()
    if isinstance(v, list):
        parts: list[str] = []
        for item in v:
            s = _field_text(item)
            if s:
                parts.append(s)
        return ", ".join(parts).strip()
    return str(v).strip()


def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def _get_field_by_aliases(fields: dict[str, Any], aliases: list[str]) -> Any:
    """
    Resolve field value by fuzzy key matching (case/space/punct-insensitive).
    Useful when Bitable returns slightly different field names.
    """
    if not isinstance(fields, dict):
        return None
    if not aliases:
        return None
    alias_norm = [_norm_key(a) for a in aliases if a]
    if not alias_norm:
        return None

    # 1) exact normalized match
    for k, v in fields.items():
        nk = _norm_key(k)
        if nk in alias_norm:
            return v
    # 2) contains match (both directions)
    for k, v in fields.items():
        nk = _norm_key(k)
        for an in alias_norm:
            if an in nk or nk in an:
                return v
    return None


def _parse_date_value(v: Any) -> Optional[date]:
    if v is None or v == "":
        return None
    if isinstance(v, list):
        for item in v:
            d = _parse_date_value(item)
            if d:
                return d
        return None
    if isinstance(v, dict):
        # common date-like keys in bitable payload
        for key in ("value", "timestamp", "ts", "date", "start_date", "end_date"):
            if key in v:
                d = _parse_date_value(v.get(key))
                if d:
                    return d
        # fallback: try dict text flatten
        sdict = _field_text(v)
        if sdict:
            return _parse_date_value(sdict)
        return None
    if isinstance(v, (int, float)):
        ts = int(v)
        if ts > 10**12:
            ts //= 1000
        try:
            return datetime.fromtimestamp(ts).date()
        except Exception:
            return None
    s = _field_text(v)
    if not s:
        return None
    fmts = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    )
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    m = re.match(r"^\s*(\d{4})[-/](\d{2})[-/](\d{2})", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m2 = re.match(r"^\s*(\d{2})[-/](\d{2})[-/](\d{4})", s)
    if m2:
        try:
            return date(int(m2.group(3)), int(m2.group(2)), int(m2.group(1)))
        except ValueError:
            return None
    return None


def _format_ddmmyyyy(d: Optional[date]) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else "-"


def col_index_to_letter(col_index: int) -> str:
    letters = ""
    while col_index > 0:
        col_index -= 1
        letters = chr(65 + (col_index % 26)) + letters
        col_index //= 26
    return letters


def get_tenant_access_token() -> str:
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data, timeout=20)
    result = resp.json()
    if result.get("code") != 0:
        raise RuntimeError(f"Failed to get token: {result}")
    return result["tenant_access_token"]


def get_sheet_metadata(token: str, spreadsheet_token: str, sheet_id: str) -> Optional[dict[str, Any]]:
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
    headers = {"Authorization": f"Bearer {token}"}
    result = requests.get(url, headers=headers, timeout=20).json()
    if result.get("code") != 0:
        debug_print(f"Metadata error: {result.get('msg')}")
        return None
    sheets = result.get("data", {}).get("sheets", [])
    for sheet in sheets:
        if sheet.get("sheetId") == sheet_id:
            return {
                "rowCount": sheet.get("rowCount"),
                "columnCount": sheet.get("columnCount"),
            }
    return None


def get_range_values(token: str, spreadsheet_token: str, sheet_id: str, range_str: str) -> Optional[list[list[Any]]]:
    url = (
        f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/"
        f"{spreadsheet_token}/values/{sheet_id}!{range_str}?valueRenderOption=FormattedValue"
    )
    headers = {"Authorization": f"Bearer {token}"}
    result = requests.get(url, headers=headers, timeout=30).json()
    if result.get("code") != 0:
        debug_print(f"Range values error: {result}")
        return None
    return result.get("data", {}).get("valueRange", {}).get("values", [])


def parse_month_year(text: Any) -> tuple[Optional[int], Optional[int]]:
    s = _field_text(text)
    if not s:
        return None, None
    for mon_name, mon_num in MONTH_MAP.items():
        pattern = rf"\b{re.escape(mon_name)}\b\s+(\d{{4}})"
        m = re.search(pattern, s, re.IGNORECASE)
        if m:
            return mon_num, int(m.group(1))
    return None, None


def _get_cached_ose_sheet_values() -> tuple[Optional[list[list[Any]]], Optional[str]]:
    """Fetch full shift sheet once; short TTL cache shared by ``get_shift_names_for_date`` and month calendar."""
    now = time.monotonic()
    if (
        _OSE_SHEET_CACHE_TTL_SEC > 0
        and isinstance(_OSE_SHEET_CACHE.get("values"), list)
        and now - float(_OSE_SHEET_CACHE.get("mono") or 0) < _OSE_SHEET_CACHE_TTL_SEC
    ):
        return _OSE_SHEET_CACHE["values"], None
    if not SPREADSHEET_TOKEN or not SHEET_ID:
        return None, "OSE_SPREADSHEET_TOKEN / OSE_SHEET_ID not set"
    try:
        token = get_tenant_access_token()
    except Exception as e:
        return None, str(e)
    props = get_sheet_metadata(token, SPREADSHEET_TOKEN, SHEET_ID)
    if not props:
        return None, "Sheet metadata unavailable"
    max_row = props.get("rowCount", 200)
    max_col = props.get("columnCount", 200)
    scan_range = f"A1:{col_index_to_letter(max_col)}{max_row}"
    values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, scan_range)
    if not values or len(values) < 2:
        return None, "Empty or invalid sheet range"
    _OSE_SHEET_CACHE["values"] = values
    _OSE_SHEET_CACHE["mono"] = now
    return values, None


def _shift_names_from_matrix(values: list[list[Any]], target_date: date) -> tuple[list[str], list[str]]:
    """Parse ``D`` / ``N`` for one calendar day from preloaded sheet ``values`` (same rules as legacy scan)."""
    current_year = target_date.year
    current_month = target_date.month
    current_day = target_date.day

    date_col = None
    for row_idx in range(1, min(15, len(values))):
        row = values[row_idx] if row_idx < len(values) else []
        for col in range(len(row)):
            try:
                day_num = int(str(row[col]).strip())
            except Exception:
                continue
            if day_num != current_day:
                continue
            header = ""
            for hcol in range(col, -1, -1):
                if hcol < len(values[0]) and values[0][hcol]:
                    header = values[0][hcol]
                    break
            mon_num, year = parse_month_year(header)
            if mon_num == current_month and year == current_year:
                date_col = col
                break
        if date_col is not None:
            break
    if date_col is None:
        return [], []

    name_rows: dict[str, int] = {}
    for row_idx in range(2, len(values)):
        row = values[row_idx]
        if not row:
            continue
        name_cell = _field_text(row[0] if len(row) > 0 else "")
        if not name_cell:
            continue
        up = name_cell.upper()
        for target in TARGET_NAMES:
            if up.startswith(target.upper()) and target not in name_rows:
                name_rows[target] = row_idx
                break

    morning: list[str] = []
    night: list[str] = []
    for name, row_idx in name_rows.items():
        row = values[row_idx]
        if date_col >= len(row):
            continue
        code = _field_text(row[date_col]).upper()
        if code == "D":
            morning.append(_title_name(name))
        elif code == "N":
            night.append(_title_name(name))
    return sorted(morning), sorted(night)


def get_shift_names_for_date(target_date: date) -> tuple[list[str], list[str]]:
    """Return (morning_names, night_names) from OSE shift sheet."""
    values, _err = _get_cached_ose_sheet_values()
    if not values:
        return [], []
    return _shift_names_from_matrix(values, target_date)


def get_ose_month_calendar(year: int, month: int) -> dict[str, Any]:
    """
    Build a month grid for the web dashboard: each day has morning (``D``) and night (``N``) from the OSE sheet,
    with the same leave filtering as ``get_ose_payload_for_date`` (mode ``date``).

    Each day cell also includes ``leave`` (list of serializable dicts) and ``offset`` (list of strings),
    using the same Bitable helpers as the Lark card — without changing those helpers.

    Returns keys: ``ok``, ``year``, ``month``, ``month_label``, ``weeks`` (Mon-first rows),
    optional ``error``, ``bitable_warning``.
    """
    month_names = (
        "",
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    )
    if month < 1 or month > 12:
        return {"ok": False, "error": "Invalid month", "year": year, "month": month, "weeks": []}
    values, sheet_err = _get_cached_ose_sheet_values()
    if not values:
        return {
            "ok": False,
            "error": sheet_err or "No sheet data",
            "year": year,
            "month": month,
            "month_label": f"{month_names[month]} {year}",
            "weeks": [],
        }

    leave_items: list[dict[str, Any]] = []
    offset_items: list[dict[str, Any]] = []
    bitable_warning: Optional[str] = None
    token: Optional[str] = None
    try:
        token = get_tenant_access_token()
        leave_items, offset_items = _get_bitable_raw_pair(token)
    except Exception as e:
        bitable_warning = f"Leave filter skipped: {e}"

    def _on_leave(shift_label: str, entries: list[dict[str, Any]]) -> bool:
        for r in entries:
            ln = str(r.get("name") or "").strip()
            if ln and _names_same_person(shift_label, ln):
                return True
        return False

    def _leave_for_json(entries: list[dict[str, Any]]) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        for row in entries:
            st = row.get("start")
            ed = row.get("end")
            out.append(
                {
                    "name": str(row.get("name") or ""),
                    "leave_type": str(row.get("leave_type") or "Leave"),
                    "start": _format_ddmmyyyy(st) if isinstance(st, date) else "",
                    "end": _format_ddmmyyyy(ed) if isinstance(ed, date) else "",
                }
            )
        return out

    _, last_day = calendar.monthrange(year, month)
    day_payload: dict[int, dict[str, Any]] = {}
    for dnum in range(1, last_day + 1):
        d = date(year, month, dnum)
        morning, night = _shift_names_from_matrix(values, d)
        leave_entries: list[dict[str, Any]] = []
        offset_lines: list[str] = []
        if leave_items and token:
            try:
                leave_entries = _extract_leave_entries_for_date(d, token, items=leave_items)
            except Exception:
                leave_entries = []
        if offset_items and token:
            try:
                offset_lines = _extract_offset_lines_for_date(d, token, items=offset_items)
            except Exception:
                offset_lines = []
        morning = [n for n in morning if not _on_leave(n, leave_entries)]
        night = [n for n in night if not _on_leave(n, leave_entries)]
        day_payload[dnum] = {
            "day": dnum,
            "morning": morning,
            "night": night,
            "leave": _leave_for_json(leave_entries),
            "offset": offset_lines,
        }

    cal_weeks = calendar.monthcalendar(year, month)
    weeks_out: list[list[Optional[dict[str, Any]]]] = []
    for row in cal_weeks:
        line: list[Optional[dict[str, Any]]] = []
        for dnum in row:
            if dnum == 0:
                line.append(None)
            else:
                line.append(day_payload.get(dnum))
        weeks_out.append(line)

    return {
        "ok": True,
        "error": None,
        "year": year,
        "month": month,
        "month_label": f"{month_names[month]} {year}",
        "weeks": weeks_out,
        "bitable_warning": bitable_warning,
    }


# Short-lived in-memory cache so morning card + /ose do not double-hit Bitable.
# Set OSE_BITABLE_CACHE_SEC=0 to disable. Daily cron calls ``invalidate_ose_bitable_cache``.
_OSE_BITABLE_RAW: dict[str, Any] = {"monotonic": 0.0, "leave": None, "offset": None}
_OSE_BITABLE_TTL_SEC = int(os.getenv("OSE_BITABLE_CACHE_SEC", "120"))


def invalidate_ose_bitable_cache() -> None:
    """Clear cached leave/offset rows (e.g. after daily sync job)."""
    _OSE_BITABLE_RAW["leave"] = None
    _OSE_BITABLE_RAW["offset"] = None
    _OSE_BITABLE_RAW["monotonic"] = 0.0


def sync_ose_leave_offset_bitable() -> str:
    """
    Force-fetch leave + offset tables from Lark (for a daily scheduler).
    Returns a one-line status for logs.
    """
    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ OSE Bitable sync (token): {e}"
    invalidate_ose_bitable_cache()
    try:
        leave, offset = _get_bitable_raw_pair(token)
    except Exception as e:
        return f"❌ OSE Bitable sync: {e}"
    return f"✅ OSE leave/offset Bitable synced ({len(leave)} leave, {len(offset)} offset rows)"


def _get_bitable_raw_pair(token: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    now = time.monotonic()
    leave = _OSE_BITABLE_RAW.get("leave")
    offset = _OSE_BITABLE_RAW.get("offset")
    ts = float(_OSE_BITABLE_RAW.get("monotonic") or 0)
    if (
        _OSE_BITABLE_TTL_SEC > 0
        and isinstance(leave, list)
        and isinstance(offset, list)
        and now - ts < _OSE_BITABLE_TTL_SEC
    ):
        return leave, offset
    leave = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_LEAVE_TABLE_ID)
    offset = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_OFFSET_TABLE_ID)
    _OSE_BITABLE_RAW["monotonic"] = now
    _OSE_BITABLE_RAW["leave"] = leave
    _OSE_BITABLE_RAW["offset"] = offset
    return leave, offset


def _bitable_get_all_records(token: str, app_token: str, table_id: str) -> list[dict[str, Any]]:
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {token}"}
    out: list[dict[str, Any]] = []
    page_token = None
    while True:
        params: dict[str, Any] = {"page_size": 200}
        if page_token:
            params["page_token"] = page_token
        res = requests.get(url, headers=headers, params=params, timeout=30).json()
        if res.get("code") != 0:
            raise RuntimeError(f"Bitable fetch failed: {res}")
        data = res.get("data", {})
        out.extend(data.get("items") or [])
        if not data.get("has_more"):
            break
        page_token = data.get("page_token")
    return out


def _is_approved(v: Any) -> bool:
    return _field_text(v).strip().lower() == "approved"


def _extract_leave_entries_for_date(
    target_date: date,
    token: str,
    *,
    items: Optional[list[dict[str, Any]]] = None,
) -> list[dict[str, Any]]:
    if items is None:
        items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_LEAVE_TABLE_ID)
    out: list[dict[str, Any]] = []
    for it in items:
        f = it.get("fields") or {}
        status_v = _get_field_by_aliases(f, ["Status", "Approval Status"])
        if not _is_approved(status_v):
            continue
        name = _title_name(_field_text(_get_field_by_aliases(f, ["Name", "Employee Name", "Person"])))
        if not name:
            continue
        st = _parse_date_value(_get_field_by_aliases(f, ["Start Date", "Leave Start Date", "From"]))
        ed = _parse_date_value(_get_field_by_aliases(f, ["End Date", "Leave End Date", "To"]))
        if not st or not ed:
            continue
        if st <= target_date <= ed:
            out.append(
                {
                    "name": name,
                    "leave_type": _field_text(_get_field_by_aliases(f, ["Leave Type", "Type"])) or "Leave",
                    "start": st,
                    "end": ed,
                }
            )
    return sorted(out, key=lambda x: x["name"])


def _extract_offset_lines_for_date(
    target_date: date,
    token: str,
    *,
    items: Optional[list[dict[str, Any]]] = None,
) -> list[str]:
    if items is None:
        items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_OFFSET_TABLE_ID)
    lines: list[str] = []
    for it in items:
        f = it.get("fields") or {}
        approval_v = _get_field_by_aliases(f, ["Approval Status", "Status"])
        if not _is_approved(approval_v):
            continue
        req = _title_name(
            _field_text(_get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]))
        )
        exc = _title_name(
            _field_text(_get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]))
        )
        od = _parse_date_value(_get_field_by_aliases(f, ["Original Date", "Request Date", "Date"]))
        xd = _parse_date_value(_get_field_by_aliases(f, ["Exchange Date", "Swap Date", "Target Date"]))
        if not req or not exc or not od or not xd:
            continue
        if target_date != od and target_date != xd:
            continue
        if req.lower() == exc.lower():
            lines.append(f"• {req} is offset with him/herself.")
            continue
        lines.append(
            f"• {req}({_format_ddmmyyyy(od)}) offset with {exc}({_format_ddmmyyyy(xd)})"
        )
    return sorted(set(lines))


def _section_lines(title: str, rows: list[str], *, empty_text: str = "• -") -> list[str]:
    out = [title]
    if rows:
        out.extend(rows)
    else:
        out.append(empty_text)
    out.append("")
    return out


def _build_ose_context(target_date: date, mode: str) -> tuple[list[str], list[str], list[str], list[dict[str, Any]], Optional[str]]:
    """
    mode:
      - 'morning': Rest yesterday night, Luck today morning
      - 'evening': Rest today morning, Luck today night
      - 'date': Morning shift (D) and Night shift (N) on ``target_date`` (/osedate)
    """
    try:
        if mode == "evening":
            rest_names, _night_unused = get_shift_names_for_date(target_date)
            _m_unused, luck_names = get_shift_names_for_date(target_date)
        elif mode == "morning":
            _, rest_names = get_shift_names_for_date(target_date - timedelta(days=1))
            luck_names, _ = get_shift_names_for_date(target_date)
        else:
            # ``date`` (/osedate): same calendar day — morning (D) first, night (N) second.
            rest_names, luck_names = get_shift_names_for_date(target_date)

        token = get_tenant_access_token()
        leave_items, offset_items = _get_bitable_raw_pair(token)
        leave_entries = _extract_leave_entries_for_date(target_date, token, items=leave_items)
        offset_lines = _extract_offset_lines_for_date(target_date, token, items=offset_items)
    except Exception as e:
        return [], [], [], [], f"❌ OSE data load failed: {e}"

    def _on_leave(shift_label: str) -> bool:
        for r in leave_entries:
            ln = str(r.get("name") or "").strip()
            if ln and _names_same_person(shift_label, ln):
                return True
        return False

    rest_names = [n for n in rest_names if not _on_leave(n)]
    luck_names = [n for n in luck_names if not _on_leave(n)]
    return sorted(rest_names), sorted(luck_names), offset_lines, leave_entries, None


def build_ose_message_card(
    *,
    target_date: date,
    rest_names: list[str],
    luck_names: list[str],
    offset_lines: list[str],
    leave_entries: list[dict[str, Any]],
    include_tag: bool = False,
    first_section_title: str = "😴 (～￣▽￣)～ Rest Well",
    second_section_title: str = "🍀 Good Luckヾ(≧▽≦*)o",
) -> dict[str, Any]:
    lines: list[str] = []
    if include_tag and TARGET_USER_OPEN_ID:
        lines.append(f'👥 <at id="{TARGET_USER_OPEN_ID}">User</at>')
    lines.append(f"📅 **{target_date.strftime('%d/%m/%Y')}**")
    lines.append("")
    lines.extend(_section_lines(first_section_title, [f"• {n}" for n in rest_names]))
    lines.extend(_section_lines(second_section_title, [f"• {n}" for n in luck_names]))
    if offset_lines:
        lines.extend(_section_lines("🔁 Offset", offset_lines))
    if leave_entries:
        leave_lines: list[str] = []
        for row in leave_entries:
            name = row["name"]
            lt = row["leave_type"]
            st = row["start"]
            ed = row["end"]
            if st == ed:
                leave_lines.append(f"• {name} ({lt}) - {_format_ddmmyyyy(st)}")
            else:
                leave_lines.append(
                    f"• {name} ({lt}) - From {_format_ddmmyyyy(st)} until {_format_ddmmyyyy(ed)}"
                )
        lines.extend(_section_lines("🏖️ Leave", leave_lines))
    content = "\n".join(lines).strip()

    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "green",
            "title": {
                "tag": "plain_text",
                "content": f"OSE DUTY FOR {target_date.strftime('%d/%m/%Y')}",
            },
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": content,
                    },
                }
            ]
        },
    }


def get_ose_payload_for_date(target_date: date, mode: str = "date", *, include_tag: bool = False) -> dict[str, Any]:
    rest_names, luck_names, offset_lines, leave_entries, err = _build_ose_context(target_date, mode)
    if err:
        return {"text": err, "lark_card": None}

    if mode == "date":
        first_title_plain = "Morning shift"
        second_title_plain = "Night shift"
        first_title_card = "🌅 Morning shift"
        second_title_card = "🌙 Night shift"
    else:
        first_title_plain = "(～￣▽￣)～ Rest Well"
        second_title_plain = "Good Luckヾ(≧▽≦*)o"
        first_title_card = "😴 (～￣▽￣)～ Rest Well"
        second_title_card = "🍀 Good Luckヾ(≧▽≦*)o"

    lines: list[str] = []
    if include_tag and TARGET_USER_OPEN_ID:
        lines.append(f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>')
    lines.append(first_title_plain)
    lines.extend([f"• {n}" for n in rest_names] or ["• -"])
    lines.append("")
    lines.append(second_title_plain)
    lines.extend([f"• {n}" for n in luck_names] or ["• -"])
    if offset_lines:
        lines.append("")
        lines.append("Offset")
        lines.extend(offset_lines)
    if leave_entries:
        lines.append("")
        lines.append("Leave")
        for row in leave_entries:
            name = row["name"]
            lt = row["leave_type"]
            st = row["start"]
            ed = row["end"]
            if st == ed:
                lines.append(f"• {name} ({lt}) - {_format_ddmmyyyy(st)}")
            else:
                lines.append(
                    f"• {name} ({lt}) - From {_format_ddmmyyyy(st)} until {_format_ddmmyyyy(ed)}"
                )

    text = "\n".join(lines).strip()
    return {
        "text": text,
        "lark_card": build_ose_message_card(
            target_date=target_date,
            rest_names=rest_names,
            luck_names=luck_names,
            offset_lines=offset_lines,
            leave_entries=leave_entries,
            include_tag=include_tag,
            first_section_title=first_title_card,
            second_section_title=second_title_card,
        ),
    }


def get_ose_payload_for_now(now_dt: Optional[datetime] = None, *, include_tag: bool = False) -> dict[str, Any]:
    now_dt = now_dt or datetime.now()
    mode = "evening" if now_dt.hour >= 19 else "morning"
    return get_ose_payload_for_date(now_dt.date(), mode=mode, include_tag=include_tag)


def get_ose_duty_for_date(target_date: date) -> str:
    return str(get_ose_payload_for_date(target_date, mode="date").get("text") or "")


def osedate(date_str: str) -> str:
    try:
        target_date = datetime.strptime(date_str, "%d/%m/%Y").date()
    except ValueError:
        return "❌ Invalid date format. Please use DD/MM/YYYY (e.g., 31/12/2026)"
    return get_ose_duty_for_date(target_date)


def get_ose_today_duty() -> str:
    return str(get_ose_payload_for_now().get("text") or "")


OSE_LEAVE_FORM_NAMES: tuple[str, ...] = (
    "Louie",
    "Bryan Peh",
    "Eduard James",
    "Chrisjames",
    "Augustine Si Yew",
    "Man Chung",
    "Jan Rei",
    "Katleen",
    "Lynette",
    "Chun Chee",
    "Renzel",
    "Jun Chen",
    "Justine Miguel",
    "Kenneth",
    "Jewel",
    "Kheng Kwan",
    "Kris Ng",
    "Jeno",
    "Faye",
    "Shie Ni",
    "Kwang Ming",
)

OSE_LEAVE_TYPES: tuple[str, ...] = (
    "Sick Leave",
    "Annual Leave",
    "Compassionate Leave",
    "Hospitalisation Leave",
    "Marriage Leave",
    "Maternity Leave",
    "Non Pay Leave",
    "Replacement Leave",
)

OSE_SHIFT_TYPES: tuple[str, ...] = ("N", "D")


def _bitable_date_ms(d: date) -> int:
    return int(datetime.combine(d, datetime.min.time()).timestamp() * 1000)


def _bitable_create_record(token: str, table_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    url = (
        f"https://open.larksuite.com/open-apis/bitable/v1/apps/"
        f"{OSE_BASE_TOKEN}/tables/{table_id}/records"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    res = requests.post(url, headers=headers, json={"fields": fields}, timeout=30).json()
    if res.get("code") != 0:
        raise RuntimeError(f"Bitable create failed: {res}")
    return res


def _person_field_value(name: str) -> list[dict[str, str]]:
    n = _title_name(name)
    return [{"name": n}]


def get_ose_submit_form_options() -> dict[str, Any]:
    return {
        "leave_names": list(OSE_LEAVE_FORM_NAMES),
        "offset_names": list(OSE_LEAVE_FORM_NAMES),
        "leave_types": list(OSE_LEAVE_TYPES),
        "shift_types": list(OSE_SHIFT_TYPES),
    }


def _iter_days_in_month(year: int, month: int) -> list[date]:
    _, last = calendar.monthrange(year, month)
    return [date(year, month, d) for d in range(1, last + 1)]


def _leave_rows_for_calendar(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for it in items:
        f = it.get("fields") or {}
        name = _title_name(_field_text(_get_field_by_aliases(f, ["Name", "Employee Name", "Person"])))
        if not name:
            continue
        st = _parse_date_value(_get_field_by_aliases(f, ["Start Date", "Leave Start Date", "From"]))
        ed = _parse_date_value(_get_field_by_aliases(f, ["End Date", "Leave End Date", "To"]))
        if not st or not ed:
            continue
        rows.append({"name": name, "start": st, "end": ed})
    return rows


def get_ose_leave_names_calendar(year: int, month: int) -> dict[str, Any]:
    """Per-day leave names for a month (all dated rows; LeaveID ignored)."""
    if month < 1 or month > 12:
        raise ValueError("month must be 1–12")
    token = get_tenant_access_token()
    items, _ = _get_bitable_raw_pair(token)
    rows = _leave_rows_for_calendar(items)
    month_start = date(year, month, 1)
    _, last = calendar.monthrange(year, month)
    month_end = date(year, month, last)
    days: dict[str, list[str]] = {}
    for d in range(1, last + 1):
        days[str(d)] = []
    for row in rows:
        st: date = row["start"]
        ed: date = row["end"]
        if ed < month_start or st > month_end:
            continue
        cur = max(st, month_start)
        end = min(ed, month_end)
        while cur <= end:
            key = str(cur.day)
            if row["name"] not in days[key]:
                days[key].append(row["name"])
            cur += timedelta(days=1)
    for key in days:
        days[key].sort(key=lambda x: x.lower())
    return {"ok": True, "year": year, "month": month, "days": days}


def _parse_iso_date(raw: str) -> date:
    s = (raw or "").strip()
    if not s:
        raise ValueError("date is required")
    try:
        return date.fromisoformat(s)
    except ValueError as e:
        raise ValueError(f"invalid date {raw!r} (use YYYY-MM-DD)") from e


def submit_ose_leave(
    *,
    name: str,
    leave_type: str,
    start_date: date,
    end_date: date,
    reason: str,
) -> dict[str, Any]:
    if start_date > end_date:
        raise ValueError("Start Date must be on or before End Date")
    nm = _title_name(name)
    if nm not in OSE_LEAVE_FORM_NAMES:
        raise ValueError(f"Unknown name {name!r}")
    lt = (leave_type or "").strip()
    if lt not in OSE_LEAVE_TYPES:
        raise ValueError(f"Unknown leave type {leave_type!r}")
    reason_s = (reason or "").strip()
    if not reason_s:
        raise ValueError("Reason is required")
    token = get_tenant_access_token()
    fields: dict[str, Any] = {
        "Name": _person_field_value(nm),
        "Leave Type": lt,
        "Start Date": _bitable_date_ms(start_date),
        "End Date": _bitable_date_ms(end_date),
        "Reason": reason_s,
    }
    res = _bitable_create_record(token, OSE_LEAVE_TABLE_ID, fields)
    invalidate_ose_bitable_cache()
    return {"ok": True, "record_id": (res.get("data") or {}).get("record", {}).get("record_id")}


def submit_ose_offset(
    *,
    request_person: str,
    exchange_person: str,
    shift_type: str,
    original_date: date,
    exchange_date: date,
    reason: str,
) -> dict[str, Any]:
    req = _title_name(request_person)
    exc = _title_name(exchange_person)
    if req not in OSE_LEAVE_FORM_NAMES:
        raise ValueError(f"Unknown request person {request_person!r}")
    if exc not in OSE_LEAVE_FORM_NAMES:
        raise ValueError(f"Unknown exchange person {exchange_person!r}")
    st = (shift_type or "").strip().upper()
    if st not in OSE_SHIFT_TYPES:
        raise ValueError("Shift Type must be N or D")
    reason_s = (reason or "").strip()
    if not reason_s:
        raise ValueError("Reason is required")
    token = get_tenant_access_token()
    today = date.today()
    fields: dict[str, Any] = {
        "Request Person": _person_field_value(req),
        "Exchange Person": _person_field_value(exc),
        "Shift Type": st,
        "Original Date": _bitable_date_ms(original_date),
        "Exchange Date": _bitable_date_ms(exchange_date),
        "Request Date": _bitable_date_ms(today),
        "Reason": reason_s,
    }
    res = _bitable_create_record(token, OSE_OFFSET_TABLE_ID, fields)
    invalidate_ose_bitable_cache()
    return {"ok": True, "record_id": (res.get("data") or {}).get("record", {}).get("record_id")}


if __name__ == "__main__":
    if "--debug" in sys.argv:
        DEBUG = True
        sys.argv.remove("--debug")
    if len(sys.argv) > 1:
        print(osedate(sys.argv[1]))
    else:
        print(get_ose_today_duty())