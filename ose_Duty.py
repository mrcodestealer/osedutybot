#!/usr/bin/env python3
"""
OSE Duty + Leave + Offset

- Shift source: OSE sheet (`D`=day, `N`=night)
- leaveose (OSE HRMS display): ``OSE_HRMS_LEAVE_TABLE_ID`` → tblvoXE0hsPjgb0j
- leave 全员 (company HRMS): ``OSE_ALL_LEAVE_TABLE_ID`` → tblmHJHe12BCJRD8 (``--sync-all-leave-month``)
- OSE submit / approve: ``OSE_LEAVE_TABLE_ID`` (default same as leave 全员 table)
- Offset source: Lark Bitable (Approved + Original/Exchange date)
"""

from __future__ import annotations

import calendar
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Optional

import requests
from dotenv import load_dotenv

import duty_list_match as dlm

load_dotenv()

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")

SPREADSHEET_TOKEN = (os.getenv("OSE_SPREADSHEET_TOKEN") or "UjF0saOVuhJSWLtBv9GlaQOkgbe").strip()
SHEET_ID = (os.getenv("OSE_SHEET_ID") or "3RIBRL").strip()

# Leave / Offset Bitable (defaults from user-provided URLs).
OSE_BASE_TOKEN = os.getenv("OSE_BASE_TOKEN", "CpdEbEofwaYyyEsSjlElKNxzgec")
# HRMS → OSE display sheet (webapp / admin ALL / duty calendar leave list).
# https://casinoplus.sg.larksuite.com/base/CpdEbEofwaYyyEsSjlElKNxzgec?table=tblvoXE0hsPjgb0j
# Do not fall back to TRACK_LEAVE_TABLE_ID — legacy env may still point at tblmHJHe12BCJRD8.
OSE_HRMS_LEAVE_TABLE_ID = os.getenv("OSE_HRMS_LEAVE_TABLE_ID", "tblvoXE0hsPjgb0j").strip()
# leave 全员 — company-wide HRMS leave (sync via leavewfh ``--sync-all-leave-month``).
# https://casinoplus.sg.larksuite.com/base/CpdEbEofwaYyyEsSjlElKNxzgec?table=tblmHJHe12BCJRD8
OSE_ALL_LEAVE_TABLE_ID = os.getenv(
    "OSE_ALL_LEAVE_TABLE_ID",
    os.getenv("OSE_LEAVE_TABLE_ID", "tblmHJHe12BCJRD8"),
).strip()
# OSE leave request + approval (Submit Leave form; not the same as webapp OSE display list).
OSE_LEAVE_TABLE_ID = os.getenv("OSE_LEAVE_TABLE_ID", OSE_ALL_LEAVE_TABLE_ID).strip()
OSE_OFFSET_TABLE_ID = os.getenv("OSE_OFFSET_TABLE_ID", "tblC5T2MAydwT42j")

# Bump when leave/admin Bitable routing changes (check /api/admin/leave-list meta).
OSE_LEAVE_API_BUILD = "20260603-leaveose-pinned-v4"

# leaveose sheet — OSE display MUST use this table ID (env cannot point at leave 全员).
LEAVEOSE_TABLE_ID_CANONICAL = "tblvoXE0hsPjgb0j"
LEAVEOSE_TABLE_ID = LEAVEOSE_TABLE_ID_CANONICAL

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

OSE_SHOWOFFSET_NAMES: tuple[str, ...] = (
    "Bryan Peh",
    "Augustine Si yew",
    "Man Chung",
    "Chun Chee",
    "Jun Chen",
    "Kheng Kwan",
    "Kris Ng",
)

DEBUG = False

# In-memory OSE shift sheet (avoids one full-sheet fetch per day for calendar / repeated /ose).
_OSE_SHEET_CACHE_TTL_SEC = int(os.getenv("OSE_SHEET_CACHE_SEC", "120"))
_OSE_SHEET_CACHE: dict[str, Any] = {"mono": 0.0, "values": None}
_OSE_DIR = os.path.dirname(os.path.abspath(__file__))
_OFFSET_SHIFT_SHEET_APPLIED_PATH = os.path.join(_OSE_DIR, "offset_shift_sheet_applied.json")


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


def _format_yyyymmdd(d: Optional[date]) -> str:
    return d.strftime("%Y/%m/%d") if isinstance(d, date) else ""


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


def _date_column_for_matrix(values: list[list[Any]], target_date: date) -> Optional[int]:
    """Column index for ``target_date`` on the OSE shift sheet (same header rules as legacy scan)."""
    current_year = target_date.year
    current_month = target_date.month
    current_day = target_date.day
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
                return col
    return None


def _target_name_rows_from_matrix(values: list[list[Any]]) -> dict[str, int]:
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
    return name_rows


def _shift_codes_from_matrix(
    values: list[list[Any]], target_date: date
) -> tuple[list[str], list[str], list[str]]:
    """Parse ``D`` / ``N`` / roster ``L`` (leave) for one calendar day."""
    date_col = _date_column_for_matrix(values, target_date)
    if date_col is None:
        return [], [], []

    morning: list[str] = []
    night: list[str] = []
    roster_leave: list[str] = []
    for name, row_idx in _target_name_rows_from_matrix(values).items():
        row = values[row_idx]
        if date_col >= len(row):
            continue
        code = _field_text(row[date_col]).upper()
        if code == "D":
            morning.append(_title_name(name))
        elif code == "N":
            night.append(_title_name(name))
        elif code == "L":
            roster_leave.append(_title_name(name))
    return sorted(morning), sorted(night), sorted(roster_leave)


def _merge_roster_sheet_leave(
    leave_entries: list[dict[str, Any]],
    roster_leave_names: list[str],
    target_date: date,
) -> list[dict[str, Any]]:
    """Add OSE sheet ``L`` markers when not already covered by Bitable leave."""
    if not roster_leave_names:
        return leave_entries
    out = list(leave_entries)
    for name in roster_leave_names:
        if any(_names_same_person(name, str(r.get("name") or "")) for r in out):
            continue
        out.append(
            {
                "name": _title_name(name),
                "leave_type": "Leave",
                "start": target_date,
                "end": target_date,
                "source": "roster",
            }
        )
    return sorted(out, key=lambda x: str(x.get("name") or "").lower())


def _shift_names_from_matrix(values: list[list[Any]], target_date: date) -> tuple[list[str], list[str]]:
    """Parse ``D`` / ``N`` for one calendar day from preloaded sheet ``values`` (same rules as legacy scan)."""
    morning, night, _ = _shift_codes_from_matrix(values, target_date)
    return morning, night


def _invalidate_ose_sheet_cache() -> None:
    _OSE_SHEET_CACHE["values"] = None
    _OSE_SHEET_CACHE["mono"] = 0.0


def _sheet_row_index_for_person(values: list[list[Any]], person: str) -> Optional[int]:
    """0-based matrix row for a roster person on the OSE shift sheet."""
    nm = _title_name(person)
    if not nm:
        return None
    for target, row_idx in _target_name_rows_from_matrix(values).items():
        if _names_same_person(target, nm):
            return row_idx
    return None


def _put_ose_shift_sheet_cells(token: str, cell_updates: list[tuple[int, int, str]]) -> None:
    """Write duty cells: each item is (matrix_row_idx, col_idx, value) both 0-based."""
    if not cell_updates:
        return
    if not SPREADSHEET_TOKEN or not SHEET_ID:
        raise RuntimeError("OSE shift sheet not configured (OSE_SPREADSHEET_TOKEN / OSE_SHEET_ID)")
    value_ranges: list[dict[str, Any]] = []
    for row_idx, col_idx, val in cell_updates:
        if row_idx < 0 or col_idx < 0:
            continue
        a1 = f"{SHEET_ID}!{col_index_to_letter(col_idx + 1)}{row_idx + 1}"
        value_ranges.append({"range": a1, "values": [[val]]})
    if not value_ranges:
        return
    url = (
        f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/"
        f"{SPREADSHEET_TOKEN}/values_batch_update"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    res = requests.post(url, headers=headers, json={"valueRanges": value_ranges}, timeout=60).json()
    if res.get("code") != 0:
        raise RuntimeError(f"OSE shift sheet write failed: {res}")
    _invalidate_ose_sheet_cache()


def _load_offset_shift_sheet_applied() -> set[str]:
    try:
        with open(_OFFSET_SHIFT_SHEET_APPLIED_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return set()
    except Exception:
        return set()
    if isinstance(data, dict):
        ids = data.get("record_ids")
        if isinstance(ids, list):
            return {str(x).strip() for x in ids if str(x).strip()}
    if isinstance(data, list):
        return {str(x).strip() for x in data if str(x).strip()}
    return set()


def _save_offset_shift_sheet_applied(record_ids: set[str]) -> None:
    tmp = _OFFSET_SHIFT_SHEET_APPLIED_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"record_ids": sorted(record_ids)}, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    os.replace(tmp, _OFFSET_SHIFT_SHEET_APPLIED_PATH)


def _mark_offset_shift_sheet_applied(record_id: str) -> None:
    rid = (record_id or "").strip()
    if not rid:
        return
    applied = _load_offset_shift_sheet_applied()
    applied.add(rid)
    _save_offset_shift_sheet_applied(applied)


def offset_shift_sheet_already_applied(record_id: str) -> bool:
    rid = (record_id or "").strip()
    return bool(rid and rid in _load_offset_shift_sheet_applied())


def apply_approved_offset_to_shift_sheet(
    *,
    request_person: str,
    exchange_person: str,
    original_date: date,
    exchange_date: date,
    shift_type: str,
) -> dict[str, Any]:
    """
    Apply an approved offset swap to OSE2026 (``3RIBRL``): ``*`` on swapped-off days,
    ``D``/``N`` on swapped-on days. Exchange with self only updates the requester's row.
    """
    st = (shift_type or "").strip().upper()
    if st not in OSE_SHIFT_TYPES:
        raise ValueError(f"Shift Type must be one of {OSE_SHIFT_TYPES}")
    req = _title_name(request_person)
    exc = _title_name(exchange_person)
    if not req:
        raise ValueError("request_person is required")
    if not exc:
        exc = req
    values, err = _get_cached_ose_sheet_values()
    if not values:
        raise RuntimeError(err or "Could not load OSE shift sheet")
    orig_col = _date_column_for_matrix(values, original_date)
    exc_col = _date_column_for_matrix(values, exchange_date)
    if orig_col is None:
        raise ValueError(f"Could not find sheet column for original date {original_date.isoformat()}")
    if exc_col is None:
        raise ValueError(f"Could not find sheet column for exchange date {exchange_date.isoformat()}")
    req_row = _sheet_row_index_for_person(values, req)
    if req_row is None:
        raise ValueError(f"Could not find shift sheet row for request person {req!r}")
    same_person = _names_same_person(req, exc)
    updates: list[tuple[int, int, str]] = [
        (req_row, orig_col, "*"),
        (req_row, exc_col, st),
    ]
    if not same_person:
        exc_row = _sheet_row_index_for_person(values, exc)
        if exc_row is None:
            raise ValueError(f"Could not find shift sheet row for exchange person {exc!r}")
        updates.extend([(exc_row, orig_col, st), (exc_row, exc_col, "*")])
    token = get_tenant_access_token()
    _put_ose_shift_sheet_cells(token, updates)
    return {
        "ok": True,
        "request_person": req,
        "exchange_person": exc,
        "original_date": original_date.isoformat(),
        "exchange_date": exchange_date.isoformat(),
        "shift_type": st,
        "cells_updated": len(updates),
        "myself": same_person,
    }


def apply_approved_offset_shift_sheet_for_record(record_id: str) -> dict[str, Any]:
    """Load an approved offset row and apply duty-sheet swap (idempotent per record_id)."""
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    if offset_shift_sheet_already_applied(rid):
        return {"ok": True, "record_id": rid, "skipped": "already_applied"}
    row = get_ose_offset_record_admin_row(rid)
    if bool(row.get("pending")):
        return {"ok": False, "record_id": rid, "skipped": "still_pending"}
    status = str(row.get("approval_status") or "").strip().title()
    if status != "Approved":
        return {"ok": False, "record_id": rid, "skipped": f"status={status or 'unknown'}"}
    od = _parse_date_value(row.get("original_date"))
    xd = _parse_date_value(row.get("exchange_date"))
    if not od or not xd:
        raise ValueError("Original Date and Exchange Date are required on the offset row")
    result = apply_approved_offset_to_shift_sheet(
        request_person=str(row.get("request_person") or ""),
        exchange_person=str(row.get("exchange_person") or ""),
        original_date=od,
        exchange_date=xd,
        shift_type=str(row.get("shift_type") or ""),
    )
    _mark_offset_shift_sheet_applied(rid)
    return {"record_id": rid, **result}


def scan_bitable_approved_offsets_for_shift_sheet() -> dict[str, int]:
    """Apply duty-sheet swaps for offsets approved directly in Base (not via bot card)."""
    invalidate_ose_bitable_cache()
    items = (get_ose_offset_records_admin() or {}).get("items") or []
    applied = 0
    errors = 0
    for row in items:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("record_id") or "").strip()
        if not rid or bool(row.get("pending")):
            continue
        if str(row.get("approval_status") or "").strip().title() != "Approved":
            continue
        if offset_shift_sheet_already_applied(rid):
            continue
        try:
            apply_approved_offset_shift_sheet_for_record(rid)
            applied += 1
        except Exception as exc:
            errors += 1
            print(f"[ose_Duty] shift sheet apply failed for {rid!r}: {exc!r}", flush=True)
    return {"scanned": len(items), "applied": applied, "errors": errors}


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
        leave_items = _get_leave_display_raw(token)
        _, offset_items = _get_bitable_raw_pair(token)
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
        morning, night, roster_leave = _shift_codes_from_matrix(values, d)
        leave_entries: list[dict[str, Any]] = []
        offset_lines: list[str] = []
        if leave_items and token:
            try:
                leave_entries = _extract_leave_entries_for_date(
                    d, token, items=leave_items, require_approved=False
                )
            except Exception:
                leave_entries = []
        leave_entries = _merge_roster_sheet_leave(leave_entries, roster_leave, d)
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
_OSE_BITABLE_RAW: dict[str, Any] = {
    "monotonic": 0.0,
    "leave_display": None,
    "leave_approval": None,
    "offset": None,
    "person_ids": None,
    "offset_person_options": None,
}
_OSE_BITABLE_TTL_SEC = int(os.getenv("OSE_BITABLE_CACHE_SEC", "120"))


def invalidate_ose_bitable_cache() -> None:
    """Clear cached leave/offset rows (e.g. after daily sync job)."""
    _OSE_BITABLE_RAW["leave_display"] = None
    _OSE_BITABLE_RAW["leave_approval"] = None
    _OSE_BITABLE_RAW["offset"] = None
    _OSE_BITABLE_RAW["person_ids"] = None
    _OSE_BITABLE_RAW["offset_person_options"] = None
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
        leave_disp, leave_appr, offset = _get_bitable_raw_triple(token)
    except Exception as e:
        return f"❌ OSE Bitable sync: {e}"
    return (
        f"✅ OSE Bitable synced ({len(leave_disp)} HRMS leave, "
        f"{len(leave_appr)} approval leave, {len(offset)} offset rows)"
    )


def _leaveose_table_id_for_display() -> str:
    """Always leaveose (``tblvoXE0hsPjgb0j``). Ignores mis-set ``OSE_HRMS_LEAVE_TABLE_ID`` / ``TRACK_LEAVE_TABLE_ID``."""
    configured = (os.getenv("OSE_HRMS_LEAVE_TABLE_ID") or os.getenv("TRACK_LEAVE_TABLE_ID") or "").strip()
    if configured and configured not in (LEAVEOSE_TABLE_ID_CANONICAL, ""):
        print(
            f"[ose_Duty] WARNING: OSE_HRMS_LEAVE_TABLE_ID={configured!r} ignored; "
            f"display reads leaveose {LEAVEOSE_TABLE_ID_CANONICAL!r} only",
            flush=True,
        )
    return LEAVEOSE_TABLE_ID_CANONICAL


def _fetch_leaveose_bitable_records(token: str) -> list[dict[str, Any]]:
    """Read **leaveose** sheet only. Never ``OSE_ALL_LEAVE_TABLE_ID`` (leave 全员)."""
    table_id = _leaveose_table_id_for_display()
    if table_id == OSE_ALL_LEAVE_TABLE_ID:
        raise RuntimeError(
            "leaveose table id must differ from leave 全员 table; "
            f"got {table_id!r} — check OSE_ALL_LEAVE_TABLE_ID / OSE_LEAVE_TABLE_ID in .env"
        )
    return _bitable_get_all_records(token, OSE_BASE_TOKEN, table_id)


def _get_leave_display_raw(token: str) -> list[dict[str, Any]]:
    """HRMS-synced OSE leave rows (leaveose sheet) for webapp, Admin ALL, duty calendar."""
    now = time.monotonic()
    cached = _OSE_BITABLE_RAW.get("leave_display")
    ts = float(_OSE_BITABLE_RAW.get("monotonic") or 0)
    if _OSE_BITABLE_TTL_SEC > 0 and isinstance(cached, list) and now - ts < _OSE_BITABLE_TTL_SEC:
        return cached
    items = _fetch_leaveose_bitable_records(token)
    _OSE_BITABLE_RAW["leave_display"] = items
    _OSE_BITABLE_RAW["monotonic"] = now
    return items


def _get_leave_approval_raw(token: str) -> list[dict[str, Any]]:
    """OSE leave request / approval workflow table."""
    now = time.monotonic()
    cached = _OSE_BITABLE_RAW.get("leave_approval")
    ts = float(_OSE_BITABLE_RAW.get("monotonic") or 0)
    if _OSE_BITABLE_TTL_SEC > 0 and isinstance(cached, list) and now - ts < _OSE_BITABLE_TTL_SEC:
        return cached
    items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_LEAVE_TABLE_ID)
    _OSE_BITABLE_RAW["leave_approval"] = items
    _OSE_BITABLE_RAW["monotonic"] = now
    return items


def _get_offset_raw(token: str) -> list[dict[str, Any]]:
    now = time.monotonic()
    cached = _OSE_BITABLE_RAW.get("offset")
    ts = float(_OSE_BITABLE_RAW.get("monotonic") or 0)
    if _OSE_BITABLE_TTL_SEC > 0 and isinstance(cached, list) and now - ts < _OSE_BITABLE_TTL_SEC:
        return cached
    items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_OFFSET_TABLE_ID)
    _OSE_BITABLE_RAW["offset"] = items
    _OSE_BITABLE_RAW["monotonic"] = now
    return items


def _get_bitable_raw_pair(token: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(HRMS leave display, offset) — backward-compatible pair for calendar/bot."""
    leave_disp, _, offset = _get_bitable_raw_triple(token)
    return leave_disp, offset


def _get_bitable_raw_triple(token: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    now = time.monotonic()
    leave_disp = _OSE_BITABLE_RAW.get("leave_display")
    leave_appr = _OSE_BITABLE_RAW.get("leave_approval")
    offset = _OSE_BITABLE_RAW.get("offset")
    ts = float(_OSE_BITABLE_RAW.get("monotonic") or 0)
    fresh = (
        _OSE_BITABLE_TTL_SEC > 0
        and isinstance(leave_disp, list)
        and isinstance(leave_appr, list)
        and isinstance(offset, list)
        and now - ts < _OSE_BITABLE_TTL_SEC
    )
    if fresh:
        return leave_disp, leave_appr, offset
    leave_disp = _fetch_leaveose_bitable_records(token)
    leave_appr = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_LEAVE_TABLE_ID)
    offset = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_OFFSET_TABLE_ID)
    _OSE_BITABLE_RAW["monotonic"] = now
    _OSE_BITABLE_RAW["leave_display"] = leave_disp
    _OSE_BITABLE_RAW["leave_approval"] = leave_appr
    _OSE_BITABLE_RAW["offset"] = offset
    return leave_disp, leave_appr, offset


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


def _is_ose_dutylist_leave_name(name: str) -> bool:
    """OSE = dutyList.csv department OSE / OSE Senior / Team Lead / Manager (not hardcoded roster)."""
    return dlm.is_ose_dutylist_name(name)


def _extract_leave_entries_for_date(
    target_date: date,
    token: str,
    *,
    items: Optional[list[dict[str, Any]]] = None,
    require_approved: bool = True,
) -> list[dict[str, Any]]:
    if items is None:
        items = _get_leave_display_raw(token)
    out: list[dict[str, Any]] = []
    for it in items:
        f = it.get("fields") or {}
        if require_approved:
            status_v = _get_field_by_aliases(f, ["Status", "Approval Status"])
            if not _is_approved(status_v):
                continue
        name = _title_name(_field_text(_get_field_by_aliases(f, ["Name", "Employee Name", "Person"])))
        if not name or not _is_ose_dutylist_leave_name(name):
            continue
        entry = dlm.match_duty_entry(name)
        display_name = entry["name"] if entry else name
        st = _parse_date_value(_get_field_by_aliases(f, ["Start Date", "Leave Start Date", "From"]))
        ed = _parse_date_value(_get_field_by_aliases(f, ["End Date", "Leave End Date", "To"]))
        if not st or not ed:
            continue
        if st <= target_date <= ed:
            out.append(
                {
                    "name": display_name,
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
        leave_entries = _extract_leave_entries_for_date(
            target_date, token, items=leave_items, require_approved=False
        )
        offset_lines = _extract_offset_lines_for_date(target_date, token, items=offset_items)
        values, _sheet_err = _get_cached_ose_sheet_values()
        if values:
            _, _, roster_leave = _shift_codes_from_matrix(values, target_date)
            leave_entries = _merge_roster_sheet_leave(leave_entries, roster_leave, target_date)
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
    first_section_title: str = "🌅 Morning shift",
    second_section_title: str = "🌙 Night Shift",
    dutylist_attendance: Optional[dict[str, Any]] = None,
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
    body_elements: list[dict[str, Any]] = [
        {
            "tag": "div",
            "text": {"tag": "lark_md", "content": "\n".join(lines).strip()},
        }
    ]
    if dutylist_attendance:
        try:
            import leavewfh as lw

            lw_sections = lw.dutylist_attendance_plain_sections(
                dutylist_attendance, target_date
            )
            if lw_sections:
                body_elements.append({"tag": "hr"})
                for _title, section_els in lw_sections:
                    body_elements.extend(section_els)
        except Exception:
            pass

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
        "body": {"elements": body_elements},
    }


def get_ose_payload_for_date(target_date: date, mode: str = "date", *, include_tag: bool = False) -> dict[str, Any]:
    rest_names, luck_names, offset_lines, leave_entries, err = _build_ose_context(target_date, mode)
    if err:
        return {"text": err, "lark_card": None}

    dutylist_attendance: dict[str, Any] = {}
    try:
        import leavewfh as lw

        dutylist_attendance = lw.get_dutylist_leave_wfh_for_date(target_date)
    except Exception:
        dutylist_attendance = {}

    if mode == "date":
        first_title_plain = "Morning shift"
        second_title_plain = "Night Shift"
        first_title_card = "🌅 Morning shift"
        second_title_card = "🌙 Night Shift"
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
    if dutylist_attendance:
        try:
            import leavewfh as lw

            lines.append("")
            lines.append("Leave & WFH")
            lines.extend(lw.format_dutylist_leave_wfh_display(dutylist_attendance, target_date))
        except Exception:
            pass

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
            dutylist_attendance=dutylist_attendance,
        ),
    }


def get_ose_payload_for_now(now_dt: Optional[datetime] = None, *, include_tag: bool = False) -> dict[str, Any]:
    now_dt = now_dt or datetime.now()
    return get_ose_payload_for_date(now_dt.date(), mode="date", include_tag=include_tag)


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

# Special Exchange person choice on offset forms (= same person as Request Person).
OFFSET_EXCHANGE_MYSELF_LABEL = "Myself"

# Excluded from offset form "Exchange person" dropdown only (leave form & OTE duty unchanged).
OSE_OFFSET_FORM_EXCHANGE_EXCLUDED: frozenset[str] = frozenset(
    _title_name(n)
    for n in (
        "Renzel",
        "Faye",
        "Shie Ni",
        "Jeno",
        "Kwang Ming",
    )
)


def ose_offset_form_exchange_names(*, exclude_person: str = "") -> tuple[str, ...]:
    """Roster names allowed as Exchange person on the Lark offset request/edit form."""
    skip = _title_name(exclude_person) if (exclude_person or "").strip() else ""
    names = tuple(
        n
        for n in OSE_LEAVE_FORM_NAMES
        if _title_name(n) not in OSE_OFFSET_FORM_EXCHANGE_EXCLUDED
        and (not skip or _title_name(n) != skip)
    )
    return (OFFSET_EXCHANGE_MYSELF_LABEL,) + names


def _is_offset_exchange_myself_label(name: str) -> bool:
    return (name or "").strip().casefold() == OFFSET_EXCHANGE_MYSELF_LABEL.casefold()


def resolve_offset_exchange_person(exchange_person: str, *, request_person: str) -> str:
    """Map form value ``Myself`` to the requester's roster name."""
    if _is_offset_exchange_myself_label(exchange_person):
        req = _title_name(request_person)
        if not req:
            raise ValueError("Request person is required when Exchange person is Myself")
        return req
    return _validate_offset_exchange_person(exchange_person)


def _validate_offset_exchange_person(exchange_person: str) -> str:
    exc = _title_name(exchange_person)
    allowed = {
        _title_name(n)
        for n in ose_offset_form_exchange_names()
        if not _is_offset_exchange_myself_label(n)
    }
    if exc not in allowed:
        raise ValueError(f"Unknown or disallowed exchange person {exchange_person!r}")
    return exc


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


def _schedule_offset_duty_wiki_sync(*, record_id: str = "", delete: bool = False, full: bool = False) -> None:
    try:
        import offsetleave as ol

        ol.schedule_offset_duty_wiki_sync(record_id=record_id, delete=delete, full=full)
    except Exception as exc:
        print(f"[ose_Duty] duty wiki offset sync schedule failed: {exc!r}", flush=True)


def _bitable_create_record(token: str, table_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    url = (
        f"https://open.larksuite.com/open-apis/bitable/v1/apps/"
        f"{OSE_BASE_TOKEN}/tables/{table_id}/records"
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
    return res


def _bitable_update_record(
    token: str,
    table_id: str,
    record_id: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    url = (
        f"https://open.larksuite.com/open-apis/bitable/v1/apps/"
        f"{OSE_BASE_TOKEN}/tables/{table_id}/records/{rid}"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    res = requests.put(
        url,
        headers=headers,
        params={"user_id_type": "open_id"},
        json={"fields": fields},
        timeout=30,
    ).json()
    if res.get("code") != 0:
        raise RuntimeError(f"Bitable update failed: {res}")
    return res


def _is_pending_approval(v: Any) -> bool:
    s = _field_text(v).strip().lower()
    return s in ("", "pending")


def _person_item_open_id(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    return str(item.get("id") or item.get("open_id") or item.get("user_id") or "").strip()


def _person_field_items(v: Any) -> list[dict[str, Any]]:
    if isinstance(v, list):
        return [it for it in v if isinstance(it, dict)]
    if isinstance(v, dict):
        return [v]
    return []


def _index_person_field_value(v: Any, idx: dict[str, str]) -> None:
    for item in _person_field_items(v):
        pid = _person_item_open_id(item)
        if not pid:
            continue
        for raw_name in (
            str(item.get("name") or "").strip(),
            str(item.get("en_name") or "").strip(),
        ):
            if not raw_name:
                continue
            nm = _title_name(raw_name)
            idx[nm] = pid
            nk = _name_key(nm)
            if nk:
                idx[nk] = pid
            for roster in OSE_LEAVE_FORM_NAMES:
                if _names_same_person(roster, raw_name):
                    roster_nm = _title_name(roster)
                    idx[roster_nm] = pid
                    roster_nk = _name_key(roster_nm)
                    if roster_nk:
                        idx[roster_nk] = pid


def _build_ose_person_open_id_index(
    leave_items: list[dict[str, Any]],
    offset_items: list[dict[str, Any]],
) -> dict[str, str]:
    idx: dict[str, str] = {}
    for it in leave_items:
        f = it.get("fields") or {}
        _index_person_field_value(_get_field_by_aliases(f, ["Name", "Employee Name", "Person"]), idx)
        _index_person_field_value(
            _get_field_by_aliases(f, ["Approver", "Approved By", "Approval Person"]),
            idx,
        )
    for it in offset_items:
        f = it.get("fields") or {}
        _index_person_field_value(
            _get_field_by_aliases(f, ["Approver", "Approved By", "Approval Person"]),
            idx,
        )
        _index_person_field_value(
            _get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]),
            idx,
        )
        _index_person_field_value(
            _get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]),
            idx,
        )
    return idx


def _get_ose_person_open_id_index(token: str) -> dict[str, str]:
    cached = _OSE_BITABLE_RAW.get("person_ids")
    if isinstance(cached, dict):
        return cached
    leave_disp, leave_appr, offset = _get_bitable_raw_triple(token)
    idx = _build_ose_person_open_id_index(leave_disp + leave_appr, offset)
    _OSE_BITABLE_RAW["person_ids"] = idx
    return idx


def _lookup_person_open_id(name: str, idx: dict[str, str]) -> str:
    nm = _title_name(name)
    if not nm:
        return ""
    direct = idx.get(nm) or idx.get(_name_key(nm))
    if direct:
        return direct
    for key, pid in idx.items():
        if key.startswith("ou_"):
            continue
        if _names_same_person(nm, key):
            return pid
    return ""


def lookup_roster_name_for_open_id(open_id: str, token: str) -> str:
    """
  Map Lark ``open_id`` → OSE roster name using leave/offset Bitable person fields
  (built from duty roster records), not the user's Lark display name.
  """
    oid = (open_id or "").strip()
    if not oid:
        return ""
    idx = _get_ose_person_open_id_index(token)
    for roster in OSE_LEAVE_FORM_NAMES:
        rnm = _title_name(roster)
        if idx.get(rnm) == oid or idx.get(_name_key(rnm)) == oid:
            return rnm
    for key, pid in idx.items():
        if pid != oid or key.startswith("ou_"):
            continue
        for roster in OSE_LEAVE_FORM_NAMES:
            if _names_same_person(roster, key):
                return _title_name(roster)
    return ""


def _person_field_value(name: str, *, token: str) -> list[dict[str, str]]:
    nm = _title_name(name)
    open_id = _lookup_person_open_id(nm, _get_ose_person_open_id_index(token))
    if not open_id:
        raise ValueError(
            f"Could not resolve Lark user id for {name!r}. "
            "Pick a name that already appears in leave/offset records."
        )
    return [{"id": open_id}]


def _approver_field_value(
    approver: str,
    *,
    token: str,
    approver_open_id: str = "",
) -> list[dict[str, str]]:
    pid = (approver_open_id or "").strip()
    if pid:
        return [{"id": pid}]
    return _person_field_value(approver, token=token)


def _index_offset_person_option_value(v: Any, idx: dict[str, str]) -> None:
    if not isinstance(v, str):
        return
    opt = v.strip()
    if not opt:
        return
    nm = _title_name(opt)
    idx[nm] = opt
    nk = _name_key(nm)
    if nk:
        idx[nk] = opt
    for roster in OSE_LEAVE_FORM_NAMES:
        if _names_same_person(roster, opt):
            roster_nm = _title_name(roster)
            idx[roster_nm] = opt
            roster_nk = _name_key(roster_nm)
            if roster_nk:
                idx[roster_nk] = opt


def _build_ose_offset_person_option_index(offset_items: list[dict[str, Any]]) -> dict[str, str]:
    idx: dict[str, str] = {}
    for it in offset_items:
        f = it.get("fields") or {}
        _index_offset_person_option_value(
            _get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]),
            idx,
        )
        _index_offset_person_option_value(
            _get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]),
            idx,
        )
    return idx


def _get_ose_offset_person_option_index(token: str) -> dict[str, str]:
    cached = _OSE_BITABLE_RAW.get("offset_person_options")
    if isinstance(cached, dict):
        return cached
    _, offset = _get_bitable_raw_pair(token)
    idx = _build_ose_offset_person_option_index(offset)
    _OSE_BITABLE_RAW["offset_person_options"] = idx
    return idx


def _lookup_offset_person_option(name: str, idx: dict[str, str]) -> str:
    nm = _title_name(name)
    if not nm:
        return ""
    direct = idx.get(nm) or idx.get(_name_key(nm))
    if direct:
        return direct
    for key, opt in idx.items():
        if _names_same_person(nm, key):
            return opt
    return ""


def _offset_person_field_value(name: str, *, token: str) -> str:
    nm = _title_name(name)
    opt = _lookup_offset_person_option(nm, _get_ose_offset_person_option_index(token))
    if opt:
        return opt
    return nm


def get_ose_submit_form_options() -> dict[str, Any]:
    return {
        "leave_names": list(OSE_LEAVE_FORM_NAMES),
        "offset_names": list(OSE_LEAVE_FORM_NAMES),
        "offset_exchange_names": list(ose_offset_form_exchange_names()),
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
        if not name or not _is_ose_dutylist_leave_name(name):
            continue
        entry = dlm.match_duty_entry(name)
        canon = entry["name"] if entry else name
        st = _parse_date_value(_get_field_by_aliases(f, ["Start Date", "Leave Start Date", "From"]))
        ed = _parse_date_value(_get_field_by_aliases(f, ["End Date", "Leave End Date", "To"]))
        if not st or not ed:
            continue
        rows.append({"name": canon, "start": st, "end": ed})
    return rows


def _record_approval_fields(fields: dict[str, Any]) -> dict[str, str]:
    approval_dt = _parse_date_value(
        _get_field_by_aliases(fields, ["Approval Date", "Approved Date", "ApprovalDate"])
    )
    return {
        "status": _field_text(_get_field_by_aliases(fields, ["Status", "Approval Status"])),
        "approver": _title_name(
            _field_text(_get_field_by_aliases(fields, ["Approver", "Approved By", "Approval Person"]))
        ),
        "approval_date": _format_yyyymmdd(approval_dt),
        "remarks": _field_text(_get_field_by_aliases(fields, ["Remarks", "Remark", "Approval Remarks"])),
    }


def _leave_row_key_for_admin(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        (row.get("name") or "").strip().lower(),
        row.get("start_date") or "",
        row.get("end_date") or "",
        (row.get("leave_type") or "").strip().lower(),
    )


def _admin_leave_row_from_bitable_item(it: dict[str, Any]) -> Optional[dict[str, str]]:
    f = it.get("fields") or {}
    name = _title_name(_field_text(_get_field_by_aliases(f, ["Name", "Employee Name", "Person"])))
    if not name:
        return None
    entry = dlm.match_duty_entry(name)
    if entry:
        name = entry["name"]
    st = _parse_date_value(_get_field_by_aliases(f, ["Start Date", "Leave Start Date", "From"]))
    ed = _parse_date_value(_get_field_by_aliases(f, ["End Date", "Leave End Date", "To"]))
    approval = _record_approval_fields(f)
    return {
        "record_id": str(it.get("record_id") or "").strip(),
        "leave_id": _field_text(_get_field_by_aliases(f, ["LeaveID", "Leave ID", "Leave Id"])),
        "name": name,
        "leave_type": _field_text(_get_field_by_aliases(f, ["Leave Type", "Type"])),
        "start_date": _format_yyyymmdd(st),
        "end_date": _format_yyyymmdd(ed),
        "reason": _field_text(_get_field_by_aliases(f, ["Reason"])),
        "status": approval["status"],
        "approver": approval["approver"],
        "approval_date": approval["approval_date"],
        "remarks": approval["remarks"],
    }


def get_ose_leave_bitable_meta(*, scope: str = "") -> dict[str, Any]:
    """Which Lark tables the app uses (for debugging Admin / webapp data source)."""
    return {
        "api_build": OSE_LEAVE_API_BUILD,
        "scope": scope or "display",
        "leaveose_table_id": LEAVEOSE_TABLE_ID_CANONICAL,
        "leaveose_table_id_enforced": True,
        "leaveose_only_display": True,
        "env_ose_hrms_leave_table_id": OSE_HRMS_LEAVE_TABLE_ID,
        "ose_hrms_leave_table_id": LEAVEOSE_TABLE_ID_CANONICAL,
        "ose_all_leave_table_id": OSE_ALL_LEAVE_TABLE_ID,
        "ose_leave_approval_table_id": OSE_LEAVE_TABLE_ID,
        "base_token": OSE_BASE_TOKEN,
        "ose_hrms_url": (
            f"https://casinoplus.sg.larksuite.com/base/{OSE_BASE_TOKEN}"
            f"?table={LEAVEOSE_TABLE_ID_CANONICAL}"
        ),
    }


def get_ose_leave_records_list() -> dict[str, Any]:
    """HRMS-synced OSE leave rows for webapp display (read-only; no approval workflow)."""
    token = get_tenant_access_token()
    rows: list[dict[str, str]] = []
    for it in _get_leave_display_raw(token):
        row = _admin_leave_row_from_bitable_item(it)
        if not row or not _is_ose_dutylist_leave_name(row["name"]):
            continue
        rows.append({k: v for k, v in row.items() if k != "record_id"})
    rows.sort(
        key=lambda r: (
            r.get("start_date") or "",
            r.get("end_date") or "",
            r.get("name") or "",
        ),
        reverse=True,
    )
    return {
        "ok": True,
        "items": rows,
        "meta": {
            **get_ose_leave_bitable_meta(),
            "source": "ose_hrms_leave_table",
        },
    }


def get_ose_leave_records_admin(*, scope: str = "display") -> dict[str, Any]:
    """
    Admin leave list (``dutyList.csv`` OSE only).

    ``scope=display`` (Admin ALL): **leaveose only** — ``OSE_HRMS_LEAVE_TABLE_ID`` / tblvoXE0hsPjgb0j.
    Does **not** read leave 全员 (``OSE_ALL_LEAVE_TABLE_ID`` / tblmHJHe12BCJRD8).

    ``scope=pending`` (Admin TODO): pending OSE form rows from ``OSE_LEAVE_TABLE_ID`` only.
    """
    scope_s = (scope or "display").strip().lower()
    if scope_s in ("merged", "all", "approval", "leave"):
        scope_s = "display"
    if scope_s not in ("display", "pending"):
        scope_s = "display"
    token = get_tenant_access_token()
    merged: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    display_items: list[dict[str, Any]] = []
    approval_items: list[dict[str, Any]] = []

    if scope_s == "display":
        display_items = _get_leave_display_raw(token)
        for it in display_items:
            row = _admin_leave_row_from_bitable_item(it)
            if not row or not _is_ose_dutylist_leave_name(row["name"]):
                continue
            row["pending"] = False
            merged[_leave_row_key_for_admin(row)] = row

    if scope_s == "pending":
        approval_items = _get_leave_approval_raw(token)
        for it in approval_items:
            row = _admin_leave_row_from_bitable_item(it)
            if not row or not _is_ose_dutylist_leave_name(row["name"]):
                continue
            pending = _is_pending_approval(
                _get_field_by_aliases(it.get("fields") or {}, ["Status", "Approval Status"])
            )
            reason = (row.get("reason") or "").strip()
            leave_id = (row.get("leave_id") or "").strip()
            if scope_s == "pending":
                if not pending:
                    continue
            elif not pending and not reason and not leave_id:
                continue
            row["pending"] = pending
            merged[_leave_row_key_for_admin(row)] = row

    rows = list(merged.values())
    rows.sort(
        key=lambda r: (
            r.get("start_date") or "",
            r.get("end_date") or "",
            r.get("name") or "",
        ),
        reverse=True,
    )
    source = {
        "display": "leaveose_table_only",
        "pending": "ose_leave_approval_pending_only",
    }[scope_s]
    return {
        "ok": True,
        "items": rows,
        "meta": {
            **get_ose_leave_bitable_meta(scope=scope_s),
            "display_table_raw_rows": len(display_items),
            "approval_table_raw_rows": len(approval_items),
            "items_after_ose_dutylist_filter": len(rows),
            "source": source,
        },
    }


def get_ose_offset_records_admin() -> dict[str, Any]:
    """Offset rows for admin approval (includes Bitable record_id)."""
    token = get_tenant_access_token()
    _, items = _get_bitable_raw_pair(token)
    rows: list[dict[str, str]] = []
    for it in items:
        f = it.get("fields") or {}
        req = _title_name(
            _field_text(_get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]))
        )
        exc = _title_name(
            _field_text(_get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]))
        )
        od = _parse_date_value(_get_field_by_aliases(f, ["Original Date", "Date"]))
        xd = _parse_date_value(_get_field_by_aliases(f, ["Exchange Date", "Swap Date", "Target Date"]))
        rd = _parse_date_value(_get_field_by_aliases(f, ["Request Date", "Submitted Date", "Created Date"]))
        approval = _record_approval_fields(f)
        rows.append(
            {
                "record_id": str(it.get("record_id") or "").strip(),
                "request_id": _field_text(_get_field_by_aliases(f, ["Request ID", "RequestID", "Request Id"])),
                "request_date": _format_yyyymmdd(rd),
                "request_person": req,
                "exchange_person": exc,
                "shift_type": _field_text(_get_field_by_aliases(f, ["Shift Type", "Shift"])).upper(),
                "original_date": _format_yyyymmdd(od),
                "exchange_date": _format_yyyymmdd(xd),
                "reason": _field_text(_get_field_by_aliases(f, ["Reason"])),
                "approval_status": approval["status"],
                "approver": approval["approver"],
                "approval_date": approval["approval_date"],
                "remarks": approval["remarks"],
                "pending": _is_pending_approval(_get_field_by_aliases(f, ["Approval Status", "Status"])),
            }
        )
    rows.sort(
        key=lambda r: (
            r.get("request_date") or "",
            r.get("original_date") or "",
            r.get("request_person") or "",
        ),
        reverse=True,
    )
    return {"ok": True, "items": rows}


def update_ose_leave_approval(
    *,
    record_id: str,
    status: str,
    approver: str,
    remarks: str = "",
    approval_date: Optional[date] = None,
    approver_open_id: str = "",
) -> dict[str, Any]:
    st = (status or "").strip().title()
    if st not in ("Approved", "Rejected"):
        raise ValueError("status must be Approved or Rejected")
    approver_s = _title_name(approver)
    if not approver_s:
        raise ValueError("approver is required")
    token = get_tenant_access_token()
    fields: dict[str, Any] = {
        "Status": st,
        "Approver": _approver_field_value(
            approver_s,
            token=token,
            approver_open_id=approver_open_id,
        ),
        "Approval Date": _bitable_date_ms(approval_date or date.today()),
        "Remarks": (remarks or "").strip(),
    }
    _bitable_update_record(token, OSE_LEAVE_TABLE_ID, record_id, fields)
    invalidate_ose_bitable_cache()
    return {"ok": True, "record_id": record_id, "status": st}


def update_ose_offset_approval(
    *,
    record_id: str,
    status: str,
    approver: str,
    remarks: str = "",
    approval_date: Optional[date] = None,
    approver_open_id: str = "",
) -> dict[str, Any]:
    st = (status or "").strip().title()
    if st not in ("Approved", "Rejected"):
        raise ValueError("status must be Approved or Rejected")
    approver_s = _title_name(approver)
    if not approver_s:
        raise ValueError("approver is required")
    token = get_tenant_access_token()
    fields: dict[str, Any] = {
        "Approval Status": st,
        "Approver": _approver_field_value(
            approver_s,
            token=token,
            approver_open_id=approver_open_id,
        ),
        "Approval Date": _bitable_date_ms(approval_date or date.today()),
        "Remarks": (remarks or "").strip(),
    }
    _bitable_update_record(token, OSE_OFFSET_TABLE_ID, record_id, fields)
    invalidate_ose_bitable_cache()
    _schedule_offset_duty_wiki_sync(record_id=record_id)
    sheet_out: dict[str, Any] = {}
    if st == "Approved":
        try:
            sheet_out = apply_approved_offset_shift_sheet_for_record(record_id)
        except Exception as exc:
            sheet_out = {"ok": False, "error": str(exc)}
            print(f"[ose_Duty] offset shift sheet apply failed for {record_id!r}: {exc!r}", flush=True)
    return {"ok": True, "record_id": record_id, "status": st, "shift_sheet": sheet_out}


def _admin_page_env() -> tuple[str, str]:
    app_token = (
        os.getenv("APPWEB_ADMINPAGETOKEN")
        or os.getenv("appweb_adminpagetoken")
        or ""
    ).strip()
    table_id = (
        os.getenv("APPWEB_ADMINPAGESHEETID")
        or os.getenv("appweb_adminpagesheetid")
        or ""
    ).strip()
    if not app_token or not table_id:
        raise RuntimeError("Admin page Bitable env is not configured")
    return app_token, table_id


def _admin_whologin_identity(v: Any) -> tuple[str, str]:
    for item in _person_field_items(v):
        pid = _person_item_open_id(item)
        if pid:
            return _title_name(_field_text(item)) or pid, pid
    return _title_name(_field_text(v)), ""


def _parse_webapp_admin_credential_row(record: dict[str, Any]) -> tuple[str, str, str, str] | None:
    f = record.get("fields") or {}
    who, open_id = _admin_whologin_identity(
        _get_field_by_aliases(f, ["whologin", "Who Login", "WhoLogin"])
    )
    login = _field_text(_get_field_by_aliases(f, ["ID", "Id"]))
    pw = _field_text(_get_field_by_aliases(f, ["PASSWORD", "Password"]))
    if not login or not pw:
        return None
    return who, login, pw, open_id


def _load_webapp_admin_credentials() -> list[tuple[str, str, str, str]]:
    app_token, table_id = _admin_page_env()
    token = get_tenant_access_token()
    records = _bitable_get_all_records(token, app_token, table_id)
    if not records:
        raise RuntimeError("Admin credential table has no rows")
    rows: list[tuple[str, str, str, str]] = []
    for record in records:
        parsed = _parse_webapp_admin_credential_row(record)
        if parsed:
            rows.append(parsed)
    if not rows:
        raise RuntimeError("Admin credential table has no valid ID/PASSWORD rows")
    return rows


def verify_webapp_admin_login(login_id: str, password: str) -> dict[str, str]:
    login_s = (login_id or "").strip()
    password_s = password or ""
    for who, expected_id, expected_pw, open_id in _load_webapp_admin_credentials():
        if login_s == expected_id and password_s == expected_pw:
            return {"who": who or login_s, "open_id": open_id}
    raise ValueError("Invalid admin ID or password")


def get_ose_offset_records_list() -> dict[str, Any]:
    """All offset rows for the submit-offset page (Request ID shown when present)."""
    token = get_tenant_access_token()
    _, items = _get_bitable_raw_pair(token)
    rows: list[dict[str, str]] = []
    for it in items:
        f = it.get("fields") or {}
        req = _title_name(
            _field_text(_get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]))
        )
        exc = _title_name(
            _field_text(_get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]))
        )
        od = _parse_date_value(_get_field_by_aliases(f, ["Original Date", "Date"]))
        xd = _parse_date_value(_get_field_by_aliases(f, ["Exchange Date", "Swap Date", "Target Date"]))
        rd = _parse_date_value(_get_field_by_aliases(f, ["Request Date", "Submitted Date", "Created Date"]))
        approval = _record_approval_fields(f)
        rows.append(
            {
                "request_id": _field_text(_get_field_by_aliases(f, ["Request ID", "RequestID", "Request Id"])),
                "request_date": _format_yyyymmdd(rd),
                "request_person": req,
                "exchange_person": exc,
                "shift_type": _field_text(_get_field_by_aliases(f, ["Shift Type", "Shift"])).upper(),
                "original_date": _format_yyyymmdd(od),
                "exchange_date": _format_yyyymmdd(xd),
                "reason": _field_text(_get_field_by_aliases(f, ["Reason"])),
                "approval_status": approval["status"],
                "approver": approval["approver"],
                "approval_date": approval["approval_date"],
                "remarks": approval["remarks"],
            }
        )
    rows.sort(
        key=lambda r: (
            r.get("request_date") or "",
            r.get("original_date") or "",
            r.get("request_person") or "",
        ),
        reverse=True,
    )
    return {"ok": True, "items": rows}


def parse_showoffset_command(text: str) -> Optional[tuple[int, int]]:
    """Return ``(year, month)`` for ``showoffset`` / ``showoffset may`` / ``showoffset 5``."""
    s = (text or "").strip()
    m = re.match(r"^showoffset(?:\s+(.+))?\s*$", s, re.I)
    if not m:
        return None
    today = date.today()
    arg = (m.group(1) or "").strip()
    if not arg:
        return today.year, today.month
    if re.fullmatch(r"\d{1,2}", arg):
        month = int(arg)
        if month < 1 or month > 12:
            raise ValueError("month must be 1–12")
        return today.year, month
    for name, num in MONTH_MAP.items():
        if name.lower() == arg.lower():
            return today.year, num
    raise ValueError(f"Unknown month {arg!r}. Use a month name or number (1–12).")


def _showoffset_canonical_name(name: str) -> Optional[str]:
    nm = _title_name(name)
    if not nm:
        return None
    for allowed in OSE_SHOWOFFSET_NAMES:
        if _names_same_person(allowed, nm):
            return _title_name(allowed)
    return None


def _add_showoffset_days(
    by_person: dict[str, dict[str, set[int]]],
    person: str,
    orig_day: int,
    exc_day: int,
) -> None:
    slot = by_person.setdefault(person, {"orig": set(), "exc": set()})
    slot["orig"].add(orig_day)
    slot["exc"].add(exc_day)


def _collect_offset_month_summary(
    year: int,
    month: int,
    *,
    items: Optional[list[dict[str, Any]]] = None,
) -> dict[str, tuple[list[int], list[int]]]:
    if month < 1 or month > 12:
        raise ValueError("month must be 1–12")
    if items is None:
        token = get_tenant_access_token()
        _, items = _get_bitable_raw_pair(token)
    by_person: dict[str, dict[str, set[int]]] = {}
    for it in items:
        f = it.get("fields") or {}
        req = _title_name(
            _field_text(_get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]))
        )
        exc = _title_name(
            _field_text(_get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]))
        )
        od = _parse_date_value(_get_field_by_aliases(f, ["Original Date", "Date"]))
        xd = _parse_date_value(_get_field_by_aliases(f, ["Exchange Date", "Swap Date", "Target Date"]))
        if not od or not xd:
            continue
        if od.year != year or od.month != month:
            continue
        req_person = _showoffset_canonical_name(req)
        if req_person:
            _add_showoffset_days(by_person, req_person, od.day, xd.day)
        exc_person = _showoffset_canonical_name(exc)
        if exc_person:
            _add_showoffset_days(by_person, exc_person, xd.day, od.day)
    out: dict[str, tuple[list[int], list[int]]] = {}
    for person, days in by_person.items():
        out[person] = (sorted(days["orig"]), sorted(days["exc"]))
    return out


def build_ose_showoffset_card(year: int, month: int) -> dict[str, Any]:
    summary = _collect_offset_month_summary(year, month)
    month_label = date(year, month, 1).strftime("%B")
    lines = [f"**{month_label}**", ""]
    if not summary:
        lines.append("No offset requests this month.")
    else:
        for person in OSE_SHOWOFFSET_NAMES:
            if person not in summary:
                continue
            orig_days, exc_days = summary[person]
            orig_s = ", ".join(str(d) for d in orig_days)
            exc_s = ", ".join(str(d) for d in exc_days)
            lines.append(f"{person} {orig_s} --> {exc_s}")
    content = "\n".join(lines).strip()
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": f"OSE offset — {month_label} {year}"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": content},
                }
            ]
        },
    }


def get_ose_leave_names_calendar(year: int, month: int) -> dict[str, Any]:
    """Per-day leave names for a month (HRMS display table; OSE roster only)."""
    if month < 1 or month > 12:
        raise ValueError("month must be 1–12")
    token = get_tenant_access_token()
    items = _get_leave_display_raw(token)
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


def _parse_submit_day_month(*, month: Any, day: Any, year: Optional[int] = None) -> date:
    try:
        m = int(month)
        d = int(day)
    except (TypeError, ValueError) as e:
        raise ValueError("month and day are required") from e
    y = int(year) if year is not None else date.today().year
    if m < 1 or m > 12:
        raise ValueError("month must be 1–12")
    _, last = calendar.monthrange(y, m)
    if d < 1 or d > last:
        raise ValueError(f"invalid day {d} for month {m}/{y}")
    return date(y, m, d)


def _resolve_submit_date(*, month: Any, day: Any, raw_date: str = "", year: Optional[int] = None) -> date:
    m_s = "" if month is None else str(month).strip()
    d_s = "" if day is None else str(day).strip()
    if m_s and d_s:
        return _parse_submit_day_month(month=m_s, day=d_s, year=year)
    return _parse_iso_date(raw_date)


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
        "Name": _person_field_value(nm, token=token),
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
    if req not in OSE_LEAVE_FORM_NAMES:
        raise ValueError(f"Unknown request person {request_person!r}")
    exc = resolve_offset_exchange_person(exchange_person, request_person=req)
    st = (shift_type or "").strip().upper()
    if st not in OSE_SHIFT_TYPES:
        raise ValueError("Shift Type must be N or D")
    reason_s = (reason or "").strip()
    if not reason_s:
        raise ValueError("Reason is required")
    token = get_tenant_access_token()
    today = date.today()
    fields: dict[str, Any] = {
        "Request Person": _offset_person_field_value(req, token=token),
        "Exchange Person": _offset_person_field_value(exc, token=token),
        "Shift Type": st,
        "Original Date": _bitable_date_ms(original_date),
        "Exchange Date": _bitable_date_ms(exchange_date),
        "Request Date": _bitable_date_ms(today),
        "Reason": reason_s,
    }
    res = _bitable_create_record(token, OSE_OFFSET_TABLE_ID, fields)
    invalidate_ose_bitable_cache()
    record_id = (res.get("data") or {}).get("record", {}).get("record_id")
    rid = str(record_id or "").strip()
    if rid:
        _schedule_offset_duty_wiki_sync(record_id=rid)
        try:
            import offsetleave as ol

            ol.notify_offset_approvers_for_record(
                rid,
                fallback_row={
                    "record_id": rid,
                    "request_date": today.isoformat(),
                    "request_person": req,
                    "exchange_person": exc,
                    "shift_type": st,
                    "original_date": original_date.isoformat(),
                    "exchange_date": exchange_date.isoformat(),
                    "reason": reason_s,
                    "approval_status": "Pending",
                    "pending": True,
                },
            )
        except Exception as exc:
            print(f"[ose_Duty] offset approver notify failed: {exc!r}", flush=True)
    return {"ok": True, "record_id": record_id}


def get_ose_offset_record_admin_row(record_id: str) -> dict[str, Any]:
    """Single offset row (same shape as :func:`get_ose_offset_records_admin` items)."""
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    for r in get_ose_offset_records_admin().get("items") or []:
        if str(r.get("record_id") or "").strip() == rid:
            return dict(r)
    raise KeyError(f"unknown offset record {rid!r}")


def delete_ose_offset_record(*, record_id: str, skip_cache_invalidate: bool = False) -> dict[str, Any]:
    """Delete an offset Bitable row (caller must enforce pending + ownership)."""
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    token = get_tenant_access_token()
    url = (
        f"https://open.larksuite.com/open-apis/bitable/v1/apps/"
        f"{OSE_BASE_TOKEN}/tables/{OSE_OFFSET_TABLE_ID}/records/{rid}"
    )
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.delete(
        url,
        headers=headers,
        params={"user_id_type": "open_id"},
        timeout=30,
    ).json()
    if res.get("code") != 0:
        raise RuntimeError(f"Bitable delete failed: {res}")
    if not skip_cache_invalidate:
        invalidate_ose_bitable_cache()
    _schedule_offset_duty_wiki_sync(record_id=rid, delete=True)
    return {"ok": True, "record_id": rid}


def _calendar_months_after(d: date, ref: date) -> int:
    """
    Whole calendar months from ``d``'s month to ``ref``'s month (same month → 0).
    Examples: Mar→May=2, May→June=1, May→July=2.
    """
    return (ref.year - d.year) * 12 + (ref.month - d.month)


def _offset_row_original_exchange_dates(fields: dict[str, Any]) -> tuple[Optional[date], Optional[date]]:
    """
    Swap dates for retention / purge. Must match :func:`get_ose_offset_records_admin`
    (do **not** treat ``Request Date`` as Original Date — that is usually submission time
    and would skip purging old March swaps while the row still displays correctly).
    """
    f = fields or {}
    od = _parse_date_value(_get_field_by_aliases(f, ["Original Date", "Date"]))
    xd = _parse_date_value(_get_field_by_aliases(f, ["Exchange Date", "Swap Date", "Target Date"]))
    return od, xd


def purge_stale_ose_offset_bitable_rows(*, ref_date: Optional[date] = None) -> dict[str, Any]:
    """
    Delete offset Bitable rows by **calendar month**, not by day within the month.

    A row is removed only if **both** Original Date and Exchange Date fall in months
    that are **at least two whole calendar months** before ``ref_date``'s month
    (``ref``'s day-of-month is ignored).

    Examples: on **any day in May** (e.g. May 16), **all of March** (and earlier)
    qualifies — e.g. 26–30 Mar is deleted. On **any day in June**, May rows are
    **not** removed (only one month behind). On **any day in July**, May rows are
    removed.
    """
    ref = ref_date or date.today()
    token = get_tenant_access_token()
    items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_OFFSET_TABLE_ID)
    to_delete: list[str] = []
    for it in items:
        f = it.get("fields") or {}
        od, xd = _offset_row_original_exchange_dates(f)
        if not od or not xd:
            continue
        if _calendar_months_after(od, ref) >= 2 and _calendar_months_after(xd, ref) >= 2:
            rid = str(it.get("record_id") or "").strip()
            if rid:
                to_delete.append(rid)
    deleted: list[str] = []
    errors: list[str] = []
    for rid in to_delete:
        try:
            delete_ose_offset_record(record_id=rid, skip_cache_invalidate=True)
            deleted.append(rid)
        except Exception as exc:
            errors.append(f"{rid}: {exc}")
    if deleted or errors:
        invalidate_ose_bitable_cache()
    return {
        "ok": not errors,
        "ref_date": ref.isoformat(),
        "scanned": len(items),
        "eligible": len(to_delete),
        "deleted": len(deleted),
        "errors": errors,
    }


def update_ose_offset_request(
    *,
    record_id: str,
    request_person: str,
    exchange_person: str,
    shift_type: str,
    original_date: date,
    exchange_date: date,
    reason: str,
) -> dict[str, Any]:
    """Update a **pending** offset row (exchange/shift/dates/reason only)."""
    row = get_ose_offset_record_admin_row(record_id)
    if not row.get("pending"):
        raise ValueError("Only pending offset requests can be edited.")
    req = _title_name(request_person)
    if _title_name(str(row.get("request_person") or "")) != req:
        raise ValueError("This offset request does not belong to you.")
    if req not in OSE_LEAVE_FORM_NAMES:
        raise ValueError(f"Unknown request person {request_person!r}")
    exc = resolve_offset_exchange_person(exchange_person, request_person=req)
    st = (shift_type or "").strip().upper()
    if st not in OSE_SHIFT_TYPES:
        raise ValueError("Shift Type must be N or D")
    reason_s = (reason or "").strip()
    if not reason_s:
        raise ValueError("Reason is required")
    token = get_tenant_access_token()
    row_refresh = get_ose_offset_record_admin_row(record_id)
    if not bool(row_refresh.get("pending")):
        raise ValueError("Only pending offset requests can be edited (record may have been approved).")
    if _title_name(str(row_refresh.get("request_person") or "")) != req:
        raise ValueError("This offset request does not belong to you.")
    fields: dict[str, Any] = {
        "Exchange Person": _offset_person_field_value(exc, token=token),
        "Shift Type": st,
        "Original Date": _bitable_date_ms(original_date),
        "Exchange Date": _bitable_date_ms(exchange_date),
        "Reason": reason_s,
    }
    _bitable_update_record(token, OSE_OFFSET_TABLE_ID, record_id, fields)
    invalidate_ose_bitable_cache()
    _schedule_offset_duty_wiki_sync(record_id=record_id)
    return {"ok": True, "record_id": record_id}


def update_ose_offset_record_fields(
    *,
    record_id: str,
    exchange_person: str,
    shift_type: str,
    original_date: date,
    exchange_date: date,
    reason: str,
) -> dict[str, Any]:
    """Update offset swap fields for **any** status (caller must enforce approver-only / authorization)."""
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    row = get_ose_offset_record_admin_row(rid)
    req = _title_name(str(row.get("request_person") or ""))
    exc = resolve_offset_exchange_person(exchange_person, request_person=req)
    st = (shift_type or "").strip().upper()
    if st not in OSE_SHIFT_TYPES:
        raise ValueError("Shift Type must be N or D")
    reason_s = (reason or "").strip()
    if not reason_s:
        raise ValueError("Reason is required")
    token = get_tenant_access_token()
    fields: dict[str, Any] = {
        "Exchange Person": _offset_person_field_value(exc, token=token),
        "Shift Type": st,
        "Original Date": _bitable_date_ms(original_date),
        "Exchange Date": _bitable_date_ms(exchange_date),
        "Reason": reason_s,
    }
    _bitable_update_record(token, OSE_OFFSET_TABLE_ID, rid, fields)
    invalidate_ose_bitable_cache()
    _schedule_offset_duty_wiki_sync(record_id=rid)
    return {"ok": True, "record_id": rid}


if __name__ == "__main__":
    if "--debug" in sys.argv:
        DEBUG = True
        sys.argv.remove("--debug")
    if len(sys.argv) > 1:
        print(osedate(sys.argv[1]))
    else:
        print(get_ose_today_duty())