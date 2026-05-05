#!/usr/bin/env python3
"""
OSE Duty + Leave + Offset

- Shift source: OSE sheet (`D`=day, `N`=night)
- Leave source: Lark Bitable (Approved + date range)
- Offset source: Lark Bitable (Approved + Original/Exchange date)
"""

from __future__ import annotations

import os
import re
import sys
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


def _parse_date_value(v: Any) -> Optional[date]:
    if v is None or v == "":
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


def get_shift_names_for_date(target_date: date) -> tuple[list[str], list[str]]:
    """Return (morning_names, night_names) from OSE shift sheet."""
    try:
        token = get_tenant_access_token()
    except Exception:
        return [], []
    props = get_sheet_metadata(token, SPREADSHEET_TOKEN, SHEET_ID)
    if not props:
        return [], []
    max_row = props.get("rowCount", 200)
    max_col = props.get("columnCount", 200)
    scan_range = f"A1:{col_index_to_letter(max_col)}{max_row}"
    values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, scan_range)
    if not values or len(values) < 2:
        return [], []

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


def _extract_leave_entries_for_date(target_date: date, token: str) -> list[dict[str, Any]]:
    items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_LEAVE_TABLE_ID)
    out: list[dict[str, Any]] = []
    for it in items:
        f = it.get("fields") or {}
        if not _is_approved(f.get("Status")):
            continue
        name = _title_name(_field_text(f.get("Name")))
        if not name:
            continue
        st = _parse_date_value(f.get("Start Date"))
        ed = _parse_date_value(f.get("End Date"))
        if not st or not ed:
            continue
        if st <= target_date <= ed:
            out.append(
                {
                    "name": name,
                    "leave_type": _field_text(f.get("Leave Type")) or "Leave",
                    "start": st,
                    "end": ed,
                }
            )
    return sorted(out, key=lambda x: x["name"])


def _extract_offset_lines_for_date(target_date: date, token: str) -> list[str]:
    items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_OFFSET_TABLE_ID)
    lines: list[str] = []
    for it in items:
        f = it.get("fields") or {}
        if not _is_approved(f.get("Approval Status")):
            continue
        req = _title_name(_field_text(f.get("Request Person")))
        exc = _title_name(_field_text(f.get("Exchange Person")))
        od = _parse_date_value(f.get("Original Date"))
        xd = _parse_date_value(f.get("Exchange Date"))
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
      - 'date': Rest previous night, Luck target morning
    """
    try:
        if mode == "evening":
            rest_names, _night_unused = get_shift_names_for_date(target_date)
            _m_unused, luck_names = get_shift_names_for_date(target_date)
        elif mode == "morning":
            _, rest_names = get_shift_names_for_date(target_date - timedelta(days=1))
            luck_names, _ = get_shift_names_for_date(target_date)
        else:
            _, rest_names = get_shift_names_for_date(target_date - timedelta(days=1))
            luck_names, _ = get_shift_names_for_date(target_date)

        token = get_tenant_access_token()
        leave_entries = _extract_leave_entries_for_date(target_date, token)
        offset_lines = _extract_offset_lines_for_date(target_date, token)
    except Exception as e:
        return [], [], [], [], f"❌ OSE data load failed: {e}"

    leave_names = {_name_key(str(r.get("name") or "").strip()) for r in leave_entries}
    rest_names = [n for n in rest_names if _name_key(n) not in leave_names]
    luck_names = [n for n in luck_names if _name_key(n) not in leave_names]
    return sorted(rest_names), sorted(luck_names), offset_lines, leave_entries, None


def build_ose_message_card(
    *,
    target_date: date,
    rest_names: list[str],
    luck_names: list[str],
    offset_lines: list[str],
    leave_entries: list[dict[str, Any]],
    include_tag: bool = False,
) -> dict[str, Any]:
    mention_line = (
        f'👥 <at id="{TARGET_USER_OPEN_ID}">User</at>'
        if include_tag and TARGET_USER_OPEN_ID
        else "👥 @CP OM Duty"
    )
    lines: list[str] = []
    lines.append(mention_line)
    lines.append(f"📅 **{target_date.strftime('%d/%m/%Y')}**")
    lines.append("")
    lines.extend(_section_lines("😴 (～￣▽￣)～ Rest Well", [f"• {n}" for n in rest_names]))
    lines.extend(_section_lines("🍀 Good Luckヾ(≧▽≦*)o", [f"• {n}" for n in luck_names]))
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

    lines: list[str] = []
    if include_tag and TARGET_USER_OPEN_ID:
        lines.append(f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>')
    else:
        lines.append("@CP OM Duty")
    lines.append("(～￣▽￣)～ Rest Well")
    lines.extend([f"• {n}" for n in rest_names] or ["• -"])
    lines.append("")
    lines.append("Good Luckヾ(≧▽≦*)o")
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


if __name__ == "__main__":
    if "--debug" in sys.argv:
        DEBUG = True
        sys.argv.remove("--debug")
    if len(sys.argv) > 1:
        print(osedate(sys.argv[1]))
    else:
        print(get_ose_today_duty())