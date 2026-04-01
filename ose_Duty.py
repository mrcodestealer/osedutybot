#!/usr/bin/env python3
"""
OSE Duty Schedule – Today's Duty or Specific Date

Usage:
    ./ose_duty.py                # shows today's duty
    ./ose_duty.py DD/MM/YYYY     # shows duty for the specified date
    ./ose_duty.py --debug        # shows debug output (add before date, or alone)
"""

import re
import sys
import requests
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()
# ================= Configuration =================
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
SPREADSHEET_TOKEN = os.getenv("OSE_SPREADSHEET_TOKEN")
SHEET_ID = os.getenv("OSE_SHEET_ID")
LEAVE_SPREADSHEET_TOKEN = os.getenv("OSE_LEAVE_SPREADSHEET_TOKEN")
LEAVE_SHEET_ID = os.getenv("OSE_LEAVE_SHEET_ID")

# Target names as they appear in column A (case‑insensitive start‑match)
TARGET_NAMES = [
    "Louie", "Bryan Peh", "Eduard James", "Chrisjames", "Augustine Siyew",
    "Man Chung", "JanRei", "Katleen", "Lynette", "Chun Chee",
    "Renzel", "Jun Chen", "Kenneth", "Jewel", "Justine Miguel",
    "Kheng Kwan", "Kris Ng"
]

# Month name to number mapping (full and abbreviated)
MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12,
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

DEBUG = False

def debug_print(*args, **kwargs):
    if DEBUG:
        print("[DEBUG]", *args, file=sys.stderr, **kwargs)

def col_index_to_letter(col_index):
    """Convert 1‑based column index to Excel column letters."""
    letters = ''
    while col_index > 0:
        col_index -= 1
        letters = chr(65 + (col_index % 26)) + letters
        col_index //= 26
    return letters

def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to get token: {result}")
    return result["tenant_access_token"]

def get_sheet_metadata(token, spreadsheet_token, sheet_id):
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        debug_print(f"Metadata error: {result.get('msg')}")
        return None
    sheets = result.get("data", {}).get("sheets", [])
    for sheet in sheets:
        if sheet.get("sheetId") == sheet_id:
            return {
                "rowCount": sheet.get("rowCount"),
                "columnCount": sheet.get("columnCount")
            }
    return None

def get_range_values(token, spreadsheet_token, sheet_id, range_str):
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_id}!{range_str}?valueRenderOption=FormattedValue"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    debug_print(f"Range values response: {result}")  # 添加这行
    if result.get("code") != 0:
        return None
    return result.get("data", {}).get("valueRange", {}).get("values", [])

def get_leaves_for_date(target_date):
    """
    从请假表读取指定日期的请假信息。
    假设请假表的结构：
        - 第一行是表头，包含 "Name" 和 "Leave Type" 列
        - 有一列是日期列（格式 DD/MM/YYYY 或类似）
    返回列表，元素为 (name, leave_type)
    """
    if not LEAVE_SPREADSHEET_TOKEN or not LEAVE_SHEET_ID:
        debug_print("Leave sheet not configured (missing token or sheet ID).")
        return []

    try:
        token = get_tenant_access_token()
    except Exception as e:
        debug_print(f"Failed to get token for leave sheet: {e}")
        return []

    # 获取请假表的最大行列数（用于全表扫描）
    props = get_sheet_metadata(token, LEAVE_SPREADSHEET_TOKEN, LEAVE_SHEET_ID)
    if not props:
        debug_print("Cannot retrieve leave sheet metadata")
        return []
    max_row = props.get("rowCount", 500)
    max_col = props.get("columnCount", 20)
    end_col = col_index_to_letter(max_col)
    scan_range = f"A1:{end_col}{max_row}"

    values = get_range_values(token, LEAVE_SPREADSHEET_TOKEN, LEAVE_SHEET_ID, scan_range)
    debug_print(f"values type: {type(values)}, length: {len(values) if values else 0}")
    if values:
        debug_print(f"First row: {values[0] if len(values)>0 else None}")
        debug_print(f"Second row: {values[1] if len(values)>1 else None}")
    if values is None or len(values) < 2:
        debug_print("No data in leave sheet")
        return []

    # 假设第一行是表头，找到 "Name" 和 "Leave Type" 以及日期列
    header = values[0]
    name_col = None
    leave_type_col = None
    date_col = None

    for idx, cell in enumerate(header):
        if cell is None:
            continue
        cell_str = str(cell).strip().lower()
        if "name" in cell_str or "姓名" in cell_str:
            name_col = idx
        elif "leave type" in cell_str or "type" in cell_str or "请假类型" in cell_str:
            leave_type_col = idx
        elif "date" in cell_str or "day" in cell_str or "日期" in cell_str:
            date_col = idx
    debug_print("First few rows of leave sheet:")
    for i, row in enumerate(values[:5]):
        debug_print(f"Row {i}: {row}")

    if name_col is None or leave_type_col is None or date_col is None:
        debug_print("Leave sheet missing required columns (Name, Leave Type, Date)")
        return []

    leaves = []
    target_date_str = target_date.strftime("%d/%m/%Y")  # 可根据实际日期格式调整

    for row in values[1:]:  # 跳过表头
        if len(row) <= max(name_col, leave_type_col, date_col):
            continue
        cell_date = row[date_col]
        if cell_date is None:
            continue
        # 尝试匹配日期，支持多种格式
        row_date_str = str(cell_date).strip()
        if target_date_str in row_date_str or target_date.strftime("%Y-%m-%d") in row_date_str:
            name = row[name_col] if name_col < len(row) else None
            leave_type = row[leave_type_col] if leave_type_col < len(row) else None
            if name and leave_type:
                leaves.append((str(name).strip(), str(leave_type).strip()))

    return leaves

def get_shift_names_for_date(target_date):
    """
    Returns a tuple (morning_names, night_names) for the given date.
    Each is a list of strings (names) sorted alphabetically.
    """
    current_year = target_date.year
    current_month = target_date.month
    current_day = target_date.day

    try:
        token = get_tenant_access_token()
    except Exception as e:
        return [], []

    props = get_sheet_metadata(token, SPREADSHEET_TOKEN, SHEET_ID)
    if not props:
        return [], []
    max_row = props.get("rowCount", 200)
    max_col = props.get("columnCount", 200)
    end_col = col_index_to_letter(max_col)
    scan_range = f"A1:{end_col}{max_row}"

    values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, scan_range)
    if values is None or len(values) < 2:
        return [], []

    # 1. Find the column whose header contains the target month/year
    target_header_col = None
    for col_idx, cell in enumerate(values[0]):
        mon_num, year = parse_month_year(cell)
        if mon_num == current_month and year == current_year:
            target_header_col = col_idx
            break
    if target_header_col is None:
        return [], []

    # 2. Find today's day column
    date_col = None
    for row_idx in range(1, min(15, len(values))):
        row = values[row_idx]
        if not row:
            continue
        for col in range(len(row)):
            cell = row[col]
            try:
                day_num = int(str(cell).strip())
            except (ValueError, TypeError):
                continue
            if day_num == current_day:
                # Verify that this column belongs to the target month
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

    # 3. Collect names from column A that match target list
    name_rows = {}
    for row_idx in range(2, len(values)):
        row = values[row_idx]
        if not row or len(row) == 0:
            continue
        cell_a = row[0]
        if not isinstance(cell_a, str):
            continue
        cell_upper = cell_a.upper()
        for target in TARGET_NAMES:
            if cell_upper.startswith(target.upper()):
                if target not in name_rows:
                    name_rows[target] = row_idx
                break

    if not name_rows:
        return [], []

    # 4. Check the cell at (name row, date column)
    morning = []
    night = []
    for name, row_idx in name_rows.items():
        if row_idx >= len(values):
            continue
        row = values[row_idx]
        if date_col >= len(row):
            continue
        cell = row[date_col]
        if not isinstance(cell, str):
            continue
        code = cell.strip().upper()
        if code == "D":
            morning.append(name)
        elif code == "N":
            night.append(name)

    morning.sort()
    night.sort()
    return morning, night

def parse_month_year(text):
    """Extract (month_num, year) from a string like 'March 2026' or 'Dec 2026'."""
    if not isinstance(text, str):
        return None, None
    for mon_name, mon_num in MONTH_MAP.items():
        pattern = rf"\b{re.escape(mon_name)}\b\s+(\d{{4}})"
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return mon_num, int(m.group(1))
    return None, None

def get_ose_duty_for_date(target_date):
    current_year = target_date.year
    current_month = target_date.month
    current_day = target_date.day

    debug_print(f"Looking for {target_date.strftime('%d/%m/%Y')} (month {current_month}, year {current_year})")

    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ Failed to get access token: {e}"

    props = get_sheet_metadata(token, SPREADSHEET_TOKEN, SHEET_ID)
    if not props:
        return "❌ Cannot retrieve sheet metadata"
    max_row = props.get("rowCount", 200)
    max_col = props.get("columnCount", 200)
    end_col = col_index_to_letter(max_col)
    scan_range = f"A1:{end_col}{max_row}"
    debug_print(f"Using range {scan_range}")

    values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, scan_range)
    if values is None:
        return "❌ Failed to read sheet data"
    if len(values) < 2:
        return "Sheet has fewer than 2 rows."

    # 1. Find the column whose header (row0) contains the target month/year
    target_header_col = None
    for col_idx, cell in enumerate(values[0]):
        mon_num, year = parse_month_year(cell)
        if mon_num == current_month and year == current_year:
            target_header_col = col_idx
            debug_print(f"Found target header at column {col_idx}")
            break

    if target_header_col is None:
        return f"❌ Could not find header for {target_date.strftime('%B %Y')} in row 1."

    # 2. Find today's day column in rows 1..15
    date_col = None
    for row_idx in range(1, min(15, len(values))):
        row = values[row_idx]
        if not row:
            continue
        for col in range(len(row)):
            cell = row[col]
            try:
                day_num = int(str(cell).strip())
            except (ValueError, TypeError):
                continue
            if day_num == current_day:
                # Verify that this column belongs to the target month
                header = ""
                for hcol in range(col, -1, -1):
                    if hcol < len(values[0]) and values[0][hcol]:
                        header = values[0][hcol]
                        break
                mon_num, year = parse_month_year(header)
                if mon_num == current_month and year == current_year:
                    date_col = col
                    debug_print(f"Found day {current_day} at column {col} (row {row_idx})")
                    break
        if date_col is not None:
            break

    if date_col is None:
        return f"📅 {target_date.strftime('%d/%m/%Y')} not found under {target_date.strftime('%B %Y')}."

    # 3. Collect names from column A that match target list
    name_rows = {}
    for row_idx in range(2, len(values)):
        row = values[row_idx]
        if not row or len(row) == 0:
            continue
        cell_a = row[0]
        if not isinstance(cell_a, str):
            continue
        cell_upper = cell_a.upper()
        for target in TARGET_NAMES:
            if cell_upper.startswith(target.upper()):
                if target not in name_rows:
                    name_rows[target] = row_idx
                break

    if not name_rows:
        return f"📅 {target_date.strftime('%d/%m/%Y')} – no OSE duty assigned (no target names found)."

    # 4. Check the cell at (name row, date column)
    duties = []   # list of (name, shift)
    for name, row_idx in name_rows.items():
        if row_idx >= len(values):
            continue
        row = values[row_idx]
        if date_col >= len(row):
            continue
        cell = row[date_col]
        if not isinstance(cell, str):
            continue
        code = cell.strip().upper()
        if code == "D":
            shift = "Morning Shift"
        elif code == "N":
            shift = "Night Shift"
        else:
            continue
        duties.append((name, shift))

    if not duties:
        return f"📅 {target_date.strftime('%d/%m/%Y')} – no OSE duty assigned."

    # Separate into morning and night
    morning = [name for name, shift in duties if shift == "Morning Shift"]
    night = [name for name, shift in duties if shift == "Night Shift"]
    # Sort each list alphabetically
    morning.sort()
    night.sort()

    lines = [f"OSE Duty – {target_date.strftime('%d/%m/%Y')}", ""]
    if morning:
        lines.append("Morning Shift")
        for name in morning:
            lines.append(f"• {name}")
    if night:
        if morning:
            lines.append("")  # blank line between shifts
        lines.append("Night Shift")
        for name in night:
            lines.append(f"• {name}")

    # 2. 获取请假信息
    leaves = get_leaves_for_date(target_date)

    # 3. 构建输出
    lines = [f"OSE Duty – {target_date.strftime('%d/%m/%Y')}", ""]

    if morning:
        lines.append("Morning Shift")
        lines.extend([f"• {name}" for name in morning])
    if night:
        if morning:
            lines.append("")
        lines.append("Night Shift")
        lines.extend([f"• {name}" for name in night])

    if leaves:
        if morning or night:
            lines.append("")
        lines.append("Leave")
        for name, leave_type in leaves:
            lines.append(f"• {name} - {leave_type}")

    if not morning and not night and not leaves:
        return f"📅 {target_date.strftime('%d/%m/%Y')} – no duty or leave records found."

    return "\n".join(lines)

def osedate(date_str):
    try:
        target_date = datetime.strptime(date_str, "%d/%m/%Y").date()
    except ValueError:
        return "❌ Invalid date format. Please use DD/MM/YYYY (e.g., 31/12/2026)"
    return get_ose_duty_for_date(target_date)

def get_ose_today_duty():
    """Return today's OSE duty (for use in main bot)."""
    return get_ose_duty_for_date(datetime.now().date())

if __name__ == "__main__":
    # Check for --debug flag
    if "--debug" in sys.argv:
        DEBUG = True
        sys.argv.remove("--debug")

    if len(sys.argv) > 1:
        print(osedate(sys.argv[1]))
    else:
        print(get_ose_today_duty())