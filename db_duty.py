#!/usr/bin/env python3
"""
DB Duty Schedule – Weekly Summary (3 weeks by default)

Usage:
    ./db_duty.py                # shows next 3 weeks of DB duty (current, next, next+1)
    ./db_duty.py DD/MM/YYYY     # shows duty for the specified date
    ./db_duty.py --week         # shows this week's duty summary (single week)
    ./db_duty.py --week-detail   # shows day-by-day duty for the current week
    ./db_duty.py --week-detail DD/MM/YYYY  # day-by-day for the week containing the given date
    ./db_duty.py --debug         # enables debug output
"""

import re
import sys
import csv
import os
import requests
from datetime import datetime, timedelta

# ================= Configuration =================
APP_ID = "cli_a9ca652b89b85ed1"
APP_SECRET = "VQJh0oFKfsyCHr5tQDMVNbr4o4kmjbFr"
SPREADSHEET_TOKEN = "O4Dfw4DVTiPpFukn801l5z3WgMd"
DUTY_LIST_PATH = "dutyList.csv"          # optional fallback

# DB team members and their CLEAN phone numbers (no extra text)
TARGET_DUTY = [
    {"name": "Kah Zheng", "phone": "+60169294328"},
    {"name": "Ken", "phone": "+60192336398"},
    {"name": "Ziyang", "phone": "+60102398909"},
    {"name": "Monlong", "phone": "+60104237748"},
]

# Month name to number mapping (full and abbreviated)
MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12,
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

# ---------- Debug flag ----------
DEBUG = False

def debug_print(*args, **kwargs):
    if DEBUG:
        print("[DEBUG]", *args, file=sys.stderr, **kwargs)

# ---------- Helper functions ----------
def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to get token: {result}")
    return result["tenant_access_token"]

def get_all_sheets_metadata(token):
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/metainfo"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        return None
    return result.get("data", {}).get("sheets", [])

def get_sheet_id_by_year(year):
    """Return sheet ID for the sheet named 'OSE{year}', or None if not found."""
    token = get_tenant_access_token()
    sheets = get_all_sheets_metadata(token)
    if not sheets:
        return None
    target_title = f"OSE{year}"
    for sheet in sheets:
        if sheet.get("title") == target_title:
            debug_print(f"Found sheet '{target_title}' with ID: {sheet.get('sheetId')}")
            return sheet.get("sheetId")
    debug_print(f"Sheet '{target_title}' not found.")
    return None

def get_range_values_for_sheet(token, sheet_id, range_str):
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{sheet_id}!{range_str}?valueRenderOption=FormattedValue"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        return None
    return result.get("data", {}).get("valueRange", {}).get("values", [])

def get_sheet_metadata_by_id(token, sheet_id):
    sheets = get_all_sheets_metadata(token)
    if not sheets:
        return None
    for sheet in sheets:
        if sheet.get("sheetId") == sheet_id:
            return {"rowCount": sheet.get("rowCount"), "columnCount": sheet.get("columnCount")}
    return None

def is_checked(cell):
    if cell is None:
        return False
    if isinstance(cell, bool):
        return cell
    if isinstance(cell, (int, float)):
        return cell == 1
    if isinstance(cell, str):
        val = cell.strip().lower()
        return val in ('✓', '✔', '是', '1', 'true', 'yes', '勾')
    return False

def extract_text_from_cell(cell):
    if cell is None:
        return ""
    if isinstance(cell, str):
        return cell
    if isinstance(cell, list):
        parts = []
        for item in cell:
            if isinstance(item, dict) and 'text' in item:
                parts.append(item['text'])
            elif isinstance(item, str):
                parts.append(item)
        return ''.join(parts)
    return str(cell)

def parse_month_year(text):
    if not isinstance(text, str):
        return None, None
    for mon_name, mon_num in MONTH_MAP.items():
        pattern = rf'\b{re.escape(mon_name)}\b[^\d]*(\d{{4}})'
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return mon_num, int(m.group(1))
    return None, None

def get_month_for_column(col_idx, headers):
    for c in range(col_idx, -1, -1):
        if c < len(headers):
            header_text = extract_text_from_cell(headers[c])
            if header_text:
                mon_num, year = parse_month_year(header_text)
                if mon_num is not None:
                    debug_print(f"Column {c} header '{header_text}' -> month {mon_num}, year {year}")
                    return mon_num, year
                else:
                    debug_print(f"Column {c} header '{header_text}' – no month/year pattern found")
    return None, None

def find_target_rows(values):
    """Find rows in column A that contain one of the target names."""
    target_rows = []  # list of (row_idx, name)
    for row_idx in range(len(values)):
        row = values[row_idx]
        if not row or len(row) == 0:
            continue
        cell_a = extract_text_from_cell(row[0])
        if not cell_a:
            continue
        for target in TARGET_DUTY:
            name = target["name"]
            if re.search(rf'\b{re.escape(name)}\b', cell_a, re.IGNORECASE):
                target_rows.append((row_idx, name))
                debug_print(f"Found target '{name}' at row {row_idx}")
                break
    return target_rows

def get_date_column(target_date, values):
    """Return the column index where the day number for target_date is located, or None."""
    current_year = target_date.year
    current_month = target_date.month
    current_day = target_date.day

    # Find the row that contains day numbers (search rows 0..10)
    day_row_idx = None
    day_row_candidates = []
    for row_idx in range(min(10, len(values))):
        row = values[row_idx]
        if not row:
            continue
        numeric_count = 0
        for cell in row:
            try:
                num = int(extract_text_from_cell(cell).strip())
                if 1 <= num <= 31:
                    numeric_count += 1
            except (ValueError, TypeError):
                continue
        if numeric_count > 2:
            day_row_candidates.append((row_idx, numeric_count))
    if not day_row_candidates:
        return None
    day_row_candidates.sort(key=lambda x: x[1], reverse=True)
    day_row_idx = day_row_candidates[0][0]

    day_row = values[day_row_idx]
    for col in range(len(day_row)):
        cell_text = extract_text_from_cell(day_row[col])
        try:
            day_num = int(cell_text.strip())
        except (ValueError, TypeError):
            continue
        if day_num == current_day:
            col_month, col_year = get_month_for_column(col, values[0] if len(values) > 0 else [])
            if col_month == current_month and col_year == current_year:
                return col
    return None

def get_duty_for_date(target_date, values, target_rows):
    """Return list of names for DB members checked on target_date."""
    date_col = get_date_column(target_date, values)
    if date_col is None:
        return []
    checked = []
    for row_idx, name in target_rows:
        if row_idx >= len(values):
            continue
        row = values[row_idx]
        if date_col >= len(row):
            continue
        cell = extract_text_from_cell(row[date_col])
        if is_checked(cell):
            checked.append(name)
    return checked

def get_values_and_targets_for_year(year):
    """Fetch sheet data and target rows for a given year."""
    sheet_id = get_sheet_id_by_year(year)
    if not sheet_id:
        return None, None, f"❌ Sheet OSE{year} not found."

    try:
        token = get_tenant_access_token()
    except Exception as e:
        return None, None, f"❌ Failed to get access token: {e}"

    props = get_sheet_metadata_by_id(token, sheet_id)
    if not props:
        return None, None, f"❌ Cannot retrieve sheet metadata for OSE{year}."

    max_row = props.get("rowCount", 200)
    scan_range = f"A1:ZZ{max_row}"
    values = get_range_values_for_sheet(token, sheet_id, scan_range)
    if values is None:
        return None, None, f"❌ Failed to read sheet data for OSE{year}."
    if len(values) < 2:
        return None, None, f"Sheet OSE{year} has fewer than 2 rows."

    target_rows = find_target_rows(values)
    return values, target_rows, None

def get_week_summary_from_data(week_start, values, target_rows):
    """Return week summary using pre‑fetched data."""
    duty_names = set()
    for i in range(7):
        day = week_start + timedelta(days=i)
        checked = get_duty_for_date(day, values, target_rows)
        duty_names.update(checked)

    week_end = week_start + timedelta(days=6)
    title = f"📅 DB Duty – {week_start.strftime('%B %d, %Y Monday')} – {week_end.strftime('%B %d, %Y Sunday')}"
    if not duty_names:
        return f"{title}\n• no duty"

    lines = [title]
    for name in sorted(duty_names):
        phone = next((t["phone"] for t in TARGET_DUTY if t["name"] == name), "未找到电话号码")
        lines.append(f"• {name} DB (Phone: {phone})")
    return "\n".join(lines)

def get_three_weeks_summary():
    """Return summaries for the next three weeks (current, next, next+1)."""
    today = datetime.now().date()
    monday = today - timedelta(days=today.weekday())   # Monday of current week

    # Determine the year of the Monday (assumes all three weeks are in same year)
    year = monday.year
    values, target_rows, error = get_values_and_targets_for_year(year)
    if error:
        return error
    if not target_rows:
        return "❌ No DB duty section found in sheet."

    results = []
    for i in range(3):
        week_start = monday + timedelta(days=7*i)
        results.append(get_week_summary_from_data(week_start, values, target_rows))
    return "\n\n".join(results)

def get_db_day_duty(target_date):
    """Return duty for a single date."""
    values, target_rows, error = get_values_and_targets_for_year(target_date.year)
    if error:
        return error
    if not target_rows:
        return f"📅 {target_date.strftime('%d/%m/%Y')} – no DB duty section found."

    checked = get_duty_for_date(target_date, values, target_rows)
    if not checked:
        return f"📅 {target_date.strftime('%d/%m/%Y')} – no db duty assigned."

    checked.sort()
    lines = [f"📅 DB Duty – {target_date.strftime('%d/%m/%Y')}"]
    for name in checked:
        phone = next((t["phone"] for t in TARGET_DUTY if t["name"] == name), "未找到电话号码")
        lines.append(f"• {name} DB (Phone: {phone})")
    return "\n".join(lines)

def get_db_week_detail(week_start=None):
    """Return a day-by-day breakdown for the week."""
    if week_start is None:
        today = datetime.now().date()
        week_start = today - timedelta(days=today.weekday())
    else:
        week_start = week_start - timedelta(days=week_start.weekday())

    values, target_rows, error = get_values_and_targets_for_year(week_start.year)
    if error:
        return error
    if not target_rows:
        return "❌ No DB duty section found in sheet."

    lines = [f"📅 DB Duty details – week starting {week_start.strftime('%d/%m/%Y')}"]
    missing_days = []
    for i in range(7):
        day = week_start + timedelta(days=i)
        checked = get_duty_for_date(day, values, target_rows)
        if not checked:
            missing_days.append(day.strftime('%A %d/%m/%Y'))
            lines.append(f"  {day.strftime('%A %d/%m/%Y')}: ❌ no duty")
        else:
            checked.sort()
            duty_list = []
            for name in checked:
                phone = next((t["phone"] for t in TARGET_DUTY if t["name"] == name), "未找到电话号码")
                duty_list.append(f"{name} DB (Phone: {phone})")
            lines.append(f"  {day.strftime('%A %d/%m/%Y')}: ✅ " + ", ".join(duty_list))
    if missing_days:
        lines.append(f"\n⚠️ Missing duty on: {', '.join(missing_days)}")
    else:
        lines.append("\n✅ All days have duty assigned.")
    return "\n".join(lines)

def parse_date_arg(arg):
    try:
        return datetime.strptime(arg, "%d/%m/%Y").date()
    except ValueError:
        return None

def list_sheet_ids():
    try:
        token = get_tenant_access_token()
        sheets = get_all_sheets_metadata(token)
        if not sheets:
            print("No sheets found.")
            return
        print("Available sheets:")
        for sheet in sheets:
            print(f"  Title: {sheet.get('title')}, ID: {sheet.get('sheetId')}")
    except Exception as e:
        print("Error listing sheets:", e)
        
def db_check(month=None, year=None):
    """
    检查 DB 值班表中指定月份（默认为当前月份）是否有空缺。
    返回字符串，格式与其他 check 命令一致。
    """
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month

    # 计算该月的总天数
    if month == 12:
        next_month_first = datetime(year + 1, 1, 1).date()
    else:
        next_month_first = datetime(year, month + 1, 1).date()
    days_in_month = (next_month_first - datetime(year, month, 1).date()).days

    # 获取该年份的表格数据
    values, target_rows, error = get_values_and_targets_for_year(year)
    if error:
        return error
    if not target_rows:
        return f"❌ 在 OSE{year} 工作表中未找到 DB 值班人员区域。"

    missing = []
    for day in range(1, days_in_month + 1):
        target_date = datetime(year, month, day).date()
        checked = get_duty_for_date(target_date, values, target_rows)
        if not checked:
            missing.append(day)

    month_name = datetime(year, month, 1).strftime("%B %Y")
    if not missing:
        return f"✅ All days in {month_name} have duty assigned."
    else:
        missing_str = ", ".join(str(d) for d in missing)
        return f"⚠️ {month_name} 缺少值班的日期：{missing_str}"

if __name__ == "__main__":
    if "--debug" in sys.argv:
        DEBUG = True
        sys.argv.remove("--debug")
        list_sheet_ids()
        print("\n" + "="*50 + "\n")

    if len(sys.argv) > 1:
        if sys.argv[1] in ("--week", "-w"):
            # Show a single week summary
            if len(sys.argv) > 2:
                user_date = parse_date_arg(sys.argv[2])
                if user_date:
                    monday = user_date - timedelta(days=user_date.weekday())
                    values, target_rows, error = get_values_and_targets_for_year(monday.year)
                    if error:
                        print(error)
                    else:
                        print(get_week_summary_from_data(monday, values, target_rows))
                else:
                    print(get_three_weeks_summary())  # fallback
            else:
                # default: show current week
                today = datetime.now().date()
                monday = today - timedelta(days=today.weekday())
                values, target_rows, error = get_values_and_targets_for_year(monday.year)
                if error:
                    print(error)
                else:
                    print(get_week_summary_from_data(monday, values, target_rows))
        elif sys.argv[1] == "--week-detail":
            if len(sys.argv) > 2:
                user_date = parse_date_arg(sys.argv[2])
                if user_date:
                    print(get_db_week_detail(user_date))
                else:
                    print(get_db_week_detail())
            else:
                print(get_db_week_detail())
        else:
            user_date = parse_date_arg(sys.argv[1])
            if user_date is None:
                print("❌ Invalid date format. Please use DD/MM/YYYY (e.g., 03/02/2026)")
                sys.exit(1)
            print(get_db_day_duty(user_date))
    else:
        # Default: show three weeks
        print(get_three_weeks_summary())