#!/usr/bin/env python3
"""
SRE Duty Schedule – Duty for a Given Date / Weekly Summary

Usage:
    ./sre_duty.py                # shows today's duty
    ./sre_duty.py DD/MM/YYYY     # shows duty for the specified date
    ./sre_duty.py --week         # shows this week and next week duty summary
    ./sre_duty.py -w              # same as --week
"""

import re
import sys
import csv
import os
import requests
from datetime import datetime, timedelta
from calendar import monthrange
from dotenv import load_dotenv
load_dotenv()
# ================= Configuration =================
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
SPREADSHEET_TOKEN = os.getenv("OSE_SPREADSHEET_TOKEN")
SHEET_ID = os.getenv("OSE_SHEET_ID")
DUTY_LIST_PATH = "dutyList.csv"

# Target names as they appear in column A (case‑insensitive start‑match)
TARGET_NAMES = [
    "Alex Tai", "Kelvin", "WeiSiong", "Bowei", "Jay",
    "Linus Lim", "Jeng Liang", "Misa", "Kai Xuan", "Yoon Hong",
    "Adrian"
]

# 表格中出现的姓名形式 → CSV 中的标准姓名（用于电话查询）
TABLE_TO_CSV = {
    "WeiSiong": "Wei Siong",
    "Bowei": "Bo Wei",
    "BoWei": "Bo Wei",
    "Kai Xuan": "Kai Xuan",
    "KaiXuan": "Kai Xuan",
    "Yoon Hong": "Yoon Hong",
    "YoonHong": "Yoon Hong",
    "Alex Tai": "Alex Tai",
    "Kelvin": "Kelvin",
    "Kelvin": "Kelvin Er",
    "Jay": "Jay",
    "Linus Lim": "Linus Lim",
    "Jeng Liang": "Jeng Liang",
    "Misa": "Misa",
    "Adrian": "Adrian",
}

# 姓名 → 项目信息（固定映射，根据您提供的列表）
NAME_TO_PROJECT = {
    "Alex Tai": "FPMS",
    "Kelvin": "FE",
    "WeiSiong": "",
    "Bowei": "PMS/IGO/CPMS",
    "Jay": "FE",
    "Linus Lim": "FPMS",
    "Jeng Liang": "FPMS",
    "Misa": "FPMS",
    "Kai Xuan": "PMS/IGO/CPMS",
    "Yoon Hong": "FE/PMS/IGO/CPMS",
    "Adrian": "",
}

# Month name to number mapping (full and abbreviated)
MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12,
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

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
    # Use valueRenderOption=FormattedValue to get computed numbers, not formulas
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_id}!{range_str}?valueRenderOption=FormattedValue"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        return None
    return result.get("data", {}).get("valueRange", {}).get("values", [])

def get_phone_from_dutylist(name):
    """从 dutyList.csv 中查询电话号码（三列：姓名,团队,电话）"""
    if not os.path.exists(DUTY_LIST_PATH):
        return "未找到电话号码文件"
    try:
        with open(DUTY_LIST_PATH, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3 and row[0].strip() == name:
                    return row[2].strip()
    except Exception:
        pass
    return "未找到电话号码"

def is_checked(cell):
    """
    Return True if the cell represents a checked checkbox or contains a check mark.
    Handles various representations: boolean True, integer 1, strings like "✓", "true", "是", etc.
    """
    if cell is None:
        return False
    if isinstance(cell, bool):
        return cell
    if isinstance(cell, (int, float)):
        return cell == 1
    if isinstance(cell, str):
        val = cell.strip().lower()
        # Common check mark representations
        return val in ('✓', '✔', '是', '1', 'true', 'yes', '勾')
    return False

def parse_month_year(text):
    """Extract (month_num, year) from a string like 'March 2026'."""
    if not isinstance(text, str):
        return None, None
    for mon_name, mon_num in MONTH_MAP.items():
        pattern = rf"{re.escape(mon_name)}\s+(\d{{4}})"
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return mon_num, int(m.group(1))
    return None, None

def extract_text_from_cell(cell):
    """将单元格内容（可能为富文本列表）转换为纯文本字符串"""
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

def _get_duty_names_for_date(target_date, values):
    """
    内部函数：给定日期和已读取的表格数据 values，返回该日值班人员列表。
    每个元素为 raw_name（表格中的原始姓名，用于查找项目映射）。
    """
    current_year = target_date.year
    current_month = target_date.month
    current_day = target_date.day

    # 定位表头列（月份/年）
    target_header_col = None
    for col_idx, cell in enumerate(values[0]):
        mon_num, year = parse_month_year(extract_text_from_cell(cell))
        if mon_num == current_month and year == current_year:
            target_header_col = col_idx
            break
    if target_header_col is None:
        return []   # 未找到月份，无人值班

    # 定位日期列
    date_col = None
    for row_idx in range(1, min(5, len(values))):
        row = values[row_idx]
        for col in range(len(row)):
            cell = extract_text_from_cell(row[col])
            try:
                day_num = int(cell.strip())
            except (ValueError, TypeError):
                continue
            if day_num == current_day:
                # 验证该列是否属于正确月份
                header = ""
                for hcol in range(col, -1, -1):
                    if hcol < len(values[0]) and values[0][hcol]:
                        header = extract_text_from_cell(values[0][hcol])
                        break
                mon_num, year = parse_month_year(header)
                if mon_num == current_month and year == current_year:
                    date_col = col
                    break
        if date_col is not None:
            break
    if date_col is None:
        return []   # 未找到日期列

    # 收集所有目标人员行号
    name_rows = {}      # target_name -> row_index
    for row_idx in range(2, len(values)):
        row = values[row_idx]
        if not row or len(row) == 0:
            continue
        cell_a = extract_text_from_cell(row[0])
        if not cell_a:
            continue
        cell_upper = cell_a.upper()
        for target in TARGET_NAMES:
            if target.upper() in cell_upper:
                if target not in name_rows:
                    name_rows[target] = row_idx
                break

    # 检查哪些人当天打勾，并返回原始姓名
    checked = []  # 列表 of raw_name
    for name, row_idx in name_rows.items():
        if row_idx >= len(values):
            continue
        row = values[row_idx]
        if date_col >= len(row):
            continue
        cell = extract_text_from_cell(row[date_col])
        if is_checked(cell):
            checked.append(name)

    return checked

# ---------- 对外接口 ----------
def get_sre_duty(target_date):
    """返回指定日期的值班信息（格式化字符串）"""
    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ Failed to get access token: {e}"

    props = get_sheet_metadata(token, SPREADSHEET_TOKEN, SHEET_ID)
    if not props:
        return "❌ Cannot retrieve sheet metadata"
    max_row = props.get("rowCount", 200)
    scan_range = f"A1:ZZ{max_row}"
    values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, scan_range)
    if values is None:
        return "❌ Failed to read sheet data"
    if len(values) < 2:
        return "Sheet has fewer than 2 rows."

    checked = _get_duty_names_for_date(target_date, values)
    if not checked:
        return f"📅 {target_date.strftime('%d/%m/%Y')} – no SRE duty assigned."

    # 按姓名排序
    checked.sort()
    lines = [f"📅 SRE Duty – {target_date.strftime('%d/%m/%Y')}"]
    for name in checked:
        csv_name = TABLE_TO_CSV.get(name, name)
        phone = get_phone_from_dutylist(csv_name)
        project = NAME_TO_PROJECT.get(name, "")
        if project:
            lines.append(f"• {name} {project} (Phone: {phone})")
        else:
            lines.append(f"• {name} (Phone: {phone})")
    return "\n".join(lines)

def get_sre_week_duty():
    """返回本周和下周的值班汇总信息（去重），每人显示一次并附带项目信息"""
    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ Failed to get access token: {e}"

    props = get_sheet_metadata(token, SPREADSHEET_TOKEN, SHEET_ID)
    if not props:
        return "❌ Cannot retrieve sheet metadata"
    max_row = props.get("rowCount", 200)
    scan_range = f"A1:ZZ{max_row}"
    values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, scan_range)
    if values is None:
        return "❌ Failed to read sheet data"
    if len(values) < 2:
        return "Sheet has fewer than 2 rows."

    today = datetime.now().date()
    # 本周一
    monday = today - timedelta(days=today.weekday())
    # 下周一
    next_monday = monday + timedelta(days=7)

    def collect_week_names(start_monday):
        """收集从周一到周日这一周内所有值班人员，返回姓名列表（去重）"""
        week_set = set()
        for i in range(7):
            day = start_monday + timedelta(days=i)
            day_checked = _get_duty_names_for_date(day, values)
            week_set.update(day_checked)
        return sorted(week_set)

    this_week_names = collect_week_names(monday)
    next_week_names = collect_week_names(next_monday)

    def format_week_block(title, names):
        if not names:
            return f"📅 {title} – no duty"
        lines = [f"📅 {title}"]
        for name in names:
            csv_name = TABLE_TO_CSV.get(name, name)
            phone = get_phone_from_dutylist(csv_name)
            project = NAME_TO_PROJECT.get(name, "")
            if project:
                lines.append(f"• {name} {project} (Phone: {phone})")
            else:
                lines.append(f"• {name} (Phone: {phone})")
        return "\n".join(lines)

    this_week_str = format_week_block(
        f"SRE Duty this week – {monday.strftime('%d/%m/%Y')}",
        this_week_names
    )
    next_week_str = format_week_block(
        f"SRE Duty next week – {next_monday.strftime('%d/%m/%Y')}",
        next_week_names
    )
    return f"{this_week_str}\n\n{next_week_str}"

def parse_date_arg(arg):
    """Parse a string in DD/MM/YYYY format and return a date object."""
    try:
        return datetime.strptime(arg, "%d/%m/%Y").date()
    except ValueError:
        return None

def get_sre_today_duty():
    """Return today's SRE duty (no arguments)."""
    return get_sre_duty(datetime.now().date())

def sre_check(month=None, year=None):
    """
    检查 SRE 值班表中指定月份（默认为当前月份）是否有空缺。
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

    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ Failed to get access token: {e}"

    props = get_sheet_metadata(token, SPREADSHEET_TOKEN, SHEET_ID)
    if not props:
        return "❌ Cannot retrieve sheet metadata"
    max_row = props.get("rowCount", 200)
    scan_range = f"A1:ZZ{max_row}"
    values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, scan_range)
    if values is None:
        return "❌ Failed to read sheet data"
    if len(values) < 2:
        return "Sheet has fewer than 2 rows."

    missing = []
    for day in range(1, days_in_month + 1):
        target_date = datetime(year, month, day).date()
        checked = _get_duty_names_for_date(target_date, values)
        if not checked:
            missing.append(day)

    month_name = datetime(year, month, 1).strftime("%B %Y")
    if not missing:
        return f"✅ All days in {month_name} have duty assigned."
    else:
        missing_str = ", ".join(str(d) for d in missing)
        return f"⚠️ {month_name} missing duty date：{missing_str}"

def _get_week_duty_summary(start_monday, title_prefix="SRE Duty week starting"):
    """
    Return a formatted string for the week starting at start_monday.
    title_prefix is used to generate the heading.
    """
    # Collect all names for the week (Monday to Sunday)
    week_names = set()
    for i in range(7):
        day = start_monday + timedelta(days=i)
        day_checked = _get_duty_names_for_date(day, values)  # needs values, but we'll pass it
        week_names.update(day_checked)
    week_names = sorted(week_names)

    heading = f"📅 {title_prefix} – {start_monday.strftime('%d/%m/%Y')}"
    if not week_names:
        return f"{heading} – no duty"

    lines = [heading]
    for name in week_names:
        csv_name = TABLE_TO_CSV.get(name, name)
        phone = get_phone_from_dutylist(csv_name)
        project = NAME_TO_PROJECT.get(name, "")
        if project:
            lines.append(f"• {name} {project} (Phone: {phone})")
        else:
            lines.append(f"• {name} (Phone: {phone})")
    return "\n".join(lines)


def srethisweek():
    """Display duty for the current week only."""
    today = datetime.now().date()
    monday = today - timedelta(days=today.weekday())

    # Need to read sheet data again inside this function
    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ Failed to get access token: {e}"

    props = get_sheet_metadata(token, SPREADSHEET_TOKEN, SHEET_ID)
    if not props:
        return "❌ Cannot retrieve sheet metadata"
    max_row = props.get("rowCount", 200)
    scan_range = f"A1:ZZ{max_row}"
    values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, scan_range)
    if values is None:
        return "❌ Failed to read sheet data"
    if len(values) < 2:
        return "Sheet has fewer than 2 rows."

    # Monkey‑patch the helper so it can access the local 'values' variable
    # We'll rewrite _get_week_duty_summary to accept values as an argument.
    # For clarity, we'll inline the logic here.
    week_names = set()
    for i in range(7):
        day = monday + timedelta(days=i)
        day_checked = _get_duty_names_for_date(day, values)
        week_names.update(day_checked)
    week_names = sorted(week_names)

    heading = f"📅 SRE Duty this week – {monday.strftime('%d/%m/%Y')}"
    if not week_names:
        return f"{heading} – no duty"

    lines = [heading]
    for name in week_names:
        csv_name = TABLE_TO_CSV.get(name, name)
        phone = get_phone_from_dutylist(csv_name)
        project = NAME_TO_PROJECT.get(name, "")
        if project:
            lines.append(f"• {name} {project} (Phone: {phone})")
        else:
            lines.append(f"• {name} (Phone: {phone})")
    return "\n".join(lines)


def sretwoweek():
    """Display duty for the current week and the following week."""
    today = datetime.now().date()
    monday = today - timedelta(days=today.weekday())
    next_monday = monday + timedelta(days=7)

    # Read sheet data
    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ Failed to get access token: {e}"

    props = get_sheet_metadata(token, SPREADSHEET_TOKEN, SHEET_ID)
    if not props:
        return "❌ Cannot retrieve sheet metadata"
    max_row = props.get("rowCount", 200)
    scan_range = f"A1:ZZ{max_row}"
    values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, scan_range)
    if values is None:
        return "❌ Failed to read sheet data"
    if len(values) < 2:
        return "Sheet has fewer than 2 rows."

    def week_summary(start_monday, title):
        week_names = set()
        for i in range(7):
            day = start_monday + timedelta(days=i)
            day_checked = _get_duty_names_for_date(day, values)
            week_names.update(day_checked)
        week_names = sorted(week_names)
        lines = [title]
        if not week_names:
            lines.append("– no duty")
        else:
            for name in week_names:
                csv_name = TABLE_TO_CSV.get(name, name)
                phone = get_phone_from_dutylist(csv_name)
                project = NAME_TO_PROJECT.get(name, "")
                if project:
                    lines.append(f"• {name} {project} (Phone: {phone})")
                else:
                    lines.append(f"• {name} (Phone: {phone})")
        return "\n".join(lines)

    this_week_str = week_summary(monday, f"📅 SRE Duty this week – {monday.strftime('%d/%m/%Y')}")
    next_week_str = week_summary(next_monday, f"📅 SRE Duty next week – {next_monday.strftime('%d/%m/%Y')}")
    return f"{this_week_str}\n\n{next_week_str}"


# Update the existing get_sre_week_duty to use the new two‑week implementation
# (keeping the original name for backward compatibility)
def get_sre_week_duty():
    """Return this week and next week duty summary (same as sretwoweek)."""
    return sretwoweek()


# Keep the original p0sre function as is
def p0sre():
    return srethisweek() + "\nIf cannot contact SRE Duty, Kindly contact the duty below\n• Wei Siong 📞60163132882\n• Adrian 📞60123156848"


if __name__ == "__main__":
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg in ("--week", "-w"):
            print(get_sre_week_duty())
        elif arg == "--thisweek":
            print(srethisweek())
        elif arg == "--twoweek":
            print(sretwoweek())
        else:
            user_date = parse_date_arg(arg)
            if user_date is None:
                print(f"❌ Invalid date format. Please use DD/MM/YYYY (e.g., 03/02/2026)")
                sys.exit(1)
            print(get_sre_duty(user_date))
    else:
        print(get_sre_duty(datetime.now().date()))