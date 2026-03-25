import csv
import requests
from datetime import datetime, timedelta
import os
from calendar import monthrange
from dotenv import load_dotenv
load_dotenv()
# ================= 配置信息 =================
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
SPREADSHEET_TOKEN = os.getenv("FPMS_SPREADSHEET_TOKEN")
SHEET_ID = os.getenv("FPMS_SHEET_ID")

DUTY_LIST_PATH = "dutyList.csv"

MONTH_ABBR = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

def col_index_to_letter(col_index):
    """将1-based列索引转换为Excel列字母（A, B, ..., Z, AA, AB, ...）"""
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
        raise Exception(f"获取 token 失败: {result}")
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
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_id}!{range_str}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        return None
    return result.get("data", {}).get("valueRange", {}).get("values", [])

def format_table(headers, rows):
    if not rows:
        return ""
    all_rows = [headers] + rows
    widths = [max(len(str(row[i])) for row in all_rows) for i in range(len(headers))]
    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    def fmt_row(row):
        cells = [f" {str(row[i]):<{widths[i]}} " for i in range(len(row))]
        return "|" + "|".join(cells) + "|"
    lines = [sep, fmt_row(headers), sep]
    for row in rows:
        lines.append(fmt_row(row))
    lines.append(sep)
    return "\n".join(lines)

def get_phone(name):
    """
    根据姓名从 dutyList.csv 中获取电话号码。
    支持姓名映射：若表格中姓名为 'nicole'，则实际查找 'nicole lai'。
    """
    # 姓名映射字典：键为表格中的名字（小写），值为 CSV 中对应的名字（原样）
    name_mapping = {
        "nicole": "nicole lai",
        # 可在此添加其他映射
    }
    # 根据映射获取实际用于查找的名字（不改变原始显示名）
    lookup_name = name_mapping.get(name.lower(), name)

    if not os.path.exists(DUTY_LIST_PATH):
        return "未找到电话号码文件"
    try:
        with open(DUTY_LIST_PATH, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3 and row[0].strip().lower() == lookup_name.lower():
                    return row[2].strip()
    except Exception:
        pass
    return "未找到电话号码"

def get_month_duty_map(year, month):
    """
    Returns a dictionary: day -> list of duty names for the given month.
    Returns None if data cannot be retrieved or month not found.
    """
    month_abbr = None
    for abbr, m in MONTH_ABBR.items():
        if m == month:
            month_abbr = abbr
            break
    if not month_abbr:
        return None

    try:
        token = get_tenant_access_token()
    except Exception:
        return None

    props = get_sheet_metadata(token, SPREADSHEET_TOKEN, SHEET_ID)
    if not props:
        return None
    max_row = props.get("rowCount", 200)
    max_col = props.get("columnCount", 26)

    # Build full data range
    max_col_letter = col_index_to_letter(max_col)
    scan_range = f"A1:{max_col_letter}{max_row}"
    values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, scan_range)
    if values is None:
        return None

    # Locate the month header ("日期 - Month")
    header_row = None
    header_col = None
    for r, row in enumerate(values):
        for c, cell in enumerate(row):
            if isinstance(cell, str) and cell.startswith("日期 - "):
                abbr = cell[4:].strip()
                if abbr == month_abbr:
                    header_row = r
                    header_col = c
                    break
        if header_row is not None:
            break

    if header_row is None:
        return None

    # Find date columns (cells containing day numbers)
    date_start_col = header_col + 1
    date_end_col = date_start_col
    for c in range(date_start_col, len(values[header_row])):
        cell = values[header_row][c]
        if str(cell).strip().isdigit():
            date_end_col = c
        else:
            break

    _, max_days = monthrange(year, month)
    num_date_cols = date_end_col - date_start_col + 1
    if num_date_cols > max_days:
        num_date_cols = max_days
        date_end_col = date_start_col + max_days - 1

    # Find name rows (people)
    name_col_idx = header_col
    name_start_row = header_row + 1
    name_end_row = name_start_row
    for r in range(name_start_row, len(values)):
        cell = values[r][name_col_idx] if name_col_idx < len(values[r]) else None
        if cell and str(cell).strip():
            name_end_row = r
        else:
            break

    data_top_row = name_start_row
    data_bottom_row = name_end_row
    data_left_col = date_start_col
    data_right_col = date_end_col

    if data_top_row > data_bottom_row or data_left_col > data_right_col:
        return None

    # Read the duty cells
    start_col_letter = col_index_to_letter(data_left_col + 1)
    end_col_letter = col_index_to_letter(data_right_col + 1)
    start_cell = f"{start_col_letter}{data_top_row+1}"
    end_cell = f"{end_col_letter}{data_bottom_row+1}"
    region_range = f"{start_cell}:{end_cell}"
    region_values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, region_range)
    if region_values is None:
        return None

    # Extract day numbers from the header row
    date_row_values = values[header_row][date_start_col:date_end_col+1]
    date_numbers = []
    for cell in date_row_values:
        try:
            date_numbers.append(int(cell))
        except:
            date_numbers.append(None)

    # Build mapping: day → list of duty names
    duty_map = {}
    for i in range(len(region_values)):
        name_cell = values[name_start_row + i][name_col_idx] if (name_start_row + i) < len(values) else None
        if not name_cell:
            continue
        name = str(name_cell).strip()
        if not name:
            continue
        for j in range(len(region_values[i])):
            cell_value = region_values[i][j]
            if cell_value and str(cell_value).strip():
                date_num = date_numbers[j] if j < len(date_numbers) else None
                if date_num is not None:
                    duty_map.setdefault(date_num, []).append(name)
    return duty_map

def get_fpms_today_duty():
    """Return FPMS schedule for today, tomorrow, and the day after tomorrow."""
    today = datetime.now().date()
    current_year = today.year
    current_month = today.month

    duty_map = get_month_duty_map(current_year, current_month)
    if duty_map is None:
        return "无法获取当月值班数据，请检查表格结构。"

    output_lines = []
    for offset in range(3):
        target_date = today + timedelta(days=offset)
        if target_date.year == current_year and target_date.month == current_month:
            day_num = target_date.day
            duty_names = duty_map.get(day_num, [])
        else:
            duty_names = []  # no data for other months

        date_str = target_date.strftime("%B %d, %Y %A")
        header = f"📅 FPMS Schedule - {date_str}"
        output_lines.append(header)
        if duty_names:
            for name in duty_names:
                phone = get_phone(name)
                output_lines.append(f"• {name}  (Phone: {phone})")
        else:
            output_lines.append("• No duty")
        output_lines.append("")  # blank line separator

    return "\n".join(output_lines).strip()

def fpms_check(month=None, year=None):
    """
    Check whether any day in the given month (default current month) has no duty.
    Returns a string listing missing dates or a confirmation that all days are covered.
    """
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month

    duty_map = get_month_duty_map(year, month)
    if duty_map is None:
        return f"❌ 无法获取 {year}年{month}月 的值班数据，请确认表格中存在该月份。"

    # Get all days in month
    _, max_days = monthrange(year, month)
    missing = []
    for day in range(1, max_days + 1):
        if day not in duty_map:
            missing.append(day)

    # Format output
    month_name = datetime(year, month, 1).strftime("%B %Y")
    if not missing:
        return f"✅ All days in {month_name} have duty assigned."
    else:
        missing_str = ", ".join(str(d) for d in missing)
        return f"⚠️ {month_name} 缺少值班的日期：{missing_str}"

if __name__ == "__main__":
    print("=== Today's Duty ===")
    print(get_fpms_today_duty())
    print("\n=== Month Check ===")
    print(fpms_check())