import csv
import requests
from datetime import datetime, timedelta
import os
import re
import difflib
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

# 预加载 duty list 缓存
_duty_cache = None

def _load_duty_cache():
    global _duty_cache
    if _duty_cache is not None:
        return _duty_cache
    _duty_cache = {}
    if not os.path.exists(DUTY_LIST_PATH):
        print(f"⚠️ {DUTY_LIST_PATH} not found")
        return _duty_cache
    try:
        with open(DUTY_LIST_PATH, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3:
                    name = row[0].strip()
                    dept = row[1].strip()
                    phone = row[2].strip()
                    # 只保留部门为 FPMS 的记录（根据您的 CSV 格式）
                    if dept.upper() == "FPMS":
                        _duty_cache[name] = phone
                    # 如果 CSV 中没有部门列或部门不是 FPMS，可根据需要调整
    except Exception as e:
        print(f"⚠️ 加载 {DUTY_LIST_PATH} 失败: {e}")
    return _duty_cache

def normalize_name(name):
    """标准化姓名：去除多余空格、括号及括号内容、常见头衔、转为小写、去除标点"""
    if not name:
        return ""
    # 移除括号及其内容（如 "(Manager)"）
    name = re.sub(r'\([^)]*\)', '', name)
    # 移除常见的头衔（例如 "Team Lead"、"Manager"）
    name = re.sub(r'\s*(Team\s*Lead|Manager)\s*', '', name, flags=re.IGNORECASE)
    # 去除空格和标点符号（保留字母数字和中文字符）
    name = re.sub(r'[^\w\u4e00-\u9fff]', '', name).lower()
    return name

def get_phone(name):
    """
    根据姓名从 dutyList.csv 中获取电话号码，仅匹配部门为 FPMS 的记录。
    支持精确匹配、标准化匹配、子串匹配和模糊匹配。
    """
    if not name:
        return "未找到电话号码"
    cache = _load_duty_cache()
    if not cache:
        return "未找到电话号码文件或无FPMS记录"

    # 1. 精确匹配（原样）
    if name in cache:
        return cache[name]

    # 2. 标准化匹配（去除括号、头衔、空格、标点）
    norm_name = normalize_name(name)
    for cached_name, phone in cache.items():
        if normalize_name(cached_name) == norm_name:
            return phone

    # 3. 子串匹配（例如 "Mark" 匹配 "Mark Paulo"）
    name_clean = re.sub(r'[^\w\u4e00-\u9fff]', '', name).lower()
    for cached_name, phone in cache.items():
        cached_clean = re.sub(r'[^\w\u4e00-\u9fff]', '', cached_name).lower()
        if name_clean in cached_clean or cached_clean in name_clean:
            return phone

    # 4. 模糊匹配（使用 difflib）
    best_ratio = 0.65
    best_match = None
    for cached_name in cache.keys():
        ratio = difflib.SequenceMatcher(None, norm_name, normalize_name(cached_name)).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = cached_name
    if best_match:
        return cache[best_match]

    return "未找到电话号码"

# 以下原有函数保持不变（col_index_to_letter, get_tenant_access_token, get_sheet_metadata, get_range_values, format_table, get_month_duty_map, get_fpms_today_duty, fpms_check）
# 注意：format_table 在 fpms_duty.py 中未被使用，但保留无妨。

def col_index_to_letter(col_index):
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

def get_month_duty_map(year, month):
    """返回字典 {day: [姓名列表]}，若获取失败返回 None"""
    month_abbr = None
    for abbr, m in MONTH_ABBR.items():
        if m == month:
            month_abbr = abbr
            break
    if not month_abbr:
        return None

    full_month = datetime(year, month, 1).strftime("%B")

    try:
        token = get_tenant_access_token()
    except Exception:
        return None

    props = get_sheet_metadata(token, SPREADSHEET_TOKEN, SHEET_ID)
    if not props:
        return None
    max_row = props.get("rowCount", 200)
    max_col = props.get("columnCount", 26)

    max_col_letter = col_index_to_letter(max_col)
    scan_range = f"A1:{max_col_letter}{max_row}"
    values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, scan_range)
    if values is None:
        return None

    # 定位月份表头
    header_row = None
    header_col = None
    for r, row in enumerate(values):
        for c, cell in enumerate(row):
            if isinstance(cell, str) and cell.startswith("日期 - "):
                suffix = cell[4:].strip()
                if suffix == month_abbr or suffix == full_month:
                    header_row = r
                    header_col = c
                    break
        if header_row is not None:
            break
    if header_row is None:
        return None

    # 查找日期列
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

    # 查找人员行
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

    start_col_letter = col_index_to_letter(data_left_col + 1)
    end_col_letter = col_index_to_letter(data_right_col + 1)
    start_cell = f"{start_col_letter}{data_top_row+1}"
    end_cell = f"{end_col_letter}{data_bottom_row+1}"
    region_range = f"{start_cell}:{end_cell}"
    region_values = get_range_values(token, SPREADSHEET_TOKEN, SHEET_ID, region_range)
    if region_values is None:
        return None

    # 提取日期数字
    date_row_values = values[header_row][date_start_col:date_end_col+1]
    date_numbers = []
    for cell in date_row_values:
        try:
            date_numbers.append(int(cell))
        except:
            date_numbers.append(None)

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
    today = datetime.now().date()
    output_lines = []
    month_cache = {}

    for offset in range(3):
        target_date = today + timedelta(days=offset)
        year = target_date.year
        month = target_date.month
        day = target_date.day

        cache_key = (year, month)
        if cache_key not in month_cache:
            month_cache[cache_key] = get_month_duty_map(year, month)
        duty_map = month_cache.get(cache_key)

        if duty_map is None:
            duty_names = []
        else:
            duty_names = duty_map.get(day, [])

        date_str = target_date.strftime("%B %d, %Y %A")
        header = f"📅 <b>FPMS Schedule - {date_str}</b>"
        output_lines.append(header)

        if duty_names:
            for name in duty_names:
                phone = get_phone(name)
                output_lines.append(f"• {name}  📞 {phone}")
        else:
            output_lines.append("• No duty")

        output_lines.append("")
    return "\n".join(output_lines).strip()

def fpms_check(month=None, year=None):
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month

    duty_map = get_month_duty_map(year, month)
    if duty_map is None:
        return f"❌ 无法获取 {year}年{month}月 的值班数据，请确认表格中存在该月份。"

    _, max_days = monthrange(year, month)
    missing = []
    for day in range(1, max_days + 1):
        if day not in duty_map:
            missing.append(day)

    month_name = datetime(year, month, 1).strftime("%B %Y")
    if not missing:
        return f"✅ All days in {month_name} have duty assigned."
    else:
        missing_str = ", ".join(str(d) for d in missing)
        return f"⚠️ {month_name} 缺少值班的日期：{missing_str}"
    
def fpmsp0():
    """返回未来三天的 FPMS 值班信息，并附加固定的 P0 额外联系人"""
    today = datetime.now().date()
    output_lines = []
    month_cache = {}

    for offset in range(3):
        target_date = today + timedelta(days=offset)
        year = target_date.year
        month = target_date.month
        day = target_date.day

        cache_key = (year, month)
        if cache_key not in month_cache:
            month_cache[cache_key] = get_month_duty_map(year, month)
        duty_map = month_cache.get(cache_key)

        if duty_map is None:
            duty_names = []
        else:
            duty_names = duty_map.get(day, [])

        date_str = target_date.strftime("%B %d, %Y %A")
        header = f"📅 <b>FPMS Schedule - {date_str}</b>"
        output_lines.append(header)

        if duty_names:
            for name in duty_names:
                phone = get_phone(name)
                output_lines.append(f"• {name}  📞 {phone}")
        else:
            output_lines.append("• No duty")

        output_lines.append("")   # 空行分隔

    # 添加固定联系人列表
    output_lines.append("<b>Addtional Contact number for P0</b>")
    output_lines.append("• David 📞 60102703549")
    output_lines.append("• Olivia 📞 60163661007")
    output_lines.append("• Eason 📞 60129687432")
    output_lines.append("• Lim Lian Cheng 📞 60196549698")
    output_lines.append("• Yang 📞 60168109045")
    output_lines.append("• BK 📞 601133798396")
    output_lines.append("• Hui Min 📞 01125808539")

    return "\n".join(output_lines).strip()

if __name__ == "__main__":
    print("=== Today's Duty ===")
    print(get_fpms_today_duty())
    print("\n=== Month Check ===")
    print(fpms_check())