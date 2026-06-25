#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
import requests
from datetime import datetime, timedelta
import os
import re
from dotenv import load_dotenv
load_dotenv()
# ================= 配置信息 =================
from dotenv import load_dotenv
load_dotenv()
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
SPREADSHEET_TOKEN = os.getenv("PMS_SPREADSHEET_TOKEN")

DUTY_LIST_PATH = "dutyList.csv"   # 统一电话号码参考文件

# 列定义
COL_START = "C"      # 开始日期
COL_END = "D"        # 结束日期
COL_FIRST = "E"      # First Level 值班人
COL_SECOND = "F"     # Second Level 值班人
COL_FINAL = "G"      # Final Level 值班人 (可包含 "/" 分隔多个名字)

DATA_START_ROW = 2   # 数据起始行（第1行为标题）

# ================= 姓名映射字典 =================
# 键：表格中出现的原始姓名 → 值：dutyList.csv 中用于查询的标准姓名
QUERY_NAME_MAPPING = {
    # 原始姓名（可能为短名或别名） -> 标准姓名（与 CSV 一致）
    "Alviss": "Alvis",
    "Bien Dave Magno": "Bien",
    "Darren": "Darren",
    "Manuel Lorenzo Pereira": "Lorenz",
    "Ramel John Brillantes": "Ramel",
    "Ray Ranil Quijada": "Ray",
    "Lucrisha Mae Siquijor": "Lucrisha",
    "Larrie Adriano": "Larrie",
    "Vince Harresh Rodriguez": "Vince",
    "Ronjon Nuno": "Ron",
    # 短名到全名的映射（如果表格中出现短名）
    "Ramel": "Ramel John Brillantes",
    "Lucrisha": "Lucrisha Mae Siquijor",
    "Ray": "Ray Ranil Quijada",
    "Larrie": "Larrie Adriano",
    "Vince": "Vince Harresh Rodriguez",
    "Ron": "Ronjon Nuno",
    "Bien": "Bien Dave Magno",
    "Lorenz": "Manuel Lorenzo Pereira",
}

# ================= 工具函数 =================
def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"获取 token 失败: {result}")
    return result["tenant_access_token"]

def get_sheet_list(token):
    """获取电子表格下所有工作表的列表"""
    url = f"https://open.larksuite.com/open-apis/sheets/v3/spreadsheets/{SPREADSHEET_TOKEN}/sheets/query"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        print(f"获取工作表列表失败: {result}")
        return []
    return result.get("data", {}).get("sheets", [])

def get_year_from_title(title):
    """从工作表标题中提取年份（假设标题包含4位数字年份）"""
    match = re.search(r'\b(20\d{2})\b', title)
    if match:
        return int(match.group(1))
    return None

def find_sheet_for_year(token, target_year):
    """查找包含指定年份的工作表，返回 (sheet_id, sheet_title) 或 (None, None)"""
    sheets = get_sheet_list(token)
    for sheet in sheets:
        title = sheet.get("title", "")
        year = get_year_from_title(title)
        if year == target_year:
            return sheet.get("sheet_id"), title
    return None, None

def get_range_values(token, sheet_id, range_str):
    """读取指定范围的值，返回二维列表"""
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{sheet_id}!{range_str}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        print(f"范围 {range_str} 请求失败: {result}")
        return None
    return result.get("data", {}).get("valueRange", {}).get("values", [])

def get_phone_from_dutylist(name):
    """从 dutyList.csv 查询电话号码"""
    if not os.path.exists(DUTY_LIST_PATH):
        return "未找到电话号码"
    try:
        with open(DUTY_LIST_PATH, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3 and row[0].strip() == name:
                    return row[2].strip()
    except Exception:
        pass
    return "未找到电话号码"

def parse_date_cell(date_cell, target_year):
    """
    解析日期单元格，返回一个 date 对象，其年份强制设为 target_year。
    支持 Excel 序列号和常见字符串格式。
    如果遇到 2 月 29 日而 target_year 不是闰年，则自动降级为 2 月 28 日。
    """
    if not date_cell:
        return None

    def make_date(month, day):
        try:
            return datetime(target_year, month, day).date()
        except ValueError:
            if month == 2 and day == 29:
                return datetime(target_year, 2, 28).date()
            else:
                return None

    # 处理 Excel 序列号
    try:
        if isinstance(date_cell, (int, float)):
            excel_val = float(date_cell)
        else:
            cleaned = str(date_cell).strip()
            excel_val = float(cleaned)

        base = datetime(1899, 12, 30)
        dt = base + timedelta(days=excel_val)
        return make_date(dt.month, dt.day)
    except (ValueError, TypeError):
        pass

    # 处理字符串格式
    date_str = str(date_cell).strip()
    patterns = [
        r'^(\d{1,2})[/-](\d{1,2})$',
        r'^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$',
        r'^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$',
        r'^(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?$',
    ]
    for pat in patterns:
        m = re.search(pat, date_str)
        if m:
            groups = m.groups()
            if len(groups) == 2:
                day, month = int(groups[0]), int(groups[1])
                return make_date(month, day)
            elif len(groups) == 3 and groups[2] is not None:
                if pat.startswith(r'^(\d{4})'):
                    y, m, d = int(groups[0]), int(groups[1]), int(groups[2])
                else:
                    d, m, y = int(groups[0]), int(groups[1]), int(groups[2])
                return make_date(m, d)
    return None

def format_table(headers, rows):
    """生成网格视图表格"""
    if not rows:
        return ""
    all_rows = [headers] + rows
    widths = []
    for col in range(len(headers)):
        max_len = max(len(str(row[col])) for row in all_rows)
        widths.append(max_len)
    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    def format_row(row):
        cells = [f" {str(row[i]):<{widths[i]}} " for i in range(len(row))]
        return "|" + "|".join(cells) + "|"
    lines = [sep, format_row(headers), sep]
    for row in rows:
        lines.append(format_row(row))
    lines.append(sep)
    return "\n".join(lines)

# ================= 内部辅助函数 =================
def _fetch_sheet_data(year):
    """获取指定年份工作表的数据行（C列到G列）"""
    try:
        token = get_tenant_access_token()
    except Exception as e:
        raise Exception(f"获取访问令牌失败: {e}")

    sheet_id, sheet_title = find_sheet_for_year(token, year)
    if not sheet_id:
        raise Exception(f"未找到 {year} 年的工作表，请确认表格中已创建（例如标题包含 {year}）。")

    max_rows = 200
    start_row = DATA_START_ROW
    end_row = start_row + max_rows - 1
    range_str = f"{COL_START}{start_row}:{COL_FINAL}{end_row}"
    values = get_range_values(token, sheet_id, range_str)
    if values is None:
        raise Exception("无法读取表格数据，请稍后重试。")
    return values, year

def _parse_duty_row(row, target_year):
    """解析单行，返回 (start_date, end_date, first_name, second_name, final_names)"""
    if len(row) < 5:
        return None, None, None, None, []
    start_cell, end_cell, first_cell, second_cell, final_cell = row[:5]
    start_date = parse_date_cell(start_cell, target_year)
    end_date = parse_date_cell(end_cell, target_year)
    if not start_date or not end_date:
        return None, None, None, None, []
    first_name = str(first_cell).strip() if first_cell else ""
    second_name = str(second_cell).strip() if second_cell else ""
    final_names = []
    if final_cell and str(final_cell).strip():
        final_names = [n.strip() for n in str(final_cell).split('/') if n.strip()]
    return start_date, end_date, first_name, second_name, final_names

def _build_level_table(level_name, names_list):
    """根据级别名称和名字列表生成表格字符串（含表头）"""
    if not names_list:
        return ""
    lines = [f"\n{level_name}"]
    rows = []
    for name in names_list:
        query_name = QUERY_NAME_MAPPING.get(name, name)
        phone = get_phone_from_dutylist(query_name)
        rows.append([name, phone])
    lines.append(format_table(["Duty name", "Phone number"], rows))
    return "\n".join(lines)

# ================= 对外接口 =================
def dutyToday():
    """返回今天的值班信息（字符串）"""
    today = datetime.now().date()
    year = today.year

    try:
        values, target_year = _fetch_sheet_data(year)
    except Exception as e:
        return f"❌ {e}"

    first_level = []
    second_level = []
    final_level = []

    for row in values:
        start_date, end_date, first_name, second_name, final_names = _parse_duty_row(row, target_year)
        if not start_date or not end_date:
            continue
        if start_date <= today <= end_date:
            if first_name:
                first_level.append(first_name)
            if second_name:
                second_level.append(second_name)
            if final_names:
                final_level.extend(final_names)

    if not (first_level or second_level or final_level):
        return f"📅 今天 ({today.strftime('%Y/%m/%d')}) 没有安排 PMS 值班人员。"

    lines = [f"Today Date: {today.strftime('%Y/%m/%d')}"]
    if first_level:
        lines.append(_build_level_table("First Level", first_level))
    if second_level:
        lines.append(_build_level_table("Second Level", second_level))
    if final_level:
        lines.append(_build_level_table("Final Level", final_level))

    return "\n".join(lines)

def _format_level_section(level_name, names_list):
    """格式化单个级别的人员列表，返回多行字符串"""
    if not names_list:
        return ""
    lines = [f"{level_name}:"]
    for name in names_list:
        query_name = QUERY_NAME_MAPPING.get(name, name)
        phone = get_phone_from_dutylist(query_name)
        lines.append(f"• {name}  📞 {phone}")
    return "\n".join(lines)


def _find_duty_for_date(values, target_year, target_date):
    """返回 target_date 所在排班周：(start, end, first, second, final_names)。"""
    for row in values:
        start_date, end_date, first_name, second_name, final_names = _parse_duty_row(row, target_year)
        if start_date and end_date and start_date <= target_date <= end_date:
            return start_date, end_date, first_name, second_name, final_names
    return None, None, "", "", []


def _format_week_duty(start_date, end_date, first_name, second_name, final_names):
    """格式化单周值班（一周内 First/Second/Final 相同）。"""
    if start_date.year == end_date.year:
        if start_date.month == end_date.month:
            date_str = f"{start_date.strftime('%B %d')} - {end_date.strftime('%d, %Y')}"
        else:
            date_str = f"{start_date.strftime('%B %d')} - {end_date.strftime('%B %d, %Y')}"
    else:
        date_str = f"{start_date.strftime('%B %d, %Y')} - {end_date.strftime('%B %d, %Y')}"

    lines = [f"📅 PMS Schedule - {date_str}"]

    if not (first_name or second_name or final_names):
        lines.append("No duty")
        return "\n".join(lines)

    if first_name:
        lines.append(_format_level_section("First Level", [first_name]))
    if second_name:
        lines.append(_format_level_section("Second Level", [second_name]))
    if final_names:
        lines.append(_format_level_section("Final Level", final_names))

    return "\n".join(lines)


def _sorted_weeks(values, target_year):
    """从表格行解析并返回按开始日期排序的排班周列表。"""
    weeks = []
    for row in values:
        start_date, end_date, first_name, second_name, final_names = _parse_duty_row(row, target_year)
        if start_date and end_date:
            weeks.append((start_date, end_date, first_name, second_name, final_names))
    weeks.sort(key=lambda w: w[0])
    return weeks


def _weeks_from_today(values, target_year, today, count=3):
    """返回从今天所在周开始的 count 个排班周。"""
    weeks = _sorted_weeks(values, target_year)
    start_idx = next(
        (i for i, (start, end, *_rest) in enumerate(weeks) if start <= today <= end),
        None,
    )
    if start_idx is None:
        return []

    selected = weeks[start_idx:start_idx + count]
    if len(selected) >= count:
        return selected

    try:
        next_values, next_year = _fetch_sheet_data(target_year + 1)
        selected.extend(_sorted_weeks(next_values, next_year)[: count - len(selected)])
    except Exception:
        pass
    return selected


def dutyNextDay():
    """返回从今天起连续三周的值班信息（每周一块）。"""
    today = datetime.now().date()
    year = today.year

    try:
        values, target_year = _fetch_sheet_data(year)
    except Exception as e:
        return f"❌ {e}"

    weeks = _weeks_from_today(values, target_year, today, count=3)

    if not weeks:
        return f"📅 No PMS duty scheduled for the week of {today.strftime('%B %d, %Y')}."

    blocks = []
    for start_date, end_date, first_name, second_name, final_names in weeks:
        blocks.append(_format_week_duty(start_date, end_date, first_name, second_name, final_names))

    return "\n\n".join(blocks)

def pmsCheck(month=None, year=None):
    """
    Check whether any day in the given month (default current month) has no duty.
    Returns a string listing missing dates or a confirmation that all days are covered.
    """
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month

    try:
        values, target_year = _fetch_sheet_data(year)
    except Exception as e:
        return f"❌ {e}"

    # Ensure the fetched year matches the target year
    if target_year != year:
        return f"❌ Cannot find sheet for {year}. Found sheet for {target_year}."

    # Get number of days in the month
    from calendar import monthrange
    num_days = monthrange(year, month)[1]
    days_in_month = [datetime(year, month, day).date() for day in range(1, num_days+1)]

    # For each day, accumulate duties (First, Second, Final)
    duties_by_day = {}
    for day in days_in_month:
        duties_by_day[day] = {"first": set(), "second": set(), "final": set()}

    for row in values:
        start_date, end_date, first_name, second_name, final_names = _parse_duty_row(row, target_year)
        if not start_date or not end_date:
            continue
        # For each day in the range, add the duties
        current = start_date
        while current <= end_date:
            if current in duties_by_day:
                if first_name:
                    duties_by_day[current]["first"].add(first_name)
                if second_name:
                    duties_by_day[current]["second"].add(second_name)
                for fn in final_names:
                    duties_by_day[current]["final"].add(fn)
            current += timedelta(days=1)

    # Find days with no duty at all
    missing = []
    for day, duties in duties_by_day.items():
        if not duties["first"] and not duties["second"] and not duties["final"]:
            missing.append(day)

    # Format output
    if not missing:
        return f"✅ All days in {datetime(year, month, 1).strftime('%B %Y')} have duty assigned."
    else:
        missing_str = ", ".join(d.strftime('%Y-%m-%d') for d in missing)
        return f"⚠️ Missing duty on: {missing_str}"

# ================= 测试入口 =================
if __name__ == "__main__":
    print("=== Today's Duty ===")
    print(dutyToday())
    print("\n=== Next Duty ===")
    print(dutyNextDay())
