#!/usr/bin/env python3
"""
CPMS Duty Schedule – Next Three Days Duty

Usage:
    ./cpms_duty.py                # shows today, tomorrow, next day duty
    ./cpms_duty.py YYYY-MM-DD     # shows duty for the three days starting from specified date

This script reads a specific Lark sheet containing CPMS duty schedules.
Sheet names should be like '03-2026' (MM-YYYY).
Column A: Day of week (Monday, Tuesday...)
Column B: Main duty person (format: "Name\\nphone & whatapp: number")
Column C: Backup duty person (same format)
"""

import re
import sys
import time
import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
load_dotenv()
# ================= Configuration =================

# !! 请务必替换为您的实际凭证 !!
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
SPREADSHEET_TOKEN = os.getenv("CPMS_SPREADSHEET_TOKEN")

_WEEKDAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
_month_duty_cache: dict[tuple[int, int], tuple[float, dict[str, tuple[str, str, str, str]]]] = {}
try:
    _MONTH_CACHE_SEC = max(0, int((os.getenv("CPMS_DUTY_CACHE_SEC") or "300").strip() or "300"))
except ValueError:
    _MONTH_CACHE_SEC = 300

# ================= Helper Functions =================
def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to get token: {result}")
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

def get_sheet_id_for_month(token, target_year, target_month):
    """根据年月查找对应的工作表ID，工作表名称格式应为 MM-YYYY (如 03-2026)"""
    sheets = get_sheet_list(token)
    target_str = f"{target_month:02d}-{target_year}"
    for sheet in sheets:
        title = sheet.get("title", "")
        if title.strip() == target_str:
            return sheet.get("sheet_id")
    return None

def get_range_values(token, sheet_id, range_str):
    """读取指定范围的值"""
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{sheet_id}!{range_str}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        print(f"读取范围 {range_str} 失败: {result}")
        return None
    return result.get("data", {}).get("valueRange", {}).get("values", [])

def extract_text_from_cell(cell):
    """将单元格内容（可能为富文本列表）转换为纯文本字符串，保留换行以便解析姓名/电话。"""
    if cell is None:
        return ""
    if isinstance(cell, str):
        return cell.strip()
    if isinstance(cell, list):
        parts = []
        for item in cell:
            if isinstance(item, dict) and "text" in item:
                parts.append(item["text"])
            elif isinstance(item, str):
                parts.append(item)
        return "\n".join(parts).strip()
    return str(cell).strip()


def _normalize_weekday_label(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    low = raw.lower()
    for day in _WEEKDAYS:
        if low == day.lower():
            return day
    return raw


def _parse_cpms_weekday_rows(values) -> dict[str, tuple[str, str, str, str]]:
    """
    从当月工作表 A:C 解析星期一到星期日的 CPMS / Backup 姓名与电话。
    表头行（如 Days of the Week）会被跳过；仅匹配 Monday…Sunday。
    """
    weekday_map: dict[str, tuple[str, str, str, str]] = {}
    if not values:
        return weekday_map
    for row in values:
        if not row:
            continue
        day_cell = _normalize_weekday_label(extract_text_from_cell(row[0]) if len(row) > 0 else "")
        if day_cell not in _WEEKDAYS:
            continue
        main_name, main_phone = parse_person_info(extract_text_from_cell(row[1]) if len(row) > 1 else "")
        backup_name, backup_phone = parse_person_info(extract_text_from_cell(row[2]) if len(row) > 2 else "")
        weekday_map[day_cell] = (main_name, main_phone, backup_name, backup_phone)
    return weekday_map


def get_cpms_weekday_duty_map(year: int, month: int) -> dict[str, tuple[str, str, str, str]]:
    """
    读取 ``MM-YYYY`` 工作表一次，返回 Monday…Sunday → (main, main_phone, backup, backup_phone)。
    结果按 ``CPMS_DUTY_CACHE_SEC``（默认 300 秒）缓存，避免整月/多日查询重复打 Lark API。
    """
    key = (year, month)
    now = time.time()
    if _MONTH_CACHE_SEC > 0:
        cached = _month_duty_cache.get(key)
        if cached and now - cached[0] < _MONTH_CACHE_SEC:
            return cached[1]

    token = get_tenant_access_token()
    sheet_id = get_sheet_id_for_month(token, year, month)
    if not sheet_id:
        raise Exception(f"未找到 {month:02d}-{year} 的工作表")

    values = get_range_values(token, sheet_id, "A1:C100")
    if values is None:
        raise Exception("无法读取工作表数据")

    weekday_map = _parse_cpms_weekday_rows(values)
    if _MONTH_CACHE_SEC > 0:
        _month_duty_cache[key] = (now, weekday_map)
    return weekday_map


def invalidate_cpms_duty_cache() -> None:
    """清空按月值班缓存（测试或强制刷新时调用）。"""
    _month_duty_cache.clear()

def parse_person_info(cell_text):
    """
    从单元格文本中解析姓名和电话。
    预期格式: "姓名\nphone & whatapp: 电话号码"
    返回: (姓名, 电话号码)
    """
    if not cell_text:
        return "", ""
    # 按换行分割
    lines = cell_text.strip().split('\n')
    if not lines:
        return "", ""
    name = lines[0].strip()
    # 查找包含 phone 的行
    phone = ""
    for line in lines[1:]:
        if 'phone' in line.lower() or 'whatsapp' in line.lower() or 'whatapp' in line.lower():
            # 提取数字部分（电话号码）
            phone_match = re.search(r'(\+?[\d\s-]+)$', line)
            if phone_match:
                phone = phone_match.group(1).strip().replace(' ', '')
            break
    return name, phone

def cpms_check(month=None, year=None):
    """
    检查指定月份（默认为当前月份）中是否有任何日期缺少值班。
    返回字符串，格式与 fpms_check 一致。
    """
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month

    # 计算该月的总天数
    first_day = datetime(year, month, 1).date()
    if month == 12:
        next_month_first = datetime(year + 1, 1, 1).date()
    else:
        next_month_first = datetime(year, month + 1, 1).date()
    days_in_month = (next_month_first - first_day).days

    missing = []
    try:
        weekday_map = get_cpms_weekday_duty_map(year, month)
    except Exception:
        weekday_map = {}

    for day in range(1, days_in_month + 1):
        target_date = datetime(year, month, day).date()
        weekday = target_date.strftime("%A")
        main_name, _, backup_name, _ = weekday_map.get(weekday, ("", "", "", ""))
        if not main_name and not backup_name:
            missing.append(day)

    month_name = datetime(year, month, 1).strftime("%B %Y")
    if not missing:
        return f"✅ All days in {month_name} have duty assigned."
    else:
        missing_str = ", ".join(str(d) for d in missing)
        return f"⚠️ {month_name} 缺少值班的日期：{missing_str}"

# ================= Core Logic =================
def get_cpms_duty_for_date(target_date):
    """
    返回指定日期 target_date 的 CPMS 值班信息。
    返回格式: (date_obj, main_name, main_phone, backup_name, backup_phone)
    """
    weekday = target_date.strftime("%A")
    weekday_map = get_cpms_weekday_duty_map(target_date.year, target_date.month)
    main_name, main_phone, backup_name, backup_phone = weekday_map.get(weekday, ("", "", "", ""))
    return target_date, main_name, main_phone, backup_name, backup_phone

def get_cpms_three_days(start_date=None):
    """获取连续三天的值班信息"""
    if start_date is None:
        start_date = datetime.now().date()

    results = []
    for i in range(3):
        target = start_date + timedelta(days=i)
        try:
            result = get_cpms_duty_for_date(target)
            results.append(result)
        except Exception as e:
            # 如果某天出错（如跨月找不到工作表），添加错误信息
            results.append((target, f"Error: {e}", "", "", ""))
    return results

def format_output(results):
    """格式化输出三天的值班信息"""
    lines = []
    for date_obj, main_name, main_phone, backup_name, backup_phone in results:
        date_str = date_obj.strftime("%B %d, %Y %A")
        lines.append(f"📅 CPMS Schedule - {date_str}")

        if main_name:
            lines.append(f"• {main_name}  📞 {main_phone}")
        else:
            lines.append("• No main duty assigned")

        if backup_name:
            lines.append(f"Backup :")
            lines.append(f"• {backup_name}  📞 {backup_phone}")
        else:
            lines.append("Backup :")
            lines.append("• No backup duty assigned")

        lines.append("")  # 空行分隔
    return "\n".join(lines)

# ================= Main =================
if __name__ == "__main__":
    start_date = None
    if len(sys.argv) > 1:
        try:
            start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        except ValueError:
            print("❌ 日期格式错误，请使用 YYYY-MM-DD 格式 (例如 2026-03-20)")
            sys.exit(1)

    try:
        three_days = get_cpms_three_days(start_date)
        print(format_output(three_days))
    except Exception as e:
        print(f"❌ 执行出错: {e}")
        sys.exit(1)