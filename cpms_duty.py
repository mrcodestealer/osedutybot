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

# 工作表名称形如 "07-2026" 或 "7-2025"（月份可能不补零），匹配 (month, year)。
_SHEET_TITLE_RE = re.compile(r"^\s*(\d{1,2})-(\d{4})\s*$")


class CpmsSheetNotPublished(Exception):
    """当目标月份的工作表尚未创建时抛出，携带最新可用月份用于友好提示。"""

    def __init__(self, year: int, month: int, latest: tuple[int, int] | None = None):
        self.year = year
        self.month = month
        self.latest = latest  # (year, month) 或 None
        super().__init__(
            f"CPMS duty for {datetime(year, month, 1).strftime('%B %Y')} has not been published yet."
        )

    @property
    def friendly_message(self) -> str:
        target_human = datetime(self.year, self.month, 1).strftime("%B %Y")
        target_sheet = f"{self.month:02d}-{self.year}"
        text = (
            f"⚠️ CPMS duty for **{target_human}** hasn't been published yet "
            f"(sheet `{target_sheet}` not found)."
        )
        if self.latest:
            ly, lm = self.latest
            latest_human = datetime(ly, lm, 1).strftime("%B %Y")
            text += f"\nLatest available roster: **{latest_human}** (`{lm:02d}-{ly}`)."
        return text


def _parse_sheet_month(title: str) -> tuple[int, int] | None:
    """把工作表标题解析为 (year, month)；非 ``M-YYYY`` / ``MM-YYYY`` 格式返回 None。"""
    m = _SHEET_TITLE_RE.match(title or "")
    if not m:
        return None
    month = int(m.group(1))
    year = int(m.group(2))
    if 1 <= month <= 12:
        return (year, month)
    return None


def _latest_available_month(sheets) -> tuple[int, int] | None:
    """返回工作表中最新的 (year, month)，用于提示尚未发布时的最近可用月份。"""
    months = []
    for sheet in sheets or []:
        parsed = _parse_sheet_month(sheet.get("title", ""))
        if parsed:
            months.append(parsed)
    return max(months) if months else None

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

def get_sheet_id_for_month(token, target_year, target_month, sheets=None):
    """根据年月查找对应的工作表ID。

    工作表名称格式为 ``MM-YYYY`` 或 ``M-YYYY``（月份可能不补零，如 03-2026 / 7-2025），
    两种写法都能匹配。``sheets`` 可传入已获取的工作表列表以避免重复请求。
    """
    if sheets is None:
        sheets = get_sheet_list(token)
    for sheet in sheets:
        if _parse_sheet_month(sheet.get("title", "")) == (target_year, target_month):
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
    sheets = get_sheet_list(token)
    sheet_id = get_sheet_id_for_month(token, year, month, sheets=sheets)
    if not sheet_id:
        raise CpmsSheetNotPublished(year, month, _latest_available_month(sheets))

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
    except CpmsSheetNotPublished as e:
        return e.friendly_message
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
        except CpmsSheetNotPublished as e:
            # 该天所在月份的工作表尚未发布，保留异常以便渲染友好提示
            results.append((target, e))
        except Exception as e:
            # 如果某天出错（如临时 API 错误），添加错误信息
            results.append((target, f"Error: {e}", "", "", ""))
    return results


def get_cpms_three_days_text(start_date=None):
    """返回连续三天值班信息的格式化文本。

    当整段日期都落在同一个尚未发布的月份时，合并为一条“未发布”提示，避免重复三遍。
    """
    results = get_cpms_three_days(start_date)
    notes = [it for it in results if len(it) == 2 and isinstance(it[1], CpmsSheetNotPublished)]
    if notes and len(notes) == len(results):
        months = {(n[1].year, n[1].month) for n in notes}
        if len(months) == 1:
            return notes[0][1].friendly_message
    return format_output(results)


def format_output(results):
    """格式化输出三天的值班信息"""
    lines = []
    for item in results:
        date_obj = item[0]
        date_str = date_obj.strftime("%B %d, %Y %A")
        lines.append(f"📅 CPMS Schedule - {date_str}")

        # 该天所在月份尚未发布：渲染友好提示后跳过主/备解析
        if len(item) == 2 and isinstance(item[1], CpmsSheetNotPublished):
            lines.append(item[1].friendly_message)
            lines.append("")
            continue

        _, main_name, main_phone, backup_name, backup_phone = item

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
        print(get_cpms_three_days_text(start_date))
    except Exception as e:
        print(f"❌ 执行出错: {e}")
        sys.exit(1)