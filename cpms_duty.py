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
        # 移除可能存在的富文本标记，但保留基本文本
        text = ''.join(parts)
        # 清理多余的空白字符
        return re.sub(r'\s+', ' ', text).strip()
    return str(cell)

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
    for day in range(1, days_in_month + 1):
        target_date = datetime(year, month, day).date()
        try:
            _, main_name, _, backup_name, _ = get_cpms_duty_for_date(target_date)
            # 如果主值班人和备份值班人都为空，则视为缺失
            if not main_name and not backup_name:
                missing.append(day)
        except Exception:
            # 例如找不到对应月份的工作表，也视为缺失
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
    year = target_date.year
    month = target_date.month
    day = target_date.day
    weekday = target_date.strftime("%A")  # 英文星期几，如 Monday
    
    token = get_tenant_access_token()
    sheets = get_sheet_list(token)
    print("Sheets found:", [s.get("title") for s in sheets])

    try:
        token = get_tenant_access_token()
    except Exception as e:
        raise Exception(f"获取访问令牌失败: {e}")

    # 1. 找到对应月份的工作表
    sheet_id = get_sheet_id_for_month(token, year, month)
    if not sheet_id:
        raise Exception(f"未找到 {month:02d}-{year} 的工作表")

    # 2. 读取整个工作表的数据（假设最多到100行）
    values = get_range_values(token, sheet_id, "A1:C100")
    if not values:
        raise Exception("无法读取工作表数据")

    # 3. 在列A中查找星期几所在的行
    main_name, main_phone, backup_name, backup_phone = "", "", "", ""
    for row_idx, row in enumerate(values):
        if not row:
            continue
        day_cell = extract_text_from_cell(row[0]) if len(row) > 0 else ""
        if day_cell == weekday:
            # 找到了对应的行
            if len(row) > 1:  # 列B
                main_name, main_phone = parse_person_info(extract_text_from_cell(row[1]))
            if len(row) > 2:  # 列C
                backup_name, backup_phone = parse_person_info(extract_text_from_cell(row[2]))
            break

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