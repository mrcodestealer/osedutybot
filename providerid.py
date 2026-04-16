#!/usr/bin/env python3
"""
Provider Name Lookup (Simple)
Usage:
    /pid <id>   e.g. /pid 30
    <number>    (when mentioning bot) e.g. 30
"""

import re
import requests
import os
from dotenv import load_dotenv
load_dotenv()

# ================= Configuration =================
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
PROVIDER_SPREADSHEET_TOKEN = os.getenv("PROVIDER_SPREADSHEET_TOKEN")
PROVIDER_SHEET_ID = os.getenv("PROVIDER_SHEET_ID")

# ================= Lark API Helpers =================
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

def col_index_to_letter(col_index):
    letters = ''
    while col_index > 0:
        col_index -= 1
        letters = chr(65 + (col_index % 26)) + letters
        col_index //= 26
    return letters

def get_range_values(token, spreadsheet_token, sheet_id, range_str):
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_id}!{range_str}?valueRenderOption=FormattedValue"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        return None
    return result.get("data", {}).get("valueRange", {}).get("values", [])

def get_all_sheet_data(token, spreadsheet_token, sheet_id):
    props = get_sheet_metadata(token, spreadsheet_token, sheet_id)
    if not props:
        return None
    max_row = props.get("rowCount", 2000)
    max_col = props.get("columnCount", 200)
    end_col = col_index_to_letter(max_col)
    range_str = f"A1:{end_col}{max_row}"
    return get_range_values(token, spreadsheet_token, sheet_id, range_str)

def extract_cell_value(cell):
    if cell is None:
        return ""
    if isinstance(cell, (str, int, float)):
        return str(cell).strip()
    if isinstance(cell, list):
        parts = []
        for part in cell:
            if isinstance(part, dict):
                if "link" in part and part["link"]:
                    parts.append(part["link"])
                elif "text" in part:
                    parts.append(part["text"])
                else:
                    parts.append(str(part))
            else:
                parts.append(str(part))
        return "".join(parts).strip()
    return str(cell).strip()

def get_provider_info(query_str):
    """
    提取第一个数字作为前缀，在表格中查找所有以该前缀开头的 Provider ID。
    返回对应的 Provider ID 和 Provider Name。
    """
    # 提取数字（忽略任何前缀）
    numbers = re.findall(r'\b(\d+)\b', query_str)
    if not numbers:
        return "❌ No provider ID found. Usage: `/pid 30` or just `30`."

    prefix = numbers[0]   # 取第一个数字作为前缀

    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ Failed to get access token: {e}"

    data = get_all_sheet_data(token, PROVIDER_SPREADSHEET_TOKEN, PROVIDER_SHEET_ID)
    if not data:
        return "❌ Failed to read sheet data."

    # 定位表头行
    header_row = None
    for i, row in enumerate(data):
        if not row:
            continue
        for cell in row:
            if isinstance(cell, str) and "Provider ID" in cell:
                header_row = i
                break
        if header_row is not None:
            break

    if header_row is None:
        return "❌ Could not find header row with 'Provider ID'."

    headers = data[header_row]

    # 查找 Provider ID 和 Provider Name 列索引
    pid_col = None
    name_col = None
    for idx, cell in enumerate(headers):
        if cell and isinstance(cell, str):
            if "provider id" in cell.lower():
                pid_col = idx
            elif "provider name" in cell.lower():
                name_col = idx

    if pid_col is None:
        return "❌ 'Provider ID' column not found."

    # 收集所有前缀匹配的结果
    matches = []
    for row in data[header_row+1:]:
        if len(row) <= pid_col:
            continue
        pid_val = extract_cell_value(row[pid_col])
        if pid_val.startswith(prefix):   # 前缀匹配
            provider_name = ""
            if name_col is not None and len(row) > name_col:
                provider_name = extract_cell_value(row[name_col])
            matches.append((pid_val, provider_name))

    if not matches:
        return f"❌ No provider ID starting with '{prefix}' found."

    # 构建返回消息（多条结果）
    lines = []
    for pid, name in matches:
        lines.append(f"🎮 Provider ID: {pid}")
        lines.append(f"📛 Provider Name: {name}")
        lines.append("")   # 空行分隔

    return "\n".join(lines).strip()