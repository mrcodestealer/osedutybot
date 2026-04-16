#!/usr/bin/env python3
"""
Provider & Game Lookup
触发方式：
    /pid <provider_id>     e.g. /pid 30
    /pid 30 31 32
    纯数字（@机器人后）        e.g. 30
"""

import re
import requests
import os
from dotenv import load_dotenv
load_dotenv()

# ================= 配置（从环境变量读取）=================
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
PROVIDER_SPREADSHEET_TOKEN = os.getenv("PROVIDER_SPREADSHEET_TOKEN")
PROVIDER_SHEET_ID = os.getenv("PROVIDER_SHEET_ID")

# 表格中的列名（请根据实际表格调整）
COLUMNS = {
    "Provider ID": "Provider ID",
    "Provider Name": "Provider Name",
    "Game ID": "Game ID",
}

# ================= Lark API 辅助函数 =================
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

# ================= 核心查询逻辑 =================
def find_provider_mappings(data, provider_id):
    """
    在表格数据中查找指定 Provider ID 对应的所有行，
    返回 provider_name 和 game_ids 列表
    """
    if not data:
        return None

    # 定位表头行（第一行包含 "Provider ID"）
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
        return None

    headers = data[header_row]

    # 查找各列索引
    col_indexes = {}
    for col_name in COLUMNS.values():
        col_indexes[col_name] = None
        for idx, cell in enumerate(headers):
            if cell and isinstance(cell, str) and col_name.lower() in cell.lower():
                col_indexes[col_name] = idx
                break

    if col_indexes["Provider ID"] is None:
        return None

    provider_name = None
    game_ids = []

    # 遍历数据行
    for row in data[header_row+1:]:
        if len(row) <= col_indexes["Provider ID"]:
            continue
        pid_cell = extract_cell_value(row[col_indexes["Provider ID"]])
        if pid_cell == str(provider_id):
            # 记录 Provider Name（取第一个非空值）
            if provider_name is None and col_indexes["Provider Name"] is not None:
                name_val = extract_cell_value(row[col_indexes["Provider Name"]])
                if name_val:
                    provider_name = name_val
            # 收集 Game ID
            if col_indexes["Game ID"] is not None:
                game_val = extract_cell_value(row[col_indexes["Game ID"]])
                if game_val and game_val not in game_ids:
                    game_ids.append(game_val)

    if provider_name is not None:
        return {"provider_name": provider_name, "game_ids": game_ids}
    return None

def get_provider_info(query_str):
    """
    主查询函数，接收用户输入的字符串，提取数字作为 Provider ID 进行查询
    支持多个 ID，用空格或逗号分隔
    """
    # 提取数字（支持 "pid" 前缀和纯数字）
    numbers = re.findall(r'(?i)pid?(\d+)', query_str)
    if not numbers:
        numbers = re.findall(r'\b(\d+)\b', query_str)
    if not numbers:
        return "❌ No valid provider IDs found. Usage: `/pid 30` or just `30` when mentioning me."

    # 去重并保持顺序
    seen = set()
    numbers_unique = []
    for n in numbers:
        if n not in seen:
            seen.add(n)
            numbers_unique.append(n)

    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ Failed to get access token: {e}"

    data = get_all_sheet_data(token, PROVIDER_SPREADSHEET_TOKEN, PROVIDER_SHEET_ID)
    if not data:
        return "❌ Failed to read provider sheet data."

    output_lines = []
    for pid in numbers_unique:
        mapping = find_provider_mappings(data, pid)
        if mapping:
            output_lines.append(f"🎮 Provider ID: {pid}")
            output_lines.append(f"📛 Provider Name: {mapping['provider_name']}")
            if mapping['game_ids']:
                output_lines.append("🕹️ Associated Game IDs:")
                for gid in mapping['game_ids']:
                    output_lines.append(f"   - {gid}")
            else:
                output_lines.append("ℹ️ No game IDs listed for this provider.")
            output_lines.append("")  # 空行分隔多个结果
        else:
            output_lines.append(f"❌ Provider ID {pid} not found.")
            output_lines.append("")

    return "\n".join(output_lines).strip()