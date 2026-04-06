#!/usr/bin/env python3
"""
Game Responsible Person (1st负责人) - 读取 Lark 表格中“负责游戏”区域的数据
"""

import os
import re
import csv
import difflib
import requests
from dotenv import load_dotenv

load_dotenv()

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
SPREADSHEET_TOKEN = "P7ATwNGfci7idPkqlm4l4YUGgFn"
SHEET_ID = "fea5bc"

DEBUG = False

def debug_print(*args, **kwargs):
    if DEBUG:
        print("[DEBUG]", *args, file=sys.stderr)

def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to get token: {result}")
    return result["tenant_access_token"]

def get_sheet_metadata(token):
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/metainfo"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to get metadata: {result}")
    sheets = result.get("data", {}).get("sheets", [])
    for sheet in sheets:
        if sheet.get("sheetId") == SHEET_ID:
            return {
                "rowCount": sheet.get("rowCount"),
                "columnCount": sheet.get("columnCount")
            }
    raise Exception(f"Sheet ID {SHEET_ID} not found")

def get_range_values(token, range_str):
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{SHEET_ID}!{range_str}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to read values: {result}")
    return result.get("data", {}).get("valueRange", {}).get("values", [])

def extract_text_from_cell(cell):
    if cell is None:
        return ""
    if isinstance(cell, str):
        return cell
    if isinstance(cell, dict):
        if 'name' in cell:
            return cell['name']
        if 'text' in cell:
            return cell['text']
        return ""
    if isinstance(cell, list):
        parts = []
        for item in cell:
            if isinstance(item, dict):
                if 'name' in item:
                    parts.append(item['name'])
                elif 'text' in item:
                    parts.append(item['text'])
            elif isinstance(item, str):
                parts.append(item)
        return ''.join(parts)
    return str(cell)

def search_phone_in_dutylist(name):
    csv_path = 'dutyList.csv'
    if not os.path.exists(csv_path):
        return "N/A"
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            entries = []
            for row in reader:
                if len(row) >= 3:
                    entries.append((row[0].strip(), row[2].strip()))
        if not entries:
            return "N/A"
        name_norm = name.lower().replace(' ', '')
        best_match = None
        best_ratio = 0
        for entry_name, phone in entries:
            entry_norm = entry_name.lower().replace(' ', '')
            ratio = difflib.SequenceMatcher(None, name_norm, entry_norm).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = phone
        if best_ratio >= 0.7:
            return best_match
        return "N/A"
    except Exception:
        return "N/A"

def get_responsible_games(target_game=None):
    """
    返回负责游戏列表（只显示1st负责人）
    """
    try:
        token = get_tenant_access_token()
        metadata = get_sheet_metadata(token)
        max_row = metadata.get("rowCount", 500)
        values = get_range_values(token, f"A1:ZZ{max_row}")
        if not values:
            return "No data found in sheet."

        # 1. 找到“负责游戏”标记
        marker = None
        for r, row in enumerate(values):
            for c, cell in enumerate(row):
                cell_text = extract_text_from_cell(cell).strip()
                if cell_text == "负责游戏":
                    marker = (r, c)
                    break
            if marker:
                break
        if not marker:
            return "Could not find '负责游戏' marker."

        marker_row, marker_col = marker

        # 2. 找到“1st负责人”所在的列（整个表格中搜索）
        responsible_col = None
        for r, row in enumerate(values):
            if not row:
                continue
            for c, cell in enumerate(row):
                cell_text = extract_text_from_cell(cell).strip()
                if not cell_text:
                    continue
                normalized = re.sub(r'\s+', ' ', cell_text).lower()
                if re.search(r'1st\s*负责人|1st\s*product\s*manager', normalized):
                    responsible_col = c
                    debug_print(f"Found 1st负责人 at row {r}, col {c}")
                    break
            if responsible_col is not None:
                break

        if responsible_col is None:
            return "Could not find '1st负责人' column."

        # 3. 提取游戏名称和负责人
        games = []
        start_row = marker_row + 1
        for r in range(start_row, len(values)):
            row = values[r]
            if not row or all(not extract_text_from_cell(cell) for cell in row):
                break
            game_name = extract_text_from_cell(row[marker_col] if marker_col < len(row) else "").strip()
            if not game_name:
                break
            responsible = extract_text_from_cell(row[responsible_col] if responsible_col < len(row) else "").strip()
            if responsible:
                phone = search_phone_in_dutylist(responsible)
                games.append((game_name, responsible, phone))
            else:
                games.append((game_name, "Unknown", "N/A"))

        if not games:
            return "No games found under '负责游戏'."

        # 4. 模糊搜索
        if target_game:
            target_lower = target_game.lower().strip()
            target_norm = target_lower.replace(' ', '')
            matches = []
            for name, resp, phone in games:
                name_lower = name.lower()
                name_norm = name_lower.replace(' ', '')
                if target_lower in name_lower or name_lower in target_lower:
                    matches.append((name, resp, phone))
                else:
                    ratio = difflib.SequenceMatcher(None, target_norm, name_norm).ratio()
                    if ratio >= 0.65:
                        matches.append((name, resp, phone))
            games = matches

        if not games:
            return f"No game found matching '{target_game}'"

        # 5. 格式化输出
        lines = []
        for name, resp, phone in games:
            lines.append(f"🎮 {name}")
            lines.append(f"  Responsible: {resp} (📞 {phone})")
            lines.append("")
        return "\n".join(lines).strip()

    except Exception as e:
        return f"Error: {e}"

def main():
    import sys
    args = sys.argv[1:]
    target_game = args[0] if args else None
    print(get_responsible_games(target_game))

if __name__ == "__main__":
    main()