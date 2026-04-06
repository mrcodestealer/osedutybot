#!/usr/bin/env python3
"""
Game Responsible Persons (1st,2nd,3rd,4th,5th负责人) - 读取 Lark 表格中“负责游戏”区域的数据
"""

import os
import re
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

def find_marker(values):
    """Find the cell that exactly contains '负责游戏'."""
    for r, row in enumerate(values):
        for c, cell in enumerate(row):
            cell_text = extract_text_from_cell(cell).strip()
            if cell_text == "负责游戏":
                debug_print(f"Marker found at row {r}, col {c}")
                return (r, c)
    return None

def find_header_row(values):
    """Find the row that contains '1st负责人' or '1st Person in Charge'."""
    for r, row in enumerate(values):
        if not row:
            continue
        for cell in row:
            cell_text = extract_text_from_cell(cell).strip()
            normalized = re.sub(r'\s+', ' ', cell_text).lower()
            if re.search(r'1st\s*负责人|1st\s*person\s*in\s*charge', normalized):
                debug_print(f"Header row found at {r}")
                return r
    return None

def get_responsible_columns(header_row):
    """
    Scan the header row and return a list of (name_col, phone_col) for each responsible person.
    Assumes order: name, phone, (optional email), then next name, etc.
    """
    phone_pattern = re.compile(r'紧急联络电话|contact\s*no\.', re.IGNORECASE)
    name_pattern = re.compile(r'负责人|person\s*in\s*charge', re.IGNORECASE)
    
    responsible_columns = []
    i = 0
    while i < len(header_row):
        cell_text = extract_text_from_cell(header_row[i]).strip()
        if not cell_text:
            i += 1
            continue
        normalized = re.sub(r'\s+', ' ', cell_text).lower()
        if name_pattern.search(normalized):
            name_col = i
            # search forward up to 5 columns for phone
            phone_col = None
            for j in range(i+1, min(i+6, len(header_row))):
                phone_text = extract_text_from_cell(header_row[j]).strip()
                if phone_pattern.search(phone_text):
                    phone_col = j
                    break
            if phone_col is not None:
                responsible_columns.append((name_col, phone_col))
                # Move i to phone_col+1 to continue scanning after this group
                i = phone_col + 1
            else:
                # No phone found for this name, skip this column
                i += 1
        else:
            i += 1
    return responsible_columns

def get_responsible_games(target_game=None):
    """Return formatted list of games with all responsible persons."""
    try:
        token = get_tenant_access_token()
        metadata = get_sheet_metadata(token)
        max_row = metadata.get("rowCount", 500)
        values = get_range_values(token, f"A1:ZZ{max_row}")
        if not values:
            return "No data found in sheet."

        # 1. Find marker
        marker = find_marker(values)
        if not marker:
            return "Could not find '负责游戏' marker."
        marker_row, marker_col = marker

        # 2. Find header row (anywhere, not necessarily above)
        header_row_idx = find_header_row(values)
        if header_row_idx is None:
            return "Could not find header row with '1st负责人'."
        header_row = values[header_row_idx]

        # 3. Get responsible columns
        responsible_cols = get_responsible_columns(header_row)
        if not responsible_cols:
            return "Could not find any responsible person columns."

        # 4. Extract games
        games = []
        start_row = marker_row + 1
        for r in range(start_row, len(values)):
            row = values[r]
            if not row or all(not extract_text_from_cell(cell) for cell in row):
                break
            game_name = extract_text_from_cell(row[marker_col] if marker_col < len(row) else "").strip()
            if not game_name:
                break
            # For each responsible person, get name and phone
            persons = []
            for name_col, phone_col in responsible_cols:
                name = extract_text_from_cell(row[name_col] if name_col < len(row) else "").strip()
                phone = extract_text_from_cell(row[phone_col] if phone_col < len(row) else "").strip()
                if name:  # Only add if name exists
                    persons.append((name, phone))
            if persons:
                games.append((game_name, persons))

        if not games:
            return "No games found under '负责游戏'."

        # 5. Fuzzy match if target_game provided
        if target_game:
            target_lower = target_game.lower().strip()
            target_norm = target_lower.replace(' ', '')
            matches = []
            for name, persons in games:
                name_lower = name.lower()
                name_norm = name_lower.replace(' ', '')
                if target_lower in name_lower or name_lower in target_lower:
                    matches.append((name, persons))
                else:
                    ratio = difflib.SequenceMatcher(None, target_norm, name_norm).ratio()
                    if ratio >= 0.65:
                        matches.append((name, persons))
            games = matches

        if not games:
            return f"No game found matching '{target_game}'"

        # 6. Format output
        lines = []
        for game_name, persons in games:
            lines.append(f"🎮 {game_name}")
            for idx, (name, phone) in enumerate(persons, start=1):
                suffix = "st" if idx == 1 else "nd" if idx == 2 else "rd" if idx == 3 else "th"
                lines.append(f"  {idx}{suffix} Responsible: {name} (📞 {phone if phone else 'N/A'})")
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