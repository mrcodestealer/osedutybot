#!/usr/bin/env python3
"""
Emergency Contact List – Extract game contacts from a Lark sheet.
Columns are aligned as:
1st PM name, phone, 1st GO name, phone, 2nd PM name, phone, 2nd GO name, phone,
3rd PM name, phone, 3rd GO name, phone, 4th GO name, phone.
"""

import os
import sys
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
FUZZY_THRESHOLD = 0.65

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

def clean_text(text):
    return ' '.join(text.split())

def find_marker_cell(values):
    """Find the cell that contains both '游戏' and 'Game' (case-insensitive)."""
    for r, row in enumerate(values):
        for c, cell in enumerate(row):
            cell_text = extract_text_from_cell(cell).strip()
            if not cell_text:
                continue
            normalized = re.sub(r'\s+', ' ', cell_text).lower()
            if '游戏' in normalized and 'game' in normalized:
                debug_print(f"Marker found at row {r}, col {c}: '{cell_text}'")
                return (r, c)
    return None

def find_header_row(values):
    """Find the row that contains '1st负责人' or '1st Product Manager'."""
    for i, row in enumerate(values):
        if not row:
            continue
        for cell in row:
            cell_text = extract_text_from_cell(cell).strip()
            normalized = re.sub(r'\s+', ' ', cell_text).lower()
            if re.search(r'1st\s*负责人|1st\s*product\s*manager', normalized):
                debug_print(f"Header row found at {i}")
                return i
    return None

def find_first_role_column(values, header_row_idx):
    """Find the column index where the first role (1st PM) starts."""
    if header_row_idx >= len(values):
        return None
    header_row = values[header_row_idx]
    for col_idx, cell in enumerate(header_row):
        cell_text = extract_text_from_cell(cell).strip()
        normalized = re.sub(r'\s+', ' ', cell_text).lower()
        if re.search(r'1st\s*负责人|1st\s*product\s*manager', normalized):
            debug_print(f"First role column found at {col_idx}")
            return col_idx
    return None

def extract_games(values, marker_row, marker_col):
    """Collect game names from the same column below the marker."""
    games = []
    start_row = marker_row + 1
    for r in range(start_row, len(values)):
        row = values[r]
        if not row or all(not extract_text_from_cell(cell) for cell in row):
            break
        cell_text = extract_text_from_cell(row[marker_col] if marker_col < len(row) else "").strip()
        if not cell_text:
            continue
        if cell_text.startswith(('部门', 'Department', 'Studio', 'Stotsenberg', 'D\'Heights', 'Lavie', 'Newport')):
            break
        games.append(clean_text(cell_text))
    return games

def get_contacts_for_game(values, game_row, start_col):
    """
    Extract contacts for a game row.
    The columns from start_col to start_col+13 are fixed:
    0: 1st PM name, 1: phone, 2: 1st GO name, 3: phone,
    4: 2nd PM name, 5: phone, 6: 2nd GO name, 7: phone,
    8: 3rd PM name, 9: phone, 10: 3rd GO name, 11: phone,
    12: 4th GO name, 13: phone
    """
    row = values[game_row]
    # Ensure row has enough columns
    while len(row) <= start_col + 13:
        row.append("")
    contacts = {}
    role_labels = {
        '1st_pm': '1st Product Manager',
        '1st_go': '1st Game Operation',
        '2nd_pm': '2nd Product Manager',
        '2nd_go': '2nd Game Operation',
        '3rd_pm': '3rd Product Manager',
        '3rd_go': '3rd Game Operation',
        '4th_go': '4th Game Operation',
    }
    offsets = {
        '1st_pm': 0, '1st_go': 2,
        '2nd_pm': 4, '2nd_go': 6,
        '3rd_pm': 8, '3rd_go': 10,
        '4th_go': 12,
    }
    for key, label in role_labels.items():
        name_col = start_col + offsets[key]
        phone_col = name_col + 1
        name = extract_text_from_cell(row[name_col]).strip()
        phone = extract_text_from_cell(row[phone_col]).strip()
        contacts[key] = {'name': name, 'phone': phone, 'label': label}
    return contacts

def fuzzy_match_games(games_contacts, target_game):
    """Return list of (game_name, contacts) that match target_game."""
    target_lower = target_game.lower().strip()
    target_norm = target_lower.replace(' ', '')
    matches = []
    for name, contacts in games_contacts:
        name_lower = name.lower()
        name_norm = name_lower.replace(' ', '')
        # Substring match
        if target_lower in name_lower or name_lower in target_lower:
            matches.append((1.0, name, contacts))
            continue
        # Fuzzy match
        ratio = difflib.SequenceMatcher(None, target_norm, name_norm).ratio()
        if ratio >= FUZZY_THRESHOLD:
            matches.append((ratio, name, contacts))
    if not matches:
        return []
    matches.sort(key=lambda x: x[0], reverse=True)
    best_ratio = matches[0][0]
    return [(name, contacts) for ratio, name, contacts in matches if ratio >= best_ratio - 0.1]

def format_output(games_contacts, target_game=None):
    if target_game:
        matches = fuzzy_match_games(games_contacts, target_game)
        if not matches:
            return f"No game found matching '{target_game}'"
        games_contacts = matches
    if not games_contacts:
        return "No games found."
    lines = []
    for game_name, contacts in games_contacts:
        lines.append(f"🎮 {game_name}")
        order = ['1st_pm', '1st_go', '2nd_pm', '2nd_go', '3rd_pm', '3rd_go', '4th_go']
        for key in order:
            if key in contacts:
                name = contacts[key]['name']
                phone = contacts[key]['phone']
                label = contacts[key]['label']
                if name:
                    lines.append(f"  {label}: {name} (📞 {phone if phone else 'N/A'})")
        lines.append("")
    return "\n".join(lines)

def get_emergency_contacts(target_game=None):
    """Main function to fetch and return emergency contacts."""
    try:
        token = get_tenant_access_token()
        debug_print("Token obtained")

        metadata = get_sheet_metadata(token)
        max_row = metadata.get("rowCount", 500)
        values = get_range_values(token, f"A1:ZZ{max_row}")
        if not values:
            return "No data found in sheet."

        marker = find_marker_cell(values)
        if not marker:
            return "Could not find '游戏\\nGame' marker in the sheet."
        marker_row, marker_col = marker
        debug_print(f"Marker at row {marker_row}, col {marker_col}")

        game_names = extract_games(values, marker_row, marker_col)
        if not game_names:
            return "No game names found below marker."
        debug_print(f"Found {len(game_names)} games: {game_names[:5]}...")

        header_row = find_header_row(values)
        if header_row is None:
            return "Could not find header row with '1st负责人'."
        debug_print(f"Header row index: {header_row}")

        start_col = find_first_role_column(values, header_row)
        if start_col is None:
            return "Could not find first role column (1st Product Manager)."
        debug_print(f"First role column index: {start_col}")

        games_contacts = []
        row_idx = marker_row + 1
        for game_name in game_names:
            # Find the row that contains this game name
            while row_idx < len(values):
                row = values[row_idx]
                cell_text = extract_text_from_cell(row[marker_col] if marker_col < len(row) else "").strip()
                if cell_text and clean_text(cell_text) == game_name:
                    contacts = get_contacts_for_game(values, row_idx, start_col)
                    games_contacts.append((game_name, contacts))
                    row_idx += 1
                    break
                row_idx += 1
            else:
                break

        if not games_contacts:
            return "No games found."

        return format_output(games_contacts, target_game)

    except Exception as e:
        return f"Error: {e}"

def main():
    """Command-line entry point."""
    args = sys.argv[1:]
    target_game = None
    list_only = False
    output_csv_flag = False

    for arg in args:
        if arg == "--debug":
            global DEBUG
            DEBUG = True
        elif arg == "--list":
            list_only = True
        elif arg == "--csv":
            output_csv_flag = True
        elif not arg.startswith("--"):
            target_game = arg

    result = get_emergency_contacts(target_game)
    if result:
        print(result)
    else:
        print("No output.")

if __name__ == "__main__":
    main()