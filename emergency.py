#!/usr/bin/env python3
"""
Emergency Contact List – Extract game contacts from a Lark sheet.
Only extracts: 1st PM, 1st GO, 2nd PM, 2nd GO, 3rd PM, 3rd GO, 4th GO.
Supports fuzzy matching for game names.
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
FUZZY_THRESHOLD = 0.75

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

def parse_headers(values, header_row_idx):
    """Hardcoded column mapping based on the known structure."""
    roles = [
        ('1st_pm', '1st Product Manager'),
        ('1st_go', '1st Game Operation'),
        ('2nd_pm', '2nd Product Manager'),
        ('2nd_go', '2nd Game Operation'),
        ('3rd_pm', '3rd Product Manager'),
        ('3rd_go', '3rd Game Operation'),
        ('4th_go', '4th Game Operation'),
    ]
    role_map = {}
    for i, (key, label) in enumerate(roles):
        name_col = i * 2
        phone_col = name_col + 1
        role_map[key] = {'name_col': name_col, 'phone_col': phone_col, 'label': label}
    return role_map

def extract_games(values, marker_row, marker_col):
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

def get_contacts_for_game(values, game_row, role_map):
    row = values[game_row]
    max_col = 14
    while len(row) <= max_col:
        row.append("")
    contacts = {}
    for role_key, cols in role_map.items():
        name = extract_text_from_cell(row[cols['name_col']]).strip()
        phone = extract_text_from_cell(row[cols['phone_col']]).strip()
        contacts[role_key] = {'name': name, 'phone': phone, 'label': cols['label']}
    return contacts

def fuzzy_match_games(games_contacts, target_game):
    """Return list of (game_name, contacts) that fuzzy match target_game."""
    target_norm = target_game.lower().replace(' ', '')
    best_matches = []
    for name, contacts in games_contacts:
        name_norm = name.lower().replace(' ', '')
        ratio = difflib.SequenceMatcher(None, target_norm, name_norm).ratio()
        if ratio >= FUZZY_THRESHOLD:
            best_matches.append((ratio, name, contacts))
    if not best_matches:
        return []
    best_matches.sort(key=lambda x: x[0], reverse=True)
    best_ratio = best_matches[0][0]
    return [(name, contacts) for ratio, name, contacts in best_matches if ratio >= best_ratio - 0.05]

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

        role_map = parse_headers(values, header_row)
        debug_print(f"Roles: {list(role_map.keys())}")

        games_contacts = []
        row_idx = marker_row + 1
        for game_name in game_names:
            while row_idx < len(values):
                row = values[row_idx]
                cell_text = extract_text_from_cell(row[marker_col] if marker_col < len(row) else "").strip()
                if cell_text and clean_text(cell_text) == game_name:
                    contacts = get_contacts_for_game(values, row_idx, role_map)
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