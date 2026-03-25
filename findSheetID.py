#!/usr/bin/env python3
"""
Find all sheet IDs in a given spreadsheet.

Usage:
    python findSheetID.py SPREADSHEET_TOKEN
"""

import sys
import requests
import json

# ================= Configuration =================
APP_ID = "cli_a9ca652b89b85ed1"
APP_SECRET = "VQJh0oFKfsyCHr5tQDMVNbr4o4kmjbFr"

def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to get token: {result}")
    return result["tenant_access_token"]

def list_sheets(spreadsheet_token):
    token = get_tenant_access_token()
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        print("Error:", result)
        return
    sheets = result.get("data", {}).get("sheets", [])
    print(f"Total sheets: {len(sheets)}")
    for sheet in sheets:
        title = sheet.get("title")
        sheet_id = sheet.get("sheetId")
        print(f"  Title: {title}, ID: {sheet_id}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python findSheetID.py <spreadsheet_token>")
        sys.exit(1)
    spreadsheet_token = sys.argv[1]
    list_sheets(spreadsheet_token)