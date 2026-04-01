#!/usr/bin/env python3
"""
Fetch approved leave records from Lark Base table.
Columns: Name, Leave Type, Start Date, End Date, Reason
Only shows records with Status = Approved.

Usage:
    ./leave.py                # Show approved leaves as table
    ./leave.py --csv          # Output CSV
    ./leave.py --debug        # Show raw API responses
"""

import sys
import csv
import json
import requests
from datetime import datetime

APP_ID = "cli_a9ca652b89b85ed1"
APP_SECRET = "VQJh0oFKfsyCHr5tQDMVNbr4o4kmjbFr"
BASE_ID = "O4Dfw4DVTiPpFukn801l5z3WgMd"
TABLE_ID = "tblfC3XoBP3as3Ci"

DEBUG = False

def debug_print(*args, **kwargs):
    if DEBUG:
        print("[DEBUG]", *args, file=sys.stderr, **kwargs)

def get_tenant_access_token():
    """Obtain a tenant access token."""
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    debug_print("Requesting token...")
    resp = requests.post(url, headers=headers, json=data)
    debug_print(f"Token response status: {resp.status_code}")
    if DEBUG:
        debug_print("Raw response:", resp.text[:500])
    try:
        result = resp.json()
    except Exception as e:
        raise Exception(f"Invalid JSON from token endpoint: {e}\nRaw: {resp.text}")
    if result.get("code") != 0:
        raise Exception(f"Token error: {result}")
    return result["tenant_access_token"]

def get_table_meta(token):
    """Fetch field metadata for the table."""
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{BASE_ID}/tables/{TABLE_ID}"
    headers = {"Authorization": f"Bearer {token}"}
    debug_print(f"Fetching table metadata: {url}")
    resp = requests.get(url, headers=headers)
    debug_print(f"Metadata response status: {resp.status_code}")
    if DEBUG:
        debug_print("Raw metadata response:", resp.text[:500])
    try:
        result = resp.json()
    except Exception as e:
        raise Exception(f"Invalid JSON from metadata: {e}\nRaw: {resp.text}")
    if result.get("code") != 0:
        raise Exception(f"Metadata error: {result}")
    fields = result["data"]["table"]["fields"]
    # Map field_id -> (field_name, field_type)
    field_map = {f["field_id"]: (f["field_name"], f["type"]) for f in fields}
    debug_print(f"Fields: {[name for name, _ in field_map.values()]}")
    return field_map

def get_all_records(token, page_size=100):
    """Retrieve all records from the table, handling pagination."""
    records = []
    page_token = None
    while True:
        url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{BASE_ID}/tables/{TABLE_ID}/records"
        params = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        headers = {"Authorization": f"Bearer {token}"}
        debug_print(f"Fetching records, page_token={page_token}")
        resp = requests.get(url, headers=headers, params=params)
        debug_print(f"Records response status: {resp.status_code}")
        if DEBUG:
            debug_print("Raw records response:", resp.text[:500])
        try:
            result = resp.json()
        except Exception as e:
            raise Exception(f"Invalid JSON from records: {e}\nRaw: {resp.text}")
        if result.get("code") != 0:
            raise Exception(f"Records error: {result}")
        data = result.get("data", {})
        items = data.get("items", [])
        records.extend(items)
        if not data.get("has_more"):
            break
        page_token = data.get("page_token")
    debug_print(f"Total records fetched: {len(records)}")
    return records

def format_field_value(value, field_type):
    """Convert a field value to a human-readable string."""
    if value is None:
        return ""
    if field_type in (1, 2):  # Text, Number
        return str(value)
    if field_type == 3:  # Checkbox
        return "✓" if value else "✗"
    if field_type == 4:  # DateTime
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(value / 1000)
            return dt.strftime("%Y-%m-%d")
        return str(value)
    if field_type == 5:  # Attachment
        return f"[{len(value)} files]"
    if field_type == 7:  # Single select
        return value.get("name", "") if isinstance(value, dict) else str(value)
    if field_type == 11:  # Multiple select
        if isinstance(value, list):
            return ", ".join(v.get("name", "") for v in value if isinstance(v, dict))
        return str(value)
    if field_type == 13:  # People
        if isinstance(value, list):
            names = [p.get("name", "") for p in value if isinstance(p, dict)]
            return ", ".join(names)
        return str(value)
    return str(value)

def extract_field_value(record, field_name, field_map):
    """Given a field name, find its field_id and extract value."""
    name_to_id = {name: (fid, ftype) for fid, (name, ftype) in field_map.items()}
    if field_name not in name_to_id:
        debug_print(f"Field '{field_name}' not found in metadata")
        return ""
    fid, ftype = name_to_id[field_name]
    value = record.get("fields", {}).get(fid)
    return format_field_value(value, ftype)

def main():
    global DEBUG
    args = sys.argv[1:]
    output_csv = False
    for arg in args:
        if arg == "--debug":
            DEBUG = True
        elif arg == "--csv":
            output_csv = True
        else:
            print(f"Unknown argument: {arg}", file=sys.stderr)
            sys.exit(1)

    try:
        token = get_tenant_access_token()
        debug_print("Token obtained successfully")

        field_map = get_table_meta(token)

        # Find Status field ID
        status_field_id = None
        for fid, (name, ftype) in field_map.items():
            if name == "Status":
                status_field_id = fid
                break
        if status_field_id is None:
            raise Exception("Status field not found in table")

        records = get_all_records(token)

        # Filter approved records
        approved = []
        for rec in records:
            status_value = rec.get("fields", {}).get(status_field_id)
            # Status is usually a single select
            if isinstance(status_value, dict):
                status_text = status_value.get("name", "")
            else:
                status_text = str(status_value) if status_value else ""
            if status_text.lower() == "approved":
                approved.append(rec)

        debug_print(f"Approved records: {len(approved)}")

        if not approved:
            print("No approved leave records found.")
            return

        # Define columns to display
        columns = ["Name", "Leave Type", "Start Date", "End Date", "Reason"]
        rows = []
        for rec in approved:
            row = [extract_field_value(rec, col, field_map) for col in columns]
            rows.append(row)

        if output_csv:
            writer = csv.writer(sys.stdout)
            writer.writerow(columns)
            writer.writerows(rows)
        else:
            # Pretty print table
            col_widths = [len(col) for col in columns]
            for row in rows:
                for i, cell in enumerate(row):
                    col_widths[i] = max(col_widths[i], len(cell))

            def print_sep():
                print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")

            print_sep()
            header = "| " + " | ".join(c.ljust(w) for c, w in zip(columns, col_widths)) + " |"
            print(header)
            print_sep()
            for row in rows:
                line = "| " + " | ".join(cell.ljust(w) for cell, w in zip(row, col_widths)) + " |"
                print(line)
            print_sep()
            print(f"Total approved leaves: {len(approved)}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()