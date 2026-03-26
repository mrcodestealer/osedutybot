#!/usr/bin/env python3
"""
Leave Records – Fetch and display all entries from a Lark Base table.

Usage:
    ./leave.py                     # Show all records from the default table
    ./leave.py --base BASE_ID      # Use a different base ID
    ./leave.py --table TABLE_ID    # Use a different table ID
    ./leave.py --csv                # Output in CSV format
    ./leave.py --debug              # Show raw API response for debugging

The default base ID and table ID are taken from the URL:
https://casinoplus.sg.larksuite.com/wiki/O4Dfw4DVTiPpFukn801l5z3WgMd?sheet=65p5cn&table=tblfC3XoBP3as3Ci&view=vewMweziue
"""

import sys
import csv
import requests
from datetime import datetime

# ================= Configuration =================
APP_ID = "cli_a9ca652b89b85ed1"
APP_SECRET = "VQJh0oFKfsyCHr5tQDMVNbr4o4kmjbFr"
DEFAULT_BASE_ID = "O4Dfw4DVTiPpFukn801l5z3WgMd"
DEFAULT_TABLE_ID = "tblfC3XoBP3as3Ci"

DEBUG = False

def debug_print(*args, **kwargs):
    if DEBUG:
        print("[DEBUG]", *args, file=sys.stderr, **kwargs)

def get_tenant_access_token():
    """Obtain a tenant access token for Lark API."""
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to get token: {result}")
    return result["tenant_access_token"]

def get_table_meta(base_id, table_id, token):
    """Fetch field metadata for the given table."""
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to get table metadata: {result}")
    fields = result["data"]["table"]["fields"]
    # Create mapping from field_id to (field_name, field_type)
    field_map = {f["field_id"]: (f["field_name"], f["type"]) for f in fields}
    return field_map

def get_all_records(base_id, table_id, token, page_size=100):
    """Retrieve all records from the specified table (with pagination)."""
    records = []
    page_token = None
    while True:
        url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records"
        params = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers, params=params)
        result = resp.json()
        if result.get("code") != 0:
            raise Exception(f"Failed to fetch records: {result}")
        data = result.get("data", {})
        items = data.get("items", [])
        records.extend(items)
        if not data.get("has_more"):
            break
        page_token = data.get("page_token")
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
        # value is timestamp in milliseconds
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(value / 1000)
            return dt.strftime("%Y-%m-%d %H:%M")
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
    # Fallback
    return str(value)

def print_records_as_table(records, field_map):
    """Print records in a formatted table."""
    if not records:
        print("No records found.")
        return

    # Collect all field IDs present in records
    field_ids = set()
    for rec in records:
        field_ids.update(rec.get("fields", {}).keys())
    # Also include all fields from metadata for completeness
    field_ids.update(field_map.keys())
    field_ids = sorted(field_ids, key=lambda fid: field_map.get(fid, ("", ""))[0])

    # Prepare header
    headers = []
    col_widths = []
    for fid in field_ids:
        name, _ = field_map.get(fid, (fid, 0))
        headers.append(name)
        col_widths.append(len(name))

    # Gather rows
    rows = []
    for rec in records:
        row = []
        fields = rec.get("fields", {})
        for fid in field_ids:
            val = fields.get(fid)
            _, ftype = field_map.get(fid, (None, 0))
            display = format_field_value(val, ftype)
            row.append(display)
            col_widths = [max(w, len(display)) for w, display in zip(col_widths, row)]
        rows.append(row)

    # Print table
    def print_sep():
        print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")

    print_sep()
    header_row = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |"
    print(header_row)
    print_sep()
    for row in rows:
        line = "| " + " | ".join(cell.ljust(w) for cell, w in zip(row, col_widths)) + " |"
        print(line)
    print_sep()
    print(f"Total records: {len(records)}")

def print_records_as_csv(records, field_map):
    """Output records in CSV format."""
    if not records:
        return

    field_ids = sorted(field_map.keys(), key=lambda fid: field_map.get(fid, ("", ""))[0])
    headers = [field_map.get(fid, (fid, 0))[0] for fid in field_ids]

    writer = csv.writer(sys.stdout)
    writer.writerow(headers)

    for rec in records:
        row = []
        fields = rec.get("fields", {})
        for fid in field_ids:
            val = fields.get(fid)
            _, ftype = field_map.get(fid, (None, 0))
            display = format_field_value(val, ftype)
            row.append(display)
        writer.writerow(row)

def main():
    global DEBUG
    args = sys.argv[1:]

    base_id = DEFAULT_BASE_ID
    table_id = DEFAULT_TABLE_ID
    output_csv = False

    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--debug":
            DEBUG = True
            i += 1
        elif arg == "--csv":
            output_csv = True
            i += 1
        elif arg == "--base":
            if i+1 >= len(args):
                print("Error: --base requires an argument")
                sys.exit(1)
            base_id = args[i+1]
            i += 2
        elif arg == "--table":
            if i+1 >= len(args):
                print("Error: --table requires an argument")
                sys.exit(1)
            table_id = args[i+1]
            i += 2
        else:
            print(f"Unknown argument: {arg}")
            sys.exit(1)

    try:
        token = get_tenant_access_token()
        debug_print("Token obtained")

        field_map = get_table_meta(base_id, table_id, token)
        debug_print(f"Found {len(field_map)} fields")

        records = get_all_records(base_id, table_id, token)
        debug_print(f"Retrieved {len(records)} records")

        if output_csv:
            print_records_as_csv(records, field_map)
        else:
            print_records_as_table(records, field_map)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()