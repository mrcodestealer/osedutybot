#!/usr/bin/env python3
"""
FT Duty Schedule – Fetch from Lark Base (Bitable)
Usage:
    ./ft.py                # shows today, tomorrow, next day duty
    ./ft.py YYYY-MM-DD     # shows duty for the specified date
    ./ft.py --debug        # shows debug output (add before date, or alone)
"""

import os
import sys
import csv
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# ================= Configuration =================
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
BASE_ID = os.getenv("FT_BASE_ID")
TABLE_ID = os.getenv("FT_TABLE_ID")

# Actual field names in the Lark Base table
FIELD_DATE = "Date"
FIELD_NAME = "Member"

DUTY_LIST_PATH = "dutyList.csv"
DEBUG = False

# Name mapping: table name -> CSV name
NAME_MAPPING = {
    "Kelvin": "Kevin Lim",           # if "Kevin Lim" exists in CSV
    "Pin Quan": "Pin Quan",
    "Xuan You": "Sim Xuan You",
    "Winson": "Winson Hong",
    "Lian Cheng": "Lim Lian Cheng",
    "Qi Xiang": "Phan Qi Xiang",
    "Ho Ching": "Ho Ching",
    "Cheong Rui Zhe": "Rui Zhe",
}

# ================= Helper Functions =================
def debug_print(*args, **kwargs):
    if DEBUG:
        print("[DEBUG]", *args, **kwargs)

def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception("Failed to get token: {}".format(result))
    return result["tenant_access_token"]

def get_table_records(token, base_id, table_id):
    """Retrieve all records from the given base/table."""
    url = "https://open.larksuite.com/open-apis/bitable/v1/apps/{}/tables/{}/records".format(base_id, table_id)
    headers = {"Authorization": "Bearer {}".format(token)}
    records = []
    page_token = None
    while True:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(url, headers=headers, params=params)
        result = resp.json()
        if result.get("code") != 0:
            raise Exception("Failed to fetch records: {}".format(result))
        data = result.get("data", {})
        records.extend(data.get("items", []))
        page_token = data.get("page_token")
        if not data.get("has_more"):
            break
    return records

def extract_date_from_record(record):
    """Extract date from record fields. Returns a date object or None."""
    fields = record.get("fields", {})
    date_field = fields.get(FIELD_DATE)
    if not date_field:
        return None
    if isinstance(date_field, int):
        # timestamp in milliseconds
        return datetime.fromtimestamp(date_field / 1000).date()
    if isinstance(date_field, str):
        try:
            return datetime.strptime(date_field, "%Y-%m-%d").date()
        except ValueError:
            return None
    return None

def extract_names_from_record(record):
    """Extract names from record fields, apply mapping, return list."""
    fields = record.get("fields", {})
    name_field = fields.get(FIELD_NAME)
    if not name_field:
        return []
    names = []
    if isinstance(name_field, str):
        raw = name_field.strip()
        mapped = NAME_MAPPING.get(raw, raw)
        names.append(mapped)
    elif isinstance(name_field, list):
        for item in name_field:
            if isinstance(item, str):
                raw = item.strip()
                mapped = NAME_MAPPING.get(raw, raw)
                names.append(mapped)
    return names

def load_phone_map(csv_path):
    """Load dutyList.csv into a dict: name -> phone.
       CSV format: Name,Department,Phone
    """
    phone_map = {}
    if not os.path.exists(csv_path):
        print("⚠️ {} not found. Phones will be 'Not found'.".format(csv_path))
        return phone_map
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3:
                    name = row[0].strip()
                    phone = row[2].strip()
                    phone_map[name] = phone
        debug_print("Loaded {} contacts".format(len(phone_map)))
    except Exception as e:
        print("❌ Error loading {}: {}".format(csv_path, e))
    return phone_map

def get_ft_duty_for_date(target_date, records, phone_map):
    """Return formatted duty string for the target date."""
    duty_names = []
    for record in records:
        rec_date = extract_date_from_record(record)
        if rec_date == target_date:
            names = extract_names_from_record(record)
            duty_names.extend(names)
            debug_print("Found duty for {}: {}".format(target_date, names))
    # Remove duplicates
    seen = set()
    unique_names = []
    for name in duty_names:
        if name not in seen:
            seen.add(name)
            unique_names.append(name)

    lines = ["📅 FT Schedule - {}".format(target_date.strftime("%B %d, %Y %A"))]
    if not unique_names:
        lines.append("• No duty assigned")
    else:
        for name in unique_names:
            phone = phone_map.get(name, "Not found")
            lines.append("• {}  📞 {}".format(name, phone))
    return "\n".join(lines)

def get_ft_three_days(start_date=None):
    """Return formatted string for three consecutive days starting from start_date (default today)."""
    if start_date is None:
        start_date = datetime.now().date()

    # Fetch records once
    try:
        token = get_tenant_access_token()
        records = get_table_records(token, BASE_ID, TABLE_ID)
        debug_print("Retrieved {} records".format(len(records)))
    except Exception as e:
        return "❌ Failed to fetch records: {}".format(e)

    phone_map = load_phone_map(DUTY_LIST_PATH)

    results = []
    for i in range(3):
        target = start_date + timedelta(days=i)
        result = get_ft_duty_for_date(target, records, phone_map)
        results.append(result)

    return "\n\n".join(results)

def ft_check(month=None, year=None):
    """Check if all days in the given month have duty assigned."""
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month

    try:
        token = get_tenant_access_token()
        records = get_table_records(token, BASE_ID, TABLE_ID)
    except Exception as e:
        return "❌ Failed to fetch records: {}".format(e)

    # Build a set of days that have duty assigned
    assigned_days = set()
    for record in records:
        date_obj = extract_date_from_record(record)
        if date_obj and date_obj.year == year and date_obj.month == month:
            names = extract_names_from_record(record)
            if names:  # only consider if there is at least one name
                assigned_days.add(date_obj.day)

    # Determine days in month
    first_day = datetime(year, month, 1).date()
    if month == 12:
        next_month_first = datetime(year + 1, 1, 1).date()
    else:
        next_month_first = datetime(year, month + 1, 1).date()
    days_in_month = (next_month_first - first_day).days

    missing = [day for day in range(1, days_in_month + 1) if day not in assigned_days]

    month_name = datetime(year, month, 1).strftime("%B %Y")
    if not missing:
        return "✅ All days in {} have duty assigned.".format(month_name)
    else:
        missing_str = ", ".join(str(d) for d in missing)
        return "⚠️ {} 缺少值班的日期：{}".format(month_name, missing_str)

# ================= Main =================
def main():
    global DEBUG

    # Parse command line
    args = sys.argv[1:]
    if "--debug" in args:
        DEBUG = True
        args.remove("--debug")

    # If no arguments, show three days
    if not args:
        print(get_ft_three_days())
        return

    # If argument is a date, show that day only
    try:
        target_date = datetime.strptime(args[0], "%Y-%m-%d").date()
        try:
            token = get_tenant_access_token()
            records = get_table_records(token, BASE_ID, TABLE_ID)
            phone_map = load_phone_map(DUTY_LIST_PATH)
            print(get_ft_duty_for_date(target_date, records, phone_map))
        except Exception as e:
            print("❌ Error: {}".format(e))
    except ValueError:
        print("❌ Invalid date format. Use YYYY-MM-DD (e.g., 2026-04-02) or no argument for three days.")

if __name__ == "__main__":
    main()