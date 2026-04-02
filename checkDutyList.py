#!/usr/bin/env python3
"""
Check dutyList.csv for duplicate names or phone numbers.
Displays the line number and the duplicated value.
"""

import csv
from collections import defaultdict

def check_duplicates(csv_path='dutyList.csv'):
    """
    Scan dutyList.csv for duplicate names and duplicate phone numbers.
    Prints the line numbers of the first occurrence and duplicates.
    """
    name_map = defaultdict(list)   # name -> list of line numbers
    phone_map = defaultdict(list)  # phone -> list of line numbers

    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for line_num, row in enumerate(reader, start=1):
                if len(row) < 3:
                    print(f"⚠️ Line {line_num}: insufficient columns (found {len(row)}), skipping.")
                    continue
                name = row[0].strip()
                phone = row[2].strip()
                if name:
                    name_map[name].append(line_num)
                if phone:
                    phone_map[phone].append(line_num)

        # Report duplicate names
        duplicates_found = False
        print("=== Duplicate Names ===")
        for name, lines in name_map.items():
            if len(lines) > 1:
                duplicates_found = True
                print(f"  '{name}' appears on lines: {', '.join(map(str, lines))}")

        # Report duplicate phone numbers
        print("\n=== Duplicate Phone Numbers ===")
        for phone, lines in phone_map.items():
            if len(lines) > 1:
                duplicates_found = True
                print(f"  '{phone}' appears on lines: {', '.join(map(str, lines))}")

        if not duplicates_found:
            print("✅ No duplicates found.")

    except FileNotFoundError:
        print(f"❌ File '{csv_path}' not found.")
    except Exception as e:
        print(f"❌ Error reading file: {e}")

if __name__ == "__main__":
    check_duplicates()