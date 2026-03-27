#!/usr/bin/env python3
"""
Generate a simplified maintenance summary from a full email.
Supports multiple email formats.
"""

import sys
import re

def extract_info(text):
    """Parse email text line by line to extract fields."""
    info = {
        'table': 'Unknown',
        'reason': 'Unknown',
        'status': 'Unknown',
        'start_time': 'Unknown',
        'end_time': 'Unknown',
        'reference': 'Unknown'
    }

    lines = [line.strip() for line in text.splitlines()]
    table_availability_affected = False

    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        if not line:
            i += 1
            continue

        # ---- Table detection ----
        # 1. Old format: "table X in Y"
        if re.search(r'^table\s+', line, re.IGNORECASE):
            match = re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE)
            if match:
                info['table'] = match.group(1).strip()
            else:
                match = re.search(r'table\s+(.*?)\s+was', line, re.IGNORECASE)
                if match:
                    info['table'] = match.group(1).strip()

        # 2. New format: "Affected table/-s:" line followed by table name
        elif re.search(r'^Affected table/-s:', line, re.IGNORECASE):
            j = i + 1
            while j < n and not lines[j]:
                j += 1
            if j < n:
                info['table'] = lines[j].strip()
            i = j
            continue

        # 3. "following tables will be unavailable:" line followed by table name
        elif re.search(r'following tables will be unavailable:', line, re.IGNORECASE):
            j = i + 1
            while j < n and not lines[j]:
                j += 1
            if j < n:
                info['table'] = lines[j].strip()
            i = j
            continue

        # ---- Reason ----
        elif re.search(r'^Reason:', line, re.IGNORECASE):
            match = re.search(r'^Reason:\s*(.*)$', line, re.IGNORECASE)
            if match:
                reason = match.group(1).strip()
                reason = re.sub(r'\s*\([^)]*\)', '', reason)
                info['reason'] = reason

        # ---- Status (explicit) ----
        elif re.search(r'^Status:', line, re.IGNORECASE):
            match = re.search(r'^Status:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['status'] = match.group(1).strip()

        # ---- Table availability (new format status) ----
        elif re.search(r'^Table availability:', line, re.IGNORECASE):
            match = re.search(r'^Table availability:\s*(.*)$', line, re.IGNORECASE)
            if match and match.group(1).strip().lower() == 'affected':
                table_availability_affected = True

        # ---- Start time ----
        elif re.search(r'^Start time:', line, re.IGNORECASE):
            match = re.search(r'^Start time:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['start_time'] = match.group(1).strip()

        # ---- End time ----
        elif re.search(r'^End time:', line, re.IGNORECASE):
            match = re.search(r'^End time:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['end_time'] = match.group(1).strip()

        # ---- Time of resolution (old format) ----
        elif re.search(r'^Time of resolution:', line, re.IGNORECASE):
            match = re.search(r'from\s+(.*?)\s+till\s+(.*?)(?:\s*\(|$)', line, re.IGNORECASE)
            if match:
                info['start_time'] = match.group(1).strip()
                info['end_time'] = match.group(2).strip()

        # ---- Reference lines ----
        elif re.search(r'^(TINC-\d+|SD-\d+|\[Service Desk\])', line, re.IGNORECASE):
            info['reference'] = line.strip()

        i += 1

    # Fallback for "Table availability: Affected" if not caught by line scanning
    if not table_availability_affected and re.search(r'Table availability:\s*Affected', text, re.IGNORECASE):
        table_availability_affected = True

    # Set status based on table availability
    if table_availability_affected:
        info['status'] = 'Affected'
    elif info['status'] == 'Unknown' and re.search(r'successfully accomplished', text, re.IGNORECASE):
        info['status'] = 'Fixed'

    return info

def generate_output(info):
    """Format the extracted info into the desired output."""
    output = [
        "Hi @QA OS Local @CS (Team) , kindly check this email. Thank you.",
        "",
        f"Affected table : {info['table']}",
        f"Reason : {info['reason']}",
        f"Status: {info['status']}",
        f"Start time: {info['start_time']}",
        f"End time : {info['end_time']}",
        "",
        f"REF EMAIL:{info['reference']}"
    ]
    return "\n".join(output)

def get_table_name(text):
    """Extract just the affected table name for the first tag message."""
    lines = [line.strip() for line in text.splitlines()]
    for i, line in enumerate(lines):
        if re.search(r'^table\s+', line, re.IGNORECASE):
            match = re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE)
            if match:
                return match.group(1).strip()
            match = re.search(r'table\s+(.*?)\s+was', line, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        elif re.search(r'^Affected table/-s:', line, re.IGNORECASE):
            for j in range(i+1, len(lines)):
                if lines[j]:
                    return lines[j].strip()
        elif re.search(r'following tables will be unavailable:', line, re.IGNORECASE):
            for j in range(i+1, len(lines)):
                if lines[j]:
                    return lines[j].strip()
    # Fallback: maybe table name is in reference line
    if re.search(r'Lightning Storm', text, re.IGNORECASE):
        return "Lightning Storm"
    return "Unknown"

def process_email(text):
    """Process email text and return the formatted summary."""
    info = extract_info(text)
    return generate_output(info)

def main():
    """Command‑line interface."""
    if len(sys.argv) > 1:
        text = " ".join(sys.argv[1:])
        if '\n' not in text:
            print("⚠️ Hint: For multiline input, please enclose the email in quotes or use a pipe (python3 maintenance.py < email.txt).", file=sys.stderr)
    else:
        text = sys.stdin.read()
    if not text.strip():
        print("No input provided.", file=sys.stderr)
        sys.exit(1)
    print(process_email(text))

if __name__ == "__main__":
    main()