#!/usr/bin/env python3
"""
Generate a simplified maintenance summary from a full email.
Supports multiple email formats: fixed, ongoing, and planned.
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
        # 2. "Affected table/-s:" line
        elif re.search(r'^Affected table/-s:', line, re.IGNORECASE):
            j = i + 1
            while j < n and not lines[j]:
                j += 1
            if j < n:
                info['table'] = lines[j].strip()
            i = j
            continue
        # 3. "following tables will be unavailable:" line
        elif re.search(r'following tables will be unavailable:', line, re.IGNORECASE):
            j = i + 1
            while j < n and not lines[j]:
                j += 1
            if j < n:
                info['table'] = lines[j].strip()
            i = j
            continue
        # 4. Inline "table X in Y" (fallback during scanning)
        elif re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE):
            match = re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE)
            if match:
                info['table'] = match.group(1).strip()

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
        elif re.search(r'^Table\s+availability\s*:', line, re.IGNORECASE):
            # Relaxed to allow spaces before colon
            match = re.search(r'^Table\s+availability\s*:\s*(.*)$', line, re.IGNORECASE)
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
            else:
                # Check for "We will inform you as soon..." pattern
                if re.search(r'We will inform you as soon', line, re.IGNORECASE):
                    info['end_time'] = "TBA"

        # ---- Reference lines ----
        elif re.search(r'^(TINC-\d+|SD-\d+|\[Service Desk\])', line, re.IGNORECASE):
            info['reference'] = line.strip()

        i += 1

    # --- Fallbacks ---
    # Start time: look for "from ... UTC" in whole text
    if info['start_time'] == 'Unknown':
        from_match = re.search(r'from\s+(.*?)\s+UTC', text, re.IGNORECASE)
        if from_match:
            info['start_time'] = from_match.group(1).strip() + " UTC"

    # End time: if "We will inform you" appears and end time still unknown, set TBA
    if info['end_time'] == 'Unknown' and re.search(r'We will inform you as soon', text, re.IGNORECASE):
        info['end_time'] = "TBA"

    # Table name: fallback to "table X in Y" pattern in whole text
    if info['table'] == 'Unknown':
        table_match = re.search(r'table\s+([^\.]+?)\s+in', text, re.IGNORECASE)
        if table_match:
            info['table'] = table_match.group(1).strip()

    # Table availability flag: if not caught in line scan, check full text
    if not table_availability_affected and re.search(r'Table\s+availability\s*:\s*Affected', text, re.IGNORECASE):
        table_availability_affected = True

    # Set status based on table availability (only if not already set)
    if info['status'] == 'Unknown' and table_availability_affected:
        info['status'] = 'Affected'
    elif info['status'] == 'Unknown' and re.search(r'successfully accomplished', text, re.IGNORECASE):
        info['status'] = 'Fixed'

    return info

def generate_output(info):
    """Format the extracted info into the desired output with user mentions."""
    qa_os_local_id = "ou_c927a378e9b464741c67b61c1641577b"
    cs_team_id = "ou_24fc78938c54f1eda34bb7a446c4a664"

    output = [
        f'Hi <at user_id="{qa_os_local_id}">QA OS Local</at> <at user_id="{cs_team_id}">CS (Team)</at> , kindly check this email. Thank you.',
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
    # (unchanged, but ensure it's present)
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
        elif re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE):
            match = re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE)
            if match:
                return match.group(1).strip()
    # Fallback
    table_match = re.search(r'table\s+([^\.]+?)\s+in', text, re.IGNORECASE)
    if table_match:
        return table_match.group(1).strip()
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