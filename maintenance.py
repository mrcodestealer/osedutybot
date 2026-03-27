#!/usr/bin/env python3
"""
Generate a simplified maintenance summary from a full email.
Supports both old and new email formats via line-by-line parsing.
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

    lines = text.splitlines()
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        # Look for table name
        if re.search(r'^table\s+', line, re.IGNORECASE):
            # Old format: "table XXXtreme Lightning Roulette in ..."
            match = re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE)
            if match:
                info['table'] = match.group(1).strip()
            else:
                match = re.search(r'table\s+(.*?)\s+was', line, re.IGNORECASE)
                if match:
                    info['table'] = match.group(1).strip()
        elif re.search(r'^Affected table/-s:', line, re.IGNORECASE):
            # New format: next non‑empty line is the table name
            i += 1
            while i < n and not lines[i].strip():
                i += 1
            if i < n:
                info['table'] = lines[i].strip()

        # Reason
        elif re.search(r'^Reason:', line, re.IGNORECASE):
            match = re.search(r'^Reason:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['reason'] = match.group(1).strip()
                # Remove parenthetical part
                info['reason'] = re.sub(r'\s*\([^)]*\)', '', info['reason'])

        # Status
        elif re.search(r'^Status:', line, re.IGNORECASE):
            match = re.search(r'^Status:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['status'] = match.group(1).strip()

        # Start time
        elif re.search(r'^Start time:', line, re.IGNORECASE):
            match = re.search(r'^Start time:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['start_time'] = match.group(1).strip()

        # End time
        elif re.search(r'^End time:', line, re.IGNORECASE):
            match = re.search(r'^End time:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['end_time'] = match.group(1).strip()

        # Time of resolution (old format)
        elif re.search(r'^Time of resolution:', line, re.IGNORECASE):
            match = re.search(r'from\s+(.*?)\s+till\s+(.*?)(?:\s*\(|$)', line, re.IGNORECASE)
            if match:
                info['start_time'] = match.group(1).strip()
                info['end_time'] = match.group(2).strip()

        # Reference: TINC-, SD-, or [Service Desk]
        elif re.search(r'^(TINC-\d+|SD-\d+|\[Service Desk\])', line, re.IGNORECASE):
            info['reference'] = line.strip()

        i += 1

    # If status is still unknown but the text mentions "successfully accomplished", set to Fixed
    if info['status'] == 'Unknown' and re.search(r'successfully accomplished', text, re.IGNORECASE):
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
    lines = text.splitlines()
    for i, line in enumerate(lines):
        line = line.strip()
        if re.search(r'^table\s+', line, re.IGNORECASE):
            match = re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE)
            if match:
                return match.group(1).strip()
            match = re.search(r'table\s+(.*?)\s+was', line, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        elif re.search(r'^Affected table/-s:', line, re.IGNORECASE):
            # next non‑empty line
            for j in range(i+1, len(lines)):
                if lines[j].strip():
                    return lines[j].strip()
    # Fallback: look for table name in reference
    if re.search(r'Instant Roulette', text, re.IGNORECASE):
        return "Instant Roulette"
    return "Unknown"

def process_email(text):
    """Process email text and return the formatted summary."""
    info = extract_info(text)
    return generate_output(info)

def main():
    """Command‑line interface."""
    if len(sys.argv) > 1:
        text = " ".join(sys.argv[1:])  # This will still lose newlines if not quoted!
        # Better to read from stdin for multiline input
        print("⚠️ Warning: multiline input may lose newlines. Use quotes or pipe.", file=sys.stderr)
    else:
        text = sys.stdin.read()
    if not text.strip():
        print("No input provided.", file=sys.stderr)
        sys.exit(1)
    print(process_email(text))

if __name__ == "__main__":
    main()