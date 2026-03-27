#!/usr/bin/env python3
"""
P0 Incident command handler.
"""

import re
from datetime import datetime

def handle_p0(command_text):
    """
    Parse the command text after "/p0" or "/p0test".
    Supports quoted issue: `/p0 "issue description" <number> <support teams>`
    If no quotes, uses a fallback that avoids percentages.
    """
    command_text = command_text.strip()
    if not command_text:
        return "❌ No arguments provided. Use: `/p0 \"<issue>\" <number> <teams>`\nExample: `/p0 \"Several players cannot login\" 6 SRE,FE`"

    issue = None
    count = None
    support = None

    # 1. Try double‑quoted issue
    dq_match = re.match(r'\s*"([^"]+)"\s*(.*)$', command_text)
    if dq_match:
        issue = dq_match.group(1)
        rest = dq_match.group(2)
        num_match = re.search(r'\b(\d+)\b', rest)
        if num_match:
            count = int(num_match.group(1))
            support = rest[num_match.end():].strip()
    else:
        # 2. Try single‑quoted issue
        sq_match = re.match(r"\s*'([^']+)'\s*(.*)$", command_text)
        if sq_match:
            issue = sq_match.group(1)
            rest = sq_match.group(2)
            num_match = re.search(r'\b(\d+)\b', rest)
            if num_match:
                count = int(num_match.group(1))
                support = rest[num_match.end():].strip()

    if issue and count is not None:
        if not support:
            support = "(no support teams specified)"
    else:
        # 3. Fallback: no quotes – find the first number that is not part of a percentage
        numbers = []
        for match in re.finditer(r'\b(\d+)\b', command_text):
            # Skip if the number is immediately followed by '%'
            after = command_text[match.end():]
            if after.startswith('%'):
                continue
            numbers.append((match.start(), match.end(), int(match.group(1))))
        if numbers:
            start, end, count = numbers[0]
            before = command_text[:start].strip()
            after = command_text[end:].strip()
            issue = before.strip('"').strip("'")
            support = after if after else "(no support teams specified)"
        else:
            return "❌ Could not find a number (players affected). Use: `/p0 \"<issue>\" <number> <teams>`\nExample: `/p0 \"Several players cannot login\" 6 SRE,FE`"

    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M")

    message = (
        f"📍 P0 Incident Overview\n"
        f"🕐 Time : {timestamp} - Incident Start\n"
        f"🔥 Issue : {issue}\n"
        f"🎯 Impact Scope: {count} players\n"
        f"👥 Support Request: {support}"
    )
    return message