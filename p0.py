#!/usr/bin/env python3
"""
P0 Incident command handler.
"""

import re
from datetime import datetime

def handle_p0(command_text):
    """
    Parse the command text after "/p0" or "/p0test".
    Extracts the first number as the affected player count.
    Everything before the number is the issue, everything after is support.
    """
    command_text = command_text.strip()
    if not command_text:
        return "❌ No arguments provided. Use: `/p0 <issue> <number> <teams>`\nExample: `/p0 Several players cannot login 6 SRE,FE`"

    # Find the first number (digits)
    match = re.search(r'\b(\d+)\b', command_text)
    if not match:
        return "❌ Could not find a number (players affected). Use: `/p0 <issue> <number> <teams>`\nExample: `/p0 Several players cannot login 6 SRE,FE`"

    count_str = match.group(1)
    count = int(count_str)

    # Split the text at the number
    before = command_text[:match.start()].strip()
    after = command_text[match.end():].strip()

    # Issue is everything before the number (clean up quotes)
    issue = before.strip('"').strip("'")
    support = after if after else "(no support teams specified)"

    if not issue:
        return "❌ Issue cannot be empty. Use: `/p0 <issue> <number> <teams>`"

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