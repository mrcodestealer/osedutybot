#!/usr/bin/env python3
"""
Convert an incident message into a P0 Incident Overview.
"""

import sys
import re
from datetime import datetime

def extract_players(text):
    matches = re.findall(r'\b(\d{7,10})\b', text)
    return matches

def extract_issue(text):
    # Remove player IDs first
    text_no_ids = re.sub(r'\b\d{7,10}\b', '', text)
    # Remove common greetings, mentions, and trailing signatures
    text_no_ids = re.sub(r'(?i)@CP\s+OM\s+Duty', '', text_no_ids)
    text_no_ids = re.sub(r'(?i)please\s+help\s+check', '', text_no_ids)
    text_no_ids = re.sub(r'(?i)^Hi\s+team\s*,?\s*', '', text_no_ids)
    text_no_ids = re.sub(r'(?i)as of now,?\s*', '', text_no_ids)
    text_no_ids = re.sub(r'(?i)some players have reached out regarding\s*', '', text_no_ids)
    text_no_ids = re.sub(r'(?i)Affected Players\s*', '', text_no_ids)
    text_no_ids = re.sub(r'(?i)Player accounts:\s*', '', text_no_ids)
    text_no_ids = re.sub(r'\s*-\s*CS\s+Shiena\s*$', '', text_no_ids, flags=re.IGNORECASE)

    # Collapse whitespace
    text_no_ids = re.sub(r'\s+', ' ', text_no_ids).strip()

    issue_lower = text_no_ids.lower()
    # Known patterns
    if 'piso flash' in issue_lower and 'free spin' in issue_lower:
        return "free spins were not credited in PISO Flash Deal"
    elif 'tile' in issue_lower and 'color land' in issue_lower:
        return "tiles from the color land page not being visible"
    elif 'email verification' in issue_lower or 'verify their email' in issue_lower:
        # Rewrite email verification issue
        return "players are prompted to verify email upon login, preventing game access"
    else:
        # Fallback: take first sentence and remove leftover fluff
        sentences = re.split(r'[.!?]', text_no_ids)
        if sentences:
            candidate = sentences[0].strip()
            # Remove any remaining "when" prefix
            candidate = re.sub(r'^when\s+', '', candidate, flags=re.IGNORECASE)
            # Remove leading "they" or "the players"
            candidate = re.sub(r'^(they|the players)\s+', '', candidate, flags=re.IGNORECASE)
            # Capitalize first letter
            candidate = candidate[0].upper() + candidate[1:] if candidate else ""
            if candidate:
                return candidate
        # If all else fails, truncate
        return text_no_ids[:80] + "..." if len(text_no_ids) > 80 else text_no_ids

def determine_teams(issue):
    issue_lower = issue.lower()
    if 'tile' in issue_lower and 'color' in issue_lower:
        return "FE Dev, SRE"
    elif 'free spin' in issue_lower or 'piso flash' in issue_lower:
        return "FPMS Dev, SRE"
    elif 'email verification' in issue_lower:
        return "CP OM Duty, Dev, SRE"
    else:
        return "Dev, SRE"

def format_timestamp():
    now = datetime.now()
    return now.strftime("%Y-%m-%d %I:%M%p").replace("AM", " AM").replace("PM", " PM")

def process_emergency(text):
    players = extract_players(text)
    count = len(players)
    issue = extract_issue(text)
    teams = determine_teams(issue)
    timestamp = format_timestamp()

    return (
        f"P0 Incident Overview\n"
        f"🕐 Time : {timestamp} – Incident Start\n"
        f"🔥 Issue : {issue}\n"
        f"🎯 Impact Scope : {count} players\n"
        f"👥 Support Request : {teams}"
    )

def main():
    if len(sys.argv) > 1:
        text = " ".join(sys.argv[1:])
    else:
        text = sys.stdin.read()
    if not text.strip():
        print("No input provided.", file=sys.stderr)
        sys.exit(1)
    print(process_emergency(text))

if __name__ == "__main__":
    main()