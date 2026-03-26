import sys
import re

def extract_info(text):
    info = {}

    # Extract table name
    table_match = re.search(r'table\s+([^\.]+?)\s+in\s+[^\.]+', text, re.IGNORECASE)
    if table_match:
        info['table'] = table_match.group(1).strip()
    else:
        table_match = re.search(r'table\s+(.*?)\s+was', text, re.IGNORECASE)
        if table_match:
            info['table'] = table_match.group(1).strip()
        else:
            info['table'] = "Unknown"

    # Extract reason: stop before next field (Status, Date, Time, or end)
    reason_match = re.search(r'Reason:\s*(.*?)(?=\s*(?:Status:|Date:|Time of resolution:|$))', text, re.IGNORECASE | re.DOTALL)
    if reason_match:
        reason = reason_match.group(1).strip()
        reason = re.sub(r'\s*\([^)]*\)', '', reason)  # remove parentheses
        info['reason'] = reason
    else:
        info['reason'] = "Unknown"

    # Extract status: stop before next field (Date, Time, or end)
    status_match = re.search(r'Status:\s*(.*?)(?=\s*(?:Date:|Time of resolution:|$))', text, re.IGNORECASE | re.DOTALL)
    info['status'] = status_match.group(1).strip() if status_match else "Unknown"

    # Extract start and end times
    resolution_match = re.search(r'Time of resolution:\s*from\s+(.*?)\s+till\s+(.*?)(?:\s*\(|$)', text, re.IGNORECASE | re.DOTALL)
    if resolution_match:
        info['start_time'] = resolution_match.group(1).strip()
        info['end_time'] = resolution_match.group(2).strip()
    else:
        from_match = re.search(r'from\s+(.*?)\s+UTC\s+till\s+(.*?)\s+UTC', text, re.IGNORECASE)
        if from_match:
            info['start_time'] = from_match.group(1).strip() + " UTC"
            info['end_time'] = from_match.group(2).strip() + " UTC"
        else:
            info['start_time'] = "Unknown"
            info['end_time'] = "Unknown"

    # Extract reference (line starting with TINC-)
    ref_match = re.search(r'(TINC-\d+\s+.*?)(?:\n|$)', text, re.IGNORECASE)
    info['reference'] = ref_match.group(1).strip() if ref_match else "Unknown"

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
    """Extract just the affected table name from email text."""
    table_match = re.search(r'table\s+([^\.]+?)\s+in\s+[^\.]+', text, re.IGNORECASE)
    if table_match:
        return table_match.group(1).strip()
    table_match = re.search(r'table\s+(.*?)\s+was', text, re.IGNORECASE)
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
    else:
        text = sys.stdin.read()
    if not text.strip():
        print("No input provided.", file=sys.stderr)
        sys.exit(1)
    print(process_email(text))

if __name__ == "__main__":
    main()