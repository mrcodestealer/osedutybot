import csv
import difflib

# Load duty list once when the module is imported
_duty_list = None

def _load_duty_list(csv_path='dutyList.csv'):
    """Internal function to load duty entries from CSV."""
    duty = []
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3:
                    duty.append({
                        'name': row[0].strip(),
                        'department': row[1].strip(),
                        'phone': row[2].strip()
                    })
        print(f"✅ Loaded {len(duty)} entries from {csv_path}")
    except FileNotFoundError:
        print(f"⚠️ {csv_path} not found. Duty command will not work.")
    return duty

def get_duty_list():
    """Return the duty list, loading it if necessary."""
    global _duty_list
    if _duty_list is None:
        _duty_list = _load_duty_list()
    return _duty_list

def search_duty(query):
    """
    Search duty_list using:
    - substring match (case‑insensitive, ignoring spaces)
    - fuzzy match as fallback
    Multiple names can be given separated by commas or '&'.
    """
    duty_list = get_duty_list()
    if not duty_list:
        return "Duty list is empty or not loaded."

    if not query:
        lines = []
        for entry in duty_list:
            lines.append(
                f"Name : {entry['name']}\n"
                f"Department : {entry['department']}\n"
                f"Phone number : {entry['phone']}"
            )
        return "\n\n".join(lines)

    # Split query on commas or ampersands
    query = query.replace('&', ',')
    parts = [part.strip() for part in query.split(',') if part.strip()]
    if not parts:
        parts = [query]

    THRESHOLD = 0.8
    results = {}  # use phone as key to avoid duplicates

    for part in parts:
        part_norm = part.lower().replace(' ', '')

        # 1. Substring match
        matched_entries = []
        for entry in duty_list:
            name_norm = entry['name'].lower().replace(' ', '')
            if part_norm in name_norm:
                matched_entries.append(entry)

        if matched_entries:
            for entry in matched_entries:
                results[entry['phone']] = entry
            continue

        # 2. Fuzzy match
        fuzzy_matches = []
        for entry in duty_list:
            name_norm = entry['name'].lower().replace(' ', '')
            similarity = difflib.SequenceMatcher(None, part_norm, name_norm).ratio()
            if similarity >= THRESHOLD:
                fuzzy_matches.append((similarity, entry))
        if fuzzy_matches:
            fuzzy_matches.sort(key=lambda x: x[0], reverse=True)
            for _, entry in fuzzy_matches:
                results[entry['phone']] = entry

    if not results:
        return f"No matching duty personnel found for '{query}'."

    # Format output
    lines = []
    for entry in results.values():
        lines.append(
            f"Name : {entry['name']}\n"
            f"Department : {entry['department']}\n"
            f"Phone number : {entry['phone']}"
        )
    return "\n\n".join(lines)