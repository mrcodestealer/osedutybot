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
    """Search duty_list using: exact match, then substring match, then fuzzy match."""
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

    query_norm = query.lower().replace(' ', '')

    # 1. Exact match
    exact_matches = []
    for entry in duty_list:
        name_norm = entry['name'].lower().replace(' ', '')
        if query_norm == name_norm:
            exact_matches.append(entry)

    if exact_matches:
        lines = [f"Name : {e['name']}\nDepartment : {e['department']}\nPhone number : {e['phone']}" for e in exact_matches]
        return "\n\n".join(lines)

    # 2. Substring match
    substring_matches = []
    for entry in duty_list:
        name_norm = entry['name'].lower().replace(' ', '')
        if query_norm in name_norm:
            substring_matches.append(entry)

    if substring_matches:
        lines = [f"Name : {e['name']}\nDepartment : {e['department']}\nPhone number : {e['phone']}" for e in substring_matches]
        return "\n\n".join(lines)

    # 3. Fuzzy match
    THRESHOLD = 0.8
    fuzzy_matches = []
    for entry in duty_list:
        name_norm = entry['name'].lower().replace(' ', '')
        similarity = difflib.SequenceMatcher(None, query_norm, name_norm).ratio()
        if similarity >= THRESHOLD:
            fuzzy_matches.append((similarity, entry))

    if not fuzzy_matches:
        return f"No matching duty personnel found for '{query}'."

    fuzzy_matches.sort(key=lambda x: x[0], reverse=True)
    lines = [f"Name : {e['name']}\nDepartment : {e['department']}\nPhone number : {e['phone']}" for sim, e in fuzzy_matches]
    return "\n\n".join(lines)