from datetime import datetime
import csv
from dotenv import load_dotenv
load_dotenv()
def get_today_date():
    """Return today's date as YYYY-MM-DD."""
    return datetime.now().strftime("%Y-%m-%d")

# ---------- Holiday handling ----------
_holidays = None

def _load_holidays(csv_path='holiday.csv'):
    """Load holidays from CSV. Expected columns: Code, Name, Date (DD/MM/YYYY), Day."""
    holidays = []
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 4:
                    date_str = row[2].strip()
                    try:
                        sort_date = datetime.strptime(date_str, "%d/%m/%Y")
                    except ValueError:
                        sort_date = None  # fallback for malformed dates
                    holidays.append({
                        'code': row[0].strip(),
                        'name': row[1].strip(),
                        'date': date_str,
                        'day': row[3].strip(),
                        'sort_date': sort_date
                    })
        # Sort by actual date (invalid dates go to the end)
        holidays.sort(key=lambda x: x['sort_date'] or datetime.max)
        print(f"✅ Loaded {len(holidays)} holidays from {csv_path}")
    except FileNotFoundError:
        print(f"⚠️ {csv_path} not found. Holiday command will not work.")
    return holidays

def get_holidays():
    """Return the holiday list, loading it if necessary."""
    global _holidays
    if _holidays is None:
        _holidays = _load_holidays()
    return _holidays

def format_holidays():
    """Return a formatted string of all holidays (sorted by date)."""
    holidays = get_holidays()
    if not holidays:
        return "Holiday list is empty or not loaded."
    lines = []
    for h in holidays:
        lines.append(f"{h['name']} ({h['code']}) - {h['date']} ({h['day']})")
    return "\n".join(lines)

def holidays_this_month():
    """Return a formatted string of holidays occurring in the current month."""
    now = datetime.now()
    holidays = get_holidays()
    # Filter holidays that fall in the current month and year, and have a valid date
    this_month = [h for h in holidays if h['sort_date'] and h['sort_date'].year == now.year and h['sort_date'].month == now.month]
    # Sort by date
    this_month.sort(key=lambda h: h['sort_date'])
    
    if not this_month:
        return f"No holidays in {now.strftime('%B %Y')}."
    
    lines = [f"Holidays in {now.strftime('%B %Y')}:"]
    for h in this_month:
        lines.append(f"  {h['name']} ({h['code']}) - {h['date']} ({h['day']})")
    return "\n".join(lines)