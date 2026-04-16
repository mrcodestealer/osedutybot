import csv
import requests
from datetime import datetime, timedelta
import os
from calendar import monthrange
from dotenv import load_dotenv
load_dotenv()
# ================= 配置信息 =================
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
SPREADSHEET_TOKEN = os.getenv("BI_SPREADSHEET_TOKEN")
SHEET_ID = os.getenv("BI_SHEET_ID")
DUTY_LIST_PATH = "dutyList.csv"

# ================= 月份映射 =================
MONTH_MAP = {
    1: [  # 1月
        ("I5:I5", "B5:H5"),
        ("I6:I6", "B6:H6"),
        ("I7:I7", "B7:H7"),
        ("I8:I8", "B8:H8"),
        ("I9:I9", "B9:H9"),
    ],
    2: [  # 2月
        ("S5:S5", "L5:R5"),
        ("S6:S6", "L6:R6"),
        ("S7:S7", "L7:R7"),
        ("S8:S8", "L8:R8"),
        ("S9:S9", "L9:R9"),
    ],
    3: [  # 三月
        ("AC5:AC5", "V5:AB5"),
        ("AC6:AC6", "V6:AB6"),
        ("AC7:AC7", "V7:AB7"),
        ("AC8:AC8", "V8:AB8"),
        ("AC9:AC9", "V9:AB9"),
    ],
    4: [  # 四月
        ("I15:I15", "B15:H15"),
        ("I16:I16", "B16:H16"),
        ("I17:I17", "B17:H17"),
        ("I18:I18", "B18:H18"),
        ("I19:I19", "B19:H19"),
    ],
    5: [  # 五月
        ("S15:S15", "L15:R15"),
        ("S16:S16", "L16:R16"),
        ("S17:S17", "L17:R17"),
        ("S18:S18", "L18:R18"),
        ("S19:S19", "L19:R19"),
    ],
    6: [  # 六月
        ("AC15:AC15", "V15:AB15"),
        ("AC16:AC16", "V16:AB16"),
        ("AC17:AC17", "V17:AB17"),
        ("AC18:AC18", "V18:AB18"),
        ("AC19:AC19", "V19:AB19"),
    ],
    7: [  # 七月
        ("I25:I25", "B25:H25"),
        ("I26:I26", "B26:H26"),
        ("I27:I27", "B27:H27"),
        ("I28:I28", "B28:H28"),
        ("I29:I29", "B29:H29"),
    ],
    8: [  # 八月
        ("S25:S25", "L25:R25"),
        ("S26:S26", "L26:R26"),
        ("S27:S27", "L27:R27"),
        ("S28:S28", "L28:R28"),
        ("S29:S29", "L29:R29"),
    ],
    9: [  # 九月
        ("AC25:AC25", "V25:AB25"),
        ("AC26:AC26", "V26:AB26"),
        ("AC27:AC27", "V27:AB27"),
        ("AC28:AC28", "V28:AB28"),
        ("AC29:AC29", "V29:AB29"),
    ],
    10: [ # 十月
        ("I35:I35", "B35:H35"),
        ("I36:I36", "B36:H36"),
        ("I37:I37", "B37:H37"),
        ("I38:I38", "B38:H38"),
        ("I39:I39", "B39:H39"),
    ],
    11: [ # 十一月
        ("S35:S35", "L35:R35"),
        ("S36:S36", "L36:R36"),
        ("S37:S37", "L37:R37"),
        ("S38:S38", "L38:R38"),
        ("S39:S39", "L39:R39"),
    ],
    12: [ # 十二月
        ("AC35:AC35", "V35:AB35"),
        ("AC36:AC36", "V36:AB36"),
        ("AC37:AC37", "V37:AB37"),
        ("AC38:AC38", "V38:AB38"),
        ("AC39:AC39", "V39:AB39"),
    ],
}

# ================= API 函数 =================
def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"获取 token 失败: {result}")
    return result["tenant_access_token"]

def get_range_values(token, range_str):
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{range_str}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        print(f"范围 {range_str} 请求失败: {result}")
        return None
    return result.get("data", {}).get("valueRange", {}).get("values", [])

def get_phone(name):
    if not os.path.exists(DUTY_LIST_PATH):
        return "未找到电话号码文件"
    try:
        with open(DUTY_LIST_PATH, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3 and row[0].strip() == name:
                    return row[2].strip()
    except Exception:
        pass
    return "未找到电话号码"

def _get_bi_duty_rows(month, year, token):
    """
    For a given month and year, return a list of tuples:
        (start_date, end_date, name)
    """
    if month not in MONTH_MAP:
        return []
    rows = []
    for name_range, date_range in MONTH_MAP[month]:
        # Get name
        name_values = get_range_values(token, f"{SHEET_ID}!{name_range}")
        if not name_values or not name_values[0]:
            continue
        name_cell = name_values[0][0]
        # Check if name cell is None or empty
        if name_cell is None:
            continue
        name = str(name_cell).strip()
        if not name:
            continue

        # Get date list (single row)
        date_values = get_range_values(token, f"{SHEET_ID}!{date_range}")
        if not date_values or not date_values[0]:
            continue
        dates = []
        for d in date_values[0]:
            if d is not None and str(d).strip():
                try:
                    dates.append(int(d))
                except:
                    pass
        if not dates:
            continue

        start_day = min(dates)
        end_day = max(dates)
        _, max_days = monthrange(year, month)
        if start_day > max_days or end_day > max_days:
            continue
        try:
            start_date = datetime(year, month, start_day).date()
            end_date = datetime(year, month, end_day).date()
            rows.append((start_date, end_date, name))
        except ValueError:
            continue
    return rows

def get_bi_today_duty():
    """Return the next three BI duty periods with their date ranges, spanning months if needed."""
    today = datetime.now().date()
    current_year = today.year
    current_month = today.month

    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ 获取访问令牌失败: {e}"

    upcoming = []
    # Look at current month and possibly next month(s) until we have at least 3 upcoming rows
    year = current_year
    month = current_month
    months_checked = 0
    while len(upcoming) < 3 and months_checked < 2:  # limit to current + next month
        rows = _get_bi_duty_rows(month, year, token)
        # Filter rows that have end_date >= today (active or future)
        for r in rows:
            if r[1] >= today:
                upcoming.append(r)
        # Move to next month
        month += 1
        if month > 12:
            month = 1
            year += 1
        months_checked += 1

    if not upcoming:
        return "📅 没有即将到来的 BI 值班。"

    # Sort by start date and take first 3
    upcoming.sort(key=lambda x: x[0])
    upcoming = upcoming[:3]

    output_lines = []
    for start_date, end_date, name in upcoming:
        range_str = f"{start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}"
        output_lines.append(f"📅 BI Duty {range_str}")
        phone = get_phone(name)
        output_lines.append(f"• {name} 📞 {phone}")
        output_lines.append("")

    return "\n".join(output_lines).strip()

def bi_check(month=None, year=None):
    """
    Check if any day in the given month (default current month) has no BI duty.
    Returns a string listing missing dates or confirmation.
    """
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month

    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ 获取访问令牌失败: {e}"

    # Get all duty rows for the month
    rows = _get_bi_duty_rows(month, year, token)
    if not rows:
        return f"❌ 没有找到 {year}年{month}月的 BI 值班数据。"

    # Collect all days covered by these ranges
    covered = set()
    for start, end, name in rows:
        day = start
        while day <= end:
            covered.add(day.day)
            day += timedelta(days=1)

    # Get all days in month
    _, max_days = monthrange(year, month)
    all_days = set(range(1, max_days + 1))

    missing = sorted(all_days - covered)

    month_name = datetime(year, month, 1).strftime("%B %Y")
    if not missing:
        return f"✅ {month_name} 所有日期都有 BI 值班。"
    else:
        missing_str = ", ".join(str(d) for d in missing)
        return f"⚠️ {month_name} detected missing duty\nMissing Date：{missing_str}"

if __name__ == "__main__":
    print(get_bi_today_duty())