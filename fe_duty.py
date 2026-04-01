import csv
import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
load_dotenv()
# ================= 配置信息 =================
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
SPREADSHEET_TOKEN = os.getenv("FE_SPREADSHEET_TOKEN")
SHEET_ID = os.getenv("FE_SHEET_ID") 

# 要读取的范围（与之前一致，共31个单元格）
RANGES = [
    f"{SHEET_ID}!A2:J2",   
    f"{SHEET_ID}!A5:J5",   
    f"{SHEET_ID}!A8:J8",   
    f"{SHEET_ID}!A11:A11", 
]

DUTY_LIST_PATH = "dutyList.csv"   # 用于查询电话号码

# ================= API 函数 =================
def get_tenant_access_token():
    """获取 tenant_access_token"""
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"获取 token 失败: {result}")
    return result["tenant_access_token"]

def get_sheet_values(token):
    """读取多个范围的值，返回一维列表（按顺序平铺所有单元格）"""
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values_batch_get"
    params = {"ranges": ",".join(RANGES)}
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, params=params)
    result = resp.json()

    if result.get("code") != 0:
        print(f"请求失败: {result}")
        return None

    all_values = []
    for value_range in result["data"]["valueRanges"]:
        rows = value_range.get("values", [])
        for row in rows:
            all_values.extend(row)   # 直接添加该行的所有单元格
    return all_values

def get_phone_from_dutylist(name):
    """从 dutyList.csv 中查找姓名对应的电话号码"""
    if not os.path.exists(DUTY_LIST_PATH):
        return None
    try:
        with open(DUTY_LIST_PATH, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3 and row[0].strip() == name:
                    return row[2].strip()
    except Exception:
        return None
    return None

# ================= 新增辅助函数：解析多姓名并获取电话 =================
def parse_names_and_get_phones(raw_name):
    """
    处理可能包含多个姓名的字符串（以逗号分隔），返回 (name, phone) 列表。
    如果 raw_name 为空或为 "No duty"，则返回空列表。
    """
    if not raw_name or raw_name == "No duty":
        return []
    # 按逗号分割，并去除每个部分的首尾空格
    names = [n.strip() for n in raw_name.split(',') if n.strip()]
    result = []
    for name in names:
        phone = get_phone_from_dutylist(name)
        if phone is None:
            phone = "Not found"
        result.append((name, phone))
    return result

def format_duty_block(date_obj, names_phones):
    """
    生成单个日期的输出块，格式：
    📅 FE Schedule - March 20, 2026 Friday
    ========================================
    • Name1  (Phone: 123456)
    • Name2  (Phone: 789012)
    """
    # 格式化日期：例如 "March 20, 2026 Friday"
    
    date_str = date_obj.strftime("%B %d, %Y %A")
    
    lines = [f"📅 FE Schedule - {date_str}"]
    if not names_phones:
        lines.append("• No duty")
    else:
        for name, phone in names_phones:
            lines.append(f"• {name}  (Phone: {phone})")
            #lines.append("=" * 40)  # 分隔线，长度可调整
    return "\n".join(lines)

# ================= 主要功能函数 =================
def get_fe_today_duty():
    """返回 FE 当天值班信息（实时从 Lark 获取），以指定格式呈现"""
    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ Failed to get access token: {e}"

    values = get_sheet_values(token)
    if values is None:
        return "❌ Unable to read data from Lark sheet, please try again later."

    if len(values) < 31:
        return f"⚠️ Insufficient duty data (only {len(values)} days)."

    today = datetime.now().date()
    day = today.day
    if day < 1 or day > 31:
        return f"Invalid day: {day}."

    raw_name = values[day - 1].strip() if values[day - 1] else ""
    if not raw_name:
        raw_name = "No duty"

    names_phones = parse_names_and_get_phones(raw_name)
    return format_duty_block(today, names_phones)

def get_fe_next_three_duty():
    """返回今天、明天、后天三天的值班信息，以指定格式呈现（每个日期独立一块）"""
    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ Failed to get access token: {e}"

    values = get_sheet_values(token)
    if values is None:
        return "❌ Unable to read data from Lark sheet, please try again later."

    if len(values) < 31:
        return f"⚠️ Insufficient duty data (only {len(values)} days)."

    today = datetime.now().date()
    blocks = []
    for offset in range(3):
        day_date = today + timedelta(days=offset)
        if day_date.day > 31:   # 简单处理：如果超出本月31号，显示占位信息
            raw_name = "Out of month"
            names_phones = []   # 无姓名
        else:
            raw_name = values[day_date.day - 1].strip() if values[day_date.day - 1] else ""
            if not raw_name:
                raw_name = "No duty"
            names_phones = parse_names_and_get_phones(raw_name)
        blocks.append(format_duty_block(day_date, names_phones))

    return "\n\n".join(blocks)   # 每个块之间空一行

def fe_check(month=None, year=None):
    """
    检查 FE 值班表中指定月份（默认为当前月份）是否有空缺。
    注意：该脚本始终读取同一个固定表格，请确保表格已更新为要检查的月份的数据。
    """
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month

    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ Failed to get access token: {e}"

    values = get_sheet_values(token)
    if values is None:
        return "❌ Unable to read data from Lark sheet, please try again later."

    if len(values) < 31:
        return f"⚠️ 数据不完整（仅 {len(values)} 天）。"

    # 获取目标月份的总天数
    first_day = datetime(year, month, 1).date()
    if month == 12:
        next_month_first = datetime(year + 1, 1, 1).date()
    else:
        next_month_first = datetime(year, month + 1, 1).date()
    days_in_month = (next_month_first - first_day).days

    missing = []
    for day in range(1, days_in_month + 1):
        raw_name = values[day - 1].strip() if day - 1 < len(values) and values[day - 1] else ""
        if not raw_name or raw_name == "No duty":
            missing.append(day)

    month_name = datetime(year, month, 1).strftime("%B %Y")
    if not missing:
        return f"✅ All days in {month_name} have duty assigned."
    else:
        missing_str = ", ".join(str(d) for d in missing)
        return f"⚠️ {month_name} 缺少值班的日期：{missing_str}"

# 测试用
if __name__ == "__main__":
    print("=== Today's duty ===")
    print(get_fe_today_duty())
    print("\n=== Next three days duty ===")
    print(get_fe_next_three_duty())