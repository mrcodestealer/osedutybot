import csv
import requests

# ================= 配置信息 =================
APP_ID = "cli_a9ca652b89b85ed1"
APP_SECRET = "VQJh0oFKfsyCHr5tQDMVNbr4o4kmjbFr"
SPREADSHEET_TOKEN = "QeSuwegKvijF7ek4c2kl1QLDgoe"

SHEET_ID = "HoMkYJ"  # 选择 "Latest Duty List"

# 要读取的范围（每个范围按顺序，不额外填充）
RANGES = [
    f"{SHEET_ID}!A2:J2",   # 第2行，10列
    f"{SHEET_ID}!A5:J5",   # 第5行，10列
    f"{SHEET_ID}!A8:J8",   # 第8行，10列
    f"{SHEET_ID}!A11:A11", # 第11行，1列
]

OUTPUT_CSV = "fe.csv"

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

def get_sheet_values(token, spreadsheet_token, ranges):
    """读取多个范围的值，返回一维列表（按范围顺序平铺所有单元格，不额外填充）"""
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_get"
    params = {"ranges": ",".join(ranges)}
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
            # 直接添加该行的所有单元格（不填充固定长度）
            all_values.extend(row)
    return all_values

def write_single_column(values, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for val in values:
            writer.writerow([val])
    print(f"✅ 成功导出 {len(values)} 个单元格到 {output_file}")
    print("预览前10个值：")
    for i, val in enumerate(values[:10]):
        print(f"  {i+1}: {val}")

def main():
    print("正在获取 tenant_access_token...")
    token = get_tenant_access_token()

    print("正在从普通表格读取数据...")
    values = get_sheet_values(token, SPREADSHEET_TOKEN, RANGES)
    if values is None:
        print("❌ 读取失败")
        return

    print(f"读取到 {len(values)} 个单元格。")
    write_single_column(values, OUTPUT_CSV)

if __name__ == "__main__":
    main()