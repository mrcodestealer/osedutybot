import requests
import json

# ================= 配置信息 =================
APP_ID = "cli_a9ca652b89b85ed1"
APP_SECRET = "VQJh0oFKfsyCHr5tQDMVNbr4o4kmjbFr"
SPREADSHEET_TOKEN = "EywEsYs6vhMaGQtsbBHluy5cgGg"   # 表格 token
SHEET_ID = "gDCoYb"                                 # TIMETABLE 工作表 ID

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

def get_sheet_metadata(token):
    """获取表格元数据，打印工作表信息"""
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/metainfo"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    print("📄 表格元数据:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result

def test_range(token, range_str):
    """测试单个范围，打印结果"""
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{range_str}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    print(f"\n🔍 测试范围: {range_str}")
    print(f"状态码: {resp.status_code}")
    print(f"响应: {json.dumps(result, indent=2, ensure_ascii=False)}")
    return result

def main():
    print("正在获取 tenant_access_token...")
    token = get_tenant_access_token()
    print("✅ token 获取成功")

    # 1. 获取元数据，查看工作表列数
    metadata = get_sheet_metadata(token)
    if metadata.get("code") != 0:
        print("❌ 获取元数据失败，请检查权限或 token")
        return

    # 找到对应 sheet 的 gridProperties
    sheets = metadata.get("data", {}).get("sheets", [])
    target_sheet = None
    for sheet in sheets:
        if sheet.get("sheetId") == SHEET_ID:
            target_sheet = sheet
            break
    if target_sheet:
        props = target_sheet.get("gridProperties", {})
        print(f"\n📊 工作表 {SHEET_ID} 属性: 行数={props.get('rowCount')}, 列数={props.get('columnCount')}")
    else:
        print(f"⚠️ 未找到 sheetId={SHEET_ID} 的工作表，请检查 ID 是否正确")

    # 2. 测试简单范围 A1
    test_range(token, f"{SHEET_ID}!A1")

    # 3. 测试 AC8
    test_range(token, f"{SHEET_ID}!AC8")

    # 4. 测试 V8:AB8
    test_range(token, f"{SHEET_ID}!V8:AB8")

    # 5. 测试更大的范围 A1:AC10，看是否包含 AC8
    test_range(token, f"{SHEET_ID}!A1:AC10")

if __name__ == "__main__":
    main()