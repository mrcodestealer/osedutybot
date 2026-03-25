import requests
import os
from dotenv import load_dotenv
load_dotenv()
# ================= 配置信息 =================
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
SPREADSHEET_TOKEN = ""  # 从您的 wiki 链接提取的 token

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

def get_spreadsheet_metadata(token, spreadsheet_token):
    """获取表格元数据，返回所有工作表信息"""
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    
    if result.get("code") != 0:
        print(f"请求失败: {result}")
        return None
    
    return result.get("data", {})

def main():
    print("正在获取 tenant_access_token...")
    token = get_tenant_access_token()
    
    print("正在获取表格元数据...")
    metadata = get_spreadsheet_metadata(token, SPREADSHEET_TOKEN)
    
    if metadata is None:
        print("❌ 获取元数据失败")
        return
    
    print("\n✅ 获取成功！\n")
    print(f"表格标题: {metadata['properties']['title']}")
    print(f"工作表数量: {metadata['properties']['sheetCount']}")
    print("\n所有工作表信息:")
    print("-" * 50)
    
    for sheet in metadata["sheets"]:
        print(f"工作表索引: {sheet['index']}")
        print(f"工作表标题: {sheet['title']}")
        print(f"工作表 ID: {sheet['sheetId']}")
        print(f"行数: {sheet['rowCount']}, 列数: {sheet['columnCount']}")
        print("-" * 50)

if __name__ == "__main__":
    main()