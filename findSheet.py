#!/usr/bin/env python3
"""
列出飞书账号下所有可访问的电子表格，用于找到正确的 spreadsheet_token 和 sheet_id
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")

def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to get token: {result}")
    return result["tenant_access_token"]

def list_spreadsheets(token):
    """通过云文档驱动 API 列出所有电子表格"""
    url = "https://open.larksuite.com/open-apis/drive/v1/files"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "page_size": 100,
        "folder_type": "spreadsheet"   # 仅电子表格
    }
    resp = requests.get(url, headers=headers, params=params)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to list files: {result}")
    return result.get("data", {}).get("items", [])

def get_sheet_meta(token, spreadsheet_token):
    """获取电子表格中所有 sheet 的 ID 和名称"""
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        return []
    return result.get("data", {}).get("sheets", [])

def main():
    try:
        token = get_tenant_access_token()
        print("✅ 获取 token 成功\n")

        files = list_spreadsheets(token)
        if not files:
            print("没有找到任何电子表格。")
            return

        print(f"找到 {len(files)} 个电子表格：\n")
        for idx, file in enumerate(files, 1):
            title = file.get("name", "无标题")
            spreadsheet_token = file.get("token")
            print(f"{idx}. {title}")
            print(f"   Spreadsheet Token: {spreadsheet_token}")
            print(f"   完整链接: https://casinoplus.sg.larksuite.com/sheets/{spreadsheet_token}")

            # 获取该表格内所有工作表的 ID
            sheets_meta = get_sheet_meta(token, spreadsheet_token)
            if sheets_meta:
                print("   工作表列表:")
                for s in sheets_meta:
                    print(f"     - ID: {s.get('sheetId')}  名称: {s.get('title')}")
            print()

    except Exception as e:
        print(f"❌ 出错: {e}")

if __name__ == "__main__":
    main()