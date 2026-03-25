#!/usr/bin/env python3
"""
Find a user's open_id by their email address (checks all email fields).
"""

import sys
import requests

APP_ID = "cli_a9ca652b89b85ed1"
APP_SECRET = "VQJh0oFKfsyCHr5tQDMVNbr4o4kmjbFr"

def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to get token: {result}")
    return result["tenant_access_token"]

def get_all_users():
    """Fetch all users in the tenant (paginated)."""
    token = get_tenant_access_token()
    url = "https://open.larksuite.com/open-apis/contact/v3/users"
    headers = {"Authorization": f"Bearer {token}"}
    users = []
    page_token = None
    while True:
        params = {"page_size": 100, "department_id_type": "open_department_id"}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            raise Exception(f"Failed to list users: {resp.status_code} {resp.text}")
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"List users error: {data}")
        items = data.get("data", {}).get("items", [])
        users.extend(items)
        if not data.get("data", {}).get("has_more"):
            break
        page_token = data["data"]["page_token"]
    return users

def get_open_id_by_email(email):
    """Search all email fields for the exact email (case‑insensitive)."""
    users = get_all_users()
    email_lower = email.lower()
    for user in users:
        # Check all possible email fields
        possible_emails = [
            user.get("email"),
            user.get("mobile_visible_email"),
            user.get("enterprise_email")
        ]
        # Also handle lists if any
        for e in possible_emails:
            if e and isinstance(e, str) and e.lower() == email_lower:
                return user.get("open_id"), user.get("name")
    return None, None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python find_user_by_email.py user@example.com")
        sys.exit(1)
    email = sys.argv[1].strip()
    try:
        open_id, name = get_open_id_by_email(email)
        if open_id:
            print(f"✅ Found user: {name}")
            print(f"   open_id: {open_id}")
            print(f"   Mention: <at user_id=\"{open_id}\">{name}</at>")
        else:
            print(f"❌ No user found with email {email}")
    except Exception as e:
        print(f"❌ Error: {e}")