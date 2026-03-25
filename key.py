import requests

# ========== CONFIGURATION (replace with your own) ==========
APP_ID = "cli_a9ca652b89b85ed1"
APP_SECRET = "VQJh0oFKfsyCHr5tQDMVNbr4o4kmjbFr"
# ============================================================

def get_tenant_access_token():
    """Fetch a tenant access token from Lark."""
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    response = requests.post(url, headers=headers, json=data)
    result = response.json()
    if result.get("code") != 0:
        print(f"❌ Failed to get tenant token: {result}")
        return None
    token = result.get("tenant_access_token")
    print(f"✅ Obtained tenant access token (first 20 chars): {token[:20]}...")
    return token

def verify_token(token):
    """Test the token by calling a simple API (e.g., get bot's user info)."""
    url = "https://open.larksuite.com/open-apis/contact/v3/users/me"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    result = response.json()
    if result.get("code") == 0:
        print("✅ Token is valid. Bot user info:")
        print(f"   Open ID: {result['data']['user']['open_id']}")
        print(f"   Name: {result['data']['user']['name']}")
    else:
        print(f"❌ Token test failed: {result}")

if __name__ == "__main__":
    token = get_tenant_access_token()
    if token:
        print(f"Full token: {token}")
        print(f"Token length: {len(token)}")
        verify_token(token)
    else:
        print("❌ Could not obtain tenant access token.")