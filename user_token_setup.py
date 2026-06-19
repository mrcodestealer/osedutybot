"""One-time setup for the Lark user_access_token (OAuth).

Run on the SERVER (where APP_ID/APP_SECRET are set):

    python user_token_setup.py url                 # print the authorization link
    python user_token_setup.py code <CODE>         # exchange the code -> store tokens
    python user_token_setup.py code "<FULL_URL>"   # or paste the whole redirect URL (Windows: use quotes)
    python user_token_setup.py status              # show token validity / scope

Before this, in the Lark developer console:
    - Security settings: add the redirect URL (LARK_OAUTH_REDIRECT_URI, default
      https://example.com/api/oauth/callback). The page may 404 — that's fine, just copy
      the `code` query param from the redirected URL.
    - Permissions: enable required scopes (e.g. ``drive:drive``, ``offline_access``,
      ``calendar:calendar:readonly`` for holiday sync), then publish.
"""
import json
import sys

import user_token as ut


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        return
    cmd = sys.argv[1].lower()

    if cmd == "url":
        print("Redirect URI:", ut.DEFAULT_REDIRECT_URI)
        print("Scopes:", ut.DEFAULT_SCOPES)
        print("\nOpen this link in a browser, authorize, then copy the `code` from the result URL:\n")
        print(ut.build_authorize_url())

    elif cmd == "code":
        if len(sys.argv) < 3:
            print("usage: python user_token_setup.py code <CODE-or-URL>")
            print('Windows example: python user_token_setup.py code "0IEnIcC2fbHEkJ64..."')
            print('Or full URL:     python user_token_setup.py code "https://example.com/api/oauth/callback?code=...&state=ose-bot"')
            return
        raw = " ".join(sys.argv[2:]).strip()
        try:
            parsed = ut.parse_authorization_code(raw)
        except ValueError as exc:
            print(f"❌ {exc}")
            return
        print(f"Using authorization code: {parsed[:8]}... (len={len(parsed)})")
        data = ut.exchange_code(parsed)
        print("Stored tokens to", ut.TOKEN_PATH)
        print("scope:", data.get("scope"))
        print("access expires_at:", data.get("access_expires_at"))
        print("has refresh_token:", bool(data.get("refresh_token")))

    elif cmd == "status":
        print(json.dumps(ut.status(), ensure_ascii=False, indent=2))

    else:
        print(__doc__)


if __name__ == "__main__":
    main()
