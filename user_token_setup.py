"""One-time setup for the Lark user_access_token used to add offset cell notes.

Run on the SERVER (where APP_ID/APP_SECRET are set):

    python user_token_setup.py url                 # print the authorization link
    python user_token_setup.py code <CODE>         # exchange the code -> store tokens
    python user_token_setup.py code "<FULL_URL>"   # or paste the whole redirect URL (Windows: use quotes)
    python user_token_setup.py status              # show token validity / scope
    python user_token_setup.py test <row> <col>    # post a test note with the user token

Before this, in the Lark developer console:
    - Security settings: add the redirect URL (LARK_OAUTH_REDIRECT_URI, default
      https://example.com/api/oauth/callback). The page may 404 — that's fine, just copy
      the `code` query param from the redirected URL.
    - Permissions: enable ``drive:drive``, ``offline_access``, and
      ``docs:document.comment:write_only`` (回复/修改/删除云文档评论), then publish.
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

    elif cmd == "test":
        if len(sys.argv) < 4:
            print("usage: python user_token_setup.py test <row> <col>   (0-based)")
            return
        import ose_Duty as od

        row, col = int(sys.argv[2]), int(sys.argv[3])
        info = od._post_ose_shift_sheet_cell_note(
            ut.get_user_access_token() or "", row, col, "TEST offset note"
        )
        print("note created:", json.dumps(info, ensure_ascii=False))

    else:
        print(__doc__)


if __name__ == "__main__":
    main()
