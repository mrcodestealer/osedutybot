"""Lark user_access_token (OAuth 2.0) helper.

Some Open APIs (e.g. adding a comment/note to a spreadsheet CELL) reject the app
``tenant_access_token`` and require a *user* identity. This module performs the one-time
authorization-code exchange and then keeps a valid ``user_access_token`` available by
refreshing it with the stored ``refresh_token`` (single-use, rotated on every refresh).

One-time setup (see ``user_token_setup.py``):
    1. In the Lark developer console add the redirect URL and enable the scopes
       (``drive:drive`` + ``offline_access`` + ``docs:document.comment:write_only``),
       then publish the app.
    2. ``python user_token_setup.py url``  -> open the printed link, authorize.
    3. Copy the ``code`` from the redirected URL.
    4. ``python user_token_setup.py code <CODE>``  -> stores the tokens.

Runtime: ``get_user_access_token()`` returns a valid token (refreshing as needed) or
``None`` when no/expired authorization exists, so callers can degrade gracefully.
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.parse
from typing import Any, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")

_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_PATH = os.path.join(_DIR, "user_token.json")

OAUTH_TOKEN_URL = "https://open.larksuite.com/open-apis/authen/v2/oauth/token"
AUTHORIZE_URL = "https://accounts.larksuite.com/open-apis/authen/v1/authorize"

# drive:drive — sheet access; offline_access — refresh_token;
# docs:document.comment:write_only — add cell comments (required by new_comments API).
DEFAULT_SCOPES = os.getenv(
    "LARK_OAUTH_SCOPES",
    "drive:drive offline_access docs:document.comment:write_only",
)
DEFAULT_REDIRECT_URI = os.getenv(
    "LARK_OAUTH_REDIRECT_URI", "https://example.com/api/oauth/callback"
)

_LOCK = threading.Lock()
_WARNED_MISSING = False


def build_authorize_url(
    redirect_uri: Optional[str] = None, scopes: Optional[str] = None, state: str = "ose-bot"
) -> str:
    if not APP_ID:
        raise RuntimeError("APP_ID is not set in environment")
    params = {
        "client_id": APP_ID,
        "redirect_uri": redirect_uri or DEFAULT_REDIRECT_URI,
        "scope": scopes or DEFAULT_SCOPES,
        "state": state,
    }
    return AUTHORIZE_URL + "?" + urllib.parse.urlencode(params, quote_via=urllib.parse.quote)


def _load() -> dict[str, Any]:
    try:
        with open(TOKEN_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _save(data: dict[str, Any]) -> None:
    tmp = TOKEN_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    os.replace(tmp, TOKEN_PATH)


def _store_token_response(resp: dict[str, Any]) -> dict[str, Any]:
    now = int(time.time())
    access = str(resp.get("access_token") or "")
    if not access:
        raise RuntimeError(f"token response missing access_token: {resp}")
    data = _load()
    data["access_token"] = access
    data["access_expires_at"] = now + int(resp.get("expires_in") or 0)
    data["scope"] = resp.get("scope") or data.get("scope") or ""
    data["obtained_at"] = now
    # refresh_token is only returned when offline_access was granted; rotate when present.
    if resp.get("refresh_token"):
        data["refresh_token"] = str(resp.get("refresh_token"))
        data["refresh_expires_at"] = now + int(resp.get("refresh_token_expires_in") or 0)
    _save(data)
    return data


def parse_authorization_code(raw: str) -> str:
    """Accept a bare ``code`` or a full redirect URL and return the code value."""
    s = (raw or "").strip().strip('"').strip("'")
    if not s:
        raise ValueError("authorization code is empty")
    if "://" in s or s.startswith("?"):
        parsed = urllib.parse.urlparse(s if "://" in s else f"https://x.invalid/{s.lstrip('?')}")
        qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=False)
        codes = qs.get("code") or []
        if not codes or not str(codes[0]).strip():
            raise ValueError(
                "could not find ?code= in the pasted URL — copy only the code value, "
                "or paste the full redirect URL inside double quotes"
            )
        return str(codes[0]).strip()
    return s


def exchange_code(code: str, redirect_uri: Optional[str] = None) -> dict[str, Any]:
    """Exchange an authorization ``code`` for tokens and persist them."""
    if not APP_ID or not APP_SECRET:
        raise RuntimeError("APP_ID / APP_SECRET not set in environment")
    auth_code = parse_authorization_code(code)
    body = {
        "grant_type": "authorization_code",
        "client_id": APP_ID,
        "client_secret": APP_SECRET,
        "code": auth_code,
        "redirect_uri": redirect_uri or DEFAULT_REDIRECT_URI,
    }
    res = requests.post(OAUTH_TOKEN_URL, json=body, timeout=30).json()
    if res.get("code") not in (0, None) or not res.get("access_token"):
        raise RuntimeError(f"authorization_code exchange failed: {res}")
    return _store_token_response(res)


def _refresh(refresh_token: str) -> Optional[str]:
    if not APP_ID or not APP_SECRET or not refresh_token:
        return None
    body = {
        "grant_type": "refresh_token",
        "client_id": APP_ID,
        "client_secret": APP_SECRET,
        "refresh_token": refresh_token,
    }
    try:
        res = requests.post(OAUTH_TOKEN_URL, json=body, timeout=30).json()
    except Exception as exc:
        print(f"[user_token] refresh request failed: {exc!r}", flush=True)
        return None
    if res.get("code") not in (0, None) or not res.get("access_token"):
        print(f"[user_token] refresh rejected: {res}", flush=True)
        return None
    data = _store_token_response(res)
    return data.get("access_token")


def get_user_access_token() -> Optional[str]:
    """Return a valid ``user_access_token`` (refreshing if needed) or ``None``."""
    global _WARNED_MISSING
    with _LOCK:
        data = _load()
        now = int(time.time())
        access = str(data.get("access_token") or "")
        access_exp = int(data.get("access_expires_at") or 0)
        if access and now < access_exp - 120:
            return access
        refresh_token = str(data.get("refresh_token") or "")
        refresh_exp = int(data.get("refresh_expires_at") or 0)
        if not refresh_token:
            if not _WARNED_MISSING:
                _WARNED_MISSING = True
                print(
                    "[user_token] no authorization yet — run "
                    "`python user_token_setup.py url` then `code <CODE>`",
                    flush=True,
                )
            return access or None
        if refresh_exp and now >= refresh_exp - 120:
            print(
                "[user_token] refresh_token expired — re-run the authorization flow "
                "(`python user_token_setup.py url`)",
                flush=True,
            )
            return None
        return _refresh(refresh_token)


def status() -> dict[str, Any]:
    data = _load()
    now = int(time.time())
    return {
        "has_token_file": bool(data),
        "has_refresh_token": bool(data.get("refresh_token")),
        "access_valid": bool(data.get("access_token")) and now < int(data.get("access_expires_at") or 0),
        "access_expires_in": max(0, int(data.get("access_expires_at") or 0) - now),
        "refresh_expires_in": max(0, int(data.get("refresh_expires_at") or 0) - now),
        "scope": data.get("scope") or "",
    }
