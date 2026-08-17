#!/usr/bin/env python3
"""Microsoft Teams (personal / teams.live.com) group watcher — READ-ONLY.

PHASE 1 — login only. This file proves we can hold a Teams web session on the
server. Message capture lands in phase 2 once a session is confirmed.

Why Playwright and not Microsoft Graph: the account is a *consumer* MSA
(``login.live.com``, ``tenant=consumers``, ``teams.live.com``) — free Teams, not
a work tenant. Graph's chat-message scopes need an Azure AD app registration and
admin consent inside the tenant that owns the chat, which does not exist for a
personal account. The browser is the only door.

Why a persistent user-data-dir and not ``storage_state`` like osmwatch.py: Teams
web keeps its MSAL refresh token in **IndexedDB**. ``context.storage_state()``
serialises cookies + localStorage only, so a storage_state session comes back
logged *out*. ``launch_persistent_context`` keeps the whole profile.

Nothing is ever sent on the Teams side — no message send, no reactions, no
typing. Note the account WILL show as online to the group while a session is
live, so use a dedicated account, not a person's daily one.

Run on the server (login is scripted from .env, no OTP unless the account has
2FA turned on):

    python teamswatch.py --login          # do the sign-in, save the profile
    python teamswatch.py --login --headed # same, visible browser (local debug)
    python teamswatch.py --check          # is the saved profile still signed in?
    python teamswatch.py --status         # what this module thinks right now

Credentials come from ``.env`` (TEAMS_EMAIL / TEAMS_PASSWORD) and are never
written to disk or logged by this module.

Exposed to main.py:
    send_status_to_lark(chat_id) -> /teamstatus (text summary + PNG)
    status_lines()               -> list[str] summary
    is_monitoring()              -> bool
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

# Windows consoles default to cp1252 and choke on the emoji below; the Linux
# server is UTF-8 already, so this is a no-op there.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    except Exception:
        pass

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - dotenv is a declared dep
    load_dotenv = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Paths / env
# ---------------------------------------------------------------------------
_ROOT_DIR = Path(__file__).resolve().parent
_ENV_PATH = _ROOT_DIR / ".env"
if load_dotenv is not None:
    load_dotenv(str(_ENV_PATH))

# The Chromium profile IS the credential once login is done. Gitignored, and
# worth a tar backup so a rebuild doesn't need a fresh sign-in.
PROFILE_DIR = _ROOT_DIR / os.getenv("TEAMS_PROFILE_DIR", "teamswatch_profile")
SHOTS_DIR = _ROOT_DIR / "teamswatch_shots"
_STATE_PATH = _ROOT_DIR / "teamswatch_state.json"

# Entry point. Do NOT hardcode a captured login.live.com URL here: those carry
# single-use code_challenge / nonce / state / epct params that expire in minutes.
# Loading teams.live.com lets MSAL mint fresh ones.
TEAMS_URL = os.getenv("TEAMS_ENTRY_URL", "https://teams.live.com/v2/")

# Same Lark group + person as the Telegram watcher alerts (telegramwatch.py:79).
ALERT_CHAT_ID = os.getenv(
    "TEAMS_ALERT_CHAT_ID", "oc_ad9b5bdbb2826ba2ee9730920ef25432"
).strip()
ALERT_OPEN_ID = os.getenv(
    "TEAMS_ALERT_OPEN_ID", "ou_5f660c0fb0769d184aca635d02209272"
).strip()

# Headless Chromium's default UA says "HeadlessChrome", which Teams web rejects
# as an unsupported browser. Pin a normal desktop Chrome UA.
_UA = os.getenv(
    "TEAMS_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
)


def _truthy(val: str | None) -> bool:
    return str(val or "").strip().lower() in ("1", "true", "yes", "on", "y")


def _tz():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(os.getenv("TEAMS_WATCH_TZ", "Asia/Manila"))
    except Exception:
        return timezone.utc


def _now_str() -> str:
    return datetime.now(_tz()).strftime("%Y-%m-%d %H:%M:%S")


def _creds() -> tuple[str, str]:
    """Account to sign in as. Never logged, never persisted by this module."""
    email = (os.getenv("TEAMS_EMAIL", "") or "").strip()
    password = os.getenv("TEAMS_PASSWORD", "") or ""
    if not email or not password:
        raise RuntimeError(
            "TEAMS_EMAIL / TEAMS_PASSWORD not set in .env — add them on the "
            "server (.env is gitignored; never commit the password)."
        )
    return email, password


def _login_timeout_s() -> int:
    try:
        return max(30, int(os.getenv("TEAMS_LOGIN_TIMEOUT", "180")))
    except ValueError:
        return 180


# ---------------------------------------------------------------------------
# Shared state (/teamstatus reads this)
# ---------------------------------------------------------------------------
_lock = threading.Lock()
_state: dict[str, Any] = {
    "phase": "idle",        # idle | logging_in | monitoring | login_failed | stopped
    "detail": "not started",
    "account": None,
    "started_at": None,
    "connected_at": None,
    "last_error": None,
    "last_stage": None,
    "last_shot": None,
    "alerted": False,
    # Written ONLY by do_login(). check_session() runs far more often (every
    # /teamstatus), and without a separate slot its generic "not signed in"
    # would overwrite the one message that says *why* the login failed —
    # "2FA required", "password rejected" — which is the actionable part.
    "last_login_ok": None,
    "last_login_stage": None,
    "last_login_reason": None,
    "last_login_at": None,
}

_PERSIST_KEYS = (
    "phase", "detail", "account", "connected_at", "last_error", "last_stage",
    "last_shot", "last_login_ok", "last_login_stage", "last_login_reason",
    "last_login_at",
)

_PHASE_EMOJI = {
    "idle": "⚪",
    "logging_in": "🟡",
    "monitoring": "🟢",
    "login_failed": "🔴",
    "stopped": "⚫",
}


def _set(**kw: Any) -> None:
    with _lock:
        _state.update(kw)


def _snapshot() -> dict[str, Any]:
    with _lock:
        return dict(_state)


def is_monitoring() -> bool:
    with _lock:
        return _state["phase"] == "monitoring"


def _persist() -> None:
    snap = _snapshot()
    payload = {k: snap.get(k) for k in _PERSIST_KEYS}
    payload["saved_at"] = _now_str()
    try:
        _STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception as err:
        print(f"[teams] state persist failed: {err!r}", flush=True)


def _load_persisted() -> None:
    try:
        data = json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return
    with _lock:
        for key in _PERSIST_KEYS:
            if data.get(key) is not None:
                _state[key] = data[key]


_load_persisted()


# ---------------------------------------------------------------------------
# Lark out (self-contained; uses this bot's APP_ID/APP_SECRET, like osmwatch.py)
# ---------------------------------------------------------------------------
def _lark_base() -> str:
    return os.getenv("LARK_OPEN_BASE", "https://open.larksuite.com").rstrip("/")


def _tenant_token() -> str:
    app_id = os.getenv("APP_ID")
    app_secret = os.getenv("APP_SECRET")
    if not app_id or not app_secret:
        raise RuntimeError("APP_ID / APP_SECRET not set in environment (.env)")
    url = f"{_lark_base()}/open-apis/auth/v3/tenant_access_token/internal"
    result = requests.post(
        url, json={"app_id": app_id, "app_secret": app_secret}, timeout=30
    ).json()
    if result.get("code") != 0:
        raise RuntimeError(f"Failed to get tenant token: {result}")
    return result["tenant_access_token"]


def send_text(chat_id: str, text: str) -> dict:
    token = _tenant_token()
    return requests.post(
        f"{_lark_base()}/open-apis/im/v1/messages",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        params={"receive_id_type": "chat_id"},
        json={
            "receive_id": chat_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}),
        },
        timeout=30,
    ).json()


def upload_image_lark(image_path: str) -> str | None:
    token = _tenant_token()
    mime, _ = mimetypes.guess_type(image_path)
    if mime not in ("image/png", "image/jpeg"):
        mime = "image/png"
    with open(image_path, "rb") as fh:
        result = requests.post(
            f"{_lark_base()}/open-apis/im/v1/images",
            headers={"Authorization": f"Bearer {token}"},
            files={"image": (os.path.basename(image_path), fh, mime)},
            data={"image_type": "message"},
            timeout=60,
        ).json()
    if result.get("code") == 0:
        return result.get("data", {}).get("image_key")
    print(f"❌ Lark image upload failed: {result}", flush=True)
    return None


def send_image(chat_id: str, image_key: str) -> dict:
    token = _tenant_token()
    return requests.post(
        f"{_lark_base()}/open-apis/im/v1/messages",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        params={"receive_id_type": "chat_id"},
        json={
            "receive_id": chat_id,
            "msg_type": "image",
            "content": json.dumps({"image_key": image_key}),
        },
        timeout=30,
    ).json()


def send_shot(chat_id: str, shot_path: str | None) -> bool:
    if not shot_path or not Path(shot_path).exists():
        return False
    try:
        key = upload_image_lark(shot_path)
        if not key:
            return False
        return send_image(chat_id, key).get("code") == 0
    except Exception as err:
        print(f"[teams] screenshot send failed: {err!r}", flush=True)
        return False


def alert_login_failed(reason: str, *, stage: str = "?", shot: str | None = None,
                       force: bool = False) -> None:
    """Post the login-failure notice to the Lark group, @-mentioning the owner.

    De-duped by default so a retry loop can't spam the group with the same
    failure; ``force=True`` for a fresh, distinct failure.
    """
    with _lock:
        if _state["alerted"] and not force:
            return
        _state["alerted"] = True

    # Plain-text mention markup is `<at user_id="ou_…">Name</at>` (card markup
    # would be `<at id=ou_…></at>` — different thing, see maintenance.py:3082).
    mention = f'<at user_id="{ALERT_OPEN_ID}"></at> ' if ALERT_OPEN_ID else ""
    text = (
        f"{mention}❌ Teams watcher: LOGIN FAILED\n"
        f"• Reason: {reason}\n"
        f"• Stopped at: {stage}\n"
        f"• Account: {os.getenv('TEAMS_EMAIL', '(TEAMS_EMAIL unset)')}\n"
        f"• Profile: {PROFILE_DIR.name} "
        f"({'present' if PROFILE_DIR.exists() else 'MISSING'})\n"
        f"• Time: {_now_str()}\n"
        f"Fix: run `python teamswatch.py --login` on the server, then restart the bot."
    )
    try:
        resp = send_text(ALERT_CHAT_ID, text)
        if resp.get("code") != 0:
            print(f"[teams] alert send failed: {resp}", flush=True)
    except Exception as err:
        print(f"[teams] alert send raised: {err!r}", flush=True)
    send_shot(ALERT_CHAT_ID, shot)


# ---------------------------------------------------------------------------
# Browser
# ---------------------------------------------------------------------------
def _open(p, *, headless: bool):
    """Persistent-context Chromium. The profile dir carries the Teams session."""
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    ctx = p.chromium.launch_persistent_context(
        str(PROFILE_DIR),
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            # Teams web is heavy; the default 64MB /dev/shm in containers is not
            # enough and Chromium crashes with SIGBUS partway through boot.
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ],
        viewport={"width": 1600, "height": 900},
        user_agent=_UA,
        locale="en-US",
        timezone_id=os.getenv("TEAMS_WATCH_TZ", "Asia/Manila"),
        ignore_https_errors=True,
    )
    try:
        # Two separate jobs in one init script.
        #
        # 1. navigator.webdriver — light touch so trivial bot checks don't flag
        #    us; NOT an anti-bot bypass.
        # 2. WebAuthn removal — this account is passkey-first, so MSA feature-
        #    detects `window.PublicKeyCredential` and routes to its FIDO bridge
        #    ("Signing in with your passkey…"), which waits on a real platform
        #    authenticator (Windows Hello / Touch ID). Headless Chromium has
        #    none, so the page hangs on "Verifying …" until the login times out.
        #    Hiding the API makes MSA fall back to the password form. Deleting it
        #    is not enough on its own — some flows keep a reference to
        #    navigator.credentials.get, so that is stubbed to reject too.
        ctx.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            try { delete window.PublicKeyCredential; } catch (e) {}
            try {
                Object.defineProperty(window, 'PublicKeyCredential', {
                    get: () => undefined, configurable: true,
                });
            } catch (e) {}
            try {
                if (navigator.credentials) {
                    navigator.credentials.get = () => Promise.reject(
                        new DOMException('no authenticator', 'NotAllowedError'));
                }
            } catch (e) {}
            """
        )
    except Exception:
        pass
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    return ctx, page


_shot_n = 0


def _shot(page, label: str) -> str | None:
    """Screenshot every stage so a failed login is diagnosable from the PNGs."""
    global _shot_n
    _shot_n += 1
    SHOTS_DIR.mkdir(parents=True, exist_ok=True)
    path = SHOTS_DIR / f"{_shot_n:02d}_{label}.png"
    try:
        page.screenshot(path=str(path), full_page=False)
        _set(last_shot=str(path))
        return str(path)
    except Exception as err:
        print(f"[teams] screenshot '{label}' failed: {err!r}", flush=True)
        return None


def _visible(page, selector: str) -> bool:
    try:
        loc = page.locator(selector).first
        return loc.count() > 0 and loc.is_visible()
    except Exception:
        return False


def _body_text(page) -> str:
    """Rendered text only.

    Deliberately not ``page.content()``: the MSA page ships every string for
    every step in its bundle, so an HTML substring test matches text that is not
    on screen and mis-detects 2FA on a plain password page.
    """
    try:
        return (page.evaluate("() => document.body.innerText || ''") or "").lower()
    except Exception:
        return ""


def _text_seen(page, needle: str, haystack: str | None = None) -> bool:
    return needle.lower() in (haystack if haystack is not None else _body_text(page))


# Sign-in selectors, in click priority order.
#
# Microsoft shipped a new MSA sign-in UX: the email box is `#usernameEntry` and
# the primary button is `[data-testid='primaryButton']` — the long-lived legacy
# ids (`loginfmt`, `idSIButton9`) are simply absent. Because Microsoft A/B-tests
# and rolls these back, each list covers new *and* legacy, and prefers
# structural selectors (`input[type=password]`) over ids where one exists, since
# those cannot be renamed without changing what the field *is*.
_EMAIL_SELS = ["#usernameEntry", "input[name='loginfmt']", "input[type='email']"]
_PASSWORD_SELS = ["#passwordEntry", "input[name='passwd']", "input[type='password']"]
_SUBMIT_SELS = [
    "[data-testid='primaryButton']",
    "#idSIButton9",
    "button[type='submit']",
    "input[type='submit']",
]
_OTC_SELS = ["input[autocomplete='one-time-code']", "#idTxtBx_SAOTCC_OTC"]
_MSA_TILE_SELS = ["#msaTile"]             # "Personal account" when a domain is both
_BACK_SELS = [
    "[data-testid='backButton']",
    "button[aria-label='Back']",
    "#backButton",
    "#idBtn_Back",
]
_LANDING_SIGNIN_SELS = ["button[data-onclick='signIn']", "button:has-text('Sign in')"]
_PROOFS_SELS = ["#idDiv_SAOTCS_Proofs"]   # "how do you want to sign in" 2FA list
_NUMBER_MATCH_SELS = ["#idRichContext_DisplaySign"]

# Teams app shell. Any one of these means we are inside.
_TEAMS_IN_SELS = [
    "[data-tid='app-bar']",
    "[data-tid='app-layout-area--main']",
    "[data-tid='chat-list']",
    "[data-tid='chat-list-item']",
    "[data-tid='messages-pane']",
    "#app-bar",
]


def _any_visible(page, sels: list[str]) -> bool:
    return any(_visible(page, s) for s in sels)


def _first_visible(page, sels: list[str]) -> str | None:
    """First visible selector *in list order*.

    Not a comma-joined selector + ``.first``: that resolves to the union's first
    node in DOM order, which is not the same as our priority order and can pick
    the wrong button.
    """
    for sel in sels:
        if _visible(page, sel):
            return sel
    return None


def _click_first(page, sels: list[str]) -> bool:
    sel = _first_visible(page, sels)
    if not sel:
        return False
    try:
        page.locator(sel).first.click(timeout=15000)
        return True
    except Exception as err:
        print(f"[teams] click {sel} failed: {err!r}", flush=True)
        return False


def _fill_first(page, sels: list[str], value: str) -> bool:
    sel = _first_visible(page, sels)
    if not sel:
        return False
    try:
        page.locator(sel).first.fill(value, timeout=15000)
        return True
    except Exception as err:
        print(f"[teams] fill {sel} failed: {err!r}", flush=True)
        return False


def _error_text(page) -> str:
    """Visible validation/error text — '' when the page is clean.

    Specific error ids are checked before the generic live regions. Nodes that
    contain a heading are skipped: the new MSA UX wraps the page title in an
    ``[aria-live=assertive]`` div, so without that filter every healthy sign-in
    page reports "Sign in / Use your Microsoft account." as an error and the
    driver aborts a perfectly good login.
    """
    try:
        return (page.evaluate(
            """() => {
                const sels = ['#passwordError', '#usernameError', '.alert-error',
                              '[role=alert]', '[aria-live=assertive]', '[aria-live=polite]'];
                for (const s of sels) {
                    for (const el of document.querySelectorAll(s)) {
                        if (!(el.offsetParent || el.offsetWidth || el.offsetHeight)) continue;
                        if (el.querySelector('h1,h2,h3,[role=heading]')) continue;
                        const t = (el.innerText || '').trim();
                        if (t) return t;
                    }
                }
                return '';
            }"""
        ) or "").strip()
    except Exception:
        return ""


_PASSWORD_OPTION_JS = """
    (doClick) => {
        // A clickable "use your password" / "Password" choice, wherever MSA has
        // hidden it this week: the passkey page's escape hatch, the "other ways
        // to sign in" list, or a credential-picker row.
        const els = [...document.querySelectorAll(
            'button,a,[role=button],[role=listitem],[role=option],div[data-testid]')];
        for (const el of els) {
            const t = (el.innerText || '').trim();
            if (!t || t.length > 70) continue;
            if (!/password/i.test(t)) continue;
            // "Forgot your password" / "Create one" are not the way forward.
            if (/forgot|reset|create|change/i.test(t)) continue;
            if (!(el.offsetParent || el.offsetWidth || el.offsetHeight)) continue;
            if (doClick) el.click();
            return t;
        }
        return '';
    }
"""


def _password_option(page, *, click: bool = False) -> str:
    try:
        return (page.evaluate(_PASSWORD_OPTION_JS, click) or "").strip()
    except Exception:
        return ""


def _stage_of(page) -> str:
    """Classify the current page so the driver loop knows what to do next.

    Ordered most-specific first: a page can satisfy several weak checks at once
    (a 2FA page still lives on login.live.com), so terminal states are tested
    before the generic ones.
    """
    url = (page.url or "").lower()
    on_teams = "teams.live.com" in url or "teams.microsoft.com" in url

    # --- terminal: signed in -------------------------------------------------
    if on_teams and _any_visible(page, _TEAMS_IN_SELS):
        return "teams_loaded"

    # --- the logged-out marketing page ---------------------------------------
    # teams.live.com/v2/ does NOT bounce to the login page when signed out; it
    # serves /gather, whose "Sign in" button is what makes MSAL mint a fresh
    # authorize URL. Checked before `teams_booting` or the landing page would be
    # mistaken for a still-painting app shell and the loop would spin to timeout.
    if _any_visible(page, _LANDING_SIGNIN_SELS) and not _any_visible(page, _EMAIL_SELS):
        return "landing"

    if on_teams and "login" not in url:
        return "teams_booting"

    body = _body_text(page)

    # --- passkey / FIDO bridge ----------------------------------------------
    # Checked before everything below: this page sits on login.microsoft.com and
    # matches none of the other probes, so it used to fall through to "unknown"
    # and spin until the timeout. It IS recoverable — there is a password escape
    # hatch — so it must not be lumped in with the 2FA dead ends.
    if "/bridge/fido" in url or _text_seen(page, "signing in with your passkey", body):
        return "passkey"

    # A password choice offered anywhere is always preferable to any prompt
    # below, so take it before the 2FA checks claim the page.
    if not _any_visible(page, _PASSWORD_SELS) and _password_option(page):
        return "use_password_option"

    # --- terminal: needs a human --------------------------------------------
    if _any_visible(page, _OTC_SELS):
        return "twofa_code"
    if _any_visible(page, _NUMBER_MATCH_SELS):
        return "twofa_number_match"
    if _any_visible(page, _PROOFS_SELS):
        return "twofa_choose"
    for needle, stage in (
        ("approve sign in request", "twofa_approve"),
        ("check your authenticator app", "twofa_approve"),
        ("help us protect your account", "twofa_enroll"),
        ("verify your identity", "twofa_choose"),
        ("account has been locked", "account_locked"),
        ("account has been temporarily", "account_locked"),
        ("unusual sign-in activity", "account_locked"),
    ):
        if _text_seen(page, needle, body):
            return stage

    # --- actionable steps ----------------------------------------------------
    # Password before email: on the password step the email box is sometimes
    # still in the DOM (read-only), and password is the more advanced state.
    if _any_visible(page, _PASSWORD_SELS):
        return "password"
    if _any_visible(page, _MSA_TILE_SELS):
        return "pick_personal"
    if _text_seen(page, "use your password", body):
        return "switch_to_password"
    if _any_visible(page, _EMAIL_SELS):
        return "email"
    if _text_seen(page, "stay signed in", body):
        return "stay_signed_in"
    if _text_seen(page, "use the web app", body):
        return "use_web_app"

    return "unknown"


_NEEDS_HUMAN = {
    "twofa_code": "account asks for a one-time code (2FA is on) — cannot be scripted",
    "twofa_number_match": "account uses Authenticator number matching — cannot be scripted",
    "twofa_choose": "account asks which 2FA method to use — cannot be scripted",
    "twofa_approve": "account waits for Authenticator approval — cannot be scripted",
    "twofa_enroll": "Microsoft demands extra security info before continuing",
    "account_locked": "Microsoft has locked/throttled this account",
}


def do_login(*, headless: bool = True, report_chat: str | None = None) -> dict:
    """Drive the MSA sign-in and leave a signed-in profile behind.

    Returns ``{"ok": bool, "stage": str, "reason": str|None, "shot": str|None}``.
    """
    from playwright.sync_api import sync_playwright

    email, password = _creds()
    _set(phase="logging_in", detail="starting browser", started_at=time.monotonic(),
         last_error=None, alerted=False)

    deadline = time.monotonic() + _login_timeout_s()
    result: dict[str, Any] = {"ok": False, "stage": "start", "reason": None, "shot": None}
    typed_email = typed_password = False
    passkey_tries = 0

    with sync_playwright() as p:
        ctx, page = _open(p, headless=headless)
        try:
            page.goto(TEAMS_URL, wait_until="domcontentloaded", timeout=60000)
            result["shot"] = _shot(page, "01_landed")

            last_stage = None
            while time.monotonic() < deadline:
                stage = _stage_of(page)
                if stage != last_stage:
                    print(f"[teams] stage: {stage}  ({page.url[:90]})", flush=True)
                    _set(detail=f"stage: {stage}", last_stage=stage)
                    result["shot"] = _shot(page, stage)
                    last_stage = stage

                if stage == "teams_loaded":
                    # Give the shell a moment to settle before the success shot.
                    page.wait_for_timeout(4000)
                    result.update(ok=True, stage=stage,
                                  shot=_shot(page, "teams_loaded_final"))
                    _set(phase="monitoring", detail="signed in, session saved",
                         account=email, connected_at=_now_str(), last_error=None)
                    break

                if stage in _NEEDS_HUMAN:
                    result.update(ok=False, stage=stage, reason=_NEEDS_HUMAN[stage])
                    break

                if stage == "landing":
                    # Logged-out marketing page: this click is what makes MSAL
                    # build a fresh authorize URL (with valid PKCE params).
                    _click_first(page, _LANDING_SIGNIN_SELS)
                    page.wait_for_timeout(6000)
                elif stage == "email":
                    if typed_email:
                        # Bounced back to the email box = address not accepted.
                        result.update(
                            ok=False, stage="bad_username",
                            reason=_error_text(page)
                            or "login returned to the email step; address not accepted",
                        )
                        break
                    if not _fill_first(page, _EMAIL_SELS, email):
                        result.update(ok=False, stage=stage,
                                      reason="could not fill the email field")
                        break
                    _click_first(page, _SUBMIT_SELS)
                    typed_email = True
                elif stage == "password":
                    if typed_password:
                        result.update(
                            ok=False, stage="bad_password",
                            reason=_error_text(page)
                            or "login returned to the password step; password not accepted",
                        )
                        break
                    if not _fill_first(page, _PASSWORD_SELS, password):
                        result.update(ok=False, stage=stage,
                                      reason="could not fill the password field")
                        break
                    _click_first(page, _SUBMIT_SELS)
                    typed_password = True
                elif stage == "passkey":
                    # Should be unreachable now that WebAuthn is hidden in
                    # _open(), but Microsoft can still land here from a cached
                    # preference — so escape to the password form rather than
                    # waiting on a security window that will never open.
                    passkey_tries += 1
                    if passkey_tries > 4:
                        result.update(
                            ok=False, stage="passkey",
                            reason="account is passkey-first and offered no "
                                   "password fallback — turn off 'passwordless "
                                   "account' for this Microsoft account",
                        )
                        break
                    if _password_option(page, click=True):
                        page.wait_for_timeout(4000)
                    elif _click_first(page, _BACK_SELS):
                        page.wait_for_timeout(4000)
                    else:
                        page.wait_for_timeout(3000)
                elif stage == "use_password_option":
                    label = _password_option(page, click=True)
                    print(f"[teams] chose password option: {label!r}", flush=True)
                    page.wait_for_timeout(4000)
                elif stage == "pick_personal":
                    # Custom domain registered as both work and personal: this
                    # account is the consumer one (tenant=consumers).
                    _click_first(page, _MSA_TILE_SELS)
                elif stage == "switch_to_password":
                    # Passwordless-first rollout: opt back into the password box.
                    try:
                        page.get_by_text("Use your password", exact=False).first.click(timeout=10000)
                    except Exception:
                        _click_first(page, _SUBMIT_SELS)
                elif stage == "stay_signed_in":
                    # "Yes" is what makes the session survive a service restart.
                    _click_first(page, _SUBMIT_SELS)
                elif stage == "use_web_app":
                    try:
                        page.get_by_text("Use the web app").first.click(timeout=10000)
                    except Exception:
                        pass
                else:
                    # teams_booting / unknown — let the page keep working, but
                    # surface a validation error if one has appeared.
                    err_txt = _error_text(page)
                    if err_txt and typed_password:
                        result.update(ok=False, stage="bad_password", reason=err_txt)
                        break
                    page.wait_for_timeout(2000)

                page.wait_for_timeout(2500)
            else:
                result.update(
                    ok=False,
                    stage=last_stage or "timeout",
                    reason=f"timed out after {_login_timeout_s()}s without reaching Teams",
                )
                result["shot"] = _shot(page, "timeout")

        except Exception as err:
            result.update(ok=False, stage="exception", reason=repr(err))
            result["shot"] = _shot(page, "exception")
        finally:
            # Closing the context is what flushes the profile to disk.
            try:
                ctx.close()
            except Exception:
                pass

    # Recorded before anything else can overwrite it — see _state's comment.
    _set(last_login_ok=bool(result["ok"]), last_login_stage=result["stage"],
         last_login_reason=result["reason"], last_login_at=_now_str())

    if result["ok"]:
        print(f"✅ Teams login OK — profile saved to {PROFILE_DIR}", flush=True)
        if report_chat:
            send_text(report_chat,
                      f"✅ Teams watcher: LOGIN OK\n"
                      f"• Account: {email}\n"
                      f"• Profile: {PROFILE_DIR.name}\n"
                      f"• Time: {_now_str()}")
            send_shot(report_chat, result["shot"])
    else:
        print(f"❌ Teams login FAILED at {result['stage']}: {result['reason']}", flush=True)
        _set(phase="login_failed", detail=f"failed at {result['stage']}",
             last_error=result["reason"])
        alert_login_failed(result["reason"] or "unknown",
                           stage=result["stage"], shot=result["shot"], force=True)

    _persist()
    return result


def check_session(*, headless: bool = True) -> dict:
    """Open the saved profile and see whether it is still signed in.

    Never types credentials — if the profile is dead this reports it rather than
    silently re-logging-in, so a broken session is visible instead of masked.
    """
    from playwright.sync_api import sync_playwright

    if not PROFILE_DIR.exists():
        _set(phase="login_failed", detail="no profile — never logged in",
             last_error="profile missing")
        _persist()
        return {"ok": False, "stage": "no_profile",
                "reason": f"{PROFILE_DIR.name} does not exist — run --login first",
                "shot": None}

    result: dict[str, Any] = {"ok": False, "stage": "start", "reason": None, "shot": None}
    with sync_playwright() as p:
        ctx, page = _open(p, headless=headless)
        try:
            page.goto(TEAMS_URL, wait_until="domcontentloaded", timeout=60000)
            # The shell boots slowly on a CPU-only box; poll instead of one wait.
            deadline = time.monotonic() + 60
            stage = "unknown"
            while time.monotonic() < deadline:
                stage = _stage_of(page)
                if stage == "teams_loaded":
                    break
                # "landing" is the logged-out marketing page — a dead session.
                if stage in _NEEDS_HUMAN or stage in (
                    "landing", "email", "password", "pick_personal"
                ):
                    break
                page.wait_for_timeout(2000)

            result["stage"] = stage
            result["shot"] = _shot(page, f"check_{stage}")
            if stage == "teams_loaded":
                result["ok"] = True
                _set(phase="monitoring", detail="session alive",
                     connected_at=_now_str(), last_error=None,
                     account=os.getenv("TEAMS_EMAIL") or None, last_stage=stage)
            else:
                result["reason"] = f"profile is not signed in (stage: {stage})"
                _set(phase="login_failed", detail=f"session dead at {stage}",
                     last_error=result["reason"], last_stage=stage)
        except Exception as err:
            result.update(stage="exception", reason=repr(err))
            _set(phase="login_failed", detail="check raised", last_error=repr(err))
        finally:
            try:
                ctx.close()
            except Exception:
                pass

    _persist()
    return result


# ---------------------------------------------------------------------------
# /teamstatus
# ---------------------------------------------------------------------------
def _profile_state() -> str:
    """'missing' | 'empty' | 'present'.

    The directory existing proves nothing: ``_open()`` mkdirs it before Chromium
    starts, so a login that died at the password step still leaves one behind.
    Chromium only writes ``Default/`` once it has really initialised a profile,
    so that is the honest test.
    """
    if not PROFILE_DIR.exists():
        return "missing"
    return "present" if (PROFILE_DIR / "Default").is_dir() else "empty"


_PROFILE_NOTE = {
    "missing": "MISSING — run `python teamswatch.py --login`",
    "empty": "EMPTY — a browser started but never finished signing in",
    "present": "present (a profile exists; not proof it is still signed in)",
}


def status_lines() -> list[str]:
    snap = _snapshot()
    phase = snap["phase"]
    emoji = _PHASE_EMOJI.get(phase, "⚪")
    monitoring = phase == "monitoring"
    prof = _profile_state()

    lines = [
        f"{emoji} Teams watcher: {'MONITORING' if monitoring else phase.upper()}",
        f"• Detail: {snap['detail']}",
        f"• Logged in as: {snap['account'] or '— not authorised —'}",
        f"• Profile: {PROFILE_DIR.name} ({_PROFILE_NOTE[prof]})",
    ]
    if snap["connected_at"]:
        lines.append(f"• Last confirmed: {snap['connected_at']}")
    if snap["last_stage"]:
        lines.append(f"• Last stage: {snap['last_stage']}")
    if snap["last_error"]:
        lines.append(f"• Last error: {snap['last_error']}")

    # Kept distinct from the line above: this survives every /teamstatus probe.
    if snap.get("last_login_at"):
        mark = "✅" if snap.get("last_login_ok") else "❌"
        lines.append(
            f"• Last LOGIN attempt: {mark} {snap['last_login_at']} "
            f"(stage: {snap.get('last_login_stage')})"
        )
        if not snap.get("last_login_ok") and snap.get("last_login_reason"):
            lines.append(f"   ↳ {snap['last_login_reason']}")
    else:
        lines.append(
            "• Last LOGIN attempt: none recorded — `--login` has not completed yet"
        )

    if phase == "idle":
        # IDLE is not a "wait for it" state — nothing runs in the background.
        lines.append(
            "• IDLE means no login has completed yet — nothing is running, so "
            "waiting will not change this. Run `python teamswatch.py --login`."
        )
    lines.append("• Phase: login only — message capture not built yet")
    return lines


def send_status_to_lark(chat_id: str) -> dict:
    """/teamstatus — text summary, plus a fresh screenshot when signed in.

    Whenever a real profile exists this re-opens it and probes the live session,
    rather than trusting the stored phase. Two reasons: a stale PNG or a stale
    "monitoring" would both be misleading, and the stored phase is only written
    when a --login/--check *finishes* — so a status read from a different
    process (the bot vs. your shell) would otherwise report IDLE forever.
    """
    shot = None
    monitoring = is_monitoring()

    if _profile_state() == "present":
        try:
            res = check_session()
            monitoring = bool(res.get("ok"))
            shot = res.get("shot")
        except Exception as err:
            print(f"[teams] status re-check failed: {err!r}", flush=True)
            _set(last_error=repr(err))

    try:
        send_text(chat_id, "\n".join(status_lines()))
    except Exception as err:
        print(f"[teams] status text send failed: {err!r}", flush=True)

    # Always send the shot when we have one: on failure it shows *where* the
    # session died, which is the whole point of asking.
    send_shot(chat_id, shot or _snapshot().get("last_shot"))

    return {"monitoring": monitoring, "screenshot": shot}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Teams (personal) watcher — login phase")
    ap.add_argument("--login", action="store_true", help="sign in and save the profile")
    ap.add_argument("--check", action="store_true", help="is the saved profile still signed in?")
    ap.add_argument("--status", action="store_true", help="print the status summary")
    ap.add_argument("--headed", action="store_true", help="visible browser (local debug)")
    ap.add_argument("--report-chat", default=None,
                    help="Lark chat_id to send the result + screenshot to")
    args = ap.parse_args(argv)

    if args.status:
        print("\n".join(status_lines()))
        return 0
    if args.login:
        res = do_login(headless=not args.headed, report_chat=args.report_chat)
        print(json.dumps(res, indent=2))
        return 0 if res["ok"] else 1
    if args.check:
        res = check_session(headless=not args.headed)
        print(json.dumps(res, indent=2))
        return 0 if res["ok"] else 1

    ap.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
