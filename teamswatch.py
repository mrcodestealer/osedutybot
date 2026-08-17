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
import contextlib
import json
import mimetypes
import os
import re
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

    # A .env written on Windows and copied to the server leaves a trailing CR on
    # every value. Microsoft then rejects the password as wrong, which looks
    # exactly like a genuinely wrong password. A CR/LF is never part of a real
    # password, so strip it — but say so, because it means the .env needs fixing.
    cleaned = password.replace("\r", "").replace("\n", "")
    if cleaned != password:
        print("[teams] WARNING: stripped CR/LF from TEAMS_PASSWORD — .env has "
              "Windows line endings (run: dos2unix .env)", flush=True)
        password = cleaned
    # Not stripped: a space could in principle be part of the password, so this
    # is flagged rather than silently changed.
    if password != password.strip():
        print("[teams] WARNING: TEAMS_PASSWORD has leading/trailing whitespace — "
              "if that is not deliberate, it is why the password is rejected",
              flush=True)
    return email, password


def _fingerprint(val: str) -> str:
    """Identify a secret without revealing it: length, short hash, odd characters."""
    import hashlib

    if val == "":
        return "EMPTY"
    flags = []
    if val != val.lstrip():
        flags.append("LEADING-SPACE")
    if val != val.rstrip():
        flags.append("TRAILING-SPACE")
    if "\r" in val:
        flags.append("HAS-CR")
    if "\n" in val:
        flags.append("HAS-LF")
    if "\t" in val:
        flags.append("HAS-TAB")
    if len(val) > 1 and val[0] == val[-1] and val[0] in "\"'":
        flags.append("WRAPPED-IN-QUOTES")
    if "#" in val:
        flags.append("HAS-HASH(inline comment?)")
    return (f"len={len(val)} sha8={hashlib.sha256(val.encode()).hexdigest()[:8]}"
            + ("  ⚠ " + " ".join(flags) if flags else ""))


def check_env() -> int:
    """Print a safe fingerprint of the credentials and where they came from.

    Answers "why is my correct password rejected?" without printing the secret.
    Two things it catches that nothing else does: stray characters picked up from
    the .env line, and a value in the real environment shadowing the .env one —
    ``load_dotenv`` does not override variables already in ``os.environ``.
    """
    print(f".env path : {_ENV_PATH}  "
          f"({'exists' if _ENV_PATH.exists() else 'MISSING'})")

    raw: dict[str, str] = {}
    seen_lines: dict[str, list[int]] = {}
    try:
        for lineno, line in enumerate(
            _ENV_PATH.read_text(encoding="utf-8", errors="replace").splitlines(), 1
        ):
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, val = stripped.split("=", 1)
            key = key.strip()
            if key in ("TEAMS_EMAIL", "TEAMS_PASSWORD"):
                # Last occurrence wins, matching python-dotenv.
                raw[key] = val
                seen_lines.setdefault(key, []).append(lineno)
    except Exception as err:
        print(f"  .env unreadable: {err!r}")

    for key in ("TEAMS_EMAIL", "TEAMS_PASSWORD"):
        hits = seen_lines.get(key, [])
        if len(hits) > 1:
            print(f"\n⚠ {key} is defined {len(hits)} times in .env "
                  f"(lines {', '.join(map(str, hits))}) — python-dotenv keeps the "
                  f"LAST one (line {hits[-1]}). Delete the stale ones.")

    for key in ("TEAMS_EMAIL", "TEAMS_PASSWORD"):
        effective = os.getenv(key) or ""
        print(f"\n{key}")
        print(f"  effective : {_fingerprint(effective)}")
        if key not in raw:
            print("  .env line : NOT FOUND")
            continue
        print(f"  .env line : {_fingerprint(raw[key])}")
        if raw[key].strip().strip("\"'") != effective:
            print("  ⚠ the effective value does NOT match the .env line — "
                  "something in the real environment is shadowing it "
                  "(load_dotenv never overrides os.environ)")

    email = os.getenv("TEAMS_EMAIL") or ""
    if email:
        print(f"\nEmail (not secret): {email}")
    print("\nCount the characters of the password you believe is correct and "
          "compare it to len= above. A mismatch means .env picked up a stray "
          "character; an exact match means the value is fine and the account "
          "itself is rejecting it.")
    return 0


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
_LOCK_PATH = _ROOT_DIR / (PROFILE_DIR.name + ".lock")
_LOCK_STALE_S = 300


@contextlib.contextmanager
def _profile_lock(what: str):
    """Serialise access to the Chromium profile.

    Two Chromium processes on one user-data-dir is unsupported: the second either
    refuses to start or writes over the first's session, which loses the login.
    That collision is easy to hit here because /teamstatus opens the profile on a
    timer while someone may be running --login by hand. Stale locks (older than
    _LOCK_STALE_S, e.g. left by a killed process) are reclaimed.
    """
    acquired = False
    try:
        for _ in range(60):
            try:
                fd = os.open(str(_LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()} {what} {_now_str()}".encode())
                os.close(fd)
                acquired = True
                break
            except FileExistsError:
                try:
                    age = time.time() - _LOCK_PATH.stat().st_mtime
                except OSError:
                    continue
                if age > _LOCK_STALE_S:
                    print(f"[teams] reclaiming stale profile lock ({int(age)}s old)",
                          flush=True)
                    _LOCK_PATH.unlink(missing_ok=True)
                    continue
                time.sleep(2)
        if not acquired:
            raise RuntimeError(
                f"profile is busy — another teamswatch run holds {_LOCK_PATH.name}"
            )
        yield
    finally:
        if acquired:
            _LOCK_PATH.unlink(missing_ok=True)


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


def _shell_selectors_present(page) -> list[str]:
    """Which app-shell selectors actually matched — logged so phase 2 can rely
    on the ones this account's Teams build really uses, instead of guesses."""
    return [s for s in _TEAMS_IN_SELS if _visible(page, s)]


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


_TEXT_OPTION_JS = """
    ([includeRe, excludeRe, skip, mark]) => {
        // Find a clickable choice by its visible label. MSA moves these between
        // buttons, links and list rows between rollouts, so match on text rather
        // than on structure. Marks the pick instead of clicking it — see
        // _click_text_option for why the click has to happen in Playwright.
        const inc = new RegExp(includeRe, 'i');
        const exc = excludeRe ? new RegExp(excludeRe, 'i') : null;
        document.querySelectorAll('[data-teamswatch-pick]').forEach(
            el => el.removeAttribute('data-teamswatch-pick'));
        const hits = [];
        for (const el of document.querySelectorAll(
                'button,a,[role=button],[role=listitem],[role=option],div[data-testid]')) {
            const t = (el.innerText || '').trim();
            if (!t || t.length > 70) continue;
            if (!inc.test(t)) continue;
            if (exc && exc.test(t)) continue;
            if (!(el.offsetParent || el.offsetWidth || el.offsetHeight)) continue;
            hits.push({el: el, t: t});
        }
        // Most specific first: real controls before generic containers, then the
        // shortest label. A wrapper div inherits its children's text, and
        // clicking a wrapper usually reaches no handler at all.
        const rank = (el) => (el.tagName === 'BUTTON' || el.tagName === 'A'
                              || el.getAttribute('role') === 'button') ? 0 : 1;
        hits.sort((a, b) => rank(a.el) - rank(b.el) || a.t.length - b.t.length);
        const pick = hits[skip || 0];
        if (!pick) return '';
        if (mark) pick.el.setAttribute('data-teamswatch-pick', '1');
        return pick.t;
    }
"""

_PASSWORD_INC = r"password"
# "Forgot your password" / "Create one" are not the way forward.
_PASSWORD_EXC = r"forgot|reset|create|change"
_OTHER_WAYS_INC = (
    r"other ways to sign in|sign in another way|more ways to sign in|other ways"
)


def _text_option(page, include: str, *, exclude: str = "", skip: int = 0,
                 mark: bool = False) -> str:
    try:
        return (page.evaluate(
            _TEXT_OPTION_JS, [include, exclude, int(skip), bool(mark)]
        ) or "").strip()
    except Exception:
        return ""


def _click_text_option(page, include: str, *, exclude: str = "", skip: int = 0) -> str:
    """Mark the best match in JS, then click it *with Playwright*.

    A raw ``el.click()`` inside ``page.evaluate`` is not enough. These option
    rows are React components whose handler often sits on a different node, so a
    synthetic click on the matched wrapper silently does nothing — which showed
    up as the driver logging "chose password option" over and over while the page
    never moved. Playwright dispatches real trusted input events, which React
    honours. ``skip`` walks to the next-best candidate when the first is inert.
    """
    label = _text_option(page, include, exclude=exclude, skip=skip, mark=True)
    if not label:
        return ""
    try:
        page.locator("[data-teamswatch-pick='1']").first.click(timeout=15000)
        return label
    except Exception as err:
        print(f"[teams] click {label!r} failed: {err!r}", flush=True)
        return ""


def _password_option(page, *, skip: int = 0) -> str:
    """The "use your password" / "Password" choice, wherever MSA hid it."""
    return _text_option(page, _PASSWORD_INC, exclude=_PASSWORD_EXC, skip=skip)


def _click_password_option(page, *, skip: int = 0) -> str:
    return _click_text_option(page, _PASSWORD_INC, exclude=_PASSWORD_EXC, skip=skip)


def _other_ways_option(page) -> str:
    """The "Other ways to sign in" escape hatch on the send-a-code page.

    Deliberately specific: that page's PRIMARY button is "Send code", which
    would email a one-time code no unattended process can read. The only safe
    control on it is this link.
    """
    return _text_option(page, _OTHER_WAYS_INC)


def _click_other_ways(page) -> str:
    return _click_text_option(page, _OTHER_WAYS_INC)


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
        # The app-bar frame paints while Teams is still booting behind its
        # "We're setting things up for you…" splash, so the selectors alone
        # report success too early — the success screenshot then shows a spinner
        # instead of the signed-in UI, and phase 2 would start reading an empty
        # chat list. Treat the splash as still-booting.
        if "setting things up" in _body_text(page):
            return "teams_booting"
        return "teams_loaded"

    # A visible password box is unambiguous whatever else the page shows, so it
    # is settled before the marketing-page probe below — whose
    # `button:has-text('Sign in')` ALSO matches the submit button on the real
    # password form, which would otherwise be misread as the landing page and
    # get its submit clicked with an empty password.
    if _any_visible(page, _PASSWORD_SELS):
        return "password"

    # --- the logged-out marketing page ---------------------------------------
    # teams.live.com/v2/ does NOT bounce to the login page when signed out; it
    # serves /gather, whose "Sign in" button is what makes MSAL mint a fresh
    # authorize URL. Checked before `teams_booting` or the landing page would be
    # mistaken for a still-painting app shell and the loop would spin to timeout.
    # Gated on on_teams: the marketing page only ever comes from teams.live.com,
    # never from login.live.com.
    if on_teams and _any_visible(page, _LANDING_SIGNIN_SELS):
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
    # below, so take it before the 2FA checks claim the page. (No need to re-test
    # for a password box — that already returned above.)
    if _password_option(page):
        return "use_password_option"

    # "Get a code to sign in": MSA's passwordless-first page. No element on it
    # mentions "password", so the check above cannot see a way forward — the
    # route to the password form is the "Other ways to sign in" link. Its
    # primary button is "Send code", which we must never press.
    if _other_ways_option(page):
        return "other_ways"

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
    # ("password" is settled near the top — it must beat the landing probe.)
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
    passkey_tries = other_ways_tries = password_option_tries = 0

    with _profile_lock("login"), sync_playwright() as p:
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
                    # Splash is already gone by here (see _stage_of); this is
                    # just letting the chat list paint before the success shot.
                    page.wait_for_timeout(8000)
                    print(f"[teams] shell selectors matched: "
                          f"{_shell_selectors_present(page)}", flush=True)
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
                    if _click_password_option(page):
                        page.wait_for_timeout(4000)
                    elif _click_first(page, _BACK_SELS):
                        page.wait_for_timeout(4000)
                    else:
                        page.wait_for_timeout(3000)
                elif stage == "use_password_option":
                    # Retry once on the same element (it may just be slow), then
                    # walk to the next-best candidate — the first match can be an
                    # inert wrapper.
                    label = _click_password_option(
                        page, skip=max(0, password_option_tries - 1)
                    )
                    password_option_tries += 1
                    if password_option_tries > 5:
                        result.update(
                            ok=False, stage="password_option_stuck",
                            reason="'Use your password' is present but clicking it "
                                   "never reaches the password form",
                        )
                        break
                    print(f"[teams] chose password option: {label!r} "
                          f"(attempt {password_option_tries})", flush=True)
                    page.wait_for_timeout(4000)
                elif stage == "other_ways":
                    other_ways_tries += 1
                    if other_ways_tries > 3:
                        result.update(
                            ok=False, stage="no_password_option",
                            reason="MSA offers only an emailed code — no password "
                                   "option behind 'Other ways to sign in'. This "
                                   "account is passwordless: turn that off at "
                                   "account.live.com > Security > Advanced "
                                   "security options.",
                        )
                        break
                    label = _click_other_ways(page)
                    print(f"[teams] opening sign-in options: {label!r}", flush=True)
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
                    # Click "Yes" by LABEL, never "whatever the primary button
                    # is". "Yes" is what issues the persistent cookie; if this
                    # rollout makes "No" the primary, clicking it still lands in
                    # Teams but with a session-only login that dies with the
                    # browser — i.e. a fresh --login needed after every service
                    # restart, which is precisely the symptom that showed up.
                    picked = _click_text_option(page, r"^yes")
                    print(
                        "[teams] stay-signed-in: "
                        + (f"clicked {picked!r}" if picked
                           else "no 'Yes' found — FELL BACK to primary button, "
                                "session may not persist"),
                        flush=True,
                    )
                    if not picked:
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

    # Pick up anything a --login in another process wrote since this process
    # started. Without this the long-lived bot re-persists the stale state it
    # loaded at import, wiping the successful-login record every /teamstatus —
    # which is why a status could still report "bad_password" after a login
    # that plainly worked.
    _load_persisted()

    if not PROFILE_DIR.exists():
        _set(phase="login_failed", detail="no profile — never logged in",
             last_error="profile missing")
        _persist()
        return {"ok": False, "stage": "no_profile",
                "reason": f"{PROFILE_DIR.name} does not exist — run --login first",
                "shot": None}

    result: dict[str, Any] = {"ok": False, "stage": "start", "reason": None, "shot": None}
    with _profile_lock("check"), sync_playwright() as p:
        ctx, page = _open(p, headless=headless)
        try:
            page.goto(TEAMS_URL, wait_until="domcontentloaded", timeout=60000)
            # The shell boots slowly on a CPU-only box, and must get past the
            # "setting things up" splash before the chat list exists — poll
            # rather than waiting a fixed period.
            deadline = time.monotonic() + max(60, int(os.getenv("TEAMS_BOOT_WAIT", "90")))
            stage = "unknown"
            while time.monotonic() < deadline:
                stage = _stage_of(page)
                if stage == "teams_loaded":
                    # Let the chat list paint so the screenshot is worth sending.
                    page.wait_for_timeout(6000)
                    print(f"[teams] shell selectors matched: "
                          f"{_shell_selectors_present(page)}", flush=True)
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
# Phase-2 discovery: what does the wire actually look like?
# ---------------------------------------------------------------------------
FRAMES_PATH = _ROOT_DIR / "teamswatch_frames.jsonl"
CHATS_PATH = _ROOT_DIR / "teamswatch_chats.json"

# Frames are recorded whole but capped: a single Teams frame can carry an entire
# conversation backfill and would otherwise blow up the log.
_FRAME_CHARS_MAX = 20000
_FRAMES_MAX = 4000

# A JSON quote that may be backslash-escaped. Trouter puts the interesting
# document in the envelope's `body` as a JSON *string*, so on the wire the keys
# appear as \"resourceType\" — matching only plain quotes finds nothing at all.
_Q = r"\\?\""

# Keys that suggest a frame carries a chat message rather than presence/typing
# noise. Used only to summarise the dump, never to filter what gets written.
_MSG_HINT = re.compile(
    rf"(?i){_Q}messagetype{_Q}|newmessage|{_Q}content{_Q}\s*:"
    rf"|clientmessageid|imdisplayname"
)

# Stronger: trouter wraps a genuinely new chat message in a NewMessage envelope,
# and chat bodies carry messagetype RichText/Text. The weak hint above also fires
# on presence and typing traffic, so early-exit keys off this one instead.
_MSG_STRONG = re.compile(
    rf"(?i){_Q}resourceType{_Q}\s*:\s*{_Q}NewMessage"
    rf"|{_Q}messagetype{_Q}\s*:\s*{_Q}(?:RichText|Text)"
)

# HTTP endpoints that carry chat content. Teams fetches messages from the
# regional messaging host (…msg.teams.microsoft.com/v1/users/ME/conversations/
# {id}/messages) and long-polls trouter at /v4/f/…/poll.
_HTTP_HINT = re.compile(
    r"(?i)/v1/users/ME/conversations"
    r"|/messages(?:\?|$|/)"
    r"|/poll(?:\?|$)"
    r"|msg\.teams\.microsoft\.com"
    r"|/threads/"
)


def dump_frames(*, seconds: int = 300, min_messages: int = 1,
                headless: bool = True) -> dict:
    """Record websocket frames from a live Teams session to teamswatch_frames.jsonl.

    Teams pushes new messages over a long-lived socket (trouter) — that is what
    drives unread badges and toast previews, so the frames carry sender and body.
    Reading them is what lets the watcher see messages WITHOUT opening the chat,
    which keeps everything unread in Teams.

    The frame schema is not documented and changes, so phase 2's parser is built
    against a real capture rather than a guess. Somebody must post in the watched
    group while this runs, or there will be no message frame to learn from.
    """
    from playwright.sync_api import sync_playwright

    stats: dict[str, Any] = {"sockets": [], "frames": 0, "msg_like": 0,
                             "messages": 0, "http": 0,
                             "path": str(FRAMES_PATH), "loaded": False}
    with _profile_lock("dump-frames"), sync_playwright() as p:
        ctx, page = _open(p, headless=headless)
        fh = FRAMES_PATH.open("w", encoding="utf-8")

        def _write(kind: str, url: str, payload: Any) -> None:
            if stats["frames"] >= _FRAMES_MAX:
                return
            if isinstance(payload, (bytes, bytearray)):
                body = payload.decode("utf-8", errors="replace")
                binary = True
            else:
                body = str(payload)
                binary = False
            stats["frames"] += 1
            if _MSG_HINT.search(body):
                stats["msg_like"] += 1
            if kind == "http":
                stats["http"] += 1
            if kind in ("recv", "http") and _MSG_STRONG.search(body):
                stats["messages"] += 1
                # Print immediately: waiting 5 minutes to find out whether the
                # capture worked is what made this look hung.
                print(f"[teams] ★ message payload #{stats['messages']} "
                      f"via {kind.upper()} ({len(body)} chars)", flush=True)
            try:
                fh.write(json.dumps({
                    "at": _now_str(),
                    "kind": kind,
                    "url": url[:200],
                    "binary": binary,
                    "chars": len(body),
                    "payload": body[:_FRAME_CHARS_MAX],
                }, ensure_ascii=False) + "\n")
                fh.flush()
            except Exception as err:  # noqa: BLE001
                print(f"[teams] frame write failed: {err!r}", flush=True)

        def _on_ws(ws) -> None:
            stats["sockets"].append(ws.url[:200])
            print(f"[teams] websocket opened: {ws.url[:130]}", flush=True)
            ws.on("framereceived", lambda pl: _write("recv", ws.url, pl))
            ws.on("framesent", lambda pl: _write("sent", ws.url, pl))

        page.on("websocket", _on_ws)

        def _on_response(resp) -> None:
            """Capture message-bearing HTTP responses too.

            Trouter frequently acts as a doorbell: it pushes a small "something
            changed" notification and the client then GETs the actual message
            over HTTPS. When that is what is happening, the websocket capture
            alone stays empty no matter how long it runs.
            """
            url = resp.url
            if not _HTTP_HINT.search(url):
                return
            try:
                if resp.status >= 300:
                    return
                body = resp.text()
            except Exception:
                return  # non-text, already consumed, or still streaming
            if not body:
                return
            if _MSG_STRONG.search(body) or _MSG_HINT.search(body):
                _write("http", url, body)

        page.on("response", _on_response)
        try:
            page.goto(TEAMS_URL, wait_until="domcontentloaded", timeout=60000)
            deadline = time.monotonic() + max(60, int(os.getenv("TEAMS_BOOT_WAIT", "90")))
            while time.monotonic() < deadline:
                if _stage_of(page) == "teams_loaded":
                    stats["loaded"] = True
                    break
                page.wait_for_timeout(2000)
            if not stats["loaded"]:
                print("[teams] never reached teams_loaded — capture may be empty",
                      flush=True)
            print(f"[teams] recording up to {seconds}s — POST IN THE WATCHED GROUP "
                  f"NOW. Stops early once {min_messages} message frame(s) are "
                  f"captured; Ctrl-C is safe (every frame is flushed as it lands).",
                  flush=True)
            end = time.monotonic() + seconds
            next_beat = time.monotonic() + 15
            stop_at: float | None = None
            while time.monotonic() < end:
                page.wait_for_timeout(1000)
                now = time.monotonic()
                if now >= next_beat:
                    next_beat = now + 15
                    print(f"[teams] {stats['frames']} frames "
                          f"({stats['http']} http) | "
                          f"{stats['msg_like']} message-like | "
                          f"{stats['messages']} ★ | "
                          f"{int(end - now)}s left", flush=True)
                # Grace period after the first real message so its follow-up
                # frames (edits, receipts, the conversation update) land too.
                if stats["messages"] >= min_messages and stop_at is None:
                    stop_at = now + 12
                    print("[teams] got what we need — stopping in 12s", flush=True)
                if stop_at is not None and now >= stop_at:
                    break
            _shot(page, "dump_frames_end")
        finally:
            try:
                fh.close()
            except Exception:
                pass
            try:
                ctx.close()
            except Exception:
                pass

    print(f"[teams] {stats['frames']} frames ({stats['msg_like']} look message-like) "
          f"-> {FRAMES_PATH}", flush=True)
    return stats


# Values safe to echo when reporting a frame's shape — these identify the schema.
_SHAPE_SHOW = {
    "resourcetype", "messagetype", "type", "threadtype", "eventtype",
    "imdisplayname", "composetime", "originalarrivaltime", "clientmessageid",
    "id", "version", "conversationid",
}
# Never echoed, at any depth: credentials, and the long opaque routing blobs.
_SHAPE_REDACT = re.compile(
    r"(?i)token|auth|cookie|password|secret|signature|key$|registrationid|surl|ssurl"
)


def _shape(node: Any, path: str = "", out: dict[str, str] | None = None,
           depth: int = 0) -> dict[str, str]:
    """Flatten a frame to ``path -> type(+safe sample)``.

    Reports the schema so a parser can be written against it, without echoing
    message bodies or credentials. Strings that are themselves JSON are recursed
    into: trouter nests a JSON document inside the envelope's ``body`` string.
    """
    if out is None:
        out = {}
    if depth > 6 or len(out) > 400:
        return out
    if isinstance(node, dict):
        for key, val in node.items():
            _shape(val, f"{path}.{key}" if path else str(key), out, depth + 1)
    elif isinstance(node, list):
        out[f"{path}[]"] = f"list({len(node)})"
        if node:
            _shape(node[0], f"{path}[0]", out, depth + 1)
    elif isinstance(node, str):
        leaf = path.rsplit(".", 1)[-1].split("[")[0].lower()
        stripped = node.strip()
        if stripped[:1] in ("{", "[") and len(stripped) > 2:
            try:
                _shape(json.loads(stripped), path + "(json)", out, depth + 1)
                return out
            except Exception:
                pass
        if _SHAPE_REDACT.search(leaf):
            out[path] = f"str({len(node)}) <redacted>"
        elif leaf in _SHAPE_SHOW:
            out[path] = f"str = {node[:60]!r}"
        else:
            out[path] = f"str({len(node)})"
    else:
        out[path] = type(node).__name__
    return out


def scan_frames(*, limit: int = 3) -> dict:
    """Summarise an existing teamswatch_frames.jsonl capture.

    Prints per-socket counts and, for the strongest message candidates, the frame
    SHAPE rather than its contents — so the capture can be shared to design the
    parser without leaking Skype tokens or message text.
    """
    if not FRAMES_PATH.exists():
        print(f"[teams] no capture at {FRAMES_PATH} — run --dump-frames first",
              flush=True)
        return {"frames": 0}

    per_socket: dict[str, int] = {}
    strong: list[dict[str, Any]] = []
    weak = total = 0
    with FRAMES_PATH.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            total += 1
            host = re.sub(r"^wss?://([^/]+).*", r"\1", str(rec.get("url") or ""))
            per_socket[host] = per_socket.get(host, 0) + 1
            body = str(rec.get("payload") or "")
            if _MSG_HINT.search(body):
                weak += 1
            if rec.get("kind") == "recv" and _MSG_STRONG.search(body):
                strong.append(rec)

    print(f"\n[teams] {total} frames in {FRAMES_PATH.name}")
    print(f"[teams] {weak} message-like, {len(strong)} ★ strong message frames")
    print("\nframes per socket:")
    for host, count in sorted(per_socket.items(), key=lambda kv: -kv[1]):
        print(f"   {count:6d}  {host}")

    if not strong:
        print("\nNo strong message frames. Falling back to showing what IS "
              "flowing — if these are all tiny pings, Teams is using the socket "
              "as a doorbell only and the message body arrives over HTTP.")

    # Biggest first: a frame carrying a message body is the fattest one there is.
    # When nothing matched, sample the largest frames anyway — that is how we find
    # out whether the traffic is keepalive noise or a shape the patterns miss.
    sample = strong or [r for r in _all_frames() if r.get("kind") != "sent"]
    sample.sort(key=lambda r: -int(r.get("chars") or 0))

    # THE diagnostic: which trouter routes are actually delivering anything. If
    # only presence shows up, this account's socket is not subscribed to chat
    # messages and no amount of waiting will produce one.
    routes: dict[str, int] = {}
    for rec in _all_frames():
        route, _ = _decode_frame(str(rec.get("payload") or ""))
        routes[route] = routes.get(route, 0) + 1
    print("\nroutes seen (what the sockets actually delivered):")
    for route, count in sorted(routes.items(), key=lambda kv: -kv[1]):
        print(f"   {count:6d}  {route}")

    print("\nlargest frames (chars | kind | host | route):")
    for rec in sample[:12]:
        host = re.sub(r"^\w+://([^/]+).*", r"\1", str(rec.get("url") or ""))[:36]
        route, _ = _decode_frame(str(rec.get("payload") or ""))
        print(f"  {rec.get('chars'):>7} | {str(rec.get('kind')):4} | {host:<36} | {route}")

    for i, rec in enumerate(sample[:limit], 1):
        label = "★ message frame" if strong else "frame sample"
        print(f"\n{'=' * 70}\n{label} {i}  "
              f"({rec.get('chars')} chars, {rec.get('kind')}, at {rec.get('at')})"
              f"\n{'=' * 70}")
        route, parsed = _decode_frame(str(rec.get("payload") or ""))
        print(f"  route: {route}")
        if parsed is None:
            head = str(rec.get("payload"))[:300]
            print(f"  (undecodable) head: {head!r}")
            continue
        for path, kind in sorted(_shape(parsed).items()):
            print(f"  {path:<62} {kind}")

    return {"frames": total, "weak": weak, "strong": len(strong),
            "http": sum(1 for r in _all_frames() if r.get("kind") == "http")}


# Trouter speaks Socket.IO 0.9 framing: "<type>:<id>:<endpoint>:<data>", e.g.
# `3:::{json}` for a message and `5:1::{"name":...,"args":[...]}` for an event.
# Without stripping this prefix json.loads fails on every single frame, which is
# why the first shape dumps all reported "not JSON at the top level".
_SIO_RE = re.compile(r"^(\d):(\d*):([^:]*):(.*)$", re.S)


def _decode_frame(payload: str) -> tuple[str, Any]:
    """Return ``(route, parsed)`` for a captured frame.

    ``route`` is a short label of what the frame IS — the trouter path for a
    delivered request, or the event name — which is what tells us whether chat
    messaging is actually subscribed on this socket or only presence is.
    """
    body = str(payload or "").strip()
    if not body:
        return "(empty)", None

    inner = body
    m = _SIO_RE.match(body)
    if m:
        inner = m.group(4).strip()
        if not inner:
            return f"sio:{m.group(1)}(no-data)", None

    try:
        parsed = json.loads(inner)
    except Exception:
        return "(not json)", None

    if isinstance(parsed, dict):
        # A trouter-delivered request: the url names the service.
        url = str(parsed.get("url") or "")
        if url:
            return "trouter:" + (url.rsplit("/", 1)[-1] or url)[:48], parsed
        name = str(parsed.get("name") or "")
        if name:
            return "event:" + name[:48], parsed
        for key in ("annotationType", "sessionUrlBase", "tokenExpirationTime"):
            if key in parsed:
                return "augloop:" + key, parsed
    return "json", parsed


def _all_frames() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not FRAMES_PATH.exists():
        return out
    with FRAMES_PATH.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def list_chats(*, headless: bool = True) -> dict:
    """Dump the sidebar's conversation titles to teamswatch_chats.json.

    Phase 2 should pin the watched group by conversation id, not by title — a
    rename would silently stop detection. This is how we learn both.
    """
    from playwright.sync_api import sync_playwright

    out: dict[str, Any] = {"titles": [], "loaded": False, "path": str(CHATS_PATH)}
    with _profile_lock("list-chats"), sync_playwright() as p:
        ctx, page = _open(p, headless=headless)
        try:
            page.goto(TEAMS_URL, wait_until="domcontentloaded", timeout=60000)
            deadline = time.monotonic() + max(60, int(os.getenv("TEAMS_BOOT_WAIT", "90")))
            while time.monotonic() < deadline:
                if _stage_of(page) == "teams_loaded":
                    out["loaded"] = True
                    break
                page.wait_for_timeout(2000)
            page.wait_for_timeout(6000)
            # Cast wide: report every plausible chat row with whatever id-ish
            # attributes it carries, rather than betting on one selector.
            out["titles"] = page.evaluate(
                """() => {
                    const rows = new Map();
                    const sels = ["[data-tid='chat-list-item']",
                                  "[data-tid^='chat-list-item']",
                                  "[role='treeitem']", "[role='listitem']"];
                    for (const s of sels) {
                        for (const el of document.querySelectorAll(s)) {
                            const t = (el.innerText || '').trim().split('\\n')[0];
                            if (!t) continue;
                            if (!rows.has(t)) rows.set(t, {
                                title: t,
                                sel: s,
                                id: el.getAttribute('data-tid')
                                    || el.getAttribute('id')
                                    || el.getAttribute('aria-labelledby') || '',
                            });
                        }
                    }
                    return [...rows.values()];
                }"""
            ) or []
            _shot(page, "list_chats")
        finally:
            try:
                ctx.close()
            except Exception:
                pass

    try:
        CHATS_PATH.write_text(json.dumps(out["titles"], ensure_ascii=False, indent=2),
                              encoding="utf-8")
    except Exception as err:  # noqa: BLE001
        print(f"[teams] chat list write failed: {err!r}", flush=True)
    print(f"[teams] {len(out['titles'])} chat rows -> {CHATS_PATH}", flush=True)
    for row in out["titles"]:
        print(f"   - {row.get('title')}", flush=True)
    return out


# ---------------------------------------------------------------------------
# Reading a group's messages from the DOM
# ---------------------------------------------------------------------------
# Why the DOM and not the websocket: two 300s trouter captures delivered only
# `unifiedPresenceService` plus `trouter.message_loss` carrying droppedIndicators
# — the socket is connected but discarding message notifications, most likely
# because this session never opens a conversation and so never subscribes.
#
# The cost is real and worth stating: opening a chat MARKS IT READ for this
# account. That is acceptable only because OM DUTY is a dedicated account which
# already shows permanently online — never point this at a person's own account.

# Candidate selectors for one message row. Teams renames these between releases,
# so every read reports which one matched (see read_latest_messages) and the
# order here is "most specific first".
_MSG_ROW_SELS = [
    "[data-tid='chat-pane-message']",
    "[data-tid='chat-pane-item']",
    "div[data-tid^='chat-pane']",
    ".fui-ChatMessage",
    "[data-tid='message-body']",
    "[role='listitem']",
]
_MSG_PANE_SELS = [
    "[data-tid='message-pane']",
    "[data-tid='messages-pane']",
    "[data-tid='chat-pane-list']",
    "[role='log']",
]

# `chat-pane-item` — the selector that actually matches on this Teams build —
# covers membership/call/system rows as well as real messages, and the newest row
# in the list is frequently one of those ("X left the chat"). Filtering them out
# is what makes "latest message" mean the latest *message*.
_SYSTEM_EVENT_RE = re.compile(
    r"(?i)\b(?:left|joined|rejoined)\s+the\s+(?:chat|conversation|group|meeting|team)"
    r"|\badded\b.{0,40}?\bto\s+the\s+(?:chat|conversation|group)"
    r"|\bremoved\b.{0,40}?\bfrom\s+the\s+(?:chat|conversation|group)"
    r"|\b(?:changed|renamed)\b.{0,30}?\b(?:name|picture|photo|image)"
    r"|\b(?:started|ended|missed|declined)\b.{0,12}?\bcall\b"
    r"|\b(?:pinned|unpinned)\s+a\s+message"
    r"|\bcreated\s+the\s+(?:chat|group)"
    r"|\bnow\s+has\s+access\s+to"
    r"|加入了聊天|离开了聊天|退出了聊天|已加入|已离开"
)
# A real EVO notice runs to hundreds of characters; a system event is one short
# line. The guard stops a genuine notice that happens to contain e.g. "left the
# chat" from being discarded.
_SYSTEM_MAX_CHARS = 300


def _dedupe_lines(text: str) -> str:
    """Collapse Teams' doubled innerText.

    Each row carries a visually-hidden accessibility copy next to the rendered
    one, so ``innerText`` returns every line twice ("X left the chat." printed
    twice in the first live read). Handles both a repeated adjacent line and a
    wholesale duplicated block.
    """
    lines = [ln.rstrip() for ln in (text or "").splitlines()]
    out: list[str] = []
    for line in lines:
        if out and line.strip() and line.strip() == out[-1].strip():
            continue
        out.append(line)
    # A B A B -> A B
    n = len(out)
    if n >= 2 and n % 2 == 0 and out[: n // 2] == out[n // 2:]:
        out = out[: n // 2]
    return "\n".join(out).strip()


def _looks_system(text: str) -> bool:
    body = (text or "").strip()
    if not body:
        return True
    if len(body) > _SYSTEM_MAX_CHARS:
        return False
    return bool(_SYSTEM_EVENT_RE.search(body))


# The conversation header, which names the chat that is ACTUALLY open.
_CHAT_TITLE_SELS = [
    "[data-tid='chat-header-title']",
    "[data-tid='chatHeaderTitle']",
    "[data-tid='conversation-header'] [role='heading']",
    "[role='heading'][aria-level='1']",
    "main h1",
    "h1",
]


def _open_chat_title(page) -> str:
    try:
        return (page.evaluate(
            """(sels) => {
                for (const s of sels) {
                    for (const el of document.querySelectorAll(s)) {
                        const t = (el.innerText || '').trim();
                        if (t && (el.offsetParent || el.offsetWidth || el.offsetHeight))
                            return t.split('\\n')[0];
                    }
                }
                return '';
            }""",
            _CHAT_TITLE_SELS,
        ) or "").strip()
    except Exception:
        return ""


def _norm_title(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip().lower()


def _titles_match(want: str, got: str) -> bool:
    """Compare on a normalised prefix — the header may truncate a long title."""
    a, b = _norm_title(want), _norm_title(got)
    if not a or not b:
        return False
    head = a[:20]
    return b.startswith(head) or a.startswith(b[:20]) or head in b


def _open_group(page, title: str) -> bool:
    """Open the sidebar row for ``title`` and CONFIRM it is the chat now showing.

    The confirmation is the whole point. Teams restores the last-viewed chat on
    load, so a message pane is already on screen before we click anything — an
    earlier version merely waited for "a pane exists", which succeeded whether or
    not the click landed, and silently scraped whatever chat happened to be open
    (it read 【关闭】ZF918(B) and reported it as the EVO group). Reading the wrong
    group could email the wrong notice, so a mismatch is a hard failure.
    """
    needle = (title or "").strip()
    if not needle:
        return False

    before = _open_chat_title(page)
    if before:
        print(f"[teams] chat open on arrival: {before!r}", flush=True)
    if _titles_match(needle, before):
        print("[teams] target chat already open", flush=True)
        page.wait_for_timeout(3000)
        return True

    # Match on a prefix: sidebar rows append the preview text and timestamp to
    # the title, so an equality test never matches. scrollIntoView first — the
    # sidebar is virtualised and an off-screen row cannot be clicked.
    found = page.evaluate(
        """(needle) => {
            const n = needle.toLowerCase().slice(0, 24);
            document.querySelectorAll('[data-teamswatch-chat]').forEach(
                el => el.removeAttribute('data-teamswatch-chat'));
            const sels = ["[data-tid='chat-list-item']", "[data-tid^='chat-list-item']",
                          "[role='treeitem']", "[role='listitem']"];
            for (const s of sels) {
                for (const el of document.querySelectorAll(s)) {
                    const t = (el.innerText || '').trim().toLowerCase();
                    if (!t || !t.includes(n)) continue;
                    el.setAttribute('data-teamswatch-chat', '1');
                    try { el.scrollIntoView({block: 'center'}); } catch (e) {}
                    return (el.innerText || '').trim().split('\\n')[0];
                }
            }
            return '';
        }""",
        needle,
    )
    if not found:
        print(f"[teams] sidebar row not found for {needle!r}", flush=True)
        return False
    print(f"[teams] clicking sidebar row: {found!r}", flush=True)
    try:
        page.locator("[data-teamswatch-chat='1']").first.click(timeout=15000)
    except Exception as err:  # noqa: BLE001
        print(f"[teams] clicking chat row failed: {err!r}", flush=True)
        return False

    # Wait for the HEADER to become the target, not merely for a pane to exist.
    deadline = time.monotonic() + 45
    seen = ""
    while time.monotonic() < deadline:
        seen = _open_chat_title(page)
        if _titles_match(needle, seen):
            page.wait_for_timeout(4000)  # let the virtualised list settle
            print(f"[teams] confirmed open: {seen!r}", flush=True)
            return True
        page.wait_for_timeout(1500)

    print(f"[teams] chat did not switch — header still {seen!r}, wanted {needle!r}",
          flush=True)
    return False


def _scroll_pane_up(page, px: int = 1200) -> bool:
    """Scroll the message list up to render older rows.

    The list is virtualised: only what is near the viewport exists in the DOM. So
    when the visible tail is all "X left the chat" there is no message to find
    without scrolling — which is exactly the case that returned a leave notice as
    the "latest message".
    """
    try:
        return bool(page.evaluate(
            """([sels, px]) => {
                const cands = [];
                for (const s of sels) {
                    for (const el of document.querySelectorAll(s)) cands.push(el);
                }
                // Also consider any ancestor that actually scrolls, since the
                // scrollable node is often a wrapper, not the labelled pane.
                document.querySelectorAll('div').forEach(el => {
                    if (el.scrollHeight > el.clientHeight + 200
                        && el.clientHeight > 200) cands.push(el);
                });
                for (const el of cands) {
                    if (el.scrollHeight > el.clientHeight + 50) {
                        const before = el.scrollTop;
                        el.scrollTop = Math.max(0, before - px);
                        if (el.scrollTop !== before) return true;
                    }
                }
                return false;
            }""",
            [_MSG_PANE_SELS, int(px)],
        ))
    except Exception as err:  # noqa: BLE001
        print(f"[teams] scroll up failed: {err!r}", flush=True)
        return False


def _scrape_rows(page, scan: int) -> dict[str, Any]:
    """Extract the last ``scan`` rows verbatim, reporting which selector worked.

    Deliberately returns everything including system events — filtering happens
    in Python where it is testable, and the counts tell us what was dropped.
    """
    return page.evaluate(
        """([sels, limit]) => {
            const out = {matched: null, counts: {}, rows: []};
            for (const s of sels) {
                let els;
                try { els = [...document.querySelectorAll(s)]; } catch (e) { continue; }
                out.counts[s] = els.length;
                if (!out.matched && els.length) {
                    out.matched = s;
                    // Virtualised list: the DOM tail is the newest.
                    for (const el of els.slice(-limit)) {
                        const pick = (q) => {
                            const n = el.querySelector(q);
                            return n ? (n.innerText || '').trim() : '';
                        };
                        const tEl = el.querySelector('time');
                        out.rows.push({
                            id: el.getAttribute('data-mid')
                                || el.getAttribute('data-tid')
                                || el.getAttribute('id') || '',
                            author: pick("[data-tid='message-author-name']")
                                 || pick('[itemprop=name]') || '',
                            time: (tEl ? (tEl.getAttribute('datetime')
                                          || tEl.innerText || '') : '').trim(),
                            // The body alone, when Teams exposes it as its own
                            // node. This is what gets emailed, so the author name
                            // and timestamp must not be glued onto the front.
                            body: pick("[data-tid='message-body']")
                               || pick("[data-tid='messageBodyContent']")
                               || pick('.fui-ChatMessage__body') || '',
                            text: (el.innerText || '').trim(),
                        });
                    }
                }
            }
            return out;
        }""",
        [_MSG_ROW_SELS, int(scan)],
    ) or {}


def _strip_meta(text: str, author: str, when: str) -> str:
    """Drop a leading author/timestamp line from a row's innerText.

    Teams concatenates the author and time into the row text ("Justin9:49\\nSD-…"),
    and that prefix would otherwise be carried into the generated email.
    """
    lines = (text or "").splitlines()
    author = (author or "").strip()
    when = (when or "").strip()
    if not lines or not (author or when):
        return (text or "").strip()
    head = lines[0].strip()
    # Only strip when the first line is *nothing but* metadata — never touch a
    # line that also carries notice content.
    residue = head
    for piece in (author, when):
        if piece:
            residue = residue.replace(piece, "")
    if head != residue and not re.sub(r"[\s:,\-–|]", "", residue):
        return "\n".join(lines[1:]).strip()
    return (text or "").strip()


def _pick_messages(rows: list[dict[str, Any]], limit: int) -> dict[str, Any]:
    """Dedupe each row's text, drop system events, return the newest ``limit``."""
    kept: list[dict[str, Any]] = []
    skipped: list[str] = []
    for row in rows or []:
        row = dict(row)
        body = _dedupe_lines(str(row.get("body") or ""))
        if body:
            row["text"] = body
        else:
            row["text"] = _strip_meta(
                _dedupe_lines(str(row.get("text") or "")),
                str(row.get("author") or ""), str(row.get("time") or ""),
            )
        if _looks_system(row["text"]):
            first = (row["text"].splitlines() or [""])[0]
            skipped.append(first[:60])
            continue
        kept.append(row)
    return {"messages": kept[-limit:] if limit > 0 else kept,
            "skipped_system": skipped, "scanned": len(rows or [])}


def read_latest_messages(*, group: str | None = None, limit: int = 3,
                         headless: bool = True) -> dict:
    """Open ``group`` in Teams and return its most recent messages.

    Returns ``{"ok", "group", "matched", "counts", "messages", "shot", "error"}``.
    ``counts`` reports every candidate selector and how many nodes it matched, so
    a Teams UI change is diagnosable from one run instead of guesswork.
    """
    from playwright.sync_api import sync_playwright

    target = (group or os.getenv("EVOTEAMS_GROUP")
              or "@EVO C88live/slot_ow.ph (RTS) CS Group NE RT FP")
    res: dict[str, Any] = {"ok": False, "group": target, "matched": None,
                           "counts": {}, "messages": [], "shot": None, "error": None,
                           "opened_title": ""}

    with _profile_lock("read-latest"), sync_playwright() as p:
        ctx, page = _open(p, headless=headless)
        try:
            page.goto(TEAMS_URL, wait_until="domcontentloaded", timeout=60000)
            deadline = time.monotonic() + max(60, int(os.getenv("TEAMS_BOOT_WAIT", "90")))
            stage = "unknown"
            while time.monotonic() < deadline:
                stage = _stage_of(page)
                if stage == "teams_loaded":
                    break
                if stage in _NEEDS_HUMAN or stage in ("landing", "email", "password"):
                    break
                page.wait_for_timeout(2000)
            if stage != "teams_loaded":
                res["error"] = f"not signed in (stage: {stage}) — run --login"
                res["shot"] = _shot(page, f"read_{stage}")
                return res

            if not _open_group(page, target):
                res["opened_title"] = _open_chat_title(page)
                res["error"] = (
                    f"could not open the target group — Teams is showing "
                    f"{res['opened_title']!r}"
                    if res["opened_title"] else "could not open the target group"
                )
                res["shot"] = _shot(page, "read_no_group")
                return res

            # Belt and braces: re-read the header immediately before scraping, so
            # a chat that switches underneath us can never be reported as this one.
            res["opened_title"] = _open_chat_title(page)
            if not _titles_match(target, res["opened_title"]):
                res["error"] = (
                    f"chat changed before reading — showing "
                    f"{res['opened_title']!r}, wanted {target!r}"
                )
                res["shot"] = _shot(page, "read_wrong_chat")
                return res

            # Scan well past `limit`: the newest rows are often system events, so
            # a window of exactly `limit` can contain no real message at all.
            scan = max(30, limit * 10)
            scraped = _scrape_rows(page, scan)
            picked = _pick_messages(scraped.get("rows") or [], limit)

            # Still nothing but "X left the chat"? Scroll up for older rows —
            # virtualisation means they are not in the DOM until we do.
            for attempt in range(1, 4):
                if picked["messages"]:
                    break
                if not _scroll_pane_up(page):
                    print("[teams] nothing scrollable — cannot reach older rows",
                          flush=True)
                    break
                print(f"[teams] only system events so far; scrolled up "
                      f"({attempt}/3)", flush=True)
                page.wait_for_timeout(2500)
                scraped = _scrape_rows(page, scan)
                picked = _pick_messages(scraped.get("rows") or [], limit)

            res["matched"] = scraped.get("matched")
            res["counts"] = scraped.get("counts") or {}
            res["messages"] = picked["messages"]
            res["skipped_system"] = picked["skipped_system"]
            res["scanned"] = picked["scanned"]
            res["ok"] = bool(res["messages"])
            res["shot"] = _shot(page, "read_latest")
            if not res["ok"]:
                res["error"] = (
                    f"scanned {picked['scanned']} row(s); all were system events "
                    f"or empty ({len(picked['skipped_system'])} skipped)"
                    if picked["skipped_system"]
                    else "opened the chat but matched no message rows"
                )
        except Exception as err:  # noqa: BLE001
            res["error"] = repr(err)
            try:
                res["shot"] = _shot(page, "read_exception")
            except Exception:
                pass
        finally:
            try:
                ctx.close()
            except Exception:
                pass
    return res


def _fmt_messages(res: dict) -> str:
    """Human-readable summary for /latestevo."""
    if not res.get("ok"):
        counts = ", ".join(f"{k}={v}" for k, v in (res.get("counts") or {}).items())
        return (f"❌ Could not read latest message from Teams\n"
                f"• Group: {res.get('group')}\n"
                f"• Reason: {res.get('error') or 'unknown'}"
                + (f"\n• Selector counts: {counts}" if counts else ""))
    lines = [f"📥 Latest in Teams group\n• Group: {res.get('group')}",
             # Echo the chat Teams actually had open — proof we read the right one.
             f"• Confirmed open: {res.get('opened_title') or '—'}",
             f"• Matched selector: {res.get('matched')}"]
    skipped = res.get("skipped_system") or []
    if skipped:
        # Say what was passed over, so a surprising "latest" is explainable.
        lines.append(f"• Skipped {len(skipped)} system event(s): "
                     + "; ".join(skipped[:3]))
    lines.append("")
    for i, msg in enumerate(res["messages"], 1):
        body = (msg.get("text") or "").strip()
        if len(body) > 1500:
            body = body[:1500] + f"\n… (+{len(msg['text']) - 1500} chars)"
        head = " | ".join(x for x in (msg.get("author"), msg.get("time")) if x)
        lines.append(f"--- {i}/{len(res['messages'])}"
                     + (f"  ({head})" if head else "") + f" ---\n{body}")
    return "\n".join(lines)


def send_latest_to_lark(chat_id: str, *, group: str | None = None,
                        limit: int = 1) -> dict:
    """``/latestevo`` — read the group and post what we found back to ``chat_id``."""
    res = read_latest_messages(group=group, limit=limit)
    try:
        send_text(chat_id, _fmt_messages(res))
    except Exception as err:  # noqa: BLE001
        print(f"[teams] /latestevo text send failed: {err!r}", flush=True)
    # The screenshot is the fastest way to see WHY a read came back empty.
    if not res.get("ok"):
        send_shot(chat_id, res.get("shot"))
    return res


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
    lines.append("• Phase: login done; websocket capture pending real frames")

    # The EVO detector is a separate module but the same operational question, so
    # /teamstatus reports both rather than making anyone run two commands.
    try:
        import detectevomaintenance as _evom

        lines.append("")
        lines.extend(_evom.status_lines())
    except Exception as err:  # noqa: BLE001
        lines.append(f"• EVO detector: unavailable ({err!r})")
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
    ap.add_argument("--shot", action="store_true",
                    help="fresh screenshot of the live session (add --report-chat to send it)")
    ap.add_argument("--status", action="store_true", help="print the status summary")
    ap.add_argument("--check-env", action="store_true",
                    help="fingerprint the .env credentials (no secrets printed)")
    ap.add_argument("--list-chats", action="store_true",
                    help="dump sidebar conversation titles to teamswatch_chats.json")
    ap.add_argument("--dump-frames", action="store_true",
                    help="record websocket frames to teamswatch_frames.jsonl (phase-2 recon)")
    ap.add_argument("--scan-frames", action="store_true",
                    help="summarise an existing capture: shape only, no secrets")
    ap.add_argument("--read-latest", action="store_true",
                    help="open the watched group and print its latest messages")
    ap.add_argument("--group", default=None, help="override the group title to read")
    ap.add_argument("--limit", type=int, default=3,
                    help="how many recent messages --read-latest returns")
    ap.add_argument("--seconds", type=int, default=300,
                    help="max seconds --dump-frames records for (default 300)")
    ap.add_argument("--min-messages", type=int, default=1,
                    help="stop --dump-frames early after N message frames (default 1)")
    ap.add_argument("--headed", action="store_true", help="visible browser (local debug)")
    ap.add_argument("--report-chat", default=None,
                    help="Lark chat_id to send the result + screenshot to")
    args = ap.parse_args(argv)

    if args.check_env:
        return check_env()
    if args.list_chats:
        res = list_chats(headless=not args.headed)
        return 0 if res.get("titles") else 1
    if args.scan_frames:
        res = scan_frames()
        return 0 if res.get("frames") else 1
    if args.read_latest:
        res = read_latest_messages(group=args.group, limit=args.limit,
                                   headless=not args.headed)
        print("\nselector counts (which message-row selector matched):")
        for sel, count in (res.get("counts") or {}).items():
            print(f"   {count:6d}  {sel}")
        print()
        print(_fmt_messages(res))
        if args.report_chat:
            send_text(args.report_chat, _fmt_messages(res))
            send_shot(args.report_chat, res.get("shot"))
        return 0 if res.get("ok") else 1
    if args.dump_frames:
        res = dump_frames(seconds=args.seconds, min_messages=args.min_messages,
                          headless=not args.headed)
        print(json.dumps({k: v for k, v in res.items() if k != "sockets"}, indent=2))
        return 0 if res.get("frames") else 1
    if args.status:
        print("\n".join(status_lines()))
        return 0
    if args.login:
        res = do_login(headless=not args.headed, report_chat=args.report_chat)
        print(json.dumps(res, indent=2))
        return 0 if res["ok"] else 1
    if args.check or args.shot:
        res = check_session(headless=not args.headed)
        print(json.dumps(res, indent=2))
        if args.shot and args.report_chat:
            sent = send_shot(args.report_chat, res.get("shot"))
            print(f"screenshot sent to {args.report_chat}: {sent}")
        return 0 if res["ok"] else 1

    ap.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
