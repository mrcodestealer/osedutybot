#!/usr/bin/env python3
"""
osmwatch.py — let the bot reach the OSM-Watch dashboard (Lark-OAuth protected)
and screenshot it.

Why this shape
--------------
``osm-watch.cliveslot.com`` is gated by **Lark OAuth** for app
``cli_a83bcf5f86fad029`` and asks for *user* contact scopes
(``contact:user.base``, ``contact:user.email``). That needs a **logged-in Lark
user session**, not this bot's app token (our APP_ID is a different Lark app and
we don't hold osm-watch's secret). So there is no server-to-server token path —
the only robust route is a real browser session.

We reuse the same trick the np/dhs/jenkins backends already use in this repo: a
**persistent Chromium profile**. Log in via Lark ONCE in a visible window; the
session cookie is saved into ``browser_data/osmwatch/`` (gitignored). Every run
after that is headless and reuses that session.

Two ways the bot can get in
---------------------------
A) Username/password (fully headless, no manual step) — RECOMMENDED for a bot.
   osm-watch's login page also offers "Use password instead" (a plain Django
   form). Put creds in .env and every run logs itself in:
       OSMWATCH_USER=...
       OSMWATCH_PASS=...
       python osmwatch.py --send

B) Lark OAuth session (one-time login, then reused).
   Sign in with Lark once; the session is saved to browser_data/osmwatch/ and
   reused headless afterwards. On a headless server the Lark QR is pushed to a
   Lark chat (default: the lab group) so you can scan it from your phone:
       python osmwatch.py --login      # QR is sent to --qr-to chat; scan it
       python osmwatch.py --send       # thereafter, headless

   NOTE: the saved session survives a service RESTART (it's on disk), but is
   lost if browser_data/ is wiped, the session expires, or you move to another
   machine. Path A (password) has no such dependency — prefer it for the bot.

Usage
-----
  python osmwatch.py                     # capture, save osmwatch.png locally
  python osmwatch.py --send              # also send the PNG to the Lark duty chat
  python osmwatch.py --send-to oc_xxx    # send to a specific chat_id
  python osmwatch.py --url https://osm-watch.cliveslot.com/some/page --send
  python osmwatch.py --user U --pass P   # password login without touching .env
  python osmwatch.py --login             # one-time Lark login (visible browser)
  python osmwatch.py --headed            # watch it run (debug), still reuses session

Requires: pip install playwright python-dotenv requests && playwright install chromium
"""

from __future__ import annotations

import argparse
import mimetypes
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

# Windows consoles default to cp1252 and choke on the ✅/→ symbols below.
# The Linux server runs UTF-8 already; this is a no-op there.
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

# Persistent Chromium profile. Lives under browser_data/ which is gitignored,
# so the saved Lark session never gets committed.
PROFILE_DIR = _ROOT_DIR / "browser_data" / "osmwatch"

# The dashboard + the full Lark-OAuth login entry point you were given.
OSM_BASE = os.getenv("OSMWATCH_BASE_URL", "https://osm-watch.cliveslot.com").rstrip("/")
LOGIN_URL = os.getenv(
    "OSMWATCH_LOGIN_URL",
    "https://accounts.larksuite.com/accounts/page/login?app_id=12&no_trap=1"
    "&redirect_uri=https%3A%2F%2Fopen.larksuite.com%2Fopen-apis%2Fauthen%2Fv1%2Fauthorize"
    "%3Fapp_id%3Dcli_a83bcf5f86fad029%26redirect_uri%3Dhttps%253A%252F%252Fosm-watch.cliveslot.com"
    "%252Fauth%252Flark%252Fcallback%252F%26response_type%3Dcode"
    "%26scope%3Dcontact%253Auser.base%253Areadonly%2Bcontact%253Auser.email%253Areadonly",
)

# Hosts that mean "you are NOT authenticated yet" (Lark login / consent walls).
_AUTH_WALL_HOSTS = ("accounts.larksuite.com", "accounts.feishu.cn", "open.larksuite.com", "open.feishu.cn")

# osm-watch serves its OWN login wall at these paths (same host as the dashboard),
# so a host match alone does not prove we're signed in. The page shows
# "Welcome Back / Sign in with Lark / Use password instead".
_LOGIN_PATH_HINTS = ("/login", "/signin", "/sign-in", "/auth/lark", "/auth/login", "/auth/callback")

DEFAULT_SHOT = _ROOT_DIR / "osmwatch.png"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _truthy(val: str | None) -> bool:
    return str(val or "").strip().lower() in ("1", "true", "yes", "on", "y")


def _headless_default() -> bool:
    """Headless unless BOT_PLAYWRIGHT_HEADLESS says otherwise (matches the repo)."""
    v = os.getenv("BOT_PLAYWRIGHT_HEADLESS")
    if v is None:
        return True
    return _truthy(v)


def _host(url: str) -> str:
    return (urlparse(url).hostname or "").lower()


def _is_auth_wall(url: str) -> bool:
    h = _host(url)
    return any(h == w or h.endswith("." + w) for w in _AUTH_WALL_HOSTS)


def _on_osmwatch(url: str) -> bool:
    return _host(url) == _host(OSM_BASE)


def _is_login_path(url: str) -> bool:
    p = (urlparse(url).path or "").lower()
    return any(p == h or p.startswith(h) for h in _LOGIN_PATH_HINTS)


def _authenticated(url: str) -> bool:
    """Signed in = on the osm-watch host AND not sitting on a login/auth wall."""
    return _on_osmwatch(url) and not _is_auth_wall(url) and not _is_login_path(url)


# ---------------------------------------------------------------------------
# Lark image send (self-contained; uses this bot's APP_ID/APP_SECRET)
# ---------------------------------------------------------------------------
def _lark_base() -> str:
    return os.getenv("LARK_OPEN_BASE", "https://open.larksuite.com").rstrip("/")


def get_tenant_access_token() -> str:
    app_id = os.getenv("APP_ID")
    app_secret = os.getenv("APP_SECRET")
    if not app_id or not app_secret:
        raise RuntimeError("APP_ID / APP_SECRET not set in environment (.env)")
    url = f"{_lark_base()}/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": app_id, "app_secret": app_secret}, timeout=30)
    result = resp.json()
    if result.get("code") != 0:
        raise RuntimeError(f"Failed to get tenant token: {result}")
    return result["tenant_access_token"]


def upload_image_lark(image_path: str) -> str | None:
    """Upload a PNG/JPEG for im/v1/messages msg_type=image; returns image_key or None."""
    token = get_tenant_access_token()
    url = f"{_lark_base()}/open-apis/im/v1/images"
    headers = {"Authorization": f"Bearer {token}"}
    ext = os.path.splitext(image_path)[1].lower()
    mime, _ = mimetypes.guess_type(image_path)
    if not mime or mime not in ("image/png", "image/jpeg"):
        mime = "image/jpeg" if ext in (".jpg", ".jpeg") else "image/png"
    with open(image_path, "rb") as f:
        files = {"image": (os.path.basename(image_path), f, mime)}
        data = {"image_type": "message"}
        resp = requests.post(url, headers=headers, files=files, data=data, timeout=60)
    result = resp.json()
    if result.get("code") == 0:
        return result.get("data", {}).get("image_key")
    print(f"❌ Lark image upload failed: {result}")
    return None


def send_image_message(chat_id: str, image_key: str) -> dict:
    import json

    token = get_tenant_access_token()
    url = f"{_lark_base()}/open-apis/im/v1/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "receive_id": chat_id,
        "msg_type": "image",
        "content": json.dumps({"image_key": image_key}),
    }
    params = {"receive_id_type": "chat_id"}
    return requests.post(url, headers=headers, params=params, json=payload, timeout=30).json()


def send_text_message(chat_id: str, text: str) -> dict:
    import json

    token = get_tenant_access_token()
    url = f"{_lark_base()}/open-apis/im/v1/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "receive_id": chat_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}),
    }
    params = {"receive_id_type": "chat_id"}
    return requests.post(url, headers=headers, params=params, json=payload, timeout=30).json()


def send_screenshot_to_lark(shot_path: str, chat_id: str) -> bool:
    key = upload_image_lark(shot_path)
    if not key:
        return False
    resp = send_image_message(chat_id, key)
    if resp.get("code") == 0:
        print(f"✅ Screenshot sent to Lark chat {chat_id}")
        return True
    print(f"❌ Lark send failed: {resp}")
    return False


# ---------------------------------------------------------------------------
# Browser flows
# ---------------------------------------------------------------------------
def _new_context(p, *, headless: bool):
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    ctx = p.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        headless=headless,
        viewport={"width": 1600, "height": 900},
        ignore_https_errors=True,
        user_agent=_UA,
    )
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    return ctx, page


def _settle_url(page, *, seconds: float = 20.0) -> str:
    """Wait through any silent SSO redirect chain until the URL stops moving."""
    deadline = time.time() + seconds
    last = page.url
    stable_for = 0.0
    while time.time() < deadline:
        page.wait_for_timeout(700)
        cur = page.url
        if cur == last:
            stable_for += 0.7
            if stable_for >= 2.0:
                break
        else:
            stable_for = 0.0
            last = cur
    return page.url


def _find_qr_element(page, *, tries: int = 8):
    """Return the visible, roughly-square QR canvas/img, or None."""
    for _ in range(max(1, tries)):
        for el in page.query_selector_all("canvas, img[src*='qr'], img[alt*='QR'], img[alt*='qr']"):
            try:
                if not el.is_visible():
                    continue
                b = el.bounding_box()
                if not b:
                    continue
                w, h = b.get("width", 0), b.get("height", 0)
                if 120 <= w <= 400 and 120 <= h <= 400 and 0.8 <= (w / max(h, 1)) <= 1.25:
                    return el
            except Exception:
                continue
        page.wait_for_timeout(700)
    return None


def _reveal_qr(page) -> None:
    """Lark defaults to email login; the top-right corner icon toggles QR view."""
    for attempt in (
        lambda: page.click(".login-qr-switch-box", position={"x": 388, "y": 12}, force=True, timeout=4000),
        lambda: page.click(".login-qr-switch-box", force=True, timeout=3000),
    ):
        try:
            attempt()
            page.wait_for_timeout(1200)
            return
        except Exception:
            continue


def _capture_qr(page, out_path: Path) -> Path:
    """Screenshot the Lark QR (tight crop). Toggle to QR view only if needed —
    re-clicking the toggle when a QR is already shown flips back to email login."""
    el = _find_qr_element(page, tries=1)  # already in QR view (e.g. periodic re-send)?
    if el is None:
        _reveal_qr(page)
        el = _find_qr_element(page, tries=8)
    if el is not None:
        try:
            el.screenshot(path=str(out_path))
            return out_path
        except Exception:
            pass
    page.screenshot(path=str(out_path), full_page=True)
    return out_path


def do_login(*, timeout_s: int, headless: bool, qr_chat_id: str | None, resend_sec: int = 90) -> int:
    """Log in to Lark once; session is saved to PROFILE_DIR.

    On a headless server nobody can see the browser, so the Lark QR is captured
    and pushed to ``qr_chat_id``; scan it from the Lark app to complete login.
    A fresh QR is re-sent every ``resend_sec`` because Lark QRs expire.
    """
    from playwright.sync_api import sync_playwright

    qr_path = PROFILE_DIR.parent / "osmwatch_qr.png"
    mode = "headless" if headless else "visible"
    print(f"→ Opening Lark login ({mode}) …")
    if qr_chat_id:
        print(f"  QR will be sent to Lark chat {qr_chat_id} — scan it with your Lark app.")
    elif not headless:
        print("  Scan the QR in the browser window (or use email/OTP/password).")

    with sync_playwright() as p:
        ctx, page = _new_context(p, headless=headless)
        try:
            page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)
            try:
                page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass

            def push_qr(first: bool) -> None:
                if not qr_chat_id:
                    return
                try:
                    _capture_qr(page, qr_path)
                    if first:
                        send_text_message(
                            qr_chat_id,
                            "🔐 OSM-Watch login: scan this QR with your Lark app to sign the "
                            "bot in. A fresh QR is re-sent every ~90s until login completes.",
                        )
                    send_screenshot_to_lark(str(qr_path), qr_chat_id)
                except Exception as e:
                    print(f"⚠️  Could not push QR to Lark: {e!r}")

            push_qr(first=True)
            deadline = time.time() + timeout_s
            next_resend = time.time() + resend_sec
            while time.time() < deadline:
                if _authenticated(page.url):
                    _settle_url(page, seconds=8)
                    if _authenticated(page.url):
                        break
                if qr_chat_id and time.time() >= next_resend and not _authenticated(page.url):
                    push_qr(first=False)
                    next_resend = time.time() + resend_sec
                page.wait_for_timeout(1000)
            else:
                print(f"⚠️  Timed out after {timeout_s}s still at: {page.url}")
                print("   Login not completed. Re-run:  python osmwatch.py --login")
                if qr_chat_id:
                    try:
                        send_text_message(qr_chat_id, "⚠️ OSM-Watch login timed out — QR not scanned in time.")
                    except Exception:
                        pass
                return 1

            print(f"✅ Logged in — landed on {page.url}")
            try:
                page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass
            page.screenshot(path=str(DEFAULT_SHOT), full_page=True)
            print(f"✅ Session saved to {PROFILE_DIR}")
            print(f"✅ Screenshot: {DEFAULT_SHOT}")
            if qr_chat_id:
                try:
                    send_text_message(qr_chat_id, "✅ OSM-Watch: bot logged in successfully.")
                except Exception:
                    pass
            print("\nYou can now run headless anytime:  python osmwatch.py --send")
            return 0
        finally:
            ctx.close()


def _password_login(page, user: str, pw: str, *, timeout_ms: int) -> bool:
    """Headless login via osm-watch's own username/password form (no Lark, no QR).

    The form is a plain Django form: #id_username, #id_password, submit "Sign In",
    revealed by the "Use password instead" toggle.
    """
    if not _is_login_path(page.url):
        page.goto(f"{OSM_BASE}/login/", wait_until="domcontentloaded", timeout=timeout_ms)
    # Reveal the password panel (best effort — it may already be visible).
    for sel in ("text=Use password instead", "button:has-text('password')", "text=password"):
        try:
            page.click(sel, timeout=2500)
            page.wait_for_timeout(500)
            break
        except Exception:
            continue
    try:
        page.fill("#id_username", user, timeout=timeout_ms)
        page.fill("#id_password", pw, timeout=timeout_ms)
    except Exception as e:
        print(f"❌ Could not fill the password form: {e!r}")
        return False
    for sel in ("button[type=submit]", "button:has-text('Sign In')", "text=Sign In"):
        try:
            page.click(sel, timeout=2500)
            break
        except Exception:
            continue
    page.wait_for_timeout(1500)
    _settle_url(page, seconds=20)
    return _authenticated(page.url)


def do_capture(
    *,
    headless: bool,
    target_url: str,
    out_path: Path,
    timeout_ms: int,
    user: str | None = None,
    pw: str | None = None,
) -> int:
    """Reuse the saved session (or password-login) then screenshot the dashboard."""
    from playwright.sync_api import sync_playwright

    mode = "headless" if headless else "headed"
    print(f"→ Opening {target_url} ({mode}, reusing saved session)…")

    with sync_playwright() as p:
        ctx, page = _new_context(p, headless=headless)
        try:
            try:
                page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
            except Exception as e:
                print(f"❌ Navigation error: {e!r}")
                return 1

            final = _settle_url(page, seconds=25)

            # Not signed in? If username/password are configured, log in headlessly.
            if not _authenticated(final) and user and pw:
                print("→ No session — attempting headless username/password login…")
                if _password_login(page, user, pw, timeout_ms=timeout_ms):
                    page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
                    final = _settle_url(page, seconds=25)
                else:
                    print("❌ Password login did not reach the dashboard (check OSMWATCH_USER/OSMWATCH_PASS).")

            if not _authenticated(final):
                where = "Lark login wall" if _is_auth_wall(final) else "osm-watch login page"
                print(f"⚠️  Not authenticated — sitting on the {where}: {final}")
                print("   Either set OSMWATCH_USER / OSMWATCH_PASS in .env for headless login,")
                print("   or do the one-time Lark login:  python osmwatch.py --login")
                return 2

            try:
                page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass

            out_path.parent.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(out_path), full_page=True)
            title = (page.title() or "").strip()
            print(f"✅ Access OK — {final}")
            print(f"   title: {title!r}")
            print(f"✅ Screenshot saved: {out_path}")
            return 0
        finally:
            ctx.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Reach the Lark-protected OSM-Watch dashboard and screenshot it.",
    )
    ap.add_argument("--login", action="store_true",
                    help="One-time: open a visible browser to log in to Lark and save the session.")
    ap.add_argument("--headed", action="store_true",
                    help="Run the capture with a visible browser (debug); still reuses the session.")
    ap.add_argument("--url", default=OSM_BASE,
                    help=f"Page to open/screenshot (default: {OSM_BASE}).")
    ap.add_argument("--out", default=str(DEFAULT_SHOT),
                    help=f"Screenshot output path (default: {DEFAULT_SHOT}).")
    ap.add_argument("--send", action="store_true",
                    help="After a successful screenshot, send it to the Lark duty chat (DUTY_CHAT_ID).")
    ap.add_argument("--send-to", default=None,
                    help="Send the screenshot to this chat_id (implies --send).")
    ap.add_argument("--user", default=os.getenv("OSMWATCH_USER"),
                    help="osm-watch username for headless password login (or set OSMWATCH_USER).")
    ap.add_argument("--pass", dest="password", default=os.getenv("OSMWATCH_PASS"),
                    help="osm-watch password for headless password login (or set OSMWATCH_PASS).")
    ap.add_argument("--qr-to", default=os.getenv("OSMWATCH_QR_CHAT_ID", "oc_ad9b5bdbb2826ba2ee9730920ef25432"),
                    help="With --login, send the Lark QR to this chat_id so you can scan it "
                         "(default: the lab group; set OSMWATCH_QR_CHAT_ID to change; '' to disable).")
    ap.add_argument("--login-timeout", type=int, default=300,
                    help="Seconds to wait for you to finish the manual login (default 300).")
    ap.add_argument("--timeout-ms", type=int, default=60_000,
                    help="Navigation timeout for the capture (default 60000).")
    args = ap.parse_args(argv)

    if args.login:
        return do_login(
            timeout_s=max(30, args.login_timeout),
            headless=(_headless_default() and not args.headed),
            qr_chat_id=(args.qr_to.strip() or None) if args.qr_to else None,
        )

    out_path = Path(args.out)
    rc = do_capture(
        headless=(_headless_default() and not args.headed),
        target_url=args.url,
        out_path=out_path,
        timeout_ms=max(5_000, args.timeout_ms),
        user=args.user,
        pw=args.password,
    )
    if rc != 0:
        return rc

    chat_id = args.send_to or (os.getenv("DUTY_CHAT_ID") if args.send else None)
    if chat_id:
        ok = send_screenshot_to_lark(str(out_path), chat_id)
        return 0 if ok else 3
    elif args.send:
        print("⚠️  --send given but no chat_id (set DUTY_CHAT_ID in .env or use --send-to).")
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
