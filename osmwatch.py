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

The session (cookies + localStorage) is stored in a portable ``osmwatch.json``
so it SURVIVES a service restart (reloaded on launch) and can be copied to
another machine. A long-lived **warm browser** (mirroring third_http_warm_pool)
stays open in the bot process, refreshes the session while idle, and — when the
session expires — auto-pushes a Lark QR to the group. If nobody scans it in time
the bot stops (no spam) and waits for a manual ``/loginosmwatch`` to re-send.

How the bot gets in — Lark QR only
----------------------------------
Sign in with Lark by scanning a QR. On a headless server nobody can see the
browser, so the QR is pushed to a Lark chat (default: the lab group); scan it
from your phone. The session is then saved to osmwatch.json and reused headless.
When it expires the warm browser auto-pushes a fresh QR; if that QR times out
unscanned, tag the bot with /loginosmwatch to get a new one.

Usage
-----
  python osmwatch.py --login             # push a login QR to the group; scan it
  python osmwatch.py                     # capture, save osmwatch.png locally
  python osmwatch.py --send              # also send the PNG to the Lark duty chat
  python osmwatch.py --send-to oc_xxx    # send to a specific chat_id
  python osmwatch.py --url https://osm-watch.cliveslot.com/some/page --send
  python osmwatch.py --headed            # watch it run locally (debug)

Requires: pip install playwright python-dotenv requests && playwright install chromium
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import queue
import sys
import threading
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

# Session store. We keep cookies + localStorage in a portable storage_state JSON
# so the session survives a service restart (reloaded on launch) and can even be
# copied to another machine. Gitignored — it holds live session tokens.
OSMWATCH_JSON = _ROOT_DIR / os.getenv("OSMWATCH_STATE_FILE", "osmwatch.json")
# QR image the bot posts to Lark, and a tiny file remembering whether we're
# waiting for a manual /loginosmwatch (so a restart doesn't re-spam the group).
QR_PNG = _ROOT_DIR / "osmwatch_qr.png"
_LOGIN_STATE = _ROOT_DIR / "browser_data" / "osmwatch_login.json"

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
    """Signed in = on the osm-watch host AND not sitting on a login/auth wall.

    URL-only signal — use :func:`_classify` after a navigation to also rule out
    Cloudflare/WAF block pages (which live on the same host, path ``/``)."""
    return _on_osmwatch(url) and not _is_auth_wall(url) and not _is_login_path(url)


# Cloudflare / WAF interstitials render on the osm-watch host itself, so a URL
# check alone would mistake them for the dashboard. These phrases identify them.
_BLOCK_MARKERS = (
    "been blocked",
    "attention required",
    "verify you are human",
    "checking your browser",
    "access denied",
    "cloudflare ray id",
)


def _looks_blocked(page) -> bool:
    try:
        body = (page.inner_text("body") or "")[:800].lower()
    except Exception:
        return False
    return any(m in body for m in _BLOCK_MARKERS)


def _classify(page, resp) -> str:
    """Post-navigation verdict: 'blocked' | 'login' | 'error' | 'authenticated'."""
    status = resp.status if resp else None
    if _looks_blocked(page):
        return "blocked"
    url = page.url
    if not _on_osmwatch(url) or _is_auth_wall(url) or _is_login_path(url):
        return "login"
    if status is not None and status >= 400:
        return "error"
    return "authenticated"


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
def _open(p, *, headless: bool):
    """Launch a browser + context, restoring the saved session from osmwatch.json."""
    browser = p.chromium.launch(
        headless=headless,
        args=["--disable-blink-features=AutomationControlled"],
    )
    kwargs = {
        "viewport": {"width": 1600, "height": 900},
        "ignore_https_errors": True,
        "user_agent": _UA,
        "locale": "en-US",
        "timezone_id": os.getenv("OSMWATCH_TZ", "Asia/Manila"),
    }
    if OSMWATCH_JSON.exists():
        kwargs["storage_state"] = str(OSMWATCH_JSON)
    context = browser.new_context(**kwargs)
    # Light touch so trivial bot checks don't flag us; NOT a Cloudflare bypass.
    try:
        context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
    except Exception:
        pass
    page = context.pages[0] if context.pages else context.new_page()
    return browser, context, page


def _save_state(context) -> None:
    """Persist cookies + localStorage so the session survives a service restart."""
    try:
        context.storage_state(path=str(OSMWATCH_JSON))
    except Exception as e:
        print(f"[osmwatch] could not save session to {OSMWATCH_JSON.name}: {e!r}", flush=True)


# --- "waiting for manual /loginosmwatch" flag (persisted across restarts) -----
def _set_needs_manual(val: bool) -> None:
    try:
        _LOGIN_STATE.parent.mkdir(parents=True, exist_ok=True)
        _LOGIN_STATE.write_text(json.dumps({"needs_manual": bool(val)}))
    except Exception:
        pass


def _get_needs_manual() -> bool:
    try:
        return bool(json.loads(_LOGIN_STATE.read_text()).get("needs_manual"))
    except Exception:
        return False


# --- warm-browser configuration ----------------------------------------------
def _warm_enabled() -> bool:
    return _truthy(os.getenv("OSMWATCH_WARM", "1"))


def _qr_chat_default() -> str:
    return os.getenv("OSMWATCH_QR_CHAT_ID", "oc_ad9b5bdbb2826ba2ee9730920ef25432").strip()


def _keepalive_sec() -> int:
    try:
        return max(120, int(os.getenv("OSMWATCH_KEEPALIVE_SEC", "1800")))
    except ValueError:
        return 1800


def _login_timeout_s() -> int:
    try:
        return max(30, int(os.getenv("OSMWATCH_LOGIN_TIMEOUT", "240")))
    except ValueError:
        return 240


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


def _qr_login_on_page(page, *, qr_chat_id: str | None, timeout_s: int, resend_sec: int = 90, log=print) -> bool:
    """Drive the Lark QR login on an already-open page.

    Pushes the QR to ``qr_chat_id`` (re-sending a fresh one every ``resend_sec``
    because Lark QRs expire) and polls until authenticated or ``timeout_s``.
    Returns True iff we land on the authenticated dashboard. Caller saves state.
    """
    page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass

    def push_qr(first: bool) -> None:
        if not qr_chat_id:
            return
        try:
            _capture_qr(page, QR_PNG)
            if first:
                send_text_message(
                    qr_chat_id,
                    "🔐 OSM-Watch login: scan this QR with your Lark app to sign the bot in. "
                    "A fresh QR is re-sent every ~90s until login completes.",
                )
            send_screenshot_to_lark(str(QR_PNG), qr_chat_id)
        except Exception as e:
            log(f"⚠️  Could not push QR to Lark: {e!r}")

    push_qr(first=True)
    deadline = time.time() + timeout_s
    next_resend = time.time() + resend_sec
    while time.time() < deadline:
        if _authenticated(page.url):
            _settle_url(page, seconds=8)
            if _authenticated(page.url):
                return True
        if qr_chat_id and time.time() >= next_resend and not _authenticated(page.url):
            push_qr(first=False)
            next_resend = time.time() + resend_sec
        page.wait_for_timeout(1000)
    return _authenticated(page.url)


def do_login(*, timeout_s: int, headless: bool, qr_chat_id: str | None, resend_sec: int = 90) -> int:
    """One-shot CLI login; session is saved to osmwatch.json for the warm bot to reuse."""
    from playwright.sync_api import sync_playwright

    mode = "headless" if headless else "visible"
    print(f"→ Opening Lark login ({mode}) …")
    if qr_chat_id:
        print(f"  QR will be sent to Lark chat {qr_chat_id} — scan it with your Lark app.")
    elif not headless:
        print("  Scan the QR in the browser window with your Lark app.")

    with sync_playwright() as p:
        browser, ctx, page = _open(p, headless=headless)
        try:
            ok = _qr_login_on_page(page, qr_chat_id=qr_chat_id, timeout_s=timeout_s, resend_sec=resend_sec)
            if not ok:
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
            _save_state(ctx)
            _set_needs_manual(False)
            page.screenshot(path=str(DEFAULT_SHOT), full_page=True)
            print(f"✅ Session saved to {OSMWATCH_JSON.name}")
            print(f"✅ Screenshot: {DEFAULT_SHOT}")
            if qr_chat_id:
                try:
                    send_text_message(qr_chat_id, "✅ OSM-Watch: bot logged in successfully.")
                except Exception:
                    pass
            print("\nYou can now run headless anytime:  python osmwatch.py --send")
            return 0
        finally:
            try:
                browser.close()
            except Exception:
                pass


def do_capture(*, headless: bool, target_url: str, out_path: Path, timeout_ms: int) -> int:
    """Reuse the saved session then screenshot the dashboard."""
    from playwright.sync_api import sync_playwright

    mode = "headless" if headless else "headed"
    print(f"→ Opening {target_url} ({mode}, reusing saved session)…")

    with sync_playwright() as p:
        browser, ctx, page = _open(p, headless=headless)
        try:
            try:
                resp = page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
            except Exception as e:
                print(f"❌ Navigation error: {e!r}")
                return 1

            _settle_url(page, seconds=25)
            verdict = _classify(page, resp)

            if verdict == "blocked":
                print(f"⛔ Blocked by Cloudflare/WAF at {page.url}")
                print("   This is not a login problem. Allowlist the bot server's IP (or add a")
                print("   WAF bypass rule) in the osm-watch Cloudflare dashboard, then retry.")
                return 4
            if verdict != "authenticated":
                where = "Lark login wall" if _is_auth_wall(page.url) else "osm-watch login page"
                print(f"⚠️  Not authenticated — sitting on the {where}: {page.url}")
                print("   Do the one-time Lark login:  python osmwatch.py --login")
                return 2

            try:
                page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass

            final = page.url
            _save_state(ctx)  # refresh the on-disk session after a good load
            out_path.parent.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(out_path), full_page=True)
            title = (page.title() or "").strip()
            print(f"✅ Access OK — {final}")
            print(f"   title: {title!r}")
            print(f"✅ Screenshot saved: {out_path}")
            return 0
        finally:
            try:
                browser.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Warm browser (long-lived, in-process; mirrors third_http_warm_pool)
# ---------------------------------------------------------------------------
# Playwright's sync API is thread-confined, so ALL browser calls run on one
# dedicated worker thread. Other threads submit tasks via a queue and (for
# capture) block on an Event. A keepalive loop refreshes the session while idle
# so it never dies mid-request; on expiry it auto-pushes a QR to the group once,
# then waits for a manual /loginosmwatch (tracked by the needs_manual flag).
class _OsmWatchWarm:
    def __init__(self) -> None:
        self._tasks: queue.Queue[dict] = queue.Queue()
        self._p = None
        self._browser = None
        self._context = None
        self._page = None
        self._login_in_progress = False
        self._started = False
        self._start_lock = threading.Lock()

    # -- lifecycle -----------------------------------------------------------
    def start(self) -> None:
        with self._start_lock:
            if self._started:
                return
            self._started = True
            threading.Thread(target=self._loop, name="osmwatch-warm", daemon=True).start()
            threading.Thread(target=self._keepalive_loop, name="osmwatch-warm-ka", daemon=True).start()

    def _launch(self) -> None:
        from playwright.sync_api import sync_playwright

        self._teardown()
        self._p = sync_playwright().start()
        self._browser, self._context, self._page = _open(self._p, headless=_headless_default())
        print("[osmwatch-warm] browser launched (kept open).", flush=True)

    def _teardown(self) -> None:
        for closer in (
            lambda: self._browser.close() if self._browser else None,
            lambda: self._p.stop() if self._p else None,
        ):
            try:
                closer()
            except Exception:
                pass
        self._p = self._browser = self._context = self._page = None

    def _healthy(self) -> bool:
        try:
            return self._page is not None and not self._page.is_closed()
        except Exception:
            return False

    # -- public submit API (thread-safe) -------------------------------------
    def submit_ensure(self, *, auto: bool = True) -> None:
        self._tasks.put({"kind": "ensure", "auto": auto})

    def request_login(self, chat_id: str | None = None) -> None:
        self._tasks.put({"kind": "login", "chat_id": chat_id})

    def capture(self, *, url: str | None = None, chat_id: str | None = None, timeout_ms: int = 60_000) -> dict:
        done = threading.Event()
        box: dict = {}
        self._tasks.put({
            "kind": "capture", "url": url or OSM_BASE, "chat_id": chat_id,
            "timeout_ms": timeout_ms, "done": done, "box": box,
        })
        done.wait()
        return box

    # -- worker loop ---------------------------------------------------------
    def _loop(self) -> None:
        while True:
            task = self._tasks.get()
            kind = task.get("kind")
            try:
                if kind in ("ensure", "keepalive"):
                    self._handle_ensure(task)
                elif kind == "login":
                    self._handle_login(task)
                elif kind == "capture":
                    self._handle_capture(task)
            except Exception as e:
                print(f"[osmwatch-warm] task {kind} error: {e!r}", flush=True)
                self._teardown()
            finally:
                if task.get("done"):
                    task["done"].set()

    def _keepalive_loop(self) -> None:
        while True:
            time.sleep(_keepalive_sec())
            self._tasks.put({"kind": "keepalive", "auto": True})

    # -- task handlers (worker thread only) ----------------------------------
    def _check_auth(self, timeout_ms: int = 60_000) -> str:
        """Return _classify() verdict after loading the dashboard root."""
        resp = self._page.goto(OSM_BASE, wait_until="domcontentloaded", timeout=timeout_ms)
        _settle_url(self._page, seconds=20)
        return _classify(self._page, resp)

    def _notify_blocked(self, chat_id: str | None) -> None:
        target = chat_id or (_qr_chat_default() or None)
        if not target:
            return
        try:
            send_text_message(
                target,
                "⛔ OSM-Watch: the request was blocked by Cloudflare/WAF (not a login issue). "
                "Allowlist the bot server's IP or add a WAF bypass rule in the osm-watch "
                "Cloudflare dashboard, then try again.",
            )
        except Exception:
            pass

    def _handle_ensure(self, task: dict) -> None:
        if not self._healthy():
            self._launch()
        verdict = self._check_auth()
        if verdict == "authenticated":
            _save_state(self._context)
            _set_needs_manual(False)
            return
        if verdict == "blocked":
            print("[osmwatch-warm] blocked by Cloudflare/WAF — QR won't help; skipping.", flush=True)
            return  # don't QR-spam; a login won't fix a WAF block
        if verdict == "error":
            print("[osmwatch-warm] dashboard returned an error status; will retry next keepalive.", flush=True)
            return
        # verdict == "login" → session expired.
        if task.get("auto") and not _get_needs_manual():
            self._do_qr_login()
        else:
            print("[osmwatch-warm] session expired; waiting for /loginosmwatch", flush=True)

    def _handle_login(self, task: dict) -> None:
        chat_id = task.get("chat_id")
        if self._login_in_progress:
            if chat_id:
                send_text_message(chat_id, "⏳ OSM-Watch login already in progress — check the group for the QR.")
            return
        if not self._healthy():
            self._launch()
        verdict = self._check_auth()
        if verdict == "authenticated":
            _save_state(self._context)
            _set_needs_manual(False)
            if chat_id:
                send_text_message(chat_id, "✅ OSM-Watch is already logged in.")
            return
        if verdict == "blocked":
            self._notify_blocked(chat_id)
            return
        _set_needs_manual(False)  # forced fresh attempt
        self._do_qr_login(ack_chat=chat_id)

    def _do_qr_login(self, *, ack_chat: str | None = None) -> bool:
        qr_chat = _qr_chat_default() or None
        self._login_in_progress = True
        try:
            if ack_chat and ack_chat != qr_chat and qr_chat:
                try:
                    send_text_message(ack_chat, "📨 Sending a fresh OSM-Watch login QR to the group…")
                except Exception:
                    pass
            ok = _qr_login_on_page(self._page, qr_chat_id=qr_chat, timeout_s=_login_timeout_s())
            if ok:
                try:
                    self._page.goto(OSM_BASE, wait_until="domcontentloaded", timeout=60_000)
                    _settle_url(self._page, seconds=15)
                except Exception:
                    pass
                _save_state(self._context)
                _set_needs_manual(False)
                if qr_chat:
                    send_text_message(qr_chat, "✅ OSM-Watch: bot logged in successfully.")
                return True
            _set_needs_manual(True)
            if qr_chat:
                send_text_message(
                    qr_chat,
                    "⚠️ OSM-Watch login QR expired (not scanned in time). "
                    "Tag me and send /loginosmwatch to get a fresh QR.",
                )
            return False
        finally:
            self._login_in_progress = False

    def _handle_capture(self, task: dict) -> None:
        box = task["box"]
        chat_id = task.get("chat_id")
        timeout_ms = int(task.get("timeout_ms") or 60_000)
        try:
            if not self._healthy():
                self._launch()
            verdict = self._check_auth(timeout_ms)
            if verdict == "blocked":
                box["error"] = "blocked"
                self._notify_blocked(chat_id)
                return
            if verdict != "authenticated":
                if verdict == "login" and not _get_needs_manual():
                    self._do_qr_login()  # one auto attempt
                    verdict = self._check_auth(timeout_ms)
                if verdict != "authenticated":
                    box["error"] = "blocked" if verdict == "blocked" else "not_authenticated"
                    if verdict == "blocked":
                        self._notify_blocked(chat_id)
                    elif chat_id:
                        send_text_message(
                            chat_id,
                            "⚠️ OSM-Watch: not logged in. Tag me and send /loginosmwatch to sign in.",
                        )
                    return
            url = task.get("url") or OSM_BASE
            if self._page.url.rstrip("/") != url.rstrip("/"):
                resp = self._page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                _settle_url(self._page, seconds=15)
                v2 = _classify(self._page, resp)
                if v2 != "authenticated":
                    box["error"] = v2
                    if v2 == "blocked":
                        self._notify_blocked(chat_id)
                    return
            try:
                self._page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass
            _save_state(self._context)
            out = str(DEFAULT_SHOT)
            self._page.screenshot(path=out, full_page=True)
            box["path"] = out
            if chat_id:
                send_screenshot_to_lark(out, chat_id)
        except Exception as e:
            box["error"] = repr(e)
            self._teardown()


_warm_singleton: _OsmWatchWarm | None = None
_warm_lock = threading.Lock()


def warm() -> _OsmWatchWarm:
    global _warm_singleton
    with _warm_lock:
        if _warm_singleton is None:
            _warm_singleton = _OsmWatchWarm()
        return _warm_singleton


def prewarm_osmwatch_on_startup() -> None:
    """Called from main.py at boot: launch the warm browser + verify the session."""
    if not _warm_enabled():
        print("[osmwatch-warm] disabled (OSMWATCH_WARM=0)", flush=True)
        return
    w = warm()
    w.start()
    w.submit_ensure(auto=True)
    print("[osmwatch-warm] startup pre-warm submitted", flush=True)


def request_login(chat_id: str | None = None) -> None:
    """`/loginosmwatch` entry point — force a fresh QR to the group."""
    w = warm()
    w.start()
    w.request_login(chat_id)


def capture_and_send(chat_id: str | None = None, url: str | None = None) -> dict:
    """`/osmwatch` entry point — screenshot the dashboard and send it to ``chat_id``."""
    w = warm()
    w.start()
    return w.capture(url=url, chat_id=chat_id)


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
