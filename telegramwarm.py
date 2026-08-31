#!/usr/bin/env python3
"""Telegram Web warm browser — QR login, live screenshots, new-message detection.

The whole Telegram integration lives here. One long-lived Chromium holds a
logged-in Telegram Web session, kept warm exactly the way ``osmwatch.py`` keeps
the OSM-Watch dashboard warm: a single worker thread owns the browser and other
threads hand it work through a queue (Playwright's sync API is not thread-safe).

Why the browser rather than MTProto/Telethon: Telegram Web logs in by **QR code**,
scanned with the phone. No ``my.telegram.org`` app registration, no ``api_id``,
and no one-time code that has to be typed on the server — the bot screenshots the
QR into the Lark group and you scan it, which is the ``/loginosmwatch`` flow you
already use.

Session persistence differs from osmwatch on purpose: Telegram Web keeps its auth
key in localStorage *and* IndexedDB, and Playwright's ``storage_state`` does not
round-trip IndexedDB. So this uses a **persistent browser profile**
(``browser_data/telegram_profile``), which keeps everything.

READ-ONLY: nothing here ever types into a Telegram message box. The only field it
will ever fill is the two-step-verification prompt, and only when
``TELEGRAM_2FA_PASSWORD`` is set — see ``_maybe_fill_2fa``.

Two login routes, both ending at the same session:
  * **QR** — a QR is posted to Lark; scan it with the phone. No secrets anywhere.
  * **phone + code** — the bot types the number from ``TELEGRAM_PHONE``, Telegram
    sends a one-time code, and a human relays it with ``/telegramcode 12345``.
Either way, if the account has two-step verification the cloud password is read
from ``TELEGRAM_2FA_PASSWORD``; it is never typed by hand or written to disk.

CLI:
    python telegramwarm.py --login       # post a fresh QR to Lark, wait for the scan
    python telegramwarm.py --login-code  # phone-number login, prompts via Lark
    python telegramwarm.py --code 12345  # relay the one-time code
    python telegramwarm.py --shot        # screenshot Telegram Web to a file
    python telegramwarm.py --probe       # dump the raw DOM verdict (selector debugging)
    python telegramwarm.py --chats       # scrape the chat list once

Exposed to main.py:
    prewarm_telegram_on_startup()   -> boot hook
    request_login(chat_id)          -> /logintelegram
    request_code_login(chat_id)     -> /logintelegram code
    submit_login_code(code, chat)   -> /telegramcode <code>
    send_status_to_lark(chat_id)    -> /telegramstatus (status text + real screenshot)
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import queue
import shutil
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

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

#: Persistent Chromium profile — cookies + localStorage + IndexedDB. This IS the
#: Telegram credential once the QR is scanned. Under the already-gitignored
#: browser_data/; worth backing up so you don't have to re-scan.
PROFILE_DIR = _ROOT_DIR / "browser_data" / os.getenv(
    "TELEGRAM_PROFILE_DIR", "telegram_profile"
)
QR_PNG = _ROOT_DIR / "telegram_qr.png"
SHOT_PNG = _ROOT_DIR / "telegram_web.png"
_LOGIN_STATE = _ROOT_DIR / "browser_data" / "telegram_login.json"
CHATS_JSON = _ROOT_DIR / "telegram_chats.json"

#: Web K ("/k/") has a lighter, stabler DOM than the /a/ client. Override if
#: Telegram reshuffles their clients again.
WEB_URL = os.getenv("TELEGRAM_WEB_URL", "https://web.telegram.org/k/").rstrip("/") + "/"

#: Lark group that hears about login failures, and the person @-mentioned there.
ALERT_CHAT_ID = os.getenv(
    "TELEGRAM_ALERT_CHAT_ID", "oc_ad9b5bdbb2826ba2ee9730920ef25432"
).strip()
ALERT_OPEN_ID = os.getenv(
    "TELEGRAM_ALERT_OPEN_ID", "ou_5f660c0fb0769d184aca635d02209272"
).strip()

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def _truthy(val: str | None) -> bool:
    return str(val or "").strip().lower() in ("1", "true", "yes", "on", "y")


def _warm_enabled() -> bool:
    """Opt-in. Unset means the browser stays down, so deploying alone is inert."""
    return _truthy(os.getenv("TELEGRAM_WARM_ENABLED"))


def _headless_default() -> bool:
    v = os.getenv("BOT_PLAYWRIGHT_HEADLESS")
    return True if v is None else _truthy(v)


def _qr_chat_default() -> str:
    return (os.getenv("TELEGRAM_QR_CHAT_ID", "").strip() or ALERT_CHAT_ID).strip()


def _forward_chat() -> str:
    return (os.getenv("TELEGRAM_FORWARD_CHAT_ID", "").strip() or ALERT_CHAT_ID).strip()


def _keepalive_sec() -> int:
    try:
        return max(120, int(os.getenv("TELEGRAM_KEEPALIVE_SEC", "1800")))
    except ValueError:
        return 1800


def _login_timeout_s() -> int:
    try:
        return max(30, int(os.getenv("TELEGRAM_LOGIN_TIMEOUT", "300")))
    except ValueError:
        return 300


def _code_wait_s() -> int:
    """How long to wait for Telegram's sendCode round trip after pressing NEXT.

    Generous by default: the browser has to complete an MTProto handshake with a
    data centre, and from a server IP that has never talked to Telegram before,
    30s was not enough in practice.
    """
    try:
        return max(20, int(os.getenv("TELEGRAM_CODE_WAIT_SEC", "120")))
    except ValueError:
        return 120


def _chat_poll_sec() -> int:
    try:
        return max(30, int(os.getenv("TELEGRAM_CHAT_POLL_SEC", "120")))
    except ValueError:
        return 120


def _chat_poll_enabled() -> bool:
    """New-message detection is the point of this module, so it defaults ON."""
    v = os.getenv("TELEGRAM_CHAT_POLL_ENABLED")
    return True if v is None else _truthy(v)


def _watch_targets() -> list[str]:
    """Chat titles to report on (substring, case-insensitive). Empty = all chats."""
    raw = os.getenv("TELEGRAM_WATCH_CHATS", "") or ""
    return [t.strip().lower() for t in raw.replace(";", ",").split(",") if t.strip()]


def _tz():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(os.getenv("TELEGRAM_WATCH_TZ", "Asia/Shanghai"))
    except Exception:
        return timezone.utc


def _now_str() -> str:
    return datetime.now(_tz()).strftime("%Y-%m-%d %H:%M:%S")


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
    result = requests.post(
        f"{_lark_base()}/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=30,
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


def _send_shot(chat_id: str, path: str) -> bool:
    try:
        key = upload_image_lark(path)
        if not key:
            return False
        return send_image(chat_id, key).get("code") == 0
    except Exception as err:
        print(f"[tg-warm] screenshot send failed: {err!r}", flush=True)
        return False


# ---------------------------------------------------------------------------
# Shared status (worker writes, /telegramstatus reads)
# ---------------------------------------------------------------------------
_state_lock = threading.Lock()
_warm_state: dict[str, Any] = {
    "phase": "idle",       # idle | launching | login | password | authenticated | error
    "detail": "not started",
    "last_verdict": None,
    "last_check": None,
    "last_error": None,
    "last_shot": None,
    "logged_in_since": None,
    "chats_seen": 0,
    "new_previews": 0,
    "last_activity": None,
    "alerted": False,      # de-dupe the login-failed alert
}


def _wset(**kw: Any) -> None:
    with _state_lock:
        _warm_state.update(kw)


def warm_snapshot() -> dict[str, Any]:
    with _state_lock:
        return dict(_warm_state)


def is_authenticated() -> bool:
    with _state_lock:
        return _warm_state["phase"] == "authenticated"


def is_monitoring() -> bool:
    """'Still monitoring' == logged in with the chat-list poller running."""
    return is_authenticated() and _chat_poll_enabled()


_PHASE_EMOJI = {
    "authenticated": "🟢",
    "login": "🔴",
    "password": "🟠",
    "launching": "🟡",
    "error": "🔴",
    "idle": "⚪",
}


def status_lines() -> list[str]:
    snap = warm_snapshot()
    phase = snap["phase"]
    monitoring = is_monitoring()
    lines = [
        f"{_PHASE_EMOJI.get(phase, '⚪')} Telegram watcher: "
        f"{'MONITORING' if monitoring else phase.upper()}",
        f"• Detail: {snap['detail']}",
        f"• Profile: browser_data/{PROFILE_DIR.name} "
        f"({'present' if PROFILE_DIR.exists() else 'MISSING — needs /logintelegram'})",
        f"• Client: {WEB_URL}",
    ]
    if snap["logged_in_since"]:
        lines.append(f"• Logged in since: {snap['logged_in_since']}")
    if snap["last_check"]:
        lines.append(f"• Last check: {snap['last_check']} (page={snap['last_verdict']})")

    targets = _watch_targets()
    lines.append(
        "• Watching: " + (", ".join(targets) if targets else "ALL chats in the sidebar")
    )
    if _chat_poll_enabled():
        lines.append(
            f"• New-message poll: every {_chat_poll_sec()}s → {_forward_chat()}"
        )
        lines.append(
            f"• Chats in sidebar: {snap['chats_seen']}  |  "
            f"new activity seen: {snap['new_previews']}"
        )
        lines.append(f"• Last activity: {snap['last_activity'] or '—'}")
    else:
        lines.append("• New-message poll: disabled (TELEGRAM_CHAT_POLL_ENABLED=0)")
    if snap["last_shot"]:
        lines.append(f"• Last screenshot: {snap['last_shot']}")
    if snap["last_error"]:
        lines.append(f"• Last error: {snap['last_error']}")
    lines.append("")
    lines.append(f"Snapshot taken {_now_str()}")
    return lines


def _alert_login_failed(reason: str, *, force: bool = False) -> None:
    """Post the login-failure notice to the Lark group, @-mentioning the owner.

    De-duped by default: the keepalive loop must not re-post the same failure
    every 30 minutes. ``force=True`` for a fresh, distinct failure.
    """
    with _state_lock:
        if _warm_state["alerted"] and not force:
            return
        _warm_state["alerted"] = True
    # Plain-text mention markup is `<at user_id="ou_…">…</at>` (card markup would
    # be `<at id=ou_…></at>` — a different thing, see maintenance.py:3082).
    mention = f'<at user_id="{ALERT_OPEN_ID}"></at> ' if ALERT_OPEN_ID else ""
    text = (
        f"{mention}❌ Telegram watcher: LOGIN FAILED\n"
        f"• Reason: {reason}\n"
        f"• Profile: browser_data/{PROFILE_DIR.name}"
        f" ({'present' if PROFILE_DIR.exists() else 'MISSING'})\n"
        f"• Client: {WEB_URL}\n"
        f"• Time: {_now_str()}\n"
        f"Fix: run /logintelegram — the bot posts a QR here, scan it with "
        f"Telegram on your phone (Settings → Devices → Link Desktop Device)."
    )
    try:
        resp = send_text(ALERT_CHAT_ID, text)
        if resp.get("code") != 0:
            print(f"❌ tg login-fail alert rejected: {resp}", flush=True)
    except Exception as err:
        print(f"❌ tg login-fail alert failed: {err!r}", flush=True)


# --- "waiting for a manual /logintelegram" flag (survives restarts) -----------
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


# ---------------------------------------------------------------------------
# DOM probing
# ---------------------------------------------------------------------------
#: Telegram Web K marks the active screen with an id on a wrapper div. Rather than
#: betting on one selector, collect every signal and let _classify() decide — this
#: is the part most likely to rot when Telegram ships a redesign, so `--probe`
#: prints the raw dict.
_PROBE_JS = """
() => {
  const vis = (el) => {
    if (!el) return false;
    const r = el.getBoundingClientRect();
    if (r.width <= 0 || r.height <= 0) return false;
    const cs = getComputedStyle(el);
    return cs.visibility !== 'hidden' && cs.display !== 'none' && cs.opacity !== '0';
  };
  const one = (s) => document.querySelector(s);
  const visOne = (s) => vis(one(s));
  const anyVis = (s) => Array.from(document.querySelectorAll(s)).some(vis);

  // A roughly-square visible canvas is the login QR.
  let qr = false;
  for (const c of document.querySelectorAll('canvas')) {
    if (!vis(c)) continue;
    const r = c.getBoundingClientRect();
    if (r.width >= 120 && r.width <= 460 && Math.abs(r.width - r.height) < 40) { qr = true; break; }
  }

  return {
    url: location.href,
    // Web K pre-mounts BOTH #auth-pages and #page-chats, so mere existence proves
    // nothing — only visibility tells you which screen the user is looking at.
    authVisible: visOne('#auth-pages'),
    chatsVisible: visOne('#page-chats'),
    qrCanvas: qr,
    // Web K renders EVERY field as <div class=input-field-input contenteditable>;
    // there is not one <input> on the auth screens, so field-type checks (e.g.
    // input[type=password]) can never match. Steps are told apart by the visible
    // copy instead, which is why authLines is collected.
    authLines: (() => {
      const a = one('#auth-pages');
      if (!a) return [];
      return (a.innerText || '').split(String.fromCharCode(10))
               .map(s => s.trim()).filter(Boolean).slice(0, 14);
    })(),
    editables: document.querySelectorAll('.input-field-input[contenteditable=true]').length,
    hasNext: Array.from(document.querySelectorAll('button')).some(
      b => vis(b) && /next/i.test(b.innerText || '')),
    // The primary button doubles as a progress indicator: after NEXT is pressed it
    // becomes "PLEASE WAIT..." and stays that way until Telegram answers sendCode.
    // Without reading it, a slow-but-healthy request is indistinguishable from a
    // stuck one.
    primaryBtn: (() => {
      const b = one('button.btn-primary:not(.btn-secondary)');
      if (!b) return null;
      return { text: (b.innerText || '').trim(), disabled: !!b.disabled };
    })(),
    // Telegram surfaces refusals ("Invalid phone number", flood waits) as toasts
    // or inline label errors rather than changing screen.
    errorText: (() => {
      const hits = [];
      for (const el of document.querySelectorAll(
             '[class*=error], .toast, [class*=Toast], .popup-title, .popup-description')) {
        if (!vis(el)) continue;
        const t = (el.innerText || '').trim();
        if (t) hits.push(t.slice(0, 160));
      }
      return hits.slice(0, 4);
    })(),
    chatItems: document.querySelectorAll('.chatlist-chat, ul.chatlist > li').length,
    chatListVisible: anyVis('.chatlist'),
    sidebarVisible: visOne('#column-left'),
    // Kept for --probe debugging: existence, so a redesign is easy to spot.
    exists: {
      authRoot: !!one('#auth-pages'),
      chatsPage: !!one('#page-chats'),
      qrPage: !!one('.page-signQR'),
      signPage: !!one('.page-sign'),
      passwordPage: !!one('.page-password'),
    },
    pageIds: Array.from(document.querySelectorAll('[id^=page-], #auth-pages'))
                  .map(e => e.id).slice(0, 8),
  };
}
"""


def _probe(page) -> dict:
    try:
        return page.evaluate(_PROBE_JS) or {}
    except Exception as err:
        return {"error": repr(err)}


def _classify(probe: dict) -> str:
    """authenticated | password | login | unknown.

    Order matters. Web K keeps ``#auth-pages`` and ``#page-chats`` both mounted, and
    the password step keeps the auth root mounted too, so this tests *visibility*
    and checks the most specific screen first. Getting this wrong is not harmless:
    an over-eager 'authenticated' makes the watcher report MONITORING while it
    stares at a login QR, and it never asks anyone to scan.
    """
    if probe.get("error"):
        return "unknown"
    step = _auth_step(probe)
    if step == "password":
        return "password"
    if step != "none":
        # Any auth screen on top — QR, phone, code, or something unrecognised.
        return "login"
    # Auth screen gone: this is the real client.
    if probe.get("chatsVisible") or probe.get("chatItems", 0) > 0 or (
        probe.get("chatListVisible") and probe.get("sidebarVisible")
    ):
        return "authenticated"
    return "unknown"


#: Which auth step is on screen, keyed off the visible copy. Order below matters:
#: the QR screen's own footer says "LOG IN BY PHONE NUMBER", so 'phone' must be
#: tested after the QR canvas, or every QR screen would read as the phone form.
_STEP_WORDS = {
    "password": ("password", "two-step", "two step"),
    "code": ("we've sent", "we have sent", "sent the code", "enter code",
             "check your telegram", "type it below", "sent you a message",
             "code we sent"),
    # Only the distinctive phrase. A bare "phone number" would also match the QR
    # screen's footer AND the code screen (which offers to correct the number),
    # so it cannot identify the phone form on its own.
    "phone": ("confirm your country",),
}


def _auth_step(probe: dict) -> str:
    """qr | phone | code | password | unknown_step | none."""
    if not probe.get("authVisible"):
        return "none"
    text = " ".join(probe.get("authLines") or []).lower()
    if any(w in text for w in _STEP_WORDS["password"]):
        return "password"
    # The code step is checked BEFORE the QR step on purpose. Telegram draws the
    # monkey sticker on the code screen into a square <canvas>, which is exactly what
    # the qrCanvas heuristic looks for — so a canvas alone cannot mean "QR screen".
    if any(w in text for w in _STEP_WORDS["code"]):
        return "code"
    # A QR screen needs the canvas AND the copy that only it carries.
    if probe.get("qrCanvas") and any(
        w in text for w in ("qr code", "scan with", "point your phone")
    ):
        return "qr"
    editables = probe.get("editables", 0) or 0
    # The phone form is the only auth screen carrying a country selector, so it is
    # the one identified positively — two fields plus the word "country".
    if any(w in text for w in _STEP_WORDS["phone"]) or (editables >= 2 and "country" in text):
        return "phone"
    # Structural fallback: an auth screen with a field that is not the QR, the
    # password or the phone form is the code step. Matching the copy alone is too
    # brittle — Telegram headlines that screen with the phone NUMBER and rewords it
    # per delivery method (app / SMS / call), so unseen wording must not read as a
    # failure when the code box is plainly sitting there.
    if editables >= 1:
        return "code"
    return "unknown_step"


#: Inventory of everything on the auth screen that could accept text. The 2FA screen
#: cannot be reached without a real login, so when a fill fails there this is shipped
#: to Lark — one round then yields the exact selector instead of another guess.
_FIELD_INVENTORY_JS = """
() => {
  const vis = (el) => {
    const r = el.getBoundingClientRect();
    return r.width > 0 && r.height > 0 && getComputedStyle(el).display !== 'none';
  };
  const desc = (e) => ({
    tag: e.tagName.toLowerCase(),
    type: e.getAttribute('type'),
    cls: (e.className || '').toString().slice(0, 70),
    editable: e.getAttribute('contenteditable'),
    visible: vis(e),
    inAuth: !!e.closest('#auth-pages'),
  });
  const out = [];
  for (const el of document.querySelectorAll(
         'input, textarea, [contenteditable], [class*=input-field]')) {
    out.push(desc(el));
  }
  return out.slice(0, 20);
}
"""


def _field_inventory(page) -> list[dict]:
    try:
        return page.evaluate(_FIELD_INVENTORY_JS) or []
    except Exception as err:
        return [{"error": repr(err)}]


def _fill_code(page, code: str, *, log=print) -> bool:
    """Type the login code digit by digit.

    The code screen is five separate single-character cells and Telegram moves focus
    to the next one itself as each is filled. ``locator.fill()`` would drop the whole
    string into the first cell and never trigger that advance, so real keystrokes
    with a pause between them are required here — this must not be routed through
    ``_fill_auth_field``, which prefers ``fill()``.
    """
    target = None
    for sel in (".input-field-input[contenteditable=true]",
                "input:not([type=hidden])",
                ".input-field-input"):
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                target = el
                break
        except Exception:
            continue
    if target is None:
        log("[tg-warm] no code field found on the page")
        return False
    try:
        target.click()
        for ch in code:
            page.keyboard.type(ch)
            page.wait_for_timeout(150)   # give Telegram time to advance the caret
        return True
    except Exception as err:
        log(f"[tg-warm] code fill failed: {err!r}")
        return False


def _fill_auth_field(page, value: str, *, log=print) -> bool:
    """Type ``value`` into the focused auth field.

    Telegram Web K builds every field as ``div.input-field-input[contenteditable]``
    — there is no ``<input>`` on the auth screens at all — so ``locator.fill()``
    does not apply. Real keystrokes are used instead. A genuine ``<input>`` is
    still tried first in case Telegram ever switches back.
    """
    # Every selector is scoped to #auth-pages and takes the first VISIBLE match rather
    # than merely the first match. Scoping is defensive, not a known fix: #page-chats
    # is pre-mounted ahead of #auth-pages in document order, and though it holds no
    # fields on the screens observed so far, an unscoped "first input" would silently
    # prefer it the moment it does. The visible-match loop is the substantive part —
    # query_selector() alone gives up alone if the first hit happens to be hidden.
    for sel in (
        "#auth-pages input[type=password]",
        "#auth-pages .input-field-phone .input-field-input[contenteditable=true]",
        "#auth-pages input:not([type=hidden])",
        "#auth-pages .input-field-input[contenteditable=true]",
        "#auth-pages [contenteditable=true]",
    ):
        try:
            for el in page.query_selector_all(sel):
                if not el.is_visible():
                    continue
                el.click()
                tag = (el.evaluate("e => e.tagName.toLowerCase()") or "").strip()
                if tag == "input":
                    el.fill(value)          # replaces any leftover text
                else:
                    page.keyboard.press("Control+A")
                    page.keyboard.press("Delete")
                    page.keyboard.type(value, delay=45)
                # Never log the value itself — only its length.
                log(f"[tg-warm] filled {sel} <{tag}> ({len(value)} chars)")
                return True
        except Exception:
            continue
    log("[tg-warm] no visible auth field matched any selector")
    return False


def _click_next(page) -> bool:
    """Press the primary NEXT button. The secondary buttons on the same screen
    ('LOG IN BY QR CODE', 'LOG IN WITH PASSKEY') also carry .btn-primary, so they
    are excluded by :not(.btn-secondary)."""
    for attempt in (
        lambda: page.get_by_role("button", name="Next", exact=False).first.click(timeout=4000),
        lambda: page.click("button.btn-primary:not(.btn-secondary)", timeout=4000),
        lambda: page.keyboard.press("Enter"),
    ):
        try:
            attempt()
            return True
        except Exception:
            continue
    return False


def _find_qr_element(page, *, tries: int = 8):
    """The visible, roughly-square QR canvas, or None. Same heuristic as osmwatch."""
    for _ in range(max(1, tries)):
        for el in page.query_selector_all("canvas, img[src*='qr'], img[alt*='QR']"):
            try:
                if not el.is_visible():
                    continue
                b = el.bounding_box()
                if not b:
                    continue
                w, h = b.get("width", 0), b.get("height", 0)
                if 120 <= w <= 460 and 120 <= h <= 460 and 0.8 <= (w / max(h, 1)) <= 1.25:
                    return el
            except Exception:
                continue
        page.wait_for_timeout(700)
    return None


def _reveal_qr(page) -> None:
    """Telegram Web sometimes opens on the phone-number form; a link switches to QR."""
    for label in ("Log in by QR Code", "QR Code", "Quick log in", "通过二维码登录"):
        try:
            loc = page.get_by_text(label, exact=False)
            if loc.count() and loc.first.is_visible():
                loc.first.click(timeout=3000)
                page.wait_for_timeout(1500)
                return
        except Exception:
            continue


def _capture_qr(page, out_path: Path) -> Path:
    """Tight crop of the QR; whole viewport if the canvas can't be isolated."""
    el = _find_qr_element(page, tries=2)
    if el is None:
        _reveal_qr(page)
        el = _find_qr_element(page, tries=8)
    if el is not None:
        try:
            # Pad the crop: phone cameras lock on faster with a quiet zone.
            box = el.bounding_box() or {}
            pad = 18
            page.screenshot(
                path=str(out_path),
                clip={
                    "x": max(0, box.get("x", 0) - pad),
                    "y": max(0, box.get("y", 0) - pad),
                    "width": box.get("width", 260) + pad * 2,
                    "height": box.get("height", 260) + pad * 2,
                },
            )
            return out_path
        except Exception:
            pass
    page.screenshot(path=str(out_path))
    return out_path


def _phone_number() -> str:
    """The login number, always in +<country><national> form.

    Typing the leading ``+`` matters: it makes Telegram Web pick the country
    itself. Relying on the pre-selected country is not safe, because that comes
    from the *server's* geolocation, which need not be Malaysia.

    TELEGRAM_PHONE must carry the country code (``60102693549`` or
    ``+60102693549``). A local-format number with a leading zero is passed through
    with a ``+`` bolted on so Telegram rejects it loudly — guessing the country
    code from a leading zero would silently dial the wrong number instead.
    """
    raw = (os.getenv("TELEGRAM_PHONE", "") or "").strip()
    for junk in (" ", "-", "(", ")"):
        raw = raw.replace(junk, "")
    if not raw:
        return ""
    return raw if raw.startswith("+") else "+" + raw


def _switch_to_phone_login(page, *, log=print) -> bool:
    """From the QR screen, open the phone-number form."""
    for label in ("Log in by phone Number", "log in by phone number", "phone number"):
        try:
            loc = page.get_by_text(label, exact=False)
            if loc.count() and loc.first.is_visible():
                loc.first.click(timeout=4000)
                page.wait_for_timeout(2500)
                return True
        except Exception:
            continue
    log("[tg-warm] could not find the 'LOG IN BY PHONE NUMBER' link")
    return False


#: The pencil beside the phone number on the code screen, which returns to the phone
#: form. Located by its icon glyph (U+E977, seen in authLines) rather than a class
#: name, since Telegram's icon-font classes are opaque and change.
_EDIT_NUMBER_JS = """
() => {
  const root = document.querySelector('#auth-pages');
  if (!root) return false;
  const pencil = String.fromCharCode(0xE977);
  for (const el of root.querySelectorAll('*')) {
    if (el.children.length === 0 && (el.textContent || '').includes(pencil)) {
      (el.closest('button, [role=button], span, div') || el).click();
      return true;
    }
  }
  return false;
}
"""


def _back_to_phone_form(page, *, log=print) -> bool:
    """From the code screen, reopen the phone form so a NEW code can be requested.

    Telegram restores the pending code screen after a page reload, so without this
    ``/logintelegram code`` would hand back a stale code instead of sending a fresh
    one. Best-effort: the caller falls back to using the pending code if this fails.
    """
    try:
        if not page.evaluate(_EDIT_NUMBER_JS):
            log("[tg-warm] edit-number pencil not found on the code screen")
            return False
        page.wait_for_timeout(2500)
        return _auth_step(_probe(page)) == "phone"
    except Exception as err:
        log(f"[tg-warm] edit-number click failed: {err!r}")
        return False


def _start_code_login(page, *, log=print) -> tuple[str, dict]:
    """Enter the phone number and ask Telegram to send a login code.

    Returns ``(step, probe)`` where step is the auth step now on screen —
    'code' on success, 'password' if 2FA came first, anything else is a failure
    the caller reports with a screenshot.
    """
    step = _auth_step(_probe(page))
    if step == "qr":
        _switch_to_phone_login(page, log=log)
        step = _auth_step(_probe(page))
    elif step == "code":
        # Telegram resumed a pending code screen after the reload. Go back to the
        # phone form so this really does send a fresh code; if the pencil cannot be
        # found, keep the pending one rather than dead-ending.
        log("[tg-warm] code screen already pending — reopening the phone form")
        if _back_to_phone_form(page, log=log):
            step = "phone"
        else:
            # Distinct from "code": NEXT was never pressed, so NOTHING was sent just
            # now. Reporting this as a fresh send is what made the bot claim a code
            # had arrived when none had.
            log("[tg-warm] keeping the pending code request (no new code sent)")
            return "code_pending", _probe(page)
    if step != "phone":
        log(f"[tg-warm] expected the phone form, got step={step}")
        return step, _probe(page)

    phone = _phone_number()
    if not phone:
        log("[tg-warm] TELEGRAM_PHONE is not set")
        return "no_phone", _probe(page)

    if not _fill_auth_field(page, phone, log=log):
        return "fill_failed", _probe(page)
    log(f"[tg-warm] phone entered ({phone[:4]}…{phone[-3:]}), requesting the code")
    if not _click_next(page):
        return "next_failed", _probe(page)

    # Telegram has to reach a data centre before it can swap in the code field, and
    # from a fresh server IP that handshake can take a while. The primary button
    # reads "PLEASE WAIT..." for the whole round trip, so treat that as healthy
    # progress and only give up once it stops saying so (or the budget runs out).
    deadline = time.time() + _code_wait_s()
    last_probe: dict = {}
    while time.time() < deadline:
        page.wait_for_timeout(1500)
        probe = last_probe = _probe(page)
        step = _auth_step(probe)
        if step in ("code", "password"):
            return step, probe
        if step == "none":  # already signed in somehow
            return "none", probe
        errs = probe.get("errorText") or []
        if errs:
            log(f"[tg-warm] Telegram refused the number: {errs}")
            return "refused", probe
        btn = (probe.get("primaryBtn") or {}).get("text", "")
        if "wait" in btn.lower():
            continue  # sendCode still in flight
    # Still on the phone form after the full budget.
    btn = ((last_probe or {}).get("primaryBtn") or {}).get("text", "")
    if "wait" in btn.lower():
        return "sendcode_timeout", last_probe or _probe(page)
    return _auth_step(last_probe or _probe(page)), last_probe or _probe(page)


def _submit_code(page, code: str, *, log=print) -> tuple[str, dict]:
    """Type the login code, then settle onto the next step.

    Returns ``(verdict, probe)`` with verdict from _classify: 'authenticated',
    'password' (2FA needed and unconfigured), or 'login' (code refused).
    """
    step = _auth_step(_probe(page))
    if step != "code":
        log(f"[tg-warm] not on the code step (step={step}); cannot submit")
        return _classify(_probe(page)), _probe(page)

    if not _fill_code(page, code, log=log):
        return "login", _probe(page)
    # Web K submits on its own once the digit count matches; NEXT is a no-op then,
    # so a failure to find it is not an error.
    _click_next(page)

    deadline = time.time() + 40
    verdict = "login"
    tried_2fa = False
    while time.time() < deadline:
        page.wait_for_timeout(1500)
        probe = _probe(page)
        verdict = _classify(probe)
        if verdict == "authenticated":
            return verdict, probe
        if verdict == "password":
            # ONCE only. A wrong password leaves the verdict at 'password', so
            # retrying inside this loop would fire the same bad value every few
            # seconds and get the account flood-limited by Telegram.
            if tried_2fa:
                log("[tg-warm] still on the password screen — the password was refused")
                return "password_refused", probe
            tried_2fa = True
            fill_status = _maybe_fill_2fa(page, log=log)
            if fill_status == "ok":
                continue          # submitted; keep waiting for the client to load
            return "password", probe
    return verdict, _probe(page)


def _secret_from_env_file(key: str) -> str:
    """Read one key straight out of .env.

    ``load_dotenv()`` runs once at import, so a value added to .env after the bot
    started is invisible to the process until a restart — which is a poor trade for
    a value only needed at the moment a login is in progress. This re-reads the file
    on demand. The value is returned to the caller and never logged.
    """
    try:
        for raw in _ENV_PATH.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, _, val = line.partition("=")
            if name.strip() != key:
                continue
            val = val.strip()
            # Strip one layer of matching quotes; systemd and dotenv both allow them.
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            if val:
                return val            # keep scanning only if empty; last wins otherwise
    except Exception as err:
        print(f"[tg-warm] could not read {_ENV_PATH.name}: {err!r}", flush=True)
    return ""


def _2fa_password() -> tuple[str, str]:
    """(password, source) — process env first, then a live re-read of .env."""
    pwd = os.getenv("TELEGRAM_2FA_PASSWORD", "")
    if pwd:
        return pwd, "process env"
    pwd = _secret_from_env_file("TELEGRAM_2FA_PASSWORD")
    if pwd:
        return pwd, ".env (re-read; the running process had it unset)"
    return "", "not found"


def _maybe_fill_2fa(page, *, log=print) -> str:
    """Fill the two-step-verification password, only if one is configured.

    Telegram asks for the cloud password *after* the QR is scanned when 2FA is on.
    Without ``TELEGRAM_2FA_PASSWORD`` there is nothing this process can do, so the
    caller screenshots the prompt to Lark and lets a human finish. The value is
    read straight from the environment and never logged or persisted.
    """
    pwd, source = _2fa_password()
    if not pwd:
        log("[tg-warm] 2FA prompt reached but TELEGRAM_2FA_PASSWORD is unset "
            "(checked the process env AND .env)")
        return "no_password"
    log(f"[tg-warm] filling the 2FA password from {source}")
    try:
        # _fill_auth_field clears the box after focusing it (fill() replaces, and the
        # contenteditable path does Ctrl+A/Delete), so leftover text from a previous
        # failed attempt is not appended to.
        if not _fill_auth_field(page, pwd, log=log):
            # A distinct outcome from "no_password": the value exists but the field
            # could not be found. Reporting both as "no password" sent the last
            # debugging round chasing .env when the real fault was the selector.
            return "fill_failed"
        _click_next(page)
        page.wait_for_timeout(4000)
        log("[tg-warm] submitted two-step verification password")
        return "ok"
    except Exception as err:
        log(f"[tg-warm] 2FA fill failed: {err!r}")
        return "error"


# ---------------------------------------------------------------------------
# Chat-list scrape — the new-message signal
# ---------------------------------------------------------------------------
_CHATS_JS = """
() => {
  const items = document.querySelectorAll('.chatlist-chat, ul.chatlist > li');
  const out = [];
  for (const li of items) {
    const t = li.querySelector('.user-title, .peer-title, .dialog-title');
    const s = li.querySelector('.row-subtitle, .dialog-subtitle, .subtitle');
    const b = li.querySelector('.badge-unread, .dialog-subtitle-badge, .badge');
    const title = (t ? t.innerText : '').trim();
    if (!title) continue;
    out.push({
      title,
      preview: (s ? s.innerText : '').trim().slice(0, 300),
      unread: (b ? parseInt((b.innerText || '0').replace(/\\D/g, ''), 10) : 0) || 0,
    });
  }
  return out.slice(0, 60);
}
"""


def _preview_key(row: dict) -> str:
    raw = f"{row.get('title', '')}|{row.get('preview', '')}"
    return hashlib.sha1(raw.encode("utf-8", "replace")).hexdigest()[:16]


def _wanted(row: dict) -> bool:
    targets = _watch_targets()
    if not targets:
        return True
    title = (row.get("title") or "").lower()
    return any(t in title for t in targets)


def _load_seen() -> set[str]:
    try:
        return set(json.loads(CHATS_JSON.read_text(encoding="utf-8")).get("seen", []))
    except Exception:
        return set()


def _save_chats(rows: list[dict], seen: set[str]) -> None:
    try:
        CHATS_JSON.write_text(
            json.dumps(
                {
                    "saved_at": _now_str(),
                    "chats": rows,
                    "seen": list(seen)[-4000:],  # bounded; the file must not grow forever
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception as err:
        print(f"[tg-warm] chats persist failed: {err!r}", flush=True)


# ---------------------------------------------------------------------------
# Warm browser
# ---------------------------------------------------------------------------
class _TelegramWarm:
    """One Chromium, one worker thread. Playwright's sync API is not thread-safe,
    so every browser call happens on ``_loop`` and callers submit work through
    ``_tasks`` — the same arrangement as ``_OsmWatchWarm``."""

    def __init__(self) -> None:
        self._tasks: queue.Queue[dict] = queue.Queue()
        self._p = None
        self._context = None
        self._page = None
        self._login_in_progress = False
        # True between 'code requested' and 'code submitted'. While set, nothing may
        # navigate the page: _check_auth() does a page.goto(), which would wipe the
        # pending code form. The chat poll runs every 120s, so without this guard a
        # code login would almost always be destroyed before it could be used.
        self._awaiting_code = False
        # Set by a NEW login request so an in-flight QR wait gives up its hold on the
        # worker thread instead of blocking the user for the full login timeout.
        self._cancel_login = threading.Event()
        self._started = False
        self._start_lock = threading.Lock()

    # -- lifecycle -----------------------------------------------------------
    def start(self) -> None:
        with self._start_lock:
            if self._started:
                return
            self._started = True
            threading.Thread(target=self._loop, name="tg-warm", daemon=True).start()
            threading.Thread(target=self._keepalive_loop, name="tg-warm-ka", daemon=True).start()
            if _chat_poll_enabled():
                threading.Thread(target=self._chat_loop, name="tg-warm-chats", daemon=True).start()

    def _launch(self) -> None:
        from playwright.sync_api import sync_playwright

        self._teardown()
        PROFILE_DIR.parent.mkdir(parents=True, exist_ok=True)
        self._p = sync_playwright().start()
        # Persistent context, NOT launch()+storage_state: Telegram Web keeps its
        # auth key in IndexedDB too, which storage_state does not round-trip.
        self._context = self._p.chromium.launch_persistent_context(
            str(PROFILE_DIR),
            headless=_headless_default(),
            args=["--disable-blink-features=AutomationControlled"],
            viewport={"width": 1600, "height": 900},
            user_agent=_UA,
            locale="en-US",
            timezone_id=os.getenv("TELEGRAM_TZ", "Asia/Kuala_Lumpur"),
            ignore_https_errors=True,
        )
        try:
            self._context.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            )
        except Exception:
            pass
        self._page = self._context.pages[0] if self._context.pages else self._context.new_page()
        _wset(phase="launching", detail="browser launched")
        print("[tg-warm] browser launched (kept open).", flush=True)

    def _teardown(self) -> None:
        for closer in (
            lambda: self._context.close() if self._context else None,
            lambda: self._p.stop() if self._p else None,
        ):
            try:
                closer()
            except Exception:
                pass
        self._p = self._context = self._page = None

    def _healthy(self) -> bool:
        try:
            return self._page is not None and not self._page.is_closed()
        except Exception:
            return False

    # -- public submit API (thread-safe) -------------------------------------
    def submit_ensure(self, *, auto: bool = True) -> None:
        self._tasks.put({"kind": "ensure", "auto": auto})

    def request_login(self, chat_id: str | None = None) -> None:
        self._cancel_login.set()      # bump any QR wait already holding the worker
        self._tasks.put({"kind": "login", "chat_id": chat_id})

    def request_code_login(self, chat_id: str | None = None) -> None:
        self._cancel_login.set()
        self._tasks.put({"kind": "login_code", "chat_id": chat_id})

    def request_reset(self, chat_id: str | None = None) -> None:
        self._cancel_login.set()
        self._tasks.put({"kind": "reset", "chat_id": chat_id})

    def submit_code(self, code: str, chat_id: str | None = None) -> None:
        self._tasks.put({"kind": "submit_code", "code": code, "chat_id": chat_id})

    def capture(self, *, chat_id: str | None = None, timeout_ms: int = 90_000) -> dict:
        done = threading.Event()
        box: dict = {}
        self._tasks.put(
            {"kind": "capture", "chat_id": chat_id, "timeout_ms": timeout_ms,
             "done": done, "box": box}
        )
        done.wait(timeout=timeout_ms / 1000.0 + 30)
        return box

    def scrape_chats(self, *, chat_id: str | None = None, block: bool = False) -> dict:
        task: dict = {"kind": "chats", "chat_id": chat_id, "auto": not block}
        if block:
            done = threading.Event()
            box: dict = {}
            task.update({"done": done, "box": box})
            self._tasks.put(task)
            done.wait(timeout=180)
            return box
        self._tasks.put(task)
        return {}

    def probe_once(self) -> dict:
        done = threading.Event()
        box: dict = {}
        self._tasks.put({"kind": "probe", "done": done, "box": box})
        done.wait(timeout=180)
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
                elif kind == "login_code":
                    self._handle_login_code(task)
                elif kind == "submit_code":
                    self._handle_submit_code(task)
                elif kind == "capture":
                    self._handle_capture(task)
                elif kind == "chats":
                    self._handle_chats(task)
                elif kind == "probe":
                    self._handle_probe(task)
                elif kind == "reset":
                    self._handle_reset(task)
            except Exception as err:
                print(f"[tg-warm] task {kind} error: {err!r}", flush=True)
                _wset(phase="error", detail=f"task {kind} failed", last_error=repr(err))
                self._teardown()
            finally:
                if task.get("done"):
                    task["done"].set()

    def _keepalive_loop(self) -> None:
        while True:
            time.sleep(_keepalive_sec())
            self._tasks.put({"kind": "keepalive", "auto": True})

    def _chat_loop(self) -> None:
        time.sleep(min(120, _chat_poll_sec()))
        while True:
            self._tasks.put({"kind": "chats", "auto": True})
            time.sleep(_chat_poll_sec())

    # -- task handlers (worker thread only) ----------------------------------
    def _check_auth(self, timeout_ms: int = 90_000) -> str:
        """Load Telegram Web, let the SPA boot, then classify what's on screen."""
        self._page.goto(WEB_URL, wait_until="domcontentloaded", timeout=timeout_ms)
        # Telegram Web restores its session from IndexedDB asynchronously — a probe
        # fired too early reports the auth screen even for a perfectly good session,
        # so keep re-probing until it settles or the window closes.
        verdict = "unknown"
        deadline = time.time() + 25
        while time.time() < deadline:
            self._page.wait_for_timeout(1200)
            verdict = _classify(_probe(self._page))
            if verdict in ("authenticated", "password"):
                break
            if verdict == "login" and time.time() > deadline - 15:
                break
        _wset(last_verdict=verdict, last_check=_now_str())
        return verdict

    def _handle_ensure(self, task: dict) -> None:
        if self._awaiting_code:
            print('[tg-warm] skip ensure — waiting for /telegramcode', flush=True)
            return
        if not self._healthy():
            self._launch()
        verdict = self._check_auth()

        if verdict == "password":
            _wset(phase="password", detail="two-step verification password required")
            if _maybe_fill_2fa(self._page) != "ok":
                self._notify_password_needed(None)
                return
            verdict = self._check_auth()

        if verdict == "authenticated":
            snap = warm_snapshot()
            _wset(
                phase="authenticated",
                detail="Telegram Web session live",
                logged_in_since=snap["logged_in_since"] or _now_str(),
                last_error=None,
                alerted=False,   # re-arm the alert for the next genuine failure
            )
            _set_needs_manual(False)
            return

        if verdict == "unknown":
            _wset(phase="error", detail="could not classify the page (run --probe)")
            print("[tg-warm] unknown page state; retrying next keepalive.", flush=True)
            return

        # verdict == "login" → no session, or it was revoked.
        #
        # Deliberately NO automatic QR here. Posting one unprompted would (a) hijack
        # this single worker thread for the whole login timeout, queueing any
        # /logintelegram or /telegramcode the user then sends, and (b) spam the group
        # with QRs nobody asked for. The spec is to *report* the failure and wait for
        # an explicit /logintelegram.
        _wset(phase="login", detail="not logged in — run /logintelegram")
        _set_needs_manual(True)
        _alert_login_failed("no Telegram Web session — run /logintelegram to sign in")
        print("[tg-warm] not logged in; waiting for /logintelegram", flush=True)

    def _handle_login(self, task: dict) -> None:
        chat_id = task.get("chat_id")
        self._cancel_login.clear()    # this IS the new request
        if self._login_in_progress:
            if chat_id:
                send_text(chat_id, "⏳ Telegram login already in progress — check the group for the QR.")
            return
        if not self._healthy():
            self._launch()
        verdict = self._check_auth()
        if verdict == "authenticated":
            _wset(phase="authenticated", detail="already logged in", alerted=False)
            _set_needs_manual(False)
            if chat_id:
                send_text(chat_id, "✅ Telegram Web is already logged in.")
            return
        if verdict == "password":
            if _maybe_fill_2fa(self._page) != "ok":
                self._notify_password_needed(chat_id)
                return
        _set_needs_manual(False)  # forced fresh attempt
        self._do_qr_login(ack_chat=chat_id)

    def _handle_login_code(self, task: dict) -> None:
        """Phone + code login: enter the number, ask Telegram to send the code,
        then wait for the human to relay it with /telegramcode."""
        chat_id = task.get("chat_id") or _qr_chat_default()
        self._cancel_login.clear()    # this IS the new request
        # Worker-pickup receipt. main.py posts "requesting…" when the task is QUEUED;
        # this only fires once the single worker thread actually starts it. Seeing the
        # first message without this one means the worker was busy, which is the
        # failure mode that used to look like "the bot did nothing".
        try:
            send_text(chat_id, "⚙️ Telegram: opening the phone-number form…")
        except Exception:
            pass
        if not self._healthy():
            self._launch()
        verdict = self._check_auth()
        if verdict == "authenticated":
            _wset(phase="authenticated", detail="already logged in", alerted=False)
            _set_needs_manual(False)
            send_text(chat_id, "✅ Telegram is already logged in — nothing to do.")
            return
        if verdict == "password":
            if _maybe_fill_2fa(self._page) == "ok":
                self._handle_ensure({"auto": False})
                return
            self._notify_password_needed(chat_id)
            return

        step, probe = _start_code_login(self._page)
        shot = str(SHOT_PNG)
        try:
            self._page.screenshot(path=shot)
        except Exception:
            shot = None

        if step in ("code", "code_pending"):
            self._awaiting_code = True
            _wset(phase="login", detail="waiting for /telegramcode <code>")
            masked = f"{_phone_number()[:4]}…{_phone_number()[-3:]}"
            # Telegram states the delivery channel on the screen itself ("…a message
            # in Telegram" vs "…an SMS"). Quote it rather than guessing, because
            # looking in the wrong place is indistinguishable from no code arriving.
            delivery = next(
                (ln.strip() for ln in (probe.get("authLines") or [])
                 if "sent" in ln.lower()),
                "",
            )
            if step == "code":
                lines = [f"📲 Telegram sent a login code to {masked}."]
            else:
                lines = [
                    f"⚠️ A code request was ALREADY pending for {masked} — "
                    "no NEW code was sent just now.",
                    "If the earlier code has expired, see below to force a fresh one.",
                ]
            if delivery:
                lines.append(f"Telegram says: “{delivery}”")
                if "sms" not in delivery.lower():
                    lines.append(
                        "→ That means an IN-APP message, not an SMS. Open Telegram on "
                        "your phone and look in the chat named “Telegram” (the blue "
                        "service account), not your SMS inbox."
                    )
            lines.append("Then reply here:    /telegramcode 12345")
            if step == "code_pending":
                lines.append(
                    "To force a brand-new code: /resettelegram (clears the session), "
                    "then /logintelegram code."
                )
            lines.append(
                "⚠️ Single-use and expires quickly. Anyone who can read this chat can "
                "read the code — prefer a direct message to the bot."
            )
            send_text(chat_id, "\n".join(lines))
            if shot:
                _send_shot(chat_id, shot)
            return

        if step == "password":
            self._notify_password_needed(chat_id)
            return

        # Anything else is a failure; the screenshot is the most useful evidence.
        reasons = {
            "no_phone": "TELEGRAM_PHONE is not set in .env",
            "fill_failed": "could not type into the phone field (Telegram DOM changed?)",
            "next_failed": "could not press NEXT",
            "qr": "still on the QR screen — the phone-number link was not found",
            "refused": "Telegram refused the number (see the error text below)",
            "sendcode_timeout": (
                f"Telegram never answered within {_code_wait_s()}s — the button was still "
                "'PLEASE WAIT…'. The number and the form were fine, so this is the "
                "server's connection to Telegram: check outbound firewall/egress, or try "
                "the QR route with /logintelegram. Raise TELEGRAM_CODE_WAIT_SEC if the "
                "link is just slow."
            ),
            "unknown_step": "unrecognised auth screen",
        }
        why = reasons.get(step, f"unexpected step '{step}'")
        _wset(phase="login", detail=f"code login failed: {why}", last_error=why)
        btn = (probe.get("primaryBtn") or {}).get("text") if probe else None
        errs = (probe.get("errorText") or []) if probe else []
        msg = [f"❌ Telegram code login failed: {why}"]
        if btn:
            msg.append(f"Button state: {btn!r}")
        if errs:
            msg.append(f"Telegram said: {errs}")
        msg.append(f"Auth screen text: {(probe.get('authLines') or [])[:8]}")
        send_text(chat_id, "\n".join(msg))
        if shot:
            _send_shot(chat_id, shot)

    def _handle_submit_code(self, task: dict) -> None:
        """Type the code the human relayed, then report where we landed."""
        chat_id = task.get("chat_id") or _qr_chat_default()
        code = (task.get("code") or "").strip()
        if not self._healthy():
            send_text(chat_id, "❌ The Telegram browser is not running — run /logintelegram code first.")
            return

        try:
            verdict, probe = _submit_code(self._page, code)
        finally:
            # Cleared either way: a refused code must not keep the browser frozen.
            self._awaiting_code = False
        shot = str(SHOT_PNG)
        try:
            self._page.screenshot(path=shot)
        except Exception:
            shot = None

        if verdict == "authenticated":
            _wset(
                phase="authenticated",
                detail="logged in via phone code",
                logged_in_since=_now_str(),
                last_error=None,
                alerted=False,
            )
            _set_needs_manual(False)
            send_text(chat_id, "✅ Telegram: logged in — the warm browser is live and monitoring.")
            if shot:
                _send_shot(chat_id, shot)
            return

        if verdict == "password_refused":
            detail = "two-step verification password refused"
            _wset(phase="password", detail=detail, last_error=detail)
            send_text(
                chat_id,
                "❌ Telegram: the code was accepted, but the two-step verification "
                f"password was REFUSED.\n"
                f"Fix TELEGRAM_2FA_PASSWORD in {_ENV_PATH.name} — it is re-read on "
                "each attempt, so no restart is needed — then run /logintelegram code "
                "again.\n"
                "It was tried once only, on purpose: Telegram flood-limits repeated "
                "wrong passwords, which can lock the account out for hours.",
            )
            if shot:
                _send_shot(chat_id, shot)
            return

        if verdict == "password":
            self._notify_password_needed(chat_id)
            return

        step = _auth_step(probe)
        detail = (
            "the code was refused or has expired" if step == "code"
            else f"unexpected screen after the code (step={step})"
        )
        _wset(phase="login", detail=detail, last_error=detail)
        send_text(
            chat_id,
            f"❌ Telegram login: {detail}.\n"
            "Run /logintelegram code again to request a fresh code.",
        )
        if shot:
            _send_shot(chat_id, shot)

    def _notify_password_needed(self, chat_id: str | None) -> None:
        target = chat_id or _qr_chat_default()
        mention = f'<at user_id="{ALERT_OPEN_ID}"></at> ' if ALERT_OPEN_ID else ""
        try:
            shot = str(SHOT_PNG)
            self._page.screenshot(path=shot)
            # Distinguish "no password configured" from "password configured but it
            # could not be entered". Conflating the two is what sent the last round of
            # debugging into .env when the real fault was a selector.
            pwd, source = _2fa_password()
            if pwd:
                fields = _field_inventory(self._page)
                body = (
                    f"{mention}🟠 Telegram: login accepted, and a two-step password IS "
                    f"configured (found in {source}), but it could not be entered on "
                    f"this screen.\n"
                    f"That is a bot-side problem, not your .env.\n"
                    f"Fields present on the page: {json.dumps(fields, ensure_ascii=False)}\n"
                    f"Send that line back — it names the exact selector needed."
                )
            else:
                body = (
                    f"{mention}🟠 Telegram: login accepted, but this account has "
                    f"two-step verification and NO password could be found.\n"
                    f"Looked in: the bot process environment AND {_ENV_PATH.name} "
                    f"(re-read live).\n"
                    f"Add this line to {_ENV_PATH.name} — no restart needed:\n"
                    f"    TELEGRAM_2FA_PASSWORD=your-password\n"
                    f"Then run /logintelegram code again."
                )
            send_text(target, body)
            _send_shot(target, shot)
        except Exception as err:
            print(f"[tg-warm] password-needed notify failed: {err!r}", flush=True)

    def _do_qr_login(self, *, ack_chat: str | None = None) -> bool:
        """Post the QR to Lark, then poll until the scan lands or we time out."""
        target = ack_chat or _qr_chat_default()
        self._login_in_progress = True
        try:
            _wset(phase="login", detail="waiting for a QR scan")
            if ack_chat:
                send_text(ack_chat, "🔐 Telegram: posting a login QR — scan it with your Telegram app.")

            deadline = time.time() + _login_timeout_s()
            last_sent = 0.0
            tried_2fa = False
            resend_sec = 60  # Telegram rotates the QR itself; re-post periodically
            while time.time() < deadline:
                if self._cancel_login.is_set():
                    print("[tg-warm] QR wait cancelled by a newer login request", flush=True)
                    return False
                verdict = _classify(_probe(self._page))

                if verdict == "password":
                    _wset(phase="password", detail="QR scanned; password required")
                    # ONCE only — see _submit_code: repeating a refused password here
                    # would hammer Telegram with wrong attempts.
                    if not tried_2fa:
                        tried_2fa = True
                        if _maybe_fill_2fa(self._page) == "ok":
                            continue
                    self._notify_password_needed(target)
                    return False

                if verdict == "authenticated":
                    _wset(
                        phase="authenticated",
                        detail="logged in via QR",
                        logged_in_since=_now_str(),
                        last_error=None,
                        alerted=False,
                    )
                    _set_needs_manual(False)
                    try:
                        send_text(target, "✅ Telegram: logged in — the warm browser is live and monitoring.")
                    except Exception:
                        pass
                    print("[tg-warm] ✅ QR login complete", flush=True)
                    return True

                if time.time() - last_sent >= resend_sec:
                    _reveal_qr(self._page)
                    _capture_qr(self._page, QR_PNG)
                    if _send_shot(target, str(QR_PNG)):
                        try:
                            send_text(
                                target,
                                "📷 Telegram login QR — on your phone: Telegram → Settings → "
                                "Devices → Link Desktop Device, then scan this. "
                                f"(rotates every ~{resend_sec}s, a fresh one follows)",
                            )
                        except Exception:
                            pass
                    last_sent = time.time()

                self._page.wait_for_timeout(2500)

            # Timed out: stop auto-QR so the keepalive can't spam the group.
            _set_needs_manual(True)
            _wset(phase="login", detail="QR timed out — run /logintelegram to retry")
            _alert_login_failed(
                f"nobody scanned the login QR within {_login_timeout_s()}s", force=True
            )
            print("[tg-warm] QR login timed out", flush=True)
            return False
        finally:
            self._login_in_progress = False

    def _handle_capture(self, task: dict) -> None:
        box = task.get("box")
        chat_id = task.get("chat_id")
        try:
            if not self._healthy():
                self._launch()
            verdict = self._check_auth(timeout_ms=task.get("timeout_ms", 90_000))
            if verdict == "password" and _maybe_fill_2fa(self._page) == "ok":
                verdict = self._check_auth()
            if verdict == "authenticated":
                _wset(phase="authenticated", detail="Telegram Web session live", alerted=False)

            out = str(SHOT_PNG)
            # Screenshot whatever is on screen — a login wall is exactly what you
            # want to see when the question is "is it actually logged in?".
            self._page.screenshot(path=out)
            _wset(last_shot=_now_str())
            if box is not None:
                box["path"] = out
                box["verdict"] = verdict
            if chat_id:
                _send_shot(chat_id, out)
        except Exception as err:
            if box is not None:
                box["error"] = repr(err)
            _wset(phase="error", detail="capture failed", last_error=repr(err))
            self._teardown()

    def _handle_chats(self, task: dict) -> None:
        if self._awaiting_code:
            print('[tg-warm] skip chat scrape — waiting for /telegramcode', flush=True)
            return
        box = task.get("box")
        chat_id = task.get("chat_id")
        try:
            if not self._healthy():
                self._launch()
            verdict = self._check_auth()
            if verdict != "authenticated":
                if box is not None:
                    box["error"] = "not_authenticated"
                print(f"[tg-warm] skip chat scrape — verdict={verdict}", flush=True)
                if verdict == "login":
                    _alert_login_failed("session dropped — chat poll cannot read anything")
                return

            rows = [r for r in (self._page.evaluate(_CHATS_JS) or []) if _wanted(r)]
            seen = _load_seen()
            # A cold cache (first poll after a restart / a fresh login) makes every
            # row look new. That's not traffic, so record it silently.
            cold_start = not seen
            fresh = [r for r in rows if _preview_key(r) not in seen]
            for r in rows:
                seen.add(_preview_key(r))
            _save_chats(rows, seen)

            _wset(chats_seen=len(rows))
            if fresh and not cold_start:
                with _state_lock:
                    _warm_state["new_previews"] += len(fresh)
                    _warm_state["last_activity"] = _now_str()

            if box is not None:
                box["chats"] = rows
                box["new"] = [] if cold_start else fresh

            target = chat_id or (_forward_chat() if task.get("auto") else None)
            if fresh and target and not cold_start:
                lines = [f"📩 Telegram — {len(fresh)} chat(s) with new messages:"]
                for r in fresh[:10]:
                    badge = f" [{r['unread']} unread]" if r.get("unread") else ""
                    lines.append(f"• {r['title']}{badge}: {r.get('preview', '')[:140]}")
                if len(fresh) > 10:
                    lines.append(f"…and {len(fresh) - 10} more")
                try:
                    send_text(target, "\n".join(lines))
                except Exception as err:
                    print(f"[tg-warm] digest send failed: {err!r}", flush=True)
            elif cold_start:
                print(f"[tg-warm] cold cache primed with {len(rows)} chats (no digest)", flush=True)
        except Exception as err:
            if box is not None:
                box["error"] = repr(err)
            _wset(phase="error", detail="chat scrape failed", last_error=repr(err))
            self._teardown()

    def _handle_reset(self, task: dict) -> None:
        """Delete the browser profile so the next login starts from nothing.

        Needed because Telegram restores a pending code screen from the profile: a
        stale, expired code request cannot otherwise be cleared, and the phone form
        will not issue a second code while one is outstanding. This also signs out
        any working session, which is why it is only ever run on request.
        """
        chat_id = task.get("chat_id") or _qr_chat_default()
        self._teardown()
        self._awaiting_code = False
        removed = False
        try:
            if PROFILE_DIR.exists():
                shutil.rmtree(PROFILE_DIR, ignore_errors=True)
                removed = not PROFILE_DIR.exists()
        except Exception as err:
            print(f"[tg-warm] profile delete failed: {err!r}", flush=True)
        for path in (CHATS_JSON, _LOGIN_STATE):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass
        _wset(
            phase="idle",
            detail="session reset — run /logintelegram code",
            logged_in_since=None,
            last_verdict=None,
            last_error=None,
            alerted=False,
            chats_seen=0,
            new_previews=0,
        )
        try:
            send_text(
                chat_id,
                ("🧹 Telegram session reset — profile "
                 + ("deleted" if removed else "could not be fully deleted")
                 + ".\nNow run /logintelegram code for a brand-new code."),
            )
        except Exception:
            pass
        print(f"[tg-warm] reset done (profile removed={removed})", flush=True)

    def _handle_probe(self, task: dict) -> None:
        box = task.get("box")
        if not self._healthy():
            self._launch()
        self._page.goto(WEB_URL, wait_until="domcontentloaded", timeout=90_000)
        self._page.wait_for_timeout(6000)
        raw = _probe(self._page)
        if box is not None:
            box["probe"] = raw
            box["verdict"] = _classify(raw)


# ---------------------------------------------------------------------------
# Module-level singleton + thin wrappers for main.py
# ---------------------------------------------------------------------------
_warm: Optional[_TelegramWarm] = None
_warm_lock = threading.Lock()


def warm() -> _TelegramWarm:
    global _warm
    with _warm_lock:
        if _warm is None:
            _warm = _TelegramWarm()
        return _warm


def prewarm_telegram_on_startup() -> None:
    """Boot hook. No-op unless TELEGRAM_WARM_ENABLED is truthy."""
    if not _warm_enabled():
        print("[tg-warm] TELEGRAM_WARM_ENABLED not set — Telegram watcher off", flush=True)
        _wset(phase="idle", detail="disabled (TELEGRAM_WARM_ENABLED not set)")
        return
    try:
        import playwright  # noqa: F401
    except Exception:
        detail = "playwright not installed — run `python -m pip install playwright`"
        _wset(phase="error", detail=detail, last_error=detail)
        print(f"[tg-warm] ❌ {detail}", flush=True)
        _alert_login_failed(detail)
        return
    w = warm()
    w.start()
    w.submit_ensure(auto=True)
    print("[tg-warm] pre-warm queued", flush=True)


def request_login(chat_id: str | None = None) -> None:
    w = warm()
    w.start()
    w.request_login(chat_id)


def request_code_login(chat_id: str | None = None) -> None:
    """/logintelegram code — phone-number login; the code comes back via Lark."""
    w = warm()
    w.start()
    w.request_code_login(chat_id)


def submit_login_code(code: str, chat_id: str | None = None) -> None:
    """/telegramcode <code> — relay the one-time code into the browser."""
    w = warm()
    w.start()
    w.submit_code(code, chat_id)


def reset_session(chat_id: str | None = None) -> None:
    """/resettelegram — wipe the profile so the next login starts clean."""
    w = warm()
    w.start()
    w.request_reset(chat_id)


def capture_and_send(chat_id: str | None = None) -> dict:
    w = warm()
    w.start()
    return w.capture(chat_id=chat_id)


def scrape_chats_now(chat_id: str | None = None) -> dict:
    w = warm()
    w.start()
    return w.scrape_chats(chat_id=chat_id, block=True)


def send_status_to_lark(chat_id: str) -> dict:
    """/telegramstatus — the status text, plus a live screenshot of Telegram Web.

    The screenshot is the answer to "is it actually logged in?", so it is sent
    whether or not the session is healthy — a login wall in the picture is the
    most useful possible reply.
    """
    if not _warm_enabled():
        try:
            send_text(
                chat_id,
                "⚪ Telegram watcher is OFF — set TELEGRAM_WARM_ENABLED=1 in .env and restart.",
            )
        except Exception:
            pass
        return {"monitoring": False, "enabled": False}

    box = capture_and_send(chat_id)
    try:
        send_text(chat_id, "\n".join(status_lines()))
    except Exception as err:
        print(f"[tg-warm] status text send failed: {err!r}", flush=True)
    return {
        "monitoring": is_monitoring(),
        "screenshot": box.get("path"),
        "verdict": box.get("verdict"),
        "error": box.get("error"),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    os.environ.setdefault("TELEGRAM_WARM_ENABLED", "1")

    if "--login" in args:
        w = warm()
        w.start()
        w.request_login(None)
        print(f"QR login requested — watch Lark chat {_qr_chat_default()}. Ctrl-C to stop waiting.")
        try:
            deadline = time.time() + _login_timeout_s() + 60
            while time.time() < deadline and not is_authenticated():
                time.sleep(3)
        except KeyboardInterrupt:
            pass
        print("\n".join(status_lines()))
        return 0 if is_authenticated() else 1

    if "--login-code" in args:
        request_code_login(None)
        print(f"Code login started — the prompt goes to Lark chat {_qr_chat_default()}.")
        print("Then send `/telegramcode 12345` in Lark, or run --code 12345 here.")
        try:
            deadline = time.time() + 180
            while time.time() < deadline and not is_authenticated():
                time.sleep(3)
        except KeyboardInterrupt:
            pass
        print("\n".join(status_lines()))
        return 0

    if "--code" in args:
        idx = args.index("--code")
        code = "".join(ch for ch in " ".join(args[idx + 1:]) if ch.isdigit())
        if not code:
            print("usage: python telegramwarm.py --code 12345")
            return 2
        submit_login_code(code, None)
        try:
            deadline = time.time() + 120
            while time.time() < deadline and not is_authenticated():
                time.sleep(3)
        except KeyboardInterrupt:
            pass
        print("\n".join(status_lines()))
        return 0 if is_authenticated() else 1

    if "--shot" in args:
        box = capture_and_send(None)
        print(json.dumps(box, indent=2))
        return 0 if box.get("path") else 1

    if "--probe" in args:
        w = warm()
        w.start()
        print(json.dumps(w.probe_once(), indent=2, ensure_ascii=False))
        return 0

    if "--chats" in args:
        box = scrape_chats_now(None)
        print(json.dumps(box, indent=2, ensure_ascii=False)[:4000])
        return 0 if not box.get("error") else 1

    print("\n".join(status_lines()))
    print("\nusage: python telegramwarm.py [--login | --shot | --probe | --chats]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
