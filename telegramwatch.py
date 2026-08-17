#!/usr/bin/env python3
"""Telegram group watcher — READ-ONLY.

Logs in as a Telegram **user account** (MTProto / Telethon) and listens for new
messages in the groups you belong to. Nothing is ever sent on the Telegram side:
there is no ``client.send_message`` call anywhere in this module, which is what
keeps the account off Telegram's anti-spam radar (bans are driven by outbound
behaviour — mass DMs, bulk joins, adding members — not by reading).

Why a user account and not a Bot API bot: a bot has to be *added to the group by
an admin*, is limited by privacy mode, and cannot read anything sent before it
joined. A user session sees every group the account is already in, with history.

Login is inherently interactive — Telegram mails a one-time code to the account's
Telegram app / SMS, so it CANNOT be automated. Run once on the server:

    python telegramwatch.py --login          # phone -> code -> (2FA password)
    python telegramwatch.py --list-chats     # print group ids so you can pick
    python telegramwatch.py --status         # what the watcher thinks right now

After that the ``telegram_session.session`` file is the credential and the
monitor comes up unattended on every ``main.py`` boot. If the session is missing,
revoked or rejected, the watcher posts a **login failed** alert to the Lark group
in ``TELEGRAM_ALERT_CHAT_ID`` and @-mentions ``TELEGRAM_ALERT_OPEN_ID``.

Exposed to main.py:
    start_monitor_on_startup()   -> boot hook, spawns the listener thread
    send_status_to_lark(chat_id) -> /telegramstatus (text summary + PNG snapshot)
    status_lines()               -> list[str] summary
    is_monitoring()              -> bool
"""

from __future__ import annotations

import asyncio
import json
import mimetypes
import os
import sys
import threading
import time
from collections import deque
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

# Telethon keeps its auth key in a sqlite file. This IS the credential once the
# one-time login is done — gitignored, and worth backing up so you don't have to
# re-do the OTP dance.
SESSION_PATH = _ROOT_DIR / os.getenv("TELEGRAM_SESSION_FILE", "telegram_session")
# Rolling counters + last-seen, so /telegramstatus stays useful across restarts.
_STATE_PATH = _ROOT_DIR / "telegramwatch_state.json"
STATUS_PNG = _ROOT_DIR / "telegramwatch_status.png"

# Lark group that hears about login failures, and the person to @-mention there.
ALERT_CHAT_ID = os.getenv(
    "TELEGRAM_ALERT_CHAT_ID", "oc_ad9b5bdbb2826ba2ee9730920ef25432"
).strip()
ALERT_OPEN_ID = os.getenv(
    "TELEGRAM_ALERT_OPEN_ID", "ou_5f660c0fb0769d184aca635d02209272"
).strip()
# Where captured Telegram messages get mirrored. Defaults to the alert group.
FORWARD_CHAT_ID = (os.getenv("TELEGRAM_FORWARD_CHAT_ID", "").strip() or ALERT_CHAT_ID)

_RECENT_MAX = 200
_MAX_LARK_FORWARDS_PER_MIN = int(os.getenv("TELEGRAM_FORWARD_RATE_PER_MIN", "20") or 20)


def _truthy(val: str | None) -> bool:
    return str(val or "").strip().lower() in ("1", "true", "yes", "on", "y")


def _watch_enabled() -> bool:
    """Opt-in. Unset means OFF, so deploying this file alone changes nothing."""
    return _truthy(os.getenv("TELEGRAM_WATCH_ENABLED"))


def _forward_enabled() -> bool:
    v = os.getenv("TELEGRAM_FORWARD_ENABLED")
    return True if v is None else _truthy(v)


def _tz():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(os.getenv("TELEGRAM_WATCH_TZ", "Asia/Shanghai"))
    except Exception:
        return timezone.utc


def _now_str() -> str:
    return datetime.now(_tz()).strftime("%Y-%m-%d %H:%M:%S")


def _watch_targets() -> list[str]:
    """Groups to watch: ids, @usernames or title substrings. Empty = every group."""
    raw = os.getenv("TELEGRAM_WATCH_CHATS", "") or ""
    return [t.strip() for t in raw.replace(";", ",").split(",") if t.strip()]


def _api_creds() -> tuple[int, str]:
    """api_id / api_hash from https://my.telegram.org/apps (per-account, one-time)."""
    raw_id = (os.getenv("TELEGRAM_API_ID", "") or "").strip()
    api_hash = (os.getenv("TELEGRAM_API_HASH", "") or "").strip()
    if not raw_id or not api_hash:
        raise RuntimeError(
            "TELEGRAM_API_ID / TELEGRAM_API_HASH not set in .env — create them at "
            "https://my.telegram.org/apps"
        )
    try:
        api_id = int(raw_id)
    except ValueError as exc:
        raise RuntimeError(f"TELEGRAM_API_ID must be an integer, got {raw_id!r}") from exc
    return api_id, api_hash


# ---------------------------------------------------------------------------
# Shared state (the monitor thread writes, /telegramstatus reads)
# ---------------------------------------------------------------------------
_lock = threading.Lock()
_state: dict[str, Any] = {
    "phase": "idle",          # idle | connecting | monitoring | login_failed | stopped
    "detail": "not started",
    "account": None,          # "Name (@user, +60…)" once authorised
    "started_at": None,       # monotonic, for uptime
    "connected_at": None,     # wall clock string
    "last_error": None,
    "last_message_at": None,
    "messages_seen": 0,
    "forwarded": 0,
    "groups_seen": {},        # title -> count, this process
    "recent": deque(maxlen=_RECENT_MAX),
    "alerted": False,         # de-dupe the login-failed alert
}
_thread: Optional[threading.Thread] = None
_forward_window: deque[float] = deque()


def _set(**kw: Any) -> None:
    with _lock:
        _state.update(kw)


def _snapshot() -> dict[str, Any]:
    with _lock:
        snap = dict(_state)
        snap["recent"] = list(_state["recent"])
        snap["groups_seen"] = dict(_state["groups_seen"])
        return snap


def is_monitoring() -> bool:
    with _lock:
        return _state["phase"] == "monitoring"


def _load_persisted() -> None:
    try:
        data = json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return
    with _lock:
        for key in ("messages_seen", "forwarded", "last_message_at", "account"):
            if data.get(key) is not None:
                _state[key] = data[key]


def _persist() -> None:
    with _lock:
        payload = {
            "messages_seen": _state["messages_seen"],
            "forwarded": _state["forwarded"],
            "last_message_at": _state["last_message_at"],
            "account": _state["account"],
            "saved_at": _now_str(),
        }
    try:
        _STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception as err:
        print(f"[telegram] state persist failed: {err!r}", flush=True)


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


def _alert_login_failed(reason: str, *, force: bool = False) -> None:
    """Post the login-failure notice to the Lark group, @-mentioning the owner.

    De-duped by default: a reconnect loop must not spam the group with the same
    failure every retry. ``force=True`` for a fresh, distinct failure.
    """
    with _lock:
        if _state["alerted"] and not force:
            return
        _state["alerted"] = True
    # Plain-text mention markup is `<at user_id="ou_…">Name</at>` (card markup
    # would be `<at id=ou_…></at>` — different thing, see maintenance.py:3082).
    mention = f'<at user_id="{ALERT_OPEN_ID}"></at> ' if ALERT_OPEN_ID else ""
    text = (
        f"{mention}❌ Telegram watcher: LOGIN FAILED\n"
        f"• Reason: {reason}\n"
        f"• Account: {os.getenv('TELEGRAM_PHONE', '(TELEGRAM_PHONE unset)')}\n"
        f"• Session: {SESSION_PATH.name}.session"
        f" ({'present' if Path(str(SESSION_PATH) + '.session').exists() else 'MISSING'})\n"
        f"• Time: {_now_str()}\n"
        f"Fix: run `python telegramwatch.py --login` on the server "
        f"(Telegram will send a one-time code that has to be typed in), then restart the bot."
    )
    try:
        resp = send_text(ALERT_CHAT_ID, text)
        if resp.get("code") != 0:
            print(f"❌ telegram login-fail alert rejected: {resp}", flush=True)
    except Exception as err:
        print(f"❌ telegram login-fail alert failed: {err!r}", flush=True)


# ---------------------------------------------------------------------------
# Status rendering
# ---------------------------------------------------------------------------
_PHASE_EMOJI = {
    "idle": "⚪",
    "connecting": "🟡",
    "monitoring": "🟢",
    "login_failed": "🔴",
    "stopped": "⚫",
}


def status_lines() -> list[str]:
    snap = _snapshot()
    phase = snap["phase"]
    emoji = _PHASE_EMOJI.get(phase, "⚪")
    monitoring = phase == "monitoring"

    lines = [
        f"{emoji} Telegram watcher: {'MONITORING' if monitoring else phase.upper()}",
        f"• Detail: {snap['detail']}",
        f"• Logged in as: {snap['account'] or '— not authorised —'}",
        f"• Session file: {SESSION_PATH.name}.session "
        f"({'present' if Path(str(SESSION_PATH) + '.session').exists() else 'MISSING'})",
    ]
    if snap["connected_at"]:
        lines.append(f"• Connected at: {snap['connected_at']}")
    if snap["started_at"]:
        up = int(time.monotonic() - snap["started_at"])
        h, rem = divmod(up, 3600)
        m, s = divmod(rem, 60)
        lines.append(f"• Uptime: {h}h {m}m {s}s")

    targets = _watch_targets()
    lines.append(
        "• Watching: " + (", ".join(targets) if targets else "ALL groups this account is in")
    )
    lines.append(
        f"• Forwarding to: {FORWARD_CHAT_ID or '(off)'}"
        + ("" if _forward_enabled() else "  [disabled]")
    )
    lines.append(
        f"• Messages seen: {snap['messages_seen']}  |  forwarded: {snap['forwarded']}"
    )
    lines.append(f"• Last message: {snap['last_message_at'] or '—'}")
    if snap["last_error"]:
        lines.append(f"• Last error: {snap['last_error']}")

    groups = snap["groups_seen"]
    if groups:
        lines.append("")
        lines.append("Groups with traffic (this run):")
        for title, count in sorted(groups.items(), key=lambda kv: -kv[1])[:10]:
            lines.append(f"   {title} — {count}")

    recent = snap["recent"]
    if recent:
        lines.append("")
        lines.append(f"Last {min(len(recent), 8)} message(s) received:")
        for item in list(recent)[-8:]:
            body = (item.get("text") or "").replace("\n", " ")
            if len(body) > 90:
                body = body[:87] + "…"
            lines.append(
                f"   [{item.get('at', '?')}] {item.get('chat', '?')} / "
                f"{item.get('sender', '?')}: {body or '(non-text)'}"
            )
    lines.append("")
    lines.append(f"Snapshot taken {_now_str()}")
    return lines


def _load_font(size: int):
    from PIL import ImageFont

    candidates = [
        "C:/Windows/Fonts/consola.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSansMono.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",
        "/Library/Fonts/Arial.ttf",
    ]
    for path in candidates:
        try:
            if os.path.exists(path):
                return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


#: Monospace fonts carry no emoji glyphs, so the status dots render as tofu boxes
#: in the PNG. The picture already signals state with the accent bar and colour,
#: so swap them for ASCII markers here only — the Lark text keeps the emoji.
_PNG_SUBS = {
    "🟢": "[*]", "🔴": "[!]", "🟡": "[~]", "⚪": "[ ]", "⚫": "[x]",
    "❌": "[X]", "📩": "[>]", "•": "-", "…": "...", "—": "-", "✅": "[ok]",
}


def _png_safe(text: str) -> str:
    for src, dst in _PNG_SUBS.items():
        text = text.replace(src, dst)
    return text


def render_status_png(out_path: Path | str = STATUS_PNG) -> str | None:
    """Render the status board to a PNG — the 'whole picture' for /telegramstatus."""
    lines = [_png_safe(ln) for ln in status_lines()]
    try:
        from PIL import Image, ImageDraw
    except Exception as err:
        print(f"[telegram] Pillow unavailable, no PNG: {err!r}", flush=True)
        return None

    font = _load_font(18)
    title_font = _load_font(24)
    pad, line_h = 24, 26

    probe = ImageDraw.Draw(Image.new("RGB", (10, 10)))

    def _w(text: str, fnt) -> int:
        try:
            box = probe.textbbox((0, 0), text, font=fnt)
            return box[2] - box[0]
        except Exception:
            return len(text) * 9

    title = "Telegram Watcher — status"
    width = max([_w(title, title_font)] + [_w(ln, font) for ln in lines]) + pad * 2
    width = max(720, min(width, 1600))
    height = pad * 2 + 40 + line_h * len(lines)

    monitoring = is_monitoring()
    accent = (34, 160, 78) if monitoring else (200, 60, 60)

    img = Image.new("RGB", (width, height), (250, 250, 252))
    draw = ImageDraw.Draw(img)
    draw.rectangle([0, 0, width, 6], fill=accent)
    draw.text((pad, pad), title, font=title_font, fill=(24, 24, 32))

    y = pad + 40
    for ln in lines:
        colour = (24, 24, 32)
        if ln.startswith(("[*]", "[!]", "[~]", "[ ]", "[x]")):
            colour = accent
        elif ln.startswith("   ") or ln.startswith("Snapshot"):
            colour = (90, 90, 102)
        draw.text((pad, y), ln, font=font, fill=colour)
        y += line_h

    out = str(out_path)
    try:
        img.save(out)
        return out
    except Exception as err:
        print(f"[telegram] status PNG save failed: {err!r}", flush=True)
        return None


def send_status_to_lark(chat_id: str) -> dict:
    """/telegramstatus — text summary, plus a PNG snapshot when monitoring."""
    monitoring = is_monitoring()
    try:
        send_text(chat_id, "\n".join(status_lines()))
    except Exception as err:
        print(f"[telegram] status text send failed: {err!r}", flush=True)

    shot = None
    if monitoring:
        # Only worth a picture when there is something live to show.
        shot = render_status_png()
        if shot:
            try:
                key = upload_image_lark(shot)
                if key:
                    send_image(chat_id, key)
            except Exception as err:
                print(f"[telegram] status image send failed: {err!r}", flush=True)

    return {"monitoring": monitoring, "screenshot": shot}


# ---------------------------------------------------------------------------
# Message capture
# ---------------------------------------------------------------------------
def _matches_target(chat_id: Any, title: str, username: str, targets: list[str]) -> bool:
    """Empty targets = watch everything. Ids match exactly, names match loosely.

    Supergroup ids come back both bare (``1234567890``) and channel-prefixed
    (``-1001234567890``) depending on where you copied them from, so both forms
    are accepted. Note ``str.lstrip("-100")`` cannot do this — it strips
    *characters*, so it would eat into the id digits too.
    """
    if not targets:
        return True

    raw_id = str(chat_id or "")
    ids = {raw_id, raw_id.removeprefix("-100"), raw_id.removeprefix("-"), f"-100{raw_id}"}
    uname = (username or "").lstrip("@").lower()
    name = (title or "").lower()

    for target in targets:
        needle = target.strip().lstrip("@").lower()
        if not needle:
            continue
        if needle.lstrip("-").isdigit():
            if needle in ids:          # numeric target -> exact id match only
                return True
        elif needle == uname or needle in name:
            return True
    return False


def _may_forward() -> bool:
    """Cheap sliding-window cap so a busy Telegram group can't flood the Lark group."""
    now = time.monotonic()
    while _forward_window and now - _forward_window[0] > 60:
        _forward_window.popleft()
    if len(_forward_window) >= _MAX_LARK_FORWARDS_PER_MIN:
        return False
    _forward_window.append(now)
    return True


async def _handle_message(event) -> None:
    try:
        chat = await event.get_chat()
    except Exception:
        chat = None
    title = getattr(chat, "title", None) or "(private)"
    username = getattr(chat, "username", "") or ""
    chat_id = getattr(chat, "id", None) or getattr(event, "chat_id", None)

    if not _matches_target(chat_id, title, username, _watch_targets()):
        return

    try:
        sender = await event.get_sender()
        who = " ".join(
            p for p in (getattr(sender, "first_name", ""), getattr(sender, "last_name", "")) if p
        ).strip()
        if getattr(sender, "username", None):
            who = f"{who} (@{sender.username})".strip()
        who = who or str(getattr(sender, "id", "unknown"))
    except Exception:
        who = "unknown"

    text = (getattr(event, "raw_text", "") or "").strip()
    if not text:
        media = type(getattr(event.message, "media", None)).__name__
        text = f"(media: {media})" if media != "NoneType" else "(empty)"

    stamp = _now_str()
    with _lock:
        _state["messages_seen"] += 1
        _state["last_message_at"] = stamp
        _state["groups_seen"][title] = _state["groups_seen"].get(title, 0) + 1
        _state["recent"].append(
            {"at": stamp, "chat": title, "chat_id": chat_id, "sender": who, "text": text}
        )

    print(f"[telegram] {title} / {who}: {text[:160]}", flush=True)

    if _forward_enabled() and FORWARD_CHAT_ID:
        if _may_forward():
            try:
                send_text(
                    FORWARD_CHAT_ID,
                    f"📩 Telegram · {title}\n{who} — {stamp}\n{text}",
                )
                with _lock:
                    _state["forwarded"] += 1
            except Exception as err:
                print(f"[telegram] forward failed: {err!r}", flush=True)
        else:
            print("[telegram] forward rate cap hit, message logged only", flush=True)

    with _lock:
        due = _state["messages_seen"] % 10 == 0
    if due:
        _persist()


# ---------------------------------------------------------------------------
# Monitor loop
# ---------------------------------------------------------------------------
async def _run_client() -> None:
    from telethon import TelegramClient, events
    from telethon.errors import FloodWaitError

    # These live in telethon's generated rpcerrorlist and have moved between
    # releases. Resolve them by name so a rename degrades to "retryable error"
    # instead of an ImportError that would spin the reconnect loop forever.
    import telethon.errors as _tg_errors

    _fatal_auth_errors = tuple(
        err
        for err in (
            getattr(_tg_errors, name, None)
            for name in (
                "AuthKeyUnregisteredError",
                "AuthKeyDuplicatedError",
                "SessionRevokedError",
                "SessionExpiredError",
                "UserDeactivatedError",
                "UserDeactivatedBanError",
            )
        )
        if isinstance(err, type) and issubclass(err, BaseException)
    )

    api_id, api_hash = _api_creds()
    client = TelegramClient(str(SESSION_PATH), api_id, api_hash)

    _set(phase="connecting", detail="connecting to Telegram…")
    await client.connect()

    # NOTE: deliberately NOT client.start() — start() would prompt for the OTP on
    # stdin, and under systemd there is no stdin. An unauthorised session is a
    # login failure we report, not something we can silently repair.
    if not await client.is_user_authorized():
        raise PermissionError(
            "session not authorised (missing/expired/revoked) — "
            "run `python telegramwatch.py --login` on the server"
        )

    try:
        me = await client.get_me()
    except _fatal_auth_errors as err:
        raise PermissionError(f"session rejected by Telegram: {type(err).__name__}") from err
    who = " ".join(
        p for p in (getattr(me, "first_name", ""), getattr(me, "last_name", "")) if p
    ).strip()
    if getattr(me, "username", None):
        who = f"{who} (@{me.username})"
    if getattr(me, "phone", None):
        who = f"{who} +{me.phone}"

    _set(
        phase="monitoring",
        detail="listening for new group messages",
        account=who,
        connected_at=_now_str(),
        started_at=time.monotonic(),
        last_error=None,
        alerted=False,
    )
    _persist()
    print(f"[telegram] ✅ logged in as {who} — monitoring started", flush=True)

    @client.on(events.NewMessage(incoming=True))
    async def _on_new(event):  # noqa: ANN001 - telethon handler
        # Groups + supergroups only; skip 1:1 DMs and broadcast channels unless asked.
        if not (event.is_group or (event.is_channel and _truthy(os.getenv("TELEGRAM_WATCH_CHANNELS")))):
            return
        try:
            await _handle_message(event)
        except Exception as err:  # one bad message must not kill the listener
            print(f"[telegram] handler error: {err!r}", flush=True)

    try:
        await client.run_until_disconnected()
    except _fatal_auth_errors as err:
        raise PermissionError(f"session invalidated by Telegram: {type(err).__name__}") from err
    except FloodWaitError as err:
        raise RuntimeError(f"FLOOD_WAIT {err.seconds}s — backing off") from err


def _monitor_thread() -> None:
    """Own thread, own event loop, reconnect with backoff. Never raises out."""
    backoff = 30
    while True:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_run_client())
            # Clean disconnect (Telegram dropped us) — retry.
            _set(phase="connecting", detail="disconnected, reconnecting…")
        except PermissionError as err:
            # Authorisation problem: a retry loop cannot fix this, a human must.
            _set(phase="login_failed", detail=str(err), last_error=str(err))
            print(f"[telegram] ❌ login failed: {err}", flush=True)
            _alert_login_failed(str(err))
            return
        except Exception as err:
            msg = f"{type(err).__name__}: {err}"
            _set(phase="connecting", detail=f"error, retrying in {backoff}s", last_error=msg)
            print(f"[telegram] ⚠️ monitor error: {msg} — retry in {backoff}s", flush=True)
        finally:
            try:
                loop.close()
            except Exception:
                pass
        time.sleep(backoff)
        backoff = min(backoff * 2, 600)


def start_monitor_on_startup() -> None:
    """Boot hook for main.py. No-op unless TELEGRAM_WATCH_ENABLED is truthy."""
    global _thread
    if not _watch_enabled():
        print("[telegram] TELEGRAM_WATCH_ENABLED not set — watcher off", flush=True)
        _set(phase="idle", detail="disabled (TELEGRAM_WATCH_ENABLED not set)")
        return
    if _thread and _thread.is_alive():
        return

    _load_persisted()

    try:
        import telethon  # noqa: F401
    except Exception:
        detail = "telethon not installed — run `pip install telethon`"
        _set(phase="login_failed", detail=detail, last_error=detail)
        print(f"[telegram] ❌ {detail}", flush=True)
        _alert_login_failed(detail)
        return

    try:
        _api_creds()
    except RuntimeError as err:
        _set(phase="login_failed", detail=str(err), last_error=str(err))
        print(f"[telegram] ❌ {err}", flush=True)
        _alert_login_failed(str(err))
        return

    if not Path(str(SESSION_PATH) + ".session").exists():
        detail = (
            f"no session file ({SESSION_PATH.name}.session) — the one-time Telegram "
            "code cannot be typed in from a service; run `python telegramwatch.py --login`"
        )
        _set(phase="login_failed", detail=detail, last_error=detail)
        print(f"[telegram] ❌ {detail}", flush=True)
        _alert_login_failed(detail)
        return

    _thread = threading.Thread(target=_monitor_thread, daemon=True, name="telegram-watch")
    _thread.start()
    print("[telegram] watcher thread started", flush=True)


# ---------------------------------------------------------------------------
# CLI — interactive login / chat listing / status
# ---------------------------------------------------------------------------
def _cli_login() -> int:
    """One-time interactive login. Telegram's OTP makes this unavoidably manual."""
    from telethon import TelegramClient
    from telethon.errors import (
        PhoneCodeExpiredError,
        PhoneCodeInvalidError,
        PhoneNumberInvalidError,
        SessionPasswordNeededError,
    )

    api_id, api_hash = _api_creds()
    phone = (os.getenv("TELEGRAM_PHONE", "") or "").strip()
    if not phone:
        phone = input("Phone number (with country code, e.g. +60102693549): ").strip()

    client = TelegramClient(str(SESSION_PATH), api_id, api_hash)

    async def _flow() -> int:
        await client.connect()
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"✅ Already authorised as {me.first_name} (@{me.username}) — nothing to do.")
            return 0
        try:
            await client.send_code_request(phone)
        except PhoneNumberInvalidError:
            print(f"❌ Telegram rejected the phone number {phone!r}.")
            return 1
        print(f"📲 Telegram sent a login code to {phone} (check the Telegram app first, then SMS).")
        code = input("Login code: ").strip()
        try:
            await client.sign_in(phone=phone, code=code)
        except (PhoneCodeInvalidError, PhoneCodeExpiredError) as err:
            print(f"❌ Code rejected: {type(err).__name__}")
            return 1
        except SessionPasswordNeededError:
            # 2FA cloud password. Read from env if present, else prompt without echo
            # so it never lands in shell history.
            pwd = os.getenv("TELEGRAM_2FA_PASSWORD", "")
            if not pwd:
                import getpass

                pwd = getpass.getpass("Two-step verification password: ")
            try:
                await client.sign_in(password=pwd)
            except Exception as err:
                print(f"❌ 2FA password rejected: {type(err).__name__}: {err}")
                return 1
        me = await client.get_me()
        print(f"✅ Logged in as {me.first_name} (@{me.username}) +{me.phone}")
        print(f"   Session saved to {SESSION_PATH}.session — keep it, it IS the credential.")
        return 0

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(_flow())
    finally:
        try:
            loop.run_until_complete(client.disconnect())
        except Exception:
            pass
        loop.close()


def _cli_list_chats() -> int:
    """Print every group so you can copy an id into TELEGRAM_WATCH_CHATS."""
    from telethon import TelegramClient

    api_id, api_hash = _api_creds()
    client = TelegramClient(str(SESSION_PATH), api_id, api_hash)

    async def _flow() -> int:
        await client.connect()
        if not await client.is_user_authorized():
            print("❌ Not authorised — run `python telegramwatch.py --login` first.")
            return 1
        print(f"{'chat_id':>16}  {'type':<10}  title")
        print("-" * 72)
        async for dialog in client.iter_dialogs():
            if not (dialog.is_group or dialog.is_channel):
                continue
            kind = "group" if dialog.is_group else "channel"
            uname = f" @{dialog.entity.username}" if getattr(dialog.entity, "username", None) else ""
            print(f"{dialog.id:>16}  {kind:<10}  {dialog.title}{uname}")
        return 0

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(_flow())
    finally:
        try:
            loop.run_until_complete(client.disconnect())
        except Exception:
            pass
        loop.close()


def _cli_run() -> int:
    """Run the watcher in the foreground (debugging; main.py normally hosts it)."""
    os.environ["TELEGRAM_WATCH_ENABLED"] = "1"
    start_monitor_on_startup()
    if not (_thread and _thread.is_alive()):
        print("❌ watcher did not start — see the error above.")
        return 1
    try:
        while _thread.is_alive():
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n[telegram] stopped by user")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if "--login" in args:
        return _cli_login()
    if "--list-chats" in args:
        return _cli_list_chats()
    if "--run" in args:
        return _cli_run()
    if "--shot" in args:
        _load_persisted()
        path = render_status_png()
        print(f"status PNG -> {path}")
        return 0 if path else 1
    _load_persisted()
    print("\n".join(status_lines()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
