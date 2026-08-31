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
import queue
import re
import sys
import threading
import time
import uuid
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

# Same Lark group + person as the Telegram watcher alerts (telegramwarm.py:87).
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


# The house budget for one Lark message. Not invented here: findmachine.py has
# `_MAX_CHARS_PER_MESSAGE = 3500` with a `- 300` line reserve, and osmwatch.py
# packs blocks to the same 3500 — and both SPLIT into successive messages rather
# than truncating, which is exactly the behaviour needed here.
# 0 means NEVER split: /latestevo must arrive as exactly ONE Lark message, however
# long. Lark's text limit is far above a notice (an EVO batch runs ~7k chars), so a
# single post is the normal path. Splitting survives only as a RECOVERY route for
# the case where Lark actually rejects the body — arriving in two pieces beats
# losing content silently. Set TEAMS_LARK_TEXT_MAX to a positive size to force it.
_LARK_TEXT_MAX = int(os.getenv("TEAMS_LARK_TEXT_MAX", "0"))
_LARK_TEXT_RESERVE = 300
# Consulted only after a single send has already been rejected. 3500 is the house
# budget: findmachine.py's _MAX_CHARS_PER_MESSAGE, and osmwatch.py packs to it.
_LARK_RECOVERY_MAX = 3500

# A batch notice is separated by a run of equals signs on its own line — the same
# separator maintenance.split_evo_sd_batch_blocks keys on (r"\n={10,}\s*\n").
# Breaking THERE keeps every part a whole notice.
_EQ_SEP_RE = re.compile(r"^={10,}\s*$")


def _split_for_lark(text: str, *, limit: int | None = None) -> list[str]:
    """Split ``text`` into Lark-sized parts, losing nothing.

    Preference order for a break: an equals-run separator line, then a blank
    line, then any line boundary, then — only for a single line longer than the
    budget — a hard character slice. Splitting mid-line is the last resort
    because that is precisely the damage the old 1500-char cap did, severing a
    notice at "维护时间：2".
    """
    cap = _LARK_TEXT_MAX if limit is None else limit
    src = text or ""
    # cap <= 0 -> one message, unconditionally. This guard has to be explicit:
    # max(500, 0 - 300) would otherwise quietly split every notice at 500 chars.
    if cap <= 0:
        return [src] if src else []
    budget = max(500, cap - _LARK_TEXT_RESERVE)
    if len(src) <= budget:
        return [src] if src else []

    # Explode any over-long single line up front, so the packer below never has
    # to make that decision.
    units: list[str] = []
    for line in src.split("\n"):
        while len(line) > budget:
            units.append(line[:budget])
            line = line[budget:]
        units.append(line)

    parts: list[str] = []
    cur: list[str] = []
    cur_len = 0
    last_sep = -1    # index in `cur` just past the newest equals-run line
    last_blank = -1  # index in `cur` just past the newest blank line
    for unit in units:
        add = len(unit) + (1 if cur else 0)
        if cur and cur_len + add > budget:
            cut = (last_sep if last_sep > 0 else
                   last_blank if last_blank > 0 else len(cur))
            parts.append("\n".join(cur[:cut]).rstrip())
            cur = cur[cut:]
            cur_len = sum(len(x) + 1 for x in cur)
            last_sep = last_blank = -1
        cur.append(unit)
        cur_len += add
        if _EQ_SEP_RE.match(unit):
            last_sep = len(cur)
        elif not unit.strip():
            last_blank = len(cur)
    if cur:
        parts.append("\n".join(cur).rstrip())
    return [p for p in parts if p]


def _send_parts(chat_id: str, parts: list[str], label: str) -> list[dict]:
    """Post already-split parts, checking every response code."""
    total = len(parts)
    out: list[dict] = []
    for n, part in enumerate(parts, 1):
        # A single part is posted verbatim — no label, no part counter.
        body = part if total == 1 else (
            f"{label or 'Latest in Teams group'} — part {n}/{total}\n{part}"
        )
        resp = send_text(chat_id, body)
        if not isinstance(resp, dict) or resp.get("code") != 0:
            print(f"[teams] part {n}/{total} send failed: {resp}", flush=True)
        out.append(resp)
    return out


def send_text_parts(chat_id: str, text: str, *, label: str = "",
                    limit: int | None = None) -> list[dict]:
    """Send ``text`` as one or more Lark messages, checking every response.

    Lark reports an oversized or invalid body as HTTP 200 with ``code != 0`` and
    raises nothing, so an unchecked send fails completely silently — no group
    message and no journal line. alert_login_failed already checks ``code``;
    this follows it.
    """
    parts = _split_for_lark(text, limit=limit) or [""]
    out = _send_parts(chat_id, parts, label)
    # One message is the goal, but content must never be lost. If that single
    # post was rejected AND it was big enough for size to be a plausible cause,
    # retry it split rather than leaving the group with nothing at all.
    failed = not isinstance(out[0], dict) or out[0].get("code") != 0
    if len(parts) == 1 and failed and len(parts[0]) > _LARK_RECOVERY_MAX:
        print(f"[teams] single {len(parts[0])}-char send rejected; retrying it "
              f"split so nothing is lost", flush=True)
        return _send_parts(chat_id,
                           _split_for_lark(text, limit=_LARK_RECOVERY_MAX),
                           label)
    return out


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
# Created by whoever WANTS the profile while a long-lived holder has it. The warm
# watcher checks it every tick and stands down; see _yield_requested(). This is
# the only way a hand-run `python teamswatch.py --login` in a SEPARATE process
# can get the bot's watcher to let go of the browser without killing the bot.
_YIELD_PATH = _ROOT_DIR / (PROFILE_DIR.name + ".yield")


def _lock_holder() -> str:
    """Who holds the lock, for error messages. '' when nobody does."""
    try:
        return _LOCK_PATH.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return ""


def _touch_profile_lock() -> None:
    """Refresh the lock's mtime — the warm watcher's heartbeat.

    The watcher holds the lock for days while _LOCK_STALE_S is 300s, so without
    this its own lock would look abandoned after five minutes and the next caller
    would RECLAIM it. That is the one outcome _profile_lock exists to prevent:
    two Chromiums on one user-data-dir lose the login.
    """
    holder = _lock_holder()
    if _lock_token and holder and not holder.startswith(_lock_token):
        # Someone reclaimed it. Touching now would keep THEIR lock looking fresh
        # while we carry on believing it is ours.
        return
    try:
        os.utime(_LOCK_PATH, None)
    except OSError:
        pass


def _yield_requested() -> bool:
    return _YIELD_PATH.exists()


# Our own token, so _release_profile_lock can tell "my lock" from "the lock
# someone reclaimed from me". Without this, a release unlinks whatever file is
# present — so after one reclaim the two holders' releases delete each other's
# locks and the mutex silently stops mutexing.
_lock_token = ""


def _acquire_profile_lock(what: str, *, wait_s: int = 120, ask: bool = True) -> bool:
    """Take the profile lock. Returns True on success, False on timeout.

    ``ask`` creates _YIELD_PATH so a long-lived holder stands down. The warm
    watcher acquires with ``ask=False``: it is the polite party, and a watcher
    that asked the CLI holding the lock to yield would ping-pong forever.
    """
    global _lock_token
    deadline = time.monotonic() + max(0, wait_s)
    asked = False
    while True:
        token = f"{os.getpid()}:{uuid.uuid4().hex[:8]}"
        try:
            fd = os.open(str(_LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"{token} {what} {_now_str()}".encode())
            os.close(fd)
            _lock_token = token
            if asked:
                _YIELD_PATH.unlink(missing_ok=True)
            return True
        except FileExistsError:
            pass
        if ask and not _YIELD_PATH.exists():
            try:
                _YIELD_PATH.write_text(f"{os.getpid()} {what} {_now_str()}\n",
                                       encoding="utf-8")
                if not asked:
                    print(f"[teams] profile held by {_lock_holder()!r} — asked it "
                          f"to yield for {what}", flush=True)
                asked = True
            except OSError:
                pass
        try:
            age = time.time() - _LOCK_PATH.stat().st_mtime
        except OSError:
            continue    # vanished between the two calls — retry immediately
        if age > _LOCK_STALE_S:
            # Only reachable once the holder stopped heartbeating, i.e. it really
            # died: a live warm watcher touches the file every _HEARTBEAT_S.
            print(f"[teams] reclaiming stale profile lock ({int(age)}s old, held by "
                  f"{_lock_holder()!r})", flush=True)
            _LOCK_PATH.unlink(missing_ok=True)
            continue
        if time.monotonic() >= deadline:
            if asked:
                _YIELD_PATH.unlink(missing_ok=True)
            return False
        time.sleep(2)


def _release_profile_lock() -> None:
    """Drop the lock, but only if it is still OURS.

    A reclaim (see _LOCK_STALE_S) hands the lock to someone else while we still
    think we hold it. Unlinking unconditionally would then delete THEIR lock and
    leave two Chromiums free to open the same profile — the exact failure the lock
    exists to prevent, made permanent.
    """
    global _lock_token
    mine = _lock_token
    _lock_token = ""
    if not mine:
        return
    holder = _lock_holder()
    if holder and not holder.startswith(mine):
        print(f"[teams] NOT releasing {_LOCK_PATH.name}: it now belongs to "
              f"{holder!r}, not to us ({mine}) — our lock was reclaimed",
              flush=True)
        return
    _LOCK_PATH.unlink(missing_ok=True)


@contextlib.contextmanager
def _profile_lock(what: str):
    """Serialise access to the Chromium profile.

    Two Chromium processes on one user-data-dir is unsupported: the second either
    refuses to start or writes over the first's session, which loses the login.
    That collision is easy to hit here because /teamstatus opens the profile on a
    timer while someone may be running --login by hand. Stale locks (older than
    _LOCK_STALE_S, e.g. left by a killed process) are reclaimed.
    """
    if not _acquire_profile_lock(what):
        raise RuntimeError(
            f"profile is busy — {_lock_holder() or 'another teamswatch run'} holds "
            f"{_LOCK_PATH.name}"
        )
    try:
        yield
    finally:
        _release_profile_lock()


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
_shot_lock = threading.Lock()
# SHOTS_DIR had no pruning anywhere and _shot_n never resets, so every login
# attempt and every watcher failure added PNGs that nothing ever removed.
_SHOTS_KEEP = int(os.getenv("TEAMS_SHOTS_KEEP", "60"))


def _prune_shots() -> None:
    """Keep only the newest _SHOTS_KEEP screenshots."""
    if _SHOTS_KEEP <= 0:
        return
    try:
        pngs = sorted(SHOTS_DIR.glob("*.png"), key=lambda f: f.stat().st_mtime)
    except OSError:
        return
    for stale in pngs[:-_SHOTS_KEEP]:
        try:
            stale.unlink()
        except OSError:
            pass


def _shot(page, label: str) -> str | None:
    """Screenshot every stage so a failed login is diagnosable from the PNGs."""
    global _shot_n
    with _shot_lock:
        _shot_n += 1
        n = _shot_n
    SHOTS_DIR.mkdir(parents=True, exist_ok=True)
    path = SHOTS_DIR / f"{n:02d}_{label}.png"
    try:
        page.screenshot(path=str(path), full_page=False)
        _set(last_shot=str(path))
        _prune_shots()
        return str(path)
    except Exception as err:
        print(f"[teams] screenshot '{label}' failed: {err!r}", flush=True)
        return None


def _shot_fixed(page, label: str) -> str | None:
    """Screenshot to a STABLE filename, overwriting the previous one.

    _shot() numbers every PNG and never deletes — right for a one-shot login
    trace, wrong for a loop: a 60s poll that shot each cycle would leave 1,440
    files a day on the server. The warm watcher passes this instead, so a
    persistent failure keeps exactly one current PNG per failure kind.
    """
    SHOTS_DIR.mkdir(parents=True, exist_ok=True)
    path = SHOTS_DIR / f"warm_{label}.png"
    try:
        page.screenshot(path=str(path), full_page=False)
        _set(last_shot=str(path))
        _prune_shots()
        return str(path)
    except Exception as err:
        print(f"[teams] screenshot '{label}' failed: {err!r}", flush=True)
        return None


# The warm watcher reads the pane once a minute, and the routine progress prints
# below would be ~3,000 journal lines a day. Set while a warm poll is in flight so
# only the abnormal ones survive; WARNINGs are never suppressed.
_quiet_reads = False


def _rprint(msg: str) -> None:
    """print() for the read path's routine progress lines."""
    if not _quiet_reads:
        print(msg, flush=True)


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
    _set(phase="logging_in", detail="waiting for the profile", last_error=None,
         alerted=False)

    result: dict[str, Any] = {"ok": False, "stage": "start", "reason": None, "shot": None}
    typed_email = typed_password = False
    passkey_tries = other_ways_tries = password_option_tries = 0
    # `deadline` is set INSIDE the with-block, after the lock is in hand. The warm
    # watcher holds the profile until it notices the yield request, and billing
    # that wait against the login budget produced a "timed out" result — plus a
    # LOGIN FAILED alert — for a login that had not started typing yet.
    deadline = 0.0

    with _profile_lock("login"), sync_playwright() as p:
        _set(detail="starting browser", started_at=time.monotonic())
        deadline = time.monotonic() + _login_timeout_s()
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


DOM_PATH = _ROOT_DIR / "teamswatch_dom.json"

_DUMP_DOM_JS = r"""
(maxRowH) => {
    const out = {url: location.href, hash: location.hash, title: document.title,
                 sidebar: [], selected: [], header: [], main: [], pane: []};
    const attrs = (el) => {
        const o = {tag: el.tagName.toLowerCase()};
        for (const a of el.attributes) {
            if (/^(class|style)$/i.test(a.name)) continue;
            o[a.name] = String(a.value).slice(0, 90);
        }
        const cls = (el.className || '').toString();
        if (cls) o._class = cls.slice(0, 120);
        const r = el.getBoundingClientRect();
        o._box = [Math.round(r.width), Math.round(r.height)];
        o._text = (el.innerText || '').trim().split('\n')[0].slice(0, 70);
        return o;
    };

    // (a) every candidate chat row, flagged for nesting
    const rowSels = ["[data-tid='chat-list-item']", "[data-tid^='chat-list-item']",
                     "[role='treeitem']", "[role='listitem']", "[role='option']"];
    for (const s of rowSels) {
        const all = [...document.querySelectorAll(s)];
        out.sidebar.push({selector: s, total: all.length});
        for (const el of all.slice(0, 40)) {
            const a = attrs(el);
            a._selector = s;
            a._nestedMatches = el.querySelectorAll(s).length;
            out.sidebar.push(a);
        }
    }

    // (c) whatever marks the ACTIVE conversation
    const selSels = ["[aria-selected='true']", "[aria-current]", "[data-selected]",
                     "[class*='selected']", "[class*='Selected']",
                     "[class*='active']", "[class*='Active']"];
    for (const s of selSels) {
        for (const el of [...document.querySelectorAll(s)].slice(0, 8)) {
            const a = attrs(el); a._selector = s; out.selected.push(a);
        }
    }

    // (b) the header, found STRUCTURALLY: walk up from the rename/edit control,
    // since the title is a rename button here, not a heading.
    const pencils = [...document.querySelectorAll(
        "button,[role='button'],[contenteditable='true']")].filter(el => {
        const lab = ((el.getAttribute('aria-label') || '') + ' ' +
                     (el.getAttribute('title') || '')).toLowerCase();
        return /edit|rename|name/.test(lab);
    });
    for (const pen of pencils.slice(0, 4)) {
        let node = pen, hops = 0;
        while (node && hops < 5) {
            const a = attrs(node);
            a._hopsFromEditControl = hops;
            out.header.push(a);
            node = node.parentElement; hops++;
        }
    }
    // Any big-font visible text near the top of the main area is a title candidate.
    for (const el of [...document.querySelectorAll('h1,h2,[role="heading"],span,div')]) {
        const r = el.getBoundingClientRect();
        if (r.top > 160 || r.height < 18 || r.height > 60 || r.width < 80) continue;
        const fs = parseFloat(getComputedStyle(el).fontSize || '0');
        if (fs < 17) continue;
        const t = (el.innerText || '').trim();
        if (!t || t.length > 90 || el.children.length > 2) continue;
        const a = attrs(el); a._fontSize = fs; a._byFontSize = true;
        out.header.push(a);
        if (out.header.length > 60) break;
    }

    // (d) main region + message pane candidates
    for (const s of ["[data-tid='app-layout-area--main']", "[role='main']", 'main',
                     "[data-tid='chat-pane']"]) {
        for (const el of [...document.querySelectorAll(s)].slice(0, 3)) {
            const a = attrs(el); a._selector = s;
            a._hasChatList = !!el.querySelector("[data-tid='chat-list'],[data-tid='chat-list-item']");
            out.main.push(a);
        }
    }
    for (const s of ["[data-tid='message-pane']", "[data-tid='messages-pane']",
                     "[role='log']", "[data-tid='chat-pane-list']",
                     "[data-tid='chat-pane-item']", "[data-tid='chat-pane-message']"]) {
        const all = [...document.querySelectorAll(s)];
        out.pane.push({selector: s, total: all.length});
        for (const el of all.slice(0, 3)) {
            const a = attrs(el); a._selector = s; out.pane.push(a);
        }
    }
    return out;
}
"""


def dump_dom(*, group: str | None = None, headless: bool = True) -> dict:
    """Dump the live sidebar / header / pane structure to teamswatch_dom.json.

    Exists because this DOM has been guessed at four times and been wrong four
    times: the header title turned out to be a rename control rather than a
    heading, and both the header and selected-row lookups returned empty. This
    reports what is actually there — including the conversation id to put in
    EVOTEAMS_THREAD_ID, which removes title matching from the critical path.
    """
    from playwright.sync_api import sync_playwright

    target = (group or os.getenv("EVOTEAMS_GROUP")
              or "@EVO C88live/slot_ow.ph (RTS) CS Group NE RT FP")
    out: dict[str, Any] = {"ok": False, "target": target}

    with _profile_lock("dump-dom"), sync_playwright() as p:
        ctx, page = _open(p, headless=headless)
        try:
            page.goto(TEAMS_URL, wait_until="domcontentloaded", timeout=60000)
            deadline = time.monotonic() + max(60, int(os.getenv("TEAMS_BOOT_WAIT", "90")))
            while time.monotonic() < deadline:
                if _stage_of(page) == "teams_loaded":
                    break
                page.wait_for_timeout(2000)
            page.wait_for_timeout(5000)

            # Try to open the target, but dump either way — a failed open is
            # exactly the state we need to inspect.
            opened = _open_group(page, target)
            page.wait_for_timeout(3000)
            data = page.evaluate(_DUMP_DOM_JS, _MAX_ROW_HEIGHT) or {}
            data["opened_ok"] = opened
            data["thread_id_seen"] = _open_thread_id(page)
            data["confirm"] = list(_confirm_open_chat(page, target))
            out.update(data)
            out["ok"] = True
            out["shot"] = _shot(page, "dump_dom")
        except Exception as err:  # noqa: BLE001
            out["error"] = repr(err)
        finally:
            try:
                ctx.close()
            except Exception:
                pass

    try:
        DOM_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2),
                            encoding="utf-8")
    except Exception as err:  # noqa: BLE001
        print(f"[teams] dom dump write failed: {err!r}", flush=True)

    print(f"\n[teams] URL      : {out.get('url', '')[:150]}")
    print(f"[teams] THREAD ID: {out.get('thread_id_seen') or '<none in URL>'}")
    print(f"[teams] opened_ok: {out.get('opened_ok')}")
    print(f"[teams] confirm  : {(out.get('confirm') or ['', ''])[1]}")
    for section in ("sidebar", "selected", "header", "main", "pane"):
        rows = out.get(section) or []
        print(f"\n--- {section} ({len(rows)} entries) ---")
        for row in rows[:14]:
            if "selector" in row and "total" in row:
                print(f"   COUNT {row['total']:>4}  {row['selector']}")
                continue
            bits = {k: v for k, v in row.items()
                    if k in ("tag", "_text", "_box", "_nestedMatches", "_selector",
                             "_hopsFromEditControl", "_fontSize", "_hasChatList")}
            print(f"   {bits}")
    print(f"\n[teams] full dump -> {DOM_PATH}")
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
# Confirmed against a DOM capture of the live build (2026-08-18). `#chat-pane-list`
# is the message RUNWAY and is listed first on purpose: the pinned-message banner
# ("Ina Huang … 親愛的團隊您好 …", pinned back in May) lives in the pane *outside* the
# runway, so anchoring here drops it without needing a text rule. Everything after
# is a fallback for older/renamed builds.
_MSG_PANE_SELS = [
    "#chat-pane-list",
    "[data-tid='message-pane-list-runway']",
    "[data-tid='message-pane-list-viewport']",
    "[data-tid='message-pane-body']",
    "[data-tid='message-pane-layout']",
    "[data-tid='message-pane']",
    "[data-tid='messages-pane']",
    "[data-tid='chat-pane-list']",
    "[role='log']",
]
# The element that actually scrolls. The runway does not — it is the full-height
# content — so scrolling must target its viewport ancestor.
_MSG_SCROLL_SELS = [
    "[data-tid='message-pane-list-viewport']",
    "[data-view='message-pane-list-viewport']",
]
# The message body as its own node. `[data-message-content]` is this build's marker
# (`<div id="content-1786918775349" data-message-content aria-label="…">`); taking
# the body from here rather than the row's innerText is what keeps the author name,
# the timestamp, the "Translate" button and the reaction summary
# ("1 Like reaction with light skin tone.") out of the generated email.
_MSG_BODY_SELS = [
    "[data-message-content]",
    "[id^='content-']",
    "[data-tid='message-body']",
    "[data-tid='messageBodyContent']",
    ".fui-ChatMessage__body",
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


# The conversation header, which names the chat that is ACTUALLY open. Ordered
# specific-first, because the generic heading selectors also match the left
# sidebar's own "Chat" heading — which is exactly what an earlier version
# returned, making every comparison fail with "Teams is showing 'Chat'".
_CHAT_TITLE_SELS = [
    # This build's real header, confirmed from a DOM capture: an <h2>, not an <h1>
    # — which is why every h1-based guess came back empty and the generic ones
    # matched the sidebar's own "Chat" heading instead.
    "[data-tid='chat-title']",
    "[data-tid='chat-header-title']",
    "[data-tid='chatHeaderTitle']",
    "[data-tid='entity-header'] [role='heading']",
    "[data-tid='chat-pane-header'] [role='heading']",
    "[data-tid='conversation-header'] [role='heading']",
    "[role='main'] [role='heading']",
    "[role='main'] h1",
    "[role='heading'][aria-level='1']",
    "h1",
]

# App chrome that is never a chat name. Any of these means we matched the shell
# rather than the conversation header.
_TITLE_CHROME = {
    "chat", "chats", "teams", "activity", "calendar", "calls", "files", "apps",
    "more", "search", "communities", "feed", "microsoft teams",
}


# A Teams conversation id, e.g. 19:abc...@thread.v2 or 19:abc...@unq.gbl.spaces.
# Opaque, unique, unlocalised, untruncated, and unchanged by a rename — so unlike
# a 20-char title prefix it has no partial-match failure mode. This is the signal
# to trust; the title is a fallback.
_THREAD_ID_RE = re.compile(r"19:[A-Za-z0-9_\-+=/.]+@(?:thread\.v2|thread\.skype|unq\.gbl\.spaces)")


# Where the open conversation's id is written into the DOM on this build. Taken
# from a real capture rather than guessed:
#   <div id="chat-header-19:29c60453c7fa48b59d142e97f7272963@thread.skype">
#   <button data-tid="sendMessageCommands-send" data-track-thread-id="19:…">
# The compose send button is the strongest of the two: it names the thread a
# message would actually be posted to, so it cannot be stale relative to the pane.
_OPEN_THREAD_JS = r"""
    () => {
        const send = document.querySelector('[data-track-thread-id]');
        const fromSend = send ? (send.getAttribute('data-track-thread-id') || '') : '';
        if (fromSend) return fromSend;
        const hdr = document.querySelector("[id^='chat-header-19:']");
        if (hdr) return hdr.id.slice('chat-header-'.length);
        return '';
    }
"""


def _open_thread_id(page) -> str:
    """Conversation id of the chat currently on screen.

    Reads the DOM first and the URL second. The URL is not reliable here — Teams
    keeps `teams.live.com/v2/` in the address bar while switching conversations
    client-side, so a URL-only read returns nothing and made id verification
    impossible to satisfy.
    """
    from urllib.parse import unquote

    try:
        found = (page.evaluate(_OPEN_THREAD_JS) or "").strip()
    except Exception:
        found = ""
    if found:
        match = _THREAD_ID_RE.search(found)
        if match:
            return match.group(0)

    try:
        url = unquote(page.url or "")
    except Exception:
        url = page.url or ""
    match = _THREAD_ID_RE.search(url)
    return match.group(0) if match else ""


# Conversation ids we have positively identified, keyed by normalised title. An id
# is opaque and unchanged by a rename, so verifying against one removes every
# partial-match failure mode the title comparison had (truncation, the archived
# 【关闭】 clone, a header that read as just "@").
_KNOWN_THREAD_IDS = {
    "@evo c88live/slot_ow.ph (rts) cs group ne rt fp":
        "19:29c60453c7fa48b59d142e97f7272963@thread.skype",
}


def _wanted_thread_id(title: str = "") -> str:
    """The id we require the open chat to have, or "" to fall back to titles.

    EVOTEAMS_THREAD_ID overrides, so a renamed or re-created group can be pointed
    at without a code change. Otherwise this only answers for titles we have
    actually confirmed — asking for an arbitrary --group must not inherit the EVO
    group's id.
    """
    env = (os.getenv("EVOTEAMS_THREAD_ID", "") or "").strip()
    if env:
        return env
    return _KNOWN_THREAD_IDS.get(_norm_title(title), "")


def _selected_chat_title(page) -> str:
    """Title of the sidebar row Teams marks as active.

    An independent read on "which conversation is open" that needs no knowledge
    of the header's markup — the header lookup returned an empty string on the
    live build, so relying on it alone left us blind. Only row-sized nodes are
    considered, for the same container reason as _click_chat_row.
    """
    try:
        return (page.evaluate(
            """(maxH) => {
                // LeftRailSelectedItem is how this build tags the active row (it
                // is listed in the row's data-tabster "observed names"); the rest
                // are fallbacks for other builds.
                const sels = ["[data-tabster*='LeftRailSelectedItem']",
                              "[aria-selected='true']", "[aria-current='page']",
                              "[aria-current='true']", "[aria-current='location']",
                              "[class*='selected']", "[class*='isSelected']"];
                for (const s of sels) {
                    for (const el of document.querySelectorAll(s)) {
                        const r = el.getBoundingClientRect();
                        if (!r.height || r.height > maxH) continue;
                        // The row carries the untruncated chat name in a dedicated
                        // span: <span id="title-chat-list-item_19:…">@EVO …</span>.
                        const named = el.querySelector("[id^='title-chat-list-item_']");
                        if (named) {
                            const n = (named.innerText || '').trim();
                            if (n) return n;
                        }
                        const t = (el.innerText || '').trim().split('\\n')[0].trim();
                        if (t) return t;
                    }
                }
                return '';
            }""",
            _MAX_ROW_HEIGHT,
        ) or "").strip()
    except Exception:
        return ""


def _open_chat_title(page) -> str:
    """Best available answer to "which chat is on screen?".

    Tries the conversation header first, then falls back to the selected sidebar
    row. Two independent signals, because each has failed on its own: the header
    selectors matched nothing on the live build, and the generic ones matched the
    sidebar's own "Chat" heading.
    """
    try:
        found = (page.evaluate(
            """([sels, chrome]) => {
                for (const s of sels) {
                    for (const el of document.querySelectorAll(s)) {
                        if (!(el.offsetParent || el.offsetWidth || el.offsetHeight))
                            continue;
                        // Teams truncates the rendered header with an ellipsis but
                        // keeps the full name in title=, and a truncated title is
                        // exactly what an anchored prefix comparison cannot judge.
                        const withTitle = el.matches('[title]')
                            ? el : el.querySelector('[title]');
                        let t = withTitle
                            ? (withTitle.getAttribute('title') || '').trim() : '';
                        if (!t)
                            t = (el.innerText || '').trim().split('\\n')[0].trim();
                        if (!t) continue;
                        if (chrome.includes(t.toLowerCase())) continue;
                        return t;
                    }
                }
                return '';
            }""",
            [_CHAT_TITLE_SELS, sorted(_TITLE_CHROME)],
        ) or "").strip()
    except Exception:
        found = ""
    if found:
        return found
    picked = _selected_chat_title(page)
    if picked:
        print(f"[teams] header not found; using selected sidebar row: {picked!r}",
              flush=True)
    return picked


_TITLE_MIN_CHARS = 12
# Archived/closed clones of a group carry these markers and are DIFFERENT chats.
# This account demonstrably has 【关闭】ZF918(B) variants alongside live ones, so a
# title differing only by such a marker must never be treated as a match.
_CLOSED_MARKER_RE = re.compile(r"(?i)【关闭】|\[关闭\]|closed|停用|已关闭|archived")


def _norm_title(text: str) -> str:
    # Strip bidi marks and a trailing ellipsis: Teams truncates long titles and
    # RTL/LRM marks ride along invisibly, both of which break naive comparison.
    cleaned = re.sub(r"[‎‏‪-‮⁦-⁩]", "", text or "")
    cleaned = re.sub(r"[\s ]+", " ", cleaned).strip()
    cleaned = re.sub(r"(?:\.{3}|…)\s*$", "", cleaned).strip()
    return cleaned.lower()


def _titles_match(want: str, got: str) -> bool:
    """Anchored prefix comparison, failing CLOSED.

    Previously ended in ``head in b or a.startswith(b[:20])``, which accepted a
    header of just ``"@"`` and accepted 「【关闭】@EVO …」 — the archived clone of
    the target — as the target itself. Since a false match here means emailing
    another group's maintenance notice, both sides must now be substantial and
    one must be an anchored prefix of the other.
    """
    a, b = _norm_title(want), _norm_title(got)
    if len(a) < _TITLE_MIN_CHARS or len(b) < _TITLE_MIN_CHARS:
        return False
    # A closed/archived marker on one side only makes these different chats.
    if bool(_CLOSED_MARKER_RE.search(a)) != bool(_CLOSED_MARKER_RE.search(b)):
        return False
    return a.startswith(b) or b.startswith(a)


def _confirm_open_chat(page, title: str) -> tuple[bool, str]:
    """Is ``title`` the conversation on screen? Returns ``(ok, human_reason)``.

    Three independent signals, because every single one of them has already
    failed on this build:
      - thread id from the URL vs EVOTEAMS_THREAD_ID — the only one that cannot
        half-match; authoritative when configured
      - the conversation header title — its selectors matched nothing here
      - the selected sidebar row — also came back empty here

    The reason string is always populated, so a failure names what was tried
    instead of reporting a bare empty title (which is what "could not open the
    target group" with no chat name meant).
    """
    thread = _open_thread_id(page)
    wanted = _wanted_thread_id(title)
    if wanted and thread:
        # Both known: this is decisive in BOTH directions. A mismatch is a hard
        # failure — it is precisely the case where we would otherwise scrape and
        # email another group's maintenance notice.
        if thread == wanted:
            return True, f"thread id matches ({thread[:28]}…)"
        return False, (f"thread id mismatch: on {thread[:28]}…, "
                       f"want {wanted[:28]}…")
    if wanted and not thread:
        # We know which id to want but the page exposes none. Do NOT hard-fail:
        # that would make a single Teams markup rename break reading entirely,
        # even with a correct chat on screen. Fall through to the title checks —
        # no worse than before ids existed, and those are now fail-closed.
        print(f"[teams] no conversation id in the DOM (want {wanted[:28]}…); "
              f"falling back to title comparison", flush=True)

    header = _open_chat_title(page)
    if _titles_match(title, header):
        return True, f"header title matches ({header!r})"

    picked = _selected_chat_title(page)
    if _titles_match(title, picked):
        return True, f"selected sidebar row matches ({picked!r})"

    bits = [
        f"header={header!r}" if header else "header=<none found>",
        f"selected-row={picked!r}" if picked else "selected-row=<none found>",
        f"url-thread={thread[:28] + '…' if thread else '<none>'}",
    ]
    hint = ""
    if thread and not wanted:
        # We are plainly inside *a* conversation but have nothing to compare it
        # against — this is the case to fix by storing the id once.
        hint = (f"  -> set EVOTEAMS_THREAD_ID={thread} in .env to verify by id "
                f"(run --dump-dom to confirm it is the right chat)")
    return False, "; ".join(bits) + hint


def _row_needle(title: str) -> str:
    """Longest whitespace-free fragment of a title, for text matching.

    e.g. "@EVO C88live/slot_ow.ph (RTS) CS Group…" -> "c88live/slot_ow.ph".
    Distinctive, and free of the whitespace that makes Playwright's has_text and
    inner_text disagree.
    """
    parts = re.split(r"[\s ]+", _norm_title(title))
    parts = [p for p in parts if len(p) >= 6]
    return max(parts, key=len) if parts else _norm_title(title)[:20]


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

    ok, why = _confirm_open_chat(page, needle)
    print(f"[teams] on arrival: {why}", flush=True)
    if ok:
        print("[teams] target chat already open", flush=True)
        page.wait_for_timeout(3000)
        return True
    before = _open_chat_title(page)

    # Match on a distinctive whitespace-free fragment, not needle[:24].
    # `has_text` and `inner_text()` use different text models — has_text includes
    # hidden text and normalises NBSP, inner_text excludes hidden text and keeps
    # NBSP/LRM — so a needle carrying spaces can make the filter and the
    # verification disagree and skip the row we actually want.
    prefix = _row_needle(needle)
    seen = before
    thread = _wanted_thread_id(needle)
    # Activation, best signal first:
    #   1. click the row keyed on the conversation id — no text matching at all
    #   2. Enter on a text-matched row (geometry-free)
    #   3. a real click on a text-matched row
    # press() silently does nothing on a node that ignores the key, so the only
    # honest test of any of these is whether the conversation actually changed.
    methods = (("thread", "enter", "click", "enter") if thread
               else ("enter", "click", "enter"))
    for attempt, method in enumerate(methods, start=1):
        if method == "thread":
            activated = _click_chat_row_by_thread(page, thread)
        else:
            activated = _click_chat_row(page, prefix, method=method)
        if not activated:
            label = thread[:28] + "…" if method == "thread" else repr(prefix)
            print(f"[teams] sidebar row not found for {label} "
                  f"(attempt {attempt}/{len(methods)}, method={method})", flush=True)
            page.wait_for_timeout(2000)
            continue
        # Wait for the HEADER to become the target — not merely for a pane to
        # exist. A pane is already on screen from the restored chat, so "a pane
        # appeared" proves nothing about which chat we are looking at.
        deadline = time.monotonic() + 25
        why = ""
        while time.monotonic() < deadline:
            ok, why = _confirm_open_chat(page, needle)
            if ok:
                page.wait_for_timeout(4000)  # let the virtualised list settle
                print(f"[teams] confirmed open — {why}", flush=True)
                return True
            page.wait_for_timeout(1500)
        seen = why
        print(f"[teams] attempt {attempt}/{len(methods)} unconfirmed — {why}",
              flush=True)

    print(f"[teams] could not confirm the target chat — {seen}", flush=True)
    return False


_CHAT_ROW_SELS = [
    "[data-tid='chat-list-item']",
    "[data-tid^='chat-list-item']",
    "[role='treeitem'][data-item-type='chat']",
    "[data-testid='list-item']",
    "[role='treeitem']",
    "[role='listitem']",
]


def _click_chat_row_by_thread(page, thread: str) -> bool:
    """Activate the sidebar row for a CONVERSATION ID, not for text.

    Every sidebar row on this build is keyed on the thread id — confirmed from a
    DOM capture:

      <div role="treeitem" data-item-type="chat"
           data-fui-tree-item-value="…|OneGQL_GroupChatConversation|19:29c6…@thread.skype"
           data-tabster='{"observed":{"names":["19:29c6…@thread.skype"]}}'>
        <div data-inp="simple-collab-chat-switch"> … </div>

    That removes every failure mode the text path had: no truncated title, no
    NBSP/LRM mismatch between has_text and inner_text, no archived 【关闭】 clone
    sharing a prefix, and no outer container that merely *contains* the title and
    whose centre lands on a mid-list chat.
    """
    thread = (thread or "").strip()
    if not thread:
        return False
    esc = thread.replace("\\", "\\\\").replace('"', '\\"')
    sels = [
        f'[role="treeitem"][data-fui-tree-item-value$="{esc}"]',
        f'[data-testid="list-item"][data-fui-tree-item-value$="{esc}"]',
        f'[role="treeitem"][data-tabster*="{esc}"]',
        f'[data-fui-tree-item-value$="{esc}"]',
    ]
    for sel in sels:
        try:
            row = page.locator(sel).first
            if not row.count():
                continue
            row.scroll_into_view_if_needed(timeout=10000)
            page.wait_for_timeout(500)
            # Click the row's own switch element when present: the treeitem is the
            # focus/DnD wrapper, the switch is what Teams binds the navigation to.
            inner = row.locator("[data-inp='simple-collab-chat-switch']").first
            target = inner if inner.count() else row
            target.click(timeout=15000)
            print(f"[teams] activated sidebar row by conversation id "
                  f"({thread[:28]}…) via {sel}", flush=True)
            return True
        except Exception as err:  # noqa: BLE001
            print(f"[teams] id click via {sel} failed: {err!r}", flush=True)
            continue
    return False


# A real chat row is one avatar tall (~50-72px observed). Anything much taller is
# a container, not a row.
_MAX_ROW_HEIGHT = 140


def _click_chat_row(page, prefix: str, method: str = "enter") -> bool:
    """Click the sidebar row for ``prefix`` — the ROW, never its container.

    This was the bug that kept opening the wrong conversation, and it needs three
    independent checks, because each alone is fooled:

    1. ``filter(has_text=…)`` matches ANY element whose subtree contains the
       text, so an outer ``[role=listitem]`` wrapping the whole chat list matches
       too — and ``.first`` is that wrapper (measured: 1040px tall).
    2. Playwright clicks an element's CENTRE. Clicking a 1040px wrapper therefore
       clicks whatever row sits at the list's vertical middle — reproducibly a
       mid-list chat (measured: hit "Ecomm|TELNOVO" while aiming 4 rows away).
    3. Re-reading the node's text does NOT catch this, because the wrapper's text
       legitimately contains the target title.

    So: reject anything too tall to be a row, and require the title to be in the
    row's FIRST line — a wrapper's first line is the first chat's name, not ours.
    """
    needle = (prefix or "").strip().lower()
    if not needle:
        return False

    for sel in _CHAT_ROW_SELS:
        try:
            rows = page.locator(sel).filter(has_text=prefix)
            count = rows.count()
        except Exception as err:  # noqa: BLE001
            print(f"[teams] locating {sel} failed: {err!r}", flush=True)
            continue
        if not count:
            continue

        for i in range(min(count, 12)):
            try:
                cand = rows.nth(i)
                box = cand.bounding_box()
                if not box:
                    continue
                height = box.get("height") or 0
                if height > _MAX_ROW_HEIGHT:
                    print(f"[teams] skipping container match via {sel} "
                          f"(height {int(height)}px > {_MAX_ROW_HEIGHT})", flush=True)
                    continue
                text = (cand.inner_text(timeout=5000) or "").strip()
                first = (text.splitlines() or [""])[0].strip()
                if needle not in first.lower():
                    print(f"[teams] skipping match whose title line is "
                          f"{first[:44]!r} (not our chat)", flush=True)
                    continue

                cand.scroll_into_view_if_needed(timeout=10000)
                page.wait_for_timeout(600)  # settle; the list is virtualised
                # Re-read after scrolling: Teams recycles row nodes, so the node
                # can now be showing a different conversation entirely.
                again = (cand.inner_text(timeout=5000) or "").strip()
                again_first = (again.splitlines() or [""])[0].strip()
                if needle not in again_first.lower():
                    print(f"[teams] row recycled under us (now "
                          f"{again_first[:44]!r}) — retrying", flush=True)
                    continue

                print(f"[teams] activating sidebar row {first[:60]!r} "
                      f"via {sel} (h={int(height)}px, method={method})", flush=True)
                if method == "enter":
                    # Keyboard activation takes geometry out of the equation —
                    # a click targets the element's CENTRE, which is what let a
                    # container match open a mid-list chat. But press() does NOT
                    # raise when the node ignores the key, so success cannot be
                    # inferred here; _open_group alternates methods across its
                    # attempts and judges by whether the chat actually changed.
                    cand.press("Enter", timeout=10000)
                else:
                    cand.click(timeout=15000)
                return True
            except Exception as err:  # noqa: BLE001
                print(f"[teams] candidate {i} via {sel} failed: {err!r}", flush=True)
                continue
    return False


# The conversation region. [data-tid='app-layout-area--main'] is this build's
# main area — proven by _TEAMS_IN_SELS already matching it — and `role=main` is
# NOT guaranteed here, so it cannot be the only candidate.
_MAIN_REGION_SELS = [
    "[data-tid='app-layout-area--main']",
    "[role='main']",
    "[data-tid='chat-pane']",
    "main",
]
# Anything that identifies the LEFT SIDEBAR. A resolved pane containing one of
# these is the chat list, not the message list.
_SIDEBAR_MARK_SELS = [
    # Real markers on this build, from the DOM capture.
    "[data-testid='simple-collab-dnd-rail']",
    "[data-tid='simple-collab-dnd-rail']",
    "[data-testid='simple-collab-rail']",
    "[data-tid='app-layout-area--mid-nav']",
    "[data-tid='app-layout-area--nav']",
    "[data-testid='list-item']",
    "[data-inp='simple-collab-chat-switch']",
    "[data-tid='chat-list']",
    "[data-tid='chat-list-item']",
    "[data-tid='app-bar']",
]

# JS helper shared by the scrape and the scroll: resolve the message pane INSIDE
# the main region, and refuse to return anything that contains the chat list.
_RESOLVE_PANE_JS = """
    (mainSels, paneSels, sidebarSels) => {
        let main = null;
        for (const s of mainSels) {
            const el = document.querySelector(s);
            if (el) { main = el; break; }
        }
        if (!main) return null;
        const bad = (el) => sidebarSels.some(s => el.querySelector(s));
        for (const s of paneSels) {
            for (const el of main.querySelectorAll(s)) {
                if (!bad(el)) return el;
            }
        }
        // No labelled pane: fall back to the main region itself, but only if it
        // does not contain the sidebar.
        return bad(main) ? null : main;
    }
"""


# Resolve the element that actually scrolls, then report where it sits and the
# largest data-mid rendered. `atBottom` is the ONLY trustworthy answer to "am I
# looking at the newest message?": data-last-visible means last *visible*, so
# Teams sets it on the last RENDERED row — it reads true even when the newest
# message is far below the viewport and absent from the DOM entirely.
_PANE_BOTTOM_JS = """
    ([mainSels, paneSels, sidebarSels, scrollSels, doScroll, resolveSrc]) => {
        const resolve = eval(resolveSrc);
        const pane = resolve(mainSels, paneSels, sidebarSels);
        if (!pane) return null;
        let box = null;
        for (const s of scrollSels) {
            for (const el of document.querySelectorAll(s)) {
                if (pane.contains(el) || el.contains(pane)) { box = el; break; }
            }
            if (box) break;
        }
        if (!box) {
            // Walk up from the pane, stopping at the main region so this can never
            // grab the left chat list.
            for (let el = pane; el; el = el.parentElement) {
                if (sidebarSels.some(s => el.matches && el.matches(s))) break;
                if (el.scrollHeight > el.clientHeight + 50) { box = el; break; }
                if (mainSels.some(s => el.matches && el.matches(s))) break;
            }
        }
        if (!box) box = pane;
        if (doScroll) box.scrollTop = box.scrollHeight;
        let maxMid = 0, rows = 0;
        pane.querySelectorAll('[data-mid]').forEach((el) => {
            rows++;
            const v = parseInt(el.getAttribute('data-mid') || '0', 10);
            if (Number.isFinite(v) && v > maxMid) maxMid = v;
        });
        return {
            top: box.scrollTop, view: box.clientHeight, full: box.scrollHeight,
            rows: rows, maxMid: maxMid ? String(maxMid) : '',
            atBottom: box.scrollTop + box.clientHeight >= box.scrollHeight - 4,
        };
    }
"""


def _pane_bottom_step(page, *, scroll: bool) -> dict[str, Any] | None:
    try:
        return page.evaluate(
            _PANE_BOTTOM_JS,
            [_MAIN_REGION_SELS, _MSG_PANE_SELS, _SIDEBAR_MARK_SELS,
             _MSG_SCROLL_SELS, bool(scroll), _RESOLVE_PANE_JS],
        )
    except Exception as err:  # noqa: BLE001
        print(f"[teams] pane bottom step failed: {err!r}", flush=True)
        return None


def _scroll_pane_to_bottom(page, *, rounds: int = 12) -> dict[str, Any]:
    """Scroll the message list to the very bottom, so the NEWEST row is rendered.

    Without this the read returns whatever Teams happened to render. The list is
    virtualised and Teams restores its own scroll position on load (at the unread
    marker, not necessarily the end), so the newest message can be missing from
    the DOM — and then the DOM tail is an OLDER message. That is exactly how a
    yesterday notice was posted as "latest" while a newer one existed.

    Loops because each scroll lazily loads more rows, which grows scrollHeight and
    moves the bottom; it settles once the position is at the end and the largest
    rendered data-mid has stopped changing.

    Each round SCROLLS, waits for the lazy load, then MEASURES in a *separate*
    evaluate. Measuring in the same tick that writes scrollTop is what made
    ``atBottom`` unconditionally true: the assignment clamps to
    ``scrollHeight - clientHeight``, so ``scrollTop + clientHeight`` equals
    ``scrollHeight`` the instant it returns. The guarantee this function exists to
    provide was therefore vacuous and the WARNING below was unreachable — which
    matters because detectevomaintenance refuses to advance its cursor on a read
    that did not reach the end.
    """
    state = _pane_bottom_step(page, scroll=False) or {}
    seen = str(state.get("maxMid") or "")
    stable = 0
    for i in range(1, rounds + 1):
        _pane_bottom_step(page, scroll=True)          # write only
        page.wait_for_timeout(900)
        state = _pane_bottom_step(page, scroll=False) or {}   # then measure
        now = str(state.get("maxMid") or "")
        stable = stable + 1 if now == seen else 0
        if now != seen:
            _rprint(f"[teams] scrolled down ({i}/{rounds}); newest rendered id "
                    f"{seen or '-'} -> {now or '-'}")
        seen = now
        if state.get("atBottom") and stable >= 2:
            break
    at_bottom = bool(state.get("atBottom"))
    _rprint(f"[teams] message list at bottom: {at_bottom} "
            f"(rendered rows {state.get('rows')}, newest id {seen or '-'})")
    if not at_bottom:
        # Say so loudly: this is the condition under which "latest" is a lie.
        print("[teams] WARNING: could not reach the end of the message list - "
              "the newest message may not be rendered", flush=True)
    return {"at_bottom": at_bottom, "newest_mid": seen,
            "rows": state.get("rows"), "top": state.get("top"),
            "full": state.get("full")}


def _scroll_pane_up(page, px: int = 1200) -> bool:
    """Scroll the message list up to render older rows.

    The list is virtualised: only what is near the viewport exists in the DOM, so
    when the visible tail is all "X left the chat" there is no message to find
    without scrolling.

    Scoped to the conversation pane. The previous version scanned EVERY div in the
    document for something scrollable, which would happily scroll the left chat
    list instead of the message list — and the chat list is exactly the thing we
    must not touch, since scrolling it recycles the row we are trying to click.
    """
    try:
        return bool(page.evaluate(
            """([mainSels, paneSels, sidebarSels, scrollSels, px, resolveSrc]) => {
                const resolve = eval(resolveSrc);
                const pane = resolve(mainSels, paneSels, sidebarSels);
                if (!pane) return false;
                const cands = [];
                // The named viewport first. The resolved pane is usually the
                // RUNWAY (#chat-pane-list), which is full-height and therefore
                // never scrolls — its viewport ancestor is what moves.
                for (const s of scrollSels) {
                    for (const el of document.querySelectorAll(s)) {
                        if (pane.contains(el) || el.contains(pane)) cands.push(el);
                    }
                }
                cands.push(pane);
                pane.querySelectorAll('div').forEach(el => {
                    if (el.scrollHeight > el.clientHeight + 200
                        && el.clientHeight > 200) cands.push(el);
                });
                // Then scrollable ancestors, stopping at the main region so this
                // can never reach the left chat list.
                for (let el = pane.parentElement; el; el = el.parentElement) {
                    if (sidebarSels.some(s => el.matches && el.matches(s))) break;
                    if (el.scrollHeight > el.clientHeight + 50) cands.push(el);
                    if (mainSels.some(s => el.matches && el.matches(s))) break;
                }
                for (const el of cands) {
                    if (el.scrollHeight > el.clientHeight + 50) {
                        const before = el.scrollTop;
                        el.scrollTop = Math.max(0, before - px);
                        if (el.scrollTop !== before) return true;
                    }
                }
                return false;
            }""",
            [_MAIN_REGION_SELS, _MSG_PANE_SELS, _SIDEBAR_MARK_SELS,
             _MSG_SCROLL_SELS, int(px), _RESOLVE_PANE_JS],
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
        """([sels, limit, mainSels, paneSels, sidebarSels, bodySels, resolveSrc]) => {
            const out = {matched: null, counts: {}, rows: [], pane: null};
            const resolve = eval(resolveSrc);
            const pane = resolve(mainSels, paneSels, sidebarSels);
            // HARD FAIL rather than falling back to the document. [role=listitem]
            // is in BOTH the chat-row and message-row selector lists, so a
            // document-wide query can return SIDEBAR CHAT ROWS as "messages" —
            // and els.slice(-limit) would take the document tail, not the
            // conversation's. That path could email another group's text.
            if (!pane) { out.error = 'no message pane inside the main region'; return out; }
            out.pane = pane.getAttribute('data-tid') || pane.tagName;
            for (const s of sels) {
                let els;
                try { els = [...pane.querySelectorAll(s)]; } catch (e) { continue; }
                out.counts[s] = els.length;
                if (!out.matched && els.length) {
                    out.matched = s;
                    // Virtualised list: the DOM tail is the newest.
                    for (const el of els.slice(-limit)) {
                        // MUST be the first statement: `byId` below closes over
                        // `mid`, and a `const` is in its temporal dead zone until
                        // declared — declaring it later throws ReferenceError,
                        // which escapes page.evaluate and turns /latestevo into
                        // the "could not read" branch instead of a message.
                        const mid = el.getAttribute('data-mid') || '';
                        const pick = (q) => {
                            const n = el.querySelector(q);
                            return n ? (n.innerText || '').trim() : '';
                        };
                        // Resolve a node by the id Teams names in the row's own
                        // aria-labelledby ("author-<mid> … timestamp-<mid>").
                        // Needed because querySelector is descendant-only and the
                        // author span is a SIBLING subtree of this row, not a
                        // child — which is why the author printed empty. Guarded
                        // by pane.contains() to keep this function's hard "never
                        // read outside the conversation pane" invariant; the same
                        // document-lookup-plus-guard idiom is used in
                        // _scroll_pane_up.
                        const byId = (prefix) => {
                            if (!mid) return null;
                            const n = document.getElementById(prefix + mid);
                            return (n && pane.contains(n)) ? n : null;
                        };
                        // Grouped-message wrapper, as a second chance only. NOT
                        // first: wrap.querySelector returns the FIRST match in the
                        // subtree, so if Teams ever groups consecutive messages it
                        // would silently stamp a NEIGHBOUR's name on this notice.
                        // The id embeds this row's own data-mid, so it cannot
                        // mis-attribute.
                        const wrap = (el.closest && el.closest('.fui-ChatMessage'))
                                     || el.parentElement || el;
                        // The body alone. This is what gets emailed, so the author
                        // name, timestamp, "Translate" button and reaction summary
                        // must not be glued onto the front or the back.
                        let body = '';
                        for (const q of bodySels) {
                            body = pick(q);
                            if (body) break;
                        }
                        const aEl = byId('author-')
                            || wrap.querySelector("[data-tid='message-author-name']")
                            || wrap.querySelector('[itemprop=name]');
                        const tEl = byId('timestamp-')
                            || el.querySelector('time')
                            || wrap.querySelector('time');
                        // innerText is layout-aware and returns '' for a node that
                        // is not rendered — this file has already been burned by
                        // that in _open_chat_title — so fall back to textContent.
                        const txt = (n) => n
                            ? ((n.innerText || n.textContent || '').trim()) : '';
                        out.rows.push({
                            // data-mid is Teams' own message id — a stable key for
                            // "have we already handled this message?", which a text
                            // hash only approximates.
                            mid: mid,
                            id: mid || el.getAttribute('data-tid')
                                || el.getAttribute('id') || '',
                            author: txt(aEl),
                            // Which signal answered, so the next Teams rename is
                            // diagnosable from a single /latestevo run rather than
                            // from another round of DOM guessing.
                            author_src: aEl ? (byId('author-') ? 'id' : 'wrap') : 'none',
                            time: (tEl ? (tEl.getAttribute('datetime') || '').trim()
                                       : ''),
                            // What the reader actually sees in Teams ("6:19 AM").
                            time_text: txt(tEl) || (tEl
                                ? (tEl.getAttribute('title') || '').trim() : ''),
                            time_src: tEl ? (byId('timestamp-') ? 'id' : 'dom') : 'none',
                            // Teams flags the newest rendered row; useful to prove
                            // we scraped the tail and not a scrolled-back window.
                            last: el.getAttribute('data-last-visible') === 'true',
                            body: body,
                            text: (el.innerText || '').trim(),
                        });
                    }
                }
            }
            return out;
        }""",
        [_MSG_ROW_SELS, int(scan), _MAIN_REGION_SELS, _MSG_PANE_SELS,
         _SIDEBAR_MARK_SELS, _MSG_BODY_SELS, _RESOLVE_PANE_JS],
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
    # Order by Teams' own message id. data-mid is the send time in epoch millis
    # — verified: 1786918775349 is 2026-08-16T22:19:35.349Z, matching that row's
    # <time datetime> to the millisecond — so it is a true newest-last key that
    # does not depend on the rendered rows happening to be in document order.
    # ONLY when every kept row has a numeric mid: under the fallback row
    # selectors data-mid is absent, and sorting all-equal keys would silently
    # redefine "newest" as "first rendered".
    if kept and all(str(r.get("mid") or "").isdigit() for r in kept):
        kept.sort(key=lambda r: int(r["mid"]))
    return {"messages": kept[-limit:] if limit > 0 else kept,
            "skipped_system": skipped, "scanned": len(rows or [])}


def _watch_target(group: str | None = None) -> str:
    """The group title every reader agrees on: argument, then env, then default.

    One resolver, because a poll and a /latestevo that resolved the group
    differently would be comparing message ids across two different chats.
    """
    return (group or os.getenv("EVOTEAMS_GROUP")
            or "@EVO C88live/slot_ow.ph (RTS) CS Group NE RT FP")


def _wait_teams_loaded(page) -> str:
    """Poll ``_stage_of`` until the Teams shell is up; return the final stage.

    The shell boots slowly on a CPU-only box and must clear the "setting things
    up" splash before the chat list exists, so this polls instead of sleeping a
    fixed period. The break set is the one read_latest_messages used inline:
    anything needing a human, plus the three logged-out login stages.
    """
    deadline = time.monotonic() + max(60, int(os.getenv("TEAMS_BOOT_WAIT", "90")))
    stage = "unknown"
    while time.monotonic() < deadline:
        stage = _stage_of(page)
        if stage == "teams_loaded":
            break
        if stage in _NEEDS_HUMAN or stage in ("landing", "email", "password"):
            break
        page.wait_for_timeout(2000)
    return stage


def _read_on_page(page, target: str, limit: int, *, shooter=None) -> dict:
    """Scrape ``target``'s newest messages from an ALREADY-BOOTED Teams page.

    Split out of read_latest_messages so the warm watcher can re-read one
    long-lived page every minute instead of cold-booting Chromium per poll. The
    cold path calls this too — one copy of the tricky part (confirm the right
    chat is open, render the end of a virtualised list, scrape, filter) so a
    poll and a /latestevo can never disagree about the group's newest message.

    ``limit=0`` returns EVERY real message in the scanned window instead of the
    newest one. That is what the poll uses: two notices can land inside one poll
    interval and _pick_messages' tail slice would drop all but the last.

    ``shooter`` overrides the screenshot function. The warm watcher passes
    _shot_fixed so a failure that repeats every minute overwrites one PNG rather
    than filling the disk with numbered ones.
    """
    shot = shooter or _shot
    res: dict[str, Any] = {"ok": False, "group": target, "matched": None,
                           "counts": {}, "messages": [], "shot": None,
                           "error": None, "opened_title": ""}

    # Already open? The warm page holds the chat open between polls, so
    # re-clicking the sidebar row every minute is wasted work — and every click is
    # another chance to land on the wrong row. A cold page fails this and opens the
    # group the normal way.
    #
    # Compares the URL's conversation id directly rather than calling
    # _confirm_open_chat: on a cold page with no chat open at all, that function's
    # title fallback logs "no conversation id in the DOM" before we have even tried
    # to open the group. Only a positive id match short-circuits; everything else
    # (no id configured, no id on the page, a different chat) falls through to
    # _open_group, and the authoritative check still runs below.
    wanted_thread = _wanted_thread_id(target)
    already_ok = bool(wanted_thread) and _open_thread_id(page) == wanted_thread
    if not already_ok and not _open_group(page, target):
        res["opened_title"] = _open_chat_title(page)
        res["error"] = (
            f"could not open the target group — Teams is showing "
            f"{res['opened_title']!r}"
            if res["opened_title"] else "could not open the target group"
        )
        res["shot"] = shot(page, "read_no_group")
        return res

    # Belt and braces: re-confirm immediately before scraping, so a chat that
    # switches underneath us can never be reported as this one. Uses the same
    # id-first check as opening did — re-checking the TITLE alone would reject a
    # correctly-open chat whenever the header read comes back empty, which is
    # exactly what happened on this build.
    res["opened_title"] = _open_chat_title(page)
    res["thread_id"] = _open_thread_id(page)
    still_ok, still_why = _confirm_open_chat(page, target)
    if not still_ok:
        res["error"] = f"chat changed before reading — {still_why}"
        res["shot"] = shot(page, "read_wrong_chat")
        return res

    # Render the END of the conversation before scraping. The list is
    # virtualised, so without this the newest message may not be in the DOM at
    # all and the tail of what IS rendered is an older message.
    bottom = _scroll_pane_to_bottom(page)
    res["at_bottom"] = bottom["at_bottom"]
    res["newest_mid"] = bottom["newest_mid"]
    res["rendered_rows"] = bottom["rows"]

    # Scan well past `limit`: the newest rows are often system events, so a
    # window of exactly `limit` can contain no real message at all. limit=0 (the
    # poll) still gets the 30-row floor.
    scan = max(30, limit * 10)
    scraped = _scrape_rows(page, scan)
    if scraped.get("error"):
        # Never read outside the conversation pane — see _scrape_rows.
        res["error"] = f"pane not resolved: {scraped['error']}"
        res["counts"] = scraped.get("counts") or {}
        res["shot"] = shot(page, "read_no_pane")
        return res
    res["pane"] = scraped.get("pane")
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
    # Did we actually return the newest rendered message? If not, say which one
    # we skipped instead of quietly presenting an older notice as "latest" — the
    # failure mode that posted a yesterday message.
    if res["messages"]:
        shown_mid = str(res["messages"][-1].get("mid") or "")
        res["shown_mid"] = shown_mid
        res["is_newest"] = bool(
            shown_mid and res.get("newest_mid")
            and shown_mid == res["newest_mid"]
        )
    if not res["ok"]:
        res["error"] = (
            f"scanned {picked['scanned']} row(s); all were system events "
            f"or empty ({len(picked['skipped_system'])} skipped)"
            if picked["skipped_system"]
            else "opened the chat but matched no message rows"
        )
    return res


def read_latest_messages(*, group: str | None = None, limit: int = 3,
                         headless: bool = True) -> dict:
    """Open ``group`` in a COLD browser and return its most recent messages.

    Returns ``{"ok", "group", "matched", "counts", "messages", "shot", "error"}``.
    ``counts`` reports every candidate selector and how many nodes it matched, so
    a Teams UI change is diagnosable from one run instead of guesswork.

    The warm watcher does not use this — it holds one page open and calls
    ``_read_on_page`` directly. This is the CLI / no-watcher path, and it holds
    the exclusive profile lock for the whole Chromium boot.
    """
    from playwright.sync_api import sync_playwright

    target = _watch_target(group)
    res: dict[str, Any] = {"ok": False, "group": target, "matched": None,
                           "counts": {}, "messages": [], "shot": None, "error": None,
                           "opened_title": ""}

    with _profile_lock("read-latest"), sync_playwright() as p:
        ctx, page = _open(p, headless=headless)
        try:
            page.goto(TEAMS_URL, wait_until="domcontentloaded", timeout=60000)
            stage = _wait_teams_loaded(page)
            if stage != "teams_loaded":
                res["error"] = f"not signed in (stage: {stage}) — run --login"
                res["shot"] = _shot(page, f"read_{stage}")
                return res
            res.update(_read_on_page(page, target, limit))
            # _read_on_page shoots only its own failure branches; the success
            # (and "all system events") paths still want the PNG that
            # /latestevo sends when a read comes back empty.
            if res.get("shot") is None:
                res["shot"] = _shot(page, "read_latest")
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
    # Proof that this really is the newest message and not the tail of a
    # part-rendered virtualised list.
    if res.get("at_bottom") is not None:
        lines.append(
            f"• Reached end of chat: {'yes' if res.get('at_bottom') else 'NO'}"
            f" (rendered {res.get('rendered_rows') or '?'} rows,"
            f" newest id {res.get('newest_mid') or '—'})"
        )
    if res.get("messages") and res.get("is_newest") is False:
        lines.append(
            f"⚠️ Showing id {res.get('shown_mid') or '—'}, but the newest"
            f" rendered message is id {res.get('newest_mid') or '—'} — it was"
            f" filtered as a system event, or the list did not finish loading."
        )
    skipped = res.get("skipped_system") or []
    if skipped:
        # Say what was passed over, so a surprising "latest" is explainable.
        lines.append(f"• Skipped {len(skipped)} system event(s): "
                     + "; ".join(skipped[:3]))
    lines.append("")
    for i, msg in enumerate(res["messages"], 1):
        # Display-only tidy. The notice's <p>&nbsp;</p> spacer paragraphs come
        # through as blank lines (_dedupe_lines rstrips each line and Python
        # treats U+00A0 as whitespace), which is what produced the big gaps in
        # the posted message. Collapse runs to ONE blank line — never to zero:
        # maintenance.py's table-block walker uses a single blank as its block
        # terminator, and the ={10,} batch separator must stay on its own line.
        #
        # This rewrites a LOCAL only. It must never touch msg["text"], because
        # that text is the future detection input and detectevomaintenance keys
        # its ledger on sha1(text) — re-keying it would mean duplicate cards and
        # a duplicate maintenance email on one tap.
        body = re.sub(r"\n{3,}", "\n\n", (msg.get("text") or "").strip())
        # No length cap. A 1500-char cap here was silently dropping 5251 chars of
        # a real notice (severing it mid-value at "维护时间：2"); oversize is now
        # handled by splitting across messages in send_text_parts, which loses
        # nothing. '—' rather than omission for a field that did not resolve, so
        # a Teams rename shows up as a visible gap instead of a silent one.
        when = (msg.get("time_text") or msg.get("time") or "").strip()
        head_bits = [msg.get("author") or "—", when or "—"]
        if msg.get("mid"):
            # Teams' own message id — the key the detection ledger dedupes on.
            head_bits.append(f"id {msg['mid']}")
        if msg.get("last") is False:
            # Not the newest RENDERED row. Legitimate whenever _pick_messages
            # correctly dropped a newer system event, so state it as a fact.
            head_bits.append("not tail row")
        head = " | ".join(head_bits) + (
            f" | meta {msg.get('author_src') or '?'}/{msg.get('time_src') or '?'}"
        )
        lines.append(f"--- {i}/{len(res['messages'])}  ({head}) ---\n{body}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Warm watcher — one long-lived Chromium, polled for new messages
# ---------------------------------------------------------------------------
# Playwright's sync API is thread-confined, so EVERY browser call runs on one
# worker thread; other threads submit jobs through a queue and block on an Event
# when they need the answer. Same shape as osmwatch._OsmWatchWarm, which solves
# the same problem for the OSM dashboard.
#
# Why warm and not a cold read per poll: read_latest_messages boots Chromium and
# waits up to 90s for the Teams shell. Doing that every minute on a CPU-only box
# is untenable, and each cold read takes the exclusive profile lock — so a poll
# in flight would make /latestevo fail with "profile is busy". The watcher owns
# the browser AND the lock for its whole life, and /latestevo + /teamstatus are
# served from the same page (which also makes them fast).
_HEARTBEAT_S = 60
# How long a blocking caller waits. Generous because the very first job may have
# to boot Teams from cold (TEAMS_BOOT_WAIT is 90s by itself).
_WARM_CALL_TIMEOUT_S = 240


def _watch_enabled() -> bool:
    """Master switch for auto-detection. Unset means OFF, so deploying this file
    changes nothing until .env says so. Shared with detectevomaintenance._enabled
    on purpose: a watcher that polls but cannot card, or a detector with nothing
    feeding it, are both half-configured states not worth supporting."""
    return _truthy(os.getenv("EVOTEAMS_ENABLED"))


def _poll_seconds() -> int:
    try:
        return max(15, int(os.getenv("EVOTEAMS_POLL_SECONDS", "60")))
    except ValueError:
        return 60


def _headless() -> bool:
    return not _truthy(os.getenv("TEAMS_HEADED"))


class _TeamsWarm:
    """The long-lived Teams session, plus the poll loop that feeds detection."""

    def __init__(self) -> None:
        self._tasks: queue.Queue[dict] = queue.Queue()
        self._p = None
        self._ctx = None
        self._page = None
        self._holding_lock = False
        self._started = False
        self._polling = False
        self._start_lock = threading.Lock()
        self._st_lock = threading.Lock()
        self._st: dict[str, Any] = {
            "launched_at": None, "stage": None, "polls": 0, "new_msgs": 0,
            "cards": 0, "last_poll_at": None, "last_poll_ok": None,
            "last_poll_error": None, "last_new_at": None, "consec_fail": 0,
            "yielded_at": None, "relaunches": 0,
        }

    # -- little state box (read by /teamstatus from another thread) -----------
    def _note(self, **kw: Any) -> None:
        with self._st_lock:
            self._st.update(kw)

    def _bump(self, key: str, by: int = 1) -> None:
        with self._st_lock:
            self._st[key] = int(self._st.get(key) or 0) + by

    def stats(self) -> dict[str, Any]:
        with self._st_lock:
            return dict(self._st)

    def started(self) -> bool:
        return self._started

    # -- lifecycle -----------------------------------------------------------
    def start(self, *, poll: bool = True) -> None:
        with self._start_lock:
            if self._started:
                return
            self._started = True
            threading.Thread(target=self._loop, name="teams-warm",
                             daemon=True).start()
            threading.Thread(target=self._heartbeat_loop, name="teams-warm-hb",
                             daemon=True).start()
            if poll and not self._polling:
                self._polling = True
                threading.Thread(target=self._poll_loop, name="teams-warm-poll",
                                 daemon=True).start()

    def _heartbeat_loop(self) -> None:
        """Keep our own lock from ageing into 'stale' — see _touch_profile_lock."""
        while True:
            time.sleep(_HEARTBEAT_S)
            if self._holding_lock:
                _touch_profile_lock()

    def _poll_loop(self) -> None:
        """Submit a poll on the interval, backing off while the read is failing.

        Backoff matters: with a dead session every poll costs a Teams boot attempt
        and an error line, and hammering that once a minute buries the one log
        line that says what to fix.
        """
        while True:
            if self._tasks.empty():
                self._tasks.put({"kind": "poll"})
            else:
                # The worker is still busy with the previous cycle — a cold Teams
                # boot alone can take 90s. Queueing another poll behind it would
                # build a backlog that never drains and would keep re-reading the
                # same rows. Skip this tick; the next one is 60s away.
                print("[teams-warm] previous job still running — skipping this "
                      "poll tick", flush=True)
            fails = int(self.stats().get("consec_fail") or 0)
            delay = _poll_seconds() * (2 ** min(fails, 4))
            time.sleep(min(max(delay, _poll_seconds()), 900))

    def _launch(self) -> bool:
        """Take the lock, start Chromium, boot Teams. False if it did not come up."""
        from playwright.sync_api import sync_playwright

        self._teardown()
        # ask=False: the watcher is the polite party. If it asked whoever holds
        # the lock (a hand-run --login) to yield, the two would ping-pong.
        if not _acquire_profile_lock("warm-watch", wait_s=0, ask=False):
            self._note(last_poll_error=f"profile busy ({_lock_holder()})")
            print(f"[teams-warm] profile held by {_lock_holder()!r} — will retry",
                  flush=True)
            return False
        self._holding_lock = True
        _touch_profile_lock()
        try:
            self._p = sync_playwright().start()
            self._ctx, self._page = _open(self._p, headless=_headless())
            self._page.goto(TEAMS_URL, wait_until="domcontentloaded", timeout=60000)
            stage = _wait_teams_loaded(self._page)
            self._note(stage=stage)
            if stage != "teams_loaded":
                reason = f"profile is not signed in (stage: {stage})"
                shot = _shot_fixed(self._page, f"launch_{stage}")
                _set(phase="login_failed", detail=f"warm watcher: {reason}",
                     last_error=reason, last_stage=stage)
                _persist()
                # Once per death, not once per poll: alert_login_failed checks the
                # `alerted` flag itself, and do_login clears it on success.
                alert_login_failed(reason, stage=stage, shot=shot)
                self._teardown()
                return False
            self._bump("relaunches")
            self._note(launched_at=_now_str())
            _set(phase="monitoring",
                 detail=f"warm watcher on {_watch_target()} "
                        f"(every {_poll_seconds()}s)",
                 connected_at=_now_str(), last_error=None, last_stage=stage,
                 account=os.getenv("TEAMS_EMAIL") or None)
            _persist()
            print(f"[teams-warm] browser up; watching {_watch_target()!r}",
                  flush=True)
            return True
        except Exception as err:  # noqa: BLE001
            print(f"[teams-warm] launch failed: {err!r}", flush=True)
            self._note(last_poll_error=repr(err))
            _set(phase="login_failed", detail="warm launch raised",
                 last_error=repr(err))
            self._teardown()
            return False

    def _teardown(self) -> None:
        for closer in (
            lambda: self._ctx.close() if self._ctx else None,
            lambda: self._p.stop() if self._p else None,
        ):
            try:
                closer()
            except Exception:
                pass
        self._p = self._ctx = self._page = None
        if self._holding_lock:
            _release_profile_lock()
            self._holding_lock = False

    def _healthy(self) -> bool:
        try:
            return self._page is not None and not self._page.is_closed()
        except Exception:
            return False

    def _stand_down(self) -> None:
        """Release the browser + lock because something else asked for the profile.

        The sentinel is NOT deleted here — the asker removes it when it takes the
        lock. Deleting it ourselves would let us relaunch straight into the fight.
        """
        if not self._healthy() and not self._holding_lock:
            return
        print(f"[teams-warm] yielding the profile to {_YIELD_PATH.name} "
              f"({_lock_holder() or 'a waiting caller'})", flush=True)
        self._teardown()
        self._note(yielded_at=_now_str())
        _set(phase="stopped", detail="yielded the profile to another run")

    def _ready(self) -> bool:
        """Ensure a booted page, honouring a pending yield. False = not now."""
        if _yield_requested():
            self._stand_down()
            return False
        if self._healthy():
            # Cheap path: no re-probe every minute. A session that died since the
            # last poll surfaces as a read failure, which _do_poll diagnoses.
            return True
        return self._launch()

    # -- jobs ----------------------------------------------------------------
    def _do_read(self, group: str | None, limit: int, *, quiet: bool) -> dict:
        global _quiet_reads
        target = _watch_target(group)
        if not self._ready():
            # `yielded` matters: standing aside for a --login is NORMAL, and
            # counting it as a failed poll would back the interval off to 16
            # minutes over four ticks, so the watcher would crawl back long after
            # the login finished.
            yielded = _yield_requested()
            return {"ok": False, "group": target, "messages": [], "counts": {},
                    "shot": _snapshot().get("last_shot"), "yielded": yielded,
                    "error": ("standing aside — another run holds the Teams "
                              "profile" if yielded else
                              self.stats().get("last_poll_error")
                              or "warm watcher has no signed-in Teams page")}
        was_quiet = _quiet_reads
        _quiet_reads = quiet
        try:
            return _read_on_page(self._page, target, limit, shooter=_shot_fixed)
        finally:
            _quiet_reads = was_quiet

    def _do_poll(self) -> None:
        res = self._do_read(None, 0, quiet=True)
        self._bump("polls")
        self._note(last_poll_at=_now_str(), last_poll_ok=bool(res.get("ok")))

        if res.get("yielded"):
            # Not a failure: hold at the normal interval and pick straight back up
            # once whoever asked for the profile is done with it.
            self._note(last_poll_error=res.get("error"), consec_fail=0)
            return

        if not res.get("ok"):
            err = res.get("error") or "unknown read failure"
            self._note(last_poll_error=err)
            self._bump("consec_fail")
            print(f"[teams-warm] poll failed: {err}", flush=True)
            # Is the SESSION gone, or was that just a bad frame? Only a dead
            # session justifies dropping the browser; rebuilding it for a transient
            # DOM miss would cost a 90s boot every time Teams re-rendered slowly.
            if self._healthy():
                try:
                    stage = _stage_of(self._page)
                except Exception as probe_err:  # noqa: BLE001
                    stage = f"probe_failed:{probe_err!r}"
                self._note(stage=stage)
                if not str(stage).startswith("teams_loaded"):
                    print(f"[teams-warm] session looks dead (stage {stage}) — "
                          f"dropping the browser so the next poll relaunches",
                          flush=True)
                    self._teardown()
            return

        self._note(last_poll_error=None, consec_fail=0)
        msgs = res.get("messages") or []
        try:
            import detectevomaintenance as _evom

            # `group` is the TARGET, not the scraped header text. That is
            # deliberate: the "is this the right chat" question was already
            # answered decisively inside _read_on_page, which calls
            # _confirm_open_chat twice and compares the URL's conversation id
            # against the pinned one for this group — a check that cannot
            # half-match. The header title, by contrast, comes back empty on this
            # Teams build, and feeding that to in_watched_group (which fails
            # closed) would silently drop every real notice.
            out = _evom.handle_new_messages(
                group=res.get("group") or _watch_target(),
                messages=msgs,
                newest_mid=str(res.get("newest_mid") or ""),
                at_bottom=res.get("at_bottom"),
            ) or {}
        except Exception as err:  # noqa: BLE001
            self._note(last_poll_error=f"detector raised: {err!r}")
            print(f"[teams-warm] detector raised: {err!r}", flush=True)
            return

        if out.get("new"):
            self._bump("new_msgs", int(out.get("new") or 0))
            self._bump("cards", int(out.get("cards") or 0))
            self._note(last_new_at=_now_str())

    # -- worker loop ---------------------------------------------------------
    def _loop(self) -> None:
        while True:
            try:
                task = self._tasks.get(timeout=2)
            except queue.Empty:
                # Idle tick: notice a yield request even when no job is queued,
                # so a hand-run --login is not stuck behind the poll interval.
                if _yield_requested():
                    self._stand_down()
                continue
            kind = str(task.get("kind") or "")
            done = task.get("done")
            box = task.get("box")
            try:
                if kind == "poll":
                    self._do_poll()
                elif kind == "read":
                    out = self._do_read(task.get("group"),
                                        int(task.get("limit") or 0), quiet=False)
                    if box is not None:
                        box.update(out)
                elif kind == "shot":
                    ready = self._ready()
                    if box is not None:
                        box.update({
                            "ok": ready and self._healthy(),
                            "shot": (_shot_fixed(self._page, "status")
                                     if self._healthy() else
                                     _snapshot().get("last_shot")),
                            "stage": self.stats().get("stage"),
                        })
                else:
                    print(f"[teams-warm] unknown job {kind!r}", flush=True)
            except Exception as err:  # noqa: BLE001
                print(f"[teams-warm] job {kind!r} raised: {err!r}", flush=True)
                self._note(last_poll_error=repr(err))
                if box is not None and not box:
                    box.update({"ok": False, "error": repr(err), "messages": [],
                                "counts": {}, "shot": None})
                # A raised job means the page is suspect. Drop it; the next job
                # relaunches. Without this a half-dead page is reused forever.
                self._teardown()
            finally:
                # ALWAYS, or a blocking caller waits out its full timeout.
                if done is not None:
                    done.set()

    # -- public, thread-safe -------------------------------------------------
    def read_latest(self, *, group: str | None = None, limit: int = 1,
                    timeout_s: int = _WARM_CALL_TIMEOUT_S) -> dict:
        self.start(poll=_watch_enabled())
        done = threading.Event()
        box: dict[str, Any] = {}
        self._tasks.put({"kind": "read", "group": group, "limit": limit,
                         "done": done, "box": box})
        if not done.wait(timeout=timeout_s):
            return {"ok": False, "group": _watch_target(group), "messages": [],
                    "counts": {}, "shot": None,
                    "error": f"the warm Teams watcher did not answer within "
                             f"{timeout_s}s — it may still be booting Teams"}
        return box

    def probe(self, *, timeout_s: int = _WARM_CALL_TIMEOUT_S) -> dict:
        """A fresh screenshot + liveness of the warm page, for /teamstatus."""
        self.start(poll=_watch_enabled())
        done = threading.Event()
        box: dict[str, Any] = {}
        self._tasks.put({"kind": "shot", "done": done, "box": box})
        if not done.wait(timeout=timeout_s):
            return {"ok": False, "shot": _snapshot().get("last_shot"),
                    "stage": "timeout"}
        return box

    def poll_now(self) -> None:
        self.start(poll=_watch_enabled())
        self._tasks.put({"kind": "poll"})


_warm: _TeamsWarm | None = None
_warm_lock = threading.Lock()


def warm() -> _TeamsWarm:
    global _warm
    with _warm_lock:
        if _warm is None:
            _warm = _TeamsWarm()
        return _warm


def warm_running() -> bool:
    """True once the worker thread exists — the signal for /latestevo and
    /teamstatus to go through it instead of taking the profile lock themselves.
    Deliberately not `_healthy()`: a watcher that is mid-boot still owns the
    lock, so a cold read would only fail."""
    return _warm is not None and _warm.started()


def start_watch_on_startup() -> None:
    """Boot hook for main.py. No-op unless EVOTEAMS_ENABLED is truthy."""
    if not _watch_enabled():
        print("[teams-warm] EVOTEAMS_ENABLED not set — Teams auto-detect is OFF",
              flush=True)
        return
    if _profile_state() != "present":
        detail = (f"no Teams profile ({_profile_state()}) — run "
                  f"`python teamswatch.py --login` before auto-detect can work")
        _set(phase="login_failed", detail=detail, last_error=detail)
        _persist()
        print(f"[teams-warm] ❌ {detail}", flush=True)
        return
    warm().start(poll=True)
    print(f"[teams-warm] auto-detect ON — {_watch_target()!r} every "
          f"{_poll_seconds()}s, cards to "
          f"{os.getenv('EVOTEAMS_CARD_CHAT_ID') or 'the /m group'}", flush=True)


def send_latest_to_lark(chat_id: str, *, group: str | None = None,
                        limit: int = 1) -> dict:
    """``/latestevo`` — read the group and post what we found back to ``chat_id``."""
    if warm_running():
        # Reuse the watcher's already-booted page. Not an optimisation: the
        # watcher holds the profile lock for its whole life, so a cold read here
        # would sit for 120s and then fail with "profile is busy".
        res = warm().read_latest(group=group, limit=limit)
    else:
        res = read_latest_messages(group=group, limit=limit)
    try:
        # Parts, not a truncation: a real EVO batch notice runs to thousands of
        # characters and /latestevo N concatenates up to ten of them.
        send_text_parts(chat_id, _fmt_messages(res), label="📥 /latestevo")
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


def _warm_status_lines() -> list[str]:
    """What the poll loop is actually doing, for /teamstatus.

    Reports the DIFFERENCE between "configured" and "running": a watcher that is
    enabled but has never completed a poll is the state most likely to be
    mistaken for working, so the poll count and last-poll time are always shown.
    """
    if not _watch_enabled():
        return ["• Auto-detect: OFF (set EVOTEAMS_ENABLED=1 in .env)"]
    if not warm_running():
        return ["🔴 Auto-detect: ENABLED but the watcher thread is NOT running — "
                "main.py's boot hook did not start it (profile missing?)"]
    st = warm().stats()
    ok = st.get("last_poll_ok")
    lines = [
        f"• Auto-detect: ON — polling every {_poll_seconds()}s",
        f"• Polls: {st.get('polls') or 0} "
        f"(last {st.get('last_poll_at') or 'never'}: "
        f"{'ok' if ok else 'FAILED' if ok is False else 'not yet'})",
        f"• New messages seen: {st.get('new_msgs') or 0}; cards posted: "
        f"{st.get('cards') or 0}"
        + (f"; last new at {st['last_new_at']}" if st.get("last_new_at") else ""),
    ]
    if st.get("last_poll_error"):
        lines.append(f"• Last poll error: {st['last_poll_error']}")
    if int(st.get("consec_fail") or 0) > 1:
        lines.append(f"⚠️ {st['consec_fail']} consecutive failed polls — the "
                     f"interval is backing off")
    if st.get("relaunches"):
        lines.append(f"• Browser launches: {st['relaunches']} "
                     f"(up since {st.get('launched_at') or '—'})")
    if st.get("yielded_at"):
        lines.append(f"• Last yielded the profile at {st['yielded_at']} "
                     f"(a --login or CLI read asked for it)")
    return lines


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
    lines.extend(_warm_status_lines())

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

    if warm_running():
        # Ask the watcher, don't open a second browser: check_session takes the
        # profile lock the watcher is holding, so it would stall then fail — and
        # asking the live page is a *better* probe than a cold re-login anyway.
        try:
            probe = warm().probe()
            monitoring = bool(probe.get("ok"))
            shot = probe.get("shot")
        except Exception as err:  # noqa: BLE001
            print(f"[teams] warm status probe failed: {err!r}", flush=True)
            _set(last_error=repr(err))
    elif _profile_state() == "present" and _LOCK_PATH.exists():
        # Something else holds the profile (a --login, a --read-latest). Do NOT
        # queue behind it: _profile_lock waits 120s and then raises, and that
        # generic lock error would overwrite the real diagnostic the caller asked
        # for. Say who has it instead.
        _set(detail=f"profile busy — not re-probed ({_lock_holder()})")
    elif _profile_state() == "present":
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
    ap.add_argument("--dump-dom", action="store_true",
                    help="dump the live sidebar/header/pane structure + thread id")
    ap.add_argument("--read-latest", action="store_true",
                    help="open the watched group and print its latest messages")
    ap.add_argument("--watch", action="store_true",
                    help="run the warm watcher + poll loop in the FOREGROUND "
                         "(what the bot does at boot); Ctrl-C to stop")
    ap.add_argument("--detect-now", action="store_true",
                    help="read the group's newest message and push it through "
                         "detection right now, ignoring the cursor — the way to "
                         "test the card without waiting for a new notice")
    ap.add_argument("--group", default=None, help="override the group title to read")
    # 1, matching /latestevo's own default (main.py clamps its argument to 1..10),
    # so the CLI and the bot command answer the same question by default.
    ap.add_argument("--limit", type=int, default=1,
                    help="how many recent messages --read-latest returns (default 1)")
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
    if args.dump_dom:
        res = dump_dom(group=args.group, headless=not args.headed)
        return 0 if res.get("ok") else 1
    if args.detect_now:
        # Cold read on purpose: --detect-now is run by hand, usually with the bot
        # (and therefore the warm watcher) either stopped or holding the profile —
        # _profile_lock asks it to yield, which is exactly what we want here.
        res = read_latest_messages(group=args.group, limit=1,
                                   headless=not args.headed)
        print(_fmt_messages(res))
        if not res.get("ok") or not res.get("messages"):
            print("\nnothing to detect — see the read error above")
            return 1
        import detectevomaintenance as _evom

        msg = res["messages"][-1]
        status = _evom.force_card(group=res.get("group"), message=msg)
        print(f"\ndetection status: {status}")
        if status == "duplicate":
            print("(already in the ledger — remove its record from "
                  "detectevomaintenance.json to card it again)")
        print("\n".join(_evom.status_lines()))
        return 0 if status in ("carded", "dry_run", "duplicate") else 1
    if args.watch:
        if not _watch_enabled():
            print("EVOTEAMS_ENABLED is not set — the poll loop would do nothing. "
                  "Set EVOTEAMS_ENABLED=1 in .env first.")
            return 2
        warm().start(poll=True)
        print(f"watching {_watch_target(args.group)!r} every "
              f"{_poll_seconds()}s — Ctrl-C to stop")
        try:
            while True:
                time.sleep(30)
                print("\n".join(status_lines()))
        except KeyboardInterrupt:
            print("\nstopping")
            return 0
    if args.read_latest:
        res = read_latest_messages(group=args.group, limit=args.limit,
                                   headless=not args.headed)
        print("\nselector counts (which message-row selector matched):")
        for sel, count in (res.get("counts") or {}).items():
            print(f"   {count:6d}  {sel}")
        print()
        print(_fmt_messages(res))
        if args.report_chat:
            send_text_parts(args.report_chat, _fmt_messages(res),
                            label="📥 /latestevo (cli)")
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
