#!/usr/bin/env python3
"""
IMAP watcher for om@hotelstotsenberg.com (or MAINTENANCE_MAIL_USER).

Processes inbox messages whose subject starts with ``TINC-`` or ``[Service Desk]``,
runs the same pipeline as ``/m``, and posts to a fixed Lark group.

State file: ``maintenance.json`` (titles + content hashes + IMAP UIDs) to avoid
re-processing after bot restart.
"""

from __future__ import annotations

import email
import hashlib
import imaplib
import json
import os
import re
import ssl
import threading
import time
from datetime import datetime, timezone
from email.header import decode_header
from email.utils import parsedate_to_datetime
from typing import Any, Callable

from dotenv import load_dotenv

load_dotenv()

_CHBOX_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(_CHBOX_DIR, "maintenance.json")
STATE_VERSION = 1
MAX_ENTRIES = 8000

MAIL_USER = (
    os.getenv("MAINTENANCE_MAIL_USER", "").strip()
    or os.getenv("maintenance_mail_user", "").strip()
    or "om@hotelstotsenberg.com"
)
MAIL_PASSWORD = (
    os.getenv("MAINTENANCE_MAIL_PASSWORD", "").strip()
    or os.getenv("maintenance_mail_password", "").strip()
)
MAIL_IMAP_HOST = (
    os.getenv("MAINTENANCE_MAIL_IMAP_HOST", "").strip()
    or os.getenv("maintenance_mail_imap_host", "").strip()
    or "imap.larksuite.com"
)
MAIL_IMAP_PORT = int(
    os.getenv("MAINTENANCE_MAIL_IMAP_PORT", "").strip()
    or os.getenv("maintenance_mail_imap_port", "").strip()
    or "993"
)
TARGET_CHAT_ID = (
    os.getenv("MAINTENANCE_MAIL_TARGET_CHAT_ID", "").strip()
    or os.getenv("maintenance_mail_target_chat_id", "").strip()
    or "oc_ad9b5bdbb2826ba2ee9730920ef25432"
)
POLL_SECONDS = float(
    os.getenv("MAINTENANCE_MAIL_POLL_SECONDS", "").strip() or "3"
)
IMAP_TIMEOUT = float(
    os.getenv("MAINTENANCE_MAIL_IMAP_TIMEOUT", "").strip() or "30"
)
# 1 = IMAP4_SSL on 993 (default). 0 = plain IMAP + STARTTLS (often port 143).
IMAP_USE_SSL = (os.getenv("MAINTENANCE_MAIL_IMAP_SSL", "").strip() or "1") not in (
    "0",
    "false",
    "no",
    "off",
)
# On connect, also scan already-read INBOX mail (not only UNSEEN). Needed for mail opened before bot started.
BACKFILL_ON_START = (os.getenv("MAINTENANCE_MAIL_BACKFILL_ON_START", "").strip() or "1") not in (
    "0",
    "false",
    "no",
    "off",
)
BACKFILL_LIMIT = int(os.getenv("MAINTENANCE_MAIL_BACKFILL_LIMIT", "").strip() or "50")

_state_lock = threading.Lock()
_watcher_started = False


def _normalize_subject(subject: str) -> str:
    """Strip Re:/Fwd:/FW: prefixes so ``Re: TINC-…`` still matches."""
    s = (subject or "").strip()
    for _ in range(8):
        m = re.match(r"^(?:Re|Fwd|FW|Aw):\s*", s, re.IGNORECASE)
        if not m:
            break
        s = s[m.end() :].strip()
    return s


def subject_matches(subject: str) -> bool:
    s = _normalize_subject(subject)
    if not s:
        return False
    if re.match(r"^TINC-", s, re.IGNORECASE):
        return True
    if re.match(r"^\[Service Desk\]", s, re.IGNORECASE):
        return True
    return False


def _decode_mime_header(raw: str | None) -> str:
    if not raw:
        return ""
    parts: list[str] = []
    for frag, enc in decode_header(raw):
        if isinstance(frag, bytes):
            parts.append(frag.decode(enc or "utf-8", errors="replace"))
        else:
            parts.append(str(frag))
    return "".join(parts).strip()


def _normalize_body(text: str) -> str:
    t = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _content_hash(body: str) -> str:
    return hashlib.sha256(_normalize_body(body).encode("utf-8")).hexdigest()


def _html_to_text(html: str) -> str:
    import html as html_mod

    t = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", html)
    t = re.sub(r"(?is)<br\s*/?>", "\n", t)
    t = re.sub(r"(?is)</p\s*>", "\n", t)
    t = re.sub(r"<[^>]+>", "", t)
    return _normalize_body(html_mod.unescape(t))


def extract_body_from_message(msg: email.message.Message) -> str:
    plain_parts: list[str] = []
    html_parts: list[str] = []

    if msg.is_multipart():
        for part in msg.walk():
            ctype = (part.get_content_type() or "").lower()
            disp = str(part.get("Content-Disposition") or "").lower()
            if "attachment" in disp:
                continue
            try:
                payload = part.get_payload(decode=True)
            except Exception:
                continue
            if not payload:
                continue
            charset = part.get_content_charset() or "utf-8"
            try:
                text = payload.decode(charset, errors="replace")
            except Exception:
                text = payload.decode("utf-8", errors="replace")
            if ctype == "text/plain":
                plain_parts.append(text)
            elif ctype == "text/html":
                html_parts.append(text)
    else:
        try:
            payload = msg.get_payload(decode=True)
        except Exception:
            payload = None
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            try:
                text = payload.decode(charset, errors="replace")
            except Exception:
                text = payload.decode("utf-8", errors="replace")
            if (msg.get_content_type() or "").lower() == "text/html":
                html_parts.append(text)
            else:
                plain_parts.append(text)

    if plain_parts:
        return _normalize_body("\n\n".join(plain_parts))
    if html_parts:
        return _html_to_text("\n\n".join(html_parts))
    return ""


def build_pipeline_input(subject: str, body: str) -> str:
    subj = (subject or "").strip()
    body = _normalize_body(body)
    if body:
        return body
    return subj


def _load_state() -> dict[str, Any]:
    if not os.path.isfile(STATE_PATH):
        return {"version": STATE_VERSION, "entries": []}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {"version": STATE_VERSION, "entries": []}
    if not isinstance(data, dict):
        return {"version": STATE_VERSION, "entries": []}
    entries = data.get("entries")
    if not isinstance(entries, list):
        entries = []
    return {"version": STATE_VERSION, "entries": entries}


def _save_state(data: dict[str, Any]) -> None:
    entries = data.get("entries") or []
    if len(entries) > MAX_ENTRIES:
        data["entries"] = entries[-MAX_ENTRIES:]
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)


def _find_duplicate_title_content(
    entries: list[dict[str, Any]], title: str, content_hash: str
) -> dict[str, Any] | None:
    nt = (title or "").strip().lower()
    for ent in reversed(entries):
        if (ent.get("title") or "").strip().lower() != nt:
            continue
        if ent.get("content_hash") == content_hash:
            return ent
    return None


def _already_processed_uid(entries: list[dict[str, Any]], imap_uid: str) -> bool:
    uid = str(imap_uid)
    return any(str(e.get("imap_uid")) == uid for e in entries)


def _record_processed(
    entries: list[dict[str, Any]],
    *,
    imap_uid: str,
    message_id: str,
    title: str,
    content_hash: str,
) -> None:
    entries.append(
        {
            "imap_uid": str(imap_uid),
            "message_id": (message_id or "").strip(),
            "title": (title or "").strip(),
            "content_hash": content_hash,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
    )


def format_duplicate_notice(new_title: str, old_title: str) -> str:
    return (
        "⚠️ **Ignored new email** — subject already processed with **identical content**.\n\n"
        f"**New title:** `{new_title}`\n"
        f"**Previous title:** `{old_title}`\n\n"
        "Kindly check and provide what email titles."
    )


def format_incoming_header(subject: str, from_addr: str, when: str | None) -> str:
    lines = ["📧 **Maintenance email (auto)**", f"**Subject:** {subject}"]
    if from_addr:
        lines.append(f"**From:** {from_addr}")
    if when:
        lines.append(f"**Date:** {when}")
    lines.append("")
    return "\n".join(lines)


class MaintenanceMailWatcher:
    def __init__(
        self,
        *,
        send_message_func: Callable[[str, str], Any],
        get_token_func: Callable[[], str | None],
    ) -> None:
        self._send = send_message_func
        self._get_token = get_token_func
        self._stop = threading.Event()

    def _send_lark(self, chat_id: str, text: str) -> None:
        try:
            resp = self._send(chat_id, text)
            if isinstance(resp, dict) and resp.get("code") not in (None, 0):
                print(
                    f"[maint-mail] send_message failed chat={chat_id}: {resp}",
                    flush=True,
                )
        except Exception as ex:
            print(f"[maint-mail] send_message error: {ex!r}", flush=True)

    def _connect(self) -> imaplib.IMAP4:
        if not MAIL_PASSWORD:
            raise RuntimeError(
                "MAINTENANCE_MAIL_PASSWORD is not set in .env"
            )
        ctx = ssl.create_default_context()
        mode = "SSL" if IMAP_USE_SSL else "STARTTLS"
        print(
            f"[maint-mail] connecting {MAIL_USER} → {MAIL_IMAP_HOST}:{MAIL_IMAP_PORT} "
            f"({mode}, timeout={IMAP_TIMEOUT}s)",
            flush=True,
        )
        try:
            if IMAP_USE_SSL:
                mail = imaplib.IMAP4_SSL(
                    MAIL_IMAP_HOST,
                    MAIL_IMAP_PORT,
                    ssl_context=ctx,
                    timeout=IMAP_TIMEOUT,
                )
            else:
                mail = imaplib.IMAP4(
                    MAIL_IMAP_HOST,
                    MAIL_IMAP_PORT,
                    timeout=IMAP_TIMEOUT,
                )
                mail.starttls(ssl_context=ctx)
            mail.login(MAIL_USER, MAIL_PASSWORD)
        except OSError as ex:
            raise OSError(
                f"Cannot reach IMAP {MAIL_IMAP_HOST}:{MAIL_IMAP_PORT} ({mode}) — "
                f"network/firewall/DNS or wrong host/port (not a password error). "
                f"Original: {ex!r}"
            ) from ex
        except imaplib.IMAP4.error as ex:
            err = (ex.args[0] if ex.args else b"") or b""
            if isinstance(err, bytes):
                err_s = err.decode("utf-8", errors="replace").lower()
            else:
                err_s = str(err).lower()
            if "wrong authorization" in err_s or "authentication failed" in err_s:
                raise RuntimeError(
                    "Lark IMAP login rejected (wrong authorization code). "
                    f"Use a Lark Mail **client/app password** for {MAIL_USER} — "
                    "not the normal web-login password. "
                    "Lark desktop → Email → Settings → Third-party client / 专用密码."
                ) from ex
            raise
        return mail

    def _process_one(
        self,
        mail: imaplib.IMAP4_SSL,
        uid: bytes,
        state: dict[str, Any],
    ) -> None:
        import maintenance

        uid_s = uid.decode() if isinstance(uid, bytes) else str(uid)
        entries: list[dict[str, Any]] = state["entries"]

        typ, data = mail.uid("fetch", uid, "(RFC822)")
        if typ != "OK" or not data or not data[0]:
            print(f"[maint-mail] fetch failed uid={uid_s}", flush=True)
            return

        raw = data[0][1]
        if not isinstance(raw, (bytes, bytearray)):
            return

        msg = email.message_from_bytes(raw)
        subject = _decode_mime_header(msg.get("Subject"))
        if not subject_matches(subject):
            print(
                f"[maint-mail] skip uid={uid_s} (subject not TINC- / [Service Desk]): {subject!r}",
                flush=True,
            )
            return

        if _already_processed_uid(entries, uid_s):
            return

        body = extract_body_from_message(msg)
        pipeline_in = build_pipeline_input(subject, body)
        chash = _content_hash(pipeline_in)

        dup = _find_duplicate_title_content(entries, subject, chash)
        if dup:
            notice = format_duplicate_notice(subject, dup.get("title") or subject)
            self._send_lark(TARGET_CHAT_ID, notice)
            _record_processed(
                entries,
                imap_uid=uid_s,
                message_id=msg.get("Message-ID") or "",
                title=subject,
                content_hash=chash,
            )
            mail.uid("store", uid, "+FLAGS", "(\\Seen)")
            print(f"[maint-mail] duplicate ignored uid={uid_s} title={subject!r}", flush=True)
            return

        from_addr = _decode_mime_header(msg.get("From"))
        when: str | None = None
        try:
            dt = parsedate_to_datetime(msg.get("Date") or "")
            if dt:
                when = dt.isoformat()
        except Exception:
            when = None

        token = self._get_token()
        first_reply, second_reply = maintenance.process_maintenance_pipeline(
            pipeline_in, token
        )

        header = format_incoming_header(subject, from_addr, when)
        if (first_reply or "").strip():
            self._send_lark(TARGET_CHAT_ID, header + first_reply)
        if (second_reply or "").strip():
            self._send_lark(TARGET_CHAT_ID, second_reply)

        _record_processed(
            entries,
            imap_uid=uid_s,
            message_id=msg.get("Message-ID") or "",
            title=subject,
            content_hash=chash,
        )
        mail.uid("store", uid, "+FLAGS", "(\\Seen)")
        print(f"[maint-mail] processed uid={uid_s} title={subject!r}", flush=True)

    def _process_uid_list(self, mail: imaplib.IMAP4, uids: list[bytes], *, label: str) -> None:
        if not uids:
            print(f"[maint-mail] {label}: 0 message(s) to check", flush=True)
            return
        print(f"[maint-mail] {label}: checking {len(uids)} message(s)", flush=True)
        with _state_lock:
            state = _load_state()
            for uid in uids:
                if self._stop.is_set():
                    break
                try:
                    self._process_one(mail, uid, state)
                except Exception as ex:
                    print(f"[maint-mail] process error uid={uid!r}: {ex!r}", flush=True)
            _save_state(state)

    def _poll_unseen(self, mail: imaplib.IMAP4) -> None:
        mail.select("INBOX", readonly=False)
        typ, data = mail.uid("search", None, "UNSEEN")
        if typ != "OK" or not data or not data[0]:
            print("[maint-mail] poll UNSEEN: 0 (or search failed)", flush=True)
            return
        uids = data[0].split()
        self._process_uid_list(mail, uids, label="UNSEEN")

    def _backfill_inbox(self, mail: imaplib.IMAP4) -> None:
        """Process matching INBOX mail already marked read (skipped by UNSEEN-only poll)."""
        if not BACKFILL_ON_START:
            return
        mail.select("INBOX", readonly=False)
        typ, data = mail.uid("search", None, "ALL")
        if typ != "OK" or not data or not data[0]:
            print("[maint-mail] backfill: search ALL failed", flush=True)
            return
        uids = data[0].split()
        if not uids:
            return
        # Newest first, cap volume
        uids = list(reversed(uids))[:BACKFILL_LIMIT]
        self._process_uid_list(mail, uids, label=f"backfill (last {len(uids)})")

    def _run_idle_or_poll(self, mail: imaplib.IMAP4_SSL) -> None:
        """Use IMAP IDLE when available; otherwise tight UNSEEN polling."""
        if hasattr(mail, "idle") and hasattr(mail, "idle_done"):
            while not self._stop.is_set():
                try:
                    mail.select("INBOX", readonly=False)
                    mail.idle()
                    if self._stop.wait(timeout=0.5):
                        try:
                            mail.idle_done()
                        except Exception:
                            pass
                        break
                    try:
                        mail.idle_done()
                    except Exception:
                        pass
                    self._poll_unseen(mail)
                except Exception as ex:
                    print(f"[maint-mail] IDLE loop error: {ex!r}", flush=True)
                    if self._stop.wait(timeout=POLL_SECONDS):
                        break
            return

        while not self._stop.is_set():
            self._poll_unseen(mail)
            if self._stop.wait(timeout=POLL_SECONDS):
                break

    def run_forever(self) -> None:
        backoff = POLL_SECONDS
        while not self._stop.is_set():
            mail: imaplib.IMAP4_SSL | None = None
            try:
                mail = self._connect()
                print(
                    f"[maint-mail] connected {MAIL_USER}@{MAIL_IMAP_HOST}:{MAIL_IMAP_PORT} "
                    f"→ chat {TARGET_CHAT_ID}",
                    flush=True,
                )
                self._backfill_inbox(mail)
                self._poll_unseen(mail)
                backoff = POLL_SECONDS
                self._run_idle_or_poll(mail)
            except Exception as ex:
                print(
                    f"[maint-mail] connection error ({MAIL_IMAP_HOST}:{MAIL_IMAP_PORT}): {ex!r}",
                    flush=True,
                )
                if self._stop.wait(timeout=min(backoff, 60)):
                    break
                backoff = min(backoff * 2, 120)
            finally:
                if mail is not None:
                    try:
                        mail.logout()
                    except Exception:
                        pass

    def stop(self) -> None:
        self._stop.set()


def start_maintenance_mail_watcher(
    *,
    send_message_func: Callable[[str, str], Any],
    get_token_func: Callable[[], str | None],
) -> bool:
    """
    Start background IMAP watcher if ``MAINTENANCE_MAIL_PASSWORD`` is set.
    Returns True if the thread was started.
    """
    global _watcher_started
    if _watcher_started:
        return True
    if not MAIL_PASSWORD:
        print(
            "[maint-mail] not started — set MAINTENANCE_MAIL_PASSWORD in .env",
            flush=True,
        )
        return False
    if not TARGET_CHAT_ID:
        print("[maint-mail] not started — MAINTENANCE_MAIL_TARGET_CHAT_ID empty", flush=True)
        return False

    watcher = MaintenanceMailWatcher(
        send_message_func=send_message_func,
        get_token_func=get_token_func,
    )

    def _target() -> None:
        try:
            watcher.run_forever()
        except Exception as ex:
            print(f"[maint-mail] watcher exited: {ex!r}", flush=True)

    threading.Thread(target=_target, name="maintenance-mail-imap", daemon=True).start()
    _watcher_started = True
    return True
