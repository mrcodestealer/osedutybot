#!/usr/bin/env python3
"""
IMAP watcher for om@hotelstotsenberg.com (or MAINTENANCE_MAIL_USER).

Processes inbox messages whose subject **literally** starts with ``TINC-`` or
``[Service Desk]`` (not ``Re:`` / ``Fwd:`` replies), **and** whose From address is
``no-reply-evolution@evolution.com`` (Jira) or ``servicedesk@evolution.com`` (Service Desk).
Runs the same pipeline as ``/m``, and posts to a fixed Lark group.

State file: ``maintenance.json`` (titles + content hashes + IMAP UIDs) to avoid
re-processing after bot restart.
"""

from __future__ import annotations

import email
import hashlib
import imaplib
import json
import maintenance as _maint_mod
import os
import re
import smtplib
import ssl
import threading
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from email.header import Header
from email.header import decode_header
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid, parsedate_to_datetime
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
    or "oc_9de3d63fc589df6feeb9b0bee9c45b72"
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
# Max messages per poll (today + TINC / [Service Desk], seen and unread).
POLL_LIMIT = int(
    os.getenv("MAINTENANCE_MAIL_POLL_LIMIT", "").strip()
    or os.getenv("MAINTENANCE_MAIL_BACKFILL_LIMIT", "").strip()
    or "50"
)
# Max UIDs from tight SUBJECT search (TINC- / [Service Desk]); no arbitrary «newest 50» drop.
SUBJECT_SEARCH_MAX = int(
    os.getenv("MAINTENANCE_MAIL_SUBJECT_MAX", "").strip() or "300"
)
MAIL_TZ = (os.getenv("MAINTENANCE_MAIL_TZ", "").strip() or "Asia/Shanghai")
MAIL_VERBOSE = (os.getenv("MAINTENANCE_MAIL_VERBOSE", "").strip() or "0") in (
    "1",
    "true",
    "yes",
    "on",
)
FORWARD_ENABLED = (os.getenv("MAINTENANCE_MAIL_FORWARD_ENABLED", "").strip() or "1") not in (
    "0",
    "false",
    "no",
    "off",
)
FORWARD_TO = (
    os.getenv("MAINTENANCE_MAIL_FORWARD_TO", "").strip()
    or "evolive.maintenance@om.hotelstotsenberg.com"
)
FORWARD_TO_NAME = (
    os.getenv("MAINTENANCE_MAIL_FORWARD_TO_NAME", "").strip()
    or "SNSoft - OM - evolive.maintenance"
)
FORWARD_CC = (
    os.getenv("MAINTENANCE_MAIL_FORWARD_CC", "").strip()
    or "om@hotelstotsenberg.com"
)
FORWARD_FROM_NAME = (
    os.getenv("MAINTENANCE_MAIL_FORWARD_FROM_NAME", "").strip() or "OM-PH"
)
NOT_CP_REPLY_CC_NAME = (
    os.getenv("MAINTENANCE_MAIL_NOT_CP_CC_NAME", "").strip() or "CP OM Duty"
)
SMTP_HOST = (
    os.getenv("MAINTENANCE_MAIL_SMTP_HOST", "").strip()
    or "smtp.larksuite.com"
)
SMTP_PORT = int(
    os.getenv("MAINTENANCE_MAIL_SMTP_PORT", "").strip() or "465"
)
# Comma-separated IMAP mailboxes (Lark folder names; quote not needed in .env).
MAIL_IMAP_FOLDERS = [
    f.strip()
    for f in (
        os.getenv("MAINTENANCE_MAIL_IMAP_FOLDERS", "").strip()
        or os.getenv("maintenance_mail_imap_folders", "").strip()
        or "INBOX,OSE Pending"
    ).split(",")
    if f.strip()
]

_state_lock = threading.Lock()


def _imap_mailbox_name(folder: str) -> str:
    """Quote folder names with spaces for IMAP SELECT."""
    name = (folder or "").strip() or "INBOX"
    if re.search(r'[\s"\']', name):
        return '"' + name.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return name


def _select_mail_folder(mail: imaplib.IMAP4, folder: str) -> bool:
    mailbox = _imap_mailbox_name(folder)
    try:
        typ, data = mail.select(mailbox, readonly=False)
    except Exception as ex:
        print(f"[maint-mail] SELECT {folder!r} failed: {ex!r}", flush=True)
        return False
    if typ != "OK":
        print(f"[maint-mail] SELECT {folder!r} not OK: {data!r}", flush=True)
        return False
    return True


def _uid_key(folder: str, uid: str) -> str:
    return f"{folder}:{uid}"


def _local_tz() -> ZoneInfo | timezone:
    try:
        return ZoneInfo(MAIL_TZ)
    except Exception:
        return timezone.utc


def _imap_since_today() -> str:
    """IMAP SINCE = start of local calendar day."""
    return datetime.now(_local_tz()).strftime("%d-%b-%Y")


def _imap_since_lookback() -> str:
    """
    IMAP internal dates are often UTC — mail at 01:32 CST may still be «yesterday» in UTC.
    Search from (local today − 1 day); keep only local-today in ``_received_today``.
    """
    d = datetime.now(_local_tz()).date() - timedelta(days=1)
    return d.strftime("%d-%b-%Y")


def _received_today(when: str | None) -> bool:
    if not (when or "").strip():
        return False
    try:
        dt = datetime.fromisoformat(when.strip().replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        try:
            tz = ZoneInfo(MAIL_TZ)
        except Exception:
            tz = timezone.utc
        return dt.astimezone(tz).date() == datetime.now(tz).date()
    except ValueError:
        return False


def _merge_uid_lists(*groups: list[bytes]) -> list[bytes]:
    seen: set[bytes] = set()
    out: list[bytes] = []
    for group in groups:
        for u in group:
            if u not in seen:
                seen.add(u)
                out.append(u)
    return sorted(out, key=lambda x: int(x))


def _uid_search(mail: imaplib.IMAP4, criteria: str) -> list[bytes]:
    """Run UID SEARCH; return [] if the mailbox response exceeds imaplib 1MB line limit."""
    try:
        typ, data = mail.uid("search", None, criteria)
    except imaplib.IMAP4.error as ex:
        err = str(ex).lower()
        if "1000000" in err or "too large" in err:
            print(
                f"[maint-mail] UID SEARCH response too large (criteria={criteria!r}); "
                "narrow date/subject search or reduce MAINTENANCE_MAIL_POLL_LIMIT.",
                flush=True,
            )
            return []
        raise
    if typ != "OK" or not data or not data[0]:
        return []
    return data[0].split()
_watcher_started = False


def subject_matches(subject: str) -> bool:
    """
    True only when the subject **starts** with ``TINC-`` or ``[Service Desk]``.

    ``Re: TINC-…`` / ``Fwd: TINC-…`` are **not** matched (QA acks, thread replies).
    """
    if _maint_mod.subject_should_ignore(subject):
        return False
    s = (subject or "").strip()
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


def _format_forward_date(msg: email.message.Message) -> str:
    """e.g. ``Fri, 22 May 2026 08:07:10 +0000 (UTC)``"""
    raw = (msg.get("Date") or "").strip()
    if not raw:
        return ""
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        tz = dt.strftime("%z") or "+0000"
        line = dt.strftime(f"%a, %d %b %Y %H:%M:%S {tz}")
        if tz == "+0000":
            line += " (UTC)"
        return line
    except Exception:
        return raw


def build_forwarded_message_body(msg: email.message.Message) -> str:
    """Gmail-style forwarded block + original plain body (launched / to_cp path)."""
    from_hdr = _decode_mime_header(msg.get("From")) or "Unknown"
    subj = _decode_mime_header(msg.get("Subject")) or ""
    date_line = _format_forward_date(msg)
    original = extract_body_from_message(msg)
    header = [
        "---------- Forwarded message ---------",
        f"From: {from_hdr}",
    ]
    if date_line:
        header.append(f"Date: {date_line}")
    header.append(f"Subject: {subj}")
    header.append("")
    if original:
        return "\n".join(header) + original
    return "\n".join(header)


def _html_to_text(html: str) -> str:
    import html as html_mod

    t = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", html)
    t = re.sub(r"(?is)<br\s*/?>", "\n", t)
    t = re.sub(r"(?is)</p\s*>", "\n", t)
    t = re.sub(r"<[^>]+>", "", t)
    t = html_mod.unescape(t)
    t = re.sub(r"(?m)^\s*>\s*", "", t)
    return _normalize_body(t)


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


def _find_duplicate_ticket(
    entries: list[dict[str, Any]], ticket_id: str, content_hash: str
) -> dict[str, Any] | None:
    """Same ticket + same body → duplicate; same ticket + different body → new email."""
    tid = (ticket_id or "").strip().upper()
    if not tid:
        return None
    for ent in reversed(entries):
        if (ent.get("ticket_id") or "").strip().upper() != tid:
            continue
        if ent.get("content_hash") == content_hash:
            return ent
    return None


def _already_processed_uid(entries: list[dict[str, Any]], uid_key: str) -> bool:
    key = str(uid_key)
    for e in entries:
        stored = str(e.get("imap_uid") or "")
        if stored == key:
            return True
        # Legacy entries: bare uid only (assume INBOX)
        if ":" not in stored and key.endswith(":" + stored):
            return True
    return False


def _reply_subject(subject: str) -> str:
    s = (subject or "").strip() or "Maintenance"
    if re.match(r"^re:\s", s, re.IGNORECASE):
        return s
    return f"Re: {s}"


def forward_maintenance_email(
    *,
    subject: str,
    original_msg: email.message.Message | None = None,
) -> None:
    """SMTP forward from om@… — Gmail-style block + original body → evolive + Cc om@."""
    if not MAIL_PASSWORD:
        raise RuntimeError("MAINTENANCE_MAIL_PASSWORD not set")
    subj = (subject or "").strip() or "Maintenance"
    body = (
        build_forwarded_message_body(original_msg)
        if original_msg is not None
        else ""
    )
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = Header(subj, "utf-8")
    msg["From"] = formataddr((FORWARD_FROM_NAME, MAIL_USER))
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()
    msg["To"] = formataddr((FORWARD_TO_NAME, FORWARD_TO))
    msg["Cc"] = FORWARD_CC
    recipients = [FORWARD_TO, FORWARD_CC]
    route = f"{FORWARD_TO} cc={FORWARD_CC}"

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=IMAP_TIMEOUT, context=ctx) as smtp:
        smtp.login(MAIL_USER, MAIL_PASSWORD)
        smtp.sendmail(MAIL_USER, recipients, msg.as_string())
    print(f"[maint-mail] forwarded {subj!r} → {route}", flush=True)


def reply_not_in_cp_email(
    *,
    subject: str,
    original_msg: email.message.Message | None = None,
) -> None:
    """
    NOT IN CP notice: body ``NOT IN CP WEBSITE``, ``Re:`` subject, blank To,
    Cc = om@ only (no evolive, no reply to ticket sender).
    """
    if not MAIL_PASSWORD:
        raise RuntimeError("MAINTENANCE_MAIL_PASSWORD not set")
    subj = _reply_subject(subject)
    msg = MIMEText(_maint_mod.NOT_IN_CP_WEBSITE_BODY, "plain", "utf-8")
    msg["Subject"] = Header(subj, "utf-8")
    msg["From"] = formataddr((FORWARD_FROM_NAME, MAIL_USER))
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()
    msg["Cc"] = formataddr((NOT_CP_REPLY_CC_NAME, FORWARD_CC))
    if original_msg is not None:
        orig_mid = (original_msg.get("Message-ID") or "").strip()
        if orig_mid:
            msg["In-Reply-To"] = orig_mid
            refs = (original_msg.get("References") or "").strip()
            msg["References"] = f"{refs} {orig_mid}".strip() if refs else orig_mid
    recipients = [FORWARD_CC]
    route = f"Cc={FORWARD_CC} only (To blank)"

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=IMAP_TIMEOUT, context=ctx) as smtp:
        smtp.login(MAIL_USER, MAIL_PASSWORD)
        smtp.sendmail(MAIL_USER, recipients, msg.as_string())
    print(f"[maint-mail] NOT IN CP reply {subj!r} → {route}", flush=True)


def _record_processed(
    entries: list[dict[str, Any]],
    *,
    imap_uid: str,
    message_id: str,
    title: str,
    content_hash: str,
    ticket_id: str = "",
) -> None:
    entries.append(
        {
            "imap_uid": str(imap_uid),
            "message_id": (message_id or "").strip(),
            "title": (title or "").strip(),
            "content_hash": content_hash,
            "ticket_id": (ticket_id or "").strip().upper(),
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
    )


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

    def _send_lark_card(self, chat_id: str, card: dict[str, Any]) -> None:
        try:
            payload = json.dumps(card, ensure_ascii=False)
            resp = self._send(chat_id, payload, msg_type="interactive")
            if isinstance(resp, dict) and resp.get("code") not in (None, 0):
                print(
                    f"[maint-mail] interactive card failed chat={chat_id}: {resp}",
                    flush=True,
                )
        except Exception as ex:
            print(f"[maint-mail] send card error: {ex!r}", flush=True)

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

    def _fetch_header_preview(
        self, mail: imaplib.IMAP4, uid: bytes
    ) -> tuple[str, str | None, str]:
        """Lightweight SUBJECT + DATE + FROM peek (before downloading full RFC822)."""
        uid_s = uid.decode() if isinstance(uid, bytes) else str(uid)
        try:
            typ, data = mail.uid(
                "fetch",
                uid,
                "(BODY.PEEK[HEADER.FIELDS (SUBJECT DATE FROM)])",
            )
        except imaplib.IMAP4.error as ex:
            print(f"[maint-mail] header fetch failed uid={uid_s}: {ex!r}", flush=True)
            return "", None, ""
        if typ != "OK" or not data:
            return "", None, ""
        for part in data:
            if not isinstance(part, tuple) or len(part) < 2:
                continue
            chunk = part[1]
            if isinstance(chunk, (bytes, bytearray)) and chunk:
                msg = email.message_from_bytes(chunk)
                subj = _decode_mime_header(msg.get("Subject"))
                from_addr = _decode_mime_header(msg.get("From"))
                when: str | None = None
                try:
                    dt = parsedate_to_datetime(msg.get("Date") or "")
                    if dt:
                        when = dt.isoformat()
                except Exception:
                    when = None
                return subj, when, from_addr
        return "", None, ""

    def _process_one(
        self,
        mail: imaplib.IMAP4,
        uid: bytes,
        state: dict[str, Any],
        *,
        folder: str = "INBOX",
    ) -> None:
        import maintenance

        uid_s = uid.decode() if isinstance(uid, bytes) else str(uid)
        entries: list[dict[str, Any]] = state["entries"]
        store_key = _uid_key(folder, uid_s)

        if _already_processed_uid(entries, store_key):
            return

        subject, when, from_hdr = self._fetch_header_preview(mail, uid)

        if _maint_mod.from_should_ignore(from_hdr):
            ticket_skip = _maint_mod.extract_ticket_card_title(subject) or ""
            _record_processed(
                entries,
                imap_uid=store_key,
                message_id="",
                title=subject or "",
                content_hash="skip:from_self",
                ticket_id=ticket_skip,
            )
            mail.uid("store", uid, "+FLAGS", "(\\Seen)")
            print(
                f"[maint-mail] skip uid={uid_s} (from OM-PH / om@): {subject!r}",
                flush=True,
            )
            return

        if not subject_matches(subject):
            if subject and MAIL_VERBOSE:
                print(
                    f"[maint-mail] skip uid={uid_s} (subject not TINC- / [Service Desk]): {subject!r}",
                    flush=True,
                )
            return

        if not _maint_mod.from_is_allowed_sender(from_hdr):
            ticket_skip = _maint_mod.extract_ticket_card_title(subject) or ""
            _record_processed(
                entries,
                imap_uid=store_key,
                message_id="",
                title=subject or "",
                content_hash="skip:from_not_allowed",
                ticket_id=ticket_skip,
            )
            mail.uid("store", uid, "+FLAGS", "(\\Seen)")
            if MAIL_VERBOSE:
                print(
                    f"[maint-mail] skip uid={uid_s} (sender not allowed): {from_hdr!r}",
                    flush=True,
                )
            return

        if when and not _received_today(when):
            if MAIL_VERBOSE:
                print(
                    f"[maint-mail] skip uid={uid_s} (not local today {MAIL_TZ}): {subject!r}",
                    flush=True,
                )
            return

        try:
            typ, data = mail.uid("fetch", uid, "(RFC822)")
        except imaplib.IMAP4.error as ex:
            err = str(ex).lower()
            if "1000000" in err:
                print(
                    f"[maint-mail] skip uid={uid_s} (message >1MB IMAP limit): {subject!r}",
                    flush=True,
                )
                return
            raise
        if typ != "OK" or not data or not data[0]:
            print(f"[maint-mail] fetch failed uid={uid_s}", flush=True)
            return

        raw = data[0][1]
        if not isinstance(raw, (bytes, bytearray)):
            return

        msg = email.message_from_bytes(raw)
        subject = _decode_mime_header(msg.get("Subject")) or subject
        if not subject_matches(subject):
            return

        display_subj = maintenance.normalize_display_subject(subject)
        from_addr = _decode_mime_header(msg.get("From"))
        if maintenance.from_should_ignore(from_addr):
            ticket_skip = maintenance.extract_ticket_card_title(subject) or ""
            _record_processed(
                entries,
                imap_uid=store_key,
                message_id=msg.get("Message-ID") or "",
                title=display_subj,
                content_hash="skip:from_self",
                ticket_id=ticket_skip,
            )
            mail.uid("store", uid, "+FLAGS", "(\\Seen)")
            print(
                f"[maint-mail] skip uid={uid_s} (from OM-PH / om@): {display_subj!r}",
                flush=True,
            )
            return

        if not maintenance.from_is_allowed_sender(from_addr):
            ticket_skip = maintenance.extract_ticket_card_title(subject) or ""
            _record_processed(
                entries,
                imap_uid=store_key,
                message_id=msg.get("Message-ID") or "",
                title=display_subj,
                content_hash="skip:from_not_allowed",
                ticket_id=ticket_skip,
            )
            mail.uid("store", uid, "+FLAGS", "(\\Seen)")
            if MAIL_VERBOSE:
                print(
                    f"[maint-mail] skip uid={uid_s} (sender not allowed): {from_addr!r}",
                    flush=True,
                )
            return

        body = extract_body_from_message(msg)
        pipeline_in = build_pipeline_input(subject, body)
        chash = _content_hash(pipeline_in)
        ticket_id = maintenance.extract_ticket_card_title(subject, body) or ""

        dup_ticket = _find_duplicate_ticket(entries, ticket_id, chash)
        if dup_ticket:
            _record_processed(
                entries,
                imap_uid=store_key,
                message_id=msg.get("Message-ID") or "",
                title=display_subj,
                content_hash=chash,
                ticket_id=ticket_id,
            )
            mail.uid("store", uid, "+FLAGS", "(\\Seen)")
            print(
                f"[maint-mail] duplicate ticket ignored {folder} uid={uid_s} "
                f"ticket={ticket_id!r}",
                flush=True,
            )
            return

        dup = _find_duplicate_title_content(entries, display_subj, chash)
        if dup:
            _record_processed(
                entries,
                imap_uid=store_key,
                message_id=msg.get("Message-ID") or "",
                title=display_subj,
                content_hash=chash,
                ticket_id=ticket_id,
            )
            mail.uid("store", uid, "+FLAGS", "(\\Seen)")
            print(
                f"[maint-mail] duplicate ignored {folder} uid={uid_s} title={display_subj!r}",
                flush=True,
            )
            return

        if not when:
            try:
                dt = parsedate_to_datetime(msg.get("Date") or "")
                if dt:
                    when = dt.isoformat()
            except Exception:
                when = None

        if when and not _received_today(when):
            if MAIL_VERBOSE:
                print(
                    f"[maint-mail] skip uid={uid_s} (not local today {MAIL_TZ}): {display_subj!r}",
                    flush=True,
                )
            return

        token = self._get_token()
        first_reply, second_reply = maintenance.process_maintenance_pipeline(
            pipeline_in,
            token,
            email_subject=display_subj,
            received_at=when,
        )
        main_card = maintenance.build_maintenance_card(
            email_subject=display_subj,
            received_at=when,
            from_addr=from_addr,
            gamelist_section=first_reply or "",
            summary_section=second_reply or "",
            email_body=body,
        )
        self._send_lark_card(TARGET_CHAT_ID, main_card)

        to_cp, launched_names = maintenance.gamelist_has_launched(
            pipeline_in, token
        )
        if MAIL_VERBOSE and launched_names:
            print(
                f"[maint-mail] gamelist launched: {launched_names!r} to_cp={to_cp}",
                flush=True,
            )

        if FORWARD_ENABLED:
            try:
                if to_cp:
                    forward_maintenance_email(
                        subject=subject, original_msg=msg
                    )
                else:
                    reply_not_in_cp_email(
                        subject=subject, original_msg=msg
                    )
            except Exception as ex:
                action = "forward" if to_cp else "NOT IN CP reply"
                print(
                    f"[maint-mail] {action} failed uid={uid_s} ticket={ticket_id!r}: {ex!r}",
                    flush=True,
                )
                return

        done_card = maintenance.build_forward_done_card(
            subject, body, to_cp=to_cp
        )
        self._send_lark_card(TARGET_CHAT_ID, done_card)

        _record_processed(
            entries,
            imap_uid=store_key,
            message_id=msg.get("Message-ID") or "",
            title=display_subj,
            content_hash=chash,
            ticket_id=ticket_id,
        )
        mail.uid("store", uid, "+FLAGS", "(\\Seen)")
        print(
            f"[maint-mail] processed {folder} uid={uid_s} ticket={ticket_id!r} "
            f"title={display_subj!r}",
            flush=True,
        )

    def _prefilter_uids(
        self,
        mail: imaplib.IMAP4,
        uids: list[bytes],
        entries: list[dict[str, Any]],
        *,
        folder: str,
    ) -> tuple[list[bytes], dict[str, int]]:
        """
        Header-only pass (oldest first). Only UIDs that match TINC- / [Service Desk]
        and local-today (if Date known) proceed to full fetch.
        """
        stats = {
            "imap_hits": len(uids),
            "already_done": 0,
            "not_today": 0,
            "ignored": 0,
            "not_maintenance": 0,
            "todo": 0,
        }
        todo: list[bytes] = []
        for uid in uids:
            uid_s = uid.decode() if isinstance(uid, bytes) else str(uid)
            if _already_processed_uid(entries, _uid_key(folder, uid_s)):
                stats["already_done"] += 1
                continue
            subject, when, from_hdr = self._fetch_header_preview(mail, uid)
            if _maint_mod.from_should_ignore(from_hdr):
                stats["ignored"] += 1
                ticket_skip = _maint_mod.extract_ticket_card_title(subject) or ""
                _record_processed(
                    entries,
                    imap_uid=_uid_key(folder, uid_s),
                    message_id="",
                    title=subject or "",
                    content_hash="skip:from_self",
                    ticket_id=ticket_skip,
                )
                continue
            if _maint_mod.subject_should_ignore(subject):
                stats["ignored"] += 1
                if MAIL_VERBOSE:
                    print(
                        f"[maint-mail] ignore uid={uid_s} (subject filter): {subject!r}",
                        flush=True,
                    )
                continue
            if not subject_matches(subject):
                stats["not_maintenance"] += 1
                continue
            if not _maint_mod.from_is_allowed_sender(from_hdr):
                stats["ignored"] += 1
                ticket_skip = _maint_mod.extract_ticket_card_title(subject) or ""
                _record_processed(
                    entries,
                    imap_uid=_uid_key(folder, uid_s),
                    message_id="",
                    title=subject or "",
                    content_hash="skip:from_not_allowed",
                    ticket_id=ticket_skip,
                )
                if MAIL_VERBOSE:
                    print(
                        f"[maint-mail] ignore uid={uid_s} (sender not allowed): {from_hdr!r}",
                        flush=True,
                    )
                continue
            if when and not _received_today(when):
                stats["not_today"] += 1
                continue
            todo.append(uid)
        stats["todo"] = len(todo)
        return todo, stats

    def _process_uid_list(
        self,
        mail: imaplib.IMAP4,
        uids: list[bytes],
        *,
        label: str,
        folder: str = "INBOX",
    ) -> None:
        if not uids:
            print(f"[maint-mail] {label}: 0 IMAP hit(s)", flush=True)
            return
        with _state_lock:
            state = _load_state()
            entries: list[dict[str, Any]] = state["entries"]
            todo, stats = self._prefilter_uids(
                mail, uids, entries, folder=folder
            )
            if not todo:
                print(
                    f"[maint-mail] {label}: "
                    f"imap={stats['imap_hits']} "
                    f"done={stats['already_done']} "
                    f"ignored={stats['ignored']} "
                    f"not_today={stats['not_today']} "
                    f"not_maint={stats['not_maintenance']} "
                    f"→ 0 to process",
                    flush=True,
                )
                _save_state(state)
                return
            print(
                f"[maint-mail] {label}: "
                f"imap={stats['imap_hits']} → process {len(todo)}",
                flush=True,
            )
            for uid in todo:
                if self._stop.is_set():
                    break
                try:
                    self._process_one(mail, uid, state, folder=folder)
                except Exception as ex:
                    print(f"[maint-mail] process error uid={uid!r}: {ex!r}", flush=True)
            _save_state(state)

    def _uids_maintenance_subject_search(
        self, mail: imaplib.IMAP4, since: str
    ) -> list[bytes]:
        """IMAP SUBJECT search — tight tokens only (avoid broad «Service Desk»)."""
        return _merge_uid_lists(
            _uid_search(mail, f'(SINCE {since} SUBJECT "TINC-")'),
            _uid_search(mail, f'(SINCE {since} SUBJECT "[Service Desk]")'),
        )

    def _uids_broad_since(self, mail: imaplib.IMAP4, since: str) -> list[bytes]:
        """Fallback when SUBJECT search returns nothing (some Lark setups)."""
        uids = _uid_search(mail, f"(SINCE {since})")
        if not uids:
            return []
        print(
            f"[maint-mail] broad SINCE {since}: {len(uids)} mail(s) (cap {POLL_LIMIT}), "
            "filter TINC- / [Service Desk] in code",
            flush=True,
        )
        return uids[-POLL_LIMIT:]

    def _uids_today_matching(self, mail: imaplib.IMAP4) -> list[bytes]:
        """
        Prefer IMAP SUBJECT search (fast, only maintenance mail).
        Fall back to broad SINCE + in-code filter if the server ignores bracket subjects.
        """
        since_today = _imap_since_today()
        uids = self._uids_maintenance_subject_search(mail, since_today)
        if not uids:
            since_lb = _imap_since_lookback()
            print(
                f"[maint-mail] maintenance search SINCE {since_today} → 0; "
                f"retry SINCE {since_lb} ({MAIL_TZ} today still enforced in code)",
                flush=True,
            )
            uids = self._uids_maintenance_subject_search(mail, since_lb)
        if not uids:
            for since in (_imap_since_today(), _imap_since_lookback()):
                uids = self._uids_broad_since(mail, since)
                if uids:
                    break
        if not uids:
            return []
        if len(uids) > SUBJECT_SEARCH_MAX:
            print(
                f"[maint-mail] IMAP subject hits: {len(uids)} → cap {SUBJECT_SEARCH_MAX} "
                "(oldest first; raise MAINTENANCE_MAIL_SUBJECT_MAX if needed)",
                flush=True,
            )
            uids = uids[:SUBJECT_SEARCH_MAX]
        else:
            print(
                f"[maint-mail] IMAP subject hits: {len(uids)} (TINC- / [Service Desk])",
                flush=True,
            )
        return uids

    def _poll_today_folders(self, mail: imaplib.IMAP4) -> None:
        """Poll today in each configured folder (seen + unread)."""
        since = _imap_since_today()
        any_mail = False
        for folder in MAIL_IMAP_FOLDERS:
            if not _select_mail_folder(mail, folder):
                continue
            uids = self._uids_today_matching(mail)
            if not uids:
                print(
                    f"[maint-mail] {folder}: 0 mail since {since} ({MAIL_TZ})",
                    flush=True,
                )
                continue
            any_mail = True
            self._process_uid_list(
                mail,
                uids,
                label=f"{folder} today ({len(uids)})",
                folder=folder,
            )
        if not any_mail:
            print(
                f"[maint-mail] all folders empty for today ({MAIL_TZ}): {MAIL_IMAP_FOLDERS!r}",
                flush=True,
            )

    def _run_idle_or_poll(self, mail: imaplib.IMAP4) -> None:
        """Poll all configured folders; IDLE only when a single folder is set."""
        use_idle = (
            len(MAIL_IMAP_FOLDERS) == 1
            and hasattr(mail, "idle")
            and hasattr(mail, "idle_done")
        )
        if use_idle:
            folder = MAIL_IMAP_FOLDERS[0]
            while not self._stop.is_set():
                try:
                    if not _select_mail_folder(mail, folder):
                        if self._stop.wait(timeout=POLL_SECONDS):
                            break
                        continue
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
                    self._poll_today_folders(mail)
                except Exception as ex:
                    print(f"[maint-mail] IDLE loop error: {ex!r}", flush=True)
                    if self._stop.wait(timeout=POLL_SECONDS):
                        break
            return

        while not self._stop.is_set():
            self._poll_today_folders(mail)
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
                    f"→ chat {TARGET_CHAT_ID} folders={MAIL_IMAP_FOLDERS!r}",
                    flush=True,
                )
                self._poll_today_folders(mail)
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
