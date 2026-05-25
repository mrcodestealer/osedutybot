#!/usr/bin/env python3
"""
Generate a simplified maintenance summary from a full email.
Supports multiple email formats, including ongoing maintenance.

Env (optional):
  gamelist / GAMELIST — Lark **spreadsheet token** for the game list workbook.
  gamelistsheetid / GAMELISTSHEETID — single worksheet id (only this sheet is read).
  Sheet must include header columns ``游戏名称 / Games Name`` and
  ``遊戲入口圖 / Game entrance map`` (often on **row 2**): ``1`` = launched,
  ``0`` / empty / other = not launched. Each email game is matched against **游戏名称**.
"""

from __future__ import annotations

import json
import sys
import re
import os
import unicodedata
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()

GAMELIST_SPREADSHEET_TOKEN = (
    os.getenv("gamelist", "").strip() or os.getenv("GAMELIST", "").strip()
)
GAMELIST_SHEET_ID = (
    os.getenv("gamelistsheetid", "").strip()
    or os.getenv("GAMELISTSHEETID", "").strip()
)

_CARD_HEADER_TITLE_MAX = 100

_CHBOX_DIR = os.path.dirname(os.path.abspath(__file__))
MAINTENANCE_STATE_PATH = os.path.join(_CHBOX_DIR, "maintenance.json")

# Lark @mentions for maintenance cards (display names come from open_id).
_CS_TEAM_OPEN_ID = "ou_c927a378e9b464741c67b61c1641577b"
_QA_SUPPORT_OPEN_ID = "ou_0342007237c6c1aa262acae839acb7c6"

# Substrings in email subject/title → skip (no card, no pipeline).
_SUBJECT_IGNORE_MARKERS = ("c88live_ow.ph",)


def _parse_from_email_address(from_addr: str | None) -> str:
    """Email address from ``Name <user@host>`` or bare ``user@host``."""
    raw = (from_addr or "").strip()
    if not raw:
        return ""
    m = re.search(r"<([^>]+)>", raw)
    if m:
        return m.group(1).strip().lower()
    m2 = re.search(r"[\w.+-]+@[\w.-]+\.\w+", raw)
    return (m2.group(0) if m2 else raw).strip().lower()


_MAINTENANCE_ALLOWED_SENDERS_DEFAULT = (
    "no-reply-evolution@evolution.com",
    "servicedesk@evolution.com",
)


def _allowed_sender_emails() -> frozenset[str]:
    emails = {e.strip().lower() for e in _MAINTENANCE_ALLOWED_SENDERS_DEFAULT if e.strip()}
    extra = (
        os.getenv("MAINTENANCE_ALLOWED_FROM", "").strip()
        or os.getenv("maintenance_allowed_from", "").strip()
    )
    for token in extra.split(","):
        t = token.strip().lower()
        if t:
            emails.add(t)
    return frozenset(emails)


def from_is_allowed_sender(from_addr: str | None) -> bool:
    """
    Only Evolution Jira (``no-reply-evolution@evolution.com``) and Service Desk
    (``servicedesk@evolution.com``). Display names may vary; match is by address.
    """
    email = _parse_from_email_address(from_addr)
    if not email:
        return False
    return email in _allowed_sender_emails()


def from_should_ignore(from_addr: str | None) -> bool:
    """Skip outbound copies from our own mailbox (OM-PH / om@…)."""
    raw = (from_addr or "").strip()
    if not raw:
        return False
    low = raw.lower()
    if "om@hotelstotsenberg.com" in low:
        return True
    if re.search(r"\bom-ph\b", raw, re.IGNORECASE):
        return True
    extra = (
        os.getenv("MAINTENANCE_IGNORE_FROM", "").strip()
        or os.getenv("maintenance_ignore_from", "").strip()
    )
    for token in extra.split(","):
        t = token.strip().lower()
        if t and t in low:
            return True
    return False


def subject_should_ignore(subject: str | None) -> bool:
    """True when this maintenance email should be skipped (e.g. C88live_ow.ph tickets)."""
    s = (subject or "").lower()
    if not s:
        return False
    for marker in _SUBJECT_IGNORE_MARKERS:
        if marker in s:
            return True
    extra = (
        os.getenv("MAINTENANCE_IGNORE_SUBJECT_CONTAINS", "").strip()
        or os.getenv("maintenance_ignore_subject_contains", "").strip()
    )
    for token in extra.split(","):
        t = token.strip().lower()
        if t and t in s:
            return True
    return False


def normalize_display_subject(subject: str) -> str:
    """
    Display subject from ``[Service Desk]`` or ``TINC-`` onward (drop leading Fw:/Re:/Fwd:).
    """
    s = (subject or "").strip()
    for _ in range(12):
        if re.match(r"^(?:TINC-|\[Service Desk\])", s, re.IGNORECASE):
            return s
        m = re.match(r"^(?:Re|Fwd|FW|Fw|Aw):\s*", s, re.IGNORECASE)
        if m:
            s = s[m.end() :].strip()
            continue
        hit = re.search(r"(\[Service Desk\]|TINC-)", s, re.IGNORECASE)
        if hit:
            return s[hit.start() :].strip()
        break
    return s


def _clean_status_for_title(raw: str) -> str:
    """One line only — avoid capturing the next field (e.g. ``Date`` after ``Fixed``)."""
    v = (raw or "").strip().splitlines()[0].strip().rstrip("/").strip()
    v = re.split(r"\s+(?:Date|Start|End|Reason)\s*:", v, maxsplit=1, flags=re.I)[0].strip()
    return v


def extract_status_for_card(subject: str, extra_text: str | None = None) -> str | None:
    """Status for card header — from subject/body ``Table availability:`` / ``Status:``."""
    hay = f"{subject or ''}\n{extra_text or ''}"
    for pattern in (
        r"Table\s+availability\s*:\s*([^\n\r/|]+)",
        r"(?<![\w])Status\s*:\s*([^\n\r/|]+)",
    ):
        m = re.search(pattern, hay, re.IGNORECASE)
        if m:
            val = _clean_status_for_title(m.group(1))
            if val and val.lower() != "unknown":
                return val
    if (extra_text or "").strip():
        info = extract_info(extra_text, email_subject=subject)
        st = (info.get("status") or "").strip()
        if st and st.lower() != "unknown":
            return st
    return None


FORWARD_DONE_BODY = "Done forward to junchen@snsoft.my"
NOT_IN_CP_WEBSITE_BODY = "NOT IN CP WEBSITE\nFrom Duty Bot Auto Reply"
FORWARD_DONE_NOT_CP_BODY = (
    "NOT IN CP WEBSITE — emailed Cc om@hotelstotsenberg.com only "
    "(no launched games on gamelist · 遊戲入口圖=1)."
)


def gamelist_has_launched(
    email_text: str, tenant_access_token: str | None
) -> tuple[bool, list[str]]:
    """
    True when at least one affected table has 遊戲入口圖 = 1 on gamelist.
    False when none launched, gamelist missing, or no table names found.
    """
    tok = (tenant_access_token or "").strip()
    ss = GAMELIST_SPREADSHEET_TOKEN
    sid = GAMELIST_SHEET_ID
    if not ss or not sid or not tok:
        return False, []
    try:
        grid = _fetch_sheet_values(tok, ss, sid)
    except Exception:
        return False, []
    if _find_header_row_and_cols(grid) is None:
        return False, []
    candidates = extract_candidate_game_names(email_text)
    if not candidates:
        return False, []
    launched: list[str] = []
    for g in candidates:
        if _row_launched_for_game(grid, g, "") is True:
            launched.append(g)
    return (len(launched) > 0, launched)


def build_forward_done_title(
    subject: str, email_body: str | None = None
) -> str:
    """Lark notice title: ``TINC-705939 Fixed`` (ticket + short status)."""
    ticket = extract_ticket_card_title(subject, email_body) or "Maintenance"
    status = extract_status_for_card(subject, email_body) or ""
    if status:
        return f"{ticket} {status}".strip()
    return ticket


def build_forward_done_card(
    subject: str,
    email_body: str | None = None,
    *,
    to_cp: bool = True,
) -> dict[str, Any]:
    """Small card after processing: green when email was forwarded to CP; orange when Lark-only."""
    title = build_forward_done_title(subject, email_body)
    if len(title) > _CARD_HEADER_TITLE_MAX:
        title = title[: _CARD_HEADER_TITLE_MAX - 3] + "..."
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "green" if to_cp else "orange",
            "title": {"tag": "plain_text", "content": title},
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": FORWARD_DONE_BODY if to_cp else FORWARD_DONE_NOT_CP_BODY,
                },
            }
        ],
    }


def build_card_header_title(
    subject: str,
    *,
    email_body: str | None = None,
    received_at: str | None = None,
) -> str:
    """Card header: ``SD-7040923 · Status : Affected`` (ticket + status when known)."""
    ticket = extract_ticket_card_title(subject, email_body)
    status_raw = extract_status_for_card(subject, email_body)
    parts: list[str] = []
    if ticket:
        parts.append(ticket)
    if status_raw:
        parts.append(f"Status : {format_status_display(status_raw)}")
    if parts:
        title = " · ".join(parts)
    else:
        title = format_received_at(received_at) or "Maintenance"
    if len(title) > _CARD_HEADER_TITLE_MAX:
        title = title[: _CARD_HEADER_TITLE_MAX - 3] + "..."
    return title


def extract_ticket_card_title(subject: str, extra_text: str | None = None) -> str | None:
    """
    Card header: ``SD-7041104`` or ``TINC-704380`` when subject/body contains
    SD/TINC + ticket number (6+ digits).
    """
    haystack = f"{subject or ''}\n{extra_text or ''}"
    for prefix in ("TINC", "SD"):
        m = re.search(
            rf"(?:{prefix})[-\s]?(\d{{6,8}})\b",
            haystack,
            re.IGNORECASE,
        )
        if m:
            return f"{prefix.upper()}-{m.group(1)}"
    return None


def ticket_id_tinc_style(subject: str, extra_text: str | None = None) -> str | None:
    """Lark card ticket id — always ``TINC-123456`` (Service Desk ``SD-`` → ``TINC-``)."""
    raw = extract_ticket_card_title(subject, extra_text)
    if not raw:
        return None
    m = re.match(r"^(?:TINC|SD)[-\s]?(\d{6,8})\b", raw.strip(), re.IGNORECASE)
    if m:
        return f"TINC-{m.group(1)}"
    return raw.upper()


_CANCEL_BODY_RE = re.compile(
    r"(?:this\s+message\s+is\s+to\s+inform\s+that\s+)?(?:the\s+)?"
    r"(?:technical\s+)?maintenance\s+has\s+been\s+cancell?ed",
    re.IGNORECASE,
)


def is_maintenance_cancelled_email(body: str | None) -> bool:
    """True when the email body is a maintenance cancellation notice (not the original schedule)."""
    text = (body or "").replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        return False
    return bool(_CANCEL_BODY_RE.search(text))


def extract_cancel_notice_text(body: str | None) -> str:
    """Core cancellation lines for the Lark card (notice + apology)."""
    lines = [
        _clean_email_line(ln)
        for ln in (body or "").replace("\r\n", "\n").replace("\r", "\n").splitlines()
    ]
    notice: str | None = None
    apology: str | None = None
    for ln in lines:
        if not ln:
            continue
        if _CANCEL_BODY_RE.search(ln):
            notice = ln.strip()
        elif re.search(r"^we\s+apologize\s+for\s+the\s+inconvenience", ln, re.I):
            apology = ln.strip()
    out: list[str] = []
    if notice:
        out.append(notice)
    else:
        out.append(
            "This message is to inform that the Technical maintenance has been cancelled."
        )
    if apology:
        out.append(apology)
    elif not any("apologize" in x.lower() for x in out):
        out.append("We apologize for the inconvenience.")
    return "\n".join(out)


def parse_service_desk_date_from_subject(subject: str) -> str:
    """``12/May/26`` from ``[Service Desk] … / 12/May/26 05:30 UTC / …``."""
    s = normalize_display_subject(subject)
    m = re.search(r"/\s*(\d{1,2}/[A-Za-z]{3}/\d{2,4})\b", s)
    return m.group(1).strip() if m else ""


_SD_SUBJECT_UTC_RE = re.compile(
    r"(\d{1,2}/[A-Za-z]{3}/\d{2,4})\s+(\d{1,2}:\d{2})\s*UTC",
    re.IGNORECASE,
)


def parse_service_desk_times_from_subject(subject: str) -> tuple[str, str]:
    """
    Start/end from Service Desk subject slashes, e.g.
    ``… / 27/May/26 07:35 UTC / … / (SD-7055392)``.
    """
    s = normalize_display_subject(subject)
    hits = _SD_SUBJECT_UTC_RE.findall(s)
    if not hits:
        return "Unknown", "Unknown"

    def _fmt(pair: tuple[str, str]) -> str:
        return f"{pair[0]} {pair[1]} UTC"

    start = _fmt(hits[0])
    end = _fmt(hits[1]) if len(hits) > 1 else "TBA"
    return start, end


def _apply_service_desk_utc_times(
    info: dict[str, Any], text: str, *, email_subject: str | None
) -> None:
    """Fill ``start_time`` / ``end_time`` from SD subject or slash-style UTC in text."""
    subj = (email_subject or "").strip()
    combined = f"{subj}\n{text}" if subj else text

    if info["start_time"] == "Unknown" or info["end_time"] == "Unknown":
        subj_start, subj_end = parse_service_desk_times_from_subject(subj)
        if info["start_time"] == "Unknown" and subj_start != "Unknown":
            info["start_time"] = subj_start
        if info["end_time"] == "Unknown" and subj_end != "Unknown":
            info["end_time"] = subj_end

    hits = _SD_SUBJECT_UTC_RE.findall(combined)
    if hits:
        def _fmt(pair: tuple[str, str]) -> str:
            return f"{pair[0]} {pair[1]} UTC"

        if info["start_time"] == "Unknown":
            info["start_time"] = _fmt(hits[0])
        if info["end_time"] == "Unknown":
            info["end_time"] = _fmt(hits[1]) if len(hits) > 1 else "TBA"


def _normalize_title_key(title: str) -> str:
    """Collapse whitespace for matching ``[Service Desk] …`` subject lines."""
    return re.sub(r"\s+", " ", (title or "").strip().lower())


def _ticket_match_keys(ticket_id: str) -> set[str]:
    """``SD-7004356`` and ``TINC-7004356`` match the same Service Desk ticket."""
    tid = (ticket_id or "").strip().upper()
    keys: set[str] = set()
    if tid:
        keys.add(tid)
    m = re.match(r"^(?:TINC|SD)[-\s]?(\d{6,8})\b", tid)
    if m:
        num = m.group(1)
        keys.add(f"SD-{num}")
        keys.add(f"TINC-{num}")
    return keys


def load_maintenance_state_entries() -> list[dict[str, Any]]:
    """Today's processed maintenance rows from ``maintenance.json`` (mail watcher)."""
    if not os.path.isfile(MAINTENANCE_STATE_PATH):
        return []
    try:
        with open(MAINTENANCE_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    if not isinstance(data, dict):
        return []
    entries = data.get("entries")
    return list(entries) if isinstance(entries, list) else []


def find_prior_maintenance_entry(
    entries: list[dict[str, Any]],
    display_subj: str,
    ticket_id: str,
) -> dict[str, Any] | None:
    """
    Latest non-cancel maintenance with the same subject title or SD/TINC ticket.
    Used by cancel cards and the mail watcher.
    """
    nt = _normalize_title_key(display_subj)
    tkeys = _ticket_match_keys(ticket_id)

    def _usable(ent: dict[str, Any]) -> bool:
        ch = str(ent.get("content_hash") or "")
        if ch.startswith("skip:"):
            return False
        if ent.get("is_cancelled_email"):
            return False
        return True

    for ent in reversed(entries):
        if not _usable(ent):
            continue
        if _normalize_title_key(str(ent.get("title") or "")) == nt:
            return ent
    if tkeys:
        for ent in reversed(entries):
            if not _usable(ent):
                continue
            et = (ent.get("ticket_id") or "").strip().upper()
            if et in tkeys or bool(_ticket_match_keys(et) & tkeys):
                return ent
    return None


def lookup_prior_maintenance_for_cancel(
    email_subject: str | None,
    email_body: str | None = None,
) -> dict[str, Any] | None:
    """Find earlier maintenance in ``maintenance.json`` for this cancel subject/ticket."""
    subj = resolve_maintenance_subject(email_subject, email_body)
    if not subj:
        return None
    tid = extract_ticket_card_title(subj, email_body) or ""
    return find_prior_maintenance_entry(
        load_maintenance_state_entries(), subj, tid
    )


def build_cancelled_card_header_title(subject: str, extra_text: str | None = None) -> str:
    """
    ``❌ [SD-7050222] Equipment maintenance - Cancelled`` or
    ``❌ TINC-708832 - Cancelled``.
    """
    subj = resolve_maintenance_subject(subject, extra_text)
    if "[service desk]" in subj.lower():
        meta = parse_service_desk_subject_metadata(subj)
        sd = meta.get("ticket_sd") or extract_ticket_card_title(subj, extra_text) or "SD-?"
        maint = meta.get("maintenance_type") or "Maintenance"
        return _truncate_header(f"❌ [{sd}] {maint} - Cancelled")
    ticket = ticket_id_tinc_style(subj, extra_text) or "Maintenance"
    return _truncate_header(f"❌ {ticket} - Cancelled")


def _studio_date_for_cancel(
    info: dict[str, Any],
    *,
    email_subject: str | None,
    email_body: str | None,
) -> tuple[str, str]:
    studio, date = _studio_and_date(info, email_subject, email_body)
    subj = resolve_maintenance_subject(email_subject, email_body)
    if (date or "").strip() in ("", "Unknown"):
        sd_date = parse_service_desk_date_from_subject(subj)
        if sd_date:
            date = sd_date
    return studio, date


def _table_for_cancel(
    info: dict[str, Any],
    *,
    email_subject: str | None,
    email_body: str | None,
    table_game: str | None = None,
) -> str:
    tg = (table_game or "").strip()
    if tg and tg.lower() != "unknown":
        return tg
    return _table_display(
        info,
        launched_tables=None,
        email_subject=resolve_maintenance_subject(email_subject, email_body),
    )


def _cancel_fields_for_card(
    info: dict[str, Any],
    *,
    email_subject: str | None,
    email_body: str | None,
    table_game: str | None = None,
    prior: dict[str, Any] | None = None,
) -> tuple[str, str, str]:
    """
    Studio / Date / Table for cancel cards — fill from ``maintenance.json`` prior
    entry with the same ``[Service Desk] …`` title or SD/TINC ticket.
    """
    if prior is None:
        prior = lookup_prior_maintenance_for_cancel(email_subject, email_body)

    studio, date = _studio_date_for_cancel(
        info, email_subject=email_subject, email_body=email_body
    )
    table = _table_for_cancel(
        info,
        email_subject=email_subject,
        email_body=email_body,
        table_game=table_game,
    )

    if prior:
        pst = str(prior.get("studio") or "").strip()
        if pst and studio in ("", "Unknown"):
            studio = pst
        pdt = str(prior.get("maint_date") or "").strip()
        if pdt and date in ("", "Unknown"):
            date = pdt
        prior_title = str(prior.get("title") or "").strip()
        if prior_title:
            if date in ("", "Unknown"):
                sd_d = parse_service_desk_date_from_subject(prior_title)
                if sd_d:
                    date = sd_d
            tinc = parse_tinc_subject_metadata(prior_title)
            if studio in ("", "Unknown") and tinc.get("studio"):
                studio = tinc["studio"]
            if date in ("", "Unknown") and tinc.get("date"):
                date = tinc["date"]
        if table in ("", "Unknown"):
            launched = [
                str(x).strip()
                for x in (prior.get("launched_names") or [])
                if str(x).strip()
            ]
            tables = [
                str(x).strip()
                for x in (prior.get("table_names") or [])
                if str(x).strip()
            ]
            if launched:
                table = ", ".join(launched)
            elif tables:
                table = ", ".join(tables)

    if date in ("", "Unknown"):
        subj = resolve_maintenance_subject(email_subject, email_body)
        sd_d = parse_service_desk_date_from_subject(subj)
        if sd_d:
            date = sd_d

    return (
        (studio or "").strip() or "Unknown",
        (date or "").strip() or "Unknown",
        (table or "").strip() or "Unknown",
    )


def build_cancelled_card_elements(
    *,
    info: dict[str, Any] | None = None,
    email_subject: str | None = None,
    email_body: str | None = None,
    table_game: str | None = None,
    prior: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Cancelled card body — same field layout as In Progress, then cancellation notice.
    """
    inf = info if info is not None else extract_info(
        email_body or "", email_subject=email_subject
    )
    studio, date, table = _cancel_fields_for_card(
        inf,
        email_subject=email_subject,
        email_body=email_body,
        table_game=table_game,
        prior=prior,
    )
    notice = extract_cancel_notice_text(email_body)
    if prior and (prior.get("title") or "").strip():
        original = str(prior["title"]).strip()
    else:
        original = resolve_maintenance_subject(email_subject, email_body) or _email_ref_line(
            inf, email_subject, email_body
        )
    return [
        _card_studio_date_columns(studio, date),
        _card_labeled_field("Table", table),
        {
            "tag": "div",
            "text": {"tag": "lark_md", "content": notice},
        },
        {"tag": "hr"},
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"<font color='grey'>📧 Original: {original}</font>",
            },
        },
    ]


def build_cancelled_maintenance_card(
    *,
    email_subject: str,
    email_body: str | None = None,
    table_game: str | None = None,
    prior: dict[str, Any] | None = None,
    received_at: str | None = None,
) -> dict[str, Any]:
    """Full red interactive card for a cancellation notice."""
    info = extract_info(email_body or "", email_subject=email_subject)
    if prior is None:
        prior = lookup_prior_maintenance_for_cancel(email_subject, email_body)
    return build_maintenance_card(
        email_subject=email_subject,
        received_at=received_at,
        summary_section="",
        body_elements=build_cancelled_card_elements(
            info=info,
            email_subject=email_subject,
            email_body=email_body,
            table_game=table_game,
            prior=prior,
        ),
        email_body=email_body,
        show_meta=False,
        header_title=build_cancelled_card_header_title(email_subject, email_body),
        header_template="red",
    )


def build_cancelled_summary(
    *,
    table_game: str,
    ref_email: str,
    email_body: str | None = None,
) -> str:
    """Plain-text fallback for cancellation cards."""
    info = extract_info(email_body or "", email_subject=ref_email)
    prior = lookup_prior_maintenance_for_cancel(ref_email, email_body)
    studio, date, table = _cancel_fields_for_card(
        info,
        email_subject=ref_email,
        email_body=email_body,
        table_game=table_game,
        prior=prior,
    )
    notice = extract_cancel_notice_text(email_body)
    if prior and (prior.get("title") or "").strip():
        original = str(prior["title"]).strip()
    else:
        original = resolve_maintenance_subject(ref_email, email_body) or ref_email
    return "\n".join(
        [
            f"**Studio:**\n{studio}",
            f"**Date:**\n{date}",
            f"**Table:**\n{table}",
            "",
            notice,
            "",
            f"📧 Original: {original}",
        ]
    )


def _at_cs_team() -> str:
    return lark_card_at_open_id(_CS_TEAM_OPEN_ID)


def _at_qa_support() -> str:
    return lark_card_at_open_id(_QA_SUPPORT_OPEN_ID)


def find_tinc_reference_line(text: str) -> str | None:
    """Last ``TINC-… Live Dealer … / Studio / Table / Date`` line in body."""
    for line in reversed((text or "").replace("\r\n", "\n").splitlines()):
        ln = _clean_email_line(line)
        if re.match(
            r"^TINC-\d+\s+.+?/.+?/.+?/",
            ln,
            re.IGNORECASE,
        ):
            return ln.strip()
    return None


def find_service_desk_reference_line(text: str) -> str | None:
    """Last ``[Service Desk] … / (SD-…)`` line in body (``/m`` paste)."""
    for line in reversed((text or "").replace("\r\n", "\n").splitlines()):
        ln = _clean_email_line(line)
        if re.match(r"^\[Service Desk\]", ln, re.IGNORECASE):
            return ln.strip()
    return None


def resolve_maintenance_subject(
    email_subject: str | None, email_body: str | None = None
) -> str:
    """Prefer ``Subject:`` / TINC / ``[Service Desk]`` line in pasted body."""
    subj = normalize_display_subject(email_subject or "")
    if re.match(r"^(?:TINC-|\[Service Desk\])", subj, re.IGNORECASE):
        return subj
    ref = find_tinc_reference_line(email_body or "")
    if ref:
        return normalize_display_subject(ref)
    sd = find_service_desk_reference_line(email_body or "")
    if sd:
        return normalize_display_subject(sd)
    return subj


def _body_has_service_desk(text: str | None) -> bool:
    return bool(re.search(r"\[Service Desk\]", text or "", re.IGNORECASE))


def parse_tinc_subject_metadata(subject: str) -> dict[str, str]:
    """
    ``TINC-708832 Live Dealer Casino Information / Lithuania Studio /
    Double Ball Roulette / 24/May/26`` → studio, table, date, email_ref.
    """
    s = normalize_display_subject(subject)
    m = re.match(
        r"^(TINC-\d+)\s+.+?\s*/\s*(.+?)\s*/\s*(.+?)\s*/\s*(.+?)\s*$",
        s,
        re.IGNORECASE,
    )
    if not m:
        return {}
    return {
        "ticket": m.group(1).upper(),
        "studio": m.group(2).strip(),
        "table": m.group(3).strip(),
        "date": m.group(4).strip(),
        "email_ref": s,
    }


def parse_service_desk_subject_metadata(subject: str) -> dict[str, str]:
    """``[Service Desk] Studio cleaning maintenance / … / (SD-7044009)``."""
    s = normalize_display_subject(subject)
    out: dict[str, str] = {"email_ref": s}
    m = re.match(r"^\[Service Desk\]\s*(.+?)\s*/", s, re.IGNORECASE)
    if m:
        out["maintenance_type"] = m.group(1).strip()
    sd = extract_ticket_card_title(s)
    if sd:
        out["ticket_sd"] = sd
    return out


def classify_maintenance_card_kind(
    info: dict[str, Any],
    *,
    email_subject: str | None = None,
    email_body: str | None = None,
) -> str:
    """
    ``in_progress`` | ``fixed`` | ``scheduled`` for picture-style Lark cards.
    Cancelled emails are handled separately.
    """
    body = email_body or ""
    subj = resolve_maintenance_subject(email_subject, body)
    status = (info.get("status") or "").strip().lower()
    subj_low = subj.lower()
    is_sd = "[service desk]" in subj_low or _body_has_service_desk(body)

    if status == "fixed" or re.search(
        r"successfully\s+accomplished|maintenance\s+(?:is\s+)?fixed",
        body,
        re.IGNORECASE,
    ):
        return "fixed"
    if status in ("in progress", "in-progress", "inprogress"):
        return "in_progress"
    card_status = (extract_status_for_card(subj, body) or "").strip().lower()
    if card_status == "affected" or status == "affected":
        if is_sd:
            return "scheduled"
    if is_sd and re.search(
        r"going\s+to\s+take\s+place|downtime\s+from|will\s+be\s+unavailable",
        body,
        re.IGNORECASE,
    ):
        return "scheduled"
    if subj_low.startswith("tinc-"):
        return "in_progress"
    return "scheduled" if is_sd else "in_progress"


def _truncate_header(title: str) -> str:
    if len(title) <= _CARD_HEADER_TITLE_MAX:
        return title
    return title[: _CARD_HEADER_TITLE_MAX - 3] + "..."


def build_in_progress_card_header(subject: str, email_body: str | None = None) -> str:
    subj = resolve_maintenance_subject(subject, email_body)
    ticket = ticket_id_tinc_style(subj, email_body) or "Maintenance"
    return _truncate_header(f"⚠️ {ticket} - In Progress")


def build_fixed_card_header(subject: str, email_body: str | None = None) -> str:
    subj = resolve_maintenance_subject(subject, email_body)
    ticket = ticket_id_tinc_style(subj, email_body) or "Maintenance"
    return _truncate_header(f"✅ {ticket} - Fixed")


def build_scheduled_card_header(subject: str, email_body: str | None = None) -> str:
    subj = resolve_maintenance_subject(subject, email_body)
    meta = parse_service_desk_subject_metadata(subj)
    sd = meta.get("ticket_sd") or extract_ticket_card_title(subj, email_body) or "SD-?"
    maint = meta.get("maintenance_type") or "Maintenance"
    return _truncate_header(f"⚠️ [{sd}] {maint} - Scheduled")


def _table_display(
    info: dict[str, Any],
    *,
    launched_tables: list[str] | None,
    email_subject: str | None,
) -> str:
    if launched_tables:
        return ", ".join(launched_tables)
    names = [str(x).strip() for x in (info.get("table_names") or []) if str(x).strip()]
    if names:
        return ", ".join(names)
    tinc = parse_tinc_subject_metadata(email_subject or "")
    if tinc.get("table"):
        return tinc["table"]
    tbl = (info.get("table") or "").strip()
    return tbl if tbl and tbl.lower() != "unknown" else "Unknown"


def _studio_and_date(
    info: dict[str, Any],
    email_subject: str | None,
    email_body: str | None = None,
) -> tuple[str, str]:
    subj = resolve_maintenance_subject(email_subject, email_body)
    tinc = parse_tinc_subject_metadata(subj)
    if not tinc.get("studio"):
        ref = (info.get("reference") or "").strip()
        if ref and ref.lower() != "unknown":
            tinc = parse_tinc_subject_metadata(ref) or tinc

    studio = (tinc.get("studio") or "").strip() or "Unknown"
    if studio == "Unknown" and email_body:
        m = re.search(
            r"\btable\s+.+?\s+in\s+(.+?)\s+is\s+unavailable",
            email_body,
            re.IGNORECASE | re.DOTALL,
        )
        if m:
            studio = m.group(1).strip()
        else:
            m2 = re.search(r"\bin\s+([\w\s]+Studio)\b", email_body, re.IGNORECASE)
            if m2:
                studio = m2.group(1).strip()

    date = (tinc.get("date") or "").strip() or "Unknown"
    if date == "Unknown":
        md = (info.get("maint_date") or "").strip()
        if md:
            date = md
    return studio, date


def _email_ref_line(
    info: dict[str, Any],
    email_subject: str | None,
    email_body: str | None = None,
) -> str:
    ref = (info.get("reference") or "").strip()
    if ref and ref.lower() != "unknown":
        return ref
    resolved = resolve_maintenance_subject(email_subject, email_body)
    return resolved or "Unknown"


def _reason_display(info: dict[str, Any], email_body: str | None) -> str:
    reason = (info.get("reason") or "").strip()
    if reason and reason.lower() != "unknown":
        return reason
    for line in (email_body or "").replace("\r\n", "\n").splitlines():
        ln = _clean_email_line(line)
        m = re.match(
            r"^(?:Technical\s+)?Reason\s*:\s*(.+)$", ln, re.IGNORECASE
        )
        if m:
            return m.group(1).strip()
    return "Unknown"


_MONTH_MAP = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _parse_maint_utc_datetime(raw: str) -> datetime | None:
    s = (raw or "").strip()
    if not s or s.upper() == "TBA":
        return None
    m = re.search(
        r"(\d{1,2})/(\w{3})/(\d{2,4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)?\s*UTC",
        s,
        re.IGNORECASE,
    )
    if not m:
        return None
    day = int(m.group(1))
    mon = _MONTH_MAP.get(m.group(2).lower()[:3])
    if not mon:
        return None
    yr = int(m.group(3))
    if yr < 100:
        yr += 2000
    hour = int(m.group(4))
    minute = int(m.group(5))
    ampm = (m.group(6) or "").upper()
    if ampm == "PM" and hour < 12:
        hour += 12
    if ampm == "AM" and hour == 12:
        hour = 0
    try:
        return datetime(yr, mon, day, hour, minute, tzinfo=timezone.utc)
    except ValueError:
        return None


def _format_utc8_from_utc_str(utc_str: str) -> str:
    dt = _parse_maint_utc_datetime(utc_str)
    if not dt:
        return (utc_str or "").strip() or "Unknown"
    local = dt.astimezone(ZoneInfo("Asia/Shanghai"))
    return local.strftime("%d/%b/%y %H:%M UTC+8")


def format_maintenance_window_utc8(start: str, end: str) -> str:
    a = _format_utc8_from_utc_str(start)
    b = _format_utc8_from_utc_str(end)
    return f"{a} -> {b}"


def format_time_of_resolution(
    info: dict[str, Any], email_body: str | None = None
) -> str:
    body = email_body or ""
    m = re.search(
        r"Time of resolution:\s*(.+)$",
        body.replace("\r\n", "\n"),
        re.IGNORECASE | re.MULTILINE,
    )
    if m:
        return m.group(1).strip()
    start = (info.get("start_time") or "").strip()
    end = (info.get("end_time") or "").strip()
    if not start or start.lower() == "unknown":
        return "Unknown"
    if not end or end.lower() in ("unknown", "tba"):
        return f"From {start} till TBA"
    dt1 = _parse_maint_utc_datetime(start)
    dt2 = _parse_maint_utc_datetime(end)
    if dt1 and dt2 and dt2 > dt1:
        mins = int((dt2 - dt1).total_seconds() // 60)
        return f"From {start} till {end} ({mins} min in total)"
    return f"From {start} till {end}"


def _card_labeled_field(label: str, value: str) -> dict[str, Any]:
    """Bold label + value on next line (picture 1 field blocks)."""
    val = (value or "").strip() or "Unknown"
    return {
        "tag": "div",
        "text": {"tag": "lark_md", "content": f"**{label}:**\n{val}"},
    }


def _card_studio_date_columns(studio: str, date: str) -> dict[str, Any]:
    """Two-column Studio | Date row (picture 1)."""
    return {
        "tag": "column_set",
        "flex_mode": "bisect",
        "background_style": "default",
        "horizontal_spacing": "8px",
        "columns": [
            {
                "tag": "column",
                "width": "weighted",
                "weight": 1,
                "vertical_align": "top",
                "elements": [_card_labeled_field("Studio", studio)],
            },
            {
                "tag": "column",
                "width": "weighted",
                "weight": 1,
                "vertical_align": "top",
                "elements": [_card_labeled_field("Date", date)],
            },
        ],
    }


def build_in_progress_card_elements(
    info: dict[str, Any],
    *,
    email_subject: str | None = None,
    email_body: str | None = None,
    launched_tables: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Lark body elements matching picture 1 (bisect Studio/Date, hr, footer)."""
    studio, date = _studio_and_date(info, email_subject, email_body)
    table = _table_display(
        info,
        launched_tables=launched_tables,
        email_subject=resolve_maintenance_subject(email_subject, email_body),
    )
    reason = _reason_display(info, email_body)
    email_ref = _email_ref_line(info, email_subject, email_body)
    return [
        _card_studio_date_columns(studio, date),
        _card_labeled_field("Table", table),
        _card_labeled_field("Reason", reason),
        {"tag": "hr"},
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "⚠️ Maintenance in progress. Will notify when fixed.",
            },
        },
        {
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"📧 Email: {email_ref}"},
        },
    ]


def build_in_progress_card_body(
    info: dict[str, Any],
    *,
    email_subject: str | None = None,
    email_body: str | None = None,
    launched_tables: list[str] | None = None,
) -> str:
    """Plain-text fallback (tests / logging)."""
    studio, date = _studio_and_date(info, email_subject, email_body)
    table = _table_display(
        info,
        launched_tables=launched_tables,
        email_subject=resolve_maintenance_subject(email_subject, email_body),
    )
    reason = _reason_display(info, email_body)
    email_ref = _email_ref_line(info, email_subject, email_body)
    return "\n".join(
        [
            f"**Studio:**\n{studio}",
            f"**Date:**\n{date}",
            f"**Table:**\n{table}",
            f"**Reason:**\n{reason}",
            "",
            "⚠️ Maintenance in progress. Will notify when fixed.",
            f"📧 Email: {email_ref}",
        ]
    )


def _fixed_card_values(
    info: dict[str, Any],
    *,
    email_subject: str | None = None,
    email_body: str | None = None,
    launched_tables: list[str] | None = None,
) -> tuple[str, str, str, str, str, str]:
    subj = resolve_maintenance_subject(email_subject, email_body)
    studio, date = _studio_and_date(info, email_subject, email_body)
    table = _table_display(
        info,
        launched_tables=launched_tables,
        email_subject=subj,
    )
    reason = _reason_display(info, email_body)
    resolution = format_time_of_resolution(info, email_body)
    email_ref = _email_ref_line(info, email_subject, email_body)
    return studio, date, table, resolution, reason, email_ref


def build_fixed_card_elements(
    info: dict[str, Any],
    *,
    email_subject: str | None = None,
    email_body: str | None = None,
    launched_tables: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Lark body elements for Fixed (picture 2 — same field layout as In Progress)."""
    studio, date, table, resolution, reason, email_ref = _fixed_card_values(
        info,
        email_subject=email_subject,
        email_body=email_body,
        launched_tables=launched_tables,
    )
    return [
        {
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"Hi {_at_cs_team()}"},
        },
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "**Kindly unset maintenance.**",
            },
        },
        _card_studio_date_columns(studio, date),
        _card_labeled_field("Table", table),
        _card_labeled_field("Time of resolution", resolution),
        _card_labeled_field("Reason", reason),
        {"tag": "hr"},
        {
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"**CC:** {_at_qa_support()}"},
        },
        {
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"📧 Email: {email_ref}"},
        },
    ]


def build_fixed_card_body(
    info: dict[str, Any],
    *,
    email_subject: str | None = None,
    email_body: str | None = None,
    launched_tables: list[str] | None = None,
) -> str:
    """Plain-text fallback (tests / logging)."""
    studio, date, table, resolution, reason, email_ref = _fixed_card_values(
        info,
        email_subject=email_subject,
        email_body=email_body,
        launched_tables=launched_tables,
    )
    return "\n".join(
        [
            f"Hi {_at_cs_team()}",
            "",
            "**Kindly unset maintenance.**",
            f"**Studio:**\n{studio}",
            f"**Date:**\n{date}",
            f"**Table:**\n{table}",
            f"**Time of resolution:**\n{resolution}",
            f"**Reason:**\n{reason}",
            "",
            f"**CC:** {_at_qa_support()}",
            f"📧 Email: {email_ref}",
        ]
    )


def _scheduled_table_display(
    info: dict[str, Any],
    *,
    launched_tables: list[str] | None,
    email_subject: str | None,
    email_body: str | None = None,
) -> str:
    """All affected tables from email (scheduled notices often list several)."""
    names = [str(x).strip() for x in (info.get("table_names") or []) if str(x).strip()]
    if names:
        return ", ".join(names)
    return _table_display(
        info,
        launched_tables=launched_tables,
        email_subject=resolve_maintenance_subject(email_subject, email_body),
    )


def _scheduled_card_values(
    info: dict[str, Any],
    *,
    email_subject: str | None = None,
    email_body: str | None = None,
    launched_tables: list[str] | None = None,
) -> tuple[str, str, str, str, str, str]:
    subj = resolve_maintenance_subject(email_subject, email_body)
    sd_meta = parse_service_desk_subject_metadata(subj)
    maint_type = (
        sd_meta.get("maintenance_type")
        or _reason_display(info, email_body)
        or "Maintenance"
    )
    table = _scheduled_table_display(
        info,
        launched_tables=launched_tables,
        email_subject=email_subject,
        email_body=email_body,
    )
    window = format_maintenance_window_utc8(
        str(info.get("start_time") or ""),
        str(info.get("end_time") or ""),
    )
    avail = extract_status_for_card(subj, email_body) or "Affected"
    original = sd_meta.get("email_ref") or _email_ref_line(info, email_subject, email_body)
    return table, maint_type, window, avail, original, subj


def _scheduled_card_main_md(
    table: str,
    maint_type: str,
    window: str,
    avail: str,
) -> str:
    """Single ``lark_md`` block — section gaps match reference card (picture 3)."""
    return "\n".join(
        [
            f"Hi {_at_cs_team()}",
            "",
            f"🎰 Table: {table}",
            f"🔧 Type: {maint_type}",
            "",
            "🧭 Phase: Scheduled maintenance",
            "⚠️ Result: Downtime is scheduled and tables will be unavailable.",
            "",
            f"⏰ Window: {window}",
            f"📊 Table Availability: {avail}",
            "",
            "⚠️ ACTION REQUIRED: Set Maintenance",
            "⚠️ 请设置维护",
        ]
    )


def build_scheduled_card_elements(
    info: dict[str, Any],
    *,
    email_subject: str | None = None,
    email_body: str | None = None,
    launched_tables: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Lark body for Scheduled / Affected (picture 3): one body block, hr, grey Original."""
    table, maint_type, window, avail, original, _subj = _scheduled_card_values(
        info,
        email_subject=email_subject,
        email_body=email_body,
        launched_tables=launched_tables,
    )
    main_md = _scheduled_card_main_md(table, maint_type, window, avail)
    footer_md = f"<font color='grey'>📧 Original: {original}</font>"
    return [
        {
            "tag": "div",
            "text": {"tag": "lark_md", "content": main_md},
        },
        {"tag": "hr"},
        {
            "tag": "div",
            "text": {"tag": "lark_md", "content": footer_md},
        },
    ]


def build_scheduled_card_body(
    info: dict[str, Any],
    *,
    email_subject: str | None = None,
    email_body: str | None = None,
    launched_tables: list[str] | None = None,
) -> str:
    """Plain-text fallback (tests / logging)."""
    table, maint_type, window, avail, original, _subj = _scheduled_card_values(
        info,
        email_subject=email_subject,
        email_body=email_body,
        launched_tables=launched_tables,
    )
    parts = [
        _scheduled_card_main_md(table, maint_type, window, avail),
        f"📧 Original: {original}",
    ]
    return "\n\n---\n\n".join(parts)


def build_maintenance_notice(
    email_text: str,
    *,
    email_subject: str | None = None,
    launched_tables: list[str] | None = None,
) -> tuple[str, str, str, list[dict[str, Any]] | None]:
    """
    Picture-style maintenance card:
    ``(header_title, header_template, body_md, body_elements)``.

    ``body_elements`` is set for **In Progress**, **Fixed**, and **Scheduled**.
    """
    info = extract_info(email_text, email_subject=email_subject)
    kind = classify_maintenance_card_kind(
        info, email_subject=email_subject, email_body=email_text
    )
    kw = {
        "email_subject": email_subject,
        "email_body": email_text,
        "launched_tables": launched_tables,
    }
    if kind == "fixed":
        return (
            build_fixed_card_header(email_subject or "", email_text),
            "green",
            "",
            build_fixed_card_elements(info, **kw),
        )
    if kind == "scheduled":
        return (
            build_scheduled_card_header(email_subject or "", email_text),
            "red",
            "",
            build_scheduled_card_elements(info, **kw),
        )
    return (
        build_in_progress_card_header(email_subject or "", email_text),
        "orange",
        "",
        build_in_progress_card_elements(info, **kw),
    )


def format_received_at(when: str | None) -> str:
    if not (when or "").strip():
        return ""
    raw = when.strip()
    tz_name = (
        os.getenv("MAINTENANCE_MAIL_TZ", "").strip()
        or os.getenv("maintenance_mail_tz", "").strip()
        or "Asia/Shanghai"
    )
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        try:
            dt = dt.astimezone(ZoneInfo(tz_name))
        except Exception:
            pass
        return dt.strftime("%d/%b/%y %H:%M")
    except ValueError:
        return raw


def lark_card_at_open_id(open_id: str) -> str:
    """@mention inside interactive card ``lark_md`` — use ``<at id=ou_…></at>`` not ``user_id``."""
    oid = (open_id or "").strip()
    return f"<at id={oid}></at>" if oid else ""


def lark_md_for_card(text: str) -> str:
    """Convert plain-message ``<at user_id=…>`` tags to card-compatible ``<at id=…></at>``."""
    return re.sub(
        r'<at\s+user_id="([^"]+)"[^>]*>[^<]*</at>',
        lambda m: lark_card_at_open_id(m.group(1)),
        text or "",
    )


def _cell_norm(c: Any) -> str:
    if c is None:
        return ""
    return str(c).replace("\r", " ").replace("\n", " ").strip().lower()


def _game_name_key(s: Any) -> str:
    """NFKC + strip accents + remove spaces for stable exact match."""
    t = unicodedata.normalize("NFKC", str(s or ""))
    t = t.lower().replace(" ", "")
    return "".join(
        c
        for c in unicodedata.normalize("NFD", t)
        if unicodedata.category(c) != "Mn"
    )


def _names_match_gamelist(a: Any, b: Any) -> bool:
    """Exact game name match — no substring (avoids «Blackjack» → «Blackjack B»)."""
    na = _game_name_key(a)
    nb = _game_name_key(b)
    if not na or not nb:
        return False
    return na == nb


def _entrance_header_score(cell_norm: str) -> int:
    """Prefer true **遊戲入口圖 / Game entrance map** column over other columns."""
    cn = cell_norm
    if "遊戲入口圖" in cn or "游戏入口图" in cn:
        return 10
    if "entrance" in cn and "map" in cn:
        return 5
    return 0


def _is_entrance_map_launched(cell: Any) -> bool:
    """
    ``遊戲入口圖 / Game entrance map``:
    ``1`` → launched; ``0`` / empty / any other value → not launched.
    """
    if cell is None:
        return False
    if isinstance(cell, bool):
        return cell is True
    if isinstance(cell, (int, float)) and not isinstance(cell, bool):
        return float(cell) == 1.0
    raw = str(cell).strip()
    if not raw:
        return False
    try:
        n = float(raw.replace(",", ""))
        if n == 1.0:
            return True
        if n == 0.0:
            return False
        return False
    except ValueError:
        if raw == "1":
            return True
        if raw == "0":
            return False
        return False


def _fetch_sheet_values(
    tenant_token: str, spreadsheet_token: str, sheet_id: str, *, max_row: int = 2500
) -> list[list[Any]]:
    rng = f"{sheet_id}!A1:ZZ{max_row}"
    enc = quote(rng, safe="")
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{enc}"
    headers = {"Authorization": f"Bearer {tenant_token}"}
    # UnformattedValue → raw 0/1 in 遊戲入口圖 (ToString may mis-read coloured cells).
    last_err: str | None = None
    for render_opt in ("UnformattedValue", "ToString"):
        params = {
            "valueRenderOption": render_opt,
            "dateTimeRenderOption": "FormattedString",
        }
        resp = requests.get(url, headers=headers, params=params, timeout=90)
        data = resp.json()
        if data.get("code") == 0:
            vr = (
                data.get("data", {}).get("valueRange")
                or data.get("data", {}).get("value_range")
                or {}
            )
            return vr.get("values") or []
        last_err = str(data.get("msg", data))
    raise RuntimeError(last_err or "gamelist fetch failed")


def _find_header_row_and_cols(grid: list[list[Any]]) -> tuple[int, int, int] | None:
    """Pick header row with **游戏名称** + **遊戲入口圖 / Game entrance map** columns."""
    scored: list[tuple[int, int, int, int]] = []
    for ri, row in enumerate(grid[:60]):
        if not row:
            continue
        name_ci: int | None = None
        entrance_ci: int | None = None
        entrance_hdr_score = 0
        for ci, cell in enumerate(row):
            cn = _cell_norm(cell)
            if "游戏名称" in cn or ("games" in cn and "name" in cn):
                name_ci = ci
            esc = _entrance_header_score(cn)
            if esc > entrance_hdr_score:
                entrance_hdr_score = esc
                entrance_ci = ci
        if name_ci is None or entrance_ci is None or entrance_hdr_score == 0:
            continue
        nc = _cell_norm(row[name_ci]) if name_ci < len(row) else ""
        score = 0
        if "游戏名称" in nc:
            score += 4
        elif "games" in nc and "name" in nc:
            score += 2
        score += entrance_hdr_score
        scored.append((score, ri, name_ci, entrance_ci))
    if not scored:
        return None
    scored.sort(key=lambda t: (-t[0], -t[1]))
    _, ri, name_ci, entrance_ci = scored[0]
    return ri, name_ci, entrance_ci


def _row_launched_for_game(
    grid: list[list[Any]], game_name: str, sheet_title: str = ""
) -> bool | None:
    parsed = _find_header_row_and_cols(grid)
    if not parsed:
        return None
    hi, ci_name, ci_entrance = parsed
    for row in grid[hi + 1 :]:
        if not row:
            continue
        name_cell = row[ci_name] if len(row) > ci_name else ""
        entrance_cell = row[ci_entrance] if len(row) > ci_entrance else ""
        match_name = _names_match_gamelist(name_cell, game_name)
        match_tab = bool(sheet_title.strip()) and _names_match_gamelist(
            name_cell, sheet_title
        )
        if match_name or match_tab:
            return _is_entrance_map_launched(entrance_cell)

    data_rows = [r for r in grid[hi + 1 :] if r]
    if (
        len(data_rows) == 1
        and sheet_title.strip()
        and _names_match_gamelist(sheet_title, game_name)
    ):
        r = data_rows[0]
        ent = r[ci_entrance] if len(r) > ci_entrance else ""
        return _is_entrance_map_launched(ent)
    return None


def _clean_email_line(line: str) -> str:
    """Strip HTML/plain-text quote markers (``> ``) from a line."""
    return re.sub(r"^>\s*", "", (line or "").strip())


def _is_plausible_game_name(name: str) -> bool:
    """Reject summary lines / URLs mistaken as table names."""
    t = (name or "").strip()
    if not t or len(t) > 80:
        return False
    low = t.lower()
    if re.search(r"https?://|@|\.com\b|evolution\b", low):
        return False
    if ":" in t and not re.search(r"privé|priv", t, re.I):
        return False
    junk = (
        "maintenance",
        "availability",
        "affected",
        "regards",
        "inform",
        "accomplished",
        "apologize",
        "inconvenience",
        "summary",
        "casino team",
        "service desk",
        "following tables",
        "unavailable",
        "downtime",
        "utc",
        "you may find",
        "best regards",
        "dear casino",
        "once maintenance",
    )
    if any(j in low for j in junk):
        return False
    if len(t.split()) > 8:
        return False
    return True


def _table_block_stop_line(line: str) -> bool:
    """True if this line ends the “list of tables” block (not a table name row)."""
    if not line or not line.strip():
        return False
    ln = _clean_email_line(line)
    if re.match(
        r"^(?:You may find summary|Start time:|End time:|Reason:|Table availability:|\[Service Desk\])",
        ln,
        re.I,
    ):
        return True
    if re.match(r"^(?:TINC-|SD-\d)", ln, re.I):
        return True
    if re.match(
        r"^(?:This is to inform|During which|Please |Kindly |Note:|Once maintenance|"
        r"We apologize|Best regards|Dear Casino|http)",
        ln,
        re.I,
    ):
        return True
    return False


def _parse_table_block_after_heading(
    lines: list[str], heading_i: int
) -> tuple[list[str], int]:
    """
    Collect consecutive table-name lines after ``Affected table`` /
    ``following tables will be unavailable`` headings.

    Skips blank lines after the heading, then reads non-empty lines until a
    blank line or a known section header.
    """
    j = heading_i + 1
    n = len(lines)
    while j < n and not lines[j].strip():
        j += 1
    names: list[str] = []
    while j < n:
        chunk = _clean_email_line(lines[j])
        if not chunk:
            break
        if _table_block_stop_line(chunk):
            break
        if _is_plausible_game_name(chunk):
            names.append(chunk)
        j += 1
    return names, j


def extract_info(text: str, *, email_subject: str | None = None):
    """Parse email text line by line to extract fields."""
    info = {
        'table': 'Unknown',
        'table_names': [],
        'reason': 'Unknown',
        'status': 'Unknown',
        'start_time': 'Unknown',
        'end_time': 'Unknown',
        'reference': 'Unknown',
    }

    lines = [_clean_email_line(line) for line in text.splitlines()]
    table_availability_value: str | None = None

    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        if not line:
            i += 1
            continue

        # ---- Table availability → Status (before ``table …`` detection) ----
        elif re.search(r'^Table availability\s*:', line, re.IGNORECASE):
            match = re.search(r'^Table availability\s*:\s*(.*)$', line, re.IGNORECASE)
            if match:
                val = match.group(1).strip()
                if val:
                    table_availability_value = val

        # ---- Table detection (not ``Table availability:``) ----
        elif re.search(r'^table\s+(?!availability\b)', line, re.IGNORECASE):
            match = re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE)
            if match:
                info['table'] = match.group(1).strip()
                info['table_names'] = [info['table']]
            else:
                match = re.search(r'table\s+(.*?)\s+was', line, re.IGNORECASE)
                if match:
                    info['table'] = match.group(1).strip()
                    info['table_names'] = [info['table']]
        elif re.search(r'^Affected tables?\s*:', line, re.IGNORECASE):
            block_names, j = _parse_table_block_after_heading(lines, i)
            if block_names:
                info["table"] = ", ".join(block_names)
                info["table_names"] = list(block_names)
            i = j
            continue
        elif re.search(r'following tables will be unavailable:', line, re.IGNORECASE):
            block_names, j = _parse_table_block_after_heading(lines, i)
            if block_names:
                info["table"] = ", ".join(block_names)
                info["table_names"] = list(block_names)
            i = j
            continue

        # ---- Reason / Technical Reason ----
        elif re.search(r'^(?:Technical\s+)?Reason\s*:', line, re.IGNORECASE):
            match = re.search(
                r'^(?:Technical\s+)?Reason\s*:\s*(.*)$', line, re.IGNORECASE
            )
            if match:
                info['reason'] = match.group(1).strip()

        # ---- Date (TINC in-progress notices) ----
        elif re.search(r'^Date\s*:', line, re.IGNORECASE):
            match = re.search(r'^Date\s*:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['maint_date'] = match.group(1).strip()

        # ---- Status (explicit) ----
        elif re.search(r'^Status:', line, re.IGNORECASE):
            match = re.search(r'^Status:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['status'] = match.group(1).strip()

        # ---- Start time ----
        elif re.search(r'^Start\s*time\s*:', line, re.IGNORECASE):
            match = re.search(r'^Start\s*time\s*:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['start_time'] = match.group(1).strip()

        # ---- End time ----
        elif re.search(r'^End\s*time\s*:', line, re.IGNORECASE):
            match = re.search(r'^End\s*time\s*:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['end_time'] = match.group(1).strip()

        # ---- Time of resolution (old format) ----
        elif re.search(r'^Time of resolution:', line, re.IGNORECASE):
            match = re.search(r'from\s+(.*?)\s+till\s+(.*?)(?:\s*\(|$)', line, re.IGNORECASE)
            if match:
                info['start_time'] = match.group(1).strip()
                info['end_time'] = match.group(2).strip()
            else:
                # Check for "We will inform you as soon..." pattern
                if re.search(r'We will inform you as soon', line, re.IGNORECASE):
                    info['end_time'] = "TBA"

        # ---- Reference lines (only if they match expected patterns) ----
        elif re.search(r'^(TINC-\d+|SD-\d+|\[Service Desk\])', line, re.IGNORECASE):
            info['reference'] = line.strip()

        i += 1

    # --- Fallbacks ---
    # If start time still unknown, try to find it in the first paragraph
    if info['start_time'] == 'Unknown':
        # Look for "from ... UTC" pattern
        from_match = re.search(r'from\s+(.*?)\s+UTC', text, re.IGNORECASE)
        if from_match:
            info['start_time'] = from_match.group(1).strip() + " UTC"
    if info['end_time'] == 'Unknown':
        till_match = re.search(
            r'(?:till|to|until)\s+(.*?)\s+UTC', text, re.IGNORECASE
        )
        if till_match:
            info['end_time'] = till_match.group(1).strip() + " UTC"
    # If end time still unknown and "We will inform you" appears, set TBA
    if info['end_time'] == 'Unknown' and re.search(r'We will inform you as soon', text, re.IGNORECASE):
        info['end_time'] = "TBA"
    _apply_service_desk_utc_times(info, text, email_subject=email_subject)
    # If table name still unknown, try to extract from "table X in Y" in the first paragraph
    if info['table'] == 'Unknown':
        table_match = re.search(r'table\s+([^\.]+?)\s+in', text, re.IGNORECASE)
        if table_match:
            info['table'] = table_match.group(1).strip()
            info['table_names'] = [info['table']]
    elif info['table'] != 'Unknown' and not info['table_names']:
        info['table_names'] = [
            x.strip() for x in info['table'].split(',') if x.strip()
        ]
    # Do NOT fallback to first line for reference; leave as "Unknown" if not found

    # Status: explicit ``Status:`` line wins; else ``Table availability: …`` → Status.
    if info['status'] == 'Unknown' and table_availability_value:
        info['status'] = table_availability_value
    if info['status'] == 'Unknown' and re.search(
        r'successfully accomplished', text, re.IGNORECASE
    ):
        info['status'] = 'Fixed'

    if info["reference"] == "Unknown":
        resolved = resolve_maintenance_subject(email_subject, text)
        if resolved:
            info["reference"] = resolved

    return info

def format_status_display(status: str) -> str:
    """Append maintenance action hint for Fixed / In progress statuses."""
    raw = (status or "").strip() or "Unknown"
    low = raw.lower()
    if low == "fixed":
        return "Fixed (Kindly Unset Maintenance)"
    if low in ("in progress", "in-progress", "inprogress"):
        return "In progress (Kindly Set Maintenance)"
    return raw


def generate_output(
    info: dict[str, Any],
    *,
    affected_tables: list[str] | None = None,
) -> str:
    """Format the extracted info into the desired output with user mentions."""
    # Use the provided open IDs for the two roles
    qa_os_local_id = "ou_0342007237c6c1aa262acae839acb7c6"
    cs_team_id = "ou_c927a378e9b464741c67b61c1641577b"

    names = (
        list(affected_tables)
        if affected_tables is not None
        else list(info.get('table_names') or [])
    )
    affected_lines = ["Affected table :"]
    if names:
        affected_lines.extend(names)
    else:
        affected_lines.append("Unknown")

    reason = (info.get("reason") or "").strip()
    show_reason = bool(reason) and reason.lower() != "unknown"

    output = [
        "Hi "
        f'{lark_card_at_open_id(qa_os_local_id)} '
        f'{lark_card_at_open_id(cs_team_id)} , kindly check this email. Thank you.',
        "",
        *affected_lines,
    ]
    if show_reason:
        output.append(f"Reason : {reason}")
    output.extend(
        [
            f"Status: {format_status_display(info['status'])}",
            f"Start time: {info['start_time']}",
            f"End time : {info['end_time']}",
            "",
            f"REF EMAIL:{info['reference']}",
        ]
    )
    return "\n".join(output)


def build_maintenance_card(
    *,
    email_subject: str,
    received_at: str | None = None,
    from_addr: str | None = None,
    gamelist_section: str = "",
    summary_section: str = "",
    body_elements: list[dict[str, Any]] | None = None,
    header_template: str = "orange",
    email_body: str | None = None,
    show_meta: bool = True,
    header_title: str | None = None,
) -> dict[str, Any]:
    """Lark interactive card: header = ticket id + Status; optional meta / gamelist / body."""
    display_subj = normalize_display_subject(email_subject) or "Maintenance email"
    rcv = format_received_at(received_at)
    if not (header_title or "").strip():
        header_title = build_card_header_title(
            email_subject,
            email_body=email_body,
            received_at=received_at,
        )

    elements: list[dict[str, Any]] = []
    if show_meta:
        meta_lines: list[str] = []
        meta_lines.append(f"**Subject:** {display_subj}")
        if from_addr:
            meta_lines.append(f"**From:** {from_addr}")
        if rcv:
            meta_lines.append(f"**Received:** {rcv}")
        if meta_lines:
            elements.append(
                {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(meta_lines)}}
            )
            elements.append({"tag": "hr"})

    if (gamelist_section or "").strip():
        elements.append(
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": lark_md_for_card(gamelist_section.strip()),
                },
            }
        )
        elements.append({"tag": "hr"})

    if body_elements:
        elements.extend(body_elements)
    elif (summary_section or "").strip():
        elements.append(
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": lark_md_for_card(summary_section.strip()),
                },
            }
        )

    while elements and elements[-1].get("tag") == "hr":
        elements.pop()
    if not elements:
        elements.append(
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": "_No content._"},
            }
        )

    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": header_template,
            "title": {"tag": "plain_text", "content": header_title},
        },
        "body": {"elements": elements},
    }


def get_table_name(text):
    """Extract just the affected table name for the first tag message."""
    lines = [line.strip() for line in text.splitlines()]
    for i, line in enumerate(lines):
        if re.search(r'^table\s+(?!availability\b)', line, re.IGNORECASE):
            match = re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE)
            if match:
                return match.group(1).strip()
            match = re.search(r'table\s+(.*?)\s+was', line, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        elif re.search(r'^Affected tables?\s*:', line, re.IGNORECASE):
            block_names, _ = _parse_table_block_after_heading(lines, i)
            if block_names:
                return block_names[0]
        elif re.search(r'following tables will be unavailable:', line, re.IGNORECASE):
            block_names, _ = _parse_table_block_after_heading(lines, i)
            if block_names:
                return block_names[0]
        elif re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE):
            match = re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE)
            if match:
                return match.group(1).strip()
    # Fallback: if none of the above, try to extract from the first line containing "table"
    first_table = re.search(r'table\s+([^\.]+?)\s+in', text, re.IGNORECASE)
    if first_table:
        return first_table.group(1).strip()
    return "Unknown"


def extract_candidate_game_names(text: str) -> list[str]:
    """Table names from structured sections only (not random ``table … in`` in HTML)."""
    info = extract_info(text)
    seen: set[str] = set()
    out: list[str] = []

    def add(raw: str) -> None:
        t = (raw or "").strip()
        if not _is_plausible_game_name(t):
            return
        key = _cell_norm(t).replace(" ", "")
        if not key or key in seen:
            return
        seen.add(key)
        out.append(t)

    for nm in info.get("table_names") or []:
        add(nm)
    if not out and info.get("table") not in (None, "", "Unknown"):
        for part in re.split(r"[,;，、]", str(info["table"])):
            add(part.strip())

    return out


def process_email(
    text: str,
    *,
    affected_launched_only: list[str] | None = None,
    email_subject: str | None = None,
) -> str:
    """
    Format QA/CS summary. If ``affected_launched_only`` is set (e.g. from ``/m``
    pipeline), **Affected table** lists only those names, one per line.
    """
    info = extract_info(text, email_subject=email_subject)
    return generate_output(
        info,
        affected_tables=affected_launched_only,
    )


def process_maintenance_pipeline(
    email_text: str,
    tenant_access_token: str | None,
    *,
    email_subject: str | None = None,
    received_at: str | None = None,
) -> tuple[str, str, str, str, list[dict[str, Any]] | None]:
    """
    Reply for bot / mail watcher:
    1) Launched vs not launched (gamelist section; often omitted on cards).
    2–5) Picture-style Lark card — always built when gamelist is checked (even if
    no game is 「上线」; ``/m`` preview still shows Scheduled / In Progress / Fixed).

    Mail watcher only *forwards* when ≥1 launched; card preview is independent.

    If gamelist env is missing, returns ("", *build_maintenance_notice(...)).
    """
    tok = (tenant_access_token or "").strip()
    ss = GAMELIST_SPREADSHEET_TOKEN
    sid = GAMELIST_SHEET_ID
    subj_kw = {"email_subject": email_subject}

    if not ss or not sid or not tok:
        h, t, b, el = build_maintenance_notice(email_text, email_subject=email_subject)
        return "", h, t, b, el

    try:
        grid = _fetch_sheet_values(tok, ss, sid)
    except Exception as e:
        h, t, b, el = build_maintenance_notice(
            email_text, email_subject=email_subject
        )
        return (
            f"⚠️ **Gamelist 表格**读取失败（仍发送下方原始摘要）: `{e}`",
            h,
            t,
            b,
            el,
        )

    candidates = extract_candidate_game_names(email_text)
    if not candidates:
        h, t, b, el = build_maintenance_notice(
            email_text, email_subject=email_subject
        )
        return (
            "⚠️ 未能从邮件中识别游戏/表名（仍发送下方原始摘要）。",
            h,
            t,
            b,
            el,
        )

    launched_list: list[str] = []
    not_launched_list: list[str] = []

    for g in candidates:
        verdict = _row_launched_for_game(grid, g, "")
        if verdict is True:
            launched_list.append(g)
        else:
            # False (遊戲入口圖=0) and None (not found on gamelist) → not launched
            not_launched_list.append(g)

    lines1 = ["📋 **游戏上线状态（gamelist · 遊戲入口圖: 1=上线, 0=非上线）**"]
    lines1.append(
        "✅ **上线 Launched：** "
        + (", ".join(launched_list) if launched_list else "（无）")
    )
    lines1.append(
        "⛔ **非上线 Not launched：** "
        + (", ".join(not_launched_list) if not_launched_list else "（无）")
    )

    msg1 = "\n".join(lines1)

    hdr_title, hdr_tpl, msg2, card_el = build_maintenance_notice(
        email_text,
        email_subject=email_subject,
        launched_tables=launched_list or None,
    )

    return msg1, hdr_title, hdr_tpl, msg2, card_el


def parse_subject_from_pasted_email(text: str) -> str | None:
    """
    Subject for ``/m`` pasted email: ``Subject:`` header, else trailing
    ``TINC-…`` or ``[Service Desk] …`` line in the body.
    """
    for line in text.splitlines()[:8]:
        m = re.match(r"^Subject:\s*(.+)$", line.strip(), re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return find_tinc_reference_line(text) or find_service_desk_reference_line(text)


def main():
    """Command‑line interface."""
    if len(sys.argv) > 1:
        text = " ".join(sys.argv[1:])
        if '\n' not in text:
            print("⚠️ Hint: For multiline input, please enclose the email in quotes or use a pipe (python3 maintenance.py < email.txt).", file=sys.stderr)
    else:
        text = sys.stdin.read()
    if not text.strip():
        print("No input provided.", file=sys.stderr)
        sys.exit(1)
    print(process_email(text))

if __name__ == "__main__":
    main()