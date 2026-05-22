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


FORWARD_DONE_BODY = (
    "Done forward to evolive.maintenance@om.hotelstotsenberg.com"
)
NOT_IN_CP_WEBSITE_BODY = "NOT IN CP WEBSITE"
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
        elif re.search(r'^Affected table/-s:', line, re.IGNORECASE):
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

        # ---- Reason ----
        elif re.search(r'^Reason:', line, re.IGNORECASE):
            match = re.search(r'^Reason:\s*(.*)$', line, re.IGNORECASE)
            if match:
                reason = match.group(1).strip()
                reason = re.sub(r'\s*\([^)]*\)', '', reason)
                info['reason'] = reason

        # ---- Status (explicit) ----
        elif re.search(r'^Status:', line, re.IGNORECASE):
            match = re.search(r'^Status:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['status'] = match.group(1).strip()

        # ---- Start time ----
        elif re.search(r'^Start time:', line, re.IGNORECASE):
            match = re.search(r'^Start time:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['start_time'] = match.group(1).strip()

        # ---- End time ----
        elif re.search(r'^End time:', line, re.IGNORECASE):
            match = re.search(r'^End time:\s*(.*)$', line, re.IGNORECASE)
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
    # If end time still unknown and "We will inform you" appears, set TBA
    if info['end_time'] == 'Unknown' and re.search(r'We will inform you as soon', text, re.IGNORECASE):
        info['end_time'] = "TBA"
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

    if info["reference"] == "Unknown" and (email_subject or "").strip():
        info["reference"] = normalize_display_subject(email_subject)

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
    header_template: str = "orange",
    email_body: str | None = None,
) -> dict[str, Any]:
    """Lark interactive card: header = ticket id + Status; subject/time in body."""
    display_subj = normalize_display_subject(email_subject) or "Maintenance email"
    rcv = format_received_at(received_at)
    header_title = build_card_header_title(
        email_subject,
        email_body=email_body,
        received_at=received_at,
    )

    elements: list[dict[str, Any]] = []
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

    if (summary_section or "").strip():
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
        elif re.search(r'^Affected table/-s:', line, re.IGNORECASE):
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
) -> tuple[str, str]:
    """
    Two-part reply for bot:
    1) Launched vs not launched (from gamelist spreadsheet; unmatched → not launched).
    2) Full QA/CS summary only if at least one candidate is 「上线 Launched」.

    If ``gamelist`` / ``gamelistsheetid`` or tenant token is missing, returns ("", process_email(...)).
    """
    tok = (tenant_access_token or "").strip()
    ss = GAMELIST_SPREADSHEET_TOKEN
    sid = GAMELIST_SHEET_ID
    subj_kw = {"email_subject": email_subject}

    if not ss or not sid or not tok:
        return "", process_email(email_text, **subj_kw)

    try:
        grid = _fetch_sheet_values(tok, ss, sid)
    except Exception as e:
        return (
            f"⚠️ **Gamelist 表格**读取失败（仍发送下方原始摘要）: `{e}`",
            process_email(email_text, **subj_kw),
        )

    candidates = extract_candidate_game_names(email_text)
    if not candidates:
        return (
            "⚠️ 未能从邮件中识别游戏/表名（仍发送下方原始摘要）。",
            process_email(email_text, **subj_kw),
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

    if not launched_list:
        return msg1, ""

    msg2 = process_email(
        email_text,
        affected_launched_only=launched_list,
        **subj_kw,
    )

    return msg1, msg2


def parse_subject_from_pasted_email(text: str) -> str | None:
    """If pasted text starts with ``Subject:``, return that line's value."""
    for line in text.splitlines()[:8]:
        m = re.match(r"^Subject:\s*(.+)$", line.strip(), re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


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