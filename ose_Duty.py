#!/usr/bin/env python3
"""
OSE Duty + Leave + Offset

- Duty + leave sheet: ``AS33r7`` (``D``/``N``/``*`` offset, ``AL``/``SL``/ leave codes)
- leaveose (OSE HRMS display): ``OSE_HRMS_LEAVE_TABLE_ID`` → tblvoXE0hsPjgb0j
- leave 全员 (company HRMS): ``OSE_ALL_LEAVE_TABLE_ID`` → tblmHJHe12BCJRD8 (``--sync-all-leave-month``)
- OSE submit / approve: ``OSE_LEAVE_TABLE_ID`` (default same as leave 全员 table)
- Offset source: Lark Bitable (Approved + Original/Exchange date)
"""

from __future__ import annotations

import calendar
import json
import os
import re
import sys
import threading
import time
from datetime import date, datetime, timedelta
from typing import Any, Optional

import requests
from dotenv import load_dotenv

import duty_list_match as dlm

load_dotenv()

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")

SPREADSHEET_TOKEN = (os.getenv("OSE_SPREADSHEET_TOKEN") or "UjF0saOVuhJSWLtBv9GlaQOkgbe").strip()
# Single OSE wiki sheet (replaces legacy ``3RIBRL`` / ``65p5cn``).
_LEGACY_OSE_SHEET_IDS = frozenset({"3RIBRL", "65p5cn"})


def _resolve_ose_duty_sheet_id() -> str:
    """Duty now lives in the merged 'FINAL OSE & QA MERGE' tab (``AS33r7``).

    A stale/legacy ``OSE_SHEET_ID`` (``3RIBRL`` / ``65p5cn``) is mapped to
    ``AS33r7`` so duty never falls back to the retired per-year tab.
    """
    sid = (os.getenv("OSE_SHEET_ID") or "").strip().replace(" ", "")
    if not sid or sid in _LEGACY_OSE_SHEET_IDS:
        return "AS33r7"
    return sid


SHEET_ID = _resolve_ose_duty_sheet_id()


def _resolve_ose_leave_sheet_id() -> str:
    """
    Leave AL/SL writes target the same wiki tab as duty unless a separate workbook is configured.
    Ignore legacy tab ids (``65p5cn``, ``3RIBRL``) when duty already points at ``AS33r7``.
    """
    duty_sid = (os.getenv("OSE_SHEET_ID") or "AS33r7").strip().replace(" ", "")
    leave_sid = (os.getenv("OSE_LEAVE_SHEET_ID") or duty_sid).strip().replace(" ", "")
    leave_ss = (
        (os.getenv("OSE_LEAVE_SPREADSHEET_TOKEN") or "").strip()
        or SPREADSHEET_TOKEN
    )
    if leave_sid in _LEGACY_OSE_SHEET_IDS and duty_sid not in _LEGACY_OSE_SHEET_IDS:
        print(
            f"[ose_Duty] OSE_LEAVE_SHEET_ID={leave_sid!r} is legacy; using duty sheet {duty_sid!r} for AL/SL",
            flush=True,
        )
        return duty_sid
    if leave_ss == SPREADSHEET_TOKEN and leave_sid != duty_sid:
        print(
            f"[ose_Duty] same spreadsheet for duty/leave — using {duty_sid!r} (ignore OSE_LEAVE_SHEET_ID={leave_sid!r})",
            flush=True,
        )
        return duty_sid
    return leave_sid


LEAVE_SHEET_ID = _resolve_ose_leave_sheet_id()

# Leave / Offset Bitable (defaults from user-provided URLs).
OSE_BASE_TOKEN = os.getenv("OSE_BASE_TOKEN", "CpdEbEofwaYyyEsSjlElKNxzgec")
# HRMS → OSE display sheet (webapp / admin ALL / duty calendar leave list).
# https://casinoplus.sg.larksuite.com/base/CpdEbEofwaYyyEsSjlElKNxzgec?table=tblvoXE0hsPjgb0j
# Do not fall back to TRACK_LEAVE_TABLE_ID — legacy env may still point at tblmHJHe12BCJRD8.
OSE_HRMS_LEAVE_TABLE_ID = os.getenv("OSE_HRMS_LEAVE_TABLE_ID", "tblvoXE0hsPjgb0j").strip()
# leave 全员 — company-wide HRMS leave (sync via leavewfh ``--sync-all-leave-month``).
# https://casinoplus.sg.larksuite.com/base/CpdEbEofwaYyyEsSjlElKNxzgec?table=tblmHJHe12BCJRD8
OSE_ALL_LEAVE_TABLE_ID = os.getenv(
    "OSE_ALL_LEAVE_TABLE_ID",
    os.getenv("OSE_LEAVE_TABLE_ID", "tblmHJHe12BCJRD8"),
).strip()
# OSE leave request + approval (Submit Leave form; not the same as webapp OSE display list).
OSE_LEAVE_TABLE_ID = os.getenv("OSE_LEAVE_TABLE_ID", OSE_ALL_LEAVE_TABLE_ID).strip()
OSE_OFFSET_TABLE_ID = os.getenv("OSE_OFFSET_TABLE_ID", "tblC5T2MAydwT42j")

# Bump when leave/admin Bitable routing changes (check /api/admin/leave-list meta).
OSE_LEAVE_API_BUILD = "20260603-leaveose-pinned-v4"

# leaveose sheet — OSE display MUST use this table ID (env cannot point at leave 全员).
LEAVEOSE_TABLE_ID_CANONICAL = "tblvoXE0hsPjgb0j"
LEAVEOSE_TABLE_ID = LEAVEOSE_TABLE_ID_CANONICAL

# OSE roster — sheet col A labels vs roster keys (31 people on ``AS33r7``).
# HRMS variants like ``Augustine (Si Yew)`` resolve to key ``Augustine Si yew``.
OSE_LEAVE_SHEET_ROSTER: tuple[tuple[str, str], ...] = (
    ("Louie", "Louie (Senior)"),
    ("Chrisjames", "Chrisjames [Game]"),
    ("Ronnel Dagatan", "Ronnel Dagatan"),
    ("Rizaldy Valdez Jr.", "Rizaldy Valdez Jr."),
    ("Renzfrd Angeles", "Renzfrd Angeles"),
    ("Art Eli Aiuri Bernrdo Bautista", "Art Eli Aiuri Bernrdo Bautista"),
    ("Man Chung", "Man Chung [Platform]"),
    ("Augustine Si yew", "Augustine Si yew (Senior)"),
    ("Bryan Peh", "Bryan Peh [Platform]"),
    ("Jan Rei", "Jan Rei [Platform]"),
    ("Katleen", "Katleen [Game]"),
    ("Mark Ginber Natal", "Mark Ginber Natal"),
    ("Eldrick Dion Marasigan", "Eldrick Dion Marasigan"),
    ("Leandro Lacson Jr.", "Leandro Lacson Jr."),
    ("Christian Rjie Reyes", "Christian Rjie Reyes"),
    ("Lynette", "Lynette (Senior)"),
    ("Eduard James", "Eduard James [Platform]"),
    ("Chris Jay Montecalvo", "Chris Jay Montecalvo"),
    ("Dexter Ortiz", "Dexter Ortiz"),
    ("Nad Kyro Bechayda", "Nad Kyro Bechayda"),
    ("Leonard Arguelles", "Leonard Arguelles"),
    ("Chun Chee", "Chun Chee [Platform]"),
    ("Jun Chen", "Jun Chen [Game]"),
    ("Kenneth", "Kenneth [Game]"),
    ("Jewel", "Jewel [Platform]"),
    ("Reuben Jherico Silerio", "Reuben Jherico Silerio"),
    ("Alexandra Del Rosario", "Alexandra Del Rosario"),
    ("Clint Nathan Calumpad", "Clint Nathan Calumpad"),
    ("Sarah Jean Sulit", "Sarah Jean Sulit [QA]"),
    ("Kheng Kwan", "Kheng Kwan [Platform]"),
    ("Kris Ng", "Kris Ng [Game]"),
)

OSE_SHIFT_ROSTER = OSE_LEAVE_SHEET_ROSTER
TARGET_NAMES = [key for key, _label in OSE_LEAVE_SHEET_ROSTER]
OSE_LEAVE_ROSTER_KEYS = TARGET_NAMES

MONTH_MAP = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}

_CJK_MONTH_NAMES: dict[str, int] = {
    "一月": 1,
    "二月": 2,
    "三月": 3,
    "四月": 4,
    "五月": 5,
    "六月": 6,
    "七月": 7,
    "八月": 8,
    "九月": 9,
    "十月": 10,
    "十一月": 11,
    "十二月": 12,
}

_OFFSET_LOOKUP_QUERY_RE = re.compile(
    r"(?i)(?:"
    r"谁.{0,12}(?:offset|调休|换班)|"
    r"(?:offset|调休|换班).{0,16}谁|"
    r"who(?:'s|\s+is|\s+are|\s+has|\s+had).{0,40}offset|"
    r"who\s+offset|"
    r"(?:show|list|check|view|see|display).{0,24}offset|"
    r"offset.{0,24}(?:calendar|schedule|list|month|who)"
    r")"
)


def parse_offset_month_from_text(
    text: str,
    *,
    default_year: Optional[int] = None,
) -> Optional[tuple[int, int]]:
    """Extract ``(year, month)`` from offset lookup phrasing (English + 中文)."""
    s = (text or "").strip()
    if not s:
        return None
    today = date.today()
    year = int(default_year) if default_year is not None else today.year

    m = re.search(r"(\d{4})\s*年?\s*(\d{1,2})\s*月", s)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            return y, mo

    for name, num in _CJK_MONTH_NAMES.items():
        if name in s:
            return year, num

    m = re.search(r"(\d{1,2})\s*月", s)
    if m:
        mo = int(m.group(1))
        if 1 <= mo <= 12:
            return year, mo

    low = s.lower()
    if re.search(r"(?i)\b(?:this|current)\s+month\b", low) or "这个月" in s or "本月" in s:
        return today.year, today.month
    if re.search(r"(?i)\bnext\s+month\b", low) or "下个月" in s:
        idx = today.year * 12 + (today.month - 1) + 1
        return idx // 12, (idx % 12) + 1
    if re.search(r"(?i)\blast\s+month\b", low) or "上个月" in s:
        idx = today.year * 12 + (today.month - 1) - 1
        return idx // 12, (idx % 12) + 1

    m = re.search(r"(?i)\b(?:for|in)\s+(\d{1,2})\b", s)
    if m:
        mo = int(m.group(1))
        if 1 <= mo <= 12:
            return year, mo
    if re.fullmatch(r"\d{1,2}", s.strip()):
        mo = int(s.strip())
        if 1 <= mo <= 12:
            return year, mo

    m = re.search(
        r"(?i)\b(?:for|in)\s+(january|february|march|april|may|june|july|august|"
        r"september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\b",
        s,
    )
    if m:
        token = m.group(1)
        for name, num in MONTH_MAP.items():
            if name.lower() == token.lower():
                return year, num

    for name, num in MONTH_MAP.items():
        if re.search(rf"(?i)\b{re.escape(name)}\b", s):
            return year, num

    return None


def _text_mentions_offset(s: str) -> bool:
    """``offset`` / 调休 / 换班 — no ``\\b`` (CJK text can touch ``offset``)."""
    return bool(re.search(r"(?i)offset|调休|换班", s or ""))


def match_roster_name_in_text(text: str) -> Optional[str]:
    """Find an OSE roster name mentioned in free text (longest unambiguous match)."""
    s = (text or "").strip()
    if not s:
        return None
    low = s.lower()
    hits: list[tuple[str, int]] = []
    for roster in OSE_LEAVE_FORM_NAMES:
        canon = _title_name(roster)
        phrase = canon.lower()
        if len(phrase) < 2:
            continue
        idx = low.find(phrase)
        if idx >= 0:
            before_ok = idx == 0 or not low[idx - 1].isalnum()
            after_idx = idx + len(phrase)
            after_ok = after_idx >= len(low) or not low[after_idx].isalnum()
            if before_ok and after_ok:
                hits.append((canon, len(phrase)))
                continue
        tokens = _word_tokens(roster)
        if len(tokens) == 1 and len(tokens[0]) >= 3:
            if re.search(rf"\b{re.escape(tokens[0])}\b", low):
                hits.append((canon, len(tokens[0])))
    for nick, canon in OSE_ROSTER_NICKNAMES.items():
        if re.search(rf"\b{re.escape(nick.lower())}\b", low):
            resolved = _title_name(canon)
            if _resolve_ose_roster_key(resolved) or resolved in {
                _title_name(r) for r in OSE_LEAVE_FORM_NAMES
            }:
                hits.append((resolved, len(nick)))
    if not hits:
        return None
    hits.sort(key=lambda item: -item[1])
    best_len = hits[0][1]
    top = [name for name, ln in hits if ln == best_len]
    if len(set(top)) != 1:
        return None
    return top[0]


def looks_like_offset_lookup_query(text: str) -> bool:
    """Read-only offset lookup (who / which month), not submit/edit/delete."""
    s = (text or "").strip()
    if not s or s.lstrip().startswith("/"):
        return False
    if not _text_mentions_offset(s):
        return False
    if _OFFSET_LOOKUP_QUERY_RE.search(s):
        return True
    if parse_offset_month_from_text(s) and not re.search(
        r"(?i)\b(apply|submit|request|swap|delete|edit|cancel|申请|删除|修改|取消)\b", s
    ):
        return True
    return False

# MY OSE team — the only people listed on the approver's offset view, in display order.
# Each entry is ``(display name, other spellings the offset table may carry)``: the sheet
# and HRMS write some of them differently (``Augustine Si yew``, ``Jeno``), and those
# variants must resolve to the one name shown here.
OSE_SHOWOFFSET_MY_ROSTER: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Kwang Ming", ()),
    ("Man Chung", ()),
    ("Augustine Si Yew", ("Augustine Si yew", "Augustine")),
    ("Jiun Hou Jeno", ("Jeno", "Jiun Hou")),
    ("Bryan Peh", ("Bryan",)),
    ("Chun Chee", ()),
    ("Jun Chen", ()),
    ("Kheng Kwan", ()),
    ("Kris Ng", ()),
)

OSE_SHOWOFFSET_NAMES: tuple[str, ...] = tuple(
    display for display, _aliases in OSE_SHOWOFFSET_MY_ROSTER
)

# Short names / chat nicknames → OSE roster key (word-boundary match in NL).
OSE_ROSTER_NICKNAMES: dict[str, str] = {
    # Jun Chen
    "jc": "Jun Chen",
    "jchen": "Jun Chen",
    "junchen": "Jun Chen",
    # Man Chung
    "mc": "Man Chung",
    "manchung": "Man Chung",
    # Chun Chee
    "cc": "Chun Chee",
    "chunchee": "Chun Chee",
    # Bryan Peh
    "bp": "Bryan Peh",
    "bryan": "Bryan Peh",
    "bryanpeh": "Bryan Peh",
    # Augustine Si yew
    "asy": "Augustine Si yew",
    "siyew": "Augustine Si yew",
    "augustine": "Augustine Si yew",
    "augustinesiyew": "Augustine Si yew",
    # Kheng Kwan / Kris Ng
    "kk": "Kheng Kwan",
    "kheng": "Kheng Kwan",
    "khengkwan": "Kheng Kwan",
    "kwan": "Kheng Kwan",
    "kn": "Kris Ng",
    "kris": "Kris Ng",
    "krisng": "Kris Ng",
    # Kenneth (single-name roster)
    "ken": "Kenneth",
    "kenneth": "Kenneth",
    # Katleen / Lynette / Jewel
    "kat": "Katleen",
    "katleen": "Katleen",
    "lynette": "Lynette",
    "lyn": "Lynette",
    "jewel": "Jewel",
    # Jan Rei
    "janrei": "Jan Rei",
    "jan": "Jan Rei",
    # Eduard James
    "eduard": "Eduard James",
    "eduardjames": "Eduard James",
    "ej": "Eduard James",
    # Dexter / Leonard / Nad
    "dexter": "Dexter Ortiz",
    "leonard": "Leonard Arguelles",
    "nad": "Nad Kyro Bechayda",
    # Mark / Eldrick / Leandro / Christian
    "mark": "Mark Ginber Natal",
    "eldrick": "Eldrick Dion Marasigan",
    "leandro": "Leandro Lacson Jr.",
    "christian": "Christian Rjie Reyes",
    "chrisjay": "Chris Jay Montecalvo",
    # Reuben / Alexandra / Clint / Sarah
    "reuben": "Reuben Jherico Silerio",
    "alexandra": "Alexandra Del Rosario",
    "clint": "Clint Nathan Calumpad",
    "sarah": "Sarah Jean Sulit",
}

DEBUG = False

# In-memory OSE shift sheet (avoids one full-sheet fetch per day for calendar / repeated /ose).
_OSE_SHEET_CACHE_TTL_SEC = int(os.getenv("OSE_SHEET_CACHE_SEC", "120"))
_OSE_SHEET_CACHE: dict[str, Any] = {"mono": 0.0, "values": None}
_OSE_LEAVE_SHEET_CACHE: dict[str, Any] = {"mono": 0.0, "values": None}

# Transient TLS/network blips (e.g. 07:00 morning card) — retry before surfacing an error card.
_OSE_LARK_HTTP_RETRIES = max(1, int(os.getenv("OSE_LARK_HTTP_RETRIES", "4")))
_OSE_LARK_HTTP_RETRY_BASE_SEC = float(os.getenv("OSE_LARK_HTTP_RETRY_BASE_SEC", "3"))
_OSE_BUILD_RETRIES = max(1, int(os.getenv("OSE_BUILD_RETRIES", "3")))
_OSE_BUILD_RETRY_SEC = float(os.getenv("OSE_BUILD_RETRY_SEC", "15"))
_TRANSIENT_REQUEST_ERRORS = (
    requests.exceptions.SSLError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def is_transient_ose_load_error(message: str) -> bool:
    """True for intermittent HTTPS/TLS failures that often succeed on retry."""
    s = (message or "").lower()
    return any(
        needle in s
        for needle in (
            "ssl:",
            "sslerror",
            "tlsv1_alert",
            "connectionpool",
            "connection error",
            "connection reset",
            "timed out",
            "temporarily unavailable",
            "max retries exceeded",
        )
    )


def _lark_request(method: str, url: str, *, retries: Optional[int] = None, **kwargs: Any) -> requests.Response:
    attempts = _OSE_LARK_HTTP_RETRIES if retries is None else max(1, retries)
    last_exc: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            return getattr(requests, method.lower())(url, **kwargs)
        except _TRANSIENT_REQUEST_ERRORS as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            delay = _OSE_LARK_HTTP_RETRY_BASE_SEC * attempt
            print(
                f"[ose_Duty] Lark HTTP retry {attempt}/{attempts} in {delay:.0f}s "
                f"({method.upper()} {url[:96]}): {exc!r}",
                flush=True,
            )
            time.sleep(delay)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"Lark HTTP {method.upper()} failed with no response")
_OSE_DIR = os.path.dirname(os.path.abspath(__file__))
_OFFSET_SHIFT_SHEET_APPLIED_PATH = os.path.join(_OSE_DIR, "offset_shift_sheet_applied.json")
_LEAVE_SHIFT_SHEET_APPLIED_PATH = os.path.join(_OSE_DIR, "leave_shift_sheet_applied.json")


def debug_print(*args, **kwargs) -> None:
    if DEBUG:
        print("[DEBUG]", *args, file=sys.stderr, **kwargs)


TARGET_USER_OPEN_ID = (
    os.getenv("omduty", "").strip()
    or os.getenv("OMDUTY", "").strip()
    or "ou_d7bc33724e2d6ced4050c944c2ca5650"
)

# Roster / offset requester names → Lark open_id when Bitable person fields are empty.
# Extend via ``OSE_PERSON_OPEN_IDS`` JSON in ``.env`` (same shape as ``LEAVE_CALENDAR_OPEN_IDS``).
_OSE_PERSON_OPEN_ID_DEFAULTS: dict[str, str] = {
    "Jewel": "ou_01a0b531dfbcc0d8af7d64c24262f7e9",
    "Jewell": "ou_01a0b531dfbcc0d8af7d64c24262f7e9",
    "Man Chung": "ou_50afe44c066a50645271f87b690d84a8",
    "Eduard James": "ou_cd2d456b36f2fab676b22e45e2b1425b",
    "Louie": "ou_da0c9fead4a1fc32475939898a42ceed",
    "Augustine Si Yew": "ou_d584e2a7bead0675ce4fd067ff1aa323",
    "Augustine (Si Yew)": "ou_d584e2a7bead0675ce4fd067ff1aa323",
    "Bryan Peh": "ou_bf2aef64949bb64b8a0b20f269f48f63",
    "Chrisjames": "ou_337d6634890698d9b707c21e3adc2616",
    "Chrisjames Dela Peña": "ou_337d6634890698d9b707c21e3adc2616",
    "Chun Chee": "ou_107031ebafc8f57d869ec3b895d064e8",
    "Katleen": "ou_a3c65f73c2c60c454fba7428b2cc98a5",
    "Katleen Cantos": "ou_a3c65f73c2c60c454fba7428b2cc98a5",
    "Kenneth": "ou_24511204a056d20506dd44c15c1310cf",
    "John Kenneth Chua": "ou_24511204a056d20506dd44c15c1310cf",
    "Kheng Kwan": "ou_ff21929cc95a20ce54db36fc69a220d8",
    "Kris Ng": "ou_3ae80e9dc0da4ee3ce8ffcb24d7ffbb2",
    "Lynette": "ou_3cb64233d3ee01b306eebd1b2878329c",
    "Lynette Enriquez": "ou_3cb64233d3ee01b306eebd1b2878329c",
    "Jun Chen": "ou_5f660c0fb0769d184aca635d02209272",
    "Jun Chen (Jc)": "ou_5f660c0fb0769d184aca635d02209272",
    "Yuxuan": "ou_c4346ace5927c14f51a89b2394b55338",
    "Jan Rei": "ou_25dd43efc70ab656c8c3f98b97ecade2",
    "Ronnel Dagatan": "ou_f72f7c06f274d5000c752ac60a7b1bd0",
    "Rizaldy Valdez Jr.": "ou_99b8cdde1f0f00b245254ed514dc10d8",
    "Rizaldy Valdez Jr": "ou_99b8cdde1f0f00b245254ed514dc10d8",
    "Renzfrd Angeles": "ou_1bd6de8e4afc675f40bda9a67a6f1f36",
    "Renzford Angeles": "ou_1bd6de8e4afc675f40bda9a67a6f1f36",
    "Art Eli Aiuri Bernrdo Bautista": "ou_ca3f27c025b86a9f09af498bbf20ebeb",
    "Art Eli Aluri Bautista": "ou_ca3f27c025b86a9f09af498bbf20ebeb",
    "Mark Ginber Natal": "ou_e41aa6319c714d15bc1160148e531be0",
    "Eldrick Dion Marasigan": "ou_1d0f1c85e21b8d68231a61e75e6c0bac",
    "Leandro Lacson Jr.": "ou_2572906e0c5a70d2bc92a176052114f3",
    "Leandro Lacson Jr": "ou_2572906e0c5a70d2bc92a176052114f3",
    "Christian Rjie Reyes": "ou_f6030ba07c02b58241aafc570d2eb35f",
    "Christian Reyes": "ou_f6030ba07c02b58241aafc570d2eb35f",
    "Chris Jay Montecalvo": "ou_eb0a1b0efb770928502b5d54dc8e7431",
    "Dexter Ortiz": "ou_8ac32d5bfd47896bc54d163588e7c4ef",
    "Nad Kyro Bechayda": "ou_c6f1647120b6c811ed7c4d4a8fa53a96",
    "Leonard Arguelles": "ou_8349d9a67d11f6b58f56b2675d9aca50",
    "Alexandra Del Rosario": "ou_47312e80fb0d2177310c29ea980f358f",
    "Clint Nathan Calumpad": "ou_e08f8c226dc76baad2cf5df423a9f75e",
    "Clint nathan Calumpad": "ou_e08f8c226dc76baad2cf5df423a9f75e",
    "Reuben Jherico Silerio": "ou_88b916c6d808cc13d8214c9a3c1a14cf",
    "Sarah Jean Sulit": "ou_a669c9f0fbe4c39be86450098e76aa40",
}


def _ose_person_open_id_overrides() -> dict[str, str]:
    """Name / name-key → open_id overrides (defaults + optional ``OSE_PERSON_OPEN_IDS`` env JSON)."""
    merged: dict[str, str] = dict(_OSE_PERSON_OPEN_ID_DEFAULTS)
    raw = (os.getenv("OSE_PERSON_OPEN_IDS") or "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                for k, v in parsed.items():
                    oid = str(v or "").strip()
                    if oid.startswith("ou_"):
                        merged[str(k).strip()] = oid
        except json.JSONDecodeError:
            pass
    out: dict[str, str] = {}
    for name, oid in merged.items():
        nm = _title_name(name)
        if not nm or not oid.startswith("ou_"):
            continue
        out[nm] = oid
        nk = _name_key(nm)
        if nk:
            out[nk] = oid
    return out


def _name_key(name: str) -> str:
    # "Augustine (Si Yew)" and "Augustine Si Yew" should match.
    return re.sub(r"[^a-z0-9]+", "", str(name or "").lower())


def _normalize_person_name_for_match(name: str) -> str:
    """Strip sheet tags ``[Platform]`` / ``(Senior)``; ``Augustine (Si Yew)`` → ``Augustine Si Yew``."""
    s = str(name or "").strip()
    s = re.sub(r"\[[^\]]*\]", "", s)
    s = re.sub(r"[()]", " ", s)
    return _title_name(re.sub(r"\s+", " ", s))


def _names_same_person_tokens(a: str, b: str) -> bool:
    if not a or not b:
        return False
    rk, lk = _name_key(a), _name_key(b)
    if rk and rk == lk:
        return True
    rt, lt = _word_tokens(a), _word_tokens(b)
    return _token_prefix_matches_roster_to_leave(rt, lt) or _token_prefix_matches_roster_to_leave(lt, rt)


def _resolve_ose_leave_roster_key(name: str) -> str:
    """Map HRMS/sheet/leave name to OSE leave-sheet roster key, or ``''`` if not on roster."""
    norm = _normalize_person_name_for_match(name)
    if not norm:
        return ""
    nk = _name_key(norm)
    for key, label in OSE_LEAVE_SHEET_ROSTER:
        if nk == _name_key(key) or nk == _name_key(label):
            return key
        if _names_same_person_tokens(norm, key) or _names_same_person_tokens(norm, label):
            # Fuzzy prefix match — but don't merge a distinct real person into
            # this roster slot (e.g. "Ken"/DB must not resolve to "Kenneth"/OSE).
            if dlm.are_distinct_known_people(name, key):
                continue
            return key
    return ""


def is_ose_leave_roster_name(name: str) -> bool:
    return bool(_resolve_ose_leave_roster_key(name))


def ose_leave_roster_sheet_label(roster_key: str) -> str:
    key = (roster_key or "").strip()
    for rk, label in OSE_LEAVE_SHEET_ROSTER:
        if rk == key:
            return label
    return key


def _resolve_ose_roster_key(name: str) -> str:
    """Map HRMS/sheet/leave name to OSE roster key, or ``''`` if not on roster."""
    return _resolve_ose_leave_roster_key(name)


def is_ose_shift_roster_name(name: str) -> bool:
    return bool(_resolve_ose_roster_key(name))


def ose_roster_sheet_label(roster_key: str) -> str:
    key = (roster_key or "").strip()
    for rk, label in OSE_LEAVE_SHEET_ROSTER:
        if rk == key:
            return label
    return key


def _word_tokens(name: str) -> list[str]:
    """Lowercase word tokens for roster short name vs leave-sheet full name."""
    return [t.lower() for t in re.findall(r"[A-Za-z0-9]+", str(name or "")) if t]


def _token_prefix_matches_roster_to_leave(roster_tokens: list[str], leave_tokens: list[str]) -> bool:
    """
    True if roster name refers to the same person as leave full name.
    Examples: Lynette ↔ Lynette Enriquez; Jun Chen ↔ Jun Chen Wong.
    First token may match by equality or (if roster is a single word) by prefix with min length 3
    to avoid matching unrelated one-letter names.
    """
    if not roster_tokens or not leave_tokens:
        return False
    if len(roster_tokens) > len(leave_tokens):
        return False
    for i, rw in enumerate(roster_tokens):
        lw = leave_tokens[i]
        if i == 0 and len(roster_tokens) == 1:
            if rw == lw:
                return True
            if len(rw) >= 3 and lw.startswith(rw):
                return True
            return False
        if rw != lw:
            return False
    return True


def _names_same_person(roster_name: str, leave_sheet_name: str) -> bool:
    """Roster / shift label vs leave Bitable full name (may differ in length)."""
    a = _normalize_person_name_for_match(roster_name)
    b = _normalize_person_name_for_match(leave_sheet_name)
    # Two confidently-known, different dutyList people are never the same person,
    # even if one name is a character-prefix of the other ("Ken" vs "Kenneth").
    if dlm.are_distinct_known_people(roster_name, leave_sheet_name):
        return False
    ra = _resolve_ose_roster_key(a) or a
    rb = _resolve_ose_roster_key(b) or b
    if ra and rb and _name_key(ra) == _name_key(rb):
        return True
    return _names_same_person_tokens(ra, rb)


def _title_name(name: str) -> str:
    return re.sub(r"\s+", " ", str(name or "").strip()).title()


def _field_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (int, float)):
        return str(v).strip()
    if isinstance(v, dict):
        t = str(v.get("name") or v.get("text") or v.get("value") or "").strip()
        if t:
            return t
        return str(v).strip()
    if isinstance(v, list):
        parts: list[str] = []
        for item in v:
            s = _field_text(item)
            if s:
                parts.append(s)
        return ", ".join(parts).strip()
    return str(v).strip()


def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def _get_field_by_aliases(fields: dict[str, Any], aliases: list[str]) -> Any:
    """
    Resolve field value by fuzzy key matching (case/space/punct-insensitive).
    Useful when Bitable returns slightly different field names.
    """
    if not isinstance(fields, dict):
        return None
    if not aliases:
        return None
    alias_norm = [_norm_key(a) for a in aliases if a]
    if not alias_norm:
        return None

    # 1) exact normalized match
    for k, v in fields.items():
        nk = _norm_key(k)
        if nk in alias_norm:
            return v
    # 2) contains match (both directions)
    for k, v in fields.items():
        nk = _norm_key(k)
        for an in alias_norm:
            if an in nk or nk in an:
                return v
    return None


def _bitable_field_original_date(fields: dict[str, Any]) -> Optional[date]:
    """Original swap-from date — never use bare ``Date`` (matches Exchange / Request Date)."""
    return _parse_date_value(_get_field_by_aliases(fields, ["Original Date", "Orig Date"]))


def _bitable_field_exchange_date(fields: dict[str, Any]) -> Optional[date]:
    return _parse_date_value(
        _get_field_by_aliases(fields, ["Exchange Date", "Swap Date", "Target Date"])
    )


def _bitable_field_request_date(fields: dict[str, Any]) -> Optional[date]:
    return _parse_date_value(
        _get_field_by_aliases(fields, ["Request Date", "Submitted Date", "Created Date"])
    )


def _parse_date_value(v: Any) -> Optional[date]:
    if v is None or v == "":
        return None
    if isinstance(v, list):
        for item in v:
            d = _parse_date_value(item)
            if d:
                return d
        return None
    if isinstance(v, dict):
        # common date-like keys in bitable payload
        for key in ("value", "timestamp", "ts", "date", "start_date", "end_date"):
            if key in v:
                d = _parse_date_value(v.get(key))
                if d:
                    return d
        # fallback: try dict text flatten
        sdict = _field_text(v)
        if sdict:
            return _parse_date_value(sdict)
        return None
    if isinstance(v, (int, float)):
        ts = int(v)
        if ts > 10**12:
            ts //= 1000
        try:
            return datetime.fromtimestamp(ts).date()
        except Exception:
            return None
    s = _field_text(v)
    if not s:
        return None
    fmts = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    )
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    m = re.match(r"^\s*(\d{4})[-/](\d{2})[-/](\d{2})", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m2 = re.match(r"^\s*(\d{2})[-/](\d{2})[-/](\d{4})", s)
    if m2:
        try:
            return date(int(m2.group(3)), int(m2.group(2)), int(m2.group(1)))
        except ValueError:
            return None
    return None


def _format_ddmmyyyy(d: Optional[date]) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else "-"


def _format_yyyymmdd(d: Optional[date]) -> str:
    return d.strftime("%Y/%m/%d") if isinstance(d, date) else ""


def col_index_to_letter(col_index: int) -> str:
    letters = ""
    while col_index > 0:
        col_index -= 1
        letters = chr(65 + (col_index % 26)) + letters
        col_index //= 26
    return letters


def get_tenant_access_token() -> str:
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = _lark_request("post", url, headers=headers, json=data, timeout=20)
    result = resp.json()
    if result.get("code") != 0:
        raise RuntimeError(f"Failed to get token: {result}")
    return result["tenant_access_token"]


def get_sheet_metadata(token: str, spreadsheet_token: str, sheet_id: str) -> Optional[dict[str, Any]]:
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
    headers = {"Authorization": f"Bearer {token}"}
    result = _lark_request("get", url, headers=headers, timeout=20).json()
    if result.get("code") != 0:
        debug_print(f"Metadata error: {result.get('msg')}")
        return None
    sheets = result.get("data", {}).get("sheets", [])
    for sheet in sheets:
        if sheet.get("sheetId") == sheet_id:
            return {
                "rowCount": sheet.get("rowCount"),
                "columnCount": sheet.get("columnCount"),
            }
    return None


def get_range_values(token: str, spreadsheet_token: str, sheet_id: str, range_str: str) -> Optional[list[list[Any]]]:
    url = (
        f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/"
        f"{spreadsheet_token}/values/{sheet_id}!{range_str}?valueRenderOption=FormattedValue"
    )
    headers = {"Authorization": f"Bearer {token}"}
    result = _lark_request("get", url, headers=headers, timeout=30).json()
    if result.get("code") != 0:
        debug_print(f"Range values error: {result}")
        return None
    return result.get("data", {}).get("valueRange", {}).get("values", [])


def parse_month_year(text: Any) -> tuple[Optional[int], Optional[int]]:
    s = _field_text(text)
    if not s:
        return None, None
    for mon_name, mon_num in MONTH_MAP.items():
        pattern = rf"\b{re.escape(mon_name)}\b\s+(\d{{4}})"
        m = re.search(pattern, s, re.IGNORECASE)
        if m:
            return mon_num, int(m.group(1))
    return None, None


def _get_cached_ose_sheet_values_for(sheet_id: str) -> tuple[Optional[list[list[Any]]], Optional[str]]:
    """Fetch full sheet once; short TTL cache per ``sheet_id``."""
    sid = (sheet_id or SHEET_ID).strip()
    cache = _OSE_LEAVE_SHEET_CACHE if sid == LEAVE_SHEET_ID and LEAVE_SHEET_ID != SHEET_ID else _OSE_SHEET_CACHE
    now = time.monotonic()
    if (
        _OSE_SHEET_CACHE_TTL_SEC > 0
        and isinstance(cache.get("values"), list)
        and now - float(cache.get("mono") or 0) < _OSE_SHEET_CACHE_TTL_SEC
    ):
        return cache["values"], None
    if not SPREADSHEET_TOKEN or not sid:
        return None, "OSE_SPREADSHEET_TOKEN / sheet id not set"
    try:
        token = get_tenant_access_token()
    except Exception as e:
        return None, str(e)
    props = get_sheet_metadata(token, SPREADSHEET_TOKEN, sid)
    if not props:
        return None, f"Sheet metadata unavailable for {sid!r}"
    max_row = props.get("rowCount", 200)
    max_col = props.get("columnCount", 200)
    scan_range = f"A1:{col_index_to_letter(max_col)}{max_row}"
    values = get_range_values(token, SPREADSHEET_TOKEN, sid, scan_range)
    if not values or len(values) < 2:
        return None, f"Empty or invalid sheet range for {sid!r}"
    cache["values"] = values
    cache["mono"] = now
    return values, None


def _get_cached_ose_sheet_values() -> tuple[Optional[list[list[Any]]], Optional[str]]:
    """OSE wiki shift sheet (``AS33r7``): duty, offset, and leave cell updates."""
    return _get_cached_ose_sheet_values_for(SHEET_ID)


def _get_cached_ose_leave_sheet_values() -> tuple[Optional[list[list[Any]]], Optional[str]]:
    """Leave markers use the same sheet as duty when ``LEAVE_SHEET_ID`` matches ``SHEET_ID``."""
    return _get_cached_ose_sheet_values_for(LEAVE_SHEET_ID)


def _date_column_for_matrix(values: list[list[Any]], target_date: date) -> Optional[int]:
    """Column index for ``target_date`` on the OSE shift sheet (same header rules as legacy scan)."""
    current_year = target_date.year
    current_month = target_date.month
    current_day = target_date.day
    for row_idx in range(1, min(15, len(values))):
        row = values[row_idx] if row_idx < len(values) else []
        for col in range(len(row)):
            try:
                day_num = int(str(row[col]).strip())
            except Exception:
                continue
            if day_num != current_day:
                continue
            header = ""
            for hcol in range(col, -1, -1):
                if hcol < len(values[0]) and values[0][hcol]:
                    header = values[0][hcol]
                    break
            mon_num, year = parse_month_year(header)
            if mon_num == current_month and year == current_year:
                return col
    return None


def _target_name_rows_from_matrix(
    values: list[list[Any]],
    targets: Optional[list[str]] = None,
) -> dict[str, int]:
    name_rows: dict[str, int] = {}
    scan_targets = sorted(
        list(targets if targets is not None else TARGET_NAMES),
        key=lambda t: len(t),
        reverse=True,
    )
    label_by_key = {key: label for key, label in OSE_LEAVE_SHEET_ROSTER}
    for row_idx in range(2, len(values)):
        row = values[row_idx]
        if not row:
            continue
        name_cell = _field_text(row[0] if len(row) > 0 else "")
        if not name_cell:
            continue
        up = name_cell.upper()
        for target in scan_targets:
            if target in name_rows:
                continue
            label = label_by_key.get(target, target)
            if up.startswith(label.upper()) or up.startswith(target.upper()):
                name_rows[target] = row_idx
                break
    return name_rows


def _shift_codes_from_matrix(
    values: list[list[Any]], target_date: date
) -> tuple[list[str], list[str], list[str]]:
    """Parse ``D`` / ``N`` / roster ``L`` (leave) for one calendar day."""
    date_col = _date_column_for_matrix(values, target_date)
    if date_col is None:
        return [], [], []

    morning: list[str] = []
    night: list[str] = []
    roster_leave: list[str] = []
    for name, row_idx in _target_name_rows_from_matrix(values).items():
        row = values[row_idx]
        if date_col >= len(row):
            continue
        code = _field_text(row[date_col]).upper()
        if code == "D":
            morning.append(_title_name(name))
        elif code == "N":
            night.append(_title_name(name))
        elif code in _OSE_SHIFT_SHEET_LEAVE_CODES:
            roster_leave.append(_title_name(name))
    return sorted(morning), sorted(night), sorted(roster_leave)


def _merge_roster_sheet_leave(
    leave_entries: list[dict[str, Any]],
    roster_leave_names: list[str],
    target_date: date,
) -> list[dict[str, Any]]:
    """Add OSE sheet ``L`` markers when not already covered by Bitable leave."""
    if not roster_leave_names:
        return leave_entries
    out = list(leave_entries)
    for name in roster_leave_names:
        if any(_names_same_person(name, str(r.get("name") or "")) for r in out):
            continue
        out.append(
            {
                "name": _title_name(name),
                "leave_type": "Leave",
                "start": target_date,
                "end": target_date,
                "source": "roster",
            }
        )
    return sorted(out, key=lambda x: str(x.get("name") or "").lower())


def _shift_names_from_matrix(values: list[list[Any]], target_date: date) -> tuple[list[str], list[str]]:
    """Parse ``D`` / ``N`` for one calendar day from preloaded sheet ``values`` (same rules as legacy scan)."""
    morning, night, _ = _shift_codes_from_matrix(values, target_date)
    return morning, night


def _invalidate_ose_sheet_cache(*, sheet_id: Optional[str] = None) -> None:
    sid = (sheet_id or "").strip()
    if not sid or sid == SHEET_ID:
        _OSE_SHEET_CACHE["values"] = None
        _OSE_SHEET_CACHE["mono"] = 0.0
    if not sid or sid == LEAVE_SHEET_ID:
        _OSE_LEAVE_SHEET_CACHE["values"] = None
        _OSE_LEAVE_SHEET_CACHE["mono"] = 0.0


def _sheet_row_index_for_person(
    values: list[list[Any]],
    person: str,
    *,
    targets: Optional[list[str]] = None,
) -> Optional[int]:
    """0-based matrix row for a roster person on an OSE sheet."""
    nm = _title_name(person)
    if not nm:
        return None
    for target, row_idx in _target_name_rows_from_matrix(values, targets).items():
        if _names_same_person(target, nm):
            return row_idx
    return None


_OSE_SHIFT_SHEET_BG_DUTY = "#FFFFFF"
_OSE_SHIFT_SHEET_BG_OFFSET = "#8F959E"
_OSE_SHIFT_SHEET_BG_HOLIDAY = "#8EE085"
_OSE_SHIFT_SHEET_STYLED_VALUES = frozenset({"D", "N", "L", "AL", "SL", "HL", "EL"})
_OSE_SHIFT_SHEET_LEAVE_CODES = frozenset({"L", "AL", "SL", "HL", "EL"})


def _shift_sheet_cell_range(row_idx: int, col_idx: int, *, sheet_id: Optional[str] = None) -> str:
    """Lark range for one cell (e.g. ``AS33r7!FG33:FG33``)."""
    sid = (sheet_id or SHEET_ID).strip()
    cell = f"{col_index_to_letter(col_idx + 1)}{row_idx + 1}"
    return f"{sid}!{cell}:{cell}"


def _date_for_matrix_column(values: list[list[Any]], col_idx: int) -> Optional[date]:
    """Map a sheet matrix column index to its calendar date (inverse of ``_date_column_for_matrix``)."""
    if col_idx < 0 or not values:
        return None
    header = ""
    top = values[0] if values else []
    for hcol in range(min(col_idx, len(top)), -1, -1):
        if hcol < len(top) and top[hcol]:
            header = _field_text(top[hcol])
            break
    mon_num, year = parse_month_year(header)
    if not mon_num or not year:
        return None
    for row_idx in range(1, min(15, len(values))):
        row = values[row_idx] if row_idx < len(values) else []
        if col_idx >= len(row):
            continue
        try:
            day_num = int(_field_text(row[col_idx]))
        except (TypeError, ValueError):
            continue
        if day_num < 1 or day_num > 31:
            continue
        try:
            candidate = date(year, mon_num, day_num)
        except ValueError:
            continue
        if _date_column_for_matrix(values, candidate) == col_idx:
            return candidate
    return None


def _shift_sheet_back_color_for_value(val: str, *, is_holiday: bool = False) -> Optional[str]:
    v = (val or "").strip().upper()
    if v not in _OSE_SHIFT_SHEET_STYLED_VALUES and v != "*":
        return None
    if is_holiday:
        return _OSE_SHIFT_SHEET_BG_HOLIDAY
    if v in _OSE_SHIFT_SHEET_STYLED_VALUES:
        return _OSE_SHIFT_SHEET_BG_DUTY
    if v == "*":
        return _OSE_SHIFT_SHEET_BG_OFFSET
    return None


def _shift_sheet_holiday_dates() -> frozenset[date]:
    try:
        from holiday import get_holiday_date_set

        return get_holiday_date_set()
    except Exception as exc:
        print(f"[ose_Duty] holiday.csv load failed for sheet styling: {exc!r}", flush=True)
        return frozenset()


def _cell_is_holiday(
    col_idx: int,
    holidays: frozenset[date],
    *,
    values: Optional[list[list[Any]]] = None,
    col_dates: Optional[dict[int, date]] = None,
) -> bool:
    if not holidays:
        return False
    if col_dates and col_idx in col_dates:
        return col_dates[col_idx] in holidays
    if values is None:
        return False
    d = _date_for_matrix_column(values, col_idx)
    return bool(d and d in holidays)


def _put_ose_shift_sheet_cell_styles(
    token: str,
    cell_updates: list[tuple[int, int, str]],
    *,
    values: Optional[list[list[Any]]] = None,
    col_dates: Optional[dict[int, date]] = None,
    sheet_id: Optional[str] = None,
) -> None:
    """Set background on duty cells: D/N/L/AL/SL/HL/EL white, ``*`` #8F959E; holiday #8EE085 for duty/leave codes."""
    sid = (sheet_id or SHEET_ID).strip()
    if not cell_updates:
        return
    if not SPREADSHEET_TOKEN or not sid:
        raise RuntimeError("OSE shift sheet not configured (OSE_SPREADSHEET_TOKEN / sheet id)")
    holidays = _shift_sheet_holiday_dates()
    data: list[dict[str, Any]] = []
    for row_idx, col_idx, val in cell_updates:
        if row_idx < 0 or col_idx < 0:
            continue
        back_color = _shift_sheet_back_color_for_value(
            val,
            is_holiday=_cell_is_holiday(col_idx, holidays, values=values, col_dates=col_dates),
        )
        if not back_color:
            continue
        if back_color == _OSE_SHIFT_SHEET_BG_HOLIDAY:
            on = (col_dates or {}).get(col_idx) or (
                _date_for_matrix_column(values, col_idx) if values else None
            )
            print(
                f"[ose_Duty] offset holiday style row={row_idx + 1} col={col_idx + 1} "
                f"val={val!r} date={on} -> {back_color}",
                flush=True,
            )
        data.append(
            {"ranges": [_shift_sheet_cell_range(row_idx, col_idx, sheet_id=sid)], "style": {"backColor": back_color}}
        )
    if not data:
        return
    url = (
        f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/"
        f"{SPREADSHEET_TOKEN}/styles_batch_update"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    res = _lark_request("put", url, headers=headers, json={"data": data}, timeout=60).json()
    if res.get("code") != 0:
        raise RuntimeError(f"OSE shift sheet style write failed: {res}")


def _put_ose_shift_sheet_cells(
    token: str,
    cell_updates: list[tuple[int, int, str]],
    *,
    values: Optional[list[list[Any]]] = None,
    col_dates: Optional[dict[int, date]] = None,
    sheet_id: Optional[str] = None,
) -> None:
    """Write duty cells: each item is (matrix_row_idx, col_idx, value) both 0-based."""
    sid = (sheet_id or SHEET_ID).strip()
    if not cell_updates:
        return
    if not SPREADSHEET_TOKEN or not sid:
        raise RuntimeError("OSE shift sheet not configured (OSE_SPREADSHEET_TOKEN / sheet id)")
    value_ranges: list[dict[str, Any]] = []
    for row_idx, col_idx, val in cell_updates:
        if row_idx < 0 or col_idx < 0:
            continue
        value_ranges.append(
            {"range": _shift_sheet_cell_range(row_idx, col_idx, sheet_id=sid), "values": [[val]]}
        )
    if not value_ranges:
        return
    url = (
        f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/"
        f"{SPREADSHEET_TOKEN}/values_batch_update"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    res = _lark_request("post", url, headers=headers, json={"valueRanges": value_ranges}, timeout=60).json()
    if res.get("code") != 0:
        raise RuntimeError(f"OSE shift sheet write failed: {res}")
    _put_ose_shift_sheet_cell_styles(
        token, cell_updates, values=values, col_dates=col_dates, sheet_id=sid
    )
    _invalidate_ose_sheet_cache(sheet_id=sid)


def _load_offset_shift_sheet_state() -> dict[str, Any]:
    try:
        with open(_OFFSET_SHIFT_SHEET_APPLIED_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return {"record_ids": [], "by_record": {}}
    except Exception:
        return {"record_ids": [], "by_record": {}}
    if isinstance(data, list):
        return {"record_ids": [str(x).strip() for x in data if str(x).strip()], "by_record": {}}
    if not isinstance(data, dict):
        return {"record_ids": [], "by_record": {}}
    ids = [str(x).strip() for x in (data.get("record_ids") or []) if str(x).strip()]
    raw_by = data.get("by_record") if isinstance(data.get("by_record"), dict) else {}
    by_record = {str(k).strip(): dict(v) for k, v in raw_by.items() if str(k).strip() and isinstance(v, dict)}
    return {"record_ids": ids, "by_record": by_record}


def _save_offset_shift_sheet_state(record_ids: set[str], by_record: dict[str, dict[str, str]]) -> None:
    tmp = _OFFSET_SHIFT_SHEET_APPLIED_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "record_ids": sorted(record_ids),
                "by_record": {k: by_record[k] for k in sorted(by_record)},
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )
        fh.write("\n")
    os.replace(tmp, _OFFSET_SHIFT_SHEET_APPLIED_PATH)


def _load_offset_shift_sheet_applied() -> set[str]:
    state = _load_offset_shift_sheet_state()
    return set(state.get("record_ids") or [])


def _offset_shift_sheet_snapshot_for_row(
    row: dict[str, Any],
    plan: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    od = _parse_date_value(row.get("original_date"))
    xd = _parse_date_value(row.get("exchange_date"))
    snap: dict[str, Any] = {
        "sheet_id": SHEET_ID,
        "request_person": str(row.get("request_person") or ""),
        "exchange_person": str(row.get("exchange_person") or ""),
        "original_date": od.isoformat() if od else "",
        "exchange_date": xd.isoformat() if xd else "",
        "shift_type": str(row.get("shift_type") or "").strip().upper(),
    }
    if plan:
        snap["cells"] = [
            {"row": r, "col": c, "val": v}
            for r, c, v in (plan.get("updates") or [])
        ]
    return snap


def _offset_shift_sheet_live_ok(
    values: list[list[Any]],
    plan: dict[str, Any],
) -> bool:
    """True when every planned cell on the sheet already shows the expected value."""
    updates = plan.get("updates") or []
    if not updates:
        return False
    for row_idx, col_idx, val in updates:
        expected = str(val or "").strip().upper()
        cur = _shift_sheet_cell_value(values, int(row_idx), int(col_idx))
        if cur != expected:
            return False
    return True


def _mark_offset_shift_sheet_applied(record_id: str, *, snapshot: Optional[dict[str, str]] = None) -> None:
    rid = (record_id or "").strip()
    if not rid:
        return
    state = _load_offset_shift_sheet_state()
    ids = set(state.get("record_ids") or [])
    by_record = dict(state.get("by_record") or {})
    ids.add(rid)
    if snapshot:
        by_record[rid] = dict(snapshot)
    _save_offset_shift_sheet_state(ids, by_record)


def _unmark_offset_shift_sheet_applied(record_id: str) -> None:
    rid = (record_id or "").strip()
    if not rid:
        return
    state = _load_offset_shift_sheet_state()
    ids = set(state.get("record_ids") or [])
    by_record = dict(state.get("by_record") or {})
    if rid not in ids and rid not in by_record:
        return
    ids.discard(rid)
    by_record.pop(rid, None)
    _save_offset_shift_sheet_state(ids, by_record)


def offset_shift_sheet_already_applied(record_id: str) -> bool:
    rid = (record_id or "").strip()
    return bool(rid and rid in _load_offset_shift_sheet_applied())


def _compute_offset_shift_sheet_plan(
    *,
    request_person: str,
    exchange_person: str,
    original_date: date,
    exchange_date: date,
    shift_type: str,
    values: list[list[Any]],
) -> dict[str, Any]:
    """Resolve the cell value updates for one approved offset swap."""
    st = (shift_type or "").strip().upper()
    if st not in OSE_SHIFT_TYPES:
        raise ValueError(f"Shift Type must be one of {OSE_SHIFT_TYPES}")
    req = _title_name(request_person)
    exc = _title_name(exchange_person)
    if not req:
        raise ValueError("request_person is required")
    if not exc:
        exc = req
    orig_col = _date_column_for_matrix(values, original_date)
    exc_col = _date_column_for_matrix(values, exchange_date)
    if orig_col is None:
        raise ValueError(f"Could not find sheet column for original date {original_date.isoformat()}")
    if exc_col is None:
        raise ValueError(f"Could not find sheet column for exchange date {exchange_date.isoformat()}")
    req_row = _sheet_row_index_for_person(values, req)
    if req_row is None:
        raise ValueError(f"Could not find shift sheet row for request person {req!r}")
    same_person = _names_same_person(req, exc)
    updates: list[tuple[int, int, str]] = [
        (req_row, orig_col, "*"),
        (req_row, exc_col, st),
    ]
    if not same_person:
        exc_row = _sheet_row_index_for_person(values, exc)
        if exc_row is None:
            raise ValueError(f"Could not find shift sheet row for exchange person {exc!r}")
        updates.extend([(exc_row, orig_col, st), (exc_row, exc_col, "*")])
    return {
        "req": req,
        "exc": exc,
        "st": st,
        "same_person": same_person,
        "updates": updates,
        "col_dates": {orig_col: original_date, exc_col: exchange_date},
    }


def apply_approved_offset_to_shift_sheet(
    *,
    request_person: str,
    exchange_person: str,
    original_date: date,
    exchange_date: date,
    shift_type: str,
) -> dict[str, Any]:
    """
    Apply an approved offset swap to OSE2026 (``3RIBRL``): ``*`` on swapped-off days,
    ``D``/``N`` on swapped-on days. Exchange with self only updates the requester's row.
    """
    values, err = _get_cached_ose_sheet_values()
    if not values:
        raise RuntimeError(err or "Could not load OSE shift sheet")
    plan = _compute_offset_shift_sheet_plan(
        request_person=request_person,
        exchange_person=exchange_person,
        original_date=original_date,
        exchange_date=exchange_date,
        shift_type=shift_type,
        values=values,
    )
    updates = plan["updates"]
    token = get_tenant_access_token()
    _put_ose_shift_sheet_cells(token, updates, values=values, col_dates=plan["col_dates"])
    return {
        "ok": True,
        "request_person": plan["req"],
        "exchange_person": plan["exc"],
        "original_date": original_date.isoformat(),
        "exchange_date": exchange_date.isoformat(),
        "shift_type": plan["st"],
        "cells_updated": len(updates),
        "myself": plan["same_person"],
    }


def apply_approved_offset_shift_sheet_for_record(record_id: str) -> dict[str, Any]:
    """Load an approved offset row and apply duty-sheet swap (idempotent per record_id)."""
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    row = get_ose_offset_record_admin_row(rid)
    if bool(row.get("pending")):
        return {"ok": False, "record_id": rid, "skipped": "still_pending"}
    status = str(row.get("approval_status") or "").strip().title()
    if status != "Approved":
        return {"ok": False, "record_id": rid, "skipped": f"status={status or 'unknown'}"}
    od_d = _parse_date_value(row.get("original_date"))
    xd = _parse_date_value(row.get("exchange_date"))
    if not od_d or not xd:
        raise ValueError("Original Date and Exchange Date are required on the offset row")
    values, err = _get_cached_ose_sheet_values()
    if not values:
        raise RuntimeError(err or f"Could not load OSE sheet {SHEET_ID!r}")
    plan = _compute_offset_shift_sheet_plan(
        request_person=str(row.get("request_person") or ""),
        exchange_person=str(row.get("exchange_person") or ""),
        original_date=od_d,
        exchange_date=xd,
        shift_type=str(row.get("shift_type") or ""),
        values=values,
    )
    state = _load_offset_shift_sheet_state()
    old_snap = dict((state.get("by_record") or {}).get(rid) or {})
    if str(old_snap.get("sheet_id") or "") in ("", "3RIBRL", "65p5cn") or old_snap.get("sheet_id") != SHEET_ID:
        old_snap = {}
    if (
        offset_shift_sheet_already_applied(rid)
        and old_snap
        and _offset_shift_sheet_live_ok(values, plan)
    ):
        return {"ok": True, "record_id": rid, "skipped": "already_applied"}
    if not plan.get("updates"):
        return {
            "ok": True,
            "record_id": rid,
            "skipped": "no_cell_updates",
            "request_person": plan.get("req"),
        }
    token = get_tenant_access_token()
    _put_ose_shift_sheet_cells(token, plan["updates"], values=values, col_dates=plan["col_dates"])
    snapshot = _offset_shift_sheet_snapshot_for_row(row, plan)
    _mark_offset_shift_sheet_applied(rid, snapshot=snapshot)
    return {
        "record_id": rid,
        "ok": True,
        "request_person": plan.get("req"),
        "exchange_person": plan.get("exc"),
        "original_date": od_d.isoformat(),
        "exchange_date": xd.isoformat(),
        "shift_type": plan.get("st"),
        "cells_updated": len(plan["updates"]),
        "myself": plan.get("same_person"),
    }


def revert_approved_offset_from_shift_sheet(
    *,
    request_person: str,
    exchange_person: str,
    original_date: date,
    exchange_date: date,
    shift_type: str,
) -> dict[str, Any]:
    """
    Undo an approved offset on OSE2026 (``3RIBRL``): inverse of
    :func:`apply_approved_offset_to_shift_sheet`.
    """
    st = (shift_type or "").strip().upper()
    if st not in OSE_SHIFT_TYPES:
        raise ValueError(f"Shift Type must be one of {OSE_SHIFT_TYPES}")
    req = _title_name(request_person)
    exc = _title_name(exchange_person)
    if not req:
        raise ValueError("request_person is required")
    if not exc:
        exc = req
    values, err = _get_cached_ose_sheet_values()
    if not values:
        raise RuntimeError(err or "Could not load OSE shift sheet")
    orig_col = _date_column_for_matrix(values, original_date)
    exc_col = _date_column_for_matrix(values, exchange_date)
    if orig_col is None:
        raise ValueError(f"Could not find sheet column for original date {original_date.isoformat()}")
    if exc_col is None:
        raise ValueError(f"Could not find sheet column for exchange date {exchange_date.isoformat()}")
    req_row = _sheet_row_index_for_person(values, req)
    if req_row is None:
        raise ValueError(f"Could not find shift sheet row for request person {req!r}")
    same_person = _names_same_person(req, exc)
    updates: list[tuple[int, int, str]] = [
        (req_row, orig_col, st),
        (req_row, exc_col, "*"),
    ]
    if not same_person:
        exc_row = _sheet_row_index_for_person(values, exc)
        if exc_row is None:
            raise ValueError(f"Could not find shift sheet row for exchange person {exc!r}")
        updates.extend([(exc_row, orig_col, "*"), (exc_row, exc_col, st)])
    token = get_tenant_access_token()
    col_dates = {orig_col: original_date, exc_col: exchange_date}
    _put_ose_shift_sheet_cells(token, updates, values=values, col_dates=col_dates)
    return {
        "ok": True,
        "request_person": req,
        "exchange_person": exc,
        "original_date": original_date.isoformat(),
        "exchange_date": exchange_date.isoformat(),
        "shift_type": st,
        "cells_updated": len(updates),
        "myself": same_person,
        "reverted": True,
    }


def _revert_shift_sheet_from_snapshot(snapshot: dict[str, str]) -> dict[str, Any]:
    od = _parse_date_value(snapshot.get("original_date"))
    xd = _parse_date_value(snapshot.get("exchange_date"))
    if not od or not xd:
        raise ValueError("snapshot missing Original Date or Exchange Date")
    return revert_approved_offset_from_shift_sheet(
        request_person=str(snapshot.get("request_person") or ""),
        exchange_person=str(snapshot.get("exchange_person") or ""),
        original_date=od,
        exchange_date=xd,
        shift_type=str(snapshot.get("shift_type") or ""),
    )


def _revert_shift_sheet_from_admin_row(row: dict[str, Any]) -> dict[str, Any]:
    od = _parse_date_value(row.get("original_date"))
    xd = _parse_date_value(row.get("exchange_date"))
    if not od or not xd:
        raise ValueError("offset row missing Original Date or Exchange Date")
    return revert_approved_offset_from_shift_sheet(
        request_person=str(row.get("request_person") or ""),
        exchange_person=str(row.get("exchange_person") or ""),
        original_date=od,
        exchange_date=xd,
        shift_type=str(row.get("shift_type") or ""),
    )


def _stored_offset_shift_snapshot(record_id: str) -> dict[str, Any]:
    rid = (record_id or "").strip()
    if not rid:
        return {}
    state = _load_offset_shift_sheet_state()
    return dict((state.get("by_record") or {}).get(rid) or {})


def resync_approved_offset_shift_sheet_after_edit(
    record_id: str,
    *,
    old_row: dict[str, Any],
) -> dict[str, Any]:
    """
    After an approver edits an approved offset row: undo the previous sheet swap,
    then apply the updated dates/people/shift when still Approved.
    """
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    old_status = str(old_row.get("approval_status") or "").strip().title()
    revert_out: dict[str, Any] = {"skipped": "was_not_approved"}
    if old_status == "Approved":
        stored = _stored_offset_shift_snapshot(rid)
        try:
            if stored.get("original_date") and stored.get("exchange_date"):
                revert_out = _revert_shift_sheet_from_snapshot(stored)
            else:
                revert_out = _revert_shift_sheet_from_admin_row(old_row)
            _unmark_offset_shift_sheet_applied(rid)
            _invalidate_ose_sheet_cache()
        except Exception as exc:
            revert_out = {"ok": False, "error": str(exc)}
            print(
                f"[ose_Duty] offset shift revert before edit failed for {rid!r}: {exc!r}",
                flush=True,
            )
            raise
    apply_out: dict[str, Any] = {"skipped": "not_approved"}
    fresh = get_ose_offset_record_admin_row(rid)
    if str(fresh.get("approval_status") or "").strip().title() == "Approved":
        apply_out = apply_approved_offset_shift_sheet_for_record(rid)
    return {"record_id": rid, "revert": revert_out, "apply": apply_out}


def revert_approved_offset_shift_sheet_for_record(record_id: str) -> dict[str, Any]:
    """Restore duty-sheet cells when an approved offset row (already applied) is deleted."""
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    if not offset_shift_sheet_already_applied(rid):
        return {"ok": True, "record_id": rid, "skipped": "not_applied"}
    stored = _stored_offset_shift_snapshot(rid)
    snapshot: Optional[dict[str, Any]] = None
    if stored.get("original_date") and stored.get("exchange_date"):
        snapshot = stored
    else:
        try:
            row = get_ose_offset_record_admin_row(rid)
        except KeyError:
            row = None
        if row is None:
            # Row already gone and no usable snapshot (legacy state written before
            # snapshots were stored). We cannot know which cells to undo, so stop
            # retrying forever — otherwise this id is polled for good and its ``*``
            # marks sit on the roster with no way to clear them.
            _unmark_offset_shift_sheet_applied(rid)
            print(
                f"[ose_Duty] offset {rid} deleted with no stored snapshot — cannot "
                f"revert its duty-sheet cells; clearing the applied flag. Check the "
                f"roster by hand if a stray '*' remains.",
                flush=True,
            )
            return {"ok": False, "record_id": rid, "skipped": "missing_snapshot_row_gone"}
        if row is not None:
            if bool(row.get("pending")):
                return {"ok": False, "record_id": rid, "skipped": "still_pending"}
            status = str(row.get("approval_status") or "").strip().title()
            if status != "Approved":
                return {"ok": False, "record_id": rid, "skipped": f"status={status or 'unknown'}"}
            snapshot = _offset_shift_sheet_snapshot_for_row(row)
    if not snapshot or not snapshot.get("original_date") or not snapshot.get("exchange_date"):
        return {"ok": True, "record_id": rid, "skipped": "missing_snapshot"}
    result = _revert_shift_sheet_from_snapshot(snapshot)
    _unmark_offset_shift_sheet_applied(rid)
    return {"record_id": rid, **result}


def scan_revert_deleted_offsets_from_shift_sheet() -> dict[str, int]:
    """Revert ``3RIBRL`` when an applied offset row was removed directly in Base."""
    state = _load_offset_shift_sheet_state()
    applied_ids = set(state.get("record_ids") or [])
    if not applied_ids:
        return {"scanned": 0, "reverted": 0, "errors": 0}
    invalidate_ose_bitable_cache()
    live_ids = {
        str(r.get("record_id") or "").strip()
        for r in (get_ose_offset_records_admin() or {}).get("items") or []
        if str(r.get("record_id") or "").strip()
    }
    reverted = 0
    errors = 0
    for rid in sorted(applied_ids):
        if rid in live_ids:
            continue
        try:
            out = revert_approved_offset_shift_sheet_for_record(rid)
            if out.get("reverted") or out.get("cells_updated"):
                reverted += 1
        except Exception as exc:
            errors += 1
            print(f"[ose_Duty] shift sheet revert poll failed for {rid!r}: {exc!r}", flush=True)
    return {"scanned": len(applied_ids), "reverted": reverted, "errors": errors}


_OFFSET_SHIFT_SHEET_REENSURE_DONE = False


def reensure_applied_offset_shift_sheet_styles_and_notes() -> dict[str, int]:
    """Re-apply offset cell backgrounds for already-applied approved rows (idempotent)."""
    state = _load_offset_shift_sheet_state()
    applied_ids = list(state.get("record_ids") or [])
    if not applied_ids:
        return {"scanned": 0, "styled": 0, "noted": 0, "errors": 0}
    values, err = _get_cached_ose_sheet_values()
    if not values:
        print(f"[ose_Duty] offset re-ensure skipped (no sheet): {err!r}", flush=True)
        return {"scanned": len(applied_ids), "styled": 0, "noted": 0, "errors": 1}
    token = get_tenant_access_token()
    styled = 0
    errors = 0
    for rid in applied_ids:
        try:
            try:
                row = get_ose_offset_record_admin_row(rid)
            except KeyError:
                continue
            if bool(row.get("pending")):
                continue
            if str(row.get("approval_status") or "").strip().title() != "Approved":
                continue
            od = _parse_date_value(row.get("original_date"))
            xd = _parse_date_value(row.get("exchange_date"))
            if not od or not xd:
                continue
            plan = _compute_offset_shift_sheet_plan(
                request_person=str(row.get("request_person") or ""),
                exchange_person=str(row.get("exchange_person") or ""),
                original_date=od,
                exchange_date=xd,
                shift_type=str(row.get("shift_type") or ""),
                values=values,
            )
            _put_ose_shift_sheet_cell_styles(
                token, plan["updates"], values=values, col_dates=plan.get("col_dates")
            )
            styled += 1
        except Exception as exc:
            errors += 1
            print(f"[ose_Duty] offset re-ensure failed for {rid!r}: {exc!r}", flush=True)
    return {"scanned": len(applied_ids), "styled": styled, "noted": 0, "errors": errors}


def probe_offset_shift_sheet_sync(*, apply: bool = False, json_out: bool = False) -> dict[str, Any]:
    """Debug approved offset -> sheet cell updates (same as poll, with per-row status)."""
    values, sheet_err = _get_cached_ose_sheet_values()
    applied_set = _load_offset_shift_sheet_applied()
    rows_out: list[dict[str, Any]] = []
    for row in (get_ose_offset_records_admin() or {}).get("items") or []:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("record_id") or "").strip()
        pending = bool(row.get("pending"))
        status = str(row.get("approval_status") or "").strip().title()
        entry: dict[str, Any] = {
            "record_id": rid,
            "request_person": row.get("request_person"),
            "exchange_person": row.get("exchange_person"),
            "original_date": row.get("original_date"),
            "exchange_date": row.get("exchange_date"),
            "shift_type": row.get("shift_type"),
            "approval_status": status,
            "pending": pending,
            "in_state_file": rid in applied_set,
        }
        if pending or status != "Approved":
            entry["skip"] = "pending" if pending else f"status={status or 'unknown'}"
        elif not values:
            entry["skip"] = f"sheet_unavailable:{sheet_err}"
        else:
            try:
                od_d = _parse_date_value(row.get("original_date"))
                xd = _parse_date_value(row.get("exchange_date"))
                plan = _compute_offset_shift_sheet_plan(
                    request_person=str(row.get("request_person") or ""),
                    exchange_person=str(row.get("exchange_person") or ""),
                    original_date=od_d,
                    exchange_date=xd,
                    shift_type=str(row.get("shift_type") or ""),
                    values=values,
                )
                live_ok = _offset_shift_sheet_live_ok(values, plan)
                drift = [
                    {
                        "cell": f"{col_index_to_letter(c + 1)}{r + 1}",
                        "current": _shift_sheet_cell_value(values, r, c),
                        "expected": v,
                    }
                    for r, c, v in (plan.get("updates") or [])
                    if _shift_sheet_cell_value(values, r, c) != str(v or "").strip().upper()
                ]
                entry["cells_to_update"] = len(plan.get("updates") or [])
                entry["live_ok"] = live_ok
                entry["drift"] = drift
                if not plan.get("updates"):
                    entry["skip"] = "no_cell_updates"
                elif live_ok:
                    entry["skip"] = "already_on_sheet"
                elif entry["in_state_file"]:
                    entry["skip"] = "state_says_applied_but_sheet_drift"
            except Exception as exc:
                entry["skip"] = f"plan_error:{exc}"
        rows_out.append(entry)
    sync_result: Optional[dict[str, Any]] = None
    if apply:
        sync_result = scan_bitable_approved_offsets_for_shift_sheet()
    out = {
        "ok": bool(values),
        "shift_sheet_id": SHEET_ID,
        "sheet_error": sheet_err,
        "approved_rows": sum(
            1 for r in rows_out if not r.get("pending") and r.get("approval_status") == "Approved"
        ),
        "needs_apply": sum(
            1
            for r in rows_out
            if not r.get("live_ok") and r.get("cells_to_update") and r.get("approval_status") == "Approved"
        ),
        "rows": rows_out,
        "sync_result": sync_result,
    }
    if json_out:
        print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    else:
        print(f"Offset -> sheet {SHEET_ID} | approved={out['approved_rows']} needs_apply={out['needs_apply']}\n")
        for r in rows_out:
            if r.get("pending") or r.get("approval_status") != "Approved":
                continue
            skip = r.get("skip") or "will_apply"
            drift_n = len(r.get("drift") or [])
            print(
                f"  {r.get('record_id','')[:14]} | {r.get('request_person')} <-> {r.get('exchange_person')} "
                f"| {r.get('original_date')} -> {r.get('exchange_date')} {r.get('shift_type')} "
                f"| {skip} drift={drift_n} state={r.get('in_state_file')}"
            )
        if sync_result:
            print(f"\nApply result: {sync_result}")
    return out


def scan_bitable_approved_offsets_for_shift_sheet() -> dict[str, int]:
    """Apply duty-sheet swaps for offsets approved directly in Base (not via bot card)."""
    global _OFFSET_SHIFT_SHEET_REENSURE_DONE
    invalidate_ose_bitable_cache()
    if not _OFFSET_SHIFT_SHEET_REENSURE_DONE:
        _OFFSET_SHIFT_SHEET_REENSURE_DONE = True
        try:
            stats = reensure_applied_offset_shift_sheet_styles_and_notes()
            print(
                f"[ose_Duty] offset re-ensure after restart: scanned={stats.get('scanned')} "
                f"styled={stats.get('styled')} noted={stats.get('noted')} "
                f"errors={stats.get('errors')}",
                flush=True,
            )
        except Exception as exc:
            print(f"[ose_Duty] offset re-ensure pass failed: {exc!r}", flush=True)
    items = (get_ose_offset_records_admin() or {}).get("items") or []
    applied = 0
    errors = 0
    for row in items:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("record_id") or "").strip()
        if not rid or bool(row.get("pending")):
            continue
        if str(row.get("approval_status") or "").strip().title() != "Approved":
            continue
        try:
            out = apply_approved_offset_shift_sheet_for_record(rid)
            if out.get("skipped") == "already_applied":
                continue
            if int(out.get("cells_updated") or 0) > 0:
                applied += 1
        except Exception as exc:
            errors += 1
            print(f"[ose_Duty] shift sheet apply failed for {rid!r}: {exc!r}", flush=True)
    return {"scanned": len(items), "applied": applied, "errors": errors}


def _fetch_ose_department_all_leave_records(token: str) -> list[dict[str, Any]]:
    """leave 全员 rows for dutyList OSE department (open_id + shift-sheet leave sync)."""
    table_id = (OSE_ALL_LEAVE_TABLE_ID or "").strip()
    if not table_id or table_id == LEAVEOSE_TABLE_ID_CANONICAL:
        return []
    items = _bitable_get_all_records(token, OSE_BASE_TOKEN, table_id)
    out: list[dict[str, Any]] = []
    for it in items:
        f = it.get("fields") or {}
        name = _title_name(_field_text(_get_field_by_aliases(f, ["Name", "Employee Name", "Person"])))
        if name and (_resolve_ose_roster_key(name) or _resolve_ose_leave_roster_key(name)):
            out.append(it)
    return out


def _ose_leave_items_for_shift_sheet(token: str) -> list[dict[str, Any]]:
    """Approved-leave sources: leaveose + OSE-filtered leave 全员 (deduped by record_id)."""
    items = list(_get_leave_display_raw(token))
    seen = {str(it.get("record_id") or "").strip() for it in items if str(it.get("record_id") or "").strip()}
    for it in _fetch_ose_department_all_leave_records(token):
        rid = str(it.get("record_id") or "").strip()
        if rid and rid not in seen:
            items.append(it)
            seen.add(rid)
    return items


def _leave_type_to_shift_code(leave_type: str) -> str:
    """Map leaveose ``Leave Type`` to shift-sheet code: AL, SL, HL, EL, else ``L``."""
    lt = (leave_type or "").strip().lower()
    if "annual" in lt or re.search(r"\bal\b", lt):
        return "AL"
    if "sick" in lt or re.search(r"\bsl\b", lt) or lt in ("mc", "medical", "medical leave"):
        return "SL"
    if "hospital" in lt or re.search(r"\bhl\b", lt):
        return "HL"
    if "emergency" in lt or re.search(r"\bel\b", lt):
        return "EL"
    return "L"


def _parse_ose_leave_bitable_item(it: dict[str, Any]) -> Optional[dict[str, Any]]:
    rid = str(it.get("record_id") or "").strip()
    if not rid:
        return None
    f = it.get("fields") or {}
    if not _leave_row_is_approved(f):
        return None
    roster_key = _resolve_ose_leave_roster_key(_leave_row_person_name(f))
    if not roster_key:
        return None
    st = _parse_date_value(_get_field_by_aliases(f, ["Start Date", "Leave Start Date", "From"]))
    ed = _parse_date_value(_get_field_by_aliases(f, ["End Date", "Leave End Date", "To"]))
    if not st or not ed:
        return None
    leave_type = _field_text(_get_field_by_aliases(f, ["Leave Type", "Type"])) or "Leave"
    shift_code = _leave_type_to_shift_code(leave_type)
    return {
        "record_id": rid,
        "person": roster_key,
        "start": st,
        "end": ed,
        "leave_type": leave_type,
        "shift_code": shift_code,
    }


def _parse_ose_leave_bitable_item_skip_reason(it: dict[str, Any]) -> str:
    """Why a leaveose row is ignored for shift-sheet ``L`` (for debugging)."""
    rid = str(it.get("record_id") or "").strip()
    if not rid:
        return "missing_record_id"
    f = it.get("fields") or {}
    if not _leave_row_is_approved(f):
        st = _field_text(_get_field_by_aliases(f, ["Status", "Approval Status"])).strip() or "(empty)"
        return f"status_not_approved:{st}"
    name = _leave_row_person_name(f)
    roster_key = _resolve_ose_leave_roster_key(name)
    if not roster_key:
        return f"not_leave_roster:{name or '(empty name)'}"
    st = _parse_date_value(_get_field_by_aliases(f, ["Start Date", "Leave Start Date", "From"]))
    ed = _parse_date_value(_get_field_by_aliases(f, ["End Date", "Leave End Date", "To"]))
    if not st or not ed:
        return "missing_start_or_end_date"
    return ""


def _load_leave_shift_sheet_state() -> dict[str, Any]:
    empty = {"record_ids": [], "by_record": {}, "edited_records": []}
    try:
        with open(_LEAVE_SHIFT_SHEET_APPLIED_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return dict(empty)
    except Exception:
        return dict(empty)
    if not isinstance(data, dict):
        return dict(empty)
    ids = [str(x).strip() for x in (data.get("record_ids") or []) if str(x).strip()]
    raw_by = data.get("by_record") if isinstance(data.get("by_record"), dict) else {}
    by_record = {str(k).strip(): dict(v) for k, v in raw_by.items() if str(k).strip() and isinstance(v, dict)}
    edited = [str(x).strip() for x in (data.get("edited_records") or []) if str(x).strip()]
    return {"record_ids": ids, "by_record": by_record, "edited_records": edited}


def _save_leave_shift_sheet_state(
    record_ids: set[str],
    by_record: dict[str, dict[str, Any]],
    *,
    edited_records: Optional[set[str]] = None,
) -> None:
    if edited_records is None:  # preserve whatever is on disk when caller doesn't touch it
        edited_records = set(_load_leave_shift_sheet_state().get("edited_records") or [])
    tmp = _LEAVE_SHIFT_SHEET_APPLIED_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "record_ids": sorted(record_ids),
                "by_record": {k: by_record[k] for k in sorted(by_record)},
                "edited_records": sorted(edited_records),
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )
        fh.write("\n")
    os.replace(tmp, _LEAVE_SHIFT_SHEET_APPLIED_PATH)


# Leave records that were EDITED after being written to the roster. Ops decision
# 2026-07-27: an edited leave is NOT marked AL/SL/L — its cells show the person's
# original D/N shift. Persisted so the periodic scan never re-marks them.
def leave_shift_sheet_edited_records() -> set[str]:
    return set(_load_leave_shift_sheet_state().get("edited_records") or [])


def _set_leave_edited_flag(record_id: str, *, edited: bool) -> None:
    rid = (record_id or "").strip()
    if not rid:
        return
    state = _load_leave_shift_sheet_state()
    flags = set(state.get("edited_records") or [])
    if (rid in flags) == edited:
        return
    flags.add(rid) if edited else flags.discard(rid)
    _save_leave_shift_sheet_state(
        set(state.get("record_ids") or []),
        dict(state.get("by_record") or {}),
        edited_records=flags,
    )


def _mark_leave_shift_sheet_applied(record_id: str, *, snapshot: dict[str, Any]) -> None:
    rid = (record_id or "").strip()
    if not rid:
        return
    state = _load_leave_shift_sheet_state()
    ids = set(state.get("record_ids") or [])
    by_record = dict(state.get("by_record") or {})
    ids.add(rid)
    by_record[rid] = snapshot
    _save_leave_shift_sheet_state(ids, by_record)


def _unmark_leave_shift_sheet_applied(record_id: str) -> None:
    rid = (record_id or "").strip()
    if not rid:
        return
    state = _load_leave_shift_sheet_state()
    ids = set(state.get("record_ids") or [])
    by_record = dict(state.get("by_record") or {})
    ids.discard(rid)
    by_record.pop(rid, None)
    _save_leave_shift_sheet_state(ids, by_record)


def _leave_shift_sheet_snapshots_match(a: dict[str, Any], b: dict[str, Any]) -> bool:
    return (
        str(a.get("person") or "") == str(b.get("person") or "")
        and str(a.get("start") or "") == str(b.get("start") or "")
        and str(a.get("end") or "") == str(b.get("end") or "")
        and str(a.get("shift_code") or "") == str(b.get("shift_code") or "")
        and list(a.get("cells") or []) == list(b.get("cells") or [])
    )


def _leave_request_identity(snap: dict[str, Any]) -> tuple[str, str, str, str, str]:
    """Identity of the leave REQUEST itself (never sheet-derived).

    Deliberately excludes ``cells``: each cell's ``prev`` is re-read from the live sheet
    on every poll, so comparing cells makes any third-party write (the offset sync, a
    manual D/N correction, a row insert) look like a user edit. Only these five fields
    change when someone actually edits the leave record.
    """
    return (
        str(snap.get("person") or "").strip().lower(),
        str(snap.get("start") or "").strip(),
        str(snap.get("end") or "").strip(),
        str(snap.get("leave_type") or "").strip().lower(),
        str(snap.get("shift_code") or "").strip().upper(),
    )


def _leave_request_was_edited(old_snap: dict[str, Any], new_snap: dict[str, Any]) -> bool:
    return bool(old_snap) and _leave_request_identity(old_snap) != _leave_request_identity(new_snap)


def _restore_original_shift_cells(
    token: str,
    snap: dict[str, Any],
    *,
    values: list[list[Any]],
    derive: bool = True,
) -> dict[str, Any]:
    """Put a snapshot's cells back to their original ``D``/``N``/``*``.

    Idempotent: a cell that no longer holds a leave code is left alone, so re-running
    issues no writes. When the stored ``prev`` is unusable (the old edit path saved
    ``""``) and ``derive`` is on, the original is inferred from the person's own row
    pattern; undecidable cells are reported instead of guessed.
    """
    updates: list[tuple[int, int, str]] = []
    col_dates: dict[int, date] = {}
    already_ok = 0
    unresolved: list[dict[str, Any]] = []
    for c in snap.get("cells") or []:
        try:
            row, col = int(c["row"]), int(c["col"])
        except (KeyError, TypeError, ValueError):
            continue
        current = _shift_sheet_cell_value(values, row, col)
        if current not in _OSE_SHIFT_SHEET_LEAVE_CODES:
            already_ok += 1  # nothing to undo (already a shift, or someone fixed it)
            continue
        on = _parse_date_value(c.get("date"))
        prev = str(c.get("prev") or "").strip().upper()
        confidence = "stored"
        if prev not in ("D", "N", "*"):
            prev, confidence = ("", "")
            if derive:
                prev, confidence = _derive_original_shift_from_pattern(values, row, col, on)
        if prev not in ("D", "N", "*"):
            unresolved.append({**c, "current": current})
            continue
        updates.append((row, col, prev))
        if on:
            col_dates[col] = on
        c["_restored_to"] = prev
        c["_confidence"] = confidence
    if updates:
        _put_ose_shift_sheet_cells(
            token, updates, values=values, col_dates=col_dates, sheet_id=SHEET_ID
        )
    return {
        "written": len(updates),
        "already_ok": already_ok,
        "unresolved": unresolved,
        "updates": updates,
    }


def _leave_shift_sheet_snapshot_from_plan(parsed: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "sheet_id": SHEET_ID,
        "person": parsed["person"],
        "start": parsed["start"].isoformat(),
        "end": parsed["end"].isoformat(),
        "leave_type": parsed.get("leave_type"),
        "shift_code": plan.get("shift_code"),
        "cells": list(plan.get("cells") or []),
    }


def _compute_leave_shift_sheet_plan(
    *,
    person: str,
    start_date: date,
    end_date: date,
    values: list[list[Any]],
    shift_code: str = "L",
    prior_cells: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """Mark roster cells as ``AL``/``SL``/``HL``/``EL``/``L`` for each leave day."""
    code = (shift_code or "L").strip().upper()
    if code not in _OSE_SHIFT_SHEET_LEAVE_CODES:
        code = _leave_type_to_shift_code(shift_code)
    prior_by_rc: dict[tuple[int, int], dict[str, Any]] = {}
    for c in prior_cells or []:
        try:
            prior_by_rc[(int(c["row"]), int(c["col"]))] = dict(c)
        except (KeyError, TypeError, ValueError):
            continue
    nm = _title_name(person)
    row = _sheet_row_index_for_person(values, nm, targets=OSE_LEAVE_ROSTER_KEYS)
    if row is None:
        raise ValueError(f"Could not find sheet row for {nm!r} on {SHEET_ID}")
    updates: list[tuple[int, int, str]] = []
    cells: list[dict[str, Any]] = []
    col_dates: dict[int, date] = {}
    d = start_date
    while d <= end_date:
        col = _date_column_for_matrix(values, d)
        if col is not None:
            row_data = values[row] if row < len(values) else []
            current = _field_text(row_data[col] if col < len(row_data) else "").upper()
            if current in ("D", "N", "*", ""):
                prev = current if current in ("D", "N", "*") else "*"
                updates.append((row, col, code))
                cells.append(
                    {"row": row, "col": col, "prev": prev, "date": d.isoformat(), "code": code}
                )
                col_dates[col] = d
            elif current in _OSE_SHIFT_SHEET_LEAVE_CODES:
                if current == code:
                    prior = prior_by_rc.get((row, col))
                    if prior:
                        cells.append(prior)
                else:
                    updates.append((row, col, code))
                    col_dates[col] = d
                    prior = prior_by_rc.get((row, col))
                    # Carry the ORIGINAL pre-leave value forward when the leave TYPE
                    # changes (AL→SL). ``*`` counts too: _revert_leave_shift_sheet_snapshot
                    # restores D/N/*, so dropping ``*`` here stranded the cell on a leave
                    # code permanently (the revert then skipped it forever).
                    if prior and str(prior.get("prev") or "").strip().upper() in ("D", "N", "*"):
                        cells.append({**prior, "code": code})
                    else:
                        cells.append(
                            {"row": row, "col": col, "prev": "", "date": d.isoformat(), "code": code}
                        )
        d += timedelta(days=1)
    return {
        "person": nm,
        "start": start_date,
        "end": end_date,
        "shift_code": code,
        "updates": updates,
        "cells": cells,
        "col_dates": col_dates,
    }


def _revert_leave_shift_sheet_snapshot(
    token: str,
    snapshot: dict[str, Any],
    *,
    values: list[list[Any]],
) -> None:
    cells = snapshot.get("cells") or []
    if not cells:
        return
    updates: list[tuple[int, int, str]] = []
    col_dates: dict[int, date] = {}
    for c in cells:
        prev = str(c.get("prev") or "").strip().upper()
        if prev not in ("D", "N", "*"):
            continue
        row_idx = int(c["row"])
        col_idx = int(c["col"])
        updates.append((row_idx, col_idx, prev))
        on = _parse_date_value(c.get("date"))
        if on:
            col_dates[col_idx] = on
    if updates:
        _put_ose_shift_sheet_cells(
            token, updates, values=values, col_dates=col_dates, sheet_id=SHEET_ID
        )


def apply_leave_to_shift_sheet(
    *, person: str, start_date: date, end_date: date, shift_code: str = "L"
) -> dict[str, Any]:
    """Write ``AL``/``SL``/``HL``/``EL``/``L`` on leave sheet days that were ``D``/``N`` for an approved OSE leave range."""
    if not _leave_shift_sheet_marking_enabled():
        return {
            "ok": True,
            "person": person,
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "cells_updated": 0,
            "skipped": "leave_marking_disabled",
        }
    values, err = _get_cached_ose_leave_sheet_values()
    if not values:
        raise RuntimeError(err or "Could not load OSE shift sheet")
    plan = _compute_leave_shift_sheet_plan(
        person=person,
        start_date=start_date,
        end_date=end_date,
        values=values,
        shift_code=shift_code,
    )
    if not plan["updates"]:
        return {
            "ok": True,
            "person": plan["person"],
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "cells_updated": 0,
            "skipped": "no_d_or_n_cells",
        }
    token = get_tenant_access_token()
    _put_ose_shift_sheet_cells(
        token, plan["updates"], values=values, col_dates=plan["col_dates"], sheet_id=SHEET_ID
    )
    return {
        "ok": True,
        "person": plan["person"],
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "cells_updated": len(plan["updates"]),
        "cells": plan["cells"],
    }


def _shift_sheet_cell_value(values: list[list[Any]], row: int, col: int) -> str:
    if row < 0 or col < 0 or row >= len(values):
        return ""
    row_data = values[row]
    if col >= len(row_data):
        return ""
    return _field_text(row_data[col]).strip().upper()


def _leave_shift_sheet_live_ok(
    values: list[list[Any]],
    snapshot: dict[str, Any],
    *,
    shift_code: str,
) -> bool:
    """True when every tracked cell on the sheet already shows the expected leave code."""
    cells = snapshot.get("cells") or []
    if not cells:
        return False
    code = str(snapshot.get("shift_code") or shift_code or "").strip().upper()
    for c in cells:
        expected = str(c.get("code") or code).strip().upper()
        if not expected:
            continue
        cur = _shift_sheet_cell_value(values, int(c["row"]), int(c["col"]))
        if cur != expected:
            return False
    return True


def apply_leave_shift_sheet_for_record(
    record_id: str,
    *,
    leave_item: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    if not _leave_shift_sheet_marking_enabled():
        # Roster never shows leave codes — the sweep in
        # scan_bitable_approved_leave_for_shift_sheet keeps the shift in place.
        return {
            "ok": True,
            "record_id": rid,
            "cells_updated": 0,
            "skipped": "leave_marking_disabled",
        }
    if leave_item is None:
        token = get_tenant_access_token()
        leave_item = None
        for it in _ose_leave_items_for_shift_sheet(token):
            if str(it.get("record_id") or "").strip() == rid:
                leave_item = it
                break
        if leave_item is None:
            raise KeyError(f"leave record {rid!r} not found")
    parsed = _parse_ose_leave_bitable_item(leave_item)
    if not parsed:
        raise ValueError(f"leave record {rid!r} is not approved OSE leave")
    state = _load_leave_shift_sheet_state()
    old_snap = dict((state.get("by_record") or {}).get(rid) or {})
    if str(old_snap.get("sheet_id") or "") not in ("", SHEET_ID, LEAVE_SHEET_ID):
        old_snap = {}
    elif str(old_snap.get("sheet_id") or "") in ("3RIBRL", "65p5cn"):
        old_snap = {}
    prior_cells = list(old_snap.get("cells") or [])
    values, err = _get_cached_ose_leave_sheet_values()
    if not values:
        raise RuntimeError(err or f"Could not load OSE sheet {SHEET_ID!r}")

    # Known edited leave → keep the original D/N shift; never re-mark AL/SL.
    # _restore_original_shift_cells is idempotent, so this writes only if the cell
    # somehow shows a leave code again.
    if rid in leave_shift_sheet_edited_records():
        restored = 0
        if old_snap:
            out = _restore_original_shift_cells(
                get_tenant_access_token(), old_snap, values=values
            )
            restored = out["written"]
            if not out["unresolved"]:
                _unmark_leave_shift_sheet_applied(rid)
        return {
            "ok": True,
            "record_id": rid,
            "person": parsed["person"],
            "cells_updated": 0,
            "restored_cells": restored,
            "skipped": "edited_keep_original_shift",
        }

    shift_code = str(parsed.get("shift_code") or "L")
    plan = _compute_leave_shift_sheet_plan(
        person=parsed["person"],
        start_date=parsed["start"],
        end_date=parsed["end"],
        values=values,
        shift_code=shift_code,
        prior_cells=prior_cells,
    )
    new_snap = _leave_shift_sheet_snapshot_from_plan(parsed, plan)
    if (
        old_snap
        and _leave_shift_sheet_snapshots_match(old_snap, new_snap)
        and not plan["updates"]
        and _leave_shift_sheet_live_ok(values, new_snap, shift_code=shift_code)
    ):
        return {
            "ok": True,
            "record_id": rid,
            "person": parsed["person"],
            "cells_updated": 0,
            "skipped": "already_applied",
        }
    token = get_tenant_access_token()
    if _leave_request_was_edited(old_snap, new_snap):
        # The leave REQUEST changed (dates / type / person) → per ops policy the roster
        # shows the ORIGINAL D/N shift, not AL/SL. Keyed off the bitable record only, so
        # another writer touching the cell can never be mistaken for an edit.
        out = _restore_original_shift_cells(token, old_snap, values=values)
        _set_leave_edited_flag(rid, edited=True)
        if out["unresolved"]:
            # Original unknown for some cells — KEEP the snapshot so
            # ``--fix-stranded-leave`` can still find them; don't pretend it's clean.
            return {
                "ok": True,
                "record_id": rid,
                "person": parsed["person"],
                "cells_updated": 0,
                "restored_cells": out["written"],
                "unresolved_cells": len(out["unresolved"]),
                "skipped": "edited_restored_original_shift_partial",
            }
        _unmark_leave_shift_sheet_applied(rid)
        return {
            "ok": True,
            "record_id": rid,
            "person": parsed["person"],
            "cells_updated": 0,
            "restored_cells": out["written"],
            "skipped": "edited_restored_original_shift",
        }
    if (
        old_snap
        and not _leave_shift_sheet_snapshots_match(old_snap, new_snap)
    ):
        # Same request, but the cells drifted (offset sync, manual fix, row shift).
        # Unchanged self-healing behaviour: revert what we wrote, then re-apply.
        _revert_leave_shift_sheet_snapshot(token, old_snap, values=values)
        values, err = _get_cached_ose_leave_sheet_values()
        if not values:
            raise RuntimeError(err or f"Could not reload OSE leave sheet after revert")
        plan = _compute_leave_shift_sheet_plan(
            person=parsed["person"],
            start_date=parsed["start"],
            end_date=parsed["end"],
            values=values,
            shift_code=shift_code,
        )
        new_snap = _leave_shift_sheet_snapshot_from_plan(parsed, plan)
    elif (
        old_snap
        and _leave_shift_sheet_snapshots_match(old_snap, new_snap)
        and not plan["updates"]
        and not _leave_shift_sheet_live_ok(values, new_snap, shift_code=shift_code)
    ):
        # State file says applied but sheet drifted (e.g. manual edit or failed write).
        plan = _compute_leave_shift_sheet_plan(
            person=parsed["person"],
            start_date=parsed["start"],
            end_date=parsed["end"],
            values=values,
            shift_code=shift_code,
        )
        new_snap = _leave_shift_sheet_snapshot_from_plan(parsed, plan)
    if not plan["updates"]:
        if old_snap:
            _unmark_leave_shift_sheet_applied(rid)
        return {
            "ok": True,
            "record_id": rid,
            "person": parsed["person"],
            "cells_updated": 0,
            "skipped": "no_d_or_n_cells",
        }
    _put_ose_shift_sheet_cells(
        token, plan["updates"], values=values, col_dates=plan["col_dates"], sheet_id=SHEET_ID
    )
    _mark_leave_shift_sheet_applied(rid, snapshot=new_snap)
    return {
        "ok": True,
        "record_id": rid,
        "person": parsed["person"],
        "start": parsed["start"].isoformat(),
        "end": parsed["end"].isoformat(),
        "shift_code": shift_code,
        "cells_updated": len(plan["updates"]),
    }


def revert_leave_shift_sheet_for_record(record_id: str) -> dict[str, Any]:
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    state = _load_leave_shift_sheet_state()
    snapshot = dict((state.get("by_record") or {}).get(rid) or {})
    if not snapshot.get("cells"):
        _unmark_leave_shift_sheet_applied(rid)
        return {"ok": True, "record_id": rid, "skipped": "not_applied"}
    values, err = _get_cached_ose_leave_sheet_values()
    if not values:
        raise RuntimeError(err or f"Could not load OSE sheet {SHEET_ID!r}")
    token = get_tenant_access_token()
    _revert_leave_shift_sheet_snapshot(token, snapshot, values=values)
    _unmark_leave_shift_sheet_applied(rid)
    return {"ok": True, "record_id": rid, "reverted": True, "cells": len(snapshot.get("cells") or [])}


_LEAVE_SHIFT_SHEET_REENSURE_DONE = False


def reensure_applied_leave_shift_sheet_styles() -> dict[str, int]:
    """Re-apply ``L`` cell backgrounds (incl. holiday green) for tracked leave rows."""
    state = _load_leave_shift_sheet_state()
    applied_ids = list(state.get("record_ids") or [])
    if not applied_ids:
        return {"scanned": 0, "styled": 0, "errors": 0}
    values, err = _get_cached_ose_leave_sheet_values()
    if not values:
        print(f"[ose_Duty] leave re-ensure skipped (no sheet): {err!r}", flush=True)
        return {"scanned": len(applied_ids), "styled": 0, "errors": 1}
    token = get_tenant_access_token()
    styled = 0
    errors = 0
    by_record = dict(state.get("by_record") or {})
    for rid in applied_ids:
        try:
            snap = dict(by_record.get(rid) or {})
            person = str(snap.get("person") or "")
            st = _parse_date_value(snap.get("start"))
            ed = _parse_date_value(snap.get("end"))
            if not person or not st or not ed:
                continue
            plan = _compute_leave_shift_sheet_plan(
                person=person,
                start_date=st,
                end_date=ed,
                values=values,
                shift_code=str(snap.get("shift_code") or "L"),
                prior_cells=list(snap.get("cells") or []),
            )
            if not plan["updates"]:
                continue
            _put_ose_shift_sheet_cell_styles(
                token,
                plan["updates"],
                values=values,
                col_dates=plan.get("col_dates"),
                sheet_id=SHEET_ID,
            )
            styled += 1
        except Exception as exc:
            errors += 1
            print(f"[ose_Duty] leave re-ensure failed for {rid!r}: {exc!r}", flush=True)
    return {"scanned": len(applied_ids), "styled": styled, "errors": errors}


def scan_revert_deleted_leave_from_shift_sheet() -> dict[str, int]:
    """Restore ``D``/``N`` when a tracked leave row is removed or no longer approved."""
    state = _load_leave_shift_sheet_state()
    applied_ids = set(state.get("record_ids") or [])
    edited_ids = set(state.get("edited_records") or [])
    if not applied_ids and not edited_ids:
        return {"scanned": 0, "reverted": 0, "errors": 0}
    invalidate_ose_bitable_cache()
    token = get_tenant_access_token()
    items = _ose_leave_items_for_shift_sheet(token)
    approved_ids = set()
    for it in items:
        parsed = _parse_ose_leave_bitable_item(it)
        if parsed:
            approved_ids.add(parsed["record_id"])
    # Only drop an edited flag once the record is really gone from the source. A single
    # empty/failed fetch must not clear flags (that would let the next scan re-mark
    # AL/SL), so require a non-empty item list first.
    if items:
        for rid in sorted(edited_ids - approved_ids):
            _set_leave_edited_flag(rid, edited=False)
    reverted = 0
    errors = 0
    for rid in sorted(applied_ids):
        if rid in approved_ids:
            continue
        try:
            out = revert_leave_shift_sheet_for_record(rid)
            if out.get("reverted"):
                reverted += 1
        except Exception as exc:
            errors += 1
            print(f"[ose_Duty] leave shift sheet revert failed for {rid!r}: {exc!r}", flush=True)
    return {"scanned": len(applied_ids), "reverted": reverted, "errors": errors}


def scan_bitable_approved_leave_for_shift_sheet() -> dict[str, int]:
    """Apply ``L`` on OSE shift sheet for approved leave from leaveose + OSE leave 全员.

    With marking disabled (the default since 2026-07-27) this instead SWEEPS any leave
    code off the roster back to the person's ``D``/``N`` shift. Nothing re-adds a code,
    so the sweep converges: once clean it writes nothing on later runs.
    """
    global _LEAVE_SHIFT_SHEET_REENSURE_DONE
    if not _leave_shift_sheet_marking_enabled():
        try:
            res = scan_sheet_leave_codes_to_original_shift(apply=True)
        except Exception as exc:
            print(f"[ose_Duty] leave sweep failed: {exc!r}", flush=True)
            return {"scanned": 0, "applied": 0, "restyled": 0, "errors": 1}
        if res.get("written") or res.get("undecidable"):
            print(
                f"[ose_Duty] leave sweep: {res.get('found')} leave cell(s) found, "
                f"{res.get('written')} restored to D/N, "
                f"{res.get('undecidable')} undecidable (left for manual fix)",
                flush=True,
            )
        return {
            "scanned": int(res.get("found") or 0),
            "applied": int(res.get("written") or 0),
            "restyled": 0,
            "errors": int(res.get("undecidable") or 0),
        }
    invalidate_ose_bitable_cache()
    if not _LEAVE_SHIFT_SHEET_REENSURE_DONE:
        _LEAVE_SHIFT_SHEET_REENSURE_DONE = True
        try:
            stats = reensure_applied_leave_shift_sheet_styles()
            print(
                f"[ose_Duty] leave re-ensure after restart: scanned={stats.get('scanned')} "
                f"styled={stats.get('styled')} errors={stats.get('errors')}",
                flush=True,
            )
        except Exception as exc:
            print(f"[ose_Duty] leave re-ensure pass failed: {exc!r}", flush=True)
    token = get_tenant_access_token()
    items = _ose_leave_items_for_shift_sheet(token)
    applied = 0
    restyled = 0
    errors = 0
    for it in items:
        parsed = _parse_ose_leave_bitable_item(it)
        if not parsed:
            continue
        rid = parsed["record_id"]
        try:
            out = apply_leave_shift_sheet_for_record(rid, leave_item=it)
            if int(out.get("cells_updated") or 0) > 0:
                applied += 1
            elif out.get("restyled"):
                restyled += 1
        except Exception as exc:
            errors += 1
            print(f"[ose_Duty] leave shift sheet apply failed for {rid!r}: {exc!r}", flush=True)
    return {"scanned": len(items), "applied": applied, "restyled": restyled, "errors": errors}


# Codes the roster must never keep — swept back to the person's D/N shift.
# Plain ``L`` is deliberately EXCLUDED (ops, 2026-07-27): it stays on the roster.
_OSE_LEAVE_CODES_SWEPT = frozenset({"AL", "SL", "HL", "EL"})


def _leave_shift_sheet_marking_enabled() -> bool:
    """Whether approved leave is written onto the OSE roster as ``AL``/``SL``/``L``/…

    Ops decision 2026-07-27: **no**. Every roster day shows the person's ``D``/``N``
    shift; leave lives in the Bitable/HRMS only. The periodic scan sweeps any leave code
    it finds back to the shift, so an edited leave (or a hand-typed code) self-corrects.
    Set ``OSE_LEAVE_MARK_SHIFT_SHEET=1`` to restore the old marking behaviour.
    """
    return (os.getenv("OSE_LEAVE_MARK_SHIFT_SHEET") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _derive_original_shift_from_pattern(
    values: list[list[Any]], row: int, col: int, on: Optional[date] = None
) -> tuple[str, str]:
    """Infer a cell's original shift from the person's PREDOMINANT shift that month.

    OSE staff work a whole month on the same shift, so "what is this person mostly on
    in this month" is the reliable signal (per ops, 2026-07-27): count every ``D`` and
    ``N`` in their row across that month and take the majority.

    ``high`` = one shift only (or ≥70% of counted days), ``medium`` = a plain majority,
    ``("", "")`` = undecidable (tie, or no D/N in the month) → reported, never guessed.
    Never returns ``*``: an offset original cannot be inferred.
    """
    if on is None:
        return "", ""
    counts = {"D": 0, "N": 0}
    d = on.replace(day=1)
    while d.month == on.month:
        c = _date_column_for_matrix(values, d)
        if c is not None and c != col:
            val = _shift_sheet_cell_value(values, row, c)
            if val in ("D", "N"):
                counts[val] += 1
        d += timedelta(days=1)
    total = counts["D"] + counts["N"]
    if not total:
        return "", ""
    win, lose = ("D", "N") if counts["D"] >= counts["N"] else ("N", "D")
    if counts[win] == counts[lose]:
        return "", ""  # genuine tie — a half-month rotation; let a human decide
    share = counts[win] / total
    return win, ("high" if (counts[lose] == 0 or share >= 0.7) else "medium")


def _leave_snapshot_on_current_sheet(snap: dict[str, Any]) -> bool:
    """Same tab guard the apply path uses — a legacy-tab snapshot's row/col would
    otherwise be mapped onto THIS tab and hit the wrong person's row."""
    sid = str(snap.get("sheet_id") or "")
    if sid in ("3RIBRL", "65p5cn"):
        return False
    return sid in ("", SHEET_ID, LEAVE_SHEET_ID)


def find_leave_records_needing_restore(
    *, include_all: bool = False
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Tracked leave records whose roster cells should show the original shift.

    Auto scope (``include_all=False``): records already flagged edited, plus records with
    a cell stuck on a leave code whose stored original is missing — the fingerprint of
    the pre-fix edit path, which saved ``prev=""`` and could never be reverted.

    ``include_all=True`` returns EVERY tracked record that currently shows a leave code,
    including correctly-marked approved leave. Use only with explicit targeting
    (``--record`` / ``--person``), because restoring a valid leave removes it from the
    roster. Also returns counters so a "0 records" result is self-explaining.
    """
    values, err = _get_cached_ose_leave_sheet_values()
    if not values:
        raise RuntimeError(err or f"Could not load OSE sheet {SHEET_ID!r}")
    state = _load_leave_shift_sheet_state()
    flagged = set(state.get("edited_records") or [])
    by_record = state.get("by_record") or {}
    stats = {
        "tracked_records": len(by_record),
        "flagged_edited": len(flagged),
        "skipped_other_sheet": 0,
        "cells_on_leave_code": 0,
        "skipped_valid_leave": 0,
    }
    out: list[dict[str, Any]] = []
    for rid, snap in sorted(by_record.items()):
        if not _leave_snapshot_on_current_sheet(snap):
            stats["skipped_other_sheet"] += 1
            continue
        cells: list[dict[str, Any]] = []
        lost = False
        for c in snap.get("cells") or []:
            try:
                row, col = int(c["row"]), int(c["col"])
            except (KeyError, TypeError, ValueError):
                continue
            current = _shift_sheet_cell_value(values, row, col)
            if current not in _OSE_SHIFT_SHEET_LEAVE_CODES:
                continue  # already a shift — nothing to restore
            stats["cells_on_leave_code"] += 1
            prev = str(c.get("prev") or "").strip().upper()
            if prev in ("D", "N", "*"):
                target, confidence = prev, "stored"
            else:
                lost = True
                target, confidence = _derive_original_shift_from_pattern(
                    values, row, col, _parse_date_value(c.get("date"))
                )
            cells.append(
                {
                    "date": c.get("date") or "",
                    "row": row,
                    "col": col,
                    "current": current,
                    "restore_to": target,
                    "confidence": confidence,
                }
            )
        if not cells:
            continue
        if rid not in flagged and not lost and not include_all:
            # A normal, correctly-marked approved leave — never touched automatically.
            stats["skipped_valid_leave"] += 1
            continue
        out.append(
            {
                "record_id": rid,
                "person": snap.get("person") or "",
                "leave_type": snap.get("leave_type") or "",
                "flagged_edited": rid in flagged,
                "lost_original": lost,
                "cells": cells,
            }
        )
    return out, stats


def restore_leave_cells_to_original_shift(
    *,
    apply: bool = False,
    json_out: bool = False,
    record_id: str | None = None,
    person: str | None = None,
    include_all: bool = False,
) -> dict[str, Any]:
    """Report (default) or restore leave-coded cells back to the original ``D``/``N``.

    DRY RUN unless ``apply=True``. Only cells with a stored original or a clear
    month-majority match are written; undecidable ones are listed for manual fixing.
    Restored records are flagged edited and their snapshot dropped, so the periodic scan
    neither re-marks them nor treats them as still applied.

    ``record_id`` / ``person`` target specific leave explicitly and therefore widen the
    scope to every tracked cell for that target (including a currently-valid leave —
    that is the point when you want it off the roster). ``include_all`` lists everything.
    """
    targeted = bool((record_id or "").strip() or (person or "").strip())
    records, stats = find_leave_records_needing_restore(
        include_all=include_all or targeted
    )
    if record_id:
        rid_want = record_id.strip()
        records = [r for r in records if r["record_id"] == rid_want]
    if person:
        want = _title_name(person).strip().lower()
        records = [r for r in records if _title_name(r["person"]).strip().lower() == want]
    total_cells = sum(len(r["cells"]) for r in records)
    writable = [
        (r, [c for c in r["cells"] if c["restore_to"]]) for r in records
    ]
    unresolved = sum(1 for r in records for c in r["cells"] if not c["restore_to"])
    result: dict[str, Any] = {
        "records": len(records),
        "cells": total_cells,
        "restorable": sum(len(cs) for _r, cs in writable),
        "unresolved": unresolved,
        "written": 0,
        "dry_run": not apply,
        "targeted": targeted,
        "stats": stats,
        "detail": records,
    }
    if apply and result["restorable"]:
        values, err = _get_cached_ose_leave_sheet_values()
        if not values:
            raise RuntimeError(err or "Could not load OSE shift sheet")
        token = get_tenant_access_token()
        for rec, cs in writable:
            if not cs:
                continue
            updates = [(c["row"], c["col"], c["restore_to"]) for c in cs]
            col_dates: dict[int, date] = {}
            for c in cs:
                on = _parse_date_value(c.get("date"))
                if on:
                    col_dates[c["col"]] = on
            _put_ose_shift_sheet_cells(
                token, updates, values=values, col_dates=col_dates, sheet_id=SHEET_ID
            )
            result["written"] += len(updates)
            # Never let the next scan re-mark or re-revert what we just restored.
            _set_leave_edited_flag(rec["record_id"], edited=True)
            if len(cs) == len(rec["cells"]):
                _unmark_leave_shift_sheet_applied(rec["record_id"])
    if json_out:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        mode = "APPLY" if apply else "DRY RUN"
        print(
            f"[ose_Duty] leave cells -> original shift ({mode}): "
            f"{len(records)} record(s), {total_cells} cell(s), "
            f"{result['restorable']} restorable, {unresolved} undecidable, "
            f"{result['written']} written",
            flush=True,
        )
        # Explain a 0-record result instead of leaving you guessing.
        print(
            f"   scanned {stats['tracked_records']} tracked leave record(s); "
            f"{stats['cells_on_leave_code']} cell(s) currently show a leave code; "
            f"{stats['flagged_edited']} flagged edited; "
            f"{stats['skipped_valid_leave']} skipped as valid leave; "
            f"{stats['skipped_other_sheet']} on another tab",
            flush=True,
        )
        if not records and stats["skipped_valid_leave"]:
            print(
                "   → nothing matched automatically. Those cells belong to leave that is "
                "still approved and correctly marked. Target them explicitly with "
                "--person \"Name\" or --record recXXXX (or --list to see them all).",
                flush=True,
            )
        for r in records:
            tag = " [edited]" if r["flagged_edited"] else (" [lost original]" if r.get("lost_original") else " [valid leave — explicit target]")
            print(f"  {r['person']} ({r['leave_type']}){tag}", flush=True)
            for c in r["cells"]:
                to = f"{c['restore_to']} ({c['confidence']})" if c["restore_to"] else "?? undecidable — fix by hand"
                print(f"     {c['date']:<12} {c['current']:>3} -> {to}", flush=True)
    return result


def scan_sheet_leave_codes_to_original_shift(
    *,
    months: Optional[list[tuple[int, int]]] = None,
    apply: bool = False,
    json_out: bool = False,
    person: str | None = None,
    codes: Optional[set[str]] = None,
) -> dict[str, Any]:
    """Find ``AL``/``SL``/``L``/``HL``/``EL`` cells ON THE SHEET and restore the shift.

    State-free: reads the roster directly, so it also finds codes the bot never tracked
    (hand-typed, or written before the state file was reset). The original shift comes
    from the person's PREDOMINANT shift that month; ties are reported, never guessed.

    DRY RUN unless ``apply=True``. This does not consult the leave Bitable, so it will
    also list leave that is currently legitimate — read the list before applying, or
    narrow it with ``person``.
    """
    values, err = _get_cached_ose_leave_sheet_values()
    if not values:
        raise RuntimeError(err or f"Could not load OSE sheet {SHEET_ID!r}")
    if not months:
        today = datetime.now(_display_tz()).date() if "_display_tz" in globals() else date.today()
        nxt = (today.replace(day=1) + timedelta(days=31)).replace(day=1)
        months = [(today.year, today.month), (nxt.year, nxt.month)]
    want_person = _title_name(person).strip().lower() if person else ""
    # Default = AL/SL/HL/EL only; plain ``L`` stays on the roster unless asked for
    # explicitly via ``codes`` (``--codes L``).
    want_codes = {c.strip().upper() for c in (codes or set()) if c.strip()} or set(
        _OSE_LEAVE_CODES_SWEPT
    )

    found: list[dict[str, Any]] = []
    rows_checked = 0
    for name in OSE_LEAVE_ROSTER_KEYS:
        nm = _title_name(name)
        if want_person and nm.strip().lower() != want_person:
            continue
        row = _sheet_row_index_for_person(values, nm, targets=OSE_LEAVE_ROSTER_KEYS)
        if row is None:
            continue
        rows_checked += 1
        for (yy, mm) in months:
            d = date(yy, mm, 1)
            while d.month == mm:
                col = _date_column_for_matrix(values, d)
                if col is not None:
                    cur = _shift_sheet_cell_value(values, row, col)
                    if cur in want_codes:
                        shift, confidence = _derive_original_shift_from_pattern(
                            values, row, col, d
                        )
                        found.append(
                            {
                                "person": nm,
                                "date": d.isoformat(),
                                "row": row,
                                "col": col,
                                "current": cur,
                                "restore_to": shift,
                                "confidence": confidence,
                            }
                        )
                d += timedelta(days=1)

    writable = [f for f in found if f["restore_to"]]
    result = {
        "rows_checked": rows_checked,
        "months": [f"{y}-{m:02d}" for y, m in months],
        "found": len(found),
        "restorable": len(writable),
        "undecidable": len(found) - len(writable),
        "written": 0,
        "dry_run": not apply,
        "cells": found,
    }
    if apply and writable:
        token = get_tenant_access_token()
        updates = [(f["row"], f["col"], f["restore_to"]) for f in writable]
        col_dates: dict[int, date] = {}
        for f in writable:
            on = _parse_date_value(f["date"])
            if on:
                col_dates[f["col"]] = on
        _put_ose_shift_sheet_cells(
            token, updates, values=values, col_dates=col_dates, sheet_id=SHEET_ID
        )
        result["written"] = len(updates)
    if json_out:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        mode = "APPLY" if apply else "DRY RUN"
        print(
            f"[ose_Duty] SHEET scan for leave codes ({mode}) on {SHEET_ID} "
            f"months={','.join(result['months'])}: {rows_checked} roster row(s), "
            f"{len(found)} leave cell(s), {len(writable)} restorable, "
            f"{result['undecidable']} undecidable, {result['written']} written",
            flush=True,
        )
        for f in found:
            to = (
                f"{f['restore_to']} ({f['confidence']})"
                if f["restore_to"]
                else "?? undecidable — fix by hand"
            )
            print(f"   {f['person']:<22} {f['date']:<12} {f['current']:>3} -> {to}", flush=True)
    return result


def suppress_leave_marking(
    *,
    person: str | None = None,
    record_id: str | None = None,
    month: Optional[tuple[int, int]] = None,
    apply: bool = False,
    unsuppress: bool = False,
    json_out: bool = False,
) -> dict[str, Any]:
    """Flag approved leave records so the roster keeps the ORIGINAL ``D``/``N`` shift.

    Restoring a cell with ``--scan-sheet-leave`` only fixes the sheet; the periodic
    ``scan_bitable_approved_leave_for_shift_sheet`` would re-mark it while the leave is
    still approved. Flagging the record makes ``apply_leave_shift_sheet_for_record``
    return early forever, so the shift stays put.

    DRY RUN unless ``apply=True``. ``unsuppress=True`` removes the flag again.
    """
    token = get_tenant_access_token()
    items = _ose_leave_items_for_shift_sheet(token)
    want_person = _title_name(person).strip().lower() if person else ""
    want_rid = (record_id or "").strip()
    flagged_now = leave_shift_sheet_edited_records()

    matched: list[dict[str, Any]] = []
    for it in items:
        parsed = _parse_ose_leave_bitable_item(it)
        if not parsed:
            continue
        rid = parsed["record_id"]
        nm = _title_name(parsed.get("person") or "")
        if want_rid and rid != want_rid:
            continue
        if want_person and nm.strip().lower() != want_person:
            continue
        if month:
            if not _overlaps_month_dates(parsed["start"], parsed["end"], month[0], month[1]):
                continue
        matched.append(
            {
                "record_id": rid,
                "person": nm,
                "start": parsed["start"].isoformat(),
                "end": parsed["end"].isoformat(),
                "leave_type": parsed.get("leave_type") or "",
                "already_flagged": rid in flagged_now,
            }
        )

    changed = 0
    if apply:
        for m in matched:
            if m["already_flagged"] == (not unsuppress):
                continue
            _set_leave_edited_flag(m["record_id"], edited=not unsuppress)
            changed += 1
    result = {
        "matched": len(matched),
        "changed": changed,
        "dry_run": not apply,
        "unsuppress": unsuppress,
        "records": matched,
    }
    if json_out:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        verb = "UN-SUPPRESS" if unsuppress else "SUPPRESS"
        mode = "APPLY" if apply else "DRY RUN"
        print(
            f"[ose_Duty] {verb} leave marking ({mode}): {len(matched)} record(s) matched, "
            f"{changed} flag(s) changed",
            flush=True,
        )
        for m in matched:
            state = "already suppressed" if m["already_flagged"] else "marked as leave"
            print(
                f"   {m['person']:<22} {m['start']} → {m['end']} "
                f"{m['leave_type']:<14} [{state}] {m['record_id']}",
                flush=True,
            )
        if not apply and matched:
            print("   (add --apply to write the flags)", flush=True)
    return result


def _overlaps_month_dates(start: date, end: date, year: int, month: int) -> bool:
    first = date(year, month, 1)
    last = (first + timedelta(days=31)).replace(day=1) - timedelta(days=1)
    return not (end < first or start > last)


def probe_leave_shift_sheet_sync(*, apply: bool = False, json_out: bool = False) -> dict[str, Any]:
    """
    Debug leaveose → OSE shift sheet ``L`` sync.

    Reads base ``OSE_BASE_TOKEN`` table ``tblvoXE0hsPjgb0j`` (leaveose) plus OSE rows from leave 全员.
    """
    token = get_tenant_access_token()
    leaveose = _fetch_leaveose_bitable_records(token)
    all_items = _ose_leave_items_for_shift_sheet(token)
    values, sheet_err = _get_cached_ose_leave_sheet_values()
    rows: list[dict[str, Any]] = []
    for it in all_items:
        f = it.get("fields") or {}
        rid = str(it.get("record_id") or "").strip()
        name = _leave_row_person_name(f)
        parsed = _parse_ose_leave_bitable_item(it)
        skip = _parse_ose_leave_bitable_item_skip_reason(it)
        plan_info: dict[str, Any] = {}
        if parsed and values:
            try:
                plan = _compute_leave_shift_sheet_plan(
                    person=parsed["person"],
                    start_date=parsed["start"],
                    end_date=parsed["end"],
                    values=values,
                    shift_code=str(parsed.get("shift_code") or "L"),
                    prior_cells=list(
                        (_load_leave_shift_sheet_state().get("by_record") or {})
                        .get(rid, {})
                        .get("cells")
                        or []
                    ),
                )
                plan_info = {
                    "shift_code": plan.get("shift_code"),
                    "leave_type": parsed.get("leave_type"),
                    "cells_to_mark": len(plan.get("updates") or []),
                    "cell_details": plan.get("cells") or [],
                }
                if not plan.get("updates"):
                    skip = skip or "no_D_or_N_on_sheet_for_leave_dates"
            except Exception as exc:
                plan_info = {"error": str(exc)}
                skip = skip or f"plan_error:{exc}"
        elif not values:
            skip = skip or f"sheet_unavailable:{sheet_err}"
        rows.append(
            {
                "record_id": rid,
                "name": name,
                "roster_key": parsed["person"] if parsed else _resolve_ose_leave_roster_key(name),
                "leave_type": parsed.get("leave_type") if parsed else None,
                "shift_code": parsed.get("shift_code") if parsed else None,
                "start": parsed["start"].isoformat() if parsed else None,
                "end": parsed["end"].isoformat() if parsed else None,
                "skip": skip or None,
                "plan": plan_info,
                "live_ok": (
                    _leave_shift_sheet_live_ok(
                        values,
                        {
                            "shift_code": parsed.get("shift_code"),
                            "cells": plan_info.get("cell_details") or [],
                        },
                        shift_code=str(parsed.get("shift_code") or "L"),
                    )
                    if parsed and values and plan_info.get("cell_details")
                    else None
                ),
            }
        )
    sync_result: Optional[dict[str, Any]] = None
    if apply:
        sync_result = scan_bitable_approved_leave_for_shift_sheet()
    out = {
        "ok": bool(values) and any(r.get("plan", {}).get("cells_to_mark") for r in rows),
        "leaveose_url_table": LEAVEOSE_TABLE_ID_CANONICAL,
        "base_token": OSE_BASE_TOKEN,
        "shift_spreadsheet": SPREADSHEET_TOKEN,
        "shift_sheet_id": SHEET_ID,
        "leave_sheet_id": LEAVE_SHEET_ID,
        "leaveose_rows": len(leaveose),
        "combined_rows": len(all_items),
        "sheet_ok": bool(values),
        "sheet_error": sheet_err,
        "rows": rows,
        "sync_result": sync_result,
    }
    if json_out:
        print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    else:
        print(
            f"Leave → shift sheet probe\n"
            f"  leaveose: {LEAVEOSE_TABLE_ID_CANONICAL} @ {OSE_BASE_TOKEN}\n"
            f"  shift sheet: {SPREADSHEET_TOKEN} / {SHEET_ID}\n"
            f"  leaveose rows: {len(leaveose)} | eligible checks: {len(rows)} | sheet OK: {bool(values)}\n"
        )
        if sheet_err:
            print(f"  sheet error: {sheet_err}\n")
        for r in rows:
            label = ose_roster_sheet_label(r["roster_key"]) if r.get("roster_key") else r.get("name")
            if r.get("skip"):
                print(f"  SKIP  {label}  {r.get('start')}..{r.get('end')}  → {r['skip']}")
            else:
                code = (r.get("plan") or {}).get("shift_code") or r.get("shift_code") or "?"
                n = (r.get("plan") or {}).get("cells_to_mark", 0)
                lt = r.get("leave_type") or ""
                live = r.get("live_ok")
                live_note = "" if live is None else (" sheet OK" if live else " sheet NOT updated yet")
                rid = r.get("record_id") or ""
                print(
                    f"  OK    {label}  {r.get('start')}..{r.get('end')}  "
                    f"→ {n} cell(s) D/N→{code} ({lt}){live_note}  [{rid[:12]}…]"
                )
        if apply and sync_result:
            print(f"\nApply: {sync_result}")
    return out


def get_shift_names_for_date(target_date: date) -> tuple[list[str], list[str]]:
    """Return (morning_names, night_names) from OSE shift sheet."""
    values, _err = _get_cached_ose_sheet_values()
    if not values:
        return [], []
    return _shift_names_from_matrix(values, target_date)


def get_ose_month_calendar(year: int, month: int) -> dict[str, Any]:
    """
    Build a month grid for the web dashboard: each day has morning (``D``) and night (``N``) from the OSE sheet,
    with the same leave filtering as ``get_ose_payload_for_date`` (mode ``date``).

    Each day cell also includes ``leave`` (list of serializable dicts) and ``offset`` (list of strings),
    using the same Bitable helpers as the Lark card — without changing those helpers.

    Returns keys: ``ok``, ``year``, ``month``, ``month_label``, ``weeks`` (Mon-first rows),
    optional ``error``, ``bitable_warning``.
    """
    month_names = (
        "",
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    )
    if month < 1 or month > 12:
        return {"ok": False, "error": "Invalid month", "year": year, "month": month, "weeks": []}
    values, sheet_err = _get_cached_ose_sheet_values()
    if not values:
        return {
            "ok": False,
            "error": sheet_err or "No sheet data",
            "year": year,
            "month": month,
            "month_label": f"{month_names[month]} {year}",
            "weeks": [],
        }

    leave_items: list[dict[str, Any]] = []
    offset_items: list[dict[str, Any]] = []
    bitable_warning: Optional[str] = None
    token: Optional[str] = None
    try:
        token = get_tenant_access_token()
        leave_items = _get_leave_display_raw(token)
        _, offset_items = _get_bitable_raw_pair(token)
    except Exception as e:
        bitable_warning = f"Leave filter skipped: {e}"

    def _on_leave(shift_label: str, entries: list[dict[str, Any]]) -> bool:
        for r in entries:
            ln = str(r.get("name") or "").strip()
            if ln and _names_same_person(shift_label, ln):
                return True
        return False

    def _leave_for_json(entries: list[dict[str, Any]]) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        for row in entries:
            st = row.get("start")
            ed = row.get("end")
            out.append(
                {
                    "name": str(row.get("name") or ""),
                    "leave_type": str(row.get("leave_type") or "Leave"),
                    "start": _format_ddmmyyyy(st) if isinstance(st, date) else "",
                    "end": _format_ddmmyyyy(ed) if isinstance(ed, date) else "",
                }
            )
        return out

    _, last_day = calendar.monthrange(year, month)
    day_payload: dict[int, dict[str, Any]] = {}
    for dnum in range(1, last_day + 1):
        d = date(year, month, dnum)
        morning, night, roster_leave = _shift_codes_from_matrix(values, d)
        leave_entries: list[dict[str, Any]] = []
        offset_lines: list[str] = []
        if leave_items and token:
            try:
                leave_entries = _extract_leave_entries_for_date(
                    d, token, items=leave_items, require_approved=False
                )
            except Exception:
                leave_entries = []
        leave_entries = _merge_roster_sheet_leave(leave_entries, roster_leave, d)
        if offset_items and token:
            try:
                offset_lines = _extract_offset_lines_for_date(d, token, items=offset_items)
            except Exception:
                offset_lines = []
        morning = [n for n in morning if not _on_leave(n, leave_entries)]
        night = [n for n in night if not _on_leave(n, leave_entries)]
        day_payload[dnum] = {
            "day": dnum,
            "morning": morning,
            "night": night,
            "leave": _leave_for_json(leave_entries),
            "offset": offset_lines,
        }

    cal_weeks = calendar.monthcalendar(year, month)
    weeks_out: list[list[Optional[dict[str, Any]]]] = []
    for row in cal_weeks:
        line: list[Optional[dict[str, Any]]] = []
        for dnum in row:
            if dnum == 0:
                line.append(None)
            else:
                line.append(day_payload.get(dnum))
        weeks_out.append(line)

    return {
        "ok": True,
        "error": None,
        "year": year,
        "month": month,
        "month_label": f"{month_names[month]} {year}",
        "weeks": weeks_out,
        "bitable_warning": bitable_warning,
    }


# Short-lived in-memory cache so morning card + /ose do not double-hit Bitable.
# Set OSE_BITABLE_CACHE_SEC=0 to disable. Daily cron calls ``invalidate_ose_bitable_cache``.
_OSE_BITABLE_RAW: dict[str, Any] = {
    "monotonic": 0.0,
    "leave_display": None,
    "leave_approval": None,
    "offset": None,
    "person_ids": None,
    "offset_person_options": None,
}
_OSE_BITABLE_TTL_SEC = int(os.getenv("OSE_BITABLE_CACHE_SEC", "120"))


def invalidate_ose_bitable_cache() -> None:
    """Clear cached leave/offset rows (e.g. after daily sync job)."""
    _OSE_BITABLE_RAW["leave_display"] = None
    _OSE_BITABLE_RAW["leave_approval"] = None
    _OSE_BITABLE_RAW["offset"] = None
    _OSE_BITABLE_RAW["person_ids"] = None
    _OSE_BITABLE_RAW["offset_person_options"] = None
    _OSE_BITABLE_RAW["monotonic"] = 0.0


def sync_ose_leave_offset_bitable() -> str:
    """
    Force-fetch leave + offset tables from Lark (for a daily scheduler).
    Returns a one-line status for logs.
    """
    try:
        token = get_tenant_access_token()
    except Exception as e:
        return f"❌ OSE Bitable sync (token): {e}"
    invalidate_ose_bitable_cache()
    try:
        leave_disp, leave_appr, offset = _get_bitable_raw_triple(token)
    except Exception as e:
        return f"❌ OSE Bitable sync: {e}"
    return (
        f"✅ OSE Bitable synced ({len(leave_disp)} HRMS leave, "
        f"{len(leave_appr)} approval leave, {len(offset)} offset rows)"
    )


def _leaveose_table_id_for_display() -> str:
    """Always leaveose (``tblvoXE0hsPjgb0j``). Ignores mis-set ``OSE_HRMS_LEAVE_TABLE_ID`` / ``TRACK_LEAVE_TABLE_ID``."""
    configured = (os.getenv("OSE_HRMS_LEAVE_TABLE_ID") or os.getenv("TRACK_LEAVE_TABLE_ID") or "").strip()
    if configured and configured not in (LEAVEOSE_TABLE_ID_CANONICAL, ""):
        print(
            f"[ose_Duty] WARNING: OSE_HRMS_LEAVE_TABLE_ID={configured!r} ignored; "
            f"display reads leaveose {LEAVEOSE_TABLE_ID_CANONICAL!r} only",
            flush=True,
        )
    return LEAVEOSE_TABLE_ID_CANONICAL


def _fetch_leaveose_bitable_records(token: str) -> list[dict[str, Any]]:
    """Read **leaveose** sheet only. Never ``OSE_ALL_LEAVE_TABLE_ID`` (leave 全员)."""
    table_id = _leaveose_table_id_for_display()
    if table_id == OSE_ALL_LEAVE_TABLE_ID:
        raise RuntimeError(
            "leaveose table id must differ from leave 全员 table; "
            f"got {table_id!r} — check OSE_ALL_LEAVE_TABLE_ID / OSE_LEAVE_TABLE_ID in .env"
        )
    return _bitable_get_all_records(token, OSE_BASE_TOKEN, table_id)


def _get_leave_display_raw(token: str) -> list[dict[str, Any]]:
    """HRMS-synced OSE leave rows (leaveose sheet) for webapp, Admin ALL, duty calendar."""
    now = time.monotonic()
    cached = _OSE_BITABLE_RAW.get("leave_display")
    ts = float(_OSE_BITABLE_RAW.get("monotonic") or 0)
    if _OSE_BITABLE_TTL_SEC > 0 and isinstance(cached, list) and now - ts < _OSE_BITABLE_TTL_SEC:
        return cached
    items = _fetch_leaveose_bitable_records(token)
    _OSE_BITABLE_RAW["leave_display"] = items
    _OSE_BITABLE_RAW["monotonic"] = now
    return items


def _get_leave_approval_raw(token: str) -> list[dict[str, Any]]:
    """OSE leave request / approval workflow table."""
    now = time.monotonic()
    cached = _OSE_BITABLE_RAW.get("leave_approval")
    ts = float(_OSE_BITABLE_RAW.get("monotonic") or 0)
    if _OSE_BITABLE_TTL_SEC > 0 and isinstance(cached, list) and now - ts < _OSE_BITABLE_TTL_SEC:
        return cached
    items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_LEAVE_TABLE_ID)
    _OSE_BITABLE_RAW["leave_approval"] = items
    _OSE_BITABLE_RAW["monotonic"] = now
    return items


def _get_offset_raw(token: str) -> list[dict[str, Any]]:
    now = time.monotonic()
    cached = _OSE_BITABLE_RAW.get("offset")
    ts = float(_OSE_BITABLE_RAW.get("monotonic") or 0)
    if _OSE_BITABLE_TTL_SEC > 0 and isinstance(cached, list) and now - ts < _OSE_BITABLE_TTL_SEC:
        return cached
    items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_OFFSET_TABLE_ID)
    _OSE_BITABLE_RAW["offset"] = items
    _OSE_BITABLE_RAW["monotonic"] = now
    return items


def _get_bitable_raw_pair(token: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(HRMS leave display, offset) — backward-compatible pair for calendar/bot."""
    leave_disp, _, offset = _get_bitable_raw_triple(token)
    return leave_disp, offset


def _get_bitable_raw_triple(token: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    now = time.monotonic()
    leave_disp = _OSE_BITABLE_RAW.get("leave_display")
    leave_appr = _OSE_BITABLE_RAW.get("leave_approval")
    offset = _OSE_BITABLE_RAW.get("offset")
    ts = float(_OSE_BITABLE_RAW.get("monotonic") or 0)
    fresh = (
        _OSE_BITABLE_TTL_SEC > 0
        and isinstance(leave_disp, list)
        and isinstance(leave_appr, list)
        and isinstance(offset, list)
        and now - ts < _OSE_BITABLE_TTL_SEC
    )
    if fresh:
        return leave_disp, leave_appr, offset
    leave_disp = _fetch_leaveose_bitable_records(token)
    leave_appr = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_LEAVE_TABLE_ID)
    offset = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_OFFSET_TABLE_ID)
    _OSE_BITABLE_RAW["monotonic"] = now
    _OSE_BITABLE_RAW["leave_display"] = leave_disp
    _OSE_BITABLE_RAW["leave_approval"] = leave_appr
    _OSE_BITABLE_RAW["offset"] = offset
    return leave_disp, leave_appr, offset


def _bitable_get_all_records(token: str, app_token: str, table_id: str) -> list[dict[str, Any]]:
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {token}"}
    out: list[dict[str, Any]] = []
    page_token = None
    while True:
        params: dict[str, Any] = {"page_size": 200}
        if page_token:
            params["page_token"] = page_token
        res = _lark_request("get", url, headers=headers, params=params, timeout=30).json()
        if res.get("code") != 0:
            raise RuntimeError(f"Bitable fetch failed: {res}")
        data = res.get("data", {})
        out.extend(data.get("items") or [])
        if not data.get("has_more"):
            break
        page_token = data.get("page_token")
    return out


def _is_approved(v: Any) -> bool:
    return _field_text(v).strip().lower() == "approved"


def _leave_row_is_approved(fields: dict[str, Any]) -> bool:
    """
    leaveose (``tblvoXE0hsPjgb0j``) HRMS sync usually has **no** Status column.
    Empty status = approved (same rule as ``leavewfh._parse_leave_row``).
    """
    status_v = _get_field_by_aliases(fields, ["Status", "Approval Status"])
    status_text = _field_text(status_v).strip().lower()
    if status_text in ("rejected", "reject", "cancelled", "canceled", "denied"):
        return False
    if status_text and status_text != "approved":
        return False
    return True


def _leave_row_person_name(fields: dict[str, Any]) -> str:
    name_raw = _get_field_by_aliases(fields, ["Name", "Employee Name", "Person"])
    name = _field_text(name_raw)
    if name:
        return name
    if isinstance(name_raw, list) and name_raw:
        first = name_raw[0]
        if isinstance(first, dict):
            return str(first.get("name") or first.get("en_name") or "").strip()
    return ""


def _is_ose_dutylist_leave_name(name: str) -> bool:
    """OSE = dutyList.csv department OSE / OSE Senior / Team Lead / Manager (not hardcoded roster)."""
    return dlm.is_ose_dutylist_name(name)


def _resolve_ose_leave_display_person(name: str) -> tuple[str, str]:
    """
    Canonical key + Leave-section display label for the daily OSE card.

    Includes the 31-person shift roster **and** dutyList.csv OSE departments
    (e.g. OSE Manager) who are not on the shift sheet.
    """
    raw = (name or "").strip()
    if not raw:
        return "", ""
    roster_key = _resolve_ose_leave_roster_key(raw)
    if roster_key:
        display = ose_roster_sheet_label(roster_key) or _title_name(roster_key)
        return roster_key, display
    entry = dlm.match_duty_entry(raw)
    if entry and dlm.is_ose_department(entry["department"]):
        canon = entry["name"]
        dept = (entry.get("department") or "").strip()
        if dept and dept.upper() != "OSE":
            display = f"{canon} ({dept})"
        else:
            display = canon
        return canon, display
    return "", ""


def _extract_ose_shift_roster_leave_for_date(
    target_date: date,
    token: str,
    *,
    items: Optional[list[dict[str, Any]]] = None,
) -> list[dict[str, Any]]:
    """Approved leaveose rows for OSE shift roster + dutyList OSE on ``target_date``."""
    if items is None:
        items = _get_leave_display_raw(token)
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for it in items:
        rid = str(it.get("record_id") or "").strip()
        if not rid:
            continue
        f = it.get("fields") or {}
        if not _leave_row_is_approved(f):
            continue
        person_key, display_name = _resolve_ose_leave_display_person(
            _leave_row_person_name(f)
        )
        if not person_key:
            continue
        st = _parse_date_value(_get_field_by_aliases(f, ["Start Date", "Leave Start Date", "From"]))
        ed = _parse_date_value(_get_field_by_aliases(f, ["End Date", "Leave End Date", "To"]))
        if not st or not ed:
            continue
        if not (st <= target_date <= ed):
            continue
        lt = _field_text(_get_field_by_aliases(f, ["Leave Type", "Type"])) or "Leave"
        key = (person_key, st.isoformat(), ed.isoformat(), lt.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "name": person_key,
                "display_name": display_name,
                "leave_type": lt,
                "start": st,
                "end": ed,
            }
        )
    return sorted(out, key=lambda x: str(x.get("name") or "").lower())


def _person_listed_on_leave(name: str, leave_entries: list[dict[str, Any]]) -> bool:
    for r in leave_entries:
        for key in ("name", "display_name"):
            ln = str(r.get(key) or "").strip()
            if ln and _names_same_person(name, ln):
                return True
    return False


def _dedupe_leave_entries_by_person(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per person — avoids duplicate names in the Leave section."""
    out: list[dict[str, Any]] = []
    for row in entries:
        nm = str(row.get("name") or row.get("display_name") or "").strip()
        if nm and any(
            _names_same_person(nm, str(r.get("name") or r.get("display_name") or ""))
            for r in out
        ):
            continue
        out.append(row)
    return out


def _leave_entry_names_for_dedupe(leave_entries: list[dict[str, Any]]) -> list[str]:
    """Roster / display names already listed in the OSE Leave section."""
    names: list[str] = []
    seen: set[str] = set()
    for row in leave_entries:
        for key in ("name", "display_name"):
            nm = str(row.get(key) or "").strip()
            if not nm:
                continue
            nk = _name_key(nm)
            if nk in seen:
                continue
            seen.add(nk)
            names.append(nm)
    return names


def _extract_leave_entries_for_date(
    target_date: date,
    token: str,
    *,
    items: Optional[list[dict[str, Any]]] = None,
    require_approved: bool = True,
) -> list[dict[str, Any]]:
    if items is None:
        items = _get_leave_display_raw(token)
    out: list[dict[str, Any]] = []
    for it in items:
        f = it.get("fields") or {}
        if require_approved:
            status_v = _get_field_by_aliases(f, ["Status", "Approval Status"])
            if not _is_approved(status_v):
                continue
        name = _title_name(_field_text(_get_field_by_aliases(f, ["Name", "Employee Name", "Person"])))
        if not name or not _is_ose_dutylist_leave_name(name):
            continue
        entry = dlm.match_duty_entry(name)
        display_name = entry["name"] if entry else name
        st = _parse_date_value(_get_field_by_aliases(f, ["Start Date", "Leave Start Date", "From"]))
        ed = _parse_date_value(_get_field_by_aliases(f, ["End Date", "Leave End Date", "To"]))
        if not st or not ed:
            continue
        if st <= target_date <= ed:
            out.append(
                {
                    "name": display_name,
                    "leave_type": _field_text(_get_field_by_aliases(f, ["Leave Type", "Type"])) or "Leave",
                    "start": st,
                    "end": ed,
                }
            )
    return sorted(out, key=lambda x: x["name"])


def _extract_offset_entries_for_date(
    target_date: date,
    token: str,
    *,
    items: Optional[list[dict[str, Any]]] = None,
) -> list[dict[str, str]]:
    """Approved offsets touching ``target_date`` as ``{"shift": "D"/"N", "line": …}``.

    ``shift`` comes from the row's Shift Type, so the card can list morning (``D``)
    and night (``N``) offsets under their own shift instead of one flat block.
    """
    if items is None:
        items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_OFFSET_TABLE_ID)
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for it in items:
        f = it.get("fields") or {}
        approval_v = _get_field_by_aliases(f, ["Approval Status", "Status"])
        if not _is_approved(approval_v):
            continue
        req = _title_name(
            _field_text(_get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]))
        )
        exc = _title_name(
            _field_text(_get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]))
        )
        od = _bitable_field_original_date(f)
        xd = _bitable_field_exchange_date(f)
        if not req or not exc or not od or not xd:
            continue
        if target_date != od and target_date != xd:
            continue
        shift = _field_text(_get_field_by_aliases(f, ["Shift Type", "Shift"])).strip().upper()
        if shift not in ("D", "N"):
            shift = ""  # unknown → listed without a shift heading rather than dropped
        if req.lower() == exc.lower():
            line = f"• {req} is offset with him/herself."
        else:
            line = f"• {req}({_format_ddmmyyyy(od)}) offset with {exc}({_format_ddmmyyyy(xd)})"
        key = (shift, line)
        if key in seen:
            continue
        seen.add(key)
        out.append({"shift": shift, "line": line})
    return sorted(out, key=lambda e: (e["shift"], e["line"]))


def _extract_offset_lines_for_date(
    target_date: date,
    token: str,
    *,
    items: Optional[list[dict[str, Any]]] = None,
) -> list[str]:
    if items is None:
        items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_OFFSET_TABLE_ID)
    lines: list[str] = []
    for it in items:
        f = it.get("fields") or {}
        approval_v = _get_field_by_aliases(f, ["Approval Status", "Status"])
        if not _is_approved(approval_v):
            continue
        req = _title_name(
            _field_text(_get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]))
        )
        exc = _title_name(
            _field_text(_get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]))
        )
        od = _bitable_field_original_date(f)
        xd = _bitable_field_exchange_date(f)
        if not req or not exc or not od or not xd:
            continue
        if target_date != od and target_date != xd:
            continue
        if req.lower() == exc.lower():
            lines.append(f"• {req} is offset with him/herself.")
            continue
        lines.append(
            f"• {req}({_format_ddmmyyyy(od)}) offset with {exc}({_format_ddmmyyyy(xd)})"
        )
    return sorted(set(lines))


def build_offset_shift_columns(
    offset_entries: list[dict[str, str]],
    *,
    first_title: str = "🌅 Morning shift",
    second_title: str = "🌙 Night Shift",
) -> list[dict[str, Any]]:
    """``🔁 Offset`` split by shift — morning on the left, night beside it.

    Only shifts that actually have an offset are rendered: no morning offset → no
    morning column, and no offsets at all → no elements (caller shows nothing).
    Offsets whose row has no usable Shift Type are listed underneath so they can
    never silently disappear.
    """
    morning = [e["line"] for e in offset_entries if e.get("shift") == "D"]
    night = [e["line"] for e in offset_entries if e.get("shift") == "N"]
    other = [e["line"] for e in offset_entries if e.get("shift") not in ("D", "N")]
    if not (morning or night or other):
        return []

    elements: list[dict[str, Any]] = [
        {"tag": "div", "text": {"tag": "lark_md", "content": "🔁 **Offset**"}}
    ]
    columns: list[dict[str, Any]] = []
    for title, rows in ((first_title, morning), (second_title, night)):
        if not rows:
            continue  # that shift has no offset today → omit the column entirely
        columns.append(
            {
                "tag": "column",
                "width": "weighted",
                "weight": 1,
                "vertical_align": "top",
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "**" + title + "**\n" + "\n".join(rows),
                        },
                    }
                ],
            }
        )
    if columns:
        elements.append(
            {"tag": "column_set", "flex_mode": "bisect", "columns": columns}
        )
    if other:
        elements.append(
            {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(other)}}
        )
    return elements


def _section_lines(title: str, rows: list[str], *, empty_text: str = "• -") -> list[str]:
    out = [title]
    if rows:
        out.extend(rows)
    else:
        out.append(empty_text)
    out.append("")
    return out


def _build_ose_context_once(
    target_date: date, mode: str
) -> tuple[list[str], list[str], list[str], list[dict[str, Any]], Optional[str]]:
    """
    mode:
      - 'morning': Rest yesterday night, Luck today morning
      - 'evening': Rest today morning, Luck today night
      - 'date': Morning shift (D) and Night shift (N) on ``target_date`` (/osedate)
    """
    try:
        if mode == "evening":
            rest_names, _night_unused = get_shift_names_for_date(target_date)
            _m_unused, luck_names = get_shift_names_for_date(target_date)
        elif mode == "morning":
            _, rest_names = get_shift_names_for_date(target_date - timedelta(days=1))
            luck_names, _ = get_shift_names_for_date(target_date)
        else:
            # ``date`` (/osedate): same calendar day — morning (D) first, night (N) second.
            rest_names, luck_names = get_shift_names_for_date(target_date)

        token = get_tenant_access_token()
        leave_items, offset_items = _get_bitable_raw_pair(token)
        leave_entries = _extract_ose_shift_roster_leave_for_date(
            target_date, token, items=leave_items
        )
        offset_lines = _extract_offset_lines_for_date(target_date, token, items=offset_items)
        values, _sheet_err = _get_cached_ose_sheet_values()
        if values:
            _, _, roster_leave = _shift_codes_from_matrix(values, target_date)
            leave_entries = _merge_roster_sheet_leave(leave_entries, roster_leave, target_date)
    except Exception as e:
        return [], [], [], [], f"❌ OSE data load failed: {e}"

    if mode == "morning":
        # 7am: Rest = last night's N (yesterday); Good Luck = today's D — filter leave per day.
        try:
            leave_yesterday = _extract_ose_shift_roster_leave_for_date(
                target_date - timedelta(days=1), token, items=leave_items
            )
        except Exception:
            leave_yesterday = []
        rest_names = [n for n in rest_names if not _person_listed_on_leave(n, leave_yesterday)]
        luck_names = [n for n in luck_names if not _person_listed_on_leave(n, leave_entries)]
    else:
        # 7pm, /ose, /osedate — both sections use today's leave list only.
        rest_names = [n for n in rest_names if not _person_listed_on_leave(n, leave_entries)]
        luck_names = [n for n in luck_names if not _person_listed_on_leave(n, leave_entries)]
    leave_entries = _dedupe_leave_entries_by_person(leave_entries)
    return sorted(rest_names), sorted(luck_names), offset_lines, leave_entries, None


def _build_ose_context(
    target_date: date, mode: str
) -> tuple[list[str], list[str], list[str], list[dict[str, Any]], Optional[str]]:
    """Load OSE context; retry transient Lark HTTPS failures before returning an error card."""
    last = _build_ose_context_once(target_date, mode)
    err = last[4]
    if not err or not is_transient_ose_load_error(err):
        return last
    for attempt in range(2, _OSE_BUILD_RETRIES + 1):
        wait = _OSE_BUILD_RETRY_SEC * (attempt - 1)
        print(
            f"[ose_Duty] OSE load retry {attempt}/{_OSE_BUILD_RETRIES} in {wait:.0f}s: {err[:160]}",
            flush=True,
        )
        time.sleep(wait)
        _OSE_SHEET_CACHE["mono"] = 0.0
        last = _build_ose_context_once(target_date, mode)
        err = last[4]
        if not err:
            return last
        if not is_transient_ose_load_error(err):
            return last
    return last


def build_ose_message_card(
    *,
    target_date: date,
    rest_names: list[str],
    luck_names: list[str],
    offset_lines: list[str],
    leave_entries: list[dict[str, Any]],
    include_tag: bool = False,
    first_section_title: str = "🌅 Morning shift",
    second_section_title: str = "🌙 Night Shift",
    dutylist_attendance: Optional[dict[str, Any]] = None,
    offset_entries: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    lines: list[str] = []
    if include_tag and TARGET_USER_OPEN_ID:
        lines.append(f'👥 <at id="{TARGET_USER_OPEN_ID}">User</at>')
    lines.append(f"📅 **{target_date.strftime('%d/%m/%Y')}**")
    lines.append("")
    lines.extend(_section_lines(first_section_title, [f"• {n}" for n in rest_names]))
    lines.extend(_section_lines(second_section_title, [f"• {n}" for n in luck_names]))
    # Offsets are rendered per shift (morning | night) as their own card elements
    # below; only fall back to the flat text block when shift info is unavailable.
    offset_shift_elements = (
        build_offset_shift_columns(
            offset_entries,
            first_title=first_section_title,
            second_title=second_section_title,
        )
        if offset_entries
        else []
    )
    if not offset_shift_elements and offset_lines:
        lines.extend(_section_lines("🔁 Offset", offset_lines))
    if leave_entries:
        leave_lines: list[str] = []
        for row in leave_entries:
            name = str(row.get("display_name") or row.get("name") or "")
            lt = row["leave_type"]
            st = row["start"]
            ed = row["end"]
            if st == ed:
                leave_lines.append(f"• {name} ({lt}) - {_format_ddmmyyyy(st)}")
            else:
                leave_lines.append(
                    f"• {name} ({lt}) - From {_format_ddmmyyyy(st)} until {_format_ddmmyyyy(ed)}"
                )
        lines.extend(_section_lines("🏖️ Leave", leave_lines))
    body_elements: list[dict[str, Any]] = [
        {
            "tag": "div",
            "text": {"tag": "lark_md", "content": "\n".join(lines).strip()},
        }
    ]
    if offset_shift_elements:
        body_elements.extend(offset_shift_elements)
    if dutylist_attendance:
        try:
            import leavewfh as lw

            lw_sections = lw.dutylist_attendance_plain_sections(
                dutylist_attendance, target_date
            )
            if lw_sections:
                body_elements.append({"tag": "hr"})
                for _title, section_els in lw_sections:
                    body_elements.extend(section_els)
        except Exception:
            pass

    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "green",
            "title": {
                "tag": "plain_text",
                "content": f"OSE DUTY FOR {target_date.strftime('%d/%m/%Y')}",
            },
        },
        "body": {"elements": body_elements},
    }


def get_ose_payload_for_date(target_date: date, mode: str = "date", *, include_tag: bool = False) -> dict[str, Any]:
    rest_names, luck_names, offset_lines, leave_entries, err = _build_ose_context(target_date, mode)
    if err:
        return {"text": err, "lark_card": None}

    dutylist_attendance: dict[str, Any] = {}
    try:
        import leavewfh as lw

        dutylist_attendance = lw.get_dutylist_leave_wfh_for_date(target_date)
        if leave_entries:
            dutylist_attendance = lw.filter_dutylist_leave_wfh_excluding_people(
                dutylist_attendance,
                _leave_entry_names_for_dedupe(leave_entries),
            )
    except Exception:
        dutylist_attendance = {}

    if mode == "date":
        first_title_plain = "Morning shift"
        second_title_plain = "Night Shift"
        first_title_card = "🌅 Morning shift"
        second_title_card = "🌙 Night Shift"
    else:
        first_title_plain = "(～￣▽￣)～ Rest Well"
        second_title_plain = "Good Luckヾ(≧▽≦*)o"
        first_title_card = "😴 (～￣▽￣)～ Rest Well"
        second_title_card = "🍀 Good Luckヾ(≧▽≦*)o"

    lines: list[str] = []
    if include_tag and TARGET_USER_OPEN_ID:
        lines.append(f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>')
    lines.append(first_title_plain)
    lines.extend([f"• {n}" for n in rest_names] or ["• -"])
    lines.append("")
    lines.append(second_title_plain)
    lines.extend([f"• {n}" for n in luck_names] or ["• -"])
    if offset_lines:
        lines.append("")
        lines.append("Offset")
        lines.extend(offset_lines)
    if leave_entries:
        lines.append("")
        lines.append("Leave")
        for row in leave_entries:
            name = str(row.get("display_name") or row.get("name") or "")
            lt = row["leave_type"]
            st = row["start"]
            ed = row["end"]
            if st == ed:
                lines.append(f"• {name} ({lt}) - {_format_ddmmyyyy(st)}")
            else:
                lines.append(
                    f"• {name} ({lt}) - From {_format_ddmmyyyy(st)} until {_format_ddmmyyyy(ed)}"
                )
    if dutylist_attendance:
        try:
            import leavewfh as lw

            lines.append("")
            lines.append("Leave & WFH")
            lines.extend(lw.format_dutylist_leave_wfh_display(dutylist_attendance, target_date))
        except Exception:
            pass

    text = "\n".join(lines).strip()
    return {
        "text": text,
        "lark_card": build_ose_message_card(
            target_date=target_date,
            rest_names=rest_names,
            luck_names=luck_names,
            offset_lines=offset_lines,
            leave_entries=leave_entries,
            include_tag=include_tag,
            first_section_title=first_title_card,
            second_section_title=second_title_card,
            dutylist_attendance=dutylist_attendance,
            offset_entries=_offset_entries_for_card(target_date),
        ),
    }


def _offset_entries_for_card(target_date: date) -> list[dict[str, str]]:
    """Shift-tagged offsets for the card; never breaks the card if it fails."""
    try:
        return _extract_offset_entries_for_date(target_date, get_tenant_access_token())
    except Exception as exc:  # noqa: BLE001 — fall back to the flat offset block
        print(f"[ose_Duty] offset shift split unavailable: {exc!r}", flush=True)
        return []


def get_ose_payload_for_now(now_dt: Optional[datetime] = None, *, include_tag: bool = False) -> dict[str, Any]:
    now_dt = now_dt or datetime.now()
    return get_ose_payload_for_date(now_dt.date(), mode="date", include_tag=include_tag)


def get_ose_duty_for_date(target_date: date) -> str:
    return str(get_ose_payload_for_date(target_date, mode="date").get("text") or "")


def osedate(date_str: str) -> str:
    try:
        target_date = datetime.strptime(date_str, "%d/%m/%Y").date()
    except ValueError:
        return "❌ Invalid date format. Please use DD/MM/YYYY (e.g., 31/12/2026)"
    return get_ose_duty_for_date(target_date)


def get_ose_today_duty() -> str:
    return str(get_ose_payload_for_now().get("text") or "")


OSE_LEAVE_FORM_NAMES: tuple[str, ...] = tuple(key for key, _label in OSE_LEAVE_SHEET_ROSTER)

# Special Exchange person choice on offset forms (= same person as Request Person).
OFFSET_EXCHANGE_MYSELF_LABEL = "Myself"

# Excluded from offset form "Exchange person" dropdown only (leave form & OTE duty unchanged).
OSE_OFFSET_FORM_EXCHANGE_EXCLUDED: frozenset[str] = frozenset(
    _title_name(n)
    for n in (
        "Renzel",
        "Faye",
        "Shie Ni",
        "Jeno",
        "Kwang Ming",
    )
)


def ose_offset_form_exchange_names(*, exclude_person: str = "") -> tuple[str, ...]:
    """Roster names allowed as Exchange person on the Lark offset request/edit form."""
    skip = _title_name(exclude_person) if (exclude_person or "").strip() else ""
    names = tuple(
        n
        for n in OSE_LEAVE_FORM_NAMES
        if _title_name(n) not in OSE_OFFSET_FORM_EXCHANGE_EXCLUDED
        and (not skip or _title_name(n) != skip)
    )
    return (OFFSET_EXCHANGE_MYSELF_LABEL,) + names


def _is_offset_exchange_myself_label(name: str) -> bool:
    return (name or "").strip().casefold() == OFFSET_EXCHANGE_MYSELF_LABEL.casefold()


def resolve_offset_exchange_person(exchange_person: str, *, request_person: str) -> str:
    """Map form value ``Myself`` to the requester's roster name."""
    if _is_offset_exchange_myself_label(exchange_person):
        req = _canonical_roster_form_name(request_person)
        if not req:
            raise ValueError("Request person is required when Exchange person is Myself")
        return req
    return _validate_offset_exchange_person(exchange_person)


def _validate_offset_exchange_person(exchange_person: str) -> str:
    exc = _canonical_roster_form_name(exchange_person)
    allowed = {
        _canonical_roster_form_name(n)
        for n in ose_offset_form_exchange_names()
        if not _is_offset_exchange_myself_label(n)
    }
    if not exc or exc not in allowed:
        raise ValueError(f"Unknown or disallowed exchange person {exchange_person!r}")
    return exc


OSE_LEAVE_TYPES: tuple[str, ...] = (
    "Sick Leave",
    "Annual Leave",
    "Compassionate Leave",
    "Hospitalisation Leave",
    "Marriage Leave",
    "Maternity Leave",
    "Non Pay Leave",
    "Replacement Leave",
)

OSE_SHIFT_TYPES: tuple[str, ...] = ("N", "D")


def _person_shift_code_on_date(
    values: list[list[Any]],
    person: str,
    target_date: date,
) -> str:
    """Return ``D`` / ``N`` / ``*`` / ``L`` / empty for one person on one calendar day."""
    row_idx = _sheet_row_index_for_person(values, person)
    if row_idx is None:
        return ""
    col = _date_column_for_matrix(values, target_date)
    if col is None:
        return ""
    row = values[row_idx]
    if col >= len(row):
        return ""
    return _field_text(row[col]).strip().upper()


def _person_is_rest_day_on_date(
    values: list[list[Any]],
    person: str,
    target_date: date,
) -> bool:
    """True when the person is not on D/N duty and the day can receive a swap (empty or ``*``)."""
    code = _person_shift_code_on_date(values, person, target_date)
    if code in OSE_SHIFT_TYPES:
        return False
    if code in _OSE_SHIFT_SHEET_LEAVE_CODES:
        return False
    return code in ("", "*")


_OFFSET_DUTY_DATE_ERROR_FOOTER = (
    "Kindly check again. If have any issue kindly let Jun Chen know. Thanks."
)


def validate_offset_swap_duty_dates(
    *,
    request_person: str,
    exchange_person: str,
    shift_type: str,
    original_date: date,
    exchange_date: date,
) -> None:
    """
    Two-person swap: Original Date must be requester's duty day with the selected shift;
    Exchange Date must be exchange person's duty day with the same shift.

    Exchange with self (``Myself``): Original Date must match the selected shift;
    Exchange Date must be the requester's rest day (blank or ``*``, not leave).
    """
    st = (shift_type or "").strip().upper()
    if st not in OSE_SHIFT_TYPES:
        raise ValueError("Shift Type must be N or D")
    values, err = _get_cached_ose_sheet_values()
    if not values:
        raise RuntimeError(err or "Could not load OSE shift sheet")
    req = _title_name(request_person)
    if not req:
        raise ValueError("Request person is required")
    exc = _title_name(exchange_person) or req
    req_orig = _person_shift_code_on_date(values, req, original_date)
    same_person = _names_same_person(req, exc)
    footer = _OFFSET_DUTY_DATE_ERROR_FOOTER
    if same_person:
        if req_orig != st:
            if req_orig in OSE_SHIFT_TYPES:
                raise ValueError(
                    f"As checked the selected shift ({st}) does not match your {req_orig} duty "
                    f"on the original date. {footer}"
                )
            raise ValueError(f"As checked the requested date is not your duty date. {footer}")
        exc_code = _person_shift_code_on_date(values, req, exchange_date)
        if exc_code in _OSE_SHIFT_SHEET_LEAVE_CODES:
            raise ValueError(
                "As checked the exchange date is a leave day — pick a rest day instead. "
                f"{footer}"
            )
        if not _person_is_rest_day_on_date(values, req, exchange_date):
            raise ValueError(f"As checked the exchange date is not your rest day. {footer}")
        return
    exc_code = _person_shift_code_on_date(values, exc, exchange_date)
    orig_shift_ok = req_orig == st
    exc_shift_ok = exc_code == st
    if orig_shift_ok and exc_shift_ok:
        return
    if not orig_shift_ok and not exc_shift_ok:
        if req_orig in OSE_SHIFT_TYPES and exc_code in OSE_SHIFT_TYPES:
            raise ValueError(
                f"As checked the selected shift ({st}) does not match duty on the original date "
                f"({req_orig}) or exchange date ({exc_code}). {footer}"
            )
        raise ValueError(
            "As checked the requested date is not your duty date and exchange date is not exchange person duty date. "
            f"{footer}"
        )
    if not orig_shift_ok:
        if req_orig in OSE_SHIFT_TYPES:
            raise ValueError(
                f"As checked the selected shift ({st}) does not match your {req_orig} duty "
                f"on the original date. {footer}"
            )
        raise ValueError(f"As checked the requested date is not your duty date. {footer}")
    if exc_code in OSE_SHIFT_TYPES:
        raise ValueError(
            f"As checked the selected shift ({st}) does not match exchange person's {exc_code} duty "
            f"on the exchange date. {footer}"
        )
    raise ValueError(f"As checked the exchange date is not exchange person duty date. {footer}")


def _bitable_date_ms(d: date) -> int:
    return int(datetime.combine(d, datetime.min.time()).timestamp() * 1000)


def _schedule_offset_duty_wiki_sync(*, record_id: str = "", delete: bool = False, full: bool = False) -> None:
    try:
        import offsetleave as ol

        ol.schedule_offset_duty_wiki_sync(record_id=record_id, delete=delete, full=full)
    except Exception as exc:
        print(f"[ose_Duty] duty wiki offset sync schedule failed: {exc!r}", flush=True)


def _bitable_create_record(token: str, table_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    url = (
        f"https://open.larksuite.com/open-apis/bitable/v1/apps/"
        f"{OSE_BASE_TOKEN}/tables/{table_id}/records"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    res = _lark_request(
        "post",
        url,
        headers=headers,
        params={"user_id_type": "open_id"},
        json={"fields": fields},
        timeout=30,
    ).json()
    if res.get("code") != 0:
        raise RuntimeError(f"Bitable create failed: {res}")
    return res


def _bitable_update_record(
    token: str,
    table_id: str,
    record_id: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    url = (
        f"https://open.larksuite.com/open-apis/bitable/v1/apps/"
        f"{OSE_BASE_TOKEN}/tables/{table_id}/records/{rid}"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    res = _lark_request(
        "put",
        url,
        headers=headers,
        params={"user_id_type": "open_id"},
        json={"fields": fields},
        timeout=30,
    ).json()
    if res.get("code") != 0:
        raise RuntimeError(f"Bitable update failed: {res}")
    return res


def _is_pending_approval(v: Any) -> bool:
    s = _field_text(v).strip().lower()
    return s in ("", "pending")


def _person_item_open_id(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    return str(item.get("id") or item.get("open_id") or item.get("user_id") or "").strip()


def _person_field_items(v: Any) -> list[dict[str, Any]]:
    if isinstance(v, list):
        return [it for it in v if isinstance(it, dict)]
    if isinstance(v, dict):
        return [v]
    return []


def _index_person_field_value(v: Any, idx: dict[str, str]) -> None:
    for item in _person_field_items(v):
        pid = _person_item_open_id(item)
        if not pid:
            continue
        for raw_name in (
            str(item.get("name") or "").strip(),
            str(item.get("en_name") or "").strip(),
        ):
            if not raw_name:
                continue
            nm = _title_name(raw_name)
            idx[nm] = pid
            nk = _name_key(nm)
            if nk:
                idx[nk] = pid
            for roster in TARGET_NAMES:
                if _names_same_person(roster, raw_name):
                    roster_nm = _title_name(roster)
                    idx[roster_nm] = pid
                    roster_nk = _name_key(roster_nm)
                    if roster_nk:
                        idx[roster_nk] = pid


def _index_shift_roster_person_field(v: Any, idx: dict[str, str]) -> None:
    """Index ``open_id`` for OSE duty (15) and leave (31) roster names."""
    blobs: list[str] = []
    if isinstance(v, str) and v.strip():
        blobs.append(v.strip())
    for item in _person_field_items(v):
        pid = _person_item_open_id(item)
        if not pid:
            continue
        for raw in (
            str(item.get("name") or "").strip(),
            str(item.get("en_name") or "").strip(),
        ):
            key = _resolve_ose_roster_key(raw) or _resolve_ose_leave_roster_key(raw)
            if not key:
                continue
            nm = _title_name(key)
            idx[nm] = pid
            nk = _name_key(nm)
            if nk:
                idx[nk] = pid


def _build_ose_person_open_id_index(
    leave_items: list[dict[str, Any]],
    offset_items: list[dict[str, Any]],
) -> dict[str, str]:
    idx: dict[str, str] = {}
    for it in leave_items:
        f = it.get("fields") or {}
        _index_shift_roster_person_field(_get_field_by_aliases(f, ["Name", "Employee Name", "Person"]), idx)
        _index_person_field_value(
            _get_field_by_aliases(f, ["Approver", "Approved By", "Approval Person"]),
            idx,
        )
    for it in offset_items:
        f = it.get("fields") or {}
        _index_person_field_value(
            _get_field_by_aliases(f, ["Approver", "Approved By", "Approval Person"]),
            idx,
        )
        _index_person_field_value(
            _get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]),
            idx,
        )
        _index_person_field_value(
            _get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]),
            idx,
        )
    return idx


def _get_ose_person_open_id_index(token: str) -> dict[str, str]:
    cached = _OSE_BITABLE_RAW.get("person_ids")
    if not isinstance(cached, dict):
        leave_disp, leave_appr, offset = _get_bitable_raw_triple(token)
        leave_all_ose = _fetch_ose_department_all_leave_records(token)
        cached = _build_ose_person_open_id_index(leave_disp + leave_appr + leave_all_ose, offset)
        _OSE_BITABLE_RAW["person_ids"] = cached
    out = dict(cached)
    out.update(_ose_person_open_id_overrides())
    return out


def _lookup_person_open_id(name: str, idx: dict[str, str]) -> str:
    nm = _title_name(name)
    if not nm:
        return ""
    direct = idx.get(nm) or idx.get(_name_key(nm))
    if direct:
        return direct
    for key, pid in idx.items():
        if key.startswith("ou_"):
            continue
        if _names_same_person(nm, key):
            return pid
    return ""


def _canonical_roster_form_name(name: str) -> str:
    """Return the exact ``OSE_LEAVE_FORM_NAMES`` roster key, or empty if unknown."""
    return _resolve_ose_leave_roster_key(name) or ""


def lookup_roster_name_for_open_id(open_id: str, token: str) -> str:
    """
  Map Lark ``open_id`` → OSE roster name using leave/offset Bitable person fields
  (built from duty roster records), not the user's Lark display name.
  """
    oid = (open_id or "").strip()
    if not oid:
        return ""
    idx = _get_ose_person_open_id_index(token)
    for roster in OSE_LEAVE_FORM_NAMES:
        rnm = _title_name(roster)
        if idx.get(rnm) == oid or idx.get(_name_key(rnm)) == oid:
            return roster
    for key, pid in idx.items():
        if pid != oid or key.startswith("ou_"):
            continue
        for roster in OSE_LEAVE_FORM_NAMES:
            if _names_same_person(roster, key):
                return roster
    return ""


def _person_field_value(name: str, *, token: str) -> list[dict[str, str]]:
    nm = _title_name(name)
    open_id = _lookup_person_open_id(nm, _get_ose_person_open_id_index(token))
    if not open_id:
        raise ValueError(
            f"Could not resolve Lark user id for {name!r}. "
            "Pick a name that already appears in leave/offset records."
        )
    return [{"id": open_id}]


def _approver_field_value(
    approver: str,
    *,
    token: str,
    approver_open_id: str = "",
) -> list[dict[str, str]]:
    pid = (approver_open_id or "").strip()
    if pid:
        return [{"id": pid}]
    return _person_field_value(approver, token=token)


def _index_offset_person_option_value(v: Any, idx: dict[str, str]) -> None:
    if not isinstance(v, str):
        return
    opt = v.strip()
    if not opt:
        return
    nm = _title_name(opt)
    idx[nm] = opt
    nk = _name_key(nm)
    if nk:
        idx[nk] = opt
    for roster in OSE_LEAVE_FORM_NAMES:
        if _names_same_person(roster, opt):
            roster_nm = _title_name(roster)
            idx[roster_nm] = opt
            roster_nk = _name_key(roster_nm)
            if roster_nk:
                idx[roster_nk] = opt


def _build_ose_offset_person_option_index(offset_items: list[dict[str, Any]]) -> dict[str, str]:
    idx: dict[str, str] = {}
    for it in offset_items:
        f = it.get("fields") or {}
        _index_offset_person_option_value(
            _get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]),
            idx,
        )
        _index_offset_person_option_value(
            _get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]),
            idx,
        )
    return idx


def _get_ose_offset_person_option_index(token: str) -> dict[str, str]:
    cached = _OSE_BITABLE_RAW.get("offset_person_options")
    if isinstance(cached, dict):
        return cached
    _, offset = _get_bitable_raw_pair(token)
    idx = _build_ose_offset_person_option_index(offset)
    _OSE_BITABLE_RAW["offset_person_options"] = idx
    return idx


def _lookup_offset_person_option(name: str, idx: dict[str, str]) -> str:
    nm = _title_name(name)
    if not nm:
        return ""
    direct = idx.get(nm) or idx.get(_name_key(nm))
    if direct:
        return direct
    for key, opt in idx.items():
        if _names_same_person(nm, key):
            return opt
    return ""


def _offset_person_field_value(name: str, *, token: str) -> str:
    nm = _title_name(name)
    opt = _lookup_offset_person_option(nm, _get_ose_offset_person_option_index(token))
    if opt:
        return opt
    return nm


def get_ose_submit_form_options() -> dict[str, Any]:
    return {
        "leave_names": list(OSE_LEAVE_FORM_NAMES),
        "offset_names": list(OSE_LEAVE_FORM_NAMES),
        "offset_exchange_names": list(ose_offset_form_exchange_names()),
        "leave_types": list(OSE_LEAVE_TYPES),
        "shift_types": list(OSE_SHIFT_TYPES),
    }


def _iter_days_in_month(year: int, month: int) -> list[date]:
    _, last = calendar.monthrange(year, month)
    return [date(year, month, d) for d in range(1, last + 1)]


def _leave_rows_for_calendar(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for it in items:
        f = it.get("fields") or {}
        name = _title_name(_field_text(_get_field_by_aliases(f, ["Name", "Employee Name", "Person"])))
        if not name or not _is_ose_dutylist_leave_name(name):
            continue
        entry = dlm.match_duty_entry(name)
        canon = entry["name"] if entry else name
        st = _parse_date_value(_get_field_by_aliases(f, ["Start Date", "Leave Start Date", "From"]))
        ed = _parse_date_value(_get_field_by_aliases(f, ["End Date", "Leave End Date", "To"]))
        if not st or not ed:
            continue
        rows.append({"name": canon, "start": st, "end": ed})
    return rows


def _record_approval_fields(fields: dict[str, Any]) -> dict[str, str]:
    approval_dt = _parse_date_value(
        _get_field_by_aliases(fields, ["Approval Date", "Approved Date", "ApprovalDate"])
    )
    return {
        "status": _field_text(_get_field_by_aliases(fields, ["Status", "Approval Status"])),
        "approver": _title_name(
            _field_text(_get_field_by_aliases(fields, ["Approver", "Approved By", "Approval Person"]))
        ),
        "approval_date": _format_yyyymmdd(approval_dt),
        "remarks": _field_text(_get_field_by_aliases(fields, ["Remarks", "Remark", "Approval Remarks"])),
    }


def _leave_row_key_for_admin(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        (row.get("name") or "").strip().lower(),
        row.get("start_date") or "",
        row.get("end_date") or "",
        (row.get("leave_type") or "").strip().lower(),
    )


def _admin_leave_row_from_bitable_item(it: dict[str, Any]) -> Optional[dict[str, str]]:
    f = it.get("fields") or {}
    name = _title_name(_field_text(_get_field_by_aliases(f, ["Name", "Employee Name", "Person"])))
    if not name:
        return None
    entry = dlm.match_duty_entry(name)
    if entry:
        name = entry["name"]
    st = _parse_date_value(_get_field_by_aliases(f, ["Start Date", "Leave Start Date", "From"]))
    ed = _parse_date_value(_get_field_by_aliases(f, ["End Date", "Leave End Date", "To"]))
    approval = _record_approval_fields(f)
    return {
        "record_id": str(it.get("record_id") or "").strip(),
        "leave_id": _field_text(_get_field_by_aliases(f, ["LeaveID", "Leave ID", "Leave Id"])),
        "name": name,
        "leave_type": _field_text(_get_field_by_aliases(f, ["Leave Type", "Type"])),
        "start_date": _format_yyyymmdd(st),
        "end_date": _format_yyyymmdd(ed),
        "reason": _field_text(_get_field_by_aliases(f, ["Reason"])),
        "status": approval["status"],
        "approver": approval["approver"],
        "approval_date": approval["approval_date"],
        "remarks": approval["remarks"],
    }


def get_ose_leave_bitable_meta(*, scope: str = "") -> dict[str, Any]:
    """Which Lark tables the app uses (for debugging Admin / webapp data source)."""
    return {
        "api_build": OSE_LEAVE_API_BUILD,
        "scope": scope or "display",
        "leaveose_table_id": LEAVEOSE_TABLE_ID_CANONICAL,
        "leaveose_table_id_enforced": True,
        "leaveose_only_display": True,
        "env_ose_hrms_leave_table_id": OSE_HRMS_LEAVE_TABLE_ID,
        "ose_hrms_leave_table_id": LEAVEOSE_TABLE_ID_CANONICAL,
        "ose_all_leave_table_id": OSE_ALL_LEAVE_TABLE_ID,
        "ose_leave_approval_table_id": OSE_LEAVE_TABLE_ID,
        "base_token": OSE_BASE_TOKEN,
        "ose_hrms_url": (
            f"https://casinoplus.sg.larksuite.com/base/{OSE_BASE_TOKEN}"
            f"?table={LEAVEOSE_TABLE_ID_CANONICAL}"
        ),
    }


def get_ose_leave_records_list() -> dict[str, Any]:
    """HRMS-synced OSE leave rows for webapp display (read-only; no approval workflow)."""
    token = get_tenant_access_token()
    rows: list[dict[str, str]] = []
    for it in _get_leave_display_raw(token):
        row = _admin_leave_row_from_bitable_item(it)
        if not row or not _is_ose_dutylist_leave_name(row["name"]):
            continue
        rows.append({k: v for k, v in row.items() if k != "record_id"})
    rows.sort(
        key=lambda r: (
            r.get("start_date") or "",
            r.get("end_date") or "",
            r.get("name") or "",
        ),
        reverse=True,
    )
    return {
        "ok": True,
        "items": rows,
        "meta": {
            **get_ose_leave_bitable_meta(),
            "source": "ose_hrms_leave_table",
        },
    }


def get_ose_leave_records_admin(*, scope: str = "display") -> dict[str, Any]:
    """
    Admin leave list (``dutyList.csv`` OSE only).

    ``scope=display`` (Admin ALL): **leaveose only** — ``OSE_HRMS_LEAVE_TABLE_ID`` / tblvoXE0hsPjgb0j.
    Does **not** read leave 全员 (``OSE_ALL_LEAVE_TABLE_ID`` / tblmHJHe12BCJRD8).

    ``scope=pending`` (Admin TODO): pending OSE form rows from ``OSE_LEAVE_TABLE_ID`` only.
    """
    scope_s = (scope or "display").strip().lower()
    if scope_s in ("merged", "all", "approval", "leave"):
        scope_s = "display"
    if scope_s not in ("display", "pending"):
        scope_s = "display"
    token = get_tenant_access_token()
    merged: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    display_items: list[dict[str, Any]] = []
    approval_items: list[dict[str, Any]] = []

    if scope_s == "display":
        display_items = _get_leave_display_raw(token)
        for it in display_items:
            row = _admin_leave_row_from_bitable_item(it)
            if not row or not _is_ose_dutylist_leave_name(row["name"]):
                continue
            row["pending"] = False
            merged[_leave_row_key_for_admin(row)] = row

    if scope_s == "pending":
        approval_items = _get_leave_approval_raw(token)
        for it in approval_items:
            row = _admin_leave_row_from_bitable_item(it)
            if not row or not _is_ose_dutylist_leave_name(row["name"]):
                continue
            pending = _is_pending_approval(
                _get_field_by_aliases(it.get("fields") or {}, ["Status", "Approval Status"])
            )
            reason = (row.get("reason") or "").strip()
            leave_id = (row.get("leave_id") or "").strip()
            if scope_s == "pending":
                if not pending:
                    continue
            elif not pending and not reason and not leave_id:
                continue
            row["pending"] = pending
            merged[_leave_row_key_for_admin(row)] = row

    rows = list(merged.values())
    rows.sort(
        key=lambda r: (
            r.get("start_date") or "",
            r.get("end_date") or "",
            r.get("name") or "",
        ),
        reverse=True,
    )
    source = {
        "display": "leaveose_table_only",
        "pending": "ose_leave_approval_pending_only",
    }[scope_s]
    return {
        "ok": True,
        "items": rows,
        "meta": {
            **get_ose_leave_bitable_meta(scope=scope_s),
            "display_table_raw_rows": len(display_items),
            "approval_table_raw_rows": len(approval_items),
            "items_after_ose_dutylist_filter": len(rows),
            "source": source,
        },
    }


def get_ose_offset_records_admin() -> dict[str, Any]:
    """Offset rows for admin approval (includes Bitable record_id)."""
    token = get_tenant_access_token()
    _, items = _get_bitable_raw_pair(token)
    rows: list[dict[str, str]] = []
    for it in items:
        f = it.get("fields") or {}
        req = _title_name(
            _field_text(_get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]))
        )
        exc = _title_name(
            _field_text(_get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]))
        )
        od = _bitable_field_original_date(f)
        xd = _bitable_field_exchange_date(f)
        rd = _bitable_field_request_date(f)
        approval = _record_approval_fields(f)
        rows.append(
            {
                "record_id": str(it.get("record_id") or "").strip(),
                "request_id": _field_text(_get_field_by_aliases(f, ["Request ID", "RequestID", "Request Id"])),
                "request_date": _format_yyyymmdd(rd),
                "request_person": req,
                "exchange_person": exc,
                "shift_type": _field_text(_get_field_by_aliases(f, ["Shift Type", "Shift"])).upper(),
                "original_date": _format_yyyymmdd(od),
                "exchange_date": _format_yyyymmdd(xd),
                "reason": _field_text(_get_field_by_aliases(f, ["Reason"])),
                "approval_status": approval["status"],
                "approver": approval["approver"],
                "approval_date": approval["approval_date"],
                "remarks": approval["remarks"],
                "pending": _is_pending_approval(_get_field_by_aliases(f, ["Approval Status", "Status"])),
            }
        )
    rows.sort(
        key=lambda r: (
            r.get("request_date") or "",
            r.get("original_date") or "",
            r.get("request_person") or "",
        ),
        reverse=True,
    )
    return {"ok": True, "items": rows}


def update_ose_leave_approval(
    *,
    record_id: str,
    status: str,
    approver: str,
    remarks: str = "",
    approval_date: Optional[date] = None,
    approver_open_id: str = "",
) -> dict[str, Any]:
    st = (status or "").strip().title()
    if st not in ("Approved", "Rejected"):
        raise ValueError("status must be Approved or Rejected")
    approver_s = _title_name(approver)
    if not approver_s:
        raise ValueError("approver is required")
    token = get_tenant_access_token()
    fields: dict[str, Any] = {
        "Status": st,
        "Approver": _approver_field_value(
            approver_s,
            token=token,
            approver_open_id=approver_open_id,
        ),
        "Approval Date": _bitable_date_ms(approval_date or date.today()),
        "Remarks": (remarks or "").strip(),
    }
    _bitable_update_record(token, OSE_LEAVE_TABLE_ID, record_id, fields)
    invalidate_ose_bitable_cache()
    return {"ok": True, "record_id": record_id, "status": st}


def update_ose_offset_approval(
    *,
    record_id: str,
    status: str,
    approver: str,
    remarks: str = "",
    approval_date: Optional[date] = None,
    approver_open_id: str = "",
) -> dict[str, Any]:
    st = (status or "").strip().title()
    if st not in ("Approved", "Rejected"):
        raise ValueError("status must be Approved or Rejected")
    approver_s = _title_name(approver)
    if not approver_s:
        raise ValueError("approver is required")
    token = get_tenant_access_token()
    fields: dict[str, Any] = {
        "Approval Status": st,
        "Approver": _approver_field_value(
            approver_s,
            token=token,
            approver_open_id=approver_open_id,
        ),
        "Approval Date": _bitable_date_ms(approval_date or date.today()),
        "Remarks": (remarks or "").strip(),
    }
    _bitable_update_record(token, OSE_OFFSET_TABLE_ID, record_id, fields)
    invalidate_ose_bitable_cache()
    _schedule_offset_duty_wiki_sync(record_id=record_id)
    sheet_out: dict[str, Any] = {}
    if st == "Approved":
        try:
            sheet_out = apply_approved_offset_shift_sheet_for_record(record_id)
        except Exception as exc:
            sheet_out = {"ok": False, "error": str(exc)}
            print(f"[ose_Duty] offset shift sheet apply failed for {record_id!r}: {exc!r}", flush=True)
    return {"ok": True, "record_id": record_id, "status": st, "shift_sheet": sheet_out}


def _admin_page_env() -> tuple[str, str]:
    app_token = (
        os.getenv("APPWEB_ADMINPAGETOKEN")
        or os.getenv("appweb_adminpagetoken")
        or ""
    ).strip()
    table_id = (
        os.getenv("APPWEB_ADMINPAGESHEETID")
        or os.getenv("appweb_adminpagesheetid")
        or ""
    ).strip()
    if not app_token or not table_id:
        raise RuntimeError("Admin page Bitable env is not configured")
    return app_token, table_id


def _admin_whologin_identity(v: Any) -> tuple[str, str]:
    for item in _person_field_items(v):
        pid = _person_item_open_id(item)
        if pid:
            return _title_name(_field_text(item)) or pid, pid
    return _title_name(_field_text(v)), ""


def _parse_webapp_admin_credential_row(record: dict[str, Any]) -> tuple[str, str, str, str] | None:
    f = record.get("fields") or {}
    who, open_id = _admin_whologin_identity(
        _get_field_by_aliases(f, ["whologin", "Who Login", "WhoLogin"])
    )
    login = _field_text(_get_field_by_aliases(f, ["ID", "Id"]))
    pw = _field_text(_get_field_by_aliases(f, ["PASSWORD", "Password"]))
    if not login or not pw:
        return None
    return who, login, pw, open_id


def _load_webapp_admin_credentials() -> list[tuple[str, str, str, str]]:
    app_token, table_id = _admin_page_env()
    token = get_tenant_access_token()
    records = _bitable_get_all_records(token, app_token, table_id)
    if not records:
        raise RuntimeError("Admin credential table has no rows")
    rows: list[tuple[str, str, str, str]] = []
    for record in records:
        parsed = _parse_webapp_admin_credential_row(record)
        if parsed:
            rows.append(parsed)
    if not rows:
        raise RuntimeError("Admin credential table has no valid ID/PASSWORD rows")
    return rows


def verify_webapp_admin_login(login_id: str, password: str) -> dict[str, str]:
    login_s = (login_id or "").strip()
    password_s = password or ""
    for who, expected_id, expected_pw, open_id in _load_webapp_admin_credentials():
        if login_s == expected_id and password_s == expected_pw:
            return {"who": who or login_s, "open_id": open_id}
    raise ValueError("Invalid admin ID or password")


def get_ose_offset_records_list() -> dict[str, Any]:
    """All offset rows for the submit-offset page (Request ID shown when present)."""
    token = get_tenant_access_token()
    _, items = _get_bitable_raw_pair(token)
    rows: list[dict[str, str]] = []
    for it in items:
        f = it.get("fields") or {}
        req = _title_name(
            _field_text(_get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]))
        )
        exc = _title_name(
            _field_text(_get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]))
        )
        od = _bitable_field_original_date(f)
        xd = _bitable_field_exchange_date(f)
        rd = _bitable_field_request_date(f)
        approval = _record_approval_fields(f)
        rows.append(
            {
                "request_id": _field_text(_get_field_by_aliases(f, ["Request ID", "RequestID", "Request Id"])),
                "request_date": _format_yyyymmdd(rd),
                "request_person": req,
                "exchange_person": exc,
                "shift_type": _field_text(_get_field_by_aliases(f, ["Shift Type", "Shift"])).upper(),
                "original_date": _format_yyyymmdd(od),
                "exchange_date": _format_yyyymmdd(xd),
                "reason": _field_text(_get_field_by_aliases(f, ["Reason"])),
                "approval_status": approval["status"],
                "approver": approval["approver"],
                "approval_date": approval["approval_date"],
                "remarks": approval["remarks"],
            }
        )
    rows.sort(
        key=lambda r: (
            r.get("request_date") or "",
            r.get("original_date") or "",
            r.get("request_person") or "",
        ),
        reverse=True,
    )
    return {"ok": True, "items": rows}


def parse_showoffset_command(text: str) -> Optional[tuple[int, int]]:
    """Return ``(year, month)`` for natural phrasing or the glued command.

    Accepts ``showoffset`` plus human talk like ``show offset``, ``show my offset``,
    ``view offset``, ``check offset``, optionally followed by a month
    (``show offset may`` / ``show offset 5``) and trailing words like
    ``calendar``/``schedule`` (``show offset calendar``).
    """
    s = (text or "").strip()
    if s.startswith("/"):
        s = s[1:].lstrip()
    m = re.match(
        r"^(?:show|showoffset|view|check|see|display)\s*(?:me\s+)?(?:my\s+|the\s+|our\s+)?offsets?(?:\s+(.+?))?\s*$",
        s,
        re.I,
    )
    if not m:
        return None
    return _resolve_offset_command_month(m.group(1) or "")


def _resolve_offset_command_month(arg: str) -> tuple[int, int]:
    """Month argument of an offset command → ``(year, month)``; empty = this month.

    Accepts ``august`` / ``8`` / ``this month`` / ``next month`` / ``last month``,
    with decorations like ``for``/``calendar`` stripped. Raises ``ValueError`` on a
    month it cannot read, so the caller can answer with the reason.
    """
    today = date.today()
    arg = (arg or "").strip()
    # Drop trailing decorations like "calendar"/"schedule"/"list".
    arg = re.sub(r"\b(?:calendar|schedule|list|summary|sheet|table)\b", "", arg, flags=re.I).strip()
    # Strip leading "for"/"in" (e.g. "for this month", "in June").
    arg = re.sub(r"^(?:for|in)\s+", "", arg, flags=re.I).strip()
    if not arg:
        return today.year, today.month
    arg_low = arg.lower()
    if arg_low in ("this month", "current month"):
        return today.year, today.month
    if arg_low == "next month":
        idx = today.year * 12 + (today.month - 1) + 1
        return idx // 12, (idx % 12) + 1
    if arg_low == "last month":
        idx = today.year * 12 + (today.month - 1) - 1
        return idx // 12, (idx % 12) + 1
    if re.fullmatch(r"\d{1,2}", arg):
        month = int(arg)
        if month < 1 or month > 12:
            raise ValueError("month must be 1–12")
        return today.year, month
    for name, num in MONTH_MAP.items():
        if name.lower() == arg.lower():
            return today.year, num
    raise ValueError(f"Unknown month {arg!r}. Use a month name or number (1–12).")


def parse_myoffset_command(text: str) -> Optional[tuple[int, int]]:
    """``myoffset`` / ``/myoffset august`` → ``(year, month)``; ``None`` if not it."""
    s = (text or "").strip()
    if s.startswith("/"):
        s = s[1:].lstrip()
    # Glued form only — "my offset" with a space stays a natural-language lookup
    # (``show my offset`` is the requester's own calendar, not the MY OSE table).
    m = re.match(r"^myoffsets?(?:\s+(.+?))?\s*$", s, re.I)
    if not m:
        return None
    return _resolve_offset_command_month(m.group(1) or "")


def _showoffset_my_person(name: str) -> Optional[str]:
    """Display name when ``name`` is one of the MY OSE people, else ``None``."""
    nm = _title_name(name)
    if not nm:
        return None
    for display, aliases in OSE_SHOWOFFSET_MY_ROSTER:
        if _names_same_person(display, nm):
            return display
        for alias in aliases:
            if _names_same_person(alias, nm):
                return display
    return None


def _showoffset_my_index(name: str) -> int:
    """Position on the MY roster (sort order); 999 for everyone else."""
    display = _showoffset_my_person(name)
    if not display:
        return 999
    for i, (allowed, _aliases) in enumerate(OSE_SHOWOFFSET_MY_ROSTER):
        if allowed == display:
            return i
    return 999


def _showoffset_canonical_name(name: str) -> Optional[str]:
    return _showoffset_my_person(name)


def _add_showoffset_days(
    by_person: dict[str, dict[str, set[int]]],
    person: str,
    orig_day: int,
    exc_day: int,
) -> None:
    slot = by_person.setdefault(person, {"orig": set(), "exc": set()})
    slot["orig"].add(orig_day)
    slot["exc"].add(exc_day)


def _showoffset_display_name(name: str) -> str:
    canon = _showoffset_canonical_name(name)
    return canon or _title_name(name)


def _showoffset_pair_sort_key(req: str, exc: str) -> tuple[Any, ...]:
    return (
        _showoffset_my_index(req),
        _showoffset_my_index(exc),
        req.lower(),
        exc.lower(),
    )


def _offset_row_touches_month(od: date, xd: date, year: int, month: int) -> bool:
    """True when original or exchange date falls in the requested month."""
    return (od.year, od.month) == (year, month) or (xd.year, xd.month) == (year, month)


def _showoffset_person_key(name: str) -> Optional[str]:
    """Prefer roster canonical name; keep unknown names instead of dropping rows."""
    canon = _showoffset_canonical_name(name)
    if canon:
        return canon
    nm = _title_name(name)
    return nm or None


def _offset_date_label(d: date) -> str:
    """Offset day as ``date/month`` — spells out the month a swap lands in."""
    return f"{d.day}/{d.month}"


def _collect_offset_month_pair_lines(
    year: int,
    month: int,
    *,
    items: Optional[list[dict[str, Any]]] = None,
    involved_person: Optional[str] = None,
    request_person_only: Optional[str] = None,
) -> list[str]:
    """Build display lines: ``Man Chung 20/8, 21/8 --> Si Yew 15/8, 17/8`` (per swap pair)."""
    if month < 1 or month > 12:
        raise ValueError("month must be 1–12")
    if items is None:
        token = get_tenant_access_token()
        _, items = _get_bitable_raw_pair(token)
    person_raw = involved_person or request_person_only
    filter_person = _showoffset_person_key(person_raw or "") if person_raw else None
    # Whole dates, not day numbers — a line must read 28/8 --> 1/9, never 28 --> 1.
    pairs: dict[tuple[str, str], dict[str, set[date]]] = {}
    for it in items:
        f = it.get("fields") or {}
        # Only approved offsets are real swaps — skip pending / rejected rows
        # (same rule as _extract_offset_lines_for_date).
        if not _is_approved(_get_field_by_aliases(f, ["Approval Status", "Status"])):
            continue
        req = _title_name(
            _field_text(_get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]))
        )
        exc = _title_name(
            _field_text(_get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]))
        )
        od = _bitable_field_original_date(f)
        xd = _bitable_field_exchange_date(f)
        if not od or not xd:
            continue
        if not _offset_row_touches_month(od, xd, year, month):
            continue
        req_person = _showoffset_person_key(req)
        exc_person = _showoffset_person_key(exc)
        if not req_person or not exc_person:
            continue
        if filter_person and not (
            _names_same_person(req_person, filter_person)
            or _names_same_person(exc_person, filter_person)
        ):
            continue
        key = (req_person, exc_person)
        slot = pairs.setdefault(key, {"orig": set(), "exc": set()})
        slot["orig"].add(od)
        slot["exc"].add(xd)

    lines: list[str] = []
    for (req_p, exc_p), days in sorted(pairs.items(), key=lambda kv: _showoffset_pair_sort_key(kv[0][0], kv[0][1])):
        orig_s = ", ".join(_offset_date_label(d) for d in sorted(days["orig"]))
        exc_s = ", ".join(_offset_date_label(d) for d in sorted(days["exc"]))
        lines.append(f"{_showoffset_display_name(req_p)} {orig_s} --> {_showoffset_display_name(exc_p)} {exc_s}")
    return lines


def _collect_offset_range_my_moves(
    start: date,
    end: date,
    *,
    items: Optional[list[dict[str, Any]]] = None,
) -> list[tuple[str, list[tuple[date, date]]]]:
    """MY OSE offsets touching ``start``–``end`` as ``[(person, [(moved off, moved to)])]``.

    A move is in range when **either** of its two dates falls inside the window, and
    both dates are then reported — so a swap that reaches out of the window still
    shows where it lands. People come in roster order, each person's moves in date
    order. A swap counts for both sides — the requester moves ``original ->
    exchange``, the exchange person the other way — so a swap between two MY people
    appears under both, and a swap with a non-MY colleague appears under the MY side
    only. Exchange person = Myself is one move, not two. Rows duplicated in the
    table collapse to one move.
    """
    if items is None:
        token = get_tenant_access_token()
        _, items = _get_bitable_raw_pair(token)
    by_person: dict[str, set[tuple[date, date]]] = {}
    for it in items:
        f = it.get("fields") or {}
        # Only approved offsets are real swaps — skip pending / rejected rows
        # (same rule as _collect_offset_month_pair_lines).
        if not _is_approved(_get_field_by_aliases(f, ["Approval Status", "Status"])):
            continue
        req = _field_text(
            _get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"])
        )
        exc = _field_text(_get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]))
        od = _bitable_field_original_date(f)
        xd = _bitable_field_exchange_date(f)
        if not od or not xd:
            continue
        if not (start <= od <= end or start <= xd <= end):
            continue
        req_my = _showoffset_my_person(req)
        exc_my = _showoffset_my_person(exc)
        if req_my:
            by_person.setdefault(req_my, set()).add((od, xd))
        if exc_my and exc_my != req_my:
            by_person.setdefault(exc_my, set()).add((xd, od))
    return [
        (person, sorted(by_person[person]))
        for person in sorted(by_person, key=lambda n: (_showoffset_my_index(n), n.lower()))
    ]


def _collect_offset_month_my_moves(
    year: int,
    month: int,
    *,
    items: Optional[list[dict[str, Any]]] = None,
) -> list[tuple[str, list[tuple[date, date]]]]:
    """:func:`_collect_offset_range_my_moves` over one calendar month."""
    if month < 1 or month > 12:
        raise ValueError("month must be 1–12")
    _, last = calendar.monthrange(year, month)
    return _collect_offset_range_my_moves(
        date(year, month, 1), date(year, month, last), items=items
    )


def next_week_range(ref: Optional[date] = None) -> tuple[date, date]:
    """Monday–Sunday of the week AFTER ``ref`` (weekend included).

    Friday 14/08 → ``(17/08, 23/08)``. Used by the Friday push so the team sees the
    whole coming week, whichever weekday the job actually fires on.
    """
    d = ref or date.today()
    monday_this_week = d - timedelta(days=d.weekday())
    start = monday_this_week + timedelta(days=7)
    return start, start + timedelta(days=6)


def _collect_offset_month_my_lines(
    year: int,
    month: int,
    *,
    items: Optional[list[dict[str, Any]]] = None,
) -> list[str]:
    """Approver listing — **one line per person**, MY OSE only, name and days only::

        Augustine Si Yew 1/8, 2/8, 7/8, 23/8 --> 11/8, 20/8, 21/8, 21/8
        Jun Chen 28/8, 29/8 --> 1/9, 2/9
        Kheng Kwan 21/8 --> 23/8

    Every approved offset a person has that month lands on their single line: days
    they move off on the left, the day each one moves to in the same position on
    the right (``7/8 --> 21/8`` and ``23/8 --> 21/8`` above are two separate offsets,
    so 21/8 is listed twice). Days are never sorted apart from their partner, and
    each carries its month so a swap into the next month is unambiguous.
    """
    lines: list[str] = []
    for person, moves in _collect_offset_month_my_moves(year, month, items=items):
        from_s = ", ".join(_offset_date_label(a) for a, _b in moves)
        to_s = ", ".join(_offset_date_label(b) for _a, b in moves)
        lines.append(f"{person} {from_s} --> {to_s}")
    return lines


def _my_offset_table_elements(
    moves_by_person: list[tuple[str, list[tuple[date, date]]]],
    *,
    empty_note: str,
) -> list[dict[str, Any]]:
    """Name / Original Date / Exchange Date table — **one row per person**.

    All of a person's offsets are comma-joined in the two date columns, e.g.
    ``Augustine Si Yew | 1/8, 2/8, 7/8, 23/8 | 20/7, 21/7, 11/8, 21/8``. Position
    *i* in Original Date pairs with position *i* in Exchange Date — the columns are
    ordered together, never sorted apart (same rule as the MY Offset text lines).
    """
    rows: list[dict[str, str]] = []
    offsets = 0
    for person, moves in moves_by_person:
        offsets += len(moves)
        rows.append(
            {
                "person": person,
                "original": ", ".join(_offset_date_label(a) for a, _b in moves),
                "exchange": ", ".join(_offset_date_label(b) for _a, b in moves),
            }
        )
    if not rows:
        return [{"tag": "div", "text": {"tag": "plain_text", "content": empty_note}}]
    return [
        # The table pages at page_size, so state the totals above it — a reader must
        # never mistake page 1 for everything there is.
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**{len(rows)}** person(s) · **{offsets}** offset(s) — MY OSE.",
            },
        },
        {
            "tag": "table",
            "page_size": 10,
            # A cell can hold several dates — "low" would clip the longer ones.
            "row_height": "middle",
            "header_style": {
                "text_align": "left",
                "text_size": "normal",
                "background_style": "grey",
                "text_color": "default",
                "bold": True,
                "lines": 1,
            },
            "columns": [
                {
                    "name": "person",
                    "display_name": "Name",
                    "data_type": "text",
                    "horizontal_align": "left",
                },
                {
                    "name": "original",
                    "display_name": "Original Date",
                    "data_type": "text",
                    "horizontal_align": "left",
                },
                {
                    "name": "exchange",
                    "display_name": "Exchange Date",
                    "data_type": "text",
                    "horizontal_align": "left",
                },
            ],
            "rows": rows,
        },
    ]


def build_ose_myoffset_card(
    year: int,
    month: int,
    *,
    items: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """``myoffset`` card — one month of MY OSE offsets as a table, one row per person."""
    month_label = date(year, month, 1).strftime("%B")
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": f"MY Offset {month_label}"},
        },
        "body": {
            "elements": _my_offset_table_elements(
                _collect_offset_month_my_moves(year, month, items=items),
                empty_note=f"No MY OSE offsets in {month_label}.",
            )
        },
    }


def build_ose_weekly_myoffset_card(
    start: date,
    end: date,
    *,
    items: Optional[list[dict[str, Any]]] = None,
    mention_open_ids: tuple[str, ...] = (),
    greeting: str = "Hi",
) -> dict[str, Any]:
    """Weekly push card — MY OSE offsets touching ``start``–``end``.

    Same table as ``myoffset``, titled with the week and led by a greeting that
    @-mentions ``mention_open_ids``. An offset counts when its original **or** its
    exchange date falls in the week, weekend included.
    """
    span = f"{start.strftime('%d/%m')} - {end.strftime('%d/%m')}"
    ats = " ".join(f"<at id={oid}></at>" for oid in mention_open_ids if (oid or "").strip())
    elements: list[dict[str, Any]] = []
    if ats:
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"{greeting} {ats}"}})
    elements.extend(
        _my_offset_table_elements(
            _collect_offset_range_my_moves(start, end, items=items),
            empty_note=f"No MY OSE offsets for {span}.",
        )
    )
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {
                "tag": "plain_text",
                "content": f"MY Offset for next week {span}",
            },
        },
        "body": {"elements": elements},
    }


def _offset_all_and_my_sections(
    year: int,
    month: int,
    *,
    items: Optional[list[dict[str, Any]]] = None,
) -> list[str]:
    """The approver block: **All offsets** (whole team) then **MY Offset** below it.

    Empty when the month has no offsets at all, so the caller can print its own
    "nothing this month" line. Both sections read the same snapshot of rows.
    """
    if items is None:
        token = get_tenant_access_token()
        _, items = _get_bitable_raw_pair(token)
    all_lines = _collect_offset_month_pair_lines(year, month, items=items)
    if not all_lines:
        return []
    out = ["**All offsets**", ""]
    out.extend(all_lines)
    my_lines = _collect_offset_month_my_lines(year, month, items=items)
    out.extend(["", "**MY Offset**", ""])
    out.extend(my_lines or ["_No MY OSE offsets this month._"])
    return out


def _collect_offset_month_summary(
    year: int,
    month: int,
    *,
    items: Optional[list[dict[str, Any]]] = None,
) -> dict[str, tuple[list[int], list[int]]]:
    if month < 1 or month > 12:
        raise ValueError("month must be 1–12")
    if items is None:
        token = get_tenant_access_token()
        _, items = _get_bitable_raw_pair(token)
    by_person: dict[str, dict[str, set[int]]] = {}
    for it in items:
        f = it.get("fields") or {}
        # Only approved offsets are real swaps — skip pending / rejected rows
        # (same rule as _extract_offset_lines_for_date).
        if not _is_approved(_get_field_by_aliases(f, ["Approval Status", "Status"])):
            continue
        req = _title_name(
            _field_text(_get_field_by_aliases(f, ["Request Person", "Requester", "Requester Person", "Name"]))
        )
        exc = _title_name(
            _field_text(_get_field_by_aliases(f, ["Exchange Person", "Replacement", "Swap Person"]))
        )
        od = _bitable_field_original_date(f)
        xd = _bitable_field_exchange_date(f)
        if not od or not xd:
            continue
        if not _offset_row_touches_month(od, xd, year, month):
            continue
        req_person = _showoffset_person_key(req)
        if req_person:
            _add_showoffset_days(by_person, req_person, od.day, xd.day)
        exc_person = _showoffset_person_key(exc)
        if exc_person:
            _add_showoffset_days(by_person, exc_person, xd.day, od.day)
    out: dict[str, tuple[list[int], list[int]]] = {}
    for person, days in by_person.items():
        out[person] = (sorted(days["orig"]), sorted(days["exc"]))
    return out


def build_ose_showoffset_card(
    year: int,
    month: int,
    *,
    involved_person: Optional[str] = None,
    include_all_team: bool = False,
    request_person_only: Optional[str] = None,
) -> dict[str, Any]:
    """
    Offset calendar card.

    - Approver only → **All offsets** + **MY Offset**
    - Requester only → rows where they are requester **or** exchange person
    - Both roles → **Your offsets** section on top of the approver sections

    **All offsets** is the whole OSE team, one line per swap pair, unchanged.
    **MY Offset** sits under it and condenses the MY OSE members to one line each
    — see :func:`_collect_offset_month_my_lines`.
    """
    person = involved_person or request_person_only
    month_label = date(year, month, 1).strftime("%B")
    lines = [f"**{month_label}**", ""]

    if include_all_team and person:
        mine = _collect_offset_month_pair_lines(year, month, involved_person=person)
        who = _showoffset_display_name(person)
        lines.append(f"**Your offsets** ({who})")
        lines.append("")
        if mine:
            lines.extend(mine)
        else:
            lines.append("_No offset requests involving you this month._")
        lines.append("")
        lines.extend(
            _offset_all_and_my_sections(year, month)
            or ["**All offsets**", "", "_No offset requests this month._"]
        )
    elif person:
        pair_lines = _collect_offset_month_pair_lines(year, month, involved_person=person)
        if pair_lines:
            lines.extend(pair_lines)
        else:
            lines.append("No offset requests this month involving you.")
    else:
        lines.extend(
            _offset_all_and_my_sections(year, month) or ["No offset requests this month."]
        )

    content = "\n".join(lines).strip()
    title = f"OSE offset — {month_label} {year}"
    if include_all_team and person:
        who = _showoffset_display_name(person)
        if who:
            title = f"OSE offset — {who} (yours + all) — {month_label} {year}"
    elif person:
        who = _showoffset_display_name(person)
        if who:
            title = f"OSE offset — {who} — {month_label} {year}"
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": title},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": content},
                }
            ]
        },
    }


def get_ose_leave_names_calendar(year: int, month: int) -> dict[str, Any]:
    """Per-day leave names for a month (HRMS display table; OSE roster only)."""
    if month < 1 or month > 12:
        raise ValueError("month must be 1–12")
    token = get_tenant_access_token()
    items = _get_leave_display_raw(token)
    rows = _leave_rows_for_calendar(items)
    month_start = date(year, month, 1)
    _, last = calendar.monthrange(year, month)
    month_end = date(year, month, last)
    days: dict[str, list[str]] = {}
    for d in range(1, last + 1):
        days[str(d)] = []
    for row in rows:
        st: date = row["start"]
        ed: date = row["end"]
        if ed < month_start or st > month_end:
            continue
        cur = max(st, month_start)
        end = min(ed, month_end)
        while cur <= end:
            key = str(cur.day)
            if row["name"] not in days[key]:
                days[key].append(row["name"])
            cur += timedelta(days=1)
    for key in days:
        days[key].sort(key=lambda x: x.lower())
    return {"ok": True, "year": year, "month": month, "days": days}


def _parse_iso_date(raw: str) -> date:
    s = (raw or "").strip()
    if not s:
        raise ValueError("date is required")
    try:
        return date.fromisoformat(s)
    except ValueError as e:
        raise ValueError(f"invalid date {raw!r} (use YYYY-MM-DD)") from e


def _parse_submit_day_month(*, month: Any, day: Any, year: Optional[int] = None) -> date:
    try:
        m = int(month)
        d = int(day)
    except (TypeError, ValueError) as e:
        raise ValueError("month and day are required") from e
    y = int(year) if year is not None else date.today().year
    if m < 1 or m > 12:
        raise ValueError("month must be 1–12")
    _, last = calendar.monthrange(y, m)
    if d < 1 or d > last:
        raise ValueError(f"invalid day {d} for month {m}/{y}")
    return date(y, m, d)


def _resolve_submit_date(*, month: Any, day: Any, raw_date: str = "", year: Optional[int] = None) -> date:
    m_s = "" if month is None else str(month).strip()
    d_s = "" if day is None else str(day).strip()
    if m_s and d_s:
        return _parse_submit_day_month(month=m_s, day=d_s, year=year)
    return _parse_iso_date(raw_date)


def submit_ose_leave(
    *,
    name: str,
    leave_type: str,
    start_date: date,
    end_date: date,
    reason: str,
) -> dict[str, Any]:
    if start_date > end_date:
        raise ValueError("Start Date must be on or before End Date")
    nm = _title_name(name)
    if nm not in OSE_LEAVE_FORM_NAMES:
        raise ValueError(f"Unknown name {name!r}")
    lt = (leave_type or "").strip()
    if lt not in OSE_LEAVE_TYPES:
        raise ValueError(f"Unknown leave type {leave_type!r}")
    reason_s = (reason or "").strip()
    if not reason_s:
        raise ValueError("Reason is required")
    token = get_tenant_access_token()
    fields: dict[str, Any] = {
        "Name": _person_field_value(nm, token=token),
        "Leave Type": lt,
        "Start Date": _bitable_date_ms(start_date),
        "End Date": _bitable_date_ms(end_date),
        "Reason": reason_s,
    }
    res = _bitable_create_record(token, OSE_LEAVE_TABLE_ID, fields)
    invalidate_ose_bitable_cache()
    return {"ok": True, "record_id": (res.get("data") or {}).get("record", {}).get("record_id")}


def submit_ose_offset(
    *,
    request_person: str,
    exchange_person: str,
    shift_type: str,
    original_date: date,
    exchange_date: date,
    reason: str,
) -> dict[str, Any]:
    req = _canonical_roster_form_name(request_person)
    if not req:
        raise ValueError(f"Unknown request person {request_person!r}")
    exc = resolve_offset_exchange_person(exchange_person, request_person=req)
    st = (shift_type or "").strip().upper()
    if st not in OSE_SHIFT_TYPES:
        raise ValueError("Shift Type must be N or D")
    reason_s = (reason or "").strip()
    if not reason_s:
        raise ValueError("Reason is required")
    validate_offset_swap_duty_dates(
        request_person=req,
        exchange_person=exc,
        shift_type=st,
        original_date=original_date,
        exchange_date=exchange_date,
    )
    token = get_tenant_access_token()
    today = date.today()
    fields: dict[str, Any] = {
        "Request Person": _offset_person_field_value(req, token=token),
        "Exchange Person": _offset_person_field_value(exc, token=token),
        "Shift Type": st,
        "Original Date": _bitable_date_ms(original_date),
        "Exchange Date": _bitable_date_ms(exchange_date),
        "Request Date": _bitable_date_ms(today),
        "Reason": reason_s,
    }
    res = _bitable_create_record(token, OSE_OFFSET_TABLE_ID, fields)
    invalidate_ose_bitable_cache()
    record_id = (res.get("data") or {}).get("record", {}).get("record_id")
    rid = str(record_id or "").strip()
    if rid:
        _schedule_offset_duty_wiki_sync(record_id=rid)
        try:
            import offsetleave as ol

            ol.notify_offset_approvers_for_record(
                rid,
                fallback_row={
                    "record_id": rid,
                    "request_date": today.isoformat(),
                    "request_person": req,
                    "exchange_person": exc,
                    "shift_type": st,
                    "original_date": original_date.isoformat(),
                    "exchange_date": exchange_date.isoformat(),
                    "reason": reason_s,
                    "approval_status": "Pending",
                    "pending": True,
                },
            )
        except Exception as exc:
            print(f"[ose_Duty] offset approver notify failed: {exc!r}", flush=True)
    return {"ok": True, "record_id": record_id}


def get_ose_offset_record_admin_row(record_id: str) -> dict[str, Any]:
    """Single offset row (same shape as :func:`get_ose_offset_records_admin` items)."""
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    for r in get_ose_offset_records_admin().get("items") or []:
        if str(r.get("record_id") or "").strip() == rid:
            return dict(r)
    raise KeyError(f"unknown offset record {rid!r}")


def delete_ose_offset_record(*, record_id: str, skip_cache_invalidate: bool = False) -> dict[str, Any]:
    """Delete an offset Bitable row (caller must enforce pending + ownership)."""
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    # Tell the guard this deletion is sanctioned (the retention purge) BEFORE
    # the row disappears, otherwise the next poll would restore it.
    try:
        mark_offset_delete_authorized(rid)
    except Exception as exc:  # noqa: BLE001 — never block a legitimate delete
        print(f"[ose_Duty] offset guard authorize failed for {rid!r}: {exc!r}", flush=True)
    shift_revert: dict[str, Any] = {}
    try:
        shift_revert = revert_approved_offset_shift_sheet_for_record(rid)
    except KeyError:
        shift_revert = {"ok": False, "record_id": rid, "skipped": "row_not_found"}
    except Exception as exc:
        shift_revert = {"ok": False, "record_id": rid, "error": str(exc)}
        print(f"[ose_Duty] shift sheet revert failed for {rid!r}: {exc!r}", flush=True)
    token = get_tenant_access_token()
    url = (
        f"https://open.larksuite.com/open-apis/bitable/v1/apps/"
        f"{OSE_BASE_TOKEN}/tables/{OSE_OFFSET_TABLE_ID}/records/{rid}"
    )
    headers = {"Authorization": f"Bearer {token}"}
    res = _lark_request(
        "delete",
        url,
        headers=headers,
        params={"user_id_type": "open_id"},
        timeout=30,
    ).json()
    if res.get("code") != 0:
        raise RuntimeError(f"Bitable delete failed: {res}")
    if not skip_cache_invalidate:
        invalidate_ose_bitable_cache()
    _schedule_offset_duty_wiki_sync(record_id=rid, delete=True)
    return {"ok": True, "record_id": rid, "shift_sheet_revert": shift_revert}


def _calendar_months_after(d: date, ref: date) -> int:
    """
    Whole calendar months from ``d``'s month to ``ref``'s month (same month → 0).
    Examples: Mar→May=2, May→June=1, May→July=2.
    """
    return (ref.year - d.year) * 12 + (ref.month - d.month)


def _offset_row_original_exchange_dates(fields: dict[str, Any]) -> tuple[Optional[date], Optional[date]]:
    """
    Swap dates for retention / purge. Must match :func:`get_ose_offset_records_admin`
    (do **not** treat ``Request Date`` as Original Date — that is usually submission time
    and would skip purging old March swaps while the row still displays correctly).
    """
    f = fields or {}
    od = _bitable_field_original_date(f)
    xd = _bitable_field_exchange_date(f)
    return od, xd


# ---------------------------------------------------------------------------
# Offset row guard — offset rows must not be deleted by hand.
#
# Every poll mirrors the live offset rows here. A row that disappears WITHOUT the
# bot having deleted it (see :func:`mark_offset_delete_authorized`, called by
# ``delete_ose_offset_record`` — the single sanctioned delete path, now only the
# retention purge) was removed straight from the Base. That row is restored from
# the mirror and the approvers are told who to talk to.
#
# The mirror also carries the fields needed to revert the duty sheet, which the
# old path could not do once the row was gone (it tried to re-read the deleted
# row and gave up with "missing_snapshot", leaving ``*`` marks stuck forever).
# ---------------------------------------------------------------------------
_OFFSET_GUARD_PATH = os.path.join(_OSE_DIR, "offset_row_guard.json")
_OFFSET_GUARD_LOCK = threading.Lock()
# Keep authorised-delete markers this long, so a slow poll still sees them.
_OFFSET_GUARD_AUTH_TTL_SEC = 7 * 24 * 3600


def _load_offset_guard_state() -> dict[str, Any]:
    empty = {"rows": {}, "authorized": {}}
    try:
        with open(_OFFSET_GUARD_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return dict(empty)
    if not isinstance(data, dict):
        return dict(empty)
    rows = data.get("rows") if isinstance(data.get("rows"), dict) else {}
    auth = data.get("authorized") if isinstance(data.get("authorized"), dict) else {}
    return {"rows": dict(rows), "authorized": dict(auth)}


def _save_offset_guard_state(state: dict[str, Any]) -> None:
    tmp = _OFFSET_GUARD_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2, default=str)
        fh.write("\n")
    os.replace(tmp, _OFFSET_GUARD_PATH)


def mark_offset_delete_authorized(record_id: str) -> None:
    """Record that the BOT is deleting this row, so the guard won't restore it."""
    rid = (record_id or "").strip()
    if not rid:
        return
    with _OFFSET_GUARD_LOCK:
        state = _load_offset_guard_state()
        state.setdefault("authorized", {})[rid] = datetime.now().isoformat(timespec="seconds")
        _save_offset_guard_state(state)


def _offset_guard_writable_fields(fields: dict[str, Any]) -> dict[str, Any]:
    """Drop values Bitable computes itself; those cannot be written back."""
    out: dict[str, Any] = {}
    for k, v in (fields or {}).items():
        if v is None or v == "" or v == []:
            continue
        name = str(k).strip().lower()
        if name in ("record id", "record_id", "created time", "created by", "auto number"):
            continue
        # Person/link cells come back as rich objects; keep only plain values and
        # the {text/link} shapes Bitable accepts on write.
        if isinstance(v, (dict, list)) and name not in ("request person", "exchange person"):
            continue
        out[k] = v
    return out


def scan_restore_directly_deleted_offsets(
    *, notify: Optional[Any] = None
) -> dict[str, Any]:
    """Mirror live offsets; restore any row deleted straight from the Base.

    Returns counts plus ``restored`` details so the caller can notify approvers.
    Never restores a row the bot itself deleted (the retention purge).
    """
    token = get_tenant_access_token()
    try:
        items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_OFFSET_TABLE_ID)
    except Exception as exc:  # noqa: BLE001 — a fetch blip must not look like mass deletion
        print(f"[ose_Duty] offset guard skipped (fetch failed): {exc!r}", flush=True)
        return {"ok": False, "error": str(exc), "restored": [], "checked": 0}
    live: dict[str, dict[str, Any]] = {}
    for it in items:
        rid = str(it.get("record_id") or "").strip()
        if rid:
            live[rid] = dict(it.get("fields") or {})

    with _OFFSET_GUARD_LOCK:
        state = _load_offset_guard_state()
        rows: dict[str, Any] = dict(state.get("rows") or {})
        auth: dict[str, Any] = dict(state.get("authorized") or {})
        known = set(rows)
        first_run = not known

        missing = [rid for rid in known if rid not in live]
        restored: list[dict[str, Any]] = []
        errors: list[str] = []
        for rid in missing:
            snap = rows.get(rid) or {}
            if rid in auth:  # bot-initiated delete → expected, just forget it
                rows.pop(rid, None)
                continue
            fields = _offset_guard_writable_fields(snap.get("fields") or {})
            if not fields:
                rows.pop(rid, None)
                continue
            try:
                res = _bitable_create_record(token, OSE_OFFSET_TABLE_ID, fields)
                new_id = str(((res.get("data") or {}).get("record") or {}).get("record_id") or "").strip()
                restored.append(
                    {
                        "old_record_id": rid,
                        "new_record_id": new_id,
                        "fields": fields,
                        "summary": snap.get("summary") or "",
                    }
                )
                rows.pop(rid, None)
                if new_id:
                    rows[new_id] = {"fields": fields, "summary": snap.get("summary") or ""}
                print(
                    f"[ose_Duty] offset guard RESTORED {rid} → {new_id} "
                    f"({snap.get('summary') or 'offset'})",
                    flush=True,
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{rid}: {exc}")
                print(f"[ose_Duty] offset guard restore failed {rid}: {exc!r}", flush=True)

        # Refresh the mirror from what is live now.
        for rid, fields in live.items():
            rows[rid] = {
                "fields": fields,
                "summary": _offset_guard_summary(fields),
            }
        cutoff = datetime.now() - timedelta(seconds=_OFFSET_GUARD_AUTH_TTL_SEC)
        for rid, ts in list(auth.items()):
            if rid in live:
                continue
            try:
                if datetime.fromisoformat(str(ts)) < cutoff:
                    auth.pop(rid, None)
            except ValueError:
                auth.pop(rid, None)
        _save_offset_guard_state({"rows": rows, "authorized": auth})

    if first_run:
        print(f"[ose_Duty] offset guard seeded with {len(live)} row(s)", flush=True)
        return {"ok": True, "seeded": len(live), "restored": [], "checked": len(live)}
    if restored and notify is not None:
        try:
            notify(restored)
        except Exception as exc:  # noqa: BLE001
            print(f"[ose_Duty] offset guard notify failed: {exc!r}", flush=True)
    return {
        "ok": not errors,
        "checked": len(live),
        "restored": restored,
        "errors": errors,
    }


def _offset_guard_summary(fields: dict[str, Any]) -> str:
    """Short human label for guard logs / approver notices."""
    f = fields or {}
    who = _field_text(_get_field_by_aliases(f, ["Request Person", "Requester", "Name"]))
    od_d = _bitable_field_original_date(f)
    xd_d = _bitable_field_exchange_date(f)
    st = _field_text(_get_field_by_aliases(f, ["Shift Type", "Shift"]))
    bits = [b for b in [who, st] if b]
    when = " → ".join([d.isoformat() for d in (od_d, xd_d) if d])
    if when:
        bits.append(when)
    return " · ".join(bits) or "offset row"


def purge_stale_ose_offset_bitable_rows(*, ref_date: Optional[date] = None) -> dict[str, Any]:
    """
    Delete offset Bitable rows by **calendar month**, not by day within the month.

    A row is removed only if **both** Original Date and Exchange Date fall in months
    that are **at least two whole calendar months** before ``ref_date``'s month
    (``ref``'s day-of-month is ignored).

    Examples: on **any day in May** (e.g. May 16), **all of March** (and earlier)
    qualifies — e.g. 26–30 Mar is deleted. On **any day in June**, May rows are
    **not** removed (only one month behind). On **any day in July**, May rows are
    removed.
    """
    ref = ref_date or date.today()
    token = get_tenant_access_token()
    items = _bitable_get_all_records(token, OSE_BASE_TOKEN, OSE_OFFSET_TABLE_ID)
    to_delete: list[str] = []
    for it in items:
        f = it.get("fields") or {}
        od, xd = _offset_row_original_exchange_dates(f)
        if not od or not xd:
            continue
        if _calendar_months_after(od, ref) >= 2 and _calendar_months_after(xd, ref) >= 2:
            rid = str(it.get("record_id") or "").strip()
            if rid:
                to_delete.append(rid)
    deleted: list[str] = []
    errors: list[str] = []
    for rid in to_delete:
        try:
            delete_ose_offset_record(record_id=rid, skip_cache_invalidate=True)
            deleted.append(rid)
        except Exception as exc:
            errors.append(f"{rid}: {exc}")
    if deleted or errors:
        invalidate_ose_bitable_cache()
    return {
        "ok": not errors,
        "ref_date": ref.isoformat(),
        "scanned": len(items),
        "eligible": len(to_delete),
        "deleted": len(deleted),
        "errors": errors,
    }


def update_ose_offset_request(
    *,
    record_id: str,
    request_person: str,
    exchange_person: str,
    shift_type: str,
    original_date: date,
    exchange_date: date,
    reason: str,
) -> dict[str, Any]:
    """Update a **pending** offset row (exchange/shift/dates/reason only)."""
    row = get_ose_offset_record_admin_row(record_id)
    if not row.get("pending"):
        raise ValueError("Only pending offset requests can be edited.")
    req = _canonical_roster_form_name(request_person)
    row_req = _canonical_roster_form_name(str(row.get("request_person") or ""))
    if not req or row_req != req:
        raise ValueError("This offset request does not belong to you.")
    exc = resolve_offset_exchange_person(exchange_person, request_person=req)
    st = (shift_type or "").strip().upper()
    if st not in OSE_SHIFT_TYPES:
        raise ValueError("Shift Type must be N or D")
    reason_s = (reason or "").strip()
    if not reason_s:
        raise ValueError("Reason is required")
    validate_offset_swap_duty_dates(
        request_person=req,
        exchange_person=exc,
        shift_type=st,
        original_date=original_date,
        exchange_date=exchange_date,
    )
    token = get_tenant_access_token()
    row_refresh = get_ose_offset_record_admin_row(record_id)
    if not bool(row_refresh.get("pending")):
        raise ValueError("Only pending offset requests can be edited (record may have been approved).")
    if _title_name(str(row_refresh.get("request_person") or "")) != req:
        raise ValueError("This offset request does not belong to you.")
    fields: dict[str, Any] = {
        "Exchange Person": _offset_person_field_value(exc, token=token),
        "Shift Type": st,
        "Original Date": _bitable_date_ms(original_date),
        "Exchange Date": _bitable_date_ms(exchange_date),
        "Reason": reason_s,
    }
    _bitable_update_record(token, OSE_OFFSET_TABLE_ID, record_id, fields)
    invalidate_ose_bitable_cache()
    _schedule_offset_duty_wiki_sync(record_id=record_id)
    return {"ok": True, "record_id": record_id}


def update_ose_offset_record_fields(
    *,
    record_id: str,
    exchange_person: str,
    shift_type: str,
    original_date: date,
    exchange_date: date,
    reason: str,
) -> dict[str, Any]:
    """Update offset swap fields for **any** status (caller must enforce approver-only / authorization)."""
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("record_id is required")
    old_row = get_ose_offset_record_admin_row(rid)
    old_status = str(old_row.get("approval_status") or "").strip().title()
    req = _title_name(str(old_row.get("request_person") or ""))
    exc = resolve_offset_exchange_person(exchange_person, request_person=req)
    st = (shift_type or "").strip().upper()
    if st not in OSE_SHIFT_TYPES:
        raise ValueError("Shift Type must be N or D")
    reason_s = (reason or "").strip()
    if not reason_s:
        raise ValueError("Reason is required")
    token = get_tenant_access_token()
    fields: dict[str, Any] = {
        "Exchange Person": _offset_person_field_value(exc, token=token),
        "Shift Type": st,
        "Original Date": _bitable_date_ms(original_date),
        "Exchange Date": _bitable_date_ms(exchange_date),
        "Reason": reason_s,
    }
    _bitable_update_record(token, OSE_OFFSET_TABLE_ID, rid, fields)
    invalidate_ose_bitable_cache()
    sheet_out: dict[str, Any] = {}
    if old_status == "Approved":
        sheet_out = resync_approved_offset_shift_sheet_after_edit(rid, old_row=old_row)
    _schedule_offset_duty_wiki_sync(record_id=rid)
    return {"ok": True, "record_id": rid, "shift_sheet": sheet_out}


def audit_person_open_ids(*, json_out: bool = False) -> dict[str, Any]:
    """List OSE roster names and whether each has a Lark ``open_id`` (bitable + defaults/env)."""
    token = get_tenant_access_token()
    leave_disp, leave_appr, offset = _get_bitable_raw_triple(token)
    bitable_idx = _build_ose_person_open_id_index(leave_disp + leave_appr, offset)
    override_idx = _ose_person_open_id_overrides()
    full_idx = dict(bitable_idx)
    full_idx.update(override_idx)

    roster_names = sorted(
        [(key, ose_roster_sheet_label(key)) for key, _label in OSE_SHIFT_ROSTER],
        key=lambda pair: pair[0].lower(),
    )

    people: list[dict[str, Any]] = []
    missing: list[str] = []
    for nm, sheet_label in roster_names:
        oid_override = _lookup_person_open_id(nm, override_idx)
        oid_bitable = _lookup_person_open_id(nm, bitable_idx)
        oid = _lookup_person_open_id(nm, full_idx)
        sources: list[str] = []
        if oid_override:
            sources.append("default/env")
        if oid_bitable:
            sources.append("bitable")
        if not oid:
            missing.append(sheet_label)
        people.append(
            {
                "name": nm,
                "sheet_label": sheet_label,
                "open_id": oid or None,
                "sources": sources,
                "ok": bool(oid),
            }
        )

    approvers: list[dict[str, Any]] = []
    try:
        from offsetleave import OFFSET_APPROVER_OPEN_IDS
    except ImportError:
        OFFSET_APPROVER_OPEN_IDS = frozenset()  # type: ignore[misc, assignment]
    for oid in sorted(OFFSET_APPROVER_OPEN_IDS):
        roster = lookup_roster_name_for_open_id(oid, token)
        approvers.append({"open_id": oid, "roster_name": roster or None})

    try:
        import leavewfh as lw

        calendar_map = lw.resolve_roster_open_ids(token)
        calendar_missing = [
            label for key, label in roster_names if not calendar_map.get(_title_name(key))
        ]
    except Exception as exc:
        calendar_map = {}
        calendar_missing = roster_names[:]
        calendar_error = str(exc)
    else:
        calendar_error = ""

    out = {
        "ok": not missing,
        "missing_count": len(missing),
        "missing": missing,
        "people": people,
        "offset_approvers": approvers,
        "calendar_missing": calendar_missing,
        "calendar_missing_count": len(calendar_missing),
        "calendar_error": calendar_error or None,
        "hint": (
            "Add missing names to OSE_PERSON_OPEN_IDS or LEAVE_CALENDAR_OPEN_IDS in .env "
            '(JSON: {"Name":"ou_…"}), or ensure leave/offset Bitable person fields are filled.'
        ),
    }
    if json_out:
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        print(f"OSE open_id audit — {len(roster_names)} shift roster(s), {len(missing)} missing\n")
        for row in people:
            mark = "OK" if row["ok"] else "MISSING"
            src = ", ".join(row["sources"]) if row["sources"] else "—"
            oid = row["open_id"] or "—"
            print(f"  [{mark:7}] {row['sheet_label']:<34} {oid}  ({src})")
        if approvers:
            print("\nOffset approvers:")
            for row in approvers:
                nm = row.get("roster_name") or "(name unknown)"
                print(f"  {nm}: {row['open_id']}")
        if calendar_missing:
            print(f"\nLeave calendar map missing ({len(calendar_missing)}):")
            print("  " + ", ".join(calendar_missing))
        elif not calendar_error:
            print("\nLeave calendar map: all roster names have open_id")
        if calendar_error:
            print(f"\nLeave calendar check skipped: {calendar_error}")
        if missing:
            print(f"\n{out['hint']}")
    return out


if __name__ == "__main__":
    if "--debug" in sys.argv:
        DEBUG = True
        sys.argv.remove("--debug")
    if "--check-open-ids" in sys.argv:
        json_out = "--json" in sys.argv
        audit_person_open_ids(json_out=json_out)
    elif "--probe-offset-shift" in sys.argv:
        json_out = "--json" in sys.argv
        apply = "--apply" in sys.argv
        probe_offset_shift_sheet_sync(apply=apply, json_out=json_out)
    elif "--probe-leave-shift" in sys.argv:
        json_out = "--json" in sys.argv
        apply = "--apply" in sys.argv
        probe_leave_shift_sheet_sync(apply=apply, json_out=json_out)
    elif "--restore-leave-shift" in sys.argv:
        # DRY RUN by default; add --apply to write.
        #   --list                 show every tracked cell currently on a leave code
        #   --record recXXXX       target one leave record
        #   --person "Kheng Kwan"  target one person's tracked leave
        json_out = "--json" in sys.argv
        apply = "--apply" in sys.argv
        list_all = "--list" in sys.argv

        def _opt(flag: str) -> Optional[str]:
            if flag in sys.argv:
                i = sys.argv.index(flag)
                if i + 1 < len(sys.argv):
                    return sys.argv[i + 1]
            return None

        restore_leave_cells_to_original_shift(
            apply=apply,
            json_out=json_out,
            record_id=_opt("--record"),
            person=_opt("--person"),
            include_all=list_all,
        )
    elif "--scan-sheet-leave" in sys.argv:
        # State-free: scan the roster itself for AL/SL/L/HL/EL and restore D/N.
        #   --month 2026-07   (repeatable-ish: one month; default = this + next month)
        #   --person "Name"   narrow to one person
        #   --apply           write (default is a dry run)
        json_out = "--json" in sys.argv
        apply = "--apply" in sys.argv

        def _opt2(flag: str) -> Optional[str]:
            if flag in sys.argv:
                i = sys.argv.index(flag)
                if i + 1 < len(sys.argv):
                    return sys.argv[i + 1]
            return None

        months = None
        _m = _opt2("--month")
        if _m:
            try:
                _y, _mo = _m.split("-")[:2]
                months = [(int(_y), int(_mo))]
            except (ValueError, IndexError):
                print(f"❌ bad --month {_m!r}; expected YYYY-MM", flush=True)
                sys.exit(2)
        _codes = _opt2("--codes")
        _scan_res = scan_sheet_leave_codes_to_original_shift(
            months=months,
            apply=apply,
            json_out=json_out,
            person=_opt2("--person"),
            codes=set(_codes.split(",")) if _codes else None,
        )
        if apply and _scan_res.get("written"):
            print(
                "\n⚠️  The sheet is fixed, but the leave is still APPROVED in the Bitable, "
                "so the periodic sync will re-mark these cells. Make it permanent with:\n"
                f"   --suppress-leave --person \"{_opt2('--person') or '<name>'}\" --apply",
                flush=True,
            )
    elif "--suppress-leave" in sys.argv or "--unsuppress-leave" in sys.argv:
        # Flag approved leave records so the roster keeps the original D/N shift.
        json_out = "--json" in sys.argv
        apply = "--apply" in sys.argv
        undo = "--unsuppress-leave" in sys.argv

        def _opt3(flag: str) -> Optional[str]:
            if flag in sys.argv:
                i = sys.argv.index(flag)
                if i + 1 < len(sys.argv):
                    return sys.argv[i + 1]
            return None

        _mo = _opt3("--month")
        _month = None
        if _mo:
            try:
                _y, _m2 = _mo.split("-")[:2]
                _month = (int(_y), int(_m2))
            except (ValueError, IndexError):
                print(f"❌ bad --month {_mo!r}; expected YYYY-MM", flush=True)
                sys.exit(2)
        suppress_leave_marking(
            person=_opt3("--person"),
            record_id=_opt3("--record"),
            month=_month,
            apply=apply,
            unsuppress=undo,
            json_out=json_out,
        )
    elif len(sys.argv) > 1:
        print(osedate(sys.argv[1]))
    else:
        print(get_ose_today_duty())