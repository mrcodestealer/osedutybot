#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read a provider announcement and decide what the maintenance sheet should say.

Deterministic, and deliberately so: this drives writes to a shared operational
sheet, so the rule that fires has to be inspectable after the fact. The LLM
(``providerllm``) sits ON TOP of this, not under it — it answers the questions
regex cannot ("is this message even a reply to us?"), while the window itself is
parsed here so a hallucinated time can never reach the Base.

ACTIONS
  fill          an upcoming window was stated -> write Start/End/Remark
  clear         an explicit "no maintenance this week" answer -> Remark = "No maintenance"
  follow_quote  the answer is in a message this one quotes; the caller must fetch it
  needs_human   it IS about maintenance but the window is ambiguous - never written
  ignore        everything else

The ordering matters more than any single pattern:

  1. RESCHEDULE first. A "时间变更 / Rescheduled" notice quotes the ORIGINAL date
     as well as the new one, and it must OVERRIDE whatever is already written.
     vawatch's first cut had `rescheduled|postponed` inside its completed/cancelled
     pattern, which made the override a guaranteed no-op.
  2. COMPLETED / CANCELLED next, and it wins outright *within its own sentence*:
     a completion notice repeats the original window verbatim, so any
     window-based rule alone re-fires on it. But "上次维护已完成。下次例行维护
     将于 X 进行。" is two statements, not one: the completion is scoped to the
     sentence that carries it, and a window stated in a DIFFERENT sentence wins.
     That is the only way a "done + next" message fills the row instead of being
     thrown away whole.
  3. Only then the ordinary "upcoming maintenance" test.

WHICH WINDOW, when a notice states more than one (NOTICE_WINDOW_PICK=best)
Only a range that is PART OF THE MAINTENANCE STATEMENT is a candidate at all
(``_relevant``): the nearest subject word in its sentence names the maintenance
and not the support desk, a promotion, settlement, a deposit channel or a
holiday; or it sits on a data line ("Time: ...", "Slots: ...") under a heading
that is not about something else. A range takes a date only by ``_bind_date``:
one written against it, the nearest eligible one before it, or - for a Time
line - a Date line directly below. A letterhead date is never lent, and a range
with no date of its own is no window (it used to take the message's first
date, promo end dates and ticket ids included).

Candidates are ranked by three commented weights in ``_score`` - highest first:

  a. not inside a superseded block (an Original/原定 region under an Updated
     heading) and not introduced by a superseded clause marker (原时间,
     "moved from", "instead of")                                   weight 8
  b. not introduced as a PAST window (上次/上周/last/previous)       weight 4
  c. it ends at or after ``now``                                    weight 1

Everything at the top score survives, one per instant: the same window printed
in two zones is one candidate, including a converted copy whose clock crossed
midnight and so borrowed a date 24h off (``_merge_zone_copies``). Survivors that
overlap, touch or leave a gap of at most two hours are pieces of ONE outage and
become their UNION (two product lines). Anything further apart is a separate
outage and is NEVER merged: the row takes the earliest one still in the future
and the rest are returned in ``others`` and named in ``reason``. The old rule
unioned everything whose start fell within 24h of the first, which joined two
nightly outages into a 26h window nobody announced.

A window the text cannot settle is ``needs_human``, never a guess: two dates
written against one range, two zones on one line that disagree, a zone the
table does not know (after, before or above the range - unless it is exactly
a known window restated, _confirm_foreign_copies), an abbreviation nothing
resolves (IST, or PST beside an offset it cannot be - _TZ_AMBIGUOUS), the
same window stated twice 12h apart, a row that could fall on either side of
midnight, a reschedule or correction that still states two windows without
marking the old one, the earliest window only described while the named
maintenance is later, and anything longer than NOTICE_MAX_WINDOW_HOURS.

Set NOTICE_WINDOW_PICK=first to get the pre-2026-09 behaviour, where the first
range in the document won outright and a passing mention of last week's window
could be stamped with this week's date. The relevance and date rules above
apply in both modes.

Window formats seen in the wild, all handled (see testing/notice_corpus_test.py):
    2026-09-16 09:00 - 12:00 GMT+8
    Time: Wednesday, September 2, 2026, 14:30 - 15:30 (UTC+8)
    时间：2026.09.23 周三 06:00 - 09:00 (UTC+8)
    日期Date：Sep 16th, 2026 (Wed)   +   时间Time：10:00 - 12:00 (GMT+8)   <- separate lines
    维护日期：2026年9月23日           +   维护时间：10:00 - 12:00 (GMT+8)
    排定维护 9/23 10:00 - 12:00 (GMT+8)          <- year inferred, see _infer_year
    Scheduled maintenance 23/09/2026 10:00 - 12:00 (GMT+0530)
    2026-09-23 10:00:00 - 12:00:00 (UTC/GMT+08:00)     <- seconds, UTC/GMT+8
    Sep 23, 2026 from 8:00 - 11:00 PM PST              <- one-sided meridiem
    维护时间：2026年9月23日 下午2:00-4:00 / 22:00 至 次日02:00
    Start: 2026-09-24 22:00 (GMT+8)  +  End: 2026-09-25 02:00 (GMT+8)

ENV FLAGS (all documented in .env.example; every one defaults to the SAFE value)
    NOTICE_TZ           assumed zone when a notice states none (Asia/Manila)
    NOTICE_DATE_ORDER   dmy | mdy — only consulted when BOTH components are <=12
    NOTICE_BARE_DATE_ORDER  ask | mdy | dmy — the year-less "10/12" with no
                        weekday to settle it: ask (a person decides) by default
    NOTICE_YEARLESS     1 | 0     — infer the year of "9/23", "9月23日", "Sep 23"
    NOTICE_WORDING      wide | strict — which headings open the gate
    NOTICE_WINDOW_PICK  best | first  — how a multi-window notice is resolved
    NOTICE_MAX_WINDOW_HOURS  48 | <h> | 0 — a longer window goes to a human
    NOTICE_EXTENSION_FILL  1 | 0  — "extended until 14:00" on a readable window
                        writes the NEW end (1), or goes to a person (0). Before
                        it existed the notice re-wrote the OLD end, so 1 is the
                        safer of the two for the sheet; 0 is the kill switch.
    NOTICE_QUESTION_GUARD  1 | 0  — a message that ASKS about maintenance is
                        not a notice (refused='question')
    NOTICE_NEGATION_GUARD  1 | 0  — a negated gate phrase does not count, and a
                        promise of no downtime vetoes (refused='negated' /
                        'no_outage')
    NOTICE_NONPROD_GUARD   1 | 0  — UAT / staging / 测试环境 maintenance is not a
                        production outage (refused='nonprod')
    NOTICE_REQUEST_GUARD   1 | 0  — an operator REQUEST to move or freeze is
                        needs_human, not the provider's window (refused='request')
A 1/0 flag set to anything else keeps its default and is logged once (_on).
"""

from __future__ import annotations

import os
import re
import unicodedata
from datetime import date, datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Env flags. Read at CALL time, never at import time, so the corpus can pin one
# per case in os.environ and restore it after. That does NOT make a .env edit
# live: the bot loads .env into os.environ once, at startup (load_dotenv in
# vawatch and telegramwarm), so changing a flag in .env needs a restart. This
# comment used to promise the opposite (F83).
# ---------------------------------------------------------------------------


def _flag(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None or not v.strip() else v.strip().lower()


#: Each setting this module could not use as written, logged once per process:
#: name -> message. vawatch.config_warnings() repeats them in /vacheck.
_CONFIG_WARNED: dict = {}


def _warn_config(name: str, message: str) -> None:
    if _CONFIG_WARNED.get(name) != message:
        _CONFIG_WARNED[name] = message
        print("[noticeparse] config: " + message, flush=True)


def config_warnings() -> list:
    return list(_CONFIG_WARNED.values())


def _on(name: str, default: str = "1") -> bool:
    """A 1/0 flag. A word that is neither ("Y", "enabled", a typo) keeps the
    DEFAULT and is logged once. It used to read as ON whatever the default,
    while vawatch read the same garbage as OFF - the two modules disagreed on
    one .env line (F83). Every flag read here defaults to its safe value, so
    the default is the right answer to a value nobody can interpret."""
    v = _flag(name, default)
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    _warn_config(name, "{}={!r} is not 1/0 (true/false, yes/no, on/off) - "
                       "using its default, {}".format(name, v, default))
    return default in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# Unicode normalisation
# ---------------------------------------------------------------------------
#
# A notice typed on a Chinese IME arrives with full-width digits, colons and
# dashes: "１０：００－１２：００" is the same window as "10:00-12:00" and not one
# character of it matched any regex here, so the whole notice was dropped. The
# fix is to parse a NORMALISED COPY - the caller still writes the original text
# to Remark, because an operator reading the row wants what the provider wrote.
#
# The map is applied one character at a time and each replacement is exactly one
# character long, so offsets in the copy are the offsets in the original. That
# is load-bearing: the line-anchored rules below (which line a range sits on,
# which date precedes it, where a sentence ends) all measure with indices, and a
# whole-string NFKC that expanded one ligature would silently shift every one of
# them. Anything NFKC would turn into more than one character is left alone.
#
# NFKC already folds ：->: （->( ，->, U+3000->space and the full-width digits,
# which is exactly what we want and leaves ordinary CJK body text untouched
# (Han ideographs have no compatibility decomposition). The dashes it does NOT
# fold are listed explicitly.
_WIDE_MAP = {
    "　": " ",      # ideographic space
    "－": "-",      # fullwidth hyphen-minus
    "‐": "-", "‑": "-", "‒": "-",
    "–": "-",      # en dash, what a pasted Word notice carries
    "—": "-",      # em dash
    "―": "-", "−": "-",
    # G1.8: the small and super/subscript minus signs ("GMT﹣5").
    "﹣": "-", "⁻": "-", "₋": "-",
    "〜": "~",      # wave dash (NFKC leaves this one alone)
    "～": "~",      # fullwidth tilde
}
_NFKC_ONE = {}

# G1.8: invisible format characters a pasted notice carries - zero-width space
# and joiners, the word joiner, a BOM, a soft hyphen. They split a gate phrase
# ("Scheduled<ZWSP>maintenance", "系统<ZWSP>维护") so the notice was dropped as
# "no scheduled-maintenance wording", and inside a clock they were worse than a
# miss: "1<ZWSP>4:00-16:00" read as 04:00-16:00, a wrong window. They are
# DROPPED from the parsing copy (a space would still split 系统 维护 and 1 4:00);
# only between two Latin letters do they become a space, because there the
# invisible break is standing in for one.
_INVISIBLE = frozenset("­​‌‍‎‏"
                       "⁠⁡⁢⁣⁤﻿")
_ZONE_WORDS_NO_SPLIT = frozenset({"UTC", "GMT", "SGT", "HKT", "PHT", "MYT", "CST", "BJT",
                                  "ICT", "WIB", "JST", "KST", "CET", "CEST", "EST", "PST"})
# Mathematical Alphanumeric Symbols (U+1D400-U+1D7FF): the bold and italic
# letters a "fancy text" generator produces ("𝐒𝐜𝐡𝐞𝐝𝐮𝐥𝐞𝐝 𝐌𝐚𝐢𝐧𝐭𝐞𝐧𝐚𝐧𝐜𝐞"). Each folds to ONE
# ASCII letter or digit, so the block is safe to fold character by character.
_MATH_LO, _MATH_HI = "\U0001d400", "\U0001d7ff"


def _norm(text: str) -> str:
    """The parsing copy of a notice: full-width forms folded, length preserved.

    ONLY the Halfwidth and Fullwidth Forms block (U+FF00-U+FFEF) is folded, plus
    the dashes listed in _WIDE_MAP. Folding everything NFKC happens to map
    one-to-one was too greedy: a circled list bullet folds ① -> "1", so
    "①2026-09-23" became "12026-09-23" and the ISO rule's (?<![\\d/.\\-])
    lookbehind then refused the date - a notice that parsed perfectly before
    normalisation existed stopped parsing at all. Circled, enclosed, superscript
    and subscript characters are decoration around a date, never part of one, so
    they pass through untouched.

    The one exception to "length preserved" is _INVISIBLE, which is removed
    before anything is indexed. Every offset any rule takes is an offset into
    THIS copy (classify and find_window never map one back to the raw text), so
    dropping a zero-width character shifts nothing a rule measures.
    """
    if not text:
        return ""
    if any(ch in _INVISIBLE for ch in text):
        kept = []
        for i, ch in enumerate(text):
            if ch not in _INVISIBLE:
                kept.append(ch)
                continue
            prev = kept[-1] if kept else ""
            nxt = next((c for c in text[i + 1:] if c not in _INVISIBLE), "")
            if prev.isascii() and prev.isalpha() and nxt.isascii() and nxt.isalpha():
                # G1.8: never inside a zone abbreviation - "U\u200bTC" is UTC,
                # and "U TC" read as no zone at all put the window on +08.
                word = re.search(r"[A-Za-z]*$", "".join(kept)).group(0)
                tail = re.match(r"[A-Za-z]*", text[i + 1:].replace("\u200b", "")).group(0)
                if (word + tail).upper() not in _ZONE_WORDS_NO_SPLIT:
                    kept.append(" ")
        text = "".join(kept)
    out = []
    for ch in text:
        r = _WIDE_MAP.get(ch)
        if r is None and _MATH_LO <= ch <= _MATH_HI:
            n = unicodedata.normalize("NFKC", ch)
            r = n if len(n) == 1 and n.isascii() else ch
        if r is None and "＀" <= ch <= "￯":
            r = _NFKC_ONE.get(ch)
            if r is None:
                n = unicodedata.normalize("NFKC", ch)
                # One character in, one character out: every rule here is
                # index-anchored (which line a range sits on, which date
                # precedes it, where a sentence ends), so a fold that changed
                # the length would silently shift all of them.
                r = n if len(n) == 1 else ch
                _NFKC_ONE[ch] = r
        out.append(r if r is not None else ch)
    return "".join(out)


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}
_MONTH_WORD = (r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|"
               r"Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|"
               r"Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)")
_ORD = r"(?:st|nd|rd|th)?"


def _mword(m) -> int:
    return _MONTHS[re.search(_MONTH_WORD, m.group(0), re.I).group(0)[:3].lower()]


# ---------------------------------------------------------------------------
# A weekday written against a date
# ---------------------------------------------------------------------------
#
# "9/23 (三)", "Sep 16th, 2026 (Wed)", "2026.09.23 周三", "Thursday, 24 September".
# The weekday is the one piece of a date a provider types twice, so it is the
# only independent evidence the text carries about the date itself. It settles
# the order of a bare "11/4 (三)" (4 November, a Wednesday - not 11 April, a
# Sunday), and when it CONTRADICTS the date it is the difference between a row
# written on the right day and a row written on the day the provider mistyped:
# the text cannot say which half is wrong, so a person reads it (see _dates).
#
# Only a SINGLE weekday touching the date counts, never a range of them:
# "Customer service 24/7 (Mon-Sun)" used to vouch for "24/7" as 24 July,
# because the old test only looked for an opening bracket and a weekday name.
_WD_NAME = (r"(?:mon(?:day)?|tue(?:s(?:day)?|sday)?|wed(?:nesday)?"
            r"|thu(?:r(?:s(?:day)?)?)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)")
_WD_ZH_PREFIX = r"(?:[周週]|星期|礼拜|禮拜)"
_WD_EN_IDX = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5,
              "sun": 6}
_WD_ZH_IDX = {"一": 0, "二": 1, "三": 2, "四": 3, "五": 4, "六": 5, "日": 6,
              "天": 6}
_WD_LONG = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
            "Saturday", "Sunday"]
# Straight after the date, on the same line: "(Thu)", "（四）", "(周四)", " 周三",
# ", Wednesday". [ \t] and not \s, so a weekday that opens the NEXT line is
# never read as this date's.
_WD_AFTER_RE = re.compile(
    r"[ \t]*[,，]?[ \t]*(?:"
    r"[\(（\[][ \t]*(?:(?P<en>" + _WD_NAME + r")\.?|" + _WD_ZH_PREFIX
    + r"?(?P<zh>[一二三四五六日天]))[ \t]*[\)）\]]"
    r"|(?P<en2>" + _WD_NAME + r")(?![A-Za-z])\.?(?![ \t]*[-~至到])"
    r"|" + _WD_ZH_PREFIX + r"(?P<zh2>[一二三四五六日天])(?![ \t]*[-~至到])"
    r")", re.I)
# Straight in front of it: "Wednesday, September 2, 2026", "周四 9/24".
_WD_BEFORE_RE = re.compile(
    r"(?:(?<![A-Za-z])(?P<en>" + _WD_NAME + r")\.?|" + _WD_ZH_PREFIX
    + r"(?P<zh>[一二三四五六日天]))[ \t]*[,，]?[ \t]*$", re.I)
# Inside the date token itself: "24th(Thu) Sep", the KingMidas English shape.
_WD_INSIDE_RE = re.compile(
    r"[\(（][ \t]*(?:(?P<en>" + _WD_NAME + r")\.?|" + _WD_ZH_PREFIX
    + r"?(?P<zh>[一二三四五六日天]))[ \t]*[\)）]", re.I)


def _wd_of(m):
    """(weekday 0-6, the token as written) for one _WD_*_RE match."""
    en = m.groupdict().get("en") or m.groupdict().get("en2")
    if en:
        return _WD_EN_IDX[en[:3].lower()], m.group(0).strip(" \t,，")
    zh = m.groupdict().get("zh") or m.groupdict().get("zh2")
    return _WD_ZH_IDX[zh], m.group(0).strip(" \t,，")


def _weekday_after(text: str, end: int):
    m = _WD_AFTER_RE.match(text, end)
    return _wd_of(m) if m else None


def _weekday_near(text: str, start: int, end: int):
    """The single weekday written against the date at text[start:end], or None."""
    m = _WD_INSIDE_RE.search(text, start, end)
    if m:
        return _wd_of(m)
    got = _weekday_after(text, end)
    if got:
        return got
    m = _WD_BEFORE_RE.search(text, max(0, start - 16), start)
    return _wd_of(m) if m else None


#: A year-less date the inference puts further ahead than this is not trusted
#: on the year alone. _infer_year reaches 245 days forward, so a notice re-posted
#: 121-245 days after it was first sent - "排定维护 5月20日" read on 09-22, a
#: FIFO-evicted ledger key re-presenting an old bubble, a reply quoting last
#: spring's window - came back as a window eight months AHEAD, which the stale
#: guard cannot see and the Base took as a real outage. Real notices announce
#: days or weeks ahead, so beyond this the notice goes to a person, unless its
#: own weekday confirms the year (the weekday of a fixed day-and-month repeats
#: only every 5-11 years, so a match is decisive).
_YEARLESS_SURE_AHEAD = timedelta(days=120)


def _date_doubt(text: str, start: int, end: int, day: date, inferred: bool,
                base: date):
    """Why the date at text[start:end] (read as ``day``) cannot be written, or None."""
    wd = _weekday_near(text, start, end)
    if wd is not None and day.weekday() != wd[0]:
        return ("the notice writes {} as '{}', but {} is a {} - the date or "
                "the weekday is a typo and the text does not say which".format(
                    text[start:end].strip(), wd[1], day.isoformat(),
                    _WD_LONG[day.weekday()]))
    if inferred and wd is None and day - base > _YEARLESS_SURE_AHEAD:
        return ("the year-less date '{}' can only be {}, {} days ahead - more "
                "likely an old notice re-posted than a new one".format(
                    text[start:end].strip(), day.isoformat(),
                    (day - base).days))
    return None


def _order_dm(a: int, b: int):
    """(month, day) for an ambiguous numeric pair, per NOTICE_DATE_ORDER.

    A component greater than 12 settles it on its own and the flag is not even
    consulted, which is why 23/09/2026 and 09/23/2026 are both 23 September. The
    flag only breaks the tie when BOTH are <=12 (05/09/2026). A pair that is not
    a real date under the chosen order falls back to the other order rather than
    being discarded: 13/09 under mdy is not month 13, it is 13 September.
    """
    if a > 12 and b <= 12:
        return b, a
    if b > 12 and a <= 12:
        return a, b
    if a > 12 and b > 12:
        raise ValueError("neither component can be a month")
    pairs = [(b, a), (a, b)] if _flag("NOTICE_DATE_ORDER", "dmy") != "mdy" \
        else [(a, b), (b, a)]
    for mo, d in pairs:
        try:
            date(2000, mo, d if d != 29 or mo != 2 else 28)
            return mo, d
        except ValueError:
            continue
    raise ValueError("no valid month/day reading")


# 23/09/2026 and its kin - named because _dates treats it specially: when both
# numbers are <=12 a weekday written against it settles which is the day.
_DMY_YEAR_RE = re.compile(
    r"(?<![\d/.\-A-Za-z_=#])(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})(?!\d)")
# "24-25/09/2026" - a day range sharing one month and year. _DMY_YEAR_RE
# refuses the "-" in front of "25/09/2026" (it is usually a range, a path or
# an id), so "Date: 24-25/09/2026 / Time: 22:00 - 02:00" had no date at all
# and the notice was dropped. Tight and day-first only: a day range can only
# stand in front of the DAY, whatever NOTICE_DATE_ORDER says. _dates reads
# the "25/09/2026" half as the date; _date_spans supplies the "24-".
_DAYSPAN_NUM_RE = re.compile(
    r"(?<![\d/.\-A-Za-z_=#])(\d{1,2})(?:-|~|至)(\d{1,2})([/.])(\d{1,2})\3"
    r"(\d{4})(?!\d)")

# Each entry is (regex, converter) and the converter returns (year|None, month,
# day). They are tried IN THIS ORDER and a match that overlaps an already
# accepted one is dropped, so the more specific shape always wins: "2026年9月
# 23日" is read by the 年月日 rule and never re-read as a bare 9月23日.
_DATE_RES = [
    # ISO-ish, year first. The lookbehind also refuses a Latin letter, "_",
    # "=" and "#" in front: "Ticket: MNT2026-10-08", "[MNT2026-10-08]" and
    # "incident?date=2026-10-01" are an id and a URL parameter, and each was
    # read as the day of a window that stated none of its own and written.
    # ASCII only on purpose - a CJK label ("日期2026-09-23") and a circled
    # bullet ("①2026-09-23") are \w too, and both sit in front of real dates.
    (re.compile(r"(?<![\d/.\-A-Za-z_=#])(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})(?!\d)"),
     lambda m: (int(m.group(1)), int(m.group(2)), int(m.group(3)))),
    # 2026年9月23日 / 2026年09月23号 — the single commonest shape among the
    # Greater-China studios and, until now, completely unreadable.
    (re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*[日号號]"),
     lambda m: (int(m.group(1)), int(m.group(2)), int(m.group(3)))),
    # September 23rd, 2026 / Sept. 23, 2026 — the \.? is the whole point of the
    # second one: the trailing period of the abbreviation used to break the rule,
    # which expected whitespace straight after the month word.
    (re.compile(_MONTH_WORD + r"\.?\s*,?\s*(\d{1,2})" + _ORD +
                r"\s*,?\s*(\d{4})(?!\d)", re.I),
     lambda m: (int(m.group(2)), _mword(m), int(m.group(1)))),
    # 23rd September 2026 / 23 Sep 2026 / 24-Sep-2026 / 24/Sep/2026. The dash
    # and slash forms are what a date picker or a spreadsheet export writes,
    # and without them "Maintenance date: 24-Sep-2026" had no date at all.
    (re.compile(r"(\d{1,2})" + _ORD + r"(?:\s*[-/]\s*|\s*)(?:of\s+)?" + _MONTH_WORD
                + r"\.?(?:\s*[-/]\s*|\s*,?\s*)(\d{4})(?!\d)", re.I),
     lambda m: (int(m.group(2)), _mword(m), int(m.group(1)))),
    # 23/09/2026, 23-09-2026, 23.09.2026, 09/23/2026 — day- or month-first with
    # the year at the END. The original rule demanded a 4-digit year in FRONT,
    # which is why every one of these produced no window at all. (_dates lets
    # a weekday written against it settle a pair that is <=12 both ways.)
    (_DMY_YEAR_RE,
     lambda m: (int(m.group(3),),) + _order_dm(int(m.group(1)), int(m.group(2)))),
    # 9月23日 with no year at all.
    (re.compile(r"(?<!\d)(\d{1,2})\s*月\s*(\d{1,2})\s*[日号號]"),
     lambda m: (None, int(m.group(1)), int(m.group(2)))),
    # Sep 23 with no year. The lookahead keeps it from eating the day out of
    # "Sep 23, 2026", which the year-bearing rule above has already taken.
    # F42: also "Sep-24".
    # F42: not the "Sep-26" of "24-Sep-26" (a two-digit year).
    (re.compile(r"(?<!\d[-/.])" + _MONTH_WORD + r"\.?(?:\s*|-)(\d{1,2})" + _ORD +
                r"(?!\s*,?\s*\d{4})(?![\d:.])(?!-\d)", re.I),
     lambda m: (None, _mword(m), int(m.group(1)))),
    # 24th Sep / 24 September / 24th(Thu) Sep, with no year - the day-first
    # twin of the rule above, and the shape of KingMidas's own English line
    # ("commence at 10:00AM 23th(Wed) Sep"). Without it "Scheduled maintenance
    # on 24th Sep (Thu) 10:00 - 12:00" was dropped as having no date. The month
    # word must be CAPITALISED: "up to 10 may be affected" is a verb, not May.
    # F42: also "24-Sep" (the year-less twin of 24-Sep-2026).
    (re.compile(r"(?<![\d:./-])(\d{1,2})" + _ORD
                + r"(?:[-/.]|\s*(?:[\(（][^)）\n]{1,10}[\)）]\s*)?(?:of\s+)?)(?-i:(?=[A-Z]))"
                + _MONTH_WORD + r"(?![A-Za-z])\.?(?!\s*[-/,]?\s*\d{4})", re.I),
     lambda m: (None, _mword(m), int(m.group(1)))),
    # 9月24 10:00 / 9月24（四）- the 日 left off. Only with a clock or a weekday
    # straight after, because a bare "12月24" could as well be the start of
    # "12月24小时客服" (24-hour support from December).
    (re.compile(r"(?<!\d)(?:(\d{4})\s*年\s*)?(\d{1,2})\s*月\s*(\d{1,2})"
                r"(?![\d日号號:.])(?=\s*(?:[\(（]|" + _WD_ZH_PREFIX
                + r"|\d{1,2}\s*[:点點时時]|[上下]午|早上|晚上|凌晨|中午))"),
     lambda m: (int(m.group(1)) if m.group(1) else None, int(m.group(2)),
                int(m.group(3)))),
]

# "9/23" — the shape inside the live KingMidas notice, and the riskiest date
# form in the file: it is also how a score, a ratio, a version number and a
# fraction are written. The guard is that it must be INTRODUCED the way a day
# is (see the introducer / weekday / veto rules below) and must not overlap a
# date another rule already read.
#
# It used to be consulted only when no other date form matched anywhere in the
# message. That switch is exactly backwards for the notices that need it: in
# "将于9/24 (四) 10:00 - 12:00 排定维护。国庆节(10月1日)期间客服正常。" the
# holiday date switched the 9/24 off and the window was written on 10-01, a
# Thursday like the (四) it claimed. It is now always read, and which date a
# range takes is _bind_date's decision: a date written straight in front of
# the range beats one elsewhere on the line, and the nearest eligible line
# above beats a line further up - which is also what keeps the live KingMidas
# notice right, where the full "Sep 23th, 2026" sits on the line directly
# above the Time line and the 9/23 several lines higher.
#
# The lookbehind refuses a digit, "/", "." or "-" in front (part of a longer
# number, a path, a version or a range) and a colon only when a DIGIT precedes
# it (the minutes of a clock). A flat colon refusal also rejected the label
# colon, so "维护日期：9/24 10:00-12:00" and "Date:9/24" - NFKC folds "：" to ":"
# - had no date, while the same text with a space after the colon filled. It is
# the label-colon trap _RANGE_HOUR_RE and _RANGE_DOT_RE were already fixed for.
_RISKY_DATE_RE = re.compile(r"(?<![\d/.\-])(?<!\d:)(\d{1,2})/(\d{1,2})(?![\d/])")
# "9.24（周四）" - the dotted twin. A dot between two numbers is a decimal, a
# price or a version far more often than a day, so this one is read ONLY when
# a single weekday is written straight after it and the weekday agrees.
_RISKY_DOT_DATE_RE = re.compile(
    r"(?<![\dA-Za-z_/.\-])(?<!\d:)(\d{1,2})\.(\d{1,2})(?![\d/.])")
_DATE_CONTEXT_RE = re.compile(
    r"\d{1,2}\s*[:.]\s*\d{2}|\d{1,2}\s*[AaPp]\.?[Mm]|\d{1,2}\s*[时時]"
    r"|日期|時間|时间|日程|维护|維護|维修|維修|停机|停機|停服|升级|升級"
    r"|\bdate\b|\btime\b|\bon\b|maintenance|downtime|upgrade|schedul",
    re.I)
_RISKY_DATE_REACH = 16

# A DAY introducer immediately in front of the bare "9/23" - anchored at the end
# so only the words touching it count. These say "what follows is a day".
_RISKY_DAY_INTRO_RE = re.compile(
    r"(?:将于|將於|将在|將在|定于|定於|于|於|日期|時間|时间|预计|預計"
    r"|\bon\b|\bfrom\b|\bdate\b|\bdated\b|\bstarting\b|\bcommenc(?:e|ing)\b"
    r"|\bscheduled\s+for\b|\bfor\b)[\s:：,，]*$",
    re.I)
# The maintenance word itself introduces the day just as often: "排定维护 9/23
# 10:00 - 12:00" is the shape the live KingMidas group uses. It is kept apart
# from the day introducers because it is weaker evidence: it also stands in
# front of a message-part marker ("System Maintenance 1/2") and a share
# ("Maintenance 3/4 servers"), where a day word never does.
_RISKY_TOPIC_INTRO_RE = re.compile(
    r"(?:维护|維護|维修|維修|停机|停機|停服|升级|升級"
    r"|\bmaintenance\b|\bdowntime\b|\bupgrade\b)[\s:：,，]*$",
    re.I)
# Kept as the one greppable union of the two.
_RISKY_INTRO_RE = re.compile(
    "(?:" + _RISKY_DAY_INTRO_RE.pattern + ")|(?:"
    + _RISKY_TOPIC_INTRO_RE.pattern + ")", re.I)
# Words that make the pair a count, a share, a phase or a version, never a day.
# Safe next to the introducers, because both are anchored at the end and the
# CLOSEST word therefore decides: in "maintenance, phase 1/2" it is "phase".
_RISKY_VETO_RE = re.compile(
    r"(?:phase|stage|step|part|version|\bv\b|round|chapter|page|ratio|out\s+of"
    r"|第|约|約|阶段|階段|版本|批次)[\s:：]*$",
    re.I)
# What may never follow a DAY: "of <noun>" (a share - "on 3/4 of our nodes"),
# a counter (个/個/家), a unit of time (a duration - "Estimated downtime: 1/2
# hour" was written as 1 February, "预计停机1/2小时" likewise) or a completion
# ("Maintenance 3/4 complete"). Two words that used to be here are now
# conditional, because they follow a day just as naturally: 的 before a clock
# ("将于9/24的10:00至12:00进行例行维护") and 台 in 台灣時間 / 台湾时间 (Taiwan
# time, the zone a Taiwan studio writes). Both notices were silently dropped.
_RISKY_AFTER_VETO_RE = re.compile(
    r"\s*(?:of\b|个|個|家"
    # F61: 的 before a clock, a meridiem word or the maintenance noun is the
    # day's own possessive ("将于9/24的凌晨02:00", "9/24的维护时间为…").
    r"|的(?!\s*(?:凌晨|上午|下午|晚上|中午|早上|傍晚|深夜|清晨|早晨)?\s*\d{1,2}\s*[:点點时時])"
    r"(?!\s*(?:维护|維護|例行|系统|系統|停机|停機))"
    r"|台(?![灣湾北中南])"
    r"|(?:hours?|hrs?|h|days?|minutes?|mins?|seconds?|secs?)\b"
    r"|个?小时|個?小時|分钟|分鐘|天|秒"
    r"|(?:complete[ds]?|done|finished)\b|%|完成)",
    re.I)
# A count noun after the pair makes it a share ("2/3 servers") - but only when
# no DAY word introduces it. "On 9/24 players will not be able to access games"
# and "Scheduled maintenance on 9/24 users cannot log in" put the players and
# the users in the subject slot after a date, and the unconditional veto threw
# both notices away.
_RISKY_NOUN_AFTER_RE = re.compile(
    r"\s*(?:servers?|nodes?|games?|users?|players?)\b", re.I)


def _risky_reading(text: str, m, now, tz, *, dotted: bool = False):
    """-> ((y, m, d), doubt | None) for a bare "9/23" / "9.23" match, or None.

    None means the pair is not a day at all (a ratio, a share, a duration, a
    part marker). Otherwise the reading, plus a reason when the text cannot say
    which reading is meant - the date is still returned, so a range bound to it
    goes to a human instead of borrowing some other date in the message.

    WHICH ORDER. When one number is >12 the pair reads one way only. When both
    are <=12 (11/4, 10/12) the global NOTICE_DATE_ORDER=dmy is the wrong
    authority: the one live sender of this bare form, KingMidas, writes it
    month-first ("9/23 (三)"), so "将于11/4 (三)" was written as 11 April 2027.
    The bracketed weekday is decisive evidence and wins when exactly one
    reading falls on it (4 November 2026 is a Wednesday, 11 April 2027 a
    Sunday). When BOTH do ("10/5 (一)": 5 October 2026 and 10 May 2027 are both
    Mondays), when NEITHER does, or when there is no weekday at all, the pair
    is ambiguous - unless NOTICE_BARE_DATE_ORDER names the order (default
    "ask": a person decides).
    """
    a, b = int(m.group(1)), int(m.group(2))
    before = text[max(0, m.start() - _RISKY_DATE_REACH):m.start()]
    rest = text[m.end():]
    after = rest[:_RISKY_DATE_REACH]
    wd = _weekday_after(text, m.end())
    # F61: "9/24的维护时间为10:00-12:00" - the day's own maintenance straight
    # after it is as good as an introducer in front.
    day_intro = bool(_RISKY_DAY_INTRO_RE.search(before) or re.match(
        r"\s*的\s*(?:例行|定期|系统|系統)?(?:维护|維護|维修|維修|停机|停機)", after))
    if _RISKY_VETO_RE.search(before):
        return None
    if wd is None:
        # A clock nearby is NOT evidence of a date: every message that reaches
        # the write path has one, which made the old proximity test vacuous.
        # So the pair must be introduced the way a day is, or carry a weekday.
        if dotted or not (day_intro or _RISKY_TOPIC_INTRO_RE.search(before)):
            return None
        if _RISKY_AFTER_VETO_RE.match(after):
            return None
        if not day_intro and _RISKY_NOUN_AFTER_RE.match(after):
            return None
        # "24/7" and "7/24" are "around the clock", not 24 July, unless a
        # weekday confirms them ("7/24 (五)").
        if {a, b} == {7, 24}:
            return None
        # A message-part marker ends its heading line: "System Maintenance
        # 1/2", "Scheduled Maintenance 2/2". 2/2 reads the same either way, so
        # the ambiguity rule below never catches it, and it was written as
        # 2 February.
        if not day_intro and a <= b <= 12 and re.match(r"[ \t]*(?:\n|\Z)", rest):
            return None
    dmy_first = _flag("NOTICE_DATE_ORDER", "dmy") != "mdy"
    readings = []
    for mo, d in (((b, a), (a, b)) if dmy_first else ((a, b), (b, a))):
        if not 1 <= mo <= 12:
            continue
        y = _infer_year(mo, d, now, tz)
        if y is not None and (y, mo, d) not in readings:
            readings.append((y, mo, d))
    if not readings:
        return None
    shown = m.group(0)
    if wd is not None:
        hits = [r for r in readings if date(*r).weekday() == wd[0]]
        if len(hits) == 1:
            return hits[0], None
        if hits:
            return readings[0], (
                "'{} {}' is a {} both as {} and as {}, so its day and month "
                "cannot be told apart".format(
                    shown, wd[1], _WD_LONG[wd[0]], date(*hits[0]).isoformat(),
                    date(*hits[1]).isoformat()))
        return readings[0], (
            "'{} {}': no reading of {} ({}) falls on that weekday".format(
                shown, wd[1], shown,
                " / ".join(date(*r).isoformat() for r in readings)))
    if len(readings) == 1:
        return readings[0], None
    order = _flag("NOTICE_BARE_DATE_ORDER", "ask")
    if order in ("mdy", "dmy"):
        want = (a, b) if order == "mdy" else (b, a)
        return next(r for r in readings if (r[1], r[2]) == want), None
    return readings[0], (
        "'{}' can be read as {} or {} and no weekday settles which (set "
        "NOTICE_BARE_DATE_ORDER=mdy|dmy if a provider always writes one "
        "way)".format(shown, date(*readings[0]).isoformat(),
                      date(*readings[1]).isoformat()))

# A year-first dotted/dashed triple straight after a version word is CalVer,
# not a day: "to deploy release 2026.10.1" was read as 1 October and, in a
# notice whose window stated no date of its own, written as the outage.
_VERSION_BEFORE_RE = re.compile(
    r"(?:\bv|\bver(?:sion)?|\brelease|\bbuild|\brev(?:ision)?)\s*[:#]?\s*$",
    re.I)


# The shared year of a date SPAN, read right after its first half: a dash /
# 至 / to, then the second half, then its four-digit year - within one short
# stretch, so "Sep 24 - see the 2026 roadmap" is not one.
_SPAN_YEAR_AFTER_RE = re.compile(
    r"\s*(?:-{1,2}|–|—|~|～|至|到|to|until|through|thru)\s*"
    r"[^\n\d]{0,12}\d{1,2}(?:st|nd|rd|th)?[^\n\d]{0,6}?,?\s*((?:19|20)\d{2})\b",
    re.I)


def _infer_year(month: int, day: int, now: datetime, tz):
    """The year a year-less date means — a pure function of (text, now).

    No clock is read here and the host's timezone is irrelevant, so the same
    message classified twice on the same ``now`` always gives the same year.
    Candidates are the same (month, day) in now.year-1/+0/+1; a candidate is
    kept only if it falls in [now-120d, now+245d) - HALF-OPEN; the earliest
    kept candidate on or after today wins, else the latest kept one. Nothing
    kept means the date is UNUSABLE and the notice yields no window — never a
    guessed year, because a row carrying a window in the wrong year looks
    exactly as healthy as a right one.

    The window is exactly 365 days wide on purpose: at most one candidate year
    can ever qualify, so the "prefer the future" tie-break never has to fire
    and the rule cannot drift with the calendar. It used to be CLOSED at both
    ends, which is 366 days: on the one day that sits exactly 120 days back,
    both it and the same day next year qualified, the future won, and
    "排定维护 5月25日" read on 09-22 was written as 2027-05-25 instead of being
    dropped as stale. 120 back because a provider re-posting last quarter's
    notice is routine (is_stale then stops it being written); the far end of
    the 245 forward is where an OLD notice lands, so _dates sends a year-less
    date more than _YEARLESS_SURE_AHEAD out to a person unless its weekday
    confirms the year.
    """
    base = now.astimezone(tz).date() if now.tzinfo else now.date()
    lo, hi = base - timedelta(days=120), base + timedelta(days=245)
    kept = []
    for y in (base.year - 1, base.year, base.year + 1):
        try:
            cand = date(y, month, day)
        except ValueError:          # 2/29 in a common year
            continue
        if lo <= cand < hi:
            kept.append(cand)
    if not kept:
        return None
    ahead = [c for c in kept if c >= base]
    return (min(ahead) if ahead else max(kept)).year


def _dates(text: str, *, now: datetime, tz, doubts=None):
    """[(start, end, (y, m, d))] for every readable date, in document order.

    The END matters: a caller looking for the time that follows a date has to
    know where the date token actually stopped. Measuring it with \\S+ grabbed
    only "Sep" out of "Sep 16th, 2026".

    ``tz`` is only used to decide what day it is where the notice was written,
    which is all _infer_year needs; a few hours either way can never move a
    365-day-wide window onto a different candidate year.

    ``doubts``, when given, is filled with {start offset: reason} for every
    date that was READ but cannot be trusted: a weekday that contradicts it, a
    bare "10/12" that reads both ways, a year-less date the inference puts
    months ahead (see _date_doubt / _risky_reading). Such a date stays in the
    list on purpose - a range bound to it must go to a person, and dropping it
    would let the range borrow some other date in the message instead.
    """
    out = []
    taken = []
    yearless = _on("NOTICE_YEARLESS", "1")
    base = now.astimezone(tz).date() if now.tzinfo else now.date()

    def _add(a, b, ymd, *, inferred=False, doubt=None):
        y, mo, d = ymd
        if y is None:
            # R1.69: "Sep 24 - Sep 25, 2026" is not a year-less date - the
            # span's one trailing year belongs to BOTH halves. With the
            # year-less rule off (NOTICE_YEARLESS=0) the first half went unread,
            # the range bound to Sep 25 and the window was written a day late.
            sm = _SPAN_YEAR_AFTER_RE.match(text, b)
            if sm:
                y = int(sm.group(1))
            elif not yearless:
                return
            else:
                y = _infer_year(mo, d, now, tz)
                if y is None:
                    return
                inferred = True
        day = date(y, mo, d)                    # raises on 31 February
        if doubts is not None:
            why = doubt or _date_doubt(text, a, b, day, inferred, base)
            if why:
                doubts[a] = why
        out.append((a, b, (y, mo, d)))
        taken.append((a, b))

    for n, (rx, conv) in enumerate(_DATE_RES):
        for m in rx.finditer(text):
            if any(m.start() < e and m.end() > s for s, e in taken):
                continue
            if n == 0 and _VERSION_BEFORE_RE.search(
                    text[max(0, m.start() - 16):m.start()]):
                continue
            try:
                ymd = conv(m)
                doubt = None
                if rx is _DMY_YEAR_RE:
                    ymd, doubt = _dmy_pick(text, m, ymd)
                _add(m.start(), m.end(), ymd, doubt=doubt)
            except (ValueError, KeyError):
                continue

    for m in _DAYSPAN_NUM_RE.finditer(text):
        if any(m.start() < e and m.end() > s for s, e in taken):
            continue
        if int(m.group(1)) >= int(m.group(2)):
            continue                    # "25-24/09/2026" is no day range
        try:
            _add(m.start(2), m.end(),
                 (int(m.group(5)), int(m.group(4)), int(m.group(2))))
        except (ValueError, KeyError):
            continue

    if yearless:
        for rx in (_RISKY_DATE_RE, _RISKY_DOT_DATE_RE):
            for m in rx.finditer(text):
                if any(m.start() < e and m.end() > s for s, e in taken):
                    continue
                got = _risky_reading(text, m, now, tz,
                                     dotted=rx is _RISKY_DOT_DATE_RE)
                if got is None:
                    continue
                try:
                    _add(m.start(), m.end(), got[0], inferred=True,
                         doubt=got[1])
                except (ValueError, KeyError):
                    continue

    out.sort(key=lambda p: p[0])
    return out


def _dmy_pick(text: str, m, ymd):
    """-> ((y, m, d), doubt) for a 23/09/2026-shaped match.

    Policy 1 (NOTICE_DATE_ORDER) breaks a <=12-both-ways tie ONLY when nothing
    else in the text does. A weekday written against the date does: "05/10/2026
    (Mon)" is 5 October (a Monday), whatever the flag says, because 10 May 2026
    is a Sunday. Both readings on that weekday, or neither, is a date the text
    cannot settle.
    """
    a, b = int(m.group(1)), int(m.group(2))
    if a > 12 or b > 12 or a == b:
        return ymd, None
    wd = _weekday_near(text, m.start(), m.end())
    if wd is None:
        return ymd, None
    y = ymd[0]
    both = [(y, ymd[1], ymd[2]), (y, ymd[2], ymd[1])]
    hits = [r for r in both if date(*r).weekday() == wd[0]]
    if len(hits) == 1:
        return hits[0], None
    return ymd, ("'{} {}' fits {} both ways round, so its day and month cannot "
                 "be told apart".format(m.group(0), wd[1], _WD_LONG[wd[0]])
                 if hits else None)


# ---------------------------------------------------------------------------
# Times
# ---------------------------------------------------------------------------

# 到, till, through and thru join two clocks exactly as 至 and "until" do.
# Without them "维护时间：2026年9月23日 10:00 到 12:00" and "10:00 till 12:00"
# had no range at all and were dropped as unparsed (F44). "till" is listed
# before "til" so the alternation never stops one letter short.
_DASH = (r"(?:-{1,2}|–|—|~|～|〜|至|到|to|until|till|til|through|thru)")
_AMPM = r"([AaPp]\.?[Mm]\.?)"
# The meridiem after a clock. Dotted "p.m." is as common as "PM" in English
# prose and was not accepted after a colon clock, so "10:00 - 11:30 p.m." was
# written as the MORNING 10:00-11:30 (F12). midnight / noon, and the
# Philippine "12 MN" / "12 NN", are meridiems of hour 12 only (see _h24).
# The letter guard keeps "am"/"MN" from being read out of a longer word.
_AMPM_WORD = (r"(?:[AaPp]\.?[Mm]\.?(?![A-Za-z])|(?i:midnight|noon)\b"
              r"|MN\b|NN\b)")
# The Chinese meridiem words, written IN FRONT of the clock: "下午2:00-4:00".
# They were not in any rule, so the clock after them was read on a 24-hour
# dial and "下午2:00-4:00" - 14:00-16:00 - went onto the row as 02:00-04:00,
# a window that does not overlap the real outage (F13). _MER_CLASS says what
# each one means.
_ZH_MER = (r"(?:凌晨|清晨|早上|早晨|上午|中午|下午|傍晚|晚上|晚间|晚間|夜间"
           r"|夜間|夜里|夜裡)")
# "22:00 至 次日02:00": the marker says the END clock is on the day after the
# stated date. Standing between the connector and the clock it broke every
# range rule, so the most explicit overnight notice there is was dropped (F44).
_NEXT_DAY = r"(?:次日|翌日|隔天|隔日|第二天|第二日|(?i:(?:the\s+)?next\s+day))"


def _clock_re(i: int, *, next_day: bool = False) -> str:
    """One end of a colon range, as named groups h<i> m<i> a<i> z<i> (+ nd).

    The lookbehinds are what the colon rule never had: without them the
    engine started inside "10:00:00" and read its MINUTES:SECONDS, so
    "10:00:00 - 12:00:00" was written as 00:00-12:00 and "08:15:00" became
    15:00 (F11). The seconds themselves are accepted and dropped - the Base
    stores minutes - and (?!\\d) stops a seconds-less match ending inside
    a longer number.
    """
    # R1.26: the marker may stand on the START too - "9月23日 次日 02:00 至 次日
    # 04:00" is two hours on the 24th, and was written as 26 from the 23rd.
    # R1.6: and AFTER the end clock - "4:00 PM - 6:00 (next day)" / "6:00 PM -
    # 8:00 next day" - where it was not read, the PM was carried onto the bare
    # end, and the row got 16:00-18:00 for an overnight window.
    nd = (r"(?:(?P<nd>" + _NEXT_DAY + r")\s*)?" if next_day
          else r"(?:(?P<sd>" + _NEXT_DAY + r")\s*)?")
    tail = (r"(?:[ \t]*[(（]?[ \t]*(?P<ndt>" + _NEXT_DAY + r")[ \t]*[)）]?)?"
            if next_day else "")
    return (nd + r"(?:(?P<z{0}>" + _ZH_MER + r")\s*)?"
            # F11: never right after a "+" (a UTC offset), nor after the
            # seconds of an ISO clock ("10:00:00-05:00"): "10:00:00+08:00 -
            # 12:00:00+08:00" was read as 08:00-12:00. A clock that carries
            # its OWN seconds is a range end, not an offset ("10:00:00-12:00:00").
            r"(?<!\d)(?<!\d:)(?<!\d\.)(?<!\+)"
            r"(?:(?<!\d:\d\d:\d\d-)|(?=\d{{1,2}}:\d{{2}}:\d{{2}}))"
            r"(?P<h{0}>\d{{1,2}}):(?P<m{0}>\d{{2}})"
            r"(?::\d{{2}})?(?!\d)\s*(?P<a{0}>" + _AMPM_WORD + r")?"
            ).format(i) + tail


# The colon range. Every other shape is a separate rule tried afterwards, so
# widening the time vocabulary cannot regress it.
# F11: an ISO-8601 offset may stand between the start clock and the dash -
# "10:00:00+08:00 - 12:00:00+08:00", "10:00:00 UTC+08:00 - ...". A bare glued
# "-" counts only after seconds, where it cannot be the range's own dash.
_ISO_OFF = (r"(?:(?:(?<=\d:\d\d:\d\d)-|\+|[ \t]+(?:(?:UTC|GMT)[ \t]?[+-]|\+))"
            r"\d{2}:\d{2}(?![:\d]))?")
_RANGE_RE = re.compile(
    _clock_re(1) + _ISO_OFF + r"\s*" + _DASH + r"\s*" + _clock_re(2, next_day=True))

# "between 10:00 and 12:00": "and" is a connector ONLY after "between" - on
# its own it joins two separate windows ("10:00-11:00 and 14:00-15:00"). The
# lookbehind keeps the match starting at the clock, where every consumer of a
# range span expects it.
_RANGE_BETWEEN_RE = re.compile(
    r"(?:(?<=[Bb]etween\s)|(?<=[Bb]etween\s\s))" + _clock_re(1)
    + r"\s*and\s*" + _clock_re(2, next_day=True))

# 10.00-12.00. Guarded on three sides, because a dot between digits is far more
# often a date, a version number or a decimal than a clock: BOTH ends must use
# the dot (a mixed "10.00-12:00" is not accepted), neither end may be part of a
# longer dotted run, and a candidate that overlaps a date the parser already
# read is discarded outright in _time_ranges.
_RANGE_DOT_RE = re.compile(
    # Same label-colon trap as _RANGE_HOUR_RE: a flat (?<![\d.:]) also rejected
    # "时间Time：10.00-12.00", because the label's own colon sits right in front
    # of the hour. Only a colon or dot with a DIGIT before it belongs to a
    # number this rule must stay out of.
    # Hunt p01: the meridiem after either end ("2.30 - 4.30 pm", "10.00 -
    # 11.30 pm GMT+8") - dropped, it wrote the afternoon window at 02:30.
    r"(?<!\d)(?<!\d:)(?<!\d\.)(?P<dh1>\d{1,2})\.(?P<dm1>\d{2})\s*(?P<da1>" + _AMPM_WORD
    + r")?\s*" + _DASH + r"\s*(?P<dh2>\d{1,2})\.(?P<dm2>\d{2})(?![\d.])(?:\s*(?P<da2>"
    + _AMPM_WORD + r"))?")

# 10AM - 12PM / 9AM-11AM. The lookbehind is what keeps it out of the minutes of
# "10:00AM - 12:00PM", where it would otherwise read "00AM".
_RANGE_HOUR_RE = re.compile(
    # The lookbehind must reject the MINUTES of "10:00AM - 12:00PM" (where it
    # would read "00AM") without rejecting a time that simply follows a label
    # colon. A flat (?<![\d:.]) did both, so "时间Time：10AM - 12PM" - the exact
    # label style the live KingMidas notice uses - produced no window at all
    # while the identical "Time: 10AM - 12PM" parsed. Only a colon with a DIGIT
    # in front of it is part of a clock.
    r"(?<!\d)(?<!\d:)(?<!\d\.)(?P<h1>\d{1,2})\s*(?P<a1>" + _AMPM_WORD
    + r")\s*" + _DASH + r"\s*(?:(?P<nd>" + _NEXT_DAY + r")\s*)?"
    r"(?P<h2>\d{1,2})\s*(?P<a2>" + _AMPM_WORD + r")(?![\d:])")

# A bare hour on ONE end and a colon clock on the other: "10PM - 11:30PM",
# "10:00 - 11PM", "10:00 - 12 noon". Neither rule above reads the pair, so
# the notice was dropped as unparsed (F12). The bare hour must carry its
# meridiem - "10 - 11:30" is not a clock.
_RANGE_MIX_HEAD_RE = re.compile(
    r"(?<!\d)(?<!\d:)(?<!\d\.)(?P<h1>\d{1,2})\s*(?P<a1>" + _AMPM_WORD
    + r")\s*" + _DASH + r"\s*" + _clock_re(2, next_day=True))
_RANGE_MIX_TAIL_RE = re.compile(
    _clock_re(1) + r"\s*" + _DASH + r"\s*(?:(?P<nd>" + _NEXT_DAY + r")\s*)?"
    r"(?<!\d)(?P<h2>\d{1,2})\s*(?P<a2>" + _AMPM_WORD + r")(?![\d:])")

# 10时 - 12时, the mainland studios' hour marker. "24小时" cannot match because
# 小 sits between the digits and 时. It takes the Chinese meridiem in front
# of either hour and the next-day marker too - "下午2时-4时" is 14:00-16:00.
_RANGE_CJK_RE = re.compile(
    r"(?:(?P<z1>" + _ZH_MER + r")\s*)?(?<!\d)(?P<h1>\d{1,2})\s*[时時]\s*"
    + _DASH + r"\s*(?:(?P<nd>" + _NEXT_DAY + r")\s*)?(?:(?P<z2>" + _ZH_MER
    + r")\s*)?(?P<h2>\d{1,2})\s*[时時](?!\d)")


# A window written on two labelled lines: "Start: 2026-09-24 22:00 (GMT+8)" /
# "End: 2026-09-25 02:00 (GMT+8)", "开始时间：…" / "结束时间：…". The label is
# the whole line in front of the value, so "From John: 2026-09-24 22:00" is
# not one. Only a START line directly followed by an END line pairs (F43).
_PAIR_LEAD = (r"[^\w\n]*(?:\d{1,2}[.)、]\s*)?(?:(?:maintenance|downtime"
              r"|estimated|expected|scheduled|维护|維護|停机|停機|预计|預計)\s*)?")
_PAIR_TAIL = (r"(?:\s*(?:time|date(?:\s*(?:&|and)\s*time)?|at|on)"
              r"|时间|時間|日期)?\s*[:：]?\s*")
_PAIR_START_RE = re.compile(
    _PAIR_LEAD + r"(?:start(?:s|ing)?|begin(?:s|ning)?|from|开始|開始|起始)"
    + _PAIR_TAIL, re.I)
_PAIR_END_RE = re.compile(
    _PAIR_LEAD + r"(?:end(?:s|ing)?|finish(?:es|ing)?|until|till|to"
    r"|结束|結束|完成|截止)" + _PAIR_TAIL, re.I)
# The same two lines with a bare CLOCK on each, the date on a line of its own:
# "Date: 2026-09-24" / "Start: 10:00" / "End: 12:00 (GMT+8)" had no range at
# all. The Start line may carry a zone after its clock and nothing else; the
# span runs from the first clock to the second, like any other range.
_RANGE_LABELLED_RE = re.compile(
    r"(?im)^" + _PAIR_LEAD
    + r"(?:start(?:s|ing)?|begin(?:s|ning)?|from|开始|開始|起始)" + _PAIR_TAIL
    + _clock_re(1)
    + r"[ \t]*(?:[(（\[][^)）\]\n]{1,24}[)）\]]"
      r"|(?:GMT|UTC)[ \t]*[+-]?[ \t]*\d{1,2}(?::?\d{2})?|[A-Z]{2,5})?[ \t]*"
    + r"\n(?:[ \t]*\n)*" + _PAIR_LEAD
    + r"(?:end(?:s|ing)?|finish(?:es|ing)?|until|till|to|结束|結束|完成|截止)"
    + _PAIR_TAIL + _clock_re(2, next_day=True))


def _named_pair(m):
    """((h, mi, meridiem), (h, mi, meridiem, next_day)) from the named groups.

    The meridiem is the English one after the clock or, failing that, the
    Chinese word in front of it. ``next_day`` is only ever on the END: it is
    the 次日 / "next day" marker, and _build puts the end on the day after.
    """
    g = m.groupdict()

    def one(i):
        return (int(g["h%d" % i]), int(g.get("m%d" % i) or 0),
                g.get("a%d" % i) or g.get("z%d" % i))
    both = bool(g.get("sd")) and bool(g.get("nd"))
    return (one(1) + ((True,) if g.get("sd") else ()),
            one(2) + (bool(g.get("nd") or g.get("ndt")) and not both,))


_RANGE_RES = [
    (_RANGE_RE, _named_pair),
    (_RANGE_BETWEEN_RE, _named_pair),
    (_RANGE_HOUR_RE, _named_pair),
    (_RANGE_MIX_HEAD_RE, _named_pair),
    (_RANGE_MIX_TAIL_RE, _named_pair),
    (_RANGE_CJK_RE, _named_pair),
    (_RANGE_LABELLED_RE, _named_pair),
]

# The dotted clock is a real but RARE European form, and it is indistinguishable
# from money, a version number, a latency, a load average or a duration: every
# one of "Min bet is 1.00 - 5.00 USD", "Client versions v2.10 - 3.00",
# "Expect 2.00 - 4.00 hours of impact" and "Latency 1.20 - 2.40 ms" satisfies
# all three guards on _RANGE_DOT_RE. Read alongside a real window it was UNIONed
# with it, so a correct two-hour outage went onto the row as an eleven-hour one.
#
# So it is a FALLBACK, not an alternative: it is consulted only on a line where
# no ordinary clock rule matched. A notice that states its window with a colon
# - which is all of them bar the dotted-form minority - can no longer have a
# price list on the same line rewrite it, a price list on another line still
# has to pass the guards below and then the relevance test in _collect, and a
# genuinely dotted window parses on its own line whatever the footer says.
_RANGE_FALLBACK_RES = [
    (_RANGE_DOT_RE, lambda m: ((int(m.group("dh1")), int(m.group("dm1")), m.group("da1")),
                               (int(m.group("dh2")), int(m.group("dm2")), m.group("da2")))),
]

# Even as a fallback the dotted form needs positive evidence that it is a clock.
# Required somewhere on its own line: a timezone token, a date, or a word that
# introduces a time.
_DOT_CLOCK_OK_RE = re.compile(
    r"(?:GMT|UTC)\s*[+-]|\d{4}[-/.]\d{1,2}|[年月]|日期|時間|时间|维护|維護"
    r"|\btime\b|\bdate\b|maintenance|downtime|window|schedul",
    re.I)
# ...and nothing on that line that marks the numbers as something else.
# Whole words only, and a unit only straight after a dotted number: the veto
# used to match INSIDE words - "between" (bet), "mistake" (stake),
# "conversion" (version), 维护单元 and 元旦 (元) - and on any "hours" or
# "minutes" anywhere on the line, so "on 23.09.2026 between 10.00 - 12.00"
# and "10.00 - 12.00 (GMT+8) (2 hours)" were dropped as unparsed (F62).
# "Expect 2.00 - 4.00 hours" and "1.20 - 2.40 ms" are still vetoed by the
# unit that follows the number itself.
_DOT_CLOCK_VETO_RE = re.compile(
    r"[$€£¥₱]|\bUSD\b|\bEUR\b|\bPHP\b|\bRMB\b|\bCNY\b|\bMYR\b|美元|人民币"
    r"|\d\s*元|元\s*\d"
    r"|\bv?\d+\.\d+\b\s*(?:client|build|release)"
    # R1.29: "10.00 - 12.00 hrs" is a clock with its unit; only a duration
    # INTRODUCED as one ("estimated 2.00 - 4.00 hrs", "expect 2.00 - 4.00 hours
    # of impact") is vetoed for hours. ms / minutes / seconds never label a clock.
    r"|(?:estimated|about|approx\w*|around|lasting|takes?|expect\w*|roughly"
    r"|up\s+to)\s+[^\n]{0,12}?\d\.\d{2}\s*(?:hours?|hrs?)\b"
    r"|\d\.\d{2}\s*(?:ms|minutes?|mins?|seconds?|secs?)\b"
    # F62: a zh unit or "h" after an estimate - "预计 2.00-4.00 小时",
    # "estimated 2.00 - 4.00 h".
    r"|\d\.\d{2}\s*(?:个|個)?(?:小时|小時|分钟|分鐘)"
    r"|(?:estimated|about|approx\w*|around|lasting|takes?|expect\w*|roughly|up\s+to"
    r"|预计|預計|大约|大約|约|約)\s*[^\n]{0,12}?\d\.\d{2}\s*(?:h|hr)\b"
    r"|\bms\b|load\s+average|latency",
    re.I)
# F62: bet / stake / version / odds words mark the numbers only when they stand
# right in front of them (or % right after): "维护期间暂停投注" and "in the new
# version" later on the line vetoed a genuine "10.00-12.00" window.
_DOT_CLOCK_NEAR_VETO_RE = re.compile(
    r"(?:\bbets?\b|\bbetting\b|\bstakes?\b|投注|下注|赔率|倍率|\bversions?\b|\bv\d"
    r"|\bodds\b|\bRTP\b|\blimits?\b|\bpayouts?\b)[^\n\d]{0,20}$", re.I)


# What each meridiem word means. "eve" (晚上 and kin) is not plain PM: 晚上12点
# is the midnight that ENDS the evening, and 晚上1点 is colloquially 1 AM of
# the NEXT day - a day the text does not state, so it is refused (_h24).
# 中午 is noon or just after it: 中午12点 is 12:00, 中午1点 13:00.
_MER_CLASS = {
    "am": "am", "pm": "pm", "midnight": "mid", "mn": "mid",
    "noon": "noon", "nn": "noon",
    "凌晨": "am", "清晨": "am", "早上": "am", "早晨": "am", "上午": "am",
    "中午": "zhnoon", "下午": "pm", "傍晚": "pm",
    "晚上": "eve", "晚间": "eve", "晚間": "eve", "夜间": "eve", "夜間": "eve",
    "夜里": "eve", "夜裡": "eve",
}


def _mer(ampm):
    """The class of a meridiem word (see _MER_CLASS), or None for none."""
    if not ampm:
        return None
    return _MER_CLASS.get(ampm.replace(".", "").lower())


def _h24(h: int, ampm, *, end: bool = False) -> int:
    """The 24-hour hour of ``h`` under ``ampm``; 24 is the midnight ENDING the day.

    ``end`` only matters for midnight: "10:00 PM - 12:00 midnight" ends at
    24:00, while "12 midnight - 2:00 AM" starts at 00:00 - the same reading
    12:00 AM always had. An hour the meridiem contradicts ("14:00 AM") or
    cannot place (晚上1点, see _MER_CLASS) raises ValueError: no window is
    better than a guessed one. "14:00 PM" and "下午14:00" merely say it twice
    and are 14:00 - they used to become hour 26 and drop the notice.
    """
    k = _mer(ampm)
    if k is None:
        return h
    if k == "am":
        if h == 12:
            return 0
        if h <= 12:
            return h
        raise ValueError("{}:xx with {!r}".format(h, ampm))
    if k == "pm":
        return h + 12 if 1 <= h <= 11 else h
    if k == "mid":
        if h in (0, 12, 24):
            return 24 if end else 0
        return h
    if k == "noon":
        return h
    if k == "zhnoon":
        return h + 12 if 1 <= h <= 5 else h
    # "eve": 晚上6点-11点 are 18:00-23:00, 晚上12点 is the midnight ending it.
    if 6 <= h <= 11:
        return h + 12
    if h in (0, 12):
        return 24
    if h >= 13:
        return h
    raise ValueError("{}:xx with {!r} falls on an unstated day".format(h, ampm))


def _clock(h: int, mi: int, ampm, *, end: bool = False):
    """-> (day_offset, hour, minute), with 24:00 resolved.

    24:00 is the midnight that ENDS the stated day, i.e. 00:00 of the next one,
    and it is resolved HERE - before the "end <= start rolls over a day" rule -
    so "22:00-24:00 on the 23rd" ends on the 24th and not the 25th, and
    "24:00-02:00" is a two-hour window and not a twenty-six-hour one. Until this
    existed, hour 24 raised ValueError inside datetime(), the guard around it
    skipped the range, and the most ordinary overnight window in the industry
    produced no window at all. 12:00AM is untouched: _h24 has already made it
    00:00, midnight at the START of the day, and the two must never be confused.
    """
    h = _h24(h, ampm, end=end)
    if h == 24 and mi == 0:
        return 1, 0, 0
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        raise ValueError("not a clock time: {}:{:02d}".format(h, mi))
    return 0, h, mi


def _readings(t1, t2):
    """Every (t1, t2) the text allows when only ONE end states its meridiem.

    "8:00 - 11:00 PM" was read as 08:00-23:00 and "9:00PM - 11:00" as
    21:00 -> 11:00 the next day: the bare end was put on a 24-hour dial, so
    the row got a 13-15h window starting or ending half a day off (F12).
    Written that way a meridiem covers the bare end too, UNLESS that makes
    no sense - "10:00 - 12:00 PM" is 10:00 to noon, not 22:00 to noon. So a
    bare end with an hour of 1-12 is tried as AM and as PM and _build keeps
    the shorter window; the two readings are exactly 12h apart, so exactly
    one is under 12h and nothing is left to guess. A bare hour of 0 or 13+
    is a 24-hour clock and has one reading.
    """
    m1, m2 = _mer(t1[2]), _mer(t2[2])
    if (m1 is None) == (m2 is None):
        return [(t1, t2)]
    bare = t1 if m1 is None else t2
    if not 1 <= bare[0] <= 12:
        return [(t1, t2)]
    alts = [(bare[0], bare[1], "am"), (bare[0], bare[1], "pm")]
    return [((x, t2) if m1 is None else (t1, x)) for x in alts]


def _build(ymd, t1, t2, tz):
    """-> (start, end) of the window t1-t2 on the day ``ymd`` in ``tz``.

    ``t2`` may carry a fourth item, the 次日 / "next day" marker: the end is
    then on the day after ``ymd`` whatever the clocks say, and an end that
    still lands at or before the start is refused rather than rolled again.
    When one end states a meridiem and the other does not, the shorter of
    the two readings wins (_readings).
    """
    nd = len(t2) > 3 and bool(t2[3])
    if len(t1) > 3 and t1[3]:
        # 次日 in front of the START: the whole window is on the next day.
        ymd = (date(*ymd) + timedelta(days=1)).timetuple()[:3]
    best, err = None, None
    for a, b in _readings(tuple(t1[:3]), tuple(t2[:3])):
        try:
            s, e = _build_one(ymd, a, b, tz, nd)
        except ValueError as x:
            err = x
            continue
        if best is None or e - s < best[1] - best[0]:
            best = (s, e)
    if best is None:
        raise err or ValueError("no reading")
    return best


def _build_one(ymd, t1, t2, tz, nd=False):
    y, mo, d = ymd
    sday, sh, sm = _clock(*t1)
    eday, eh, em = _clock(*t2, end=True)
    try:
        start = datetime(y, mo, d, sh, sm, tzinfo=tz) + timedelta(days=sday)
        end = datetime(y, mo, d, eh, em, tzinfo=tz) + timedelta(days=eday)
        if nd:
            end += timedelta(days=1)
            if end <= start:
                raise ValueError("the next-day end is not after the start")
        if end <= start:
            end += timedelta(days=1)
        if end == start:
            # "24:00 - 00:00" and its two spellings: _clock has already pushed
            # the START into the next day, so the rollover above lands the end
            # exactly ON it. Of all 167,281 clock pairs this file accepts these
            # are the only six that do it. A zero-length outage is not a window
            # anyone announced, and writing Start Time == End Time onto the row
            # flips the sheet's Maintenance formula to "Yes" for no downtime -
            # so the candidate is refused rather than repaired.
            raise ValueError("zero-length window")
    except OverflowError:
        # datetime.max + one rollover day. A garbled "9999-12-31 23:00-01:00"
        # is not a window; it must not be an exception either, because this
        # runs unattended over whatever the group happens to contain.
        raise ValueError("date out of range")
    return start, end


def _time_ranges(text: str, dspans):
    """[(start, end, t1, t2)] for every time range, in document order.

    Forms are tried most-specific first and a candidate overlapping one already
    accepted is dropped, so "10:00AM - 12:00PM" is read once by the colon rule
    and not a second time by the bare-hour rule. A candidate overlapping a DATE
    is dropped too — that is what stops the dotted-clock rule reading "26.09"
    out of "2026.09.23".
    """
    taken = []
    out = []
    for rx, conv in _RANGE_RES:
        for m in rx.finditer(text):
            # The colon rule ends in \s*(AM|PM)? and so swallows the NEWLINE
            # after a range with no meridiem. Every consumer that asks "which
            # line is this range on" then measured from the next line: the
            # zone of line 2 was stamped on line 1's range, and line 1's
            # sentence ran on into line 2's. The span stops at the last
            # non-blank character.
            a, b = m.start(), m.start() + len(m.group(0).rstrip())
            # A range starts at its first clock (or the 下午 in front of it),
            # not at a label the rule had to read to find it (Start: / End:).
            a = m.start("z1") if m.groupdict().get("z1") else m.start("h1")
            if any(a < e and b > s for s, e in taken):
                continue
            if any(a < e and b > s for s, e in dspans):
                continue
            taken.append((a, b))
            t1, t2 = conv(m)
            out.append((a, b, t1, t2))
    # Only now the dotted form, and only on a LINE that carries no ordinary
    # range - see _RANGE_FALLBACK_RES for why it may never compete with a colon
    # range. The test used to be per MESSAGE, so one colon range anywhere
    # switched the dotted form off everywhere: "Time: 10.00 - 12.00" plus a
    # footer "Support desk hours: 09:00 - 18:00" threw the real window away
    # and wrote the desk hours instead. Per line, a price list typed on the
    # window's own line still cannot widen it (J7-J9), and a dotted window on
    # its own labelled line still parses next to a colon footer.
    busy = [(text.rfind("\n", 0, a) + 1) for a, _b, _t1, _t2 in out]
    for rx, conv in _RANGE_FALLBACK_RES:
        for m in rx.finditer(text):
            a, b = m.start(), m.start() + len(m.group(0).rstrip())
            if any(a < e and b > s for s, e in taken):
                continue
            if any(a < e and b > s for s, e in dspans):
                continue
            ls = text.rfind("\n", 0, a) + 1
            if ls in busy:
                continue
            le = text.find("\n", b)
            line = text[ls:len(text) if le == -1 else le]
            if not _DOT_CLOCK_OK_RE.search(line):
                continue
            if _DOT_CLOCK_VETO_RE.search(line):
                continue
            if (_DOT_CLOCK_NEAR_VETO_RE.search(text[max(ls, a - 30):a])
                    or re.match(r"\s*%", text[b:b + 4])):
                continue
            taken.append((a, b))
            t1, t2 = conv(m)
            out.append((a, b, t1, t2))
    out.sort(key=lambda r: r[0])
    return out


def _datetime_points(text: str, dates):
    """[(start, end, (y, m, d), hh, mm, ampm)] for every "<date> <time>" pair.

    A date immediately followed by a time is one instant. Two of them joined by
    a dash is a window that spans days - which the time-only range regex cannot
    see, because a whole date sits between the dash and the second time.
    """
    out = []
    for pos, end, ymd in dates:
        # Tolerate a weekday or bracketed token between the date and the time:
        # "Sep 16th, 2026 (Wed) 22:00" and "2026.09.23 周三 06:00" both occur.
        # The clock takes the same seconds, dotted meridiem and Chinese
        # meridiem word as a range end: "2026-09-23 10:00:00 ~ 2026-09-23
        # 12:00:00" had no point at all, and "9月23日 下午2:00" lost its 下午.
        # R1.19: [ \t], never \s - a point is one line. The \s after the clock
        # swallowed the line end, the window's span reached into the next line
        # ("Please plan accordingly."), and a line that names no maintenance
        # dropped the whole window.
        t = re.match(r"[ \t,，]*(?:[\(（][^)）\n]{1,12}[\)）][ \t,，]*)?"
                     r"(?:(?P<z>" + _ZH_MER + r")[ \t]*|[^\s\d:]{1,4}[ \t,，]*)?"
                     r"(\d{1,2}):(\d{2})(?::\d{2})?(?!\d)[ \t]*"
                     r"(" + _AMPM_WORD + r")?", text[end:])
        if not t:
            continue
        out.append((pos, end + len(t.group(0).rstrip()), ymd, int(t.group(2)),
                    int(t.group(3)), t.group(4) or t.group("z")))
    out.sort(key=lambda p: p[0])
    return out


# ---------------------------------------------------------------------------
# Timezones
# ---------------------------------------------------------------------------
#
# A fixed, greppable table: no DST and no guessing. EST is -05 flat because
# these notices come from studios that mean "New York", and a parser that
# guessed EDT in summer would move a window by an hour with nothing on the page
# to justify it. A name that is NOT in this table is never invented: next to a
# range it sends the notice to a human (_unknown_zone_after / _before), and
# anywhere else it is no zone at all.
_TZ_NAMES = {
    "SGT": 8 * 60, "MYT": 8 * 60, "PHT": 8 * 60, "HKT": 8 * 60,
    # PHST is Philippine Standard Time spelled out - no other zone uses the
    # letters. Missing from the table, it read as an unknown zone and sent a
    # plain "10:00 - 12:00 PHST" notice to a person, where bd9fc12 filled
    # it on the home clock (R1.8).
    "PHST": 8 * 60,
    # R1.77: Beijing Time, the mainland studios' own label - "(BJT)" alone was
    # read as a zone the parser does not know and sent to a person.
    "BJT": 8 * 60,
    # regress#7: the other +08 abbreviations a notice beside "(UTC+8, AWST)"
    # uses - Western Australia, central Indonesia, Brunei, Ulaanbaatar,
    # Irkutsk. Unknown, each sent a determinable +08 notice to a person.
    "AWST": 8 * 60, "WITA": 8 * 60, "BNT": 8 * 60, "ULAT": 8 * 60, "IRKT": 8 * 60,
    "JST": 9 * 60, "KST": 9 * 60,
    "ICT": 7 * 60, "WIB": 7 * 60,
    "EST": -5 * 60, "EDT": -4 * 60, "PDT": -7 * 60,
}

# The abbreviations that name two or more zones. PST is the one that matters
# here: in the Philippines it is Philippine Standard Time - the operator's own
# clock, and the abbreviation Python's tzdata itself prints for Asia/Manila -
# while the table read it as US Pacific (-08), sixteen hours off, so a
# "10:00 - 12:00 PST" window landed on the row the next morning (F14). CST
# (China / US Central) and IST (India / Ireland / Israel) are the same trap.
# Each is resolved, in this order, by (_resolve_ambiguous):
#   1. an explicit offset or unambiguous zone on the SAME line that equals one
#      reading: "10:00 - 12:00 PST (GMT+8)" is +08, "PST (UTC-8)" is -08;
#   2. the reading that equals NOTICE_TZ's own offset - for this deployment
#      (Asia/Manila) PST and CST are +08, which is what a Philippine or Chinese
#      studio writing to a Philippine operator means;
#   3. otherwise nothing: the window goes to a human, because every remaining
#      reading is a guess hours off (IST under Asia/Manila, or a PST line that
#      also says GMT+9).
_GLOSS_RE = {
    "Philippine": re.compile(r"philippine|philippines|manila|\bPH\b|菲律宾|菲律賓|马尼拉|馬尼拉", re.I),
    "US Pacific": re.compile(r"\bpacific\b|\bLos\s+Angeles\b|\bUS\s+west|美国西部|美國西部|太平洋", re.I),
    "China": re.compile(r"\bchina\b|\bbeijing\b|中国|中國|北京", re.I),
    "US Central": re.compile(r"\bcentral\s+(?:standard\s+)?time\b|\bUS\s+central\b|\bChicago\b", re.I),
    "India": re.compile(r"\bindia(?:n)?\b|印度", re.I),
    "Irish": re.compile(r"\birish\b|\bireland\b|\bdublin\b", re.I),
    "Israel": re.compile(r"\bisrael(?:i)?\b", re.I),
}
_TZ_AMBIGUOUS = {
    "PST": ((8 * 60, "Philippine"), (-8 * 60, "US Pacific")),
    "CST": ((8 * 60, "China"), (-6 * 60, "US Central")),
    "IST": ((5 * 60 + 30, "India"), (60, "Irish"), (2 * 60, "Israel")),
}

# Zones written as words. Places without daylight saving only, so the offset
# is a fact and not a season: "Bangkok time", "泰国时间" and "日本时间" used to
# fall back to NOTICE_TZ and land an hour off (F16). A place that HAS daylight
# saving (Malta, London, "Pacific Time") is deliberately absent - next to a
# range it is an unknown zone and a person reads the notice.
_TZ_WORDS = {
    8 * 60: ("北京时间", "北京時間", "新加坡时间", "新加坡時間", "中国时间",
             "中國時間", "香港时间", "香港時間", "台北时间", "台北時間",
             "台湾时间", "台灣時間", "澳门时间", "澳門時間", "马来西亚时间",
             "馬來西亞時間", "菲律宾时间", "菲律賓時間", "马尼拉时间",
             "馬尼拉時間",
             "philippine standard time", "philippine time",
             "philippines time", "manila time", "singapore time",
             "singapore standard time", "china standard time", "beijing time",
             "hong kong time", "taipei time", "malaysia time",
             "asia/manila", "asia/shanghai", "asia/singapore",
             "asia/hong_kong", "asia/taipei", "asia/kuala_lumpur"),
    9 * 60: ("日本时间", "日本時間", "东京时间", "東京時間", "韩国时间",
             "韓國時間", "首尔时间", "首爾時間",
             "japan standard time", "japan time", "tokyo time",
             "korea standard time", "korea time", "seoul time",
             "asia/tokyo", "asia/seoul"),
    7 * 60: ("河内时间", "河內時間", "胡志明时间", "胡志明時間", "泰国时间", "泰國時間", "曼谷时间", "曼谷時間", "越南时间",
             "越南時間", "雅加达时间", "雅加達時間",
             "indochina time", "bangkok time", "thailand time",
             "vietnam time", "jakarta time", "western indonesia time",
             "asia/bangkok", "asia/ho_chi_minh", "asia/jakarta"),
    5 * 60 + 30: ("印度时间", "印度時間", "india standard time",
                  "asia/kolkata"),
}
_TZ_WORD_OFF = {w: off for off, ws in _TZ_WORDS.items() for w in ws}
# R1.11: "UTC时间：02:00-04:00" beside "北京时间：10:00-12:00" is the SAME window in
# UTC; unread, it became a second window eight hours early and was written.
# (Keyed lower-case too: _offset_of looks words up after .lower().)
_TZ_WORD_OFF.update({w: 0 for w in ("utc时间", "utc時間", "gmt时间", "gmt時間")})
_TZ_WORD_OFF.update({w: 0 for w in ("UTC时间", "UTC時間", "GMT时间", "GMT時間",
                                    "格林威治时间", "格林威治時間",
                                    "格林尼治时间", "世界标准时间", "世界標準時間",
                                    "协调世界时")})

# The three-letter names are matched CASE-SENSITIVELY on purpose: a
# case-insensitive \bEST\b would fire on an ordinary English "Est." and hand a
# Manila provider a New York clock. UTC/GMT are folded because they are written
# both ways and mean the same thing either way.
#
# The compact 4-digit offset (GMT+0530) is the reason the hour group is followed
# by an OPTIONAL ":?(\d{2})" and the (?!\d) sits outside it: on "GMT+0530" the
# greedy hour takes "05" and the minutes take "30", and on "GMT+530" the engine
# backtracks the hour to "5" and still gets +05:30. Before this, the minutes
# were dropped and the window landed half an hour early.
#
# UTC/GMT with its sign LOST ("(GMT 8)", "(GMT 08:00)" - a "+" is exactly what
# a URL or a form eats) is the offset it names (``uh``), but only where an
# offset can stand - before a bracket, punctuation or the line end. "UTC
# 02:00-04:00" is a prefix-style converted RANGE, and stays a bare UTC.
#
# A bare UTC/GMT (``zulu``) is +00 only when no offset FOLLOWS it: "(UTC/GMT
# +08:00)" is how Chinese studios write Beijing time, and the zulu alternative
# used to win on its first three letters and write the window eight hours late
# (F8). The lookaheads step over it so the finditer reaches the signed offset.
_TZ_RE = re.compile(
    # R1.62: "UTC+5.5" is +05:30, and was read as +05:00 - the ".5" dropped.
    # F8: "GMT时间+8" / "UTC時間 +8" - the zh "time" between the name and the offset.
    r"(?i:GMT|UTC)\s*(?:时间|時間)?\s*(?P<gsign>[+-])\s*(?P<gh>\d{1,2})(?:[.,](?P<gfrac>5|25|75)(?!\d)"
    r"|(?::?(?P<gm>\d{2}))?(?!\d))"
    r"|(?<![\d:])(?P<bsign>[+-])(?P<bh>\d{1,2}):(?P<bm>\d{2})(?!\d)"
    # F11: the ISO-8601 offset glued to a clock's seconds - "10:00:00+08:00".
    r"|(?<=\d:\d\d:\d\d)(?P<isign>[+-])(?P<ih>\d{2}):(?P<im>\d{2})(?!\d)"
    # Hunt z05: "东七区" / "东7区" / "西五区" - the zh name of a UTC offset.
    r"|(?P<zhdir>[东東西])\s*(?P<zhn>1[0-2]|[1-9]|十[一二]?|[一二三四五六七八九])\s*[区區]"
    # Hunt z16: the glued, sign-less "GMT7" / "UTC0" / "GMT2" (a spaced
    # "GMT 7" was already read).
    r"|(?i:GMT|UTC)(?P<gz>1[0-4]|[0-9])(?![\d:.])"
    r"|(?i:GMT|UTC)[ \t]+(?P<uh>\d{1,2})(?::(?P<um>\d{2}))?(?![\d:.])"
    r"(?=[ \t]*(?:[)）\]\n,，;；。]|$|(?i:hours?|hrs?)\b))"
    r"|\b(?P<abbr>SGT|MYT|PHST|PHT|BJT|AWST|WITA|BNT|ULAT|IRKT|CST|HKT|JST|KST|ICT|WIB|IST|EST|EDT"
    r"|PST|PDT)\b"
    r"|(?P<cjk>" + "|".join(w for w in _TZ_WORD_OFF if not w.isascii()) + r")"
    r"|\b(?i:(?P<word>" + "|".join(sorted(
        (w.replace(" ", r"[ \t]+") for w in _TZ_WORD_OFF if w.isascii()),
        key=len, reverse=True)) + r"))\b"
    r"|\b(?i:(?P<zulu>UTC|GMT))\b(?!\s*/\s*(?i:GMT|UTC)\s*[+-]?\s*\d)"
    r"|(?<=\d)(?P<z>Z)(?![A-Za-z])")


def _tz_default():
    """NOTICE_TZ: the zone of a window that states none.

    A name ZoneInfo cannot load (a typo such as Asia/Kolkatta, or a host with
    no tz database) falls back to UTC+08:00, the Asia/Manila offset this
    deployment runs on. It used to do that silently, so a mistyped zone parsed
    every zone-less notice at +08 with nothing to say why (F83). It is now
    logged once, and /vacheck repeats it.
    """
    name = (os.getenv("NOTICE_TZ") or "").strip() or "Asia/Manila"
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(name)
    except Exception as err:          # noqa: BLE001
        _warn_config("NOTICE_TZ", "NOTICE_TZ={!r} cannot be loaded ({}) - a "
                                  "notice that states no zone is read at "
                                  "UTC+08:00".format(name, type(err).__name__))
        return timezone(timedelta(hours=8))


#: No place on earth is further from UTC than +14:00. Anything past that is a
#: typo or a number that merely looked like an offset ("GMT+9999", a stray
#: "+25:00"), and timezone() REFUSES it with a ValueError - which, before this
#: bound existed, escaped classify() and took the whole sweep down rather than
#: the one unreadable token. An offset out of range is treated exactly like a
#: zone name that is not in the table: not invented, just ignored.
_MAX_OFFSET_MIN = 14 * 60


def _offset_of(m):
    """The minutes east of UTC a _TZ_RE match denotes, or None."""
    if m.group("gh") is not None:
        frac = {"5": 30, "25": 15, "75": 45}.get(m.group("gfrac") or "", 0)
        off = int(m.group("gh")) * 60 + int(m.group("gm") or 0) + frac
        if off > _MAX_OFFSET_MIN or int(m.group("gm") or 0) > 59:
            return None
        return -off if m.group("gsign") == "-" else off
    if m.group("bh") is not None:
        off = int(m.group("bh")) * 60 + int(m.group("bm") or 0)
        if off > _MAX_OFFSET_MIN or int(m.group("bm") or 0) > 59:
            return None
        return -off if m.group("bsign") == "-" else off
    if m.groupdict().get("ih") is not None:
        off = int(m.group("ih")) * 60 + int(m.group("im"))
        if off > _MAX_OFFSET_MIN or int(m.group("im")) > 59:
            return None
        return -off if m.group("isign") == "-" else off
    if m.groupdict().get("zhdir"):
        n = m.group("zhn")
        zh = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9,
              "十": 10, "十一": 11, "十二": 12}
        hours = int(n) if n.isdigit() else zh.get(n)
        if hours is None:
            return None
        return (-hours if m.group("zhdir") == "西" else hours) * 60
    if m.groupdict().get("gz") is not None:
        return int(m.group("gz")) * 60
    if m.group("uh") is not None:
        off = int(m.group("uh")) * 60 + int(m.group("um") or 0)
        if off > _MAX_OFFSET_MIN or int(m.group("um") or 0) > 59:
            return None
        return off
    if m.group("abbr"):
        # None for PST/CST/IST: only _tz_tokens can resolve those, because
        # it takes the rest of the line into account (_resolve_ambiguous).
        return _TZ_NAMES.get(m.group("abbr"))
    for key in ("cjk", "word"):
        if m.group(key):
            return _TZ_WORD_OFF.get(re.sub(r"\s+", " ", m.group(key).lower()))
    if m.group("zulu") or m.group("z"):
        return 0
    return None


# An abbreviation is a zone only where a zone can stand: straight after a
# clock ("12:00 PM PST", "12:00 (ICT)"), straight before one ("PST 10:00",
# "(PST): 10:00"), or after a zone label ("Time zone: PST", "all times in
# EST"). Anywhere else it is a word: "our ICT team" is the IT department and
# an all-caps "EST. DURATION" is "estimated", and both used to put the window
# on the wrong clock (F14).
_ABBR_BEFORE_OK_RE = re.compile(
    r"(?:\d[:.]?\d{2}(?::\d{2})?[ \t]*(?:" + _AMPM_WORD + r")?"
    r"|\d[ \t]*" + _AMPM_WORD + r"|\d\s*[时時点點]"
    # ...or straight after a whole DATE, bracketed weekday and all: "Date:
    # 2026-09-23 (JST)", "Sep 23, 2026 (Wed) JST", "9月23日（JST）". A mere
    # digit is not enough - "our 24/7 ICT desk" is no zone.
    r"|(?:\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{1,2}[-/.]\d{1,2}[-/.]\d{4}"
    r"|(?i:" + _MONTH_WORD + r")\.?[ \t]*\d{1,2}(?:st|nd|rd|th)?(?:,?[ \t]*\d{4})?"
    r"|\d{1,2}(?:st|nd|rd|th)?[ \t]+(?i:" + _MONTH_WORD + r")\.?(?:,?[ \t]*\d{4})?"
    r"|[日号號])[ \t,，]*(?:[(（][^)）\n]{1,12}[)）][ \t]*)?"
    # ...or straight after an explicit offset, as its second name: "10:00 -
    # 12:00 (GMT+8 / PHT)", "(UTC+8, SGT)", "GMT+8 PST". Only the abbreviation
    # stood where the rule above did not look, so a notice whose every token
    # says +08 went to a person instead of filling (R1.8). Tying it to the
    # offset keeps it a zone for _resolve_ambiguous (PST settles on the +08
    # beside it) and for the two-zone check (GMT+8 / JST still disagrees).
    r"|(?i:GMT|UTC)[ \t]*[+\-][ \t]*\d{1,2}(?::?\d{2})?(?!\d)[ \t]*[/,，|]?"
    r"|(?i:time[ \t]*-?[ \t]*zone|\bzone|时区|時區|\bin"
    r"|\ball\s+times?(?:\s+(?:are|is))?))[ \t]*[:：]?[ \t]*[(（\[]?[ \t]*$")
_ABBR_AFTER_OK_RE = re.compile(
    r"[ \t]*[)）\]]?[ \t]*(?:[(（\[][^)）\]\n]{1,16}[)）\]][ \t]*)?[:：]?[ \t]*"
    r"(?:(?i:between|from|at)[ \t]+)?(?:" + _ZH_MER + r"\s*)?"
    r"(?:\d{1,2}[:.]\d{2}|\d{1,2}\s*[AaPp]\.?[Mm]|\d{1,2}\s*[时時点點])")
# A signless "(UTC 02:00)" straight after a LONE clock ("starts at 10:00 (UTC
# 02:00)") is either that clock converted to UTC or GMT+2 with its sign lost;
# the two readings are hours apart and the text does not say which.
_LONE_CLOCK_BEFORE_RE = re.compile(
    r"(\d{1,2}:\d{2}(?::\d{2})?\s*(?:" + _AMPM_WORD + r")?)[ \t]*[(（\[]?[ \t]*$")


# An abbreviation used as a WORD: followed by a full stop ("EST. DURATION" is
# "estimated") or a lower-case word ("ICT team", "(ICT) for help"), or led
# in by an article or a verb of contact ("our ICT", "contact ICT").
_ABBR_WORD_AFTER_RE = re.compile(r"[)）]?(?:\.(?!\d)|[ \t]+[a-z&])")
_ABBR_WORD_BEFORE_RE = re.compile(
    r"(?i:\b(?:our|the|your|their|its|contact|call|email|e-mail|via|by|of"
    r"|with|and)[ \t]+[(（]?)$")


_ZONE_MEMBER = (r"(?:(?:GMT|UTC)\s*[+-]\s*\d{1,2}(?::?\d{2})?|[A-Z]{2,5}T\b|UTC\b"
                r"|GMT\b|BJT\b|PHST\b)")
_ZONE_GROUP_BEFORE_RE = re.compile(_ZONE_MEMBER + r"\s*(?:[/,，&、|]|\band\b)\s*$", re.I)
_ZONE_GROUP_AFTER_RE = re.compile(r"\s*(?:[/,，&、|]|\band\b)\s*" + _ZONE_MEMBER, re.I)


_EST_ESTIMATE_RE = re.compile(r"[.:：]?[ \t]*(?:\d+(?:\.\d+)?|one|two|three|half)[ \t]*"
                              r"(?:hours?|hrs?|h\b|mins?|minutes?)", re.I)


def _abbr_placement(text: str, m, ls: int) -> str:
    """'zone' | 'word' | 'loose' for an abbreviation match (see _tz_tokens).

    zone   written against a time, a date or a zone label, or on its own in
           brackets ("Scheduled maintenance (JST)")
    word   plainly an ordinary word (_ABBR_WORD_*)
    loose  neither - a zone the text does not tie to anything
    """
    if re.match(r"[ \t]+(?:Support|support|Team|team|Department|department|Dept|dept|Staff|staff"
                r"|Helpdesk|helpdesk|will|shall|is|are)\b", text[m.end():]):
        # F14: "ICT Support will monitor" / "ICT Helpdesk" - more likely the IT
        # team than Indochina Time, but not certainly: a person reads it. With
        # an owner in front ("Our ICT helpdesk") it is plainly the team.
        return "word" if re.search(r"(?i)\b(?:our|the|your|their)\s*$", text[ls:m.start()]) else "loose"
    if m.group("abbr") == "EST" and _EST_ESTIMATE_RE.match(text, m.end()):
        # F14: "(EST. 2 HOURS)" / "EST 2 hours" is ESTIMATED, not US Eastern -
        # read as a zone it put the window thirteen hours out.
        return "word"
    if (_ABBR_BEFORE_OK_RE.search(text[ls:m.start()])
            or _ABBR_AFTER_OK_RE.match(text, m.end())):
        return "zone"
    # R1.77: a member of a zone GROUP - "(HKT/SGT)", "GMT+8 (HKT/SGT)", "(SGT &
    # HKT)", "(GMT+8, SGT)" - is placed like its neighbour. Only the first of
    # them touched the time, so the rest read as loose words and a notice every
    # token of which means +08 went to a person. Whether the members AGREE is
    # a separate question (_tz_for, _unknown_zone_after).
    if (_ZONE_GROUP_BEFORE_RE.search(text[ls:m.start()])
            or _ZONE_GROUP_AFTER_RE.match(text, m.end())):
        return "zone"
    if (_ABBR_WORD_AFTER_RE.match(text, m.end())
            or _ABBR_WORD_BEFORE_RE.search(text[ls:m.start()])):
        return "word"
    if (m.start() > 0 and text[m.start() - 1] in "(（"
            and text[m.end():m.end() + 1] in (")", "）")):
        return "zone"
    return "loose"


def _home_offsets():
    """NOTICE_TZ's offsets in minutes, winter and summer, this year."""
    home = _tz_default()
    y = datetime.now(timezone.utc).year
    return {int(datetime(y, mo, 15, 12, tzinfo=home).utcoffset().total_seconds()
                // 60) for mo in (1, 7)}


def _fmt_off(minutes: int) -> str:
    sign = "-" if minutes < 0 else "+"
    return "UTC{}{:02d}:{:02d}".format(sign, abs(minutes) // 60, abs(minutes) % 60)


def _resolve_ambiguous(text: str, m, resolved):
    """-> (minutes, None) or (None, reason) for a PST/CST/IST match.

    See _TZ_AMBIGUOUS for the order: an explicit zone on the same line, then
    NOTICE_TZ, then nobody - a reason for the card instead of a guess.
    """
    abbr = m.group("abbr")
    readings = _TZ_AMBIGUOUS[abbr]
    # R1.7: an explicit GLOSS decides first - "PST (Pacific Standard Time)",
    # "PST (US Pacific)", "PST = Pacific Standard Time". The home-clock rule
    # below read all three as Philippine (+08), sixteen hours from what the
    # notice itself says. Read on the abbreviation's line and the next one.
    ls0, le0 = _line_at(text, m.start())
    nxt = text.find("\n", le0 + 1)
    near = text[ls0:len(text) if nxt == -1 else nxt]
    gl = [r for r in readings if _GLOSS_RE[r[1]].search(near)]
    if len(gl) == 1:
        return gl[0][0], None
    ls, le = _line_at(text, m.start())
    line_offs = {off for a, _b, off in resolved if ls <= a < le}
    hit = [r for r in readings if r[0] in line_offs]
    if len(hit) == 1:
        return hit[0][0], None
    names = " or ".join("{} {}".format(n, _fmt_off(o)) for o, n in readings)
    if line_offs and not hit:
        return None, "{} ({}) disagrees with the {} on the same line".format(
            abbr, names, ", ".join(_fmt_off(o) for o in sorted(line_offs)))
    hit = [r for r in readings if r[0] in _home_offsets()]
    if len(hit) == 1:
        return hit[0][0], None
    return None, "{} may be {}".format(abbr, names)


def _tz_tokens(text: str, rspans, doubts=None):
    """[(start, end, minutes)] for every zone token that is not a range end.

    The bare-offset alternative happily matches the SEPARATOR of a time range:
    in "10:00 -12:00 (GMT+8)" it read " -12:00" as UTC-12 and put the window ~20
    hours out. So any candidate overlapping a time range is skipped - a range
    end is never a timezone.

    ``doubts``, when a list, receives (start, end, reason) for zone text that
    IS a zone but cannot be pinned to one offset: an ambiguous abbreviation
    nothing resolves, or a signless offset that may be a converted clock. The
    token is left out, and _collect sends the window to a human.
    """
    out = []
    pending = []
    for m in _TZ_RE.finditer(text):
        if any(m.start() < e and m.end() > s for s, e in rspans):
            continue
        if m.group("abbr"):
            ls = text.rfind("\n", 0, m.start()) + 1
            placed = _abbr_placement(text, m, ls)
            if placed == "word":
                continue
            if placed == "loose":
                # A zone name standing somewhere a zone is not usually
                # written ("Scheduled maintenance JST" on a heading line):
                # silently dropping it put the window on NOTICE_TZ, silently
                # keeping it is how "our ICT team" became +07. Neither.
                if doubts is not None:
                    doubts.append((m.start(), m.end(), "'{}' may be a time "
                                   "zone, but it is not written against a "
                                   "time".format(m.group("abbr"))))
                continue
            if m.group("abbr") in _TZ_AMBIGUOUS:
                pending.append(m)
                continue
        if m.group("um") is not None:
            ls = text.rfind("\n", 0, m.start()) + 1
            lone = _LONE_CLOCK_BEFORE_RE.search(text[ls:m.start()])
            if lone and not any(s <= ls + lone.start(1) < e for s, e in rspans):
                if doubts is not None:
                    doubts.append((m.start(), m.end(), "'{}' after a single "
                                   "clock may be that clock in UTC or an offset "
                                   "without its sign".format(m.group(0))))
                continue
        off = _offset_of(m)
        if off is None:
            continue
        out.append((m.start(), m.end(), off))
    for m in pending:
        off, why = _resolve_ambiguous(text, m, out)
        if off is None:
            if doubts is not None:
                doubts.append((m.start(), m.end(), why))
            continue
        out.append((m.start(), m.end(), off))
    out.sort()
    return out


def _tz_for(text: str, span, tokens, rspans=()):
    """The zone for ONE range: its own line first, then the document, then NOTICE_TZ.

    Line first is what makes a notice that prints the same window twice - a UTC
    line above a GMT+8 line - come out right: each clock is stamped with its own
    line's offset, so both lines denote the same instant and it does not matter
    which one wins. Taking the first zone token in the WHOLE message instead is
    how you stamp line A's clock with line B's offset and put a window eight
    hours out without anything on the row looking wrong.

    A line that names TWO different zones is the same trap one level down:
    "10:00 - 12:00 GMT+8 (02:00 - 04:00 UTC)" gave both ranges the line's first
    token, so the UTC copy became 02:00 Manila and the union wrote 02:00-12:00.
    Such a line is written either suffix-style ("<range> <zone>", by far the
    commoner) or prefix-style ("北京时间 10:00-12:00（UTC 02:00-04:00）"), and
    the line's first zone token says which: before the first range means
    prefix, so each range takes the nearest token IN FRONT of it; otherwise
    each range takes the nearest token AFTER it. A line that mixes the two
    styles can still be mis-bound, and _flag_two_zone_lines sends that case to
    a human rather than trusting this rule.
    """
    ls = text.rfind("\n", 0, span[0]) + 1
    le = text.find("\n", span[0])
    le = len(text) if le == -1 else le
    line = [t for t in tokens if ls <= t[0] < le]
    if line:
        if len({off for _a, _b, off in line}) == 1:
            return timezone(timedelta(minutes=line[0][2]))
        firsts = [a for a, _b in rspans if ls <= a < le]
        prefix = bool(firsts) and line[0][0] < min(firsts)
        before = [t for t in line if t[1] <= span[0]]
        after = [t for t in line if t[0] >= span[1]]
        if prefix:
            pick = (before[-1:] or after[:1] or line[:1])[0]
        else:
            pick = (after[:1] or before[-1:] or line[:1])[0]
        return timezone(timedelta(minutes=pick[2]))
    lend = [t for t in tokens if _lends_zone(text, t[0], rspans)]
    if lend:
        return timezone(timedelta(minutes=lend[0][2]))
    return _tz_default()


# Hunt z24 / r06 / r08: the document-level zone used to be the FIRST token in
# the message, wherever it stood - "Our support team (GMT+7) is available
# 09:00 - 18:00", "Support hotline (UTC+0)", "Head office: London (UTC+0)",
# "Tech Ops, Malta Office (CET)", and another market's conversion line ("For
# Thailand: 13:00-15:00 (GMT+7)") each moved a zone-less maintenance window by
# hours. Only a line that states no range of its own and is not about a desk,
# an office or another market lends its zone to the rest of the notice.
_NO_LEND_RE = re.compile(
    r"\b(?:support|hotline|helpdesk|help\s+desk|customer\s+service|CS|office|HQ|headquarters?"
    r"|regards|sent\s+from|team\s+(?:is|are)|available|contact|market|partners?\s+in|for\s+"
    r"(?-i:[A-Z])[a-z]+)\b|客服|服务时间|服務時間|办公室|辦公室|总部|總部|发布|發佈|站点|站點|地区|地區"
    r"|市场|市場|联系|聯繫", re.I)


def _lends_zone(text: str, pos: int, rspans) -> bool:
    ls, le = _line_at(text, pos)
    if any(ls <= a < le for a, _b in rspans):
        return False
    return not _NO_LEND_RE.search(text, ls, le)


def _tz_of(text: str):
    """The notice-level zone, else the configured default.

    Kept for callers and tests that ask about a message as a whole; the window
    machinery uses _tz_for, which resolves per range.
    """
    t = _norm(text or "")
    rspans = [(m.start(), m.end()) for m in _RANGE_RE.finditer(t)]
    tokens = _tz_tokens(t, rspans)
    return timezone(timedelta(minutes=tokens[0][2])) if tokens else _tz_default()


# ---------------------------------------------------------------------------
# Wording
# ---------------------------------------------------------------------------

# A reschedule restates the ORIGINAL date too; its updated block wins.
# F28: 新时间 is also the tail of 更新时间 ("update time"), 刷新时间 and 最新时间,
# and the head of 新时间段: "例行维护：09-23 10:00-12:00\n客户端更新时间：09-24
# 02:00-03:00" cut the maintenance line off as superseded and wrote the client
# update hour. Those spellings are not an "updated schedule" heading.
_UPDATED_RE = re.compile(r"更新[后後]时间|更新[后後]時間|Updated\s+Schedule|"
                         r"(?<![更刷最重])新的?(?:时间|時間)(?!段)"
                         # R1.36: "最新时间：" as a LABEL is the latest window.
                         r"|最新(?:时间|時間)\s*[:：]"
                         r"|Revised\s+Schedule", re.I)

# F53: an Updated-Schedule marker that PROMISES a schedule is not one. "If the
# work overruns, an updated schedule will be shared in this group." and
# "如维护延期，将另行通知新的时间" turned a plain notice into a false
# reschedule and cut its own window off as superseded. The marker is refused
# when it is indefinite (an/any/a/further), conditional (if/should/如/若) or
# promised (will be / to follow / 另行 / 将 / 待定).
_UPDATED_INDEFINITE_RE = re.compile(
    r"\b(?:an|any|a|another|further|future|some)\s+$", re.I)
_CONDITION_RE = re.compile(
    r"\b(?:if|unless|whether|in\s+case|should|once|when)\b"
    r"|如果|假如|倘若|若是|万一|萬一|一旦|如有|若有|(?<![例比])如(?![下此期同今])|若",
    re.I)
# Read with .match() at the marker's end. No digit may stand between: in
# "新时间：9月25日将进行维护" the 将 belongs to the stated date, not a promise.
_PROMISED_AFTER_RE = re.compile(
    r"[^.。;；!?！？\n\d]{0,12}?(?:\b(?:will|shall|would|to\s+be|to\s+follow"
    r"|is\s+to|TBA|TBC|TBD)\b|另行|将|將|稍后|稍後|后续|後續|待定|待通知|再行"
    r"|随后|隨後|会|會)", re.I)


# ...or the marker is the OBJECT of a verb that promises it: "将另行通知新的
# 时间", "稍后公布新时间", "we will share the updated schedule".
_PROMISED_BEFORE_RE = re.compile(
    r"(?:将|將|会|會|另行|再行|稍后|稍後|随后|隨後|后续|後續)[^。，,；;\n\d]{0,6}"
    r"(?:通知|公布|告知|提供|发布|發佈|公告)\s*$"
    r"|\b(?:will|shall)\s+(?:be\s+)?(?:share|send|announce|provide|advise|inform"
    r"|notify|post|publish|release|update|confirm)\w*\b[^.;\n\d]{0,24}$", re.I)


def _promised_marker(text: str, m) -> bool:
    """True when marker ``m`` announces a schedule still to come (F53)."""
    head = _clause_before(text, m.start())
    return bool(_UPDATED_INDEFINITE_RE.search(head)
                or _CONDITION_RE.search(head)
                or _PROMISED_BEFORE_RE.search(head)
                or _PROMISED_AFTER_RE.match(text, m.end()))

# The object nouns that, standing between (or just before) the maintenance and
# the verb, mean the verb is about THEM and not about the maintenance.
_OBJ_EN = (r"rounds?|bets?|wagers?|bonus(?:es)?|promo(?:tion)?s?|tickets?"
           r"|transactions?|tournaments?|sessions?|spins?|withdrawals?"
           r"|deposits?|orders?|events?|jackpots?|payouts?")
_OBJ_ZH = (r"注单|注單|投注|赛事|賽事|比赛|比賽|活动|活動|优惠|優惠|促销|促銷"
           r"|交易|订单|訂單|游戏|遊戲|奖金|獎金|红利|紅利|派彩|局")
_OBJ_EN_RE = re.compile(r"\b(?:" + _OBJ_EN + r")\b", re.I)
_OBJ_ZH_RE = re.compile(_OBJ_ZH)

# A dot between two digits is part of a clock or a date ("10.00-12.00",
# "2026.09.23"), NOT the end of a sentence. Every bounded gap below that stops
# at a full stop has to carve out that exception, or one notice gets two
# different verdicts depending on which clock the provider happens to type:
# in "The scheduled maintenance on 2026-09-23 10.00-12.00 has been cancelled"
# the cancel clause could not reach its own verb, the cancellation went unseen,
# and a WITHDRAWN window was written to the sheet as a live outage - the one
# direction this path must never fail in. Each alternative still consumes
# exactly one character, so the {0,n} bounds keep counting characters.
_NOT_STOP = r"(?:[^.\n]|(?<=\d)\.(?=\d))"
_NOT_STOP_ZH = r"(?:[^。．.\n]|(?<=\d)\.(?=\d))"

# A reschedule verb only counts when the MAINTENANCE is its subject. An
# anniversary promo that is 延期 is an ordinary notice that happens to contain
# the word - and the pairing of that stray 延期 with an unrelated 原定 used to
# return needs_human and write nothing at all, the worst of the three outcomes.
# The noun guard is the same idea as the cancel guard: an object noun standing
# between 维护 and the verb means the verb is about that noun.
# The operands either side of "moved from X to Y": a date or a clock, never
# a bare noun like "staging".
_MOVED_OPERAND = (
    r"(?=[^.\n]{0,24}?(?:\d{1,2}\s*[:.]\s*\d{2}|\d{4}[-/.]\d{1,2}"
    r"|\d{1,2}[-/.]\d{1,2}|\d{1,2}\s*[月日号]|" + _MONTH_WORD + r"))")

# F26/F22: 推迟/延迟/暂缓 ("delayed", "put on hold") and 调整为/变更为 are the
# same move as 延期/改为 - "原定 09-23 … 的维护推迟，新时间另行公布" wrote the
# withdrawn 23rd because no reschedule verb matched and the ordinary path
# never looks at the superseded flag. 延迟 also means LATENCY ("维护期间可能
# 会有延迟"), so it only counts before a direction or a stop, and a modal in
# the gap (可能/或许/会有) disqualifies every verb of the list.
_RESCHED_ZH_V = (r"(?:改期|延期|延后|延後|顺延|順延|改至|改到|延至|提前至|改为|改為"
                 r"|推迟|推遲|推后|推後|暂缓|暫緩|挪到|挪至|移至|移到"
                 r"|(?:调整|調整|变更|變更)(?:为|為|至|到)"
                 r"|(?:延迟|延遲)(?!到[账帳])(?=至|到|为|為|进行|進行|举行|舉行|执行"
                 r"|執行|实施|實施|[，,。.；;\s]|$))")
# "维护期间提款将延迟到账": DURING the maintenance something else is delayed.
# A money noun, or 期间/过程/时 right after 维护, makes the verb not the
# maintenance's own.
_RESCHED_ZH_GAP = (r"(?:(?!" + _OBJ_ZH + r"|可能|或许|或許|也许|也許|会有|會有"
                   r"|期间|期間|过程|過程|时(?![间間])|時(?![间間])|提款|存款|出款|充值"
                   r"|提现|提現|到账|到帳|结算|結算|登录|登錄)[^。，,；;\n])")
_RESCHED_EN_GAP = (r"(?:(?!\b(?:" + _OBJ_EN + r"|players?|users?|members?"
                   r"|accounts?|launch(?:es)?|releases?|features?|versions?"
                   r"|payments?|e-?mails?|reports?|responses?|loading"
                   r"|may|might|could|can|would)\b)" + _NOT_STOP + r")")
RESCHEDULE_RE = re.compile(
    r"时间变更|時間變更|时间调整|時間調整|重新安排"
    # R1.33 / R1.35: "— change of plan", "之前通知的例行维护 <X> 有变动".
    r"|\bchange\s+of\s+plans?\b|\bplans?\s+(?:have|has)\s+changed\b"
    # F54: "The maintenance previously announced for X will instead be Y".
    r"|\bwill\s+instead\s+(?:be|take\s+place|run|start)\b"
    r"|(?:维护|維護)[^。；;\n]{0,60}?(?:有变动|有變動|有调整|有調整|有更改|有变更|有變更)"
    r"|(?:维护|維護|维修|維修|停机|停機|停服)"
    + _RESCHED_ZH_GAP + r"{0,10}" + _RESCHED_ZH_V
    # R1.14: "维护 <window> 改期到 <window>" - the whole old window between the
    # noun and the verb. Only window material may fill the gap, so "维护
    # <window> 期间，活动延期" (a promo postponed during it) stays out.
    + r"|(?:维护|維護|维修|維修|停机|停機|停服)(?:[\s\d:：\-–—~～/.()（）+,，]|GMT|UTC"
    r"|年|月|日|号|號|时|時|分){4,60}?(?:的)?(?:" + _RESCHED_ZH_V + r")"
    + r"|(?:改期|延期|延后|延後|顺延|順延|推迟|推遲|暂缓|暫緩)"
    r"(?:(?!" + _OBJ_ZH + r")[^。，,；;\n]){0,6}(?:维护|維護|维修|維修)"
    # "维护时间由 X 改为 Y" / "从 X 延期至 Y": the verb sits after the OLD
    # window, too far from 维护 for the gap above.
    # 由于/由於 ("because") is not a from-word, and a date must follow it.
    r"|(?:由(?![于於])|从|從)(?=\s*\d)[^。；;\n]{4,40}?" + _RESCHED_ZH_V
    + r"|Rescheduled?|Re-scheduled|Postponed|Postponement"
    r"|Updated\s+Schedule|Revised\s+Schedule"
    r"|maintenance" + _NOT_STOP + r"{0,30}\bmoved\s+(?:from|to)\b"
    # F26/F22: the maintenance as the subject of a delay or move verb, with
    # its own window in between ("The maintenance originally scheduled for
    # 2026-09-23 10:00 - 12:00 (GMT+8) has been moved to next week" is 70
    # characters from noun to verb). A noun or a modal in the gap makes the
    # verb about something else ("during maintenance, deposits may be
    # delayed").
    r"|maintenance" + _RESCHED_EN_GAP + r"{0,80}?\b(?:moved\s+(?:from|to|up"
    r"|back|forward)|moved(?=\s*[:：])|delayed|deferred|pushed\s+(?:back|to"
    r"|forward)|put\s+on\s+hold|brought\s+forward|changed\s+(?:from|to)"
    r"|shifted\s+(?:from|to)"
    # R1.33 / R1.34: "…will be moved", "…previously announced for <X> has
    # changed" - the move stated with no destination, or before it.
    r"|(?:will\s+be|has\s+been|have\s+been|is\s+being)\s+(?:moved|changed"
    r"|rescheduled)\b|(?:has|have)\s+changed\b"
    r"|will\s+now\s+(?:take\s+place|be\s+(?:held"
    r"|carried\s+out|performed|conducted|done)))\b"
    # G2.2: "UPDATE: Scheduled maintenance time changed. New time: … Old
    # time: …" names no reschedule verb of the list above.
    r"|(?:maintenance|downtime)\s+(?:time|date|schedule|window|slot)s?\s+"
    r"(?:(?:has|have)\s+been\s+|is\s+|was\s+|are\s+|were\s+)?(?:changed"
    r"|updated|revised|amended|adjusted)\b"
    # "moved from X to Y" is a reschedule only when X and Y are DATES or TIMES.
    # A bare from->to matched "The build was moved from staging to production",
    # which flipped an ordinary notice to reschedule=True and made the
    # Laboratory card claim the row had changed when it had not.
    r"|\bmoved\s+from\b\s*" + _MOVED_OPERAND
    + _NOT_STOP + r"{0,40}\bto\b\s*" + _MOVED_OPERAND,
    re.I)

# "originally planned for X" - the window quoted in a reschedule that is being
# SUPERSEDED. Line-anchored: it opens a superseded REGION inside an updated
# block, so it must stay narrow.
#
# F29: 原订 is the SIMPLIFIED spelling of 原訂 (訂 simplifies to 订, so every
# traditional-to-simplified converter produces it), and only the traditional
# one was listed: "维护时间调整\n原订时间：09-23 …\n调整为：09-25 …" wrote the
# withdrawn 23rd with "(rescheduled)" on the card. F27/F54: 先前 as in 如先前
# 通知 ("as previously announced") CONFIRMS the window it introduces; it is
# not the old half of a reschedule.
_CONFIRM_REF_ZH = (r"(?!的?(?:通知|公告|告知|所述|发布|發佈|发出|發出|提到|提及"
                   r"|说明|說明|安排的?(?:维护|維護)))")
ORIGINAL_RE = re.compile(
    r"原定|原訂|原订|原计划|原計劃|先前" + _CONFIRM_REF_ZH
    + r"|previously\s+(?:set|scheduled|planned)"
    r"|originally\s+(?:set|scheduled|planned)|original\s+schedule",
    re.I)

# A line that merely restates the superseded window inside an updated block.
# The line-start label forms are safe to list broadly (F29): 原先/原安排/原预定
# /原排定 and the Old:/Before:/变更前/调整前 labels of a before-and-after block
# (F22) only ever head the withdrawn half.
_ORIGINAL_LABEL_RE = re.compile(
    r"\s*(?:original|old|before|wrong|incorrect"
    r"|原(?:定|訂|订|计划|計劃|先|安排|预定|預定|排定|时间|時間)"
    r"|(?:变更|變更|调整|調整|修改)前|错误|錯誤)"
    r"(?:\s*(?:time|date|schedule|window|slot|时间|時間|日期))?\s*[:：]", re.I)

# The CLAUSE-level superset of ORIGINAL_RE. Deliberately a separate constant:
# this one only demotes one candidate window, while anything added to
# ORIGINAL_RE supersedes a whole region and could black out the only window in
# a single-line notice such as "时间变更：新时间 X，原时间 Y".
#
# F54: "previously announced / notified / our previous notice" REFERS BACK to
# the notice this one confirms - "The maintenance previously announced for
# 09-23 10:00-12:00 will proceed as scheduled. The backoffice will be
# unavailable 10:00-10:30." demoted the real window and wrote the 30-minute
# backoffice hour. It is not a withdrawal, so it is carved out here and in
# _PAST_CLAUSE_RE.
# G2.2: a correction names its wrong half too - Wrong:/错误：/变更前/调整前,
# "(was X)", "…, not X", 而非/而不是 X, "rather than X". Each is a marker
# immediately in front of the window it withdraws; none of them occurs in
# front of a live window.
_CONFIRM_REF_EN = (r"(?!\s+(?:announced|notified|informed|communicated"
                   r"|mentioned|advised|shared|posted|published|stated|sent"
                   r"|circulated))")
_SUPERSEDED_CLAUSE_RE = re.compile(
    r"原定|原訂|原订|原时间|原時間|原计划|原計劃|原本|原先|原安排|原预定|原預定"
    r"|原排定|原来的?(?:时间|安排|计划)|原來的?(?:時間|安排|計劃)|本来|本來"
    r"|旧时间|舊時間|先前" + _CONFIRM_REF_ZH + r"|之前的" + _CONFIRM_REF_ZH
    + r"|(?:变更|變更|调整|調整|修改)前|错误[:：]|錯誤[:：]|有误的|有誤的"
    r"|而非|而不是"
    # R1.40: "Previously announced: <X>" as a LABEL is the value replaced. The
    # confirmation "the maintenance previously announced for X will proceed"
    # (F54) has no colon and stays excluded by _CONFIRM_REF_EN.
    r"|\bpreviously\s+(?:announced|notified|stated|scheduled|planned|informed)\s*[:：]"
    r"|\boriginal(?:ly)?\b|\bpreviously\b" + _CONFIRM_REF_EN
    + r"|\binstead\s+of\b|\brather\s+than\b"
    r"|\b(?:moved|rescheduled|postponed|changed|shifted|deferred|delayed"
    r"|pushed|brought\s+forward)\s+from\b"
    r"|\bwas\s+(?:previously\s+)?scheduled\s+for\b"
    # "$": the lead-in is searched as a SLICE that ends right at the window,
    # so the digit the lookahead wants is past its end.
    r"|\(\s*was\b|\bnot\s+(?:on\s+)?(?=\d|$)"
    r"|\b(?:old|wrong|incorrect)(?:\s+(?:time|date|schedule|window|slot))?\s*[:：]"
    r"|\bbefore\s*[:：]",
    re.I)

# A window mentioned only to say it is already behind us. The first range in a
# notice is very often this, which is how "上周的维护窗口为 02:00 - 04:00" once
# borrowed this week's date and invented an outage nobody announced.
_PAST_CLAUSE_RE = re.compile(
    r"上次|上周|上週|上一次|前次|之前" + _CONFIRM_REF_ZH
    + r"|已完成|已結束|已结束|已顺利完成|已順利完成"
    r"|\blast\s+(?:week|month|night|window|time|maintenance|run)\b"
    # F54: "previously announced" / "our previous notice" point back at the
    # notice being confirmed, not at a window that is behind us.
    r"|\bpreviously\b" + _CONFIRM_REF_EN
    + r"|\bprevious\b(?!\s+(?:notice|notification|announcement|message|e-?mail"
    r"|post|update|communication|mail|memo|advisory|reminder)s?\b)"
    r"|\bhas\s+been\s+completed\b|\bwent\s+smoothly\b",
    re.I)

# The letterhead date of a notice ("issued on ..."), which is NOT the date of
# the maintenance. A window that had to borrow its date from one of these lines
# is the most dangerous thing this parser can produce: a confident, plausible,
# entirely wrong row that nothing about the sheet flags.
_LETTERHEAD_RE = re.compile(
    r"(?:發佈日期|发布日期|發布日期|公告日期|通知日期|發文日期|发文日期|刊登日期)\s*[:：]"
    r"|\b(?:issued|published|posted|release\s+date|notice\s+date|date\s+issued)\b"
    r"\s*[:：]"
    # F2: the other spellings of "the day this notice went out".
    r"|\b(?:issue|publish(?:ed)?|announcement|release)\s+date\s*[:：]|\breleased\s*[:：]"
    r"|\bdate\s+of\s+(?:issue|publication|notice)\s*[:：]|\bsent\s*[:：]"
    r"|\bnotice\s+issued\s+on\b|\bissued\s+on\b"
    r"|(?:发布时间|發布時間|發佈時間|公告时间|公告時間|通知时间|通知時間)\s*[:：]",
    re.I)

# Today's gate, kept intact behind NOTICE_WORDING=strict so the widening can be
# switched off from .env without a deploy.
SCHED_RE = re.compile(
    r"例行性维护|例行性維護|例行维护|例行維護|定期维护|定期維護"
    r"|正式环境.{0,12}维护|正式環境.{0,12}維護|生产环境.{0,12}维护"
    r"|进行.{0,8}维护|進行.{0,8}維護|停机维护|停機維護|系统维护|系統維護"
    r"|排定维护|排定維護|维护公告|維護公告|维护通知|維護通知"
    r"|scheduled\s+maintenance|routine\s+maintenance|production\s+maintenance"
    r"|maintenance\s+notification|maintenance\s+notice|under\s+maintenance"
    r"|will\s+have\s+.{0,30}maintenance|maintenance\s+will\s+commence",
    re.I)

# The service nouns that can be the SUBJECT of "will be unavailable". This list
# is the whole difference between "our games will be unavailable" (an outage,
# write it) and "the promo page will be unavailable" (a banner swap, ignore it),
# and it is why the widened gate is about subjects and not keywords.
_SERVICE_NOUN = (r"games?|services?|platforms?|systems?|servers?|sites?"
                 r"|websites?|casinos?|products?|titles?|lobby|lobbies|slots?"
                 r"|tables?|api|apis|backend|back-?office")

# F19: the support desk is not the service. Customer service, live chat,
# finance and settlement all have "services" that close for a holiday, and
# "our customer service will be unavailable on 2026-09-25 10:00 - 18:00" is
# the Mid-Autumn support hours, not an outage. Fixed-width look-behinds, one
# per word, so each can be greppable and none needs a variable-width engine.
_NOT_CS_EN = ("(?<![A-Za-z])"
              + "".join(r"(?<!{}\s)".format(w) for w in (
                  "customer", "support", "chat", "cs", "finance", "settlement",
                  "payment", "account", "billing", "helpdesk", "withdrawal",
                  "deposit", "marketing", "sales", "office")))
# F19: also with a modal between - "客服将暂停服务", "客服会暂停服务".
_NOT_CS_ZH = (r"(?<!客服)(?<!客服部)(?<!客服中心)(?<!在线客服)(?<!财务)(?<!財務)"
              r"(?<!客服将)(?<!客服將)(?<!客服会)(?<!客服會)(?<!客服也)(?<!客服亦)")

# NOTICE_WORDING=wide. The strict gate rejects 27 of the 28 headings providers
# actually send - 系统升级, 停服公告, Server Maintenance, Scheduled Downtime -
# every one of them carrying a perfectly readable window, which made this single
# regex the largest source of lost notices in the whole path. Each addition
# below names a SUBJECT (the service, the platform, the server, the games); none
# of them is a bare keyword, because "活动暂停" and "the promo page will be
# unavailable" have to stay out.
SCHED_WIDE_RE = re.compile(
    SCHED_RE.pattern
    # -- Chinese: upgrade wording, which takes the service down just as hard
    + r"|系统升级|系統升級|平台升级|平台升級|服务器升级|服務器升級|伺服器升級"
    r"|版本升级|版本升級|程序升级|程式升級|系统更新|系統更新"
    # -- Chinese: the 维护 headings the strict gate happens not to list
    r"|服务器维护|服務器維護|伺服器維護|平台维护|平台維護|紧急维护|緊急維護"
    r"|临时维护|臨時維護|例行維護|维护通告|維護通告|维护安排|維護安排"
    r"|维护时间|維護時間|维护窗口|維護窗口|维护日期|維護日期|维护期间|維護期間"
    # -- Chinese: outage stated without the word 维护 at all
    r"|停服公告|停服通知|停服维护|停服維護|停机公告|停機公告|停机通知|停機通知"
    r"|停机时间|停機時間|" + _NOT_CS_ZH + r"(?:停止服务|停止服務)"
    # -- Chinese: 维修, which the traditional-character studios use for 維護
    r"|系统维修|系統維修|平台维修|平台維修|维修公告|維修公告|维修通知|維修通知"
    r"|紧急维修|緊急維修|进行.{0,8}维修|進行.{0,8}維修"
    # -- Chinese: 暂停, allowed ONLY with the service as its subject. A bare
    #    暂停 matched 活动暂停, a promo pause, and would have written an outage.
    #    F19: nor with the SUPPORT DESK as its subject - 中秋节 客服暂停服务 is
    #    the holiday hours of customer service, and it wrote an 8-hour outage.
    r"|" + _NOT_CS_ZH + r"(?:服务暂停|服務暫停|暂停服务|暫停服務|服务中断|服務中斷)"
    r"|系统中断|系統中斷"
    r"|暂停游戏|暫停遊戲|游戏暂停|遊戲暫停|暂停开放|暫停開放"
    # -- Chinese: the outage described with the service as subject (G1.5):
    #    游戏将无法进入, 平台将关闭, 系统将暂停运营. 将停机 stands alone because
    #    a machine stop is never a promo.
    r"|(?:游戏|遊戲|平台|系统|系統|服务器|服務器|伺服器|网站|網站)"
    r"(?:将|將)(?:会|會)?(?:暂时|暫時)?(?:无法进入|無法進入|无法登录|無法登錄"
    r"|无法登入|無法登入|无法访问|無法訪問|关闭|關閉|暂停运营|暫停運營|暂停使用"
    r"|暫停使用|停机|停機)"
    r"|(?:将|將)(?:会|會)?(?:暂时|暫時)?(?:停机|停機)(?![后後前])"
    # -- English: the heading is <thing> maintenance, not only "scheduled"
    r"|" + _NOT_CS_EN + r"(?:server|system|platform|service|database|network"
    r"|game|api|backend|emergency|urgent|unplanned|planned|periodic|regular)"
    r"\s+maintenance"
    r"|maintenance\s+(?:window|schedule|advisory|alert|announcement)"
    # -- English: upgrade and downtime
    r"|(?:system|platform|server|service|version|software|database|api|client)"
    r"\s+upgrade"
    r"|(?:scheduled|planned|system|service|emergency|unplanned|maintenance)"
    r"\s+downtime"
    r"|" + _NOT_CS_EN + r"service\s+(?:interruption|disruption|outage|suspension)"
    # -- English: the outage described rather than named. The subject noun must
    #    be the service itself and must sit in the same sentence - and must not
    #    be the support desk's (F19: "our customer service will be unavailable
    #    on <Mid-Autumn hours>", "live chat services will be offline").
    #    G1.5 adds the other ways a provider says it: closed, shut down, taken
    #    offline, "will not be available", "will go offline".
    r"|" + _NOT_CS_EN + r"(?:" + _SERVICE_NOUN + r")\b" + _NOT_STOP
    + r"{0,40}\bwill\s+(?:be\s+(?:temporarily\s+|briefly\s+)?"
    r"(?:unavailable|offline|down|suspended|inaccessible|interrupted|closed"
    r"|shut\s+down|taken\s+(?:offline|down))"
    r"|not\s+be\s+(?:available|accessible)|go\s+(?:offline|down))"
    r"|" + _NOT_CS_EN + r"(?:" + _SERVICE_NOUN + r")\b" + _NOT_STOP
    + r"{0,40}\bwill\s+undergo\b"
    # "The maintenance previously announced for <X> will proceed as scheduled"
    # confirms the window, and names no heading the gate knew.
    r"|\bthe\s+(?:scheduled\s+|planned\s+)?maintenance\b" + _NOT_STOP
    + r"{0,80}\b(?:will|shall)\s+(?:still\s+)?(?:proceed|go\s+ahead|take\s+place"
    r"|be\s+(?:held|carried\s+out|performed|conducted))\b"
    # ...and "shut down / taken offline FOR MAINTENANCE", where the bare word
    # is safe because the verb in front of it already states the outage.
    r"|\b(?:down|offline|shut\s+down|taken\s+(?:offline|down))"
    r"\s+for\s+(?:a\s+|an\s+|the\s+)?(?:\w+\s+)?maintenance\b"
    # F54: "The maintenance previously announced for X will instead be Y" /
    # "... has changed".
    r"|\bthe\s+(?:scheduled\s+|planned\s+)?maintenance\b" + _NOT_STOP
    + r"{0,80}\b(?:will\s+(?:instead|now)\s+(?:be|take\s+place|start|run)"
    r"|has\s+(?:been\s+)?(?:changed|moved|updated|rescheduled))\b"
    # F67: "Maintenance reminder: <X>", 第二阶段维护 <X>.
    r"|\bmaintenance\s+(?:reminder|alert|update|schedule)\s*[:：]"
    r"|第[一二三四五六七八九十\d]+(?:阶段|階段|期|次)(?:的)?(?:维护|維護)"
    # G1.5: the provider's own name as the subject - "Hacksaw will be offline
    # on <X>", "JILI will go offline", "PG将于<X>停机". Capitalised, and never a
    # desk, a page or a feature ("Customer Service", "Live Chat", "Promo Page",
    # "Registration", "Tournament Lobby").
    r"|(?-i:(?<![\w])(?!(?:The|Our|Your|This|That|These|Its|Customer|Service|Services"
    r"|Support|Live|Chat|Promo|Promotion|Promotions|Page|Registration|Tournament|Lobby"
    r"|Bank|Deposits?|Withdrawals?|Payments?|Office|Hotline|Email|Website|Site|Domain"
    r"|Old|Event|Events|Help|Desk|CS|Finance|Settlement|Cashier|Chatroom|App|Feature"
    r"|Features|Leaderboard|Jackpot|Jackpots|Bonus|Mission|Missions|Shop|Store)\b)"
    r"[A-Z][A-Za-z0-9&'-]*(?:\s+(?-i:[A-Z])[A-Za-z0-9&'-]*){0,2})\s+will\s+(?:be\s+"
    r"(?:temporarily\s+|briefly\s+)?(?:offline|unavailable|down|inaccessible)|go\s+"
    r"(?:offline|down))\b"
    r"|(?-i:[A-Z][A-Za-z0-9]{1,15})\s*(?:将于|將於|将在|將在)[^。\n]{0,40}?(?:停机|停機|下线"
    r"|下線|暂停服务|暫停服務)",
    re.I)


def _sched_re():
    """The gate regex NOTICE_WORDING selects.

    F75: strict is the pre-2026-09 gate in full - SCHED_RE over the whole
    message, and none of the plain-wording shapes (_anchored_hit / _plain_hit
    check the flag themselves). The write-suppressing guards keep their own
    switches, NOTICE_QUESTION_GUARD / NOTICE_NEGATION_GUARD /
    NOTICE_NONPROD_GUARD (default on); strict with all three at 0 reproduces
    67cf5f4's verdicts.
    """
    return SCHED_RE if _flag("NOTICE_WORDING", "wide") == "strict" \
        else SCHED_WIDE_RE


# A negator in front of the phrase that opened the gate, with at most a few
# FILLER words between them. Anchored at the END so it can only ever match the
# words leading up to the phrase: "无法访问" (unable to access) must NOT read as
# a negated 无, and it cannot, because 法 sits between the 无 and the phrase.
#
# G1.7: it used to require the negator to TOUCH the phrase, so "There will not
# be any service interruption", "without any service interruption", "with zero
# service interruption", "won't be any ..." and 无需/无须/不用 停机维护 all
# opened the gate and wrote a zero-downtime hot update onto the row as an
# outage. The filler list is closed on purpose - only words that carry no
# subject of their own - so "Players will not be able to access games during
# the scheduled maintenance" (an outage) still reads as un-negated: "able",
# "access" and "during" are not fillers and the $ anchor refuses the match.
_NEG_FILLER_BASE = (r"be|been|any|a|an|cause|causes|causing|require|requires"
                    r"|required|need|needs|needed|expect|expected|anticipate"
                    r"|anticipated|have|has|experience|experienced|result"
                    r"|results|in|lead|leads|to|involve|involves|planned"
                    r"|scheduled|there|for|of|see|notice|affect|affected")
# The degree words negate the phrase ("no significant service interruption"
# is not an announcement of one) but do NOT promise that nothing goes down, so
# _NO_OUTAGE_RE below uses the base list only.
_NEG_FILLER_EN = (r"(?:" + _NEG_FILLER_BASE
                  + r"|further|significant|major|noticeable|prolonged)")
_NEG_BEFORE_RE = re.compile(
    r"(?:\b(?:no|not|never|without|nor|avoid|avoids|avoiding|zero)\b"
    r"|n['’]t\b|\bwont\b)(?:\s+" + _NEG_FILLER_EN + r"\b){0,4}[\s,，、的]*$"
    r"|(?:不会|不會|不需要|不需|不須|无需|無需|无须|無須|毋须|毋須|不必|不用"
    r"|不再|不涉及|不存在|无|無|沒有|没有|免|零)"
    # G1.7: 出现 / 发生 / 存在 - "不会出现服务中断", "不会发生服务中断".
    r"(?:造成|产生|產生|导致|導致|引起|会有|會有|有|任何|进行|進行|的|出现|出現|发生|發生"
    r"|存在)*[\s,，、]*$",
    re.I)

# A negator INSIDE the matched phrase. The gate's `进行.{0,8}维护` spans
# "进行不停机维护" (maintenance WITHOUT downtime), and the look-before test above
# cannot see a 不 that sits after m.start().
_NEG_INSIDE_RE = re.compile(r"不停|无需|無需|无须|無須|不需|不必|不用|免停|不中断|不中斷")

# "请问…吗？" / "could you confirm your maintenance window …?" — a QUESTION about
# a window is not an announcement of one. The same group carries our own weekly
# ask and the partner's clarifying questions, and both were being classified as
# notices: a question quoting a window would have written that window onto the
# provider's row.
#
# F45 / G1.4: the guard used to fire on REQUESTS as well as questions ("please
# advise your players", "please confirm receipt", 麻烦确认收到, the provider's
# own "可以确认…进行系统维护" answering our ask), on any line that also held the
# gate phrase, and - for a message of 300 characters or less - on a '?'
# ANYWHERE, so "Scheduled maintenance <window>.\nAny questions, please contact
# us?" (the example this docstring used to promise was safe) was dropped whole
# and ledgered as "not about maintenance". A question is now decided per
# SENTENCE, by its form alone:
#   * it ends in ?/？, or ends in 吗/嗎 (with or without the mark), or
#   * it contains 请问/請問 (always interrogative), or
#   * it OPENS with an English interrogative (is there / do you / could you ...)
# and it counts only when that same sentence carries maintenance wording or a
# clock. "Any questions?", "收到吗？" and "Noted?" carry neither.
_QUESTION_END_RE = re.compile(r"(?:[?？]|[吗嗎][\s。.!！~～]*)[\s\"'”’)）]*$")
_QUESTION_WORD_RE = re.compile(r"请问|請問|想[请請]问|想[请請]問"
                               # F20: "维护能否延期至 X" asks, with no ？.
                               r"|能否|可否|能不能|可不可以|是否可以|是否能")
_QUESTION_OPEN_RE = re.compile(
    r"^[\W_]*(?:(?:hi|hello|hey|dear)\b[^,，:：]{0,24}[,，:：]\s*)?"
    r"(?:(?:is|are|was|were)\s+there|do\s+you|does\s+(?:your|the)|did\s+you"
    r"|(?:could|can|would|will)\s+(?:you|we)|may\s+(?:i|we)|shall\s+we"
    # G4.3 / F64: "Is it possible to postpone …", "Is it no maintenance this
    # week" - asked without the mark.
    r"|is\s+it|would\s+it\s+be\s+possible|any\s+chance"
    # F45: "Hi team, will there be maintenance on X", "Any scheduled maintenance X".
    r"|(?:will|would)\s+there\s+be|any\s+(?:scheduled\s+|planned\s+|upcoming\s+)?"
    r"(?:maintenance|downtime)(?!\s+(?:will|is|has|was)\b))\b",
    re.I)
# "Could you please (help to) inform your players ..." is the provider asking
# us to RELAY its notice to a third party. It has the form of a question and is
# an announcement. The third party has to be named: "could you inform US of
# your maintenance window?" is still our side asking.
_RELAY_WHO = (r"(?:all\s+)?(?:your|the|our)?\s*(?:players|members|customers|users"
              r"|clients|operators|agents|merchants|partners|teams?|staff"
              r"|everyone|them)\b")
_RELAY_REQUEST_RE = re.compile(
    r"\b(?:could|can|would|will)\s+you\s+(?:please\s+|kindly\s+|pls\s+)*"
    r"(?:help\s+(?:us\s+)?(?:to\s+)?)?"
    r"(?:(?:inform|notify|advise|remind|tell|update|alert)\s+" + _RELAY_WHO
    + r"|(?:relay|forward|share|announce|pass|circulate)\b"
    r"|let\s+" + _RELAY_WHO + r"\s*know\b)"
    # F45: "可以帮忙通知一下玩家吗？" - the provider asking us to relay it.
    r"|(?:帮忙|幫忙|协助|協助|帮我们|幫我們)?(?:通知|告知|转告|轉告|转达|轉達|知会|知會)(?:一下)?"
    r"(?:贵司|貴司|你们|你們|您)?的?(?:玩家|会员|會員|客户|客戶|用户|用戶|代理|下线|下線)",
    re.I)
# What makes a question about MAINTENANCE: the wording, or a clock it quotes.
_ASK_TOPIC_RE = re.compile(
    r"maint(?:enance|ainance|enence)|\bmaint\.|downtime|outage|upgrade"
    r"|维护|維護|维修|維修|停机|停機|停服|升级|升級"
    r"|(?<![\d:])\d{1,2}\s*[:.]\s*\d{2}(?![\d:])|\b\d{1,2}\s*[AaPp]\.?[Mm]\b",
    re.I)


# G1.4: a sentence that ONLY asks whether what came before is right - "对吗？",
# "Correct?", "Can you confirm?", "Is that right?", "是这样吗？". It carries no
# maintenance wording of its own, so the per-sentence question guard let the
# window before it through and it was written: someone checking a window is
# not the provider announcing it.
_CHECK_Q_RE = re.compile(
    r"[\W_]*(?:(?:is|was)\s+(?:that|this|it)\s+(?:right|correct|ok(?:ay)?|accurate|so)"
    r"|(?:correct|right|accurate)|(?:can|could|would)\s+you\s+(?:please\s+)?confirm"
    r"(?:\s+(?:this|that|it))?|am\s+i\s+right|对吗|對嗎|对不对|對不對|是吗|是嗎|是这样吗"
    r"|是這樣嗎|没错吧|沒錯吧|没错吗|沒錯嗎|对吧|對吧|是不是|正确吗|正確嗎)"
    r"[\s?？。.!！~～]*", re.I)


def _check_question(t: str):
    """The check-question sentence of ``t`` (see _CHECK_Q_RE), or None."""
    for s_, e_ in _question_sentences(t):
        sent = t[s_:e_].strip()
        if _CHECK_Q_RE.fullmatch(sent):
            return sent
    return None


_COURTESY_Q_RE = re.compile(
    r"(?:请问|請問)?(?:还|還)?(?:有)?(?:什么|甚麼|任何)?(?:问题|問題|疑问|疑問)(?:吗|嗎|么|麼|嘛)?[？?]?"
    r"|(?:any\s+)?(?:questions?|concerns?)\s*[?？]|(?:收到|知悉|明白)(?:吗|嗎)?[？?]", re.I)


def _question_sentences(t: str):
    """(start, end) of every sentence of ``t`` that ASKS (see _QUESTION_END_RE).

    Sentences end at 。.;!?！？ and newlines, with the terminator kept, so a
    question keeps its own '?' and does not lend it to the notice before it.
    """
    out, s = [], 0
    for i, ch in enumerate(t):
        if ch in "?？" and i + 1 < len(t) and (t[i + 1].isalnum() or t[i + 1] in "=&"):
            # A URL's query mark ("…/notice?id=12") ends no sentence.
            continue
        if _is_bound(t, i) or i == len(t) - 1:
            e = i + 1
            sent = t[s:e].strip()
            tail = re.split(r"[，,]", sent)[-1]
            if _COURTESY_Q_RE.fullmatch(tail.strip()):
                # F45: "…进行系统维护，请问有什么问题吗？" - the closing courtesy is
                # the only question; the notice before it tells.
                s = e
                continue
            if sent and (_QUESTION_END_RE.search(sent)
                         or _QUESTION_WORD_RE.search(sent)
                         or _QUESTION_OPEN_RE.search(sent)) \
                    and not _RELAY_REQUEST_RE.search(sent):
                out.append((s, e))
            s = e
    return out


def _asks_rather_than_tells(t: str) -> bool:
    """True when the message is a question about maintenance, not a notice.

    Scoped to the SENTENCE that asks: a notice with an unrelated question in it
    ("Any questions, please contact us?", "Could you please help to inform your
    players?") is still a notice. NOTICE_QUESTION_GUARD=0 turns the guard off.
    """
    if not _on("NOTICE_QUESTION_GUARD"):
        return False
    qs = _question_sentences(t)
    if not any(_ASK_TOPIC_RE.search(t, s, e) for s, e in qs):
        return False
    # R1.72: the message ASKS only when every mention of the maintenance is
    # inside a question. One closing line - "Could you please inform your end
    # users about this maintenance?", "Any concerns about the downtime?",
    # "可以帮忙通知玩家这次维护吗？" - used to refuse a whole notice whose first
    # paragraph announced the window in plain statements; bd9fc12 filled it.
    # "请问…维护时间是 X 吗？" and "may I know your maintenance schedule? Ours is
    # X" still ask: there the maintenance is named only inside the question.
    for m in _TOPIC_STRONG_RE.finditer(t):
        if not any(s <= m.start() < e for s, e in qs):
            return False
    return True


def _sched_hit(t: str):
    """The gate, with negated and interrogative matches refused.

    SCHED_WIDE_RE lists a few phrases that name the outage directly
    ("service interruption", 停止服务, "planned downtime"). Those read the same
    whether the provider is announcing one or PROMISING there will not be one,
    so "New game launch 10:00 - 12:00 GMT+8. There will be no service
    interruption." opened the gate and wrote a two-hour outage nobody announced.
    Over 432 messages that explicitly promise no outage the bare gate fired on
    every one. A match governed by a negator is therefore not a match (see
    _NEG_BEFORE_RE / _NEG_INSIDE_RE; NOTICE_NEGATION_GUARD=0 turns this off).
    Whether the message AS A WHOLE promises no outage is a separate veto, applied
    in classify() - see _no_outage.
    """
    if _asks_rather_than_tells(t):
        return None
    rx = _sched_re()
    neg = _on("NOTICE_NEGATION_GUARD")
    for m in rx.finditer(t):
        if neg and _negated(t, m):
            continue
        return m
    return None


def _negated(t: str, m) -> bool:
    """Is the gate match ``m`` negated - "no service interruption", 无需停机维护?"""
    head = t[max(0, m.start() - 40):m.start()]
    # Clause-local: a negator on the far side of a delimiter governs a
    # different statement and must not suppress this one.
    head = re.split(r"[。．.;；\n、,，]", head)[-1]
    return bool(_NEG_BEFORE_RE.search(head) or _NEG_INSIDE_RE.search(m.group(0)))


# ---------------------------------------------------------------------------
# G1.1 / G1.2: the plain wordings, admitted only where they GOVERN the window
# ---------------------------------------------------------------------------
#
# The gate lists qualified phrases ("scheduled maintenance", 系统维护) and
# refused the everyday plain ones: "Evolution maintenance <window>",
# "Maintenance Time: <window>", "we will perform maintenance on <window>",
# "there will be a maintenance on", 游戏维护, 维护：, 将于<window>维护, 每周维护.
# Every one was ledgered "not about maintenance" with no card - the provider's
# whole house template lost, every week.
#
# A bare "maintenance" anywhere in the message cannot open the gate: measured
# over the audit's negatives that admits 16 false fills (promos that mention
# the maintenance, "No maintenance: <window>", "Maintenance-free update",
# "Maintenance\nNone this week.\nTournament <window>"). So the plain word
# counts only in one of three SHAPES, each tied to the window's position:
#   heading   a line that is nothing but "<≤3 words> maintenance [notice|time
#             |…]" within 3 lines above the window, every line between being a
#             "Label: value" line with a digit;
#   label     "<≤3 words> maintenance [time|date|…]" opening the window's own
#             line, followed by ':', ' - ', 'on' or the date itself;
#   verb      a future verb phrase in the window's own sentence: "will perform
#             / be doing / conduct / carry out / undergo maintenance", "there
#             will be a maintenance", "maintenance will take place", 将于…维护.
# None of the prefix or gap words may be a negator, and a verb phrase is also
# run through _negated. NOTICE_WORDING=strict turns all three off.

_PLAIN_M = r"(?:maint(?:enance|ainance|enence|enace)\b|maint\.)"
# Neither a negator nor a past/other subject may stand in the prefix: "No
# maintenance: <window>", "Last maintenance", "Customer service maintenance",
# "Bank maintenance" (a third party's, not the provider's service).
_NEGW = (r"(?!(?:no|not|none|zero|without|non|free|nothing|after|post|last"
         r"|previous|past|completed|finished|cancell?ed|postponed|customer"
         r"|support|cs|chat|office|bank|banking|promo|promotion|tournament"
         r"|event|campaign)\b)")
_PLAIN_PRE = r"(?:" + _NEGW + r"[A-Za-z0-9][\w&'’.]*\s+){0,3}"
_BRACKET = r"(?:[【\[][^】\]\n]{0,30}[】\]]\s*)?"
# "Dear Partner, Evolution maintenance <window>" on one line: the salutation
# is not part of the prefix, so it is skipped before the ≤3 words are counted.
_GREET_EN = r"(?:(?:dear|hi|hello|hey)\b[^,，:：\n]{0,30}[,，:：]\s*)?"
_GREET_ZH = r"(?:(?:亲爱的|親愛的|尊敬的|您好|你好)[^，,：:\n]{0,12}[，,：:]\s*)?"
_HEAD_EN_RE = re.compile(
    r"^[\W_]*" + _BRACKET + _PLAIN_PRE + _PLAIN_M
    + r"(?:\s+(?:notice|notification|update|info(?:rmation)?|details|reminder"
    r"|plan|arrangement|time|date|period|hours|schedule|this\s+week"
    r"|next\s+week))?\s*[:：]?\s*$", re.I)
_LABEL_EN_RE = re.compile(
    r"^[\W_]*" + _GREET_EN + _BRACKET + _PLAIN_PRE + _PLAIN_M
    + r"(?:\s+(?:time|date|period|schedule|hours|window|date\s*(?:&|and)\s*time))?"
    r"\s*(?:[:：]|\s[\-–]\s|\s+(?=\d)|\s+on\b|\s+(?:from|at|between)\b"
    r"|\s+(?=(?:" + _MONTH_WORD + r"|" + _WD_NAME + r")\b))", re.I)
_LEAD_EN_RE = re.compile(r"^[\W_]*" + _GREET_EN + _BRACKET + _PLAIN_PRE + _PLAIN_M
                         + r"\s*[:：]?\s*", re.I)
# A CJK prefix is counted per character, and none of them may negate or date
# the maintenance into the past: 无维护, 不维护, 上次维护, 已维护.
_ZH_PRE = (r"(?:(?!无|無|没|沒|不|免|上次|上周|上週|已|暂停|暫停|取消|客服|活动|活動"
           r"|银行|銀行|促销|促銷|优惠|優惠)[一-鿿A-Za-z0-9]){0,8}")
_ZH_M = r"(?:维护|維護)"
_ZH_SUFFIX = r"(?:预告|預告|提醒|计划|計劃|时段|時段|安排|通告|时间|時間|日期)?"
_HEAD_ZH_RE = re.compile(
    r"^[\W_]*(?:【[^】\n]{0,20}】)?" + _ZH_PRE + _ZH_M + _ZH_SUFFIX
    + r"\s*[:：]?\s*$")
_LABEL_ZH_RE = re.compile(
    r"^[\W_]*" + _GREET_ZH + r"(?:【[^】\n]{0,20}】)?" + _ZH_PRE + _ZH_M + _ZH_SUFFIX
    + r"\s*(?:[:：]|\s*(?=\d))"
    # "【维护】<window>": the bracketed tag IS the label.
    r"|^[\W_]*【" + _ZH_PRE + _ZH_M + _ZH_SUFFIX + r"】")
# The lines a heading may stand over: "Date: 25/09/2026", "时间：14:00-16:00".
_LABEL_LINE_RE = re.compile(r"^\s*[^:：\n]{1,20}[:：].*\d")
_GAP = r"(?:(?:a|an|the|our|some|its|their)\s+)?(?:" + _NEGW + r"[\w\-]+\s+){0,2}"
_VERB_EN_RE = re.compile(
    r"\b(?:will|shall|to|going\s+to)\s+(?:be\s+)?(?:perform(?:ing)?|do(?:ing)?"
    r"|conduct(?:ing)?|carry(?:ing)?\s+out|undergo(?:ing)?|hav(?:e|ing)"
    r"|hold(?:ing)?|run(?:ning)?)\s+" + _GAP + _PLAIN_M
    + r"|\bthere\s+will\s+be\s+" + _GAP + _PLAIN_M
    + r"|\b" + _PLAIN_M + r"\s+(?:is|will\s+be)\s+(?:planned|scheduled|held"
    r"|performed|carried\s+out|conducted)"
    r"|\b" + _PLAIN_M + r"\s+will\s+(?:take\s+place|start|begin|commence|run)"
    r"|\b(?:informed|advised|notified)\s+(?:that\s+)?(?:of\s+)?" + _GAP + _PLAIN_M
    + r"|\bwe\s+have\s+" + _GAP + _PLAIN_M,
    re.I)
# 将于 <window> 维护 - but not 维护后/维护前 (the window is of something else
# that happens around it) and not across a comma.
_VERB_ZH_RE = re.compile(
    r"(?:将于|將於|定于|定於|将在|將在|预计于|預計於|计划于|計劃於|拟于|擬於)"
    r"(?:(?!不|无|無|暂停|暫停|取消|停止)[^。．\n，,；;]){0,48}?"
    r"(?:维护|維護)(?![后後前完结結])")
# "<window> 游戏维护，届时…" / "<window>维护，谢谢配合": the window, at most four
# CJK characters and 维护, with no comma between - which keeps "(GMT+8)，如遇维护
# 顺延" (a promo that moves IF there is maintenance) out. The clause in front
# of the window must not ask or forbid: "请不要在 <window> 安排维护" is a
# request NOT to hold one.
_TRAIL_ZH_RE = re.compile(
    r"(?<=[)）\d])\s*(?:(?!不|无|無|暂停|暫停|取消|停止|如|若|遇|请|請|安排)[一-鿿]){0,4}"
    r"(?:维护|維護)(?![后後前完结結])")
_TRAIL_VETO_RE = re.compile(r"不|勿|别|別|避免|无|無|没|沒|请|請|如|若|要求|希望|建议|建議")


_ABBR_STOP_RE = re.compile(
    r"\b(Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sept?|Oct|Nov|Dec)\.(?=\s*\d)", re.I)


class _Hit:
    """A match-like (start / end / group) for a shape found on a slice of t."""

    def __init__(self, t: str, start: int, end: int):
        self._t, self._s, self._e = t, start, end

    def start(self):
        return self._s

    def end(self):
        return self._e

    def group(self, _i=0):
        return self._t[self._s:self._e]


def _line_bounds(t: str):
    out, s = [], 0
    for ln in t.split("\n"):
        out.append((s, s + len(ln)))
        s += len(ln) + 1
    return out


def _under_heading(x: str) -> bool:
    """May line ``x`` stand between a maintenance heading and its window?

    A "Label: value" line with a digit ("Date: 25/09/2026"), or a bare date line
    ("2026-09-25 (Thu)"); never one that says there is none, and never one
    naming another subject ("Tournament: 2026-09-25" heads its OWN block).
    """
    if NO_MAINT_RE.search(x) or _TOPIC_OFF_RE.search(x):
        return False
    if _LABEL_LINE_RE.search(x):
        return True
    return bool(re.search(r"\d", x) and re.fullmatch(
        r"[\W\d_年月日号號]*", _WEEKDAY_RE.sub("", x)))


def _anchored_hit(t: str, at):
    """A plain maintenance wording that governs the window at ``at``, or None.

    Returns a match object in ``t``'s coordinates (callers read .start()).
    See the block comment above for the three shapes.
    """
    if at is None or _flag("NOTICE_WORDING", "wide") == "strict":
        return None
    lines = _line_bounds(t)
    li = max(i for i, (s, _e) in enumerate(lines) if s <= at)
    ls, le = lines[li]
    # heading: up to 3 lines above, only label lines between
    for j in range(max(0, li - 3), li):
        hs, he = lines[j]
        head = t[hs:he]
        m = _HEAD_EN_RE.search(head) or _HEAD_ZH_RE.search(head)
        if not m and _LABEL_LINE_RE.search(head):
            # "Maintenance Date: 2026-09-25" over "Time: 14:00 - 16:00": a
            # label line heads the block as well as a bare heading does.
            m = _LABEL_EN_RE.search(head) or _LABEL_ZH_RE.search(head)
        if not m:
            continue
        between = [t[s:e] for s, e in lines[j + 1:li] if t[s:e].strip()]
        if all(_under_heading(x) for x in between):
            return _Hit(t, hs + m.start(), hs + m.end())
    # label: the window's own line opens with it
    line = t[ls:le]
    m = _LABEL_EN_RE.search(line) or _LABEL_ZH_RE.search(line)
    if m:
        return _Hit(t, ls + m.start(), ls + m.end())
    # verb: in the window's own sentence. "Sep." / "Sept." is an abbreviation,
    # not a full stop - "There will be a maintenance on Sep. 25, 2026 <range>"
    # is one sentence (the copy is length-preserving, so offsets still hold).
    s, e = _sentence_span(_ABBR_STOP_RE.sub(lambda x: x.group(1) + " ", t), at, at)
    for rx in (_VERB_EN_RE, _VERB_ZH_RE):
        for m in rx.finditer(t, s, e):
            if _on("NOTICE_NEGATION_GUARD") and _negated(t, m):
                continue
            return m
    for m in _TRAIL_ZH_RE.finditer(t, s, e):
        if not _TRAIL_VETO_RE.search(_clause_before(t, m.start())):
            return m
    return None


def _plain_hit(t: str):
    """A plain-wording shape ANYWHERE in ``t``, for a message with no window.

    Never used to fill: with no window there is nothing to write. It only makes
    the verdict truthful - "Evolution maintenance tomorrow 14:00-16:00" is a
    maintenance notice with no readable day (needs_human, policy 9), not "no
    scheduled-maintenance wording".
    """
    if _flag("NOTICE_WORDING", "wide") == "strict" or _asks_rather_than_tells(t):
        return None
    lines = _line_bounds(t)
    for i, (ls, le) in enumerate(lines):
        line = t[ls:le]
        # A label only with a value that could be a time ("Maintenance:
        # tomorrow 14:00"), never "维护：无" / "Maintenance: TBA".
        m = _LABEL_EN_RE.search(line) or _LABEL_ZH_RE.search(line)
        if m and (re.search(r"\d", line[m.end():])
                  or _RELATIVE_WHEN_RE.search(line, m.end())):
            return _Hit(t, ls + m.start(), ls + m.end())
        # "Evolution maintenance tomorrow 14:00-16:00": the day word stands
        # where the label shape wants the date.
        m = _LEAD_EN_RE.search(line)
        if m and _RELATIVE_WHEN_RE.match(line, m.end()):
            return _Hit(t, ls + m.start(), ls + m.end())
        # A heading only over "Label: value" lines, one of them with a digit -
        # "Maintenance\nNone this week.\nTournament <window>" is a digest.
        m = _HEAD_EN_RE.search(line) or _HEAD_ZH_RE.search(line)
        if m:
            under = [t[s:e] for s, e in lines[i + 1:i + 4] if t[s:e].strip()]
            if under and not NO_MAINT_RE.search(under[0]) and (
                    _LABEL_LINE_RE.search(under[0])
                    or (re.match(r"\s*[^:：\n]{1,20}[:：]", under[0])
                        and _RELATIVE_WHEN_RE.search(under[0]))):
                return _Hit(t, ls + m.start(), ls + m.end())
    for rx in (_VERB_EN_RE, _VERB_ZH_RE):
        for m in rx.finditer(t):
            if _on("NOTICE_NEGATION_GUARD") and _negated(t, m):
                continue
            return m
    return None


def _gate_refusal(t: str):
    """(kind, reason) when the raw gate matched but _sched_hit refused it.

    F63: a notice the question or negation guard refused was reported as "no
    scheduled-maintenance wording", which vawatch ledgers as final and /vacheck
    prints as "not about maintenance" - false on both counts. The reason has to
    say which guard spoke.
    """
    rx = _sched_re()
    ms = list(rx.finditer(t))
    if not ms:
        # F63: the anchored shapes ("Maintenance on <X>?", "游戏将于 <X> 维护？")
        # never reach _sched_re; asked about, they were still "no wording".
        if (_asks_rather_than_tells(t) and _TOPIC_STRONG_RE.search(t)
                and _ANY_RANGE_RE.search(t)):
            return ("question", "maintenance wording, but the message asks about "
                                "it rather than announcing it (a question)")
        return None
    if _asks_rather_than_tells(t):
        return ("question", "maintenance wording, but the message asks about "
                            "it rather than announcing it (a question)")
    m = ms[0]
    return ("negated", "maintenance wording, but negated (“{}”) - the message "
                       "says it will NOT happen".format(
                           _quote(_clause_before(t, m.start()) + m.group(0))))


def _named_but_unworded(t: str) -> bool:
    """A maintenance word the gate did not trust, used as the SUBJECT: not
    negated or forbidden ("No maintenance:", "Maintenance-free", "请不要…安排维护",
    "暂停维护"), not adverbial ("维护后上线"), not a desk's ("Customer service
    maintenance", 客服将暂停服务), and never under NOTICE_WORDING=strict."""
    if _flag("NOTICE_WORDING", "wide") == "strict":
        return False
    if _CORRECTS_RE.search(t) or _RETRACT_RE.search(t):
        # A correction or retraction FOLLOW-UP is vawatch's to card (G2.3): its
        # card names the row's current window, which this verdict cannot.
        return False
    for m in _TOPIC_STRONG_RE.finditer(t):
        pre = t[max(0, m.start() - 10):m.start()]
        post = t[m.end():m.end() + 6]
        if re.search(r"(?i)(?:\bno|\bnon|\bwithout|\bany|不要|别|勿|不|暂停|暫停|停止|无需|無需)\W{0,3}$", pre) \
                or re.match(r"(?i)-?\s*free\b|后|後|之后|之後|完成后|结束后", post):
            continue
        sent = _sentence_of(t, m.start())
        if _DOUBT_DESK_RE.search(sent) or _TOPIC_OFF_RE.search(sent) or re.search(
                r"(?i)不要|请勿|請勿|避免|别在|別在|\bplease\s+(?:do\s+not|don['’]t)\b|\bavoid\b", sent):
            continue
        return True
    return False


_LONE_CLOCK_RE = re.compile(r"(?<![\d:])\d{1,2}\s*[:：]\s*\d{2}(?![\d:])|(?<!\d)\d{1,2}\s*[点點]"
                            r"|(?<![\d.])\d{1,2}\.\d{2}\s*(?:[ap]\.?m\.?)")


def _quote(s: str, width: int = 60) -> str:
    """The last ``width`` characters of ``s``, whitespace folded, cut at a word."""
    s = " ".join(s.split())
    if len(s) > width:
        s = s[-width:]
        s = s[s.find(" ") + 1:] if " " in s[:20] else s
    return s


# F18: a PROMISE that there is no outage vetoes the whole notice. The negation
# guard above only refuses the one gate phrase it is looking at, so "Platform
# upgrade <window>. There will be no service interruption." still opened the
# gate through its un-negated heading, and so did 版本升级 …，无需停机维护 and
# 进行不停机维护: a zero-downtime release was written onto the provider's row
# as an outage and REPLACED the real window already there. The row answers
# "when is this provider down", and the provider has just said: never.
_NO_OUTAGE_RE = re.compile(
    r"\b(?:no|zero|without(?:\s+any)?|nor)\s+"
    r"(?:(?:planned|scheduled|expected)\s+)?"
    r"(?:(?:service|system|game(?:play)?|player)\s+)?"
    r"(?:downtime|down\s+time|interruptions?|disruptions?|outages?)\b"
    r"|(?:\bnot|n['’]t|\bwont|\bnever)(?:\s+(?:" + _NEG_FILLER_BASE + r")\b){0,4}\s+"
    r"(?:(?:service|system|game(?:play)?)\s+)?"
    r"(?:downtime|down\s+time|interruptions?|disruptions?|outages?)\b"
    r"|\bno\s+maintenance\s+(?:downtime|required|needed|necessary|impact)\b"
    r"|不停机|不停機|不停服|免停机|免停機|免停服"
    r"|(?:无需|無需|无须|無須|不需要?|不必|不用)(?:任何)?(?:停机|停機|停服|维护|維護"
    r"|暂停服务|暫停服務|中断服务|中斷服務|下线|下線)"
    r"|不(?:会|會)(?:造成|有|产生|產生)?(?:任何)?(?:停止服务|停止服務|服务中断|服務中斷"
    r"|停机|停機|停服|中断|中斷)"
    r"|(?:无|無|没有|沒有)(?:任何)?(?:服务中断|服務中斷|停机|停機|停服)"
    r"|不中断服务|不中斷服務|服务不中断|服務不中斷|服务不会中断|服務不會中斷"
    r"|不影响(?:正常)?(?:游戏|遊戲|服务|服務|使用|营运|營運|运营|運營)"
    r"|不影響(?:正常)?(?:遊戲|服務|使用|營運|運營)"
    r"|(?:游戏|遊戲)(?:可|将|將|仍|依然)?(?:照常|正常)(?:运行|運行|进行|進行|运营|運營)",
    re.I)

# A frame that puts the promise AFTER the outage: "维护结束后游戏将正常运行",
# "games will run normally once the maintenance is completed" is an ordinary
# notice's closing line, not a promise that nothing goes down.
_AFTER_FRAME_RE = re.compile(
    r"后|後|结束|結束|完成|恢复|恢復|\bafter\b|\bonce\b|\bwhen\b|\buntil\b"
    r"|\bupon\b|\bcompleted?\b|\bfinished\b", re.I)

# What an outage looks like when the text STATES one: the service unavailable,
# players unable to get in. A message that holds one of these as well as a
# no-outage promise ("slots will be unavailable; live casino has no downtime")
# is a partial outage that cannot be read either way - needs_human.
_OUTAGE_STATED_RE = re.compile(
    r"\bwill\s+be\s+(?:temporarily\s+|briefly\s+)?(?:unavailable|inaccessible"
    r"|offline|down|suspended|closed|shut\s+down)\b"
    r"|\b(?:not|n['’]t|unable)\s+(?:be\s+)?(?:able\s+)?to\s+(?:access|log\s*in"
    r"|login|play|enter)\b|\bcannot\s+(?:access|log\s*in|login|play|enter)\b"
    r"|无法(?:进入|登录|登入|访问|游戏|使用)|無法(?:進入|登錄|登入|訪問|遊戲|使用)"
    r"|暂停(?:使用|服务|游戏|开放)|暫停(?:使用|服務|遊戲|開放)"
    r"|停止服务|停止服務|(?:将|將)停机|(?:将|將)停機",
    re.I)


# ...and a promise LIMITED to part of the day: "no downtime beyond the window",
# "no downtime outside the window", "no downtime during peak hours" all say an
# outage exists - they are an ordinary notice's reassurance.
_LIMITED_PROMISE_RE = re.compile(
    r"[^.。;；,，\n]{0,12}?\b(?:beyond|outside|except|besides|apart\s+from"
    r"|other\s+than|(?:during|in|at)\s+(?:the\s+)?(?:peak|busy|prime))\b"
    r"|[^。；，\n]{0,6}?(?:以外|之外|高峰)", re.I)
# The Chinese limit stands IN FRONT: "高峰时段不停服", "除维护时段外不停机".
_LIMITED_BEFORE_ZH_RE = re.compile(r"高峰|除[^，。；\n]{0,12}外|以外|之外")


def _no_outage(t: str):
    """The no-outage promise the message makes, or None (see _NO_OUTAGE_RE)."""
    if not _on("NOTICE_NEGATION_GUARD"):
        return None
    for m in _NO_OUTAGE_RE.finditer(t):
        if _AFTER_FRAME_RE.search(_clause_before(t, m.start())):
            continue
        if _LIMITED_PROMISE_RE.match(t, m.end()) or _LIMITED_BEFORE_ZH_RE.search(
                _clause_before(t, m.start())):
            continue
        # R1.74: "The back office will have no downtime", "no service
        # interruption to the back office", "no downtime for the reporting
        # API" promise it for ONE subsystem. The maintenance still takes the
        # rest down, and an explicit "Scheduled maintenance <window>" notice
        # carrying such a line was ignored whole.
        after = re.split(r"[。．.;；\n,，]", t[m.end():m.end() + 60])[0]
        if _SUBSYSTEM_SCOPE_RE.search(_clause_before(t, m.start())) \
                or _SUBSYSTEM_SCOPE_RE.search(after):
            continue
        return m
    return None


_SUBSYSTEM_SCOPE_RE = re.compile(
    r"\bback[\s-]?office\b|\bBO\b|\badmin(?:istration)?\s+(?:panel|portal|site"
    r"|system)\b|\bdashboard\b|\breport(?:ing|s)?\b|\bAPIs?\b|\bportal\b"
    r"|\b(?:agent|merchant|operator|management)\s+(?:system|site|portal|backend"
    r"|console|platform)\b"
    r"|后台|後台|报表|報表|管理平台|代理系统|代理系統|接口",
    re.I)


def _outage_stated(t: str) -> bool:
    for m in _OUTAGE_STATED_RE.finditer(t):
        head = re.split(r"[。．.;；\n、,，]", t[max(0, m.start() - 40):m.start()])[-1]
        if _NEG_BEFORE_RE.search(head):
            continue
        return True
    return False


# R1.61: a window the provider has not committed to. "Proposed maintenance
# window: <X>. Please confirm if OK", "Tentative maintenance <X> (TBC)",
# "预计<X>维护，具体时间待定" and "初步定于<X>维护，待确认" were written as the
# provider's confirmed window; the row must change only on a confirmed one.
# Each alternative names the COMMITMENT, never a hedge about the outage, so
# "there might be a short disconnection", "as soon as possible", "预计维护2小时"
# and "the time is subject to change" stay ordinary confirmed notices. Read in
# the window's own sentence.
_TENTATIVE_RE = re.compile(
    r"\btentative(?:ly)?\b|\bprovisional(?:ly)?\b"
    r"|\b(?:proposed|preferred|suggest(?:ed)?)\s+(?:maintenance\s+)?(?:window|time"
    r"|slot|date|schedule|period)s?\b|\bproposed\s+maintenance\b"
    r"|\bto\s+be\s+confirmed\b|\bTBC\b|\bnot\s+(?:yet\s+)?confirmed\b"
    r"|\bpending\s+(?:your\s+|our\s+)?(?:approval|confirmation)\b"
    r"|\b(?:please|pls|kindly)\s+confirm\s+(?:if|whether)\b"
    r"|\b(?:might|may|could)\s+(?:be|have)\s+(?:an?\s+)?(?:emergency\s+|urgent\s+)?"
    r"maintenance\b|\bpossible\s+(?:emergency\s+|urgent\s+)?maintenance\b"
    r"|待定|待确认|待確認|初步定于|初步定於|初定|暂定|暫定|拟定|擬定|拟于|擬於"
    r"|建议(?:的)?(?:维护)?时间|建議(?:的)?(?:維護)?時間",
    re.I)
# R1.61: an OPERATOR asking the provider to cancel or hold off. "Please cancel
# the maintenance on <X>" names the very window it wants gone, and wrote it.
_CANCEL_REQUEST_RE = re.compile(
    r"\b(?:please|pls|plz|kindly|can\s+you|could\s+you|would\s+you|we\s+(?:would"
    r"\s+like|need|want|ask)\s+(?:you\s+)?to)\s+(?:help\s+(?:to\s+)?)?(?:cancel"
    r"|call\s+off|skip|postpone|delay|hold\s+off(?:\s+on)?)\s+(?:the\s+|your\s+"
    r"|this\s+|that\s+|today['’]s\s+|tonight['’]s\s+)?(?:scheduled\s+|planned\s+"
    r"|upcoming\s+)?maintenance"
    r"|(?:请|請|麻烦|麻煩|烦请|煩請)(?:贵司|貴司|你们|你們)?(?:取消|暂停|暫停|推迟|推遲)"
    r"[^。\n]{0,20}(?:维护|維護)",
    re.I)
# R1.24: our side asking the provider to CONFIRM a window, with no '?' -
# "Pls confirm maintenance <X>", "麻烦确认下你们<X>是否有维护", "确认一下贵司<X>
# 是否维护". "Please confirm receipt" (the provider asking us to acknowledge its
# own notice) names no maintenance after the verb and is not one.
_CONFIRM_REQUEST_RE = re.compile(
    r"\b(?:please|pls|plz|kindly)\s+(?:help\s+(?:to\s+)?)?confirm\s+(?:(?:if|whether|that)\s+)?"
    r"(?:the\s+|your\s+|this\s+)?(?:scheduled\s+|planned\s+|upcoming\s+)?maint(?:enance)?\b"
    r"|(?:麻烦|麻煩|烦请|煩請|请|請)?(?:确认|確認)(?:一)?下[^。\n]{0,40}?是否(?:有)?(?:维护|維護)"
    r"|(?:确认|確認)一下[^。\n]{0,40}?是否(?:有)?(?:维护|維護)",
    re.I)
# R1.66: a TEST, sample or template message is not an announcement.
_TEST_MSG_RE = re.compile(
    r"^\s*[\[【(（]?\s*(?:test(?:ing)?|测试|測試)(?:\s*(?:message|msg|only"
    r"|消息|信息|讯息|訊息))?\s*[\]】)）:：\-–—]"
    r"|^\s*(?:测试|測試)(?:消息|信息|讯息|訊息)"
    r"|[\[【(（]\s*(?:test(?:\s+(?:message|msg|only))?|测试|測試)[^\]】)）\n]{0,40}"
    r"[\]】)）]\s*[.!。]*\s*$"
    r"|\btest\s+message\b|\bthis\s+is\s+(?:a|an|only\s+a)\s+(?:test|sample|example"
    r"|template)\b|^\s*(?:example|sample|template|e\.g\.)\s*[:：]"
    r"|\bsample\s+template\b",
    re.I)
# R1.56: the provider says its PLAYERS are not affected - a back-office job
# ("后台系统维护 <X>，游戏不受影响", "maintenance for BO <X>, gameplay
# unaffected"), or "games are running normally / players can continue playing
# as usual / no impact on gameplay". Written as a full outage on the row. The
# subject must be the games or players THEMSELVES, close to the verb: "player
# balances are not affected" is the usual reassurance inside a REAL outage
# notice and must not turn it into a non-event.
_NO_PLAYER_IMPACT_RE = re.compile(
    r"\b(?:games?|gameplay|game\s+(?:service|play)|players?|all\s+games)\b"
    r"(?:(?!balance|fund|wallet|data|account|record|bet|transaction|credit"
    r"|history|progress)[^.;\n]){0,15}?"
    r"\b(?:unaffected|not\s+(?:be\s+)?(?:affected|impacted|interrupted)"
    r"|(?:will\s+)?remain\s+(?:available|online|playable|accessible|open)"
    r"|(?:are|is|will\s+be)\s+(?:running|operating)\s+(?:as\s+)?normal(?:ly)?"
    r"|running\s+normally|as\s+usual|continue\s+(?:playing|to\s+play"
    r"|to\s+be\s+available))\b"
    r"|\bno\s+impact\s+on\s+(?:the\s+)?(?:gameplay|games?|players?)\b"
    r"|\b(?:the\s+)?(?:platform|website|site|lobby|all\s+services)\s+(?:will\s+)?"
    r"(?:remain|stay)\s+(?:available|online|accessible|open|operational)\b"
    r"|\b(?:will|does|shall|should)\s+not\s+(?:affect|impact|interrupt|disrupt)\s+"
    r"(?:the\s+)?(?:gameplay|games?|players?|game\s+service)\b"
    r"|\b(?:services?|games?)\s+(?:won['’]t|will\s+not)\s+be\s+(?:interrupted"
    r"|affected|impacted)\b"
    r"|游戏不受影响|遊戲不受影響|不影响游戏|不影響遊戲|游戏(?:可)?正常(?:进行|進行|运行"
    r"|運行|游玩|使用)|玩家可(?:以)?正常",
    re.I)
_BRAND_BEFORE_RE = re.compile(r"(?-i:[A-Z][A-Za-z0-9]+)(?:\s*(?:,|and|&)\s*(?-i:[A-Z][A-Za-z0-9]+))*\s*$")
# ...and a PARTIAL outage: one region, one game, one lobby, "all other games
# available". Real downtime, but not "this provider is down" - a person decides.
_PARTIAL_OUTAGE_RE = re.compile(
    r"\b(?:region|market)\s+only\b|\bonly\s+(?:the\s+)?(?:EU|Europe|Asia|LATAM|US"
    r"|APAC)\b|\((?:EU|Europe|Asia|LATAM|US|APAC|[A-Z][a-z]+)\s+only\)"
    r"|\b(?:other|asia|apac|sea|philippines?)\s+(?:operators|regions?|markets?"
    r"|players)\s+(?:are\s+|will\s+)?(?:not\s+(?:be\s+)?affected|unaffected)"
    r"|\ball\s+other\s+(?:games|products|titles|tables)\b[^.;\n]{0,20}?(?:available"
    r"|unaffected|not\s+affected|normal|back\s+online|online|restored)"
    # A NAMED game or product only ("for Sweet Bonanza only"); the capital is
    # case-sensitive, and "for production only" names the environment, not a
    # slice of the provider (R1.73).
    r"|\bfor\s+(?!(?:the\s+)?(?:production|prod|live|all)\b)(?-i:[A-Z])[\w' ]{1,30}?\s+only\b"
    r"|\b(?:slots?|live(?:\s+casino)?|fishing|bingo|sports?|lottery|table\s+games"
    r"|arcade)\s+(?:will\s+)?(?:remain|stay|are|is)\s+(?:available|online|open"
    r"|unaffected|playable)\b"
    r"|其他游戏(?:正常|不受影响)|其它游戏(?:正常|不受影响)|其他遊戲(?:正常|不受影響)"
    # R1.28: the reassurance scoped to OTHER games - "Only Aviator is affected;
    # no downtime for other games", "仅捕鱼游戏停服，其他游戏不停服". It was read
    # as a promise of no outage and ignored whole.
    r"|(?-i:[A-Z])[\w&' -]{1,24}?\s+only\s*[;；,，]\s*(?:all\s+)?other\s+(?:games|products|titles|tables)"
    r"|\bonly\s+(?-i:[A-Z])[\w&' -]{1,30}?\s+(?:is|are|will\s+be)\s+(?:affected|down"
    r"|unavailable|offline)\b"
    r"|\bno\s+(?:downtime|impact|interruption)\s+(?:for|to|on)\s+(?:all\s+)?other\s+"
    r"(?:games|products|titles|tables)\b"
    r"|(?:仅|僅|只有)[^，,。\n]{1,12}?(?:停服|停机|停機|维护|維護|受影响|受影響)"
    r"|(?:其他|其它)(?:游戏|遊戲)[^，,。\n]{0,4}?(?:不停服|不停机|不停機|不受影响|不受影響|正常运行|正常運行)",
    re.I)
# R1.63: one window per REGION ("Europe region: <X>\nAsia region: <Y>"). The
# parser cannot know which region this operator is in, and the first line won.
_REGION_LINE_RE = re.compile(
    r"^[ \t]*[-•*·]?[ \t]*(?:(?:Europe(?:an)?|EU|Asia(?:n)?|APAC|SEA|LATAM"
    r"|Latin\s+America|North\s+America|South\s+America|America[sn]?|Africa"
    r"|Oceania|Australia|Middle\s+East|MENA|Global|International|Philippines?"
    r"|China|Japan|Korea|India|Brazil)(?:\s+(?:region|market|server|cluster|site"
    r"|players|users|licen[cs]e))?"
    r"|(?:亚洲|亞洲|欧洲|歐洲|美洲|北美|南美|拉美|东南亚|東南亞|中国|中國|国际|國際"
    r"|海外)(?:区|區|地区|地區|区域|區域|服|服务器|伺服器|市场|市場)?)[ \t]*[:：]"
    r"[^\n]*?\d{1,2}[:：.]\d{2}",
    re.I | re.M)


def _veto(t: str, at, hit):
    """-> None, or (action, reason, kind) when a notice that WOULD fill must not.

    F18: the provider promises there is no outage at all - checked before
    anything is written, because the phrase that opened the gate (an upgrade
    heading, 进行…维护) is not itself negated. A message that ALSO states an
    outage is a partial one and goes to a person. F46: the notice is about a
    test environment (see _nonprod). G4.3: the window's sentence is an
    OPERATOR request - to move the provider's maintenance, or to freeze
    releases/callbacks during the operator's own (see _operator_request).
    """
    sent = _sentence_of(t, at) if at is not None else t
    chk = _check_question(t) if at is not None and _on("NOTICE_QUESTION_GUARD") else None
    if chk:
        return ("needs_human",
                "the message states a window and asks whether it is right (“{}”) "
                "- a check, not an announcement; nothing is written".format(_quote(chk)),
                "question")
    if _TEST_MSG_RE.search(t):
        return ("ignore", "a test, sample or template message, not an "
                "announcement", "test")
    ask = _CANCEL_REQUEST_RE.search(sent) or _CONFIRM_REQUEST_RE.search(sent)
    if ask:
        return ("needs_human",
                "an operator request (“{}”) with a window, not a provider "
                "announcement - nothing is written".format(_quote(ask.group(0))),
                "request")
    tent = _TENTATIVE_RE.search(sent)
    if tent:
        return ("needs_human",
                "the window is proposed or tentative (“{}”), not confirmed - "
                "nothing is written until the provider confirms it".format(
                    _quote(tent.group(0))), "tentative")
    part = _PARTIAL_OUTAGE_RE.search(t)
    if part:
        return ("needs_human",
                "a partial outage (“{}”) - one region, game or lobby, not the "
                "whole provider; nothing is written".format(_quote(part.group(0))),
                "partial")
    # "Hacksaw and Relax games are NOT affected" scopes the reassurance to
    # OTHER brands named in front of "games" - in a shared group that is how a
    # Yggdrasil notice says the co-tenant is fine. Not a whole-notice statement.
    calm = next((c for c in _NO_PLAYER_IMPACT_RE.finditer(t)
                 if not _BRAND_BEFORE_RE.search(t[max(0, c.start() - 40):c.start()])),
                None)
    if calm:
        if _outage_stated(t):
            return ("needs_human",
                    "the notice states an outage AND says “{}” - a partial outage "
                    "that cannot be written either way".format(_quote(calm.group(0))),
                    "partial")
        return ("ignore", "not a player outage: the notice says “{}”".format(
            _quote(calm.group(0))), "no_outage")
    if len(_REGION_LINE_RE.findall(t)) >= 2:
        return ("needs_human",
                "the notice gives a separate window per region and does not say "
                "which one applies here - nothing is written", "region")
    req = _operator_request(_sentence_of(t, at)) if at is not None else None
    if req:
        return ("needs_human",
                "an operator request ({}) with a window, not a provider "
                "announcement - nothing is written".format(
                    "to move the maintenance" if req == "move" else
                    "to hold releases or callbacks during a maintenance"),
                "request")
    promise = _no_outage(t)
    if promise:
        if _outage_stated(t):
            return ("needs_human",
                    "the notice states an outage AND says “{}” - a partial "
                    "outage that cannot be written either way".format(
                        _quote(promise.group(0))), "no_outage")
        return ("ignore", "not an outage: the notice says “{}”".format(
            _quote(_clause_before(t, promise.start()) + promise.group(0))),
            "no_outage")
    env = _nonprod(t, at, hit)
    return env + ("nonprod",) if env else None


# F46: maintenance of a TEST environment is not a production outage. "UAT
# server maintenance <window>. Production is not affected." and
# 【测试环境维护通知】… 正式环境不受影响 were written onto the provider's row as
# an outage, and replaced the real production window already there. The
# environment has to be the SUBJECT: it is looked for in the window's own
# sentence, the gate phrase's sentence and the notice's first sentence, so "The
# build was moved from staging to production yesterday" beside an ordinary
# notice (J16) does not count.
_NONPROD_RE = re.compile(
    r"测试环境|測試環境|测试服|測試服|测试站|測試站|测试线|測試線|沙盒|沙箱"
    # R1.72: (?<![A-Za-z]) not \b - "UAT环境维护": a CJK letter is \w too.
    r"|(?<![A-Za-z])UAT(?![A-Za-z])|\bstaging\b|\bsandbox\b|\btest(?:ing)?\s+(?:environment|env|server"
    r"|site|platform)s?\b|\bpre-?prod(?:uction)?\b|\bSIT\s+environment\b"
    # F46: 预发布 (pre-release), QA / demo / dev environments.
    r"|预发布|預發佈|預發布|预发环境|預發環境|开发环境|開發環境"
    r"|\b(?:QA|demo|dev(?:elopment)?|test|integration|INT|SIT)\s+(?:environment|env|server|site|platform)s?\b"
    r"|\b(?:integration|INT|SIT)\s+environment\b|\(\s*(?:INT|SIT|UAT|QA)\s*\)",
    re.I)
# F46: a line that SETS the scope - "Affected: UAT only", "Environment: Staging",
# "Scope: staging environment" - or a short heading that is only the environment
# ("UAT Environment"). The window's own sentence is often a bare "Time:" row.
_SCOPE_LINE_RE = re.compile(
    r"^[\W_]*(?:affected(?:\s+(?:environment|systems?|services?))?|scope|environments?|env"
    r"|applies\s+to|impact(?:ed)?|影响范围|影響範圍|适用环境|適用環境|环境|環境|范围|範圍)"
    r"\s*[:：]", re.I)
_PROD_RE = re.compile(
    r"正式环境|正式環境|生产环境|生產環境|正式服|正式站|线上环境|線上環境"
    r"|\bproduction\b|\bprod\b|\blive\s+(?:environment|env|server|site|platform)s?\b",
    re.I)
# Production named as UNAFFECTED - the usual second half of such a notice.
_PROD_OK_RE = re.compile(
    r"(?:正式环境|正式環境|生产环境|生產環境|正式服|正式站|线上环境|線上環境)"
    r"[^。．\n]{0,12}(?:不受影响|不受影響|不影响|不影響|正常运行|正常運行|正常运营|正常運營|照常)"
    r"|\b(?:production|prod|live)\b[^.\n]{0,40}?(?:\b(?:is|are|will|shall)\s+(?:be\s+)?"
    r"(?:not|n['’]t)\s+(?:be\s+)?(?:affected|impacted)|\bunaffected\b"
    r"|\b(?:operate|run|work)s?\s+normally\b|\bremains?\s+(?:available|online|open))",
    re.I)


_NONPROD_OK_RE = re.compile(
    r"[ \t]*(?:environments?|envs?|servers?|sites?|platforms?)?[ \t]*"
    r"(?:(?:is|are|will|shall|would|should)[ \t]+)?(?:(?:not|n['’]t)[ \t]+(?:be[ \t]+)?"
    r"(?:affected|impacted|involved|included)|(?:be[ \t]+)?(?:unaffected|excluded)"
    r"|(?:remains?|stays?|remain|stay|be)[ \t]+(?:available|online|up|operational"
    r"|accessible|open|unaffected))\b"
    r"|[ \t]*(?:不受影响|不受影響|不影响|不影響|正常(?:运行|運行|使用|开放|開放)?|照常)",
    re.I)


def _nonprod(t: str, at, hit):
    """-> None, or (action, reason) for a notice about a non-production env."""
    if not _on("NOTICE_NONPROD_GUARD"):
        return None
    first = len(t) - len(t.lstrip())
    scope = [_sentence_of(t, at) if at is not None else "",
             _sentence_of(t, hit.start()) if hit is not None else "",
             _sentence_of(t, first) if t.strip() else ""]
    for line in t.split("\n"):
        ln = line.strip()
        if _SCOPE_LINE_RE.match(ln) or (ln and len(ln.split()) <= 4 and len(ln) <= 40
                                        and not _ANY_RANGE_RE.search(ln)):
            scope.append(ln)
    # R1.73: "…(GMT+8), UAT is not affected." / "(UAT not affected)" /
    # "staging remains available" name the test environment as the thing that
    # is NOT down - the notice is about production - and were ignored whole.
    at_line = _line_at(t, at) if at is not None else None

    def _separate(sc, x):
        # F15: "测试环境：08:00-10:00" under the production "维护时间：14:00-16:00"
        # is the test environment's own window on its own line, not the scope
        # of the notice.
        if at_line is None:
            return False
        pos = t.find(sc)
        if pos < 0:
            return False
        ln = _line_at(t, pos + x.start())
        return ln != at_line and bool(_ANY_RANGE_RE.search(t, ln[0], ln[1]))

    m = next((x for sc in scope for x in _NONPROD_RE.finditer(sc)
              if not _NONPROD_OK_RE.match(sc, x.end()) and not _separate(sc, x)), None)
    if not m:
        return None
    if _PROD_RE.search(t) and not _PROD_OK_RE.search(t):
        return ("needs_human",
                "maintenance of “{}” - the notice names a test environment and "
                "production, and does not say production is unaffected".format(
                    m.group(0)))
    return ("ignore",
            "maintenance of a non-production environment (“{}”) - not a "
            "production outage".format(m.group(0)))


# Wins outright (except over a reschedule, which is checked first), and only
# inside its own sentence - see _done_spans and classify().
#
# The cancel half used to be a bare `取消|cancell?ed`, which matched any use of
# the word: "All ongoing rounds will be cancelled during the maintenance" is a
# perfectly ordinary notice and was being thrown away. The cancel word now has
# to be tied to the maintenance itself, on BOTH sides.
#
# The gaps below are shared by every branch, so one notice cannot be read two
# ways depending on which verb the provider happened to use.
#
# English. ";" always opens a new clause, and so does an "unfinished/pending"
# noun phrase: F60 "Scheduled maintenance on <window>; unfinished games will be
# cancelled" walked the 80-character gap straight to the GAMES' verb and a
# real notice was thrown away as a cancelled one. "games" itself stays OUT of
# the noun list - with it, "Scheduled maintenance for Baccarat games on
# <window> is cancelled" filled a withdrawn window, which is worse. An object
# noun that names a SUBSYSTEM ("maintenance of the withdrawal system",
# "jackpot server") is the maintenance's own subject, not a cancelled object
# (F30), so it may stand in the gap.
_SUBSYS_EN = (r"(?:system|server|platform|service|module|feature|function|page"
              r"|channel|gateway|engine|api)s?\b")
_GAP_EN_DONE = (r"(?:(?!\b(?:" + _OBJ_EN + r")\b(?!\s+" + _SUBSYS_EN + r"))"
                r"(?!\b(?:unfinished|pending|ongoing|unsettled|incomplete"
                r"|outstanding|in[\s-]progress)\b)"
                r"(?:[^.;；\n]|(?<=\d)\.(?=\d)))")
# Chinese. 申请/请求/提款/存款 are cancelled objects too ("维护期间提款申请将取消")
# - kept out of _OBJ_ZH itself because the reschedule guard shares that list.
# A noun directly followed by 系统/平台/维护 names the maintenance (游戏维护,
# 交易系统维护: F30) and is let through.
_OBJ_ZH_DONE = _OBJ_ZH + r"|申请|申請|请求|請求|提款|存款"
_SUBSYS_ZH = r"(?:系统|系統|平台|服务|服務|模块|模塊|功能|通道|服务器|伺服器|维护|維護)"
_GAP_ZH_DONE = (r"(?:(?!(?:" + _OBJ_ZH_DONE + r")(?!" + _SUBSYS_ZH + r"))"
                + _NOT_STOP_ZH + r")")
# The completion gap additionally stops at a comma: "维护时间：<window>，12:00后
# 游戏恢复正常" is a timetable, and "例行维护：<window>，维护将于12:00结束" (F47)
# describes the END of an upcoming window. A completion that restates the
# window puts it before the verb with no comma ("例行维护 <window> 提前完成").
_GAP_ZH_DONE_CLAUSE = (r"(?:(?!(?:" + _OBJ_ZH_DONE + r")(?!" + _SUBSYS_ZH + r"))"
                       r"(?:[^。．.\n，,；;]|(?<=\d)\.(?=\d)))")
# A negated verb is the opposite report: "维护期间未完成的游戏将自动结算" is about
# the UNFINISHED games, and "本次维护不会取消" says it goes ahead.
_ZH_NOT_BEFORE = (r"(?<![未没沒不无無])(?<!没有)(?<!沒有)(?<!不会)(?<!不會)"
                  r"(?<!并未)(?<!並未)(?<!尚未)")
# The English verbs. The copula is spelled out per tense on purpose: a loose
# "has (been)? cancelled" also read "Scheduled maintenance on <window>, we have
# cancelled all open rounds" as a cancelled maintenance.
_CANCEL_EN_V = (
    r"(?:(?:is|are|was|were|being)\s+(?:now\s+|already\s+|officially\s+)?"
    r"|(?:has|have|had)\s+(?:now\s+|already\s+|officially\s+)?been\s+"
    r"|(?:will|shall)\s+(?:now\s+)?be\s+)"
    r"(?:cancell?ed|called\s+off|withdrawn|scrapped|aborted|revoked|voided"
    r"|annulled)\b"
    # F21: the negation that comes AFTER the subject.
    r"|(?:will\s+not|won['’]t|shall\s+not|would\s+not|is\s+not\s+going\s+to)"
    r"\s+(?:be\s+)?(?:take\s+place|taking\s+place|proceed(?:ing)?|go(?:ing)?"
    r"\s+ahead|happen(?:ing)?|occur|carried\s+out|held|conducted|performed)\b"
    r"|no\s+longer\s+(?:takes?\s+place|proceeds?|go(?:es|ing)?\s+ahead|happens?"
    r"|appl(?:y|ies)|required|needed|necessary|scheduled|planned|valid"
    r"|be\s+(?:required|needed|carried\s+out|held|taking\s+place))\b"
    # The bare participle of a heading or a terse update: "Maintenance
    # Cancelled", "【Maintenance Cancelled】", "Scheduled maintenance <window>
    # cancelled." - at the end of its clause, or before its reason.
    r"|(?<![A-Za-z])(?<!\bbe\s)(?<!\bbeen\s)(?<!\bbeing\s)(?<!\bto\s)"
    r"(?<!\bget\s)(?:cancell?ed|called\s+off)"
    r"(?=[ \t]*(?:[.!。,，;；:：\n)\]】]|$)|[ \t]+(?:due\s+to|because|owing\s+to)\b)")
# F65: "has ended early", "is complete", "<window> completed.", "is over". A
# bare past participle only counts at the end of its clause or before
# early/successfully - "will be completed by 12:00" and "completed within two
# hours" are promises.
_COMPLETE_EN_V = (
    r"(?:has|have|had)\s+(?:now\s+|already\s+|just\s+|successfully\s+)?"
    r"(?:been\s+)?(?:completed|finished|concluded|ended|done)\b"
    r"|(?:is|are|was|were)\s+(?:now\s+|already\s+|successfully\s+)?"
    r"(?:complete|completed|finished|concluded|ended)\b"
    # "is over" / "is done" only at the end of the clause: "the maintenance
    # on <window> is over 2 hours long" is a duration.
    r"|(?:is|are|was|were)\s+(?:now\s+|already\s+)?(?:over|done)"
    r"(?=[ \t]*(?:[.!。,，;；\n]|$)|\s+(?:now|early|already)\b)"
    r"|(?<![A-Za-z])(?<!\bbe\s)(?<!\bbeen\s)(?<!\bto\s)(?<!\bbeing\s)(?<!\bget\s)"
    r"(?:ended|finished|completed|concluded)"
    r"(?=\s+(?:early|successfully|ahead\s+of\s+schedule|on\s+time)\b"
    r"|[ \t]*(?:[.!。,，;；\n]|$))")
# Only what a restated window is made of: digits, date/time punctuation, zone
# and month/weekday words, AM/PM, and the prepositions that introduce a window.
# A noun can never be part of it, which is the whole point (see R1.57 below).
_WIN_ONLY = (r"(?:[\s\d:：\-–—~～/.,()（）+]|\b(?:GMT|UTC|on|for|at|from|to"
             r"|until|scheduled|planned|originally|between|and|am|pm"
             r"|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|mon|tue|wed"
             r"|thu|fri|sat|sun)[a-z]*)\b)")

_DONE_RES = [
    # Chinese completion. The gap excludes an object noun for the same reason
    # the cancel branch does, and the lookahead is the future frame in
    # miniature: "维护完成后将通知", "维护完成前请勿登录", "维护结束时间以实际为准",
    # "维护结束即恢复" (F47) all describe an upcoming maintenance's END, not a
    # report that it ended. 提前 is the early finish (F65 "提前完成").
    # F47: an estimate in the gap ("维护大约12:00完成", 预估/约/最晚/最迟) is the
    # expected END of a coming maintenance, and 完成将另行通知 is a promise.
    re.compile(r"(?:维护|維護|维修|維修|升级|升級)(?:(?!大约|大約|大概|约|約|预估|預估|预计"
               r"|預計|最晚|最迟|最遲|左右|应该|應該|应当|應當|有望|计划|計劃|按计划|按計劃|原则上|原則上"
               r"|\d{1,2}[:：]\d{2}(?:之?前)?(?:将|將|会|會)?(?:在|于|於)?\s*(?:完成|结束|結束))"
               + _GAP_ZH_DONE_CLAUSE
               + r"){0,40}?" + _ZH_NOT_BEFORE + r"(?P<v>(?:提前|顺利|順利)?"
               r"(?:完成|結束|结束|恢复正常|恢復正常))"
               r"(?![后後前时時即将將会會])(?!之[后後前])(?!以[后後前])(?!为止|為止)"
               r"(?!通知|公告|消息|邮件|郵件|报告|報告)"),
    re.compile(r"(?P<v>已经完成|已經完成)"),
    # F47: an estimate AND a past marker - "维护大约在11:30已完成" is done.
    re.compile(r"(?:维护|維護)[^。\n]{0,30}?(?P<v>(?:已|现已|現已)(?:提前)?(?:完成|结束|結束))"),
    # Chinese cancellation, forward: 维护 ... 取消. "维护期间未结算注单将取消"
    # cancels the BETS, so an object noun in the gap blocks the match - the
    # English branch has had this guard for a while and the Chinese one did not.
    # The reach is 40, not 14 (F21): "例行维护（2026-09-23 10:00-12:00 GMT+8）
    # 已取消" puts the whole window between the noun and the verb.
    # A cancelled OBJECT may also follow the verb once the reach crosses a
    # comma: "例行维护：<window>，届时将取消所有未完成订单" cancels the ORDERS.
    re.compile(r"(?:维护|維護|维修|維修)" + _GAP_ZH_DONE
               + r"{0,40}?" + _ZH_NOT_BEFORE
               + r"(?P<v>(?:已|将|將|被|已被|已经|已經)?取消)"
               r"(?![^。．.，,；;\n]{0,8}?(?:" + _OBJ_ZH_DONE + r")(?!"
               + _SUBSYS_ZH + r"))"),
    # ... and backward: 取消 ... 维护. Long only when the gap is an 原定 clause
    # ("已取消原定于<window>的例行维护"): a bare 取消 forty characters before an
    # unrelated 维护 is somebody else's cancellation.
    # R1.44: 撤销 / 撤回 withdraw it just as 取消 does ("撤销<window>维护").
    re.compile(_ZH_NOT_BEFORE + r"(?P<v>(?:已|将|將)?(?:取消|撤销|撤銷|撤回))"
               r"(?:" + _GAP_ZH_DONE + r"{0,14}"
               r"|(?:了)?(?:原定|原訂|原计划|原計劃|定于|定於)" + _GAP_ZH_DONE
               + r"{0,40}?"
               # R1.20: "我们已取消<window>的游戏维护" puts the whole window between
               # the verb and the noun. Long reach only when it ends in a
               # 的-attached maintenance noun, so a bare 取消 far from an
               # unrelated 维护 is still somebody else's cancellation.
               r"|" + _GAP_ZH_DONE + r"{0,40}?的(?:游戏|遊戲|系统|系統|例行|定期|平台"
               r"|服务器|伺服器)?"
               # R1.44: "撤销<window>维护" with no 的 - only when nothing but
               # window material stands between the verb and the noun.
               r"|(?:[\s\d:：\-–—~～/.()（）+]|GMT|UTC|年|月|日|号|號){4,40}?)"
               r"(?:维护|維護)"),
    # F21: 将不进行 / 不再进行 / 撤销 - the negation after the subject. The verb
    # must end its clause: "维护期间不进行结算" suspends SETTLEMENT.
    re.compile(r"(?:维护|維護|维修|維修)" + _GAP_ZH_DONE
               + r"{0,40}?(?P<v>(?:将|將|会|會|也)?(?:不再|不会|不會|暂不|暫不|不)"
               # R1.57: 不做了 / 不进行了 / 不搞了 end the clause with a 了.
               r"(?:进行|進行|执行|執行|实施|實施|举行|舉行|做|搞)(?:了|啦)?"
               r"(?=[\s。．.，,；;！!）)\]】]|$)"
               r"|撤销(?![一-鿿])|撤銷(?![一-鿿])|作罢|作罷)"),
    # R1.45: "<window> 维护暂停" - the maintenance itself is suspended. Only
    # when 暂停 ends the clause, and never after 因 / 由于 / 期间: "游戏因维护暂停，"
    # and "维护期间暂停服务" pause the GAMES and the service, not the work.
    re.compile(r"(?<![因为為于於间間期])(?:维护|維護)(?:计划|計劃)?(?P<v>暂停|暫停|中止)"
               r"(?=[\s。．.，,；;！!）)]|$)"),
    # R1.57: 作废 / 撤回 withdraw the maintenance outright ("<window>维护作废",
    # "维护计划撤回"). Its own rule because the same words also void a NOTICE -
    # "以上维护公告（<window>）作废" retracts the announcement, which _retraction
    # sends to a person (the row may hold the withdrawn window) - so the gap
    # may not cross 公告 / 通知 / 消息 / 信息.
    re.compile(r"(?:维护|維護|维修|維修)(?:(?!公告|通知|消息|信息|訊息)"
               + _GAP_ZH_DONE + r"){0,40}?(?P<v>(?:计划|計劃)?(?:撤回|作废|作廢))"
               r"(?![一-鿿])"),
    # English completion. Kept broad on purpose and disarmed by the future-frame
    # guard instead: "we will notify you once it has been completed" is the
    # single commonest closing line in this corpus and it is NOT a report.
    # R1.75: "Maintenance completed notice will be sent afterwards" and "a
    # maintenance completed notification will be posted" use the phrase as a
    # NOUN MODIFIER - a promise of a later message - and threw away the notice.
    re.compile(r"maintenance\s+(?P<v>(?:has\s+been\s+|have\s+been\s+|was\s+|is\s+)?"
               r"(?:completed|finished|concluded))(?!\s+(?:notices?|notifications?"
               r"|messages?|announcements?|updates?|e-?mails?|reports?|confirmations?"
               r"|alerts?)\b)", re.I),
    # F65: the same with the window restated between the noun and the verb.
    re.compile(r"maintenance" + _GAP_EN_DONE + r"{0,80}?(?:[ \t]*\n[ \t]*)?"
               r"\b(?P<v>" + _COMPLETE_EN_V + r")", re.I),
    # F65: "<window>. It has ended early." / "<window>; it has ended early." -
    # the same pronoun bridge the cancel branch has (R1.25).
    re.compile(r"maintenance" + _GAP_EN_DONE + r"{0,80}?[ \t]*[.;；][ \t]*(?:\n[ \t]*)?"
               r"(?:it|this|that|the\s+maintenance)[ \t]+(?:now[ \t]+)?"
               r"\b(?P<v>" + _COMPLETE_EN_V + r")", re.I),
    re.compile(r"\b(?P<v>(?:has|have)\s+been\s+completed)\b", re.I),
    re.compile(r"\b(?P<v>completed\s+successfully)\b", re.I),
    # The service stated as RESTORED - "All games are back online now", "Services
    # have been restored", 游戏已恢复正常. After a notice ("Scheduled maintenance
    # <X>. All games are back online now.") it re-announced the finished outage,
    # and on its own /vacheck force walked past it and re-wrote the notice above.
    # Present or perfect only: "will be back online at 12:00" and "once games
    # are back online" are the future frame and the condition, refused below.
    re.compile(r"\b(?:all\s+)?(?:(?:our|the)\s+)?(?<!other\s)(?:games?|services?|systems?|platforms?"
               r"|servers?|site|website|lobby|lobbies|tables?)\s+(?P<v>(?:are|is|have\s+been"
               r"|has\s+been)\s+(?:now\s+|fully\s+|all\s+)*(?:back\s+(?:online|up|to\s+normal)"
               r"|(?:fully\s+)?restored|operational\s+again|running\s+normally\s+again))\b",
               re.I),
    re.compile(r"(?:游戏|遊戲|服务|服務|系统|系統|平台|服务器|伺服器)(?P<v>(?:已|已经|已經)"
               r"(?:全部|全面)?(?:恢复|恢復)(?:正常|上线|上線|运行|運行)?)"),
    # English cancellation. The gap already refuses an object noun; the noun
    # standing to the LEFT of "maintenance" is checked in _done_spans, because
    # "All ongoing rounds before the maintenance will be cancelled" put its
    # subject on the other side and walked straight through.
    #
    # The gap has to clear the window the notice restates - "maintenance
    # planned for 2026-09-24 10:00 - 12:00 (GMT+8) is cancelled" is 46
    # characters - so {0,40} silently missed real cancellations and the window
    # was then written as active. One line break may stand right in front of
    # the verb (F21 "Scheduled maintenance <window>\nhas been cancelled."): a
    # line that OPENS with the verb has no subject of its own.
    #
    # R1.25: the same holds across ONE ';', '；' or '.' when the next clause
    # has no subject of its own or only a pronoun for it. F60 made ';' end
    # the gap so "<window>; unfinished games will be cancelled" stays live,
    # and that also hid "Scheduled maintenance on <window>; it has been
    # cancelled" - ignore before, a write of the withdrawn window after. A
    # pronoun (it/this/that/which) or nothing at all can only point back at
    # the maintenance; a noun ("unfinished games") is a new subject and is
    # still not bridged. The '.' form ("<window>. It has been cancelled.")
    # wrote the withdrawn window at bd9fc12 too.
    # F24: the pronoun may open the NEXT LINE too, with no full stop before it
    # ("<window> (GMT+8)\nThe above has been cancelled.").
    re.compile(r"maintenance" + _GAP_EN_DONE + r"{0,80}?(?:(?:[ \t]*\n[ \t]*"
               r"|[ \t]*[.;；][ \t]*(?:\n[ \t]*)?)(?:and[ \t]+)?"
               # F24: "Please note (that) it…", "The above…", "The maintenance
               # above…" - each points back at the maintenance just stated.
               r"(?:(?:please[ \t]+note[ \t]+(?:that[ \t]+)?)?(?:it|this|that|which"
               r"|the[ \t]+above|the[ \t]+(?:above[ \t]+)?(?:maintenance|notice))"
               r"(?:[ \t]+above)?[ \t]+)?(?:now[ \t]+)?)?"
               r"\b(?P<v>" + _CANCEL_EN_V + r")", re.I),
    # F25: "Slots: <window> has been cancelled" / "Slots <window> cancelled" -
    # the cancelled SIBLING of a per-product notice names the product, not the
    # word maintenance, and was unioned into the live window.
    # Either the product is a LABEL (colon) or the verb has its auxiliary: a
    # bare "(withdrawn)" tag after the window stays with R1.57 (needs_human).
    re.compile(r"(?:^|(?<=[\n.;；]))[ \t]*(?-i:[A-Z])[\w &'-]{1,24}?[ \t]*[:：]"
               + _WIN_ONLY + r"{4,60}?\b(?P<v>(?:(?:has|have)\s+been\s+|is\s+|was\s+)?"
               r"(?:cancell?ed|called\s+off|withdrawn))\b", re.I | re.M),
    re.compile(r"(?:^|(?<=[\n.;；]))[ \t]*(?-i:[A-Z])[\w &'-]{1,24}?[ \t]*"
               + _WIN_ONLY + r"{4,60}?\b(?P<v>(?:(?:has|have)\s+been|is|was)\s+"
               r"(?:cancell?ed|called\s+off|withdrawn))\b", re.I | re.M),
    # "Cancellation of the scheduled maintenance" - direct object only. The
    # cancellation OF THE PROMO during the maintenance is about the promo.
    re.compile(r"(?P<v>cancell?ation)\s+of\s+(?:the\s+|our\s+|this\s+)?"
               r"(?:scheduled\s+|routine\s+|planned\s+|upcoming\s+)?maintenance"
               r"|maintenance\s+(?P<v2>cancell?ation)", re.I),
    # "We are cancelling the maintenance", and F21 "We have cancelled the
    # scheduled maintenance" / "we will not have the scheduled maintenance".
    # Only determiners and maintenance adjectives may stand between: the
    # {0,12} of the -ing form is deliberately short, because "cancelling the
    # rounds during the maintenance" is 22 characters between and must NOT
    # match - and "cancelled bets during maintenance" must not either.
    re.compile(r"(?P<v>cancell?ing)\s+(?:the\s+)?" + _NOT_STOP + r"{0,12}maintenance",
               re.I),
    # F30: "We have cancelled the game / Bonus system / Tournament platform
    # maintenance on <X>" - a subsystem named between the determiner and the
    # noun. No preposition may stand there: "cancelled the rounds during the
    # maintenance" cancels the rounds.
    re.compile(r"(?P<v>cancell?ed|called\s+off)\s+(?:the|our|this|that|all)\s+"
               r"(?:(?!(?:during|before|after|for|in|at|on|of|with|from|by|bets?|rounds?"
               r"|orders?|games\s+in|spins?|requests?|withdrawals?|deposits?|transactions?)\b)"
               r"[A-Za-z][\w-]*\s+){1,3}maintenance\b", re.I),
    # F30: "Scheduled maintenance on <X> for tournaments and jackpots has been
    # cancelled" - the maintenance's own purpose phrase before the verb.
    re.compile(r"maintenance" + _WIN_ONLY + r"{0,60}?\bfor\s+(?:[\w&-]+\s+){1,4}?"
               r"(?P<v>(?:has|have)\s+been\s+(?:cancell?ed|called\s+off|withdrawn))\b", re.I),
    re.compile(r"(?P<v>(?:cancell?ed|called\s+off|(?:will\s+not|won['’]t)"
               r"\s+(?:have|hold|perform|conduct|carry\s+out|proceed\s+with"
               r"|go\s+ahead\s+with))"
               r")\s+(?:(?:the|our|this|that|today['’]s|tonight['’]s|upcoming"
               r"|scheduled|routine|planned|regular|weekly|monthly|server"
               r"|system|platform|previously|announced|any)\s+){0,4}"
               r"maintenance", re.I),
    # R1.57: the withdrawal said another way - "is on hold", "has been put
    # off", "will be skipped this week", "has been suspended", "is not
    # required anymore". These words describe the OUTAGE itself just as often
    # ("all services will be suspended", "a restart is not required"), so they
    # count only when nothing but the window stands between "maintenance" and
    # the verb: _WIN_ONLY is digits, date/time punctuation, zone and month
    # words and the prepositions that introduce a window - never a noun.
    re.compile(r"maintenance" + _WIN_ONLY + r"{0,60}?\b(?P<v>(?:(?:has|have|had)"
               r"\s+been|is|are|was|were|will\s+be)\s+(?:now\s+|currently\s+"
               r"|temporarily\s+|officially\s+)?(?:suspended|(?:put\s+)?on\s+hold"
               r"|put\s+off|skipped|not\s+(?:required|needed|necessary)"
               r"(?:\s+any\s*more|\s+anymore)?))\b", re.I),
    # "We are not able to do maintenance on <window>" - the provider saying
    # the work will not happen. Only a doing-verb may follow, so "you will not
    # be able to access the games during maintenance" is not one.
    re.compile(r"\b(?P<v>(?:(?:are|is|am|will\s+be)\s+)?(?:not\s+able\s+to"
               r"|unable\s+to|cannot|can['’]t|could\s+not|couldn['’]t)\s+(?:do"
               r"|perform|conduct|carry\s+out|hold|proceed\s+with|go\s+ahead"
               r"\s+with))\s+(?:the\s+|our\s+|this\s+|any\s+|a\s+)?(?:scheduled"
               r"\s+|planned\s+|routine\s+)?maintenance", re.I),
]

# F21: a status word standing for the whole notice - "CANCELLED: Scheduled
# maintenance <window>", "<window> — CANCELLED", "Status: Cancelled", 【已取消】.
# Line-anchored so "Cancelled bets will be refunded" is not one; the line must
# name no object noun, and the message must be about maintenance at all.
_STATUS_DONE_RES = [
    re.compile(r"^[^\w\n]*(?:status\s*[:：]\s*)?(?P<v>cancell?ed)\b"
               r"(?=[ \t]*(?:[:：!.\-—–|】\])）]|$))", re.I | re.M),
    re.compile(r"(?:[—–\-:：|,，]|\bstatus\s*[:：])[ \t]*[(\[【（]?"
               r"(?P<v>cancell?ed)[)\]】）]?[ \t]*[.!]*[ \t]*$", re.I | re.M),
    # F24: a line that is ONLY the cancellation, under the notice it cancels:
    # "【维护通知】\n维护时间：<X>\n已取消。"
    re.compile(r"^[^\w\n]*(?P<v>(?:已|现已|現已)?(?:取消|撤回|作废|作廢))[。.!！]*[ \t]*$", re.M),
    # R1.57: a heading that IS the cancellation - "取消通知：<window> 维护".
    re.compile(r"^[^\w\n]*(?P<v>(?:(?:维护|維護)\s*)?取消(?:通知|公告))", re.M),
    # R1.44: "Cancel maintenance <window>" opening the message, and "<window>
    # cancel" closing it - an operator's request or a terse withdrawal, never
    # a notice to write.
    re.compile(r"^[^\w\n]*(?P<v>cancel(?:led)?)[ \t]+(?:the[ \t]+)?(?:scheduled[ \t]+)?"
               r"maintenance\b", re.I | re.M),
    re.compile(r"(?<=[\d)）])[ \t]+(?P<v>cancel(?:led)?)[ \t]*[.!]*[ \t]*$", re.I | re.M),
    re.compile(r"(?P<v>【(?:已)?取消】|[（(](?:已)?取消[)）]"
               r"|(?:状态|狀態)\s*[:：]\s*(?:已)?取消)"),
    # F65: the COMPLETION as a status - "[Completed] <notice>", "Status:
    # Completed", "【已完成】<notice>". A re-post of a finished notice re-wrote
    # the row and re-carded it as scheduled.
    re.compile(r"^[^\w\n]*[\[【(（][ \t]*(?P<v>completed|complete|finished|done|resolved"
               r"|已完成|已结束|已結束|完成)[ \t]*[\]】)）]", re.I | re.M),
    re.compile(r"(?:\bstatus|状态|狀態)[ \t]*[:：][ \t]*(?P<v>completed|complete|finished|done"
               r"|resolved|已完成|已结束|已結束|完成)(?![\w\u4e00-\u9fff])", re.I),
]

# Kept as a single greppable pattern for callers and for `grep DONE_RE`; the
# guards above are what classify() actually consults.
# (The per-branch "v" groups mark where each verb starts; a combined pattern
# cannot name one group twice, so they are plain groups here.)
DONE_RE = re.compile("|".join(
    "(?:{})".format(re.sub(r"\(\?P<v2?>", "(?:", r.pattern))
    for r in _DONE_RES + _STATUS_DONE_RES), re.I | re.M)

# A completion promised for later is not a completion. Anything governed by one
# of these is a future frame and never suppresses.
#
# "would" only as a modal: "We would like to inform you that the scheduled
# maintenance ... has been cancelled" is the politest way to REPORT a
# cancellation, and reading it as a future frame wrote the withdrawn window
# as live (F21).
_FUTURE_FRAME_RE = re.compile(
    r"\b(?:will|shall|would(?!\s+(?:like|love)\b)|going\s+to|once|after|when"
    r"|upon|before|until|till|as\s+soon\s+as|in\s+the\s+event)\b", re.I)

# The clause right in front of the VERB, not only in front of the noun (F47):
# "Scheduled maintenance <window>, we will notify you if this maintenance is
# cancelled" names the maintenance at the start of the sentence and promises
# a cancellation notice at the end - a conditional, not a report. The Chinese
# lists are split because 将 is a real cancellation ("本次维护将取消" = it will
# be cancelled) but a future completion ("维护将于12:00结束" = it will END then).
_VERB_FRAME_EN_RE = re.compile(
    r"\b(?:if|unless|whether|in\s+case|should)\b", re.I)
_VERB_FRAME_ZH_COND = (r"如果|假如|倘若|若是|万一|萬一|一旦|是否|可能|或将|或將"
                       # 如下/例如 and 待定 are not conditions: "维护安排如下：
                       # <window> 已取消" reports the cancellation.
                       r"|(?<![例比])如(?![下此期同今])|若|待(?!定)"
                       r"|可以|可|能够|能夠")
_VERB_FRAME_ZH_CANCEL_RE = re.compile(_VERB_FRAME_ZH_COND)
_VERB_FRAME_ZH_DONE_RE = re.compile(
    _VERB_FRAME_ZH_COND + r"|将|將|预计|預計|预期|預期|会|會|即将|即將|届时|屆時"
    r"|要|后|後|何时|何時")

_CLAUSE_BOUND = "。．.,，;；!?！？\n"
_SENT_BOUND = "。．.;；!?！？\n"

# A correction notice: it restates the wrong window next to the right one.
# G2.2: also the Wrong:/Correct: and 错误：/正确： label pairs, "is wrong", and
# 而非/而不是/"rather than" - the forms a follow-up naming both windows uses.
_CORRECTION_RE = re.compile(
    r"\btypo\b|\bmistake\s+in\s+(?:the|our)\b|笔误|筆誤|(?:作废|作廢)"
    r"|\bcorrect(?:ion|ed)\b|\bdisregard\b|\b(?:was|is|were|are)\s+(?:wrong"
    r"|incorrect)\b|\b(?:wrong|incorrect|correct)\s*[:：]|\brather\s+than\b"
    r"|\bnot\s+(?:on\s+)?(?=\d)|更正|勘误|勘誤|有误|有誤|错误[:：]|錯誤[:：]"
    r"|正确[:：]|正確[:：]|而非|而不是",
    re.I)

# G2.1: the provider RETRACTS a notice - posted in the wrong group, sent by
# mistake, meant for another merchant, void. A retraction very often restates
# the withdrawn window ("Please ignore the scheduled maintenance notice for
# 26/09/2026 02:00-06:00 (GMT+8) above, it was sent to the wrong group"), and
# that sentence reads exactly like a fresh notice: the withdrawn window was
# written to the shared sheet and carded as "Scheduled maintenance". It is
# never written; classify() returns needs_human so a person checks what the
# row now holds. "Please ignore this message if you have already updated" is
# a condition, not a retraction, and "please ignore any error messages during
# the maintenance" names no notice.
_RETRACT_NOTICE = r"(?:notice|notification|message|msg|announcement|post)s?\b"
_RETRACT_RE = re.compile(
    # "Please ignore / disregard" + WHICH notice: a colon, a bare pronoun,
    # "the above/previous", a notice noun qualified as the maintenance one or
    # the earlier one, or a notice noun that is then pointed at ("the notice
    # for <window> above"). "Please ignore the error messages" names none.
    r"\b(?:please|pls|plz|kindly)\s+(?:ignore|disregard)\s*(?:[:：]"
    r"|\b(?:it|this|that|them|these|those)\b(?!\s+(?!notice|notification"
    r"|message|msg|announcement|post)[a-z])"
    r"|\bthe\s+(?:above|previous|earlier|last|prior)\b"
    r"|\b(?:the|this|that|our|my)\s+(?:(?:above|previous|earlier|last|prior"
    r"|wrong|maintenance|scheduled|system|server|emergency|downtime|upgrade"
    r"|recent|latest|former|mentioned)\s+){1,4}" + _RETRACT_NOTICE
    + r"|\b(?:the|this|that|our|my)\s+" + _RETRACT_NOTICE
    + r"(?=\s*(?:above|below|earlier|sent|posted|for|about|regarding|of|on|dated"
    r"|[(（,，.;；:：]|$)))"
    r"(?![^.;\n]{0,60}\bif\b)"
    r"|\b(?:sent|posted|shared|forwarded)\s+(?:to|in|into)\s+(?:the\s+|a\s+)?wrong"
    r"\s+(?:group|chat|channel|chatroom)\b|\bwrong\s+(?:group|chat|channel)\b"
    r"|\b(?:sent|posted|shared|published)\s+(?:out\s+)?(?:by\s+mistake|in\s+error"
    r"|by\s+accident|accidentally|mistakenly|erroneously)\b"
    r"|\b(?:mistakenly|accidentally|erroneously)\s+(?:sent|posted|shared|published)\b"
    r"|\b(?:was|is|were)\s+(?:meant|intended)\s+for\s+(?:another|a\s+different"
    r"|other)\b"
    r"|\b(?:notice|announcement|message|notification)\s+(?:is|was|has\s+been)\s+"
    r"(?:now\s+)?(?:void|invalid|null\s+and\s+void|retracted)\b"
    # G2.1: the same with the window between - "The maintenance notice for <X>
    # is void / is null and void / is invalid, please ignore".
    r"|\b(?:notice|announcement|notification)\b[^.;\n]{0,60}?\b(?:is|was|has\s+been)\s+"
    r"(?:now\s+)?(?:void|invalid|null\s+and\s+void|retracted|no\s+longer\s+valid)\b"
    r"|\bwe\s+(?:hereby\s+)?(?:retract|withdraw|revoke|rescind)\s+(?:the|our|this|that)\s+"
    r"(?:[\w-]+\s+){0,3}?(?:notice|announcement|notification|message)\b"
    r"|(?:^|(?<=[\s，,。]))(?:忽略|无视|無視)(?:以上|上面|上述|之前|前面|此|该|該|这|這)"
    r"(?:的|条|條|则|則)?[^。\n]{0,6}?(?:通知|公告|消息)"
    r"|(?:以上|上面|上述|之前|前面|刚才|剛才)的?(?:维护|維護)?(?:通知|公告|消息)[^。\n]{0,60}?"
    r"(?:请|請)?(?:忽略|无视|無視)"
    # 请忽略 names what is ignored: "维护期间如出现异常提示请忽略" is an
    # ordinary notice telling players to ignore an error screen, and so is
    # "如收到报错信息请忽略".
    r"|(?:请|請|烦请|煩請|敬请|敬請)(?:忽略|无视|無視)(?=[^。\n]{0,6}?(?:以上|上面"
    r"|上述|上条|上條|上一|此|该|該|本|这|這|前面|之前|刚才|剛才|通知|公告|消息))"
    r"|(?:通知|公告)(?:发送错误|發送錯誤|发错了|發錯了|有误|有誤)?[，,]?\s*"
    r"(?:请|請)(?:忽略|无视|無視)"
    r"|发错群|發錯群|发错了|發錯了|误发|誤發|错发|錯發"
    r"|[【\[]撤回[】\]]"
    r"|撤回(?:此|该|該|以上|上述|上面|这条|這條|本)?(?:则|則|条|條)?(?:通知|公告|消息|信息)"
    r"|(?:通知|公告|消息|信息)(?:已|已经|已經)?撤回"
    r"|(?:通知|公告)[^。\n]{0,40}?(?:作废|作廢)"
    r"|(?:以上|上述|此|该|該|本)(?:则|則|条|條)?(?:通知|公告|消息)(?:已)?(?:无效|無效)"
    # A void TAG opening the message: "[VOID] <notice>", "【作废】<notice>",
    # "[Please ignore] <notice>", "[VOID - resent below] <notice>". The notice
    # under it is withdrawn, not announced, and a fresh post of one wrote the
    # voided window. Square or 【】 brackets, and the tag is the void word
    # itself: "(Ignore if already done)" and "[Maintenance Notice]" are not.
    r"|^\s*[\[【]\s*(?:void(?:ed)?|invalid|null\s+and\s+void"
    r"|(?:(?:please|pls|kindly)\s+)?(?:ignore|disregard)"
    r"|作废|作廢|无效|無效|(?:请|請)?忽略)\s*(?:[-–—:：]\s*[^\]】\n]{0,30})?[\]】]",
    re.I)
# R1.57: a void TAG at either END of a notice. The leading [VOID]/【作废】 form
# is in _RETRACT_RE; these were not, and each one - "<notice> [VOID]", "VOID -
# <notice>", "(VOID) <notice>", "<notice>（作废）", "<notice> - please ignore,
# wrong info", "<notice> (withdrawn)" - wrote the withdrawn window and carded
# it as scheduled. Checked BEFORE the correction exemption below: "(incorrect)"
# as a tag voids the notice it hangs on; it is not a correction naming a new
# window. The tag must close right after its word, so "(Ignore if already
# done)" and "- please ignore the error popup" are not tags.
_VOID_TAG_RE = re.compile(
    r"[\[【(（]\s*(?:void(?:ed)?|invalid|withdrawn|retracted|incorrect|outdated"
    r"|obsolete|cancell?ed|(?:(?:please|pls|kindly)\s+)?(?:ignore|disregard)"
    r"|作废|作廢|无效|無效|撤回|已撤回|(?:请|請)?忽略|已取消)\s*[\]】)）]"
    r"\s*[.!。！]*\s*$"
    r"|\s[-–—]\s*(?:(?:please|pls|kindly)\s+)?(?:ignore|disregard)"
    r"(?:\s+(?:this|it|that))?\s*(?:[,，(（]\s*(?:wrong|sent|posted|mistake|error"
    r"|incorrect|outdated)[^\n]{0,40})?[.!。]*\s*$"
    r"|\s[-–—]\s*(?:void(?:ed)?|withdrawn|retracted|invalid|outdated|obsolete)"
    r"\b[.!]*\s*$"
    r"|^\s*(?:void(?:ed)?|withdrawn|retracted|invalid)\s*[-–—:：]\s"
    # R1.30 / R1.45: "[VOID] <notice>" and "<window> void".
    r"|^\s*[\[【]\s*(?:void(?:ed)?|invalid|withdrawn|retracted|作废|作廢|无效|無效)\s*[\]】]"
    r"|(?<=[\d)）])\s+(?:void(?:ed)?|withdrawn|retracted)\b[.!]*\s*$"
    r"|^\s*[(（]\s*(?:void(?:ed)?|invalid|withdrawn|retracted|作废|作廢|无效|無效)"
    r"\s*[)）]",
    re.I)
# A CORRECTION also says "disregard the previous notice", but it states the
# right window next to it and is read as one (step 4, _CORRECTION_RE).
_CORRECTS_RE = re.compile(
    r"\bcorrect(?:ion|ed)?\b|\bwas\s+(?:wrong|incorrect)\b|\bincorrect\b"
    r"|\b(?:new|updated|revised)\s+(?:maintenance\s+)?(?:time|window|schedule|date)\b"
    r"|更正|勘误|勘誤|有误|有誤|正确时间|正確時間|新的?时间|新的?時間", re.I)


_WRONG_ONLY_RE = re.compile(r"有误|有誤|incorrect|was\s+(?:wrong|incorrect)", re.I)


def _retraction(t: str):
    """The retraction phrase in ``t`` (see _RETRACT_RE), or None.

    A void TAG in front of an otherwise unchanged notice ("[VOID - wrong group]
    Scheduled maintenance ...", "（發錯群，請忽略）...") counts like any other
    retraction. It used to be skipped (_LEAD_TAG_RE) so that vawatch's edit
    rule (G2.5) still saw a fill, but that also let a FRESH post of a voided
    notice write the voided window onto the row. vawatch's edit rule now takes
    the needs_human verdict and still cards the edit with the row's own window.
    """
    tag = _VOID_TAG_RE.search(t)
    if tag:
        return tag
    # R1.30-R1.32: "an updated schedule WILL BE SHARED" / "a revised schedule
    # will follow" PROMISES a correction - it is not one, and it switched this
    # check off, so "Please disregard the previous notice for <X>; an updated
    # schedule will be shared" wrote X. Only a correction actually made counts.
    # G2.1: a word that only says the notice was WRONG (有误, incorrect, "was
    # wrong") corrects nothing unless a second window is given - "上面的维护
    # 通知 <X> 有误，请忽略" withdraws X, and the exemption wrote it.
    ranges = len(_ANY_RANGE_RE.findall(t))
    if any(not _PROMISED_AFTER_RE.match(t, m.end())
           and not (_WRONG_ONLY_RE.fullmatch(m.group(0)) and ranges < 2)
           for m in _CORRECTS_RE.finditer(t)):
        return None
    for m in _RETRACT_RE.finditer(t):
        # "If you received this in the wrong group, please tell us" is a
        # condition in an ordinary notice, not a retraction of it.
        if _EXTEND_FRAME_RE.search(_clause_before(t, m.start())):
            continue
        # R1.42: "维护时间：<X>，之前通知的<Y>作废" voids Y and states X. The
        # phrase names a window and another one stands outside it, so it is a
        # correction (_CORRECTION_RE + _mark_moved read it), not a retraction
        # of the whole message - which sent a determinable X to a person.
        if (_ANY_RANGE_RE.search(m.group(0))
                and _ANY_RANGE_RE.search(t[:m.start()] + "\n" + t[m.end():])):
            continue
        if _replaces_previous(t, m):
            continue
        return m
    return None


# R1.76: the notice REPLACES an earlier one, or tells a reader who already has
# it to ignore the duplicate - it does not withdraw itself. "Please ignore the
# earlier draft", "The earlier notice is void", "请忽略之前的通知，以此为准",
# "Updated notice (please disregard the previous message):", "If you have
# already received this notice, please ignore it", "如已收到，请忽略此消息" each
# sat under a complete notice of their own and sent a determinable window to a
# person (bd9fc12 filled them). Only when the message states a window OUTSIDE
# the retraction's own line: "請忽略上面 <window> 的維護通知，發錯群了" names the
# withdrawn window inside the phrase and is still a retraction.
_POINTS_BACK_RE = re.compile(
    r"\b(?:previous|earlier|prior|last|above|old|older|former|draft|first)\b"
    r"|之前|先前|此前|上一|前面|上面|早前|前一", re.I)
_PREVAILS_RE = re.compile(
    r"以此(?:公告|通知|消息)?(?:为准|為準)|以本(?:公告|通知|消息)(?:为准|為準)"
    r"|\bthis\s+(?:one|notice|message|announcement)\s+(?:is\s+the\s+(?:correct|valid"
    r"|latest|final|current)|prevails|stands|supersedes|replaces)\b"
    r"|\bupdated\s+(?:notice|notification|announcement|version)\b"
    r"|\bsupersedes?\b|\breplaces?\s+the\s+(?:previous|earlier)\b", re.I)
_ALREADY_HAVE_RE = re.compile(
    r"\bif\s+(?:you\s+)?(?:have\s+)?(?:already\s+)?(?:received|got|seen)\b"
    r"|\bin\s+case\s+you\s+(?:have\s+)?(?:already\s+)?received\b"
    r"|如已|如果已|若已|如您已|如果您已|倘若已", re.I)


def _replaces_previous(t: str, m) -> bool:
    # R1.15: read per SENTENCE, not per line - a one-line notice ending "...
    # (GMT+8). If you have already received this notice, please ignore it." or
    # "... Please ignore the earlier notice with the wrong date." states its own
    # window in the sentence before the retraction phrase, and the line-based
    # test saw that window as inside the phrase and sent it to a person.
    ss, se = _sentence_bounds(t, m.start(), m.end())
    sent = t[ss:se]
    if _ALREADY_HAVE_RE.search(sent):
        # "If you have already received this notice, please ignore it" is a
        # condition for a DUPLICATE; it never withdraws the notice it ends.
        return True
    outside = t[:ss] + "\n" + t[se:]
    if not _ANY_RANGE_RE.search(outside):
        return False
    return bool(_POINTS_BACK_RE.search(sent) or _PREVAILS_RE.search(t))


def _sentence_bounds(t: str, a: int, b: int) -> tuple:
    """(start, end) of the sentence around t[a:b]. A dot between digits
    ("2026.09.23", "10.00") ends no sentence."""
    def bound(i):
        ch = t[i]
        if ch in "。！？!?；;\n":
            return True
        return ch == "." and not (0 < i < len(t) - 1 and t[i - 1].isdigit()
                                  and t[i + 1].isdigit())
    s = a
    while s > 0 and not bound(s - 1):
        s -= 1
    e = b
    while e < len(t) and not bound(e):
        e += 1
    return s, min(len(t), e + 1)


_ANY_RANGE_RE = re.compile(r"\d{1,2}\s*[:：.]\s*\d{2}\s*(?:-{1,2}|–|—|~|～|至|到|to)\s*"
                           r"\d{1,2}\s*[:：.]\s*\d{2}", re.I)


# An explicit "nothing planned" answer to our weekly ask.
#
# F66: the bare "no maintenance" alternative also matched the phrase used as a
# MODIFIER - "the hotfix will be deployed tonight with no maintenance
# downtime", "v2.3 released, no maintenance required" - and a release note was
# carded as "the provider says there is no maintenance" (and, with
# VAWATCH_CLEAR_ENABLED=1, would have blanked a live window). The bare form now
# refuses a "with"/"without" in front and a requirement or downtime word after:
# those say the RELEASE needs no maintenance, not that none is planned.
NO_MAINT_RE = re.compile(
    r"no\s+maintenance(?:\s+(?:is|are|was))?(?:\s+\w+){0,3}\s*(?:this|next)?\s*week"
    r"|no\s+maintenance\s+(?:planned|scheduled|plan)"
    r"|(?<!with\s)(?<!without\s)\bno\s+(?:scheduled\s+)?maintenance\b"
    r"(?!\s*(?:is\s+|will\s+be\s+|was\s+)?(?:required|needed|necessary|involved"
    r"|downtime|down\s*time|impact|interruption|window|period|mode))"
    r"|there\s+(?:is|are)\s+no\s+maintenance"
    r"|本周(?:暂|暫)?无(?:任何)?维护|本週(?:暫)?無(?:任何)?維護"
    r"|没有维护|沒有維護|无维护计划|無維護計劃|暂无维护|暫無維護"
    r"|这周没有维护|這週沒有維護",
    re.I)

# A short answer that points at another message ("refer to this notification").
REFERRAL_RE = re.compile(
    r"refer\s+to\s+(?:this|the\s+(?:above|below|attached))"
    r"|please\s+(?:see|refer|check)\s+(?:this|the\s+)?(?:notification|notice|message|above|below)"
    r"|as\s+per\s+(?:the\s+)?(?:notification|notice|announcement)\s+(?:above|below)"
    r"|参考(?:下方|上方|以下|附件)|參考(?:下方|上方|以下|附件)"
    r"|请见|請見|详见|詳見|见下方|見下方",
    re.I)

_REFERRAL_MAX_CHARS = 400

# F64: "no maintenance" quoted inside a QUESTION or a CONDITION is not the
# provider's answer. "请问本周没有维护吗？", "Hi team, no maintenance this week?",
# "如没有维护请回复" and our own ask "Please reply 'No maintenance' if none" all
# came back `clear`, which cards "the provider says there is no maintenance"
# (and with VAWATCH_CLEAR_ENABLED=1 blanks the row). The frame is read in the
# clause in front of the phrase.
# A bare 如/若 counts only directly in front of the phrase ("如没有维护"), so
# "维护安排如下：本周没有维护" is still an answer.
_NO_MAINT_FRAME_RE = re.compile(
    r"\b(?:if|unless|whether|reply|respond|answer|say|type|write)\b"
    r"|如果|假如|假若|倘若|若是|回复|回覆|回答|是否|(?:如|若)\s*$"
    # F64: 若 with the period between ("若本周没有维护"), "In case of no
    # maintenance", and a REQUEST to confirm ("Kindly confirm no maintenance
    # this week", 请确认本周没有维护) - our ask, not the provider's answer.
    r"|(?:如|若)(?:本周|本週|这周|這週|下周|下週|今天|今日|明天|本月)?\s*$"
    r"|\bin\s+case\s+(?:of|there\s+(?:is|are))\b|\bshould\s+there\s+be\b"
    r"|(?:^|[，,\s])(?:确认|確認)(?:一)?下"
    r"|\b(?:please|pls|plz|kindly|can\s+you|could\s+you)\s+(?:help\s+(?:to\s+)?)?confirm\b"
    r"|(?:请|請|麻烦|麻煩|烦请|煩請)(?:您|你|贵司|貴司)?(?:帮忙)?(?:确认|確認)",
    re.I)
# F64: "没有维护的话请告知" (the condition comes after), "本周没有维护嘛".
_NO_MAINT_AFTER_FRAME_RE = re.compile(r"\s*(?:的话|的話|嘛|么|麼|对吧|對吧|是吧|吧|对不对|對不對)")
# F66: a release or hotfix note saying it NEEDS no maintenance is not an answer
# about the week. Only with a week scope ("this week", 本周) does it clear.
_RELEASE_CTX_RE = re.compile(
    r"\b(?:hot-?fix(?:es)?|patch(?:es)?|releas(?:e|ed|es|ing)|release\s+notes?|update[ds]?"
    r"|v\d+(?:\.\d+)+|deploy\w*|roll-?out|config(?:uration)?\s+change|promotion\s+starts)\b"
    r"|热更新|熱更新|版本更新|更新|发版|發版|补丁|補丁|配置变更|配置變更", re.I)
_WEEK_SCOPE_RE = re.compile(
    r"\b(?:this|next|coming)\s+week\b|本周|本週|这周|這週|下周|下週|本星期|这星期|這星期", re.I)


def _no_maint_answer(t: str):
    """The NO_MAINT_RE match that is the provider's own answer, or None.

    A match in a sentence that ASKS (_question_sentences, whatever
    NOTICE_QUESTION_GUARD says - the flag guards writes, and a clear that is
    really a question is a false card either way) or behind a condition /
    reply-with frame does not count.
    """
    qs = _question_sentences(t)
    for m in NO_MAINT_RE.finditer(t):
        if any(s <= m.start() < e for s, e in qs):
            continue
        pre = _clause_before(t, m.start())
        if _NO_MAINT_FRAME_RE.search(pre):
            continue
        if _NO_MAINT_AFTER_FRAME_RE.match(t, m.end()):
            continue
        if re.search(r"\b(?:requires?|required|needs?|needed)\s*$", pre, re.I) or re.match(
                r"\s+(?:is\s+|will\s+be\s+)?(?:required|needed|necessary)\b", t[m.end():], re.I):
            continue
        if _RELEASE_CTX_RE.search(t) and not _WEEK_SCOPE_RE.search(t):
            continue
        return m
    return None


# ---------------------------------------------------------------------------
# Small text helpers
# ---------------------------------------------------------------------------


def _clause_before(text: str, pos: int, bounds: str = _CLAUSE_BOUND) -> str:
    """The text from the nearest clause boundary up to ``pos``.

    Scoping every subject test to a clause is what lets one message hold a past
    window and an upcoming one, or a promise and a report, without the two
    contaminating each other.

    A dot between two digits is skipped for the same reason the regex gaps skip
    it: in "上周维护 2026.09.23 10:00-12:00" the dotted date would otherwise cut
    the clause short and hide the 上周 that marks the window as last week's.
    """
    i = pos - 1
    while i >= 0:
        ch = text[i]
        if ch in bounds and not (ch in ".．" and 0 < i < len(text) - 1
                                 and text[i - 1].isdigit()
                                 and text[i + 1].isdigit()):
            break
        i -= 1
    return text[i + 1:pos]


def _sentence_of(text: str, pos: int) -> str:
    """The text of the sentence ``pos`` falls in, by _sentence_index's own rule.

    Used to ask whether the sentence carrying a window is about maintenance at
    all — a completion notice must not be rescued by an unrelated time range
    somewhere else in the bubble.
    """
    want = _sentence_index(text, pos)
    out, n = [], 0
    i = 0
    while i < len(text):
        ch = text[i]
        if n == want:
            out.append(ch)
        if ch in _SENT_BOUND and not (
                ch in ".．" and i and text[i - 1].isdigit()
                and i + 1 < len(text) and text[i + 1].isdigit()):
            n += 1
            if n > want:
                break
        i += 1
    return "".join(out)


def _sentence_index(text: str, pos: int) -> int:
    """Which sentence ``pos`` falls in. A dot between digits is not a full stop.

    Without that exception "2026.09.23" would end a sentence three times over
    and the completion scoping below would read a one-line notice as four.
    """
    n = 0
    for i in range(min(pos, len(text))):
        ch = text[i]
        if ch not in _SENT_BOUND:
            continue
        if ch in ".．" and i and text[i - 1].isdigit() \
                and i + 1 < len(text) and text[i + 1].isdigit():
            continue
        n += 1
    return n


def _lines(text: str):
    start = 0
    for line in text.splitlines(keepends=True):
        yield start, start + len(line.rstrip("\n"))
        start += len(line)


def _future_framed(text: str, pos: int) -> bool:
    return bool(_FUTURE_FRAME_RE.search(
        _clause_before(text, pos, _SENT_BOUND)))


# F30: the object noun LEFT of the maintenance counts only when it is the
# verb's real subject, and the maintenance a mere time reference for it -
# "All ongoing rounds BEFORE the maintenance will be cancelled", "注单在维护期间
# 将取消". The old test vetoed on ANY object noun earlier in the sentence, so a
# noun that NAMES the maintenance ("Withdrawal system maintenance ... has been
# cancelled", 游戏维护已取消, 交易系统维护已取消), a heading glued to the body
# (【游戏维护通知】原定于…的维护已取消) or boilerplate (为提升游戏体验，…的维护已取消)
# disarmed a real cancellation and the withdrawn window was written as live.
# The noun now has to be joined to the maintenance by a preposition.
_LEFT_OBJ_EN_RE = re.compile(
    # R1.59: "Rounds interrupted BY the maintenance … will be cancelled" and
    # "UNFINISHED games during the maintenance … will be cancelled" cancel the
    # rounds and the games, and were read as the maintenance being withdrawn -
    # harmless on a fresh notice, but a live row was blanked with
    # VAWATCH_CLEAR_ENABLED=1. A qualified noun ("unfinished games", "pending
    # requests") is an object whatever the noun, and a bare "by" is a link too.
    r"(?:\b(?:" + _OBJ_EN + r")\b|\b(?:unfinished|pending|ongoing|unsettled"
    r"|incomplete|outstanding|interrupted|open)\s+[\w'’\-]+)"
    r"(?:\s+[\w'’\-]+){0,4}?\s+(?:before|during|after"
    r"|prior\s+to|in|throughout|until|till|at|for|from|through|within"
    r"|ahead\s+of|pending|amid|because\s+of|due\s+to|caused\s+by|affected\s+by"
    r"|by|with)"
    r"\s+(?:(?:the|this|our|its|their|a|an|any|today['’]?s|upcoming|scheduled"
    r"|routine|planned|regular|system|server|platform|weekly|monthly)\s+)*$",
    re.I)
_LEFT_OBJ_ZH_RE = re.compile(
    r"(?:" + _OBJ_ZH_DONE + r")[^，,。；;\n]{0,6}?(?:在|于|於|至|到|直到|截至|趁)"
    r"\s*(?:本次|此次|这次|這次|该|該|是次)?\s*$")
_CJK_RE = re.compile(r"[一-鿿]")
_ANY_MAINT_RE = re.compile(r"\bmaint(?:enance)?\b|维护|維護|维修|維修", re.I)


def _verb_at(m) -> int:
    """Where the branch's verb starts (its "v" group), else the match start."""
    for g in ("v", "v2"):
        if g in m.re.groupindex and m.group(g) is not None:
            return m.start(g)
    return m.start()


_NEG_FUTURE_VERB_RE = re.compile(
    r"(?:no\s+longer|not\s+(?:be\s+)?(?:take\s+place|taking\s+place|proceed|go(?:ing)?"
    r"\s+ahead|happen|occur|held|carried\s+out|conducted|performed)|won['’]t)", re.I)


def _done_spans(text: str):
    """Spans where a completion/cancellation is asserted OF THE MAINTENANCE."""
    out = []
    for rx in _DONE_RES:
        for m in rx.finditer(text):
            if _future_framed(text, m.start()):
                continue
            # The frame in front of the VERB (F47): the clause it opens may
            # be a condition or, for a completion, a future.
            v = _verb_at(m)
            pre = _clause_before(text, v)
            verb = text[v:m.end()]
            if _CJK_RE.search(verb):
                frame = (_VERB_FRAME_ZH_DONE_RE
                         if re.search(r"完成|结束|結束|恢复|恢復", verb)
                         else _VERB_FRAME_ZH_CANCEL_RE)
                if frame.search(pre):
                    continue
            elif ((_FUTURE_FRAME_RE.search(pre) or _VERB_FRAME_EN_RE.search(pre))
                  and not _NEG_FUTURE_VERB_RE.match(verb)):
                # F21: "…will no longer take place" / "…will not go ahead" is a
                # cancellation stated in the future tense - the "will" in front
                # of it is not a promise, and it disarmed the verb.
                continue
            # The subject standing to the LEFT. "All ongoing rounds before the
            # maintenance will be cancelled" and "注单在维护期间将取消" both put
            # the cancelled thing before the word the guard was watching.
            left = _clause_before(text, m.start(), _SENT_BOUND)
            if _LEFT_OBJ_EN_RE.search(left) or _LEFT_OBJ_ZH_RE.search(left):
                continue
            out.append((m.start(), m.end()))
    out = [(a, b) for a, b in out
           if not (re.search(r"\d{1,2}[:：]\d{2}\s*(?:之?前)?\s*(?:完成|结束|結束)$", text[a:b])
                   and not re.search(r"已|了|现已|現已|提前", text[a:b]))]
    if _ANY_MAINT_RE.search(text):
        for rx in _STATUS_DONE_RES:
            for m in rx.finditer(text):
                ls = text.rfind("\n", 0, m.start()) + 1
                le = text.find("\n", m.end())
                line = text[ls:le if le >= 0 else len(text)]
                if _OBJ_EN_RE.search(line) or _OBJ_ZH_RE.search(line):
                    continue
                out.append((m.start(), m.end()))
    out.sort()
    return out


# ---------------------------------------------------------------------------
# Which window a completion / cancellation is about (policy 7)
# ---------------------------------------------------------------------------
#
# A done statement vetoes every window it may be about. It is about a window
# in its own CLAUSE, always. A window elsewhere in the bubble survives only
# when the text says, deterministically, that the two are different
# maintenances:
#   * the done clause quotes its OWN window and that window does not overlap
#     this one ("Maintenance on 09-23 has been cancelled. Next scheduled
#     maintenance: 09-30"; "Live Casino 14:00-16:00 will proceed as planned.
#     The Slots maintenance on 10:00-11:00 has been cancelled");
#   * the done clause is about a PAST maintenance (上次维护已完成, "last week's
#     maintenance has been completed");
#   * or this window comes later and is introduced as the NEXT one (下次例行
#     维护将于, "Next maintenance:").
# The old rule rescued any window in another sentence whose sentence carried
# gate wording - and the window line of a cancellation notice always does
# ("例行维护时间：…", "Scheduled maintenance: …"), so "【取消维护通知】\n例行维护
# 时间：<window>\n本次维护取消。" wrote the cancelled window as live (F24), and
# a cancel headline over the ordinary 维护时间 line did too (F21). It also
# tested only the FIRST window of the chosen union, so a cancelled sibling was
# merged into the row (F25), and a cancelled window that sorted first threw a
# live later one away (F49).
# The marker has to modify the maintenance, not its notice: "上次通知的维护已
# 取消" and "the last maintenance notice has been cancelled" withdraw the very
# window the notice restates.
_DONE_PAST_RE = re.compile(
    r"(?:上次|上周|上週|上一次|上回|前次|上个|上個|昨天|昨日|昨晚|前天)(?:的)?"
    r"(?:例行|定期|系统|系統|服务器|伺服器|游戏|遊戲|平台)?(?:维护|維護)"
    r"(?!通知|公告|消息|信息|通告)"
    r"|\b(?:last\s+(?:week|night|month)['’]?s?|last|previous"
    r"|yesterday['’]?s?)\s+(?:(?:scheduled|routine|regular|planned|system"
    r"|server|weekly|monthly)\s+)?maintenance\b"
    r"(?!\s+(?:notice|notification|announcement|message|post|reminder))",
    re.I)
# "next" has to modify the maintenance itself: "The maintenance scheduled for
# next Wednesday" is the cancelled one. "upcoming" and "following" are left
# out on purpose - "Maintenance cancelled. Upcoming maintenance: <window>" and
# "The following maintenance has been cancelled:" can both mean the very
# window being withdrawn.
_NEXT_M_RE = re.compile(
    r"(?:下次|下一次|下周|下週|下个|下個|下回)(?:的)?[一-鿿]{0,4}?(?:维护|維護)"
    r"|\bnext\s+(?:(?:week|month)['’]?s?\s+)?(?:(?:scheduled|routine|regular"
    r"|planned|system|server|weekly|monthly)\s+)*maintenance(?![\-\w])",
    re.I)


def _is_clause_bound(text: str, i: int) -> bool:
    ch = text[i]
    return ch in _CLAUSE_BOUND and not (
        ch in ".．" and 0 < i < len(text) - 1
        and text[i - 1].isdigit() and text[i + 1].isdigit())


def _clause_span(text: str, a: int, b: int):
    """(start, end) of the clause(s) holding text[a:b]."""
    s = a
    while s > 0 and not _is_clause_bound(text, s - 1):
        s -= 1
    e = b
    while e < len(text) and not _is_clause_bound(text, e):
        e += 1
    return s, e


def _done_scopes(text: str, done):
    """The clause-extended region each done span governs, in ``done`` order."""
    return [_clause_span(text, a, b) for a, b in done]


def _in_scopes(cand, scopes) -> bool:
    a = cand["span"][0]
    return any(s <= a < e for s, e in scopes)


def _next_hit(t: str, at):
    """G1.6: "Next maintenance: <window>" / "Next week's maintenance will be on
    <window>" / 下次维护：<window>, in the window's own sentence and in front of
    it - the wording the done+next notice uses for its announcement half. The
    wide gate does not open on it, so "Last maintenance has been completed.
    Next maintenance: 25/09/2026 14:00-16:00" was dropped whole. Plain-wording
    rules apply: off under NOTICE_WORDING=strict, and never when negated.
    """
    if at is None or _flag("NOTICE_WORDING", "wide") == "strict":
        return None
    s, _e = _sentence_span(t, at, at)
    for m in _NEXT_M_RE.finditer(t, s, at):
        if _on("NOTICE_NEGATION_GUARD") and _negated(t, m):
            continue
        return m
    # F48: "Previous maintenance done, next one: <X>" - only after the
    # maintenance has been named in the message.
    for m in re.finditer(r"(?i)\bnext\s+(?:one|window|slot)\b", t[s:at]):
        if _TOPIC_STRONG_RE.search(t, 0, s + m.start()):
            return m
    # G1.6: "Upcoming maintenance: <X>" after a completion in the same bubble.
    if _COMPLETION_WORD_RE.search(t, 0, s):
        for m in _UPCOMING_M_RE.finditer(t, s, at):
            if _on("NOTICE_NEGATION_GUARD") and _negated(t, m):
                continue
            return m
    return None


def _done_excused(t: str, done, scopes, gone, detail, win_past: bool) -> bool:
    """May the window ``detail`` stand despite every done span? See above."""
    if detail["superseded"] or win_past:
        return False
    at = detail["at"]
    ws, we = detail["start"], detail["end"]
    s0, _e0 = _sentence_span(t, at, at)
    for (_a, b), (ss, se) in zip(done, scopes):
        own = [c for c in gone if ss <= c["span"][0] < se]
        if own:
            if any(c["start"] < we and ws < c["end"] for c in own):
                return False
            continue
        seg = t[ss:se]
        if ((_ANY_RANGE_RE.search(seg) or _DATE_TOKEN_RE.search(seg))
                and _names_other_window(seg, ws, we, ws)) or _names_other_day(seg, ws, we):
            # G2.2 / F25: the done clause names a window of its OWN that no
            # candidate carries ("Previously announced 24/09/2026 10:00-12:00 is
            # cancelled", "Note: <day> 14:00 - 16:00 cancelled", "The 24/09
            # maintenance is cancelled") - a different maintenance from this one.
            continue
        if _DONE_PAST_RE.search(t[ss:se]):
            continue
        # The NEXT marker only tells two maintenances apart when the cancelled
        # clause is not itself about the next one, and when the window is not
        # narrated in the past tense. "The next maintenance has been
        # cancelled, the next maintenance window was 2026-09-30 10:00 - 12:00"
        # restates the withdrawn window, and the rescue wrote it live (R1.22).
        if (b <= at and not _NEXT_M_RE.search(t[ss:se])
                and _NEXT_M_RE.search(t[max(s0, b):at])
                and not _WAS_RE.search(t[max(s0, b):at])):
            continue
        # G1.6: after a COMPLETION, "Upcoming maintenance: <X>" is the next one
        # - determinable, unlike after a cancellation, where "upcoming" may be
        # the cancelled one itself (which is why _NEXT_M_RE leaves it out).
        if (b <= at and _COMPLETION_WORD_RE.search(t[_a:b])
                and not _UPCOMING_M_RE.search(t[ss:se])
                and _UPCOMING_M_RE.search(t[max(s0, b):at])
                and not _WAS_RE.search(t[max(s0, b):at])):
            continue
        return False
    return True


_COMPLETION_WORD_RE = re.compile(
    r"complet|finish|ended|conclud|\bdone\b|\bover\b|完成|结束|結束", re.I)
_UPCOMING_M_RE = re.compile(
    r"\b(?:upcoming|following|subsequent)\s+(?:(?:scheduled|routine|regular|planned|system"
    r"|server|weekly|monthly)\s+)*maintenance(?![\-\w])|接下来的?(?:维护|維護)"
    r"|接下來的?(?:维护|維護)", re.I)


# A past-tense copula between the NEXT marker and the window ("the next
# maintenance window was <window>", 原定/原本) narrates the withdrawn window,
# not a new one.
_WAS_RE = re.compile(r"\b(?:was|were|had\s+been)\b|原定|原本|原计划|原計劃", re.I)


# ---------------------------------------------------------------------------
# Extended maintenance (F50)
# ---------------------------------------------------------------------------
#
# "The scheduled maintenance on 2026-09-23 10:00 - 12:00 (GMT+8) is extended
# until 14:00" restates the ORIGINAL window and names the new end as a bare
# clock. Nothing handled the word: the notice re-wrote 10:00-12:00 over a
# row a person may already have corrected to 14:00, carded "End 12:00", and
# when it arrived after 12:00 it was dropped as stale. "维护延长至14:00" and the
# other windowless forms were dropped as "not maintenance". The new end is
# now built on the original window's date (the next day when the clock is
# before the start: an overnight window) and written as a reschedule; with
# no original window, or an end that does not lengthen it by at most
# _EXTEND_MAX, a person corrects End Time instead. A full new range in the
# same sentence ("extended from 10:00 - 12:00 to 10:00 - 14:00") is left to
# the ordinary rules, which already read it.
_EXTEND_RES = [
    # The bare participle ("Maintenance extended until 14:00") may not follow
    # be/been: "the maintenance may be extended if needed" is a possibility.
    re.compile(r"maintenance" + _GAP_EN_DONE + r"{0,80}?\b(?P<v>"
               r"(?:(?:has|have)\s+(?:now\s+)?been|is|are|was|were"
               r"|will\s+(?:now\s+)?be|being)\s+(?:further\s+)?extended"
               r"|(?<!\bbe\s)(?<!\bbeen\s)extended)\b", re.I),
    re.compile(r"\b(?P<v>extend(?:ing|ed)?)\s+(?:the\s+|our\s+|this\s+"
               r"|today['’]s\s+)?(?:(?:scheduled|routine|planned|regular|system"
               r"|server)\s+)*maintenance\b", re.I),
    re.compile(r"\bmaintenance\s+(?P<v>extension)\b|\b(?P<v2>extension)\s+of\s+"
               r"(?:the\s+|our\s+|this\s+)?(?:(?:scheduled|routine|planned"
               r"|regular|system|server)\s+)*maintenance\b", re.I),
    re.compile(r"(?:维护|維護|维修|維修)" + _GAP_ZH_DONE_CLAUSE
               + r"{0,40}?(?P<v>延长|延長|延时|延時)"),
    re.compile(r"(?P<v>延长|延長)(?:维护|維護)"),
]
# A possibility or a condition is not an extension: "if the maintenance is
# extended we will inform you", "维护可能延长", "如维护延长将另行通知".
_EXTEND_FRAME_RE = re.compile(
    r"\b(?:if|unless|whether|in\s+case|should|may|might|could|possibly"
    r"|probably)\b|如果|假如|倘若|若|(?<![例比])如(?![下此期同今])|可能|或将|或將"
    r"|是否|一旦|万一|萬一", re.I)
# Only these words may stand between the verb and its new end, so a clock
# later in the sentence ("... extended; the new end time will be announced at
# 13:00") is never mistaken for it.
_EXTEND_LEAD_RE = re.compile(
    r"\s*(?:(?:until|till|to|through|by|for|another|an?\s+additional|additional"
    r"|approximately|approx\.?|about|around|roughly|further|at\s+least|up\s+to"
    r"|an?\s+extra|extra|the\s+end\s+time\s+(?:to|until)|至|到|为|為|约|約|大约"
    r"|大約|了|再|延长|延長)\s*)*", re.I)
_EXTEND_CLOCK_RE = re.compile(
    r"(?:(?P<z>" + _ZH_MER + r")\s*)?(?P<h>\d{1,2})\s*(?:[:：.]\s*(?P<m>\d{2})"
    r"|[点點时時]\s*(?:(?P<zm>\d{1,2})\s*分?|(?P<half>半))?)(?!\d)"
    r"\s*(?P<a>" + _AMPM_WORD + r")?"
    r"|(?P<h2>\d{1,2})\s*(?P<a2>[AaPp]\.?[Mm]\.?)(?![A-Za-z])")
_EXTEND_DURATION_RE = re.compile(
    r"(?P<n>\d+(?:\.\d+)?|an?|one|two|three|half\s+an?)\s*(?:more\s+"
    r"|additional\s+|extra\s+)?(?P<u>hours?|hrs?|minutes?|mins?)\b"
    r"|(?P<zn>\d+(?:\.\d+)?|半|一|两|兩|二|三)\s*(?:个|個)?\s*"
    r"(?P<zu>小时|小時|钟头|鐘頭|分钟|分鐘)", re.I)
_EXTEND_NUM = {"a": 1, "an": 1, "one": 1, "two": 2, "three": 3, "半": 0.5,
               "一": 1, "两": 2, "兩": 2, "二": 2, "三": 3}
# An extension longer than this is not an overrun - it is a new outage, and
# a person reads it.
_EXTEND_MAX = timedelta(hours=12)


def _extension_target(tail: str):
    """What ``tail`` (the text after an extension verb) extends TO.

    -> ("clock", h, m, meridiem, zone_minutes_or_None) | ("dur", timedelta)
    | None.
    """
    lead = _EXTEND_LEAD_RE.match(tail)
    rest = tail[lead.end():]
    if _RANGE_RE.match(rest):
        # "延长至 10:00-14:00": the new window itself, which the ordinary
        # rules read - not a bare end clock.
        return ("range",)
    m = _EXTEND_DURATION_RE.match(rest)
    if m:
        if m.group("n") is not None:
            n = m.group("n").lower()
            q = 0.5 if n.startswith("half") else _EXTEND_NUM.get(n) or float(n)
            unit = m.group("u").lower()
        else:
            q = _EXTEND_NUM.get(m.group("zn")) or float(m.group("zn"))
            unit = m.group("zu")
        mins = q * (60 if unit[0] in "h小钟鐘" else 1)
        return ("dur", timedelta(minutes=mins))
    m = _EXTEND_CLOCK_RE.match(rest)
    if not m:
        return None
    if m.group("h2") is not None:
        h, mi, mer = int(m.group("h2")), 0, m.group("a2")
    else:
        h = int(m.group("h"))
        mi = int(m.group("m") or m.group("zm") or (30 if m.group("half") else 0))
        mer = m.group("a") or m.group("z")
    after = rest[m.end():]
    z = re.match(r"\s*[(（]?\s*", after)
    zm = _TZ_RE.match(after, z.end())
    return ("clock", h, mi, mer, _offset_of(zm) if zm else None)


def _extension(t: str, detail, cands, now: datetime):
    """-> None when ``t`` extends no maintenance (or states the extended
    window in full, for the ordinary rules), else the verdict dict."""
    hits = []
    for rx in _EXTEND_RES:
        for m in rx.finditer(t):
            if not _EXTEND_FRAME_RE.search(_clause_before(t, _verb_at(m))):
                hits.append(m)
    if not hits:
        return None
    targets = []
    for m in hits:
        _s, e = _sentence_span(t, m.end(), m.end())
        tail = t[m.end():e]
        tg = _extension_target(tail)
        if tg is None or tg[0] == "range":
            continue
        # One end stated twice, once with its zone ("until 14:00" ... "until
        # 14:00 GMT+8"), is one end.
        same = next((x for x in targets if x[:4] == tg[:4] and (
            x[0] == "dur" or None in (x[4], tg[4]) or x[4] == tg[4])), None)
        if same is None:
            targets.append(tg)
        elif tg[0] == "clock" and same[4] is None and tg[4] is not None:
            targets[targets.index(same)] = tg
    if not targets and len({(c["start"], c["end"]) for c in cands}) > 1:
        # No bare end, but the old AND the new window are both stated
        # ("extended from 10:00 - 12:00 to 10:00 - 14:00", "... will be
        # extended. New window: 10:00 - 14:00"): the ordinary rules read the
        # new one. With a single window it is the original restated or the
        # new one - the text does not say - so a person decides.
        return None
    said = "" if len(targets) != 1 else (
        " by {:g} min".format(targets[0][1].total_seconds() / 60)
        if targets[0][0] == "dur" else " to {}:{:02d}{}".format(
            targets[0][1], targets[0][2],
            " " + targets[0][3] if targets[0][3] else ""))

    def human(why):
        return {"action": "needs_human", "reschedule": True,
                "reason": "maintenance extended{} - {}; correct End Time by "
                          "hand".format(said, why)}

    if detail is None:
        return human("the original window could not be read")
    if len(targets) > 1:
        return human("the notice states {} different new end times".format(
            len(targets)))
    if not targets:
        return human("the new end time could not be read")
    if detail["ambiguous"]:
        return human("the original window is ambiguous: " + detail["ambiguous"])
    if detail["others"]:
        return human("the notice states {} windows and does not say which "
                     "one is extended".format(len(detail["others"]) + 1))
    ws, we = detail["start"], detail["end"]
    tg = targets[0]
    if tg[0] == "dur":
        new_end = we + tg[1]
    else:
        tz = ws.tzinfo
        if tg[4] is not None and timedelta(minutes=tg[4]) != ws.utcoffset():
            return human("the new end is stated in a different time zone")
        try:
            day, hh, mm = _clock(tg[1], tg[2], tg[3], end=True)
        except ValueError:
            return human("the new end time is not a clock time")
        base = we.astimezone(tz)
        new_end = (datetime(base.year, base.month, base.day, hh, mm, tzinfo=tz)
                   + timedelta(days=day))
        if new_end <= ws:
            new_end += timedelta(days=1)
    if new_end < we or new_end - we > _EXTEND_MAX:
        return human("the new end {} does not extend {} by at most {:g}h".format(
            "{:%Y-%m-%d %H:%M}".format(new_end), _fmt_window(ws, we),
            _EXTEND_MAX.total_seconds() / 3600))
    veto = _veto(t, detail["at"], hits[0])
    if veto:
        return {"action": veto[0], "reason": veto[1], "refused": veto[2]}
    reason = "maintenance extended: {} becomes {} (overrides the scheduled end)".format(
        _fmt_window(ws, we), _fmt_window(ws, new_end))
    if not _on("NOTICE_EXTENSION_FILL"):
        out = human("NOTICE_EXTENSION_FILL=0")
        out["reason"] = reason + " - not written (NOTICE_EXTENSION_FILL=0); " \
                                 "correct End Time by hand"
        return out
    return {"action": "fill", "reschedule": True, "reason": reason,
            "start": ws, "end": new_end, "stale": is_stale(new_end, now=now)}


# ---------------------------------------------------------------------------
# Window candidates and selection
# ---------------------------------------------------------------------------


def _superseded_regions(text: str):
    """Char spans whose windows an Updated block has superseded.

    An "Updated Schedule" block is authoritative and EXCLUSIVE: everything
    before the LAST updated marker is the old plan (these notices head the whole
    section and then list "Original: ..." underneath, so anchoring on the FIRST
    marker swallowed the superseded window all over again), and an Original
    label inside the block opens a region that runs to the END - the superseded
    window is often on the line BELOW its label, so dropping only the labelled
    line left it in scope.
    """
    # F53: a marker that only promises a schedule ("an updated schedule will
    # be shared", 新时间另行通知) heads no block and supersedes nothing.
    marks = [m for m in _UPDATED_RE.finditer(text)
             if not _promised_marker(text, m)]
    if not marks:
        return []
    cut = marks[-1].start()
    regions = [(0, cut)] if cut else []
    tail = text[cut:]
    for ls, le in _lines(tail):
        line = tail[ls:le]
        if ORIGINAL_RE.search(line) or _ORIGINAL_LABEL_RE.match(line):
            regions.append((cut + ls, len(text)))
            break
    return regions


_LABEL_ONLY_LINE_RE = re.compile(r"[^\d\n]{1,24}[:：]\s*")


def _lead_start(text: str, pos: int) -> int:
    """Where the lead-in of the token at ``pos`` begins: its sentence start,
    or the start of a bare label line directly above it.

    "原订时间：\\n2026-09-23 10:00-12:00" puts the label on its own line, and a
    lead-in that stopped at the newline lost it - but only a LABEL line
    (short, no digits, ends in a colon) may be crossed, never a sentence.
    """
    lo = pos - len(_clause_before(text, pos, _SENT_BOUND))
    if lo > 0 and text[lo - 1] == "\n" and not text[lo:pos].strip():
        ls = text.rfind("\n", 0, lo - 1) + 1
        if _LABEL_ONLY_LINE_RE.fullmatch(text[ls:lo - 1]):
            return ls
    return lo


def _superseded_dates(text: str, dates):
    """Indices of dates a "moved from" / 原定 marker introduces as the OLD one.

    The lookback stops at the previous date so that, in "moved from 2026-09-23
    to 2026-09-25", the marker taints the 23rd and not the 25th - writing the
    superseded window is exactly the failure this guard exists to prevent.

    F27: it also stops at the start of the date's own SENTENCE (a newline or a
    full stop). "原定的周年庆活动延期。系统维护时间：09-23 …。新活动时间：09-26 …"
    lent the promo's 原定 across the 。 to the maintenance date, and the row
    got the promo window; "感谢您之前的支持。例行维护：…" did the same.
    """
    sup = set()
    prev_end = 0
    for i, (pos, end, _ymd) in enumerate(dates):
        lo = max(prev_end, pos - 28, _lead_start(text, pos))
        if _SUPERSEDED_CLAUSE_RE.search(text[lo:pos]):
            sup.add(i)
        prev_end = end
    return sup


# ---------------------------------------------------------------------------
# Which window a reschedule MOVES and which one it moves TO (F22, F23, F26)
# ---------------------------------------------------------------------------
#
# The superseded markers above are all words IN FRONT of the old window
# (原定 X, "moved from X", "instead of X"). The commonest reschedule has none:
# "Scheduled maintenance on X has been rescheduled to Y", "X 的维护改期至 Y",
# "X -> Y". X is the SUBJECT of the move verb, and with both windows ahead
# the earliest-first pick wrote the withdrawn X (two days apart) or a 26h
# union of X and Y (same day) - labelled "(rescheduled)" on the card. A
# postponement with no new date ("X has been postponed. A new date will
# follow.") re-asserted X the same way.
#
# _mark_moved runs only when classify() has already decided the message is a
# reschedule or a correction, and reads the MASKED text (dates, ranges and
# zones blanked), so "(GMT+8) has been rescheduled to" is seen as words.
# For each window, in order:
#   1. its lead-in (from the previous window, within its sentence or a label
#      line straight above) - the NEAREST marker decides: an old-marker
#      (_SUPERSEDED_CLAUSE_RE, a bare from/由/从 or 原 right in front of its
#      date) makes it old, a direction marker (to/至/改为/现/new/变更后/
#      正确/"will now") makes it the target;
#   2. otherwise its tail (up to the next window, within its sentence): a
#      move or cancel verb whose subject it is ("has been postponed", "的维护
#      改期至", "postponed until", "will now take place on"), or a bare
#      arrow/"to" joining it to the next window, makes it old.
# Only ever marks a window OLD. If that leaves no live window, classify()
# asks a human ("the only window found is the superseded one").
_NEW_LEAD_RE = re.compile(
    r"\b(?:to|until|till|into|now|new|updated|revised|correct(?:ed|ion)?)\b"
    r"|->|→|=>|⇒|改为|改為|改至|改到|改在|延至|至|到|现|現"
    r"|新的?(?:时间|時間|日期|安排)|更新[后後]|(?:变更|變更|调整|調整|修改)[后後]"
    r"|正确|正確|\bafter\s*[:：]|\bwill\s+now\b", re.I)
_OLD_LEAD_TAIL_RE = re.compile(
    r"(?:\bfrom|由|从|從)\s*$|(?<![^\W\d_])原\s*[:：]?\s*$|\bwas\s*[:：]?\s*$",
    re.I)
_MOVE_TAIL_EN = (r"re-?scheduled|postponed|moved|shifted|changed|deferred|delayed"
                 r"|pushed(?:\s+back)?|brought\s+forward")
_MOVED_TAIL_RE = re.compile(
    r"\b(?:has|have|had|is|are|was|were|will|shall|would|being|been|be|gets?"
    r"|got)\s+(?:(?:been|be|being|now|also|hereby|further|officially)\s+)*"
    r"(?:" + _MOVE_TAIL_EN + r"|cancel(?:l)?ed|called\s+off|put\s+on\s+hold"
    r"|withdrawn|replaced|(?:adjusted|updated|revised|amended)(?=\s+to\b))\b"
    r"(?!\s+from\b)"
    r"|(?<![(\[【（])\b(?:" + _MOVE_TAIL_EN + r")\s+(?:to|until|till|into"
    r"|forward|back|by|indefinitely)\b"
    r"|\bwill\s+now\s+(?:take\s+place|be\s+(?:held|carried\s+out|performed"
    r"|conducted|done))\b"
    # R1.57/F23: "<window> — delayed, new date TBA" / "<window> (postponed)":
    # the participle alone, closing its clause, makes the window it follows
    # the withdrawn one. Before this the only window was written as the NEW one.
    # R1.41: "10:00-12:00 was wrong" in a correction withdraws that window.
    r"|\b(?:was|is|were)\s+(?:wrong|incorrect|a\s+typo|a\s+mistake|in\s+error)\b"
    r"|有误|有誤"
    r"|(?<![A-Za-z])(?:postponed|delayed|deferred|put\s+off|on\s+hold)"
    r"(?=\s*(?:[,，.;；!。)）\]】\n]|$|until\s+further|indefinitely))"
    r"|(?:作废|作廢)"
    r"|(?:改期|延期|延后|延後|顺延|順延|推迟|推遲|推后|推後|暂缓|暫緩|取消|挪到|挪至"
    r"|移至|移到|改至|改到|改为|改為|改在|延至|提前至|提前到"
    r"|(?:调整|調整|变更|變更)(?:为|為|至|到)"
    r"|(?:延迟|延遲)(?=至|到|进行|進行|[，,。\s]|$))(?![后後])", re.I)
_BARE_JOIN = {"->", "→", "=>", "⇒", "to", "至", "到"}
_TAIL_GLUE_RE = re.compile(
    r"[\s\x01()（）\[\]【】,，:：\-–—]*"
    r"(?:(?:the|this|our)\s+)?(?:(?:scheduled|planned|routine)\s+)?"
    r"(?:(?:maintenance|downtime|window|slot|schedule|session)\s*)?"
    r"(?:的|之)?(?:例行|系统|系統|定期|排定|本次|原定)?(?:维护|維護|维修|維修)?"
    r"(?:时间|時間|工作|安排)?(?:将|將|已|已经|已經|会|會|需|现|現|则|則)?\s*", re.I)


_PREV_STATED_RE = re.compile(
    r"\b(?:previously|earlier|originally|wrongly|incorrectly|mistakenly)\s+"
    r"(?:stated|announced|notified|mentioned|posted|sent|given|written|shared"
    r"|informed|communicated)\b"
    r"|\bstated\s+(?:previously|earlier|before)\b"
    r"|(?:之前|此前|先前|早前|前面)(?:所)?(?:通知|公告|告知)的?|原通知的?",
    re.I)


def _lead_role(lead: str, lead_is_date: bool):
    """'old' | 'new' | None from a window's lead-in: the nearest marker wins."""
    if not lead_is_date:
        # "on 2026-09-25 from 10:00 to 12:00": a from in front of a CLOCK is
        # the range's own, not a move.
        lead = re.sub(r"(?i)\bfrom\s*$", "", lead)
    old = max((m.end() for m in _SUPERSEDED_CLAUSE_RE.finditer(lead)),
              default=-1)
    # R1.39/R1.42: _lead_role only runs for a reschedule or a correction, and
    # there "previously stated X" / "之前通知的X" names the window being
    # corrected. Outside one it is a confirmation (F54 "the maintenance
    # previously announced for X will proceed"), which is why
    # _SUPERSEDED_CLAUSE_RE itself must not carry it.
    old = max([old] + [m.end() for m in _PREV_STATED_RE.finditer(lead)])
    if _OLD_LEAD_TAIL_RE.search(lead):
        old = len(lead)
    new = max((m.end() for m in _NEW_LEAD_RE.finditer(lead)), default=-1)
    if old < 0 and new < 0:
        return None
    return "old" if old >= new else "new"


def _tail_role(tail: str, reaches: bool):
    """'old' when the window's tail makes it the subject of a move."""
    cut = next((i for i, ch in enumerate(tail) if ch in _SENT_BOUND), None)
    if cut is not None:
        tail, reaches = tail[:cut], False
    if reaches and tail.strip(" \t\x01()（）[]【】").lower() in _BARE_JOIN:
        return "old"
    m = _MOVED_TAIL_RE.search(tail)
    if not m:
        return None
    # Only glue may stand between the window and its verb - the zone, a
    # bracket, "the maintenance", 的维护/已/将. "…10:00-12:00 (GMT+8), the
    # server will be moved to a new data centre" moves the SERVER, and a
    # date in front of the verb ("（已由原定9月23日改期）") makes it that
    # date's verb.
    return "old" if _TAIL_GLUE_RE.fullmatch(tail[:m.start()]) else None


def _mark_moved(text: str, masked: str, cands, dates):
    """Flag superseded the window(s) a reschedule moves away from. In place."""
    by_span = {}
    for c in cands:
        by_span.setdefault(c["span"], []).append(c)
    info, prev_end = [], 0
    for sp in sorted(by_span):
        di = by_span[sp][0]["di"]
        dpos = dates[di][0] if di is not None else None
        lead = dpos if dpos is not None and prev_end <= dpos < sp[0] else sp[0]
        info.append((sp, lead, lead == dpos))
        prev_end = sp[1]
    for k, (sp, lead, lead_is_date) in enumerate(info):
        lo = max(info[k - 1][0][1] if k else 0, _lead_start(text, lead))
        role = _lead_role(masked[lo:lead], lead_is_date)
        if role is None:
            reaches = k + 1 < len(info)
            nxt = info[k + 1][1] if reaches else len(masked)
            role = _tail_role(masked[sp[1]:nxt], reaches)
        if role == "old":
            for c in by_span[sp]:
                c["superseded"] = True


# ---------------------------------------------------------------------------
# Date spans: "24/09/2026 - 25/09/2026", "9月24日至9月25日", "23-24 Sep 2026"
# ---------------------------------------------------------------------------
#
# An overnight notice very often gives BOTH days and then one clock range:
# "Date: 24/09/2026 - 25/09/2026 / Time: 22:00 - 02:00". The range was bound
# to the NEAREST preceding date, which is the span's second half, so the whole
# window went onto the sheet 24h late (25 22:00 -> 26 02:00) - never stale,
# carded as "Base row updated", and marked filled so nothing corrected it. A
# range bound to either half of a span is now built on the span's FIRST day,
# and it must END on the span's last day: 22:00-02:00 across 24-25 does, a
# same-day 10:00-12:00 across 24-25 does not (every day of the span? one long
# window? the text does not say), and neither does one night across 24-26.
# Those go to a person.

_SPAN_CONN = (r"(?:-{1,2}|~|至|到|\bto\b|\buntil\b|\btill\b|\bthrough\b"
              r"|\bthru\b)")
# What may stand between the two dates of a span: the first date's weekday
# and the connector, nothing else - "2026-09-24 (Thu) ~ 2026-09-25 (Fri)".
_SPAN_JOIN_RE = re.compile(
    # F1: "Thu 24 Sep - Fri 25 Sep 2026" / "Thursday, 24 September - Friday,
    # 25 September 2026" - the weekday of the SECOND half stands unbracketed
    # after the connector, which broke the span and bound the range to the
    # 25th: the whole overnight window a day late.
    r"[ \t]*(?:[\(（][^)）\n]{1,12}[\)）][ \t]*)?" + _SPAN_CONN + r"[ \t]*"
    r"(?:(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*\.?,?[ \t]*)?", re.I)
# A date LIST is the same trap with a different connector: "on 2026-09-24 and
# 2026-09-25, 10:00 - 12:00" bound its one range to the nearest date, the 25th,
# and the 24th's outage - the imminent one - never reached the row. A list
# states one window PER DAY: each becomes a candidate, the row takes the
# earliest still ahead and the rest are named in ``others``. The words are
# explicit list words only; a bare comma counts solely between two WHOLE
# dates, because "Phase 2, 24 Sep 2026" is not the 2nd and the 24th.
_LIST_WORD = r"(?:&|＆|、|\band\b|及|和|与|與)"
_LIST_JOIN_RE = re.compile(
    r"[ \t]*(?:[\(（][^)）\n]{1,12}[\)）][ \t]*)?(?:[,，][ \t]*" + _LIST_WORD
    + r"?|" + _LIST_WORD + r")[ \t]*", re.I)
# The connector of a shorthand span or list, captured so _date_spans can tell
# the two apart ("24 - 25 Sep" one span, "24 & 25 Sep" two days).
_ANY_CONN = "(?P<conn>" + _SPAN_CONN + "|" + _LIST_WORD + ")"
# The shorthands, where only ONE half is a whole date:
#   "Sep 24 - 25, 2026" / "September 24th - 25th"   (month-first, day after)
_SPAN_TAIL_DAY_RE = re.compile(
    r"[ \t]*(?:[\(（][^)）\n]{1,12}[\)）][ \t]*)?" + _ANY_CONN
    + r"[ \t]*(\d{1,2})" + _ORD + r"(?![\d:./月A-Za-z])(?:[ \t]*,?[ \t]*(\d{4})(?!\d))?",
    re.I)
#   "9月24日-25日" / "2026年9月24日至25日"
_SPAN_TAIL_ZH_RE = re.compile(
    r"[ \t]*(?:[\(（][^)）\n]{1,12}[\)）][ \t]*)?" + _ANY_CONN
    + r"[ \t]*(\d{1,2})\s*[日号號]", re.I)
#   "24/09 - 25/09"
_SPAN_TAIL_BARE_RE = re.compile(
    r"[ \t]*(?:[\(（][^)）\n]{1,12}[\)）][ \t]*)?" + _ANY_CONN
    + r"[ \t]*(\d{1,2})/(\d{1,2})(?![\d/])", re.I)
#   "23-24 Sep 2026" / "24th - 25th September 2026"  (day-first, day BEFORE)
_SPAN_HEAD_DAY_RE = re.compile(
    r"(?<![\d:./\-])(\d{1,2})" + _ORD + r"[ \t]*" + _ANY_CONN + r"[ \t]*$", re.I)
#   "24-25/09/2026" - the numeric twin, TIGHT only (no space, no list word):
#   "Round 3 - 24/09/2026" is a heading and a date, not the 3rd to the 24th.
#   _dates reads its "25/09/2026" half through _DAYSPAN_NUM_RE.
_SPAN_HEAD_NUM_RE = re.compile(r"(?<![\d:./\-])(\d{1,2})(?P<conn>-|~|至)$")
_MONTH_FIRST_YEARLESS_RE = re.compile(
    _MONTH_WORD + r"\.?\s*\d{1,2}" + _ORD, re.I)
_DAY_FIRST_MONTH_RE = re.compile(r"\d{1,2}" + _ORD + r"\b.*" + _MONTH_WORD,
                                 re.I | re.S)
# "rescheduled from 2026-09-23 to 2026-09-25" is a MOVE, not a span: the first
# date is the withdrawn one.
_MOVED_FROM_RE = re.compile(r"(?:\bfrom|由|从|從)\s*$", re.I)
_MOVE_VERB_RE = re.compile(
    r"\b(?:moved|shifted|changed|delayed|deferred|pushed|brought|resched\w*"
    r"|postpone\w*)\b|改期|延期|延后|延後|顺延|順延|调整|調整|变更|變更|改至|延至",
    re.I)


def _date_spans(text: str, dates, sup):
    """-> {date index: span} for every date that is one half of a date span.

    A span is {"lo": (y, m, d), "hi": (y, m, d), "at": (start, end),
    "members": [date indices]}; both halves of a two-date span map to the
    same dict. ``sup`` (_superseded_dates) excludes a moved-from date.

    A date LIST ("2026-09-24 and 2026-09-25", "24 & 25 Sep", "9月24日、25日")
    comes back in the same shape with "kind": "list" and "days": every day
    it names, in order - _collect builds one window per day from it.
    """
    out = {}

    def _ok(ymd):
        try:
            date(*ymd)
            return True
        except (ValueError, TypeError):
            return False

    def _moved(a, b):
        if not _MOVED_FROM_RE.search(text, max(0, a - 8), a):
            return False
        s, e = _sentence_span(text, a, b)
        return bool(_MOVE_VERB_RE.search(text, s, e) or RESCHEDULE_RE.search(text, s, e))

    for i, (p, e, ymd) in enumerate(dates):
        if i in out or i in sup:
            continue
        sp = None
        nxt = dates[i + 1] if i + 1 < len(dates) else None
        if nxt is not None and _SPAN_JOIN_RE.fullmatch(text, e, nxt[0]):
            if i + 1 not in sup:
                sp = {"lo": ymd, "hi": nxt[2], "at": (p, nxt[1]),
                      "members": [i, i + 1]}
        elif nxt is not None and _LIST_JOIN_RE.fullmatch(text, e, nxt[0]):
            # "2026-09-24, 2026-09-25 and 2026-09-26": follow the chain.
            k = i + 1
            while k + 1 < len(dates) and _LIST_JOIN_RE.fullmatch(
                    text, dates[k][1], dates[k + 1][0]):
                k += 1
            members = [j for j in range(i, k + 1) if j not in sup]
            if len(members) > 1:
                sp = {"kind": "list", "lo": dates[members[0]][2],
                      "hi": dates[members[-1]][2],
                      "days": [dates[j][2] for j in members],
                      "at": (p, dates[k][1]), "members": members}
        else:
            y, mo, d = ymd
            tok = text[p:e]
            m = None
            # Group 1 of each tail regex is the connector (_ANY_CONN).
            if _MONTH_FIRST_YEARLESS_RE.fullmatch(tok):
                m = _SPAN_TAIL_DAY_RE.match(text, e)
                if m:
                    yy = int(m.group(3)) if m.group(3) else y
                    sp = {"lo": (yy, mo, d), "hi": (yy, mo, int(m.group(2))),
                          "at": (p, m.end()), "members": [i]}
            if sp is None and tok[-1:] in "日号號":
                m = _SPAN_TAIL_ZH_RE.match(text, e)
                if m:
                    sp = {"lo": ymd, "hi": (y, mo, int(m.group(2))),
                          "at": (p, m.end()), "members": [i]}
            if sp is None and re.fullmatch(r"\d{1,2}/\d{1,2}", tok):
                m = _SPAN_TAIL_BARE_RE.match(text, e)
                if m:
                    c1, c2 = int(m.group(2)), int(m.group(3))
                    day_first = int(tok.split("/")[0]) == d
                    hi = (y,) + ((c2, c1) if day_first else (c1, c2))
                    if _ok(hi) and date(*hi) < date(*ymd):
                        hi = (y + 1,) + hi[1:]
                    sp = {"lo": ymd, "hi": hi, "at": (p, m.end()),
                          "members": [i]}
            if sp is None and _DAY_FIRST_MONTH_RE.match(tok):
                m = _SPAN_HEAD_DAY_RE.search(text, max(0, p - 12), p)
                if m:
                    sp = {"lo": (y, mo, int(m.group(1))), "hi": ymd,
                          "at": (m.start(), e), "members": [i]}
            # ...and only on a date read DAY-first: "3~4/09/2026" under
            # NOTICE_DATE_ORDER=mdy was read as 9 April, and "3~" is a day.
            if (sp is None and _DMY_YEAR_RE.fullmatch(tok)
                    and int(re.split(r"[-/.]", tok)[0]) == d):
                m = _SPAN_HEAD_NUM_RE.search(text, max(0, p - 3), p)
                if m and int(m.group(1)) < d:
                    sp = {"lo": (y, mo, int(m.group(1))), "hi": ymd,
                          "at": (m.start(), e), "members": [i]}
            if sp is not None and not re.fullmatch(
                    _SPAN_CONN, m.group("conn"), re.I):
                # "24 & 25 Sep", "9月24日、25日": two days, not one span.
                # In order or not at all - "Top 3 and 25 Sep" is no list.
                if not (_ok(sp["lo"]) and _ok(sp["hi"])
                        and date(*sp["lo"]) < date(*sp["hi"])):
                    continue
                sp.update(kind="list", days=[sp["lo"], sp["hi"]])
        if sp is None or not (_ok(sp["lo"]) and _ok(sp["hi"])):
            continue
        if _moved(sp["at"][0], sp["at"][1]):
            continue
        for j in sp["members"]:
            out[j] = sp
    return out


# R1.65: _build reads an end that is not after its start as the NEXT day.
# That is right for an overnight window (23:00-01:00, 22:00-06:00) and wrong
# for a slip: "12:00-10:00" became 22 hours, "10:00-10:00" 24, and the common
# "10:00 AM - 12:00 AM" (meaning noon) 14. Only an evening start with an
# early-morning end, 12 hours at most, is taken as overnight without a word;
# an explicit next-day marker (次日, 翌日, "next day", "+1") always is.
_NEXT_DAY_RE = re.compile(r"次日|翌日|隔天|隔日|第二天|\bnext\s+day\b|\(\s*\+1\s*\)", re.I)
_END_OF_DAY_RE = re.compile(r"(?<!\d)24\s*(?:[:：.]\s*00|[时時点點])(?!\d)")


def _rollover_doubt(rtext: str, start, end):
    """Why a range read as overnight may not be one, or None."""
    local_end = end.astimezone(start.tzinfo)
    if local_end.date() <= start.date() or _NEXT_DAY_RE.search(rtext or ""):
        return None
    hours = (end - start).total_seconds() / 3600
    if start.hour >= 17 and (local_end.hour, local_end.minute) <= (9, 0) and hours <= 12:
        return None
    if _END_OF_DAY_RE.search(rtext or ""):
        # "22:00 - 24:00" / "00:00 - 24:00": a written 24:00 is the midnight
        # that ends the day, on purpose - the whole-day outage included.
        return None
    return ("the time range {:%H:%M}-{:%H:%M} ends before it starts, and a "
            "{:.0f}-hour overnight window is not a plausible reading of it"
            .format(start, local_end, hours))


def _span_doubt(sp, start, end):
    """Why a window built on span ``sp`` does not fit it, or None."""
    lo, hi = date(*sp["lo"]), date(*sp["hi"])
    last = end.astimezone(start.tzinfo).date()
    # "24:00 - 02:00" across 24-25 starts at the midnight that ENDS the 24th,
    # which _clock has already moved onto the 25th.
    first = start.date() == lo or (start.date() == lo + timedelta(days=1)
                                   and (start.hour, start.minute) == (0, 0))
    if first and last == hi and (hi > lo or start.date() == lo):
        return None
    return ("the notice gives the dates as a span, {} - {}, and the time range "
            "{:%H:%M}-{:%H:%M} does not run from the first day into the last "
            "(it may mean every day of the span)".format(
                lo.isoformat(), hi.isoformat(), start,
                end.astimezone(start.tzinfo)))


# ---------------------------------------------------------------------------
# Which ranges belong to the maintenance statement
# ---------------------------------------------------------------------------
#
# A notice is rarely only its window. It carries the support desk's hours, the
# promotion that is "not affected", the settlement run after the outage, a
# deposit channel's pause - every one a perfectly readable range, and every
# one used to be a candidate. The 24h union then folded them into the outage:
# "Our customer service team is available 09:00-21:00 daily" under a
# 10:00-12:00 window wrote 09:00-21:00, and a Mid-Autumn promotion period
# wrote a ten-day outage that also outlived the stale guard. A range is now a
# candidate only when the words around it make it part of the maintenance
# statement - see _relevant, which is the whole rule.

# Words that make a range the maintenance's own. A reschedule verb
# (RESCHEDULE_RE) counts only in a sentence that names no subject at all -
# "时间变更：新时间 X，原时间 Y" - see _topic_near.
_TOPIC_STRONG_RE = re.compile(
    # maintainance / maintenence: the misspellings G1.1 found in real headings.
    r"maintenance|maintainance|maintenence|downtime|upgrade|outage"
    r"|维护|維護|维修|維修|升级|升級|停机|停機|停服",
    re.I)

# Words that make a range the time of something ELSE. Each is a subject a
# provider notice names next to its own hours: the support desk, a promotion or
# tournament, bet settlement, the payment channels, a holiday period.
_TOPIC_OFF_RE = re.compile(
    r"customer\s+(?:service|support|care)|\bsupport\s+(?:team|desk|hours?)"
    # R1.67: "The live chat will be offline <range>" is the chat desk's own
    # window, and it was unioned into the maintenance window.
    r"|\blive\s+chat\b|\bchat\s+(?:support|service|desk)\b|在线客服|在線客服"
    # R1.10 / R1.27: a "Support:" / "Hotline:" / "Cutoff:" / "Report period:"
    # line under the maintenance block is the desk's time, and the new
    # earliest-window pick wrote it in place of the outage.
    r"|\bsupport\s*[:：]|\bhotline\b|\bcut-?off\b|\breport(?:ing)?\s+period\b"
    r"|\bsupport\s+(?:is\s+|will\s+be\s+)?available\b"
    # R1.26: the post-maintenance watch period is not the outage.
    r"|观察期|觀察期|观察时间|觀察時間|\b(?:observation|monitoring)\s+period\b"
    # F15: a "Contact:" / "Contact us:" line, and a TEST environment's own
    # line ("UAT: 09:00 - 12:00", "Staging:") under the production window.
    r"|\bcontact(?:\s+us)?\s*[:：]|\b(?:UAT|staging|sandbox|test(?:ing)?\s+environment)"
    r"\s*[:：]"
    # F17: a new game's early-access LABEL ("New game early access: <X>").
    r"|\b(?:new\s+games?|early\s+access)\b[^:：\n]{0,24}[:：]"
    r"|(?:新游戏|新遊戲|抢先体验|搶先體驗)[^:：\n]{0,12}[:：]"
    # F20: a webinar, meeting or livestream moved is not the maintenance.
    r"|\bwebinars?\b|\bmeetings?\b|\btraining\b|\blive\s*streams?\b"
    r"|直播|会议|會議|培训|培訓|讲座|講座"
    # F31: short data-line labels for staffed hours.
    r"|在线支持|線上支持|在線支持|人工服务|人工服務|\bour\s+team\s*[:：]"
    # F32: an event or festival period - label ("Event period:", "Mid-Autumn
    # Festival:") or a heading line that is only its name ("Mid-Autumn Event").
    r"|\b(?:events?|festivals?|campaigns?|celebrations?)\b(?:\s+(?:period|time|dates?"
    r"|schedule|duration))?\s*[:：]|\b(?:events?|festivals?)\s*$"
    r"|\bhelp\s*desk|\b(?:office|business|working|opening|service|support|cs)"
    r"\s+hours?\b|\bcs\s+team\b"
    r"|\bpromo(?:tion)?s?\b|\btournaments?\b|\bcampaigns?\b|\bcashback\b"
    r"|\bbonus(?:es)?\b|\bjackpots?\b|\blucky\s+draw\b|\bgiveaways?\b"
    r"|\b(?:un)?settle(?:d|s|ment|ments)?\b|\bpayouts?\b|\bdeposits?\b"
    r"|\bwithdraw(?:al|als|s)?\b|\brecharge\b|\btop-?ups?\b"
    r"|\bpayment\s+channels?\b|\bbets?\b|\bwagers?\b|\brounds?\b|\bholidays?\b"
    r"|客服|营业时间|營業時間|办公时间|辦公時間|工作时间|工作時間"
    r"|活动|活動|优惠|優惠|促销|促銷|锦标赛|錦標賽|比赛|比賽|赛事|賽事"
    r"|红利|紅利|奖金|獎金|结算|結算|派彩|充值|提款|提现|提現|存款|出款"
    r"|支付通道|注单|注單|投注|下注|假期|假日|放假",
    re.I)

# The outage described rather than named. Not enough on its own to beat an
# off-topic subject, only to admit a range whose sentence has nothing else.
_TOPIC_WEAK_RE = re.compile(
    r"\bunavailable\b|\boffline\b|\bsuspen(?:d|ded|ds|sion)\b"
    r"|\binterrupt(?:ed|ion|ions)?\b|\binaccessible\b"
    r"|暂停|暫停|中断|中斷|无法登录|無法登錄|无法访问|無法訪問|无法进入|無法進入"
    r"|下线|下線",
    re.I)

_WEEKDAY_RE = re.compile(
    r"\b(?:(mon)(?:day)?|(tue)(?:s|sday)?|(wed)(?:nesday)?|(thu)(?:rs?|rsday)?"
    r"|(fri)(?:day)?|(sat)(?:urday)?|(sun)(?:day)?)\b\.?"
    r"|(?:周|週|星期|礼拜|禮拜)([一二三四五六日天])",
    re.I)
_ZH_WEEKDAY = {"一": 0, "二": 1, "三": 2, "四": 3, "五": 4, "六": 5,
               "日": 6, "天": 6}

# What may sit around the dates, ranges and zones of a DATA sentence - one that
# is a label and the window and nothing else ("Time: 10:00 - 12:00 (GMT+8)",
# "Slots: 10:00 - 12:00", "| Fishing | 2026-09-24 | 09:00-10:00 |",
# "日期Date：Sep 16th, 2026 (Wed)"). See _data_kind for the three shapes.
_LEFTOVER_GLUE_RE = re.compile(
    _WEEKDAY_RE.pattern + r"|[\(（][一二三四五六日天][\)）]"
    r"|\b(?:from|to|at|on|between|and|until|till)\b|至|到|及|和",
    re.I)
_DATA_LINE_RE = re.compile(
    r"[^\w]*(?:\d{1,2}[.)、][^\w]*)?(?:[^:：|\n]{1,40}?[:：|])?[^\w]*")
# A sentence that OPENS with a time/date label: "New window: 10:00 - 14:00",
# "Time: 2026-09-24 10:00 - 12:00 (GMT+8), unfinished games will be cancelled."
# The label says what the range is the time of, whatever follows it.
_TIME_LABEL_RE = re.compile(
    r"[^\w]*(?:\d{1,2}[.)、][^\w]*)?[^:：|\n]{0,24}?"
    r"(?:time|date|schedule|window|period|duration|slot"
    r"|時間|时间|日期|時段|时段|期间|期間|窗口|排程)"
    r"[^:：|\n]{0,12}?[:：]",
    re.I)

# The mask character _collect writes over every date, range and zone token, so
# the tests below read only the WORDS of the line. "Sept. 23, 2026" carries a
# full stop, and without the mask it ended the sentence and cut the window off
# from the "Scheduled maintenance" in front of it.
_MASK = "\x00"
# Zone tokens get a mask of their own: they may sit between a date and its
# range ("2026-09-23 GMT+8 10:00") and count as glue there, where a masked
# date or range must NOT - "<date> 06:00 - 08:00 (GMT+8) / 22:00 - 00:00" would
# otherwise attach the date to the second range straight through the first.
_MASK_TZ = "\x01"


def _mask(text: str, spans, ch: str = _MASK) -> str:
    out = list(text)
    for a, b in spans:
        for i in range(a, min(b, len(out))):
            if out[i] != "\n":
                out[i] = ch
    return "".join(out)


def _weekdays(s: str) -> set:
    out = set()
    for m in _WEEKDAY_RE.finditer(s):
        for i in range(7):
            if m.group(i + 1):
                out.add(i)
        if m.group(8):
            out.add(_ZH_WEEKDAY[m.group(8)])
    return out


def _line_at(text: str, pos: int):
    ls = text.rfind("\n", 0, pos) + 1
    le = text.find("\n", pos)
    return ls, (len(text) if le == -1 else le)


def _is_bound(text: str, i: int) -> bool:
    ch = text[i]
    return ch in _SENT_BOUND and not (
        ch in ".．" and i and text[i - 1].isdigit()
        and i + 1 < len(text) and text[i + 1].isdigit())


def _sentence_span(text: str, a: int, b: int):
    """(start, end) of the sentence holding text[a:b], by _sentence_index's rule."""
    s = a
    while s > 0 and not _is_bound(text, s - 1):
        s -= 1
    e = b
    while e < len(text) and not _is_bound(text, e):
        e += 1
    return s, e


def _data_kind(msent: str, *, dates_only: bool = False):
    """'bare' | 'label' | 'row' | None for one MASKED sentence.

      bare   nothing but one short label and the window: "Time: <window>",
             "Slots: <window>", "<date> <window> (<zone>)", "| Slots | ... |"
      label  opens with a time/date label, whatever follows: "New window:
             <window>", "Time: <window>, unfinished games will be cancelled."
      row    the window plus at most three words and no label: "Slot <window>",
             "Wed <window> Slots", "Part 1: <window> (database)". Only a row
             under a maintenance heading counts (see _relevant) - on its own
             "New game launch <window>" is the same shape.

    ``dates_only`` refuses the label shape: a date is lent to a range on
    another line only from a bare date line, because "Date: Tue, Sep 22, 2026
    at 3:15 PM" is a forwarded mail header that opens with a date label too.
    """
    left = _LEFTOVER_GLUE_RE.sub(
        " ", msent.replace(_MASK, " ").replace(_MASK_TZ, " "))
    if _DATA_LINE_RE.fullmatch(left):
        return "bare"
    if not dates_only and _TIME_LABEL_RE.match(msent):
        return "label"
    if _MASK in msent:
        words = re.findall(r"\w+", left)
        if len(words) <= 3 and len("".join(words)) <= 24:
            return "row"
    return None


def _is_data_line(mline: str) -> bool:
    return _data_kind(mline, dates_only=True) == "bare"


def _block_heading(masked: str, s: int) -> str:
    """The nearest sentence before ``s`` that is not blank and not data.

    Walked by sentence, not by line, so a label that follows a full stop on
    the same line ("... will be extended. New window: 10:00 - 14:00") finds
    the sentence in front of it.
    """
    pos = s
    while pos > 0:
        k = pos - 1
        ps = k
        while ps > 0 and not _is_bound(masked, ps - 1):
            ps -= 1
        sent = masked[ps:k]
        if sent.strip() and _data_kind(sent) is None \
                and not _BLOCK_DESCRIPTOR_RE.search(sent):
            return sent
        # R1.23: "Maintenance Notice:" / "维护通知：" / "Scheduled maintenance:"
        # over a "<product> <window>" row - a label with nothing after its
        # colon reads as a bare data line, and the heading was skipped, so the
        # row under it was nobody's time.
        if (re.fullmatch(r"\s*[^\n:：]{1,40}[:：]\s*", sent)
                and _TOPIC_STRONG_RE.search(sent) and not _ANY_RANGE_RE.search(sent)):
            return sent
        pos = ps
    return ""


# R1.71: lines that DESCRIBE the maintenance sitting between its heading and
# the Date/Time block - "Affected Games: … Jackpot Fishing", "影响范围：所有游戏、
# 投注及充值提款", "Impact: bets, deposits and withdrawals", "Kindly inform your
# players and customer service team of the schedule below:", "如有任何问题，请
# 联系我们的客服。" They are not the block's heading: taking them as one made the
# block "about" the jackpot or the support desk, and the window was dropped.
_BLOCK_DESCRIPTOR_RE = re.compile(
    r"^\s*(?:affected|impact(?:ed)?|scope|products?|games?|services?|platforms?"
    r"|modules?|systems?|notes?|remarks?|details?|reasons?|contents?|description)"
    r"(?:\s+(?:games?|products?|services?|scope|areas?|platforms?|systems?"
    r"|items?|range))?\s*[:：]"
    r"|^\s*(?:影响范围|影響範圍|影响|影響|受影响|受影響|涉及|范围|範圍|产品|產品|游戏"
    r"|遊戲|备注|備註|说明|說明|原因|内容|內容)[^:：\n]{0,6}[:：]"
    r"|\b(?:kindly|please)\s+(?:inform|notify|advise|remind|update)\s+(?:your|all)\b"
    r"|\bduring\s+(?:this|the)\s+(?:period|time|maintenance)\b"
    r"|\b(?:contact|reach)\s+(?:our|the|your)\s+(?:support|cs|customer|account"
    r"|service)|\bdo\s+not\s+hesitate\s+to\s+contact\b"
    r"|如有任何(?:问题|問題|疑问|疑問)|如有(?:问题|問題|疑问|疑問)|请联系|請聯繫|请联络|請聯絡",
    re.I)


# F17: a heading that names a RELEASE heads that release's own "Time:" line.
# "Thanks for your patience during yesterday's scheduled maintenance.\nNew game
# release\nTime: <window>" opened the gate on the first sentence and wrote the
# launch window as an outage. Only headings are read against this list - in a
# sentence, "maintenance for the v2.3 release 10:00-12:00" is still the
# maintenance's own window, so it is not added to _TOPIC_OFF_RE.
_OTHER_HEADING_RE = re.compile(
    r"\brelease[sd]?\b|\blaunch(?:es|ed)?\b|\bgo-?live\b|\bnew\s+games?\b"
    r"|上线|上線|发布|發布|新游戏|新遊戲|开服|開服|发行|發行",
    re.I)


def _off_heading(head: str) -> bool:
    return bool((_TOPIC_OFF_RE.search(head) or _OTHER_HEADING_RE.search(head))
                and not _TOPIC_STRONG_RE.search(head))


# R1.71: an off-topic noun that the MAINTENANCE takes down. "因系统维护，投注与
# 提款将于<window>暂停", "Due to scheduled maintenance, bets and deposits will be
# suspended on <window>", "all games and jackpots will be unavailable … due to
# scheduled maintenance", "维护期间将暂停所有投注：<window>" - the nearest subject
# is the bets, but they are what goes DOWN, and the maintenance is named as the
# cause. The window was dropped as somebody else's time. Needs BOTH the cause
# marker and an outage verb, so "our customer service is available 09:00-21:00
# during the maintenance" stays the desk's own hours.
_MAINT_CAUSE_RE = re.compile(
    r"(?:因|由于|由於|因为|因為)[^。；\n]{0,10}(?:维护|維護|维修|維修|升级|升級)"
    r"|(?:维护|維護|维修|維修|升级|升級)(?:期间|期間|时段|時段|过程中|過程中)"
    r"|\b(?:due\s+to|because\s+of|owing\s+to|during|for)\s+(?:the\s+|our\s+|a\s+)?"
    r"(?:scheduled\s+|planned\s+|routine\s+|system\s+|server\s+|emergency\s+)?"
    r"(?:maintenance|upgrade|downtime)\b",
    re.I)


_OFF_PREDICATE_RE = re.compile(
    r"[ \t]*(?:为|為|是|作为|作為)[ \t]*(?:观察期|觀察期|观察时间|觀察時間|监控期|監控期)"
    r"|[ \t]*(?:is|will\s+be)\s+(?:the\s+)?(?:observation|monitoring|support)\s+"
    r"(?:period|window|hours)", re.I)


def _victim_of_maintenance(sent: str) -> bool:
    return bool(_MAINT_CAUSE_RE.search(sent) and _TOPIC_WEAK_RE.search(sent))


def _topic_near(masked: str, a: int, b: int):
    """'strong' | 'off' | None: the subject word nearest to text[a:b].

    The words immediately in front of a range say what it is the time OF, so
    the last subject word before it in its sentence decides; only a sentence
    with none in front looks after it. Nearest wins, which is what separates
    "Scheduled maintenance 10:00-12:00, customer service 09:00-21:00" (two
    subjects, one per range) from a sentence that merely mentions both.
    """
    s, e = _sentence_span(masked, a, b)
    kinds = ((_TOPIC_STRONG_RE, "strong"), (_TOPIC_OFF_RE, "off"))
    best, kind = -1, None
    for rx, k in kinds:
        for m in rx.finditer(masked, s, a):
            if m.end() > best:
                best, kind = m.end(), k
    if kind == "off" and _victim_of_maintenance(masked[s:e]):
        return "strong"
    # R1.26: "<range> 为观察期" / "<range> is the observation period" names what
    # the range IS right after it, and that beats the maintenance word before.
    if _OFF_PREDICATE_RE.match(masked, b):
        return "off"
    if kind:
        return kind
    for rx, k in kinds:
        m = rx.search(masked, b, e)
        if m and (kind is None or m.start() < best):
            best, kind = m.start(), k
    if kind:
        return kind
    # A reschedule VERB is not a subject: in "The Mid-Autumn tournament has
    # been postponed to 20:00-22:00" the tournament is what moved. It only
    # decides a sentence that names no subject at all ("时间变更：新时间 X").
    if RESCHEDULE_RE.search(masked, s, e):
        return "strong"
    return None


def _relevant(masked: str, a: int, b: int, *, dates_only: bool = False):
    """Is the range (or date) at text[a:b] part of the maintenance statement?

    -> "named" | "described" | None. In order: the nearest subject word in its
    sentence (maintenance wording -> named, an off-topic subject -> None); else
    a DATA sentence (_data_kind) is named as part of the block it sits in,
    unless that block's heading is about something else ("Customer Service
    Hours" over "Mon-Fri: 09:00-18:00") - and a bare ROW only under a heading
    that names the maintenance; else the sentence must at least DESCRIBE an
    outage ("our games will be unavailable") or open the wording gate. A range
    that passes none of these is somebody else's time and is never a window.
    """
    kind = _topic_near(masked, a, b)
    if kind == "off":
        return None
    if kind == "strong":
        return "named"
    s, e = _sentence_span(masked, a, b)
    sent = masked[s:e]
    dk = _data_kind(sent, dates_only=dates_only)
    if dk:
        head = _block_heading(masked, s)
        if _off_heading(head):
            return None
        if dk == "row" and not (_TOPIC_STRONG_RE.search(head)
                                or RESCHEDULE_RE.search(head)):
            # G1.5: a Chinese clause has no spaces, so "<window> 游戏将无法进入。"
            # counts as ONE word and reads as a bare row. A row whose own
            # words open the wording gate is the outage's own statement.
            if _sched_re().search(sent):
                return "described"
            return None
        return "named"
    if _TOPIC_WEAK_RE.search(sent) or _sched_re().search(sent):
        return "described"
    # A sentence with no subject word at all continues the one before it when
    # that one announced a RESCHEDULE: "Scheduled maintenance rescheduled.
    # Originally scheduled for X, now Y." - the new window is in a sentence
    # that names nothing, and dropping it turned a readable reschedule into
    # "no new window could be read". Only a reschedule heading lends its topic
    # this way; an ordinary heading does not, or "Our office is open
    # 09:00-18:00." after a maintenance line would be read as the outage.
    if RESCHEDULE_RE.search(_block_heading(masked, s)):
        return "named"
    # F49: "The maintenance on X was cancelled. It will take place on Y
    # instead." - the pronoun is the maintenance of the sentence before, and
    # Y was dropped as nobody's time (a silent, final ignore).
    if _PRONOUN_MAINT_RE.match(sent) and s > 0:
        ps, pe = _sentence_span(masked, s - 1, s - 1)
        if _TOPIC_STRONG_RE.search(masked[ps:pe]):
            return "named"
    return None


_PRONOUN_MAINT_RE = re.compile(
    # F49: "We will do it on <Y>" / "We will perform it on <Y> instead".
    r"\W*we\s+will\s+(?:now\s+)?(?:do|perform|carry\s+out|hold|run|conduct)\s+(?:it|the\s+"
    r"maintenance)\b|"
    r"\W*(?:it|this|the\s+(?:new|rescheduled)\s+(?:one|date|time|window))\s+will\s+"
    r"(?:now\s+|instead\s+)?(?:take\s+place|be\s+(?:held|carried\s+out|performed|conducted"
    r"|done|moved|rescheduled|on)|start|begin|run|happen|proceed)\b", re.I)


# ---------------------------------------------------------------------------
# Relative days: today / tomorrow / tonight / 今天 / 明天 / 本周四
# ---------------------------------------------------------------------------
#
# The scraper hands over each bubble's clock but NOT its date (telegramwarm
# keeps only "HH:MM"), so "tomorrow" is relative to a day this code does not
# know. Resolving it against the READ time is a guess that is wrong whenever
# the bubble is read on a later day than it was posted - a backlog read, a
# /vacheck, a bubble posted at 23:50 and read at 00:10 - and the row would
# carry a confident window one day off. Every "emergency maintenance today
# 15:00-16:00" notice is exactly this shape, and it used to be retried as
# "unparsed" three times and then dropped with nobody told. Such a notice is
# needs_human now: nothing is written, and the card names the relative word.

# The DAY words: a range whose own sentence says one of these has been given
# its day, and must not borrow a calendar date from another line (_bind_date).
# Weekdays are not here - a borrowed date is checked against them instead.
_RELATIVE_DAY_RE = re.compile(
    r"\b(?:today|tonight|tomorrow|tmrw?|later\s+today"
    r"|this\s+(?:morning|afternoon|evening))\b"
    r"|今天|今日|今晚|今夜|今早|明天|明日|明早|明晚|后天|後天|即日|今明",
    re.I)
# Everything that dates a notice only relative to when it was posted, for the
# needs_human verdict: the day words, "now", a weekday with no date.
_RELATIVE_WHEN_RE = re.compile(
    _RELATIVE_DAY_RE.pattern
    + r"|\b(?:starting|from|right|effective|as\s+of)\s+now\b"
    r"|\bnow\s+(?:underway|in\s+progress|ongoing)\b|\bimmediately\b"
    r"|\b(?:this|next|coming)\s+(?:week(?:end)?|" + _WD_NAME + r")\b"
    r"|\b(?:mon|tues|wednes|thurs|fri|satur|sun)day\b"
    r"|(?:本|这|這|下个?|下個)?" + _WD_ZH_PREFIX + r"[一二三四五六日天]"
    r"|即刻|立即|现在|現在|马上|馬上",
    re.I)
# A clock anywhere in the sentence: a relative word with no time at all
# ("thanks for your patience during today's emergency maintenance") is not a
# window a person could fill.
_ANY_CLOCK_RE = re.compile(
    r"(?<![\d:])\d{1,2}\s*[:.]\s*\d{2}(?![\d:])|\b\d{1,2}\s*[AaPp]\.?[Mm]\b"
    r"|\d{1,2}\s*[时時点點]")


def _relative_when(t: str, hit=None):
    """The word a dateless maintenance notice is dated by, or None.

    Looked for in the sentence of the wording-gate hit and in the sentence of
    every clock, and only when the message carries a clock at all. ``hit`` is
    the gate match the caller already has (a plain-wording shape has no
    _sched_hit of its own).
    """
    clocks = [m.start() for m in _ANY_CLOCK_RE.finditer(t)]
    if not clocks:
        return None
    if _ACK_OPENER_RE.match(t):
        # R1.9: "Thanks for the maintenance notice, see you tomorrow at 10:00"
        # thanks us for a notice - it is not one - and was carded as a
        # maintenance "dated only tomorrow".
        return None
    hit = hit or _sched_hit(t)
    for pos in ([hit.start()] if hit else []) + clocks:
        s, e = _sentence_span(t, pos, pos)
        m = _RELATIVE_WHEN_RE.search(t, s, e)
        if m:
            if _NONE_STATED_RE.search(t, s, e):
                # R1.5: "Maintenance: none this week." says there is NO
                # maintenance this week; its "this week" dates nothing.
                continue
            return m.group(0)
    return None


_ACK_OPENER_RE = re.compile(
    r"\s*(?:(?:thanks?|thank\s+you|thx|noted|well\s+noted|received|got\s+it|ok(?:ay)?"
    r"|acknowledged)\b"
    # R1.9: the zh opener has no \b after it - a CJK letter is \w too, so
    # "收到维护通知，谢谢，明天10:00见" never matched and was carded.
    r"|收到|好的|谢谢|謝謝|感谢|感謝|知悉|了解)", re.I)
_NONE_STATED_RE = re.compile(
    r"\bnone\b|\bno\s+(?:scheduled\s+|planned\s+)?maintenance\b|\bnothing\s+(?:planned|scheduled)\b"
    r"|无维护|無維護|没有维护|沒有維護|暂无维护|暫無維護|无$|无。", re.I)


def _relative_between(masked: str, p: int, e: int, a: int, b: int) -> bool:
    """Does a relative DAY word stand between a borrowed date and its range?

    ``p``..``e`` is the date, ``a``..``b`` the range. Checked in the range's
    own sentence only - from the date to the sentence end when the date is
    before the range, from the sentence start to the date when it is after -
    so "Our next maintenance is on 2026-10-15.\nToday we have emergency
    maintenance 15:00-16:00." no longer writes 10-15, while an unrelated
    "today" in some other sentence cannot block a real Date line.
    """
    s, se = _sentence_span(masked, a, b)
    lo, hi = (max(e, s), se) if e <= a else (s, min(p, se))
    return lo < hi and bool(_RELATIVE_DAY_RE.search(masked, lo, hi))


# ---------------------------------------------------------------------------
# Which date a range takes
# ---------------------------------------------------------------------------

# A date ATTACHED in front of a range: only a comma, a bracketed weekday or
# zone, or one short word ("from", "周三") between them - the same tolerance
# _datetime_points applies. "2026-09-23 10:00", "Sep 16th, 2026 (Wed) 22:00",
# "on 2026-09-23 from 10:00".
# Matched on the MASKED text, so a zone token between them ("2026-09-23 GMT+8
# 10:00") is mask characters and counts as glue.
_ATTACH_GLUE_RE = re.compile(
    r"[\s,，\x01]*(?:[\(（][^)）\n\x00]{1,12}[\)）][\s,，\x01]*)?"
    r"(?:[^\s\d:：\x00\x01]{1,4}[\s,，\x01]*)?")

# ...and a date INTRODUCED straight after one: "10:00 - 12:00 (GMT+8) on
# 2026-09-30", "10:00-12:00 (GMT+8)，日期：9月30日", "10:00 - 12:00 2026-09-24".
# A list of ranges may stand between ("10:00-12:00 (Slots), 14:00-16:00 (Live)
# on 2026-09-23"): the trailing date governs the whole list.
_AFTER_INTRO_RE = re.compile(
    r"(?:[\s\x00\x01,，、/&]|\band\b|及|和|[\(（][^)）\n\x00]{1,12}[\)）])*"
    r"(?:(?:on|dated?)\b\s*|日期\s*[:：]?\s*|date\s*[:：]\s*|于\s*|於\s*)?"
    r"(?:(?:mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)[a-z]*\.?\s*,?\s*)?"
    r"[\(（]?\s*",
    re.I)


def _bind_date(text: str, masked: str, a: int, b: int, dates, sup,
               letterhead, ineligible, rstarts=()):
    """-> (date index | None, own, far, ambiguity | None) for the range text[a:b].

    ``own`` means the date is written against this range (attached in front,
    or introduced right after); ``far`` means it was borrowed from another
    line. In order:

      1. A date attached in front, or introduced after. Both, disagreeing, is
         a contradiction on one line and the window is ambiguous. The one
         introduced AFTER beats a merely earlier date on the line: "Because of
         the National Day holiday (2026-10-01), ... maintenance ... 10:00 -
         12:00 on 2026-09-30" wrote 10-01, because nearest-before was the only
         rule.
      2. The nearest ELIGIBLE date earlier on the same line.
      3. The nearest eligible date on an earlier line - unless the range's own
         sentence names a weekday that date does not fall on.
      4. For a range on a data line ("Time: 22:00 - 02:00"): the date on the
         next non-empty line, if that is a bare date line ("Date: ..."). For a
         prose range the same date is only a guess - it may be a sign-off - so
         it comes back AMBIGUOUS and a human decides.
      5. Otherwise NONE. It used to be the message's first date, which is how
         a promo end date, a sign-off date, a ticket id and a URL parameter
         each became the day of an outage that stated no day of its own.

    A letterhead date ("Issued:", "發佈日期：") is never lent, not even by rule
    1: it is the one date on the page that is certainly not the window's.
    ELIGIBLE means the date is itself part of the maintenance statement
    (_relevant) - "New game Dragon Fortune launches on 2026-09-25!" does not
    date the "Scheduled maintenance this Wednesday" below it, and neither does
    a forwarded mail's "Date: Tue, Sep 22, 2026 at 3:15 PM" header. Within each
    pool a date NOT marked superseded is preferred, which is what keeps "moved
    from 2026-09-24 to 2026-09-26, 10:00 - 12:00" on the 26th.
    """
    ls, le = _line_at(text, a)
    before = [i for i, (p, e, _d) in enumerate(dates)
              if ls <= p and e <= a and i not in letterhead]
    after = [i for i, (p, _e, _d) in enumerate(dates)
             if b <= p < le and i not in letterhead]

    att = before[-1] if before and _ATTACH_GLUE_RE.fullmatch(
        masked[dates[before[-1]][1]:a]) else None
    itr = after[0] if after and _AFTER_INTRO_RE.fullmatch(
        masked[b:dates[after[0]][0]]) else None
    if itr is not None and any(
            _ATTACH_GLUE_RE.fullmatch(masked[dates[itr][1]:r])
            # R1.41: never across a sentence end. "...14:00-16:00 (GMT+8) on
            # 24/09/2026. 10:00-12:00 was wrong." handed the date to the NEXT
            # sentence's range, the 14:00 window lost it, and the correction
            # wrote exactly the window it said was wrong.
            and not re.search(r"[.。．;；!?！？]", masked[dates[itr][1]:r])
            for r in rstarts if dates[itr][1] <= r < le):
        # "10:00-12:00, 2026-09-24 14:00-16:00": that date is written in
        # front of the NEXT range and is its date, not a trailer of this one.
        itr = None
    own = [i for i in (att, itr) if i is not None]
    if own:
        live = [i for i in own if i not in sup] or own
        if len(live) == 2 and dates[live[0]][2] != dates[live[1]][2]:
            return live[0], True, False, (
                "the line gives the window two dates, {:04d}-{:02d}-{:02d} "
                "before it and {:04d}-{:02d}-{:02d} after it".format(
                    *dates[live[0]][2], *dates[live[1]][2]))
        return (itr if itr in live else live[0]), True, False, None

    s, e = _sentence_span(masked, a, b)
    # F3 / F4: the weekday may also stand in the HEADING over a time row
    # ("本周四进行例行维护\n时间：10:00-12:00\n2026年9月22日") - a heading line of
    # the same block that names the maintenance. A borrowed date that falls
    # on another day is a posting or sign-off date, not the window's.
    head_wd, head_rel = _heading_day_words(masked, s)
    wd = _weekdays(masked[s:e]) or head_wd

    def _day_ok(i):
        return not wd or date(*dates[i][2]).weekday() in wd

    pool = [i for i in before if i not in ineligible]
    if pool:
        use = [i for i in pool if i not in sup] or pool
        # "Following the 2026-09-16 maintenance, emergency maintenance
        # tomorrow 10:00-12:00": the day the range is given is "tomorrow",
        # not the date further up the line (G1.3).
        if _relative_between(masked, dates[use[-1]][0], dates[use[-1]][1], a, b):
            return None, False, False, None
        # F4: "[23/09/2026] Scheduled maintenance 10:00 - 12:00 on Thursday."
        # - the earlier date on the line is the posting date; the window's day
        # is the weekday the sentence names, which the 23rd is not.
        if not _day_ok(use[-1]):
            return None, False, False, None
        return use[-1], False, False, None

    # Only a labelled or bare time line borrows forward: a short prose
    # sentence ("排定维护，时间 X，敬请留意。") also passes as a row, and the
    # date under it is a sign-off, not the window's day.
    data_range = _data_kind(masked[s:e]) in ("bare", "label")

    prev = [i for i, (p, _e, _d) in enumerate(dates)
            if p < ls and i not in letterhead and i not in ineligible]
    if prev:
        use = [i for i in prev if i not in sup] or prev
        if _relative_between(masked, dates[use[-1]][0], dates[use[-1]][1], a, b):
            return None, False, False, None
        if head_rel and not _on_date_label_line(masked, dates[use[-1]][0]):
            # F3: "明天进行例行维护" over the time row - the day is "tomorrow",
            # and an unlabelled date elsewhere cannot say which day that is.
            return None, False, False, None
        if data_range and not _on_date_label_line(masked, dates[use[-1]][0]):
            # F2: "2026-09-22\nScheduled maintenance\nTime: 22:00 - 02:00\n
            # Date: 2026-09-24 (Thu)" - the date above is the day the notice
            # went out (a bare first line, a heading's date), the labelled
            # "Date:" line under the time row is the window's. Nearest-earlier
            # wrote the 22nd, two days early.
            below = _date_line_below(text, masked, le, dates, letterhead, ineligible)
            if below is not None and _day_ok(below) and not _relative_between(
                    masked, dates[below][0], dates[below][1], a, b):
                return below, False, True, None
        return (use[-1], False, True, None) if _day_ok(use[-1]) \
            else (None, False, False, None)

    pos = le + 1
    hops = 0
    while pos < len(text):
        ne = text.find("\n", pos)
        ne = len(text) if ne == -1 else ne
        if text[pos:ne].strip():
            # R1.12: step over a SIBLING range line ("Time (UTC): 17:00 - 19:00"
            # under "Time (GMT+8): 01:00 - 03:00") to reach the date line below
            # them. Stopping at it left the GMT+8 line with no date, the UTC
            # copy alone got 2026-09-23, and a day that could be either zone's
            # was written as UTC's with nothing to say it was a guess
            # (_merge_zone_copies now flags exactly that shape).
            if (data_range and hops < 3 and _range_on(text, pos, ne)
                    and _is_data_line(masked[pos:ne])):
                hops += 1
                pos = ne + 1
                continue
            break
        pos = ne + 1
    else:
        return None, False, False, None
    nline = masked[pos:ne]
    if not _is_data_line(nline) or _range_on(text, pos, ne):
        return None, False, False, None
    lab = re.match(r"[^\w]*([^:：|\n]{1,40}?)\s*[:：|]", nline)
    if lab and not _DATE_LABEL_RE.search(lab.group(1)):
        # "Next regular maintenance: 10/08" dates ANOTHER maintenance; only an
        # unlabelled date or a Date/日期 label dates the window above it.
        return None, False, False, None
    nxt = [i for i, (p, _e, _d) in enumerate(dates)
           if pos <= p < ne and i not in letterhead and i not in ineligible]
    if not nxt or not _day_ok(nxt[0]) or _relative_between(
            masked, dates[nxt[0]][0], dates[nxt[0]][1], a, b):
        return None, False, False, None
    if head_rel and not (lab and _PLAIN_DATE_LABEL_RE.fullmatch(lab.group(1).strip())):
        # F3: a relative day in the heading and a bare date under the row: the
        # date is the sign-off, "明天" is the day - which the text cannot give.
        return None, False, False, None
    if data_range:
        return nxt[0], False, True, None
    # The window is a prose sentence and the only date is a bare date on the
    # line below it. "Scheduled maintenance 10:00 - 12:00 (GMT+8)\n2026-09-24
    # (Thu)" means the 24th; "排定维护，时间 10:00 - 12:00，敬请留意。\n2026-09-22"
    # is a sign-off. The text cannot tell them apart, so a human reads it.
    return nxt[0], False, True, ("the only date is on the line after the "
                                 "window and may be a sign-off date")


_DATE_LABEL_RE = re.compile(r"date|\bday\b|日期|日子", re.I)
# F2: exactly "Date:" / "Maintenance date:" / "日期：" - not "Date of next
# maintenance:", which dates another one.
_PLAIN_DATE_LABEL_RE = re.compile(
    r"(?:(?:maintenance|维护|維護)\s*)?(?:date|日期)(?:\s*[(（][^)）]{0,20}[)）])?", re.I)
_LABEL_HEAD_RE = re.compile(r"[^\w]*([^:：|\n]{1,40}?)\s*[:：|]")


def _heading_day_words(masked: str, s: int):
    """(weekdays, relative-word match) stated by the maintenance heading lines
    of the block above sentence-start ``s`` (up to the nearest blank line)."""
    bs = masked.rfind("\n\n", 0, s)
    bs = 0 if bs == -1 else bs + 2
    wd, rel = set(), None
    for line in masked[bs:s].split("\n"):
        if not line.strip() or not _TOPIC_STRONG_RE.search(line) or _ANY_RANGE_RE.search(line):
            continue
        wd |= _weekdays(line)
        rel = rel or _RELATIVE_WHEN_RE.search(line)
    return wd, rel


def _on_date_label_line(masked: str, p: int) -> bool:
    """True when the date at ``p`` is on a line labelled Date / 日期."""
    ls, le = _line_at(masked, p)
    lab = _LABEL_HEAD_RE.match(masked[ls:le])
    return bool(lab and lab.end() <= p - ls + 1 and _DATE_LABEL_RE.search(lab.group(1)))


def _date_line_below(text, masked, le, dates, letterhead, ineligible):
    """The date on the plain "Date:" line right under line-end ``le`` (over at
    most three sibling time rows), or None."""
    pos, hops = le + 1, 0
    while pos < len(text):
        ne = text.find("\n", pos)
        ne = len(text) if ne == -1 else ne
        if not text[pos:ne].strip():
            pos = ne + 1
            continue
        if hops < 3 and _range_on(text, pos, ne) and _is_data_line(masked[pos:ne]):
            hops += 1
            pos = ne + 1
            continue
        lab = _LABEL_HEAD_RE.match(masked[pos:ne])
        if not lab or not _PLAIN_DATE_LABEL_RE.fullmatch(lab.group(1).strip()):
            return None
        if _range_on(text, pos, ne):
            return None
        nxt = [i for i, (p, _e, _d) in enumerate(dates)
               if pos <= p < ne and i not in letterhead and i not in ineligible]
        return nxt[0] if nxt else None
    return None


def _range_on(text: str, ls: int, le: int) -> bool:
    """Does text[ls:le] carry a time range of its own? Then its date is its own."""
    line = text[ls:le]
    return any(rx.search(line) for rx, _c in _RANGE_RES)


# ---------------------------------------------------------------------------
# Candidates and resolution
# ---------------------------------------------------------------------------

# A zone word straight after a range that the _TZ_NAMES table does not know:
# "04:00 - 06:00 CEST", "06:00-08:00 CET (13:00-15:00 GMT+8)", "(Malta time)".
# Such a range was stamped with the document's zone or NOTICE_TZ, i.e. read in
# the wrong zone, and once separate windows stopped being unioned it became a
# window of its own on the wrong hours. The whole notice goes to a human.
#
# The same in Chinese, where the zone is a place: "04:00-06:00 欧洲中部时间",
# "美国东部时间". A place with a fixed offset is in _TZ_WORDS and is not
# unknown; the ones listed here all keep daylight saving, so their offset
# depends on a season the parser will not guess (F16).
_ZH_FOREIGN_ZONE = (
    r"(?:欧洲|歐洲|英国|英國|伦敦|倫敦|马耳他|馬耳他|美国|美國|美东|美東|美西"
    r"|美中|美加|太平洋|大西洋|澳洲|澳大利亚|澳大利亞|悉尼|雪梨|莫斯科|俄罗斯"
    r"|俄羅斯|迪拜|巴西|墨西哥|德国|德國|法国|法國|意大利|西班牙|葡萄牙|荷兰"
    r"|荷蘭|瑞典|土耳其|以色列|南非|加拿大|纽约|紐約|洛杉矶|洛杉磯|夏令|冬令)"
    r"[\u4e00-\u9fff]{0,4}?(?:时间|時間)")
# Upper-case words that end in T and are not zones. "10:00 - 12:00 NEXT WEEK"
# is not a zone called NEXT.
_NOT_ZONE_WORDS = (r"NEXT|LAST|PAST|JUST|MOST|BEST|TEST|TEXT|POST|HOST|LIST"
                   r"|REST|EAST|WEST|FIRST|START|ALERT|EVENT|ABOUT|SHORT|SPORT"
                   r"|SOFT|SHIFT|RESET|LIMIT|INPUT|AUDIT|EXIT|EDIT|UNIT|VISIT"
                   r"|WAIT|NOT|BUT|OUT|GET|SET|LET|PUT|YET|LOT|HOT|ACT")
_UNKNOWN_ZONE_AFTER_RE = re.compile(
    r"[\s(（\[]*(?:"
    r"(?!(?:SGT|MYT|PHST|PHT|BJT|CST|HKT|JST|KST|ICT|WIB|IST|EST|EDT|PST|PDT|GMT|"
    + _NOT_ZONE_WORDS + r")\b)"
    r"[A-Z]{2,4}T\b|MSK\b"
    r"|(?!(?:Manila|Singapore|Beijing|Hong|HK|Taipei|Philippine|Philippines"
    r"|China|Malaysia|Macau|Local)\b)[A-Z][a-z]+\s+time\b"
    r"|" + _ZH_FOREIGN_ZONE + r")")

# R1.62: more of the zones a range can end with that the table does not know,
# each of which was silently stamped with NOTICE_TZ (+08): "UK time",
# "Central European Summer Time", "Eastern Time", a city or IANA name in
# brackets ("(Malta)", "(Europe/London)"), the bare US abbreviations, and the
# provider's own "server time". SAME LINE only ([ \t], not \s): on the next
# line "End Time:" is a label, not a zone. "local time" is not here - it is how
# the Asian studios qualify an offset they already gave ("(GMT+8) local time").
_FOREIGN_PLACE = (r"Malta|London|Sofia|Seoul|Tokyo|Dubai|Istanbul|Moscow|Paris"
                  r"|Berlin|Madrid|Rome|Amsterdam|Stockholm|Oslo|Helsinki|Athens"
                  r"|Kyiv|Kiev|Warsaw|Prague|Vienna|Lisbon|Dublin|Riga|Tallinn"
                  r"|Vilnius|Belgrade|Bucharest|Budapest|Zagreb|Nicosia|Cyprus"
                  r"|Gibraltar|Curacao|Curaçao|New\s+York|Los\s+Angeles|Toronto"
                  r"|Vancouver|Sydney|Melbourne|Auckland|Mumbai|Delhi|Kolkata"
                  r"|Karachi|Dhaka|Sao\s+Paulo|São\s+Paulo|Lima|Bogota|Mexico\s+City"
                  r"|Buenos\s+Aires|Santiago|UK|England|Estonia|Latvia|Lithuania"
                  r"|Bulgaria|Romania|Serbia|Ukraine|Russia|Israel|Turkey|Greece"
                  r"|Sweden|Norway|Finland|Denmark|Germany|France|Spain|Italy"
                  r"|Portugal|Netherlands|Belgium|Austria|Switzerland|Poland"
                  r"|Czechia|Hungary|Croatia|Korea|Japan|India|Pakistan|Brazil"
                  r"|Argentina|Chile|Peru|Colombia|Canada|USA|US")
_PLUS8_IANA = (r"Asia/(?:Manila|Shanghai|Singapore|Hong_Kong|Kuala_Lumpur|Taipei"
               r"|Macau|Chongqing|Harbin|Brunei|Makassar)|Australia/Perth")
_UNKNOWN_ZONE_AFTER2_RE = re.compile(
    r"[ \t(（\[]*(?:"
    r"(?!(?:Start|End|Maintenance|Local|Update|Down|Up|Restore|Recovery|Completion"
    r"|Estimated|Expected|Resume|Open|Close|Opening|Closing|Finish|Begin"
    r"|Beginning|Effective|Report|Check|Total|New|Old|Original|Next|Last|Final"
    r"|Manila|Singapore|Beijing|Hong|HK|Taipei|Philippine|Philippines|China"
    r"|Malaysia|Macau|Real|Response|Run|Lead|Wait|Service|Working|Office"
    r"|Business)\b)"
    r"(?:[A-Z]{2,3}|[A-Z][a-z]+)(?:[ \t]+[A-Z][a-z]+){0,3}[ \t]+[Tt]ime\b"
    r"|(?:" + _FOREIGN_PLACE + r")(?=[ \t]*[)）\]])"
    r"|(?!(?:" + _PLUS8_IANA + r")\b)(?:Europe|America|Asia|Africa|Australia"
    r"|Pacific|Atlantic|Indian|Antarctica)/[A-Za-z_]+"
    r"|(?:ET|CT|MT|PT|Z|WEST)\b(?![:\-])"
    r"|(?:server|system|platform|our|provider|studio|your|their)[ \t]+time\b)")


_ZONE_GROUP_RE = re.compile(r"[ \t]*[(（\[]?[ \t]*[A-Za-z0-9+:.\-/,，&、| \t]{2,40}?[)）\]]?(?=[ \t]*(?:\n|$|[.。;；]))")


def _unknown_zone_after(text: str, pos: int, rspans=()):
    """The unknown zone word straight after position ``pos``, or None.

    Not when the word is followed by a zone the table DOES know before the
    next range starts: "10:00 - 12:00 Vietnam time (GMT+7)" states its offset.
    """
    m = (_UNKNOWN_ZONE_AFTER_RE.match(text, pos)
         or _UNKNOWN_ZONE_AFTER2_RE.match(text, pos))
    if not m:
        # R1.77: "10:00 - 12:00 (GMT+8 / CEST)" labels ONE range with two zones
        # that disagree (+08 and +02). The known member won and the row got +08
        # with nothing to say the notice contradicts itself.
        g = _ZONE_GROUP_RE.match(text, pos)
        # _FOREIGN_ABBR is defined further down; re caches the compiled form.
        f = re.search(r"\b(?:" + _FOREIGN_ABBR + r")\b", g.group(0)) if g else None
        return f.group(0) if f else None
    _ls, le = _line_at(text, m.end())
    stop = min([le] + [a for a, _b in rspans if a >= m.end()])
    # Scanned from the word itself, so a zone later added to _TZ_NAMES / _TZ_RE
    # stops counting as unknown without anyone having to edit this rule.
    for z in _TZ_RE.finditer(text, pos, stop):
        if _offset_of(z) is not None:
            return None
    return m.group(0).strip(" \t([（")


# The prefix style: "CEST 10:00 - 12:00", "(CET) 04:00-06:00", "美国东部时间
# 22:00-23:00". Stamped with NOTICE_TZ it was written on the wrong hours with
# nothing on the page to say so (F16). Only real zone abbreviations count
# here - a word in front of a range is far more often a label ("START 10:00")
# than a zone, so the generic [A-Z]{2,4}T of the suffix rule is not used.
# (CAT, EAT, ART and WEST are zones too, but far commoner as words.)
_FOREIGN_ABBR = (r"CET|CEST|WET|EET|EEST|BST|MSK|AEST|AEDT|ACST|ACDT"
                 r"|NZST|NZDT|AST|ADT|CDT|MST|MDT|HST|AKST|AKDT|BRT"
                 r"|SAST|TRT|IRST|GST|PKT|NPT|BDT|MMT")
# An English "<Place> time" counts here only in brackets - "(Malta time)
# 10:00" - because unbracketed in front of a clock it is usually a label
# ("Start time 10:00").
_UNKNOWN_ZONE_BEFORE_RE = re.compile(
    r"(?:^|(?<=[\s(（\[:：]))(?P<w>\b(?:" + _FOREIGN_ABBR + r")\b|"
    + _ZH_FOREIGN_ZONE + r"|(?<=[(（\[])[A-Z][a-z]+\s+time\b(?=\s*[)）\]]))"
    r"[\s)）\]]*[:：]?[ \t]*(?:(?i:between|from|at)[ \t]+)?$")


def _unknown_zone_before(text: str, pos: int, rspans=()):
    """The unknown zone word straight in front of position ``pos``, or None.

    A word the zone table knows ("(Bangkok time) 10:00") is not unknown.
    """
    ls, _le = _line_at(text, pos)
    seg = max([ls] + [b for _a, b in rspans if b <= pos])
    m = _UNKNOWN_ZONE_BEFORE_RE.search(text, seg, pos)
    if not m:
        return None
    for z in _TZ_RE.finditer(text, m.start("w"), m.end("w")):
        if _offset_of(z) is not None:
            return None
    return m.group("w")


# A notice that names its zone on a line of its own: "Time zone: CEST",
# "时区：欧洲中部时间". A range with no zone on its own line took NOTICE_TZ in
# its place (F16). A value the table knows is a token like any other; one it
# does not know holds back every window that would have fallen back.
_ZONE_LABEL_RE = re.compile(
    r"(?im)^[^\w\n]*(?:time[ \t]*-?[ \t]*zone|timezone|时区|時區)[ \t]*[:：]"
    r"[ \t]*(?P<v>\S[^\n]{0,39}?)[ \t]*$")


def _zone_label_doubts(text: str, tokens):
    """[(start, end, reason)] for each zone label whose value is not a zone."""
    out = []
    for m in _ZONE_LABEL_RE.finditer(text):
        a, b = m.start("v"), m.end("v")
        if any(a <= s < b for s, _e, _o in tokens):
            continue
        out.append((a, b, "the notice gives its time zone as '{}', which the "
                    "parser does not know".format(m.group("v"))))
    return out


def _zone_doubt(text: str, span, zdoubts, tokens):
    """Why this range's zone cannot be trusted, or None.

    A doubt on the range's own line(s) holds it back. So does any doubt in the
    notice when those lines state no zone of their own, because the range
    then takes the document's zone - the very thing in doubt.
    """
    if not zdoubts:
        return None
    ls = _line_at(text, span[0])[0]
    le = _line_at(text, max(span[0], span[1] - 1))[1]
    mine = [d for d in zdoubts if ls <= d[0] < le]
    if mine:
        return mine[0][2]
    if any(ls <= a < le for a, _b, _o in tokens):
        return None
    return zdoubts[0][2]


# Two "<date> <time>" points form one window when only a connector stands
# between them - once any zone written against either end is set aside.
# "2026-09-24 22:00 (GMT+8) - 2026-09-25 02:00 (GMT+8)" and "... 22:00 UTC+8
# to ... 02:00 UTC+8" put a zone in that gap, and the test used to demand the
# connector ALONE, so the most explicit overnight notice there is was dropped
# (F43).
# F33: never across a line end - a "- " bullet opening the next line paired
# "2026-09-24 02:00" with the next line's "2026-09-24 22:00" into one long range.
_POINT_GAP_RE = re.compile(r"[ \t]*" + _DASH + r"[ \t]*")


# A zone the table does not know, anywhere in such a gap: it is blanked so the
# two points still pair ("22:00 CEST - 2026-09-25 02:00 CEST"), and the window
# then carries it as zone_unknown - a person reads it, rather than nobody.
_FOREIGN_ZONE_RE = re.compile(r"\b(?:" + _FOREIGN_ABBR + r")\b|" + _ZH_FOREIGN_ZONE)


def _gap_text(text: str, a: int, b: int, tokens) -> str:
    """text[a:b] with every zone in it blanked and the brackets that held it dropped."""
    chars = list(text[a:b])
    for s, e, _o in tokens:
        for i in range(max(s, a), min(e, b)):
            chars[i - a] = " "
    gap = _FOREIGN_ZONE_RE.sub(lambda m: " " * len(m.group(0)), "".join(chars))
    return re.sub(r"[(（\[]\s*[)）\]]", " ", gap)


def _labelled_pair(text: str, a, b, tokens) -> bool:
    """Are points ``a`` and ``b`` a Start line followed by an End line?"""
    la, _lae = _line_at(text, a[0])
    lb, _lbe = _line_at(text, b[0])
    if lb <= la or a[1] > lb:
        return False
    if _gap_text(text, a[1], lb, tokens).strip():
        return False
    return bool(_PAIR_START_RE.fullmatch(_gap_text(text, la, a[0], tokens))
                and _PAIR_END_RE.fullmatch(_gap_text(text, lb, b[0], tokens)))


_CLOCK_AT_RE = re.compile(
    r"(?:" + _NEXT_DAY + r"\s*)?(?:" + _ZH_MER + r"\s*)?\d{1,2}:\d{2}(?::\d{2})?"
    r"(?!\d)(?:\s*" + _AMPM_WORD + r")?")


def _clock_spans(text: str, a: int, b: int):
    """The CLOCK parts of the range text[a:b].

    One span for an ordinary range. A Start / End pair (_RANGE_LABELLED_RE)
    runs over two lines, and whatever stands between its clocks - "(GMT+9)"
    or "CEST" after the Start clock, the End label - is not part of either:
    treated as range text, the Start line's zone was skipped as if it were a
    range separator and the window took NOTICE_TZ in its place.
    """
    if "\n" not in text[a:b]:
        return [(a, b)]
    first = _CLOCK_AT_RE.match(text, a)
    ls2 = text.rfind("\n", a, b) + 1
    second = _CLOCK_AT_RE.search(text, ls2, b)
    if not (first and second):
        return [(a, b)]
    return [(a, first.end()), (second.start(), b)]


#: Pieces of ONE outage: two windows whose gap is at most this are unioned
#: (corpus I6 - Slots 10:00-12:00, Live Casino 14:00-16:00 - is the widest
#: pinned). Anything further apart is a separate outage and is never merged.
_PIECE_GAP = timedelta(hours=2)


def _max_window():
    """NOTICE_MAX_WINDOW_HOURS as a timedelta, or None when switched off (0).

    A written window longer than this goes to a human instead. 48h is the
    ceiling a real maintenance notice from these providers reaches (an
    overnight migration across a weekend day), and it is well below a promo,
    tournament or holiday period - the ten-day "Mid-Autumn promotion" and the
    seven-day Golden Week pause both used to be written as outages. It is the
    backstop behind the relevance and piece rules, not a substitute for them.
    """
    raw = _flag("NOTICE_MAX_WINDOW_HOURS", "48")
    try:
        h = float(raw)
    except ValueError:
        h = -1.0
    # Only 0 (the documented "no cap") or a real number of hours is honoured.
    # "inf" and "1e30" made timedelta() raise inside classify() on every
    # message, and "-1" or "nan" failed the h > 0 test and switched the cap OFF
    # without a word. Anything outside 0..10000 keeps the default and is logged.
    if not 0 <= h <= 10000:
        _warn_config("NOTICE_MAX_WINDOW_HOURS",
                     "NOTICE_MAX_WINDOW_HOURS={!r} is not a number of hours "
                     "from 0 to 10000 - using 48".format(raw))
        h = 48.0
    return timedelta(hours=h) if h > 0 else None


def _score(cand) -> int:
    """Rank one candidate window. Explicit weights, in policy order.

    Deliberately a handful of commented additions rather than a heuristic pile:
    this decides what gets written to a shared sheet unattended, so an operator
    asking "why did it pick that one" has to be able to read the answer. The
    old letterhead weight is gone because a letterhead date is now never lent
    to a range at all (_bind_date), which is stronger than out-ranking it: a
    single-candidate notice used to fill with the letterhead date anyway.
    """
    return ((0 if cand["superseded"] else 8)      # a. not a withdrawn window
            + (0 if cand["past"] else 4)          # b. not last week's, in passing
            + (1 if cand["future"] else 0))       # c. has not already ended


def _collect(text: str, now: datetime, *, moves: bool = False):
    """-> (candidates, dropped).

    ``candidates`` are the windows that belong to the maintenance statement,
    each with the flags _score and _resolve read; ``dropped`` counts readable
    time ranges that did NOT become one (somebody else's hours, or no date
    that belongs to them). classify's "no maintenance" answer needs the second
    number: a message that carries any range at all must not be stamped
    "No maintenance" on the strength of a regex.
    """
    tzdef = _tz_default()
    doubts = {}
    dates = _dates(text, now=now, tz=tzdef, doubts=doubts)
    if not dates:
        # Every readable range is one that did NOT become a window. Returning
        # 0 here let "No maintenance this week. Emergency maintenance tomorrow
        # 10:00-12:00." be stamped "No maintenance" - the range has no date,
        # so it looked as if the message carried none.
        return [], len(_time_ranges(text, []))
    sup_dates = _superseded_dates(text, dates)
    spans = _date_spans(text, dates, sup_dates)
    # A span's connector and shorthand half ("23-", "-25日", "- 25, 2026") are
    # part of the date, not words of the sentence: masked with it, so a
    # "Date: 23-24 Sep 2026" line still reads as a bare date line.
    dspans = [(a, b) for a, b, _d in dates] + sorted(
        {sp["at"] for sp in spans.values()})
    doubt_of = {i: doubts[p] for i, (p, _e, _d) in enumerate(dates)
                if p in doubts}
    ranges = _time_ranges(text, dspans)
    rspans = [(a, b) for a, b, _t1, _t2 in ranges]
    zdoubts = []
    tokens = _tz_tokens(text, [c for a, b in rspans
                               for c in _clock_spans(text, a, b)], zdoubts)
    zdoubts += _zone_label_doubts(text, tokens)
    zdoubts.sort()
    regions = _superseded_regions(text)
    pts = _datetime_points(text, dates)
    # Only the CLOCK of a "<date> <time>" point is masked, never the words a
    # point may span ("9月23日例行维护 10:00"): masking those hid the very
    # maintenance word that makes the range relevant.
    clocks = []
    for q in pts:
        tm = re.search(r"\d{1,2}:\d{2}(?::\d{2})?\s*(?:" + _AMPM_WORD + r")?\s*$",
                       text[q[0]:q[1]])
        if tm:
            clocks.append((q[0] + tm.start(), q[1]))
    # A zone the table does not know is masked like one it does: it is zone
    # glue, not a word of the sentence, and counted as a word it pushed a
    # "Start: <date> CEST" / "End: <date> CEST" pair out of the data-line
    # shape, so the window was dropped instead of reaching a person.
    masked = _mask(_mask(text, dspans + rspans + clocks),
                   [(a, b) for a, b, _o in tokens]
                   + [m.span() for m in _FOREIGN_ZONE_RE.finditer(text)],
                   _MASK_TZ)

    # A letterhead date is the one whose OWN label is "Issued:"/"發佈日期：" -
    # the label straight in front of it, with no other date between. Testing
    # the whole line flagged "Issued: 2026-09-23. Scheduled maintenance 10:00
    # - 12:00 on 2026-09-25." as letterhead twice and threw the 25th away.
    letterhead = set()
    for i, (p, _e, _d) in enumerate(dates):
        head = _clause_before(masked, p, _SENT_BOUND)
        m = None
        for m in _LETTERHEAD_RE.finditer(head):
            pass
        if m and _MASK not in head[m.end():] and len(head) - m.end() <= 16:
            letterhead.add(i)
    ineligible = {i for i, (p, e, _d) in enumerate(dates)
                  if _relevant(masked, p, e, dates_only=True) is None}

    # F52: the superseded marker is read only in the window's own LEAD-IN -
    # from the end of the previous date or range, not from the clause start.
    # "moved from 2026-09-23 to 2026-09-25 10:00-12:00" has no comma, so the
    # clause of the 10:00 range carried "moved from" and the NEW window was
    # flagged superseded (needs_human, "the only window found is the
    # superseded one"). The OLD date is still caught by its own lookback
    # (_superseded_dates), which stops at the previous date the same way.
    tok_ends = sorted([e for _p, e, _d in dates] + [b for _a, b in rspans])

    def _flags(span, date_idx):
        a, _b = span
        in_region = any(a >= s and a < e for s, e in regions)
        clause = _clause_before(text, a)
        prev = max((e for e in tok_ends if e <= a), default=0)
        lead = text[max(a - len(clause), prev):a]
        superseded = in_region or bool(_SUPERSEDED_CLAUSE_RE.search(lead))
        if date_idx is not None and date_idx in sup_dates:
            superseded = True
        past = bool(_PAST_CLAUSE_RE.search(clause))
        return superseded, past

    def _cand(span, start, end, di, own, far, amb, rel, doubt=None):
        superseded, past = _flags(span, di)
        return {"span": span, "start": start, "end": end,
                "superseded": superseded, "past": past,
                "future": end >= now, "di": di, "own": own, "far": far,
                "ambiguous": amb, "named": rel == "named",
                "zone_unknown": next((z for cs, ce in _clock_spans(text, *span)
                                      for z in (_unknown_zone_after(text, ce,
                                                                    rspans),
                                                _unknown_zone_before(text, cs,
                                                                     rspans))
                                      if z), None),
                # A zone that IS stated but cannot be pinned to one offset
                # (PST/CST/IST nothing resolves, a signless "(UTC 02:00)"
                # after a lone clock, a "Time zone:" line naming an unknown
                # zone) - see _zone_doubt. Notice-wide, like zone_unknown.
                "zone_doubt": _zone_doubt(text, span, zdoubts, tokens),
                # Why this window's DATE cannot be trusted (a contradicting
                # weekday, a bare pair that reads both ways, a range that does
                # not fit its date span...). Unlike ``ambiguous`` it holds the
                # whole notice back when ANY live candidate carries it - see
                # _resolve - because a misread date also misranks the window.
                "doubt": doubt,
                "line": _line_at(text, span[0])[0]}

    cands = []
    taken = []
    dropped = 0
    listed = {}         # id(date list) -> [the candidates of each range on it]

    # A window with a date on BOTH ends is unambiguous - collect it first so the
    # single-date rules cannot re-read half of it.
    for a, b in zip(pts, pts[1:]):
        if not (_POINT_GAP_RE.fullmatch(_gap_text(text, a[1], b[0], tokens))
                or _labelled_pair(text, a, b, tokens)):
            continue
        span = (a[0], b[1])
        tz = _tz_for(text, span, tokens, rspans)
        # The same one-sided-meridiem rule as a plain range (_readings):
        # "2026-10-14 8:00 - 2026-10-14 11:00 PM" is 20:00-23:00, not 15h.
        best = None
        for t1, t2 in _readings((a[3], a[4], a[5]), (b[3], b[4], b[5])):
            try:
                sd, sh, sm = _clock(*t1)
                ed, eh, em = _clock(*t2, end=True)
                st = datetime(*a[2], sh, sm, tzinfo=tz) + timedelta(days=sd)
                en = datetime(*b[2], eh, em, tzinfo=tz) + timedelta(days=ed)
            except (ValueError, OverflowError):
                continue
            if en > st and (best is None or en - st < best[1] - best[0]):
                best = (st, en)
        if best is None:
            continue
        start, end = best
        taken.append(span)
        di = next((i for i, (p, _e, _d) in enumerate(dates) if p == a[0]), None)
        dj = next((i for i, (p, _e, _d) in enumerate(dates) if p == b[0]), None)
        rel = _relevant(masked, *span)
        if di in letterhead or rel is None:
            dropped += 1
            continue
        # R1.25: "2026-09-24 22:00 (GMT+8) - 2026-09-25 02:00 (GMT+7)" and
        # "Start: … (UTC)\nEnd: … (GMT+8)" give the two ENDS different zones.
        # Stamping both with one put the start an hour (or eight) off.
        ends_le = text.find("\n", b[1])
        ends_le = len(text) if ends_le == -1 else ends_le
        offs = {o for p0, _e0, o in tokens if span[0] <= p0 < min(ends_le, b[1] + 16)}
        split_zone = ("the two ends of the window are stated in different time "
                      "zones" if len(offs) > 1 else None)
        c = _cand(span, start, end, di, True, False, None, rel,
                  doubt_of.get(di) or doubt_of.get(dj) or split_zone)
        foreign = _FOREIGN_ZONE_RE.search(text, a[1], b[0])
        if foreign and not c["zone_unknown"]:
            c["zone_unknown"] = foreign.group(0)
        cands.append(c)

    for a, b, t1, t2 in ranges:
        if any(a < e and b > s for s, e in taken):
            continue
        rel = _relevant(masked, a, b)
        if rel is None:
            dropped += 1
            continue
        di, own, far, amb = _bind_date(text, masked, a, b, dates, sup_dates,
                                       letterhead, ineligible,
                                       [r[0] for r in rspans])
        if di is None:
            dropped += 1
            continue
        tz = _tz_for(text, (a, b), tokens, rspans)
        sp = spans.get(di)
        doubt = doubt_of.get(di)
        # F43: "Start: 10:00 (UTC)\nEnd: 18:00 (GMT+8)" - the two ends of one
        # labelled range carry different zones (R1.25's point-pair check,
        # for the time-only form). One zone stamped on both wrote 10:00-18:00 +08.
        rle = text.find("\n", b)
        rle = len(text) if rle == -1 else rle
        if len({o for p0, _e0, o in tokens if a <= p0 < min(rle, b + 16)}) > 1:
            doubt = doubt or ("the two ends of the window are stated in "
                              "different time zones")
        if sp and sp.get("kind") == "list":
            # A date LIST: one window per day it names, whichever day the
            # range bound to (it used to be the nearest - the LAST - so the
            # first day's outage never reached the row).
            doubt = doubt or next((doubt_of[j] for j in sp["members"]
                                   if j in doubt_of), None)
            got = []
            for ymd in sp["days"]:
                try:
                    start, end = _build(ymd, t1, t2, tz)
                except ValueError:
                    continue
                got.append(dict(_cand((a, b), start, end, di, own, far, amb,
                                      rel, doubt), list_day=ymd))
            if got:
                listed.setdefault(id(sp), []).append(got)
                cands.extend(got)
            continue
        try:
            # One half of a date span: the window starts on the span's FIRST
            # day, whichever half the range happened to bind to.
            start, end = _build(sp["lo"] if sp else dates[di][2], t1, t2, tz)
        except ValueError:
            continue
        doubt = doubt or _rollover_doubt(text[a:b], start, end)
        if sp:
            doubt = (doubt or next((doubt_of[j] for j in sp["members"]
                                    if j in doubt_of), None)
                     or _span_doubt(sp, start, end))
        cands.append(_cand((a, b), start, end, di, own, far, amb, rel, doubt))

    cands.sort(key=lambda c: c["span"][0])
    if moves:
        # A reschedule or correction: flag the window it moves AWAY from
        # (see _mark_moved) before any copy/sibling rule reads the flags.
        _mark_moved(text, masked, cands, dates)
    _merge_zone_copies(cands, now, dates)
    _confirm_foreign_copies(cands, now)
    for per_range in listed.values():
        # Two or more DIFFERENT ranges under one list ("24 & 25 Sep: Slots
        # 10:00-12:00, Live 14:00-16:00") may apply to every day or pair up
        # day by day ("respectively"); the text does not say which. The same
        # range printed in two zones is one instant, so it does not count -
        # which is why this runs after _merge_zone_copies has put a copy
        # that crossed midnight back on its owner's instant.
        if len({(g[0]["start"], g[0]["end"]) for g in per_range}) > 1:
            why = ("the notice lists {} days and {} different time ranges "
                   "without saying which range falls on which day".format(
                       len(per_range[0]), len(per_range)))
            for c in (c for g in per_range for c in g):
                c["doubt"] = c["doubt"] or why
    _flag_overnight_siblings(cands)
    _flag_two_zone_lines(text, cands, tokens)
    _flag_meridiem_twins(cands)
    return cands, dropped


def _flag_meridiem_twins(cands):
    """The same window stated twice, exactly 12 hours apart, is one misread.

    "维护时间：2026年9月23日 下午2:00-4:00 / Maintenance time: Sep 23, 2026
    2:00PM-4:00PM" is ONE window in two languages; the Chinese copy loses its
    下午 and reads 02:00-04:00. The union used to hide that inside 02:00-16:00;
    with separate windows kept apart, the earliest-first rule would write the
    misread copy alone. Same zone, same length, same day, starts exactly 12h
    apart: the text does not say which copy is right, so neither is written.
    """
    half = timedelta(hours=12)
    live = [c for c in cands if not (c["superseded"] or c["past"])]
    for i, a in enumerate(live):
        for b in live[i + 1:]:
            if a["start"].utcoffset() != b["start"].utcoffset():
                continue
            if a["end"] - a["start"] != b["end"] - b["start"]:
                continue
            if a["start"].date() != b["start"].date():
                continue
            if abs(b["start"] - a["start"]) != half:
                continue
            why = ("the notice gives the same window twice, 12 hours apart "
                   "- one copy misreads AM/PM")
            a["ambiguous"] = a["ambiguous"] or why
            b["ambiguous"] = b["ambiguous"] or why


def _merge_zone_copies(cands, now, dates=None):
    """Put a converted copy of a window on the SAME instant as the window.

    "Date: 2026-09-23 / Time: 01:00 - 03:00 (GMT+8) / UTC: 17:00 - 19:00" is
    one outage said twice, but the UTC line has no date of its own and took the
    notice's: 09-23 17:00 UTC, which is exactly 24h after the real window. The
    union then wrote 26 hours, and once the real window had passed the copy
    alone was still "future" and wrote a phantom 09-24 window the stale guard
    could not see. Any conversion that crosses midnight lands exactly one day
    out, so the rule is exact rather than approximate: two candidates that
    share a date, have the same length and different offsets, and start
    exactly 24h apart are the same window, and the copy (the one the date is
    NOT written against; failing that, the later in the text) is moved onto
    the owner's instant, where _one_per_instant then collapses it.
    """
    day = timedelta(days=1)
    for i, b in enumerate(cands):
        for a in cands[:i]:
            if a["di"] is None or a["di"] != b["di"]:
                continue
            # Under a date LIST every day shares the one date index, and day
            # 25's window sits exactly 24h after day 24's UTC copy - which
            # was "merged" onto the wrong day. A copy is its own day's copy.
            if a.get("list_day") != b.get("list_day"):
                continue
            if a["start"].utcoffset() == b["start"].utcoffset():
                continue
            if a["end"] - a["start"] != b["end"] - b["start"]:
                continue
            if abs(b["start"] - a["start"]) != day:
                continue
            owner, copy = (b, a) if (b["own"] and not a["own"]) else (a, b)
            if copy["own"]:
                continue
            if (not owner["own"] and dates and a["di"] is not None
                    and dates[a["di"]][0] > max(a["span"][0], b["span"][0])):
                # R1.12: neither line owns the date and the date is written
                # BELOW both ("Time (GMT+8): 01:00-03:00\nTime (UTC): 17:00-
                # 19:00\nDate: 2026-09-23"). Above both, it is the first
                # (local) line's date by convention; below, nothing says which
                # zone it is in, and the two readings are a day apart.
                why = ("the date is written after two lines in different zones "
                       "and could belong to either, so the window's day is "
                       "not certain")
                a["doubt"] = a.get("doubt") or why
                b["doubt"] = b.get("doubt") or why
                break
            shift = owner["start"] - copy["start"]
            copy["start"] += shift
            copy["end"] += shift
            copy["future"] = copy["end"] >= now
            break


# What each unknown zone abbreviation denotes BY DEFINITION - used only to
# recognise a conversion copy (_confirm_foreign_copies), never to write a
# window. "CEST" is +02 by definition, but a studio that writes "CET" all
# summer means +02 too, so a window stated ONLY in such a zone still goes to
# a person. Several readings where the letters are shared (BST: British
# Summer Time or Bangladesh).
_FOREIGN_DEFINED = {
    "CET": (60,), "CEST": (120,), "WET": (0,), "EET": (120,), "EEST": (180,),
    "BST": (60, 360), "MSK": (180,), "AEST": (600,), "AEDT": (660,),
    "ACST": (570,), "ACDT": (630,), "AWST": (480,), "NZST": (720,),
    "NZDT": (780,), "AST": (-240, 180), "ADT": (-180,), "CDT": (-300,),
    "MST": (-420,), "MDT": (-360,), "HST": (-600,), "AKST": (-540,),
    "AKDT": (-480,), "BRT": (-180,), "SAST": (120,), "TRT": (180,),
    "IRST": (210,), "GST": (240,), "PKT": (300,), "NPT": (345,),
    "BDT": (360,), "MMT": (390,),
}


def _confirm_foreign_copies(cands, now):
    """A range in an unknown zone that is exactly a known window, restated.

    "Time: 10:00 - 12:00 (GMT+8)" / "For European partners: 04:00 - 06:00
    CEST." is one outage said twice. The CEST range cannot be written - the
    parser does not keep zones with daylight saving - but read at CEST's
    defined +02 it is the same instant, the same length, as the GMT+8 window,
    so it is that window's copy and not a second window in an unknown zone:
    it is moved onto that instant (where _one_per_instant collapses it) and
    no longer holds the notice back. A range that matches no known window
    under any defined reading - a "CET" that meant summer time, or a window
    stated only in CEST - keeps its zone_unknown and goes to a person.
    """
    known = [c for c in cands if not c["zone_unknown"]]
    for c in cands:
        offs = _FOREIGN_DEFINED.get(c["zone_unknown"] or "")
        if not offs:
            continue
        s0, e0 = c["start"].replace(tzinfo=None), c["end"].replace(tzinfo=None)
        for off in offs:
            z = timezone(timedelta(minutes=off))
            s, e = s0.replace(tzinfo=z), e0.replace(tzinfo=z)
            twin = next((k for k in known if e - s == k["end"] - k["start"]
                         and abs(k["start"] - s) in (timedelta(0),
                                                     timedelta(days=1))), None)
            if twin is not None:
                c["start"], c["end"] = twin["start"], twin["end"]
                c["future"] = c["end"] >= now
                c["zone_unknown"] = None
                break


def _flag_overnight_siblings(cands):
    """A product row after an overnight row, under one date, is either day.

    "Scheduled maintenance on 2026-09-23 / Slots: 23:00 - 01:00 / Live Casino:
    00:30 - 02:00" - the Live Casino row most likely continues the Slots night
    (09-24 00:30), but it may equally be the early hours of the 23rd, and the
    date on the page does not say. Bound to the 23rd it was unioned into a
    24.5h window; on its own it would be written on a day nobody can confirm.
    Both rows are marked ambiguous so a human reads the notice.
    """
    day = timedelta(days=1)
    for a in cands:
        if not a["far"] or a["past"] or a["superseded"]:
            continue
        if a["end"].astimezone(a["start"].tzinfo).date() <= a["start"].date():
            continue
        for b in cands:
            if b is a or not b["far"] or b["di"] != a["di"]:
                continue
            if b["end"].astimezone(b["start"].tzinfo).date() != b["start"].date():
                continue
            if b["start"] >= a["start"]:
                continue
            if b["start"] + day <= a["end"] and b["end"] + day >= a["start"]:
                why = ("a row listed under the same date as an overnight row "
                       "could fall on either day")
                a["ambiguous"] = a["ambiguous"] or why
                b["ambiguous"] = b["ambiguous"] or why


def _flag_two_zone_lines(text, cands, tokens):
    """A line naming two zones must describe ONE window, or nobody can say which.

    _tz_for binds each range on such a line to its own zone token, and a line
    that restates one window in two zones then collapses to one instant. When
    it does NOT - the provider's arithmetic is off, or the line mixes prefix
    and suffix zones so a range took the wrong token - the ranges are marked
    ambiguous: two different instants off one line is exactly the shape the
    old first-token rule turned into a 10-hour window.
    """
    by_line = {}
    for c in cands:
        if not (c["superseded"] or c["past"]):
            # One range under a date LIST is one window per listed day, all
            # on one line and all legitimately different - compare the zone
            # copies of each day with each other, not the days.
            by_line.setdefault((c["line"], c.get("list_day")), []).append(c)
    for (ls, _day), group in by_line.items():
        if len(group) < 2:
            continue
        le = text.find("\n", ls)
        le = len(text) if le == -1 else le
        offs = {off for a, _b, off in tokens if ls <= a < le}
        if len(offs) < 2:
            continue
        if len({(c["start"], c["end"]) for c in group}) > 1:
            for c in group:
                c["ambiguous"] = c["ambiguous"] or (
                    "one line states the window in two zones that do not "
                    "describe the same time")


def _one_per_instant(cands):
    """Collapse candidates that denote the SAME instant, keeping the home clock.

    A notice that prints one outage twice - a UTC line above a GMT+8 line -
    yields two candidates that are the same window said twice. The Base cannot
    tell them apart (what reaches it is epoch ms via vawatch._ms), but the
    Laboratory card prints the parsed LOCAL time, so the copy written in the
    zone the sheet is actually read in is the one a duty engineer can act on
    without doing arithmetic in his head. Everything else keeps document order.
    """
    home = _tz_default()

    def _is_home(c):
        return c["start"].utcoffset() == c["start"].astimezone(home).utcoffset()

    best = {}
    for c in cands:
        key = (c["start"], c["end"])
        cur = best.get(key)
        if cur is None or (_is_home(c) and not _is_home(cur)):
            best[key] = c
    return sorted(best.values(), key=lambda c: c["span"][0])


# F16: a footer that sets the zone for EVERY time in the notice - "All times in
# CEST", "All times are Malta time.", "(times in CEST)", "以上时间均为北京时间".
# It stands on its own line, far from any range, so _tz_for never tied it to
# the window, which was read on NOTICE_TZ: a 10:00 CEST outage went onto the
# row as 10:00 +08, six hours early.
_ALL_TIMES_RE = re.compile(
    r"(?:\ball\s+times?\b[ \t]*(?:(?:are|is|shown|listed|stated|given|quoted)[ \t]+){0,2}"
    r"(?:in|are|as)?[ \t]*|\btimes?\s+(?:are\s+|shown\s+|listed\s+)?in\b[ \t]*"
    r"|(?:所有|以上|上述|以下)?时间(?:均|皆|都)(?:为|是|按|以)|(?:所有|以上|上述)時間(?:均|皆|都)(?:為|是|按|以))"
    r"(?P<z>[^\n.。;；)）]{2,40})", re.I)


def _all_times_doubt(text: str, detail):
    """A reason when a notice-wide "All times in <zone>" contradicts the window."""
    for m in _ALL_TIMES_RE.finditer(text):
        zs, ze = m.start("z"), m.end("z")
        toks = [off for s, e, off in _tz_tokens(text, ()) if zs <= s < ze]
        if toks:
            want = {timedelta(minutes=o) for o in toks}
            got = {detail["start"].utcoffset(), detail["end"].utcoffset()}
            if len(want) > 1 or got != want:
                return ("the notice says all times are in {}, but the window was "
                        "read in another zone".format(m.group("z").strip()))
            continue
        if _UNKNOWN_ZONE_AFTER2_RE.match(" " + m.group("z") + ")") or re.match(
                r"[ \t]*[A-Z]{3,5}\b", m.group("z")):
            return ("the notice says all times are in {}, a zone the parser "
                    "does not know".format(m.group("z").strip()))
    return None


def _resolve(cands, now=None):
    """Pick the window the row should carry.

    -> {start, end, superseded, at, others, ambiguous} or None. ``at`` is where
    in the message the window was read from, because the completion scoping in
    classify() has to know, and recomputing it separately is how the two
    halves of one decision drift apart. ``others`` are the further upcoming
    windows the notice states and the row does NOT get, as (start, end, at);
    ``ambiguous`` is a reason when the chosen window must not be written.

    best (the default): the top-scoring candidates, one per instant, are
    grouped into pieces of one outage - windows that overlap, touch, or leave
    a gap of at most _PIECE_GAP - and a group becomes its UNION (two product
    lines, corpus I6). Separate groups are separate outages and are NEVER
    merged: the row takes the earliest one still in the future and the rest go
    to ``others``. The old rule merged everything whose START fell within 24h
    of the first, which joined two nightly 02:00-04:00 outages into one 26h
    window that nobody announced.
    """
    if not cands:
        return None
    if _flag("NOTICE_WINDOW_PICK", "best") == "first":
        # Pre-2026-09 behaviour, kept switchable: the first range in the
        # document wins outright, superseded regions aside.
        live = [c for c in cands if not c["superseded"]] or cands
        chosen = [live[0]]
        rest = _one_per_instant([c for c in live[1:]
                                 if c["future"] and not c["past"]])
        others = [(c["start"], c["end"], c["span"][0]) for c in rest
                  if (c["start"], c["end"]) != (live[0]["start"], live[0]["end"])]
    else:
        top = max(_score(c) for c in cands)
        live = _one_per_instant([c for c in cands if _score(c) == top])
        groups = []
        for c in sorted(live, key=lambda c: (c["start"], c["span"][0])):
            if groups and c["start"] <= max(x["end"] for x in groups[-1]) + _PIECE_GAP:
                groups[-1].append(c)
            else:
                groups.append([c])
        chosen = next((g for g in groups if any(c["future"] for c in g)),
                      groups[0])
        later = [g for g in groups
                 if g is not chosen and any(c["future"] for c in g)]
        others = [(min(c["start"] for c in g), max(c["end"] for c in g),
                   min(c["span"][0] for c in g)) for g in later]
        if (not any(c["named"] for c in chosen)
                and any(c["named"] for g in later for c in g)):
            # "Scheduled maintenance 09-23 10:00-12:00. Backoffice will be
            # unavailable 09-22 22:00-23:00." The earliest window is only
            # DESCRIBED (a subsystem "will be unavailable") and the NAMED
            # maintenance is a later, separate one. Which of the two the row
            # should carry is not in the text: the earliest-first rule wrote the
            # backoffice hour and the games outage never reached the row.
            chosen = [dict(c, ambiguous=c["ambiguous"] or (
                "the earliest window is only described as an outage and the "
                "named maintenance is a separate, later window"))
                for c in chosen]

    start = min(c["start"] for c in chosen)
    end = max(c["end"] for c in chosen)
    ambiguous = next((c["ambiguous"] for c in chosen if c["ambiguous"]), None)
    if ambiguous is None:
        # Notice-wide, like the unknown zone below: a window whose date was
        # misread is also mis-RANKED - "10/7" read as 10 July is stale, so a
        # later window would be chosen in its place - so a doubtful date
        # anywhere in the live windows is not something the others can outvote.
        shaky = next((c for c in cands if c.get("doubt")
                      and not (c["superseded"] or c["past"])), None)
        if shaky is not None:
            ambiguous = shaky["doubt"]
    if ambiguous is None:
        odd = next((c for c in cands if c["zone_unknown"]
                    and not (c["superseded"] or c["past"])), None)
        if odd is not None:
            ambiguous = ("a window is stated in a zone the parser does not know "
                         "({}), so its hours cannot be trusted".format(
                             odd["zone_unknown"]))
    if ambiguous is None:
        odd = next((c for c in cands if c.get("zone_doubt")
                    and not (c["superseded"] or c["past"])), None)
        if odd is not None:
            ambiguous = ("the window's time zone cannot be pinned down: "
                         + odd["zone_doubt"])
    if ambiguous is None and len({c["start"].utcoffset() for c in chosen}) > 1:
        # Two copies that did not collapse into one instant: the notice's two
        # zones disagree, and a union of them is neither.
        ambiguous = ("the window is stated in two zones that do not describe "
                     "the same time")
    limit = _max_window()
    if ambiguous is None and limit is not None and end - start > limit:
        ambiguous = ("the window runs {:.1f}h, longer than NOTICE_MAX_WINDOW_HOURS"
                     " - more likely an event or holiday period than an "
                     "outage".format((end - start).total_seconds() / 3600))
    return {"start": start, "end": end,
            "superseded": all(c["superseded"] for c in chosen),
            "at": min(c["span"][0] for c in chosen),
            "others": others, "ambiguous": ambiguous,
            # The distinct windows the union above was built from. A union
            # that is not itself one of them was never stated as a window -
            # the reschedule branch refuses to write that (F22).
            "pieces": sorted({(c["start"], c["end"]) for c in chosen}),
            # Where each chosen window was read - the clean-notice check reads
            # the lines around them (see _doubt).
            "spans": sorted(c["span"] for c in chosen)}


def _cand_dates(cands, gone) -> set:
    """Every day a window the parser read falls on - chosen, superseded or
    removed as done. The clean-notice check leaves those days alone."""
    out = set()
    for c in list(cands) + list(gone or []):
        try:
            out |= {c["start"].date(), c["end"].date()}
        except Exception:      # noqa: BLE001
            pass
    return out


def _aware(now, tz):
    if now is None:
        return datetime.now(tz)
    if now.tzinfo is None:
        return now.replace(tzinfo=tz)
    return now


def _window_detail(t: str, now: datetime):
    """-> _resolve's dict or None, on NORMALISED text."""
    return _resolve(_collect(t, now)[0], now)


def find_window(text: str, *, now=None):
    """-> (start, end) aware datetimes, or None.

    Dates and time-ranges are collected separately and paired, because providers
    routinely put them on different lines; _bind_date says which date a range
    takes. A range with no date that belongs to it is unusable: guessing the
    day is how a row ends up with a window in the wrong week.

    A window classify() refuses as ambiguous is still returned here. The one
    caller (providerask) asks only "does this message carry a window at all?"
    before it stamps "No maintenance" on a row, and for that question an
    ambiguous window is still a window. Never WRITE what this returns without
    classify()'s verdict.

    ``now`` is threaded through rather than read from the clock inside, so a
    year-less date resolves reproducibly (see _infer_year) and the corpus can
    pin it. It defaults to the real clock for vawatch and providerask, which
    call this with the text alone.
    """
    if not text:
        return None
    detail = _window_detail(_norm(text), _aware(now, _tz_default()))
    if not detail:
        return None
    return detail["start"], detail["end"]


def _fmt_window(s, e) -> str:
    same = s.date() == e.astimezone(s.tzinfo).date()
    return "{:%Y-%m-%d %H:%M} -> {}".format(
        s, "{:%H:%M}".format(e.astimezone(s.tzinfo)) if same
        else "{:%Y-%m-%d %H:%M}".format(e.astimezone(s.tzinfo))) + \
        " ({})".format(s.strftime("%z")[:3] + ":" + s.strftime("%z")[3:])


def is_stale(end, *, now=None) -> bool:
    """Has this window already finished?

    Decided here, by the clock, rather than by the model. Asked to reason about
    it the LLM got the right answer with wildly different confidence (0.50 and
    0.90 on two equivalent cases), and a provider re-quoting last month's notice
    must never overwrite the row with a window that is already over.
    """
    if end is None:
        return False
    now = now or datetime.now(end.tzinfo or _tz_default())
    if now.tzinfo is None:
        now = now.replace(tzinfo=end.tzinfo or _tz_default())
    return end < now


# ---------------------------------------------------------------------------
# The reschedule gate (F20, F53, G4.3)
# ---------------------------------------------------------------------------
#
# A reschedule OVERRIDES the row, so it used to be checked before the wording
# gate and the question gate - and every one of these was written as the
# provider's new window, labelled "(rescheduled)":
#   "The tournament has been postponed to 09-26 20:00-22:00"      (subject)
#   "中秋节客服时间调整：9月25日 10:00-22:00"                        (subject)
#   "Hi team, can the maintenance be postponed to 09-25 02:00-04:00?"  (question)
#   "an updated schedule will be shared in this group"            (promise)
#   "如维护延期，将另行通知新的时间"                                   (condition)
#   "Please reschedule your maintenance to 09-25 03:00-05:00"     (OPERATOR request)
# A reschedule word now counts only when it STATES a move of the maintenance.

# G4.3: a request FROM THE OPERATOR is not a provider announcement. Two
# shapes, both deterministic:
#   move   - a request lead, a move verb and the maintenance owned by the
#            addressee ("your maintenance", 贵司/你们的维护), or an imperative
#            "please reschedule / 请将维护…改到";
#   freeze - a request lead, a stop/avoid verb and a RELEASE object (deploy,
#            releases, callbacks, 发版, 上线, 部署): "kindly hold all releases
#            on 09-24 02:00-04:00 due to our system maintenance" is the
#            operator's own freeze, not the provider's outage.
# A provider's "Please note the maintenance has been rescheduled" has no
# addressee-owned maintenance and no release object, so it is untouched.
# NOTICE_REQUEST_GUARD=0 turns this off.
_REQ_LEAD_RE = re.compile(
    r"\b(?:please|pls|plz|kindly)\b|\bwe\s+(?:would\s+like\s+to\s+|kindly\s+"
    r"|hereby\s+)?(?:request|ask|urge)\b|\b(?:could|can|would|will)\s+you\b"
    r"|麻烦|麻煩|烦请|煩請|恳请|懇請|敬请|敬請|拜托|拜託|请|請|希望|要求", re.I)
_REQ_MOVE_RE = re.compile(
    r"\b(?:re-?schedul\w*|postpon\w*|move[ds]?|moving|shift\w*|chang\w*"
    r"|delay\w*|defer\w*|push\w*|adjust\w*|bring\s+forward)\b"
    # 提前 alone is "in advance" ("敬请贵司提前做好维护准备" is the provider
    # asking us to prepare), so only 提前至/提前到 is a move.
    r"|改期|改到|改至|改为|改為|改在|调整|調整|延后|延後|推迟|推遲|延迟|延遲|挪|移到|移至"
    r"|提前(?:至|到)", re.I)
_REQ_OWNER_RE = re.compile(
    r"\byour\s+(?:[\w-]+\s+){0,2}?(?:maintenance|maint\.|downtime)"
    # R1.16: "烦请贵司注意：维护时间调整为<X>" is the provider's "please note",
    # not a request to move - 注意 / 留意 / 知悉 after 贵司 is the notice formula.
    r"|(?:贵司|貴司|贵方|貴方|你们|你們|您们|您們|贵公司|貴公司|你司)"
    r"(?![\s：:，,]*(?:注意|留意|知悉|周知|查收|知晓|知曉|悉知))(?:的)?"
    r"[^。，,；;\n]{0,8}?(?:维护|維護)", re.I)
_REQ_IMPERATIVE_RE = re.compile(
    r"\b(?:please|pls|plz|kindly)\s+(?:help\s+(?:to\s+|us\s+)?)?(?:re-?schedule"
    r"|postpone|move|shift|delay|defer|push\s+back)\b"
    r"|(?:请|請|麻烦|麻煩|烦请|煩請)(?:您|贵司|貴司)?(?:将|將|把)[^。，,；;\n]{0,10}?"
    r"(?:维护|維護)"
    # G4.3: "We would like to ask you to reschedule the maintenance to X",
    # "We kindly ask that the scheduled maintenance be postponed to X".
    r"|\b(?:ask|request|urge)\s+(?:you\s+)?(?:to\s+)?(?:help\s+(?:to\s+)?)?(?:re-?schedule"
    r"|postpone|move|shift|delay|defer|push\s+back)\b"
    r"|\b(?:ask|request)\s+that\b[^.;\n]{0,80}?\bbe\s+(?:re-?scheduled|postponed|moved"
    r"|shifted|delayed|deferred|pushed\s+back)\b", re.I)
_REQ_FREEZE_RE = re.compile(
    r"\b(?:avoid\w*|hold|stop\w*|paus\w*|freez\w*|refrain\w*|suspend\w*|halt\w*)\b"
    r"|\bdo\s+not\b|\bdon'?t\b|\bnot\s+to\b|暂停|暫停|停止|避免|不要|勿|禁止|暂缓|暫緩",
    re.I)
_REQ_RELEASE_RE = re.compile(
    r"\b(?:deploy(?:ment)?s?|deploying|releases?|releasing|callbacks?"
    r"|call-backs?|hot-?fix(?:es)?|go-?lives?)\b"
    r"|发版|發版|上线|上線|部署|发布版本|發佈版本|回调|回調"
    # G4.3: "Please do not push any updates during our scheduled maintenance".
    r"|\bpush\w*\s+(?:(?:any|new|the)\s+)*(?:updates?|releases?|changes?|builds?"
    r"|versions?|patch(?:es)?)\b", re.I)


def _operator_request(sent: str):
    """'move' | 'freeze' | None for one sentence (G4.3)."""
    if not _on("NOTICE_REQUEST_GUARD") or not _REQ_LEAD_RE.search(sent):
        return None
    # "Please reschedule the call to 15:00" names no maintenance: not ours.
    if (_REQ_MOVE_RE.search(sent) and _TOPIC_STRONG_RE.search(sent)
            and (_REQ_OWNER_RE.search(sent) or _REQ_IMPERATIVE_RE.search(sent))):
        return "move"
    if _REQ_FREEZE_RE.search(sent) and _REQ_RELEASE_RE.search(sent):
        return "freeze"
    return None


_NEW_TBA_RE = re.compile(
    r"\b(?:new|updated|revised|final)\s+(?:maintenance\s+)?(?:time|date|window"
    r"|schedule|slot)s?\b[^.;\n]{0,40}?\b(?:will\s+be\s+(?:shared|announced|sent"
    r"|posted|confirmed|provided|informed|notified)|to\s+(?:be\s+)?(?:follow|announced"
    r"|confirmed|shared)|TBA|TBC|TBD)\b"
    r"|\b(?:an?\s+)?(?:updated|revised|new)\s+schedule\s+will\s+(?:follow|be\s+shared)"
    r"|新的?(?:时间|時間|日期|安排)[^。\n]{0,10}(?:另行|稍后|稍後|待定|再行)"
    r"|另行通知|另行公布|另行公告|待定"
    # F23: "We will share a new date soon", "we will inform you of the new time".
    r"|\bwill\s+(?:share|announce|send|provide|confirm|let\s+you\s+know|inform\s+you\s+of"
    r"|update\s+you\s+(?:on|with))\s+(?:you\s+)?(?:the\s+|a\s+)?(?:new|updated|revised"
    r"|rescheduled)\s+(?:maintenance\s+)?(?:time|date|window|schedule|slot)s?\b", re.I)


def _reschedule_match(t: str):
    """-> (match, refusal). ``match`` is the first RESCHEDULE_RE hit that
    states a move of the maintenance; ``refusal`` is 'question' or 'request'
    when the only hits were asked or requested, else None.

    Refused, in order: a hit inside a question about maintenance (the same
    sentence test as the notice question guard - NOTICE_QUESTION_GUARD), an
    operator move request (NOTICE_REQUEST_GUARD), a hit behind a condition
    (if / should / 如 / 若 - "如维护延期"), an Updated/Revised Schedule that is
    only promised (_promised_marker), and a bare reschedule word whose
    sentence names another subject (a promo, a tournament, 客服) and no
    maintenance.
    """
    qs = []
    if _on("NOTICE_QUESTION_GUARD"):
        qs = [(s, e) for s, e in _question_sentences(t)
              if _ASK_TOPIC_RE.search(t, s, e)]
    refusal = None
    for m in RESCHEDULE_RE.finditer(t):
        q = next(((s, e) for s, e in qs if s <= m.start() < e), None)
        if q:
            # Only a question that names the maintenance is refused AS one;
            # "Could we reschedule our integration call to 15:00?" is simply
            # not about maintenance and falls to the ordinary gate.
            if _TOPIC_STRONG_RE.search(t, *q):
                refusal = refusal or "question"
            continue
        if re.match(r"[ \t]*[?？]", t[m.end():m.end() + 4]):
            # F20: "Rescheduled? <window>" asks whether it moved - the "?" is
            # on the move word itself, and the window after it was written as
            # the new one.
            refusal = refusal or "question"
            continue
        sent = _sentence_of(t, m.start())
        if _operator_request(sent) == "move":
            refusal = "request"
            continue
        if _CONDITION_RE.search(_clause_before(t, m.start())):
            continue
        if (re.match(r"(?i)(?:updated|revised)\s+schedule", m.group(0))
                and _promised_marker(t, m)):
            continue
        if (not _TOPIC_STRONG_RE.search(m.group(0))
                and _TOPIC_OFF_RE.search(sent)
                and not _TOPIC_STRONG_RE.search(sent)):
            continue
        return m, None
    return None, refusal


# ---------------------------------------------------------------------------
# The clean-notice check (NOTICE_DOUBT_GUARD, default on)
# ---------------------------------------------------------------------------
#
# Every rule above reads one KIND of statement, and the September 2026 audit
# showed that each kind has a long tail: after two rounds of fixes, 298 fresh
# wordings still wrote a wrong window - "is off", "aborted", "put on hold",
# "(TBD)", "Last updated: <date>", "CS: 09:00-18:00", "10:00 - 11:30 in the
# evening", "(+1)", "世界协调时", "PST (California time)", "Suggest to
# reschedule", 暂停…的维护, 请无视 <window>. Adding each word one at a time does
# not converge. So a fill is written only when the notice is CLEAN: nothing in
# it withdraws, pauses or questions the maintenance, promises no impact, or
# names a date, a time of day, a next-day marker or a zone that the window
# does not account for. Anything that does goes to a person with the phrase
# quoted - needs_human writes nothing, and the Laboratory card shows the text.
# The exemptions are the boilerplate real notices carry around a clean window:
# "unfinished bets will be cancelled", "balances will not be affected",
# "please confirm receipt", "all services will be suspended".

_DOUBT_OBJECT_RE = re.compile(
    r"\b(?:bets?|wagers?|rounds?|spins?|hands?|orders?|transactions?|requests?|"
    r"withdrawals?|deposits?|payouts?|tickets?|sessions?|unfinished|unsettled|pending|"
    r"ongoing|in-?progress|incomplete|open\s+games?|games?\s+in\s+progress|bonus(?:es)?|"
    r"promotions?|free\s*spins?|errors?|warnings?|pop-?ups?|prompts?|alerts?|messages?\s+you"
    r"|events?|tournaments?|campaigns?|draws?|missions?|leaderboards?)\b"
    r"|注单|注單|订单|訂單|投注|未完成|未结算|未結算|交易|提款|充值|游戏局|遊戲局|牌局|"
    r"报错|報錯|错误提示|錯誤提示|异常提示|異常提示|提示|优惠|優惠|活动|活動|锦标赛|錦標賽|比赛|比賽"
    r"|抽奖|抽獎", re.I)
# A frame that makes the word hypothetical: "if this maintenance is cancelled",
# "如维护取消将另行通知", "may be extended if needed", "维护结束后游戏将正常运行".
_DOUBT_FRAME_BEFORE_RE = re.compile(
    r"(?:\b(?:if|unless|whether|should|in\s+case|may|might|could|possibly|once|after|when"
    r"|following)\b|如果|如|若|假如|倘若|如有|可能|结束后|結束後|完成后|完成後|之后|之後|以后"
    r"|以後|届时|屆時)[^.。;；,，\n]{0,40}$", re.I)
_DOUBT_SCOPED_CALM_RE = re.compile(
    r"\s*(?:outside|beyond|before|after|besides)\b"
    r"|\s+(?:for|on|to|of|in)\s+(?:the\s+|our\s+)?(?:reporting|reports?|back\s*-?\s*office|BO\b"
    r"|admin|API|data\s+feeds?|UAT|staging|sandbox|test)"
    r"|(?:高峰|其他|其余|其餘|非维护|非維護)"
    r"(?:时段|時段|时间|時間)", re.I)
_DOUBT_WITHDRAW_RE = re.compile(
    r"\b(?:cancel(?:l?ed|l?ing|l?ation|s)?|call(?:ed|ing)?\s+off|void(?:ed)?|withdr[ae]wn?"
    r"|withdrawing|retract\w*|revok\w*|rescind\w*|abort\w*|scrap(?:ped|ping)?|disregard\w*"
    r"|ignore[ds]?|annul\w*|invalid(?:ated)?|null\s+and\s+void)\b"
    r"|\bon\s+hold\b|\bput\s+off\b|\bno\s+longer\b|\bnot\s+going\s+ahead\b|\bis\s+(?:now\s+)?off\b"
    r"|\b(?:has|have)\s+been\s+dropped\b|\b(?:is|was)\s+(?:now\s+)?dropped\b"
    r"|\b(?:will|shall)\s+not\s+(?:be\s+)?(?:happen|go\s+ahead|proceed|take\s+place|held"
    r"|performed|performing|carried\s+out|conducted)\b|\bwon['’]t\s+(?:happen|go\s+ahead"
    r"|proceed|take\s+place|be\s+(?:held|performed))\b"
    r"|\b(?:by\s+mistake|in\s+error|mistaken(?:ly)?|was\s+a\s+mistake|typo|erratum"
    r"|wrong\s+(?:group|chat|channel|info(?:rmation)?|time|date|window|notice|one))\b"
    r"|\bnot\s+(?:applicable|for\s+(?:your|this)\s+(?:group|chat|team))\b"
    r"|\bmeant\s+for\s+(?:another|a\s+different|other)\b|\btest\s+(?:notice|message|post)\b"
    r"|作废|作廢|撤回|撤销|撤銷|取消|忽略|无视|無視|无效|無效|有误|有誤|发错|發錯|误发|誤發"
    r"|错发|錯發|不进行|不進行|无需进行|無需進行|不用了|不再进行|不再進行|测试通知|測試通知",
    re.I)
_DOUBT_DONE_RE = re.compile(
    r"\b(?:has|have|had|is|was|were)\s+(?:now\s+|already\s+|successfully\s+)?(?:been\s+)?"
    r"(?:completed|finished|concluded|ended|done|over)\b|\bended\s+(?:at|early)\b"
    r"|(?:^|[\n:：\-–—|])\s*(?:status\s*[:：]\s*)?(?:completed|done|finished|resolved)\b(?:\s*[✅✔]|\s*[.!]|\s*$)"
    r"|\ball\s+(?:done|clear)\b|\bgames?\s+(?:have\s+)?reopened\b|\bback\s+online\b(?!\s+(?:at|by|after))"
    r"|现已结束|現已結束|已结束|已結束|已完成|已恢复|已恢復|已经恢复|已經恢復|顺利完成|順利完成", re.I | re.M)
_DOUBT_HOLD_RE = re.compile(
    r"\b(?:paused?|suspended|postponed|delayed|deferred|shifted|halted|stopped|pushed\s+back"
    r"|prolonged|extended|overrun)\b|暂停|暫停|暂缓|暫緩|中止|搁置|擱置|押后|押後|推迟|推遲"
    r"|延期|延后|延後|延迟|延遲|延长|延長", re.I)
_DOUBT_SERVICE_AFTER_RE = re.compile(
    r"\s*(?:all\s+|the\s+|our\s+|所有|全部|一切|各项|各項)?(?:服务|服務|游戏|遊戲|投注|充值|提款|访问|訪問|登录|登錄|"
    r"services?|games?|access|deposits?|withdrawals?|betting|logins?|gameplay|operations?|"
    r"transactions?|traffic)", re.I)
_DOUBT_MAINT_RE = re.compile(r"maint(?:enance|\.)|downtime|upgrade|维护|維護|维修|維修|升级|升級",
                             re.I)
_DOUBT_CALM_RE = re.compile(
    r"(?:\bnot|n['’]t|\bnever)\s+(?:be\s+)?(?:affect(?:ed)?|impact(?:ed)?|interrupt(?:ed)?"
    r"|disrupt(?:ed)?|notice)\b|\bunaffected\b|\bno\s+(?:impact|effect|interruption|disruption"
    r"|downtime|outage)\b|\b(?:remain|remains|stay|stays|continue|continues)\s+(?:to\s+be\s+)?"
    r"(?:available|online|open|accessible|up|operational|running|normal(?:ly)?|playable"
    r"|uninterrupted)\b|\bas\s+(?:normal|usual)\b|\bseamless(?:ly)?\s+(?:maintenance|upgrade|transition|experience"
    r"|for\s+(?:players|users))\b|\b(?:will\s+be|is)\s+seamless\b|\buninterrupted\b"
    r"|\b(?:run|operate|work|function)s?\s+normally\b|\bonly\s+(?:the\s+)?(?:admin|back\s*-?"
    r"\s*office|BO|reporting|report|portal|dashboard|UAT|test|staging)\b"
    r"|不受影响|不受影響|不影响|不影響|无影响|無影響|没有影响|沒有影響|照常|正常(?:运行|運行|运营"
    r"|運營|开放|開放|进行|進行|游戏|遊戲|访问|訪問|使用|登录|登錄)|无感知|無感知|不停服|不停机"
    r"|不停機|不中断|不中斷|仅后台|僅後台|只影响后台"
    # #124: 前台游戏正常 / 游戏均正常 - the games named first.
    r"|(?:前台|前端|游戏|遊戲)(?:均|都|将|將|会|會|仍|依然)?(?:正常|照常)(?!结束|結束|后|後)", re.I)
_DOUBT_CALM_OK_BEFORE_RE = re.compile(
    r"(?:balances?|funds?|accounts?|data|wallets?|bonus(?:es)?|jackpots?|progress|history"
    r"|records?|winnings?|promotions?|promos?|events?|tournaments?|campaigns?|customer\s+"
    r"(?:service|support)|support|live\s*chat|CS\b|helpdesk|余额|餘額|资金|資金|账户|帳戶|数据"
    r"|數據|记录|紀錄|钱包|錢包|活动|活動|优惠|優惠|锦标赛|錦標賽|客服"
    # R1.73 / R1.74: a test environment or a subsystem named as the part that
    # stays UP - the notice is about the production games.
    r"|UAT|staging|sandbox|test(?:ing)?\s+(?:environment|env|server)s?|测试环境|測試環境|测试服"
    r"|back\s*-?\s*office|BO\b|admin(?:\s+(?:portal|panel|console))?|reporting|reports?|API"
    r"|data\s+feeds?|后台|後台|报表|報表|接口)[^.。;；\n]{0,60}$", re.I)
_DOUBT_ASK_RE = re.compile(
    r"\b(?:suggest(?:ed|ing|s)?|propos(?:e|ed|al|ing)|please\s+consider|consider(?:ing)?\s+"
    r"(?:moving|postponing|rescheduling|changing|shifting)|we\s+(?:would\s+like|wish|hope|want)"
    r"\s+to\s+(?:ask|request|move|postpone|reschedule|change|shift|confirm)|(?:management|team"
    r"|operator|client|we)\s+requests?\b|request(?:ed|ing)?\s+(?:to|that|for)\b|is\s+it\s+possible"
    r"|would\s+it\s+be\s+possible|can\s+(?:you|we)\s+(?:move|postpone|reschedule|change|shift"
    r"|confirm)|(?:please|kindly|pls|plz)\s+(?:confirm|verify)(?!\s+(?:the\s+)?(?:receipt"
    r"|received|you\s+have\s+received))|need\s+(?:your\s+)?confirmation|to\s+be\s+confirmed"
    r"|TBD|TBC|tentative(?:ly)?|provisional(?:ly)?|(?<=[\[【(（])\s*draft(?=\s*[\]】)）])|draft(?=\s*[:：])"
    r"|subject\s+to\s+(?:confirmation"
    r"|approval)|not\s+(?:yet\s+)?(?:confirmed|final(?:ised|ized)?))\b"
    r"|建议|建議|提议|提議|希望(?:能|可以|将|將|把|贵司|貴司)|能否|可否|能不能|可不可以|是否可以"
    r"|是否能|确认一下|確認一下|确认下|確認下|请确认(?!\s*(?:收到|已收到|接收))|請確認(?!\s*(?:收到"
    r"|已收到))|帮忙确认|幫忙確認|待定|待确认|待確認|暂定|暫定|初步|未确定|未確定|尚未确定", re.I)
_DOUBT_EVENING_RE = re.compile(
    r"\b(?:evening|night|tonight|midnight)\b|今晚|今夜|晚上|夜间|夜間|深夜|半夜|晚间|晚間", re.I)
_DOUBT_AFTERNOON_RE = re.compile(r"\bafternoon\b|下午|午后|午後|黄昏|黃昏|傍晚|\bP\.?M\.?(?=\s*(?:\d|$))",
                                 re.I)
_DOUBT_MORNING_RE = re.compile(r"\bmorning\b|上午|早上|清晨|早晨|凌晨|\bA\.?M\.?(?=\s*(?:\d|$))", re.I)
_DOUBT_NEXTDAY_RE = re.compile(
    r"\(\s*\+\s*1\s*(?:d(?:ay)?)?\s*\)|\+\s*1\s*(?:day|d)\b|\bD\s*\+\s*1\b|\bT\s*\+\s*1\b"
    r"|\bfollowing\s+(?:day|morning)\b|\bnext\s+(?:day|morning)\b|次日|翌日|隔日|隔天|第二天"
    r"|第二日|明早", re.I)
_DOUBT_ZONE_WORD_RE = re.compile(
    r"(?<![A-Za-z])(?!(?:Maintenance|Start|End|Down|Up|Local|Update|Restore|Recovery"
    r"|Completion|Estimated|Expected|Resume|Open|Close|Opening|Closing|Finish|Begin|Beginning"
    r"|Effective|Report|Check|Total|New|Old|Original|Next|Last|Final|Real|Response|Run|Lead"
    r"|Wait|Service|Working|Office|Business|Beijing|Singapore|Manila|Philippine|Philippines"
    r"|Hong\s+Kong|Taipei|Taiwan|Malaysia|Kuala\s+Lumpur|China|Perth|Macau|Brunei|Irkutsk"
    r"|Western\s+Australia|Australian\s+Western|Any|Same|Each|Every|Our|Your|This|That|The"
    r"|Maint|System|Server|Game|Games|Downtime|Outage|Upgrade|Release|Event|Promo|Launch"
    r"|Standard|Daylight|Summer|Scheduled|Correction|Updated|Postponed"
    r"|Rescheduled|Please|Kindly|Note|Notice|Dear|Hi|Hello|Date|Time)"
    r"\b)[A-Z][a-z]+(?:[ \t]+[A-Z][a-z]+)?[ \t]+(?:Standard[ \t]+|Daylight[ \t]+|Summer[ \t]+)?"
    r"[Tt]ime\b"
    r"|\b(?:America|Europe|Africa|Australia|Pacific|Atlantic|Indian|Antarctica)/[A-Za-z_]+"
    r"|\bZulu\b|\b(?:[Ll]ocal|[Ss]erver|[Ss]ystem|[Pp]latform|[Pp]rovider|[Ss]tudio|[Oo]ur)"
    r"[ \t]+time\b"
    r"|世界协调时|世界協調時|零时区|零時區|世界时间|世界時間|国际标准时间|國際標準時間|国际时间"
    r"|國際時間|系统时间|系統時間|服务器时间|伺服器時間|当地时间|當地時間|海外时间|海外時間"
    r"|美国|美國|美东|美西|欧洲时间|歐洲時間")
_DOUBT_BARE_UTC_RE = re.compile(r"\b(?:UTC|GMT)\b(?!\s*[+\-]?\s*\d)(?!\s*时间\s*[+\-]?\s*\d)")
_DOUBT_PLUS8_RE = re.compile(r"(?:UTC|GMT)\s*\+\s*0?8\b|\+08:?00\b|北京时间|北京時間|SGT|HKT"
                             r"|PHT|MYT|BJT|AWST|北京|台北|新加坡|马尼拉|馬尼拉", re.I)
_DOUBT_LABEL_RE = re.compile(
    r"^[\W_]*((?=[^:：|\n]*[A-Za-z\u4e00-\u9fff])[^:：|\n\d]{1,40}?)\s*[:：|]\s*(?=\S)")
_DOUBT_LABEL_DENY_RE = re.compile(
    r"\b(?:support|cs|customer|contact|hotline|office|on-?call|account|manager|happy|free|bonus"
    r"|promo\w*|events?|tournaments?|draws?|webinar|meeting|training|ticket\w*|freeze|cutoff"
    r"|settlement|report\w*|client|release|version|app\s+update|chat|email|finance)\b"
    r"|发布|發佈|客户端|客戶端|版本|上线|上線|客服|支持|联系|聯繫|值班|活动|活動|优惠|優惠|结算|結算"
    r"|报表|報表|对账|對賬", re.I)
_DOUBT_LABEL_OK_RE = re.compile(
    r"maint|维护|維護|维修|維修|升级|升級|downtime|outage|upgrade"
    r"|\b(?:time|date|schedule|window|period|start|end|from|to|when|day"
    r"|slots?|live|casino|tables?|games?|products?|platform|server|system|lobby|api|app|site"
    r"|phase|part|night|stage|round|batch|step|region|all|asia|europe|provider|brand|notice"
    r"|reminder|update|new|revised|updated|correct|final|latest|original|old|previous)\b"
    r"|维护|維護|维修|維修|升级|升級|时间|時間|日期|时段|時段|开始|開始|结束|結束|游戏|遊戲|平台"
    r"|系统|系統|服务器|伺服器|产品|產品|电子|電子|真人|捕鱼|捕魚|体育|體育|彩票|棋牌|老虎机|老虎機"
    r"|阶段|階段|第|全部|所有|公告|通知|提醒|更新|最新|原定|原先|新"
    r"|\b(?:after|before|now|was|changed|change|note|attention|important|urgent|please|kindly"
    r"|dear|hi|hello|issued|posted|published|announcement|correction|erratum|wrong|right"
    r"|rescheduled|postponed|moved|delayed|extended|changed|revised)\b"
    r"|变更|變更|调整|調整|更正|正确|正確|错误|錯誤|温馨|溫馨|提示|注意|烦请|煩請|请|請|重要|紧急"
    r"|緊急|改为|改為|延后|延後|推迟|推遲|原订|原訂", re.I)
_CLOCK_TOKEN_RE = re.compile(r"(?<![\d:])(\d{1,2})\s*[:：.]\s*(\d{2})(?![\d:])")
# The outage named, in any of the ways the gate accepts except a bare
# capitalised subject ("John will be offline" is a person, G1.5).
_DOUBT_NAMED_RE = re.compile(
    r"maint\w*|downtime|down\s+time|upgrade|outage|interruption|维护|維護|维修|維修|升级|升級"
    r"|停机|停機|停服|暂停服务|暫停服務|服务暂停|服務暫停|下线|下線|无法(?:登录|登錄|进入|進入|投注|访问|訪問)"
    r"|無法(?:登錄|進入|投注|訪問)|(?:游戏|遊戲|平台|系统|系統|服务器|伺服器)[^。\n]{0,6}(?:关闭|關閉|暂停|暫停)"
    r"|\b(?:games?|services?|platforms?|tables?|lobb(?:y|ies)|systems?|servers?|sites?|casino"
    r"|products?|titles?|slots?)\b[^.\n]{0,40}\b(?:unavailable|offline|down|closed|suspended"
    r"|inaccessible|interrupted|shut\s+down)\b", re.I)
_DOUBT_CHANGE_RE = re.compile(
    r"->|→|=>|\bto\b|\binstead\b|\bnot\b|\bNOT\b|\bupdated\b|\bamended\b|\bchanged\b|\bnow\b"
    r"|\bwas\s+(?:wrong|incorrect)|\bcorrect\b|\bprevious(?:ly)?\b|\boriginal(?:ly)?\b"
    r"|改为|改為|改成|改至|改到|更改|变更|變更|调整|調整|不是|而非|有误|有誤|错误|錯誤|原定|原先|原本", re.I)
_DOUBT_OLD_RE = re.compile(
    r"\bpreviously\s+(?:stated|shared|communicated|announced|notified|mentioned|set|planned"
    r"|scheduled)|\bprevious\s+(?:date|time|notice|window|schedule)|\b(?:Original|Old|Was|Before)"
    r"\s*[:：]|\boriginal(?:ly)?\s+(?:date|time|window|schedule|planned|scheduled)|原定|原先|原本"
    r"|原订|原訂|此前|之前通知|先前通知|早前|变更前|變更前|错误|錯誤|旧|舊", re.I)
_DOUBT_NEW_RE = re.compile(
    r"最新|更新后|更新後|变更后|變更後|调整为|調整為|改为|改為|更改为|更改為|现改为|現改為|修改为|改成|正确|正確"
    r"|\b(?:updated|latest|revised|amended|new|correct)\s+(?:maintenance\s+)?(?:time|date|window"
    r"|schedule|slot)\b|\b(?:Latest|Updated|New|Now|After|To|Correct|Revised)\s*[:：]", re.I)
_DOUBT_UPDATE_RE = re.compile(
    r"最新|更新后|更新後|变更后|變更後|调整为|調整為|改为|改為|更改为|更改為|现改为|現改為|修改为|改成"
    r"|\b(?:updated|latest|revised|amended|new)\s+(?:maintenance\s+)?(?:time|date|window|schedule"
    r"|slot)\b|\bLatest\s*[:：]|^\s*(?:To|Now|After|New)\s*[:：]|\bpreviously\s+(?:stated|shared"
    r"|communicated|announced|notified|mentioned|set|planned|scheduled)|\bprevious\s+(?:date|time"
    r"|notice|window)|原定|原先|原本|此前|之前通知|先前通知|早前", re.I | re.M)
_DOUBT_CHECK_WORD_RE = re.compile(
    r"\b(?:correct|right|confirm(?:ed)?|accurate|ok(?:ay)?|still\s+on|still\s+valid)\b"
    r"|对吗|對嗎|对吧|對吧|正确|正確|没错|沒錯|确认|確認|是这个|是這個|是否", re.I)
_DOUBT_DESK_RE = re.compile(
    r"\b(?:support|CS|customer\s+(?:service|support)|hotline|contact(?:\s+us)?|live\s*chat"
    r"|on-?call|account\s+manager|monitor(?:ing)?|office\s+hours?|help\s*desk|ticket(?:ing)?"
    r"(?:\s+system)?|e-?mail(?:\s+service)?|telegram|whatsapp|phone\s+line|finance|settlement"
    r"|reporting\s+period|report\s+period|freeze)\b"
    r"|客服|技术支持|技術支持|联系我们|聯繫我們|值班|工单|工單|邮件|郵件|财务|財務|结算|結算", re.I)
_DOUBT_SHAPES = [
    (re.compile(r"(?:downtime|duration|lasting|last|takes?|expected|estimated|approx\w*|about"
                r"|预计|預計|约|約|持续|持續|时长|時長)[^\n]{0,14}?\d{1,2}[.:]\d{2}\s*[-–~至到]\s*"
                r"\d{1,2}[.:]\d{2}\s*(?:hours?|hrs?|h\b|小时|小時|分钟|分鐘|mins?|minutes?)", re.I),
     "the time reads like a duration"),
    (re.compile(r"(?:->|→|=>)\s*(?:TBA|TBC|TBD|待定|另行通知|to\s+be\s+announced)", re.I),
     "the new time is still to come"),
    (re.compile(r"~~[^~\n]{6,}~~"), "the notice is struck through"),
    (re.compile(r"(?:服务中断|服務中斷|停服|停机|停機|维护|維護|downtime|service\s+interruption"
                r"|outage|maintenance)\s*[:：]\s*(?:无|無|没有|沒有|否|none|nil|no\b|n/?a\b)", re.I),
     "a label answers that there is none"),
    (re.compile(r"^[ \t]*(?:[-*•][ \t]*)?(?:Europe|EU|Asia|APAC|America|Americas|LATAM|US|UK|Africa"
                r"|Oceania|MENA|欧洲|歐洲|亚洲|亞洲|美洲|拉美|北美|南美)\b[ \t]*(?:region|market|server"
                r"s?|区|區)?[ \t]*[-–—:：|]", re.I | re.M),
     "the notice gives windows per region"),
    (re.compile(r"(?-i:[A-Z])[\w -]{1,30}?\s+(?:jurisdiction|market|country|region|licen[cs]e|operators?)"
                r"\s+only\b|\baffects?\s+(?:the\s+)?(?-i:[A-Z])[\w -]{1,30}?\s+only\b", re.I),
     "the outage is limited to one market"),
    (re.compile(r"(?:only|applies\s+to|is\s+for|for\s+the)\s+(?:the\s+)?(?:test|testing|UAT|staging"
                r"|sandbox|QA|demo|dev|integration|INT|SIT)\b|(?:仅|僅|只)(?:限|针对|針對|适用|適用)?"
                r"(?:测试|測試|UAT)", re.I),
     "the scope is a test environment"),
    (re.compile(r"\b(?:please|pls|plz|kindly)\s+(?:re-?schedule|postpone|move|shift|delay|defer|push"
                r"|cancel)\b|(?:请|請|麻烦|麻煩|烦请|煩請)(?:您|贵司|貴司|你们|你們)?(?:将|將|把)",
                re.I),
     "it asks for the maintenance to be moved"),
]



def _sentence_text(t: str, pos: int) -> str:
    s, e = _sentence_span(t, pos, pos)
    return t[s:e]


def _line_text(t: str, pos: int) -> str:
    ls, le = _line_at(t, pos)
    return t[ls:le]


def _names_other_window(seg: str, st, en, now) -> bool:
    """True when ``seg`` states a date or clock and none of them is the window's."""
    try:
        ds = [date(*ymd) for _p, _e, ymd in _dates(seg, now=now, tz=st.tzinfo)]
    except Exception:          # noqa: BLE001
        ds = []
    if ds and not any(dd in (st.date(), en.date()) for dd in ds):
        return True
    # A same-day sibling at another time ("Maintenance for <day> 14:00-16:00
    # has been cancelled" beside the live 10:00-12:00) is another window too.
    if _CLOCK_TOKEN_RE.search(seg):
        return not (_names_clock(seg, st) or _names_clock(seg, en))
    return False


_DATE_TOKEN_RE = re.compile(r"\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{1,2}\s*月\s*\d{1,2}|\d{1,2}/\d{1,2}/\d{4}")


def _names_other_day(seg: str, st, en) -> bool:
    """True when ``seg`` holds a bare d/m or m/d day that is neither end's day."""
    for m in re.finditer(r"(?<![\d/.\-])(\d{1,2})[/.](\d{1,2})(?![\d/.])", seg):
        x, y = int(m.group(1)), int(m.group(2))
        if not ((1 <= x <= 12 and 1 <= y <= 31) or (1 <= y <= 12 and 1 <= x <= 31)):
            continue
        days = {(st.month, st.day), (en.month, en.day)}
        if (x, y) not in days and (y, x) not in days:
            return True
    return False


def _names_clock(seg: str, dt) -> bool:
    for m in _CLOCK_TOKEN_RE.finditer(seg):
        h, mi = int(m.group(1)), int(m.group(2))
        if mi == dt.minute and h in (dt.hour, dt.hour % 12, (dt.hour % 12) or 12):
            return True
    return False


def _doubt(t: str, v: dict, now) -> Optional[str]:
    """Why a FILL verdict must not be written as it stands, or None (see above)."""
    if not _on("NOTICE_DOUBT_GUARD"):
        return None
    spans = v.get("_spans") or []
    if not spans or v.get("start") is None:
        return None
    st, en = v["start"], v["end"]
    home = spans[0][0]
    win_sent = _sentence_text(t, home)

    def quote(m):
        return "“{}”".format(_quote(t[max(0, m.start() - 25):m.end() + 15], 70))

    # 1. Withdrawn / voided / retracted / a mistake - unless the words name
    #    an object ("unfinished bets will be cancelled", "ignore any error
    #    pop-up"), or their sentence names a DIFFERENT window from the one being
    #    written ("The Slots maintenance on 10:00-11:00 has been cancelled").
    for m in _DOUBT_WITHDRAW_RE.finditer(t):
        cs, ce = _clause_span(t, m.start(), m.end())
        clause_pre = t[cs:m.start()]
        # The object the verb applies to stands between the maintenance word
        # and the verb ("维护期间未结算注单将取消", "bets placed during the
        # maintenance will be cancelled"), or right after an active verb
        # ("please ignore any error pop-up").
        lastm = list(_DOUBT_MAINT_RE.finditer(clause_pre))
        after_m = clause_pre[lastm[-1].end():] if lastm else clause_pre
        if (_DOUBT_OBJECT_RE.search(clause_pre[:30]) or _DOUBT_OBJECT_RE.search(after_m)) \
                or _DOUBT_OBJECT_RE.match(t, m.end(), min(len(t), m.end() + 24)) \
                or re.match(r"\s+(?:any|all|the)\s+(?:error|warning|pop-?up|prompt|alert|message)",
                            t[m.end():], re.I):
            continue
        if _DOUBT_FRAME_BEFORE_RE.search(clause_pre) or re.match(
                r"\s*(?:\S+\s+){0,4}?(?:if\b|如果|如已|若已)", t[m.end():m.end() + 40], re.I):
            continue
        # Negated: "本次维护不会取消", "the maintenance will not be cancelled".
        if re.search(r"(?:不会|不會|不|未|没有|沒有|\bnot\b|\bnever\b|\bno\b|n['’]t)\s*(?:be\s+)?$",
                     t[max(0, m.start() - 8):m.start()], re.I):
            continue
        # R1.76: it points BACK at an earlier notice or draft this one replaces
        # ("please ignore the earlier draft", "如已收到，请忽略此消息",
        # "以此公告为准，之前的公告作废"), and the window is not in its sentence.
        ss, se = _sentence_bounds(t, m.start(), m.end())
        hard = re.match(r"(?i)wrong\s+(?:group|chat|channel)|by\s+mistake|in\s+error|meant\s+for"
                        r"|not\s+(?:applicable|for)|发错|發錯|误发|誤發|错发|錯發|test", m.group(0))
        if not hard and (_ALREADY_HAVE_RE.search(t[ss:se]) or _PREVAILS_RE.search(t) or (
                _POINTS_BACK_RE.search(t[ss:se]) and not any(ss <= a < se for a, _b in spans))):
            continue
        if _names_other_window(t[cs:ce], st, en, now) or _names_other_day(t[cs:ce], st, en):
            continue
        if re.search(r"原定|原先|原本|原计划|原計劃|\boriginal(?:ly)?\b|\bprevious(?:ly)?\b"
                     r"|\bold\b|\bformer\b", t[cs:ce], re.I) and v.get("reschedule"):
            continue
        if v.get("reschedule") and RESCHEDULE_RE.search(t, m.end(), m.end() + 40):
            # "cancelled and rescheduled to <Y>" / "取消，改为 <Y>": the move names Y.
            continue
        return ("the notice reads as {}, but it also says {} - it may withdraw or "
                "void it".format(_fmt_window(st, en), quote(m)))
    # 1b. The maintenance reported as FINISHED ("…，现已结束，游戏恢复正常",
    #     "maintenance ended at 11:15", "Update 11:20: completed", "- DONE ✅")
    #     - a re-post of a finished notice re-wrote the row and re-carded it.
    win_next = bool(_NEXT_M_RE.search(win_sent) or _UPCOMING_M_RE.search(win_sent)
                    or re.search(r"(?i)\bnext\s+(?:one|window|week['’]?s?)\b|下次|下一次|下周|下週", win_sent))
    for m in _DOUBT_DONE_RE.finditer(t):
        cpre = _clause_before(t, m.start())
        dss, dse = _sentence_bounds(t, m.start(), m.end())
        if win_next or _DONE_PAST_RE.search(t[dss:dse]) or re.search(
                r"(?i)\b(?:last|previous|today['’]?s|yesterday['’]?s|earlier|prior)\b|上次|上周|上週|今天的|今日的",
                t[dss:m.start()]):
            continue
        if _names_other_window(t[dss:dse], st, en, now) and (
                _ANY_RANGE_RE.search(t, dss, dse) or _DATE_TOKEN_RE.search(t[dss:dse])):
            continue
        if re.match(r"(?i)over\s+\d", t[m.end() - 4:m.end() + 4].strip()) or re.match(
                r"\s+\d", t[m.end():m.end() + 3]):
            continue
        if re.search(r"(?i)\bas\s+soon\s+as\b|\bonce\b|\bwhen\b|\bafter\b|\buntil\b", t[dss:m.start()]):
            continue
        if _DOUBT_FRAME_BEFORE_RE.search(cpre) or re.search(
                r"(?i)\b(?:will|would|shall|should|expected|estimated|once|after|until|by)\b"
                r"|将|將|会|會|预计|預計|之后|之後|以后|以後|后|後|前", cpre[-24:]):
            continue
        if _DOUBT_OBJECT_RE.search(cpre[-20:]):
            continue
        return ("the notice reads as {}, but it also says {} - the maintenance may "
                "already be over".format(_fmt_window(st, en), quote(m)))
    # 2. The MAINTENANCE paused, held, postponed or extended ("维护已暂停",
    #    "maintenance ... suspended") - not "all services will be suspended",
    #    which is the outage itself. A reschedule fill is left to rule 7.
    for m in _DOUBT_HOLD_RE.finditer(t):
        if v.get("reschedule"):
            break
        if _DOUBT_SERVICE_AFTER_RE.match(t, m.end()):
            continue
        clause = _clause_before(t, m.start())
        if len(clause.strip()) <= 4:
            # "例行维护 <X>，已推迟" - the clause has no subject of its own.
            clause = t[_sentence_bounds(t, m.start(), m.start())[0]:m.start()]
        near = clause[-14:]
        mw = list(_DOUBT_MAINT_RE.finditer(clause))
        # The maintenance has to be what is paused: its word stands right in
        # front of the verb (or right after it: "暂停例行维护"), with no other
        # subject (an event, a promotion) between the two.
        if mw:
            between = clause[mw[-1].end():]
            if _DOUBT_OBJECT_RE.search(between) or len(between) > 70:
                mw = []
        if not mw and not _DOUBT_MAINT_RE.match(t, m.end(), m.end() + 8) and not re.match(
                r"(?:" + _WIN_ONLY + r"){0,60}?(?:的|之)?\s*(?:例行|定期|系统|系統|游戏|遊戲)?(?:维护|維護)",
                t[m.end():m.end() + 80]):
            continue
        cs, ce = _clause_span(t, m.start(), m.end())
        if _names_other_window(t[cs:ce], st, en, now):
            continue
        if _DOUBT_FRAME_BEFORE_RE.search(near) or re.search(
                r"\b(?:may|might|could|if\s+(?:needed|necessary|required))\b|如有需要|如需|必要时",
                t[max(0, m.start() - 14):m.end() + 20], re.I):
            continue
        # The extension the window already carries ("extended from 10:00 -
        # 12:00 to 10:00 - 14:00" read as 10:00-14:00).
        tgt = re.match(r"[^.\n。]{0,40}?(?:\bto\b|\buntil\b|\btill\b|至|到)\s*[^.\n。]{0,12}?"
                       r"(\d{1,2})\s*[:：.]\s*(\d{2})", t[m.end():], re.I)
        if tgt and (int(tgt.group(1)), int(tgt.group(2))) in ((en.hour, en.minute),
                                                                 (st.hour, st.minute)):
            continue
        return ("the notice reads as {}, but it also says {} - the maintenance may "
                "be paused, moved or extended".format(_fmt_window(st, en), quote(m)))
    # 3. A promise of no impact, or a partial scope ("only the admin portal").
    for m in _DOUBT_CALM_RE.finditer(t):
        cpre = _clause_before(t, m.start())
        # Only the calm phrase's OWN clause says what stays up: "后台将于 <X> 维护，
        # 前台游戏正常" names the back office as the part going DOWN.
        if _DOUBT_CALM_OK_BEFORE_RE.search(cpre):
            continue
        if _DOUBT_FRAME_BEFORE_RE.search(cpre) or _DOUBT_SCOPED_CALM_RE.match(t, m.end()) \
                or _DOUBT_SCOPED_CALM_RE.search(cpre[-10:]):
            continue
        bb = _BRAND_BEFORE_RE.search(t[max(0, m.start() - 40):m.start()])
        if bb and re.match(r"(?:Games?|Services?|Players?|Tables?|Platforms?|Slots?|Lobb(?:y|ies)|Systems?"
                           r"|Servers?|Sites?|Products?|Titles?|All|The|Our|It|They|This)\b", bb.group(0).strip()):
            bb = None   # "Games stay online" - the games themselves, not another brand
        if bb or re.search(
                r"(?-i:(?!(?:All|The|Our|Your|These|Those|Other|Some|Most|Every|Each|Any|No|Its|Their)\b)"
                r"[A-Z][\w&'-]+)(?:\s*(?:,|and|&)\s*(?-i:[A-Z][\w&'-]+))*\s+(?:games?|tables?"
                r"|products?|titles?|slots?|studios?|brands?|rows?|lobb(?:y|ies))\s+(?:are|is|will|shall"
                r"|remain\w*|stay\w*)?[^.。\n]{0,14}$", t[max(0, m.start() - 60):m.start()]):
            # "Hacksaw games are not affected" in a Yggdrasil notice: the
            # reassurance is about ANOTHER brand in a shared group (F37).
            continue
        return ("the notice reads as {}, but it also says {} - the outage may be "
                "partial or none at all".format(_fmt_window(st, en), quote(m)))
    # 4. A request, a proposal, or an unconfirmed window.
    m = next((x for x in _DOUBT_ASK_RE.finditer(t)
              if not _RELAY_REQUEST_RE.search(_sentence_text(t, x.start()))), None)
    if m:
        return ("the notice reads as {}, but it also says {} - a request or an "
                "unconfirmed window, not an announcement".format(_fmt_window(st, en), quote(m)))
    # 5. A time-of-day word the hours contradict ("10:00 - 11:30 in the
    #    evening", "今晚10:00", "午后2:00", "3:00 - 5:00 in the afternoon").
    # Only the words attached to the window's own clocks count: just before
    # the start clock ("今晚10:00", "午后2:00") or right after the end clock
    # ("10:00 - 11:30 in the evening"). "晚上10:00 至 次日凌晨2:00" puts 凌晨 on
    # the END, which the parser has already applied.
    a0, b0 = spans[0]
    clocks = list(_CLOCK_TOKEN_RE.finditer(t, max(0, a0 - 2), b0 + 2))
    ls0 = _line_at(t, a0)[0]
    pre = t[max(ls0, (clocks[0].start() if clocks else a0) - 12):(clocks[0].start() if clocks else a0)]
    post_at = clocks[-1].end() if clocks else b0
    post = t[post_at:post_at + 26]
    h = st.hour
    for rx, bad in ((_DOUBT_EVENING_RE, lambda x: 6 <= x < 17),
                    (_DOUBT_AFTERNOON_RE, lambda x: x < 12),
                    (_DOUBT_MORNING_RE, lambda x: x >= 12)):
        m = rx.search(pre) or rx.search(post)
        if m and bad(h):
            return ("the notice reads as {}, but the window is written with “{}” - the "
                    "hours may be meant as the other half of the day".format(
                        _fmt_window(st, en), m.group(0)))
    # 6. A next-day marker on the END that the window did not apply.
    m = _DOUBT_NEXTDAY_RE.search(t, clocks[0].end() if clocks else a0, post_at + 26)
    if m and en.date() == st.date() and not _DOUBT_NEXTDAY_RE.search(pre):
        return ("the notice reads as {}, but it also says {} - the end may be on "
                "the next day".format(_fmt_window(st, en), quote(m)))
    # 7. A reschedule whose only window is not introduced as the NEW one
    #    ("POSTPONED: Scheduled maintenance <X>", "<X> put on hold",
    #    "<X> -> TBA").
    if v.get("reschedule") and len(_ANY_RANGE_RE.findall(t)) <= 1 and not v.get("others"):
        lead = t[_sentence_bounds(t, home, home)[0]:home]
        if not (re.search(r"(?:\bto\b|\buntil\b|\binstead\b|\bnow\b|->|→|=>|至|到|为|為|改在"
                          r"|新的?(?:时间|時間|日期)|\bnew\s+(?:time|date|window|schedule)\b)[^\n]{0,30}$",
                          lead, re.I) or re.search(r"(?i)(?:rescheduled|postponed|moved|shifted|delayed"
                                                   r"|deferred|changed|改期|延期|推迟|推遲|调整|調整|变更|變更)"
                                                   r"\s*[:：]?\s*(?:on\s+|for\s+)?$", lead)):
            return ("the notice moves the maintenance, but its only window {} is not "
                    "stated as the new one".format(_fmt_window(st, en)))
    # 8. A zone the window did not use: an unread zone name, or a bare UTC/GMT
    #    beside a window read on +08 with no +08 token anywhere.
    for m in _DOUBT_ZONE_WORD_RE.finditer(t):
        # A zone the window DID take ("Vietnam time (GMT+7)", "PST (Pacific
        # Standard Time)"), or one written right beside a zone token the
        # parser reads ("(GMT+8) local time"), is accounted for.
        if st.utcoffset() != timedelta(hours=8):
            break
        generic = re.match(r"(?i)(?:local|server|system|platform|provider|studio|our)\s+time"
                           r"|当地时间|當地時間|系统时间|系統時間|服务器时间|伺服器時間", m.group(0))
        lzs, lze = _line_at(t, m.start())
        if generic and (_TZ_RE.search(t, lzs, lze) and not re.match(
                r"(?:系统|系統|服务器|伺服器)", m.group(0))):
            continue
        return ("the notice reads as {}, but it names a time zone the parser does "
                "not read ({}) - check the hours".format(_fmt_window(st, en), quote(m)))
    if (st.utcoffset() != timedelta(0) and _DOUBT_BARE_UTC_RE.search(t)
            and not _DOUBT_PLUS8_RE.search(t)):
        return ("the notice reads as {} on +08, but it says UTC/GMT without an "
                "offset - check the hours".format(_fmt_window(st, en)))
    # 10. A second window in the window's own SENTENCE, or an old/new marker
    #     beside a listed other window: "X -> Y", "X has been updated to Y",
    #     "X 更改为 Y", "Y (10:00-12:00 was wrong)", "Previously shared: X.
    #     Latest: Y", "最新维护时间：Y". Which one stands is for a person.
    ss0, se0 = _sentence_bounds(t, home, home)

    def _excused(m):
        """A zone copy of the window, or a later window the verdict lists -
        unless a change marker stands between the two ("X -> Y", "updated to",
        "更改为", "not X", "X was wrong")."""
        lo, hi = sorted((home, m.start()))
        if _DOUBT_CHANGE_RE.search(t, lo, hi + 1) or _DOUBT_CHANGE_RE.match(
                t, m.end(), min(len(t), m.end() + 14)):
            return False
        listed = [o for o in v.get("others") or [] if _names_clock(m.group(0), o[0])]
        if listed and (any(o[0].date() != st.date() for o in listed) or re.search(
                r"(?:\band\b|&|、|，|,|及|和|以及|;|；)\s*$", t[max(0, m.start() - 8):m.start()])):
            # A later window the verdict lists (another night, "... and
            # 14:00 - 15:00") - unless it is the zone copy of this one.
            return True
        oc0, oc1 = _clause_span(t, m.start(), m.end())
        if _TOPIC_OFF_RE.search(t, oc0, oc1) or _OFF_PREDICATE_RE.search(t, m.end(), oc1 + 1):
            return True
        zm = (_TZ_RE.search(t, m.end(), min(len(t), m.end() + 12))
              or _TZ_RE.search(t, max(0, m.start() - 10), m.start()))
        if not zm and not re.search(r"[(（/]\s*$", t[max(0, m.start() - 3):m.start()]):
            return False
        # A zone COPY states the same instant: convert its start with its own
        # zone and compare ("PST, i.e. 02:00 - 04:00 UTC+8" is not 10:00 +08,
        # and "(10:00-12:00 was wrong)" beside 14:00-16:00 is no copy at all).
        c0 = _CLOCK_TOKEN_RE.search(m.group(0))
        off = _offset_of(zm) if zm else None
        if c0 is None or off is None:
            return False
        mins = (int(c0.group(1)) * 60 + int(c0.group(2)) - off) % 1440
        su = st.astimezone(timezone.utc)
        return mins == (su.hour * 60 + su.minute) % 1440

    sent_ranges = [] if v.get("reschedule") else [
        m for m in _ANY_RANGE_RE.finditer(t, ss0, se0)
        if not any(a <= m.start() < b for a, b in spans) and not _excused(m)]
    for (a1, b1), (a2, b2) in zip(spans, spans[1:]):
        last = list(_CLOCK_TOKEN_RE.finditer(t, a2, b2 + 1))
        if last and _names_clock(last[0].group(0), st) and _names_clock(last[-1].group(0), en):
            # The later piece IS the window ("extended from 10:00 - 12:00 to
            # 10:00 - 14:00"): the change named the window being written.
            continue
        if _DOUBT_CHANGE_RE.search(t, b1, a2) or _DOUBT_NEW_RE.search(t, b1, a2):
            return ("the notice reads as {}, joined from two times with a change "
                    "between them (“{}”) - check which one stands".format(
                        _fmt_window(st, en), _quote(t[b1:a2], 30)))
    # "... 10:00 - 12:00 PST, i.e. 02:00 - 04:00 UTC+8": an explicit
    # conversion, whatever the sentence splitter makes of "i.e.".
    ls2, le2 = _line_at(t, home)
    for m in _ANY_RANGE_RE.finditer(t, ls2, le2):
        if any(a <= m.start() < b for a, b in spans) or m in sent_ranges:
            continue
        if re.search(r"(?:i\.\s?e\.|e\.\s?g\.|=|即|换算|換算|converted|equivalent|equals?)[^\n]{0,12}$",
                     t[max(ls2, m.start() - 16):m.start()], re.I) and not _excused(m):
            sent_ranges.append(m)
    if sent_ranges:
        return ("the notice reads as {}, but the same sentence states another time "
                "(“{}”) - check which one is the maintenance".format(
                    _fmt_window(st, en), _quote(sent_ranges[0].group(0), 30)))
    other_any = [m for m in _ANY_RANGE_RE.finditer(t)
                 if not any(a <= m.start() < b for a, b in spans) and not _excused(m)]
    other_all = [m for m in _ANY_RANGE_RE.finditer(t)
                 if not any(a <= m.start() < b for a, b in spans)]
    other_any = other_all
    if other_any and not v.get("reschedule"):
        def _lead_of(pos):
            ls_, _le = _line_at(t, pos)
            return t[max(ls_, _sentence_bounds(t, pos, pos)[0]):pos]
        mine = _lead_of(home)
        old_mine = _DOUBT_OLD_RE.search(mine)
        new_other = next((m for m in other_any if _DOUBT_NEW_RE.search(_lead_of(m.start()))), None)
        if old_mine or (new_other and not _DOUBT_NEW_RE.search(mine)):
            return ("the notice reads as {}, but it marks {} - check which one stands".format(
                _fmt_window(st, en),
                "that window as the old one (“{}”)".format(old_mine.group(0)) if old_mine else
                "another window as the new one (“{}”)".format(_quote(new_other.group(0), 30))))
    # 11. The window is asked about, or a sentence checks it.
    for qs, qe in (_question_sentences(t) if _on("NOTICE_QUESTION_GUARD") else []):
        if any(qs <= a < qe for a, _b in spans) or _DOUBT_CHECK_WORD_RE.search(t, qs, qe):
            return ("the notice reads as {}, but a question asks about it (“{}”) - a "
                    "check, not an announcement".format(_fmt_window(st, en),
                                                        _quote(t[qs:qe], 50)))
    # 12. The window's clause names a desk, a channel or another system as
    #     what is down or open ("CS online 09:00 through 18:00", "support 09:00
    #     到 18:00", "The ticketing system will be down 09:00-13:00", "our email
    #     service will be unavailable").
    for a, b in spans:
        cs, ce = _clause_span(t, a, b)
        mm = _DOUBT_DESK_RE.search(t, cs, ce)
        if mm:
            return ("the window {} is stated for “{}”, not the provider's games".format(
                _fmt_window(st, en), mm.group(0)))
    # 13. A date token right against the window that is not its date ("9/30
    #     10:00-12:00" read on 10月1日, "10:00-12:00 (GMT+8), 24/09").
    ls1, le1 = _line_at(t, home)
    near = t[max(ls1, (clocks[0].start() if clocks else home) - 10):(clocks[0].start() if clocks else home)]
    near += " " + t[post_at:min(le1, post_at + 22)]
    for dm in re.finditer(r"(?<![\d/.\-+A-Za-z])(\d{1,2})[/.\-](\d{1,2})(?![\d/\-]|\.\d|:\d|\s*[-–~]\s*\d)", near):
        x, y = int(dm.group(1)), int(dm.group(2))
        if not ((1 <= x <= 12 and 1 <= y <= 31) or (1 <= y <= 12 and 1 <= x <= 31)):
            continue
        if (st.month, st.day) not in ((x, y), (y, x)) and (en.month, en.day) not in ((x, y), (y, x)):
            return ("the notice reads as {}, but “{}” stands against the window - "
                    "check the day".format(_fmt_window(st, en), dm.group(0)))
    for wm in re.finditer(r"(?<![A-Za-z])(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*\.?,?[ \t]+(\d{1,2})"
                          r"(?:st|nd|rd|th)?\b(?![ \t]*[:：.]\d)", t):
        dn = int(wm.group(1))
        if 1 <= dn <= 31 and dn not in (st.day, en.day, (st - timedelta(hours=6)).day):
            return ("the notice reads as {}, but it also names “{}” - check the day".format(
                _fmt_window(st, en), wm.group(0)))
    if _NEW_TBA_RE.search(t) and _DOUBT_OLD_RE.search(t):
        return ("the notice reads as {}, but it marks that window as the old one and "
                "the new one as still to come".format(_fmt_window(st, en)))
    # 14. Shapes that are never a plain announcement: a duration read as a
    #     clock, "-> TBA", a struck-through notice, a label answer ("服务中断：无",
    #     "Maintenance: none"), region lines, a test environment named as the
    #     scope, an imperative move request, the provider named only as
    #     "offline" with no maintenance word.
    for rx, why in _DOUBT_SHAPES:
        mm = rx.search(t)
        if mm:
            return ("the notice reads as {}, but {} (“{}”)".format(
                _fmt_window(st, en), why, _quote(mm.group(0), 40)))
    if not _DOUBT_NAMED_RE.search(t) and not (RESCHEDULE_RE.search(t) and not re.search(
            r"\b(?:draw|tournament|promo\w*|event|webinar|meeting|call|payout|withdrawal|campaign"
            r"|launch|release|lottery|raffle|match|game\s+launch)\b|活动|活動|抽奖|抽獎|比赛|比賽|直播",
            t, re.I)):
        return ("the notice reads as {}, but it never names a maintenance - check "
                "who is down".format(_fmt_window(st, en)))
    # 9. A date the window does not account for: a future date the notice
    #    names that neither the window nor a listed later window is on
    #    ("Last updated: 2026-09-22 ... Date: 2026-09-24", "Thursday 24th -
    #    Friday 25th", a holiday date), or a label on the window's own line
    #    that names something other than the maintenance ("CS: 09:00-18:00").
    try:
        tz = st.tzinfo
        ds = _dates(t, now=now, tz=tz)
    except Exception:          # noqa: BLE001 - a guard must not raise
        ds = []
    used = {st.date(), en.date(), (st - timedelta(hours=6)).date()}
    for o in v.get("others") or []:
        try:
            used |= {o[0].date(), o[1].date()}
        except Exception:      # noqa: BLE001
            pass
    today = (now or datetime.now(st.tzinfo)).astimezone(st.tzinfo).date()
    used |= set(v.get("_cand_dates") or ())
    for p, e, ymd in ds:
        try:
            dd = date(*ymd)
        except Exception:      # noqa: BLE001
            continue
        if dd in used:
            continue
        if dd <= today and not _on_date_label_line(t, p):
            # A day not after the post's own is its posting date or a past
            # maintenance - never the one being announced. A "Date:" / 日期 line
            # is the window's own, whatever the day it is read on.
            continue
        if v.get("reschedule") and p < home and _sentence_bounds(t, p, p) == _sentence_bounds(t, home, home):
            # "The maintenance on 24/09 has been rescheduled to 25/09 ..." - the
            # old day, named before the new window in the move's own sentence.
            continue
        if re.search(r"(?:\bfrom|\binstead\s+of|\brather\s+than|\bnot|\bNOT|原定|原先|原本|原计划"
                     r"|原計劃|原订|原訂|而非|不是|\bpreviously|\boriginally|\bwas)\s*(?:on\s+|for\s+)?"
                     r"[^\n]{0,6}$", t[max(0, p - 24):p], re.I):
            continue
        line = _line_text(t, p)
        if _LETTERHEAD_RE.search(line):
            continue
        # Another thing's own datetime ("充值通道将于 2026-09-22 20:00-23:59
        # 暂停"), a holiday in brackets ("国庆节(10月1日)"), or the NEXT one
        # ("The next one is planned for 2026-10-15", "下次维护：2026-10-15").
        if re.match(r"[^\n]{0,14}?\d{1,2}\s*[:：]\s*\d{2}", t[e:]):
            continue
        if re.search(r"(?:国庆|國慶|中秋|春节|春節|元旦|新年|假期|假日|节日|節日|holiday|national\s+day"
                     r"|mid-?autumn|golden\s+week|christmas|new\s+year)[^\n]{0,6}[(（\[]?[^)）\]\n]{0,20}$",
                     t[max(0, p - 40):p], re.I):
            continue
        sent = _sentence_text(t, p)
        if _NEXT_M_RE.search(sent) or re.search(r"\bnext\s+(?:one|time|window)\b|下次|下一次",
                                                sent, re.I):
            continue
        return ("the notice reads as {}, but it also names {} ({}) and no window "
                "is on that day - check which day is meant".format(
                    _fmt_window(st, en), dd.isoformat(), _quote(t[p:e], 30)))
    # Hunt h04 / l05: the window's own line states no zone, and the only zone
    # in the notice sits on a line that does not lend it (an office, a
    # signature, a desk, another market's conversion) - or another range is
    # stated in an explicit zone and is not the same instant. The window was
    # read on the home clock; whose clock the provider meant is for a person.
    wls, wle = _line_at(t, home)
    if not _TZ_RE.search(t, wls, wle) and not _ALL_TIMES_RE.search(t):
        rsp = [(m.start(), m.end()) for m in _ANY_RANGE_RE.finditer(t)]
        zms = list(_TZ_RE.finditer(t)) + list(_FOREIGN_ZONE_RE.finditer(t))
        for zm in zms:
            zls, zle = _line_at(t, zm.start())
            if (zls, zle) == (wls, wle) or _lends_zone(t, zm.start(), rsp):
                continue
            if zm.re is _TZ_RE and _offset_of(zm) is None and not zm.groupdict().get("abbr"):
                continue
            rng = [m for m in _ANY_RANGE_RE.finditer(t, zls, zle)]
            if rng and all(_excused(m) for m in rng):
                continue           # a same-instant conversion copy
            if not rng and not _NO_LEND_RE.search(t, zls, zle):
                continue
            if (not rng and _DOUBT_DESK_RE.search(t, zls, zle)
                    and not re.search(r"(?i)office|HQ|headquarters?|regards|sent\s+from|办公室|辦公室|总部|總部|发布|發佈", t[zls:zle])):
                continue           # a desk's own hours in its own zone
            return ("the notice reads as {} on the home clock, but its only other "
                    "zone (“{}”) is on a line about something else - check whose "
                    "time is meant".format(_fmt_window(st, en), _quote(t[zls:zle], 40)))
    # F4: "Following the 2026-09-24 maintenance, the next scheduled maintenance
    # is 10:00 - 12:00 on Thursday" - the only date is the PREVIOUS one's, and
    # the window was put on it.
    if _NEXT_M_RE.search(win_sent):
        for p, e, ymd in ds:
            try:
                dd = date(*ymd)
            except Exception:      # noqa: BLE001
                continue
            if dd == st.date() and re.search(
                    r"(?i)(?:\bfollowing|\bafter|\bsince|继|繼|在)\s*(?:the\s+|our\s+)?[^\n,，]{0,6}$",
                    t[max(0, p - 20):p]) and re.match(r"[^\n,，]{0,16}?(?:maint\w*|维护|維護)", t[e:], re.I):
                return ("the notice reads as {}, but its only date is the previous "
                        "maintenance's (“{}”) - check the day".format(_fmt_window(st, en),
                                                                   _quote(t[max(0, p - 14):e + 14], 40)))
    for a, _b in spans:
        lab = _DOUBT_LABEL_RE.match(_sentence_text(t, a).lstrip())
        piece = [c for c in _CLOCK_TOKEN_RE.finditer(t, a, _b + 1)]
        edges = any(_names_clock(c.group(0), st) or _names_clock(c.group(0), en) for c in piece)
        name = lab.group(1).strip() if lab else ""
        brand = re.fullmatch(r"(?:[A-Z][\w&'.-]*)(?:[ \t]+[A-Z0-9][\w&'.-]*){0,3}", name)
        if lab and edges and not _DOUBT_MAINT_RE.search(name) and (
                _DOUBT_LABEL_DENY_RE.search(name)
                or not (brand or _DOUBT_LABEL_OK_RE.search(name))):
            return ("the window {} is read from a line labelled “{}”, which does not "
                    "name the maintenance".format(_fmt_window(st, en), lab.group(1).strip()))
    return None


def classify(text: str, *, now=None) -> dict:
    """See _classify. F82: never raises on str input.

    A date the calendar cannot hold ("9999-12-31 22:00 - 23:00 (GMT-5)" moved
    to +08, or a day added past year 9999) raised OverflowError out of the
    window ranking, and vawatch logged the bubble as an error on every sweep.
    Such input is unparseable, not a crash: it comes back ``ignore``, with
    ``undated`` set when the message names maintenance so vawatch still cards
    it at the reparse cap.
    """
    try:
        v = _classify(text, now=now)
        spans = v.pop("_spans", None)
        cdates = v.pop("_cand_dates", None)
        if v["action"] == "fill":
            why = _doubt(_norm((text or "").strip()),
                         dict(v, _spans=spans, _cand_dates=cdates or set()), now)
            if why:
                # The reason names the window; the verdict carries none, like
                # every other needs_human, so nothing downstream treats it as one.
                v = dict(v, action="needs_human", reason=why, refused="doubt",
                         start=None, end=None, stale=False, others=[])
        return v
    except (OverflowError, ValueError) as ex:
        named = bool(_TOPIC_STRONG_RE.search(text or ""))
        return {"action": "ignore", "reason": "unparseable date/time ({})".format(
                    type(ex).__name__), "start": None, "end": None,
                "reschedule": False, "stale": False, "others": [], "refused": None,
                "undated": 1 if named else 0}


def _classify(text: str, *, now=None) -> dict:
    """-> {action, reason, start, end, reschedule, stale, others, refused}

    action is one of 'fill' | 'clear' | 'follow_quote' | 'needs_human' | 'ignore'.
    ``stale`` means the window has already passed; the caller decides whether to
    write it (a passive backlog read should not, a deliberate re-read may).
    ``others`` lists the further upcoming windows [(start, end), ...] a fill
    notice states but the row does not get - a phased or multi-night notice
    fills the earliest one, and the rest are named here and in ``reason``
    rather than dropped without a word.
    ``refused`` names the guard that kept a maintenance-worded message from
    filling (F63), so a caller can tell "not about maintenance" from "about
    maintenance, and deliberately not written": None, or one of
      'question'        the message asks about maintenance (NOTICE_QUESTION_GUARD)
      'negated'         the only gate phrase is negated (NOTICE_NEGATION_GUARD)
      'no_outage'       the notice promises no downtime (NOTICE_NEGATION_GUARD)
      'nonprod'         UAT / staging / 测试环境 maintenance (NOTICE_NONPROD_GUARD)
      'no_maint_asked'  "no maintenance" inside a question or reply-with request
      'request'         an OPERATOR request - to move the provider's
                        maintenance, or to hold releases/callbacks during a
                        maintenance - not a provider announcement; always
                        needs_human (G4.3, NOTICE_REQUEST_GUARD)
    A reschedule question ("can the maintenance be postponed to X?") is
    'question' too (F20). The reason text says the same in words; none of
    them reads "no scheduled-maintenance wording".
    ``undated`` is set only on the "maintenance wording but no parseable
    date/time window" ignore: how many readable time ranges the message
    carries that no date belongs to (0 = it states no clock at all).
    ``reschedule`` is also True on a fill of a CORRECTION that names the
    window it withdraws ("Wrong: X / Correct: Y", "Y, not X") - it overrides
    the row the same way (G2.2).
    """
    raw = (text or "").strip()
    out = {"action": "ignore", "reason": "", "start": None, "end": None,
           "reschedule": False, "stale": False, "others": [], "refused": None}
    if not raw:
        out["reason"] = "empty message"
        return out

    # Parse a normalised COPY; the caller still writes `text` to Remark.
    t = _norm(raw)
    now = _aware(now, _tz_default())

    # F20/G4.3: whether this is a reschedule is decided BEFORE the windows
    # are read, because a reschedule (or a correction) reads them differently
    # - the window it moves away from is flagged superseded (_mark_moved).
    rs_hit, rs_refusal = _reschedule_match(t)
    resched = rs_hit is not None
    # A correction names the wrong window beside the right one (G2.2).
    corrected = bool(_CORRECTION_RE.search(t))

    cands, dropped = _collect(t, now, moves=resched or corrected)
    # Policy 7: a window in the clause of a completion or cancellation is the
    # window being completed / withdrawn. It leaves the candidate list BEFORE
    # ranking (F25, F49): testing only the chosen union's first span merged a
    # cancelled sibling into the row ("Scheduled maintenance 09-23 10:00-12:00.
    # Maintenance for 09-23 14:00-16:00 has been cancelled." wrote 10:00-16:00)
    # and let a cancelled window that sorted first throw a live later one away.
    # Removed windows still count as ranges the message carries (``dropped``),
    # so step 3 cannot stamp "No maintenance" on them.
    done = _done_spans(t)
    scopes = _done_scopes(t, done)
    gone = [c for c in cands if _in_scopes(c, scopes)]
    if gone:
        cands = [c for c in cands if not _in_scopes(c, scopes)]
        dropped += len(gone)
    detail = _resolve(cands, now)
    if detail and detail["ambiguous"] is None:
        foot = _all_times_doubt(t, detail)
        if foot:
            detail = dict(detail, ambiguous=foot)
    win = (detail["start"], detail["end"]) if detail else None
    win_superseded = bool(detail and detail["superseded"])
    # A reschedule or a correction states the OLD window as well as the new
    # one. When ranking has not told them apart - no Original/原定/"instead of"
    # marker demoted one, no move verb made one its subject - the notice still
    # carries two separate windows, and the earliest-first rule would write
    # whichever came first: "postponed from 09-23 10:00-12:00 to 09-24
    # 10:00-12:00" wrote the withdrawn 23rd. ``corrected`` is read above.
    # A window the notice states but that cannot be written as it stands: two
    # dates for one range, two zones that disagree, a row that could be either
    # day, a span longer than NOTICE_MAX_WINDOW_HOURS. Per the overriding rule
    # a wrong window on the shared sheet is worse than none, so every path
    # that would FILL asks a human instead.
    ambiguous = detail["ambiguous"] if detail else None
    # A later window in a sentence that cancels or completes it is not one the
    # row is missing: "maintenance on 09-24 02:00-04:00 has been cancelled.
    # Scheduled maintenance 09-23 10:00-12:00 is still on." lists no others.
    others = [(s, e) for s, e, at in (detail["others"] if detail else [])
              if all(_sentence_index(t, at) != _sentence_index(t, d[0])
                     for d in done)]

    def _fill_reason(base):
        if not others:
            return base
        return "{}; the notice also states {} later window{} NOT written: {}".format(
            base, len(others), "" if len(others) == 1 else "s",
            ", ".join(_fmt_window(s, e) for s, e in others))

    # 1a. An extended maintenance (F50) is a reschedule of the END only.
    ext = _extension(t, detail, cands, now)
    if ext is not None:
        out.update(ext)
        return out

    # 0. Reschedule wording that only ASKS for a move (F20) or is the
    #    OPERATOR requesting one (G4.3). Nothing the provider announced moved:
    #    a question is ignored, like any question about maintenance, and a
    #    request goes to a person without a write - the row changes when the
    #    provider confirms.
    if not resched and rs_refusal == "request":
        out.update(action="needs_human", refused="request", reason=(
            "an operator request to move the maintenance, not a provider "
            "announcement - nothing is written"))
        return out
    if not resched and rs_refusal == "question":
        out.update(refused="question", reason=(
            "a question about moving the maintenance, not a notice"))
        return out

    # 1. A reschedule OVERRIDES whatever is on the row - checked before the
    #    completed/cancelled test, which would otherwise swallow "postponed",
    #    and before it, "维护延后至X，原定Y的维护取消" was dropped whole and the
    #    provider went down on the 25th with the row saying nothing.
    if resched:
        if not win:
            out.update(action="needs_human", reschedule=True,
                       reason="rescheduled, but no new window could be read")
            return out
        if win_superseded:
            # "postponed from <window>; new date to follow" - the only window in
            # the message is the one being CANCELLED. Writing it would re-assert
            # the very schedule the provider just withdrew.
            out.update(action="needs_human", reschedule=True,
                       reason="rescheduled, but the only window found is the "
                              "superseded one")
            return out
        if ambiguous:
            out.update(action="needs_human", reschedule=True,
                       reason="rescheduled, but the new window is ambiguous: "
                              + ambiguous)
            return out
        if others:
            out.update(action="needs_human", reschedule=True,
                       reason="rescheduled, but the notice states {} separate "
                              "windows and does not mark which one is the old "
                              "one".format(len(others) + 1))
            return out
        if len(detail["pieces"]) > 1 and win not in detail["pieces"]:
            # F22: two windows close enough to join (the old 10:00-12:00 and
            # the new 14:00-16:00 of a same-day move) and neither marked old.
            # Their union was never announced by anyone.
            out.update(action="needs_human", reschedule=True,
                       reason="rescheduled, but the notice states {} windows "
                              "({}) and does not mark which one is the old "
                              "one".format(len(detail["pieces"]), ", ".join(
                                  _fmt_window(s, e)
                                  for s, e in detail["pieces"])))
            return out
        # The same vetoes as an ordinary notice: a UAT reschedule, or an
        # upgrade "postponed to <window>. There will be no downtime.", moves
        # nothing on the production row.
        veto = _veto(t, detail["at"], rs_hit)
        if veto:
            # reschedule stays False: vawatch's card would otherwise tell the
            # Laboratory group the production row "still shows the superseded
            # window", and nothing on that row was superseded.
            out.update(action=veto[0], reason=veto[1], refused=veto[2])
            return out
        if _NEW_TBA_RE.search(t) and not others:
            # R1.30: "<X> — change of plan, an updated schedule will be shared
            # shortly": a reschedule whose NEW window is still to come. The one
            # window it carries is therefore the old one, even with no word
            # marking it so - and it was written as "(rescheduled)".
            out.update(action="needs_human", reschedule=True,
                       reason="rescheduled, and the new window is still to be "
                              "announced - the only window found is the old one")
            return out
        out.update(action="fill",
                   reason=_fill_reason("rescheduled maintenance (overrides)"),
                   start=win[0], end=win[1], reschedule=True, others=others,
                   stale=is_stale(win[1], now=now), _spans=detail["spans"],
                   _cand_dates=_cand_dates(cands, gone))
        return out

    # 2. Completed / cancelled. A completion notice repeats the original window
    #    verbatim, so it has to win over any window-based rule. The windows in
    #    its own clause are already gone (above); a window elsewhere survives
    #    only on the terms _done_excused spells out - a different window of
    #    its own, a past maintenance, or a later NEXT one. "上次维护已完成。下次
    #    例行维护将于 X 进行。" is a report and an announcement in one bubble,
    #    and so is the same with a comma (F48); "【取消维护通知】\n例行维护时间：
    #    X\n本次维护取消。" is one cancellation (F24).
    if done:
        at = detail["at"] if detail else None
        win_past = at is not None and any(
            c["past"] for c in cands if c["span"][0] == at)
        fresh = (at is not None
                 and _done_excused(t, done, scopes, gone, detail, win_past)
                 # ...and the surviving window's OWN sentence has to be about
                 # maintenance. Sentence-scoping alone let any unrelated time
                 # range rescue a cancellation: "New game launch 10:00 - 12:00
                 # GMT+8. The scheduled maintenance has been cancelled." wrote
                 # the launch window as an outage. The label and verb shapes
                 # of the plain gate count (G1.6 "Next maintenance: <window>"),
                 # and so does the NEXT marker that excused it.
                 and bool(_sched_re().search(_sentence_of(t, at))
                          or _NEXT_M_RE.search(_sentence_of(t, at))
                          or _UPCOMING_M_RE.search(_sentence_of(t, at))
                          or _PRONOUN_MAINT_RE.match(_sentence_of(t, at).lstrip())
                          or _anchored_hit(t, at) is not None))
        if not fresh:
            out["reason"] = "maintenance completed / cancelled notice"
            return out

    # 2b. A retraction (G2.1). It restates the withdrawn window as often as
    #     not, so it must not reach the fill below. One with NO window stays
    #     with the rules below (normally `ignore`): vawatch cards that kind
    #     itself, and only while the notice it retracts is on screen above it
    #     (G2.3) - a parser card for every "please ignore the above" would
    #     fire on ordinary chat days later.
    retract = _retraction(t) if win else None
    if retract:
        out.update(action="needs_human", reason=(
            "the provider retracts or voids a notice (“{}”){} - nothing is "
            "written; check whether the row carries the withdrawn "
            "window".format(retract.group(0).strip(),
                            " stating " + _fmt_window(*win) if win else "")))
        return out

    # 3. An explicit answer that there is nothing planned. ``dropped`` keeps
    #    this exactly as strict as it was before ranges had to belong to the
    #    maintenance statement: a message carrying ANY readable range is not
    #    stamped "No maintenance" on the strength of a regex.
    nm_note = None
    if not win and not dropped and NO_MAINT_RE.search(t):
        if _no_maint_answer(t):
            out.update(action="clear", reason="provider says no maintenance")
            return out
        # F64: asked or quoted, not answered. Falls through - a referral or a
        # dateless notice later in the message still gets its own verdict.
        nm_note = ("no_maint_asked",
                   "“no maintenance” is asked or quoted (a question, or a "
                   "reply-with instruction), not answered")

    # 4. An ordinary upcoming notice.
    hit = _sched_hit(t) if win else None
    if win and not hit and not _asks_rather_than_tells(t):
        # G1.1 / G1.2: the plain wording, where it governs the window.
        hit = _anchored_hit(t, detail["at"]) or _next_hit(t, detail["at"])
    if hit:
        veto = _veto(t, detail["at"], hit)
        if veto:
            out.update(action=veto[0], reason=veto[1], refused=veto[2])
            return out
        if ambiguous:
            out.update(action="needs_human",
                       reason="maintenance notice, but the window is "
                              "ambiguous: " + ambiguous)
            return out
        if corrected and others:
            out.update(action="needs_human",
                       reason="a correction that states {} separate windows "
                              "without marking which one is withdrawn".format(
                                  len(others) + 1))
            return out
        if win_superseded and not corrected:
            # R1.37 / R1.3: the only window left is one the notice itself marks
            # as the old one ("Original date: <X>\nNew date: to be announced",
            # "原定 <X> 的维护暂停，新的时间将另行通知") - step 1 handles this for a
            # reschedule verb, but plain wording reached here and wrote it.
            out.update(action="needs_human", reschedule=True,
                       reason="the only window found is the one the notice "
                              "marks as superseded")
            return out
        if corrected and win_superseded:
            # "Wrong: 24/09 10:00-12:00" with no right window beside it: the
            # only window is the one the correction withdraws.
            out.update(action="needs_human", reschedule=True,
                       reason="a correction, but the only window found is "
                              "the one it withdraws")
            return out
        # G2.2: a correction that names its wrong window ("Wrong: X / Correct:
        # Y", "Y, not X", 而非 X) OVERRIDES the row exactly like a reschedule
        # - the row very likely holds X from the notice being corrected, and
        # an ordinary fill of a later Y beside a live X would be carded as a
        # second outage instead of landing.
        fixed = corrected and any(c["superseded"] for c in cands)
        out.update(action="fill", reason=_fill_reason(
                       "corrected maintenance window (overrides)" if fixed
                       else "scheduled maintenance"),
                   start=win[0], end=win[1], others=others, reschedule=fixed,
                   stale=is_stale(win[1], now=now), _spans=detail["spans"],
                   _cand_dates=_cand_dates(cands, gone))
        return out

    # 5. A short reply pointing at another message - the answer is in the quote.
    if len(t) <= _REFERRAL_MAX_CHARS and REFERRAL_RE.search(t) and not win:
        out.update(action="follow_quote",
                   reason="the answer is in the message this one quotes")
        return out

    gate = _sched_hit(t) or _plain_hit(t)
    if gate and win:
        # A window that is about maintenance on its own terms (it survived
        # _relevant) and plain maintenance wording that is NOT on its line, in
        # its sentence or over its block. Neither side can be told to govern
        # the other, and "no parseable date/time window" would be false - a
        # person decides. Nothing in the corpora reaches this today.
        out.update(action="needs_human", reason=(
            "maintenance wording and a window {}, but the wording is not on "
            "the window's line, in its sentence or over its block".format(
                _fmt_window(*win))))
        return out
    if gate:
        rel = _relative_when(t, gate)
        if rel:
            # G1.3: a maintenance notice dated only "today" / "明天" / "this
            # Thursday". The day cannot be resolved without the post's own date
            # (see _RELATIVE_DAY_RE), and `ignore` here was a silent drop after
            # three reparse sweeps. needs_human writes nothing and cards it.
            out.update(action="needs_human", reason=(
                "maintenance notice dated only '{}' - the post's own date is "
                "not captured, so the day cannot be resolved; fill the row by "
                "hand".format(rel)))
            return out
        # Policy 9: with no day at all this stays `ignore` - vawatch retries an
        # unparsed notice (it may be a half-rendered bubble) before giving up.
        out["reason"] = "maintenance wording but no parseable date/time window"
        # ...and ``undated`` tells it whether the notice stated a CLOCK that
        # no date belongs to ("Emergency maintenance 15:00-16:00 (GMT+8)").
        # Such a notice was retried three times and then dropped with no card
        # (G1.3); vawatch cards it at the reparse cap. An ack such as "Thanks
        # for the maintenance notice" states no clock, so it stays silent.
        out["undated"] = dropped
        if not dropped and _LONE_CLOCK_RE.search(t) and not _ACK_OPENER_RE.match(t):
            # G1.3: a single clock ("now until 16:00", "ETA 16:00", 预计16:00恢复)
            # is a stated time too - and the only one an emergency gives.
            out["undated"] = 1
    else:
        why = _gate_refusal(t) or nm_note
        if why:
            out.update(refused=why[0], reason=why[1])
        elif win and _on("NOTICE_DOUBT_GUARD") and _named_but_unworded(t):
            # The clean-notice rule, the other way round: the message names a
            # maintenance and states a readable window, but in wording none of
            # the rules trusts ("Reminder: maintenance <X>", "we are doing
            # maintenance on <X>", 维护定于 <X>, "Maintenance date change: ...").
            # A terminal ignore was a silent drop of a real notice; a person
            # reads it instead, and nothing is written.
            out.update(action="needs_human", refused="doubt", reason=(
                "the message names a maintenance and a window {}, but not in "
                "wording the parser can confirm - check it".format(_fmt_window(*win))))
        else:
            out["reason"] = "no scheduled-maintenance wording"
    return out


if __name__ == "__main__":
    import json
    import sys

    text = sys.stdin.read() if not sys.argv[1:] else open(
        sys.argv[1], encoding="utf-8").read()
    v = classify(text)
    print(json.dumps({"action": v["action"], "reason": v["reason"],
                      "reschedule": v["reschedule"],
                      "start": str(v["start"]), "end": str(v["end"])},
                     ensure_ascii=False, indent=2))
