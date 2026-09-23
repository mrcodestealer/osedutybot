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
table does not know, the same window stated twice 12h apart, a row that could
fall on either side of midnight, a reschedule or correction that still states
two windows without marking the old one, the earliest window only described
while the named maintenance is later, and anything longer than
NOTICE_MAX_WINDOW_HOURS.

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

ENV FLAGS (all documented in .env.example; every one defaults to the SAFE value)
    NOTICE_TZ           assumed zone when a notice states none (Asia/Manila)
    NOTICE_DATE_ORDER   dmy | mdy — only consulted when BOTH components are <=12
    NOTICE_YEARLESS     1 | 0     — infer the year of "9/23", "9月23日", "Sep 23"
    NOTICE_WORDING      wide | strict — which headings open the gate
    NOTICE_WINDOW_PICK  best | first  — how a multi-window notice is resolved
    NOTICE_MAX_WINDOW_HOURS  48 | <h> | 0 — a longer window goes to a human
"""

from __future__ import annotations

import os
import re
import unicodedata
from datetime import date, datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Env flags. Read at CALL time, never at import time: the corpus pins them in
# os.environ before importing this module, and an operator flipping one in
# .env must not have to restart the bot to see it take effect.
# ---------------------------------------------------------------------------


def _flag(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None or not v.strip() else v.strip().lower()


def _on(name: str, default: str = "1") -> bool:
    return _flag(name, default) not in ("0", "false", "no", "off")


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
    "〜": "~",      # wave dash (NFKC leaves this one alone)
    "～": "~",      # fullwidth tilde
}
_NFKC_ONE = {}


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
    """
    if not text:
        return ""
    out = []
    for ch in text:
        r = _WIDE_MAP.get(ch)
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
    (re.compile(_MONTH_WORD + r"\.?\s*(\d{1,2})" + _ORD +
                r"(?!\s*,?\s*\d{4})(?![\d:.])", re.I),
     lambda m: (None, _mword(m), int(m.group(1)))),
    # 24th Sep / 24 September / 24th(Thu) Sep, with no year - the day-first
    # twin of the rule above, and the shape of KingMidas's own English line
    # ("commence at 10:00AM 23th(Wed) Sep"). Without it "Scheduled maintenance
    # on 24th Sep (Thu) 10:00 - 12:00" was dropped as having no date. The month
    # word must be CAPITALISED: "up to 10 may be affected" is a verb, not May.
    (re.compile(r"(?<![\d:./])(\d{1,2})" + _ORD
                + r"\s*(?:[\(（][^)）\n]{1,10}[\)）]\s*)?(?:of\s+)?(?-i:(?=[A-Z]))"
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
    r"|\bscheduled\s+for\b)[\s:：,，]*$",
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
    r"|的(?!\s*\d{1,2}\s*[:点點时時])"
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
    day_intro = bool(_RISKY_DAY_INTRO_RE.search(before))
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
            if not yearless:
                return
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

_DASH = r"(?:-{1,2}|–|—|~|～|〜|至|to|until)"
_AMPM = r"([AaPp]\.?[Mm]\.?)"

# The original colon range, unchanged. Every other shape is a separate rule
# tried afterwards, so widening the time vocabulary cannot regress it.
_RANGE_RE = re.compile(
    r"(\d{1,2}):(\d{2})\s*([AaPp][Mm])?\s*" + _DASH
    + r"\s*(\d{1,2}):(\d{2})\s*([AaPp][Mm])?")

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
    r"(?<!\d)(?<!\d:)(?<!\d\.)(\d{1,2})\.(\d{2})\s*" + _DASH
    + r"\s*(\d{1,2})\.(\d{2})(?![\d.])")

# 10AM - 12PM / 9AM-11AM. The lookbehind is what keeps it out of the minutes of
# "10:00AM - 12:00PM", where it would otherwise read "00AM".
_RANGE_HOUR_RE = re.compile(
    # The lookbehind must reject the MINUTES of "10:00AM - 12:00PM" (where it
    # would read "00AM") without rejecting a time that simply follows a label
    # colon. A flat (?<![\d:.]) did both, so "时间Time：10AM - 12PM" - the exact
    # label style the live KingMidas notice uses - produced no window at all
    # while the identical "Time: 10AM - 12PM" parsed. Only a colon with a DIGIT
    # in front of it is part of a clock.
    r"(?<!\d)(?<!\d:)(?<!\d\.)(\d{1,2})\s*" + _AMPM + r"\s*" + _DASH
    + r"\s*(\d{1,2})\s*" + _AMPM + r"(?![\d:])")

# 10时 - 12时, the mainland studios' hour marker. "24小时" cannot match because
# 小 sits between the digits and 时.
_RANGE_CJK_RE = re.compile(
    r"(?<!\d)(\d{1,2})\s*[时時]\s*" + _DASH + r"\s*(\d{1,2})\s*[时時](?!\d)")

_RANGE_RES = [
    (_RANGE_RE, lambda m: ((int(m.group(1)), int(m.group(2)), m.group(3)),
                           (int(m.group(4)), int(m.group(5)), m.group(6)))),
    (_RANGE_HOUR_RE, lambda m: ((int(m.group(1)), 0, m.group(2)),
                                (int(m.group(3)), 0, m.group(4)))),
    (_RANGE_CJK_RE, lambda m: ((int(m.group(1)), 0, None),
                               (int(m.group(2)), 0, None))),
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
    (_RANGE_DOT_RE, lambda m: ((int(m.group(1)), int(m.group(2)), None),
                               (int(m.group(3)), int(m.group(4)), None))),
]

# Even as a fallback the dotted form needs positive evidence that it is a clock.
# Required somewhere on its own line: a timezone token, a date, or a word that
# introduces a time.
_DOT_CLOCK_OK_RE = re.compile(
    r"(?:GMT|UTC)\s*[+-]|\d{4}[-/.]\d{1,2}|[年月]|日期|時間|时间|维护|維護"
    r"|\btime\b|\bdate\b|maintenance|downtime|window|schedul",
    re.I)
# ...and nothing on that line that marks the numbers as something else.
_DOT_CLOCK_VETO_RE = re.compile(
    r"[$€£¥₱]|\bUSD\b|\bEUR\b|\bPHP\b|\bRMB\b|\bCNY\b|\bMYR\b|美元|人民币|元"
    r"|\bv?\d+\.\d+\b\s*(?:client|build|release)|version|versions|\bv\d"
    r"|\bms\b|\bms\.|seconds?|hours?|minutes?|load\s+average|latency|bet|stake"
    r"|投注|下注|赔率|倍率",
    re.I)


def _h24(h: int, ampm) -> int:
    if not ampm:
        return h
    ampm = ampm.replace(".", "").lower()
    if ampm == "pm" and h != 12:
        return h + 12
    if ampm == "am" and h == 12:
        return 0
    return h


def _clock(h: int, mi: int, ampm):
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
    h = _h24(h, ampm)
    if h == 24 and mi == 0:
        return 1, 0, 0
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        raise ValueError("not a clock time: {}:{:02d}".format(h, mi))
    return 0, h, mi


def _build(ymd, t1, t2, tz):
    y, mo, d = ymd
    sday, sh, sm = _clock(*t1)
    eday, eh, em = _clock(*t2)
    try:
        start = datetime(y, mo, d, sh, sm, tzinfo=tz) + timedelta(days=sday)
        end = datetime(y, mo, d, eh, em, tzinfo=tz) + timedelta(days=eday)
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
        t = re.match(r"[\s,，]*(?:[\(（][^)）]{1,12}[\)）][\s,，]*)?"
                     r"(?:[^\s\d:]{1,4}[\s,，]*)?"
                     r"(\d{1,2}):(\d{2})\s*([AaPp][Mm])?", text[end:])
        if not t:
            continue
        out.append((pos, end + t.end(), ymd, int(t.group(1)), int(t.group(2)),
                    t.group(3)))
    out.sort(key=lambda p: p[0])
    return out


# ---------------------------------------------------------------------------
# Timezones
# ---------------------------------------------------------------------------
#
# A fixed, greppable table: no DST and no guessing. EST is -05 flat because
# these notices come from studios that mean "New York", and a parser that
# guessed EDT in summer would move a window by an hour with nothing on the page
# to justify it. A name that is NOT in this table is never invented - the notice
# falls back to NOTICE_TZ, exactly as if it had stated no zone at all.
_TZ_NAMES = {
    "SGT": 8 * 60, "MYT": 8 * 60, "PHT": 8 * 60, "CST": 8 * 60, "HKT": 8 * 60,
    "JST": 9 * 60, "KST": 9 * 60,
    "ICT": 7 * 60, "WIB": 7 * 60,
    "IST": 5 * 60 + 30,
    "EST": -5 * 60, "EDT": -4 * 60, "PST": -8 * 60, "PDT": -7 * 60,
    "北京时间": 8 * 60, "北京時間": 8 * 60,
    "新加坡时间": 8 * 60, "新加坡時間": 8 * 60,
    "中国时间": 8 * 60, "中國時間": 8 * 60,
}

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
_TZ_RE = re.compile(
    r"(?i:GMT|UTC)\s*(?P<gsign>[+-])\s*(?P<gh>\d{1,2})(?::?(?P<gm>\d{2}))?(?!\d)"
    r"|(?<![\d:])(?P<bsign>[+-])(?P<bh>\d{1,2}):(?P<bm>\d{2})(?!\d)"
    r"|\b(?P<abbr>SGT|MYT|PHT|CST|HKT|JST|KST|ICT|WIB|IST|EST|EDT|PST|PDT)\b"
    r"|(?P<cjk>北京时间|北京時間|新加坡时间|新加坡時間|中国时间|中國時間)"
    r"|\b(?i:(?P<zulu>UTC|GMT))\b"
    r"|(?<=\d)(?P<z>Z)(?![A-Za-z])")


def _tz_default():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(os.getenv("NOTICE_TZ", "Asia/Manila"))
    except Exception:
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
        off = int(m.group("gh")) * 60 + int(m.group("gm") or 0)
        if off > _MAX_OFFSET_MIN or int(m.group("gm") or 0) > 59:
            return None
        return -off if m.group("gsign") == "-" else off
    if m.group("bh") is not None:
        off = int(m.group("bh")) * 60 + int(m.group("bm") or 0)
        if off > _MAX_OFFSET_MIN or int(m.group("bm") or 0) > 59:
            return None
        return -off if m.group("bsign") == "-" else off
    for key in ("abbr", "cjk"):
        if m.group(key):
            return _TZ_NAMES.get(m.group(key))
    if m.group("zulu") or m.group("z"):
        return 0
    return None


def _tz_tokens(text: str, rspans):
    """[(start, end, minutes)] for every zone token that is not a range end.

    The bare-offset alternative happily matches the SEPARATOR of a time range:
    in "10:00 -12:00 (GMT+8)" it read " -12:00" as UTC-12 and put the window ~20
    hours out. So any candidate overlapping a time range is skipped - a range
    end is never a timezone.
    """
    out = []
    for m in _TZ_RE.finditer(text):
        if any(m.start() < e and m.end() > s for s, e in rspans):
            continue
        off = _offset_of(m)
        if off is None:
            continue
        out.append((m.start(), m.end(), off))
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
    if tokens:
        return timezone(timedelta(minutes=tokens[0][2]))
    return _tz_default()


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
_UPDATED_RE = re.compile(r"更新[后後]时间|更新[后後]時間|Updated\s+Schedule|"
                         r"新的?时间|新的?時間|Revised\s+Schedule", re.I)

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

RESCHEDULE_RE = re.compile(
    r"时间变更|時間變更|时间调整|時間調整|重新安排"
    r"|(?:维护|維護|维修|維修|停机|停機|停服)"
    r"(?:(?!" + _OBJ_ZH + r")[^。，,；;\n]){0,10}"
    r"(?:改期|延期|延后|延後|顺延|順延|改至|改到|延至|提前至|改为|改為)"
    r"|(?:改期|延期|延后|延後|顺延|順延)"
    r"(?:(?!" + _OBJ_ZH + r")[^。，,；;\n]){0,6}(?:维护|維護|维修|維修)"
    r"|Rescheduled?|Re-scheduled|Postponed|Postponement"
    r"|Updated\s+Schedule|Revised\s+Schedule"
    r"|maintenance" + _NOT_STOP + r"{0,30}\bmoved\s+(?:from|to)\b"
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
ORIGINAL_RE = re.compile(
    r"原定|原訂|原计划|原計劃|先前|previously\s+(?:set|scheduled|planned)"
    r"|originally\s+(?:set|scheduled|planned)|original\s+schedule",
    re.I)

# A line that merely restates the superseded window inside an updated block.
_ORIGINAL_LABEL_RE = re.compile(r"\s*(?:original|原(?:定|訂|计划|計劃))\s*[:：]", re.I)

# The CLAUSE-level superset of ORIGINAL_RE. Deliberately a separate constant:
# this one only demotes one candidate window, while anything added to
# ORIGINAL_RE supersedes a whole region and could black out the only window in
# a single-line notice such as "时间变更：新时间 X，原时间 Y".
_SUPERSEDED_CLAUSE_RE = re.compile(
    r"原定|原訂|原时间|原時間|原计划|原計劃|原本|旧时间|舊時間|先前|之前的"
    r"|\boriginal(?:ly)?\b|\bpreviously\b|\binstead\s+of\b|\bmoved\s+from\b"
    r"|\bwas\s+(?:previously\s+)?scheduled\s+for\b",
    re.I)

# A window mentioned only to say it is already behind us. The first range in a
# notice is very often this, which is how "上周的维护窗口为 02:00 - 04:00" once
# borrowed this week's date and invented an outage nobody announced.
_PAST_CLAUSE_RE = re.compile(
    r"上次|上周|上週|上一次|前次|之前|已完成|已結束|已结束|已顺利完成|已順利完成"
    r"|\blast\s+(?:week|month|night|window|time|maintenance|run)\b"
    r"|\bprevious(?:ly)?\b|\bhas\s+been\s+completed\b|\bwent\s+smoothly\b",
    re.I)

# The letterhead date of a notice ("issued on ..."), which is NOT the date of
# the maintenance. A window that had to borrow its date from one of these lines
# is the most dangerous thing this parser can produce: a confident, plausible,
# entirely wrong row that nothing about the sheet flags.
_LETTERHEAD_RE = re.compile(
    r"(?:發佈日期|发布日期|發布日期|公告日期|通知日期|發文日期|发文日期|刊登日期)\s*[:：]"
    r"|\b(?:issued|published|posted|release\s+date|notice\s+date|date\s+issued)\b"
    r"\s*[:：]",
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
    r"|停机时间|停機時間|停止服务|停止服務"
    # -- Chinese: 维修, which the traditional-character studios use for 維護
    r"|系统维修|系統維修|平台维修|平台維修|维修公告|維修公告|维修通知|維修通知"
    r"|紧急维修|緊急維修|进行.{0,8}维修|進行.{0,8}維修"
    # -- Chinese: 暂停, allowed ONLY with the service as its subject. A bare
    #    暂停 matched 活动暂停, a promo pause, and would have written an outage.
    r"|服务暂停|服務暫停|暂停服务|暫停服務|服务中断|服務中斷|系统中断|系統中斷"
    r"|暂停游戏|暫停遊戲|游戏暂停|遊戲暫停|暂停开放|暫停開放"
    # -- English: the heading is <thing> maintenance, not only "scheduled"
    r"|(?:server|system|platform|service|database|network|game|api|backend"
    r"|emergency|urgent|unplanned|planned|periodic|regular)\s+maintenance"
    r"|maintenance\s+(?:window|schedule|advisory|alert|announcement)"
    # -- English: upgrade and downtime
    r"|(?:system|platform|server|service|version|software|database|api|client)"
    r"\s+upgrade"
    r"|(?:scheduled|planned|system|service|emergency|unplanned|maintenance)"
    r"\s+downtime"
    r"|service\s+(?:interruption|disruption|outage|suspension)"
    # -- English: the outage described rather than named. The subject noun must
    #    be the service itself and must sit in the same sentence.
    r"|(?:" + _SERVICE_NOUN + r")\b" + _NOT_STOP + r"{0,40}\bwill\s+be\s+"
    r"(?:temporarily\s+|briefly\s+)?"
    r"(?:unavailable|offline|down|suspended|inaccessible|interrupted)"
    r"|(?:" + _SERVICE_NOUN + r")\b" + _NOT_STOP + r"{0,40}\bwill\s+undergo\b",
    re.I)


def _sched_re():
    return SCHED_RE if _flag("NOTICE_WORDING", "wide") == "strict" \
        else SCHED_WIDE_RE


# A negator sitting immediately in front of the phrase that opened the gate.
# Anchored at the END so it can only ever match the words touching the phrase:
# "无法访问" (unable to access) must NOT read as a negated 无, and it cannot,
# because 法 sits between the 无 and the matched phrase.
_NEG_BEFORE_RE = re.compile(
    r"(?:\b(?:no|not|never|without|nor|avoid|avoids|avoiding)\b"
    r"|不会|不會|不需|不須|不再|无|無|沒有|没有|免)[\s,，、的]*$",
    re.I)

# "请问…吗？" / "could you confirm your maintenance window …?" — a QUESTION about
# a window is not an announcement of one. The same group carries our own weekly
# ask and the partner's clarifying questions, and both were being classified as
# notices: a question quoting a window would have written that window onto the
# provider's row.
_QUESTION_RE = re.compile(
    r"[?？]\s*$"
    r"|[吗嗎呢]\s*[?？]"
    r"|请问|請問|想[请請]问|麻烦确认|麻煩確認|可以确认|可以確認"
    r"|\b(?:could|can|would|will)\s+you\b|\bmay\s+i\b|\bdo\s+you\s+have\b"
    r"|\bplease\s+(?:confirm|advise|kindly\s+confirm)\b"
    r"|\bis\s+there\s+(?:any|an)\b|\bany\s+(?:planned|scheduled)\b",
    re.I)


def _asks_rather_than_tells(t: str) -> bool:
    """True when the message is a question about maintenance, not a notice.

    Scoped to the line the question mark is on, so an ordinary notice that ends
    with "Any questions, please contact us?" is not mistaken for one.
    """
    for ln in t.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        if _QUESTION_RE.search(ln) and _sched_re().search(ln):
            return True
    # A short message that is ONE question, with the window quoted inside it.
    return bool(len(t) <= 300 and re.search(r"[?？]", t)
                and _QUESTION_RE.search(t) and _sched_re().search(t))


def _sched_hit(t: str):
    """The gate, with negated and interrogative matches refused.

    SCHED_WIDE_RE lists a few phrases that name the outage directly
    ("service interruption", 停止服务, "planned downtime"). Those read the same
    whether the provider is announcing one or PROMISING there will not be one,
    so "New game launch 10:00 - 12:00 GMT+8. There will be no service
    interruption." opened the gate and wrote a two-hour outage nobody announced.
    Over 432 messages that explicitly promise no outage the bare gate fired on
    every one. A match whose negator touches it is therefore not a match.
    """
    if _asks_rather_than_tells(t):
        return None
    rx = _sched_re()
    for m in rx.finditer(t):
        head = t[max(0, m.start() - 24):m.start()]
        # Clause-local: a negator on the far side of a delimiter governs a
        # different statement and must not suppress this one.
        head = re.split(r"[。．.;；\n、,，]", head)[-1]
        if _NEG_BEFORE_RE.search(head):
            continue
        return m
    return None


# Wins outright (except over a reschedule, which is checked first), and only
# inside its own sentence - see _done_spans and classify().
#
# The cancel half used to be a bare `取消|cancell?ed`, which matched any use of
# the word: "All ongoing rounds will be cancelled during the maintenance" is a
# perfectly ordinary notice and was being thrown away. The cancel word now has
# to be tied to the maintenance itself, on BOTH sides.
_DONE_RES = [
    # Chinese completion. The gap excludes an object noun for the same reason
    # the cancel branch does, and (?![后後]) is the future frame in miniature:
    # "维护完成后将通知" is a promise, not a report.
    re.compile(r"(?:维护|維護|维修|維修|升级|升級)"
               r"(?:(?!" + _OBJ_ZH + r")" + _NOT_STOP_ZH + r"){0,10}"
               r"(?:完成|結束|结束|恢复正常|恢復正常)(?![后後])"),
    re.compile(r"(?:已经完成|已經完成)"),
    # Chinese cancellation, forward: 维护 ... 取消. "维护期间未结算注单将取消"
    # cancels the BETS, so an object noun in the gap blocks the match - the
    # English branch has had this guard for a while and the Chinese one did not.
    re.compile(r"(?:维护|維護|维修|維修)"
               r"(?:(?!" + _OBJ_ZH + r")" + _NOT_STOP_ZH + r"){0,14}"
               r"(?:已|将|將|被)?取消"),
    # ... and backward: 取消 ... 维护.
    re.compile(r"(?:已|将|將)?取消"
               r"(?:(?!" + _OBJ_ZH + r")" + _NOT_STOP_ZH + r"){0,14}(?:维护|維護)"),
    # English completion. Kept broad on purpose and disarmed by the future-frame
    # guard instead: "we will notify you once it has been completed" is the
    # single commonest closing line in this corpus and it is NOT a report.
    re.compile(r"maintenance\s+(?:has\s+been\s+|have\s+been\s+|was\s+|is\s+)?"
               r"(?:completed|finished|concluded)", re.I),
    re.compile(r"\b(?:has|have)\s+been\s+completed\b", re.I),
    re.compile(r"\bcompleted\s+successfully\b", re.I),
    # English cancellation. The gap already refuses an object noun; the noun
    # standing to the LEFT of "maintenance" is checked in _done_spans, because
    # "All ongoing rounds before the maintenance will be cancelled" put its
    # subject on the other side and walked straight through.
    #
    # The gap has to clear the window the notice restates - "maintenance
    # planned for 2026-09-24 10:00 - 12:00 (GMT+8) is cancelled" is 46
    # characters - so {0,40} silently missed real cancellations and the window
    # was then written as active.
    re.compile(r"maintenance(?:(?!\b(?:" + _OBJ_EN + r")\b)" + _NOT_STOP + r"){0,80}"
               r"\b(?:is|are|was|were|has\s+been|have\s+been|will\s+be|being)"
               r"\s+cancell?ed", re.I),
    # "Cancellation of the scheduled maintenance" - direct object only. The
    # cancellation OF THE PROMO during the maintenance is about the promo.
    re.compile(r"cancell?ation\s+of\s+(?:the\s+|our\s+|this\s+)?"
               r"(?:scheduled\s+|routine\s+|planned\s+|upcoming\s+)?maintenance"
               r"|maintenance\s+cancell?ation", re.I),
    # "We are cancelling the maintenance". The {0,12} window is deliberately
    # short: "cancelling the rounds during the maintenance" is 22 characters
    # between and must NOT match.
    re.compile(r"cancell?ing\s+(?:the\s+)?" + _NOT_STOP + r"{0,12}maintenance", re.I),
]

# Kept as a single greppable pattern for callers and for `grep DONE_RE`; the
# guards above are what classify() actually consults.
DONE_RE = re.compile("|".join("(?:{})".format(r.pattern) for r in _DONE_RES),
                     re.I)

# A completion promised for later is not a completion. Anything governed by one
# of these is a future frame and never suppresses.
_FUTURE_FRAME_RE = re.compile(
    r"\b(?:will|shall|would|going\s+to|once|after|when|upon|before|until|till"
    r"|as\s+soon\s+as|in\s+the\s+event)\b", re.I)

_CLAUSE_BOUND = "。．.,，;；!?！？\n"
_SENT_BOUND = "。．.;；!?！？\n"

# A correction notice: it restates the wrong window next to the right one.
_CORRECTION_RE = re.compile(
    r"\bcorrect(?:ion|ed)\b|\bdisregard\b|\bwas\s+(?:wrong|incorrect)\b"
    r"|\bnot\s+(?:on\s+)?(?=\d)|更正|勘误|勘誤|有误|有誤",
    re.I)

# An explicit "nothing planned" answer to our weekly ask.
NO_MAINT_RE = re.compile(
    r"no\s+maintenance(?:\s+(?:is|are|was))?(?:\s+\w+){0,3}\s*(?:this|next)?\s*week"
    r"|no\s+maintenance\s+(?:planned|scheduled|plan)"
    r"|no\s+(?:scheduled\s+)?maintenance\b"
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


def _done_spans(text: str):
    """Spans where a completion/cancellation is asserted OF THE MAINTENANCE."""
    out = []
    for rx in _DONE_RES:
        for m in rx.finditer(text):
            if _future_framed(text, m.start()):
                continue
            # The subject standing to the LEFT. "All ongoing rounds before the
            # maintenance will be cancelled" and "维护前的注单将取消" both put
            # the cancelled thing before the word the guard was watching.
            left = _clause_before(text, m.start(), _SENT_BOUND)
            if _OBJ_EN_RE.search(left) or _OBJ_ZH_RE.search(left):
                continue
            out.append((m.start(), m.end()))
    out.sort()
    return out


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
    marks = list(_UPDATED_RE.finditer(text))
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


def _superseded_dates(text: str, dates):
    """Indices of dates a "moved from" / 原定 marker introduces as the OLD one.

    The lookback stops at the previous date so that, in "moved from 2026-09-23
    to 2026-09-25", the marker taints the 23rd and not the 25th - writing the
    superseded window is exactly the failure this guard exists to prevent.
    """
    sup = set()
    prev_end = 0
    for i, (pos, end, _ymd) in enumerate(dates):
        lo = max(prev_end, pos - 28)
        if _SUPERSEDED_CLAUSE_RE.search(text[lo:pos]):
            sup.add(i)
        prev_end = end
    return sup


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
    r"[ \t]*(?:[\(（][^)）\n]{1,12}[\)）][ \t]*)?" + _SPAN_CONN + r"[ \t]*", re.I)
# The shorthands, where only ONE half is a whole date:
#   "Sep 24 - 25, 2026" / "September 24th - 25th"   (month-first, day after)
_SPAN_TAIL_DAY_RE = re.compile(
    r"[ \t]*(?:[\(（][^)）\n]{1,12}[\)）][ \t]*)?" + _SPAN_CONN
    + r"[ \t]*(\d{1,2})" + _ORD + r"(?![\d:./月A-Za-z])(?:[ \t]*,?[ \t]*(\d{4})(?!\d))?",
    re.I)
#   "9月24日-25日" / "2026年9月24日至25日"
_SPAN_TAIL_ZH_RE = re.compile(
    r"[ \t]*(?:[\(（][^)）\n]{1,12}[\)）][ \t]*)?" + _SPAN_CONN
    + r"[ \t]*(\d{1,2})\s*[日号號]")
#   "24/09 - 25/09"
_SPAN_TAIL_BARE_RE = re.compile(
    r"[ \t]*(?:[\(（][^)）\n]{1,12}[\)）][ \t]*)?" + _SPAN_CONN
    + r"[ \t]*(\d{1,2})/(\d{1,2})(?![\d/])")
#   "23-24 Sep 2026" / "24th - 25th September 2026"  (day-first, day BEFORE)
_SPAN_HEAD_DAY_RE = re.compile(
    r"(?<![\d:./\-])(\d{1,2})" + _ORD + r"[ \t]*" + _SPAN_CONN + r"[ \t]*$", re.I)
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
        else:
            y, mo, d = ymd
            tok = text[p:e]
            m = None
            if _MONTH_FIRST_YEARLESS_RE.fullmatch(tok):
                m = _SPAN_TAIL_DAY_RE.match(text, e)
                if m:
                    yy = int(m.group(2)) if m.group(2) else y
                    sp = {"lo": (yy, mo, d), "hi": (yy, mo, int(m.group(1))),
                          "at": (p, m.end()), "members": [i]}
            if sp is None and tok[-1:] in "日号號":
                m = _SPAN_TAIL_ZH_RE.match(text, e)
                if m:
                    sp = {"lo": ymd, "hi": (y, mo, int(m.group(1))),
                          "at": (p, m.end()), "members": [i]}
            if sp is None and re.fullmatch(r"\d{1,2}/\d{1,2}", tok):
                m = _SPAN_TAIL_BARE_RE.match(text, e)
                if m:
                    c1, c2 = int(m.group(1)), int(m.group(2))
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
        if sp is None or not (_ok(sp["lo"]) and _ok(sp["hi"])):
            continue
        if _moved(sp["at"][0], sp["at"][1]):
            continue
        for j in sp["members"]:
            out[j] = sp
    return out


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
    r"maintenance|downtime|upgrade|outage"
    r"|维护|維護|维修|維修|升级|升級|停机|停機|停服",
    re.I)

# Words that make a range the time of something ELSE. Each is a subject a
# provider notice names next to its own hours: the support desk, a promotion or
# tournament, bet settlement, the payment channels, a holiday period.
_TOPIC_OFF_RE = re.compile(
    r"customer\s+(?:service|support|care)|\bsupport\s+(?:team|desk|hours?)"
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
        if sent.strip() and _data_kind(sent) is None:
            return sent
        pos = ps
    return ""


def _off_heading(head: str) -> bool:
    return bool(_TOPIC_OFF_RE.search(head) and not _TOPIC_STRONG_RE.search(head))


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
    return None


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


def _relative_when(t: str):
    """The word a dateless maintenance notice is dated by, or None.

    Looked for in the sentence of the wording-gate hit and in the sentence of
    every clock, and only when the message carries a clock at all.
    """
    clocks = [m.start() for m in _ANY_CLOCK_RE.finditer(t)]
    if not clocks:
        return None
    hit = _sched_hit(t)
    for pos in ([hit.start()] if hit else []) + clocks:
        s, e = _sentence_span(t, pos, pos)
        m = _RELATIVE_WHEN_RE.search(t, s, e)
        if m:
            return m.group(0)
    return None


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

    pool = [i for i in before if i not in ineligible]
    if pool:
        use = [i for i in pool if i not in sup] or pool
        # "Following the 2026-09-16 maintenance, emergency maintenance
        # tomorrow 10:00-12:00": the day the range is given is "tomorrow",
        # not the date further up the line (G1.3).
        if _relative_between(masked, dates[use[-1]][0], dates[use[-1]][1], a, b):
            return None, False, False, None
        return use[-1], False, False, None

    s, e = _sentence_span(masked, a, b)
    wd = _weekdays(masked[s:e])

    def _day_ok(i):
        return not wd or date(*dates[i][2]).weekday() in wd

    prev = [i for i, (p, _e, _d) in enumerate(dates)
            if p < ls and i not in letterhead and i not in ineligible]
    if prev:
        use = [i for i in prev if i not in sup] or prev
        if _relative_between(masked, dates[use[-1]][0], dates[use[-1]][1], a, b):
            return None, False, False, None
        return (use[-1], False, True, None) if _day_ok(use[-1]) \
            else (None, False, False, None)

    # Only a labelled or bare time line borrows forward: a short prose
    # sentence ("排定维护，时间 X，敬请留意。") also passes as a row, and the
    # date under it is a sign-off, not the window's day.
    data_range = _data_kind(masked[s:e]) in ("bare", "label")
    pos = le + 1
    while pos < len(text):
        ne = text.find("\n", pos)
        ne = len(text) if ne == -1 else ne
        if text[pos:ne].strip():
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
    if data_range:
        return nxt[0], False, True, None
    # The window is a prose sentence and the only date is a bare date on the
    # line below it. "Scheduled maintenance 10:00 - 12:00 (GMT+8)\n2026-09-24
    # (Thu)" means the 24th; "排定维护，时间 10:00 - 12:00，敬请留意。\n2026-09-22"
    # is a sign-off. The text cannot tell them apart, so a human reads it.
    return nxt[0], False, True, ("the only date is on the line after the "
                                 "window and may be a sign-off date")


_DATE_LABEL_RE = re.compile(r"date|\bday\b|日期|日子", re.I)


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
_UNKNOWN_ZONE_AFTER_RE = re.compile(
    r"[\s(（\[]*(?:"
    r"(?!(?:SGT|MYT|PHT|CST|HKT|JST|KST|ICT|WIB|IST|EST|EDT|PST|PDT|GMT)\b)"
    r"[A-Z]{2,4}T\b|MSK\b"
    r"|(?!(?:Manila|Singapore|Beijing|Hong|HK|Taipei|Philippine|Philippines"
    r"|China|Malaysia|Macau|Local)\b)[A-Z][a-z]+\s+time\b)")

def _unknown_zone_after(text: str, pos: int, rspans=()):
    """The unknown zone word straight after position ``pos``, or None.

    Not when the word is followed by a zone the table DOES know before the
    next range starts: "10:00 - 12:00 Vietnam time (GMT+7)" states its offset.
    """
    m = _UNKNOWN_ZONE_AFTER_RE.match(text, pos)
    if not m:
        return None
    _ls, le = _line_at(text, m.end())
    stop = min([le] + [a for a, _b in rspans if a >= m.end()])
    # Scanned from the word itself, so a zone later added to _TZ_NAMES / _TZ_RE
    # stops counting as unknown without anyone having to edit this rule.
    for z in _TZ_RE.finditer(text, pos, stop):
        if _offset_of(z) is not None:
            return None
    return m.group(0).strip(" \t([（")


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
    try:
        h = float(_flag("NOTICE_MAX_WINDOW_HOURS", "48"))
    except ValueError:
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


def _collect(text: str, now: datetime):
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
    tokens = _tz_tokens(text, rspans)
    regions = _superseded_regions(text)
    pts = _datetime_points(text, dates)
    # Only the CLOCK of a "<date> <time>" point is masked, never the words a
    # point may span ("9月23日例行维护 10:00"): masking those hid the very
    # maintenance word that makes the range relevant.
    clocks = []
    for q in pts:
        tm = re.search(r"\d{1,2}:\d{2}\s*(?:[AaPp][Mm])?$", text[q[0]:q[1]])
        if tm:
            clocks.append((q[0] + tm.start(), q[1]))
    masked = _mask(_mask(text, dspans + rspans + clocks),
                   [(a, b) for a, b, _o in tokens], _MASK_TZ)

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

    def _flags(span, date_idx):
        a, _b = span
        in_region = any(a >= s and a < e for s, e in regions)
        clause = _clause_before(text, a)
        superseded = in_region or bool(_SUPERSEDED_CLAUSE_RE.search(clause))
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
                "zone_unknown": _unknown_zone_after(text, span[1], rspans),
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

    # A window with a date on BOTH ends is unambiguous - collect it first so the
    # single-date rules cannot re-read half of it.
    for a, b in zip(pts, pts[1:]):
        between = text[a[1]:b[0]]
        if not re.fullmatch(r"\s*" + _DASH + r"\s*", between or ""):
            continue
        span = (a[0], b[1])
        tz = _tz_for(text, span, tokens, rspans)
        try:
            start = datetime(a[2][0], a[2][1], a[2][2], _h24(a[3], a[5]), a[4],
                             tzinfo=tz)
            end = datetime(b[2][0], b[2][1], b[2][2], _h24(b[3], b[5]), b[4],
                           tzinfo=tz)
        except ValueError:
            continue
        if end <= start:
            continue
        taken.append(span)
        di = next((i for i, (p, _e, _d) in enumerate(dates) if p == a[0]), None)
        dj = next((i for i, (p, _e, _d) in enumerate(dates) if p == b[0]), None)
        rel = _relevant(masked, *span)
        if di in letterhead or rel is None:
            dropped += 1
            continue
        cands.append(_cand(span, start, end, di, True, False, None, rel,
                           doubt_of.get(di) or doubt_of.get(dj)))

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
        try:
            # One half of a date span: the window starts on the span's FIRST
            # day, whichever half the range happened to bind to.
            start, end = _build(sp["lo"] if sp else dates[di][2], t1, t2, tz)
        except ValueError:
            continue
        if sp:
            doubt = (doubt or next((doubt_of[j] for j in sp["members"]
                                    if j in doubt_of), None)
                     or _span_doubt(sp, start, end))
        cands.append(_cand((a, b), start, end, di, own, far, amb, rel, doubt))

    cands.sort(key=lambda c: c["span"][0])
    _merge_zone_copies(cands, now)
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


def _merge_zone_copies(cands, now):
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
            if a["start"].utcoffset() == b["start"].utcoffset():
                continue
            if a["end"] - a["start"] != b["end"] - b["start"]:
                continue
            if abs(b["start"] - a["start"]) != day:
                continue
            owner, copy = (b, a) if (b["own"] and not a["own"]) else (a, b)
            if copy["own"]:
                continue
            shift = owner["start"] - copy["start"]
            copy["start"] += shift
            copy["end"] += shift
            copy["future"] = copy["end"] >= now
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
            by_line.setdefault(c["line"], []).append(c)
    for ls, group in by_line.items():
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
            "others": others, "ambiguous": ambiguous}


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


def classify(text: str, *, now=None) -> dict:
    """-> {action, reason, start, end, reschedule, stale, others}

    action is one of 'fill' | 'clear' | 'follow_quote' | 'needs_human' | 'ignore'.
    ``stale`` means the window has already passed; the caller decides whether to
    write it (a passive backlog read should not, a deliberate re-read may).
    ``others`` lists the further upcoming windows [(start, end), ...] a fill
    notice states but the row does not get - a phased or multi-night notice
    fills the earliest one, and the rest are named here and in ``reason``
    rather than dropped without a word.
    """
    raw = (text or "").strip()
    out = {"action": "ignore", "reason": "", "start": None, "end": None,
           "reschedule": False, "stale": False, "others": []}
    if not raw:
        out["reason"] = "empty message"
        return out

    # Parse a normalised COPY; the caller still writes `text` to Remark.
    t = _norm(raw)
    now = _aware(now, _tz_default())

    cands, dropped = _collect(t, now)
    detail = _resolve(cands, now)
    win = (detail["start"], detail["end"]) if detail else None
    win_superseded = bool(detail and detail["superseded"])
    # A reschedule or a correction states the OLD window as well as the new
    # one. When ranking has not told them apart - no Original/原定/"instead of"
    # marker demoted one - the notice still carries two separate windows, and
    # the earliest-first rule would write whichever came first: "postponed from
    # 09-23 10:00-12:00 to 09-24 10:00-12:00" wrote the withdrawn 23rd.
    corrected = bool(_CORRECTION_RE.search(t))
    # A window the notice states but that cannot be written as it stands: two
    # dates for one range, two zones that disagree, a row that could be either
    # day, a span longer than NOTICE_MAX_WINDOW_HOURS. Per the overriding rule
    # a wrong window on the shared sheet is worse than none, so every path
    # that would FILL asks a human instead.
    ambiguous = detail["ambiguous"] if detail else None
    done = _done_spans(t)
    # A later window in a sentence that cancels or completes it is not one the
    # row is missing: "maintenance on 09-24 02:00-04:00 has been cancelled.
    # Scheduled maintenance 09-23 10:00-12:00 is still on." lists no others.
    others = [(s, e) for s, e, at in (detail["others"] if detail else [])
              if all(_sentence_index(t, at) != _sentence_index(t, d[0])
                     for d in done)]
    resched = bool(RESCHEDULE_RE.search(t))

    def _fill_reason(base):
        if not others:
            return base
        return "{}; the notice also states {} later window{} NOT written: {}".format(
            base, len(others), "" if len(others) == 1 else "s",
            ", ".join(_fmt_window(s, e) for s, e in others))

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
        out.update(action="fill",
                   reason=_fill_reason("rescheduled maintenance (overrides)"),
                   start=win[0], end=win[1], reschedule=True, others=others,
                   stale=is_stale(win[1], now=now))
        return out

    # 2. Completed / cancelled, scoped to the SENTENCE that carries it. A
    #    completion notice repeats the original window verbatim, so it has to
    #    win over any window-based rule - but only over a window in its OWN
    #    sentence. "上次维护已完成。下次例行维护将于 X 进行。" is a report and an
    #    announcement in one bubble, and reading it as one statement threw the
    #    announcement away; a window stated in a different sentence wins.
    if done:
        at = detail["at"] if detail else None
        fresh = (at is not None
                 and all(_sentence_index(t, at) != _sentence_index(t, d[0])
                         for d in done)
                 and not any(d[0] <= at < d[1] for d in done)
                 # ...and the surviving window's OWN sentence has to be about
                 # maintenance. Sentence-scoping alone let any unrelated time
                 # range rescue a cancellation: "New game launch 10:00 - 12:00
                 # GMT+8. The scheduled maintenance has been cancelled." wrote
                 # the launch window as an outage. The escape hatch exists for
                 # "上次维护已完成。下次例行维护将于 X 进行。", where the second
                 # sentence names the maintenance itself - so require that.
                 and bool(_sched_re().search(_sentence_of(t, at))))
        if not fresh:
            out["reason"] = "maintenance completed / cancelled notice"
            return out

    # 3. An explicit answer that there is nothing planned. ``dropped`` keeps
    #    this exactly as strict as it was before ranges had to belong to the
    #    maintenance statement: a message carrying ANY readable range is not
    #    stamped "No maintenance" on the strength of a regex.
    if NO_MAINT_RE.search(t) and not win and not dropped:
        out.update(action="clear", reason="provider says no maintenance")
        return out

    # 4. An ordinary upcoming notice.
    if _sched_hit(t) and win:
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
        out.update(action="fill", reason=_fill_reason("scheduled maintenance"),
                   start=win[0], end=win[1], others=others,
                   stale=is_stale(win[1], now=now))
        return out

    # 5. A short reply pointing at another message - the answer is in the quote.
    if len(t) <= _REFERRAL_MAX_CHARS and REFERRAL_RE.search(t) and not win:
        out.update(action="follow_quote",
                   reason="the answer is in the message this one quotes")
        return out

    if _sched_hit(t):
        rel = _relative_when(t)
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
