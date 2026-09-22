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
Candidates are ranked by four commented weights in ``_score`` — highest first:

  a. not inside a superseded block (an Original/原定 region under an Updated
     heading) and not introduced by a superseded clause marker (原时间,
     "moved from", "instead of")                                   weight 8
  b. not introduced as a PAST window (上次/上周/last/previous)       weight 4
  c. its date did not come from a letterhead "發佈日期 / Issued:" line weight 2
  d. it ends at or after ``now``                                    weight 1

Everything at the top score survives; ties break by document order. Two
survivors on the same date (or within 24h) become their UNION, because the row
answers one question — when is this provider down — and a span shorter than
reality is the dangerous error. Survivors more than 24h apart are two separate
outages and the row takes the earliest one still in the future.

Set NOTICE_WINDOW_PICK=first to get the pre-2026-09 behaviour, where the first
range in the document won outright and a passing mention of last week's window
could be stamped with this week's date.

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


# Each entry is (regex, converter) and the converter returns (year|None, month,
# day). They are tried IN THIS ORDER and a match that overlaps an already
# accepted one is dropped, so the more specific shape always wins: "2026年9月
# 23日" is read by the 年月日 rule and never re-read as a bare 9月23日.
_DATE_RES = [
    # ISO-ish, year first. Unchanged from the original rule.
    (re.compile(r"(?<![\d/.\-])(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})(?!\d)"),
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
    # 23rd September 2026 / 23 Sep 2026
    (re.compile(r"(\d{1,2})" + _ORD + r"\s*(?:of\s+)?" + _MONTH_WORD +
                r"\.?\s*,?\s*(\d{4})(?!\d)", re.I),
     lambda m: (int(m.group(2)), _mword(m), int(m.group(1)))),
    # 23/09/2026, 23-09-2026, 23.09.2026, 09/23/2026 — day- or month-first with
    # the year at the END. The original rule demanded a 4-digit year in FRONT,
    # which is why every one of these produced no window at all.
    (re.compile(r"(?<![\d/.\-])(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})(?!\d)"),
     lambda m: (int(m.group(3),),) + _order_dm(int(m.group(1)), int(m.group(2)))),
    # 9月23日 with no year at all.
    (re.compile(r"(?<!\d)(\d{1,2})\s*月\s*(\d{1,2})\s*[日号號]"),
     lambda m: (None, int(m.group(1)), int(m.group(2)))),
    # Sep 23 with no year. The lookahead keeps it from eating the day out of
    # "Sep 23, 2026", which the year-bearing rule above has already taken.
    (re.compile(_MONTH_WORD + r"\.?\s*(\d{1,2})" + _ORD +
                r"(?!\s*,?\s*\d{4})(?![\d:.])", re.I),
     lambda m: (None, _mword(m), int(m.group(1)))),
]

# "9/23" — the shape inside the live KingMidas notice, and the riskiest date
# form in the file: it is also how a score, a ratio, a version number and a
# fraction are written. Two guards, both deliberate:
#   1. it is consulted ONLY when no other date form matched anywhere in the
#      message, so a notice that states a real date somewhere can never have a
#      stray "9/23" override it (the KingMidas notice itself states
#      "Sep 23th, 2026" further down, and that is the one that must win);
#   2. it must sit next to a date/time context word or an actual clock.
_RISKY_DATE_RE = re.compile(r"(?<![\d/.:\-])(\d{1,2})/(\d{1,2})(?![\d/])")
_DATE_CONTEXT_RE = re.compile(
    r"\d{1,2}\s*[:.]\s*\d{2}|\d{1,2}\s*[AaPp]\.?[Mm]|\d{1,2}\s*[时時]"
    r"|日期|時間|时间|日程|维护|維護|维修|維修|停机|停機|停服|升级|升級"
    r"|\bdate\b|\btime\b|\bon\b|maintenance|downtime|upgrade|schedul",
    re.I)
_RISKY_DATE_REACH = 16

# A date INTRODUCER immediately in front of the bare "9/23" — anchored at the
# end so only the words touching it count.
_RISKY_INTRO_RE = re.compile(
    r"(?:将于|將於|将在|將在|定于|定於|于|於|日期|時間|时间|预计|預計"
    r"|\bon\b|\bfrom\b|\bdate\b|\bdated\b|\bstarting\b|\bcommenc(?:e|ing)\b"
    r"|\bscheduled\s+for\b"
    # The maintenance word itself introduces the day just as often:
    # "排定维护 9/23 10:00 - 12:00" is the shape the live KingMidas group uses.
    # Safe next to the veto below, because both are anchored at the end and the
    # CLOSEST word therefore decides: in "maintenance, phase 1/2" it is "phase".
    r"|维护|維護|维修|維修|停机|停機|停服|升级|升級"
    r"|\bmaintenance\b|\bdowntime\b|\bupgrade\b)[\s:：,，]*$",
    re.I)
# ...or the bracketed weekday straight after it, which is how the Chinese half
# of a bilingual notice writes the day: "将于9/23 (三) 10:00AM".
_RISKY_WEEKDAY_RE = re.compile(
    r"\s*[\(（]\s*(?:[一二三四五六日天]|Mon|Tue|Wed|Thu|Fri|Sat|Sun)", re.I)
# Words that make the pair a count, a share, a phase or a version, never a day.
_RISKY_VETO_RE = re.compile(
    r"(?:phase|stage|step|part|version|\bv\b|round|chapter|page|ratio|out\s+of"
    r"|第|约|約|阶段|階段|版本|批次)[\s:：]*$",
    re.I)
# A share reads exactly like a day once an introducer is in front of it:
# "Server maintenance on 3/4 of our nodes" survived every guard above. A day is
# never followed by "of <noun>" or by 的/个/台/家.
_RISKY_AFTER_VETO_RE = re.compile(
    r"\s*(?:of\b|的|个|個|台|家|servers?|nodes?|games?|users?|players?)", re.I)


def _infer_year(month: int, day: int, now: datetime, tz):
    """The year a year-less date means — a pure function of (text, now).

    No clock is read here and the host's timezone is irrelevant, so the same
    message classified twice on the same ``now`` always gives the same year.
    Candidates are the same (month, day) in now.year-1/+0/+1; a candidate is
    kept only if it falls in [now-120d, now+245d]; the earliest kept candidate
    on or after today wins, else the latest kept one. Nothing kept means the
    date is UNUSABLE and the notice yields no window — never a guessed year,
    because a row carrying a window in the wrong year looks exactly as healthy
    as a right one.

    The window is 365 days wide on purpose: at most one candidate year can ever
    qualify, so the "prefer the future" tie-break almost never fires and the
    rule cannot drift with the calendar. 120 back because a provider re-posting
    last quarter's notice is routine (is_stale then stops it being written);
    245 forward because nobody announces maintenance eight months out.
    """
    base = now.astimezone(tz).date() if now.tzinfo else now.date()
    lo, hi = base - timedelta(days=120), base + timedelta(days=245)
    kept = []
    for y in (base.year - 1, base.year, base.year + 1):
        try:
            cand = date(y, month, day)
        except ValueError:          # 2/29 in a common year
            continue
        if lo <= cand <= hi:
            kept.append(cand)
    if not kept:
        return None
    ahead = [c for c in kept if c >= base]
    return (min(ahead) if ahead else max(kept)).year


def _dates(text: str, *, now: datetime, tz):
    """[(start, end, (y, m, d))] for every readable date, in document order.

    The END matters: a caller looking for the time that follows a date has to
    know where the date token actually stopped. Measuring it with \\S+ grabbed
    only "Sep" out of "Sep 16th, 2026".

    ``tz`` is only used to decide what day it is where the notice was written,
    which is all _infer_year needs; a few hours either way can never move a
    365-day-wide window onto a different candidate year.
    """
    out = []
    taken = []
    yearless = _on("NOTICE_YEARLESS", "1")

    def _add(m, ymd):
        y, mo, d = ymd
        if y is None:
            if not yearless:
                return
            y = _infer_year(mo, d, now, tz)
            if y is None:
                return
        datetime(y, mo, d)                      # raises on 31 February
        out.append((m.start(), m.end(), (y, mo, d)))
        taken.append((m.start(), m.end()))

    for rx, conv in _DATE_RES:
        for m in rx.finditer(text):
            if any(m.start() < e and m.end() > s for s, e in taken):
                continue
            try:
                _add(m, conv(m))
            except (ValueError, KeyError):
                continue

    if not out and yearless:
        for m in _RISKY_DATE_RE.finditer(text):
            before = text[max(0, m.start() - _RISKY_DATE_REACH):m.start()]
            after = text[m.end():m.end() + _RISKY_DATE_REACH]
            # A nearby CLOCK is not evidence of a date: every message that
            # reaches the write path has one, which made the old proximity test
            # vacuous exactly where it mattered. "Scheduled maintenance, phase
            # 1/2, 10:00-12:00 GMT+8" was read as 1 February 2027 and written.
            # So the ratio must be introduced the way a date is - 将于 / 于 /
            # 日期 / Date / on / from - or be followed straight away by the
            # bracketed weekday that the live KingMidas notice carries
            # ("将于9/23 (三) 10:00AM").
            if not (_RISKY_INTRO_RE.search(before)
                    or _RISKY_WEEKDAY_RE.match(after)):
                continue
            # ...and never after a word that makes the pair a count, a share or
            # a version rather than a day.
            if _RISKY_VETO_RE.search(before):
                continue
            if _RISKY_AFTER_VETO_RE.match(after):
                continue
            try:
                mo, d = _order_dm(int(m.group(1)), int(m.group(2)))
                _add(m, (None, mo, d))
            except (ValueError, KeyError):
                continue

    out.sort(key=lambda p: p[0])
    return out


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
# So it is a FALLBACK, not an alternative: it is consulted only when no ordinary
# clock rule matched anywhere in the message. A notice that states its window
# with a colon - which is all of them bar the dotted-form minority - can no
# longer have a price list rewrite it, and a genuinely dotted notice still
# parses because it has no colon range to be outranked by.
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
            a, b = m.start(), m.end()
            if any(a < e and b > s for s, e in taken):
                continue
            if any(a < e and b > s for s, e in dspans):
                continue
            taken.append((a, b))
            t1, t2 = conv(m)
            out.append((a, b, t1, t2))
    if not out:
        # Only now the dotted form — see _RANGE_FALLBACK_RES for why it may
        # never compete with a colon range.
        for rx, conv in _RANGE_FALLBACK_RES:
            for m in rx.finditer(text):
                a, b = m.start(), m.end()
                if any(a < e and b > s for s, e in taken):
                    continue
                if any(a < e and b > s for s, e in dspans):
                    continue
                ls = text.rfind("\n", 0, a) + 1
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


def _tz_for(text: str, span, tokens):
    """The zone for ONE range: its own line first, then the document, then NOTICE_TZ.

    Line first is what makes a notice that prints the same window twice - a UTC
    line above a GMT+8 line - come out right: each clock is stamped with its own
    line's offset, so both lines denote the same instant and it does not matter
    which one wins. Taking the first zone token in the WHOLE message instead is
    how you stamp line A's clock with line B's offset and put a window eight
    hours out without anything on the row looking wrong.
    """
    ls = text.rfind("\n", 0, span[0]) + 1
    le = text.find("\n", span[1])
    le = len(text) if le == -1 else le
    for a, _b, off in tokens:
        if ls <= a < le:
            return timezone(timedelta(minutes=off))
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


def _date_for_range(text: str, pos: int, dates, sup):
    """Index of the date a range at ``pos`` should take, or None.

    NEAREST same-line date BEFORE the range wins; only if there is none does a
    date written AFTER the range apply ("Deadline 10:00 - 12:00 on 2026-12-16");
    only then the nearest preceding date anywhere. Taking the FIRST date on the
    line made "planned for 2026-09-24 has been moved to 2026-09-26, 10:00 -
    12:00" write the SUPERSEDED date - two days before the real outage, and not
    stale, so nothing caught it. Within each pool a date NOT marked superseded
    is preferred, which is the other half of the same fix.
    """
    ls = text.rfind("\n", 0, pos) + 1
    le = text.find("\n", pos)
    le = len(text) if le == -1 else le
    same_before, same_after, before = [], [], []
    for i, (p, _e, _d) in enumerate(dates):
        if p <= pos:
            before.append(i)
        if ls <= p < le:
            (same_before if p <= pos else same_after).append(i)
    for pool, take_last in ((same_before, True), (same_after, False),
                            (before, True)):
        if not pool:
            continue
        use = [i for i in pool if i not in sup] or pool
        return use[-1] if take_last else use[0]
    return 0 if dates else None


def _score(cand) -> int:
    """Rank one candidate window. Explicit weights, in policy order.

    Deliberately a handful of commented additions rather than a heuristic pile:
    this decides what gets written to a shared sheet unattended, so an operator
    asking "why did it pick that one" has to be able to read the answer.
    """
    return ((0 if cand["superseded"] else 8)      # a. not a withdrawn window
            + (0 if cand["past"] else 4)          # b. not last week's, in passing
            + (0 if cand["letterhead"] else 2)    # c. not the Issued: date
            + (1 if cand["future"] else 0))       # d. has not already ended


def _collect(text: str, now: datetime):
    """Every window the notice states, each with the flags _score reads."""
    tzdef = _tz_default()
    dates = _dates(text, now=now, tz=tzdef)
    if not dates:
        return []
    dspans = [(a, b) for a, b, _d in dates]
    ranges = _time_ranges(text, dspans)
    rspans = [(a, b) for a, b, _t1, _t2 in ranges]
    tokens = _tz_tokens(text, rspans)
    regions = _superseded_regions(text)
    sup_dates = _superseded_dates(text, dates)

    def _flags(span, date_idx):
        a, _b = span
        in_region = any(a >= s and a < e for s, e in regions)
        clause = _clause_before(text, a)
        superseded = in_region or bool(_SUPERSEDED_CLAUSE_RE.search(clause))
        if date_idx is not None and date_idx in sup_dates:
            superseded = True
        past = bool(_PAST_CLAUSE_RE.search(clause))
        letterhead = False
        if date_idx is not None:
            dpos = dates[date_idx][0]
            dls = text.rfind("\n", 0, dpos) + 1
            dle = text.find("\n", dpos)
            dle = len(text) if dle == -1 else dle
            letterhead = bool(_LETTERHEAD_RE.search(text[dls:dle]))
        return superseded, past, letterhead

    cands = []
    taken = []

    # A window with a date on BOTH ends is unambiguous - collect it first so the
    # single-date rules cannot re-read half of it.
    pts = _datetime_points(text, dates)
    for a, b in zip(pts, pts[1:]):
        between = text[a[1]:b[0]]
        if not re.fullmatch(r"\s*" + _DASH + r"\s*", between or ""):
            continue
        span = (a[0], b[1])
        tz = _tz_for(text, span, tokens)
        try:
            start = datetime(a[2][0], a[2][1], a[2][2], _h24(a[3], a[5]), a[4],
                             tzinfo=tz)
            end = datetime(b[2][0], b[2][1], b[2][2], _h24(b[3], b[5]), b[4],
                           tzinfo=tz)
        except ValueError:
            continue
        if end <= start:
            continue
        di = next((i for i, (p, _e, _d) in enumerate(dates) if p == a[0]), None)
        superseded, past, letterhead = _flags(span, di)
        taken.append(span)
        cands.append({"span": span, "start": start, "end": end,
                      "superseded": superseded, "past": past,
                      "letterhead": letterhead, "future": end >= now})

    for a, b, t1, t2 in ranges:
        if any(a < e and b > s for s, e in taken):
            continue
        di = _date_for_range(text, a, dates, sup_dates)
        if di is None:
            continue
        tz = _tz_for(text, (a, b), tokens)
        try:
            start, end = _build(dates[di][2], t1, t2, tz)
        except ValueError:
            continue
        superseded, past, letterhead = _flags((a, b), di)
        cands.append({"span": (a, b), "start": start, "end": end,
                      "superseded": superseded, "past": past,
                      "letterhead": letterhead, "future": end >= now})

    cands.sort(key=lambda c: c["span"][0])
    return cands


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


def _resolve(cands):
    """Pick the window the row should carry.

    -> (start, end, superseded, span_start) or None. The span comes back with
    the instants because the completion scoping in classify() has to know WHERE
    in the message the window was read from, and recomputing it separately is
    how the two halves of one decision drift apart.
    """
    if not cands:
        return None
    if _flag("NOTICE_WINDOW_PICK", "best") == "first":
        # Pre-2026-09 behaviour, kept switchable: the first range in the
        # document wins outright, superseded regions aside.
        live = [c for c in cands if not c["superseded"]] or cands
        c = live[0]
        return c["start"], c["end"], c["superseded"], c["span"][0]

    top = max(_score(c) for c in cands)
    live = _one_per_instant([c for c in cands if _score(c) == top])

    # Survivors within 24h of each other are ONE outage described in pieces (two
    # product lines, or the same window printed twice in two zones) and the row
    # takes their union: the row answers "when is this provider down", and a
    # span shorter than reality is the error that sends a duty engineer chasing
    # a live incident that is really the announced maintenance. Survivors more
    # than 24h apart are separate outages; the later one arrives on its own tick.
    clusters = []
    for c in sorted(live, key=lambda c: (c["start"], c["span"][0])):
        if clusters and c["start"] - clusters[-1][0]["start"] <= timedelta(days=1):
            clusters[-1].append(c)
        else:
            clusters.append([c])

    chosen = next((cl for cl in clusters if any(c["future"] for c in cl)),
                  clusters[0])
    start = min(c["start"] for c in chosen)
    end = max(c["end"] for c in chosen)
    first = min(c["span"][0] for c in chosen)
    return start, end, all(c["superseded"] for c in chosen), first


def _aware(now, tz):
    if now is None:
        return datetime.now(tz)
    if now.tzinfo is None:
        return now.replace(tzinfo=tz)
    return now


def _window_detail(t: str, now: datetime):
    """-> (start, end, superseded, span_start) or None, on NORMALISED text."""
    return _resolve(_collect(t, now))


def find_window(text: str, *, now=None):
    """-> (start, end) aware datetimes, or None.

    Dates and time-ranges are collected separately and paired, because providers
    routinely put them on different lines. Each range takes the nearest date at
    or before it. A range with no date ANYWHERE is unusable: guessing the year is
    how a row ends up with a window in the wrong month.

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
    start, end = detail[0], detail[1]
    return start, end


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
    """-> {action, reason, start, end, reschedule, stale}

    action is one of 'fill' | 'clear' | 'follow_quote' | 'needs_human' | 'ignore'.
    ``stale`` means the window has already passed; the caller decides whether to
    write it (a passive backlog read should not, a deliberate re-read may).
    """
    raw = (text or "").strip()
    out = {"action": "ignore", "reason": "", "start": None, "end": None,
           "reschedule": False, "stale": False}
    if not raw:
        out["reason"] = "empty message"
        return out

    # Parse a normalised COPY; the caller still writes `text` to Remark.
    t = _norm(raw)
    now = _aware(now, _tz_default())

    detail = _window_detail(t, now)
    win = (detail[0], detail[1]) if detail else None
    win_superseded = bool(detail and detail[2])
    resched = bool(RESCHEDULE_RE.search(t))

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
        out.update(action="fill", reason="rescheduled maintenance (overrides)",
                   start=win[0], end=win[1], reschedule=True,
                   stale=is_stale(win[1], now=now))
        return out

    # 2. Completed / cancelled, scoped to the SENTENCE that carries it. A
    #    completion notice repeats the original window verbatim, so it has to
    #    win over any window-based rule - but only over a window in its OWN
    #    sentence. "上次维护已完成。下次例行维护将于 X 进行。" is a report and an
    #    announcement in one bubble, and reading it as one statement threw the
    #    announcement away; a window stated in a different sentence wins.
    done = _done_spans(t)
    if done:
        at = detail[3] if detail else None
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

    # 3. An explicit answer that there is nothing planned.
    if NO_MAINT_RE.search(t) and not win:
        out.update(action="clear", reason="provider says no maintenance")
        return out

    # 4. An ordinary upcoming notice.
    if _sched_hit(t) and win:
        out.update(action="fill", reason="scheduled maintenance",
                   start=win[0], end=win[1], stale=is_stale(win[1], now=now))
        return out

    # 5. A short reply pointing at another message - the answer is in the quote.
    if len(t) <= _REFERRAL_MAX_CHARS and REFERRAL_RE.search(t) and not win:
        out.update(action="follow_quote",
                   reason="the answer is in the message this one quotes")
        return out

    if _sched_hit(t):
        # Per the ambiguity policy this stays `ignore`, not `needs_human`: both
        # write nothing, and nothing downstream consumes needs_human today.
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
