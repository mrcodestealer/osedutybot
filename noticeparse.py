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
  2. COMPLETED / CANCELLED next, and it wins outright: a completion notice repeats
     the original window verbatim, so any window-based rule alone re-fires on it.
  3. Only then the ordinary "upcoming maintenance" test.

Window formats seen in the wild, all handled (see tests):
    2026-09-16 09:00 - 12:00 GMT+8
    Time: Wednesday, September 2, 2026, 14:30 - 15:30 (UTC+8)
    时间：2026.09.23 周三 06:00 - 09:00 (UTC+8)
    日期Date：Sep 16th, 2026 (Wed)   +   时间Time：10:00 - 12:00 (GMT+8)   <- separate lines
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Dates and times
# ---------------------------------------------------------------------------

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}
_MONTH_WORD = (r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|"
               r"Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|"
               r"Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)")
_ORD = r"(?:st|nd|rd|th)?"

_DATE_RES = [
    (re.compile(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})"),
     lambda m: (int(m.group(1)), int(m.group(2)), int(m.group(3)))),
    (re.compile(_MONTH_WORD + r"\s+(\d{1,2})" + _ORD + r",?\s+(\d{4})", re.I),
     lambda m: (int(m.group(2)), _MONTHS[m.group(0)[:3].lower()], int(m.group(1)))),
    (re.compile(r"(\d{1,2})" + _ORD + r"\s+" + _MONTH_WORD + r",?\s+(\d{4})", re.I),
     lambda m: (int(m.group(2)),
                _MONTHS[re.search(_MONTH_WORD, m.group(0), re.I).group(0)[:3].lower()],
                int(m.group(1)))),
]

_DASH = r"(?:-{1,2}|–|—|~|～|至|to|until)"
_RANGE_RE = re.compile(
    r"(\d{1,2}):(\d{2})\s*([AaPp][Mm])?\s*" + _DASH
    + r"\s*(\d{1,2}):(\d{2})\s*([AaPp][Mm])?")

_TZ_RE = re.compile(r"(?:GMT|UTC)\s*([+-])\s*(\d{1,2})(?::(\d{2}))?"
                    r"|(?<![\d:])([+-])(\d{1,2}):(\d{2})(?!\d)", re.I)

# A reschedule restates the ORIGINAL date too; its updated block wins.
_UPDATED_RE = re.compile(r"更新[后後]时间|更新[后後]時間|Updated\s+Schedule|"
                         r"新的?时间|新的?時間|Revised\s+Schedule", re.I)

RESCHEDULE_RE = re.compile(
    r"时间变更|時間變更|改期|延期|重新安排|"
    r"Rescheduled?|Re-scheduled|Postponed|Updated\s+Schedule|Revised\s+Schedule",
    re.I)

# Wins outright (except over a reschedule, which is checked first).
#
# The cancel half used to be a bare `取消|cancell?ed`, which matched any use of
# the word: "All ongoing rounds will be cancelled during the maintenance" is a
# perfectly ordinary notice and was being thrown away. The cancel word now has
# to be tied to the maintenance itself.
DONE_RE = re.compile(
    r"维护完成|維護完成|維護已完成|维护已完成|已经完成|已經完成|维护结束|維護結束"
    r"|maintenance\s+(?:has\s+been\s+)?completed"
    r"|completed\s+successfully|has\s+been\s+completed"
    r"|(?:维护|維護)[^。．.\n]{0,12}(?:已|将|將)?取消"
    r"|(?:已|将|將)?取消[^。．.\n]{0,12}(?:维护|維護)"
    # Requires an auxiliary, so the maintenance ITSELF is what was cancelled.
    # The reverse direction ("cancelled ... maintenance") is deliberately gone:
    # it matched "All ongoing rounds will be cancelled during the maintenance",
    # an ordinary notice, and threw the whole thing away.
    r"|maintenance[^.\n]{0,40}\b(?:is|are|was|were|has\s+been|have\s+been|"
    r"will\s+be|being)\s+cancell?ed",
    re.I)

# "originally planned for X" - the window quoted in a reschedule that is being
# SUPERSEDED. Its presence means a bare window is ambiguous.
ORIGINAL_RE = re.compile(
    r"原定|原訂|原计划|原計劃|先前|previously\s+(?:set|scheduled|planned)"
    r"|originally\s+(?:set|scheduled|planned)|original\s+schedule",
    re.I)

# A line that merely restates the superseded window inside an updated block.
_ORIGINAL_LABEL_RE = re.compile(r"\s*(?:original|原(?:定|訂|计划|計劃))\s*[:：]", re.I)

SCHED_RE = re.compile(
    r"例行性维护|例行性維護|例行维护|例行維護|定期维护|定期維護"
    r"|正式环境.{0,12}维护|正式環境.{0,12}維護|生产环境.{0,12}维护"
    r"|进行.{0,8}维护|進行.{0,8}維護|停机维护|停機維護|系统维护|系統維護"
    r"|排定维护|排定維護|维护公告|維護公告|维护通知|維護通知"
    r"|scheduled\s+maintenance|routine\s+maintenance|production\s+maintenance"
    r"|maintenance\s+notification|maintenance\s+notice|under\s+maintenance"
    r"|will\s+have\s+.{0,30}maintenance|maintenance\s+will\s+commence",
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


def _tz_default():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(os.getenv("NOTICE_TZ", "Asia/Manila"))
    except Exception:
        return timezone(timedelta(hours=8))


def _tz_of(text: str):
    """The zone the notice states, else the configured default.

    The bare-offset alternative in _TZ_RE happily matches the SEPARATOR of a time
    range: in "10:00 -12:00 (GMT+8)" it read " -12:00" as UTC-12 and put the
    window ~20 hours out. So any candidate overlapping a time range is skipped -
    a range end is never a timezone.
    """
    t = text or ""
    spans = [(m.start(), m.end()) for m in _RANGE_RE.finditer(t)]

    def _inside_range(a: int, b: int) -> bool:
        return any(a < e and b > st for st, e in spans)

    m = None
    for cand in _TZ_RE.finditer(t):
        if not _inside_range(cand.start(), cand.end()):
            m = cand
            break
    if not m:
        return _tz_default()
    if m.group(2) is not None:
        sign, hh, mm = m.group(1), int(m.group(2)), int(m.group(3) or 0)
    else:
        sign, hh, mm = m.group(4), int(m.group(5)), int(m.group(6) or 0)
    off = timedelta(hours=hh, minutes=mm)
    return timezone(-off if sign == "-" else off)


def _dates(text: str):
    """[(start, end, (y, m, d))] for every full date, in document order.

    The END matters: a caller looking for the time that follows a date has to
    know where the date token actually stopped. Measuring it with \\S+ grabbed
    only "Sep" out of "Sep 16th, 2026".
    """
    out = []
    for rx, conv in _DATE_RES:
        for m in rx.finditer(text):
            try:
                y, mo, d = conv(m)
                datetime(y, mo, d)
                out.append((m.start(), m.end(), (y, mo, d)))
            except (ValueError, KeyError):
                continue
    out.sort(key=lambda p: p[0])
    return out


def _h24(h: int, ampm) -> int:
    if not ampm:
        return h
    ampm = ampm.lower()
    if ampm == "pm" and h != 12:
        return h + 12
    if ampm == "am" and h == 12:
        return 0
    return h


def _datetime_points(text: str):
    """[(start, end, (y, m, d), hh, mm, ampm)] for every "<date> <time>" pair.

    A date immediately followed by a time is one instant. Two of them joined by
    a dash is a window that spans days - which the time-only range regex cannot
    see, because a whole date sits between the dash and the second time.
    """
    out = []
    for pos, end, ymd in _dates(text):
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


def _two_date_window(text: str, tz):
    """A window whose two ends each carry their own date, or None."""
    pts = _datetime_points(text)
    for a, b in zip(pts, pts[1:]):
        between = text[a[1]:b[0]]
        if not re.fullmatch(r"\s*" + _DASH + r"\s*", between or ""):
            continue
        try:
            start = datetime(a[2][0], a[2][1], a[2][2], _h24(a[3], a[5]), a[4],
                             tzinfo=tz)
            end = datetime(b[2][0], b[2][1], b[2][2], _h24(b[3], b[5]), b[4],
                           tzinfo=tz)
        except ValueError:
            continue
        if end > start:
            return start, end
    return None


def find_window(text: str):
    """-> (start, end) aware datetimes, or None.

    Dates and time-ranges are collected separately and paired, because providers
    routinely put them on different lines. Each range takes the nearest date at
    or before it. A range with no date ANYWHERE is unusable: guessing the year is
    how a row ends up with a window in the wrong month.
    """
    t = text or ""
    marks = list(_UPDATED_RE.finditer(t))
    if marks:
        # An "Updated Schedule" block is authoritative and EXCLUSIVE. Falling
        # back to the whole text when it yields nothing returned the superseded
        # ORIGINAL window and then labelled it "(rescheduled)" - the worst of
        # both. If the updated block cannot be read, the answer is "unknown".
        #
        # Anchor on the LAST marker, not the first: these notices head the whole
        # section "Updated Schedule" and then list "Original: ..." underneath, so
        # the first marker swallowed the superseded window all over again.
        scope = t[marks[-1].start():]
        scope = "\n".join(
            ln for ln in scope.splitlines()
            if not ORIGINAL_RE.search(ln) and not _ORIGINAL_LABEL_RE.match(ln))
        scopes = [scope]
    else:
        scopes = [t]

    for scope in scopes:
        dates = _dates(scope)
        if not dates:
            continue
        tz = _tz_of(scope) if _TZ_RE.search(scope) else _tz_of(t)
        # A window with a date on BOTH ends is unambiguous - take it first.
        two = _two_date_window(scope, tz)
        if two:
            return two
        for rm in _RANGE_RE.finditer(scope):
            # Prefer a date on the SAME LINE as the range, in either direction.
            # "Deadline 10:00 - 12:00 on 2026-12-16" writes the date AFTER its
            # range, and nearest-date-BEFORE would bind the letterhead date at
            # the top of the notice instead.
            ls = scope.rfind("\n", 0, rm.start()) + 1
            le = scope.find("\n", rm.end())
            le = len(scope) if le == -1 else le
            same_line = [d for p, _e, d in dates if ls <= p < le]
            if same_line:
                y, mo, d = same_line[0]
            else:
                before = [d for p, _e, d in dates if p <= rm.start()]
                y, mo, d = before[-1] if before else dates[0][2]
            try:
                start = datetime(y, mo, d, _h24(int(rm.group(1)), rm.group(3)),
                                 int(rm.group(2)), tzinfo=tz)
                end = datetime(y, mo, d, _h24(int(rm.group(4)), rm.group(6)),
                               int(rm.group(5)), tzinfo=tz)
            except ValueError:
                continue
            if end <= start:
                end += timedelta(days=1)
            return start, end
    return None


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

    action is one of 'fill' | 'clear' | 'follow_quote' | 'ignore'.
    ``stale`` means the window has already passed; the caller decides whether to
    write it (a passive backlog read should not, a deliberate re-read may).
    """
    t = (text or "").strip()
    out = {"action": "ignore", "reason": "", "start": None, "end": None,
           "reschedule": False, "stale": False}
    if not t:
        out["reason"] = "empty message"
        return out

    win = find_window(t)
    resched = bool(RESCHEDULE_RE.search(t))

    # 1. A reschedule OVERRIDES whatever is on the row - checked before the
    #    completed/cancelled test, which would otherwise swallow "postponed".
    if resched:
        if not win:
            out.update(action="needs_human",
                       reason="rescheduled, but no new window could be read")
            return out
        if not _UPDATED_RE.search(t) and ORIGINAL_RE.search(t):
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

    # 2. Completed / cancelled wins outright: it quotes the original window.
    if DONE_RE.search(t):
        out["reason"] = "maintenance completed / cancelled notice"
        return out

    # 3. An explicit answer that there is nothing planned.
    if NO_MAINT_RE.search(t) and not win:
        out.update(action="clear", reason="provider says no maintenance")
        return out

    # 4. An ordinary upcoming notice.
    if SCHED_RE.search(t) and win:
        out.update(action="fill", reason="scheduled maintenance",
                   start=win[0], end=win[1], stale=is_stale(win[1], now=now))
        return out

    # 5. A short reply pointing at another message - the answer is in the quote.
    if len(t) <= _REFERRAL_MAX_CHARS and REFERRAL_RE.search(t) and not win:
        out.update(action="follow_quote",
                   reason="the answer is in the message this one quotes")
        return out

    if SCHED_RE.search(t):
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
