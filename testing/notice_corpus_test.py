#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""The regression corpus for ``noticeparse`` — the contract, written first.

Every provider in the maintenance Base posts its notice in its own dialect, and
the audit measured what that costs: of 27 realistic formats, 17 produce no
window or the wrong verdict. The KingMidas row is the standing proof — it still
carries 2026-09-16 10:00-12:00 because the notice for 2026-09-23 arrived in a
shape the parser cannot read, and a row that silently keeps LAST week's window
is worse than a blank one: nobody looks twice at a filled cell.

So this file is not a test of the parser as it is. It is the statement of what
every provider's notice MUST parse to, written before a line of the parser
changes, so the implementers have something to work against that does not move
while they work. A large number of failures today is the point.

Usage
-----
    python3 testing/notice_corpus_test.py              # summary + failures
    python3 testing/notice_corpus_test.py --verbose    # print every case
    python3 testing/notice_corpus_test.py --group E    # one group only
    python3 testing/notice_corpus_test.py --id A1      # one case
    python3 testing/notice_corpus_test.py --policy     # the ambiguity rules
    python3 testing/notice_corpus_test.py --strict-offset

Exit code is 0 only when every selected case passes.

Nothing here touches the network and nothing here writes to the Base. It
imports ``noticeparse`` and only ``noticeparse``: importing ``vawatch`` would
pull in requests and the Lark block, and a corpus that can reach the Base is a
corpus that will eventually write to it.

WHAT IS COMPARED
----------------
``action``, ``start`` and ``end`` always; ``stale`` and ``reschedule`` only
where a case pins them, because most cases have nothing to say about them.

start/end are compared as INSTANTS, not as wall clocks. What reaches the Base
is ``_ms()`` — epoch milliseconds — so "02:00 +00:00" and "10:00 +08:00" are
the same write and the same correct answer. The wall clock still matters for
the Laboratory card, which prints the parsed local time, so --strict-offset
additionally requires the offset to match; it is off by default because a
notice that states its window twice in two zones (case D12) is legitimately
right either way.

Each case carries the provider whose group the format came from. The formats
are the ones the 18 watched TELEGRAM groups actually use; the three TEAMS rows
(GEMINI, PG Soft, RTG) are represented too, because nothing watches them today
and the parser must be ready on the day something does.
"""

from __future__ import annotations

import argparse
import os
import sys
import textwrap
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Pinned, not merely defaulted. noticeparse reads NOTICE_TZ at call time, so a
# developer with NOTICE_TZ=UTC in their shell would otherwise see half this
# corpus fail for a reason that has nothing to do with the parser.
os.environ["NOTICE_TZ"] = "Asia/Manila"
# The flags the ambiguity policy below proposes. setdefault, so an implementer
# can still run the corpus against the legacy value to see what it costs.
os.environ.setdefault("NOTICE_DATE_ORDER", "dmy")
os.environ.setdefault("NOTICE_YEARLESS", "1")
os.environ.setdefault("NOTICE_WORDING", "wide")
os.environ.setdefault("NOTICE_WINDOW_PICK", "best")

import noticeparse  # noqa: E402  (after the sys.path and env pinning above)


# ---------------------------------------------------------------------------
# The ambiguity policy. Printable with --policy, because an implementer three
# weeks from now needs the reasoning, not just the expected values.
# ---------------------------------------------------------------------------

POLICY = """\
AMBIGUITY POLICY — the decisions this corpus pins. A parser change that
disagrees with one of these is wrong even if it makes more cases pass.

1. DATE ORDER — NOTICE_DATE_ORDER, default "dmy" (values: dmy | mdy)
   A component greater than 12 disambiguates on its own and the flag is not
   consulted: 23/09/2026 and 09/23/2026 are both 2026-09-23. The flag ONLY
   breaks the tie when both components are <= 12, e.g. 05/09/2026.
     default dmy -> 2026-09-05
   Justified from the providers on THIS Base, not from a general preference.
   The watched groups are Greater-China and SEA studios (KingMidas, JDB, JILI,
   SimplePlay, VA, TWSlot, Dropball) and Malta/Nordic studios selling into Asia
   (Hacksaw, Yggdrasil, PG Soft, Playtech, Evolution). Every one of those
   regions writes day-first, and the operator reading the sheet is in Manila,
   which also writes day-first. The single provider with US lineage, RTG, is an
   APP=TEAMS row that this path does not read at all today. So dmy is not a
   coin toss, it is the majority and then some — and the day RTG or another
   mdy-native provider is wired in, it is one env var, not a deploy.
   A date that is invalid under the chosen order falls back to the other order
   rather than being discarded: 13/09/2026 under mdy is not month 13, it is
   13 September.

2. A DATE WITH NO YEAR — "9/23", "9月23日", "Sep 23" — NOTICE_YEARLESS=1
   The year is a pure function of (text, now). No clock is read, no timezone
   of the host matters, and the same message classified twice on the same
   `now` always gives the same year.
     candidates = the same (month, day) in now.year-1, now.year, now.year+1,
                  discarding any that is not a real calendar date
     keep       = candidates whose DATE falls in [now-120d, now+245d]
                  (dates compared in the notice's own timezone)
     pick       = the earliest kept candidate on or after now's date;
                  if none is on or after, the latest kept candidate;
                  if nothing is kept, the date is UNUSABLE and the notice
                  yields no window (never a guessed year)
   The window is exactly 365 days wide on purpose: at most one of the three
   candidate years can ever qualify, so "prefer the future" is a tie-break that
   almost never fires and the rule cannot drift with the calendar. The split is
   120 back / 245 forward because a provider re-posting a notice from last
   quarter is routine — and that window is then caught by is_stale and not
   written — while nobody announces maintenance more than eight months out.
   Worked, with now = 2026-09-22:
     "9月23日"  -> 2026-09-23   (this week)
     "Sep 15"   -> 2026-09-15   (last week, stale, fill but not written)
     "1月5日"   -> 2027-01-05   (2026-01-05 is 260 days back, out of window)
   Set NOTICE_YEARLESS=0 to get today's behaviour: a year-less date is no date
   and the notice produces no window.

3. 24:00
   24:00 is the midnight that ENDS the stated day, i.e. 00:00 of the next day,
   wherever it appears. It is normalised BEFORE the "end <= start rolls over a
   day" rule, so 22:00-24:00 on the 23rd ends 2026-09-24 00:00 (not the 25th),
   and 24:00-02:00 on the 23rd runs 2026-09-24 00:00 -> 02:00.
   Today this raises ValueError inside datetime() and the whole notice is
   discarded — a provider writing the most ordinary overnight window in the
   industry gets silently ignored.

4. TIMEZONES
   An offset is taken from the line the time range sits on. Only if that line
   states none does the notice-level zone apply (the first zone token that is
   not inside a time range), and only if there is none of those does
   NOTICE_TZ (Asia/Manila, +08) apply.
   This is what makes a notice that prints the SAME window twice — a UTC line
   above a GMT+8 line — come out right: each clock is stamped with its own
   line's offset, so both lines denote the same instant and it does not matter
   which one wins. Stamping line A's clock with line B's offset is how you put
   a window eight hours out and never notice.
   Half-hour offsets are kept whole: GMT+0530 is +05:30, not +05:00.
   Named zones are a fixed, greppable table — no DST, no guessing:
     SGT MYT PHT CST HKT 北京时间 新加坡时间 = +08 | JST KST = +09
     ICT WIB = +07 | IST = +05:30 | UTC GMT Z = +00 | EST = -05 | PST = -08
   EST is -05 flat: these notices are written by studios that mean "New York",
   and a parser that guesses EDT in summer would move a window by an hour with
   nothing on the page to justify it. A zone not in the table is not invented;
   the notice falls back to NOTICE_TZ.
   Today every named zone silently becomes +08 — right for SGT/PHT/MYT by
   accident, an hour wrong for JST, thirteen hours wrong for EST.

5. WHICH WINDOW, WHEN A NOTICE HOLDS MORE THAN ONE — NOTICE_WINDOW_PICK=best
   A range is a candidate only if it is PART OF THE MAINTENANCE STATEMENT: the
   nearest subject word in its sentence names the maintenance (not the support
   desk, a promotion, a tournament, settlement, a deposit channel, a holiday),
   or it is a data line ("Time: …", "Slots: …") under a heading that is not
   about something else. Support hours and promo periods are never windows.
   A range takes a date only if one belongs to it, in this order:
     1. a date written against it - attached in front, or introduced after
        ("… 10:00-12:00 on 2026-09-30", "…，日期：9月30日"); an introduced date
        beats a merely earlier date on the line; both, disagreeing, is
        ambiguous
     2. the nearest eligible date earlier on the same line
     3. the nearest eligible date on an earlier line, unless the range's own
        sentence names a weekday that date does not fall on
     4. for a Time line only: a bare Date line directly below it
   A letterhead "Issued:"/"发布日期" date is never lent. A date is ELIGIBLE only
   if it is itself part of the maintenance statement. A range with no date that
   belongs to it is no window (policy 9: ignore) - it used to take the first
   date in the message, promo end dates and ticket ids included.
   Candidate windows are then ranked, highest first:
     a. not inside an Original/原定 block that an Updated block supersedes
     b. not introduced as a PAST window (上次/上周/last/previous/已完成)
     c. it ends at or after `now`
   The same window in two zones is ONE window (a converted copy that crossed
   midnight is moved back onto the same instant). Survivors that overlap,
   touch, or leave at most a two-hour gap are pieces of one outage and the row
   takes their UNION (case I6). Survivors further apart are separate outages
   and are NEVER merged: the row takes the EARLIEST one still in the future and
   classify() returns the rest in `others` and names them in `reason`.
   Where the text cannot settle the window the verdict is needs_human (nothing
   is written, a person is carded): two dates against one range, two zones on
   one line that disagree, a zone not in the table, the same window twice 12h
   apart, a row that could fall either side of midnight, a reschedule or
   correction still stating two windows with neither marked old, an earliest
   window only described ("the backoffice will be unavailable") while the
   named maintenance is later, and anything longer than
   NOTICE_MAX_WINDOW_HOURS (default 48; 0 switches the cap off).
   Set NOTICE_WINDOW_PICK=first for the old behaviour (the first range in the
   text wins), which is what makes case I6 write 10:00-12:00 only. The
   relevance and date rules apply in both modes.

6. WHAT COUNTS AS A MAINTENANCE NOTICE — NOTICE_WORDING=wide
   The gate widens to the headings providers actually use (group E) — upgrade,
   downtime, emergency maintenance, 維修, 停服, service interruption, "will be
   unavailable", "temporarily suspend". It stays closed on the same words when
   their subject is not the service: a promo that is 暂停, a banner page that
   "will be unavailable", a tournament with a time range (group F). Subject,
   not keyword. Set NOTICE_WORDING=strict for today's SCHED_RE.

7. COMPLETED / CANCELLED (group G)
   A completion or cancellation suppresses the notice only when it is asserted
   of the MAINTENANCE, in a non-future frame.
     * "We will notify you once it has been completed" is a promise about a
       maintenance that has not happened yet. A clause governed by
       will/shall/once/after/when/upon/as soon as never suppresses.
     * A cancelled OBJECT is not a cancelled maintenance. The English guard
       already exists but only looks to the right of the word "maintenance",
       so "All ongoing rounds before the maintenance will be cancelled" still
       throws the notice away. The guard must read the clause's subject on
       both sides, and the Chinese branch needs the same noun list
       (注单/投注/赛事/活动/优惠/交易/订单/局).
     * A message may hold both: "上次维护已完成。下次例行维护将于 …". The
       completion is scoped to its own sentence; a LATER sentence stating a
       future window wins and the notice fills.

8. RESCHEDULE (group H)
   The reschedule verb must take the maintenance as its subject. A notice whose
   only 延期 is a postponed anniversary promo is an ordinary notice that
   happens to contain the word, and must fill its window with reschedule=False
   — today it is worse than wrong: paired with an unrelated 原定 it returns
   needs_human and nothing is written at all. "moved from X to Y" and 改期/延后
   are reschedules and must set reschedule=True, because the card's
   "(rescheduled)" label is how the Laboratory group knows the row changed.
   The NEW window is written, never the superseded one.

9. OUT OF SCOPE, ON PURPOSE
   A message with maintenance wording and no readable window stays `ignore`,
   not `needs_human`. Both write nothing, nothing in vawatch consumes
   needs_human today, and relabelling would only inflate a category no one
   reads. When something does consume it, that is its own change with its own
   corpus.
"""


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

NOW = "2026-09-22T09:00:00+08:00"          # today, 09:00 Manila

GROUPS = {
    "A": "REGRESSION — must pass today and never break",
    "B": "DATE FORMATS",
    "C": "TIME FORMATS",
    "D": "TIMEZONES",
    "E": "WORDING GATE — real headings that must open it",
    "F": "FALSE POSITIVES — must stay ignored",
    "G": "COMPLETED / CANCELLED — false negatives and true positives",
    "H": "RESCHEDULE",
    "I": "WINDOW SELECTION",
    "K": "WINDOW CHOICE — audit round 2 (which range, which date)",
}


def M(s: str) -> str:
    """A multi-line notice, dedented and stripped of its leading newline.

    Indentation in the source must not reach the parser: several rules are
    line-anchored (the Original-label test matches at the start of a line) and
    a corpus that quietly indents every line is testing a notice no provider
    ever sent.
    """
    return textwrap.dedent(s).strip("\n")


def C(cid, group, provider, text, action, start=None, end=None, *,
      now=NOW, stale=None, resched=None, others=None, env=None, note=""):
    """One case. ``others`` pins classify()'s list of further windows the
    notice states but the row does not get, as [(start, end), ...] ISO pairs;
    ``env`` sets flags for this case only (noticeparse reads them per call)."""
    return {"id": cid, "group": group, "provider": provider, "text": text,
            "action": action, "start": start, "end": end, "now": now,
            "stale": stale, "resched": resched, "others": others,
            "env": env or {}, "note": note}


KINGMIDAS = M("""
    🚧🚧🚧  維護公告 Maintenance Notification  🚧🚧🚧

    尊敬的客户,
    Hi Team,

    我们恭敬地向您报告， KingMidas - KM系统 将于9/23 (三) 10:00AM (GMT+8) 排定维护，在这个期间 KingMidas - KM系统所有的游戏服务都会暂停使用。
    有任何更新信息，我们将尽快向您报告，不便之处敬请原谅。

    Please note that the KingMidas - Sysyem scheduled maintenance will commence at 10:00AM 23th(Wed) Sep (GMT+8).
    During this period, you will not be able to access to all the games.
    We will keep you posted on the status. Thank you for your understanding of any inconvenience caused.

    日期Date：Sep 23th, 2026 (Wed)
    时间Time：10:00 - 12:00  (GMT+8)
    受影响之游戏Affected Game :
    正式环境所有的游戏服务、后台与API
    All games(Slots, TableGames), Backoffice and API service on Production
""")


CASES = [

    # -- A. REGRESSION ------------------------------------------------------
    # These are the formats noticeparse's own docstring claims, plus the real
    # KingMidas message. They are expected to pass against the UNMODIFIED
    # parser; any failure here is a defect to report, not a target to build.

    C("A1", "A", "KingMidas", KINGMIDAS, "fill",
      "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00", stale=False,
      note="the live notice that failed to overwrite the row: the date line and "
           "the time line are separate, and the earlier 9/23 must not confuse it"),

    C("A2", "A", "Hacksaw",
      "Scheduled maintenance 2026-09-16 09:00 - 12:00 GMT+8",
      "fill", "2026-09-16T09:00:00+08:00", "2026-09-16T12:00:00+08:00",
      stale=True,
      note="docstring format 1; last week, so fill but stale — vawatch declines "
           "to write a window that is already over"),

    C("A3", "A", "Playtech",
      "Scheduled maintenance notification.\n"
      "Time: Wednesday, September 2, 2026, 14:30 - 15:30 (UTC+8)",
      "fill", "2026-09-02T14:30:00+08:00", "2026-09-02T15:30:00+08:00",
      stale=True,
      note="docstring format 2: weekday word, month name, comma-heavy"),

    C("A4", "A", "VA",
      "例行性维护通知\n时间：2026.09.23 周三 06:00 - 09:00 (UTC+8)",
      "fill", "2026-09-23T06:00:00+08:00", "2026-09-23T09:00:00+08:00",
      note="docstring format 3: dotted ISO date with a Chinese weekday between "
           "the date and the time"),

    C("A5", "A", "SimplePlay",
      "維護公告\n日期Date：Sep 16th, 2026 (Wed)\n時間Time：10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-16T10:00:00+08:00", "2026-09-16T12:00:00+08:00",
      stale=True,
      note="docstring format 4: the date and the range on different lines"),

    C("A6", "A", "JDB",
      "排定维护 2026-09-23 23:00 - 01:00 (GMT+8)",
      "fill", "2026-09-23T23:00:00+08:00", "2026-09-24T01:00:00+08:00",
      note="end <= start already rolls over one day; the overnight window is "
           "the commonest shape there is and must stay working"),

    C("A7", "A", "Evolution",
      "Scheduled maintenance 2026-09-23 22:00 - 2026-09-24 02:00 (GMT+8)",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-24T02:00:00+08:00",
      note="a date on BOTH ends: the two-date path, which the time-only range "
           "regex cannot see because a whole date sits inside the dash"),

    C("A8", "A", "Yggdrasil",
      "Scheduled maintenance on 2026-09-23 from 10:00 to 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="'to' as the range separator, with the date written before it"),

    C("A9", "A", "TWSlot",
      "排定维护 2026-09-23 10:00~12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="tilde separator"),

    C("A10", "A", "PG Soft",
      "Scheduled maintenance on 23rd September 2026, 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="day-ordinal then month word then year — already supported"),

    C("A11", "A", "Dropball",
      "维护公告：2026-09-23 10:00 - 12:00",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="no zone stated at all: NOTICE_TZ (Asia/Manila, +08) applies"),

    C("A12", "A", "Playstar",
      M("""
        Updated Schedule
        Date: 2026-09-25
        Time: 10:00 - 12:00 (GMT+8)
        Original: 2026-09-23 10:00 - 12:00 (GMT+8)
      """),
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True,
      note="the updated block is authoritative and the Original line beneath it "
           "is dropped — the fix that stopped the superseded window winning"),

    C("A13", "A", "JILI",
      "The scheduled maintenance on 2026-09-23 has been cancelled.",
      "ignore",
      note="a genuine cancellation of the maintenance itself: nothing written"),

    C("A14", "A", "5G",
      "No maintenance this week, thank you.",
      "clear",
      note="the explicit answer to our weekly ask -> Remark = No maintenance"),

    C("A15", "A", "EEZE",
      "本周无维护计划，谢谢。",
      "clear",
      note="the same answer in Chinese"),

    C("A16", "A", "Gemini",
      "維護已完成，所有遊戲服務已恢復正常。",
      "ignore",
      note="a completion notice quotes nothing and asserts nothing upcoming"),

    C("A17", "A", "RTG",
      "Please refer to this notification above.",
      "follow_quote",
      note="a short reply whose answer lives in the message it quotes; the "
           "caller fetches the quote rather than guessing"),

    C("A18", "A", "ColorGame",
      "The maintenance originally scheduled for 2026-09-23 10:00 - 12:00 "
      "(GMT+8) has been postponed. A new date will follow.",
      "needs_human", resched=True,
      note="postponed with NO new window: the only window present is the one "
           "being withdrawn, so writing it would re-assert what was cancelled"),

    C("A19", "A", "Baccarat", "   ", "ignore",
      note="an empty bubble — whitespace-only messages arrive from the scrape"),

    C("A20", "A", "PaiGow",
      "維護公告 2026-09-23 10:00 -12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="no space after the dash: ' -12:00' once read as UTC-12 and moved "
           "the window twenty hours; a range end is never a timezone"),

    # -- B. DATE FORMATS ----------------------------------------------------
    # Each isolates ONE date shape. The wording and the time range are held at
    # formats the parser already accepts, so a failure here is the date and
    # nothing else.

    C("B1", "B", "KingMidas",
      "维护公告\n维护日期：2026年9月23日\n维护时间：10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="年月日 — the single commonest date shape among the zh providers "
           "and completely unreadable today"),

    C("B2", "B", "JDB",
      "维护公告\n日期：9月23日\n时间：10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="月日 with no year -> policy 2 picks 2026, the nearest future"),

    C("B3", "B", "VA",
      "排定维护 2026.9.23 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="dotted ISO with unpadded month/day — already supported, kept here "
           "so a rewrite of the date rules cannot lose it"),

    C("B4", "B", "Hacksaw",
      "Scheduled maintenance 23/09/2026 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="dd/mm/yyyy; 23 > 12 so the order flag is not even consulted"),

    C("B5", "B", "RTG",
      "Scheduled maintenance 09/23/2026 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="mm/dd/yyyy; 23 > 12 disambiguates in the other direction, so both "
           "orders must yield the same day"),

    C("B6", "B", "Yggdrasil",
      "Scheduled maintenance 23-09-2026 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="dashes, day first — today the date regex demands a 4-digit year in "
           "front and reads nothing here"),

    C("B7", "B", "KingMidas",
      "排定维护 9/23 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the exact shape inside the live KingMidas notice; 23 > 12 fixes the "
           "order, policy 2 fixes the year"),

    C("B8", "B", "Playtech",
      "Scheduled maintenance Sept. 23, 2026, 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="abbreviation WITH a period: the trailing dot breaks the month-word "
           "rule, which expects whitespace straight after the month"),

    C("B9", "B", "PG Soft",
      "Scheduled maintenance 23rd September 2026 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="ordinal suffix, month spelled out — already supported"),

    C("B10", "B", "Evolution",
      "Scheduled maintenance Sep 23, 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="month word with no year -> policy 2"),

    C("B11", "B", "SimplePlay",
      "排定维护 2026/09/23 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="slashed ISO — already supported"),

    C("B12", "B", "EEZE",
      "Scheduled maintenance 23.09.2026 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="dotted, day first — the European shape the Malta studios use"),

    C("B13", "B", "Hacksaw",
      "Scheduled maintenance 05/09/2026 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-05T10:00:00+08:00", "2026-09-05T12:00:00+08:00",
      stale=True,
      note="BOTH components <= 12: this is the case NOTICE_DATE_ORDER exists "
           "for, and dmy makes it 5 September, already past"),

    C("B14", "B", "Yggdrasil",
      "Scheduled maintenance 11/12/2026 10:00 - 12:00 (GMT+8)",
      "fill", "2026-12-11T10:00:00+08:00", "2026-12-11T12:00:00+08:00",
      note="the other dmy/mdy tie, chosen forward in time so the wrong reading "
           "would still look plausible on the sheet — which is why the flag has "
           "to be decided once, in writing"),

    C("B15", "B", "TWSlot",
      "维护公告\n日期：2026年09月23日\n时间：10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="年月日 zero-padded"),

    C("B16", "B", "Dropball",
      "维护公告\n日期：9月23日(周三)\n时间：10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="year-less 月日 with the weekday glued on in full-width brackets"),

    C("B17", "B", "JILI",
      "维护公告\n日期：12月30日\n时间：10:00 - 12:00 (GMT+8)",
      "fill", "2026-12-30T10:00:00+08:00", "2026-12-30T12:00:00+08:00",
      note="year-less, 99 days out: still this year, inside the +245d window"),

    C("B18", "B", "JILI",
      "维护公告\n日期：1月5日\n时间：10:00 - 12:00 (GMT+8)",
      "fill", "2027-01-05T10:00:00+08:00", "2027-01-05T12:00:00+08:00",
      note="year-less across the year boundary: 2026-01-05 is 260 days back, "
           "outside the window, so the only candidate is 2027"),

    C("B19", "B", "Playstar",
      "Scheduled maintenance Sep 15, 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-15T10:00:00+08:00", "2026-09-15T12:00:00+08:00",
      stale=True,
      note="year-less and in the recent past: the rule must NOT jump to 2027 "
           "to keep it in the future — a re-posted old notice stays old, and "
           "stale is what stops it overwriting a live row"),

    C("B20", "B", "KingMidas",
      "排定维护\n2026年9月23日 10:00 至 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="年月日 with 至 as the separator, all on one line"),

    C("B21", "B", "Playtech",
      "Scheduled maintenance September 23rd, 2026, 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="month word then ordinal day — already supported"),

    C("B22", "B", "Evolution",
      "Scheduled maintenance 23 Sep 2026 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="bare day month year, no punctuation — already supported"),

    C("B23", "B", "JILI",
      "维护公告\n日期：12月30日\n时间：10:00 - 12:00 (GMT+8)",
      "fill", "2026-12-30T10:00:00+08:00", "2026-12-30T12:00:00+08:00",
      now="2027-01-05T09:00:00+08:00", stale=True,
      note="the same text as B17 read two weeks later resolves to the PREVIOUS "
           "year — proof the year rule is a function of (text, now) and of "
           "nothing else"),

    # -- C. TIME FORMATS ----------------------------------------------------

    C("C1", "C", "Hacksaw",
      "Scheduled maintenance 2026-09-23, 10AM - 12PM (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="no minutes at all; the range regex demands :MM and reads nothing"),

    C("C2", "C", "JDB",
      "排定维护 2026-09-23 10.00-12.00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="dot as the hour/minute separator; must not be confused with a "
           "dotted date, hence: only inside a dash-joined pair of HH.MM"),

    C("C3", "C", "VA",
      "排定维护 2026-09-23 10：00-12：00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="full-width colon — what a zh IME produces by default"),

    C("C4", "C", "TWSlot",
      "排定维护 2026-09-23 １０：００－１２：００ (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="full-width digits, colon and dash together; normalising them with "
           "a 1:1 translation table keeps every offset in the text valid, "
           "which the line-anchored rules depend on"),

    C("C5", "C", "Evolution",
      "Scheduled maintenance 2026-09-23 22:00-24:00 (GMT+8)",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-24T00:00:00+08:00",
      note="24:00 ends the stated day; today hour 24 raises inside datetime(), "
           "the range is skipped by the ValueError guard, and the notice ends "
           "up with no window at all"),

    C("C6", "C", "Playtech",
      "Scheduled maintenance 2026-09-23 00:00-24:00 (GMT+8)",
      "fill", "2026-09-23T00:00:00+08:00", "2026-09-24T00:00:00+08:00",
      note="a full-day outage: 00:00 to the midnight that ends the day"),

    C("C7", "C", "Yggdrasil",
      "Scheduled maintenance 2026-09-23 23:00-01:00 (GMT+8)",
      "fill", "2026-09-23T23:00:00+08:00", "2026-09-24T01:00:00+08:00",
      note="crosses midnight by the end<=start rule — already supported"),

    C("C8", "C", "PG Soft",
      "Scheduled maintenance on 2026-09-23 from 10:00 to 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="'from X to Y' — already supported"),

    C("C9", "C", "SimplePlay",
      "排定维护 2026-09-23 10:00~12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="ASCII tilde — already supported"),

    C("C10", "C", "KingMidas",
      "排定维护 2026-09-23 22:00 - 2026-09-24 02:00 (GMT+8)",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-24T02:00:00+08:00",
      note="both ends carry their own date — already supported"),

    C("C11", "C", "Playstar",
      "Scheduled maintenance 2026-09-23 10:00 – 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="en dash, which is what a pasted Word notice carries"),

    C("C12", "C", "EEZE",
      "Scheduled maintenance 2026-09-23 10:00—12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="em dash with no spaces"),

    C("C13", "C", "Dropball",
      "排定维护 2026-09-23 10:00至12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="至 with no spaces — already supported"),

    C("C14", "C", "JILI",
      "Scheduled maintenance 2026-09-23 10:00AM - 12:00PM (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="am/pm with minutes — already supported"),

    C("C15", "C", "Baccarat",
      "Scheduled maintenance 2026-09-23 10:00 PM - 11:30 PM (GMT+8)",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-23T23:30:00+08:00",
      note="pm with a space before it, both ends"),

    C("C16", "C", "Roulette",
      "Scheduled maintenance 2026-09-23 12:00AM - 02:00AM (GMT+8)",
      "fill", "2026-09-23T00:00:00+08:00", "2026-09-23T02:00:00+08:00",
      note="12AM is midnight at the START of the day — the mirror of the 24:00 "
           "rule and the one place they must not be confused"),

    C("C17", "C", "5G",
      "Scheduled maintenance 2026-09-23, 9AM-11AM (GMT+8)",
      "fill", "2026-09-23T09:00:00+08:00", "2026-09-23T11:00:00+08:00",
      note="single-digit hour, no minutes, no spaces"),

    C("C18", "C", "ColorGame",
      "排定维护 2026-09-23 10时 - 12时 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="时 as the hour marker with no minutes — common from the mainland "
           "studios and unreadable today"),

    C("C19", "C", "PaiGow",
      "排定维护 2026-09-23 10:00～12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="full-width tilde — already supported"),

    C("C20", "C", "Gemini",
      "Scheduled maintenance 2026-09-23 24:00 - 02:00 (GMT+8)",
      "fill", "2026-09-24T00:00:00+08:00", "2026-09-24T02:00:00+08:00",
      note="24:00 as the START: it normalises to the next day's 00:00 BEFORE "
           "the rollover rule, so the window is two hours, not twenty-six"),

    # -- D. TIMEZONES -------------------------------------------------------

    C("D1", "D", "KingMidas",
      "排定维护 2026-09-23 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the baseline every other D case is measured against"),

    C("D2", "D", "JDB",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (UTC+7)",
      "fill", "2026-09-23T10:00:00+07:00", "2026-09-23T12:00:00+07:00",
      note="an hour that is NOT +08 — already supported and easy to regress"),

    C("D3", "D", "Playtech",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+0530)",
      "fill", "2026-09-23T10:00:00+05:30", "2026-09-23T12:00:00+05:30",
      note="four-digit offset with no colon: today the minutes are dropped and "
           "the window lands half an hour early"),

    C("D4", "D", "Playtech",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+05:30)",
      "fill", "2026-09-23T10:00:00+05:30", "2026-09-23T12:00:00+05:30",
      note="the same offset written with a colon — already supported, which is "
           "what makes D3 a formatting bug rather than a missing feature"),

    C("D5", "D", "Hacksaw",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 SGT",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="named zone. Right today only by luck: nothing matches, so the "
           "Manila default applies and happens to be +08"),

    C("D6", "D", "VA",
      "排定维护 2026-09-23 10:00 - 12:00 PHT",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="named zone, right today for the same accidental reason"),

    C("D7", "D", "Yggdrasil",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 MYT",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="named zone, +08"),

    C("D8", "D", "Evolution",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 JST",
      "fill", "2026-09-23T10:00:00+09:00", "2026-09-23T12:00:00+09:00",
      note="the luck runs out: JST is +09 and today becomes +08, putting the "
           "row an hour late — small enough that nobody would question it"),

    C("D9", "D", "RTG",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 EST",
      "fill", "2026-09-23T10:00:00-05:00", "2026-09-23T12:00:00-05:00",
      note="thirteen hours wrong today. EST is pinned at -05 with no DST "
           "guessing, per policy 4"),

    C("D10", "D", "TWSlot",
      "排定维护 2026-09-23 10:00 - 12:00 北京时间",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the zone named in Chinese, +08"),

    C("D11", "D", "Dropball",
      "排定维护 2026-09-23 10:00 - 12:00",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="no zone anywhere: NOTICE_TZ decides, and that must stay explicit "
           "rather than following the host clock"),

    C("D12", "D", "PG Soft",
      M("""
        Scheduled maintenance
        Date: 2026-09-23
        02:00 - 04:00 (UTC+0)
        10:00 - 12:00 (GMT+8)
      """),
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="ONE window printed twice in two zones — the two lines denote the "
           "same instant, so either may win, but each clock must be stamped "
           "with ITS OWN line's offset. This one passes today only because two "
           "errors cancel: the whole notice takes the FIRST zone found (+00) "
           "and the first line happens to be the one written in it. Run with "
           "--strict-offset and it fails, and D13 shows what happens when the "
           "same luck runs the other way"),

    C("D13", "D", "PG Soft",
      M("""
        Scheduled maintenance
        Date: 2026-09-23
        02:00 - 04:00 (UTC)
        10:00 - 12:00 (GMT+8)
      """),
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the same trap with a bare UTC and no digits: 'UTC' alone is +00, "
           "not 'no zone stated'"),

    C("D14", "D", "SimplePlay",
      "排定维护 2026-09-23 10:00 - 12:00 (GMT +8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="a space between GMT and the sign — already supported"),

    C("D15", "D", "Playstar",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (UTC-4)",
      "fill", "2026-09-23T10:00:00-04:00", "2026-09-23T12:00:00-04:00",
      note="a negative offset, which must survive the rule that ignores a "
           "minus sign belonging to a range separator"),

    C("D16", "D", "JILI",
      "Scheduled maintenance 2026-09-23 (GMT+8) 10:00 - 12:00",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the zone written BEFORE the range on the same line"),

    C("D17", "D", "EEZE",
      "排定维护 2026-09-23 10:00 - 12:00 新加坡时间",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the zone named in Chinese by city, +08"),

    C("D18", "D", "Baccarat",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 ICT",
      "fill", "2026-09-23T10:00:00+07:00", "2026-09-23T12:00:00+07:00",
      note="ICT is +07: an Indochina studio's window is an hour off if the "
           "Manila default swallows it"),

    C("D19", "D", "Gemini",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 +08:00",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="a bare offset with no GMT/UTC in front — already supported, and the "
           "rule that skips range separators is what keeps it working"),

    # -- E. WORDING GATE ----------------------------------------------------
    # The audit's finding #8: 27 of 28 realistic headings are rejected. Every
    # case below carries a clean, already-supported window, so the ONLY thing
    # under test is whether the heading opens the gate.

    C("E1", "E", "KingMidas",
      "【系统升级】\n2026-09-23 10:00 - 12:00 (GMT+8)\n升级期间玩家无法登录。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="升级 without the word 维护 anywhere: an upgrade takes the service "
           "down exactly like maintenance does"),

    C("E2", "E", "JDB",
      "【服务器维护】\n2026-09-23 10:00 - 12:00 (GMT+8)\n期间玩家将无法登录游戏。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="服务器维护 — the gate lists 系统维护 and 停机维护 but not this one"),

    C("E3", "E", "VA",
      "【平台维护】\n2026-09-23 10:00 - 12:00 (GMT+8)\n敬请留意。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="平台维护"),

    C("E4", "E", "TWSlot",
      "【紧急维护】\n因机房故障，紧急维护安排如下：\n2026-09-23 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="紧急维护 — the one kind of notice you least want dropped"),

    C("E5", "E", "Dropball",
      "维护通告\n2026-09-23 10:00 - 12:00 (GMT+8)\n感谢您的理解。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="通告, one character away from the 通知/公告 the gate accepts"),

    C("E6", "E", "JILI",
      "停服公告\n2026-09-23 10:00 - 12:00 (GMT+8)\n届时所有游戏将下线。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="停服 — states the outage without ever saying 维护"),

    C("E7", "E", "SimplePlay",
      "各位好，我們將進行系統維修。\n時間：2026-09-23 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="維修 rather than 維護 — the traditional-character studios use both"),

    C("E8", "E", "Playstar",
      "服务暂停通知\n2026-09-23 10:00 - 12:00 (GMT+8)\n期间无法投注。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="服务暂停 — 暂停 counts only because its subject is 服务; see F9"),

    C("E9", "E", "PaiGow",
      "暫停服務公告\n2026-09-23 10:00 - 12:00 (GMT+8)\n造成不便敬請見諒。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="暫停服務, the same idea with the words the other way round"),

    C("E10", "E", "Hacksaw",
      M("""
        Dear Partners,
        Server Maintenance
        Date: 2026-09-23
        Time: 10:00 - 12:00 (GMT+8)
        Games will be offline during this period.
      """),
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="'Server Maintenance' is not 'scheduled maintenance', so the gate "
           "rejects the plainest English heading in the corpus"),

    C("E11", "E", "Yggdrasil",
      "System Upgrade\n2026-09-23 10:00 - 12:00 (GMT+8)\nAll games will be offline.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="upgrade, not maintenance"),

    C("E12", "E", "Evolution",
      "Platform Upgrade\n2026-09-23 10:00 - 12:00 (GMT+8)\nThank you.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="Platform Upgrade"),

    C("E13", "E", "Playtech",
      "Scheduled Downtime\n2026-09-23 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="'Scheduled Downtime' — the gate matches 'scheduled maintenance' "
           "and misses the synonym by one word"),

    C("E14", "E", "PG Soft",
      "Emergency Maintenance\n2026-09-23 10:00 - 12:00 (GMT+8)\nWe apologise for the short notice.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="emergency maintenance in English, the twin of E4"),

    C("E15", "E", "5G",
      "Dear Team, our games will be unavailable on 2026-09-23 from 10:00 to 12:00 (GMT+8).",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the outage described without naming it; the subject is 'our games', "
           "which is what separates it from F17"),

    C("E16", "E", "EEZE",
      "All game services will be temporarily suspended on 2026-09-23, 10:00 - 12:00 (GMT+8).",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="'temporarily suspend', subject 'all game services'"),

    C("E17", "E", "ColorGame",
      "Notice of service interruption: 2026-09-23 10:00 - 12:00 (GMT+8).",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="'service interruption'"),

    C("E18", "E", "Baccarat",
      "Planned Downtime\nDate: 2026-09-23\nTime: 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="'Planned Downtime', the sibling of E13"),

    C("E19", "E", "Gemini",
      "停机公告\n2026-09-23 10:00 - 12:00 (GMT+8)\n请提前下线。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="停机公告 — the gate has 停机维护 but not this"),

    # -- F. FALSE POSITIVES -------------------------------------------------
    # The guardrails. Group E widens the gate; these are what stops the widening
    # from turning every promo with a clock in it into a written outage.

    C("F1", "F", "Hacksaw",
      "Last week's maintenance went smoothly, thanks for your patience.",
      "ignore",
      note="maintenance mentioned, nothing scheduled, no window: nothing to write"),

    C("F2", "F", "JILI",
      "🎉 Weekend Cashback! Bonus runs 2026-09-23 10:00 - 12:00 (GMT+8). Join now!",
      "ignore",
      note="a promo with a perfectly parseable range — the reason the gate "
           "cannot simply be 'has a window'"),

    C("F3", "F", "VA",
      "維護已完成，所有游戏服务已恢复。",
      "ignore",
      note="a genuine completion"),

    C("F4", "F", "Yggdrasil",
      "No maintenance this week.",
      "clear",
      note="the weekly all-clear"),

    C("F5", "F", "TWSlot",
      "本周无维护计划。",
      "clear",
      note="the weekly all-clear in Chinese"),

    C("F6", "F", "PG Soft",
      "API parameter change effective 2026-09-23 10:00 (GMT+8). No downtime expected.",
      "ignore",
      note="an API announcement carrying a date and the word downtime, but "
           "explicitly no outage — the gate must read the sentence, not the word"),

    C("F7", "F", "Evolution",
      "Tournament runs 2026-09-23 10:00 - 12:00 (GMT+8). Prize pool 50,000.",
      "ignore",
      note="a tournament schedule shaped exactly like a maintenance window"),

    C("F8", "F", "Playstar",
      "New game 'Gold Rush' launches 2026-09-23 10:00 (GMT+8).",
      "ignore",
      note="a launch announcement"),

    C("F9", "F", "KingMidas",
      "周年庆活动暂停，2026-09-23 10:00 - 12:00 (GMT+8) 期间不参与积分。",
      "ignore",
      note="暂停 whose subject is 活动, not 服务 — the direct guardrail for E8 "
           "and E9, and the one a keyword-only widening breaks first"),

    C("F10", "F", "Playtech",
      "The scheduled maintenance has been completed successfully.",
      "ignore",
      note="a genuine completion in English"),

    C("F11", "F", "SimplePlay",
      "The scheduled maintenance on 2026-09-23 has been cancelled.",
      "ignore",
      note="a genuine cancellation, window still quoted: it must not be written"),

    C("F12", "F", "JDB",
      "维护已取消。",
      "ignore",
      note="a genuine cancellation in Chinese"),

    C("F13", "F", "5G",
      "Daily report 2026-09-22: all systems normal, 10:00 - 12:00 peak traffic.",
      "ignore",
      note="an operational report with a date and a range and no outage at all"),

    C("F14", "F", "EEZE",
      "There is no maintenance scheduled for next week.",
      "clear",
      note="the all-clear phrased with the word scheduled in it"),

    C("F15", "F", "Dropball",
      "Hi team, good morning!",
      "ignore",
      note="ordinary chatter — the watcher reads whole groups, most of it is this"),

    C("F16", "F", "ColorGame",
      "We may have maintenance soon, will confirm the schedule later.",
      "ignore",
      note="maintenance with no window and no commitment; per policy 9 this "
           "stays ignore rather than becoming needs_human"),

    C("F17", "F", "Gemini",
      "The promo page will be unavailable on 2026-09-23 10:00 - 12:00 (GMT+8) "
      "while we update the banner.",
      "ignore",
      note="'will be unavailable' with a promo page as its subject: the twin of "
           "E15 and the case that forces the gate to be about subjects"),

    # -- G. COMPLETED / CANCELLED ------------------------------------------

    C("G1", "G", "Hacksaw",
      "Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8). We will notify "
      "you once it has been completed.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="finding #9: 'once it has been completed' is a promise about a "
           "future maintenance, and the completion test reads it as a report"),

    C("G2", "G", "KingMidas",
      "维护时间 2026-09-23 10:00-12:00。维护期间未结算注单将取消。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="finding #10: the BETS are cancelled, not the maintenance. The "
           "English branch already has this noun guard; the Chinese one does not"),

    C("G3", "G", "Evolution",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8). All ongoing "
      "rounds before the maintenance will be cancelled.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="finding #11: the guard only looks to the RIGHT of 'maintenance', so "
           "a subject noun standing to its left slips straight through"),

    C("G4", "G", "VA",
      "上次维护已完成。下次例行维护将于 2026-09-25 10:00 - 12:00 (GMT+8) 进行。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note="a completion AND the next window in one message: the completion is "
           "scoped to its own sentence and the future window wins"),

    C("G5", "G", "Playtech",
      "The scheduled maintenance on 2026-09-23 has been cancelled.",
      "ignore",
      note="the true positive that the G1-G3 fixes must not break"),

    C("G6", "G", "TWSlot",
      "维护已取消",
      "ignore",
      note="true positive, Chinese, no window"),

    C("G7", "G", "JDB",
      "维护完成",
      "ignore",
      note="true positive, the shortest form there is"),

    C("G8", "G", "JILI",
      "排定维护 2026-09-23 10:00 - 12:00 (GMT+8)。维护期间所有优惠活动将取消。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the promos are cancelled, not the maintenance — the same guard as "
           "G2 with a different object noun"),

    C("G9", "G", "Yggdrasil",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8). Bets placed "
      "before the maintenance will be cancelled and refunded.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the English mirror of G3 with 'bets' as the subject"),

    C("G10", "G", "PG Soft",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8). We will inform "
      "you as soon as the maintenance has been completed.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="'as soon as ... has been completed' — the same future frame as G1, "
           "this time naming the maintenance outright"),

    C("G11", "G", "SimplePlay",
      "上次维护已完成，感谢配合。",
      "ignore",
      note="a completion with no future window anywhere: still ignore, which is "
           "what keeps the G4 fix honest"),

    C("G12", "G", "Playstar",
      "The maintenance scheduled for 2026-09-23 10:00 - 12:00 (GMT+8) has been "
      "cancelled.",
      "ignore",
      note="a cancellation with the whole window between the noun and the verb: "
           "the widened gap must keep matching, or this is written as live"),

    C("G13", "G", "Dropball",
      "维护延后至2026-09-25 10:00 - 12:00 (GMT+8)，原定2026-09-23的维护取消。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True,
      note="a postponement that cancels the OLD date in the same breath. Today "
           "维护取消 matches and the whole thing is dropped — the provider is "
           "down on the 25th and the row says nothing"),

    C("G14", "G", "EEZE",
      "本次维护已顺利完成，下周的维护时间另行通知。",
      "ignore",
      note="completed, with a vague promise and no window: nothing to write"),

    # -- H. RESCHEDULE ------------------------------------------------------

    C("H1", "H", "Hacksaw",
      "Please note the scheduled maintenance has been moved from 2026-09-23 to "
      "2026-09-25, 10:00-12:00 (GMT+8).",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True,
      note="'moved from X to Y' is a reschedule: the NEW date is written and the "
           "card must say so, because the Laboratory group reads the label to "
           "know the row changed"),

    C("H2", "H", "KingMidas",
      M("""
        維護時間變更
        更新後時間：2026-09-25 10:00 - 12:00 (GMT+8)
        原定時間：2026-09-23 10:00 - 12:00 (GMT+8)
      """),
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True,
      note="updated block with the superseded line beneath it, traditional "
           "characters throughout"),

    C("H3", "H", "VA",
      "【维护通知】周年庆活动延期至下月举行。系统维护时间：2026-09-23 10:00 - 12:00 (GMT+8)。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      resched=False,
      note="the 延期 belongs to the PROMO. The window is right today but the "
           "reschedule flag is a lie, and the card then tells the group a row "
           "changed when it did not"),

    C("H4", "H", "VA",
      "原定的周年庆活动延期。系统维护时间：2026-09-23 10:00 - 12:00 (GMT+8)。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      resched=False,
      note="the same unrelated 延期 plus an unrelated 原定: today the pair "
           "returns needs_human and NOTHING is written, which is the worst "
           "outcome of the three"),

    C("H5", "H", "Evolution",
      "Rescheduled: the maintenance will now take place on 2026-09-25 10:00 - "
      "12:00 (GMT+8) instead of 2026-09-23.",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True,
      note="the new date sits before the range and the superseded one after it; "
           "nearest-date-before is what keeps this right"),

    C("H6", "H", "JDB",
      "时间变更：新时间 2026-09-25 10:00 - 12:00 (GMT+8)，原时间 2026-09-23 10:00 - 12:00。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True,
      note="both windows on ONE line, so the line-dropping rule cannot help and "
           "only the ordering saves it — a standing hazard worth pinning"),

    C("H7", "H", "Playtech",
      M("""
        维护通知：时间变更
        更新后时间：
        2026-09-25 10:00 - 12:00 (GMT+8)
        原定时间：
        2026-09-23 10:00 - 12:00 (GMT+8)
      """),
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True,
      note="the 原定 label and its window are on DIFFERENT lines, so dropping "
           "the labelled line leaves the superseded window in scope and only "
           "document order keeps the answer right. It passes today — pinned so "
           "that a rewrite of the updated-block scoping cannot quietly start "
           "writing the withdrawn date"),

    C("H8", "H", "Yggdrasil",
      "Postponed: new window to be advised.",
      "needs_human", resched=True,
      note="a reschedule with no window at all — never guess, never write. The "
           "flag is pinned True because the two needs_human paths disagree "
           "today: the superseded-window path (A18) sets reschedule, this one "
           "silently drops it, so the card cannot say why a human is needed"),

    C("H9", "H", "TWSlot",
      "改期通知：维护改至2026-09-25 10:00 - 12:00 (GMT+8)。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True,
      note="改期 with the new window inline"),

    C("H10", "H", "KingMidas",
      "改期通知：维护改至2026年9月25日 10:00 - 12:00 (GMT+8)。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True,
      note="the same reschedule with a 年月日 date: two gaps compounding, and "
           "today it returns needs_human because the new window is unreadable"),

    # -- I. WINDOW SELECTION ------------------------------------------------

    C("I1", "I", "VA",
      M("""
        上周的维护窗口为 02:00 - 04:00。
        本次排定维护：2026-09-23 10:00 - 12:00 (GMT+8)。
      """),
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="finding #6: the first range in the text is a PAST window mentioned "
           "in passing, and today it wins and borrows the operative date — "
           "02:00-04:00 on the 23rd, a window that was never announced"),

    C("I2", "I", "KingMidas",
      M("""
        維護公告
        發佈日期：2026-09-20
        維護日期：2026年9月23日
        時間：10:00 - 12:00 (GMT+8)
      """),
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="finding #33: a letterhead date the parser CAN read above a window "
           "date it cannot. Today the row gets the 20th — a confident, "
           "plausible, entirely wrong window, which is the worst failure mode "
           "in this whole corpus"),

    C("I3", "I", "Hacksaw",
      M("""
        Scheduled maintenance notice
        Issued: 2026-09-20
        Maintenance date: 2026-09-23
        Time: 10:00 - 12:00 (GMT+8)
      """),
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the same letterhead shape with both dates readable: nearest date "
           "before the range already wins, and must keep winning"),

    C("I4", "I", "JDB",
      M("""
        提醒：上次维护窗口 02:00 - 04:00。
        排定维护
        日期：2026-09-23
        时间：10:00 - 12:00 (GMT+8)
      """),
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the past window sits above the operative block and has no date of "
           "its own, so it inherits one and outranks the real window"),

    C("I5", "I", "Evolution",
      "Our last window ran 02:00-04:00 without issue. The upcoming scheduled "
      "maintenance is 10:00-12:00 on 2026-09-23 (GMT+8).",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the English form of I1, on a single line, where 'last' and "
           "'upcoming' are the only things distinguishing the two ranges"),

    C("I6", "I", "Playtech",
      M("""
        Scheduled maintenance 2026-09-23 (GMT+8)
        Slots: 10:00 - 12:00
        Live Casino: 14:00 - 16:00
      """),
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note="two windows on the SAME day for two product lines. The row answers "
           "one question — when is this provider down — so it takes the union; "
           "writing only 10:00-12:00 tells the duty engineer the 15:00 outage "
           "is a live incident"),

    C("I7", "I", "PG Soft",
      M("""
        Scheduled maintenance (GMT+8)
        2026-09-23 10:00 - 12:00
        2026-09-30 10:00 - 12:00
      """),
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="two windows a week apart are two separate outages, not one long "
           "one: the row takes the nearer future one and the later notice "
           "arrives on its own tick"),

    C("I8", "I", "KingMidas",
      "排定维护 2026-09-16 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-16T10:00:00+08:00", "2026-09-16T12:00:00+08:00",
      stale=True,
      note="exactly what the KingMidas row holds right now. The parser is right "
           "to read it; stale is what tells vawatch not to write it over a live "
           "row, and it is the flag that must never be lost"),

    C("I9", "I", "SimplePlay",
      "排定维护，时间 10:00 - 12:00 (GMT+8)，敬请留意。",
      "ignore",
      note="a range with no date ANYWHERE. Guessing the day is how a row ends "
           "up with a window in the wrong week, so nothing is written and, per "
           "policy 9, the action stays ignore"),

    # -- J. THE FIX ROUND ---------------------------------------------------
    # Every case here is a defect three independent reviewers found AFTER this
    # corpus first went green — i.e. a real bug the other 151 cases did not
    # catch. They are pinned so the widenings above can never bring them back.

    # A sentence that DENIES an outage must not open the wording gate. The
    # widened gate carried "service interruption" / 停止服务 / "planned downtime"
    # as bare keywords, so 432 of 432 reassurance messages wrote a live window.
    C("J1", "F", "Playstar",
      "New game launch 2026-10-14 10:00 - 12:00 GMT+8. "
      "There will be no service interruption.", "ignore",
      note="negated outage: the gate phrase is the thing the sentence denies"),
    C("J2", "F", "BNG",
      "Version rollout 2026-09-24 10:00 - 11:00 (GMT+8). There will be no "
      "service interruption and no planned downtime.", "ignore",
      note="both gate phrases negated in one sentence"),
    C("J3", "F", "CQ9",
      "新版本上线：2026年9月24日 10:00-11:00 (GMT+8)，不会停止服务。", "ignore",
      note="不会 negates 停止服务 — and 无法 must NOT read as a negated 无"),
    C("J4", "F", "JDB",
      "新版本上线：2026年9月24日 10:00-11:00 (GMT+8)，无服务中断。", "ignore",
      note="无 negates 服务中断"),

    # A QUESTION about a window is not an announcement of one, and the group
    # carries our own weekly ask as well as the provider's replies.
    C("J5", "F", "KingMidas",
      "请问贵司下次的维护时间是 2026年9月24日 10:00-12:00 (GMT+8) 吗？", "ignore",
      note="吗？ — this is our side asking, not the provider telling"),
    C("J6", "F", "VP",
      "Hi team, could you confirm your maintenance window on "
      "2026-09-24 01:00 - 02:00 (GMT+8)?", "ignore",
      note="a request to confirm, quoting a window we proposed"),

    # The dotted-clock rule read money, versions, durations and latencies as
    # clocks and was then UNIONed with the real window, so a two-hour outage
    # went onto the row as an eleven-hour one.
    C("J7", "C", "Yellow Bat",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 (GMT+8). "
      "Min bet is 1.00 - 5.00 USD.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="a price list may not widen the window"),
    C("J8", "C", "OMNIPLAY",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 (GMT+8). "
      "Expect 2.00 - 4.00 hours of impact.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="a duration estimate may not widen the window"),
    C("J9", "C", "FC",
      "系统维护 2026-09-24 10:00 - 12:00 (GMT+8)。投注额 1.00 - 5.00 美元。",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="the zh half of the same defect"),
    C("J10", "C", "EEZE Slot",
      "Scheduled maintenance 2026-09-24 10.00-12.00 (GMT+8)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="the genuine dotted form still parses — it is a fallback, not a ban"),

    # 24:00 paired with midnight is the only clock pair that can produce
    # Start Time == End Time, which flips the sheet to "Yes" for no downtime.
    C("J11", "C", "5G",
      "Scheduled maintenance 2026-09-23 24:00 - 00:00 (GMT+8)", "ignore",
      note="a zero-length window is refused, not repaired"),

    # NFKC folded a circled list bullet into a digit and destroyed the date.
    C("J12", "B", "SimplePlay",
      "维护公告\n①2026-09-23 10:00-12:00 (GMT+8) 系统维护",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the fold is for full-width forms only; a circled bullet is decoration"),

    # A bare ratio is not a date. The old guard accepted any nearby clock, and
    # every message that reaches the write path has one.
    C("J13", "F", "JILI",
      "Scheduled maintenance, phase 1/2, 10:00-12:00 GMT+8.", "ignore",
      note="'phase' vetoes the ratio — 1 February 2027 was being written"),
    C("J14", "F", "YGR",
      "Server maintenance on 3/4 of our nodes, 10:00-12:00 GMT+8.", "ignore",
      note="'of' after the pair makes it a share, even with 'on' in front"),
    C("J15", "B", "KingMidas",
      "维护公告 将于9/23 (三) 10:00 - 12:00 (GMT+8) 排定维护",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="the shape the live group uses must survive the ratio guard"),

    # A bare from->to flipped an ordinary notice to reschedule=True, and the
    # Laboratory card then claimed the row had changed when it had not.
    C("J16", "H", "PP",
      "Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). The build was "
      "moved from staging to production yesterday.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      resched=False,
      note="'staging' and 'production' are not dates, so this is no reschedule"),


    # The CJK label colon sat in the lookbehind of both the bare-hour and the
    # dotted rule, so the label style the live KingMidas notice uses produced
    # no window while the identical ASCII "Time: ..." parsed.
    C("J17", "C", "KingMidas",
      "Scheduled maintenance\n日期Date：2026-09-23\n时间Time：10AM - 12PM (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="bare-hour time directly after a full-width label colon"),
    C("J18", "C", "JDB",
      "Scheduled maintenance\n日期Date：2026-09-23\n时间Time：10.00-12.00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="dotted time directly after a full-width label colon"),

    # Sentence-scoping alone let ANY unrelated range rescue a cancellation.
    C("J19", "G", "Playstar",
      "New game launch 2026-10-14 10:00 - 12:00 GMT+8. "
      "The scheduled maintenance has been cancelled.", "ignore",
      note="the surviving window's own sentence must itself be about maintenance"),


    # -- K. WINDOW CHOICE, AUDIT ROUND 2 -----------------------------------
    # The 24h union and the "any date will do" fallback wrote windows nobody
    # announced. Each case is a finding from the second audit (F<n>), pinned
    # at the outcome policy 5 now states. Where a later lane may legitimately
    # upgrade a needs_human to a fill (it learns to read a new shape), the
    # note says so and says what the fill must NEVER be.

    # F2 - a letterhead / unrelated date above the Time line was lent to it.
    C("K-F2a", "K", "Hacksaw",
      M("""
        Scheduled maintenance notice
        Issued: 2026-09-22
        Time: 22:00 - 02:00 (GMT+8)
        Date: 2026-09-24 (Thu)
      """),
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      now="2026-09-22T08:00:00+08:00",
      note="F2: the letterhead is never lent; the Date line BELOW a Time line "
           "dates it. Was 09-22 22:00, two days early and not stale"),
    C("K-F2b", "K", "KingMidas",
      M("""
        維護公告
        發佈日期：2026-09-22
        維護時間：22:00 - 02:00 (GMT+8)
        維護日期：2026年9月24日
      """),
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      now="2026-09-22T08:00:00+08:00",
      note="F2: the same shape in traditional Chinese"),
    C("K-F2c", "K", "Hacksaw",
      M("""
        Scheduled maintenance notice
        Issued: 2026-09-22
        Time: 22:00 - 02:00 (GMT+8)
        Maintenance date: to be confirmed
      """),
      "ignore", now="2026-09-22T08:00:00+08:00",
      note="F2: the letterhead is the ONLY date - no window, never 09-22"),
    C("K-F2d", "K", "PP",
      M("""
        ---------- Forwarded message ---------
        From: Ops <ops@example.com>
        Date: Tue, Sep 22, 2026 at 3:15 PM
        Subject: Maintenance

        Scheduled maintenance on Thursday from 22:00 to 02:00 (GMT+8).
      """),
      "ignore", now="2026-09-22T08:00:00+08:00",
      note="F2: a forwarded mail header is not the maintenance date. If "
           "relative days become readable the answer is Thursday 09-24, "
           "never Tuesday 09-22"),
    C("K-F2e", "K", "JILI",
      "New game Dragon Fortune launches on 2026-09-25!\n"
      "Scheduled maintenance 10:00-12:00 (GMT+8).",
      "ignore",
      note="F2: a date in an unrelated sentence does not date the window"),
    C("K-F2f", "K", "Hacksaw",
      "Issued: 2026-09-23. Scheduled maintenance 10:00 - 12:00 (GMT+8) on "
      "2026-09-25.",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note="F2: letterhead is decided per DATE (its own label), not per line; "
           "the 25th on the same line is the window's"),

    # F3 - a range with no date of its own took any date-shaped token.
    C("K-F3a", "K", "KingMidas",
      "Scheduled maintenance 10:00 - 12:00 (GMT+8).\n"
      "Note: the Mid-Autumn promotion runs until 2026-09-30.",
      "ignore", note="F3: a promo end date is not the window's day"),
    C("K-F3b", "K", "KingMidas",
      M("""
        Dear partners,
        Scheduled maintenance 10:00 - 12:00 (GMT+8).
        Thank you.
        KingMidas Team
        2026/09/22
      """),
      "ignore", note="F3: a sign-off date is not the window's day"),
    C("K-F3c", "K", "Hacksaw",
      "Scheduled maintenance 10:00 - 12:00 (GMT+8) to deploy release 2026.10.1",
      "ignore", note="F3: CalVer after 'release' is not a date"),
    C("K-F3d", "K", "Hacksaw",
      "Scheduled maintenance 10:00 - 12:00 (GMT+8). Ticket: MNT2026-10-08",
      "ignore", note="F3: a ticket id is not a date"),
    C("K-F3e", "K", "Hacksaw",
      "[MNT2026-10-08] Scheduled maintenance 10:00 - 12:00 (GMT+8)",
      "ignore", note="F3: nor when it sits in front of the range"),
    C("K-F3f", "K", "Hacksaw",
      "Scheduled maintenance 10:00 - 12:00 (GMT+8). Details: "
      "https://status.example.com/incident?date=2026-10-01",
      "ignore", note="F3: a URL parameter is not a date"),
    C("K-F3g", "K", "Yggdrasil",
      "Scheduled maintenance\nTime: 22:00 - 02:00 (GMT+8)\nDate: 24/09/2026",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F3 load-bearing: Time ABOVE Date still fills (the next line is a "
           "labelled date line)"),
    C("K-F3h", "K", "JDB",
      "维护公告\n维护时间：10:00-12:00\n维护日期：2026年9月24日",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F3 load-bearing: the Chinese Time-above-Date layout"),
    C("K-F3i", "K", "VA",
      "Scheduled maintenance\n10:00 - 12:00 (GMT+8)\n2026-09-24 (Thu)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F3 load-bearing: a bare range line over a bare date line"),
    C("K-F3j", "K", "SimplePlay",
      "排定维护，时间 10:00 - 12:00 (GMT+8)，敬请留意。\n2026-09-22",
      "needs_human",
      note="F3: a prose window over a bare date - the date may be a sign-off "
           "(it was written as 09-22); nothing is written, a human decides"),

    # F4 - the nearest same-line date BEFORE beat the "on <date>" after.
    C("K-F4a", "K", "PG Soft",
      "Because of the National Day holiday (2026-10-01), this week's scheduled "
      "maintenance will be held 10:00 - 12:00 (GMT+8) on 2026-09-30.",
      "fill", "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      note="F4: the date introduced after the range governs it (was 10-01)"),
    C("K-F4b", "K", "JDB",
      "因国庆节(10月1日)放假，本周例行维护时间为 10:00-12:00 (GMT+8)，日期：9月30日。",
      "fill", "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      note="F4: the same with 日期： (was 10-01)"),
    C("K-F4c", "K", "Evolution",
      "Following the 2026-09-16 maintenance, the next scheduled maintenance is "
      "10:00 - 12:00 (GMT+8) on 2026-09-24.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F4: last time's date in front is not this window's (was 09-16, "
           "stale, silently dropped)"),
    C("K-F4d", "K", "Evolution",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8) on 2026-09-24.",
      "needs_human",
      note="F4: two dates written against one range disagree - ambiguous"),

    # F6 - the bare n/m date was switched off by any other date in the bubble.
    C("K-F6a", "K", "KingMidas",
      "维护公告 将于9/24 (四) 10:00 - 12:00 (GMT+8) 排定维护。国庆节(10月1日)期间客服正常。",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F6: a holiday date elsewhere no longer disables 9/24 (was 10-01)"),
    C("K-F6b", "K", "KingMidas",
      "Scheduled maintenance on 9/24 (Thu) 10:00-12:00 GMT+8. The next one is "
      "planned for 2026-10-15.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F6: next week's full date no longer wins (was 10-15)"),
    C("K-F6c", "K", "KingMidas",
      "發佈日期：2026-09-22\n维护公告\n将于9/24 (四) 22:00-02:00 (GMT+8) 进行例行维护",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      now="2026-09-22T08:00:00+08:00",
      note="F6: nor does the letterhead (was 09-22 22:00)"),
    C("K-F6d", "K", "KingMidas",
      "国庆节(10月1日-10月7日)期间，本周例行维护将于9/30 (三) 10:00-12:00 (GMT+8) 进行。",
      "fill", "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      note="F6: the holiday span's end date no longer wins (was 10-07)"),

    # F9 - two zones on ONE line: every range took the line's first token.
    C("K-F9a", "K", "Playtech",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 GMT+8 (02:00 - 04:00 UTC).",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F9: each range takes the zone written after it (was 02:00-12:00)"),
    C("K-F9b", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-23, 06:00-08:00 UTC / 14:00-16:00 GMT+8",
      "fill", "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note="F9: foreign zone first (was 14:00 -> 09-24 00:00)"),
    C("K-F9c", "K", "KingMidas",
      "【维护通知】2026年9月23日 北京时间 10:00-12:00（UTC 02:00-04:00）进行系统维护",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F9: prefix-style zones take the token in FRONT of each range"),
    C("K-F9d", "K", "Playtech",
      "Scheduled maintenance 2026-09-23 (GMT+8) 10:00 - 12:00 / 02:00 - 04:00 UTC",
      "needs_human",
      note="F9: a line mixing prefix and suffix zones cannot be bound with "
           "confidence; its two copies do not agree, so a human decides"),

    # F10 - a converted-time line took the local date and landed 24h away.
    C("K-F10a", "K", "PG Soft",
      M("""
        Scheduled maintenance
        Date: 2026-09-23
        Time: 01:00 - 03:00 (GMT+8)
        UTC: 17:00 - 19:00
      """),
      "fill", "2026-09-23T01:00:00+08:00", "2026-09-23T03:00:00+08:00",
      note="F10: the UTC copy is the SAME instant, not a window 24h later (was "
           "a 26h union)"),
    C("K-F10b", "K", "PG Soft",
      M("""
        Scheduled maintenance
        Date: 2026-09-23
        Time: 01:00 - 03:00 (GMT+8)
        UTC: 17:00 - 19:00
      """),
      "fill", "2026-09-23T01:00:00+08:00", "2026-09-23T03:00:00+08:00",
      now="2026-09-23T04:00:00+08:00", stale=True,
      note="F10: read after the window, it is stale - not a phantom 09-24 "
           "01:00-03:00 that the stale guard cannot see"),
    C("K-F10c", "K", "BNG",
      "Scheduled maintenance 2026-09-23 06:00 - 08:00 (GMT+8) / 22:00 - 00:00 (UTC)",
      "fill", "2026-09-23T06:00:00+08:00", "2026-09-23T08:00:00+08:00",
      note="F10: the same-line conversion across midnight (was 18h)"),

    # F15 / F31 - unrelated ranges replaced or widened the window.
    C("K-F15a", "K", "EEZE Slot",
      M("""
        Scheduled maintenance
        Date: 23.09.2026
        Time: 10.00 - 12.00 (GMT+8)
        Support desk hours: 09:00 - 18:00
      """),
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F15: the dotted window survives a colon footer, and desk hours are "
           "not the outage (was 09:00-18:00)"),
    C("K-F15b", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8). Please stop "
      "placing bets from 09:45 - 10:00.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F15: a betting cut-off is not the outage (was 09:45-12:00)"),
    C("K-F31a", "K", "Yggdrasil",
      "Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\n"
      "Our customer service team is available 09:00-21:00 daily.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F31: support hours (was 09:00-21:00)"),
    C("K-F31b", "K", "Evolution",
      "Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8). Unsettled bets "
      "will be settled between 12:00-18:00.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F31: settlement (was 10:00-18:00)"),
    C("K-F31c", "K", "FC",
      "系统维护通知\n维护时间：2026-09-23 10:00-12:00 (GMT+8)\n充值通道将于 2026-09-22 20:00-23:59 暂停。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F31: a deposit-channel pause the evening before (was 09-22 20:00)"),
    C("K-F31d", "K", "Playtech",
      "Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8).\n"
      "Backoffice will be unavailable on 2026-09-22 22:00-23:00 for data migration.",
      "needs_human",
      note="F31: the earliest window is only DESCRIBED (a subsystem is "
           "unavailable) and the named maintenance is a separate later one - "
           "which the row carries is a human's call (was 09-22 22:00 -> "
           "09-23 12:00)"),
    C("K-F31e", "K", "Playtech",
      "Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8).\n"
      "API will be unavailable 12:00-12:30.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:30:00+08:00",
      note="F31 control: a genuine piece of the same outage, touching it, is "
           "still unioned"),

    # F32 - a cross-day promo / holiday period won and outlived the stale guard.
    C("K-F32a", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\n"
      "Note: the Mid-Autumn promotion (2026-09-20 00:00 - 2026-09-30 23:59) "
      "is not affected.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F32: the promotion is not the outage (was a ten-day window)"),
    C("K-F32b", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-21 10:00-12:00 (GMT+8).\n"
      "Note: the Mid-Autumn promotion (2026-09-20 00:00 - 2026-09-30 23:59) "
      "is not affected.",
      "fill", "2026-09-21T10:00:00+08:00", "2026-09-21T12:00:00+08:00",
      stale=True,
      note="F32: a re-quoted old notice is stale again - the promo no longer "
           "defeats the stale guard"),
    C("K-F32c", "K", "JDB",
      "国庆假期期间（2026-10-01 00:00 - 2026-10-07 23:59）暂停例行维护，"
      "最后一次例行维护为 2026-09-30 10:00-12:00 (GMT+8)。",
      "fill", "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      now="2026-09-30T13:00:00+08:00", stale=True,
      note="F32: read after the last window, the Golden Week pause is still "
           "not written as a seven-day outage"),
    C("K-F32d", "K", "Evolution",
      "Scheduled maintenance 2026-09-23 00:00 - 2026-09-26 00:00 (GMT+8)",
      "needs_human",
      note="F32 backstop: a 72h window exceeds NOTICE_MAX_WINDOW_HOURS (48)"),
    C("K-F32e", "K", "Evolution",
      "Scheduled maintenance 2026-09-23 00:00 - 2026-09-26 00:00 (GMT+8)",
      "fill", "2026-09-23T00:00:00+08:00", "2026-09-26T00:00:00+08:00",
      env={"NOTICE_MAX_WINDOW_HOURS": "0"},
      note="F32: NOTICE_MAX_WINDOW_HOURS=0 switches the cap off"),

    # F33 - separate days were joined into one 21-26h window.
    C("K-F33a", "K", "PG Soft",
      "Scheduled maintenance (GMT+8)\n2026-09-23 02:00 - 04:00\n2026-09-24 02:00 - 04:00",
      "fill", "2026-09-23T02:00:00+08:00", "2026-09-23T04:00:00+08:00",
      others=[("2026-09-24T02:00:00+08:00", "2026-09-24T04:00:00+08:00")],
      note="F33: two nights are two outages; the second is named in others "
           "(was 09-23 02:00 -> 09-24 04:00)"),
    C("K-F33b", "K", "Playtech",
      "Scheduled maintenance (GMT+8)\n| Slots | 2026-09-23 | 10:00-12:00 |\n"
      "| Fishing | 2026-09-24 | 09:00-10:00 |",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      others=[("2026-09-24T09:00:00+08:00", "2026-09-24T10:00:00+08:00")],
      note="F33: a table across two days (was a 24h window)"),
    C("K-F33c", "K", "Playtech",
      "Scheduled maintenance on 2026-09-23 (GMT+8)\nSlots: 23:00 - 01:00\n"
      "Live Casino: 00:30 - 02:00",
      "needs_human",
      note="F33: a row after an overnight row under one date could be either "
           "day - never the 24.5h union"),
    C("K-F33d", "K", "PG Soft",
      "Scheduled maintenance (GMT+8)\n2026-09-23 02:00 - 04:00\n2026-09-24 02:00 - 04:00",
      "fill", "2026-09-23T02:00:00+08:00", "2026-09-23T04:00:00+08:00",
      env={"NOTICE_WINDOW_PICK": "first"},
      note="F33: NOTICE_WINDOW_PICK=first still takes the first range"),

    # F67 - a second window more than 24h later was dropped without a word.
    C("K-F67", "K", "Hacksaw",
      "Scheduled maintenance (GMT+8)\nPhase 1: 2026-09-23 22:00 - 23:59\n"
      "Phase 2: 2026-09-25 00:00 - 02:00",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-23T23:59:00+08:00",
      others=[("2026-09-25T00:00:00+08:00", "2026-09-25T02:00:00+08:00")],
      note="F67: Phase 1 fills and Phase 2 is returned in others (and named "
           "in reason) so the caller can card or re-act on it"),

    # Guards that keep "separate windows are never merged" from turning the
    # old superset into a WRONG narrower window where the parse is unsure.
    C("K-G1", "K", "Hacksaw",
      "The maintenance scheduled for 2026-09-23 10:00-12:00 (GMT+8) has been "
      "postponed to 2026-09-24 10:00-12:00 (GMT+8).",
      "needs_human", resched=True,
      note="guard: a reschedule stating two windows with neither marked old. "
           "May become a fill of the NEW 09-24 window once 'postponed to' is "
           "understood; never the withdrawn 09-23, never a union"),
    C("K-G2", "K", "Yggdrasil",
      "Correction: scheduled maintenance is 25/09/2026 10:00-12:00 (GMT+8), "
      "NOT 24/09/2026 10:00-12:00.",
      "needs_human",
      note="guard: a correction stating the wrong window beside the right one"),
    C("K-G3", "K", "Yggdrasil",
      "Scheduled maintenance 2026-09-23 04:00 - 06:00 CEST (10:00 - 12:00 GMT+8)",
      "needs_human",
      note="guard: a zone the table does not know - its range was read as "
           "+08 and would be written as a 04:00 window. May become a fill once "
           "CEST is in the table; never 04:00-06:00 +08"),
    C("K-G4", "K", "BNG",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 Vietnam time (GMT+7)",
      "fill", "2026-09-23T10:00:00+07:00", "2026-09-23T12:00:00+07:00",
      note="guard control: an unknown zone NAME followed by a known offset "
           "is not unknown"),
    C("K-G5", "K", "KingMidas",
      M("""
        维护公告 Maintenance Notice
        维护时间：2026年9月23日 下午2:00-4:00 (GMT+8)
        Maintenance time: Sep 23, 2026 2:00PM-4:00PM (GMT+8)
      """),
      "needs_human",
      note="guard: the same window twice, 12h apart - one copy lost its "
           "下午. May become 14:00-16:00 once 下午 is read; never 02:00-04:00"),
    C("K-G6", "K", "Yggdrasil",
      "Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\n"
      "Our customer service team is available 09:00-21:00 daily.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      env={"NOTICE_WINDOW_PICK": "first"},
      note="the relevance rule applies under NOTICE_WINDOW_PICK=first too"),

]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def _dt(s):
    return None if s is None else datetime.fromisoformat(s)


def check(case, strict_offset=False):
    """-> [] when the case passes, else a list of one-line mismatches."""
    saved = {k: os.environ.get(k) for k in case.get("env", {})}
    os.environ.update(case.get("env", {}))
    try:
        v = noticeparse.classify(case["text"], now=_dt(case["now"]))
    except Exception as err:                        # noqa: BLE001
        # A raise is a failure, never a crash of the suite: 24:00 already
        # raises ValueError inside the parser today and a corpus that dies on
        # the first one tells you nothing about the other 150.
        return ["classify() RAISED {!r}".format(err)]
    finally:
        for k, old in saved.items():
            if old is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = old

    bad = []
    got_action = v.get("action")
    if got_action != case["action"]:
        bad.append("action={!r} expected {!r}  ({})".format(
            got_action, case["action"], v.get("reason") or "no reason given"))

    for key in ("start", "end"):
        want, got = _dt(case[key]), v.get(key)
        if want is None and got is not None:
            bad.append("{}={} expected None".format(key, got))
        elif want is not None and got is None:
            bad.append("{}=None expected {}".format(key, want.isoformat()))
        elif want is not None and got is not None:
            if getattr(got, "tzinfo", None) is None:
                bad.append("{}={} is NAIVE; the Base write needs an aware "
                           "datetime".format(key, got))
                continue
            if got != want:
                bad.append("{}={} expected {}".format(
                    key, got.isoformat(), want.isoformat()))
            elif strict_offset and got.utcoffset() != want.utcoffset():
                bad.append("{} offset {} expected {} (same instant, different "
                           "wall clock on the card)".format(
                               key, got.utcoffset(), want.utcoffset()))

    if case["stale"] is not None and bool(v.get("stale")) != case["stale"]:
        bad.append("stale={} expected {}".format(
            bool(v.get("stale")), case["stale"]))
    if case["resched"] is not None and bool(v.get("reschedule")) != case["resched"]:
        bad.append("reschedule={} expected {}".format(
            bool(v.get("reschedule")), case["resched"]))
    if case.get("others") is not None:
        want = [(_dt(a), _dt(b)) for a, b in case["others"]]
        got = list(v.get("others") or [])
        if got != want:
            bad.append("others={} expected {}".format(
                [(a.isoformat(), b.isoformat()) for a, b in got],
                [(a.isoformat(), b.isoformat()) for a, b in want]))
    return bad


def _one_line(text, width=88):
    flat = " ".join(str(text).split())
    return flat if len(flat) <= width else flat[:width - 1] + "…"


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Regression corpus for noticeparse.classify()")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="print every case, not just the failures")
    ap.add_argument("--group", "-g", action="append", default=[],
                    help="only this group letter (repeatable)")
    ap.add_argument("--id", action="append", default=[],
                    help="only this case id (repeatable)")
    ap.add_argument("--strict-offset", action="store_true",
                    help="also require the UTC offset to match, not only the "
                         "instant")
    ap.add_argument("--policy", action="store_true",
                    help="print the ambiguity policy and exit")
    args = ap.parse_args(argv)

    if args.policy:
        print(POLICY)
        return 0

    groups = {g.strip().upper() for g in args.group}
    ids = {i.strip().upper() for i in args.id}
    cases = [c for c in CASES
             if (not groups or c["group"] in groups)
             and (not ids or c["id"].upper() in ids)]
    if not cases:
        print("no cases selected", file=sys.stderr)
        return 2

    tally = {}
    failures = []
    for c in cases:
        bad = check(c, strict_offset=args.strict_offset)
        t = tally.setdefault(c["group"], [0, 0])
        t[0] += 1
        if bad:
            t[1] += 1
            failures.append((c, bad))
        if args.verbose:
            print("{:>4}  {}  [{}] {}".format(
                c["id"], "PASS" if not bad else "FAIL", c["provider"],
                _one_line(c["text"])))
            if bad:
                for b in bad:
                    print("        -> {}".format(b))

    if failures and not args.verbose:
        print("FAILURES")
        print("=" * 78)
        for c, bad in failures:
            print("{}  [{}]  {}".format(c["id"], c["provider"],
                                        GROUPS[c["group"]]))
            print("    notice : {}".format(_one_line(c["text"])))
            print("    why    : {}".format(_one_line(c["note"], 200)))
            for b in bad:
                print("    got    : {}".format(b))
            print("")

    total = sum(t[0] for t in tally.values())
    failed = sum(t[1] for t in tally.values())
    print("=" * 78)
    print("{:<6}{:<52}{:>6}{:>6}{:>7}".format(
        "GRP", "WHAT IT COVERS", "CASES", "PASS", "FAIL"))
    print("-" * 78)
    for g in sorted(tally):
        n, f = tally[g]
        print("{:<6}{:<52}{:>6}{:>6}{:>7}".format(g, GROUPS[g][:50], n, n - f, f))
    print("-" * 78)
    print("{:<6}{:<52}{:>6}{:>6}{:>7}".format(
        "", "TOTAL", total, total - failed, failed))
    print("")
    print("{} of {} cases pass. Run --policy for the ambiguity rules the "
          "expected values follow.".format(total - failed, total))
    # The corpus must never be able to reach the Base or a group: nothing that
    # can send may even be LOADED by the parser it tests. A failure here means
    # noticeparse grew an import that can talk to the network.
    senders = sorted(m for m in _NETWORK_MODULES if m in sys.modules)
    print("Network-capable modules loaded: {}".format(", ".join(senders) or "none"))
    return 1 if (failed or senders) else 0


_NETWORK_MODULES = ("requests", "urllib3", "http.client", "vawatch", "providerask",
                    "providerllm", "telegramwarm", "groupcheck", "main")


if __name__ == "__main__":
    sys.exit(main())
