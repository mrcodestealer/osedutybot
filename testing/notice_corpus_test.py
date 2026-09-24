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
   A WEEKDAY written against the date outranks the flag: "10/05/2026 (Mon)" is
   5 October (a Monday) whatever the flag says, because 10 May 2026 is a
   Sunday. A weekday that fits neither reading, or a date whose weekday
   contradicts it on any form, is a typo the text cannot settle: needs_human.
   THE BARE "n/m" (no year) is the exception to the flag: its one live
   sender, KingMidas, writes it month-first ("9/23 (三)"), so under dmy
   "将于11/4 (三)" was 11 April 2027. When both parts are <= 12 the bracketed
   weekday decides when exactly one reading falls on it (4 Nov 2026 is a
   Wednesday); when both readings do ("10/5 (一)": 5 Oct 2026 and 10 May 2027
   are both Mondays), when neither does, or when there is no weekday, the
   verdict is needs_human - NOTICE_BARE_DATE_ORDER=ask (default) | mdy | dmy
   lets an operator who knows every sender's habit name the order instead.
   (K-F5*)

2. A DATE WITH NO YEAR — "9/23", "9月23日", "Sep 23" — NOTICE_YEARLESS=1
   The year is a pure function of (text, now). No clock is read, no timezone
   of the host matters, and the same message classified twice on the same
   `now` always gives the same year.
     candidates = the same (month, day) in now.year-1, now.year, now.year+1,
                  discarding any that is not a real calendar date
     keep       = candidates whose DATE falls in [now-120d, now+245d) -
                  HALF-OPEN (dates compared in the notice's own timezone)
     pick       = the earliest kept candidate on or after now's date;
                  if none is on or after, the latest kept candidate;
                  if nothing is kept, the date is UNUSABLE and the notice
                  yields no window (never a guessed year)
   The window is exactly 365 days wide on purpose: at most one of the three
   candidate years can ever qualify, so "prefer the future" is a tie-break that
   almost never fires and the rule cannot drift with the calendar. (It was
   closed at both ends - 366 days - so the one date exactly 120 days back had
   two candidates and went to NEXT year: "5月25日" read on 09-22 was written
   as 2027-05-25 instead of being dropped as stale. K-F73a.) The split is
   120 back / 245 forward because a provider re-posting a notice from last
   quarter is routine — and that window is then caught by is_stale and not
   written. The far end of the 245 forward is where an OLD notice lands (a
   re-post, a quoted reply, a ledger key evicted while its bubble is still on
   screen), so a year-less date inferred MORE than 120 days ahead is
   needs_human unless a weekday written against it confirms the year - the
   weekday of a fixed day-and-month repeats only every 5-11 years.
   Worked, with now = 2026-09-22:
     "9月23日"  -> 2026-09-23   (this week)
     "Sep 15"   -> 2026-09-15   (last week, stale, fill but not written)
     "1月5日"   -> 2027-01-05   (2026-01-05 is 260 days back, out of window)
     "5月25日"  -> 2026-05-25   (exactly 120 back: stale, not written)
     "5月20日"  -> needs_human  (2027-05-20 is 240 days ahead, no weekday)
     "1月25日 (一)" -> 2027-01-25 (125 ahead, and the Monday confirms it)
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

3b. CLOCKS (K-F11, K-F12, K-F13, K-F43, K-F44)
   Seconds are read and dropped: "10:00:00 - 12:00:00" is 10:00-12:00. The
   colon rule used to start INSIDE the first clock and read its minutes and
   seconds, so it was written 00:00-12:00.
   A meridiem on ONE end covers the other end too, unless that makes no
   sense: the bare end is read both as AM and as PM and the SHORTER window
   wins. The two readings are exactly 12h apart, so one is always under 12h:
   "8:00 - 11:00 PM" is 20:00-23:00, "9:00PM-11:00" 21:00-23:00,
   "11:00 - 1:00 AM" 23:00-01:00, while "10:00 - 12:00 PM" stays 10:00-12:00
   and "11:00 - 1:00 PM" 11:00-13:00. Dotted "p.m." counts, "12 midnight" /
   "12 MN" ends a window at 24:00, "12 noon" / "12 NN" is 12:00.
   The Chinese meridiem sits IN FRONT of the clock and means the same:
   凌晨 清晨 早上 早晨 上午 = AM, 下午 傍晚 = PM, 中午 = noon (12 is 12:00,
   1-5 are 13:00-17:00),
   晚上 晚间 夜间 夜里 = evening: 6-11 are 18:00-23:00, 12 is the midnight
   ENDING that evening, and 1-5 ("晚上1点") name a day the text does not
   state and are refused. "下午14:00" / "14:00 PM" just say it twice.
   "22:00 至 次日02:00": 次日 / 翌日 / 隔天 / "next day" puts the END on the
   day after the date, whatever the clocks say.
   到 / till / through / thru join two clocks like 至 and "until" do, and
   "between 10:00 and 12:00" is a range ("and" is a connector only there).
   Two datetimes may carry a zone on each end ("22:00 (GMT+8) - 2026-09-25
   02:00 (GMT+8)"), and a Start / 开始时间 line followed by an End / 结束时间
   line is one window, with a date on each line or on a Date line above.

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
     SGT MYT PHT HKT 北京时间 新加坡时间 香港时间 台北时间 = +08
     JST KST 日本时间 韩国时间 = +09 | ICT WIB 泰国时间 越南时间 = +07
     UTC GMT Z = +00 | EST = -05 | EDT = -04 | PDT = -07
     and the same places spelled out: "Philippine Standard Time",
     "Bangkok time", "Asia/Tokyo" ... (noticeparse._TZ_WORDS)
   EST is -05 flat: these notices are written by studios that mean "New York",
   and a parser that guesses EDT in summer would move a window by an hour with
   nothing on the page to justify it.
   "UTC/GMT+8", "GMT/UTC +8" are +08 - the bare UTC in front of the offset
   is not +00 (K-F8). A sign-less "(GMT 8)" / "(GMT 08:00)" is the offset
   with its "+" lost; "(UTC 02:00)" after a single clock may equally be that
   clock converted, so that one is needs_human.
   AMBIGUOUS ABBREVIATIONS (K-F14) — PST, CST, IST each name several zones.
   PST is Philippine Standard Time for this operator (and what tzdata itself
   prints for Asia/Manila); reading it as US Pacific put a window 16 hours
   late. Each is resolved by, in order: an explicit offset on the same line
   that equals one reading ("PST (GMT+8)" +08, "PST (UTC-8)" -08); else the
   reading equal to NOTICE_TZ's own offset (Asia/Manila: PST = CST = +08);
   else nothing - needs_human (IST under Asia/Manila: India, Irish or Israel).
   A line whose explicit offset matches no reading ("PST (GMT+9)") is
   needs_human too.
   An abbreviation is a zone only where a zone is written: against a clock,
   a date or a zone label ("Time zone:", "all times in"), or alone in
   brackets. "our ICT team" and an all-caps "EST. DURATION" are words; any
   other stray abbreviation is needs_human, never silently NOTICE_TZ.
   UNKNOWN ZONES (K-F16) — a zone the table does not know (CEST, CET, BST,
   "(Malta time)", 美国东部时间 ...) written after OR before a range, or on a
   "Time zone:" line, is needs_human. It is never replaced by NOTICE_TZ.
   The one exception is a CONVERSION COPY: "04:00 - 06:00 CEST (10:00 - 12:00
   GMT+8)" read at CEST's defined +02 is exactly the GMT+8 window, so it is
   that window restated and the notice fills. A "CET" that meant summer
   time matches nothing at +01 and still goes to a person.
   Before 2026-09 every named zone silently became +08 — right for SGT/PHT/MYT
   by accident, an hour wrong for JST, thirteen hours wrong for EST.

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
   A date that is one half of a DATE SPAN ("24/09/2026 - 25/09/2026",
   "9月24日至9月25日", "23-24 Sep 2026", "Sep 24 - 25, 2026") dates a range
   from the span's FIRST day, whichever half the range bound to, and the
   range must END on the span's last day: 22:00-02:00 across 24-25 is
   24 22:00 -> 25 02:00. A range that does not fit (10:00-12:00 across 24-25:
   every day, or one 26h window? one night across 24-26) is needs_human.
   The nearest-date rule used to bind the overnight range to the second half
   and wrote the whole window a day late (K-F1*). "moved from D1 to D2" is a
   move, not a span. A date LIST ("2026-09-24 and 2026-09-25", "24 & 25 Sep",
   "9月24日、25日") under ONE range is one window per listed day: the row
   takes the earliest still ahead and the rest go to `others`. Two or more
   different ranges under one list (Slots / Live) is needs_human - they may
   apply to every day or pair up day by day, and the text does not say.
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
   "will be unavailable", a tournament with a time range (group F), the
   support desk's holiday hours (客服暂停服务, "customer service will be
   unavailable", K-F19). Subject, not keyword.
   The PLAIN wordings - "Evolution maintenance <window>", "Maintenance Time:",
   "we will perform maintenance on", "there will be a maintenance", 游戏维护,
   维护：, 将于<window>维护 - count only where they GOVERN the window: a heading
   within three lines above it (only "Label: value" lines between), a label
   opening the window's own line, or a future verb phrase in the window's own
   sentence, with no negator in the prefix (K-G1.1*, K-G1.2*). A bare
   "maintenance" elsewhere in the bubble never opens the gate for a window
   whose own sentence or heading is about something else (K-F17*).
   Three things REFUSE a notice the gate opened, each with its own reason and
   classify()'s `refused` key (never "no scheduled-maintenance wording"):
     * a QUESTION - a sentence that ends in ?/？/吗, carries 请问, or opens with
       is there / do you / could you, AND carries maintenance wording or a
       clock. "Any questions, please contact us?" does not; a request to
       relay ("could you please inform your players") is not a question
       (K-F45*, K-G1.4*). NOTICE_QUESTION_GUARD=0 turns it off.
     * a promise of NO OUTAGE anywhere in the message - no downtime, "will not
       cause any service interruption", 无需停机, 不停服, 不影响正常游戏 (not
       when it follows the outage: "维护结束后游戏将正常运行"). With an outage
       ALSO stated it is a partial outage: needs_human (K-F18*, K-G1.7*).
       NOTICE_NEGATION_GUARD=0 turns this and the per-phrase negation off.
     * a TEST environment as the subject - UAT, staging, sandbox, 测试环境 -
       ignore; with production also named and not said to be unaffected,
       needs_human (K-F46*). NOTICE_NONPROD_GUARD=0 turns it off.
   Set NOTICE_WORDING=strict for the pre-2026-09 SCHED_RE, whole-message, with
   none of the plain shapes. strict with all three guards at 0 is exactly the
   67cf5f4 gate (K-F75*).

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
   How that is decided (K-F21*, K-F24*, K-F25*, K-F48*, K-F49*, K-G1.6*):
     * A window in the CLAUSE of a completion or cancellation is the one
       completed / withdrawn. It is removed before any window is chosen, so
       it never joins the row's union and never hides a live window.
     * A window anywhere else survives only when the text says the two are
       different maintenances: the done clause quotes its OWN, non-overlapping
       window ("Slots 10:00-11:00 cancelled; Live Casino 14:00-16:00 goes
       ahead"); the done clause is about a PAST maintenance (上次维护已完成,
       "last week's maintenance has been completed" - with or without a
       comma); or the window comes later and is introduced as the NEXT one
       (下次维护, "Next maintenance:"). Otherwise - a cancel headline over the
       ordinary 维护时间 line, "The above maintenance has been cancelled" under
       it - the notice is a cancellation and nothing is written.
     * Vocabulary: cancelled / called off / withdrawn / will not take place /
       no longer applies / "CANCELLED:" / "— CANCELLED" / 取消 / 将不进行 /
       不再进行 / 撤销, and has ended / ended early / is complete / is over /
       提前完成 (K-F21*, K-F65*). A negated verb (未完成, 不会取消) is not one.
     * The frame in front of the VERB counts too: "..., we will notify you if
       this maintenance is cancelled", 维护将于12:00结束, 预计…完成, 如维护取消,
       维护结束时间 (a noun) describe an upcoming window (K-F47*). "We would
       like to inform you that ... has been cancelled" reports one.
     * A noun that NAMES the maintenance (游戏维护, "Withdrawal system
       maintenance") does not disarm it; a noun joined to it by a preposition
       ("rounds before the maintenance", 注单在维护期间) does (K-F30*). ";" and
       an "unfinished/pending" noun phrase end the maintenance's clause
       (K-F60*).
   EXTENDED (K-F50*): "extended until 14:00" / 延长至14:00 / "by 2 hours" on a
   readable original window fills that window with the NEW end, reschedule
   =True (the next day when the clock is before the start). No original
   window, no readable end, two different ends, or an extension of more than
   12h: needs_human, reschedule=True. A full new range in the sentence is read
   by the ordinary rules. NOTICE_EXTENSION_FILL=0 sends every extension to a
   person.
   RETRACTED (K-G2.1*): a message that retracts or voids a notice ("please
   ignore the notice for <window>", "sent to the wrong group", 请忽略上面…的
   维护通知, 发错群, 【撤回】, …公告作废) and states a window is needs_human -
   never a fill of the withdrawn window. So is a notice under a void TAG that
   opens the message ("[VOID] …", "[VOID - wrong group] …", 【作废】…,
   （發錯群，請忽略）…), whether posted fresh or added by an edit (K-G2.1t*);
   a heading tag ("[Maintenance Notice]") is not one. A correction that states the right
   window is not a retraction. With no window it stays with the ordinary
   rules; vawatch cards it against the row it retracts (G2.3).

8. RESCHEDULE (group H)
   The reschedule verb must take the maintenance as its subject. A notice whose
   only 延期 is a postponed anniversary promo is an ordinary notice that
   happens to contain the word, and must fill its window with reschedule=False
   — today it is worse than wrong: paired with an unrelated 原定 it returns
   needs_human and nothing is written at all. "moved from X to Y" and 改期/延后
   are reschedules and must set reschedule=True, because the card's
   "(rescheduled)" label is how the Laboratory group knows the row changed.
   The NEW window is written, never the superseded one.

9. NO READABLE WINDOW
   A message with maintenance wording and NO date word at all ("Emergency
   maintenance 15:00-16:00 (GMT+8)") stays `ignore` with the "no parseable
   date/time window" reason: that is what a half-rendered Telegram bubble
   looks like, and vawatch re-reads exactly that reason before giving up.
   What vawatch does at its reparse cap is vawatch's decision, not this one's.
   The exception is a notice dated only RELATIVE to its post - today /
   tonight / tomorrow / now / a weekday / this|next <weekday> / 今天 / 今晚 /
   明天 / 即日 / 本周四 / 下周三 - with a clock in the same sentence. That is
   `needs_human`, and the reason names the word ("dated only 'tomorrow'").
   The scrape carries each bubble's clock but NOT its date, so resolving
   "tomorrow" against the read time is a guess that is a day off whenever the
   bubble is read on a later day than it was posted (a backlog, a /vacheck, a
   bubble from 23:50 read at 00:10) - and `ignore` was a silent drop of every
   "emergency maintenance today" notice. vawatch cards needs_human; nothing
   is written. A relative word beside a real date ("today (9月23日)") fills
   from the date, and a relative DAY word standing between a range and an
   earlier date stops the range borrowing that date (K-G1.3m/n).
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
      now=NOW, stale=None, resched=None, others=None, env=None, why=None,
      note=""):
    """One case. ``others`` pins classify()'s list of further windows the
    notice states but the row does not get, as [(start, end), ...] ISO pairs;
    ``env`` sets flags for this case only (noticeparse reads them per call);
    ``why`` is a substring the verdict's reason must contain - for a
    needs_human case, what the card tells the person is part of the contract
    (a relative-date card that does not name "tomorrow" leaves them guessing
    which word the parser refused)."""
    return {"id": cid, "group": group, "provider": provider, "text": text,
            "action": action, "start": start, "end": end, "now": now,
            "stale": stale, "resched": resched, "others": others,
            "env": env or {}, "why": why, "note": note}


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
      "needs_human", now="2026-09-22T08:00:00+08:00",
      why="'Thursday'",
      note="F2: a forwarded mail header is not the maintenance date - never "
           "Tuesday 09-22. G1.3: the notice is dated only 'Thursday' and the "
           "scrape carries no post date, so a person resolves the day (it "
           "was `ignore`, a silent drop, before relative days were carded)"),
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
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      resched=True,
      note="was a needs_human guard until 'postponed to' was understood (F22): "
           "09-23 is the SUBJECT of the move verb, so it is the withdrawn "
           "window and the NEW 09-24 is written - never 09-23, never a union"),
    C("K-G2", "K", "Yggdrasil",
      "Correction: scheduled maintenance is 25/09/2026 10:00-12:00 (GMT+8), "
      "NOT 24/09/2026 10:00-12:00.",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True,
      note="was a needs_human guard: 'NOT <window>' now marks the wrong half "
           "(G2.2), so the right one is written and overrides the row"),
    C("K-G3", "K", "Yggdrasil",
      "Scheduled maintenance 2026-09-23 04:00 - 06:00 CEST (10:00 - 12:00 GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="a zone the table does not know, beside its GMT+8 copy. Read at "
           "CEST's defined +02 the CEST range IS the GMT+8 window, so it is a "
           "copy and the notice fills (K-F16h); never 04:00-06:00 +08. See "
           "K-F16i for the CET that does not match and stays needs_human"),
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
      "fill", "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note="the same window in two languages. It was needs_human while the "
           "Chinese copy lost its 下午 and read 02:00-04:00; 下午 is read now "
           "(F13), both copies are one instant, and it fills. Never "
           "02:00-04:00, never 02:00-16:00"),
    C("K-G6", "K", "Yggdrasil",
      "Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\n"
      "Our customer service team is available 09:00-21:00 daily.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      env={"NOTICE_WINDOW_PICK": "first"},
      note="the relevance rule applies under NOTICE_WINDOW_PICK=first too"),

    # -- P2 "dates" (audit round 2) -------------------------------------------
    # F1: a date SPAN and one overnight range. The range used to bind to the
    # span's second half (the nearest preceding date) and the whole window was
    # written 24h late - never stale, carded "Base row updated".
    C("K-F1a", "K", "Yggdrasil",
      "Scheduled maintenance\nDate: 24/09/2026 - 25/09/2026\n"
      "Time: 22:00 - 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F1: the span's FIRST day is the start day (was 09-25 22:00)"),
    C("K-F1b", "K", "PP",
      "Scheduled maintenance\nDate: 2026-09-24 (Thu) ~ 2026-09-25 (Fri)\n"
      "Time: 22:00 - 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F1: ISO span with a weekday on each half and a '~' connector"),
    C("K-F1c", "K", "JDB",
      "系统维护通知\n日期：2026年9月24日至9月25日\n时间：22:00 - 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F1: 年月日 至 月日 span"),
    C("K-F1d", "K", "JILI",
      "系统维护通知\n维护日期：9月24日(四)-9月25日(五)\n"
      "维护时间：22:00 - 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F1: year-less 月日 span with weekdays"),
    C("K-F1e", "K", "Hacksaw",
      "Scheduled maintenance on 24/09/2026 - 25/09/2026, 22:00 - 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F1: the span and the range on one line"),
    C("K-F1f", "K", "Yggdrasil",
      "Scheduled maintenance\nDate: 23-24 Sep 2026\nTime: 23:00 - 01:00 (GMT+8)",
      "fill", "2026-09-23T23:00:00+08:00", "2026-09-24T01:00:00+08:00",
      note="F1: day-first shorthand span '23-24 Sep 2026'"),
    C("K-F1g", "K", "PG Soft",
      "Scheduled maintenance\nDate: Sep 24 - Sep 25, 2026\n"
      "Time: 22:00 - 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F1: month-word span, year only on the second half"),
    C("K-F1h", "K", "Playtech",
      "Scheduled maintenance\nDate: 24/09/2026 (Thu) to 25/09/2026 (Fri)\n"
      "Time: 22:00 - 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F1: 'to' connector"),
    C("K-F1i", "K", "Evolution",
      "Scheduled maintenance\nDate: 24th - 25th September 2026\n"
      "Time: 22:00 - 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F1: ordinal shorthand span"),
    C("K-F1j", "K", "Yggdrasil",
      "Scheduled maintenance\nDate: 24/09/2026 - 25/09/2026\n"
      "Time: 10:00 - 12:00 (GMT+8)",
      "needs_human", why="span",
      note="F1 guard: a same-day range across a two-day span - every day of "
           "the span, or one 26h window? The text does not say"),
    C("K-F1k", "K", "Yggdrasil",
      "Scheduled maintenance\nDate: 24/09/2026 - 26/09/2026\n"
      "Time: 22:00 - 02:00 (GMT+8)",
      "needs_human", why="span",
      note="F1 guard: one night cannot fill a three-day span"),
    C("K-F1l", "K", "Yggdrasil",
      "Scheduled maintenance\nDate: 24/09/2026 - 25/09/2026\n"
      "Time: 22:00 - 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      now="2026-09-26T09:00:00+08:00", stale=True,
      note="F1: read after the window ended it is STALE (never written); the "
           "day-late binding made it a 'future' window that beat the guard"),
    C("K-F1m", "K", "Hacksaw",
      "The maintenance has been rescheduled from 2026-09-23 to 2026-09-25, "
      "22:00 - 02:00 (GMT+8).",
      "fill", "2026-09-25T22:00:00+08:00", "2026-09-26T02:00:00+08:00",
      resched=True,
      note="F1 control: 'from D1 to D2' under a move verb is a MOVE, not a "
           "span - the new day is the second one"),
    C("K-F1n", "K", "Yggdrasil",
      "Scheduled maintenance\nDate: 24-25/09/2026\nTime: 22:00 - 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F1 narrowing: the numeric day-range shorthand had no date at all "
           "and the notice was dropped"),
    # The same nearest-date trap with a LIST instead of a span: one window
    # per listed day, the earliest ahead on the row, the rest in others.
    C("K-F1o", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-24 and 2026-09-25, 10:00 - 12:00 "
      "(GMT+8)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      others=[("2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00")],
      note="F1 sibling: a date list bound its range to the LAST day - the "
           "24th's outage never reached the row"),
    C("K-F1p", "K", "Evolution",
      "Scheduled maintenance on 24 & 25 Sep 2026, 22:00 - 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      others=[("2026-09-25T22:00:00+08:00", "2026-09-26T02:00:00+08:00")],
      note="F1 sibling: shorthand list, two nights"),
    C("K-F1q", "K", "JDB",
      "系统维护通知\n日期：9月24日、25日\n时间：10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      others=[("2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00")],
      note="F1 sibling: 、 list - the 25th used to vanish without a word"),
    C("K-F1r", "K", "Evolution",
      "Scheduled maintenance on 24 & 25 Sep 2026: Slots 10:00-12:00, Live "
      "14:00-16:00 (GMT+8)",
      "needs_human", why="which range falls on which day",
      note="F1 sibling guard: two ranges under two days may pair up "
           "('respectively') or apply to both"),
    C("K-F1s", "K", "Yggdrasil",
      "Scheduled maintenance\nDate: 2026-09-24 and 2026-09-25\n"
      "Time: 01:00 - 03:00 (GMT+8)\nUTC: 17:00 - 19:00",
      "fill", "2026-09-24T01:00:00+08:00", "2026-09-24T03:00:00+08:00",
      others=[("2026-09-25T01:00:00+08:00", "2026-09-25T03:00:00+08:00")],
      note="F1 sibling: a UTC copy that crosses midnight is its own day's "
           "copy, not two ranges and not a phantom 26th"),
    C("K-F1t", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-24 and 2026-09-25, 10:00 - 12:00 "
      "(GMT+8)",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      now="2026-09-24T13:00:00+08:00", others=[],
      note="F1 sibling: read after the first day's window, the second fills"),

    # F5: the bare "n/m" is written month-first by its one live sender
    # (KingMidas, "9/23 (三)"), but NOTICE_DATE_ORDER=dmy read "11/4 (三)" as
    # 11 April 2027. The bracketed weekday is decisive when exactly one
    # reading falls on it; otherwise a person decides.
    C("K-F5a", "K", "KingMidas",
      "维护公告 将于11/4 (三) 10:00 - 12:00 (GMT+8) 排定维护",
      "fill", "2026-11-04T10:00:00+08:00", "2026-11-04T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F5: 4 Nov 2026 is a Wednesday, 11 Apr 2027 a Sunday (was 04-11)"),
    C("K-F5b", "K", "KingMidas",
      "维护公告 将于12/2 (三) 10:00 - 12:00 (GMT+8) 排定维护",
      "fill", "2026-12-02T10:00:00+08:00", "2026-12-02T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F5: the weekday picks month-first (was 2027-02-12)"),
    C("K-F5c", "K", "KingMidas",
      "维护公告 将于10/7 (三) 10:00 - 12:00 (GMT+8) 排定维护",
      "fill", "2026-10-07T10:00:00+08:00", "2026-10-07T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F5: day-first read it as 10 July - STALE, a silent drop"),
    C("K-F5d", "K", "KingMidas",
      "排定维护 1/2 (六) 10:00 - 12:00 (GMT+8)",
      "fill", "2027-01-02T10:00:00+08:00", "2027-01-02T12:00:00+08:00",
      now="2026-12-30T09:00:00+08:00",
      note="F5: across the year end - 2 Jan 2027 is a Saturday (was 02-01)"),
    C("K-F5e", "K", "KingMidas",
      "维护公告 将于10/5 (一) 10:00 - 12:00 (GMT+8) 排定维护",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="both",
      note="F5: 5 Oct 2026 and 10 May 2027 are BOTH Mondays - the weekday "
           "cannot settle it, so no guess 7 months out"),
    C("K-F5f", "K", "KingMidas",
      "排定维护 10/12 10:00 - 12:00 (GMT+8)",
      "needs_human", now="2026-09-23T09:00:00+08:00",
      why="NOTICE_BARE_DATE_ORDER",
      note="F5: <=12 both ways and no weekday - was 10 December"),
    C("K-F5g", "K", "KingMidas",
      "排定维护 10/12 10:00 - 12:00 (GMT+8)",
      "fill", "2026-10-12T10:00:00+08:00", "2026-10-12T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      env={"NOTICE_BARE_DATE_ORDER": "mdy"},
      note="F5: an operator who knows the provider writes month-first says so"),
    C("K-F5h", "K", "KingMidas",
      "维护公告 将于9/24 (三) 10:00 - 12:00 (GMT+8) 排定维护",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="weekday",
      note="F5: 24 Sep 2026 is a Thursday - date or weekday is a typo"),
    C("K-F5i", "K", "Yggdrasil",
      "Scheduled maintenance on 10/05/2026 (Mon) 10:00 - 12:00 (GMT+8)",
      "fill", "2026-10-05T10:00:00+08:00", "2026-10-05T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F5: the weekday is checked on the year-bearing form too - 10 May "
           "2026 is a Sunday, 5 Oct a Monday, whatever NOTICE_DATE_ORDER says"),

    # F7: a duration, a message-part marker and "24/7" were read as a day.
    C("K-F7a", "K", "PP",
      "Scheduled maintenance 10:00 - 10:30 (GMT+8). Estimated downtime: 1/2 "
      "hour.",
      "ignore", now="2026-09-23T09:00:00+08:00",
      note="F7: '1/2 hour' is a duration, not 1 February (was 2027-02-01)"),
    C("K-F7b", "K", "PP",
      "Scheduled maintenance tomorrow 10:00 - 10:30 (GMT+8). Estimated "
      "downtime: 1/2 hour.",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'tomorrow'",
      note="F7: the finding's own trigger - the day is 'tomorrow' (G1.3 "
           "cards it), never the '1/2' of the duration"),
    C("K-F7c", "K", "PP",
      "System Maintenance 1/2\nWe will perform maintenance this Thursday "
      "10:00 - 12:00 (GMT+8).",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="this Thursday",
      note="F7: a part marker ending its heading line is not a day (was "
           "2027-02-01); the notice is dated only 'this Thursday'"),
    C("K-F7d", "K", "PP",
      "Scheduled Maintenance 2/2\nMaintenance 10:00 - 12:00 (GMT+8).",
      "ignore", now="2026-09-23T09:00:00+08:00",
      note="F7: '2/2' reads the same both ways, so only the part-marker rule "
           "stops it (was 2027-02-02)"),
    C("K-F7e", "K", "JDB",
      "系统维护 10:00-10:30 (GMT+8)，预计停机1/2小时。",
      "ignore", now="2026-09-23T09:00:00+08:00",
      note="F7: 1/2小时 is a duration"),
    C("K-F7f", "K", "Evolution",
      "Scheduled maintenance 10:00 - 12:00 (GMT+8). Customer service remains "
      "available 24/7 (Mon-Sun).",
      "ignore", now="2026-11-21T09:00:00+08:00",
      note="F7: '24/7 (Mon-Sun)' is around the clock; a weekday RANGE never "
           "vouches for a date (was 2027-07-24 from 11-21 on)"),
    C("K-F7g", "K", "Evolution",
      "Scheduled maintenance on 2026-11-26 10:00 - 12:00 (GMT+8). Customer "
      "service remains available 24/7 (Mon-Sun).",
      "fill", "2026-11-26T10:00:00+08:00", "2026-11-26T12:00:00+08:00",
      now="2026-11-21T09:00:00+08:00",
      note="F7 control: the real date still fills beside a 24/7"),

    # F42: date shapes that were unreadable, so the notice was dropped.
    C("K-F42a", "K", "Evolution",
      "Scheduled maintenance on 24th Sep (Thu) 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F42: year-less day-first month word with a weekday"),
    C("K-F42b", "K", "KingMidas",
      "Scheduled maintenance will commence at 10:00 - 12:00 24th(Thu) Sep "
      "(GMT+8).",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F42: KingMidas's English shape, weekday inside the date token"),
    C("K-F42c", "K", "Playtech",
      "Scheduled maintenance on Thursday, 24 September, 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F42: weekday in front, year-less '24 September'"),
    C("K-F42d", "K", "Hacksaw",
      "Scheduled maintenance 24-Sep-2026 10:00 - 12:00 (GMT+8)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F42: date-picker / spreadsheet export shape"),
    C("K-F42e", "K", "JDB",
      "系统维护：9月24 10:00-12:00 (GMT+8)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F42: 月 with the 日 left off, a clock straight after"),
    C("K-F42f", "K", "JILI",
      "系统维护：9.24（周四）10:00-12:00 (GMT+8)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F42: dotted m.d, read only because the weekday agrees"),
    C("K-F42g", "K", "JDB",
      "维护日期：9/24 10:00-12:00 (GMT+8)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F42 sibling: a bare n/m glued to a label colon (NFKC folds '：')"),
    C("K-F42h", "K", "JDB",
      "系统维护 10:00-12:00 (GMT+8)，12月24小时客服正常。",
      "ignore", now="2026-09-23T09:00:00+08:00",
      note="F42 guard: '12月24小时' is 24-hour support, not 24 December"),
    C("K-F42i", "K", "PP",
      "Scheduled maintenance 10:00 - 12:00 (GMT+8). Up to 10 May be affected.",
      "ignore", now="2026-09-23T09:00:00+08:00",
      note="F42 guard: the day-first month word must be capitalised AND "
           "stand as a date - '10 May be' is a verb"),
    C("K-F42j", "K", "JILI",
      "系统维护：9.24 10:00-12:00 (GMT+8)",
      "ignore", now="2026-09-23T09:00:00+08:00",
      note="F42 guard: a dotted pair with no weekday stays a decimal"),
    C("K-F42k", "K", "Evolution",
      "Issued: 2026-09-21\nScheduled maintenance on 24th Sep (Thu) 10:00 - "
      "12:00 (GMT+8)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F42: with the shape unreadable the range borrowed the letterhead "
           "and wrote 09-21 - a wrong write, not only a drop"),
    C("K-F42l", "K", "JDB",
      "系统维护：9月24 10:00-12:00 (GMT+8)。下次维护：2026-10-15",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F42: ...or the NEXT maintenance's date (was 10-15)"),

    # F61: the after-veto threw a real bare-date notice away when a noun
    # (players / users / games) or 的 / 台 followed the date.
    C("K-F61a", "K", "PP",
      "On 9/24 players will not be able to access games from 10:00 - 12:00 "
      "(GMT+8) due to scheduled maintenance.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F61: 'players' is the sentence subject after a day word"),
    C("K-F61b", "K", "PP",
      "Scheduled maintenance on 9/24 users cannot log in 10:00 - 12:00 "
      "(GMT+8).",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F61: 'users' likewise"),
    C("K-F61c", "K", "JDB",
      "将于9/24的10:00至12:00进行例行维护",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F61: 的 before a clock is possessive of the day, not a count"),
    C("K-F61d", "K", "TWSlot",
      "將於9/24 台灣時間 10:00 - 12:00 進行例行維護",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="F61: 台 of 台灣時間 is Taiwan time, not a machine counter"),
    C("K-F61e", "K", "Hacksaw",
      "Server maintenance on 3/4 of our nodes, 10:00-12:00 GMT+8.",
      "ignore", now="2026-09-23T09:00:00+08:00",
      note="F61 guard: 'of <noun>' after the pair is still a share"),
    C("K-F61f", "K", "Hacksaw",
      "Maintenance 2/3 servers 10:00-12:00 GMT+8.",
      "ignore", now="2026-09-23T09:00:00+08:00",
      note="F61 guard: a count noun with NO day word in front is a share"),

    # F73: _infer_year's window was closed at both ends (366 days), so the one
    # day exactly 120 back had two candidate years and went to NEXT year.
    C("K-F73a", "K", "KingMidas",
      "排定维护 5月25日 10:00 - 12:00 (GMT+8)",
      "fill", "2026-05-25T10:00:00+08:00", "2026-05-25T12:00:00+08:00",
      now="2026-09-22T09:00:00+08:00", stale=True,
      note="F73: now-120d is last spring's notice, stale and not written "
           "(was 2027-05-25, a future write)"),
    C("K-F73b", "K", "KingMidas",
      "排定维护 5月20日 10:00 - 12:00 (GMT+8)",
      "needs_human", now="2026-09-22T09:00:00+08:00", why="240 days ahead",
      note="F73: a year-less date the inference puts 8 months ahead is far "
           "likelier a re-posted old notice - a person decides"),
    C("K-F73c", "K", "KingMidas",
      "排定维护 5月20日 (三) 10:00 - 12:00 (GMT+8)",
      "needs_human", now="2026-09-22T09:00:00+08:00", why="Thursday",
      note="F73: the stated (三) contradicts 2027-05-20 (a Thursday)"),
    C("K-F73d", "K", "KingMidas",
      "排定维护 1月5日 10:00 - 12:00 (GMT+8)",
      "fill", "2027-01-05T10:00:00+08:00", "2027-01-05T12:00:00+08:00",
      now="2026-09-22T09:00:00+08:00",
      note="F73 control: policy 2's own example, 105 days ahead, still fills"),
    C("K-F73e", "K", "KingMidas",
      "排定维护 1月25日 (一) 10:00 - 12:00 (GMT+8)",
      "fill", "2027-01-25T10:00:00+08:00", "2027-01-25T12:00:00+08:00",
      now="2026-09-22T09:00:00+08:00",
      note="F73 control: beyond 120 days a weekday that confirms the year "
           "(25 Jan 2027 is a Monday) lets it fill"),

    # G1.3: a notice dated only relative to its post ("today", "明天", "this
    # Thursday"). The scrape carries each bubble's clock but not its DATE, so
    # the day cannot be resolved without guessing; it used to be `ignore`
    # (three reparse sweeps, then a silent drop). needs_human cards a person
    # and the reason names the word.
    C("K-G1.3a", "K", "PP",
      "Emergency maintenance today 15:00-16:00 (GMT+8)",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'today'",
      note="G1.3: the commonest emergency shape"),
    C("K-G1.3b", "K", "Hacksaw",
      "Scheduled maintenance tomorrow 10:00-12:00 GMT+8",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'tomorrow'"),
    C("K-G1.3c", "K", "Evolution",
      "Scheduled maintenance this Thursday 10:00-12:00 (GMT+8)",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'this Thursday'"),
    C("K-G1.3d", "K", "JDB",
      "明天 10:00-12:00 (GMT+8) 系统维护",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'明天'"),
    C("K-G1.3e", "K", "JILI",
      "紧急维护：今天15:00-16:00 (GMT+8)",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'今天'"),
    C("K-G1.3f", "K", "JILI",
      "紧急维护：即日起至16:00 (GMT+8)",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'即日'"),
    C("K-G1.3g", "K", "JDB",
      "系统维护：本周四 10:00-12:00 (GMT+8)",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'本周四'"),
    C("K-G1.3h", "K", "Yggdrasil",
      "Scheduled maintenance tonight 23:00 - 01:00 (GMT+8)",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'tonight'",
      note="G1.3: the near-midnight case - read after 00:00 'tonight' is "
           "already yesterday's"),
    C("K-G1.3i", "K", "PP",
      "Urgent maintenance: 15:00-16:00 (GMT+8), starting now.",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'starting now'"),
    C("K-G1.3j", "K", "JILI",
      "今晚 23:00-01:00 (GMT+8) 紧急维护",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'今晚'"),
    C("K-G1.3k", "K", "PP",
      "Emergency maintenance today 2026-09-23 15:00-16:00 (GMT+8)",
      "fill", "2026-09-23T15:00:00+08:00", "2026-09-23T16:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="G1.3 control: a relative word beside a real date still fills"),
    C("K-G1.3l", "K", "JDB",
      "今天（9月23日）15:00-16:00 (GMT+8) 紧急维护",
      "fill", "2026-09-23T15:00:00+08:00", "2026-09-23T16:00:00+08:00",
      now="2026-09-23T09:00:00+08:00",
      note="G1.3 control: 今天 with the date in brackets"),
    C("K-G1.3m", "K", "PP",
      "Following the 2026-09-16 maintenance, emergency maintenance tomorrow "
      "10:00-12:00 (GMT+8).",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'tomorrow'",
      note="G1.3 guard: 'tomorrow' stands between the range and the only "
           "date, so the range does not borrow 09-16"),
    C("K-G1.3n", "K", "PP",
      "Our next maintenance is on 2026-10-15.\nToday we have emergency "
      "maintenance 15:00-16:00 (GMT+8).",
      "needs_human", now="2026-09-23T09:00:00+08:00", why="'Today'",
      note="G1.3 guard: never 10-15 - that date belongs to another sentence"),
    C("K-G1.3o", "K", "PP",
      "Emergency maintenance today 15:00-16:00 (GMT+8) has been completed.",
      "ignore", now="2026-09-23T09:00:00+08:00",
      note="G1.3 guard: a completion stays a completion"),
    C("K-G1.3p", "K", "PP",
      "Thanks for your patience during today's emergency maintenance.",
      "ignore", now="2026-09-23T09:00:00+08:00",
      note="G1.3 guard: a relative word with no clock is no window a person "
           "could fill"),
    C("K-G1.3q", "K", "PP",
      "Emergency maintenance 15:00-16:00 (GMT+8)",
      "ignore", now="2026-09-23T09:00:00+08:00", why="no parseable",
      note="G1.3 / policy 9: NO date word at all stays `ignore` with the "
           "unparsed reason - vawatch re-reads it (a half-rendered bubble "
           "looks exactly like this) and owns what happens at its cap"),

    # -- K-F8 / F11 / F12 / F13 / F14 / F16 / F43 / F44 / F62: clocks and
    # zones (audit round 2, lane P stage 3). Policy 3b and 4.
    C("K-F8a", "K", "JDB",
      "维护公告\n时间：2026-09-23 10:00-12:00 (UTC/GMT+08:00)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F8: the Chinese studios' 'UTC/GMT+8' is +08. The bare UTC in "
           "front used to win and write the window 8 hours late"),
    C("K-F8b", "K", "CQ9",
      "维护公告\n时间：2026-09-23 10:00-12:00 (GMT/UTC +8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F8: the same with GMT first and a space before the sign"),
    C("K-F8c", "K", "FC",
      "Scheduled maintenance\nTimezone: UTC/GMT+8\nDate: 2026-09-23\n"
      "Time: 10:00-12:00",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F8: on a zone line of its own, taken as the notice's zone"),
    C("K-F8d", "K", "YGR",
      "Scheduled maintenance 2026-09-23 10:00-12:00 (GMT 9)",
      "fill", "2026-09-23T10:00:00+09:00", "2026-09-23T12:00:00+09:00",
      note="F8: a sign-less 'GMT 9' is +09 with its '+' lost, not +00 - and "
           "not NOTICE_TZ either, which is why the case uses 9 and not 8"),
    C("K-F8e", "K", "YGR",
      "Scheduled maintenance 2026-09-23 10:00-12:00 (GMT 08:00)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F8: sign-less HH:MM straight after a range is its offset"),
    C("K-F8f", "K", "PG Soft",
      "Scheduled maintenance 2026-09-23: starts 10:00 (UTC 02:00), "
      "service back 12:00 - 12:30",
      "needs_human", why="'UTC 02:00' after a single clock",
      note="F8 guard: after a LONE clock, '(UTC 02:00)' may be that clock "
           "converted or GMT+2 without its sign - hours apart, so a person"),
    C("K-F8g", "K", "PG Soft",
      "维护公告\n2026-09-23 北京时间 10:00-12:00（UTC 02:00-04:00）",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F8 control: a prefix-style 'UTC 02:00-04:00' copy is still UTC"),
    C("K-F11a", "K", "PP",
      "Scheduled maintenance 2026-09-23 10:00:00 - 12:00:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F11: HH:MM:SS. The rule used to start inside the first clock and "
           "read its minutes:seconds - written as 00:00-12:00"),
    C("K-F11b", "K", "PP",
      "Scheduled maintenance 2026-09-23 08:15:00 - 10:15:00 (GMT+8)",
      "fill", "2026-09-23T08:15:00+08:00", "2026-09-23T10:15:00+08:00",
      note="F11: was 15:00 -> 09-24 10:15 (the seconds rolled it a day)"),
    C("K-F11c", "K", "PP",
      "Scheduled maintenance 2026-09-23 23:00:00 - 01:00:00 (GMT+8)",
      "fill", "2026-09-23T23:00:00+08:00", "2026-09-24T01:00:00+08:00",
      note="F11: overnight with seconds - was 00:00-01:00, missing the "
           "real window entirely"),
    C("K-F11d", "K", "PP",
      "Scheduled maintenance 2026-09-23 22:30:00 ~ 2026-09-24 01:00:00 (GMT+8)",
      "fill", "2026-09-23T22:30:00+08:00", "2026-09-24T01:00:00+08:00",
      note="F11: a date on both ends with seconds used to have no point at "
           "all and was dropped"),
    C("K-F11e", "K", "JDB",
      "维护日期：2026年9月24日\n维护时间：10:00:00-12:00:00",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F11: the Chinese date-line layout (was 00:00-12:00)"),
    C("K-F12a", "K", "Playtech",
      "Scheduled maintenance on Sep 23, 2026 from 10:00 - 11:30 p.m. (GMT+8)",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-23T23:30:00+08:00",
      note="F12: dotted p.m. on the end only - was the MORNING 10:00-11:30"),
    C("K-F12b", "K", "Playtech",
      "Scheduled maintenance on Sep 23, 2026 from 8:00 - 11:00 PM (GMT+8)",
      "fill", "2026-09-23T20:00:00+08:00", "2026-09-23T23:00:00+08:00",
      note="F12: one-sided PM covers the bare start - was 08:00-23:00"),
    C("K-F12c", "K", "Evolution",
      "Scheduled maintenance on Sep 23, 2026 from 9:00PM-11:00 (GMT+8)",
      "fill", "2026-09-23T21:00:00+08:00", "2026-09-23T23:00:00+08:00",
      note="F12: one-sided PM covers the bare end - was 21:00 -> 09-24 11:00"),
    C("K-F12d", "K", "Evolution",
      "Scheduled maintenance on Sep 23, 2026 from 11:00 - 1:00 AM (GMT+8)",
      "fill", "2026-09-23T23:00:00+08:00", "2026-09-24T01:00:00+08:00",
      note="F12: the shorter reading of the bare start is 23:00"),
    C("K-F12e", "K", "Hacksaw",
      "Scheduled maintenance on Sep 23, 2026 from 10:00 PM - 12:00 midnight "
      "(GMT+8)",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-24T00:00:00+08:00",
      note="F12: '12:00 midnight' ends the day - was 22:00 -> 09-24 12:00"),
    C("K-F12f", "K", "VA",
      "Scheduled maintenance 2026-09-23 10:00 PM - 12:00 MN (GMT+8)",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-24T00:00:00+08:00",
      note="F12: the Philippine '12 MN' is midnight"),
    C("K-F12g", "K", "Playtech",
      "Scheduled maintenance on Sep 23, 2026 from 10:00 p.m. - 11:30 p.m. "
      "(GMT+8)",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-23T23:30:00+08:00",
      note="F12: dotted on both ends used to be no range at all"),
    C("K-F12h", "K", "Playtech",
      "Scheduled maintenance on Sep 23, 2026 from 10PM - 11:30PM (GMT+8)",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-23T23:30:00+08:00",
      note="F12: a bare hour on one end and a colon clock on the other"),
    C("K-F12i", "K", "KingMidas",
      "Scheduled maintenance on Sep 23, 2026 from 10:00 - 12:00 PM (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F12 control: the morning slot to noon stays 10:00-12:00, not "
           "22:00 -> noon"),
    C("K-F12j", "K", "KingMidas",
      "Scheduled maintenance on Sep 23, 2026 from 11:00 - 1:00 PM (GMT+8)",
      "fill", "2026-09-23T11:00:00+08:00", "2026-09-23T13:00:00+08:00",
      note="F12 control: across noon, 11:00-13:00"),
    C("K-F13a", "K", "JILI",
      "系统维护通知\n维护时间：2026年9月23日 下午2:00-4:00",
      "fill", "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note="F13: 下午 was thrown away and the row got 02:00-04:00, a window "
           "that does not overlap the outage"),
    C("K-F13b", "K", "JILI",
      "系统维护通知\n维护时间：2026年9月23日 晚上10:00-11:30",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-23T23:30:00+08:00",
      note="F13: 晚上 is evening (was 10:00-11:30)"),
    C("K-F13c", "K", "CQ9",
      "系统维护通知\n维护时间：2026年9月23日 下午2:00 ~ 下午4:00",
      "fill", "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note="F13: a 下午 on both ends used to break the range - dropped"),
    C("K-F13d", "K", "FC",
      "系统维护通知\n维护时间：2026年9月23日 下午2时-4时",
      "fill", "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note="F13: the 时 hour form takes the meridiem too"),
    C("K-F13e", "K", "FC",
      "系统维护通知\n维护时间：2026年9月23日 凌晨12:00-2:00",
      "fill", "2026-09-23T00:00:00+08:00", "2026-09-23T02:00:00+08:00",
      note="F13: 凌晨12点 is the midnight STARTING the day (was noon -> 02:00)"),
    C("K-F13f", "K", "JDB",
      "系统维护通知\n维护时间：2026年9月23日 晚上11:00-1:00",
      "fill", "2026-09-23T23:00:00+08:00", "2026-09-24T01:00:00+08:00",
      note="F13: evening start, bare end rolls to the next morning"),
    C("K-F13g", "K", "JDB",
      "系统维护通知\n维护时间：2026年9月23日 晚上1:00-3:00",
      "ignore", why="no parseable",
      note="F13 guard: 晚上1点 is 1 AM of a day the text does not state - "
           "refused, never written as 01:00 or 13:00 on the 23rd"),
    C("K-F13h", "K", "JDB",
      "系统维护通知\n维护时间：2026年9月23日 上午10:00-12:00",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F13 control: 上午 to noon, 10:00-12:00"),
    C("K-F14a", "K", "VA",
      "Scheduled maintenance on Sep 23, 2026 from 10:00 AM to 12:00 PM PST "
      "(Philippine Standard Time).",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F14: PST is Philippine Standard Time for this operator - it was "
           "read as US Pacific and written 16 hours late"),
    C("K-F14b", "K", "VA",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 PST",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F14: bare PST resolves to NOTICE_TZ's reading (Asia/Manila +08)"),
    C("K-F14c", "K", "VA",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 PST (GMT+8)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F14: PST beside GMT+8 - it used to take PST's -08 and ignore the "
           "explicit +08 on the same line"),
    C("K-F14d", "K", "RTG",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 PST (UTC-8)",
      "fill", "2026-09-24T10:00:00-08:00", "2026-09-24T12:00:00-08:00",
      note="F14: an explicit offset on the line picks the US reading"),
    C("K-F14e", "K", "RTG",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 PST (GMT+9)",
      "needs_human", why="disagrees",
      note="F14 guard: an offset that matches no reading of PST"),
    C("K-F14f", "K", "Playtech",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 IST",
      "needs_human", why="IST may be India",
      note="F14: IST is India, Irish or Israel and none is NOTICE_TZ - it "
           "used to be pinned at +05:30"),
    C("K-F14g", "K", "Playtech",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 IST (GMT+5:30)",
      "fill", "2026-09-24T10:00:00+05:30", "2026-09-24T12:00:00+05:30",
      note="F14 control: IST with its offset written is determinable"),
    C("K-F14h", "K", "CQ9",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 CST",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F14: CST is China Standard Time under NOTICE_TZ=Asia/Manila"),
    C("K-F14i", "K", "RTG",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 PST",
      "fill", "2026-09-24T10:00:00-08:00", "2026-09-24T12:00:00-08:00",
      env={"NOTICE_TZ": "America/Los_Angeles"},
      note="F14: the same rule on a US deployment gives US Pacific"),
    C("K-F14j", "K", "Baccarat",
      "Scheduled maintenance\nDate: 2026-09-24\nTime: 10:00 - 12:00\n"
      "Our ICT helpdesk will assist with any questions.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F14: 'our ICT helpdesk' is the IT department, not +07 (it put "
           "the window an hour off)"),
    C("K-F14k", "K", "RTG",
      "SCHEDULED MAINTENANCE 2026-09-24 10:00 - 12:00\nEST. DURATION: 2 HOURS",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F14: an all-caps 'EST.' is 'estimated', not New York (-05)"),
    C("K-F14l", "K", "Evolution",
      "Scheduled maintenance\nDate: 2026-09-24 (JST)\nTime: 10:00 - 12:00",
      "fill", "2026-09-24T10:00:00+09:00", "2026-09-24T12:00:00+09:00",
      note="F14 control: an abbreviation written against the DATE is a zone"),
    C("K-F14m", "K", "Baccarat",
      "Scheduled maintenance\nDate: 2026-09-24\nTime: 10:00 - 12:00\n"
      "ICT Helpdesk: +63 2 1234",
      "needs_human", why="not written against a time",
      note="F14 guard: a stray zone-like abbreviation that is neither a zone "
           "nor plainly a word - a person, never NOTICE_TZ in silence"),
    C("K-F16a", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 CEST",
      "needs_human", why="(CEST)",
      note="F16: a Malta studio's zone. Never written as 10:00-12:00 +08"),
    C("K-F16b", "K", "Yggdrasil",
      "Scheduled maintenance 2026-09-24 CEST 10:00 - 12:00",
      "needs_human", why="(CEST)",
      note="F16: the zone written IN FRONT of the range (was +08)"),
    C("K-F16c", "K", "Yggdrasil",
      "Scheduled maintenance\nTime zone: CEST\nTime: 2026-09-24 10:00 - 12:00",
      "needs_human", why="'CEST'",
      note="F16: the zone on a line of its own (was +08)"),
    C("K-F16d", "K", "Playtech",
      "维护通知 2026-09-24 04:00-06:00 欧洲中部夏令时间",
      "needs_human", why="欧洲中部夏令时间",
      note="F16: a European zone named in Chinese (was +08)"),
    C("K-F16e", "K", "BNG",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 (Bangkok time)",
      "fill", "2026-09-24T10:00:00+07:00", "2026-09-24T12:00:00+07:00",
      note="F16: a fixed-offset place is in the table - +07, not an hour off"),
    C("K-F16f", "K", "BNG",
      "系统维护 2026-09-24 10:00 - 12:00 泰国时间",
      "fill", "2026-09-24T10:00:00+07:00", "2026-09-24T12:00:00+07:00",
      note="F16: 泰国时间 is +07 (was +08)"),
    C("K-F16g", "K", "Evolution",
      "系统维护 2026-09-24 10:00 - 12:00 日本时间",
      "fill", "2026-09-24T10:00:00+09:00", "2026-09-24T12:00:00+09:00",
      note="F16: 日本时间 is +09 (was +08)"),
    C("K-F16h", "K", "Hacksaw",
      M("""
        Scheduled maintenance
        Date: 2026-09-23
        Time: 10:00 - 12:00 (GMT+8)
        For European partners: 04:00 - 06:00 CEST.
      """),
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F16: the GMT+8 window. The CEST line is its copy (04:00 CEST is "
           "10:00 +08), never unioned with it read as +08 (was 04:00-12:00)"),
    C("K-F16i", "K", "Yggdrasil",
      "Scheduled maintenance 2026-09-23 04:00 - 06:00 CET (10:00 - 12:00 GMT+8)",
      "needs_human", why="(CET)",
      note="F16 guard: 'CET' written in summer for +02 - at CET's defined +01 "
           "it is NOT the GMT+8 window, so the text contradicts itself and a "
           "person reads it"),
    C("K-F16j", "K", "Yggdrasil",
      "Scheduled maintenance 2026-09-23 05:00 - 07:00 CET (12:00 - 14:00 GMT+8)",
      "fill", "2026-09-23T12:00:00+08:00", "2026-09-23T14:00:00+08:00",
      note="F16 control: a CET range that IS its GMT+8 copy fills"),
    C("K-F43a", "K", "Evolution",
      "Scheduled maintenance\nStart: 2026-09-24 22:00 (GMT+8)\n"
      "End: 2026-09-25 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F43: a Start line and an End line are one window (was dropped)"),
    C("K-F43b", "K", "Evolution",
      "维护通知\n开始时间：2026-09-24 22:00\n结束时间：2026-09-25 02:00 (GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F43: the same in Chinese"),
    C("K-F43c", "K", "Evolution",
      "Scheduled maintenance: 2026-09-24 22:00 (GMT+8) - 2026-09-25 02:00 "
      "(GMT+8)",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F43: a zone after each end no longer breaks the pair"),
    C("K-F43d", "K", "Evolution",
      "Scheduled maintenance 2026-09-24 22:00 UTC+8 to 2026-09-25 02:00 UTC+8",
      "fill", "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note="F43: the same with 'to'"),
    C("K-F43e", "K", "PG Soft",
      "Scheduled maintenance\nDate: 2026-09-24\nStart: 10:00\nEnd: 12:00 (GMT+8)",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F43: Start / End lines with bare clocks under a Date line"),
    C("K-F43f", "K", "Evolution",
      M("""
        ※SD-7362822 ※
        Dear Casino Team,

        This is to inform you that an exceptional maintenance is going to take place with a downtime from 2026-09-24 07:00:00 UTC till 2026-09-24 08:00:00, during which the following tables will be unavailable:

        ○ Dragonara Roulette

        ● Start Time: 2026-09-24 07:00:00
        ● End Time: 2026-09-24 08:00:00
        ● UTC+8 Time: 2026-09-24 15:00:00 UTC +8 to 2026-09-24 16:00:00 UTC +8
        ● Reason: Equipment maintenance
        ● Table availability: Affected
      """),
      "fill", "2026-09-24T15:00:00+08:00", "2026-09-24T16:00:00+08:00",
      note="F43/F44/F11: Evolution's real SD-block shape (seconds, till, "
           "Start/End lines, a UTC+8 line) - every copy is one instant"),
    C("K-F43g", "K", "Yggdrasil",
      "Scheduled maintenance\nStart: 2026-09-24 22:00 CEST\n"
      "End: 2026-09-25 02:00 CEST",
      "needs_human", why="(CEST)",
      note="F43 guard: the pair forms, and its unknown zone reaches a person "
           "instead of the window being dropped"),
    C("K-F43h", "K", "Evolution",
      "Scheduled maintenance\nDate: 2026-09-24\nStart: 10:00 (GMT+9)\nEnd: 12:00",
      "fill", "2026-09-24T10:00:00+09:00", "2026-09-24T12:00:00+09:00",
      note="F43: the zone after the START clock is the pair's zone - it sat "
           "inside the two-line range and was skipped as range text (+08)"),
    C("K-F43i", "K", "Yggdrasil",
      "Scheduled maintenance\nDate: 2026-09-24\nStart: 10:00 CEST\nEnd: 12:00",
      "needs_human", why="(CEST)",
      note="F43/F16 guard: an unknown zone after the Start clock"),
    C("K-F43j", "K", "Playtech",
      "Scheduled maintenance\nStart: 2026-09-24 8:00\nEnd: 2026-09-24 11:00 PM",
      "fill", "2026-09-24T20:00:00+08:00", "2026-09-24T23:00:00+08:00",
      note="F43/F12: one-sided PM across two datetimes - 20:00, not 08:00"),
    C("K-F44a", "K", "JILI",
      "系统维护通知\n维护时间：2026年9月23日 22:00 至 次日02:00",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-24T02:00:00+08:00",
      note="F44: 次日 between the connector and the clock (was dropped)"),
    C("K-F44b", "K", "JILI",
      "系统维护通知\n维护时间：2026年9月23日 14:00 到 次日02:00",
      "fill", "2026-09-23T14:00:00+08:00", "2026-09-24T02:00:00+08:00",
      note="F44: 到 is a connector, and 次日 sets the end's day"),
    C("K-F44c", "K", "JILI",
      "系统维护通知\n维护时间：2026年9月23日 10:00 至 次日12:00",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F44: 次日 is honoured even when the clocks alone would not roll"),
    C("K-F44d", "K", "CQ9",
      "系统维护通知\n维护时间：2026年9月23日 10:00 到 12:00",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F44: 到 with spaces (was dropped)"),
    C("K-F44e", "K", "Playtech",
      "Scheduled maintenance on 2026-09-23 between 10:00 and 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F44: 'between X and Y'"),
    C("K-F44f", "K", "Playtech",
      "Scheduled maintenance on 2026-09-23 from 10:00 till 12:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F44: 'till'"),
    C("K-F44g", "K", "JDB",
      "系统维护通知\n维护时间：2026年9月23日 晚上10:00 至 次日凌晨2:00",
      "fill", "2026-09-23T22:00:00+08:00", "2026-09-24T02:00:00+08:00",
      note="F44/F13: next-day marker and meridiem words together"),
    C("K-F44h", "K", "Playtech",
      "Scheduled maintenance 2026-09-23 10:00 - 11:00 and 14:00 - 15:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T11:00:00+08:00",
      others=[("2026-09-23T14:00:00+08:00", "2026-09-23T15:00:00+08:00")],
      note="F44 control: 'and' outside 'between' joins two RANGES, never the "
           "11:00 and the 14:00 into one window (policy 5: two outages)"),
    C("K-F62a", "K", "JDB",
      "Scheduled maintenance on 23.09.2026 between 10.00 - 12.00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F62: the dotted-clock veto read 'bet' inside 'between'"),
    C("K-F62b", "K", "JDB",
      "Scheduled maintenance\nTime: 25.09.2026 10.00 - 12.00 (GMT+8) (2 hours)",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note="F62: a duration elsewhere on the line is not the unit of the "
           "dotted numbers"),
    C("K-F62c", "K", "JDB",
      "Scheduled maintenance 23.09.2026 10.00 - 12.00 (GMT+8), we apologise "
      "for any mistake",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F62: 'stake' inside 'mistake'"),
    C("K-F62d", "K", "JDB",
      "系统维护 2026年9月23日 10.00-12.00 （北京时间）维护单元：全部游戏",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F62: 元 inside 维护单元 is not money"),
    C("K-F62e", "K", "JDB",
      "Scheduled maintenance 25.09.2026. Expect 2.00 - 4.00 hours of impact.",
      "ignore",
      note="F62 guard: a unit straight after the dotted number still vetoes"),
    C("K-F62f", "K", "JDB",
      "系统维护 2026年9月25日 单注 1.00-5.00 元",
      "ignore",
      note="F62 guard: an amount in 元 still vetoes"),

    # -- P4: the wording gate - what it must govern, and what it must refuse --
    # F17: the gate word has to govern the WINDOW. A promo / release window
    # whose own sentence or heading names something else is not written because
    # "maintenance" appears elsewhere in the bubble.
    C("K-F17a", "K", "JILI",
      "Weekend Cashback! Bonus runs 2026-09-26 10:00 - 12:00 (GMT+8). Note: "
      "our routine maintenance stays on Wednesdays.", "ignore",
      note="F17: F2 plus one unrelated maintenance sentence"),
    C("K-F17b", "K", "CQ9",
      "感谢各位在系统维护期间的耐心等待。周年庆活动时间：2026年9月26日 10:00-12:00 (GMT+8)。",
      "ignore", note="F17: the CJK form - the window is the anniversary event's"),
    C("K-F17c", "K", "JILI",
      "Weekly update:\n1. Tournament: 2026-09-24 10:00 - 12:00 (GMT+8)\n"
      "2. Scheduled maintenance: 2026-09-30 02:00 - 04:00 (GMT+8)",
      "fill", "2026-09-30T02:00:00+08:00", "2026-09-30T04:00:00+08:00",
      note="F17: a bulletin writes the maintenance item, not the earlier "
           "tournament"),
    C("K-F17d", "K", "Playtech",
      "Thanks for your patience during yesterday's scheduled maintenance.\n"
      "New game release\nTime: 2026-09-26 10:00-12:00 (GMT+8)", "ignore",
      note="F17: a 'Time:' line under a RELEASE heading is the release's"),
    C("K-F17e", "K", "Playtech",
      "Scheduled maintenance will commence as below.\n公告\nDate: 2026-09-26\n"
      "Time: 10:00-12:00 (GMT+8)",
      "fill", "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note="F17 control: a neutral heading (公告) over the maintenance block"),

    # F18: a promise of no outage vetoes the notice, whatever heading opened
    # the gate.
    C("K-F18a", "K", "Playstar",
      "Platform upgrade 2026-10-14 10:00 - 12:00 GMT+8. There will be no "
      "service interruption.", "ignore", why="not an outage",
      note="F18: J1 with 'New game launch' changed to 'Platform upgrade'"),
    C("K-F18b", "K", "CQ9",
      "新版本升级：2026年9月24日 10:00-11:00 (GMT+8)，不会停止服务。", "ignore",
      note="F18: J3 with 上线 changed to 升级"),
    C("K-F18c", "K", "JDB",
      "2026年9月24日 10:00-12:00 (GMT+8) 进行不停机维护，期间游戏正常运行，玩家无需下线。",
      "ignore", note="F18: the 不 sits INSIDE the 进行…维护 match"),
    C("K-F18d", "K", "BNG",
      "Dear partner, we will perform a system upgrade on 2026-09-24 from 10:00 "
      "to 11:00 (GMT+8). There will be no downtime.", "ignore",
      note="F18: upgrade heading + 'no downtime'"),
    C("K-F18e", "K", "JILI",
      "Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8): slots will be "
      "unavailable; live casino has no downtime.", "needs_human",
      why="partial outage",
      note="F18: an outage stated AND a no-downtime promise - a person decides"),
    C("K-F18f", "K", "JILI",
      "系统维护：2026年9月24日 10:00-12:00 (GMT+8)，维护结束后游戏将正常运行。",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F18 control: 'games run normally AFTER the maintenance' is no promise"),
    C("K-F18h", "K", "JILI",
      "Scheduled maintenance 2026-09-25 03:00-05:00 (GMT+8). No downtime "
      "outside the window.",
      "fill", "2026-09-25T03:00:00+08:00", "2026-09-25T05:00:00+08:00",
      note="F18 control: a promise LIMITED to outside the window is no veto"),
    C("K-F18i", "K", "JDB",
      "系统维护：2026年9月25日 03:00-05:00 (GMT+8)，高峰时段不停服。",
      "fill", "2026-09-25T03:00:00+08:00", "2026-09-25T05:00:00+08:00",
      note="F18 control: 高峰时段不停服 limits the promise to peak hours"),
    C("K-F18g", "K", "JILI",
      "Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). Players will not "
      "be able to access games during the maintenance.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F18 control: 'will not be able to access' is the outage itself"),

    # G1.7: the negator need not TOUCH the phrase.
    C("K-G1.7a", "K", "BNG",
      "Version rollout 2026-09-24 10:00-11:00 (GMT+8), which will not cause any "
      "service interruption.", "ignore", why="negated",
      note="G1.7: 'not cause any' between negator and phrase"),
    C("K-G1.7b", "K", "BNG",
      "Version rollout 2026-09-24 10:00-11:00 (GMT+8) with zero service "
      "interruption.", "ignore", note="G1.7: 'zero'"),
    C("K-G1.7c", "K", "CQ9",
      "新版本上线：2026年9月24日 10:00-11:00 (GMT+8)，无需停机维护。", "ignore",
      note="G1.7: 无需 (was not a negator)"),
    C("K-G1.7d", "K", "CQ9",
      "热更新：2026年9月24日 10:00-11:00 (GMT+8)，无须暂停服务。", "ignore",
      note="G1.7: 无须"),
    C("K-G1.7e", "K", "BNG",
      "Version rollout 2026-09-24 10:00-11:00 (GMT+8). There won't be any "
      "service interruption.", "ignore", note="G1.7: won't be any"),
    C("K-G1.7f", "K", "JILI",
      "Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). We chose off-peak "
      "hours so it will not cause major disruption.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="G1.7 control: 'not major disruption' is still an outage"),

    # F19: the support desk's holiday hours are not an outage.
    C("K-F19a", "K", "JDB",
      "Dear partners, due to the Mid-Autumn Festival holiday, our customer "
      "service will be unavailable on 2026-09-25 10:00 - 18:00 (GMT+8). For "
      "urgent matters please email support@example.com.", "ignore",
      note="F19: customer service is the subject"),
    C("K-F19b", "K", "JDB",
      "Please note our live chat services will be offline 2026-09-25 "
      "10:00-18:00 (GMT+8) due to a team event.", "ignore",
      note="F19: live chat 'services' are not the platform"),
    C("K-F19c", "K", "JDB",
      "中秋节假期通知：2026年9月25日 10:00-18:00 (GMT+8) 客服暂停服务，紧急问题请发邮件。",
      "ignore", note="F19: 客服暂停服务"),
    C("K-F19d", "K", "JDB",
      "Please note our games will be unavailable on 2026-09-25 10:00-18:00 (GMT+8).",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T18:00:00+08:00",
      note="F19 control: the games themselves are the subject"),

    # F45: a request to relay or confirm is not a question about the window.
    C("K-F45a", "K", "Yggdrasil",
      "Dear partners, please be informed that we will have a scheduled "
      "maintenance on 24 Sep 2026 from 10:00 to 12:00 (GMT+8). Could you please "
      "help to inform your players? Thank you.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F45: the question is about informing players"),
    C("K-F45b", "K", "Yggdrasil",
      "Scheduled maintenance on 2026-09-24 10:00-12:00 (GMT+8), please advise "
      "your players accordingly.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F45: 'please advise' is a request, not a question"),
    C("K-F45c", "K", "JDB",
      "我司将于2026年9月24日 10:00-12:00 (GMT+8) 进行系统维护，麻烦确认收到，谢谢！",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F45: 麻烦确认收到 asks for a receipt"),
    C("K-F45d", "K", "KingMidas",
      "您好，可以确认本周四（2026年9月24日）10:00-12:00 (GMT+8) 进行系统维护。",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F45: the provider CONFIRMING our ask"),
    C("K-F45e", "K", "Hacksaw",
      "Could you please help to inform your players about the scheduled "
      "maintenance on 25/09/2026 14:00-16:00 (GMT+8)?",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="F45: a relay request carrying the window is the notice"),
    C("K-F45g", "K", "Hacksaw",
      "Scheduled maintenance details: https://status.example.com/maintenance?"
      "id=12 on 2026-09-25 14:00-16:00 (GMT+8)",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="F45: a URL's query '?' ends no sentence"),
    C("K-F45h", "K", "VP",
      "Could you inform us of your maintenance window on 2026-09-25 "
      "14:00-16:00 (GMT+8)?", "ignore", why="asks about it",
      note="F45 control: 'inform US' is asking - only a third party makes a "
           "relay request"),
    C("K-F45f", "K", "VP",
      "Is there any scheduled maintenance on 2026-09-24 10:00-12:00 (GMT+8)?",
      "ignore", why="asks about it",
      note="F45 control: a real question still asks, and SAYS so (F63)"),

    # F46: a test environment's maintenance is not a production outage.
    C("K-F46a", "K", "KingMidas",
      "【测试环境维护通知】2026年9月24日 10:00-12:00 (GMT+8) 测试环境(UAT)进行系统维护，"
      "正式环境不受影响。", "ignore", why="non-production",
      note="F46: 测试环境, production unaffected"),
    C("K-F46b", "K", "Hacksaw",
      "UAT server maintenance on 2026-09-24 10:00-12:00 (GMT+8). Production is "
      "not affected.", "ignore", note="F46: UAT"),
    C("K-F46c", "K", "Yggdrasil",
      "Staging environment scheduled maintenance: 2026-09-24 10:00 - 12:00 "
      "(GMT+8). The live (production) environment will operate normally.",
      "ignore", note="F46: staging"),
    C("K-F46d", "K", "Hacksaw",
      "UAT and production maintenance 2026-09-24 10:00-12:00 (GMT+8).",
      "needs_human", note="F46: both environments named - a person decides"),
    C("K-F46e", "K", "Hacksaw",
      "UAT maintenance rescheduled to 2026-09-25 14:00-16:00 (GMT+8). Production "
      "is not affected.", "ignore", resched=False,
      note="F46: the veto covers the reschedule path too"),

    # G1.1: plain English wordings, admitted where they govern the window.
    C("K-G1.1a", "K", "Evolution",
      "Evolution maintenance 25/09/2026 14:00-16:00 (GMT+8)",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.1: '<Provider> maintenance <window>'"),
    C("K-G1.1b", "K", "Evolution",
      "Maintenance Time: 25/09/2026 14:00-16:00 (GMT+8)",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.1: label on the window's own line"),
    C("K-G1.1c", "K", "JILI",
      "JILI will be doing maintenance on 25/09/2026 14:00-16:00 (GMT+8).",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.1: future verb phrase in the window's sentence"),
    C("K-G1.1d", "K", "Hacksaw",
      "Kindly be informed that there will be a maintenance on 25/09/2026 "
      "14:00-16:00 (GMT+8).",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.1: 'there will be a maintenance'"),
    C("K-G1.1e", "K", "Evolution",
      "Dear Partner,\nEvolution Maintenance\nDate: 25/09/2026\n"
      "Time: 14:00 - 16:00 (GMT+8)",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.1: heading over Label: value lines"),
    C("K-G1.1f", "K", "Playtech",
      "Scheduled Maintainance 25/09/2026 14:00-16:00 (GMT+8)",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.1: the common misspelling"),
    C("K-G1.1l", "K", "Hacksaw",
      "There will be a maintenance on Sep. 25, 2026 14:00-16:00 GMT+8.",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.1: 'Sep.' is an abbreviation, not the end of the sentence"),
    C("K-G1.1m", "K", "Evolution",
      "Dear Partner, Evolution maintenance Sept. 25, 2026 14:00-16:00 (GMT+8)",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.1: a greeting, then a label led by the month name"),
    C("K-G1.1n", "K", "Evolution",
      "Customer service maintenance 2026-09-25 14:00-16:00 (GMT+8)", "ignore",
      note="G1.1/F19 guard: the support desk's system is not the service"),
    C("K-G1.1g", "K", "Playtech",
      "No maintenance: 25/09/2026 14:00-16:00 (GMT+8) is our Mid-Autumn freeze.",
      "ignore", note="G1.1 guard: a negator in the label prefix"),
    C("K-G1.1h", "K", "Playtech",
      "Maintenance\nNone this week.\nTournament 25/09/2026 14:00-16:00 (GMT+8)",
      "ignore", note="G1.1 guard: a heading over a non-label line"),
    C("K-G1.1i", "K", "Playtech",
      "Maintenance-free update 25/09/2026 14:00-16:00 (GMT+8)", "ignore",
      note="G1.1 guard: '-free' is not a label separator"),
    C("K-G1.1j", "K", "Playtech",
      "We will not perform maintenance on 25/09/2026 14:00-16:00 (GMT+8) due to "
      "the event.", "ignore", note="G1.1 guard: a negated verb phrase"),
    C("K-G1.1k", "K", "Evolution",
      "Evolution maintenance tomorrow 14:00-16:00 (GMT+8)", "needs_human",
      why="tomorrow",
      note="G1.1 + policy 9: the plain wording dated only 'tomorrow'"),

    # G1.2: plain Chinese wordings.
    C("K-G1.2a", "K", "KingMidas",
      "【KingMidas】游戏维护\n日期：2026年9月25日\n时间：14:00-16:00 (GMT+8)\n"
      "届时游戏将无法进入，敬请见谅。",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.2: 游戏维护 heading"),
    C("K-G1.2b", "K", "JDB", "维护：25/09/2026 14:00-16:00 (GMT+8)",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.2: 维护： label"),
    C("K-G1.2c", "K", "JDB",
      "亲爱的客户，游戏将于 25/09/2026 14:00-16:00 (GMT+8) 维护。",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.2: 将于…维护 without 进行"),
    C("K-G1.2d", "K", "TWSlot",
      "每週維護：2026年9月25日 14:00-16:00 (GMT+8)",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.2: 每週維護 (Traditional)"),
    C("K-G1.2e", "K", "JDB",
      "2026年9月25日 14:00-16:00 (GMT+8) 游戏维护，届时将无法进入游戏。",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.2: <window> 游戏维护 (the trailing shape)"),
    C("K-G1.2f", "K", "JDB",
      "请不要在2026年9月25日 14:00-16:00 (GMT+8) 安排维护，当天是中秋高峰。",
      "ignore", note="G1.2 guard: a request NOT to hold one"),
    C("K-G1.2g", "K", "JDB",
      "我司将于中秋期间（2026年9月25日 14:00-16:00）暂停维护。", "ignore",
      note="G1.2 guard: 暂停维护 is a maintenance freeze"),
    C("K-G1.2h", "K", "JDB",
      "新游戏将于2026年9月25日 14:00-16:00 (GMT+8) 维护后上线", "ignore",
      note="G1.2 guard: 维护后 - the window is the launch's"),

    # G1.4: a question on another line does not veto the notice.
    C("K-G1.4a", "K", "Hacksaw",
      "Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8).\n"
      "Any questions, please contact us?",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.4: the docstring's own example"),
    C("K-G1.4b", "K", "JDB",
      "系统维护：2026年9月25日 14:00-16:00 (GMT+8)\n收到吗？",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.4: 收到吗？ asks for a receipt"),
    C("K-G1.4c", "K", "Hacksaw",
      "Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8). Please advise your "
      "players accordingly. Details: https://example.com/notice?id=12",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.4: a URL query '?' plus 'please advise'"),

    # G1.5: the outage described as closed / shut down / taken offline.
    C("K-G1.5a", "K", "Evolution",
      "Please be informed that all Evolution tables will be closed on "
      "25/09/2026 14:00-16:00 (GMT+8).",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.5: tables will be closed"),
    C("K-G1.5b", "K", "JILI",
      "The platform will be shut down for maintenance on 25/09/2026 14:00-16:00 "
      "(GMT+8).",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.5: shut down for maintenance"),
    C("K-G1.5c", "K", "JDB", "2026年9月25日 14:00-16:00 (GMT+8) 平台将关闭。",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.5: 平台将关闭"),
    C("K-G1.5d", "K", "JDB",
      "The promo page will be closed on 25/09/2026 14:00-16:00 (GMT+8).",
      "ignore", note="G1.5 guard: a promo page is not the service"),

    # G1.8: invisible and styled characters.
    C("K-G1.8a", "K", "Hacksaw",
      "Scheduled​maintenance 25/09/2026 14:00-16:00 (GMT+8)",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.8: a zero-width space splitting the heading"),
    C("K-G1.8b", "K", "Hacksaw",
      "Scheduled maintenance 25/09/2026 1​4:00-16:00 (GMT+8)",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.8: a ZWSP inside a clock read 04:00 - a wrong window"),
    C("K-G1.8c", "K", "Hacksaw",
      "\U0001d412\U0001d41c\U0001d421\U0001d41e\U0001d41d\U0001d42e\U0001d425"
      "\U0001d41e\U0001d41d \U0001d40c\U0001d41a\U0001d422\U0001d427\U0001d42d"
      "\U0001d41e\U0001d427\U0001d41a\U0001d427\U0001d41c\U0001d41e "
      "25/09/2026 14:00-16:00 (GMT+8)",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.8: a mathematical-bold heading"),

    # F63: a guard-refused notice says which guard refused it.
    C("K-F63a", "K", "VP",
      "Hi team, could you confirm your maintenance window on 2026-09-24 "
      "01:00 - 02:00 (GMT+8)?", "ignore", why="asks about it",
      note="F63: J6 again - the reason is no longer 'no scheduled-maintenance "
           "wording'"),
    C("K-F63b", "K", "BNG",
      "Version rollout 2026-09-24 10:00 - 11:00 (GMT+8). There will be no "
      "service interruption and no planned downtime.", "ignore",
      why="negated", note="F63: J2 again, with a truthful reason"),

    # F64: "no maintenance" inside a question or a reply-with request.
    C("K-F64a", "K", "KingMidas", "请问本周没有维护吗？", "ignore",
      note="F64: our side asking"),
    C("K-F64b", "K", "Hacksaw", "Hi team, no maintenance this week?", "ignore",
      note="F64: a confirming question"),
    C("K-F64c", "K", "Hacksaw",
      "Hi team, are there any maintenance plans for this week? Please reply "
      "'No maintenance' if none.", "ignore",
      note="F64: an ask that quotes the reply wording"),
    C("K-F64d", "K", "KingMidas", "如没有维护请回复", "ignore",
      note="F64: 如没有维护 is a condition"),
    C("K-F64e", "K", "KingMidas", "维护安排如下：本周没有维护", "clear",
      note="F64 control: 如下 is not a condition"),

    # F66: "no maintenance" as a modifier of a release.
    C("K-F66a", "K", "PG Soft",
      "The hotfix will be deployed tonight with no maintenance downtime.",
      "ignore", note="F66: a hotfix note"),
    C("K-F66b", "K", "PG Soft",
      "Game update v2.3 released, no maintenance required.", "ignore",
      note="F66: a release note"),
    C("K-F66c", "K", "Hacksaw",
      "Hi, no maintenance this week, but next week we have scheduled "
      "maintenance on Wednesday, time TBC.", "clear",
      note="F66 control: answers for this week"),
    C("K-F66d", "K", "Hacksaw", "Hi team, no maintenance.", "clear",
      note="F66 control: the bare answer"),

    # F75: the kill switches, and strict + switches off = the 67cf5f4 gate.
    C("K-F75a", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). There will be no "
      "service interruption.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      env={"NOTICE_WORDING": "strict", "NOTICE_NEGATION_GUARD": "0"},
      note="F75: NOTICE_NEGATION_GUARD=0 restores the old fill"),
    C("K-F75b", "K", "VP",
      "Hi team, could you confirm your maintenance window on 2026-09-24 "
      "01:00 - 02:00 (GMT+8)? Scheduled maintenance.",
      "fill", "2026-09-24T01:00:00+08:00", "2026-09-24T02:00:00+08:00",
      env={"NOTICE_QUESTION_GUARD": "0"},
      note="F75: NOTICE_QUESTION_GUARD=0 turns the question guard off"),
    C("K-F75c", "K", "Evolution",
      "Evolution maintenance 25/09/2026 14:00-16:00 (GMT+8)", "ignore",
      env={"NOTICE_WORDING": "strict"},
      note="F75: strict is the old gate - the plain shapes are off too"),
    C("K-F75d", "K", "Hacksaw",
      "Staging environment scheduled maintenance: 2026-09-24 10:00 - 12:00 (GMT+8).",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      env={"NOTICE_NONPROD_GUARD": "0"},
      note="F75: NOTICE_NONPROD_GUARD=0 turns the F46 veto off"),

    # --- Stage P5: completions, cancellations, extensions, retractions ---
    # F21: cancellation vocabulary and reach.
    C("K-F21a", "K", "JDB",
      "例行维护（2026-09-23 10:00-12:00 GMT+8）已取消。", "ignore",
      note="F21: the window stands between 维护 and 已取消 (reach 40, not 14)"),
    C("K-F21b", "K", "Hacksaw",
      "We have cancelled the scheduled maintenance on 2026-09-23 10:00 - 12:00 "
      "(GMT+8).", "ignore", note="F21: the active 'we have cancelled'"),
    C("K-F21c", "K", "Hacksaw",
      "CANCELLED: Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8)",
      "ignore", note="F21: a status word heading the notice"),
    C("K-F21d", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8) — CANCELLED",
      "ignore", note="F21: a status word closing the notice"),
    C("K-F21e", "K", "PP",
      "Dear partners,\nScheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8)\n"
      "has been cancelled.", "ignore",
      note="F21: the verb opens the next line"),
    C("K-F21f", "K", "PP",
      "The scheduled maintenance on 2026-09-24 10:00-12:00 (GMT+8) will not "
      "take place.", "ignore", note="F21: negation after the subject"),
    C("K-F21g", "K", "PP",
      "Update: the server maintenance on 2026-09-24 10:00-12:00 (GMT+8) has "
      "been called off.", "ignore", note="F21: called off"),
    C("K-F21h", "K", "JILI",
      "原定于2026年9月24日 10:00-12:00 (GMT+8) 的系统维护将不进行。", "ignore",
      note="F21: 将不进行"),
    C("K-F21i", "K", "JILI",
      "2026-09-23 10:00-12:00 (GMT+8) 例行维护撤销。", "ignore", note="F21: 撤销"),
    C("K-F21j", "K", "Hacksaw",
      "We would like to inform you that the scheduled maintenance on "
      "2026-09-24 10:00-12:00 (GMT+8) has been cancelled.", "ignore",
      note="F21: 'would like to' is not a future frame"),
    C("K-F21k", "K", "JDB",
      "【维护取消通知】\n尊敬的合作伙伴：\n原定维护时间：2026-09-23 10:00-12:00 "
      "(GMT+8)", "ignore",
      note="F21 regression: a cancel headline over the ordinary 维护时间 line"),
    C("K-F21l", "K", "Hacksaw",
      "Scheduled maintenance is cancelled.\nMaintenance window: 2026-09-23 "
      "10:00-12:00 (GMT+8)", "ignore",
      note="F21 regression: the window line alone does not rescue it"),

    # F24: the cancel verb on another line / sentence than the window.
    C("K-F24a", "K", "JDB",
      "【取消维护通知】\n例行维护时间：2026-09-23 10:00-12:00 (GMT+8)\n本次维护取消。",
      "ignore", note="F24: heading and closing line both cancel it"),
    C("K-F24b", "K", "Hacksaw",
      "【Maintenance Cancelled】\nScheduled maintenance: 2026-09-23 10:00 - "
      "12:00 (GMT+8)\nThe above maintenance has been cancelled.", "ignore",
      note="F24: English template"),
    C("K-F24c", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-23 10:00 - 12:00 (GMT+8). This "
      "maintenance has been cancelled.", "ignore",
      note="F24: a later sentence cancels the earlier window"),

    # F25: a cancelled sibling window never joins the row's union.
    C("K-F25a", "K", "Hacksaw",
      "Scheduled maintenance: 2026-09-23 10:00-12:00 (GMT+8). Maintenance for "
      "2026-09-23 14:00-16:00 has been cancelled.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      others=[], note="F25: was 10:00-16:00"),
    C("K-F25b", "K", "Evolution",
      "Scheduled maintenance for Live Casino: 2026-09-24 14:00-16:00 (GMT+8) "
      "will proceed as planned. The Slots maintenance on 2026-09-24 "
      "10:00-11:00 has been cancelled.",
      "fill", "2026-09-24T14:00:00+08:00", "2026-09-24T16:00:00+08:00",
      others=[], note="F25: the cancelled window sorted first"),
    C("K-F25c", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8).\nThe second "
      "maintenance on 2026-09-24 02:00 - 04:00 (GMT+8) has been cancelled.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      others=[], note="F25: the cancelled window is not listed as another"),

    # F30: a noun that NAMES the maintenance does not disarm its cancellation.
    C("K-F30a", "K", "JILI",
      "游戏维护通知：2026-09-23 10:00-12:00 (GMT+8) 的游戏维护取消。", "ignore",
      note="F30: 游戏维护 is the maintenance itself"),
    C("K-F30b", "K", "Hacksaw",
      "Withdrawal system maintenance on 2026-09-23 10:00 - 12:00 (GMT+8) has "
      "been cancelled.", "ignore", note="F30: a subsystem name"),
    C("K-F30c", "K", "JDB",
      "为提升游戏体验，原定于2026-09-23 10:00-12:00 (GMT+8)的维护已取消。",
      "ignore", note="F30: boilerplate earlier in the sentence"),
    C("K-F30d", "K", "JDB",
      "2026-09-23 10:00-12:00 (GMT+8) 交易系统维护已取消。", "ignore",
      note="F30: 交易系统维护"),
    C("K-F30e", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8). All ongoing "
      "rounds before the maintenance will be cancelled.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F30 control: the ROUNDS are cancelled"),
    C("K-F30f", "K", "JDB",
      "例行维护：2026-09-23 10:00-12:00 (GMT+8)。所有注单在维护期间将取消。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F30 control: 注单在维护期间 - the bets are cancelled"),

    # F47: a future END of an upcoming window is not a completion.
    C("K-F47a", "K", "JDB",
      "例行维护：2026-09-23 10:00-12:00 (GMT+8)，维护将于12:00结束。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F47: 将于…结束"),
    C("K-F47b", "K", "JDB",
      "2026-09-23 10:00-12:00 (GMT+8) 进行例行维护，预计12:00完成。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F47: 预计…完成"),
    C("K-F47c", "K", "JDB",
      "例行维护：2026-09-23 10:00-12:00 (GMT+8)，维护结束时间以实际为准。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F47: 结束时间 is a noun"),
    C("K-F47d", "K", "JDB",
      "例行维护：2026-09-23 10:00-12:00 (GMT+8)，如维护取消将另行通知。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F47: 如 - a condition"),
    C("K-F47e", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8), we will notify "
      "you if this maintenance is cancelled.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F47: the frame in front of the VERB"),
    C("K-F47f", "K", "JDB",
      "例行维护：2026-09-23 10:00-12:00 (GMT+8)，维护期间未完成的游戏将自动结算。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F47: 未完成 is not a completion"),

    # F48: done + next joined by a comma.
    C("K-F48a", "K", "JDB",
      "上次维护已完成，下次例行维护将于 2026-09-30 10:00-12:00 (GMT+8) 进行。",
      "fill", "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      note="F48: G4 with a comma"),
    C("K-F48b", "K", "Hacksaw",
      "Last week's maintenance has been completed, and the next scheduled "
      "maintenance is on 2026-09-30 10:00 - 12:00 (GMT+8).",
      "fill", "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      note="F48: English"),
    C("K-F48c", "K", "JDB",
      "维护已取消，原定 2026-09-30 10:00-12:00 (GMT+8) 的例行维护不再进行。",
      "ignore", note="F48 trap: a comma split must not turn this into a fill"),
    C("K-F48d", "K", "Hacksaw",
      "The maintenance has been cancelled, the scheduled maintenance window "
      "2026-09-30 10:00 - 12:00 (GMT+8) no longer applies.", "ignore",
      note="F48 trap, English"),

    # F49: a cancelled / completed window sorting first no longer drops a
    # live one.
    C("K-F49a", "K", "Hacksaw",
      "Maintenance on 2026-09-23 10:00-12:00 has been cancelled. Next "
      "scheduled maintenance: 2026-09-30 10:00-12:00 (GMT+8).",
      "fill", "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      note="F49: cancelled, then the next"),
    C("K-F49b", "K", "Hacksaw",
      "The maintenance on 2026-09-23 10:00-12:00 has been completed. Next "
      "scheduled maintenance: 2026-09-30 10:00-12:00 (GMT+8).",
      "fill", "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      now="2026-09-23T11:00:00+08:00", note="F49: finished early, then the next"),
    C("K-F49c", "K", "Evolution",
      "Update: The Slots maintenance on 2026-09-24 10:00-11:00 has been "
      "cancelled. The Live Casino scheduled maintenance will still take place "
      "on 2026-09-24 14:00-16:00 (GMT+8).",
      "fill", "2026-09-24T14:00:00+08:00", "2026-09-24T16:00:00+08:00",
      note="F49: a partial cancellation by product"),

    # F50: an extended maintenance writes the NEW end.
    C("K-F50a", "K", "Hacksaw",
      "Maintenance extended until 14:00. The scheduled maintenance on "
      "2026-09-23 10:00 - 12:00 (GMT+8) is extended until 14:00 GMT+8.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T14:00:00+08:00",
      now="2026-09-23T11:00:00+08:00", resched=True, stale=False,
      note="F50: was a re-write of 12:00"),
    C("K-F50b", "K", "JDB",
      "【维护延长通知】原定2026-09-23 10:00-12:00的维护将延长至14:00 (GMT+8)",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T14:00:00+08:00",
      now="2026-09-23T11:00:00+08:00", resched=True, note="F50: 延长至"),
    C("K-F50c", "K", "JDB", "维护延长至14:00", "needs_human",
      now="2026-09-23T11:00:00+08:00", resched=True,
      why="original window could not be read",
      note="F50: no original window - a person corrects End"),
    C("K-F50d", "K", "Hacksaw",
      "The scheduled maintenance on 2026-09-23 10:00 - 12:00 (GMT+8) is "
      "extended until 14:00 GMT+8.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T14:00:00+08:00",
      now="2026-09-23T12:30:00+08:00", resched=True, stale=False,
      note="F50: posted after the old end - no longer dropped as stale"),
    C("K-F50e", "K", "Hacksaw",
      "The scheduled maintenance on 2026-09-23 10:00 - 12:00 (GMT+8) has been "
      "extended by 2 hours.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T14:00:00+08:00",
      now="2026-09-23T11:00:00+08:00", resched=True, note="F50: a duration"),
    C("K-F50f", "K", "Hacksaw",
      "The scheduled maintenance on 2026-09-23 10:00 - 12:00 (GMT+8) is "
      "extended until 14:00 GMT+8.", "needs_human",
      now="2026-09-23T11:00:00+08:00", resched=True,
      env={"NOTICE_EXTENSION_FILL": "0"}, why="NOTICE_EXTENSION_FILL=0",
      note="F50: the kill switch sends every extension to a person"),
    C("K-F50g", "K", "JDB",
      "例行维护通知\n维护开始时间：2026-09-23 10:00\n维护结束时间：2026-09-23 12:00",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F50 (correction 2): 维护结束时间 is a label, not a completion"),
    C("K-F50h", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-23 has been extended from 10:00 - "
      "12:00 to 10:00 - 14:00 (GMT+8).",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T14:00:00+08:00",
      now="2026-09-23T11:00:00+08:00",
      note="F50 control: the full new range is read by the ordinary rules"),
    C("K-F50i", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8) is extended "
      "until further notice.", "needs_human",
      now="2026-09-23T11:00:00+08:00", resched=True,
      why="new end time could not be read",
      note="F50: no end - never a re-write of 12:00"),
    C("K-F50j", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8). The "
      "maintenance may be extended if needed.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F50 control: a possibility is not an extension"),

    # F60: what is cancelled is the unfinished games, not the maintenance.
    C("K-F60a", "K", "Evolution",
      "Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); unfinished "
      "games will be cancelled.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F60: ';' ends the maintenance's clause"),
    C("K-F60b", "K", "Evolution",
      "Scheduled maintenance 2026-09-24 10:00 - 12:00 (GMT+8), any unfinished "
      "games will be cancelled",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="F60: an 'unfinished' noun phrase is a new subject"),
    C("K-F60c", "K", "Evolution",
      "Scheduled maintenance for Baccarat games on 2026-09-24 10:00 - 12:00 "
      "(GMT+8) is cancelled.", "ignore",
      note="F60 control: 'games' is not an object noun - this IS a cancellation"),

    # F65: an early finish is a completion.
    C("K-F65a", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8) has ended early. "
      "All games are back online.", "ignore",
      now="2026-09-23T11:30:00+08:00", note="F65: has ended early"),
    C("K-F65b", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8) is complete.",
      "ignore", now="2026-09-23T11:30:00+08:00", note="F65: is complete"),
    C("K-F65c", "K", "JDB",
      "例行维护 2026-09-23 10:00-12:00 提前完成，游戏已开放。", "ignore",
      now="2026-09-23T11:30:00+08:00", note="F65: 提前完成 after the window"),
    C("K-F65d", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8) completed.",
      "ignore", now="2026-09-23T11:30:00+08:00", note="F65: bare participle"),
    C("K-F65e", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8). The maintenance "
      "is expected to be completed by 12:00.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F65 control: 'to be completed' is a promise"),

    # G1.6: "Last maintenance completed. Next maintenance: <window>".
    C("K-G1.6a", "K", "Hacksaw",
      "Last maintenance has been completed. Next maintenance: 25/09/2026 "
      "14:00-16:00 (GMT+8).",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.6: bare 'Next maintenance:'"),
    C("K-G1.6b", "K", "JDB",
      "上次维护已完成。下次维护：2026年9月25日 14:00-16:00 (GMT+8)。",
      "fill", "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note="G1.6: 下次维护："),
    C("K-G1.6c", "K", "Hacksaw",
      "Today's maintenance has been completed. Next week's maintenance will "
      "be on 02/10/2026 14:00-16:00 (GMT+8).",
      "fill", "2026-10-02T14:00:00+08:00", "2026-10-02T16:00:00+08:00",
      note="G1.6: next week's maintenance will be on"),

    # G2.1: a retraction that restates the withdrawn window.
    C("K-G2.1a", "K", "Hacksaw",
      "Please ignore the scheduled maintenance notice for 26/09/2026 "
      "02:00-06:00 (GMT+8) above, it was sent to the wrong group. Sorry!",
      "needs_human", why="retracts or voids a notice",
      note="G2.1: English"),
    C("K-G2.1b", "K", "JDB",
      "请忽略上面 2026年9月26日 02:00-06:00 (GMT+8) 的维护通知，发错群了，抱歉！",
      "needs_human", why="retracts or voids a notice", note="G2.1: 简体"),
    C("K-G2.1c", "K", "JDB",
      "請忽略上面 2026年9月26日 02:00-06:00 (GMT+8) 的維護通知，發錯群了，抱歉！",
      "needs_human", why="retracts or voids a notice", note="G2.1: 繁體"),
    C("K-G2.1d", "K", "JDB",
      "以上维护公告（2026/09/24 10:00-12:00 GMT+8）作废。", "needs_human",
      why="retracts or voids a notice", note="G2.1: 作废"),
    C("K-G2.1e", "K", "Hacksaw",
      "Please ignore the scheduled maintenance notice for 24/09/2026 "
      "10:00-12:00 (GMT+8); there will be no maintenance.", "needs_human",
      why="retracts or voids a notice",
      note="G2.1: was a fill despite 'no maintenance'"),
    # A void TAG opening a fresh post of the notice. The carve-out that kept
    # "[VOID - wrong group] <notice>" a fill (for vawatch's edit rule) also
    # wrote the voided window when the tagged notice was POSTED, not edited;
    # "[VOID]" and 【作废】 were never recognised at all (integration stage).
    C("K-G2.1t1", "K", "Hacksaw",
      "[VOID - wrong group] Scheduled maintenance on 2026-09-25 10:00 - 12:00 "
      "(GMT+8).", "needs_human", why="retracts or voids a notice",
      note="lead void tag, formerly carved out"),
    C("K-G2.1t2", "K", "Hacksaw",
      "[VOID] Scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8).",
      "needs_human", why="“[VOID]”", note="lead void tag"),
    C("K-G2.1t3", "K", "JDB",
      "【作废】系统维护通知 2026年9月25日 10:00-12:00 (GMT+8)", "needs_human",
      why="“【作废】”", note="lead void tag, 简体"),
    C("K-G2.1t4", "K", "JDB",
      "（發錯群，請忽略）系統維護 2026-09-25 10:00 - 12:00 (GMT+8)", "needs_human",
      why="retracts or voids a notice", note="lead void tag, 繁體, formerly carved out"),
    C("K-G2.1t5", "K", "Hacksaw",
      "[Maintenance Notice] Scheduled maintenance on 2026-09-25 10:00 - 12:00 "
      "(GMT+8).", "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note="lead-tag control: a heading tag is not a void tag"),
    C("K-G2.1t6", "K", "Hacksaw",
      "(Ignore if already done) Scheduled maintenance on 2026-09-25 10:00 - "
      "12:00 (GMT+8).", "fill", "2026-09-25T10:00:00+08:00",
      "2026-09-25T12:00:00+08:00",
      note="lead-tag control: a condition in brackets is not a void tag"),
    C("K-G2.1f", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). Please ignore "
      "this message if you have already updated your whitelist.",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="G2.1 control: a condition, not a retraction"),
    C("K-G2.1g", "K", "JDB",
      "例行维护：2026-09-24 10:00-12:00 (GMT+8)。维护期间如出现异常提示请忽略。",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note="G2.1 control: ignore an error screen"),
    C("K-G2.1h", "K", "Hacksaw",
      "Please ignore the above maintenance notice, it was sent to the wrong "
      "group.", "ignore",
      note="G2.1: window-less - vawatch cards it against the row (G2.3)"),

    # P5 guards that keep the widened vocabulary honest.
    C("K-F21m", "K", "JDB",
      "维护时间：2026-10-14 10:00-12:00 (GMT+8)。本次维护不会取消。",
      "fill", "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note="F21 control: 不会取消 says it goes ahead"),
    C("K-F21n", "K", "Hacksaw",
      "Maintenance Cancelled\nScheduled maintenance: 2026-10-14 10:00 - 12:00 "
      "(GMT+8)", "ignore", note="F21: a bare-participle heading"),
    C("K-F24d", "K", "Hacksaw",
      "The last maintenance notice has been cancelled.\nMaintenance time: "
      "2026-10-14 10:00-12:00 (GMT+8)", "ignore",
      note="F24: 'last' modifies the NOTICE, so it is not a past maintenance"),
    C("K-F30g", "K", "JDB",
      "例行维护：2026-10-14 10:00-12:00 (GMT+8)，届时将取消所有未完成订单。",
      "fill", "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note="F30 control: the object follows the verb - the ORDERS are cancelled"),
    C("K-F65f", "K", "Hacksaw",
      "Scheduled maintenance on 2026-10-14 10:00 - 12:00 (GMT+8) is over 2 "
      "hours long.",
      "fill", "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note="F65 control: 'is over' + a duration is not a completion"),
    C("K-G2.1i", "K", "Hacksaw",
      "Scheduled maintenance 2026-10-14 10:00-12:00 (GMT+8). If you received "
      "this in the wrong group, please let us know.",
      "fill", "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note="G2.1 control: a condition, not a retraction"),

    # -- K-F20 / F22 / F23 / F26 / F27 / F28 / F29 / F52 / F53 / F54 / G2.2 /
    #    G4.3: reschedules, superseded windows, corrections, requests. ------
    #
    # A reschedule OVERRIDES the row, so every one of these used to be a
    # confident write: the withdrawn window, a union nobody announced, a
    # tournament, a question, or the OPERATOR's own request.

    # F20: a reschedule word that does not state a move of the maintenance.
    C("K-F20", "K", "Hacksaw",
      "Hi team, can the maintenance be postponed to 2026-09-25 02:00-04:00 "
      "(GMT+8)?",
      "ignore", resched=False, why="question about moving",
      note="F20: a QUESTION with reschedule wording was written as the new "
           "window and carded '(rescheduled)'"),
    C("K-F20b", "K", "PP", "The tournament has been postponed to 2026-09-26 "
      "20:00-22:00 (GMT+8).", "ignore", resched=False,
      note="F20: the tournament is the subject of 'postponed', not the "
           "maintenance"),
    C("K-F20c", "K", "PP", "Promo rescheduled: 2026-09-26 20:00 - 22:00 GMT+8",
      "ignore", resched=False, note="F20: a promo, not the maintenance"),
    C("K-F20d", "K", "Evolution",
      "Could we reschedule our integration call to 2026-09-25 15:00-16:00 "
      "(GMT+8)?", "ignore", resched=False,
      note="F20: a call, and a question - nothing about maintenance"),
    C("K-F20e", "K", "KingMidas",
      "请问维护可以改期到2026年9月25日 10:00-12:00 (GMT+8) 吗？",
      "ignore", resched=False, why="question",
      note="F20: 请问…吗？ asks; it announces nothing"),
    C("K-F20f", "K", "JILI", "中秋节客服时间调整：2026年9月25日 10:00-22:00",
      "ignore", resched=False,
      note="F20: bare 时间调整 of the SUPPORT desk (客服) is no reschedule"),
    C("K-F20g", "K", "Yggdrasil",
      "Could you reschedule the maintenance to 2026-09-25 02:00 - 04:00 "
      "(GMT+8)? Peak hours for us.", "ignore", resched=False,
      note="F20: our side asking the provider to move - not a notice"),
    C("K-F20h", "K", "Hacksaw",
      "Please note the maintenance has been rescheduled to 2026-09-25 "
      "10:00-12:00 (GMT+8).",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F20 control: a provider STATING the move still fills"),

    # F22: the OLD window is the subject of the move verb.
    C("K-F22", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-23 10:00 - 12:00 (GMT+8) has been "
      "rescheduled to 2026-09-25 10:00 - 12:00 (GMT+8).",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: wrote the withdrawn 09-23 at HEAD"),
    C("K-F22b", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-23 10:00 - 12:00 (GMT+8) has been "
      "rescheduled to 2026-09-23 14:00 - 16:00 (GMT+8).",
      "fill", "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      resched=True, note="F22: a same-day move wrote the union 10:00-16:00"),
    C("K-F22c", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-23 10:00 - 12:00 (GMT+8) has been "
      "rescheduled to 2026-09-24 10:00 - 12:00 (GMT+8).",
      "fill", "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      resched=True, note="F22: a next-day move wrote a 26h union"),
    C("K-F22d", "K", "Evolution",
      "Scheduled maintenance rescheduled: 2026-09-25 10:00 - 12:00 (GMT+8) "
      "(was 2026-09-23 10:00 - 12:00).",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: '(was X)' marks X withdrawn (new window first)"),
    C("K-F22e", "K", "TWSlot",
      "2026-09-23 10:00-12:00 的维护改期至 2026-09-25 10:00-12:00 (GMT+8)。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: X 的维护改期至 Y"),
    C("K-F22f", "K", "JDB",
      "维护时间由2026-09-23 10:00-12:00改为2026-09-25 10:00-12:00 (GMT+8)。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: 由X改为Y (was an ordinary fill of X)"),
    C("K-F22g", "K", "VA",
      "本来定于2026-09-23 10:00-12:00的维护顺延至2026-09-25 10:00-12:00 (GMT+8)。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: 本来定于X…顺延至Y"),
    C("K-F22h", "K", "JDB",
      "维护时间调整\n变更前：2026-09-23 10:00-12:00\n变更后：2026-09-25 "
      "10:00-12:00 (GMT+8)",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: the 变更前/变更后 label pair"),
    C("K-F22i", "K", "Playtech",
      "Maintenance rescheduled\nOld: 2026-09-23 10:00-12:00 (GMT+8)\nNew: "
      "2026-09-25 10:00-12:00 (GMT+8)",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: the Old:/New: label pair"),
    C("K-F22j", "K", "Playtech",
      "Maintenance rescheduled\nBefore: 2026-09-23 10:00-12:00 (GMT+8)\nAfter: "
      "2026-09-25 10:00-12:00 (GMT+8)",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: the Before:/After: label pair"),
    C("K-F22k", "K", "Hacksaw",
      "Scheduled maintenance moved: 2026-09-23 10:00-12:00 -> 2026-09-25 "
      "10:00-12:00 (GMT+8)",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: an arrow joins the old window to the new"),
    C("K-F22l", "K", "Evolution",
      "The maintenance planned for 2026-09-23 10:00-12:00 (GMT+8) will now take "
      "place on 2026-09-25 10:00-12:00 (GMT+8).",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: 'will now take place on' (was ignored)"),
    C("K-F22m", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-23 10:00-12:00 has been deferred to "
      "2026-09-25 10:00-12:00 (GMT+8).",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: 'deferred to' (was an ordinary fill of 09-23)"),
    C("K-F22n", "K", "JILI",
      "2026-09-23 10:00-12:00 的维护推迟到 2026-09-25 10:00-12:00 (GMT+8)。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: 推迟到"),
    C("K-F22o", "K", "JDB",
      "维护时间调整为 2026-09-25 10:00-12:00 (GMT+8)，原 2026-09-23 10:00-12:00 取消。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F22: a bare 原 in front of the old window"),
    C("K-F22p", "K", "Hacksaw",
      "Maintenance rescheduled:\n2026-09-23 10:00-12:00 (GMT+8)\n2026-09-23 "
      "14:00-16:00 (GMT+8)",
      "needs_human", resched=True, why="does not mark which one is the old",
      note="F22 guard: two windows, neither marked old - never their union"),

    # F23: a postponement with no new date re-asserted the withdrawn window.
    C("K-F23", "K", "ColorGame",
      "The maintenance scheduled for 2026-09-23 10:00 - 12:00 (GMT+8) has been "
      "postponed. A new date will follow.",
      "needs_human", resched=True, why="superseded",
      note="F23: A18 without 'originally' - still the withdrawn window"),
    C("K-F23b", "K", "Hacksaw",
      "scheduled maintenance on 23/09 10:00-12:00 GMT+8 postponed until further "
      "notice", "needs_human", resched=True, why="superseded",
      note="F23: 'postponed until further notice'"),
    C("K-F23c", "K", "Hacksaw",
      "Kindly be informed the scheduled maintenance on 2026-09-23 from 10:00 to "
      "12:00 is postponed. We will update the new schedule soon.",
      "needs_human", resched=True, why="superseded", note="F23"),
    C("K-F23d", "K", "JDB",
      "2026-09-23 10:00-12:00 (GMT+8) 的例行维护已取消，将重新安排时间，另行通知。",
      "needs_human", resched=True, note="F23: cancelled, to be rescheduled"),

    # F26: delay verbs (推迟/delayed/deferred/pushed back) were no reschedule,
    # so the withdrawn window filled as an ordinary notice.
    C("K-F26", "K", "Hacksaw",
      "Scheduled maintenance has been delayed. It was previously scheduled for "
      "2026-09-23 10:00 - 12:00 (GMT+8). A new date will be announced.",
      "needs_human", resched=True, note="F26"),
    C("K-F26b", "K", "VA",
      "例行维护通知：原定 2026-09-23 10:00-12:00 (GMT+8) 的维护推迟，新时间另行公布。",
      "needs_human", resched=True, why="superseded", note="F26: 推迟, no date"),
    C("K-F26c", "K", "Hacksaw",
      "The maintenance originally scheduled for 2026-09-23 10:00 - 12:00 "
      "(GMT+8) has been moved to next week.",
      "needs_human", resched=True, why="superseded", note="F26"),
    C("K-F26d", "K", "Hacksaw",
      "The maintenance originally planned for 2026-09-23 10:00 - 12:00 (GMT+8) "
      "is deferred until further notice.",
      "needs_human", resched=True, why="superseded", note="F26"),
    C("K-F26e", "K", "VA",
      "原定 2026-09-23 10:00-12:00 (GMT+8) 的维护推迟至 2026-09-25 10:00-12:00 (GMT+8)。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F26: 推迟至 with the new date - the NEW one"),
    C("K-F26f", "K", "Hacksaw",
      "The maintenance originally set for 2026-09-23 10:00-12:00 is delayed to "
      "2026-09-25 10:00-12:00 (GMT+8).",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F26: 'delayed to' with the new date"),
    C("K-F26g", "K", "Hacksaw",
      "the scheduled maintenance on 2026-09-23 10:00 - 12:00 (GMT+8) has been "
      "delayed. We will update you on the new schedule.",
      "needs_human", resched=True, why="superseded",
      note="F26: no originally-marker - the move verb makes 09-23 withdrawn"),
    C("K-F26h", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8) is pushed back, "
      "new schedule TBA", "needs_human", resched=True, why="superseded",
      note="F26: 'pushed back'"),
    C("K-F26i", "K", "JDB",
      "例行维护：2026-09-25 10:00-12:00 (GMT+8)，维护期间提款将延迟到账。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=False,
      note="F26 control: 延迟 of withdrawals DURING the maintenance is latency"),
    C("K-F26j", "K", "Hacksaw",
      "Scheduled maintenance 2026-09-25 10:00-12:00 (GMT+8). During maintenance, "
      "some deposits may be delayed.",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=False, note="F26 control: deposits may be delayed"),
    C("K-F26k", "K", "Hacksaw",
      "Scheduled maintenance on 2026-09-25 10:00-12:00 (GMT+8), the new game "
      "launch has been delayed to next week.",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=False, note="F26 control: the LAUNCH is delayed"),

    # F27: an unrelated 原定/之前的 in an earlier sentence tainted the window.
    C("K-F27", "K", "VA",
      "原定的周年庆活动延期。系统维护时间：2026-09-23 10:00 - 12:00 (GMT+8)。"
      "新活动时间：2026-09-26 20:00-22:00。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F27: H4 plus a promo line wrote the promo window"),
    C("K-F27b", "K", "VA",
      "感谢您之前的支持。例行维护：2026-09-23 10:00-12:00 (GMT+8)。新游戏上线："
      "2026-09-25 14:00-16:00 (GMT+8)。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F27: 之前的 across a 。 wrote the launch window"),
    C("K-F27c", "K", "VA",
      "如先前通知。例行维护：2026-09-30 10:00-12:00 (GMT+8)。上周的维护 "
      "2026-09-16 10:00-12:00 已完成。",
      "fill", "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      note="F27: 如先前通知 confirms; was dropped as a completion notice"),

    # F28: 更新时间 ("update time") is not a 新时间 heading.
    C("K-F28", "K", "JDB",
      "例行维护：2026-09-23 10:00-12:00 (GMT+8)\n客户端更新时间：2026-09-24 "
      "02:00-03:00",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F28: wrote the client update hour 09-24 02:00-03:00"),
    C("K-F28b", "K", "JDB",
      "例行维护：2026-09-23 10:00-12:00 (GMT+8)\n版本更新时间：2026-09-23 "
      "10:30-11:00",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F28: wrote the 30-minute sub-window"),
    C("K-F28c", "K", "JDB",
      "例行维护：2026-09-23 10:00-12:00 (GMT+8)\n遊戲更新時間：14:00-15:00",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T15:00:00+08:00",
      note="F28: wrote 14:00-15:00 alone; policy 5 now joins the pieces"),
    C("K-F28d", "K", "JDB",
      "维护时间调整为 2026-09-25 10:00-12:00 (GMT+8)。原定 2026-09-23 "
      "10:00-12:00。\n公告更新时间：2026-09-22 08:00",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F28: a 公告更新时间 footer cut the new window off"),

    # F29: the simplified 原订 and its siblings.
    C("K-F29", "K", "KingMidas",
      "维护时间调整\n原订时间：2026-09-23 10:00-12:00\n调整为：2026-09-25 "
      "10:00-12:00 (GMT+8)",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F29: 原订 was not a superseded marker"),
    C("K-F29b", "K", "KingMidas",
      "例行维护时间变更：原订于 2026-09-23 10:00-12:00 的维护，现改期至 "
      "2026-09-25 10:00-12:00 (GMT+8)",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F29: inline 原订于"),
    C("K-F29c", "K", "KingMidas",
      "维护改期至 2026-09-25 10:00-12:00 (GMT+8)（原订 2026-09-23 10:00-12:00）",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F29: new window first, （原订 old）"),
    C("K-F29d", "K", "JDB",
      "维护时间调整：原先 2026-09-23 10:00-12:00，现调整为 2026-09-25 "
      "10:00-12:00 (GMT+8)",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F29: 原先"),
    C("K-F29e", "K", "JDB",
      "维护时间调整：原安排 2026-09-23 10:00-12:00，现调整为 2026-09-25 "
      "10:00-12:00 (GMT+8)",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F29: 原安排"),

    # F52: the marker that governs the OLD date tainted the NEW window.
    C("K-F52", "K", "Hacksaw",
      "Please note the scheduled maintenance has been moved from 2026-09-23 to "
      "2026-09-25 10:00-12:00 (GMT+8).",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F52: H1 without its comma"),
    C("K-F52b", "K", "VA",
      "维护时间变更：原定 2026-09-23 10:00-12:00 的例行维护改期至 2026-09-25 "
      "10:00-12:00 (GMT+8)",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F52"),
    C("K-F52c", "K", "Hacksaw",
      "Scheduled maintenance has been moved from 2026-09-23 10:00-12:00 to "
      "2026-09-25 10:00-12:00 (GMT+8).",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F52: both windows in one comma-free clause"),
    C("K-F52d", "K", "VA",
      "原本定于2026-09-23 10:00-12:00的维护将延至2026-09-25 10:00-12:00",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F52: 原本定于…延至"),
    C("K-F52e", "K", "Hacksaw",
      "The maintenance originally planned for 2026-09-23 10:00-12:00 has been "
      "postponed to 2026-09-25 10:00-12:00 (GMT+8).",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="F52"),

    # F53: a footer that PROMISES a schedule is not an updated schedule.
    C("K-F53", "K", "Hacksaw",
      "Scheduled maintenance: 2026-09-23 10:00-12:00 (GMT+8). If the work "
      "overruns, an updated schedule will be shared in this group.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      resched=False,
      note="F53: was a false RESCHEDULE needs_human ('the only window found "
           "is the superseded one')"),
    C("K-F53b", "K", "Yggdrasil",
      "Scheduled maintenance: 2026-09-23 10:00-12:00 (GMT+8). Should there be "
      "any changes, an updated schedule will be provided.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      resched=False, note="F53"),
    C("K-F53c", "K", "Evolution",
      "Scheduled maintenance: 2026-09-23 10:00-12:00 (GMT+8). Any revised "
      "schedule will be announced in this group.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      resched=False, note="F53: 'Any revised schedule'"),
    C("K-F53d", "K", "JDB",
      "例行维护：2026-09-23 10:00-12:00 (GMT+8)。如维护延期，将另行通知新的时间。",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      resched=False, note="F53: 如维护延期 is a condition, 新的时间 promised"),

    # F54: "previously announced" confirms; it does not withdraw.
    C("K-F54", "K", "Hacksaw",
      "The maintenance previously announced for 2026-09-23 10:00-12:00 (GMT+8) "
      "will proceed as scheduled. The backoffice will be unavailable "
      "10:00-10:30.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F54: wrote the 30-minute backoffice hour"),
    C("K-F54b", "K", "Hacksaw",
      "Further to our previous notice the scheduled maintenance on 2026-09-23 "
      "10:00-12:00 (GMT+8) will go ahead. Backoffice unavailable 10:00-10:30.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note="F54: 'our previous notice'"),
    C("K-F54c", "K", "Hacksaw",
      "The maintenance previously announced for 2026-09-23 10:00-12:00 (GMT+8) "
      "will proceed as scheduled. The backoffice will be unavailable "
      "11:30-12:30.",
      "fill", "2026-09-23T10:00:00+08:00", "2026-09-23T12:30:00+08:00",
      note="F54: wrote 11:30-12:30 (wrong START); policy 5 joins the pieces"),

    # G2.2: a correction naming both the wrong and the right window.
    C("K-G2.2", "K", "Hacksaw",
      "Sorry, the scheduled maintenance date in the notice above is wrong.\n"
      "Wrong: 24/09/2026 10:00-12:00 (GMT+8)\nCorrect: 26/09/2026 10:00-12:00 "
      "(GMT+8)",
      "fill", "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      resched=True, note="G2.2: wrote the WRONG 09-24"),
    C("K-G2.2b", "K", "Hacksaw",
      "Correction to the scheduled maintenance notice: the maintenance is on "
      "26/09/2026 10:00-12:00 (GMT+8), not 24/09/2026 10:00-12:00.",
      "fill", "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      resched=True, note="G2.2: '…, not X'"),
    C("K-G2.2c", "K", "Hacksaw",
      "UPDATE: Scheduled maintenance time changed.\nNew time: 26/09/2026 "
      "10:00-12:00 (GMT+8)\nOld time: 24/09/2026 10:00-12:00",
      "fill", "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      resched=True, note="G2.2: New time:/Old time:"),
    C("K-G2.2d", "K", "JDB",
      "更正维护通知\n错误：2026-09-24 10:00-12:00\n正确：2026-09-26 10:00-12:00 (GMT+8)",
      "fill", "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      resched=True, note="G2.2: 错误/正确"),
    C("K-G2.2e", "K", "KingMidas",
      "更正維護通知\n錯誤：2026-09-24 10:00-12:00\n正確：2026-09-26 10:00-12:00 (GMT+8)",
      "fill", "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      resched=True, note="G2.2: 錯誤/正確"),
    C("K-G2.2f", "K", "Hacksaw",
      "Correction: the scheduled maintenance on 24/09/2026 will be 14:00-16:00 "
      "(GMT+8), not 10:00-12:00.",
      "fill", "2026-09-24T14:00:00+08:00", "2026-09-24T16:00:00+08:00",
      resched=True, note="G2.2: wrote the union 10:00-16:00"),
    C("K-G2.2g", "K", "JDB",
      "更正：9月24日系统维护时间为 14:00-16:00 (GMT+8)，而非 10:00-12:00。",
      "fill", "2026-09-24T14:00:00+08:00", "2026-09-24T16:00:00+08:00",
      resched=True, note="G2.2: 而非"),
    C("K-G2.2h", "K", "KingMidas",
      "更正：9月24日系統維護時間為 14:00-16:00 (GMT+8)，而非 10:00-12:00。",
      "fill", "2026-09-24T14:00:00+08:00", "2026-09-24T16:00:00+08:00",
      resched=True, note="G2.2: 而非 (繁)"),
    C("K-G2.2i", "K", "Hacksaw",
      "Scheduled maintenance rescheduled from 24/09/2026 10:00-12:00 to "
      "26/09/2026 10:00-12:00 (GMT+8)",
      "fill", "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      resched=True, note="G2.2: 'rescheduled from X to Y'"),
    C("K-G2.2j", "K", "Hacksaw",
      "Postponed\nNew time: 26/09/2026 10:00-12:00 (GMT+8)\nOld time: "
      "24/09/2026 10:00-12:00",
      "fill", "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      resched=True, note="G2.2"),
    C("K-G2.2k", "K", "Hacksaw",
      "Correction: the maintenance notice above is wrong.\nWrong: 24/09/2026 "
      "10:00-12:00 (GMT+8)",
      "needs_human", resched=True, why="the one it withdraws",
      note="G2.2 guard: the only window is the one the correction withdraws"),

    # G4.3: the OPERATOR's request is not the provider's window.
    C("K-G4.3", "K", "Hacksaw",
      "Please reschedule your maintenance to 2026-09-25 03:00-05:00 (GMT+8), we "
      "have a big event on the 24th.",
      "needs_human", resched=False, why="operator request",
      note="G4.3: our request was written as the provider's reschedule"),
    C("K-G4.3b", "K", "Hacksaw",
      "We request that your maintenance be rescheduled to 25/09/2026 "
      "03:00-05:00 GMT+8.", "needs_human", resched=False,
      why="operator request", note="G4.3"),
    C("K-G4.3c", "K", "JDB",
      "麻烦贵司将维护改到2026-09-25 03:00-05:00 (GMT+8)，24号我们有活动。",
      "needs_human", resched=False, why="operator request", note="G4.3"),
    C("K-G4.3d", "K", "JDB",
      "请把你们的维护时间调整至2026-09-25 03:00-05:00 (GMT+8)。",
      "needs_human", resched=False, why="operator request", note="G4.3"),
    C("K-G4.3e", "K", "Hacksaw",
      "Hi team, kindly avoid any deployment on 24/09/2026 02:00-04:00 (GMT+8) "
      "as we will have maintenance.",
      "needs_human", why="operator request",
      note="G4.3: the operator's release freeze was written as an outage"),
    C("K-G4.3f", "K", "Hacksaw",
      "Kindly hold all releases on 2026-09-24 02:00 - 04:00 GMT+8 due to our "
      "system maintenance.", "needs_human", why="operator request",
      note="G4.3"),
    C("K-G4.3g", "K", "Hacksaw",
      "Please stop sending callbacks during our scheduled maintenance "
      "24/09/2026 02:00-04:00 (GMT+8).", "needs_human",
      why="operator request", note="G4.3"),
    C("K-G4.3h", "K", "JDB",
      "请贵司在2026-09-24 02:00-04:00 (GMT+8) 我方维护期间暂停发版。",
      "needs_human", why="operator request", note="G4.3"),
    C("K-G4.3i", "K", "Hacksaw",
      "Can you reschedule your maintenance to 2026-09-25 03:00-05:00 (GMT+8)?",
      "ignore", resched=False, why="question",
      note="G4.3: the operator's QUESTION (the question guard never ran for "
           "reschedules)"),
    C("K-G4.3j", "K", "Hacksaw",
      "Please be informed that our maintenance has been rescheduled to "
      "2026-09-25 10:00-12:00 (GMT+8).",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      resched=True, note="G4.3 control: 'please' + the provider's OWN move"),
    C("K-G4.3k", "K", "Hacksaw",
      "Kindly note the scheduled maintenance on 2026-09-25 10:00-12:00 "
      "(GMT+8). Please avoid placing bets during this period.",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note="G4.3 control: 'avoid placing bets' has no release object"),
    C("K-G4.3l", "K", "JDB",
      "敬请贵司提前做好维护准备：2026-09-25 10:00-12:00 (GMT+8) 例行维护。",
      "fill", "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note="G4.3 control: 提前 is 'in advance', not a move"),
    C("K-G4.3m", "K", "Hacksaw",
      "Kindly hold all releases on 2026-09-24 02:00 - 04:00 GMT+8 due to our "
      "system maintenance.",
      "fill", "2026-09-24T02:00:00+08:00", "2026-09-24T04:00:00+08:00",
      env={"NOTICE_REQUEST_GUARD": "0"},
      note="G4.3: NOTICE_REQUEST_GUARD=0 is the kill switch"),
    C("K-F20i", "K", "Hacksaw",
      "Hi team, can the maintenance be postponed to 2026-09-25 02:00-04:00 "
      "(GMT+8)?",
      "fill", "2026-09-25T02:00:00+08:00", "2026-09-25T04:00:00+08:00",
      resched=True, env={"NOTICE_QUESTION_GUARD": "0"},
      note="F20: NOTICE_QUESTION_GUARD=0 restores the old behaviour"),

    # -- M1: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M1-R1.13-1', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); it has been cancelled.', 'ignore', None, None,
      note='fix-up R1.13: pinned current verdict'),
    C('M1-R1.13-2', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); this has been cancelled.', 'ignore', None, None,
      note='fix-up R1.13: pinned current verdict'),
    C('M1-R1.13-3', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); which was cancelled.', 'ignore', None, None,
      note='fix-up R1.13: pinned current verdict'),
    C('M1-R1.13-4', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); that is cancelled.', 'ignore', None, None,
      note='fix-up R1.13: pinned current verdict'),
    C('M1-R1.13-5', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); has been cancelled.', 'ignore', None, None,
      note='fix-up R1.13: pinned current verdict'),
    C('M1-R1.13-6', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8)； it was cancelled.', 'ignore', None, None,
      note='fix-up R1.13: pinned current verdict'),
    C('M1-R1.13-keep-7', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); unfinished games will be cancelled.', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up R1.13-keep: pinned current verdict'),
    C('M1-R1.14-1', 'H', 'audit', '维护 2026-09-23 10:00-12:00 (GMT+8) 改期到 2026-09-25 10:00-12:00 (GMT+8)。', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up R1.14: pinned current verdict'),
    C('M1-R1.21-1', 'G', 'audit', 'The next maintenance has been cancelled, the next maintenance window was 2026-09-30 10:00 - 12:00 (GMT+8).', 'ignore', None, None,
      note='fix-up R1.21: pinned current verdict'),
    C('M1-R1.21-2', 'G', 'audit', "Next week's maintenance has been cancelled, next week's maintenance window: 2026-09-30 10:00 - 12:00 (GMT+8).", 'ignore', None, None,
      note='fix-up R1.21: pinned current verdict'),
    C('M1-R1.21-3', 'G', 'audit', 'The next scheduled maintenance has been cancelled, next maintenance window 2026-09-30 10:00 - 12:00 (GMT+8).', 'ignore', None, None,
      note='fix-up R1.21: pinned current verdict'),
    C('M1-R1.21-keep-4', 'G', 'audit', '上次维护已完成。下次例行维护将于 2026-09-25 10:00 - 12:00 (GMT+8) 进行。', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up R1.21-keep: pinned current verdict'),
    C('M1-R1.20-1', 'G', 'audit', '我们已取消2026-09-23 10:00-12:00 (GMT+8)的游戏维护。', 'ignore', None, None,
      note='fix-up R1.20: pinned current verdict'),
    C('M1-R1.20-2', 'G', 'audit', '现通知取消2026年9月23日10:00-12:00(GMT+8)的维护。', 'ignore', None, None,
      note='fix-up R1.20: pinned current verdict'),
    C('M1-R1.20-keep-3', 'G', 'audit', '维护时间 2026-09-23 10:00-12:00 (GMT+8)。维护期间未结算注单将取消。', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up R1.20-keep: pinned current verdict'),
    C('M1-R1.39-1', 'H', 'audit', 'Typo in the previous notice. Scheduled maintenance: 26/09/2026 10:00-12:00 (GMT+8), previously stated 24/09/2026 10:00-12:00.', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up R1.39: pinned current verdict'),
    C('M1-R1.42-1', 'H', 'audit', '维护时间：2026-09-26 10:00-12:00 (GMT+8)，之前通知的2026-09-24 10:00-12:00作废。', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up R1.42: pinned current verdict'),
    C('M1-R1.57-1', 'G', 'audit', 'The maintenance on 2026-09-26 10:00-12:00 is on hold.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-2', 'G', 'audit', 'The maintenance on 2026-09-26 10:00-12:00 has been suspended.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-3', 'G', 'audit', 'The maintenance on 2026-09-26 10:00-12:00 will be skipped this week.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-4', 'G', 'audit', 'Maintenance on 2026-09-25 10:00-12:00 is no longer going ahead.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-5', 'G', 'audit', 'The maintenance on 2026-09-26 10:00-12:00 has been put off.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-6', 'G', 'audit', 'Maintenance 2026-09-25 10:00-12:00 is not required anymore.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-7', 'G', 'audit', 'We are not able to do maintenance on 2026-09-26 10:00-12:00.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-8', 'G', 'audit', '2026-09-26 10:00-12:00维护作废', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-9', 'G', 'audit', '2026-09-26 10:00-12:00维护不进行了', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-10', 'G', 'audit', '2026-09-25 10:00-12:00 的维护不做了', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-11', 'G', 'audit', '2026-09-25 10:00-12:00 维护计划撤回', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-12', 'G', 'audit', '取消通知：2026-09-25 10:00-12:00 维护', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-13', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 [VOID]', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-14', 'G', 'audit', 'VOID - Scheduled maintenance 2026-09-26 10:00-12:00', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-15', 'G', 'audit', '(VOID) Scheduled maintenance 2026-09-26 10:00-12:00', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-16', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (void)', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-17', 'G', 'audit', '系统维护 2026-09-26 10:00-12:00【作废】', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-18', 'G', 'audit', '系统维护 2026-09-26 10:00-12:00（作废）', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-19', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 - please ignore, wrong info', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-20', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (IGNORE)', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-21', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (withdrawn)', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-22', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 - retracted', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-23', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (incorrect)', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-24', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (outdated)', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M1-R1.57-25', 'G', 'audit', 'Maintenance on 2026-09-25 10:00-12:00 — delayed, new date TBA', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    # -- M2: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M2-R1.13-1', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); it has been cancelled.', 'ignore', None, None,
      note='fix-up R1.13: pinned current verdict'),
    C('M2-R1.13-2', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); this has been cancelled.', 'ignore', None, None,
      note='fix-up R1.13: pinned current verdict'),
    C('M2-R1.13-3', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); which was cancelled.', 'ignore', None, None,
      note='fix-up R1.13: pinned current verdict'),
    C('M2-R1.13-4', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); that is cancelled.', 'ignore', None, None,
      note='fix-up R1.13: pinned current verdict'),
    C('M2-R1.13-5', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); has been cancelled.', 'ignore', None, None,
      note='fix-up R1.13: pinned current verdict'),
    C('M2-R1.13-6', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8)； it was cancelled.', 'ignore', None, None,
      note='fix-up R1.13: pinned current verdict'),
    C('M2-R1.13-keep-7', 'G', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); unfinished games will be cancelled.', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up R1.13-keep: pinned current verdict'),
    C('M2-R1.14-1', 'H', 'audit', '维护 2026-09-23 10:00-12:00 (GMT+8) 改期到 2026-09-25 10:00-12:00 (GMT+8)。', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up R1.14: pinned current verdict'),
    C('M2-R1.21-1', 'G', 'audit', 'The next maintenance has been cancelled, the next maintenance window was 2026-09-30 10:00 - 12:00 (GMT+8).', 'ignore', None, None,
      note='fix-up R1.21: pinned current verdict'),
    C('M2-R1.21-2', 'G', 'audit', "Next week's maintenance has been cancelled, next week's maintenance window: 2026-09-30 10:00 - 12:00 (GMT+8).", 'ignore', None, None,
      note='fix-up R1.21: pinned current verdict'),
    C('M2-R1.21-3', 'G', 'audit', 'The next scheduled maintenance has been cancelled, next maintenance window 2026-09-30 10:00 - 12:00 (GMT+8).', 'ignore', None, None,
      note='fix-up R1.21: pinned current verdict'),
    C('M2-R1.21-keep-4', 'G', 'audit', '上次维护已完成。下次例行维护将于 2026-09-25 10:00 - 12:00 (GMT+8) 进行。', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up R1.21-keep: pinned current verdict'),
    C('M2-R1.20-1', 'G', 'audit', '我们已取消2026-09-23 10:00-12:00 (GMT+8)的游戏维护。', 'ignore', None, None,
      note='fix-up R1.20: pinned current verdict'),
    C('M2-R1.20-2', 'G', 'audit', '现通知取消2026年9月23日10:00-12:00(GMT+8)的维护。', 'ignore', None, None,
      note='fix-up R1.20: pinned current verdict'),
    C('M2-R1.20-keep-3', 'G', 'audit', '维护时间 2026-09-23 10:00-12:00 (GMT+8)。维护期间未结算注单将取消。', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up R1.20-keep: pinned current verdict'),
    C('M2-R1.39-1', 'H', 'audit', 'Typo in the previous notice. Scheduled maintenance: 26/09/2026 10:00-12:00 (GMT+8), previously stated 24/09/2026 10:00-12:00.', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up R1.39: pinned current verdict'),
    C('M2-R1.42-1', 'H', 'audit', '维护时间：2026-09-26 10:00-12:00 (GMT+8)，之前通知的2026-09-24 10:00-12:00作废。', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up R1.42: pinned current verdict'),
    C('M2-R1.57-1', 'G', 'audit', 'The maintenance on 2026-09-26 10:00-12:00 is on hold.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-2', 'G', 'audit', 'The maintenance on 2026-09-26 10:00-12:00 has been suspended.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-3', 'G', 'audit', 'The maintenance on 2026-09-26 10:00-12:00 will be skipped this week.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-4', 'G', 'audit', 'Maintenance on 2026-09-25 10:00-12:00 is no longer going ahead.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-5', 'G', 'audit', 'The maintenance on 2026-09-26 10:00-12:00 has been put off.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-6', 'G', 'audit', 'Maintenance 2026-09-25 10:00-12:00 is not required anymore.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-7', 'G', 'audit', 'We are not able to do maintenance on 2026-09-26 10:00-12:00.', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-8', 'G', 'audit', '2026-09-26 10:00-12:00维护作废', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-9', 'G', 'audit', '2026-09-26 10:00-12:00维护不进行了', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-10', 'G', 'audit', '2026-09-25 10:00-12:00 的维护不做了', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-11', 'G', 'audit', '2026-09-25 10:00-12:00 维护计划撤回', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-12', 'G', 'audit', '取消通知：2026-09-25 10:00-12:00 维护', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-13', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 [VOID]', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-14', 'G', 'audit', 'VOID - Scheduled maintenance 2026-09-26 10:00-12:00', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-15', 'G', 'audit', '(VOID) Scheduled maintenance 2026-09-26 10:00-12:00', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-16', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (void)', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-17', 'G', 'audit', '系统维护 2026-09-26 10:00-12:00【作废】', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-18', 'G', 'audit', '系统维护 2026-09-26 10:00-12:00（作废）', 'ignore', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-19', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 - please ignore, wrong info', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-20', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (IGNORE)', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-21', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (withdrawn)', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-22', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 - retracted', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-23', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (incorrect)', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-24', 'G', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (outdated)', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.57-25', 'G', 'audit', 'Maintenance on 2026-09-25 10:00-12:00 — delayed, new date TBA', 'needs_human', None, None,
      note='fix-up R1.57: pinned current verdict'),
    C('M2-R1.61-1', 'F', 'audit', 'Please cancel the maintenance on 2026-09-26 10:00-12:00', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-2', 'F', 'audit', 'Please cancel your maintenance on 2026-09-26 10:00-12:00, we have a big event.', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-3', 'F', 'audit', 'Our preferred maintenance window is 2026-09-26 10:00-12:00', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-4', 'F', 'audit', 'Suggest maintenance time: 2026-09-26 10:00-12:00', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-5', 'F', 'audit', 'Proposed maintenance window: 2026-09-26 10:00-12:00 (GMT+8). Please confirm if OK.', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-6', 'F', 'audit', 'Proposed maintenance: 2026-09-26 10:00-12:00, pending your approval.', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-7', 'F', 'audit', 'Tentative maintenance 2026-09-26 10:00-12:00 (TBC)', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-8', 'F', 'audit', 'Maintenance 2026-09-26 10:00-12:00 (to be confirmed)', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-9', 'F', 'audit', 'Planned maintenance 2026-09-26 10:00-12:00 — tentative, final schedule to follow', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-10', 'F', 'audit', 'There might be an emergency maintenance 2026-09-26 10:00-12:00, will update.', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-11', 'F', 'audit', 'Possible maintenance on 2026-09-26 10:00-12:00, not confirmed yet.', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-12', 'F', 'audit', '预计2026-09-26 10:00-12:00维护，具体时间待定', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-13', 'F', 'audit', '初步定于2026-09-26 10:00-12:00维护，待确认', 'needs_human', None, None,
      note='fix-up R1.61: pinned current verdict'),
    C('M2-R1.61-keep-14', 'F', 'audit', '系统维护通知\n维护时间：2026-09-26 10:00-12:00 (GMT+8)\n预计维护2小时，请提前做好准备。', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up R1.61-keep: pinned current verdict'),
    C('M2-R1.61-keep-15', 'F', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (GMT+8). There might be a short disconnection. Please log in again as soon as possible.', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up R1.61-keep: pinned current verdict'),
    C('M2-R1.61-keep-16', 'F', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (GMT+8). The time is subject to change.', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up R1.61-keep: pinned current verdict'),
    C('M2-R1.62-1', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 UK time', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-2', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 Central European Summer Time', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-3', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 Central European Time', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-4', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 (Malta)', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-5', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 (London)', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-6', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 (Sofia)', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-7', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 (Europe/Malta)', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-8', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 (Europe/London)', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-9', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 (America/New_York)', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-10', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 Eastern Time', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-11', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 ET', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-12', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 CT', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-13', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 server time', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-14', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 system time', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-15', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 platform time', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-16', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 our time', 'needs_human', None, None,
      note='fix-up R1.62: pinned current verdict'),
    C('M2-R1.62-seoul-17', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 (Seoul)', 'needs_human', None, None,
      note='fix-up R1.62-seoul: pinned current verdict'),
    C('M2-R1.62-z-18', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 Z', 'needs_human', None, None,
      note='fix-up R1.62-z: pinned current verdict'),
    C('M2-R1.62-5.5-19', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 UTC+5.5', 'fill', "2026-09-25T10:00:00+05:30", "2026-09-25T12:00:00+05:30",
      note='fix-up R1.62-5.5: pinned current verdict'),
    C('M2-R1.62-keep-20', 'D', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 (GMT+8) local time', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up R1.62-keep: pinned current verdict'),
    C('M2-R1.63-1', 'I', 'audit', 'Scheduled maintenance\nEurope region: 2026-09-24 02:00-04:00 (GMT+8)\nAsia region: 2026-09-25 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up R1.63: pinned current verdict'),
    C('M2-R1.63-2', 'I', 'audit', 'Scheduled maintenance\nAsia region: 2026-09-25 10:00-12:00 (GMT+8)\nEurope region: 2026-09-24 02:00-04:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up R1.63: pinned current verdict'),
    C('M2-R1.65-1', 'C', 'audit', 'Scheduled maintenance 2026-09-25 12:00-10:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up R1.65: pinned current verdict'),
    C('M2-R1.65-2', 'C', 'audit', 'Scheduled maintenance 2026-09-25 10:00-10:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up R1.65: pinned current verdict'),
    C('M2-R1.65-3', 'C', 'audit', 'Scheduled maintenance on 2026-09-25 10:00 AM - 12:00 AM (GMT+8)', 'needs_human', None, None,
      note='fix-up R1.65: pinned current verdict'),
    C('M2-R1.65-keep-4', 'C', 'audit', 'Scheduled maintenance 2026-09-25 23:00-01:00 (GMT+8)', 'fill', "2026-09-25T23:00:00+08:00", "2026-09-26T01:00:00+08:00",
      note='fix-up R1.65-keep: pinned current verdict'),
    C('M2-R1.65-keep-5', 'C', 'audit', 'Scheduled maintenance 2026-09-25 22:00-06:00 (GMT+8)', 'fill', "2026-09-25T22:00:00+08:00", "2026-09-26T06:00:00+08:00",
      note='fix-up R1.65-keep: pinned current verdict'),
    C('M2-R1.65-keep-6', 'C', 'audit', '维护时间：2026年9月25日 14:00 至 次日02:00 (GMT+8)', 'fill', "2026-09-25T14:00:00+08:00", "2026-09-26T02:00:00+08:00",
      note='fix-up R1.65-keep: pinned current verdict'),
    C('M2-R1.66-1', 'F', 'audit', 'TEST: Scheduled maintenance 2026-09-26 10:00-12:00', 'ignore', None, None,
      note='fix-up R1.66: pinned current verdict'),
    C('M2-R1.66-2', 'F', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 [TEST]', 'ignore', None, None,
      note='fix-up R1.66: pinned current verdict'),
    C('M2-R1.66-3', 'F', 'audit', '测试：系统维护 2026-09-26 10:00-12:00', 'ignore', None, None,
      note='fix-up R1.66: pinned current verdict'),
    C('M2-R1.66-4', 'F', 'audit', '测试消息 系统维护 2026-09-26 10:00-12:00', 'ignore', None, None,
      note='fix-up R1.66: pinned current verdict'),
    C('M2-R1.66-5', 'F', 'audit', '[Test] Scheduled maintenance 2026-09-25 10:00-12:00 (test message please ignore)', 'ignore', None, None,
      note='fix-up R1.66: pinned current verdict'),
    C('M2-R1.66-6', 'F', 'audit', 'Example: Scheduled maintenance 2026-09-25 10:00-12:00', 'ignore', None, None,
      note='fix-up R1.66: pinned current verdict'),
    C('M2-R1.66-7', 'F', 'audit', 'This is a sample template: Scheduled maintenance e.g. 2026-09-25 10:00-12:00', 'ignore', None, None,
      note='fix-up R1.66: pinned current verdict'),
    C('M2-R1.66-keep-8', 'F', 'audit', 'Scheduled maintenance 2026-09-26 10:00-12:00 (GMT+8). Please test your integration after 12:00.', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up R1.66-keep: pinned current verdict'),
    C('M2-R1.67-1', 'I', 'audit', 'Scheduled maintenance 2026-09-25 10:00-12:00. The live chat will be offline 2026-09-25 09:00-13:00.', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up R1.67: pinned current verdict'),
    # -- M3: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M3-R1.71-1', 'I', 'audit', '因系统维护，投注与提款将于2026年10月14日 10:00-12:00 (GMT+8) 暂停。', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.71: pinned current verdict'),
    C('M3-R1.71-2', 'I', 'audit', 'Due to scheduled maintenance, bets and deposits will be suspended on 2026-10-14 10:00 - 12:00 (GMT+8).', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.71: pinned current verdict'),
    C('M3-R1.71-3', 'I', 'audit', 'Please be informed that all games and jackpots will be unavailable on 2026-10-14 from 10:00 to 12:00 (GMT+8) due to scheduled maintenance.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.71: pinned current verdict'),
    C('M3-R1.71-4', 'I', 'audit', 'Scheduled maintenance\nAffected Games: Fortune Gems, Super Ace, Jackpot Fishing\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.71: pinned current verdict'),
    C('M3-R1.71-5', 'I', 'audit', 'Scheduled maintenance\n影响范围：所有游戏、投注及充值提款\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.71: pinned current verdict'),
    C('M3-R1.71-6', 'I', 'audit', 'Scheduled maintenance\nKindly inform your players and customer service team of the schedule below:\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.71: pinned current verdict'),
    C('M3-R1.71-7', 'I', 'audit', '【系统维护】\n维护期间将暂停所有投注：2026年10月14日 10:00-12:00 (GMT+8)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.71: pinned current verdict'),
    C('M3-R1.71-8', 'I', 'audit', 'Scheduled maintenance\nProducts: Slots / Fishing / Bingo / Jackpot\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.71: pinned current verdict'),
    C('M3-R1.71-9', 'I', 'audit', 'Scheduled maintenance\nImpact: bets, deposits and withdrawals\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.71: pinned current verdict'),
    C('M3-R1.71-10', 'I', 'audit', 'Scheduled maintenance\nDuring this period, players will not be able to place bets:\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.71: pinned current verdict'),
    C('M3-R1.71-11', 'I', 'audit', 'Scheduled maintenance\nPlease do not hesitate to contact our support team.\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.71: pinned current verdict'),
    C('M3-R1.71-12', 'I', 'audit', '系统维护通知\n如有任何问题，请联系我们的客服。\n日期：2026年10月14日\n时间：10:00-12:00 (GMT+8)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.71: pinned current verdict'),
    C('M3-R1.72-1', 'F', 'audit', 'Dear Partners,\nPlease be informed that we will perform scheduled maintenance on our platform to improve stability and performance for all partners.\nDate: 2026-10-14 (Wed)\nTime: 10:00 - 12:00 (GMT+8)\nAffected games: All games\nDuring the maintenance, all games, the back office and the API will be unavailable. We apologize for any inconvenience caused.\nCould you please inform your end users about this maintenance?\nBest regards,\nOps Team', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.72: pinned current verdict'),
    C('M3-R1.72-2', 'F', 'audit', 'Dear Partners,\nPlease be informed that we will perform scheduled maintenance on our platform to improve stability and performance for all partners.\nDate: 2026-10-14 (Wed)\nTime: 10:00 - 12:00 (GMT+8)\nAffected games: All games\nDuring the maintenance, all games, the back office and the API will be unavailable. We apologize for any inconvenience caused.\nCould you please kindly help to inform your valued players about the maintenance?\nBest regards,\nOps Team', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.72: pinned current verdict'),
    C('M3-R1.72-3', 'F', 'audit', 'Dear Partners,\nPlease be informed that we will perform scheduled maintenance on our platform to improve stability and performance for all partners.\nDate: 2026-10-14 (Wed)\nTime: 10:00 - 12:00 (GMT+8)\nAffected games: All games\nDuring the maintenance, all games, the back office and the API will be unavailable. We apologize for any inconvenience caused.\nCould you please arrange to inform your players of the maintenance?\nBest regards,\nOps Team', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.72: pinned current verdict'),
    C('M3-R1.72-4', 'F', 'audit', 'Dear Partners,\nPlease be informed that we will perform scheduled maintenance on our platform to improve stability and performance for all partners.\nDate: 2026-10-14 (Wed)\nTime: 10:00 - 12:00 (GMT+8)\nAffected games: All games\nDuring the maintenance, all games, the back office and the API will be unavailable. We apologize for any inconvenience caused.\nWould you mind informing your players about the maintenance?\nBest regards,\nOps Team', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.72: pinned current verdict'),
    C('M3-R1.72-5', 'F', 'audit', 'Dear Partners,\nPlease be informed that we will perform scheduled maintenance on our platform to improve stability and performance for all partners.\nDate: 2026-10-14 (Wed)\nTime: 10:00 - 12:00 (GMT+8)\nAffected games: All games\nDuring the maintenance, all games, the back office and the API will be unavailable. We apologize for any inconvenience caused.\nCould you kindly broadcast this maintenance to your players?\nBest regards,\nOps Team', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.72: pinned current verdict'),
    C('M3-R1.72-6', 'F', 'audit', 'Dear Partners,\nPlease be informed that we will perform scheduled maintenance on our platform to improve stability and performance for all partners.\nDate: 2026-10-14 (Wed)\nTime: 10:00 - 12:00 (GMT+8)\nAffected games: All games\nDuring the maintenance, all games, the back office and the API will be unavailable. We apologize for any inconvenience caused.\nDo you have any questions about the maintenance?\nBest regards,\nOps Team', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.72: pinned current verdict'),
    C('M3-R1.72-7', 'F', 'audit', 'Dear Partners,\nPlease be informed that we will perform scheduled maintenance on our platform to improve stability and performance for all partners.\nDate: 2026-10-14 (Wed)\nTime: 10:00 - 12:00 (GMT+8)\nAffected games: All games\nDuring the maintenance, all games, the back office and the API will be unavailable. We apologize for any inconvenience caused.\nAny questions regarding this maintenance?\nBest regards,\nOps Team', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.72: pinned current verdict'),
    C('M3-R1.72-8', 'F', 'audit', 'Dear Partners,\nPlease be informed that we will perform scheduled maintenance on our platform to improve stability and performance for all partners.\nDate: 2026-10-14 (Wed)\nTime: 10:00 - 12:00 (GMT+8)\nAffected games: All games\nDuring the maintenance, all games, the back office and the API will be unavailable. We apologize for any inconvenience caused.\nAny concerns about the downtime?\nBest regards,\nOps Team', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.72: pinned current verdict'),
    C('M3-R1.72-9', 'F', 'audit', 'Dear Partners,\nPlease be informed that we will perform scheduled maintenance on our platform to improve stability and performance for all partners.\nDate: 2026-10-14 (Wed)\nTime: 10:00 - 12:00 (GMT+8)\nAffected games: All games\nDuring the maintenance, all games, the back office and the API will be unavailable. We apologize for any inconvenience caused.\nWill you be able to inform your players before 10:00?\nBest regards,\nOps Team', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.72: pinned current verdict'),
    C('M3-R1.72-10', 'F', 'audit', 'Dear Partners,\nPlease be informed that we will perform scheduled maintenance on our platform to improve stability and performance for all partners.\nDate: 2026-10-14 (Wed)\nTime: 10:00 - 12:00 (GMT+8)\nAffected games: All games\nDuring the maintenance, all games, the back office and the API will be unavailable. We apologize for any inconvenience caused.\nCan you acknowledge by 09:00?\nBest regards,\nOps Team', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.72: pinned current verdict'),
    C('M3-R1.72-11', 'F', 'audit', 'Dear Partners,\nPlease be informed that we will perform scheduled maintenance on our platform to improve stability and performance for all partners.\nDate: 2026-10-14 (Wed)\nTime: 10:00 - 12:00 (GMT+8)\nAffected games: All games\nDuring the maintenance, all games, the back office and the API will be unavailable. We apologize for any inconvenience caused.\n可以帮忙通知玩家这次维护吗？\nBest regards,\nOps Team', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.72: pinned current verdict'),
    C('M3-R1.72-12', 'F', 'audit', 'Dear Partners,\nPlease be informed that we will perform scheduled maintenance on our platform to improve stability and performance for all partners.\nDate: 2026-10-14 (Wed)\nTime: 10:00 - 12:00 (GMT+8)\nAffected games: All games\nDuring the maintenance, all games, the back office and the API will be unavailable. We apologize for any inconvenience caused.\n可否请贵司协助通知玩家本次维护？\nBest regards,\nOps Team', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.72: pinned current verdict'),
    C('M3-R1.72-keep-13', 'F', 'audit', '请问贵司下次的维护时间是 2026年9月24日 10:00-12:00 (GMT+8) 吗？', 'ignore', None, None,
      note='fix-up R1.72-keep: pinned current verdict'),
    C('M3-R1.72-keep-14', 'F', 'audit', 'Hi team, could you confirm your maintenance window on 2026-09-24 01:00 - 02:00 (GMT+8)?', 'ignore', None, None,
      note='fix-up R1.72-keep: pinned current verdict'),
    C('M3-R1.73-1', 'E', 'audit', 'Scheduled maintenance on 2026-10-14 10:00 - 12:00 (GMT+8), UAT is not affected.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.73: pinned current verdict'),
    C('M3-R1.73-2', 'E', 'audit', 'Scheduled maintenance on 2026-10-14 10:00 - 12:00 (GMT+8) for production only, UAT is not affected.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.73: pinned current verdict'),
    C('M3-R1.73-3', 'E', 'audit', 'Scheduled maintenance 2026-10-14 10:00 - 12:00 (GMT+8) (UAT not affected)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.73: pinned current verdict'),
    C('M3-R1.73-4', 'E', 'audit', 'Scheduled maintenance on 2026-10-14 10:00 - 12:00 (GMT+8), staging remains available.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.73: pinned current verdict'),
    C('M3-R1.73-5', 'E', 'audit', 'Scheduled maintenance\nTime: 2026-10-14 10:00 - 12:00 (GMT+8) The test environment will not be affected.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.73: pinned current verdict'),
    C('M3-R1.73-keep-6', 'E', 'audit', 'UAT server maintenance 2026-10-14 10:00 - 12:00 (GMT+8). Production is not affected.', 'ignore', None, None,
      note='fix-up R1.73-keep: pinned current verdict'),
    C('M3-R1.74-1', 'F', 'audit', 'Dear partners, we will perform scheduled maintenance on 2026-10-14 from 10:00 to 12:00 (GMT+8). The back office will have no downtime. Thank you.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.74: pinned current verdict'),
    C('M3-R1.74-2', 'F', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)\nThere will be no service interruption to the back office.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.74: pinned current verdict'),
    C('M3-R1.74-3', 'F', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)\nThere is no downtime for the reporting API.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.74: pinned current verdict'),
    C('M3-R1.74-4', 'F', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)\nBack office: no service interruption.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.74: pinned current verdict'),
    C('M3-R1.74-5', 'F', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)\nThe back office will remain available with no service interruption.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.74: pinned current verdict'),
    C('M3-R1.74-keep-6', 'F', 'audit', 'System Upgrade 2026-09-24 10:00 - 11:00 (GMT+8). No downtime.', 'ignore', None, None,
      note='fix-up R1.74-keep: pinned current verdict'),
    C('M3-R1.74-keep-7', 'F', 'audit', 'New game launch 2026-10-14 10:00 - 12:00 GMT+8. There will be no service interruption.', 'ignore', None, None,
      note='fix-up R1.74-keep: pinned current verdict'),
    C('M3-R1.75-1', 'G', 'audit', 'Dear partners, we will perform scheduled maintenance on 2026-10-14 from 10:00 to 12:00 (GMT+8). Maintenance completed notice will be sent afterwards. Thank you.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.75: pinned current verdict'),
    C('M3-R1.75-2', 'G', 'audit', '系统维护通知\n维护时间：2026年10月14日 10:00-12:00 (GMT+8)\nA maintenance completed notification will be posted in this group.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.75: pinned current verdict'),
    C('M3-R1.75-keep-3', 'G', 'audit', 'The scheduled maintenance on 2026-10-14 10:00 - 12:00 (GMT+8) has been completed.', 'ignore', None, None,
      note='fix-up R1.75-keep: pinned current verdict'),
    # -- M4: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M4-R1.76-1', 'G', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)\nIf you have already received this notice, please ignore it.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.76: pinned current verdict'),
    C('M4-R1.76-2', 'G', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)\n请忽略之前的通知，以此为准。', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.76: pinned current verdict'),
    C('M4-R1.76-3', 'G', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)\n如已收到，请忽略此消息。', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.76: pinned current verdict'),
    C('M4-R1.76-4', 'G', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)\nPlease ignore the earlier draft.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.76: pinned current verdict'),
    C('M4-R1.76-5', 'G', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)\nThe earlier notice is void.', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.76: pinned current verdict'),
    C('M4-R1.76-6', 'G', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)\n以此公告为准，之前的公告作废。', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.76: pinned current verdict'),
    C('M4-R1.76-7', 'G', 'audit', 'Updated notice (please disregard the previous message):\nScheduled maintenance 2026-10-14 10:00 - 12:00 (GMT+8)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.76: pinned current verdict'),
    C('M4-R1.76-keep-8', 'G', 'audit', '請忽略上面 2026年9月26日 02:00-06:00 (GMT+8) 的維護通知，發錯群了，抱歉！', 'needs_human', None, None,
      note='fix-up R1.76-keep: pinned current verdict'),
    C('M4-R1.76-keep-9', 'G', 'audit', 'Please ignore the scheduled maintenance notice for 24/09/2026 10:00-12:00 (GMT+8); it was sent to the wrong group.', 'needs_human', None, None,
      note='fix-up R1.76-keep: pinned current verdict'),
    C('M4-R1.76-keep-10', 'G', 'audit', 'Scheduled maintenance 2026-10-14 10:00 - 12:00 (GMT+8). Sorry, wrong group, please ignore this message.', 'needs_human', None, None,
      note='fix-up R1.76-keep: pinned current verdict'),
    C('M4-R1.77-1', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 GMT+8 (HKT/SGT)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-2', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (HKT/SGT)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-3', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (SGT/MYT)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-4', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (PHT/SGT)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-5', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8, SGT)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-6', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (SGT & HKT)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-7', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (CST/HKT)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-8', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (BJT)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-9', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (PHST)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-10', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8 / PHT)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-11', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (UTC+8 / SGT)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-12', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8, HKT)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-13', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 GMT+8 / PST', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-14', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8/PST)', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-15', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 GMT+8 PST', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-16', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 PHST', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up R1.77: pinned current verdict'),
    C('M4-R1.77-keep-17', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8 / CEST)', 'needs_human', None, None,
      note='fix-up R1.77-keep: pinned current verdict'),
    # -- M5: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M5-R1.44-1', 'G', 'audit', '取消2026-09-25 10:00 - 12:00的维护', 'ignore', None, None,
      note='fix-up R1.44: pinned current verdict'),
    C('M5-R1.44-2', 'G', 'audit', '撤销2026-09-25 10:00-12:00维护', 'ignore', None, None,
      note='fix-up R1.44: pinned current verdict'),
    C('M5-R1.44-3', 'G', 'audit', '取消9月25日10:00-12:00的维护', 'ignore', None, None,
      note='fix-up R1.44: pinned current verdict'),
    C('M5-R1.44-4', 'G', 'audit', '2026-09-25 10:00-12:00 维护已撤回', 'ignore', None, None,
      note='fix-up R1.44: pinned current verdict'),
    C('M5-R1.44-5', 'G', 'audit', 'Cancel maintenance 2026-09-25 10:00 - 12:00 (GMT+8)', 'ignore', None, None,
      note='fix-up R1.44: pinned current verdict'),
    C('M5-R1.44-6', 'G', 'audit', 'Maintenance 2026-09-25 10:00 - 12:00 (GMT+8) cancel', 'ignore', None, None,
      note='fix-up R1.44: pinned current verdict'),
    C('M5-R1.45-1', 'G', 'audit', 'Maintenance 2026-09-25 10:00 - 12:00 (GMT+8) delayed, new time TBA', 'needs_human', None, None,
      note='fix-up R1.45: pinned current verdict'),
    C('M5-R1.45-2', 'G', 'audit', 'Maintenance 2026-09-25 10:00 - 12:00 (GMT+8) void', 'needs_human', None, None,
      note='fix-up R1.45: pinned current verdict'),
    C('M5-R1.45-3', 'G', 'audit', 'Maintenance 2026-09-25 10:00 - 12:00 (GMT+8) is not needed anymore', 'ignore', None, None,
      note='fix-up R1.45: pinned current verdict'),
    C('M5-R1.45-4', 'G', 'audit', '2026-09-25 10:00-12:00 维护暂停', 'ignore', None, None,
      note='fix-up R1.45: pinned current verdict'),
    C('M5-R1.45-keep-5', 'G', 'audit', '系统维护通知\n维护时间：2026-09-25 10:00-12:00 (GMT+8)\n维护期间暂停服务，请知悉。', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up R1.45-keep: pinned current verdict'),
    C('M5-R1.30-1', 'G', 'audit', '[VOID] Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8). An updated schedule will be shared.', 'needs_human', None, None,
      note='fix-up R1.30: pinned current verdict'),
    C('M5-R1.30-2', 'G', 'audit', 'Please disregard the previous maintenance notice for 2026-09-23 10:00-12:00 (GMT+8); an updated schedule will be shared.', 'needs_human', None, None,
      note='fix-up R1.30: pinned current verdict'),
    C('M5-R1.30-3', 'G', 'audit', 'Please ignore the above notice (scheduled maintenance 2026-09-23 10:00-12:00 GMT+8). A revised schedule will follow.', 'needs_human', None, None,
      note='fix-up R1.30: pinned current verdict'),
    C('M5-R1.30-4', 'G', 'audit', 'Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8) will be moved. An updated schedule will be shared.', 'needs_human', None, None,
      note='fix-up R1.30: pinned current verdict'),
    C('M5-R1.30-5', 'G', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8) postponed. An updated schedule will be shared.', 'needs_human', None, None,
      note='fix-up R1.30: pinned current verdict'),
    C('M5-R1.30-6', 'G', 'audit', 'Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8) — change of plan, an updated schedule will be shared shortly.', 'needs_human', None, None,
      note='fix-up R1.30: pinned current verdict'),
    C('M5-R1.30-keep-7', 'G', 'audit', 'Scheduled maintenance: 2026-09-23 10:00-12:00 (GMT+8). If the work overruns, an updated schedule will be shared in this group.', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up R1.30-keep: pinned current verdict'),
    C('M5-R1.37-1', 'H', 'audit', '原定 2026-09-23 10:00-12:00 (GMT+8) 的维护暂停，新的时间将另行通知。', 'ignore', None, None,
      note='fix-up R1.37: pinned current verdict'),
    C('M5-R1.37-2', 'H', 'audit', '原计划 2026-09-23 10:00-12:00 (GMT+8) 的维护暂停，新的时间将另行通知。', 'ignore', None, None,
      note='fix-up R1.37: pinned current verdict'),
    C('M5-R1.37-3', 'H', 'audit', '2026-09-23 10:00-12:00 (GMT+8) 的维护暂停。', 'ignore', None, None,
      note='fix-up R1.37: pinned current verdict'),
    C('M5-R1.37-4', 'H', 'audit', 'Maintenance on 2026-09-23 10:00-12:00 (GMT+8) delayed. New time to be announced.', 'needs_human', None, None,
      note='fix-up R1.37: pinned current verdict'),
    C('M5-R1.37-5', 'H', 'audit', 'Maintenance\nOriginal date: 25/09/2026 14:00-16:00 (GMT+8)\nNew date: to be announced', 'needs_human', None, None,
      note='fix-up R1.37: pinned current verdict'),
    C('M5-R1.41-1', 'H', 'audit', 'Correction: scheduled maintenance time is 14:00-16:00 (GMT+8) on 24/09/2026. 10:00-12:00 was wrong.', 'fill', "2026-09-24T14:00:00+08:00", "2026-09-24T16:00:00+08:00",
      note='fix-up R1.41: pinned current verdict'),
    C('M5-R1.40-1', 'H', 'audit', 'Scheduled maintenance update: 26/09/2026 10:00-12:00 (GMT+8). Previously announced: 24/09/2026 10:00-12:00.', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up R1.40: pinned current verdict'),
    C('M5-R1.34-1', 'H', 'audit', 'Scheduled maintenance previously announced for 2026-09-23 10:00-12:00 (GMT+8) has changed. New time: 2026-09-25 10:00-12:00 (GMT+8).', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up R1.34: pinned current verdict'),
    C('M5-R1.34-2', 'H', 'audit', 'Scheduled maintenance previously notified: 2026-09-23 10:00-12:00 (GMT+8). Updated: 2026-09-25 10:00-12:00 (GMT+8).', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up R1.34: pinned current verdict'),
    C('M5-R1.35-1', 'H', 'audit', 'Scheduled maintenance previously announced for 2026-09-23 10:00-12:00 (GMT+8) has changed. New time: 2026-09-23 14:00-16:00 (GMT+8).', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up R1.35: pinned current verdict'),
    C('M5-R1.35-2', 'H', 'audit', '之前通知的例行维护 2026-09-23 10:00-12:00 (GMT+8) 有变动，最新时间：2026-09-23 14:00-16:00 (GMT+8)。', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up R1.35: pinned current verdict'),
    C('M5-R1.36-1', 'H', 'audit', '【维护时间更新】\n例行维护：2026-09-23 10:00-12:00 (GMT+8)\n最新时间：2026-09-23 14:00-16:00 (GMT+8)', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up R1.36: pinned current verdict'),
    C('M5-R1.36-2', 'H', 'audit', '例行维护：2026-09-23 10:00-12:00 (GMT+8)\n最新时间：2026-09-25 10:00-12:00 (GMT+8)', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up R1.36: pinned current verdict'),
    C('M5-R1.34-keep-3', 'H', 'audit', 'The maintenance previously announced for 2026-09-23 10:00-12:00 (GMT+8) will proceed as scheduled.', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up R1.34-keep: pinned current verdict'),
    # -- M6: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M6-R1.10-1', 'I', 'audit', 'Scheduled maintenance\nDate: 2026-09-23\nTime: 14:00 - 16:00 (GMT+8)\nSupport: 08:00 - 10:00', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up R1.10: pinned current verdict'),
    C('M6-R1.10-2', 'I', 'audit', 'Scheduled maintenance\nDate: 2026-09-23\nTime: 14:00 - 16:00 (GMT+8)\nHotline: 08:00 - 10:00', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up R1.10: pinned current verdict'),
    C('M6-R1.10-3', 'I', 'audit', 'Scheduled maintenance\nDate: 2026-09-23\nTime: 14:00 - 16:00 (GMT+8)\nCutoff: 08:00 - 10:00', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up R1.10: pinned current verdict'),
    C('M6-R1.10-4', 'I', 'audit', 'Scheduled maintenance\nDate: 2026-09-23\nTime: 14:00 - 16:00 (GMT+8)\nReport period: 00:00 - 06:00', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up R1.10: pinned current verdict'),
    C('M6-R1.10-5', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 14:00-16:00 (GMT+8).\nSupport: 08:00 - 10:00', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up R1.10: pinned current verdict'),
    C('M6-R1.11-1', 'D', 'audit', '维护公告\n日期：2026年9月23日\n北京时间：10:00-12:00\nUTC时间：02:00-04:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up R1.11: pinned current verdict'),
    C('M6-R1.11-2', 'D', 'audit', '例行维护\n北京时间：10:00-12:00\nUTC时间：02:00-04:00\n日期：2026年9月23日', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up R1.11: pinned current verdict'),
    C('M6-R1.12-1', 'D', 'audit', 'Scheduled maintenance\nTime (GMT+8): 01:00 - 03:00\nTime (UTC): 17:00 - 19:00\nDate: 2026-09-23', 'needs_human', None, None,
      note='fix-up R1.12: pinned current verdict'),
    C('M6-R1.19-1', 'C', 'audit', 'Scheduled maintenance (GMT+8)\nPhase 1: 2026-09-23 22:00 - 2026-09-24 02:00\nPlease plan accordingly.', 'fill', "2026-09-23T22:00:00+08:00", "2026-09-24T02:00:00+08:00",
      note='fix-up R1.19: pinned current verdict'),
    C('M6-R1.25-1', 'D', 'audit', 'Scheduled maintenance: 2026-09-24 22:00 (GMT+8) - 2026-09-25 02:00 (GMT+7)', 'needs_human', None, None,
      note='fix-up R1.25: pinned current verdict'),
    C('M6-R1.25-2', 'D', 'audit', 'Scheduled maintenance\nStart: 2026-09-24 14:00 (UTC)\nEnd: 2026-09-24 22:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up R1.25: pinned current verdict'),
    C('M6-R1.25-3', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-09-24\nStart: 10:00 (UTC)\nEnd: 18:00 (GMT+8)', 'needs_human', None, None,
      note='F43: the two ends carry different zones (UTC / GMT+8) - the M6 pin recorded the wrong 10:00-18:00 UTC write'),
    C('M6-R1.26-1', 'C', 'audit', '系统维护通知\n维护时间：2026年9月23日 次日 02:00 至 次日 04:00', 'fill', "2026-09-24T02:00:00+08:00", "2026-09-24T04:00:00+08:00",
      note='fix-up R1.26: pinned current verdict'),
    C('M6-R1.26-2', 'C', 'audit', '系统维护通知\n维护时间：2026年9月24日 23:00 至 次日 01:00，次日 09:00 到 10:00 为观察期', 'fill', "2026-09-24T23:00:00+08:00", "2026-09-25T01:00:00+08:00",
      note='fix-up R1.26: pinned current verdict'),
    C('M6-R1.27-1', 'I', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 (GMT+8), estimated to last 2 hours; support available 09:00 through 18:00.', 'ignore', None, None,
      note='fix-up R1.27: pinned current verdict'),
    C('M6-R1.29-1', 'C', 'audit', 'Scheduled maintenance on 25.09.2026 10.00 - 12.00 hrs (GMT+8)', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up R1.29: pinned current verdict'),
    C('M6-R1.29-keep-2', 'C', 'audit', 'Scheduled maintenance on 25.09.2026, estimated 2.00 - 4.00 hrs.', 'ignore', None, None,
      note='fix-up R1.29-keep: pinned current verdict'),
    C('M6-R1.4-1', 'F', 'audit', 'Evolution maintenance 25/09/2026 14:00-16:00 (GMT+8) will not affect gameplay.', 'ignore', None, None,
      note='fix-up R1.4: pinned current verdict'),
    C('M6-R1.6-1', 'C', 'audit', 'Scheduled maintenance on Sep 25, 2026 from 4:00 PM - 6:00 (next day) (GMT+8)', 'fill', "2026-09-25T16:00:00+08:00", "2026-09-26T06:00:00+08:00",
      note='fix-up R1.6: pinned current verdict'),
    C('M6-R1.6-2', 'C', 'audit', 'Scheduled maintenance on Sep 25, 2026 from 6:00 PM - 8:00 next day (GMT+8)', 'fill', "2026-09-25T18:00:00+08:00", "2026-09-26T08:00:00+08:00",
      note='fix-up R1.6: pinned current verdict'),
    C('M6-R1.7-1', 'D', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 PST (Pacific Standard Time)', 'fill', "2026-09-24T10:00:00-08:00", "2026-09-24T12:00:00-08:00",
      note='fix-up R1.7: pinned current verdict'),
    C('M6-R1.7-2', 'D', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 PST (US Pacific)', 'fill', "2026-09-24T10:00:00-08:00", "2026-09-24T12:00:00-08:00",
      note='fix-up R1.7: pinned current verdict'),
    C('M6-R1.7-3', 'D', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 PST\nPST = Pacific Standard Time', 'fill', "2026-09-24T10:00:00-08:00", "2026-09-24T12:00:00-08:00",
      note='fix-up R1.7: pinned current verdict'),
    C('M6-R1.15-1', 'G', 'audit', 'Reminder: Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). If you have already received this notice, please ignore it.', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up R1.15: pinned current verdict'),
    C('M6-R1.15-2', 'G', 'audit', 'Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). Please ignore the earlier notice with the wrong date.', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up R1.15: pinned current verdict'),
    C('M6-R1.16-1', 'H', 'audit', '烦请贵司注意：维护时间调整为2026年9月25日 02:00-04:00 (GMT+8)。', 'fill', "2026-09-25T02:00:00+08:00", "2026-09-25T04:00:00+08:00",
      note='fix-up R1.16: pinned current verdict'),
    C('M6-R1.5-1', 'F', 'audit', 'Maintenance: none this week. Tournament 25/09/2026 14:00-16:00 (GMT+8)', 'ignore', None, None,
      note='fix-up R1.5: pinned current verdict'),
    C('M6-R1.9-1', 'F', 'audit', 'Thanks for the maintenance notice, see you tomorrow at 10:00', 'ignore', None, None,
      note='fix-up R1.9: pinned current verdict'),
    # -- M7: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M7-F1-1', 'B', 'audit', 'Scheduled maintenance\nDate: Thu 24 Sep - Fri 25 Sep 2026\nTime: 22:00 - 02:00 (GMT+8)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F1: pinned current verdict'),
    C('M7-F1-2', 'B', 'audit', 'Scheduled maintenance\nDate: Thursday 24 September - Friday 25 September 2026\nTime: 22:00 - 02:00 (GMT+8)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F1: pinned current verdict'),
    C('M7-F1-3', 'B', 'audit', 'Scheduled maintenance\nDate: Thursday, 24 September - Friday, 25 September 2026\nTime: 22:00 - 02:00 (GMT+8)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F1: pinned current verdict'),
    C('M7-F10-1', 'D', 'audit', '维护公告\n日期：2026年9月23日\n北京时间：10:00-12:00\nGMT时间：02:00-04:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F10: pinned current verdict'),
    C('M7-F11-1', 'C', 'audit', 'Scheduled maintenance 2026-09-24 10:00:00+08:00 - 12:00:00+08:00', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F11: pinned current verdict'),
    C('M7-F11-2', 'C', 'audit', 'Scheduled maintenance 2026-09-24 10:00:00 +08:00 - 12:00:00 +08:00', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F11: pinned current verdict'),
    C('M7-F11-3', 'C', 'audit', 'Scheduled maintenance 2026-09-24 10:00:00 UTC+08:00 - 12:00:00 UTC+08:00', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F11: pinned current verdict'),
    C('M7-F11-4', 'C', 'audit', 'Scheduled maintenance 2026-09-24 02:00:00-05:00 - 04:00:00-05:00', 'fill', "2026-09-24T02:00:00-05:00", "2026-09-24T04:00:00-05:00",
      note='fix-up F11: pinned current verdict'),
    C('M7-F13-1', 'G', 'audit', '维护时间：2026年9月25日 下午4:00-6:00（次日）', 'fill', "2026-09-25T16:00:00+08:00", "2026-09-26T06:00:00+08:00",
      note='fix-up F13: pinned current verdict'),
    C('M7-F14-1', 'D', 'audit', 'SCHEDULED MAINTENANCE\nDATE: 2026-09-24\nTIME: 10:00 - 12:00 (EST. 2 HOURS)', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F14: pinned current verdict'),
    C('M7-F14-2', 'D', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 EST. 2 HOURS', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F14: pinned current verdict'),
    C('M7-F14-3', 'D', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 (EST 2 hours)', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F14: pinned current verdict'),
    C('M7-F14-4', 'D', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 (GMT+8), contact the ICT TEAM for help', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F14: pinned current verdict'),
    C('M7-F15-1', 'I', 'audit', 'Scheduled maintenance\nDate: 23.09.2026\nTime: 10.00 - 12.00 (GMT+8)\nSupport: 09:00 - 18:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F15: pinned current verdict'),
    C('M7-F15-2', 'I', 'audit', 'Scheduled maintenance\nDate: 23.09.2026\nTime: 10.00 - 12.00 (GMT+8)\nContact us: 09:00 - 18:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F15: pinned current verdict'),
    C('M7-F15-3', 'I', 'audit', 'Scheduled maintenance\nDate: 23.09.2026\nTime: 10.00 - 12.00 (GMT+8)\nContact: 09:00 - 18:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F15: pinned current verdict'),
    C('M7-F15-4', 'I', 'audit', 'Scheduled maintenance\nDate: 23.09.2026\nTime: 10.00 - 12.00 (GMT+8)\nLive chat: 09:00 - 18:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F15: pinned current verdict'),
    C('M7-F15-5', 'I', 'audit', 'Scheduled maintenance\nDate: 23.09.2026\nTime: 10.00 - 12.00 (GMT+8)\nUAT: 09:00 - 18:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F15: pinned current verdict'),
    C('M7-F15-6', 'I', 'audit', 'Scheduled maintenance\nDate: 23.09.2026\nTime: 10.00 - 12.00 (GMT+8)\nStaging: 09:00 - 18:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F15: pinned current verdict'),
    C('M7-F15-7', 'I', 'audit', 'Scheduled maintenance\nDate: 23.09.2026\nTime: 10.00 - 12.00 (GMT+8)\nTest environment: 09:00 - 18:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F15: pinned current verdict'),
    C('M7-F15-8', 'I', 'audit', 'Scheduled maintenance\nDate: 2026-09-23\nTime: 14:00 - 16:00 (GMT+8)\nUAT: 09:00 - 12:00', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up F15: pinned current verdict'),
    C('M7-F16-1', 'D', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 (Malta Time)', 'needs_human', None, None,
      note='fix-up F16: pinned current verdict'),
    C('M7-F16-2', 'D', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 Malta Time', 'needs_human', None, None,
      note='fix-up F16: pinned current verdict'),
    C('M7-F16-3', 'D', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 Pacific Time', 'needs_human', None, None,
      note='fix-up F16: pinned current verdict'),
    C('M7-F16-4', 'D', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 WEST', 'needs_human', None, None,
      note='fix-up F16: pinned current verdict'),
    C('M7-F16-5', 'D', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 (Europe/Berlin)', 'needs_human', None, None,
      note='fix-up F16: pinned current verdict'),
    C('M7-F16-6', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-09-24\nTime: 10:00 - 12:00\nAll times in CEST', 'needs_human', None, None,
      note='fix-up F16: pinned current verdict'),
    C('M7-F16-7', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-09-24\nTime: 10:00 - 12:00\nAll times are CET.', 'needs_human', None, None,
      note='fix-up F16: pinned current verdict'),
    C('M7-F16-8', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-09-24\nTime: 10:00 - 12:00\nAll times are Malta time.', 'needs_human', None, None,
      note='fix-up F16: pinned current verdict'),
    C('M7-F16-9', 'D', 'audit', 'Scheduled maintenance\nDate: 2026-09-24\nTime: 10:00 - 12:00\n(times in CEST)', 'needs_human', None, None,
      note='fix-up F16: pinned current verdict'),
    C('M7-F17-1', 'F', 'audit', 'Maintenance notice: 2026-09-30 07:00-09:00 (GMT+8). New game early access: 2026-09-25 10:00-12:00 (GMT+8).', 'fill', "2026-09-30T07:00:00+08:00", "2026-09-30T09:00:00+08:00",
      note='fix-up F17: pinned current verdict'),
    C('M7-F17-2', 'F', 'audit', '系统维护通知：2026年9月30日 07:00-09:00 (GMT+8)。新游戏抢先体验：2026年9月25日 10:00-12:00 (GMT+8)。', 'fill', "2026-09-30T07:00:00+08:00", "2026-09-30T09:00:00+08:00",
      note='fix-up F17: pinned current verdict'),
    C('M7-F18-1', 'F', 'audit', '系统更新 2026年9月24日 10:00-12:00 (GMT+8)，更新期间游戏不受影响。', 'ignore', None, None,
      note='fix-up F18: pinned current verdict'),
    C('M7-F18-2', 'F', 'audit', 'System upgrade 2026-09-24 10:00-12:00 (GMT+8), no impact on gameplay.', 'ignore', None, None,
      note='fix-up F18: pinned current verdict'),
    C('M7-F18-3', 'F', 'audit', 'System upgrade 2026-09-24 10:00-12:00 (GMT+8). Services will not be affected.', 'ignore', None, None,
      note='fix-up F18: pinned current verdict'),
    C('M7-F18-4', 'F', 'audit', 'System upgrade 2026-09-24 10:00-12:00 (GMT+8). Players can continue playing as normal.', 'ignore', None, None,
      note='fix-up F18: pinned current verdict'),
    C('M7-F18-5', 'F', 'audit', 'System upgrade 2026-09-24 10:00-12:00 (GMT+8). The platform will remain available.', 'ignore', None, None,
      note='fix-up F18: pinned current verdict'),
    C('M7-F18-keep-6', 'F', 'audit', 'Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). Players will not be able to access games during this period.', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F18-keep: pinned current verdict'),
    C('M7-F19-1', 'F', 'audit', '客服将暂停服务：2026年9月25日 10:00-18:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F19: pinned current verdict'),
    C('M7-F19-2', 'F', 'audit', '中秋节客服将暂停服务：2026年9月25日 10:00-18:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F19: pinned current verdict'),
    C('M7-F19-3', 'F', 'audit', '中秋节假期，客服将暂停服务。时间：2026年9月25日 10:00-18:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F19: pinned current verdict'),
    C('M7-F19-4', 'F', 'audit', '中秋节假期，客服会暂停服务，2026年9月25日 10:00-18:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F19: pinned current verdict'),
    C('M7-F2-1', 'K', 'audit', 'Issue date: 2026-09-22\nScheduled maintenance\nTime: 22:00 - 02:00 (GMT+8)\nDate: 2026-09-24 (Thu)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F2: pinned current verdict'),
    C('M7-F2-2', 'K', 'audit', 'Publish date: 2026-09-22\nScheduled maintenance\nTime: 22:00 - 02:00 (GMT+8)\nDate: 2026-09-24 (Thu)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F2: pinned current verdict'),
    C('M7-F2-3', 'K', 'audit', 'Announcement date: 2026-09-22\nScheduled maintenance\nTime: 22:00 - 02:00 (GMT+8)\nDate: 2026-09-24 (Thu)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F2: pinned current verdict'),
    C('M7-F2-4', 'K', 'audit', 'Released: 2026-09-22\nScheduled maintenance\nTime: 22:00 - 02:00 (GMT+8)\nDate: 2026-09-24 (Thu)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F2: pinned current verdict'),
    C('M7-F2-5', 'K', 'audit', 'Notice issued on 2026-09-22\nScheduled maintenance\nTime: 22:00 - 02:00 (GMT+8)\nDate: 2026-09-24 (Thu)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F2: pinned current verdict'),
    C('M7-F2-6', 'K', 'audit', 'Date of issue: 2026-09-22\nScheduled maintenance\nTime: 22:00 - 02:00 (GMT+8)\nDate: 2026-09-24 (Thu)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F2: pinned current verdict'),
    C('M7-F2-7', 'K', 'audit', 'Sent: 2026-09-22\nScheduled maintenance\nTime: 22:00 - 02:00 (GMT+8)\nDate: 2026-09-24 (Thu)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F2: pinned current verdict'),
    C('M7-F2-8', 'K', 'audit', '发布时间：2026-09-22\nScheduled maintenance\nTime: 22:00 - 02:00 (GMT+8)\nDate: 2026-09-24 (Thu)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F2: pinned current verdict'),
    C('M7-F2-9', 'K', 'audit', '公告时间：2026-09-22\nScheduled maintenance\nTime: 22:00 - 02:00 (GMT+8)\nDate: 2026-09-24 (Thu)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F2: pinned current verdict'),
    C('M7-F2-10', 'K', 'audit', '2026-09-22\nScheduled maintenance\nTime: 22:00 - 02:00 (GMT+8)\nDate: 2026-09-24 (Thu)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F2: pinned current verdict'),
    C('M7-F2-11', 'K', 'audit', 'Scheduled maintenance notice 2026-09-22\nScheduled maintenance\nTime: 22:00 - 02:00 (GMT+8)\nDate: 2026-09-24 (Thu)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F2: pinned current verdict'),
    C('M7-F20-1', 'F', 'audit', 'The webinar has been rescheduled to 2026-09-25 15:00-16:00 (GMT+8).', 'ignore', None, None,
      note='fix-up F20: pinned current verdict'),
    C('M7-F20-2', 'F', 'audit', '直播时间变更：2026年9月26日 20:00-22:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F20: pinned current verdict'),
    C('M7-F20-3', 'F', 'audit', 'Rescheduled? 2026-09-26 20:00-22:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F20: pinned current verdict'),
    C('M7-F20-4', 'F', 'audit', '维护能否延期至2026年9月25日 02:00-04:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F20: pinned current verdict'),
    C('M7-F20-5', 'F', 'audit', 'The meeting has been postponed to 2026-09-25 15:00-16:00 (GMT+8).', 'ignore', None, None,
      note='fix-up F20: pinned current verdict'),
    C('M7-F21-1', 'G', 'audit', 'The scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8) will no longer take place.', 'ignore', None, None,
      note='fix-up F21: pinned current verdict'),
    C('M7-F22-1', 'H', 'audit', '例行维护 2026-09-23 10:00-12:00 (GMT+8) 改期至 2026-09-25 10:00-12:00 (GMT+8)。', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up F22: pinned current verdict'),
    C('M7-F22-2', 'H', 'audit', '系统维护 2026-09-23 10:00-12:00 (GMT+8) 延期至 2026-09-25 10:00-12:00 (GMT+8)。', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up F22: pinned current verdict'),
    C('M7-F22-3', 'H', 'audit', '例行维护 2026-09-23 10:00-12:00 (GMT+8)，改期至 2026-09-25 10:00-12:00 (GMT+8)。', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up F22: pinned current verdict'),
    C('M7-F22-4', 'H', 'audit', '例行维护 2026-09-23 10:00-12:00 改期至 2026-09-23 14:00-16:00 (GMT+8)。', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up F22: pinned current verdict'),
    C('M7-F24-1', 'G', 'audit', 'Scheduled maintenance on 2026-09-23 10:00 - 12:00 (GMT+8). It has been cancelled.', 'ignore', None, None,
      note='fix-up F24: pinned current verdict'),
    C('M7-F24-2', 'G', 'audit', 'Scheduled maintenance on 2026-09-23 10:00 - 12:00 (GMT+8). Please note it has been cancelled.', 'ignore', None, None,
      note='fix-up F24: pinned current verdict'),
    C('M7-F24-3', 'G', 'audit', 'Scheduled maintenance on 2026-09-23 10:00 - 12:00 (GMT+8).\nThis has been cancelled, sorry for the inconvenience.', 'ignore', None, None,
      note='fix-up F24: pinned current verdict'),
    C('M7-F24-4', 'G', 'audit', 'Scheduled maintenance: 2026-09-23 10:00 - 12:00 (GMT+8)\nThe above has been cancelled.', 'ignore', None, None,
      note='fix-up F24: pinned current verdict'),
    C('M7-F24-5', 'G', 'audit', '【维护通知】\n维护时间：2026-09-23 10:00-12:00 (GMT+8)\n已取消。', 'ignore', None, None,
      note='fix-up F24: pinned current verdict'),
    C('M7-F25-1', 'G', 'audit', 'Scheduled maintenance:\nLive Casino: 2026-09-24 10:00 - 12:00 (GMT+8)\nSlots: 2026-09-24 14:00 - 16:00 (GMT+8) has been cancelled', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F25: pinned current verdict'),
    C('M7-F25-2', 'G', 'audit', 'Scheduled maintenance: Live Casino 2026-09-24 10:00 - 12:00 (GMT+8). Slots 2026-09-24 14:00 - 16:00 (GMT+8) cancelled.', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F25: pinned current verdict'),
    # -- M8: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M8-F23-1', 'H', 'audit', 'Maintenance on 2026-09-23 10:00-12:00 (GMT+8) postponed. We will inform you of the new date.', 'needs_human', None, None,
      note='fix-up F23: pinned current verdict'),
    C('M8-F23-2', 'H', 'audit', 'Maintenance 2026-09-23 10:00-12:00 (GMT+8) - POSTPONED', 'needs_human', None, None,
      note='fix-up F23: pinned current verdict'),
    C('M8-F23-3', 'H', 'audit', '[POSTPONED] Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8). New date to follow.', 'needs_human', None, None,
      note='fix-up F23: pinned current verdict'),
    C('M8-F23-4', 'H', 'audit', 'Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8) postponed, new date TBA.', 'needs_human', None, None,
      note='fix-up F23: pinned current verdict'),
    C('M8-F23-5', 'H', 'audit', 'The scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8) needs to be postponed. We will share a new date soon.', 'needs_human', None, None,
      note='fix-up F23: pinned current verdict'),
    C('M8-F26-3', 'H', 'audit', '原定于 2026-09-23 10:00-12:00 (GMT+8) 的例行维护暂停进行，新的维护时间另行通知。', 'needs_human', None, None,
      note='fix-up F26: pinned current verdict'),
    C('M8-F26-4', 'H', 'audit', 'The scheduled maintenance originally planned for 2026-09-23 10:00-12:00 (GMT+8) is suspended. New date to follow.', 'ignore', None, None,
      note='fix-up F26: pinned current verdict'),
    C('M8-F26-5', 'H', 'audit', 'The scheduled maintenance originally planned for 2026-09-23 10:00-12:00 (GMT+8) has been paused. New date to follow.', 'needs_human', None, None,
      note='fix-up F26: pinned current verdict'),
    C('M8-F26-6', 'H', 'audit', 'The scheduled maintenance previously set for 2026-09-23 10:00-12:00 (GMT+8): on hold, new schedule TBA.', 'needs_human', None, None,
      note='fix-up F26: pinned current verdict'),
    C('M8-F28-3', 'H', 'audit', '【维护时间更新】\n例行维护：2026-09-23 10:00-12:00 (GMT+8)\n最新時間：2026-09-25 10:00-12:00 (GMT+8)', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up F28: pinned current verdict'),
    C('M8-F3-1', 'B', 'audit', '本周四进行例行维护\n时间：10:00-12:00 (GMT+8)\n2026年9月22日', 'needs_human', None, None,
      note='fix-up F3: pinned current verdict'),
    C('M8-F3-2', 'B', 'audit', 'Scheduled maintenance this Thursday\nTime: 10:00 - 12:00 (GMT+8)\n2026-09-22', 'needs_human', None, None,
      note='fix-up F3: pinned current verdict'),
    C('M8-F3-3', 'B', 'audit', 'Scheduled maintenance on Thursday\nTime: 10:00 - 12:00 (GMT+8)\n2026/09/22', 'needs_human', None, None,
      note='fix-up F3: pinned current verdict'),
    C('M8-F3-4', 'B', 'audit', '明天进行例行维护\n维护时间：10:00-12:00\n2026-09-22', 'needs_human', None, None,
      note='fix-up F3: pinned current verdict'),
    C('M8-F30-1', 'G', 'audit', 'We have cancelled the game maintenance on 2026-09-23 10:00 - 12:00 (GMT+8).', 'ignore', None, None,
      note='fix-up F30: pinned current verdict'),
    C('M8-F30-2', 'G', 'audit', 'We have cancelled the Bonus system maintenance on 2026-09-23 10:00 - 12:00 (GMT+8).', 'ignore', None, None,
      note='fix-up F30: pinned current verdict'),
    C('M8-F30-3', 'G', 'audit', 'We have cancelled the Tournament platform maintenance on 2026-09-23 10:00 - 12:00 (GMT+8).', 'ignore', None, None,
      note='fix-up F30: pinned current verdict'),
    C('M8-F30-4', 'G', 'audit', 'We have cancelled the Sessions server maintenance on 2026-09-23 10:00 - 12:00 (GMT+8).', 'ignore', None, None,
      note='fix-up F30: pinned current verdict'),
    C('M8-F30-5', 'G', 'audit', 'We have cancelled the Jackpot maintenance on 2026-09-23 10:00 - 12:00 (GMT+8).', 'ignore', None, None,
      note='fix-up F30: pinned current verdict'),
    C('M8-F30-6', 'G', 'audit', 'We have cancelled the Promotions maintenance on 2026-09-23 10:00 - 12:00 (GMT+8).', 'ignore', None, None,
      note='fix-up F30: pinned current verdict'),
    C('M8-F30-7', 'G', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8) for tournaments and jackpots has been cancelled.', 'ignore', None, None,
      note='fix-up F30: pinned current verdict'),
    C('M8-F31-1', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\nSupport: 09:00-21:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F31: pinned current verdict'),
    C('M8-F31-2', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\nLive chat: 09:00-21:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F31: pinned current verdict'),
    C('M8-F31-3', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\n在线支持: 09:00-21:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F31: pinned current verdict'),
    C('M8-F31-4', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\nOur team: 09:00-21:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F31: pinned current verdict'),
    C('M8-F31-5', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\nHotline: 09:00-21:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F31: pinned current verdict'),
    C('M8-F31-6', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\nCutoff: 09:00-21:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F31: pinned current verdict'),
    C('M8-F31-7', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\nReport period: 09:00-21:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F31: pinned current verdict'),
    C('M8-F31-8', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 14:00-16:00 (GMT+8).\nReport period: 00:00 - 12:00', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up F31: pinned current verdict'),
    C('M8-F32-1', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\nEvent period: 2026-09-23 11:00 - 2026-09-24 18:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F32: pinned current verdict'),
    C('M8-F32-2', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\nMid-Autumn Festival: 2026-09-22 20:00 - 2026-09-24 11:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F32: pinned current verdict'),
    C('M8-F32-3', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\nMid-Autumn Event\n2026-09-23 11:00 - 2026-09-24 18:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F32: pinned current verdict'),
    C('M8-F32-4', 'I', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\nEvent period: 2026-09-23 13:00 - 2026-09-24 18:00', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F32: pinned current verdict'),
    C('M8-F33-1', 'I', 'audit', 'Scheduled maintenance (GMT+8)\nNight 1: 2026-09-23 22:00 - 2026-09-24 02:00\nNight 2: 2026-09-24 22:00 - 2026-09-25 02:00', 'fill', "2026-09-23T22:00:00+08:00", "2026-09-24T02:00:00+08:00",
      note='fix-up F33: pinned current verdict'),
    C('M8-F33-2', 'I', 'audit', 'Scheduled maintenance (GMT+8)\nFirst night: 2026-09-23 22:00 - 2026-09-24 02:00\nSecond night: 2026-09-24 22:00 - 2026-09-25 02:00', 'fill', "2026-09-23T22:00:00+08:00", "2026-09-24T02:00:00+08:00",
      note='fix-up F33: pinned current verdict'),
    C('M8-F33-3', 'I', 'audit', 'Scheduled maintenance (GMT+8)\nNight 1 - 2026-09-23 22:00 to 2026-09-24 02:00\nNight 2 - 2026-09-24 22:00 to 2026-09-25 02:00', 'fill', "2026-09-23T22:00:00+08:00", "2026-09-24T02:00:00+08:00",
      note='fix-up F33: pinned current verdict'),
    C('M8-F67-1', 'I', 'audit', 'Scheduled maintenance (GMT+8)\nPart 1: 2026-09-23 22:00 - 2026-09-24 02:00\nPart 2: 2026-09-25 22:00 - 2026-09-26 02:00', 'fill', "2026-09-23T22:00:00+08:00", "2026-09-24T02:00:00+08:00",
      note='fix-up F67: pinned current verdict'),
    C('M8-F67-2', 'I', 'audit', 'Maintenance reminder: 2026-09-25 00:00 - 02:00 (GMT+8)', 'fill', "2026-09-25T00:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F67: pinned current verdict'),
    C('M8-F67-3', 'I', 'audit', '温馨提示：第二阶段维护 2026-09-25 00:00 - 02:00 (GMT+8)', 'fill', "2026-09-25T00:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up F67: pinned current verdict'),
    C('M8-F4-1', 'B', 'audit', '[23/09/2026] Scheduled maintenance 10:00 - 12:00 (GMT+8) on Thursday.', 'needs_human', None, None,
      note='fix-up F4: pinned current verdict'),
    C('M8-F4-2', 'B', 'audit', '[2026-09-23] Scheduled maintenance this Thursday 10:00 - 12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up F4: pinned current verdict'),
    C('M8-F4-3', 'B', 'audit', '2026-09-23: next scheduled maintenance will be 10:00 - 12:00 (GMT+8) on Thursday', 'needs_human', None, None,
      note='fix-up F4: pinned current verdict'),
    C('M8-F4-4', 'B', 'audit', '[2026-09-23] 下次例行维护时间为 10:00-12:00 (GMT+8)，周四进行。', 'needs_human', None, None,
      note='fix-up F4: pinned current verdict'),
    C('M8-F42-1', 'B', 'audit', 'Further to our email of 2026-09-21, scheduled maintenance on 24-Sep 10:00 - 12:00 (GMT+8).', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F42: pinned current verdict'),
    C('M8-F42-2', 'B', 'audit', 'Scheduled maintenance on 24-Sep 10:00 - 12:00 (GMT+8).', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F42: pinned current verdict'),
    C('M8-F42-3', 'B', 'audit', 'Scheduled maintenance on Sep-24 10:00 - 12:00 (GMT+8).', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F42: pinned current verdict'),
    C('M8-F44-2', 'C', 'audit', '维护时间：2026年9月24日 23:00 至 次日 01:00，次日 09:00 到 10:00 为观察期', 'fill', "2026-09-24T23:00:00+08:00", "2026-09-25T01:00:00+08:00",
      note='fix-up F44: pinned current verdict'),
    C('M8-F45-1', 'F', 'audit', '我司将于2026年9月24日 10:00-12:00 (GMT+8) 进行系统维护，可以帮忙通知一下玩家吗？', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F45: pinned current verdict'),
    C('M8-F45-2', 'F', 'audit', '我司将于2026年9月24日 10:00-12:00 (GMT+8) 进行系统维护，麻烦帮忙通知玩家，谢谢！', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F45: pinned current verdict'),
    C('M8-F46-1', 'F', 'audit', 'Hi all,\n\nScheduled maintenance 2026-09-24 10:00-12:00 (GMT+8).\n\nAffected: UAT only. Production is not affected.', 'ignore', None, None,
      note='fix-up F46: pinned current verdict'),
    C('M8-F46-2', 'F', 'audit', 'Dear Partner,\nUAT Environment\nScheduled maintenance\nDate: 2026-09-24\nTime: 10:00-12:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F46: pinned current verdict'),
    C('M8-F46-3', 'F', 'audit', 'Please be informed that we will have scheduled maintenance.\nEnvironment: Staging\nDate: 2026-09-24\nTime: 10:00-12:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F46: pinned current verdict'),
    C('M8-F46-4', 'F', 'audit', 'Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8)\nScope: staging environment', 'ignore', None, None,
      note='fix-up F46: pinned current verdict'),
    C('M8-F46-5', 'F', 'audit', '预发布环境维护通知 2026-09-24 10:00-12:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F46: pinned current verdict'),
    C('M8-F46-6', 'F', 'audit', 'QA environment scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F46: pinned current verdict'),
    C('M8-F46-7', 'F', 'audit', 'Demo environment scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F46: pinned current verdict'),
    C('M8-F46-keep-8', 'F', 'audit', 'Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). UAT is not affected.', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F46-keep: pinned current verdict'),
    C('M8-F46-keep-9', 'F', 'audit', 'Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8) on production. Staging is not affected.', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F46-keep: pinned current verdict'),
    C('M8-F47-1', 'G', 'audit', '例行维护：2026-09-23 10:00-12:00 (GMT+8)，维护大约12:00完成。', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F47: pinned current verdict'),
    C('M8-F47-2', 'G', 'audit', '例行维护：2026-09-23 10:00-12:00 (GMT+8)，维护预估12:00结束。', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F47: pinned current verdict'),
    C('M8-F47-3', 'G', 'audit', '例行维护：2026-09-23 10:00-12:00 (GMT+8)，维护约12:00结束。', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F47: pinned current verdict'),
    C('M8-F47-4', 'G', 'audit', '例行维护：2026-09-23 10:00-12:00 (GMT+8)，维护大概在12:00完成。', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F47: pinned current verdict'),
    C('M8-F47-5', 'G', 'audit', '例行维护：2026-09-23 10:00-12:00 (GMT+8)，维护最晚12:00结束。', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F47: pinned current verdict'),
    C('M8-F47-6', 'G', 'audit', '例行维护：2026-09-23 10:00-12:00 (GMT+8)，维护最迟12:00完成。', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F47: pinned current verdict'),
    C('M8-F47-7', 'G', 'audit', '【维护通知】\n维护时间：2026-09-23 10:00-12:00 (GMT+8)\n维护大约12:00完成。', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F47: pinned current verdict'),
    C('M8-F47-8', 'G', 'audit', '【维护通知】\n维护时间：2026-09-23 10:00-12:00 (GMT+8)\n维护完成将另行通知。', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F47: pinned current verdict'),
    C('M8-F49-1', 'G', 'audit', 'The maintenance on 2026-09-23 10:00-12:00 was cancelled. It will take place on 2026-09-30 10:00-12:00 (GMT+8) instead.', 'fill', "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      note='fix-up F49: pinned current verdict'),
    C('M8-F49-2', 'G', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 was cancelled. It will take place on 2026-09-30 10:00-12:00 (GMT+8).', 'fill', "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      note='fix-up F49: pinned current verdict'),
    C('M8-F49-3', 'G', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8) has been cancelled and rescheduled to 2026-09-30 10:00-12:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up F49: pinned current verdict'),
    C('M8-F53-3', 'G', 'audit', 'Please ignore the above notice (scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8)). A revised schedule will follow.', 'needs_human', None, None,
      note='fix-up F53: pinned current verdict'),
    C('M8-F53-6', 'G', 'audit', 'Maintenance 2026-09-23 10:00-12:00 (GMT+8) - change of plan, an updated schedule will be shared shortly.', 'needs_human', None, None,
      note='fix-up F53: pinned current verdict'),
    C('M8-F54-2', 'H', 'audit', 'Scheduled maintenance previously notified: 2026-09-23 10:00-12:00 (GMT+8). Updated: 2026-09-25 10:00-12:00 (GMT+8)', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up F54: pinned current verdict'),
    C('M8-F54-4', 'H', 'audit', '之前通知的例行维护 2026-09-23 10:00-12:00 (GMT+8) 有变动，最新时间：14:00-16:00', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up F54: pinned current verdict'),
    C('M8-F54-5', 'H', 'audit', 'The maintenance previously announced for 2026-09-23 10:00-12:00 (GMT+8) will instead be 14:00-16:00.', 'fill', "2026-09-23T14:00:00+08:00", "2026-09-23T16:00:00+08:00",
      note='fix-up F54: pinned current verdict'),
    C('M8-F61-1', 'B', 'audit', '将于9/24的凌晨02:00至04:00进行例行维护', 'fill', "2026-09-24T02:00:00+08:00", "2026-09-24T04:00:00+08:00",
      note='fix-up F61: pinned current verdict'),
    C('M8-F61-2', 'B', 'audit', '将于9/24的上午10:00至12:00进行例行维护', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F61: pinned current verdict'),
    C('M8-F61-3', 'B', 'audit', '将于9/24的下午14:00至16:00进行例行维护', 'fill', "2026-09-24T14:00:00+08:00", "2026-09-24T16:00:00+08:00",
      note='fix-up F61: pinned current verdict'),
    C('M8-F61-4', 'B', 'audit', '将于9/24的晚上22:00至23:00进行例行维护', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-24T23:00:00+08:00",
      note='fix-up F61: pinned current verdict'),
    C('M8-F61-5', 'B', 'audit', '9/24的维护时间为10:00-12:00 (GMT+8)，届时游戏将无法进入。', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up F61: pinned current verdict'),
    C('M8-F62-1', 'C', 'audit', '系统维护 2026年9月25日 10.00-12.00（北京时间），维护期间暂停投注', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up F62: pinned current verdict'),
    C('M8-F62-2', 'C', 'audit', 'Scheduled maintenance 2026-09-25 10.00 - 12.00 (GMT+8); bets placed before will settle', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up F62: pinned current verdict'),
    C('M8-F62-3', 'C', 'audit', 'Scheduled maintenance 2026-09-25 10.00 - 12.00 (GMT+8) in the new version', 'fill', "2026-09-25T10:00:00+08:00", "2026-09-25T12:00:00+08:00",
      note='fix-up F62: pinned current verdict'),
    C('M8-F62-4', 'C', 'audit', '系统维护 2026年9月25日，预计 2.00-4.00 小时', 'ignore', None, None,
      note='fix-up F62: pinned current verdict'),
    C('M8-F62-5', 'C', 'audit', 'Scheduled maintenance 2026-09-25, estimated 2.00 - 4.00 h', 'ignore', None, None,
      note='fix-up F62: pinned current verdict'),
    C('M8-F62-6', 'C', 'audit', 'Scheduled maintenance 2026-09-25 (GMT+8). RTP 10.00 - 12.00 %', 'ignore', None, None,
      note='fix-up F62: pinned current verdict'),
    C('M8-F62-7', 'C', 'audit', 'Scheduled maintenance 2026-09-25 (GMT+8). odds 1.50 - 2.00', 'ignore', None, None,
      note='fix-up F62: pinned current verdict'),
    C('M8-F63-1', 'F', 'audit', 'Maintenance on 25/09/2026 14:00-16:00 (GMT+8)? Please confirm.', 'ignore', None, None,
      note='fix-up F63: pinned current verdict'),
    C('M8-F63-2', 'F', 'audit', '游戏将于 2026年9月25日 14:00-16:00 (GMT+8) 维护？', 'ignore', None, None,
      note='fix-up F63: pinned current verdict'),
    C('M8-F64-1', 'F', 'audit', '若本周没有维护，请回复“无”', 'ignore', None, None,
      note='fix-up F64: pinned current verdict'),
    C('M8-F64-2', 'F', 'audit', '没有维护的话请告知', 'ignore', None, None,
      note='fix-up F64: pinned current verdict'),
    C('M8-F64-3', 'F', 'audit', "In case of no maintenance, reply 'none'.", 'ignore', None, None,
      note='fix-up F64: pinned current verdict'),
    C('M8-F64-4', 'F', 'audit', 'Please confirm there is no maintenance this week.', 'ignore', None, None,
      note='fix-up F64: pinned current verdict'),
    C('M8-F64-5', 'F', 'audit', 'Kindly confirm no maintenance this week.', 'ignore', None, None,
      note='fix-up F64: pinned current verdict'),
    C('M8-F64-6', 'F', 'audit', '麻烦确认一下本周没有维护', 'ignore', None, None,
      note='fix-up F64: pinned current verdict'),
    C('M8-F64-7', 'F', 'audit', '请确认本周没有维护', 'ignore', None, None,
      note='fix-up F64: pinned current verdict'),
    C('M8-F64-8', 'F', 'audit', '本周没有维护嘛', 'ignore', None, None,
      note='fix-up F64: pinned current verdict'),
    C('M8-F64-9', 'F', 'audit', 'Is it no maintenance this week', 'ignore', None, None,
      note='fix-up F64: pinned current verdict'),
    C('M8-F64-keep-10', 'F', 'audit', '本周没有维护。', 'clear', None, None,
      note='fix-up F64-keep: pinned current verdict'),
    C('M8-F64-keep-11', 'F', 'audit', 'No maintenance this week.', 'clear', None, None,
      note='fix-up F64-keep: pinned current verdict'),
    C('M8-F65-1', 'G', 'audit', 'Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8). It has ended early.', 'ignore', None, None,
      note='fix-up F65: pinned current verdict'),
    C('M8-F65-2', 'G', 'audit', 'Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8); it has ended early.', 'ignore', None, None,
      note='fix-up F65: pinned current verdict'),
    C('M8-F65-3', 'G', 'audit', '[Completed] Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F65: pinned current verdict'),
    C('M8-F65-4', 'G', 'audit', 'Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8)\nStatus: Completed', 'ignore', None, None,
      note='fix-up F65: pinned current verdict'),
    C('M8-F65-5', 'G', 'audit', 'Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8). Status: completed.', 'ignore', None, None,
      note='fix-up F65: pinned current verdict'),
    C('M8-F65-6', 'G', 'audit', '【已完成】例行维护 2026-09-23 10:00 - 12:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F65: pinned current verdict'),
    C('M8-F65-keep-7', 'G', 'audit', 'Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8). It will be completed by 12:00.', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up F65-keep: pinned current verdict'),
    C('M8-F66-1', 'F', 'audit', 'The patch requires no maintenance.', 'ignore', None, None,
      note='fix-up F66: pinned current verdict'),
    C('M8-F66-2', 'F', 'audit', 'There is no maintenance needed for the update.', 'ignore', None, None,
      note='fix-up F66: pinned current verdict'),
    C('M8-F66-3', 'F', 'audit', 'Game update v2.3 released today, no maintenance.', 'ignore', None, None,
      note='fix-up F66: pinned current verdict'),
    C('M8-F66-4', 'F', 'audit', 'Release note v2.3: bug fixes, no maintenance.', 'ignore', None, None,
      note='fix-up F66: pinned current verdict'),
    C('M8-F66-5', 'F', 'audit', 'Hotfix tonight (no maintenance).', 'ignore', None, None,
      note='fix-up F66: pinned current verdict'),
    C('M8-F66-6', 'F', 'audit', '今晚热更新，没有维护。', 'ignore', None, None,
      note='fix-up F66: pinned current verdict'),
    C('M8-F66-7', 'F', 'audit', '版本更新已完成，没有维护。', 'ignore', None, None,
      note='fix-up F66: pinned current verdict'),
    C('M8-G1.3-1', 'F', 'audit', 'Emergency maintenance now until 16:00 (GMT+8)', 'ignore', None, None,
      note='fix-up G1.3: pinned current verdict'),
    C('M8-G1.3-2', 'F', 'audit', 'Emergency maintenance until 16:00 (GMT+8)', 'ignore', None, None,
      note='fix-up G1.3: pinned current verdict'),
    C('M8-G1.3-3', 'F', 'audit', 'Emergency maintenance at 15:00 (GMT+8), about 1 hour', 'ignore', None, None,
      note='fix-up G1.3: pinned current verdict'),
    C('M8-G1.3-4', 'F', 'audit', 'Urgent maintenance in progress, ETA 16:00 GMT+8', 'ignore', None, None,
      note='fix-up G1.3: pinned current verdict'),
    C('M8-G1.3-5', 'F', 'audit', '紧急维护中，预计16:00恢复', 'ignore', None, None,
      note='fix-up G1.3: pinned current verdict'),
    C('M8-G1.3-6', 'F', 'audit', '紧急维护，预计 16:00 结束 (GMT+8)', 'ignore', None, None,
      note='fix-up G1.3: pinned current verdict'),
    C('M8-G1.4-1', 'F', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8)\n对吗？', 'needs_human', None, None,
      note='fix-up G1.4: pinned current verdict'),
    C('M8-G1.4-2', 'F', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8). Correct?', 'needs_human', None, None,
      note='fix-up G1.4: pinned current verdict'),
    C('M8-G1.4-3', 'F', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8)\nCan you confirm?', 'needs_human', None, None,
      note='fix-up G1.4: pinned current verdict'),
    C('M8-G1.4-4', 'F', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8). Is that right?', 'needs_human', None, None,
      note='fix-up G1.4: pinned current verdict'),
    C('M8-G1.4-5', 'F', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8)\nIs this correct?', 'needs_human', None, None,
      note='fix-up G1.4: pinned current verdict'),
    C('M8-G1.4-6', 'F', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8)\n是这样吗？', 'needs_human', None, None,
      note='fix-up G1.4: pinned current verdict'),
    C('M8-G1.4-7', 'F', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8). Right?', 'needs_human', None, None,
      note='fix-up G1.4: pinned current verdict'),
    C('M8-G1.4-8', 'F', 'audit', '系统维护：2026年9月25日 14:00-16:00 (GMT+8)\n对吗？', 'needs_human', None, None,
      note='fix-up G1.4: pinned current verdict'),
    C('M8-G1.4-9', 'F', 'audit', 'Just to confirm: scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8). Correct?', 'needs_human', None, None,
      note='fix-up G1.4: pinned current verdict'),
    C('M8-G1.4-keep-10', 'F', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8)\nAny questions, please contact us?', 'fill', "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note='fix-up G1.4-keep: pinned current verdict'),
    C('M8-G1.4-keep-11', 'F', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8)\n收到吗？', 'fill', "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note='fix-up G1.4-keep: pinned current verdict'),
    C('M8-G1.4-keep-12', 'F', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8)\nThank you. Noted?', 'fill', "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note='fix-up G1.4-keep: pinned current verdict'),
    C('M8-G1.5-1', 'E', 'audit', 'Hacksaw will be offline on 25/09/2026 14:00-16:00 (GMT+8).', 'needs_human', None, None,
      note='G1.5 + clean-notice check: a provider named only as offline, with no maintenance word, goes to a person (the watcher cards it)'),
    C('M8-G1.5-2', 'E', 'audit', 'Evolution will be offline on 25/09/2026 14:00-16:00 (GMT+8).', 'needs_human', None, None,
      note='G1.5 + clean-notice check: a provider named only as offline, with no maintenance word, goes to a person (the watcher cards it)'),
    C('M8-G1.5-3', 'E', 'audit', 'Pragmatic Play will be unavailable on 25/09/2026 14:00-16:00 (GMT+8).', 'needs_human', None, None,
      note='G1.5 + clean-notice check: a provider named only as offline, with no maintenance word, goes to a person (the watcher cards it)'),
    C('M8-G1.5-4', 'E', 'audit', 'JILI will go offline on 25/09/2026 14:00-16:00 (GMT+8).', 'needs_human', None, None,
      note='G1.5 + clean-notice check: a provider named only as offline, with no maintenance word, goes to a person (the watcher cards it)'),
    C('M8-G1.5-5', 'E', 'audit', 'Hacksaw will be down on 25/09/2026 14:00-16:00 (GMT+8).', 'needs_human', None, None,
      note='G1.5 + clean-notice check: a provider named only as offline, with no maintenance word, goes to a person (the watcher cards it)'),
    C('M8-G1.5-6', 'E', 'audit', 'PG将于2026年9月25日 14:00-16:00 (GMT+8)停机', 'fill', "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note='G1.5: 停机 names the outage, so the clean-notice check lets it fill'),
    C('M8-G1.5-neg-7', 'E', 'audit', 'Customer Service will be offline on 25/09/2026 14:00-16:00 (GMT+8).', 'ignore', None, None,
      note='fix-up G1.5-neg: pinned current verdict'),
    C('M8-G1.5-neg-8', 'E', 'audit', 'Live Chat will be unavailable on 25/09/2026 14:00-16:00 (GMT+8).', 'ignore', None, None,
      note='fix-up G1.5-neg: pinned current verdict'),
    C('M8-G1.5-neg-9', 'E', 'audit', 'The Promo Page will be offline on 25/09/2026 14:00-16:00 (GMT+8).', 'ignore', None, None,
      note='fix-up G1.5-neg: pinned current verdict'),
    C('M8-G1.5-neg-10', 'E', 'audit', 'Registration will be closed on 25/09/2026 14:00-16:00 (GMT+8).', 'ignore', None, None,
      note='fix-up G1.5-neg: pinned current verdict'),
    C('M8-G1.5-neg-11', 'E', 'audit', 'Tournament Lobby will be closed on 25/09/2026 14:00-16:00 (GMT+8).', 'ignore', None, None,
      note='fix-up G1.5-neg: pinned current verdict'),
    C('M8-G1.6-1', 'G', 'audit', 'Maintenance has been completed. Upcoming maintenance: 2026-09-25 14:00-16:00 (GMT+8).', 'fill', "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note='fix-up G1.6: pinned current verdict'),
    C('M8-G1.6-2', 'G', 'audit', 'Last maintenance has been completed. Upcoming maintenance: 2026-09-25 14:00-16:00 (GMT+8).', 'fill', "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note='fix-up G1.6: pinned current verdict'),
    C('M8-G1.6-3', 'G', 'audit', 'Maintenance completed.\nUpcoming maintenance: 2026-09-25 14:00-16:00 (GMT+8).', 'fill', "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note='fix-up G1.6: pinned current verdict'),
    C('M8-G1.7-1', 'F', 'audit', '新版本上线：2026年9月24日 10:00-11:00 (GMT+8)，不会出现服务中断。', 'ignore', None, None,
      note='fix-up G1.7: pinned current verdict'),
    C('M8-G1.7-2', 'F', 'audit', '新版本上线：2026年9月24日 10:00-11:00 (GMT+8)，期间不会出现服务中断。', 'ignore', None, None,
      note='fix-up G1.7: pinned current verdict'),
    C('M8-G1.7-3', 'F', 'audit', '新版本上线：2026年9月24日 10:00-11:00 (GMT+8)，不会发生服务中断。', 'ignore', None, None,
      note='fix-up G1.7: pinned current verdict'),
    C('M8-G1.7-4', 'F', 'audit', '新版本上线：2026年9月24日 10:00-11:00 (GMT+8)，预计不会出现服务中断。', 'ignore', None, None,
      note='fix-up G1.7: pinned current verdict'),
    C('M8-G1.7-5', 'F', 'audit', '新版本上线：2026年9月24日 10:00-11:00 (GMT+8)，不存在服务中断。', 'ignore', None, None,
      note='fix-up G1.7: pinned current verdict'),
    C('M8-G1.7-6', 'F', 'audit', '新版本上线：2026年9月24日 10:00-11:00 (GMT+8)，全程不会出现服务中断。', 'ignore', None, None,
      note='fix-up G1.7: pinned current verdict'),
    C('M8-G1.7-7', 'F', 'audit', '新版本上线：2026年9月24日 10:00-11:00 (GMT+8)，不会出现任何服务中断的情况。', 'ignore', None, None,
      note='fix-up G1.7: pinned current verdict'),
    C('M8-G1.7-8', 'F', 'audit', '新版本上线：2026年9月24日 10:00-11:00 (GMT+8)，不会出现停服维护。', 'ignore', None, None,
      note='fix-up G1.7: pinned current verdict'),
    C('M8-G2.1-1', 'G', 'audit', 'The scheduled maintenance notice for 2026-09-24 10:00-12:00 (GMT+8) is void.', 'needs_human', None, None,
      note='fix-up G2.1: pinned current verdict'),
    C('M8-G2.1-2', 'G', 'audit', '无视上面的维护通知 2026-09-24 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up G2.1: pinned current verdict'),
    C('M8-G2.1-3', 'G', 'audit', '上面的维护通知 2026-09-24 10:00-12:00 (GMT+8) 有误，请忽略。', 'needs_human', None, None,
      note='fix-up G2.1: pinned current verdict'),
    C('M8-G2.1-4', 'G', 'audit', 'Void: scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up G2.1: pinned current verdict'),
    C('M8-G2.1-5', 'G', 'audit', 'VOID - Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up G2.1: pinned current verdict'),
    C('M8-G2.1-6', 'G', 'audit', 'Retracted: scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up G2.1: pinned current verdict'),
    C('M8-G2.1-7', 'G', 'audit', 'We retract the maintenance notice for 2026-09-24 10:00-12:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up G2.1: pinned current verdict'),
    C('M8-G2.1-8', 'G', 'audit', 'The maintenance notice for 2026-09-24 10:00-12:00 (GMT+8) is null and void.', 'needs_human', None, None,
      note='fix-up G2.1: pinned current verdict'),
    C('M8-G2.1-9', 'G', 'audit', 'The maintenance notice for 2026-09-24 10:00-12:00 (GMT+8) is invalid, please ignore.', 'needs_human', None, None,
      note='fix-up G2.1: pinned current verdict'),
    C('M8-G2.1-10', 'G', 'audit', 'Please disregard the maintenance notice for 24/09 10:00-12:00; there will be no maintenance.', 'needs_human', None, None,
      note='fix-up G2.1: pinned current verdict'),
    C('M8-G2.2-1', 'H', 'audit', 'Correction: scheduled maintenance on 24/09/2026 is 14:00-16:00 (GMT+8). 10:00-12:00 was wrong.', 'fill', "2026-09-24T14:00:00+08:00", "2026-09-24T16:00:00+08:00",
      note='fix-up G2.2: pinned current verdict'),
    C('M8-G4.3-1', 'F', 'audit', 'We would like to ask you to reschedule the maintenance to 2026-09-25 03:00-05:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up G4.3: pinned current verdict'),
    C('M8-G4.3-2', 'F', 'audit', 'We kindly ask that the scheduled maintenance be postponed to 2026-09-25 03:00-05:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up G4.3: pinned current verdict'),
    C('M8-G4.3-3', 'F', 'audit', '能否将维护延期至2026-09-25 03:00-05:00 (GMT+8)', 'ignore', None, None,
      note='fix-up G4.3: pinned current verdict'),
    C('M8-G4.3-4', 'F', 'audit', 'Is it possible to postpone the scheduled maintenance to 2026-09-25 03:00-05:00 (GMT+8)', 'ignore', None, None,
      note='fix-up G4.3: pinned current verdict'),
    C('M8-G4.3-5', 'F', 'audit', 'Please do not push any updates during our scheduled maintenance on 2026-09-24 02:00-04:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up G4.3: pinned current verdict'),
    C('M8-G4.3-keep-6', 'F', 'audit', 'Please be informed that the scheduled maintenance has been rescheduled to 2026-09-25 03:00-05:00 (GMT+8).', 'fill', "2026-09-25T03:00:00+08:00", "2026-09-25T05:00:00+08:00",
      note='fix-up G4.3-keep: pinned current verdict'),
    # -- M9: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M9-R1.23-1', 'I', 'audit', 'Maintenance Notice:\nJILI 2026-09-23 10:00-12:00 (GMT+8)', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up R1.23: pinned current verdict'),
    C('M9-R1.23-2', 'I', 'audit', '维护通知：\n全部游戏 2026-09-23 10:00-12:00 (GMT+8)', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up R1.23: pinned current verdict'),
    C('M9-R1.23-3', 'I', 'audit', 'Scheduled maintenance:\nAll games 2026-09-23 10:00-12:00 (GMT+8)', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up R1.23: pinned current verdict'),
    C('M9-R1.24-1', 'F', 'audit', 'Pls confirm maintenance 2026-09-25 14:00-16:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up R1.24: pinned current verdict'),
    C('M9-R1.24-2', 'F', 'audit', 'Please confirm maintenance 2026-09-25 14:00-16:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up R1.24: pinned current verdict'),
    C('M9-R1.24-3', 'F', 'audit', '麻烦确认下你们2026-09-25 14:00-16:00 (GMT+8)是否有维护', 'needs_human', None, None,
      note='fix-up R1.24: pinned current verdict'),
    C('M9-R1.24-4', 'F', 'audit', '确认一下贵司2026-09-25 14:00-16:00 (GMT+8)是否维护', 'needs_human', None, None,
      note='fix-up R1.24: pinned current verdict'),
    C('M9-R1.24-5', 'F', 'audit', 'Please confirm if the scheduled maintenance is 2026-09-25 14:00-16:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up R1.24: pinned current verdict'),
    C('M9-R1.24-keep-6', 'F', 'audit', 'Scheduled maintenance 2026-09-25 14:00-16:00 (GMT+8). Please confirm receipt.', 'fill', "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note='fix-up R1.24-keep: pinned current verdict'),
    C('M9-R1.28-1', 'F', 'audit', 'Scheduled maintenance 2026-09-25 14:00-16:00 (GMT+8). Only Aviator is affected; no downtime for other games.', 'needs_human', None, None,
      note='fix-up R1.28: pinned current verdict'),
    C('M9-R1.28-2', 'F', 'audit', '系统维护 2026年9月25日 14:00-16:00 (GMT+8)，仅捕鱼游戏停服，其他游戏不停服。', 'needs_human', None, None,
      note='fix-up R1.28: pinned current verdict'),
    C('M9-F82-1', 'A', 'audit', 'Test\n9999-12-31 22:00 - 23:00 (GMT-5)\n9999-12-31 22:00 - 23:00 (GMT-5)', 'ignore', None, None,
      note='fix-up F82: pinned current verdict'),
    C('M9-F82-2', 'A', 'audit', 'Scheduled maintenance 9999-12-31 22:00 - 23:00 (GMT-5)', 'fill', "9999-12-31T22:00:00-05:00", "9999-12-31T23:00:00-05:00",
      note='fix-up F82: pinned current verdict'),
    C('M9-F82-3', 'A', 'audit', 'Scheduled maintenance 0001-01-01 00:30 - 01:00 (GMT+8)', 'ignore', None, None,
      note='F82: a year-0001 window is unparseable input - ignored, never a crash'),
    C('M9-F82-4', 'A', 'audit', 'Scheduled maintenance 9999-12-31 23:00 - 01:00 (GMT+8)', 'ignore', None, None,
      note='fix-up F82: pinned current verdict'),
    # -- M10: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M10-W-restored-1', 'G', 'audit', 'All games are back online now.', 'ignore', None, None,
      note='fix-up W-restored: pinned current verdict'),
    C('M10-W-restored-2', 'G', 'audit', 'Services have been restored.', 'ignore', None, None,
      note='fix-up W-restored: pinned current verdict'),
    C('M10-W-restored-3', 'G', 'audit', '游戏已恢复正常。', 'ignore', None, None,
      note='fix-up W-restored: pinned current verdict'),
    C('M10-W-restored-4', 'G', 'audit', 'Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8). All games are back online now.', 'ignore', None, None,
      note='fix-up W-restored: pinned current verdict'),
    C('M10-W-restored-keep-5', 'G', 'audit', 'Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). Games will be back online at 12:00.', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up W-restored-keep: pinned current verdict'),
    C('M10-W-restored-keep-6', 'G', 'audit', 'Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). Once all games are back online we will notify you.', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up W-restored-keep: pinned current verdict'),
    C('M10-W-restored-keep-7', 'G', 'audit', '系统维护 2026-09-24 10:00-12:00 (GMT+8)，维护结束后游戏将恢复正常。', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up W-restored-keep: pinned current verdict'),
    C('M10-W-restored-partial-8', 'G', 'audit', 'Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). All other games are back online.', 'needs_human', None, None,
      note='fix-up W-restored-partial: pinned current verdict'),
    # -- M11: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M11-V-F1-1', 'K', 'audit', '系统维护\n日期：9月24日周四 - 9月25日周五\n时间：22:00-02:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F1: pinned current verdict'),
    C('M11-V-F1-2', 'K', 'audit', 'Scheduled maintenance\nDate: Thursday 24th - Friday 25th September 2026\nTime: 22:00 - 02:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F1: pinned current verdict'),
    C('M11-V-F2-3', 'K', 'audit', 'Scheduled maintenance notice\nLast updated: 2026-09-22\nTime: 22:00 - 02:00 (GMT+8)\nDate: 2026-09-24 (Thu)', 'needs_human', None, None,
      note='fix-up V-F2: pinned current verdict'),
    C('M11-V-F2-4', 'K', 'audit', '维护公告\n更新日期：2026-09-22\n维护时间：22:00 - 02:00 (GMT+8)\n维护日期：2026年9月24日', 'needs_human', None, None,
      note='fix-up V-F2: pinned current verdict'),
    C('M11-V-F2-5', 'K', 'audit', '维护公告\n日期：2026年9月22日\n维护时间：22:00 - 02:00 (GMT+8)\n维护日期：2026年9月24日', 'needs_human', None, None,
      note='fix-up V-F2: pinned current verdict'),
    C('M11-V-F2-6', 'K', 'audit', 'Scheduled maintenance notice\nDate: 2026-09-22\nMaintenance time: 22:00 - 02:00 (GMT+8)\nMaintenance date: 2026-09-24', 'needs_human', None, None,
      note='fix-up V-F2: pinned current verdict'),
    C('M11-V-F10-7', 'K', 'audit', '维护公告\n日期：2026年9月23日\n北京时间：10:00-12:00\n世界协调时：02:00-04:00', 'needs_human', None, None,
      note='fix-up V-F10: pinned current verdict'),
    C('M11-V-F10-8', 'K', 'audit', '维护公告\n日期：2026年9月23日\n北京时间 01:00-03:00\n世界协调时 17:00-19:00', 'needs_human', None, None,
      note='fix-up V-F10: pinned current verdict'),
    C('M11-V-F12-9', 'K', 'audit', 'Scheduled maintenance on Sep 25, 2026 from 4:00 PM - 6:00 (+1) (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F12: pinned current verdict'),
    C('M11-V-F12-10', 'K', 'audit', 'Scheduled maintenance on Sep 25, 2026 from 10:00 - 11:30 in the evening (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F12: pinned current verdict'),
    C('M11-V-F12-11', 'K', 'audit', 'Scheduled maintenance on Sep 25, 2026 from 3:00 - 5:00 in the afternoon (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F12: pinned current verdict'),
    C('M11-V-F13-12', 'K', 'audit', '系统维护通知\n维护时间：2026年9月23日 今晚10:00-11:30', 'needs_human', None, None,
      note='fix-up V-F13: pinned current verdict'),
    C('M11-V-F13-13', 'K', 'audit', '系统维护通知\n维护时间：2026年9月23日 午后2:00-4:00', 'needs_human', None, None,
      note='fix-up V-F13: pinned current verdict'),
    C('M11-V-F13-14', 'K', 'audit', '系统维护通知\n维护时间：2026年9月23日 PM 2:00-4:00', 'needs_human', None, None,
      note='fix-up V-F13: pinned current verdict'),
    C('M11-V-F14-15', 'K', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 (EST: 2 HRS)', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up V-F14: pinned current verdict'),
    C('M11-V-F14-16', 'K', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 ICT Support will monitor', 'needs_human', None, None,
      note='fix-up V-F14: pinned current verdict'),
    C('M11-V-F14-17', 'K', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 PST (America/Los_Angeles)', 'needs_human', None, None,
      note='fix-up V-F14: pinned current verdict'),
    C('M11-V-F14-18', 'K', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 PST, i.e. 02:00 - 04:00 UTC+8 on 09-25', 'needs_human', None, None,
      note='fix-up V-F14: pinned current verdict'),
    C('M11-V-F15-19', 'K', 'audit', 'Scheduled maintenance\nDate: 2026-09-23\nTime: 10:00 - 12:00 (GMT+8)\nCS: 09:00 - 18:00', 'needs_human', None, None,
      note='fix-up V-F15: pinned current verdict'),
    C('M11-V-F15-20', 'K', 'audit', '系统维护通知\n日期：2026年9月23日\n维护时间：14:00-16:00 (GMT+8)\n技术支持：08:00-10:00', 'needs_human', None, None,
      note='fix-up V-F15: pinned current verdict'),
    C('M11-V-F16-21', 'K', 'audit', 'Scheduled maintenance\nDate: 2026-09-24\nTime: 10:00 - 12:00\n*All times are shown in CEST', 'needs_human', None, None,
      note='fix-up V-F16: pinned current verdict'),
    C('M11-V-F16-22', 'K', 'audit', '系统维护\n日期：2026-09-24\n时间：10:00-12:00（世界协调时）', 'needs_human', None, None,
      note='fix-up V-F16: pinned current verdict'),
    C('M11-V-F17-23', 'K', 'audit', 'Maintenance Notice\nMaintenance: 2026-09-30 07:00-09:00 (GMT+8)\nFree spins: 2026-09-25 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F17: pinned current verdict'),
    C('M11-V-F17-24', 'K', 'audit', 'Maintenance Notice\nMaintenance: 2026-09-30 07:00-09:00 (GMT+8)\nHappy hour: 2026-09-30 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F17: pinned current verdict'),
    C('M11-V-F18-25', 'K', 'audit', 'Platform upgrade 2026-09-24 10:00-12:00 (GMT+8). All games stay online.', 'needs_human', None, None,
      note='fix-up V-F18: pinned current verdict'),
    C('M11-V-F19-26', 'K', 'audit', '中秋节假期，客服部门暂停服务：2026年9月25日 10:00-18:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F19: pinned current verdict'),
    C('M11-V-F19-27', 'K', 'audit', 'Please be informed that our email service will be unavailable 2026-09-25 10:00-18:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-F19: pinned current verdict'),
    C('M11-V-F20-28', 'K', 'audit', 'The draw has been postponed to 2026-09-26 20:00-22:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-F20: pinned current verdict'),
    C('M11-V-F20-29', 'K', 'audit', '我们希望维护改期至2026年9月26日 20:00-22:00 (GMT+8)，请确认', 'needs_human', None, None,
      note='fix-up V-F20: pinned current verdict'),
    C('M11-V-F21-30', 'K', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8) is not going ahead.', 'needs_human', None, None,
      note='fix-up V-F21: pinned current verdict'),
    C('M11-V-F22-31', 'K', 'audit', 'Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8) -> 2026-09-25 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F22: pinned current verdict'),
    C('M11-V-F22-32', 'K', 'audit', 'Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8) -> 2026-09-23 14:00-16:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F22: pinned current verdict'),
    C('M11-V-F23-33', 'K', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8); it has been postponed.', 'needs_human', None, None,
      note='fix-up V-F23: pinned current verdict'),
    C('M11-V-F24-34', 'K', 'audit', '【维护通知】\n维护时间：2026-09-23 10:00-12:00 (GMT+8)\n此次取消。', 'needs_human', None, None,
      note='fix-up V-F24: pinned current verdict'),
    C('M11-V-F25-35', 'K', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 (GMT+8) and 2026-09-24 14:00 - 16:00 (GMT+8). The second one has been cancelled.', 'needs_human', None, None,
      note='fix-up V-F25: pinned current verdict'),
    C('M11-V-F8-36', 'K', 'audit', '系统维护 2026-09-23 10:00-12:00 GMT时间+8', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up V-F8: pinned current verdict'),
    C('M11-V-F9-37', 'K', 'audit', 'Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8) (02:00-04:00)', 'needs_human', None, None,
      note='fix-up V-F9: pinned current verdict'),
    C('M11-V-G1.4-38', 'K', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8)\nCould you confirm this is correct?', 'needs_human', None, None,
      note='fix-up V-G1.4: pinned current verdict'),
    C('M11-V-G1.4-39', 'K', 'audit', '系统维护：2026年9月25日 14:00-16:00 (GMT+8)\n请确认是否正确？', 'needs_human', None, None,
      note='fix-up V-G1.4: pinned current verdict'),
    C('M11-V-G1.1-40', 'K', 'audit', 'Evolution maintenance 25/09/2026 14:00-16:00 (GMT+8) will not affect the live games.', 'needs_human', None, None,
      note='fix-up V-G1.1: pinned current verdict'),
    C('M11-V-G1.1-41', 'K', 'audit', '游戏维护：2026年9月25日 14:00-16:00 (GMT+8)，不影响玩家游戏。', 'needs_human', None, None,
      note='fix-up V-G1.1: pinned current verdict'),
    C('M11-V-G1.2-42', 'K', 'audit', '游戏维护：25/09/2026 14:00-16:00 (GMT+8)，玩家无感知。', 'needs_human', None, None,
      note='fix-up V-G1.2: pinned current verdict'),
    C('M11-V-G1.5-43', 'K', 'audit', 'Hi all, John will be offline on 25/09/2026 14:00-16:00 (GMT+8). Please contact Mary instead.', 'needs_human', None, None,
      note='fix-up V-G1.5: pinned current verdict'),
    C('M11-V-G1.6-44', 'K', 'audit', 'Maintenance completed. Next maintenance was planned for 25/09/2026 14:00-16:00 (GMT+8) but is now postponed.', 'needs_human', None, None,
      note='fix-up V-G1.6: pinned current verdict'),
    C('M11-V-G1.7-45', 'K', 'audit', '新版本上线：2026年9月24日 10:00-11:00 (GMT+8)，服务中断：无。', 'needs_human', None, None,
      note='fix-up V-G1.7: pinned current verdict'),
    C('M11-V-G1.8-46', 'K', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (U\u200bTC)', 'fill', "2026-09-25T14:00:00+00:00", "2026-09-25T16:00:00+00:00",
      note='fix-up V-G1.8: pinned current verdict'),
    C('M11-V-G1.8-47', 'K', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT﹣5)', 'fill', "2026-09-25T14:00:00-05:00", "2026-09-25T16:00:00-05:00",
      note='fix-up V-G1.8: pinned current verdict'),
    C('M11-V-G2.1-48', 'K', 'audit', 'Apologies, the maintenance announcement for 24/09/2026 10:00-12:00 (GMT+8) was not for your group.', 'needs_human', None, None,
      note='fix-up V-G2.1: pinned current verdict'),
    C('M11-V-G2.1-49', 'K', 'audit', 'We would like to withdraw the scheduled maintenance notice for 2026-09-24 10:00-12:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-G2.1: pinned current verdict'),
    C('M11-V-G2.1-50', 'K', 'audit', '请无视 2026-09-24 10:00-12:00 (GMT+8) 系统维护通知，谢谢。', 'needs_human', None, None,
      note='fix-up V-G2.1: pinned current verdict'),
    C('M11-V-G2.1-51', 'K', 'audit', '撤销：2026-09-24 10:00-12:00 (GMT+8) 系统维护', 'needs_human', None, None,
      note='fix-up V-G2.1: pinned current verdict'),
    C('M11-V-G2.1-52', 'K', 'audit', 'Maintenance notice 2026-09-24 10:00-12:00 (GMT+8) sent to this group by mistake. Please ignore.', 'needs_human', None, None,
      note='fix-up V-G2.1: pinned current verdict'),
    C('M11-V-G2.1-53', 'K', 'audit', 'Scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8) - this notice is not applicable to you, please ignore.', 'needs_human', None, None,
      note='fix-up V-G2.1: pinned current verdict'),
    C('M11-V-G2.1-54', 'K', 'audit', 'Scheduled maintenance 24/09 10:00-12:00 notice was a mistake. There will be no maintenance.', 'needs_human', None, None,
      note='fix-up V-G2.1: pinned current verdict'),
    C('M11-V-G2.2-55', 'K', 'audit', 'Scheduled maintenance: 26/09/2026 10:00-12:00 (GMT+8) (previous notice said 24/09/2026 10:00-12:00 by mistake)', 'needs_human', None, None,
      note='fix-up V-G2.2: pinned current verdict'),
    C('M11-V-G2.2-56', 'K', 'audit', 'Kindly note the correct scheduled maintenance time is 26/09/2026 10:00-12:00 (GMT+8). Please ignore 24/09/2026 10:00-12:00.', 'needs_human', None, None,
      note='fix-up V-G2.2: pinned current verdict'),
    C('M11-V-G2.2-57', 'K', 'audit', 'Scheduled maintenance: 24/09/2026 10:00-12:00 -> 26/09/2026 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-G2.2: pinned current verdict'),
    C('M11-V-G4.3-58', 'K', 'audit', 'Suggest to reschedule the maintenance to 2026-09-25 03:00-05:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-G4.3: pinned current verdict'),
    C('M11-V-G4.3-59', 'K', 'audit', 'Please consider postponing the scheduled maintenance to 2026-09-25 03:00-05:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-G4.3: pinned current verdict'),
    C('M11-V-G4.3-60', 'K', 'audit', 'Our management requests the maintenance to be rescheduled to 2026-09-25 03:00-05:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-G4.3: pinned current verdict'),
    C('M11-V-G4.3-61', 'K', 'audit', 'Please reschedule to 2026-09-25 03:00-05:00 (GMT+8) instead.', 'needs_human', None, None,
      note='fix-up V-G4.3: pinned current verdict'),
    C('M11-V-G4.3-62', 'K', 'audit', '请贵司将维护延期至2026-09-25 03:00-05:00 (GMT+8)。', 'needs_human', None, None,
      note='fix-up V-G4.3: pinned current verdict'),
    C('M11-V-F6-63', 'K', 'audit', '国庆节(10月1日)前，例行维护安排在9/30 10:00-12:00 (GMT+8)。', 'needs_human', None, None,
      note='fix-up V-F6: pinned current verdict'),
    C('M11-V-F6-64', 'K', 'audit', '例行维护时间为9/30 10:00-12:00 (GMT+8)，10月1日起恢复正常。', 'needs_human', None, None,
      note='fix-up V-F6: pinned current verdict'),
    C('M11-V-F6-65', 'K', 'audit', '10月1日国庆节，本周维护提前至9/30 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F6: pinned current verdict'),
    C('M11-V-F6-66', 'K', 'audit', '由于国庆节（10月1日），例行维护时间调整为9/30 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F6: pinned current verdict'),
    C('M11-V-F60-67', 'K', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); sorry, it has been cancelled.', 'needs_human', None, None,
      note='fix-up V-F60: pinned current verdict'),
    C('M11-V-F60-68', 'K', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8); the downtime is cancelled.', 'needs_human', None, None,
      note='fix-up V-F60: pinned current verdict'),
    C('M11-V-F60-69', 'K', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 - 12:00 (GMT+8). The downtime is cancelled.', 'needs_human', None, None,
      note='fix-up V-F60: pinned current verdict'),
    C('M11-V-F62-70', 'K', 'audit', 'Scheduled maintenance 25.09.2026, expected downtime: 2.00 - 3.00 hours', 'needs_human', None, None,
      note='fix-up V-F62: pinned current verdict'),
    C('M11-V-F62-71', 'K', 'audit', 'Scheduled maintenance 25.09.2026, maintenance duration 2.00 - 3.00 hours', 'needs_human', None, None,
      note='fix-up V-F62: pinned current verdict'),
    C('M11-V-F42-72', 'K', 'audit', 'Scheduled maintenance on 24-Sep-26 10:00 - 12:00 (GMT+8).', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up V-F42: pinned current verdict'),
    C('M11-V-F42-73', 'K', 'audit', 'Scheduled maintenance on 05-Oct-26 10:00 - 12:00 (GMT+8).', 'fill', "2026-10-05T10:00:00+08:00", "2026-10-05T12:00:00+08:00",
      note='fix-up V-F42: pinned current verdict'),
    C('M11-V-F42-74', 'K', 'audit', 'Further to our email of 2026-09-23, scheduled maintenance on 24/Sep 10:00 - 12:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-F42: pinned current verdict'),
    C('M11-V-F44-75', 'K', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 (GMT+8), expected to last 2 hours; support 09:00 到 18:00.', 'needs_human', None, None,
      note='fix-up V-F44: pinned current verdict'),
    C('M11-V-F45-76', 'K', 'audit', '麻烦确认一下2026年9月24日 10:00-12:00 (GMT+8)的例行维护', 'needs_human', None, None,
      note='fix-up V-F45: pinned current verdict'),
    C('M11-V-F45-77', 'K', 'audit', 'Hi team, will there be maintenance on 2026-09-24 10:00-12:00 (GMT+8)', 'ignore', None, None,
      note='fix-up V-F45: pinned current verdict'),
    C('M11-V-F45-78', 'K', 'audit', 'Any scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8)', 'ignore', None, None,
      note='fix-up V-F45: pinned current verdict'),
    C('M11-V-F46-79', 'K', 'audit', 'Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8)\nThis maintenance only applies to the test server.', 'needs_human', None, None,
      note='fix-up V-F46: pinned current verdict'),
    C('M11-V-F46-80', 'K', 'audit', 'Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). Only the UAT will be down.', 'needs_human', None, None,
      note='fix-up V-F46: pinned current verdict'),
    C('M11-V-F46-81', 'K', 'audit', 'Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8).\n\n\n\n\n\nPS: this is for the test environment only, production is fine.', 'needs_human', None, None,
      note='fix-up V-F46: pinned current verdict'),
    C('M11-V-F46-82', 'K', 'audit', 'Scheduled maintenance for the integration environment (INT) 2026-09-24 10:00-12:00 (GMT+8).', 'ignore', None, None,
      note='fix-up V-F46: pinned current verdict'),
    C('M11-V-F47-83', 'K', 'audit', '例行维护：2026-09-23 10:00-12:00 (GMT+8)，维护大约在11:30已完成。', 'ignore', None, None,
      note='fix-up V-F47: pinned current verdict'),
    C('M11-V-F48-84', 'K', 'audit', 'The upcoming maintenance is cancelled, next scheduled maintenance: 2026-09-30 10:00 - 12:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-F48: pinned current verdict'),
    C('M11-V-F48-85', 'K', 'audit', '上次维护已完成，下次维护 2026-09-30 10:00-12:00 (GMT+8) 暂停。', 'needs_human', None, None,
      note='fix-up V-F48: pinned current verdict'),
    C('M11-V-F49-86', 'K', 'audit', 'The Slots maintenance on 2026-09-24 10:00-11:00 has been cancelled. The Live Casino maintenance on 2026-09-24 14:00-16:00 (GMT+8) has also been cancelled.', 'needs_human', None, None,
      note='fix-up V-F49: pinned current verdict'),
    C('M11-V-F50-87', 'K', 'audit', 'The maintenance on 2026-09-23 10:00-12:00 (GMT+8) will be prolonged until 14:00.', 'needs_human', None, None,
      note='fix-up V-F50: pinned current verdict'),
    C('M11-V-F50-88', 'K', 'audit', '例行维护 2026-09-23 10:00-12:00 (GMT+8) 结束时间调整为14:00。', 'needs_human', None, None,
      note='fix-up V-F50: pinned current verdict'),
    C('M11-V-F51-89', 'K', 'audit', '2026-09-25 10:00-12:00 的维护无需进行', 'needs_human', None, None,
      note='fix-up V-F51: pinned current verdict'),
    C('M11-V-F51-90', 'K', 'audit', '~~Scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8).~~ Cancelled', 'needs_human', None, None,
      note='fix-up V-F51: pinned current verdict'),
    C('M11-V-F51-91', 'K', 'audit', 'JILI maintenance on 2026-09-25 10:00-12:00 has been cancelled.', 'ignore', None, None,
      note='fix-up V-F51: pinned current verdict'),
    C('M11-V-F52-92', 'K', 'audit', 'Maintenance on 2026-09-23 10:00-12:00 is moved one day to 2026-09-24 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F52: pinned current verdict'),
    C('M11-V-F52-93', 'K', 'audit', 'Maintenance on 2026-09-23 10:00-12:00 (GMT+8) will instead be on 2026-09-24.', 'needs_human', None, None,
      note='fix-up V-F52: pinned current verdict'),
    C('M11-V-F53-94', 'K', 'audit', 'Please disregard: scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8). We will share an updated schedule.', 'needs_human', None, None,
      note='fix-up V-F53: pinned current verdict'),
    C('M11-V-F53-95', 'K', 'audit', '请忽略：例行维护 2026-09-23 10:00-12:00 (GMT+8)，新的时间另行通知。', 'needs_human', None, None,
      note='fix-up V-F53: pinned current verdict'),
    C('M11-V-F53-96', 'K', 'audit', 'Correction: the maintenance notice for 2026-09-23 10:00-12:00 (GMT+8) was sent in error.', 'needs_human', None, None,
      note='fix-up V-F53: pinned current verdict'),
    C('M11-V-F54-97', 'K', 'audit', 'Scheduled maintenance previously announced for 2026-09-23 10:00-12:00 (GMT+8) has been updated to 2026-09-25 10:00-12:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-F54: pinned current verdict'),
    C('M11-V-F54-98', 'K', 'audit', '先前通知的维护 2026-09-23 10:00-12:00 (GMT+8) 更改为 2026-09-25 10:00-12:00 (GMT+8)。', 'needs_human', None, None,
      note='fix-up V-F54: pinned current verdict'),
    C('M11-V-F54-99', 'K', 'audit', 'Scheduled maintenance previously announced for 2026-09-23 10:00-12:00 (GMT+8); correct time is 2026-09-25 10:00-12:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-F54: pinned current verdict'),
    C('M11-V-F54-100', 'K', 'audit', 'Scheduled maintenance previously announced for 2026-09-23 10:00-12:00 (GMT+8). Please note the new time 2026-09-23 14:00-16:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-F54: pinned current verdict'),
    C('M11-V-F54-101', 'K', 'audit', 'Scheduled maintenance previously announced for 2026-09-23 10:00-12:00 (GMT+8) is amended to 2026-09-23 14:00-16:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-F54: pinned current verdict'),
    C('M11-V-F4-102', 'K', 'audit', 'Further to the 2026-09-22 notice, scheduled maintenance 10:00-12:00 (GMT+8) - 2026-09-24', 'needs_human', None, None,
      note='fix-up V-F4: pinned current verdict'),
    C('M11-V-F4-103', 'K', 'audit', 'As announced on 22/09, scheduled maintenance is 10:00-12:00 (GMT+8), 24/09.', 'needs_human', None, None,
      note='fix-up V-F4: pinned current verdict'),
    C('M11-V-F4-104', 'K', 'audit', '国庆(10月1日)后首次例行维护时间为 10:00-12:00 (GMT+8)，周四10月8日。', 'needs_human', None, None,
      note='fix-up V-F4: pinned current verdict'),
    C('M11-V-F26-105', 'K', 'audit', '例行维护 2026-09-23 10:00-12:00 (GMT+8)，已推迟，另行通知。', 'needs_human', None, None,
      note='fix-up V-F26: pinned current verdict'),
    C('M11-V-F26-106', 'K', 'audit', '例行维护 2026-09-23 10:00-12:00 (GMT+8) —— 已推迟', 'needs_human', None, None,
      note='fix-up V-F26: pinned current verdict'),
    C('M11-V-F26-107', 'K', 'audit', 'POSTPONED: Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F26: pinned current verdict'),
    C('M11-V-F26-108', 'K', 'audit', 'Scheduled maintenance delayed: 2026-09-23 10:00-12:00 (GMT+8) -> TBA', 'needs_human', None, None,
      note='fix-up V-F26: pinned current verdict'),
    C('M11-V-F28-109', 'K', 'audit', '【维护时间更新】\n例行维护：2026-09-23 10:00-12:00 (GMT+8)\n最新维护时间：2026-09-25 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F28: pinned current verdict'),
    C('M11-V-F28-110', 'K', 'audit', '例行维护：2026-09-23 10:00-12:00 (GMT+8)\n客户端发布时间：2026-09-24 02:00-03:00', 'needs_human', None, None,
      note='fix-up V-F28: pinned current verdict'),
    C('M11-V-F29-111', 'K', 'audit', 'Scheduled maintenance change\nFrom: 2026-09-23 10:00-12:00\nTo: 2026-09-25 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-F29: pinned current verdict'),
    C('M11-V-F30-112', 'K', 'audit', 'We have decided to cancel the scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-F30: pinned current verdict'),
    C('M11-V-F31-113', 'K', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\nAccount manager: 09:00-18:00', 'needs_human', None, None,
      note='fix-up V-F31: pinned current verdict'),
    C('M11-V-F32-114', 'K', 'audit', '例行维护：2026-09-23 10:00-12:00 (GMT+8)\n中秋节：2026-09-22 20:00 - 2026-09-24 11:00', 'needs_human', None, None,
      note='fix-up V-F32: pinned current verdict'),
    C('M11-V-F33-115', 'K', 'audit', '例行维护 (GMT+8)\n- 2026-09-23 22:00 - 2026-09-24 02:00\n- 2026-09-24 22:00 - 2026-09-25 02:00', 'fill', "2026-09-23T22:00:00+08:00", "2026-09-24T02:00:00+08:00",
      note='fix-up V-F33: pinned current verdict'),
    C('M11-V-regress#1-116', 'K', 'audit', 'Scheduled maintenance 25/09/2026 14:00-16:00 (GMT+8)\nPlease confirm?', 'needs_human', None, None,
      note='fix-up V-regress#1: pinned current verdict'),
    C('M11-V-regress#1-117', 'K', 'audit', '系统维护：2026年9月25日 14:00-16:00 (GMT+8)\n确认是这个时间吗？', 'needs_human', None, None,
      note='fix-up V-regress#1: pinned current verdict'),
    C('M11-V-regress#2-118', 'K', 'audit', 'Maintenance\nPrevious date: 25/09/2026 14:00-16:00 (GMT+8)\nNew date: will be informed later', 'needs_human', None, None,
      note='fix-up V-regress#2: pinned current verdict'),
    C('M11-V-regress#3-119', 'K', 'audit', "Maintenance 25/09/2026 14:00-16:00 (GMT+8) won't affect gameplay.", 'needs_human', None, None,
      note='fix-up V-regress#3: pinned current verdict'),
    C('M11-V-regress#4-120', 'K', 'audit', 'Maintenance: none this week. Event 25/09/2026 14:00-16:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-regress#4: pinned current verdict'),
    C('M11-V-regress#6-121', 'K', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 PST (California time)', 'needs_human', None, None,
      note='fix-up V-regress#6: pinned current verdict'),
    C('M11-V-regress#9-122', 'K', 'audit', 'Scheduled maintenance\nDate: 2026-09-23\nTime: 14:00 - 16:00 (GMT+8)\nOn-call: 08:00 - 10:00', 'needs_human', None, None,
      note='fix-up V-regress#9: pinned current verdict'),
    C('M11-V-regress#10-123', 'K', 'audit', '维护公告\n日期：2026年9月23日\n北京时间：10:00-12:00\n服务器时间：02:00-04:00', 'needs_human', None, None,
      note='fix-up V-regress#10: pinned current verdict'),
    C('M11-V-regress#13-124', 'K', 'audit', '维护 2026-09-23 10:00-12:00 (GMT+8) 改成 2026-09-25 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-regress#13: pinned current verdict'),
    C('M11-V-regress#16-125', 'K', 'audit', 'Disregard the maintenance notice for 24/09 10:00-12:00; there will be no maintenance.', 'needs_human', None, None,
      note='fix-up V-regress#16: pinned current verdict'),
    C('M11-V-regress#19-126', 'K', 'audit', '我们已取消2026-09-23 10:00-12:00 (GMT+8)游戏维护。', 'needs_human', None, None,
      note='fix-up V-regress#19: pinned current verdict'),
    C('M11-V-regress#19-127', 'K', 'audit', '暂停2026-09-23 10:00-12:00 (GMT+8)的维护。', 'needs_human', None, None,
      note='fix-up V-regress#19: pinned current verdict'),
    C('M11-V-regress#26-128', 'K', 'audit', 'Scheduled maintenance on 2026-09-24 10:00 (GMT+8), estimated to last 2 hours; CS online 09:00 through 18:00.', 'needs_human', None, None,
      note='fix-up V-regress#26: pinned current verdict'),
    C('M11-V-regress#29-129', 'K', 'audit', '~~Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8)~~ An updated schedule will be shared.', 'needs_human', None, None,
      note='fix-up V-regress#29: pinned current verdict'),
    C('M11-V-regress#32-130', 'K', 'audit', 'Scheduled maintenance 2026-09-23 10:00-12:00 (GMT+8) will be shifted. Updated schedule to follow.', 'needs_human', None, None,
      note='fix-up V-regress#32: pinned current verdict'),
    C('M11-V-regress#33-131', 'K', 'audit', 'Scheduled maintenance previously shared: 2026-09-23 10:00-12:00 (GMT+8). Latest: 2026-09-25 10:00-12:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-regress#33: pinned current verdict'),
    C('M11-V-regress#34-132', 'K', 'audit', 'Scheduled maintenance previously communicated for 2026-09-23 10:00-12:00 (GMT+8) has been updated. New time: 2026-09-23 14:00-16:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up V-regress#34: pinned current verdict'),
    C('M11-V-regress#35-133', 'K', 'audit', '例行维护：2026-09-23 10:00-12:00 (GMT+8)\n最新的时间：2026-09-25 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-regress#35: pinned current verdict'),
    C('M11-V-regress#36-134', 'K', 'audit', '2026-09-23 10:00-12:00 (GMT+8) 的维护将暂停，新时间另行通知。', 'needs_human', None, None,
      note='fix-up V-regress#36: pinned current verdict'),
    C('M11-V-regress#37-135', 'K', 'audit', 'Maintenance on 2026-09-23 10:00-12:00 (GMT+8) put on hold.', 'needs_human', None, None,
      note='fix-up V-regress#37: pinned current verdict'),
    C('M11-V-regress#38-136', 'K', 'audit', 'Scheduled maintenance: 26/09/2026 10:00-12:00 (GMT+8) (previously stated as 24/09/2026 10:00-12:00).', 'needs_human', None, None,
      note='fix-up V-regress#38: pinned current verdict'),
    C('M11-V-regress#39-137', 'K', 'audit', 'Scheduled maintenance update: 26/09/2026 10:00-12:00 (GMT+8). Previously shared: 24/09/2026 10:00-12:00.', 'needs_human', None, None,
      note='fix-up V-regress#39: pinned current verdict'),
    C('M11-V-regress#40-138', 'K', 'audit', 'Correction: scheduled maintenance time is 14:00-16:00 (GMT+8) on 24/09/2026 (10:00-12:00 was wrong).', 'needs_human', None, None,
      note='fix-up V-regress#40: pinned current verdict'),
    C('M11-V-regress#40-139', 'K', 'audit', '更正：维护时间为2026年9月24日 14:00-16:00 (GMT+8)，不是10:00-12:00。', 'needs_human', None, None,
      note='fix-up V-regress#40: pinned current verdict'),
    C('M11-V-regress#41-140', 'K', 'audit', '维护时间：2026-09-26 10:00-12:00 (GMT+8)，之前通知的2026-09-24 10:00-12:00请忽略。', 'needs_human', None, None,
      note='fix-up V-regress#41: pinned current verdict'),
    C('M11-V-regress#43-141', 'K', 'audit', 'Maintenance 2026-09-25 10:00 - 12:00 (GMT+8) aborted', 'needs_human', None, None,
      note='fix-up V-regress#43: pinned current verdict'),
    C('M11-V-regress#44-142', 'K', 'audit', '2026-09-25 10:00-12:00 维护已暂停', 'needs_human', None, None,
      note='fix-up V-regress#44: pinned current verdict'),
    C('M11-V-regress#55-143', 'K', 'audit', 'BO maintenance 2026-09-25 10:00-12:00. Front end not affected.', 'needs_human', None, None,
      note='fix-up V-regress#55: pinned current verdict'),
    C('M11-V-regress#56-144', 'K', 'audit', 'The maintenance on 2026-09-26 10:00-12:00 is paused.', 'needs_human', None, None,
      note='fix-up V-regress#56: pinned current verdict'),
    C('M11-V-regress#60-145', 'K', 'audit', 'Maintenance 2026-09-26 10:00-12:00 (TBD)', 'needs_human', None, None,
      note='fix-up V-regress#60: pinned current verdict'),
    C('M11-V-regress#61-146', 'K', 'audit', 'Scheduled maintenance on 2026-09-25 10:00-12:00 Zulu', 'needs_human', None, None,
      note='fix-up V-regress#61: pinned current verdict'),
    C('M11-V-regress#62-147', 'K', 'audit', 'Scheduled maintenance\nEurope - 2026-09-24 02:00-04:00 (GMT+8)\nAsia - 2026-09-25 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up V-regress#62: pinned current verdict'),
    C('M11-V-regress#66-148', 'K', 'audit', 'Scheduled maintenance 2026-09-25 10:00-12:00. The ticketing system will be down 09:00-13:00.', 'needs_human', None, None,
      note='fix-up V-regress#66: pinned current verdict'),
    C('M11-V-regress#68-149', 'K', 'audit', 'Scheduled maintenance Date: 24/09 - 25/09/2026 Time: 22:00 - 02:00 (GMT+8)', 'fill', "2026-09-24T22:00:00+08:00", "2026-09-25T02:00:00+08:00",
      note='fix-up V-regress#68: pinned current verdict'),
    C('M11-V-regress#71-150', 'K', 'audit', 'We saw your maintenance notice. Is the maintenance on 2026-10-14 10:00-12:00 (GMT+8) confirmed?', 'needs_human', None, None,
      note='fix-up V-regress#71: pinned current verdict'),
    C('M11-V-regress#72-151', 'K', 'audit', 'UAT环境维护 2026-10-14 10:00 - 12:00 (GMT+8)。', 'ignore', None, None,
      note='fix-up V-regress#72: pinned current verdict'),
    C('M11-V-regress#75-152', 'K', 'audit', 'Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)\nKindly ignore the above, wrong group.', 'needs_human', None, None,
      note='fix-up V-regress#75: pinned current verdict'),
    # -- M12: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M12-D-F25-1', 'K', 'audit', 'Scheduled maintenance 2026-09-24 10:00 - 12:00 (GMT+8). Note: 2026-09-24 14:00 - 16:00 (GMT+8) cancelled.', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up D-F25: pinned current verdict'),
    C('M12-D-G1.1-2', 'K', 'audit', 'Please note we are doing maintenance on 25/09/2026 14:00-16:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up D-G1.1: pinned current verdict'),
    C('M12-D-G1.2-3', 'K', 'audit', '维护定于 25/09/2026 14:00-16:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up D-G1.2: pinned current verdict'),
    C('M12-D-G1.5-4', 'K', 'audit', 'PG電子將於2026年9月25日 14:00-16:00 (GMT+8)停機。', 'needs_human', None, None,
      note='fix-up D-G1.5: pinned current verdict'),
    C('M12-D-G1.6-5', 'K', 'audit', 'Maintenance is over. Following maintenance: 25/09/2026 14:00-16:00 (GMT+8).', 'fill', "2026-09-25T14:00:00+08:00", "2026-09-25T16:00:00+08:00",
      note='fix-up D-G1.6: pinned current verdict'),
    C('M12-D-G2.2-6', 'K', 'audit', 'The 24/09 maintenance is cancelled. New scheduled maintenance: 26/09/2026 10:00-12:00 (GMT+8).', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up D-G2.2: pinned current verdict'),
    C('M12-D-F6-7', 'K', 'audit', 'Following our 2026/09/17 maintenance, the next scheduled maintenance is 9/24 10:00 - 12:00 GMT+8.', 'needs_human', None, None,
      note='fix-up D-F6: pinned current verdict'),
    C('M12-D-F6-8', 'K', 'audit', '继9月17日维护后，下次例行维护为9/24 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up D-F6: pinned current verdict'),
    C('M12-D-F6-9', 'K', 'audit', '上周维护(9月16日)已完成，下次维护改至9/24 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up D-F6: pinned current verdict'),
    C('M12-D-F64-10', 'K', 'audit', "Should there be no maintenance, please reply 'No'.", 'ignore', None, None,
      note='fix-up D-F64: pinned current verdict'),
    C('M12-D-F64-11', 'K', 'audit', '确认下本周没有维护哈', 'ignore', None, None,
      note='fix-up D-F64: pinned current verdict'),
    C('M12-D-F64-12', 'K', 'audit', '本周没有维护对吧', 'ignore', None, None,
      note='fix-up D-F64: pinned current verdict'),
    C('M12-D-F65-13', 'K', 'audit', '例行维护 2026-09-23 10:00 - 12:00 (GMT+8)，现已结束，游戏恢复正常。', 'needs_human', None, None,
      note='fix-up D-F65: pinned current verdict'),
    C('M12-D-F65-14', 'K', 'audit', 'Scheduled maintenance 2026-09-23 10:00 - 12:00 (GMT+8)\n\nUpdate 11:20: completed.', 'needs_human', None, None,
      note='fix-up D-F65: pinned current verdict'),
    C('M12-D-F66-15', 'K', 'audit', '今天的更新没有维护。', 'ignore', None, None,
      note='fix-up D-F66: pinned current verdict'),
    C('M12-D-F66-16', 'K', 'audit', 'Config change today, no maintenance on our side.', 'ignore', None, None,
      note='fix-up D-F66: pinned current verdict'),
    C('M12-D-F67-17', 'K', 'audit', 'Reminder: maintenance 2026-09-25 00:00 - 02:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up D-F67: pinned current verdict'),
    C('M12-D-F67-18', 'K', 'audit', 'Reminder: Phase 2 maintenance 2026-09-25 00:00 - 02:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up D-F67: pinned current verdict'),
    C('M12-D-F67-19', 'K', 'audit', 'Maintenance reminder 2026-09-25 00:00 - 02:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up D-F67: pinned current verdict'),
    C('M12-D-F45-20', 'K', 'audit', '我司将于2026年9月24日 10:00-12:00 (GMT+8) 进行系统维护，请问有什么问题吗？', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up D-F45: pinned current verdict'),
    C('M12-D-F47-21', 'K', 'audit', '例行维护：2026-09-23 10:00-12:00 (GMT+8)，维护应该在12:00结束。', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up D-F47: pinned current verdict'),
    C('M12-D-F48-22', 'K', 'audit', 'Previous maintenance done, next one: 2026-09-30 10:00 - 12:00 (GMT+8).', 'fill', "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      note='fix-up D-F48: pinned current verdict'),
    C('M12-D-F49-23', 'K', 'audit', 'The maintenance on 2026-09-23 10:00-12:00 was cancelled. We will do it on 2026-09-30 10:00-12:00 (GMT+8).', 'fill', "2026-09-30T10:00:00+08:00", "2026-09-30T12:00:00+08:00",
      note='fix-up D-F49: pinned current verdict'),
    C('M12-D-F49-24', 'K', 'audit', 'The maintenance planned on 2026-09-23 10:00-12:00 (GMT+8) will now happen on 2026-09-30 10:00-12:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up D-F49: pinned current verdict'),
    C('M12-D-F5-25', 'K', 'audit', 'Scheduled maintenance on 10/8/2026 10:00 - 12:00 (GMT+8)', 'fill', "2026-08-10T10:00:00+08:00", "2026-08-10T12:00:00+08:00",
      note='fix-up D-F5: pinned current verdict'),
    C('M12-D-F50-26', 'K', 'audit', 'Maintenance overrun: 2026-09-23 10:00-12:00 (GMT+8) will now end at 14:00.', 'needs_human', None, None,
      note='fix-up D-F50: pinned current verdict'),
    C('M12-D-F51-27', 'K', 'audit', 'We will not be performing the maintenance on 2026-09-25 10:00-12:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up D-F51: pinned current verdict'),
    C('M12-D-F52-28', 'K', 'audit', 'Maintenance date change: 2026-09-23 to 2026-09-25, 10:00-12:00 (GMT+8)', 'needs_human', None, None,
      note='fix-up D-F52: pinned current verdict'),
    C('M12-D-F54-29', 'K', 'audit', 'The maintenance previously communicated for 2026-09-23 10:00-12:00 (GMT+8) is revised: 2026-09-25 10:00-12:00 (GMT+8).', 'needs_human', None, None,
      note='fix-up D-F54: pinned current verdict'),
    C('M12-D-F4-30', 'K', 'audit', '上周（9月17日）维护延误，本周例行维护时间为 10:00-12:00 (GMT+8)，周四9月24日。', 'needs_human', None, None,
      note='fix-up D-F4: pinned current verdict'),
    C('M12-D-regress#8-31', 'K', 'audit', '收到维护通知，谢谢，明天10:00见', 'ignore', None, None,
      note='fix-up D-regress#8: pinned current verdict'),
    C('M12-D-regress#21-32', 'K', 'audit', '【维护通知】\n维护时间：2026-09-23 10:00-12:00 (GMT+8)\n维护12:00结束。', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up D-regress#21: pinned current verdict'),
    C('M12-D-regress#27-33', 'K', 'audit', 'Scheduled maintenance 2026-09-24 10:00-12:00 (GMT+8). Aviator only; other games will not be affected.', 'needs_human', None, None,
      note='fix-up D-regress#27: pinned current verdict'),
    C('M12-D-regress#74-34', 'K', 'audit', '系统维护通知\n维护时间：2026年10月14日 10:00-12:00 (GMT+8)\n维护完成通知将另行发送。', 'fill', "2026-10-14T10:00:00+08:00", "2026-10-14T12:00:00+08:00",
      note='fix-up D-regress#74: pinned current verdict'),
    # -- M13: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M13-W-G4.4-1', 'K', 'audit', 'Scheduled maintenance 25/09/2026 10:00-12:00 (GMT+8). Games stay online.', 'needs_human', None, None,
      note='fix-up W-G4.4: pinned current verdict'),
    C('M13-W-G4.4-2', 'K', 'audit', 'Scheduled maintenance on 25/09/2026 10:00-12:00 (GMT+8) - affects the Brazil jurisdiction only.', 'needs_human', None, None,
      note='fix-up W-G4.4: pinned current verdict'),
    C('M13-W-G4.4-3', 'K', 'audit', '后台将于2026年9月25日10:00-12:00 (GMT+8) 维护，前台游戏正常。', 'needs_human', None, None,
      note='fix-up W-G4.4: pinned current verdict'),
    C('M13-W-F51-4', 'K', 'audit', 'The maintenance on 2026-09-25 10:00-12:00 (GMT+8) has been dropped.', 'needs_human', None, None,
      note='fix-up W-F51: pinned current verdict'),
    C('M13-W-G1.3-5', 'K', 'audit', '收到维护通知，谢谢，明天10:00再联系', 'ignore', None, None,
      note='fix-up W-G1.3: pinned current verdict'),
    C('M13-W-F4-6', 'K', 'audit', 'Following the 2026-09-24 maintenance, the next scheduled maintenance is 10:00 - 12:00 (GMT+8) on Thursday.', 'needs_human', None, None,
      note='fix-up W-F4: pinned current verdict'),
    C('M13-W-keep-7', 'K', 'audit', '系统维护 2026年9月24日 10:00-12:00 (GMT+8)，维护结束后游戏将正常运行。', 'fill', "2026-09-24T10:00:00+08:00", "2026-09-24T12:00:00+08:00",
      note='fix-up W-keep: pinned current verdict'),
    C('M13-W-keep-8', 'K', 'audit', 'Scheduled maintenance on 2026-09-23 10:00-12:00 (GMT+8).\nNote: the Mid-Autumn promotion (2026-09-20 00:00 - 2026-09-30 23:59) is not affected.', 'fill', "2026-09-23T10:00:00+08:00", "2026-09-23T12:00:00+08:00",
      note='fix-up W-keep: pinned current verdict'),
    # -- M14: pinned by the main-loop fix-up (see audit2/me/cases.py) --
    C('M14-H-hunt-1', 'K', 'audit', '【维护通知】\n维护时间：2026年9月26日 09:00-11:00（东七区）', 'fill', "2026-09-26T09:00:00+07:00", "2026-09-26T11:00:00+07:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-2', 'K', 'audit', 'Scheduled maintenance on 26/09/2026 10:00 - 12:00 GMT7', 'fill', "2026-09-26T10:00:00+07:00", "2026-09-26T12:00:00+07:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-3', 'K', 'audit', 'Scheduled maintenance\nDate: 26/09/2026\nTime: 10:00 - 12:00\nOur support team (GMT+7) is available 09:00 - 18:00.', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-4', 'K', 'audit', 'Dear Partners,\nPlease be informed that we will have a scheduled maintenance on 26/09/2026 from 2.30 - 4.30 pm (GMT+8).\nThank you.', 'fill', "2026-09-26T14:30:00+08:00", "2026-09-26T16:30:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-5', 'K', 'audit', 'Scheduled maintenance on Saturday, 26 September 2026, 10.00 - 11.30 pm GMT+8.', 'fill', "2026-09-26T22:00:00+08:00", "2026-09-26T23:30:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-6', 'K', 'audit', 'Scheduled maintenance 26 Sep 2026, 2.00-4.00 PM (GMT+8)', 'fill', "2026-09-26T14:00:00+08:00", "2026-09-26T16:00:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-7', 'K', 'audit', 'Scheduled maintenance 26 Sep 2026, 8.00 - 10.00 p.m. (GMT+8)', 'fill', "2026-09-26T20:00:00+08:00", "2026-09-26T22:00:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-8', 'K', 'audit', 'Hi team 👋\nBNG maintenance: Sat 26.09.2026, 3.00–5.00 pm GMT+8.\nRegards', 'fill', "2026-09-26T15:00:00+08:00", "2026-09-26T17:00:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-9', 'K', 'audit', '【维护通知】\n维护时间：2026年9月26日 2.30-4.30 PM（GMT+8）', 'fill', "2026-09-26T14:30:00+08:00", "2026-09-26T16:30:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-10', 'K', 'audit', 'Scheduled maintenance 26 Sep 2026, 7.15 - 8.45 pm (GMT+8)', 'fill', "2026-09-26T19:15:00+08:00", "2026-09-26T20:45:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-11', 'K', 'audit', 'Scheduled maintenance on 26/09/2026 10:00 - 12:00 UTC7', 'fill', "2026-09-26T10:00:00+07:00", "2026-09-26T12:00:00+07:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-12', 'K', 'audit', 'Scheduled maintenance on 26/09/2026 10:00 - 12:00 (GMT0)', 'fill', "2026-09-26T10:00:00+00:00", "2026-09-26T12:00:00+00:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-13', 'K', 'audit', 'Scheduled maintenance on 26/09/2026 02:00 - 04:00 GMT2', 'fill', "2026-09-26T02:00:00+02:00", "2026-09-26T04:00:00+02:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-14', 'K', 'audit', '【维护通知】维护时间：2026年9月26日 11:00-13:00（东九区）', 'fill', "2026-09-26T11:00:00+09:00", "2026-09-26T13:00:00+09:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-15', 'K', 'audit', '【维护通知】维护时间：2026年9月25日 21:00-23:00（西五区）', 'fill', "2026-09-25T21:00:00-05:00", "2026-09-25T23:00:00-05:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-16', 'K', 'audit', '【维护通知】维护时间：2026年9月26日 09:00-11:00 东7区', 'fill', "2026-09-26T09:00:00+07:00", "2026-09-26T11:00:00+07:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-17', 'K', 'audit', '【维护通知】维护时间：2026年9月26日 09:00-11:00（河内时间）', 'fill', "2026-09-26T09:00:00+07:00", "2026-09-26T11:00:00+07:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-18', 'K', 'audit', 'Scheduled maintenance\nDate: 26/09/2026\nTime: 10:00 - 12:00\nSupport hotline (UTC+0) available 24/7.', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-19', 'K', 'audit', 'Scheduled maintenance 26/09/2026 10:00 - 12:00.\nFor partners in the UTC+7 market (VN/TH) the time is 09:00 - 11:00.', 'needs_human', None, None,
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-20', 'K', 'audit', 'Scheduled maintenance\nDate: 26/09/2026\nTime: 10:00 - 12:00\nHead office: London (UTC+0)', 'needs_human', None, None,
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-21', 'K', 'audit', 'Scheduled maintenance\nDate: 26/09/2026\nTime: 10:00 - 12:00\nSent from our Kyiv office (UTC+3) - reach us any time.', 'needs_human', None, None,
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-22', 'K', 'audit', 'Scheduled maintenance\nDate: 26/09/2026\nTime: 10:00 - 12:00\n\nKind regards,\nTech Ops, Malta Office (CET)', 'needs_human', None, None,
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-23', 'K', 'audit', '【维护通知】\n维护时间：2026年9月26日 10:00-12:00\n如有疑问请联系客服（服务时间 UTC+7 09:00-18:00）', 'fill', "2026-09-26T10:00:00+08:00", "2026-09-26T12:00:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-24', 'K', 'audit', '【维护通知】\n维护时间：2026年9月26日 10:00-12:00\n发布：伦敦办公室（UTC+1）', 'needs_human', None, None,
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-25', 'K', 'audit', 'Scheduled maintenance 26/09/2026, 2.30 to 4.30 p.m. (GMT+8)', 'fill', "2026-09-26T14:30:00+08:00", "2026-09-26T16:30:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-26', 'K', 'audit', 'Scheduled maintenance 26/09/2026, 10.30-11.30 PM SGT', 'fill', "2026-09-26T22:30:00+08:00", "2026-09-26T23:30:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-27', 'K', 'audit', 'Scheduled maintenance 26/09/2026, 3.00 – 5.00 PM MYT', 'fill', "2026-09-26T15:00:00+08:00", "2026-09-26T17:00:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-28', 'K', 'audit', '維護公告\n日期Date：2026/09/26 (Sat)\n時間Time：2.00 - 4.00 PM (GMT+8)', 'fill', "2026-09-26T14:00:00+08:00", "2026-09-26T16:00:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-29', 'K', 'audit', '🔧 Maintenance Notice 🔧\n\nDear Valued Partners,\n\nPlease be informed that PlayStar will conduct a scheduled server maintenance as follows:\n\n📅 Date: 27 September 2026 (Sunday)\n⏰ Time: 1.00 - 3.00 pm (GMT+8)\n\nDuring this period all games will be inaccessible.\nWe apologise for any inconvenience caused.\n\nThank you,\nPlayStar Support Team', 'fill', "2026-09-27T13:00:00+08:00", "2026-09-27T15:00:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-30', 'K', 'audit', 'Hi Casinoplus team 😊\nYGR system upgrade:\n• Date: 28/09/2026 (Mon)\n• Time: 9.00 – 11.00 pm GMT+8\n• Impact: all YGR games unavailable\nThanks for your support!', 'fill', "2026-09-28T21:00:00+08:00", "2026-09-28T23:00:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-31', 'K', 'audit', '【Maintenance Notice】\nVP will perform a scheduled maintenance.\nDate: 2026/09/29 (Tue)\nTime: 10.30 - 11.30 PM (UTC+8)\nThank you for your kind understanding.', 'fill', "2026-09-29T22:30:00+08:00", "2026-09-29T23:30:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-32', 'K', 'audit', 'Hello Team,\nOMNIPLAY scheduled maintenance on 29/09/2026 from 09:00 until 11:00.\nFor your reference, Manila time: 15:00 - 17:00.', 'needs_human', None, None,
      note='fix-up H-hunt: pinned current verdict'),
    C('M14-H-hunt-33', 'K', 'audit', 'Dear Partners,\nEEZE scheduled maintenance 29/09/2026 10:00 - 12:00.\nLatAm partners (GMT-3): 23:00 - 01:00 (28/09).', 'fill', "2026-09-29T10:00:00+08:00", "2026-09-29T12:00:00+08:00",
      note='fix-up H-hunt: pinned current verdict'),
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
    if case.get("why") and case["why"] not in (v.get("reason") or ""):
        bad.append("reason {!r} does not contain {!r}".format(
            v.get("reason"), case["why"]))
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
