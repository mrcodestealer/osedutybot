#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline selftest for ``/provideraskmaintenance`` (providerask + providerllm).

providerask is the only code in the repo that WRITES into partner Telegram
groups from a real account, and it files the answers into a SHARED Lark Base.
So this suite is built so that nothing in it can reach either:

  * ``requests`` is replaced before anything else is imported: every
    post/get/put/request raises and is counted, and the suite FAILS if the
    count is not zero at the end;
  * APP_ID / APP_SECRET are removed from this process's environment, and
    ``vawatch._tenant_token`` raises - a real Lark call cannot authenticate;
  * ``vawatch.find_provider_row`` / ``update_row`` / ``send_card`` /
    ``send_text`` are replaced by an in-memory Base and by counters that must
    stay at zero (providerask posts its cards through the ``send_card``
    callback it is handed, which here is a local list);
  * ``providerllm._chat`` raises unless a test installs a canned reply, and
    ``providerllm.classify_reply`` is stubbed per test;
  * the run journal and the vawatch ledger live in a temp dir;
  * main and telegramwarm are never imported (they start the scheduler and
    the browser).

Usage
-----
    python3 testing/providerask_selftest.py            # summary + failures
    python3 testing/providerask_selftest.py --verbose  # every test
    python3 testing/providerask_selftest.py -k G3.3    # tests whose name matches

Each audit finding has at least one test named after its id (G3.2 ... G3.8);
the BASE tests pin behaviour that already worked and must keep working.
Exit code is 0 only when every selected test passes and no network call was
attempted.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import traceback
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

# --- 1. no credentials, no stray feature flags -----------------------------
for _k in list(os.environ):
    if _k.startswith(("VAWATCH_", "PROVIDERASK_", "NOTICE_")) or _k in (
            "APP_ID", "APP_SECRET"):
        del os.environ[_k]

# --- 2. the network is gone before any project module is imported ----------
import requests  # noqa: E402

NET: list = []


def _no_net(*a, **k):
    NET.append(str(a[:1]))
    raise RuntimeError("providerask_selftest: network call blocked")


requests.post = requests.get = requests.put = requests.patch = _no_net
requests.delete = requests.request = _no_net
requests.Session.request = lambda self, *a, **k: _no_net(*a, **k)

# Importing providerllm runs load_dotenv(); a developer's .env must not turn
# credentials back on for this process.
try:
    import dotenv  # noqa: E402

    dotenv.load_dotenv = lambda *a, **k: False
except Exception:  # noqa: BLE001
    pass

import noticeparse  # noqa: E402,F401
import providerask as pa  # noqa: E402
import providerllm as pl  # noqa: E402
import vawatch  # noqa: E402

for _k in ("APP_ID", "APP_SECRET"):
    os.environ.pop(_k, None)

# The real classifier, kept before any test stubs it (G3.8 drives it end to end
# with a canned model reply).
_real_classify = pl.classify_reply

TZ = timezone(timedelta(hours=8))
NOW = [datetime(2026, 9, 23, 10, 0, tzinfo=TZ)]
TMP = Path(tempfile.mkdtemp(prefix="providerask_selftest_"))
vawatch.LEDGER_PATH = TMP / "vawatch.json"
pa.STATE_PATH = TMP / "providerask_state.json"
pa._now = lambda: NOW[0]

COUNT = {"token": 0, "va_card": 0, "va_text": 0, "llm_chat": 0}


def _token():
    COUNT["token"] += 1
    raise RuntimeError("providerask_selftest: tenant token blocked")


def _va_card(chat, card):
    COUNT["va_card"] += 1
    raise RuntimeError("providerask_selftest: vawatch.send_card blocked")


def _va_text(chat, text):
    COUNT["va_text"] += 1
    raise RuntimeError("providerask_selftest: vawatch.send_text blocked")


def _llm_chat_blocked(*a, **k):
    COUNT["llm_chat"] += 1
    raise RuntimeError("providerask_selftest: providerllm._chat blocked")


vawatch._tenant_token = _token
vawatch.send_card = _va_card
vawatch.send_text = _va_text
pl._chat = _llm_chat_blocked

# --- 3. an in-memory Base ---------------------------------------------------
BASE: dict = {}
WRITES: list = []
FAIL = {"update": None, "times": 0}


def _find(provider):
    for rid, f in BASE.items():
        if f["Provider / Games"].casefold() == (provider or "").casefold():
            return {"record_id": rid, "fields": dict(f)}
    return {}


def _update(rid, fields):
    if FAIL["update"] is not None and FAIL["times"] != 0:
        FAIL["times"] -= 1
        raise FAIL["update"]
    WRITES.append((rid, dict(fields)))
    BASE[rid].update(fields)
    return {"code": 0}


vawatch.find_provider_row = _find
vawatch.update_row = _update

CARDS: list = []
NOTES: list = []


def ms(dt):
    return int(dt.timestamp() * 1000)


def T(m, d, h=12, mi=0):
    return datetime(2026, m, d, h, mi, tzinfo=TZ)


def row(provider):
    f = _find(provider)["fields"]

    def fmt(v):
        return None if v is None else datetime.fromtimestamp(
            v / 1000, TZ).strftime("%m-%d %H:%M")
    return (fmt(f.get("Start Time")), fmt(f.get("End Time")), f.get("Remark") or "")


def add_row(provider, start=None, end=None, remark=""):
    BASE["rec_" + provider.replace(" ", "_")] = {
        "Provider / Games": provider, "Start Time": start, "End Time": end,
        "Remark": remark}


def msg(mid, text, out=False, sender="Provider CS"):
    return {"mid": str(mid), "text": text, "out": out, "edited": False,
            "sender": sender, "time": "", "kind": "text"}


ASK = lambda mid=90: msg(mid, pa.ASK_TEXT, out=True, sender="me")  # noqa: E731


def ok(verdict, conf=0.95, answers=True, why="stub"):
    return {"ok": True, "verdict": verdict, "answers_us": answers,
            "confidence": conf, "why": why}


def by_window(text, **k):
    """A realistic stub: maintenance when the text carries a window."""
    return ok("maintenance" if noticeparse.find_window(text, now=NOW[0])
              else "no_maintenance")


def fresh(now=None):
    BASE.clear()
    WRITES.clear()
    CARDS.clear()
    NOTES.clear()
    FAIL.update(update=None, times=0)
    for k in ("PROVIDERASK_CLEAR_ENABLED", "VAWATCH_CLEAR_ENABLED",
              "PROVIDERASK_WRITE_ATTEMPTS"):
        os.environ.pop(k, None)
    NOW[0] = now or T(9, 23, 10)
    pl.classify_reply = lambda text, **k: (_ for _ in ()).throw(
        AssertionError("classify_reply not stubbed for this test"))
    pl._chat = _llm_chat_blocked
    try:
        pa.STATE_PATH.unlink()
    except FileNotFoundError:
        pass


def open_run(provs, deadline_min=60):
    st = {"started_at": "x", "asked_at": NOW[0].strftime("%Y-%m-%d %H:%M:%S"),
          "chat_id": "oc_selftest", "finished": False, "aborted": "",
          "window_min": 60,
          "deadline": (NOW[0] + timedelta(minutes=deadline_min)).isoformat(),
          "providers": {}}
    for p, g in provs:
        st["providers"][p] = {"group": g, "peer": "-100", "asked": True,
                              "outcome": "waiting", "seen": [], "why": "",
                              "wrote": ""}
    pa.save_state(st)


def sweep(read, at=None):
    if at is not None:
        NOW[0] = at
    return pa.do_sweep(read, NOTES.append, CARDS.append)


def screen(*msgs):
    return lambda title, count: {"ok": True, "messages": list(msgs)[-count:]}


def rec(p):
    return pa.load_state()["providers"][p]


def title(c):
    return c["header"]["title"]["content"]


def template(c):
    return c["header"]["template"]


def body(c):
    return "\n".join(e["text"]["content"] for e in c["body"]["elements"]
                     if e.get("tag") == "div")


def fake_groupcheck(rows):
    gc = types.ModuleType("groupcheck")
    gc.fetch_rows = lambda: [dict(r) for r in rows]
    gc.partition = lambda rs: (list(rs), [], [])
    ps = types.ModuleType("peerstore")
    ps.peer_for = lambda g: "-100" + str(len(g or ""))
    sys.modules["groupcheck"] = gc
    sys.modules["peerstore"] = ps


def check(cond, what):
    if not cond:
        raise AssertionError(what)


SP, SPG = "Spadegaming", "[CP] Spade group"
JDB, JDBG = "JDB", "[CP] JDB group"
SH = "[SG190- IGO Casinoplus YG/ RG/ HS] CS group"
FILL_2409 = "Scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8)."

TESTS: list = []


def test(name):
    def deco(fn):
        TESTS.append((name, fn))
        return fn
    return deco


# ===========================================================================
# BASE - behaviour that already worked and must keep working
# ===========================================================================

@test("BASE single window answer is filled and carded")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)])
    pl.classify_reply = by_window
    r = sweep(screen(ASK(), msg(91, FILL_2409)), T(9, 23, 10, 10))
    check(r["filed"] == 1, r)
    check(row(SP)[:2] == ("09-24 10:00", "09-24 12:00"), row(SP))
    check(rec(SP)["outcome"] == "filled", rec(SP))
    check(len(CARDS) == 1 and template(CARDS[0]) == "orange", [title(c) for c in CARDS])


@test("G3.2 a blank row gets Remark + Last Check only - no null DateTimes")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)])
    pl.classify_reply = lambda t, **k: ok("no_maintenance")
    sweep(screen(ASK(), msg(91, "Hi team, no maintenance this week. Thank you!")),
          T(9, 23, 10, 10))
    check(rec(SP)["outcome"] == "no_maintenance", rec(SP))
    check(len(WRITES) == 1 and sorted(WRITES[0][1]) == ["Last Check", "Remark"],
          WRITES)


@test("BASE a notice posted BEFORE our ask is not an answer")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)])
    pl.classify_reply = by_window
    sweep(screen(msg(80, FILL_2409), ASK()), T(9, 23, 10, 10))
    check(not WRITES and rec(SP)["outcome"] == "waiting", (WRITES, rec(SP)))


@test("BASE unrelated chatter leaves the provider silent")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)], deadline_min=0)
    pl.classify_reply = lambda t, **k: ok("unrelated", answers=False)
    sweep(screen(ASK(), msg(91, "Ticket 55231 was created, our team will check.")))
    check(rec(SP)["outcome"] == "waiting", rec(SP))
    check("No update from the provider" in body(CARDS[-1]), body(CARDS[-1]))


@test("BASE a quoted past window does not block a later 'no maintenance'")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)])
    pl.classify_reply = by_window
    sweep(screen(ASK(), msg(91, "Scheduled maintenance on 10/09/2026 10:00-12:00 (GMT+8)."),
                 msg(92, "No maintenance this week, thanks.")), T(9, 23, 10, 10))
    check(rec(SP)["outcome"] == "no_maintenance", rec(SP))


@test("BASE a judge() crash does not abort the sweep or close the run early")
def _():
    fresh(); add_row(SP); add_row(JDB); open_run([(SP, SPG), (JDB, JDBG)])

    def llm(text, **k):
        if "boom" in text:
            raise ValueError("boom")
        return by_window(text)
    pl.classify_reply = llm
    r = sweep(lambda t, c: {"ok": True, "messages": [ASK(), msg(91, "boom" if t == SPG else FILL_2409)]},
              T(9, 23, 10, 10))
    check(rec(JDB)["outcome"] == "filled" and rec(SP)["outcome"] == "waiting",
          (rec(JDB), rec(SP)))
    check(r["done"] is False, r)


@test("BASE the run closes at the deadline with one summary card")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)], deadline_min=0)
    pl.classify_reply = by_window
    r = sweep(screen(ASK(), msg(91, FILL_2409)))
    check(r["done"] and pa.load_state()["finished"], r)
    check(title(CARDS[-1]).startswith("📋 Provider maintenance"), title(CARDS[-1]))
    check(template(CARDS[-1]) == "green", template(CARDS[-1]))


# ===========================================================================
# G3.2 - a 'no maintenance' answer must not blank a window it cannot vouch for
# ===========================================================================

NO_MAINT = "No maintenance this week, thanks."


def _clear_case(start, end, remark="notice"):
    fresh(); add_row(SP, start, end, remark); open_run([(SP, SPG)])
    pl.classify_reply = lambda t, **k: ok("no_maintenance")
    sweep(screen(msg(1, "Dear partners, scheduled maintenance on 01/10/2026 10:00-12:00 (GMT+8). Thank you."),
                 ASK(2), msg(3, NO_MAINT)), T(9, 23, 10, 10))


@test("G3.2 next week's window is kept; the answer goes to a human")
def _():
    _clear_case(ms(T(10, 1, 10)), ms(T(10, 1, 12)))
    check(not WRITES, WRITES)
    check(row(SP)[:2] == ("10-01 10:00", "10-01 12:00"), row(SP))
    check(rec(SP)["outcome"] == "needs_human" and "upcoming window" in rec(SP)["why"],
          rec(SP))


@test("G3.2 an ongoing window (started, not ended) is never blanked")
def _():
    _clear_case(ms(T(9, 23, 9)), ms(T(9, 23, 11)))
    check(not WRITES and rec(SP)["outcome"] == "needs_human", (WRITES, rec(SP)))


@test("G3.2 a past window with the clear flag unset is carded, not nulled")
def _():
    _clear_case(ms(T(9, 17, 10)), ms(T(9, 17, 12)))
    check(not WRITES, WRITES)
    check(rec(SP)["outcome"] == "needs_human"
          and "PROVIDERASK_CLEAR_ENABLED" in rec(SP)["why"], rec(SP))


@test("G3.2 PROVIDERASK_CLEAR_ENABLED=1 blanks a past window")
def _():
    fresh()
    os.environ["PROVIDERASK_CLEAR_ENABLED"] = "1"
    add_row(SP, ms(T(9, 17, 10)), ms(T(9, 17, 12)), "old"); open_run([(SP, SPG)])
    pl.classify_reply = lambda t, **k: ok("no_maintenance")
    sweep(screen(ASK(), msg(3, NO_MAINT)), T(9, 23, 10, 10))
    check(row(SP) == (None, None, "No maintenance"), row(SP))
    check(WRITES[0][1]["Start Time"] is None and WRITES[0][1]["End Time"] is None, WRITES)
    check(rec(SP)["outcome"] == "no_maintenance", rec(SP))


@test("G3.2 unset PROVIDERASK_CLEAR_ENABLED follows VAWATCH_CLEAR_ENABLED")
def _():
    fresh()
    os.environ["VAWATCH_CLEAR_ENABLED"] = "1"
    check(pa._clear_enabled(), "VAWATCH_CLEAR_ENABLED=1 not followed")
    os.environ["PROVIDERASK_CLEAR_ENABLED"] = "0"
    check(not pa._clear_enabled(), "PROVIDERASK_CLEAR_ENABLED=0 did not override")
    os.environ["PROVIDERASK_CLEAR_ENABLED"] = ""
    check(pa._clear_enabled(), "empty PROVIDERASK_CLEAR_ENABLED should mean unset")


@test("G3.2 an unreadable DateTime cell is left alone")
def _():
    _clear_case("next Tuesday", None)
    check(not WRITES and rec(SP)["outcome"] == "needs_human", (WRITES, rec(SP)))


# ===========================================================================
# G3.3 - the whole reply is judged, not only the newest bubble
# ===========================================================================

M1 = "Hi team, yes - scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8)."


def _two_bubble(m2, row_first=False):
    fresh()
    if row_first:
        add_row(SP, ms(T(9, 24, 10)), ms(T(9, 24, 12)), M1)
    else:
        add_row(SP)
    open_run([(SP, SPG)])
    calls = []

    def llm(text, **k):
        calls.append(text)
        return by_window(text)
    pl.classify_reply = llm
    sweep(screen(ASK(), msg(91, M1), msg(92, m2)), T(9, 23, 10, 10))
    return calls


@test("G3.3 window + 'No other maintenance this week.' files the window")
def _():
    for first in (False, True):
        calls = _two_bubble("No other maintenance this week.", row_first=first)
        check(len(calls) == 2, f"both bubbles must be judged: {calls}")
        check(row(SP)[:2] == ("09-24 10:00", "09-24 12:00"), row(SP))
        check(rec(SP)["outcome"] == "filled", rec(SP))


@test("G3.3 window + 'Apart from that / No more ...' files the window")
def _():
    for m2 in ("Apart from that, no maintenance this week.",
               "No more maintenance for this week, thanks."):
        _two_bubble(m2)
        check(rec(SP)["outcome"] == "filled", (m2, rec(SP)))


@test("G3.3 window + a retraction-looking 'no maintenance' goes to a human")
def _():
    _two_bubble("Sorry, please ignore that - no maintenance this week.", row_first=True)
    check(not WRITES and row(SP)[:2] == ("09-24 10:00", "09-24 12:00"), (WRITES, row(SP)))
    check(rec(SP)["outcome"] == "needs_human", rec(SP))


@test("G3.3 two different windows in one reply go to a human")
def _():
    _two_bubble("Also scheduled maintenance on 26/09/2026 02:00-04:00 (GMT+8).")
    check(not WRITES and rec(SP)["outcome"] == "needs_human"
          and "two different windows" in rec(SP)["why"], (WRITES, rec(SP)))


@test("G3.3 a reschedule overrides the earlier window in the same reply")
def _():
    _two_bubble("The maintenance on 24/09 has been rescheduled to 25/09/2026 10:00-12:00 (GMT+8).")
    check(row(SP)[:2] == ("09-25 10:00", "09-25 12:00"), row(SP))


@test("G3.3 'no maintenance' then a window files the (newer) window")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)])
    pl.classify_reply = by_window
    sweep(screen(ASK(), msg(91, NO_MAINT), msg(92, M1)), T(9, 23, 10, 10))
    check(rec(SP)["outcome"] == "filled", rec(SP))


@test("G3.3 an undecided bubble after a window stops the window being filed")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)], deadline_min=15)

    def llm(text, **k):
        if "see the image" in text:
            return ok("maintenance")        # maintenance, but no window here
        return by_window(text)
    pl.classify_reply = llm
    s = screen(ASK(), msg(91, M1), msg(92, "Sorry, time changed, please see the image."))
    sweep(s, T(9, 23, 10, 10))
    check(not WRITES and rec(SP)["outcome"] == "waiting", (WRITES, rec(SP)))
    sweep(s, T(9, 23, 10, 20))              # deadline
    check(not WRITES and rec(SP)["outcome"] == "needs_human"
          and "not filed" in rec(SP)["why"], rec(SP))


# ===========================================================================
# G3.4 - a failed Base write is retried and never reported as filled
# ===========================================================================

def _answers(title_, count):
    ans = {SPG: FILL_2409, JDBG: "Hi team, no maintenance this week. Thank you!"}
    return {"ok": True, "messages": [ASK(), msg(91, ans[title_])]}


@test("G3.4 a write that fails once is retried and lands on the next sweep")
def _():
    fresh(); add_row(SP); add_row(JDB); open_run([(SP, SPG), (JDB, JDBG)])
    pl.classify_reply = by_window
    FAIL.update(update=RuntimeError("Base update failed: TooManyRequest"), times=2)
    r1 = sweep(_answers, T(9, 23, 10, 10))
    check(r1["filed"] == 0, r1)
    check(rec(SP)["outcome"] == "waiting" and rec(JDB)["outcome"] == "waiting",
          (rec(SP), rec(JDB)))
    check(not CARDS, [title(c) for c in CARDS])
    r2 = sweep(_answers, T(9, 23, 10, 20))
    check(r2["filed"] == 2, r2)
    check(row(SP)[:2] == ("09-24 10:00", "09-24 12:00") and row(JDB)[2] == "No maintenance",
          (row(SP), row(JDB)))
    sweep(_answers, T(9, 23, 11, 5))
    check(template(CARDS[-1]) == "green", template(CARDS[-1]))


@test("G3.4 a write failing until the deadline is 'not written', never green")
def _():
    fresh(); add_row(SP); add_row(JDB); open_run([(SP, SPG), (JDB, JDBG)], deadline_min=0)
    pl.classify_reply = by_window
    FAIL.update(update=RuntimeError("DatetimeFieldConvFail"), times=-1)
    r = sweep(_answers)
    check(r["filed"] == 0, r)
    check(rec(SP)["outcome"] == "write_failed" and rec(JDB)["outcome"] == "write_failed",
          (rec(SP), rec(JDB)))
    card = CARDS[-1]
    check(template(card) != "green", template(card))
    check("could NOT be written" in body(card) and "filled into the sheet" not in body(card),
          body(card))


@test("G3.4 attempts are bounded; the last failure cards red once")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)])
    pl.classify_reply = by_window
    FAIL.update(update=RuntimeError("TooManyRequest"), times=-1)
    for mi in (10, 20, 30, 40):
        sweep(_answers, T(9, 23, 10, mi))
    check(rec(SP)["outcome"] == "write_failed" and rec(SP)["write_attempts"] == 3, rec(SP))
    red = [c for c in CARDS if template(c) == "red"]
    check(len(red) == 1 and not WRITES, ([title(c) for c in CARDS], WRITES))


@test("G3.4 summary is not green when any row is unreadable")
def _():
    fresh(); add_row(SP); add_row(JDB); open_run([(SP, SPG), (JDB, JDBG)], deadline_min=0)
    pl.classify_reply = by_window
    sweep(lambda t, c: _answers(t, c) if t == SPG else {"ok": False, "error": "header timeout"})
    check(rec(SP)["outcome"] == "filled" and rec(JDB)["outcome"] == "error",
          (rec(SP), rec(JDB)))
    check(template(CARDS[-1]) == "orange", template(CARDS[-1]))


# ===========================================================================
# G3.5 - a chat shared by two Base rows keeps BOTH rows
# ===========================================================================

SHARED_ROWS = [
    {"record_id": "r1", "provider": SP, "group": SPG},
    {"record_id": "r2", "provider": "Hacksaw", "group": SH},
    {"record_id": "r3", "provider": "YGG", "group": SH},
]


def _shared_run(answer, verdict):
    fresh()
    add_row(SP); add_row("Hacksaw")
    add_row("YGG", ms(T(9, 25, 14)), ms(T(9, 25, 16)), "older YGG notice")
    fake_groupcheck(SHARED_ROWS)
    b = pa.begin("oc_selftest")
    sent = []

    def send_one(title_, text, pin):
        sent.append(title_)
        return {"ok": True, "status": "sent"}
    for _i in range(6):
        pa.ask_one(send_one, NOTES.append)
    pl.classify_reply = lambda t, **k: ok(verdict)
    reads = []

    def read(title_, count):
        reads.append(title_)
        ans = answer if title_ == SH else NO_MAINT
        return {"ok": True, "messages": [ASK(), msg(91, ans)]}
    st = pa.load_state()
    st["deadline"] = T(9, 23, 11, 5).isoformat()
    pa.save_state(st)
    sweep(read, T(9, 23, 11, 5))
    return b, sent, reads


@test("G3.5 both rows are tracked; the shared chat is asked and read once")
def _():
    b, sent, reads = _shared_run(
        "Hi team, we have scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8).",
        "maintenance")
    check([r["provider"] for r in b["plan"]["shared"]] == ["YGG"], b["plan"])
    check(set(b["state"]["providers"]) == {SP, "Hacksaw", "YGG"}, b["state"]["providers"])
    check(sent.count(SH) == 1 and len(sent) == 2, sent)
    check(reads.count(SH) == 1, reads)
    check(rec("YGG")["asked"] and rec("YGG")["shared_with"] == "Hacksaw", rec("YGG"))


@test("G3.5 a reply naming nobody is filed onto both rows (as vawatch does)")
def _():
    _shared_run("Hi team, we have scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8).",
                "maintenance")
    for p in ("Hacksaw", "YGG"):
        check(row(p)[:2] == ("09-24 10:00", "09-24 12:00") and rec(p)["outcome"] == "filled",
              (p, row(p), rec(p)))
    check("**YGG** _(asked in Hacksaw's group)_" in body(CARDS[-1]), body(CARDS[-1]))


@test("G3.5 'YGG maintenance ... Hacksaw not affected' is never filed onto Hacksaw")
def _():
    _shared_run("YGG scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8). "
                "Hacksaw games are not affected.", "maintenance")
    check(row("Hacksaw")[:2] == (None, None), row("Hacksaw"))
    check(row("YGG")[:2] == ("09-25 14:00", "09-25 16:00"), row("YGG"))
    for p in ("Hacksaw", "YGG"):
        check(rec(p)["outcome"] == "needs_human", (p, rec(p)))
    check("YGG" in body(CARDS[-1]), body(CARDS[-1]))


@test("G3.5 a reply naming only YGG does not touch Hacksaw")
def _():
    _shared_run("YGG: scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8).", "maintenance")
    check(row("Hacksaw")[:2] == (None, None) and rec("Hacksaw")["outcome"] == "needs_human",
          (row("Hacksaw"), rec("Hacksaw")))


@test("G3.5 two rows for the SAME provider and chat stay deduped")
def _():
    fresh()
    fake_groupcheck([{"provider": "YGG", "group": SH}, {"provider": "YGG", "group": SH}])
    plan = pa.targets()
    check(len(plan["ask"]) == 1 and not plan["shared"]
          and plan["skipped"][0]["why"] == "another row already covers this group", plan)


# ===========================================================================
# G3.6 - a lost boundary is OUR problem, and a deep read tries to recover it
# ===========================================================================

def _busy(n):
    older = [msg(40 + i, f"old chatter {i}") for i in range(5)]
    busy = [msg(100 + i, f"Player {i} deposit issue, ticket #{5000 + i}") for i in range(n)]
    return older + [ASK(50)] + busy + [msg(300, "Hi team, no maintenance this week. Thank you!")]


def _busy_llm(text, **k):
    return ok("no_maintenance") if "no maintenance" in text else ok("unrelated", answers=False)


@test("G3.6 ask scrolled out of the normal read: one deep read finds it")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)])
    pl.classify_reply = _busy_llm
    full, counts = _busy(11), []

    def read(t, c):
        counts.append(c)
        return {"ok": True, "messages": full[-c:]}
    sweep(read, T(9, 23, 10, 13))
    check(counts == [12, pa._DEEP_READ], counts)
    check(rec(SP)["outcome"] == "no_maintenance", rec(SP))


@test("G3.6 ask lost even in the deep read: reported as OUR problem, not silence")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)], deadline_min=0)
    pl.classify_reply = _busy_llm
    full = _busy(60)
    sweep(lambda t, c: {"ok": True, "messages": full[-c:]})
    check(rec(SP)["outcome"] == "error" and "could not find our question" in rec(SP)["why"],
          rec(SP))
    b = body(CARDS[-1])
    check("OUR problem" in b and "No update from the provider" not in b, b)
    check(not WRITES, WRITES)


@test("G3.6 a message id reported by the send is kept as the boundary")
def _():
    fresh(); add_row(SP)
    fake_groupcheck([{"provider": SP, "group": SPG}])
    pa.begin("oc_selftest")
    pa.ask_one(lambda t, x, p: {"ok": True, "status": "sent", "mid": "50"}, NOTES.append)
    check(rec(SP)["ask_mid"] == "50", rec(SP))
    pl.classify_reply = _busy_llm
    full, counts = _busy(40), []

    def read(t, c):
        counts.append(c)
        return {"ok": True, "messages": full[-c:]}
    sweep(read, T(9, 23, 10, 13))
    check(counts == [12] and rec(SP)["outcome"] == "no_maintenance", (counts, rec(SP)))


# ===========================================================================
# G3.7 - only a real outcome ends collection
# ===========================================================================

def _later_answer(first):
    return lambda t, c: first if NOW[0] < T(9, 23, 10, 15) else {
        "ok": True, "messages": [ASK(), msg(91, "Let me check."), msg(92, FILL_2409)]}


@test("G3.7 a read failure, then the answer: filled")
def _():
    for first in ({"ok": False, "error": "header check timed out"}, "raise"):
        fresh(); add_row(SP); open_run([(SP, SPG)])
        pl.classify_reply = lambda t, **k: by_window(t) if "Scheduled" in t else ok("unclear", 0.7)
        if first == "raise":
            base = _later_answer({})

            def read(t, c, base=base):
                if NOW[0] < T(9, 23, 10, 15):
                    raise TimeoutError("page.evaluate timed out")
                return base(t, c)
        else:
            read = _later_answer(first)
        sweep(read, T(9, 23, 10, 10))
        check(rec(SP)["outcome"] == "waiting" and rec(SP)["read_problem"], rec(SP))
        sweep(read, T(9, 23, 10, 20))
        check(rec(SP)["outcome"] == "filled", (first, rec(SP)))


@test("G3.7 a holding reply, then the answer: filled")
def _():
    for hold, verdict in (("Let me check with our tech team and get back to you.", ok("unclear", 0.7)),
                          ("Let me check with our tech team and get back to you.", ok("unclear", 0.3)),
                          ("We will send you the maintenance schedule shortly.", ok("maintenance", 0.9))):
        fresh(); add_row(SP); open_run([(SP, SPG)])
        pl.classify_reply = lambda t, v=verdict, **k: by_window(t) if "Scheduled" in t else v
        sweep(screen(ASK(), msg(91, hold)), T(9, 23, 10, 10))
        check(rec(SP)["outcome"] == "waiting", rec(SP))
        sweep(screen(ASK(), msg(91, hold), msg(92, FILL_2409)), T(9, 23, 10, 20))
        check(rec(SP)["outcome"] == "filled", (hold, verdict, rec(SP)))


@test("G3.7 an unreachable model: re-judged next sweep, asked once per sweep")
def _():
    fresh(); add_row(SP); add_row(JDB); open_run([(SP, SPG), (JDB, JDBG)])
    down, calls = [True], []

    def llm(text, **k):
        calls.append(text)
        if down[0]:
            return {"ok": False, "why": "model unreachable: ReadTimeout", "verdict": "unclear",
                    "answers_us": False, "confidence": 0.0}
        return by_window(text)
    pl.classify_reply = llm
    s = lambda t, c: {"ok": True, "messages": [ASK(), msg(91, "Checking, one moment please."),  # noqa: E731
                                               msg(92, FILL_2409)]}
    sweep(s, T(9, 23, 10, 10))
    check(len(calls) == 1, f"model asked {len(calls)} times while down")
    check(rec(SP)["outcome"] == "waiting" and rec(JDB)["outcome"] == "waiting",
          (rec(SP), rec(JDB)))
    down[0] = False
    sweep(s, T(9, 23, 10, 20))
    check(rec(SP)["outcome"] == "filled" and rec(JDB)["outcome"] == "filled",
          (rec(SP), rec(JDB)))


@test("G3.7 a holding reply and nothing else: needs a human at the deadline")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)], deadline_min=15)
    pl.classify_reply = lambda t, **k: ok("unclear", 0.7, why="holding reply")
    s = screen(ASK(), msg(91, "Let me check with our tech team and get back to you."))
    sweep(s, T(9, 23, 10, 10))
    check(rec(SP)["outcome"] == "waiting", rec(SP))
    sweep(s, T(9, 23, 10, 20))
    check(rec(SP)["outcome"] == "needs_human" and "holding reply" in rec(SP)["why"], rec(SP))


@test("G3.7 a read failing all hour ends as unreadable (OUR problem)")
def _():
    fresh(); add_row(SP); open_run([(SP, SPG)], deadline_min=15)
    bad = lambda t, c: {"ok": False, "error": "could not identify this group"}  # noqa: E731
    sweep(bad, T(9, 23, 10, 10))
    check(rec(SP)["outcome"] == "waiting", rec(SP))
    sweep(bad, T(9, 23, 10, 20))
    check(rec(SP)["outcome"] == "error" and rec(SP)["read_errors"] == 2, rec(SP))


# ===========================================================================
# G3.8 - answers_us must be a real boolean
# ===========================================================================

def _raw(raw):
    pl._chat = lambda messages, **k: raw
    return _real_classify("@BrandX_ops No maintenance for BrandX this week.", group="G")


@test("G3.8 answers_us 'false' / 'no' / '0' / 0 read as False")
def _():
    fresh()
    for v in ('"false"', '"no"', '"0"', "0", "false", '" False "'):
        r = _raw('{"answers_us": %s, "verdict": "no_maintenance", "confidence": 0.9, "why": "x"}' % v)
        check(r["ok"] and r["answers_us"] is False, (v, r))


@test("G3.8 unreadable answers_us fails closed (ok=False)")
def _():
    fresh()
    for v in ('"yes"', "1", '"maybe"', "null", None):
        field = "" if v is None else '"answers_us": %s, ' % v
        r = _raw('{%s"verdict": "no_maintenance", "confidence": 0.9, "why": "x"}' % field)
        check(not r["ok"], (v, r))


@test("G3.8 true / 'true' still read as True")
def _():
    fresh()
    for v in ("true", '"true"', '"TRUE"'):
        r = _raw('{"answers_us": %s, "verdict": "no_maintenance", "confidence": 0.9, "why": "x"}' % v)
        check(r["ok"] and r["answers_us"] is True, (v, r))


@test("G3.8 a reply to someone else with answers_us 'false' does not clear our row")
def _():
    fresh(); add_row(SP, ms(T(9, 17, 10)), ms(T(9, 17, 12)), "old")
    os.environ["PROVIDERASK_CLEAR_ENABLED"] = "1"
    open_run([(SP, SPG)])
    pl.classify_reply = _real_classify
    pl._chat = lambda messages, **k: ('{"answers_us": "false", "verdict": "no_maintenance", '
                                      '"confidence": 0.9, "why": "reply to another operator"}')
    sweep(screen(ASK(), msg(91, "@BrandX_ops No maintenance for BrandX this week.")),
          T(9, 23, 10, 10))
    check(not WRITES and rec(SP)["outcome"] == "waiting", (WRITES, rec(SP)))


# ===========================================================================

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("-k", default="", help="run tests whose name contains this")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args(argv)

    if not args.verbose:
        # The modules log every refused write and unreachable model; in a
        # suite that provokes those on purpose that is noise, not signal.
        quiet = lambda *a, **k: None  # noqa: E731
        pa.print = pl.print = quiet
    chosen = [(n, f) for n, f in TESTS if args.k in n]
    failed = []
    for name, fn in chosen:
        try:
            fn()
            if args.verbose:
                print(f"  PASS  {name}")
        except Exception as err:  # noqa: BLE001
            failed.append(name)
            print(f"  FAIL  {name}\n        {err!r}")
            if args.verbose:
                traceback.print_exc()
    stray = {k: v for k, v in COUNT.items() if k != "llm_chat" and v}
    print("-" * 78)
    print(f"network calls attempted: {len(NET)}   tenant tokens: {COUNT['token']}   "
          f"vawatch cards/texts: {COUNT['va_card']}/{COUNT['va_text']}   "
          f"unstubbed LLM calls: {COUNT['llm_chat']}")
    ok_all = not failed and not NET and not stray and not COUNT["llm_chat"]
    print(f"{len(chosen) - len(failed)} of {len(chosen)} tests pass."
          + ("" if ok_all or failed else "  FAIL: a network/send path was reached."))
    return 0 if ok_all else 1


if __name__ == "__main__":
    raise SystemExit(main())
