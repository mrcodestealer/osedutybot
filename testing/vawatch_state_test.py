#!/usr/bin/env python3
"""Watcher STATE across messages - offline, end to end through handle_messages.

  python3 testing/vawatch_state_test.py           exit 1 on any failure

OFFLINE. Every network, Base and send function is replaced by a fake before
anything runs; a real network call raises. Nothing can reach Lark or Telegram.

Each block is a trigger from the 2026-09-24 audit:
  R1.52  the chat's message ids restart (re-created / migrated group)
  R1.58  a deferred second outage that the provider then voids
  R1.59  VAWATCH_CLEAR_ENABLED=1 blanking a row for messages that cancel
         something else (questions, bets, someone else's maintenance, UAT)
  R1.68  a write whose card fails, with VAWATCH_WRITE_ATTEMPTS=1
"""
import datetime as d
import os
import pathlib
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import vawatch as va  # noqa: E402

NET = {"n": 0}


def _blocked(*_a, **_k):
    NET["n"] += 1
    raise RuntimeError("network blocked in test")


va._tenant_token = _blocked

# Confirm-before-write: the model is STUBBED to agree, so these tests keep
# testing the rules; the gate itself is tested in testing/vawatch_state_test.py
# and testing/providerllm_confirm_test.py. Never a real model call.
CONFIRM = {"verdict": "yes", "transient": False, "why": "test stub: confirmed"}
REAL_LLM_CONFIRM = va._llm_confirm
va._llm_confirm = lambda *a, **k: dict(CONFIRM)
TZ = d.timezone(d.timedelta(hours=8))
G, P = "PP - IGO PR [A-SW-S/LC][A-SPE14-2117]", "PP"
ROW, WRITES, CARDS = {}, [], []
FAILS = []


def fresh(card_ok=True):
    va.LEDGER_PATH = pathlib.Path(tempfile.mkdtemp()) / "vawatch.json"
    ROW.clear(); WRITES.clear(); CARDS.clear()

    def upd(_rid, f):
        WRITES.append(dict(f)); ROW.update(f)
        return {"code": 0}

    def card(_chat, c):
        if not card_ok:
            raise RuntimeError("card send failed (simulated)")
        CARDS.append(c)
        return {"code": 0}

    va.find_provider_row = lambda _p: {"record_id": "rec_PP", "fields": dict(ROW)}
    va.update_row = upd
    va.get_row = lambda _rid: dict(ROW)
    va.send_card = card
    va.send_text = card


def at(dt):
    va._now_dt = lambda: dt


def sweep(msgs):
    return va.handle_messages(msgs, provider=P, group=G, shared_with=[],
                              record_id="rec_PP")


def starts():
    return [d.datetime.fromtimestamp(f["Start Time"] / 1000, TZ).strftime("%m-%d %H:%M")
            for f in WRITES if f.get("Start Time")]


def check(name, ok, got=""):
    print(("PASS  " if ok else "FAIL  ") + name + ("" if ok else f"   -> {got}"))
    if not ok:
        FAILS.append(name)


import json  # noqa: E402

# ---------------------------------------------------------------- R1.52
fresh()
at(d.datetime(2026, 9, 23, 12, 0, tzinfo=TZ))
cold = [{"mid": str(50000 + i), "text": f"chat {i}"} for i in range(8)]
sweep(cold)
sweep(cold + [{"mid": "50008", "text": "ok thanks"}])
r = sweep([{"mid": "1", "text": "This group was upgraded"},
           {"mid": "2", "text": "Dear partners, scheduled maintenance on 2026-09-27 "
                                "02:00 - 04:00 (GMT+8)."}])
check("R1.52 restarted message ids: the new notice fills", starts() == ["09-27 02:00"], starts())
check("R1.52 the restart is reported in the result", r.get("mid_restart") == 1, r.get("mid_restart"))

# ---------------------------------------------------------------- R1.58
A = "Scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8)."
B = "Scheduled maintenance on 2026-09-28 10:00 - 12:00 (GMT+8)."
SCEN = {
    "an in-place edit of B to [VOID]": [{"mid": "1", "text": A},
                                        {"mid": "2", "text": "[VOID] " + B, "edited": True}],
    "a follow-up 'on hold'": [{"mid": "1", "text": A}, {"mid": "2", "text": B},
                              {"mid": "3", "text": "The maintenance on 2026-09-28 10:00 - 12:00 is on hold."}],
    "a follow-up 不进行了": [{"mid": "1", "text": A}, {"mid": "2", "text": B},
                          {"mid": "3", "text": "2026-09-28 10:00-12:00维护不进行了"}],
    "B deleted": [{"mid": "1", "text": A}, {"mid": "3", "text": "ok noted"}],
}
for name, last in list(SCEN.items()) + [("control", None)]:
    fresh()
    at(d.datetime(2026, 9, 23, 11, 0, tzinfo=TZ)); sweep([{"mid": "0", "text": "hi"}])
    at(d.datetime(2026, 9, 23, 12, 0, tzinfo=TZ)); sweep([{"mid": "0", "text": "hi"}, {"mid": "1", "text": A}])
    at(d.datetime(2026, 9, 23, 13, 0, tzinfo=TZ))
    sweep([{"mid": "0", "text": "hi"}, {"mid": "1", "text": A}, {"mid": "2", "text": B}])
    tail = last if last is not None else [{"mid": "1", "text": A}, {"mid": "2", "text": B},
                                          {"mid": "3", "text": "ok noted"}]
    at(d.datetime(2026, 9, 23, 14, 0, tzinfo=TZ)); sweep([{"mid": "0", "text": "hi"}] + tail)
    at(d.datetime(2026, 9, 25, 13, 0, tzinfo=TZ)); sweep([{"mid": "0", "text": "hi"}] + tail)
    wins = sorted({s[:5] for s in starts()})
    if name == "control":
        check("R1.58 control: an unvoided deferred window is written once the first ends",
              wins == ["09-25", "09-28"], wins)
    else:
        check(f"R1.58 deferred window voided by {name}: never written", wins == ["09-25"], wins)

# ---------------------------------------------------------------- #277
# A deferred second outage followed by "Sorry, wrong group!" (directly under
# the deferred notice, not the row's own): the follow-up read as chat, and the
# deferred window was written with a green card once the first one ended.
for follow in ["Sorry, wrong group!", "Oops wrong chat", "发错群了"]:
    fresh()
    hi = {"mid": "0", "text": "hi"}
    at(d.datetime(2026, 9, 23, 11, 0, tzinfo=TZ)); sweep([hi])
    at(d.datetime(2026, 9, 23, 12, 0, tzinfo=TZ)); sweep([hi, {"mid": "1", "text": A}])
    at(d.datetime(2026, 9, 23, 13, 0, tzinfo=TZ))
    sweep([hi, {"mid": "1", "text": A}, {"mid": "2", "text": "Dear Partners,\n" + B}])
    tail = [hi, {"mid": "1", "text": A}, {"mid": "2", "text": "Dear Partners,\n" + B},
            {"mid": "3", "text": follow}]
    at(d.datetime(2026, 9, 23, 14, 0, tzinfo=TZ)); sweep(tail)
    WRITES.clear(); CARDS.clear()
    at(d.datetime(2026, 9, 25, 13, 0, tzinfo=TZ)); sweep(tail)
    check(f"#277 deferred window then {follow!r}: never written", not starts(), starts())
    check(f"#277 ...and carded as NOT written",
          any("was NOT written" in json.dumps(c, ensure_ascii=False) for c in CARDS), CARDS)

# ---------------------------------------------------------------- #272
# The read-back before a deferred write fails (Base 5xx) on 3 ticks: the
# pending entry used to be dropped with no card, and never written even after
# the Base recovered.
fresh()
hi = {"mid": "0", "text": "hi"}
at(d.datetime(2026, 9, 23, 11, 0, tzinfo=TZ)); sweep([hi])
at(d.datetime(2026, 9, 23, 12, 0, tzinfo=TZ)); sweep([hi, {"mid": "1", "text": A}])
at(d.datetime(2026, 9, 23, 13, 0, tzinfo=TZ)); sweep([hi, {"mid": "1", "text": A}, {"mid": "2", "text": B}])
_good_get = va.get_row


def _bad_get(_rid):
    raise RuntimeError("Lark 503 (simulated)")


va.get_row = _bad_get
WRITES.clear(); CARDS.clear()
for k in range(3):
    at(d.datetime(2026, 9, 25, 13, 10 * k, tzinfo=TZ))
    sweep([hi, {"mid": "1", "text": A}, {"mid": "2", "text": B}])
check("#272 a read-back that keeps failing is carded once", len(CARDS) == 1
      and "NOT written" in json.dumps(CARDS[0], ensure_ascii=False), CARDS)
va.get_row = _good_get
at(d.datetime(2026, 9, 25, 14, 0, tzinfo=TZ)); sweep([hi, {"mid": "1", "text": A}, {"mid": "2", "text": B}])
check("#272 ...and the deferred window is written once the read works", starts() == ["09-28 10:00"],
      starts())

# ---------------------------------------------------------------- G3.1 #89
# The row holds a still-future 24/09 set by HAND (no ledger owner): a notice for
# 01/10 overwrote it with no word of 24/09. It is now deferred and carded.
fresh()
ROW.update({"Start Time": int(d.datetime(2026, 9, 24, 10, 0, tzinfo=TZ).timestamp() * 1000),
            "End Time": int(d.datetime(2026, 9, 24, 12, 0, tzinfo=TZ).timestamp() * 1000)})
N01 = "Scheduled maintenance on 01/10/2026 10:00-12:00 (GMT+8)."
at(d.datetime(2026, 9, 22, 12, 0, tzinfo=TZ)); sweep([{"mid": "1", "text": "hi"}])
at(d.datetime(2026, 9, 22, 13, 0, tzinfo=TZ))
sweep([{"mid": "1", "text": "hi"}, {"mid": "2", "text": N01}])
check("#89 a hand-set upcoming 24/09 is not overwritten by 01/10", not starts(), starts())
check("#89 ...the card names both windows",
      any("2026-09-24 10:00" in json.dumps(c, ensure_ascii=False)
          and "2026-10-01 10:00" in json.dumps(c, ensure_ascii=False) for c in CARDS), CARDS)
at(d.datetime(2026, 9, 23, 13, 0, tzinfo=TZ))
sweep([{"mid": "1", "text": "hi"}, {"mid": "2", "text": N01}])
check("#89 ...nothing moves while 24/09 is ahead", not starts(), starts())
at(d.datetime(2026, 9, 24, 13, 0, tzinfo=TZ))
sweep([{"mid": "1", "text": "hi"}, {"mid": "2", "text": N01}])
check("#89 ...and 01/10 is written once 24/09 has ended", starts() == ["10-01 10:00"], starts())

# ---------------------------------------------------------------- G3.1 #90
# Reverse order: 01/10 filled, then a sooner 24/09 replaced it. Once 24/09 has
# ended, 01/10 is written back (it used to be lost for good).
fresh()
N24 = "Scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8)."
hi = {"mid": "0", "text": "hi"}
at(d.datetime(2026, 9, 22, 11, 0, tzinfo=TZ)); sweep([hi])
at(d.datetime(2026, 9, 22, 12, 0, tzinfo=TZ)); sweep([hi, {"mid": "1", "text": N01}])
at(d.datetime(2026, 9, 22, 13, 0, tzinfo=TZ))
sweep([hi, {"mid": "1", "text": N01}, {"mid": "2", "text": N24}])
check("#90 the sooner 24/09 is written", starts() == ["10-01 10:00", "09-24 10:00"], starts())
WRITES.clear(); CARDS.clear()
at(d.datetime(2026, 9, 24, 12, 30, tzinfo=TZ))
sweep([hi, {"mid": "1", "text": N01}, {"mid": "2", "text": N24}])
check("#90 ...and the displaced 01/10 is written back once 24/09 has ended",
      starts() == ["10-01 10:00"] and ROW.get("Remark") == N01, (starts(), ROW.get("Remark")))

# ---------------------------------------------------------------- G3.1 audit-2
# A reschedule or extension of the DEFERRED outage skipped the deferral and
# erased the row's imminent 25/09.
for C, want in (("Update: the maintenance on 2026-09-28 is rescheduled to 2026-09-29 10:00 - 12:00 (GMT+8).",
                 "09-29 10:00"),
                ("The maintenance on 2026-09-28 10:00-12:00 is extended until 14:00.", "09-28 10:00"),
                ("Rescheduled: maintenance moved to 2026-09-30 10:00 - 12:00 (GMT+8).", None)):
    fresh()
    at(d.datetime(2026, 9, 23, 11, 0, tzinfo=TZ)); sweep([hi])
    at(d.datetime(2026, 9, 23, 12, 0, tzinfo=TZ)); sweep([hi, {"mid": "1", "text": A}])
    at(d.datetime(2026, 9, 23, 13, 0, tzinfo=TZ)); sweep([hi, {"mid": "1", "text": A}, {"mid": "2", "text": B}])
    at(d.datetime(2026, 9, 23, 14, 0, tzinfo=TZ))
    CARDS.clear()
    sweep([hi, {"mid": "1", "text": A}, {"mid": "2", "text": B}, {"mid": "3", "text": C}])
    check(f"audit-2 the row keeps 25/09 after {C[:45]!r}", starts() == ["09-25 10:00"], starts())
    check("audit-2 ...and it is carded", len(CARDS) == 1, CARDS)
    at(d.datetime(2026, 9, 25, 13, 0, tzinfo=TZ))
    sweep([hi, {"mid": "1", "text": A}, {"mid": "2", "text": B}, {"mid": "3", "text": C}])
    check(f"audit-2 ...then the CHANGED deferred outage (not the old 28/09) goes on the row",
          starts()[1:] == ([want] if want else []), starts())

# ---------------------------------------------------------------- R1.49
# The deferred window is written only after the row is read back: a row a
# person has since set by hand to another upcoming window is left alone.
import json  # noqa: E402


def _ms(dt):
    return int(dt.timestamp() * 1000)


def deferred_then(hand):
    fresh()
    hi = {"mid": "0", "text": "hi"}
    at(d.datetime(2026, 9, 23, 11, 0, tzinfo=TZ)); sweep([hi])
    at(d.datetime(2026, 9, 23, 12, 0, tzinfo=TZ)); sweep([hi, {"mid": "1", "text": A}])
    at(d.datetime(2026, 9, 23, 13, 0, tzinfo=TZ))
    sweep([hi, {"mid": "1", "text": A}, {"mid": "2", "text": B}])
    if hand:
        ROW.update({"Start Time": _ms(hand[0]), "End Time": _ms(hand[1])})
    WRITES.clear(); CARDS.clear()
    at(d.datetime(2026, 9, 25, 13, 0, tzinfo=TZ))
    sweep([hi, {"mid": "1", "text": A}, {"mid": "2", "text": B}, {"mid": "3", "text": "ok noted"}])
    return starts(), [json.dumps(c, ensure_ascii=False) for c in CARDS]


w, cards = deferred_then((d.datetime(2026, 9, 26, 20, 0, tzinfo=TZ), d.datetime(2026, 9, 26, 22, 0, tzinfo=TZ)))
check("R1.49 a row hand-set to another upcoming window is not overwritten by the deferred one",
      not w, w)
check("R1.49 ...and the deferred window is carded as NOT written",
      any("was NOT written" in c and "2026-09-26 20:00" in c for c in cards), cards)
w, _c = deferred_then((d.datetime(2026, 9, 24, 20, 0, tzinfo=TZ), d.datetime(2026, 9, 24, 22, 0, tzinfo=TZ)))
check("R1.49 control: a hand-set window that has ENDED does not block the deferred write",
      w == ["09-28 10:00"], w)
w, _c = deferred_then(None)
check("R1.49 control: an untouched row gets the deferred window", w == ["09-28 10:00"], w)

# ------------------------------------------------- force stops on "back online"
# /vacheck force walks newest-first and stops at the first message about
# maintenance. "All games are back online now." named no maintenance word, so
# the walk went past it and re-wrote the finished notice above it.
def force_after(last):
    fresh()
    hi = {"mid": "0", "text": "hi"}
    at(d.datetime(2026, 9, 23, 9, 0, tzinfo=TZ)); sweep([hi])
    at(d.datetime(2026, 9, 23, 9, 30, tzinfo=TZ)); sweep([hi, {"mid": "1", "text": A}])
    WRITES.clear()
    at(d.datetime(2026, 9, 24, 12, 0, tzinfo=TZ))   # A (09-25 10:00-12:00) still ahead
    msgs = [hi, {"mid": "1", "text": A}] + ([{"mid": "2", "text": last}] if last else [])
    r = va.handle_messages(msgs, force=True, provider=P, group=G, shared_with=[],
                           record_id="rec_PP")
    return starts(), r.get("force_stop") or ""


w, why = force_after("All games are back online now.")
check("force stops at 'All games are back online now.' - nothing re-written", not w, (w, why))
check("...and says why it stopped", "completed" in why, why)
w, why = force_after("游戏已恢复正常。")
check("force stops at 游戏已恢复正常 - nothing re-written", not w, (w, why))
w, why = force_after(None)
check("force control: with no completion below it the notice is re-written",
      w == ["09-25 10:00"], (w, why))

# ---------------------------------------------------------------- R1.59
os.environ["VAWATCH_CLEAR_ENABLED"] = "1"
try:
    def cleared_by(follow):
        fresh()
        at(d.datetime(2026, 9, 23, 12, 0, tzinfo=TZ))
        sweep([{"mid": "0", "text": "hi"}])
        sweep([{"mid": "0", "text": "hi"}, {"mid": "1", "text": A}])
        at(d.datetime(2026, 9, 23, 13, 0, tzinfo=TZ))
        WRITES.clear()
        sweep([{"mid": "0", "text": "hi"}, {"mid": "1", "text": A}, {"mid": "2", "text": follow}])
        return any("Start Time" in f and f["Start Time"] is None for f in WRITES)

    for t in ["Is the maintenance on 2026-09-25 10:00-12:00 cancelled?",
              "Can the maintenance on 2026-09-25 10:00-12:00 be cancelled?",
              "Maintenance on 2026-09-25 10:00-12:00 cancelled? Please confirm.",
              "Did you cancel the maintenance on 2026-09-25 10:00-12:00?",
              "2026-09-25 10:00-12:00的维护取消了吗？",
              "Bets placed during the maintenance on 2026-09-25 10:00-12:00 will be cancelled.",
              "Rounds interrupted by the maintenance on 2026-09-25 10:00-12:00 will be cancelled and refunded.",
              "Unfinished games during the maintenance on 2026-09-25 10:00-12:00 will be cancelled; "
              "the maintenance itself is unchanged.",
              "Withdrawals will be cancelled during maintenance on 2026-09-25 10:00-12:00.",
              "Pending withdrawal requests before maintenance on 2026-09-25 10:00-12:00 will be cancelled",
              "Promotion cancelled during maintenance on 2026-09-25 10:00-12:00.",
              "The free spins campaign during maintenance on 2026-09-25 10:00-12:00 is cancelled.",
              "Evolution maintenance on 2026-09-25 10:00-12:00 has been cancelled.",
              "CasinoPlus maintenance on 2026-09-25 10:00-12:00 has been cancelled.",
              "Our (CasinoPlus) maintenance on 2026-09-25 10:00-12:00 has been cancelled.",
              "The maintenance on 2026-09-25 10:00-12:00 for UAT environment is cancelled.",
              # #192 / #278: a question without "?", a request to confirm, a
              # conditional, a cancellation of one region only.
              "Is the maintenance on 2026-09-25 10:00-12:00 cancelled",
              "Just to confirm the maintenance on 2026-09-25 10:00-12:00 is cancelled",
              "2026-09-25 10:00-12:00 的维护是不是取消",
              "Is the maintenance on 2026-09-25 10:00-12:00 still on or cancelled",
              "Maintenance on 2026-09-25 10:00-12:00 will be cancelled if there is no issue.",
              "Maintenance on 2026-09-25 10:00-12:00 will be cancelled if the release is ready.",
              "The maintenance on 2026-09-25 10:00-12:00 for the Brazil region is cancelled."]:
        check(f"R1.59 NOT blanked by: {t[:70]}", not cleared_by(t))
    check("R1.59 a real cancellation of the row's own window IS blanked",
          cleared_by("The scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8) has been cancelled."))
finally:
    os.environ.pop("VAWATCH_CLEAR_ENABLED", None)

# ---------------------------------------------------------------- R1.68
os.environ["VAWATCH_WRITE_ATTEMPTS"] = "1"
try:
    fresh(card_ok=False)
    at(d.datetime(2026, 9, 23, 12, 0, tzinfo=TZ))
    sweep([{"mid": "1", "text": "hi"}])
    r = sweep([{"mid": "1", "text": "hi"},
               {"mid": "2", "text": "Scheduled maintenance on 2026-10-25 10:00 - 12:00 (GMT+8)."}])
    summary = va.format_check_summary(dict(r, group=G, provider=P))
    check("R1.68 a write whose card failed is counted", r.get("card_failed") == 1, r.get("card_failed"))
    check("R1.68 ...and /vacheck says so", "will NOT be re-sent" in summary, summary)
finally:
    os.environ.pop("VAWATCH_WRITE_ATTEMPTS", None)

# ------------------------------------------------ confirm-before-write gate
# The regex parser proposes a window; the local model must confirm it before
# anything is written (VAWATCH_LLM_CONFIRM). Stubbed here - never a real call.
NOTICE = "Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8)."


def gate_run(replies, n=1, env=None):
    """Sweep NOTICE n times with the model answering ``replies`` in turn."""
    fresh()
    asked = []

    def fake(text, verdict, **k):
        asked.append(verdict.get("start"))
        return dict(replies[min(len(asked), len(replies)) - 1])
    va._llm_confirm = fake
    old = {k: os.environ.get(k) for k in (env or {})}
    os.environ.update(env or {})
    try:
        at(d.datetime(2026, 9, 23, 12, 0, tzinfo=TZ)); sweep([{"mid": "0", "text": "hi"}])
        rs = []
        for i in range(n):
            at(d.datetime(2026, 9, 23, 12, 10 + i, tzinfo=TZ))
            rs.append(sweep([{"mid": "0", "text": "hi"}, {"mid": "1", "text": NOTICE}]))
    finally:
        for k, v in old.items():
            os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
        va._llm_confirm = lambda *a, **k: dict(CONFIRM)
    return starts(), [json.dumps(c, ensure_ascii=False) for c in CARDS], asked, rs


NO = {"verdict": "no", "transient": False, "why": "the model says: the window is not the one the parser read"}
DOWN = {"verdict": "unclear", "transient": True, "why": "model unreachable: TimeoutError()"}
YES = {"verdict": "yes", "transient": False, "why": "ok"}
w, cards, asked, _r = gate_run([NO], n=3)
check("gate: a model NO writes nothing", not w, w)
check("gate: ...and cards it ONCE, naming the parser's window and the model's reason",
      len(cards) == 1 and "did not agree" in cards[0] and "2026-09-27 02:00" in cards[0], cards)
check("gate: ...and the decision is final (asked once, not every sweep)", len(asked) == 1, asked)
w, cards, asked, rs = gate_run([DOWN], n=3)
check("gate: an unreachable model writes nothing", not w, w)
check("gate: ...retries silently before its cap (no card on tries 1-2)",
      rs[0].get("confirm_pending") == 1 and rs[1].get("confirm_pending") == 1, [r.get("confirm_pending") for r in rs])
check("gate: ...and cards at the cap (VAWATCH_CONFIRM_ATTEMPTS=3)",
      len(cards) == 1 and "could not be reached" in cards[0] and len(asked) == 3, (cards, asked))
w, cards, asked, _r = gate_run([DOWN, YES], n=2)
check("gate: a model that comes back writes the window on the next sweep",
      w == ["09-27 02:00"] and not any("could not be reached" in c or "did not agree" in c
                                        for c in cards), (w, cards))
w, cards, asked, _r = gate_run([YES], n=1)
check("gate: a YES writes as before", w == ["09-27 02:00"], w)


# The deferred second outage is confirmed too, when it is finally written.
fresh()
hi = {"mid": "0", "text": "hi"}
at(d.datetime(2026, 9, 23, 11, 0, tzinfo=TZ)); sweep([hi])
at(d.datetime(2026, 9, 23, 12, 0, tzinfo=TZ)); sweep([hi, {"mid": "1", "text": A}])
at(d.datetime(2026, 9, 23, 13, 0, tzinfo=TZ)); sweep([hi, {"mid": "1", "text": A}, {"mid": "2", "text": B}])
WRITES.clear(); CARDS.clear()
va._llm_confirm = lambda *a, **k: dict(NO)
at(d.datetime(2026, 9, 25, 13, 0, tzinfo=TZ))
sweep([hi, {"mid": "1", "text": A}, {"mid": "2", "text": B}, {"mid": "3", "text": "ok noted"}])
va._llm_confirm = lambda *a, **k: dict(CONFIRM)
check("gate: a deferred window the model rejects is NOT written when the first ends",
      not starts(), starts())
check("gate: ...and is carded as not confirmed",
      any("did not agree" in json.dumps(c, ensure_ascii=False) for c in CARDS),
      [json.dumps(c, ensure_ascii=False)[:120] for c in CARDS])

# VAWATCH_LLM_CONFIRM=0: the real gate says yes without asking the model; on,
# it asks, and a model that raises is a transient "unclear" - never a yes.
import providerllm  # noqa: E402
_saved_cw = providerllm.confirm_window
ASKS = []


def _asked(*a, **k):
    ASKS.append(k.get("provider"))
    raise RuntimeError("model down (simulated)")


providerllm.confirm_window = _asked
try:
    V = {"start": d.datetime(2026, 9, 27, 2, 0, tzinfo=TZ), "end": d.datetime(2026, 9, 27, 4, 0, tzinfo=TZ)}
    os.environ["VAWATCH_LLM_CONFIRM"] = "0"
    g_off = REAL_LLM_CONFIRM(NOTICE, V, provider="PP", group=G)
    os.environ.pop("VAWATCH_LLM_CONFIRM", None)
    g_on = REAL_LLM_CONFIRM(NOTICE, V, provider="PP", group=G)
finally:
    providerllm.confirm_window = _saved_cw
    os.environ.pop("VAWATCH_LLM_CONFIRM", None)
check("gate: VAWATCH_LLM_CONFIRM=0 writes on the parser alone, without asking",
      g_off["verdict"] == "yes" and ASKS == ["PP"], (g_off, ASKS))
check("gate: on, a model that raises is a transient unclear - never a yes",
      g_on["verdict"] == "unclear" and g_on["transient"], g_on)
va._now_dt = lambda: d.datetime.now(TZ)
print("-" * 78)
print(f"{'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILED'}   | real network attempts: {NET['n']}")
sys.exit(1 if FAILS or NET["n"] else 0)
