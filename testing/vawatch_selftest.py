#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline selftest for the passive maintenance watcher — nothing can be sent.

    python3 testing/vawatch_selftest.py            # every test
    python3 testing/vawatch_selftest.py F37 G4.1   # only tests whose id matches

Exit code is 0 only when every selected test passes AND the harness recorded
zero attempts to reach the network.

WHAT THIS COVERS
----------------
vawatch.handle_messages end to end (classify -> attribution -> ledger -> write
-> card), the /vacheck handler in main.py (its real source is exec'd, not
copied), and telegramwarm._handle_va_watch with the browser replaced. Every
audit finding fixed on this path has a test named after its id; later stages
add theirs with the same ``@case("<id>", ...)`` decorator.

Stage W2 (ledger and cross-message state) adds reads of ONE group over several
ticks: LRU eviction and the cold-start floor (F40, F35), data-mid order against
the message that set the row (F34, F36, F41, G2.5), a deleted row notice
(G2.4), a deferred second outage (G3.1), a renamed row (F69), the read-gap
re-read (F57), and a ledger that cannot be read or saved (F76, F77) or a bubble
that raises (F82). Also the cases found while finishing it: an older message
after a newer one was carded to a person (F34/F35 human-fix), a group unread
30+ days and a bd9fc12 ledger already FIFO-trimmed (F40 evicted mark), a
cancellation or human card that must stop a deferred write (G3.1), and a void
edit on a row filled before this release (G2.5). File faults are simulated by
wrapping builtins.open around the tempdir ledger only; the clock is pinned
with ``at(...)``.

Stage W3 (what the Laboratory group and /vacheck are told) adds: ordinary CS
chat that must not card and the per-row card cap (F58), posters (F59), a
cancellation of the row's window (F51), a retraction or correction of the
notice above (G2.3), "no maintenance" replies owned by an open
/provideraskmaintenance run (F72), a card that failed after a write landed
(F71), a PUT whose answer timed out (F80), the window as the row displays it
(F74), a stale cached watch list (F78) and a TELEGRAM+TEAMS row (F79).
vawatch.get_row (the read-back) is a fake over the WRITES list; providerask's
state file lives in the tempdir, and only its load_state() is ever reached.

The integration stage (where the parser lanes and these lanes meet) adds: a
clock-only notice carded at the reparse cap (G1.3), a phased notice's later
windows named on the card (F67), "no maintenance" never blanking an upcoming
window (G3.2), a FRESH post of a voided notice (G2.5), guard refusals counted
apart (F63), and config garbage / unloadable zones (F83).

WHY IT CANNOT SEND ANYTHING
---------------------------
This path writes a SHARED Lark Base and cards the Laboratory group, so the
harness is built to fail closed rather than to be careful:

  * ``requests`` (every verb, and Session.request) and ``socket.connect`` are
    replaced before any project module is imported. A call is RECORDED and
    raises; the run fails if the record is non-empty at the end.
  * vawatch._tenant_token raises (and is recorded), so even a sender this file
    forgot to stub cannot authenticate. vawatch.send_card / send_text /
    update_row / find_provider_row are fakes that append to lists.
  * groupcheck is the real module with fetch_rows, its senders and its token
    replaced, so the real partition() and cell flattener run on fake rows.
  * telegramwarm is imported (it starts nothing at import) and its send_text is
    replaced; the worker is constructed but never start()ed, its browser
    helpers are fakes, and _launch / _check_auth raise.
  * vawatch.LEDGER_PATH points into a tempdir; VAWATCH_* / NOTICE_* / APP_ID /
    APP_SECRET are removed from the environment before and after import.
  * main is NEVER imported (it starts the scheduler). The /vacheck block is
    read from main.py as text and exec'd with send_message captured.
"""

from __future__ import annotations

import importlib.util
import os
import socket
import sys
import tempfile
import textwrap
import time
import traceback
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

# ---------------------------------------------------------------------------
# Fail-closed network block — BEFORE any project import
# ---------------------------------------------------------------------------

NET: list = []          # every attempt to reach the network, whatever the route


def _blocked(*a, **k):
    NET.append({"args": [str(x)[:120] for x in a[:2]], "url": str(k.get("url"))[:120]})
    raise RuntimeError("NETWORK BLOCKED by vawatch_selftest")


import requests  # noqa: E402

requests.get = requests.post = requests.put = requests.patch = _blocked
requests.delete = requests.request = _blocked
requests.Session.request = lambda self, *a, **k: _blocked(*a, **k)
socket.socket.connect = lambda self, *a, **k: _blocked(*a, **k)
socket.create_connection = _blocked


def _scrub_env() -> None:
    for k in list(os.environ):
        if k.startswith(("VAWATCH_", "NOTICE_", "GROUPCHECK_", "PROVIDERASK_")) \
                or k in ("APP_ID", "APP_SECRET", "LABORATORY_GROUP"):
            del os.environ[k]


_scrub_env()

# The real groupcheck, network parts replaced.
_spec = importlib.util.spec_from_file_location("groupcheck", REPO / "groupcheck.py")
gc = importlib.util.module_from_spec(_spec)
sys.modules["groupcheck"] = gc
_spec.loader.exec_module(gc)
ROWS: list = []
gc.fetch_rows = lambda: [dict(r, apps=set(r.get("apps") or {"telegram"})) for r in ROWS]
gc._tenant_token = gc.send_text = gc.send_card = gc.upload_image_lark = _blocked

import noticeparse  # noqa: E402
import vawatch as va  # noqa: E402

_scrub_env()            # vawatch ran load_dotenv(); a stray .env must not count

TZ8 = timezone(timedelta(hours=8))
NOW = datetime(2026, 9, 23, 12, 0, tzinfo=TZ8)
_REAL_FIND = va.find_provider_row
TOKEN: list = []
WRITES: list = []       # (record_id, fields)
CARDS: list = []        # (title, full card or text)
FINDS: list = []


def _token():
    TOKEN.append(1)
    raise RuntimeError("tenant token requested in vawatch_selftest")


def _fake_card(chat, card):
    CARDS.append((card["header"]["title"]["content"], card))
    return {"code": 0}


def _fake_text(chat, text):
    CARDS.append(("TEXT:" + text.split("\n", 1)[0], text))
    return {"code": 0}


def _fake_find(provider):
    FINDS.append(provider)
    return {"record_id": "rec_" + provider, "fields": {}}


GETS: list = []         # record_ids read back with get_row (F51, F80)


def _fake_get_row(rid):
    """The fake Base: a record holds whatever the fake update_row last put."""
    GETS.append(rid)
    f: dict = {}
    for r, fl in WRITES:
        if r == rid:
            f.update(fl)
    return f


va._tenant_token = _token

# Confirm-before-write: the model is STUBBED to agree, so these tests keep
# testing the rules; the gate itself is tested in testing/vawatch_state_test.py
# and testing/providerllm_confirm_test.py. Never a real model call.
CONFIRM = {"verdict": "yes", "transient": False, "why": "test stub: confirmed"}
va._llm_confirm = lambda *a, **k: dict(CONFIRM)
va.send_card = _fake_card
va.send_text = _fake_text
va.update_row = lambda rid, fields: (WRITES.append((rid, dict(fields))), {"code": 0})[1]
va.find_provider_row = _fake_find
if hasattr(va, "get_row"):
    va.get_row = _fake_get_row
va.classify = lambda text, *a, **k: noticeparse.classify(text, now=NOW)
# The watcher's own clock (owner/pending rules) pinned to the same instant, or
# "is the row's window still ahead?" would be asked of the real date.
if hasattr(va, "_now_dt"):
    va._now_dt = lambda: NOW
_TMP = Path(tempfile.mkdtemp(prefix="vawatch_selftest_"))
va.LEDGER_PATH = _TMP / "vawatch.json"

# vawatch reads /provideraskmaintenance's run state (F72) - the state FILE only,
# never a send. It must be a tempdir file, not the repo's real one.
import providerask as pa  # noqa: E402  (defines only; nothing runs at import)

pa.STATE_PATH = _TMP / "providerask_state.json"

import telegramwarm as tw  # noqa: E402  (defines only; nothing starts at import)

tw.send_text = _blocked
_scrub_env()


def _files_fenced() -> str:
    """Why the ledger or the providerask state is NOT in the tempdir, or "".

    The network fences above fail closed on their own; these two paths did not.
    With the ``va.LEDGER_PATH`` line lost, every test wrote the repo's REAL
    vawatch.json - on the bot's host, the live watcher's ledger - before 104 of
    them failed; with the ``pa.STATE_PATH`` line lost, the F72 tests wrote an
    open /provideraskmaintenance run into the real providerask_state.json and
    the suite still passed, so the live bot would have swept that fake run and
    posted its summary card to the Laboratory group. main() refuses to run any
    test until both point into _TMP.
    """
    tmp = _TMP.resolve()
    for name, p in (("vawatch.LEDGER_PATH", va.LEDGER_PATH),
                    ("providerask.STATE_PATH", pa.STATE_PATH)):
        if tmp not in Path(p).resolve().parents:
            return f"{name} is {p}, outside the selftest tempdir {tmp}"
    return ""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SG = "[SG190- IGO Casinoplus YG/ RG/ HS] CS group"
PP, PPG = "Pragmatic Play", "Pragmatic Play x Casinoplus CS"


def fresh(rows=None, env=None) -> None:
    """Empty ledger, empty sinks, the given Base rows, only the given env."""
    WRITES.clear(); CARDS.clear(); FINDS.clear(); GETS.clear()
    _scrub_env()
    for k, v in (env or {}).items():
        os.environ[k] = v
    va._LAST_WATCH.clear()
    if hasattr(va, "_LAST_SOURCE"):
        va._LAST_SOURCE.clear()
    if hasattr(va, "_CURSOR_MEM"):
        va._CURSOR_MEM = None       # a failed-save cursor must not leak between tests
    for mod in (va, noticeparse):   # F83: a config warning belongs to one test only
        if hasattr(mod, "_CONFIG_WARNED"):
            mod._CONFIG_WARNED.clear()
    for p in list(_TMP.glob("vawatch*")) + list(_TMP.glob("providerask*")):
        p.unlink()
    ROWS[:] = [dict(r) for r in (rows or [])]
    if ROWS:
        va.watch_list()            # the rotation's own read; also seeds the cache


def row(provider, group, rid=None, apps=("telegram",)):
    return {"provider": provider, "group": group, "record_id": rid or "rec" + provider,
            "apps": set(apps)}


BASE = [row(PP, PPG, "recPP"), row("Hacksaw", SG, "recHS"), row("YGG", SG, "recYGG"),
        row("Evolution", "Evolution CS", "recEVO"), row("JILI", "JILI CS", "recJILI"),
        row("VA", va._chat_title(), "recVA"), row("GEMINI", "Gemini Teams", "recGEM",
                                                   apps=("teams",))]


def msg(mid, text, **kw):
    m = {"mid": str(mid), "text": text, "edited": False, "out": False,
         "sender": "", "time": "", "kind": None}
    m.update(kw)
    return m


def sweep(msgs, provider=PP, group=PPG, *, baseline=True, **kw):
    """One handle_messages call exactly as telegramwarm makes it."""
    if baseline and not va.is_baselined(group, provider):
        va.mark_baselined(group, provider)
    tgt = next((r for r in va.watch_list()["telegram"]
                if va._norm(r["group"]) == va._norm(group)
                and va._norm(r["provider"]) == va._norm(provider)), {})
    kw.setdefault("record_id", tgt.get("record_id", ""))
    kw.setdefault("shared_with", tgt.get("shared_with"))
    return va.handle_messages(msgs, provider=provider, group=group, **kw)


def ms(v):
    return datetime.fromtimestamp(v / 1000, TZ8).strftime("%Y-%m-%d %H:%M") if v else v


def written():
    return [(rid, ms(f.get("Start Time")), ms(f.get("End Time"))) for rid, f in WRITES]


def titles():
    return [t for t, _ in CARDS]


def actions(res):
    return [x.get("action") for x in res["details"]]


def check(cond, what):
    if not cond:
        raise AssertionError(what)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

TESTS: list = []


def case(fid, name):
    def deco(fn):
        TESTS.append((fid, name, fn))
        return fn
    return deco


def _vacheck_source() -> str:
    """main.py's real /vacheck branch, as a function taking the handler's
    locals. Read as TEXT: importing main starts the scheduler."""
    src = (REPO / "main.py").read_text(encoding="utf-8").splitlines()
    i0 = next(i for i, l in enumerate(src) if l.strip() == "elif cmd == '/vacheck':")
    ind = len(src[i0]) - len(src[i0].lstrip())
    i1 = next(i for i in range(i0 + 1, len(src))
              if src[i].strip().startswith("elif ")
              and len(src[i]) - len(src[i].lstrip()) == ind)
    body = textwrap.dedent("\n".join(src[i0 + 1:i1]))
    return "def _vacheck():\n" + textwrap.indent(body, "    ")


def run_vacheck(parts, va_check_now):
    """Run /vacheck as main.py does, with telegramwarm.va_check_now faked and
    its thread run inline. -> the operator-facing replies."""
    sent = []

    class _Inline:
        def __init__(self, target=None, daemon=None, **k):
            self.target = target

        def start(self):
            self.target()

    ns = {"cmd_parts": list(parts), "chat_id": "oc_selftest_operator",
          "send_message": lambda chat, text: sent.append(text),
          "threading": types.SimpleNamespace(Thread=_Inline),
          "_lark_im_done": lambda: None}
    fake_tw = types.SimpleNamespace(va_check_now=va_check_now)
    saved = sys.modules.get("telegramwarm")
    sys.modules["telegramwarm"] = fake_tw
    try:
        exec(_vacheck_source(), ns)
        ns["_vacheck"]()
    finally:
        sys.modules["telegramwarm"] = saved
    return sent


def real_va_check_now(msgs, seen_kwargs=None):
    """A va_check_now that runs the REAL handle_messages the way the worker
    does, on the target /vacheck passed."""
    def _now(force=False, timeout_s=300, target=None, **kw):
        if seen_kwargs is not None:
            seen_kwargs.append({"force": force, "target": target, **kw})
        t = target or {}
        r = va.handle_messages(msgs, force=force, provider=t.get("provider", ""),
                               group=t.get("group", ""),
                               record_id=t.get("record_id", ""),
                               shared_with=t.get("shared_with"))
        r["group"], r["provider"] = t.get("group", ""), t.get("provider", "")
        return {"ok": True, "error": "", "result": r}
    return _now


def worker(read_msgs):
    """telegramwarm's real worker object, browser replaced, never started."""
    w = tw._TelegramWarm()
    w._code_wait_active = lambda: False
    w._healthy = lambda: True
    w._va_auth_at = time.monotonic()

    def _no_browser(*a, **k):
        raise RuntimeError("the browser was touched in vawatch_selftest")

    w._launch = w._check_auth = _no_browser
    # Recorded, never sent: the real one posts to Lark when the watcher is
    # enabled. (VAWATCH_ENABLED is scrubbed too, so even the real one returns.)
    w.failures = []
    w._va_note_failure = lambda title, why: w.failures.append((title, why))
    tw._open_verified = lambda page, title, pin="", **k: {"ok": True}
    tw._pinned_peer_for = lambda title: ""
    tw._back_to_chat_list = lambda page, **k: None
    tw._read_last_messages = lambda page, n, max_chars=0, **k: {"messages": list(read_msgs)}
    return w


# ---------------------------------------------------------------------------
# F55 — /vacheck summary crashed on the `unwatched` dict
# ---------------------------------------------------------------------------

@case("F55", "summary renders a real handle_messages result, dict unwatched included")
def _():
    fresh(BASE)
    r = sweep([msg(1, "Dear partners, scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8).")])
    check(isinstance(r["unwatched"], dict) and r["unwatched"]["teams"],
          f"precondition: unwatched is the cached dict with a TEAMS row: {r['unwatched']}")
    r["group"], r["provider"] = PPG, PP
    text = va.format_check_summary(r)
    check("• filled: 1" in text, text)
    check("NOT autofilled by any code path: GEMINI (APP is TEAMS" in text, text)
    check("1 card(s) posted to the Laboratory group." in text, text)
    check("recPP" in text and "2026-09-27 02:00" in text, f"row/window not reported: {text}")


@case("F55", "/vacheck and /vacheck force (main.py's real block) reply with the summary")
def _():
    for parts in (["/vacheck"], ["/vacheck", "force"], ["/vacheck", "Pragmatic", "Play"]):
        fresh(BASE)
        va.mark_baselined(va._chat_title(), "VA")
        va.mark_baselined(PPG, PP)
        m = [msg(1, "Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8).")]
        replies = run_vacheck(parts, real_va_check_now(m))
        check(len(replies) == 2, f"{parts}: {replies}")
        check(not any("failed" in x for x in replies), f"{parts}: {replies}")
        check(replies[1].startswith("✅ VA sweep done"), f"{parts}: {replies[1]}")
        check("• filled: 1" in replies[1] and "card(s) posted" in replies[1],
              f"{parts}: {replies[1]}")


@case("F55", "fresh ledger: no empty 'NOT autofilled' line, no crash")
def _():
    fresh([])
    r = va.handle_messages([msg(1, "hello")], provider=PP, group=PPG)
    text = va.format_check_summary(dict(r, group=PPG, provider=PP))
    check("NOT autofilled" not in text, text)
    check("card(s) posted" not in text, text)


@case("F55", "a clear is not counted as 'filled'; a suppressed card is not claimed")
def _():
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    r = sweep([msg(1, "Hi team, no maintenance this week.")])
    text = va.format_check_summary(dict(r, group=PPG, provider=PP))
    check("• filled: 0" in text and "row cleared (no maintenance): 1" in text, text)
    fresh(BASE, env={"VAWATCH_NEEDS_HUMAN_CARD": "0"})
    # "Maintenance" is in it on purpose: a bare "Postponed: …" names nothing
    # and is ordinary chat since F58, so it would not reach the card at all.
    r = sweep([msg(1, "Maintenance postponed: new window to be advised.")])
    text = va.format_check_summary(dict(r, group=PPG, provider=PP))
    check("needs a human: 1" in text and "card(s) posted" not in text, text)
    check(not CARDS, f"a disabled card was sent: {titles()}")


# ---------------------------------------------------------------------------
# F38 — our own bubbles were classified and written
# ---------------------------------------------------------------------------

F38_TEXTS = [
    "Dear partners, please note Casinoplus will have scheduled maintenance on "
    "2026-09-25 02:00 - 04:00 (GMT+8). Kindly take note.",
    "Hi team, can the maintenance be postponed to 2026-09-25 02:00-04:00 (GMT+8)?",
    "Noted with thanks. We will set scheduled maintenance for 2026-09-24 10:00-12:00 "
    "(GMT+8) on our side.",
]


@case("F38", "an out=True bubble is skipped before classify: no write, no card")
def _():
    for text in F38_TEXTS:
        fresh(BASE)
        called = []
        real = va.classify
        va.classify = lambda t, *a, **k: (called.append(t), real(t))[1]
        try:
            r = sweep([msg(1, text, out=True, sender="me")])
        finally:
            va.classify = real
        check(not WRITES and not CARDS, f"{text[:40]}: {written()} {titles()}")
        check(actions(r) == ["outbound"] and r["ignored_outbound"] == 1, str(r["details"]))
        check(not called, "classify ran on an outbound bubble")
        r = sweep([msg(1, text, out=True)])
        check(actions(r) == ["already handled"], f"re-read: {actions(r)}")


@case("F38", "VAWATCH_SKIP_OUTBOUND=0 restores classification (the documented escape)")
def _():
    # F38_TEXTS[1] is a reschedule QUESTION, which F20's question guard now refuses
    # on its own; NOTICE_QUESTION_GUARD=0 isolates the one switch under test here,
    # so this still proves the outbound skip is what kept the bubble unwritten.
    fresh(BASE, env={"VAWATCH_SKIP_OUTBOUND": "0", "VAWATCH_OWNER_CHECK": "0",
                     "NOTICE_QUESTION_GUARD": "0"})
    sweep([msg(1, F38_TEXTS[1], out=True)])
    check(written() == [("recPP", "2026-09-25 02:00", "2026-09-25 04:00")], str(written()))


@case("F38", "an inbound provider notice in the same read still fills")
def _():
    fresh(BASE)
    r = sweep([msg(1, F38_TEXTS[1], out=True),
               msg(2, "Scheduled maintenance on 2026-09-26 10:00 - 12:00 (GMT+8).")])
    check(actions(r) == ["outbound", "filled"], str(actions(r)))
    check(written() == [("recPP", "2026-09-26 10:00", "2026-09-26 12:00")], str(written()))


# ---------------------------------------------------------------------------
# F37 — the shared Hacksaw/YGG group wrote every notice onto BOTH rows
# ---------------------------------------------------------------------------

def shared_sweep(text, mid=7, rows=("Hacksaw", "YGG"), **kw):
    out = {}
    for p in rows:
        out[p] = sweep([msg(mid, text)], provider=p, group=SG, **kw)
    return out


@case("F37", "watch_list annotates the co-tenants of a shared group")
def _():
    fresh(BASE)
    rows = {r["provider"]: r for r in va.watch_list()["telegram"]}
    check(rows["Hacksaw"]["shared_with"] == ["YGG"], str(rows["Hacksaw"]))
    check(rows["YGG"]["shared_with"] == ["Hacksaw"], str(rows["YGG"]))
    check(rows[PP]["shared_with"] == [], str(rows[PP]))


@case("F37", "a Yggdrasil-only notice ('Hacksaw NOT affected') writes only the YGG row")
def _():
    fresh(BASE)
    r = shared_sweep("Please be informed that Yggdrasil (YGG) will undergo scheduled "
                     "maintenance on 2026-09-26 from 02:00 to 04:00 (GMT+8). Hacksaw "
                     "and Relax games are NOT affected.")
    check(written() == [("recYGG", "2026-09-26 02:00", "2026-09-26 04:00")], str(written()))
    check(actions(r["Hacksaw"]) == ["other-provider"], str(r["Hacksaw"]["details"]))
    check(titles() == ["🛠️ Scheduled maintenance · YGG"], str(titles()))


@case("F37", "a Hacksaw notice does not overwrite YGG's live window")
def _():
    fresh(BASE)
    shared_sweep("Yggdrasil scheduled maintenance on 2026-09-26 02:00 - 04:00 (GMT+8).", mid=1)
    WRITES.clear()
    shared_sweep("HS scheduled maintenance on 2026-09-27 05:00 - 06:00 (GMT+8).", mid=2)
    check(written() == [("recHS", "2026-09-27 05:00", "2026-09-27 06:00")], str(written()))


@case("F37", "a notice naming neither row: no write, ONE needs-human card for the group")
def _():
    for text in ["Scheduled maintenance 26/09/2026 10:00 - 12:00 (GMT+8)",
                 "Relax Gaming (RG) scheduled maintenance on 2026-09-26 02:00 - 04:00 "
                 "(GMT+8). YGG and Hacksaw games are NOT affected."]:
        fresh(BASE)
        r = shared_sweep(text)
        check(not WRITES, f"{text[:30]}: {written()}")
        check(len(CARDS) == 1 and CARDS[0][0].startswith("❓"), f"{text[:30]}: {titles()}")
        check(r["YGG"]["details"][0].get("card_by") == "Hacksaw", str(r["YGG"]["details"]))
        check(all(x["needs_human"] == 1 for x in r.values()), "both rows must record it")


@case("F37", "a notice naming BOTH rows positively is not guessed")
def _():
    fresh(BASE)
    shared_sweep("YGG and Hacksaw will both have scheduled maintenance on 2026-09-26 "
                 "02:00 - 04:00 (GMT+8).")
    check(not WRITES and len(CARDS) == 1, f"{written()} {titles()}")


@case("F37", "a card that FAILED is not recorded, so the co-tenant row still sends it")
def _():
    fresh(BASE)
    va.send_card = lambda chat, card: {"code": 99}
    va.send_text = lambda chat, text: {"code": 99}
    try:
        sweep([msg(7, "Scheduled maintenance 26/09/2026 10:00 - 12:00 (GMT+8)")],
              provider="Hacksaw", group=SG)
    finally:
        va.send_card, va.send_text = _fake_card, _fake_text
    sweep([msg(7, "Scheduled maintenance 26/09/2026 10:00 - 12:00 (GMT+8)")],
          provider="YGG", group=SG)
    check(len(CARDS) == 1, f"the YGG row should have carded it: {titles()}")


@case("F37", "aliases are configurable (VAWATCH_PROVIDER_ALIASES replaces the default)")
def _():
    fresh(BASE, env={"VAWATCH_PROVIDER_ALIASES": "Hacksaw=HSG;YGG=Yggdrasil"})
    shared_sweep("HS scheduled maintenance on 2026-09-26 02:00 - 04:00 (GMT+8).", mid=1)
    check(not WRITES, f"'HS' is no longer an alias: {written()}")
    shared_sweep("HSG scheduled maintenance on 2026-09-26 02:00 - 04:00 (GMT+8).", mid=2)
    check(written() == [("recHS", "2026-09-26 02:00", "2026-09-26 04:00")], str(written()))


@case("F37", "a caller that passes no shared_with (old cached target) is still attributed")
def _():
    fresh(BASE)
    va._LAST_WATCH.clear()          # only the ledger's cached list is left
    for p in ("Hacksaw", "YGG"):
        va.mark_baselined(SG, p)
        va.handle_messages([msg(7, "Yggdrasil scheduled maintenance on 2026-09-26 "
                                   "02:00 - 04:00 (GMT+8).")], provider=p, group=SG)
    check([w[0] for w in written()] == ["rec_YGG"], str(written()))


@case("F37", "clear in the shared group naming both rows: nothing is cleared")
def _():
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    shared_sweep("Hi team, no maintenance for Hacksaw this week. YGG will share its "
                 "own schedule separately.")
    check(not WRITES and len(CARDS) == 1, f"{written()} {titles()}")


@case("F37", "VAWATCH_SHARED_GROUP_CHECK=0 is the kill-switch (old behaviour, both rows)")
def _():
    fresh(BASE, env={"VAWATCH_SHARED_GROUP_CHECK": "0"})
    shared_sweep("Scheduled maintenance 26/09/2026 10:00 - 12:00 (GMT+8)")
    check(sorted(w[0] for w in written()) == ["recHS", "recYGG"], str(written()))


@case("F37", "a one-row group still fills a notice that names nobody")
def _():
    fresh(BASE)
    sweep([msg(1, "Scheduled maintenance 26/09/2026 10:00 - 12:00 (GMT+8)")])
    check(written() == [("recPP", "2026-09-26 10:00", "2026-09-26 12:00")], str(written()))


# ---------------------------------------------------------------------------
# G4.1 — the operator's own maintenance was written onto the provider's row
# ---------------------------------------------------------------------------

G41_OPERATOR = [
    "Dear partner, Casinoplus will perform system maintenance on 24/09/2026 02:00-04:00 "
    "(GMT+8). Kindly avoid deployments during this period.",
    "Hi team, our side (CasinoPlus) will have platform maintenance on 25/09/2026 "
    "03:00-05:00 GMT+8, please take note.",
    "Hi team, IGO scheduled maintenance: 2026-09-25 03:00-05:00 (GMT+8). Players will not "
    "be able to launch your games during this window.",
    "【通知】我司将于2026年9月24日02:00-04:00（GMT+8）进行系统维护，届时请暂停发版。",
    "各位好，我方平台将于2026-09-24 02:00-04:00进行例行维护，期间无流量，请知悉。",
    "CP will have scheduled maintenance on 2026-09-25 03:00-05:00 (GMT+8).",
]

# Provider notices that mention the operator only as the ADDRESSEE or co-brand.
G41_PROVIDER_CONTROLS = [
    "Dear CasinoPlus, Pragmatic Play will have scheduled maintenance on 2026-09-27 "
    "02:00 - 04:00 (GMT+8).",
    "Dear Casinoplus team,\nScheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8).",
    "To: CasinoPlus\nScheduled maintenance 2026-09-27 02:00 - 04:00 (GMT+8)",
    "Hi IGO team, we will have scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8).",
    "Pragmatic Play x CasinoPlus scheduled maintenance 2026-09-27 02:00 - 04:00 (GMT+8)",
    "尊敬的CasinoPlus，我司将于2026-09-27 02:00-04:00（GMT+8）进行系统维护，敬请知悉。",
    "Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8). Our games will be "
    "removed from your lobby and your players cannot log in during this period.",
]


@case("G4.1", "operator-side maintenance: no write, a needs-human card, earlier window kept")
def _():
    for text in G41_OPERATOR:
        fresh(BASE)
        sweep([msg(1, "Scheduled maintenance on 2026-09-28 10:00 - 12:00 (GMT+8).")])
        WRITES.clear(); CARDS.clear()
        r = sweep([msg(2, text, sender="CasinoPlus Ops")])
        check(not WRITES, f"wrote {written()} for: {text[:50]}")
        check(actions(r) == ["needs-human"] and "operator" in r["details"][0]["why"],
              f"{text[:50]}: {r['details']}")
        check(len(CARDS) == 1 and CARDS[0][0].startswith("❓"), str(titles()))


@case("G4.1", "the operator's own 'no maintenance' never clears the provider's row")
def _():
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    sweep([msg(1, "Scheduled maintenance on 2026-09-28 10:00 - 12:00 (GMT+8).")])
    WRITES.clear()
    text = "Hi team, CasinoPlus has no maintenance this week."
    r = sweep([msg(2, text)])
    check(not WRITES, f"cleared: {written()}")
    if va.classify(text)["action"] == "clear":
        check(actions(r) == ["needs-human"], str(r["details"]))


@case("G4.1", "a provider notice addressed TO the operator still fills")
def _():
    for text in G41_PROVIDER_CONTROLS:
        fresh(BASE)
        sweep([msg(1, text)])
        check(written() == [("recPP", "2026-09-27 02:00", "2026-09-27 04:00")],
              f"{written()} for: {text[:60]}")


@case("G4.1", "VAWATCH_OPERATOR_SENDERS: a colleague's bubble is carded, run names inherited")
def _():
    fresh(BASE, env={"VAWATCH_OPERATOR_SENDERS": "CP Ops Jun | Night Shift"})
    r = sweep([msg(1, "Hi all", sender="CP Ops Jun"),
               msg(2, "Scheduled maintenance on 2026-09-26 10:00 - 12:00 (GMT+8).", sender=""),
               msg(3, "Scheduled maintenance on 2026-09-27 10:00 - 12:00 (GMT+8).",
                   sender="Pragmatic CS")])
    check(written() == [("recPP", "2026-09-27 10:00", "2026-09-27 12:00")], str(written()))
    check(actions(r)[1] == "needs-human" and "CP Ops Jun" in r["details"][1]["why"],
          str(r["details"]))


# ---------------------------------------------------------------------------
# G4.2 — payment / telco / cloud maintenance relayed into a provider group
# ---------------------------------------------------------------------------

G42_TEXTS = [
    "FYI GCash will have scheduled maintenance on 24/09/2026 00:00-02:00 (GMT+8). Cash-in "
    "via GCash will be unavailable, expect lower traffic.",
    "Maya system maintenance on 2026-09-24 01:00-03:00 (GMT+8), deposits via Maya will be "
    "temporarily unavailable.",
    "Please be informed BDO online banking will undergo scheduled maintenance on "
    "24/09/2026 00:00-04:00 GMT+8.",
    "Globe Telecom network maintenance on 24/09/2026 01:00-05:00 (GMT+8), some players may "
    "experience connection issues.",
    "Cloudflare scheduled maintenance in the MNL data centre on 2026-09-24 02:00-04:00 "
    "UTC+8, traffic will be rerouted.",
    "AWS scheduled maintenance: 2026-09-24 02:00-04:00 (GMT+8) for the ap-southeast-1 region.",
    "Kindly note: GCash scheduled maintenance 24/09/2026 03:00-04:00 GMT+8. No impact to "
    "our games.",
    "PLDT scheduled maintenance on 2026-09-24 01:00-03:00 (GMT+8).",
]


@case("G4.2", "a payment/telco/cloud window is never written onto the provider's row")
def _():
    for text in G42_TEXTS:
        fresh(BASE)
        r = sweep([msg(1, text)])
        check(not WRITES, f"wrote {written()} for: {text[:50]}")
        check(actions(r) == ["needs-human"], f"{text[:50]}: {r['details']}")


@case("G4.2", "the provider's OWN outage mentioning infrastructure still fills")
def _():
    for text in ["Pragmatic Play will perform scheduled maintenance on 2026-09-27 02:00 - "
                 "04:00 (GMT+8) due to an AWS upgrade.",
                 "Our data centre will have scheduled maintenance on 2026-09-27 02:00 - "
                 "04:00 (GMT+8). Games will be unavailable."]:
        fresh(BASE)
        sweep([msg(1, text)])
        check(written() == [("recPP", "2026-09-27 02:00", "2026-09-27 04:00")],
              f"{written()} for: {text[:50]}")


@case("G4.2", "VAWATCH_OWNER_CHECK=0 is the kill-switch")
def _():
    fresh(BASE, env={"VAWATCH_OWNER_CHECK": "0"})
    sweep([msg(1, G42_TEXTS[5])])
    check(len(WRITES) == 1, str(written()))


# ---------------------------------------------------------------------------
# G4.4 — another brand's maintenance relayed into a one-row group
# ---------------------------------------------------------------------------

@case("G4.4", "another Base provider's notice, or 'not your games', is not written")
def _():
    for text in ["FYI Evolution will have scheduled maintenance on 2026-09-24 10:00-12:00 "
                 "(GMT+8), players may move to your live tables.",
                 "Please note JILI has emergency maintenance on 24/09/2026 14:00-16:00 "
                 "GMT+8, not your games.",
                 "FYI Evolution will have scheduled maintenance on 2026-09-24 10:00-12:00 "
                 "(GMT+8).",
                 "GEMINI scheduled maintenance on 2026-09-24 10:00-12:00 (GMT+8)."]:
        fresh(BASE)
        r = sweep([msg(1, text)])
        check(not WRITES, f"wrote {written()} for: {text[:50]}")
        check(actions(r) == ["needs-human"], f"{text[:50]}: {r['details']}")


@case("G4.4", "a notice that names this provider only as NOT affected is not written")
def _():
    fresh(BASE)
    text = ("Scheduled maintenance on 2026-09-24 10:00-12:00 (GMT+8). "
            "Pragmatic Play games are not affected.")
    r = sweep([msg(1, text)])
    check(not WRITES, f"{written()} {r['details']}")
    # Pinned only while noticeparse still reads this as a fill; if the parser
    # lane learns to ignore no-impact notices itself, "no write" is what counts.
    if va.classify(text)["action"] == "fill":
        check(actions(r) == ["needs-human"], str(r["details"]))


@case("G4.4", "the provider's own notice naming a sub-brand or partner still fills")
def _():
    fresh(BASE)
    sweep([msg(1, "Pragmatic Play and Evolution tables will have scheduled maintenance "
                  "on 2026-09-27 02:00 - 04:00 (GMT+8).")])
    check(written() == [("recPP", "2026-09-27 02:00", "2026-09-27 04:00")], str(written()))


# ---------------------------------------------------------------------------
# F39 — a blank Provider became "VA"
# ---------------------------------------------------------------------------

@case("F39", "blank-Provider target through the real worker: VA row untouched, card says so")
def _():
    fresh(BASE)
    va.mark_baselined("[SG200] NewProvider CS", "")
    w = worker([msg(1, "Dear partners, NewProvider will perform scheduled maintenance on "
                       "2026-09-26 03:00 - 05:00 (GMT+8).")])
    box: dict = {}
    w._handle_va_watch({"kind": "va_watch", "box": box,
                        "target": {"group": "[SG200] NewProvider CS", "provider": ""}})
    check(box.get("ok"), str(box))
    check(not WRITES and not FINDS, f"wrote {written()} / looked up {FINDS}")
    check(box["result"]["provider"] == "", f"provider became {box['result']['provider']!r}")
    card = CARDS[0][1]
    body = card["body"]["elements"][0]["text"]["content"]
    check("(blank in the Base)" in body and "no Provider" in body, body)


@case("F39", "watch_list moves a blank-Provider TELEGRAM row to `skipped`, named")
def _():
    fresh(BASE + [row("", "[SG200] NewProvider CS", "recBLANK")])
    rep = va.watch_list()
    check(all(r["provider"] for r in rep["telegram"]), str(rep["telegram"]))
    check(any(s["group"] == "[SG200] NewProvider CS" and "no Provider" in s["why"]
              for s in rep["skipped"]), str(rep["skipped"]))


@case("F39", "no target at all still falls back to the configured VA pair")
def _():
    fresh([])
    real_nt, va.next_target = va.next_target, (lambda: {})
    try:
        w = worker([msg(1, "hello")])
        box: dict = {}
        w._handle_va_watch({"kind": "va_watch", "box": box})
    finally:
        va.next_target = real_nt
    check(box["result"]["provider"] == "VA" and box["result"]["group"] == va._chat_title(),
          str(box.get("result", {}).get("provider")))


# ---------------------------------------------------------------------------
# F70 — the write re-found the row by name
# ---------------------------------------------------------------------------

@case("F70", "the watched record_id is written directly; no lookup by name")
def _():
    fresh([row("Play'n GO", "PNG CS", "recPNG")])
    tgt = va.next_target()
    check(tgt.get("record_id") == "recPNG", str(tgt))
    w = worker([msg(1, "Scheduled maintenance 2026-09-26 10:00 - 12:00 (GMT+8)")])
    va.mark_baselined("PNG CS", "Play'n GO")
    box: dict = {}
    w._handle_va_watch({"kind": "va_watch", "box": box, "target": tgt})
    check(written() == [("recPNG", "2026-09-26 10:00", "2026-09-26 12:00")], str(written()))
    check(not FINDS, f"find_provider_row was called: {FINDS}")


@case("F70", "fallback lookup flattens like groupcheck and refuses duplicate names")
def _():
    pages = [{"code": 0, "data": {"has_more": False, "items": [
        {"record_id": "recA", "fields": {"Provider / Games": [{"text": "Play'n "},
                                                              {"text": "GO"}]}},
        {"record_id": "recY1", "fields": {"Provider / Games": "YGG"}},
        {"record_id": "recY2", "fields": {"Provider": "YGG"}},
    ]}}]
    real_get, real_tok = va.requests.get, va._tenant_token
    va.requests.get = lambda *a, **k: types.SimpleNamespace(json=lambda: pages[0])
    va._tenant_token = lambda: "offline-fake-token"
    try:
        watched = gc._field_text(gc._first_field(pages[0]["data"]["items"][0]["fields"],
                                                 gc._PROVIDER_FIELDS))
        got = _REAL_FIND(watched)
        check(got.get("record_id") == "recA", f"{watched!r} -> {got}")
        try:
            _REAL_FIND("YGG")
            raise AssertionError("two rows named YGG were not refused")
        except RuntimeError as err:
            check("refusing to guess" in str(err), str(err))
    finally:
        va.requests.get, va._tenant_token = real_get, real_tok


# ---------------------------------------------------------------------------
# F56 — /vacheck force: wrong group, cursor consumed, stale/done/cancel walked past
# ---------------------------------------------------------------------------

@case("F56", "/vacheck targets the configured VA group and leaves the cursor alone")
def _():
    fresh(BASE)
    va.next_target()
    before = va._load().get("cursor_pair")
    seen: list = []
    replies = run_vacheck(["/vacheck", "force"], real_va_check_now([], seen))
    check(va._load().get("cursor_pair") == before, "the rotation cursor moved")
    check(seen and seen[0]["target"]["provider"] == "VA"
          and seen[0]["target"]["record_id"] == "recVA", str(seen))
    check(va._chat_title() in replies[0], replies[0])


@case("F56", "/vacheck <name> resolves by provider, alias or group; ambiguity is refused")
def _():
    fresh(BASE)
    check(va.manual_target("pragmatic play")["record_id"] == "recPP", "by provider")
    check(va.manual_target("Yggdrasil")["record_id"] == "recYGG", "by alias")
    check(va.manual_target("Evolution CS")["record_id"] == "recEVO", "by group")
    check("matches 2 rows" in va.manual_target(SG).get("error", ""), "shared group title")
    check("no watched TELEGRAM row" in va.manual_target("Nope").get("error", ""), "unknown")
    replies = run_vacheck(["/vacheck", "Nope"], real_va_check_now([]))
    check(len(replies) == 1 and replies[0].startswith("❌ /vacheck:"), str(replies))


@case("F56", "the worker does not consume next_target() when given a target")
def _():
    fresh(BASE)
    real_nt = va.next_target
    va.next_target = lambda: (_ for _ in ()).throw(AssertionError("next_target consumed"))
    try:
        w = worker([msg(1, "hello")])
        box: dict = {}
        w._handle_va_watch({"kind": "va_watch", "box": box, "force": True,
                            "target": va.manual_target("JILI")})
    finally:
        va.next_target = real_nt
    check(box.get("ok") and box["result"]["provider"] == "JILI", str(box))


@case("F56", "force stops at done / cancelled / no-maintenance / stale / unreadable")
def _():
    old = msg(1, "Scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8).")
    for label, newest in [
            ("done", "The maintenance has been completed."),
            ("cancelled", "The scheduled maintenance on 2026-09-25 has been cancelled."),
            ("no-maint", "Hi team, no maintenance this week."),
            ("stale", "Scheduled maintenance on 2026-09-21 10:00 - 12:00 (GMT+8)."),
            ("unparsed", "Scheduled maintenance tomorrow, time to be confirmed.")]:
        fresh(BASE)
        sweep([old])                         # the timer wrote 09-25 when it was new
        WRITES.clear(); CARDS.clear()
        r = sweep([old, msg(2, newest), msg(3, "thanks team")], force=True)
        check(not WRITES, f"{label}: force wrote {written()}")
        check(r["force_stop"], f"{label}: no force_stop reported: {r['details']}")
        check(not any(t.startswith("🛠️") for t in titles()), f"{label}: {titles()}")


@case("F56", "force still proves the path: walks past chatter to the newest notice")
def _():
    fresh(BASE)
    r = sweep([msg(1, "Scheduled maintenance on 2026-09-26 10:00 - 12:00 (GMT+8)."),
               msg(2, "thanks"), msg(3, "ok noted")], force=True)
    check(written() == [("recPP", "2026-09-26 10:00", "2026-09-26 12:00")], str(written()))
    check(actions(r)[-1] == "filled", str(actions(r)))


# ---------------------------------------------------------------------------
# F68 — force buried every needs-human card after the first
# ---------------------------------------------------------------------------

@case("F68", "under force a needs-human is carded and ends the walk; the next tick cards the rest")
def _():
    fresh(BASE)
    old = msg(1, "hello team")
    n1 = msg(2, "Maintenance for Game A is postponed, new schedule to be announced.")
    n2 = msg(3, "Maintenance for Game B is rescheduled; we will confirm the new date later.")
    sweep([old])
    r = sweep([old, n1, n2], force=True)
    check(r["needs_human"] == 1 and len(CARDS) == 1, f"{r['details']} {titles()}")
    r = sweep([old, n1, n2])
    check(actions(r) == ["already handled", "needs-human", "already handled"], str(actions(r)))
    check(len(CARDS) == 2, f"every new needs-human must get its card: {titles()}")
    for k, v in va._load()["handled"].items():
        check(v.get("outcome") != "needs-human" or not v.get("error"),
              f"{k} marked final with a suppressed card: {v}")


# ===========================================================================
# Stage W2 — ledger and cross-message state
# ===========================================================================
#
# Every wrong write below had the same shape: two messages for one row, and the
# OLDER one written last. The fixes key on data-mid order within one
# provider@group (vawatch's "Per-row state"), so the tests are written as reads
# of a group over several ticks, oldest bubble first, exactly as telegramwarm
# hands them over.

NA = "Dear Partners,\nScheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8).\nThank you."
NB = ("Dear Partners,\nThe maintenance has been rescheduled to 2026-09-26 10:00 - 12:00 "
      "(GMT+8).\nThank you.")


class at:
    """Pin classify()'s now AND the watcher's clock to one instant."""

    def __init__(self, when):
        self.when = when

    def __enter__(self):
        self.saved = (va.classify, va._now_dt)
        va.classify = lambda text, *a, **k: noticeparse.classify(text, now=self.when)
        va._now_dt = lambda: self.when
        return self

    def __exit__(self, *exc):
        va.classify, va._now_dt = self.saved


def row_now():
    """The last window written to the Base, as the row would show it."""
    return written()[-1] if WRITES else None


def scope(provider=PP, group=PPG):
    return (va._load().get("scopes") or {}).get(va._baseline_key(group, provider)) or {}


# ---------------------------------------------------------------------------
# F40 — FIFO eviction revived on-screen, already-decided messages
# ---------------------------------------------------------------------------

def _churn(nkeys, start=10_000, groups=17):
    """`nkeys` new bubbles spread over `groups` other busy groups."""
    mid = start
    for j in range(nkeys):
        mid += 1
        g = j % groups
        va.handle_messages([msg(mid, f"Hi team, please check round id {mid}.")],
                           provider=f"P{g}", group=f"Busy {g}")
    return mid


@case("F40", "a quiet group's filled notice survives churn past the cap (LRU, not FIFO)")
def _():
    fresh(BASE)
    saved = va._LEDGER_CAP, va._keep_per_scope
    va._LEDGER_CAP, va._keep_per_scope = 120, (lambda: 4)
    try:
        q = [msg(900, "Scheduled maintenance on 2026-10-20 02:00 - 04:00 (GMT+8)."),
             msg(901, "Thanks team")]
        sweep(q)
        mid = 10_000
        for _rot in range(20):               # 20 rotations x 17 new keys = 340 > 120
            mid = _churn(17, start=mid)
            r = sweep(q)
            check(actions(r) == ["already handled"] * 2, f"rotation {_rot}: {actions(r)}")
        d = va._load()
        check(len(d["order"]) <= 120 + 4 * 18, f"ledger not bounded: {len(d['order'])}")
        check(len(WRITES) == 1 and len(CARDS) == 1, f"re-acted: {written()} {titles()}")
    finally:
        va._LEDGER_CAP, va._keep_per_scope = saved


@case("F40", "a group not visited while others churn keeps its newest keys (per-scope floor)")
def _():
    fresh(BASE)
    saved = va._LEDGER_CAP, va._keep_per_scope
    va._LEDGER_CAP, va._keep_per_scope = 60, (lambda: 4)
    try:
        q = [msg(900, "Scheduled maintenance on 2026-10-20 02:00 - 04:00 (GMT+8)."),
             msg(901, "no maintenance this week? we will confirm")]
        sweep(q)
        n_cards = len(CARDS)
        _churn(400)                          # never revisited meanwhile
        r = sweep(q)
        check(actions(r) == ["already handled"] * 2, str(actions(r)))
        check(len(WRITES) == 1 and len(CARDS) == n_cards, f"{written()} {titles()}")
    finally:
        va._LEDGER_CAP, va._keep_per_scope = saved


@case("F40", "a backlog key that IS evicted anyway never revives (cold-start floor)")
def _():
    fresh(BASE)
    q = [msg(900, "Scheduled maintenance: May 20 (Wed), 02:00-04:00 GMT+8"),
         msg(901, "Scheduled maintenance on 2026-10-20 02:00 - 04:00 (GMT+8).")]
    va.handle_messages(q, provider=PP, group=PPG, record_id="recPP")      # cold
    check(not WRITES and scope().get("floor") == 901, f"floor: {scope()}")
    d = va._load()
    for k in [k for k in d["handled"] if "|mid:90" in k]:
        d["handled"].pop(k)                  # what the old FIFO trim did
    va._save(d)
    r = va.handle_messages(q, provider=PP, group=PPG, record_id="recPP")
    check(actions(r) == ["history", "history"], str(actions(r)))
    check(not WRITES and not CARDS, f"{written()} {titles()}")


@case("F40", "a group unread 30+ days loses every key to churn: its filled notice never revives")
def _():
    # The per-scope floor only protects scopes read in the last 30 days, so a
    # group whose title failed for longer lost even its on-screen keys - and
    # its WARM-phase notice (above the cold-start floor) was written and carded
    # again. The evicted mark (_note_evicted) makes that history.
    fresh(BASE)
    saved = va._LEDGER_CAP, va._keep_per_scope
    va._LEDGER_CAP, va._keep_per_scope = 60, (lambda: 4)
    try:
        sweep([msg(1, "hello")])
        q = [msg(1, "hello"),
             msg(900, "Scheduled maintenance on 2026-10-20 02:00 - 04:00 (GMT+8)."),
             msg(901, "Maintenance may move, we will confirm the new time later.")]
        sweep(q)
        check(len(WRITES) == 1, f"setup: {written()}")
        n_w, n_c = len(WRITES), len(CARDS)
        d = va._load()
        d["scopes"][va._baseline_key(PPG, PP)]["seen"] = "2026-08-01 00:00:00"
        va._save(d)
        _churn(400)
        d = va._load()
        bk = va._baseline_key(PPG, PP)
        check(not [k for k in d["handled"] if k.startswith(bk + "|")],
              "setup: the scope's keys should all have been evicted")
        check(d["scopes"][bk].get("evicted") == 901, f"evicted mark: {d['scopes'][bk]}")
        r = sweep(q)
        check(actions(r) == ["history"] * 3, str(actions(r)))
        check(len(WRITES) == n_w and len(CARDS) == n_c, f"revived: {written()} {titles()}")
        # A message posted AFTER the eviction is above the mark and still fills.
        r = sweep(q + [msg(902, "Scheduled maintenance on 2026-10-21 02:00 - 04:00 (GMT+8).")])
        check(actions(r)[-1] == "filled", str(actions(r)))
        check(row_now() == ("recPP", "2026-10-21 02:00", "2026-10-21 04:00"), str(written()))
    finally:
        va._LEDGER_CAP, va._keep_per_scope = saved


@case("F40", "a bd9fc12 ledger already FIFO-trimmed at 2000: nothing revives on the first sweep after deploy")
def _():
    # bd9fc12 uses the same key scheme, so _migrate keeps its ledger - with the
    # keys its first-insertion trim had already dropped. Here the quiet group's
    # filled notice (900) was trimmed and its newer chat (901, 902) survived.
    import json as _json
    fresh(BASE)
    bk = va._baseline_key(PPG, PP)
    handled, order = {}, []
    for j in range(1998):
        k = f"p{j % 17}@busy {j % 17}|mid:{10000 + j}"
        handled[k] = {"outcome": "ignored", "at": "2026-09-20 10:00:00", "attempts": 1}
        order.append(k)
    for m in (901, 902):
        handled[f"{bk}|mid:{m}"] = {"outcome": "ignored", "at": "2026-09-20 10:00:00",
                                    "attempts": 1}
        order.append(f"{bk}|mid:{m}")
    va.LEDGER_PATH.write_text(_json.dumps({"handled": handled, "order": order,
                                           "key_scheme": 2,
                                           "baselined": {bk: "2026-09-01 00:00:00"}}),
                              encoding="utf-8")
    q = [msg(900, "Scheduled maintenance on 2026-10-20 02:00 - 04:00 (GMT+8)."),
         msg(901, "Thanks team"), msg(902, "noted")]
    r = sweep(q, baseline=False)
    check(actions(r) == ["history", "already handled", "already handled"], str(actions(r)))
    check(not WRITES and not CARDS, f"revived on upgrade: {written()} {titles()}")
    r = sweep(q + [msg(903, "Scheduled maintenance on 2026-10-22 02:00 - 04:00 (GMT+8).")],
              baseline=False)
    check(actions(r)[-1] == "filled" and len(WRITES) == 1, f"{actions(r)} {written()}")


# ---------------------------------------------------------------------------
# F41 — an edit of an older, superseded notice overwrote the newer reschedule
# ---------------------------------------------------------------------------

@case("F41", "editing the superseded notice (typo, [POSTPONED], see-latest) keeps the reschedule")
def _():
    for label, edit in [("typo", NA.replace("Partners", "Partner")),
                        ("postponed", "[POSTPONED] " + NA),
                        ("see-latest", NA + "\n(Postponed - please see the latest notice)"),
                        ("moved to the same new date", NA.replace("09-25", "09-26"))]:
        fresh(BASE)
        sweep([msg(40, NA)])
        sweep([msg(40, NA), msg(41, NB)])
        check(row_now() == ("recPP", "2026-09-26 10:00", "2026-09-26 12:00"), str(written()))
        WRITES.clear(); CARDS.clear()
        r = sweep([msg(40, edit, edited=True), msg(41, NB)])
        check(not WRITES and not CARDS, f"{label}: {written()} {titles()}")
        check(actions(r) == ["superseded", "already handled"], f"{label}: {actions(r)}")
        r = sweep([msg(40, edit, edited=True), msg(41, NB)])
        check(actions(r) == ["already handled"] * 2, f"{label} re-read: {actions(r)}")


@case("F41", "a lone notice edited in place to a new date is still written (the edit hash's job)")
def _():
    fresh(BASE)
    sweep([msg(40, NA)])
    WRITES.clear()
    r = sweep([msg(40, NA.replace("09-25", "09-27"), edited=True)])
    check(written() == [("recPP", "2026-09-27 10:00", "2026-09-27 12:00")], str(written()))
    check(scope()["owner"]["start"].startswith("2026-09-27T10:00"), str(scope()["owner"]))
    check(actions(r) == ["filled"], str(actions(r)))


@case("F41", "an older edit after a newer NON-reschedule notice is carded, never written")
def _():
    fresh(BASE)
    sweep([msg(40, NA), msg(41, "Scheduled maintenance on 2026-09-25 14:00 - 16:00 (GMT+8).")])
    WRITES.clear(); CARDS.clear()
    r = sweep([msg(40, NA.replace("Partners", "Partner"), edited=True),
               msg(41, "Scheduled maintenance on 2026-09-25 14:00 - 16:00 (GMT+8).")])
    check(not WRITES, str(written()))
    check(actions(r)[0] == "needs-human" and len(CARDS) == 1, f"{actions(r)} {titles()}")
    body = CARDS[0][1]["body"]["elements"][0]["text"]["content"]
    check("Older than the message that last set this row" in body, body)


# ---------------------------------------------------------------------------
# F34 — a retry of an older failed write landed after the newer one was final
# ---------------------------------------------------------------------------

def _flaky_first_write():
    calls = []

    def upd(rid, fields):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("Lark 502 (simulated)")
        WRITES.append((rid, dict(fields)))
        return {"code": 0}
    return upd


@case("F34", "older write fails, newer reschedule succeeds: the retry is superseded, row keeps 26")
def _():
    fresh(BASE)
    real = va.update_row
    va.update_row = _flaky_first_write()
    try:
        sweep([msg(10, NA), msg(11, NB)])
        r = sweep([msg(10, NA), msg(11, NB)])
    finally:
        va.update_row = real
    check(written() == [("recPP", "2026-09-26 10:00", "2026-09-26 12:00")], str(written()))
    check(actions(r) == ["superseded", "already handled"], str(actions(r)))
    check(va._seen(va._load(), va._key(msg(10, NA), PPG, PP)), "retry must now be final")


@case("F34", "older retry after a newer correction (no reschedule verb) is carded, not written")
def _():
    fresh(BASE)
    real = va.update_row
    va.update_row = _flaky_first_write()
    newer = "Correction: scheduled maintenance on 2026-09-25 14:00 - 16:00 (GMT+8)."
    try:
        sweep([msg(10, NA), msg(11, newer)])
        CARDS.clear()
        r = sweep([msg(10, NA), msg(11, newer)])
    finally:
        va.update_row = real
    check(row_now() == ("recPP", "2026-09-25 14:00", "2026-09-25 16:00"), str(written()))
    check(actions(r)[0] == "needs-human" and len(CARDS) == 1, f"{actions(r)} {titles()}")


NB_ASK = "Update: the maintenance on 25 Sep may be postponed, the new time will be announced."
# A needs-human that does NOT retract the notice above it: an ambiguous zone. It
# reaches _newer_ask on the retry, where NB_ASK is now caught earlier as a
# same-read retraction.
NB_ZONE = "Scheduled maintenance on 2026-09-25 10:00 - 12:00 (server time)."


@case("F34", "older retry after a newer message was carded to a HUMAN: carded, never written")
def _():
    # The human-fix variant: A's write fails for the whole sweep, the newer B is
    # a needs-human card, a person fixes the row by hand from it - and A's
    # retry used to write A's window over that fix (owner was None: B wrote
    # nothing, so the owner rule never fired). B here is a needs-human that
    # does NOT retract A (an ambiguous zone), so it is the ledgered card, not
    # the same-read retraction rule below, that stops the retry.
    fresh(BASE)
    real = va.update_row

    def down(rid, fields):
        raise RuntimeError("Lark 502 (simulated)")
    va.update_row = down
    try:
        r = sweep([msg(10, NA), msg(11, NB_ZONE)])
    finally:
        va.update_row = real
    check(actions(r) == ["filled", "needs-human"] and not WRITES, f"setup: {actions(r)}")
    CARDS.clear()
    r = sweep([msg(10, NA), msg(11, NB_ZONE)])
    check(not WRITES, f"A's retry overwrote the hand fix: {written()}")
    check(actions(r) == ["needs-human", "already handled"], str(actions(r)))
    body = CARDS[0][1]["body"]["elements"][0]["text"]["content"]
    check("Older than message 11, which was already sent to a human" in body, body)
    r = sweep([msg(10, NA), msg(11, NB_ZONE)])
    check(actions(r) == ["already handled"] * 2 and len(CARDS) == 1,
          f"repeated: {actions(r)} {titles()}")


@case("F34", "a newer 'may be postponed' in the SAME read: the notice is carded, never written")
def _():
    # Messages run oldest first, so A used to be written and only then was B
    # carded - a window the next bubble takes back sat on the sheet under a
    # confident card. Now the fill sees B before writing.
    fresh(BASE)
    r = sweep([msg(10, NA), msg(11, NB_ASK)])
    check(not WRITES, f"written despite the newer postponement: {written()}")
    check(actions(r) == ["needs-human", "needs-human"], str(actions(r)))
    body = CARDS[0][1]["body"]["elements"][0]["text"]["content"]
    check("A NEWER message (11) WITHDREW or CORRECTED the maintenance" in body, body)
    r = sweep([msg(10, NA), msg(11, NB_ASK)])
    check(actions(r) == ["already handled"] * 2 and not WRITES, str(actions(r)))


@case("F34", "write fails, then a CANCELLATION in the same read: the retry is carded, never written")
def _():
    # Tick 1: A's PUT fails, B cancels while the row is still empty - nothing
    # to card against, so B used to end as a final plain 'ignored'. Tick 2: A's
    # retry then wrote the cancelled window with a 'Scheduled maintenance'
    # card, and no card ever mentioned the cancellation.
    for cancel in ("The scheduled maintenance on 2026-09-25 has been cancelled.",
                   "Update: maintenance cancelled.",
                   "Please be informed that the maintenance will not proceed."):
        fresh(BASE)
        real = va.update_row

        def down(rid, fields):
            raise RuntimeError("Lark 502 (simulated)")
        va.update_row = down
        try:
            r1 = sweep([msg(9, "hi"), msg(10, NA), msg(11, cancel)])
        finally:
            va.update_row = real
        # The fill sees the cancellation in the same read before it writes: A
        # is carded in tick 1 and never retried; B is final as 'cancelled'.
        check(actions(r1) == ["ignored", "needs-human", "cancelled"] and not WRITES,
              f"{cancel!r}: tick 1 {actions(r1)} {written()}")
        body = CARDS[-1][1]["body"]["elements"][0]["text"]["content"]
        check("CANCELLED the maintenance" in body, body)
        r2 = sweep([msg(9, "hi"), msg(10, NA), msg(11, cancel)])
        check(not WRITES, f"{cancel!r}: the cancelled window was written: {written()}")
        check(actions(r2) == ["already handled"] * 3 and len(CARDS) == 1,
              f"{cancel!r}: tick 2 {actions(r2)} {titles()}")


@case("F35", "a cancellation read first, the notice rendering later: never written")
def _():
    # Lazy render: the newest bubble (B, a cancellation) is read on its own,
    # and the notice A above it only in the next, fuller read. A is older than
    # a final cancellation and must not go on the sheet.
    for cancel in ("The scheduled maintenance on 2026-09-25 has been cancelled.",
                   "原定9月25日的维护已取消"):
        fresh(BASE)
        sweep([msg(5, "hi")])
        r1 = sweep([msg(11, cancel)])
        check(actions(r1) == ["cancelled"], f"{cancel!r}: {actions(r1)}")
        r2 = sweep([msg(5, "hi"), msg(10, NA), msg(11, cancel)])
        check(not WRITES, f"{cancel!r}: late notice written after its cancellation: {written()}")
        check(actions(r2)[1] == "needs-human", f"{cancel!r}: {actions(r2)}")


@case("F51", "'withdrawn' / 'aborted' / 'no longer required' cancel the row's window: carded")
def _():
    # The parser gave these its cancelled verdict, but _CANCEL_TEXT_RE knew none
    # of the words, so they were dropped silently while the row kept the window.
    for text in ("The maintenance notice above has been withdrawn.",
                 "The scheduled maintenance has been aborted.",
                 "The maintenance on 25/09 is no longer required.",
                 "9月25日的维护已撤回"):
        fresh(BASE)
        sweep([msg(9, "hi")])
        sweep([msg(9, "hi"), msg(10, NA)])
        CARDS.clear()
        r = sweep([msg(9, "hi"), msg(10, NA), msg(11, text)])
        check(actions(r)[-1] == "needs-human" and len(CARDS) == 1,
              f"{text!r}: {actions(r)} {titles()}")
        check("CANCELLED" in CARDS[0][1]["body"]["elements"][0]["text"]["content"],
              f"{text!r}: card does not say cancelled")
        check(written() == [("recPP", "2026-09-25 10:00", "2026-09-25 12:00")],
              f"{text!r}: row changed: {written()}")


@case("F56", "/vacheck force stops at a retraction instead of re-writing the withdrawn window")
def _():
    # The G2.3 follow-up rule was gated `not force`: the walk met "Please
    # disregard..." first, called it chat, walked on and re-wrote the notice a
    # person had just blanked. Two histories: the timer wrote A (owner set),
    # or A and its retraction arrived in one read (A carded, no owner).
    for retract in ("Please disregard the above notice, sent by mistake.",
                    "Kindly ignore the previous message.",
                    "请忽略上一条消息，发错群了",
                    "Correction: the date should be 26/09"):
        # (a) owner set by the timer, then the retraction, then force.
        fresh(BASE)
        sweep([msg(9, "hi")])
        sweep([msg(9, "hi"), msg(10, NA)])
        r = sweep([msg(9, "hi"), msg(10, NA), msg(11, retract)])
        check(actions(r)[-1] == "needs-human", f"{retract!r}: timer {actions(r)}")
        n_writes = len(WRITES)
        rf = sweep([msg(9, "hi"), msg(10, NA), msg(11, retract)], force=True)
        check(len(WRITES) == n_writes and rf["force_stop"],
              f"{retract!r}: force re-wrote: {written()} stop={rf['force_stop']!r}")
        # (b) both in one read: A is carded, never written; force must agree.
        fresh(BASE)
        sweep([msg(9, "hi")])
        r = sweep([msg(9, "hi"), msg(10, NA), msg(11, retract)])
        check(not WRITES and "needs-human" in actions(r),
              f"{retract!r}: same-read {actions(r)} {written()}")
        rf = sweep([msg(9, "hi"), msg(10, NA), msg(11, retract)], force=True)
        check(not WRITES and rf["force_stop"],
              f"{retract!r}: force wrote the withdrawn window: {written()}")


@case("F68", "after a force WRITE, an older needs-human is left for the next tick, not 'superseded'")
def _():
    fresh(BASE)
    sweep([msg(1, "hi")])
    pend = "Maintenance for Game A is postponed, new schedule to be announced."
    nb = "Scheduled maintenance for Game B on 2026-09-27 02:00 - 04:00 (GMT+8)."
    rf = sweep([msg(1, "hi"), msg(2, pend), msg(3, nb)], force=True)
    check(written() == [("recPP", "2026-09-27 02:00", "2026-09-27 04:00")], str(written()))
    check("superseded" not in actions(rf), f"force buried the needs-human: {actions(rf)}")
    CARDS.clear()
    r = sweep([msg(1, "hi"), msg(2, pend), msg(3, nb)])
    check([x for x in r["details"] if x["key"].endswith("mid:2")][0]["action"] == "needs-human"
          and len(CARDS) == 1, f"next tick: {actions(r)} {titles()}")


@case("F36", "force on two SEPARATE outages: the older imminent one is carded, not buried")
def _():
    fresh(BASE)
    sweep([msg(9, "hello")])
    a = "Scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8)."
    b = "Scheduled maintenance on 2026-10-01 10:00 - 12:00 (GMT+8)."
    rf = sweep([msg(9, "hello"), msg(10, a), msg(11, b)], force=True)
    check(written() == [("recPP", "2026-10-01 10:00", "2026-10-01 12:00")], str(written()))
    check("superseded" not in actions(rf), f"09-25 buried as superseded: {actions(rf)}")
    CARDS.clear()
    r = sweep([msg(9, "hello"), msg(10, a), msg(11, b)])
    body = CARDS[0][1]["body"]["elements"][0]["text"]["content"] if CARDS else ""
    check(actions(r)[1] == "needs-human" and "2026-09-25 10:00" in body,
          f"next tick: {actions(r)} {body[:120]}")
    check(len(WRITES) == 1, f"the older window was written after all: {written()}")
    # ...while a RESCHEDULE of the older notice is still superseded at once.
    fresh(BASE)
    sweep([msg(9, "hello")])
    rf = sweep([msg(9, "hello"), msg(10, NA), msg(11, NB)], force=True)
    check("superseded" in actions(rf), f"reschedule no longer supersedes: {actions(rf)}")


@case("G2.3", "a correction posted after the notice scrolled off the read is still carded")
def _():
    # Busy group: tick 1 [A], tick 2 [A + 7 tickets], tick 3 [tickets 3-9 + R]
    # with A no longer on screen. `own_n in known` failed and R was final,
    # uncarded, while the row kept 24/09.
    a = "Scheduled maintenance on 24/09/2026 10:00 - 12:00 (GMT+8)."
    tickets = [msg(101 + k, f"ticket #{k}: player 88{k} cannot log in") for k in range(9)]
    for corr in ("Correction: the maintenance date should be 25/09 instead of 24/09.",
                 "Kindly disregard the previous maintenance announcement.",
                 "更正：维护日期应为9月25日，而不是9月24日。",
                 "请无视之前的维护公告。"):
        fresh(BASE)
        sweep([msg(90, "hi")])
        sweep([msg(90, "hi"), msg(100, a)])
        sweep([msg(100, a)] + tickets[:7])
        CARDS.clear()
        r = sweep(tickets[2:] + [msg(120, corr)])
        check(actions(r)[-1] == "needs-human" and len(CARDS) == 1,
              f"{corr!r}: {actions(r)} {titles()}")
        body = CARDS[0][1]["body"]["elements"][0]["text"]["content"]
        check("2026-09-24 10:00" in body and "RETRACT or CORRECT" in body, body[:200])
        check(written() == [("recPP", "2026-09-24 10:00", "2026-09-24 12:00")], str(written()))


@case("G2.5", "an edit undone back to the original text is re-applied, not 'text unchanged'")
def _():
    # A (10:00) filled, edited to 14:00 (written), edited BACK: the hash matched
    # the FIRST version and the row kept 14:00 for good with no card.
    a = "Scheduled maintenance notice: 24/09/2026 10:00-12:00 (GMT+8). All games unavailable."
    a2 = "Scheduled maintenance notice: 24/09/2026 14:00-16:00 (GMT+8). All games unavailable."
    fresh(BASE)
    sweep([msg(99, "hi")])
    sweep([msg(99, "hi"), msg(100, a)])
    sweep([msg(99, "hi"), msg(100, a2, edited=True)])
    check(written() == [("recPP", "2026-09-24 10:00", "2026-09-24 12:00"),
                        ("recPP", "2026-09-24 14:00", "2026-09-24 16:00")], str(written()))
    r = sweep([msg(99, "hi"), msg(100, a, edited=True)])
    check(actions(r)[-1] != "already handled", f"edit-back read as unchanged: {actions(r)}")
    check(written()[-1] == ("recPP", "2026-09-24 10:00", "2026-09-24 12:00")
          or actions(r)[-1] == "needs-human", f"row left on 14:00: {written()} {actions(r)}")
    # A plain strikethrough of the CURRENT text is still unchanged.
    r = sweep([msg(99, "hi"), msg(100, a, edited=True)])
    check(actions(r)[-1] == "already handled", str(actions(r)))


@case("F58", "a CS reply 'no maintenance on our side, please check your network' is not a clear")
def _():
    # The row holds a live window; these are ticket answers, not schedule
    # answers. Each was a red card, and with VAWATCH_CLEAR_ENABLED=1 on an
    # empty row would have blanked it.
    for text in ("Hi, there is no maintenance on our side, please check your network.",
                 "No maintenance on our side, the game server is running normally.",
                 "There is no maintenance issue on our side, please check your network.",
                 "No maintenance was done on the server, the issue is on the operator side.",
                 "No maintenance ongoing, please retry.",
                 "我们这边没有维护，请检查网络"):
        fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
        sweep([msg(9, "hi")])
        sweep([msg(9, "hi"), msg(10, NA)])
        CARDS.clear()
        r = sweep([msg(9, "hi"), msg(10, NA), msg(11, text)])
        check(actions(r)[-1] == "ignored" and not CARDS, f"{text!r}: {actions(r)} {titles()}")
        check(written() == [("recPP", "2026-09-25 10:00", "2026-09-25 12:00")],
              f"{text!r}: row changed: {written()}")
    # ...while the weekly answer "No maintenance on our side this week" still clears.
    fresh(BASE)
    sweep([msg(9, "hi")])
    r = sweep([msg(9, "hi"), msg(11, "No maintenance on our side this week.")])
    check(actions(r)[-1] in ("clear-skipped", "cleared"), f"weekly answer: {actions(r)}")


@case("F59", "a notice split over two bubbles is carded (with the joined window) at the reparse cap")
def _():
    for head, detail in (("【Maintenance Notice】",
                          "Date: 2026-09-26\nTime: 02:00 - 04:00 (GMT+8)\nAffected: all games"),
                         ("Dear partners, please be informed of the following maintenance:",
                          "2026-09-26 02:00 - 04:00 (GMT+8)")):
        fresh(BASE)
        sweep([msg(9, "hi")])
        runs = [actions(sweep([msg(9, "hi"), msg(10, head), msg(11, detail)]))
                for _ in range(4)]
        check(not WRITES, f"{head!r}: written from a joined pair: {written()}")
        check(any("needs-human" in a for a in runs) and len(CARDS) == 1,
              f"{head!r}: {runs} {titles()}")
        body = CARDS[0][1]["body"]["elements"][0]["text"]["content"]
        check("2026-09-26 02:00" in body and "split over two messages" in body, body[:200])
    # text-only pointer with no media in the read: carded at the cap too.
    fresh(BASE)
    sweep([msg(9, "hi")])
    runs = [actions(sweep([msg(9, "hi"), msg(10, "Maintenance notice: please see attached.")]))
            for _ in range(4)]
    check(len(CARDS) == 1 and "points at an attachment" in
          CARDS[0][1]["body"]["elements"][0]["text"]["content"], f"{runs} {titles()}")
    # ...but a poster carded by the F59 poster rule is not carded a second
    # time through the text beside it, and an album is one card.
    for msgs_ in ([msg(10, "", kind="media-photo"), msg(11, "Maintenance notice, see image above")],
                  [msg(10, "Dear partners, maintenance details as below", kind="media-photo"),
                   msg(11, "", kind="media-photo")]):
        fresh(BASE)
        sweep([msg(9, "hi")])
        for _ in range(4):
            sweep([msg(9, "hi")] + msgs_)
        check(len(CARDS) == 1 and not WRITES, f"{msgs_[0]['text'][:30]!r}: {titles()}")


@case("F40", "a capped ledger that kept none of a baselined group's keys: one careful sweep, no revival")
def _():
    # bd9fc12's first-insertion trim evicted every key of a quiet group; with
    # no evicted mark the first sweep after deploy re-wrote and re-carded the
    # decided notices still on its screen.
    fresh(BASE)
    va.mark_baselined("JILI CS", "JILI")
    d = va._load()
    for i in range(va._LEDGER_CAP):
        k = f"busy0@busy0 cs|mid:{10000 + i}"
        d["handled"][k] = {"outcome": "ignored", "at": "2026-09-20 10:00:00", "attempts": 1}
        d["order"].append(k)
    d.setdefault("baselined", {})["busy0@busy0 cs"] = "2026-08-01 10:00:00"
    with va._ledger_lock:
        check(va._save(d), "setup save")
    screen = [msg(500, "Scheduled maintenance on 2026-09-28 02:00 - 04:00 (GMT+8)."),
              msg(501, "Maintenance for Game B is rescheduled; we will confirm the new date later.")]
    r = sweep(screen, provider="JILI", group="JILI CS")
    check(r["cold_start"] and not WRITES and not CARDS,
          f"revived: {actions(r)} {written()} {titles()}")
    # A NEW message after that sweep is handled normally.
    r = sweep(screen + [msg(502, "Scheduled maintenance on 2026-09-29 02:00 - 04:00 (GMT+8).")],
              provider="JILI", group="JILI CS")
    check(written() == [("recJILI", "2026-09-29 02:00", "2026-09-29 04:00")],
          f"after the careful sweep: {actions(r)} {written()}")


@case("R1.79", "a pointer AT an attachment (详见附件 on a document, 'see below' over a poster) is carded")
def _():
    fresh(BASE)
    sweep([msg(100, "hello")])
    r = sweep([msg(100, "hello"), msg(101, "详见附件", kind="document")])
    check(actions(r)[-1] == "needs-human" and len(CARDS) == 1
          and "points at an attachment" in CARDS[0][1]["body"]["elements"][0]["text"]["content"],
          f"document pointer: {actions(r)} {titles()}")
    fresh(BASE)
    sweep([msg(100, "hello")])
    r = sweep([msg(100, "hello"), msg(101, "Please see below"), msg(102, "", kind="photo")])
    check("needs-human" in actions(r) and len(CARDS) == 1, f"see-below + poster: {actions(r)} {titles()}")
    check(not WRITES, str(written()))
    # ...but a pointer with nothing attached beside it stays chat (F58).
    fresh(BASE)
    sweep([msg(100, "hello")])
    r = sweep([msg(100, "hello"), msg(101, "", kind="photo"),
               msg(102, "Please check the screenshot above")])
    check(not CARDS, f"ticket screenshot carded: {titles()} {actions(r)}")


@case("F34", "a retry with NO newer human card or newer write still lands (the retry's job)")
def _():
    fresh(BASE)
    real = va.update_row
    va.update_row = _flaky_first_write()
    try:
        sweep([msg(10, NA), msg(11, "any update on the round id?")])
        r = sweep([msg(10, NA), msg(11, "any update on the round id?")])
    finally:
        va.update_row = real
    check(written() == [("recPP", "2026-09-25 10:00", "2026-09-25 12:00")], str(written()))
    check(actions(r)[0] == "filled", str(actions(r)))


# ---------------------------------------------------------------------------
# F35 — an older bubble first seen after the newer notice was applied
# ---------------------------------------------------------------------------

@case("F35", "lazy render: the older notice seen late is superseded, row keeps the reschedule")
def _():
    fresh(BASE)
    sweep([msg(20, "chat"), msg(21, NB)])
    r = sweep([msg(10, NA)] + [msg(11 + k, f"chat {k}") for k in range(8)] + [msg(21, NB)])
    check(written() == [("recPP", "2026-09-26 10:00", "2026-09-26 12:00")], str(written()))
    check(actions(r)[0] == "superseded", str(actions(r)))


@case("F35", "a cold read that saw only the newest bubbles: older ones seen later are backlog")
def _():
    fresh(BASE)
    va.handle_messages([msg(21, NB)], provider=PP, group=PPG, record_id="recPP")
    r = va.handle_messages([msg(10, NA), msg(21, NB)], provider=PP, group=PPG,
                           record_id="recPP")
    check(actions(r) == ["history", "already handled"], str(actions(r)))
    check(not WRITES and not CARDS, f"{written()} {titles()}")


@case("F35", "VAWATCH_READ_COUNT raised 8 -> 20: the newly visible history is not acted on")
def _():
    fresh(BASE)
    hist = [msg(k, NA if k == 3 else f"chat {k}") for k in range(1, 21)]
    va.handle_messages(hist[-8:], provider=PP, group=PPG, record_id="recPP")   # cold, 8
    r = va.handle_messages(hist, provider=PP, group=PPG, record_id="recPP")    # 20
    check(not WRITES and not CARDS, f"{written()} {titles()}")
    check(r["history"] == 12, f"history={r['history']} {actions(r)}")


@case("F35", "an older notice rendered AFTER a newer needs-human card: carded, never written")
def _():
    fresh(BASE)
    sweep([msg(5, "hi")])
    sweep([msg(5, "hi"), msg(11, NB_ASK)])           # A (mid 10) not rendered yet
    check(titles() == ["❓ Maintenance notice needs a human · Pragmatic Play"], str(titles()))
    CARDS.clear()
    r = sweep([msg(5, "hi"), msg(10, NA), msg(11, NB_ASK)])
    check(not WRITES, f"late A written after B was sent to a human: {written()}")
    check(actions(r) == ["already handled", "needs-human", "already handled"], str(actions(r)))
    # VAWATCH_ORDER_CHECK=0 switches this rule off with every other order rule.
    fresh(BASE, env={"VAWATCH_ORDER_CHECK": "0"})
    sweep([msg(5, "hi")])
    sweep([msg(5, "hi"), msg(11, NB_ASK)])
    sweep([msg(5, "hi"), msg(10, NA), msg(11, NB_ASK)])
    check(written() == [("recPP", "2026-09-25 10:00", "2026-09-25 12:00")],
          f"kill-switch: {written()}")


# ---------------------------------------------------------------------------
# F36 — /vacheck force left the bubbles older than its write unmarked
# ---------------------------------------------------------------------------

@case("F36", "cold group: force writes the newest, the rest is backlog; the next tick does nothing")
def _():
    fresh(BASE)
    va.handle_messages([msg(10, NA), msg(11, NB)], provider=PP, group=PPG,
                       record_id="recPP", force=True)
    r = va.handle_messages([msg(10, NA), msg(11, NB)], provider=PP, group=PPG,
                           record_id="recPP")
    check(written() == [("recPP", "2026-09-26 10:00", "2026-09-26 12:00")], str(written()))
    check(actions(r) == ["already handled"] * 2 and len(CARDS) == 1, f"{actions(r)} {titles()}")


@case("F36", "warm group: bubbles older than force's write are superseded, not written next tick")
def _():
    fresh(BASE)
    sweep([msg(9, "hello")])
    r = sweep([msg(9, "hello"), msg(10, NA), msg(11, NB)], force=True)
    check("superseded" in actions(r), str(actions(r)))
    r = sweep([msg(9, "hello"), msg(10, NA), msg(11, NB)])
    check(actions(r) == ["already handled"] * 3, str(actions(r)))
    check(written() == [("recPP", "2026-09-26 10:00", "2026-09-26 12:00")], str(written()))


@case("F36", "cold group: an older needs-human force walked past is NOT carded next tick")
def _():
    fresh(BASE)
    older = msg(10, "Maintenance for Game A is postponed, new schedule to be announced.")
    va.handle_messages([older, msg(11, NB)], provider=PP, group=PPG, record_id="recPP",
                       force=True)
    CARDS.clear()
    va.handle_messages([older, msg(11, NB)], provider=PP, group=PPG, record_id="recPP")
    check(not CARDS, f"backlog carded after a cold force: {titles()}")


# ---------------------------------------------------------------------------
# F76 — one failed ledger read wiped the baselines, cursor and target cache
# ---------------------------------------------------------------------------

class _FailFirstRead:
    """open() of the ledger for READING fails once with `err`."""

    def __init__(self, err, every=False):
        self.err, self.armed, self.every = err, True, every

    def __enter__(self):
        import builtins
        self.b, self.real = builtins, builtins.open

        def op(path, *a, **k):
            if self.armed and str(path) == str(va.LEDGER_PATH) and (not a or "r" in a[0]):
                self.armed = self.every
                raise self.err
            return self.real(path, *a, **k)
        builtins.open = op
        return self

    def __exit__(self, *exc):
        self.b.open = self.real


@case("F76", "EMFILE on the sweep's read: LedgerError before acting, ledger left intact")
def _():
    fresh(BASE)
    sweep([msg(1, "hello")])
    va.next_target()
    before = va._load()
    with _FailFirstRead(OSError(24, "Too many open files")):
        try:
            va.handle_messages([msg(1, "hello"), msg(2, "Scheduled maintenance on 2026-09-27 "
                                                    "02:00 - 04:00 (GMT+8).")],
                               provider=PP, group=PPG, record_id="recPP", shared_with=[])
            raise AssertionError("no LedgerError")
        except va.LedgerError as err:
            check("could not be read" in str(err), str(err))
    after = va._load()
    check(not WRITES and not CARDS, f"{written()} {titles()}")
    for k in ("baselined", "cursor_pair", "targets"):
        check(after.get(k) == before.get(k), f"{k} changed: {before.get(k)} -> {after.get(k)}")


@case("F76", "an unreadable ledger is never saved over by next_target / caches / baselines")
def _():
    fresh(BASE)
    sweep([msg(1, "hello")])
    before = va.LEDGER_PATH.read_text(encoding="utf-8")
    for fn in (va.next_target, lambda: va.mark_baselined("X", "Y"),
               lambda: va._bump_empty_reads("X", "Y"),
               lambda: va._remember_targets({"telegram": [{"provider": "Z", "group": "Z"}]})):
        with _FailFirstRead(OSError(5, "Input/output error"), every=True):
            fn()
        check(va.LEDGER_PATH.read_text(encoding="utf-8") == before, f"{fn} overwrote it")


@case("F76", "a corrupt ledger is kept as vawatch.json.bad; the group re-baselines silently")
def _():
    fresh(BASE)
    sweep([msg(1, "hello")])
    va.LEDGER_PATH.write_text("{not json", encoding="utf-8")
    r = va.handle_messages([msg(2, "Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8).")],
                           provider=PP, group=PPG, record_id="recPP")
    bad = va.LEDGER_PATH.with_name(va.LEDGER_PATH.name + ".bad")
    check(bad.exists() and bad.read_text(encoding="utf-8") == "{not json", "evidence not kept")
    check(r["cold_start"] and not WRITES and not CARDS, f"{r['cold_start']} {written()}")


@case("F76", "R1.47: a ledger saved as GBK is moved aside once, not a LedgerError every tick")
def _():
    # A text-mode read raised UnicodeDecodeError inside the READ, so a hand
    # edit saved as GBK counted as transient: every tick raised LedgerError and
    # the watcher stopped acting until somebody deleted the file.
    import json
    fresh(BASE)
    sweep([msg(1, "hello")])
    raw = '{"handled": {}, "note": "维护"}'.encode("gbk")
    va.LEDGER_PATH.write_bytes(raw)
    bad = va.LEDGER_PATH.with_name(va.LEDGER_PATH.name + ".bad")
    for i in (2, 3):              # the same bubble twice: tick 3 must not re-reset
        r = va.handle_messages([msg(2, "Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8).")],
                               provider=PP, group=PPG, record_id="recPP")
        check(r["cold_start"] if i == 2 else not r["cold_start"], f"tick {i}: {r['cold_start']}")
    check(bad.exists() and bad.read_bytes() == raw, "GBK evidence not kept as .bad")
    check(json.loads(va.LEDGER_PATH.read_text(encoding="utf-8")).get("handled") is not None,
          "no fresh UTF-8 ledger was written")
    check(not WRITES and not CARDS, f"re-baseline wrote {written()}")


@case("F76", "the worker survives a next_target() that raises, and counts it")
def _():
    fresh(BASE)
    real = va.next_target
    va.next_target = lambda: (_ for _ in ()).throw(va.LedgerError("simulated"))
    try:
        w = worker([msg(1, "hello")])
        box: dict = {}
        w._handle_va_watch({"kind": "va_watch", "box": box})
    finally:
        va.next_target = real
    check(not box.get("ok") and "could not pick" in box.get("error", ""), str(box))
    check(w.failures and w.failures[0][0] == tw._TelegramWarm._VA_LEDGER_STREAK, str(w.failures))


# ---------------------------------------------------------------------------
# F77 — an unwritable ledger re-filled and re-carded every tick, rotation frozen
# ---------------------------------------------------------------------------

class _NoSpace:
    """Every write under the ledger's directory fails with ENOSPC."""

    def __enter__(self):
        import builtins
        self.b, self.real = builtins, builtins.open

        def op(path, *a, **k):
            if str(path).startswith(str(va.LEDGER_PATH.parent)) and a and "w" in a[0]:
                raise OSError(28, "No space left on device")
            return self.real(path, *a, **k)
        builtins.open = op
        return self

    def __exit__(self, *exc):
        self.b.open = self.real


@case("F77", "ENOSPC: every tick raises LedgerError before acting - zero writes, zero cards")
def _():
    fresh(BASE)
    va.mark_baselined(PPG, PP)
    with _NoSpace():
        for _ in range(3):
            try:
                va.handle_messages([msg(1, "Scheduled maintenance on 2026-09-27 02:00 - "
                                           "04:00 (GMT+8).")], provider=PP, group=PPG,
                                   record_id="recPP")
                raise AssertionError("acted on an unsaveable ledger")
            except va.LedgerError as err:
                check("cannot be saved" in str(err), str(err))
    check(not WRITES and not CARDS, f"{written()} {titles()}")
    r = va.handle_messages([msg(1, "Scheduled maintenance on 2026-09-27 02:00 - 04:00 "
                                   "(GMT+8).")], provider=PP, group=PPG, record_id="recPP")
    check(len(WRITES) == 1 and r["acted"] == 1, "space back: the notice is acted on once")


@case("F77", "ENOSPC: the rotation still advances (cursor kept in memory)")
def _():
    fresh(BASE)
    with _NoSpace():
        seen = [va.next_target().get("provider") for _ in range(4)]
    check(len(set(seen)) == 4, f"rotation froze: {seen}")
    after = va.next_target().get("provider")      # disk back: carries on from memory
    check(after not in seen[:3], f"rotation restarted: {seen} then {after}")
    check(va._CURSOR_MEM is None, "a good save must clear the in-memory cursor")


@case("F77", "a save that fails AFTER a write stops the sweep and reports it")
def _():
    fresh(BASE)
    va.mark_baselined(PPG, PP)
    real_save = va._save
    state = {"n": 0}

    def save_once(d):
        state["n"] += 1
        return real_save(d) if state["n"] == 1 else False   # probe ok, then the disk fills
    va._save = save_once
    try:
        r = va.handle_messages([msg(1, "Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8)."),
                                msg(2, "Scheduled maintenance on 2026-09-28 02:00 - 04:00 (GMT+8).")],
                               provider=PP, group=PPG, record_id="recPP")
    finally:
        va._save = real_save
    check(len(WRITES) == 1, f"kept acting on an unsaveable ledger: {written()}")
    check(r["ledger_error"], str(r))


@case("F77", "the worker counts a ledger fault as ONE watcher-wide streak")
def _():
    fresh(BASE)
    va.mark_baselined(PPG, PP)
    w = worker([msg(1, "Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8).")])
    with _NoSpace():
        box: dict = {}
        w._handle_va_watch({"kind": "va_watch", "box": box, "target": va.manual_target(PP)})
    check(not box.get("ok") and "LedgerError" in box.get("error", ""), str(box))
    check([t for t, _ in w.failures] == [tw._TelegramWarm._VA_LEDGER_STREAK], str(w.failures))
    check(not WRITES and not CARDS, f"{written()} {titles()}")


# ---------------------------------------------------------------------------
# F82 — one exception in classify aborted the sweep before the ledger was saved
# ---------------------------------------------------------------------------

F82_BAD = "Test\n9999-12-31 22:00 - 23:00 (GMT-5)\n9999-12-31 22:00 - 23:00 (GMT-5)"


@case("F82", "a raising bubble is marked 'error'; the rest of the read is handled and saved")
def _():
    fresh(BASE)
    real = va.classify

    def boom(text, *a, **k):
        if "BOOM" in text:
            raise OverflowError("date value out of range (simulated)")
        return real(text)
    va.classify = boom
    m = [msg(1, "Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8)."),
         msg(2, "BOOM"), msg(3, "Scheduled maintenance on 2026-09-28 02:00 - 04:00 (GMT+8).")]
    try:
        r1 = sweep(m)
        r2 = sweep(m)
        r3 = sweep(m)
        r4 = sweep(m)
    finally:
        va.classify = real
    check(actions(r1) == ["filled", "error", "filled"], str(actions(r1)))
    check(len(WRITES) == 2 and len(CARDS) == 2, f"re-acted: {written()} {titles()}")
    check(actions(r2)[0] == "already handled" and actions(r2)[2] == "already handled",
          str(actions(r2)))
    check(actions(r4) == ["already handled"] * 3, f"error not capped: {actions(r4)}")
    check(r1["errors"] == 1 and r3["errors"] == 1, f"{r1['errors']} {r3['errors']}")


@case("F82", "the real 9999-12-31 trigger never escapes handle_messages")
def _():
    fresh(BASE)
    r = sweep([msg(1, "Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8)."),
               msg(2, F82_BAD)])
    check(len(WRITES) == 1 and actions(r)[0] == "filled", f"{actions(r)} {written()}")
    r = sweep([msg(1, "Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8)."),
               msg(2, F82_BAD)])
    check(len(WRITES) == 1, f"re-written: {written()}")


@case("F82", "a detector exception in the worker is counted in the group's failure streak")
def _():
    fresh(BASE)
    va.mark_baselined(PPG, PP)
    real = va.handle_messages
    va.handle_messages = lambda *a, **k: (_ for _ in ()).throw(ValueError("simulated"))
    try:
        w = worker([msg(1, "hello")])
        box: dict = {}
        w._handle_va_watch({"kind": "va_watch", "box": box, "target": va.manual_target(PP)})
    finally:
        va.handle_messages = real
    check("detector failed" in box.get("error", ""), str(box))
    check([t for t, _ in w.failures] == [PPG], str(w.failures))


# ---------------------------------------------------------------------------
# G2.4 — a deleted notice was never noticed
# ---------------------------------------------------------------------------

@case("G2.4", "the row's notice deleted from between two surviving bubbles: ONE card, no write")
def _():
    fresh(BASE)
    sweep([msg(99, "hi"), msg(100, "Scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8).")])
    WRITES.clear(); CARDS.clear()
    tail = [msg(99, "hi"), msg(101, "Sorry wrong group, please ignore the message above"),
            msg(102, "ok")]
    r = sweep(tail)
    check(not WRITES, str(written()))
    check(titles() == ["❓ Maintenance notice deleted · Pragmatic Play"], str(titles()))
    check(r["deleted_notice"] == 1, str(r["details"]))
    sweep(tail + [msg(103, "ok2")])
    check(len(CARDS) == 1, f"carded twice: {titles()}")


@case("G2.4", "scrolled away (older than every bubble read) or reposted: no deleted card")
def _():
    fresh(BASE)
    sweep([msg(100, "Scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8).")])
    CARDS.clear()
    sweep([msg(101, "a"), msg(102, "b")])
    check(not CARDS, f"scrolled away is not deleted: {titles()}")
    fresh(BASE)
    sweep([msg(99, "hi"), msg(100, "Scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8).")])
    CARDS.clear()
    sweep([msg(99, "hi"), msg(101, "Scheduled maintenance on 24/09/2026 14:00-16:00 (GMT+8).")])
    check(titles() == ["🛠️ Scheduled maintenance · Pragmatic Play"], str(titles()))


@case("G2.4", "an explicit cancellation of the row's window is carded; completion stays silent")
def _():
    for text, carded in [
            ("Dear Partner, the scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8) has "
             "been cancelled.", True),
            ("Dear Partner, the maintenance has been cancelled.", True),
            ("维护已取消，请知悉。", True),
            ("Dear Partner, the maintenance on 30/09/2026 10:00-12:00 (GMT+8) has been "
             "cancelled.", False),           # names ANOTHER window: not this row's
            ("Maintenance completed. Thank you.", False)]:
        fresh(BASE)
        sweep([msg(100, "Scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8).")])
        WRITES.clear(); CARDS.clear()
        r = sweep([msg(100, "Scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8)."),
                   msg(101, text)])
        check(not WRITES, f"{text[:30]}: {written()}")
        if carded:
            check(actions(r)[1] == "needs-human" and len(CARDS) == 1
                  and "CANCELLED" in CARDS[0][1]["body"]["elements"][0]["text"]["content"],
                  f"{text[:30]}: {actions(r)} {titles()}")
        else:
            check(not CARDS, f"{text[:30]}: {titles()}")


# ---------------------------------------------------------------------------
# G2.5 — an edit that voids a notice re-wrote and re-announced it
# ---------------------------------------------------------------------------

G25 = "Scheduled maintenance notice: 24/09/2026 10:00-12:00 (GMT+8). Games unavailable."


@case("G2.5", "strikethrough only (innerText unchanged, 'edited' marker): already handled")
def _():
    fresh(BASE)
    sweep([msg(100, G25)])
    WRITES.clear(); CARDS.clear()
    r = sweep([msg(100, G25, edited=True)])
    check(not WRITES and not CARDS and r["already"] == 1, f"{actions(r)} {titles()}")


@case("G2.5", "[VOID] / 【作废】 / （發錯群，請忽略） added by an edit: needs-human, nothing written")
def _():
    for prefix in ("[VOID - wrong group] ", "【作废】", "（發錯群，請忽略）", "[CANCELLED] "):
        fresh(BASE)
        sweep([msg(100, G25)])
        WRITES.clear(); CARDS.clear()
        r = sweep([msg(100, prefix + G25, edited=True)])
        check(not WRITES, f"{prefix}: {written()}")
        check(actions(r) == ["needs-human"] and len(CARDS) == 1
              and CARDS[0][0].startswith("❓"), f"{prefix}: {actions(r)} {titles()}")
        body = CARDS[0][1]["body"]["elements"][0]["text"]["content"]
        check("was EDITED" in body and "still shows" in body, body)


@case("G2.5", "an edit that only fixes a typo on the row's own notice: silent, nothing re-sent")
def _():
    fresh(BASE)
    sweep([msg(100, G25)])
    WRITES.clear(); CARDS.clear()
    r = sweep([msg(100, G25.replace("unavailable", "unavailable."), edited=True)])
    check(not WRITES and not CARDS, f"{written()} {titles()}")
    check(r["details"][0].get("why") == "edited - same window", str(r["details"]))


@case("G2.5", "an older bubble voided by edit after a newer notice: superseded, silent")
def _():
    fresh(BASE)
    newer = "Scheduled maintenance notice: 24/09/2026 14:00-16:00 (GMT+8)."
    sweep([msg(100, G25), msg(101, newer)])
    WRITES.clear(); CARDS.clear()
    r = sweep([msg(100, "[VOID] " + G25, edited=True), msg(101, newer)])
    check(not WRITES and not CARDS and actions(r)[0] == "superseded", f"{actions(r)} {titles()}")


@case("G2.5", "a row filled BEFORE this release (legacy owner, window unknown): [VOID] edit is carded")
def _():
    # _init_scope derives the owner of a pre-existing ledger with no window, so
    # _owner_live() is False and the void edit used to fall through to a fresh
    # "Scheduled maintenance" card until that row's next fill.
    import json as _json
    bk = va._baseline_key(PPG, PP)
    for edit, want in [("[VOID - wrong group] " + G25, "needs-human"),
                       ("（發錯群，請忽略）" + G25, "needs-human"),
                       (G25.replace("24/09", "25/09"), "filled")]:      # a real correction
        fresh(BASE)
        k = f"{bk}|mid:100"
        va.LEDGER_PATH.write_text(_json.dumps({
            "handled": {k: {"outcome": "filled", "at": "2026-09-22 10:00:00", "attempts": 1,
                            "window": "2026-09-24 10:00 → 12:00"}},
            "order": [k], "key_scheme": 2, "baselined": {bk: "2026-09-01 00:00:00"}}),
            encoding="utf-8")
        r = sweep([msg(100, edit, edited=True)], baseline=False)
        check(actions(r) == [want], f"{edit[:24]}: {actions(r)} {titles()}")
        if want == "needs-human":
            body = CARDS[0][1]["body"]["elements"][0]["text"]["content"]
            check(not WRITES and "2026-09-24 10:00 → 12:00" in body and "EDITED" in body, body)
        else:
            check(row_now() == ("recPP", "2026-09-25 10:00", "2026-09-25 12:00"), str(written()))


@case("G2.5", "a FRESH post of a voided notice ('[VOID - wrong group] ...', 【作废】) is carded, never written")
def _():
    # noticeparse used to carve "[VOID - wrong group] <notice>" out of its
    # retraction rule so the edit rule above still saw a fill; a new bubble
    # carrying the same text then wrote the voided window onto the row.
    for prefix in ("[VOID - wrong group] ", "[VOID] ", "【作废】", "（發錯群，請忽略）"):
        fresh(BASE)
        r = sweep([msg(100, prefix + G25)])
        check(not WRITES and actions(r) == ["needs-human"] and len(CARDS) == 1,
              f"{prefix}: {actions(r)} {written()} {titles()}")


# ---------------------------------------------------------------------------
# G3.1 — a later, separate notice silently erased an imminent window
# ---------------------------------------------------------------------------

G31_A = "Scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8)."
G31_B = "Scheduled maintenance on 01/10/2026 10:00-12:00 (GMT+8)."


@case("G3.1", "a later outage is deferred and carded; written once the earlier window has ended")
def _():
    fresh(BASE)
    sweep([msg(1, G31_A)])
    WRITES.clear(); CARDS.clear()
    r = sweep([msg(1, G31_A), msg(2, G31_B)])
    check(not WRITES and actions(r) == ["already handled", "deferred"], f"{actions(r)} {written()}")
    body = CARDS[0][1]["body"]["elements"][0]["text"]["content"]
    check("SECOND, separate outage" in body and "written automatically" in body, body)
    r = sweep([msg(1, G31_A), msg(2, G31_B)])
    check(not WRITES and r["already"] == 2, "nothing moves while 24/09 is ahead")
    with at(datetime(2026, 9, 24, 13, 0, tzinfo=TZ8)):
        r = sweep([msg(1, G31_A), msg(2, G31_B)])
        check(written() == [("recPP", "2026-10-01 10:00", "2026-10-01 12:00")], str(written()))
        check(r["pending_written"] == 1, str(r["details"]))
        card = CARDS[-1][1]["body"]["elements"][0]["text"]["content"]
        check("Deferred window" in card, card)
        sweep([msg(1, G31_A), msg(2, G31_B)])
        check(len(WRITES) == 1, f"written twice: {written()}")


@case("G3.1", "a sooner outage is written, and its card names the later window it replaced")
def _():
    fresh(BASE)
    sweep([msg(1, G31_B)])
    WRITES.clear(); CARDS.clear()
    sweep([msg(1, G31_B), msg(2, G31_A)])
    check(written() == [("recPP", "2026-09-24 10:00", "2026-09-24 12:00")], str(written()))
    body = CARDS[0][1]["body"]["elements"][0]["text"]["content"]
    check("Replaced a LATER window still ahead" in body and "2026-10-01" in body, body)


@case("G3.1", "a reschedule, or a correction within 24h, still overwrites (newest wins)")
def _():
    for newer in ["The maintenance has been rescheduled to 01/10/2026 10:00-12:00 (GMT+8).",
                  "Correction: scheduled maintenance on 24/09/2026 14:00-16:00 (GMT+8)."]:
        fresh(BASE)
        sweep([msg(1, G31_A)])
        WRITES.clear()
        r = sweep([msg(1, G31_A), msg(2, newer)])
        check(len(WRITES) == 1 and actions(r)[1] == "filled", f"{newer[:30]}: {actions(r)}")


@case("G3.1", "a reminder keeps the deferred outage; a newer notice about it replaces it")
def _():
    fresh(BASE)
    upd = "Update: scheduled maintenance on 01/10/2026 14:00-16:00 (GMT+8)."
    sweep([msg(1, G31_A), msg(2, G31_B)])
    sweep([msg(1, G31_A), msg(2, G31_B), msg(3, "Reminder: " + G31_A)])
    check([p["mid"] for p in scope().get("pending") or []] == ["2"],
          f"the reminder must not drop it: {scope().get('pending')}")
    sweep([msg(1, G31_A), msg(2, G31_B), msg(3, "Reminder: " + G31_A), msg(4, upd)])
    check([p["mid"] for p in scope().get("pending") or []] == ["4"],
          f"the newer notice about 01/10 must replace it: {scope().get('pending')}")
    WRITES.clear()
    with at(datetime(2026, 9, 24, 13, 0, tzinfo=TZ8)):
        sweep([msg(1, G31_A), msg(2, G31_B), msg(3, "Reminder: " + G31_A), msg(4, upd)])
    check(written() == [("recPP", "2026-10-01 14:00", "2026-10-01 16:00")], str(written()))


@case("G3.1", "a newer 'no maintenance' never lets the deferred outage be written later")
def _():
    # With the clear on, this used to BLANK the row while G31_A (24/09) was
    # still ahead, which is how the deferred list got emptied. Since G3.2 a
    # window that has not ended is never blanked by a plain "no maintenance":
    # the row keeps G31_A and a person is carded. The deferred G31_B is still
    # never written - it is dropped, with a card, once G31_A has ended.
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    sweep([msg(1, G31_A), msg(2, G31_B)])
    WRITES.clear(); CARDS.clear()
    r = sweep([msg(1, G31_A), msg(2, G31_B), msg(3, "Hi team, no maintenance this week.")])
    check(actions(r)[-1] == "clear-skipped" and not WRITES, f"{actions(r)} {written()}")
    check("the row was NOT cleared: the row still holds an upcoming window"
          in str(CARDS[-1][1]), str(CARDS[-1][1]))
    WRITES.clear(); CARDS.clear()
    with at(G31_ENDED):
        sweep([msg(1, G31_A), msg(2, G31_B), msg(3, "Hi team, no maintenance this week.")])
    check(not WRITES and not scope().get("pending"), f"{written()} {scope().get('pending')}")
    check(len(CARDS) == 1 and "was NOT written" in str(CARDS[0][1]), str(titles()))


G31_ENDED = datetime(2026, 9, 24, 13, 0, tzinfo=TZ8)     # G31_A's window is over


@case("G3.1", "a newer cancellation of the DEFERRED window drops it: carded, never written")
def _():
    for third in ["Dear Partners, the scheduled maintenance on 01/10/2026 10:00-12:00 "
                  "(GMT+8) has been cancelled.",
                  "Dear Partners, the maintenance has been cancelled."]:
        fresh(BASE)
        sweep([msg(1, G31_A)])
        sweep([msg(1, G31_A), msg(2, G31_B)])
        WRITES.clear(); CARDS.clear()
        r = sweep([msg(1, G31_A), msg(2, G31_B), msg(3, third)])
        check(actions(r)[-1] == "needs-human" and not scope().get("pending"),
              f"{third[:40]}: {actions(r)} {scope().get('pending')}")
        body = CARDS[-1][1]["body"]["elements"][0]["text"]["content"]
        check("Deferred window dropped" in body and "2026-10-01" in body, body)
        WRITES.clear(); CARDS.clear()
        with at(G31_ENDED):
            sweep([msg(1, G31_A), msg(2, G31_B), msg(3, third)])
        check(not WRITES and not CARDS, f"{third[:40]}: {written()} {titles()}")


@case("G3.1", "a newer message sent to a human holds the deferred write (auto-clear off, TBA)")
def _():
    for third in ["Hi team, no maintenance this week.",
                  "The maintenance on 1 Oct may be postponed, the new time will be announced."]:
        fresh(BASE)
        sweep([msg(1, G31_A)])
        sweep([msg(1, G31_A), msg(2, G31_B)])
        sweep([msg(1, G31_A), msg(2, G31_B), msg(3, third)])
        WRITES.clear(); CARDS.clear()
        with at(G31_ENDED):
            r = sweep([msg(1, G31_A), msg(2, G31_B), msg(3, third)])
        check(not WRITES, f"{third[:40]}: written after a human was asked: {written()}")
        check(len(CARDS) == 1 and not scope().get("pending"), f"{titles()} {scope()}")
        body = CARDS[0][1]["body"]["elements"][0]["text"]["content"]
        check("was NOT written" in body and "message 3" in body, body)
        with at(G31_ENDED):
            sweep([msg(1, G31_A), msg(2, G31_B), msg(3, third)])
        check(not WRITES and len(CARDS) == 1, f"repeated: {written()} {titles()}")


@case("G3.1", "two deferred outages: the second does not hold back the first's write")
def _():
    g31_c = "Scheduled maintenance on 08/10/2026 10:00-12:00 (GMT+8)."
    fresh(BASE)
    sweep([msg(1, G31_A)])
    sweep([msg(1, G31_A), msg(2, G31_B), msg(3, g31_c)])
    check([p["mid"] for p in scope().get("pending") or []] == ["2", "3"], str(scope()))
    WRITES.clear()
    with at(G31_ENDED):
        sweep([msg(1, G31_A), msg(2, G31_B), msg(3, g31_c)])
    check(written() == [("recPP", "2026-10-01 10:00", "2026-10-01 12:00")], str(written()))
    with at(datetime(2026, 10, 1, 13, 0, tzinfo=TZ8)):
        sweep([msg(1, G31_A), msg(2, G31_B), msg(3, g31_c)])
    check(written()[-1] == ("recPP", "2026-10-08 10:00", "2026-10-08 12:00"), str(written()))


# ---------------------------------------------------------------------------
# F69 — renaming a row's Group Name / Provider buried the notice on screen
# ---------------------------------------------------------------------------

HAB_OLD, HAB_NEW = "[SG150] Habanero x CasinoPlus", "[SG150] Habanero x CasinoPlus (Official)"


@case("F69", "renamed row: the new notice is carded (not buried), nothing written, backlog silent")
def _():
    rows = [dict(r) for r in BASE] + [row("Habanero", HAB_OLD, "recHAB")]
    fresh(rows)
    sweep([msg(49, "Scheduled maintenance on 2026-09-26 01:00 - 03:00 GMT+8"),
           msg(50, "hello")], provider="Habanero", group=HAB_OLD)
    WRITES.clear(); CARDS.clear()
    ROWS[:] = rows[:-1] + [row("Habanero", HAB_NEW, "recHAB")]
    va.watch_list()
    r = sweep([msg(49, "Scheduled maintenance on 2026-09-26 01:00 - 03:00 GMT+8"),
               msg(50, "hello"),
               msg(51, "Habanero scheduled maintenance on 2026-09-27 01:00 - 03:00 GMT+8")],
              provider="Habanero", group=HAB_NEW, baseline=False)
    check(not WRITES, str(written()))
    check(len(CARDS) == 1 and "renamed" in CARDS[0][1]["body"]["elements"][0]["text"]["content"],
          f"{titles()}")
    check(r["renamed_from"] and va.is_baselined(HAB_NEW, "Habanero"), str(r["renamed_from"]))
    r = sweep([msg(50, "hello"), msg(51, "Habanero scheduled maintenance on 2026-09-27 "
                                         "01:00 - 03:00 GMT+8"),
               msg(52, "Habanero scheduled maintenance on 2026-09-26 04:00 - 06:00 GMT+8")],
              provider="Habanero", group=HAB_NEW, baseline=False)
    check(written() == [("recHAB", "2026-09-26 04:00", "2026-09-26 06:00")], str(written()))


@case("F69", "renamed BACK: the stale old scope does not re-act on what the new name handled")
def _():
    rows = [dict(r) for r in BASE] + [row("Habanero", HAB_OLD, "recHAB")]
    fresh(rows)
    sweep([msg(50, "hello")], provider="Habanero", group=HAB_OLD)
    ROWS[:] = rows[:-1] + [row("Habanero", HAB_NEW, "recHAB")]
    va.watch_list()
    sweep([msg(50, "hello")], provider="Habanero", group=HAB_NEW, baseline=False)
    sweep([msg(50, "hello"),
           msg(51, "Habanero scheduled maintenance on 2026-09-27 01:00 - 03:00 GMT+8")],
          provider="Habanero", group=HAB_NEW, baseline=False)
    check(len(WRITES) == 1, f"precondition: the new name wrote 51: {written()}")
    ROWS[:] = rows                          # the operator renames it back
    va.watch_list()
    WRITES.clear(); CARDS.clear()
    r = sweep([msg(50, "hello"),
               msg(51, "Habanero scheduled maintenance on 2026-09-27 01:00 - 03:00 GMT+8")],
              provider="Habanero", group=HAB_OLD, baseline=False)
    check(not WRITES and not CARDS, f"re-acted after renaming back: {written()} {titles()}")
    check(r["renamed_from"], str(r))


@case("F69", "a genuinely new row (new record_id) still gets its one silent cold sweep")
def _():
    rows = [dict(r) for r in BASE] + [row("Habanero", HAB_NEW, "recHAB2")]
    fresh(rows)
    sweep([msg(51, "Habanero scheduled maintenance on 2026-09-27 01:00 - 03:00 GMT+8")],
          provider="Habanero", group=HAB_NEW, baseline=False)
    check(not WRITES and not CARDS, f"{written()} {titles()}")


# ---------------------------------------------------------------------------
# F57 — a notice pushed out of the read window before the rotation came back
# ---------------------------------------------------------------------------

def _gap_worker(full, reads):
    w = worker([])

    def rd(page, n, max_chars=0, **k):
        reads.append(n)
        return {"messages": list(full[-n:])}
    tw._read_last_messages = rd
    return w


@case("F57", "no bubble reaches the last visit: the worker re-reads 50 and fills the notice")
def _():
    fresh(BASE)
    sweep([msg(k, f"chat {k}") for k in range(1, 9)])
    full = ([msg(k, f"chat {k}") for k in range(1, 9)]
            + [msg(9, "Dear partners, scheduled maintenance on 2026-09-26 02:00 - 04:00 (GMT+8)")]
            + [msg(k, "Noted, thanks") for k in range(10, 18)])
    reads: list = []
    w = _gap_worker(full, reads)
    box: dict = {}
    w._handle_va_watch({"kind": "va_watch", "box": box, "target": va.manual_target(PP)})
    check(reads == [8, 50], f"read counts {reads}")
    check(written() == [("recPP", "2026-09-26 02:00", "2026-09-26 04:00")], str(written()))
    check(box["result"].get("widened") == {"from": 8, "to": 50} and not box["result"]["gap"],
          str(box["result"].get("widened")))


@case("F57", "a read that overlaps the last visit is not widened")
def _():
    fresh(BASE)
    sweep([msg(k, f"chat {k}") for k in range(1, 9)])
    reads: list = []
    w = _gap_worker([msg(k, f"chat {k}") for k in range(1, 12)], reads)
    box: dict = {}
    w._handle_va_watch({"kind": "va_watch", "box": box, "target": va.manual_target(PP)})
    check(reads == [8], f"read counts {reads}")


@case("F57", "even 50 does not reach back: gap reported, ONE card per group per 6 hours")
def _():
    fresh(BASE)
    sweep([msg(k, f"chat {k}") for k in range(1, 9)])
    full = [msg(k, f"chat {k}") for k in range(1, 9)] + [msg(k, "ack") for k in range(100, 160)]
    reads: list = []
    w = _gap_worker(full, reads)
    box: dict = {}
    w._handle_va_watch({"kind": "va_watch", "box": box, "target": va.manual_target(PP)})
    check(box["result"]["gap"] and box["result"]["gap"]["carded"], str(box["result"]["gap"]))
    check(titles() == ["⚠️ Possibly missed messages · Pragmatic Play"], str(titles()))
    full += [msg(k, "ack") for k in range(200, 260)]
    w._handle_va_watch({"kind": "va_watch", "box": {}, "target": va.manual_target(PP)})
    check(len(CARDS) == 1, f"carded again inside 6h: {titles()}")
    text = va.format_check_summary(dict(box["result"]))
    check("possible missed messages" in text, text)


@case("F57", "VAWATCH_GAP_CARD=0 keeps the log line and result, drops the card")
def _():
    fresh(BASE, env={"VAWATCH_GAP_CARD": "0"})
    sweep([msg(k, f"chat {k}") for k in range(1, 9)])
    r = sweep([msg(k, "ack") for k in range(100, 160)])
    check(r["gap"] and not CARDS, f"{r['gap']} {titles()}")


@case("F34", "VAWATCH_ORDER_CHECK=0 is the kill-switch (newest-in-the-read wins again)")
def _():
    fresh(BASE, env={"VAWATCH_ORDER_CHECK": "0"})
    real = va.update_row
    va.update_row = _flaky_first_write()
    try:
        sweep([msg(10, NA), msg(11, NB)])
        sweep([msg(10, NA), msg(11, NB)])
    finally:
        va.update_row = real
    check(row_now() == ("recPP", "2026-09-25 10:00", "2026-09-25 12:00"),
          f"switch off must restore the old order: {written()}")
    fresh(BASE, env={"VAWATCH_ORDER_CHECK": "0"})
    sweep([msg(1, G31_A)])
    sweep([msg(1, G31_A), msg(2, G31_B)])
    check(row_now() == ("recPP", "2026-10-01 10:00", "2026-10-01 12:00"), str(written()))


# ---------------------------------------------------------------------------
# Migration: a ledger written before per-row state existed
# ---------------------------------------------------------------------------

@case("F34", "a pre-existing ledger: floor and owner are derived from its entries on first use")
def _():
    fresh(BASE)
    va.mark_baselined(PPG, PP)
    d = va._load()
    for k, outcome in ((5, "cold-start"), (6, "cold-start"), (11, "filled")):
        va._mark(d, va._key(msg(k, "x"), PPG, PP), outcome,
                 {"window": "2026-09-26 10:00 → 12:00 (rescheduled)"} if outcome == "filled"
                 else None)
    d.pop("scopes", None)
    va._save(d)
    r = sweep([msg(4, "Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8)."),
               msg(10, NA), msg(11, "x")])
    check(actions(r) == ["history", "superseded", "already handled"], str(actions(r)))
    check(not WRITES and not CARDS, f"{written()} {titles()}")


# ===========================================================================
# Stage W3 — what the Laboratory group and /vacheck are told
# ===========================================================================

import requests as _rq  # noqa: E402  (already stubbed above; only its exception types)

NOTICE_27 = "Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8)."


def card_text(i=0):
    """Every lark_md block of card ``i`` joined, or the text fallback itself."""
    c = CARDS[i][1]
    if isinstance(c, str):
        return c
    return "\n".join(e["text"]["content"] for e in c["body"]["elements"]
                     if e.get("tag") == "div")


class _Sinks:
    """Swap vawatch's send/write fakes for one test; always put back."""

    def __init__(self, **fakes):
        self.fakes = fakes

    def __enter__(self):
        self.saved = {k: getattr(va, k) for k in self.fakes}
        for k, v in self.fakes.items():
            setattr(va, k, v)
        return self

    def __exit__(self, *exc):
        for k, v in self.saved.items():
            setattr(va, k, v)


# ---------------------------------------------------------------------------
# F79 — a TELEGRAM+TEAMS row was logged as "NO watcher reads"
# ---------------------------------------------------------------------------

@case("F79", "a row tagged TELEGRAM+TEAMS is watched and NOT listed as unread")
def _():
    fresh(BASE + [row("Dual", "Dual CS", "recDUAL", apps=("telegram", "teams"))])
    rep = va.watch_list()
    check("Dual" in [r["provider"] for r in rep["telegram"]], str(rep["telegram"]))
    check([r["provider"] for r in rep["teams"]] == ["GEMINI"], str(rep["teams"]))
    check([r["provider"] for r in rep["also_teams"]] == ["Dual"], str(rep["also_teams"]))
    r = sweep([msg(1, "hello")])
    text = va.format_check_summary(dict(r, group=PPG, provider=PP))
    check("Dual" not in text and "GEMINI" in text, text)
    # The cached list (a Base outage) carries the same split.
    va._LAST_WATCH.clear()
    real = gc.fetch_rows
    gc.fetch_rows = lambda: (_ for _ in ()).throw(RuntimeError("Lark 502"))
    try:
        rep = va.watch_list()
    finally:
        gc.fetch_rows = real
    check(rep["source"] == "cache" and [r["provider"] for r in rep["teams"]] == ["GEMINI"]
          and [r["provider"] for r in rep["also_teams"]] == ["Dual"], str(rep))


# ---------------------------------------------------------------------------
# F74 — the card's window was in the notice's zone, unlabelled, end date cut
# ---------------------------------------------------------------------------

@case("F74", "card and ledger state the window the row displays: GMT+8, end date, no 'PST'")
def _():
    for text, head, foot in [
            ("Scheduled maintenance 2026-09-30 10:00 - 12:00 UTC",
             ["**Start:** 2026-09-30 18:00 GMT+8 (notice: 2026-09-30 10:00 UTC)",
              "**End:** 2026-09-30 20:00 GMT+8 (notice: 2026-09-30 12:00 UTC)"],
             "2026-09-30 18:00 → 20:00 GMT+8"),
            ("Scheduled maintenance on 2026-09-26 from 22:00 to 2026-09-27 02:00 (GMT+8)",
             ["**Start:** 2026-09-26 22:00 GMT+8", "**End:** 2026-09-27 02:00 GMT+8"],
             "2026-09-26 22:00 → 2026-09-27 02:00 GMT+8"),
            ("Scheduled maintenance 2026-09-30 10:00 - 12:00",
             ["**Start:** 2026-09-30 10:00 GMT+8", "**End:** 2026-09-30 12:00 GMT+8"],
             "2026-09-30 10:00 → 12:00 GMT+8")]:
        fresh(BASE)
        r = sweep([msg(5, text)])
        body = card_text()
        check(all(h in body for h in head), f"{text}: {body}")
        check(f"Base row updated · {foot}" in body, f"{text}: {body}")
        check("PST" not in body and "UTC+08:00" not in body, f"{text}: {body}")
        check(r["details"][0]["window"] == foot, str(r["details"]))
        summary = va.format_check_summary(dict(r, group=PPG, provider=PP))
        check(f"wrote {foot}" in summary, summary)


# ---------------------------------------------------------------------------
# F71 — a write that landed but whose card failed was final and unannounced
# ---------------------------------------------------------------------------

def _raise_conn(*a, **k):
    raise _rq.exceptions.ConnectionError("simulated: refused before anything was sent")


@case("F71", "card RAISES before sending (ConnectionError / token): the text fallback delivers it")
def _():
    fresh(BASE)
    with _Sinks(send_card=_raise_conn):
        r = sweep([msg(5, NOTICE_27)])
    check(written() == [("recPP", "2026-09-27 02:00", "2026-09-27 04:00")], str(written()))
    check(titles() == ["TEXT:🛠️ Detected scheduled maintenance for Pragmatic Play x "
                       "Casinoplus CS"], str(titles()))
    check(r["details"][0]["carded"] is True and not r["unannounced"], str(r["details"]))


@case("F71", "card AND text fail after the write: ledgered filled, card-only retry next visit")
def _():
    fresh(BASE)
    with _Sinks(send_card=_raise_conn, send_text=lambda c, t: {"code": 11232}):
        r = sweep([msg(5, NOTICE_27)])
    check(len(WRITES) == 1 and not CARDS, f"{written()} {titles()}")
    check(r["unannounced"] == 1 and r["details"][0]["carded"] is False, str(r))
    s = va.format_check_summary(dict(r, group=PPG, provider=PP))
    check("written, but its card was NOT delivered" in s and "card(s) posted" not in s, s)
    ent = va._load()["handled"]["pragmatic play@pragmatic play x casinoplus cs|mid:5"]
    check(ent["outcome"] == "filled" and ent["card"] == "failed", str(ent))
    # Next visit, messaging works again: the CARD is sent, the row is NOT re-written.
    r = sweep([msg(5, NOTICE_27)])
    check(len(WRITES) == 1, f"re-written: {written()}")
    check(titles() == ["🛠️ Scheduled maintenance · Pragmatic Play"], str(titles()))
    check("Announced late" in card_text() and "Base row updated" in card_text(), card_text())
    check(r["announced_late"] == 1 and actions(r) == ["announced-late", "already handled"],
          str(r["details"]))
    check("card delivered late for an earlier write: 1"
          in va.format_check_summary(dict(r, group=PPG, provider=PP)), "summary")
    # ...and only once.
    sweep([msg(5, NOTICE_27)])
    check(len(CARDS) == 1, f"late card sent twice: {titles()}")


@case("F71", "the card-only retry is bounded by VAWATCH_WRITE_ATTEMPTS, then logged and dropped")
def _():
    fresh(BASE)
    dead = dict(send_card=_raise_conn, send_text=lambda c, t: {"code": 230002})
    with _Sinks(**dead):
        r1 = sweep([msg(5, NOTICE_27)])
        r2 = sweep([msg(5, NOTICE_27)])
        r3 = sweep([msg(5, NOTICE_27)])
        r4 = sweep([msg(5, NOTICE_27)])
    check(len(WRITES) == 1 and not CARDS, f"{written()} {titles()}")
    check((r1["unannounced"], r2["unannounced_dropped"], r3["unannounced_dropped"])
          == (1, 0, 1), f"{r1['unannounced']} {r2['unannounced_dropped']} "
                        f"{r3['unannounced_dropped']}")
    check(not r4["unannounced_dropped"] and "announce-gave-up" not in actions(r4),
          str(r4["details"]))


@case("F71", "a card send that TIMED OUT is not re-sent (it may have arrived): reported instead")
def _():
    fresh(BASE)

    def _timeout(*a, **k):
        raise _rq.exceptions.ReadTimeout("simulated: sent, no answer")
    with _Sinks(send_card=_timeout):
        r = sweep([msg(5, NOTICE_27)])
    check(not CARDS and r["card_unknown"] == 1 and not r["unannounced"], str(r))
    s = va.format_check_summary(dict(r, group=PPG, provider=PP))
    check("card send timed out" in s, s)
    sweep([msg(5, NOTICE_27)])
    check(not CARDS and len(WRITES) == 1, f"{titles()} {written()}")


@case("F71", "a CLEAR that landed unannounced is re-sent the same way (auto-clear on)")
def _():
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    with _Sinks(send_card=_raise_conn, send_text=_raise_conn):
        r = sweep([msg(5, "Hi team, no maintenance this week.")])
    check(len(WRITES) == 1 and WRITES[0][1]["Start Time"] is None and not CARDS,
          f"{written()} {titles()}")
    check(r["unannounced"] == 1, str(r))
    sweep([msg(5, "Hi team, no maintenance this week.")])
    check(len(WRITES) == 1 and titles() == ["🧹 No maintenance · Pragmatic Play"],
          f"{written()} {titles()}")


@case("F71", "main.py's real /vacheck: an undelivered card is reported, never claimed as posted")
def _():
    fresh(BASE)
    va.mark_baselined(va._chat_title(), "VA")
    with _Sinks(send_card=_raise_conn, send_text=lambda c, t: {"code": 11232}):
        replies = run_vacheck(["/vacheck"], real_va_check_now([msg(1, NOTICE_27)]))
    check(len(replies) == 2 and replies[1].startswith("✅ VA sweep done"), str(replies))
    check("written, but its card was NOT delivered" in replies[1]
          and "card(s) posted" not in replies[1], replies[1])


@case("F78", "main.py's real /vacheck: a stale cached watch list is named in the reply")
def _():
    fresh(BASE)
    va.mark_baselined(va._chat_title(), "VA")
    d = va._load()
    d["targets"]["at"] = "2026-09-01 09:00:00"
    va._save(d)
    va._LAST_WATCH.clear()
    real = gc.fetch_rows
    gc.fetch_rows = lambda: (_ for _ in ()).throw(RuntimeError("code 1254030"))
    try:
        replies = run_vacheck(["/vacheck"], real_va_check_now([msg(1, "hello")]))
    finally:
        gc.fetch_rows = real
    check("⚠️ watch list from the cache (read at 2026-09-01 09:00:00" in replies[-1]
          and "1254030" in replies[-1], replies[-1])


# ---------------------------------------------------------------------------
# F80 — PUT applied, response timed out: a red "NOT updated" card
# ---------------------------------------------------------------------------

PUT_AT: list = []     # len(GETS) when each PUT went out: the G3.1 row read-back
                      # (#89) happens BEFORE the write, the F80 confirm AFTER it


def _put_then_timeout(rid, fields):
    PUT_AT.append(len(GETS))
    WRITES.append((rid, dict(fields)))        # Lark applied it...
    raise _rq.exceptions.ReadTimeout("simulated: ...but never answered")


def _gets_after_put():
    return GETS[PUT_AT[-1]:] if PUT_AT else list(GETS)


@case("F80", "PUT applied but timed out: read back, confirmed, orange 'Base row updated'")
def _():
    fresh(BASE)
    with _Sinks(update_row=_put_then_timeout):
        r = sweep([msg(5, NOTICE_27)])
    check(_gets_after_put() == ["recPP"], f"read back: {GETS} {PUT_AT}")
    c = CARDS[0][1]
    check(c["header"]["template"] == "orange" and "NOT updated" not in card_text()
          and "reading the row back shows it landed" in card_text(), card_text())
    check(actions(r) == ["filled"] and r["acted"] == 1 and not r["write_failed"], str(r))
    check(va._load()["handled"]["pragmatic play@pragmatic play x casinoplus cs|mid:5"]
          ["outcome"] == "filled", "ledgered as a write")


@case("F80", "timed out and the read-back fails too: 'outcome unknown' (yellow), retried")
def _():
    fresh(BASE)

    def _get_fails(rid):
        GETS.append(rid)
        raise _rq.exceptions.ConnectionError("simulated")
    with _Sinks(update_row=_put_then_timeout, get_row=_get_fails):
        r = sweep([msg(5, NOTICE_27)])
    c = CARDS[0][1]
    check(c["header"]["template"] == "yellow" and "may or may not have landed" in card_text()
          and "NOT updated" not in card_text(), card_text())
    check(r["write_failed"] == 1 and r["write_unknown"] == 1, str(r))
    check("outcome UNKNOWN" in va.format_check_summary(dict(r, group=PPG, provider=PP)),
          "summary")
    r = sweep([msg(5, NOTICE_27)])            # the retry re-PUTs (idempotent)
    check(actions(r) == ["filled"] and r["retried"] == 1, str(r["details"]))


@case("F80", "a PUT that timed out and did NOT land is still reported as NOT updated")
def _():
    fresh(BASE)

    def _timeout_no_write(rid, fields):
        raise _rq.exceptions.ReadTimeout("simulated")
    with _Sinks(update_row=_timeout_no_write):
        r = sweep([msg(5, NOTICE_27)])
    check(CARDS[0][1]["header"]["template"] == "red" and "NOT updated" in card_text(),
          card_text())
    check(r["write_failed"] == 1 and not r["write_unknown"], str(r))
    fresh(BASE)                               # a Lark code != 0 is not read back

    def _code_fail(rid, f):
        PUT_AT.append(len(GETS))
        raise RuntimeError("Base update failed: {'code': 1254001}")
    with _Sinks(update_row=_code_fail):
        sweep([msg(5, NOTICE_27)])
    check(not _gets_after_put() and "NOT updated" in card_text(), f"{GETS} {card_text()}")


# ---------------------------------------------------------------------------
# F78 — the cached watch list had no age limit and nobody was told
# ---------------------------------------------------------------------------

class _BaseDown:
    """groupcheck.fetch_rows failing the way a deleted view does."""

    def __enter__(self):
        self.real = gc.fetch_rows
        gc.fetch_rows = lambda: (_ for _ in ()).throw(
            RuntimeError("Base read failed (code 1254030): view not found"))

    def __exit__(self, *exc):
        gc.fetch_rows = self.real


def _age_cache(stamp):
    d = va._load()
    d["targets"]["at"] = stamp
    va._save(d)


@case("F78", "cache-only for 24h+: ONE stale-list card, /vacheck says so; re-armed by a good read")
def _():
    fresh(BASE)
    _age_cache("2026-09-01 09:00:00")
    CARDS.clear()
    with _BaseDown():
        for _ in range(4):
            rep = va.watch_list()
        r = sweep([msg(5, "hello")])
    check(rep["source"] == "cache", str(rep))
    check(titles() == ["⚠️ VA watcher: the watch list is stale"], str(titles()))
    check("NOT watched" in card_text() and "1254030" in card_text(), card_text())
    s = va.format_check_summary(dict(r, group=PPG, provider=PP))
    check("watch list from the cache (read at 2026-09-01 09:00:00, 531h ago)" in s
          and "1254030" in s, s)
    va.watch_list()                            # the Base is back: fresh stamp, re-armed
    check(va._load()["targets"]["at"] == "2026-09-23 12:00:00", "good read re-cached")
    r = sweep([msg(6, "hello")])
    check("watch list from" not in va.format_check_summary(dict(r, group=PPG, provider=PP)),
          "a good read must clear the summary line")
    _age_cache("2026-09-20 09:00:00")
    with _BaseDown():
        va.watch_list()
    check(len(CARDS) == 2, f"a new stale stretch must alert again: {titles()}")


@case("F78", "a young cache (a 5xx blip) or VAWATCH_TARGET_CACHE_MAX_H=0: no card")
def _():
    fresh(BASE)
    _age_cache("2026-09-23 02:00:00")          # 10 hours
    CARDS.clear()
    with _BaseDown():
        va.watch_list()
    check(not CARDS, f"alerted on a 10h-old list: {titles()}")
    fresh(BASE, env={"VAWATCH_TARGET_CACHE_MAX_H": "0"})
    _age_cache("2026-08-01 09:00:00")
    CARDS.clear()
    with _BaseDown():
        va.watch_list()
    check(not CARDS, f"MAX_H=0 still alerted: {titles()}")


@case("F78", "the stale-list card that FAILED is tried again next tick, then only once")
def _():
    fresh(BASE)
    _age_cache("2026-09-01 09:00:00")
    CARDS.clear()
    with _BaseDown():
        with _Sinks(send_card=lambda c, k: {"code": 99}, send_text=lambda c, t: {"code": 99}):
            va.watch_list()
        check(va._load().get("targets_alerted") is None, "a failed alert was recorded")
        va.watch_list()
        va.watch_list()
    check(titles() == ["⚠️ VA watcher: the watch list is stale"], str(titles()))


# ---------------------------------------------------------------------------
# F58 — ordinary CS chat produced red "needs a human" cards
# ---------------------------------------------------------------------------

F58_CHAT = [
    "Hi team, player 88231 bet dispute, round ID 55123-AB, please see below.",
    "详见附件，谢谢", "请见截图", "玩家投诉，详见下方截图",
    "Please refer to the attached game list",
    "The payout for player 88231 is postponed, we will update you.",
    "Can we reschedule the call to tomorrow?",
    "The hotfix will be deployed tonight with no maintenance downtime.",
]


@case("F58", "ordinary CS chat (referrals, payout/call 'postponed', 'no maintenance downtime'): no card")
def _():
    for text in F58_CHAT:
        for env in ({}, {"VAWATCH_CLEAR_ENABLED": "1"}):
            # The row holds a live window (so a false clear would blank it),
            # and the chat is NOT directly under its notice - that one case is
            # G2.3's, where a bare "postponed" may well be about the notice.
            fresh(BASE, env=env)
            sweep([msg(1, NOTICE_27), msg(2, "noted, thanks")])
            WRITES.clear(); CARDS.clear()
            r = sweep([msg(1, NOTICE_27), msg(2, "noted, thanks"), msg(3, text)])
            check(not CARDS, f"{text[:40]} {env}: carded {titles()}")
            check(not WRITES, f"{text[:40]} {env}: wrote {written()} - the row lost its window")
            # "...with no maintenance downtime" is a NEGATION refusal, counted
            # as ignored_refused since the integration stage; either way it is
            # ignored, with no card and no write.
            check(actions(r)[2] == "ignored"
                  and r["ignored_not_maintenance"] + r["ignored_refused"] == 1,
                  f"{text[:40]}: {r['details']}")
        fresh(BASE)                                # the shared group: no card either
        shared_sweep(text)
        check(not CARDS, f"shared group, {text[:40]}: {titles()}")


@case("F58", "the same verdicts about MAINTENANCE are still carded (no silent drop)")
def _():
    for text in ["The maintenance scheduled for 26/09 has been postponed, new time TBA.",
                 "维护时间延后，新的时间另行通知。",
                 "Scheduled maintenance: please see the notice below.",
                 "Maintenance update: please refer to the attached schedule."]:
        fresh(BASE)
        r = sweep([msg(1, text)])
        check(len(CARDS) == 1 and CARDS[0][0].startswith("❓") and not WRITES,
              f"{text[:40]}: {titles()} {r['details']}")
    fresh(BASE)                                    # a real "no maintenance" still clears
    sweep([msg(1, "Hi team, no maintenance required this week.")],)
    check(len(CARDS) == 1 and "no maintenance" in card_text().lower(), str(titles()))


@case("F58", "per-row cap: 6 red cards a day, then ONE 'limit reached' card; resets after 24h")
def _():
    fresh(BASE)
    texts = [f"Scheduled maintenance #{k}: the new time will be announced, it is postponed."
             for k in range(10)]
    for k, t in enumerate(texts):
        sweep([msg(10 + k, t)])
    red = [t for t in titles() if t.startswith("❓")]
    check(len(red) == 6, f"{len(red)} red cards: {titles()}")
    check(titles().count("⚠️ Card limit reached · Pragmatic Play") == 1, str(titles()))
    check("held back until 2026-09-24 12:00 GMT+8" in card_text(6), card_text(6))
    r = sweep([msg(30, "Scheduled maintenance #x postponed, new time to follow.")])
    check(r["capped"] == 1 and r["details"][0]["capped"] is True and len(CARDS) == 7,
          f"{r['details']} {titles()}")
    check("HELD BACK by the per-row limit (read the group by hand): 1"
          in va.format_check_summary(dict(r, group=PPG, provider=PP)), "summary")
    check(actions(sweep([msg(30, "Scheduled maintenance #x postponed, new time to follow.")]))
          == ["already handled"], "a held-back card is decided, not retried every tick")
    with at(NOW + timedelta(hours=25)):
        sweep([msg(40, "Scheduled maintenance #y postponed, new time to follow.")])
    check(titles()[-1].startswith("❓"), f"the cap must reset: {titles()}")
    fresh(BASE, env={"VAWATCH_NEEDS_HUMAN_CAP": "0"})
    for k, t in enumerate(texts):
        sweep([msg(10 + k, t)])
    check(len(CARDS) == 10, f"VAWATCH_NEEDS_HUMAN_CAP=0 must not cap: {len(CARDS)}")


@case("F58", "the cap counts once per shared-group message, and a capped one is not sent by the co-tenant")
def _():
    fresh(BASE, env={"VAWATCH_NEEDS_HUMAN_CAP": "2"})
    for k in range(3):
        shared_sweep(f"Scheduled maintenance #{k}: postponed, new time TBA.", mid=50 + k)
    red = [t for t in titles() if t.startswith("❓")]
    check(len(red) == 2 and titles().count("⚠️ Card limit reached · Hacksaw") == 1,
          str(titles()))


# ---------------------------------------------------------------------------
# F72 — /provideraskmaintenance answers also raised a red vawatch card
# ---------------------------------------------------------------------------

def _pa_state(finished=False, outcome="waiting", asked_at="2026-09-23 11:00:00"):
    import json as _json
    pa.STATE_PATH.write_text(_json.dumps({
        "started_at": asked_at, "asked_at": asked_at,
        "deadline": "2026-09-23T13:00:00+08:00", "finished": finished,
        "providers": {PP: {"group": PPG, "asked": True, "asked_at": asked_at,
                           "outcome": outcome},
                      "Hacksaw": {"group": SG, "asked": True, "asked_at": asked_at,
                                  "outcome": outcome},
                      "YGG": {"group": SG, "asked": True, "asked_at": asked_at,
                              "outcome": outcome, "shared_with": "Hacksaw"}}}))


@case("F72", "an open providerask run owns 'no maintenance' replies: no vawatch card, one group or shared")
def _():
    fresh(BASE)
    _pa_state()
    r = sweep([msg(5, "Hi team, no maintenance this week. Thank you!")])
    check(not CARDS and not WRITES, f"{titles()} {written()}")
    check(actions(r) == ["clear-skipped"] and r["left_to_providerask"] == 1
          and not r["needs_human"], str(r["details"]))
    check("left to the open /provideraskmaintenance run (no card from here): 1"
          in va.format_check_summary(dict(r, group=PPG, provider=PP)), "summary")
    shared_sweep("Hi team, no maintenance this week. Thank you!")
    check(not CARDS, f"shared group: {titles()}")
    # A group the run did NOT ask is still carded.
    r = sweep([msg(5, "Hi team, no maintenance this week.")], provider="JILI", group="JILI CS")
    check(len(CARDS) == 1, f"unasked group: {titles()}")


@case("F72", "a finished run: owned only if it filed that answer within a day; no run: carded")
def _():
    for finished, outcome, asked, owned in [
            (True, "no_maintenance", "2026-09-23 10:00:00", True),
            (True, "no_update", "2026-09-23 10:00:00", False),       # a late reply is news
            (True, "no_maintenance", "2026-09-20 10:00:00", False),  # last week's run
            (None, "", "", False)]:
        fresh(BASE)
        if finished is not None:
            _pa_state(finished=finished, outcome=outcome, asked_at=asked)
        sweep([msg(5, "Hi team, no maintenance this week.")])
        check(bool(CARDS) != owned, f"{finished} {outcome} {asked}: {titles()}")


@case("F72", "auto-clear ON: the clear still happens during a run (it is true), and is announced")
def _():
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    _pa_state()
    sweep([msg(5, "Hi team, no maintenance this week.")])
    check(len(WRITES) == 1 and titles() == ["🧹 No maintenance · Pragmatic Play"],
          f"{written()} {titles()}")


# ---------------------------------------------------------------------------
# F51 — a detected cancellation left the withdrawn window with no card
# ---------------------------------------------------------------------------

F51_ROW = "Scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8)."


@case("F51", "window-less cancellations ('Update: maintenance cancelled.', 'called off') are carded")
def _():
    for text in ["Update: maintenance cancelled.", "The maintenance has been called off.",
                 "The scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8) has been "
                 "cancelled.", "尊敬的合作伙伴：原定于9月25日10:00-12:00的维护已取消。"]:
        fresh(BASE)
        sweep([msg(100, F51_ROW)])
        WRITES.clear(); CARDS.clear()
        r = sweep([msg(100, F51_ROW), msg(101, text)])
        check(not WRITES, f"{text[:30]}: {written()}")
        check(actions(r)[1] == "needs-human" and len(CARDS) == 1
              and "CANCELLED" in card_text() and "2026-09-25 10:00 → 12:00 GMT+8"
              in card_text(), f"{text[:30]}: {r['details']} {titles()}")


@case("F51", "not a cancellation: 'NOT cancelled', a completion, a cancelled promo - no card")
def _():
    for text in ["The maintenance is not cancelled and will proceed as scheduled.",
                 "Maintenance completed. Thank you.",
                 "The weekend tournament has been cancelled."]:
        fresh(BASE)
        sweep([msg(100, F51_ROW)])
        WRITES.clear(); CARDS.clear()
        sweep([msg(100, F51_ROW), msg(101, text)])
        check(not CARDS and not WRITES, f"{text[:30]}: {titles()} {written()}")


@case("F51", "/vacheck no longer calls a completion or cancellation 'not about maintenance'")
def _():
    for text in ["Maintenance completed. Thank you.",
                 "The maintenance on 30/09/2026 10:00-12:00 (GMT+8) has been cancelled.",
                 "Update: maintenance cancelled."]:
        fresh(BASE)
        r = sweep([msg(101, text)])                # nothing on the row to cancel
        s = va.format_check_summary(dict(r, group=PPG, provider=PP))
        check("ignored (maintenance completed / cancelled" in s
              and "not about maintenance" not in s and not CARDS, f"{text[:30]}: {s}")


@case("F51", "a row this watcher did NOT fill (read back from the Base) is carded too")
def _():
    fresh(BASE)
    WRITES.append(("recPP", {"Start Time": va._ms(datetime(2026, 9, 25, 10, 0, tzinfo=TZ8)),
                             "End Time": va._ms(datetime(2026, 9, 25, 12, 0, tzinfo=TZ8))}))
    r = sweep([msg(101, "Update: maintenance cancelled.")])
    check(GETS == ["recPP"] and len(CARDS) == 1 and "not filled by this watcher"
          in card_text(), f"{GETS} {titles()} {r['details']}")
    fresh(BASE)                                    # an empty / past row: nothing to say
    r = sweep([msg(101, "Update: maintenance cancelled.")])
    check(not CARDS, str(titles()))


@case("F51", "VAWATCH_CLEAR_ENABLED=1 clears ONLY an exact, read-back match; anything less is carded")
def _():
    exact = "The scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8) has been cancelled."
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    sweep([msg(100, F51_ROW)])
    WRITES.clear(); CARDS.clear()
    real_get = va.get_row
    va.get_row = lambda rid: (GETS.append(rid), {
        "Start Time": va._ms(datetime(2026, 9, 25, 10, 0, tzinfo=TZ8)),
        "End Time": va._ms(datetime(2026, 9, 25, 12, 0, tzinfo=TZ8))})[1]
    try:
        r = sweep([msg(100, F51_ROW), msg(101, exact)])
    finally:
        va.get_row = real_get
    check(len(WRITES) == 1 and WRITES[0][1]["Start Time"] is None
          and WRITES[0][1]["Remark"] == exact, str(WRITES))
    check(titles() == ["🧹 Maintenance CANCELLED · Pragmatic Play"] and r["cleared"] == 1,
          f"{titles()} {r['details']}")
    for text, why in [
            ("Update: maintenance cancelled.", "no window named"),
            ("The maintenance on 2026-09-25 10:00 - 11:00 (GMT+8) has been cancelled.",
             "a nearby, not the same, window")]:
        fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
        sweep([msg(100, F51_ROW)])
        WRITES.clear(); CARDS.clear()
        sweep([msg(100, F51_ROW), msg(101, text)])
        check(not WRITES and len(CARDS) == 1 and CARDS[0][0].startswith("❓"),
              f"{why}: {written()} {titles()}")
    # The row was edited by hand since: the read-back differs, so no blanking.
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    sweep([msg(100, F51_ROW)])
    WRITES.append(("recPP", {"Start Time": va._ms(datetime(2026, 9, 26, 10, 0, tzinfo=TZ8)),
                             "End Time": va._ms(datetime(2026, 9, 26, 12, 0, tzinfo=TZ8))}))
    n0 = len(WRITES); CARDS.clear()
    sweep([msg(100, F51_ROW), msg(101, exact)])
    check(len(WRITES) == n0 and len(CARDS) == 1 and CARDS[0][0].startswith("❓"),
          f"hand-edited row blanked: {WRITES[n0:]} {titles()}")
    # A shared group is never cleared on its own.
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    sweep([msg(1, "HS scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8).")],
          provider="Hacksaw", group=SG)
    WRITES.clear(); CARDS.clear()
    sweep([msg(1, "HS scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8)."),
           msg(2, "HS: the scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8) has "
                  "been cancelled.")], provider="Hacksaw", group=SG)
    check(not WRITES and len(CARDS) == 1, f"{written()} {titles()}")


@case("F51", "under /vacheck force a window-less cancellation ends the walk (nothing re-written)")
def _():
    fresh(BASE)
    va.mark_baselined(PPG, PP)
    r = va.handle_messages([msg(100, F51_ROW), msg(101, "Update: maintenance cancelled.")],
                           force=True, provider=PP, group=PPG, record_id="recPP")
    check(not WRITES and "cancel" in r["force_stop"], f"{written()} {r['force_stop']!r}")


# ---------------------------------------------------------------------------
# G2.3 — a plain retraction / correction of the notice above was dropped
# ---------------------------------------------------------------------------

G23_A = "Scheduled maintenance notice: 24/09/2026 10:00-12:00 (GMT+8). All games will be unavailable."
G23_FOLLOWUPS = [
    "Please ignore the previous message, it was sent to the wrong group. Sorry!",
    "Sorry wrong group", "Kindly disregard the previous maintenance announcement.",
    "Please ignore the maintenance notice above, it was meant for another merchant.",
    "Correction: the maintenance date should be 25/09 instead of 24/09.",
    "Sorry, typo in the notice above. The correct time is 14:00-16:00 (GMT+8).",
    "Correction: maintenance will be on 25/09/2026 14:00-16:00 (GMT+8), not 24/09.",
    "请忽略上一条消息，发错群了", "不好意思，发错群了", "请无视之前的维护公告。",
    "请忽略上面的维护通知，那是发给其他商户的。", "更正：维护日期应为9月25日，而不是9月24日。",
    "抱歉，上面的通知有误，正确时间为 14:00-16:00 (GMT+8)。",
    "更正：维护改在 2026年9月25日 14:00-16:00 (GMT+8)，不是24日。",
    "請忽略上一條消息，發錯群了", "不好意思，發錯群了", "請無視之前的維護公告。",
    "請忽略上面的維護通知，那是發給其他商戶的。", "更正：維護日期應為9月25日，而不是9月24日。",
    "抱歉，上面的通知有誤，正確時間為 14:00-16:00 (GMT+8)。",
]


@case("G2.3", "every EN/ZH/TW retraction or correction shape: ONE card naming the row's window")
def _():
    for text in G23_FOLLOWUPS:
        fresh(BASE)
        sweep([msg(100, G23_A)])
        WRITES.clear(); CARDS.clear()
        outs = [actions(sweep([msg(100, G23_A), msg(101, text)]))[1] for _ in range(4)]
        check(not WRITES, f"{text[:30]}: {written()}")
        check(outs == ["needs-human"] + ["already handled"] * 3, f"{text[:30]}: {outs}")
        check(len(CARDS) == 1 and "RETRACT or CORRECT" in card_text()
              and "2026-09-24 10:00 → 12:00 GMT+8" in card_text(), f"{text[:30]}: {titles()}")
    fresh(BASE)                                    # the corrected window is named too
    sweep([msg(100, G23_A)])
    CARDS.clear()
    sweep([msg(100, G23_A), msg(101, G23_FOLLOWUPS[6])])
    check("It mentions 2026-09-25 14:00 → 16:00 GMT+8" in card_text(), card_text())


@case("G2.3", "a bare 'postponed' directly under the row's notice is still carded (F58 gate)")
def _():
    # Carded before the F58 gate as a window-less reschedule; the gate alone
    # would now drop it (no maintenance word) and leave the withdrawn window.
    for text in ["Update: postponed, new time TBA.", "延期了，时间另行通知",
                 "Rescheduled, new window to follow."]:
        fresh(BASE)
        sweep([msg(100, G23_A)])
        WRITES.clear(); CARDS.clear()
        r = sweep([msg(100, G23_A), msg(101, text)])
        check(len(CARDS) == 1 and "2026-09-24 10:00 → 12:00 GMT+8" in card_text()
              and not WRITES, f"{text}: {titles()} {r['details']}")


@case("G2.3", "a correction the PARSER already cards (unknown zone) keeps the parser's reason")
def _():
    fresh(BASE)
    sweep([msg(100, G23_A)])
    WRITES.clear(); CARDS.clear()
    r = sweep([msg(100, G23_A),
               msg(101, "Correction: scheduled maintenance 2026-09-25 10:00 - 12:00 CEST")])
    check(len(CARDS) == 1 and not WRITES, f"{titles()} {written()}")
    check("zone the parser does not know (CEST)" in card_text()
          and "RETRACT or CORRECT" not in card_text(), card_text())


@case("G2.3", "no card when the row's notice is not on screen, its window is over, or nothing is on the row")
def _():
    fresh(BASE)                                    # notice scrolled out of this read
    sweep([msg(100, G23_A)])
    CARDS.clear()
    sweep([msg(101, "hi"), msg(102, "Sorry wrong group")])
    check(not CARDS, f"scrolled out: {titles()}")
    fresh(BASE)                                    # nothing written from this group
    sweep([msg(100, "hello"), msg(101, "Please ignore the previous message")])
    check(not CARDS, f"no row window: {titles()}")
    fresh(BASE)                                    # the window has passed
    sweep([msg(100, G23_A)])
    CARDS.clear()
    with at(datetime(2026, 9, 24, 13, 0, tzinfo=TZ8)):
        sweep([msg(100, G23_A), msg(101, "Sorry wrong group")])
    check(not CARDS, f"window over: {titles()}")
    fresh(BASE)                                    # older than the notice: not about it
    sweep([msg(99, "Sorry wrong group"), msg(100, G23_A)])
    check(titles() == ["🛠️ Scheduled maintenance · Pragmatic Play"], str(titles()))
    fresh(BASE)                                    # not under it, not pointing at it
    sweep([msg(100, G23_A)])
    CARDS.clear()
    sweep([msg(100, G23_A), msg(101, "hi team"),
           msg(102, "Correction: the bonus amount should be 500")])
    check(not CARDS, f"a correction about something else: {titles()}")
    CARDS.clear()                                  # ...but pointing at the notice is carded
    sweep([msg(100, G23_A), msg(101, "hi team"), msg(102, "hello"),
           msg(103, "Sorry, please ignore the notice above, wrong group")])
    check(len(CARDS) == 1, f"a retraction pointing at the notice: {titles()}")


# ---------------------------------------------------------------------------
# F59 — image / poster notices were never carded
# ---------------------------------------------------------------------------

POSTER_CAPTION = ("【Maintenance Notice】Dear partners, please find the maintenance "
                  "schedule in the image above.")


@case("F59", "a captioned poster with maintenance wording: ONE card at once, nothing written")
def _():
    fresh(BASE)
    for tick in range(4):
        r = sweep([msg(5, POSTER_CAPTION, kind="media-photo")])
        if tick == 0:
            check(actions(r) == ["image-notice"] and r["image_notice"] == 1, str(r["details"]))
    check(titles() == ["🖼️ Maintenance notice may be an image · Pragmatic Play"] and not WRITES,
          f"{titles()} {written()}")
    check("in its caption" in card_text() and "fill the row by hand" in card_text(), card_text())
    s = va.format_check_summary(dict(r, group=PPG, provider=PP))
    check("already handled: 1" in s, s)


@case("F59", "a captionless poster beside '【Maintenance Notice】' is carded once; a document too")
def _():
    for kind in ("media-photo", "document"):
        fresh(BASE)
        for _ in range(4):
            sweep([msg(5, "【Maintenance Notice】"), msg(6, "", kind=kind)])
        check(len(CARDS) == 1 and "in the message next to it" in card_text(), f"{kind}: {titles()}")
    fresh(BASE)                                    # poster first, wording after it
    sweep([msg(5, "", kind="media-photo"), msg(6, "系统维护通知，详情见上图")])
    check(len(CARDS) == 1 and CARDS[0][0].startswith("🖼️"), str(titles()))


@case("F59", "NOT carded: captionless photo alone, poster beside a readable notice, our own ask, sticker")
def _():
    for msgs, why in [
            ([msg(5, "", kind="media-photo")], "no wording anywhere"),
            ([msg(5, "Hi team, player complaint, see screenshot", kind="media-photo")],
             "caption not about maintenance"),
            ([msg(5, NOTICE_27), msg(6, "", kind="media-photo")], "illustrates a readable notice"),
            ([msg(5, "", kind="media-photo"), msg(6, NOTICE_27)], "illustrates a readable notice"),
            ([msg(5, "Any scheduled maintenance this week?", out=True),
              msg(6, "", kind="media-photo")], "the wording is our own question"),
            ([msg(5, "【Maintenance Notice】"), msg(6, "", kind="sticker")], "a sticker"),
            ([msg(5, "【Maintenance Notice】"), msg(6, "", kind="media-photo", out=True)],
             "a photo WE sent"),
            ([msg(5, "Maintenance completed. Thank you."), msg(6, "", kind="media-photo")],
             "beside a completion")]:
        fresh(BASE)
        sweep(msgs)
        check(not any(t.startswith("🖼️") for t in titles()), f"{why}: {titles()}")


@case("F59", "through the real telegramwarm worker: the scraped `kind` reaches vawatch")
def _():
    fresh(BASE)
    va.mark_baselined(PPG, PP)
    tgt = next(r for r in va.watch_list()["telegram"] if r["provider"] == PP)
    w = worker([msg(5, "【Maintenance Notice】", kind=None),
                msg(6, "", kind="media-photo")])
    box: dict = {}
    w._handle_va_watch({"kind": "va_watch", "box": box, "target": tgt})
    check(box.get("ok") and box["result"]["image_notice"] == 1, str(box))
    check(titles() == ["🖼️ Maintenance notice may be an image · Pragmatic Play"]
          and not WRITES, f"{titles()} {written()}")


@case("F59", "a group's first read records a poster as backlog; the shared group cards it once")
def _():
    fresh(BASE)
    sweep([msg(5, "【Maintenance Notice】"), msg(6, "", kind="media-photo")], baseline=False)
    sweep([msg(5, "【Maintenance Notice】"), msg(6, "", kind="media-photo")])
    check(not CARDS, f"backlog carded: {titles()}")
    fresh(BASE)
    for p in ("Hacksaw", "YGG"):
        sweep([msg(7, POSTER_CAPTION, kind="media-photo")], provider=p, group=SG)
    check(len(CARDS) == 1, f"shared group carded twice: {titles()}")
    fresh(BASE, env={"VAWATCH_IMAGE_CARD": "0"})
    sweep([msg(5, POSTER_CAPTION, kind="media-photo")])
    check(not any(t.startswith("🖼️") for t in titles()), f"kill-switch: {titles()}")


@case("F63", "a guard refusal (no downtime / UAT / question) is counted apart from 'not about maintenance'")
def _():
    fresh(BASE)
    r = sweep([msg(1, "Scheduled maintenance on 2026-09-25 10:00-12:00 (GMT+8). "
                      "There will be no downtime."),
               msg(2, "UAT environment maintenance 2026-09-25 10:00-12:00 (GMT+8)."),
               msg(3, "ok thanks")])
    check(r["ignored_refused"] == 2 and r["ignored_not_maintenance"] == 1
          and not WRITES and not CARDS, f"{r['ignored_refused']} {r['ignored_not_maintenance']}")
    text = va.format_check_summary(dict(r, group=PPG, provider=PP))
    check("ignored (about maintenance, but a question" in text and
          "ignored (not about maintenance): 1" in text, text)


# ---------------------------------------------------------------------------
# G3.2 (vawatch half) — "no maintenance this week" blanked next week's window
# ---------------------------------------------------------------------------

G32_NEXT = "Dear partners, scheduled maintenance on 01/10/2026 10:00-12:00 (GMT+8). Thank you."
G32_NONE = "No maintenance this week, thanks."


@case("G3.2", "clear ON: an upcoming window this watcher wrote is never blanked; carded")
def _():
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    sweep([msg(1, G32_NEXT)])
    WRITES.clear(); CARDS.clear()
    r = sweep([msg(1, G32_NEXT), msg(2, G32_NONE)])
    check(actions(r)[-1] == "clear-skipped" and not WRITES, f"{actions(r)} {written()}")
    check(len(CARDS) == 1 and "row was NOT cleared" in str(CARDS[0][1])
          and "2026-10-01 10:00" in str(CARDS[0][1]), str(CARDS))


@case("G3.2", "clear ON: a future window NOT from this watcher (read back) is kept; an ended one clears")
def _():
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    WRITES.append(("recPP", {"Start Time": va._ms(datetime(2026, 10, 1, 10, 0, tzinfo=TZ8)),
                             "End Time": va._ms(datetime(2026, 10, 1, 12, 0, tzinfo=TZ8))}))
    r = sweep([msg(2, G32_NONE)])
    check(actions(r) == ["clear-skipped"] and len(WRITES) == 1, f"{actions(r)} {written()}")
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    WRITES.append(("recPP", {"Start Time": va._ms(datetime(2026, 9, 16, 10, 0, tzinfo=TZ8)),
                             "End Time": va._ms(datetime(2026, 9, 16, 12, 0, tzinfo=TZ8))}))
    r = sweep([msg(2, G32_NONE)])
    check(actions(r) == ["cleared"] and WRITES[-1][1]["Start Time"] is None,
          f"last week's window was not cleared: {actions(r)} {WRITES[-1:]}")


@case("G3.2", "clear ON: a row that cannot be read back is left alone, not blanked blind")
def _():
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    real = va.get_row

    def _down(rid):
        raise RuntimeError("Lark 502 (simulated)")
    va.get_row = _down
    try:
        r = sweep([msg(2, G32_NONE)])
    finally:
        va.get_row = real
    check(actions(r) == ["clear-skipped"] and not WRITES, f"{actions(r)} {written()}")
    check("could not be read back" in str(CARDS[0][1]), str(CARDS))


# ---------------------------------------------------------------------------
# F67 (vawatch half) — a phased notice's later window vanished without a word
# ---------------------------------------------------------------------------

F67_TEXT = ("Scheduled maintenance (GMT+8)\nPhase 1: 2026-09-23 22:00 - 23:59\n"
            "Phase 2: 2026-09-25 00:00 - 02:00")


@case("F67", "phase 1 is written; the card and /vacheck name phase 2 as NOT written")
def _():
    fresh(BASE)
    r = sweep([msg(1, F67_TEXT)])
    check(written() == [("recPP", "2026-09-23 22:00", "2026-09-23 23:59")], str(written()))
    body = str(CARDS[0][1])
    check("1 later window(s), NOT written:** 2026-09-25 00:00 → 02:00 GMT+8" in body, body)
    text = va.format_check_summary(dict(r, group=PPG, provider=PP))
    check("also states 1 later window(s), NOT written: 2026-09-25 00:00 → 02:00" in text, text)
    # Nothing writes phase 2 later on its own: the row keeps phase 1.
    va._now_dt = lambda: NOW + timedelta(days=1)
    try:
        sweep([msg(1, F67_TEXT)])
    finally:
        va._now_dt = lambda: NOW
    check(len(WRITES) == 1, f"phase 2 was written: {written()}")


@case("F67", "a single-window notice carries no 'later window' line")
def _():
    fresh(BASE)
    r = sweep([msg(1, "Scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8).")])
    check("later window" not in str(CARDS[0][1]), str(CARDS[0][1]))
    check("later window" not in va.format_check_summary(dict(r, group=PPG, provider=PP)),
          "summary")


# ---------------------------------------------------------------------------
# G1.3 (vawatch half) — a clock-only notice was re-read 3 times, then dropped
# ---------------------------------------------------------------------------

@case("G1.3", "a clock with no date: unparsed twice, then ONE needs-human card, never written")
def _():
    for text in ("Emergency maintenance 15:00-16:00 (GMT+8)",
                 "Emergency maintenance (23/09) 15:00-16:00 (GMT+8)"):
        fresh(BASE)
        acts = [actions(sweep([msg(1, text)])) for _ in range(4)]
        check(acts == [["unparsed"], ["unparsed"], ["needs-human"], ["already handled"]],
              f"{text}: {acts}")
        check(len(CARDS) == 1 and not WRITES, f"{titles()} {written()}")


@case("G1.3", "a relative-date notice is carded on the FIRST read; an ack with no clock stays silent")
def _():
    fresh(BASE)
    r = sweep([msg(1, "Emergency maintenance today 15:00-16:00 (GMT+8)")])
    check(actions(r) == ["needs-human"] and len(CARDS) == 1 and not WRITES,
          f"{actions(r)} {titles()}")
    fresh(BASE)
    for _ in range(4):
        sweep([msg(1, "Thanks for the maintenance notice"),
               msg(2, "Please be informed of the scheduled maintenance.")])
    check(not CARDS and not WRITES, f"an ack was carded: {titles()}")


@case("G1.3", "the shared group cards a clock-only notice once; a failed card is retried as a card")
def _():
    fresh(BASE)
    text = "Emergency maintenance 15:00-16:00 (GMT+8)"
    for _ in range(3):
        shared_sweep(text)
    check(len(CARDS) == 1 and not WRITES, f"{titles()} {written()}")
    fresh(BASE, env={"VAWATCH_REPARSE_ATTEMPTS": "1"})
    real = va.send_card
    va.send_card = lambda chat, card: {"code": 99}
    va.send_text = lambda chat, text: {"code": 99}
    try:
        r = sweep([msg(1, text)])
    finally:
        va.send_card, va.send_text = real, _fake_text
    check(actions(r) == ["needs-human"] and not r["details"][0]["carded"], str(r["details"]))
    r = sweep([msg(1, text)])
    check(actions(r) == ["needs-human"] and len(CARDS) == 1, f"{actions(r)} {titles()}")


# ---------------------------------------------------------------------------
# F83 — config: garbage in a 1/0 flag, a zone that cannot load, VAWATCH_TZ
# ---------------------------------------------------------------------------

F83_NH = "Maintenance rescheduled, new time to be confirmed."


@case("F83", "'Y' / 'enabled' in a default-on flag keeps it ON, and says so in /vacheck")
def _():
    for bad in ("Y", "enabled"):
        fresh(BASE, env={"VAWATCH_NEEDS_HUMAN_CARD": bad})
        r = sweep([msg(1, F83_NH)])
        check(actions(r) == ["needs-human"] and len(CARDS) == 1 and not WRITES,
              f"{bad}: {actions(r)} {titles()} {written()}")
        text = va.format_check_summary(dict(r, group=PPG, provider=PP))
        check(f"config: VAWATCH_NEEDS_HUMAN_CARD='{bad}' is not 1/0" in text, text)
    fresh(BASE, env={"VAWATCH_TARGET_CACHE": "Y"})
    check(va._target_cache_enabled() is True, "VAWATCH_TARGET_CACHE=Y read as off")
    fresh(BASE)
    r = sweep([msg(1, F83_NH)])
    check("config:" not in va.format_check_summary(dict(r, group=PPG, provider=PP)),
          "a clean config printed a config line")


@case("F83", "'Y' in the default-OFF VAWATCH_CLEAR_ENABLED keeps it off: carded, never blanked")
def _():
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "Y"})
    check(va._clear_enabled() is False, "VAWATCH_CLEAR_ENABLED=Y read as on")
    for word, want in (("1", True), ("on", True), ("0", False), ("off", False)):
        os.environ["VAWATCH_CLEAR_ENABLED"] = word
        check(va._clear_enabled() is want, f"VAWATCH_CLEAR_ENABLED={word}")


@case("F83", "a NOTICE_TZ that cannot load reads zone-less notices at +08 and is reported")
def _():
    fresh(BASE, env={"NOTICE_TZ": "Asia/Kolkatta"})
    r = sweep([msg(1, "Scheduled maintenance on 2026-09-25 10:00 - 12:00.")])
    check(written() == [("recPP", "2026-09-25 10:00", "2026-09-25 12:00")], str(written()))
    text = va.format_check_summary(dict(r, group=PPG, provider=PP))
    check("config: NOTICE_TZ='Asia/Kolkatta' cannot be loaded" in text, text)


@case("F83", "VAWATCH_TZ is the clock zone only: it does not move the parse zone")
def _():
    fresh(BASE, env={"VAWATCH_TZ": "Asia/Kolkata"})
    sweep([msg(1, "Scheduled maintenance on 2026-09-25 10:00 - 12:00.")])
    check(written() == [("recPP", "2026-09-25 10:00", "2026-09-25 12:00")], str(written()))
    fresh(BASE, env={"VAWATCH_TZ": "Mars/Olympus"})
    check(va._tz().utcoffset(None) == timedelta(hours=8), "bad VAWATCH_TZ fallback")
    check(any("VAWATCH_TZ='Mars/Olympus'" in w for w in va.config_warnings()),
          str(va.config_warnings()))


@case("F83", "NOTICE_MAX_WINDOW_HOURS=inf / -1 / nan: no crash, the 48h cap still holds")
def _():
    for bad in ("inf", "-1", "nan", "1e30"):
        fresh(BASE, env={"NOTICE_MAX_WINDOW_HOURS": bad})
        r = sweep([msg(1, "Scheduled maintenance from 2026-09-25 10:00 to "
                          "2026-09-28 10:00 (GMT+8).")])
        check(not WRITES and actions(r) == ["needs-human"],
              f"{bad}: {actions(r)} {written()} {r.get('errors')}")

# ---------------------------------------------------------------------------
# W2 (audit2 round): follow-ups, cancellations and same-read retractions
# ---------------------------------------------------------------------------

def _body(i=0):
    c = CARDS[i][1]
    return c if isinstance(c, str) else "\n".join(
        e["text"]["content"] for e in c["body"]["elements"] if e.get("tag") == "div")


@case("doubt", "the parser's clean-notice check (refused='doubt') is carded, never written")
def _():
    # noticeparse._doubt turns a fill whose text carries an unaccounted cancel /
    # void / zone signal into needs_human with refused="doubt" and no window.
    # It must reach a person like any needs_human - also when its words are
    # not on the F58 maintenance-topic list.
    doubt = {"action": "needs_human", "refused": "doubt", "start": None, "end": None,
             "reason": "the notice reads as 2026-09-25 10:00 -> 12:00 (+08:00), but it "
                       "also says “void” - it may withdraw or correct the window",
             "reschedule": False, "stale": False, "others": []}
    for text in ("Games offline 25/09 10:00-12:00, void.",
                 "Scheduled maintenance 25/09/2026 10:00-12:00 (GMT+8). VOID"):
        fresh(BASE)
        saved = va.classify
        va.classify = lambda t, *a, _t=text, **k: dict(doubt) if t == _t else saved(t)
        try:
            sweep([msg(9, "hi")])
            r = sweep([msg(9, "hi"), msg(10, text)])
        finally:
            va.classify = saved
        check(not WRITES and actions(r)[-1] == "needs-human" and len(CARDS) == 1,
              f"{text!r}: {actions(r)} {written()} {titles()}")


@case("F51", "#70/#190: 'no longer valid' / 'on hold' / 'suspended' / 不进行了 cancel the row's window: carded")
def _():
    for text in ("The maintenance on 2026-09-25 10:00-12:00 (GMT+8) is no longer valid.",
                 "The maintenance on 2026-09-25 10:00-12:00 (GMT+8) will not happen.",
                 "The maintenance on 2026-09-25 10:00-12:00 (GMT+8) is on hold until further notice.",
                 "The maintenance on 2026-09-25 10:00-12:00 (GMT+8) has been suspended.",
                 "2026-09-25 10:00-12:00 的维护不进行了"):
        fresh(BASE)
        sweep([msg(9, "hi")])
        sweep([msg(9, "hi"), msg(10, NA)])
        CARDS.clear()
        r = sweep([msg(9, "hi"), msg(10, NA), msg(11, text)])
        check(actions(r)[-1] == "needs-human" and len(CARDS) == 1,
              f"{text!r}: {actions(r)} {titles()}")
        check("CANCELLED" in _body(), f"{text!r}: {_body()[:200]}")
        check(written() == [("recPP", "2026-09-25 10:00", "2026-09-25 12:00")], str(written()))


@case("G2.2", "#74/#76: a notice that cancels the row's window and names a new one replaces it")
def _():
    a = "Scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8)."
    for text in ("Scheduled maintenance: 26/09/2026 10:00-12:00 (GMT+8). Previously announced "
                 "24/09/2026 10:00-12:00 is cancelled.",
                 "24/09维护取消，改为 2026-09-26 10:00-12:00 (GMT+8) 进行系统维护。"):
        fresh(BASE)
        with at(datetime(2026, 9, 22, 12, 0, tzinfo=TZ8)):
            sweep([msg(9, "hi")])
            sweep([msg(9, "hi"), msg(10, a)])
            CARDS.clear()
            r = sweep([msg(9, "hi"), msg(10, a), msg(11, text)])
        check(row_now() == ("recPP", "2026-09-26 10:00", "2026-09-26 12:00"),
              f"{text!r}: {actions(r)} {written()}")
        check("Replaces 2026-09-24 10:00" in _body(), _body()[:300])
    # Control: a later notice that cancels nothing is still a SECOND outage.
    fresh(BASE)
    with at(datetime(2026, 9, 22, 12, 0, tzinfo=TZ8)):
        sweep([msg(9, "hi")])
        sweep([msg(9, "hi"), msg(10, a)])
        r = sweep([msg(9, "hi"), msg(10, a),
                   msg(11, "Scheduled maintenance: 26/09/2026 10:00-12:00 (GMT+8).")])
    check(actions(r)[-1] == "deferred" and len(WRITES) == 1, f"{actions(r)} {written()}")


@case("G2.3", "#77-#81: void / invalid / no longer valid / a test / 无效 / 以上通知作废 follow-ups are carded")
def _():
    a = "Scheduled maintenance on 24/09/2026 10:00-12:00 (GMT+8)."
    for text in ("The notice above is void.", "The above notice is invalid.",
                 "Please treat the above notice as void.",
                 "The previous notice is no longer valid.", "以上公告无效。",
                 "Sorry, that was a test."):
        fresh(BASE)
        with at(datetime(2026, 9, 22, 12, 0, tzinfo=TZ8)):
            sweep([msg(99, "hi")])
            sweep([msg(99, "hi"), msg(100, a)])
            CARDS.clear()
            r = sweep([msg(99, "hi"), msg(100, a), msg(101, text)])
        check(actions(r)[-1] == "needs-human" and len(CARDS) == 1,
              f"{text!r}: {actions(r)} {titles()}")
        check("2026-09-24 10:00" in _body(), _body()[:200])
    # #81: the notice has scrolled off the read; the follow-up names THE NOTICE.
    tickets = [msg(101 + k, f"ticket #{k}") for k in range(10)]
    for text, want in (("以上通知作废", 1), ("Please ignore the notice I sent earlier, sorry.", 1),
                       ("Please ignore", 0)):
        fresh(BASE)
        with at(datetime(2026, 9, 22, 12, 0, tzinfo=TZ8)):
            sweep([msg(99, "hi")])
            sweep([msg(99, "hi"), msg(100, a)])
            sweep([msg(100, a)] + tickets[:7])
            CARDS.clear()
            r = sweep(tickets[3:] + [msg(120, text)])
        check(len(CARDS) == want, f"{text!r}: {actions(r)} {titles()}")


@case("F58", "#210: 'no maintenance from our end, kindly check your IT' is a ticket reply, not a clear")
def _():
    for text in ("Hi, no maintenance from our end, kindly check with your IT.",
                 "No maintenance from our side, kindly check your connection.",
                 "No maintenance at our end, please contact your IT team.",
                 "There is no maintenance currently, please check your connection."):
        fresh(BASE)
        sweep([msg(9, "hi")])
        sweep([msg(9, "hi"), msg(10, NA)])
        CARDS.clear()
        r = sweep([msg(9, "hi"), msg(10, NA), msg(11, text)])
        check(not CARDS and actions(r)[-1] == "ignored", f"{text!r}: {actions(r)} {titles()}")
    fresh(BASE)
    sweep([msg(9, "hi")])
    r = sweep([msg(9, "hi"), msg(10, "No maintenance this week.")])
    check(actions(r)[-1] == "clear-skipped" and len(CARDS) == 1, f"{actions(r)} {titles()}")


N25 = "Dear partners, scheduled maintenance on 2026-09-25 10:00 - 12:00 (GMT+8)."


@case("F56", "#206/#207/#229: 'wrong group' / 'meant for another group' in the SAME read: carded, never written")
def _():
    for follow in ("Sorry wrong group", "[VOID] wrong group", "Oops, wrong group, please ignore",
                   "发错群了", "Sorry, this was meant for another group.",
                   "Sorry, wrong group. Please ignore.", "Wrong chat, sorry!", "发错群了，请忽略"):
        fresh(BASE)
        sweep([msg(9, "hi")])
        r = sweep([msg(9, "hi"), msg(10, N25), msg(11, follow)])
        check(not WRITES and len(CARDS) == 1, f"{follow!r}: {actions(r)} {written()} {titles()}")
        check("NOT written" in _body(), _body()[:200])


@case("F56", "#209: a completion under a notice whose window has STARTED, in the same read: not written")
def _():
    e = "Dear partners, emergency maintenance on 2026-09-23 10:00 - 14:00 (GMT+8)."
    for follow in ("Maintenance completed early. All games are back online.",
                   "维护已完成，游戏已恢复正常。", "Done, thanks for waiting."):
        fresh(BASE)
        sweep([msg(9, "hi")])
        r = sweep([msg(9, "hi"), msg(10, e), msg(11, follow)])
        check(not WRITES and len(CARDS) == 1, f"{follow!r}: {actions(r)} {written()} {titles()}")
    # A FUTURE window under an unrelated "done" is still written.
    fresh(BASE)
    sweep([msg(9, "hi")])
    sweep([msg(9, "hi"), msg(10, N25), msg(11, "Done, thanks for waiting.")])
    check(written() == [("recPP", "2026-09-25 10:00", "2026-09-25 12:00")], str(written()))


@case("F36", "#231: a 'Correction: ... 26/09, not 25 Sep' below the notice stops force and the same read")
def _():
    corr = "Correction: the maintenance is on 2026-09-26 10:00 - 12:00 (GMT+8), not 25 Sep."
    for force in (True, False):
        fresh(BASE)
        sweep([msg(9, "hello")])
        r = sweep([msg(9, "hello"), msg(10, NA), msg(11, corr)], force=force)
        check(not WRITES and len(CARDS) == 1, f"force={force}: {actions(r)} {written()} {titles()}")
        check("CORRECTED (to 2026-09-26 10:00" in _body(), _body()[:300])


@case("F56", "#208: /vacheck force stops at 'Done, thanks for waiting' / 'games are up' / 已恢复")
def _():
    e = "Dear partners, emergency maintenance on 2026-09-23 10:00 - 14:00 (GMT+8)."
    for follow in ("Done, thanks for waiting.", "All good now, games are up.",
                   "Maintenance done ✅", "已恢复"):
        fresh(BASE)
        with at(datetime(2026, 9, 23, 9, 0, tzinfo=TZ8)):
            sweep([msg(9, "hi")])
            sweep([msg(9, "hi"), msg(10, e)])
        WRITES.clear()
        r = sweep([msg(9, "hi"), msg(10, e), msg(11, follow)], force=True)
        check(not WRITES, f"{follow!r}: {actions(r)} {written()}")
        check("completed" in r.get("force_stop", ""), r.get("force_stop"))


@case("F34", "#230: a retraction / 'will NOT have maintenance' read FIRST blocks the older notice seen later")
def _():
    for last in ("Sorry, wrong group. Please ignore.",
                 "Update: we will NOT have maintenance on Friday."):
        fresh(BASE)
        hist = ([msg(20, N25)] + [msg(21 + k, f"ok thanks {k}") for k in range(8)]
                + [msg(30, last)])
        sweep([msg(19, "hi")])
        sweep(hist[-3:])
        r = sweep(hist)
        check(not WRITES and actions(r)[0] == "needs-human",
              f"{last!r}: {actions(r)} {written()}")


@case("F40", "#233: an EMPTY first read does not spend the careful sweep on a capped, keyless scope")
def _():
    fresh(BASE)
    va.mark_baselined("JILI CS", "JILI")
    d = va._load()
    for i in range(va._LEDGER_CAP):
        k = f"busy0@busy0 cs|mid:{10000 + i}"
        d["handled"][k] = {"outcome": "ignored", "at": "2026-09-20 10:00:00", "attempts": 1}
        d["order"].append(k)
    with va._ledger_lock:
        check(va._save(d), "setup save")
    r = sweep([], provider="JILI", group="JILI CS")
    check(r["cold_start"] and not scope("JILI", "JILI CS").get("revive_sweep"),
          f"empty read stamped the careful sweep: {scope('JILI', 'JILI CS')}")
    screen = [msg(499, "ok thanks"),
              msg(500, "Scheduled maintenance on 2026-09-28 02:00 - 04:00 (GMT+8)."),
              msg(501, "noted")]
    r = sweep(screen, provider="JILI", group="JILI CS")
    check(r["cold_start"] and not WRITES and not CARDS,
          f"revived after an empty read: {actions(r)} {written()} {titles()}")


@case("R1.52", "#273: a chat re-created after only a few dozen messages is still a restart")
def _():
    n = "Dear partners, scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8)."
    for old, new in (((40, 48), [msg(1, "This group was upgraded"), msg(2, n)]),
                     ((10, 18), [msg(1, "This group was upgraded"), msg(2, n)]),
                     ((30, 38), [msg(k, f"chat {k}") for k in range(1, 12)] + [msg(12, n)])):
        fresh(BASE)
        cold = [msg(k, "chat i") for k in range(*old)]
        sweep(cold, baseline=False)
        sweep(cold + [msg(old[1], "ok")])
        r = sweep(new)
        check(written() == [("recPP", "2026-09-27 02:00", "2026-09-27 04:00")]
              and r.get("mid_restart") == 1, f"{old}: {actions(r)} {written()}")
    # A quiet group whose newest bubbles were deleted is NOT a restart.
    fresh(BASE)
    cold = [msg(k, "chat i") for k in range(40, 48)]
    sweep(cold, baseline=False)
    sweep(cold + [msg(48, "ok")])
    r = sweep([msg(k, "chat i") for k in range(38, 46)])
    check(not r.get("mid_restart"), str(r.get("mid_restart")))


@case("F76", "#60: valid JSON of the wrong SHAPE is moved aside and re-baselined, not a crash every tick")
def _():
    import json as _json
    for patch in ({"handled": []}, {"handled": None}, {"scopes": []}, {"scopes": None},
                  {"baselined": []}, {"baselined": "x"}, {"scopes": {"a@b": []}}):
        fresh(BASE)
        sweep([msg(1, "hi")])
        d = _json.loads(va.LEDGER_PATH.read_text(encoding="utf-8"))
        d.update(patch)
        va.LEDGER_PATH.write_text(_json.dumps(d), encoding="utf-8")
        r = sweep([msg(1, "hi")], baseline=False)
        check(r["cold_start"] and not r["errors"], f"{patch}: {r['details']}")
        check(va.LEDGER_PATH.with_name(va.LEDGER_PATH.name + ".bad").exists(),
              f"{patch}: not moved aside")
        r = sweep([msg(1, "hi"), msg(2, "Scheduled maintenance 2026-09-25 10:00-12:00 (GMT+8)")])
        check(written() == [("recPP", "2026-09-25 10:00", "2026-09-25 12:00")],
              f"{patch}: after re-baseline {actions(r)} {written()}")


@case("G2.5", "#88: an edit loop A -> X -> A -> X re-writes X; the bubble's latest text wins")
def _():
    a = "Scheduled maintenance notice: 24/09/2026 10:00-12:00 (GMT+8). All games will be unavailable."
    x = "Scheduled maintenance notice: 24/09/2026 14:00-16:00 (GMT+8). All games will be unavailable."
    fresh(BASE)
    seq = [(12, 0, a, False), (12, 30, x, True), (13, 0, a, True), (13, 30, x, True)]
    for hh, mm, t, ed in seq:
        with at(datetime(2026, 9, 22, hh, mm, tzinfo=TZ8)):
            if (hh, mm) == (12, 0):
                sweep([msg(99, "hi")])
            r = sweep([msg(99, "hi"), msg(100, t, edited=ed)])
    check(row_now() == ("recPP", "2026-09-24 14:00", "2026-09-24 16:00") and len(CARDS) == 4,
          f"{actions(r)} {written()} {titles()}")
    # ...and a re-render of that same latest text is still "already handled".
    with at(datetime(2026, 9, 22, 14, 0, tzinfo=TZ8)):
        r = sweep([msg(99, "hi"), msg(100, x, edited=True)])
    check(actions(r)[-1] == "already handled" and len(WRITES) == 4, f"{actions(r)} {written()}")


@case("F36", "#232/#271: cold group, force writes 10-01; the older SEPARATE 09-25 outage is carded")
def _():
    fresh(BASE)
    older = msg(10, NA)                   # 2026-09-25, still ahead
    newer = msg(11, "Scheduled maintenance on 2026-10-01 10:00 - 12:00 (GMT+8).")
    va.handle_messages([msg(9, "hello"), older, newer], provider=PP, group=PPG,
                       record_id="recPP", force=True)
    check(written() == [("recPP", "2026-10-01 10:00", "2026-10-01 12:00")], str(written()))
    check(len(CARDS) == 2 and "2026-09-25 10:00" in _body(1) and "NOT" in _body(1),
          f"{titles()} {_body(1)[:200] if len(CARDS) > 1 else ''}")
    r = va.handle_messages([msg(9, "hello"), older, newer], provider=PP, group=PPG,
                           record_id="recPP")
    check(actions(r) == ["already handled"] * 3 and len(CARDS) == 2, f"{actions(r)} {titles()}")


@case("F69", "#154/#155: after a rename, a new cancellation / 'postponed, TBA' / poster is carded, not buried")
def _():
    rows = [dict(r) for r in BASE] + [row("Habanero", HAB_OLD, "recHAB")]
    n27 = "Habanero scheduled maintenance on 2026-09-27 01:00 - 03:00 GMT+8"
    for text, kw, title in (
            ("The Habanero maintenance on 2026-09-27 01:00 - 03:00 (GMT+8) is cancelled.", {},
             "needs a human"),
            ("Habanero 2026-09-27 01:00-03:00 的维护取消。", {}, "needs a human"),
            ("Habanero maintenance is postponed, new schedule to be announced.", {},
             "needs a human"),
            ("Maintenance notice 👇", {"kind": "media-photo"}, "image")):
        fresh(rows)
        sweep([msg(99, "hi")], provider="Habanero", group=HAB_OLD)
        sweep([msg(99, "hi"), msg(100, n27)], provider="Habanero", group=HAB_OLD)
        WRITES.clear(); CARDS.clear()
        ROWS[:] = rows[:-1] + [row("Habanero", HAB_NEW, "recHAB")]
        va.watch_list()
        r = sweep([msg(99, "hi"), msg(100, n27), msg(110, text, **kw)],
                  provider="Habanero", group=HAB_NEW, baseline=False)
        check(r["renamed_from"] and not WRITES and len(CARDS) == 1 and title in titles()[0],
              f"{text!r}: {actions(r)} {titles()} {written()}")
        ROWS[:] = rows


@case("G1.3", "#57: an outage timed by 'now' / a duration (no clock) is carded at the reparse cap")
def _():
    for text in ("紧急维护：现在开始，约1小时",
                 "Emergency maintenance in 30 minutes, for 1 hour",
                 "Emergency maintenance ongoing, ETA 2 hours."):
        fresh(BASE)
        acts = [actions(sweep([msg(1, text)])) for _ in range(4)]
        check(acts == [["unparsed"], ["unparsed"], ["needs-human"], ["already handled"]],
              f"{text}: {acts}")
        check(len(CARDS) == 1 and not WRITES, f"{titles()} {written()}")


@case("F59", "#132-#135: split notices across a reply / our bubble / a sticker / three bubbles are carded")
def _():
    det = "Date: 2026-09-26\nTime: 02:00 - 04:00 (GMT+8)"
    for name, parts in (
            ("reply between", [msg(5, "【Maintenance Notice】"), msg(6, "noted"), msg(7, det)]),
            ("zh reply between", [msg(5, "【维护通知】"), msg(6, "收到"),
                                  msg(7, "日期：2026-09-26\n时间：02:00-04:00 (GMT+8)")]),
            ("our bubble between", [msg(5, "【Maintenance Notice】"), msg(6, "noted", out=True),
                                    msg(7, det)]),
            ("sticker between", [msg(5, "【Maintenance Notice】"), msg(6, "", kind="sticker"),
                                 msg(7, det)]),
            ("three bubbles", [msg(5, "【Maintenance Notice】"), msg(6, "Date: 2026-09-26"),
                               msg(7, "Time: 02:00 - 04:00 (GMT+8)")]),
            ("undated join", [msg(5, "Maintenance notice for PP"),
                              msg(6, "Games affected: all\nStart: 2026-09-26 02:00\n"
                                     "End: 2026-09-26 04:00 (GMT+8)")])):
        fresh(BASE)
        sweep([msg(1, "hi")])
        runs = [actions(sweep([msg(1, "hi")] + parts)) for _ in range(4)]
        check(not WRITES and len(CARDS) == 1 and any("needs-human" in a for a in runs),
              f"{name}: {runs} {titles()} {written()}")
        check("split over" in _body(), f"{name}: {_body()[:200]}")


@case("R1.79", "#297: a pointer two bubbles from its attachment, a short caption, or 'see above' is carded")
def _():
    for name, parts in (("caption 👇", [msg(11, "Please see below"), msg(12, "👇", kind="photo")]),
                        ("caption Notice", [msg(11, "Please see below"),
                                            msg(12, "Notice", kind="photo")]),
                        ("our reply between", [msg(11, "Please see below"),
                                               msg(12, "ok", out=True),
                                               msg(13, "", kind="photo")]),
                        ("photo first", [msg(10, "", kind="photo"), msg(11, "Please see above")])):
        fresh(BASE)
        sweep([msg(1, "hi")])
        runs = [actions(sweep([msg(1, "hi")] + parts)) for _ in range(3)]
        check(len(CARDS) == 1 and not WRITES and "attachment" in _body(),
              f"{name}: {runs} {titles()}")


@case("F63", "#142: a QUESTION that states a window is carded, never written; one naming none stays silent")
def _():
    for text in ("Maintenance on 25/09/2026 14:00-16:00 (GMT+8)? Please confirm.",
                 "游戏将于 2026年9月25日 14:00-16:00 (GMT+8) 维护？"):
        fresh(BASE)
        sweep([msg(1, "hi")])
        r = sweep([msg(1, "hi"), msg(2, text)])
        check(actions(r)[-1] == "needs-human" and len(CARDS) == 1 and not WRITES,
              f"{text!r}: {actions(r)} {titles()} {written()}")
        check("QUESTION" in _body() and "2026-09-25 14:00" in _body(), _body()[:200])
    fresh(BASE)
    sweep([msg(1, "hi")])
    r = sweep([msg(1, "hi"), msg(2, "Could you confirm the maintenance window?")])
    check(not CARDS and r["ignored_refused"] == 1, f"{actions(r)} {titles()}")


@case("F71", "audit-3: a text fallback that timed out, or a card dropped mid-response, is never re-posted")
def _():
    import urllib3
    sends: list = []

    def _card_conn(chat, card):
        sends.append("card")
        raise _rq.exceptions.ConnectionError("simulated: connect refused")

    def _text_timeout(chat, text):
        sends.append("text")
        raise _rq.exceptions.ReadTimeout("simulated: sent, never answered")

    fresh(BASE)
    with _Sinks(send_card=_card_conn, send_text=_text_timeout):
        r = sweep([msg(5, "Scheduled maintenance on 2026-10-25 10:00 - 12:00 (GMT+8).")])
        check(r["acted"] == 1 and sends == ["card", "text"], f"{sends} {r['details']}")
        with at(NOW + timedelta(hours=1)):
            r = sweep([msg(5, "Scheduled maintenance on 2026-10-25 10:00 - 12:00 (GMT+8)."),
                       msg(6, "ok")])
    check(sends == ["card", "text"] and not r["announced_late"],
          f"re-posted: {sends} {r['details']}")
    # A card whose connection dropped AFTER it went out: no text fallback.
    sends.clear()
    dropped = _rq.exceptions.ConnectionError(urllib3.exceptions.ProtocolError(
        "Connection aborted.", ConnectionResetError(54, "reset by peer")))

    def _card_dropped(chat, card):
        sends.append("card")
        raise dropped
    fresh(BASE)
    with _Sinks(send_card=_card_dropped, send_text=_text_timeout):
        r = sweep([msg(5, "Scheduled maintenance on 2026-10-25 10:00 - 12:00 (GMT+8).")])
    check(sends == ["card"] and r["card_unknown"] == 1, f"{sends} {r.get('card_unknown')}")
    # ...while a card refused before anything was sent still falls back to text.
    sends.clear()
    fresh(BASE)
    with _Sinks(send_card=_card_conn, send_text=lambda c, t: sends.append("text") or {"code": 0}):
        sweep([msg(5, "Scheduled maintenance on 2026-10-25 10:00 - 12:00 (GMT+8).")])
    check(sends == ["card", "text"], str(sends))


@case("F37", "audit-4/5: .env.example documents the CURRENT alias / other-studio / operator defaults")
def _():
    import re as _re
    env = (REPO / ".env.example").read_text(encoding="utf-8")
    for name, default in (("VAWATCH_PROVIDER_ALIASES", va._DEFAULT_PROVIDER_ALIASES),
                          ("VAWATCH_OTHER_STUDIOS", va._DEFAULT_OTHER_STUDIOS),
                          ("VAWATCH_OPERATOR_NAMES", va._DEFAULT_OPERATOR_NAMES)):
        m = _re.search(r"^# " + name + r"=(.*)$", env, _re.M)
        check(m and m.group(1).strip() == default,
              f"{name}: documented {m.group(1).strip() if m else None!r} != code {default!r}")
    # Uncommenting the documented alias line keeps the R1.60 guard for PP.
    m = _re.search(r"^# VAWATCH_PROVIDER_ALIASES=(.*)$", env, _re.M)
    fresh([row("PP", "PP - IGO PR [A-SW-S/LC][A-SPE14-2117]", "recPP")],
          env={"VAWATCH_PROVIDER_ALIASES": m.group(1).strip()})
    g = "PP - IGO PR [A-SW-S/LC][A-SPE14-2117]"
    sweep([msg(1, "hi")], provider="PP", group=g)
    r = sweep([msg(1, "hi"), msg(2, "Dear Pragmatic team, our site will be under scheduled "
                                    "maintenance on 2026-09-25 10:00-12:00 (GMT+8).")],
              provider="PP", group=g)
    check(not WRITES and actions(r)[-1] == "needs-human", f"{actions(r)} {written()}")


def main(argv=None) -> int:
    want = [a for a in (argv if argv is not None else sys.argv[1:])]
    sel = [t for t in TESTS if not want or any(w.lower() in t[0].lower() for w in want)]
    fence = _files_fenced()
    if fence:
        print(f"REFUSING TO RUN: {fence}")
        return 1
    failed = 0
    for fid, name, fn in sel:
        before = len(NET), len(TOKEN)
        try:
            fn()
            leak = (len(NET), len(TOKEN)) != before
            if leak:
                raise AssertionError(f"network reached: {NET[before[0]:]} token calls "
                                     f"{len(TOKEN) - before[1]}")
            print(f"PASS  {fid:<5} {name}")
        except Exception as err:     # noqa: BLE001
            failed += 1
            print(f"FAIL  {fid:<5} {name}\n      {type(err).__name__}: {err}")
            if os.getenv("SELFTEST_TRACE"):
                traceback.print_exc()
        finally:
            _scrub_env()
    print("-" * 78)
    print(f"{len(sel) - failed} of {len(sel)} tests pass. Network attempts: {len(NET)}. "
          f"Tenant-token requests: {len(TOKEN)}. Lark sends: 0 (all sinks are fakes).")
    if NET or TOKEN:
        print(f"NETWORK WAS REACHED: {NET}")
        return 1
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
