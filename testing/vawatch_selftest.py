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
that raises (F82). File faults are simulated by wrapping builtins.open around
the tempdir ledger only; the clock is pinned with ``at(...)``.

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


va._tenant_token = _token
va.send_card = _fake_card
va.send_text = _fake_text
va.update_row = lambda rid, fields: (WRITES.append((rid, dict(fields))), {"code": 0})[1]
va.find_provider_row = _fake_find
va.classify = lambda text, *a, **k: noticeparse.classify(text, now=NOW)
# The watcher's own clock (owner/pending rules) pinned to the same instant, or
# "is the row's window still ahead?" would be asked of the real date.
if hasattr(va, "_now_dt"):
    va._now_dt = lambda: NOW
_TMP = Path(tempfile.mkdtemp(prefix="vawatch_selftest_"))
va.LEDGER_PATH = _TMP / "vawatch.json"

import telegramwarm as tw  # noqa: E402  (defines only; nothing starts at import)

tw.send_text = _blocked
_scrub_env()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SG = "[SG190- IGO Casinoplus YG/ RG/ HS] CS group"
PP, PPG = "Pragmatic Play", "Pragmatic Play x Casinoplus CS"


def fresh(rows=None, env=None) -> None:
    """Empty ledger, empty sinks, the given Base rows, only the given env."""
    WRITES.clear(); CARDS.clear(); FINDS.clear()
    _scrub_env()
    for k, v in (env or {}).items():
        os.environ[k] = v
    va._LAST_WATCH.clear()
    if hasattr(va, "_CURSOR_MEM"):
        va._CURSOR_MEM = None       # a failed-save cursor must not leak between tests
    for p in _TMP.glob("vawatch*"):
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
    r = sweep([msg(1, "Postponed: new window to be advised.")])
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
    fresh(BASE, env={"VAWATCH_SKIP_OUTBOUND": "0", "VAWATCH_OWNER_CHECK": "0"})
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


@case("G3.1", "a newer 'no maintenance' drops the deferred outage instead of writing it later")
def _():
    fresh(BASE, env={"VAWATCH_CLEAR_ENABLED": "1"})
    sweep([msg(1, G31_A), msg(2, G31_B)])
    sweep([msg(1, G31_A), msg(2, G31_B), msg(3, "Hi team, no maintenance this week.")])
    check(not scope().get("pending"), str(scope().get("pending")))


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


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    want = [a for a in (argv if argv is not None else sys.argv[1:])]
    sel = [t for t in TESTS if not want or any(w.lower() in t[0].lower() for w in want)]
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
