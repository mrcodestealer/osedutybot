#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""The Teams provider reader (teamswatch -> vawatch) - offline self-test.

  python testing/teams_provider_test.py        exit 1 on any failure

What it covers:
  * vawatch: the APP=TEAMS rows carried with record_id; the on/off switch;
    /vacheck's Teams target; card footers labelled Teams; the ledger lock that
    serialises a Teams sweep against a Telegram one (with a lost-update race);
  * peerstore: Teams conversation-id pins, kept apart from Telegram peer ids;
  * teamswatch: pin + exact-title verification before AND after the scrape,
    every refusal path, the row -> vawatch mapping (our own messages, edits),
    the worker job, the rotation, failure alerts, the /vacheck reply;
  * the real scraper JS, in headless Chromium on a Teams-shaped page - it also
    feeds the EVO watcher, so its existing fields are checked unchanged;
  * groupcheck pinning the Teams id, and main.py's /vacheck routing.

Never touches the network (requests and sockets are blocked), Lark (every
sender is a recorder), the real ledger / providerask state / pin store (all in
a tempdir), or a real Teams profile (the browser part is faked or a local page).
"""
from __future__ import annotations

import importlib.util
import json
import os
import socket
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

NET: list = []


def _blocked(*a, **k):
    NET.append(str(a[:1])[:80])
    raise RuntimeError("NETWORK BLOCKED by teams_provider_test")


import requests  # noqa: E402

requests.get = requests.post = requests.put = requests.patch = _blocked
requests.delete = requests.request = _blocked
requests.Session.request = lambda self, *a, **k: _blocked(*a, **k)
_real_connect = socket.socket.connect


def _connect(self, addr, *a, **k):
    # Playwright talks to its own driver over localhost; nothing else may connect.
    host = addr[0] if isinstance(addr, tuple) else str(addr)
    if host in ("127.0.0.1", "::1", "localhost"):
        return _real_connect(self, addr, *a, **k)
    return _blocked(addr)


socket.socket.connect = _connect


def _scrub_env() -> None:
    for k in list(os.environ):
        if k.startswith(("VAWATCH_", "NOTICE_", "GROUPCHECK_", "PROVIDERASK_", "EVOTEAMS_",
                         "TEAMS_SELF")) or k in ("APP_ID", "APP_SECRET", "LABORATORY_GROUP"):
            del os.environ[k]


_scrub_env()

_spec = importlib.util.spec_from_file_location("groupcheck", REPO / "groupcheck.py")
gc = importlib.util.module_from_spec(_spec)
sys.modules["groupcheck"] = gc
_spec.loader.exec_module(gc)
ROWS: list = []
gc.fetch_rows = lambda: [dict(r, apps=set(r.get("apps") or {"telegram"})) for r in ROWS]
GC_SENT: list = []
gc._tenant_token = _blocked
gc.send_text = lambda chat, text: GC_SENT.append(("text", text)) or {"code": 0}
gc.send_card = lambda chat, card: GC_SENT.append(("card", card)) or {"code": 0}
gc.upload_image_lark = lambda path: None

import noticeparse  # noqa: E402
import vawatch as va  # noqa: E402

_scrub_env()
TZ8 = timezone(timedelta(hours=8))
NOW = datetime(2026, 9, 23, 12, 0, tzinfo=TZ8)
WRITES: list = []
CARDS: list = []
TEXTS: list = []


def _fake_get_row(rid):
    f: dict = {}
    for r, fl in WRITES:
        if r == rid:
            f.update(fl)
    return f


CONFIRM_DELAY = [0.0]


def _confirm(*a, **k):
    if CONFIRM_DELAY[0]:
        time.sleep(CONFIRM_DELAY[0])
    return {"verdict": "yes", "transient": False, "why": "test stub"}


va._tenant_token = _blocked
va._llm_confirm = _confirm
va.send_card = lambda chat, card: CARDS.append(card) or {"code": 0}
va.send_text = lambda chat, text: TEXTS.append(text) or {"code": 0}
va.update_row = lambda rid, fields: (WRITES.append((rid, dict(fields))), {"code": 0})[1]
va.find_provider_row = lambda p: {"record_id": "rec_" + p, "fields": {}}
if hasattr(va, "get_row"):
    va.get_row = _fake_get_row
va.classify = lambda text, *a, **k: noticeparse.classify(text, now=NOW)
va._now_dt = lambda: NOW
_TMP = Path(tempfile.mkdtemp(prefix="teams_provider_test_"))
va.LEDGER_PATH = _TMP / "vawatch.json"

import providerask as pa  # noqa: E402

pa.STATE_PATH = _TMP / "providerask_state.json"

import peerstore  # noqa: E402

peerstore.STORE_PATH = _TMP / "provider_peers.json"

import teamswatch as tws  # noqa: E402  (nothing starts at import)

tws.send_text = _blocked
_scrub_env()

for name, p in (("ledger", va.LEDGER_PATH), ("providerask", pa.STATE_PATH),
                ("peerstore", peerstore.STORE_PATH)):
    assert _TMP.resolve() in Path(p).resolve().parents, f"{name} not fenced: {p}"

FAILS: list = []


def check(name, cond, detail=""):
    print(("PASS  " if cond else "FAIL  ") + name + ("" if cond else f"   -> {detail}"))
    if not cond:
        FAILS.append(name)


def row(provider, group, rid=None, apps=("telegram",)):
    return {"provider": provider, "group": group, "record_id": rid or "rec" + provider,
            "apps": set(apps)}


PP, PPG = "Pragmatic Play", "Pragmatic Play x Casinoplus CS"
GEM, GEMG = "GEMINI", "GEMINI x CasinoPlus"
PGS, PGSG = "PG Soft", "PG Soft Support Casinoplus"
RTG, RTGG = "RTG", "RTG CS group"
BASE = [row(PP, PPG, "recPP"), row("JILI", "JILI CS", "recJILI"),
        row(GEM, GEMG, "recGEM", apps=("teams",)), row(PGS, PGSG, "recPGS", apps=("teams",)),
        row(RTG, RTGG, "recRTG", apps=("teams",))]
THREAD = {GEMG: "19:aaaa1111@thread.v2", PGSG: "19:bbbb2222@thread.v2",
          RTGG: "19:cccc3333@thread.skype"}
ON = {"EVOTEAMS_ENABLED": "1"}


def fresh(rows=None, env=None):
    WRITES.clear(); CARDS.clear(); TEXTS.clear(); GC_SENT.clear()
    CONFIRM_DELAY[0] = 0.0
    _scrub_env()
    for k, v in (env or {}).items():
        os.environ[k] = v
    va._LAST_WATCH.clear()
    if hasattr(va, "_LAST_SOURCE"):
        va._LAST_SOURCE.clear()
    if hasattr(va, "_CURSOR_MEM"):
        va._CURSOR_MEM = None
    for p in list(_TMP.glob("vawatch*")) + list(_TMP.glob("providerask*")) + list(_TMP.glob("provider_peers*")):
        p.unlink()
    ROWS[:] = [dict(r) for r in (rows if rows is not None else BASE)]


def msg(mid, text, **kw):
    m = {"mid": str(mid), "text": text, "edited": False, "out": False, "sender": "", "time": ""}
    m.update(kw)
    return m


NOTICE = "Dear partner, scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8). Games will be unavailable."

# ===================================================================== vawatch
fresh()
for env, want in (({}, False), (ON, True), (dict(ON, VAWATCH_ENABLED="0"), False),
                  (dict(ON, VAWATCH_ENABLED=""), True), (dict(ON, VAWATCH_ENABLED="1"), True),
                  (dict(ON, VAWATCH_ENABLED="garbage"), False),
                  (dict(ON, VAWATCH_TEAMS_ENABLED="0"), False), (dict(ON, VAWATCH_TEAMS_ENABLED="off"), False),
                  ({"EVOTEAMS_ENABLED": "0"}, False)):
    fresh(env=env)
    check(f"teams_watch_enabled {env} -> {want}", va.teams_watch_enabled() is want)

fresh(env=ON)
va.mark_teams_reader_alive(120)
rep = va.watch_list()
check("watch_list: TEAMS rows carried with record_id",
      [(r["provider"], r["group"], r["record_id"]) for r in rep["teams_rows"]]
      == [(GEM, GEMG, "recGEM"), (PGS, PGSG, "recPGS"), (RTG, RTGG, "recRTG")], rep.get("teams_rows"))
check("watch_list: TEAMS rows are not in the Telegram rotation",
      {r["provider"] for r in rep["telegram"]} == {PP, "JILI"}, rep["telegram"])
check("watch_list: shared_with annotated on TEAMS rows",
      all("shared_with" in r for r in rep["teams_rows"]), rep["teams_rows"])
check("watch_list: TEAMS note says they are read now",
      all("read by the Teams provider watcher" in n["why"] for n in rep["teams"]), rep["teams"])
fresh()
check("watch_list, reader off: note says it is not running",
      all("not running" in n["why"] for n in va.watch_list()["teams"]))
fresh(env=ON)
va._TEAMS_READER.update(at=0.0, every=0.0)
check("switch on but the loop never ticked (no Teams profile): not 'running'",
      not va.teams_reader_running() and all("not running" in n["why"] for n in va.watch_list()["teams"]))
va.mark_teams_reader_alive(120)
check("after a heartbeat: running", va.teams_reader_running())
va._TEAMS_READER["at"] -= 3 * 120 + 301
check("a heartbeat older than 3 intervals + 5 min: not running", not va.teams_reader_running())

fresh(BASE + [row("", "No provider teams chat", "recBLANK", apps=("teams",))], env=ON)
rep = va.watch_list()
check("blank-Provider TEAMS row: not read, listed as skipped",
      "No provider teams chat" not in [r["group"] for r in rep["teams_rows"]]
      and any(s["group"] == "No provider teams chat" for s in rep["skipped"]), rep)

fresh(env=ON)
va.watch_list()                                   # seeds the cache
ROWS[:] = []
gc.fetch_rows, _real_fetch = (lambda: (_ for _ in ()).throw(RuntimeError("Base 503"))), gc.fetch_rows
try:
    rep = va.watch_list()
finally:
    gc.fetch_rows = _real_fetch
check("watch_list from the cache still carries the TEAMS rows",
      rep["source"] == "cache" and [r["provider"] for r in rep["teams_rows"]] == [GEM, PGS, RTG], rep)

fresh(env=ON)
check("manual_teams_target by provider", va.manual_teams_target("gemini").get("record_id") == "recGEM")
check("manual_teams_target by exact group", va.manual_teams_target(RTGG).get("provider") == RTG)
check("manual_teams_target unknown -> error listing Teams rows",
      "GEMINI" in (va.manual_teams_target("nope").get("error") or ""))
check("manual_target (Telegram) says 'no watched TELEGRAM row' for a Teams name",
      "no watched TELEGRAM row" in (va.manual_target("GEMINI").get("error") or ""))

# unwatched = read by nothing
for env, alive, want_teams in (({}, True, True), (ON, False, True), (ON, True, False)):
    fresh(env=env)
    va._TEAMS_READER.update(at=0.0, every=0.0)
    if alive:
        va.mark_teams_reader_alive(120)
    va.watch_list()
    va.mark_baselined(PPG, PP)
    r = va.handle_messages([msg(1, "hello")], provider=PP, group=PPG, record_id="recPP")
    has = bool(r["unwatched"]["teams"])
    check(f"unwatched lists the TEAMS rows unless the reader is running ({env}, alive={alive})",
          has is want_teams, r["unwatched"])

# card footers
fresh(env=ON)
va.watch_list()
va.mark_baselined(GEMG, GEM)
r = va.handle_messages([msg(1786918775349, NOTICE, sender="Natalia")], provider=GEM, group=GEMG,
                       record_id="recGEM", platform="Teams")
foot = json.dumps(CARDS[-1], ensure_ascii=False) if CARDS else ""
check("Teams sweep: row written to the Teams provider's record", WRITES and WRITES[-1][0] == "recGEM", WRITES)
check("Teams sweep: card footer says Teams", "Read-only in Teams" in foot and "in Telegram" not in foot, foot[-200:])
check("platform label resets after the sweep", va._platform_label() == "Telegram")
fresh(env=ON)
va.watch_list()
va.mark_baselined(PPG, PP)
va.handle_messages([msg(5, NOTICE)], provider=PP, group=PPG, record_id="recPP")
check("Telegram sweep (default): card footer says Telegram",
      CARDS and "Read-only in Telegram" in json.dumps(CARDS[-1], ensure_ascii=False))

# the lock
fresh(env=ON)
check("ledger lock is re-entrant", hasattr(va._ledger_lock, "_is_owned") or type(va._ledger_lock).__name__ == "RLock")
owned = []
_orig_confirm = va._llm_confirm
va._llm_confirm = lambda *a, **k: (owned.append(va._ledger_lock._is_owned()), _confirm())[1]
va.watch_list()
va.mark_baselined(GEMG, GEM)
va.handle_messages([msg(2, NOTICE)], provider=GEM, group=GEMG, record_id="recGEM", platform="Teams")
va._llm_confirm = _orig_confirm
check("handle_messages holds the ledger lock for the whole sweep", owned and all(owned), owned)


def race(fn):
    """Two sweeps at once (Telegram group + Teams group), each slowed inside."""
    fresh(env=ON)
    va.watch_list()
    va.mark_baselined(PPG, PP)
    va.mark_baselined(GEMG, GEM)
    CONFIRM_DELAY[0] = 0.4
    t1 = threading.Thread(target=fn, args=([msg(10, NOTICE)],),
                          kwargs=dict(provider=PP, group=PPG, record_id="recPP"))
    t2 = threading.Thread(target=fn, args=([msg(20, NOTICE.replace("02:00", "03:00"))],),
                          kwargs=dict(provider=GEM, group=GEMG, record_id="recGEM"))
    t1.start(); time.sleep(0.05); t2.start(); t1.join(); t2.join()
    CONFIRM_DELAY[0] = 0.0
    handled = va._load().get("handled") or {}
    return (any(k.startswith(va._baseline_key(PPG, PP) + "|") for k in handled),
            any(k.startswith(va._baseline_key(GEMG, GEM) + "|") for k in handled))


both = race(va.handle_messages)
check("concurrent Telegram + Teams sweeps: neither loses the other's ledger entries", all(both), both)
unlocked = race(va._sweep_messages)
print(f"      (for contrast, the same race WITHOUT the lock kept: {unlocked})")

# ===================================================================== peerstore
fresh()
peerstore.remember("TG group", "-1001234567", provider="JILI")
check("teams pin stored", peerstore.remember_teams(GEMG, THREAD[GEMG], provider=GEM))
check("teams pin read back (case/space-insensitive title)", peerstore.thread_for("  gemini   x casinoplus ") == THREAD[GEMG])
check("a Telegram lookup never sees a Teams pin", peerstore.peer_for(GEMG) == "")
check("a Teams lookup never sees a Telegram pin", peerstore.thread_for("TG group") == "")
check("Telegram pin untouched by Teams writes", peerstore.peer_for("TG group") == "-1001234567")
for bad in ("", "@thread.v2", "-100123", "19:abc", "19:has space@thread.v2", "https://x/19:a@thread.v2"):
    check(f"not a conversation id refused: {bad!r}", not peerstore.remember_teams(PGSG, bad))
check("forget_teams", peerstore.forget_teams(GEMG) and peerstore.thread_for(GEMG) == "")
raw = json.loads(peerstore.STORE_PATH.read_text(encoding="utf-8"))
check("store keeps both sections", "groups" in raw and "teams" in raw, raw)

# ===================================================================== teamswatch mapping
os.environ["TEAMS_SELF_NAMES"] = "OM Duty, Casino Plus OM"
m = tws._to_va_message({"mid": "5", "text": "hi", "author": "Natalia", "mine": False, "time": "t", "time_text": "6:19 AM"})
check("mapping: provider message", m == {"mid": "5", "text": "hi", "sender": "Natalia", "out": False,
                                         "edited": False, "time": "t", "when": "6:19 AM"}, m)
check("mapping: DOM 'mine' -> out, no sender", tws._to_va_message({"mid": "6", "text": "x", "author": "", "mine": True}) ["out"])
m = tws._to_va_message({"mid": "7", "text": "x", "author": "om  duty", "mine": False})
check("mapping: TEAMS_SELF_NAMES author -> out", m["out"] and m["sender"] == "", m)
check("mapping: edited marker", tws._to_va_message({"mid": "8", "text": "x"}, "Natalia\n6:19 AM\nEdited\nx")["edited"])
check("mapping: body mentioning 'edited' inline is not an edit",
      not tws._to_va_message({"mid": "9", "text": "x"}, "Natalia\nthe schedule was edited today")["edited"])
del os.environ["TEAMS_SELF_NAMES"]


# ===================================================================== teamswatch reader logic
class FakePage:
    def wait_for_timeout(self, ms):
        pass


class Scene:
    """What the fake Teams page shows; every helper _read_provider_on_page calls."""

    def __init__(self, *, title, thread, rows=None, by_id=True, exact=True, bottom=True,
                 scrape_error=None, switch_after_scrape=None, row_title=None):
        self.title, self.thread = title, thread
        self.row_title = row_title
        self.rows = rows if rows is not None else [
            {"mid": "100", "text": "Natalia\nold chatter", "body": "old chatter", "author": "Natalia"},
            {"mid": "200", "text": "Natalia\n" + NOTICE, "body": NOTICE, "author": "Natalia"},
            {"mid": "300", "text": "6:21 AM\nThanks", "body": "Thanks", "author": "", "mine": True}]
        self.by_id, self.exact, self.bottom = by_id, exact, bottom
        self.scrape_error, self.switch_after_scrape = scrape_error, switch_after_scrape
        self.home = 0
        self.calls = []

    def install(self):
        s = self
        tws._chat_list_home = lambda page: setattr(s, "home", s.home + 1)
        tws._click_chat_row_by_thread = lambda page, t: s.calls.append(("id", t)) or s.by_id
        tws._open_group_exact = lambda page, t: (s.calls.append(("title", t)) or (s.exact, "sidebar"))
        tws._open_thread_id = lambda page: s.thread
        tws._confirm_exact_chat = lambda page, t: ((tws._titles_equal(t, s.title), f"header={s.title!r}"))
        tws._scroll_pane_to_bottom = lambda page, **k: {"at_bottom": s.bottom, "newest_mid": "300"}
        tws._row_title_for_thread = lambda page, t: s.row_title

        def scrape(page, n):
            s.calls.append(("scrape", n))
            if s.switch_after_scrape:
                s.title, s.thread = s.switch_after_scrape
            if s.scrape_error:
                return {"error": s.scrape_error}
            return {"rows": [dict(r) for r in s.rows]}
        tws._scrape_rows = scrape


_SAVED = {n: getattr(tws, n) for n in ("_chat_list_home", "_click_chat_row_by_thread", "_open_group_exact",
                                        "_open_thread_id", "_confirm_exact_chat", "_scroll_pane_to_bottom",
                                        "_scrape_rows", "_row_title_for_thread")}
_REAL_EXACT_S = tws._EXACT_CONFIRM_S
tws._EXACT_CONFIRM_S = 0


def restore_page_helpers():
    for n, f in _SAVED.items():
        setattr(tws, n, f)


def read(scene, title=GEMG, pin=THREAD[GEMG]):
    scene.install()
    return tws._read_provider_on_page(FakePage(), title, pin=pin)


sc = Scene(title=GEMG, thread=THREAD[GEMG])
r = read(sc)
check("reader: pinned + exact -> ok", r["ok"] and r["opened_by"] == "id", r)
check("reader: rows mapped for vawatch, system/own handled",
      [(m["mid"], m["out"], m["sender"]) for m in r["messages"]]
      == [("100", False, "Natalia"), ("200", False, "Natalia"), ("300", True, "")], r["messages"])
check("reader: the notice body is the message body only", r["messages"][1]["text"] == NOTICE)
check("reader: scraped wide enough for vawatch's gap re-read", ("scrape", tws._PROVIDER_SCAN) in sc.calls)
check("reader: sidebar scroll handed back", sc.home >= 2, sc.home)

sc = Scene(title=GEMG, thread=THREAD[GEMG])
r = read(sc, pin="")
check("reader: no pin -> refused before touching the page", not r["ok"] and "not pinned" in r["error"]
      and not sc.calls, (r, sc.calls))

sc = Scene(title=GEMG, thread=THREAD[GEMG], by_id=False)
r = read(sc)
check("reader: row not rendered -> exact-title open, pin still checked",
      r["ok"] and r["opened_by"] == "title" and ("title", GEMG) in sc.calls, (r, sc.calls))

sc = Scene(title=GEMG, thread="19:impostor999@thread.v2", by_id=False)
r = read(sc)
check("reader: same exact title, different conversation -> refused", not r["ok"] and "different chat" in r["error"]
      and not any(c[0] == "scrape" for c in sc.calls), r)

sc = Scene(title="GEMINI x CasinoPlus (old)", thread=THREAD[GEMG])
r = read(sc)
check("reader: pinned chat renamed -> refused", not r["ok"] and "not titled" in r["error"], r)

sc = Scene(title=GEMG, thread="")
r = read(sc)
check("reader: no conversation id visible -> refused", not r["ok"] and "conversation id" in r["error"], r)

sc = Scene(title="EVO", thread=THREAD[GEMG], by_id=False, exact=False)
r = read(sc)
check("reader: no exact-title chat -> refused", not r["ok"] and "no chat named exactly" in r["error"], r)

sc = Scene(title=GEMG, thread=THREAD[GEMG], bottom=False)
r = read(sc)
check("reader: not at the newest message -> not handed on", not r["ok"] and "newest" in r["error"]
      and not r["messages"], r)

sc = Scene(title=GEMG, thread=THREAD[GEMG], scrape_error="no message pane")
r = read(sc)
check("reader: pane not resolved -> refused", not r["ok"] and "pane" in r["error"], r)

sc = Scene(title=GEMG, thread=THREAD[GEMG], switch_after_scrape=("@EVO C88live", "19:evo@thread.skype"))
r = read(sc)
check("reader: chat switched during the scrape -> discarded", not r["ok"] and "changed while reading" in r["error"]
      and not r["messages"], r)
check("reader: sidebar handed back on every refusal too", sc.home >= 2, sc.home)

os.environ["EVOTEAMS_THREAD_ID"] = "19:evo@thread.skype"     # the trap _read_on_page falls into
sc = Scene(title="@EVO C88live/slot_ow.ph (RTS) CS Group NE RT FP", thread="19:evo@thread.skype", by_id=False)
r = read(sc)
check("reader: EVOTEAMS_THREAD_ID set and EVO open -> EVO is NOT read as GEMINI", not r["ok"], r)
del os.environ["EVOTEAMS_THREAD_ID"]
restore_page_helpers()


# ===================================================================== worker job
class W(tws._TeamsWarm):
    def __init__(self):
        super().__init__()
        self._page = FakePage()
        self.ready = True
        self.healthy = True
        self.ready_calls = 0
        self.stood_down = 0

    def _ready(self):
        self.ready_calls += 1
        return self.ready

    def _healthy(self):
        return self.healthy

    def _stand_down(self):
        self.stood_down += 1

    def _teardown(self):
        self.healthy = False


RESTORES: list = []
_real_restore = tws._restore_evo
tws._restore_evo = lambda page: RESTORES.append(len(HM) if "HM" in globals() else 0) or True


HM: list = []
_real_hm = va.handle_messages


def fake_hm(messages, **kw):
    HM.append((list(messages), dict(kw)))
    return {"seen": len(messages), "acted": 0, "details": []}


def job(w, target, reader_result, *, box=False, force=False):
    tws._read_provider_on_page = lambda page, title, pin: dict(reader_result, pin_seen=pin)
    task = {"kind": "provider_read", "target": target, "force": force}
    if box:
        task["box"] = {}
    return w._do_provider_read(task)


_real_reader = tws._read_provider_on_page
fresh(env=ON)
va.handle_messages = fake_hm
peerstore.remember_teams(GEMG, THREAD[GEMG], provider=GEM)
tgt = dict(va.watch_list()["teams_rows"][0])
w = W()
many = [msg(i, f"m{i}") for i in range(1, 31)]
out = job(w, tgt, {"ok": True, "messages": many, "thread": THREAD[GEMG]})
check("job: vawatch called once, labelled Teams, with the row's record_id",
      len(HM) == 1 and HM[0][1].get("platform") == "Teams" and HM[0][1].get("record_id") == "recGEM"
      and HM[0][1].get("provider") == GEM and HM[0][1].get("group") == GEMG
      and "shared_with" in HM[0][1], HM[-1][1] if HM else None)
check("job: hands vawatch the newest VAWATCH_READ_COUNT messages",
      [m["mid"] for m in HM[0][0]] == [str(i) for i in range(31 - va._read_count(), 31)], [m["mid"] for m in HM[0][0]])
check("job: result carries group/provider for the /vacheck summary", out["ok"] and out["result"]["group"] == GEMG)

HM.clear()
_gap = va.gap_read_count
va.gap_read_count = lambda batch, **k: 20
job(w, tgt, {"ok": True, "messages": many, "thread": THREAD[GEMG]})
va.gap_read_count = _gap
check("job: a gap widens the batch as vawatch asks", len(HM[0][0]) == 20, len(HM[0][0]))

HM.clear()
fresh(env=ON)
peerstore.remember_teams(GEMG, THREAD[GEMG], provider=GEM)
w = W()
for i in range(1, 4):
    job(w, tgt, {"ok": False, "error": "a different chat is open"})
check("job: failed reads never reach vawatch", not HM)
check("job: 3 timer failures in a row -> one alert to the Laboratory group",
      len(TEXTS) == 1 and "Teams provider watcher" in TEXTS[0] and "3 times" in TEXTS[0], TEXTS)
job(w, tgt, {"ok": False, "error": "x"}, box=True)
check("job: a manual /vacheck failure does not count", w.provider_stats()[GEMG]["fails"] == 3, w.provider_stats())
job(w, tgt, {"ok": True, "messages": many[:3], "thread": THREAD[GEMG]})
check("job: a good read clears the streak", w.provider_stats()[GEMG]["fails"] == 0 and w.provider_stats()[GEMG]["last_ok"])

fresh()                                         # watcher OFF: failures never post
w = W()
for i in range(3):
    job(w, tgt, {"ok": False, "error": "x"})
check("job: no alert while the watcher is off (a test or REPL cannot post)", not TEXTS, TEXTS)

fresh(env=ON)
w = W()
w.healthy = False
out = job(w, tgt, {"ok": True, "messages": many})
check("job (timer): no page -> error, NOT a launch, not counted",
      not out["ok"] and w.ready_calls == 0 and not w.provider_stats() and "EVO poll relaunches" in out["error"], out)
w = W()
os.environ["TEAMS_TEST_YIELD"] = "1"
_yr = tws._yield_requested
tws._yield_requested = lambda: True
out = job(w, tgt, {"ok": True, "messages": many})
tws._yield_requested = _yr
check("job (timer): a yield request stands the browser down, no read", w.stood_down == 1 and not out["ok"], out)
w = W()
w.healthy = False
out = job(w, tgt, {"ok": True, "messages": many}, box=True)
check("job (manual /vacheck): may launch via _ready()", w.ready_calls == 1, w.ready_calls)
HM.clear(); RESTORES.clear()
w = W()
job(w, tgt, {"ok": True, "messages": many, "thread": THREAD[GEMG]})
check("job: EVO put back after the read, BEFORE the sweep", RESTORES == [0] and len(HM) == 1, (RESTORES, len(HM)))
RESTORES.clear()
job(W(), tgt, {"ok": False, "error": "x"})
check("job: EVO put back after a failed read too", RESTORES == [len(HM)], RESTORES)
out = job(W(), {"provider": GEM}, {"ok": True, "messages": many})
check("job: target without a Group Name refused", not out["ok"] and "no Group Name" in out["error"], out)

va.handle_messages = lambda *a, **k: (_ for _ in ()).throw(va.LedgerError("disk full"))
out = job(W(), tgt, {"ok": True, "messages": many})
check("job: detector failure reported, not raised", not out["ok"] and "detector failed" in out["error"], out)
va.handle_messages = fake_hm

# the pin the job passes is the store's
fresh(env=ON)
seen = {}
tws._read_provider_on_page = lambda page, title, pin: seen.update(pin=pin) or {"ok": False, "error": "x"}
W()._do_provider_read({"target": tgt})
check("job: unpinned group reaches the reader with pin '' (which refuses)", seen.get("pin") == "", seen)
peerstore.remember_teams(GEMG, THREAD[GEMG], provider=GEM)
W()._do_provider_read({"target": tgt})
check("job: pinned group reaches the reader with its pin", seen.get("pin") == THREAD[GEMG], seen)

# rotation
fresh(env=ON)
w = W()
order = [w._next_provider_target()["provider"] for _ in range(5)]
check("rotation: one TEAMS row per tick, round-robin", order == [GEM, PGS, RTG, GEM, PGS], order)
fresh(BASE[:2], env=ON)
check("rotation: no TEAMS rows -> nothing queued", W()._next_provider_target() == {})

# through the real worker queue (poll off: no timer threads, no browser)
fresh()
HM.clear()
tws._read_provider_on_page = lambda page, title, pin: {"ok": True, "thread": pin, "messages": [
    msg(1, "hello", sender="Natalia", when="6:19 AM"), msg(2, "Thanks", out=True, when="6:21 AM")]}
peerstore.remember_teams(GEMG, THREAD[GEMG], provider=GEM)
w = W()
res = w.provider_check(tgt, force=True, timeout_s=20)
check("provider_check: runs on the worker and answers", res.get("ok") and HM and HM[-1][1].get("force") is True, res)
txt = tws.format_read_lines(res)
check("/vacheck reply lists who sent what, (us) marked",
      "Natalia · hello" in txt and "(us) · Thanks" in txt and THREAD[GEMG][:20] in txt, txt)
check("/vacheck reply with nothing read", "no messages" in tws.format_read_lines({}))
check("status: OFF line when the reader is off", "OFF" in tws._provider_status_lines()[0])

va.handle_messages = _real_hm
tws._read_provider_on_page = _real_reader
tws._EXACT_CONFIRM_S = _REAL_EXACT_S

# ===================================================================== groupcheck pins the id
fresh(env=ON)
found, missing, seenidx = [], [], set()
sink = gc._make_sink("oc_test", [row(GEM, GEMG, "recGEM", apps=("teams",))], "Teams", found, missing, seenidx)
sink({"title": GEMG, "ok": True, "opened": GEMG, "thread": THREAD[GEMG], "shot": ""})
check("/telegramgroupcheck pins the Teams conversation id", peerstore.thread_for(GEMG) == THREAD[GEMG])
sink2 = gc._make_sink("oc_test", [row(PGS, PGSG, "recPGS", apps=("teams",))], "Teams", [], [], set())
sink2({"title": PGSG, "ok": False, "reason": "not found", "thread": THREAD[PGSG]})
check("/telegramgroupcheck: a failed probe pins nothing", peerstore.thread_for(PGSG) == "")
sink3 = gc._make_sink("oc_test", [row(RTG, RTGG, "recRTG", apps=("teams",))], "Telegram", [], [], set())
sink3({"title": RTGG, "ok": True, "thread": THREAD[RTGG], "peerId": ""})
check("/telegramgroupcheck: a Telegram result never writes a Teams pin", peerstore.thread_for(RTGG) == "")
src = (REPO / "teamswatch.py").read_text(encoding="utf-8")
check("the Teams probe reports the (verified) conversation id", 'out["thread"] = thread' in src)

# ===================================================================== review fixes
# -- the pinned id's own sidebar row must carry the title
sc = Scene(title=GEMG, thread=THREAD[GEMG], row_title="PG Soft Support Casinoplus")
r = read(sc)
check("reader: pinned id's own row titled differently -> refused",
      not r["ok"] and "sidebar row is titled" in r["error"], r)
sc = Scene(title=GEMG, thread=THREAD[GEMG], row_title=GEMG)
check("reader: pinned id's own row titled exactly -> ok", read(sc)["ok"])

# -- deletion placeholders never reach vawatch
sc = Scene(title=GEMG, thread=THREAD[GEMG], rows=[
    {"mid": "100", "text": "Natalia\nhello", "body": "hello", "author": "Natalia"},
    {"mid": "200", "text": "This message has been deleted.", "body": "This message has been deleted.",
     "author": "Natalia"},
    {"mid": "300", "text": "此消息已被删除", "body": "此消息已被删除", "author": ""}])
r = read(sc)
check("reader: deleted-message placeholders dropped (EN + ZH)",
      r["ok"] and [m["mid"] for m in r["messages"]] == ["100"] and r["deleted_rows"] == 2, r)
restore_page_helpers()

# -- _confirm_exact_chat: the id decides when both ids are readable
_oct, _sct = tws._open_chat_title, tws._selected_chat_title
_otid, _ert = tws._open_thread_id, tws._exact_row_threads
PGS_ID, GEM_ID = THREAD[PGSG], THREAD[GEMG]
for open_id, rows, header, want, label in (
        (PGS_ID, [PGS_ID], GEMG, True, "PG Soft open by id, header still GEMINI's (the 2026-09-26 case) -> yes"),
        (GEM_ID, [PGS_ID], PGSG, False, "header says PG Soft but the open conversation is GEMINI's -> NO"),
        (PGS_ID, [PGS_ID, "19:dupe@thread.v2"], PGSG, False, "two chats named exactly this -> refuse"),
        (GEM_ID, [PGS_ID], "", False, "selected row lagged onto PG Soft, conversation still GEMINI -> NO")):
    tws._open_thread_id = lambda page, x=open_id: x
    tws._exact_row_threads = lambda page, t, r=rows: list(r)
    tws._open_chat_title = lambda page, h=header: h
    tws._selected_chat_title = lambda page: PGSG
    got = tws._confirm_exact_chat(FakePage(), PGSG)[0]
    check(f"_confirm_exact_chat (id first): {label}", got is want, (open_id, rows, header, got))
# -- ...and falls back to the header rules only when an id is missing
tws._open_thread_id = lambda page: ""
tws._exact_row_threads = lambda page, t: []
for header, selected, want, label in (
        ("EVO group", GEMG, False, "header names another chat, selected row = title -> NO"),
        ("", GEMG, True, "no header, selected row = title -> yes"),
        ("GEMINI x Casino…", GEMG, True, "truncated header, selected row = title -> yes"),
        (GEMG, "EVO group", True, "header = title -> yes"),
        ("GEMINI x CasinoPlus (old)", GEMG, False, "header is a longer, different title -> NO")):
    tws._open_chat_title = lambda page, h=header: h
    tws._selected_chat_title = lambda page, x=selected: x
    got = tws._confirm_exact_chat(FakePage(), GEMG)[0]
    check(f"_confirm_exact_chat (no ids): {label}", got is want, (header, selected, got))
tws._open_chat_title, tws._selected_chat_title = _oct, _sct
tws._open_thread_id, tws._exact_row_threads = _otid, _ert

# -- the harvest pins only an id tied to the title
_saved_probe = {n: getattr(tws, n) for n in ("_open_group_exact", "_open_chat_title", "_open_thread_id",
                                             "_row_title_for_thread", "_pane_shot")}


def probe(header, thread, row_title):
    tws._open_group_exact = lambda page, t: (True, "ok")
    tws._open_chat_title = lambda page: header
    tws._open_thread_id = lambda page: thread
    tws._row_title_for_thread = lambda page, t: row_title
    tws._pane_shot = lambda page, p: ""
    return tws._probe_group(FakePage(), GEMG, shot_path="x.png")


r = probe(GEMG, THREAD[GEMG], GEMG)
check("harvest: header + own row = title -> id offered", r["ok"] and r.get("thread") == THREAD[GEMG], r)
r = probe(GEMG, THREAD[GEMG], None)
check("harvest: row not rendered, header = title -> id offered", r.get("thread") == THREAD[GEMG], r)
r = probe("EVO group", THREAD[GEMG], GEMG)
check("harvest: stale header text, but the open id's own row = title -> pinned (PG Soft case)",
      r["ok"] and r.get("thread") == THREAD[GEMG], r)
r = probe("EVO group", THREAD[GEMG], None)
check("harvest: stale header AND the id's row not rendered -> NOT pinned",
      r["ok"] and not r.get("thread") and r.get("thread_note"), r)
r = probe(GEMG, "19:evo@thread.skype", "@EVO C88live")
check("harvest: the id's own row names another chat -> NOT pinned", not r.get("thread"), r)
r = probe(GEMG, "", None)
check("harvest: no id visible -> NOT pinned", not r.get("thread"), r)
for n, f in _saved_probe.items():
    setattr(tws, n, f)

# -- _restore_evo
tws._restore_evo = _real_restore
EVO_ID = tws._wanted_thread_id(tws._watch_target())


class EvoScene:
    def __init__(self, open_id, rendered_at_sweep):
        self.open_id, self.at = open_id, rendered_at_sweep
        self.sweep, self.clicks, self.home = 0, 0, 0

    def install(self):
        e = self
        tws._open_thread_id = lambda page: e.open_id

        def home(page):
            e.home += 1
            e.sweep = 0
        tws._chat_list_home = home

        def click(page, t):
            e.clicks += 1
            if e.at is not None and e.sweep >= e.at and t == EVO_ID:
                e.open_id = EVO_ID
                return True
            return False
        tws._click_chat_row_by_thread = click


class EvoPage(FakePage):
    def __init__(self, scene):
        self.scene = scene

    def evaluate(self, js, arg=None):
        self.scene.sweep += 1
        return 700 * self.scene.sweep


check("EVO has a known conversation id (default group)", bool(EVO_ID), EVO_ID)
es = EvoScene(EVO_ID, 0)
es.install()
check("_restore_evo: EVO already open -> nothing clicked", tws._restore_evo(EvoPage(es)) and es.clicks == 0)
es = EvoScene(THREAD[GEMG], 2)
es.install()
ok = tws._restore_evo(EvoPage(es))
check("_restore_evo: EVO row below the rendered top -> found by sweeping, reopened",
      ok and es.open_id == EVO_ID and es.clicks >= 3 and es.home >= 2, vars(es))
es = EvoScene(THREAD[GEMG], None)
es.install()
check("_restore_evo: EVO nowhere -> False, no exception", tws._restore_evo(EvoPage(es)) is False)
restore_page_helpers()

# -- vawatch with Teams mids: a deleted notice is noticed, and no false "restart"
fresh(env=ON)
va.mark_teams_reader_alive(120)
va.watch_list()
base = 1786918775000
screen0 = [msg(base + 1, "hello", sender="Natalia"), msg(base + 2, "ok", sender="Natalia")]
r0 = va.handle_messages(screen0, provider=GEM, group=GEMG, record_id="recGEM", platform="Teams")
check("Teams first visit baselines", r0["cold_start"] and not WRITES, r0["cold_start"])
notice = msg(base + 60000, NOTICE, sender="Natalia")
va.handle_messages(screen0 + [notice], provider=GEM, group=GEMG, record_id="recGEM", platform="Teams")
check("Teams new notice written", WRITES and WRITES[-1][0] == "recGEM", WRITES)
n_writes = len(WRITES)
# The NEWEST message deleted: its id is thousands of "ids" (ms) below `last`,
# which the Telegram restart rule would have read as a re-created chat.
r2 = va.handle_messages(screen0, provider=GEM, group=GEMG, record_id="recGEM", platform="Teams")
check("Teams: newest message deleted -> no false 'ids restarted', nothing re-written",
      not r2.get("mid_restart") and len(WRITES) == n_writes, (r2.get("mid_restart"), WRITES[n_writes:]))
# A notice deleted from BETWEEN two surviving messages (vawatch's G2.4 rule).
fresh(env=ON)
va.mark_teams_reader_alive(120)
va.watch_list()
va.handle_messages(screen0, provider=GEM, group=GEMG, record_id="recGEM", platform="Teams")
after = msg(base + 120000, "noted, thanks", sender="Natalia")
va.handle_messages(screen0 + [notice, after], provider=GEM, group=GEMG, record_id="recGEM", platform="Teams")
n_cards = len(CARDS)
r3 = va.handle_messages(screen0 + [after], provider=GEM, group=GEMG, record_id="recGEM", platform="Teams")
check("Teams: the written notice deleted between two messages -> carded (G2.4 fires), no restart",
      int(r3.get("deleted_notice") or 0) >= 1 and len(CARDS) > n_cards and not r3.get("mid_restart"),
      (r3.get("deleted_notice"), len(CARDS) - n_cards, r3.get("mid_restart")))

# -- the stale-list alert is claimed, so two watchers post it once
fresh(env=ON)
va.watch_list()
d = va._load()
d["targets"]["at"] = (NOW - timedelta(hours=30)).strftime("%Y-%m-%d %H:%M:%S")
with va._ledger_lock:
    va._save(d)
POSTS: list = []
_real_post = va._post_note


def slow_post(*a, **k):
    POSTS.append(threading.current_thread().name)
    time.sleep(0.5)
    return {"carded": True, "error": "", "unknown": False}


va._post_note = slow_post
gc.fetch_rows, _real_fetch = (lambda: (_ for _ in ()).throw(RuntimeError("view gone"))), gc.fetch_rows
try:
    ts = [threading.Thread(target=va.watch_list, name=n) for n in ("telegram-worker", "teams-provider-poll")]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    check("stale-list alert: two watchers at once -> posted once", len(POSTS) == 1, POSTS)
    POSTS.clear()
    d = va._load()
    d["targets"]["at"] = (NOW - timedelta(hours=40)).strftime("%Y-%m-%d %H:%M:%S")
    with va._ledger_lock:
        va._save(d)
    va._post_note = lambda *a, **k: (POSTS.append("fail"), {"carded": False, "error": "500", "unknown": False})[1]
    va.watch_list()
    va._post_note = lambda *a, **k: (POSTS.append("ok"), {"carded": True, "error": "", "unknown": False})[1]
    va.watch_list()
    va.watch_list()
    check("stale-list alert: a failed send gives the claim back (retried once, then quiet)",
          POSTS == ["fail", "ok"], POSTS)
finally:
    gc.fetch_rows = _real_fetch
    va._post_note = _real_post

# -- peerstore never writes over a store it could not read
fresh()
peerstore.remember("TG A", "-1001", provider="A")
peerstore.remember_teams(GEMG, THREAD[GEMG], provider=GEM)
before = peerstore.STORE_PATH.read_bytes()
import builtins  # noqa: E402

_real_open = builtins.open


def flaky_open(p, mode="r", *a, **k):
    if str(p) == str(peerstore.STORE_PATH) and "w" not in mode:
        raise OSError(24, "Too many open files")
    return _real_open(p, mode, *a, **k)


builtins.open = flaky_open
try:
    w1 = peerstore.remember_teams(PGSG, THREAD[PGSG], provider=PGS)
    w2 = peerstore.remember("TG B", "-1002", provider="B")
    w3 = peerstore.forget("TG A")
    w4 = peerstore.forget_teams(GEMG)
finally:
    builtins.open = _real_open
check("peerstore: unreadable store -> every writer refuses", not any((w1, w2, w3, w4)), (w1, w2, w3, w4))
check("peerstore: unreadable store -> file untouched (no pin of either app lost)",
      peerstore.STORE_PATH.read_bytes() == before)
peerstore.STORE_PATH.write_text("{not json", encoding="utf-8")
check("peerstore: corrupt store -> moved aside, write proceeds",
      peerstore.remember_teams(GEMG, THREAD[GEMG])
      and peerstore.STORE_PATH.with_name(peerstore.STORE_PATH.name + ".bad").exists())

# ===================================================================== main.py wiring
msrc = (REPO / "main.py").read_text(encoding="utf-8")
check("/vacheck falls through to Teams only for 'no watched TELEGRAM row'",
      '"no watched TELEGRAM row" in tgt["error"]' in msrc and "manual_teams_target(name_va)" in msrc
      and "provider_check_now(ttgt, force=force_va)" in msrc)
_reader_src = src.split("def _read_provider_on_page", 1)[1].split("def _restore_evo", 1)[0]
check("teamswatch provider reader never goes through _read_on_page / _open_group",
      "_read_on_page(" not in _reader_src and "_open_group(" not in _reader_src)

# ===================================================================== the real scraper JS
FIXTURE = """<!doctype html><html><body>
<div data-tid="app-layout-area--nav"><div data-tid="chat-list"><div role="treeitem">GEMINI x CasinoPlus</div></div></div>
<div data-tid="app-layout-area--main"><div id="chat-pane-list">
  <div data-tid="chat-pane-message" data-mid="1786918775349">
    <div class="fui-ChatMessage">
      <span id="author-1786918775349">Natalia</span>
      <time id="timestamp-1786918775349" datetime="2026-08-16T22:19:35.349Z">6:19 AM</time>
      <div id="content-1786918775349" data-message-content>Scheduled maintenance on 2026-09-27 02:00 - 04:00 (GMT+8).</div>
    </div>
  </div>
  <div data-tid="chat-pane-message" data-mid="1786918775400">
    <div class="fui-ChatMyMessage">
      <time id="timestamp-1786918775400" datetime="2026-08-16T22:21:00.000Z">6:21 AM</time>
      <div id="content-1786918775400" data-message-content>Hi team, are there any maintenance plans for this week?</div>
    </div>
  </div>
  <div class="fui-ChatMyMessage r-wrap"><div data-tid="chat-pane-message" data-mid="1786918775500">
      <div id="content-1786918775500" data-message-content>Noted, thanks</div>
  </div></div>
</div></div></body></html>"""
try:
    from playwright.sync_api import sync_playwright
    have_pw = True
except Exception as err:  # noqa: BLE001
    have_pw = False
    print(f"SKIP  scraper JS (no Playwright: {err!r})")
if have_pw:
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        pg = b.new_page()
        pg.set_content(FIXTURE)
        got = tws._scrape_rows(pg, 50)
        rows = got.get("rows") or []
        check("scraper JS runs (EVO shares it)", not got.get("error") and len(rows) == 3, got)
        if len(rows) == 3:
            check("scraper: EVO fields unchanged (mid/author/time/body)",
                  rows[0]["mid"] == "1786918775349" and rows[0]["author"] == "Natalia"
                  and rows[0]["time"] == "2026-08-16T22:19:35.349Z"
                  and rows[0]["body"].startswith("Scheduled maintenance"), rows[0])
            check("scraper: provider message is not ours", rows[0]["mine"] is False, rows[0])
            check("scraper: ChatMyMessage inside the row -> ours", rows[1]["mine"] is True, rows[1])
            check("scraper: ChatMyMessage around the row -> ours", rows[2]["mine"] is True, rows[2])
            picked = tws._pick_messages(rows, 0)["messages"]
            vm = [tws._to_va_message(r) for r in picked]
            check("scraper -> mapping end to end",
                  [(m["sender"], m["out"]) for m in vm] == [("Natalia", False), ("", True), ("", True)], vm)

        # The 2026-09-26 PG Soft case: the chat just left (GEMINI) still mounted,
        # hidden, EARLIER in the document than the chat on screen (PG Soft).
        GT, PT = "IGO/Gemini(FA) integration group", "PG & CP _ ZF918(B)【技术】对接群"
        GID, PID = "19:gemini0001@thread.v2", "19:pgsoft0002@thread.v2"

        def conv(tid, title, mids, style=""):
            rows = "".join(
                f'<div data-tid="chat-pane-message" data-mid="{m}"><div class="fui-ChatMessage">'
                f'<span id="author-{m}">Agent</span><div id="content-{m}" data-message-content>'
                f'{title} message {m}</div></div></div>' for m in mids)
            return (f'<div class="conv" style="{style}"><h2 data-tid="chat-title" title="{title}">{title[:12]}…</h2>'
                    f'<div id="chat-pane-list">{rows}</div>'
                    f'<button data-tid="sendMessageCommands-send" data-track-thread-id="{tid}">send</button></div>')

        sidebar = (f'<div data-tid="app-layout-area--nav"><div data-tid="chat-list">'
                   f'<div role="treeitem" data-fui-tree-item-value="a|b|{GID}"><span id="title-chat-list-item_{GID}">{GT}</span></div>'
                   f'<div role="treeitem" data-fui-tree-item-value="a|b|{PID}"><span id="title-chat-list-item_{PID}">{PT}</span></div>'
                   f'</div></div>')
        for how, style in (("visibility:hidden", "visibility:hidden"), ("opacity:0", "opacity:0"),
                           ("moved off-screen", "position:absolute; left:-6000px; top:0; width:800px"),
                           ("display:none", "display:none"),
                           ("stacked underneath", "position:absolute; left:0; top:0; width:900px; z-index:1")):
            # "stacked underneath": both chats in the same place, the shown one on top.
            top_style = ("position:absolute; left:0; top:0; width:900px; z-index:2; background:#fff"
                         if how == "stacked underneath" else "")
            pg.set_content("<!doctype html><html><body>" + sidebar
                           + '<div data-tid="app-layout-area--main" style="position:relative; height:600px">'
                           + conv(GID, GT, [1790000000001, 1790000000002], style)
                           + conv(PID, PT, [1790000000101, 1790000000102, 1790000000103], top_style)
                           + "</div></body></html>")
            check(f"hidden previous chat ({how}): conversation id is the SHOWN chat's",
                  tws._open_thread_id(pg) == PID, tws._open_thread_id(pg))
            check(f"hidden previous chat ({how}): header is the SHOWN chat's",
                  tws._open_chat_title(pg) == PT, tws._open_chat_title(pg))
            got = tws._scrape_rows(pg, 50)
            check(f"hidden previous chat ({how}): scraper reads ONLY the shown chat",
                  [r["mid"] for r in got.get("rows") or []] == ["1790000000101", "1790000000102", "1790000000103"],
                  [r.get("mid") for r in got.get("rows") or []])
            st = tws._pane_bottom_step(pg, scroll=False) or {}
            check(f"hidden previous chat ({how}): bottom check measures the shown chat",
                  str(st.get("maxMid")) == "1790000000103", st)
            check(f"hidden previous chat ({how}): PG Soft confirmed by id",
                  tws._confirm_exact_chat(pg, PT)[0], tws._confirm_exact_chat(pg, PT))
            check(f"hidden previous chat ({how}): GEMINI NOT confirmed",
                  not tws._confirm_exact_chat(pg, GT)[0], tws._confirm_exact_chat(pg, GT))
        check("row name span ties id to title", tws._row_title_for_thread(pg, PID) == PT
              and tws._exact_row_threads(pg, PT) == [PID])

        # The review's lag case: selection/rows say PG Soft, the SHOWN conversation is GEMINI.
        pg.set_content("<!doctype html><html><body>" + sidebar
                       + '<div data-tid="app-layout-area--main">' + conv(GID, GT, [1790000000001]) + "</div></body></html>")
        check("shown conversation is GEMINI: PG Soft NOT confirmed", not tws._confirm_exact_chat(pg, PT)[0])
        # One conversation on the page (how EVO always runs): unchanged answers.
        pg.set_content("<!doctype html><html><body>" + sidebar
                       + '<div data-tid="app-layout-area--main">' + conv(PID, PT, [1790000000101]) + "</div></body></html>")
        check("single conversation: id, header and rows as before",
              tws._open_thread_id(pg) == PID and tws._open_chat_title(pg) == PT
              and [r["mid"] for r in tws._scrape_rows(pg, 50).get("rows") or []] == ["1790000000101"])
        b.close()

print("-" * 78)
print(f"{'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILED'}   | network attempts: {len(NET)} "
      f"| Lark sends: recorders only")
sys.exit(1 if FAILS or NET else 0)
