"""
Self-test for the Teams -> EVO maintenance auto-detection path.

Exercises detectevomaintenance end to end with REAL notice text and fake Teams
rows, so the parts that decide whether a maintenance email goes out can be checked
without a browser, without Lark, and without sending anything.

What it pins down — every one of these is a bug this path actually had, or a
guarantee the rest of the design leans on:

  * the first poll BASELINES and cards nothing (enabling the feature must not card
    the group's existing backlog)
  * a new ※SD※ batch notice produces exactly ONE card, whatever its length
  * the same notice on a later poll produces NO second card
  * two notices arriving inside one poll interval both get carded (the poll reads
    with limit=0 precisely so the tail slice cannot drop one)
  * ordinary chatter is not carded
  * a notice-shaped message that /m would refuse is recorded `soft_skipped`,
    counted, and NOT carded
  * a failed card post HOLDS THE CURSOR BACK so the next poll retries
  * a read that did not reach the end of the chat is refused outright
  * the cursor never regresses
  * two taps of [Generate Email] send the email exactly ONCE
  * a /m run that emailed nothing leaves the notice retryable, not green
  * Cancel is refused once a send is already in flight

Usage
-----
    python evoteams_selftest.py            # run every case, print a score
    python evoteams_selftest.py -v         # also print the cases that passed

Exit code is 0 only when every case passes.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import threading
import types
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

GROUP = "@EVO C88live/slot_ow.ph (RTS) CS Group NE RT FP"
SEP = "=" * 32


def _sd_block(ticket: str, table: str, day: str) -> str:
    """One ※SD※ block in the shape Evolution actually posts."""
    return (
        f"※SD-{ticket} ※\n"
        f"Dear Casino Team,\n\n"
        f"This is to inform you that an exceptional maintenance is going to take "
        f"place with a downtime from 2026-08-{day} 07:00:00 UTC till 2026-08-{day} "
        f"08:00:00, during which the following tables will be unavailable:\n\n"
        f"○ {table}\n\n"
        f"● Start Time: 2026-08-{day} 07:00:00\n"
        f"● End Time: 2026-08-{day} 08:00:00\n"
        f"● UTC+8 Time: 2026-08-{day} 15:00:00 UTC +8 to 2026-08-{day} 16:00:00 UTC +8\n"
        f"● Reason: Equipment maintenance\n"
        f"● Table availability: Affected\n\n"
        f"We apologize for the inconvenience.\n"
        f"------------------------------------------------\n"
        f"※SD-{ticket}定期维护通知※\n"
        f"亲爱的团队您好，\n"
        f"我司进行列表时间进行定期维护，该部分游戏将受到影响\n"
        f"● 受影响游戏：\n\n"
        f"○ {table}\n\n"
        f"● 影响状况：玩家无法进行游戏\n"
        f"● 维护时间：2026-08-{day} 07:00:00 到 2026-08-{day} 08:00:00 UTC\n"
        f"● 北京时间：2026-08-{day} 15:00:00 UTC +8 到 2026-08-{day} 16:00:00 UTC +8\n"
        f"● 维护事由：设备维护\n\n"
        f"因本通知为统一发出，如以上内容包含不属于贵司的赌桌，敬请直接忽略该维护项目。"
        f"造成您的不便，希望您能谅解。\n"
    )


# The real thing: a multi-block batch, ~8 tickets, thousands of characters.
BATCH_NOTICE = (
    SEP + "\n"
    + ("\n" + SEP + "\n").join([
        _sd_block("7362822", "Dragonara Roulette", "19"),
        _sd_block("7362800", "Blackjack Classic 59", "20"),
        _sd_block("7362799", "Blackjack Classic 27", "19"),
        _sd_block("7362442", "Dragon Tiger Relâmpago", "19"),
        _sd_block("7362434", "Speed Baccarat Z", "18"),
        _sd_block("7362433", "Speed Baccarat 3", "18"),
        _sd_block("7362432", "Speed Baccarat 2", "18"),
        _sd_block("7362431", "Speed Baccarat 1", "18"),
    ])
    + "\n" + SEP + "\n"
)

SECOND_NOTICE = SEP + "\n" + _sd_block("7362999", "Lightning Roulette", "21") + "\n" + SEP + "\n"

CHATTER = "noted, thanks. will check with the team and revert by tomorrow morning."

# Notice-shaped, but no ※SD※ marker — /m refuses this format, so it must not card.
SOFT_NOTICE = (
    "Hi team, please note the following tables will be unavailable tomorrow "
    "2026-08-19 from 07:00 to 08:00 UTC for equipment maintenance:\n"
    "- Speed Baccarat A\n- Speed Baccarat B\n"
)


def _row(mid: str, text: str, *, author: str = "Justin Lo",
         when: str = "2026-08-18T05:43:00.000Z") -> dict:
    """A scraped row in exactly the shape teamswatch._pick_messages returns."""
    return {"mid": mid, "id": mid, "author": author, "author_src": "id",
            "time": when, "time_text": "5:43 AM", "time_src": "id",
            "last": True, "body": text, "text": text}


class Harness:
    """A fresh ledger + a fake `main`, so nothing leaves the process."""

    def __init__(self, tmp: Path) -> None:
        import detectevomaintenance as evom

        self.evom = evom
        evom._STATE_PATH = tmp / "ledger.json"
        self.cards: list[dict] = []
        self.replies: list[tuple[str, str]] = []
        self.texts: list[tuple[str, str]] = []
        self.m_runs: list[str] = []
        self.patches: list[dict] = []
        self.card_send_ok = True
        self.m_result: dict = {"ok": True, "email_sent": True, "reason": ""}
        self.m_delay = 0.0
        self._mid = 0
        self._install_fakes()

    # -- fake main / offsetleave ------------------------------------------
    def _install_fakes(self) -> None:
        h = self

        def send_message(chat_id, payload, msg_type="text", **kw):
            if msg_type == "interactive":
                if not h.card_send_ok:
                    return {"code": 232000, "msg": "simulated card refusal"}
                h._mid += 1
                mid = f"om_fake{h._mid}"
                h.cards.append({"chat_id": chat_id, "mid": mid,
                                "card": json.loads(payload)})
                return {"code": 0, "data": {"message_id": mid}}
            h.texts.append((chat_id, str(payload)))
            return {"code": 0, "data": {"message_id": "om_text"}}

        def reply_message_in_thread(parent_mid, text):
            h.replies.append((parent_mid, text))
            return {"code": 0}

        def _extract_lark_message_id(resp):
            return ((resp or {}).get("data") or {}).get("message_id") or ""

        def _process_evo_sd_batch_paste(chat_id, email_text):
            if h.m_delay:
                import time as _t
                _t.sleep(h.m_delay)
            h.m_runs.append(email_text)
            return dict(h.m_result)

        fake_main = types.ModuleType("main")
        fake_main.send_message = send_message
        fake_main.reply_message_in_thread = reply_message_in_thread
        fake_main._extract_lark_message_id = _extract_lark_message_id
        fake_main._process_evo_sd_batch_paste = _process_evo_sd_batch_paste
        sys.modules["main"] = fake_main

        fake_ol = types.ModuleType("offsetleave")

        def _try_patch_interactive_card_message(card_mid, card):
            h.patches.append({"mid": card_mid, "card": card})
            return True

        fake_ol._try_patch_interactive_card_message = _try_patch_interactive_card_message
        sys.modules["offsetleave"] = fake_ol

    # -- helpers ----------------------------------------------------------
    def poll(self, rows: list[dict], **kw) -> dict:
        return self.evom.handle_new_messages(group=GROUP, messages=rows, **kw)

    def tap(self, key: str, action: str = "evom_gen", card_mid: str = "om_fake1"):
        return self.evom.handle_card_callback(
            {"k": action, "i": key},
            {"context": {"open_message_id": card_mid}},
            self.evom.CARD_CHAT_ID,
        )

    def pending_key(self) -> str:
        return self.evom.content_key(BATCH_NOTICE)

    def header(self, idx: int = -1) -> str:
        return self.cards[idx]["card"]["header"]["title"]["content"]

    def patch_header(self, idx: int = -1) -> str:
        return self.patches[idx]["card"]["header"]["title"]["content"]


RESULTS: list[tuple[bool, str, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    RESULTS.append((bool(cond), name, detail))


def run() -> None:
    import os

    os.environ["EVOTEAMS_ENABLED"] = "1"
    os.environ.pop("EVOTEAMS_SOFT_CARDS", None)
    os.environ.pop("EVOTEAMS_DRY_RUN", None)

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)

        # -- the format gate itself ---------------------------------------
        import maintenance

        check("real batch notice is recognised as an EVO ※SD※ batch",
              maintenance.is_evo_sd_batch_paste(BATCH_NOTICE))
        check("batch splits into 8 blocks",
              len(maintenance.split_evo_sd_batch_blocks(BATCH_NOTICE)) == 8,
              f"got {len(maintenance.split_evo_sd_batch_blocks(BATCH_NOTICE))}")
        check("chatter is NOT a batch",
              not maintenance.is_evo_sd_batch_paste(CHATTER))

        # -- the soft tier must not fire on ordinary CS-group traffic ------
        # These are the shapes that used to pass on one weak signal alone: a table
        # name, or a "cancel/prolong" step kind. Every soft match is now reported
        # on /teamstatus as "a real notice may have been missed", so chatter
        # getting in there would bury the case that warning exists for.
        import detectevomaintenance as _ev

        for chat in ("is Speed Baccarat 3 back up? players are complaining",
                     "maintenance for Blackjack Classic 59 has been cancelled",
                     "Dragon Tiger Relâmpago done na, thanks",
                     CHATTER):
            v = _ev.classify(chat)
            check(f"chatter not classified as a notice: {chat[:38]!r}",
                  not v["is_notice"], f"why={v['why']}")
        v = _ev.classify(SOFT_NOTICE)
        check("a genuine non-※SD※ notice IS still classified as soft",
              v["is_notice"] and not v["m_ready"], f"verdict={v}")
        v = _ev.classify(BATCH_NOTICE)
        check("the ※SD※ batch is classified /m-ready",
              v["is_notice"] and v["m_ready"], f"verdict={v}")

        # -- 1. first poll baselines --------------------------------------
        h = Harness(tmp)
        out = h.poll([_row("1787000000000", CHATTER),
                      _row("1787002985728", BATCH_NOTICE)])
        check("first poll baselines instead of carding",
              out["baselined"] and out["cards"] == 0 and not h.cards,
              f"out={out} cards={len(h.cards)}")

        # -- 2. a genuinely new notice cards exactly once ------------------
        out = h.poll([_row("1787002985728", BATCH_NOTICE),
                      _row("1787009999999", SECOND_NOTICE)])
        check("a new notice past the cursor is carded",
              out["new"] == 1 and out["cards"] == 1 and len(h.cards) == 1,
              f"out={out} cards={len(h.cards)}")
        check("card went to the configured chat",
              h.cards[0]["chat_id"] == h.evom.CARD_CHAT_ID,
              h.cards[0]["chat_id"])
        check("card has Generate Email + Cancel buttons",
              json.dumps(h.cards[0]["card"]).count('"tag": "button"') == 2)
        check("full notice posted as a threaded reply under the card",
              h.replies and h.replies[0][0] == h.cards[0]["mid"]
              and SECOND_NOTICE.strip() in h.replies[0][1])
        check("card shows the Teams post time, not just the detect time",
              "Posted in Teams:" in json.dumps(h.cards[0]["card"],
                                               ensure_ascii=False))

        # -- 3. re-polling the same rows cards nothing ---------------------
        before = len(h.cards)
        out = h.poll([_row("1787002985728", BATCH_NOTICE),
                      _row("1787009999999", SECOND_NOTICE)])
        check("re-polling the same rows cards nothing",
              len(h.cards) == before and out["cards"] == 0, f"out={out}")

        # -- 4. two notices in one interval both card ---------------------
        h2 = Harness(tmp / "b")
        (tmp / "b").mkdir()
        h2.poll([_row("1000", CHATTER)])                       # baseline
        out = h2.poll([_row("1000", CHATTER),
                       _row("2000", BATCH_NOTICE),
                       _row("3000", SECOND_NOTICE)])
        check("two notices inside one poll interval BOTH card",
              out["new"] == 2 and out["cards"] == 2 and len(h2.cards) == 2,
              f"out={out} cards={len(h2.cards)}")

        # -- 5. chatter never cards --------------------------------------
        h3 = Harness(tmp / "c")
        (tmp / "c").mkdir()
        h3.poll([_row("1000", CHATTER)])
        out = h3.poll([_row("1000", CHATTER), _row("2000", CHATTER + " again")])
        check("chatter is not carded", out["cards"] == 0 and not h3.cards,
              f"out={out}")

        # -- 6. soft (non-※SD※) notice is recorded but not carded ---------
        out = h3.poll([_row("3000", SOFT_NOTICE)])
        soft_key = h3.evom.content_key(SOFT_NOTICE)
        rec = h3.evom.get_record(soft_key)
        check("notice-shaped non-※SD※ message is NOT carded",
              out["cards"] == 0 and not h3.cards, f"out={out}")
        check("...but it IS recorded as soft_skipped, so it is visible",
              rec.get("outcome") == "soft_skipped", f"rec={rec}")
        check("...and /teamstatus warns about it",
              any("UNCARDED" in ln for ln in h3.evom.status_lines()))

        # -- 7. a failed card post holds the cursor back -----------------
        h4 = Harness(tmp / "d")
        (tmp / "d").mkdir()
        h4.poll([_row("1000", CHATTER)])
        h4.card_send_ok = False
        out = h4.poll([_row("2000", BATCH_NOTICE)])
        cur_after_fail = h4.evom.get_last_seen(GROUP).get("message_id")
        check("a refused card post reports card_failed and holds the cursor",
              out["cards"] == 0 and out.get("retry_pending")
              and cur_after_fail == "1000",
              f"out={out} cursor={cur_after_fail}")
        h4.card_send_ok = True
        out = h4.poll([_row("2000", BATCH_NOTICE)])
        check("the next poll RETRIES it and the card lands",
              out["cards"] == 1 and len(h4.cards) == 1, f"out={out}")

        # -- 8. a read that did not reach the end is refused --------------
        h5 = Harness(tmp / "e")
        (tmp / "e").mkdir()
        h5.poll([_row("1000", CHATTER)])
        out = h5.poll([_row("2000", BATCH_NOTICE)], at_bottom=False)
        check("a read that never reached the end of the chat is refused",
              out["cards"] == 0 and "did not reach the end" in out["why"],
              f"out={out}")
        check("...and the cursor did not move",
              h5.evom.get_last_seen(GROUP).get("message_id") == "1000")

        # -- 9. the cursor never regresses -------------------------------
        h5.evom.set_last_seen(GROUP, "500")
        check("cursor refuses to move backwards",
              h5.evom.get_last_seen(GROUP).get("message_id") == "1000")

        # -- 10. double-tap sends the email exactly once ------------------
        h6 = Harness(tmp / "f")
        (tmp / "f").mkdir()
        h6.poll([_row("1000", CHATTER)])
        h6.poll([_row("2000", BATCH_NOTICE)])
        key = h6.evom.content_key(BATCH_NOTICE)
        h6.m_delay = 0.35                       # a /m run takes real time
        t1 = threading.Thread(target=h6.tap, args=(key,))
        t2 = threading.Thread(target=h6.tap, args=(key,))
        t1.start(); t2.start(); t1.join(); t2.join()
        for _ in range(40):
            if h6.evom.get_record(key).get("outcome") in ("emailed", "pending"):
                break
            import time as _t
            _t.sleep(0.05)
        check("two simultaneous taps run the /m pipeline exactly ONCE",
              len(h6.m_runs) == 1, f"runs={len(h6.m_runs)}")
        check("the emailed notice is recorded emailed",
              h6.evom.get_record(key).get("outcome") == "emailed",
              str(h6.evom.get_record(key).get("outcome")))
        check("a later tap is refused",
              "nothing to do" in json.dumps(h6.tap(key)), json.dumps(h6.tap(key)))
        check("buttons come off BEFORE the run, not after",
              any("Generating" in p["card"]["header"]["title"]["content"]
                  for p in h6.patches)
              and all('"tag": "button"' not in json.dumps(p["card"])
                      for p in h6.patches),
              json.dumps([p["card"]["header"]["title"]["content"]
                          for p in h6.patches]))

        # -- 11. a run that emailed nothing stays retryable ---------------
        h7 = Harness(tmp / "g")
        (tmp / "g").mkdir()
        h7.poll([_row("1000", CHATTER)])
        h7.poll([_row("2000", BATCH_NOTICE)])
        key = h7.evom.content_key(BATCH_NOTICE)
        h7.m_result = {"ok": True, "email_sent": False,
                       "reason": "no CP-launched games in the notice"}
        h7.tap(key)
        for _ in range(40):
            if h7.evom.get_record(key).get("last_error"):
                break
            import time as _t
            _t.sleep(0.05)
        rec = h7.evom.get_record(key)
        check("a /m run that sent NO email is not recorded as emailed",
              rec.get("outcome") == "pending", f"outcome={rec.get('outcome')}")
        check("...the buttons are restored so it can be retried",
              any('"tag": "button"' in json.dumps(p["card"]) for p in h7.patches),
              json.dumps([p["card"]["header"]["title"]["content"]
                          for p in h7.patches]))
        check("...and the group is told why nothing was sent",
              any("No EVO maintenance email was sent" in t for _c, t in h7.texts),
              json.dumps([t for _c, t in h7.texts])[:200])

        # -- 12. Cancel is refused mid-send ------------------------------
        h8 = Harness(tmp / "h")
        (tmp / "h").mkdir()
        h8.poll([_row("1000", CHATTER)])
        h8.poll([_row("2000", BATCH_NOTICE)])
        key = h8.evom.content_key(BATCH_NOTICE)
        h8.m_delay = 0.5
        threading.Thread(target=h8.tap, args=(key,)).start()
        import time as _t
        _t.sleep(0.15)                          # while the /m run is in flight
        resp = h8.tap(key, action="evom_cancel")
        # Either refusal is correct: the _TERMINAL guard usually answers first
        # ("Already generating"), and the claim() race-loser path answers
        # "Too late". What must NOT happen is the cancel taking effect.
        blob = json.dumps(resp)
        check("Cancel is refused once the email is already generating",
              ("Already generating" in blob or "Too late" in blob)
              and h8.evom.get_record(key).get("outcome") != "cancelled", blob)
        _t.sleep(0.6)
        check("...and the email still went out exactly once",
              len(h8.m_runs) == 1, f"runs={len(h8.m_runs)}")
        check("...and the notice ends up emailed, not cancelled",
              h8.evom.get_record(key).get("outcome") == "emailed",
              str(h8.evom.get_record(key).get("outcome")))

        # -- 13. dry run posts nothing -----------------------------------
        os.environ["EVOTEAMS_DRY_RUN"] = "1"
        h9 = Harness(tmp / "i")
        (tmp / "i").mkdir()
        h9.poll([_row("1000", CHATTER)])
        out = h9.poll([_row("2000", BATCH_NOTICE)])
        check("EVOTEAMS_DRY_RUN detects but posts nothing",
              out["new"] == 1 and not h9.cards
              and h9.evom.get_record(h9.evom.content_key(BATCH_NOTICE))
                        .get("outcome") == "dry_run",
              f"out={out} cards={len(h9.cards)}")
        os.environ.pop("EVOTEAMS_DRY_RUN", None)

        # -- 14. the poll loop <-> detector seam -------------------------
        # The bug this whole change fixes was that NOTHING called the detector.
        # Stub the browser out and drive _TeamsWarm._do_poll directly, so the
        # wiring itself is covered and not just each side of it.
        import teamswatch as tw

        h11 = Harness(tmp / "k")
        (tmp / "k").mkdir()
        served: list[dict] = []
        rows_to_serve = [_row("1000", CHATTER)]

        def fake_read_on_page(page, target, limit, *, shooter=None):
            served.append({"target": target, "limit": limit})
            return {"ok": True, "group": target, "messages": list(rows_to_serve),
                    "counts": {}, "shot": None, "error": None,
                    "at_bottom": True, "newest_mid": rows_to_serve[-1]["mid"],
                    "rendered_rows": len(rows_to_serve), "matched": "stub"}

        real_read_on_page = tw._read_on_page
        tw._read_on_page = fake_read_on_page
        try:
            w = tw._TeamsWarm()
            w._ready = lambda: True          # pretend the browser is up
            w._page = object()
            w._do_poll()                                     # first: baseline
            check("the poll reads with limit=0 so a burst cannot be truncated",
                  served and served[0]["limit"] == 0, str(served))
            check("the poll reads the watched group",
                  served[0]["target"] == GROUP, str(served))
            check("first poll through the loop cards nothing",
                  not h11.cards and w.stats()["polls"] == 1,
                  f"cards={len(h11.cards)} stats={w.stats()}")

            rows_to_serve.append(_row("2000", BATCH_NOTICE))
            w._do_poll()                                     # then: a real notice
            check("a new notice arriving via the poll loop is carded",
                  len(h11.cards) == 1, f"cards={len(h11.cards)}")
            check("the watcher counts the card it posted",
                  w.stats()["cards"] == 1 and w.stats()["new_msgs"] == 1,
                  str(w.stats()))

            w._do_poll()                                     # and: no repeat
            check("polling again does not re-card",
                  len(h11.cards) == 1 and w.stats()["polls"] == 3,
                  f"cards={len(h11.cards)} stats={w.stats()}")

            # A failing read must not be mistaken for "no new messages".
            def failing_read(page, target, limit, *, shooter=None):
                return {"ok": False, "group": target, "messages": [],
                        "counts": {}, "error": "pane not resolved: stub",
                        "shot": None}

            tw._read_on_page = failing_read
            w._healthy = lambda: False       # skip the _stage_of probe
            w._do_poll()
            check("a failed read is recorded as a failure, not as 'nothing new'",
                  w.stats()["last_poll_ok"] is False
                  and w.stats()["consec_fail"] == 1
                  and "pane not resolved" in str(w.stats()["last_poll_error"]),
                  str(w.stats()))

            # Standing aside for a --login is normal, not a failure: if it counted
            # as one, four ticks of backoff would put the watcher on a 16-minute
            # interval and it would crawl back long after the login finished.
            w._ready = lambda: False
            real_yield = tw._yield_requested
            tw._yield_requested = lambda: True
            try:
                w._do_poll()
                check("yielding the profile is NOT counted as a failed poll",
                      w.stats()["consec_fail"] == 0
                      and "standing aside" in str(w.stats()["last_poll_error"]),
                      str(w.stats()))
            finally:
                tw._yield_requested = real_yield
        finally:
            tw._read_on_page = real_read_on_page

        # -- 15. wrong group is ignored ----------------------------------
        h10 = Harness(tmp / "j")
        (tmp / "j").mkdir()
        out = h10.evom.handle_new_messages(
            group="Some Other Chat Entirely", messages=[_row("2000", BATCH_NOTICE)])
        check("a message from another group is ignored",
              out["cards"] == 0 and "not the watched group" in out["why"],
              f"out={out}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="also print the cases that passed")
    args = ap.parse_args()

    try:
        run()
    except Exception as err:  # noqa: BLE001
        import traceback

        traceback.print_exc()
        print(f"\nHARNESS CRASHED: {err!r}")
        return 2

    failed = [r for r in RESULTS if not r[0]]
    for ok, name, detail in RESULTS:
        if ok and args.verbose:
            print(f"  PASS  {name}")
        elif not ok:
            print(f"  FAIL  {name}" + (f"\n          -> {detail}" if detail else ""))
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
