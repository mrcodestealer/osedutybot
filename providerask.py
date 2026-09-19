#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""``/provideraskmaintenance`` — ask every provider group about this week, then
collect and file the answers.

FLOW
  1. Read the maintenance Base. Take every Telegram row except the excluded
     groups (VA announcements and the BTi support group) and the rows with no
     Group Name — the same partition ``/telegramgroupcheck`` uses.
  2. Send "Hi team, are there any maintenance plans for this week?" into each
     group, PINNED TO ITS PEER ID, paced, stopping on the first anomaly.
  3. For the next hour, re-read each group every ~10 minutes. Messages newer than
     our ask go to qwen3.6:35b-a3b, which decides whether they answer US; the
     window itself is parsed by ``noticeparse`` so a hallucinated time cannot
     reach the sheet.
  4. Fill the provider row: a window -> Start/End/Remark; "no maintenance" ->
     Remark = "No maintenance". Last Check is stamped either way.
  5. At the hour, post a summary card: filled, answered-but-unclear, silent.

SAFETY, because this is the only code in the repo that writes to external
partner groups from a real personal account:
  * every send is pinned to a peer id harvested by /telegramgroupcheck
    (``peerstore``). A group with no pin is REPORTED, never messaged — the send
    route runs with allow_search=False precisely so a same-titled stranger can
    never be a target, and without a pin most groups would not resolve at all.
  * sends are paced with a randomised gap and the run ABORTS on the first send
    that does not confirm. A failure is almost always systemic (flood wait, slow
    mode, a changed send-with-Enter setting) and would repeat for every group.
  * one run at a time, and the whole run is journalled to disk so a restart
    mid-window neither loses the collection nor re-asks anybody.
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

_ROOT_DIR = Path(__file__).resolve().parent
STATE_PATH = Path(os.getenv("PROVIDERASK_STATE")
                  or (_ROOT_DIR / "providerask_state.json"))

ASK_TEXT = (os.getenv("PROVIDERASK_TEXT")
            or "Hi team, are there any maintenance plans for this week?")


def _int_env(name: str, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(os.getenv(name, str(default)))))
    except ValueError:
        return default


def _gap_range() -> tuple:
    """Randomised seconds between sends.

    Not politeness — flood protection. Eighteen identical messages from one
    personal account inside a few minutes is a textbook spam signature, and the
    penalty (PEER_FLOOD: restricted from messaging groups, often for days) lands
    on a real account this bot cannot re-create. Randomised so the cadence does
    not itself look scripted.
    """
    lo = _int_env("PROVIDERASK_GAP_MIN_SEC", 30, 5, 600)
    hi = _int_env("PROVIDERASK_GAP_MAX_SEC", 60, lo, 900)
    return lo, hi


def _window_minutes() -> int:
    return _int_env("PROVIDERASK_WINDOW_MIN", 60, 5, 24 * 60)


def _sweep_minutes() -> int:
    return _int_env("PROVIDERASK_SWEEP_MIN", 10, 2, 120)


def _read_count() -> int:
    return _int_env("PROVIDERASK_READ_COUNT", 12, 1, 50)


def _card_chat_id() -> str:
    return (os.getenv("PROVIDERASK_CARD_CHAT_ID")
            or os.getenv("LABORATORY_GROUP")
            or "oc_ad9b5bdbb2826ba2ee9730920ef25432").strip()


def _tz():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(os.getenv("PROVIDERASK_TZ", "Asia/Manila"))
    except Exception:
        from datetime import timezone

        return timezone(timedelta(hours=8))


def _now() -> datetime:
    return datetime.now(_tz())


def _now_str() -> str:
    return _now().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Run state — journalled so a restart does not lose the window
# ---------------------------------------------------------------------------

_state_lock = threading.Lock()


def load_state() -> dict:
    try:
        with open(STATE_PATH, encoding="utf-8") as fh:
            d = json.load(fh)
        if isinstance(d, dict) and d.get("providers") is not None:
            return d
    except Exception:
        pass
    return {}


def save_state(state: dict) -> None:
    with _state_lock:
        try:
            tmp = STATE_PATH.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(state, fh, ensure_ascii=False, indent=1)
            tmp.replace(STATE_PATH)
        except Exception as err:  # noqa: BLE001
            print(f"[providerask] could not save state: {err!r}", flush=True)


def run_active() -> bool:
    """Is the collection window still open? (used to refuse a second run)"""
    st = load_state()
    if not st or st.get("finished"):
        return False
    try:
        deadline = datetime.fromisoformat(st["deadline"])
    except Exception:
        return False
    return _now() < deadline


def run_open() -> bool:
    """Is there a run that has not been CLOSED OUT yet?

    Distinct from run_active on purpose. The sweep loop has to keep firing after
    the deadline passes, because the final sweep is what posts the summary card
    and marks the run finished. Gating the loop on run_active meant that at the
    very moment the summary became due the loop stopped enqueueing, so a run
    just silently never ended and the state file stayed open forever - blocking
    every later /provideraskmaintenance with "already running".
    """
    st = load_state()
    return bool(st) and not st.get("finished")


# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

def targets() -> dict:
    """{ask: [...], unpinned: [...], skipped: [...]} straight from the Base.

    Reuses groupcheck's partition so the exclusion list and the blank-row rule
    stay defined in exactly one place.
    """
    import groupcheck as gc
    import peerstore

    rows = gc.fetch_rows()
    telegram, _teams, skipped = gc.partition(rows)
    # groupcheck's exclusion list governs what is CHECKED. Asking has a second,
    # stricter rule: a broadcast announcements channel is somewhere we listen,
    # not somewhere we post a question nobody there will answer.
    no_ask = _never_ask()
    for row in list(telegram):
        if _norm(row.get("group")) in no_ask or _norm(row.get("provider")) in no_ask:
            row["why"] = "announcement-only group - watched, never asked"
            skipped.append(row)
            telegram.remove(row)
    ask, unpinned = [], []
    for row in telegram:
        peer = peerstore.peer_for(row.get("group") or "")
        if peer:
            ask.append({**row, "peer": peer})
        else:
            unpinned.append(row)
    return {"ask": ask, "unpinned": unpinned, "skipped": skipped,
            "rows": len(rows)}


# ---------------------------------------------------------------------------
# Filing an answer into the Base
# ---------------------------------------------------------------------------

def _norm(value) -> str:
    return " ".join(str(value or "").split()).casefold()


def _never_ask() -> set:
    """Groups to watch but never post into.

    Separate from groupcheck's GROUPCHECK_SKIP: that one governs whether a group
    is CHECKED at all. This one governs whether we speak in it.
    """
    raw = os.getenv("PROVIDERASK_NEVER_ASK")
    if raw is None:
        raw = "\U0001F195VA \u516c\u544a\uff5cVA announcements"
    import re as _re

    return {_norm(p) for p in _re.split(r"[\n;]+", raw) if p.strip()}


def file_answer(row: dict, verdict: dict, text: str) -> dict:
    """Write one provider's answer. -> {wrote, error}"""
    import vawatch as va

    out = {"wrote": "", "error": ""}
    try:
        found = va.find_provider_row(row.get("provider") or "")
        if not found:
            raise RuntimeError(f"no Base row for {row.get('provider')!r}")
        fields: dict[str, Any] = {"Last Check": va._ms(_now())}
        if verdict["action"] == "fill":
            fields["Start Time"] = va._ms(verdict["start"])
            fields["End Time"] = va._ms(verdict["end"])
            fields["Remark"] = text
            out["wrote"] = (f"{verdict['start']:%Y-%m-%d %H:%M} → "
                            f"{verdict['end']:%H:%M}"
                            + (" (rescheduled)" if verdict.get("reschedule") else ""))
        else:                       # 'clear'
            fields["Remark"] = "No maintenance"
            # Blank the window too. The Base's 'Maintenance' column is
            # IF(ISBLANK(Start Time), "No", "Yes"), so leaving last week's dates
            # in place leaves the sheet reading "Yes" next to a Remark that says
            # there is none - and the row contradicts itself.
            fields["Start Time"] = None
            fields["End Time"] = None
            out["wrote"] = "No maintenance (window cleared)"
        va.update_row(found["record_id"], fields)
    except Exception as err:        # noqa: BLE001
        out["error"] = repr(err)
        print(f"[providerask] filing {row.get('provider')!r} failed: {err!r}",
              flush=True)
    return out


def judge(text: str, *, group: str, asked_at: str, sender: str = "",
          quoted: str = "") -> dict:
    """LLM decides relevance, noticeparse decides the times.

    Returns the noticeparse verdict, with 'action' downgraded to 'ignore' when
    the model says the message is not addressed to us. An unreachable or
    undecided model yields 'needs_human' rather than a write: the sheet is
    shared and a wrong entry is worse than a gap.
    """
    import noticeparse as np
    import providerllm as pl

    parsed = np.classify(text)
    if parsed["action"] == "needs_human":
        out = {**parsed, "llm": {"ok": False, "why": "not consulted"}}
        return out
    llm = pl.classify_reply(text, group=group, asked_at=asked_at,
                            sender=sender, quoted_text=quoted)
    out = {**parsed, "llm": llm}

    if not llm.get("ok"):
        out["action"] = "needs_human"
        out["reason"] = f"LLM unavailable ({llm.get('why')})"
        return out
    if llm["verdict"] == "unrelated" or not llm.get("answers_us"):
        out["action"] = "ignore"
        out["reason"] = f"not addressed to us ({llm.get('why')})"
        return out
    if llm["confidence"] < pl.min_confidence():
        out["action"] = "needs_human"
        out["reason"] = (f"low confidence {llm['confidence']:.2f} "
                         f"({llm.get('why')})")
        return out

    if llm["verdict"] == "no_maintenance":
        # "Nothing planned" is only believable when the message carries NO
        # window. Two separate ways this used to go wrong:
        #   * parse said fill -> the old code passed that straight through,
        #     skipping the staleness guard its sibling branch applies, so a
        #     provider quoting an ancient notice wrote dead dates over a live row;
        #   * parse said anything else -> the old code read that as "no window",
        #     but parse only EXPOSES start/end on the fill path, so a message
        #     that plainly announced a window got "No maintenance" stamped on it.
        # Asking find_window directly settles both.
        if np.find_window(text) is not None:
            out["action"] = "needs_human"
            out["reason"] = ("says there is no maintenance, but the message also "
                             "carries a maintenance window - refusing to guess")
            return out
        out["action"] = "clear"
        out["reason"] = "provider says no maintenance"
        return out

    if llm["verdict"] == "maintenance":
        if parsed["action"] == "fill" and parsed.get("stale"):
            out["action"] = "needs_human"
            out["reason"] = ("announces a window that has already passed - "
                             "probably quoting an old notice")
        elif parsed["action"] != "fill":
            # The model sees maintenance but no window could be parsed - this is
            # the reply-quote case, where the times live in the quoted message.
            out["action"] = "needs_human"
            out["reason"] = ("says maintenance but no window in this message "
                             "(likely quoting another message)")
        return out

    out["action"] = "needs_human"
    out["reason"] = f"model unsure ({llm.get('why')})"
    return out


# ---------------------------------------------------------------------------
# Cards
# ---------------------------------------------------------------------------

def _div(md: str) -> dict:
    return {"tag": "div", "text": {"tag": "lark_md", "content": md}}


def _clamp(t: str, n: int = 3200) -> str:
    return t if len(t) <= n else t[:n] + "\n…"


def _card(title: str, template: str, elements: list) -> dict:
    return {"schema": "2.0",
            "config": {"update_multi": True, "width_mode": "fill"},
            "header": {"template": template,
                       "title": {"tag": "plain_text", "content": title[:100]}},
            "body": {"elements": elements}}


def build_summary_card(state: dict) -> dict:
    provs = state.get("providers") or {}
    filled = [(k, v) for k, v in provs.items() if v.get("outcome") == "filled"]
    cleared = [(k, v) for k, v in provs.items() if v.get("outcome") == "no_maintenance"]
    human = [(k, v) for k, v in provs.items() if v.get("outcome") == "needs_human"]
    silent = [(k, v) for k, v in provs.items()
              if v.get("outcome") in (None, "", "waiting") and v.get("asked")]
    unsent = [(k, v) for k, v in provs.items() if not v.get("asked")]

    el = [_div(
        f"**Asked {sum(1 for v in provs.values() if v.get('asked'))} provider "
        f"group(s) at {state.get('asked_at', '?')}**\n"
        f"Collected for {state.get('window_min', 60)} minutes.\n"
        f"✅ filled: **{len(filled)}**   🟢 no maintenance: **{len(cleared)}**   "
        f"❓ needs a human: **{len(human)}**   ⌛ no reply: **{len(silent)}**")]

    def block(head, rows, fmt):
        if not rows:
            return
        el.append({"tag": "hr"})
        el.append(_div(_clamp(head + "\n" + "\n".join(fmt(k, v) for k, v in rows))))

    block("**✅ Maintenance filled into the sheet:**", filled,
          lambda k, v: f"• **{k}** — {v.get('wrote') or ''}")
    block("**🟢 Answered: no maintenance this week:**", cleared,
          lambda k, v: f"• **{k}**")
    block("**❓ Answered, but needs a human:**", human,
          lambda k, v: f"• **{k}** — _{v.get('why') or ''}_")
    block("**⌛ No update from the provider:**", silent,
          lambda k, v: f"• **{k}**")
    block("**⚠️ Not asked:**", unsent,
          lambda k, v: f"• **{k}** — _{v.get('why') or 'not sent'}_")

    if state.get("aborted"):
        el.append({"tag": "hr"})
        el.append(_div(f"**⚠️ The send run stopped early:** "
                       f"_{state.get('aborted')}_"))

    el.append({"tag": "hr"})
    el.append(_div(f"_{_now_str()}_"))
    template = "green" if (filled or cleared) and not human else (
        "orange" if (filled or cleared or human) else "red")
    return _card(f"📋 Provider maintenance — {len(filled) + len(cleared)} answered, "
                 f"{len(silent)} silent", template, el)


def build_hit_card(provider: str, group: str, verdict: dict, text: str,
                   wrote: str) -> dict:
    v = verdict
    head = f"**{provider}** · {group}"
    if v["action"] == "fill":
        head += (f"\n**Start:** {v['start']:%Y-%m-%d %H:%M}"
                 f"\n**End:** {v['end']:%Y-%m-%d %H:%M}")
        if v.get("reschedule"):
            head += "\n_Rescheduled — this overrides the previous window._"
    else:
        head += "\n**No maintenance this week**"
    el = [_div(head), {"tag": "hr"}, _div(_clamp(text)), {"tag": "hr"},
          _div(f"_Base row updated · {wrote} · {_now_str()}_" if wrote
               else f"_⚠️ the Base row was NOT updated · {_now_str()}_")]
    icon = "🛠️" if v["action"] == "fill" else "🟢"
    return _card(f"{icon} {provider} — maintenance answer", "orange" if wrote else "red", el)


# ---------------------------------------------------------------------------
# Runtime — driven from the Telegram worker thread
# ---------------------------------------------------------------------------
# telegramwarm supplies the two closures over its live page; this module never
# touches Playwright. Same split as vawatch/detectevomaintenance: the browser
# driver stays a driver, the decision logic stays testable.

def begin(chat_id: str) -> dict:
    """Plan the run and journal it. Sends nothing."""
    plan = targets()
    state = {
        "started_at": _now_str(),
        "asked_at": "",
        "deadline": (_now() + timedelta(minutes=_window_minutes())).isoformat(),
        "window_min": _window_minutes(),
        "chat_id": chat_id,
        "finished": False,
        "aborted": "",
        "providers": {},
    }
    for row in plan["ask"]:
        state["providers"][row.get("provider") or row.get("group")] = {
            "group": row.get("group"), "peer": row.get("peer"),
            "asked": False, "outcome": "", "why": "", "wrote": "", "seen": [],
        }
    for row in plan["unpinned"]:
        state["providers"][row.get("provider") or row.get("group")] = {
            "group": row.get("group"), "peer": "", "asked": False,
            "outcome": "", "wrote": "", "seen": [],
            "why": "no peer id pinned — run /telegramgroupcheck first",
        }
    save_state(state)
    return {"state": state, "plan": plan}


def next_to_ask() -> dict:
    """The next pinned provider still to ask, or {}. Cheap; safe to poll."""
    st = load_state()
    if not st or st.get("finished") or st.get("aborted"):
        return {}
    for prov, rec in (st.get("providers") or {}).items():
        if rec.get("peer") and not rec.get("asked"):
            return {"provider": prov, **rec}
    return {}


def _arm_window(st: dict) -> None:
    """Start the collection clock from the last send, not the first."""
    if not st.get("asked_at"):
        st["asked_at"] = _now_str()
    st["deadline"] = (_now() + timedelta(minutes=_window_minutes())).isoformat()
    save_state(st)


def ask_one(send_one, notify) -> dict:
    """Ask exactly ONE provider. -> {done, sent, aborted}

    One provider per worker task, with the gap slept on the TIMER thread between
    tasks. The pacing is unchanged - it is flood protection and still 30-60s -
    but the worker is free between sends, so /vacheck, /checktelegramgroup, the
    keepalive and the VA watcher are not blocked for a quarter of an hour.
    """
    st = load_state()
    if not st or st.get("finished") or st.get("aborted"):
        return {"done": True, "sent": False, "aborted": (st or {}).get("aborted", "")}

    target = None
    for prov, rec in (st.get("providers") or {}).items():
        if rec.get("peer") and not rec.get("asked"):
            target = (prov, rec)
            break
    if target is None:
        _arm_window(st)
        return {"done": True, "sent": False, "aborted": ""}

    prov, rec = target
    try:
        res = send_one(rec["group"], ASK_TEXT, rec["peer"])
    except Exception as err:  # noqa: BLE001
        res = {"ok": False, "status": "error", "reason": repr(err)}

    status = res.get("status")
    if res.get("ok") or status == "already_sent":
        rec["asked"] = True
        rec["asked_at"] = _now_str()
        rec["outcome"] = "waiting"
        _arm_window(st)
        return {"done": False, "sent": True, "aborted": ""}

    # Stop the whole run. A send that does not confirm is almost always systemic
    # - a flood wait, group slow mode, or a changed send-with-Enter setting -
    # and every remaining group would hit it too.
    why = f"{prov}: {res.get('reason') or status or 'send not confirmed'}"
    rec["why"] = why
    st["aborted"] = why
    save_state(st)
    try:
        notify(f"\u26d4 Stopped at {prov} - {why}\nNothing further was sent. This "
               f"usually means a flood wait or a group setting, which would "
               f"affect the rest too.")
    except Exception:
        pass
    return {"done": True, "sent": False, "aborted": why}


def do_ask(send_one, notify) -> dict:
    """Send the question to every pinned provider. Paced, aborts on anomaly.

    ``send_one(title, text, pin) -> {ok, status, reason, ...}``
    """
    state = load_state()
    provs = state.get("providers") or {}
    lo, hi = _gap_range()
    todo = [(p, v) for p, v in provs.items() if v.get("peer") and not v.get("asked")]
    sent = 0

    for idx, (prov, rec) in enumerate(todo):
        try:
            res = send_one(rec["group"], ASK_TEXT, rec["peer"])
        except Exception as err:  # noqa: BLE001
            res = {"ok": False, "status": "error", "reason": repr(err)}

        status = res.get("status")
        if res.get("ok") or status == "already_sent":
            rec["asked"] = True
            rec["asked_at"] = _now_str()
            rec["outcome"] = "waiting"
            sent += 1
        else:
            # Stop the whole run. A send that does not confirm is almost always
            # systemic - a flood wait, group slow mode, or a changed
            # send-with-Enter setting - and every remaining group would hit it
            # too, turning one problem into eighteen.
            why = (f"{prov}: {res.get('reason') or status or 'send not confirmed'}")
            rec["why"] = why
            state["aborted"] = why
            save_state(state)
            try:
                notify(f"⛔ Stopped after {sent} of {len(todo)} sends — {why}\n"
                       f"Nothing further was sent. This usually means a flood "
                       f"wait or a group setting, which would affect the rest too.")
            except Exception:
                pass
            break

        save_state(state)
        if idx < len(todo) - 1:
            time.sleep(random.uniform(lo, hi))

    if not state.get("asked_at") and sent:
        state["asked_at"] = _now_str()
    state["deadline"] = (_now() + timedelta(minutes=_window_minutes())).isoformat()
    save_state(state)
    return {"sent": sent, "planned": len(todo), "aborted": state.get("aborted")}


def _after_our_ask(messages: list) -> list:
    """Only the messages that came AFTER our own question.

    Without this, a maintenance notice the provider posted BEFORE we asked is
    read as their answer and written to the row. Our own ask is in the
    transcript as an outgoing bubble, so its position IS the boundary - no extra
    state to keep and nothing to get stale. If our ask is not in the window that
    was read, nothing is judged: better silent than wrong.
    """
    want = " ".join(ASK_TEXT.split()).casefold()
    cut = -1
    for i, m in enumerate(messages or []):
        if not m.get("out"):
            continue
        body = " ".join(str(m.get("text") or "").split()).casefold()
        if want and want in body:
            cut = i
    return list(messages or [])[cut + 1:] if cut >= 0 else []


def _message_is_new(rec: dict, msg: dict) -> bool:
    """Only messages we have not already judged. Content-hashed, because the
    Telegram read gives no reliable ordering cursor."""
    import hashlib

    body = " ".join(str(msg.get("text") or "").split())
    if not body:
        return False
    mid = str(msg.get("mid") or "").strip()
    key = f"mid:{mid}" if mid else "sha:" + hashlib.sha1(
        body.encode("utf-8")).hexdigest()[:16]
    if key in (rec.get("seen") or []):
        return False
    rec.setdefault("seen", []).append(key)
    return True


def do_sweep(read_one, notify, send_card) -> dict:
    """One collection pass. Files what it can, cards each hit, and at the
    deadline posts the summary and closes the run.

    ``read_one(title, count) -> {ok, messages: [...], error}``
    """
    state = load_state()
    if not state or state.get("finished"):
        return {"done": True, "reason": "no active run"}

    provs = state.get("providers") or {}
    asked_at = state.get("asked_at") or state.get("started_at") or ""
    waiting = [(p, v) for p, v in provs.items()
               if v.get("asked") and v.get("outcome") in ("", "waiting")]
    checked = filed = 0

    for prov, rec in waiting:
        try:
            res = read_one(rec["group"], _read_count())
        except Exception as err:  # noqa: BLE001
            # A group that cannot be opened is reported, never guessed at: it
            # stays "waiting" and appears in the summary as no-reply.
            print(f"[providerask] read {prov} failed: {err!r}", flush=True)
            rec["why"] = repr(err)[:200]
            continue
        checked += 1
        if not res.get("ok"):
            continue

        msgs = _after_our_ask(res.get("messages") or [])
        # Newest first: a provider who posts a notice and then a correction
        # inside one sweep interval must have the CORRECTION win, not the notice
        # it superseded.
        for msg in reversed(msgs):
            if msg.get("out"):            # our own message
                continue
            if not _message_is_new(rec, msg):
                continue
            text = str(msg.get("text") or "")
            try:
                verdict = judge(text, group=rec["group"], asked_at=asked_at,
                                sender=str(msg.get("sender") or ""))
            except Exception as err:  # noqa: BLE001
                # One unparseable message must not abort the sweep: the deadline
                # block at the end is what posts the summary and CLOSES the run,
                # so an escape here would leave the run open forever and block
                # every later /provideraskmaintenance.
                print(f"[providerask] judging a message for {prov} failed: "
                      f"{err!r}", flush=True)
                continue
            action = verdict["action"]
            if action in ("fill", "clear"):
                done = file_answer({"provider": prov, "group": rec["group"]},
                                   verdict, text)
                rec["outcome"] = "filled" if action == "fill" else "no_maintenance"
                rec["wrote"] = done["wrote"]
                rec["why"] = done["error"]
                filed += 1
                try:
                    send_card(build_hit_card(prov, rec["group"], verdict, text,
                                             done["wrote"]))
                except Exception as err:  # noqa: BLE001
                    print(f"[providerask] hit card failed: {err!r}", flush=True)
                break
            if action == "needs_human":
                rec["outcome"] = "needs_human"
                rec["why"] = verdict.get("reason") or ""
                break
        save_state(state)

    try:
        deadline = datetime.fromisoformat(state["deadline"])
    except Exception:
        deadline = _now()
    if _now() >= deadline:
        state["finished"] = True
        state["finished_at"] = _now_str()
        save_state(state)
        try:
            send_card(build_summary_card(state))
        except Exception as err:  # noqa: BLE001
            print(f"[providerask] summary card failed: {err!r}", flush=True)
            try:
                notify("📋 Provider maintenance sweep finished "
                       "(the summary card was rejected — see the service log).")
            except Exception:
                pass
        return {"done": True, "checked": checked, "filed": filed}

    return {"done": False, "checked": checked, "filed": filed,
            "waiting": len(waiting)}
