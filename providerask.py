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
     Remark = "No maintenance". Last Check is stamped either way. The row is
     decided over the WHOLE reply, never the newest bubble alone, and a "no
     maintenance" answer never blanks a window that has not ended yet.
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
import re
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


# How far back a sweep looks, once, when our question is not in the normal read.
# Sweeps only start after the whole paced send run (~12 minutes for the first
# group asked), so a busy support group can push the ask out of a 12-bubble
# read before anyone looks. Every later sweep then found no boundary and the
# summary reported "no update from the provider" while the answer sat on
# screen. 50 is the ceiling PROVIDERASK_READ_COUNT itself allows.
_DEEP_READ = 50


def _write_attempts() -> int:
    """How many sweeps may try one answer's Base write (1 = no retry).

    A failed write used to be recorded exactly like a successful one - the row
    was marked "filled", never looked at again, and the summary card went green
    over a sheet nobody had written. Retrying is bounded because a permanently
    broken row fails identically every time; three sweeps is ~30 minutes, long
    enough to ride out a rate limit or a token refresh.
    """
    return _int_env("PROVIDERASK_WRITE_ATTEMPTS", 3, 1, 10)


def _clear_enabled() -> bool:
    """May a "no maintenance" answer BLANK a row that still holds an old window?

    Blanking a bitable DateTime means sending it EMPTY, and if this tenant's
    field config rejects that the whole PUT is rejected - the same narrow risk
    VAWATCH_CLEAR_ENABLED guards in vawatch.act_on_clear, whose docstring says to
    watch one clear land before switching it on. This path used to send the
    nulls regardless of that flag. PROVIDERASK_CLEAR_ENABLED decides; unset, it
    follows VAWATCH_CLEAR_ENABLED, so one switch governs both paths. Off (the
    default), the answer is reported for a human instead of the nulls being
    sent. A row with NO window needs no nulls and is written either way.
    """
    raw = os.getenv("PROVIDERASK_CLEAR_ENABLED")
    if raw is None or not raw.strip():
        raw = os.getenv("VAWATCH_CLEAR_ENABLED") or "0"
    return raw.strip().lower() in {"1", "true", "yes", "on"}


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
    """{ask: [...], unpinned: [...], shared: [...], skipped: [...]} from the Base.

    Reuses groupcheck's partition so the exclusion list and the blank-row rule
    stay defined in exactly one place.

    ``shared`` holds the rows whose chat is ALREADY being asked for another
    provider (live: Hacksaw and YGG share "[SG190- IGO Casinoplus YG/ RG/ HS] CS
    group"). The group is still asked only once, but the row is no longer
    dropped: it used to land in ``skipped``, vanish from the run state and the
    summary card, and keep last week's window with nobody told.
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
    ask, unpinned, shared = [], [], []
    first_by_group: dict = {}
    for row in telegram:
        # Two Base rows can name the SAME chat (the sheet already has two YGG
        # rows). Asking one partner group twice is both rude and pointless.
        gkey = _norm(row.get("group"))
        first = first_by_group.get(gkey)
        if first is not None:
            first_key = _row_key(first)
            if _norm(_row_key(row)) == _norm(first_key):
                # The same provider twice: one state entry covers both.
                row["why"] = "another row already covers this group"
                skipped.append(row)
            else:
                row["shared_with"] = first_key
                row["why"] = f"shares this group with {first_key} - asked once there"
                shared.append(row)
            continue
        first_by_group[gkey] = row
        peer = peerstore.peer_for(row.get("group") or "")
        if peer:
            ask.append({**row, "peer": peer})
        else:
            unpinned.append(row)
    return {"ask": ask, "unpinned": unpinned, "shared": shared,
            "skipped": skipped, "rows": len(rows)}


def _row_key(row: dict) -> str:
    """The run-state key for a Base row: its provider, else its group."""
    return row.get("provider") or row.get("group") or ""


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


def _cell_ms(value) -> Optional[int]:
    """A bitable DateTime cell -> epoch ms; None when blank.

    Raises ValueError for anything else, so a cell this code does not
    understand is never mistaken for "blank" (which would green-light a clear).
    """
    if value is None or value == "" or value == []:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return int(value)
        except (ValueError, OverflowError):     # NaN / inf
            pass
    elif isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    raise ValueError(f"unreadable DateTime cell {value!r}")


def _ms_str(ms: Optional[int]) -> str:
    if ms is None:
        return "?"
    return datetime.fromtimestamp(ms / 1000, _tz()).strftime("%Y-%m-%d %H:%M")


def _clear_plan(fields: dict) -> tuple:
    """May a "no maintenance" answer be written onto this row? -> (refusal, blank)

    ``refusal`` non-empty means write NOTHING and hand the row to a human;
    ``blank`` means Start/End must be sent empty.

    The clear used to blank whatever the row held. Our question asks about THIS
    week, so "no maintenance this week" erased a correctly announced window for
    next week that vawatch had written days earlier - the row read "No
    maintenance" through the outage, and because that notice was already final
    in vawatch's ledger nothing ever put it back. The same happens when a CS
    agent's "nothing planned" contradicts a notice for this week. Which of the
    two is right is not something the text can settle, so a window that has not
    ENDED yet is never blanked on this path. A window that is over is last
    week's leftover and blanking it is the whole point - behind
    _clear_enabled(), because it needs null DateTimes.
    """
    try:
        start = _cell_ms(fields.get("Start Time"))
        end = _cell_ms(fields.get("End Time"))
    except ValueError as err:
        return (f"says there is no maintenance, but the row's current window "
                f"could not be read ({err}) - left alone", False)
    if start is None and end is None:
        return "", False
    shown = f"{_ms_str(start)} → {_ms_str(end)}"
    last = end if end is not None else start
    if last > int(_now().timestamp() * 1000):
        return (f"says there is no maintenance, but the row still holds an "
                f"upcoming window ({shown}) from an earlier notice - left in "
                f"place; check which one is right", False)
    if not _clear_enabled():
        return (f"says there is no maintenance; blanking the row's old window "
                f"({shown}) needs PROVIDERASK_CLEAR_ENABLED=1 (or "
                f"VAWATCH_CLEAR_ENABLED=1) - nothing written", False)
    return "", True


def file_answer(row: dict, verdict: dict, text: str) -> dict:
    """Write one provider's answer. -> {wrote, error, refused}

    ``refused`` is set, and NOTHING is written, when a 'no maintenance' answer
    would erase a window it cannot vouch for - see _clear_plan.
    """
    import vawatch as va

    out = {"wrote": "", "error": "", "refused": ""}
    try:
        found = va.find_provider_row(row.get("provider") or "")
        if not found:
            raise RuntimeError(f"no Base row for {row.get('provider')!r}")
        fields: dict[str, Any] = {"Last Check": va._ms(_now())}
        if verdict["action"] == "fill":
            fields["Start Time"] = va._ms(verdict["start"])
            fields["End Time"] = va._ms(verdict["end"])
            fields["Remark"] = text
            pending = (f"{verdict['start']:%Y-%m-%d %H:%M} → "
                       f"{verdict['end']:%H:%M}"
                       + (" (rescheduled)" if verdict.get("reschedule") else ""))
        else:                       # 'clear'
            refusal, blank = _clear_plan(found.get("fields") or {})
            if refusal:
                out["refused"] = refusal
                return out
            fields["Remark"] = "No maintenance"
            if blank:
                # Blank the window too. The Base's 'Maintenance' column is
                # IF(ISBLANK(Start Time), "No", "Yes"), so leaving last week's
                # dates in place leaves the sheet reading "Yes" next to a Remark
                # that says there is none - and the row contradicts itself.
                fields["Start Time"] = None
                fields["End Time"] = None
                pending = "No maintenance (old window cleared)"
            else:
                # Nothing to blank, so no null DateTime is sent at all.
                pending = "No maintenance"
        va.update_row(found["record_id"], fields)
        # Only AFTER the write returns. Setting it beforehand meant a failed
        # write still produced a non-empty `wrote`, which do_sweep reads as
        # success: the row was marked "filled" and the summary card went green
        # claiming the sheet had been updated.
        out["wrote"] = pending
    except Exception as err:        # noqa: BLE001
        out["error"] = repr(err)
        print(f"[providerask] filing {row.get('provider')!r} failed: {err!r}",
              flush=True)
    return out


# A "no maintenance" bubble that is plainly ADDITIVE - it rules out anything
# beyond what the provider already said ("No other maintenance this week",
# "Apart from that, no maintenance"). Only such a bubble may follow a window in
# the same reply without cancelling it; see _decide. A retraction word anywhere
# in the bubble makes it non-additive, because "Sorry, ignore that - no more
# maintenance" means the opposite.
_ADDITIVE_RE = re.compile(
    r"\b(?:no|not\s+any|nothing|none)\s+(?:other|further|additional|more|else)\b"
    r"|\b(?:apart|aside)\s+from\s+(?:that|this|it|the\s+above)\b"
    r"|\b(?:besides|other\s+than|except(?:\s+for)?)\s+(?:that|this|it|the\s+above)\b",
    re.I)
_RETRACT_RE = re.compile(
    r"\b(?:cancel\w*|call(?:ed|ing)?\s+off|postpon\w*|withdr[ae]w\w*|disregard\w*|"
    r"ignore|correction|mistake\w*|wrong|sorry|retract\w*|no\s+longer)\b", re.I)


def _additive(text: str) -> bool:
    t = " ".join(str(text or "").split())
    return bool(_ADDITIVE_RE.search(t)) and not _RETRACT_RE.search(t)


def judge(text: str, *, group: str, asked_at: str, sender: str = "",
          quoted: str = "", llm_down: str = "") -> dict:
    """LLM decides relevance, noticeparse decides the times.

    Returns the noticeparse verdict, with 'action' downgraded to 'ignore' when
    the model says the message is not addressed to us. An unreachable or
    undecided model yields 'needs_human' rather than a write: the sheet is
    shared and a wrong entry is worse than a gap.

    Extra keys, for do_sweep's whole-reply decision (see _decide):
      kind      fill | clear | human | soft | note | ignore. 'human' is a
                definite "a person must look"; 'soft' is an undecided bubble
                (model down, low confidence, "unclear", maintenance with no
                window) that must not END collection - a holding reply like
                "Let me check with our tech team" used to stop the provider
                being read for the rest of the hour, so the real answer that
                followed was never filed. 'note' is a past window (a quoted old
                notice): reported, never decisive.
      retry     the model was not asked or not understood; re-judge next sweep.
      mx        this bubble may announce a maintenance, so a later "no
                maintenance" bubble cannot simply cancel it.
      additive  (clear only) see _additive.
      model_down  the model was unreachable - do_sweep stops asking it for the
                rest of the sweep, so a 600s timeout is paid once, not per bubble.
    ``llm_down`` (a reason) skips the model call and returns a retry verdict.
    """
    import noticeparse as np
    import providerllm as pl

    # providerask's clock, not noticeparse's own: the two agree in production,
    # and this is what lets a test pin "now" without patching noticeparse.
    now = _now()
    parsed = np.classify(text, now=now)
    extra = {"kind": "soft", "retry": False, "mx": False, "additive": False,
             "model_down": False}
    if parsed["action"] == "needs_human":
        return {**parsed, **extra, "kind": "human", "mx": True,
                "llm": {"ok": False, "why": "not consulted"}}
    has_window = parsed["action"] == "fill" and not parsed.get("stale")
    if llm_down:
        llm = {"ok": False, "why": f"not asked - {llm_down}"}
    else:
        llm = pl.classify_reply(text, group=group, asked_at=asked_at,
                                sender=sender, quoted_text=quoted)
    out = {**parsed, **extra, "llm": llm}

    if not llm.get("ok"):
        out["action"] = "needs_human"
        out["reason"] = f"LLM unavailable ({llm.get('why')})"
        out.update(kind="soft", retry=True, mx=True,
                   model_down=str(llm.get("why") or "").startswith(
                       ("model unreachable", "not asked")))
        return out
    if llm["verdict"] == "unrelated" or not llm.get("answers_us"):
        out["action"] = "ignore"
        out["reason"] = f"not addressed to us ({llm.get('why')})"
        out["kind"] = "ignore"
        return out
    if llm["confidence"] < pl.min_confidence():
        out["action"] = "needs_human"
        out["reason"] = (f"low confidence {llm['confidence']:.2f} "
                         f"({llm.get('why')})")
        out.update(kind="soft",
                   mx=has_window or llm["verdict"] == "maintenance")
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
        if np.find_window(text, now=now) is not None:
            out["action"] = "needs_human"
            out["reason"] = ("says there is no maintenance, but the message also "
                             "carries a maintenance window - refusing to guess")
            out.update(kind="human", mx=True)
            return out
        out["action"] = "clear"
        out["reason"] = "provider says no maintenance"
        out.update(kind="clear", additive=_additive(text))
        return out

    if llm["verdict"] == "maintenance":
        if parsed["action"] == "fill" and parsed.get("stale"):
            out["action"] = "needs_human"
            out["reason"] = ("announces a window that has already passed - "
                             "probably quoting an old notice")
            out["kind"] = "note"
        elif parsed["action"] != "fill":
            # The model sees maintenance but no window could be parsed - this is
            # the reply-quote case, where the times live in the quoted message.
            # Soft: "we will send the schedule shortly" is followed by the
            # schedule, and that has to be read.
            out["action"] = "needs_human"
            out["reason"] = ("says maintenance but no window in this message "
                             "(likely quoting another message)")
            out.update(kind="soft", mx=True)
        else:
            out.update(kind="fill", mx=True)
        return out

    out["action"] = "needs_human"
    out["reason"] = f"model unsure ({llm.get('why')})"
    out.update(kind="soft", mx=has_window)
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
    errored = [(k, v) for k, v in provs.items() if v.get("outcome") == "error"]
    unwritten = [(k, v) for k, v in provs.items()
                 if v.get("outcome") == "write_failed"]
    silent = [(k, v) for k, v in provs.items()
              if v.get("outcome") in (None, "", "waiting") and v.get("asked")]
    unsent = [(k, v) for k, v in provs.items() if not v.get("asked")]

    # A shared chat is asked once but serves two rows, so "groups" and "rows"
    # differ; saying "asked 18 groups" when 17 were messaged would be false.
    rows_asked = sum(1 for v in provs.values() if v.get("asked"))
    groups_asked = sum(1 for v in provs.values()
                       if v.get("asked") and not v.get("shared_with"))
    asked_line = (f"**Asked {groups_asked} provider group(s) at "
                  f"{state.get('asked_at', '?')}**" if rows_asked == groups_asked
                  else f"**Asked {groups_asked} provider group(s) "
                       f"({rows_asked} rows) at {state.get('asked_at', '?')}**")
    el = [_div(
        f"{asked_line}\n"
        f"Collected for {state.get('window_min', 60)} minutes.\n"
        f"✅ filled: **{len(filled)}**   🟢 no maintenance: **{len(cleared)}**   "
        f"❓ needs a human: **{len(human)}**   ⚠️ unreadable: **{len(errored)}**"
        + (f"   ⚠️ not written: **{len(unwritten)}**" if unwritten else "")
        + f"   ⌛ no reply: **{len(silent)}**")]

    def name(k, v):
        # Label a shared row so the operator can see why its answer is the
        # other row's answer too (and where to look).
        shared = v.get("shared_with")
        return f"**{k}**" + (f" _(asked in {shared}'s group)_" if shared else "")

    def block(head, rows, fmt):
        if not rows:
            return
        el.append({"tag": "hr"})
        el.append(_div(_clamp(head + "\n" + "\n".join(fmt(k, v) for k, v in rows))))

    def unsent_why(v):
        if v.get("shared_with") and not v.get("peer"):
            prim = provs.get(v["shared_with"]) or {}
            if not prim.get("asked"):
                return f"shares {v['shared_with']}'s group, which was not asked"
        return v.get("why") or "not sent"

    block("**✅ Maintenance filled into the sheet:**", filled,
          lambda k, v: f"• {name(k, v)} — {v.get('wrote') or ''}")
    block("**🟢 Answered: no maintenance this week:**", cleared,
          lambda k, v: f"• {name(k, v)}")
    block("**❓ Answered, but needs a human:**", human,
          lambda k, v: f"• {name(k, v)} — _{v.get('why') or ''}_")
    block("**⚠️ Answered, but the sheet could NOT be written - OUR problem:**",
          unwritten, lambda k, v: f"• {name(k, v)} — _{v.get('why') or ''}_")
    block("**⚠️ Could not be read - OUR problem, not theirs:**", errored,
          lambda k, v: f"• {name(k, v)} — _{v.get('why') or ''}_")
    block("**⌛ No update from the provider:**", silent,
          lambda k, v: f"• {name(k, v)}")
    block("**⚠️ Not asked:**", unsent,
          lambda k, v: f"• {name(k, v)} — _{unsent_why(v)}_")

    if state.get("aborted"):
        el.append({"tag": "hr"})
        el.append(_div(f"**⚠️ The send run stopped early:** "
                       f"_{state.get('aborted')}_"))

    el.append({"tag": "hr"})
    el.append(_div(f"_{_now_str()}_"))
    # Green only when every answer landed. It used to ignore unreadable rows,
    # and a failed write was counted as filled, so a run whose writes all
    # failed still posted a green card.
    problems = human or errored or unwritten
    template = "green" if (filled or cleared) and not problems else (
        "orange" if (filled or cleared or human or unwritten) else "red")
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
    for row in plan.get("shared") or []:
        # Tracked, never sent to: peer stays empty so ask_one/next_to_ask skip
        # it, and _mirror_shared marks it asked the moment its group's ask
        # confirms. do_sweep then reads the chat once for both rows.
        key = _row_key(row)
        if not key or key in state["providers"]:
            continue
        state["providers"][key] = {
            "group": row.get("group"), "peer": "", "asked": False,
            "outcome": "", "wrote": "", "seen": [],
            "shared_with": row.get("shared_with") or "",
            "why": row.get("why") or "",
        }
    save_state(state)
    return {"state": state, "plan": plan}


def _mirror_shared(st: dict, prov: str) -> None:
    """Mark every row that shares ``prov``'s chat as asked along with it."""
    rec = (st.get("providers") or {}).get(prov) or {}
    for other in (st.get("providers") or {}).values():
        if other.get("shared_with") == prov and not other.get("asked"):
            other["asked"] = True
            other["asked_at"] = rec.get("asked_at") or _now_str()
            other["outcome"] = "waiting"
            other["why"] = ""
            if rec.get("ask_mid"):
                other["ask_mid"] = rec["ask_mid"]


def _record_ask(st: dict, prov: str, rec: dict, res: dict) -> None:
    """Bookkeeping for one confirmed ask (shared by ask_one and do_ask)."""
    rec["asked"] = True
    rec["asked_at"] = _now_str()
    rec["outcome"] = "waiting"
    # If the send route ever reports the new message's id, keep it: the ask is
    # then locatable even when a busy group scrolls it out before the first
    # sweep (the send route does not report one today, so this is a no-op).
    mid = str((res or {}).get("mid") or "").strip()
    if mid:
        rec["ask_mid"] = mid
    _mirror_shared(st, prov)


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
        _record_ask(st, prov, rec, res)
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
            _record_ask(state, prov, rec, res)
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


def _after_our_ask(messages: list, rec: Optional[dict] = None) -> tuple:
    """-> (messages after our question, boundary_found)

    Without a boundary, a notice the provider posted BEFORE we asked is read as
    their answer. Our own ask is in the transcript as an outgoing bubble, so its
    position is the natural boundary.

    But only ~12 bubbles are read per sweep, and a busy support group can push
    the ask out of that window inside one 10-minute interval. Re-deriving the
    boundary from the text every sweep then returned NOTHING, permanently, for
    the rest of the hour - while the provider's answer sat right there - and the
    summary blamed the provider for not replying. So the ask's message id is
    remembered the first time it is seen: once the ask is older than everything
    on screen, everything on screen is by definition after it.

    boundary_found is False only when we have never located the ask at all; the
    caller must report that rather than silently treating the group as silent.
    """
    msgs = list(messages or [])
    want = " ".join(ASK_TEXT.split()).casefold()
    cut = -1
    for i, m in enumerate(msgs):
        if not m.get("out"):
            continue
        body = " ".join(str(m.get("text") or "").split()).casefold()
        if want and want in body:
            cut = i
    if cut >= 0:
        mid = str(msgs[cut].get("mid") or "")
        if rec is not None and mid:
            rec["ask_mid"] = mid
        return msgs[cut + 1:], True

    remembered = str((rec or {}).get("ask_mid") or "")
    if remembered:
        for i, m in enumerate(msgs):
            if str(m.get("mid") or "") == remembered:
                return msgs[i + 1:], True
        # The ask is older than the oldest bubble we can see, so everything
        # read is newer than it.
        return msgs, True
    return [], False


def _message_key(msg: dict) -> str:
    """The seen-list key for one bubble ("" for an empty one)."""
    import hashlib

    body = " ".join(str(msg.get("text") or "").split())
    if not body:
        return ""
    mid = str(msg.get("mid") or "").strip()
    return f"mid:{mid}" if mid else "sha:" + hashlib.sha1(
        body.encode("utf-8")).hexdigest()[:16]


def _message_is_new(rec: dict, msg: dict) -> bool:
    """Only messages we have not already judged. Content-hashed, because the
    Telegram read gives no reliable ordering cursor."""
    key = _message_key(msg)
    if not key:
        return False
    if key in (rec.get("seen") or []):
        return False
    rec.setdefault("seen", []).append(key)
    return True


def _unsee(rec: dict, key: str) -> None:
    """Let the next sweep judge this bubble again (the model was not reached)."""
    rec["seen"] = [k for k in (rec.get("seen") or []) if k != key]


# ---------------------------------------------------------------------------
# The whole-reply decision
# ---------------------------------------------------------------------------
# Every bubble after our ask is judged once and kept, in order, in
# rec["replies"]. The row's outcome is then decided over ALL of them, not over
# the newest one alone: a provider answering "Hi team, yes - maintenance on
# 24/09 10:00-12:00" and then "No other maintenance this week." had the second
# bubble judged alone, which blanked tomorrow's announced window and reported
# "no maintenance".

def _entry(key: str, verdict: dict, text: str) -> Optional[dict]:
    """One judged bubble, as stored in rec["replies"]; None when irrelevant."""
    kind = verdict.get("kind") or "soft"
    if kind == "ignore":
        return None
    e = {"k": key, "kind": kind, "retry": bool(verdict.get("retry")),
         "mx": bool(verdict.get("mx")), "add": bool(verdict.get("additive")),
         "why": str(verdict.get("reason") or "")[:240],
         # Fill and clear keep the whole text: it is the Remark, and a write
         # that failed is retried from here on a later sweep.
         "text": text if kind in ("fill", "clear") else text[:300]}
    if kind == "fill":
        e.update(start=verdict["start"].isoformat(),
                 end=verdict["end"].isoformat(),
                 rs=bool(verdict.get("reschedule")))
    return e


def _put_entry(rec: dict, e: dict) -> None:
    """Append, or replace in place a bubble that is being re-judged."""
    replies = rec.setdefault("replies", [])
    for i, old in enumerate(replies):
        if old.get("k") == e["k"]:
            replies[i] = e
            return
    replies.append(e)


def _entry_window(e: dict) -> str:
    try:
        s = datetime.fromisoformat(e["start"])
        f = datetime.fromisoformat(e["end"])
    except Exception:
        return "?"
    return f"{s:%Y-%m-%d %H:%M} → {f:%H:%M}"


def _pick_fill(seq: list) -> dict:
    """The newest window in ``seq`` - unless the reply carries two different
    windows and the newer one does not say it replaces the older. Then it is a
    correction or a second maintenance, the row holds only one, and the text
    cannot say which."""
    fills = [e for e in seq if e.get("kind") == "fill"]
    top = fills[-1]
    other = [e for e in fills[:-1]
             if (e.get("start"), e.get("end")) != (top.get("start"), top.get("end"))]
    if other and not top.get("rs"):
        return {"action": "needs_human", "entry": None,
                "why": (f"two different windows in one reply "
                        f"({_entry_window(other[-1])}; {_entry_window(top)}) - a "
                        f"correction or a second maintenance? refusing to guess")}
    return {"action": "fill", "entry": top, "why": ""}


def _decide(replies: list) -> dict:
    """-> {action: wait | fill | clear | needs_human, entry, why}

    Deterministic, newest-decisive:
      * a bubble the model could not judge (retry) holds everything: wait;
      * the newest relevant bubble is a window -> file it (see _pick_fill);
        older "no maintenance" or unreadable bubbles do not override it;
      * the newest is undecided (soft) -> wait; it may be a correction of an
        earlier window, so that window is NOT filed on its strength;
      * the newest is a definite needs-a-human -> needs_human;
      * the newest says "no maintenance" -> clear, but only when nothing
        earlier in the reply announced a maintenance. After one, only an
        ADDITIVE bubble ("no other maintenance") may stand, and then the
        window is what gets filed. Anything else is a retraction-or-"no
        other" question the text cannot settle: needs_human.
    'note' bubbles (a quoted past window) are never decisive; at the deadline
    they only explain why nothing was filed.
    """
    live = [e for e in replies if e.get("kind") != "note"]
    pending = [e for e in live if e.get("retry")]
    if pending:
        return {"action": "wait", "entry": None, "why": pending[-1].get("why") or ""}
    if not live:
        notes = [e for e in replies if e.get("kind") == "note"]
        return {"action": "wait", "entry": None,
                "why": notes[-1].get("why") if notes else ""}
    newest = live[-1]
    kind = newest.get("kind")
    if kind == "fill":
        return _pick_fill(live)
    if kind == "human":
        return {"action": "needs_human", "entry": None, "why": newest.get("why") or ""}
    if kind == "clear":
        older = [(i, e) for i, e in enumerate(live[:-1]) if e.get("mx")]
        if not older:
            return {"action": "clear", "entry": newest, "why": ""}
        idx, last_mx = older[-1]
        if newest.get("add") and last_mx.get("kind") == "fill":
            return _pick_fill(live[:idx + 1])
        return {"action": "needs_human", "entry": None,
                "why": ("says there is no maintenance after an earlier bubble "
                        "that announced one ("
                        + (_entry_window(last_mx) if last_mx.get("kind") == "fill"
                           else (last_mx.get("why") or "")[:80])
                        + ") - a retraction, or 'no other'? refusing to guess")}
    # soft
    why = newest.get("why") or "undecided reply"
    fills = [e for e in live if e.get("kind") == "fill"]
    if fills:
        why += (f" - it came after a window ({_entry_window(fills[-1])}), "
                f"which was therefore not filed")
    return {"action": "wait", "entry": None, "why": why}


def _verdict_of(e: dict) -> dict:
    if e.get("kind") == "fill":
        return {"action": "fill", "start": datetime.fromisoformat(e["start"]),
                "end": datetime.fromisoformat(e["end"]),
                "reschedule": bool(e.get("rs"))}
    return {"action": "clear", "start": None, "end": None, "reschedule": False}


def _conclude(prov: str, rec: dict, send_card) -> int:
    """Decide this row from its replies so far and act. -> 1 if a row was written.

    Collection ends only on a real outcome: a write that LANDED, or a definite
    needs-a-human. A failed write stays waiting and is retried next sweep (at
    most _write_attempts() times), instead of being recorded as "filled".
    """
    d = _decide(rec.get("replies") or [])
    if d["action"] == "wait":
        rec["pending"] = d["why"]
        rec.pop("write_error", None)
        return 0
    rec["pending"] = ""
    if d["action"] == "needs_human":
        rec.pop("write_error", None)
        rec["outcome"] = "needs_human"
        rec["why"] = d["why"]
        return 0
    e = d["entry"]
    verdict = _verdict_of(e)
    if verdict["action"] == "fill" and verdict["end"] <= _now():
        # A window judged on an earlier sweep (a write being retried) can have
        # ended since; writing it now would put a dead window on the row.
        rec.pop("write_error", None)
        rec["outcome"] = "needs_human"
        rec["why"] = (f"the announced window ({_entry_window(e)}) ended before it "
                      f"could be written")
        return 0
    text = e.get("text") or ""
    done = file_answer({"provider": prov, "group": rec["group"]}, verdict, text)
    if done.get("refused"):
        rec.pop("write_error", None)
        rec["outcome"] = "needs_human"
        rec["why"] = done["refused"]
        return 0
    if done.get("wrote"):
        rec.pop("write_error", None)
        rec["outcome"] = "filled" if verdict["action"] == "fill" else "no_maintenance"
        rec["wrote"] = done["wrote"]
        rec["why"] = ""
        try:
            send_card(build_hit_card(prov, rec["group"], verdict, text, done["wrote"]))
        except Exception as err:  # noqa: BLE001
            print(f"[providerask] hit card failed: {err!r}", flush=True)
        return 1
    tries = int(rec.get("write_attempts") or 0) + 1
    rec["write_attempts"] = tries
    rec["write_error"] = (f"the Base write failed ({tries} of {_write_attempts()} "
                          f"attempts): {done.get('error') or 'not confirmed'}")[:300]
    if tries >= _write_attempts():
        rec["outcome"] = "write_failed"
        rec["why"] = rec["write_error"]
        try:
            send_card(build_hit_card(prov, rec["group"], verdict, text, ""))
        except Exception as err:  # noqa: BLE001
            print(f"[providerask] hit card failed: {err!r}", flush=True)
    return 0


def _close_out(rec: dict) -> None:
    """At the deadline, turn a still-waiting row into what actually happened.

    A read failure or a lost boundary is OUR problem and says so; an undecided
    reply is a human's; only a row we read cleanly and heard nothing from is
    reported as the provider being silent. The boundary case used to land in
    "⌛ No update from the provider" - blaming a provider whose answer was on
    screen for our own failure to find the question.
    """
    if rec.get("write_error"):
        rec["outcome"] = "write_failed"
        rec["why"] = rec["write_error"]
        return
    d = _decide(rec.get("replies") or [])
    problem = rec.get("read_problem") or ""
    if d["action"] == "wait" and d["why"]:
        rec["outcome"] = "needs_human"
        rec["why"] = d["why"] + (f"; the last read also failed: {problem}"
                                 if problem else "")
        return
    if problem:
        rec["outcome"] = "error"
        rec["why"] = problem


def _read(read_one, title: str, count: int) -> dict:
    try:
        res = read_one(title, count) or {}
    except Exception as err:  # noqa: BLE001
        print(f"[providerask] read {title!r} failed: {err!r}", flush=True)
        return {"ok": False, "messages": [], "error": repr(err)[:200]}
    if not res.get("ok"):
        return {"ok": False, "messages": [],
                "error": str(res.get("error") or "could not read the group")[:200]}
    return res


def _shared_route(verdict: dict, text: str, sharers: list) -> dict:
    """In a chat that serves two Base rows, decide whose answer a bubble is.

    A bubble that names none of the sharing providers is filed onto all of
    them - the same thing vawatch does with a notice in that chat. A bubble
    that names any of them cannot be attributed from its text: "YGG scheduled
    maintenance ... Hacksaw games are not affected" names both and belongs to
    one, and an alias ("Yggdrasil", "HS") defeats any name match. That goes to
    a human, never onto the row it did not mean.
    """
    if len(sharers) < 2 or verdict.get("kind") not in ("fill", "clear"):
        return verdict
    body = _norm(text)
    named = [p for p in sharers
             if p and re.search(r"(?<![0-9a-z])" + re.escape(_norm(p)) + r"(?![0-9a-z])",
                                body)]
    if not named:
        return verdict
    return {**verdict, "action": "needs_human", "kind": "human", "mx": True,
            "reason": (f"this chat serves {', '.join(sharers)} and the answer "
                       f"names {', '.join(named)} - which row it is for cannot be "
                       f"told from the text, so nothing was filed")}


def do_sweep(read_one, notify, send_card) -> dict:
    """One collection pass. Files what it can, cards each hit, and at the
    deadline posts the summary and closes the run.

    ``read_one(title, count) -> {ok, messages: [...], error}``

    Each chat is read ONCE per sweep, even when it serves two Base rows, and
    every bubble after our ask is judged once; the row is then decided over the
    whole reply (see _decide). A read failure, a lost boundary, a model that
    could not be reached and an undecided bubble all leave the row waiting for
    the next sweep - they used to end collection on the first sweep, so the
    real answer that arrived ten minutes later was never read. What is still
    unresolved at the deadline is reported as what it is (_close_out).
    """
    state = load_state()
    if not state or state.get("finished"):
        return {"done": True, "reason": "no active run"}

    provs = state.get("providers") or {}
    asked_at = state.get("asked_at") or state.get("started_at") or ""
    waiting = [(p, v) for p, v in provs.items()
               if v.get("asked") and v.get("outcome") in ("", "waiting")]
    checked = filed = 0
    # Once the model is unreachable, stop asking it for the rest of this sweep:
    # with PROVIDERASK_TIMEOUT=600 each further call would hold the Telegram
    # worker for another ten minutes. The bubbles stay unjudged and are retried.
    llm_down = {"why": ""}

    by_group: dict = {}
    for prov, rec in waiting:
        by_group.setdefault(_norm(rec.get("group")), []).append((prov, rec))

    for gkey, members in by_group.items():
        title = members[0][1]["group"]
        res = _read(read_one, title, _read_count())
        checked += 1
        if not res.get("ok"):
            # "could not identify this group" is NOT "the provider did not
            # reply". Blaming the partner for our own lookup failure is exactly
            # the silent-wrong-answer this command must not produce - and one
            # header timeout is not a reason to stop reading for the hour.
            for _prov, rec in members:
                rec["read_errors"] = int(rec.get("read_errors") or 0) + 1
                rec["read_problem"] = res.get("error") or "could not read the group"
            save_state(state)
            continue
        msgs = res.get("messages") or []
        if (_read_count() < _DEEP_READ
                and any(not _after_our_ask(msgs, rec)[1] for _p, rec in members)):
            deep = _read(read_one, title, _DEEP_READ)
            if deep.get("ok") and len(deep.get("messages") or []) > len(msgs):
                msgs = deep["messages"]
        sharers = [p for p, r in provs.items()
                   if _norm(r.get("group")) == gkey and _norm(p) != gkey]
        judged: dict = {}

        for prov, rec in members:
            after, boundary = _after_our_ask(msgs, rec)
            if not boundary:
                # Never guess. Say so, and let the summary show it as OUR
                # problem rather than as "the provider did not reply".
                rec["read_problem"] = (
                    f"could not find our question in the last {len(msgs)} "
                    f"messages - the group may be too busy; raise "
                    f"PROVIDERASK_READ_COUNT")
                continue
            rec["read_problem"] = ""
            for msg in after:             # oldest first: replies keep their order
                if msg.get("out"):        # our own message
                    continue
                if not _message_is_new(rec, msg):
                    continue
                key = _message_key(msg)
                text = str(msg.get("text") or "")
                verdict = judged.get(key)
                if verdict is None:
                    try:
                        verdict = judge(text, group=rec["group"], asked_at=asked_at,
                                        sender=str(msg.get("sender") or ""),
                                        llm_down=llm_down["why"])
                    except Exception as err:  # noqa: BLE001
                        # One unparseable message must not abort the sweep: the
                        # deadline block at the end is what posts the summary
                        # and CLOSES the run. It is kept as an undecided bubble,
                        # so it cannot be skipped over in favour of an older one.
                        print(f"[providerask] judging a message for {prov} "
                              f"failed: {err!r}", flush=True)
                        verdict = {"action": "needs_human", "kind": "soft",
                                   "mx": True, "retry": False,
                                   "reason": f"could not judge this message ({err!r})"[:200]}
                    if verdict.get("model_down") and not llm_down["why"]:
                        llm_down["why"] = str((verdict.get("llm") or {}).get("why")
                                              or "model unreachable")[:120]
                    judged[key] = verdict
                routed = _shared_route(verdict, text, sharers)
                if routed.get("retry"):
                    _unsee(rec, key)
                e = _entry(key, routed, text)
                if e is not None:
                    _put_entry(rec, e)
                else:
                    # Re-judged as irrelevant: its old "could not judge" entry
                    # must go, or it would hold the row waiting forever.
                    rec["replies"] = [x for x in (rec.get("replies") or [])
                                      if x.get("k") != key]
            try:
                filed += _conclude(prov, rec, send_card)
            except Exception as err:  # noqa: BLE001
                # Same reason as the judge guard above: an escape here would
                # skip the deadline block and leave the run open forever. The
                # row stays waiting and is decided again next sweep.
                print(f"[providerask] deciding {prov} failed: {err!r}", flush=True)
        save_state(state)

    try:
        deadline = datetime.fromisoformat(state["deadline"])
    except Exception:
        deadline = _now()
    if _now() >= deadline:
        for rec in provs.values():
            if rec.get("asked") and rec.get("outcome") in ("", "waiting"):
                try:
                    _close_out(rec)
                except Exception as err:  # noqa: BLE001
                    print(f"[providerask] closing out a row failed: {err!r}",
                          flush=True)
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
