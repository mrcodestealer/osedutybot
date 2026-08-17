#!/usr/bin/env python3
"""Detect EVO maintenance notices posted in a Microsoft Teams group.

The Teams side (session, capture) lives in :mod:`teamswatch`. This module owns
everything after a message has been read: deciding whether it is an EVO
maintenance notice, posting the confirm card, and — on **Generate Email** —
running the exact same pipeline as ``/m``.

Flow
----
Teams message -> handle_teams_message()
  -> ledger: remember it as the group's latest (survives a restart)
  -> classify: is this an EVO maintenance notice, or ordinary chatter?
  -> card to EVOTEAMS_CARD_CHAT_ID: "Detected new EVO maintenance message"
     with [Generate Email] [Cancel], and the raw content posted as a THREADED
     reply under that card
  -> Generate Email -> main._process_evo_sd_batch_paste() == what typing `/m`
     does, then the ledger records ``emailed``
  -> Cancel        -> the ledger records ``cancelled``, nothing is sent

Why the classifier is a format gate and not an LLM
--------------------------------------------------
``/m`` only accepts Evolution Service-Desk **batch** pastes — the ``※SD-xxxxx※``
blocks — and rejects anything else with 未识别为 EVO 批量维护格式
(main.py:6619). So ``maintenance.is_evo_sd_batch_paste()`` is simultaneously the
right detector AND the precondition for **Generate Email** to be able to succeed:
classifying something looser as "maintenance" would only produce a card whose
button is guaranteed to fail. main.py:5857 relies on the same gate for Lark
pastes, noting the ※SD※ format is distinctive enough that ordinary chatter is
never mistaken for it.

Softer signals (game-name extraction, the "following tables will be unavailable"
phrasing) are still reported, but marked as *not* ``/m``-ready so nobody expects
the button to work. An optional LLM confirmation exists for the soft cases and is
**off by default**: prod is CPU-only serving qwen2.5:0.5b, and a 0.5b model
guessing at maintenance notices would add false positives to a gate that is
already precise.

Exposed to main.py:
    handle_teams_message(...)       -> called by teamswatch on each new message
    handle_card_callback(...)       -> the [Generate Email] / [Cancel] buttons
    status_lines()                  -> summary for /teamstatus
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - dotenv is a declared dep
    load_dotenv = None  # type: ignore[assignment]

_ROOT_DIR = Path(__file__).resolve().parent
if load_dotenv is not None:
    load_dotenv(str(_ROOT_DIR / ".env"))

# The ledger. Named after this module so the pairing is obvious on disk.
_STATE_PATH = _ROOT_DIR / "detectevomaintenance.json"
_STATE_LOCK = threading.Lock()
# Keep the tail only; an ops group runs for years and the file must stay small.
_MAX_HANDLED = 400

# Where the confirm card goes. Defaults to the group that already owns /m
# (maintenance.EVO_BATCH_COMMAND_CHAT_ID's default), because Generate Email runs
# /m as that chat and _process_evo_sd_batch_paste refuses any other group.
CARD_CHAT_ID = os.getenv(
    "EVOTEAMS_CARD_CHAT_ID", "oc_51b6fbf2636525acfb4ead3afa3c93ce"
).strip()

# Teams group to watch. Matched case-insensitively as a substring of the
# conversation title, so the sidebar's truncated name still matches.
WATCH_GROUP = os.getenv(
    "EVOTEAMS_GROUP", "@EVO C88live/slot_ow.ph (RTS) CS Group NE RT FP"
).strip()


def _truthy(val: str | None) -> bool:
    return str(val or "").strip().lower() in ("1", "true", "yes", "on", "y")


def _enabled() -> bool:
    """Opt-in. Unset means OFF, so deploying this file alone changes nothing."""
    return _truthy(os.getenv("EVOTEAMS_ENABLED"))


def _llm_enabled() -> bool:
    """Off by default — see the module docstring."""
    return _truthy(os.getenv("EVOTEAMS_LLM"))


def _tz():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(os.getenv("EVOTEAMS_TZ", "Asia/Manila"))
    except Exception:
        return timezone.utc


def _now_str() -> str:
    return datetime.now(_tz()).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Ledger — what we have seen, and what was decided about it
# ---------------------------------------------------------------------------
# Two jobs in one file, deliberately:
#
#   last_seen  the newest message id/timestamp per group. Written the moment a
#              message is SEEN, which is what makes a restart safe: without it a
#              restart would re-read the group's backlog and re-post cards.
#   handled    per-message outcome: pending | emailed | cancelled. `pending`
#              also carries the full notice text, so a button tapped after a
#              service restart still has something to email.
#
# Recording only on a button tap (as first sketched) cannot work: a restart
# before anyone taps would duplicate the card, and a message nobody ever taps
# would re-fire forever.


def _blank() -> dict[str, Any]:
    return {"last_seen": {}, "handled": {}, "order": []}


def _load_state() -> dict[str, Any]:
    try:
        with open(_STATE_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return _blank()
    if not isinstance(data, dict):
        return _blank()
    out = _blank()
    if isinstance(data.get("last_seen"), dict):
        out["last_seen"] = data["last_seen"]
    if isinstance(data.get("handled"), dict):
        out["handled"] = data["handled"]
    if isinstance(data.get("order"), list):
        out["order"] = [str(x) for x in data["order"]]
    return out


def _save_state(state: dict[str, Any]) -> None:
    """Atomic write — a torn ledger would re-post every card on the next boot."""
    order = [str(x) for x in (state.get("order") or [])][-_MAX_HANDLED:]
    handled = {k: v for k, v in (state.get("handled") or {}).items() if k in set(order)}
    payload = {
        "last_seen": state.get("last_seen") or {},
        "handled": handled,
        "order": order,
        "saved_at": _now_str(),
    }
    tmp = str(_STATE_PATH) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    os.replace(tmp, _STATE_PATH)


def _group_key(group: str) -> str:
    return (group or "").strip().lower() or "(unknown)"


def get_last_seen(group: str) -> dict[str, Any]:
    with _STATE_LOCK:
        return dict((_load_state()["last_seen"].get(_group_key(group)) or {}))


def set_last_seen(group: str, message_id: str, ts: Any = None) -> None:
    with _STATE_LOCK:
        state = _load_state()
        state["last_seen"][_group_key(group)] = {
            "message_id": str(message_id or ""),
            "ts": ts,
            "at": _now_str(),
        }
        _save_state(state)


def record(key: str, **fields: Any) -> None:
    """Upsert a handled-message record, keeping insertion order for the cap."""
    with _STATE_LOCK:
        state = _load_state()
        entry = dict(state["handled"].get(key) or {})
        entry.update(fields)
        entry["at"] = _now_str()
        state["handled"][key] = entry
        order = [x for x in state["order"] if x != key]
        order.append(key)
        state["order"] = order
        _save_state(state)


def get_record(key: str) -> dict[str, Any]:
    with _STATE_LOCK:
        return dict(_load_state()["handled"].get(key) or {})


def already_handled(key: str) -> bool:
    """True once a card exists for this content — pending counts, so a restart
    does not post a second card for a notice still awaiting a tap."""
    return bool(get_record(key))


def content_key(text: str) -> str:
    """Stable id for a notice. Content-hashed rather than a counter so the same
    notice re-posted in Teams maps to the same record instead of a new card."""
    return hashlib.sha1((text or "").strip().encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------
# Cheap signal that a blob is worth looking at, drawn from patterns already in
# maintenance.py. Only used to decide whether to run the *soft* checks — the
# ※SD※ gate below does not need it.
_EVO_HINT = re.compile(
    r"(?is)※\s*SD-\d+"
    r"|\[Service Desk\]"
    r"|TINC-\d"
    r"|Dear Casino Team"
    r"|定期维护通知"
    r"|following tables?\s+(?:will be|was|were)\s+unavailable"
    r"|going to take place with a downtime"
    r"|Table availability"
    r"|受影响游戏"
    r"|mainten|维护"
)


def _worth_looking(body: str) -> bool:
    """Copied from newportwatch._worth_classifying: long text always passes; a
    short one needs a hint word AND a digit, because a real notice always
    carries a date, time or count."""
    if len(body) >= 40:
        return True
    return bool(_EVO_HINT.search(body)) and bool(re.search(r"\d", body))


def classify(text: str) -> dict[str, Any]:
    """Decide whether ``text`` is an EVO maintenance notice.

    Returns ``{"is_notice": bool, "m_ready": bool, "why": str, "kind": str}``.

    ``m_ready`` is the part that matters operationally: only a ※SD※ batch paste
    can actually be emailed by ``/m``, so a soft match produces a card that says
    so rather than a button that is certain to fail.
    """
    body = (text or "").strip()
    if not body:
        return {"is_notice": False, "m_ready": False, "why": "empty", "kind": ""}

    try:
        import maintenance
    except Exception as err:  # pragma: no cover - maintenance is core
        return {"is_notice": False, "m_ready": False,
                "why": f"maintenance import failed: {err!r}", "kind": ""}

    # --- decisive: the format /m accepts -----------------------------------
    try:
        if maintenance.is_evo_sd_batch_paste(body):
            return {"is_notice": True, "m_ready": True,
                    "why": "※SD-xxxxx※ EVO batch format", "kind": "sd_batch"}
    except Exception as err:  # noqa: BLE001
        print(f"[evoteams] is_evo_sd_batch_paste raised: {err!r}", flush=True)

    if not _worth_looking(body):
        return {"is_notice": False, "m_ready": False, "why": "no signal", "kind": ""}

    # --- soft signals: a notice, but not one /m can send --------------------
    soft: list[str] = []
    try:
        if maintenance._FOLLOWING_TABLES_UNAVAILABLE_RE.search(body):
            soft.append("'following tables will be unavailable'")
        if maintenance._DOWNTIME_SCHEDULE_TABLES_RE.search(body):
            soft.append("'downtime' schedule phrasing")
        games = maintenance.extract_candidate_game_names(body) or []
        if games:
            soft.append(f"{len(games)} plausible game name(s)")
        kind = maintenance.classify_checkemail_step_kind(body, email_subject="")
        if kind and kind != "other":
            soft.append(f"step kind '{kind}'")
    except Exception as err:  # noqa: BLE001
        print(f"[evoteams] soft signal check raised: {err!r}", flush=True)
        kind = "other"

    if not soft:
        return {"is_notice": False, "m_ready": False, "why": "no notice signals",
                "kind": ""}

    if _llm_enabled() and not _llm_confirms(body):
        return {"is_notice": False, "m_ready": False,
                "why": "soft signals but LLM said chatter", "kind": ""}

    return {"is_notice": True, "m_ready": False, "why": "; ".join(soft),
            "kind": kind if kind and kind != "other" else "soft"}


def _llm_confirms(body: str) -> bool:
    """Optional confirmation for soft matches, mirroring newportwatch's shape.

    Fails **open** (returns True) on any error: a dead Ollama or a swapped model
    must not silently mute detection — the same reasoning as
    newportwatch.is_new_activity's missing-key handling.
    """
    try:
        import newportwatch as _np

        result = _np.classify_message(body) or {}
        if not result:
            return True
        speech = str(result.get("speech_act") or "").strip().lower()
        return speech in ("", "announces_new")
    except Exception as err:  # noqa: BLE001
        print(f"[evoteams] LLM confirm unavailable ({err!r}) — allowing", flush=True)
        return True


# ---------------------------------------------------------------------------
# Card
# ---------------------------------------------------------------------------
_PREVIEW_CHARS = 900


def _preview(text: str) -> str:
    body = (text or "").strip()
    if len(body) <= _PREVIEW_CHARS:
        return body
    return body[:_PREVIEW_CHARS] + f"\n… (+{len(body) - _PREVIEW_CHARS} more chars)"


def build_card(key: str, *, group: str, sender: str, text: str,
               m_ready: bool, why: str) -> dict[str, Any]:
    """Schema-2.0 card with Generate Email / Cancel.

    The notice text is NOT carried in the button values — it can be thousands of
    characters. Buttons carry only ``key``; the text is read back from the
    ledger, which is also what lets a tap survive a service restart.
    """
    note = (
        "Tap **Generate Email** to send it exactly as `/m` would."
        if m_ready else
        "⚠️ This is **not** in `※SD-xxxxx※` batch format, so `/m` will refuse it. "
        "Review it by hand — **Generate Email** will report the rejection."
    )
    elements: list[dict[str, Any]] = [
        {"tag": "div", "text": {"tag": "lark_md", "content":
            f"**Group:** {group or '—'}\n"
            f"**From:** {sender or '—'}\n"
            f"**Matched:** {why}\n"
            f"**Seen:** {_now_str()}"}},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": _preview(text)}},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": note}},
        {
            "tag": "column_set",
            "columns": [
                {"tag": "column", "elements": [{
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "Generate Email"},
                    "type": "primary",
                    "behaviors": [{"type": "callback",
                                   "value": {"k": "evom_gen", "i": key}}],
                }]},
                {"tag": "column", "elements": [{
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "Cancel"},
                    "type": "default",
                    "behaviors": [{"type": "callback",
                                   "value": {"k": "evom_cancel", "i": key}}],
                }]},
            ],
        },
    ]
    return {
        "schema": "2.0",
        # update_multi is required or an in-place update may not reach every
        # viewer (repo convention, 50+ occurrences).
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "orange" if m_ready else "yellow",
            "title": {"tag": "plain_text",
                      "content": "Detected new EVO maintenance message in group"},
        },
        "body": {"elements": elements},
    }


def _post_card_and_thread(key: str, *, group: str, sender: str, text: str,
                          m_ready: bool, why: str) -> Optional[str]:
    """Post the card, then thread the raw notice underneath it.

    Order matters: the CARD is the standalone parent and the notice is a TEXT
    reply. Interactive cards posted *as* thread replies render invisibly in
    Feishu (main.py:4345) — the user would see only a reaction.
    """
    import main as _main

    card = build_card(key, group=group, sender=sender, text=text,
                      m_ready=m_ready, why=why)
    # reply_to_message_id="" is mandatory: left at None, send_message silently
    # turns this into a quote-reply to whatever inbound message is in the
    # _lark_user_message_id contextvar (main.py:1904).
    resp = _main.send_message(
        CARD_CHAT_ID, json.dumps(card, ensure_ascii=False),
        msg_type="interactive", reply_to_message_id="",
    )
    if int((resp or {}).get("code", -1)) != 0:
        print(f"[evoteams] card send failed: {resp!r}", flush=True)
        return None

    card_mid = _main._extract_lark_message_id(resp)
    if not card_mid:
        print(f"[evoteams] no message_id in card response: {resp!r}", flush=True)
        return None

    # The /reply endpoint ignores chat_id and posts into the parent's chat, so
    # this always lands under the card (main.py:1898).
    try:
        rep = _main.reply_message_in_thread(card_mid, (text or "").strip())
        if int((rep or {}).get("code", -1)) != 0:
            print(f"[evoteams] thread reply failed: {rep!r}", flush=True)
    except Exception as err:  # noqa: BLE001
        print(f"[evoteams] thread reply raised: {err!r}", flush=True)

    return card_mid


# ---------------------------------------------------------------------------
# Entry point from teamswatch
# ---------------------------------------------------------------------------
_GROUP_MIN_CHARS = 12


def in_watched_group(group: str) -> bool:
    """Anchored, case-insensitive title comparison that fails CLOSED.

    The previous version returned **True** for an empty group name: ``hay in
    needle`` with ``hay == ""`` is trivially true, so a message whose group could
    not be determined was treated as the watched group and could be emailed.
    Both sides must now be substantial, and one must be an anchored prefix of the
    other — a bare "@" or an archived clone no longer qualifies.
    """
    try:
        import teamswatch as _tw

        return _tw._titles_match(WATCH_GROUP, group)
    except Exception:
        # Self-contained fallback so detection never depends on teamswatch importing.
        needle = (WATCH_GROUP or "").strip().lower()
        hay = (group or "").strip().lower()
        if len(needle) < _GROUP_MIN_CHARS or len(hay) < _GROUP_MIN_CHARS:
            return False
        return needle.startswith(hay) or hay.startswith(needle)


def handle_teams_message(*, group: str, message_id: str, text: str,
                         sender: str = "", ts: Any = None) -> bool:
    """Process one Teams message. Returns True when a card was posted."""
    if not _enabled():
        return False
    if not in_watched_group(group):
        return False

    # Remember it FIRST, so a crash between here and the card cannot make the
    # watcher replay the group's backlog on the next boot.
    set_last_seen(group, message_id, ts)

    body = (text or "").strip()
    if not body:
        return False

    key = content_key(body)
    if already_handled(key):
        print(f"[evoteams] already handled {key} — no second card", flush=True)
        return False

    verdict = classify(body)
    if not verdict["is_notice"]:
        print(f"[evoteams] not a notice ({verdict['why']}) — msg {message_id}",
              flush=True)
        return False

    # Stored before the card goes out: the button reads the text back from here,
    # so it must exist even if the process dies immediately after posting.
    record(key, outcome="pending", group=group, sender=sender,
           teams_message_id=str(message_id or ""), text=body,
           m_ready=bool(verdict["m_ready"]), why=verdict["why"])

    card_mid = _post_card_and_thread(
        key, group=group, sender=sender, text=body,
        m_ready=bool(verdict["m_ready"]), why=verdict["why"],
    )
    if not card_mid:
        # Leave it pending but flag it, so a failed post is visible instead of
        # looking like a message that was never a notice.
        record(key, outcome="pending", card_error="card post failed")
        return False

    record(key, card_message_id=card_mid)
    print(f"[evoteams] card posted for {key} -> {CARD_CHAT_ID}", flush=True)
    return True


# ---------------------------------------------------------------------------
# Card buttons
# ---------------------------------------------------------------------------
def handle_card_callback(parsed_ca: dict, ev_ca: dict, chat_id_ca: str) -> Optional[dict]:
    """Synchronous handler for Generate Email / Cancel.

    Returns None when the click is not ours, so main.py's dispatch chain can
    keep looking. Slow work goes to a thread — Lark drops the callback if the
    HTTP response takes longer than ~3s (main.py:3745).
    """
    k = str(parsed_ca.get("k") or "").strip().lower()
    if k not in ("evom_gen", "evom_cancel"):
        return None

    key = str(parsed_ca.get("i") or "").strip()
    if not key:
        return {"toast": {"type": "error", "content": "Card is missing its record id."}}

    rec = get_record(key)
    if not rec:
        return {"toast": {"type": "error",
                          "content": "This notice is no longer in the ledger."}}

    prior = str(rec.get("outcome") or "")
    if prior in ("emailed", "cancelled"):
        # Idempotent: a double-tap must never send the email twice.
        return {"toast": {"type": "info",
                          "content": f"Already {prior} — nothing to do."}}

    ctx = ev_ca.get("context") if isinstance(ev_ca.get("context"), dict) else {}
    card_mid = (ctx.get("open_message_id") or ev_ca.get("open_message_id") or "").strip()

    if k == "evom_cancel":
        record(key, outcome="cancelled")
        threading.Thread(target=_finish_card, args=(card_mid, key, "Cancelled"),
                         daemon=True).start()
        return {"toast": {"type": "info", "content": "Cancelled — recorded."}}

    text = str(rec.get("text") or "").strip()
    if not text:
        return {"toast": {"type": "error", "content": "No notice text stored."}}

    def _gen_job() -> None:
        try:
            import main as _main

            # This IS `/m`: the same function the command and the auto-detect
            # both converge on (main.py:4290). It reads no event fields, posts
            # the forward card + check-email ping + result card, and sends the
            # mail. CARD_CHAT_ID must be the EVO batch command group or
            # _process_evo_sd_batch_paste refuses it.
            _main._process_evo_sd_batch_paste(CARD_CHAT_ID, text)
            record(key, outcome="emailed")
            _finish_card(card_mid, key, "Email generated")
        except Exception as err:  # noqa: BLE001
            print(f"[evoteams] generate email failed: {err!r}", flush=True)
            record(key, outcome="pending", last_error=repr(err))
            try:
                import main as _main

                _main.send_message(
                    CARD_CHAT_ID,
                    f"❌ EVO maintenance email failed for `{key}`: `{err}`",
                    reply_to_message_id="",
                )
            except Exception:
                pass

    threading.Thread(target=_gen_job, daemon=True).start()
    return {"toast": {"type": "success", "content": "Generating email…"}}


def _finish_card(card_mid: str, key: str, outcome_label: str) -> None:
    """Replace the card with a button-less version.

    No card in this repo uses a ``disabled`` attribute — buttons are removed by
    PATCHing a rebuilt card (main.py:4592). That is what stops a second tap.
    """
    if not card_mid:
        return
    rec = get_record(key)
    card = {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "green" if outcome_label == "Email generated" else "grey",
            "title": {"tag": "plain_text",
                      "content": f"EVO maintenance — {outcome_label}"},
        },
        "body": {"elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content":
                f"**Group:** {rec.get('group') or '—'}\n"
                f"**From:** {rec.get('sender') or '—'}\n"
                f"**Outcome:** {rec.get('outcome') or '—'} at {rec.get('at') or '—'}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md",
                                    "content": _preview(str(rec.get("text") or ""))}},
        ]},
    }
    try:
        import offsetleave as _ol

        # Returns False rather than raising; ephemeral cards often cannot be
        # patched at all, and that is not an error worth shouting about.
        if not _ol._try_patch_interactive_card_message(card_mid, card):
            print(f"[evoteams] card patch declined for {card_mid}", flush=True)
    except Exception as err:  # noqa: BLE001
        print(f"[evoteams] card patch failed: {err!r}", flush=True)


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
def status_lines() -> list[str]:
    with _STATE_LOCK:
        state = _load_state()
    handled = state.get("handled") or {}
    counts: dict[str, int] = {}
    for rec in handled.values():
        out = str(rec.get("outcome") or "?")
        counts[out] = counts.get(out, 0) + 1
    seen = state.get("last_seen", {}).get(_group_key(WATCH_GROUP)) or {}
    lines = [
        f"{'🟢' if _enabled() else '⚪'} EVO Teams detector: "
        f"{'ON' if _enabled() else 'OFF (set EVOTEAMS_ENABLED=1)'}",
        f"• Watching group: {WATCH_GROUP}",
        f"• Card goes to: {CARD_CHAT_ID}",
        f"• Ledger: {_STATE_PATH.name} "
        f"({'present' if _STATE_PATH.exists() else 'not created yet'})",
        f"• Latest message seen: {seen.get('at') or '—'}",
        f"• Records: " + (", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
                          if counts else "none"),
    ]
    if _llm_enabled():
        lines.append("• LLM confirmation: ON (soft matches only)")
    return lines


if __name__ == "__main__":
    print("\n".join(status_lines()))
