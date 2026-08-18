#!/usr/bin/env python3
"""Detect EVO maintenance notices posted in a Microsoft Teams group.

The Teams side (session, capture) lives in :mod:`teamswatch`. This module owns
everything after a message has been read: deciding whether it is an EVO
maintenance notice, posting the confirm card, and — on **Generate Email** —
running the exact same pipeline as ``/m``.

Flow
----
teamswatch's warm watcher polls the group every EVOTEAMS_POLL_SECONDS and hands
every scraped row to handle_new_messages():

  -> cursor: which Teams data-mid did we already pass? (survives a restart)
     * never polled before -> adopt the newest as a BASELINE and card nothing,
       so switching this on does not card the group's existing backlog
  -> for each message newer than the cursor, OLDEST FIRST:
     -> classify: an EVO maintenance notice, or ordinary chatter?
     -> ※SD※ batch  -> card to EVOTEAMS_CARD_CHAT_ID: "Detected new EVO
        maintenance message" with [Generate Email] [Cancel], and the raw content
        posted as a THREADED reply under that card
     -> notice-shaped but NOT ※SD※ -> recorded ``soft_skipped`` and counted on
        /teamstatus; no card, because /m would refuse it (EVOTEAMS_SOFT_CARDS=1
        to card these anyway)
     -> advance the cursor — except after a failed card post, where it is held
        back deliberately so the next poll retries
  -> Generate Email -> claim pending->sending (one tap only, ever), buttons come
     off, then main._process_evo_sd_batch_paste() == what typing `/m` does. The
     ledger records ``emailed`` only if a mail actually went out; otherwise the
     buttons come BACK and the group is told why.
  -> Cancel        -> the ledger records ``cancelled``, nothing is sent. Refused
     once a send is already in flight, because /m cannot be recalled.

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

Exposed:
    handle_new_messages(...)        -> the poll entry point (teamswatch)
    handle_teams_message(...)       -> one message, for callers that have just one
    force_card(...)                 -> `--detect-now`, card on demand for testing
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


def _soft_cards_enabled() -> bool:
    """Card notice-shaped messages that ``/m`` cannot email?

    OFF by default: every such card carries a button guaranteed to fail, so the
    group would collect cards nobody can use. The skip is RECORDED (outcome
    ``soft_skipped``) and counted by /teamstatus, so a real notice arriving in an
    unrecognised format is visible rather than silently dropped — which is the
    only reason not to have this on.
    """
    return _truthy(os.getenv("EVOTEAMS_SOFT_CARDS"))


def _dry_run() -> bool:
    """Detect and log, but post nothing.

    For the first days of a deployment: watch the journal, confirm it fires on
    real notices and nothing else, then clear the flag. Ledger records are still
    written (outcome ``dry_run``) so the same message is not re-logged forever.
    """
    return _truthy(os.getenv("EVOTEAMS_DRY_RUN"))


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
    order = [str(x) for x in (state.get("order") or [])]
    handled = dict(state.get("handled") or {})
    # Cap the tail, but NEVER evict a record that is still awaiting a tap. Its
    # button reads the notice text back from here, so an evicted `pending` is both
    # a dead button and a forgotten dedupe entry — the same notice would card
    # again on the next restart.
    keep = set(order[-_MAX_HANDLED:])
    keep |= {k for k, v in handled.items()
             if str((v or {}).get("outcome") or "") in ("pending", "sending")}
    order = [k for k in order if k in keep]
    handled = {k: v for k, v in handled.items() if k in keep}
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
    """Advance the group's cursor. Never regresses.

    Teams' data-mid is the send time in epoch millis, so a SMALLER id means an
    out-of-order call, and moving the cursor backwards would re-card every notice
    in between on the next poll. A read that fell back to selectors carrying no
    data-mid must likewise not clobber a usable numeric cursor — it records that
    it happened and leaves the cursor where it was.
    """
    with _STATE_LOCK:
        state = _load_state()
        gkey = _group_key(group)
        prev = state["last_seen"].get(gkey) or {}
        new_id = str(message_id or "")
        prev_id = str(prev.get("message_id") or "")
        if new_id.isdigit() and prev_id.isdigit() and int(new_id) < int(prev_id):
            print(f"[evoteams] ignoring cursor regression {prev_id} -> {new_id}",
                  flush=True)
            return
        if prev_id.isdigit() and not new_id.isdigit():
            state["last_seen"][gkey] = {**prev, "at": _now_str(),
                                        "last_unnumbered_at": _now_str()}
            _save_state(state)
            return
        state["last_seen"][gkey] = {
            "message_id": new_id,
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


# Outcomes that settle a notice. "sending" belongs here: a /m run in flight must
# refuse a second tap, and it cannot be cancelled either — the email is already on
# its way out.
_TERMINAL = ("emailed", "cancelled", "sending", "soft_skipped", "dry_run")


def claim(key: str, *, frm: tuple[str, ...], to: str) -> bool:
    """Compare-and-set a record's ``outcome`` under the ledger lock.

    Replaces a read-then-act pair that was a TOCTOU: the callback checked
    ``outcome == "pending"``, then only wrote ``"emailed"`` AFTER the whole /m
    pipeline (token fetch, gamelist sheet reads, SMTP, three Lark posts) had
    finished — tens of seconds during which the card still showed live buttons. A
    second delivery inside that window passed the same check, and the maintenance
    email went to EVO twice.
    """
    with _STATE_LOCK:
        state = _load_state()
        entry = dict(state["handled"].get(key) or {})
        if not entry:
            return False
        if str(entry.get("outcome") or "") not in frm:
            return False
        entry["outcome"] = to
        entry["at"] = _now_str()
        state["handled"][key] = entry
        order = [x for x in state["order"] if x != key]
        order.append(key)
        state["order"] = order
        _save_state(state)
        return True


def already_handled(key: str) -> bool:
    """True once this notice is settled, or a card is actually live for it.

    Deliberately NOT ``bool(get_record(key))``. A record whose card POST failed is
    left ``pending`` with no ``card_message_id``, and counting that as handled
    buried the notice permanently: no card, no retry, nothing but one log line.
    Such a record is retried instead — handle_new_messages holds the cursor back
    on a failed post so the next poll sees the message again.
    """
    rec = get_record(key)
    if not rec:
        return False
    if str(rec.get("outcome") or "") in _TERMINAL:
        return True
    return bool(rec.get("card_message_id"))


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


# An explicit ticket reference. Strong on its own: nobody types SD-7362822 or
# TINC-1234 in passing.
_TICKET_REF_RE = re.compile(r"(?i)※\s*SD-\d+|\bSD-\d{5,}|\[Service Desk\]|\bTINC-\d")


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
    # Split into STRONG and CORROBORATING. A single weak signal used to be enough
    # to call something a notice, and the weak ones fire on ordinary CS-group
    # traffic: extract_candidate_game_names finds table names in "is Speed
    # Baccarat 3 back up?", and classify_checkemail_step_kind labels a plain
    # "maintenance cancelled" message. Since soft matches are now recorded and
    # counted on /teamstatus as "a real notice may have been missed", letting
    # chatter in would bury the one case that warning exists for.
    strong: list[str] = []
    weak: list[str] = []
    kind = "other"
    try:
        if _TICKET_REF_RE.search(body):
            strong.append("Service Desk ticket reference")
        if maintenance._FOLLOWING_TABLES_UNAVAILABLE_RE.search(body):
            strong.append("'following tables will be unavailable'")
        if maintenance._DOWNTIME_SCHEDULE_TABLES_RE.search(body):
            strong.append("'downtime' schedule phrasing")
        games = maintenance.extract_candidate_game_names(body) or []
        if games:
            weak.append(f"{len(games)} plausible game name(s)")
        kind = maintenance.classify_checkemail_step_kind(body, email_subject="")
        if kind and kind != "other":
            weak.append(f"step kind '{kind}'")
    except Exception as err:  # noqa: BLE001
        print(f"[evoteams] soft signal check raised: {err!r}", flush=True)
        kind = "other"

    if not strong:
        return {"is_notice": False, "m_ready": False,
                "why": "no maintenance-notice phrasing"
                       + (f" (only {'; '.join(weak)})" if weak else ""),
                "kind": ""}

    if _llm_enabled() and not _llm_confirms(body):
        return {"is_notice": False, "m_ready": False,
                "why": "soft signals but LLM said chatter", "kind": ""}

    return {"is_notice": True, "m_ready": False,
            "why": "; ".join(strong + weak),
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
               m_ready: bool, why: str, when: str = "") -> dict[str, Any]:
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
            # BOTH times. "Seen" alone cannot tell a notice posted a minute ago
            # from a backlog message the watcher only just got to — and the
            # difference decides whether it is still worth acting on.
            f"**Posted in Teams:** {when or 'unknown'}\n"
            f"**Detected:** {_now_str()}"}},
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
                          m_ready: bool, why: str,
                          when: str = "") -> Optional[str]:
    """Post the card, then thread the raw notice underneath it.

    Order matters: the CARD is the standalone parent and the notice is a TEXT
    reply. Interactive cards posted *as* thread replies render invisibly in
    Feishu (main.py:4345) — the user would see only a reaction.
    """
    import main as _main

    card = build_card(key, group=group, sender=sender, text=text,
                      m_ready=m_ready, why=why, when=when)
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
    body = (text or "").strip()
    try:
        rep = _main.reply_message_in_thread(card_mid, body)
        if int((rep or {}).get("code", -1)) != 0:
            print(f"[evoteams] thread reply failed: {rep!r}", flush=True)
            # A real batch notice runs to ~9,000 characters. If Lark refused the
            # single reply, split on the notice's own "=====" separators so every
            # part is a whole SD block — losing content here would mean the card
            # shows a preview of something nobody can read in full.
            _reply_in_parts(_main, card_mid, body)
    except Exception as err:  # noqa: BLE001
        print(f"[evoteams] thread reply raised: {err!r}", flush=True)
        _reply_in_parts(_main, card_mid, body)

    return card_mid


def _reply_in_parts(_main, card_mid: str, body: str) -> None:
    """Fallback for an oversized threaded reply: post it in pieces."""
    try:
        import teamswatch as _tw

        parts = _tw._split_for_lark(body, limit=3500)
    except Exception as err:  # noqa: BLE001
        print(f"[evoteams] cannot split the notice ({err!r}) — reply skipped",
              flush=True)
        return
    if len(parts) <= 1:
        return
    for i, part in enumerate(parts, 1):
        try:
            _main.reply_message_in_thread(
                card_mid, f"({i}/{len(parts)})\n{part}")
        except Exception as err:  # noqa: BLE001
            print(f"[evoteams] part {i}/{len(parts)} reply failed: {err!r}",
                  flush=True)


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
                         sender: str = "", ts: Any = None,
                         force: bool = False) -> str:
    """Process one Teams message.

    Returns a status string — ``"carded"``, ``"duplicate"``, ``"not_notice"``,
    ``"soft_skipped"``, ``"dry_run"``, ``"card_failed"``, ``"disabled"``,
    ``"wrong_group"`` or ``"empty"``. A string rather than a bool because the poll
    needs to tell "nothing to do" apart from "a notice we chose not to card":
    only the second is worth surfacing on /teamstatus.

    ``force`` is the ``--detect-now`` path: it skips the enabled gate and the
    group check so a card can be produced on demand for testing. It still honours
    the ledger, so testing can never send the same maintenance email twice.
    """
    if not force and not _enabled():
        return "disabled"
    if not force and not in_watched_group(group):
        return "wrong_group"

    # The CURSOR is advanced by handle_new_messages, after this returns — not here
    # and not first. Advancing before the outcome is known is what made a failed
    # card post unrecoverable: the message was marked seen, so the retry that
    # already_handled now permits would never get a second look at it.
    body = (text or "").strip()
    if not body:
        return "empty"

    key = content_key(body)
    if already_handled(key):
        prior = str(get_record(key).get("outcome") or "?")
        print(f"[evoteams] already handled {key} ({prior}) — no second card",
              flush=True)
        return "duplicate"

    verdict = classify(body)
    if not verdict["is_notice"]:
        print(f"[evoteams] not a notice ({verdict['why']}) — msg {message_id}",
              flush=True)
        return "not_notice"

    if not verdict["m_ready"] and not _soft_cards_enabled():
        # Recorded, not just logged: the record is what stops this repeating and
        # what lets /teamstatus say "N notice-shaped messages went uncarded".
        record(key, outcome="soft_skipped", group=group, sender=sender,
               teams_message_id=str(message_id or ""), text=body,
               msg_time=str(ts or ""), m_ready=False, why=verdict["why"])
        print(f"[evoteams] notice-shaped but NOT ※SD※ batch, so /m would refuse "
              f"it — no card (set EVOTEAMS_SOFT_CARDS=1 to card these). "
              f"Signals: {verdict['why']} — msg {message_id} from {sender!r}",
              flush=True)
        return "soft_skipped"

    if _dry_run():
        record(key, outcome="dry_run", group=group, sender=sender,
               teams_message_id=str(message_id or ""), text=body,
               msg_time=str(ts or ""), m_ready=bool(verdict["m_ready"]),
               why=verdict["why"])
        print(f"[evoteams] DRY RUN — would card {key} to {CARD_CHAT_ID} "
              f"({len(body)} chars, {verdict['why']}) — msg {message_id}",
              flush=True)
        return "dry_run"

    # Stored before the card goes out: the button reads the text back from here,
    # so it must exist even if the process dies immediately after posting.
    record(key, outcome="pending", group=group, sender=sender,
           teams_message_id=str(message_id or ""), text=body,
           msg_time=str(ts or ""), m_ready=bool(verdict["m_ready"]),
           why=verdict["why"])

    card_mid = _post_card_and_thread(
        key, group=group, sender=sender, text=body,
        m_ready=bool(verdict["m_ready"]), why=verdict["why"],
        when=str(ts or ""),
    )
    if not card_mid:
        # Leave it pending but flag it, so a failed post is visible instead of
        # looking like a message that was never a notice.
        record(key, outcome="pending", card_error="card post failed")
        return "card_failed"

    record(key, card_message_id=card_mid)
    print(f"[evoteams] card posted for {key} -> {CARD_CHAT_ID}", flush=True)
    return "carded"


# ---------------------------------------------------------------------------
# Poll entry point — what is new since last time?
# ---------------------------------------------------------------------------
def _cursor(group: str) -> tuple[bool, Optional[int]]:
    """``(polled_before, cursor_mid)`` for ``group``.

    Two separate facts. "Never polled" means adopt a baseline and card nothing;
    "polled but no usable id" means fall back to content dedupe. Collapsing them
    into one Optional would make an unnumbered read look like a first run and
    silently re-baseline, skipping everything that arrived meanwhile.
    """
    seen = get_last_seen(group)
    if not seen:
        return False, None
    raw = str(seen.get("message_id") or "").strip()
    return True, (int(raw) if raw.isdigit() else None)


def handle_new_messages(*, group: str, messages: list[dict[str, Any]],
                        newest_mid: str = "",
                        at_bottom: Any = None) -> dict[str, Any]:
    """Feed one poll's worth of scraped rows through detection.

    ``messages`` is teamswatch's oldest-first list (read with ``limit=0`` so a
    burst of notices inside one poll interval is all present, not just the tail).

    Returns ``{"new", "cards", "soft", "baselined", "why"}``.

    Ordering is Teams' own ``data-mid`` — epoch millis — because "everything
    newer than last time" is only answerable with a key that orders. Rows are
    processed OLDEST FIRST so the stored cursor lands on the newest even if a
    middle message throws.
    """
    out: dict[str, Any] = {"new": 0, "cards": 0, "soft": 0, "baselined": False,
                           "why": ""}
    if not _enabled():
        out["why"] = "EVOTEAMS_ENABLED not set"
        return out
    if not in_watched_group(group):
        out["why"] = f"not the watched group ({group!r})"
        print(f"[evoteams] {out['why']} — nothing processed", flush=True)
        return out

    rows = [m for m in (messages or []) if str(m.get("text") or "").strip()]
    if not rows:
        out["why"] = "no messages in this read"
        return out

    # A read that never reached the end of a virtualised list can present an OLD
    # row as the tail. Advancing the cursor on that would permanently skip
    # everything between it and the real end, so refuse the whole poll — the next
    # one costs 60 seconds, a skipped notice costs a missed maintenance.
    if at_bottom is False:
        out["why"] = ("read did not reach the end of the chat — poll ignored so "
                      "the cursor cannot skip past unrendered messages")
        print(f"[evoteams] {out['why']}", flush=True)
        return out

    polled_before, cursor = _cursor(group)
    numbered = [m for m in rows if str(m.get("mid") or "").isdigit()]
    numbered.sort(key=lambda m: int(m["mid"]))

    # FIRST, before any other branch: never card on a first run, whatever shape the
    # rows came back in. Adopt the newest as the baseline instead — otherwise
    # switching the feature on would card, and offer to email, whatever backlog the
    # group happens to be showing.
    if not polled_before:
        newest = (numbered or rows)[-1]
        set_last_seen(group, str(newest.get("mid") or ""),
                      newest.get("time") or newest.get("time_text"))
        out["baselined"] = True
        out["why"] = (
            f"baseline set at mid {newest.get('mid') or '(none)'} — the "
            f"{len(rows)} message(s) already in the group were left alone. New "
            f"posts from here on will be carded."
        )
        print(f"[evoteams] {out['why']}", flush=True)
        return out

    if len(numbered) != len(rows) or cursor is None:
        # No usable ordering: the fallback row selectors carry no data-mid, or the
        # stored cursor is not a numeric id. Treat only the newest rendered row as
        # current and let the content ledger dedupe, rather than inventing an
        # order and risking a skip.
        out["why"] = (
            f"{len(rows) - len(numbered)}/{len(rows)} row(s) carry no Teams "
            f"message id"
            if len(numbered) != len(rows) else
            "stored cursor is not a numeric message id"
        ) + " — content dedupe on the newest row only"
        print(f"[evoteams] {out['why']}", flush=True)
        tail = rows[-1]
        _tally(out, handle_teams_message(
            group=group, message_id=str(tail.get("mid") or ""),
            text=str(tail.get("text") or ""),
            sender=str(tail.get("author") or ""),
            ts=tail.get("time") or tail.get("time_text"),
        ))
        set_last_seen(group, str(tail.get("mid") or ""),
                      tail.get("time") or tail.get("time_text"))
        return out

    fresh = [m for m in numbered if int(m["mid"]) > int(cursor)]
    if not fresh:
        out["why"] = f"nothing newer than mid {cursor}"
        return out

    out["new"] = len(fresh)
    print(f"[evoteams] {len(fresh)} new message(s) past mid {cursor}: "
          f"{[m['mid'] for m in fresh]}", flush=True)
    for msg in fresh:  # oldest first — the cursor must end on the newest
        status = handle_teams_message(
            group=group, message_id=str(msg.get("mid") or ""),
            text=str(msg.get("text") or ""),
            sender=str(msg.get("author") or ""),
            ts=msg.get("time") or msg.get("time_text"),
        )
        _tally(out, status)
        if status == "card_failed":
            # STOP advancing. Lark refused the card, so leaving the cursor behind
            # this message is the only thing that gets it another attempt — and
            # already_handled lets the retry through because no card exists. Every
            # later message in this batch is re-examined next poll and deduped by
            # its own record, so nothing is lost by stopping here.
            out["retry_pending"] = True
            out["why"] = (f"card post failed for mid {msg.get('mid')} — cursor "
                          f"held back so the next poll retries")
            print(f"[evoteams] {out['why']}", flush=True)
            return out
        set_last_seen(group, str(msg.get("mid") or ""),
                      msg.get("time") or msg.get("time_text"))
    out["why"] = (f"{out['new']} new, {out['cards']} carded"
                  + (f", {out['soft']} notice-shaped but uncarded" if out["soft"]
                     else ""))
    return out


def _tally(out: dict[str, Any], status: str) -> None:
    if status == "carded":
        out["cards"] = int(out.get("cards") or 0) + 1
    elif status == "soft_skipped":
        out["soft"] = int(out.get("soft") or 0) + 1


def force_card(*, group: str | None = None, message: dict[str, Any]) -> str:
    """``--detect-now``: card the given scraped row regardless of the cursor.

    Bypasses the enabled flag and the baseline, NOT the ledger — so it can be run
    twice without any risk of two maintenance emails for one notice.
    """
    return handle_teams_message(
        group=group or WATCH_GROUP,
        message_id=str(message.get("mid") or message.get("id") or ""),
        text=str(message.get("text") or ""),
        sender=str(message.get("author") or ""),
        ts=message.get("time") or message.get("time_text"),
        force=True,
    )


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
    if prior in _TERMINAL:
        # Idempotent: a double-tap must never send the email twice.
        return {"toast": {"type": "info",
                          "content": "Already generating — hold on."
                                     if prior == "sending"
                                     else f"Already {prior} — nothing to do."}}

    ctx = ev_ca.get("context") if isinstance(ev_ca.get("context"), dict) else {}
    card_mid = (ctx.get("open_message_id") or ev_ca.get("open_message_id") or "").strip()

    if k == "evom_cancel":
        # Only a notice that is not already sending may be cancelled: a /m run
        # cannot be recalled, so a card reading "Cancelled" while the mail went out
        # would be a lie.
        if not claim(key, frm=("pending",), to="cancelled"):
            now = str(get_record(key).get("outcome") or "?")
            return {"toast": {"type": "info",
                              "content": f"Too late — already {now}."}}
        threading.Thread(target=_finish_card, args=(card_mid, key, "Cancelled"),
                         daemon=True).start()
        return {"toast": {"type": "info", "content": "Cancelled — recorded."}}

    text = str(rec.get("text") or "").strip()
    if not text:
        return {"toast": {"type": "error", "content": "No notice text stored."}}

    # Claim it BEFORE any slow work, and before returning the toast. This is the
    # only thing standing between a second delivery of this tap and a second
    # maintenance email — Lark redelivers callbacks, and the sync dispatch in
    # main.py records the event id only after this handler returns.
    if not claim(key, frm=("pending",), to="sending"):
        now = str(get_record(key).get("outcome") or "?")
        return {"toast": {"type": "info", "content": f"Already {now} — nothing to do."}}

    def _gen_job() -> None:
        # Buttons off first, not last: they used to stay live for the whole
        # pipeline, inviting the impatient second tap.
        _finish_card(card_mid, key, "Generating email…")
        try:
            import main as _main

            # This IS `/m`: the same function the command and the auto-detect
            # both converge on (main.py:4290). It reads no event fields, posts
            # the forward card + check-email ping + result card, and sends the
            # mail. CARD_CHAT_ID must be the EVO batch command group or
            # _process_evo_sd_batch_paste refuses it.
            out = _main._process_evo_sd_batch_paste(CARD_CHAT_ID, text) or {}
            if out.get("email_sent"):
                record(key, outcome="emailed")
                _finish_card(card_mid, key, "Email generated")
                return
            # No mail went out. Recording "emailed" here — which is what merely
            # returning used to mean — greened the card AND made already_handled
            # refuse every retry, losing the notice behind a success indication.
            reason = str(out.get("reason") or "the /m pipeline sent no email")
            record(key, outcome="pending", last_error=reason)
            _restore_card(card_mid, key, reason)
            _notify(f"⚠️ No EVO maintenance email was sent for `{key}`: {reason}\n"
                    f"The card's **Generate Email** button is live again.")
        except Exception as err:  # noqa: BLE001
            print(f"[evoteams] generate email failed: {err!r}", flush=True)
            record(key, outcome="pending", last_error=repr(err))
            _restore_card(card_mid, key, repr(err))
            _notify(f"❌ EVO maintenance email failed for `{key}`: `{err}`\n"
                    f"The card's **Generate Email** button is live again.")

    threading.Thread(target=_gen_job, daemon=True).start()
    return {"toast": {"type": "success", "content": "Generating email…"}}


def _notify(text: str) -> None:
    """Post a plain heads-up into the card group. Never raises."""
    try:
        import main as _main

        _main.send_message(CARD_CHAT_ID, text, reply_to_message_id="")
    except Exception as err:  # noqa: BLE001
        print(f"[evoteams] notify failed: {err!r}", flush=True)


_HEADER_TEMPLATE = {
    "Email generated": "green",
    "Generating email…": "blue",
    "Cancelled": "grey",
}


def _patch_card(card_mid: str, card: dict[str, Any]) -> None:
    """PATCH an already-posted card. Never raises."""
    if not card_mid:
        return
    try:
        import offsetleave as _ol

        # Returns False rather than raising; ephemeral cards often cannot be
        # patched at all, and that is not an error worth shouting about.
        if not _ol._try_patch_interactive_card_message(card_mid, card):
            print(f"[evoteams] card patch declined for {card_mid}", flush=True)
    except Exception as err:  # noqa: BLE001
        print(f"[evoteams] card patch failed: {err!r}", flush=True)


def _finish_card(card_mid: str, key: str, outcome_label: str) -> None:
    """Replace the card with a button-less version.

    No card in this repo uses a ``disabled`` attribute — buttons are removed by
    PATCHing a rebuilt card (main.py:4592). That is what stops a second tap, which
    is why this is now called BEFORE the /m run and not after it.
    """
    if not card_mid:
        return
    rec = get_record(key)
    card = {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": _HEADER_TEMPLATE.get(outcome_label, "grey"),
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
    _patch_card(card_mid, card)


def _restore_card(card_mid: str, key: str, reason: str) -> None:
    """Put the buttons BACK after a run that sent nothing.

    The alternative — leaving the button-less "Generating email…" card up — would
    present a notice that was never emailed as finished, with no way to retry from
    the card.
    """
    if not card_mid:
        return
    rec = get_record(key)
    card = build_card(key, group=str(rec.get("group") or ""),
                      sender=str(rec.get("sender") or ""),
                      text=str(rec.get("text") or ""),
                      m_ready=bool(rec.get("m_ready")),
                      why=str(rec.get("why") or ""),
                      when=str(rec.get("msg_time") or ""))
    card["header"]["template"] = "red"
    card["header"]["title"]["content"] = "EVO maintenance — NOT sent, retry available"
    card["body"]["elements"].insert(
        0, {"tag": "div", "text": {"tag": "lark_md",
                                   "content": f"⚠️ **Last attempt sent nothing:** {reason}"}})
    _patch_card(card_mid, card)


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
        # The cursor, not just its timestamp: "which message id have we passed" is
        # the one number that explains why a notice was or was not carded.
        f"• Cursor: mid {seen.get('message_id') or '(none — next poll sets the '
                                                  'baseline and cards nothing)'}"
        + (f" at {seen['at']}" if seen.get("at") else ""),
        f"• Records: " + (", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
                          if counts else "none"),
    ]
    if _dry_run():
        lines.append("🧪 DRY RUN: EVOTEAMS_DRY_RUN=1 — detects and logs, posts "
                     "NOTHING. Clear it to start carding.")
    lines.append(
        "• Cards for: ※SD※ batch notices only"
        + ("" if not _soft_cards_enabled() else " + soft matches (EVOTEAMS_SOFT_CARDS=1)")
    )
    if counts.get("soft_skipped"):
        lines.append(
            f"⚠️ {counts['soft_skipped']} notice-shaped message(s) went UNCARDED "
            f"because /m would refuse the format — check them by hand, or set "
            f"EVOTEAMS_SOFT_CARDS=1"
        )
    stuck = sum(1 for r in handled.values()
                if str(r.get("outcome") or "") == "pending"
                and not r.get("card_message_id"))
    if stuck:
        lines.append(f"🔴 {stuck} notice(s) have no card posted (Lark refused) — "
                     f"the poll retries these")
    if counts.get("sending"):
        lines.append(f"⏳ {counts['sending']} email(s) generating right now")
    if _llm_enabled():
        lines.append("• LLM confirmation: ON (soft matches only)")
    return lines


if __name__ == "__main__":
    print("\n".join(status_lines()))
