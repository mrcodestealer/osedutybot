#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Watch every provider Telegram group for scheduled-maintenance notices.

One group is read per tick, round-robin (see ``next_target``), so a full cycle
over ~17 groups costs under 20 minutes of a single-threaded browser instead of
saturating it. The group list comes from the Base via ``groupcheck.partition``,
so the excluded groups and the blank rows are defined in one place.

When a provider posts a notice for an UPCOMING maintenance window, this:
  * fills that provider's row in the maintenance Lark Base —
      Start Time, End Time (from the notice), Remark (the whole message),
      Last Check (now). Reference is left alone.
  * posts a card to the Laboratory group naming the group and quoting the notice.

A "maintenance completed" notice and an API-change announcement are IGNORED and
nothing is written. A completion notice quotes the SAME window as the original,
which is why the completed/cancelled test runs first and wins outright: a rule
that only looked for a window would re-fire on it.

The three OTHER verdicts noticeparse can return are not ignored either, because
each of them means a human has to do something and the row is wrong until they
do: a reschedule whose new window could not be read leaves the SUPERSEDED window
on the row, an explicit "no maintenance" leaves last week's window on it, and a
reply whose answer sits in the quoted message cannot be resolved from here. All
three post a card instead of vanishing (see ``handle_messages``).

A window is written only when it is provably THIS row's (see ``_attribute``):
bubbles we sent ourselves are skipped; in a group two rows share, a notice
fills only the row it names; and the operator's own platform notice, a
payment/telco/cloud relay, or another Base provider's notice relayed into the
group is carded for a human instead of overwriting the provider's real window.

Nothing is ever sent in Telegram. The watcher only reads.

Detection here is rule-based on purpose (``noticeparse``): this path writes to a
shared operational sheet unattended, so the rule that fired has to be
inspectable afterwards. The LLM is used only where a judgement call is genuinely
needed - deciding whether a message REPLIES to us - which is
/provideraskmaintenance's job, not this one's.

Deliberately NOT importing main (it starts the scheduler and can forward a real
maintenance email) and NOT importing ose_Duty: its bitable helpers hard-code
open.larksuite.com and default to the OSE base token, so a forgotten keyword
argument would write into the wrong Base and SUCCEED. The Lark block below is
self-contained, the same way groupcheck/telegramwarm/teamswatch each carry one.
"""

from __future__ import annotations

import functools
import hashlib
import json
import os
import re
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import requests

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

_ROOT_DIR = Path(__file__).resolve().parent
LEDGER_PATH = _ROOT_DIR / "vawatch.json"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _chat_title() -> str:
    """The Telegram group to read. Must match the Base's Group Name exactly."""
    return (os.getenv("VAWATCH_CHAT")
            or "\U0001F195VA 公告｜VA announcements").strip()


def _provider() -> str:
    """The Provider / Games value whose row gets filled."""
    return (os.getenv("VAWATCH_PROVIDER") or "VA").strip()


def _card_chat_id() -> str:
    """Where the "detected" card goes. Defaults to the Laboratory group."""
    return (os.getenv("VAWATCH_CARD_CHAT_ID")
            or os.getenv("LABORATORY_GROUP")
            or "oc_ad9b5bdbb2826ba2ee9730920ef25432").strip()


def _read_count() -> int:
    try:
        return max(1, min(50, int(os.getenv("VAWATCH_READ_COUNT", "8"))))
    except ValueError:
        return 8


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(os.getenv(name, str(default)))))
    except (TypeError, ValueError):
        return default


def _gap_read_cap() -> int:
    """How many bubbles to re-read when a read does not reach back to the
    newest bubble seen on the previous visit (see ``gap_read_count``).

    Each group is visited about every 18 minutes and only its last
    VAWATCH_READ_COUNT (8) bubbles are read, so a notice followed by 8 more
    bubbles before the rotation came back - partner acks, a CS ticket's
    screenshots, or simply a watcher pause - was never read at all and nothing
    noticed (F57). 50 is the same ceiling _read_count allows; a read never asks
    for fewer than the normal count.
    """
    return max(_read_count(), _env_int("VAWATCH_GAP_READ_COUNT", 50, 1, 50))


def _order_check() -> bool:
    """The data-mid ORDER rules in handle_messages. Default on.

    An older message never overwrites the one that set the row, history below
    the cold-start floor is not acted on, a second outage is deferred, a deleted
    notice and a read gap are reported - all of them trust Telegram's data-mid
    to increase within a chat. That is how Telegram numbers messages, but it is
    one scraped attribute, so (like VAWATCH_SKIP_OUTBOUND) there is a way back:
    0 restores newest-in-the-read-wins. The ledger's own safety (LRU eviction,
    refusing to act on an unreadable or unsaveable ledger, one bad bubble not
    aborting the sweep) is not behind this switch.
    """
    return _env_flag("VAWATCH_ORDER_CHECK", "1")


def _gap_card_enabled() -> bool:
    """Card the Laboratory group when even the wider re-read leaves a gap.
    Default on; at most once per group per _GAP_CARD_EVERY."""
    return _env_flag("VAWATCH_GAP_CARD", "1")


def _env_flag(name: str, default: str) -> bool:
    """A yes/no setting, where PRESENT-BUT-EMPTY means "unset", not "off".

    `os.getenv(name, default)` only returns the default when the variable is
    ABSENT, so a bare `VAWATCH_NEEDS_HUMAN_CARD=` line in .env - which dotenv
    loads as the empty string - read as OFF and silently switched the cards back
    off, and `VAWATCH_TARGET_CACHE=` collapsed the watch list on the next Lark
    5xx. Both are documented as defaulting to 1. _env_int already behaves this
    way (int("") raises and falls back to the default); this now matches it.
    """
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        raw = default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _write_attempts() -> int:
    """How many times one notice's Base write is tried in total (1 = no retry).

    A write that FAILED used to be ledgered exactly like a write that succeeded,
    and the ledger is what suppresses a second look - so one Lark 502, one
    expired tenant token or one rate-limit lost that provider's notice for good.
    Retrying is now the default, but it has to be BOUNDED: a permanently broken
    row (a Provider / Games value that exists in no Base row) fails identically
    every time, and every attempt cards the Laboratory group. At one group per
    60s tick a full rotation over the 18 watched groups is ~18 minutes, so three
    attempts is ~36 minutes of retrying and at most three cards - long enough to
    ride out a token refresh or a 5xx, short enough that a typo in a provider
    name is not a permanent alarm.
    """
    return _env_int("VAWATCH_WRITE_ATTEMPTS", 3, 1, 20)


def _reparse_attempts() -> int:
    """How many sweeps may re-read a notice whose window would not parse.

    Telegram Web renders its message list lazily, so a bubble can be read while
    only half of it exists: the heading is there, the "10:00 - 12:00" line is
    not. That classified as "maintenance wording but no parseable date/time
    window", got marked, and was never looked at again even though the very next
    sweep would have read it in full. Three passes over the same message is two
    more chances at ~18 minutes apart and costs nothing - no write, no card,
    just a re-run of the regexes on text already in hand.
    """
    return _env_int("VAWATCH_REPARSE_ATTEMPTS", 3, 1, 20)


def _clear_enabled() -> bool:
    """May a "no maintenance" notice BLANK the row? Default no.

    noticeparse's ``clear`` verdict means the provider stated there is nothing
    planned, and the honest answer is to empty Start Time / End Time and write
    Remark = "No maintenance" - otherwise the row keeps last week's window and
    the formula column keeps saying "Yes". The reason this is off by default is
    narrow and specific: clearing a bitable DateTime cell means sending the
    field with an empty value, and if this tenant's field config rejects the
    null the WHOLE payload is rejected - so the row would not even get its
    Remark. Switch it on once you have watched one clear land (see
    VAWATCH_CLEAR_ENABLED in .env.example); until then the notice is carded for
    a human instead of being silently dropped, which is what used to happen.
    """
    return _env_flag("VAWATCH_CLEAR_ENABLED", "0")


def _needs_human_card() -> bool:
    """Kill-switch for the "a human must look at this" cards. Default on."""
    return _env_flag("VAWATCH_NEEDS_HUMAN_CARD", "1")


def _target_cache_enabled() -> bool:
    """Kill-switch for reusing the last known good watch list. Default on."""
    return _env_flag("VAWATCH_TARGET_CACHE", "1")


def _skip_outbound() -> bool:
    """Ignore bubbles WE sent. Default on. See the guard in handle_messages.

    A kill-switch rather than an unconditional rule only because it depends on
    one scraped attribute: if a future Telegram Web build broke telegramwarm's
    isOut detection every bubble could read as ours and the watcher would go
    quiet. Setting this to 0 restores the old behaviour - our own messages are
    classified like anyone else's - which is why the default is 1.
    """
    return _env_flag("VAWATCH_SKIP_OUTBOUND", "1")


def _shared_group_check() -> bool:
    """In a group two Base rows share, write a row only when the notice names it.

    Default on. Hacksaw and YGG are both "[SG190- IGO Casinoplus YG/ RG/ HS] CS
    group", and with the ledger keyed per provider@group every notice posted
    there was written onto BOTH rows - a Yggdrasil-only window landed on
    Hacksaw, and a later Hacksaw notice then overwrote YGG's real window. Off
    restores that behaviour, which is why this is a kill-switch and not a knob.
    """
    return _env_flag("VAWATCH_SHARED_GROUP_CHECK", "1")


def _owner_check() -> bool:
    """Refuse to write maintenance that is visibly not the provider's. Default on.

    The operator's own platform notice (CasinoPlus/IGO, 我司…暂停发版), a
    GCash/BDO/Globe/AWS relay, or another Base provider's notice relayed into
    this group all parse as a perfect window, and each one used to overwrite the
    provider's real window. See ``_foreign_owner``. Off = the old behaviour.
    """
    return _env_flag("VAWATCH_OWNER_CHECK", "1")


# Names a provider goes by in its own notices, keyed by any one of them. The Base
# says "YGG" while the studio signs "Yggdrasil"; "HS" is how the shared group's
# own title abbreviates Hacksaw. Every name in one entry is an alias of every
# other, so a row renamed from "YGG" to "Yggdrasil" keeps matching.
_DEFAULT_PROVIDER_ALIASES = "Hacksaw=HS,Hacksaw Gaming;YGG=Yggdrasil,YG"

# The operator whose CS groups these are. A notice whose SUBJECT is one of these
# is the operator's maintenance, not the provider's.
_DEFAULT_OPERATOR_NAMES = "CasinoPlus,Casino Plus,IGO,CP"


def _split_names(raw: str) -> list:
    return [" ".join(x.split()) for x in re.split(r"[,|]+", raw or "") if x.strip()]


def _alias_groups() -> list:
    """VAWATCH_PROVIDER_ALIASES as a list of name sets.

    Format: ``Name=alias,alias;Other=alias``. A non-empty value REPLACES the
    default rather than extending it, so a wrong default alias can be removed;
    dropping one only makes the shared-group rule ask a human more often, which
    is the safe direction.
    """
    raw = os.getenv("VAWATCH_PROVIDER_ALIASES")
    if raw is None or not raw.strip():
        raw = _DEFAULT_PROVIDER_ALIASES
    groups = []
    for part in re.split(r"[;\n]+", raw):
        if "=" not in part:
            continue
        head, tail = part.split("=", 1)
        names = _split_names(head) + _split_names(tail)
        if names:
            groups.append(names)
    return groups


def _operator_names() -> list:
    raw = os.getenv("VAWATCH_OPERATOR_NAMES")
    if raw is None or not raw.strip():
        raw = _DEFAULT_OPERATOR_NAMES
    return _split_names(raw)


def _operator_senders() -> set:
    """Telegram display names of OUR colleagues (VAWATCH_OPERATOR_SENDERS).

    Empty by default because nobody has listed them yet. A bubble from one of
    them is the operator talking - relaying its own platform window or a
    partner's - so it is carded, never written. Matched whole and
    case-insensitively after whitespace is collapsed; ``|`` or ``,`` separated.
    """
    return {_norm(x) for x in _split_names(os.getenv("VAWATCH_OPERATOR_SENDERS") or "")}


def _tz():
    """The zone a bare 'GMT+8'-less notice is assumed to be in."""
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(os.getenv("VAWATCH_TZ", "Asia/Manila"))
    except Exception:
        return timezone(timedelta(hours=8))


def _now_dt() -> datetime:
    """The watcher's clock, in one place.

    The row-owner and deferred-window rules below compare a window's END with
    now ("is the window this row holds still ahead?"), and noticeparse decides
    `stale` with its own now. Two clocks read a few ms apart are harmless in
    production, but the offline selftest pins classify() to a fixed instant and
    must be able to pin this one to the same instant, or "still ahead" and
    "already passed" would be decided against different days.
    """
    return datetime.now(_tz())


def _now_str() -> str:
    return _now_dt().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify(text: str) -> dict:
    """Delegates to noticeparse.

    The rules moved there when the watcher grew from one group to all of them:
    a "Rescheduled / 时间变更" notice must OVERRIDE the row, and this module's
    first cut had `rescheduled|postponed` inside its completed/cancelled pattern,
    which made the override a guaranteed no-op. Keeping one copy of the rules
    means /provideraskmaintenance and the passive watcher can never disagree
    about what a message means.
    """
    import noticeparse

    return noticeparse.classify(text)


def find_window(text: str):
    import noticeparse

    return noticeparse.find_window(text)


# ---------------------------------------------------------------------------
# Which groups to watch
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    return " ".join(str(s or "").split()).casefold()


def _row_note(row: dict, why: str) -> dict:
    """One Base row nothing on this path reads, in a shape a card can print."""
    return {"provider": row.get("provider") or "", "group": row.get("group") or "",
            "why": row.get("why") or why}


# The last watch list read in this process, whatever VAWATCH_TARGET_CACHE says.
# handle_messages needs to know which OTHER rows share its group and which other
# providers exist; this lets it know without a Base call.
_LAST_WATCH: dict = {}


def _annotate_shared(rows: list) -> None:
    """Give each row ``shared_with``: the other providers whose row names the
    same group. Empty for the ordinary one-row group. In place."""
    by_group: dict = {}
    for r in rows:
        by_group.setdefault(_norm(r.get("group")), []).append(r.get("provider") or "")
    for r in rows:
        mine = _norm(r.get("provider"))
        r["shared_with"] = [p for p in by_group.get(_norm(r.get("group")), [])
                            if p and _norm(p) != mine]


def watch_list() -> dict:
    """What the Base says, split into what is watched and what is not.

    -> {"telegram": [{provider, group, record_id, shared_with}],
        "teams": [...], "skipped": [...],
        "source": "base"|"cache"|"fallback", "at": str, "error": str}

    ``shared_with`` lists the OTHER providers whose row names the same group
    (Hacksaw <-> YGG today); see ``_attribute``.

    Reuses groupcheck's partition, so the excluded groups and the blank rows are
    defined in exactly one place and the watcher can never drift from what
    /telegramgroupcheck reports.

    ``teams`` and ``skipped`` used to be thrown away at the `_teams, _skipped`
    underscore. Three real providers (GEMINI, PG Soft, RTG) are APP=TEAMS rows,
    which means nothing autofills them and nothing anywhere SAID so - the gap
    read as "those providers never post a notice". They are carried out of here
    now so the rotation can log them and /vacheck can name them. This is not a
    Teams reader; it is the gap made loud.

    A failed read no longer collapses the watch list. The old code caught every
    exception and returned the single configured VAWATCH_CHAT row, so one
    transient 5xx shrank 18 groups to 1 for that tick AND (via the old index
    cursor, `idx % 1` -> 0) reset the rotation to the top, starving every
    provider past the first. The last good list is kept in the ledger and reused
    instead; the single-group fallback is only for a watcher that has never had
    a good read.
    """
    err = ""
    try:
        import groupcheck as gc

        telegram, teams, skipped = gc.partition(gc.fetch_rows())
        # The record_id travels with the row now. The write used to throw it
        # away and re-find the row by NAME over the whole table with a
        # different flattener, so a two-segment Provider cell never matched and
        # two rows with one name always hit the first (F70).
        rows = [{"provider": (r.get("provider") or "").strip(),
                 "group": r.get("group") or "",
                 "record_id": r.get("record_id") or ""}
                for r in telegram if (r.get("group") or "").strip()]
        # A TELEGRAM row with a blank Provider has nothing to write to.
        # Watching it anyway is how its notices ended up on the VA row:
        # telegramwarm substituted _provider() ("VA") for the blank before
        # act_on_notice's refusal could run (F39). It is named in `skipped`
        # instead, which is where /vacheck and the rotation log look.
        blank = [r for r in rows if not r["provider"]]
        rows = [r for r in rows if r["provider"]]
        _annotate_shared(rows)
        if rows:
            rep = {"telegram": rows,
                   "teams": [_row_note(r, "APP is TEAMS - this watcher only "
                                          "reads Telegram") for r in teams],
                   "skipped": [_row_note(r, "skipped by groupcheck")
                               for r in skipped]
                              + [_row_note(r, "no Provider / Games value - "
                                              "nothing to write to")
                                 for r in blank],
                   "source": "base", "at": _now_str(), "error": ""}
            _remember_targets(rep)
            _LAST_WATCH["rep"] = rep
            return rep
        err = "the Base returned no watchable TELEGRAM row"
    except Exception as exc:  # noqa: BLE001
        err = repr(exc)

    cached = (_load().get("targets") or {}) if _target_cache_enabled() else {}
    if cached.get("telegram"):
        print(f"[vawatch] could not read the Base ({err}); reusing the watch "
              f"list read at {cached.get('at')} "
              f"({len(cached['telegram'])} group(s))", flush=True)
        # Re-derived rather than trusted: a cache written before shared_with
        # existed has no annotation, and a missing one reads as "this group has
        # one row" - the very assumption that wrote YGG notices onto Hacksaw.
        # A cache written by the previous version can still hold a
        # blank-Provider row (F39); it is dropped here for the same reason.
        rows = [dict(r) for r in cached["telegram"]
                if str(r.get("provider") or "").strip()]
        _annotate_shared(rows)
        return {"telegram": rows,
                "teams": list(cached.get("teams") or []),
                "skipped": list(cached.get("skipped") or []),
                "source": "cache", "at": cached.get("at") or "", "error": err}

    print(f"[vawatch] could not read the Base ({err}) and no watch list has "
          f"ever been cached; falling back to the configured single group",
          flush=True)
    single = _chat_title()
    return {"telegram": ([{"provider": _provider(), "group": single}]
                         if single else []),
            "teams": [], "skipped": [], "source": "fallback",
            "at": _now_str(), "error": err}


def targets() -> list:
    """Every Telegram provider group worth watching. See ``watch_list``."""
    return watch_list()["telegram"]


def _log_unwatched(rep: dict) -> None:
    """Say out loud, once per rotation, which Base rows nothing reads."""
    gaps = list(rep.get("teams") or []) + list(rep.get("skipped") or [])
    if rep.get("source") != "base":
        print(f"[vawatch] watch list came from {rep.get('source')} "
              f"(read at {rep.get('at')}): {rep.get('error')}", flush=True)
    if not gaps:
        return
    bits = "; ".join(f"{g['provider'] or '(no provider)'} "
                     f"[{g['group'] or 'no group'}] — {g['why']}" for g in gaps)
    print(f"[vawatch] {len(gaps)} Base row(s) NO watcher reads: {bits}",
          flush=True)


def _as_index(value: Any) -> Optional[int]:
    """A stored cursor as an int, or None when the ledger has no usable one.

    `int(d.get("cursor_idx") or d.get("cursor") or 0)` treated a LEGITIMATE
    cursor_idx of 0 as absent and fell through to the legacy `cursor`, which is
    written as idx+1 - so a watcher sitting on index 0 whose group then left the
    Base resumed at index 1 and the new index-0 group waited a whole extra
    cycle. `is not None` is the test that was meant, and it needs the parse to
    say "no value" separately from "the value is zero".
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def next_target() -> dict:
    """One group per tick, round-robin.

    Reading all ~18 groups on every tick would take ~5 minutes of the single
    Telegram worker and starve every other command. One per 60s tick means a
    full cycle in under 20 minutes, which is far inside the notice period a
    provider gives, and leaves the worker free the rest of the time.

    The cursor is the (provider, group) PAIR - the ledger's own scope, see
    ``_baseline_key`` - resolved through the remembered INDEX first. Neither
    alone is enough:

      * an index alone is only meaningful against the list it was computed
        from, and this list changes length whenever a row is added, blanked or
        excluded - `idx % len(rows)` then silently jumped the rotation
        somewhere else;
      * a NAME alone is not unique. Two Base rows name ONE group today -
        Hacksaw and YGG are both "[SG190- IGO Casinoplus YG/ RG/ HS] CS group"
        - so `names.index(last)` answered with the FIRST of the pair, and once
        the rotation reached the SECOND it recomputed that same row for ever:
        50 of 60 ticks spent on YGG and seven providers not read at all in an
        hour. That is worse than the teleporting index it replaced.

    So the index is trusted only when the pair still sitting at it is the pair
    we served last tick; otherwise that exact pair is searched for; and only
    when it has left the Base entirely do we resume from the remembered index.
    The legacy integer cursor is still written so a rollback keeps rotating.
    """
    rep = watch_list()
    rows = rep["telegram"]
    if not rows:
        return {}
    # The same provider@group string the ledger and the baselines key on, so
    # the rotation and the ledger can never disagree about what "one target" is.
    pairs = [_baseline_key(r.get("group") or "", r.get("provider") or "")
             for r in rows]
    # Load and save under ONE lock: the cursor is read-modify-written, and a
    # concurrent _bump_empty_reads / _remember_targets save between the two
    # would drop the new position and re-serve the group we just served.
    global _CURSOR_MEM
    with _ledger_lock:
        d = _load()
        if _CURSOR_MEM:
            # The last save of the cursor FAILED (full disk, read-only remount,
            # unreadable ledger), so the file still holds the position before
            # it. Resuming from the file served the same group on every 60s
            # tick and never opened the other 17 again (F77); the position this
            # process last served is the true one.
            d.update(_CURSOR_MEM)
        prev_pair = str(d.get("cursor_pair") or "").strip()
        prev_idx = _as_index(d.get("cursor_idx"))
        at: Optional[int] = None          # where the last tick left us, in THIS list
        if prev_idx is not None and 0 <= prev_idx < len(rows):
            if prev_pair:
                if pairs[prev_idx] == prev_pair:
                    at = prev_idx
            elif _norm(d.get("cursor_group") or "") == _norm(
                    rows[prev_idx].get("group") or ""):
                # A ledger written before cursor_pair existed (the tick before
                # this upgrade): the index still points at the remembered group,
                # so it is still trustworthy - and using it rather than a name
                # search is what keeps the duplicate-name case honest.
                at = prev_idx
        if at is None and prev_pair and prev_pair in pairs:
            # The list moved under us: find the very row we served again. Two
            # rows sharing a name are two DIFFERENT pairs, so this cannot pin
            # itself to the first of them the way names.index() did.
            at = pairs.index(prev_pair)
        if at is not None:
            idx = (at + 1) % len(rows)
        else:
            # Never run, or the row we were on has left the Base: resume from
            # the remembered position rather than from the top, so a list that
            # changed under us costs one group, not a whole cycle's worth of
            # starvation.
            hint = prev_idx
            if hint is None:
                hint = _as_index(d.get("cursor"))    # legacy shape, = idx + 1
            idx = (hint if hint is not None else 0) % len(rows)
        row = rows[idx]
        d["cursor_pair"] = pairs[idx]
        d["cursor_group"] = row["group"]     # legacy readers, and a rollback
        d["cursor_idx"] = idx
        d["cursor"] = (idx + 1) % len(rows)  # legacy shape, for a rollback
        _CURSOR_MEM = None if _save(d) else {
            k: d[k] for k in ("cursor_pair", "cursor_group", "cursor_idx", "cursor")}
    if idx == 0:
        # Top of a fresh cycle: one line naming every provider this path will
        # never fill, ~18 minutes apart rather than every tick.
        _log_unwatched(rep)
    return dict(row)


def manual_target(name: str = "") -> dict:
    """The group a manual /vacheck reads. -> row dict, or {"error": str}.

    /vacheck used to queue a sweep with no target, so the worker called
    next_target(): it read whichever provider the rotation reached next - not
    "the announcements group" the reply claimed - and it consumed the timer's
    cursor doing so, so that provider then waited a full extra cycle (F56).
    This resolves the target without touching the cursor:

      * no name  -> the configured VAWATCH_CHAT / VAWATCH_PROVIDER pair, taken
        from the watch list when it is there so the write goes by record_id;
      * a name   -> the one watched row whose Provider (or alias) or Group Name
        is that name. A name that matches several rows - the shared Hacksaw/YGG
        group's title - is refused rather than guessed, because the sweep
        writes to exactly one row.
    """
    rows = watch_list()["telegram"]
    want = _norm(name)
    if not want:
        cfg_g, cfg_p = _norm(_chat_title()), _norm(_provider())
        for r in rows:
            if _norm(r.get("group")) == cfg_g and _norm(r.get("provider")) == cfg_p:
                return dict(r)
        return {"provider": _provider(), "group": _chat_title(), "record_id": "",
                "shared_with": []}
    hits = [r for r in rows
            if want in {_norm(n) for n in _names_for(r.get("provider") or "")}]
    if not hits:
        hits = [r for r in rows if _norm(r.get("group")) == want]
    if len(hits) == 1:
        return dict(hits[0])
    if hits:
        return {"error": f"{name!r} matches {len(hits)} rows ("
                         + ", ".join(r.get("provider") or "?" for r in hits)
                         + ") - name the provider"}
    return {"error": f"no watched TELEGRAM row is named {name!r}. Watched: "
                     + ", ".join(sorted({r.get("provider") or "?" for r in rows}))}


# ---------------------------------------------------------------------------
# Lark API (self-contained)
# ---------------------------------------------------------------------------

APP_TOKEN = (os.getenv("GROUPCHECK_APP_TOKEN")
             or "IjeqbdzCealXp1sDczClVL0sgOb").strip()
TABLE_ID = (os.getenv("GROUPCHECK_TABLE_ID") or "tbl0or9xQZGgnPz2").strip()


def _lark_base() -> str:
    return os.getenv("LARK_OPEN_BASE", "https://open.larksuite.com").rstrip("/")


def _tenant_token() -> str:
    app_id, app_secret = os.getenv("APP_ID"), os.getenv("APP_SECRET")
    if not app_id or not app_secret:
        raise RuntimeError("APP_ID / APP_SECRET not set in environment (.env)")
    r = requests.post(
        f"{_lark_base()}/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret}, timeout=30).json()
    if r.get("code") != 0:
        raise RuntimeError(f"Failed to get tenant token: {r}")
    return r["tenant_access_token"]


def send_text(chat_id: str, text: str) -> dict:
    return requests.post(
        f"{_lark_base()}/open-apis/im/v1/messages",
        headers={"Authorization": f"Bearer {_tenant_token()}",
                 "Content-Type": "application/json"},
        params={"receive_id_type": "chat_id"},
        json={"receive_id": chat_id, "msg_type": "text",
              "content": json.dumps({"text": text})}, timeout=30).json()


def send_card(chat_id: str, card: dict) -> dict:
    return requests.post(
        f"{_lark_base()}/open-apis/im/v1/messages",
        headers={"Authorization": f"Bearer {_tenant_token()}",
                 "Content-Type": "application/json"},
        params={"receive_id_type": "chat_id"},
        json={"receive_id": chat_id, "msg_type": "interactive",
              "content": json.dumps(card, ensure_ascii=False)}, timeout=30).json()


def _ms(dt: datetime) -> int:
    """Aware datetime -> epoch ms, which is what a bitable DateTime field takes.

    NOT ose_Duty._bitable_date_ms: that one is date-only AND timezone-naive
    (datetime.combine(d, min.time()).timestamp() resolves in the process's local
    zone), so on a UTC server it would land 08:00 off. This feature is the first
    in the repo to write a time-of-day to a bitable field, so it does the
    conversion explicitly from an aware datetime.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_tz())
    return int(dt.timestamp() * 1000)


def _provider_cell(fields: dict) -> str:
    """A row's Provider / Games as groupcheck reads it.

    The SAME flattener the watch list used, or the two disagree: this used to
    join rich-text segments with " " and read only "Provider / Games", while
    groupcheck joins non-empty segments with ", " and accepts the column's
    aliases - so [{"text": "Play'n "}, {"text": "GO"}] was watched as
    "Play'n, GO" and then looked up as "Play'n GO", and never matched (F70).
    """
    try:
        import groupcheck as gc

        return gc._field_text(gc._first_field(fields, gc._PROVIDER_FIELDS))
    except Exception:                 # noqa: BLE001
        val = fields.get("Provider / Games")
        if isinstance(val, list):
            val = ", ".join(str(v.get("text", v) if isinstance(v, dict) else v)
                            for v in val)
        return str(val or "")


def find_provider_row(provider: str) -> dict:
    """The Base row for ``provider`` -> {record_id, fields}. {} when absent.

    Only a FALLBACK now: the watcher writes by the record_id the watch list
    read. Two rows with the same name raise instead of returning the first -
    first-match wrote the notice onto whichever row the table listed first
    while the watched one stayed stale, and a loud failure (red card, bounded
    retries) is the safe side of that.
    """
    token = _tenant_token()
    url = (f"{_lark_base()}/open-apis/bitable/v1/apps/{APP_TOKEN}"
           f"/tables/{TABLE_ID}/records")
    headers = {"Authorization": f"Bearer {token}"}
    want = _norm(provider)
    page_token = None
    hits: list = []
    while True:
        params: dict = {"page_size": 200}
        if page_token:
            params["page_token"] = page_token
        data = requests.get(url, headers=headers, params=params, timeout=60).json()
        if data.get("code") != 0:
            raise RuntimeError(f"Base read failed (code {data.get('code')}): "
                               f"{data.get('msg')}")
        d = data.get("data") or {}
        for rec in d.get("items") or []:
            if want and _norm(_provider_cell(rec.get("fields") or {})) == want:
                hits.append({"record_id": rec.get("record_id") or "",
                             "fields": rec.get("fields") or {}})
        if not d.get("has_more"):
            break
        page_token = d.get("page_token")
    if len(hits) > 1:
        raise RuntimeError(f"{len(hits)} Base rows are named {provider!r} - "
                           f"refusing to guess which one to write")
    return hits[0] if hits else {}


def update_row(record_id: str, fields: dict) -> dict:
    """PUT the given fields onto one record.

    PUT rather than POST because it is idempotent: a create that times out after
    the server accepted it would duplicate the row on retry.

    'Maintenance' is a FORMULA (IF(ISBLANK(Start Time),"No","Yes")) and must
    never appear in the payload - Lark rejects writes to computed fields. Writing
    Start Time is what flips it to "Yes".
    """
    if not record_id:
        raise ValueError("record_id is required")
    url = (f"{_lark_base()}/open-apis/bitable/v1/apps/{APP_TOKEN}"
           f"/tables/{TABLE_ID}/records/{record_id}")
    res = requests.put(
        url,
        headers={"Authorization": f"Bearer {_tenant_token()}",
                 "Content-Type": "application/json"},
        json={"fields": fields}, timeout=30).json()
    if res.get("code") != 0:
        raise RuntimeError(f"Base update failed: {res}")
    return res


# ---------------------------------------------------------------------------
# Ledger — so one notice is acted on exactly once
# ---------------------------------------------------------------------------

_ledger_lock = threading.Lock()


# Bumped whenever _key changes shape. Every entry written under an older scheme
# is unmatchable by the new one, so _migrate drops them - and, critically, drops
# the per-group baselines with them. See _migrate.
_KEY_SCHEME = 2
_migration_announced = False


def _key(msg: dict, group: str = "", provider: str = "") -> str:
    """Identity of one message, as seen BY ONE PROVIDER ROW.

    The group is part of the key because the watcher rotates over ~18 chats that
    all share one ledger. Telegram's data-mid is per-CHAT, so ids collide across
    groups, and providers relay each other's notices verbatim so the content
    hash collides too. Without the group, one provider's notice silently marked
    another provider's identical notice as already handled - and that provider's
    row was never filled. That reason still holds; the group stays.

    The PROVIDER is here for the case the group alone cannot express: two Base
    rows can name the SAME group, and two do today - Hacksaw and YGG are both
    "[SG190- IGO Casinoplus YG/ RG/ HS] CS group". Keyed on the group, whichever
    of the two the rotation reached first marked the notice handled and the
    other row was never filled at all. Keyed on provider+group, each row gets
    its own decision about the same message.

    An EDITED bubble keeps its data-mid, so a provider correcting a wrong date
    by editing the message - which they routinely do - produced a key we had
    already handled, and the correction never landed. When the scraper says the
    bubble carries Telegram's "edited" marker (telegramwarm reports it per
    message) the body hash is folded in, so an edit reads as a new message while
    an ordinary re-read of the same text does not. The hash is NOT folded in
    unconditionally: Telegram Web can render the same bubble with a different
    amount of whitespace or a truncated tail, and keying every message on its
    text would re-act on notices we have already written.
    """
    p = _norm(provider)
    g = _norm(group)
    body = " ".join(str(msg.get("text") or "").split())
    mid = str(msg.get("mid") or "").strip()
    if mid:
        edit = ("|ed:" + hashlib.sha1(body.encode("utf-8")).hexdigest()[:8]
                if msg.get("edited") else "")
        return f"{p}@{g}|mid:{mid}{edit}"
    # No id at all: the body hash already distinguishes an edit by itself.
    return f"{p}@{g}|sha:" + hashlib.sha1(body.encode("utf-8")).hexdigest()[:16]


def _migrate(d: dict) -> dict:
    """Bring a ledger written by an older key scheme forward, safely.

    Every key the previous scheme wrote is unmatchable now (the provider and the
    edit marker changed the shape), so those entries can only lie: they say
    "already handled" to nothing, and every message still on screen looks new.
    The dangerous half is ``baselined``, which is keyed on the GROUP alone and
    so survives a key change untouched - all 18 groups would still count as
    baselined, every group's ~8 on-screen messages would look new, and the
    watcher would act on the entire backlog of every group at once, cards and
    Base writes included.

    So the baselines go too. That costs exactly one silent sweep per group - the
    same price switching the feature on has always cost - and a notice posted
    during that one sweep is recorded rather than acted on. Losing one notice
    once is the small side of this trade; re-announcing eighteen groups' history
    is the large one.
    """
    if int(d.get("key_scheme") or 0) == _KEY_SCHEME:
        return d
    global _migration_announced
    if not _migration_announced:
        print(f"[vawatch] ledger key scheme {d.get('key_scheme') or 1} -> "
              f"{_KEY_SCHEME}: {len(d.get('handled') or {})} entry(ies) dropped "
              f"and every group re-baselined (one silent sweep each)", flush=True)
        _migration_announced = True
    d["handled"], d["order"] = {}, []
    d["baselined"], d["empty_reads"] = {}, {}
    # The per-row state (see _scope) is keyed on the same provider@group scope
    # and was computed against the dropped entries; the silent sweep that
    # follows rebuilds it.
    d["scopes"], d["records"] = {}, {}
    d["key_scheme"] = _KEY_SCHEME
    return d


class LedgerError(RuntimeError):
    """The ledger could not be read or saved, so the sweep did not act.

    Raised by handle_messages BEFORE any write or card. The ledger is the only
    thing that stops a notice from being written and carded again on every
    tick, so a sweep that cannot trust it - or cannot record what it does - must
    not act at all (F76, F77).
    """


# The last save's error, "" after a good one. Read by handle_messages to say
# WHY it refused to act, and by the selftest.
_SAVE_ERROR = ""

# Where next_target last pointed when it could not SAVE the cursor (F77). None
# while saves succeed, so the file stays the one source of truth.
_CURSOR_MEM: Optional[dict] = None


def _fresh_ledger() -> dict:
    return {"handled": {}, "order": [], "key_scheme": _KEY_SCHEME}


def _load() -> dict:
    """The ledger, or a fresh one - but never a fresh one that can be SAVED
    over a real ledger it failed to read.

    Every error used to read as "no ledger" (`except Exception: pass`), and the
    caller then saved that empty dict: one EMFILE in the Playwright process, or
    one undecodable byte, and every group's baseline, the rotation cursor and
    the target cache were gone - while the same sweep, whose later
    is_baselined() read succeeded, acted on an empty ledger (F76). Now:

      * the file does not exist   -> a fresh ledger (first run, or the
        documented "delete vawatch.json to re-baseline");
      * it cannot be READ (EMFILE, EIO, EACCES) -> a fresh ledger carrying
        ``_unreadable``, which _save refuses to write and handle_messages
        refuses to act on - the next tick simply tries again;
      * it is not JSON / not an object -> moved aside to vawatch.json.bad, so
        the evidence survives, and a fresh ledger (every group re-baselines
        with one silent sweep, the same cost as deleting it by hand).
    """
    try:
        with open(LEDGER_PATH, encoding="utf-8") as fh:
            raw = fh.read()
    except FileNotFoundError:
        return _fresh_ledger()
    except Exception as err:          # noqa: BLE001
        print(f"[vawatch] could not READ the ledger ({err!r}) - acting on "
              f"nothing and saving nothing this tick", flush=True)
        return dict(_fresh_ledger(), _unreadable=repr(err))
    try:
        d = json.loads(raw)
        if not isinstance(d, dict):
            raise ValueError(f"top level is a {type(d).__name__}, not an object")
    except Exception as err:          # noqa: BLE001
        bad = LEDGER_PATH.with_name(LEDGER_PATH.name + ".bad")
        try:
            LEDGER_PATH.replace(bad)
        except FileNotFoundError:
            pass                      # another reader already moved it aside
        except Exception as err2:     # noqa: BLE001
            print(f"[vawatch] the ledger is corrupt ({err!r}) and could not be "
                  f"moved aside ({err2!r}) - acting on nothing", flush=True)
            return dict(_fresh_ledger(),
                        _unreadable=f"corrupt ({err!r}), not moved ({err2!r})")
        print(f"[vawatch] the ledger was corrupt ({err!r}); kept as {bad.name} "
              f"and starting fresh - every group re-baselines with one silent "
              f"sweep", flush=True)
        return _fresh_ledger()
    d.setdefault("handled", {})
    d.setdefault("order", [])
    return _migrate(d)


# How many keys the ledger keeps in total, and how many of each provider@group
# scope's most recently SEEN keys are never evicted whatever the total says.
_LEDGER_CAP = 2000
_SCOPE_RE = re.compile(r"^(.*)\|(?:mid|sha):")


def _scope_of(key: str) -> str:
    m = _SCOPE_RE.match(key or "")
    return m.group(1) if m else ""


def _keep_per_scope() -> int:
    # Room for the widest read this path ever makes (the gap re-read, 50) plus
    # the edit keys that sit beside their bubbles, with margin.
    return max(64, 4 * _read_count(), 2 * _gap_read_cap())


def _prune(d: dict) -> None:
    """Bound the ledger by LAST-SEEN order, never evicting a scope's newest keys.

    ``order`` used to be first-insertion order trimmed to its last 2000, so a
    quiet group's messages - still on screen, long since decided - were evicted
    by 2000 bubbles of chatter in the busy groups (about 118 rotations in the
    audit's simulation) and came back as NEW: an old notice re-written over a
    row someone had corrected, needs-human cards re-sent, and a backlog that
    cold start had suppressed acted on (F40). ``order`` is now least-recently-
    SEEN first (handle_messages touches every key it reads, see _touch), so
    what gets evicted is what has not been on any screen for longest; and each
    scope keeps its newest _keep_per_scope() keys regardless, so a group that
    was not visited for a while (a failing title, a long pause) does not lose
    its on-screen decisions to the others' traffic.
    """
    order = [k for k in d.get("order") or [] if k in d["handled"]]
    if len(order) > _LEDGER_CAP:
        keep_n = _keep_per_scope()
        scopes_now = d.get("scopes") if isinstance(d.get("scopes"), dict) else {}
        # Only rows read in the last 30 days hold on to their floor; the scope a
        # renamed or removed row left behind must not pin its keys for ever.
        cutoff = (_now_dt() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        per: dict = {}
        protected = set()
        for k in reversed(order):
            s = _scope_of(k)
            live = s not in scopes_now or str(scopes_now[s].get("seen") or "") >= cutoff
            if live and per.get(s, 0) < keep_n:
                protected.add(k)
                per[s] = per.get(s, 0) + 1
        excess = len(order) - _LEDGER_CAP
        kept = []
        for k in order:
            if excess > 0 and k not in protected:
                excess -= 1
                continue
            kept.append(k)
        order = kept
    d["order"] = order
    keep = set(order)
    d["handled"] = {k: v for k, v in d["handled"].items() if k in keep}
    scopes = d.get("scopes")
    if isinstance(scopes, dict) and len(scopes) > 400:
        # A renamed group leaves its old scope behind; keep the 400 most
        # recently read, which is ~20x the Base.
        for s in sorted(scopes, key=lambda s: str(scopes[s].get("seen") or ""))[:-400]:
            scopes.pop(s, None)


def _save(d: dict) -> bool:
    """Write the ledger atomically. -> True when it is on disk.

    It used to swallow every error and return nothing, so a full disk or a
    read-only remount looked exactly like success: each tick re-wrote and
    re-carded the same notice, and the cursor never advanced (F77). Callers
    that are about to act now check the answer.
    """
    global _SAVE_ERROR
    if d.get("_unreadable"):
        # A stand-in for a ledger we could not read. Writing it would replace
        # the real one with nothing (F76).
        _SAVE_ERROR = f"the ledger could not be read ({d['_unreadable']})"
        return False
    d.setdefault("order", [])
    d.setdefault("handled", {})
    d.setdefault("key_scheme", _KEY_SCHEME)
    _prune(d)
    try:
        tmp = LEDGER_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(d, fh, ensure_ascii=False, indent=1)
        tmp.replace(LEDGER_PATH)
    except Exception as err:          # noqa: BLE001
        _SAVE_ERROR = repr(err)
        print(f"[vawatch] could not save the ledger: {err!r}", flush=True)
        return False
    _SAVE_ERROR = ""
    return True


def _touch(d: dict, key: str) -> None:
    """Move a key to the most-recently-seen end of ``order`` (see _prune)."""
    if key in d["handled"]:
        try:
            d["order"].remove(key)
        except ValueError:
            pass
        d["order"].append(key)


def _remember_targets(rep: dict) -> None:
    """Keep the last GOOD watch list, so a 5xx cannot shrink the rotation.

    In the ledger because the ledger is already the one thing on this path that
    survives a restart, and a watch list that only lived in memory would be gone
    at exactly the moment it is needed - a service restarted during a Lark
    outage.
    """
    if not _target_cache_enabled():
        return
    with _ledger_lock:
        d = _load()
        d["targets"] = {"at": rep.get("at") or _now_str(),
                        "source": rep.get("source") or "base",
                        "telegram": list(rep.get("telegram") or []),
                        "teams": list(rep.get("teams") or []),
                        "skipped": list(rep.get("skipped") or [])}
        _save(d)


def _unwatched_from_cache(d: dict) -> dict:
    """The rows nothing reads, from the ledger - no Base call, no network."""
    cached = d.get("targets") or {}
    return {"teams": list(cached.get("teams") or []),
            "skipped": list(cached.get("skipped") or []),
            "at": cached.get("at") or ""}


# An outcome in here is NOT "handled": the message stays eligible for another
# look, up to the cap its outcome names. Everything else is final on the first
# mark. This is the whole fix for "a failed write is ledgered as handled": the
# ledger keeps recording every attempt (the operator needs the history) and only
# the SUCCESSFUL outcomes suppress a retry.
_RETRY_CAPS = {
    "write-failed": _write_attempts,
    "clear-failed": _write_attempts,
    "card-failed": _write_attempts,
    "unparsed": _reparse_attempts,
    # An exception while handling ONE message (F82). Deterministic code fails
    # the same way every time, so it gets the reparse cap - a couple of retries
    # in case the fault was transient, then it stops being looked at.
    "error": _reparse_attempts,
}


def _seen(d: dict, key: str) -> bool:
    """Is this message finished with?

    Not "is it in the ledger" any more. A key whose last outcome was a FAILED
    write or an unreadable window is in the ledger precisely so we know how many
    times it has been tried, and it stays eligible until it hits its cap.
    """
    ent = d["handled"].get(key)
    if ent is None:
        return False
    if not isinstance(ent, dict):
        return True                     # a shape we did not write: leave it alone
    outcome = str(ent.get("outcome") or "")
    cap = _RETRY_CAPS.get(outcome)
    if cap is not None and int(ent.get("attempts") or 1) < cap():
        return False
    return True


def _mark(d: dict, key: str, outcome: str, extra: Optional[dict] = None) -> None:
    """Record what happened to this message, counting repeats of the same fate.

    ``attempts`` only climbs while the outcome stays the same, so a write that
    failed twice and then succeeded is recorded as one success, and a notice
    that becomes readable on the third sweep starts its next life at 1.
    """
    prev = d["handled"].get(key)
    prev = prev if isinstance(prev, dict) else {}
    attempts = (int(prev.get("attempts") or 0) + 1
                if prev.get("outcome") == outcome else 1)
    d["handled"][key] = {"outcome": outcome, "at": _now_str(),
                         "attempts": attempts, **(extra or {})}
    _touch(d, key)                  # a decision is also a sighting (see _prune)


def ledger_is_cold() -> bool:
    """Deprecated: file-existence is NOT a usable cold-start test any more.

    Kept only so an external caller does not break. ``next_target`` writes the
    ledger to persist its rotation cursor, so by the time a sweep reaches
    handle_messages the file always exists - which silently disabled the
    backlog guard for EVERY group, including the first. Use ``is_baselined``.
    """
    return not LEDGER_PATH.exists()


def _baseline_key(group: str, provider: str = "") -> str:
    """The same provider@group scope the ledger keys use. See ``_key``.

    It has to be the same scope, or the two disagree in exactly the case this
    release fixes: two Base rows sharing one group. Baselining per GROUP while
    keying per PROVIDER meant the first row to reach a shared group spent the
    one silent sweep, and the second row then found a backlog of messages that
    were new to IT and filled its row from history - the very thing the cold
    start exists to prevent.
    """
    return f"{_norm(provider)}@{_norm(group)}"


def is_baselined(group: str, provider: str = "") -> bool:
    """Has this provider's backlog in this group already been recorded?

    Per GROUP, not per file: the watcher rotates over ~18 groups, so a single
    global flag meant group #2 onwards were never baselined at all and would act
    on whatever old notices happened to be in them - up to 18 provider rows
    filled from history the moment the watcher was switched on. And now per
    PROVIDER as well, for the shared-group case above.
    """
    key = _baseline_key(group, provider)
    return bool((_load().get("baselined") or {}).get(key))


def _bump_empty_reads(group: str, provider: str = "") -> int:
    """Count consecutive reads of this group that returned nothing."""
    key = _baseline_key(group, provider)
    with _ledger_lock:
        d = _load()
        n = int((d.setdefault("empty_reads", {})).get(key) or 0) + 1
        d["empty_reads"][key] = n
        _save(d)
    return n


def mark_baselined(group: str, provider: str = "") -> None:
    key = _baseline_key(group, provider)
    with _ledger_lock:
        d = _load()
        d.setdefault("baselined", {})[key] = _now_str()
        (d.setdefault("empty_reads", {})).pop(key, None)
        _save(d)


# ---------------------------------------------------------------------------
# Per-row state: which message owns the row, and where history begins
# ---------------------------------------------------------------------------
#
# The handled-keys ledger answers "has THIS message been decided?". It could not
# answer the question every wrong write in F34/F35/F36/F41/G2.5 turned on: "is
# this message OLDER than the one whose window the row already holds?". A retry
# of a failed older notice, an older bubble that rendered late, an older bubble
# /vacheck force walked past, and an edit to a superseded notice all overwrote a
# newer reschedule, because nothing compared them.
#
# Telegram's data-mid increases monotonically within one chat, so it IS that
# order. Each provider@group scope (the ledger's own scope, _baseline_key)
# keeps, in d["scopes"][scope]:
#
#   owner    the message whose window this watcher last WROTE on the row
#            {mid, kind fill|clear, start, end (ISO), resched, key, at}.
#            A fill or clear from a LOWER mid is never written (see
#            handle_messages); an edit of the owner itself is judged against
#            what it replaced.
#   floor    the highest mid in the group's cold-start read. Anything at or
#            below it that shows up unseen later (a lazily rendered bubble, a
#            raised VAWATCH_READ_COUNT, an evicted key) is backlog by
#            definition and is recorded, not acted on (F35, F40).
#   last     the highest mid in the previous read - the gap detector (F57).
#   pending  a later, separate outage deferred while the row still holds an
#            earlier future window; written once that window has ended (G3.1).
#
# ``d["records"]`` maps a Base record_id to the scope that last read it, so a
# row whose Group Name or Provider was edited is recognised as the SAME row
# (F69). Mids are only ever compared within one scope.

_MID_RE = re.compile(r"^\d+(?:\.\d+)?$")


def _mid_num(mid: Any) -> Optional[float]:
    """data-mid as a number, or None when it is not one (a sha-keyed bubble).
    Every order rule is skipped when either side is None: no order, no guess."""
    s = str(mid if mid is not None else "").strip()
    return float(s) if _MID_RE.match(s) else None


def _body_hash(text: str) -> str:
    """The same hash _key folds into an edited bubble's key."""
    body = " ".join(str(text or "").split())
    return hashlib.sha1(body.encode("utf-8")).hexdigest()[:8]


def _scope(d: dict, bk: str) -> dict:
    return d.setdefault("scopes", {}).setdefault(bk, {})


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if isinstance(dt, datetime) else None


def _from_iso(s: Any) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(str(s)) if s else None
    except ValueError:
        return None


def _owner_window(owner: Optional[dict]) -> tuple:
    if not owner:
        return None, None
    return _from_iso(owner.get("start")), _from_iso(owner.get("end"))


def _fmt_win(start: Optional[datetime], end: Optional[datetime]) -> str:
    if not (start and end):
        return "(window unknown)"
    same = start.date() == end.date()
    return f"{start:%Y-%m-%d %H:%M} → {end:%H:%M}" if same else \
        f"{start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M}"


def _same_window(verdict: dict, owner: Optional[dict]) -> bool:
    s, e = _owner_window(owner)
    vs, ve = verdict.get("start"), verdict.get("end")
    return bool(s and e and vs and ve and _ms(s) == _ms(vs) and _ms(e) == _ms(ve))


def _owner_live(owner: Optional[dict], now: datetime) -> bool:
    """Does the row still hold a window that has not ended?"""
    _s, e = _owner_window(owner)
    return bool(owner and owner.get("kind") == "fill" and e and e > now)


def _near(s1: Optional[datetime], e1: Optional[datetime],
          s2: Optional[datetime], e2: Optional[datetime]) -> bool:
    """Do two windows overlap or sit within 24h of each other - the same
    "two separate outages" line noticeparse draws inside one notice?"""
    if not (s1 and e1 and s2 and e2):
        return False
    day = timedelta(hours=24)
    return not (s2 >= e1 + day or e2 + day <= s1)


def _set_owner(sc: dict, *, mid: str, key: str, verdict: dict, kind: str) -> None:
    sc["owner"] = {"mid": str(mid or ""), "kind": kind, "key": key,
                   "start": _iso(verdict.get("start")) if kind == "fill" else None,
                   "end": _iso(verdict.get("end")) if kind == "fill" else None,
                   "resched": bool(verdict.get("reschedule")), "at": _now_str()}
    # A deferred outage stays deferred when a newer message merely restates the
    # row's current window (a reminder). It goes when the newer message is
    # about THAT outage (within a day of it) or says there is no maintenance.
    sc["pending"] = [] if kind == "clear" else [
        p for p in sc.get("pending") or []
        if not _near(_from_iso(p.get("start")), _from_iso(p.get("end")),
                     verdict.get("start"), verdict.get("end"))]


_KEY_MID_RE = re.compile(r"\|mid:([^|]+)")


def _scope_entries(d: dict, bk: str):
    """(key, entry, mid-number) for every handled key of one scope."""
    pre = bk + "|"
    for k, v in (d.get("handled") or {}).items():
        if k.startswith(pre) and isinstance(v, dict):
            m = _KEY_MID_RE.search(k[len(bk):])
            yield k, v, (_mid_num(m.group(1)) if m else None)


def _init_scope(d: dict, sc: dict, bk: str) -> None:
    """Derive floor/owner once for a scope baselined before they existed.

    Without this the first sweep after deploy would have no floor and no owner
    for any group - exactly the moment an older superseded notice, a retry or a
    raised read count is most likely to be in the ledger. The ledger already
    knows both: the cold-start marks ARE the baseline read, and the newest
    'filled' key IS the message whose window the row holds (its window is not
    recoverable from the old display string, so it is left unknown - which only
    makes the order rule card instead of silently skipping).
    """
    if sc.get("init"):
        return
    sc["init"] = 1
    cold = [n for _k, v, n in _scope_entries(d, bk)
            if n is not None and v.get("outcome") == "cold-start"]
    if cold and sc.get("floor") is None:
        sc["floor"] = max(cold)
    if not sc.get("owner"):
        filled = [(n, k, v) for k, v, n in _scope_entries(d, bk)
                  if n is not None and v.get("outcome") in ("filled", "cleared")]
        if filled:
            n, k, v = max(filled, key=lambda t: t[0])
            sc["owner"] = {"mid": (f"{n:.6f}".rstrip("0").rstrip(".")),
                           "kind": "fill" if v.get("outcome") == "filled" else "clear",
                           "key": k, "start": None, "end": None,
                           "resched": "(rescheduled)" in str(v.get("window") or ""),
                           "at": v.get("at") or "", "legacy": 1}


def _mid_versions(d: dict, base_key: str) -> list:
    """Every ledger entry for one bubble: its unedited key and each edit's.

    An edit is judged against what was handled BEFORE it, and that can be any
    earlier version - a bubble first read already edited has no unedited entry.
    """
    out = []
    ent = d["handled"].get(base_key)
    if isinstance(ent, dict):
        out.append(ent)
    pre = base_key + "|ed:"
    out += [v for k, v in d["handled"].items()
            if k.startswith(pre) and isinstance(v, dict)]
    return out


def gap_read_count(messages: list, *, provider: str = "", group: str = "",
                   asked: int = 0) -> int:
    """How many bubbles to re-read so this read reaches the previous one. 0 = none.

    A read "reaches" the previous visit when at least one of its bubbles is at
    or below the newest mid that visit saw. When every bubble is newer, the
    bubbles in between were never read - possibly a notice (F57). This cannot
    prove a loss (exactly N new bubbles and nothing missing looks the same), so
    it only asks for a wider read, capped at _gap_read_cap(); handle_messages
    then decides, from the read it actually got, whether to say so.
    Read-only: no save, no network.
    """
    if not _order_check():
        return 0
    try:
        d = _load()
        sc = (d.get("scopes") or {}).get(_baseline_key(group, provider)) or {}
    except Exception:                 # noqa: BLE001
        return 0
    last = sc.get("last")
    msgs = list(messages or [])
    nums = [n for n in (_mid_num(m.get("mid")) for m in msgs) if n is not None]
    cap = _gap_read_cap()
    if (last is None or not nums or min(nums) <= float(last)
            or len(msgs) < max(1, int(asked or 0)) or cap <= int(asked or 0)):
        # A read SHORTER than asked is the whole rendered list: nothing was
        # pushed out of it, so there is no window to widen.
        return 0
    return cap


# A CANCELLATION, as opposed to a completion. noticeparse gives both one verdict
# (ignore, "maintenance completed / cancelled notice") because neither may fill
# the row - but only one of them leaves the row WRONG: "the maintenance on 26/09
# has been cancelled" left the cancelled window on the sheet with nobody told,
# the sibling of a deleted notice (G2.4).
_CANCEL_REASON_RE = re.compile(r"cancel", re.I)
_CANCEL_TEXT_RE = re.compile(
    r"(?i:\bcancel\w*|\bcalled\s+off\b|\bwill\s+not\s+(?:be\s+)?(?:proceed|go\s+ahead|take\s+place))"
    r"|取消|撤销|撤銷|不再进行|不再進行")


# An EDIT that retracts a notice. Used on edits only, and only when the words
# were not in the text first handled: noticeparse still reads "[VOID] <notice>",
# "【作废】<notice>" and "(Postponed - please see the latest notice)" as a fill of
# the voided window, and an edit that adds them re-announced it as a fresh
# "Scheduled maintenance" (G2.5, F41). Greppable on purpose.
_EDIT_RETRACT_RE = re.compile(
    r"(?i:\bvoid(?:ed)?\b|\bdisregard\w*|\bignore\b|\bwrong\s+(?:group|chat"
    r"|channel|message|post|notice)\b|\bpostpon\w*|\bcancel\w*|\bcalled\s+off\b"
    r"|\bwithdr[ae]wn?\b|\brevoked?\b|\bretract\w*|\binvalid\b|\bobsolete\b"
    r"|\bsuperseded\b|\boutdated\b|\bno\s+longer\b"
    r"|\bsee\s+(?:the\s+)?(?:latest|updated|new|next)\b)"
    r"|作废|作廢|忽略|无效|無效|取消|撤回|撤销|撤銷|延期|延后|延後|改期"
    r"|发错|發錯|错发|錯發|以最新")


# ---------------------------------------------------------------------------
# The card
# ---------------------------------------------------------------------------

def build_card(group: str, provider: str, text: str, verdict: dict,
               wrote: Optional[str], note: str = "") -> dict:
    start, end = verdict.get("start"), verdict.get("end")
    body = text if len(text) <= 3000 else text[:3000] + "\n…"
    elements: list = [
        {"tag": "div", "text": {"tag": "lark_md", "content":
            (f"**Detected scheduled maintenance for {group}**\n"
             f"**Provider:** {provider}\n"
             f"**Start:** {start:%Y-%m-%d %H:%M} ({start:%Z})\n"
             f"**End:** {end:%Y-%m-%d %H:%M} ({end:%Z})"
             if start and end else
             f"**Detected scheduled maintenance for {group}**")
            + (f"\n{note}" if note else "")}},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": body}},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content":
            (f"_Base row updated · {wrote}_" if wrote
             else "_⚠️ the Base row was NOT updated — see the service log_")
            + f"\n_Read-only in Telegram · {_now_str()}_"}},
    ]
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "orange" if wrote else "red",
                   "title": {"tag": "plain_text",
                             "content": f"\U0001F6E0️ Scheduled maintenance · "
                                        f"{provider}"[:100]}},
        "body": {"elements": elements},
    }


def build_text(group: str, provider: str, text: str, verdict: dict,
               wrote: Optional[str], note: str = "") -> str:
    s, e = verdict.get("start"), verdict.get("end")
    head = f"🛠️ Detected scheduled maintenance for {group}\nProvider: {provider}"
    if s and e:
        head += f"\nStart: {s:%Y-%m-%d %H:%M}\nEnd:   {e:%Y-%m-%d %H:%M}"
    if note:
        head += "\n" + note.replace("**", "")
    head += (f"\nBase row updated · {wrote}" if wrote
             else "\n⚠️ the Base row was NOT updated — see the service log")
    return head + "\n\n" + (text if len(text) <= 2500 else text[:2500] + "\n…")


def build_note_card(title: str, template: str, lines: list, text: str) -> dict:
    """A card for the verdicts that are not a plain fill.

    Same schema-2.0 / div+hr vocabulary as build_card - 2.0 rejects
    {"tag": "note"} with ErrCode 200861, which is why there is no note element
    anywhere in this repo.
    """
    body = text if len(text) <= 3000 else text[:3000] + "\n…"
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": template,
                   "title": {"tag": "plain_text", "content": title[:100]}},
        "body": {"elements": [
            {"tag": "div", "text": {"tag": "lark_md",
                                    "content": "\n".join(lines)}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": body}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md",
                                    "content": f"_Read-only in Telegram · "
                                               f"{_now_str()}_"}},
        ]},
    }


def _post_note(title: str, template: str, lines: list, text: str) -> dict:
    """Send one note card, falling back to plain text.

    -> {"carded": bool, "error": str, "disabled": bool}. ``disabled`` marks the
    one non-failure: the card was switched off rather than rejected, which the
    ledger must not treat as something to retry.
    """
    out = {"carded": False, "error": "", "disabled": False}
    chat = _card_chat_id()
    try:
        resp = send_card(chat, build_note_card(title, template, lines, text))
        if isinstance(resp, dict) and resp.get("code") == 0:
            out["carded"] = True
            return out
        print(f"[vawatch] card rejected: {resp!r}", flush=True)
        body = text if len(text) <= 2500 else text[:2500] + "\n…"
        resp = send_text(chat, title + "\n" + "\n".join(lines) + "\n\n" + body)
        out["carded"] = isinstance(resp, dict) and resp.get("code") == 0
        if not out["carded"]:
            out["error"] = f"card and text both rejected: {resp!r}"
    except Exception as err:          # noqa: BLE001
        out["error"] = repr(err)
        print(f"[vawatch] note send failed: {err!r}", flush=True)
    return out


def card_needs_human(group: str, provider: str, text: str, verdict: dict,
                     *, extra: str = "", tail: Optional[str] = None) -> dict:
    """Tell the Laboratory group that a notice needs a person. -> {carded, error}.

    This exists because the three non-fill verdicts used to be one catch-all
    `ignore` and nothing was said about any of them. Each one leaves the row
    WRONG, not merely unfilled: a reschedule whose new window could not be read
    leaves the superseded window sitting there looking current, and "the answer
    is in the quoted message" is an answer we simply cannot reach from a
    scraper that reads bubbles one at a time. A card costs one message and is
    the only thing standing between those cases and silence.
    """
    if not _needs_human_card():
        # Switched off on purpose, so this is not a failure to retry.
        return {"carded": False, "disabled": True,
                "error": "VAWATCH_NEEDS_HUMAN_CARD=0"}
    lines = [f"**Group:** {group or '(unknown)'}",
             f"**Provider:** {provider or '(blank in the Base)'}",
             f"**Verdict:** {verdict.get('action') or '?'} — "
             f"{verdict.get('reason') or 'no reason given'}"]
    if verdict.get("reschedule"):
        lines.append("**This is a RESCHEDULE** — the row still shows the "
                     "superseded window.")
    if extra:
        lines.append(extra)
    # ``tail`` replaces the closing instruction for the one card whose honest
    # ending is not "fill the row by hand" - a deferred second outage, which the
    # watcher WILL write later (see handle_messages, G3.1).
    lines.append(tail if tail is not None else
                 "**Nothing was written to the Base** — please fill the row by "
                 "hand.")
    return _post_note(f"❓ Maintenance notice needs a human · "
                      f"{provider or group}", "red", lines, text)


def card_deleted_notice(group: str, provider: str, owner: dict,
                        around: tuple) -> dict:
    """The message that set the row's window is gone from the group (G2.4).

    Only a deletion can make a mid vanish from BETWEEN two mids that are both
    still on screen (scrolling away removes the oldest first). A provider that
    deletes a wrong or called-off notice usually posts nothing else, and the row
    kept the window with nobody told. Nothing is cleared here - a deletion is
    also how a notice gets reposted corrected - so a person decides.
    """
    if not _needs_human_card():
        return {"carded": False, "disabled": True,
                "error": "VAWATCH_NEEDS_HUMAN_CARD=0"}
    s, e = _owner_window(owner)
    return _post_note(
        f"❓ Maintenance notice deleted · {provider or group}", "red",
        [f"**Group:** {group or '(unknown)'}",
         f"**Provider:** {provider or '(blank in the Base)'}",
         f"**The notice that set {_fmt_win(s, e)} on this row is no longer in "
         f"the group** — it was message {owner.get('mid')}, between messages "
         f"{around[0]:g} and {around[1]:g}, which are both still there.",
         "**The row still shows that window.** If the maintenance was called "
         "off, clear the row by hand; if it was reposted, the new notice fills "
         "the row on its own."],
        "(The deleted notice's text is still in the row's Remark.)")


# At most one "possible missed messages" card per group in this long.
_GAP_CARD_EVERY = timedelta(hours=6)


def card_gap(group: str, provider: str, gap: dict) -> dict:
    """Even the wider re-read does not reach the previous visit (F57)."""
    return _post_note(
        f"⚠️ Possibly missed messages · {provider or group}", "orange",
        [f"**Group:** {group or '(unknown)'}",
         f"**Provider:** {provider or '(blank in the Base)'}",
         f"The oldest of the {gap.get('read')} bubble(s) read (message "
         f"{gap.get('oldest'):g}) is newer than the newest one seen on the last "
         f"visit (message {gap.get('last'):g}), so messages in between were "
         f"never read - a maintenance notice may be among them.",
         "**Please scroll the group back and check.** Nothing was written for "
         "the unread messages."],
        "(No message text: this note is about messages that were never read.)")


# ---------------------------------------------------------------------------
# The detector
# ---------------------------------------------------------------------------

def _resolve_record(provider: str, record_id: str) -> str:
    """The record to write: the watched one when known, else a name lookup."""
    if record_id:
        return record_id
    row = find_provider_row(provider)
    if not row:
        raise RuntimeError(f"no row in the Base for provider {provider!r}")
    return row["record_id"]


def act_on_notice(text: str, verdict: dict, *, provider: str = "",
                  group: str = "", record_id: str = "", note: str = "") -> dict:
    """Write the provider's row, then card the Laboratory group.

    -> {"wrote", "error", "record_id", "carded"}. ``carded`` is whether the
    Laboratory group actually received the card (or its text fallback), so a
    /vacheck reply can stop claiming a card it cannot see. ``note`` is one more
    line on that card - what this write replaced, or why it is late.
    """
    # NO fallback when the caller passed a group. A Base row with a blank
    # Provider used to fall through to _provider() (default "VA"), filing
    # another group's notice onto the VA row.
    if group and not (provider or "").strip():
        out = {"wrote": None, "record_id": "", "carded": False,
               "error": f"the Base row for {group!r} has no Provider - refusing "
                        f"to guess which row to write"}
        print(f"[vawatch] {out['error']}", flush=True)
        return out
    provider = provider or _provider()
    group = group or _chat_title()
    out: dict = {"wrote": None, "error": "", "record_id": "", "carded": False}
    try:
        out["record_id"] = _resolve_record(provider, record_id)
        update_row(out["record_id"], {
            "Start Time": _ms(verdict["start"]),
            "End Time": _ms(verdict["end"]),
            "Remark": text,
            "Last Check": _ms(datetime.now(_tz())),
            # Reference is deliberately absent: leaving it out leaves it alone.
        })
        out["wrote"] = (f"{verdict['start']:%Y-%m-%d %H:%M} → "
                        f"{verdict['end']:%H:%M}"
                        + (" (rescheduled)" if verdict.get("reschedule") else ""))
    except Exception as err:          # noqa: BLE001
        # A failed write must never look like an ignored message, so the card
        # still goes out and says so.
        out["error"] = repr(err)
        print(f"[vawatch] Base update FAILED: {err!r}", flush=True)

    chat = _card_chat_id()
    try:
        resp = send_card(chat, build_card(group, provider, text, verdict,
                                          out["wrote"], note))
        out["carded"] = isinstance(resp, dict) and resp.get("code") == 0
        if not out["carded"]:
            print(f"[vawatch] card rejected: {resp!r}", flush=True)
            resp = send_text(chat, build_text(group, provider, text, verdict,
                                              out["wrote"], note))
            out["carded"] = isinstance(resp, dict) and resp.get("code") == 0
    except Exception as err:          # noqa: BLE001
        print(f"[vawatch] card send failed: {err!r}", flush=True)
    return out


def act_on_clear(text: str, verdict: dict, *, provider: str = "",
                 group: str = "", record_id: str = "") -> dict:
    """The provider says there is no maintenance: blank the row.

    -> {"wrote", "error", "record_id", "skipped", "carded"}.

    Start Time and End Time are sent EMPTY, which is how a bitable DateTime
    cell is cleared, and Remark becomes "No maintenance" - the same words
    /provideraskmaintenance writes for the same answer, so the sheet reads the
    same however the answer arrived. 'Maintenance' itself is a FORMULA
    (IF(ISBLANK(Start Time),"No","Yes")) and must never appear in the payload;
    emptying Start Time is what flips it back to "No".

    Behind VAWATCH_CLEAR_ENABLED and OFF by default, for one specific reason:
    if this tenant's field configuration rejects an empty DateTime the whole PUT
    is rejected, so the row would not even get its Remark, and this path runs
    unattended. With the flag off the notice is carded for a human instead -
    which is still strictly better than the silent drop it used to get.
    """
    if group and not (provider or "").strip():
        # Same refusal as act_on_notice: a blank Provider used to fall through
        # to _provider() (default "VA") and blank the VA row on another group's
        # say-so.
        out = {"wrote": None, "record_id": "", "skipped": "", "carded": False,
               "error": f"the Base row for {group!r} has no Provider - refusing "
                        f"to guess which row to clear"}
        print(f"[vawatch] {out['error']}", flush=True)
        return out
    if not _clear_enabled():
        return {"wrote": None, "record_id": "", "error": "", "carded": False,
                "skipped": "VAWATCH_CLEAR_ENABLED is not set"}
    provider = provider or _provider()
    group = group or _chat_title()
    out: dict = {"wrote": None, "error": "", "record_id": "", "skipped": "",
                 "carded": False}
    try:
        out["record_id"] = _resolve_record(provider, record_id)
        update_row(out["record_id"], {
            "Start Time": None,
            "End Time": None,
            "Remark": "No maintenance",
            "Last Check": _ms(datetime.now(_tz())),
            # Reference is deliberately absent: leaving it out leaves it alone.
        })
        out["wrote"] = "cleared (No maintenance)"
    except Exception as err:          # noqa: BLE001
        out["error"] = repr(err)
        print(f"[vawatch] Base clear FAILED: {err!r}", flush=True)

    sent = _post_note(f"\U0001F9F9 No maintenance · {provider}",
                      "green" if out["wrote"] else "red",
                      [f"**Group:** {group}", f"**Provider:** {provider}",
                       (f"**Start Time / End Time cleared, Remark = "
                        f"“No maintenance”**" if out["wrote"] else
                        f"**⚠️ the Base row was NOT cleared** — "
                        f"{out['error']}")], text)
    out["carded"] = bool(sent.get("carded"))
    return out


# Which `ignore` reasons are worth a second look. noticeparse separates "no
# scheduled-maintenance wording" (this message is not about maintenance - drop
# it, cheaply, once) from "maintenance wording but no parseable date/time
# window" (it IS a notice and we failed to read it - which is exactly what a
# half-rendered bubble looks like). Matching on the reason text rather than on
# a new return key keeps classify()'s contract unchanged; an unfamiliar reason
# falls to the non-retrying side on purpose, because that is the side that
# cannot cost anything.
_UNPARSED_REASON_RE = re.compile(r"no\s+(?:parse?able|readable)|"
                                 r"wording\s+but\s+no", re.I)


def _is_unparsed(reason: str) -> bool:
    return bool(_UNPARSED_REASON_RE.search(str(reason or "")))


def _note_outcome(sent: dict) -> str:
    """A card nobody received is worth another go; a card switched off is not."""
    if sent.get("carded") or sent.get("disabled"):
        return "needs-human"
    return "card-failed"


# ---------------------------------------------------------------------------
# Attribution - WHOSE maintenance is this?
# ---------------------------------------------------------------------------
#
# noticeparse answers "is this a maintenance window, and when". It cannot say
# whose window it is, and the group was the only proxy for that. The audit broke
# the proxy four ways, each of which parsed as a perfect window and overwrote a
# provider's real one on the shared sheet:
#
#   F37   a group two Base rows share (Hacksaw + YGG): every notice went onto
#         BOTH rows, so a Yggdrasil-only window landed on Hacksaw;
#   G4.1  the operator's own platform notice posted into a provider's group
#         ("Casinoplus will perform system maintenance…", 我司…请暂停发版);
#   G4.2  a payment / telco / cloud relay (GCash, BDO, Globe, AWS…);
#   G4.4  another Base provider's notice relayed in ("JILI has emergency
#         maintenance…, not your games").
#
# These rules run only on a fill or clear verdict and only choose between
# "write" and "do not write": a notice they stop becomes a needs-human card (or,
# in a shared group, is left to the co-tenant row it names). Every refusal names
# the phrase that fired, so the card says why. A notice that names nobody in a
# one-row group is still that row's - that is how most providers write, and
# refusing it would trade a wrong write for a missed one on every notice.

@functools.lru_cache(maxsize=512)
def _name_re(name: str):
    """One name as a regex.

    ASCII-alnum boundaries instead of \\b: to Python a CJK character is \\w, so
    \\bHS\\b would miss "HS游戏", while "YG" must still not match inside "YGG".
    A name of three characters or fewer matches case-SENSITIVELY - "CP", "HS",
    "IGO", "VA" are brands in capitals; "cp"/"va" in running text are not.
    """
    core = r"\s+".join(re.escape(p) for p in name.split())
    flags = 0 if len(re.sub(r"\s+", "", name)) <= 3 else re.I
    return re.compile(r"(?<![A-Za-z0-9])" + core + r"(?![A-Za-z0-9])", flags)


def _names_for(provider: str) -> list:
    """The provider's own name plus every alias in its VAWATCH_PROVIDER_ALIASES
    entry, de-duplicated case-insensitively."""
    p = " ".join(str(provider or "").split())
    if not p:
        return []
    names = [p]
    for grp in _alias_groups():
        if _norm(p) in {_norm(n) for n in grp}:
            names += grp
    out: list = []
    for n in names:
        if _norm(n) not in {_norm(x) for x in out}:
            out.append(n)
    return out


# A sentence that says the provider it names is NOT down. Sentence-scoped, and
# deliberately so: "Yggdrasil will undergo maintenance on X. Hacksaw and Relax
# games are NOT affected." names Hacksaw only in the second sentence. Splitting
# finer, on commas, would read "Hacksaw" in "Hacksaw, Relax and NetEnt are not
# affected" as a positive mention and write Relax's window onto Hacksaw.
_NOT_AFFECTED_RE = re.compile(
    r"\bnot\s+(?:be\s+)?(?:affected|impacted|included|involved|part\s+of)\b"
    r"|\bun(?:affected|impacted)\b"
    r"|\bno\s+(?:impact|effect|downtime|interruption|maintenance)\b"
    r"|\b(?:remain|remains|stay|stays)\s+(?:available|online|open|operational"
    r"|accessible|unaffected|normal)\b"
    r"|\b(?:operate|operates|run|runs|running)\s+(?:as\s+)?normal(?:ly)?\b"
    r"|\bas\s+usual\b|\bexcept\b|\bexcluding\b|\bother\s+than\b"
    r"|不受影响|不影响|无影响|没有影响|不包括|不包含|除外|照常"
    r"|正常(?:运行|运营|营运|开放|使用)", re.I)

_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|[。！？!?；;\n]+")


def _mentions(text: str, names: list) -> tuple:
    """-> (positive, negated) counts of sentences naming any of ``names``."""
    pos = neg = 0
    if not names:
        return 0, 0
    for s in _SENTENCE_SPLIT_RE.split(text or ""):
        if s and any(_name_re(n).search(s) for n in names):
            if _NOT_AFFECTED_RE.search(s):
                neg += 1
            else:
                pos += 1
    return pos, neg


def _operator_subject_re():
    """The operator as the SUBJECT of a maintenance sentence.

    Adjacency is what separates the subject from an addressee: "Casinoplus will
    perform system maintenance", "IGO scheduled maintenance:", "CP平台将于…",
    "our side (CasinoPlus)". "Dear CasinoPlus, we will…" does not match - the
    comma breaks it - and neither does "To: CasinoPlus" followed by a heading on
    the next line, because the gap allowed is spaces and tabs, never a newline.
    """
    names = []
    for n in _operator_names():
        body = r"[ \t]+".join(re.escape(p) for p in n.split())
        # Same case rule as _name_re: "CP"/"IGO" in capitals only.
        names.append("(?i:" + body + ")" if len(n.replace(" ", "")) > 3 else body)
    if not names:
        return None
    alt = "(?:" + "|".join(names) + ")"
    tail = (r"(?:[ \t]*[(（][^()（）\n]{1,30}[)）])?(?:'s|’s)?"
            r"(?:(?i:[ \t]+(?:platform|system|systems|site|website|side"
            r"|back[ \t]*office|backend|servers?))?"
            r"(?i:[ \t]+(?:will|shall|is[ \t]+going[ \t]+to|is[ \t]+scheduled"
            r"|ha(?:s|ve)[ \t]+(?:an?[ \t]+)?(?:scheduled|planned|emergency"
            r"|routine|system|platform)"
            # ...and the operator's own "no maintenance": a clear, which with
            # VAWATCH_CLEAR_ENABLED=1 would blank the provider's live window.
            r"|ha(?:s|ve)[ \t]+no[ \t]+(?:scheduled[ \t]+|planned[ \t]+)?"
            r"maintenance"
            r"|(?:scheduled|planned|emergency|routine|system|platform)[ \t]+"
            r"(?:maintenance|downtime|upgrade)"
            r"|maintenance|downtime|upgrade))(?![A-Za-z])"
            r"|[ \t]*(?:平台|系统|方)?[ \t]*(?:将|计划|定于|拟于|进行|例行维护"
            r"|系统维护|维护|停机|升级))")
    ours = (r"(?i:\b(?:our|my)[ \t]+(?:side|platform|system|site|website|end"
            r"|team))[ \t]*[(（][ \t]*" + alt)
    return re.compile("(?<![A-Za-z0-9])" + alt + tail + "|" + ours)


# The message is ADDRESSED TO the provider, so the speaker is the operator: a
# provider says "our games", never "your games", and never asks its operator to
# stop deploying or warns it of "no traffic". Only things the PROVIDER owns are
# listed after "your": "your lobby" / "your players" / "your site" are the
# operator's, so a provider says them ("our games will leave your lobby").
_OPERATOR_AUDIENCE_RE = re.compile(
    r"(?i:\byour[ \t]+(?:games?|products?|live[ \t]+tables?|tables?"
    r"|slots?|titles?)\b"
    r"|\b(?:no|zero|low|lower|less|reduced)[ \t]+(?:player[ \t]+)?traffic\b"
    r"|\bavoid(?:ing)?[ \t]+(?:any[ \t]+|all[ \t]+)?(?:deployments?|releases?"
    r"|deploying|releasing)\b"
    r"|\b(?:do[ \t]+not|don'?t)[ \t]+(?:deploy|release)\b)"
    r"|暂停发版|停止发版|请勿发版|避免发版|不要发版|无流量|没有流量"
    r"|贵司(?:的)?(?:游戏|产品)|你们的游戏|您的游戏")

# What may sit just before the operator's name when the operator is NOT the
# subject: an addressee ("Dear CasinoPlus", "致CasinoPlus") or a co-brand joiner
# ("Pragmatic Play x CasinoPlus scheduled maintenance", "the maintenance for
# CasinoPlus will be…"). Checked on the same line only.
_NOT_SUBJECT_BEFORE_RE = re.compile(
    r"(?i)(?:\bdear|\bhi|\bhello|\bto|\battn\.?|\battention|\bfor|\bwith|\band"
    r"|\bx|×|&|致|尊敬的|亲爱的|给)[ \t,:：]*$")


def _operator_hit(text: str) -> tuple:
    """-> ("subject" | "audience", phrase), or ("", "") when neither fires.

    "subject": the operator is what goes down. "audience": the text is written
    TO the provider - the operator's own notice or a relay of someone else's -
    so the card can say which it saw instead of calling a relay of Evolution's
    window "the operator's maintenance".
    """
    t = text or ""
    rx = _operator_subject_re()
    for m in (rx.finditer(t) if rx else ()):
        line_start = t.rfind("\n", 0, m.start()) + 1
        if not _NOT_SUBJECT_BEFORE_RE.search(t[line_start:m.start()]):
            return "subject", m.group(0).strip()
    m = _OPERATOR_AUDIENCE_RE.search(t)
    return ("audience", m.group(0).strip()) if m else ("", "")


# A payment channel, bank, telco or cloud vendor as the thing under maintenance.
# One list, greppable, behind VAWATCH_OWNER_CHECK. The short or common-word
# names (Maya, Globe, Smart, BDO, AWS…) are case-sensitive, and "Smart" only
# counts next to a telco word. A bare "data centre" is NOT here: "Our data centre
# will have maintenance, games unavailable" is the provider's own outage.
_THIRD_PARTY_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:(?i:G-?Cash|PayMaya|UnionBank|Union[ \t]+Bank|Metrobank"
    r"|Landbank|InstaPay|PESONet|GrabPay|ShopeePay|Coins\.ph|Converge"
    r"|Cloudflare|Amazon[ \t]+Web[ \t]+Services|Azure|Alibaba[ \t]+Cloud|Aliyun"
    r"|Google[ \t]+Cloud|Akamai|Tencent[ \t]+Cloud|Huawei[ \t]+Cloud)"
    r"|Maya|BDO|BPI|RCBC|Globe|PLDT|DITO|AWS|GCP|ISP"
    r"|Smart(?=[ \t]+(?:Communications|Telecom|network|signal|subscribers?"
    r"|users?|prepaid|postpaid)))(?![A-Za-z0-9])"
    r"|(?i:\b(?:online[ \t]+)?banking\b|\bbanks?[ \t]+(?:maintenance|system"
    r"|systems|transfers?|channels?)\b|\btelco\b|\be-?wallets?\b"
    r"|\bpayment[ \t]+(?:channels?|gateways?|providers?|methods?)\b"
    r"|\bcash-?in\b|\bdeposits?[ \t]+via\b)"
    r"|支付通道|支付渠道|银行|电信|运营商网络|网银")


def _known_providers(d: dict) -> list:
    """Every provider the Base names (watched or not), from this process's last
    watch list or the ledger's cached one - no Base call."""
    rep = _LAST_WATCH.get("rep") or (d.get("targets") or {})
    out: list = []
    for part in ("telegram", "teams", "skipped"):
        for r in rep.get(part) or []:
            p = " ".join(str(r.get("provider") or "").split())
            if p and _norm(p) not in {_norm(x) for x in out}:
                out.append(p)
    return out


def _shared_from_watch(d: dict, provider: str, group: str) -> list:
    """The co-tenants of this row's group, when the caller did not say.

    A caller that predates ``shared_with`` (or a cold process) must not read as
    "one row in this group" - that assumption is F37 - so the last watch list,
    then the ledger's cached one, is asked instead.
    """
    rep = _LAST_WATCH.get("rep") or (d.get("targets") or {})
    g, me = _norm(group), _norm(provider)
    return [r.get("provider") for r in rep.get("telegram") or []
            if _norm(r.get("group")) == g and r.get("provider")
            and _norm(r.get("provider")) != me]


def _attribute(text: str, verdict: dict, *, provider: str, group: str,
               shared_with: list, others: list, sender: str = "") -> Optional[dict]:
    """Is this fill/clear really this row's? None = yes, write it.

    Otherwise -> {"action": "needs_human" | "ignore", "reason": str}. "ignore"
    is used for exactly one case: a shared group where the notice names only a
    co-tenant. That row takes it; carding it from here too would be the
    duplicate red card F58 describes.
    """
    action = verdict.get("action")
    if action not in ("fill", "clear"):
        return None

    def ask(reason: str) -> dict:
        return {"action": "needs_human", "reason": reason}

    if group and not (provider or "").strip():
        # F39's last line of defence: never guess the row.
        return ask("the Base row for this group has no Provider / Games value "
                   "- there is no row to write")
    if sender:
        return ask(f"posted by an operator-side account ({sender}), not by "
                   f"{provider}")
    fill = action == "fill"

    def named(names: list) -> bool:
        # For a fill, "Hacksaw is NOT affected" is not naming Hacksaw. For a
        # clear the negation IS the content ("no maintenance for Hacksaw"), so
        # any mention counts.
        p, n = _mentions(text, names)
        return p > 0 if fill else (p + n) > 0

    owner = _owner_check()
    if owner:
        kind, phrase = _operator_hit(text)
        if kind == "subject":
            return ask(f"maintenance belongs to the operator, not {provider} "
                       f"(“{phrase}”)")
        if kind == "audience":
            return ask(f"written by the operator side to the provider (“{phrase}”)"
                       f" - the operator's own notice or a relay, not "
                       f"{provider}'s")
    own = named(_names_for(provider))
    shared = list(shared_with or []) if _shared_group_check() else []
    co = [p for p in shared if named(_names_for(p))]
    if co and not own:
        return {"action": "ignore",
                "reason": f"names {', '.join(co)}, not {provider} - left to "
                          f"that row"}
    if owner and not own:
        pos, neg = _mentions(text, _names_for(provider))
        if fill and neg and not pos:
            return ask(f"says {provider} is not affected")
        m = _THIRD_PARTY_RE.search(text or "")
        if m:
            return ask(f"third-party maintenance (“{m.group(0)}”), not "
                       f"{provider}")
        mine = {_norm(n) for n in _names_for(provider)} | {_norm(p) for p in shared}
        for other in others:
            if _norm(other) in mine:
                continue
            if named(_names_for(other)):
                return ask(f"names {other}, not {provider}")
    if shared:
        if own and co:
            return ask(f"names both {provider} and {', '.join(co)} - cannot tell "
                       f"whose window it is")
        if not own:
            return ask(f"this group is shared by {provider} and "
                       f"{', '.join(shared)}, and the notice names none of them")
    return None


def _is_out(msg: dict) -> bool:
    v = msg.get("out")
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes"}
    return bool(v)


def _effective_senders(msgs: list) -> list:
    """Each bubble's sender, with Web K's blank run-continuation names filled.

    Web K prints the name only on the FIRST bubble of a run from one sender, so
    a colleague's second and third bubbles read as sender "". A blank inbound
    bubble inherits the bubble before it, unless that one was ours (an outbound
    bubble ends the run). The first bubble of the read window can still be
    blank - it stays "", which is treated like any provider's.
    """
    out, prev = [], ""
    for m in msgs:
        if _is_out(m):
            out.append("me")
            prev = ""
            continue
        s = " ".join(str(m.get("sender") or "").split()) or prev
        out.append(s)
        prev = s
    return out


# Under /vacheck force, the walk runs NEWEST first and stops at the first message
# that says anything about maintenance - not merely the first one it can write.
# It used to walk straight past "the maintenance has been completed", "…has been
# cancelled" and "no maintenance this week" (all verdicts that write nothing) and
# re-assert the older window beneath them, carding it as scheduled (F56). The
# only ignores it may walk past are these: messages about something else.
_NOT_MAINTENANCE_REASON_RE = re.compile(
    r"no\s+scheduled[- ]maintenance\s+wording|empty\s+message", re.I)


def _shared_card_key(msg: dict, group: str) -> str:
    return "shared:" + _key(msg, group, "")


def _apply_pending(d: dict, sc: dict, res: dict, *, provider: str, group: str,
                   record_id: str, now: datetime, checkpoint) -> bool:
    """Write a deferred second outage once the row's earlier window has ended.

    G3.1: a later, separate outage announced while the row still holds an
    earlier future window is carded and kept here instead of overwriting the
    imminent one. Newest-wins alone erased that imminent window from the row
    and nothing ever put the later one back either, because its message was
    already ledgered 'filled'. -> False when the ledger could not be saved.
    """
    before = list(sc.get("pending") or [])
    pend = [p for p in before if (_from_iso(p.get("end")) or now) > now]
    for p in before:
        if p not in pend:
            print(f"[vawatch] {group!r}: the deferred window "
                  f"{_fmt_win(_from_iso(p.get('start')), _from_iso(p.get('end')))} "
                  f"ended while the row still held another one - it was carded "
                  f"when it arrived and is dropped now", flush=True)
    sc["pending"] = pend
    owner = sc.get("owner")
    if not pend or _owner_live(owner, now):
        return True
    p = min(pend, key=lambda x: _from_iso(x.get("start")) or now)
    ps, pe = _from_iso(p.get("start")), _from_iso(p.get("end"))
    verdict = {"action": "fill", "start": ps, "end": pe,
               "reschedule": bool(p.get("resched")), "stale": False,
               "reason": "deferred second outage"}
    os_, oe = _owner_window(owner)
    done = act_on_notice(
        p.get("text") or "", verdict, provider=provider, group=group,
        record_id=record_id,
        note=(f"**Deferred window:** announced while the row still held "
              f"{_fmt_win(os_, oe)}, which has now ended."
              if owner else "**Deferred window**, written now."))
    if done["wrote"]:
        prev = d["handled"].get(p.get("key")) or {}
        _set_owner(sc, mid=p.get("mid") or "", key=p.get("key") or "",
                   verdict=verdict, kind="fill")
        if p.get("key"):
            _mark(d, p["key"], "filled",
                  {"h": prev.get("h"), "window": done["wrote"],
                   "why": "deferred second outage, written after the earlier "
                          "window ended"})
        res["acted"] += 1
        res["pending_written"] += 1
    else:
        p["attempts"] = int(p.get("attempts") or 0) + 1
        if p["attempts"] >= _write_attempts():
            sc["pending"] = [x for x in sc.get("pending") or [] if x is not p]
        res["write_failed"] += 1
    res["details"].append({"key": p.get("key") or "", "action": "filled",
                           "window": done["wrote"], "error": done["error"],
                           "why": "deferred second outage",
                           "record_id": done.get("record_id") or "",
                           "carded": done.get("carded", False)})
    return checkpoint()


def handle_messages(messages: list, *, force: bool = False,
                    provider: str = "", group: str = "", record_id: str = "",
                    shared_with: Optional[list] = None) -> dict:
    """Classify each message, act on new maintenance notices.

    ``force`` re-acts on the newest maintenance notice even if the ledger has
    already handled it — for a manual /vacheck when you want to prove the path.
    It walks newest-first and STOPS at the first message that says anything
    about maintenance, whatever that says: a completed / cancelled / "no
    maintenance" / unreadable / needs-a-human verdict ends the walk without
    writing, and a window that has already passed is reported, never written.
    It used to walk past all of those and re-assert the older window beneath
    them (F56), and to mark every needs-human after the first FINAL without
    sending its card, so a new notice's card was never sent at all (F68).
    When the walk stops on a WRITE, every older bubble it did not reach is
    recorded as superseded (or, on a cold group, as backlog): they used to be
    left unmarked, and the next tick wrote the superseded notice over the one
    force had just written (F36).

    ``record_id`` is the row the watch list read, written directly (F70).
    ``shared_with`` names the other rows whose Group Name is this one; None
    means "look it up", never "there are none" (F37).

    Every verdict is answered. The `if action != "fill": ignore` this replaced
    threw away three outcomes that each leave the row WRONG rather than merely
    unfilled - needs_human, clear and follow_quote - and the single `ignored`
    counter then reported all of them, plus the genuinely unrelated messages,
    under one number the caller prints as "not scheduled maintenance". The
    counter is still there and still the grand total; the breakdown beside it
    says which of the four actually happened.

    ORDER. The handled-keys ledger says whether a message was decided, not
    whether it is older than the message the row already reflects; every rule
    below uses data-mid order within this provider@group (see "Per-row state"):

      * a fill or clear from a message OLDER than the one that last wrote the
        row is never written. It is recorded as superseded when writing it
        would change nothing (same window), when the newer message was a
        reschedule, or when the older one was edited to retract itself; any
        other case is carded (F34 retry, F35 late render, F41/G2.5 edit);
      * an edit whose text is the one already handled is already handled; an
        edit of the row's own notice that adds retraction wording, or no
        longer reads as that window, is carded; one that moves the window is
        written - the correction the edit hash exists for (G2.5);
      * an unseen bubble at or below the cold-start floor is backlog (F35, F40);
      * a separate outage starting 24h+ after the row's still-future window is
        carded and deferred, then written once that window has ended; one
        ending 24h+ before it is written, and the card names what it replaced
        (G3.1);
      * the row's own notice vanishing from between two surviving bubbles is
        carded once (G2.4); a read that does not reach the previous visit is
        reported, and carded at most every _GAP_CARD_EVERY (F57).

    A message whose handling raises is marked 'error' and the sweep carries on
    (F82). The ledger is saved after every write and card, and a sweep that
    cannot read the ledger, or cannot save it, raises LedgerError BEFORE acting
    (F76, F77): the ledger is what stops the same notice being re-written and
    re-carded on every tick.
    """
    d = _load()
    if d.get("_unreadable"):
        raise LedgerError(f"the ledger could not be read ({d['_unreadable']}) - "
                          f"nothing was acted on and nothing was saved over it")
    bk = _baseline_key(group, provider)
    # From THIS load. is_baselined() re-read the file, so one failed first read
    # plus one good second read acted on an empty ledger as a warm group (F76).
    cold = (not bool((d.get("baselined") or {}).get(bk))) if group \
        else ledger_is_cold()
    with _ledger_lock:
        persisted = _save(d)
    if not persisted:
        raise LedgerError(f"the ledger cannot be saved ({_SAVE_ERROR}) - every "
                          f"write and card is held until it can, or each would "
                          f"repeat on every tick")
    now = _now_dt()
    sc = _scope(d, bk)
    _init_scope(d, sc, bk)
    res = {"seen": 0, "acted": 0, "ignored": 0, "already": 0,
           "cold_start": cold, "details": [],
           # `ignored` above stays the grand total main.py has always printed.
           # These split it; `acted` now counts only writes that SUCCEEDED, with
           # the failures counted separately instead of being reported as fills.
           "ignored_not_maintenance": 0, "ignored_unparsed": 0,
           "ignored_stale": 0, "needs_human": 0, "cleared": 0,
           "write_failed": 0, "retried": 0,
           "ignored_outbound": 0, "ignored_other_provider": 0,
           # The order/state rules above, each counted where it fired.
           "superseded": 0, "history": 0, "deferred": 0, "pending_written": 0,
           "deleted_notice": 0, "errors": 0, "gap": {}, "ledger_error": "",
           "renamed_from": "",
           # Under force: the verdict the walk stopped at, when it wrote nothing.
           "force_stop": "",
           # The Base rows nothing on this path reads (APP=TEAMS, excluded,
           # blank), straight from the ledger cache - no Base call here.
           "unwatched": _unwatched_from_cache(d)}

    if shared_with is None:
        shared_with = _shared_from_watch(d, provider, group) if group else []
    others = _known_providers(d)
    msgs = list(messages or [])
    senders = _effective_senders(msgs)
    operators = _operator_senders()
    nums = [_mid_num(m.get("mid")) for m in msgs]
    known = [n for n in nums if n is not None]
    floor, last = sc.get("floor"), sc.get("last")
    ordered = _order_check()
    if not ordered:
        floor = last = None             # no history floor, no gap (see _order_check)

    # F69: a cold scope whose record_id was last read under ANOTHER scope is a
    # renamed row (Group Name after a Telegram rename, or an edited Provider),
    # not a new one. Its first sweep still records the backlog without writing
    # - the new title could even be a different chat, whose mids mean nothing
    # against the old ones - but a fill-able notice newer than anything the old
    # scope saw is carded instead of being buried as "cold-start".
    #
    # Any change of scope counts, not only a first one: a row renamed BACK
    # lands on a scope that is still baselined but stale - everything handled
    # under the other name would look new to it - so it gets the same careful
    # sweep.
    renamed, old_last = "", None
    prev = (d.get("records") or {}).get(record_id) if (group and record_id) else None
    if prev and prev != bk:
        renamed = prev
        cold = True
        res["cold_start"] = True
        old = (d.get("scopes") or {}).get(prev) or {}
        old_last = old.get("last")
        if old.get("owner"):
            # The same RECORD, so the other scope's last write is what the row
            # holds now; its mid may belong to another chat, so it is dropped.
            sc["owner"] = dict(old["owner"], mid="", carried_from=prev)
        res["renamed_from"] = prev

    def checkpoint() -> bool:
        with _ledger_lock:
            ok = _save(d)
        if not ok and not res["ledger_error"]:
            res["ledger_error"] = (f"the ledger could not be saved after a write "
                                   f"or card ({_SAVE_ERROR}) - the sweep stopped "
                                   f"so nothing repeats")
        return ok

    halted = False
    if not cold and not force and ordered and sc.get("pending"):
        try:
            halted = not _apply_pending(d, sc, res, provider=provider, group=group,
                                        record_id=record_id, now=now,
                                        checkpoint=checkpoint)
        except Exception as err:      # noqa: BLE001
            res["errors"] += 1
            print(f"[vawatch] deferred-window write for {group!r} failed: "
                  f"{err!r}", flush=True)

    # Bubbles arrive OLDEST first. Normally that is what we want - each later
    # message overwrites the earlier one, so the newest ends up on the row. But
    # `force` stops at the first message it acts on, so it has to start from the
    # NEWEST or it would re-assert a notice that a later reschedule superseded.
    order = list(reversed(range(len(msgs)))) if force else list(range(len(msgs)))
    stop_pos: Optional[int] = None   # where a force walk stopped
    force_wrote = False              # ...and whether it stopped on a write
    for pos, i in enumerate(order):
        if halted:
            break
        msg = msgs[i]
        text = str(msg.get("text") or "").strip()
        if not text:
            continue
        res["seen"] += 1
        key = _key(msg, group, provider)
        n = nums[i]
        mid = str(msg.get("mid") or "").strip()
        edited = bool(msg.get("edited")) and bool(mid)
        base_key = _key(dict(msg, edited=False), group, provider) if edited else key
        h = _body_hash(text)
        rx = bool(_EDIT_RETRACT_RE.search(text))
        nh_extra = ""

        def mark(outcome: str, extra: Optional[dict] = None, _k=key, _h=h,
                 _rx=rx) -> None:
            # The body hash rides on every entry so a later EDIT of this bubble
            # can be told apart from a re-render of the same text (G2.5).
            _mark(d, _k, outcome, {"h": _h, **({"rx": 1} if _rx else {}),
                                   **(extra or {})})

        try:
            # The ledger is still consulted before anything is DONE, but what
            # counts as handled now depends on how the last attempt ended (see
            # _seen): a failed write and an unreadable window come back round,
            # a success and a plain "not about maintenance" do not.
            if _seen(d, key) and not force:
                _touch(d, key)
                if edited:
                    _touch(d, base_key)
                res["already"] += 1
                res["details"].append({"key": key, "action": "already handled"})
                continue
            versions = _mid_versions(d, base_key) if edited else []
            # What the bubble said before this edit, as far as the ledger knows.
            base = {"h": next((v.get("h") for v in versions if v.get("h")), None),
                    "rx": any(v.get("rx") for v in versions)} if versions else None
            if edited and not force and any(v.get("h") == h for v in versions):
                # Telegram's "edited" marker with the text we already handled:
                # a strikethrough, a formatting change, an edit that was undone.
                # innerText is identical, so the key changed only because the
                # edit hash is folded in - and the notice was re-written and
                # re-announced as fresh "Scheduled maintenance" (G2.5).
                _touch(d, base_key)
                mark("edit-unchanged",
                     {"why": "edited, but the text is the one already handled"})
                res["already"] += 1
                res["details"].append({"key": key, "action": "already handled",
                                       "why": "edited - text unchanged"})
                continue
            if key in d["handled"] and not _seen(d, key):
                # Only a key still inside its retry cap is a re-try. Under force
                # a FINISHED key reaches here too, and /vacheck force reported
                # it as "re-tried after an earlier failure" when nothing had.
                res["retried"] += 1
            if (floor is not None and n is not None and n <= float(floor)
                    and not cold and not force and not versions
                    and key not in d["handled"]):
                # At or below the highest mid of this group's cold-start read,
                # and never decided: history that became visible only now - a
                # bubble the lazy list had not rendered, a raised
                # VAWATCH_READ_COUNT, a key evicted from the ledger. The cold
                # sweep exists so history is recorded, not acted on; it used to
                # protect only what the first read happened to show (F35, F40).
                # An "edited" bubble is no exception unless this mid was seen
                # before: Web K shows the marker for ever, so a bubble edited
                # months ago that renders late is backlog like any other.
                mark("cold-start", {"why": "older than this group's first read "
                                           "- backlog that became visible later"})
                res["history"] += 1
                res["details"].append({"key": key, "action": "history"})
                continue

            if _is_out(msg) and _skip_outbound():
                # A bubble WE sent (telegramwarm reads Web K's is-out class). It
                # is never the provider's notice: it is our ops account relaying
                # the operator's own maintenance, or asking "can it be postponed
                # to <window>?" - and both parsed as a fill and overwrote the
                # provider's row (F38). Skipped before classify, as the switch's
                # docstring always said; VAWATCH_SKIP_OUTBOUND=0 restores it.
                mark("outbound", {"why": "sent from our own account"})
                res["ignored"] += 1
                res["ignored_outbound"] += 1
                res["details"].append({"key": key, "action": "outbound"})
                continue

            verdict = classify(text)
            action = verdict["action"]
            why = verdict.get("reason") or ""
            # With VAWATCH_ORDER_CHECK=0 there is no owner to compare against,
            # which switches off every order rule below at once.
            owner = sc.get("owner") if ordered else None
            own_n = _mid_num((owner or {}).get("mid"))

            if (edited and not force and n is not None and own_n is not None
                    and n == own_n and _owner_live(owner, now)):
                # An edit of the very notice whose window the row holds.
                added_rx = rx and not (base or {}).get("rx")
                if action == "fill" and not verdict.get("stale") and not added_rx:
                    if _same_window(verdict, owner):
                        # A typo fixed, a line added: the row already says this.
                        mark("edit-same-window",
                             {"why": "edited; still states the row's window"})
                        res["already"] += 1
                        res["details"].append({"key": key,
                                               "action": "already handled",
                                               "why": "edited - same window"})
                        continue
                    # The window MOVED and nothing retracts it: the in-place
                    # correction providers routinely make. Falls through and is
                    # written like any notice.
                else:
                    # "[VOID]" / 【作废】 / "(發錯群，請忽略)" added, or the notice
                    # now reads as cancelled / completed / something else. The
                    # parser still reads a voided notice as a fill of the voided
                    # window, which is how it was re-announced (G2.5). Nothing
                    # is cleared automatically: a person decides.
                    os_, oe = _owner_window(owner)
                    hit = _EDIT_RETRACT_RE.search(text) if added_rx else None
                    nh_extra = (f"**The notice that set {_fmt_win(os_, oe)} on this "
                                f"row was EDITED** — "
                                + (f"retraction wording was added (“{hit.group(0)}”)."
                                   if hit else
                                   f"it now reads: {action} — {why or 'no reason'}.")
                                + " The row still shows that window.")
                    verdict = dict(verdict, action="needs_human",
                                   reason="the notice this row was filled from was "
                                          "edited")
                    action, why = "needs_human", verdict["reason"]

            if (action == "ignore" and owner is not None and not cold and not force
                    and _owner_live(owner, now)
                    and _CANCEL_REASON_RE.search(why) and _CANCEL_TEXT_RE.search(text)
                    and not (n is not None and own_n is not None and n <= own_n)):
                # A NEWER message cancelling maintenance while the row still
                # holds a window this watcher wrote. It names that window, or
                # none ("the maintenance has been cancelled"): either way the
                # row is now wrong and a person has to clear it - this path
                # never blanks a row on its own (VAWATCH_CLEAR_ENABLED is for
                # "no maintenance", not for this).
                os_, oe = _owner_window(owner)
                try:
                    import noticeparse
                    w = noticeparse.find_window(text, now=now)
                except Exception:     # noqa: BLE001
                    w = None
                if w is None or _near(w[0], w[1], os_, oe):
                    nh_extra = (f"**The provider says the maintenance was "
                                f"CANCELLED** — the row still shows "
                                f"{_fmt_win(os_, oe)}. If that is the one called "
                                f"off, clear the row by hand.")
                    verdict = dict(verdict, action="needs_human",
                                   reason="the provider cancelled maintenance the "
                                          "row still shows")
                    action, why = "needs_human", verdict["reason"]

            if action == "ignore":
                # Cheap and side-effect free, so it runs even on a cold start.
                outcome = "unparsed" if _is_unparsed(why) else "ignored"
                if outcome == "unparsed" and cold and not force:
                    # ...but "unparsed" is RETRY-eligible, and this branch sits
                    # ahead of the cold-start guard below. A half-rendered
                    # backlog bubble therefore never got the cold-start mark,
                    # the group was baselined anyway (seen > 0), and the NEXT
                    # sweep read the same mid in full and wrote a backlog notice
                    # to the row - exactly what cold start exists to stop.
                    # Worse, this release's ledger migration re-baselines every
                    # group at once, so it would have fired on all 18 the moment
                    # it deployed. On a cold sweep the mark has to be terminal.
                    outcome = "cold-start"
                mark(outcome, {"why": why})
                res["ignored"] += 1
                if outcome == "unparsed":
                    res["ignored_unparsed"] += 1
                else:
                    res["ignored_not_maintenance"] += 1
                res["details"].append({"key": key, "action": outcome, "why": why})
                if force and not _NOT_MAINTENANCE_REASON_RE.search(why):
                    # Completed, cancelled, unreadable - or a reason this code
                    # does not know, which stops the walk on the side that
                    # writes nothing.
                    res["force_stop"] = f"{outcome} — {why}"
                    stop_pos = pos
                    break
                continue

            if action == "fill" and verdict.get("stale"):
                # A window that has already finished: the provider is re-quoting
                # an old notice. Writing it would replace a live row with dead
                # dates. Under force too: a manual command is not a reason to
                # put last week's window back on a row that
                # /provideraskmaintenance or a human has since cleared - which
                # is what force used to do (F56).
                if not force or not _seen(d, key):
                    mark("stale", {"why": "window already passed"})
                res["ignored"] += 1
                res["ignored_stale"] += 1
                res["details"].append({"key": key, "action": "stale"})
                if force:
                    res["force_stop"] = "stale — the newest notice's window has " \
                                        "already passed, so nothing was written"
                    stop_pos = pos
                    break
                continue

            if cold and not force:
                # First sweep after the feature is switched on: record the
                # backlog without acting, or turning it on re-announces an old
                # notice and overwrites a row someone already curated by hand.
                # This guard now covers the carding verdicts too, or enabling
                # the watcher would empty every group's backlog into the
                # Laboratory group at once.
                if (renamed and action == "fill"
                        and (old_last is None or n is None or n > float(old_last))
                        and not _same_window(verdict, sc.get("owner"))):
                    attr = _attribute(text, verdict, provider=provider,
                                      group=group, shared_with=shared_with,
                                      others=others)
                    if attr is None or attr["action"] != "ignore":
                        nv = dict(verdict, action="needs_human",
                                  reason="found on the first read after this "
                                         "row's Group Name / Provider changed")
                        extra = (f"**This Base row was renamed** (last read as "
                                 f"{renamed!r}). The first read under the new name "
                                 f"records the backlog without writing, so this "
                                 f"notice ({_fmt_win(verdict.get('start'), verdict.get('end'))}) "
                                 f"was NOT written — it may be new.")
                        sent = _card_once(d, msg, group, shared_with, provider,
                                          lambda: card_needs_human(
                                              group, provider, text, nv, extra=extra))
                        mark("cold-start", {"why": "backlog at the first read after "
                                                   "a rename - carded",
                                            "carded": bool(sent.get("carded")),
                                            "renamed_from": renamed})
                        res["needs_human"] += 1
                        res["details"].append({"key": key, "action": "needs-human",
                                               "why": nv["reason"],
                                               "carded": sent["carded"],
                                               "error": sent["error"]})
                        if not checkpoint():
                            halted = True
                            break
                        continue
                mark("cold-start", {"why": "backlog at first run"})
                res["details"].append({"key": key, "action": "cold-start skip"})
                continue

            op_sender = senders[i] if (senders[i] and _norm(senders[i]) in operators) \
                else ""
            attr = _attribute(text, verdict, provider=provider, group=group,
                              shared_with=shared_with, others=others,
                              sender=op_sender)
            if attr is not None and attr["action"] == "ignore":
                # A shared group's notice that names only the OTHER row. That
                # row writes and cards it; this one records why it did not.
                mark("other-provider", {"why": attr["reason"]})
                res["ignored"] += 1
                res["ignored_other_provider"] += 1
                res["details"].append({"key": key, "action": "other-provider",
                                       "why": attr["reason"]})
                continue
            if attr is not None:
                # Not this row's window, or not provably: ask a person instead
                # of writing. The parsed window stays on the verdict for the card.
                verdict = dict(verdict, action="needs_human", reason=attr["reason"],
                               parsed_action=action)
                action, why = "needs_human", attr["reason"]

            if (action in ("fill", "clear") and n is not None and own_n is not None
                    and n < own_n):
                # OLDER than the message whose window the row holds: a retry of
                # a write that failed while a newer reschedule succeeded (F34),
                # a bubble that rendered after the newer one was applied (F35),
                # an edit to a superseded notice (F41, G2.5). Each used to be
                # written last and win, and nothing afterwards put the newer
                # window back - its key was final.
                os_, oe = _owner_window(owner)
                if ((action == "fill" and _same_window(verdict, owner))
                        or (action == "fill" and owner.get("resched"))
                        or (edited and rx and not (base or {}).get("rx"))):
                    # Writing it would change nothing, or the newer message
                    # explicitly moved the window, or the provider has just
                    # voided this older notice itself.
                    mark("superseded", {"why": f"older than message "
                                               f"{owner.get('mid')}, which set "
                                               f"the row ({_fmt_win(os_, oe)})"})
                    res["superseded"] += 1
                    res["details"].append({"key": key, "action": "superseded",
                                           "why": f"older than message "
                                                  f"{owner.get('mid')}"})
                    if force:
                        res["force_stop"] = ("superseded — the newest notice on "
                                             "screen is older than the one that "
                                             "set the row")
                        stop_pos = pos
                        break
                    continue
                nh_extra = (f"**Older than the message that last set this row** "
                            f"(message {owner.get('mid')}: {_fmt_win(os_, oe)}). "
                            f"Read as a {action}"
                            + (f" ({_fmt_win(verdict.get('start'), verdict.get('end'))})"
                               if action == "fill" else "")
                            + " but NOT written — a newer message decides this row.")
                verdict = dict(verdict, action="needs_human",
                               reason="older than the message that last set this row")
                action, why = "needs_human", verdict["reason"]

            fill_note = ""
            if (action == "fill" and _owner_live(owner, now)
                    and not verdict.get("reschedule")
                    and not (n is not None and own_n is not None and n <= own_n)):
                # G3.1: the row holds a window that has not ended, and this is
                # a DIFFERENT outage (a day or more apart, no reschedule verb).
                # Newest-wins stays the rule - it is how a correction without
                # reschedule wording lands - except in the one case where it
                # silently erased an imminent outage for a later one.
                os_, oe = _owner_window(owner)
                vs, ve = verdict.get("start"), verdict.get("end")
                if os_ and oe and vs and ve and vs >= oe + timedelta(hours=24):
                    nv = dict(verdict, action="needs_human",
                              reason="a second, later outage while the row still "
                                     "holds an earlier one that has not ended")
                    extra = (f"**A SECOND, separate outage.** The row keeps the "
                             f"earlier window {_fmt_win(os_, oe)} (still ahead, set "
                             f"by message {owner.get('mid') or '?'}). This notice "
                             f"announces {_fmt_win(vs, ve)}.")
                    tail = (f"**Nothing was written yet** — it is written "
                            f"automatically once {_fmt_win(os_, oe)} has ended, "
                            f"unless a newer notice changes this row first. If it "
                            f"REPLACES the earlier window instead, correct the row "
                            f"by hand now.")
                    sent = _card_once(d, msg, group, shared_with, provider,
                                      lambda: card_needs_human(
                                          group, provider, text, nv, extra=extra,
                                          tail=tail))
                    outcome = "deferred" if _note_outcome(sent) != "card-failed" \
                        else "card-failed"
                    # A newer notice about the SAME deferred outage (within a
                    # day of it) replaces the older one - newest wins among the
                    # deferred just as it does on the row.
                    pend = [p for p in sc.get("pending") or []
                            if p.get("key") != key
                            and not _near(_from_iso(p.get("start")),
                                          _from_iso(p.get("end")), vs, ve)]
                    pend.append({"mid": mid, "key": key, "start": _iso(vs),
                                 "end": _iso(ve), "resched": False,
                                 "text": text[:20000], "at": _now_str(),
                                 "attempts": 0})
                    sc["pending"] = pend[-5:]
                    mark(outcome, {"why": nv["reason"], "window": _fmt_win(vs, ve),
                                   "error": sent["error"]})
                    res["ignored"] += 1
                    res["needs_human"] += 1
                    res["deferred"] += 1
                    res["details"].append({"key": key, "action": "deferred",
                                           "why": nv["reason"],
                                           "carded": sent["carded"],
                                           "error": sent["error"]})
                    if not checkpoint():
                        halted = True
                        break
                    if force:
                        res["force_stop"] = "deferred — a second, later outage; " \
                                            "the row keeps the earlier window"
                        stop_pos = pos
                        break
                    continue
                if os_ and oe and vs and ve and ve + timedelta(hours=24) <= os_:
                    # Sooner AND newer: both rules agree it goes on the row. The
                    # later window it displaces may still stand (a second
                    # outage) or be dead (this was a correction) - that cannot
                    # be told from here, so the card says what was replaced.
                    fill_note = (f"⚠️ **Replaced a LATER window still ahead** "
                                 f"({_fmt_win(os_, oe)}, set by an earlier "
                                 f"notice). If that maintenance still stands, "
                                 f"re-enter it by hand after this one ends.")

            if action in ("needs_human", "follow_quote"):
                # follow_quote cannot be resolved here: the scraper reads
                # bubbles, not the messages they quote. Carding it as
                # needs-human puts it in front of a person, which is the only
                # outcome left that is not a silent drop.
                if nh_extra:
                    extra = nh_extra
                else:
                    extra = ("**The answer is in the message this one QUOTES** — "
                             "open the group to read it."
                             if action == "follow_quote" else "")
                    if verdict.get("parsed_action"):
                        extra = (f"Read as a {verdict['parsed_action']}"
                                 + (f" ({verdict['start']:%Y-%m-%d %H:%M} → "
                                    f"{verdict['end']:%Y-%m-%d %H:%M})"
                                    if verdict.get("start") and verdict.get("end")
                                    else "")
                                 + " but NOT written: it is not provably this "
                                   "row's.")
                if shared_with:
                    extra += ("\n" if extra else "") + (
                        f"**Shared group** — rows: "
                        f"{', '.join([provider] + list(shared_with))}")
                sent = _card_once(d, msg, group, shared_with, provider,
                                  lambda: card_needs_human(
                                      group, provider, text, verdict, extra=extra))
                mark(_note_outcome(sent), {"why": why, "error": sent["error"]})
                res["ignored"] += 1
                res["needs_human"] += 1
                res["details"].append({"key": key, "action": "needs-human",
                                       "why": why, "carded": sent["carded"],
                                       "error": sent["error"],
                                       "card_by": sent.get("by") or ""})
                if not checkpoint():
                    halted = True
                    break
                if force:
                    res["force_stop"] = f"needs a human — {why}"
                    stop_pos = pos
                    break
                continue

            if action == "clear":
                done = act_on_clear(text, verdict, provider=provider, group=group,
                                    record_id=record_id)
                if done.get("skipped"):
                    # The write half is switched off; say so to a human rather
                    # than leaving last week's window on the row with nobody
                    # told.
                    sent = card_needs_human(
                        group, provider, text, verdict,
                        extra=f"**The provider says there is no maintenance** — "
                              f"the automatic clear is off ({done['skipped']}), "
                              f"so the row still shows its previous window.")
                    mark("clear-skipped"
                         if _note_outcome(sent) != "card-failed" else "card-failed",
                         {"why": done["skipped"], "error": sent["error"]})
                    res["ignored"] += 1
                    res["needs_human"] += 1
                    res["details"].append({"key": key, "action": "clear-skipped",
                                           "why": done["skipped"],
                                           "carded": sent["carded"]})
                    if not checkpoint():
                        halted = True
                        break
                    if force:
                        # "No maintenance this week" is the newest word; force
                        # used to walk past it and re-write last week's window
                        # (F56).
                        res["force_stop"] = "no maintenance — the automatic " \
                                            "clear is off, so a card was sent " \
                                            "instead"
                        stop_pos = pos
                        break
                    continue
                mark("cleared" if done["wrote"] else "clear-failed",
                     {"window": done["wrote"] or "", "error": done["error"]})
                if done["wrote"]:
                    _set_owner(sc, mid=mid, key=key, verdict=verdict, kind="clear")
                    res["acted"] += 1
                    res["cleared"] += 1
                else:
                    res["write_failed"] += 1
                res["details"].append({"key": key, "action": "cleared",
                                       "window": done["wrote"],
                                       "error": done["error"],
                                       "record_id": done.get("record_id") or "",
                                       "carded": done.get("carded", False)})
                if not checkpoint():
                    halted = True
                    break
                if force:
                    stop_pos, force_wrote = pos, bool(done["wrote"])
                    break
                continue

            done = act_on_notice(text, verdict, provider=provider, group=group,
                                 record_id=record_id, note=fill_note)
            mark("filled" if done["wrote"] else "write-failed",
                 {"window": done["wrote"] or "", "error": done["error"]})
            if done["wrote"]:
                _set_owner(sc, mid=mid, key=key, verdict=verdict, kind="fill")
                res["acted"] += 1
            else:
                res["write_failed"] += 1
            res["details"].append({"key": key, "action": "filled",
                                   "window": done["wrote"], "error": done["error"],
                                   "record_id": done.get("record_id") or "",
                                   "carded": done.get("carded", False)})
            if not checkpoint():
                halted = True
                break
            if force:
                stop_pos, force_wrote = pos, bool(done["wrote"])
                break                  # one is enough to prove the path
        except Exception as err:      # noqa: BLE001
            # One message must not abort the sweep: an OverflowError inside
            # classify used to escape before the ledger was saved, so every
            # fill earlier in the same read was re-written and re-carded on
            # every rotation, and every notice after it in that group was never
            # looked at (F82). It is recorded and retried a bounded number of
            # times, like an unreadable window.
            print(f"[vawatch] message {mid or '?'} in {group!r} failed: {err!r}",
                  flush=True)
            try:
                mark("error", {"why": repr(err)[:300]})
            except Exception:         # noqa: BLE001
                pass
            res["errors"] += 1
            res["details"].append({"key": key, "action": "error",
                                   "why": repr(err)[:200]})
            continue

    if force and stop_pos is not None and not halted:
        # F36: force stopped at the newest maintenance message, so the older
        # bubbles were never visited. Left unmarked, the next tick wrote the
        # superseded notice force had just walked past, and on a cold group
        # carded the whole backlog (the group was baselined all the same).
        # After a stop that WROTE, older = superseded. After a stop that wrote
        # nothing, a warm group's older messages stay for the next tick to
        # handle - the needs-human it has not carded yet must still go (F68).
        for i in order[stop_pos + 1:]:
            m = msgs[i]
            t = str(m.get("text") or "").strip()
            if not t:
                continue
            k = _key(m, group, provider)
            if _seen(d, k):
                _touch(d, k)
                continue
            if cold:
                _mark(d, k, "cold-start", {"h": _body_hash(t),
                                           "why": "backlog at first run, older "
                                                  "than what /vacheck force read"})
                res["details"].append({"key": k, "action": "cold-start skip"})
            elif force_wrote:
                _mark(d, k, "superseded", {"h": _body_hash(t),
                                           "why": "older than the notice "
                                                  "/vacheck force just wrote"})
                res["superseded"] += 1
                res["details"].append({"key": k, "action": "superseded"})

    owner = sc.get("owner") if ordered else None
    on = _mid_num((owner or {}).get("mid"))
    if (not cold and not halted and on is not None and known and on not in known
            and _owner_live(owner, now) and not owner.get("deleted_carded")):
        below = [k for k in known if k < on]
        above = [k for k in known if k > on]
        if below and above:
            try:
                sent = card_deleted_notice(group, provider, owner,
                                           (max(below), min(above)))
            except Exception as err:  # noqa: BLE001
                sent = {"carded": False, "error": repr(err)}
            if sent.get("carded") or sent.get("disabled"):
                owner["deleted_carded"] = _now_str()
            res["deleted_notice"] += 1
            res["details"].append({"key": owner.get("key") or "",
                                   "action": "deleted-notice",
                                   "carded": bool(sent.get("carded")),
                                   "error": sent.get("error") or ""})

    if (not cold and last is not None and known and min(known) > float(last)
            and len(msgs) >= _read_count()):
        # F57, after telegramwarm's wider re-read (gap_read_count): this read
        # is a FULL window and still does not reach the newest bubble the
        # previous visit saw. A shorter read is the whole rendered list, which
        # nothing was pushed out of.
        gap = {"last": float(last), "oldest": min(known), "read": len(msgs),
               "carded": False}
        res["gap"] = gap
        print(f"[vawatch] {group!r}: possible missed messages - the oldest bubble "
              f"read ({gap['oldest']:g}) is newer than the last one seen "
              f"({gap['last']:g})", flush=True)
        gk = _norm(group)
        prev_at = _from_iso((d.get("gap_carded") or {}).get(gk))
        if (_gap_card_enabled() and not halted
                and (prev_at is None or now - prev_at >= _GAP_CARD_EVERY)):
            try:
                sent = card_gap(group, provider, gap)
            except Exception as err:  # noqa: BLE001
                sent = {"carded": False, "error": repr(err)}
            gap["carded"] = bool(sent.get("carded"))
            if sent.get("carded"):
                d.setdefault("gap_carded", {})[gk] = now.isoformat()

    if known:
        sc["last"] = max(known + ([float(last)] if last is not None else []))
    if cold and known:
        sc["floor"] = max(known + ([float(floor)] if floor is not None else []))
    sc["seen"] = _now_str()
    if group and record_id:
        d.setdefault("records", {})[record_id] = bk
    if cold and group and not halted:
        # Record the baseline only AFTER the backlog has been marked - now in
        # the SAME atomic save, so a crash can never leave the group looking
        # baselined with its history unrecorded.
        #
        # And only when the read actually SAW something. A chat whose
        # virtualised list had not rendered yet returns zero messages, and
        # baselining on that spends the one-time guard on nothing: the next
        # tick would treat the group as baselined and act on its whole real
        # backlog. A second consecutive empty read is accepted as "this group
        # is genuinely empty", so a quiet group does not stay unbaselined.
        er = d.setdefault("empty_reads", {})
        if res["seen"] > 0 or int(er.get(bk) or 0) + 1 >= 2:
            d.setdefault("baselined", {})[bk] = _now_str()
            er.pop(bk, None)
        else:
            er[bk] = int(er.get(bk) or 0) + 1
    checkpoint()
    return res


def _card_once(d: dict, msg: dict, group: str, shared_with: list,
               provider: str, send) -> dict:
    """Send a needs-human card - once per MESSAGE in a shared group, not per row.

    Both Hacksaw and YGG read the same bubble, so the "names none of them" card
    (and any other needs-human) went out twice, ~9 minutes apart. The first row
    to card it records that here; the second records "needs-human" without a
    second card. Only a DELIVERED card is recorded, so a failed one still gets
    its retry from whichever row comes next.
    """
    if not shared_with:
        return send()
    gk = _shared_card_key(msg, group)
    cards = d.setdefault("shared_cards", {})
    prev = cards.get(gk)
    if isinstance(prev, dict):
        return {"carded": False, "disabled": True, "error": "",
                "by": prev.get("by") or "the other row"}
    sent = send()
    if sent.get("carded"):
        cards[gk] = {"at": _now_str(), "by": provider}
        for k in list(cards)[:-500]:    # bounded, oldest first
            cards.pop(k, None)
    return sent


# ---------------------------------------------------------------------------
# The /vacheck reply
# ---------------------------------------------------------------------------

def _unwatched_names(unwatched: Any) -> list:
    """``unwatched`` as printable names. It is a dict {teams, skipped, at} from
    _unwatched_from_cache; main.py sliced it like a list (`unwatched[:8]`) and
    every /vacheck ended "❌ /vacheck failed: slice(None, 8, None)" AFTER the
    sweep had already written the Base and carded the Laboratory group (F55).
    A plain list is still accepted, for a caller that builds one."""
    if isinstance(unwatched, dict):
        gaps = list(unwatched.get("teams") or []) + list(unwatched.get("skipped") or [])
    elif isinstance(unwatched, (list, tuple)):
        gaps = list(unwatched)
    else:
        gaps = []
    out = []
    for g in gaps:
        if isinstance(g, dict):
            out.append(f"{g.get('provider') or '(no provider)'} "
                       f"({g.get('why') or 'not watched'})")
        elif str(g).strip():
            out.append(str(g).strip())
    return out


def format_check_summary(r: dict) -> str:
    """The /vacheck reply for one handle_messages result (+ group/provider).

    Lives here, not in main.py, so it runs in vawatch's offline selftest against
    a REAL result dict - the crash above shipped because nothing ever did.
    Every line is a distinct outcome: "filled" no longer counts clears, and "a
    card was posted" is only said when a detail reports a delivered card.
    """
    r = r or {}
    details = [x for x in (r.get("details") or []) if isinstance(x, dict)]
    acted, cleared = int(r.get("acted") or 0), int(r.get("cleared") or 0)
    bits = [f"✅ VA sweep done — {r.get('group') or 'group'} "
            f"({r.get('provider') or '?'}) — read {r.get('seen', 0)} message(s)",
            f"• filled: {max(0, acted - cleared)}"]
    for x in details:
        if x.get("action") in ("filled", "cleared") and x.get("window"):
            bits.append(f"   ↳ wrote {x['window']}"
                        + (f" (record {x['record_id']})" if x.get("record_id") else ""))
    for label, key in (
            ("row cleared (no maintenance)", "cleared"),
            ("⚠️ write FAILED (see the red card)", "write_failed"),
            ("⚠️ needs a human", "needs_human"),
            ("ignored (not about maintenance)", "ignored_not_maintenance"),
            ("ignored (maintenance wording, no readable window)",
             "ignored_unparsed"),
            ("ignored (window already passed)", "ignored_stale"),
            ("ignored (sent from our own account)", "ignored_outbound"),
            ("ignored (names the other row of this shared group)",
             "ignored_other_provider"),
            ("re-tried after an earlier failure", "retried"),
            ("superseded (older than the message that set the row)",
             "superseded"),
            ("deferred (a second, later outage — written once the row's "
             "window ends)", "deferred"),
            ("deferred window written now", "pending_written"),
            ("backlog that became visible late (recorded, not acted on)",
             "history"),
            ("⚠️ the row's notice was DELETED from the group", "deleted_notice"),
            ("⚠️ messages that could not be handled (see the log)", "errors"),
    ):
        if r.get(key):
            bits.append(f"• {label}: {r[key]}")
    bits.append(f"• already handled: {r.get('already', 0)}")
    if r.get("ledger_error"):
        bits.append(f"• ⚠️ ledger: {r['ledger_error']}")
    gap = r.get("gap") or {}
    if gap:
        bits.append(f"• ⚠️ possible missed messages: the oldest bubble read "
                    f"({gap.get('oldest', 0):g}) is newer than the last one seen "
                    f"({gap.get('last', 0):g})")
    if r.get("widened"):
        w = r["widened"]
        bits.append(f"• read widened from {w.get('from')} to {w.get('to')} "
                    f"bubbles to reach the previous visit")
    if r.get("renamed_from"):
        bits.append(f"• this row was renamed (last read as {r['renamed_from']!r})")
    if r.get("force_stop"):
        bits.append(f"• force stopped at the newest maintenance message: "
                    f"{r['force_stop']}")
    if r.get("cold_start"):
        bits.append("• first run — the existing backlog was recorded "
                    "without acting, so a new notice fires from now on")
    gaps = _unwatched_names(r.get("unwatched"))
    if gaps:
        # These rows are not a sweep result at all: nothing on this path can
        # ever fill them (APP=TEAMS, excluded, or blank). Saying so here is the
        # only place an operator finds out.
        bits.append("• NOT autofilled by any code path: " + ", ".join(gaps[:8])
                    + (f" … and {len(gaps) - 8} more" if len(gaps) > 8 else ""))
    cards = sum(1 for x in details if x.get("carded"))
    if cards:
        bits.append(f"{cards} card(s) posted to the Laboratory group.")
    return "\n".join(bits)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[list] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="VA maintenance watcher")
    ap.add_argument("--classify", metavar="FILE",
                    help="classify the text in FILE and print the verdict")
    ap.add_argument("--row", action="store_true",
                    help="show the Base row this would write to")
    ap.add_argument("--targets", action="store_true",
                    help="show the watch list AND the Base rows nothing reads")
    args = ap.parse_args(argv)

    if args.classify:
        text = open(args.classify, encoding="utf-8").read()
        v = classify(text)
        print(json.dumps({"action": v["action"], "reason": v["reason"],
                          "start": str(v["start"]), "end": str(v["end"])},
                         ensure_ascii=False, indent=2))
        return 0
    if args.row:
        row = find_provider_row(_provider())
        print(json.dumps(row, ensure_ascii=False, indent=2)[:2000])
        return 0
    if args.targets:
        # The unwatched half is printed on purpose: "which providers does this
        # never fill?" had no answer anywhere before, and the honest answer is
        # three of them.
        rep = watch_list()
        print(json.dumps(rep, ensure_ascii=False, indent=2))
        return 0
    ap.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
