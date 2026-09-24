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
three post a card instead of vanishing (see ``handle_messages``) - but only when
the message is about maintenance at all: noticeparse returns the same verdicts
for "the payout is postponed" and "请见截图", and the watched chats are two-way CS
groups (F58). A row's red cards are capped per day (VAWATCH_NEEDS_HUMAN_CAP).

Four more things leave the row wrong with nobody told, and are carded too: a
cancellation of the window the row holds (F51; cleared instead, with
VAWATCH_CLEAR_ENABLED=1, only when it names exactly that window and the row
still holds it), a follow-up retracting or correcting the notice right above it
(G2.3), a poster whose caption or neighbour names maintenance (F59), and a
write that landed but whose card never arrived - re-sent, card only (F71).

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

    An older message never overwrites the one that set the row, nor is it
    written after a newer one was carded to a person; history below the
    cold-start floor or the ledger's evicted mark is not acted on; a second
    outage is deferred; a deleted notice and a read gap are reported - all of
    them trust Telegram's data-mid to increase within a chat. That is how
    Telegram numbers messages, but it is one scraped attribute, so (like
    VAWATCH_SKIP_OUTBOUND) there is a way back: 0 turns the rules off, and
    then the LAST NEW message processed wins, whatever its data-mid - an older
    notice rendering late, or an edit of an older one, overwrites a newer
    reschedule (R1.70: this used to be documented as "newest-in-the-read
    wins", which it is not). The ledger's own safety (LRU eviction,
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

    A value that is neither yes nor no ("Y", "enabled", a typo) keeps the
    DEFAULT and is logged once (F83). It used to read as OFF, so
    VAWATCH_NEEDS_HUMAN_CARD=Y silently dropped every needs-a-human card, and
    the ledger then recorded those messages as handled for good. Every
    default-on flag here is a safety switch; a word the code does not know must
    not be the thing that turns one off. noticeparse._on does the same.
    """
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        raw = default
    word = raw.strip().lower()
    if word in _YES_WORDS:
        return True
    if word in _NO_WORDS:
        return False
    _warn_config(name, f"{name}={raw.strip()!r} is not 1/0 (true/false, yes/no, "
                       f"on/off) - using its default, {default}")
    return default.strip().lower() in _YES_WORDS


_YES_WORDS = frozenset({"1", "true", "yes", "on"})
_NO_WORDS = frozenset({"0", "false", "no", "off"})

#: Each misconfiguration logged once per process: name -> the message. The
#: settings are read on every sweep, so a print per read would bury the log.
#: /vacheck repeats them (config_warnings), because nobody reads stdout.
_CONFIG_WARNED: dict = {}


def _warn_config(name: str, message: str) -> None:
    if _CONFIG_WARNED.get(name) != message:
        _CONFIG_WARNED[name] = message
        print(f"[vawatch] config: {message}", flush=True)


def config_warnings() -> list:
    """Every setting this process could not use as written - this module's
    and noticeparse's - for the /vacheck reply."""
    out = list(_CONFIG_WARNED.values())
    try:
        import noticeparse

        out += [m for m in noticeparse.config_warnings() if m not in out]
    except Exception:                 # noqa: BLE001
        pass
    return out


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
# R1.60: PP writes itself "Pragmatic Play" / "Pragmatic" and KingMidas "KM" -
# without these, "Dear Pragmatic team, our site will be under maintenance"
# could not be seen as written TO the row's provider.
_DEFAULT_PROVIDER_ALIASES = ("Hacksaw=HS,Hacksaw Gaming;YGG=Yggdrasil,YG"
                             ";PP=Pragmatic Play,Pragmatic;KingMidas=KM,King Midas"
                             ";SimplePlay=Simple Play;Yellow Bat=YellowBat"
                             ";EEZE Slot=EEZE")

# The operator whose CS groups these are. A notice whose SUBJECT is one of these
# is the operator's maintenance, not the provider's.
# G4.1 (#94): "Casino-Plus scheduled maintenance ..." - the hyphenated spelling
# matched no name, so the operator's own window was written onto PP's row.
_DEFAULT_OPERATOR_NAMES = "CasinoPlus,Casino Plus,Casino-Plus,IGO,CP"


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
    """The watcher's CLOCK zone: log lines, ledger 'at' stamps, Last Check and
    the card footers. NOT the parse zone - a notice that states no zone is read
    in NOTICE_TZ, by noticeparse. This docstring used to say otherwise, and so
    did .env.example, so VAWATCH_TZ=Asia/Kolkata looked like a way to move
    parsing and changed nothing (F83).

    A name ZoneInfo cannot load falls back to UTC+08:00, and is logged once
    instead of silently.
    """
    name = (os.getenv("VAWATCH_TZ") or "").strip() or "Asia/Manila"
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(name)
    except Exception as err:          # noqa: BLE001
        _warn_config("VAWATCH_TZ", f"VAWATCH_TZ={name!r} cannot be loaded "
                                   f"({type(err).__name__}) - using UTC+08:00")
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

    # One clock: `stale` is decided against the same instant as the watcher's
    # own "still ahead?" rules (see _now_dt). In production the two are the
    # same moment; offline, a test that pins _now_dt had stale decided on the
    # real day instead.
    return noticeparse.classify(text, now=_now_dt())


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


def _row_ident(row: dict) -> str:
    """One Base row's identity: its record_id, else provider@group."""
    return str(row.get("record_id") or "").strip() or \
        f"{_norm(row.get('provider'))}@{_norm(row.get('group'))}"


# The last watch list read in this process, whatever VAWATCH_TARGET_CACHE says.
# handle_messages needs to know which OTHER rows share its group and which other
# providers exist; this lets it know without a Base call.
_LAST_WATCH: dict = {}

# Where the last watch list came from ("base" / "cache" / "fallback"), when it
# was read and why the Base read failed - carried into every sweep's result so
# /vacheck can say the list is stale instead of staying silent (F78).
_LAST_SOURCE: dict = {}


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
        # F79: partition() puts a row tagged TELEGRAM *and* TEAMS in both
        # halves, so a row this watcher reads and fills was also logged under
        # "NO watcher reads". It is left out of that list and named on its own:
        # its Telegram group is watched, only its Teams channel is not read.
        watched = {_row_ident(r) for r in rows}
        both = [r for r in teams if _row_ident(r) in watched]
        teams = [r for r in teams if _row_ident(r) not in watched]
        if rows:
            rep = {"telegram": rows,
                   "teams": [_row_note(r, "APP is TEAMS - this watcher only "
                                          "reads Telegram") for r in teams],
                   "also_teams": [_row_note(r, "APP is TELEGRAM+TEAMS - the "
                                               "Telegram group is watched, the "
                                               "Teams channel is not read")
                                  for r in both],
                   "skipped": [_row_note(r, "skipped by groupcheck")
                               for r in skipped]
                              + [_row_note(r, "no Provider / Games value - "
                                              "nothing to write to")
                                 for r in blank],
                   "source": "base", "at": _now_str(), "error": ""}
            _remember_targets(rep)
            _LAST_WATCH["rep"] = rep
            _LAST_SOURCE.update(source="base", at=rep["at"], error="")
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
        _LAST_SOURCE.update(source="cache", at=cached.get("at") or "", error=err)
        _alert_stale_targets(cached, err)
        return {"telegram": rows,
                "teams": list(cached.get("teams") or []),
                "also_teams": list(cached.get("also_teams") or []),
                "skipped": list(cached.get("skipped") or []),
                "source": "cache", "at": cached.get("at") or "", "error": err}

    print(f"[vawatch] could not read the Base ({err}) and no watch list has "
          f"ever been cached; falling back to the configured single group",
          flush=True)
    single = _chat_title()
    _LAST_SOURCE.update(source="fallback", at=_now_str(), error=err)
    return {"telegram": ([{"provider": _provider(), "group": single}]
                         if single else []),
            "teams": [], "skipped": [], "source": "fallback",
            "at": _now_str(), "error": err}


def _target_cache_max_h() -> int:
    """Hours the watch list may come from the cache before ONE alert. 0 = never.

    VAWATCH_TARGET_CACHE_MAX_H, default 24. The cache has no age limit on
    purpose - dropping it would collapse the rotation to one group - but a
    view that was deleted, or a wrong GROUPCHECK_VIEW_ID, failed every read
    for weeks with only a stdout line per tick, and a provider row added in
    that time was never watched (F78)."""
    return _env_int("VAWATCH_TARGET_CACHE_MAX_H", 24, 0, 24 * 365)


def _parse_now_str(s: Any) -> Optional[datetime]:
    """A ``_now_str()`` stamp back to an aware datetime (the VAWATCH_TZ zone)."""
    try:
        return datetime.strptime(str(s), "%Y-%m-%d %H:%M:%S").replace(tzinfo=_tz())
    except (TypeError, ValueError):
        return None


def _alert_stale_targets(cached: dict, err: str) -> None:
    """Card the Laboratory group ONCE when the cached watch list is too old.

    Once per cached list: the stamp of the list that was alerted on is kept in
    the ledger, and the next GOOD read replaces the list (and its stamp), which
    re-arms the alert. A send that failed is tried again on the next tick."""
    cap = _target_cache_max_h()
    at = _parse_now_str(cached.get("at"))
    if not cap or at is None or _now_dt() - at < timedelta(hours=cap):
        return
    with _ledger_lock:
        d = _load()
        if d.get("_unreadable") or d.get("targets_alerted") == cached.get("at"):
            return
    hours = int((_now_dt() - at).total_seconds() // 3600)
    sent = _post_note(
        "⚠️ VA watcher: the watch list is stale", "orange",
        [f"**The Base's provider list could not be read for {hours} hours.** The "
         f"watcher keeps reading the {len(cached.get('telegram') or [])} group(s) "
         f"it last saw (read at {cached.get('at')}), so a provider row added or "
         f"renamed since then is NOT watched.",
         f"**Last error:** {str(err)[:300]}",
         "Check the maintenance Base's view (GROUPCHECK_VIEW_ID) and the APP "
         "column. This card is sent once; it re-arms after a good read."],
        "(No message text: this note is about the watcher's own provider list.)")
    if sent.get("carded") or sent.get("unknown"):
        with _ledger_lock:
            d = _load()
            if not d.get("_unreadable"):
                d["targets_alerted"] = cached.get("at")
                _save(d)


def targets() -> list:
    """Every Telegram provider group worth watching. See ``watch_list``."""
    return watch_list()["telegram"]


def _log_unwatched(rep: dict) -> None:
    """Say out loud, once per rotation, which Base rows nothing reads."""
    gaps = list(rep.get("teams") or []) + list(rep.get("skipped") or [])
    if rep.get("source") != "base":
        print(f"[vawatch] watch list came from {rep.get('source')} "
              f"(read at {rep.get('at')}): {rep.get('error')}", flush=True)
    both = list(rep.get("also_teams") or [])
    if both:
        # Watched, so not in `gaps` (F79) - but half of the row's APP is unread.
        print(f"[vawatch] {len(both)} Base row(s) tagged TELEGRAM+TEAMS - the "
              f"Telegram group is watched, the Teams channel is not read: "
              + "; ".join(f"{g['provider'] or '(no provider)'} "
                          f"[{g['group'] or 'no group'}]" for g in both),
              flush=True)
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


def get_row(record_id: str) -> dict:
    """GET one record's fields. Read-only; {} when the record has none.

    Used where a write's outcome is not known from its own response: a PUT
    that Lark applied but whose answer timed out was reported as "the Base row
    was NOT updated" on a red card, uncorrected once the notice scrolled out
    (F80), and a cancellation is cleared only while the row still holds the
    cancelled window (F51).
    """
    if not record_id:
        raise ValueError("record_id is required")
    url = (f"{_lark_base()}/open-apis/bitable/v1/apps/{APP_TOKEN}"
           f"/tables/{TABLE_ID}/records/{record_id}")
    data = requests.get(url, headers={"Authorization": f"Bearer {_tenant_token()}"},
                        timeout=30).json()
    if data.get("code") != 0:
        raise RuntimeError(f"Base read failed (code {data.get('code')}): "
                           f"{data.get('msg')}")
    return ((data.get("data") or {}).get("record") or {}).get("fields") or {}


def _cell_ms(value: Any) -> Optional[int]:
    """A bitable DateTime cell as epoch ms, None when empty or unreadable."""
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


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


# #60 / F76: the ledger's containers and the type each must have. Valid JSON of
# the wrong SHAPE - "handled": [], "scopes": null, "baselined": "x" - passed the
# old top-level check, and every sweep then raised AttributeError / TypeError
# for every group, forever: the watcher stayed halted and the file was never
# moved aside. A wrong-typed container is as corrupt as a missing brace.
_LEDGER_SHAPE = {"handled": dict, "order": list, "baselined": dict,
                 "empty_reads": dict, "scopes": dict, "records": dict,
                 "gap_carded": dict, "card_cap": dict, "shared_cards": dict,
                 "targets": dict}


def _check_shape(d: dict) -> None:
    """Raise ValueError when a known container of the ledger has the wrong type."""
    for name, typ in _LEDGER_SHAPE.items():
        if name in d and not isinstance(d[name], typ):
            raise ValueError(f"{name!r} is a {type(d[name]).__name__}, "
                             f"not a {typ.__name__}")
    for bk, sc in (d.get("scopes") or {}).items():
        if not isinstance(sc, dict):
            raise ValueError(f"scope {bk!r} is a {type(sc).__name__}, not a dict")


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
      * it is not UTF-8, not JSON, or not an object -> moved aside to
        vawatch.json.bad, so the evidence survives, and a fresh ledger (every
        group re-baselines with one silent sweep, the same cost as deleting it
        by hand).

    The file is read as BYTES and decoded in the "corrupt" branch on purpose. A
    text-mode read raised UnicodeDecodeError from inside the READ, so a ledger
    hand-edited and saved as GBK counted as a transient read error: every tick
    raised LedgerError and the whole watcher stopped acting, forever, until
    someone found and deleted the file. An undecodable byte is as permanent as
    a missing brace, so it takes the same move-aside-and-rebaseline path.
    """
    try:
        with open(LEDGER_PATH, "rb") as fh:
            raw = fh.read()
    except FileNotFoundError:
        return _fresh_ledger()
    except Exception as err:          # noqa: BLE001
        # Worded for every caller: the sweep's own read halts the tick, but
        # next_target's or gap_read_count's failed read only costs that one
        # answer, and the sweep that follows acts normally (F76) - so this line
        # must not claim the tick did nothing.
        print(f"[vawatch] could not READ the ledger ({err!r}) - this read "
              f"returns nothing; a sweep on it acts on nothing and saves "
              f"nothing", flush=True)
        return dict(_fresh_ledger(), _unreadable=repr(err))
    try:
        d = json.loads(raw.decode("utf-8"))
        if not isinstance(d, dict):
            raise ValueError(f"top level is a {type(d).__name__}, not an object")
        _check_shape(d)
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


def _note_evicted(d: dict, key: str) -> None:
    """Remember the highest mid each scope has had evicted (its "evicted" mark).

    The per-scope floor in _prune protects only scopes read in the last 30
    days. A group whose title failed to open for longer (or a long pause) lost
    every key, even the ones still on screen, and its already-filled notice
    came back as NEW when the group was read again: a second write and a second
    "Scheduled maintenance" card (F40, found after the LRU fix). Mids only grow
    within one chat, so an unseen bubble at or below this mark existed when its
    evicted neighbour was read - it is history, never a new message, and
    handle_messages records it exactly like the cold-start floor.
    """
    s = _scope_of(key)
    m = _KEY_MID_RE.search(key[len(s):]) if s else None
    n = _mid_num(m.group(1)) if m else None
    if n is None:
        return
    sc = _scope(d, s)
    prev = sc.get("evicted")
    if prev is None or n > float(prev):
        sc["evicted"] = n


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
                _note_evicted(d, k)
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
                        "also_teams": list(rep.get("also_teams") or []),
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
    # The confirm-before-write model could not be reached (cold, down,
    # unreadable answer): the notice comes back round, then goes to a person.
    "confirm-pending": lambda: _confirm_attempts(),
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


def _zone_label(dt: datetime) -> str:
    """A datetime's zone as a numeric offset: "GMT+8", "GMT+5:30", "UTC".

    ``%Z`` printed Asia/Manila as its tzdata abbreviation "PST" - Philippine
    Standard Time to a reader in Manila, US Pacific (UTC-8) to noticeparse,
    which reads "PST" in a notice as UTC-8 - and a bare "UTC+08:00" for a
    notice's fixed offset, so one card could say both (F74). An offset
    cannot be misread either way.
    """
    off = dt.utcoffset() if isinstance(dt, datetime) else None
    if off is None:
        return ""
    mins = int(off.total_seconds() // 60)
    if mins == 0:
        return "UTC"
    h, m = divmod(abs(mins), 60)
    return f"GMT{'+' if mins > 0 else '-'}{h}" + (f":{m:02d}" if m else "")


def _base_zone(dt: datetime) -> datetime:
    """``dt`` in the zone the Base displays (VAWATCH_TZ, default Asia/Manila)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_tz())
    return dt.astimezone(_tz())


def _fmt_win(start: Optional[datetime], end: Optional[datetime]) -> str:
    """A window as the Base row SHOWS it: in the Base's zone, with the end
    date whenever it is not the start date, and the zone named.

    It used to print the notice's own zone with no label and the end as a bare
    clock, so "10:00 - 12:00 UTC" was reported as "Base row updated ·
    10:00 → 12:00" while the row displays 18:00 → 20:00, and a window ending
    the next day read as ending the same day (F74). The stored moments were
    always right; only this summary misstated them.
    """
    if not (start and end):
        return "(window unknown)"
    s, e = _base_zone(start), _base_zone(end)
    body = (f"{s:%Y-%m-%d %H:%M} → {e:%H:%M}" if s.date() == e.date() else
            f"{s:%Y-%m-%d %H:%M} → {e:%Y-%m-%d %H:%M}")
    return f"{body} {_zone_label(s)}".rstrip()


def _later_windows(verdict: dict) -> list:
    """The further windows a fill notice states that the row does NOT get.

    noticeparse returns them in ``others`` ("Phase 1: 09-23 22:00-23:59 /
    Phase 2: 09-25 00:00-02:00" fills phase 1): separate outages, never
    merged. The row holds one window at a time.
    """
    return [(s, e) for s, e in (verdict.get("others") or [])
            if isinstance(s, datetime) and isinstance(e, datetime)]


def _later_note(verdict: dict) -> str:
    """One card line naming the windows ``_later_windows`` finds, or "".

    F67: the later window was never written and nothing said so - the card
    announced phase 1 only, the message was ledgered 'filled' for good, and
    phase 2 reached the sheet only if the provider happened to post again.
    It is not written automatically here either: whether it still stands
    once phase 1 is over is for a person to judge, and a wrong window on the
    shared sheet is worse than one entered by hand a day later.
    """
    later = _later_windows(verdict)
    if not later:
        return ""
    shown = "; ".join(_fmt_win(s, e) for s, e in later[:5]) + (
        f" … and {len(later) - 5} more" if len(later) > 5 else "")
    return (f"⚠️ **The notice also states {len(later)} later window(s), NOT "
            f"written:** {shown}. The row holds one window at a time - enter "
            f"the next one by hand once this one has ended.")


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


def _set_owner(sc: dict, *, mid: str, key: str, verdict: dict, kind: str,
               text: str = "") -> None:
    sc["owner"] = {"mid": str(mid or ""), "kind": kind, "key": key,
                   "start": _iso(verdict.get("start")) if kind == "fill" else None,
                   "end": _iso(verdict.get("end")) if kind == "fill" else None,
                   "resched": bool(verdict.get("reschedule")), "at": _now_str(),
                   # #90: the notice's own text, so a window a sooner outage
                   # displaces can be written back later with its Remark.
                   **({"text": str(text)[:4000]} if kind == "fill" and text else {})}
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
    if len(d.get("order") or []) >= _LEDGER_CAP:
        # A ledger that already hit the cap under the OLD first-insertion trim
        # (bd9fc12 shares this key scheme, so _migrate keeps it) has forgotten
        # keys nobody recorded - the F40 revival would fire once more on the
        # first sweep after deploy. Everything at or below the newest mid this
        # scope has in the ledger was on screen and decided before; a new
        # message is always above it. A scope with no entries is untouched.
        seen = [n for _k, _v, n in _scope_entries(d, bk) if n is not None]
        if seen:
            prev = sc.get("evicted")
            sc["evicted"] = max(seen + ([float(prev)] if prev is not None else []))
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


# The outcomes that put a message in front of a PERSON about this row: a
# needs-human card (sent, switched off, or still to be retried), a deferred
# second outage, and a "no maintenance" whose automatic clear is off. Each card
# invites a hand edit of the row. So does a poster carded as a possible notice
# (F59): the person may fill the row from the image.
_ASKED_OUTCOMES = frozenset({"needs-human", "card-failed", "deferred",
                             "clear-skipped", "image-carded"})
# ...and the one FINAL outcome that says the maintenance is off without a card:
# a cancellation that found no window on the row to card against ("Update:
# maintenance cancelled." while the row was still empty, F34/F35). It must
# still stop an OLDER fill: the older notice A whose write failed, or which
# rendered late, was retried after that cancellation and put the cancelled
# window on the sheet with a confident card - the cancellation itself, being
# final, was never looked at again.
# #230 / #277: "retracted" is the same for a "Sorry, wrong group" that found no
# notice of the row to card against: an older notice rendering later, or a
# deferred one, must not be written past it.
_BLOCKING_OUTCOMES = _ASKED_OUTCOMES | {"cancelled", "retracted"}


def _newer_ask(d: dict, bk: str, n: float, skip: tuple = ()) -> Optional[tuple]:
    """(raw mid, entry) of the newest message in this scope NEWER than mid `n`
    that was carded to a human - or that cancelled maintenance - or None.
    Outcomes in ``skip`` do not count.

    The owner rule only knows what this watcher WROTE. The F34/F35 variant it
    missed: the older notice A's write failed (or A rendered late), the newer B
    was a needs-human card, a person fixed the row by hand from that card - and
    A's retry, or its late first sighting, then wrote A's window over the fix.
    A newer B that CANCELLED maintenance while the row was empty is the same
    trap without the card (see _BLOCKING_OUTCOMES).
    """
    best = None
    for k, v, m in _scope_entries(d, bk):
        oc = str(v.get("outcome") or "")
        if m is None or m <= n or oc not in _BLOCKING_OUTCOMES or oc in skip:
            continue
        if best is None or m > best[0]:
            raw = _KEY_MID_RE.search(k[len(bk):])
            best = (m, raw.group(1) if raw else f"{m:g}", v)
    return (best[1], best[2]) if best else None


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
    r"(?i:\bcancel\w*|\bcalled\s+off\b|\bwill\s+not\s+(?:be\s+)?(?:proceed|go\s+ahead|take\s+place)"
    # F51: "the maintenance notice has been withdrawn / aborted / is no longer
    # required" got the parser's cancelled verdict but matched none of the words
    # above, so the row kept the window with nobody told. "withdrawn" only as
    # the participle: "player withdrawal failed" is CS chat, not a cancellation.
    r"|\bwithdrawn\b|\baborted\b|\brevoked\b|\bscrapped\b"
    r"|\bno\s+longer\s+(?:required|needed|necessary|going\s+ahead))"
    r"|取消|撤销|撤銷|撤回|作废|作廢|不再进行|不再進行|不再需要")
# "...is NOT cancelled and will proceed as scheduled" must not read as one.
_CANCEL_NEG_RE = re.compile(
    r"(?i:\b(?:not|never|n't)\s+(?:been\s+|be\s+)?(?:cancel\w*|called\s+off))"
    r"|不会取消|不會取消|未取消|没有取消|沒有取消|不取消")

# #70 / #190: words that withdraw a window but are too common to count on
# their own - "is no longer valid", "will not happen", "is on hold", "has been
# suspended", "is off", "dropped", 不进行了, 维护暂停, 无效. The parser gave each
# its cancelled verdict, but none matched _CANCEL_TEXT_RE, so the cancellation
# was treated as a completion: no card while the row kept the window. They
# count only together with that verdict: "games will be suspended during the
# maintenance" is how a notice describes its outage.
_CANCEL_WEAK_RE = re.compile(
    r"(?i:\bno\s+longer\s+(?:valid|happening|applicable|taking\s+place)\b|\binvalid\b"
    r"|\bvoid(?:ed)?\b|\b(?:will\s+not|won['’]?t|is\s+not\s+going\s+to)\s+happen\b"
    r"|\bon\s+hold\b|\bsuspended\b|\bis\s+off\b|\bdropped\b"
    # #230: "Update: we will NOT have maintenance on Friday."
    r"|\b(?:will|shall)\s+not\s+have\s+(?:any\s+|the\s+)?maintenance\b"
    r"|\bwon['’]?t\s+have\s+(?:any\s+|the\s+)?maintenance\b)"
    r"|不进行了|不進行了|不进行|不進行|暂停|暫停|无效|無效|作罢|作罷")


# F56 (#208, #209): a short bubble saying the maintenance is OVER - "Done,
# thanks for waiting.", "All good now, games are up.", "Maintenance done ✅",
# 已恢复. noticeparse reads most of them as "no scheduled-maintenance wording",
# so /vacheck force walked past them and re-wrote the finished window a person
# had just blanked. Only short bubbles (see _is_done_text): inside a notice
# "back online" describes the end of its own window.
_DONE_TEXT_RE = re.compile(
    r"(?i:^\W*(?:the\s+)?(?:(?:maintenance|mtc|mt|upgrade)\s+)?(?:is\s+|has\s+been\s+)?"
    r"(?:all\s+)?(?:done|completed?|finished|over|resolved)\b"
    r"|\ball\s+good\s+now\b|\b(?:games?|site|services?|servers?|everything|all)\s+"
    r"(?:is|are)\s+(?:now\s+)?(?:back\s+)?(?:up|online|live|normal)\b"
    r"|\bback\s+(?:online|up|to\s+normal)\b|\bthanks?\s+(?:you\s+)?for\s+(?:your\s+)?"
    r"waiting\b)"
    # Not "thank you for your patience" and not a bare ✅: both are said as
    # often when a window STARTS (or as an acknowledgement) as when it ends.
    r"|已恢复|已恢復|恢复正常|恢復正常|已完成|维护完成|維護完成|维护结束|維護結束")


def _is_done_text(text: str) -> bool:
    t = (text or "").strip()
    return len(t) <= 120 and bool(_DONE_TEXT_RE.search(t))


def _is_cancel_text(text: str, why: str) -> bool:
    """A cancellation - a strong cancel word next to maintenance (or with the
    parser's cancelled verdict), or a weak one WITH that verdict (see above)."""
    t = text or ""
    if _CANCEL_NEG_RE.search(t):
        return False
    if _CANCEL_TEXT_RE.search(t) and (_CANCEL_REASON_RE.search(why or "")
                                      or _about_maintenance(t)):
        return True
    return bool(_CANCEL_WEAK_RE.search(t) and _CANCEL_REASON_RE.search(why or ""))


def _names_day(text: str, dt: Optional[datetime]) -> bool:
    """Does ``text`` name the calendar day of ``dt`` (dd/mm, mm/dd, 9月24日,
    "24 Sep", "Sep 24")? Both orders are tried; see _names_owner_day."""
    if dt is None:
        return False
    dd, mm = dt.day, dt.month
    mon = dt.strftime("%b")
    pats = (rf"(?<![\d/.-])0?{dd}[/.-]0?{mm}(?![\d/.-]\d{{1,2}}\b)(?![\d])",
            rf"(?<![\d/.-])0?{mm}[/.-]0?{dd}(?![\d])(?![/.-]\d{{1,2}}\b)",
            rf"(?<!\d)0?{mm}\s*月\s*0?{dd}\s*[日号號]",
            rf"(?<!\d)0?{dd}(?:st|nd|rd|th)?\s+(?i:{mon})",
            rf"(?i:{mon})\w*\.?\s+0?{dd}(?!\d)",
            rf"{dt.year}\s*[-/.年]\s*0?{mm}\s*[-/.月]\s*0?{dd}(?!\d)")
    return any(re.search(p, text or "") for p in pats)


_MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}
_DAY_TOKEN_RE = re.compile(
    r"(?<!\d)(?P<y1>\d{4})\s*[-/.年]\s*(?P<m1>\d{1,2})\s*[-/.月]\s*(?P<d1>\d{1,2})(?!\d)"
    r"|(?<![\d/.-])(?P<a2>\d{1,2})[/.-](?P<b2>\d{1,2})[/.-](?P<y2>\d{2,4})(?!\d)"
    r"|(?<![\d/.:-])(?P<a3>\d{1,2})/(?P<b3>\d{1,2})(?![\d/:])"
    r"|(?<!\d)(?P<m4>\d{1,2})\s*月\s*(?P<d4>\d{1,2})\s*[日号號]"
    r"|(?i:(?<![A-Za-z])(?P<mn5>jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?"
    r"\s+(?P<d5>\d{1,2})(?:st|nd|rd|th)?(?!\d))"
    r"|(?i:(?<!\d)(?P<d6>\d{1,2})(?:st|nd|rd|th)?\s+(?P<mn6>jan|feb|mar|apr|may|jun|jul"
    r"|aug|sep|oct|nov|dec)[a-z]*)")


def _days_named(text: str) -> list:
    """audit-2: every calendar day a text names, as a set of (month, day)
    readings per date - dd/mm and mm/dd both, when both are valid."""
    out = []
    for m in _DAY_TOKEN_RE.finditer(text or ""):
        g = m.groupdict()
        cands = []
        if g["m1"]:
            cands = [(int(g["m1"]), int(g["d1"]))]
        elif g["a2"]:
            a, b = int(g["a2"]), int(g["b2"])
            cands = [(b, a), (a, b)]
        elif g["a3"]:
            a, b = int(g["a3"]), int(g["b3"])
            cands = [(b, a), (a, b)]
        elif g["m4"]:
            cands = [(int(g["m4"]), int(g["d4"]))]
        elif g["mn5"]:
            cands = [(_MONTHS[g["mn5"].lower()[:3]], int(g["d5"]))]
        elif g["mn6"]:
            cands = [(_MONTHS[g["mn6"].lower()[:3]], int(g["d6"]))]
        ok = {(mo, dy) for mo, dy in cands if 1 <= mo <= 12 and 1 <= dy <= 31}
        if ok:
            out.append(ok)
    return out


# audit-2: an EXTENSION of a window ("... is extended until 14:00", 延长至).
_EXTEND_RE = re.compile(r"(?i:\bextend(?:ed|s|ing)?\b|\bprolong\w*)|延长|延長|延至")


def _about_other_outage(text: str, verdict: dict, row_start: Optional[datetime]) -> str:
    """G3.1 (audit-2): why a RESCHEDULE is about an outage other than the one
    the row holds, or "".

    "The maintenance on 28/09 is rescheduled to 29/09" and "The maintenance on
    28/09 10:00-12:00 is extended until 14:00" arrived while the row held an
    imminent 25/09 and the 28/09 outage was deferred behind it: the reschedule
    flag skipped the deferral, and the row's 25/09 vanished - the G3.1 failure
    by another door. A reschedule that names the row's own day moves the row's
    window; one that extends a window, or names a day that is neither the
    row's nor its own new window's, is about another outage. A bare
    "rescheduled to 01/10" names neither and is left to the caller.
    """
    if row_start is None or _names_day(text, row_start):
        return ""
    if _EXTEND_RE.search(text or ""):
        return "it extends a window the row does not hold"
    vs, ve = verdict.get("start"), verdict.get("end")
    own = {(x.month, x.day) for x in (vs, ve) if x is not None}
    for reading in _days_named(text):
        if not (reading & own):
            mo, dy = sorted(reading)[0]
            return f"it moves the maintenance of another day ({dy:02d}/{mo:02d})"
    return ""


def _cancels_day(text: str, dt: Optional[datetime]) -> bool:
    """G2.2 (#74, #76): does one sentence of ``text`` both name ``dt``'s day
    and cancel it ("Previously announced 24/09/2026 10:00-12:00 is cancelled",
    24/09维护取消，改为 …)?"""
    if dt is None:
        return False
    for sent in _SENTENCE_SPLIT_RE.split(text or ""):
        if (sent and _names_day(sent, dt) and not _CANCEL_NEG_RE.search(sent)
                and (_CANCEL_TEXT_RE.search(sent) or _CANCEL_WEAK_RE.search(sent))):
            return True
    return False


# F58: is a message ABOUT maintenance at all? noticeparse hands back
# needs_human for ANY "postponed" / "reschedule" with no window, follow_quote for
# ANY short "please see below" / 详见附件 / 请见截图, and the passive watcher
# carded every one of them red - "The payout for player 88231 is postponed",
# "Can we reschedule the call?", "玩家投诉，详见下方截图" - in two-way CS groups
# where those come up daily. A verdict straight from classify is carded only
# when the text names maintenance (this list, or noticeparse's own gate). It
# is a subject test, not a notice test: "the maintenance has been postponed"
# passes, "the payout is postponed" does not.
_MAINT_TOPIC_RE = re.compile(
    r"(?i:\bmaint(?:enance|\.)|\bdown\s*time\b|\boutages?\b"
    r"|\b(?:system|server|platform)\s+upgrade\b)"
    r"|维护|維護|维修|維修|停机|停機|停服|停止服务|停止服務"
    r"|系统升级|系統升級|服务器升级|服務器升級|伺服器升級|平台升级|平台升級")


def _about_maintenance(text: str) -> bool:
    if _MAINT_TOPIC_RE.search(text or ""):
        return True
    try:
        import noticeparse
        return bool(noticeparse._sched_re().search(noticeparse._norm(text or "")))
    except Exception:                 # noqa: BLE001 - a private helper; the list above stands alone
        return False


# F58: "no maintenance" as a MODIFIER, not the answer "nothing is planned".
# "The hotfix will be deployed tonight with no maintenance downtime" classified
# as clear: a red card by default, and with VAWATCH_CLEAR_ENABLED=1 it would
# BLANK a row holding a real window. Deliberately narrow - "no maintenance
# required this week" is an answer and still clears.
_NOT_A_CLEAR_RE = re.compile(
    r"(?i:\bno\s+(?:scheduled\s+)?maintenance\s+(?:downtime|down\s*time|impact"
    r"|interruption)\b|\bwith(?:out|\s+no)\s+(?:any\s+)?(?:scheduled\s+)?maintenance\b"
    # F58: a CS agent answering a player-issue ticket - "no maintenance on our
    # side, please check your network", "no maintenance issue on our side",
    # "no maintenance was done on the server, the issue is on the operator
    # side", "no maintenance ongoing, please retry". Each said "no maintenance"
    # and was carded red (and with VAWATCH_CLEAR_ENABLED=1 cleared the row)
    # although nobody asked about a schedule. The tell is the troubleshooting
    # that follows or the word "issue"; the weekly answer "No maintenance on our
    # side this week" has neither and still clears.
    r"|\bno\s+(?:scheduled\s+)?maintenance\s+(?:issues?|problems?)\b"
    r"|\bno\s+maintenance\s+(?:was|has\s+been|is\s+being)\s+(?:done|performed"
    r"|carried\s+out|conducted)\b"
    # #210: "no maintenance from our end, kindly check with your IT", "No
    # maintenance at our end, please contact your IT team", "There is no
    # maintenance currently, please check your connection" - the same ticket
    # reply with another preposition, and "contact / your IT" as the advice.
    r"|\b(?:there\s+is\s+|there['’]s\s+|we\s+have\s+)?no\s+(?:ongoing\s+)?maintenance\s+"
    r"(?:(?:is\s+)?ongoing|(?:on|from|at)\s+our\s+(?:side|end|part)|on\s+our\s+part"
    r"|at\s+the\s+moment|right\s+now|currently|now|at\s+present)\b"
    r"[^.\n]*(?:\bcheck\b|\bretry\b|\btry\s+again\b"
    r"|\bnetwork\b|\brunning\s+normal(?:ly)?\b|\bworking\s+(?:fine|normal(?:ly)?)\b"
    r"|\bissue\b|\bproblem\b|\boperator\b|\bconnection\b|\bcache\b|\brefresh\b"
    r"|\bcontact\b|\byour\s+(?:IT|tech|team|side|end|provider)\b|\bclear\b|\brestart\b))"
    r"|无需停机|無需停機"
    r"|没有维护[^。\n]*(?:检查|网络|重试|正常|问题|缓存|刷新)"
    r"|沒有維護[^。\n]*(?:檢查|網絡|網路|重試|正常|問題|緩存|刷新)")


# G2.3: a short follow-up that withdraws or corrects the notice above it -
# "Sorry wrong group", "Please ignore the previous message", "Correction: the
# date should be 25/09", 请忽略上一条消息，发错群了, 更正：… It names no window
# (or only the corrected part), fails the wording gate, and was dropped as
# "not about maintenance" while the row kept the withdrawn window. Used only
# while the row's own notice is still on screen above it (see handle_messages).
_FOLLOWUP_RE = re.compile(
    r"(?i:\bcorrect(?:ion|ed)\b|\bcorrect\s+(?:time|date|window|schedule)\b"
    r"|\btypo\b|\bamend(?:ed|ment)?\b|\bmistaken?\b|\bby\s+mistake\b"
    r"|\bwrong\s+(?:group|chat|channel|merchant|partner|date|time|notice|message|post)\b"
    r"|\b(?:ignore|disregard)\b|\bshould\s+(?:be|read)\b|\binstead\s+of\b"
    r"|\bnot\s+(?:on\s+)?\d{1,2}[/.-]\d{1,2}\b"
    # "Update: postponed, new time TBA." under the notice names no maintenance,
    # so the F58 gate would drop what was always a needs-human card - and the
    # row would keep the withdrawn window. Cancel words are NOT here: a
    # cancellation is F51's rule (a maintenance word, or the parser's own
    # cancelled verdict), and "the player cancelled the withdrawal" is not one.
    r"|\bpostpon\w*|\breschedul\w*|\bdelayed\b"
    # G2.3 (#77-#80): "The notice above is void", "The above notice is
    # invalid", "The previous notice is no longer valid", "Sorry, that was a
    # test", 以上公告无效 - a withdrawal in other words; each was dropped as
    # "not about maintenance" while the row kept the withdrawn window.
    r"|\bvoid(?:ed)?\b|\binvalid\b|\bno\s+longer\s+(?:valid|applicable|stands?)\b"
    r"|\b(?:that|this|it|the\s+above(?:\s+(?:one|message|notice))?)\s+(?:was|is)\s+"
    r"(?:just\s+|only\s+)?a\s+test\b|\btest(?:ing)?\s+(?:message|msg|notice|post)\b"
    r"|\bfor\s+testing\s+only\b"
    # F56 (#207): "Sorry, this was meant for another group".
    r"|\b(?:meant|intended|supposed\s+to\s+go)\s+(?:for|to)\s+(?:an)?other\s+"
    r"(?:group|chat|channel|merchant|partner|operator|client)\b"
    r"|\bwrong\s+(?:info(?:rmation)?|recipient|details?)\b)"
    r"|更正|有误|有誤|勘误|勘誤|发错|發錯|错发|錯發|忽略|无视|無視|作废|作廢"
    r"|无效|無效|测试消息|測試消息|测试通知|測試通知|是测试|是測試"
    r"|应为|應為|而不是|延期|延后|延後|改期|推迟|推遲")

# G2.3 (#81): a follow-up that names THE NOTICE ("以上通知作废", "Please ignore
# the notice I sent earlier") is about the row's notice even after that notice
# has scrolled off the read - it used to need a maintenance word or the row's
# own date.
_NOTICE_NOUN_RE = re.compile(
    r"(?i:\bnotice\b|\bannouncement\b|\bnotification\b)|公告|通知|通告")

# F56 / F34 (#206, #207, #229, #230, #277): a short follow-up that plainly takes
# back the sender's own last post - "Sorry, wrong group", "Oops, wrong chat",
# "this was meant for another group", 发错群了. It points at nothing, so the
# object rule above does not apply to it; it withdraws whatever the provider
# posted just before, which is the notice when one is there.
_SELF_RETRACT_RE = re.compile(
    r"(?i:\bwrong\s+(?:group|chat|channel|gc|room)\b|\bnot\s+(?:for\s+)?this\s+"
    r"(?:group|chat|channel)\b|\b(?:meant|intended|supposed\s+to\s+go)\s+(?:for|to)\s+"
    r"(?:an)?other\s+(?:group|chat|channel|merchant|partner|operator|client)\b"
    r"|\bsent\s+(?:it\s+|this\s+)?(?:here\s+)?by\s+mistake\b)"
    r"|发错群|發錯群|错群|錯群|发错了|發錯了|发错地方|發錯地方")


# G2.3: what a follow-up must point AT, unless it sits directly under the row's
# notice: the notice itself ("the notice above", 上一条, 之前的公告), maintenance,
# or the date / time being corrected. "Correction: the bonus amount should be
# 500" three bubbles below a notice is about the bonus.
_FOLLOWUP_OBJECT_RE = re.compile(
    r"(?i:\bnotice\b|\bannouncement\b|\bnotification\b|\bprevious\b|\babove\b"
    r"|\bearlier\b)|公告|通知|通告|上一条|上一條|上面|上方|之前|前面"
    r"|\d{1,2}\s*[:：]\s*\d{2}|\b\d{1,2}[/.]\d{1,2}\b|\d{1,2}\s*月|\d{1,2}\s*[日号號]")


def _names_owner_day(text: str, owner: Optional[dict]) -> bool:
    """Does ``text`` name the calendar day of the window the row holds?

    G2.3: a correction posted after the notice has scrolled off the read
    ("Correction: the maintenance date should be 25/09 instead of 24/09") can
    still be tied to the row's window by the day it names. dd/mm and mm/dd are
    both tried - a day named either way about the row's own date is a follow-up
    about that window, and a card is the safe reading. A legacy owner (window
    unknown) never matches.
    """
    os_, _oe = _owner_window(owner)
    if os_ is None:
        return False
    d, m = os_.day, os_.month
    mon = os_.strftime("%b")
    pats = (rf"(?<![\d/.-])0?{d}[/.-]0?{m}(?![\d/.-])",
            rf"(?<![\d/.-])0?{m}[/.-]0?{d}(?![\d/.-])",
            rf"(?<!\d)0?{m}\s*月\s*0?{d}\s*[日号號]",
            rf"(?<!\d)0?{d}(?:st|nd|rd|th)?\s+(?i:{mon})",
            rf"(?i:{mon})\w*\.?\s+0?{d}(?!\d)")
    return any(re.search(p, text or "") for p in pats)


def _points_at_attachment(msgs: list, i: int) -> str:
    """What a "see below" / 详见附件 bubble points AT, or '' when nothing.

    R1.79: the F58 wording gate asked every follow_quote verdict to name
    maintenance, so a DOCUMENT captioned only 详见附件, or "Please see below"
    followed by a caption-less poster, was dropped silently - bd9fc12 carded
    both. A pointer whose attachment is right there is not ordinary chat: the
    bubble itself is a document, or the provider's very next bubble is a
    caption-less photo/document/video. A pointer with no attachment beside it
    ("please check the screenshot above" in a ticket thread) stays chat.
    """
    m = msgs[i]
    kind = str(m.get("kind") or "")
    if re.search(r"document", kind, re.I):
        return f"this {kind} bubble"
    # #297: the attachment was accepted only as the VERY next bubble and only
    # caption-less, so "Please see below" + a photo captioned "👇" / "Notice",
    # our own reply between the pointer and the photo, or a photo FIRST and
    # "Please see above" after it, all ended silently (bd9fc12 carded them).
    # Now: up to two bubbles away in either direction, skipping our own and
    # empty bubbles; a provider's text bubble in between ends that direction;
    # a short caption with no clock is still "just the attachment". A ticket
    # screenshot ABOVE a "please check the screenshot above" stays chat (F58).
    t = str(m.get("text") or "")
    dirs = [(range(i + 1, min(len(msgs), i + 4)), "below")]
    if not re.search(r"(?i:screen[ \t]*shots?)|截图|截圖", t):
        dirs.append((range(i - 1, max(-1, i - 4), -1), "above"))
    for rng, where in dirs:
        steps = 0
        for j in rng:
            nx = msgs[j]
            cap = str(nx.get("text") or "").strip()
            if _is_out(nx) or (not cap and not _is_poster(nx)):
                continue              # our reply, a sticker, an empty bubble
            steps += 1
            if steps > 2:
                break
            if _is_poster(nx):
                if not cap or (len(cap) <= 40
                               and not re.search(r"\d{1,2}\s*[:：]\s*\d{2}", cap)):
                    return (f"the {nx.get('kind')} {where} it"
                            + ("" if not cap else f" (captioned “{cap[:40]}”)"))
                break
            break                     # a provider's text bubble: not an attachment
    return ""


# F59: words a header-only maintenance bubble uses to point at where the
# details are - an attachment, a poster, the message below it.
_POINTER_RE = re.compile(
    r"(?i:\battach(?:ed|ment)s?\b|\bbelow\b|\babove\b|\bimage\b|\bposter\b"
    r"|\bpicture\b|\bscreenshot\b|\bpdf\b|\bfile\b|\bfollowing\b)"
    r"|附件|如图|如圖|下图|下圖|上图|上圖|图片|圖片|海报|海報|以下|如下|下方|上方")


# G1.3 (#57): a time that is not a clock - a duration, "in 30 minutes", "now",
# 下午4点, "in progress". Read only on an unparsed maintenance bubble at its
# last retry, to decide whether a person is told.
_TIMEISH_RE = re.compile(
    r"(?i:\bin[ \t]+\d+[ \t]*(?:min(?:ute)?s?|hours?|hrs?)\b"
    r"|\b(?:for|about|around|approx(?:imately)?\.?|~|eta|within)[ \t]*\d+(?:\.\d+)?[ \t]*"
    r"(?:min(?:ute)?s?|hours?|hrs?|h)\b"
    r"|\b\d+(?:\.\d+)?[ \t]*(?:min(?:ute)?s?|hours?|hrs?)\b"
    r"|\b\d{1,2}[ \t]*(?:am|pm)\b|\b(?:right[ \t]+)?now\b|\bongoing\b|\bin[ \t]+progress\b"
    r"|\bimmediately\b|\bstarting[ \t]+soon\b)"
    r"|(?:上午|下午|中午|晚上|凌晨|早上)?[ \t]*\d{1,2}[ \t]*[点點时時](?:半|\d{1,2}分?)?"
    r"|约?[ \t]*\d+(?:\.\d+)?[ \t]*(?:个|個)?(?:小时|小時|分钟|分鐘)"
    r"|现在|現在|立即|马上|馬上|正在|进行中|進行中|即将|即將")


def _split_notice_hint(msgs: list, i: int, text: str, now: datetime) -> str:
    """Why an 'unparsed' maintenance bubble at its LAST retry still deserves a
    card, or '' when it is just chat about maintenance.

    F59: providers post "【Maintenance Notice】" in one bubble and "Date: ...
    Time: ..." in the next, or "Maintenance notice: please see attached" with
    the schedule in a PDF. Each half alone is unreadable - the header has no
    clock (undated == 0), the detail no maintenance word - so both ended
    'unparsed' three times and vanished, while the G1.3 cap card only knew
    about clocks. Read together with the provider's bubble right beside it, the
    pair may parse as a notice: that window goes on the card, never on the
    row (joining bubbles is a guess). A header pointing at an attachment is
    carded as such. A neighbour that is a readable notice on its own is left
    alone - it was, or will be, written itself.
    """
    if not _about_maintenance(text):
        return ""
    if any(0 <= j < len(msgs) and _is_poster(msgs[j]) and not _is_out(msgs[j])
           for j in (i + 1, i - 1)):
        # "Maintenance notice, see image above" under a poster: the poster
        # rule has carded that image with this text as its hint. A second
        # card here would be the two-cards-per-notice F59 complains of.
        return ""

    def _txt(j):
        return str(msgs[j].get("text") or "").strip()

    def _inbound(j):
        return 0 <= j < len(msgs) and not _is_out(msgs[j]) and bool(_txt(j))

    # F59 (#132-#135): the join used to take only the bubble right beside
    # this one. "noted" between the header and the detail, our own reply or a
    # sticker between them, or a notice in THREE bubbles (header / date /
    # time) each ended silently. The next two provider text bubbles within
    # three places, either way, are tried alone and together; our own and
    # empty bubbles are skipped.
    below = [j for j in range(i + 1, min(len(msgs), i + 4)) if _inbound(j)][:2]
    above = [j for j in range(i - 1, max(-1, i - 4), -1) if _inbound(j)][:2]
    combos = [[j] for j in below] + [[j] for j in above]
    if len(below) == 2:
        combos.append(below)
    if len(above) == 2:
        combos.append(above)
    for combo in combos:
        try:
            if any(classify(_txt(j)).get("start") for j in combo):
                continue              # a readable notice on its own: not a half
            idx = sorted([i] + combo)
            jtext = "\n".join(_txt(k) for k in idx)
            joined = classify(jtext)
        except Exception:             # noqa: BLE001 - unreadable neighbour: not a hint
            continue
        ws, we = joined.get("start"), joined.get("end")
        if joined.get("action") in ("fill", "needs_human") or joined.get("undated"):
            if not (ws and we):
                # A needs_human names the window in its reason but may leave
                # start/end empty ("maintenance wording and a window ..."); a
                # join the parser leaves 'undated' (#135: "Games affected:
                # all" before Start/End) may still hold one: read it off the
                # joined text for the card.
                try:
                    import noticeparse
                    w = noticeparse.find_window(jtext, now=now)
                except Exception:     # noqa: BLE001
                    w = None
                ws, we = w if w else (None, None)
            where = ("below" if min(combo) > i else "above" if max(combo) < i
                     else "around")
            n_ = len(combo)
            if not (ws and we) and joined.get("undated") and _days_named(jtext):
                # #135: the parser reads a date and a clock in the join but no
                # window ("Games affected: all" breaks the Start/End lines).
                return (f"**Read together with the "
                        f"{'bubble' if n_ == 1 else 'two bubbles'} {where} it, this "
                        f"names maintenance with a date and a time the parser "
                        f"cannot read as a window** — split over "
                        f"{'two' if n_ == 1 else 'three'} messages, so nothing was "
                        f"written. Open the group and fill by hand.")
            if ws and we:
                return (f"**Read together with the "
                        f"{'bubble' if n_ == 1 else 'two bubbles'} {where} it, this "
                        f"is a maintenance notice for {_fmt_win(ws, we)}** — split "
                        f"over {'two' if n_ == 1 else 'three'} messages, so nothing "
                        f"was written. Verify and fill by hand.")
    if _POINTER_RE.search(text):
        return ("**It names maintenance and points at an attachment or another "
                "message** the parser cannot read — open the group; nothing was "
                "written.")
    return ""


def _newer_retraction_in_read(msgs: list, nums: list, i: int,
                              now: datetime, started: bool = False) -> Optional[tuple]:
    """(mid, what it did, snippet) of the newest bubble in THIS read, newer than
    bubble ``i``, that cancels or retracts maintenance without stating a window
    of its own - or None.

    Messages run oldest first, so notice A was written before its retraction B
    two bubbles below was even looked at, and B then only produced a card over
    a row already holding the withdrawn window (F34/F35 'disregard', F56). The
    fill has to know about B BEFORE it writes. Only the provider's own bubbles
    count (a question we asked is not a retraction), and a B that states a
    window is a reschedule or a second notice - the owner rule decides between
    those, not this. "should be finished by 12:00" is a comment on the notice,
    not a correction, so the follow-up word "should be" is not taken here.

    Three more shapes, each of which let the notice be written and only THEN
    carded (F56, F34):
      * #206/#207/#229: "Sorry wrong group", "Oops, wrong chat", "this was meant
        for another group", 发错群了 - a retraction of the sender's own last
        post that names no notice (_SELF_RETRACT_RE);
      * #231: "Correction: the maintenance is on 26/09 ..., not 25 Sep." - a
        correction the parser does not read as a notice, so it is not a fill
        and the owner rule never sees it; it states the CORRECTED window, which
        is why the window test above skipped it;
      * #209: "Maintenance completed early. All games are back online." under a
        notice whose window has already STARTED (``started``) - the newest word
        says it is over.
    """
    if i >= len(nums) or nums[i] is None:
        return None
    best = None
    for j, m in enumerate(msgs):
        nj = nums[j] if j < len(nums) else None
        if j == i or nj is None or nj <= nums[i] or _is_out(m):
            continue
        t = str(m.get("text") or "").strip()
        if not t or _CANCEL_NEG_RE.search(t):
            continue
        try:
            v = classify(t)
        except Exception:             # noqa: BLE001 - one unreadable bubble is not a retraction
            continue
        if v.get("action") == "fill" or v.get("start"):
            continue
        why = str(v.get("reason") or "")
        kind = ""
        if _is_cancel_text(t, why):
            kind = "CANCELLED"
        elif _SELF_RETRACT_RE.search(t):
            kind = "WITHDREW"
        elif started and (_CANCEL_REASON_RE.search(why) or _is_done_text(t)):
            kind = "reported the END of"
        else:
            fu = _FOLLOWUP_RE.search(t)
            if (fu and not fu.group(0).lower().startswith("should")
                    and (_about_maintenance(t) or _FOLLOWUP_OBJECT_RE.search(t))):
                try:
                    import noticeparse
                    w = noticeparse.find_window(t, now=now)
                except Exception:     # noqa: BLE001
                    w = None
                kind = ("WITHDREW or CORRECTED" if w is None
                        else f"CORRECTED (to {_fmt_win(w[0], w[1])})")
        if kind and (best is None or nj > best[0]):
            best = (nj, str(m.get("mid") or f"{nj:g}"), kind, t[:80])
    return best[1:] if best else None


# F59: the Web K media classes telegramwarm reports in ``kind`` that can carry a
# maintenance POSTER. Stickers, voice notes, polls and link previews cannot.
_POSTER_KIND_RE = re.compile(r"photo|document|video|^media$", re.I)


def _is_poster(msg: dict) -> bool:
    return bool(_POSTER_KIND_RE.search(str(msg.get("kind") or "")))


# An EDIT that retracts a notice. Used on edits only, and only when the words
# were not in the text first handled. noticeparse now reads a leading "[VOID]" /
# 【作废】 / "[VOID - wrong group]" tag as a retraction (needs_human), but other
# added words - "(Postponed - please see the latest notice)", a bare "void" in
# the body - still parse as a fill of the voided window, and an edit that added
# them re-announced it as a fresh "Scheduled maintenance" (G2.5, F41). This
# rule cards the edit with the row's own window either way. Greppable on purpose.
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

def _fmt_point(dt: datetime) -> str:
    """One Start/End line: the Base-zone time with its offset, plus the
    notice's own clock when the notice was written in another zone (F74)."""
    b = _base_zone(dt)
    out = f"{b:%Y-%m-%d %H:%M} {_zone_label(b)}"
    if dt.tzinfo is not None and dt.utcoffset() != b.utcoffset():
        out += f" (notice: {dt:%Y-%m-%d %H:%M} {_zone_label(dt)})"
    return out


# The card's last line for a write whose outcome is not known (F80).
_UNKNOWN_WRITE = ("⚠️ the Base did not answer the write and the row could not "
                  "be read back — it may or may not have landed. It is retried "
                  "on this group's next visit; check the row.")


def build_card(group: str, provider: str, text: str, verdict: dict,
               wrote: Optional[str], note: str = "", *,
               unknown: bool = False) -> dict:
    start, end = verdict.get("start"), verdict.get("end")
    body = text if len(text) <= 3000 else text[:3000] + "\n…"
    elements: list = [
        {"tag": "div", "text": {"tag": "lark_md", "content":
            (f"**Detected scheduled maintenance for {group}**\n"
             f"**Provider:** {provider}\n"
             f"**Start:** {_fmt_point(start)}\n"
             f"**End:** {_fmt_point(end)}"
             if start and end else
             f"**Detected scheduled maintenance for {group}**")
            + (f"\n{note}" if note else "")}},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": body}},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content":
            (f"_Base row updated · {wrote}_" if wrote
             else f"_{_UNKNOWN_WRITE}_" if unknown
             else "_⚠️ the Base row was NOT updated — see the service log_")
            + f"\n_Read-only in Telegram · {_now_str()}_"}},
    ]
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "orange" if wrote else "yellow" if unknown else "red",
                   "title": {"tag": "plain_text",
                             "content": f"\U0001F6E0️ Scheduled maintenance · "
                                        f"{provider}"[:100]}},
        "body": {"elements": elements},
    }


def build_text(group: str, provider: str, text: str, verdict: dict,
               wrote: Optional[str], note: str = "", *,
               unknown: bool = False) -> str:
    s, e = verdict.get("start"), verdict.get("end")
    head = f"🛠️ Detected scheduled maintenance for {group}\nProvider: {provider}"
    if s and e:
        head += f"\nStart: {_fmt_point(s)}\nEnd:   {_fmt_point(e)}"
    if note:
        head += "\n" + note.replace("**", "")
    head += (f"\nBase row updated · {wrote}" if wrote
             else f"\n{_UNKNOWN_WRITE}" if unknown
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


def _maybe_delivered(err: BaseException) -> bool:
    """audit-3: may the request have REACHED Lark before ``err``?

    A ReadTimeout, or a connection dropped mid-response ("Connection aborted",
    RemoteDisconnected, a reset or a truncated body), comes after the request
    went out - Lark may well have posted it. A refused / unresolvable / timed
    out CONNECT, or any non-transport error, happened before anything was sent.
    """
    if isinstance(err, requests.exceptions.ReadTimeout):
        return True
    if isinstance(err, requests.exceptions.ConnectTimeout):
        return False
    if isinstance(err, requests.exceptions.ChunkedEncodingError):
        return True
    if isinstance(err, requests.exceptions.ConnectionError):
        return bool(re.search(r"Connection aborted|RemoteDisconnected|Connection reset"
                              r"|BrokenPipe|IncompleteRead|ProtocolError", repr(err)))
    return False


def _deliver(card: dict, text: str) -> dict:
    """Send a card to the Laboratory group, falling back to plain text.

    -> {"carded": bool, "error": str, "unknown": bool}.

    The fallback used to run only when Lark REJECTED the card. When send_card
    RAISED - a ConnectionError, or the fresh tenant token every send fetches
    failing - nothing else was tried, and after a successful Base write the
    fill was ledgered as final with nobody told (F71). Those failures happen
    before anything reaches Lark, so the text is tried then too. A ReadTimeout
    is the one exception: the request was sent and may have been delivered,
    and a fallback would post the same card twice - it is reported as
    ``unknown`` instead.
    """
    out = {"carded": False, "error": "", "unknown": False}
    chat = _card_chat_id()
    try:
        resp = send_card(chat, card)
        if isinstance(resp, dict) and resp.get("code") == 0:
            out["carded"] = True
            return out
        print(f"[vawatch] card rejected: {resp!r}", flush=True)
        out["error"] = f"card rejected: {resp!r}"[:300]
    except Exception as err:          # noqa: BLE001
        if _maybe_delivered(err):
            # audit-3: a connection dropped AFTER the card went out is the
            # ReadTimeout case too - a text fallback would post it twice.
            out["unknown"] = True
            out["error"] = (f"card send failed after the request went out - it may "
                            f"have been delivered: {err!r}")[:300]
            print(f"[vawatch] {out['error']}", flush=True)
            return out
        out["error"] = f"card send failed: {err!r}"[:300]
        print(f"[vawatch] {out['error']}", flush=True)
    try:
        resp = send_text(chat, text)
        out["carded"] = isinstance(resp, dict) and resp.get("code") == 0
        if out["carded"]:
            out["error"] = ""
        else:
            out["error"] = f"{out['error']}; text fallback rejected: {resp!r}"[:300]
    except Exception as err:          # noqa: BLE001
        out["error"] = f"{out['error']}; text fallback failed: {err!r}"[:300]
        if _maybe_delivered(err):
            # audit-3: the TEXT timed out (or dropped) after it was sent. As
            # "not delivered" it was queued and re-posted on the next visit;
            # it may already be in the group, so it is "unknown", not re-sent.
            out["unknown"] = True
        print(f"[vawatch] text fallback failed: {err!r}", flush=True)
    return out


def _post_note(title: str, template: str, lines: list, text: str) -> dict:
    """Send one note card, falling back to plain text (see ``_deliver``).

    -> {"carded": bool, "error": str, "disabled": bool, "unknown": bool}.
    ``disabled`` marks the one non-failure: the card was switched off rather
    than rejected, which the ledger must not treat as something to retry.
    """
    body = text if len(text) <= 2500 else text[:2500] + "\n…"
    card = build_note_card(title, template, lines, text)
    ctext = title + "\n" + "\n".join(lines) + "\n\n" + body
    sent = _deliver(card, ctext)
    out = {"carded": sent["carded"], "error": sent["error"], "disabled": False,
           "unknown": sent["unknown"]}
    if not sent["carded"] and not sent["unknown"]:
        out["undelivered"] = {"card": card, "text": ctext}
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


def _card_cap() -> int:
    """How many red needs-a-human cards ONE row may post per _CARD_CAP_EVERY.

    VAWATCH_NEEDS_HUMAN_CAP, default 6; 0 = no cap. F58: a chatty CS group
    produced a red card per matching message, and the flood buried the real
    ones. Past the cap ONE "limit reached" card says so and the rest are held
    back (ledgered, listed by /vacheck, never written) until the period ends.
    """
    return _env_int("VAWATCH_NEEDS_HUMAN_CAP", 6, 0, 1000)


_CARD_CAP_EVERY = timedelta(hours=24)


def card_cap_reached(group: str, provider: str, cap: int, until: datetime) -> dict:
    """The one card that says a row's needs-a-human cards are being held back."""
    if not _needs_human_card():
        return {"carded": False, "disabled": True,
                "error": "VAWATCH_NEEDS_HUMAN_CARD=0"}
    return _post_note(
        f"⚠️ Card limit reached · {provider or group}", "orange",
        [f"**Group:** {group or '(unknown)'}",
         f"**Provider:** {provider or '(blank in the Base)'}",
         f"**{cap} “needs a human” cards for this row within 24 hours.** Further "
         f"ones are held back until {_base_zone(until):%Y-%m-%d %H:%M} "
         f"{_zone_label(_base_zone(until))}, so one noisy group cannot bury the "
         f"rest.",
         "**Open the group and read its recent messages by hand.** Nothing is "
         "written for the held-back messages; /vacheck lists them."],
        "(No message text: this note is about the cards held back.)")


def _image_card_enabled() -> bool:
    """Kill-switch for the "notice may be an image" card (F59). Default on."""
    return _env_flag("VAWATCH_IMAGE_CARD", "1")


def card_image_notice(group: str, provider: str, text: str, kind: str,
                      hint: str, in_caption: bool) -> dict:
    """A photo / document next to maintenance wording: a person must look (F59).

    Web K renders a poster as an image; the scraper reads only its caption,
    and "please find the maintenance schedule in the image above" has no
    window to read. It was retried as 'unparsed' three times and then went
    quiet for good. Nothing is written - only a person can read the image.
    """
    if not (_needs_human_card() and _image_card_enabled()):
        return {"carded": False, "disabled": True,
                "error": "VAWATCH_NEEDS_HUMAN_CARD=0 or VAWATCH_IMAGE_CARD=0"}
    what = ("document" if "document" in (kind or "").lower() else
            "video" if "video" in (kind or "").lower() else "image")
    return _post_note(
        f"🖼️ Maintenance notice may be an {what} · {provider or group}", "orange",
        [f"**Group:** {group or '(unknown)'}",
         f"**Provider:** {provider or '(blank in the Base)'}",
         f"**An {what} was posted with maintenance wording** "
         + ("in its caption" if in_caption else "in the message next to it")
         + f": “{' '.join(hint.split())[:200]}”",
         f"The watcher cannot read the {what}, so no window was taken from it. "
         f"Open the group and check it; if it announces maintenance, fill the "
         f"row by hand.",
         "**Nothing was written to the Base.**"],
        text or f"({what} with no caption)")


def _providerask_owner(group: str) -> str:
    """Why /provideraskmaintenance owns a "no maintenance" reply here, or "".

    F72: every weekly run asks up to 16 groups, the usual answer is "no
    maintenance this week", and the passive watcher - reading the same groups
    on its own rotation - carded each reply red ("the automatic clear is off,
    fill the row by hand") next to providerask's own card for the same answer,
    on a row providerask had already cleared. Read-only: the state file only.
    """
    try:
        import providerask
        st = providerask.load_state()
    except Exception:                 # noqa: BLE001
        return ""
    if not isinstance(st, dict) or not st:
        return ""
    g = _norm(group)
    recs = [r for r in (st.get("providers") or {}).values()
            if isinstance(r, dict) and r.get("asked") and _norm(r.get("group")) == g]
    if not recs:
        return ""
    if not st.get("finished"):
        return (f"left to /provideraskmaintenance - its open run (started "
                f"{st.get('started_at') or '?'}) asked this group and files the "
                f"answer itself")
    # A finished run that already decided this group's answer, within a day of
    # asking: the reply is the one it filed. A group it heard nothing from is
    # NOT owned - a late reply there is news.
    for r in recs:
        asked = _parse_now_str(r.get("asked_at"))
        if (r.get("outcome") in ("no_maintenance", "needs_human") and asked
                and _now_dt() - asked < timedelta(hours=24)):
            return (f"left to /provideraskmaintenance - its run asked this group at "
                    f"{r.get('asked_at')} and already filed the answer "
                    f"({r.get('outcome')})")
    return ""


def _row_window(record_id: str) -> tuple:
    """The (start, end) the Base row holds now, read back; (None, None) when
    it is empty or cannot be read. For a row this watcher did not fill (F51)."""
    try:
        f = get_row(record_id) if record_id else {}
    except Exception as err:          # noqa: BLE001
        print(f"[vawatch] could not read {record_id} back: {err!r}", flush=True)
        return None, None
    s, e = _cell_ms(f.get("Start Time")), _cell_ms(f.get("End Time"))
    if s is None or e is None:
        return None, None
    return (datetime.fromtimestamp(s / 1000, _tz()),
            datetime.fromtimestamp(e / 1000, _tz()))


def _clear_refusal(owner: Optional[dict], provider: str, record_id: str,
                   now: datetime) -> str:
    """Why a "no maintenance" notice must NOT blank this row, or "".

    G3.2, the vawatch half: with VAWATCH_CLEAR_ENABLED=1 the clear blanked
    whatever the row held, so "No maintenance this week, thanks" erased next
    week's window that a notice had announced days earlier, and the row read
    "No maintenance" through the outage. /provideraskmaintenance already
    refuses this (providerask._clear_plan); this is the same rule: a window
    that has not ENDED is never blanked by a plain "no maintenance" - which of
    the two messages is right is for a person. The row is read back, so a
    window entered by hand or by providerask counts too, and a read that fails
    refuses rather than blanking blind. A cancellation that names exactly the
    row's window is a different path (F51) and still clears.
    """
    if _owner_live(owner, now):
        os_, oe = _owner_window(owner)
        return (f"the row still holds an upcoming window ({_fmt_win(os_, oe)}) "
                f"from an earlier notice - left in place; check which one is right")
    try:
        rid = _resolve_record(provider, record_id)
        f = get_row(rid) or {}
    except Exception as err:          # noqa: BLE001
        return (f"the row's current window could not be read back ({err!r}) - "
                f"left alone rather than blanked blind")
    s, e = _cell_ms(f.get("Start Time")), _cell_ms(f.get("End Time"))
    last = e if e is not None else s
    if last is not None and last > _ms(now):
        shown = (_fmt_win(datetime.fromtimestamp(s / 1000, _tz()),
                          datetime.fromtimestamp(e / 1000, _tz()))
                 if s is not None and e is not None else "its End Time")
        return (f"the row still holds an upcoming window ({shown}) - left in "
                f"place; check which one is right")
    return ""


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


# ---------------------------------------------------------------------------
# Confirm-before-write (VAWATCH_LLM_CONFIRM, default on)
# ---------------------------------------------------------------------------
#
# The regex parser proposes; the local model (providerllm - the same one
# /provideraskmaintenance uses, on the bot's own box) confirms. A window is
# written only when the model answers yes to all three of: the message
# ANNOUNCES it, it is THIS provider's outage, and it is EXACTLY this window.
# The model never supplies a time, and anything short of that yes is a person's
# (needs-human card, nothing written). A model that cannot be reached is not a
# verdict: the notice is retried on later sweeps (VAWATCH_CONFIRM_ATTEMPTS) and
# only then carded. VAWATCH_LLM_CONFIRM=0 writes on the parser alone, as before.


def _llm_confirm_on() -> bool:
    return os.getenv("VAWATCH_LLM_CONFIRM", "1").strip().lower() not in ("0", "false", "no", "off")


def _confirm_attempts() -> int:
    try:
        return max(1, int(os.getenv("VAWATCH_CONFIRM_ATTEMPTS", "3")))
    except ValueError:
        return 3


def _llm_confirm(text: str, verdict: dict, *, provider: str, group: str = "") -> dict:
    """-> {"verdict": "yes"|"no"|"unclear", "transient": bool, "why": str}."""
    if not _llm_confirm_on():
        return {"verdict": "yes", "transient": False, "why": "confirm switched off"}
    try:
        import providerllm
        return providerllm.confirm_window(
            text, provider=provider, start=verdict.get("start"), end=verdict.get("end"),
            names=list(_names_for(provider) or []), reschedule=bool(verdict.get("reschedule")),
            group=group)
    except Exception as err:          # noqa: BLE001 - fail closed, retried later
        return {"verdict": "unclear", "transient": True, "why": f"confirm failed: {err!r}"[:200]}


def _gate_card_extra(verdict: dict, gate: dict, tries: int) -> str:
    win = _fmt_win(verdict.get("start"), verdict.get("end"))
    if gate.get("transient"):
        return (f"**Nothing was written.** The parser read {win}, but the model that "
                f"confirms every write could not be reached after {tries} tries "
                f"({gate.get('why') or 'no answer'}). Check the notice and fill the "
                f"row by hand if it is right.")
    return (f"**Nothing was written.** The parser read {win}, but the confirming "
            f"model did not agree: {gate.get('why') or 'no reason given'}.")


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
    out: dict = {"wrote": None, "error": "", "record_id": "", "carded": False,
                 "unknown": False, "card_error": "", "card_unknown": False}
    fields = {
        "Start Time": _ms(verdict["start"]),
        "End Time": _ms(verdict["end"]),
        "Remark": text,
        "Last Check": _ms(datetime.now(_tz())),
        # Reference is deliberately absent: leaving it out leaves it alone.
    }
    # What the row now DISPLAYS (Base zone, end date, zone named), not the
    # notice's own clock with the end date cut off (F74).
    shown = (_fmt_win(verdict["start"], verdict["end"])
             + (" (rescheduled)" if verdict.get("reschedule") else ""))
    try:
        out["record_id"] = _resolve_record(provider, record_id)
        update_row(out["record_id"], fields)
        out["wrote"] = shown
    except Exception as err:          # noqa: BLE001
        # A failed write must never look like an ignored message, so the card
        # still goes out and says so.
        out["error"] = repr(err)
        print(f"[vawatch] Base update FAILED: {err!r}", flush=True)
        if out["record_id"] and isinstance(err, requests.exceptions.RequestException):
            # F80: a transport error AFTER the PUT went out (a ReadTimeout, a
            # connection dropped mid-response) says nothing about whether Lark
            # applied it, and the card said "NOT updated" either way. Read the
            # row back: if it holds exactly what was sent, the write landed.
            got = _confirm_fields(out["record_id"], fields)
            if got is True:
                out["wrote"] = shown
                out["error"] = ""
                note = ((note + "\n") if note else "") + (
                    "_The Base did not answer the write in time; reading the row "
                    "back shows it landed._")
                print(f"[vawatch] the write to {out['record_id']} was confirmed "
                      f"by reading it back", flush=True)
            elif got is None:
                out["unknown"] = True

    card = build_card(group, provider, text, verdict, out["wrote"], note,
                      unknown=out["unknown"])
    ctext = build_text(group, provider, text, verdict, out["wrote"], note,
                       unknown=out["unknown"])
    sent = _deliver(card, ctext)
    out["carded"] = sent["carded"]
    out["card_error"] = sent["error"]
    out["card_unknown"] = sent["unknown"]
    if not sent["carded"] and not sent["unknown"]:
        # Kept so handle_messages can re-send THIS card, and only it, once the
        # messaging side recovers - the write itself is final (F71).
        out["undelivered"] = {"card": card, "text": ctext}
    return out


def _confirm_fields(record_id: str, fields: dict) -> Optional[bool]:
    """Does the record hold ``fields``' Start/End Time now? None = cannot tell.

    Only the two DateTime cells are compared: they are what the row's window
    IS, and Remark comes back from Lark as rich text rather than the string
    that was sent. A cleared cell (None sent) must read back empty."""
    try:
        got = get_row(record_id)
    except Exception as err:          # noqa: BLE001
        print(f"[vawatch] could not read {record_id} back: {err!r}", flush=True)
        return None
    for name in ("Start Time", "End Time"):
        if _cell_ms(got.get(name)) != fields.get(name):
            return False
    return True


def act_on_clear(text: str, verdict: dict, *, provider: str = "",
                 group: str = "", record_id: str = "",
                 remark: str = "No maintenance", title: str = "",
                 headline: str = "") -> dict:
    """The provider says there is no maintenance: blank the row.

    -> {"wrote", "error", "record_id", "skipped", "carded", "unknown",
        "card_error", "card_unknown"}.

    ``remark`` / ``title`` / ``headline`` are for the one other caller, a
    provider CANCELLING the window the row holds (F51): the row is blanked the
    same way, but Remark keeps the cancellation's own words and the card says
    what happened.

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
                 "carded": False, "unknown": False, "card_error": "",
                 "card_unknown": False}
    fields = {
        "Start Time": None,
        "End Time": None,
        "Remark": remark,
        "Last Check": _ms(datetime.now(_tz())),
        # Reference is deliberately absent: leaving it out leaves it alone.
    }
    shown = "cleared (No maintenance)" if remark == "No maintenance" else "cleared"
    try:
        out["record_id"] = _resolve_record(provider, record_id)
        update_row(out["record_id"], fields)
        out["wrote"] = shown
    except Exception as err:          # noqa: BLE001
        out["error"] = repr(err)
        print(f"[vawatch] Base clear FAILED: {err!r}", flush=True)
        if out["record_id"] and isinstance(err, requests.exceptions.RequestException):
            # The same read-back as act_on_notice (F80).
            got = _confirm_fields(out["record_id"], fields)
            if got is True:
                out["wrote"], out["error"] = shown, ""
            elif got is None:
                out["unknown"] = True

    shown_remark = remark if len(remark) <= 60 else remark[:60] + "…"
    sent = _post_note(title or f"\U0001F9F9 No maintenance · {provider}",
                      "green" if out["wrote"] else "yellow" if out["unknown"]
                      else "red",
                      [f"**Group:** {group}", f"**Provider:** {provider}"]
                      + ([headline] if headline else [])
                      + [(f"**Start Time / End Time cleared, Remark = "
                          f"“{shown_remark}”**" if out["wrote"] else
                          f"**{_UNKNOWN_WRITE}**" if out["unknown"] else
                          f"**⚠️ the Base row was NOT cleared** — "
                          f"{out['error']}")], text)
    out["carded"] = bool(sent.get("carded"))
    out["card_error"] = sent.get("error") or ""
    out["card_unknown"] = bool(sent.get("unknown"))
    if sent.get("undelivered"):
        out["undelivered"] = sent["undelivered"]
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


def _unparsed_last_try(d: dict, key: str) -> bool:
    """Is this read of an unparsed notice the last one the reparse cap allows?

    Also True straight after that last try's card FAILED to send: card-failed
    earns a retry, and the retry must go to the card again rather than start
    the three unparsed reads over (G1.3).
    """
    prev = d["handled"].get(key)
    prev = prev if isinstance(prev, dict) else {}
    if prev.get("outcome") == "card-failed":
        return True
    tries = (int(prev.get("attempts") or 0) + 1
             if prev.get("outcome") == "unparsed" else 1)
    return tries >= _reparse_attempts()


def _note_outcome(sent: dict) -> str:
    """A card nobody received is worth another go; a card switched off is not.

    Nor is one whose send timed out (``unknown``): it was sent and probably
    delivered, and a retry would post the same red card twice (F71)."""
    if sent.get("carded") or sent.get("disabled") or sent.get("unknown"):
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
    pre = post = ""
    if re.fullmatch(r"\d+G", name.strip()):
        # R1.78: the provider "5G" is also the mobile network - "players on
        # 4G/5G mobile networks will also be affected" read as naming 5G and
        # refused a JDB notice as somebody else's.
        # #296: the network lookahead sat BEFORE the name, where it tested the
        # text at "5G" itself for "mobile ..." and so never fired: "Players on
        # 5G networks may experience brief disconnects", "5G and 4G users are
        # also affected" and "All players including 5G users" each refused a
        # real JDB notice as 5G's. It now follows the name, and a 4G/5G pair
        # joined by and/or/&/, counts as the network too.
        pre = (r"(?<!\dG/)(?<!\dG\s)(?<!\dG,)(?<!\dG and )(?<!\dG or )"
               r"(?<!\dG & )(?<!\dG, )")
        post = (r"(?![ \t/]*(?i:mobile|networks?|signal|data|connections?|LTE"
                r"|internet|coverage|users?|speeds?|phones?|devices?|towers?"
                r"|sim|broadband))"
                r"(?![ \t]*(?:/|,|&|(?i:and|or))[ \t]*\dG(?![A-Za-z0-9]))")
    return re.compile(r"(?<![A-Za-z0-9])" + pre + core + post + r"(?![A-Za-z0-9])",
                      flags)


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
    # G4.4: the ACTIVE voice - "Evolution's maintenance ... does not affect
    # Pragmatic Play". Only the passive was known, so the row's own name in
    # that sentence counted as a positive mention, the other-provider check was
    # skipped, and Evolution's window went onto Pragmatic Play's row.
    r"|\b(?:does|do|did|will|would|should|shall)\s+not\s+(?:affect|impact"
    r"|involve|include|concern|apply\s+to)\b"
    r"|\b(?:doesn|don|didn|won|wouldn|shouldn)['’]?t\s+(?:affect|impact|involve"
    r"|include|concern|apply\s+to)\b"
    r"|\bno\s+(?:impact|effect)\s+(?:on|to|for)\b"
    r"|不会影响|不會影響|不會影响|不涉及"
    r"|\bno\s+(?:impact|effect|downtime|interruption|maintenance)\b"
    r"|\b(?:remain|remains|stay|stays)\s+(?:available|online|open|operational"
    r"|accessible|unaffected|normal)\b"
    r"|\b(?:operate|operates|run|runs|running)\s+(?:as\s+)?normal(?:ly)?\b"
    r"|\bas\s+usual\b|\bexcept\b|\bexcluding\b|\bother\s+than\b"
    # audit-1: "Hacksaw games are excluded from this maintenance", "games from
    # Hacksaw excluded" and "(not including Hacksaw)" said the named provider is
    # LEFT OUT, and each counted as a positive mention - the window was written
    # onto the very row the notice excluded (vawatch and providerask's
    # _shared_route both read this list).
    r"|\bexcluded\b|\bexempt(?:ed)?\b|\bnot\s+including\b|\bnot\s+incl\.?"
    r"|不受影响|不影响|无影响|没有影响|不包括|不包含|除外|照常|不含"
    r"|正常(?:运行|运营|营运|开放|使用)", re.I)

_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|[。！？!?；;\n]+")


@functools.lru_cache(maxsize=512)
def _without_re(name: str):
    """audit-1: "without <name>" - the name bound to the negation, since a bare
    "without" is everywhere in real notices ("without prior notice")."""
    return re.compile(r"(?i:\bwithout)[ \t]+(?:(?i:the|any|all)[ \t]+)?"
                      r"(?:[\w'’-]+[ \t]+){0,2}?" + _name_re(name).pattern,
                      _name_re(name).flags)


def _mentions(text: str, names: list) -> tuple:
    """-> (positive, negated) counts of sentences naming any of ``names``."""
    pos = neg = 0
    if not names:
        return 0, 0
    for s in _SENTENCE_SPLIT_RE.split(text or ""):
        if s and any(_name_re(n).search(s) for n in names):
            if (_NOT_AFFECTED_RE.search(s)
                    or any(_without_re(n).search(s) for n in names)):
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
    # G4.1 (#95, #96): "CasinoPlus PH will undergo ..." put a market code
    # between the name and the verb, and "The CasinoPlus website and app will be
    # under maintenance" two nouns joined by "and" - neither matched, and both
    # windows went onto the provider's row.
    noun = (r"(?:platform|system|systems|site|website|side|back[ \t]*office"
            r"|backend|servers?|apps?|mobile[ \t]+apps?|web|portal|services?)")
    tail = (r"(?:[ \t]*[(（][^()（）\n]{1,30}[)）])?(?:'s|’s)?"
            r"(?:[ \t]+(?:PH|MY|SG|TH|VN|ID|(?i:Philippines|Pilipinas|Official|Ops)))?"
            r"(?:(?i:[ \t]+" + noun + r"(?:[ \t]*(?:,|&|/|and)[ \t]*" + noun + r")*)?"
            # "will"/"shall" makes the operator the subject only when what
            # follows is something it DOES. "Casinoplus side will be unable to
            # launch our games", "CP side will not be able to open the games"
            # and "IGO platform will show the maintenance page" make it the
            # party AFFECTED by the provider's window; reading those as the
            # operator's own maintenance carded real provider notices (R1.63).
            r"(?i:[ \t]+(?:(?:will|shall)(?![ \t]+(?:not[ \t]+be[ \t]+able"
            r"|be[ \t]+unable|unable|not[ \t]+have[ \t]+access|lose[ \t]+access"
            r"|not[ \t]+(?:be[ \t]+)?(?:able|accessible|access|launch|open"
            r"|load|connect|reach)|be[ \t]+(?:affected|impacted)|show|display"
            r"|see|experience|encounter|receive|get|notice))"
            r"|is[ \t]+going[ \t]+to|is[ \t]+scheduled|(?:will[ \t]+)?undergo(?:es|ing)?\b"
            r"|are[ \t]+(?:temporarily[ \t]+)?(?:under|down)\b"
            # G4.1: "the Casinoplus site is going under / is undergoing / is
            # under scheduled maintenance" - the operator's platform as the
            # thing being maintained.
            r"|is[ \t]+(?:currently[ \t]+)?(?:going[ \t]+)?under(?:going)?\b"
            r"|ha(?:s|ve)[ \t]+(?:an?[ \t]+)?(?:scheduled|planned|emergency"
            r"|routine|system|platform)"
            # ...and the operator's own "no maintenance": a clear, which with
            # VAWATCH_CLEAR_ENABLED=1 would blank the provider's live window.
            r"|ha(?:s|ve)[ \t]+no[ \t]+(?:scheduled[ \t]+|planned[ \t]+)?"
            r"maintenance"
            r"|(?:scheduled|planned|emergency|routine|system|platform)[ \t]+"
            r"(?:maintenance|downtime|upgrade)"
            r"|maintenance|downtime|upgrade))(?![A-Za-z])"
            r"|[ \t]*(?:平台|系统|方)?[ \t]*(?:将(?!无法|不能|会无法|受|显示|看到)|计划|定于|拟于|进行|例行维护"
            r"|系统维护|维护|停机|升级))")
    # "our side (CasinoPlus)", and G4.1's "we (Casinoplus) will have scheduled
    # maintenance" - the speaker naming itself as the operator.
    # #279: 我方（CasinoPlus）系统维护 - the same self-naming in Chinese.
    ours = (r"(?:(?i:\b(?:our|my)[ \t]+(?:side|platform|system|site|website|end"
            r"|team)|\bwe|\bus)|我方|我司|我们|我們|本司|本公司|本平台)[ \t]*[(（][ \t]*" + alt)
    # G4.1: the maintenance OF the operator - "a scheduled maintenance of
    # Casinoplus on ...", "maintenance on the Casinoplus side ...". A provider
    # writes "our maintenance", never "maintenance of <the operator>".
    # #97 / #279: "Scheduled maintenance by CasinoPlus ...", "Maintenance at
    # CasinoPlus ..." - the same, with the other two prepositions.
    of_op = (r"(?i:\b(?:maintenance|downtime|upgrade))[ \t]+(?i:of|for|on|by|at)[ \t]+"
             r"(?i:the[ \t]+)?" + alt
             + r"(?:[ \t]+(?i:platform|system|site|website|side|end))?(?![A-Za-z0-9])")
    return re.compile("(?<![A-Za-z0-9])" + alt + tail + "|" + ours + "|" + of_op)


# The message is ADDRESSED TO the provider, so the speaker is the operator: a
# provider says "our games", never "your games", and never asks its operator to
# stop deploying or warns it of "no traffic". Only things the PROVIDER owns are
# listed after "your": "your lobby" / "your players" / "your site" are the
# operator's, so a provider says them ("our games will leave your lobby").
# R1.78: "Your games will show a maintenance message", "维护期间，贵司的游戏将无法
# 访问", "Please avoid any releases on your side during the maintenance" and
# "There will be no traffic to the games" all turned out to be PROVIDERS
# telling the operator what its players will see, and real notices were carded
# as the operator's. What is left names the maintenance as the ADDRESSEE's:
# "your maintenance", "贵司的维护" - which a provider never says of its own.
#
# "no traffic" / 无流量 and "avoid releases" STAY: "我方平台将于<X>进行例行维护，
# 期间无流量" is the operator warning the provider, and writing it put the
# operator's window on the provider's row. A provider that says the same is
# carded rather than filled - when the speaker cannot be told, a card is the
# safe outcome, never a write.
_OPERATOR_AUDIENCE_RE = re.compile(
    r"(?i:\byour[ \t]+(?:scheduled[ \t]+|planned[ \t]+|upcoming[ \t]+)?"
    r"(?:maintenance|downtime|upgrade)\b(?![ \t]+(?:page|message|notice|screen))"
    r"|\b(?:no|zero|low|lower|less|reduced)[ \t]+(?:player[ \t]+)?traffic\b"
    r"|\bavoid(?:ing)?[ \t]+(?:any[ \t]+|all[ \t]+)?(?:deployments?|releases?"
    r"|deploying|releasing)\b"
    r"|\b(?:do[ \t]+not|don'?t)[ \t]+(?:deploy|release)\b)"
    r"|暂停发版|停止发版|请勿发版|避免发版|不要发版|无流量|没有流量"
    r"|贵司(?:的)?(?:维护|維護|系统维护|系統維護)|你们的维护|您的维护"
    # G4.4 (#118): "... emergency maintenance on <X>, not your games", "this does
    # not concern your games", "FYI another provider has scheduled maintenance"
    # - the speaker says outright that the window is NOT the addressee's. Only
    # the negated forms: "your games will show a maintenance message" is a
    # provider talking (R1.78).
    r"|(?i:\bnot[ \t]+(?:for[ \t]+|about[ \t]+|related[ \t]+to[ \t]+|affecting[ \t]+"
    r"|involving[ \t]+)?your[ \t]+(?:games?|products?|platform|studio)\b"
    r"|\b(?:does|do|will|would|should)[ \t]+not[ \t]+(?:concern|affect|involve|impact"
    r"|apply[ \t]+to)[ \t]+your[ \t]+(?:games?|products?|platform|studio)\b"
    r"|\b(?:doesn|don|won|wouldn)['’]?t[ \t]+(?:concern|affect|involve|impact"
    r"|apply[ \t]+to)[ \t]+your[ \t]+(?:games?|products?|platform|studio)\b"
    r"|\banother[ \t]+(?:game[ \t]+)?provider\b|\ba[ \t]+different[ \t]+provider\b"
    r"|\bother[ \t]+provider['’]s\b)"
    r"|不是贵司的游戏|与贵司(?:的)?游戏无关|與貴司(?:的)?遊戲無關|其他供应商|其他廠商|其他厂商")

# G4.1 (#99-#102): a notice ADDRESSED to providers in general - "Dear provider,",
# "To all providers:", "Dear game providers,", 致各游戏供应商： - is the operator
# writing to its providers, whoever the row is. _salutes only knew the row's own
# names. "Dear partners" is NOT here: providers greet their operators that way.
_PROVIDER_ADDRESSEE_RE = re.compile(
    r"^\s*(?:(?i:dear|hi|hello|hey|to|attn\.?|attention)[ \t]+(?i:all[ \t]+)?(?i:(?:our|the)[ \t]+)?"
    r"(?i:(?:valued|game|gaming|content|slot|casino)[ \t]+)?"
    r"(?i:providers?|suppliers?|vendors?|studios?)(?i:[ \t]+(?:team|teams|partners?))?"
    r"[ \t]*[,，:：!]"
    r"|(?:致|给|給|尊敬的|亲爱的|親愛的)[ \t]*(?:各|所有|全体|全體)?(?:位)?(?:游戏|遊戲)?"
    r"(?:供应商|供應商|厂商|廠商|服务商|服務商)(?:们|們)?[ \t]*[,，:：!])")

# What may sit just before the operator's name when the operator is NOT the
# subject: an addressee ("Dear CasinoPlus", "致CasinoPlus") or a co-brand joiner
# ("Pragmatic Play x CasinoPlus scheduled maintenance", "the maintenance for
# CasinoPlus will be…"). Checked on the same line only.
# G4.1 (#93): a greeting followed by a COMMA is a greeting, not an addressee -
# "Hello, CP platform maintenance 2026-09-25 03:00-05:00" starts a new clause
# whose subject is the operator, while "Dear CasinoPlus, ..." and "Hi CP team"
# put the name right after the greeting. So a greeting counts only when the
# name follows it directly; "To:" / "Attn:" keep their colon.
_NOT_SUBJECT_BEFORE_RE = re.compile(
    r"(?i)(?:(?:\bdear|\bhi|\bhello|\bhey|尊敬的|亲爱的)[ \t]*"
    r"|(?:\bto|\battn\.?|\battention|致|给)[ \t:：]*"
    r"|(?:\bfor|\bwith|\band|\bx|×|&)[ \t,]*)$")

# G4.1 (#98): the operator introducing ITSELF - "Hi CP team here, scheduled
# maintenance ... on our platform", "This is CasinoPlus ops", "CasinoPlus team
# here". The greeting in front makes it look addressed to CP, so this is
# checked on its own, without _NOT_SUBJECT_BEFORE_RE.
def _operator_self_re():
    names = []
    for n in _operator_names():
        body = r"[ \t]+".join(re.escape(p) for p in n.split())
        names.append("(?i:" + body + ")" if len(n.replace(" ", "")) > 3 else body)
    if not names:
        return None
    alt = "(?:" + "|".join(names) + ")"
    return re.compile(
        r"(?<![A-Za-z0-9])" + alt
        + r"(?i:[ \t]+(?:team|side|ops|operations|support|CS|tech|IT))?(?i:[ \t]+here)\b"
        r"|(?i:\bthis[ \t]+is|\bgreetings[ \t]+from|\bmessage[ \t]+from)[ \t]+(?i:the[ \t]+)?"
        + alt + r"(?![A-Za-z0-9])"
        r"|(?:这里是|這裡是|我们是|我們是)[ \t]*" + alt)


def _operator_extra_re():
    """R1.60: the operator as subject in the shapes _operator_subject_re misses -
    "CasinoPlus DB/database/app maintenance", "Casinoplus: scheduled
    maintenance", "【CasinoPlus】系统维护", "Maintenance on CasinoPlus side",
    "Notice from CasinoPlus:", "Operator side (CasinoPlus) will do DB
    maintenance". Each wrote the operator's own window onto the provider's row.
    """
    names = []
    for n in _operator_names():
        body = r"[ \t]+".join(re.escape(p) for p in n.split())
        names.append("(?i:" + body + ")" if len(n.replace(" ", "")) > 3 else body)
    if not names:
        return None
    alt = "(?:" + "|".join(names) + ")"
    maint = r"(?i:maintenance|downtime|upgrade|migration)|维护|維護|升级|升級|停机|停機"
    return re.compile(
        r"(?<![A-Za-z0-9])" + alt + r"(?:[ \t]+(?i:DB|database|app|apps|mobile[ \t]+app"
        r"|web(?:site)?|site|payment|wallet|core))+[ \t]+(?:" + maint + r")"
        # #279: "CasinoPlus - Scheduled maintenance ...", "CasinoPlus | ..." -
        # a dash or bar as the heading separator, as well as a colon.
        r"|(?<![A-Za-z0-9])" + alt + r"(?:[ \t]*[:：】\]|｜]|[ \t]+[-–—][ \t]+)"
        r"[^\n]{0,24}?(?:" + maint + r")"
        r"|(?i:maintenance|downtime)[ \t]+on[ \t]+(?:the[ \t]+)?" + alt
        + r"[ \t]+(?i:side|end)\b"
        r"|(?i:notice|announcement|message|update)[ \t]+from[ \t]+(?:the[ \t]+)?" + alt
        + r"(?![A-Za-z0-9])"
        r"|(?i:operator|merchant)[ \t]+side[ \t]*[(（][ \t]*" + alt
        # "Our (CasinoPlus) maintenance…": the operator bracketed right after
        # "our", with no noun in between.
        + r"|(?i:\b(?:our|my))[ \t]*[(（][ \t]*" + alt)


def _operator_hit(text: str) -> tuple:
    """-> ("subject" | "audience", phrase), or ("", "") when neither fires.

    "subject": the operator is what goes down. "audience": the text is written
    TO the provider - the operator's own notice or a relay of someone else's -
    so the card can say which it saw instead of calling a relay of Evolution's
    window "the operator's maintenance".
    """
    t = text or ""
    rs = _operator_self_re()
    ms_ = rs.search(t) if rs else None
    if ms_:
        return "subject", ms_.group(0).strip()
    rx = _operator_subject_re()
    for m in (rx.finditer(t) if rx else ()):
        line_start = t.rfind("\n", 0, m.start()) + 1
        if not _NOT_SUBJECT_BEFORE_RE.search(t[line_start:m.start()]):
            return "subject", m.group(0).strip()
    rx2 = _operator_extra_re()
    for m in (rx2.finditer(t) if rx2 else ()):
        line_start = t.rfind("\n", 0, m.start()) + 1
        if not _NOT_SUBJECT_BEFORE_RE.search(t[line_start:m.start()]):
            return "subject", m.group(0).strip()
    first = next((ln for ln in t.splitlines() if ln.strip()), "")
    m = _PROVIDER_ADDRESSEE_RE.search(first)
    if m:
        return "audience", m.group(0).strip()
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
    # G4.2: the banks and gateways the list did not know wrote their windows
    # onto a game provider's row ("Security Bank scheduled maintenance ...",
    # "China Bank will have ...", "PNB ...", "DragonPay ..."). Named ones, plus
    # any "<Proper Name> Bank" / "Bank of <X>" - no game provider is a Bank.
    r"|PNB|LBP|AUB|UCPB|PSBank|Chinabank|EastWest|Maybank|DragonPay|Dragonpay"
    r"|Paynamics|Xendit|PayMongo|PayPal|Payoneer|Wise|Alipay|WeChat[ \t]+Pay"
    # G4.2 (#104-#110): telcos, remittance, interbank and crypto rails that
    # wrote their windows onto a game provider's row - "Sun Cellular network
    # maintenance", "TNT network maintenance", "Cebuana Lhuillier scheduled
    # maintenance", "Bancnet ...", "PSP ...", "TRON network ... USDT deposits
    # delayed", "Binance ...".
    r"|Sun[ \t]+Cellular|TNT|Cebuana(?:[ \t]+Lhuillier)?|M[ \t]*Lhuillier|Palawan[ \t]+Express"
    r"|Bancnet|BancNet|BANCNET|PSP|TRON|TRC-?20|ERC-?20|BEP-?20|USDT|Tether|Binance"
    r"|OKX|Bybit|Coinbase|Ethereum|Bitcoin|(?i:blockchain)"
    # ...the generic shapes only when the sentence goes on to say what the bank
    # is doing: "Thank You Bank on us" is not a bank.
    r"|(?:(?:[A-Z][A-Za-z]+[ \t]+){1,2}Bank|Bank[ \t]+of[ \t]+(?:the[ \t]+)?[A-Z][A-Za-z]+)"
    r"(?=[^.。\n]*(?i:maintenance|downtime|upgrade|system|transfers?|deposits?"
    r"|withdrawals?|cash|unavailable|offline|outage|service))"
    # #105: "Smart scheduled network maintenance" - a schedule word between.
    r"|Smart(?=[ \t]+(?:(?:scheduled|planned|emergency|system)[ \t]+)?(?:Communications"
    r"|Telecom|network|signal|subscribers?|users?|prepaid|postpaid)))(?![A-Za-z0-9])"
    r"|(?i:\b(?:online[ \t]+)?banking\b|\bbanks?[ \t]+(?:maintenance|system"
    r"|systems|transfers?|channels?)\b|\btelco\b|\be-?wallets?\b"
    r"|\bpayment[ \t]+(?:channels?|gateways?|providers?|methods?)\b"
    r"|\bcash-?in\b|\bdeposits?[ \t]+via\b)"
    # #111 / #112: 支付宝 (Alipay) and 阿里云 (Aliyun) in Chinese, and their
    # neighbours.
    r"|支付通道|支付渠道|银行|电信|运营商网络|网银|支付宝|支付寶|微信支付|财付通"
    r"|阿里云|阿里雲|腾讯云|騰訊雲|华为云|華為雲|亚马逊云|亞馬遜雲|币安|幣安")

# #103: a PARTNER is a different company by definition, so "Our payment partner
# will have scheduled maintenance" is a third party even after "our" - the
# _OWN_BEFORE_RE exemption ("our payment gateway integration") must not apply.
_THIRD_PARTY_PARTNER_RE = re.compile(
    r"(?i:\b(?:payment|banking|bank|telco|network|cloud|hosting|remittance|wallet"
    r"|e-?wallet|crypto|PSP)[ \t]+(?:partners?|vendors?)\b)"
    r"|支付合作方|支付伙伴|支付夥伴|第三方支付")


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


def _salutes(text: str, names: list) -> str:
    """The salutation addressing one of ``names`` at the top of ``text``, or ''."""
    first = next((ln for ln in (text or "").splitlines() if ln.strip()), "")
    for n in names:
        body = r"[ \t]+".join(re.escape(p) for p in n.split())
        # #279: "Dear partner PP," (a noun before the name) and 致PP团队：/
        # 尊敬的PP团队， (a Chinese salutation, no space after it) were not seen
        # as written TO the provider.
        rx = re.compile(r"^\s*(?:(?i:dear|hi|hello|hey|to|attn\.?|attention)[ \t]+"
                        r"(?i:the[ \t]+)?(?i:(?:partners?|colleagues?|friends?|team)[ \t]+)?"
                        r"|(?:致|给|給|尊敬的|亲爱的|親愛的)[ \t]*)"
                        + ("(?i:" + body + ")" if len(n) > 3 else body)
                        + r"(?:[ \t]+(?i:team|gaming|colleagues|partners?|friends)"
                          r"|[ \t]*(?:团队|團隊|同事|伙伴|夥伴|合作伙伴|合作夥伴|方))?"
                        r"[ \t]*[,，:：!]")
        m = rx.search(first)
        if m:
            return m.group(0).strip()
    return ""


# #284: a test, template or example posted in the group - "Test notice -
# Scheduled maintenance ...", "test test ...", "#test ...", "Template - ...",
# "For example, ...", "Mock notice: ...", "Dummy: ...", "Format: ...",
# 这是一条测试消息：... - parses as a perfect notice and was written. Only a
# marker at the very START of the message counts (plus "please use this
# format"): "the test environment will also be updated" inside a real notice
# is noticeparse's UAT rule, not this one.
_SAMPLE_MARK_RE = re.compile(
    r"^\W{0,4}(?:(?i:test(?:ing)?|template|sample|example|for[ \t]+example|e\.g\.|mock"
    r"|dummy|draft|format|demo)(?:[ \t]+(?i:notice|message|msg|post|only|announcement))?"
    r"[ \t]*[-–—:：,.|)]"
    r"|(?i:test[ \t]+test)\b|#(?i:test(?:ing)?)\b"
    r"|(?:测试|測試|模板|示例|范例|範例|样本|樣本|草稿|格式)(?:消息|信息|通知|公告|訊息)?[ \t]*[：:，,\-]"
    r"|这是一条测试|這是一條測試|这是测试|這是測試)"
    r"|(?i:\bplease[ \t]+use[ \t]+(?:this|the[ \t]+following)[ \t]+(?:format|template)\b)")

_RELAY_MARK_RE = re.compile(
    r"(?i:forwarded[ \t]+message|-{3,}[ \t]*forwarded|^[ \t]*fwd?[ \t]*:)"
    r"|转发自|轉發自|转发：|轉發：", re.M)
_SCHED_LABEL_SKIP = re.compile(
    r"(?i)^(?:date|time|start|end|from|to|duration|period|window|schedule|note"
    r"|remarks?|time\s*zone|zone|affected|impact|gmt|utc|day|when|new|old"
    r"|original|updated|revised)\b|^(?:日期|时间|時間|开始|結束|结束|备注|影响|原定|新)")


def _multi_provider_schedule(text: str, providers: list) -> bool:
    """Two or more "<Name>: <clock>" lines naming DIFFERENT known providers.

    Only a name the Base (or the other-studio list) knows counts, alias
    included - "Phase 1: … / Phase 2: …" and "Slots: … / Live: …" are one
    provider's own schedule, not several providers'.
    """
    canon = {}
    for p in providers:
        for n in _names_for(p) or [p]:
            canon[_norm(n)] = _norm(p)
    seen = set()
    for ln in (text or "").splitlines():
        # #283: "PP - 2026-09-26 02:00-04:00" / "NetEnt 2026-09-25 10:00-12:00"
        # - a dash, a bar or plain space before the date, not only a colon.
        # JILI's window went onto PP's row. Every length of the leading words
        # is tried, since "Pragmatic" is a prefix of "Pragmatic Play".
        if not re.search(r"\d{1,2}[:：.]\d{2}", ln):
            continue
        m = re.match(r"^[ \t]*[-•*·]?[ \t]*([A-Za-z][\w &'.-]{0,30}|[一-鿿]{2,8})", ln)
        if not m:
            continue
        head, base = m.group(1), m.start(1)
        for k in range(len(head), 0, -1):
            nm = head[:k].rstrip(" \t-–—")
            after = ln[base + len(nm):]
            if nm and _norm(nm) in canon and re.match(
                    r"[ \t]*[:：|｜]|[ \t]+[-–—][ \t]+|[ \t]+(?=\d)", after):
                seen.add(canon[_norm(nm)])
                break
    return len(seen) >= 2


# R1.66: game studios that are NOT rows in this Base, as the subject or the
# signature of a notice relayed into a provider group. Greppable, and
# VAWATCH_OTHER_STUDIOS replaces it.
_DEFAULT_OTHER_STUDIOS = ("NetEnt,Relax Gaming,Evolution,Evolution Gaming,Microgaming"
                          ",Play'n GO,Red Tiger,Nolimit City,Push Gaming,Thunderkick"
                          ",Quickspin,BGaming,Habanero,Spadegaming,Playtech"
                          ",Big Time Gaming,ELK Studios,Blueprint Gaming,Endorphina"
                          ",Booongo,Wazdan,Betsoft,PAGCOR")


def _other_studios() -> list:
    raw = os.getenv("VAWATCH_OTHER_STUDIOS")
    if raw is None or not raw.strip():
        raw = _DEFAULT_OTHER_STUDIOS
    return _split_names(raw)


# R1.78: a payment/cloud/banking word the PROVIDER owns ("our AWS server
# migration", "migrating our CDN to Cloudflare", "our payment gateway
# integration", "our internal banking system module") or merely lists as
# affected ("e-wallet transfers will be unavailable") is the provider's own
# outage. Only a third party that is the maintenance's SUBJECT counts.
_OWN_BEFORE_RE = re.compile(r"(?i:\b(?:our|we|us)\b|\bto[ \t]+$)|我们|我們|我司|本司|本公司")
_VICTIM_AFTER_RE = re.compile(
    # No "^": _VICTIM_AFTER_RE is used with .match(text, pos), which already
    # anchors at pos - and "^" there matches only at the string's real start.
    r"(?i)[ \t]*(?:\w+[ \t]+){0,2}(?:will|may|might|shall|would)[ \t]+"
    r"(?:(?:also|temporarily|briefly)[ \t]+)?(?:be[ \t]+)?(?:unavailable|affected"
    r"|impacted|suspended|updated|interrupted|delayed|disabled)")
_PLAIN_THIRD_PARTY_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:PAGCOR|(?i:Telegram|WhatsApp|Lark|Feishu)(?=[ \t]+"
    r"(?i:will|is|has|maintenance|server|servers|app|service|outage|down)))"
    r"(?![A-Za-z0-9])")


def _third_party_subject(text: str):
    m = _THIRD_PARTY_PARTNER_RE.search(text)
    if m and not _VICTIM_AFTER_RE.match(text, m.end()):
        return m
    for rx in (_THIRD_PARTY_RE, _PLAIN_THIRD_PARTY_RE):
        for m in rx.finditer(text):
            head = re.split(r"[。．.;；\n]", text[max(0, m.start() - 40):m.start()])[-1]
            if _OWN_BEFORE_RE.search(head):
                continue
            if _VICTIM_AFTER_RE.match(text, m.end()):
                continue
            return m
    return None


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
    if owner:
        # R1.60: written TO the row's own provider ("Dear PP team, we will have
        # maintenance", "Hi Pragmatic team, FYI our site…") - the speaker is
        # the operator, and its window is not the provider's.
        salute = _salutes(text, _names_for(provider))
        if salute:
            return ask(f"written to {provider} (“{salute}”), so the speaker is the "
                       f"operator - not {provider}'s own notice")
        # R1.66: a FORWARDED notice is someone else's words; whose it is must be
        # read by a person.
        fw = _RELAY_MARK_RE.search(text or "")
        if fw:
            return ask(f"a forwarded notice (“{fw.group(0).strip()}”) - whose "
                       f"maintenance it is cannot be read from it")
        sm = _SAMPLE_MARK_RE.search(text or "")
        if sm:
            return ask(f"a test / template / example message (“{sm.group(0).strip()}”)"
                       f" - not a notice to write")
        # R1.64: one message listing several providers' windows ("Pragmatic
        # Play: <X>\nEvolution: <Y>") wrote the earliest line - another
        # provider's - onto this row.
        if fill and _multi_provider_schedule(text, [provider] + list(others)
                                             + list(shared_with or []) + _other_studios()):
            return ask("a schedule listing several providers' windows - which "
                       "line is this row's is not certain")
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
        m = _third_party_subject(text or "")
        if m:
            return ask(f"third-party maintenance (“{m.group(0)}”), not "
                       f"{provider}")
        mine = {_norm(n) for n in _names_for(provider)} | {_norm(p) for p in shared}
        # R1.66: a studio that is not a row in this Base ("NetEnt maintenance
        # on <X>", "…Regards, Relax Gaming Team") is somebody else too.
        for other in list(others) + _other_studios():
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


# How many written-but-unannounced cards one scope keeps for a retry (F71).
_UNANNOUNCED_KEEP = 5


def _hold_announcement(sc: dict, res: dict, *, key: str, done: dict,
                       what: str) -> dict:
    """Queue the card of a write that LANDED but whose card was never delivered.

    -> ledger extras to record beside the write. F71: the Base PUT succeeded,
    the card and its text fallback both failed, and the notice was ledgered
    'filled' - final, nobody told, and /vacheck said a card was posted. The
    write itself stays final (re-writing would re-announce nothing new and risk
    racing a hand edit); only the CARD is re-sent, on this group's next visits,
    up to VAWATCH_WRITE_ATTEMPTS sends in all. A send that timed out is not
    queued: it may have arrived, and the retry would post it twice.
    """
    if not done.get("wrote") or done.get("carded"):
        return {}
    if done.get("card_unknown"):
        res["card_unknown"] += 1
        return {"card": "unknown", "card_error": done.get("card_error") or ""}
    und = done.get("undelivered")
    extra = {"card": "failed", "card_error": done.get("card_error") or ""}
    if not und or _write_attempts() <= 1:
        # R1.68: with VAWATCH_WRITE_ATTEMPTS=1 the card is not re-sent - but it
        # must still be COUNTED. This returned before anything was recorded, so
        # a row written with no card was invisible to /vacheck and the log.
        res["card_failed"] = int(res.get("card_failed") or 0) + 1
        print(f"[vawatch] {key}: written, but its card was not delivered "
              f"({done.get('card_error') or 'no reason'}) and is NOT re-sent "
              f"(VAWATCH_WRITE_ATTEMPTS={_write_attempts()}) - nobody in the "
              f"Laboratory group has seen this write", flush=True)
        return extra
    q = [a for a in sc.get("unannounced") or [] if a.get("key") != key]
    q.append({"key": key, "what": what, "card": und.get("card") or {},
              "text": str(und.get("text") or "")[:6000], "at": _now_str(),
              "attempts": 1, "error": done.get("card_error") or ""})
    sc["unannounced"] = q[-_UNANNOUNCED_KEEP:]
    res["unannounced"] += 1
    print(f"[vawatch] {key}: written, but its card was not delivered "
          f"({done.get('card_error') or 'no reason'}) - it is re-sent on the next "
          f"visit", flush=True)
    return extra


def _retry_announcements(sc: dict, res: dict) -> bool:
    """Re-send the queued cards of writes that landed unannounced (F71).

    -> True when anything was sent (so the caller saves the ledger)."""
    keep, sent_any = [], False
    for a in list(sc.get("unannounced") or []):
        card = json.loads(json.dumps(a.get("card") or {}))
        late = (f"⏱ Announced late — this was written to the Base at "
                f"{a.get('at')}, but its card could not be delivered then.")
        try:
            card["body"]["elements"].append(
                {"tag": "div", "text": {"tag": "lark_md", "content": f"_{late}_"}})
        except Exception:             # noqa: BLE001
            pass
        sent = _deliver(card, late + "\n\n" + str(a.get("text") or ""))
        sent_any = True
        if sent["carded"] or sent["unknown"]:
            res["announced_late"] += 1
            res["details"].append({"key": a.get("key") or "", "action": "announced-late",
                                   "why": a.get("what") or "",
                                   "carded": sent["carded"]})
            continue
        a["attempts"] = int(a.get("attempts") or 1) + 1
        a["error"] = sent["error"]
        if a["attempts"] >= _write_attempts():
            print(f"[vawatch] {a.get('key')}: gave up announcing a write after "
                  f"{a['attempts']} sends ({sent['error']})", flush=True)
            res["unannounced_dropped"] += 1
            res["details"].append({"key": a.get("key") or "",
                                   "action": "announce-gave-up",
                                   "error": sent["error"], "carded": False})
            continue
        keep.append(a)
    sc["unannounced"] = keep
    return sent_any


# How far below the last-seen id a whole read must sit to be a RESTART, not a
# few deleted bubbles (R1.52). Telegram ids climb by one per message, so a busy
# group's genuine history is never this far behind its own newest bubble.
_MID_RESTART_GAP = 50


# R1.58: what a ledger entry's reason says when its message WITHDREW something.
_WITHDRAW_WHY_RE = re.compile(
    r"cancel|retract|void|withdraw|called\s+off|on\s+hold|not\s+take\s+place"
    r"|completed\s*/\s*cancelled|作废|作廢|撤回|取消|不进行|不進行|发错|發錯|wrong\s+group",
    re.I)


def _pending_withdrawn(d: dict, bk: str, p: dict, pn: float, seen) -> Optional[tuple]:
    """(label, entry) when the deferred notice ``p`` was withdrawn after it was
    deferred, else None.

    R1.58: _newer_ask counts only CARDED messages with a HIGHER id, so three ways
    of voiding the deferred notice never stopped it: an in-place EDIT of its own
    bubble ("[VOID] ...", "【作废】...", same id), a follow-up that cancels it
    ("The maintenance on 2026-09-28 is on hold", "2026-09-28维护不进行了" -
    ledgered, not carded, because the row did not hold that window), and the
    bubble being DELETED. The window was then written, with the original text as
    Remark, and announced as scheduled.
    """
    for k, v, m in _scope_entries(d, bk):
        if m is None or m < pn or k == p.get("key"):
            continue
        why = " ".join(str(v.get(x) or "") for x in ("why", "outcome", "reason"))
        if _WITHDRAW_WHY_RE.search(why) or (m == pn and str(v.get("outcome") or "")
                                             in _ASKED_OUTCOMES):
            raw = _KEY_MID_RE.search(k[len(bk):])
            return (raw.group(1) if raw else f"{m:g}", v)
    if seen:
        nums = [x for x in seen if x is not None]
        if nums and min(nums) <= pn <= max(nums) and pn not in nums:
            return ("(deleted)", {"why": "its bubble is no longer in the chat"})
    return None


_UNCHANGED_RE = re.compile(
    r"(?i:\bmaintenance\b[^.;\n]{0,20}\b(?:itself\s+)?(?:is|remains?|stays?)\s+"
    r"(?:unchanged|as\s+(?:planned|scheduled|announced))\b|\b(?:will\s+)?(?:proceed|go\s+ahead)"
    r"\s+as\s+(?:planned|scheduled|announced)\b)|维护(?:时间)?(?:不变|照常进行|如期进行)"
    r"|維護(?:時間)?(?:不變|照常進行|如期進行)")


# #192 / #278: VAWATCH_CLEAR_ENABLED=1 blanked a live row for "Is the maintenance
# on <X> cancelled" (no "?"), "Just to confirm the maintenance on <X> is
# cancelled", "…的维护是不是取消", "… still on or cancelled", "… will be cancelled
# if there is no issue" and "… for the Brazil region is cancelled". None of
# them is the provider withdrawing its window; each is carded instead.
_CLEAR_DOUBT_RE = re.compile(
    r"(?i:^\W*(?:is|are|was|were|did|does|do|has|have|can|could|will|would|should"
    r"|shall|may)\b"
    r"|\b(?:just\s+)?to\s+confirm\b|\b(?:please|pls|kindly)\s+(?:help\s+)?confirm\b"
    r"|\bconfirm\s+(?:if|whether|that)\b|\bwhether\b|\bstill\s+on\b|\bor\s+not\b"
    r"|\b(?:if|unless|provided|in\s+case|depending|subject\s+to)\b"
    r"|\bfor\s+(?:the\s+)?(?:\w+\s+){1,3}(?:region|market|jurisdiction|country|brand"
    r"|product|server|environment|site|merchant|operator|lobby|table|game)s?\b"
    r"|\b(?:\w+\s+)?(?:region|market|jurisdiction|country|brand|product|lobby)s?\s+only\b"
    r"|\bonly\s+(?:for|in|on)\b)"
    r"|是不是|是否|有没有|有沒有|还是|還是|如果|若是|假如|视情况|視情況|看情况|看情況"
    r"|确认一下|確認一下|请确认|請確認|仅限|僅限|只限|部分")


def _clear_is_safe(text: str, why: str, *, provider: str, group: str,
                   others: list) -> bool:
    """May this cancellation BLANK the row (VAWATCH_CLEAR_ENABLED=1)?

    R1.59: the blanking rule was "a cancel word next to a maintenance word", so
    with the flag on it blanked a live row for "Is the maintenance on <X>
    cancelled?", "Bets placed during the maintenance on <X> will be cancelled",
    "Evolution / CasinoPlus maintenance on <X> has been cancelled" and "the
    maintenance on <X> for UAT is cancelled" - sixteen shapes, none of them the
    provider withdrawing its own window. Blanking is the one irreversible write
    this watcher makes, so each of these must hold; anything less is carded:
      * the PARSER says the maintenance itself was cancelled (its object-noun
        and future-frame guards are what refuse the bets / rounds / promo);
      * the message is not a question;
      * it is this provider's own statement (_attribute on a clear);
      * no test environment is named.
    """
    if not _CANCEL_REASON_RE.search(why or ""):
        return False
    try:
        import noticeparse as _np
        if any(_CANCEL_TEXT_RE.search(text, a, b)
               for a, b in _np._question_sentences(text)):
            return False
        if _np._NONPROD_RE.search(text):
            return False
    except Exception:                 # noqa: BLE001 - unreadable: do not blank
        return False
    if re.search(r"[?？]|[吗嗎]\s*$", text or ""):
        return False
    if _CLEAR_DOUBT_RE.search(text or ""):
        # #192 / #278: a question without its "?", a request to confirm, a
        # conditional or a cancellation of one region/product only.
        return False
    if _UNCHANGED_RE.search(text or ""):
        # "…will be cancelled; the maintenance itself is unchanged" - the
        # provider says outright that the window stands.
        return False
    verdict = {"action": "clear", "start": None, "end": None}
    return _attribute(text, verdict, provider=provider, group=group,
                      shared_with=[], others=list(others or [])) is None


def _apply_pending(d: dict, sc: dict, res: dict, *, provider: str, group: str,
                   record_id: str, now: datetime, checkpoint, seen=None) -> bool:
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
    he = _from_iso(p.get("held_end"))
    if he is not None and he > now:
        # #89: deferred against a window the Base row held WITHOUT this
        # watcher having written it (no owner): wait for that one to end too.
        return True
    pn = _mid_num(p.get("mid"))
    asked = _newer_ask(d, _baseline_key(group, provider), pn,
                       skip=("deferred",)) if pn is not None else None
    withdrawn = (None if asked or pn is None else
                 _pending_withdrawn(d, _baseline_key(group, provider), p, pn, seen))
    if withdrawn:
        wmid, went = withdrawn
        sc["pending"] = [x for x in sc.get("pending") or [] if x is not p]
        nv = {"action": "needs_human", "start": ps, "end": pe,
              "reschedule": False, "stale": False,
              "reason": "a deferred window was not written: it was withdrawn"}
        sent = card_needs_human(
            group, provider, p.get("text") or "", nv,
            extra=(f"**The deferred window {_fmt_win(ps, pe)} was NOT written** "
                   f"— message {wmid} withdrew or replaced it "
                   f"({str(went.get('why') or went.get('outcome'))[:120]}). "
                   f"Enter it by hand if it still stands."))
        print(f"[vawatch] {group!r}: deferred window {_fmt_win(ps, pe)} dropped - "
              f"withdrawn by message {wmid}", flush=True)
        res["needs_human"] += 1
        res["details"].append({"key": p.get("key") or "", "action": "needs-human",
                               "why": nv["reason"], "carded": sent.get("carded", False),
                               "error": sent.get("error") or ""})
        return checkpoint()
    if asked:
        # Something NEWER than the deferred notice was put in front of a
        # person: "no maintenance this week" with the automatic clear off, a
        # reschedule with no readable window, a cancellation naming no window.
        # The deferred window was written anyway once the earlier one ended -
        # possibly over a row the person had already fixed from that card.
        # Another DEFERRED notice does not count: it is a separate outage
        # queued behind this one, not a decision about it.
        amid, aent = asked
        sc["pending"] = [x for x in sc.get("pending") or [] if x is not p]
        aoc = str(aent.get("outcome") or "")
        nv = {"action": "needs_human", "start": ps, "end": pe,
              "reschedule": False, "stale": False,
              "reason": ("a deferred window was not written: a newer message "
                         + ("retracted a post" if aoc == "retracted" else
                            "cancelled maintenance" if aoc == "cancelled" else
                            "was sent to a human"))}
        sent = card_needs_human(
            group, provider, p.get("text") or "", nv,
            extra=(f"**The deferred window {_fmt_win(ps, pe)} was NOT written** "
                   f"— message {amid}, newer than its notice, was already sent "
                   f"to a human ({str(aent.get('why') or aent.get('outcome'))[:120]})."
                   f" Enter it by hand if it still stands."))
        print(f"[vawatch] {group!r}: deferred window {_fmt_win(ps, pe)} dropped - "
              f"newer message {amid} was sent to a human", flush=True)
        res["needs_human"] += 1
        res["details"].append({"key": p.get("key") or "", "action": "needs-human",
                               "why": nv["reason"], "carded": sent.get("carded", False),
                               "error": sent.get("error") or ""})
        return checkpoint()
    verdict = {"action": "fill", "start": ps, "end": pe,
               "reschedule": bool(p.get("resched")), "stale": False,
               "reason": "deferred second outage"}
    os_, oe = _owner_window(owner)
    # R1.49: read the row back before the deferred write. The deferral was
    # decided against the window the watcher had written; a person who has
    # since set the row by hand (from the "second, later outage" card, say, to
    # 25/09 20:00-22:00) had that overwritten days later, and the card claimed
    # the row "still held" the old window. A row now holding an upcoming
    # window the watcher did not put there is a person's decision: nothing is
    # written and the deferred window is carded. A read that fails writes
    # nothing this tick.
    try:
        f = get_row(record_id) if record_id else {}
        rs_ms, re_ms = _cell_ms(f.get("Start Time")), _cell_ms(f.get("End Time"))
    except Exception as err:          # noqa: BLE001
        print(f"[vawatch] {group!r}: could not read {record_id} back before the "
              f"deferred write ({err!r}) - retried next tick", flush=True)
        p["attempts"] = int(p.get("attempts") or 0) + 1
        if p["attempts"] >= _write_attempts() and not p.get("readback_carded"):
            # #272: the pending entry used to be DROPPED here with no card at
            # all - the deferred outage was never written, even after the Base
            # recovered, nobody was told, and /vacheck pointed at "the red
            # card" that did not exist. A failed READ changes nothing on the
            # row, so the entry now stays and is tried every tick until the
            # read works (or the window ends); one card says so meanwhile.
            p["readback_carded"] = _now_str()
            nv = {"action": "needs_human", "start": ps, "end": pe,
                  "reschedule": False, "stale": False,
                  "reason": "a deferred window is not written yet: the row could "
                            "not be read back"}
            try:
                sent = card_needs_human(
                    group, provider, p.get("text") or "", nv,
                    extra=(f"**The deferred window {_fmt_win(ps, pe)} is NOT written "
                           f"yet** — the Base row could not be read back before "
                           f"the write on {p['attempts']} tick(s) ({err!r:.120}). "
                           f"It is tried again every tick until the read works; "
                           f"enter it by hand if it must be on the sheet now."))
            except Exception as err2:  # noqa: BLE001
                sent = {"carded": False, "error": repr(err2)}
            res["needs_human"] += 1
            res["details"].append({"key": p.get("key") or "", "action": "needs-human",
                                   "why": nv["reason"],
                                   "carded": sent.get("carded", False),
                                   "error": sent.get("error") or ""})
        res["write_failed"] += 1
        return checkpoint()
    rs = datetime.fromtimestamp(rs_ms / 1000, _tz()) if rs_ms is not None else None
    re_ = datetime.fromtimestamp(re_ms / 1000, _tz()) if re_ms is not None else None

    def _same(x, y):
        return x is not None and y is not None and abs((x - y).total_seconds()) < 60

    hand = (rs is not None and re_ is not None and re_ > now
            and not (_same(rs, os_) and _same(re_, oe))
            and not (_same(rs, ps) and _same(re_, pe)))
    if hand:
        sc["pending"] = [x for x in sc.get("pending") or [] if x is not p]
        nv = {"action": "needs_human", "start": ps, "end": pe,
              "reschedule": False, "stale": False,
              "reason": "a deferred window was not written: the row was changed "
                        "since it was deferred"}
        sent = card_needs_human(
            group, provider, p.get("text") or "", nv,
            extra=(f"**The deferred window {_fmt_win(ps, pe)} was NOT written** "
                   f"— the row now holds {_fmt_win(rs, re_)}, which it did not "
                   f"hold when this window was deferred and which has not ended. "
                   f"Decide which one the row should show."))
        print(f"[vawatch] {group!r}: deferred window {_fmt_win(ps, pe)} not written "
              f"- the row was changed to {_fmt_win(rs, re_)}", flush=True)
        res["needs_human"] += 1
        res["details"].append({"key": p.get("key") or "", "action": "needs-human",
                               "why": nv["reason"], "carded": sent.get("carded", False),
                               "error": sent.get("error") or ""})
        return checkpoint()
    held_s, held_e = (rs, re_) if rs is not None and re_ is not None else (os_, oe)
    gate = _llm_confirm(p.get("text") or "", verdict, provider=provider, group=group)
    if gate["verdict"] != "yes":
        p["confirm_tries"] = int(p.get("confirm_tries") or 0) + 1
        if gate.get("transient") and p["confirm_tries"] < _confirm_attempts():
            res["confirm_pending"] += 1
            return checkpoint()        # kept pending; asked again next tick
        sc["pending"] = [x for x in sc.get("pending") or [] if x is not p]
        why = ("the model did not confirm the deferred window {} - {}".format(
                   _fmt_win(ps, pe), gate.get("why") or "")
               if not gate.get("transient") else
               "the model could not be reached to confirm the deferred window {}".format(
                   _fmt_win(ps, pe)))
        nv = {"action": "needs_human", "start": ps, "end": pe,
              "reschedule": False, "stale": False, "reason": why}
        sent = card_needs_human(group, provider, p.get("text") or "", nv,
                                extra=_gate_card_extra(verdict, gate, p["confirm_tries"]))
        res["needs_human"] += 1
        res["model_refused"] += 1
        res["details"].append({"key": p.get("key") or "", "action": "needs-human",
                               "why": why, "carded": sent.get("carded", False),
                               "error": sent.get("error") or ""})
        return checkpoint()
    done = act_on_notice(
        p.get("text") or "", verdict, provider=provider, group=group,
        record_id=record_id,
        note=((f"**Written back:** a sooner outage replaced this window on the "
               f"row; that one ({_fmt_win(held_s, held_e)}) has now ended."
               if p.get("reinstated") else
               f"**Deferred window:** announced while the row still held "
               f"{_fmt_win(held_s, held_e)}, which has now ended.")
              if owner or rs is not None else "**Deferred window**, written now."))
    if done["wrote"]:
        prev = d["handled"].get(p.get("key")) or {}
        _set_owner(sc, mid=p.get("mid") or "", key=p.get("key") or "", text=p.get("text") or "",
                   verdict=verdict, kind="fill")
        held = _hold_announcement(sc, res, key=p.get("key") or "", done=done,
                                  what="deferred fill")
        if p.get("key"):
            _mark(d, p["key"], "filled",
                  {"h": prev.get("h"), "window": done["wrote"],
                   "why": "deferred second outage, written after the earlier "
                          "window ended", **held})
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
      * a fill or clear from a message older than one ALREADY CARDED to a
        person (needs-human, deferred, clear-skipped) is carded, never written:
        that person may have fixed the row by hand (F34/F35, human-fix case);
      * an edit whose text is the one already handled is already handled; an
        edit of the row's own notice that adds retraction wording, or no
        longer reads as that window, is carded; one that moves the window is
        written - the correction the edit hash exists for (G2.5);
      * an unseen bubble at or below the cold-start floor, or at or below the
        highest mid the ledger has evicted, is backlog (F35, F40);
      * a separate outage starting 24h+ after the row's still-future window is
        carded and deferred, then written once that window has ended - unless
        a newer message cancelled it or was carded to a person first; one
        ending 24h+ before it is written, and the card names what it replaced
        (G3.1);
      * the row's own notice vanishing from between two surviving bubbles is
        carded once (G2.4); a read that does not reach the previous visit is
        reported, and carded at most every _GAP_CARD_EVERY (F57).

    WHAT A PERSON IS TOLD (stage W3):

      * a needs_human / follow_quote straight from classify is carded only when
        the message names maintenance; "no maintenance downtime" is not a
        clear; at most VAWATCH_NEEDS_HUMAN_CAP red cards per row per day (F58);
      * a "no maintenance" reply in a group an open /provideraskmaintenance run
        asked is left to that run (F72);
      * a cancellation - a cancel word next to a maintenance word, windowed or
        not - of the window the row holds, whoever wrote it (read back), is
        carded; cleared only under VAWATCH_CLEAR_ENABLED=1 when it names that
        exact window and the row still holds it (F51);
      * a retraction / correction while the row's notice is still on screen
        above it is carded, naming the row's window (G2.3);
      * a photo / document beside maintenance wording is carded once (F59);
      * a write that landed but whose card never arrived is re-sent, card only,
        on the next visits; an ambiguous PUT is read back before it is called
        failed (F71, F80).

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
    if (not cold and group and len(d.get("order") or []) >= _LEDGER_CAP
            and sc.get("evicted") is None and not sc.get("revive_sweep")
            and not any(True for _ in _scope_entries(d, bk))):
        # F40, the deploy transition: a group baselined long ago whose EVERY
        # key was evicted under the old first-insertion trim (bd9fc12 shares
        # the key scheme), and not revisited since. _init_scope has nothing to
        # anchor an evicted mark on, so every bubble still on its screen -
        # decided months ago, possibly corrected by hand since - would read as
        # new: re-written and re-carded (the F40 revival). Such a scope gets
        # ONE careful sweep, recorded like the F69 rename sweep: the backlog
        # is marked cold-start, nothing is written, nothing is carded. A
        # quiet group with a genuinely empty history pays one silent sweep.
        cold = True
        # #233: an EMPTY read (the virtualised list not rendered yet) spent the
        # one careful sweep on nothing and stamped it, so the next read - the
        # real backlog - was acted on as new: the F40 revival again. The stamp
        # waits for a read that shows something; this sweep is careful anyway.
        if any(str(m.get("text") or "").strip() for m in (messages or [])):
            sc["revive_sweep"] = _now_str()
            print(f"[vawatch] {provider!r} in {group!r}: the capped ledger kept "
                  f"none of this group's keys - one careful sweep, acting on "
                  f"nothing", flush=True)
    res = {"seen": 0, "acted": 0, "ignored": 0, "already": 0,
           "cold_start": cold, "details": [],
           # `ignored` above stays the grand total main.py has always printed.
           # These split it; `acted` now counts only writes that SUCCEEDED, with
           # the failures counted separately instead of being reported as fills.
           "ignored_not_maintenance": 0, "ignored_unparsed": 0,
           # About maintenance, but a parser guard refused it (classify's
           # `refused`): a question, "no downtime", a UAT window.
           "ignored_refused": 0,
           "ignored_stale": 0, "needs_human": 0, "cleared": 0,
           "write_failed": 0, "retried": 0,
           "ignored_outbound": 0, "ignored_other_provider": 0,
           # The order/state rules above, each counted where it fired.
           "superseded": 0, "history": 0, "deferred": 0, "pending_written": 0,
           "confirm_pending": 0, "model_refused": 0,
           "deleted_notice": 0, "errors": 0, "gap": {}, "ledger_error": "",
           "renamed_from": "",
           # F71/F80: a write that landed but whose card was not delivered
           # (queued for a card-only retry), one whose card send timed out, a
           # late card for an earlier write, a card given up on, and a write
           # whose own outcome is unknown.
           "unannounced": 0, "card_unknown": 0, "announced_late": 0,
           "card_failed": 0,
           "unannounced_dropped": 0, "write_unknown": 0,
           # Stage W3: a completion/cancellation that changes nothing on this
           # row (was "not about maintenance", F51), a "no maintenance" reply
           # left to an open /provideraskmaintenance run (F72), a card held
           # back by the per-row cap (F58), a poster carded (F59).
           "ignored_done": 0, "left_to_providerask": 0, "capped": 0,
           "image_notice": 0,
           # Under force: the verdict the walk stopped at, when it wrote nothing.
           "force_stop": "",
           # The Base rows nothing on this path reads (APP=TEAMS, excluded,
           # blank), straight from the ledger cache - no Base call here.
           "unwatched": _unwatched_from_cache(d),
           # Where this process's last watch list came from (F78).
           "watch": dict(_LAST_SOURCE)}

    if shared_with is None:
        shared_with = _shared_from_watch(d, provider, group) if group else []
    others = _known_providers(d)
    msgs = list(messages or [])
    senders = _effective_senders(msgs)
    operators = _operator_senders()
    nums = [_mid_num(m.get("mid")) for m in msgs]
    known = [n for n in nums if n is not None]
    floor, last = sc.get("floor"), sc.get("last")
    evicted = sc.get("evicted")         # the highest mid _prune forgot (F40)
    # R1.52: the chat's message ids RESTARTED - it was re-created, or a basic
    # group was migrated to a supergroup, under the same Group Name. A normal
    # read always carries the newest bubble, at or above `last`; one whose
    # HIGHEST id is far below it is not late-rendering history. Every notice
    # after the restart sat below the old floor and was dropped as "history",
    # silently, until the new ids caught up (bd9fc12 filled them). The old
    # mid-based state means nothing against the new ids, so it is reset; the
    # row's window is left alone, and the message ids simply start over.
    # #273: a chat re-created (or migrated) after fewer than ~58 messages was
    # never detected - the ids fell short of the 50-id gap, so "Dear partners,
    # scheduled maintenance ..." at the new mid 2 was dropped as history. A
    # read whose NEWEST bubble is at or below half the old cold-start floor is
    # a restart too: late-rendering history always arrives with the newest
    # bubbles beside it, and a recent deletion never removes half the chat.
    if (known and last is not None and floor is not None
            and max(known) < float(floor)
            and (float(last) - max(known) > _MID_RESTART_GAP
                 or max(known) <= float(floor) / 2)):
        print(f"[vawatch] {provider!r} in {group!r}: message ids restarted "
              f"(read up to {max(known):g}, last seen {float(last):g}) - the "
              f"chat was re-created or migrated; its mid history is reset",
              flush=True)
        for k in ("floor", "last", "evicted", "owner"):
            sc.pop(k, None)
        sc["pending"] = []
        floor = last = evicted = None
        res["mid_restart"] = 1
    ordered = _order_check()
    if not ordered:
        floor = last = evicted = None   # no history floor, no gap (see _order_check)

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
    if not cold and sc.get("unannounced"):
        # F71: cards of earlier writes that landed unannounced, re-sent before
        # anything new so they arrive in order.
        try:
            if _retry_announcements(sc, res):
                halted = not checkpoint()
        except Exception as err:      # noqa: BLE001
            res["errors"] += 1
            print(f"[vawatch] late announcement for {group!r} failed: {err!r}",
                  flush=True)
    if not halted and not cold and not force and ordered and sc.get("pending"):
        try:
            halted = not _apply_pending(d, sc, res, provider=provider, group=group,
                                        record_id=record_id, now=now,
                                        checkpoint=checkpoint, seen=set(known))
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
        # F69 (#154, #155): on the first read after a RENAME, a bubble newer
        # than anything the old scope saw may be new. Only a fill of that kind
        # used to be carded; a cancellation of the row's window, a "postponed,
        # TBA" or a poster were all buried as cold-start. They are handled as
        # on a warm sweep - each of them only CARDS - and a fill is still carded,
        # never written (below).
        ni_ = nums[i]
        renamed_new = bool(cold and renamed and not force
                           and (old_last is None or ni_ is None or ni_ > float(old_last)))
        cold_i = cold and not renamed_new
        if not text:
            if (_is_poster(msg) and str(msg.get("mid") or "").strip() and not force
                    and not (_is_out(msg) and _skip_outbound())
                    and _needs_human_card() and _image_card_enabled()):
                # F59: a poster posted with no caption (see _captionless_poster).
                ni = nums[i]
                below = ni is not None and any(
                    x is not None and ni <= float(x) for x in (floor, evicted))
                try:
                    if _captionless_poster(d, msgs, i, res, cold=cold_i, history=below,
                                           provider=provider, group=group,
                                           shared_with=shared_with) \
                            and not checkpoint():
                        halted = True
                except Exception as err:  # noqa: BLE001
                    res["errors"] += 1
                    print(f"[vawatch] image bubble {msg.get('mid')} in {group!r} "
                          f"failed: {err!r}", flush=True)
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
            versions = _mid_versions(d, base_key) if edited else []
            # What the bubble said before this edit, as far as the ledger knows.
            base = {"h": next((v.get("h") for v in versions if v.get("h")), None),
                    "rx": any(v.get("rx") for v in versions)} if versions else None
            # The version handled LAST, not any version: A (10:00) was filled,
            # edited to 14:00 (written), then edited BACK to the original text -
            # matching against every earlier version called that "text
            # unchanged" and left 14:00 on the row for good while the bubble
            # said 10:00 (G2.5). Only the latest version tells whether this
            # edit changed anything. The owner's own entry wins when it is one
            # of them: that is the text the row was written from.
            latest = None
            if versions:
                own_key = str((sc.get("owner") or {}).get("key") or "")
                own_ent = d["handled"].get(own_key) if own_key else None
                if (own_key == base_key or own_key.startswith(base_key + "|ed:")) \
                        and isinstance(own_ent, dict):
                    latest = own_ent
                else:
                    # Two versions stamped in the same second tie on "at"; the
                    # one with this text wins then, so a re-render is not a
                    # re-edit (#88).
                    latest = max(versions, key=lambda v: (str(v.get("at") or ""),
                                                          v.get("h") == h))
            # G2.5 (#88): an edit back to a version handled BEFORE - A, edited
            # to 14:00, back to A, then to 14:00 again - has a key already in
            # the ledger, so it was "already handled" before the latest-version
            # comparison below could run, and the row kept A while the bubble
            # said 14:00. A seen key is final only when its text is also the
            # version handled last.
            reedit = bool(edited and latest is not None and latest.get("h")
                          and latest.get("h") != h)
            if _seen(d, key) and not force and not reedit:
                _touch(d, key)
                if edited:
                    _touch(d, base_key)
                res["already"] += 1
                res["details"].append({"key": key, "action": "already handled"})
                continue
            if edited and not force and latest is not None and latest.get("h") == h:
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
            below_floor = floor is not None and n is not None and n <= float(floor)
            below_evicted = (evicted is not None and n is not None
                             and n <= float(evicted))
            if ((below_floor or below_evicted)
                    and not cold and not force and not versions
                    and key not in d["handled"]):
                # At or below the highest mid of this group's cold-start read,
                # and never decided: history that became visible only now - a
                # bubble the lazy list had not rendered, a raised
                # VAWATCH_READ_COUNT, a key evicted from the ledger. The cold
                # sweep exists so history is recorded, not acted on; it used to
                # protect only what the first read happened to show (F35, F40).
                # At or below a mid the ledger has EVICTED is the same thing
                # for a warm group: that message was on screen and decided,
                # and the ledger forgot it (see _note_evicted).
                # An "edited" bubble is no exception unless this mid was seen
                # before: Web K shows the marker for ever, so a bubble edited
                # months ago that renders late is backlog like any other.
                mark("cold-start", {"why": "older than this group's first read "
                                           "- backlog that became visible later"
                                    if below_floor else
                                    "at or below a message the ledger has "
                                    "evicted - already decided once"})
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
            if action == "clear" and _NOT_A_CLEAR_RE.search(text):
                # F58: "...deployed tonight with no maintenance downtime" is not
                # "nothing is planned". It was a red card, and with the
                # automatic clear on it would have BLANKED a real window.
                hit = _NOT_A_CLEAR_RE.search(text).group(0)
                verdict = dict(verdict, action="ignore",
                               reason=f"no scheduled-maintenance wording - “{hit}” "
                                      f"is not a 'no maintenance' answer")
                action, why = "ignore", verdict["reason"]
            # With VAWATCH_ORDER_CHECK=0 there is no owner to compare against,
            # which switches off every order rule below at once.
            owner = sc.get("owner") if ordered else None
            own_n = _mid_num((owner or {}).get("mid"))

            if (edited and not force and n is not None and own_n is not None
                    and n == own_n
                    and (_owner_live(owner, now)
                         # A row filled BEFORE this release (_init_scope's
                         # legacy owner): its window is unknown, so "live"
                         # cannot be asked. A still-future fill is enough to
                         # card an added "[VOID]" instead of re-announcing the
                         # voided window; any other edit falls through below
                         # exactly as it did before (the window is unknown, so
                         # it is never judged "the same"). The added "[VOID]"
                         # now reads as a retraction (needs_human), so that
                         # verdict is let in too when the edit added the words.
                         or (owner.get("legacy") and action == "fill"
                             and not verdict.get("stale"))
                         or (owner.get("legacy") and action == "needs_human"
                             and rx and not (base or {}).get("rx")))):
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
                    # A legacy owner's window survives only as the display
                    # string its 'filled' entry recorded.
                    shown = (_fmt_win(os_, oe) if (os_ and oe) else
                             str((d["handled"].get(owner.get("key") or "") or {})
                                 .get("window") or "(window unknown)"))
                    hit = _EDIT_RETRACT_RE.search(text) if added_rx else None
                    nh_extra = (f"**The notice that set {shown} on this "
                                f"row was EDITED** — "
                                + (f"retraction wording was added (“{hit.group(0)}”)."
                                   if hit else
                                   f"it now reads: {action} — {why or 'no reason'}.")
                                + " The row still shows that window.")
                    verdict = dict(verdict, action="needs_human",
                                   reason="the notice this row was filled from was "
                                          "edited")
                    action, why = "needs_human", verdict["reason"]

            # F51: a cancellation is a cancel word next to a maintenance word,
            # whatever the parser's reason. "Update: maintenance cancelled." and
            # "The maintenance has been called off." fail the wording gate and
            # came back "no scheduled-maintenance wording" - the same silent
            # drop by another route, and /vacheck called them "not about
            # maintenance". A completion is never one of these.
            # #70 / #190: _is_cancel_text also takes the weaker words ("no
            # longer valid", "on hold", "suspended", 不进行了) with the parser's
            # own cancelled verdict.
            is_cancel = bool(action == "ignore" and _is_cancel_text(text, why))
            if is_cancel and ordered and not cold_i and not force:
                # A NEWER message cancelling maintenance while the row still
                # holds a window this watcher wrote. It names that window, or
                # none ("the maintenance has been cancelled"): either way the
                # row is now wrong and a person has to clear it. The one
                # exception is below: with VAWATCH_CLEAR_ENABLED=1 a
                # cancellation naming EXACTLY the window the row still holds
                # (read back) blanks it (F51).
                #
                # The same goes for a DEFERRED window (G3.1) the cancellation
                # names, or when it names none: "the maintenance on 01/10 has
                # been cancelled" used to be ignored silently, and 01/10 was
                # then written automatically - with a "Scheduled maintenance"
                # card - once the row's earlier window ended. Dropping it
                # writes nothing, so it is done here; the card says so.
                newer = not (owner is not None and n is not None
                             and own_n is not None and n <= own_n)
                row_live = owner is not None and _owner_live(owner, now) and newer
                os_, oe = _owner_window(owner) if row_live else (None, None)
                row_src = "the row still shows"
                if not row_live and newer and record_id:
                    # F51: a row this watcher did NOT fill - by hand, by
                    # /provideraskmaintenance, before the ledger existed - can
                    # hold the cancelled window just as well. Read it back.
                    rs, re_ = _row_window(record_id)
                    if rs and re_ and re_ > now:
                        row_live, os_, oe = True, rs, re_
                        row_src = "the Base row shows (not filled by this watcher)"
                pend_old = [p for p in sc.get("pending") or []
                            if n is None or _mid_num(p.get("mid")) is None
                            or _mid_num(p.get("mid")) < n]
                w = None
                if row_live or pend_old:
                    try:
                        import noticeparse
                        w = noticeparse.find_window(text, now=now)
                    except Exception:     # noqa: BLE001
                        w = None
                row_hit = row_live and (w is None or _near(w[0], w[1], os_, oe))
                pend_hit = [p for p in pend_old
                            if w is None or _near(w[0], w[1], _from_iso(p.get("start")),
                                                  _from_iso(p.get("end")))]
                exact = bool(row_hit and w is not None and os_ and oe
                             and _ms(w[0]) == _ms(os_) and _ms(w[1]) == _ms(oe))
                if (exact and not pend_hit and _clear_enabled() and record_id
                        and not shared_with
                        and _clear_is_safe(text, why, provider=provider, group=group,
                                           others=others)
                        and _confirm_fields(record_id, {"Start Time": _ms(os_),
                                                        "End Time": _ms(oe)}) is True):
                    # VAWATCH_CLEAR_ENABLED=1, and the cancellation names EXACTLY
                    # the window the row holds - read back just now, so a hand
                    # edit since is never blanked. Anything less (no window
                    # named, a nearby one, one of two announced, a shared
                    # group) is carded below: a wrong blank is worse than a
                    # stale window.
                    done = act_on_clear(
                        text, verdict, provider=provider, group=group,
                        record_id=record_id, remark=text,
                        title=f"\U0001F9F9 Maintenance CANCELLED · {provider}",
                        headline=(f"**The provider CANCELLED {_fmt_win(os_, oe)}** "
                                  f"— the window this row held."))
                    held = _hold_announcement(sc, res, key=key, done=done,
                                              what="cancellation clear")
                    mark("cleared" if done["wrote"] else "clear-failed",
                         {"window": done["wrote"] or "", "error": done["error"],
                          "why": "the provider cancelled the row's window", **held,
                          **({"unknown": 1} if done.get("unknown") else {})})
                    if done["wrote"]:
                        _set_owner(sc, mid=mid, key=key, verdict=verdict, kind="clear")
                        res["acted"] += 1
                        res["cleared"] += 1
                    else:
                        res["write_failed"] += 1
                        res["write_unknown"] += int(bool(done.get("unknown")))
                    res["details"].append({"key": key, "action": "cleared",
                                           "why": "cancelled", "window": done["wrote"],
                                           "error": done["error"],
                                           "record_id": done.get("record_id") or "",
                                           "carded": done.get("carded", False),
                                           "card_error": done.get("card_error") or ""})
                    if not checkpoint():
                        halted = True
                        break
                    continue
                if row_hit or pend_hit:
                    lines = []
                    if row_hit:
                        lines.append(f"**The provider says the maintenance was "
                                     f"CANCELLED** — {row_src} "
                                     f"{_fmt_win(os_, oe)}. If that is the one "
                                     f"called off, clear the row by hand.")
                    if pend_hit:
                        sc["pending"] = [p for p in sc.get("pending") or []
                                         if p not in pend_hit]
                        lines.append(
                            "**Deferred window dropped** — "
                            + ", ".join(_fmt_win(_from_iso(p.get("start")),
                                                 _from_iso(p.get("end")))
                                        for p in pend_hit)
                            + " will NOT be written automatically"
                            + (" (the cancellation names no window)." if w is None
                               else "."))
                    nh_extra = "\n".join(lines)
                    verdict = dict(verdict, action="needs_human",
                                   reason="the provider cancelled maintenance the "
                                          "row still shows" if row_hit else
                                          "the provider cancelled a deferred "
                                          "maintenance window")
                    action, why = "needs_human", verdict["reason"]

            if ((action in ("ignore", "follow_quote")
                 # A needs_human of classify's own is carded with ITS reason (an
                 # unknown zone, two windows); only the window-less reschedule,
                 # which the F58 gate below would drop, is taken as a follow-up.
                 or (action == "needs_human" and verdict.get("reschedule")))
                    and not nh_extra
                    and ordered and not cold_i and not is_cancel
                    and not _CANCEL_NEG_RE.search(text)
                    and not verdict.get("start")
                    and owner is not None and _owner_live(owner, now)
                    and n is not None and own_n is not None and n > own_n
                    and (own_n in known or _about_maintenance(text)
                         or _names_owner_day(text, owner)
                         or _NOTICE_NOUN_RE.search(text))):
                # G2.3: "Sorry wrong group", "Please ignore the notice above",
                # "Correction: the date should be 25/09", 请忽略上一条消息，发错群了.
                # A follow-up like that names no window of its own and failed
                # the wording gate, so it was dropped as "not about
                # maintenance" while the row kept the window it withdrew. It is
                # carded while the row's window is still ahead and the row's
                # OWN notice is on screen above it (in this read) - "the
                # message above" then has one meaning - OR, once the notice has
                # scrolled off between visits in a busy group, when the
                # follow-up itself names maintenance or the row's own date
                # ("Correction: the maintenance date should be 25/09 instead
                # of 24/09" three days later left 24/09 on the row, final and
                # uncarded). "Please ignore" in ordinary chat with neither
                # still does not raise a card.
                #
                # Not gated on `force` any more: /vacheck force walks newest
                # first, so it met "Please disregard the above notice, sent by
                # mistake" FIRST, called it "not about maintenance", walked on
                # and re-wrote the withdrawn window a person had just blanked
                # (F56). As a needs-human it stops the walk instead.
                fu = _FOLLOWUP_RE.search(text)
                prev_n = next((nums[j] for j in range(i - 1, -1, -1)
                               if str(msgs[j].get("text") or "").strip()), None)
                if fu and not (prev_n == own_n or _about_maintenance(text)
                               or _FOLLOWUP_OBJECT_RE.search(text)):
                    # Neither directly under the notice nor pointing at it.
                    fu = None
                if fu:
                    os_, oe = _owner_window(owner)
                    try:
                        import noticeparse
                        w = noticeparse.find_window(text, now=now)
                    except Exception:     # noqa: BLE001
                        w = None
                    nh_extra = (f"**A follow-up may RETRACT or CORRECT the notice "
                                f"that set {_fmt_win(os_, oe)} on this row** "
                                f"(message {owner.get('mid')}, above it in the "
                                f"group): "
                                f"“{fu.group(0)}”."
                                + (f" It mentions {_fmt_win(w[0], w[1])}." if w else "")
                                + " The row still shows that window — if the notice "
                                  "was withdrawn or corrected, fix the row by hand.")
                    verdict = dict(verdict, action="needs_human",
                                   reason="a follow-up may retract or correct the "
                                          "notice this row was filled from")
                    action, why = "needs_human", verdict["reason"]

            if (action == "ignore" and verdict.get("refused") == "question"
                    and not nh_extra and not cold_i and not force):
                # F63 (#142): "Maintenance on 25/09/2026 14:00-16:00 (GMT+8)?
                # Please confirm." - the parser rightly refuses to WRITE a
                # question, but it names a real window in the provider's group
                # and ended as a terminal ignore that nobody saw. A question
                # that states a window is put in front of a person.
                try:
                    import noticeparse
                    qw = noticeparse.find_window(text, now=now)
                except Exception:     # noqa: BLE001
                    qw = None
                if qw:
                    nh_extra = (f"**A QUESTION about a maintenance window** "
                                f"({_fmt_win(qw[0], qw[1])}) — someone may be asking "
                                f"to confirm it, or announcing it as a question. "
                                f"Nothing was written; check the group.")
                    verdict = dict(verdict, action="needs_human",
                                   reason="a question that states a maintenance window")
                    action, why = "needs_human", verdict["reason"]

            if (action in ("ignore", "follow_quote") and not nh_extra
                    and _is_poster(msg) and not cold_i and not force):
                # F59: a photo / document whose caption - or the provider's
                # message right beside it - talks about maintenance, with no
                # window we can read: the notice is probably IN the image. It
                # used to be retried as 'unparsed' three times and then dropped
                # silently. One card per bubble (per group when two rows share
                # it); a poster beside a READABLE notice only illustrates it
                # and is not carded.
                hint, in_cap = _poster_hint(msgs, i, text)
                if hint:
                    sent = _card_once(d, msg, group, shared_with, provider,
                                      lambda: card_image_notice(
                                          group, provider, text,
                                          str(msg.get("kind") or ""), hint, in_cap),
                                      res=res)
                    mark(_image_outcome(sent),
                         {"why": f"a {msg.get('kind')} beside maintenance wording "
                                 f"({why})", "error": sent.get("error") or ""})
                    res["ignored"] += 1
                    res["needs_human"] += 1
                    res["image_notice"] += 1
                    res["details"].append({"key": key, "action": "image-notice",
                                           "why": why, "carded": sent["carded"],
                                           "error": sent.get("error") or "",
                                           "card_by": sent.get("by") or ""})
                    if not checkpoint():
                        halted = True
                        break
                    continue

            if action == "ignore":
                # Cheap and side-effect free, so it runs even on a cold start.
                # A cancellation that reached here found nothing to card
                # against (the row empty, no owner, no deferred window): it is
                # still recorded as what it IS, so an older fill retried or
                # rendered later sees it and is carded, not written (F34/F35).
                outcome = ("unparsed" if _is_unparsed(why)
                           else "cancelled" if is_cancel else "ignored")
                sr = (_SELF_RETRACT_RE.search(text)
                      if outcome == "ignored" and not _is_out(msg) else None)
                if sr:
                    # #230 / #277: "Sorry, wrong group!" with nothing of the row's
                    # to card against. Recorded as what it is, so an older notice
                    # seen later, or a deferred one, is not written past it.
                    outcome = "retracted"
                    why = f"a follow-up retracts a post (“{sr.group(0)}”)"
                if outcome == "unparsed" and cold_i and not force:
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
                split = ""
                if (outcome == "unparsed" and not force and not verdict.get("undated")
                        and _unparsed_last_try(d, key)):
                    split = _split_notice_hint(msgs, i, text, now)
                    tm = (_TIMEISH_RE.search(text)
                          if not split and _about_maintenance(text) else None)
                    if tm:
                        # G1.3 (#57): "紧急维护：现在开始，约1小时", "Emergency
                        # maintenance in 30 minutes, for 1 hour" - an outage
                        # starting NOW, timed by a duration or "now" rather
                        # than a clock, so the parser's undated count stayed 0
                        # and it ended silently after the last retry.
                        split = (f"**It names maintenance and a time the parser "
                                 f"cannot place on the calendar** (“{tm.group(0)}”) — "
                                 f"it may be happening now. Check the group; nothing "
                                 f"was written.")
                if (outcome == "unparsed" and not force
                        and (verdict.get("undated") or split)
                        and _unparsed_last_try(d, key)):
                    # G1.3: "Emergency maintenance 15:00-16:00 (GMT+8)" states a
                    # clock but no date, so no day can be written. It was
                    # re-read _reparse_attempts() times (it might have been a
                    # half-rendered bubble) and then became final with no card
                    # at all. On the last try a person is told, once per group,
                    # and nothing is written. A message with maintenance wording
                    # and NO clock ("Thanks for the maintenance notice") has
                    # undated == 0 and still ends silently - unless it is half
                    # of a notice split over two bubbles, or points at an
                    # attachment (F59, `split`): those are told too.
                    sent = _card_once(d, msg, group, shared_with, provider,
                                      lambda: card_needs_human(
                                          group, provider, text,
                                          dict(verdict, action="needs_human"),
                                          extra=(split or
                                                 "**The notice states a time but no "
                                                 "date the parser can read** (read "
                                                 f"on {_reparse_attempts()} sweep(s)). "
                                                 "Check the group for the day.")),
                                      res=res)
                    mark(_note_outcome(sent), {"why": why, "error": sent["error"],
                                               **({"capped": 1} if sent.get("capped")
                                                  else {})})
                    res["ignored"] += 1
                    res["needs_human"] += 1
                    res["details"].append({"key": key, "action": "needs-human",
                                           "why": why, "carded": sent["carded"],
                                           "error": sent["error"],
                                           "card_by": sent.get("by") or "",
                                           "capped": bool(sent.get("capped"))})
                    if not checkpoint():
                        halted = True
                        break
                    continue
                mark(outcome, {"why": why})
                res["ignored"] += 1
                if outcome == "unparsed":
                    res["ignored_unparsed"] += 1
                elif is_cancel or _CANCEL_REASON_RE.search(why) or outcome == "retracted":
                    # A completion, or a cancellation of something this row
                    # does not hold: about maintenance, nothing to change (F51).
                    # A retraction with nothing to card against counts here too.
                    res["ignored_done"] += 1
                elif verdict.get("refused"):
                    # A parser guard spoke (a question, "no downtime", UAT).
                    # /vacheck used to call these "not about maintenance".
                    res["ignored_refused"] += 1
                else:
                    res["ignored_not_maintenance"] += 1
                res["details"].append({"key": key, "action": outcome, "why": why})
                if force and (is_cancel or not _NOT_MAINTENANCE_REASON_RE.search(why)
                              or _is_done_text(text)):
                    # Completed, cancelled, unreadable - or a reason this code
                    # does not know, which stops the walk on the side that
                    # writes nothing.
                    res["force_stop"] = (f"{outcome} — "
                                         + ("a cancellation: " if is_cancel
                                            and not _CANCEL_REASON_RE.search(why)
                                            else "")
                                         + ("says the maintenance is completed / "
                                            "over: " if _is_done_text(text)
                                            and not _CANCEL_REASON_RE.search(why)
                                            else "") + why)
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

            if cold and not force and not (renamed_new and action in ("needs_human",
                                                                       "follow_quote")):
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
                                              group, provider, text, nv, extra=extra),
                                          res=res)
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

            pa_why = (_providerask_owner(group)
                      if action == "clear" and not _clear_enabled() else "")
            if pa_why:
                # F72: this group was asked by /provideraskmaintenance, which
                # reads the same reply, files it (clearing the row itself when
                # its own clear is on, routing a shared group's answer to the
                # right row) and cards the result. The red "fill the row by
                # hand" from here - or, in the shared Hacksaw/YGG group, "names
                # none of them" - was a second, false card for the same answer.
                # Recorded exactly like a carded clear-skipped, so the order
                # rules still see that a newer "no maintenance" exists. With
                # VAWATCH_CLEAR_ENABLED=1 nothing changes: the clear is true.
                mark("clear-skipped", {"why": pa_why, "by": "providerask"})
                res["ignored"] += 1
                res["left_to_providerask"] += 1
                res["details"].append({"key": key, "action": "clear-skipped",
                                       "why": pa_why, "carded": False,
                                       "by": "providerask"})
                if not checkpoint():
                    halted = True
                    break
                if force:
                    res["force_stop"] = ("no maintenance — left to the open "
                                         "/provideraskmaintenance run")
                    stop_pos = pos
                    break
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

            # The provider has just voided this bubble by editing it. noticeparse
            # reads a voided notice as a retraction (needs_human) since the
            # "[VOID - wrong group] <notice>" carve-out that kept it a fill for
            # this code was removed - that carve-out also let a FRESH voided
            # post write the voided window. An older, superseded notice voided
            # this way is still silent here, as it was when it read as a fill.
            voided_edit = bool(edited and rx and not (base or {}).get("rx"))
            if ((action in ("fill", "clear") or (voided_edit and action == "needs_human"))
                    and n is not None and own_n is not None and n < own_n):
                # OLDER than the message whose window the row holds: a retry of
                # a write that failed while a newer reschedule succeeded (F34),
                # a bubble that rendered after the newer one was applied (F35),
                # an edit to a superseded notice (F41, G2.5). Each used to be
                # written last and win, and nothing afterwards put the newer
                # window back - its key was final.
                os_, oe = _owner_window(owner)
                if ((action == "fill" and _same_window(verdict, owner))
                        or (action == "fill" and owner.get("resched"))
                        or voided_edit):
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

            asked = (_newer_ask(d, bk, n)
                     if (action in ("fill", "clear") and ordered and not force
                         and n is not None) else None)
            if asked:
                # A NEWER message of this row was already put in front of a
                # person, who may have set the row by hand from that card. This
                # older one - a retry after a failed write (F34), a bubble
                # that rendered late (F35), an edit of an older notice - used
                # to be written over that fix. Messages run oldest first, so
                # this only fires on something older arriving in a LATER
                # sweep; ordinary reads never reach it.
                amid, aent = asked
                if str(aent.get("outcome") or "") == "retracted":
                    # #230: "Sorry, wrong group. Please ignore." read first, the
                    # notice it took back rendering on a later read.
                    nh_extra = (f"**A NEWER message ({amid}) RETRACTED a post** "
                                f"({str(aent.get('why') or '')[:100]}). Read as a "
                                f"{action}"
                                + (f" ({_fmt_win(verdict.get('start'), verdict.get('end'))})"
                                   if action == "fill" else "")
                                + " but NOT written — if this window still stands, "
                                  "fill the row by hand.")
                    verdict = dict(verdict, action="needs_human",
                                   reason="older than a message that retracted a post")
                elif str(aent.get("outcome") or "") == "cancelled":
                    # The newer message CANCELLED maintenance while the row was
                    # still empty, so there was no card; the cancellation is
                    # the reason this older window must not go on the sheet.
                    nh_extra = (f"**A NEWER message ({amid}) CANCELLED the "
                                f"maintenance** while the row was empty. Read as a "
                                f"{action}"
                                + (f" ({_fmt_win(verdict.get('start'), verdict.get('end'))})"
                                   if action == "fill" else "")
                                + " but NOT written — if this window still stands, "
                                  "fill the row by hand.")
                    verdict = dict(verdict, action="needs_human",
                                   reason="older than a message that cancelled maintenance")
                else:
                    nh_extra = (f"**Older than message {amid}, which was already "
                                f"sent to a human** ({str(aent.get('why') or aent.get('outcome'))[:120]}). "
                                f"Read as a {action}"
                                + (f" ({_fmt_win(verdict.get('start'), verdict.get('end'))})"
                                   if action == "fill" else "")
                                + " but NOT written — the row may already have been set "
                                  "by hand from that card.")
                    verdict = dict(verdict, action="needs_human",
                                   reason="older than a message already sent to a human")
                action, why = "needs_human", verdict["reason"]

            retract = (_newer_retraction_in_read(
                           msgs, nums, i, now,
                           started=bool(verdict.get("start") and verdict["start"] <= now))
                       if (action == "fill" and ordered and n is not None) else None)
            if retract:
                # The SAME read holds a newer bubble that cancels or withdraws
                # the maintenance ("Update: maintenance cancelled.", "Please
                # disregard the notice above, sent by mistake."). Messages run
                # oldest first, so this notice used to be written, and only
                # then was the retraction carded - the withdrawn window sat on
                # the sheet under a confident card (F34/F35 'disregard'). A
                # window that a newer message in the same screen takes back is
                # never written; the card says which message did. Under
                # /vacheck force too: the walk met the retraction first, called
                # it chat when no owner was set, and re-wrote the withdrawn
                # window a person had just blanked (F56) - here it stops on the
                # needs-human instead.
                rmid, rkind, rsnip = retract
                nh_extra = (f"**A NEWER message ({rmid}) {rkind} the maintenance**: "
                            f"“{rsnip}”. Read as a fill "
                            f"({_fmt_win(verdict.get('start'), verdict.get('end'))}) "
                            f"but NOT written — if the window still stands, fill "
                            f"the row by hand.")
                verdict = dict(verdict, action="needs_human",
                               reason=f"a newer message in the same read {rkind} "
                                      f"the maintenance")
                action, why = "needs_human", verdict["reason"]

            fill_note = ""
            displaced = None       # #90: a later window this write pushes off the row
            g31 = (action == "fill" and ordered
                   and not (n is not None and own_n is not None and n <= own_n))
            os_ = oe = None
            held_row = False
            if g31 and _owner_live(owner, now):
                os_, oe = _owner_window(owner)
            elif g31 and record_id:
                # G3.1 (#89): the row holds a still-future window this watcher
                # did not write - set by hand, by /provideraskmaintenance, or
                # before the ledger existed. The rule only knew the ledger
                # owner, so 01/10 went over a hand-set 24/09 with no word of
                # it. Read the row back, as the F51 cancellation rule does.
                rs, re_ = _row_window(record_id)
                if rs and re_ and re_ > now:
                    os_, oe, held_row = rs, re_, True
            vs, ve = verdict.get("start"), verdict.get("end")
            if g31 and os_ and oe and vs and ve:
                # G3.1: the row holds a window that has not ended, and this is
                # a DIFFERENT outage (a day or more apart, no reschedule verb).
                # Newest-wins stays the rule - it is how a correction without
                # reschedule wording lands - except in the one case where it
                # silently erased an imminent outage for a later one.
                #
                # G2.2 (#74, #76): "Scheduled maintenance: 26/09 ... Previously
                # announced 24/09 ... is cancelled" / 24/09维护取消，改为 26/09 -
                # the notice cancels the row's own window and names the new
                # one. It is a replacement, not a second outage: deferring it
                # left the cancelled 24/09 on the row under a "second outage"
                # card. Newest wins, and the card says what it replaced.
                replaces = _cancels_day(text, os_)
                if replaces:
                    fill_note = (f"⚠️ **Replaces {_fmt_win(os_, oe)}**, which this "
                                 f"notice says is cancelled.")
                resched = bool(verdict.get("reschedule"))
                # audit-2: a reschedule / extension of ANOTHER outage (the one
                # deferred behind the row's) bypassed the deferral and erased
                # the row's imminent window. See _about_other_outage.
                other = _about_other_outage(text, verdict, os_) if resched else ""
                later = vs >= oe + timedelta(hours=24) and not replaces
                src = (f"set on the Base row outside this watcher" if held_row
                       else f"set by message {owner.get('mid') or '?'}")
                if (later and resched and not other and not _names_day(text, os_)
                        and sc.get("pending")):
                    # "Rescheduled: maintenance moved to 30/09" while the row
                    # holds 25/09 AND 28/09 is deferred: which one moved cannot
                    # be told. Nothing is written or queued; a person decides.
                    nh_extra = (f"**A reschedule while a second outage is deferred.** "
                                f"The row keeps {_fmt_win(os_, oe)} ({src}); "
                                + ", ".join(_fmt_win(_from_iso(p.get("start")),
                                                     _from_iso(p.get("end")))
                                            for p in sc.get("pending") or [])
                                + f" is still queued behind it. This message moves a "
                                  f"maintenance to {_fmt_win(vs, ve)} without saying "
                                  f"which - nothing was written; fix the row (and the "
                                  f"queued window) by hand if needed.")
                    verdict = dict(verdict, action="needs_human",
                                   reason="a reschedule that may move either the row's "
                                          "window or a deferred one")
                    action, why = "needs_human", verdict["reason"]
                elif later and (not resched or other):
                    nv = dict(verdict, action="needs_human",
                              reason="a second, later outage while the row still "
                                     "holds an earlier one that has not ended")
                    extra = (f"**A SECOND, separate outage.** The row keeps the "
                             f"earlier window {_fmt_win(os_, oe)} (still ahead, "
                             f"{src}). This notice announces {_fmt_win(vs, ve)}"
                             + (f" - a reschedule, but {other}." if other else "."))
                    if _later_windows(verdict):
                        extra += "\n" + _later_note(verdict)
                    tail = (f"**Nothing was written yet** — it is written "
                            f"automatically once {_fmt_win(os_, oe)} has ended, "
                            f"unless a newer notice changes this row first. If it "
                            f"REPLACES the earlier window instead, correct the row "
                            f"by hand now.")
                    sent = _card_once(d, msg, group, shared_with, provider,
                                      lambda: card_needs_human(
                                          group, provider, text, nv, extra=extra,
                                          tail=tail), res=res)
                    outcome = "deferred" if _note_outcome(sent) != "card-failed" \
                        else "card-failed"
                    # A newer notice about the SAME deferred outage (within a
                    # day of it) replaces the older one - newest wins among the
                    # deferred just as it does on the row. A reschedule of a
                    # deferred outage also drops the one whose day it names.
                    pend = [p for p in sc.get("pending") or []
                            if p.get("key") != key
                            and not _near(_from_iso(p.get("start")),
                                          _from_iso(p.get("end")), vs, ve)
                            and not (other and _names_day(text, _from_iso(p.get("start"))))]
                    pend.append({"mid": mid, "key": key, "start": _iso(vs),
                                 "end": _iso(ve), "resched": resched,
                                 "text": text[:20000], "at": _now_str(),
                                 "attempts": 0,
                                 **({"held_start": _iso(os_), "held_end": _iso(oe)}
                                    if held_row else {})})
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
                elif (ve + timedelta(hours=24) <= os_ and not replaces and not resched):
                    # Sooner AND newer: both rules agree it goes on the row. The
                    # later window it displaces may still stand (a second
                    # outage) or be dead (this was a correction) - that cannot
                    # be told from here. #90: it used to be left for a person to
                    # "re-enter by hand after this one ends", and nothing ever
                    # did; it is now queued like any deferred outage and written
                    # back once this one has ended, unless a newer message
                    # changes the row first. A window set outside this watcher
                    # has no notice text to write back, so it is only named.
                    if not held_row and owner and owner.get("key"):
                        displaced = dict(owner)
                    fill_note = (f"⚠️ **Replaced a LATER window still ahead** "
                                 f"({_fmt_win(os_, oe)}, {src}). "
                                 + ("It is written back automatically once this one "
                                    "has ended, unless a newer message changes this "
                                    "row first - if it was withdrawn, say so in the "
                                    "group or clear it by hand then."
                                    if displaced else
                                    "If that maintenance still stands, re-enter it by "
                                    "hand after this one ends."))

            attach = (_points_at_attachment(msgs, i)
                      if action == "follow_quote" and not _is_out(msg) else "")
            if (action in ("needs_human", "follow_quote") and not nh_extra
                    and not verdict.get("parsed_action")
                    # The parser's clean-notice check (refused="doubt") turned a
                    # FILL into this needs_human: it read a window, so it is
                    # never "ordinary chat", whatever words it uses.
                    and verdict.get("refused") != "doubt"
                    and not _about_maintenance(text) and not attach):
                # F58: the verdict came straight from classify - a "postponed"
                # or "reschedule" with no window, a "please see below" / 详见附件 -
                # and nothing in the message names maintenance: "The payout
                # for player 88231 is postponed", "Can we reschedule the
                # call?", "请见截图". Ordinary CS chat, not a notice; it was a
                # red card per message (two in the shared Hacksaw/YGG group
                # before _card_once). A verdict produced by THIS module's own
                # rules (a cancellation, an edit, a follow-up, attribution) is
                # never gated: each of those is about a window on the row. Nor
                # is a pointer whose attachment is right there (R1.79): 详见附件
                # ON a document, "Please see below" over a caption-less poster.
                mark("ignored", {"why": f"{why} - but nothing in it names "
                                        f"maintenance: ordinary chat, not carded"})
                res["ignored"] += 1
                res["ignored_not_maintenance"] += 1
                res["details"].append({"key": key, "action": "ignored",
                                       "why": f"{why} - no maintenance wording"})
                continue

            if action in ("needs_human", "follow_quote"):
                # follow_quote cannot be resolved here: the scraper reads
                # bubbles, not the messages they quote. Carding it as
                # needs-human puts it in front of a person, which is the only
                # outcome left that is not a silent drop.
                if nh_extra:
                    extra = nh_extra
                elif attach:
                    extra = (f"**This message points at an attachment** ({attach}) "
                             f"that may hold a maintenance notice — open the group "
                             f"and read it; nothing was written.")
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
                                      group, provider, text, verdict, extra=extra),
                                  res=res)
                mark(_note_outcome(sent), {"why": why, "error": sent["error"],
                                           **({"capped": 1} if sent.get("capped")
                                              else {})})
                res["ignored"] += 1
                res["needs_human"] += 1
                res["details"].append({"key": key, "action": "needs-human",
                                       "why": why, "carded": sent["carded"],
                                       "error": sent["error"],
                                       "card_by": sent.get("by") or "",
                                       "capped": bool(sent.get("capped"))})
                if not checkpoint():
                    halted = True
                    break
                if force:
                    res["force_stop"] = f"needs a human — {why}"
                    stop_pos = pos
                    break
                continue

            if action == "clear":
                # G3.2: an upcoming window is never blanked by a plain "no
                # maintenance" (see _clear_refusal). Checked only when the
                # clear is switched on; off, nothing is blanked anyway.
                hold = (_clear_refusal(owner, provider, record_id, now)
                        if _clear_enabled() else "")
                done = ({"wrote": None, "record_id": "", "error": "",
                         "carded": False, "skipped": hold} if hold else
                        act_on_clear(text, verdict, provider=provider, group=group,
                                     record_id=record_id))
                if done.get("skipped"):
                    # The write half is switched off, or the row holds a window
                    # that has not ended; say so to a human rather than leaving
                    # the row as it is with nobody told. Once per group when two
                    # rows share it (F58/F72: this was the one card still sent
                    # by both rows).
                    sent = _card_once(
                        d, msg, group, shared_with, provider,
                        lambda: card_needs_human(
                            group, provider, text, verdict,
                            extra=(f"**The provider says there is no maintenance** — "
                                   f"the row was NOT cleared: {hold}."
                                   if hold else
                                   f"**The provider says there is no maintenance** — "
                                   f"the automatic clear is off ({done['skipped']}), "
                                   f"so the row still shows its previous window.")),
                        res=res)
                    mark("clear-skipped"
                         if _note_outcome(sent) != "card-failed" else "card-failed",
                         {"why": done["skipped"], "error": sent["error"],
                          **({"capped": 1} if sent.get("capped") else {})})
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
                held = _hold_announcement(sc, res, key=key, done=done, what="clear")
                mark("cleared" if done["wrote"] else "clear-failed",
                     {"window": done["wrote"] or "", "error": done["error"], **held,
                      **({"unknown": 1} if done.get("unknown") else {})})
                if done["wrote"]:
                    _set_owner(sc, mid=mid, key=key, verdict=verdict, kind="clear")
                    res["acted"] += 1
                    res["cleared"] += 1
                else:
                    res["write_failed"] += 1
                    res["write_unknown"] += int(bool(done.get("unknown")))
                res["details"].append({"key": key, "action": "cleared",
                                       "window": done["wrote"],
                                       "error": done["error"],
                                       "record_id": done.get("record_id") or "",
                                       "carded": done.get("carded", False),
                                       "card_error": done.get("card_error") or ""})
                if not checkpoint():
                    halted = True
                    break
                if force:
                    stop_pos, force_wrote = pos, bool(done["wrote"])
                    break
                continue

            gate = _llm_confirm(text, verdict, provider=provider, group=group)
            if gate["verdict"] != "yes":
                prev_ent = d["handled"].get(key)
                prev_ent = prev_ent if isinstance(prev_ent, dict) else {}
                tries = (int(prev_ent.get("attempts") or 0) + 1
                         if prev_ent.get("outcome") == "confirm-pending" else 1)
                if gate.get("transient") and tries < _confirm_attempts():
                    # A cold or unreachable model is not a verdict: try again on
                    # the next sweep; nothing is written and nothing is carded yet.
                    mark("confirm-pending", {"why": f"the model could not confirm "
                                                    f"{_fmt_win(verdict['start'], verdict['end'])}"
                                                    f" yet: {gate.get('why') or ''}"[:300]})
                    res["confirm_pending"] += 1
                    res["details"].append({"key": key, "action": "confirm-pending",
                                           "why": gate.get("why") or ""})
                    print(f"[vawatch] {group!r}: {_fmt_win(verdict['start'], verdict['end'])} "
                          f"held - confirm model unavailable (try {tries}): "
                          f"{gate.get('why')}", flush=True)
                    if not checkpoint():
                        halted = True
                        break
                    if force:
                        res["force_stop"] = ("held — the confirming model could not be "
                                             "reached; retried on the next sweep")
                        stop_pos = pos
                        break
                    continue
                why = ("the model did not confirm {} - {}".format(
                           _fmt_win(verdict["start"], verdict["end"]), gate.get("why") or "")
                       if not gate.get("transient") else
                       "the model could not be reached to confirm {} ({} tries)".format(
                           _fmt_win(verdict["start"], verdict["end"]), tries))
                nv = dict(verdict, action="needs_human", reason=why)
                extra = _gate_card_extra(verdict, gate, tries)
                if shared_with:
                    extra += f"\n**Shared group** — rows: {', '.join([provider] + list(shared_with))}"
                sent = _card_once(d, msg, group, shared_with, provider,
                                  lambda: card_needs_human(group, provider, text, nv,
                                                           extra=extra),
                                  res=res)
                mark(_note_outcome(sent), {"why": why[:300], "error": sent["error"], "model": 1,
                                           **({"capped": 1} if sent.get("capped") else {})})
                res["needs_human"] += 1
                res["model_refused"] += 1
                res["details"].append({"key": key, "action": "needs-human", "why": why,
                                       "carded": sent["carded"], "error": sent["error"],
                                       "card_by": sent.get("by") or "",
                                       "capped": bool(sent.get("capped"))})
                if not checkpoint():
                    halted = True
                    break
                if force:
                    res["force_stop"] = f"needs a human — {why}"
                    stop_pos = pos
                    break
                continue
            later = _later_windows(verdict)
            fill_note = "\n".join(x for x in (fill_note, _later_note(verdict)) if x)
            done = act_on_notice(text, verdict, provider=provider, group=group,
                                 record_id=record_id, note=fill_note)
            held = _hold_announcement(sc, res, key=key, done=done, what="fill")
            mark("filled" if done["wrote"] else "write-failed",
                 {"window": done["wrote"] or "", "error": done["error"], **held,
                  **({"unknown": 1} if done.get("unknown") else {}),
                  **({"later": [_fmt_win(s, e) for s, e in later[:5]]}
                     if later else {})})
            if done["wrote"]:
                _set_owner(sc, mid=mid, key=key, verdict=verdict, kind="fill",
                           text=text)
                if displaced:
                    # #90: the later window this sooner outage pushed off the
                    # row, queued to be written back once this one has ended.
                    sc["pending"] = ([p for p in sc.get("pending") or []
                                      if p.get("key") != displaced.get("key")]
                                     + [{"mid": displaced.get("mid") or "",
                                         "key": displaced.get("key") or "",
                                         "start": displaced.get("start"),
                                         "end": displaced.get("end"),
                                         "resched": bool(displaced.get("resched")),
                                         "text": displaced.get("text") or "",
                                         "at": _now_str(), "attempts": 0,
                                         "reinstated": 1}])[-5:]
                res["acted"] += 1
            else:
                res["write_failed"] += 1
                res["write_unknown"] += int(bool(done.get("unknown")))
            res["details"].append({"key": key, "action": "filled",
                                   "window": done["wrote"], "error": done["error"],
                                   "record_id": done.get("record_id") or "",
                                   "carded": done.get("carded", False),
                                   "card_error": done.get("card_error") or "",
                                   "later": [_fmt_win(s, e) for s, e in later[:5]]})
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
                # F36 (#232, #271): force on a COLD group wrote the newest
                # notice (10-01) and buried the older one - an imminent,
                # separate 09-25 - as backlog: never written, never carded. The
                # warm path's owner rule cards exactly that. Backlog stays
                # silent otherwise; a still-future SEPARATE outage is told.
                own_now = sc.get("owner") or {}
                os_, oe = _owner_window(own_now)
                try:
                    v = classify(t) if (force_wrote and os_ and oe) else {}
                except Exception:     # noqa: BLE001 - unreadable: plain backlog
                    v = {}
                if (v.get("action") == "fill" and not v.get("stale")
                        and not own_now.get("resched") and not _is_out(m)
                        and not _near(v.get("start"), v.get("end"), os_, oe)):
                    nv = dict(v, action="needs_human",
                              reason="an older, separate outage below the notice "
                                     "/vacheck force wrote")
                    extra = (f"**/vacheck force wrote the newest notice** "
                             f"({_fmt_win(os_, oe)}) on a group read for the first "
                             f"time. This OLDER message announces a separate outage, "
                             f"{_fmt_win(v.get('start'), v.get('end'))}, that was NOT "
                             f"written - enter it by hand if it still stands.")
                    try:
                        sent = _card_once(d, m, group, shared_with, provider,
                                          lambda: card_needs_human(
                                              group, provider, t, nv, extra=extra),
                                          res=res)
                    except Exception as err:  # noqa: BLE001
                        sent = {"carded": False, "error": repr(err)}
                    _mark(d, k, _note_outcome(sent), {"h": _body_hash(t),
                                                      "why": nv["reason"],
                                                      "error": sent.get("error") or ""})
                    res["needs_human"] += 1
                    res["details"].append({"key": k, "action": "needs-human",
                                           "why": nv["reason"],
                                           "carded": sent.get("carded", False)})
                    continue
                _mark(d, k, "cold-start", {"h": _body_hash(t),
                                           "why": "backlog at first run, older "
                                                  "than what /vacheck force read"})
                res["details"].append({"key": k, "action": "cold-start skip"})
            elif force_wrote:
                # Only what the write really displaced is superseded: an older
                # notice the written one RESCHEDULED, or one stating the same
                # window. Marking every older bubble that way buried an
                # imminent SEPARATE outage (09-25 under a 10-01 write, F36)
                # and a needs-human never carded (F68) as final, uncarded.
                # Anything else is left for the next tick, whose ordinary
                # rules card it: the owner rule for an older different window,
                # the plain needs-human card for a "postponed, TBA".
                own_now = sc.get("owner") or {}
                try:
                    v = classify(t)
                except Exception:     # noqa: BLE001 - unreadable: leave it for the next tick
                    v = {}
                if own_now.get("resched") or (v.get("action") == "fill"
                                              and _same_window(v, own_now)):
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
               provider: str, send, res: Optional[dict] = None) -> dict:
    """Send a needs-human card - once per MESSAGE in a shared group, not per row.

    Both Hacksaw and YGG read the same bubble, so the "names none of them" card
    (and any other needs-human) went out twice, ~9 minutes apart. The first row
    to card it records that here; the second records "needs-human" without a
    second card. Only a DELIVERED card is recorded, so a failed one still gets
    its retry from whichever row comes next.

    Also the per-row cap (F58, see ``_card_cap``): past it, the card is held
    back - returned as ``disabled`` + ``capped`` so the ledger records it as
    decided rather than retrying it - and ONE "limit reached" card goes out.
    """
    gk = _shared_card_key(msg, group) if shared_with else ""
    cards = d.setdefault("shared_cards", {}) if shared_with else {}
    prev = cards.get(gk) if gk else None
    if isinstance(prev, dict):
        return {"carded": False, "disabled": True, "error": "",
                "by": prev.get("by") or "the other row"}
    cap, st = _card_cap(), None
    if cap and _needs_human_card():
        now = _now_dt()
        bk = _baseline_key(group, provider)
        caps = d.setdefault("card_cap", {})
        st = caps.get(bk) if isinstance(caps.get(bk), dict) else {}
        since = _from_iso(st.get("since"))
        if since is None or now - since >= _CARD_CAP_EVERY:
            st = {"since": now.isoformat(), "n": 0, "noted": ""}
        caps[bk] = st
        if int(st.get("n") or 0) >= cap:
            if not st.get("noted"):
                try:
                    note = card_cap_reached(group, provider, cap,
                                            (_from_iso(st["since"]) or now)
                                            + _CARD_CAP_EVERY)
                except Exception as err:  # noqa: BLE001
                    note = {"carded": False, "error": repr(err)}
                if note.get("carded") or note.get("unknown"):
                    st["noted"] = _now_str()
            if res is not None:
                res["capped"] += 1
            if gk:
                # ...and the co-tenant row must not then send it after all.
                cards[gk] = {"at": _now_str(), "by": provider, "capped": 1}
            print(f"[vawatch] {group!r}/{provider!r}: needs-a-human card held back "
                  f"- {cap} already sent since {st.get('since')}", flush=True)
            return {"carded": False, "disabled": True, "capped": True,
                    "error": f"held back by VAWATCH_NEEDS_HUMAN_CAP={cap}"}
    sent = send()
    if st is not None and (sent.get("carded") or sent.get("unknown")):
        st["n"] = int(st.get("n") or 0) + 1
    if gk and sent.get("carded"):
        cards[gk] = {"at": _now_str(), "by": provider}
        for k in list(cards)[:-500]:    # bounded, oldest first
            cards.pop(k, None)
    return sent


def _poster_hint(msgs: list, i: int, caption: str) -> tuple:
    """(wording, in_caption): why media bubble ``i`` may BE a maintenance notice.

    Its own caption naming maintenance, or the provider's own message right
    before or after it doing so without a readable window ("【Maintenance
    Notice】" above a poster). Our own bubbles are not evidence (a question we
    asked is not the provider's word), and a neighbour that is a readable
    notice, a completion / cancellation, or a verdict carded on its own means
    the image is at most an illustration. ("", False) = no card.
    """
    if caption and _about_maintenance(caption):
        return caption, True
    for j in (i - 1, i + 1):
        if not 0 <= j < len(msgs) or _is_out(msgs[j]):
            continue
        t = str(msgs[j].get("text") or "").strip()
        if not t or not _about_maintenance(t):
            continue
        try:
            v = classify(t)
        except Exception:             # noqa: BLE001
            continue
        if (v.get("action") in ("ignore", "follow_quote")
                and not _CANCEL_REASON_RE.search(v.get("reason") or "")
                and not _CANCEL_TEXT_RE.search(t)):
            return t, False
    return "", False


def _image_outcome(sent: dict) -> str:
    """Like _note_outcome: a poster card nobody received is tried again."""
    return "image-carded" if (sent.get("carded") or sent.get("disabled")
                              or sent.get("unknown")) else "card-failed"


def _captionless_poster(d: dict, msgs: list, i: int, res: dict, *, cold: bool,
                        history: bool, provider: str, group: str,
                        shared_with: list) -> bool:
    """A photo / document with NO caption: card it only when the provider's
    message beside it names maintenance (see _poster_hint). -> True if carded.

    Nothing is recorded for an ordinary captionless photo (a CS screenshot, a
    promo banner): there is nothing to classify, and carding every one in 18
    provider groups would flood the Laboratory group (F59). A hinted one is
    recorded like any message - including "cold-start" on a group's first read
    and "history" below its floor, so enabling this never cards the backlog.
    """
    msg = msgs[i]
    key = _key(msg, group, provider)
    if _seen(d, key):
        _touch(d, key)
        return False
    for j in (i - 1, i + 1):
        if (0 <= j < len(msgs) and _is_poster(msgs[j]) and not _is_out(msgs[j])
                and _about_maintenance(str(msgs[j].get("text") or ""))):
            # An ALBUM: the captioned photo beside this one carries the
            # maintenance wording and is carded itself; this caption-less one
            # is the same notice's second page, not a second notice (F59's
            # two cards for one album).
            return False
    hint, _in_cap = _poster_hint(msgs, i, "")
    if not hint:
        return False
    if cold or history:
        _mark(d, key, "cold-start", {"why": "an image beside maintenance wording "
                                            "- backlog, not acted on"})
        return False
    kind = str(msg.get("kind") or "")
    sent = _card_once(d, msg, group, shared_with, provider,
                      lambda: card_image_notice(group, provider, "", kind, hint,
                                                False), res=res)
    _mark(d, key, _image_outcome(sent), {"why": f"a captionless {kind} beside "
                                                f"maintenance wording",
                                         "error": sent.get("error") or ""})
    res["seen"] += 1
    res["ignored"] += 1
    res["needs_human"] += 1
    res["image_notice"] += 1
    res["details"].append({"key": key, "action": "image-notice",
                           "why": "captionless image beside maintenance wording",
                           "carded": sent.get("carded", False),
                           "error": sent.get("error") or "",
                           "card_by": sent.get("by") or ""})
    return True


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
        if x.get("action") == "filled" and x.get("later"):
            # F67: the notice's further windows, which nothing writes.
            bits.append(f"   ↳ ⚠️ the notice also states {len(x['later'])} later "
                        f"window(s), NOT written: {'; '.join(x['later'])}")
    for label, key in (
            ("row cleared (no maintenance)", "cleared"),
            ("⚠️ write FAILED (see the red card)", "write_failed"),
            ("⚠️ of those, outcome UNKNOWN — the Base did not answer and could "
             "not be read back (retried next visit)", "write_unknown"),
            ("⚠️ written, but its card was NOT delivered (re-sent on the next "
             "visit)", "unannounced"),
            ("⚠️ written, but its card was NOT delivered and will NOT be re-sent "
             "(VAWATCH_WRITE_ATTEMPTS=1) — check the row", "card_failed"),
            ("⚠️ card send timed out — it may have been delivered",
             "card_unknown"),
            ("card delivered late for an earlier write", "announced_late"),
            ("⚠️ gave up re-sending a written row's card (see the log)",
             "unannounced_dropped"),
            ("⚠️ needs a human", "needs_human"),
            ("of those, the card was HELD BACK by the per-row limit (read the "
             "group by hand)", "capped"),
            ("of those, the confirming model did NOT agree with the parser's "
             "window (nothing written)", "model_refused"),
            ("of those, a poster / image that may be a notice", "image_notice"),
            ("held — the confirming model could not be reached yet (re-tried "
             "next sweep, nothing written)", "confirm_pending"),
            ("ignored (not about maintenance)", "ignored_not_maintenance"),
            ("ignored (about maintenance, but a question, a 'no downtime' "
             "notice or a test environment — nothing to write)", "ignored_refused"),
            ("ignored (maintenance completed / cancelled — nothing on this row "
             "to change)", "ignored_done"),
            ("no maintenance — left to the open /provideraskmaintenance run "
             "(no card from here)", "left_to_providerask"),
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
    w = r.get("watch") or {}
    if w.get("source") in ("cache", "fallback"):
        # F78: the Base read is failing and the rotation runs on a stale list;
        # a provider row added since is not watched. Said here, not only in
        # the service log.
        age = _parse_now_str(w.get("at"))
        hours = int((_now_dt() - age).total_seconds() // 3600) if age else None
        bits.append(f"• ⚠️ watch list from the {w['source']} (read at {w.get('at')}"
                    + (f", {hours}h ago" if hours is not None else "")
                    + f") — the Base read is failing: {str(w.get('error'))[:160]}")
    gaps = _unwatched_names(r.get("unwatched"))
    if gaps:
        # These rows are not a sweep result at all: nothing on this path can
        # ever fill them (APP=TEAMS, excluded, or blank). Saying so here is the
        # only place an operator finds out.
        bits.append("• NOT autofilled by any code path: " + ", ".join(gaps[:8])
                    + (f" … and {len(gaps) - 8} more" if len(gaps) > 8 else ""))
    for warning in config_warnings()[:6]:
        # F83: a mistyped NOTICE_TZ, or "Y" in a 1/0 flag, used to change
        # behaviour with no trace; the stdout line is easy to miss.
        bits.append(f"• ⚠️ config: {warning}")
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
