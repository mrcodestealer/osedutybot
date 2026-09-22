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


def _tz():
    """The zone a bare 'GMT+8'-less notice is assumed to be in."""
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(os.getenv("VAWATCH_TZ", "Asia/Manila"))
    except Exception:
        return timezone(timedelta(hours=8))


def _now_str() -> str:
    return datetime.now(_tz()).strftime("%Y-%m-%d %H:%M:%S")


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


def watch_list() -> dict:
    """What the Base says, split into what is watched and what is not.

    -> {"telegram": [{provider, group}], "teams": [...], "skipped": [...],
        "source": "base"|"cache"|"fallback", "at": str, "error": str}

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
        rows = [{"provider": r.get("provider") or "", "group": r.get("group") or ""}
                for r in telegram if (r.get("group") or "").strip()]
        if rows:
            rep = {"telegram": rows,
                   "teams": [_row_note(r, "APP is TEAMS - this watcher only "
                                          "reads Telegram") for r in teams],
                   "skipped": [_row_note(r, "skipped by groupcheck")
                               for r in skipped],
                   "source": "base", "at": _now_str(), "error": ""}
            _remember_targets(rep)
            return rep
        err = "the Base returned no watchable TELEGRAM row"
    except Exception as exc:  # noqa: BLE001
        err = repr(exc)

    cached = (_load().get("targets") or {}) if _target_cache_enabled() else {}
    if cached.get("telegram"):
        print(f"[vawatch] could not read the Base ({err}); reusing the watch "
              f"list read at {cached.get('at')} "
              f"({len(cached['telegram'])} group(s))", flush=True)
        return {"telegram": list(cached["telegram"]),
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
    with _ledger_lock:
        d = _load()
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
        _save(d)
    if idx == 0:
        # Top of a fresh cycle: one line naming every provider this path will
        # never fill, ~18 minutes apart rather than every tick.
        _log_unwatched(rep)
    return dict(row)


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


def find_provider_row(provider: str) -> dict:
    """The Base row for ``provider`` -> {record_id, fields}. {} when absent."""
    token = _tenant_token()
    url = (f"{_lark_base()}/open-apis/bitable/v1/apps/{APP_TOKEN}"
           f"/tables/{TABLE_ID}/records")
    headers = {"Authorization": f"Bearer {token}"}
    want = " ".join((provider or "").split()).casefold()
    page_token = None
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
            val = (rec.get("fields") or {}).get("Provider / Games")
            if isinstance(val, list):
                val = " ".join(str(v.get("text", v) if isinstance(v, dict) else v)
                               for v in val)
            if " ".join(str(val or "").split()).casefold() == want:
                return {"record_id": rec.get("record_id") or "",
                        "fields": rec.get("fields") or {}}
        if not d.get("has_more"):
            return {}
        page_token = d.get("page_token")


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
    d["key_scheme"] = _KEY_SCHEME
    return d


def _load() -> dict:
    try:
        with open(LEDGER_PATH, encoding="utf-8") as fh:
            d = json.load(fh)
        if isinstance(d, dict):
            d.setdefault("handled", {})
            d.setdefault("order", [])
            return _migrate(d)
    except Exception:
        pass
    return {"handled": {}, "order": [], "key_scheme": _KEY_SCHEME}


def _save(d: dict) -> None:
    d.setdefault("order", [])
    d.setdefault("handled", {})
    d.setdefault("key_scheme", _KEY_SCHEME)
    d["order"] = d["order"][-2000:]
    d["handled"] = {k: v for k, v in d["handled"].items() if k in set(d["order"])}
    try:
        tmp = LEDGER_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(d, fh, ensure_ascii=False, indent=1)
        tmp.replace(LEDGER_PATH)
    except Exception as err:
        print(f"[vawatch] could not save the ledger: {err!r}", flush=True)


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
    if key not in d["handled"]:
        d["order"].append(key)
    attempts = (int(prev.get("attempts") or 0) + 1
                if prev.get("outcome") == outcome else 1)
    d["handled"][key] = {"outcome": outcome, "at": _now_str(),
                         "attempts": attempts, **(extra or {})}


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
# The card
# ---------------------------------------------------------------------------

def build_card(group: str, provider: str, text: str, verdict: dict,
               wrote: Optional[str]) -> dict:
    start, end = verdict.get("start"), verdict.get("end")
    body = text if len(text) <= 3000 else text[:3000] + "\n…"
    elements: list = [
        {"tag": "div", "text": {"tag": "lark_md", "content":
            f"**Detected scheduled maintenance for {group}**\n"
            f"**Provider:** {provider}\n"
            f"**Start:** {start:%Y-%m-%d %H:%M} ({start:%Z})\n"
            f"**End:** {end:%Y-%m-%d %H:%M} ({end:%Z})"
            if start and end else
            f"**Detected scheduled maintenance for {group}**"}},
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
               wrote: Optional[str]) -> str:
    s, e = verdict.get("start"), verdict.get("end")
    head = f"🛠️ Detected scheduled maintenance for {group}\nProvider: {provider}"
    if s and e:
        head += f"\nStart: {s:%Y-%m-%d %H:%M}\nEnd:   {e:%Y-%m-%d %H:%M}"
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
                     *, extra: str = "") -> dict:
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
    lines.append("**Nothing was written to the Base** — please fill the row by "
                 "hand.")
    return _post_note(f"❓ Maintenance notice needs a human · "
                      f"{provider or group}", "red", lines, text)


# ---------------------------------------------------------------------------
# The detector
# ---------------------------------------------------------------------------

def act_on_notice(text: str, verdict: dict, *, provider: str = "",
                  group: str = "") -> dict:
    """Write the provider's row, then card the Laboratory group."""
    # NO fallback when the caller passed a group. A Base row with a blank
    # Provider used to fall through to _provider() (default "VA"), filing
    # another group's notice onto the VA row.
    if group and not (provider or "").strip():
        out = {"wrote": None, "record_id": "",
               "error": f"the Base row for {group!r} has no Provider - refusing "
                        f"to guess which row to write"}
        print(f"[vawatch] {out['error']}", flush=True)
        return out
    provider = provider or _provider()
    group = group or _chat_title()
    out: dict = {"wrote": None, "error": "", "record_id": ""}
    try:
        row = find_provider_row(provider)
        if not row:
            raise RuntimeError(f"no row in the Base for provider {provider!r}")
        out["record_id"] = row["record_id"]
        update_row(row["record_id"], {
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
                                          out["wrote"]))
        if not isinstance(resp, dict) or resp.get("code") != 0:
            print(f"[vawatch] card rejected: {resp!r}", flush=True)
            send_text(chat, build_text(group, provider, text, verdict, out["wrote"]))
    except Exception as err:          # noqa: BLE001
        print(f"[vawatch] card send failed: {err!r}", flush=True)
    return out


def act_on_clear(text: str, verdict: dict, *, provider: str = "",
                 group: str = "") -> dict:
    """The provider says there is no maintenance: blank the row.

    -> {"wrote", "error", "record_id", "skipped"}.

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
        out = {"wrote": None, "record_id": "", "skipped": "",
               "error": f"the Base row for {group!r} has no Provider - refusing "
                        f"to guess which row to clear"}
        print(f"[vawatch] {out['error']}", flush=True)
        return out
    if not _clear_enabled():
        return {"wrote": None, "record_id": "", "error": "",
                "skipped": "VAWATCH_CLEAR_ENABLED is not set"}
    provider = provider or _provider()
    group = group or _chat_title()
    out: dict = {"wrote": None, "error": "", "record_id": "", "skipped": ""}
    try:
        row = find_provider_row(provider)
        if not row:
            raise RuntimeError(f"no row in the Base for provider {provider!r}")
        out["record_id"] = row["record_id"]
        update_row(row["record_id"], {
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

    _post_note(f"\U0001F9F9 No maintenance · {provider}",
               "green" if out["wrote"] else "red",
               [f"**Group:** {group}", f"**Provider:** {provider}",
                (f"**Start Time / End Time cleared, Remark = "
                 f"“No maintenance”**" if out["wrote"] else
                 f"**⚠️ the Base row was NOT cleared** — "
                 f"{out['error']}")], text)
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


def handle_messages(messages: list, *, force: bool = False,
                    provider: str = "", group: str = "") -> dict:
    """Classify each message, act on new maintenance notices.

    ``force`` re-acts on the newest maintenance notice even if the ledger has
    already handled it — for a manual /vacheck when you want to prove the path.

    Every verdict is answered. The `if action != "fill": ignore` this replaced
    threw away three outcomes that each leave the row WRONG rather than merely
    unfilled - needs_human, clear and follow_quote - and the single `ignored`
    counter then reported all of them, plus the genuinely unrelated messages,
    under one number the caller prints as "not scheduled maintenance". The
    counter is still there and still the grand total; the breakdown beside it
    says which of the four actually happened.
    """
    d = _load()
    cold = not is_baselined(group, provider) if group else ledger_is_cold()
    res = {"seen": 0, "acted": 0, "ignored": 0, "already": 0,
           "cold_start": cold, "details": [],
           # `ignored` above stays the grand total main.py has always printed.
           # These split it; `acted` now counts only writes that SUCCEEDED, with
           # the failures counted separately instead of being reported as fills.
           "ignored_not_maintenance": 0, "ignored_unparsed": 0,
           "ignored_stale": 0, "needs_human": 0, "cleared": 0,
           "write_failed": 0, "retried": 0,
           # The Base rows nothing on this path reads (APP=TEAMS, excluded,
           # blank), straight from the ledger cache - no Base call here.
           "unwatched": _unwatched_from_cache(d)}

    # Bubbles arrive OLDEST first. Normally that is what we want - each later
    # message overwrites the earlier one, so the newest ends up on the row. But
    # `force` stops at the first message it acts on, so it has to start from the
    # NEWEST or it would re-assert a notice that a later reschedule superseded.
    for msg in (list(reversed(messages or [])) if force else (messages or [])):
        text = str(msg.get("text") or "").strip()
        if not text:
            continue
        res["seen"] += 1
        key = _key(msg, group, provider)
        verdict = classify(text)
        action = verdict["action"]
        why = verdict.get("reason") or ""

        # The ledger is still consulted before anything is DONE, but what counts
        # as handled now depends on how the last attempt ended (see _seen): a
        # failed write and an unreadable window come back round, a success and a
        # plain "not about maintenance" do not.
        if _seen(d, key) and not force:
            res["already"] += 1
            res["details"].append({"key": key, "action": "already handled"})
            continue
        if key in d["handled"]:
            res["retried"] += 1

        if action == "ignore":
            # Cheap and side-effect free, so it runs even on a cold start.
            outcome = "unparsed" if _is_unparsed(why) else "ignored"
            if outcome == "unparsed" and cold and not force:
                # ...but "unparsed" is RETRY-eligible, and this branch sits
                # ahead of the cold-start guard below. A half-rendered backlog
                # bubble therefore never got the cold-start mark, the group was
                # baselined anyway (seen > 0), and the NEXT sweep read the same
                # mid in full and wrote a backlog notice to the row - exactly
                # what cold start exists to stop. Worse, this release's ledger
                # migration re-baselines every group at once, so it would have
                # fired on all 18 the moment it deployed. On a cold sweep the
                # mark has to be terminal.
                outcome = "cold-start"
            _mark(d, key, outcome, {"why": why})
            res["ignored"] += 1
            if outcome == "unparsed":
                res["ignored_unparsed"] += 1
            else:
                res["ignored_not_maintenance"] += 1
            res["details"].append({"key": key, "action": outcome, "why": why})
            continue

        if action == "fill" and verdict.get("stale") and not force:
            # A window that has already finished: the provider is re-quoting an
            # old notice. Writing it would replace a live row with dead dates.
            _mark(d, key, "stale", {"why": "window already passed"})
            res["ignored"] += 1
            res["ignored_stale"] += 1
            res["details"].append({"key": key, "action": "stale"})
            continue

        if cold and not force:
            # First sweep after the feature is switched on: record the backlog
            # without acting, or turning it on re-announces an old notice and
            # overwrites a row someone already curated by hand. This guard now
            # covers the carding verdicts too, or enabling the watcher would
            # empty every group's backlog into the Laboratory group at once.
            _mark(d, key, "cold-start", {"why": "backlog at first run"})
            res["details"].append({"key": key, "action": "cold-start skip"})
            continue

        if action in ("needs_human", "follow_quote"):
            # follow_quote cannot be resolved here: the scraper reads bubbles,
            # not the messages they quote. Carding it as needs-human puts it in
            # front of a person, which is the only outcome left that is not a
            # silent drop.
            #
            # Under `force` only the FIRST of these cards. /vacheck force reads
            # up to VAWATCH_READ_COUNT bubbles and bypasses the ledger on all of
            # them, so without this one manual command could answer with eight
            # cards - including ones a previous sweep already sent.
            sent = ({"carded": False, "disabled": True,
                     "error": "force: only the first is carded"}
                    if force and res["needs_human"] else card_needs_human(
                        group, provider, text, verdict,
                        extra=("**The answer is in the message this one QUOTES**"
                               " — open the group to read it."
                               if action == "follow_quote" else "")))
            _mark(d, key, _note_outcome(sent),
                  {"why": why, "error": sent["error"]})
            res["ignored"] += 1
            res["needs_human"] += 1
            res["details"].append({"key": key, "action": "needs-human",
                                   "why": why, "carded": sent["carded"],
                                   "error": sent["error"]})
            continue

        if action == "clear":
            done = act_on_clear(text, verdict, provider=provider, group=group)
            if done.get("skipped"):
                # The write half is switched off; say so to a human rather than
                # leaving last week's window on the row with nobody told.
                sent = card_needs_human(
                    group, provider, text, verdict,
                    extra=f"**The provider says there is no maintenance** — the "
                          f"automatic clear is off ({done['skipped']}), so the "
                          f"row still shows its previous window.")
                _mark(d, key, "clear-skipped"
                      if _note_outcome(sent) != "card-failed" else "card-failed",
                      {"why": done["skipped"], "error": sent["error"]})
                res["ignored"] += 1
                res["needs_human"] += 1
                res["details"].append({"key": key, "action": "clear-skipped",
                                       "why": done["skipped"]})
                continue
            _mark(d, key, "cleared" if done["wrote"] else "clear-failed",
                  {"window": done["wrote"] or "", "error": done["error"]})
            if done["wrote"]:
                res["acted"] += 1
                res["cleared"] += 1
            else:
                res["write_failed"] += 1
            res["details"].append({"key": key, "action": "cleared",
                                   "window": done["wrote"],
                                   "error": done["error"]})
            if force:
                break
            continue

        done = act_on_notice(text, verdict, provider=provider, group=group)
        _mark(d, key, "filled" if done["wrote"] else "write-failed",
              {"window": done["wrote"] or "", "error": done["error"]})
        if done["wrote"]:
            res["acted"] += 1
        else:
            res["write_failed"] += 1
        res["details"].append({"key": key, "action": "filled",
                               "window": done["wrote"], "error": done["error"]})
        if force:
            break                      # one is enough to prove the path

    with _ledger_lock:
        _save(d)
    if cold and group:
        # Record the baseline only AFTER the backlog has been marked, so a crash
        # midway does not leave the group looking baselined with its history
        # unrecorded - which would then act on it next tick.
        #
        # And only when the read actually SAW something. A chat whose virtualised
        # list had not rendered yet returns zero messages, and baselining on that
        # spends the one-time guard on nothing: the next tick would treat the
        # group as baselined and act on its whole real backlog. A second
        # consecutive empty read is accepted as "this group is genuinely empty",
        # so a quiet group does not stay unbaselined forever.
        if res["seen"] > 0 or _bump_empty_reads(group, provider) >= 2:
            mark_baselined(group, provider)
    return res


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
