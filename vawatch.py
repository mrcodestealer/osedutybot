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

Anything that is not an upcoming maintenance window is IGNORED and nothing is
written — in particular a "maintenance completed" notice and an API-change
announcement. A completion notice quotes the SAME window as the original, which
is why the completed/cancelled test runs first and wins outright: a rule that
only looked for a window would re-fire on it.

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

def targets() -> list:
    """Every Telegram provider group worth watching, from the Base.

    Reuses groupcheck's partition, so the excluded groups and the blank rows are
    defined in exactly one place and the watcher can never drift from what
    /telegramgroupcheck reports.
    """
    try:
        import groupcheck as gc

        telegram, _teams, _skipped = gc.partition(gc.fetch_rows())
        out = [{"provider": r.get("provider") or "", "group": r.get("group") or ""}
               for r in telegram if (r.get("group") or "").strip()]
        if out:
            return out
    except Exception as err:  # noqa: BLE001
        print(f"[vawatch] could not read the Base ({err!r}); "
              f"falling back to the configured single group", flush=True)
    single = _chat_title()
    return [{"provider": _provider(), "group": single}] if single else []


def next_target() -> dict:
    """One group per tick, round-robin.

    Reading all ~17 groups on every tick would take ~5 minutes of the single
    Telegram worker and starve every other command. One per 60s tick means a
    full cycle in under 20 minutes, which is far inside the notice period a
    provider gives, and leaves the worker free the rest of the time.
    """
    rows = targets()
    if not rows:
        return {}
    d = _load()
    idx = int(d.get("cursor") or 0) % len(rows)
    d["cursor"] = (idx + 1) % len(rows)
    with _ledger_lock:
        _save(d)
    return rows[idx]


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


def _key(msg: dict, group: str = "") -> str:
    """Identity of a message WITHIN a group.

    The group is part of the key because the watcher now rotates over ~17 chats
    that all share one ledger. Telegram's data-mid is per-CHAT, so ids collide
    across groups, and providers relay each other's notices verbatim so the
    content hash collides too. Without the group, one provider's notice silently
    marked another provider's identical notice as already handled - and that
    provider's row was never filled.
    """
    g = " ".join(str(group or "").split()).casefold()
    mid = str(msg.get("mid") or "").strip()
    if mid:
        return f"{g}|mid:{mid}"
    body = " ".join(str(msg.get("text") or "").split())
    return f"{g}|sha:" + hashlib.sha1(body.encode("utf-8")).hexdigest()[:16]


def _load() -> dict:
    try:
        with open(LEDGER_PATH, encoding="utf-8") as fh:
            d = json.load(fh)
        if isinstance(d, dict):
            d.setdefault("handled", {})
            d.setdefault("order", [])
            return d
    except Exception:
        pass
    return {"handled": {}, "order": []}


def _save(d: dict) -> None:
    d.setdefault("order", [])
    d.setdefault("handled", {})
    d["order"] = d["order"][-2000:]
    d["handled"] = {k: v for k, v in d["handled"].items() if k in set(d["order"])}
    try:
        tmp = LEDGER_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(d, fh, ensure_ascii=False, indent=1)
        tmp.replace(LEDGER_PATH)
    except Exception as err:
        print(f"[vawatch] could not save the ledger: {err!r}", flush=True)


def _seen(d: dict, key: str) -> bool:
    return key in d["handled"]


def _mark(d: dict, key: str, outcome: str, extra: Optional[dict] = None) -> None:
    if key not in d["handled"]:
        d["order"].append(key)
    d["handled"][key] = {"outcome": outcome, "at": _now_str(), **(extra or {})}


def ledger_is_cold() -> bool:
    """Deprecated: file-existence is NOT a usable cold-start test any more.

    Kept only so an external caller does not break. ``next_target`` writes the
    ledger to persist its rotation cursor, so by the time a sweep reaches
    handle_messages the file always exists - which silently disabled the
    backlog guard for EVERY group, including the first. Use ``is_baselined``.
    """
    return not LEDGER_PATH.exists()


def is_baselined(group: str) -> bool:
    """Has this group's existing backlog already been recorded?

    Per GROUP, not per file. The watcher rotates over ~17 groups, so a single
    global flag meant group #2 onwards were never baselined at all and would act
    on whatever old notices happened to be in them - up to 17 provider rows
    filled from history the moment the watcher was switched on.
    """
    key = " ".join(str(group or "").split()).casefold()
    return bool((_load().get("baselined") or {}).get(key))


def _bump_empty_reads(group: str) -> int:
    """Count consecutive reads of ``group`` that returned nothing."""
    key = " ".join(str(group or "").split()).casefold()
    with _ledger_lock:
        d = _load()
        n = int((d.setdefault("empty_reads", {})).get(key) or 0) + 1
        d["empty_reads"][key] = n
        _save(d)
    return n


def mark_baselined(group: str) -> None:
    key = " ".join(str(group or "").split()).casefold()
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


def handle_messages(messages: list, *, force: bool = False,
                    provider: str = "", group: str = "") -> dict:
    """Classify each message, act on new maintenance notices.

    ``force`` re-acts on the newest maintenance notice even if the ledger has
    already handled it — for a manual /vacheck when you want to prove the path.
    """
    d = _load()
    cold = not is_baselined(group) if group else ledger_is_cold()
    res = {"seen": 0, "acted": 0, "ignored": 0, "already": 0,
           "cold_start": cold, "details": []}

    # Bubbles arrive OLDEST first. Normally that is what we want - each later
    # message overwrites the earlier one, so the newest ends up on the row. But
    # `force` stops at the first message it acts on, so it has to start from the
    # NEWEST or it would re-assert a notice that a later reschedule superseded.
    for msg in (list(reversed(messages or [])) if force else (messages or [])):
        text = str(msg.get("text") or "").strip()
        if not text:
            continue
        res["seen"] += 1
        key = _key(msg, group)
        verdict = classify(text)
        known = _seen(d, key)

        if known and not force:
            res["already"] += 1
            res["details"].append({"key": key, "action": "already handled"})
            continue
        if verdict["action"] == "fill" and verdict.get("stale") and not force:
            # A window that has already finished: the provider is re-quoting an
            # old notice. Writing it would replace a live row with dead dates.
            _mark(d, key, "stale", {"why": "window already passed"})
            res["ignored"] += 1
            res["details"].append({"key": key, "action": "stale"})
            continue
        if verdict["action"] != "fill":
            _mark(d, key, "ignored", {"why": verdict["reason"]})
            res["ignored"] += 1
            res["details"].append({"key": key, "action": "ignore",
                                   "why": verdict["reason"]})
            continue
        if cold and not force:
            # First sweep after the feature is switched on: record the backlog
            # without acting, or turning it on re-announces an old notice and
            # overwrites a row someone already curated by hand.
            _mark(d, key, "cold-start", {"why": "backlog at first run"})
            res["details"].append({"key": key, "action": "cold-start skip"})
            continue

        done = act_on_notice(text, verdict, provider=provider, group=group)
        _mark(d, key, "filled" if done["wrote"] else "write-failed",
              {"window": done["wrote"] or "", "error": done["error"]})
        res["acted"] += 1
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
        if res["seen"] > 0 or _bump_empty_reads(group) >= 2:
            mark_baselined(group)
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
    ap.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
