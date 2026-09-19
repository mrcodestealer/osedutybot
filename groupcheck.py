#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""``/telegramgroupcheck`` — does every provider group in the Base still exist?

Reads the "Weekly Maintenance" table of the maintenance Lark Base, takes the
``Group Name`` of each row, and asks the platform named in that row's ``APP``
column (TELEGRAM or TEAMS) whether a chat with EXACTLY that name exists. Each
group that is found gets a card carrying a screenshot of its chat window,
captioned with the provider the screenshot belongs to. Everything that could not
be found is listed by name at the end, which is the point of the command.

READ-ONLY, on both platforms. Nothing is typed into any conversation and nothing
is sent. The only interactions are: opening a chat, scrolling a chat list, and —
on Telegram only — typing into the sidebar SEARCH box (``#column-left``), which
is how a virtualised chat list renders a group that is not recently active. The
message composer is never touched on either platform.

One caveat worth knowing: opening a Teams chat marks it READ for the Teams
account. There is no read-only existence probe in the Teams web client, so this
is unavoidable for any command that photographs the chat window.

Exact matching is the whole point. The caller captions each screenshot with a
provider's name, so a substring or prefix hit would photograph one provider's
group and attribute it to another. Both halves therefore compare whole strings
after collapsing whitespace, case-insensitively:
  * Telegram — ``telegramwarm._titles_match(..., allow_substring=False)``
  * Teams    — ``teamswatch._titles_equal`` (equality, NOT the watcher's
               anchored-prefix ``_titles_match``)

Deliberately does NOT import ``main``: importing it starts the scheduler and can
forward a real maintenance email. The Lark senders below are the same
self-contained block telegramwarm/teamswatch each carry, for exactly that reason.
"""

from __future__ import annotations

import json
import mimetypes
import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

import requests

try:  # so APP_ID / APP_SECRET resolve when run standalone
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

_ROOT_DIR = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# The Base
# ---------------------------------------------------------------------------
# https://casinoplus.sg.larksuite.com/base/IjeqbdzCealXp1sDczClVL0sgOb
#        ?table=tbl0or9xQZGgnPz2&view=vewveZ9FDf
APP_TOKEN = (os.getenv("GROUPCHECK_APP_TOKEN")
             or "IjeqbdzCealXp1sDczClVL0sgOb").strip()
TABLE_ID = (os.getenv("GROUPCHECK_TABLE_ID") or "tbl0or9xQZGgnPz2").strip()
# The view matters: it fixes both the row set and their order, so the cards come
# back in the same order as the sheet the user is looking at.
VIEW_ID = (os.getenv("GROUPCHECK_VIEW_ID") or "vewveZ9FDf").strip()

# Column aliases, first hit wins — a renamed column should not need a code change.
_PROVIDER_FIELDS = ("Provider / Games", "Provider/Games", "Provider", "Games")
_GROUPNAME_FIELDS = ("Group Name", "GroupName", "Group")
_APP_FIELDS = ("APP", "App", "Platform")

_TELEGRAM_WORDS = {"telegram", "tg"}
_TEAMS_WORDS = {"teams", "team", "msteams", "ms teams"}

# Groups to leave alone. Matched against the row's Group Name OR its provider,
# by the same rule the lookup itself uses (trim, collapse whitespace, case-fold,
# whole string) so an entry here cannot accidentally silence a similarly-named
# group. Separate several with a newline or a semicolon — NOT a comma, which is
# legal inside a chat title.
_SKIP_DEFAULT = "[CasinoPlus] CasinoPlus x BTi(SL) Support"


def _norm(value: str) -> str:
    """The lookup's own normalisation, reused so exclusions match identically."""
    return " ".join(str(value or "").split()).casefold()


def _skip_set() -> set:
    raw = os.getenv("GROUPCHECK_SKIP")
    if raw is None:
        raw = _SKIP_DEFAULT
    return {_norm(p) for p in re.split(r"[\n;]+", raw) if p.strip()}


def _tz():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(os.getenv("GROUPCHECK_TZ", "Asia/Manila"))
    except Exception:
        return None


def _now_str() -> str:
    return datetime.now(_tz()).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Lark API (self-contained — never import main)
# ---------------------------------------------------------------------------

def _lark_base() -> str:
    return os.getenv("LARK_OPEN_BASE", "https://open.larksuite.com").rstrip("/")


def _tenant_token() -> str:
    app_id = os.getenv("APP_ID")
    app_secret = os.getenv("APP_SECRET")
    if not app_id or not app_secret:
        raise RuntimeError("APP_ID / APP_SECRET not set in environment (.env)")
    result = requests.post(
        f"{_lark_base()}/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=30,
    ).json()
    if result.get("code") != 0:
        raise RuntimeError(f"Failed to get tenant token: {result}")
    return result["tenant_access_token"]


def send_text(chat_id: str, text: str) -> dict:
    token = _tenant_token()
    return requests.post(
        f"{_lark_base()}/open-apis/im/v1/messages",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
        params={"receive_id_type": "chat_id"},
        json={"receive_id": chat_id, "msg_type": "text",
              "content": json.dumps({"text": text})},
        timeout=30,
    ).json()


def upload_image_lark(image_path: str) -> Optional[str]:
    token = _tenant_token()
    mime, _ = mimetypes.guess_type(image_path)
    if mime not in ("image/png", "image/jpeg"):
        mime = "image/png"
    with open(image_path, "rb") as fh:
        result = requests.post(
            f"{_lark_base()}/open-apis/im/v1/images",
            headers={"Authorization": f"Bearer {token}"},
            files={"image": (os.path.basename(image_path), fh, mime)},
            data={"image_type": "message"},
            timeout=60,
        ).json()
    if result.get("code") == 0:
        return result.get("data", {}).get("image_key")
    print(f"[groupcheck] image upload failed: {result}", flush=True)
    return None


def send_card(chat_id: str, card: dict) -> dict:
    token = _tenant_token()
    return requests.post(
        f"{_lark_base()}/open-apis/im/v1/messages",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
        params={"receive_id_type": "chat_id"},
        json={"receive_id": chat_id, "msg_type": "interactive",
              "content": json.dumps(card, ensure_ascii=False)},
        timeout=30,
    ).json()


def _send_card_or_text(chat_id: str, card: dict, fallback: str) -> None:
    """Lark answers HTTP 200 with code != 0 for a rejected card and raises
    nothing, so an unchecked send fails completely silently."""
    try:
        resp = send_card(chat_id, card)
    except Exception as err:  # noqa: BLE001
        print(f"[groupcheck] card send raised: {err!r}", flush=True)
        resp = None
    if not isinstance(resp, dict) or resp.get("code") != 0:
        print(f"[groupcheck] card rejected: {resp!r}", flush=True)
        try:
            send_text(chat_id, fallback)
        except Exception as err:  # noqa: BLE001
            print(f"[groupcheck] fallback text failed: {err!r}", flush=True)


# ---------------------------------------------------------------------------
# Base reading
# ---------------------------------------------------------------------------

def _field_text(value: Any) -> str:
    """Flatten a bitable field to plain text.

    A Text column is a plain string, but degrades to a rich-text array of
    ``{"text": ..., "type": "text"}`` segments the moment the cell holds a link
    or a mention — which is why this cannot simply be ``str(value)``.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "label", "name", "value", "en_name"):
            if value.get(key):
                return str(value[key]).strip()
        return ""
    if isinstance(value, list):
        return ", ".join(p for p in (_field_text(v) for v in value) if p)
    return str(value).strip()


def _first_field(fields: dict, names: tuple) -> Any:
    for n in names:
        if n in fields and fields.get(n) not in (None, "", []):
            return fields.get(n)
    return None


def _app_set(value: Any) -> set:
    """The APP cell as a normalised set of platform words.

    A MultiSelect comes back as a list of plain strings. Kept as a SET rather
    than flattened to text, so a row tagged both TELEGRAM and TEAMS fans out to
    two checks instead of matching neither — and so 'Teams' never substring-hits
    inside some other value. Note bitable OMITS the key entirely for an empty
    cell, so this legitimately returns an empty set.
    """
    if value is None:
        return set()
    if isinstance(value, str):
        parts = re.split(r"[,/;|]+", value)
    elif isinstance(value, list):
        parts = [_field_text(v) for v in value]
    else:
        parts = [_field_text(value)]
    return {p.strip().lower() for p in parts if str(p).strip()}


def fetch_rows() -> list:
    """Every row of the configured view, in the view's own order.

    Returns ``[{"provider", "group", "apps", "record_id"}]``. Raises RuntimeError
    with the Lark payload on failure — a permission problem (the 91403 class)
    must be reported as such, not as "no rows".
    """
    token = _tenant_token()
    url = (f"{_lark_base()}/open-apis/bitable/v1/apps/{APP_TOKEN}"
           f"/tables/{TABLE_ID}/records")
    headers = {"Authorization": f"Bearer {token}"}
    items: list = []
    page_token = None
    while True:
        params: dict = {"page_size": 200}
        if VIEW_ID:
            params["view_id"] = VIEW_ID
        if page_token:
            params["page_token"] = page_token
        data = requests.get(url, headers=headers, params=params, timeout=60).json()
        if data.get("code") != 0:
            raise RuntimeError(
                f"Lark Base read failed (code {data.get('code')}): "
                f"{data.get('msg')}"
            )
        d = data.get("data", {}) or {}
        items.extend(d.get("items") or [])
        if not d.get("has_more"):
            break
        page_token = d.get("page_token")

    rows: list = []
    for rec in items:
        fields = rec.get("fields", {}) or {}
        rows.append({
            "record_id": rec.get("record_id") or "",
            "provider": _field_text(_first_field(fields, _PROVIDER_FIELDS)),
            "group": _field_text(_first_field(fields, _GROUPNAME_FIELDS)),
            "apps": _app_set(_first_field(fields, _APP_FIELDS)),
        })
    return rows


def partition(rows: list) -> tuple:
    """Split rows into (telegram, teams, skipped).

    A row is skipped when it has no group name or no recognised APP value — the
    Base has rows like that, and folding them into "cannot detect" would hide a
    real detection failure behind a data gap.
    """
    telegram: list = []
    teams: list = []
    skipped: list = []
    skip = _skip_set()
    for row in rows:
        name = (row.get("group") or "").strip()
        apps = row.get("apps") or set()
        want_tg = bool(apps & _TELEGRAM_WORDS)
        want_tm = bool(apps & _TEAMS_WORDS)
        if skip and (_norm(name) in skip or _norm(row.get("provider")) in skip):
            # Checked before everything else: an excluded group is never opened,
            # never screenshotted, and never reported as undetectable.
            row["why"] = "excluded from scanning (GROUPCHECK_SKIP)"
        elif not name and not apps:
            row["why"] = "no Group Name and no APP set"
        elif not name:
            row["why"] = "no Group Name set"
        elif not (want_tg or want_tm):
            row["why"] = (f"APP is {sorted(apps) or 'empty'} — "
                          f"not TELEGRAM or TEAMS")
        else:
            if want_tg:
                telegram.append(row)
            if want_tm:
                teams.append(row)
            continue
        skipped.append(row)
    return telegram, teams, skipped


# ---------------------------------------------------------------------------
# Cards
# ---------------------------------------------------------------------------

_CARD_CONFIG = {"update_multi": True, "width_mode": "fill"}


def _card(header_text: str, template: str, elements: list) -> dict:
    # Schema 2.0, like every other card in this repo. Only div / hr / img:
    # 2.0 rejects {"tag": "note"} with ErrCode 200861.
    return {
        "schema": "2.0",
        "config": dict(_CARD_CONFIG),
        "header": {"template": template,
                   "title": {"tag": "plain_text", "content": header_text[:100]}},
        "body": {"elements": elements},
    }


def _div(md: str) -> dict:
    return {"tag": "div", "text": {"tag": "lark_md", "content": md}}


def build_row_card(row: dict, platform: str, result: dict,
                   image_key: Optional[str]) -> dict:
    """One card per group: who it belongs to, then the picture of its window."""
    ok = bool(result.get("ok"))
    provider = row.get("provider") or "(no provider)"
    group = row.get("group") or ""
    elements: list = []

    if ok:
        # The caption is the deliverable: a screenshot nobody can attribute is
        # worth nothing, so the provider is named before the image, not after.
        elements.append(_div(
            f"**📸 This screenshot belongs to: {provider}**\n"
            f"**App:** {platform}\n"
            f"**Group:** {group}"
        ))
        if image_key:
            elements.append({"tag": "img", "img_key": image_key,
                             "alt": {"tag": "plain_text",
                                     "content": f"{provider} — {group}"[:100]}})
        else:
            elements.append(_div("_(the chat was found, but the screenshot "
                                 "could not be captured or uploaded)_"))
        proof = []
        if result.get("verifiedBy"):
            proof.append(f"identity confirmed by {result['verifiedBy']}")
        if result.get("peerId"):
            proof.append(f"peer id `{result['peerId']}`")
        opened = (result.get("opened") or "").strip()
        if opened and opened != group:
            # Whitespace/case differences only — the match rule collapses both.
            proof.append(f"on-screen title {opened!r}")
        if proof:
            elements.append(_div("_" + " · ".join(proof) + "_"))
    else:
        elements.append(_div(
            f"**❌ Could not detect this group**\n"
            f"**Provider:** {provider}\n"
            f"**App:** {platform}\n"
            f"**Group:** {group}\n"
            f"**Why:** {str(result.get('reason') or 'unknown')[:600]}"
        ))

    elements.append({"tag": "hr"})
    elements.append(_div(f"_Read-only · nothing was sent · {_now_str()}_"))
    icon = "✅" if ok else "❌"
    return _card(f"{icon} {provider} · {platform}",
                 "green" if ok else "red", elements)


def build_summary_card(found: list, missing: list, skipped: list,
                       errors: list) -> dict:
    total = len(found) + len(missing)
    elements: list = [_div(
        f"**Checked {total} group(s) from the Base**\n"
        f"✅ Detected: **{len(found)}**   ❌ Not detected: **{len(missing)}**"
        f"   ⚪ Skipped: **{len(skipped)}**"
    )]

    if missing:
        elements.append({"tag": "hr"})
        lines = ["**❌ Could not detect these groups:**"]
        for row, platform, reason in missing:
            lines.append(
                f"• **{row.get('provider') or '?'}** ({platform}) — "
                f"`{row.get('group') or ''}`\n  _{str(reason)[:160]}_"
            )
        elements.append(_div(_clamp("\n".join(lines))))

    if skipped:
        elements.append({"tag": "hr"})
        lines = ["**⚪ Skipped — not looked up:**"]
        for row in skipped:
            lines.append(f"• **{row.get('provider') or '?'}** — "
                         f"_{row.get('why') or 'no Group Name / APP'}_")
        elements.append(_div(_clamp("\n".join(lines))))

    if found:
        elements.append({"tag": "hr"})
        names = ", ".join(f"{r.get('provider') or '?'}" for r, _p in found)
        elements.append(_div(_clamp(f"**✅ Detected:** {names}")))

    if errors:
        elements.append({"tag": "hr"})
        elements.append(_div(_clamp(
            "**⚠️ Platform problems:**\n"
            + "\n".join(f"• {e}" for e in errors))))

    if any(p == "Telegram" for _r, p in found):
        # Said once here rather than on all 17 cards. Telegram's chat list is
        # virtualised, so finding a group that is not recently active means
        # falling back to the app's search — which also returns public chats
        # this account has never joined. An exact-name collision with one is
        # unlikely but not impossible, and the peer id on each card is what
        # settles it.
        elements.append({"tag": "hr"})
        elements.append(_div(
            "_Telegram groups are located via the app's search, which also "
            "returns public chats this account has never joined. Each card "
            "prints the peer id it opened — check that if a result surprises "
            "you._"))

    elements.append({"tag": "hr"})
    elements.append(_div(f"_Read-only · nothing was sent · {_now_str()}_"))
    template = "green" if (not missing and not errors) else (
        "orange" if found else "red")
    return _card(
        f"📋 Group check — {len(found)} detected, {len(missing)} missing",
        template, elements)


def _clamp(text: str, limit: int = 3500) -> str:
    return text if len(text) <= limit else text[:limit] + "\n…"


def build_summary_text(found: list, missing: list, skipped: list,
                       errors: list) -> str:
    """Plain-text twin of the summary, for when Lark rejects the card."""
    lines = [f"📋 Group check — {len(found)} detected, {len(missing)} missing, "
             f"{len(skipped)} not configured"]
    if missing:
        lines.append("\n❌ Could not detect:")
        lines += [f"  • {r.get('provider') or '?'} ({p}) — "
                  f"{r.get('group') or ''} — {reason}"
                  for r, p, reason in missing]
    if skipped:
        lines.append("\n⚪ Skipped:")
        lines += [f"  • {r.get('provider') or '?'} — {r.get('why') or ''}"
                  for r in skipped]
    if found:
        lines.append("\n✅ Detected: "
                     + ", ".join(r.get("provider") or "?" for r, _p in found))
    if errors:
        lines.append("\n⚠️ " + "; ".join(errors))
    return _clamp("\n".join(lines))


# ---------------------------------------------------------------------------
# The command
# ---------------------------------------------------------------------------

_run_lock = threading.Lock()
_running = False


def is_running() -> bool:
    return _running


def _make_sink(chat_id: str, rows: list, platform: str,
               found: list, missing: list, seen: set) -> Callable:
    """Post one card per group AS IT COMPLETES, so a 20-group sweep is not silent.

    Results arrive in the order the titles were submitted, so position is what
    pairs a result with its Base row — a title lookup would mis-attribute the
    screenshot if the Base ever holds the same group name twice.

    ``seen`` collects the indices actually reported. A platform that dies or
    times out half way has already had its finished titles posted as cards, so
    the caller must only account for the REST — otherwise the same provider is
    printed as both detected and not detected, and the totals over-count.
    """
    counter = {"i": 0}

    def sink(result: dict) -> None:
        idx = counter["i"]
        counter["i"] = idx + 1
        seen.add(idx)
        row = rows[idx] if idx < len(rows) else {"provider": "?", "group":
                                                 result.get("title") or ""}
        # Harvest the peer id while we have it. This is the only command that
        # resolves every provider group, and /provideraskmaintenance refuses to
        # message a group it cannot pin to an id.
        if platform == "Telegram" and result.get("ok") and result.get("peerId"):
            try:
                import peerstore

                peerstore.remember(row.get("group") or result.get("title") or "",
                                   result["peerId"],
                                   provider=row.get("provider") or "")
            except Exception as err:  # noqa: BLE001
                print(f"[groupcheck] could not store the peer id: {err!r}",
                      flush=True)

        image_key = None
        shot = (result.get("shot") or "").strip()
        if result.get("ok") and shot and os.path.exists(shot):
            try:
                image_key = upload_image_lark(shot)
            except Exception as err:  # noqa: BLE001
                print(f"[groupcheck] upload failed for {shot}: {err!r}",
                      flush=True)
        if result.get("ok"):
            found.append((row, platform))
        else:
            missing.append((row, platform, result.get("reason") or "not found"))
        card = build_row_card(row, platform, result, image_key)
        _send_card_or_text(
            chat_id, card,
            f"{'✅' if result.get('ok') else '❌'} {row.get('provider')} "
            f"({platform}) — {row.get('group')}"
            + ("" if result.get("ok") else f" — {result.get('reason')}"),
        )

    return sink


def _tg_check(titles: list, sink: Callable) -> dict:
    import telegramwarm as _tg

    return _tg.check_groups_exist(titles, sink=sink)


def _tm_check(titles: list, sink: Callable) -> dict:
    import teamswatch as _tm

    return _tm.check_groups_exist(titles, sink=sink)


def _run_platform(chat_id: str, rows: list, platform: str, call: Callable,
                  found: list, missing: list, errors: list) -> None:
    """Drive one platform, then account for every row it did not report.

    Two rules earn their keep here:

    * Only an explicit ``ok is True`` counts as success. An empty dict — what a
      crashed worker used to return — must not read as "no error, no results"
      and so produce a green all-clear for groups nobody looked at.
    * Rows are marked missing only if the sink never reported them. The sink
      posts a card per title as it lands, so a half-way failure has already
      filed some rows; re-adding them all would print a provider as both
      detected and not detected.
    """
    seen: set = set()
    why = ""
    try:
        res = call([r["group"] for r in rows],
                   _make_sink(chat_id, rows, platform, found, missing, seen))
        if not (isinstance(res, dict) and res.get("ok") is True):
            why = ((res or {}).get("error")
                   if isinstance(res, dict) else "") or \
                  f"the {platform} check returned no result"
    except Exception as err:  # noqa: BLE001
        why = repr(err)

    if why:
        errors.append(f"{platform}: {why}")
    unreported = [(i, r) for i, r in enumerate(rows) if i not in seen]
    if unreported:
        reason = why or f"{platform} stopped before reaching this group"
        if not why:
            errors.append(f"{platform}: {len(unreported)} group(s) were never "
                          f"checked")
        for _i, row in unreported:
            missing.append((row, platform, reason))


def run_check(chat_id: str) -> dict:
    """``/telegramgroupcheck`` — the whole sweep. Safe to call on a daemon thread."""
    global _running
    with _run_lock:
        if _running:
            send_text(chat_id, "⏳ A group check is already running — "
                               "wait for it to finish before starting another.")
            return {"ok": False, "error": "already running"}
        _running = True

    found: list = []
    missing: list = []
    errors: list = []
    skipped: list = []
    try:
        try:
            rows = fetch_rows()
        except Exception as err:  # noqa: BLE001
            send_text(chat_id, f"❌ Could not read the Base: {err}")
            return {"ok": False, "error": str(err)}

        telegram, teams, skipped = partition(rows)
        send_text(
            chat_id,
            f"🔍 Group check: {len(rows)} row(s) in the Base — "
            f"{len(telegram)} Telegram, {len(teams)} Teams, "
            f"{len(skipped)} skipped.\n"
            f"Opening each group read-only and screenshotting its chat window. "
            f"Nothing will be sent in Telegram or Teams.",
        )

        if telegram:
            _run_platform(chat_id, telegram, "Telegram",
                          lambda t, s: _tg_check(t, s), found, missing, errors)
        if teams:
            _run_platform(chat_id, teams, "Teams",
                          lambda t, s: _tm_check(t, s), found, missing, errors)

        _send_card_or_text(
            chat_id,
            build_summary_card(found, missing, skipped, errors),
            build_summary_text(found, missing, skipped, errors),
        )
        return {"ok": True, "found": len(found), "missing": len(missing),
                "skipped": len(skipped), "errors": errors}
    finally:
        with _run_lock:
            _running = False


# ---------------------------------------------------------------------------
# CLI — `python groupcheck.py --rows` to inspect the Base without a browser
# ---------------------------------------------------------------------------

def main(argv: Optional[list] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Provider group check")
    ap.add_argument("--rows", action="store_true",
                    help="print the Base rows and how they partition, then exit")
    ap.add_argument("--chat", default="", help="Lark chat_id to post results to")
    args = ap.parse_args(argv)

    if args.rows:
        rows = fetch_rows()
        telegram, teams, skipped = partition(rows)
        print(f"{len(rows)} rows: {len(telegram)} telegram, "
              f"{len(teams)} teams, {len(skipped)} skipped")
        for label, group in (("TELEGRAM", telegram), ("TEAMS", teams)):
            for r in group:
                print(f"  {label:9} {r['provider']!r:32} {r['group']!r}")
        for r in skipped:
            print(f"  SKIP      {r['provider']!r:32} ({r.get('why')})")
        return 0

    if not args.chat:
        print("nothing to do: pass --rows, or --chat <chat_id> to run the sweep")
        return 2
    out = run_check(args.chat)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
