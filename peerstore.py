#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Remembered Telegram peer ids for the provider groups.

A peer id is the only identity a title trick cannot fool, and it is what makes
sending to a provider group safe. The send route in telegramwarm deliberately
runs with ``allow_search=False`` — Telegram's search returns public groups this
account has never joined, so an identically-named stranger would otherwise be a
valid target — but the chat list is virtualised, so most provider rows are not
in the DOM to match against either. Pinning each group to its peer id resolves
both: the id is checked before the click and again after the chat opens.

``/telegramgroupcheck`` already resolves every group and reports its peer id, so
it fills this store as a side effect. ``/provideraskmaintenance`` reads it and
refuses to message any group that is not pinned here.

The file is a cache, not a source of truth: deleting it costs one
/telegramgroupcheck run, nothing more.

TEAMS. The same file keeps a separate "teams" section: each APP=TEAMS provider
group's Teams conversation id (19:...@thread.v2 / .skype), the Teams equivalent
of a peer id - opaque, unchanged by a rename, and not something a look-alike
chat can copy. /telegramgroupcheck records it; teamswatch's provider reader
refuses to read a group that has none, and checks it on every read. The two
sections never mix: a Telegram lookup only ever reads "groups".
"""

from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

_ROOT_DIR = Path(__file__).resolve().parent
STORE_PATH = Path(os.getenv("PROVIDER_PEERS_PATH")
                  or (_ROOT_DIR / "provider_peers.json"))

_lock = threading.Lock()


def _norm(value: str) -> str:
    """Same normalisation the group lookup uses, so keys line up."""
    return " ".join(str(value or "").split()).casefold()


def _load() -> dict:
    """The store, or an empty one - but never an empty one that can be SAVED
    over a store we merely failed to read.

    Every read error used to look like "no pins": one EMFILE in the Playwright
    process, and the next remember() (Telegram) or remember_teams() (Teams)
    wrote {its one pin} over the file, erasing every pin of the OTHER app too.
    Now a missing file is empty, a corrupt one is moved aside to .bad and
    starts empty (it is a cache - /telegramgroupcheck refills it), and any
    other read error returns a stand-in marked ``_unreadable`` that every
    writer refuses to save.
    """
    try:
        with open(STORE_PATH, encoding="utf-8") as fh:
            raw = fh.read()
    except FileNotFoundError:
        return {"groups": {}}
    except Exception as err:  # noqa: BLE001
        print(f"[peerstore] could not read {STORE_PATH.name}: {err!r} - "
              f"not writing over it", flush=True)
        return {"groups": {}, "_unreadable": repr(err)}
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and isinstance(data.get("groups"), dict):
            return data
        raise ValueError("not a pin store")
    except Exception as err:  # noqa: BLE001
        try:
            STORE_PATH.replace(STORE_PATH.with_name(STORE_PATH.name + ".bad"))
        except Exception:  # noqa: BLE001
            return {"groups": {}, "_unreadable": f"corrupt ({err!r}), not moved"}
        print(f"[peerstore] {STORE_PATH.name} was corrupt ({err!r}); kept as .bad, "
              f"starting empty", flush=True)
        return {"groups": {}}


def _write(data: dict) -> None:
    try:
        tmp = STORE_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=1)
        tmp.replace(STORE_PATH)
    except Exception as err:  # noqa: BLE001
        print(f"[peerstore] could not save: {err!r}", flush=True)


def remember(group: str, peer: str, *, provider: str = "") -> bool:
    """Record ``group``'s peer id. False when there is nothing worth storing.

    A username hash ('@name') is refused: the send route compares against the
    row's numeric data-peer-id, so an '@name' could never match and would only
    look like a pin that mysteriously never works.
    """
    g, p = _norm(group), str(peer or "").strip().lstrip("#").strip()
    if not g or not p or p.startswith("@"):
        return False
    with _lock:
        data = _load()
        if data.get("_unreadable"):
            return False
        prev = (data["groups"].get(g) or {}).get("peer")
        data["groups"][g] = {
            "peer": p,
            "provider": str(provider or "").strip(),
            "title": " ".join(str(group or "").split()),
            "seen_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        _write(data)
    if prev and prev != p:
        # Worth shouting about: a group was re-created, or a lookup matched the
        # wrong chat. Either way the next send would go somewhere new.
        print(f"[peerstore] {group!r} peer CHANGED {prev} -> {p}", flush=True)
    return True


def peer_for(group: str) -> str:
    """The pinned peer id for ``group``, or '' when it has never been seen."""
    return str((_load()["groups"].get(_norm(group)) or {}).get("peer") or "")


def all_pins() -> dict:
    """{normalised group -> record}, for reporting."""
    return dict(_load()["groups"])


def forget(group: str) -> bool:
    with _lock:
        data = _load()
        if data.get("_unreadable"):
            return False
        if data["groups"].pop(_norm(group), None) is None:
            return False
        _write(data)
    return True


# A Teams conversation id, e.g. 19:29c6...@thread.skype (see teamswatch's
# _THREAD_ID_RE, which this mirrors).
_THREAD_RE = re.compile(r"^19:[A-Za-z0-9_\-+=/.]+@(?:thread\.v2|thread\.skype|thread\.tacv2|unq\.gbl\.spaces)$")


def _teams(data: dict) -> dict:
    t = data.get("teams")
    if not isinstance(t, dict):
        t = {}
        data["teams"] = t
    return t


def remember_teams(group: str, thread: str, *, provider: str = "") -> bool:
    """Record a Teams group's conversation id. False when it is not one."""
    g, t = _norm(group), str(thread or "").strip()
    if not g or not _THREAD_RE.match(t):
        return False
    with _lock:
        data = _load()
        if data.get("_unreadable"):
            return False
        teams = _teams(data)
        prev = (teams.get(g) or {}).get("thread")
        teams[g] = {
            "thread": t,
            "provider": str(provider or "").strip(),
            "title": " ".join(str(group or "").split()),
            "seen_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        _write(data)
    if prev and prev != t:
        print(f"[peerstore] Teams {group!r} conversation id CHANGED {prev} -> {t}",
              flush=True)
    return True


def thread_for(group: str) -> str:
    """The pinned Teams conversation id for ``group``, or ''."""
    rec = _teams(_load()).get(_norm(group)) or {}
    t = str(rec.get("thread") or "")
    return t if _THREAD_RE.match(t) else ""


def all_teams_pins() -> dict:
    return dict(_teams(_load()))


def forget_teams(group: str) -> bool:
    with _lock:
        data = _load()
        if data.get("_unreadable"):
            return False
        if _teams(data).pop(_norm(group), None) is None:
            return False
        _write(data)
    return True


def main(argv: Optional[list] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Telegram peer-id / Teams conversation-id pins")
    ap.add_argument("--list", action="store_true", help="show every pin")
    ap.add_argument("--forget", metavar="GROUP", help="drop one Telegram pin")
    ap.add_argument("--forget-teams", metavar="GROUP", help="drop one Teams pin")
    args = ap.parse_args(argv)

    if args.forget:
        print("dropped" if forget(args.forget) else "not pinned")
        return 0
    if args.forget_teams:
        print("dropped" if forget_teams(args.forget_teams) else "not pinned")
        return 0
    pins = all_pins()
    print(f"{len(pins)} pinned group(s)  [{STORE_PATH}]")
    for _key, rec in sorted(pins.items(), key=lambda kv: kv[1].get("provider", "")):
        print(f"  {rec.get('provider', ''):14} {rec.get('peer', ''):16} "
              f"{rec.get('title', '')}")
    tpins = all_teams_pins()
    print(f"{len(tpins)} pinned Teams group(s)")
    for _key, rec in sorted(tpins.items(), key=lambda kv: kv[1].get("provider", "")):
        print(f"  {rec.get('provider', ''):14} {rec.get('thread', '')}  "
              f"{rec.get('title', '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
