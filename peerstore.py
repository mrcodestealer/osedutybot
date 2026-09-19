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
"""

from __future__ import annotations

import json
import os
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
    try:
        with open(STORE_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and isinstance(data.get("groups"), dict):
            return data
    except Exception:
        pass
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
        if data["groups"].pop(_norm(group), None) is None:
            return False
        _write(data)
    return True


def main(argv: Optional[list] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Telegram peer-id pins")
    ap.add_argument("--list", action="store_true", help="show every pin")
    ap.add_argument("--forget", metavar="GROUP", help="drop one pin")
    args = ap.parse_args(argv)

    if args.forget:
        print("dropped" if forget(args.forget) else "not pinned")
        return 0
    pins = all_pins()
    print(f"{len(pins)} pinned group(s)  [{STORE_PATH}]")
    for _key, rec in sorted(pins.items(), key=lambda kv: kv[1].get("provider", "")):
        print(f"  {rec.get('provider', ''):14} {rec.get('peer', ''):16} "
              f"{rec.get('title', '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
