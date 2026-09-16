"""
Resigned Member list — read + write.

Source of truth is the ``Resigned Member`` Bitable table (Name + Status), kept by hand:
https://casinoplus.sg.larksuite.com/base/CpdEbEofwaYyyEsSjlElKNxzgec?table=tblQHimmQDEIlQ9n

Automatic detection is deliberately NOT attempted. Probing 2026-09-16 showed every
available Lark signal is unusable for this org:

* ``contact/v3`` ``status.is_resigned`` is false for all 194 in-scope users — Lark drops
  a resigned user from their departments rather than flagging them.
* ``status.is_frozen`` covers 22 accounts but mixes in shared/service accounts, and the
  org's own "Lark Account Renewal Review Guidelines" states an employee may resign while
  "the account has not yet been deactivated".
* Absence-from-directory cannot be used either: only 2 of 235 leave-calendar names match
  the directory at all, because the app's contact scope and the company leave calendar
  cover different populations.

So a human maintains the table and the bot reads it. Reads are cached for
``RESIGNED_CACHE_TTL`` seconds (default 300) so the list stays current without
re-fetching on every command.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Optional

import ose_Duty as od

BASE_TOKEN = (os.getenv("OSE_BASE_TOKEN") or "CpdEbEofwaYyyEsSjlElKNxzgec").strip()
TABLE_ID = (os.getenv("RESIGNED_TABLE_ID") or "tblQHimmQDEIlQ9n").strip()

NAME_FIELD = "Name"
STATUS_FIELD = "Status"
RESIGNED_STATUS = "Resigned"

def _int_env(name: str, default: int) -> int:
    """A bad env value must not take the whole bot down at import time."""
    try:
        return int((os.getenv(name) or "").strip() or default)
    except ValueError:
        print(f"[resigned] {name} is not a number; using {default}", flush=True)
        return default


CACHE_TTL = _int_env("RESIGNED_CACHE_TTL", 300)
# After a failed read, wait this long before trying again instead of retrying every call.
FAIL_BACKOFF = _int_env("RESIGNED_FAIL_BACKOFF", 60)

_cache: dict[str, Any] = {"at": 0.0, "names": [], "rows": []}


def _field_text(value: Any) -> str:
    """Bitable text cells come back as a plain string or as rich-text segments."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return str(value.get("text") or value.get("name") or "").strip()
    if isinstance(value, list):
        return " ".join(_field_text(v) for v in value).strip()
    return str(value).strip()


def fetch_rows(token: Optional[str] = None) -> list[dict[str, Any]]:
    """Every row of the Resigned Member table, uncached."""
    tok = token or od.get_tenant_access_token()
    records = od._bitable_get_all_records(tok, BASE_TOKEN, TABLE_ID)
    rows: list[dict[str, Any]] = []
    for rec in records or []:
        fields = rec.get("fields") or {}
        rows.append(
            {
                "name": _field_text(fields.get(NAME_FIELD)),
                "status": _field_text(fields.get(STATUS_FIELD)),
                "record_id": rec.get("record_id") or "",
            }
        )
    return rows


def resigned_names(token: Optional[str] = None, *, force: bool = False) -> list[str]:
    """
    Names whose Status is ``Resigned``, cached for ``CACHE_TTL`` seconds.

    Rows with an empty Status are ignored on purpose: a name only takes effect once a
    human has set the Status, so a half-typed row never silently hides someone's leave.
    """
    now = time.time()
    if not force and _cache["names"] is not None and (now - _cache["at"]) < CACHE_TTL:
        return list(_cache["names"])
    try:
        rows = fetch_rows(token)
    except Exception as exc:  # never let this break a duty/leave command
        # Remember the failure, otherwise the cache stays permanently expired and every
        # single command re-attempts the token POST + Bitable read for no benefit.
        _cache["at"] = now - CACHE_TTL + FAIL_BACKOFF
        print(f"[resigned] fetch failed, using last known list: {exc!r}", flush=True)
        return list(_cache["names"] or [])
    names = [
        r["name"]
        for r in rows
        if r["name"] and r["status"].strip().lower() == RESIGNED_STATUS.lower()
    ]
    _cache.update({"at": now, "names": names, "rows": rows})
    return list(names)


def is_resigned(name: str, names: Optional[list[str]] = None) -> bool:
    """True if ``name`` matches a Resigned Member entry."""
    nm = (name or "").strip()
    if not nm:
        return False
    pool = resigned_names() if names is None else names
    return any(ex and od._names_same_person(nm, ex) for ex in pool)


def filter_rows(
    rows: list[dict[str, Any]],
    *,
    key: str = "name",
    names: Optional[list[str]] = None,
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    """
    Drop rows belonging to resigned people.

    Returns ``(kept_rows, dropped_names, ambiguous_entries)``.

    An entry only takes effect when it identifies exactly ONE person among ``rows``.
    ``od._names_same_person`` does prefix matching, so a bare first name is dangerous:
    against the real September calendar, ``Yong`` matches three different people and
    ``Jia`` matches three. Hiding the wrong person's leave is worse than showing a
    resigned one, so an entry that matches more than one distinct name is skipped and
    returned in ``ambiguous_entries`` for the caller to surface.

    An exact ``_name_key`` hit wins over fuzzy matching, so a genuinely short full name
    (``Ken``) still resolves to itself rather than colliding with ``Ken T.``.
    """
    if not rows:
        return list(rows or []), [], []
    pool = [str(p).strip() for p in (resigned_names() if names is None else names) if str(p or "").strip()]
    if not pool:
        return list(rows), [], []

    row_names: set[str] = set()
    for r in rows:
        nm = str(r.get(key) or "").strip()
        if nm:
            row_names.add(nm)

    by_key: dict[str, set[str]] = {}
    for nm in row_names:
        by_key.setdefault(od._name_key(nm), set()).add(nm)

    targets: set[str] = set()
    ambiguous: list[str] = []
    for entry in pool:
        exact = by_key.get(od._name_key(entry)) or set()
        if len(exact) == 1:
            targets |= exact
            continue
        if len(exact) > 1:
            ambiguous.append(entry)
            continue
        fuzzy = {nm for nm in row_names if od._names_same_person(nm, entry)}
        if len(fuzzy) == 1:
            targets |= fuzzy
        elif len(fuzzy) > 1:
            ambiguous.append(entry)

    kept = [r for r in rows if str(r.get(key) or "").strip() not in targets]
    return kept, sorted(targets), ambiguous


def add_resigned(name: str, token: Optional[str] = None) -> dict[str, Any]:
    """
    Add a person to the Resigned Member table. Writes Status=Resigned immediately.

    Refuses duplicates so repeated calls are safe.
    """
    nm = (name or "").strip()
    if not nm:
        raise ValueError("name is required")
    tok = token or od.get_tenant_access_token()
    existing = fetch_rows(tok)
    for r in existing:
        if r["name"] and od._names_same_person(nm, r["name"]):
            return {"added": False, "reason": "already listed", "matched": r["name"]}
    res = od._bitable_create_record(
        tok,
        TABLE_ID,
        {NAME_FIELD: nm, STATUS_FIELD: RESIGNED_STATUS},
        base_token=BASE_TOKEN,
    )
    record_id = ((res.get("data") or {}).get("record") or {}).get("record_id") or ""
    _cache["at"] = 0.0  # force refresh on next read
    out: dict[str, Any] = {"added": True, "name": nm, "record_id": record_id}
    if len(nm.split()) < 2:
        out["warning"] = (
            f"{nm!r} is a single word. Prefix matching means it may identify several "
            "people, in which case it is ignored rather than hiding the wrong person. "
            "Prefer the full name as it appears on the leave calendar."
        )
    return out


def _main(argv: list[str]) -> int:
    if not argv or argv[0] in ("--list", "-l"):
        rows = fetch_rows()
        active = [r for r in rows if r["status"].strip().lower() == RESIGNED_STATUS.lower()]
        blank = [r for r in rows if r["name"] and not r["status"].strip()]
        print(f"Resigned Member table ({BASE_TOKEN}/{TABLE_ID})")
        print(f"  rows: {len(rows)}   marked Resigned: {len(active)}")
        for r in active:
            print(f"    - {r['name']}")
        if blank:
            print(f"  {len(blank)} row(s) have a Name but no Status — ignored until set:")
            for r in blank:
                print(f"    ? {r['name']}")
        return 0
    if argv[0] in ("--add", "-a"):
        if len(argv) < 2:
            print("usage: resigned.py --add \"Full Name\"", file=sys.stderr)
            return 2
        res = add_resigned(" ".join(argv[1:]))
        print(res)
        return 0
    if argv[0] in ("--check", "-c"):
        if len(argv) < 2:
            print("usage: resigned.py --check \"Full Name\"", file=sys.stderr)
            return 2
        nm = " ".join(argv[1:])
        # Dry-run the real filter against this month's leave calendar so the answer
        # reflects what the bot would actually hide — including the ambiguity guard.
        import datetime
        import leavewfh as lw

        today = datetime.date.today()
        rows, _ = lw.fetch_leave_from_company_leave_calendar(
            od.get_tenant_access_token(), today.year, today.month
        )
        _, dropped, ambiguous = filter_rows(rows, names=[nm])
        if ambiguous:
            print(
                f"AMBIGUOUS — {nm!r} matches more than one person on the "
                f"{today:%B %Y} leave calendar, so it would be IGNORED.\n"
                "Use the person's full name exactly as it appears there."
            )
            return 1
        if dropped:
            print(f"OK — {nm!r} resolves to exactly one person: {dropped[0]!r}")
            return 0
        print(
            f"NO MATCH — {nm!r} matches nobody on the {today:%B %Y} leave calendar. "
            "That is fine if they have no leave this month; otherwise check the spelling."
        )
        return 0
    print(f"unknown argument: {argv[0]}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
