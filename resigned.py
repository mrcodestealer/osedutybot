"""
Resigned Member list — read + write.

Source of truth is the ``Resigned Member`` Bitable table (Name + Status), kept by hand:
https://casinoplus.sg.larksuite.com/base/CpdEbEofwaYyyEsSjlElKNxzgec?table=tblQHimmQDEIlQ9n

Detection (``--sync``) is a HELPER, not a source of truth. It can only ever find people
whose Lark account still exists, and it finds them two ways:

* ``name_suffix`` — HR renames a departing account to ``<Full Name>_Resigned``. Explicit
  and deliberate, so these are written with ``Status=Resigned`` directly. 7 real people
  as of 2026-09-17.
* ``frozen`` — ``status.is_frozen``. Weak: 10 of the 22 frozen accounts share one join
  date and look like shared/service accounts, and the org's own "Lark Account Renewal
  Review Guidelines" describes employees who resign while "the account has not yet been
  deactivated". Written with ``Status`` BLANK so a human confirms before anything is
  hidden. Note the two signals barely overlap — only 1 account is both.

What detection CANNOT do, and why the table still needs people added by hand: Lark removes
a resigned user from their departments, so they vanish from ``contact/v3`` entirely.
``status.is_resigned`` is therefore false for all 194 in-scope users, and a fully-removed
person has no account left to carry a suffix or a frozen flag. Zi Yang is the worked
example — confirmed resigned, absent from the directory, yet still holding three leave
rows on the company calendar. Absence itself is not usable as a signal either: only 2 of
235 leave-calendar names match the directory at all, because the app's contact scope and
the company leave calendar cover different populations.

Reads are cached for ``RESIGNED_CACHE_TTL`` seconds (default 300) so the list stays
current without re-fetching on every command.
"""

from __future__ import annotations

import os
import re
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
        by_key.setdefault(_key(nm), set()).add(nm)

    targets: set[str] = set()
    ambiguous: list[str] = []
    for entry in pool:
        exact = by_key.get(_key(entry)) or set()
        if len(exact) == 1:
            targets |= exact
            continue
        if len(exact) > 1:
            ambiguous.append(entry)
            continue
        # Fuzzy matching is only safe between two multi-token names. Allowing a longer
        # entry to swallow a one-word calendar name hides the wrong person: the entry
        # "Jerry Ann Bisonga" matched a calendar person called plain "Jerry", who is a
        # different human (the directory has a separate "Jerry" account). A one-word name
        # on either side must therefore match exactly or not at all.
        if len(entry.split()) < 2:
            # One-word entry with no exact hit. Report it if it would have matched
            # anyone, so a useless table row is visible rather than silently inert.
            if any(od._names_same_person(nm, entry) for nm in row_names):
                ambiguous.append(entry)
            continue
        fuzzy = {
            nm for nm in row_names
            if len(nm.split()) >= 2 and od._names_same_person(nm, entry)
        }
        if len(fuzzy) == 1:
            targets |= fuzzy
        elif len(fuzzy) > 1:
            ambiguous.append(entry)

    kept = [r for r in rows if str(r.get(key) or "").strip() not in targets]
    return kept, sorted(targets), ambiguous


def _open_api_base() -> str:
    return (os.getenv("LARK_OPEN_API_BASE") or "https://open.larksuite.com/open-apis").rstrip("/")


# HR renames a departing employee's Lark display name to "<Full Name>_Resigned".
# Anchored on the underscore and the end of the string on purpose: it must match
# "Larry Beltran_Resigned" but NOT the shared accounts "Resignations" or
# "User Acquisition (Resigned)", which are not people.
RESIGNED_SUFFIX_RE = re.compile(r"\s*_resigned\s*$", re.I)


def strip_resigned_suffix(name: str) -> str:
    return RESIGNED_SUFFIX_RE.sub("", (name or "").strip()).strip()


def _key(name: str) -> str:
    """
    Identity key for exact name comparison.

    ``od._name_key`` strips everything non-ASCII, so every CJK name keys to ``''`` and
    would compare equal to every other — 白墨, 白驹, 白龙 and 神书 are different people.
    Fall back to the casefolded raw string whenever the ASCII key comes out empty.
    """
    nm = (name or "").strip()
    return od._name_key(nm) or " ".join(nm.casefold().split())


def fetch_directory_users(token: Optional[str] = None) -> list[dict[str, Any]]:
    """
    Every user in the app's contact scope, with the fields needed to spot a leaver.

    ``status.is_resigned`` is useless here — it is false for everyone, because Lark drops
    a resigned user out of their departments entirely rather than flagging them. The two
    signals that do work are the ``_Resigned`` display-name suffix (an explicit HR action,
    high confidence) and ``is_frozen`` (account suspended — a weak hint, since the org's
    own account guidelines describe employees who resign while "the account has not yet
    been deactivated", and shared/service accounts are frozen too).
    """
    import requests

    tok = token or od.get_tenant_access_token()
    base = _open_api_base()
    headers = {"Authorization": f"Bearer {tok}"}

    def _get(path: str, **params: Any) -> dict[str, Any]:
        return requests.get(f"{base}{path}", headers=headers, params=params, timeout=25).json()

    depts: list[str] = []
    page: Optional[str] = None
    while True:
        data = (_get("/contact/v3/scopes", page_size=50, **({"page_token": page} if page else {}))
                .get("data") or {})
        depts += data.get("department_ids") or []
        page = data.get("page_token")
        if not data.get("has_more"):
            break

    out: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for dept in depts:
        page = None
        while True:
            data = (_get("/contact/v3/users", department_id=dept,
                         department_id_type="open_department_id", user_id_type="open_id",
                         page_size=50, **({"page_token": page} if page else {})).get("data") or {})
            for u in data.get("items") or []:
                oid = u.get("open_id") or ""
                if oid in seen_ids:
                    continue
                seen_ids.add(oid)
                out.append(
                    {
                        "name": (u.get("name") or "").strip(),
                        "en_name": (u.get("en_name") or "").strip(),
                        "open_id": oid,
                        "employee_no": u.get("employee_no") or "",
                        "job_title": u.get("job_title") or "",
                        "join_time": u.get("join_time") or 0,
                        "frozen": bool((u.get("status") or {}).get("is_frozen")),
                    }
                )
            page = data.get("page_token")
            if not data.get("has_more"):
                break
    out.sort(key=lambda u: u["name"].lower())
    return out


def detect_leavers(token: Optional[str] = None) -> list[dict[str, Any]]:
    """
    Candidate leavers from the Lark directory, each tagged with how it was found.

    ``source="name_suffix"`` — HR renamed the account to ``<Name>_Resigned``. An explicit,
    deliberate marking, so ``confident`` is True and the clean name is the stripped one.

    ``source="frozen"`` — the account is suspended. ``confident`` is False: 10 of the 22
    frozen accounts share a single join date and look like shared/service accounts, and a
    resigned employee's account is often left active, so this both over- and under-counts.
    """
    found: list[dict[str, Any]] = []
    for u in fetch_directory_users(token):
        raw = u["name"] or u["en_name"]
        if not raw:
            continue
        if RESIGNED_SUFFIX_RE.search(raw):
            found.append({**u, "clean_name": strip_resigned_suffix(raw),
                          "source": "name_suffix", "confident": True})
        elif u["frozen"]:
            found.append({**u, "clean_name": raw, "source": "frozen", "confident": False})
    return found


def sync_detected(
    token: Optional[str] = None,
    *,
    sources: tuple[str, ...] = ("name_suffix", "frozen"),
    mark_frozen_resigned: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Record detected leavers in the Resigned Member table.

    ``name_suffix`` hits are written with ``Status=Resigned`` — HR renaming an account to
    ``<Name>_Resigned`` is an explicit statement that the person has left.

    ``frozen`` hits are written with ``Status`` BLANK unless ``mark_frozen_resigned`` is
    set. A blank row is listed for review but does NOT filter anything, because
    ``resigned_names()`` only honours rows whose Status a human has set. That keeps a
    suspended-but-still-employed account from silently hiding someone's leave.
    """
    tok = token or od.get_tenant_access_token()
    detected = [d for d in detect_leavers(tok) if d["source"] in sources]
    existing = fetch_rows(tok)

    # Dedup on the EXACT normalised name, never od._names_same_person. These are distinct
    # directory accounts with known identities, and the fuzzy matcher prefix-matches:
    # it collapses "Jerry Ann Bisonga" into "Jerry", and folds every CJK handle account
    # (白驹, 白龙, 神书 …) into whichever one was seen first.
    have: set[str] = {_key(r["name"]) for r in existing if r["name"]}

    added: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for u in detected:
        nm = u["clean_name"]
        match = nm if _key(nm) in have else None
        if match:
            skipped.append({"name": nm, "source": u["source"], "reason": "already listed"})
            continue
        confirmed = u["confident"] or mark_frozen_resigned
        entry = {"name": nm, "source": u["source"], "status": RESIGNED_STATUS if confirmed else ""}
        if not dry_run:
            fields: dict[str, Any] = {NAME_FIELD: nm}
            if confirmed:
                fields[STATUS_FIELD] = RESIGNED_STATUS
            res = od._bitable_create_record(tok, TABLE_ID, fields, base_token=BASE_TOKEN)
            entry["record_id"] = ((res.get("data") or {}).get("record") or {}).get("record_id") or ""
        added.append(entry)
        have.add(_key(nm))

    if added and not dry_run:
        _cache["at"] = 0.0
    return {
        "detected": len(detected),
        "added": added,
        "skipped": skipped,
        "dry_run": dry_run,
    }


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
    # Exact match blocks the add; a fuzzy near-match only warns. "Jerry Ann Bisonga" and
    # "Jerry" are different people, so od._names_same_person must not veto the write.
    key = _key(nm)
    for r in existing:
        if r["name"] and _key(r["name"]) == key:
            return {"added": False, "reason": "already listed", "matched": r["name"]}
    near = [r["name"] for r in existing if r["name"] and od._names_same_person(nm, r["name"])]
    res = od._bitable_create_record(
        tok,
        TABLE_ID,
        {NAME_FIELD: nm, STATUS_FIELD: RESIGNED_STATUS},
        base_token=BASE_TOKEN,
    )
    record_id = ((res.get("data") or {}).get("record") or {}).get("record_id") or ""
    _cache["at"] = 0.0  # force refresh on next read
    out: dict[str, Any] = {"added": True, "name": nm, "record_id": record_id}
    if near:
        out["near_matches"] = near
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
    if argv[0] == "--sync":
        dry = "--dry-run" in argv
        mark_frozen = "--mark-frozen-resigned" in argv
        srcs: tuple[str, ...] = ("name_suffix", "frozen")
        if "--suffix-only" in argv:
            srcs = ("name_suffix",)
        elif "--frozen-only" in argv:
            srcs = ("frozen",)
        res = sync_detected(sources=srcs, mark_frozen_resigned=mark_frozen, dry_run=dry)
        verb = "would add" if dry else "added"
        print(f"detected        : {res['detected']}   (sources: {', '.join(srcs)})")
        print(f"already listed  : {len(res['skipped'])}")
        print(f"{verb:<15} : {len(res['added'])}\n")
        for a in res["added"]:
            tag = "Resigned" if a["status"] else "blank — review"
            print(f"    + {a['name']:<28} [{a['source']:<11}] status={tag}")
        for s in res["skipped"]:
            print(f"    = {s['name']:<28} [{s['source']:<11}] {s['reason']}")
        if any(a["source"] == "frozen" and not a["status"] for a in res["added"]):
            print("\nfrozen rows are CANDIDATES only — they change nothing until you set")
            print("Status=Resigned in Lark. Blank rows are ignored by the leave/WFH filter.")
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
