"""
Resigned Member list — read + write.

Source of truth is the ``Resigned Member`` Bitable table (Name + Status), kept by hand:
https://casinoplus.sg.larksuite.com/base/CpdEbEofwaYyyEsSjlElKNxzgec?table=tblQHimmQDEIlQ9n

Detection (``--sync``) is a HELPER, not a source of truth. Two signals, measured 2026-09-17
against the full 1454-user directory:

* ``name_suffix`` — HR renames a departing account to ``<Full Name>_Resigned``. Explicit,
  deliberate and monotone, so these are written with ``Status=Resigned``. 8 accounts, no
  false positives, and all 8 hold zero leave rows so they cannot hide anyone by accident.
* ``frozen`` — ``status.is_frozen``. Written with ``Status`` BLANK, always: there is
  deliberately no option to mark these Resigned automatically. ``is_frozen`` is an
  untimestamped, mutable snapshot Lark also uses for dormant and unprovisioned accounts,
  and the org's own "Lark Account Renewal Review Guidelines" describes employees who
  resign while "the account has not yet been deactivated". Guarded by: an employee number
  must exist, the name must not be role/room/bot shaped, and EVERY account sharing the
  name must be frozen — "Edmond" and "Jerry" each exist twice, one frozen and one not,
  and without unanimity a dormant namesake would hide a working colleague.

``status.is_resigned`` is dead: false for all 1454 users, because Lark drops a resigned
user from their departments rather than flagging them.

Absence from the directory is NOT a usable signal, despite appearances. It looked
compelling only because this module used to query the 18 root departments alone, which
returns 194 accounts — mostly meeting rooms, bots and role handles — making ~230 employed
people look absent. Zi Yang was the worked example of that mistake: apparently gone, in
fact present as SN0310 "Database Administrator" with ``is_frozen`` set. Hence
``MIN_DIRECTORY_USERS``, which refuses to detect at all on a short fetch.

What detection still cannot catch, and why names are added by hand: anyone who leaves with
an account that is neither renamed nor frozen, plus the ~24 calendar names with no
directory account at all. Both depend on IT/HR housekeeping that does not always happen.

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

# Shared/role/equipment accounts, never individual people.
NON_PERSON_RE = re.compile(
    r"(?i)\b(bot|room|dept|department|team|test|on[- ]?duty|shift|payroll|public|"
    r"supervisor|trainer|technician|acquisition|resignations)\b"
)

# Job-function handles appended to a name: "Myra QC-TL", "Imee RC". These belong to a role
# rather than a person, so they must never be written as an active filter entry.
ROLE_HANDLE_RE = re.compile(r"(?i)(?:^|[\s\-])(RC|QC|TL|HR|OPS|SUP|ADMIN)(?:[\s\-]|$)")


def is_safe_to_activate(name: str) -> bool:
    """
    Whether a detected name may be written with ``Status=Resigned`` rather than left blank.

    Deliberately does NOT require two tokens. Real leavers here include ``Henry`` and
    ``Lily``, and ``filter_rows`` already refuses to act on any entry that matches more
    than one calendar person — so a namesake collision is caught at filter time, where it
    can be reported, rather than silently costing recall at write time.

    What is still blocked: a CJK-only name (``od._name_key`` cannot key it, so it could
    collide with any other CJK name) and a role/equipment handle, which is not a person.
    """
    nm = (name or "").strip()
    return bool(
        nm
        and od._name_key(nm)
        and not ROLE_HANDLE_RE.search(nm)
        and not NON_PERSON_RE.search(nm)
    )

# The directory must come back roughly whole. A truncated fetch is the one failure that
# silently reclassifies hundreds of employed people, so detection refuses to run below this.
MIN_DIRECTORY_USERS = _int_env("RESIGNED_MIN_DIRECTORY_USERS", 1000)


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

    roots: list[str] = []
    page: Optional[str] = None
    while True:
        data = (_get("/contact/v3/scopes", page_size=50, **({"page_token": page} if page else {}))
                .get("data") or {})
        roots += data.get("department_ids") or []
        page = data.get("page_token")
        if not data.get("has_more"):
            break

    # /contact/v3/users returns DIRECT members of a department only — it does not recurse.
    # Querying just the 18 roots returns 194 accounts, and they are almost entirely meeting
    # rooms, bots and role handles ("IT on Duty- Stots", "QA on Duty - Dheights"); the real
    # staff all sit in child departments. Expanding to the 372 departments yields 1454 users.
    # Getting this wrong makes ~230 employed people look "absent from the directory".
    depts: set[str] = set(roots)
    for root in roots:
        page = None
        while True:
            data = (_get(f"/contact/v3/departments/{root}/children",
                         department_id_type="open_department_id", fetch_child=True,
                         page_size=50, **({"page_token": page} if page else {})).get("data") or {})
            for child in data.get("items") or []:
                if child.get("open_department_id"):
                    depts.add(child["open_department_id"])
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
    users = fetch_directory_users(token)
    if len(users) < MIN_DIRECTORY_USERS:
        raise RuntimeError(
            f"directory fetch returned only {len(users)} users (expected >= {MIN_DIRECTORY_USERS}). "
            "Refusing to detect: a truncated fetch makes employed people look absent."
        )

    # Namesake map: a frozen account only counts if EVERY account sharing its name is
    # frozen. "Edmond" exists twice — SN0448 frozen, T0174 not — and "Jerry" likewise.
    # Without this, one dormant namesake hides a working colleague's leave.
    by_name: dict[str, list[dict[str, Any]]] = {}
    for u in users:
        raw = u["name"] or u["en_name"]
        if raw:
            by_name.setdefault(_key(strip_resigned_suffix(raw)), []).append(u)

    found: list[dict[str, Any]] = []
    for u in users:
        raw = u["name"] or u["en_name"]
        if not raw:
            continue
        if RESIGNED_SUFFIX_RE.search(raw):
            # Opt-in only, and never confident. Checked against ground truth 2026-09-17:
            # the _Resigned rename is the Manila back-office convention (HR Supervisor,
            # HR Assistant, Payroll, Document Control — W###/B###/IGO### numbers) and was
            # wrong for 7 of 7 of them here, while catching only 1 of 8 real leavers.
            found.append({**u, "clean_name": strip_resigned_suffix(raw),
                          "source": "name_suffix", "confident": False})
            continue
        if not u["frozen"]:
            continue
        # A real departing employee has an employee number. The 2025-01-08 batch without
        # one, and anything role/room/bot shaped, are shared accounts — never people.
        if not (u["employee_no"] or "").strip():
            continue
        if NON_PERSON_RE.search(raw):
            continue
        if not all(o["frozen"] for o in by_name.get(_key(raw), [])):
            continue
        found.append({**u, "clean_name": raw, "source": "frozen", "confident": False})
    return found


def calendar_names(token: Optional[str] = None, months: int = 4) -> set[str]:
    """Distinct person names on the company leave calendar over the trailing window."""
    import datetime

    import leavewfh as lw

    tok = token or od.get_tenant_access_token()
    today = datetime.date.today()
    out: set[str] = set()
    y, m = today.year, today.month
    for _ in range(max(1, months)):
        try:
            rows, _w = lw.fetch_leave_from_company_leave_calendar(tok, y, m)
            out |= {_key(r["name"]) for r in rows if r.get("name")}
        except Exception as exc:
            print(f"[resigned] calendar {y}-{m:02d} unreadable: {exc!r}", flush=True)
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out


def detect_absent(token: Optional[str] = None, months: int = 6) -> list[dict[str, Any]]:
    """
    People holding leave-calendar rows who have NO Lark account at all.

    When someone leaves, IT eventually deletes the account — they stop being searchable in
    Lark while their past leave rows remain on the company calendar. Confirmed by the user
    on 2026-09-17 for 6 of 6 such names (Chin Yi, Yin Wei, Jin Wei, Ivan Wong, Crystal Tan,
    James Koh — "can't search" them any more).

    This is only trustworthy because ``fetch_directory_users`` now walks child departments:
    against the old 194-account fetch it would have flagged ~230 employed people. The
    ``MIN_DIRECTORY_USERS`` floor inside ``detect_leavers`` is what keeps that honest, and
    is re-checked here.

    Excludes anyone with leave dated in the current month or later — you cannot hold
    current leave if your account is gone; that combination means bad data, not a leaver.
    """
    import datetime

    import leavewfh as lw

    tok = token or od.get_tenant_access_token()
    users = fetch_directory_users(tok)
    if len(users) < MIN_DIRECTORY_USERS:
        raise RuntimeError(
            f"directory fetch returned only {len(users)} users (expected >= {MIN_DIRECTORY_USERS}); "
            "refusing to infer resignation from absence."
        )
    known = {_key(strip_resigned_suffix(u["name"] or u["en_name"])) for u in users}
    known.discard("")

    def name_variants(nm: str) -> list[str]:
        """
        Every spelling of a calendar name that might be how the directory holds it.

        The calendar writes aliases in brackets — "Jay (Chee Wai Nyin)" is the directory's
        "Jay". Comparing only the full string marks that person as absent, i.e. resigned.
        """
        out = [nm]
        outside = re.sub(r"\([^)]*\)", " ", nm).strip()
        if outside and outside != nm:
            out.append(outside)
        out += [m.strip() for m in re.findall(r"\(([^)]*)\)", nm) if m.strip()]
        return out

    today = datetime.date.today()
    cutoff = today.replace(day=1)
    seen: dict[str, dict[str, Any]] = {}
    y, m = today.year, today.month
    for _ in range(max(1, months)):
        try:
            rows, _w = lw.fetch_leave_from_company_leave_calendar(tok, y, m)
        except Exception as exc:
            print(f"[resigned] calendar {y}-{m:02d} unreadable: {exc!r}", flush=True)
            rows = []
        for r in rows:
            nm = (r.get("name") or "").strip()
            if not nm:
                continue
            if any(_key(v) in known for v in name_variants(nm)):
                continue
            k = _key(nm)
            e = seen.setdefault(k, {"clean_name": nm, "rows": 0, "last": None})
            e["rows"] += 1
            end = r.get("end")
            if end and (e["last"] is None or end > e["last"]):
                e["last"] = end
        m -= 1
        if m == 0:
            y, m = y - 1, 12

    out = []
    for k, e in seen.items():
        if e["last"] and e["last"] >= cutoff:
            continue  # still booking leave this month — not a leaver
        # A one-word calendar name is usually a nickname the directory holds under a full
        # name ("Eng" for "Shou Kwee", "Augustine" for "Augustine Si Yew"), so absence
        # proves nothing. A single leave row is likewise too thin to act on. Every name the
        # user confirmed as a real leaver had two tokens and three or more rows.
        if len(e["clean_name"].split()) < 2 or e["rows"] < 2:
            continue
        out.append({
            "name": e["clean_name"], "en_name": "", "open_id": "", "employee_no": "",
            "job_title": "", "join_time": 0, "frozen": False,
            "clean_name": e["clean_name"], "source": "absent", "confident": True,
            "calendar_rows": e["rows"], "last_leave": e["last"],
        })
    out.sort(key=lambda u: u["clean_name"].lower())
    return out


def sync_detected(
    token: Optional[str] = None,
    *,
    sources: tuple[str, ...] = ("frozen", "absent"),
    relevant_only: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Record detected leavers in the Resigned Member table.

    ``name_suffix`` hits are written with ``Status=Resigned`` — HR renaming an account to
    ``<Name>_Resigned`` is an explicit statement that the person has left.

    ``frozen`` hits are ALWAYS written with ``Status`` blank — there is deliberately no
    option to mark them Resigned automatically. ``is_frozen`` is an untimestamped, mutable
    snapshot that Lark also uses for dormant and not-yet-provisioned accounts, so a blank
    row is listed for review and filters nothing until a human sets the Status.
    """
    tok = token or od.get_tenant_access_token()
    detected = [d for d in detect_leavers(tok) if d["source"] in sources]
    if "absent" in sources:
        detected += detect_absent(tok)

    # A frozen account only earns a row if the person actually appears on the leave
    # calendar. Zi Yang is the template: frozen AND still holding leave rows the bot would
    # otherwise display. A frozen ex-colleague who never appears there is invisible either
    # way, so recording them is pure noise — it was that noise (27 inert rows) that made
    # the table unreadable. Suffix hits are exempt: an HR rename is evidence on its own.
    skipped_irrelevant: list[str] = []
    if relevant_only and any(d["source"] == "frozen" for d in detected):
        on_calendar = calendar_names(tok)
        keep = []
        for d in detected:
            if d["source"] == "frozen" and _key(d["clean_name"]) not in on_calendar:
                skipped_irrelevant.append(d["clean_name"])
            else:
                keep.append(d)
        detected = keep

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
        # "frozen AND appears on the leave calendar" is the validated rule: checked against
        # user-confirmed ground truth on 2026-09-17 it caught 7 of 8 real leavers with 0
        # false positives, so it writes an ACTIVE row. Anything else lands blank for review.
        validated = u["confident"] or (u["source"] == "frozen" and relevant_only)
        confirmed = validated and is_safe_to_activate(nm)
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
        "skipped_irrelevant": skipped_irrelevant,
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
        # Default is frozen-only. The _Resigned suffix leg is opt-in because it was wrong
        # for 7 of 7 against confirmed ground truth (it tracks a different org's convention).
        srcs: tuple[str, ...] = ("frozen", "absent")
        if "--with-suffix" in argv:
            srcs = ("frozen", "absent", "name_suffix")
        elif "--frozen-only" in argv:
            srcs = ("frozen",)
        elif "--absent-only" in argv:
            srcs = ("absent",)
        res = sync_detected(sources=srcs, relevant_only="--all-frozen" not in argv, dry_run=dry)
        verb = "would add" if dry else "added"
        print(f"detected        : {res['detected']}   (sources: {', '.join(srcs)})")
        print(f"already listed  : {len(res['skipped'])}")
        print(f"{verb:<15} : {len(res['added'])}\n")
        for a in res["added"]:
            tag = "Resigned" if a["status"] else "blank — review"
            print(f"    + {a['name']:<28} [{a['source']:<11}] status={tag}")
        for s in res["skipped"]:
            print(f"    = {s['name']:<28} [{s['source']:<11}] {s['reason']}")
        if res.get("skipped_irrelevant"):
            n = len(res["skipped_irrelevant"])
            print(f"\n  ({n} frozen account(s) skipped: no leave-calendar rows, so recording")
            print("   them would change nothing. Use --all-frozen to include them.)")
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
