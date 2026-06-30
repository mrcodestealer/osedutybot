#!/usr/bin/env python3
"""Match HRMS / calendar person names to dutyList.csv (fuzzy + aliases)."""

from __future__ import annotations

import csv
import difflib
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parent
DEFAULT_DUTY_CSV = ROOT / "dutyList.csv"
_FUZZY_THRESHOLD = 0.82

# Calendar / HRMS label (normalized) → canonical dutyList.csv Name column
_CANONICAL_ALIASES: dict[str, str] = {
    "jeno": "Jeno",
    "jiunhou": "Jeno",
    "shieni": "Shie Ni",
    "bk": "Bk",
    "yuxuan": "Yuxuan",
    "yuk": "YK (Eng Khon)",
    "ykengkhon": "YK (Eng Khon)",
    "nicsonsoh": "Nicson",
    "nicolelai": "Nicole Lai",
    "ericlee": "Eric Lee",
    "junxian": "Jun Xian",
    "seehong": "See Hong",
    "jiaong": "Jia Hong",
    "kaixuan": "Kai Xuan",
    "eduardjames": "Eduard",
    "eduard": "Eduard",
    "katleen": "Kat",
    "kat": "Kat",
    "jewel": "Jewell",
    "jewell": "Jewell",
    "chrisjames": "Chrisjames",
    "augustinesiyew": "Augustine Si Yew",
    "manchung": "Man Chung",
    "chunchee": "Chun Chee",
    "junchen": "Jun Chen",
    "krisng": "Kris Ng",
    "khengkwan": "Kheng Kwan",
    "kwangming": "Kwang Ming",
    "faye": "Faye",
    "renzel": "Renzel",
    "rocklim": "Rock Lim",
    "wengyong": "Weng Yong",
    "maoshu": "Mau Shu",
    "maushu": "Mau Shu",
    "bryanpeh": "Bryan Peh",
    "bryan": "Bryan Peh",
}


def default_duty_csv_path() -> Path:
    return Path(os.getenv("DUTY_LIST_CSV", str(DEFAULT_DUTY_CSV))).expanduser().resolve()


def normalize_name(name: str) -> str:
    if not name:
        return ""
    s = re.sub(r"\([^)]*\)", "", name)
    s = re.sub(r"\[[^\]]*\]", "", s)
    s = re.sub(r"\s*(team\s*lead|manager|senior)\s*", "", s, flags=re.I)
    return re.sub(r"[^\w\u4e00-\u9fff]", "", s).lower()


def name_match_keys(raw: str) -> list[str]:
    """Several normalized keys for one calendar label (incl. text in parentheses)."""
    s = (raw or "").strip()
    if not s:
        return []
    keys: list[str] = []
    seen: set[str] = set()

    def _add(part: str) -> None:
        k = normalize_name(part)
        if k and k not in seen:
            seen.add(k)
            keys.append(k)

    _add(s)
    m = re.search(r"\(([^)]+)\)", s)
    if m:
        _add(m.group(1))
    _add(re.sub(r"\([^)]*\)", "", s))
    return keys


def is_ose_department(department: str) -> bool:
    d = (department or "").strip().upper()
    return d == "OSE" or d.startswith("OSE ")


@lru_cache(maxsize=4)
def _cached_duty_entries(csv_path: str) -> tuple[dict[str, str], ...]:
    entries: list[dict[str, str]] = []
    path = Path(csv_path)
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if len(row) < 3:
                continue
            name = row[0].strip()
            if not name:
                continue
            entries.append(
                {
                    "name": name,
                    "department": row[1].strip(),
                    "phone": row[2].strip(),
                }
            )
    return tuple(entries)


def load_duty_list(csv_path: Optional[Path] = None) -> list[dict[str, str]]:
    path = (csv_path or default_duty_csv_path()).resolve()
    return [dict(e) for e in _cached_duty_entries(str(path))]


def _build_norm_map(duty_entries: list[dict[str, str]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for e in duty_entries:
        nk = normalize_name(e["name"])
        if nk:
            out[nk] = e["name"]
    return out


def match_duty_entry(
    name: str,
    duty_entries: Optional[list[dict[str, str]]] = None,
    *,
    csv_path: Optional[Path] = None,
) -> Optional[dict[str, str]]:
    """Return full dutyList row if ``name`` matches (canonical name from CSV)."""
    raw = (name or "").strip()
    if not raw:
        return None
    entries = duty_entries if duty_entries is not None else load_duty_list(csv_path)
    if not entries:
        return None

    by_name = {e["name"]: e for e in entries}
    if raw in by_name:
        return dict(by_name[raw])

    norm_map = _build_norm_map(entries)
    for key in name_match_keys(raw):
        alias_target = _CANONICAL_ALIASES.get(key)
        if alias_target and alias_target in by_name:
            return dict(by_name[alias_target])
        if key in norm_map:
            return dict(by_name[norm_map[key]])

    keys = name_match_keys(raw)
    if not keys:
        return None
    primary = keys[0]
    if primary in norm_map:
        return dict(by_name[norm_map[primary]])

    for key in keys:
        for nk, canonical in norm_map.items():
            if key in nk or nk in key:
                return dict(by_name[canonical])

    best_ratio = 0.0
    best_name: Optional[str] = None
    for key in keys:
        for nk, canonical in norm_map.items():
            ratio = difflib.SequenceMatcher(None, key, nk).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_name = canonical
    if best_ratio >= _FUZZY_THRESHOLD and best_name:
        return dict(by_name[best_name])
    return None


def match_duty_name(
    name: str,
    duty_entries: Optional[list[dict[str, str]]] = None,
    *,
    csv_path: Optional[Path] = None,
) -> Optional[str]:
    row = match_duty_entry(name, duty_entries, csv_path=csv_path)
    return row["name"] if row else None


def is_ose_dutylist_name(
    name: str,
    duty_entries: Optional[list[dict[str, str]]] = None,
    *,
    csv_path: Optional[Path] = None,
) -> bool:
    """True when ``name`` maps to a dutyList.csv row whose department is OSE (incl. OSE Senior / Team Lead)."""
    row = match_duty_entry(name, duty_entries, csv_path=csv_path)
    return bool(row and is_ose_department(row["department"]))


def list_ose_dutylist_names(*, csv_path: Optional[Path] = None) -> list[str]:
    """Canonical OSE names from dutyList.csv (sorted)."""
    entries = load_duty_list(csv_path)
    names = sorted(
        {e["name"] for e in entries if is_ose_department(e["department"])},
        key=lambda x: x.lower(),
    )
    return names


def filter_leave_rows_to_ose_dutylist(
    rows: list[dict],
    duty_entries: Optional[list[dict[str, str]]] = None,
    *,
    csv_path: Optional[Path] = None,
) -> list[dict]:
    """
    Keep HRMS/Bitable leave rows for OSE duty display.

    Primary gate: name on the 31-person OSE shift roster (``ose_Duty.OSE_SHIFT_ROSTER``).
    Fallback: dutyList.csv row with OSE department (legacy / phone lookup).
    """
    import ose_Duty as od

    entries = duty_entries if duty_entries is not None else load_duty_list(csv_path)
    out: list[dict] = []
    seen: set[tuple[Any, ...]] = set()
    for row in rows:
        raw_name = str(row.get("name") or "")
        roster_key = od._resolve_ose_roster_key(raw_name)
        if not roster_key:
            for rk, _label in od.OSE_SHIFT_ROSTER:
                if od._names_same_person(raw_name, rk):
                    roster_key = rk
                    break
        entry = match_duty_entry(raw_name, entries)
        if roster_key:
            canonical = roster_key
            dept = (entry or {}).get("department") or "OSE"
        elif entry and is_ose_department(entry["department"]):
            canonical = entry["name"]
            dept = entry["department"]
        else:
            continue
        key = (
            canonical.lower(),
            row.get("start"),
            row.get("end"),
            str(row.get("leave_type") or "").strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        merged = dict(row)
        merged["name"] = canonical
        merged["department"] = dept
        out.append(merged)
    return out
