#!/usr/bin/env python3
"""
Scan HRMS company leave + WFH calendars for this month and list names
that do not appear in dutyList.csv.

Usage (from repo root or Chatbox/):
  python3 scanCompany.py
  python3 scanCompany.py --month 2026-05
  python3 scanCompany.py --csv /path/to/dutyList.csv
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Optional

import duty_list_match as dlm

ROOT = Path(__file__).resolve().parent
DEFAULT_DUTY_CSV = ROOT / "dutyList.csv"


def _load_env() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(ROOT / ".env")
    except ImportError:
        env_path = ROOT / ".env"
        if not env_path.is_file():
            return
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip("\"'")
            if key and key not in __import__("os").environ:
                __import__("os").environ[key] = value


def _parse_month_arg(raw: str) -> tuple[int, int]:
    import re

    s = (raw or "").strip()
    m = re.match(r"^(\d{4})-(\d{1,2})$", s)
    if not m:
        raise ValueError(f"invalid --month {raw!r} (use YYYY-MM)")
    year, month = int(m.group(1)), int(m.group(2))
    if month < 1 or month > 12:
        raise ValueError("month must be 1–12")
    return year, month


def _fetch_company_names(year: int, month: int) -> tuple[set[str], set[str], list[str]]:
    import leavewfh as lw

    token = lw.get_tenant_access_token()
    warnings: list[str] = []
    leave_rows, w0 = lw.fetch_leave_from_company_leave_calendar(token, year, month)
    wfh_rows, w1 = lw.fetch_wfh_from_company_calendar(token, year, month)
    warnings.extend(w0)
    warnings.extend(w1)
    leave_names = {str(r["name"]).strip() for r in leave_rows if r.get("name")}
    wfh_names = {str(r["name"]).strip() for r in wfh_rows if r.get("name")}
    return leave_names, wfh_names, warnings


def _print_section(title: str, names: list[str]) -> None:
    print(f"\n{title} ({len(names)})")
    print("-" * 72)
    if not names:
        print("  (none)")
        return
    for name in names:
        print(f"  • {name}")


def main(argv: Optional[list[str]] = None) -> int:
    _load_env()
    parser = argparse.ArgumentParser(
        description="List company leave/WFH names this month that are missing from dutyList.csv"
    )
    parser.add_argument(
        "--month",
        metavar="YYYY-MM",
        help="Month to scan (default: current month)",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_DUTY_CSV,
        help=f"Path to duty list CSV (default: {DEFAULT_DUTY_CSV.name})",
    )
    args = parser.parse_args(argv)

    if args.month:
        year, month = _parse_month_arg(args.month)
    else:
        today = date.today()
        year, month = today.year, today.month

    csv_path = args.csv.expanduser().resolve()
    if not csv_path.is_file():
        print(f"❌ duty list not found: {csv_path}", file=sys.stderr)
        return 1

    duty = dlm.load_duty_list(csv_path)
    if not duty:
        print(f"❌ duty list is empty: {csv_path}", file=sys.stderr)
        return 1

    try:
        leave_names, wfh_names, warnings = _fetch_company_names(year, month)
    except Exception as exc:
        print(f"❌ Could not fetch HRMS calendars: {exc}", file=sys.stderr)
        return 1

    all_company = leave_names | wfh_names
    missing_leave = sorted(
        (n for n in leave_names if not dlm.match_duty_name(n, duty)),
        key=str.lower,
    )
    missing_wfh = sorted(
        (n for n in wfh_names if not dlm.match_duty_name(n, duty)),
        key=str.lower,
    )
    missing_any = sorted(
        (n for n in all_company if not dlm.match_duty_name(n, duty)),
        key=str.lower,
    )
    in_duty = sorted(
        (n for n in all_company if dlm.match_duty_name(n, duty)),
        key=str.lower,
    )

    month_label = f"{year}-{month:02d}"
    print(f"Company leave + WFH scan — {month_label}")
    print(f"dutyList.csv: {csv_path} ({len(duty)} entries)")
    print(f"HRMS unique names: {len(all_company)} (leave {len(leave_names)}, WFH {len(wfh_names)})")
    print(f"In dutyList.csv: {len(in_duty)}")
    print(f"NOT in dutyList.csv: {len(missing_any)}")

    for w in warnings:
        print(f"⚠️  {w}")

    _print_section("On leave this month — NOT in dutyList.csv", missing_leave)
    _print_section("On WFH this month — NOT in dutyList.csv", missing_wfh)
    _print_section("Leave or WFH — NOT in dutyList.csv (combined)", missing_any)

    only_leave = sorted(leave_names - wfh_names, key=str.lower)
    only_wfh = sorted(wfh_names - leave_names, key=str.lower)
    both = sorted(leave_names & wfh_names, key=str.lower)
    if only_leave or only_wfh or both:
        print(f"\nBreakdown (all {len(all_company)} company names)")
        print("-" * 72)
        print(f"  Leave only: {len(only_leave)}  |  WFH only: {len(only_wfh)}  |  Both: {len(both)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
