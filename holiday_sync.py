#!/usr/bin/env python3
"""
Sync SNSoft Public Holiday Listing (Lark calendar) → ``holiday.csv``.

CSV columns (no header): Code, Name, Date (DD/MM/YYYY), Day

Usage:
    python holiday_sync.py
    python holiday_sync.py --year 2026
    python holiday_sync.py --dry-run
"""

from __future__ import annotations

import argparse
import calendar
import csv
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

load_dotenv()

import ose_Duty as od

try:
    from leavewfh import CalendarFetchError, _calendar_get_json, _event_date_range
except ImportError:
    from leave import CalendarFetchError, _calendar_get_json, _event_date_range  # type: ignore

_HOLIDAY_CSV_PATH = Path(os.getenv("HOLIDAY_CSV_PATH", "holiday.csv"))
_PUBLIC_HOLIDAY_CALENDAR_QUERY = (
    os.getenv("PUBLIC_HOLIDAY_CALENDAR_QUERY") or "SNSoft Public Holiday Listing"
).strip()
_PUBLIC_HOLIDAY_CALENDAR_ID = (
    os.getenv("PUBLIC_HOLIDAY_CALENDAR_ID")
    or os.getenv("OSE_PUBLIC_HOLIDAY_CALENDAR_ID")
    or ""
).strip()

_HOLIDAY_CODE_PREFIX_RE = re.compile(
    r"^(?P<code>N\d+|R\d+)\s*(?:[-–—,:]\s*|\s+)(?P<name>.+)$",
    re.IGNORECASE,
)
_HOLIDAY_CSV_LINE_RE = re.compile(
    r"^(?P<code>N\d+|R\d+)\s*,\s*(?P<name>[^,]+)\s*,\s*(?P<date>\d{2}/\d{2}/\d{4})\s*,\s*(?P<day>.+?)\s*$",
    re.IGNORECASE,
)
_WEEKDAY_NAMES = tuple(calendar.day_name)


def _open_api_base() -> str:
    return (os.getenv("LARK_OPEN_API_BASE") or "https://open.larksuite.com/open-apis").rstrip("/")


_DESC_TAG_RE = re.compile(r"<[^>]+>")


def _plain_text(text: str) -> str:
    s = _DESC_TAG_RE.sub("", text or "")
    return re.sub(r"\s+", " ", s).strip()


def _year_unix_range(year: int) -> tuple[int, int]:
    start = int(datetime(year, 1, 1).timestamp())
    end = int(datetime(year, 12, 31, 23, 59, 59).timestamp())
    return start, end


def resolve_public_holiday_calendar_id(token: str) -> tuple[str, str]:
    """Return ``(calendar_id, calendar_title)`` for the SNSoft public holiday calendar."""
    if _PUBLIC_HOLIDAY_CALENDAR_ID:
        return _PUBLIC_HOLIDAY_CALENDAR_ID, _PUBLIC_HOLIDAY_CALENDAR_QUERY
    title = _PUBLIC_HOLIDAY_CALENDAR_QUERY
    url = f"{_open_api_base()}/calendar/v4/calendars/search"
    res = _calendar_get_json(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json_body={"query": title},
        method="post",
    )
    for item in (res.get("data") or {}).get("items") or []:
        cal = item.get("calendar") or item
        summary = str(cal.get("summary") or "").strip()
        cal_id = str(cal.get("calendar_id") or "").strip()
        if cal_id and summary.lower() == title.lower():
            return cal_id, summary
    for item in (res.get("data") or {}).get("items") or []:
        cal = item.get("calendar") or item
        summary = str(cal.get("summary") or "").strip()
        cal_id = str(cal.get("calendar_id") or "").strip()
        hay = summary.lower()
        if cal_id and title.lower() in hay and "holiday" in hay:
            return cal_id, summary
    return "", title


def fetch_public_holiday_events(token: str, calendar_id: str, year: int) -> list[dict[str, Any]]:
    """All non-cancelled events on the public holiday calendar for ``year``."""
    start_ts, end_ts = _year_unix_range(year)
    url = f"{_open_api_base()}/calendar/v4/calendars/{calendar_id}/events"
    items: list[dict[str, Any]] = []
    page_token = ""
    while True:
        params: dict[str, str] = {
            "start_time": str(start_ts),
            "end_time": str(end_ts),
            "page_size": "500",
        }
        if page_token:
            params["page_token"] = page_token
        res = _calendar_get_json(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            method="get",
        )
        data = res.get("data") or {}
        items.extend(data.get("items") or [])
        if not data.get("has_more"):
            break
        page_token = str(data.get("page_token") or "").strip()
        if not page_token:
            break
    return [ev for ev in items if (ev.get("status") or "").strip().lower() != "cancelled"]


def _infer_holiday_code(name: str) -> str:
    n = (name or "").strip()
    if re.search(r"(?i)\breplacement\b", n):
        return "R01"
    if re.search(r"(?i)federal\s+territory", n):
        return "N02"
    return "N01"


def _parse_holiday_text_line(text: str) -> Optional[dict[str, str]]:
    line = (text or "").strip()
    if not line:
        return None
    m = _HOLIDAY_CSV_LINE_RE.match(line)
    if m:
        return {
            "code": m.group("code").upper(),
            "name": m.group("name").strip(),
            "date": m.group("date").strip(),
            "day": m.group("day").strip(),
        }
    m = _HOLIDAY_CODE_PREFIX_RE.match(line)
    if m:
        return {
            "code": m.group("code").upper(),
            "name": m.group("name").strip(),
            "date": "",
            "day": "",
        }
    return None


def _row_from_event_day(code: str, name: str, on_date: date) -> dict[str, str]:
    return {
        "code": code.upper(),
        "name": name.strip(),
        "date": on_date.strftime("%d/%m/%Y"),
        "day": _WEEKDAY_NAMES[on_date.weekday()],
        "sort_date": on_date,
    }


def _parse_holiday_event(event: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert one Lark calendar event into zero or more holiday CSV rows."""
    summary = _plain_text(str(event.get("summary") or ""))
    description = _plain_text(str(event.get("description") or ""))
    rows: list[dict[str, Any]] = []

    for blob in (summary, description):
        for line in re.split(r"[\r\n]+", blob):
            parsed = _parse_holiday_text_line(line)
            if parsed and parsed.get("date"):
                try:
                    sort_date = datetime.strptime(parsed["date"], "%d/%m/%Y").date()
                except ValueError:
                    continue
                day = parsed.get("day") or _WEEKDAY_NAMES[sort_date.weekday()]
                rows.append(
                    {
                        "code": parsed["code"],
                        "name": parsed["name"],
                        "date": parsed["date"],
                        "day": day,
                        "sort_date": sort_date,
                    }
                )
        if rows:
            return rows

    code = _infer_holiday_code(summary)
    name = summary
    prefix = _parse_holiday_text_line(summary)
    if prefix:
        code = prefix["code"]
        name = prefix["name"]
    elif not name:
        return []

    start_d, end_d = _event_date_range(event)
    if not start_d:
        return []
    cur = start_d
    while cur <= end_d:
        rows.append(_row_from_event_day(code, name, cur))
        cur += timedelta(days=1)
    return rows


def events_to_holiday_rows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge parsed events, dedupe by date+name, sort by date."""
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for ev in events:
        for row in _parse_holiday_event(ev):
            key = (row["date"], row["name"].lower())
            by_key[key] = row
    out = list(by_key.values())
    out.sort(key=lambda r: r.get("sort_date") or date.max)
    return out


def write_holiday_csv(rows: list[dict[str, Any]], csv_path: Path | str = _HOLIDAY_CSV_PATH) -> None:
    path = Path(csv_path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow([row["code"], row["name"], row["date"], row["day"]])
    tmp.replace(path)


def probe_public_holiday_calendar(*, year: Optional[int] = None, limit: int = 10) -> dict[str, Any]:
    """Fetch raw calendar events for debugging event title/description format."""
    ref_year = year or date.today().year
    token = od.get_tenant_access_token()
    cal_id, cal_title = resolve_public_holiday_calendar_id(token)
    if not cal_id:
        return {"ok": False, "error": "calendar not found", "year": ref_year}
    events = fetch_public_holiday_events(token, cal_id, ref_year)
    sample = []
    for ev in events[:limit]:
        sample.append(
            {
                "summary": ev.get("summary"),
                "description": ev.get("description"),
                "start_time": ev.get("start_time"),
                "end_time": ev.get("end_time"),
                "parsed": events_to_holiday_rows([ev]),
            }
        )
    return {
        "ok": True,
        "year": ref_year,
        "calendar_id": cal_id,
        "calendar_title": cal_title,
        "raw_events": len(events),
        "sample": sample,
    }


def _sync_years(explicit_year: Optional[int] = None) -> list[int]:
    if explicit_year is not None:
        return [explicit_year]
    raw = (os.getenv("PUBLIC_HOLIDAY_SYNC_YEARS") or "").strip()
    if raw:
        out: list[int] = []
        for part in re.split(r"[,;\s]+", raw):
            part = part.strip()
            if not part:
                continue
            try:
                out.append(int(part))
            except ValueError:
                continue
        if out:
            return sorted(set(out))
    y = date.today().year
    return [y - 1, y, y + 1]


def sync_public_holidays_from_calendar(
    *,
    year: Optional[int] = None,
    csv_path: Path | str = _HOLIDAY_CSV_PATH,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Fetch SNSoft Public Holiday Listing and write ``holiday.csv``.

    Returns a result dict with ``ok``, ``count``, ``calendar_title``, ``years``, etc.
    """
    years = _sync_years(year)
    token = od.get_tenant_access_token()
    cal_id, cal_title = resolve_public_holiday_calendar_id(token)
    if not cal_id:
        return {
            "ok": False,
            "error": (
                f"Public holiday calendar not found (search: {_PUBLIC_HOLIDAY_CALENDAR_QUERY!r}). "
                "Set PUBLIC_HOLIDAY_CALENDAR_ID in .env."
            ),
            "years": years,
        }

    all_events: list[dict[str, Any]] = []
    fetch_errors: list[str] = []
    for ref_year in years:
        try:
            all_events.extend(fetch_public_holiday_events(token, cal_id, ref_year))
        except CalendarFetchError as exc:
            fetch_errors.append(f"{ref_year}: {exc}")

    if fetch_errors and not all_events:
        return {
            "ok": False,
            "error": "; ".join(fetch_errors),
            "years": years,
            "calendar_id": cal_id,
        }

    rows = events_to_holiday_rows(all_events)
    if not rows:
        return {
            "ok": False,
            "error": f"No holiday events parsed for {years} from {cal_title!r}.",
            "years": years,
            "calendar_id": cal_id,
            "calendar_title": cal_title,
            "raw_events": len(all_events),
            "warnings": fetch_errors,
        }

    if not dry_run:
        write_holiday_csv(rows, csv_path)
        try:
            from holiday import reload_holidays

            reload_holidays()
        except ImportError:
            pass

    return {
        "ok": True,
        "years": years,
        "count": len(rows),
        "calendar_id": cal_id,
        "calendar_title": cal_title,
        "raw_events": len(all_events),
        "csv_path": str(csv_path),
        "dry_run": dry_run,
        "warnings": fetch_errors,
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Sync SNSoft Public Holiday Listing → holiday.csv")
    parser.add_argument("--year", type=int, default=None, help="Calendar year (default: current year)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and parse only; do not write CSV")
    parser.add_argument("--probe", action="store_true", help="Print sample raw calendar events")
    parser.add_argument("--csv", default=str(_HOLIDAY_CSV_PATH), help="Output CSV path")
    args = parser.parse_args(argv)

    if args.probe:
        import json

        result = probe_public_holiday_calendar(year=args.year)
        print(json.dumps(result, indent=2, default=str))
        return 0 if result.get("ok") else 1

    result = sync_public_holidays_from_calendar(
        year=args.year,
        csv_path=args.csv,
        dry_run=args.dry_run,
    )
    if not result.get("ok"):
        print(f"Holiday sync failed: {result.get('error')}", file=sys.stderr)
        if result.get("raw_events") is not None:
            print(f"  raw_events={result['raw_events']}", file=sys.stderr)
        return 1

    print(
        f"Holiday sync OK: {result['count']} row(s) from {result.get('calendar_title')!r} "
        f"({', '.join(str(y) for y in result.get('years') or [])})"
        + (" (dry-run)" if result.get("dry_run") else f" → {result['csv_path']}")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
