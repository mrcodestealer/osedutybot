"""Expand machine / asset id ranges for the /list bot command."""

from __future__ import annotations

import re

_MACHINE_PREFIXES = r"NWR|NCH|NC|NP|TBR|TBP|MDR|DHS|CP|OSM|WF|WINFORD"
_TOKEN_RE = re.compile(rf"^({_MACHINE_PREFIXES})?(\d+)$", re.I)
_UNTIL_RE = re.compile(rf"^(.+?)(?:until|til|to)(.+)$", re.I)

_MAX_ITEMS = 500

USAGE_EXAMPLES = (
    "Usage: `/list <start>-<end>`, `/list <start>,<end>`, or `/list id1,id2,...`\n"
    "Examples:\n"
    "• `/list NWR8900-NWR8911`\n"
    "• `/list 8900-8911` or `/list 8905,8910`\n"
    "• `/list 8900,8901` (explicit ids)\n"
    "• `/list 8900until8911` (also `til` / `to`)"
)


def _parse_token(token: str) -> tuple[str | None, int]:
    token = (token or "").strip().upper()
    if not token:
        raise ValueError("empty token")
    m = _TOKEN_RE.match(token)
    if not m:
        raise ValueError(f"invalid token: {token!r}")
    prefix = m.group(1).upper() if m.group(1) else None
    return prefix, int(m.group(2))


def _expand_pair(start_t: str, end_t: str) -> tuple[str | None, int, int]:
    start_prefix, start_num = _parse_token(start_t)
    end_prefix, end_num = _parse_token(end_t)

    if start_prefix and end_prefix and start_prefix != end_prefix:
        raise ValueError("start and end prefix must match")

    prefix = start_prefix or end_prefix
    if start_num > end_num:
        raise ValueError("start must be <= end")
    return prefix, start_num, end_num


def parse_list_range(query: str) -> tuple[str | None, int, int]:
    """
    Parse range specs such as:
    - ``NWR8900-NWR8911``
    - ``8900-8911``
    - ``8900until8911`` / ``8900 til 8911`` / ``8900to8911``
    - ``NWR8900-8911``
    - ``8905,8910`` (comma between start and end)
    """
    raw = (query or "").strip()
    if not raw:
        raise ValueError("missing range")

    until_m = _UNTIL_RE.match(raw)
    if until_m:
        start_t, end_t = until_m.group(1).strip(), until_m.group(2).strip()
    elif "," in raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if len(parts) != 2:
            raise ValueError("comma range needs exactly start,end (or use as explicit id list)")
        start_t, end_t = parts
    elif "-" in raw:
        start_t, end_t = raw.split("-", 1)
        start_t, end_t = start_t.strip(), end_t.strip()
    else:
        raise ValueError("use -, comma, or until/til/to between start and end")

    return _expand_pair(start_t, end_t)


def _format_item(prefix: str | None, num: int) -> str:
    return f"{prefix}{num}" if prefix else str(num)


def expand_list_range(query: str, *, max_items: int = _MAX_ITEMS) -> list[str]:
    raw = (query or "").strip()
    if not raw:
        raise ValueError("missing input")

    if "," in raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if not parts:
            raise ValueError("empty list")
        if len(parts) == 2:
            prefix, start_num, end_num = _expand_pair(parts[0], parts[1])
            count = end_num - start_num + 1
            if count > max_items:
                raise ValueError(f"range too large ({count} items; max {max_items})")
            return [_format_item(prefix, n) for n in range(start_num, end_num + 1)]
        if len(parts) > max_items:
            raise ValueError(f"list too large ({len(parts)} items; max {max_items})")
        return [_format_item(*_parse_token(p)) for p in parts]

    prefix, start_num, end_num = parse_list_range(raw)
    count = end_num - start_num + 1
    if count > max_items:
        raise ValueError(f"range too large ({count} items; max {max_items})")
    return [_format_item(prefix, n) for n in range(start_num, end_num + 1)]


def format_list_range(query: str) -> str:
    """Return comma-separated expansion or a usage / error message."""
    try:
        items = expand_list_range(query)
    except ValueError as exc:
        return "❌ " + str(exc) + "\n\n" + USAGE_EXAMPLES
    return ", ".join(items)
