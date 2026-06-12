"""
Maintenance machine agent — schedule "set / unset maintenance & test" stress-test reminders.

When the bot is @mentioned with a *scheduled* maintenance announcement such as::

    @bot Please set maintenance and test ALL WF MACHINES Good Fortune later
    JUNE 09, 2026  09:45 pm, due to Change Parameters Settings tomorrow at 10:00am
    and followed by Stress Test.

    5 Dragons-WF8145
    Dragon of the Eastern Ocean-WF8146
    ...

this module parses:

* the **action** (``set`` / ``unset`` + ``maintenance`` / ``test`` / both),
* the **action time** (``JUNE 09, 2026  09:45 pm``),
* the **reason** (collapsed to *Stress Test* — the "Change Parameters" part is only the
  business reason and is ignored for the reminder), and
* the **target machines** — either the explicit pasted list, or, for
  ``ALL WF MACHINES Good Fortune``, every WF machine of that venue read from
  ``webmachine_data.json`` (so no live page-by-page lookup is needed just to schedule).

It then schedules a **one-time** reminder **10 minutes before** the action time (e.g. 09:35 pm)
via the existing Bitable reminder sheet (``reminder.add_sheet_reminder`` with ``When = One time``),
so it survives a bot restart. The reminder fires a rich card that lists the action + machines and
carries an "I have set maintenance" confirm button.

The agent only *schedules a reminder*; it never sets maintenance automatically. At the action
time the duty staff set maintenance manually (the existing ``@bot <env> set maintenance`` /
``/sm`` prod-batch flow), then confirm on the reminder card.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Sequence

# Fire the reminder this many minutes before the announced action time.
MAINT_LEAD_MINUTES = 10

# Map (set/unset) + (maintenance/test/both) → prod-batch action code + human label.
ACTION_LABELS: dict[str, str] = {
    "set_maint": "Set maintenance",
    "set_test": "Set test",
    "set_both": "Set maintenance and test",
    "unset_maint": "Unset maintenance",
    "unset_test": "Unset test",
    "unset_both": "Unset maintenance and test",
}

_MONTHS: dict[str, int] = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

# Env prefixes used to recognise pasted machine names + "ALL <ENV> MACHINES …" phrases.
_ENV_PREFIXES = ("NWR", "MDR", "NCH", "TBR", "TBP", "DHS", "CP", "OSM", "WF", "WINFORD")

# "5 Dragons-WF8145", "Pure Diamonds-WF8147", "NWR2113", "Echo-TBP8671" …
_MACHINE_LINE_RE = re.compile(
    r"(?:" + "|".join(_ENV_PREFIXES) + r")\s*-?\s*\d{2,}",
    re.I,
)

# "ALL WF MACHINES Good Fortune", "ALL MACHINES Good Fortune", "all wf machine Good Fortune"
_ALL_GROUP_RE = re.compile(
    r"\ball\b\s*(?P<env>" + "|".join(_ENV_PREFIXES) + r")?\s*machines?\s+"
    r"(?P<venue>[A-Za-z][A-Za-z .'&-]*?)"
    r"(?=\s+(?:later|due\b|tomorrow|at\b|on\b|by\b|before\b|followed\b)|[,.\n]|$)",
    re.I,
)

# Month DD[,] [YYYY] [at] HH[:MM][am/pm]   (date then time, as in the examples)
_DATETIME_RE = re.compile(
    r"\b(?P<mon>" + "|".join(_MONTHS.keys()) + r")\.?\s+"
    r"(?P<day>\d{1,2})(?:st|nd|rd|th)?\s*,?\s*"
    r"(?P<year>\d{4})?\s*,?\s*(?:at\s+)?"
    r"(?P<hh>\d{1,2})(?::(?P<mm>\d{2}))?\s*(?P<ap>am|pm)?",
    re.I,
)


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------
def _strip_mentions(text: str, mention_keys: Sequence[str]) -> str:
    t = text or ""
    for key in mention_keys or ():
        if key:
            t = t.replace(key, "")
    t = re.sub(r"@_user_\d+", "", t)
    t = re.sub(r"<[^>]+>", "", t)
    return t


_ENV_TOKEN_RE = re.compile(
    r"(NWR|MDR|NCH|TBR|TBP|DHS|OSM|CP|WF|WINFORD)\s*-?\s*\d",
    re.I,
)


def _env_from_machine_name(machine_name: str) -> str | None:
    """
    WF / NWR / … environment from a machine display name.

    Recognises the env token immediately before the asset id anywhere in the name
    (``5 Dragons-WF8145`` → ``WF``, ``Echo-TBP8671`` → ``TBP``), and falls back to the
    smmachine prefix rule / ``winford`` keyword.
    """
    raw = (machine_name or "").strip()
    if not raw:
        return None
    m = _ENV_TOKEN_RE.search(raw)
    if m:
        env = m.group(1).upper()
        if env == "WINFORD":
            return "WF"
        if env == "OSM":
            return "CP"
        return env
    if re.search(r"winford", raw, re.I):
        return "WF"
    try:
        import smmachine

        return smmachine._prod_batch_machine_env_from_name(raw)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------
def parse_action(text: str) -> str | None:
    """Return a prod-batch action code (``set_both`` …) from natural-language text, or ``None``."""
    tl = (text or "").lower()
    m = re.search(r"\b(set|unset)\b", tl)
    if not m:
        return None
    op = m.group(1)
    # Action phrase = from the verb up to the target / reason boundary.
    seg = tl[m.start():]
    seg = re.split(
        r"\bdue to\b|\blater\b|\btomorrow\b|\ball\b|\bfollowed by\b|\bon\b|\bat\b|\n",
        seg,
        maxsplit=1,
    )[0]
    has_maint = bool(re.search(r"maintenance|maintain", seg))
    has_test = bool(re.search(r"\btest\b", seg))
    if not (has_maint or has_test):
        return None
    if has_maint and has_test:
        what = "both"
    elif has_maint:
        what = "maint"
    else:
        what = "test"
    return f"{op}_{what}"


def parse_action_datetime(text: str, *, now: datetime | None = None) -> datetime | None:
    """Parse the first ``Month DD[, YYYY] HH:MM[am/pm]`` occurrence into a datetime."""
    now = now or datetime.now()
    m = _DATETIME_RE.search(text or "")
    if not m:
        return None
    mon = _MONTHS.get(m.group("mon").lower())
    if not mon:
        return None
    day = int(m.group("day"))
    hh = int(m.group("hh"))
    ap = (m.group("ap") or "").lower()
    # Hour-only is only a valid time when am/pm is present (avoids matching a stray number).
    if m.group("mm") is None and not ap:
        return None
    mm = int(m.group("mm")) if m.group("mm") is not None else 0
    if ap == "pm" and hh != 12:
        hh += 12
    elif ap == "am" and hh == 12:
        hh = 0
    if not (0 <= hh <= 23 and 0 <= mm <= 59 and 1 <= day <= 31):
        return None
    year_raw = m.group("year")
    year = int(year_raw) if year_raw else now.year
    try:
        dt = datetime(year, mon, day, hh, mm)
    except ValueError:
        return None
    # No explicit year and the date already passed → assume next year.
    if not year_raw and dt < now - timedelta(minutes=1):
        try:
            dt = dt.replace(year=year + 1)
        except ValueError:
            return None
    return dt


def parse_reason(text: str) -> str:
    """Collapse the announcement reason to a short label (defaults to *Stress Test*)."""
    if re.search(r"stress\s*test", text or "", re.I):
        return "Stress Test"
    m = re.search(r"\bdue to\s+(.+?)(?=[,.\n]|$)", text or "", re.I)
    if m:
        return m.group(1).strip()
    return "Stress Test"


def extract_machine_lines(text: str) -> list[str]:
    """Pasted machine display names, one per line (``5 Dragons-WF8145`` …)."""
    out: list[str] = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        # Skip the imperative / reason line even if it mentions an env keyword.
        if re.search(r"\b(set|unset)\b", line, re.I) and not _MACHINE_LINE_RE.search(line):
            continue
        if _MACHINE_LINE_RE.search(line):
            out.append(line)
    # De-dupe, keep order.
    seen: set[str] = set()
    uniq: list[str] = []
    for x in out:
        if x.lower() not in seen:
            seen.add(x.lower())
            uniq.append(x)
    return uniq


def parse_all_group(text: str) -> dict[str, str] | None:
    """Detect ``ALL [ENV] MACHINES <Venue>`` → ``{"env_code": "WF", "venue": "Good Fortune"}``."""
    m = _ALL_GROUP_RE.search(text or "")
    if not m:
        return None
    env = (m.group("env") or "").strip().upper()
    if env == "WINFORD":
        env = "WF"
    if env == "OSM":
        env = "CP"
    venue = re.sub(r"\s+", " ", (m.group("venue") or "").strip())
    # Drop a trailing "machine(s)" noise word if the regex over-captured.
    venue = re.sub(r"\bmachines?\b", "", venue, flags=re.I).strip()
    return {"env_code": env, "venue": venue}


# ---------------------------------------------------------------------------
# webmachine_data.json lookup (for "ALL <ENV> MACHINES <Venue>")
# ---------------------------------------------------------------------------
def _webmachine_data_path() -> Path:
    custom = (os.environ.get("WEBMACHINE_DATA_PATH") or "").strip()
    if custom:
        return Path(custom)
    return Path(__file__).resolve().parent / "webmachine_data.json"


def load_webmachine_rows() -> list[dict]:
    """Load + normalise rows from ``webmachine_data.json`` (empty list if missing/invalid)."""
    p = _webmachine_data_path()
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if isinstance(raw, dict):
        raw = raw.get("machines") or raw.get("rows") or raw.get("data") or []
    if not isinstance(raw, list):
        return []
    rows: list[dict] = []
    for r in raw:
        if isinstance(r, dict):
            rows.append(r)
    return rows


def _row_matches_env(row: dict, env_code: str) -> bool:
    env = (env_code or "").strip().upper()
    if not env or env == "ALL":
        return True
    belongs = str(row.get("belongs") or "").strip().upper()
    name = str(row.get("name") or row.get("machine") or "")
    if env == "CP":
        return belongs in ("CP", "OSM") or _env_from_machine_name(name) == "CP"
    return belongs == env or _env_from_machine_name(name) == env


def _row_display_name(row: dict) -> str:
    return str(row.get("name") or row.get("machine") or "").strip()


def resolve_all_group(env_code: str, venue: str) -> tuple[list[str], str]:
    """
    Resolve ``ALL <ENV> MACHINES <Venue>`` to machine display names from ``webmachine_data.json``.

    Returns ``(machine_names, note)``. Matching strategy:

    1. Keep rows in the requested environment (``WF`` …).
    2. If a venue phrase is given, keep rows whose ``belongs`` / name / game_type contains it.
    3. If the venue phrase matches nothing but the environment has machines, fall back to **all**
       machines of that environment and say so in ``note`` (the persisted data only labels the
       environment, not the venue, so this keeps the schedule usable).
    """
    rows = load_webmachine_rows()
    if not rows:
        return [], "⚠️ webmachine_data.json is empty or missing — could not resolve the machine list."

    env_rows = [r for r in rows if _row_matches_env(r, env_code)]
    if not env_rows:
        return [], f"⚠️ No {env_code or 'matching'} machines found in webmachine_data.json."

    venue_key = re.sub(r"[^a-z0-9]", "", (venue or "").lower())
    if not venue_key:
        names = [_row_display_name(r) for r in env_rows if _row_display_name(r)]
        return names, ""

    def _hay(r: dict) -> str:
        bits = [r.get("belongs"), r.get("name"), r.get("machine"), r.get("game_type"), r.get("venue")]
        return re.sub(r"[^a-z0-9]", "", " ".join(str(b or "") for b in bits).lower())

    venue_rows = [r for r in env_rows if venue_key in _hay(r)]
    if venue_rows:
        names = [_row_display_name(r) for r in venue_rows if _row_display_name(r)]
        return names, ""

    names = [_row_display_name(r) for r in env_rows if _row_display_name(r)]
    note = (
        f"ℹ️ Venue “{venue}” is not separately labelled in webmachine_data.json — "
        f"used all {len(names)} {env_code} machine(s) instead."
    )
    return names, note


# ---------------------------------------------------------------------------
# Announcement → reminder
# ---------------------------------------------------------------------------
def parse_announcement(text: str, *, now: datetime | None = None) -> dict[str, Any] | None:
    """
    Parse a scheduled maintenance announcement.

    Returns ``None`` when the message is not a *scheduled* maintenance request (no action, or no
    future date/time, or no resolvable machines). A return value means the message should be
    handled as a maintenance schedule.
    """
    action = parse_action(text)
    if not action:
        return None
    action_dt = parse_action_datetime(text, now=now)
    if not action_dt:
        return None

    machines = extract_machine_lines(text)
    all_group = None
    note = ""
    if not machines:
        all_group = parse_all_group(text)
        if all_group:
            machines, note = resolve_all_group(all_group["env_code"], all_group["venue"])
    if not machines:
        return None

    env_codes = sorted({e for e in (_env_from_machine_name(m) for m in machines) if e})
    env_summary = "/".join(env_codes) if env_codes else (all_group or {}).get("env_code", "") or "?"

    return {
        "action": action,
        "action_label": ACTION_LABELS.get(action, action),
        "action_dt": action_dt,
        "reminder_dt": action_dt - timedelta(minutes=MAINT_LEAD_MINUTES),
        "reason": parse_reason(text),
        "machines": machines,
        "env_summary": env_summary,
        "all_group": all_group,
        "note": note,
    }


def is_maintenance_schedule_message(original_text: str, mention_keys: Sequence[str]) -> bool:
    """True when the (mention-stripped) message is a *scheduled* maintenance announcement."""
    body = _strip_mentions(original_text, mention_keys)
    return parse_announcement(body) is not None


def build_reminder_reason(parsed: dict[str, Any]) -> str:
    """Rich reminder text stored in the sheet ``Reason`` (starts with the maintenance marker)."""
    from reminder import MAINT_REMINDER_MARKER

    action_label = parsed["action_label"]
    action_dt: datetime = parsed["action_dt"]
    machines: list[str] = parsed["machines"]
    when_str = action_dt.strftime("%b %d, %Y %I:%M %p").replace(" 0", " ")
    env_summary = parsed.get("env_summary") or "?"
    reason = (parsed.get("reason") or "Stress Test").strip()
    header = MAINT_REMINDER_MARKER
    if reason and reason.lower() != "stress test":
        header += f" — {reason}"
    lines = [
        header,
        f"**Action:** {action_label} at **{when_str}** ({env_summary})",
        f"**Machines ({len(machines)}):**",
    ]
    lines.extend(f"• {m}" for m in machines)
    note = parsed.get("note")
    if note:
        lines.append("")
        lines.append(note)
    return "\n".join(lines)


def handle_maintenance_schedule_message(
    original_text: str,
    mention_keys: Sequence[str],
    *,
    chat_id: str,
    send_message: Callable[..., Any],
    get_token_func: Callable[[], str],
    scheduler: Any,
    target_user_id: str,
    schedule_chat_id: str | None = None,
) -> tuple[bool, str | None]:
    """
    Parse the announcement and schedule a one-time reminder ``MAINT_LEAD_MINUTES`` before the
    action time. Returns ``(handled, optional_reply_text)``.
    """
    import reminder

    body = _strip_mentions(original_text, mention_keys)
    parsed = parse_announcement(body)
    if not parsed:
        return False, None

    reminder_dt: datetime = parsed["reminder_dt"]
    action_dt: datetime = parsed["action_dt"]
    now = datetime.now()
    when_str = action_dt.strftime("%b %d, %Y %I:%M %p")
    if action_dt <= now:
        return True, (
            f"⚠️ The action time **{when_str}** is already in the past "
            f"(now is {now.strftime('%b %d, %Y %I:%M %p')}). Nothing scheduled — "
            f"if this still needs doing, set maintenance now, or re-send with a future date/time."
        )
    if reminder_dt <= now:
        return True, (
            f"⚠️ The action time **{when_str}** is less than {MAINT_LEAD_MINUTES} min away, "
            f"so the {MAINT_LEAD_MINUTES}-min-early reminder would land in the past. "
            f"Nothing scheduled — please set maintenance now if needed."
        )

    reason_text = build_reminder_reason(parsed)
    try:
        result = reminder.add_sheet_reminder(
            start_raw=action_dt.strftime("%Y/%m/%d"),
            end_raw=action_dt.strftime("%Y/%m/%d"),
            time_raw=reminder_dt.strftime("%H:%M"),
            reason=reason_text,
            when_labels=["One time"],
            get_token_func=get_token_func,
            scheduler=scheduler,
            send_func=send_message,
            chat_id=chat_id,
            target_user_id=target_user_id,
            schedule_chat_id=schedule_chat_id,
        )
    except Exception as e:  # noqa: BLE001
        return True, f"❌ Failed to schedule maintenance reminder: {e}"

    if result:
        # add_sheet_reminder returns a non-empty string only on failure.
        return True, result

    summary = (
        f"✅ Scheduled stress-test reminder for **{reminder_dt.strftime('%b %d, %Y %I:%M %p')}** "
        f"({MAINT_LEAD_MINUTES} min before {action_dt.strftime('%I:%M %p')}).\n"
        f"• Action: {parsed['action_label']} ({parsed.get('env_summary')})\n"
        f"• Machines: {len(parsed['machines'])}"
    )
    if parsed.get("note"):
        summary += f"\n{parsed['note']}"
    return True, summary
