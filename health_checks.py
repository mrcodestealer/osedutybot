"""Checks for the daily health report card (health_report.py).

``start(bot)`` is called once from ``main._run_main_entry()`` with the running main
module, so the checks can read its scheduler and Lark settings without importing main
again (CLI tools import main; this must never start from there).

Every check is read-only: in-memory state, file mtimes, or one short request (a Lark
tenant-token fetch, the Ollama model list). Nothing here sends a message, writes a
sheet/Bitable, logs in anywhere new, or touches a browser page. The card itself is only
sent from the systemd service (see start()), never from a local PC run.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

import requests

import health_report

_LARK_BASE = "https://open.larksuite.com"  # same host main.py hardcodes for REST
# Registered unconditionally at import time in main.py — one missing means a broken boot.
_CORE_JOBS = (
    "ose_leave_wfh_calendar_sync_interval",
    "ose_leave_offset_daily_sync",
    "poll_offset_approver_notifications",
    "public_holiday_csv_sync_daily",
    "reminder_sheet_daily_sync",
    "resigned_member_sync_daily",
    "monthly_duty_check",
)

_bot: Any = None
_started = False


def _ago(seconds: float) -> str:
    s = int(max(0, seconds))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s // 3600}h {s % 3600 // 60}m"
    return f"{s // 86400}d {s % 86400 // 3600}h"


def _stamp_age(stamp, tz) -> float | None:
    """Age in seconds of a watcher ``_now_str()`` stamp ('YYYY-MM-DD HH:MM:SS' in ``tz``)."""
    try:
        at = datetime.strptime(str(stamp), "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
    except (TypeError, ValueError):
        return None
    return time.time() - at.timestamp()


def _thread_alive(name: str) -> bool:
    """Exact-name match. health_report's thread row matches by prefix, so there
    "tg-warm" would also be satisfied by its never-exiting "tg-warm-ka" sibling."""
    return any(t.name == name and t.is_alive() for t in threading.enumerate())


# ---------------------------------------------------------------- checks


def check_lark_api():
    """Fresh tenant-token fetch (not main's cache, so an outage is not hidden by it)."""
    if not (_bot.APP_ID and _bot.APP_SECRET):
        return "fail", "APP_ID / APP_SECRET not set"
    t0 = time.monotonic()
    r = requests.post(
        f"{_LARK_BASE}/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": _bot.APP_ID, "app_secret": _bot.APP_SECRET},
        timeout=10,
    )
    ms = (time.monotonic() - t0) * 1000
    try:
        body = r.json()
    except ValueError:
        return "fail", f"HTTP {r.status_code}, non-JSON reply"
    if body.get("code") == 0 and body.get("tenant_access_token"):
        return "ok", f"tenant token OK in {ms:.0f} ms"
    return "fail", f"HTTP {r.status_code}, Lark code {body.get('code')}: {str(body.get('msg') or '')[:60]}"


def check_scheduler():
    """APScheduler alive, core jobs registered, and no job stuck in the past."""
    sch = _bot.scheduler
    if not sch.running:
        return "fail", "APScheduler is not running"
    jobs = sch.get_jobs()
    ids = {j.id for j in jobs}
    missing = [j for j in _CORE_JOBS if j not in ids]
    # A healthy scheduler always moves next_run_time into the future when it fires a
    # job, so one far in the past means its loop is stalled. 3x the interval for
    # interval jobs (the 3-min offset poll catches a stall within ~9 min), 10 min for cron.
    now = time.time()
    overdue = []
    for j in jobs:
        nrt = getattr(j, "next_run_time", None)
        if nrt is None:
            continue
        iv = getattr(j.trigger, "interval", None)
        limit = 3 * iv.total_seconds() if iv is not None else 600.0
        if now - nrt.timestamp() > limit:
            overdue.append(j.id)
    prefix = getattr(sys.modules.get("reminder"), "_SHEET_JOB_PREFIX", "") or None
    n_sheet = sum(1 for j in jobs if prefix and str(j.id).startswith(prefix))
    detail = f"{len(jobs)} jobs ({n_sheet} sheet reminders)"
    if missing:
        return "fail", f"{detail}; missing: {', '.join(missing)}"
    if overdue:
        return "warn", f"{detail}; overdue: {', '.join(overdue[:3])}"
    return "ok", f"running, {detail}"


def check_llm():
    """Ollama answers /api/tags and has the chat + command models pulled (no generation)."""
    ca = sys.modules.get("chatagent")
    if ca is None:
        return None, "chatagent not loaded"
    models = []
    if ca.is_enabled() and ca.llm_available():
        models.append(ca._llm_model())
    cmd = sys.modules.get("commandagent")
    if cmd is not None and cmd._cmd_llm_enabled():
        models.append(cmd._cmd_llm_model())
    if not models:
        return None, "disabled (BOT_USE_CHATAGENT / BOT_USE_AI off or no BOT_CHAT_API_KEY)"
    base = ca._llm_base_url()
    parts = urlsplit(base)
    host = f"{parts.hostname}:{parts.port}" if parts.port else str(parts.hostname)
    if "11434" not in base and "ollama" not in base.lower():
        return None, f"hosted API at {parts.hostname}; not probed"
    native = base[:-3] if base.endswith("/v1") else base
    t0 = time.monotonic()
    r = requests.get(native.rstrip("/") + "/api/tags", timeout=5)
    ms = (time.monotonic() - t0) * 1000
    if r.status_code != 200:
        return "fail", f"HTTP {r.status_code} from {host}"
    have = {str(m.get("name") or "") for m in (r.json().get("models") or [])}
    wanted = sorted(set(models))
    missing = [m for m in wanted if m not in have and f"{m}:latest" not in have]
    if missing:
        return "warn", f"{host} up but not pulled: {', '.join(missing)}"
    return "ok", f"{host} HTTP 200 in {ms:.0f} ms, {', '.join(wanted)} installed"


def check_mail():
    """IMAP watcher started and the allemail.json header scan (own IMAP login) is fresh."""
    mm = sys.modules.get("maintenance_mail")
    if mm is None:
        return None, "maintenance_mail not loaded"
    if not mm.MAIL_PASSWORD:
        return None, "disabled (MAINTENANCE_MAIL_PASSWORD not set)"
    if not mm._watcher_started:
        return "fail", "IMAP watcher not started (MAINTENANCE_MAIL_TARGET_CHAT_ID empty?)"
    if not mm._allemail_scanner_started:
        return "ok", "IMAP watcher started (header cache off)"
    iv = float(mm.ALLEMAIL_SCAN_INTERVAL_SEC)
    try:
        age = time.time() - os.path.getmtime(mm.ALLEMAIL_STORE_PATH)
    except OSError:
        return "warn", "allemail.json not written yet"
    if age > 3 * iv:
        return "warn", f"header scan last saved {_ago(age)} ago (every {_ago(iv)}); IMAP login failing?"
    return "ok", f"IMAP watcher started, header scan {_ago(age)} ago"


def check_machine_scrape():
    """Webmachine live scrape: last run age, row count, failing backend sites."""
    wa = sys.modules.get("webapp")
    if wa is None:
        return None, "dashboard not mounted (WEBMACHINE_MOUNT_IN_MAIN)"
    if not wa._scrape_enabled():
        return None, "disabled by WEBMACHINE_SCRAPE"
    if not wa._bg_started:
        return "fail", "scrape loop not started"
    with wa._scrape_lock:
        ts = float(wa._scrape_ts or 0.0)
        n_rows = len(wa._scrape_rows)
        errs = dict(wa._scrape_errs)
    if not ts:
        return "warn", "no scrape finished yet"
    try:
        iv = int((os.environ.get("WEBMACHINE_SCRAPE_INTERVAL_SEC") or "30").strip() or "30")
    except ValueError:
        iv = 30
    # One cycle is a full multi-site browser scrape plus the interval, so floor at 10 min.
    stale = max(3 * max(iv, 3), 600)
    age = time.time() - ts
    fatal = sorted(k for k in errs if str(k).startswith("_"))  # _fatal / _worker / _import
    # "skipped — same EGM as ..." are permanent alias notes, not failures.
    bad = sorted(k for k, v in errs.items() if not str(k).startswith("_") and not str(v).startswith("skipped"))
    # QAT/UAT test backends ("QAT:CP") are listed but do not turn the row amber.
    bad_test = [k for k in bad if str(k).upper().startswith(("QAT:", "UAT:"))]
    bad = [k for k in bad if k not in bad_test]
    if fatal:
        return "fail", f"scrape error ({', '.join(fatal)}), last run {_ago(age)} ago"
    if n_rows == 0:
        return "fail", f"0 machines in the last scrape ({_ago(age)} ago)"
    detail = f"{n_rows} machines, updated {_ago(age)} ago"
    if age > stale:
        return "warn", f"{detail} (every {iv}s)"
    if bad:
        more = f" +{len(bad) - 4}" if len(bad) > 4 else ""
        return "warn", f"{detail}; failing: {', '.join(bad[:4])}{more}"
    if bad_test:
        more = f" +{len(bad_test) - 4}" if len(bad_test) > 4 else ""
        return "ok", f"{detail}; test backends failing: {', '.join(bad_test[:4])}{more}"
    return "ok", detail


def check_osmwatch():
    """OSM-Watch warm browser: started, not waiting for /loginosmwatch, encoder data fresh."""
    ow = sys.modules.get("osmwatch")
    if ow is None or not ow._warm_enabled():
        return None, "disabled by OSMWATCH_WARM"
    w = ow._warm_singleton
    if w is None or not w._started:
        return "fail", "warm browser not started"
    if not _thread_alive("osmwatch-warm"):
        return "fail", "worker thread osmwatch-warm is dead"
    if ow._get_needs_manual():
        return "fail", "session expired, waiting for /loginosmwatch"
    if not ow._encoder_enabled():
        return "ok", "session OK (encoder scrape off)"
    iv = ow._encoder_interval_sec()
    try:
        age = time.time() - os.path.getmtime(ow.LATESTENCODER_JSON)
    except OSError:
        return "warn", "session OK, no latestencoder.json yet"
    if age > 3 * iv:
        return "warn", f"encoder data {_ago(age)} old (every {_ago(iv)})"
    return "ok", f"session OK, encoder data {_ago(age)} old"


def check_teams():
    """Teams EVO-maintenance watcher: poll loop alive and succeeding."""
    tw = sys.modules.get("teamswatch")
    if tw is None or not tw._watch_enabled():
        return None, "disabled by EVOTEAMS_ENABLED"
    if not tw.warm_running():
        return "fail", f"watcher not running ({tw._snapshot().get('phase')})"
    if not _thread_alive("teams-warm"):
        return "fail", "worker thread teams-warm is dead"
    st = tw.warm().stats()
    poll = tw._poll_seconds()
    fails = int(st.get("consec_fail") or 0)
    age = _stamp_age(st.get("last_poll_at"), tw._tz())
    if age is None:
        return "warn", "no poll yet"
    if fails >= 3:
        return "fail", f"{fails} polls failed in a row, last {_ago(age)} ago"
    # A slow read or a browser relaunch (~90 s boot) can delay one poll, so floor at 5 min.
    if age > max(3 * poll, 300):
        return "warn", f"last poll {_ago(age)} ago (every {poll}s)"
    return "ok", f"last poll {_ago(age)} ago, {st.get('polls') or 0} polls, {st.get('relaunches') or 0} relaunches"


def check_telegram():
    """Telegram Web warm session: authenticated and keepalive checks still running."""
    tg = sys.modules.get("telegramwarm")
    if tg is None or not tg._warm_enabled():
        return None, "disabled by TELEGRAM_WARM_ENABLED"
    w = tg._warm
    if w is not None and w._started and not _thread_alive("tg-warm"):
        return "fail", "worker thread tg-warm is dead"
    snap = tg.warm_snapshot()
    phase = str(snap.get("phase") or "?")
    if phase == "launching":
        return "warn", "browser launching"
    if phase != "authenticated":
        return "fail", f"session {phase} (needs /logintelegram?)"
    ka = tg._keepalive_sec()
    age = _stamp_age(snap.get("last_check"), tg._tz())
    if age is not None and age > 3 * ka:
        return "warn", f"logged in, last session check {_ago(age)} ago (every {_ago(ka)})"
    return "ok", "logged in" + (f", last check {_ago(age)} ago" if age is not None else "")


# ---------------------------------------------------------------- wiring


def _flag(fn) -> bool:
    try:
        return bool(fn())
    except Exception:
        return False


def _expected_threads() -> list[str]:
    """Long-lived threads this boot actually started (each depends on a flag or creds)."""
    names = []
    if _flag(lambda: _bot.scheduler.running):
        names.append("APScheduler")
    if _flag(_bot._lark_ws_uses_persistent_connection):
        names.append("larkbot-flask")  # started right after this hook in websocket mode
    mm = sys.modules.get("maintenance_mail")
    if _flag(lambda: mm._watcher_started):
        names.append("maintenance-mail-imap")
    if _flag(lambda: mm._allemail_scanner_started):
        names.append("allemail-cache-scan")
    if _flag(lambda: sys.modules["webapp"]._bg_started):
        names.append("webmachine-scrape")
    # The browser workers (teams-warm, tg-warm, osmwatch-warm) are checked by exact name in
    # their own rows; listed here, a prefix match would let a sibling thread stand in.
    if _flag(lambda: sys.modules["teamswatch"].warm_running()):
        names.append("teams-warm-poll")
    if _flag(lambda: sys.modules["telegramwarm"]._warm._started):
        names.append("tg-warm-ka")
    if _flag(lambda: sys.modules["osmwatch"]._warm_singleton._started):
        names.append("osmwatch-warm-ka")
    return names


def start(bot) -> bool:
    """Start the daily health report once per process. ``bot`` is the running main module."""
    global _bot, _started
    if _started:
        return False
    _started = True
    _bot = bot
    # Only the systemd service sends: health_report stays off in a PC run (run_local_bot.py,
    # python main.py) that uses a copy of the server .env, unless HEALTH_REPORT_ENABLE=1.
    checks = [
        ("Lark API", check_lark_api),
        ("Scheduler", check_scheduler),
        ("LLM (Ollama)", check_llm),
        ("Maintenance mail", check_mail),
        ("Machine scrape", check_machine_scrape),
        ("OSM-Watch", check_osmwatch),
        ("Teams watcher", check_teams),
        ("Telegram watcher", check_telegram),
    ]
    threads = _expected_threads()
    # health_report's sender, not main.send_message: a direct POST (never a quote-reply)
    # with a 15 s timeout and one Lark uuid per report, so Lark drops a retry of a send
    # that did arrive instead of posting the card twice.
    send = health_report.make_lark_sender(bot.APP_ID or "", bot.APP_SECRET or "", _LARK_BASE)
    ok = health_report.start("osedutybot", send_card=send, checks=checks, expect_threads=threads)
    if ok:
        print(
            f"[health] daily report scheduled ({len(checks)} checks, "
            f"{len(threads)} background threads watched)",
            flush=True,
        )
    else:
        print("[health] daily report not started here (not a systemd service, HEALTH_REPORT_ENABLE=0, "
              "or a start error)", flush=True)
    return ok
