#!/usr/bin/env python3
"""
Jenkins: open FPMS UAT branch update (with parameters), sign in, fill **Environment** then **Services**
(then **Branch** and **Version**) from **interactive terminal menus** or a pasted config block, then **re-read** the same fields on the
page and print **✅ / ❌** vs your choices. **Branch** and **Version** are saved with leading/trailing
spaces stripped (e.g. ``"  wad  "`` → ``wad``). In the terminal, type **yes** (only when every check
is ✅) to let the script click **Build**; **no** skips Build. Then **AFK** for ``--review-seconds``
(default **90** s) — **no further clicks** after that.

**Normal flow:** no post-login warm-up reload (``FPMS_WARMUP_RELOAD`` default **off**). After the
build page is ready, the script waits ``FPMS_MS_POST_LOGIN_BEFORE_FORM`` (default **3000** ms), then
fills the form (**Environment** ``select_option`` first so UnoChoice mounts Services for that env, then **Services** ticks). After each **Environment**
``select_option``, optional waits can stabilize UnoChoice:
``FPMS_ENV_POST_SELECT_SERVICES_MS`` (re-attach Services checkboxes), ``FPMS_ENV_POST_SELECT_NETWORKIDLE_MS``
(``networkidle``, off by default), ``FPMS_DEBUG_MS_BEFORE_ENV_SELECT`` (debug delay before select),
``FPMS_ENV_SELECT_FORCE``, ``FPMS_MS_ENV_SELECT_HOVER``.

**If Services cannot be filled** (detection / timeout), in the **same** browser session: open the
build-with-parameters URL (refresh), **re-login**, tick **Refresh pipeline**, click **Build**, wait
``FPMS_POST_BUILD_RECOVER_WAIT_MS`` (default **10000** ms), open the build URL again, **re-login**,
wait ``FPMS_MS_POST_LOGIN_BEFORE_FORM``, then **retry** Environment + Services + Branch + Version.
If that retry still fails → error (**Services 找不到**).

When UnoChoice **clears the Services list** after Environment or a tick but checkboxes still read
checked on the wider **Services** form row, the script can **skip long stabilize waits** and continue
(``FPMS_SERVICES_UI_EMPTY_OK``, default **on**; set **0** to restore stricter behavior).

Browser stays open after a successful fill + review until you press **Ctrl+C** in this terminal.

Use ``python3 updateJenkins.py --tick`` to only tick **Refresh pipeline** (no form fill, no Build).

Job URL: https://jenkins.client8.me/job/FPMS/job/FPMS_UAT_BRANCH_UPDATE/build?delay=0sec

Credentials: ``JENKINS_USERNAME`` / ``JENKINS_PASSWORD`` (recommended), else defaults below.

Usage::

  python3 updateJenkins.py
  python3 updateJenkins.py --review-seconds 120
  python3 updateJenkins.py --headless   # not useful for review
  python3 updateJenkins.py --tick   # only sign in + tick Refresh pipeline; no prompts, no Build, no other fields

**Persistent browser profile** (less “incognito-like” than the default ephemeral context)::

  python3 updateJenkins.py --user-data-dir ~/.fpms-playwright-profile
  # or:  FPMS_PLAYWRIGHT_USER_DATA_DIR=~/.fpms-playwright-profile python3 updateJenkins.py

**Fill speed** (default: **fastest** — short waits, Services quiet-waits skipped, aggressive service clicks)::

  # optional — slower, more conservative (longer FPMS_* from env, human-like services on by default):
  FPMS_STABLE_FILL=1 python3 updateJenkins.py

**Config block** (no interactive Environment / Services / Branch / Version menus)::

  python3 updateJenkins.py --paste-config
  # paste labeled lines, end with an empty line; or:
  python3 updateJenkins.py --config-file myparams.txt
  python3 updateJenkins.py --config-file - < myparams.txt

Block uses ``branch:`` (value → lowercased, trimmed), ``version:`` (trimmed, case kept),
``services:`` with comma-separated **ports** (``3000, 9000``) and/or **fuzzy names**
(``MGNT_API_server, mgnt_web``), or ``name,1,2`` to pick ranks from the fuzzy list without a prompt.
In an **interactive** terminal, a **text-only** token (no trailing ``1,2`` ranks) shows a **numbered**
near-match list — type ``1`` or ``1 2 3`` to choose. Set ``FPMS_CONFIG_SERVICE_TEXT_AUTO=1`` to keep
auto-pick top match on a TTY; non-TTY (e.g. stdin from file) always auto-picks.
A title line ``Update FPMS UAT2 Branch`` selects ``fpms-uat2-branch`` when ``environment:`` is omitted.
``Email (reply email):`` lines are ignored. See ``SERVICE_PORT_TO_ID`` for port numbers.

``--tick`` waits for the row help spinner (optional): ``FPMS_TICK_REFRESH_HELP_MS`` (default 22000),
and an extra settle before clicking: ``FPMS_TICK_REFRESH_SETTLE_MS`` (default 1200).
After ticking, the script **re-reads** the checkbox (``FPMS_TICK_VERIFY_MS``, default 900 ms settle).
With a visible browser, it keeps the window open ``FPMS_TICK_REVIEW_SEC`` seconds (default **5**;
use ``0`` to close immediately). Only **that** automation tab shows the tick — not another Jenkins tab.

After **Environment**, Services must appear within ``FPMS_SERVICES_APPEAR_MS`` (default **32000** ms).
If the list stays empty, the script **nudges** CascadeChoice by briefly selecting another Environment
then restoring yours (``FPMS_ENV_SERVICES_NUDGE_TRIES``, default **3**). If they appear, stability
waits up to ``FPMS_SERVICES_STABLE_MS`` (default **36000** ms).

**Services selection**: default ``FPMS_SERVICES_SELECT_MODE=sequential`` (one-by-one). Set ``auto`` to
try a **single JS batch** first, then sequential for leftovers; ``batch`` for batch-only.

By default each service tries **``Space`` on the checkbox** (``FPMS_SERVICES_SPACE_FIRST``) before any
mouse path — different event path than ``click()``. Optional **quiet** waits:
``FPMS_SKIP_SERVICES_QUIET=1`` disables them. Sequential picks use **human-like** mouse fallbacks
(``FPMS_HUMAN_LIKE_SERVICES=0`` for aggressive). Use ``--browser firefox`` if Chromium flakes persist.
"""
from __future__ import annotations

import argparse
import contextvars
import difflib
import functools
import json
import os
import re
import secrets
import sys
import threading
import time
from collections.abc import Sequence
from pathlib import Path

from playwright.sync_api import (
    Error as PlaywrightError,
    sync_playwright,
    TimeoutError as PlaywrightTimeout,
)

BUILD_URL = (
    "https://jenkins.client8.me/job/FPMS/job/FPMS_UAT_BRANCH_UPDATE/build?delay=0sec"
)
BI_API_UPDATE_BUILD_URL = (
    "https://jenkins.client8.me/job/BI-GO/job/BI-API-UPDATE/build?delay=0sec"
)

# Lark ``/jenkinsupdate``: keyword → (short title, build URL(s); multiple lines = several links).
JENKINS_UPDATE_JOB_REGISTRY: dict[str, tuple[str, str]] = {
    "fpms uat branch": ("FPMS UAT BRANCH UPDATE", BUILD_URL),
    "fpms prod script": (
        "FPMS PROD SCRIPT",
        "https://jenkins.client8.me/job/FPMS/job/FPMS_PROD_SCRIPT_RUN/",
    ),
    "frontend uat1 h5": (
        "FRONTEND UAT1 H5",
        "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-1/job/h5-uat/build?delay=0sec",
    ),
    "frontend uat2 h5": (
        "FRONTEND UAT2 H5",
        "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-2/job/h5-uat/build?delay=0sec",
    ),
    "frontend uat3 h5": (
        "FRONTEND UAT3 H5",
        "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-3/job/h5-uat/build?delay=0sec",
    ),
    "frontend uat4 h5": (
        "FRONTEND UAT4 H5",
        "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-4/job/h5-uat/build?delay=0sec",
    ),
    "frontend uat1 web": (
        "FRONTEND UAT1 WEB",
        "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-1/job/web-uat/build?delay=0sec",
    ),
    "frontend uat2 web": (
        "FRONTEND UAT2 WEB",
        "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-2/job/web-uat/build?delay=0sec",
    ),
    "frontend uat3 web": (
        "FRONTEND UAT3 WEB",
        "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-3/job/web-uat/build?delay=0sec",
    ),
    "frontend uat4 web": (
        "FRONTEND UAT4 WEB",
        "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-4/job/web-uat/build?delay=0sec",
    ),
    "fpms uat fgs": (
        "FPMS FGS",
        "https://jenkins.client8.me/job/FGS_CLIENT/job/FGS-UAT-UPDATE/build?delay=0sec",
    ),
    "ccms uat fe bo": (
        "FPMS_NT_UAT_BO_UPDATE",
        "https://jenkins.client8.me/job/FPMS_NT/view/all/job/FPMS_NT_UAT_BO_UPDATE/build?delay=0sec",
    ),
    "cpms uat update": (
        "CPMS-UAT-UPDATE",
        "https://jenkins.client8.me/job/CPMS/job/UAT/job/CPMS-UAT-UPDATE/build?delay=0sec",
    ),
    "igo uat script run": (
        "IGO UAT SCRIPT RUN",
        "https://jenkins.client8.me/job/IGO/job/UAT/job/IGO-UAT-SCRIPT-RUN/build?delay=0sec",
    ),
    "telesales": (
        "CRS UAT Master(telesales)",
        "https://jenkins.client8.me/job/FNT/job/TELESALES-UAT-UPDATE/build?delay=0sec",
    ),
    "rc uat master": (
        "FPMS FNT(RC)",
        "https://jenkins.client8.me/job/FNT/job/RC-UAT-UPDATE/build?delay=0sec",
    ),
    "fnt uat script run": (
        "FNT UAT SCRIPT RUN",
        "https://jenkins.client8.me/job/FNT/job/FNT_UAT_SCRIPT_RUN/build?delay=0sec",
    ),
    "sms uat update": (
        "SMS UAT UPDATE",
        "https://jenkins.client8.me/job/SMS/job/UAT/job/SMS-UAT-UPDATE/build?delay=0sec",
    ),
    "fpms nt uat branch": (
        "FPMS NT UAT BRANCH UPDATE",
        "https://jenkins.client8.me/job/FPMS_NT/view/all/job/FPMS_NT_UAT_BRANCH_UPDATE/build?delay=0sec",
    ),
    "fpms nt uat master": (
        "FPMS NT UAT MASTER UPDATE",
        "https://jenkins.client8.me/job/FPMS_NT/view/all/job/FPMS_NT_UAT_MASTER_UPDATE/build?delay=0sec",
    ),
    "fpms uat master": (
        "FPMS UAT MASTER UPDATE",
        "https://jenkins.client8.me/job/FPMS/view/FPMS-UAT/job/FPMS_UAT_MASTER_UPDATE/",
    ),
    "igo prod script": (
        "IGO PROD SCRIPT RUN",
        "https://jenkins.client8.me/job/IGO/job/PROD/job/IGO-PROD-SCRIPT-RUN/build?delay=0sec",
    ),
    "fpms prod script": (
        "FPMS PROD SCRIPT RUN",
        "https://jenkins.client8.me/job/FPMS/job/FPMS_PROD_SCRIPT_RUN/build?delay=0sec",
    ),
    "fpms prod script run": (
        "FPMS PROD SCRIPT RUN",
        "https://jenkins.client8.me/job/FPMS/job/FPMS_PROD_SCRIPT_RUN/build?delay=0sec",
    ),
}

JENKINS_UPDATE_CMD_RE = re.compile(r"/jenkinsupdate\b", re.I)
FPMS_PROD_SCRIPT_FLAG_RE = re.compile(r"--fpmsprodscript\b", re.I)
FPMS_PROD_SCRIPT_BUILD_URL = (
    "https://jenkins.client8.me/job/FPMS/job/FPMS_PROD_SCRIPT_RUN/build?delay=0sec"
)

# FNT ``RC-UAT-UPDATE`` (RC UAT master; alias ``rc uat master``) — checkbox ``value`` / ``json`` from Jenkins
# (ECP extended-choice parameter; order matches job UI).
FNT_RC_UAT_MASTER_SERVICES = [
    "backend-apiserver",
    "rc-apiserver",
    "rc-client",
    "risk-analysis-rollout",
    "risk-analysis-worker",
    "risk-analysis-worker-inactive-player-snapshot",
    "scheduler-depwith",
    "script-apiserver",
]

_FNT_RC_SERVICE_IDS_CASEFOLD = frozenset(s.casefold() for s in FNT_RC_UAT_MASTER_SERVICES)

# SMS ``SMS-UAT-UPDATE`` (alias ``sms uat update``) — same ECP **Services** widget as FNT RC jobs.
SMS_UAT_UPDATE_SERVICES = [
    "dlr-server",
    "email-scheduler",
    "scheduler-sms-aliyun",
    "scheduler-sms-all",
    "scheduler-sms-marketing",
    "scheduler-sms-marketing-aliyun",
    "scheduler-sms-marketingbalancer",
    "scheduler-sms-otpbalancer",
    "scheduler-sms-pldt",
    "scheduler-sms-smpp",
    "sms-api",
    "sms-cron",
    "sms-lark-ops-agent",
    "sms-public-api",
    "sms-web",
]

_SMS_UAT_SERVICE_IDS_CASEFOLD = frozenset(s.casefold() for s in SMS_UAT_UPDATE_SERVICES)

_DEFAULT_USER = "junchen"
_DEFAULT_PASSWORD = "junchen"

# After Services detect failure: same tab — goto build URL → login → Refresh pipeline → Build →
# wait FPMS_POST_BUILD_RECOVER_WAIT_MS → goto build URL → login → refill (same answers from prompts).
_MS_POST_BUILD_RECOVER_WAIT_MS = int(
    os.environ.get("FPMS_POST_BUILD_RECOVER_WAIT_MS", "10000")
)

# Shorter waits after the page is up (increase via env if UnoChoice flakes on slow networks).
_MS_AFTER_LOGIN = int(os.environ.get("FPMS_MS_AFTER_LOGIN", "2000"))
# After build-with-parameters page is up: wait before Environment/Services fill (post-login, no warm-up reload).
_MS_POST_LOGIN_BEFORE_FORM = int(os.environ.get("FPMS_MS_POST_LOGIN_BEFORE_FORM", "3000"))
_MS_POST_FILL_VERIFY = int(os.environ.get("FPMS_MS_POST_FILL_VERIFY", "600"))
_MS_ENV_SETTLE = int(os.environ.get("FPMS_MS_ENV_SETTLE", "200"))
_MS_AFTER_ENV_CASCADE = int(os.environ.get("FPMS_MS_AFTER_ENV_CASCADE", "650"))
# Environment ``select_option`` helpers (optional — reduce UnoChoice / Services flake).
_MS_DEBUG_BEFORE_ENV_SELECT = int(os.environ.get("FPMS_DEBUG_MS_BEFORE_ENV_SELECT", "0"))
_MS_ENV_POST_SELECT_NETWORKIDLE = int(
    os.environ.get("FPMS_ENV_POST_SELECT_NETWORKIDLE_MS", "0")
)
# 0 = skip ``wait_for`` on Services checkbox re-attach (avoids doubling wait with FPMS_SERVICES_APPEAR_MS).
_MS_ENV_POST_SELECT_SERVICES_WAIT = int(
    os.environ.get("FPMS_ENV_POST_SELECT_SERVICES_MS", "12000")
)
_MS_ENV_SELECT_HOVER = int(os.environ.get("FPMS_MS_ENV_SELECT_HOVER", "0"))
_ENV_SELECT_FORCE = os.environ.get("FPMS_ENV_SELECT_FORCE", "0").strip().lower() in (
    "1",
    "true",
    "yes",
)
_MS_FORM_READY = int(os.environ.get("FPMS_MS_FORM_READY", "400"))
_MS_SERVICES_PRE_STRIP = int(os.environ.get("FPMS_MS_SERVICES_PRE_STRIP", "400"))
_MS_BETWEEN_SERVICES = int(os.environ.get("FPMS_MS_BETWEEN_SERVICES", "900"))
_MS_SERVICES_TAIL = int(os.environ.get("FPMS_MS_SERVICES_TAIL", "150"))
# First selected service often triggers a heavier UnoChoice reflow than later picks.
_MS_BEFORE_FIRST_SERVICE = int(os.environ.get("FPMS_MS_BEFORE_FIRST_SERVICE", "750"))
_MS_AFTER_FIRST_SERVICE = int(os.environ.get("FPMS_MS_AFTER_FIRST_SERVICE", "1650"))
# After Environment: wait this long for the first Services checkbox; if still 0 → new browser session.
_MS_SERVICES_APPEAR = int(os.environ.get("FPMS_SERVICES_APPEAR_MS", "32000"))
# If Services stay empty, re-apply Environment: briefly select another branch then restore (CascadeChoice).
_ENV_SERVICES_NUDGE_TRIES = int(os.environ.get("FPMS_ENV_SERVICES_NUDGE_TRIES", "3"))
_MS_ENV_NUDGE_DWELL = int(os.environ.get("FPMS_MS_ENV_NUDGE_DWELL", "800"))
# After Services appeared: max time for checkbox count to stabilize before filling.
_MS_SERVICES_STABLE = int(os.environ.get("FPMS_SERVICES_STABLE_MS", "36000"))
# UnoChoice often clears the list for a few hundred ms; require this many consecutive zero-count polls
# before treating “gone” as real (avoids refresh during a normal refetch blink).
_SERVICES_GONE_POLLS = int(os.environ.get("FPMS_SERVICES_GONE_POLLS", "4"))
_SERVICES_GONE_POLL_MS = int(os.environ.get("FPMS_SERVICES_GONE_POLL_MS", "220"))
# After warm-up ``page.reload``, settle before continuing (re-login path uses a shorter default).
_MS_WARMUP_POST_RELOAD = int(os.environ.get("FPMS_MS_WARMUP_POST_RELOAD", "900"))
_MS_WARMUP_POST_RELOGIN = int(os.environ.get("FPMS_MS_WARMUP_POST_RELOGIN_MS", "650"))
# Post-login warm-up ``page.reload`` (off by default; set FPMS_WARMUP_RELOAD=1 to enable).
_WARMUP_RELOAD = os.environ.get("FPMS_WARMUP_RELOAD", "0").strip().lower() in ("1", "true", "yes", "")
# Service ticks: prefer label + non-forced clicks (closer to a human). Set to 0 for the older aggressive order.
_HUMAN_LIKE_SERVICE_CLICKS = os.environ.get("FPMS_HUMAN_LIKE_SERVICES", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "",
)
_MS_HUMAN_POINTER_SETTLE = int(os.environ.get("FPMS_MS_HUMAN_POINTER_SETTLE", "120"))
_MS_HUMAN_PRE_CLICK = int(os.environ.get("FPMS_MS_HUMAN_PRE_CLICK", "400"))
# Before each Services click: wait until count is steady + target exists (avoids UnoChoice mid-reflow).
_MS_PRE_SERVICE_CLICK = int(os.environ.get("FPMS_MS_PRE_SERVICE_CLICK", "200"))
_MS_SERVICES_QUIET_BEFORE_CLICK = int(os.environ.get("FPMS_MS_SERVICES_QUIET_BEFORE_CLICK", "16000"))
_SERVICES_QUIET_STREAK = int(os.environ.get("FPMS_SERVICES_QUIET_STREAK", "4"))
_SERVICES_QUIET_POLL_MS = int(os.environ.get("FPMS_SERVICES_QUIET_POLL_MS", "150"))
# After a service is ticked: wait until the list is non-empty and steady again before the next tick.
_MS_AFTER_PICK_STABLE = int(os.environ.get("FPMS_MS_AFTER_PICK_STABLE", "14000"))
_SERVICES_AFTER_PICK_STREAK = int(os.environ.get("FPMS_SERVICES_AFTER_PICK_STREAK", "3"))
_SERVICES_AFTER_PICK_POLL_MS = int(os.environ.get("FPMS_SERVICES_AFTER_PICK_POLL_MS", "180"))
_SKIP_SERVICES_QUIET = os.environ.get("FPMS_SKIP_SERVICES_QUIET", "0").strip().lower() in (
    "1",
    "true",
    "yes",
)
# Services: ``sequential`` (default) = one service at a time; ``auto`` = JS batch first then sequential
# for stragglers; ``batch`` = batch only. (Batch-first was flaky for some UnoChoice builds.)
_SERVICES_SELECT_MODE = os.environ.get("FPMS_SERVICES_SELECT_MODE", "sequential").strip().lower()
_MS_BATCH_POST_APPLY = int(os.environ.get("FPMS_MS_BATCH_POST_APPLY", "550"))
# --tick: after action, wait this long then re-read ``checked`` (catches UnoChoice / React resetting).
_MS_TICK_VERIFY_SETTLE = int(os.environ.get("FPMS_TICK_VERIFY_MS", "900"))
# --tick headed: keep browser open this many seconds (env FPMS_TICK_REVIEW_SEC overrides; empty = default).
_TICK_REVIEW_SEC_HEADED_DEFAULT = float(
    os.environ.get("FPMS_TICK_REVIEW_SEC_HEADED_DEFAULT", "5")
)
# Before mouse paths, try ``Space`` on the focused checkbox (keyboard semantics, not pointer events).
_SERVICES_SPACE_FIRST = os.environ.get("FPMS_SERVICES_SPACE_FIRST", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "",
)
# If Services checkboxes vanish mid-flow (UnoChoice reflow) but a **wide** read of the Services row
# still shows every requested value checked, skip long ``…STABLE`` / ``…QUIET`` waits (set **0** for old strict behavior).
_SERVICES_UI_EMPTY_OK = os.environ.get("FPMS_SERVICES_UI_EMPTY_OK", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)


def _jenkins_update_all_stapler_name() -> str:
    """
    Jenkins hidden field ``input[name="name"][value=…]`` for the “update all services” checkbox.

    Override when your job uses a different parameter id (all jobs in this script default to the same).
    Env: ``JENKINS_UPDATE_ALL_STAPLER_NAME`` (default ``Update_All_Services``).
    """
    n = (os.environ.get("JENKINS_UPDATE_ALL_STAPLER_NAME") or "").strip()
    return n if n else "Update_All_Services"


def _service_lines_mean_update_all(service_lines: list[str]) -> bool:
    """
    True when the ``services:`` section is **only** a single keyword meaning “tick update-all / every service”
    (e.g. ``services: all`` or ``services:\\n  all``), not a service name.
    """
    toks: list[str] = []
    for raw in service_lines or []:
        line = _normalize_config_colons(raw).strip()
        if not line or line.lstrip().startswith("#"):
            continue
        for part in re.split(r"[,，;]+", line):
            t = part.strip()
            if t:
                toks.append(t)
    if len(toks) != 1:
        return False
    return toks[0].casefold() in ("all", "*", "every", "全部", "__all__")


# Default: apply ``_ensure_fast_fill_mode`` at import unless ``FPMS_STABLE_FILL=1`` (conservative pacing).
_FPMS_FAST_FILL_NOTE_SHOWN = False
# When true: aggressive service ticks + shorter post-click settles (set only by ``_ensure_fast_fill_mode``).
_FPMS_FAST_FILL_ACTIVE = False


def _truthy_stable_fill_env() -> bool:
    """If set, skip default fast caps (use env defaults / ``FPMS_HUMAN_LIKE_SERVICES`` as configured)."""
    return os.environ.get("FPMS_STABLE_FILL", "0").strip().lower() in ("1", "true", "yes")


def _ensure_fast_fill_mode(*, announce: bool = True) -> None:
    """
    Cap the slowest Jenkins/UnoChoice waits and enable ``FPMS_SKIP_SERVICES_QUIET``-equivalent behavior
    so Environment / Services / Branch fill finishes sooner.

    Also forces **aggressive** service selection (same effect as ``FPMS_HUMAN_LIKE_SERVICES=0`` for this
    process) and shortens per-service post-click settles. Idempotent (safe to call repeatedly).

    **Default** in this script: applied at import (unless ``FPMS_STABLE_FILL=1``). ``announce`` controls
    whether the one-line CLI notice is printed (import uses ``announce=False`` to avoid noise on ``import``).
    """
    global _MS_POST_LOGIN_BEFORE_FORM, _MS_SERVICES_APPEAR, _MS_SERVICES_STABLE
    global _MS_ENV_POST_SELECT_SERVICES_WAIT, _MS_SERVICES_QUIET_BEFORE_CLICK, _MS_AFTER_PICK_STABLE
    global _MS_BETWEEN_SERVICES, _MS_BEFORE_FIRST_SERVICE, _MS_AFTER_FIRST_SERVICE
    global _MS_SERVICES_PRE_STRIP, _MS_ENV_SETTLE, _MS_AFTER_ENV_CASCADE, _MS_POST_FILL_VERIFY
    global _MS_FORM_READY, _SKIP_SERVICES_QUIET, _MS_ENV_NUDGE_DWELL, _MS_AFTER_LOGIN
    global _FPMS_FAST_FILL_NOTE_SHOWN
    global _FPMS_FAST_FILL_ACTIVE, _HUMAN_LIKE_SERVICE_CLICKS
    global _MS_HUMAN_PRE_CLICK, _MS_HUMAN_POINTER_SETTLE, _MS_PRE_SERVICE_CLICK, _MS_BATCH_POST_APPLY
    global _MS_SERVICES_TAIL

    _FPMS_FAST_FILL_ACTIVE = True
    _HUMAN_LIKE_SERVICE_CLICKS = False
    _MS_HUMAN_PRE_CLICK = min(_MS_HUMAN_PRE_CLICK, 30)
    _MS_HUMAN_POINTER_SETTLE = min(_MS_HUMAN_POINTER_SETTLE, 30)
    _MS_PRE_SERVICE_CLICK = min(_MS_PRE_SERVICE_CLICK, 55)
    _MS_BATCH_POST_APPLY = min(_MS_BATCH_POST_APPLY, 220)

    _MS_POST_LOGIN_BEFORE_FORM = min(_MS_POST_LOGIN_BEFORE_FORM, 700)
    _MS_AFTER_LOGIN = min(_MS_AFTER_LOGIN, 900)
    _MS_SERVICES_APPEAR = min(_MS_SERVICES_APPEAR, 10_000)
    _MS_SERVICES_STABLE = min(_MS_SERVICES_STABLE, 10_000)
    _MS_ENV_POST_SELECT_SERVICES_WAIT = min(_MS_ENV_POST_SELECT_SERVICES_WAIT, 3_000)
    _MS_SERVICES_QUIET_BEFORE_CLICK = min(_MS_SERVICES_QUIET_BEFORE_CLICK, 2_500)
    _MS_AFTER_PICK_STABLE = min(_MS_AFTER_PICK_STABLE, 4_000)
    _MS_BETWEEN_SERVICES = min(_MS_BETWEEN_SERVICES, 80)
    _MS_BEFORE_FIRST_SERVICE = min(_MS_BEFORE_FIRST_SERVICE, 120)
    _MS_AFTER_FIRST_SERVICE = min(_MS_AFTER_FIRST_SERVICE, 200)
    _MS_SERVICES_PRE_STRIP = min(_MS_SERVICES_PRE_STRIP, 120)
    _MS_ENV_SETTLE = min(_MS_ENV_SETTLE, 120)
    _MS_AFTER_ENV_CASCADE = min(_MS_AFTER_ENV_CASCADE, 280)
    _MS_POST_FILL_VERIFY = min(_MS_POST_FILL_VERIFY, 220)
    _MS_FORM_READY = min(_MS_FORM_READY, 180)
    _MS_ENV_NUDGE_DWELL = min(_MS_ENV_NUDGE_DWELL, 400)
    _MS_SERVICES_TAIL = min(_MS_SERVICES_TAIL, 80)
    _SKIP_SERVICES_QUIET = True
    if announce and not _FPMS_FAST_FILL_NOTE_SHOWN:
        _FPMS_FAST_FILL_NOTE_SHOWN = True
        print(
            "→ **Default: fastest fill** — capped waits, Services quiet-waits skipped, aggressive service clicks. "
            "Set ``FPMS_STABLE_FILL=1`` for slower human-like pacing / env defaults; tune ``FPMS_*_MS`` as needed.\n"
            "  中文：默认已用最快速度填表（短等待、激进勾选服务）。若要更稳、更慢：``FPMS_STABLE_FILL=1``；"
            "或单独调大 ``FPMS_*``。",
            flush=True,
        )


if not _truthy_stable_fill_env():
    _ensure_fast_fill_mode(announce=False)


class ServiceNotDetectedError(Exception):
    """A requested service checkbox was not found or could not be checked (``run()`` may retry in a new browser)."""


class ServicesListGoneError(ServiceNotDetectedError):
    """Services missing or cleared; ``run()`` closes the browser and starts a new session, then refills (no re-prompts)."""


ENVIRONMENTS = [
    "fpms-uat-branch",
    "fpms-uat2-branch",
    "fpms-uat3-branch",
    "fpms-uat4-branch",
    "fpms-uat5-branch",
    "fpms-uat-master",
    "fpms-uat2-master",
    "fpms-uat3-master",
    "fpms-nt-uat-master",
    "fpms-nt-uat2-master",
    "fpms-nt-uat3-master",
]

# Checkbox ``value`` / label text for **FPMS UAT branch update** job only (same order as Jenkins UI).
FPMS_UAT_BRANCH_SERVICES = [
    "check-rest-server",
    "client-apiserver",
    "exrestful-apiserver",
    "external-sms-mission",
    "fg-exrestful-apiserver",
    "fpmsinternal-rest",
    "geoip-apiserver",
    "jackpot-server",
    "kycapi-apiserver",
    "lazada-restserver",
    "livechat-apiserver",
    "maya-restserver",
    "message-server",
    "mgnt-apiserver",
    "mgnt-newskin-webserver",
    "mgnt-webserver",
    "micro-fe-fpms",
    "pagcor-rest-apiserver",
    "provider-apiserver",
    "restful-apiserver",
    "schedule-server",
    "schedule-server2",
    "schedule-serverviber",
    "script-apiserver",
    "settlement-report",
    "settlement-schedule",
    "settlement-server",
    # FPMS_NT_UAT_MASTER_UPDATE services:
    "admin-rollout",
    "auth-rollout",
    "card-rollout",
    "ccms-rollout",
    "ccms-rust-rollout",
    "ccms-scheduler",
    "fpms-internal-rollout",
    "live-draw-event-scheduler",
    "livechat-rest-rollout",
    "livechat-rollout",
    "livechat-scheduler",
    "packet-consumer",
    "packet-main-probability-rollout",
    "packet-rollout",
    "payment-rollout",
    "player-rollout",
    "player-scheduler",
    "promotion-rollout",
    "promotion-scheduler",
    "proposal-rollout",
    "provider-rollout",
    "push-rollout",
    "recommend-rollout",
    "recommend-scheduler",
    "risk-control-rollout",
    "rulex-rollout",
    "social-engagement-rollout",
    "user-engagement-rollout",
    "user-engagement-scheduler",
]

# Backward-compatible name (prefer ``FPMS_UAT_BRANCH_SERVICES`` in new code).
SERVICES = FPMS_UAT_BRANCH_SERVICES

_FPMS_SERVICE_IDS_CASEFOLD = frozenset(s.casefold() for s in FPMS_UAT_BRANCH_SERVICES)


def _normalize_service_query_key(tok: str) -> str:
    """Lowercase + unify hyphens/underscores for matching user paste to Jenkins ``value``."""
    t = (tok or "").strip()
    t = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", t)
    t = t.replace("_", "-")
    return t.casefold()


def _fnt_rc_canonical_service_id(tok: str) -> str | None:
    """Return catalog id if ``tok`` matches an FNT RC service (exact after normalize)."""
    k = _normalize_service_query_key(tok)
    for s in FNT_RC_UAT_MASTER_SERVICES:
        if _normalize_service_query_key(s) == k:
            return s
    return None


def _fpms_lark_is_fnt_rc_only_service_token(tok: str) -> bool:
    """
    True if ``tok`` names an FNT RC service that is **not** on the FPMS UAT Services list.

    Used to stop FPMS flows from fuzzy-matching ``rc-client`` → ``client-apiserver``.
    """
    k = _normalize_service_query_key(tok)
    if k not in _FNT_RC_SERVICE_IDS_CASEFOLD:
        return False
    return k not in _FPMS_SERVICE_IDS_CASEFOLD


def _sms_uat_canonical_service_id(tok: str) -> str | None:
    """Return catalog id if ``tok`` matches an SMS UAT update service (exact after normalize)."""
    k = _normalize_service_query_key(tok)
    for s in SMS_UAT_UPDATE_SERVICES:
        if _normalize_service_query_key(s) == k:
            return s
    return None


def _fpms_lark_is_sms_uat_only_service_token(tok: str) -> bool:
    """True if ``tok`` is an SMS UAT service id but **not** on the FPMS UAT Services list."""
    k = _normalize_service_query_key(tok)
    if k not in _SMS_UAT_SERVICE_IDS_CASEFOLD:
        return False
    return k not in _FPMS_SERVICE_IDS_CASEFOLD


def _service_search_score(query: str, service: str) -> float:
    """Higher = more similar (substring boost + difflib on full name and hyphen tokens)."""
    q = query.strip().casefold()
    if not q:
        return 0.0
    n = service.casefold()
    if q in n:
        return 2.0 + 10.0 / (1.0 + float(n.index(q)))
    best = difflib.SequenceMatcher(None, q, n).ratio()
    for tok in re.split(r"[-_]+", service):
        t = tok.casefold()
        if not t:
            continue
        best = max(best, difflib.SequenceMatcher(None, q, t).ratio())
    return best


def _rank_services_by_query(query: str, limit: int = 12, *, for_menu: bool = False) -> list[str]:
    """
    Return up to ``limit`` service names, best fuzzy match first.

    ``for_menu=True`` (numbered pick lists): keep the list **short** — substring hits on the full
    query first, then only services whose score stays in a tight band vs the best match (avoids
    unrelated names that barely pass the loose 0.32 floor).
    """
    q_raw = (query or "").strip()
    q = q_raw.casefold()
    if not q:
        return []

    scored = [(_service_search_score(q_raw, s), s) for s in FPMS_UAT_BRANCH_SERVICES]
    scored.sort(key=lambda x: (-x[0], x[1]))
    if not scored:
        return []

    if not for_menu:
        floor = 0.32
        strong = [s for sc, s in scored if sc >= floor][:limit]
        if strong:
            return strong
        return [s for _, s in scored[:limit]]

    cap = min(limit, 10)
    best_sc, _ = scored[0]
    out: list[str] = []
    seen: set[str] = set()

    for sc, s in scored:
        if s in seen:
            continue
        if len(out) >= cap:
            break
        if q in s.casefold():
            seen.add(s)
            out.append(s)

    for sc, s in scored:
        if s in seen:
            continue
        if len(out) >= cap:
            break
        # ``best_sc`` can be large when the full query is a substring of one service (e.g. 12+).
        # Do not scale ``need`` with that magnitude or only the substring hit would pass.
        if best_sc > 5.0:
            need = 0.70
        elif best_sc >= 1.35:
            need = max(0.58, min(0.82, best_sc * 0.28))
        else:
            need = max(0.46, best_sc - 0.11)
        if sc < need:
            continue
        seen.add(s)
        out.append(s)

    return out if out else [scored[0][1]]


def _rank_catalog_services_by_query(
    catalog: Sequence[str],
    query: str,
    limit: int = 12,
    *,
    for_menu: bool = False,
) -> list[str]:
    """Fuzzy rank against a fixed Jenkins checkbox id list (ECP / extended-choice jobs)."""
    q_raw = (query or "").strip()
    q = q_raw.casefold()
    qk = _normalize_service_query_key(q_raw)
    if not q:
        return []
    exact_first = [s for s in catalog if _normalize_service_query_key(s) == qk]
    scored = [(_service_search_score(q_raw, s), s) for s in catalog]
    scored.sort(key=lambda x: (-x[0], x[1]))
    if not scored:
        return []
    if not for_menu:
        if exact_first:
            rest = [s for s in [x[1] for x in scored] if s not in exact_first][: max(0, limit - len(exact_first))]
            return (exact_first + rest)[:limit]
        floor = 0.32
        strong = [s for sc, s in scored if sc >= floor][:limit]
        if strong:
            return strong
        return [s for _, s in scored[:limit]]
    cap = min(limit, 10)
    best_sc, _ = scored[0]
    out: list[str] = []
    seen: set[str] = set()
    for s in exact_first:
        if s not in seen:
            seen.add(s)
            out.append(s)
    for sc, s in scored:
        if s in seen:
            continue
        if len(out) >= cap:
            break
        if q in s.casefold():
            seen.add(s)
            out.append(s)
    for sc, s in scored:
        if s in seen:
            continue
        if len(out) >= cap:
            break
        if best_sc > 5.0:
            need = 0.70
        elif best_sc >= 1.35:
            need = max(0.58, min(0.82, best_sc * 0.28))
        else:
            need = max(0.46, best_sc - 0.11)
        if sc < need:
            continue
        seen.add(s)
        out.append(s)
    return out if out else [scored[0][1]]


def _rank_fnt_rc_services_by_query(
    query: str, limit: int = 12, *, for_menu: bool = False
) -> list[str]:
    """Like ``_rank_services_by_query`` but against ``FNT_RC_UAT_MASTER_SERVICES``."""
    return _rank_catalog_services_by_query(
        FNT_RC_UAT_MASTER_SERVICES, query, limit, for_menu=for_menu
    )


def _rank_sms_uat_services_by_query(
    query: str, limit: int = 12, *, for_menu: bool = False
) -> list[str]:
    """Like ``_rank_services_by_query`` but against ``SMS_UAT_UPDATE_SERVICES``."""
    return _rank_catalog_services_by_query(SMS_UAT_UPDATE_SERVICES, query, limit, for_menu=for_menu)


# Deploy / listener port → Jenkins Services checkbox ``value`` (same strings as ``FPMS_UAT_BRANCH_SERVICES``).
# Use ``python3 updateJenkins.py --paste-config`` (or ``--config-file``) with lines like ``7300 - fg_exrestful``.
# ``8000`` maps to ``check-rest-server`` (alias ``check-server-status``). ``9998`` → ``schedule-server2``.
SERVICE_PORT_TO_ID: dict[int, str] = {
    3000: "mgnt-webserver",
    6000: "kycapi-apiserver",
    6001: "jackpot-server",
    7000: "exrestful-apiserver",
    7100: "restful-apiserver",
    7300: "fg-exrestful-apiserver",
    7400: "pagcor-rest-apiserver",
    7600: "external-sms-mission",
    7700: "fpmsinternal-rest",
    7800: "lazada-restserver",
    7900: "maya-restserver",
    8000: "check-rest-server",
    8001: "settlement-server",
    8002: "settlement-report",
    8003: "settlement-schedule",
    9000: "mgnt-apiserver",
    9280: "client-apiserver",
    9380: "provider-apiserver",
    9580: "message-server",
    9997: "schedule-serverviber",
    9998: "schedule-server2",
    9999: "schedule-server",
}
for _port, _svc in SERVICE_PORT_TO_ID.items():
    if _svc not in FPMS_UAT_BRANCH_SERVICES:
        raise RuntimeError(
            f"SERVICE_PORT_TO_ID port {_port} maps to {_svc!r} which is not in FPMS_UAT_BRANCH_SERVICES — fix the table."
        )


class ConfigBlockError(ValueError):
    """Invalid ``--paste-config`` / ``--config-file`` block (branch, version, ports, environment)."""


def _normalize_config_colons(s: str) -> str:
    """ASCII colon + trim; map fullwidth colon and strip common list-bullet prefixes."""
    t = (s or "").replace("\uff1a", ":").replace("\u200b", "").strip()
    # Lark/IM often injects bullet prefixes (e.g. • · 🔹 - >) before key lines.
    # Strip them early so ``Branch:`` / ``Version:`` / ``Services:`` still match.
    t = re.sub(
        r"^\s*(?:[-*>]|[•·\u2022\u00b7\u30fb\u25cf\u25cb\u25aa\u25ab]|[🔹🔸🔵])+[\s\u00a0]*",
        "",
        t,
    )
    return t


def _branch_from_config_block(raw: str) -> str:
    """Strip + lowercase (Jenkins branch field; user asked no outer spaces, lowercase)."""
    return (raw or "").strip().lower()


def _version_from_config_block(raw: str) -> str:
    """Strip only; preserve inner case (e.g. ``3.2.128g``)."""
    return normalize_parameter_text(raw)


def _resolve_environment_token(raw: str) -> str:
    """Match ``ENVIRONMENTS`` by exact id, index 1–5, or case-insensitive substring."""
    t = normalize_parameter_text(raw)
    if not t:
        raise ConfigBlockError("environment: value is empty.")
    if t in ENVIRONMENTS:
        return t
    if t.isdigit():
        i = int(t)
        if 1 <= i <= len(ENVIRONMENTS):
            return ENVIRONMENTS[i - 1]
    low = t.casefold()
    for e in ENVIRONMENTS:
        if e.casefold() == low:
            return e
    hits = [e for e in ENVIRONMENTS if low in e.casefold() or e.casefold() in low]
    if len(hits) == 1:
        return hits[0]
    if not hits:
        raise ConfigBlockError(
            f"Unknown environment {raw!r}. Use one of: {', '.join(ENVIRONMENTS)} "
            f"or a number 1–{len(ENVIRONMENTS)} (same order as the interactive menu)."
        )
    raise ConfigBlockError(
        f"Ambiguous environment {raw!r}; matches: {', '.join(hits)}. "
        "Use the full id (e.g. fpms-uat-branch)."
    )


def _environment_hint_from_banner(line: str) -> str | None:
    """
    Map a title like ``Update FPMS UAT2 Branch`` → ``fpms-uat2-branch`` when ``environment:`` is omitted.

    Checks ``UAT5`` … ``UAT2`` before plain ``UAT`` so ``UAT2`` is not swallowed as ``UAT``.
    """
    s = line.casefold().replace("_", " ")
    # FPMS NT UAT MASTER jobs (same fill flow, different env values).
    if re.search(r"\bfpms[\s-]*nt[\s-]*uat[\s-]*master\b", s):
        return "fpms-nt-uat-master"
    # FPMS UAT MASTER jobs.
    if re.search(r"\bfpms[\s-]*uat[\s-]*master\b", s):
        return "fpms-uat-master"
    if re.search(r"\bfpms[\s-]*uat\s*5\b", s) or re.search(r"\buat\s*5\b", s):
        return "fpms-uat5-branch"
    if re.search(r"\bfpms[\s-]*uat\s*4\b", s) or re.search(r"\buat\s*4\b", s):
        return "fpms-uat4-branch"
    if re.search(r"\bfpms[\s-]*uat\s*3\b", s) or re.search(r"\buat\s*3\b", s):
        return "fpms-uat3-branch"
    if re.search(r"\bfpms[\s-]*uat\s*2\b", s) or re.search(r"\buat\s*2\b", s) or "uat2" in s.replace(
        " ", ""
    ):
        return "fpms-uat2-branch"
    if re.search(r"\bfpms[\s-]*uat\b", s) or re.search(r"\buat\b", s):
        return "fpms-uat-branch"
    return None


def _service_ids_from_service_block_lines(lines: list[str]) -> list[str]:
    """
    ``services:`` payload: deploy ports (``3000``), fuzzy names (``MGNT_API_server``, ``mgnt_web``),
    or ``name,1,2`` where ``1``/``2`` pick 1-based rows from the fuzzy rank list for ``name``.

    If stdin/stdout are a **TTY** and a text token has **no** trailing rank numbers, the user is shown
    a numbered near-match list and must pick ``1`` / ``1 2 3`` (unless ``FPMS_CONFIG_SERVICE_TEXT_AUTO=1``).
    """
    port_tokens = re.compile(r"\b(\d{3,5})\b")
    seen: set[str] = set()
    out: list[str] = []

    def _consume_one_line(raw_line: str) -> None:
        line = _normalize_config_colons(raw_line).strip()
        if not line or line.startswith("#"):
            return
        if not port_tokens.search(line) and not re.search(r"[a-zA-Z_]", line):
            raise ConfigBlockError(
                f"Service line has no port and no letters: {raw_line!r}\n"
                "EN: use `3000`, `mgnt-apiserver`, or `MGNT_API_server, mgnt_web`."
            )
        toks = [t.strip() for t in re.split(r"[,，;]+", line) if t.strip()]
        i = 0
        while i < len(toks):
            tok = toks[i]
            if re.fullmatch(r"\d{3,5}", tok):
                port = int(tok)
                sid = SERVICE_PORT_TO_ID.get(port)
                if sid is None:
                    raise ConfigBlockError(
                        f"Unknown port {tok}; known: "
                        f"{', '.join(str(p) for p in sorted(SERVICE_PORT_TO_ID))}"
                    )
                if sid not in seen:
                    seen.add(sid)
                    out.append(sid)
                i += 1
                continue
            if re.fullmatch(r"\d{1,2}", tok):
                raise ConfigBlockError(
                    f"Token {tok!r} looks like a rank but it must follow a **name** token "
                    "in the same comma-separated list (e.g. ``MGNT_API,1,2``)."
                )
            q = tok.replace("_", "-")
            ranked = _rank_services_by_query(q, limit=min(30, len(FPMS_UAT_BRANCH_SERVICES)))
            if not ranked:
                raise ConfigBlockError(f"No Jenkins service matches token {tok!r}.")
            j = i + 1
            rank_picks: list[int] = []
            while j < len(toks) and re.fullmatch(r"\d{1,2}", toks[j].strip()):
                rank_picks.append(int(toks[j].strip()))
                j += 1
            if rank_picks:
                for ri in rank_picks:
                    if ri < 1 or ri > len(ranked):
                        raise ConfigBlockError(
                            f"Rank {ri} out of range for token {tok!r} (1–{len(ranked)})."
                        )
                    sid = ranked[ri - 1]
                    if sid not in seen:
                        seen.add(sid)
                        out.append(sid)
                i = j
                continue

            if _stdin_stdout_interactive() and not _config_text_service_auto_pick():
                ranked_menu = _rank_services_by_query(q, limit=12, for_menu=True)
                picked = _prompt_service_ids_for_config_text_token(tok, ranked_menu, seen)
                for sid in picked:
                    if sid in seen:
                        print(f"  (Skip — already in list: {sid})", flush=True)
                        continue
                    seen.add(sid)
                    out.append(sid)
                i += 1
                continue

            top = ranked[0]
            sc0 = _service_search_score(q, top)
            sc1 = _service_search_score(q, ranked[1]) if len(ranked) > 1 else -1.0
            if sc0 < 0.26:
                shown = "\n".join(f"    {k}. {n}" for k, n in enumerate(ranked[:8], start=1))
                raise ConfigBlockError(
                    f"Service token {tok!r} is too vague (best score={sc0:.2f}). "
                    f"Use a port, exact id, rank picks ``name,1,2``, or run from a TTY for an interactive menu:\n{shown}"
                )
            if len(ranked) > 1 and (sc0 - sc1) < 0.07 and sc0 < 0.52:
                shown = "\n".join(f"    {k}. {n}" for k, n in enumerate(ranked[:8], start=1))
                raise ConfigBlockError(
                    f"Ambiguous service token {tok!r}; tie between top matches. "
                    f"Use ``{tok},1`` / ``{tok},1,2`` to pick by rank, exact checkbox id, or run from a TTY for a menu:\n{shown}"
                )
            if top not in seen:
                seen.add(top)
                out.append(top)
            i += 1

    for raw in lines:
        _consume_one_line(raw)
    if not out:
        raise ConfigBlockError(
            "No services resolved — add ports (e.g. 3000) and/or names under services:."
        )
    return out


_KEY_LINE_RE = re.compile(
    r"^(?:[>\-\*\u2022]\s*)*"
    r"(?:`+|\*{1,2})?(?P<key>environment|branch|version|services?)(?:`+|\*{1,2})?"
    r"\s*:\s*(?P<rest>.*)$",
    re.IGNORECASE,
)


def _match_key_line_fuzzy(line: str):
    """
    Parse key lines robustly even with rich-text wrappers/bullets, e.g.:
      **Branch:** master
      1. • `Version` : v1.2.3
      🔹 Services - risk-analysis-rollout
    """
    s = _normalize_config_colons(line)
    s = re.sub(r"^\s*\d+\s*[.)]\s*", "", s)
    m = _KEY_LINE_RE.match(s)
    if m:
        return m
    # Strip markdown wrappers and retry with flexible separators (: / - / en/em dash)
    plain = re.sub(r"[`*_]", "", s).strip()
    return re.match(
        r"^(?P<key>environment|branch|version|services?)\s*[:\-–—]\s*(?P<rest>.*)$",
        plain,
        re.IGNORECASE,
    )


def _clean_key_rest(rest: str) -> str:
    t = (rest or "").strip()
    t = re.sub(r"^\s*[:\-–—]+\s*", "", t)
    t = re.sub(r"^(?:`+|\*{1,2})+\s*", "", t)
    t = re.sub(r"\s*(?:`+|\*{1,2})+$", "", t)
    return t.strip()


def parse_fpms_config_block(text: str) -> tuple[str, list[str], str, str, bool]:
    """
    Parse a pasted block (``branch:``, ``version:``, ``Service(s):``, ``environment:``).

    Keys may appear in **any order**. Preamble lines (titles, ``Email (reply email):``, etc.)
    before the first ``branch:`` / ``version:`` / … are skipped. A title like
    ``Update FPMS UAT2 Branch`` sets **environment** to ``fpms-uat2-branch`` when ``environment:``
    is omitted.

    If you paste **two jobs**, only the **first complete** job is used (branch + version +
    at least one ``services:`` payload line).

    * **branch** — stripped, lowercased.
    * **version** — stripped only (case preserved).
    * **services** — comma-separated **ports** (``3000``), **fuzzy names** (``MGNT_API_server``), or
      ``name,1,2`` to pick ranks without a menu. On a **TTY**, a bare fuzzy **name** opens a numbered
      near-match list (type ``1`` / ``1 2 3``); ``FPMS_CONFIG_SERVICE_TEXT_AUTO=1`` forces auto top match.
      Use **only** ``all`` (or ``*``, ``every``, ``全部``) under ``services:`` to mean **Update_All_Services**
      (same as ``--allservice``); returned service list is empty and the fifth tuple value is ``True``.
    * **environment** — optional; else from banner ``UAT`` / ``UAT2`` …, else ``FPMS_DEFAULT_ENVIRONMENT``
      or ``fpms-uat-branch``.

    Returns ``(environment, services, branch, version, update_all_services)``.
    """
    raw_lines = [_normalize_config_colons(L) for L in (text or "").splitlines()]
    lines = [L.strip() for L in raw_lines if L.strip() != ""]
    if not lines:
        raise ConfigBlockError("Config block is empty.")

    env: str | None = None
    env_from_banner: str | None = None
    branch: str | None = None
    version: str | None = None
    service_lines: list[str] = []
    last_key: str | None = None
    port_head = re.compile(r"^\d{3,5}\b")

    def _first_job_complete() -> bool:
        return (
            branch is not None
            and version is not None
            and len(service_lines) > 0
        )

    for line in lines:
        m = _match_key_line_fuzzy(line)
        if m and _first_job_complete():
            print(
                "→ First job already has branch, version, and service line(s); "
                f"ignoring the rest of the paste (next key line was: {line!r}).\n"
                "  中文：同一次粘贴里若有多段任务，只采用**最先凑齐** branch + version + Service 的那一段。\n"
                "  EN: Use one job per paste, or keep only the first block before a second header.",
                flush=True,
            )
            break
        if m:
            key = m.group("key").lower()
            if key == "service":
                key = "services"
            rest = _clean_key_rest(m.group("rest") or "")
            last_key = key

            if key == "environment":
                env = _resolve_environment_token(rest)
            elif key == "branch":
                branch = _branch_from_config_block(rest)
                if not branch:
                    raise ConfigBlockError("branch: is empty after trim.")
            elif key == "version":
                version = _version_from_config_block(rest)
                if not version:
                    raise ConfigBlockError("version: is empty after trim.")
            elif key == "services":
                if rest:
                    service_lines.append(rest)
            else:
                raise ConfigBlockError(f"Unknown key in line: {line!r}")
            continue

        if last_key == "services":
            if port_head.match(line):
                service_lines.append(line)
            elif (
                re.search(r"[a-zA-Z_]", line)
                and len(line) < 200
                and not re.match(r"^\s*update\b", line, re.I)
            ):
                service_lines.append(line)
            elif re.match(r"^\s*update\b", line, re.I):
                print(f"→ Skipping title-like line under services: {line!r}", flush=True)
            elif line.lstrip().startswith("#"):
                continue
            else:
                print(
                    f"→ Skipping non-service line under services: (ignored) {line!r}",
                    flush=True,
                )
            continue

        if last_key is None:
            if re.match(r"^\s*email\b", line, re.I):
                print(f"→ Skipping email line: {line!r}", flush=True)
                continue
            hint = _environment_hint_from_banner(line)
            if hint and env_from_banner is None:
                env_from_banner = hint
                print(f"→ Environment hint from title line: {hint!r} ({line!r})", flush=True)
            else:
                print(f"→ Skipping preamble line: {line!r}", flush=True)
            continue

        raise ConfigBlockError(
            f"Unexpected line after {last_key!r} (use a label like branch: or put ports under services:): {line!r}"
        )

    if branch is None:
        raise ConfigBlockError("Missing branch: line.")
    if version is None:
        raise ConfigBlockError("Missing version: line.")
    if not service_lines:
        raise ConfigBlockError(
            "Missing services: section or no service payload (ports or names, e.g. 3000 or MGNT_API_server)."
        )

    if _service_lines_mean_update_all(service_lines):
        print(
            "→ Parsed ``services:`` as **update all** (keyword only) — will tick **"
            + _jenkins_update_all_stapler_name()
            + "**; no per-service ids.\n",
            flush=True,
        )
        services = []
        update_all = True
    else:
        services = _service_ids_from_service_block_lines(service_lines)
        update_all = False
    if env is None:
        if env_from_banner is not None:
            env = env_from_banner
            print(f"→ Using environment from banner/title: {env!r}", flush=True)
        else:
            env = normalize_parameter_text(
                os.environ.get("FPMS_DEFAULT_ENVIRONMENT", "fpms-uat-branch")
            )
            if env not in ENVIRONMENTS:
                env = ENVIRONMENTS[0]
            print(
                f"→ No environment: in block — using {env!r} "
                "(set environment: …, a title like ``Update FPMS UAT2 Branch``, or FPMS_DEFAULT_ENVIRONMENT)."
            )

    return env, services, branch, version, update_all


def _extract_keyed_value(
    text: str,
    keys: Sequence[str],
    *,
    stop_keys: Sequence[str],
) -> str | None:
    """
    Extract one keyed value from mixed text, handling both:
      1) dedicated lines, e.g. ``repository: ds-superjackpot-api``
      2) inline blocks, e.g. ``... repository: ds-superjackpot-api env: prod ...``
    """
    if not text:
        return None
    key_alt = "|".join(re.escape(k) for k in keys)
    stop_alt = "|".join(re.escape(k) for k in stop_keys if k not in keys)
    line_re = re.compile(
        rf"^\s*(?:`+|\*{{1,2}})?(?:{key_alt})(?:`+|\*{{1,2}})?\s*[:=]\s*(?P<v>.*?)\s*$",
        re.I,
    )
    def _trim_before_next_key(v: str) -> str:
        vv = normalize_parameter_text(v)
        if not vv or not stop_alt:
            return vv
        parts = re.split(rf"\s+\b(?:{stop_alt})\b\s*[:=]", vv, maxsplit=1, flags=re.I)
        return normalize_parameter_text(parts[0] if parts else vv)

    for raw in text.splitlines():
        m = line_re.match((raw or "").strip())
        if m:
            v = _trim_before_next_key(m.group("v") or "")
            if v:
                return v

    if not stop_alt:
        stop_alt = key_alt
    inline_re = re.compile(
        rf"\b(?:{key_alt})\b\s*[:=]\s*(?P<v>.+?)(?=(?:\s+\b(?:{stop_alt})\b\s*[:=])|$)",
        re.I | re.S,
    )
    m2 = inline_re.search(text)
    if not m2:
        return None
    v2 = _trim_before_next_key(m2.group("v") or "")
    return v2 or None


def _bi_repo_canonical(token: str) -> str:
    t = normalize_parameter_text(token).casefold()
    t = t.replace("_", "-").replace("/", "-")
    t = re.sub(r"\s+", "-", t)
    t = re.sub(r"[^a-z0-9-]+", "-", t)
    t = re.sub(r"-{2,}", "-", t).strip("-")
    if t.startswith("ds-"):
        t = t[3:]
    if t.startswith("bi-"):
        t = t[3:]
    if t.endswith("-api"):
        t = t[:-4]
    return t.strip("-")


def _find_ds_or_bi_repo_token(text: str) -> str | None:
    """First token like ``ds-xxx`` / ``bi-xxx`` from mixed free text."""
    if not text:
        return None
    m = re.search(r"\b((?:ds|bi)-[a-z0-9][a-z0-9._/-]*)\b", text, re.I)
    if not m:
        return None
    tok = normalize_parameter_text(m.group(1) or "")
    return tok or None


def parse_bi_api_update_message_block(text: str) -> tuple[str, str, str]:
    """
    Parse free text (chat paste / one-line command) for BI-API-UPDATE fields:
    ``repository``, ``env``/``environment``, ``branch``/``source_branch``.
    """
    body = (text or "").strip()
    if not body:
        raise ConfigBlockError("BI-API-UPDATE text is empty.")
    keys_all = (
        "repository",
        "repo",
        "service",
        "services",
        "environment",
        "env",
        "source_branch",
        "source branch",
        "branch",
    )
    repo = _extract_keyed_value(
        body,
        ("repository", "repo", "service", "services"),
        stop_keys=keys_all,
    )
    if not repo:
        repo = _find_ds_or_bi_repo_token(body)
    env = _extract_keyed_value(
        body,
        ("environment", "env"),
        stop_keys=keys_all,
    )
    branch = _extract_keyed_value(
        body,
        ("source_branch", "source branch", "branch"),
        stop_keys=keys_all,
    )
    if not repo:
        raise ConfigBlockError(
            "Missing repository/services for BI-API-UPDATE (use ds-... or bi-... token)."
        )
    if not env:
        raise ConfigBlockError("Missing env: / environment: for BI-API-UPDATE.")
    if not branch:
        raise ConfigBlockError("Missing branch: / source_branch: for BI-API-UPDATE.")
    repo_norm = normalize_parameter_text(repo)
    repo_low = repo_norm.casefold()
    if not (repo_low.startswith("ds-") or repo_low.startswith("bi-")):
        raise ConfigBlockError(
            f"Repository/service {repo_norm!r} must start with ds- or bi- for BI-API-UPDATE shortcut."
        )
    env_norm = normalize_parameter_text(env).casefold()
    branch_norm = normalize_parameter_text(branch)
    if not env_norm:
        raise ConfigBlockError("environment value is empty.")
    if not branch_norm:
        raise ConfigBlockError("source branch value is empty.")
    return repo_norm, env_norm, branch_norm


def parse_bi_api_update_config_block(text: str) -> tuple[str, str, str]:
    """Parse internal ``BI_API_UPDATE_V1`` block passed to ``run()``."""
    body = (text or "").strip()
    if not body:
        raise ConfigBlockError("BI_API_UPDATE_V1 config is empty.")
    lines = body.splitlines()
    if not lines or lines[0].strip().upper() != "BI_API_UPDATE_V1":
        raise ConfigBlockError("Missing BI_API_UPDATE_V1 header.")
    return parse_bi_api_update_message_block("\n".join(lines[1:]))


def _bi_api_update_build_config_block(repository: str, environment: str, source_branch: str) -> str:
    return (
        "BI_API_UPDATE_V1\n"
        f"repository: {normalize_parameter_text(repository)}\n"
        f"environment: {normalize_parameter_text(environment).casefold()}\n"
        f"source_branch: {normalize_parameter_text(source_branch)}\n"
    )


def read_multiline_config_paste() -> str:
    """
    Read lines until an **empty line** (Enter on an empty line ends the block).
    """
    print(
        "\n—— Paste config block ——\n"
        "Include lines like:\n"
        "  environment: fpms-uat-branch\n"
        "  branch: master\n"
        "  version: 3.2.128g\n"
        "  services:\n"
        "  7300 - fg_exrestful\n"
        "  or:  services: all   (tick Jenkins “update all services” — FPMS / FNT / SMS blocks)\n"
        "  or:  service: 3000, 9000, 9280\n"
        "  or:  Update FPMS UAT2 Branch  (selects environment fpms-uat2-branch)\n"
        "       service: MGNT_API_server, mgnt_web\n"
        "  7400 - pagcor\n"
        "End with an **empty line** (press Enter twice).\n"
        "Lines before ``branch:`` / ``version:`` (e.g. a title or ``email:``) are ignored.\n"
        "EN: Empty line finishes input; Ctrl+D (EOF) also finishes.\n"
    )
    parts: list[str] = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        if line == "":
            break
        parts.append(line)
    block = "\n".join(parts).strip()
    if not block:
        raise ConfigBlockError("No text pasted before empty line / EOF.")
    return block


# UnoChoice ``active-choice`` pane: hidden ``name=Services`` + ``.dynamic_checkbox`` (see Jenkins HTML).
# Must sit *inside* ``() => { ... }`` / ``(v) => { ... }`` — Playwright evaluates the string as one expression.
_SERVICES_UNOCHOICE_JS_FN = r"""
    function __fpmsServicesCheckboxRoot() {
        for (const item of document.querySelectorAll("div.jenkins-form-item")) {
            const lab = item.querySelector(".jenkins-form-label");
            if (!lab) continue;
            const t = (lab.textContent || "").replace(/\s+/g, " ").trim();
            if (!/^Services$/i.test(t)) continue;
            const marker = item.querySelector(
                'div.active-choice input[type="hidden"][name="name"][value="Services"]'
            );
            if (marker) {
                const pane = marker.closest("div.active-choice");
                if (pane) {
                    const inner = pane.querySelector(".dynamic_checkbox");
                    return inner || pane;
                }
            }
            const box = item.querySelector(".dynamic_checkbox");
            if (box) return box;
            const pane = item.querySelector("div.active-choice");
            if (pane) return pane;
        }
        return null;
    }
    function __fpmsFindServiceInput(root, v) {
        return (
            root.querySelector('input[type="checkbox"][value="' + v + '"]') ||
            root.querySelector('input[type="checkbox"][json="' + v + '"]')
        );
    }
    function __fpmsServicesFormItem() {
        for (const item of document.querySelectorAll("div.jenkins-form-item")) {
            const lab = item.querySelector(".jenkins-form-label");
            if (!lab) continue;
            const t = (lab.textContent || "").replace(/\s+/g, " ").trim();
            if (!/^Services$/i.test(t)) continue;
            return item;
        }
        return null;
    }
    function __fpmsServicesCheckedListWide() {
        const item = __fpmsServicesFormItem();
        if (!item) return [];
        const acc = [];
        for (const el of item.querySelectorAll('input[type="checkbox"]')) {
            if (!el.checked) continue;
            const v = (el.getAttribute('value') || el.getAttribute('json') || '').trim();
            if (v) acc.push(v);
        }
        return acc;
    }
"""


def _credentials() -> tuple[str, str]:
    u = os.environ.get("JENKINS_USERNAME", _DEFAULT_USER).strip()
    p = os.environ.get("JENKINS_PASSWORD", _DEFAULT_PASSWORD)
    return u, p


def _safe_page_wait(page, ms: int) -> None:
    """
    ``page.wait_for_timeout`` with a clear error if the browser window was closed
    (otherwise Playwright raises a generic “Target page… has been closed”).
    """
    if page.is_closed():
        raise RuntimeError(
            "页面已关闭，无法继续（请勿在脚本运行中途关闭浏览器）。"
            " / Page is closed — do not close the browser while the script runs."
        )
    try:
        page.wait_for_timeout(ms)
    except Exception as exc:
        if page.is_closed() or "has been closed" in str(exc).lower():
            raise RuntimeError(
                "浏览器或页面在等待期间被关闭；如非手动关闭请重试。"
                " / Browser or page was closed during a wait. "
                "If this was right after a warm-up reload, try FPMS_WARMUP_RELOAD=0 or a shorter "
                "FPMS_MS_WARMUP_POST_RELOGIN_MS."
            ) from exc
        raise


def _form_row(page, label: str):
    return page.locator("div.jenkins-form-item").filter(
        has=page.locator(
            "div.jenkins-form-label",
            has_text=re.compile(rf"^\s*{re.escape(label)}\s*$", re.I),
        )
    )


def prompt_environment() -> str:
    print("\nWhat Environment? (Only choose one)")
    for i, e in enumerate(ENVIRONMENTS, start=1):
        print(f"  {i}. {e}")
    n = len(ENVIRONMENTS)
    while True:
        raw = input("> ").strip()
        if not raw.isdigit():
            print(f"  Enter a single number from 1 to {n}.")
            continue
        idx = int(raw)
        if 1 <= idx <= n:
            choice = ENVIRONMENTS[idx - 1]
            print(f"  → Selected: {choice}")
            return choice
        print(f"  Invalid choice. Use 1–{n}.")


def _parse_multi_indices(line: str, n_max: int) -> list[int] | None:
    """Parse '1,2', '1 2', '1, 3 5' → unique 1-based indices, stable order."""
    parts = [p for p in re.split(r"[\s,]+", (line or "").strip()) if p]
    if not parts:
        return None
    out: list[int] = []
    seen: set[int] = set()
    for p in parts:
        if not p.isdigit():
            return None
        idx = int(p)
        if idx < 1 or idx > n_max:
            return None
        if idx not in seen:
            seen.add(idx)
            out.append(idx)
    return out


def _parse_single_menu_index(line: str, n_max: int) -> int | None:
    """Exactly one digit token in ``1..n_max`` (job picker); ``1 2`` → None."""
    idxs = _parse_multi_indices(line, n_max)
    if idxs is None or len(idxs) != 1:
        return None
    return idxs[0]


def _stdin_stdout_interactive() -> bool:
    try:
        return bool(sys.stdin.isatty() and sys.stdout.isatty())
    except Exception:
        return False


def _config_text_service_auto_pick() -> bool:
    """If true, do not prompt for bare fuzzy ``services:`` text tokens (use best automatic match)."""
    return os.environ.get("FPMS_CONFIG_SERVICE_TEXT_AUTO", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _prompt_service_ids_for_config_text_token(
    token: str,
    ranked: list[str],
    already: set[str],
) -> list[str]:
    """
    Show ``ranked`` (1-based menu) for a ``services:`` line text token; return chosen checkbox ids.

    Caller adds to ``out`` / ``already``; duplicates in ``already`` are skipped with a short notice.
    """
    print(
        f"\n→ Service text {token!r} — near matches (best first). Pick one or more: **1**, **2**, "
        "**1 2 3**, or **1,2,3**:",
        flush=True,
    )
    for k, name in enumerate(ranked, start=1):
        tag = " (already in list)" if name in already else ""
        print(f"  {k}. {name}{tag}", flush=True)
    n = len(ranked)
    while True:
        raw = input("  > ").strip()
        idxs = _parse_multi_indices(raw, n)
        if idxs is None:
            print(f"  Use numbers 1–{n} only, separated by spaces or commas.", flush=True)
            continue
        if not idxs:
            print(f"  Pick at least one number from 1 to {n}.", flush=True)
            continue
        chosen = [ranked[i - 1] for i in idxs]
        if all(s in already for s in chosen):
            print("  Those are already selected — choose other numbers.", flush=True)
            continue
        return chosen


def prompt_services() -> tuple[list[str], bool]:
    """
    Interactive FPMS service selection.

    Returns ``(service_ids, update_all)``. Typing **all** / **\\*** / **全部** (alone, before any pick)
    means tick the Jenkins update-all checkbox instead of individual services.
    """
    print(
        '\nWhat services? (can be multiple 1 2 or 1,2 — type a name to search, '
        'then choose number(s); type **all** to update every service via the job checkbox; '
        'type **end** to finish this step)'
    )
    selected: list[str] = []
    seen: set[str] = set()
    last_matches: list[str] | None = None

    while True:
        raw = input("> ").strip()
        low = raw.casefold()
        if not raw:
            print("  Type a search string, numbers from the last list, or **end**.")
            continue

        if low in ("all", "*", "every", "全部") and not selected and last_matches is None:
            print(
                f"→ **{low}** — will tick **{_jenkins_update_all_stapler_name()}** "
                "in Jenkins (no per-service checklist).\n",
                flush=True,
            )
            return [], True

        if low == "end":
            if not selected:
                print("  Pick at least one service (search → numbers) before **end**.")
                continue
            print(f"→ Selected: {', '.join(selected)}")
            return selected, False

        idxs = _parse_multi_indices(raw, len(last_matches) if last_matches else 0)
        if last_matches and idxs is not None:
            added_any = False
            for i in idxs:
                name = last_matches[i - 1]
                if name in seen:
                    print(f"  (Already in selection: {name})")
                    continue
                seen.add(name)
                selected.append(name)
                added_any = True
            if added_any:
                print(f"  Selected so far: {', '.join(selected)}")
            continue

        if last_matches and idxs is None and raw.isdigit():
            print(f"  Use numbers 1–{len(last_matches)} from the list above, or a new search word.")
            continue

        if last_matches is None and re.fullmatch(r"[\d\s,]+", raw):
            print("  Search by name first (you will get a numbered list), then pick 1, 1 2, or 1,2.")
            continue

        # New search (not valid index line for current list)
        last_matches = _rank_services_by_query(raw, limit=12, for_menu=True)
        if not last_matches:
            print("  No services in list.")
            continue
        for i, s in enumerate(last_matches, start=1):
            print(f"  {i}. {s}")


def prompt_text(label: str) -> str:
    print(f"\n{label}")
    while True:
        raw = input("> ").strip()
        if raw:
            return raw
        print("  (Required — please type a value.)")


def jenkins_login_if_needed(page, username: str, password: str, timeout_ms: int = 60_000) -> None:
    user_loc = page.locator("input#j_username, input[name='j_username']").first
    try:
        user_loc.wait_for(state="visible", timeout=8_000)
    except PlaywrightTimeout:
        return

    print("→ Jenkins login form detected, signing in…")
    user_loc.fill(username)
    pw = page.locator("input#j_password, input[name='j_password']").first
    pw.fill(password)
    sub = page.locator(
        "button[name='Submit'], input[name='Submit'][type='submit'], "
        "button:has-text('Sign in'), button:has-text('log in')"
    ).first
    sub.click()
    page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    # Give Jenkins time to finish rendering after login (parameters / UnoChoice, etc.)
    _safe_page_wait(page, _MS_AFTER_LOGIN)


def _jenkins_build_form_url_candidates(url: str) -> list[str]:
    """
    Build-parameters URL candidates for a Jenkins job URL.

    Some aliases may point to a job root page instead of the form page; this helper
    adds ``/build?delay=0sec`` and ``/buildWithParameters`` fallbacks.
    """
    raw = (url or "").strip()
    if not raw:
        return []

    out: list[str] = []
    seen: set[str] = set()

    def _add(u: str) -> None:
        uu = (u or "").strip()
        if not uu or uu in seen:
            return
        seen.add(uu)
        out.append(uu)

    _add(raw)
    base_q = raw.split("?", 1)[0].rstrip("/")
    low = base_q.casefold()

    if low.endswith("/buildwithparameters"):
        root = base_q[: -len("/buildWithParameters")].rstrip("/")
        _add(root + "/build?delay=0sec")
    elif low.endswith("/build"):
        root = base_q[: -len("/build")].rstrip("/")
        _add(root + "/buildWithParameters")
        _add(root + "/build?delay=0sec")
    elif "/build?" in raw.casefold():
        root = raw[: raw.casefold().find("/build?")].rstrip("/")
        _add(root + "/buildWithParameters")
    else:
        root = base_q
        _add(root + "/build?delay=0sec")
        _add(root + "/buildWithParameters")

    return out


def _ensure_jenkins_parameters_form_visible(
    page,
    username: str,
    password: str,
    *,
    preferred_url: str,
    timeout_ms: int = 60_000,
) -> None:
    """
    Ensure ``div.jenkins-form-item`` is visible; if not, retry likely parameter URLs.
    """
    form_sel = "div.jenkins-form-item"
    try:
        page.wait_for_selector(form_sel, timeout=min(12_000, timeout_ms))
        return
    except PlaywrightTimeout:
        pass

    candidates: list[str] = []
    for src in ((page.url or "").strip(), preferred_url):
        for u in _jenkins_build_form_url_candidates(src):
            if u not in candidates:
                candidates.append(u)

    for u in candidates:
        cur = (page.url or "").strip()
        if cur and cur.rstrip("/") == u.rstrip("/"):
            continue
        print(f"→ Parameters form not visible; trying build URL fallback: {u}")
        try:
            page.goto(u, wait_until="domcontentloaded", timeout=90_000)
            jenkins_login_if_needed(page, username, password)
            page.wait_for_selector(form_sel, timeout=min(20_000, timeout_ms))
            return
        except Exception:
            continue

    raise RuntimeError(
        "Jenkins parameters form not visible after login and build URL fallbacks. "
        f"Tried: {', '.join(candidates) if candidates else preferred_url}"
    )


def open_fpms_build_with_login(
    page,
    username: str,
    password: str,
    *,
    first_visit: bool,
    warmup: bool | None = None,
    build_url: str | None = None,
) -> None:
    """
    ``goto`` build-with-parameters URL, login if needed, optional warm-up reload (same as a fresh run).

    Pass ``warmup=False`` to skip the post-login reload (e.g. ``--tick`` mode).
    """
    url = (build_url or BUILD_URL).strip()
    if first_visit:
        print(f"\n→ Opening {url}")
    page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    jenkins_login_if_needed(page, username, password)
    do_warmup = _WARMUP_RELOAD if warmup is None else warmup
    if do_warmup:
        print(
            "→ Warm-up: reloading build-with-parameters page once (often fixes first-load UnoChoice flake)."
            if first_visit
            else "→ Warm-up after re-login: reloading build-with-parameters page once…"
        )
        page.reload(wait_until="domcontentloaded", timeout=90_000)
        w = _MS_WARMUP_POST_RELOAD if first_visit else _MS_WARMUP_POST_RELOGIN
        _safe_page_wait(page, w)
        jenkins_login_if_needed(page, username, password)
    _ensure_jenkins_parameters_form_visible(
        page,
        username,
        password,
        preferred_url=url,
        timeout_ms=60_000,
    )


def _service_checkbox_in_dom(page, value: str) -> bool:
    """True if Services has a checkbox for ``value`` (``value`` or ``json`` attr, per UnoChoice)."""
    return bool(
        page.evaluate(
            "(v) => {"
            + _SERVICES_UNOCHOICE_JS_FN
            + """const root = __fpmsServicesCheckboxRoot();
                if (!root) return false;
                return !!(__fpmsFindServiceInput(root, v));
            }""",
            value,
        )
    )


def _scroll_services_pane_to_reveal_service(page, value: str) -> bool:
    """
    Scroll Services scroll parents / root so a lazily mounted row (virtual list) appears.
    Returns true once ``__fpmsFindServiceInput`` finds the checkbox.
    """
    return bool(
        page.evaluate(
            "(v) => {"
            + _SERVICES_UNOCHOICE_JS_FN
            + r"""
                const root = __fpmsServicesCheckboxRoot();
                if (!root) return false;
                const hit = () => !!__fpmsFindServiceInput(root, v);
                if (hit()) return true;
                function scrollableAncestors(el) {
                    const out = [];
                    let e = el;
                    for (let d = 0; d < 18 && e; d++, e = e.parentElement) {
                        if (!e || e === document.body) break;
                        const st = window.getComputedStyle(e);
                        const oy = e.scrollHeight - e.clientHeight;
                        if (oy > 3 && /(auto|scroll)/i.test(st.overflowY)) {
                            out.push(e);
                        }
                    }
                    return out;
                }
                function walk(pane) {
                    const ch = pane.clientHeight || 200;
                    const step = Math.max(120, Math.floor(ch * 0.88));
                    const maxTop = Math.max(0, pane.scrollHeight - pane.clientHeight);
                    for (let top = 0; top <= maxTop; top += step) {
                        pane.scrollTop = top;
                        if (hit()) return true;
                    }
                    pane.scrollTop = maxTop;
                    return hit();
                }
                for (const p of scrollableAncestors(root)) {
                    if (walk(p)) return true;
                }
                return walk(root);
            }""",
            value,
        )
    )


def _reveal_all_requested_services_for_batch(page, names: list[str]) -> None:
    """Scroll so every requested service input exists (best-effort before ``_services_apply_batch_js``)."""
    for n in names:
        if _service_checkbox_in_dom(page, n):
            continue
        _strip_unochoice_max_count(page)
        _safe_page_wait(page, 100)
        if _scroll_services_pane_to_reveal_service(page, n):
            print(f"→ Batch prep: scrolled to reveal {n!r}.")


def _services_apply_batch_js(page, names: list[str]) -> list[str]:
    """
    One synchronous ``page.evaluate``: for every name, set the Services checkbox checked and dispatch
    input/change/(click). Fewer UnoChoice reflows than clicking services one after another.

    Returns the subset of ``names`` that are **still** not ``.checked`` after the call (re-read in JS).
    """
    if not names:
        return []
    still = page.evaluate(
        """(names) => {"""
        + _SERVICES_UNOCHOICE_JS_FN
        + r"""
            const root = __fpmsServicesCheckboxRoot();
            if (!root) return names.slice();
            for (const v of names) {
                const el = __fpmsFindServiceInput(root, v);
                if (!el || el.checked) continue;
                try {
                    el.focus();
                } catch (e) {}
                el.checked = true;
                el.dispatchEvent(new Event("input", { bubbles: true, cancelable: true }));
                el.dispatchEvent(new Event("change", { bubbles: true, cancelable: true }));
                try {
                    el.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }));
                } catch (e2) {}
                try {
                    el.dispatchEvent(
                        new KeyboardEvent("keydown", { key: " ", code: "Space", bubbles: true })
                    );
                    el.dispatchEvent(
                        new KeyboardEvent("keyup", { key: " ", code: "Space", bubbles: true })
                    );
                } catch (e3) {}
            }
            const still = [];
            for (const v of names) {
                const el = __fpmsFindServiceInput(root, v);
                if (!el || !el.checked) still.push(v);
            }
            return still;
        }""",
        names,
    )
    return list(still)


def _services_checkbox_count(page) -> int:
    """Count checkboxes in the Services parameter row (survives DOM swap if re-queried in JS)."""
    return page.evaluate(
        "() => {"
        + _SERVICES_UNOCHOICE_JS_FN
        + r"""
            const root = __fpmsServicesCheckboxRoot();
            if (!root) return 0;
            return root.querySelectorAll('input[type="checkbox"]').length;
        }"""
    )


def _services_confirmed_empty(
    page, *, polls: int | None = None, gap_ms: int | None = None, initial_settle_ms: int = 0
) -> bool:
    """
    True only if Services checkbox count is **0** for ``polls`` consecutive samples.

    CascadeChoiceParameter often drops the list for a short blink; a single ``count==0`` read
    would false-trigger a full page reload.
    """
    p = polls if polls is not None else _SERVICES_GONE_POLLS
    g = gap_ms if gap_ms is not None else _SERVICES_GONE_POLL_MS
    if initial_settle_ms > 0:
        _safe_page_wait(page, initial_settle_ms)
    p = max(2, p)
    for i in range(p):
        if _services_checkbox_count(page) != 0:
            return False
        if i < p - 1:
            _safe_page_wait(page, g)
    return True


def _wait_services_list_stable(
    page,
    timeout_ms: int = 45_000,
    *,
    need_streak: int = 3,
    poll_ms: int = 180,
    empty_skip_if_targets_satisfied: list[str] | None = None,
) -> int:
    """
    After Environment changes, UnoChoice replaces the Services DOM asynchronously.
    Wait until checkbox **count** is stable (unchanged for several polls) and > 0.

    If the inner list **blinks empty** after we already saw checkboxes, but
    ``empty_skip_if_targets_satisfied`` is set and a **wide** row read shows every target checked,
    return immediately (see ``FPMS_SERVICES_UI_EMPTY_OK``).
    """
    deadline = time.time() + timeout_ms / 1000.0
    last_n = -1
    same_streak = 0
    last_log = time.time()
    saw_nonempty = False
    while time.time() < deadline:
        n = _services_checkbox_count(page)
        if n > 0:
            saw_nonempty = True
        if (
            empty_skip_if_targets_satisfied
            and saw_nonempty
            and n == 0
            and _services_confirmed_empty(page, polls=3, gap_ms=min(200, _SERVICES_GONE_POLL_MS))
            and _services_requested_satisfied_wide(page, empty_skip_if_targets_satisfied)
        ):
            print(
                "→ Services list cleared in the UI but every requested service still reads as checked "
                "(wide row scan) — continuing without full stabilize wait "
                "(FPMS_SERVICES_UI_EMPTY_OK=1)."
            )
            return 1
        if n == last_n and n > 0:
            same_streak += 1
            if same_streak >= need_streak:
                print(f"→ Services list stable ({n} checkbox(es) visible).")
                return n
        else:
            same_streak = 1 if n > 0 else 0
            last_n = n
        now = time.time()
        if now - last_log >= 8.0:
            rem = max(0, int(deadline - now))
            print(
                f"→ Services list stabilizing… count={n}, ~{rem}s left "
                f"(FPMS_SERVICES_STABLE_MS={timeout_ms})",
                flush=True,
            )
            last_log = now
        _safe_page_wait(page, poll_ms)
    n = _services_checkbox_count(page)
    print(f"⚠️ Services list did not fully stabilize in time (last count={n}).")
    if n <= 0:
        if (
            empty_skip_if_targets_satisfied
            and saw_nonempty
            and _services_confirmed_empty(page, polls=3, gap_ms=min(200, _SERVICES_GONE_POLL_MS))
            and _services_requested_satisfied_wide(page, empty_skip_if_targets_satisfied)
        ):
            print(
                "→ Services UI empty at stabilize deadline; wide row scan shows all requested checked — continuing."
            )
            return 1
        raise ServicesListGoneError(
            "Services list empty or never appeared after wait — will retry in a new browser session (same answers)."
        )
    return n


def _nudge_environment_cascade(page, sel, env_value: str) -> None:
    """
    UnoChoice sometimes never mounts Services checkboxes until Environment changes twice.
    Pick another ``<option>`` briefly, dispatch change, then restore ``env_value``.
    """
    try:
        vals = sel.evaluate("el => [...el.options].map(o => o.value).filter((v) => v != null && v !== '')")
    except Exception:
        vals = []
    if not isinstance(vals, list):
        vals = []
    alts = [str(v) for v in vals if str(v) != str(env_value)]
    _strip_unochoice_max_count(page)
    try:
        _form_row(page, "Services").scroll_into_view_if_needed()
    except Exception:
        pass
    _safe_page_wait(page, 280)
    if alts:
        try:
            if _ENV_SELECT_FORCE:
                sel.select_option(value=alts[0], force=True)
            else:
                sel.select_option(alts[0])
        except Exception:
            return
        _safe_page_wait(page, _MS_ENV_NUDGE_DWELL)
        try:
            sel.evaluate(
                """el => {
                    el.dispatchEvent(new Event("input", { bubbles: true }));
                    el.dispatchEvent(new Event("change", { bubbles: true }));
                }"""
            )
        except Exception:
            pass
        _safe_page_wait(page, 260)
    try:
        if _ENV_SELECT_FORCE:
            sel.select_option(value=env_value, force=True)
        else:
            sel.select_option(env_value)
        sel.evaluate(
            """el => {
                el.dispatchEvent(new Event("input", { bubbles: true }));
                el.dispatchEvent(new Event("change", { bubbles: true }));
            }"""
        )
    except Exception:
        pass
    _safe_page_wait(page, 420)


def _wait_network_idle_after_env_select_if_configured(page) -> None:
    """
    Optional ``page.wait_for_load_state("networkidle")`` after Environment changes.
    Jenkins often keeps long-polling connections — use a **bounded** timeout via
    ``FPMS_ENV_POST_SELECT_NETWORKIDLE_MS`` (default **0** = disabled).
    """
    ms = _MS_ENV_POST_SELECT_NETWORKIDLE
    if ms <= 0:
        return
    try:
        page.wait_for_load_state("networkidle", timeout=ms)
    except PlaywrightTimeout:
        print(
            f"⚠️ networkidle not reached within {ms} ms (FPMS_ENV_POST_SELECT_NETWORKIDLE_MS); continuing.",
            flush=True,
        )
    except Exception as ex:
        print(f"⚠️ wait_for_load_state(networkidle) skipped: {ex!r}", flush=True)


def _wait_services_checkbox_reattached_after_env(page, timeout_ms: int) -> None:
    """
    After ``select_option``, UnoChoice may replace the Services DOM — wait until at least one
    checkbox exists again (``wait_for_selector`` / locator ``attached``).
    """
    if timeout_ms <= 0:
        return
    row = _form_row(page, "Services")
    row.locator(
        'div.active-choice .dynamic_checkbox input[type="checkbox"], '
        'div.active-choice input[type="checkbox"]'
    ).first.wait_for(state="attached", timeout=timeout_ms)


def _wait_services_after_environment(page) -> None:
    """
    After Environment is applied:

    1. Poll until at least one Services checkbox exists, or ``FPMS_SERVICES_APPEAR_MS`` elapses.
       If still **0** → raise `ServicesListGoneError` → ``run()`` **starts a new browser session** (no long blind wait).
    2. If options exist → ``_wait_services_list_stable`` for up to ``FPMS_SERVICES_STABLE_MS``.

    This loop can run **many seconds** (default ``FPMS_SERVICES_APPEAR_MS`` = 32s) by design so UnoChoice
    has time to mount; lower that env var for faster failure when Services will never appear.
    """
    t0 = time.time()
    appear_deadline = t0 + _MS_SERVICES_APPEAR / 1000.0
    last_log = t0
    while time.time() < appear_deadline:
        if _services_checkbox_count(page) > 0:
            _wait_services_list_stable(page, timeout_ms=_MS_SERVICES_STABLE)
            return
        now = time.time()
        if now - last_log >= 5.0:
            rem = max(0, int(appear_deadline - now))
            print(
                f"→ Waiting for first Services checkbox… ~{rem}s left "
                f"(FPMS_SERVICES_APPEAR_MS={_MS_SERVICES_APPEAR})",
                flush=True,
            )
            last_log = now
        _safe_page_wait(page, 150)
    if _services_checkbox_count(page) == 0:
        raise ServicesListGoneError(
            f"No Services checkboxes after Environment within {_MS_SERVICES_APPEAR}ms "
            f"(FPMS_SERVICES_APPEAR_MS) — new browser session will retry with the same answers."
        )
    _wait_services_list_stable(page, timeout_ms=_MS_SERVICES_STABLE)


def select_environment(page, env_value: str) -> None:
    row = _form_row(page, "Environment")
    row.wait_for(state="visible", timeout=30_000)
    srv_row = _form_row(page, "Services")
    try:
        srv_row.wait_for(state="visible", timeout=15_000)
    except PlaywrightTimeout:
        raise ServicesListGoneError(
            "Services row not visible when starting Environment — will retry in a new browser session (same answers)."
        ) from None
    sel = row.locator("select.jenkins-select__input").first
    sel.wait_for(state="visible", timeout=15_000)

    printed = False
    for nudge in range(1, _ENV_SERVICES_NUDGE_TRIES + 1):
        if nudge > 1:
            print(
                f"⚠️ Services did not load for Environment {env_value!r}; cascade nudge "
                f"(attempt {nudge} of {_ENV_SERVICES_NUDGE_TRIES}: briefly switch branch + restore)…"
            )
            _nudge_environment_cascade(page, sel, env_value)

        if _MS_DEBUG_BEFORE_ENV_SELECT > 0:
            print(
                f"→ DEBUG: FPMS_DEBUG_MS_BEFORE_ENV_SELECT={_MS_DEBUG_BEFORE_ENV_SELECT} ms "
                "pause before Environment select_option…"
            )
            _safe_page_wait(page, _MS_DEBUG_BEFORE_ENV_SELECT)

        if _MS_ENV_SELECT_HOVER > 0:
            try:
                sel.hover(timeout=8_000)
                _safe_page_wait(page, _MS_ENV_SELECT_HOVER)
            except Exception:
                pass

        if _ENV_SELECT_FORCE:
            sel.select_option(value=env_value, force=True)
        else:
            sel.select_option(env_value)
        try:
            sel.evaluate(
                """el => {
                    el.dispatchEvent(new Event("input", { bubbles: true }));
                    el.dispatchEvent(new Event("change", { bubbles: true }));
                }"""
            )
        except Exception:
            pass
        if not printed:
            print(f"→ Environment selected in browser: {env_value!r}")
            printed = True

        _wait_network_idle_after_env_select_if_configured(page)
        try:
            _wait_services_checkbox_reattached_after_env(
                page, _MS_ENV_POST_SELECT_SERVICES_WAIT
            )
        except PlaywrightTimeout:
            print(
                "⚠️ Services checkboxes not re-attached within "
                f"{_MS_ENV_POST_SELECT_SERVICES_WAIT} ms (FPMS_ENV_POST_SELECT_SERVICES_MS); "
                "continuing with FPMS_SERVICES_APPEAR wait…",
                flush=True,
            )

        _strip_unochoice_max_count(page)
        _safe_page_wait(page, _MS_ENV_SETTLE)
        srv_row = _form_row(page, "Services")
        _wait_unochoice_services_ready(srv_row, page)

        try:
            _wait_services_after_environment(page)
        except ServicesListGoneError:
            if nudge >= _ENV_SERVICES_NUDGE_TRIES:
                raise
            continue

        _strip_unochoice_max_count(page)
        _safe_page_wait(page, 200)
        _safe_page_wait(page, _MS_AFTER_ENV_CASCADE)
        _safe_page_wait(page, 200)
        if _services_checkbox_count(page) == 0:
            if nudge >= _ENV_SERVICES_NUDGE_TRIES:
                raise ServicesListGoneError(
                    "Services has no checkboxes after Environment change — will retry in a new browser session (same answers)."
                )
            continue
        return


def _max_service_selections(row) -> int | None:
    """Read UnoChoice ``data-max-count`` (informational only; we do not block multi-select)."""
    h = row.locator("span.checkbox-content-data-holder[data-max-count]").first
    if h.count() == 0:
        return None
    raw = h.get_attribute("data-max-count")
    try:
        return int(raw) if raw is not None else None
    except ValueError:
        return None


def _wait_unochoice_services_ready(row, page) -> None:
    """UnoChoice often shows a spinner until options are ready."""
    try:
        spin = row.locator("[id*='spinner']").first
        if spin.count() > 0:
            try:
                spin.wait_for(state="hidden", timeout=30_000)
            except PlaywrightTimeout:
                pass
    except Exception:
        pass
    _safe_page_wait(page, 200)


def _wait_services_quiet_before_click(
    page, value: str, *, quiet_skip_if_all_checked: list[str] | None = None
) -> None:
    """
    Wait until UnoChoice is unlikely to be mid-reflow: spinner settled, checkbox count steady,
    and the target row exists. Clicks during the brief “empty list” blink cause flakes.

    Set ``FPMS_SKIP_SERVICES_QUIET=1`` to skip (faster but riskier). Tune streak/timeout via
    ``FPMS_SERVICES_QUIET_STREAK``, ``FPMS_MS_SERVICES_QUIET_BEFORE_CLICK``, ``FPMS_SERVICES_QUIET_POLL_MS``.

    If ``quiet_skip_if_all_checked`` is set and a wide row read shows every name checked while the
    inner list is empty, return early (same idea as ``FPMS_SERVICES_UI_EMPTY_OK``).
    """
    if _SKIP_SERVICES_QUIET:
        return
    row = _form_row(page, "Services")
    _wait_unochoice_services_ready(row, page)
    deadline = time.time() + _MS_SERVICES_QUIET_BEFORE_CLICK / 1000.0
    last_n = -1
    streak = 0
    need = max(2, _SERVICES_QUIET_STREAK)
    poll = max(80, _SERVICES_QUIET_POLL_MS)
    saw_nonempty = False
    while time.time() < deadline:
        n = _services_checkbox_count(page)
        if n > 0:
            saw_nonempty = True
        in_dom = _service_checkbox_in_dom(page, value)
        if (
            quiet_skip_if_all_checked
            and saw_nonempty
            and n == 0
            and _services_confirmed_empty(page, polls=3, gap_ms=min(200, _SERVICES_GONE_POLL_MS))
            and _services_requested_satisfied_wide(page, quiet_skip_if_all_checked)
        ):
            print(
                f"→ Quiet-wait: inner Services list empty before {value!r}, "
                "but wide scan shows every requested service already checked — proceeding."
            )
            return
        if n > 0 and in_dom:
            if n == last_n:
                streak += 1
            else:
                streak = 1
            last_n = n
            if streak >= need:
                _safe_page_wait(page, _MS_PRE_SERVICE_CLICK)
                return
        else:
            streak = 0
            last_n = n if n > 0 else -1
        _safe_page_wait(page, poll)
    if (
        quiet_skip_if_all_checked
        and saw_nonempty
        and _services_confirmed_empty(page, polls=3, gap_ms=min(200, _SERVICES_GONE_POLL_MS))
        and _services_requested_satisfied_wide(page, quiet_skip_if_all_checked)
    ):
        print(
            f"→ Quiet-wait deadline for {value!r}: empty UI but wide scan shows all requested checked — proceeding."
        )
        return
    print(
        f"⚠️ Services UI did not reach a quiet state before {value!r} "
        f"within {_MS_SERVICES_QUIET_BEFORE_CLICK}ms — clicking anyway (try increasing "
        "FPMS_MS_SERVICES_QUIET_BEFORE_CLICK or FPMS_SERVICES_QUIET_STREAK)."
    )


def _wait_services_stable_after_pick(
    page, *, satisfied_names: list[str] | None = None
) -> None:
    """
    After ticking one service, UnoChoice may rebuild the list. Wait until checkboxes are back
    and the count has settled briefly before touching the next service.
    """
    if _SKIP_SERVICES_QUIET:
        _safe_page_wait(page, min(400, _MS_BETWEEN_SERVICES))
        return
    deadline = time.time() + _MS_AFTER_PICK_STABLE / 1000.0
    last_n = -1
    streak = 0
    need = max(2, _SERVICES_AFTER_PICK_STREAK)
    poll = max(80, _SERVICES_AFTER_PICK_POLL_MS)
    saw_nonempty = False
    while time.time() < deadline:
        n = _services_checkbox_count(page)
        if n > 0:
            saw_nonempty = True
        if (
            satisfied_names
            and saw_nonempty
            and n == 0
            and _services_confirmed_empty(page, polls=3, gap_ms=min(200, _SERVICES_GONE_POLL_MS))
            and _services_requested_satisfied_wide(page, satisfied_names)
        ):
            print(
                "→ Services list cleared after a pick but all requested services read checked (wide) — next step."
            )
            return
        if n > 0:
            if n == last_n:
                streak += 1
            else:
                streak = 1
            last_n = n
            if streak >= need:
                return
        else:
            streak = 0
            last_n = -1
        _safe_page_wait(page, poll)
    if (
        satisfied_names
        and saw_nonempty
        and _services_confirmed_empty(page, polls=3, gap_ms=min(200, _SERVICES_GONE_POLL_MS))
        and _services_requested_satisfied_wide(page, satisfied_names)
    ):
        print(
            "→ Post-pick stabilize deadline hit with empty inner list; wide scan shows all requested checked — continuing."
        )
        return
    print(
        f"⚠️ Services list did not restabilize after pick within {_MS_AFTER_PICK_STABLE}ms "
        "(continuing — increase FPMS_MS_AFTER_PICK_STABLE if the next click flakes)."
    )


def _fresh_services_row_and_box(page):
    """Re-query Services row every time — UnoChoice swaps DOM; old locators go stale."""
    row = _form_row(page, "Services")
    row.wait_for(state="visible", timeout=30_000)
    box = row.locator(".dynamic_checkbox, div.active-choice").first
    box.wait_for(state="visible", timeout=20_000)
    return row, box


def _strip_unochoice_max_count(page) -> None:
    """UnoChoice reads data-max-count; loosen so multiple checks are not auto-reverted."""
    page.evaluate(
        r"""() => {
            for (const item of document.querySelectorAll("div.jenkins-form-item")) {
                const lab = item.querySelector(".jenkins-form-label");
                if (!lab) continue;
                const t = (lab.textContent || "").replace(/\s+/g, " ").trim();
                if (!/^Services$/i.test(t)) continue;
                const pane = item.querySelector("div.active-choice");
                if (!pane) continue;
                pane
                    .querySelectorAll("span.checkbox-content-data-holder[data-max-count]")
                    .forEach((el) => el.setAttribute("data-max-count", "99"));
                return;
            }
        }"""
    )


def _playwright_click_service_option_row(page, value: str, *, force: bool = True) -> bool:
    """
    Click the visible ``div.tr`` row for one service (heavier than a label tick — last resort).
    """
    row = _form_row(page, "Services")
    try:
        row.wait_for(state="visible", timeout=20_000)
    except PlaywrightTimeout:
        return False
    opt = row.locator(
        f'div.active-choice div.dynamic_checkbox div.tr:has(input[type="checkbox"][value="{value}"]), '
        f'div.active-choice div.dynamic_checkbox div.tr:has(input[type="checkbox"][json="{value}"])'
    ).first
    if opt.count() == 0:
        return False
    try:
        opt.scroll_into_view_if_needed()
        _safe_page_wait(page, _MS_HUMAN_POINTER_SETTLE if _HUMAN_LIKE_SERVICE_CLICKS else 40)
        opt.click(timeout=14_000, force=force)
        return True
    except PlaywrightTimeout:
        return False
    except Exception:
        return False


def _playwright_click_service_label_human(page, value: str) -> bool:
    """
    Click the row's ``label.attach-previous`` (or the checkbox) with **no** ``force=`` — closest to a human tick.
    """
    row = _form_row(page, "Services")
    try:
        row.wait_for(state="visible", timeout=20_000)
    except PlaywrightTimeout:
        return False
    tr = row.locator(
        f'div.active-choice div.dynamic_checkbox div.tr:has(input[type="checkbox"][value="{value}"]), '
        f'div.active-choice div.dynamic_checkbox div.tr:has(input[type="checkbox"][json="{value}"])'
    ).first
    if tr.count() == 0:
        return False
    lab = tr.locator("label.attach-previous").first
    inp = tr.locator('input[type="checkbox"]').first
    try:
        tr.scroll_into_view_if_needed()
        _safe_page_wait(page, _MS_HUMAN_POINTER_SETTLE)
        try:
            lab.wait_for(state="visible", timeout=3_000)
            lab.click(timeout=14_000)
            return True
        except (PlaywrightTimeout, Exception):
            pass
        inp.click(timeout=14_000)
        return True
    except PlaywrightTimeout:
        return False
    except Exception:
        return False


def _playwright_checkbox_click_service(page, value: str, *, force: bool) -> bool:
    """Plain ``click()`` on the service checkbox (not ``check()``), optional ``force``."""
    row = _form_row(page, "Services")
    try:
        row.wait_for(state="visible", timeout=20_000)
    except PlaywrightTimeout:
        return False
    inp = row.locator(
        f'div.active-choice input[type="checkbox"][value="{value}"], '
        f'div.active-choice input[type="checkbox"][json="{value}"]'
    ).first
    if inp.count() == 0:
        return False
    try:
        inp.scroll_into_view_if_needed()
        _safe_page_wait(page, _MS_HUMAN_POINTER_SETTLE if _HUMAN_LIKE_SERVICE_CLICKS else 40)
        inp.click(timeout=14_000, force=force)
        return True
    except PlaywrightTimeout:
        return False
    except Exception:
        return False


def _playwright_check_service(page, value: str, *, force: bool = True) -> bool:
    """
    Playwright ``check()`` on the Services checkbox only (``force`` defaults on for legacy callers).
    """
    row = _form_row(page, "Services")
    try:
        row.wait_for(state="visible", timeout=20_000)
    except PlaywrightTimeout:
        return False
    inp = row.locator(
        f'div.active-choice input[type="checkbox"][value="{value}"], '
        f'div.active-choice input[type="checkbox"][json="{value}"]'
    ).first
    if inp.count() == 0:
        return False
    try:
        inp.scroll_into_view_if_needed()
        _safe_page_wait(page, _MS_HUMAN_POINTER_SETTLE if _HUMAN_LIKE_SERVICE_CLICKS else 40)
        inp.check(timeout=14_000, force=force)
        return True
    except PlaywrightTimeout:
        return False
    except Exception:
        return False


def _playwright_space_toggle_service(page, value: str) -> bool:
    """
    Focus the Services checkbox and press ``Space`` (keyboard toggle), up to twice.
    Uses a different activation path than mouse ``click()`` / ``check()``, which some UnoChoice
    skins handle more safely.
    """
    row = _form_row(page, "Services")
    try:
        row.wait_for(state="visible", timeout=20_000)
    except PlaywrightTimeout:
        return False
    inp = row.locator(
        f'div.active-choice input[type="checkbox"][value="{value}"], '
        f'div.active-choice input[type="checkbox"][json="{value}"]'
    ).first
    if inp.count() == 0:
        return False
    try:
        inp.scroll_into_view_if_needed()
        _safe_page_wait(page, _MS_HUMAN_POINTER_SETTLE if _HUMAN_LIKE_SERVICE_CLICKS else 50)
        for _ in range(2):
            inp.focus()
            _safe_page_wait(page, 60)
            inp.press("Space")
            _safe_page_wait(page, 160)
            if _service_checked_js(page, value):
                return True
        return True
    except Exception:
        return False


def _service_checked_js(page, value: str) -> bool:
    """Read checkbox.checked in real DOM (avoids stale Playwright handles)."""
    return bool(
        page.evaluate(
            "(v) => {"
            + _SERVICES_UNOCHOICE_JS_FN
            + """const root = __fpmsServicesCheckboxRoot();
                if (!root) return false;
                const el = __fpmsFindServiceInput(root, v);
                return !!(el && el.checked);
            }""",
            value,
        )
    )


def _native_click_service_checkbox(page, value: str) -> bool:
    """
    Real browser click() on the checkbox input (same as ticking the box in the UI).
    """
    return bool(
        page.evaluate(
            "(v) => {"
            + _SERVICES_UNOCHOICE_JS_FN
            + """const root = __fpmsServicesCheckboxRoot();
                if (!root) return false;
                const inp = __fpmsFindServiceInput(root, v);
                if (!inp) return false;
                inp.scrollIntoView({ block: "center", inline: "nearest" });
                inp.focus();
                inp.click();
                return true;
            }""",
            value,
        )
    )


def _native_click_service_label(page, value: str) -> bool:
    """Click ``label.attach-previous`` next to the checkbox (typical human tick)."""
    return bool(
        page.evaluate(
            "(v) => {"
            + _SERVICES_UNOCHOICE_JS_FN
            + """const root = __fpmsServicesCheckboxRoot();
                if (!root) return false;
                const inp = __fpmsFindServiceInput(root, v);
                if (!inp) return false;
                const wrap = inp.parentElement;
                const lab = wrap && wrap.querySelector("label.attach-previous");
                (lab || inp).scrollIntoView({ block: "center", inline: "nearest" });
                if (lab) {
                    lab.click();
                } else {
                    inp.click();
                }
                return true;
            }""",
            value,
        )
    )


def _force_check_service_in_dom(page, value: str) -> None:
    """Last resort: set checked + InputEvent/ChangeEvent (some plugins only listen to these)."""
    ok = page.evaluate(
        "(v) => {"
        + _SERVICES_UNOCHOICE_JS_FN
        + """const root = __fpmsServicesCheckboxRoot();
            if (!root) return false;
            const el = __fpmsFindServiceInput(root, v);
            if (!el) return false;
            el.checked = true;
            el.dispatchEvent(new Event("input", { bubbles: true, cancelable: true }));
            el.dispatchEvent(new Event("change", { bubbles: true }));
            el.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }));
            return true;
        }""",
        value,
    )
    _safe_page_wait(page, 70 if _FPMS_FAST_FILL_ACTIVE else 220)
    if not ok:
        raise ServicesListGoneError(
            f"Could not find checkbox {value!r} in DOM — will retry in a new browser session (same answers)."
        )


def _ensure_service_selected(
    page,
    value: str,
    *,
    attempts: int = 8,
    is_first_pick: bool = False,
    all_requested: list[str] | None = None,
) -> None:
    """
    Tick a Services checkbox.

    After quiet-wait: default **Space key** on the checkbox (``FPMS_SERVICES_SPACE_FIRST=0`` to skip),
    then human-like mouse fallbacks (**FPMS_HUMAN_LIKE_SERVICES=0** for aggressive legacy order).

    The **first** pick in a batch still uses ``FPMS_MS_BEFORE_FIRST_SERVICE`` / ``FPMS_MS_AFTER_FIRST_SERVICE``.

    ``_wait_services_quiet_before_click`` / ``FPMS_SKIP_SERVICES_QUIET`` control pre-click stability waits.

    ``all_requested``: when UnoChoice clears the inner list, a **wide** row read may still show every
    requested id checked — then we treat the tick as successful and continue (``FPMS_SERVICES_UI_EMPTY_OK``).
    """
    if _service_checked_js(page, value):
        print(f"→ Service already selected: {value!r}")
        return

    def _tick_verified() -> bool:
        if _service_checked_js(page, value):
            return True
        vn = normalize_parameter_text(value)
        if (
            all_requested
            and _SERVICES_UI_EMPTY_OK
            and vn
            and vn in set(_read_services_checked_values_wide(page))
        ):
            return True
        return False

    if not _service_checkbox_in_dom(page, value):
        if not _scroll_services_pane_to_reveal_service(page, value):
            _strip_unochoice_max_count(page)
            _safe_page_wait(page, 220)
            if _scroll_services_pane_to_reveal_service(page, value):
                print(f"→ Scrolled Services list to reveal {value!r}.")
        else:
            print(f"→ Scrolled Services list to reveal {value!r}.")

    if not _HUMAN_LIKE_SERVICE_CLICKS:
        _strip_unochoice_max_count(page)

    if is_first_pick:
        _safe_page_wait(page, _MS_BEFORE_FIRST_SERVICE)
        if not _HUMAN_LIKE_SERVICE_CLICKS:
            _strip_unochoice_max_count(page)
    elif _HUMAN_LIKE_SERVICE_CLICKS:
        _safe_page_wait(page, _MS_HUMAN_PRE_CLICK)
    else:
        _safe_page_wait(page, 120)

    def _first_pick_settle() -> None:
        if is_first_pick:
            _safe_page_wait(page, _MS_AFTER_FIRST_SERVICE)

    def _after_interaction_wait_for_ui() -> None:
        if _FPMS_FAST_FILL_ACTIVE:
            pause = 150 if is_first_pick else 90
        elif _HUMAN_LIKE_SERVICE_CLICKS:
            pause = 680 if is_first_pick else 520
        else:
            pause = 520 if is_first_pick else 400
        _safe_page_wait(page, pause)
        if _services_confirmed_empty(page, initial_settle_ms=0):
            if (
                all_requested
                and _SERVICES_UI_EMPTY_OK
                and _services_requested_satisfied_wide(page, all_requested)
            ):
                print(
                    "→ Services inner list empty after interaction; wide row scan shows every requested "
                    "service still checked — treating as OK (FPMS_SERVICES_UI_EMPTY_OK)."
                )
                return
            print("→ Services list still empty after interaction (confirmed) — new browser session will retry.")
            raise ServicesListGoneError(
                "Services list cleared after a checkbox action — will retry in a new browser session (same answers)."
            )

    _wait_services_quiet_before_click(
        page, value, quiet_skip_if_all_checked=all_requested
    )

    def _try_space_toggle() -> bool:
        """Keyboard ``Space`` on the checkbox (not mouse); ``FPMS_SERVICES_SPACE_FIRST=0`` skips."""
        if not _SERVICES_SPACE_FIRST:
            return False
        if not _playwright_space_toggle_service(page, value):
            return False
        _after_interaction_wait_for_ui()
        if _tick_verified():
            _first_pick_settle()
            print(f"→ Service selected (checkbox Space key): {value!r}")
            return True
        return False

    if _HUMAN_LIKE_SERVICE_CLICKS:
        if _try_space_toggle():
            return
        if _playwright_click_service_label_human(page, value):
            _after_interaction_wait_for_ui()
            if _tick_verified():
                _first_pick_settle()
                print(f"→ Service selected (label click, human-like): {value!r}")
                return

        for _ in range(6):
            if not _native_click_service_label(page, value):
                break
            _after_interaction_wait_for_ui()
            if _tick_verified():
                _first_pick_settle()
                print(f"→ Service selected (JS label click): {value!r}")
                return

        if _playwright_checkbox_click_service(page, value, force=False):
            _after_interaction_wait_for_ui()
            if _tick_verified():
                _first_pick_settle()
                print(f"→ Service selected (checkbox click, no force): {value!r}")
                return

        if _playwright_check_service(page, value, force=False):
            _after_interaction_wait_for_ui()
            if _tick_verified():
                _first_pick_settle()
                print(f"→ Service selected (Playwright check, no force): {value!r}")
                return

        if _playwright_check_service(page, value, force=True):
            _after_interaction_wait_for_ui()
            if _tick_verified():
                _first_pick_settle()
                print(f"→ Service selected (Playwright check, force): {value!r}")
                return

        for _ in range(attempts):
            if not _native_click_service_checkbox(page, value):
                break
            _after_interaction_wait_for_ui()
            if _tick_verified():
                _first_pick_settle()
                print(f"→ Service selected (native checkbox click): {value!r}")
                return

        if _playwright_click_service_option_row(page, value, force=False):
            _after_interaction_wait_for_ui()
            if _tick_verified():
                _first_pick_settle()
                print(f"→ Service selected (option row, no force): {value!r}")
                return
        if _playwright_click_service_option_row(page, value, force=True):
            _after_interaction_wait_for_ui()
            if _tick_verified():
                _first_pick_settle()
                print(f"→ Service selected (option row, force): {value!r}")
                return

        _strip_unochoice_max_count(page)
        _safe_page_wait(page, 120)
        _force_check_service_in_dom(page, value)
        _after_interaction_wait_for_ui()
        if not _tick_verified():
            raise ServicesListGoneError(
                f"{value!r}: could not stay checked — will retry in a new browser session (same answers)."
            )
        _first_pick_settle()
        print(f"→ Service selected (DOM assign + events, last resort): {value!r}")
        return

    # Legacy aggressive path (FPMS_HUMAN_LIKE_SERVICES=0)
    if _try_space_toggle():
        return
    if not is_first_pick and _playwright_click_service_option_row(page, value, force=True):
        _safe_page_wait(page, 90 if _FPMS_FAST_FILL_ACTIVE else 320)
        if _services_confirmed_empty(
            page, initial_settle_ms=55 if _FPMS_FAST_FILL_ACTIVE else 120
        ):
            if not (
                all_requested
                and _SERVICES_UI_EMPTY_OK
                and _services_requested_satisfied_wide(page, all_requested)
            ):
                raise ServicesListGoneError(
                    "Services still empty after row click (confirmed) — will retry in a new browser session (same answers)."
                )
        if _tick_verified():
            _first_pick_settle()
            print(f"→ Service selected (clicked option row .tr): {value!r}")
            return

    if _playwright_check_service(page, value, force=True):
        _safe_page_wait(
            page,
            (150 if _FPMS_FAST_FILL_ACTIVE else 380) if is_first_pick else (85 if _FPMS_FAST_FILL_ACTIVE else 260),
        )
        if _services_confirmed_empty(
            page, initial_settle_ms=(90 if _FPMS_FAST_FILL_ACTIVE else 180) if is_first_pick else (70 if _FPMS_FAST_FILL_ACTIVE else 120)
        ):
            if not (
                all_requested
                and _SERVICES_UI_EMPTY_OK
                and _services_requested_satisfied_wide(page, all_requested)
            ):
                raise ServicesListGoneError(
                    "Services still empty after check() (confirmed) — will retry in a new browser session (same answers)."
                )
        if _tick_verified():
            _first_pick_settle()
            print(f"→ Service selected (Playwright check): {value!r}")
            return

    for _ in range(attempts):
        if not _native_click_service_label(page, value):
            break
        _after_interaction_wait_for_ui()
        if _tick_verified():
            _first_pick_settle()
            print(f"→ Service selected (label click, like ticking the checkbox): {value!r}")
            return
    for _ in range(attempts):
        if not _native_click_service_checkbox(page, value):
            break
        _after_interaction_wait_for_ui()
        if _tick_verified():
            _first_pick_settle()
            print(f"→ Service selected (native checkbox click): {value!r}")
            return
    _strip_unochoice_max_count(page)
    _safe_page_wait(page, 80)
    _force_check_service_in_dom(page, value)
    _after_interaction_wait_for_ui()
    if not _tick_verified():
        raise ServicesListGoneError(
            f"{value!r}: could not stay checked — will retry in a new browser session (same answers)."
        )
    _first_pick_settle()
    print(f"→ Service selected (DOM assign + events): {value!r}")


def select_services(page, service_names: list[str]) -> None:
    cleaned: list[str] = []
    for name in service_names:
        n = (name or "").strip()
        if not re.match(r"^[\w.-]+$", n):
            raise ValueError(f"Invalid service name: {n!r}")
        cleaned.append(n)

    empty_ok_targets = cleaned if _SERVICES_UI_EMPTY_OK else None

    row, box = _fresh_services_row_and_box(page)
    _wait_unochoice_services_ready(row, page)
    _safe_page_wait(
        page,
        95
        if _FPMS_FAST_FILL_ACTIVE
        else (520 if _HUMAN_LIKE_SERVICE_CLICKS else 450),
    )
    if _services_confirmed_empty(page, polls=3, gap_ms=280, initial_settle_ms=0):
        if not (
            _SERVICES_UI_EMPTY_OK and _services_requested_satisfied_wide(page, cleaned)
        ):
            raise ServicesListGoneError(
                "Services not shown when starting service selection (confirmed) — will retry in a new browser session (same answers)."
            )
        print(
            "→ Services inner list empty at selection start; wide row scan shows all requested already checked — continuing."
        )
    _wait_services_list_stable(
        page,
        timeout_ms=_MS_SERVICES_STABLE,
        empty_skip_if_targets_satisfied=empty_ok_targets,
    )
    _safe_page_wait(page, _MS_SERVICES_PRE_STRIP)
    _safe_page_wait(page, 200)
    if _services_checkbox_count(page) == 0:
        if not (
            _SERVICES_UI_EMPTY_OK and _services_requested_satisfied_wide(page, cleaned)
        ):
            raise ServicesListGoneError(
                "Services has no checkboxes after load — will retry in a new browser session (same answers)."
            )

    max_sel = _max_service_selections(row)
    if max_sel is not None and len(service_names) > max_sel:
        print(
            f"⚠️ Page reports data-max-count={max_sel}; loosening to 99 in DOM and selecting "
            f"{len(service_names)} service(s): {service_names!r}"
        )
    _strip_unochoice_max_count(page)
    _safe_page_wait(page, 150)

    mode = _SERVICES_SELECT_MODE
    if mode not in ("auto", "batch", "sequential"):
        mode = "auto"

    if mode in ("auto", "batch"):
        print(
            f"→ Services: trying **single DOM batch** first (mode={mode!r}; "
            "FPMS_SERVICES_SELECT_MODE=sequential skips this — fewer UnoChoice reflows than many clicks)."
        )
        _reveal_all_requested_services_for_batch(page, cleaned)
        _strip_unochoice_max_count(page)
        _safe_page_wait(page, 75 if _FPMS_FAST_FILL_ACTIVE else 180)
        _services_apply_batch_js(page, cleaned)
        _safe_page_wait(page, _MS_BATCH_POST_APPLY)
        still_unchecked = [n for n in cleaned if not _service_line_checked(page, n)]
        if still_unchecked:
            _services_apply_batch_js(page, cleaned)
            _safe_page_wait(
                page,
                min(180 if _FPMS_FAST_FILL_ACTIVE else 420, _MS_BATCH_POST_APPLY),
            )
        still_unchecked = [n for n in cleaned if not _service_line_checked(page, n)]

        if not still_unchecked:
            if _services_checkbox_count(page) == 0:
                if not (
                    _SERVICES_UI_EMPTY_OK
                    and _services_requested_satisfied_wide(page, cleaned)
                ):
                    raise ServicesListGoneError(
                        "Services list empty immediately after batch apply — will retry in a new browser session (same answers)."
                    )
            print("→ All requested services are checked (batch DOM path, no per-item clicking).")
            _safe_page_wait(page, _MS_SERVICES_TAIL)
            return
        if mode == "batch":
            raise ServiceNotDetectedError(
                f"Batch mode could not tick all services; still unchecked: {still_unchecked!r}. "
                "Use FPMS_SERVICES_SELECT_MODE=auto (default) to fall back to sequential clicks, or sequential only."
            )
        print(f"→ Batch incomplete; completing with sequential clicks for: {still_unchecked!r}")

    first_sequential_pick = True
    for n in cleaned:
        if _service_line_checked(page, n):
            print(f"→ Service already selected: {n!r}")
            continue
        row, box = _fresh_services_row_and_box(page)
        if not _service_checkbox_in_dom(page, n):
            if _service_line_checked(page, n):
                print(
                    f"→ Service {n!r} not in inner UnoChoice list but reads checked (wide) — skipping click."
                )
                continue
            _strip_unochoice_max_count(page)
            _safe_page_wait(page, 75 if _FPMS_FAST_FILL_ACTIVE else 220)
            if not _scroll_services_pane_to_reveal_service(page, n):
                if _services_requested_satisfied_wide(page, cleaned):
                    print(
                        "→ Could not scroll to "
                        f"{n!r}, but wide scan shows every requested service checked — continuing."
                    )
                    break
                raise ServicesListGoneError(
                    f"Service checkbox {n!r} missing from list — will retry in a new browser session (same answers)."
                )
            print(f"→ Scrolled Services list to reveal {n!r} before selecting.")
        if first_sequential_pick:
            print(
                "→ Sequential service picks: human-like click order by default "
                "(FPMS_HUMAN_LIKE_SERVICES=0 for legacy aggressive clicks)."
            )
        _ensure_service_selected(
            page, n, is_first_pick=first_sequential_pick, all_requested=cleaned
        )
        first_sequential_pick = False
        _wait_services_stable_after_pick(
            page, satisfied_names=empty_ok_targets
        )
        _safe_page_wait(page, _MS_BETWEEN_SERVICES)
        if _services_confirmed_empty(page, initial_settle_ms=100):
            if _services_requested_satisfied_wide(page, cleaned):
                print(
                    "→ Services inner list empty after a pick; wide scan shows all requested checked — done selecting."
                )
                break
            raise ServicesListGoneError(
                "Services still empty after selecting a service (confirmed) — will retry in a new browser session (same answers)."
            )

    _safe_page_wait(page, _MS_SERVICES_TAIL)


def _form_row_label_regex(page, pattern: re.Pattern[str]):
    """``jenkins-form-item`` whose label matches ``pattern`` (first match)."""
    return page.locator("div.jenkins-form-item").filter(
        has=page.locator("div.jenkins-form-label", has_text=pattern)
    ).first


def _stapler_boolean_checkbox_locator(page, stapler_name: str):
    """
    Checkbox inside ``div[name="parameter"]`` for a Jenkins boolean / choice checkbox whose
    hidden Stapler field is ``input[name="name"][value="<stapler_name>"]``.
    """
    return (
        page.locator('div[name="parameter"]')
        .filter(
            has=page.locator(
                f'input[type="hidden"][name="name"][value="{stapler_name}"]'
            )
        )
        .locator('input[type="checkbox"][name="value"]')
        .first
    )


def _refresh_pipeline_checkbox_locator(page):
    """Hidden ``Refresh pipeline`` + ``input[type="checkbox"][name="value"]``."""
    return _stapler_boolean_checkbox_locator(page, "Refresh pipeline")


def _read_stapler_boolean_checkbox_checked(page, stapler_name: str) -> bool:
    """Best-effort read of a Stapler-style boolean checkbox (same locator family as ticking)."""
    cb = _stapler_boolean_checkbox_locator(page, stapler_name)
    if cb.count() == 0:
        return False
    try:
        if cb.is_checked():
            return True
        return bool(
            cb.evaluate("el => !!(el && el.type === 'checkbox' && el.checked)")
        )
    except Exception:
        return False


def _verify_refresh_pipeline_checked(page) -> bool:
    """
    Re-locate the Refresh pipeline checkbox after a short settle — catches false positives
    when ``force`` toggles DOM briefly but Jenkins scripts reset it.
    """
    _safe_page_wait(page, max(100, _MS_TICK_VERIFY_SETTLE))
    cb = _refresh_pipeline_checkbox_locator(page)
    if cb.count() == 0:
        return False
    for _ in range(5):
        try:
            if cb.is_checked():
                return True
            if bool(
                cb.evaluate(
                    "el => !!(el && el.type === 'checkbox' && el.checked)"
                )
            ):
                return True
        except Exception:
            pass
        _safe_page_wait(page, 220)
    return False


def _wait_refresh_pipeline_help_idle(page, *, timeout_ms: int) -> None:
    """Best-effort: wait until the row's ``.jenkins-spinner`` is gone (UnoChoice / help)."""
    row = page.locator(
        'div.jenkins-form-item:has(input[type="hidden"][name="name"][value="Refresh pipeline"])'
    ).first
    try:
        row.wait_for(state="attached", timeout=min(timeout_ms, 10_000))
    except PlaywrightTimeout:
        return
    spin = row.locator(".jenkins-spinner")
    try:
        if spin.count() > 0 and spin.first.is_visible():
            spin.first.wait_for(state="hidden", timeout=max(3_000, timeout_ms))
    except Exception:
        pass


def _tick_checkbox_playwright_then_js(
    cb,
    *,
    how: str,
    log_label: str = "Refresh pipeline",
    try_label_attach_previous: bool = True,
    label_click_regex: re.Pattern[str] | None = None,
) -> bool:
    """
    ``scroll_into_view_if_needed``, ``check()``, then ``force``, optional label click, JS ``checked`` + events.

    When ``try_label_attach_previous`` is True, tries ``label.attach-previous`` with ``label_click_regex``
    (default: Refresh pipeline). Set ``try_label_attach_previous=False`` for parameters without that label.
    """
    if try_label_attach_previous and label_click_regex is None:
        label_click_regex = re.compile(r"^\s*refresh\s+pipeline\s*$", re.I)
    cb.wait_for(state="attached", timeout=20_000)
    try:
        cb.scroll_into_view_if_needed(timeout=10_000)
    except Exception:
        pass
    if cb.is_checked():
        print(f"→ {log_label} already checked ({how}).")
        return True

    def _ok(detail: str) -> bool:
        if cb.is_checked():
            print(f"→ Checked {log_label} ({how}, {detail}).")
            return True
        return False

    for detail, fn in (
        ("playwright check", lambda: cb.check(timeout=10_000)),
        ("playwright check force", lambda: cb.check(timeout=8_000, force=True)),
    ):
        try:
            fn()
            if _ok(detail):
                return True
        except Exception:
            pass

    if try_label_attach_previous and label_click_regex is not None:
        try:
            wrap = cb.locator("xpath=..")
            if wrap.count() > 0:
                lab = wrap.locator("label.attach-previous").filter(has_text=label_click_regex)
                if lab.count() > 0:
                    lab.first.click(timeout=8_000)
                    if _ok("label click"):
                        return True
        except Exception:
            pass

    try:
        mode = cb.evaluate(
            """el => {
              if (!el || el.type !== 'checkbox') return 'bad';
              if (el.checked) return 'already';
              el.scrollIntoView({block: 'center', inline: 'nearest'});
              const span = el.closest('.jenkins-checkbox');
              const lab = span && span.querySelector('label.attach-previous');
              if (lab) { lab.click(); }
              if (el.checked) return 'label';
              el.click();
              if (el.checked) return 'native-click';
              el.checked = true;
              el.dispatchEvent(new Event('input', { bubbles: true }));
              el.dispatchEvent(new Event('change', { bubbles: true }));
              el.dispatchEvent(new Event('click', { bubbles: true }));
              return el.checked ? 'js-set' : 'fail';
            }"""
        )
        if _ok(f"js ({mode})"):
            return True
    except Exception:
        pass

    return False


def _tick_refresh_pipeline_checkbox(page) -> bool:
    """
    Tick the boolean **Refresh pipeline** parameter (FPMS job).

    Jenkins 2.5x uses ``jenkins-form-item--tight`` without ``jenkins-form-label``; the real
    control is ``div[name="parameter"]`` + hidden ``Refresh pipeline``. Help may show
    ``Loading...`` until scripts finish — we settle, wait for spinner, then Playwright + JS.
    """
    extra = int(os.environ.get("FPMS_TICK_REFRESH_SETTLE_MS", "1200"))
    _safe_page_wait(page, max(0, extra))

    wait_help = int(os.environ.get("FPMS_TICK_REFRESH_HELP_MS", "22000"))
    _wait_refresh_pipeline_help_idle(page, timeout_ms=wait_help)
    _safe_page_wait(page, 250)

    cb = _refresh_pipeline_checkbox_locator(page)
    try:
        cb.wait_for(state="attached", timeout=25_000)
        if _tick_checkbox_playwright_then_js(cb, how='div[name="parameter"]'):
            return True
    except PlaywrightTimeout:
        pass
    except Exception as ex:
        print(f"⚠️ Refresh pipeline primary locator: {ex!r}", file=sys.stderr)

    row = page.locator(
        'div.jenkins-form-item:has(input[type="hidden"][name="name"][value="Refresh pipeline"])'
    ).first
    try:
        row.wait_for(state="attached", timeout=8_000)
        inner = row.locator(
            'span.jenkins-checkbox input[type="checkbox"], '
            'input[type="checkbox"][name="value"]'
        ).first
        if inner.count() > 0 and _tick_checkbox_playwright_then_js(
            inner, how="jenkins-form-item + hidden name"
        ):
            return True
    except Exception as ex:
        print(f"⚠️ Refresh pipeline row fallback: {ex!r}", file=sys.stderr)

    try:
        cb2 = page.get_by_role("checkbox", name=re.compile(r"^\s*refresh\s+pipeline\s*$", re.I))
        if cb2.count() > 0 and _tick_checkbox_playwright_then_js(cb2.first, how="role name"):
            return True
    except Exception:
        pass

    exact_labels = (
        "Refresh pipeline",
        "Refresh Pipeline",
        "refresh pipeline",
    )
    for lab in exact_labels:
        try:
            r = _form_row(page, lab).first
            r.wait_for(state="visible", timeout=4_000)
            inner = r.locator("input[type=checkbox]").first
            if inner.count() > 0 and _tick_checkbox_playwright_then_js(
                inner, how=f"form label {lab!r}"
            ):
                return True
        except Exception:
            continue

    try:
        row = _form_row_label_regex(
            page, re.compile(r"\brefresh\b.*\bpipeline\b|\bpipeline\b.*\brefresh\b", re.I)
        )
        row.wait_for(state="attached", timeout=6_000)
        inner = row.locator("input[type=checkbox]").first
        if inner.count() > 0 and _tick_checkbox_playwright_then_js(inner, how="label pattern"):
            return True
    except PlaywrightTimeout:
        print("⚠️ No Refresh pipeline checkbox found (skipped).", file=sys.stderr)
        return False
    except Exception as ex:
        print(f"⚠️ Could not tick Refresh pipeline: {ex!r}", file=sys.stderr)
        return False

    print(
        "⚠️ Refresh pipeline: found controls but checkbox stayed unchecked after all strategies.",
        file=sys.stderr,
    )
    return False


def _tick_update_all_services_checkbox(page) -> bool:
    """
    Tick the Jenkins **update all services** boolean (Active Choices / Stapler hidden ``name`` + checkbox).

    Hidden field value defaults to ``Update_All_Services``; override with ``JENKINS_UPDATE_ALL_STAPLER_NAME``.
    Uses the same ``div[name="parameter"]`` pattern as **Refresh pipeline**; falls back to the
    ``jenkins-form-item`` row when the primary locator fails.
    """
    extra = int(os.environ.get("FPMS_TICK_UPDATE_ALL_SETTLE_MS", "800"))
    _safe_page_wait(page, max(0, extra))
    st = _jenkins_update_all_stapler_name()

    cb = _stapler_boolean_checkbox_locator(page, st)
    try:
        cb.wait_for(state="attached", timeout=25_000)
        if _tick_checkbox_playwright_then_js(
            cb,
            how='div[name="parameter"]',
            log_label=st,
            try_label_attach_previous=False,
        ):
            return True
    except PlaywrightTimeout:
        pass
    except Exception as ex:
        print(f"⚠️ {st!r} (update-all) primary locator: {ex!r}", file=sys.stderr)

    try:
        row = _form_row_label_regex(
            page, re.compile(r"update[_\s]all[_\s]services", re.I)
        )
        row.wait_for(state="attached", timeout=8_000)
        inner = row.locator(
            'span.jenkins-checkbox input[type="checkbox"], '
            'input[type="checkbox"][name="value"]'
        ).first
        if inner.count() > 0 and _tick_checkbox_playwright_then_js(
            inner,
            how="jenkins-form-item (label pattern)",
            log_label=st,
            try_label_attach_previous=False,
        ):
            return True
    except Exception as ex:
        print(f"⚠️ {st!r} (update-all) row fallback: {ex!r}", file=sys.stderr)

    for lab in (st, "Update_All_Services", "Update All Services"):
        try:
            r = _form_row(page, lab).first
            r.wait_for(state="visible", timeout=4_000)
            inner = r.locator("input[type=checkbox]").first
            if inner.count() > 0 and _tick_checkbox_playwright_then_js(
                inner,
                how=f"form label {lab!r}",
                log_label=st,
                try_label_attach_previous=False,
            ):
                return True
        except Exception:
            continue

    print(
        f"❌ {st!r}: update-all checkbox not found or stayed unchecked after all strategies.",
        file=sys.stderr,
    )
    return False


def _click_jenkins_build_button(page) -> None:
    """Primary **Build** on the parameterized job form (Jenkins 2.x)."""
    print("→ Clicking Jenkins **Build**…")
    sticky = page.locator("#bottom-sticker")
    if sticky.count() > 0:
        btn = sticky.get_by_role(
            "button", name=re.compile(r"^\s*build\s*$", re.I)
        )
        if btn.count() > 0:
            b = btn.first
            b.wait_for(state="visible", timeout=30_000)
            b.click(timeout=45_000)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=60_000)
            except Exception:
                pass
            _safe_page_wait(page, 800)
            return
    btn = page.get_by_role("button", name=re.compile(r"^\s*build\s*$", re.I)).first
    btn.wait_for(state="visible", timeout=30_000)
    btn.click(timeout=45_000)
    try:
        page.wait_for_load_state("domcontentloaded", timeout=60_000)
    except Exception:
        pass
    _safe_page_wait(page, 800)


def _recover_services_not_found_sequence(
    page, username: str, password: str, *, build_url: str | None = None
) -> None:
    """
    Services missing (same browser tab):

    1. Open build-with-parameters URL (refresh) → login if needed.
    2. Tick **Refresh pipeline** → click **Build**.
    3. Wait ``FPMS_POST_BUILD_RECOVER_WAIT_MS`` (default 10 s).
    4. Open build URL again → login → settle ``FPMS_MS_POST_LOGIN_BEFORE_FORM`` — caller then
       retries Environment, Services, Branch, Version.
    """
    w_ms = max(0, _MS_POST_BUILD_RECOVER_WAIT_MS)
    w_sec = w_ms / 1000.0
    print(
        "\n→ Services 未找到：同一会话 recovery — 打开 build 页 → **re-login** → **Refresh pipeline** "
        f"→ **Build** → 等待 {w_sec:g}s → 再打开 build 页 → **re-login** → 随后重新填整条表单。"
    )
    print(
        "→ Services missing: same session — goto build URL, re-login, Refresh pipeline, Build, "
        f"wait {w_sec:g}s, goto build URL + re-login again, then refill (your prompts are unchanged)."
    )

    bu = (build_url or BUILD_URL).strip()
    print(f"→ Recovery (1/2): opening {bu}")
    page.goto(bu, wait_until="domcontentloaded", timeout=90_000)
    _safe_page_wait(page, 900)
    jenkins_login_if_needed(page, username, password)
    page.wait_for_selector("div.jenkins-form-item", timeout=60_000)
    _safe_page_wait(page, _MS_FORM_READY)

    ticked = _tick_refresh_pipeline_checkbox(page)
    if ticked:
        _verify_refresh_pipeline_checked(page)
    else:
        print(
            "⚠️ Refresh pipeline checkbox not ticked; continuing to Build anyway.",
            file=sys.stderr,
        )
    _click_jenkins_build_button(page)

    print(f"→ Post-Build wait: {w_ms} ms ({w_sec:g} s) before re-opening parameters…")
    time.sleep(w_sec)

    print(f"→ Recovery (2/2): opening {bu} again + re-login")
    page.goto(bu, wait_until="domcontentloaded", timeout=90_000)
    _safe_page_wait(page, 900)
    jenkins_login_if_needed(page, username, password)
    page.wait_for_selector("div.jenkins-form-item", timeout=60_000)
    _safe_page_wait(page, _MS_FORM_READY)
    print(
        f"→ Post-login: waiting {_MS_POST_LOGIN_BEFORE_FORM} ms before retrying Environment / Services…"
    )
    _safe_page_wait(page, _MS_POST_LOGIN_BEFORE_FORM)
    print("→ Recovery sequence done — retrying Environment + Services + Branch + Version.")


def normalize_parameter_text(value: str) -> str:
    """Strip leading/trailing whitespace on Branch / Version (e.g. ``\"  wad  \"`` → ``\"wad\"``)."""
    return (value or "").strip()


def fill_text_parameter(page, label: str, value: str) -> None:
    value = normalize_parameter_text(value)
    row = _form_row(page, label)
    row.wait_for(state="visible", timeout=30_000)
    inp = row.locator(
        "input.setting-input[type='text'], "
        "input.jenkins-input[type='text'], "
        "input[name='value'][type='text']"
    ).first
    inp.wait_for(state="visible", timeout=15_000)
    inp.click()
    inp.fill("")
    inp.fill(value)
    print(f"→ {label} filled in browser: {value!r}")


def _read_select_options(page, label: str) -> list[tuple[str, str]]:
    row = _form_row(page, label)
    row.wait_for(state="visible", timeout=30_000)
    sel = row.locator("select.jenkins-select__input, select").first
    sel.wait_for(state="attached", timeout=15_000)
    out = sel.evaluate(
        """el => {
            if (!el || el.tagName !== 'SELECT') return [];
            return Array.from(el.options || []).map(opt => [
                ((opt && opt.value) || '').trim(),
                ((opt && opt.textContent) || '').replace(/\\s+/g, ' ').trim(),
            ]);
        }"""
    )
    if not isinstance(out, list):
        return []
    rows: list[tuple[str, str]] = []
    for it in out:
        if isinstance(it, list) and len(it) >= 2:
            rows.append((normalize_parameter_text(str(it[0])), normalize_parameter_text(str(it[1]))))
    return rows


def read_choice_parameter_value(page, label: str) -> str:
    row = _form_row(page, label)
    sel = row.locator("select.jenkins-select__input, select").first
    sel.wait_for(state="attached", timeout=15_000)
    v = sel.evaluate(
        """el => {
            if (!el || el.tagName !== 'SELECT') return '';
            const oi = el.selectedIndex;
            if (oi < 0) return (el.value || '').trim();
            const opt = el.options[oi];
            return ((opt && opt.value) || el.value || '').trim();
        }"""
    )
    return normalize_parameter_text(str(v) if v is not None else "")


def _rank_bi_repository_options(
    want_value: str, options: list[tuple[str, str]]
) -> list[tuple[str, str, float]]:
    want_raw = normalize_parameter_text(want_value)
    want_cf = want_raw.casefold()
    want_can = _bi_repo_canonical(want_raw)
    ranked: list[tuple[str, str, float]] = []
    for ov, ot in options:
        ov_cf = ov.casefold()
        ot_cf = ot.casefold()
        ov_can = _bi_repo_canonical(ov)
        ot_can = _bi_repo_canonical(ot)
        if ov_cf == want_cf or ot_cf == want_cf:
            sc = 10.0
        elif want_can and (ov_can == want_can or ot_can == want_can):
            sc = 8.0
        else:
            sc = max(
                difflib.SequenceMatcher(None, want_can, ov_can).ratio(),
                difflib.SequenceMatcher(None, want_can, ot_can).ratio(),
                difflib.SequenceMatcher(None, want_cf, ov_cf).ratio(),
                difflib.SequenceMatcher(None, want_cf, ot_cf).ratio(),
            )
        ranked.append((ov, ot, sc))
    ranked.sort(key=lambda x: (-x[2], x[0]))
    return ranked


def _choose_bi_repository_option_value(
    want_value: str, options: list[tuple[str, str]]
) -> str:
    """
    Resolve BI REPOSITORY option:
      - exact value/text match → direct
      - otherwise show near matches and let user choose (TTY), or auto-pick top in non-interactive mode.
    """
    want = normalize_parameter_text(want_value)
    if not want:
        raise ValueError("REPOSITORY: requested value is empty.")
    for ov, ot in options:
        if ov.casefold() == want.casefold() or ot.casefold() == want.casefold():
            return ov
    ranked = _rank_bi_repository_options(want, options)
    if not ranked:
        raise ValueError("REPOSITORY: no options available on Jenkins page.")
    top = ranked[: min(8, len(ranked))]
    if not _stdin_stdout_interactive():
        pick = top[0][0]
        print(
            "⚠️ REPOSITORY is not an exact match; non-interactive mode auto-picked nearest "
            f"{pick!r} for input {want!r}."
        )
        return pick
    print(
        "\nREPOSITORY did not match exactly. Choose the closest Jenkins option:"
        f"\nInput: {want!r}"
    )
    for i, (ov, ot, _sc) in enumerate(top, start=1):
        show = f"{ov} ({ot})" if ot and ot != ov else ov
        print(f"  {i}. {show}")
    n = len(top)
    while True:
        raw = input(f"> Pick one number 1-{n} (or type 'cancel'): ").strip()
        if raw.casefold() in ("cancel", "c", "q", "quit", "exit"):
            raise RuntimeError("Cancelled while choosing BI REPOSITORY option.")
        idx = _parse_single_menu_index(raw, n)
        if idx is None:
            print(f"  Enter one number from 1 to {n}, or type cancel.")
            continue
        pick = top[idx - 1][0]
        print(f"  → Selected repository option: {pick}")
        return pick


def select_choice_parameter_by_value(
    page,
    label: str,
    want_value: str,
    *,
    normalize_for_match=None,
) -> str:
    """
    Select a Jenkins <select> parameter by value/text.
    Returns the selected option value (useful when aliases are auto-mapped).
    """
    want = normalize_parameter_text(want_value)
    if not want:
        raise ValueError(f"{label}: requested value is empty.")
    options = _read_select_options(page, label)
    if not options:
        raise RuntimeError(f"{label}: no options found on page.")

    def _norm(v: str) -> str:
        if normalize_for_match is None:
            return normalize_parameter_text(v).casefold()
        return normalize_parameter_text(str(normalize_for_match(v))).casefold()

    want_cf = want.casefold()
    want_n = _norm(want)
    picked_value: str | None = None
    for ov, ot in options:
        if ov.casefold() == want_cf or ot.casefold() == want_cf:
            picked_value = ov
            break
    if picked_value is None:
        for ov, ot in options:
            if _norm(ov) == want_n or _norm(ot) == want_n:
                picked_value = ov
                break
    if picked_value is None:
        shown = ", ".join(f"{v} ({t})" if t and t != v else v for v, t in options)
        raise ValueError(
            f"{label}: could not map {want_value!r} to Jenkins option. Available: {shown}"
        )

    row = _form_row(page, label)
    row.wait_for(state="visible", timeout=30_000)
    sel = row.locator("select.jenkins-select__input, select").first
    sel.wait_for(state="attached", timeout=15_000)
    sel.select_option(value=picked_value)
    _safe_page_wait(page, max(120, _MS_ENV_SETTLE))
    got = read_choice_parameter_value(page, label)
    if got != picked_value:
        raise RuntimeError(
            f"{label} select mismatch: page={got!r} expected={picked_value!r} (from {want_value!r})"
        )
    print(f"→ {label} selected in browser: {got!r} (input: {want_value!r})")
    return got


def select_environment_by_value(page, env_value: str) -> None:
    """Select Environment by option value (works for fpms-prod single-option jobs too)."""
    select_choice_parameter_by_value(page, "Environment", env_value)


def read_environment_value(page) -> str:
    return read_choice_parameter_value(page, "Environment")


def read_services_checked_values(page) -> list[str]:
    """Service ``value`` / ``json`` for each checked checkbox under the Services UnoChoice root."""
    out = page.evaluate(
        "() => {\n"
        + _SERVICES_UNOCHOICE_JS_FN
        + r"""
        const root = __fpmsServicesCheckboxRoot();
        if (!root) return [];
        const acc = [];
        for (const el of root.querySelectorAll('input[type="checkbox"]')) {
            if (!el.checked) continue;
            const v = (el.getAttribute('value') || el.getAttribute('json') || '').trim();
            if (v) acc.push(v);
        }
        return acc;
    }"""
    )
    if not isinstance(out, list):
        return []
    return [normalize_parameter_text(str(x)) for x in out if str(x).strip()]


def read_fnt_rc_services_checked_values(page) -> list[str]:
    """Checked service ids under **Services** for FNT ECP extended-choice (``tbl_ecp_choice-parameter-*``)."""
    out = page.evaluate(
        r"""() => {
            const items = document.querySelectorAll("div.jenkins-form-item");
            for (const item of items) {
                const lab = item.querySelector(".jenkins-form-label");
                if (!lab) continue;
                const t = (lab.textContent || "").replace(/\s+/g, " ").trim();
                if (!/^Services$/i.test(t)) continue;
                const acc = [];
                for (const el of item.querySelectorAll(
                    'div[id^="tbl_ecp_choice-parameter"] input[type="checkbox"]'
                )) {
                    if (!el.checked) continue;
                    const v = (el.getAttribute("value") || el.getAttribute("json") || "").trim();
                    if (v) acc.push(v);
                }
                return acc;
            }
            return [];
        }"""
    )
    if not isinstance(out, list):
        return []
    return [normalize_parameter_text(str(x)) for x in out if str(x).strip()]


def select_fnt_rc_services(page, service_names: list[str]) -> None:
    """
    Tick **Services** for FNT RC jobs (``RC-UAT-UPDATE`` / ``FNT_UAT_SCRIPT_RUN``) — ECP extended-choice, not FPMS UnoChoice.
    """
    cleaned: list[str] = []
    for name in service_names:
        n = (name or "").strip()
        if not re.match(r"^[\w.-]+$", n):
            raise ValueError(f"Invalid service name: {n!r}")
        cleaned.append(n)
    row = _form_row(page, "Services")
    row.wait_for(state="visible", timeout=30_000)
    root = row.locator('div[id^="tbl_ecp_choice-parameter"]')
    try:
        root.first.wait_for(state="visible", timeout=45_000)
    except PlaywrightTimeout as ex:
        raise ServicesListGoneError(
            "FNT RC Services (ECP table) not visible — wrong job page or Jenkins UI changed."
        ) from ex
    gap = min(120, _MS_BETWEEN_SERVICES) if _FPMS_FAST_FILL_ACTIVE else _MS_BETWEEN_SERVICES
    for n in cleaned:
        cb = root.locator(
            f'input[type="checkbox"][value="{n}"], input[type="checkbox"][json="{n}"]'
        ).first
        try:
            cb.wait_for(state="attached", timeout=25_000)
        except PlaywrightTimeout as ex:
            raise ServiceNotDetectedError(
                f"FNT RC: no Services checkbox for {n!r} (value/json must match Jenkins)."
            ) from ex
        try:
            if not cb.is_checked():
                cb.scroll_into_view_if_needed()
                _safe_page_wait(page, 80)
                cb.click(timeout=15_000)
        except Exception as ex:
            raise ServiceNotDetectedError(f"FNT RC: could not tick {n!r}: {ex!r}") from ex
        print(f"→ FNT RC service ticked: {n!r}")
        _safe_page_wait(page, gap)


def _read_services_checked_values_wide(page) -> list[str]:
    """
    Checked service ids under the whole **Services** ``jenkins-form-item`` (not only ``.dynamic_checkbox``).

    When UnoChoice clears the inner list, inputs sometimes remain attached on the row — used to detect
    “UI gone but selections already applied” and continue without burning ``FPMS_SERVICES_STABLE_MS``.
    """
    out = page.evaluate(
        "() => {\n"
        + _SERVICES_UNOCHOICE_JS_FN
        + r"""
        return __fpmsServicesCheckedListWide();
    }"""
    )
    if not isinstance(out, list):
        return []
    return [normalize_parameter_text(str(x)) for x in out if str(x).strip()]


def _services_requested_satisfied_wide(page, names: list[str]) -> bool:
    """True iff every non-empty normalized name appears checked somewhere on the Services form row."""
    if not _SERVICES_UI_EMPTY_OK:
        return False
    want = {normalize_parameter_text(n) for n in (names or []) if normalize_parameter_text(n)}
    if not want:
        return False
    got = set(_read_services_checked_values_wide(page))
    return want <= got


def _service_line_checked(page, name: str) -> bool:
    """Narrow UnoChoice root ``.checked``, else wide row scan when ``FPMS_SERVICES_UI_EMPTY_OK``."""
    if _service_checked_js(page, name):
        return True
    if not _SERVICES_UI_EMPTY_OK:
        return False
    vn = normalize_parameter_text(name)
    return bool(vn) and vn in set(_read_services_checked_values_wide(page))


def read_text_parameter_value(page, label: str) -> str:
    row = _form_row(page, label)
    inp = row.locator(
        "input.setting-input[type='text'], "
        "input.jenkins-input[type='text'], "
        "input[name='value'][type='text']"
    ).first
    inp.wait_for(state="attached", timeout=15_000)
    return normalize_parameter_text(inp.input_value() or "")


def verify_fpms_parameters_display(
    page,
    environment: str,
    services_expected: list[str],
    branch_expected: str,
    version_expected: str,
    *,
    update_all_services: bool = False,
) -> tuple[bool, list[str]]:
    """
    Re-read Environment, Services, Branch, Version from the page and compare to intended values.
    Returns ``(all_ok, lines)`` for terminal display (leading emoji ✅ / ❌).

    When ``update_all_services`` is True, the Services line checks **Update_All_Services** only
    (the per-service checkbox list is not compared to ``services_expected``).
    """
    want_env = normalize_parameter_text(environment)
    want_br = normalize_parameter_text(branch_expected)
    want_ver = normalize_parameter_text(version_expected)
    want_svc = sorted(
        {normalize_parameter_text(s) for s in (services_expected or []) if normalize_parameter_text(s)}
    )

    lines: list[str] = []
    ok_all = True

    try:
        got_env = read_environment_value(page)
    except Exception as ex:
        got_env = f"(read failed: {ex})"
        env_ok = False
    else:
        env_ok = got_env == want_env
    ok_all = ok_all and env_ok
    em = "✅" if env_ok else "❌"
    lines.append(f"{em} Environment — page: {got_env!r} — expected: {want_env!r}")

    if update_all_services:
        st_name = _jenkins_update_all_stapler_name()
        u_all = _read_stapler_boolean_checkbox_checked(page, st_name)
        svc_ok = u_all
        ok_all = ok_all and svc_ok
        em = "✅" if svc_ok else "❌"
        lines.append(
            f"{em} Services — {st_name} is **{'checked' if u_all else 'not checked'}** "
            "(per-service list not verified in this mode)"
        )
    else:
        try:
            got_svc = sorted(set(read_services_checked_values(page)))
        except Exception as ex:
            got_svc = []
            lines.append(f"❌ Services — read failed: {ex!r}")
            ok_all = False
        else:
            svc_ok = got_svc == want_svc
            ok_all = ok_all and svc_ok
            em = "✅" if svc_ok else "❌"
            if svc_ok:
                lines.append(
                    f"{em} Services — {len(want_svc)} checked on page, matches: {', '.join(want_svc)}"
                )
            else:
                missing = [x for x in want_svc if x not in got_svc]
                extra = [x for x in got_svc if x not in want_svc]
                lines.append(
                    f"{em} Services — page checked ({len(got_svc)}): {', '.join(got_svc) or '(none)'} "
                    f"| expected: {', '.join(want_svc)}"
                )
                if missing:
                    lines.append(f"   … missing on page: {', '.join(missing)}")
                if extra:
                    lines.append(f"   … extra on page: {', '.join(extra)}")

    try:
        got_br = read_text_parameter_value(page, "Branch")
    except Exception as ex:
        got_br = f"(read failed: {ex})"
        br_ok = False
    else:
        br_ok = got_br.casefold() == want_br.casefold()
    ok_all = ok_all and br_ok
    em = "✅" if br_ok else "❌"
    lines.append(f"{em} Branch — page: {got_br!r} — expected: {want_br!r}")

    try:
        got_ver = read_text_parameter_value(page, "Version")
    except Exception as ex:
        got_ver = f"(read failed: {ex})"
        ver_ok = False
    else:
        ver_ok = got_ver == want_ver
    ok_all = ok_all and ver_ok
    em = "✅" if ver_ok else "❌"
    lines.append(f"{em} Version — page: {got_ver!r} — expected: {want_ver!r}")

    return ok_all, lines


def verify_fnt_rc_parameters_display(
    page,
    services_expected: list[str],
    branch_expected: str,
    version_expected: str,
    *,
    update_all_services: bool = False,
) -> tuple[bool, list[str]]:
    """Re-read Services, Branch, Version (no Environment) for FNT RC / SMS-style ECP jobs."""
    want_br = normalize_parameter_text(branch_expected)
    want_ver = normalize_parameter_text(version_expected)
    want_svc = sorted(
        {normalize_parameter_text(s) for s in (services_expected or []) if normalize_parameter_text(s)}
    )
    lines: list[str] = []
    ok_all = True
    if update_all_services:
        st_name = _jenkins_update_all_stapler_name()
        u_all = _read_stapler_boolean_checkbox_checked(page, st_name)
        svc_ok = u_all
        ok_all = ok_all and svc_ok
        em = "✅" if svc_ok else "❌"
        lines.append(
            f"{em} Services — {st_name} is **{'checked' if u_all else 'not checked'}** "
            "(per-service list not verified in this mode)"
        )
    else:
        try:
            got_svc = sorted(set(read_fnt_rc_services_checked_values(page)))
        except Exception as ex:
            got_svc = []
            lines.append(f"❌ Services — read failed: {ex!r}")
            ok_all = False
        else:
            svc_ok = got_svc == want_svc
            ok_all = ok_all and svc_ok
            em = "✅" if svc_ok else "❌"
            if svc_ok:
                lines.append(
                    f"{em} Services — {len(want_svc)} checked on page, matches: {', '.join(want_svc)}"
                )
            else:
                missing = [x for x in want_svc if x not in got_svc]
                extra = [x for x in got_svc if x not in want_svc]
                lines.append(
                    f"{em} Services — page checked ({len(got_svc)}): {', '.join(got_svc) or '(none)'} "
                    f"| expected: {', '.join(want_svc)}"
                )
                if missing:
                    lines.append(f"   … missing on page: {', '.join(missing)}")
                if extra:
                    lines.append(f"   … extra on page: {', '.join(extra)}")
    try:
        got_br = read_text_parameter_value(page, "Branch")
    except Exception as ex:
        got_br = f"(read failed: {ex})"
        br_ok = False
    else:
        br_ok = got_br.casefold() == want_br.casefold()
    ok_all = ok_all and br_ok
    em = "✅" if br_ok else "❌"
    lines.append(f"{em} Branch — page: {got_br!r} — expected: {want_br!r}")
    try:
        got_ver = read_text_parameter_value(page, "Version")
    except Exception as ex:
        got_ver = f"(read failed: {ex})"
        ver_ok = False
    else:
        ver_ok = got_ver == want_ver
    ok_all = ok_all and ver_ok
    em = "✅" if ver_ok else "❌"
    lines.append(f"{em} Version — page: {got_ver!r} — expected: {want_ver!r}")
    return ok_all, lines


def verify_fpms_prod_script_parameters_display(
    page,
    environment_expected: str,
    command_expected: str,
) -> tuple[bool, list[str]]:
    """Re-read Environment + Command for FPMS PROD SCRIPT RUN."""
    want_env = normalize_parameter_text(environment_expected)
    want_cmd = normalize_parameter_text(command_expected)
    lines: list[str] = []
    ok_all = True

    try:
        got_env = read_environment_value(page)
    except Exception as ex:
        got_env = f"(read failed: {ex})"
        env_ok = False
    else:
        env_ok = got_env == want_env
    ok_all = ok_all and env_ok
    lines.append(f"{'✅' if env_ok else '❌'} Environment — page: {got_env!r} — expected: {want_env!r}")

    try:
        got_cmd = read_text_parameter_value(page, "Command")
    except Exception as ex:
        got_cmd = f"(read failed: {ex})"
        cmd_ok = False
    else:
        cmd_ok = got_cmd == want_cmd
    ok_all = ok_all and cmd_ok
    em = "✅" if cmd_ok else "❌"
    lines.append(
        f"{em} Command\n\n"
        f"Page: {got_cmd!r}\n\n"
        f"Expected: {want_cmd!r}"
    )
    return ok_all, lines


def verify_bi_api_update_parameters_display(
    page,
    repository_expected: str,
    environment_expected: str,
    source_branch_expected: str,
) -> tuple[bool, list[str]]:
    """Re-read BI-API-UPDATE fields (REPOSITORY / ENVIRONMENT / SOURCE_BRANCH)."""
    want_repo = normalize_parameter_text(repository_expected)
    want_env = normalize_parameter_text(environment_expected).casefold()
    want_branch = normalize_parameter_text(source_branch_expected)
    lines: list[str] = []
    ok_all = True

    try:
        got_repo = read_choice_parameter_value(page, "REPOSITORY")
    except Exception as ex:
        got_repo = f"(read failed: {ex})"
        repo_ok = False
    else:
        repo_ok = _bi_repo_canonical(got_repo) == _bi_repo_canonical(want_repo)
    ok_all = ok_all and repo_ok
    lines.append(f"{'✅' if repo_ok else '❌'} Repository — page: {got_repo!r} — expected: {want_repo!r}")

    try:
        got_env = read_choice_parameter_value(page, "ENVIRONMENT")
    except Exception as ex:
        got_env = f"(read failed: {ex})"
        env_ok = False
    else:
        env_ok = got_env.casefold() == want_env
    ok_all = ok_all and env_ok
    lines.append(f"{'✅' if env_ok else '❌'} Environment — page: {got_env!r} — expected: {want_env!r}")

    try:
        got_branch = read_text_parameter_value(page, "SOURCE_BRANCH")
    except Exception as ex:
        got_branch = f"(read failed: {ex})"
        branch_ok = False
    else:
        branch_ok = got_branch.casefold() == want_branch.casefold()
    ok_all = ok_all and branch_ok
    lines.append(
        f"{'✅' if branch_ok else '❌'} Source Branch — page: {got_branch!r} — expected: {want_branch!r}"
    )
    return ok_all, lines


def prompt_yes_to_click_build_bi_api_update(
    page,
    repository: str,
    environment: str,
    source_branch: str,
) -> bool:
    """Terminal yes/no gate for BI-API-UPDATE."""
    print(
        "\n—— 终端核对 / Terminal ——\n"
        "若上面全部为 ✅，输入 **yes** 后脚本会点击 Jenkins **Build**。\n"
        "若有 ❌，请先在浏览器里改对参数，再在此输入 **yes**（会重新读页面）；输入 **no** 不点 Build。\n"
        "EN: Type **yes** to click **Build** once every line is ✅ (re-reads the page each time). "
        "**no** skips Build."
    )
    while True:
        raw = input("> yes / no: ").strip().casefold()
        if raw in ("no", "n"):
            return False
        if raw in ("yes", "y"):
            ok, lines = verify_bi_api_update_parameters_display(
                page, repository, environment, source_branch
            )
            print("\n→ Re-check before Build:")
            for ln in lines:
                print("  ", ln)
            if not ok:
                print(
                    "  ⚠️ 仍有 ❌，未点击 Build。请修正浏览器中的参数后再输入 yes，或输入 no。\n"
                    "  EN: Still ❌ — Build not clicked. Fix the form, then **yes** again, or **no**."
                )
                continue
            return True
        print("  Please type **yes** or **no**.")


def prompt_yes_to_click_build(
    page,
    environment: str,
    services: list[str],
    branch: str,
    version: str,
    *,
    update_all_services: bool = False,
) -> bool:
    """
    Ask **yes** / **no** in the terminal. **yes** is accepted only after a fresh re-read shows all ✅
    (so you can fix the browser and retry). **yes** → click **Build**; **no** → skip Build.
    """
    print(
        "\n—— 终端核对 / Terminal ——\n"
        "若上面全部为 ✅，输入 **yes** 后脚本会点击 Jenkins **Build**。\n"
        "若有 ❌，请先在浏览器里改对参数，再在此输入 **yes**（会重新读页面）；输入 **no** 不点 Build。\n"
        "EN: Type **yes** to click **Build** once every line is ✅ (re-reads the page each time). "
        "**no** skips Build."
    )
    while True:
        raw = input("> yes / no: ").strip().casefold()
        if raw in ("no", "n"):
            return False
        if raw in ("yes", "y"):
            ok, lines = verify_fpms_parameters_display(
                page,
                environment,
                services,
                branch,
                version,
                update_all_services=update_all_services,
            )
            print("\n→ Re-check before Build:")
            for ln in lines:
                print("  ", ln)
            if not ok:
                print(
                    "  ⚠️ 仍有 ❌，未点击 Build。请修正浏览器中的参数后再输入 yes，或输入 no。\n"
                    "  EN: Still ❌ — Build not clicked. Fix the form, then **yes** again, or **no**."
                )
                continue
            return True
        print("  Please type **yes** or **no**.")


def prompt_yes_to_click_build_prod_script(
    page,
    environment: str,
    command: str,
) -> bool:
    """
    Terminal yes/no gate for FPMS PROD SCRIPT RUN.
    """
    print(
        "\n—— 终端核对 / Terminal ——\n"
        "若上面全部为 ✅，输入 **yes** 后脚本会点击 Jenkins **Build**。\n"
        "若有 ❌，请先在浏览器里改对参数，再在此输入 **yes**（会重新读页面）；输入 **no** 不点 Build。"
    )
    while True:
        raw = input("> yes / no: ").strip().casefold()
        if raw in ("no", "n"):
            return False
        if raw in ("yes", "y"):
            ok, lines = verify_fpms_prod_script_parameters_display(
                page, environment, command
            )
            print("\n→ Re-check before Build:")
            for ln in lines:
                print("  ", ln)
            if not ok:
                print(
                    "  ⚠️ 仍有 ❌，未点击 Build。请修正浏览器中的参数后再输入 yes，或输入 no。"
                )
                continue
            return True
        print("  Please type **yes** or **no**.")


def wait_review(seconds: float, *, build_was_clicked: bool = False) -> None:
    s = max(0, int(seconds))
    if build_was_clicked:
        print(
            f"\n→ AFK {s}s — Build was already clicked; **no further Jenkins clicks** in this period."
        )
    else:
        print(
            f"\n→ AFK {s}s for review — **no further Jenkins clicks** in this period."
        )
    time.sleep(s)


# ----- Lark / Chat bot: /jenkinsupdate (job match → optional FPMS parameter flow → yes → Jenkins) -----
# Interactive cards use **卡片 JSON 2.0** + ``behaviors[type=callback]`` (legacy ``tag: action`` is
# deprecated). Clicks arrive as ``card.action.trigger`` — subscribe in the console and use request URL
# ``/webhook/event``. Users can still type **yes** / **no** / **1** as before.
_fpms_lark_sessions_lock = threading.Lock()
_fpms_lark_sessions: dict[str, dict] = {}

# Job-picker interactive cards: bind button taps to the session row even when ``card.action`` ``operator``
# ids do not match the keys used for ``im.message.receive_v1`` (common in groups / mixed open_id vs union_id).
_fpms_lark_picker_sid_lock = threading.Lock()
_fpms_lark_picker_sid_to_session_key: dict[str, str] = {}

# Set by :func:`handle_lark_jenkins_update_message` so session rows can be keyed by **union_id**
# alias (card callbacks sometimes send ``operator.union_id`` without ``open_id``).
_fpms_lark_sender_union_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "fpms_lark_sender_union_id", default=None
)


def _fpms_lark_session_key(chat_id: str, sender_id: str) -> str:
    return f"{chat_id}:{sender_id}"


def _fpms_lark_unregister_picker_sid_from_sess(sess: dict) -> None:
    ps = str(sess.get("picker_sid") or "").strip()
    if not ps:
        return
    with _fpms_lark_picker_sid_lock:
        _fpms_lark_picker_sid_to_session_key.pop(ps, None)


def _fpms_lark_register_picker_sid(picker_sid: str, session_key: str) -> None:
    ps = (picker_sid or "").strip()
    sk = (session_key or "").strip()
    if not ps or not sk or ":" not in sk:
        return
    with _fpms_lark_picker_sid_lock:
        _fpms_lark_picker_sid_to_session_key[ps] = sk


def resolve_jenkins_job_card_session(chat_id: str, picker_sid: object) -> tuple[str, str] | None:
    """
    Map ``picker_sid`` from a job-picker card button to ``(chat_id, sender_id)`` for :data:`_fpms_lark_sessions`.

    ``chat_id`` must match the callback (same chat as the card).
    """
    ps = str(picker_sid or "").strip()
    if not ps:
        return None
    with _fpms_lark_picker_sid_lock:
        sk = _fpms_lark_picker_sid_to_session_key.get(ps)
    if not sk or ":" not in sk:
        return None
    cchat, sender = sk.split(":", 1)
    if (chat_id or "").strip() != (cchat or "").strip():
        return None
    return (cchat, sender)


def _fpms_lark_sessions_put(
    chat_id: str,
    sender_open_id: str,
    sess: dict,
    sender_union_id: str | None = None,
) -> None:
    """Store session under ``chat_id:open_id`` and optionally ``chat_id:union_id`` (same dict)."""
    uid = (sender_union_id if sender_union_id is not None else _fpms_lark_sender_union_id.get()) or None
    uid = (uid or "").strip() or None
    ou = (sender_open_id or "").strip()
    if ou:
        sess["_lark_open_id"] = ou
    if uid:
        sess["_lark_union_id"] = uid
    if not ou:
        with _fpms_lark_sessions_lock:
            prev_u = _fpms_lark_sessions.get(_fpms_lark_session_key(chat_id, uid or ""))
            if isinstance(prev_u, dict):
                _fpms_lark_unregister_picker_sid_from_sess(prev_u)
            _fpms_lark_sessions[_fpms_lark_session_key(chat_id, uid or "")] = sess
        return
    key_ou = _fpms_lark_session_key(chat_id, ou)
    with _fpms_lark_sessions_lock:
        prev = _fpms_lark_sessions.get(key_ou)
        if isinstance(prev, dict):
            _fpms_lark_unregister_picker_sid_from_sess(prev)
        _fpms_lark_sessions[key_ou] = sess
        if uid and uid != ou:
            _fpms_lark_sessions[_fpms_lark_session_key(chat_id, uid)] = sess


def _fpms_lark_sessions_put_chat_key(session_key: str, sess: dict) -> None:
    """Parse ``chat_id:sender_open_id`` from ``session_key`` (see :func:`_fpms_lark_session_key`)."""
    if ":" not in session_key:
        with _fpms_lark_sessions_lock:
            _fpms_lark_sessions[session_key] = sess
        return
    chat_id, open_id = session_key.split(":", 1)
    _fpms_lark_sessions_put(chat_id, open_id, sess)


def resolve_lark_jenkins_card_sender(
    chat_id: str,
    extracted_sender_id: str,
    operator: object,
) -> str:
    """
    Pick ``open_id`` or ``union_id`` so the ID matches an existing ``/jenkinsupdate`` session row.
    Feishu sometimes omits ``operator.open_id`` but sends ``union_id``, while IM events keyed the
    session with ``open_id``.
    """
    op = operator if isinstance(operator, dict) else {}
    cand = [
        (extracted_sender_id or "").strip(),
        (op.get("open_id") or "").strip(),
        (op.get("union_id") or "").strip(),
    ]
    cand = [c for c in cand if c]
    with _fpms_lark_sessions_lock:
        for c in cand:
            if _fpms_lark_session_key(chat_id, c) in _fpms_lark_sessions:
                return c
    return cand[0] if cand else ""


def jenkins_update_has_active_lark_session(chat_id: str, sender_id: str) -> bool:
    with _fpms_lark_sessions_lock:
        return _fpms_lark_session_key(chat_id, sender_id) in _fpms_lark_sessions


def _fpms_lark_clear_session(chat_id: str, sender_id: str) -> None:
    with _fpms_lark_sessions_lock:
        k = _fpms_lark_session_key(chat_id, sender_id)
        sess = _fpms_lark_sessions.pop(k, None)
        if sess is None or not isinstance(sess, dict):
            return
        _fpms_lark_unregister_picker_sid_from_sess(sess)
        ou = sess.get("_lark_open_id")
        uid = sess.get("_lark_union_id")
        for sid in (ou, uid):
            if not sid:
                continue
            kk = _fpms_lark_session_key(chat_id, str(sid))
            if kk != k and _fpms_lark_sessions.get(kk) is sess:
                _fpms_lark_sessions.pop(kk, None)


def _fpms_lark_clear_session_key(session_key: str) -> None:
    if ":" not in session_key:
        with _fpms_lark_sessions_lock:
            _fpms_lark_sessions.pop(session_key, None)
        return
    chat_id, sender_id = session_key.split(":", 1)
    _fpms_lark_clear_session(chat_id, sender_id)


def _jenkins_update_primary_url(raw: str) -> str:
    return (raw or "").strip().splitlines()[0].strip()


def _jenkins_update_job_url_is_fpms_uat_branch_form(raw_urls: str) -> bool:
    """True only for ``…/job/FPMS/job/FPMS_UAT_BRANCH_UPDATE/…`` (Playwright parameter fill)."""
    return _jenkins_update_job_automation_profile(raw_urls) == "fpms"


def _jenkins_update_job_automation_profile(raw_urls: str) -> str | None:
    """
    Which automated fill path applies to this Jenkins URL (first line if several).

    Returns ``\"fpms\"`` | ``\"fnt_rc\"`` | ``\"sms_uat\"`` | ``\"fpms_prod_script\"`` |
    ``\"bi_api_update\"`` or ``None``.
    """
    u = _jenkins_update_primary_url(raw_urls).replace("\\", "/")
    ul = u.casefold()
    if "/job/fpms/job/fpms_uat_branch_update/" in ul:
        return "fpms"
    if "/job/fpms/view/fpms-uat/job/fpms_uat_master_update/" in ul:
        return "fpms"
    if "/job/fpms_nt/view/all/job/fpms_nt_uat_master_update/" in ul:
        return "fpms"
    if "/job/fnt/job/fnt_uat_script_run/" in ul or "/job/fnt/job/rc-uat-update/" in ul:
        return "fnt_rc"
    if "/job/sms/job/uat/job/sms-uat-update/" in ul:
        return "sms_uat"
    if "/job/fpms/job/fpms_prod_script_run/" in ul:
        return "fpms_prod_script"
    if "/job/bi-go/job/bi-api-update/" in ul:
        return "bi_api_update"
    return None


def _jenkins_update_first_non_empty_line(body: str) -> str:
    """First non-empty line (headline before ``Branch:`` / ``Services:`` blocks)."""
    for line in (body or "").splitlines():
        t = line.strip()
        if t:
            return t
    return (body or "").strip()


def _jenkins_update_job_score(query_text: str, alias: str) -> float:
    q = JENKINS_UPDATE_CMD_RE.sub("", (query_text or ""), count=1).strip().casefold()
    a = (alias or "").strip().casefold()
    if not q or not a:
        return 0.0
    if a in q:
        return 2.0 + 10.0 / (1.0 + float(q.index(a)))
    best = difflib.SequenceMatcher(None, q, a).ratio()
    for chunk in re.split(r"[\s:：,，;+]+", q):
        c = chunk.strip()
        if len(c) < 2:
            continue
        best = max(best, difflib.SequenceMatcher(None, c, a).ratio())
        if a in c:
            best = max(best, 1.3)
    return best


def _rank_jenkins_update_job_matches(query_text: str) -> list[tuple[str, float, str, str]]:
    """Best-first rows: ``(alias_key, score, label, url_raw)``."""
    scored: list[tuple[str, float, str, str]] = []
    for alias, (label, url) in JENKINS_UPDATE_JOB_REGISTRY.items():
        sc = _jenkins_update_job_score(query_text, alias)
        scored.append((alias, sc, label, url))
    scored.sort(key=lambda x: (-x[1], x[0]))
    return scored


def _jenkins_update_disambiguation_ties(
    ranked: list[tuple[str, float, str, str]], *, band: float = 0.05, cap: int = 8
) -> list[tuple[str, float, str, str]]:
    if not ranked:
        return []
    best_sc = ranked[0][1]
    if best_sc < 0.28:
        return []
    return [r for r in ranked if r[1] >= best_sc - band][:cap]


def _fpms_lark_normalize_card_action_value(value: object) -> dict[str, object] | None:
    """Feishu may send ``value`` as JSON object or string."""
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            o = json.loads(s)
            return o if isinstance(o, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _fpms_lark_v2_callback_payload_strings(payload: dict[str, object]) -> dict[str, object]:
    """
    Lark/OpenAPI docs recommend callback ``behaviors[].value`` as **object**; SDK notes imply all scalar
    values as strings improve compatibility (avoid number coercion bugs on some clients).
    """
    out: dict[str, object] = {}
    for key, val in payload.items():
        ks = str(key)
        if isinstance(val, (dict, list)):
            out[ks] = val
        elif val is None:
            out[ks] = ""
        else:
            out[ks] = str(val)
    return out


def _fpms_lark_v2_callback_button(
    label: str,
    btn_type: str,
    payload: dict[str, object],
    *,
    element_id: str | None = None,
) -> dict[str, object]:
    """
    Lark **卡片 JSON 2.0** button: ``behaviors[type=callback]`` is required; legacy ``tag: action``
    rows are deprecated and often yield client ``code: undefined`` on tap (see open.larksuite.com
    card-json-v2 / button docs).

    ``element_id`` is optional but recommended (≤20 chars, letter-leading — fixes routing on some builds).
    """
    cb_val = _fpms_lark_v2_callback_payload_strings(payload)
    btn: dict[str, object] = {
        "tag": "button",
        "text": {"tag": "plain_text", "content": label},
        "type": btn_type,
        "behaviors": [{"type": "callback", "value": cb_val}],
    }
    eid = (element_id or "").strip()
    if eid:
        btn["element_id"] = eid
    return btn


def _fpms_lark_v2_column_set_button_row(
    buttons: list[dict[str, object]],
) -> dict[str, object]:
    """One horizontal row of buttons (up to 5) using ``column_set`` + ``flex_mode: flow``."""
    columns: list[dict[str, object]] = []
    for b in buttons:
        columns.append(
            {
                "tag": "column",
                # Match Lark JSON 2.0 examples ("auto"); "weighted" has caused tap/callback issues on some clients.
                "width": "auto",
                "weight": 1,
                "vertical_align": "top",
                "elements": [b],
            }
        )
    return {
        "tag": "column_set",
        "flex_mode": "flow",
        "background_style": "default",
        "horizontal_spacing": "8px",
        "columns": columns,
    }


def _fpms_lark_job_choice_card_json(
    candidates: list[tuple[str, float, str, str]],
    *,
    picker_sid: str | None = None,
) -> str:
    """Lark ``msg_type=interactive``: JSON **2.0** card — numbered body + rows of digit buttons + Cancel."""
    ps = (picker_sid or "").strip()
    lines_md: list[str] = [
        "Several Jenkins jobs match your text. **Tap a number** below to choose, or **Cancel**.",
        "",
    ]
    buttons: list[dict[str, object]] = []
    for i, (alias, _sc, label, url_raw) in enumerate(candidates, start=1):
        u0 = _jenkins_update_primary_url(url_raw)
        lines_md.append(f"**{i}.** **{label}** — `{alias}` — {u0}")
        payload: dict[str, object] = {"k": "job", "i": i}
        if ps:
            payload["sid"] = ps
        buttons.append(
            _fpms_lark_v2_callback_button(
                str(i),
                "primary" if i == 1 else "default",
                payload,
                element_id=f"ju_job_{i}"[:20],
            )
        )
    body_elements: list[dict[str, object]] = [
        {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines_md)}},
    ]
    for off in range(0, len(buttons), 5):
        chunk = buttons[off : off + 5]
        body_elements.append(_fpms_lark_v2_column_set_button_row(chunk))
    body_elements.append({"tag": "hr"})
    cancel_pl: dict[str, object] = {"k": "ju_cancel"}
    if ps:
        cancel_pl["sid"] = ps
    body_elements.append(
        _fpms_lark_v2_callback_button(
            "Cancel",
            "default",
            cancel_pl,
            element_id="ju_cancel",
        )
    )
    card: dict[str, object] = {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "Jenkins job — pick one"},
        },
        "body": {"elements": body_elements},
    }
    return json.dumps(card, ensure_ascii=False)


def _fpms_format_jenkins_job_menu(candidates: list[tuple[str, float, str, str]]) -> str:
    """Plain-text fallback when interactive cards cannot be sent (no buttons — typing only)."""
    n = len(candidates)
    if n == 1:
        lines = [
            "Pick the Jenkins job (interactive buttons unavailable here — reply **1** or **cancel**):",
        ]
    else:
        lines = [
            f"Several jobs match (**1**–**{n}**). Buttons unavailable in this view — reply one number or **cancel**:",
        ]
    for i, (alias, _sc, label, url_raw) in enumerate(candidates, start=1):
        u0 = _jenkins_update_primary_url(url_raw)
        lines.append(f"  {i}. **{label}** — `{alias}` — {u0}")
    return "\n".join(lines)


def _environment_from_bot_trigger_line(head: str) -> str | None:
    """Infer ``fpms-uat*-branch`` from the first line (``… update FPMS UAT2`` / ``UAT`` …)."""
    hint = _environment_hint_from_banner(head)
    if hint:
        return hint
    s = head.casefold().replace("_", " ")
    if re.search(r"\bfpms[\s-]*nt[\s-]*uat[\s-]*master\b", s):
        return "fpms-nt-uat-master"
    if re.search(r"\bfpms[\s-]*uat[\s-]*master\b", s):
        return "fpms-uat-master"
    if re.search(r"\buat\s*5\b", s) or "uat5" in s.replace(" ", ""):
        return "fpms-uat5-branch"
    if re.search(r"\buat\s*4\b", s) or "uat4" in s.replace(" ", ""):
        return "fpms-uat4-branch"
    if re.search(r"\buat\s*3\b", s) or "uat3" in s.replace(" ", ""):
        return "fpms-uat3-branch"
    if re.search(r"\buat\s*2\b", s) or "uat2" in s.replace(" ", ""):
        return "fpms-uat2-branch"
    if re.search(r"\bupdate\s+fpms\s+uat\b", s) or re.search(r"\bfpms\s+uat\b", s):
        return "fpms-uat-branch"
    return None


def parse_jenkins_update_fpms_bot_block(text: str) -> dict:
    """
    Parse a Lark-pasted **multi-line** block whose first line contains ``/jenkinsupdate``.

    Returns ``environment``, ``branch``, ``version``, ``service_tokens`` (raw fuzzy strings, not Jenkins ids),
    and optionally ``update_all_services`` when the block is ``services:`` **only** ``all`` / ``*`` / ``every`` / ``全部``.
    """
    raw_lines = [_normalize_config_colons(L) for L in (text or "").splitlines()]
    lines = [L.strip() for L in raw_lines if L.strip() != ""]
    if not lines:
        raise ValueError("Empty message.")
    head = lines[0]
    if not JENKINS_UPDATE_CMD_RE.search(head):
        raise ValueError("First line must include `/jenkinsupdate`.")

    env: str | None = _environment_from_bot_trigger_line(head)
    env_from_banner: str | None = None
    branch: str | None = None
    version: str | None = None
    service_lines: list[str] = []
    last_key: str | None = None
    port_head = re.compile(r"^\d{3,5}\b")

    for line in lines[1:]:
        m = _match_key_line_fuzzy(line)
        if m:
            key = m.group("key").lower()
            if key == "service":
                key = "services"
            rest = _clean_key_rest(m.group("rest") or "")
            last_key = key
            if key == "environment":
                env = _resolve_environment_token(rest)
            elif key == "branch":
                branch = _branch_from_config_block(rest)
                if not branch:
                    raise ValueError("branch: is empty.")
            elif key == "version":
                version = _version_from_config_block(rest)
                if not version:
                    raise ValueError("version: is empty.")
            elif key == "services":
                if rest:
                    service_lines.append(rest)
            else:
                raise ValueError(f"Unknown key: {line!r}")
            continue

        if last_key == "services":
            if port_head.match(line):
                service_lines.append(line)
            elif (
                re.search(r"[a-zA-Z_]", line)
                and len(line) < 200
                and not re.match(r"^\s*update\b", line, re.I)
            ):
                service_lines.append(line)
            elif re.match(r"^\s*email\b", line, re.I):
                continue
            elif re.match(r"^\s*update\b", line, re.I):
                continue
            elif line.lstrip().startswith("#"):
                continue
            continue

        if last_key is None:
            if re.match(r"^\s*email\b", line, re.I):
                continue
            hint = _environment_hint_from_banner(line)
            if hint and env_from_banner is None:
                env_from_banner = hint
            continue

    if branch is None or version is None:
        raise ValueError("Missing branch: or version: in the block.")
    if not service_lines:
        raise ValueError("Missing services (no lines under Service(s):).")

    if _service_lines_mean_update_all(service_lines):
        if env is None:
            env = env_from_banner
        if env is None:
            env = normalize_parameter_text(os.environ.get("FPMS_DEFAULT_ENVIRONMENT", "fpms-uat-branch"))
            if env not in ENVIRONMENTS:
                env = ENVIRONMENTS[0]
        return {
            "environment": env,
            "branch": branch,
            "version": version,
            "service_tokens": [],
            "update_all_services": True,
        }

    tokens: list[str] = []
    for raw in service_lines:
        for part in re.split(r"[,，;]+", raw):
            t = part.strip()
            if t:
                tokens.append(t)
    if not tokens:
        raise ValueError("No service name tokens parsed.")

    if env is None:
        env = env_from_banner
    if env is None:
        env = normalize_parameter_text(os.environ.get("FPMS_DEFAULT_ENVIRONMENT", "fpms-uat-branch"))
        if env not in ENVIRONMENTS:
            env = ENVIRONMENTS[0]

    return {
        "environment": env,
        "branch": branch,
        "version": version,
        "service_tokens": tokens,
    }


def parse_fnt_rc_uat_master_bot_block(text: str) -> dict:
    """
    Lark block for **FNT UAT script run** (RC UAT master): ``/jenkinsupdate`` … then
    ``Branch:`` / ``Version:`` / ``Services:`` (no Environment).
    """
    raw_lines = [_normalize_config_colons(L) for L in (text or "").splitlines()]
    lines = [L.strip() for L in raw_lines if L.strip() != ""]
    if not lines:
        raise ValueError("Empty message.")
    head = lines[0]
    if not JENKINS_UPDATE_CMD_RE.search(head):
        raise ValueError("First line must include `/jenkinsupdate`.")
    branch: str | None = None
    version: str | None = None
    service_lines: list[str] = []
    last_key: str | None = None
    port_head = re.compile(r"^\d{3,5}\b")

    for line in lines[1:]:
        m = _match_key_line_fuzzy(line)
        if m:
            key = m.group("key").lower()
            if key == "service":
                key = "services"
            rest = _clean_key_rest(m.group("rest") or "")
            last_key = key
            if key == "environment":
                continue
            elif key == "branch":
                branch = _branch_from_config_block(rest)
                if not branch:
                    raise ValueError("branch: is empty.")
            elif key == "version":
                version = _version_from_config_block(rest)
                if not version:
                    raise ValueError("version: is empty.")
            elif key == "services":
                if rest:
                    service_lines.append(rest)
            else:
                raise ValueError(f"Unknown key: {line!r}")
            continue
        if last_key == "services":
            if port_head.match(line):
                service_lines.append(line)
            elif (
                re.search(r"[a-zA-Z_]", line)
                and len(line) < 200
                and not re.match(r"^\s*update\b", line, re.I)
            ):
                service_lines.append(line)
            elif re.match(r"^\s*email\b", line, re.I):
                continue
            elif re.match(r"^\s*update\b", line, re.I):
                continue
            elif line.lstrip().startswith("#"):
                continue
            continue
        if last_key is None:
            if re.match(r"^\s*email\b", line, re.I):
                continue
            continue

    # Lark 某些消息体会把多行压扁成一行；补一轮 inline key:value 解析。
    if branch is None or version is None or not service_lines:
        flat = " ".join(lines)
        if branch is None:
            m_b = re.search(r"\bbranch\s*:\s*([^\s,;`]+)", flat, re.I)
            if m_b:
                branch = _branch_from_config_block(m_b.group(1))
        if version is None:
            m_v = re.search(r"\bversion\s*:\s*([^\s,;`]+)", flat, re.I)
            if m_v:
                version = _version_from_config_block(m_v.group(1))
        if not service_lines:
            m_s = re.search(r"\bservices?\s*:\s*(.+)$", flat, re.I)
            if m_s:
                rest = (m_s.group(1) or "").strip()
                if rest:
                    service_lines.append(rest)

    if branch is None or version is None:
        raise ValueError("Missing branch: or version: in the block.")
    if not service_lines:
        raise ValueError("Missing services (no lines under Service(s):).")
    if _service_lines_mean_update_all(service_lines):
        return {
            "_job_kind": "fnt_rc",
            "branch": branch,
            "version": version,
            "service_tokens": [],
            "update_all_services": True,
        }
    tokens: list[str] = []
    for raw in service_lines:
        for part in re.split(r"[,，;]+", raw):
            t = part.strip()
            if t:
                tokens.append(t)
    if not tokens:
        raise ValueError("No service name tokens parsed.")
    return {
        "_job_kind": "fnt_rc",
        "branch": branch,
        "version": version,
        "service_tokens": tokens,
    }


def _fnt_rc_bot_build_config_block(data: dict, resolved_ids: list[str]) -> str:
    if data.get("update_all_services"):
        svc_lines = "all"
    else:
        svc_lines = "\n".join(resolved_ids)
    return (
        "FNT_RC_UAT_MASTER_V1\n"
        f"branch: {data['branch']}\n"
        f"version: {data['version']}\n"
        f"services:\n{svc_lines}\n"
    )


def parse_fnt_rc_run_config_block(text: str) -> tuple[list[str], str, str, bool]:
    """Parse internal ``FNT_RC_UAT_MASTER_V1`` block passed to ``run()``.

    Returns ``(services, branch, version, update_all_services)``. If ``services:`` is only ``all``
    (or ``*`` / ``every`` / ``全部``), ``services`` is empty and ``update_all_services`` is ``True``.
    """
    raw_lines = [_normalize_config_colons(L) for L in (text or "").splitlines()]
    lines = [L.strip() for L in raw_lines if L.strip() != ""]
    if not lines or not lines[0].upper().startswith("FNT_RC_UAT_MASTER_V1"):
        raise ConfigBlockError("Internal FNT RC config must start with FNT_RC_UAT_MASTER_V1.")
    branch: str | None = None
    version: str | None = None
    service_lines: list[str] = []
    last_key: str | None = None
    port_head = re.compile(r"^\d{3,5}\b")
    for line in lines[1:]:
        m = _match_key_line_fuzzy(line)
        if m:
            key = m.group("key").lower()
            if key == "service":
                key = "services"
            rest = _clean_key_rest(m.group("rest") or "")
            last_key = key
            if key == "branch":
                branch = _branch_from_config_block(rest)
            elif key == "version":
                version = _version_from_config_block(rest)
            elif key == "services":
                if rest:
                    service_lines.append(rest)
            elif key == "environment":
                continue
            else:
                raise ConfigBlockError(f"Unknown key: {line!r}")
            continue
        if last_key == "services":
            if port_head.match(line):
                service_lines.append(line)
            elif re.search(r"[a-zA-Z_]", line) and len(line) < 200:
                service_lines.append(line)
            continue
        if last_key is None:
            continue
    if branch is None or version is None:
        raise ConfigBlockError("FNT RC config: missing branch: or version:.")
    if not service_lines:
        raise ConfigBlockError("FNT RC config: missing services: lines.")
    if _service_lines_mean_update_all(service_lines):
        print(
            "→ FNT RC: ``services:`` is **update all** only — will tick **"
            + _jenkins_update_all_stapler_name()
            + "**.\n",
            flush=True,
        )
        return [], branch, version, True
    out: list[str] = []
    for raw in service_lines:
        for part in re.split(r"[,，;]+", raw):
            t = part.strip()
            if t:
                out.append(t)
    if not out:
        raise ConfigBlockError("FNT RC config: no service ids parsed.")
    return out, branch, version, False


def parse_sms_uat_update_bot_block(text: str) -> dict:
    """
    Lark block for **SMS UAT update**: ``/jenkinsupdate`` … then ``Branch:`` / ``Version:`` /
    ``Services:`` (no Environment) — same shape as FNT RC ECP jobs.
    """
    raw_lines = [_normalize_config_colons(L) for L in (text or "").splitlines()]
    lines = [L.strip() for L in raw_lines if L.strip() != ""]
    if not lines:
        raise ValueError("Empty message.")
    head = lines[0]
    if not JENKINS_UPDATE_CMD_RE.search(head):
        raise ValueError("First line must include `/jenkinsupdate`.")
    branch: str | None = None
    version: str | None = None
    service_lines: list[str] = []
    last_key: str | None = None
    port_head = re.compile(r"^\d{3,5}\b")

    for line in lines[1:]:
        m = _match_key_line_fuzzy(line)
        if m:
            key = m.group("key").lower()
            if key == "service":
                key = "services"
            rest = _clean_key_rest(m.group("rest") or "")
            last_key = key
            if key == "environment":
                continue
            elif key == "branch":
                branch = _branch_from_config_block(rest)
                if not branch:
                    raise ValueError("branch: is empty.")
            elif key == "version":
                version = _version_from_config_block(rest)
                if not version:
                    raise ValueError("version: is empty.")
            elif key == "services":
                if rest:
                    service_lines.append(rest)
            else:
                raise ValueError(f"Unknown key: {line!r}")
            continue
        if last_key == "services":
            if port_head.match(line):
                service_lines.append(line)
            elif (
                re.search(r"[a-zA-Z_]", line)
                and len(line) < 200
                and not re.match(r"^\s*update\b", line, re.I)
            ):
                service_lines.append(line)
            elif re.match(r"^\s*email\b", line, re.I):
                continue
            elif re.match(r"^\s*update\b", line, re.I):
                continue
            elif line.lstrip().startswith("#"):
                continue
            continue
        if last_key is None:
            if re.match(r"^\s*email\b", line, re.I):
                continue
            continue

    if branch is None or version is None:
        raise ValueError("Missing branch: or version: in the block.")
    if not service_lines:
        raise ValueError("Missing services (no lines under Service(s):).")
    if _service_lines_mean_update_all(service_lines):
        return {
            "_job_kind": "sms_uat",
            "branch": branch,
            "version": version,
            "service_tokens": [],
            "update_all_services": True,
        }
    tokens: list[str] = []
    for raw in service_lines:
        for part in re.split(r"[,，;]+", raw):
            t = part.strip()
            if t:
                tokens.append(t)
    if not tokens:
        raise ValueError("No service name tokens parsed.")
    return {
        "_job_kind": "sms_uat",
        "branch": branch,
        "version": version,
        "service_tokens": tokens,
    }


def parse_bi_api_update_bot_block(text: str) -> dict:
    """
    Parse Lark block for BI-API-UPDATE:
      /jenkinsupdate ...
      repository/services: ds-... or bi-...
      env/environment: ...
      branch/source_branch: ...
    """
    raw = _normalize_config_colons((text or "").replace("\r\n", "\n")).strip()
    if not raw:
        raise ValueError("Empty message.")
    lines = [L.strip() for L in raw.splitlines() if L.strip()]
    if not lines or not JENKINS_UPDATE_CMD_RE.search(lines[0]):
        raise ValueError("First line must include `/jenkinsupdate`.")
    repo, env, branch = parse_bi_api_update_message_block(raw)
    return {
        "_job_kind": "bi_api_update",
        "repository": repo,
        "environment": env,
        "source_branch": branch,
    }


def _bi_api_update_bot_build_config_block(data: dict) -> str:
    return _bi_api_update_build_config_block(
        str(data.get("repository") or ""),
        str(data.get("environment") or ""),
        str(data.get("source_branch") or ""),
    )


def _sms_uat_bot_build_config_block(data: dict, resolved_ids: list[str]) -> str:
    if data.get("update_all_services"):
        svc_lines = "all"
    else:
        svc_lines = "\n".join(resolved_ids)
    return (
        "SMS_UAT_UPDATE_V1\n"
        f"branch: {data['branch']}\n"
        f"version: {data['version']}\n"
        f"services:\n{svc_lines}\n"
    )


def parse_fpms_prod_script_bot_block(text: str) -> dict:
    """
    Parse:
      /jenkinsupdate --fpmsprodscript
      Command: node ...
    or
      /jenkinsupdate --fpmsprodscript node ...
    """
    raw_lines = [_normalize_config_colons(L) for L in (text or "").splitlines()]
    lines = [L.strip() for L in raw_lines if L.strip() != ""]
    if not lines:
        raise ValueError("Empty message.")
    head = lines[0]
    if not JENKINS_UPDATE_CMD_RE.search(head):
        raise ValueError("First line must include `/jenkinsupdate`.")

    cmd = ""
    m_inline = re.search(
        r"--fpmsprodscript\b\s*(?P<rest>.+)$", head, re.I
    )
    if m_inline:
        cmd = (m_inline.group("rest") or "").strip()

    cmd_lines: list[str] = []
    cmd_key_re = re.compile(
        r"^(?:[>\-\*\u2022]\s*)*(?:`+|\*{1,2})?command(?:`+|\*{1,2})?\s*[:\-–—]\s*(?P<rest>.*)$",
        re.I,
    )
    for line in lines[1:]:
        mk = cmd_key_re.match(line)
        if mk:
            rest = _clean_key_rest(mk.group("rest") or "")
            if rest:
                cmd_lines.append(rest)
            continue
        if cmd_lines:
            cmd_lines.append(line)
            continue
        if not cmd and line:
            cmd_lines.append(line)
    if cmd_lines:
        cmd = "\n".join(cmd_lines).strip()

    if not cmd:
        raise ValueError("Missing command line.")
    if cmd != cmd.strip():
        raise ValueError("Command has leading/trailing spaces; remove spaces at front/end.")
    return {
        "_job_kind": "fpms_prod_script",
        "environment": "fpms-prod",
        "command": cmd,
    }


def _fpms_prod_script_bot_build_config_block(data: dict) -> str:
    return (
        "FPMS_PROD_SCRIPT_RUN_V1\n"
        f"environment: {data.get('environment') or 'fpms-prod'}\n"
        f"command: {data['command']}\n"
    )


def parse_fpms_prod_script_run_config_block(text: str) -> tuple[str, str]:
    """Parse internal ``FPMS_PROD_SCRIPT_RUN_V1`` block passed to ``run()``."""
    raw_lines = [_normalize_config_colons(L) for L in (text or "").splitlines()]
    lines = [L.strip() for L in raw_lines if L.strip() != ""]
    if not lines or not lines[0].upper().startswith("FPMS_PROD_SCRIPT_RUN_V1"):
        raise ConfigBlockError("Internal FPMS PROD SCRIPT config must start with FPMS_PROD_SCRIPT_RUN_V1.")
    env = "fpms-prod"
    cmd = ""
    cmd_key_re = re.compile(
        r"^(?:[>\-\*\u2022]\s*)*(?:`+|\*{1,2})?command(?:`+|\*{1,2})?\s*[:\-–—]\s*(?P<rest>.*)$",
        re.I,
    )
    for line in lines[1:]:
        m = _match_key_line_fuzzy(line)
        if m:
            key = m.group("key").lower()
            rest = _clean_key_rest(m.group("rest") or "")
            if key == "environment":
                env = normalize_parameter_text(rest) or "fpms-prod"
            continue
        cm = cmd_key_re.match(line.strip())
        if cm:
            cmd = _clean_key_rest(cm.group("rest") or "")
    if not cmd:
        raise ConfigBlockError("FPMS PROD SCRIPT config: missing command: line.")
    if cmd != cmd.strip():
        raise ConfigBlockError("FPMS PROD SCRIPT config: command has leading/trailing spaces.")
    return env, cmd


def parse_sms_uat_run_config_block(text: str) -> tuple[list[str], str, str, bool]:
    """Parse internal ``SMS_UAT_UPDATE_V1`` block passed to ``run()``.

    Returns ``(services, branch, version, update_all_services)``. ``services: all`` (alone) sets
    ``update_all_services`` and returns an empty ``services`` list.
    """
    raw_lines = [_normalize_config_colons(L) for L in (text or "").splitlines()]
    lines = [L.strip() for L in raw_lines if L.strip() != ""]
    if not lines or not lines[0].upper().startswith("SMS_UAT_UPDATE_V1"):
        raise ConfigBlockError("Internal SMS UAT config must start with SMS_UAT_UPDATE_V1.")
    branch: str | None = None
    version: str | None = None
    service_lines: list[str] = []
    last_key: str | None = None
    port_head = re.compile(r"^\d{3,5}\b")
    for line in lines[1:]:
        m = _match_key_line_fuzzy(line)
        if m:
            key = m.group("key").lower()
            if key == "service":
                key = "services"
            rest = _clean_key_rest(m.group("rest") or "")
            last_key = key
            if key == "branch":
                branch = _branch_from_config_block(rest)
            elif key == "version":
                version = _version_from_config_block(rest)
            elif key == "services":
                if rest:
                    service_lines.append(rest)
            elif key == "environment":
                continue
            else:
                raise ConfigBlockError(f"Unknown key: {line!r}")
            continue
        if last_key == "services":
            if port_head.match(line):
                service_lines.append(line)
            elif re.search(r"[a-zA-Z_]", line) and len(line) < 200:
                service_lines.append(line)
            continue
        if last_key is None:
            continue
    if branch is None or version is None:
        raise ConfigBlockError("SMS UAT config: missing branch: or version:.")
    if not service_lines:
        raise ConfigBlockError("SMS UAT config: missing services: lines.")
    if _service_lines_mean_update_all(service_lines):
        print(
            "→ SMS UAT: ``services:`` is **update all** only — will tick **"
            + _jenkins_update_all_stapler_name()
            + "**.\n",
            flush=True,
        )
        return [], branch, version, True
    out: list[str] = []
    for raw in service_lines:
        for part in re.split(r"[,，;]+", raw):
            t = part.strip()
            if t:
                out.append(t)
    if not out:
        raise ConfigBlockError("SMS UAT config: no service ids parsed.")
    return out, branch, version, False


_FPMS_PORT_IN_TOKEN_RE = re.compile(r"\b(\d{3,5})\b")


def _fpms_lark_resolve_token_by_port_or_none(token: str) -> list[str] | None:
    """
    Lark ``/jenkinsupdate`` FPMS flow: if a service token contains a deploy **port** (3–5 digits in
    ``SERVICE_PORT_TO_ID``), map by port and skip the fuzzy menu for that line.

    Returns:
        - ``list`` of Jenkins service ids (possibly multiple ports in one token), or
        - ``None`` when the token has **no** 3–5 digit group → caller uses text fuzzy match.

    Raises:
        ``ValueError`` if the token contains digit group(s) but at least one is not a known port.
    """
    t = (token or "").strip()
    if not t:
        return None
    matches = list(_FPMS_PORT_IN_TOKEN_RE.finditer(t))
    if not matches:
        return None
    unknown: list[int] = []
    out: list[str] = []
    seen: set[str] = set()
    for m in matches:
        port = int(m.group(1))
        sid = SERVICE_PORT_TO_ID.get(port)
        if sid is None:
            unknown.append(port)
        elif sid not in seen:
            seen.add(sid)
            out.append(sid)
    if unknown:
        sample = ", ".join(str(p) for p in sorted(SERVICE_PORT_TO_ID)[:28])
        raise ValueError(
            f"Unknown port(s) {unknown} in service line {token!r}. "
            f"Known deploy ports include: {sample}, …"
        )
    return out


def _fpms_format_service_menu_message(token: str, ranked: list[str]) -> str:
    lines = [
        f"Service text `{token}` — near matches (best first). Pick one or more: **1**, **2**, "
        "**1 2 3**, or **1,2,3**:",
    ]
    for i, name in enumerate(ranked, start=1):
        lines.append(f"  {i}. {name}")
    return "\n".join(lines)


def _fpms_format_config_preview(data: dict, resolved: list[str]) -> str:
    """Read-only preview (no confirmation step) before the headless Jenkins run starts."""
    jk = str(data.get("_job_kind") or "")
    if jk == "fnt_rc":
        lines = [
            "**Configuration (FNT RC UAT master — from your message)**",
            f"- **Branch:** `{data['branch']}`",
            f"- **Version:** `{data['version']}`",
            "- **Services (Jenkins checkbox ids):**",
        ]
        for i, sid in enumerate(resolved, start=1):
            lines.append(f"  {i}. `{sid}`")
        return "\n".join(lines)
    if jk == "sms_uat":
        lines = [
            "**Configuration (SMS UAT update — from your message)**",
            f"- **Branch:** `{data['branch']}`",
            f"- **Version:** `{data['version']}`",
            "- **Services (Jenkins checkbox ids):**",
        ]
        for i, sid in enumerate(resolved, start=1):
            lines.append(f"  {i}. `{sid}`")
        return "\n".join(lines)
    if jk == "fpms_prod_script":
        return "\n".join(
            [
                "**Configuration (FPMS PROD SCRIPT RUN — from your message)**",
                f"- **Environment:** `{data.get('environment') or 'fpms-prod'}`",
                f"- **Command:** `{data['command']}`",
            ]
        )
    if jk == "bi_api_update":
        return "\n".join(
            [
                "**Configuration (BI API UPDATE — from your message)**",
                f"- **Repository:** `{data['repository']}`",
                f"- **Environment:** `{data['environment']}`",
                f"- **Source Branch:** `{data['source_branch']}`",
            ]
        )
    lines = [
        "**Configuration (from your message)**",
        f"- **Environment:** `{data['environment']}`",
        f"- **Branch:** `{data['branch']}`",
        f"- **Version:** `{data['version']}`",
        "- **Services (Jenkins checkbox ids):**",
    ]
    for i, sid in enumerate(resolved, start=1):
        lines.append(f"  {i}. `{sid}`")
    return "\n".join(lines)


def _fpms_lark_safe_code_fence(s: str) -> str:
    return (s or "").replace("```", "'''").strip()


def _fpms_lark_verification_card_json(
    *,
    prompt_echo: str,
    verify_lines: list[str],
    ok_all: bool,
    build_url: str,
    job_profile: str = "fpms",
) -> str:
    """Lark ``msg_type=interactive`` payload: JSON string of the card."""
    safe = _fpms_lark_safe_code_fence(prompt_echo) or "(empty)"
    md_checks = "\n".join(verify_lines)
    footer_ok = (
        "✅ All lines above are **OK**. Tap **YES** / **NO** below (or type **yes** / **no**) to click "
        "**Build** in Jenkins or skip."
    )
    footer_bad = (
        "⚠️ At least one line shows **❌**. **Build** stays disabled until every line is ✅. "
        "Fix Jenkins or your config, then run `/jenkinsupdate` again. Tap **NO** (or type **no**) to "
        "close without **Build**."
    )
    block_a = f"📋 **Your message**\n```\n{safe}\n```"
    block_b = (
        f"🔗 **Jenkins**\n[{build_url}]({build_url})\n\n"
        f"🧪 **Form filled and re-check**\n{md_checks}\n\n"
        f"{footer_ok if ok_all else footer_bad}"
    )
    jp = (job_profile or "fpms").strip()
    if jp == "fnt_rc":
        title_text = "FNT RC UAT — form filled & re-check"
    elif jp == "sms_uat":
        title_text = "SMS UAT UPDATE — form filled & re-check"
    elif jp == "fpms_prod_script":
        title_text = "FPMS PROD SCRIPT RUN — form filled & re-check"
    elif jp == "bi_api_update":
        title_text = "BI API UPDATE — form filled & re-check"
    else:
        title_text = "FPMS UAT — form filled & re-check"

    yes_btn = _fpms_lark_v2_callback_button(
        "YES — Build",
        "primary",
        {"k": "wb", "v": "y"},
        element_id="ju_wb_y",
    )
    no_btn = _fpms_lark_v2_callback_button(
        "NO — Skip",
        "default",
        {"k": "wb", "v": "n"},
        element_id="ju_wb_n",
    )
    card: dict = {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "green" if ok_all else "orange",
            "title": {
                "tag": "plain_text",
                "content": title_text,
            },
        },
        "body": {
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": block_a}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": block_b}},
                # Stacked buttons (avoids some clients failing on column_set in narrow layouts)
                yes_btn,
                no_btn,
            ],
        },
    }
    return json.dumps(card, ensure_ascii=False)


def _fpms_lark_verification_plain_fallback(
    *,
    prompt_echo: str,
    verify_lines: list[str],
    ok_all: bool,
    build_url: str,
    job_profile: str = "fpms",
) -> str:
    jp = (job_profile or "fpms").strip()
    if jp == "fnt_rc":
        head = "FNT RC UAT — form filled & re-check"
    elif jp == "sms_uat":
        head = "SMS UAT UPDATE — form filled & re-check"
    elif jp == "fpms_prod_script":
        head = "FPMS PROD SCRIPT RUN — form filled & re-check"
    elif jp == "bi_api_update":
        head = "BI API UPDATE — form filled & re-check"
    else:
        head = "FPMS UAT — form filled & re-check"
    safe = _fpms_lark_safe_code_fence(prompt_echo) or "(empty)"
    lines = [
        f"🧾 **{head}**",
        "",
        "📋 **Your message**",
        "```",
        safe,
        "```",
        "",
        f"🔗 **Jenkins (build page):** {build_url}",
        "",
        "🧪 **Form filled and re-check**",
        *verify_lines,
        "",
    ]
    if ok_all:
        lines.append(
            "✅ All lines above are **OK**. Reply **yes** in this chat to click **Build** in Jenkins, "
            "or **no** to skip **Build**."
        )
    else:
        lines.append(
            "⚠️ At least one line shows **❌**. **Build** stays disabled until every line is ✅. "
            "Fix Jenkins or your config, then run `/jenkinsupdate` again. Reply **no** here to close "
            "without clicking **Build**."
        )
    return "\n".join(lines)


def _fpms_lark_send_verification_summary(
    send,
    chat_id: str,
    *,
    prompt_echo: str,
    verify_lines: list[str],
    ok_all: bool,
    build_url: str,
    job_profile: str = "fpms",
) -> None:
    card = _fpms_lark_verification_card_json(
        prompt_echo=prompt_echo,
        verify_lines=verify_lines,
        ok_all=ok_all,
        build_url=build_url,
        job_profile=job_profile,
    )
    try:
        send(chat_id, card, msg_type="interactive")
    except TypeError:
        send(
            chat_id,
            _fpms_lark_verification_plain_fallback(
                prompt_echo=prompt_echo,
                verify_lines=verify_lines,
                ok_all=ok_all,
                build_url=build_url,
                job_profile=job_profile,
            ),
        )


def _fpms_lark_begin_jenkins_run(
    chat_id: str,
    session_key: str,
    data: dict,
    resolved: list[str],
    send,
    raw_prompt_body: str = "",
    *,
    jenkins_build_url: str | None = None,
    job_profile: str = "fpms",
) -> None:
    """Install ``jenkins_wait_build`` gate, post preview + start message, spawn Playwright thread."""
    jp = (job_profile or "fpms").strip() or "fpms"
    if jp == "fnt_rc":
        cfg = _fnt_rc_bot_build_config_block(data, resolved)
    elif jp == "sms_uat":
        cfg = _sms_uat_bot_build_config_block(data, resolved)
    elif jp == "fpms_prod_script":
        cfg = _fpms_prod_script_bot_build_config_block(data)
    elif jp == "bi_api_update":
        cfg = _bi_api_update_bot_build_config_block(data)
    else:
        cfg = _fpms_bot_build_config_block(data, resolved)
    ev = threading.Event()
    ju = (jenkins_build_url or BUILD_URL).strip()
    _fpms_lark_sessions_put_chat_key(
        session_key,
        {
            "state": "jenkins_wait_build",
            "build_gate_event": ev,
            "approve_build": None,
            "lark_cancel": False,
        },
    )
    preview = _fpms_format_config_preview(data, resolved)
    send(
        chat_id,
        preview
        + "\n\n⏳ Starting **headless** Jenkins — filling **all** parameters, running **two** on-page "
        "re-checks, then you will be asked here to click **Build** or skip. Say **cancel** anytime.",
    )
    _fpms_lark_spawn_run(
        chat_id,
        session_key,
        cfg,
        send,
        raw_prompt_body=raw_prompt_body,
        jenkins_build_url=ju,
        job_profile=jp,
    )


def _fpms_bot_build_config_block(data: dict, resolved_ids: list[str]) -> str:
    if data.get("update_all_services"):
        svc_lines = "all"
    else:
        svc_lines = "\n".join(resolved_ids)
    return (
        f"environment: {data['environment']}\n"
        f"branch: {data['branch']}\n"
        f"version: {data['version']}\n"
        f"services:\n{svc_lines}\n"
    )


def _fpms_lark_spawn_run(
    chat_id: str,
    session_key: str,
    config_block: str,
    send,
    *,
    raw_prompt_body: str = "",
    jenkins_build_url: str | None = None,
    job_profile: str = "fpms",
) -> None:
    """``session_key`` must already hold ``jenkins_wait_build`` with ``build_gate_event``."""

    ju = (jenkins_build_url or BUILD_URL).strip()
    jp = (job_profile or "fpms").strip() or "fpms"

    def _job() -> None:
        try:
            run(
                review_seconds=float(os.environ.get("FPMS_BOT_REVIEW_SECONDS", "12")),
                headless=True,
                browser=os.environ.get("FPMS_PLAYWRIGHT_BROWSER", "chromium"),
                config_block=config_block,
                user_data_dir=(os.environ.get("FPMS_PLAYWRIGHT_USER_DATA_DIR") or "").strip() or None,
                update_all_services=False,
                bot_lark_gate={
                    "session_key": session_key,
                    "chat_id": chat_id,
                    "send": send,
                    "timeout_sec": float(os.environ.get("FPMS_BOT_BUILD_WAIT_SEC", "7200")),
                    "prompt_echo": raw_prompt_body,
                    "build_url": ju,
                    "job_profile": jp,
                },
                jenkins_build_url=ju,
                job_profile=jp,
            )
        except Exception as ex:
            try:
                send(chat_id, f"❌ FPMS Jenkins automation failed:\n```\n{ex}\n```")
            except Exception:
                pass
            print(f"[jenkinsupdate bot] run failed: {ex!r}", flush=True)
        finally:
            _fpms_lark_clear_session_key(session_key)

    threading.Thread(target=_job, name="fpms-uat-jenkins", daemon=True).start()


def _fpms_lark_dispatch_job_row(
    chat_id: str,
    session_key: str,
    body: str,
    row: tuple[str, float, str, str],
    send,
) -> bool:
    """After a Jenkins job alias is chosen: link-only jobs vs automated parameter jobs."""
    alias, _sc, label, url_raw = row
    prof = _jenkins_update_job_automation_profile(url_raw)
    if prof is None:
        lines = [
            f"✅ **Job:** {label}",
            f"**Matched:** `{alias}`",
            "",
            "**Jenkins URL(s):**",
        ]
        for i, uu in enumerate([u.strip() for u in url_raw.splitlines() if u.strip()], 1):
            lines.append(f"{i}. {uu}")
        lines.append(
            "\n_Only **FPMS UAT branch / NT UAT master update**, **FPMS PROD SCRIPT RUN**, **FNT RC** / **FNT script** ECP jobs, and **SMS UAT update** "
            "are auto-filled by this bot; use the links for other jobs._"
        )
        send(chat_id, "\n".join(lines))
        return True
    ju = _jenkins_update_primary_url(url_raw)
    if prof == "fnt_rc":
        return _fpms_lark_dispatch_fnt_rc_parameter_flow(
            chat_id, session_key, body, ju, send
        )
    if prof == "sms_uat":
        return _fpms_lark_dispatch_sms_uat_parameter_flow(
            chat_id, session_key, body, ju, send
        )
    if prof == "fpms_prod_script":
        return _fpms_lark_dispatch_fpms_prod_script_parameter_flow(
            chat_id, session_key, body, ju, send
        )
    if prof == "bi_api_update":
        return _fpms_lark_dispatch_bi_api_update_parameter_flow(
            chat_id, session_key, body, ju, send
        )
    return _fpms_lark_dispatch_fpms_parameter_flow(
        chat_id, session_key, body, ju, send
    )


def _fpms_lark_dispatch_fpms_prod_script_parameter_flow(
    chat_id: str,
    session_key: str,
    body: str,
    jenkins_build_url: str,
    send,
) -> bool:
    """Parse FPMS PROD SCRIPT block; ask follow-up command block if missing; then headless run."""
    try:
        data = parse_fpms_prod_script_bot_block(body)
    except Exception as ex:
        with _fpms_lark_sessions_lock:
            _fpms_lark_sessions[session_key] = {
                "state": "fpms_prod_script_need_command",
                "jenkins_job_url": jenkins_build_url,
            }
        send(
            chat_id,
            "❌ Could not parse FPMS PROD script block.\n```\n%s\n```\n"
            "请直接再发一次（可不带 `/jenkinsupdate`）：\n"
            "Command: node Server/dataPatch/scriptModule.js \"createTestUserFG.js\" \"true\" \"50\" \"1\" \"999\" \"888888\" \"fangting01\" \"2000000\" \"639018101\" \"0\" \"false\" \"false\""
            % ex,
        )
        return True

    _fpms_lark_begin_jenkins_run(
        chat_id,
        session_key,
        data,
        [],
        send,
        raw_prompt_body=body,
        jenkins_build_url=jenkins_build_url,
        job_profile="fpms_prod_script",
    )
    return True


def _fpms_lark_dispatch_fnt_rc_parameter_flow(
    chat_id: str,
    session_key: str,
    body: str,
    jenkins_build_url: str,
    send,
) -> bool:
    """Parse FNT RC block; fuzzy-pick services from ``FNT_RC_UAT_MASTER_SERVICES``; then headless run."""
    try:
        data = parse_fnt_rc_uat_master_bot_block(body)
    except Exception as ex:
        # Lark 某些场景下 pending_body 可能只剩首行（/jenkinsupdate…），此处改为进入“补配置”状态而非直接失败终止。
        with _fpms_lark_sessions_lock:
            _fpms_lark_sessions[session_key] = {
                "state": "fnt_rc_need_block",
                "jenkins_job_url": jenkins_build_url,
            }
        send(
            chat_id,
            "❌ Could not parse FNT RC block. Need `/jenkinsupdate` then `Branch:`, `Version:`, "
            f"`Service(s):` lines.\n```\n{ex}\n```\n"
            "请直接再发一次（可不带 `/jenkinsupdate`）：\n"
            "Branch: master\nVersion: v1.10.35\nServices:\nrisk-analysis-rollout",
        )
        return True
    with _fpms_lark_sessions_lock:
        prev = _fpms_lark_sessions.get(session_key)
        if isinstance(prev, dict) and prev.get("state") == "jenkins_wait_build":
            send(
                chat_id,
                "⏳ A Jenkins **Build** confirmation is already waiting for you in this chat. "
                "**Tap YES/NO** on that card (or type **yes** / **no**), or say **cancel** before starting a new run.",
            )
            return True
    tokens: list[str] = data["service_tokens"]
    if data.get("update_all_services"):
        _fpms_lark_begin_jenkins_run(
            chat_id,
            session_key,
            data,
            [],
            send,
            raw_prompt_body=body,
            jenkins_build_url=jenkins_build_url,
            job_profile="fnt_rc",
        )
        return True
    resolved_ids: list[str] = []
    tokens_to_pick: list[str] = []
    for tok in tokens:
        t = (tok or "").strip()
        hit = _fnt_rc_canonical_service_id(t)
        if hit is not None:
            if hit not in resolved_ids:
                resolved_ids.append(hit)
        else:
            tokens_to_pick.append(tok)
    if not tokens_to_pick:
        if not resolved_ids:
            send(chat_id, "❌ No RC services parsed.")
            return True
        _fpms_lark_begin_jenkins_run(
            chat_id,
            session_key,
            data,
            resolved_ids,
            send,
            raw_prompt_body=body,
            jenkins_build_url=jenkins_build_url,
            job_profile="fnt_rc",
        )
        return True
    first = tokens_to_pick[0]
    q0 = first.replace("_", "-")
    ranked0 = _rank_fnt_rc_services_by_query(q0, limit=12, for_menu=True)
    if not ranked0:
        send(chat_id, f"❌ No RC service matches first text token `{first}`.")
        return True
    sess_new = {
        "state": "pick",
        "job_profile": "fnt_rc",
        "data": data,
        "service_tokens": tokens_to_pick,
        "pick_index": 0,
        "resolved_ids": resolved_ids,
        "current_ranked": ranked0,
        "raw_prompt_body": body,
        "jenkins_job_url": jenkins_build_url,
    }
    with _fpms_lark_sessions_lock:
        _fpms_lark_sessions[session_key] = sess_new
    send(chat_id, _fpms_format_service_menu_message(first, ranked0))
    return True


def _fpms_lark_dispatch_sms_uat_parameter_flow(
    chat_id: str,
    session_key: str,
    body: str,
    jenkins_build_url: str,
    send,
) -> bool:
    """Parse SMS UAT block; fuzzy-pick services from ``SMS_UAT_UPDATE_SERVICES``; then headless run."""
    try:
        data = parse_sms_uat_update_bot_block(body)
    except Exception as ex:
        send(
            chat_id,
            "❌ Could not parse SMS UAT block. Need `/jenkinsupdate` then `Branch:`, `Version:`, "
            f"`Service(s):` lines.\n```\n{ex}\n```",
        )
        return True
    with _fpms_lark_sessions_lock:
        prev = _fpms_lark_sessions.get(session_key)
        if isinstance(prev, dict) and prev.get("state") == "jenkins_wait_build":
            send(
                chat_id,
                "⏳ A Jenkins **Build** confirmation is already waiting for you in this chat. "
                "**Tap YES/NO** on that card (or type **yes** / **no**), or say **cancel** before starting a new run.",
            )
            return True
    tokens: list[str] = data["service_tokens"]
    if data.get("update_all_services"):
        _fpms_lark_begin_jenkins_run(
            chat_id,
            session_key,
            data,
            [],
            send,
            raw_prompt_body=body,
            jenkins_build_url=jenkins_build_url,
            job_profile="sms_uat",
        )
        return True
    resolved_ids: list[str] = []
    tokens_to_pick: list[str] = []
    for tok in tokens:
        t = (tok or "").strip()
        hit = _sms_uat_canonical_service_id(t)
        if hit is not None:
            if hit not in resolved_ids:
                resolved_ids.append(hit)
        else:
            tokens_to_pick.append(tok)
    if not tokens_to_pick:
        if not resolved_ids:
            send(chat_id, "❌ No SMS UAT services parsed.")
            return True
        _fpms_lark_begin_jenkins_run(
            chat_id,
            session_key,
            data,
            resolved_ids,
            send,
            raw_prompt_body=body,
            jenkins_build_url=jenkins_build_url,
            job_profile="sms_uat",
        )
        return True
    first = tokens_to_pick[0]
    q0 = first.replace("_", "-")
    ranked0 = _rank_sms_uat_services_by_query(q0, limit=12, for_menu=True)
    if not ranked0:
        send(chat_id, f"❌ No SMS UAT service matches first text token `{first}`.")
        return True
    sess_new = {
        "state": "pick",
        "job_profile": "sms_uat",
        "data": data,
        "service_tokens": tokens_to_pick,
        "pick_index": 0,
        "resolved_ids": resolved_ids,
        "current_ranked": ranked0,
        "raw_prompt_body": body,
        "jenkins_job_url": jenkins_build_url,
    }
    with _fpms_lark_sessions_lock:
        _fpms_lark_sessions[session_key] = sess_new
    send(chat_id, _fpms_format_service_menu_message(first, ranked0))
    return True


def _fpms_lark_dispatch_fpms_parameter_flow(
    chat_id: str,
    session_key: str,
    body: str,
    jenkins_build_url: str,
    send,
) -> bool:
    """Parse FPMS block, resolve services, then headless run or service pick session."""
    try:
        data = parse_jenkins_update_fpms_bot_block(body)
    except Exception as ex:
        send(
            chat_id,
            "❌ Could not parse `/jenkinsupdate` block. Need first line with `/jenkinsupdate update FPMS UAT`, "
            f"then `branch:`, `version:`, `Service:` lines.\n```\n{ex}\n```",
        )
        return True
    with _fpms_lark_sessions_lock:
        prev = _fpms_lark_sessions.get(session_key)
        if isinstance(prev, dict) and prev.get("state") == "jenkins_wait_build":
            send(
                chat_id,
                "⏳ A Jenkins **Build** confirmation is already waiting for you in this chat. "
                "**Tap YES/NO** on that card (or type **yes** / **no**), or say **cancel** before starting a new run.",
            )
            return True
    tokens: list[str] = data["service_tokens"]
    if data.get("update_all_services"):
        _fpms_lark_begin_jenkins_run(
            chat_id,
            session_key,
            data,
            [],
            send,
            raw_prompt_body=body,
            jenkins_build_url=jenkins_build_url,
            job_profile="fpms",
        )
        return True
    resolved_ids: list[str] = []
    tokens_to_pick: list[str] = []
    for tok in tokens:
        try:
            port_ids = _fpms_lark_resolve_token_by_port_or_none(tok)
        except ValueError as ex:
            send(chat_id, f"❌ {ex}")
            return True
        if port_ids is not None:
            for sid in port_ids:
                if sid not in resolved_ids:
                    resolved_ids.append(sid)
        else:
            tokens_to_pick.append(tok)
    if not tokens_to_pick:
        if not resolved_ids:
            send(chat_id, "❌ No services parsed after resolving ports.")
            return True
        _fpms_lark_begin_jenkins_run(
            chat_id,
            session_key,
            data,
            resolved_ids,
            send,
            raw_prompt_body=body,
            jenkins_build_url=jenkins_build_url,
            job_profile="fpms",
        )
        return True
    first = tokens_to_pick[0]
    if _fpms_lark_is_fnt_rc_only_service_token(first):
        send(
            chat_id,
            f"❌ `{first}` is an **FNT RC UAT master** service, not on the **FPMS UAT branch** job list. "
            "Your message matched the **FPMS** job — the menu would wrongly suggest names like `client-apiserver`.\n\n"
            "Include **rc uat master** / **RC UAT** in the same `/jenkinsupdate` message so the bot selects "
            "the **RC-UAT-UPDATE** Jenkins job, then list `rc-client`, etc. Say **cancel** to clear this session.",
        )
        return True
    if _fpms_lark_is_sms_uat_only_service_token(first):
        send(
            chat_id,
            f"❌ `{first}` is an **SMS UAT update** service, not on the **FPMS UAT branch** job list. "
            "Your message matched the **FPMS** job.\n\n"
            "Include **sms uat update** in the same `/jenkinsupdate` message so the bot selects **SMS-UAT-UPDATE**, "
            "then list services (e.g. `sms-api`). Say **cancel** to clear this session.",
        )
        return True
    q0 = first.replace("_", "-")
    ranked0 = _rank_services_by_query(q0, limit=12, for_menu=True)
    if not ranked0:
        send(chat_id, f"❌ No Jenkins service matches first text token `{first}`.")
        return True
    sess_new = {
        "state": "pick",
        "job_profile": "fpms",
        "data": data,
        "service_tokens": tokens_to_pick,
        "pick_index": 0,
        "resolved_ids": resolved_ids,
        "current_ranked": ranked0,
        "raw_prompt_body": body,
        "jenkins_job_url": jenkins_build_url,
    }
    with _fpms_lark_sessions_lock:
        _fpms_lark_sessions[session_key] = sess_new
    send(chat_id, _fpms_format_service_menu_message(first, ranked0))
    return True


def _fpms_lark_dispatch_bi_api_update_parameter_flow(
    chat_id: str,
    session_key: str,
    body: str,
    jenkins_build_url: str,
    send,
) -> bool:
    """Parse BI API UPDATE block from `/jenkinsupdate`, then run headless Jenkins fill."""
    try:
        data = parse_bi_api_update_bot_block(body)
    except Exception as ex:
        send(
            chat_id,
            "❌ Could not parse BI API UPDATE block. Need `/jenkinsupdate` plus "
            "`repository:` (or `services:`) with `ds-`/`bi-`, `env:`, and `branch:`.\n"
            f"```\n{ex}\n```",
        )
        return True
    with _fpms_lark_sessions_lock:
        prev = _fpms_lark_sessions.get(session_key)
        if isinstance(prev, dict) and prev.get("state") == "jenkins_wait_build":
            send(
                chat_id,
                "⏳ A Jenkins **Build** confirmation is already waiting for you in this chat. "
                "**Tap YES/NO** on that card (or type **yes** / **no**), or say **cancel** before starting a new run.",
            )
            return True
    _fpms_lark_begin_jenkins_run(
        chat_id,
        session_key,
        data,
        [],
        send,
        raw_prompt_body=body,
        jenkins_build_url=jenkins_build_url,
        job_profile="bi_api_update",
    )
    return True


def _fpms_lark_with_sender_union_scope(fn):
    """Bind ``lark_sender_union_id`` into :data:`_fpms_lark_sender_union_id` for session aliasing."""

    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        uid = kwargs.get("lark_sender_union_id")
        tok = _fpms_lark_sender_union_id.set(uid)
        try:
            return fn(*args, **kwargs)
        finally:
            _fpms_lark_sender_union_id.reset(tok)

    return wrapped


@_fpms_lark_with_sender_union_scope
def handle_lark_jenkins_update_message(
    chat_id: str,
    sender_id: str,
    clean_text: str,
    original_text: str,
    send,
    *,
    allow_start: bool,
    lark_sender_union_id: str | None = None,
) -> bool:
    """
    Lark ``/jenkinsupdate``: match a registered Jenkins job from keywords (or ask 1–N),
    then either post job link(s) or run the FPMS UAT branch parameter automation.

    ``allow_start`` — in **group** chats, only **True** when the bot was @mentioned (first message).
    **cancel** works anytime.

    ``lark_sender_union_id`` — optional **union_id** from ``im.message.receive_v1`` so card taps can
    resolve sessions when Feishu sends ``union_id`` but not ``open_id`` on ``card.action.trigger``.

    Returns **True** if this message was consumed (caller should stop processing).
    """
    key = _fpms_lark_session_key(chat_id, sender_id)
    low = (clean_text or "").strip().casefold()

    if low == "cancel":
        released_build_wait = False
        had_other = False
        with _fpms_lark_sessions_lock:
            s = _fpms_lark_sessions.get(key)
            if isinstance(s, dict) and s.get("state") == "jenkins_wait_build":
                ev = s.get("build_gate_event")
                if isinstance(ev, threading.Event):
                    s["state"] = "jenkins_cancelled"
                    s["lark_cancel"] = True
                    s["approve_build"] = False
                    ev.set()
                released_build_wait = True
            elif key in _fpms_lark_sessions:
                had_other = True
                _fpms_lark_sessions.pop(key, None)
        if released_build_wait:
            # One user-facing outcome: the Playwright thread sends the final line after the gate.
            return True
        if had_other:
            send(chat_id, "⏹️ **All `/jenkinsupdate` steps cancelled.**")
        else:
            send(chat_id, "ℹ️ No active `/jenkinsupdate` session to cancel.")
        return True

    with _fpms_lark_sessions_lock:
        sess = _fpms_lark_sessions.get(key)

    if sess is not None:
        st = sess.get("state")
        if st == "fpms_prod_script_need_command":
            body2 = (original_text or clean_text or "").replace("\r\n", "\n").strip()
            if not JENKINS_UPDATE_CMD_RE.search(body2):
                body2 = "/jenkinsupdate --fpmsprodscript\n" + body2
            ju = str(sess.get("jenkins_job_url") or FPMS_PROD_SCRIPT_BUILD_URL).strip() or FPMS_PROD_SCRIPT_BUILD_URL
            with _fpms_lark_sessions_lock:
                _fpms_lark_sessions.pop(key, None)
            return _fpms_lark_dispatch_fpms_prod_script_parameter_flow(
                chat_id, key, body2, ju, send
            )
        if st == "fnt_rc_need_block":
            # 允许用户只发 branch/version/services，不强制再写 /jenkinsupdate
            body2 = (original_text or clean_text or "").replace("\r\n", "\n").strip()
            if not JENKINS_UPDATE_CMD_RE.search(body2):
                body2 = "/jenkinsupdate\n" + body2
            ju = str(sess.get("jenkins_job_url") or BUILD_URL).strip() or BUILD_URL
            with _fpms_lark_sessions_lock:
                _fpms_lark_sessions.pop(key, None)
            return _fpms_lark_dispatch_fnt_rc_parameter_flow(
                chat_id, key, body2, ju, send
            )
        if st == "choose_job":
            cands = sess.get("job_candidates")
            pending = str(sess.get("pending_body") or "")
            if not isinstance(cands, list) or not cands or not pending:
                _fpms_lark_clear_session(chat_id, sender_id)
                send(chat_id, "Session error — start again with `/jenkinsupdate`.")
                return True
            idx = _parse_single_menu_index(clean_text.strip(), len(cands))
            if idx is None:
                ps = str(sess.get("picker_sid") or "").strip()
                card_js = _fpms_lark_job_choice_card_json(cands, picker_sid=ps or None)
                try:
                    send(chat_id, card_js, msg_type="interactive")
                except TypeError:
                    send(
                        chat_id,
                        _fpms_format_jenkins_job_menu(cands),
                    )
                return True
            row = cands[idx - 1]
            _fpms_lark_clear_session(chat_id, sender_id)
            return _fpms_lark_dispatch_job_row(chat_id, key, pending, row, send)
        if st == "jenkins_wait_build":
            if not isinstance(sess.get("build_gate_event"), threading.Event):
                _fpms_lark_clear_session(chat_id, sender_id)
                send(chat_id, "Session error — start again with `/jenkinsupdate`.")
                return True
            if low in ("yes", "y"):
                with _fpms_lark_sessions_lock:
                    sg = _fpms_lark_sessions.get(key)
                    if isinstance(sg, dict) and sg.get("state") == "jenkins_wait_build":
                        sg["approve_build"] = True
                        ev2 = sg.get("build_gate_event")
                        if isinstance(ev2, threading.Event):
                            ev2.set()
                return True
            if low in ("no", "n"):
                with _fpms_lark_sessions_lock:
                    sg = _fpms_lark_sessions.get(key)
                    if isinstance(sg, dict) and sg.get("state") == "jenkins_wait_build":
                        sg["approve_build"] = False
                        ev2 = sg.get("build_gate_event")
                        if isinstance(ev2, threading.Event):
                            ev2.set()
                return True
            send(
                chat_id,
                "**Tap YES / NO** on the card (or type **yes** / **no**) — Build vs skip **Build** (browser closes after).",
            )
            return True
        if st == "jenkins_cancelled":
            # Build gate already cancelled; keep quiet until worker thread closes and clears session.
            return True

        if st == "pick":
            ranked: list[str] = sess["current_ranked"]
            idxs = _parse_multi_indices(clean_text.strip(), len(ranked))
            if idxs is None:
                send(
                    chat_id,
                    f"Please reply with numbers **1–{len(ranked)}** (e.g. `1` or `1 2`). Or say **cancel**.",
                )
                return True
            picked = [ranked[i - 1] for i in idxs]
            sess.setdefault("resolved_ids", [])
            for sid in picked:
                if sid not in sess["resolved_ids"]:
                    sess["resolved_ids"].append(sid)
            sess["pick_index"] = int(sess["pick_index"]) + 1
            if sess["pick_index"] >= len(sess["service_tokens"]):
                data = sess["data"]
                resolved_ids: list[str] = sess["resolved_ids"]
                raw_pb = str(sess.get("raw_prompt_body") or "")
                ju = str(sess.get("jenkins_job_url") or BUILD_URL).strip() or BUILD_URL
                jp = str(sess.get("job_profile") or "fpms").strip() or "fpms"
                _fpms_lark_begin_jenkins_run(
                    chat_id,
                    key,
                    data,
                    resolved_ids,
                    send,
                    raw_prompt_body=raw_pb,
                    jenkins_build_url=ju,
                    job_profile=jp,
                )
                return True
            next_tok = sess["service_tokens"][sess["pick_index"]]
            jp_sess = str(sess.get("job_profile") or "fpms").strip() or "fpms"
            if jp_sess == "fpms" and _fpms_lark_is_fnt_rc_only_service_token(next_tok):
                _fpms_lark_clear_session(chat_id, sender_id)
                send(
                    chat_id,
                    f"❌ `{next_tok}` is an **FNT RC UAT master** service, not FPMS. "
                    "Say **cancel**, then start again with **rc uat master** in your `/jenkinsupdate` message.",
                )
                return True
            if jp_sess == "fpms" and _fpms_lark_is_sms_uat_only_service_token(next_tok):
                _fpms_lark_clear_session(chat_id, sender_id)
                send(
                    chat_id,
                    f"❌ `{next_tok}` is an **SMS UAT update** service, not FPMS. "
                    "Say **cancel**, then start again with **sms uat update** in your `/jenkinsupdate` message.",
                )
                return True
            if (
                jp_sess == "fnt_rc"
                and _sms_uat_canonical_service_id(next_tok) is not None
                and _fnt_rc_canonical_service_id(next_tok) is None
            ):
                _fpms_lark_clear_session(chat_id, sender_id)
                send(
                    chat_id,
                    f"❌ `{next_tok}` is an **SMS UAT** service, not on the **FNT RC** job list. "
                    "Say **cancel**, then start again with **sms uat update** for **SMS-UAT-UPDATE**.",
                )
                return True
            if (
                jp_sess == "sms_uat"
                and _fnt_rc_canonical_service_id(next_tok) is not None
                and _sms_uat_canonical_service_id(next_tok) is None
            ):
                _fpms_lark_clear_session(chat_id, sender_id)
                send(
                    chat_id,
                    f"❌ `{next_tok}` is an **FNT RC** service, not on the **SMS UAT** job list. "
                    "Say **cancel**, then start again with **rc uat master** / **fnt uat script run**.",
                )
                return True
            q = next_tok.replace("_", "-")
            if jp_sess == "fnt_rc":
                nranked = _rank_fnt_rc_services_by_query(q, limit=12, for_menu=True)
            elif jp_sess == "sms_uat":
                nranked = _rank_sms_uat_services_by_query(q, limit=12, for_menu=True)
            else:
                nranked = _rank_services_by_query(q, limit=12, for_menu=True)
            if not nranked:
                _fpms_lark_clear_session(chat_id, sender_id)
                send(chat_id, f"❌ No Jenkins service matches `{next_tok}`. Cancelled.")
                return True
            sess["current_ranked"] = nranked
            with _fpms_lark_sessions_lock:
                _fpms_lark_sessions[key] = sess
            send(chat_id, _fpms_format_service_menu_message(next_tok, nranked))
            return True

        _fpms_lark_clear_session(chat_id, sender_id)
        send(chat_id, "⚠️ Internal session state was reset. Start again with `/jenkinsupdate`.")
        return True

    if not JENKINS_UPDATE_CMD_RE.search(clean_text or ""):
        return False
    if not allow_start:
        return False

    body = original_text or clean_text
    for pat in (r"@_user_\d+", r"<[^>]+>"):
        body = re.sub(pat, "", body)
    body = body.replace("\r\n", "\n").strip()

    # Explicit shortcut: /jenkinsupdate --fpmsprodscript ...
    if FPMS_PROD_SCRIPT_FLAG_RE.search(body):
        return _fpms_lark_dispatch_fpms_prod_script_parameter_flow(
            chat_id,
            key,
            body,
            FPMS_PROD_SCRIPT_BUILD_URL,
            send,
        )

    # BI API UPDATE shortcut:
    # if the same /jenkinsupdate message already contains ds-/bi- repository (or services) + env + branch,
    # skip generic alias matching and dispatch directly to BI-API-UPDATE.
    try:
        _repo_bi, _env_bi, _branch_bi = parse_bi_api_update_message_block(body)
    except Exception:
        pass
    else:
        return _fpms_lark_dispatch_bi_api_update_parameter_flow(
            chat_id,
            key,
            body,
            BI_API_UPDATE_BUILD_URL,
            send,
        )

    with _fpms_lark_sessions_lock:
        prev = _fpms_lark_sessions.get(key)
        if isinstance(prev, dict) and prev.get("state") == "jenkins_wait_build":
            send(
                chat_id,
                "⏳ A Jenkins **Build** confirmation is already waiting for you in this chat. "
                "**Tap YES/NO** on that card (or type **yes** / **no**), or say **cancel** before starting a new run.",
            )
            return True

    head_line = _jenkins_update_first_non_empty_line(body)
    ranked_h = _rank_jenkins_update_job_matches(head_line)
    ties_h = _jenkins_update_disambiguation_ties(ranked_h, band=0.08)
    if ties_h:
        ranked, ties = ranked_h, ties_h
    else:
        ranked = _rank_jenkins_update_job_matches(body)
        ties = _jenkins_update_disambiguation_ties(ranked, band=0.05)
    if not ties:
        sample = ", ".join(sorted(JENKINS_UPDATE_JOB_REGISTRY.keys())[:14])
        send(
            chat_id,
            "❌ Could not match your text to a Jenkins job. Use a known keyword in the message "
            f"(e.g. **fpms uat branch**, **frontend uat1 h5**). Aliases include: {sample}, …",
        )
        return True
    prof0 = _jenkins_update_job_automation_profile(ties[0][3])
    need_menu = (len(ties) > 1) or (len(ties) == 1 and prof0 in ("fnt_rc", "sms_uat"))
    if need_menu:
        picker_sid = secrets.token_hex(16)
        sess_menu = {
            "state": "choose_job",
            "job_candidates": ties,
            "pending_body": body,
            "picker_sid": picker_sid,
        }
        sk_menu = _fpms_lark_session_key(chat_id, sender_id)
        _fpms_lark_register_picker_sid(picker_sid, sk_menu)
        _fpms_lark_sessions_put(
            chat_id,
            sender_id,
            sess_menu,
        )
        card_js = _fpms_lark_job_choice_card_json(ties, picker_sid=picker_sid)
        try:
            send(chat_id, card_js, msg_type="interactive")
        except TypeError:
            send(chat_id, _fpms_format_jenkins_job_menu(ties))
        return True
    return _fpms_lark_dispatch_job_row(chat_id, key, body, ties[0], send)


def handle_lark_jenkins_card_action(
    chat_id: str,
    sender_id: str,
    value: object,
    send,
) -> bool:
    """
    Feishu ``card.action.trigger``: YES/NO (**k** ``wb``), job index (**k** ``job``), or Cancel (**k** ``ju_cancel``).
    Mirrors typed **yes** / **no** / **1** … / **cancel** in :func:`handle_lark_jenkins_update_message`.

    Job-picker payloads include **sid** so taps bind to the correct session even when ``operator`` ids
    differ from ``im.message`` session keys (see **picker_sid** on ``choose_job`` sessions).
    """
    parsed = _fpms_lark_normalize_card_action_value(value)
    if not parsed:
        return False
    sid = str(parsed.get("sid") or "").strip()
    if sid:
        resolved = resolve_jenkins_job_card_session(chat_id, sid)
        if resolved:
            chat_id, sender_id = resolved
    k = str(parsed.get("k") or "").strip().lower()
    if k == "ju_cancel":
        return handle_lark_jenkins_update_message(
            chat_id,
            sender_id,
            "cancel",
            "cancel",
            send,
            allow_start=True,
        )
    if k == "wb":
        v = str(parsed.get("v") or "").strip().lower()
        if v == "y":
            token = "yes"
        elif v == "n":
            token = "no"
        else:
            return False
        return handle_lark_jenkins_update_message(
            chat_id,
            sender_id,
            token,
            token,
            send,
            allow_start=True,
        )
    if k == "job":
        try:
            idx = int(str(parsed.get("i")).strip())
        except (TypeError, ValueError):
            return False
        token = str(idx)
        return handle_lark_jenkins_update_message(
            chat_id,
            sender_id,
            token,
            token,
            send,
            allow_start=True,
        )
    return False


# Backward-compatible names (older imports / docs).
handle_lark_fpms_uat_branch_message = handle_lark_jenkins_update_message
fpms_uat_has_active_lark_session = jenkins_update_has_active_lark_session
parse_fpms_uat_bot_block = parse_jenkins_update_fpms_bot_block


def _playwright_browser_context_and_page(
    p,
    *,
    browser_name: str,
    headless: bool,
    slow_mo: int,
    user_data_dir: str | None,
):
    """
    Default: ``launch`` + ``new_context`` (ephemeral storage — each run is isolated).

    With ``user_data_dir``: ``launch_persistent_context`` so cookies/profile persist on disk
    (closer to a normal non-private window than a fresh incognito-like context).
    """
    viewport = {"width": 1400, "height": 900}
    udir = (user_data_dir or "").strip()
    if udir:
        profile = Path(udir).expanduser()
        profile.mkdir(parents=True, exist_ok=True)
        print(
            f"→ Playwright **persistent profile** (not ephemeral): {profile}\n"
            "  EN: Jenkins cookies can persist; do not run two scripts on the **same** directory at once.\n"
            "  中文：使用本机目录保存浏览器数据（非每次全新的无痕式会话）；不要两个脚本共用一个目录同时跑。",
            flush=True,
        )
        pc_kw: dict = {
            "user_data_dir": str(profile),
            "headless": headless,
            "viewport": viewport,
            "ignore_https_errors": True,
        }
        if slow_mo:
            pc_kw["slow_mo"] = slow_mo
        if browser_name == "firefox":
            context = p.firefox.launch_persistent_context(**pc_kw)
        else:
            context = p.chromium.launch_persistent_context(**pc_kw)
        page = context.pages[0] if context.pages else context.new_page()
        return None, context, page

    if browser_name == "firefox":
        browser_obj = p.firefox.launch(headless=headless, slow_mo=slow_mo)
    else:
        browser_obj = p.chromium.launch(headless=headless, slow_mo=slow_mo)
    context = browser_obj.new_context(viewport=viewport, ignore_https_errors=True)
    page = context.new_page()
    return browser_obj, context, page


def run_tick_only(
    *,
    headless: bool,
    browser: str,
    user_data_dir: str | None = None,
) -> int:
    """
    Open the build-with-parameters page, sign in if needed, tick **Refresh pipeline** only,
    then close the browser. No prompts, no Environment/Services/Branch/Version, no Build.
    """
    user, pw = _credentials()
    bname = (browser or os.environ.get("FPMS_PLAYWRIGHT_BROWSER") or "chromium").strip().lower()
    if bname not in ("chromium", "firefox"):
        print(f"⚠️ Unknown browser {bname!r} — using chromium.")
        bname = "chromium"

    print(
        f"\n→ --tick: {BUILD_URL}\n"
        "  (only Jenkins login if required + Refresh pipeline checkbox; then exit — no Build.)"
    )

    with sync_playwright() as p:
        browser_obj, context, page = _playwright_browser_context_and_page(
            p,
            browser_name=bname,
            headless=headless,
            slow_mo=0,
            user_data_dir=user_data_dir,
        )
        try:
            open_fpms_build_with_login(page, user, pw, first_visit=True, warmup=False)
            page.wait_for_selector("div.jenkins-form-item", timeout=60_000)
            _safe_page_wait(page, _MS_FORM_READY)
            if not _tick_refresh_pipeline_checkbox(page):
                print(
                    "❌ Refresh pipeline checkbox not found or could not be checked.",
                    file=sys.stderr,
                )
                return 1
            if not _verify_refresh_pipeline_checked(page):
                print(
                    "❌ Refresh pipeline did not **stay** checked after the action "
                    "(Jenkins/UnoChoice may have reset it). Try again or increase "
                    "FPMS_TICK_REFRESH_HELP_MS / FPMS_TICK_VERIFY_MS.",
                    file=sys.stderr,
                )
                return 1
            print(
                "→ Verified: “Refresh pipeline” is checked in **this** Playwright browser tab "
                "(scroll to the **bottom** of the parameters form)."
            )
            print(
                "  EN: Only this automation window shows the tick. A Jenkins tab you opened "
                "yourself is a **separate** session — it will not mirror this. "
                "Nothing is sent to the server until **Build**."
            )
            print(
                "  中文：只有脚本自动打开的那个浏览器窗口里能看到勾选；你自己开的同一网址标签页是另一次会话，"
                "不会同步。点 **Build** 才会把参数提交到 Jenkins。"
            )
            raw_rev = (os.environ.get("FPMS_TICK_REVIEW_SEC") or "").strip()
            if raw_rev:
                review_sec = max(0.0, float(raw_rev))
            else:
                review_sec = 0.0 if headless else max(0.0, _TICK_REVIEW_SEC_HEADED_DEFAULT)
            if review_sec > 0:
                print(
                    f"→ Holding browser open {review_sec:g}s so you can confirm "
                    f"(set FPMS_TICK_REVIEW_SEC=0 to close immediately)."
                )
                time.sleep(review_sec)
            print("→ --tick finished (only Refresh pipeline was changed in this session).")
            return 0
        finally:
            context.close()
            if browser_obj is not None:
                browser_obj.close()


def run(
    *,
    review_seconds: float,
    headless: bool,
    browser: str = "chromium",
    config_block: str | None = None,
    user_data_dir: str | None = None,
    bot_lark_gate: dict | None = None,
    jenkins_build_url: str | None = None,
    job_profile: str | None = None,
    update_all_services: bool = False,
) -> None:
    jp_g = (job_profile or "").strip()
    if not jp_g and bot_lark_gate:
        jp_g = str(bot_lark_gate.get("job_profile") or "").strip()
    jp = jp_g or "fpms"
    skip_env = jp in ("fnt_rc", "sms_uat")
    is_prod_script = jp == "fpms_prod_script"
    is_bi_api_update = jp == "bi_api_update"

    parsed_update_all = False
    command = ""
    repository = ""
    if config_block:
        cl = (config_block or "").lstrip()
        if is_bi_api_update:
            if cl.upper().startswith("BI_API_UPDATE_V1"):
                repository, environment, branch = parse_bi_api_update_config_block(config_block)
            else:
                repository, environment, branch = parse_bi_api_update_message_block(config_block)
            services = []
            version = ""
            print(
                "\n→ Parsed BI-API-UPDATE config block:\n"
                f"    repository:  {repository!r}\n"
                f"    environment: {environment!r}\n"
                f"    source_branch: {branch!r}\n"
            )
        elif cl.upper().startswith("FNT_RC_UAT_MASTER_V1"):
            services, branch, version, parsed_update_all = parse_fnt_rc_run_config_block(config_block)
            environment = ""
            svc_note = (
                f"update-all ({_jenkins_update_all_stapler_name()})"
                if parsed_update_all
                else ", ".join(services)
            )
            print(
                "\n→ Parsed FNT RC UAT master config block:\n"
                f"    branch:      {branch!r}\n"
                f"    version:     {version!r}\n"
                f"    services ({len(services) if not parsed_update_all else 'all'}): {svc_note}\n"
            )
        elif cl.upper().startswith("SMS_UAT_UPDATE_V1"):
            services, branch, version, parsed_update_all = parse_sms_uat_run_config_block(config_block)
            environment = ""
            svc_note = (
                f"update-all ({_jenkins_update_all_stapler_name()})"
                if parsed_update_all
                else ", ".join(services)
            )
            print(
                "\n→ Parsed SMS UAT update config block:\n"
                f"    branch:      {branch!r}\n"
                f"    version:     {version!r}\n"
                f"    services ({len(services) if not parsed_update_all else 'all'}): {svc_note}\n"
            )
        elif cl.upper().startswith("FPMS_PROD_SCRIPT_RUN_V1"):
            environment, command = parse_fpms_prod_script_run_config_block(config_block)
            services = []
            branch = ""
            version = ""
            print(
                "\n→ Parsed FPMS PROD SCRIPT config block:\n"
                f"    environment: {environment!r}\n"
                f"    command:     {command!r}\n"
            )
        else:
            environment, services, branch, version, parsed_update_all = parse_fpms_config_block(
                config_block
            )
            svc_note = (
                f"update-all ({_jenkins_update_all_stapler_name()})"
                if parsed_update_all
                else ", ".join(services)
            )
            print(
                "\n→ Parsed config block:\n"
                f"    environment: {environment}\n"
                f"    branch:      {branch!r}\n"
                f"    version:     {version!r}\n"
                f"    services ({len(services) if not parsed_update_all else 'all'}): {svc_note}\n"
            )
    else:
        if is_bi_api_update:
            repository = normalize_parameter_text(prompt_text("What REPOSITORY?"))
            environment = normalize_parameter_text(prompt_text("What ENVIRONMENT?")).casefold()
            services = []
            branch = normalize_parameter_text(prompt_text("What SOURCE_BRANCH?"))
            version = ""
        elif is_prod_script:
            environment = "fpms-prod"
            services = []
            branch = ""
            version = ""
            command = normalize_parameter_text(prompt_text("What Command?"))
        else:
            environment = prompt_environment()
            prompted_update_all = False
            if update_all_services:
                services = []
                print(
                    "\n→ **Update all services** mode: skipping the interactive Services menu; "
                    f"the script will tick **{_jenkins_update_all_stapler_name()}** after Environment.\n"
                    "  中文：已启用“更新全部服务”——跳过逐项选 Services；登录后在浏览器里勾选对应全选框。\n",
                    flush=True,
                )
            else:
                services, prompted_update_all = prompt_services()
            branch = normalize_parameter_text(prompt_text("What branch?"))
            version = normalize_parameter_text(prompt_text("What Version?"))
            parsed_update_all = parsed_update_all or prompted_update_all

    update_all_services = bool(update_all_services) or parsed_update_all

    if update_all_services and services:
        print(
            f"→ **Update-all** mode: ignoring {len(services)} configured / picked service id(s); "
            f"only **{_jenkins_update_all_stapler_name()}** will be ticked.\n",
            flush=True,
        )

    user, pw = _credentials()

    raw_slow = (os.environ.get("FPMS_PLAYWRIGHT_SLOW_MO_MS") or "").strip()
    if raw_slow.isdigit():
        slow_mo = int(raw_slow)
    else:
        # Small default delay between actions reduces UnoChoice race; set FPMS_PLAYWRIGHT_SLOW_MO_MS=0 to disable.
        slow_mo = 35 if not headless else 0
    if _FPMS_FAST_FILL_ACTIVE:
        slow_mo = min(slow_mo, 8 if not headless else 0)
    if slow_mo and not headless:
        print(f"→ Playwright slow_mo={slow_mo}ms (set FPMS_PLAYWRIGHT_SLOW_MO_MS=0 for fastest).")

    bname = (browser or os.environ.get("FPMS_PLAYWRIGHT_BROWSER") or "chromium").strip().lower()
    if bname not in ("chromium", "firefox"):
        print(f"⚠️ Unknown browser {bname!r} — using chromium.")
        bname = "chromium"

    with sync_playwright() as p:
        if bname == "firefox":
            print(
                "→ Browser: Firefox (if missing: `playwright install firefox`). "
                "Different engine can change UnoChoice / Services behavior vs Chromium."
            )
        browser_obj, context, page = _playwright_browser_context_and_page(
            p,
            browser_name=bname,
            headless=headless,
            slow_mo=slow_mo,
            user_data_dir=user_data_dir,
        )
        try:
            ju = (jenkins_build_url or "").strip()
            if not ju and bot_lark_gate:
                ju = str(bot_lark_gate.get("build_url") or "").strip()
            if not ju:
                ju = BUILD_URL
            print("\n→ Single browser session (post-login warm-up reload: **off**).")
            open_fpms_build_with_login(page, user, pw, first_visit=True, warmup=False, build_url=ju)
            page.wait_for_selector("div.jenkins-form-item", timeout=60_000)
            _safe_page_wait(page, _MS_FORM_READY)
            print(
                f"→ Post-login: waiting {_MS_POST_LOGIN_BEFORE_FORM} ms before "
                + (
                    "REPOSITORY / ENVIRONMENT / SOURCE_BRANCH…"
                    if is_bi_api_update
                    else "Services / Branch / Version…"
                    if skip_env
                    else "Environment / Services / Branch…"
                )
            )
            _safe_page_wait(page, _MS_POST_LOGIN_BEFORE_FORM)

            # FPMS: UnoChoice rebuilds Services when Environment changes — Environment first, then Services.
            # FNT RC UAT master: no Environment row — Services only before Branch/Version.
            environment_tick_done = False
            services_tick_done = False

            def _apply_services_selection() -> None:
                if skip_env:
                    select_fnt_rc_services(page, services)
                else:
                    select_services(page, services)

            def _apply_services_or_update_all_phase() -> None:
                if update_all_services:
                    if not _tick_update_all_services_checkbox(page):
                        raise RuntimeError(
                            f"{_jenkins_update_all_stapler_name()}: checkbox not found or could not be checked."
                        )
                    print(
                        f"→ **{_jenkins_update_all_stapler_name()}** checked — skipped individual Services checkboxes.",
                        flush=True,
                    )
                    return
                _apply_services_selection()

            if is_bi_api_update:
                repo_option_value = _choose_bi_repository_option_value(
                    repository, _read_select_options(page, "REPOSITORY")
                )
                select_choice_parameter_by_value(
                    page,
                    "REPOSITORY",
                    repo_option_value,
                )
                repository = repo_option_value
                select_choice_parameter_by_value(page, "ENVIRONMENT", environment)
                fill_text_parameter(page, "SOURCE_BRANCH", branch)
            elif is_prod_script:
                # FPMS PROD SCRIPT RUN: Environment + Command only.
                select_environment_by_value(page, environment)
                fill_text_parameter(page, "Command", command)
            else:
                try:
                    if not skip_env:
                        select_environment(page, environment)
                    environment_tick_done = True
                    _apply_services_or_update_all_phase()
                    services_tick_done = True
                except (
                    ServiceNotDetectedError,
                    ServicesListGoneError,
                    PlaywrightTimeout,
                    PlaywrightError,
                ) as e:
                    print(
                        f"\n→ First Environment/Services attempt failed ({e!r}); "
                        "in-tab recovery: goto build URL → re-login → Refresh pipeline → Build → "
                        f"wait {_MS_POST_BUILD_RECOVER_WAIT_MS/1000:g}s → goto build URL → re-login → refill…"
                    )
                    _recover_services_not_found_sequence(page, user, pw, build_url=ju)
                    try:
                        if skip_env:
                            _apply_services_or_update_all_phase()
                            services_tick_done = True
                        elif environment_tick_done and not services_tick_done:
                            print(
                                "→ Recovery retry: **Environment** is already set — skipping a second "
                                "``select_environment`` (it would clear/rebuild Services in UnoChoice); "
                                "only re-running ``select_services``.\n"
                                "  中文：Environment 已选好，recovery 不再重复切换环境（避免 Services 被刷掉），只重试勾选 Services。",
                                flush=True,
                            )
                            _apply_services_or_update_all_phase()
                            services_tick_done = True
                        else:
                            if not skip_env:
                                select_environment(page, environment)
                                environment_tick_done = True
                            _apply_services_or_update_all_phase()
                            services_tick_done = True
                    except (
                        ServiceNotDetectedError,
                        ServicesListGoneError,
                        PlaywrightTimeout,
                        PlaywrightError,
                    ) as e2:
                        msg = (
                            "Services 找不到：recovery（Refresh pipeline + Build + 再登录）后重新填表仍失败。\n"
                            "Services still not found after recovery and refill."
                        )
                        print(f"❌ {msg}", file=sys.stderr)
                        raise RuntimeError(msg) from e2

                fill_text_parameter(page, "Branch", branch)
                fill_text_parameter(page, "Version", version)

            _safe_page_wait(page, max(0, _MS_POST_FILL_VERIFY))
            if bot_lark_gate is not None:
                if is_bi_api_update:
                    ok_first, lines_first = verify_bi_api_update_parameters_display(
                        page, repository, environment, branch
                    )
                elif is_prod_script:
                    ok_first, lines_first = verify_fpms_prod_script_parameters_display(
                        page, environment, command
                    )
                elif skip_env:
                    ok_first, lines_first = verify_fnt_rc_parameters_display(
                        page,
                        services,
                        branch,
                        version,
                        update_all_services=update_all_services,
                    )
                else:
                    ok_first, lines_first = verify_fpms_parameters_display(
                        page,
                        environment,
                        services,
                        branch,
                        version,
                        update_all_services=update_all_services,
                    )
                print("\n→ ===== First parameter re-check (page vs your choices) =====")
                for ln in lines_first:
                    print(f"    {ln}")
                _safe_page_wait(page, max(250, min(800, _MS_POST_FILL_VERIFY)))
                if is_bi_api_update:
                    ok_second, verify_lines = verify_bi_api_update_parameters_display(
                        page, repository, environment, branch
                    )
                elif is_prod_script:
                    ok_second, verify_lines = verify_fpms_prod_script_parameters_display(
                        page, environment, command
                    )
                elif skip_env:
                    ok_second, verify_lines = verify_fnt_rc_parameters_display(
                        page,
                        services,
                        branch,
                        version,
                        update_all_services=update_all_services,
                    )
                else:
                    ok_second, verify_lines = verify_fpms_parameters_display(
                        page,
                        environment,
                        services,
                        branch,
                        version,
                        update_all_services=update_all_services,
                    )
                print("\n→ ===== Second parameter re-check (page vs your choices) =====")
                for ln in verify_lines:
                    print(f"    {ln}")
                print("→ =====================================================\n")
                ok_all = ok_first and ok_second
            else:
                if is_bi_api_update:
                    ok_all, verify_lines = verify_bi_api_update_parameters_display(
                        page, repository, environment, branch
                    )
                elif is_prod_script:
                    ok_all, verify_lines = verify_fpms_prod_script_parameters_display(
                        page, environment, command
                    )
                elif skip_env:
                    ok_all, verify_lines = verify_fnt_rc_parameters_display(
                        page,
                        services,
                        branch,
                        version,
                        update_all_services=update_all_services,
                    )
                else:
                    ok_all, verify_lines = verify_fpms_parameters_display(
                        page,
                        environment,
                        services,
                        branch,
                        version,
                        update_all_services=update_all_services,
                    )
                lines_first = verify_lines
                print("\n→ ===== Parameter re-check (page vs your choices) =====")
                for ln in verify_lines:
                    print(f"    {ln}")
                print("→ =====================================================\n")

            build_clicked = False
            if bot_lark_gate is not None:
                sk = str(bot_lark_gate["session_key"])
                cid = str(bot_lark_gate["chat_id"])
                send = bot_lark_gate["send"]
                to = float(bot_lark_gate.get("timeout_sec", 7200))
                build_url = str(bot_lark_gate.get("build_url") or BUILD_URL)
                prompt_echo = str(bot_lark_gate.get("prompt_echo") or "")
                _fpms_lark_send_verification_summary(
                    send,
                    cid,
                    prompt_echo=prompt_echo,
                    verify_lines=verify_lines,
                    ok_all=ok_all,
                    build_url=build_url,
                    job_profile=jp,
                )
                with _fpms_lark_sessions_lock:
                    gate = _fpms_lark_sessions.get(sk)
                    ev = gate.get("build_gate_event") if isinstance(gate, dict) else None
                if not isinstance(ev, threading.Event):
                    raise RuntimeError("Lost Lark build gate event (session_key).")
                if not ev.wait(timeout=to):
                    send(cid, "Timed out waiting for **yes** / **no**. **Build** skipped.")
                    build_clicked = False
                else:
                    with _fpms_lark_sessions_lock:
                        approved = _fpms_lark_sessions.get(sk, {}).get("approve_build")
                    if approved is True:
                        if ok_all:
                            _click_jenkins_build_button(page)
                            build_clicked = True
                            print("→ **Build** clicked (Lark-approved).")
                            send(cid, "**Build** clicked in Jenkins.")
                        else:
                            build_clicked = False
                            send(
                                cid,
                                "**Build** was NOT clicked — verification still has ❌. Fix the job in Jenkins if needed.",
                            )
                    else:
                        build_clicked = False
                        with _fpms_lark_sessions_lock:
                            gate_after = _fpms_lark_sessions.get(sk, {})
                            cancelled = bool(
                                isinstance(gate_after, dict) and gate_after.get("lark_cancel")
                            )
                        if cancelled:
                            send(
                                cid,
                                "⏹️ **Cancelled.** **Build** skipped; the Jenkins session will close.",
                            )
                        else:
                            send(cid, "**Build** skipped (you replied **no**).")
            elif skip_env:
                print(
                    "→ ECP job (no Environment — FNT RC / SMS UAT): interactive **yes** to Build is only wired "
                    "for the Lark bot; skipping Build in this session.",
                    flush=True,
                )
            elif is_bi_api_update:
                if prompt_yes_to_click_build_bi_api_update(page, repository, environment, branch):
                    _click_jenkins_build_button(page)
                    build_clicked = True
                    print("→ **Build** clicked (parameters submitted to Jenkins).")
                else:
                    print("→ **Build** skipped (you answered **no**).")
            elif is_prod_script:
                if prompt_yes_to_click_build_prod_script(page, environment, command):
                    _click_jenkins_build_button(page)
                    build_clicked = True
                    print("→ **Build** clicked (parameters submitted to Jenkins).")
                else:
                    print("→ **Build** skipped (you answered **no**).")
            elif prompt_yes_to_click_build(
                page,
                environment,
                services,
                branch,
                version,
                update_all_services=update_all_services,
            ):
                _click_jenkins_build_button(page)
                build_clicked = True
                print("→ **Build** clicked (parameters submitted to Jenkins).")
            else:
                print("→ **Build** skipped (you answered **no**).")

            wait_review(review_seconds, build_was_clicked=build_clicked)

            print(
                "\n→ Review period finished. The script will not click anything else on Jenkins."
            )
            if bot_lark_gate is not None:
                print("→ Lark bot session: closing browser after review.")
            else:
                print(
                    "→ Browser stays open — use Jenkins as you need, then press **Ctrl+C** here to exit "
                    "(browser will close)."
                )
                while True:
                    time.sleep(60)
        except KeyboardInterrupt:
            print("\n→ Ctrl+C received, closing browser…")
            return
        finally:
            context.close()
            if browser_obj is not None:
                browser_obj.close()


def main(argv: list[str] | None = None) -> int:
    _epilog = """
Examples (config is **not** extra words on the same shell line as the script):
  %(prog)s --paste-config
      # wait for the prompt, then paste branch:/version:/services:/…, end with an **empty line**

  %(prog)s --config-file myparams.txt

  %(prog)s --config-file - <<'EOF'
  environment: fpms-uat-branch
  branch: master
  version: 3.2.128g
  services:
  7300 - fg_exrestful
  EOF

Wrong: ``%(prog)s update FPMS UAT branch`` — ``update`` / ``FPMS`` … are not valid options (use --paste-config).
Wrong: typing ``branch:`` at the **zsh** prompt — the shell runs that as a command; it must go **inside** the paste.
中文：整段 ``branch:`` / ``version:`` 不能跟在 ``python3 updateJenkins.py`` 同一行后面当参数；要用 ``--paste-config`` 在脚本提示后粘贴，或 ``--config-file``。

BI-API-UPDATE shortcut (same-line free text) is supported:
  %(prog)s /python3 jenkinsupdate ... repository: ds-superjackpot-api env: prod branch: main
中文：BI-API-UPDATE 可直接在同一行带 ``repository/env/branch``，脚本会自动解析并填 Jenkins。

Slower / more stable Jenkins UI: ``FPMS_STABLE_FILL=1 %(prog)s …``
""".strip()
    ap = argparse.ArgumentParser(
        description=(
            "Jenkins FPMS UAT — fill Environment then Services then Branch/Version, re-verify (✅/❌), yes→Build, AFK. "
            "If Services fail: same tab — goto build → re-login → Refresh pipeline → Build → wait → "
            "goto build → re-login → refill; if still fail → error."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_epilog,
    )
    ap.add_argument(
        "--review-seconds",
        type=float,
        default=90.0,
        help="Seconds to AFK after yes/no + optional Build (default: 90); no further UI clicks",
    )
    ap.add_argument("--headless", action="store_true", help="Headless browser (not recommended)")
    ap.add_argument(
        "--allservice",
        action="store_true",
        help=(
            "Tick the Jenkins **update-all-services** checkbox (hidden name default: Update_All_Services), "
            "skip per-service checkboxes — FPMS, FNT RC, and SMS UAT jobs. Same effect as ``services: all`` "
            "(only that token) in a config block. "
            "After yes/no and optional Build, ``--review-seconds`` still applies (default 90s AFK)."
        ),
    )
    ap.add_argument(
        "--tick",
        action="store_true",
        help="Only open build page, sign in, tick Refresh pipeline; no prompts / no other fields / no Build",
    )
    ap.add_argument(
        "--fpmsprodscript",
        action="store_true",
        help=(
            "FPMS PROD SCRIPT RUN flow: fill Environment=fpms-prod + Command, re-check twice, then yes/no to Build."
        ),
    )
    _raw_br = os.environ.get("FPMS_PLAYWRIGHT_BROWSER", "").strip().lower()
    _br_default = _raw_br if _raw_br in ("chromium", "firefox") else "chromium"
    ap.add_argument(
        "--browser",
        choices=("chromium", "firefox"),
        default=_br_default,
        help="Playwright browser (default chromium; env FPMS_PLAYWRIGHT_BROWSER=firefox allowed)",
    )
    ap.add_argument(
        "--user-data-dir",
        metavar="DIR",
        default=None,
        help=(
            "Persistent browser profile path (cookies/storage survive between runs; not ephemeral). "
            "Overrides env FPMS_PLAYWRIGHT_USER_DATA_DIR when set."
        ),
    )
    cfg = ap.add_mutually_exclusive_group()
    cfg.add_argument(
        "--config-file",
        metavar="PATH",
        default=None,
        help="Read branch / version / services (ports) / environment from file; use - for stdin",
    )
    cfg.add_argument(
        "--paste-config",
        action="store_true",
        help="Paste a labeled config block in the terminal; finish with an empty line",
    )
    args, unknown = ap.parse_known_args(argv)
    cli_bi_config_block: str | None = None
    if unknown:
        unknown_text = " ".join(str(x) for x in unknown).strip()
        try:
            repo_x, env_x, branch_x = parse_bi_api_update_message_block(unknown_text)
        except ConfigBlockError:
            print(
                "\n❌ Unrecognized arguments: "
                + " ".join(repr(x) if re.search(r"\s", str(x)) else str(x) for x in unknown),
                file=sys.stderr,
            )
            print(
                "\n  原因 / Why:\n"
                "  • ``python3 updateJenkins.py`` 后面只能跟 **选项**（如 ``--paste-config``），"
                "不能把 ``update FPMS UAT branch`` 当参数。\n"
                "  • ``email：``、``branch:`` 等必须出现在 **--paste-config 提示之后** 的输入里，"
                "或写在文件里用 ``--config-file``；不能写在 zsh 提示符下当独立命令（会 command not found）。\n"
                "  • BI-API-UPDATE 例外：若同一行里带 ``repository: ... env: ... branch: ...``，会自动识别。\n",
                file=sys.stderr,
            )
            print(
                "  EN: Put ``branch:``, ``version:``, ``services:`` lines **after** you run "
                "``python3 updateJenkins.py --paste-config`` (or use ``--config-file`` / a heredoc).\n"
                "  BI-API-UPDATE shortcut is accepted when your same-line text contains "
                "``repository:``, ``env:`` (or ``environment:``), and ``branch:``.\n",
                file=sys.stderr,
            )
            print("  Try:  python3 updateJenkins.py --paste-config", file=sys.stderr)
            print("  Help:  python3 updateJenkins.py -h\n", file=sys.stderr)
            return 2
        cli_bi_config_block = _bi_api_update_build_config_block(repo_x, env_x, branch_x)
        print(
            "\n→ Parsed BI-API-UPDATE free text from CLI arguments:\n"
            f"    repository:  {repo_x!r}\n"
            f"    environment: {env_x!r}\n"
            f"    source_branch: {branch_x!r}\n"
            "  (Running in headed mode by default unless you pass --headless.)"
        )

    print(f"→ Script: {Path(__file__).resolve()}  |  cwd: {Path.cwd()}")
    _udd = (args.user_data_dir or os.environ.get("FPMS_PLAYWRIGHT_USER_DATA_DIR", "")).strip() or None

    if not _truthy_stable_fill_env():
        _ensure_fast_fill_mode(announce=True)

    if args.tick:
        try:
            return run_tick_only(
                headless=args.headless,
                browser=args.browser,
                user_data_dir=_udd,
            )
        except Exception as ex:
            print(f"❌ {ex}", file=sys.stderr)
            return 1

    print(
        "→ Tip: --browser firefox | FPMS_PLAYWRIGHT_BROWSER=firefox; "
        "FPMS_SERVICES_SPACE_FIRST=0 to skip Space-before-mouse; "
        "FPMS_SERVICES_SELECT_MODE=sequential|auto|batch (default sequential; auto tries DOM batch first); "
        "FPMS_SKIP_SERVICES_QUIET=1 skips pre/post-click stability waits (faster, riskier); "
        "FPMS_MS_POST_LOGIN_BEFORE_FORM (default 3000); "
        "FPMS_DEBUG_MS_BEFORE_ENV_SELECT (default 0) debug ms before Environment select_option; "
        "FPMS_ENV_POST_SELECT_NETWORKIDLE_MS (default 0) bounded networkidle after Environment select; "
        "FPMS_ENV_POST_SELECT_SERVICES_MS (default 12000; **0** skips re-attach wait — avoids stacking with FPMS_SERVICES_APPEAR_MS); "
        "FPMS_SERVICES_APPEAR_MS / FPMS_SERVICES_STABLE_MS (defaults 32s / 36s — lower if “hang” is too long); "
        "FPMS_MS_ENV_SELECT_HOVER (default 0) ms pause after hovering Environment <select>; "
        "FPMS_ENV_SELECT_FORCE=1 → select_option(..., force=True); "
        "FPMS_POST_BUILD_RECOVER_WAIT_MS (default 10000) after recovery Build before second goto/login; "
        "FPMS_MS_POST_FILL_VERIFY (default 600) settle before re-reading form; "
        "FPMS_ENV_SERVICES_NUDGE_TRIES / FPMS_MS_ENV_NUDGE_DWELL (Environment away+back if Services empty); "
        "FPMS_MS_SERVICES_QUIET_BEFORE_CLICK / FPMS_MS_AFTER_PICK_STABLE tune anti-disappear waits; "
        "FPMS_HUMAN_LIKE_SERVICES=0 restores aggressive service clicks; "
        "FPMS_MS_HUMAN_PRE_CLICK / FPMS_MS_HUMAN_POINTER_SETTLE tune human-like pacing; "
        "FPMS_WARMUP_RELOAD=1 enables optional post-login reload (main run forces warmup off); "
        "FPMS_MS_WARMUP_POST_RELOAD / FPMS_MS_WARMUP_POST_RELOGIN_MS; FPMS_SERVICES_GONE_POLLS / FPMS_MS_* / "
        "FPMS_PLAYWRIGHT_SLOW_MO_MS tune stability vs speed; "
        "FPMS_SERVICES_UI_EMPTY_OK=0 disables “list gone but checks still readable on row → continue”; "
        "FPMS_CONFIG_SERVICE_TEXT_AUTO=1 skips the numbered menu for fuzzy service **names** in config (TTY auto-picks top); "
        "--paste-config / --config-file for labeled branch/version/services (ports) / environment; "
        "--user-data-dir or FPMS_PLAYWRIGHT_USER_DATA_DIR for a persistent profile (non-ephemeral browser); "
        "default fill is fastest (short FPMS_* caps, quiet-waits off, aggressive services); "
        "FPMS_STABLE_FILL=1 for slower env-default pacing."
    )

    config_block: str | None = None
    if args.config_file is not None:
        if args.config_file.strip() == "-":
            config_block = sys.stdin.read()
        else:
            config_block = Path(args.config_file).expanduser().read_text(encoding="utf-8")
        if not (config_block or "").strip():
            print("❌ --config-file is empty.", file=sys.stderr)
            return 1
    elif args.paste_config:
        config_block = read_multiline_config_paste()
    elif cli_bi_config_block is not None:
        config_block = cli_bi_config_block

    run_job_profile = "fpms"
    run_build_url = None
    if cli_bi_config_block is not None:
        run_job_profile = "bi_api_update"
        run_build_url = BI_API_UPDATE_BUILD_URL
    elif args.fpmsprodscript:
        run_job_profile = "fpms_prod_script"
        run_build_url = FPMS_PROD_SCRIPT_BUILD_URL
        if config_block is None:
            cmd = normalize_parameter_text(
                prompt_text(
                    "What Command? (must not have leading/trailing spaces)"
                )
            )
            if cmd != cmd.strip() or not cmd:
                print(
                    "❌ Command cannot be empty and must not start/end with spaces.",
                    file=sys.stderr,
                )
                return 1
            config_block = (
                "FPMS_PROD_SCRIPT_RUN_V1\n"
                "environment: fpms-prod\n"
                f"command: {cmd}\n"
            )
        elif not str(config_block).lstrip().upper().startswith("FPMS_PROD_SCRIPT_RUN_V1"):
            # Allow users to provide plain "Command: ..." style in --paste-config / --config-file.
            try:
                data = parse_fpms_prod_script_bot_block(
                    "/jenkinsupdate --fpmsprodscript\n" + str(config_block)
                )
                config_block = _fpms_prod_script_bot_build_config_block(data)
            except Exception as ex:
                print(
                    f"❌ Invalid prod script config: {ex}",
                    file=sys.stderr,
                )
                return 1

    try:
        run(
            review_seconds=args.review_seconds,
            headless=args.headless,
            browser=args.browser,
            config_block=config_block,
            user_data_dir=_udd,
            update_all_services=args.allservice,
            jenkins_build_url=run_build_url,
            job_profile=run_job_profile,
        )
    except Exception as ex:
        print(f"❌ {ex}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
