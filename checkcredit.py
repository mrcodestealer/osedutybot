#!/usr/bin/env python3
"""
Find user log errors (--finderror).

  LogNavigator (browser, headed): default — drives UI like manual flow.
  OSS (HTTP): faster, no browser — GET plain log file from object storage.

  python3 checkcredit.py --finderror 2074 --date 2026-04-27
  python3 checkcredit.py --finderror CP0231 --date 2026-02-05 --oss
  python3 checkcredit.py 185 --debug
    After the log report, prompts for **which user**, then opens the routed backend (Log Third Http → Detail screenshot).

Env (optional):
  A ``.env`` file next to ``checkcredit.py`` is auto-loaded (``pip install python-dotenv``).
  CHECKCREDIT_USER / CHECKCREDIT_PASSWORD — LogNavigator login
  LOG_NAVIGATOR_BASE — LogNavigator base URL
  OSM_LOG_OSS_TEMPLATE — default:
    https://oss-osm-log.osmplay.com/MINIPC/{machine}/logic/{date}.log
  OSS_MACHINE_FOLDER_TEMPLATE — when --finderror is digits-only (default NWR{n}, e.g. 2074 → NWR2074)
  CHECKCREDIT_USE_OSS — set to 1/true so callers (e.g. main.py bot) use OSS without --oss
  CHECKCREDIT_HEADLESS=1 — force headless Chromium (for Linux servers without X11)
  CHECKCREDIT_HEADED=1 — force headed window (needs DISPLAY; macOS/Windows OK)
  CHECKCREDIT_ERROR_CTX_DPR — error-log screenshot sharpness (default ``2``, max ``3``): Playwright
    ``device_scale_factor`` + Pillow font scale (Retina-like PNG).

  On Linux, if $DISPLAY is unset, LogNavigator mode uses headless automatically.

  NP backend (screenshot_np_recharge_detail — Duty Bot /npthirdhttp):
  NP_BACKEND_BASE (default https://backend-np.osmplay.com), NP_BACKEND_USER, NP_BACKEND_PASSWORD
  NP_BACKEND_WINDOW_MINUTES (default 10), NP_BACKEND_MAX_PAGES (default 20, table pagination),
  NP_BACKEND_AMOUNT_EPS (default 0.05) — max |Request amount − log credit| when matching Detail rows.
  Headless tuning: ``NP_BACKEND_HEADLESS_POST_SEARCH_MS`` (default 5500 after Search),
  ``NP_BACKEND_POST_SEARCH_MS``, ``NP_BACKEND_DIALOG_SETTLE_MS`` (ms wait before reading Detail JSON).
  NP_BACKEND_HEADLESS / NP_BACKEND_HEADED (or **WF_THIRD_HTTP_HEADED** / **THIRD_HTTP_PLAYWRIGHT_HEADED**
  — visible Chromium when ``headed=None``). **Duty Bot** calls ``screenshot_np_recharge_detail(..., headed=False)``
  so server screenshots are always headless; use CLI ``--checkuser`` / ``--pause`` for a visible window.
  ``NP_CHECKUSER_PAUSE_OPEN_SEC`` (default ``90``): when ``--pause`` but stdin is not a TTY, seconds to keep
  Chromium open before ``browser.close()`` (avoids instant close on ``input()`` EOF).
  If **Machine** looks Winford (folder / label **starts with ``WF``** … or ``NWR8173`` OSS alias),
  Log Third Http uses ``https://backend-winford.osmplay.com`` + ``WF_BACKEND_USER`` /
  ``WF_BACKEND_PASSWORD`` (defaults ``omduty1``).
  If **Machine** label starts with ``DHS`` (e.g. ``DHS3178``), uses ``https://backend-dhs.osmplay.com``
  + ``DHS_BACKEND_USER`` / ``DHS_BACKEND_PASSWORD``.
  If **Machine** label starts with ``NCH`` (e.g. ``NCH1171``), uses ``https://backend-nc.osmplay.com``;
  login prefers ``NCH_BACKEND_USER`` / ``NCH_BACKEND_PASSWORD``, else falls back to ``NP_BACKEND_*``
  (same duty account on multiple backends).
  If **Machine** label starts with ``CP`` (e.g. ``CP7178``) or ``OSM`` (e.g. ``OSM7178``), uses ``https://backend.osmplay.com``
  + ``CP_BACKEND_USER`` / ``CP_BACKEND_PASSWORD``. Login redirect lands on ``/egm/egmStatusList``; Playwright then opens
  ``/log/logThirdHttpReq`` for the same Log Third Http / Detail screenshot flow as NP.
  If **Machine** label starts with ``MDR`` (e.g. ``MDR7178``), uses ``https://backend-midori.osmplay.com``
  (Midori; Log Third Http path same as NP: ``/log/logThirdHttpReq``; EGM status UI lives under ``/egm/``).
  Login defaults ``MDR_BACKEND_USER`` / ``MDR_BACKEND_PASSWORD`` to ``mdr-omduty`` when unset.
  If **Machine** label starts with ``TBP`` (e.g. ``TBP8641``), uses ``https://backend-tbp.osmplay.com``
  + ``TBP_BACKEND_USER`` / ``TBP_BACKEND_PASSWORD``.
  TBP extras: ``TBP_THIRD_HTTP_AMOUNT_SCALE`` (default ``1`` — set e.g. ``100`` if Request amounts are in cents),
  ``TBP_THIRD_HTTP_NO_MACHINE_ONLY_FALLBACK=1`` to disable a second pass that matches **machine id only**
  (ignores log credit vs Request amount) when the strict pass finds nothing.

  NP debug — **visible Chromium** (not headless), same logic as Duty Bot ``/npthirdhttp``::
    python3 checkcredit.py --checkuser --player-id 132594948 --date 2026-04-27 \\
      --time 23:55:12.092 --machine-substr 2074 --credit 1352 --pause
    Add ``--machine-display WF8173`` / ``DHS3178`` / ``NCH1171`` / ``CP7178`` / ``OSM7178`` / ``MDR7178`` / ``TBP8641`` when testing non-NP backends from CLI
    (Duty Bot passes machine from ``/checkcreditdate`` context automatically).
  Use ``--pause`` to leave Chromium open until you press **Enter in this terminal** (then the script closes
  the browser — that is intentional). If stdin is not a TTY (IDE Run / piped input), ``input()`` gets EOF and
  the window would close immediately unless you set ``NP_CHECKUSER_PAUSE_OPEN_SEC`` (seconds to wait instead).
  Do **not** set ``NP_BACKEND_HEADLESS=1`` when you want to watch the browser.

Requires: playwright (+ chromium) for LogNavigator; requests for OSS.
"""

from __future__ import annotations

import argparse
import html
import os
import re
import sys
import tempfile
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Optional

# CLI / subprocess: load `.env` from this repo so NP_BACKEND_* matches main.py (Duty Bot).
_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(_ROOT_DIR, ".env"))
except ImportError:
    pass

DEFAULT_BASE = os.environ.get("LOG_NAVIGATOR_BASE", "https://lognavigator.cliveslot.com").rstrip("/")
DEFAULT_USER = os.environ.get("CHECKCREDIT_USER", "osm")
DEFAULT_PASS = os.environ.get("CHECKCREDIT_PASSWORD", "osm123")

DEFAULT_OSS_TEMPLATE = os.environ.get(
    "OSM_LOG_OSS_TEMPLATE",
    "https://oss-osm-log.osmplay.com/MINIPC/{machine}/logic/{date}.log",
)


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _error_ctx_dpr() -> float:
    """
    Device pixel ratio for error-context log screenshots (closer to native OS screenshots).
    - Playwright: passed as ``device_scale_factor`` (CSS layout unchanged, more physical pixels).
    - Pillow: scales font size and padding by this factor.

    Env: ``CHECKCREDIT_ERROR_CTX_DPR`` (default ``2``, max ``3``).
    """
    try:
        v = float(os.environ.get("CHECKCREDIT_ERROR_CTX_DPR", "2").strip() or "2")
    except ValueError:
        v = 2.0
    return max(1.0, min(3.0, v))


def _playwright_headless() -> bool:
    """
    Headed Chromium needs a display on Linux. Servers (no $DISPLAY) must use headless.
    CHECKCREDIT_HEADLESS=1 → always headless; CHECKCREDIT_HEADED=1 → always headed.
    """
    if _env_truthy("CHECKCREDIT_HEADLESS"):
        return True
    if _env_truthy("CHECKCREDIT_HEADED"):
        return False
    if sys.platform == "linux" and not (os.environ.get("DISPLAY") or "").strip():
        return True
    return False


def resolve_oss_machine_folder(machine_query: str) -> str:
    """OSS path segment: digits-only → OSS_MACHINE_FOLDER_TEMPLATE (default NWR{n}); else folder name (uppercase, e.g. nch2074 → NCH2074)."""
    q = (machine_query or "").strip()
    if not q:
        raise ValueError("empty machine query")
    if q.isdigit():
        tpl = os.environ.get("OSS_MACHINE_FOLDER_TEMPLATE", "NWR{n}")
        return tpl.format(n=q)
    return q.upper()


def fetch_log_via_oss(machine_query: str, td: date, *, timeout_sec: float = 120.0) -> tuple[str, list[str]]:
    """HTTP GET log text from OSS URL built from OSM_LOG_OSS_TEMPLATE."""
    try:
        import requests
    except ImportError as e:
        raise RuntimeError("OSS mode requires `requests` (pip install requests)") from e

    folder = resolve_oss_machine_folder(machine_query)
    tpl = os.environ.get("OSM_LOG_OSS_TEMPLATE", DEFAULT_OSS_TEMPLATE)
    url = tpl.format(machine=folder, date=td.isoformat())
    r = requests.get(
        url,
        timeout=timeout_sec,
        headers={"User-Agent": "checkcredit/1.0 (OSS)"},
    )
    if r.status_code == 404:
        raise RuntimeError(f"OSS log not found (404): {url}")
    r.raise_for_status()
    text = r.text
    if not text:
        raise RuntimeError(f"OSS returned empty body: {url}")
    meta = [
        "→ Source: OSS (HTTP GET)",
        f"→ URL: {url}",
        f"→ Machine folder: {folder}",
        f"→ Date file: {td.isoformat()}",
    ]
    return text, meta


def fetch_log_via_navigator(
    machine_query: str,
    td: date,
    *,
    timeout_ms: int,
    base: str,
    user: str,
    pw: str,
    debug_headed: bool = False,
) -> tuple[str, str, list[str]]:
    """
    Pull logic log text via LogNavigator UI (same as ``run_finderror`` when ``source=\"navigator\"``).
    Returns ``(log_body, machine_display_from_selector, status_lines)``.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise RuntimeError(
            "LogNavigator mode needs Playwright (`pip install playwright` + install chromium)."
        ) from e

    mq = (machine_query or "").strip()
    machine_display = mq
    text_parts: list[str] = []
    headless = False if debug_headed else _playwright_headless()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            context = browser.new_context(
                ignore_https_errors=True,
                viewport={"width": 1400, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()
            lognavigator_login(page, base, user, pw, timeout_ms=timeout_ms)

            chosen = select_machine_by_number(page, mq, timeout_ms=timeout_ms)
            machine_display = (chosen or "").strip() or machine_display
            text_parts.append(f"→ Machine selected: {chosen}")
            wait_for_logs_file_browser(page, timeout_ms=timeout_ms)

            navigate_to_logic_folder(page, timeout_ms=timeout_ms)
            click_log_file_for_date(page, td, timeout_ms=timeout_ms)
            bump_tail_and_execute(page, timeout_ms=timeout_ms)

            pre = page.locator("section[role='results'] pre, pre.nofloat").first
            pre.wait_for(state="attached", timeout=timeout_ms)
            log_body = pre.inner_text() or ""

            text_parts.append(f"→ Date file: {td.isoformat()}")
        finally:
            browser.close()

    return log_body, machine_display, text_parts


def resolve_machine_display_from_lognavigator(
    machine_query: str,
    *,
    timeout_ms: int = 90_000,
    base: str | None = None,
    user: str | None = None,
    pw: str | None = None,
    debug_headed: bool = False,
) -> str:
    """
    LogNavigator only: login + machine Select2 — returns the canonical folder label (e.g. ``NCH2074``,
    ``OSMCP181``) so :func:`_np_resolve_backend` routes EGM / Third Http to the correct environment.

    Does **not** open a dated logic log or parse credit — lighter than :func:`fetch_log_via_navigator`.
    """
    mq = (machine_query or "").strip()
    if not mq:
        raise ValueError("empty machine query")
    bu = (base or DEFAULT_BASE).rstrip("/")
    us = user or DEFAULT_USER
    pwv = pw or DEFAULT_PASS
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise RuntimeError(
            "LogNavigator resolution needs Playwright (`pip install playwright` + install chromium)."
        ) from e
    headless = False if debug_headed else _playwright_headless()
    chosen = ""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            context = browser.new_context(
                ignore_https_errors=True,
                viewport={"width": 1400, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()
            lognavigator_login(page, bu, us, pwv, timeout_ms=timeout_ms)
            chosen = select_machine_by_number(page, mq, timeout_ms=timeout_ms)
        finally:
            browser.close()
    out = (chosen or "").strip()
    return out if out else mq


def resolve_machine_display_for_egm_route(
    machine_query: str,
    *,
    timeout_ms: int = 90_000,
) -> tuple[str, str]:
    """
    Machine folder label + EGM Search token for the correct backend:

    - ``CHECKCREDIT_USE_OSS=1``: :func:`resolve_oss_machine_folder` (no LogNavigator).
    - Otherwise: :func:`resolve_machine_display_from_lognavigator` so **NCH** / **CP** / … match Select2.
    """
    mq = (machine_query or "").strip()
    if not mq:
        raise ValueError("empty machine query")
    oss_on = os.getenv("CHECKCREDIT_USE_OSS", "").strip().lower() in ("1", "true", "yes", "on")
    if oss_on:
        md = resolve_oss_machine_folder(mq)
    else:
        md = resolve_machine_display_from_lognavigator(mq, timeout_ms=timeout_ms)
    md = (md or "").strip() or mq
    ms = machine_match_substr_from_display(md)
    return md, ms


# ----- machine option matching -----
def _machine_query_alnum_upper(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", (s or "")).upper()


def option_matches_machine_query(option_text: str, query: str) -> bool:
    """
    Match Select2 option label to user query:
      - All digits: any digit group whose int equals int(query) (130 vs 1300 rules).
      - Else (e.g. nch2074, NCH2074, CP0231): case-insensitive alphanumeric match;
        also matches suffix on labels like LUCKYLINK-NCH1327 when query is NCH1327.
    """
    q = (query or "").strip()
    if not q:
        return False
    if q.isdigit():
        qv = int(q)
        for m in re.finditer(r"\d+", option_text):
            try:
                if int(m.group(0)) == qv:
                    return True
            except ValueError:
                continue
        return False
    qn = _machine_query_alnum_upper(q)
    if not qn:
        return False
    on = _machine_query_alnum_upper(option_text)
    if on == qn:
        return True
    if len(qn) >= 4 and on.endswith(qn):
        return True
    return False


def _native_select_label_then_sync_select2(page, label: str) -> None:
    """Select hidden native #logAccessConfigId (display:none); sync Select2 UI."""
    lb = label.strip()
    ok = page.evaluate(
        """([text]) => {
          const el = document.querySelector('#logAccessConfigId');
          if (!el) return false;
          const opts = [...el.options];
          const o = opts.find(x => (x.textContent || '').trim() === text);
          if (!o) return false;
          el.selectedIndex = o.index;
          el.value = o.value;
          el.dispatchEvent(new Event('change', { bubbles: true }));
          var $ = window.jQuery;
          if ($ && $(el).data('select2')) $(el).trigger('change');
          return true;
        }""",
        [lb],
    )
    if not ok:
        page.locator("#logAccessConfigId").select_option(label=lb, force=True)
        page.locator("#logAccessConfigId").evaluate(
            """el => {
              el.dispatchEvent(new Event('change', { bubbles: true }));
              var $ = window.jQuery;
              if ($ && $(el).data('select2')) $(el).trigger('change');
            }"""
        )


def select_machine_by_number(page, machine_query: str, *, timeout_ms: int = 30_000) -> str:
    """Open Select2 #logAccessConfigId, search, click best matching option. Returns chosen label."""
    from playwright.sync_api import TimeoutError as PWTimeout

    menu = page.locator("#log-access-configs-menu-item")
    try:
        menu.scroll_into_view_if_needed(timeout=10_000)
    except Exception:
        pass

    # Prefer native <select> when we already know the label (hidden select → force=True).
    opts = page.locator("#logAccessConfigId option")
    try:
        opts.first.wait_for(state="attached", timeout=min(15_000, timeout_ms))
    except PWTimeout:
        pass
    for i in range(opts.count()):
        txt = (opts.nth(i).inner_text() or "").strip()
        if txt and option_matches_machine_query(txt, machine_query):
            _native_select_label_then_sync_select2(page, txt)
            page.wait_for_timeout(800)
            return txt

    sel_root = page.locator("#s2id_logAccessConfigId")
    sel_root.wait_for(state="visible", timeout=timeout_ms)
    sel_root.locator("a.select2-choice").first.click(timeout=10_000)
    page.wait_for_timeout(200)

    # Select2 v3: input#s2id_autogen1_search.class=select2-input
    search = page.locator(
        "input.select2-input[role='combobox'], "
        "#s2id_autogen1_search, "
        ".select2-drop-active input.select2-input, "
        ".select2-dropdown input.select2-search__field, "
        "#select2-drop input[type='search']"
    ).first
    search.wait_for(state="visible", timeout=10_000)
    search.fill("")
    search.type(machine_query, delay=30)
    page.wait_for_timeout(400)

    results = page.locator(".select2-results li.select2-result-selectable")
    try:
        results.first.wait_for(state="visible", timeout=8_000)
    except PWTimeout:
        pass

    n = results.count()
    candidates: list[tuple[str, str]] = []
    for i in range(n):
        li = results.nth(i)
        txt = (li.inner_text() or "").strip()
        if not txt or txt.upper() == "SEARCHING":
            continue
        # Option text is visible label
        if option_matches_machine_query(txt, machine_query):
            candidates.append((txt, txt))

    # Fallback: read native <option> values (hidden select)
    if not candidates:
        opts = page.locator("#logAccessConfigId option")
        oc = opts.count()
        for i in range(oc):
            opt = opts.nth(i)
            val = (opt.inner_text() or opt.get_attribute("value") or "").strip()
            if val and option_matches_machine_query(val, machine_query):
                candidates.append((val, val))

    if not candidates:
        raise RuntimeError(
            f"No machine option matches query {machine_query!r}. "
            "Try digits only (e.g. 2074) or full id (e.g. NCH2074, nch2074)."
        )

    chosen_label = candidates[0][0]
    # Click matching visible result
    clicked = False
    for i in range(results.count()):
        li = results.nth(i)
        txt = (li.inner_text() or "").strip()
        if option_matches_machine_query(txt, machine_query):
            li.click(timeout=10_000)
            clicked = True
            break
    if not clicked:
        _native_select_label_then_sync_select2(page, chosen_label)

    page.wait_for_timeout(800)
    shown = page.locator("#select2-chosen-1, .select2-chosen").first
    try:
        shown.wait_for(state="visible", timeout=5_000)
        chosen_label = (shown.inner_text() or "").strip() or chosen_label
    except Exception:
        pass
    return chosen_label


def lognavigator_login(page, base: str, user: str, pw: str, *, timeout_ms: int) -> None:
    """Best-effort login for LogNavigator."""
    login_urls = (
        f"{base}/lognavigator/login",
        f"{base}/lognavigator/",
        f"{base}/login",
    )
    last_err = None
    for u in login_urls:
        try:
            page.goto(u, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(500)
            pw_input = page.locator(
                'input[type="password"], input[name="password"], input#password'
            ).first
            if pw_input.count() == 0:
                continue
            user_sel = (
                'input[name="username"], input[name="user"], input#username, '
                'input[type="text"]:near(input[type="password"])'
            )
            user_in = page.locator(user_sel).first
            if user_in.count():
                user_in.fill(user)
            pw_input.fill(pw)
            sub = page.locator(
                'button[type="submit"], input[type="submit"], '
                'button:has-text("Sign in"), button:has-text("Login"), '
                'button:has-text("Log in")'
            ).first
            if sub.count():
                sub.click()
            else:
                pw_input.press("Enter")
            page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(1000)
            return
        except Exception as e:
            last_err = e
            continue
    if page.locator(".navbar-brand:has-text('LogNavigator'), a.navbar-brand").count():
        return
    raise RuntimeError(f"Could not complete login (last error: {last_err!r})")


def wait_for_logs_file_browser(page, *, timeout_ms: int) -> None:
    """After switching machine, wait until folder/file table or logic link is visible."""
    page.locator(
        '#resultsTable, table#resultsTable, a[href*="subPath=logic"], '
        'a.text-warning[href*="logic"]'
    ).first.wait_for(state="visible", timeout=timeout_ms)


def navigate_to_logic_folder(page, *, timeout_ms: int) -> None:
    link = page.locator(
        '#resultsTable a[href*="subPath=logic"], '
        'a.text-warning[href*="subPath=logic"], '
        'table a[href*="subPath=logic"]'
    ).first
    link.wait_for(state="visible", timeout=timeout_ms)
    link.click()
    page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    page.wait_for_timeout(800)


def click_log_file_for_date(page, target: date, *, timeout_ms: int) -> None:
    """Click row whose File links to logic/YYYY-MM-DD.log"""
    date_str = target.strftime("%Y-%m-%d")
    pattern = f"logic/{date_str}.log"
    row_link = page.locator(f'#resultsTable tbody a[href*="relativePath={pattern}"]').first
    if row_link.count() == 0:
        row_link = page.locator(f'#resultsTable tbody a[href*="{date_str}.log"]').first
    row_link.wait_for(state="visible", timeout=timeout_ms)
    row_link.click()
    page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    page.wait_for_timeout(1000)


def bump_tail_and_execute(page, *, timeout_ms: int) -> None:
    cmd = page.locator("#cmd")
    cmd.wait_for(state="visible", timeout=timeout_ms)
    raw = cmd.input_value()
    new_raw = re.sub(r"tail\s*-\s*\d+", "tail -100000", raw, flags=re.I)
    if new_raw == raw:
        new_raw = re.sub(r"tail\s+\d+", "tail -100000", raw, flags=re.I)
    cmd.fill("")
    cmd.fill(new_raw)
    page.wait_for_timeout(2000)
    exe = page.locator("#executeButton")
    exe.click(timeout=15_000)
    page.wait_for_timeout(2500)
    page.wait_for_selector("section[role='results'] pre, pre.nofloat", timeout=min(timeout_ms, 120_000))


# New player segment: extra1 / extra2 / extra3 + userid (lines below belong to that player until the next marker).
_USERID_START = re.compile(
    r'extra[123]\s*:\s*["\']?userid\s*:\s*(\d+)',
    re.I,
)
_ERR_ZERO = re.compile(r"""['\"]error['\"]\s*:\s*0\b""")
_CUR_COIN = re.compile(r"""['\"]cur_coin['\"]\s*:\s*([\d.]+)""")
_REDUCE_NUM = re.compile(r"""['\"]reduce_num['\"]\s*:\s*([\d.]+)""")
_AMOUNT_NUM = re.compile(r"""['\"]amount['\"]\s*:\s*(-?\d+(?:\.\d+)?)""")
_LINE_TIME_PREFIX = re.compile(r"^(\d{2}:\d{2}:\d{2}(?:\.\d{3})?)")


def _line_time_prefix(line: str) -> str:
    m = _LINE_TIME_PREFIX.match(line.strip())
    return m.group(1) if m else ""


def _parse_success_cur_coin(line: str) -> tuple[float, str] | None:
    """successJson line with cur_coin and error 0 — caller picks last match per block."""
    if "successJson" not in line:
        return None
    if not _ERR_ZERO.search(line):
        return None
    m = _CUR_COIN.search(line)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    tshort = _line_time_prefix(line)
    return (val, tshort)


def _parse_reduce_num_credit(line: str) -> tuple[float, str] | None:
    """When cur_coin is absent: extra/JSON line with reduce_num and error 0 (e.g. bet/settle extra)."""
    if "reduce_num" not in line:
        return None
    if not _ERR_ZERO.search(line):
        return None
    m = _REDUCE_NUM.search(line)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    tshort = _line_time_prefix(line)
    return (val, tshort)


def _parse_aft_interrogation_fail_credit(line: str) -> tuple[float, str] | None:
    """
    successJson with ``desc: aft interrogation faild``:
    use ``data.amount`` as latest credit fallback for this player block.
    """
    if "successJson" not in line:
        return None
    if "aft interrogation faild" not in line.lower():
        return None
    m = _AMOUNT_NUM.search(line)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    tshort = _line_time_prefix(line)
    return (val, tshort)


_ENTER_TIMEOUT = re.compile(
    r"enter\s+game\s+time\s*out|errorJson\s*\{[^}]*time\s*out|['\"]desc['\"]\s*:\s*['\"]time\s*out['\"]",
    re.I,
)


def _block_has_enter_game_timeout(blines: list[tuple[int, str]]) -> bool:
    """AFT / enter-game flow failed after pending (e.g. ``enter game time out``, ``desc: time out``)."""
    for _, line in blines:
        if _ENTER_TIMEOUT.search(line):
            return True
    return False


def _parse_enter_game_credit(line: str) -> tuple[float, str] | None:
    """
    ``httpaft:enter_game add_num:…,target:…`` — intended credit for AFT enter-game.
    Prefer ``target`` (float); else ``add_num``.
    """
    low = line.lower()
    if "enter_game" not in low:
        return None
    if "add_num" not in line and "target" not in low:
        return None
    mt = re.search(r"target\s*:\s*(-?\d+(?:\.\d+)?)", line, re.I)
    ma = re.search(r"add_num\s*:\s*(-?\d+(?:\.\d+)?)", line, re.I)
    raw = mt.group(1) if mt else (ma.group(1) if ma else None)
    if raw is None:
        return None
    try:
        val = float(raw)
    except ValueError:
        return None
    tshort = _line_time_prefix(line)
    return (val, tshort)


def parse_user_blocks_full(log_text: str) -> list[dict[str, Any]]:
    """
    Split by extra1/extra2/extra3 userid markers; lines until the next marker belong to that player.
    Every block included (errors may be empty). Error lines carry line_idx for ordering.
    Multi-line log records: timestamp on the first line is carried to following lines (no leading time)
    so reduce_num / successJson on `extra:` continuation lines get the correct time.

    **Enter-game AFT timeout:** if the block contains ``enter game time out`` / ``errorJson`` time-out
    etc., ``latest_credit`` can fall back to the last ``httpaft:enter_game`` line with ``add_num`` /
    ``target`` when there is no ``successJson`` ``cur_coin``.

    **reduce_num first:** if the block has any parsed ``reduce_num`` credit line, that wins over
    ``cur_coin`` and ``enter_game`` for ``latest_credit`` (last ``reduce_num`` in the block by line order).
    """
    raw = log_text.splitlines()
    blocks: list[tuple[str, list[tuple[int, str]]]] = []
    cur_uid: str | None = None
    cur_lines: list[tuple[int, str]] = []
    for i, line in enumerate(raw):
        um = _USERID_START.search(line)
        if um:
            if cur_uid is not None:
                blocks.append((cur_uid, cur_lines))
            cur_uid = um.group(1)
            cur_lines = [(i, line)]
        else:
            if cur_uid is not None:
                cur_lines.append((i, line))
    if cur_uid is not None:
        blocks.append((cur_uid, cur_lines))

    err_re = re.compile(r"""['\"]error['\"]\s*:\s*(\d+)""")

    out: list[dict[str, Any]] = []
    for uid, blines in blocks:
        findings: list[dict[str, Any]] = []
        best_coin: dict[str, Any] | None = None
        best_reduce: dict[str, Any] | None = None
        best_aft_fail: dict[str, Any] | None = None
        best_enter: dict[str, Any] | None = None
        block_max_line = max((ln for ln, _ in blines), default=-1)
        rolling_ts = ""
        for line_idx, line in blines:
            tp = _line_time_prefix(line)
            if tp:
                rolling_ts = tp
            sc = _parse_success_cur_coin(line)
            if sc:
                val, tshort = sc
                eff_ts = (tshort or rolling_ts).strip()
                best_coin = {
                    "line_idx": line_idx,
                    "value": val,
                    "time_short": eff_ts,
                    "source": "cur_coin",
                }
            rn = _parse_reduce_num_credit(line)
            if rn:
                val, tshort = rn
                eff_ts = (tshort or rolling_ts).strip()
                best_reduce = {
                    "line_idx": line_idx,
                    "value": val,
                    "time_short": eff_ts,
                    "source": "reduce_num",
                }
            af = _parse_aft_interrogation_fail_credit(line)
            if af:
                val, tshort = af
                eff_ts = (tshort or rolling_ts).strip()
                best_aft_fail = {
                    "line_idx": line_idx,
                    "value": val,
                    "time_short": eff_ts,
                    "source": "aft_interrogation_faild_amount",
                }
            eg = _parse_enter_game_credit(line)
            if eg:
                val, tshort = eg
                eff_ts = (tshort or rolling_ts).strip()
                best_enter = {
                    "line_idx": line_idx,
                    "value": val,
                    "time_short": eff_ts,
                    "source": "enter_game_target",
                }
            em = err_re.search(line)
            if not em:
                continue
            try:
                ec = int(em.group(1))
            except ValueError:
                continue
            if ec <= 0:
                continue
            tm = re.match(r"^(\d{2}:\d{2}:\d{2}\.\d{3})", line)
            time_part = tm.group(1) if tm else rolling_ts
            json_like = line
            if "|" in line:
                parts = line.split("|", 2)
                if len(parts) >= 3:
                    json_like = parts[2].strip()
            ctx_start = max(0, line_idx - 4)
            ctx_end = min(len(raw), line_idx + 5)
            ctx_lines: list[str] = []
            for gi in range(ctx_start, ctx_end):
                marker = ">>" if gi == line_idx else "  "
                ctx_lines.append(f"{marker} {raw[gi]}")
            findings.append(
                {
                    "time": time_part,
                    "error_count": ec,
                    "snippet": json_like[:800],
                    "full_line": line.rstrip(),
                    "line_idx": line_idx,
                    "context_lines": ctx_lines,
                }
            )
        row: dict[str, Any] = {
            "user_id": uid,
            "errors": findings,
            "block_max_line": block_max_line,
        }
        has_enter_timeout = _block_has_enter_game_timeout(blines)
        if best_aft_fail is not None:
            latest_credit: dict[str, Any] | None = best_aft_fail
        elif best_reduce is not None:
            latest_credit: dict[str, Any] | None = best_reduce
        elif has_enter_timeout and best_enter is not None:
            latest_credit = best_enter
        elif best_coin is not None:
            latest_credit = best_coin
        elif best_enter is not None:
            latest_credit = best_enter
        else:
            latest_credit = None
        if latest_credit:
            row["latest_credit"] = latest_credit
        out.append(row)
    return out


def parse_user_blocks_for_errors(log_text: str) -> list[dict[str, Any]]:
    """Backward compat: only blocks that have at least one error > 0."""
    return [b for b in parse_user_blocks_full(log_text) if b.get("errors")]


def _error_line_time_key(err: dict[str, Any]) -> str:
    """Sort key HH:MM:SS.mmm — missing sorts as earliest."""
    t = (err.get("time") or "").strip()
    return t if t else "00:00:00.000"


def _sort_players_latest_credit_first(merged: list[dict[str, Any]]) -> None:
    """In-place: players with latest_credit first (higher log line_idx = newer); rest by max error time."""
    def key(row: dict[str, Any]) -> tuple:
        lc = row.get("latest_credit")
        if lc:
            return (1, int(lc["line_idx"]))
        errs = row.get("errors") or []
        mt = max((_error_line_time_key(e) for e in errs), default="00:00:00.000")
        return (0, mt)

    merged.sort(key=key, reverse=True)


def _block_contribution_max_line(blk: dict[str, Any]) -> int:
    mx = -1
    for e in blk.get("errors") or []:
        mx = max(mx, int(e.get("line_idx", -1)))
    lc = blk.get("latest_credit")
    if lc:
        mx = max(mx, int(lc.get("line_idx", -1)))
    bm = blk.get("block_max_line")
    if bm is not None:
        mx = max(mx, int(bm))
    return mx


def merge_players_full(payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge blocks by user_id; track max_line_idx (last line index in log for this player)."""
    if not payload:
        return []

    order: list[str] = []
    by_uid: dict[str, dict[str, Any]] = {}
    for blk in payload:
        uid = blk["user_id"]
        if uid not in by_uid:
            order.append(uid)
            by_uid[uid] = {
                "errors": [],
                "_lc_aft_fail": None,
                "_lc_coin": None,
                "_lc_reduce": None,
                "_lc_enter": None,
                "max_line_idx": -1,
            }
        by_uid[uid]["errors"].extend(blk.get("errors") or [])
        lc = blk.get("latest_credit")
        if lc:
            src = (lc.get("source") or "").strip()
            if src == "aft_interrogation_faild_amount":
                slot = "_lc_aft_fail"
            elif src == "reduce_num":
                slot = "_lc_reduce"
            elif src == "enter_game_target":
                slot = "_lc_enter"
            else:
                slot = "_lc_coin"
            old = by_uid[uid][slot]
            if old is None or int(lc["line_idx"]) > int(old["line_idx"]):
                by_uid[uid][slot] = lc
        by_uid[uid]["max_line_idx"] = max(
            by_uid[uid]["max_line_idx"],
            _block_contribution_max_line(blk),
        )

    merged: list[dict[str, Any]] = []
    for uid in order:
        errs = by_uid[uid]["errors"][:]
        errs.sort(key=_error_line_time_key, reverse=True)
        d = by_uid[uid]
        # Prefer aft_interrogation_faild amount, then reduce_num, then cur_coin / enter_game.
        if d["_lc_aft_fail"] is not None:
            lc_final = d["_lc_aft_fail"]
        elif d["_lc_reduce"] is not None:
            lc_final = d["_lc_reduce"]
        else:
            lc_cands = [d["_lc_coin"], d["_lc_enter"]]
            lc_cands = [c for c in lc_cands if c is not None]
            lc_final = max(lc_cands, key=lambda x: int(x["line_idx"])) if lc_cands else None
        merged.append(
            {
                "user_id": uid,
                "errors": errs,
                "latest_credit": lc_final,
                "max_line_idx": d["max_line_idx"],
            }
        )
    return merged


def merge_finderror_by_user(payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge + legacy sort: players with latest_credit first, then by max error time."""
    merged = merge_players_full(payload)
    _sort_players_latest_credit_first(merged)
    return merged


def _latest_error_line_idx(row: dict[str, Any]) -> int:
    return max((int(e["line_idx"]) for e in (row.get("errors") or [])), default=-1)


def select_top2_error_players(merged: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Top 2 players by most recent error line (log line index)."""
    with_err = [r for r in merged if r.get("errors")]
    with_err.sort(key=_latest_error_line_idx, reverse=True)
    return with_err[:2]


def select_top2_overall(merged: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Top 2 players by last activity in log (max_line_idx)."""
    return sorted(merged, key=lambda r: int(r.get("max_line_idx", -1)), reverse=True)[:2]


def select_no_error_players(
    merged: list[dict[str, Any]],
    *,
    limit: int = 4,
) -> list[dict[str, Any]]:
    """Players with no parsed error lines, newest log activity first (Third Http / Detail flow)."""
    rows = [r for r in merged if not (r.get("errors") or [])]
    rows.sort(key=lambda r: int(r.get("max_line_idx", -1)), reverse=True)
    return rows[: max(0, int(limit))]


def machine_match_substr_from_display(machine_display: str) -> str:
    """Digits substring to match NP `machineId` (e.g. NWR2074 → `2074`)."""
    nums = re.findall(r"\d+", machine_display or "")
    return max(nums, key=len) if nums else ""


def resolve_player_log_credit_snapshot(
    machine_query: str,
    player_id: str,
    target_date: date,
    *,
    timeout_sec: float = 120.0,
) -> tuple[str | None, dict[str, Any] | None, str]:
    """
    Load logic log (**OSS HTTP** when ``CHECKCREDIT_USE_OSS`` is set; otherwise **LogNavigator / Playwright**
    like ``/checkcreditdate`` without OSS), merge player blocks, return
    ``(machine_folder_display, latest_credit_dict_or_None, error_message)``.
    ``latest_credit`` matches rows from ``merge_players_full`` / ``parse_user_blocks_full``.
    """
    pid = str(player_id or "").strip()
    if not pid:
        return None, None, "Player ID is empty."
    mq = str(machine_query or "").strip()
    if not mq:
        return None, None, "Machine type is empty."
    oss_on = os.getenv("CHECKCREDIT_USE_OSS", "").strip().lower() in ("1", "true", "yes", "on")
    timeout_ms = max(15_000, int(timeout_sec * 1000))
    try:
        if oss_on:
            log_body, _meta = fetch_log_via_oss(mq, target_date, timeout_sec=timeout_sec)
            md = resolve_oss_machine_folder(mq)
        else:
            log_body, md_nav, _parts = fetch_log_via_navigator(
                mq,
                target_date,
                timeout_ms=timeout_ms,
                base=DEFAULT_BASE,
                user=DEFAULT_USER,
                pw=DEFAULT_PASS,
                debug_headed=False,
            )
            md = (md_nav or "").strip() or resolve_oss_machine_folder(mq)
    except Exception as e:
        return None, None, f"Log load failed: {e}"
    merged = merge_players_full(parse_user_blocks_full(log_body))
    for row in merged:
        if str(row.get("user_id", "")).strip() != pid:
            continue
        lc = row.get("latest_credit")
        if isinstance(lc, dict) and (str(lc.get("time_short") or "").strip()):
            return md, lc, ""
        return (
            md,
            lc if isinstance(lc, dict) else None,
            "Found this user in the log but could not parse a credit timestamp.",
        )
    return md, None, f"No log block found for user `{pid}` on that file."


def build_checkcredit_player_form_card() -> dict[str, Any]:
    """Lark card 2.0: machine + player + date → Duty Bot ``checkcredit_player_submit``."""
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "checkcredit — by player"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**Machine type** (folder label, e.g. `NWR2074`, `DHS3189`) · "
                            "**Player ID** · **Log date**.\n"
                            "Opened by **`/checkcreditdate`** alone (with **`@Duty Bot`** in groups). "
                            "Submit loads that day's logic log (**OSS** if `CHECKCREDIT_USE_OSS=1`, else "
                            "**LogNavigator / Playwright**, same as `/checkcreditdate <machine>`) and runs "
                            "**Third Http → Detail** when a credit time is found."
                        ),
                    },
                },
                {
                    "tag": "form",
                    "name": "checkcredit_player_form",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Machine type"},
                        },
                        {
                            "tag": "input",
                            "name": "machine_type",
                            "placeholder": {
                                "tag": "plain_text",
                                "content": "e.g. NWR2074 / DHS3189",
                            },
                            "required": True,
                        },
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Player ID"},
                        },
                        {
                            "tag": "input",
                            "name": "player_id",
                            "placeholder": {
                                "tag": "plain_text",
                                "content": "Numeric user id",
                            },
                            "required": True,
                        },
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Date"},
                        },
                        {
                            "tag": "date_picker",
                            "name": "log_date",
                            "placeholder": {
                                "tag": "plain_text",
                                "content": "Pick log date",
                            },
                            "required": True,
                        },
                        {
                            "tag": "button",
                            "name": "submit_checkcredit_player",
                            "text": {"tag": "plain_text", "content": "Submit"},
                            "type": "primary",
                            "form_action_type": "submit",
                            "behaviors": [
                                {
                                    "type": "callback",
                                    "value": {"k": "checkcredit_player_submit"},
                                }
                            ],
                        },
                    ],
                },
            ]
        },
    }


def _np_lark_v2_callback_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Stringify callback ``value`` scalars for Feishu card 2.0 (same idea as Jenkins cards)."""
    out: dict[str, Any] = {}
    for key, val in payload.items():
        ks = str(key)
        if isinstance(val, (dict, list)):
            out[ks] = val
        elif val is None:
            out[ks] = ""
        else:
            out[ks] = str(val)
    return out


def _np_lark_v2_button(
    label: str,
    btn_type: str,
    payload: dict[str, Any],
    *,
    element_id: str = "",
) -> dict[str, Any]:
    btn: dict[str, Any] = {
        "tag": "button",
        "text": {"tag": "plain_text", "content": label},
        "type": btn_type,
        "behaviors": [{"type": "callback", "value": _np_lark_v2_callback_payload(payload)}],
    }
    eid = (element_id or "").strip()[:20]
    if eid:
        btn["element_id"] = eid
    return btn


def _np_lark_v2_button_row(buttons: list[dict[str, Any]]) -> dict[str, Any]:
    columns: list[dict[str, Any]] = []
    for b in buttons:
        columns.append(
            {
                "tag": "column",
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


def build_np_choice_lark_card(
    np_choices: list[dict[str, Any]],
    *,
    target_date_iso: str = "",
    machine_display: str = "",
    third_http_backend: str = "NP",
    image_key: str = "",
    intro_line: str = "",
    same_last_line: str = "",
    extra_md: str = "",
    extra_error_images: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Lark card 2.0: log date + machine + players; buttons **1**..**N** (N = len(choices), max 4) or type digits in chat."""
    lines: list[str] = []
    td = (target_date_iso or "").strip()
    md = (machine_display or "").strip()
    be = (third_http_backend or "NP").strip().upper()
    if be not in ("NP", "WF", "DHS", "NCH", "CP", "OSM", "MDR", "TBP"):
        be = "NP"
    if td or md:
        bits: list[str] = []
        if td:
            bits.append(f"Log date ({be} window): `{td}`")
        if md:
            bits.append(f"Machine: `{md}`")
        lines.append(" · ".join(bits))
        lines.append("Screenshot uses the log date above + each line's credit time.")
        lines.append("")
    il = (intro_line or "").strip()
    if il:
        lines.append(il)
        lines.append("")
    ex = (extra_md or "").strip()
    if ex:
        lines.append(ex)
        lines.append("")
    for i, ch in enumerate(np_choices):
        uid = ch.get("user_id", "")
        cr = ch.get("credit", "n/a")
        ts = ch.get("time_short") or "n/a"
        lines.append(f"{i + 1}) User ID `{uid}` — last credit `{cr}` @ `{ts}`")
    sll = (same_last_line or "").strip()
    if sll:
        lines.append("")
        lines.append(f"ℹ️ {sll}")
    content = "\n".join(lines) if lines else "_No players._"
    title = "Kindly choose one of the player"
    body_elements: list[dict[str, Any]] = []
    ik = (image_key or "").strip()
    if ik:
        body_elements.append(
            {
                "tag": "img",
                "img_key": ik,
                "alt": {"tag": "plain_text", "content": "Machine control window"},
            }
        )
    # Put error-context screenshots before the long markdown so clients do not hide them below the fold.
    ex_imgs = extra_error_images or []
    for it in ex_imgs:
        ik2 = str(it.get("img_key") or "").strip()
        if not ik2:
            continue
        title2 = str(it.get("title") or "Error context screenshot").strip()
        body_elements.append(
            {"tag": "div", "text": {"tag": "lark_md", "content": f"📷 **{title2}**"}}
        )
        body_elements.append(
            {
                "tag": "img",
                "img_key": ik2,
                "alt": {"tag": "plain_text", "content": title2},
            }
        )
    body_elements.append({"tag": "div", "text": {"tag": "lark_md", "content": content}})
    n = len(np_choices)
    if n > 0:
        body_elements.append(
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**Tap** a number below, or type **1**–**{n}** in chat (no @).",
                },
            }
        )
        buttons = [
            _np_lark_v2_button(
                str(i),
                "primary" if i == 1 else "default",
                {"k": "np_pick", "i": i},
                element_id=f"npcc{i}"[:20],
            )
            for i in range(1, n + 1)
        ]
        body_elements.append(_np_lark_v2_button_row(buttons))
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": title},
        },
        "body": {"elements": body_elements},
    }


def build_np_followup_payload(
    top2_any: list[dict[str, Any]],
    top2_err: list[dict[str, Any]],
    machine_display: str,
    td: date,
    merged_players: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Third Http / Detail picker: up to 4 choices (backend tag from machine label: NP, WF, DHS, …).

    When there are **no** parsed error lines (``top2_err`` empty), choices are filled only from
    players **without** errors (latest activity first), so `/checkcredit` still works on every routed backend.

    When errors exist: latest 2 in log order plus latest 2 with error (deduped), same as before.
    """
    def _row_choice(r: dict[str, Any], source: str) -> dict[str, Any]:
        lc = r.get("latest_credit") or {}
        val = lc.get("value")
        credit_s = str(val) if val is not None else "n/a"
        credit_val: float | None = None
        if val is not None:
            try:
                credit_val = float(val)
            except (TypeError, ValueError):
                credit_val = None
        return {
            "user_id": str(r["user_id"]),
            "time_short": (lc.get("time_short") or "").strip(),
            "credit": credit_s,
            "credit_value": credit_val,
            "source": source,
        }

    merged = merged_players or []
    np_choices: list[dict[str, Any]] = []
    seen_uid: set[str] = set()
    no_err_pool = select_no_error_players(merged, limit=4) if not top2_err else []

    if top2_err:
        for r in top2_any[:2]:
            uid = str(r.get("user_id") or "").strip()
            if not uid or uid in seen_uid:
                continue
            np_choices.append(_row_choice(r, "latest_in_log"))
            seen_uid.add(uid)
        for r in top2_err[:2]:
            uid = str(r.get("user_id") or "").strip()
            if not uid or uid in seen_uid:
                continue
            np_choices.append(_row_choice(r, "with_error"))
            seen_uid.add(uid)
    else:
        for r in no_err_pool:
            uid = str(r.get("user_id") or "").strip()
            if not uid or uid in seen_uid:
                continue
            np_choices.append(_row_choice(r, "no_error"))
            seen_uid.add(uid)
    np_choices = np_choices[:4]

    latest_two_players: list[dict[str, str]] = []
    if top2_err:
        for r in top2_any[:2]:
            lc = r.get("latest_credit") or {}
            latest_two_players.append(
                {
                    "user_id": str(r["user_id"]),
                    "time_short": (lc.get("time_short") or "").strip(),
                }
            )
    else:
        for r in no_err_pool[:2]:
            lc = r.get("latest_credit") or {}
            latest_two_players.append(
                {
                    "user_id": str(r["user_id"]),
                    "time_short": (lc.get("time_short") or "").strip(),
                }
            )

    err_only: list[dict[str, Any]] = []
    seen_err_uid: set[str] = set()
    for r in top2_err[:2]:
        uid = str(r.get("user_id") or "").strip()
        if not uid or uid in seen_err_uid:
            continue
        err_only.append(_row_choice(r, "with_error"))
        seen_err_uid.add(uid)

    return {
        "machine_display": machine_display,
        "machine_match_substr": machine_match_substr_from_display(machine_display),
        "target_date": td.isoformat(),
        "third_http_backend": _np_log_backend_tag(machine_display),
        "latest_two_players": latest_two_players,
        "np_choices": np_choices,
        "np_choices_error_only": err_only,
    }


def _interactive_debug_pick_player(
    merged_players: list[dict[str, Any]],
    *,
    machine_display: str,
) -> dict[str, Any] | None:
    """
    After ``--finderror`` + ``--debug``, prompt for a user row then drive ``screenshot_np_recharge_detail``.
    Returns ``user_id``, ``time_short``, optional ``credit_value``, or ``None`` to skip.
    """
    if not merged_players:
        print("[checkcredit --debug] No players in log — skipping backend step.", flush=True)
        return None
    md = (machine_display or "").strip()
    be = _np_log_backend_tag(md or None)
    bar = "─" * 58
    print(f"\n{bar}", flush=True)
    print("[checkcredit --debug] Choose a user — backend Log Third Http → Detail screenshot:", flush=True)
    print(f"  Machine `{md or '?'}' → backend `{be}` (from folder label)", flush=True)
    print(bar, flush=True)

    for i, row in enumerate(merged_players, start=1):
        uid = str(row.get("user_id", ""))
        lc = row.get("latest_credit") or {}
        ts = (lc.get("time_short") or "").strip()
        val = lc.get("value")
        cred_s = str(val) if val is not None else "n/a"
        warn = "" if ts else "  ⚠ needs credit time in log"
        print(f"  {i}) User `{uid}` — credit `{cred_s}` @ `{ts or '—'}`{warn}", flush=True)
    print(bar, flush=True)
    print("Enter index 1–%d, or paste User ID, or q to skip:" % len(merged_players), flush=True)

    while True:
        try:
            raw = input("> ").strip()
        except EOFError:
            print("[checkcredit --debug] EOF — skip backend.", flush=True)
            return None
        if not raw or raw.lower() in ("q", "quit"):
            print("[checkcredit --debug] Skipped backend.", flush=True)
            return None
        pick_row: dict[str, Any] | None = None
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(merged_players):
                pick_row = merged_players[idx - 1]
            else:
                print(f"  Enter a number between 1 and {len(merged_players)}.", flush=True)
                continue
        else:
            for row in merged_players:
                if str(row.get("user_id", "")).strip() == raw:
                    pick_row = row
                    break
            if pick_row is None:
                print("  No matching User ID — try again or q.", flush=True)
                continue
        lc = pick_row.get("latest_credit") or {}
        ts = (lc.get("time_short") or "").strip()
        if not ts:
            print("  This user has no parsed credit time — pick another row.", flush=True)
            continue
        uid = str(pick_row["user_id"]).strip()
        credit_val: float | None = None
        v = lc.get("value")
        if v is not None:
            try:
                credit_val = float(v)
            except (TypeError, ValueError):
                credit_val = None
        return {"user_id": uid, "time_short": ts, "credit_value": credit_val}


def pick_latest_error_uid(merged: list[dict[str, Any]]) -> tuple[str | None, int]:
    """Single player whose newest error line appears latest in the log."""
    best_uid: str | None = None
    best_line = -1
    for r in merged:
        for e in r.get("errors") or []:
            li = int(e.get("line_idx", -1))
            if li > best_line:
                best_line = li
                best_uid = r["user_id"]
    return best_uid, best_line


def pick_latest_any_uid(merged: list[dict[str, Any]]) -> tuple[str | None, int]:
    """Player whose log lines extend furthest (last line index overall)."""
    if not merged:
        return None, -1
    best = max(merged, key=lambda r: int(r.get("max_line_idx", -1)))
    return best["user_id"], int(best.get("max_line_idx", -1))


def _row_display_times(row: dict[str, Any]) -> tuple[str, str]:
    """(credit_time_or_empty, last_error_time_or_empty) for markdown."""
    cr = row.get("latest_credit")
    ct = (cr.get("time_short") or "").strip() if cr else ""
    errs = row.get("errors") or []
    et = (errs[0].get("time") or "").strip() if errs else ""
    return ct, et


def _player_detail_block(
    row: dict[str, Any],
    machine_display: str,
    *,
    error_log_mode: str,
) -> str:
    """error_log_mode: 'if_any' only append log when errors exist; 'always' always show log section. Lark markdown."""
    ct, et = _row_display_times(row)
    uid = row["user_id"]
    cr = row.get("latest_credit")
    if cr:
        ts = (cr.get("time_short") or "").strip()
        val = cr["value"]
        rn_note = " *(reduce_num)*" if cr.get("source") == "reduce_num" else ""
        credit_s = (f"`{val}` @ `{ts}`{rn_note}" if ts else f"`{val}`{rn_note}")
    else:
        credit_s = "*(none)*"
    time_s = ct or et or "*(n/a)*"
    errs = row.get("errors") or []
    lines_body = "\n".join(e.get("full_line") or e.get("snippet") or "" for e in errs)
    log_md = _truncate_log(lines_body) if lines_body else ""

    head = "\n".join(
        [
            f"🕐 **Time:** `{time_s}`",
            f"🖥 **Machine:** `{machine_display}`",
            f"🆔 **User ID:** `{uid}`",
            f"💰 **Last credit:** {credit_s}",
        ]
    )
    if error_log_mode == "always":
        inner = log_md if log_md else "(no error lines)"
        return f"{head}\n\n📋 **Error log:**\n{inner}"
    if log_md:
        return f"{head}\n\n📋 **Error log:**\n{log_md}"
    return head


def build_latest_two_overall_lark_card(
    rows: list[dict[str, Any]],
    *,
    machine_display: str,
    target_date: date,
) -> dict[str, Any]:
    """Latest 2 players by log order (may have zero errors): time, uid, last credit, error log if any."""
    dstr = target_date.isoformat()
    elements: list[dict[str, Any]] = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**Machine:** `{machine_display}`  ·  **Date:** `{dstr}`",
            },
        }
    ]
    slice_rows = rows[:2]
    if not slice_rows:
        elements.append(
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": "No user blocks found (`extra1|extra2|extra3: userid`).",
                },
            }
        )
    else:
        for i, r in enumerate(slice_rows):
            if i > 0:
                elements.append({"tag": "hr"})
            elements.append(
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": _player_detail_block(r, machine_display, error_log_mode="if_any"),
                    },
                }
            )
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "wathet",
            "title": {"tag": "plain_text", "content": "checkcredit — latest 2 players (log order, any)"},
        },
        "elements": elements,
    }


def build_latest_two_error_lark_card(
    rows: list[dict[str, Any]],
    *,
    machine_display: str,
    target_date: date,
) -> dict[str, Any]:
    """Latest 2 players that have error > 0: time, uid, last credit, error log."""
    dstr = target_date.isoformat()
    elements: list[dict[str, Any]] = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**Machine:** `{machine_display}`  ·  **Date:** `{dstr}`",
            },
        }
    ]
    slice_rows = rows[:2]
    if not slice_rows:
        elements.append(
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": "No `error` > 0 in scanned user blocks.",
                },
            }
        )
    else:
        for i, r in enumerate(slice_rows):
            if i > 0:
                elements.append({"tag": "hr"})
            elements.append(
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": _player_detail_block(r, machine_display, error_log_mode="always"),
                    },
                }
            )
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "checkcredit — latest 2 players (with error)"},
        },
        "elements": elements,
    }


def build_same_latest_players_card(
    *,
    machine_display: str,
    target_date: date,
    latest_err_uid: str | None,
    latest_any_uid: str | None,
    same_uid: bool,
    third_http_backend: str = "",
) -> dict[str, Any]:
    """Whether the last player in the log and the last player with an error are the same uid."""
    dstr = target_date.isoformat()
    le = latest_err_uid or "*(none)*"
    la = latest_any_uid or "*(none)*"
    be = (third_http_backend or "").strip().upper() or "NP"
    if not latest_err_uid:
        same_line = (
            f"No parsed errors — picker shows **no-error players only** (`{be}` routed backend); "
            "use **1**–**N** / Third Http.\n"
            f"未解析到 error — 列表仅为 **无 error 玩家**（当前 **`{be}`** 后端）；请 **1–N** / Third Http。"
        )
    elif same_uid and latest_err_uid and latest_any_uid:
        same_line = "Same player: last activity in log and last error line refer to this user ID."
    else:
        same_line = "Different: last player in log ≠ last player with error."
    body = (
        f"**Machine:** `{machine_display}`\n"
        f"**Date:** `{dstr}`\n\n"
        f"**Last player with error — User ID:** `{le}`\n"
        f"**Last player in log (any) — User ID:** `{la}`\n\n"
        f"{same_line}"
    )
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "checkcredit — same last player?"},
        },
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": body}}],
    }


def _truncate_log(text: str, limit: int = 1800) -> str:
    t = (text or "").strip()
    if len(t) <= limit:
        return t
    return t[: limit - 20] + "\n… (truncated)"


def format_finderror_terminal_from_merged(merged: list[dict[str, Any]]) -> str:
    """Plain terminal text from merged rows (same order as card)."""
    if not merged:
        return "✅ No error > 0 found in scanned user blocks (extra1|2|3: userid).\n"

    blocks: list[str] = []
    for row in merged:
        uid = row["user_id"]
        errs = row["errors"]
        n = len(errs)
        lines_body = "\n".join(e.get("full_line") or e.get("snippet") or "" for e in errs)
        lc_line = ""
        cr = row.get("latest_credit")
        if cr:
            ts = cr.get("time_short") or ""
            val = cr["value"]
            rn_note = " (reduce_num)" if cr.get("source") == "reduce_num" else ""
            lc_line = (
                f"💰 Latest credit : {val} at {ts}{rn_note}\n"
                if ts
                else f"💰 Latest credit : {val}{rn_note}\n"
            )
        blocks.append(
            f"🔔 User ID : {uid}\n"
            f"⚠️ Error detected count : {n}\n"
            f"{lc_line}"
            f"📋 Error found List :\n"
            f"{lines_body}"
        )
    return "\n\n".join(blocks) + "\n"


def _plain_player_block(
    row: dict[str, Any],
    machine_display: str,
    *,
    error_log_mode: str,
) -> str:
    ct, et = _row_display_times(row)
    uid = row["user_id"]
    cr = row.get("latest_credit")
    if cr:
        ts = (cr.get("time_short") or "").strip()
        val = cr["value"]
        rn_note = " (reduce_num)" if cr.get("source") == "reduce_num" else ""
        credit_s = f"{val} @ {ts}{rn_note}" if ts else f"{val}{rn_note}"
    else:
        credit_s = "(none)"
    time_s = ct or et or "(n/a)"
    errs = row.get("errors") or []
    lines_body = "\n".join(e.get("full_line") or e.get("snippet") or "" for e in errs)
    lines = [
        f"Time: {time_s}",
        f"Machine: {machine_display}",
        f"User ID: {uid}",
        f"Last credit: {credit_s}",
    ]
    if error_log_mode == "always":
        lines.append("Error log:")
        lines.append(lines_body if lines_body else "(no error lines)")
    elif lines_body.strip():
        lines.append("Error log:")
        lines.append(lines_body)
    return "\n".join(lines)


def format_dual_terminal_report(
    top2_any: list[dict[str, Any]],
    top2_err: list[dict[str, Any]],
    machine_display: str,
    target_date: date,
) -> str:
    """Plain text mirroring card order: latest-2 any, then latest-2 with error."""
    dstr = target_date.isoformat()
    sec1 = (
        "\n\n---\n\n".join(
            _plain_player_block(r, machine_display, error_log_mode="if_any") for r in top2_any[:2]
        )
        if top2_any
        else "(no players)"
    )
    sec2 = (
        "\n\n---\n\n".join(
            _plain_player_block(r, machine_display, error_log_mode="always") for r in top2_err[:2]
        )
        if top2_err
        else "(no players with error)"
    )
    return (
        f"--- Latest 2 players (log order, any) — Machine: {machine_display} Date: {dstr} ---\n"
        f"{sec1}\n\n"
        f"--- Latest 2 players (with error) ---\n"
        f"{sec2}\n"
    )


def format_finderror_report_terminal(payload: list[dict[str, Any]]) -> str:
    """Plain terminal output (merge + sort); kept for callers that only need text."""
    return format_finderror_terminal_from_merged(merge_finderror_by_user(payload))


def _machineerror_player_md(
    row: dict[str, Any] | None,
    *,
    machine_display: str,
    title: str,
    force_no_error_log: bool = False,
) -> str:
    if not row:
        return (
            f"{title}\n"
            f"🕐 **Time:** `n/a`\n"
            f"🖥 **Machine:** `{machine_display}`\n"
            f"🆔 **User ID:** `n/a`\n"
            f"💰 **Last credit:** `n/a` @ `n/a`\n"
            f"📋 **Error log:**\n(no error lines)"
        )
    lc = row.get("latest_credit") or {}
    ts = (lc.get("time_short") or "").strip() or "n/a"
    uid = str(row.get("user_id") or "n/a")
    val = lc.get("value")
    credit_s = "n/a" if val is None else str(val)
    rn = " (reduce_num)" if lc.get("source") == "reduce_num" else ""
    errs = row.get("errors") or []
    if force_no_error_log:
        log_md = "(no error lines)"
    elif errs:
        log_md = (
            f"({len(errs)} error entr(y/ies) — log context screenshots appear under the machine preview "
            "when image render + Lark upload succeed.)"
        )
    else:
        log_md = "(no error lines)"
    return (
        f"{title}\n"
        f"🕐 **Time:** `{ts}`\n"
        f"🖥 **Machine:** `{machine_display}`\n"
        f"🆔 **User ID:** `{uid}`\n"
        f"💰 **Last credit:** `{credit_s}` @ `{ts}`{rn}\n"
        f"📋 **Error log:**\n{log_md}"
    )


def _wrap_log_line(s: str, width: int = 118) -> list[str]:
    """Split a long log line into fixed-width chunks for bitmap rendering."""
    t = (s or "").replace("\r", "").replace("\t", "    ")
    if len(t) <= width:
        return [t] if t else [""]
    out: list[str] = []
    i = 0
    while i < len(t):
        out.append(t[i : i + width])
        i += width
    return out


def _strip_ctx_line_for_screenshot(line: str) -> str:
    """
    Parser prefixes lines with ``>> `` (error) or three spaces (context); reference log UI has none.
    """
    s = (line or "").replace("\r", "")
    if s.startswith(">> "):
        return s[3:]
    if s.startswith("   "):
        return s[3:]
    return s


def _compact_error_ctx_lines(lines: list[str], *, max_lines: int = 9, max_chars: int = 220) -> list[str]:
    """
    Keep screenshot payload small/stable: exact line count window (default 9),
    each line truncated to max_chars with ellipsis.
    """
    out: list[str] = []
    src = lines[:] if lines else [">> (empty)"]
    for ln in src[: max(1, int(max_lines))]:
        t = _strip_ctx_line_for_screenshot(ln)
        t = t.replace("\t", "    ")
        if len(t) > max_chars:
            t = t[: max_chars - 1] + "…"
        out.append(t)
    return out


# Log viewer style (user reference): light gray canvas, black monospace, no chrome.
_ERROR_CTX_BG = (244, 244, 244)  # #F4F4F4
_ERROR_CTX_FG = (0, 0, 0)


def _load_mono_font_for_error_png(*, font_px: int) -> Optional[Any]:
    """Monospace stack aligned with macOS log / terminal look (SF Mono / Menlo / Courier)."""
    try:
        from PIL import ImageFont
    except Exception:
        return None
    size = max(10, min(40, int(font_px)))
    candidates = [
        ("/System/Library/Fonts/Supplemental/SFMono-Regular.otf", False),
        ("/System/Library/Fonts/SFNSMono.ttf", False),
        ("/System/Library/Fonts/Supplemental/Menlo.ttc", True),
        ("/Library/Fonts/Menlo.ttc", True),
        ("/System/Library/Fonts/Supplemental/Courier New.ttf", False),
        ("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", False),
        ("/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf", False),
    ]
    for p, is_ttc in candidates:
        if os.path.isfile(p):
            try:
                if is_ttc or p.lower().endswith(".ttc"):
                    return ImageFont.truetype(p, size, index=0)
                return ImageFont.truetype(p, size)
            except Exception:
                continue
    try:
        return ImageFont.load_default()
    except Exception:
        return None


def _measure_text(draw, text: str, font) -> tuple[int, int]:
    try:
        bbox = draw.textbbox((0, 0), text, font=font)
        return max(1, int(bbox[2] - bbox[0])), max(1, int(bbox[3] - bbox[1]))
    except Exception:
        return max(1, len(text) * 7), 14


def _render_text_lines_png(lines: list[str], *, title: str, output_path: str) -> bool:
    """
    Render log-style PNG: #F4F4F4 background, black monospace (matches LogNavigator-style crop).
    ``title`` is ignored for pixels (reference has no header); kept for API compatibility.
    """
    del title  # design: log body only, same as user reference screenshot
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception as e:
        print(f"[checkcredit] error-context PNG: PIL not available: {e!r}", flush=True)
        return False
    dpr = _error_ctx_dpr()
    font_px = int(round(12 * dpr))
    font = _load_mono_font_for_error_png(font_px=font_px)
    if font is None:
        print("[checkcredit] error-context PNG: could not load any font", flush=True)
        return False
    # Wide wrap so long INFO|...|extra: lines behave like a log viewer (continuations from left).
    wrap_ch = max(80, int(168 / max(dpr, 1.0) ** 0.5))
    body_lines: list[str] = []
    for raw_ln in lines[:] if lines else ["(empty)"]:
        for part in _wrap_log_line(raw_ln, width=wrap_ch):
            body_lines.append(part)
    all_lines = body_lines
    probe = Image.new("RGB", (10, 10), _ERROR_CTX_BG)
    draw = ImageDraw.Draw(probe)
    max_w = 0
    line_h = int(14 * dpr)
    for ln in all_lines:
        w, h = _measure_text(draw, ln, font)
        max_w = max(max_w, w)
        line_h = max(line_h, h + max(2, int(round(3 * dpr))))
    pad_x = int(round(14 * dpr))
    pad_y = int(round(12 * dpr))
    width = min(int(4800 * dpr), max(int(640 * dpr), max_w + pad_x * 2))
    height = max(120, line_h * len(all_lines) + pad_y * 2)
    try:
        img = Image.new("RGB", (width, height), _ERROR_CTX_BG)
        d = ImageDraw.Draw(img)
        y = pad_y
        for ln in all_lines:
            safe = (ln or "").encode("utf-8", "replace").decode("utf-8")
            try:
                d.text((pad_x, y), safe, fill=_ERROR_CTX_FG, font=font)
            except Exception:
                d.text(
                    (pad_x, y),
                    safe.encode("ascii", "replace").decode("ascii"),
                    fill=_ERROR_CTX_FG,
                    font=font,
                )
            y += line_h
        img.save(output_path, format="PNG")
    except Exception as e:
        print(f"[checkcredit] error-context PNG render failed: {e!r}", flush=True)
        return False
    return True


def _render_error_context_playwright(lines: list[str], *, title: str, output_path: str, timeout_ms: int = 45_000) -> bool:
    """
    Fallback when Pillow is missing or bitmap render fails: headless Chromium → PNG.
    Pixel style matches user reference: #F4F4F4, black monospace, log body only.
    """
    del title
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        print(f"[checkcredit] error-context Playwright import failed: {e!r}", flush=True)
        return False
    dpr = _error_ctx_dpr()
    body = "\n".join(html.escape(ln or "") for ln in (lines or ["(empty)"]))
    doc = f"""<!DOCTYPE html><html><head><meta charset="utf-8"/>
<style>
html, body {{ margin:0; padding:0; background:#F4F4F4; }}
pre.wrap {{
  margin:0;
  padding:12px 14px 14px 14px;
  background:#F4F4F4;
  color:#000000;
  font-family: "SF Mono", "SFMono-Regular", ui-monospace, Menlo, Monaco, Consolas, "Courier New", monospace;
  font-size:12px;
  line-height:1.25;
  white-space:pre-wrap;
  word-break:break-word;
  border:0;
  -webkit-font-smoothing: antialiased;
}}
</style></head><body><pre class="wrap">{body}</pre></body></html>"""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = None
            try:
                context = browser.new_context(
                    viewport={"width": 1100, "height": 1600},
                    device_scale_factor=float(dpr),
                )
                page = context.new_page()
                page.set_content(doc, wait_until="domcontentloaded", timeout=timeout_ms)
                page.locator("pre.wrap").wait_for(state="visible", timeout=10_000)
                time.sleep(0.2)
                try:
                    page.locator("pre.wrap").screenshot(path=output_path, type="png")
                except Exception:
                    page.screenshot(path=output_path, full_page=True)
            finally:
                if context is not None:
                    try:
                        context.close()
                    except Exception:
                        pass
                browser.close()
        return os.path.isfile(output_path) and os.path.getsize(output_path) > 80
    except Exception as e:
        print(f"[checkcredit] error-context Playwright render failed: {e!r}", flush=True)
        return False


def _prefer_smaller_jpeg_if_pillow(png_path: str) -> str:
    """If Pillow can read the PNG, try JPEG; keep whichever file is smaller (unlink the other)."""
    if not png_path.lower().endswith(".png"):
        return png_path
    try:
        from PIL import Image
    except Exception:
        return png_path
    jpg_path = png_path[:-4] + ".jpg"
    try:
        Image.open(png_path).convert("RGB").save(jpg_path, "JPEG", quality=82, optimize=True)
        if not os.path.isfile(jpg_path):
            return png_path
        sp, sj = os.path.getsize(png_path), os.path.getsize(jpg_path)
        if sj < sp * 0.92:
            try:
                os.unlink(png_path)
            except OSError:
                pass
            return jpg_path
        try:
            os.unlink(jpg_path)
        except OSError:
            pass
    except Exception as e:
        print(f"[checkcredit] error-context JPEG compress skipped: {e!r}", flush=True)
    return png_path


def format_error_context_text_fallback(row: dict[str, Any] | None, *, max_errors: int = 6) -> str:
    """Plain markdown: ±4 lines per error when PNG / upload is unavailable."""
    if not row:
        return ""
    uid = str(row.get("user_id") or "n/a")
    parts: list[str] = []
    parts.append(f"**User `{uid}` — error log (text, ±4 lines each)**")
    for i, e in enumerate((row.get("errors") or [])[: max(1, int(max_errors))], start=1):
        ctx = e.get("context_lines") or []
        if ctx:
            body = "\n".join(ctx)
        else:
            body = (e.get("full_line") or e.get("snippet") or "").strip() or "(no line)"
        parts.append(f"```{body}```")
    return "\n\n".join(parts)


def build_error_context_screenshots(
    row: dict[str, Any] | None,
    *,
    max_errors: int = 6,
    lines_before_after: int = 4,
) -> list[dict[str, str]]:
    """
    Build local PNG screenshots for row error contexts.
    Returns list of {"path": ..., "title": ...}; each image contains 9 lines by default.
    """
    if not row:
        return []
    uid = str(row.get("user_id") or "n/a")
    errs = row.get("errors") or []
    out: list[dict[str, str]] = []
    n_err = max(1, int(max_errors or 6))
    for idx, e in enumerate(errs[:n_err], start=1):
        ctx = e.get("context_lines") or []
        if not ctx:
            one = (e.get("full_line") or e.get("snippet") or "").strip()
            ctx = [f">> {one}"] if one else [">> (no printable log line)"]
        compact_ctx = _compact_error_ctx_lines(
            ctx,
            max_lines=lines_before_after * 2 + 1,
            max_chars=520,
        )
        title = f"User {uid} error #{idx} ({lines_before_after}+1+{lines_before_after} lines)"
        path = os.path.join(
            tempfile.gettempdir(),
            f"checkcredit_errctx_{uid}_{idx}_{uuid.uuid4().hex[:8]}.png",
        )
        ok = _render_text_lines_png(compact_ctx, title=title, output_path=path)
        if not ok:
            ok = _render_error_context_playwright(compact_ctx, title=title, output_path=path)
        if not ok:
            print(
                f"[checkcredit] error-context: PNG+Playwright both failed user={uid} err#{idx}",
                flush=True,
            )
            continue
        # Keep PNG only — JPEG changes #F4F4F4 and does not match reference log viewer.
        out.append({"path": path, "title": title})
    return out


def run_finderror(
    machine_query: str,
    *,
    target_date: date | None,
    timeout_ms: int,
    base: str,
    user: str,
    pw: str,
    source: str = "navigator",
    debug_headed: bool = False,
) -> dict[str, Any]:
    """
    source:
      - \"oss\" — GET log from OSM_LOG_OSS_TEMPLATE (no browser).
      - \"navigator\" — LogNavigator UI + tail (Chromium; headless on Linux without DISPLAY).
    ``debug_headed`` (CLI ``--debug``): force visible LogNavigator Chromium regardless of env / DISPLAY.
    """
    td = target_date or date.today()
    parsed: list[dict[str, Any]] = []
    text_parts: list[str] = []
    machine_display = (machine_query or "").strip()

    if source == "oss":
        log_body, text_parts = fetch_log_via_oss(machine_query, td, timeout_sec=max(30.0, timeout_ms / 1000.0))
        machine_display = resolve_oss_machine_folder(machine_query)
        parsed = parse_user_blocks_full(log_body)
    else:
        log_body, machine_display, nav_parts = fetch_log_via_navigator(
            machine_query,
            td,
            timeout_ms=timeout_ms,
            base=base,
            user=user,
            pw=pw,
            debug_headed=debug_headed,
        )
        text_parts.extend(nav_parts)
        parsed = parse_user_blocks_full(log_body)

    header = "\n".join(text_parts)
    merged = merge_players_full(parsed)
    merged_players_ordered = merged[:]
    _sort_players_latest_credit_first(merged_players_ordered)
    top2_err = select_top2_error_players(merged)
    top2_any = select_top2_overall(merged)
    le_uid, _le_line = pick_latest_error_uid(merged)
    la_uid, _la_line = pick_latest_any_uid(merged)
    same_uid = bool(le_uid and la_uid and le_uid == la_uid)

    report = format_dual_terminal_report(top2_any, top2_err, machine_display, td)
    plain = f"{header}\n\n{report}" if header else report
    np_followup = build_np_followup_payload(
        top2_any, top2_err, machine_display, td, merged_players_ordered
    )
    if isinstance(np_followup, dict):
        _be = str(np_followup.get("third_http_backend") or "").strip().upper() or "NP"
        if not le_uid:
            same_line = (
                f"Tap **1**–**N** → **Third Http / Detail** (`{_be}` backend) · "
                f"点 **1–N** → **Third Http / Detail**（**{_be}** 后端，与各机台路由一致）。"
            )
            _choices = np_followup.get("np_choices") or []
            if _choices:
                np_followup["np_choice_intro"] = (
                    f"**无 error log 的玩家**（解析未命中 error；**{_be}** 后端与其它机台同一 checkcredit 流程）：\n"
                    f"**Players with no error log** (`{_be}` routed backend — same flow for all cabinets):"
                )
            else:
                np_followup["np_choice_intro"] = (
                    "**本日志未解析到 error，且没有可列出的玩家。**\n"
                    "**No error lines parsed and no players to list.**"
                )
        elif same_uid and le_uid and la_uid:
            same_line = "Same player: last activity in log and last error line refer to this user ID."
        else:
            same_line = "Different: last player in log != last player with error."
        np_followup["same_last_line"] = same_line
        np_followup["latest_err_uid"] = le_uid or ""
        np_followup["latest_any_uid"] = la_uid or ""
        noerr_row = None
        for r in merged_players_ordered:
            if not (r.get("errors") or []):
                noerr_row = r
                break
        err_row = top2_err[0] if top2_err else None
        if not le_uid:
            same_player_line = (
                f"No error lines — only **no-error** players listed for **Third Http / Detail** (`{_be}`).\n"
                f"无 error 行 — 下列仅为 **无 error** 玩家（**{_be}** · Third Http / Detail）。"
            )
        elif same_uid and le_uid and la_uid:
            same_player_line = (
                "Last player out and Last player with error is same player"
            )
        elif le_uid and la_uid:
            same_player_line = (
                "Last player out and Last player with error is different player"
            )
        else:
            same_player_line = (
                'No error > 0 in log - cannot compare "last with error" to "last in log".'
            )
        md_chunks = [same_player_line, ""]
        md_chunks.append(
            _machineerror_player_md(
                noerr_row,
                machine_display=machine_display,
                title="For player with no error",
                force_no_error_log=True,
            )
        )
        if err_row:
            md_chunks.append("")
            md_chunks.append(
                _machineerror_player_md(
                    err_row,
                    machine_display=machine_display,
                    title="For player with error",
                )
            )
        np_followup["machineerror_context_md"] = "\n".join(md_chunks)
    return {
        "text": plain,
        "merged_players": merged_players_ordered,
        "machine_display_resolved": machine_display,
        "target_date": td,
        "np_followup": np_followup,
        "lark_card": build_latest_two_overall_lark_card(
            top2_any,
            machine_display=machine_display,
            target_date=td,
        ),
        "lark_card_summary": build_latest_two_error_lark_card(
            top2_err,
            machine_display=machine_display,
            target_date=td,
        ),
        # Third card: NP / third-HTTP player picker (buttons 1..N); fourth: same-last-player summary.
        "lark_card_candidates": build_np_choice_lark_card(
            np_followup["np_choices"],
            target_date_iso=str(np_followup.get("target_date") or ""),
            machine_display=str(np_followup.get("machine_display") or ""),
            third_http_backend=str(np_followup.get("third_http_backend") or "NP"),
        ),
        "lark_card_same_player": build_same_latest_players_card(
            machine_display=machine_display,
            target_date=td,
            latest_err_uid=le_uid,
            latest_any_uid=la_uid,
            same_uid=same_uid,
            third_http_backend=str(np_followup.get("third_http_backend") or ""),
        ),
    }


# ----- NP backend — Log Third Http Req (Duty Bot /npthirdhttp) -----
NP_BACKEND_DEFAULT_BASE = os.environ.get("NP_BACKEND_BASE", "https://backend-np.osmplay.com").rstrip("/")
NP_BACKEND_WINDOW_MINUTES = int(os.environ.get("NP_BACKEND_WINDOW_MINUTES", "10"))
try:
    NP_BACKEND_MAX_PAGES = max(1, int(os.environ.get("NP_BACKEND_MAX_PAGES", "20").strip() or "20"))
except ValueError:
    NP_BACKEND_MAX_PAGES = 20


def _np_amount_match_eps() -> float:
    """Max |request_amount − log_credit| to accept (some backends round differently). Default 0.05."""
    try:
        return max(0.0, float(os.environ.get("NP_BACKEND_AMOUNT_EPS", "0.05").strip() or "0.05"))
    except ValueError:
        return 0.05


def _np_backend_env_cred() -> tuple[str, str]:
    u = (os.environ.get("NP_BACKEND_USER") or "").strip()
    p = (os.environ.get("NP_BACKEND_PASSWORD") or "").strip()
    return u, p


_WINFORD_NP_BASE = "https://backend-winford.osmplay.com".rstrip("/")
_DHS_BACKEND_BASE = "https://backend-dhs.osmplay.com".rstrip("/")
_NCH_BACKEND_BASE = "https://backend-nc.osmplay.com".rstrip("/")
_TBP_BACKEND_BASE = "https://backend-tbp.osmplay.com".rstrip("/")
_CP_BACKEND_BASE = "https://backend.osmplay.com".rstrip("/")
_MDR_BACKEND_BASE = "https://backend-midori.osmplay.com".rstrip("/")
_MDR_BACKEND_DEFAULT_USER = "mdr-omduty"
_MDR_BACKEND_DEFAULT_PASSWORD = "mdr-omduty"


def _np_tbp_amount_scale() -> float:
    """Divide Request JSON numeric amount by this before comparing to log credit (TBP-only caller)."""
    try:
        v = float(os.environ.get("TBP_THIRD_HTTP_AMOUNT_SCALE", "1").strip() or "1")
        return v if v > 0 else 1.0
    except ValueError:
        return 1.0


def _np_use_dhs_log_backend(machine_display: str | None) -> bool:
    """DHS cabinet — folder / last path segment starts with ``DHS`` (e.g. ``DHS3178``, ``DHS8173``)."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)DHS", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("DHS"))


def _np_use_nch_log_backend(machine_display: str | None) -> bool:
    """NCH cabinet — folder / last path segment starts with ``NCH`` (e.g. ``NCH1171``)."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)NCH", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("NCH"))


def _np_use_cp_log_backend(machine_display: str | None) -> bool:
    """CP cabinet — folder / last path segment starts with ``CP`` (e.g. ``CP7178``, ``CP0231``)."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)CP", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("CP"))


def _np_use_osm_log_backend(machine_display: str | None) -> bool:
    """OSM cabinet — folder / last path segment starts with ``OSM`` (e.g. ``OSM7178``). Same backend as ``CP*``."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)OSM", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("OSM"))


def _np_use_backend_osmplay_com(machine_display: str | None) -> bool:
    """``https://backend.osmplay.com`` — ``CP*`` or ``OSM*`` (shared ``CP_BACKEND_*`` creds and EGM login redirect)."""
    return _np_use_cp_log_backend(machine_display) or _np_use_osm_log_backend(machine_display)


def _np_use_mdr_log_backend(machine_display: str | None) -> bool:
    """MDR cabinet — folder / last path segment starts with ``MDR`` (e.g. ``MDR7178``)."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)MDR", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("MDR"))


def _np_use_tbp_log_backend(machine_display: str | None) -> bool:
    """TBP cabinet — folder / last path segment starts with ``TBP`` (e.g. ``TBP8641``)."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)TBP", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("TBP"))


def _np_use_winford_log_backend(machine_display: str | None) -> bool:
    """
    Use Winford ``backend-winford`` Log Third Http instead of NP.

    - Any **folder / last path segment** starting with ``WF`` (case-insensitive), e.g. ``WF8123``,
      ``WF8173``, ``MINIPC/WF8123``.
    - ``winford`` anywhere in the machine string.
    - ``NWR8173`` (alnum or substring): digits-only query ``8173`` → default OSS template ``NWR{n}``.
    """
    raw = (machine_display or "").strip()
    if not raw:
        return False
    if re.search(r"(?i)winford", raw):
        return True
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)WF", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    if alnum and (alnum == "NWR8173" or "NWR8173" in alnum):
        return True
    return False


def _np_log_backend_tag(machine_display: str | None) -> str:
    """Short label for Lark / Duty Bot: ``DHS``, ``NCH``, ``OSM``, ``CP``, ``MDR``, ``TBP``, ``WF``, or ``NP``."""
    if _np_use_dhs_log_backend(machine_display):
        return "DHS"
    if _np_use_nch_log_backend(machine_display):
        return "NCH"
    if _np_use_osm_log_backend(machine_display):
        return "OSM"
    if _np_use_cp_log_backend(machine_display):
        return "CP"
    if _np_use_mdr_log_backend(machine_display):
        return "MDR"
    if _np_use_tbp_log_backend(machine_display):
        return "TBP"
    if _np_use_winford_log_backend(machine_display):
        return "WF"
    return "NP"


def _np_resolve_backend(machine_display: str | None) -> tuple[str, str, str]:
    """
    (base_url, username, password) for Log Third Http Req.

    **DHS** (machine label ``DHS*``) → ``backend-dhs.osmplay.com`` + ``DHS_BACKEND_*`` (else ``NP_BACKEND_*``).
    **NCH** (machine label ``NCH*``) → ``backend-nc.osmplay.com`` + ``NCH_BACKEND_*`` (else ``NP_BACKEND_*``).
    **CP** / **OSM** (machine labels ``CP*`` / ``OSM*``) → ``backend.osmplay.com`` + ``CP_BACKEND_USER`` / ``CP_BACKEND_PASSWORD``.
    **MDR** (machine label ``MDR*``) → ``backend-midori.osmplay.com`` + ``MDR_BACKEND_*``
    (default ``mdr-omduty`` / ``mdr-omduty`` when unset).
    **TBP** (machine label ``TBP*``) → ``backend-tbp.osmplay.com`` + ``TBP_BACKEND_*``.
    **Winford** (``WF*``, ``winford``, ``NWR8173`` OSS alias) → ``backend-winford`` + ``WF_BACKEND_*``
    (default ``omduty1``).
    Otherwise → ``NP_BACKEND_BASE`` / ``NP_BACKEND_USER`` / ``NP_BACKEND_PASSWORD``.
    """
    if _np_use_dhs_log_backend(machine_display):
        nu, npw = _np_backend_env_cred()
        u = (os.environ.get("DHS_BACKEND_USER") or nu).strip()
        p = (os.environ.get("DHS_BACKEND_PASSWORD") or npw).strip()
        return _DHS_BACKEND_BASE, u, p
    if _np_use_nch_log_backend(machine_display):
        nu, npw = _np_backend_env_cred()
        u = (os.environ.get("NCH_BACKEND_USER") or nu).strip()
        p = (os.environ.get("NCH_BACKEND_PASSWORD") or npw).strip()
        return _NCH_BACKEND_BASE, u, p
    if _np_use_backend_osmplay_com(machine_display):
        u = (os.environ.get("CP_BACKEND_USER") or "").strip()
        p = (os.environ.get("CP_BACKEND_PASSWORD") or "").strip()
        return _CP_BACKEND_BASE, u, p
    if _np_use_mdr_log_backend(machine_display):
        u = (os.environ.get("MDR_BACKEND_USER") or _MDR_BACKEND_DEFAULT_USER).strip() or _MDR_BACKEND_DEFAULT_USER
        p = (os.environ.get("MDR_BACKEND_PASSWORD") or _MDR_BACKEND_DEFAULT_PASSWORD).strip() or (
            _MDR_BACKEND_DEFAULT_PASSWORD
        )
        return _MDR_BACKEND_BASE, u, p
    if _np_use_tbp_log_backend(machine_display):
        u = (os.environ.get("TBP_BACKEND_USER") or "").strip()
        p = (os.environ.get("TBP_BACKEND_PASSWORD") or "").strip()
        return _TBP_BACKEND_BASE, u, p
    if _np_use_winford_log_backend(machine_display):
        u = (os.environ.get("WF_BACKEND_USER") or "omduty1").strip() or "omduty1"
        p = (os.environ.get("WF_BACKEND_PASSWORD") or "omduty1").strip() or "omduty1"
        return _WINFORD_NP_BASE, u, p
    return NP_BACKEND_DEFAULT_BASE, *_np_backend_env_cred()


def _np_combine_date_and_credit_time(date_iso: str, time_short: str) -> datetime:
    """date_iso YYYY-MM-DD; time_short like 23:55:12.092 or 23:55:12."""
    t = (time_short or "").strip()
    if not t:
        raise ValueError("empty credit time")
    parts = t.split(".")
    hms = parts[0]
    micro = 0
    if len(parts) > 1 and re.match(r"^\d+", parts[1]):
        ms = parts[1][:3].ljust(3, "0")
        micro = int(ms) * 1000  # milliseconds → microseconds
    dt_s = f"{date_iso} {hms}"
    d = datetime.strptime(dt_s, "%Y-%m-%d %H:%M:%S")
    return d.replace(microsecond=micro)


def _np_format_element_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _np_window_strings(date_iso: str, time_short: str) -> tuple[str, str]:
    mid = _np_combine_date_and_credit_time(date_iso, time_short)
    lo = mid - timedelta(minutes=NP_BACKEND_WINDOW_MINUTES)
    hi = mid + timedelta(minutes=NP_BACKEND_WINDOW_MINUTES)
    return _np_format_element_datetime(lo), _np_format_element_datetime(hi)


def _np_parse_cell_datetime(text: str, date_iso: str) -> datetime | None:
    """Parse Request/Response time from a table cell (full datetime or time-only)."""
    text = (text or "").strip().replace("\n", " ")
    if not text:
        return None
    m = re.search(
        r"(?P<d>\d{4}-\d{2}-\d{2})\s+(?P<t>\d{2}:\d{2}:\d{2})(?:\.(?P<ms>\d{1,6}))?",
        text,
    )
    if m:
        d = m.group("d")
        t = m.group("t")
        ms = (m.group("ms") or "").strip()
        base = datetime.strptime(f"{d} {t}", "%Y-%m-%d %H:%M:%S")
        if ms:
            ms3 = (ms + "000")[:3]
            micro = int(ms3) * 1000
            return base.replace(microsecond=micro)
        return base
    m = re.search(r"\b(\d{2}:\d{2}:\d{2}(?:\.\d+)?)\b", text)
    if m:
        return _np_combine_date_and_credit_time(date_iso, m.group(1))
    return None


def _np_log_third_http_header_columns(page) -> dict[str, int]:
    """Map header label → column index for Log Third Http Req table."""
    out: dict[str, int] = {}
    ths = page.locator(".el-table__header-wrapper thead tr th")
    if ths.count() == 0:
        ths = page.locator(".el-table__header thead tr th")
    n = ths.count()
    for i in range(n):
        raw = (ths.nth(i).inner_text() or "").strip().lower()
        h = " ".join(raw.split())
        if "request" in h and "time" in h:
            out["request"] = i
        elif "response" in h and "time" in h:
            out["response"] = i
        elif "event" in h and "type" in h:
            out["event"] = i
    return out


def _np_row_req_resp_times(
    row,
    date_iso: str,
    cols: dict[str, int],
) -> tuple[datetime | None, datetime | None]:
    """Read request/response datetimes from a data row; fallback: first two times in row."""
    tds = row.locator("td")
    n = tds.count()
    req_i = cols.get("request")
    resp_i = cols.get("response")
    req_dt: datetime | None = None
    resp_dt: datetime | None = None
    if req_i is not None and req_i < n:
        req_dt = _np_parse_cell_datetime((tds.nth(req_i).inner_text() or ""), date_iso)
    if resp_i is not None and resp_i < n:
        resp_dt = _np_parse_cell_datetime((tds.nth(resp_i).inner_text() or ""), date_iso)
    if req_dt is not None or resp_dt is not None:
        return req_dt, resp_dt
    found: list[datetime] = []
    for j in range(n):
        dt = _np_parse_cell_datetime((tds.nth(j).inner_text() or ""), date_iso)
        if dt is not None:
            found.append(dt)
    if len(found) >= 2:
        return found[0], found[1]
    if len(found) == 1:
        return found[0], None
    return None, None


def _np_row_time_distance_to_target(
    req_dt: datetime | None,
    resp_dt: datetime | None,
    target_dt: datetime,
) -> float | None:
    """Smallest |dt - target| among request and response; None if no times."""
    deltas: list[float] = []
    if req_dt is not None:
        deltas.append(abs((req_dt - target_dt).total_seconds()))
    if resp_dt is not None:
        deltas.append(abs((resp_dt - target_dt).total_seconds()))
    if not deltas:
        return None
    return min(deltas)


def _np_cell_looks_like_recharge_event(cell_text: str) -> bool:
    raw = cell_text or ""
    t = raw.strip().lower()
    if not t:
        return False
    if t == "recharge" or "recharge" in t:
        return True
    if "充值" in raw:
        return True
    return False


def _np_row_is_recharge(rows, ev_i: int, i: int) -> bool:
    row = rows.nth(i)
    tds = row.locator("td")
    tc = tds.count()
    if tc == 0:
        return False
    if tc > ev_i:
        if _np_cell_looks_like_recharge_event(tds.nth(ev_i).inner_text() or ""):
            return True
    for j in range(tc):
        if _np_cell_looks_like_recharge_event(tds.nth(j).inner_text() or ""):
            return True
    return False


def _np_list_recharge_indices_time_ordered(
    page,
    rows,
    n: int,
    date_iso: str,
    time_short: str,
) -> list[int]:
    """
    All Event Type == recharge rows, ordered by smallest distance from Request/Response time
    to log credit time (best match first; rows without parseable times last).
    """
    cols = _np_log_third_http_header_columns(page)
    ev_i = cols.get("event", 2)
    target_dt = _np_combine_date_and_credit_time(date_iso, time_short)

    scored: list[tuple[float, int]] = []
    no_time_fallback: list[int] = []

    for i in range(n):
        if not _np_row_is_recharge(rows, ev_i, i):
            continue
        row = rows.nth(i)
        req_dt, resp_dt = _np_row_req_resp_times(row, date_iso, cols)
        dist = _np_row_time_distance_to_target(req_dt, resp_dt, target_dt)
        if dist is None:
            no_time_fallback.append(i)
        else:
            scored.append((dist, i))

    scored.sort(key=lambda x: (x[0], x[1]))
    return [idx for _, idx in scored] + no_time_fallback


def _np_detail_request_section(full_text: str) -> str:
    """
    Keep Request JSON only so ``amount`` / ``machineId`` are not confused with response.

    Do **not** trim at the first ``response data`` from the start of the dialog text: some UIs
    flatten DOM order so **Response** appears before **Request** in ``inner_text``. In that case
    ``full_text[:first_response]`` omits the Request block entirely and matching always fails.
    """
    if not full_text:
        return ""
    low = full_text.lower()

    def _first_of(labels: tuple[str, ...]) -> int:
        best = -1
        for lab in labels:
            j = low.find(lab)
            if j != -1 and (best == -1 or j < best):
                best = j
        return best

    req_i = _first_of(("request data", "请求数据"))
    resp_i = _first_of(("response data", "响应数据"))

    if req_i != -1 and resp_i != -1:
        if req_i < resp_i:
            return full_text[req_i:resp_i]
        return full_text[req_i:]
    if req_i != -1:
        return full_text[req_i:]
    if resp_i != -1:
        return full_text[:resp_i]
    return full_text


def _np_normalize_jsonish_quotes(s: str) -> str:
    """NP / browser may render curly quotes or split DOM so plain regex misses."""
    for a, b in (
        ("\u201c", '"'),
        ("\u201d", '"'),
        ("\u2018", "'"),
        ("\u2019", "'"),
    ):
        s = s.replace(a, b)
    return s


def _np_parse_machine_amount_from_request_blob(blob: str) -> tuple[str | None, float | None]:
    """
    Best-effort Request JSON fields across NP / NCH / DHS / CP / OSM / MDR / TBP / WF style payloads.
    Some cabinets use ``machineNo`` / ``add_num`` instead of ``machineId`` / ``amount``.
    """
    if not blob:
        return None, None
    s = _np_normalize_jsonish_quotes(blob)

    def _first_str_group(patterns: tuple[str, ...]) -> str | None:
        for pat in patterns:
            m = re.search(pat, s, re.I | re.DOTALL)
            if not m:
                continue
            raw = m.group(1)
            if raw is None:
                continue
            t = str(raw).strip()
            if t:
                return t.replace("\\/", "/")
        return None

    def _first_float_group(patterns: tuple[str, ...]) -> float | None:
        for pat in patterns:
            m = re.search(pat, s, re.I)
            if not m:
                continue
            try:
                return float(m.group(1))
            except (ValueError, IndexError):
                continue
        return None

    # machine id: string-valued keys first, then numeric machineId
    mid = _first_str_group(
        (
            r'"machineId"\s*:\s*"((?:\\.|[^"\\])*)"',
            r"'machineId'\s*:\s*'((?:\\.|[^'\\])*)'",
            r"machineId\s*:\s*\"([^\"]*)\"",
            r'"machineNo"\s*:\s*"((?:\\.|[^"\\])*)"',
            r"'machineNo'\s*:\s*'((?:\\.|[^'\\])*)'",
            r'"machine_no"\s*:\s*"((?:\\.|[^"\\])*)"',
            r'"machineCode"\s*:\s*"((?:\\.|[^"\\])*)"',
            r'"assetId"\s*:\s*"((?:\\.|[^"\\])*)"',
            r'"cabinetId"\s*:\s*"((?:\\.|[^"\\])*)"',
            r'"cabinet_id"\s*:\s*"((?:\\.|[^"\\])*)"',
        )
    )
    if mid is None:
        m_num = re.search(r'"machineId"\s*:\s*(\d+)\b', s)
        if m_num:
            mid = m_num.group(1).strip()

    amt = _first_float_group(
        (
            r'"amount"\s*:\s*(-?\d+(?:\.\d+)?)',
            r"'amount'\s*:\s*(-?\d+(?:\.\d+)?)",
            r'(?<![\w.])amount\s*:\s*(-?\d+(?:\.\d+)?)',
            r'"amount"\s*:\s*"(-?\d+(?:\.\d+)?)"',
            r'"add_num"\s*:\s*(-?\d+(?:\.\d+)?)',
            r'"addNum"\s*:\s*(-?\d+(?:\.\d+)?)',
            r"'add_num'\s*:\s*(-?\d+(?:\.\d+)?)",
            r'"credit"\s*:\s*(-?\d+(?:\.\d+)?)',
            r'"creditAmount"\s*:\s*(-?\d+(?:\.\d+)?)',
            r'"rechargeAmount"\s*:\s*(-?\d+(?:\.\d+)?)',
            r'"recharge_num"\s*:\s*(-?\d+(?:\.\d+)?)',
            r'"rechargeNum"\s*:\s*(-?\d+(?:\.\d+)?)',
            r'"change_num"\s*:\s*(-?\d+(?:\.\d+)?)',
            r'"changeNum"\s*:\s*(-?\d+(?:\.\d+)?)',
            r'"cur_coin"\s*:\s*(-?\d+(?:\.\d+)?)',
            r'"money"\s*:\s*(-?\d+(?:\.\d+)?)',
            r'"totalAmount"\s*:\s*(-?\d+(?:\.\d+)?)',
        )
    )
    return mid, amt


def _np_machine_substr_for_log_third_http_detail(raw: str | None) -> str:
    """
    Value used to match Request ``machineId`` inside Log Third Http **Detail** dialogs.

    Duty Bot / Lark and logs often pass cabinet labels (``DHS3189``, ``dhs3189``); JSON ``machineId``
    is usually ``…-3189`` with ``studioId`` elsewhere. **Only digit runs** are compared to ``machineId``:
    take the **longest** ``\\d+`` substring; on length ties, the **last** (suffix cabinet id, e.g. after
    ``4179-…-``). If there are no digits, use the full trimmed string (letters-only match).
    """
    s = (raw or "").strip()
    if not s:
        return s
    nums = re.findall(r"\d+", s)
    if not nums:
        return s
    best = max(len(n) for n in nums)
    longs = [n for n in nums if len(n) == best]
    return longs[-1]


def _np_machine_id_contains_substr(machine_substr: str | None, machine_id_value: str | None) -> bool:
    """
    Request ``machineId`` must contain the **detail-match** substring (case-insensitive).

    ``machine_substr`` is normalized by :func:`_np_machine_substr_for_log_third_http_detail` so
    ``dhs3189`` / ``DHS3189`` / ``NWR2074`` all reduce to the numeric token used inside ``machineId``
    (``3189``, ``2074``, …). If normalization changes the string, the original ``machine_substr`` is
    still tried as a literal fallback (non-numeric cabinet tokens in ``machineId``).
    """
    ms = (machine_substr or "").strip()
    if not ms:
        return True
    mid = (machine_id_value or "").strip()
    if not mid:
        return False
    eff = _np_machine_substr_for_log_third_http_detail(ms)
    if eff.casefold() in mid.casefold():
        return True
    if eff != ms and ms.casefold() in mid.casefold():
        return True
    return False


def _np_expected_credit_for_match(expected_credit: float | None) -> float | None:
    """
    Keep numeric log credit for amount matching (including ``0.0`` and negatives).
    Return ``None`` only when credit is missing/unparseable.
    """
    if expected_credit is None:
        return None
    try:
        v = float(expected_credit)
    except (TypeError, ValueError):
        return None
    return v


def _np_detail_matches_credit_and_machine_id(
    req_blob: str,
    machine_substr: str | None,
    expected_credit: float | None,
    *,
    amount_scale: float = 1.0,
) -> bool:
    """
    Request JSON: ``machineId`` contains machine digits; when ``expected_credit`` is not ``None``,
    ``amount`` matches within ``NP_BACKEND_AMOUNT_EPS`` (default 0.05), after optional
    ``amount_scale`` (TBP: ``TBP_THIRD_HTTP_AMOUNT_SCALE``).
    """
    mid, amt = _np_parse_machine_amount_from_request_blob(req_blob)
    if mid is None:
        return False
    if not _np_machine_id_contains_substr(machine_substr, mid):
        return False
    if expected_credit is None:
        # Machine-only match (e.g. TBP fallback): accept if amount missing; reject obvious non-positive.
        if amt is not None and amt <= 0:
            return False
        return True
    if amt is None:
        return False
    try:
        sc = float(amount_scale)
    except (TypeError, ValueError):
        sc = 1.0
    if sc <= 0:
        sc = 1.0
    scaled = float(amt) / sc
    if abs(scaled - float(expected_credit)) > _np_amount_match_eps():
        return False
    return True


def _np_indices_table_same_minute(
    page,
    rows,
    ordered_indices: list[int],
    date_iso: str,
    time_short: str,
) -> list[int]:
    """Row indices whose table Request/Response time is in the same HH:MM as log credit (closest first)."""
    target_dt = _np_combine_date_and_credit_time(date_iso, time_short)
    cols = _np_log_third_http_header_columns(page)
    scored: list[tuple[float, int]] = []
    for i in ordered_indices:
        row = rows.nth(i)
        req_dt, resp_dt = _np_row_req_resp_times(row, date_iso, cols)
        best_dist = float("inf")
        hit = False
        for dt in (req_dt, resp_dt):
            if dt is not None and _np_same_calendar_minute(dt, target_dt):
                hit = True
                best_dist = min(best_dist, abs((dt - target_dt).total_seconds()))
        if hit:
            scored.append((best_dist, i))
    scored.sort(key=lambda x: (x[0], x[1]))
    return [i for _, i in scored]


def _np_merge_same_minute_then_rest(same_minute: list[int], ordered: list[int]) -> list[int]:
    """
    Prefer same-calendar-minute table rows first, then every other recharge index in ``ordered``.
    If we only scanned ``same_minute``, rows with unparseable or off-minute table times were never
    opened — common on headless / dense tables; CLI on Mac often still hit the right row first by luck.
    """
    seen: set[int] = set()
    out: list[int] = []
    for i in same_minute:
        if i not in seen:
            seen.add(i)
            out.append(i)
    for i in ordered:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


def _np_same_calendar_minute(a: datetime, b: datetime) -> bool:
    return (
        a.year == b.year
        and a.month == b.month
        and a.day == b.day
        and a.hour == b.hour
        and a.minute == b.minute
    )


def _np_pagination_can_go_next(page) -> bool:
    """Element UI ``btn-next`` when not disabled."""
    btn = page.locator(".el-pagination button.btn-next").first
    if btn.count() == 0:
        return False
    try:
        return not btn.is_disabled()
    except Exception:
        return False


def _np_click_pagination_next(page, *, timeout_ms: int) -> None:
    btn = page.locator(".el-pagination button.btn-next").first
    btn.wait_for(state="visible", timeout=min(15_000, timeout_ms))
    btn.click(timeout=min(30_000, timeout_ms))
    page.wait_for_timeout(900)


def _np_pagination_go_first_page(page, *, timeout_ms: int) -> None:
    """Element UI: click ``btn-prev`` until disabled so a second scan starts from page 1."""
    btn = page.locator(".el-pagination button.btn-prev").first
    for _ in range(max(NP_BACKEND_MAX_PAGES, 30) + 5):
        if btn.count() == 0:
            return
        try:
            if btn.is_disabled():
                return
        except Exception:
            return
        try:
            btn.click(timeout=min(15_000, timeout_ms))
        except Exception:
            return
        page.wait_for_timeout(450)


def _np_dialog_text_layers_for_match(dlg) -> str:
    """
    ``inner_text()`` can miss or fragment JSON (syntax-highlighted spans, delayed paint). Combine
    layers so ``machineId`` / ``amount`` still match the real Request payload.
    """
    chunks: list[str] = []
    try:
        t = dlg.inner_text() or ""
        if t.strip():
            chunks.append(t)
    except Exception:
        pass
    try:
        t = dlg.text_content() or ""
        if t.strip():
            chunks.append(t)
    except Exception:
        pass
    try:
        subs = dlg.locator("pre, textarea, code")
        for i in range(min(subs.count(), 40)):
            t = subs.nth(i).inner_text() or ""
            if t.strip():
                chunks.append(t)
    except Exception:
        pass
    return "\n".join(chunks)


def _np_click_row_show_details_link(row, *, timeout_ms: int) -> None:
    """Element table action link — EN ``Show Details`` or CN ``详情`` / ``查看详情``."""
    last_err: Exception | None = None
    for pat in (
        re.compile(r"Show\s*Details", re.I),
        re.compile(r"查看详情|显示详情|^\s*详情\s*$", re.I),
    ):
        try:
            loc = row.get_by_text(pat)
            if loc.count() == 0:
                continue
            btn = loc.first
            btn.scroll_into_view_if_needed()
            btn.click(timeout=min(60_000, timeout_ms))
            return
        except Exception as e:
            last_err = e
            continue
    try:
        alt = row.locator("a, button").filter(
            has_text=re.compile(r"Show\s*Details|查看详情|显示详情|详情", re.I)
        ).first
        if alt.count():
            alt.scroll_into_view_if_needed()
            alt.click(timeout=min(60_000, timeout_ms))
            return
    except Exception as e:
        last_err = e
    raise RuntimeError(
        "Could not find or click the row detail link (Show Details / 查看详情 / 详情). "
        f"Last error: {last_err!r}"
    )


def _np_close_np_detail_dialog(page, dlg) -> None:
    """
    Close **this** detail dialog only (header X on ``dlg``).

    Do **not** use a page-wide ``.first`` close + Escape: after a wrong row, Vue may leave the old
    dialog node in the DOM (hidden) while the new Detail opens on top. ``.first`` then attaches to
    the stale layer; Escape closes the **topmost** visible dialog — the user sees the correct
    credit row dismissed even though the script was still discarding an older candidate.
    """
    try:
        btn = dlg.locator(".el-dialog__headerbtn, .el-dialog__close").first
        if btn.count():
            btn.click(timeout=5_000)
        else:
            page.keyboard.press("Escape")
    except Exception:
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
    page.wait_for_timeout(450)


def _np_try_screenshot_matching_detail(
    page,
    rows,
    ordered_indices: list[int],
    *,
    machine_substr: str | None,
    expected_credit: float | None,
    out_path: str,
    timeout_ms: int,
    dialog_settle_ms: int = 900,
    amount_scale: float = 1.0,
) -> bool:
    """
    For each candidate row: accept when Request JSON ``machineId`` contains the machine digits and
    ``amount`` matches latest credit. Uses multiple text layers — the correct row often opens first
    but ``inner_text()`` alone may omit JSON bits; that looked like a wrong row and closed the dialog.

    Always bind to **.last** details-dialog after each row click so we read / screenshot the **newest**
    modal, not a hidden stale ``.first`` node left in the DOM from a previous attempt.
    """
    ms = (machine_substr or "").strip()
    need = bool(ms) or expected_credit is not None
    if not need:
        return False

    for pick_i in ordered_indices:
        row = rows.nth(pick_i)
        _np_click_row_show_details_link(row, timeout_ms=timeout_ms)
        dlg_sel = page.locator(
            ".el-dialog.details-dialog, div[role='dialog'].details-dialog"
        ).last
        dlg_sel.wait_for(state="visible", timeout=timeout_ms)
        page.wait_for_timeout(max(300, dialog_settle_ms))
        try:
            dlg_sel.get_by_text(
                re.compile(
                    r"machineId|machine\s*id|machineNo|machine_no|amount|加款|充值|金额",
                    re.I,
                )
            ).first.wait_for(state="visible", timeout=min(8_000, timeout_ms))
        except Exception:
            pass
        full_txt = dlg_sel.inner_text() or ""
        layers = _np_dialog_text_layers_for_match(dlg_sel)
        blob = _np_detail_request_section(full_txt)
        ok = _np_detail_matches_credit_and_machine_id(
            blob,
            machine_substr,
            expected_credit,
            amount_scale=amount_scale,
        )
        if not ok:
            ok = _np_detail_matches_credit_and_machine_id(
                layers,
                machine_substr,
                expected_credit,
                amount_scale=amount_scale,
            )
        if not ok:
            ok = _np_detail_matches_credit_and_machine_id(
                full_txt,
                machine_substr,
                expected_credit,
                amount_scale=amount_scale,
            )
        if not ok:
            blob2 = _np_detail_request_section(layers)
            ok = _np_detail_matches_credit_and_machine_id(
                blob2,
                machine_substr,
                expected_credit,
                amount_scale=amount_scale,
            )
        if not ok:
            ok = _np_detail_matches_credit_and_machine_id(
                "\n".join((blob, layers, full_txt)),
                machine_substr,
                expected_credit,
                amount_scale=amount_scale,
            )
        if ok:
            _np_capture_detail_dialog_screenshot(
                page,
                dlg_sel,
                out_path,
                timeout_ms=timeout_ms,
                settle_ms=dialog_settle_ms,
            )
            return True
        _np_close_np_detail_dialog(page, dlg_sel)

    return False


def _np_capture_detail_dialog_screenshot(
    page,
    dlg,
    out_path: str,
    *,
    timeout_ms: int,
    settle_ms: int,
) -> None:
    """
    Capture the *real* Log Third Http Detail dialog as shown in browser.
    No synthetic rendering; only wait for content/layout settle before screenshot.
    """
    try:
        dlg.get_by_text(
            re.compile(r"Request\s*Data|Response\s*Data|Request\s*Time|Api\s*Name", re.I)
        ).first.wait_for(state="visible", timeout=min(10_000, timeout_ms))
    except Exception:
        pass
    page.wait_for_timeout(max(500, settle_ms))
    last = None
    for _ in range(6):
        try:
            box = dlg.bounding_box()
        except Exception:
            box = None
        if box and last:
            dx = abs(float(box.get("x", 0)) - float(last.get("x", 0)))
            dy = abs(float(box.get("y", 0)) - float(last.get("y", 0)))
            dw = abs(float(box.get("width", 0)) - float(last.get("width", 0)))
            dh = abs(float(box.get("height", 0)) - float(last.get("height", 0)))
            if max(dx, dy, dw, dh) < 0.5:
                break
        last = box
        page.wait_for_timeout(180)
    # User-requested: capture ONLY the real Detail small window.
    dlg.screenshot(path=out_path, animations="disabled")


def _np_truthy_env(*names: str) -> bool:
    for name in names:
        v = (os.environ.get(name) or "").strip().lower()
        if v in ("1", "true", "yes", "on"):
            return True
    return False


def _np_backend_playwright_headless() -> bool:
    """Prefer visible window when any *HEADED* env is set (NP / WF / generic alias for Duty Bot debug)."""
    if _np_truthy_env(
        "NP_BACKEND_HEADED",
        "WF_THIRD_HTTP_HEADED",
        "THIRD_HTTP_PLAYWRIGHT_HEADED",
    ):
        return False
    if os.environ.get("NP_BACKEND_HEADLESS", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    if sys.platform == "linux" and not (os.environ.get("DISPLAY") or "").strip():
        return True
    return False


def _np_egm_machine_search_token(machine_display: str | None, machine_substr: str | None) -> str:
    """
    EGM Status Search input token: always prefer number-like machine id (e.g. ``MDR7066`` -> ``7066``).
    """
    ms = _np_machine_substr_for_log_third_http_detail(machine_substr)
    if ms:
        return ms
    md = (machine_display or "").strip()
    if md:
        tok = _np_machine_substr_for_log_third_http_detail(md)
        if tok:
            return tok
        tok2 = machine_match_substr_from_display(md)
        if tok2:
            return tok2
    return ""


def _egm_cog_button_is_actionable(btn: Any) -> bool:
    """True if the small EGM op button can receive a click (not disabled in DOM)."""
    try:
        if btn.count() == 0:
            return False
        cls = (btn.get_attribute("class") or "")
        if "is-disabled" in cls:
            return False
        aria_d = (btn.get_attribute("aria-disabled") or "").strip().lower()
        if aria_d == "true":
            return False
        dis = btn.get_attribute("disabled")
        if dis is not None:
            dsl = str(dis).strip().lower()
            if dsl and dsl not in ("false", "0"):
                return False
        return bool(btn.is_enabled())
    except Exception:
        return False


def _egm_first_enabled_button_matching(scope: Any, pat: re.Pattern[str]) -> Any | None:
    """First enabled ``button`` under ``scope`` whose visible text matches ``pat`` (e.g. CCTV)."""
    btns = scope.locator("button")
    n = btns.count()
    for i in range(n):
        b = btns.nth(i)
        try:
            if not _egm_cog_button_is_actionable(b):
                continue
            tx = (b.inner_text() or "").strip()
            if pat.search(tx):
                return b
        except Exception:
            continue
    return None


def _pick_enabled_egm_cog_button(op_cell: Any) -> Any:
    """
    EGM Status list: last column has small buttons; we only click a **cog** icon
    button that is **enabled**. The 3rd button (index 2) is preferred when it matches.
    If every cog is ``disabled``, Playwright would wait until timeout — avoid that.
    """
    danger_re = re.compile(r"Maintenance|Kick\s*Out", re.I)
    op_btns = op_cell.locator("button.el-button--small")
    nbtn = op_btns.count()
    if nbtn <= 0:
        raise RuntimeError("Operation column has no clickable buttons; aborting.")
    safe: list[tuple[int, Any]] = []
    for i in range(nbtn):
        b = op_btns.nth(i)
        try:
            if b.locator("i.fa.fa-cog").count() == 0:
                continue
            bt = (b.inner_text() or "").strip()
        except Exception:
            continue
        if danger_re.search(bt):
            continue
        safe.append((i, b))
    if not safe:
        raise RuntimeError("No safe cog operation button found; refusing unsafe click.")

    page = op_cell.page
    for _ in range(3):
        for i, b in safe:
            if i == 2 and _egm_cog_button_is_actionable(b):
                return b
        for i, b in safe:
            if _egm_cog_button_is_actionable(b):
                return b
        try:
            page.wait_for_timeout(1500)
        except Exception:
            break

    raise RuntimeError(
        "EGM row found, but every cog operation button is disabled "
        "(machine offline / not session-ready / permission or UI lock). "
        "Open EGM Status in browser to confirm; checkcredit continues without preview image."
    )


def screenshot_egm_status_window(
    *,
    machine_display: str | None,
    machine_substr: str | None = None,
    timeout_ms: int = 120_000,
    headed: bool | None = None,
) -> str:
    """
    Login to backend EGM page and screenshot the operation small window safely.

    Flow:
    1) open ``/egm/egmStatusList`` (same host as resolved backend),
    2) fill Search with machine number token,
    3) click the first **enabled** operation button that has the **cog** icon (prefers the 3rd
       button when it is enabled; skips Maintenance / Kick Out),
    4) in dialog: if button shows ``Hide Grid``, click once to make it ``Show Grid``,
    5) screenshot the dialog (operation window), not the full page.
    """
    md = (machine_display or "").strip()
    if not md:
        raise RuntimeError("EGM screenshot requires machine_display (do not default to NP route).")
    base, user, pw = _np_resolve_backend(md)
    tag = _np_log_backend_tag(md)
    if not user or not pw:
        raise RuntimeError(f"Missing credentials for {tag} EGM screenshot backend.")
    tok = _np_egm_machine_search_token(md, machine_substr)
    if not tok:
        raise RuntimeError("Could not derive machine number token for EGM Search.")

    login_url = f"{base}/login?redirect=%2Fegm%2FegmStatusList"
    egm_url = f"{base}/egm/egmStatusList"
    from playwright.sync_api import sync_playwright

    if headed is True:
        headless = False
    elif headed is False:
        headless = True
    else:
        headless = _np_backend_playwright_headless()

    out_fd, out_path = tempfile.mkstemp(suffix=".png", prefix="np_egm_status_")
    os.close(out_fd)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            context = browser.new_context(
                viewport={"width": 1600, "height": 900},
                ignore_https_errors=True,
                device_scale_factor=2,
            )
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            page.goto(login_url, wait_until="domcontentloaded")
            page.wait_for_timeout(900)

            pwd_box = page.locator('input[type="password"]').first
            pwd_box.wait_for(state="visible", timeout=min(30_000, timeout_ms))
            form = pwd_box.locator("xpath=ancestor::form[1]")
            if form.count():
                tin = form.locator(
                    'input[type="text"], input:not([type]), input[type="tel"], input[type="email"]'
                ).first
                tin.fill(user)
            else:
                page.locator('input[type="text"]').first.fill(user)
            pwd_box.fill(pw)
            lb = page.get_by_role("button", name=re.compile(r"login|sign in|log in", re.I))
            if lb.count():
                lb.first.click()
            else:
                page.locator('button[type="submit"], button.el-button--primary').first.click()

            page.wait_for_timeout(1800)
            if "/egm/egmStatusList" not in (page.url or ""):
                page.goto(egm_url, wait_until="domcontentloaded")

            page.wait_for_selector(".app-container, .filter-container", timeout=timeout_ms)
            search_in = page.locator(".el-form-item").filter(has_text=re.compile(r"Search", re.I)).locator(
                "input.el-input__inner"
            ).first
            if search_in.count() == 0:
                cands = page.locator(".filter-container input.el-input__inner")
                if cands.count() == 0:
                    raise RuntimeError("Could not find EGM Search input.")
                search_in = cands.last
            search_in.click()
            search_in.fill("")
            search_in.fill(tok)
            page.keyboard.press("Enter")
            try:
                view_btn = page.locator(".filter-container button.el-button--small").filter(
                    has_text=re.compile(r"View|查看", re.I)
                ).first
                if view_btn.count():
                    view_btn.click(timeout=min(30_000, timeout_ms))
            except Exception:
                pass
            page.wait_for_timeout(1000)
            try:
                page.wait_for_function(
                    "() => !Array.from(document.querySelectorAll('.el-loading-mask')).some(x => x && x.offsetParent !== null)",
                    timeout=min(timeout_ms, 30_000),
                )
            except Exception:
                pass

            tbody = page.locator(".el-table__body tbody").first
            tbody.wait_for(state="visible", timeout=timeout_ms)
            rows = page.locator(".el-table__body tr.el-table__row")
            n = rows.count()
            if n <= 0:
                raise RuntimeError(f"No EGM rows after Search `{tok}`.")

            target = None
            tok_re = re.compile(re.escape(tok), re.I)
            try:
                cands = rows.filter(has_text=tok_re)
                if cands.count() > 0:
                    target = cands.first
            except Exception:
                target = None
            if target is None:
                tok_cf = tok.casefold()
                for i in range(n):
                    row = rows.nth(i)
                    try:
                        row_txt = (row.inner_text(timeout=min(4_000, timeout_ms)) or "").casefold()
                    except Exception:
                        continue
                    if tok_cf in row_txt:
                        target = row
                        break
            if target is None:
                target = rows.nth(0)

            op_cell = target.locator("td").last
            cog_btn = _pick_enabled_egm_cog_button(op_cell)
            cog_btn.click(timeout=min(60_000, timeout_ms))

            dlg = page.locator(".el-dialog.add-floor, div[role='dialog'].add-floor").last
            dlg.wait_for(state="visible", timeout=timeout_ms)
            page.wait_for_timeout(1000)
            grid_btn = dlg.locator("button.el-button--primary.el-button--mini").filter(
                has_text=re.compile(r"Hide\s*Grid|Show\s*Grid", re.I)
            ).first
            if grid_btn.count():
                gtxt = (grid_btn.inner_text() or "").strip()
                if re.search(r"Hide\s*Grid", gtxt, re.I):
                    grid_btn.click(timeout=min(15_000, timeout_ms))
                    page.wait_for_timeout(350)
            # Capture only the small operation window (same as user screenshot), not whole page.
            dlg.screenshot(path=out_path, animations="disabled", scale="css")
        finally:
            browser.close()
    return out_path


def screenshot_egm_cctv_window(
    *,
    machine_display: str | None,
    machine_substr: str | None = None,
    timeout_ms: int = 120_000,
    headed: bool | None = None,
) -> str:
    """
    EGM Status: click **CCTV** (row or inside operation dialog), then screenshot the dialog — no credit / log checks.

    Tries **CCTV** on the row operation buttons first; if absent, opens the usual cog dialog and clicks **CCTV**
    inside ``.el-dialog__body``. Waits briefly for a ``video`` element when present, then captures the topmost
    visible ``.el-dialog``.
    """
    md = (machine_display or "").strip()
    if not md:
        raise RuntimeError("CCTV screenshot requires machine_display (machine label / id token).")
    base, user, pw = _np_resolve_backend(md)
    tag = _np_log_backend_tag(md)
    if not user or not pw:
        raise RuntimeError(f"Missing credentials for {tag} EGM CCTV backend.")
    tok = _np_egm_machine_search_token(md, machine_substr)
    if not tok:
        raise RuntimeError("Could not derive machine number token for EGM Search.")

    login_url = f"{base}/login?redirect=%2Fegm%2FegmStatusList"
    egm_url = f"{base}/egm/egmStatusList"
    from playwright.sync_api import sync_playwright

    if headed is True:
        headless = False
    elif headed is False:
        headless = True
    else:
        headless = _np_backend_playwright_headless()

    out_fd, out_path = tempfile.mkstemp(suffix=".png", prefix="np_egm_cctv_")
    os.close(out_fd)
    cctv_re = re.compile(r"CCTV", re.I)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            context = browser.new_context(
                viewport={"width": 1600, "height": 900},
                ignore_https_errors=True,
                device_scale_factor=2,
            )
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            page.goto(login_url, wait_until="domcontentloaded")
            page.wait_for_timeout(900)

            pwd_box = page.locator('input[type="password"]').first
            pwd_box.wait_for(state="visible", timeout=min(30_000, timeout_ms))
            form = pwd_box.locator("xpath=ancestor::form[1]")
            if form.count():
                tin = form.locator(
                    'input[type="text"], input:not([type]), input[type="tel"], input[type="email"]'
                ).first
                tin.fill(user)
            else:
                page.locator('input[type="text"]').first.fill(user)
            pwd_box.fill(pw)
            lb = page.get_by_role("button", name=re.compile(r"login|sign in|log in", re.I))
            if lb.count():
                lb.first.click()
            else:
                page.locator('button[type="submit"], button.el-button--primary').first.click()

            page.wait_for_timeout(1800)
            if "/egm/egmStatusList" not in (page.url or ""):
                page.goto(egm_url, wait_until="domcontentloaded")

            page.wait_for_selector(".app-container, .filter-container", timeout=timeout_ms)
            search_in = page.locator(".el-form-item").filter(has_text=re.compile(r"Search", re.I)).locator(
                "input.el-input__inner"
            ).first
            if search_in.count() == 0:
                cands = page.locator(".filter-container input.el-input__inner")
                if cands.count() == 0:
                    raise RuntimeError("Could not find EGM Search input.")
                search_in = cands.last
            search_in.click()
            search_in.fill("")
            search_in.fill(tok)
            page.keyboard.press("Enter")
            try:
                view_btn = page.locator(".filter-container button.el-button--small").filter(
                    has_text=re.compile(r"View|查看", re.I)
                ).first
                if view_btn.count():
                    view_btn.click(timeout=min(30_000, timeout_ms))
            except Exception:
                pass
            page.wait_for_timeout(1000)
            try:
                page.wait_for_function(
                    "() => !Array.from(document.querySelectorAll('.el-loading-mask')).some(x => x && x.offsetParent !== null)",
                    timeout=min(timeout_ms, 30_000),
                )
            except Exception:
                pass

            tbody = page.locator(".el-table__body tbody").first
            tbody.wait_for(state="visible", timeout=timeout_ms)
            rows = page.locator(".el-table__body tr.el-table__row")
            n = rows.count()
            if n <= 0:
                raise RuntimeError(f"No EGM rows after Search `{tok}`.")

            target = None
            tok_re = re.compile(re.escape(tok), re.I)
            try:
                cands = rows.filter(has_text=tok_re)
                if cands.count() > 0:
                    target = cands.first
            except Exception:
                target = None
            if target is None:
                tok_cf = tok.casefold()
                for i in range(n):
                    row = rows.nth(i)
                    try:
                        row_txt = (row.inner_text(timeout=min(4_000, timeout_ms)) or "").casefold()
                    except Exception:
                        continue
                    if tok_cf in row_txt:
                        target = row
                        break
            if target is None:
                target = rows.nth(0)

            op_cell = target.locator("td").last
            row_cctv = _egm_first_enabled_button_matching(op_cell, cctv_re)
            if row_cctv is not None:
                row_cctv.click(timeout=min(60_000, timeout_ms))
                page.wait_for_timeout(1200)
            else:
                cog_btn = _pick_enabled_egm_cog_button(op_cell)
                cog_btn.click(timeout=min(60_000, timeout_ms))
                dlg0 = page.locator(".el-dialog.add-floor, div[role='dialog'].add-floor, div.el-dialog").last
                dlg0.wait_for(state="visible", timeout=timeout_ms)
                page.wait_for_timeout(700)
                inner_cctv = _egm_first_enabled_button_matching(dlg0.locator(".el-dialog__body"), cctv_re)
                if inner_cctv is None:
                    inner_cctv = _egm_first_enabled_button_matching(dlg0, cctv_re)
                if inner_cctv is None:
                    raise RuntimeError(
                        "No enabled **CCTV** button on the EGM row or inside the operation dialog. "
                        "Confirm the UI label in browser."
                    )
                inner_cctv.click(timeout=min(60_000, timeout_ms))
                page.wait_for_timeout(1200)

            try:
                page.locator("video").first.wait_for(state="attached", timeout=10_000)
            except Exception:
                pass
            page.wait_for_timeout(int(os.environ.get("CCTV_SCREENSHOT_SETTLE_MS", "2000").strip() or "2000"))

            dlg_cap = page.locator(".el-dialog").last
            dlg_cap.wait_for(state="visible", timeout=min(30_000, timeout_ms))
            dlg_cap.screenshot(path=out_path, animations="disabled", scale="css")
        finally:
            browser.close()
    return out_path


def get_egm_member_user_id(
    *,
    machine_display: str | None,
    machine_substr: str | None = None,
    timeout_ms: int = 120_000,
    headed: bool | None = None,
) -> str | None:
    """
    Open EGM Status small window and read current ``Member`` user id.
    Returns user-id digits (e.g. ``123881283``) or ``None`` if empty/unset.
    """
    md = (machine_display or "").strip()
    if not md:
        raise RuntimeError("EGM member check requires machine_display.")
    base, user, pw = _np_resolve_backend(md)
    tag = _np_log_backend_tag(md)
    if not user or not pw:
        raise RuntimeError(f"Missing credentials for {tag} EGM backend.")
    tok = _np_egm_machine_search_token(md, machine_substr)
    if not tok:
        raise RuntimeError("Could not derive machine number token for EGM Search.")

    login_url = f"{base}/login?redirect=%2Fegm%2FegmStatusList"
    egm_url = f"{base}/egm/egmStatusList"
    from playwright.sync_api import sync_playwright

    if headed is True:
        headless = False
    elif headed is False:
        headless = True
    else:
        headless = _np_backend_playwright_headless()

    def _extract_member_id(text: str) -> str | None:
        s = (text or "").strip()
        if not s:
            return None
        m = re.search(r"Member:\s*.*?\((\d{5,})\)", s, re.I | re.S)
        if m:
            return m.group(1)
        m = re.search(r"Member:\s*(\d{5,})", s, re.I)
        if m:
            return m.group(1)
        nums = re.findall(r"\b(\d{5,})\b", s)
        if nums:
            return nums[0]
        return None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            context = browser.new_context(
                viewport={"width": 1600, "height": 900},
                ignore_https_errors=True,
                device_scale_factor=2,
            )
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            page.goto(login_url, wait_until="domcontentloaded")
            page.wait_for_timeout(900)

            pwd_box = page.locator('input[type="password"]').first
            pwd_box.wait_for(state="visible", timeout=min(30_000, timeout_ms))
            form = pwd_box.locator("xpath=ancestor::form[1]")
            if form.count():
                tin = form.locator(
                    'input[type="text"], input:not([type]), input[type="tel"], input[type="email"]'
                ).first
                tin.fill(user)
            else:
                page.locator('input[type="text"]').first.fill(user)
            pwd_box.fill(pw)
            lb = page.get_by_role("button", name=re.compile(r"login|sign in|log in", re.I))
            if lb.count():
                lb.first.click()
            else:
                page.locator('button[type="submit"], button.el-button--primary').first.click()

            page.wait_for_timeout(1800)
            if "/egm/egmStatusList" not in (page.url or ""):
                page.goto(egm_url, wait_until="domcontentloaded")

            page.wait_for_selector(".app-container, .filter-container", timeout=timeout_ms)
            search_in = page.locator(".el-form-item").filter(has_text=re.compile(r"Search", re.I)).locator(
                "input.el-input__inner"
            ).first
            if search_in.count() == 0:
                cands = page.locator(".filter-container input.el-input__inner")
                if cands.count() == 0:
                    raise RuntimeError("Could not find EGM Search input.")
                search_in = cands.last
            search_in.click()
            search_in.fill("")
            search_in.fill(tok)
            page.keyboard.press("Enter")
            try:
                view_btn = page.locator(".filter-container button.el-button--small").filter(
                    has_text=re.compile(r"View|查看", re.I)
                ).first
                if view_btn.count():
                    view_btn.click(timeout=min(30_000, timeout_ms))
            except Exception:
                pass
            page.wait_for_timeout(1000)
            try:
                page.wait_for_function(
                    "() => !Array.from(document.querySelectorAll('.el-loading-mask')).some(x => x && x.offsetParent !== null)",
                    timeout=min(timeout_ms, 30_000),
                )
            except Exception:
                pass

            tbody = page.locator(".el-table__body tbody").first
            tbody.wait_for(state="visible", timeout=timeout_ms)
            rows = page.locator(".el-table__body tr.el-table__row")
            n = rows.count()
            if n <= 0:
                raise RuntimeError(f"No EGM rows after Search `{tok}`.")

            target = None
            tok_re = re.compile(re.escape(tok), re.I)
            try:
                cands = rows.filter(has_text=tok_re)
                if cands.count() > 0:
                    target = cands.first
            except Exception:
                target = None
            if target is None:
                tok_cf = tok.casefold()
                for i in range(n):
                    row = rows.nth(i)
                    try:
                        row_txt = (row.inner_text(timeout=min(4_000, timeout_ms)) or "").casefold()
                    except Exception:
                        continue
                    if tok_cf in row_txt:
                        target = row
                        break
            if target is None:
                target = rows.nth(0)

            op_cell = target.locator("td").last
            cog_btn = _pick_enabled_egm_cog_button(op_cell)
            cog_btn.click(timeout=min(60_000, timeout_ms))

            dlg = page.locator(".el-dialog.add-floor, div[role='dialog'].add-floor").last
            dlg.wait_for(state="visible", timeout=timeout_ms)
            page.wait_for_timeout(700)

            # Prefer the Member row text; fallback to whole dialog text.
            member_row = dlg.locator(".el-row").filter(
                has=dlg.locator("span.operation-label", has_text=re.compile(r"^Member:\s*$", re.I))
            ).first
            txt = ""
            if member_row.count():
                try:
                    txt = member_row.inner_text(timeout=min(8_000, timeout_ms)) or ""
                except Exception:
                    txt = ""
            if not txt:
                txt = dlg.inner_text(timeout=min(8_000, timeout_ms)) or ""
            return _extract_member_id(txt)
        finally:
            browser.close()


def screenshot_np_recharge_detail(
    player_id: str,
    date_iso: str,
    time_short: str,
    *,
    timeout_ms: int = 120_000,
    machine_substr: str | None = None,
    expected_credit: float | None = None,
    machine_display: str | None = None,
    headed: bool | None = None,
    pause_for_input: bool = False,
) -> str:
    """
    Login to NP backend, open Log Third Http Req, set date range ±NP_BACKEND_WINDOW_MINUTES,
    filter UserId, Search, then scan recharge rows (same-minute rows first, then **all** other recharge
    rows so headless / table-parse quirks do not skip a match).     Accept a Detail when Request JSON has ``machineId`` containing the machine digits; when
    ``expected_credit`` is set and **> 0**, also require ``amount`` > 0 and within ``NP_BACKEND_AMOUNT_EPS``
    of that credit (default 0.05).
    (non-positive ``expected_credit`` is ignored — same as ``None``). **Header Request Time is not
    used to reject** (avoids closing valid dialogs when UI text differs slightly from log seconds).
    ``machine_display``: LogNavigator / OSS folder label (``DHS*`` / ``NCH*`` / ``CP*`` / ``OSM*`` / ``MDR*`` / ``TBP*`` → respective backend;
    creds fall back to ``NP_BACKEND_*`` when ``DHS_BACKEND_*`` / ``NCH_BACKEND_*`` unset).
    ``WF*`` / ``NWR8173`` → Winford + ``WF_BACKEND_*``; else NP + ``NP_BACKEND_*``.

    ``headed``: ``True`` = always show browser; ``False`` = force headless; ``None`` = env / platform default.

    Returns path to a temporary PNG (caller should delete).
    """
    base, user, pw = _np_resolve_backend(machine_display)
    _log_http_backend_tag = _np_log_backend_tag(machine_display)
    if not user or not pw:
        if _log_http_backend_tag == "DHS":
            raise RuntimeError(
                "Missing credentials for DHS Log Third Http: set DHS_BACKEND_USER / DHS_BACKEND_PASSWORD "
                "or NP_BACKEND_USER / NP_BACKEND_PASSWORD in `.env` (loaded from the Chatbox folder), then restart Duty Bot."
            )
        if _log_http_backend_tag == "NCH":
            raise RuntimeError(
                "Missing credentials for NCH Log Third Http: set NCH_BACKEND_USER / NCH_BACKEND_PASSWORD "
                "or NP_BACKEND_USER / NP_BACKEND_PASSWORD in `.env` (loaded from the Chatbox folder), then restart Duty Bot."
            )
        if _log_http_backend_tag == "TBP":
            raise RuntimeError(
                "Missing credentials for TBP Log Third Http: set TBP_BACKEND_USER / TBP_BACKEND_PASSWORD "
                "in `.env` (loaded from the Chatbox folder), then restart Duty Bot."
            )
        if _log_http_backend_tag in ("CP", "OSM"):
            raise RuntimeError(
                "Missing credentials for CP/OSM Log Third Http: set CP_BACKEND_USER / CP_BACKEND_PASSWORD "
                "in `.env` (loaded from the Chatbox folder), then restart Duty Bot."
            )
        raise RuntimeError(
            "Set NP_BACKEND_USER and NP_BACKEND_PASSWORD in the environment "
            "(not required for Winford (WF* / NWR8173 alias) — defaults omduty1 unless WF_BACKEND_* is set)."
        )

    start_s, end_s = _np_window_strings(date_iso, time_short)
    if _np_use_backend_osmplay_com(machine_display):
        login_url = f"{base}/login?redirect=%2Fegm%2FegmStatusList"
    else:
        login_url = f"{base}/login?redirect=%2Flog%2FlogThirdHttpReq"
    log_url = f"{base}/log/logThirdHttpReq"

    from playwright.sync_api import sync_playwright

    if headed is True:
        headless = False
    elif headed is False:
        headless = True
    else:
        headless = _np_backend_playwright_headless()
    post_search_ms = int(os.environ.get("NP_BACKEND_POST_SEARCH_MS", "2500").strip() or "2500")
    if headless:
        post_search_ms = max(
            post_search_ms,
            int(os.environ.get("NP_BACKEND_HEADLESS_POST_SEARCH_MS", "5500").strip() or "5500"),
        )
    dialog_settle_ms = int(os.environ.get("NP_BACKEND_DIALOG_SETTLE_MS", "0").strip() or "0")
    if dialog_settle_ms <= 0:
        dialog_settle_ms = 1500 if headless else 900
    out_fd, out_path = tempfile.mkstemp(suffix=".png", prefix="np_third_http_")
    os.close(out_fd)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            context = browser.new_context(
                viewport={"width": 1600, "height": 900},
                ignore_https_errors=True,
            )
            page = context.new_page()
            page.set_default_timeout(timeout_ms)

            page.goto(login_url, wait_until="domcontentloaded")
            page.wait_for_timeout(800)

            pwd_box = page.locator('input[type="password"]').first
            pwd_box.wait_for(state="visible", timeout=min(30_000, timeout_ms))
            form = pwd_box.locator("xpath=ancestor::form[1]")
            if form.count():
                tin = form.locator(
                    'input[type="text"], input:not([type]), input[type="tel"], input[type="email"]'
                ).first
                tin.fill(user)
            else:
                page.locator('input[type="text"]').first.fill(user)
            pwd_box.fill(pw)

            login_btn = page.get_by_role("button", name=re.compile(r"login|sign in|log in", re.I))
            if login_btn.count():
                login_btn.first.click()
            else:
                page.locator('button[type="submit"], button.el-button--primary').first.click()

            page.wait_for_timeout(2000)
            if "/log/logThirdHttpReq" not in (page.url or ""):
                page.goto(log_url, wait_until="domcontentloaded")

            page.wait_for_selector(".filter-container", timeout=timeout_ms)

            dinputs = page.locator(".filter-container .data-content .ssdate input.el-input__inner")
            if dinputs.count() >= 2:
                dinputs.nth(0).click()
                dinputs.nth(0).fill("")
                dinputs.nth(0).fill(start_s)
                page.keyboard.press("Enter")
                dinputs.nth(1).click()
                dinputs.nth(1).fill("")
                dinputs.nth(1).fill(end_s)
                page.keyboard.press("Enter")
            else:
                raise RuntimeError("Could not find date range inputs on Log Third Http Req page.")

            uid_in = page.locator(".filter-container .el-form-item").filter(has_text="UserId").locator(
                "input.el-input__inner"
            ).first
            uid_in.fill("")
            uid_in.fill(str(player_id).strip())
            page.wait_for_timeout(400)

            # Search label varies (EN / CN / Element Plus inner span); strict "^Search$" often fails.
            def _click_np_search() -> None:
                attempts = [
                    page.locator(".filter-container button, .filter-container .el-button").filter(
                        has_text=re.compile(r"Search|查询", re.I)
                    ),
                    page.get_by_role("button", name=re.compile(r"search|查询", re.I)),
                    page.locator('button.el-button--primary').filter(
                        has_text=re.compile(r"Search|查询|search", re.I)
                    ),
                    page.locator("button").filter(has_text=re.compile(r"^Search$")),
                ]
                last_err: Exception | None = None
                for loc in attempts:
                    try:
                        if loc.count() == 0:
                            continue
                        btn = loc.first
                        btn.wait_for(state="visible", timeout=min(30_000, timeout_ms))
                        btn.click(timeout=min(60_000, timeout_ms))
                        return
                    except Exception as e:
                        last_err = e
                        continue
                raise RuntimeError(
                    "Could not find or click the Search button on Log Third Http Req "
                    f"(UI may have changed). Last error: {last_err!r}"
                )

            _click_np_search()
            page.wait_for_timeout(post_search_ms)

            ms = (machine_substr or "").strip()
            exp_match = _np_expected_credit_for_match(expected_credit)
            need_detail_match = bool(ms) or exp_match is not None
            amt_scale = _np_tbp_amount_scale() if _np_use_tbp_log_backend(machine_display) else 1.0

            if need_detail_match:
                def _scan_detail_pages(exp_try: float | None) -> bool:
                    _np_pagination_go_first_page(page, timeout_ms=timeout_ms)
                    for _pi in range(NP_BACKEND_MAX_PAGES):
                        page.locator(".el-table__body tbody").wait_for(state="visible", timeout=timeout_ms)
                        rows = page.locator(".el-table__body tr.el-table__row")
                        n = rows.count()
                        ordered = _np_list_recharge_indices_time_ordered(page, rows, n, date_iso, time_short)
                        if ordered:
                            same_min_rows = _np_indices_table_same_minute(
                                page, rows, ordered, date_iso, time_short
                            )
                            to_scan = _np_merge_same_minute_then_rest(same_min_rows, ordered)
                            ok = _np_try_screenshot_matching_detail(
                                page,
                                rows,
                                to_scan,
                                machine_substr=machine_substr,
                                expected_credit=exp_try,
                                out_path=out_path,
                                timeout_ms=timeout_ms,
                                dialog_settle_ms=dialog_settle_ms,
                                amount_scale=amt_scale,
                            )
                            if ok:
                                return True
                        if not _np_pagination_can_go_next(page):
                            break
                        _np_click_pagination_next(page, timeout_ms=timeout_ms)
                    return False

                matched = _scan_detail_pages(exp_match)
                ran_tbp_machine_only = False
                if (
                    not matched
                    and _log_http_backend_tag == "TBP"
                    and ms
                    and exp_match is not None
                    and not _np_truthy_env("TBP_THIRD_HTTP_NO_MACHINE_ONLY_FALLBACK")
                ):
                    ran_tbp_machine_only = True
                    matched = _scan_detail_pages(None)

                if not matched:
                    bits: list[str] = []
                    if ms:
                        bits.append(f"`machineId` containing `{ms}`")
                    if exp_match is not None:
                        bits.append(
                            f"`amount` within {_np_amount_match_eps()} of `{exp_match}`"
                            + (f" (÷ `{amt_scale}` scale)" if amt_scale != 1.0 else "")
                        )
                    elif ms:
                        bits.append(
                            "latest credit was 0 or unset — only `machineId` is matched, not `amount`"
                        )
                    if ran_tbp_machine_only:
                        bits.append("TBP machine-only fallback (no log amount match) also found nothing")
                    crit = "; ".join(bits) if bits else "expected filters"
                    raise RuntimeError(
                        f"No {_log_http_backend_tag} Detail on pages 1–{NP_BACKEND_MAX_PAGES} with {crit}. "
                        "Increase NP_BACKEND_MAX_PAGES or NP_BACKEND_WINDOW_MINUTES."
                    )
            else:
                page.locator(".el-table__body tbody").wait_for(state="visible", timeout=timeout_ms)
                rows = page.locator(".el-table__body tr.el-table__row")
                n = rows.count()
                ordered = _np_list_recharge_indices_time_ordered(page, rows, n, date_iso, time_short)
                if not ordered:
                    raise RuntimeError(
                        'No table row with Event Type "recharge" for this UserId/time window.'
                    )

                pick_i = ordered[0]
                row = rows.nth(pick_i)
                _np_click_row_show_details_link(row, timeout_ms=timeout_ms)

                dlg = page.locator(
                    ".el-dialog.details-dialog, div[role='dialog'].details-dialog"
                ).last
                dlg.wait_for(state="visible", timeout=timeout_ms)
                _np_capture_detail_dialog_screenshot(
                    page,
                    dlg,
                    out_path,
                    timeout_ms=timeout_ms,
                    settle_ms=600,
                )
        finally:
            if pause_for_input:
                if sys.stdin.isatty():
                    print(
                        f"[checkcredit] {_log_http_backend_tag} Chromium is still open — inspect the window.\n"
                        "    → Press Enter **in this terminal** (not in the browser) to close Chromium.",
                        flush=True,
                    )
                    try:
                        input()
                    except EOFError:
                        pass
                else:
                    try:
                        sec = max(
                            1,
                            int(
                                (os.environ.get("NP_CHECKUSER_PAUSE_OPEN_SEC") or "90").strip() or "90"
                            ),
                        )
                    except ValueError:
                        sec = 90
                    print(
                        f"[checkcredit] {_log_http_backend_tag} stdin is not a TTY — "
                        f"keeping Chromium open **{sec}s** (set NP_CHECKUSER_PAUSE_OPEN_SEC). "
                        "Use a real terminal with --pause for Enter-to-close.",
                        flush=True,
                    )
                    try:
                        time.sleep(sec)
                    except KeyboardInterrupt:
                        pass
            browser.close()

    return out_path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="checkcredit — LogNavigator helpers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 checkcredit.py --finderror 2074 --date 2026-04-27\n"
            "  python3 checkcredit.py --finderror CP0231 --date 2026-02-05 --oss\n"
            "  python3 checkcredit.py 185 --debug\n"
            "    LogNavigator tail → report → pick user → headed backend Log Third Http screenshot\n"
            "\n"
            "NP --checkuser (visible Chromium, headed — watch detection):\n"
            "  python3 checkcredit.py --checkuser --player-id 132594948 --date 2026-04-27 \\\n"
            "    --time 23:55:12.092 --machine-substr 2074 --credit 1352 --pause\n"
        ),
    )
    ap.add_argument("--timeout-ms", type=int, default=90_000)
    ap.add_argument("--finderror", metavar="NUMBER", help="Digits to match machine in LogNavigator Select2")
    ap.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Log file date (default: today)",
    )
    ap.add_argument("--base-url", default=DEFAULT_BASE, help="LogNavigator base URL")
    ap.add_argument(
        "--oss",
        action="store_true",
        help="Fetch log via OSS HTTP (OSM_LOG_OSS_TEMPLATE) instead of LogNavigator browser",
    )
    ap.add_argument(
        "--debug",
        action="store_true",
        help=(
            "With --finderror / MACHINE: visible LogNavigator when using browser mode; after the report, "
            "prompt for a user then open the routed backend (Log Third Http → Detail PNG). "
            "Uses stdin — non-TTY skips the picker."
        ),
    )
    ap.add_argument(
        "--checkuser",
        action="store_true",
        help=(
            "NP Log Third Http Req: headed Chromium — same as Duty Bot /npthirdhttp. "
            "Needs --date, --time; User ID can be first positional after --checkuser "
            "(same as --player-id)."
        ),
    )
    ap.add_argument("--player-id", metavar="USER_ID", help="With --checkuser")
    ap.add_argument(
        "--time",
        metavar="HH:MM:SS[.mmm]",
        dest="credit_time",
        help="Log credit time (with --checkuser)",
    )
    ap.add_argument(
        "--machine-substr",
        default="",
        metavar="SUBSTR",
        help=(
            "Substring for machineId in Request JSON (e.g. 2074, or 3189). "
            "Cabinet+number e.g. dhs3189 matches machineId …-3189 (studioId is separate). "
            "If --machine-display is omitted, DHS3178 / dhs3189 also selects the backend."
        ),
    )
    ap.add_argument(
        "--credit",
        type=float,
        default=None,
        metavar="AMOUNT",
        help="Latest credit amount to match NP Request JSON amount (with --checkuser)",
    )
    ap.add_argument(
        "--pause",
        action="store_true",
        help=(
            "With --checkuser: keep Chromium open until Enter **in this terminal** (then closes browser). "
            "Non-TTY: waits NP_CHECKUSER_PAUSE_OPEN_SEC (default 90) instead of input()."
        ),
    )
    ap.add_argument(
        "--machine-display",
        default="",
        metavar="NAME",
        help=(
            "With --checkuser: machine label (e.g. WF8123 / DHS3178 / NCH1171) to pick backend "
            "(overrides routing from --machine-substr). Same as Duty Bot LogNavigator folder name."
        ),
    )
    ap.add_argument(
        "machine_positional",
        nargs="?",
        default=None,
        metavar="MACHINE_OR_USER_ID",
        help=(
            "With --finderror: shorthand machine query (e.g. 185). "
            "With --checkuser and no --player-id: shorthand user id (e.g. 130000720)."
        ),
    )
    args = ap.parse_args(argv)

    fe_opt = (args.finderror or "").strip() if getattr(args, "finderror", None) else ""
    fe_pos = (args.machine_positional or "").strip() if getattr(args, "machine_positional", None) else ""

    # --checkuser 130000720 → positional is User ID, not --finderror MACHINE
    if getattr(args, "checkuser", False) and fe_pos and not fe_opt:
        if not (args.player_id or "").strip():
            args.player_id = fe_pos
            fe_pos = ""

    if fe_opt and fe_pos and fe_opt != fe_pos:
        print(
            "❌ --finderror and positional MACHINE disagree — use one or the same value.",
            file=sys.stderr,
        )
        return 2
    finderror_resolved = fe_opt or fe_pos or None

    if getattr(args, "checkuser", False):
        if fe_pos:
            print(
                "❌ Extra positional argument with --checkuser — put User ID right after --checkuser "
                "(or use --player-id), not a second positional.",
                file=sys.stderr,
            )
            return 2
        if finderror_resolved:
            print("❌ Use either --finderror / MACHINE or --checkuser, not both.", file=sys.stderr)
            return 2
        pid = (args.player_id or "").strip()
        ds = (args.date or "").strip()
        ts = (getattr(args, "credit_time", None) or "").strip()
        missing = [x for x, ok in (("--player-id / User ID", pid), ("--date YYYY-MM-DD", ds), ("--time HH:MM:SS[.mmm]", ts)) if not ok]
        if missing:
            print(
                "❌ --checkuser still needs: " + ", ".join(missing),
                file=sys.stderr,
            )
            print(
                "   Example:\n"
                "     python3 checkcredit.py --checkuser 130000720 --date 2026-04-29 --time 14:30:05.123 \\\n"
                "       --machine-substr 2074 --credit 1352",
                file=sys.stderr,
            )
            print(
                "   Use --machine-display DHS3178 (or put the same in --machine-substr) to use DHS backend; "
                "digits-only --machine-substr keeps NP.",
                file=sys.stderr,
            )
            return 2
        try:
            datetime.strptime(args.date.strip(), "%Y-%m-%d")
        except ValueError:
            print("❌ --date must be YYYY-MM-DD", file=sys.stderr)
            return 2
        ms = (args.machine_substr or "").strip() or None
        md = (getattr(args, "machine_display", None) or "").strip() or None
        # Duty Bot passes folder label as machine_display; CLI users often only set --machine-substr.
        # Use substr for routing too when it looks like DHS3178 / dhs3189 / CP0231 (digits-only → still NP).
        if not md and ms:
            md = ms
        try:
            _b_base, _, _ = _np_resolve_backend(md)
            _tag = _np_log_backend_tag(md)
            print(f"→ Backend ({_tag}): {_b_base}", flush=True)
            out_png = screenshot_np_recharge_detail(
                str(args.player_id).strip(),
                args.date.strip(),
                str(args.credit_time).strip(),
                timeout_ms=max(60_000, args.timeout_ms),
                machine_substr=ms,
                expected_credit=args.credit,
                machine_display=md,
                headed=True,
                pause_for_input=bool(args.pause),
            )
            print(out_png)
            return 0
        except Exception as e:
            print(f"❌ {e}", file=sys.stderr)
            return 1

    if finderror_resolved:
        td = None
        if args.date:
            try:
                td = datetime.strptime(args.date.strip(), "%Y-%m-%d").date()
            except ValueError:
                print("❌ --date must be YYYY-MM-DD", file=sys.stderr)
                return 2
        use_oss = bool(args.oss) or _env_truthy("CHECKCREDIT_USE_OSS")
        want_debug = bool(getattr(args, "debug", False))
        try:
            out = run_finderror(
                str(finderror_resolved).strip(),
                target_date=td,
                timeout_ms=max(15_000, args.timeout_ms),
                base=args.base_url.rstrip("/"),
                user=DEFAULT_USER,
                pw=DEFAULT_PASS,
                source="oss" if use_oss else "navigator",
                debug_headed=want_debug and not use_oss,
            )
        except Exception as e:
            print(f"❌ {e}", file=sys.stderr)
            return 1
        print(out["text"])
        if want_debug and sys.stdin.isatty():
            pick = _interactive_debug_pick_player(
                out.get("merged_players") or [],
                machine_display=str(out.get("machine_display_resolved") or ""),
            )
            if pick:
                md = (out.get("machine_display_resolved") or "").strip() or None
                np_fu = out.get("np_followup") or {}
                td_iso = np_fu.get("target_date")
                if td_iso is None and out.get("target_date") is not None:
                    td = out["target_date"]
                    td_iso = td.isoformat() if isinstance(td, date) else str(td)
                if not td_iso:
                    print("❌ missing target date for backend step", file=sys.stderr)
                    return 1
                ms = (np_fu.get("machine_match_substr") or "").strip()
                if not ms:
                    ms = machine_match_substr_from_display(md or "")
                ms = ms.strip() or None
                try:
                    b_base, _, _ = _np_resolve_backend(md)
                    tag = _np_log_backend_tag(md)
                    print(f"\n→ Backend ({tag}): {b_base} — Log Third Http / Detail…", flush=True)
                    out_png = screenshot_np_recharge_detail(
                        pick["user_id"],
                        str(td_iso).strip(),
                        pick["time_short"],
                        timeout_ms=max(60_000, args.timeout_ms),
                        machine_substr=ms,
                        expected_credit=pick.get("credit_value"),
                        machine_display=md,
                        headed=True,
                        pause_for_input=True,
                    )
                    print(f"\n→ Detail screenshot: {out_png}", flush=True)
                except Exception as e:
                    print(f"❌ backend screenshot: {e}", file=sys.stderr)
                    return 1
        elif want_debug and not sys.stdin.isatty():
            print(
                "[checkcredit --debug] stdin is not a TTY — skipping user picker / backend step.",
                file=sys.stderr,
            )
        return 0

    ap.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
