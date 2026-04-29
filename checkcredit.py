#!/usr/bin/env python3
"""
Find user log errors (--finderror).

  LogNavigator (browser, headed): default — drives UI like manual flow.
  OSS (HTTP): faster, no browser — GET plain log file from object storage.

  python3 checkcredit.py --finderror 2074 --date 2026-04-27
  python3 checkcredit.py --finderror CP0231 --date 2026-02-05 --oss

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

  On Linux, if $DISPLAY is unset, LogNavigator mode uses headless automatically.

  NP backend (screenshot_np_recharge_detail — Duty Bot /npthirdhttp):
  NP_BACKEND_BASE (default https://backend-np.osmplay.com), NP_BACKEND_USER, NP_BACKEND_PASSWORD
  NP_BACKEND_WINDOW_MINUTES (default 10), NP_BACKEND_MAX_PAGES (default 20, table pagination)
  NP_BACKEND_HEADLESS / NP_BACKEND_HEADED (or **WF_THIRD_HTTP_HEADED** / **THIRD_HTTP_PLAYWRIGHT_HEADED**
  — same effect: visible Chromium for Log Third Http screenshots on Linux servers that default to headless).
  If **Machine** looks Winford (folder / label **starts with ``WF``** e.g. ``WF8123``, ``WF8173``;
  ``winford`` in the text; or ``NWR8173`` from digits-only OSS ``NWR{n}`` for that cabinet), Log Third
  Http uses Winford instead of NP:
  ``https://backend-winford.osmplay.com`` with user/password ``omduty1`` (override via
  ``WF_BACKEND_USER`` / ``WF_BACKEND_PASSWORD``).

  NP debug — **visible Chromium** (not headless), same logic as Duty Bot ``/npthirdhttp``::
    python3 checkcredit.py --checkuser --player-id 132594948 --date 2026-04-27 \\
      --time 23:55:12.092 --machine-substr 2074 --credit 1352 --pause
    Add ``--machine-display WF8173`` when testing Winford NP from CLI (after ``/checkcreditdate`` the
    bot passes machine from context automatically).
  Use ``--pause`` to leave the window open until you press Enter in the terminal.
  Do **not** set ``NP_BACKEND_HEADLESS=1`` when you want to watch the browser.

Requires: playwright (+ chromium) for LogNavigator; requests for OSS.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from datetime import date, datetime, timedelta
from typing import Any

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
    etc., ``latest_credit`` is taken from the last ``httpaft:enter_game`` line that includes
    ``add_num`` / ``target`` (prefer ``target``), e.g. ``target:20265.0``, instead of leaving n/a when
    there is no ``successJson`` ``cur_coin``.
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
            findings.append(
                {
                    "time": time_part,
                    "error_count": ec,
                    "snippet": json_like[:800],
                    "full_line": line.rstrip(),
                    "line_idx": line_idx,
                }
            )
        row: dict[str, Any] = {
            "user_id": uid,
            "errors": findings,
            "block_max_line": block_max_line,
        }
        has_enter_timeout = _block_has_enter_game_timeout(blines)
        if has_enter_timeout and best_enter is not None:
            latest_credit: dict[str, Any] | None = best_enter
        elif best_coin is not None:
            latest_credit = best_coin
        elif best_reduce is not None:
            latest_credit = best_reduce
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
                "_lc_coin": None,
                "_lc_reduce": None,
                "_lc_enter": None,
                "max_line_idx": -1,
            }
        by_uid[uid]["errors"].extend(blk.get("errors") or [])
        lc = blk.get("latest_credit")
        if lc:
            src = (lc.get("source") or "").strip()
            if src == "reduce_num":
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
        lc_cands = [d["_lc_coin"], d["_lc_reduce"], d["_lc_enter"]]
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


def machine_match_substr_from_display(machine_display: str) -> str:
    """Digits substring to match NP `machineId` (e.g. NWR2074 → `2074`)."""
    nums = re.findall(r"\d+", machine_display or "")
    return max(nums, key=len) if nums else ""


def build_np_choice_lark_card(
    np_choices: list[dict[str, Any]],
    *,
    target_date_iso: str = "",
    machine_display: str = "",
) -> dict[str, Any]:
    """Lark card: context line (log date + machine for NP/WF window) + four numbered lines (reply 1–4)."""
    lines: list[str] = []
    td = (target_date_iso or "").strip()
    md = (machine_display or "").strip()
    if td or md:
        bits: list[str] = []
        if td:
            bits.append(f"**Log date (NP/WF window):** `{td}`")
        if md:
            bits.append(f"**Machine:** `{md}`")
        lines.append(" · ".join(bits))
        lines.append("_Screenshot uses the log date above + each line’s credit time._")
        lines.append("")
    for i, ch in enumerate(np_choices):
        uid = ch.get("user_id", "")
        cr = ch.get("credit", "n/a")
        ts = ch.get("time_short") or "n/a"
        lines.append(f"{i + 1}) User ID `{uid}` — last credit `{cr}` @ `{ts}`")
    content = "\n".join(lines) if lines else "_No players._"
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "\u200b"},
        },
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": content},
            },
        ],
    }


def build_np_followup_payload(
    top2_any: list[dict[str, Any]],
    top2_err: list[dict[str, Any]],
    machine_display: str,
    td: date,
) -> dict[str, Any]:
    """
    Cards 1–2: latest 2 in log; cards 3–4: latest 2 with error.
    np_choices index 0..3 → user picks 1–4 in chat.
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

    np_choices: list[dict[str, Any]] = []
    for r in top2_any[:2]:
        np_choices.append(_row_choice(r, "latest_in_log"))
    for r in top2_err[:2]:
        np_choices.append(_row_choice(r, "with_error"))
    np_choices = np_choices[:4]

    latest_two_players: list[dict[str, str]] = []
    for r in top2_any[:2]:
        lc = r.get("latest_credit") or {}
        latest_two_players.append(
            {
                "user_id": str(r["user_id"]),
                "time_short": (lc.get("time_short") or "").strip(),
            }
        )

    return {
        "machine_display": machine_display,
        "machine_match_substr": machine_match_substr_from_display(machine_display),
        "target_date": td.isoformat(),
        "latest_two_players": latest_two_players,
        "np_choices": np_choices,
    }


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
) -> dict[str, Any]:
    """Whether the last player in the log and the last player with an error are the same uid."""
    dstr = target_date.isoformat()
    le = latest_err_uid or "*(none)*"
    la = latest_any_uid or "*(none)*"
    if not latest_err_uid:
        same_line = 'No error > 0 in log — cannot compare "last with error" to "last in log".'
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


def run_finderror(
    machine_query: str,
    *,
    target_date: date | None,
    timeout_ms: int,
    base: str,
    user: str,
    pw: str,
    source: str = "navigator",
) -> dict[str, Any]:
    """
    source:
      - \"oss\" — GET log from OSM_LOG_OSS_TEMPLATE (no browser).
      - \"navigator\" — LogNavigator UI + tail (Chromium; headless on Linux without DISPLAY).
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
        from playwright.sync_api import sync_playwright

        headless = _playwright_headless()
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

                chosen = select_machine_by_number(page, machine_query, timeout_ms=timeout_ms)
                machine_display = (chosen or "").strip() or machine_display
                text_parts.append(f"→ Machine selected: {chosen}")
                wait_for_logs_file_browser(page, timeout_ms=timeout_ms)

                navigate_to_logic_folder(page, timeout_ms=timeout_ms)
                click_log_file_for_date(page, td, timeout_ms=timeout_ms)
                bump_tail_and_execute(page, timeout_ms=timeout_ms)

                pre = page.locator("section[role='results'] pre, pre.nofloat").first
                pre.wait_for(state="attached", timeout=timeout_ms)
                log_body = pre.inner_text() or ""

                parsed = parse_user_blocks_full(log_body)

                text_parts.append(f"→ Date file: {td.isoformat()}")
            finally:
                browser.close()

    header = "\n".join(text_parts)
    merged = merge_players_full(parsed)
    top2_err = select_top2_error_players(merged)
    top2_any = select_top2_overall(merged)
    le_uid, _le_line = pick_latest_error_uid(merged)
    la_uid, _la_line = pick_latest_any_uid(merged)
    same_uid = bool(le_uid and la_uid and le_uid == la_uid)

    report = format_dual_terminal_report(top2_any, top2_err, machine_display, td)
    plain = f"{header}\n\n{report}" if header else report
    return {
        "text": plain,
        "np_followup": build_np_followup_payload(top2_any, top2_err, machine_display, td),
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
        "lark_card_candidates": build_same_latest_players_card(
            machine_display=machine_display,
            target_date=td,
            latest_err_uid=le_uid,
            latest_any_uid=la_uid,
            same_uid=same_uid,
        ),
    }


# ----- NP backend — Log Third Http Req (Duty Bot /npthirdhttp) -----
NP_BACKEND_DEFAULT_BASE = os.environ.get("NP_BACKEND_BASE", "https://backend-np.osmplay.com").rstrip("/")
NP_BACKEND_WINDOW_MINUTES = int(os.environ.get("NP_BACKEND_WINDOW_MINUTES", "10"))
try:
    NP_BACKEND_MAX_PAGES = max(1, int(os.environ.get("NP_BACKEND_MAX_PAGES", "20").strip() or "20"))
except ValueError:
    NP_BACKEND_MAX_PAGES = 20


def _np_backend_env_cred() -> tuple[str, str]:
    u = (os.environ.get("NP_BACKEND_USER") or "").strip()
    p = (os.environ.get("NP_BACKEND_PASSWORD") or "").strip()
    return u, p


_WINFORD_NP_BASE = "https://backend-winford.osmplay.com".rstrip("/")


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


def _np_resolve_backend(machine_display: str | None) -> tuple[str, str, str]:
    """
    (base_url, username, password) for Log Third Http Req.

    Winford cabinet (machine label / path segment starting with **WF**, ``winford`` in name, or ``NWR8173`` OSS alias)
    → ``backend-winford.osmplay.com`` + ``WF_BACKEND_USER`` / ``WF_BACKEND_PASSWORD`` (default ``omduty1``).
    Otherwise → ``NP_BACKEND_BASE`` / env NP credentials.
    """
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


def _np_row_is_recharge(rows, ev_i: int, i: int) -> bool:
    row = rows.nth(i)
    tds = row.locator("td")
    tc = tds.count()
    if tc == 0:
        return False
    if tc > ev_i:
        et_txt = (tds.nth(ev_i).inner_text() or "").strip().lower()
        if et_txt == "recharge":
            return True
    for j in range(tc):
        if (tds.nth(j).inner_text() or "").strip().lower() == "recharge":
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
    if not blob:
        return None, None
    s = _np_normalize_jsonish_quotes(blob)
    m_mid = re.search(r'"machineId"\s*:\s*"((?:\\.|[^"\\])*)"', s)
    if not m_mid:
        m_mid = re.search(r"'machineId'\s*:\s*'((?:\\.|[^'\\])*)'", s)
    if not m_mid:
        m_mid = re.search(r"machineId\s*:\s*\"([^\"]*)\"", s, re.I)
    m_amt = re.search(r'"amount"\s*:\s*(-?\d+(?:\.\d+)?)', s)
    if not m_amt:
        m_amt = re.search(r"'amount'\s*:\s*(-?\d+(?:\.\d+)?)", s)
    if not m_amt:
        m_amt = re.search(r"(?<![\w])amount\s*:\s*(-?\d+(?:\.\d+)?)", s, re.I)
    mid = m_mid.group(1) if m_mid else None
    if mid is not None:
        mid = mid.replace("\\/", "/")
    amt: float | None = None
    if m_amt:
        try:
            amt = float(m_amt.group(1))
        except ValueError:
            amt = None
    return mid, amt


def _np_machine_id_contains_substr(machine_substr: str | None, machine_id_value: str | None) -> bool:
    """``machineId`` JSON value must contain the log machine digits (e.g. ``2074`` in ``4182-BIGFULINK-2074``)."""
    ms = (machine_substr or "").strip()
    if not ms:
        return True
    mid = (machine_id_value or "").strip()
    return ms in mid


def _np_detail_matches_credit_and_machine_id(
    req_blob: str,
    machine_substr: str | None,
    expected_credit: float | None,
) -> bool:
    """
    Request JSON: ``machineId`` contains machine digits; ``amount`` > 0 and matches latest credit when set.
    """
    mid, amt = _np_parse_machine_amount_from_request_blob(req_blob)
    if mid is None or amt is None:
        return False
    if not _np_machine_id_contains_substr(machine_substr, mid):
        return False
    if expected_credit is not None:
        if amt <= 0:
            return False
        if abs(amt - float(expected_credit)) > 0.05:
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


def _np_close_np_detail_dialog(page) -> None:
    hdr = page.locator(
        ".el-dialog.details-dialog .el-dialog__headerbtn, "
        ".el-dialog.details-dialog .el-dialog__close, "
        "div[role='dialog'].details-dialog .el-dialog__headerbtn"
    ).first
    try:
        if hdr.count():
            hdr.click(timeout=5_000)
        else:
            page.keyboard.press("Escape")
    except Exception:
        page.keyboard.press("Escape")
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
) -> bool:
    """
    For each candidate row: accept when Request JSON ``machineId`` contains the machine digits and
    ``amount`` matches latest credit. Uses multiple text layers — the correct row often opens first
    but ``inner_text()`` alone may omit JSON bits; that looked like a wrong row and closed the dialog.
    """
    ms = (machine_substr or "").strip()
    need = bool(ms) or expected_credit is not None
    if not need:
        return False

    dlg_sel = page.locator(".el-dialog.details-dialog, div[role='dialog'].details-dialog").first

    for pick_i in ordered_indices:
        row = rows.nth(pick_i)
        link = row.get_by_text("Show Details", exact=False)
        link.scroll_into_view_if_needed()
        link.click()
        dlg_sel.wait_for(state="visible", timeout=timeout_ms)
        page.wait_for_timeout(900)
        try:
            dlg_sel.get_by_text(re.compile(r"machineId|machine\s*id|amount", re.I)).first.wait_for(
                state="visible", timeout=min(8_000, timeout_ms)
            )
        except Exception:
            pass
        full_txt = dlg_sel.inner_text() or ""
        layers = _np_dialog_text_layers_for_match(dlg_sel)
        blob = _np_detail_request_section(full_txt)
        ok = _np_detail_matches_credit_and_machine_id(blob, machine_substr, expected_credit)
        if not ok:
            ok = _np_detail_matches_credit_and_machine_id(layers, machine_substr, expected_credit)
        if not ok:
            ok = _np_detail_matches_credit_and_machine_id(full_txt, machine_substr, expected_credit)
        if not ok:
            blob2 = _np_detail_request_section(layers)
            ok = _np_detail_matches_credit_and_machine_id(blob2, machine_substr, expected_credit)
        if not ok:
            ok = _np_detail_matches_credit_and_machine_id(
                "\n".join((blob, layers, full_txt)), machine_substr, expected_credit
            )
        if ok:
            dlg_sel.screenshot(path=out_path, animations="disabled")
            return True
        _np_close_np_detail_dialog(page)

    return False


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
    filter UserId, Search, then scan recharge rows (prefer same calendar minute as log credit when
    table times parse). Accept a Detail when Request JSON has ``machineId`` containing the machine
    digits,
    ``amount`` > 0, and ``amount`` matches latest credit — **header Request Time is not used to
    reject** (avoids closing valid dialogs when UI text differs slightly from log seconds).
    ``machine_display``: LogNavigator / OSS folder label (e.g. ``WF8123``, ``NWR8173`` from digits-only
    ``NWR{n}``) — Winford routing uses ``_np_use_winford_log_backend``; login from ``WF_BACKEND_*``.

    ``headed``: ``True`` = always show browser; ``False`` = force headless; ``None`` = env / platform default.

    Returns path to a temporary PNG (caller should delete).
    """
    base, user, pw = _np_resolve_backend(machine_display)
    _log_http_backend_tag = "WF" if _np_use_winford_log_backend(machine_display) else "NP"
    if not user or not pw:
        raise RuntimeError(
            "Set NP_BACKEND_USER and NP_BACKEND_PASSWORD in the environment "
            "(not required for Winford (WF* / NWR8173 alias) — defaults omduty1 unless WF_BACKEND_* is set)."
        )

    start_s, end_s = _np_window_strings(date_iso, time_short)
    login_url = f"{base}/login?redirect=%2Flog%2FlogThirdHttpReq"
    log_url = f"{base}/log/logThirdHttpReq"

    from playwright.sync_api import sync_playwright

    if headed is True:
        headless = False
    elif headed is False:
        headless = True
    else:
        headless = _np_backend_playwright_headless()
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
            page.wait_for_timeout(2500)

            ms = (machine_substr or "").strip()
            need_detail_match = bool(ms) or expected_credit is not None

            if need_detail_match:
                matched = False
                for pi in range(NP_BACKEND_MAX_PAGES):
                    page.locator(".el-table__body tbody").wait_for(state="visible", timeout=timeout_ms)
                    rows = page.locator(".el-table__body tr.el-table__row")
                    n = rows.count()
                    ordered = _np_list_recharge_indices_time_ordered(page, rows, n, date_iso, time_short)
                    if ordered:
                        same_min_rows = _np_indices_table_same_minute(
                            page, rows, ordered, date_iso, time_short
                        )
                        to_scan = same_min_rows if same_min_rows else ordered
                        ok = _np_try_screenshot_matching_detail(
                            page,
                            rows,
                            to_scan,
                            machine_substr=machine_substr,
                            expected_credit=expected_credit,
                            out_path=out_path,
                            timeout_ms=timeout_ms,
                        )
                        if ok:
                            matched = True
                            break
                    if not _np_pagination_can_go_next(page):
                        break
                    _np_click_pagination_next(page, timeout_ms=timeout_ms)

                if not matched:
                    raise RuntimeError(
                        f"No {_log_http_backend_tag} Detail on pages 1–"
                        f"{NP_BACKEND_MAX_PAGES} with Request JSON `machineId` containing `{ms or '…'}`, "
                        f"positive `amount`, and amount matching `{expected_credit}`. "
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
                link = row.get_by_text("Show Details", exact=False)
                link.scroll_into_view_if_needed()
                link.click()

                dlg = page.locator(".el-dialog.details-dialog, div[role='dialog'].details-dialog").first
                dlg.wait_for(state="visible", timeout=timeout_ms)
                page.wait_for_timeout(600)
                dlg.screenshot(path=out_path, animations="disabled")
        finally:
            if pause_for_input:
                print(
                    f"[checkcredit] {_log_http_backend_tag} browser left open — watch the window; "
                    "press Enter here to close…",
                    flush=True,
                )
                try:
                    input()
                except EOFError:
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
        "--checkuser",
        action="store_true",
        help=(
            "NP Log Third Http Req: always use headed (visible) Chromium — same detection as "
            "Duty Bot /npthirdhttp; requires --player-id, --date, --time"
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
        metavar="DIGITS",
        help="Digits inside NP machineId (e.g. 2074); empty = skip machine filter",
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
        help="With --checkuser: wait for Enter before closing browser",
    )
    ap.add_argument(
        "--machine-display",
        default="",
        metavar="NAME",
        help="With --checkuser: machine label (e.g. WF8123) to pick Winford vs NP backend — matches Duty Bot context",
    )
    args = ap.parse_args(argv)

    if getattr(args, "checkuser", False):
        if args.finderror:
            print("❌ Use either --finderror or --checkuser, not both.", file=sys.stderr)
            return 2
        if not args.player_id or not args.date or not getattr(args, "credit_time", None):
            print(
                "❌ --checkuser requires --player-id, --date YYYY-MM-DD, --time HH:MM:SS[.mmm]",
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
        try:
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

    if args.finderror:
        td = None
        if args.date:
            try:
                td = datetime.strptime(args.date.strip(), "%Y-%m-%d").date()
            except ValueError:
                print("❌ --date must be YYYY-MM-DD", file=sys.stderr)
                return 2
        use_oss = bool(args.oss) or _env_truthy("CHECKCREDIT_USE_OSS")
        try:
            out = run_finderror(
                str(args.finderror).strip(),
                target_date=td,
                timeout_ms=max(15_000, args.timeout_ms),
                base=args.base_url.rstrip("/"),
                user=DEFAULT_USER,
                pw=DEFAULT_PASS,
                source="oss" if use_oss else "navigator",
            )
        except Exception as e:
            print(f"❌ {e}", file=sys.stderr)
            return 1
        print(out["text"])
        return 0

    ap.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
