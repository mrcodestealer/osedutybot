#!/usr/bin/env python3
"""
Find user log errors (--finderror).

  LogNavigator (browser, headed): default — drives UI like manual flow.
  OSS (HTTP): faster, no browser — GET plain log file from object storage.

  python3 checkcredit.py --finderror 2074 --date 2026-04-27
  python3 checkcredit.py --finderror CP0231 --date 2026-02-05 --oss

Env (optional):
  CHECKCREDIT_USER / CHECKCREDIT_PASSWORD — LogNavigator login
  LOG_NAVIGATOR_BASE — LogNavigator base URL
  OSM_LOG_OSS_TEMPLATE — default:
    https://oss-osm-log.osmplay.com/MINIPC/{machine}/logic/{date}.log
  OSS_MACHINE_FOLDER_TEMPLATE — when --finderror is digits-only (default NWR{n}, e.g. 2074 → NWR2074)
  CHECKCREDIT_USE_OSS — set to 1/true so callers (e.g. main.py bot) use OSS without --oss
  CHECKCREDIT_HEADLESS=1 — force headless Chromium (for Linux servers without X11)
  CHECKCREDIT_HEADED=1 — force headed window (needs DISPLAY; macOS/Windows OK)

  On Linux, if $DISPLAY is unset, LogNavigator mode uses headless automatically.

Requires: playwright (+ chromium) for LogNavigator; requests for OSS.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, datetime
from typing import Any

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


def parse_user_blocks_full(log_text: str) -> list[dict[str, Any]]:
    """
    Split by extra1/extra2/extra3 userid markers; lines until the next marker belong to that player.
    Every block included (errors may be empty). Error lines carry line_idx for ordering.
    Multi-line log records: timestamp on the first line is carried to following lines (no leading time)
    so reduce_num / successJson on `extra:` continuation lines get the correct time.
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
        latest_credit: dict[str, Any] | None = best_coin if best_coin is not None else best_reduce
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
                "max_line_idx": -1,
            }
        by_uid[uid]["errors"].extend(blk.get("errors") or [])
        lc = blk.get("latest_credit")
        if lc:
            slot = "_lc_reduce" if lc.get("source") == "reduce_num" else "_lc_coin"
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
        lc_final = d["_lc_coin"] if d["_lc_coin"] is not None else d["_lc_reduce"]
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

    parts = [
        "---",
        f"🕐 **Time:** `{time_s}`",
        f"🖥 **Machine:** `{machine_display}`",
        f"🆔 **User ID:** `{uid}`",
        f"💰 **Last credit:** {credit_s}",
    ]
    if error_log_mode == "always":
        inner = log_md if log_md else "(no error lines)"
        parts.append("📋 **Error log:**")
        parts.append(f"```\n{inner}\n```")
    elif log_md:
        parts.append("📋 **Error log:**")
        parts.append(f"```\n{log_md}\n```")
    return "\n".join(parts)


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
        body = "\n".join(_player_detail_block(r, machine_display, error_log_mode="if_any") for r in slice_rows)
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": body}})
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
        body = "\n".join(_player_detail_block(r, machine_display, error_log_mode="always") for r in slice_rows)
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": body}})
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
        "\n\n".join(_plain_player_block(r, machine_display, error_log_mode="if_any") for r in top2_any[:2])
        if top2_any
        else "(no players)"
    )
    sec2 = (
        "\n\n".join(_plain_player_block(r, machine_display, error_log_mode="always") for r in top2_err[:2])
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


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="checkcredit — LogNavigator helpers")
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
    args = ap.parse_args(argv)

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
    print(
        "\nExamples:\n"
        "  python3 checkcredit.py --finderror 2074 --date 2026-04-27\n"
        "  python3 checkcredit.py --finderror CP0231 --date 2026-02-05 --oss\n",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
