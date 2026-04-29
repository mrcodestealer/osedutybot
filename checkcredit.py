#!/usr/bin/env python3
"""
LogNavigator: find user log errors (--finderror). Browser runs with a visible window.

  python3 checkcredit.py --finderror 1300
  python3 checkcredit.py --finderror 130 --date 2026-04-27

Env (optional):
  CHECKCREDIT_USER / CHECKCREDIT_PASSWORD (default osm / osm123)
  LOG_NAVIGATOR_BASE (default https://lognavigator.cliveslot.com)

Requires: pip install playwright && playwright install chromium
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


# ----- machine option matching -----
def option_matches_machine_query(option_text: str, query: str) -> bool:
    """
    Match Select2 option label to user digits, e.g.:
      query "1300" -> NCH1300 (digit group 1300)
      query "130"  -> NCH0130 or NCH130 (int 130), not NCH1300 (1300)
    Any digit run whose int value equals int(query) counts.
    """
    q = (query or "").strip()
    if not q.isdigit():
        return False
    qv = int(q)
    for m in re.finditer(r"\d+", option_text):
        try:
            if int(m.group(0)) == qv:
                return True
        except ValueError:
            continue
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
            f"No machine option matches digits {machine_query!r}. "
            "Try full machine id (e.g. 2074 for NWR2074)."
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


_USERID_START = re.compile(
    r'extra1\s*:\s*["\']?userid\s*:\s*(\d+)',
    re.I,
)
_ERR_ZERO = re.compile(r"""['\"]error['\"]\s*:\s*0\b""")
_CUR_COIN = re.compile(r"""['\"]cur_coin['\"]\s*:\s*([\d.]+)""")
_LINE_TIME_HMS = re.compile(r"^(\d{2}:\d{2}:\d{2})")


def _parse_success_cur_coin(line: str) -> tuple[float, str] | None:
    """Last successJson line with cur_coin and error 0 in a userid block — caller keeps last match."""
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
    tm = _LINE_TIME_HMS.match(line.strip())
    tshort = tm.group(1) if tm else ""
    return (val, tshort)


def parse_user_blocks_for_errors(log_text: str) -> list[dict[str, Any]]:
    """
    Split by extra1 userid markers; within each block collect lines where JSON-ish payload has error > 0.
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
        latest_credit: dict[str, Any] | None = None
        for line_idx, line in blines:
            sc = _parse_success_cur_coin(line)
            if sc:
                val, tshort = sc
                latest_credit = {
                    "line_idx": line_idx,
                    "value": val,
                    "time_short": tshort,
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
            time_part = tm.group(1) if tm else ""
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
                }
            )
        if findings:
            row: dict[str, Any] = {"user_id": uid, "errors": findings}
            if latest_credit:
                row["latest_credit"] = latest_credit
            out.append(row)
    return out


def format_finderror_report_terminal(payload: list[dict[str, Any]]) -> str:
    """Plain terminal output; merge rows that share the same User ID."""
    if not payload:
        return "✅ No error > 0 found in scanned user blocks (extra1: userid).\n"

    order: list[str] = []
    by_uid: dict[str, list[dict[str, Any]]] = {}
    credit_by_uid: dict[str, dict[str, Any]] = {}
    for blk in payload:
        uid = blk["user_id"]
        if uid not in by_uid:
            order.append(uid)
            by_uid[uid] = []
        by_uid[uid].extend(blk["errors"])
        lc = blk.get("latest_credit")
        if lc:
            old = credit_by_uid.get(uid)
            if old is None or lc["line_idx"] > old["line_idx"]:
                credit_by_uid[uid] = lc

    blocks: list[str] = []
    for uid in order:
        errs = by_uid[uid]
        n = len(errs)
        lines_body = "\n".join(
            e.get("full_line") or e.get("snippet") or "" for e in errs
        )
        lc_line = ""
        cr = credit_by_uid.get(uid)
        if cr:
            ts = cr.get("time_short") or ""
            val = cr["value"]
            if ts:
                lc_line = f"💰 Latest credit : {val} at {ts}\n"
            else:
                lc_line = f"💰 Latest credit : {val}\n"
        blocks.append(
            f"🔔 User ID : {uid}\n"
            f"⚠️ Error detected count : {n}\n"
            f"{lc_line}"
            f"📋 Error found List :\n"
            f"{lines_body}"
        )
    return "\n\n".join(blocks) + "\n"


def run_finderror(
    machine_query: str,
    *,
    target_date: date | None,
    timeout_ms: int,
    base: str,
    user: str,
    pw: str,
) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    td = target_date or date.today()
    text_parts: list[str] = []
    parsed: list[dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
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
            text_parts.append(f"→ Machine selected: {chosen}")
            wait_for_logs_file_browser(page, timeout_ms=timeout_ms)

            navigate_to_logic_folder(page, timeout_ms=timeout_ms)
            click_log_file_for_date(page, td, timeout_ms=timeout_ms)
            bump_tail_and_execute(page, timeout_ms=timeout_ms)

            pre = page.locator("section[role='results'] pre, pre.nofloat").first
            pre.wait_for(state="attached", timeout=timeout_ms)
            log_body = pre.inner_text() or ""

            parsed = parse_user_blocks_for_errors(log_body)

            text_parts.append(f"→ Date file: {td.isoformat()}")
        finally:
            browser.close()

    header = "\n".join(text_parts)
    report = format_finderror_report_terminal(parsed)
    plain = f"{header}\n\n{report}" if header else report
    return {"text": plain}


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
    args = ap.parse_args(argv)

    if args.finderror:
        td = None
        if args.date:
            try:
                td = datetime.strptime(args.date.strip(), "%Y-%m-%d").date()
            except ValueError:
                print("❌ --date must be YYYY-MM-DD", file=sys.stderr)
                return 2
        try:
            out = run_finderror(
                str(args.finderror).strip(),
                target_date=td,
                timeout_ms=max(15_000, args.timeout_ms),
                base=args.base_url.rstrip("/"),
                user=DEFAULT_USER,
                pw=DEFAULT_PASS,
            )
        except Exception as e:
            print(f"❌ {e}", file=sys.stderr)
            return 1
        print(out["text"])
        return 0

    ap.print_help()
    print(
        "\nExample:\n  python3 checkcredit.py --finderror 2074 --date 2026-04-27\n",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
