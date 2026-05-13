#!/usr/bin/env python3
"""
SM machine list — login routing matches ``checkcredit.py`` backends; tick table row checkboxes with pagination.

Usage::

    python3 smmachine.py nwr
    1932
    NCH1933
    nch1922
    <press Enter on an empty line to finish — or press Ctrl+D>

Or one line (no stdin needed)::

    python3 smmachine.py nwr 1932 NCH1933 nch1922

First argument is a **site alias** (which backend / login to open):

- ``nwr``, ``np`` → NP (``backend-np``), synthetic route ``NWR0001``
- ``nch``, ``nc``, ``new`` → NCH (``backend-nc``)
- **Check status (read-only):** alias suffix ``cs``. Groups found machines under headings like ``Machine in online, maintain, no test mode`` then lists names; **only non-empty groups** are printed. **Test** = ``span.test`` or ``(TEST)`` in text. **Not found** section only if any request is missing.
- ``tbr`` → TBR (``backend-tbr``)
- ``tbp``, ``mdr``, ``dhs``, ``cp``, ``osm``, ``wf``, ``winford`` → same mapping as ``checkcredit``

Credentials: same env / ``.env`` as Duty Bot (``NP_BACKEND_*``, ``NCH_BACKEND_*``, ``TBR_BACKEND_*``, …).

Flow:

1. Login and open the machine table (default ``/egm/egmStatusList``; override with ``SM_MACHINE_PATH``).
2. Ensure pagination is on **first** page (Previous until disabled).
3. In **request order**, find each machine; **only tick** if Status is **normal** or **occupy** and Online/Offline is **online**. Rows in **maintenance** / **offline** / other statuses are **not ticked**; if their checkbox is on, it is cleared. Those machines are listed before the backward pass.
4. If some targets remain unfound, click **Next**, repeat (bounded by ``NP_BACKEND_MAX_PAGES`` / ``SM_MACHINE_MAX_PAGES``).
5. After every **eligible** row is ticked, walk **backward** with **Previous** through every page visited; on each page re-verify checkboxes for ticked machines only (do not assume).
6. Print machine row labels that are still checked; then AFK ``SM_MACHINE_AFK_SEC`` (default **90**) seconds.

Env:

- ``SM_MACHINE_PATH`` — path after host (default ``/egm/egmStatusList``).
- ``SM_MACHINE_AFK_SEC`` — idle seconds at end (default ``90``).
- ``SM_MACHINE_MAX_PAGES`` — max Next steps for **CLI** tick/report (default: ``NP_BACKEND_MAX_PAGES``, often 20).
- ``SM_MACHINE_COLLECT_MAX_PAGES`` — for **read-only** ``smachine_collect_all_machine_rows`` / web dashboard only:
  max Next steps when ``SM_MACHINE_MAX_PAGES`` is **unset** (default **500** so full machine lists are not cut off early).
- ``SM_MACHINE_HEADLESS=1`` — headless Chromium (default: headed unless Linux without DISPLAY).
- ``SM_MACHINE_STRICT_BACKWARD=1`` — do not re-tick on backward verify if checkboxes were cleared by paging (Element UI tables often drop selection across pages unless ``reserve-selection`` is enabled).

Programmatic read-only export (for dashboards / ``webmachine``):

- ``smachine_collect_all_machine_rows(site, …)`` — one backend, all table pages (read-only); returns ``(rows, truncation_warning)``.
- ``smachine_collect_machines_multi_sites()`` — all default backends (deduped by EGM URL); ``WEBMACHINE_SITES`` overrides.
"""

from __future__ import annotations

import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import quote

_ROOT_DIR = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT_DIR / ".env")
except ImportError:
    pass


def _truthy_env(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def _site_routing_key(site: str) -> str:
    """
    ``<alias>cs`` (suffix ``cs`` = check status) routes like ``<alias>`` for backend / credentials.
    Example: ``mdrcs`` → ``mdr``, ``nchcs`` → ``nch``.
    """
    s = (site or "").strip().lower()
    if len(s) > 2 and s.endswith("cs"):
        return s[:-2]
    return s


def _site_synthetic_machine(site: str) -> str:
    """Machine label that routes ``checkcredit._np_resolve_backend`` to the desired host."""
    s = _site_routing_key(site)
    aliases: dict[str, str] = {
        "nwr": "NWR0001",
        "np": "NWR0001",
        "nch": "NCH0001",
        "nc": "NCH0001",
        "new": "NCH0001",
        "tbr": "TBR0001",
        "tbp": "TBP0001",
        "mdr": "MDR0001",
        "dhs": "DHS0001",
        "cp": "CP0001",
        "osm": "OSM0001",
        "wf": "WF0001",
        "winford": "WF0001",
    }
    syn = aliases.get(s)
    if not syn:
        raise SystemExit(
            f"Unknown site alias {site!r}. Try: {', '.join(sorted(set(aliases.keys())))}"
        )
    return syn


def _parse_target_line(line: str) -> tuple[str, str]:
    """
    Returns (kind, key) where kind is 'digits' or 'full'.
    digits: standalone numeric id match in row text; full: alphanumeric substring match (case-insensitive).
    """
    raw = (line or "").strip()
    if not raw:
        raise ValueError("empty machine line")
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw)
    if not alnum:
        raise ValueError(f"no machine token in {line!r}")
    if alnum.isdigit():
        return ("digits", alnum)
    return ("full", alnum.upper())


def _site_belongs_label(site_key: str) -> str:
    """Venue / property code for dashboard ``belongs`` column (PROD site aliases)."""
    labels = {
        "nwr": "NP",
        "np": "NP",
        "nch": "NCH",
        "nc": "NCH",
        "new": "NCH",
        "tbr": "TBR",
        "tbp": "TBP",
        "mdr": "MDR",
        "dhs": "DHS",
        "cp": "CP",
        "osm": "CP",
        "wf": "WF",
        "winford": "WF",
    }
    return labels.get((site_key or "").strip().lower(), (site_key or "").upper())


def _osmslot_admin_credentials() -> tuple[str, str]:
    user = (os.environ.get("WEBMACHINE_OSMSLOT_USER") or os.environ.get("OSMSLOT_ADMIN_USER") or "admin").strip()
    pw = (os.environ.get("WEBMACHINE_OSMSLOT_PASSWORD") or os.environ.get("OSMSLOT_ADMIN_PASSWORD") or "123456").strip()
    return user, pw


def _nonprod_backend_specs(deployment: str) -> list[dict[str, str | bool]]:
    """QAT / UAT EGM backends on ``*.osmslot.org`` (see webmachine deployment tabs)."""
    dep = (deployment or "").strip().upper()
    if dep not in ("QAT", "UAT"):
        return []
    prefix = "qat" if dep == "QAT" else "uat"
    user, pw = _osmslot_admin_credentials()
    hosts: tuple[tuple[str, str], ...] = (
        ("CP", f"https://{prefix}-cp.osmslot.org"),
        ("TBP", f"https://{prefix}-tbp.osmslot.org"),
        ("TBR", f"https://{prefix}-tbr.osmslot.org"),
        ("DHS", f"https://{prefix}-dhs.osmslot.org"),
        ("NCH", f"https://{prefix}-nc.osmslot.org"),
        ("WF", f"https://{prefix}-wf.osmslot.org"),
        ("MDR", f"https://{prefix}-mdr.osmslot.org"),
        ("NP", f"https://{prefix}-np.osmslot.org"),
    )
    out: list[dict[str, str | bool]] = []
    for belongs, base in hosts:
        out.append(
            {
                "belongs": belongs,
                "base": base,
                "user": user,
                "password": pw,
                "deployment": dep,
                "dismiss_warning_dialog": dep == "QAT",
                "list_path": "/egm/egmStatusList",
                "login_path": "/login",
            }
        )
    return out


def _dismiss_warning_dialog(page, timeout_ms: int) -> None:
    """Close Element UI ``Warnning`` modal (QAT) via header X before reading the EGM table."""
    try:
        dialog = page.locator('.el-dialog[aria-label="Warnning"], .el-dialog:has(.el-dialog__title:has-text("Warnning"))').first
        if dialog.count() == 0:
            return
        close = dialog.locator(".el-dialog__headerbtn[aria-label='Close'], .el-dialog__headerbtn").first
        if close.count() and close.is_visible(timeout=min(5000, timeout_ms)):
            close.click()
            page.wait_for_timeout(450)
    except Exception:
        pass


def _resolve_collect_page_limit(max_pages: int | None) -> int:
    from checkcredit import NP_BACKEND_MAX_PAGES  # noqa: WPS433

    if max_pages is None:
        explicit = (os.environ.get("SM_MACHINE_MAX_PAGES") or "").strip()
        if explicit:
            try:
                return max(1, int(explicit))
            except ValueError:
                return max(1, NP_BACKEND_MAX_PAGES)
        try:
            collect_cap = int((os.environ.get("SM_MACHINE_COLLECT_MAX_PAGES") or "500").strip() or "500")
        except ValueError:
            collect_cap = 500
        return max(1, collect_cap)
    return max(1, int(max_pages))


def smachine_collect_rows_at_backend(
    *,
    base_url: str,
    username: str,
    password: str,
    belongs: str,
    deployment: str,
    list_path: str = "/egm/egmStatusList",
    login_path: str = "/login",
    dismiss_warning_dialog: bool = False,
    headless: bool | None = None,
    max_pages: int | None = None,
    timeout_ms: int = 120_000,
) -> tuple[list[dict], str | None]:
    """
    Log in to one explicit EGM origin, optionally dismiss the QAT warning dialog, walk
    ``/egm/egmStatusList`` (read-only), and return normalized rows for webmachine.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise RuntimeError("Install playwright: pip install playwright && playwright install chromium") from e

    base = (base_url or "").strip().rstrip("/")
    if not base:
        raise ValueError("empty base_url")
    user = (username or "").strip()
    pw = (password or "").strip()
    if not user or not pw:
        raise RuntimeError(f"missing credentials for {belongs!r} @ {deployment}")

    path = (list_path or "/egm/egmStatusList").strip() or "/egm/egmStatusList"
    if not path.startswith("/"):
        path = "/" + path
    login = (login_path or "/login").strip() or "/login"
    if not login.startswith("/"):
        login = "/" + login
    login_url = f"{base}{login}?redirect={quote(path, safe='')}"
    list_url = f"{base}{path}"

    limit = _resolve_collect_page_limit(max_pages)
    hl = _smachine_resolve_headless(headless)
    dep_label = (deployment or "PROD").strip().upper() or "PROD"
    belong_label = (belongs or "—").strip() or "—"
    collected: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    trunc_msg: str | None = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=hl)
        try:
            context = browser.new_context(
                viewport={"width": 1600, "height": 900},
                ignore_https_errors=True,
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
            if dismiss_warning_dialog:
                _dismiss_warning_dialog(page, timeout_ms)
            if path not in (page.url or ""):
                page.goto(list_url, wait_until="domcontentloaded")
            if dismiss_warning_dialog:
                _dismiss_warning_dialog(page, timeout_ms)

            page.wait_for_selector(".app-container, .filter-container, .el-table", timeout=timeout_ms)
            _wait_table_idle(page, timeout_ms)

            _go_first_page(page, timeout_ms=timeout_ms, max_steps=limit)
            _wait_table_idle(page, timeout_ms)

            next_clicks = 0
            while True:
                for mn, test, st, onl in _collect_visible_table_machine_rows(page, timeout_ms=timeout_ms):
                    key = (dep_label, belong_label, mn)
                    if key in seen:
                        continue
                    seen.add(key)
                    collected.append(
                        {
                            "environment": dep_label,
                            "belongs": belong_label,
                            "name": mn,
                            "status": st,
                            "online": onl,
                            "is_test": test,
                        }
                    )

                if not _can_pagination_next(page):
                    break
                if next_clicks >= limit:
                    try:
                        if _can_pagination_next(page):
                            trunc_msg = (
                                f"pagination stopped after {limit} page(s); more data exists — "
                                "raise SM_MACHINE_COLLECT_MAX_PAGES or set SM_MACHINE_MAX_PAGES"
                            )
                    except Exception:
                        trunc_msg = f"pagination stopped after {limit} page(s) (could not verify Next)"
                    break
                _click_pagination_next(page, timeout_ms=timeout_ms)
                next_clicks += 1
                _wait_table_idle(page, timeout_ms)
        finally:
            browser.close()

    return collected, trunc_msg


def _row_text_matches(kind: str, key: str, row_text: str) -> bool:
    t = (row_text or "").upper()
    if kind == "full":
        return key in re.sub(r"[^A-Z0-9]", "", t)
    # digits — avoid matching a shorter number inside a longer id when possible
    return bool(re.search(rf"(?<![0-9]){re.escape(key)}(?![0-9])", t))


def _wait_table_idle(page, timeout_ms: int) -> None:
    try:
        page.wait_for_function(
            "() => !Array.from(document.querySelectorAll('.el-loading-mask')).some(x => x && x.offsetParent !== null)",
            timeout=min(timeout_ms, 30_000),
        )
    except Exception:
        pass
    page.wait_for_timeout(350)


def _pagination_root(page):
    """Prefer the list page footer inside ``.app-container`` (dialogs often teleport outside)."""
    scoped = page.locator(".app-container .el-pagination")
    if scoped.count():
        return scoped.first
    return page.locator(".el-pagination").first


def _pagination_prev_btn(page):
    return _pagination_root(page).locator("button.btn-prev").first


def _pagination_next_btn(page):
    return _pagination_root(page).locator("button.btn-next").first


def _can_pagination_prev(page) -> bool:
    btn = _pagination_prev_btn(page)
    if btn.count() == 0:
        return False
    try:
        return not btn.is_disabled()
    except Exception:
        return False


def _can_pagination_next(page) -> bool:
    btn = _pagination_next_btn(page)
    if btn.count() == 0:
        return False
    try:
        return not btn.is_disabled()
    except Exception:
        return False


def _click_pagination_prev(page, *, timeout_ms: int) -> None:
    btn = _pagination_prev_btn(page)
    btn.wait_for(state="visible", timeout=min(15_000, timeout_ms))
    btn.click(timeout=min(30_000, timeout_ms))
    page.wait_for_timeout(900)


def _click_pagination_next(page, *, timeout_ms: int) -> None:
    btn = _pagination_next_btn(page)
    btn.wait_for(state="visible", timeout=min(15_000, timeout_ms))
    btn.click(timeout=min(30_000, timeout_ms))
    page.wait_for_timeout(900)


def _go_first_page(page, *, timeout_ms: int, max_steps: int) -> None:
    for _ in range(max_steps + 5):
        if not _can_pagination_prev(page):
            return
        _click_pagination_prev(page, timeout_ms=timeout_ms)


def _row_checkbox_input(row):
    # Prefer the selection column only (avoids accidental inputs elsewhere in wide rows).
    sel_cell = row.locator("td.el-table-column--selection").first
    if sel_cell.count():
        for sel in (
            'input.el-checkbox__original[type="checkbox"]',
            ".el-checkbox__input input.el-checkbox__original",
            'input[type="checkbox"]',
        ):
            cand = sel_cell.locator(sel).first
            if cand.count():
                return cand
    # Element UI row selection: label.el-checkbox > span.el-checkbox__input > input.el-checkbox__original
    for sel in (
        'input.el-checkbox__original[type="checkbox"]',
        ".el-checkbox__input input.el-checkbox__original",
        '.el-checkbox input[type="checkbox"]',
    ):
        cand = row.locator(sel).first
        if cand.count():
            return cand
    return row.locator('td.el-table-column--selection input[type="checkbox"]').first


def _read_dom_checked(inp, *, timeout_ms: int) -> bool:
    """Read ``HTMLInputElement.checked`` with a short bound (avoids 120s default action timeout)."""
    try:
        return bool(inp.evaluate("el => el.checked", timeout=max(500, min(10_000, timeout_ms))))
    except Exception:
        return False


def _wait_until_checked(page, inp, *, total_ms: int, poll_ms: int = 200) -> bool:
    """Poll until checked or ``total_ms`` elapsed (Vue / Element UI may update after paint)."""
    total_ms = max(500, total_ms)
    poll_ms = max(80, poll_ms)
    deadline = time.monotonic() + total_ms / 1000.0
    while time.monotonic() < deadline:
        if _read_dom_checked(inp, timeout_ms=min(3_000, total_ms)):
            return True
        page.wait_for_timeout(poll_ms)
    return _read_dom_checked(inp, timeout_ms=min(3_000, total_ms))


def _wait_until_unchecked(page, inp, *, total_ms: int, poll_ms: int = 200) -> bool:
    total_ms = max(500, total_ms)
    poll_ms = max(80, poll_ms)
    deadline = time.monotonic() + total_ms / 1000.0
    while time.monotonic() < deadline:
        if not _read_dom_checked(inp, timeout_ms=min(3_000, total_ms)):
            return True
        page.wait_for_timeout(poll_ms)
    return not _read_dom_checked(inp, timeout_ms=min(3_000, total_ms))


def _norm_cell_upper(s: str) -> str:
    return " ".join((s or "").upper().split())


def _row_tick_eligibility(row, *, timeout_ms: int) -> tuple[bool, str]:
    """
    Tick only when Status (cell index **6**) is **normal** or **occupy** and Online/Offline (index **7**) is **online**.
    Reject maintenance, offline, or any other status.
    """
    cells = row.locator("td.el-table__cell")
    try:
        n = cells.count()
    except Exception:
        n = 0
    if n < 8:
        return False, "fewer than 8 columns — cannot read Status / Online-Offline"

    status_raw = _cell_text_one_line(cells.nth(6), timeout_ms=timeout_ms)
    online_raw = _cell_text_one_line(cells.nth(7), timeout_ms=timeout_ms)
    su = _norm_cell_upper(status_raw)
    ou = _norm_cell_upper(online_raw)

    reasons: list[str] = []
    if "MAINTAIN" in su:
        reasons.append("status is maintenance")
    elif "NORMAL" not in su and "OCCUPY" not in su:
        reasons.append(f"status is not normal or occupy ({status_raw or 'empty'})")

    if "OFFLINE" in ou:
        reasons.append("Online/Offline column shows offline")
    elif "ONLINE" not in ou:
        reasons.append(f"Online/Offline column is not online ({online_raw or 'empty'})")

    if reasons:
        return False, "; ".join(reasons)
    return True, ""


def _row_display_name(row) -> str:
    try:
        return " ".join(((row.inner_text() or "").strip()).split())
    except Exception:
        return ""


def _cell_raw_text(cell, *, timeout_ms: int) -> str:
    """
    Prefer ``text_content()`` for full subtree text; use with ``span.test`` detection because ``(TEST)``
    may be CSS-only (not in text nodes).
    """
    t = min(8_000, timeout_ms)
    try:
        tc = cell.text_content(timeout=t)
        if tc is not None and tc.strip():
            return tc
    except Exception:
        pass
    try:
        return cell.inner_text(timeout=t) or ""
    except Exception:
        return ""


def _cell_text_one_line(cell, *, timeout_ms: int) -> str:
    raw = _cell_raw_text(cell, timeout_ms=timeout_ms)
    return " ".join((raw or "").strip().split())


def _machine_name_cell_test_mode_and_display(cell, *, timeout_ms: int) -> tuple[bool, str]:
    """
    Detect EGM test row: Vue uses ``<div>…name…</div><span class="test"></span>``; ``(TEST)`` is often
    **not** in the DOM (only ``::after`` / CSS), so ``textContent`` misses it. Fallback: literal ``(TEST)`` in text.
    """
    name_line = _cell_text_one_line(cell, timeout_ms=timeout_ms)
    literal = bool(re.search(r"\(TEST\)", name_line or "", re.I))
    span_test = False
    try:
        span_test = cell.locator("span.test").first.count() > 0
    except Exception:
        span_test = False
    is_test = literal or span_test
    if is_test and span_test and not literal:
        display = f"{name_line}(TEST)" if name_line else "(TEST)"
    else:
        display = name_line
    return is_test, display


def _row_summary_label(row, *, timeout_ms: int) -> str:
    """
    Short row label for EGM status table (Element UI): machine name, game type, status column.
    Typical columns: 0 selection, 1 Machine Name, 2 Game Type, …, 6 Status (``occupy`` / …).
    """
    cells = row.locator("td.el-table__cell")
    try:
        n = cells.count()
    except Exception:
        n = 0
    parts: list[str] = []
    if n >= 2:
        _tm, t = _machine_name_cell_test_mode_and_display(cells.nth(1), timeout_ms=timeout_ms)
        if t:
            parts.append(t)
    if n >= 3:
        t = _cell_text_one_line(cells.nth(2), timeout_ms=timeout_ms)
        if t:
            parts.append(t)
    if n >= 7:
        t = _cell_text_one_line(cells.nth(6), timeout_ms=timeout_ms)
        if t:
            parts.append(t)
    if parts:
        return " ".join(parts)
    return _row_display_name(row)


def _row_report_fields(row, *, timeout_ms: int) -> tuple[str, bool, str, str]:
    """
    Machine name (col 1), test mode (``span.test`` and/or literal ``(TEST)``), Status (col 7),
    Online/Offline (col 8). Returns ``(machine_name, is_test_mode, status_text, online_or_offline)``.
    """
    cells = row.locator("td.el-table__cell")
    try:
        n = cells.count()
    except Exception:
        n = 0
    if n >= 2:
        is_test, name = _machine_name_cell_test_mode_and_display(cells.nth(1), timeout_ms=timeout_ms)
    else:
        is_test, name = False, ""
    status = _cell_text_one_line(cells.nth(6), timeout_ms=timeout_ms) if n >= 7 else ""
    online_raw = _cell_text_one_line(cells.nth(7), timeout_ms=timeout_ms) if n >= 8 else ""
    ol = " ".join((online_raw or "").lower().split())
    if "offline" in ol:
        online_disp = "offline"
    elif "online" in ol:
        online_disp = "online"
    else:
        online_disp = online_raw or "(unknown)"
    return name, is_test, status, online_disp


def _norm_online_word(onl: str) -> str:
    ol = " ".join((onl or "").lower().split())
    if "offline" in ol:
        return "offline"
    if "online" in ol:
        return "online"
    return ol or "unknown"


def _norm_status_word(st: str) -> str:
    return " ".join((st or "").lower().split()) or "unknown"


def _print_check_status_groups(
    report: dict[tuple[str, str], tuple[str, bool, str, str]],
    targets: list[tuple[str, str, str]],
) -> None:
    """
    One section per (online/offline, Status column, test | no test) combo that has at least one machine.
    Heading: ``Machine in online, maintain, test mode`` then indented machine display names.
    """
    groups: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    not_found_lines: list[str] = []

    for _line, kind, key in targets:
        row = report.get((kind, key))
        if row is None:
            not_found_lines.append(_line)
            continue
        mn, is_test, st, onl = row
        conn = _norm_online_word(onl)
        stat = _norm_status_word(st)
        test_phrase = "test" if is_test else "no test"
        groups[(conn, stat, test_phrase)].append(mn)

    n_req = len(targets)
    n_ok = n_req - len(not_found_lines)
    print(f"Checked {n_req} request(s); found {n_ok}, not found {len(not_found_lines)}.")
    print("")

    def _group_sort_key(k: tuple[str, str, str]) -> tuple:
        conn, stat, test_phrase = k
        conn_i = {"online": 0, "offline": 1}.get(conn, 2)
        test_i = 0 if test_phrase == "test" else 1
        return (conn_i, stat, test_i)

    for key in sorted(groups.keys(), key=_group_sort_key):
        names = groups[key]
        if not names:
            continue
        conn, stat, test_phrase = key
        print(f"Machine in {conn}, {stat}, {test_phrase} mode")
        for name in names:
            print(f"  {name}")
        print("")

    if not_found_lines:
        print("Not found in table (within page limit):")
        for line in not_found_lines:
            print(f"  {line!r}")
        print("")


def _scan_targets_report_only(
    page,
    targets: list[tuple[str, str, str]],
    *,
    timeout_ms: int,
    max_pages: int,
) -> dict[tuple[str, str], tuple[str, bool, str, str]]:
    """Paginate forward until every target row is found; no checkbox interaction."""
    pending = targets.copy()
    next_clicks = 0
    found: dict[tuple[str, str], tuple[str, bool, str, str]] = {}
    safety = 0
    while pending:
        safety += 1
        if safety > max_pages * max(len(targets), 1) + 50:
            raise RuntimeError("Status scan exceeded safety iteration limit.")

        matched_this_page: list[tuple[str, str, str]] = []
        for spec in list(pending):
            _line, kind, key = spec
            row = _find_row_for_target(page, kind, key, timeout_ms)
            if row is None:
                continue
            mn, test, st, onl = _row_report_fields(row, timeout_ms=timeout_ms)
            found[(kind, key)] = (mn, test, st, onl)
            matched_this_page.append(spec)

        for spec in matched_this_page:
            pending.remove(spec)

        if not pending:
            break

        if not _can_pagination_next(page):
            missing = [s[0] for s in pending]
            raise RuntimeError(f"No Next page; still missing machines: {missing}")

        if next_clicks >= max_pages:
            missing = [s[0] for s in pending]
            raise RuntimeError(f"Hit SM_MACHINE_MAX_PAGES ({max_pages}); missing: {missing}")

        _click_pagination_next(page, timeout_ms=timeout_ms)
        next_clicks += 1
        _wait_table_idle(page, timeout_ms)

    return found


def _table_body_rows(page):
    """
    Data rows only (not header). Target **main** scroll body only — fixed-column tables also use
    ``tr.el-table__row`` and duplicate rows; loose selectors pick clones whose checkbox does not
    reflect the real selection.
    """
    strict = page.locator(
        "div.el-table__body-wrapper > table.el-table__body > tbody > tr.el-table__row"
    )
    if strict.count():
        return strict
    primary = page.locator(
        ".el-table__body-wrapper:not(.el-table__fixed-body-wrapper) tbody tr.el-table__row"
    )
    if primary.count():
        return primary
    fallback = page.locator(".el-table__body tbody tr.el-table__row")
    if fallback.count():
        return fallback
    return page.locator(".el-table__body tr.el-table__row")


def _find_row_for_target(
    page,
    kind: str,
    key: str,
    timeout_ms: int,
    *,
    prefer_checked: bool = False,
):
    rows = _table_body_rows(page)
    try:
        rows.first.wait_for(state="visible", timeout=min(15_000, timeout_ms))
    except Exception:
        pass
    n = rows.count()
    matched_indices: list[int] = []
    for i in range(n):
        row = rows.nth(i)
        try:
            txt = row.inner_text(timeout=min(8_000, timeout_ms))
        except Exception:
            continue
        if _row_text_matches(kind, key, txt):
            if not prefer_checked:
                return row
            matched_indices.append(i)

    if not prefer_checked or not matched_indices:
        return None

    for i in matched_indices:
        row = rows.nth(i)
        if _verify_row_checkbox_checked(page, row, timeout_ms=timeout_ms):
            return row
    return rows.nth(matched_indices[0])


def _ensure_row_checkbox_checked(page, row, *, timeout_ms: int) -> None:
    try:
        row.scroll_into_view_if_needed(timeout=min(15_000, timeout_ms))
    except Exception:
        pass
    inp = _row_checkbox_input(row)
    if inp.count() == 0:
        raise RuntimeError("Row matched machine but no checkbox input (selector mismatch).")
    inp.wait_for(state="attached", timeout=min(15_000, timeout_ms))
    if _read_dom_checked(inp, timeout_ms=3_000):
        return
    lab = row.locator(".el-checkbox").first
    if lab.count():
        lab.click(timeout=min(30_000, timeout_ms))
    else:
        inp.click(timeout=min(30_000, timeout_ms))
    if _wait_until_checked(page, inp, total_ms=12_000):
        return
    inp.click(force=True, timeout=min(30_000, timeout_ms))
    if not _wait_until_checked(page, inp, total_ms=12_000):
        raise RuntimeError("Could not tick checkbox after click (still unchecked after ~12s polls).")


def _ensure_row_checkbox_unchecked(page, row, *, timeout_ms: int) -> None:
    """Clear row selection if checked (maintenance / offline skips)."""
    try:
        row.scroll_into_view_if_needed(timeout=min(15_000, timeout_ms))
    except Exception:
        pass
    inp = _row_checkbox_input(row)
    if inp.count() == 0:
        return
    inp.wait_for(state="attached", timeout=min(15_000, timeout_ms))
    if not _read_dom_checked(inp, timeout_ms=3_000):
        return
    lab = row.locator(".el-checkbox").first
    if lab.count():
        lab.click(timeout=min(30_000, timeout_ms))
    else:
        inp.click(timeout=min(30_000, timeout_ms))
    if _wait_until_unchecked(page, inp, total_ms=12_000):
        return
    inp.click(force=True, timeout=min(30_000, timeout_ms))
    if not _wait_until_unchecked(page, inp, total_ms=12_000):
        raise RuntimeError("Could not clear checkbox after click (still checked after ~12s polls).")


def _verify_row_checkbox_checked(page, row, *, timeout_ms: int) -> bool:
    try:
        row.scroll_into_view_if_needed(timeout=min(8_000, timeout_ms))
    except Exception:
        pass
    inp = _row_checkbox_input(row)
    if inp.count() == 0:
        return False
    try:
        inp.wait_for(state="attached", timeout=min(8_000, timeout_ms))
        return _read_dom_checked(inp, timeout_ms=min(8_000, timeout_ms))
    except Exception:
        return False


def _collect_visible_table_machine_rows(page, *, timeout_ms: int) -> list[tuple[str, bool, str, str]]:
    """All data rows on the current page: ``(machine_name, is_test, status, online_word)``."""
    rows = _table_body_rows(page)
    try:
        rows.first.wait_for(state="visible", timeout=min(15_000, timeout_ms))
    except Exception:
        pass
    out: list[tuple[str, bool, str, str]] = []
    try:
        n = rows.count()
    except Exception:
        n = 0
    for i in range(n):
        row = rows.nth(i)
        try:
            mn, test, st, onl = _row_report_fields(row, timeout_ms=timeout_ms)
        except Exception:
            continue
        name = (mn or "").strip()
        if not name:
            continue
        out.append((name, test, (st or "").strip(), (onl or "").strip()))
    return out


def _smachine_resolve_headless(headless: bool | None) -> bool:
    if headless is not None:
        return bool(headless)
    if _truthy_env("SM_MACHINE_HEADLESS"):
        return True
    if _truthy_env("SM_MACHINE_HEADED"):
        return False
    return sys.platform == "linux" and not (os.environ.get("DISPLAY") or "").strip()


def smachine_collect_all_machine_rows(
    site: str,
    *,
    headless: bool | None = None,
    max_pages: int | None = None,
    timeout_ms: int = 120_000,
) -> tuple[list[dict], str | None]:
    """
    Log in to one backend (same routing as CLI), walk the EGM status table from first page forward,
    and return every visible row (**read-only**, no checkbox changes, no other UI actions).

    Pagination: if ``max_pages`` is ``None`` and ``SM_MACHINE_MAX_PAGES`` is unset, uses
    ``SM_MACHINE_COLLECT_MAX_PAGES`` (default **500**) so large sites are fully walked. Set
    ``SM_MACHINE_MAX_PAGES`` to override with the same knob as the CLI.

    Returns ``(rows, truncation_warning)`` where ``truncation_warning`` is set if the table still
    had a enabled **Next** when the page cap was hit (list may be incomplete).
    """
    from checkcredit import _np_resolve_backend  # noqa: WPS433

    site_key = _site_routing_key(site or "")
    if not site_key:
        raise ValueError("empty site")
    try:
        synth = _site_synthetic_machine(site)
    except SystemExit as e:
        raise ValueError(str(e)) from e
    base, user, pw = _np_resolve_backend(synth)
    if not user or not pw:
        raise RuntimeError(f"missing backend credentials for {site_key!r}")

    path = (os.environ.get("SM_MACHINE_PATH") or "/egm/egmStatusList").strip() or "/egm/egmStatusList"
    return smachine_collect_rows_at_backend(
        base_url=base,
        username=user,
        password=pw,
        belongs=_site_belongs_label(site_key),
        deployment="PROD",
        list_path=path,
        login_path="/login",
        dismiss_warning_dialog=False,
        headless=headless,
        max_pages=max_pages,
        timeout_ms=timeout_ms,
    )


def _dedupe_site_keys_by_resolved_backend(site_keys: list[str]) -> tuple[list[str], dict[str, str]]:
    """
    Each distinct EGM origin (``base_url`` + login user) is scraped once; later aliases that map to
    the same login (e.g. ``osm`` after ``cp`` on ``backend.osmplay.com``) are skipped with a note.
    """
    from checkcredit import _np_resolve_backend  # noqa: WPS433

    seen: dict[tuple[str, str], str] = {}
    order: list[str] = []
    skipped: dict[str, str] = {}
    for sk in site_keys:
        try:
            synth = _site_synthetic_machine(sk)
        except SystemExit:
            order.append(sk)
            continue
        try:
            base, u, pw = _np_resolve_backend(synth)
        except Exception:
            order.append(sk)
            continue
        if not pw:
            order.append(sk)
            continue
        key = (base.rstrip("/"), (u or "").strip())
        if key in seen:
            skipped[sk] = f"skipped — same EGM as {seen[key]!r}"
            continue
        seen[key] = sk
        order.append(sk)
    return order, skipped


DEFAULT_WEBMACHINE_SITES: tuple[str, ...] = ("nwr", "nch", "tbr", "tbp", "mdr", "dhs", "cp", "osm", "wf")


def smachine_collect_machines_multi_sites(
    sites: Sequence[str] | None = None,
    **kwargs: Any,
) -> tuple[list[dict], dict[str, str]]:
    """
    Scrape several site aliases in sequence. ``kwargs`` are passed to ``smachine_collect_all_machine_rows``
    (e.g. ``headless=``, ``max_pages=``, ``timeout_ms=``).

    Returns ``(rows, errors_by_site_key)`` where ``errors_by_site_key`` holds per-site failure or
    truncation messages (and skipped-alias notes from :func:`_dedupe_site_keys_by_resolved_backend`).

    Default site list: ``DEFAULT_WEBMACHINE_SITES`` (every routed backend from ``checkcredit``) or
    env ``WEBMACHINE_SITES`` (comma-separated).
    """
    raw_env = (os.environ.get("WEBMACHINE_SITES") or "").strip()
    if sites is not None:
        use = [s.strip().lower() for s in sites if (s or "").strip()]
    elif raw_env:
        use = [s.strip().lower() for s in raw_env.split(",") if s.strip()]
    else:
        use = list(DEFAULT_WEBMACHINE_SITES)

    use, skipped = _dedupe_site_keys_by_resolved_backend(use)
    errs: dict[str, str] = dict(skipped)
    all_rows: list[dict] = []
    for sk in use:
        try:
            part, twarn = smachine_collect_all_machine_rows(sk, **kwargs)
            all_rows.extend(part)
            if twarn:
                errs[sk] = twarn
        except Exception as e:
            errs[sk] = str(e)
    return all_rows, errs


def smachine_collect_nonprod_deployment(
    deployment: str,
    **kwargs: Any,
) -> tuple[list[dict], dict[str, str]]:
    """Scrape every QAT or UAT ``*.osmslot.org`` backend in :func:`_nonprod_backend_specs`."""
    dep = (deployment or "").strip().upper()
    specs = _nonprod_backend_specs(dep)
    if not specs:
        return [], {dep: f"unsupported deployment {deployment!r}"}
    errs: dict[str, str] = {}
    all_rows: list[dict] = []
    for spec in specs:
        belongs = str(spec["belongs"])
        key = f"{dep}:{belongs}"
        try:
            part, twarn = smachine_collect_rows_at_backend(
                base_url=str(spec["base"]),
                username=str(spec["user"]),
                password=str(spec["password"]),
                belongs=belongs,
                deployment=dep,
                list_path=str(spec["list_path"]),
                login_path=str(spec["login_path"]),
                dismiss_warning_dialog=bool(spec["dismiss_warning_dialog"]),
                **kwargs,
            )
            all_rows.extend(part)
            if twarn:
                errs[key] = twarn
        except Exception as e:
            errs[key] = str(e)
    return all_rows, errs


def smachine_collect_machines_all_deployments(
    **kwargs: Any,
) -> tuple[list[dict], dict[str, str]]:
    """
    Scrape configured deployments (``WEBMACHINE_DEPLOYMENTS``, default ``prod,qat,uat``).
    PROD uses :func:`smachine_collect_machines_multi_sites`; QAT/UAT use explicit osmslot hosts.
    """
    raw = (os.environ.get("WEBMACHINE_DEPLOYMENTS") or "prod,qat,uat").strip()
    deployments = [d.strip().upper() for d in raw.split(",") if d.strip()]
    if not deployments:
        deployments = ["PROD"]
    all_rows: list[dict] = []
    errs: dict[str, str] = {}
    for dep in deployments:
        if dep == "PROD":
            part, e = smachine_collect_machines_multi_sites(**kwargs)
        elif dep in ("QAT", "UAT"):
            part, e = smachine_collect_nonprod_deployment(dep, **kwargs)
        else:
            errs[dep] = f"unknown deployment {dep!r}"
            continue
        all_rows.extend(part)
        errs.update(e)
    return all_rows, errs


def main() -> None:
    if len(sys.argv) < 2:
        print(
            __doc__.strip(),
            file=sys.stderr,
        )
        sys.exit(2)

    site = sys.argv[1].strip()
    synth = _site_synthetic_machine(site)

    if len(sys.argv) > 2:
        raw_targets = [x.strip() for x in sys.argv[2:] if x.strip()]
    else:
        raw_targets = []
        tty_in = sys.stdin.isatty()
        if tty_in:
            print(
                "Machine lines: type one name per line, then press Enter on an empty line to start.\n"
                "Tip: one-shot — python3 smmachine.py <site> <machine> <machine> …",
                file=sys.stderr,
            )
        for line in sys.stdin:
            s = line.strip()
            # Interactive terminal: EOF is easy to forget; empty line ends input (pipes still use EOF).
            if tty_in and not s and raw_targets:
                break
            if s:
                raw_targets.append(s)

    if not raw_targets:
        print("No machine lines provided (stdin or argv after site).", file=sys.stderr)
        sys.exit(2)

    targets: list[tuple[str, str, str]] = []
    for line in raw_targets:
        kind, key = _parse_target_line(line)
        targets.append((line, kind, key))

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise SystemExit("Install playwright: pip install playwright && playwright install chromium")

    from checkcredit import NP_BACKEND_MAX_PAGES, _np_log_backend_tag, _np_resolve_backend  # noqa: WPS433

    base, user, pw = _np_resolve_backend(synth)
    tag = _np_log_backend_tag(synth)
    if not user or not pw:
        raise SystemExit(f"Missing backend credentials for routed backend {tag} (see checkcredit env vars).")

    path = (os.environ.get("SM_MACHINE_PATH") or "/egm/egmStatusList").strip() or "/egm/egmStatusList"
    if not path.startswith("/"):
        path = "/" + path
    login_url = f"{base}/login?redirect={quote(path, safe='')}"
    list_url = f"{base}{path}"

    try:
        max_pages = max(1, int((os.environ.get("SM_MACHINE_MAX_PAGES") or "").strip() or str(NP_BACKEND_MAX_PAGES)))
    except ValueError:
        max_pages = max(1, NP_BACKEND_MAX_PAGES)

    try:
        afk_sec = max(0, int((os.environ.get("SM_MACHINE_AFK_SEC") or "90").strip() or "90"))
    except ValueError:
        afk_sec = 90

    # Default: show a window when possible so you can confirm ticks during AFK.
    headless = _smachine_resolve_headless(None)

    timeout_ms = 120_000

    sk = (site or "").strip().lower()
    report_only = len(sk) > 2 and sk.endswith("cs")
    print(f"Site alias: {site!r} → backend tag {tag!r} ({base})")
    if report_only:
        base_alias = _site_routing_key(site)
        print(
            f"Mode: status report only (suffix 'cs' = check status; same backend as {base_alias!r}; "
            "no checkbox changes)."
        )
    print(f"Targets (order): {raw_targets}")

    pending = targets.copy()
    next_clicks = 0

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
            if path not in (page.url or ""):
                page.goto(list_url, wait_until="domcontentloaded")

            page.wait_for_selector(".app-container, .filter-container, .el-table", timeout=timeout_ms)
            _wait_table_idle(page, timeout_ms)

            _go_first_page(page, timeout_ms=timeout_ms, max_steps=max_pages)
            _wait_table_idle(page, timeout_ms)

            if report_only:
                report = _scan_targets_report_only(page, targets, timeout_ms=timeout_ms, max_pages=max_pages)
                print("")
                _print_check_status_groups(report, targets)
                print(f"AFK {afk_sec}s — inspect the browser; close the window manually when done.")
                time.sleep(afk_sec)
                return

            tick_targets: list[tuple[str, str, str]] = []
            skipped_report: list[tuple[str, str, str]] = []

            # --- Forward: tick eligible targets only ---
            safety = 0
            while pending:
                safety += 1
                if safety > max_pages * max(len(targets), 1) + 50:
                    raise RuntimeError("Forward scan exceeded safety iteration limit.")

                matched_this_page: list[tuple[str, str, str]] = []
                for spec in list(pending):
                    _line, kind, key = spec
                    row = _find_row_for_target(page, kind, key, timeout_ms)
                    if row is None:
                        continue
                    summ = _row_summary_label(row, timeout_ms=timeout_ms)
                    ok_elig, why_not = _row_tick_eligibility(row, timeout_ms=timeout_ms)
                    if not ok_elig:
                        print(
                            f"  Skip (ineligible): {_line!r} → {summ!r} — {why_not}",
                            file=sys.stderr,
                        )
                        _ensure_row_checkbox_unchecked(page, row, timeout_ms=timeout_ms)
                        skipped_report.append((_line, summ, why_not))
                        matched_this_page.append(spec)
                        continue

                    print(f"  Tick (forward): {_line!r} → {summ!r}")
                    _ensure_row_checkbox_checked(page, row, timeout_ms=timeout_ms)
                    if not _verify_row_checkbox_checked(page, row, timeout_ms=timeout_ms):
                        raise RuntimeError(f"Checkbox for {_line!r} did not read as checked after tick.")
                    matched_this_page.append(spec)
                    tick_targets.append(spec)

                for spec in matched_this_page:
                    pending.remove(spec)

                if not pending:
                    break

                if not _can_pagination_next(page):
                    missing = [s[0] for s in pending]
                    raise RuntimeError(f"No Next page; still missing machines: {missing}")

                if next_clicks >= max_pages:
                    missing = [s[0] for s in pending]
                    raise RuntimeError(f"Hit SM_MACHINE_MAX_PAGES ({max_pages}); missing: {missing}")

                _click_pagination_next(page, timeout_ms=timeout_ms)
                next_clicks += 1
                _wait_table_idle(page, timeout_ms)

            if skipped_report:
                print("", file=sys.stderr)
                print(
                    "Will not tick these machines (checkbox cleared if it was checked), since they are in "
                    "maintenance status or offline, or status is not normal/occupy:",
                    file=sys.stderr,
                )
                for sl, lbl, rs in skipped_report:
                    print(f"  {sl!r} → {lbl} — {rs}", file=sys.stderr)
                print("", file=sys.stderr)

            # --- Backward: re-verify every page (only rows we ticked) ---
            confirmed: dict[tuple[str, str], str] = {}

            for step in range(next_clicks, -1, -1):
                print(f"Re-verify page (backward step {next_clicks - step}/{next_clicks})…")
                _wait_table_idle(page, timeout_ms)
                for _line, kind, key in tick_targets:
                    row = _find_row_for_target(
                        page, kind, key, timeout_ms, prefer_checked=True
                    )
                    if row is None:
                        continue
                    if not _verify_row_checkbox_checked(page, row, timeout_ms=timeout_ms):
                        # Element UI often clears row selection when leaving the page unless the table uses
                        # reserve-selection — DOM then reads unchecked even though we ticked earlier.
                        if _truthy_env("SM_MACHINE_STRICT_BACKWARD"):
                            raise RuntimeError(
                                f"Backward verify failed: {_line!r} row present but checkbox not checked "
                                f"({_row_summary_label(row, timeout_ms=timeout_ms)!r}). "
                                f"Try enabling reserve-selection on the table, or omit SM_MACHINE_STRICT_BACKWARD "
                                f"to allow one automatic re-tick during backward pass."
                            )
                        print(
                            f"  Backward: {_line!r} reads unchecked after paging — "
                            f"re-ticking once (selection often clears across pages in Element UI).",
                            file=sys.stderr,
                        )
                        _ensure_row_checkbox_checked(page, row, timeout_ms=timeout_ms)
                        if not _verify_row_checkbox_checked(page, row, timeout_ms=timeout_ms):
                            raise RuntimeError(
                                f"Backward verify failed after re-tick: {_line!r} "
                                f"({_row_summary_label(row, timeout_ms=timeout_ms)!r})."
                            )
                    label = _row_summary_label(row, timeout_ms=timeout_ms)
                    if not label:
                        raise RuntimeError(f"Backward verify: empty row text for requested {_line!r}.")
                    confirmed[(kind, key)] = label

                if step > 0:
                    if not _can_pagination_prev(page):
                        raise RuntimeError("Expected Previous during backward walk but button disabled.")
                    _click_pagination_prev(page, timeout_ms=timeout_ms)

            for _line, kind, key in tick_targets:
                if (kind, key) not in confirmed:
                    raise RuntimeError(
                        f"Backward pass never re-located a row for {_line!r}; cannot verify checkbox state."
                    )

            print("")
            print("Verified ticked machines (re-checked on walk-back, checkbox read from DOM):")
            printed = 0
            for _line, kind, key in tick_targets:
                lbl = confirmed.get((kind, key))
                if lbl:
                    print(f"  {_line!r} → {lbl}")
                    printed += 1
            if printed == 0:
                print("  (none — no rows matched during backward pass)")

            print("")
            print(f"AFK {afk_sec}s — inspect the browser; close the window manually when done.")
            time.sleep(afk_sec)
        finally:
            browser.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted (Ctrl+C); browser cleanup runs before exit.", file=sys.stderr)
        raise SystemExit(130)
