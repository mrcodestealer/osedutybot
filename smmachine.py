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

**Batch maintenance + test** (same as webapp ``set_both``; opens a **headed** browser by default)::

    python3 smmachine.py maintenancetest nch1422
    python3 smmachine.py maintenancetest "Dragons Trio-NCH1462"

Optional remark: ``SM_BATCH_REMARK=your note`` (max 100 chars on the EGM dialog).

**Batch toolbar dry-run** (maintenance/test buttons only; opens dialog then **Cancel** — never Save)::

    python3 smmachine.py batchbuttontest
    python3 smmachine.py batchbuttontest nch cp wf

Tests: ``BatchMaintenance``, ``BatchTest``, ``BatchStart Using``, ``BatchTestCancel`` only
(ignores ``BatchKick Out``, ``Sync DB Config``, …).

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
- ``SM_MACHINE_HEADED=1`` — force headed (used by ``maintenancetest`` mode).
- ``SM_BATCH_REMARK`` — optional remark for ``maintenancetest`` / batch EGM save dialog.
- ``SM_MACHINE_STRICT_BACKWARD=1`` — do not re-tick on backward verify if checkboxes were cleared by paging (Element UI tables often drop selection across pages unless ``reserve-selection`` is enabled).

Programmatic read-only export (for dashboards / ``webapp``):

- ``smachine_collect_all_machine_rows(site, …)`` — one backend, all table pages (read-only); returns ``(rows, truncation_warning)``.
- ``smachine_collect_machines_multi_sites()`` — all default backends (deduped by EGM URL); ``WEBMACHINE_SITES`` overrides.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import threading
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Sequence
from urllib.parse import quote

logger = logging.getLogger(__name__)

_ROOT_DIR = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT_DIR / ".env")
except ImportError:
    pass


def _truthy_env(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


# CLI batch modes (first argv): run ``prod_machine_batch`` ``set_both`` (BatchMaintenance + BatchTest).
_BATCH_CLI_MODES: dict[str, str] = {
    "maintenancetest": "set_both",
    "maintaintest": "set_both",
    "setboth": "set_both",
}


def _infer_belongs_from_machine_line(line: str) -> str:
    """Guess belongs code from machine token (e.g. ``nch1422`` → ``NCH``)."""
    alnum = re.sub(r"[^A-Za-z0-9]", "", _normalize_machine_target_line(line)).upper()
    if not alnum:
        return ""
    prefixes = (
        ("NCH", "NCH"),
        ("NWR", "NP"),
        ("NP", "NP"),
        ("TBR", "TBR"),
        ("TBP", "TBP"),
        ("MDR", "MDR"),
        ("DHS", "DHS"),
        ("OSM", "CP"),
        ("CP", "CP"),
        ("WF", "WF"),
        ("WINFORD", "WF"),
    )
    for needle, belongs in prefixes:
        if alnum.startswith(needle) or needle in alnum:
            return belongs
    return ""


def _run_batch_cli_mode(action: str, raw_targets: list[str]) -> None:
    """Headed Playwright batch maintenance/test (webapp-equivalent ``set_both``)."""
    from prod_machine_batch import ACTION_LABELS, run_prod_batch_job

    os.environ.setdefault("SM_MACHINE_HEADED", "1")
    os.environ["SMACHINE_HEADLESS"] = "0"

    remark = (os.environ.get("SM_BATCH_REMARK") or "").strip()[:100]
    machines: list[dict] = []
    for line in raw_targets:
        belongs = _infer_belongs_from_machine_line(line)
        if not belongs:
            raise SystemExit(
                f"Cannot infer belongs for {line!r}. "
                "Use a name with NCH/NWR/MDR/… in it, or pass belongs explicitly later."
            )
        machines.append({"belongs": belongs, "machine": line.strip()})

    label = ACTION_LABELS.get(action, action)
    print(f"Batch mode: {label!r} (action={action})")
    print(f"Headed browser (SM_MACHINE_HEADED=1, SMACHINE_HEADLESS=0)")
    print(f"Targets: {machines}")
    if remark:
        print(f"Remark: {remark!r}")

    summary = run_prod_batch_job(action, machines, remark=remark)
    ok = summary.get("success") or []
    fail = summary.get("failed") or []
    print("")
    print(f"Success: {len(ok)}")
    for m in ok:
        print(f"  ✓ {m.get('belongs')} — {m.get('machine')}")
    print(f"Failed: {len(fail)}")
    for m in fail:
        err = (m.get("error") or "").strip()
        suffix = f" — {err}" if err else ""
        print(f"  ✗ {m.get('belongs')} — {m.get('machine')}{suffix}")
    if fail:
        sys.exit(1)


def _run_batch_button_probe_cli(site_filters: list[str] | None) -> None:
    """Probe EGM batch toolbar buttons on each backend (Cancel only, never Save)."""
    from prod_machine_batch import EGM_TOOLBAR_BATCH_BUTTONS, run_egm_batch_button_probe

    os.environ.setdefault("SM_MACHINE_HEADED", "1")
    os.environ["SMACHINE_HEADLESS"] = "0"

    print("Batch toolbar probe — maintenance/test buttons only; Cancel on confirm (never Save)")
    print(f"Buttons: {', '.join(EGM_TOOLBAR_BATCH_BUTTONS)}")
    if site_filters:
        print(f"Sites: {site_filters}")
    else:
        print("Sites: all PROD backends (WEBMACHINE_SITES or default list)")

    report = run_egm_batch_button_probe(site_filters, headless=False)
    failed = 0
    for sk, site in (report.get("sites") or {}).items():
        print("")
        print(f"=== {sk} ({site.get('belongs', '?')}) ===")
        if site.get("error"):
            print(f"  ERROR: {site['error']}")
            failed += 1
            continue
        if site.get("sample_machine"):
            print(f"  sample row: {site['sample_machine']}")
        for label, probes in (site.get("buttons") or {}).items():
            wo = probes.get("without_selection") or {}
            ws = probes.get("with_selection") or {}
            wo_ok = wo.get("ok")
            ws_ok = ws.get("ok")
            mark = "OK" if wo_ok and ws_ok else "FAIL"
            if not (wo_ok and ws_ok):
                failed += 1
            print(f"  [{mark}] {label}")
            print(f"       no selection: {wo.get('detail') or wo.get('detail', '—')}")
            print(f"       with selection: {ws.get('detail') or '—'}")
    skipped = report.get("skipped") or {}
    if skipped:
        print("")
        print("Skipped (duplicate backend):", skipped)
    print("")
    if failed:
        print(f"Probe finished with {failed} issue(s).")
        sys.exit(1)
    print("Probe finished — all toolbar buttons behaved as expected.")


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


def _normalize_machine_target_line(line: str) -> str:
    """
    Strip dashboard-style ``(TEST)`` suffix before row matching.

    Scraped names often append ``(TEST)`` when ``span.test`` is present, but EGM row
    ``inner_text`` usually omits it — matching would fail on ``…1422(TEST)`` vs ``…1422``.
    """
    raw = (line or "").strip()
    raw = re.sub(r"\(TEST\)\s*$", "", raw, flags=re.I).strip()
    return raw


def _parse_target_line(line: str) -> tuple[str, str]:
    """
    Returns (kind, key) where kind is 'digits' or 'full'.
    digits: standalone numeric id match in row text; full: alphanumeric substring match (case-insensitive).
    """
    raw = _normalize_machine_target_line(line)
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
    """QAT / UAT EGM backends on ``*.osmslot.org`` (see webapp deployment tabs)."""
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
    stall_check: Callable[[], bool] | None = None,
    on_progress: Callable[[int, int], None] | None = None,
) -> tuple[list[dict], str | None]:
    """
    Log in to one explicit EGM origin, optionally dismiss the QAT warning dialog, walk
    ``/egm/egmStatusList`` (read-only), and return normalized rows for webapp.
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
    trunc_msg: str | None = None
    expected_total: int | None = None

    def _tick(pages: int, rows: int) -> None:
        if on_progress:
            on_progress(pages, rows)

    def _maybe_stall(where: str) -> None:
        if stall_check and stall_check():
            raise RuntimeError(f"EGM scrape stalled ({where}; no progress detected)")

    _tick(0, 0)

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
            _maybe_stall("login page")

            pwd_box = page.locator('input[type="password"]').first
            pwd_box.wait_for(state="visible", timeout=min(30_000, timeout_ms))
            _tick(0, 0)
            _maybe_stall("login form")
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
            _tick(0, 0)
            _maybe_stall("after login")
            if dismiss_warning_dialog:
                _dismiss_warning_dialog(page, timeout_ms)
            if path not in (page.url or ""):
                page.goto(list_url, wait_until="domcontentloaded")
            if dismiss_warning_dialog:
                _dismiss_warning_dialog(page, timeout_ms)

            page.wait_for_selector(".app-container, .filter-container, .el-table", timeout=timeout_ms)
            _wait_table_idle(page, timeout_ms)
            _tick(0, 0)
            _maybe_stall("machine table")

            _go_first_page(page, timeout_ms=timeout_ms, max_steps=limit)
            _wait_table_idle(page, timeout_ms)
            expected_total = _pagination_total_entries(page)

            next_clicks = 0
            while True:
                _maybe_stall("pagination")
                for mn, test, game_type, st, onl in _collect_visible_table_machine_rows(page, timeout_ms=timeout_ms):
                    collected.append(
                        {
                            "environment": dep_label,
                            "belongs": belong_label,
                            "name": mn,
                            "game_type": game_type,
                            "status": st,
                            "online": onl,
                            "is_test": test,
                        }
                    )

                _tick(next_clicks + 1, len(collected))

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
            if expected_total is not None and len(collected) < expected_total:
                note = (
                    f"table reports {expected_total} entries but collected {len(collected)} "
                    f"for {belong_label} @ {dep_label}"
                )
                trunc_msg = f"{trunc_msg}; {note}" if trunc_msg else note
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


def _row_report_fields(row, *, timeout_ms: int) -> tuple[str, bool, str, str, str]:
    """
    Machine name (col 1), test mode, Game Type (col 2), Status (col 7),
    Online/Offline (col 8). Returns ``(machine_name, is_test_mode, game_type, status_text, online_or_offline)``.
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
    game_type = _cell_text_one_line(cells.nth(2), timeout_ms=timeout_ms) if n >= 3 else ""
    status = _cell_text_one_line(cells.nth(6), timeout_ms=timeout_ms) if n >= 7 else ""
    online_raw = _cell_text_one_line(cells.nth(7), timeout_ms=timeout_ms) if n >= 8 else ""
    ol = " ".join((online_raw or "").lower().split())
    if "offline" in ol:
        online_disp = "offline"
    elif "online" in ol:
        online_disp = "online"
    else:
        online_disp = online_raw or "(unknown)"
    return name, is_test, game_type, status, online_disp


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
            mn, test, _gt, st, onl = _row_report_fields(row, timeout_ms=timeout_ms)
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


def _find_all_rows_for_target_on_page(page, kind: str, key: str, *, timeout_ms: int):
    rows = _table_body_rows(page)
    try:
        rows.first.wait_for(state="visible", timeout=min(15_000, timeout_ms))
    except Exception:
        pass
    matched = []
    n = rows.count()
    for i in range(n):
        row = rows.nth(i)
        try:
            txt = row.inner_text(timeout=min(8_000, timeout_ms))
        except Exception:
            continue
        if _row_text_matches(kind, key, txt):
            matched.append(row)
    return matched


def _scan_targets_collect_rows(
    page,
    targets: list[tuple[str, str, str]],
    *,
    belongs: str,
    deployment: str,
    timeout_ms: int,
    max_pages: int,
) -> tuple[list[dict], list[str]]:
    """Paginate until target tokens are resolved; return normalized rows + not-found tokens."""
    pending = targets.copy()
    collected: list[dict] = []
    seen_names: set[str] = set()
    dep_label = (deployment or "PROD").strip().upper() or "PROD"
    belong_label = (belongs or "—").strip() or "—"
    next_clicks = 0
    safety = 0

    while pending:
        safety += 1
        if safety > max_pages * max(len(targets), 1) + 50:
            break

        resolved: list[tuple[str, str, str]] = []
        for spec in list(pending):
            line, kind, key = spec
            if kind == "invalid":
                continue
            matched_here = False
            for row in _find_all_rows_for_target_on_page(page, kind, key, timeout_ms=timeout_ms):
                matched_here = True
                mn, test, game_type, st, onl = _row_report_fields(row, timeout_ms=timeout_ms)
                name_key = (mn or "").strip()
                if not name_key or name_key in seen_names:
                    continue
                seen_names.add(name_key)
                collected.append(
                    {
                        "environment": dep_label,
                        "belongs": belong_label,
                        "name": mn,
                        "game_type": game_type,
                        "status": st,
                        "online": onl,
                        "is_test": test,
                    }
                )
            if kind == "full" and matched_here:
                resolved.append(spec)

        for spec in resolved:
            pending.remove(spec)

        if not pending:
            break
        if not _can_pagination_next(page):
            break
        if next_clicks >= max_pages:
            break
        _click_pagination_next(page, timeout_ms=timeout_ms)
        next_clicks += 1
        _wait_table_idle(page, timeout_ms)

    not_found = [line for line, kind, _key in pending if kind != "invalid"]
    not_found.extend(line for line, kind, _key in targets if kind == "invalid")
    return collected, not_found


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


def _pagination_total_entries(page) -> int | None:
    """Parse Element UI footer text like ``Showing 1 to 200 of 247 entries``."""
    try:
        txt = _pagination_root(page).inner_text(timeout=5_000) or ""
    except Exception:
        return None
    m = re.search(r"of\s+([\d,]+)\s+entries", txt, re.I)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _collect_visible_table_machine_rows(page, *, timeout_ms: int) -> list[tuple[str, bool, str, str, str]]:
    """All data rows on the current page: ``(machine_name, is_test, game_type, status, online_word)``."""
    rows = _table_body_rows(page)
    try:
        rows.first.wait_for(state="visible", timeout=min(15_000, timeout_ms))
    except Exception:
        pass
    out: list[tuple[str, bool, str, str, str]] = []
    try:
        n = rows.count()
    except Exception:
        n = 0
    for i in range(n):
        row = rows.nth(i)
        try:
            mn, test, game_type, st, onl = _row_report_fields(row, timeout_ms=timeout_ms)
        except Exception:
            continue
        name = (mn or "").strip()
        if not name:
            continue
        out.append((name, test, (game_type or "").strip(), (st or "").strip(), (onl or "").strip()))
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
    stall_check: Callable[[], bool] | None = None,
    on_progress: Callable[[int, int], None] | None = None,
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
        stall_check=stall_check,
        on_progress=on_progress,
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
    site_key = site.lower()

    if site_key in ("batchbuttontest", "batchbuttonprobe", "probbatch"):
        optional_sites = [x.strip().lower() for x in sys.argv[2:] if x.strip()]
        _run_batch_button_probe_cli(optional_sites or None)
        return

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

    batch_action = _BATCH_CLI_MODES.get(site_key)
    if batch_action:
        _run_batch_cli_mode(batch_action, raw_targets)
        return

    synth = _site_synthetic_machine(site)

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


# ---------------------------------------------------------------------------
# Lark bot: /{site}{set|unset}{maintenance|test|maintenancetest|…} + machine lines
# ---------------------------------------------------------------------------

PROD_BATCH_BOT_CARD_KEY = "prod_batch_set"
PROD_BATCH_BOT_CARD_CALLBACK_KEYS = frozenset({PROD_BATCH_BOT_CARD_KEY})

_PROD_BATCH_BOT_CMD_RE = re.compile(
    r"/(?P<site>nwr|np|nch|nc|new|tbr|tbp|mdr|dhs|cp|osm|wf|winford)"
    r"(?P<op>set|unset)"
    r"(?P<what>maintenancetest|testmaintenance|maintenance|test)\b",
    re.I,
)

_PROD_BATCH_SITE_ENV: dict[str, str] = {
    "nwr": "NWR",
    "np": "NWR",
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

_PROD_BATCH_PENDING: dict[str, dict[str, Any]] = {}
_PROD_BATCH_PENDING_LOCK = threading.Lock()
_PROD_BATCH_PENDING_TTL_SEC = 600

_PROD_BATCH_JOBS: dict[str, dict[str, Any]] = {}
_PROD_BATCH_JOBS_LOCK = threading.Lock()

_PROD_BATCH_ENV_TO_SITE: dict[str, str] = {
    "NWR": "nwr",
    "NCH": "nch",
    "TBR": "tbr",
    "TBP": "tbp",
    "MDR": "mdr",
    "DHS": "dhs",
    "CP": "cp",
    "WF": "wf",
}


def _prod_batch_action_from_parts(op: str, what: str) -> str | None:
    op_l = (op or "").strip().lower()
    what_l = (what or "").strip().lower()
    set_map = {
        "maintenance": "set_maint",
        "test": "set_test",
        "maintenancetest": "set_both",
        "testmaintenance": "set_both",
    }
    unset_map = {
        "maintenance": "unset_maint",
        "test": "unset_test",
        "maintenancetest": "unset_both",
        "testmaintenance": "unset_both",
    }
    if op_l == "set":
        return set_map.get(what_l)
    if op_l == "unset":
        return unset_map.get(what_l)
    return None


def _prod_batch_machine_env_from_name(machine_name: str) -> str | None:
    """Match SET PROD MACHINE page (``wm_prod_set`` ``machineEnvFromName``)."""
    raw = (machine_name or "").strip()
    if not raw:
        return None
    seg = raw.replace("\\", "/").split("/")[-1].strip()
    alnum = re.sub(r"[^A-Za-z0-9]", "", seg).upper()
    if re.match(r"^DHS", seg, re.I) or alnum.startswith("DHS"):
        return "DHS"
    if re.match(r"^NCH", seg, re.I) or alnum.startswith("NCH"):
        return "NCH"
    if re.match(r"^OSM", seg, re.I) or alnum.startswith("OSM"):
        return "CP"
    if re.match(r"^CP", seg, re.I) or alnum.startswith("CP"):
        return "CP"
    if re.match(r"^MDR", seg, re.I) or alnum.startswith("MDR"):
        return "MDR"
    if re.match(r"^TBR", seg, re.I) or alnum.startswith("TBR"):
        return "TBR"
    if re.match(r"^TBP", seg, re.I) or alnum.startswith("TBP"):
        return "TBP"
    if re.match(r"^NWR", seg, re.I) or alnum.startswith("NWR") or re.search(r"NWR[0-9]", alnum):
        return "NWR"
    if re.search(r"winford", raw, re.I):
        return "WF"
    if re.match(r"^WF", seg, re.I) or alnum.startswith("WF"):
        return "WF"
    return None


def _prod_batch_row_matches_env(row: dict, env_code: str) -> bool:
    env = (env_code or "").strip().upper()
    if not env or env == "ALL":
        return True
    belongs = str(row.get("belongs") or "").upper()
    machine = str(row.get("name") or row.get("machine") or "")
    if env == "NWR":
        return _prod_batch_machine_env_from_name(machine) == "NWR"
    if env == "CP":
        return belongs in ("CP", "OSM") or _prod_batch_machine_env_from_name(machine) == "CP"
    return belongs == env or _prod_batch_machine_env_from_name(machine) == env


def _prod_batch_split_target_tokens(line: str) -> list[str]:
    """
    One pasted machine name per line (may contain spaces, e.g. ``5 Dragons-NWR2113``).

    Only ``,`` / ``;`` split multiple names on the same line — never split on whitespace
    inside a display name (otherwise ``5`` matches every machine with ``5`` in the title).
    """
    line = (line or "").strip()
    if not line:
        return []
    if re.search(r"[,;]", line):
        return [p.strip() for p in re.split(r"[,;]+", line) if p.strip()]
    # Full display name with spaces + asset digits — keep whole line.
    if re.search(r"(?:NWR|MDR|NCH|TBR|TBP|DHS|CP|OSM|WF|WINFORD)\s*-?\s*\d", line, re.I):
        return [line]
    if re.search(r"\d", line) and len(line) > 12:
        return [line]
    # Same-line shorthand: ``NWR2113 NWR2114`` or ``2113 2114``
    parts = line.split()
    if len(parts) > 1:
        return parts
    return [line]


def _prod_batch_strip_mention_text(text: str, mention_keys: Sequence[str]) -> str:
    t = text or ""
    for key in mention_keys:
        t = t.replace(key, "")
    t = re.sub(r"@_user_\d+", "", t)
    t = re.sub(r"<[^>]+>", "", t)
    return t


def parse_prod_batch_bot_command(text: str) -> dict[str, Any] | None:
    m = _PROD_BATCH_BOT_CMD_RE.search(text or "")
    if not m:
        return None
    site = m.group("site").lower()
    action = _prod_batch_action_from_parts(m.group("op"), m.group("what"))
    env_code = _PROD_BATCH_SITE_ENV.get(site)
    if not action or not env_code:
        return None
    return {
        "action": action,
        "env_code": env_code,
        "site": site,
        "match": m,
    }


def is_prod_batch_bot_message(original_text: str, mention_keys: Sequence[str]) -> bool:
    body = _prod_batch_strip_mention_text(original_text, mention_keys)
    return parse_prod_batch_bot_command(body) is not None


def _prod_batch_scrape_stall_sec() -> int:
    try:
        return max(60, int((os.environ.get("PROD_BATCH_SCRAPE_STALL_SEC") or "180").strip()))
    except ValueError:
        return 180


def _prod_batch_lookup_target_rows(
    site: str,
    env_code: str,
    target_lines: list[str],
) -> tuple[list[dict], list[str], str]:
    """
    Login once and paginate only until requested machine tokens are resolved (fast path).
    Falls back to the same stall detection as the old full-site scrape.
    """
    sk = (site or "").strip().lower()
    if not sk:
        return [], ["empty site"], "empty site"

    target_specs: list[tuple[str, str, str]] = []
    parse_not_found: list[str] = []
    for line in target_lines:
        for token in _prod_batch_split_target_tokens(line):
            try:
                kind, key = _parse_target_line(token)
            except ValueError:
                parse_not_found.append(token)
                continue
            target_specs.append((token, kind, key))

    if not target_specs:
        return [], parse_not_found, "no valid machine tokens"

    try:
        from checkcredit import _np_resolve_backend  # noqa: WPS433
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        return [], parse_not_found, str(exc)

    try:
        synth = _site_synthetic_machine(sk)
    except SystemExit as exc:
        return [], parse_not_found, str(exc)

    base, user, pw = _np_resolve_backend(synth)
    if not user or not pw:
        return [], parse_not_found, f"missing credentials for {sk!r}"

    path = (os.environ.get("SM_MACHINE_PATH") or "/egm/egmStatusList").strip() or "/egm/egmStatusList"
    if not path.startswith("/"):
        path = "/" + path
    login_url = f"{base.rstrip('/')}/login?redirect={quote(path, safe='')}"
    list_url = f"{base.rstrip('/')}{path}"
    max_pages = _resolve_collect_page_limit(None)
    stall_sec = _prod_batch_scrape_stall_sec()
    timeout_ms = max(120_000, stall_sec * 1000 + 60_000)
    progress = {"last_at": time.monotonic()}
    progress_lock = threading.Lock()

    def on_progress(_pages: int, _rows: int) -> None:
        with progress_lock:
            progress["last_at"] = time.monotonic()

    def stall_check() -> bool:
        with progress_lock:
            idle = time.monotonic() - progress["last_at"]
        return idle >= stall_sec

    belong_label = _site_belongs_label(sk)
    rows: list[dict] = []
    scan_not_found: list[str] = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page(
                    viewport={"width": 1600, "height": 900},
                    ignore_https_errors=True,
                )
                page.set_default_timeout(timeout_ms)
                on_progress(0, 0)

                page.goto(login_url, wait_until="domcontentloaded")
                page.wait_for_timeout(900)
                if stall_check():
                    raise RuntimeError(f"EGM scrape stalled (login page; no progress for {stall_sec}s)")

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
                on_progress(0, 0)
                if stall_check():
                    raise RuntimeError(f"EGM scrape stalled (after login; no progress for {stall_sec}s)")
                if path not in (page.url or ""):
                    page.goto(list_url, wait_until="domcontentloaded")

                page.wait_for_selector(".app-container, .filter-container, .el-table", timeout=timeout_ms)
                _wait_table_idle(page, timeout_ms)
                _go_first_page(page, timeout_ms=timeout_ms, max_steps=max_pages)
                _wait_table_idle(page, timeout_ms)
                on_progress(0, 0)

                rows, scan_not_found = _scan_targets_collect_rows(
                    page,
                    target_specs,
                    belongs=belong_label,
                    deployment="PROD",
                    timeout_ms=timeout_ms,
                    max_pages=max_pages,
                )
                on_progress(1, len(rows))
            finally:
                browser.close()
    except RuntimeError as exc:
        if "stalled" in str(exc).lower():
            return [], parse_not_found, (
                f"Scrape stuck — no progress for {stall_sec}s "
                f"(EGM login or table may be hung). Try again later."
            )
        raise
    except Exception as exc:
        logger.exception("prod-batch bot targeted lookup failed for %r", sk)
        return [], parse_not_found, str(exc)

    matched, resolve_not_found = resolve_prod_batch_bot_targets(env_code, target_lines, rows)
    not_found = list(dict.fromkeys(parse_not_found + scan_not_found + resolve_not_found))
    data_src = f"live EGM fast lookup ({sk.upper()}, {len(matched)} matched)"
    return matched, not_found, data_src


def _prod_batch_scrape_site_rows(site: str) -> tuple[list[dict], str]:
    """
    Live read-only EGM scrape for one PROD backend.

    While ``⏳ Scraping…`` is shown, only a **stall** (no login/table/page progress for
    ``PROD_BATCH_SCRAPE_STALL_SEC``, default 180s) is treated as a scrape error. Slow but
    moving scrapes are allowed to run until finished.
    """
    sk = (site or "").strip().lower()
    if not sk:
        return [], "empty site"

    stall_sec = _prod_batch_scrape_stall_sec()
    progress = {"last_at": time.monotonic()}
    progress_lock = threading.Lock()

    def on_progress(_pages: int, _rows: int) -> None:
        with progress_lock:
            progress["last_at"] = time.monotonic()

    def stall_check() -> bool:
        with progress_lock:
            idle = time.monotonic() - progress["last_at"]
        return idle >= stall_sec

    try:
        rows, twarn = smachine_collect_all_machine_rows(
            sk,
            headless=True,
            stall_check=stall_check,
            on_progress=on_progress,
            timeout_ms=max(120_000, stall_sec * 1000 + 60_000),
        )
        src = f"live EGM ({sk.upper()})"
        if twarn:
            src = f"{src} — {twarn}"
        return rows, src
    except RuntimeError as exc:
        if "stalled" in str(exc).lower():
            return [], (
                f"Scrape stuck — no progress for {stall_sec}s "
                f"(EGM login or table may be hung). Try again later."
            )
        raise
    except Exception as exc:
        logger.exception("prod-batch bot scrape failed for %r", sk)
        return [], str(exc)


def _prod_batch_format_live_summary_md(action: str, summary: dict, *, title_prefix: str) -> str:
    from prod_machine_batch import ACTION_LABELS

    ok = summary.get("success") or []
    fail = summary.get("failed") or []
    lines = [
        f"**{title_prefix} — {ACTION_LABELS.get(action, action)}**",
        f"**Done:** {len(ok)}",
        f"**Not done:** {len(fail)}",
        "",
    ]
    if ok:
        lines.append("**Done (goal met on EGM):**")
        for m in ok[:40]:
            lines.append(f"✓ {m.get('belongs', '')} — {m.get('machine', '')}")
        if len(ok) > 40:
            lines.append(f"... and {len(ok) - 40} more")
    if fail:
        lines.append("")
        lines.append("**Not done:**")
        for m in fail[:40]:
            err = (m.get("error") or "").strip()
            suffix = f" ({err})" if err else ""
            lines.append(f"✗ {m.get('belongs', '')} — {m.get('machine', '')}{suffix}")
        if len(fail) > 40:
            lines.append(f"... and {len(fail) - 40} more")
    return "\n".join(lines)


def _prod_batch_cancel_button(job_id: str) -> dict:
    return {
        "tag": "button",
        "text": {"tag": "plain_text", "content": "Cancel"},
        "type": "danger",
        "behaviors": [
            {
                "type": "callback",
                "value": {
                    "k": PROD_BATCH_BOT_CARD_KEY,
                    "j": job_id,
                    "a": "job_cancel",
                },
            }
        ],
    }


def _prod_batch_job_is_running(job_id: str) -> bool:
    with _PROD_BATCH_JOBS_LOCK:
        job = _PROD_BATCH_JOBS.get(job_id)
        return bool(job and job.get("status") == "running")


def _prod_batch_request_job_cancel(
    job_id: str,
    chat_id: str,
    send_message: Callable[..., Any],
) -> None:
    with _PROD_BATCH_JOBS_LOCK:
        job = _PROD_BATCH_JOBS.get(job_id)
        if not job:
            send_message(chat_id, "⏭️ Job not found or already finished.")
            return
        if job.get("status") != "running":
            send_message(chat_id, "⏭️ Job already finished.")
            return
        thread_root = (job.get("thread_root_message_id") or "").strip() or None
        job["cancel_requested"] = True
    if thread_root:
        try:
            import main as _main_mod  # noqa: WPS433

            _main_mod._set_prod_batch_thread_root(chat_id, thread_root)
        except Exception:
            pass
    send_message(chat_id, "🛑 Cancel requested — stopping after the current step…")


def _prod_batch_send_cancel_live_summary(
    job_id: str,
    send_message: Callable[..., Any],
) -> None:
    from prod_machine_batch import ACTION_LABELS, live_verify_prod_machines

    with _PROD_BATCH_JOBS_LOCK:
        job = _PROD_BATCH_JOBS.get(job_id)
        if not job or job.get("cancel_summary_sent"):
            return
        job["cancel_summary_sent"] = True
        job["status"] = "cancelled"
        action = str(job.get("action") or "")
        machines = list(job.get("machines") or [])
        chat_id = str(job.get("chat_id") or "")
        thread_root = (job.get("thread_root_message_id") or "").strip() or None

    if thread_root and chat_id:
        try:
            import main as _main_mod  # noqa: WPS433

            _main_mod._set_prod_batch_thread_root(chat_id, thread_root)
        except Exception:
            pass

    if not chat_id or not action or not machines:
        return

    try:
        summary = live_verify_prod_machines(action, machines)
    except Exception as exc:
        logger.exception("prod-batch bot cancel verify %s failed", job_id)
        summary = {
            "action": action,
            "success": [],
            "failed": [
                {
                    "belongs": m.get("belongs", ""),
                    "machine": m.get("machine") or m.get("name") or "",
                    "error": str(exc),
                }
                for m in machines
            ],
        }

    with _PROD_BATCH_JOBS_LOCK:
        if job_id in _PROD_BATCH_JOBS:
            _PROD_BATCH_JOBS[job_id]["summary"] = summary

    fail_n = len(summary.get("failed") or [])
    tpl = "red" if fail_n else "green"
    _prod_batch_send_lark_md(
        chat_id,
        f"Cancelled — {ACTION_LABELS.get(action, action)}",
        _prod_batch_format_live_summary_md(action, summary, title_prefix="Cancelled"),
        send_message,
        header_template=tpl,
    )
    _prod_batch_send_machine_screenshots_background(chat_id, machines, summary, send_message)


def resolve_prod_batch_bot_targets(
    env_code: str,
    target_lines: list[str],
    all_rows: list[dict],
) -> tuple[list[dict], list[str]]:
    matched: list[dict] = []
    not_found: list[str] = []
    seen: set[tuple[str, str]] = set()

    for line in target_lines:
        for token in _prod_batch_split_target_tokens(line):
            try:
                kind, key = _parse_target_line(token)
            except ValueError:
                not_found.append(token)
                continue
            hits: list[dict] = []
            for row in all_rows:
                if not _prod_batch_row_matches_env(row, env_code):
                    continue
                machine_name = str(row.get("name") or row.get("machine") or "").strip()
                if not machine_name:
                    continue
                if _row_text_matches(kind, key, machine_name):
                    hits.append(row)
            if not hits:
                not_found.append(token)
                continue
            for row in hits:
                belongs = str(row.get("belongs") or "").strip()
                machine_name = str(row.get("name") or row.get("machine") or "").strip()
                dedupe = (belongs.upper(), machine_name)
                if dedupe in seen:
                    continue
                seen.add(dedupe)
                matched.append(
                    {
                        "belongs": belongs,
                        "machine": machine_name,
                        "status": str(row.get("status") or "").strip(),
                        "online": str(row.get("online") or "").strip(),
                        "is_test": bool(row.get("is_test")),
                    }
                )

    return matched, not_found


def _prod_batch_format_matched_line(m: dict) -> str:
    """One confirm-card bullet — name from live EGM row + status / online."""
    head = f"{m.get('belongs', '')} — {m.get('machine', '')}"
    bits: list[str] = []
    st = (m.get("status") or "").strip()
    onl = (m.get("online") or "").strip()
    if st:
        bits.append(st)
    if onl:
        bits.append(onl)
    if m.get("is_test"):
        bits.append("TEST")
    if bits:
        return f"• {head} — {' / '.join(bits)}"
    return f"• {head}"


def _prod_batch_cleanup_pending() -> None:
    now = time.time()
    with _PROD_BATCH_PENDING_LOCK:
        expired = [
            tok
            for tok, ent in _PROD_BATCH_PENDING.items()
            if now - float(ent.get("created_at") or 0) > _PROD_BATCH_PENDING_TTL_SEC
        ]
        for tok in expired:
            _PROD_BATCH_PENDING.pop(tok, None)


def _prod_batch_confirm_card(
    *,
    token: str,
    action: str,
    env_code: str,
    matched: list[dict],
    not_found: list[str],
    data_src: str,
) -> dict:
    from prod_machine_batch import ACTION_LABELS, LARK_INTRO

    intro = LARK_INTRO.get(action, action)
    label = ACTION_LABELS.get(action, action)
    lines = [
        f"**{label}** ({env_code})",
        intro,
        "",
        f"**Matched ({len(matched)})** — names from live EGM (not copied from your message):",
    ]
    for m in matched[:80]:
        lines.append(_prod_batch_format_matched_line(m))
    if len(matched) > 80:
        lines.append(f"... and {len(matched) - 80} more")
    if not_found:
        lines.append("")
        lines.append(f"**Not found ({len(not_found)}):**")
        for nf in not_found[:40]:
            lines.append(f"• {nf}")
        if len(not_found) > 40:
            lines.append(f"... and {len(not_found) - 40} more")
    if data_src:
        lines.append("")
        lines.append(f"_Source: {data_src}_")
    body_md = "\n".join(lines)
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": f"Confirm — {label}"[:80]},
        },
        "body": {
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": body_md[:4000]}},
                {
                    "tag": "column_set",
                    "flex_mode": "none",
                    "columns": [
                        {
                            "tag": "column",
                            "width": "weighted",
                            "weight": 1,
                            "elements": [
                                {
                                    "tag": "button",
                                    "text": {"tag": "plain_text", "content": "Proceed"},
                                    "type": "primary",
                                    "behaviors": [
                                        {
                                            "type": "callback",
                                            "value": {
                                                "k": PROD_BATCH_BOT_CARD_KEY,
                                                "t": token,
                                                "a": "proceed",
                                            },
                                        }
                                    ],
                                }
                            ],
                        },
                        {
                            "tag": "column",
                            "width": "weighted",
                            "weight": 1,
                            "elements": [
                                {
                                    "tag": "button",
                                    "text": {"tag": "plain_text", "content": "Cancel"},
                                    "type": "default",
                                    "behaviors": [
                                        {
                                            "type": "callback",
                                            "value": {
                                                "k": PROD_BATCH_BOT_CARD_KEY,
                                                "t": token,
                                                "a": "cancel",
                                            },
                                        }
                                    ],
                                }
                            ],
                        },
                    ],
                },
            ]
        },
    }


def _prod_batch_send_lark_card(
    chat_id: str,
    card: dict,
    send_message: Callable[..., Any],
) -> None:
    send_message(chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive")


def _prod_batch_resolve_image_helpers() -> tuple[Callable[..., Any] | None, Callable[..., Any] | None]:
    try:
        import main as _main_mod  # noqa: WPS433

        up = getattr(_main_mod, "upload_image_lark", None)
        si = getattr(_main_mod, "prod_batch_send_image_message", None)
        if not callable(si):
            si = getattr(_main_mod, "send_image_message", None)
        if callable(up) and callable(si):
            return up, si
    except Exception:
        pass
    return None, None


def _prod_batch_cleanup_screenshot_paths(paths: list[str]) -> None:
    for pth in paths:
        if not pth:
            continue
        try:
            os.remove(pth)
        except OSError:
            pass


def _prod_batch_send_machine_screenshots_background(
    chat_id: str,
    machines: list[dict],
    summary: dict | None,
    send_message: Callable[..., Any],
) -> None:
    threading.Thread(
        target=_prod_batch_send_machine_screenshots,
        args=(chat_id, machines, summary, send_message),
        daemon=True,
        name="prod-batch-screenshots",
    ).start()


def _prod_batch_send_machine_screenshots(
    chat_id: str,
    machines: list[dict],
    summary: dict | None,
    send_message: Callable[..., Any],
) -> None:
    from prod_machine_batch import capture_prod_machine_screenshots, prod_batch_screenshots_enabled

    if not prod_batch_screenshots_enabled():
        return

    shots = list((summary or {}).get("screenshots") or [])
    shot_errors = list((summary or {}).get("screenshot_errors") or [])

    if not shots and machines:
        try:
            shots, extra_err = capture_prod_machine_screenshots(machines)
            shot_errors.extend(extra_err)
        except Exception as exc:
            logger.exception("prod-batch bot standalone screenshot capture failed")
            send_message(chat_id, f"⚠️ Machine screenshots unavailable: {exc}")
            return

    if not shots:
        if shot_errors:
            send_message(
                chat_id,
                f"⚠️ Could not capture machine screenshots ({len(shot_errors)} failed).",
            )
        return

    upload_fn, send_img_fn = _prod_batch_resolve_image_helpers()
    paths_to_clean: list[str] = []
    if not upload_fn or not send_img_fn:
        for item in shots:
            pth = str(item.get("path") or "")
            if pth:
                paths_to_clean.append(pth)
        _prod_batch_cleanup_screenshot_paths(paths_to_clean)
        send_message(
            chat_id,
            "⚠️ Machine screenshots were captured but Lark image upload is unavailable on this host.",
        )
        return

    sent = 0
    for item in shots:
        pth = str(item.get("path") or "")
        if not pth:
            continue
        paths_to_clean.append(pth)
        key = upload_fn(pth) or ""
        if not key:
            continue
        belongs = str(item.get("belongs") or "").strip()
        machine = str(item.get("machine") or "").strip()
        label = f"{belongs} — {machine}".strip(" —")
        send_message(chat_id, f"📸 **{label}**")
        resp = send_img_fn(chat_id, key)
        if isinstance(resp, dict) and resp.get("code") == 0:
            sent += 1

    _prod_batch_cleanup_screenshot_paths(paths_to_clean)

    if sent < len(shots):
        send_message(chat_id, f"⚠️ Sent {sent}/{len(shots)} machine screenshot(s).")


def _prod_batch_send_lark_md(
    chat_id: str,
    title: str,
    body_md: str,
    send_message: Callable[..., Any],
    *,
    header_template: str | None = None,
    job_id: str | None = None,
) -> None:
    header: dict[str, Any] = {"title": {"tag": "plain_text", "content": title[:80]}}
    if header_template:
        header["template"] = header_template
    elements: list[dict] = [
        {"tag": "div", "text": {"tag": "lark_md", "content": body_md[:4000]}},
    ]
    if job_id and _prod_batch_job_is_running(job_id):
        elements.append(
            {
                "tag": "action",
                "actions": [_prod_batch_cancel_button(job_id)],
            }
        )
    card = {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": header,
        "body": {"elements": elements},
    }
    _prod_batch_send_lark_card(chat_id, card, send_message)


def _run_prod_batch_bot_job_thread(
    job_id: str,
    chat_id: str,
    action: str,
    remark: str,
    machines: list[dict],
    send_message: Callable[..., Any],
) -> None:
    from prod_machine_batch import ACTION_LABELS, run_prod_batch_job

    with _PROD_BATCH_JOBS_LOCK:
        if job_id not in _PROD_BATCH_JOBS:
            _PROD_BATCH_JOBS[job_id] = {
                "status": "running",
                "action": action,
                "machines": machines,
                "chat_id": chat_id,
                "cancel_requested": False,
                "cancel_summary_sent": False,
            }
        else:
            _PROD_BATCH_JOBS[job_id]["status"] = "running"

    def cancel_check() -> bool:
        with _PROD_BATCH_JOBS_LOCK:
            return bool(_PROD_BATCH_JOBS.get(job_id, {}).get("cancel_requested"))

    def manual_stop_check() -> bool:
        with _PROD_BATCH_JOBS_LOCK:
            return bool(_PROD_BATCH_JOBS.get(job_id, {}).get("manual_stop"))

    def on_manual(summary: dict) -> None:
        fail_n = len(summary.get("failed") or [])
        _prod_batch_send_lark_md(
            chat_id,
            f"Manual needed — {ACTION_LABELS.get(action, action)}",
            (
                f"Some machines failed ({fail_n}) — may have players inside.\n"
                "Finish manually on EGM, then check live status.\n\n"
                "Tap **Cancel** below to stop retries."
            ),
            send_message,
            header_template="red",
            job_id=job_id,
        )

    def on_phase_retry(step_verify: str, attempt: int, failed: list) -> None:
        from prod_machine_batch import PHASE_LABELS

        label = PHASE_LABELS.get(step_verify, step_verify)
        lines = [
            f"**{label} — failed ({len(failed)} machine(s))**",
            "Occupy / game is currently running — will retry when the row allows batch action.",
            f"Will retry automatically (attempt {attempt}) unless you tap **Cancel** below.",
            "",
        ]
        for m in failed[:30]:
            nm = m.get("machine") or m.get("name") or ""
            err = (m.get("error") or "").strip()
            suffix = f" — {err}" if err else ""
            lines.append(f"• {m.get('belongs', '')} — {nm}{suffix}")
        if len(failed) > 30:
            lines.append(f"... and {len(failed) - 30} more")
        _prod_batch_send_lark_md(
            chat_id,
            f"{label} — retry {attempt}",
            "\n".join(lines),
            send_message,
            header_template="red",
            job_id=job_id,
        )

    cancelled = False
    try:
        summary = run_prod_batch_job(
            action,
            machines,
            remark=remark,
            cancel_check=cancel_check,
            manual_stop_check=manual_stop_check,
            on_manual_stop=on_manual,
            on_phase_retry=on_phase_retry,
        )
        with _PROD_BATCH_JOBS_LOCK:
            cancelled = bool(_PROD_BATCH_JOBS.get(job_id, {}).get("cancel_requested"))
            if cancelled:
                _PROD_BATCH_JOBS[job_id]["status"] = "cancelled"
            else:
                _PROD_BATCH_JOBS[job_id]["status"] = "done"
                _PROD_BATCH_JOBS[job_id]["summary"] = summary

        if cancelled:
            _prod_batch_send_cancel_live_summary(job_id, send_message)
            return

        ok_n = len(summary.get("success") or [])
        fail_n = len(summary.get("failed") or [])
        lines = [
            f"**SUMMARY — {ACTION_LABELS.get(action, action)}**",
            f"Success: {ok_n}",
            f"Failed: {fail_n}",
            "",
        ]
        for m in (summary.get("success") or [])[:30]:
            lines.append(f"✓ {m.get('belongs')} — {m.get('machine')}")
        if fail_n:
            lines.append("")
            lines.append("**Still failed:**")
        for m in (summary.get("failed") or [])[:30]:
            err = (m.get("error") or "").strip()
            suffix = f" ({err})" if err else ""
            lines.append(f"✗ {m.get('belongs')} — {m.get('machine')}{suffix}")
        if fail_n > 30:
            lines.append(f"... and {fail_n - 30} more failed")
        tpl = "red" if fail_n else "green"
        title = (
            f"Failed — {ACTION_LABELS.get(action, action)}"
            if fail_n
            else f"Success — {ACTION_LABELS.get(action, action)}"
        )
        _prod_batch_send_lark_md(chat_id, title, "\n".join(lines), send_message, header_template=tpl)
        _prod_batch_send_machine_screenshots_background(chat_id, machines, summary, send_message)
    except Exception as exc:
        logger.exception("prod-batch bot job %s failed", job_id)
        with _PROD_BATCH_JOBS_LOCK:
            if job_id in _PROD_BATCH_JOBS:
                _PROD_BATCH_JOBS[job_id]["status"] = "done"
        _prod_batch_send_lark_md(
            chat_id,
            f"Failed — {ACTION_LABELS.get(action, action)}",
            f"**Job error**\n\n{str(exc)[:3500]}",
            send_message,
            header_template="red",
        )


def _prod_batch_bot_prepare_confirm(
    parsed: dict[str, Any],
    target_lines: list[str],
    *,
    chat_id: str,
    send_message: Callable[..., Any],
    thread_root_message_id: str | None = None,
) -> None:
    env_code = parsed["env_code"]
    site = _PROD_BATCH_ENV_TO_SITE.get(env_code) or parsed.get("site") or ""
    matched, not_found, data_src = _prod_batch_lookup_target_rows(site, env_code, target_lines)
    if "stuck" in data_src.lower() or "stalled" in data_src.lower():
        send_message(chat_id, f"❌ {data_src}")
        return
    if not matched:
        nf = ", ".join(not_found[:20]) if not_found else "(none parsed)"
        send_message(chat_id, f"❌ No machines matched for **{env_code}**. Not found: {nf}")
        return

    token = uuid.uuid4().hex[:16]
    with _PROD_BATCH_PENDING_LOCK:
        _PROD_BATCH_PENDING[token] = {
            "action": parsed["action"],
            "env_code": env_code,
            "machines": matched,
            "not_found": not_found,
            "chat_id": chat_id,
            "thread_root_message_id": (thread_root_message_id or "").strip() or None,
            "created_at": time.time(),
        }

    card = _prod_batch_confirm_card(
        token=token,
        action=parsed["action"],
        env_code=env_code,
        matched=matched,
        not_found=not_found,
        data_src=data_src,
    )
    _prod_batch_send_lark_card(chat_id, card, send_message)


def handle_prod_batch_bot_command(
    original_text: str,
    mention_keys: Sequence[str],
    *,
    chat_id: str,
    send_message: Callable[..., Any],
    thread_root_message_id: str | None = None,
) -> tuple[bool, str | None]:
    """
    Parse bot message, live-scrape EGM for the command site, send confirm card.
    Returns ``(handled, optional_error_text)``.
    """
    _prod_batch_cleanup_pending()
    body = _prod_batch_strip_mention_text(original_text, mention_keys)
    parsed = parse_prod_batch_bot_command(body)
    if not parsed:
        return False, None

    m = parsed["match"]
    first_line = body.splitlines()[0] if body.splitlines() else body
    rest_first = first_line[m.end() :].strip()

    target_lines: list[str] = []
    if rest_first:
        target_lines.append(rest_first)
    for ln in body.splitlines()[1:]:
        ln = ln.strip()
        if ln:
            target_lines.append(ln)

    if not target_lines:
        from prod_machine_batch import ACTION_LABELS

        site = parsed["site"]
        label = ACTION_LABELS.get(parsed["action"], parsed["action"])
        usage = (
            f"❌ Usage: `/{site}{'set' if 'set_' in parsed['action'] else 'unset'}…` "
            f"then machine name(s) on the next lines.\n\n"
            f"Example:\n"
            f"@bot /{site}setmaintenancetest\n"
            f"NCH1422\n"
            f"1423\n\n"
            f"Action: {label}"
        )
        return True, usage

    env_code = parsed["env_code"]
    send_message(
        chat_id,
        f"⏳ 正在 fast lookup live EGM（**{env_code}**）… 只查你列出的机器",
    )
    threading.Thread(
        target=_prod_batch_bot_prepare_confirm,
        args=(parsed, target_lines),
        kwargs={
            "chat_id": chat_id,
            "send_message": send_message,
            "thread_root_message_id": thread_root_message_id,
        },
        daemon=True,
    ).start()
    return True, None


def handle_prod_batch_card_callback(
    parsed: dict[str, Any],
    *,
    chat_id: str,
    send_message: Callable[..., Any],
) -> bool:
    key = str(parsed.get("k") or "").strip().lower()
    if key != PROD_BATCH_BOT_CARD_KEY:
        return False

    job_id = str(parsed.get("j") or "").strip()
    action_btn = str(parsed.get("a") or "").strip().lower()

    if job_id and action_btn == "job_cancel":
        _prod_batch_request_job_cancel(job_id, chat_id, send_message)
        return True

    token = str(parsed.get("t") or "").strip()
    if not token:
        send_message(chat_id, "⏭️ This confirmation expired or was already handled. Send the command again.")
        return True

    _prod_batch_cleanup_pending()

    with _PROD_BATCH_PENDING_LOCK:
        pending = _PROD_BATCH_PENDING.pop(token, None)

    if not pending:
        send_message(chat_id, "⏭️ This confirmation expired or was already handled. Send the command again.")
        return True

    if action_btn == "cancel":
        send_message(chat_id, "Cancelled — no machines were changed.")
        return True

    if action_btn != "proceed":
        send_message(chat_id, "❌ Unknown action on confirmation card.")
        return True

    machines = pending.get("machines") or []
    action = str(pending.get("action") or "").strip()
    if not machines or not action:
        send_message(chat_id, "❌ Confirmation data missing. Send the command again.")
        return True

    thread_root = (pending.get("thread_root_message_id") or "").strip() or None
    if thread_root:
        try:
            import main as _main_mod  # noqa: WPS433

            _main_mod._set_prod_batch_thread_root(chat_id, thread_root)
        except Exception:
            pass

    from prod_machine_batch import ACTION_LABELS, LARK_INTRO

    run_job_id = uuid.uuid4().hex
    intro = LARK_INTRO.get(action, action)
    lines = [
        f"**{ACTION_LABELS.get(action, action)}** — started",
        intro,
        "",
        "Tap **Cancel** below to stop and receive a live EGM done / not-done summary.",
        "",
    ]
    for m in machines[:40]:
        lines.append(f"• {m.get('belongs', '')} — {m.get('machine', '')}")
    if len(machines) > 40:
        lines.append(f"... and {len(machines) - 40} more")

    with _PROD_BATCH_JOBS_LOCK:
        _PROD_BATCH_JOBS[run_job_id] = {
            "status": "running",
            "action": action,
            "machines": machines,
            "chat_id": chat_id,
            "thread_root_message_id": (pending.get("thread_root_message_id") or "").strip() or None,
            "cancel_requested": False,
            "cancel_summary_sent": False,
        }

    _prod_batch_send_lark_md(
        chat_id,
        f"Started — {ACTION_LABELS.get(action, action)}",
        "\n".join(lines),
        send_message,
        header_template="blue",
        job_id=run_job_id,
    )

    threading.Thread(
        target=_run_prod_batch_bot_job_thread,
        args=(run_job_id, chat_id, action, "", machines, send_message),
        daemon=True,
    ).start()
    return True


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted (Ctrl+C); browser cleanup runs before exit.", file=sys.stderr)
        raise SystemExit(130)
