"""
Playwright automation for PROD machine batch maintenance/test on EGM status pages.

Recheck / verification always reads the **live** EGM table in the headless browser
(``smmachine`` row parsers). It never uses webapp JSON or ``webmachine_data.json``.
"""
from __future__ import annotations

import logging
import os
import re
import tempfile
import time
from typing import Any, Callable, Optional
from urllib.parse import quote

from smmachine import (
    _can_pagination_next,
    _click_pagination_next,
    _ensure_row_checkbox_checked,
    _ensure_row_checkbox_unchecked,
    _find_row_for_target,
    _go_first_page,
    _parse_target_line,
    _resolve_collect_page_limit,
    _row_match_text_for_target,
    _row_report_fields,
    _row_text_matches,
    _site_synthetic_machine,
    _smachine_resolve_headless,
    _table_body_rows,
    _wait_table_idle,
)

logger = logging.getLogger(__name__)

ACTION_BUTTONS = {
    "set_maint": ["BatchMaintenance"],
    "set_test": ["BatchTest"],
    "set_both": ["BatchMaintenance", "BatchTest"],
    "unset_maint": ["BatchStart Using"],
    "unset_test": ["BatchTestCancel"],
    "unset_both": ["BatchStart Using", "BatchTestCancel"],
}

# Two-step actions: finish phase 1 on live EGM (retry until pass or max), then phase 2, then summary.
PHASED_STEPS: dict[str, list[tuple[str, list[str]]]] = {
    "set_both": [
        ("set_maint", ["BatchMaintenance"]),
        ("set_test", ["BatchTest"]),
    ],
    "unset_both": [
        ("unset_maint", ["BatchStart Using"]),
        ("unset_test", ["BatchTestCancel"]),
    ],
}

PHASE_LABELS = {
    "set_maint": "Set maintenance",
    "set_test": "Set test",
    "unset_maint": "Unset maintenance",
    "unset_test": "Unset test",
}

# Single-step actions: same retry + live EGM recheck as each phase in set_both (e.g. player in game).
AUTO_RETRY_ACTIONS = frozenset(
    {"set_maint", "set_test", "unset_maint", "unset_test"} | set(PHASED_STEPS.keys())
)

ACTION_LABELS = {
    "set_maint": "Set maintenance",
    "set_test": "Set test",
    "set_both": "Set maintenance and Set test",
    "unset_maint": "Unset maintenance",
    "unset_test": "Unset test",
    "unset_both": "Unset maintenance and unset test",
}

LARK_INTRO = {
    "set_maint": "Will set maintenance to machines below:",
    "set_test": "Will set test to machines below:",
    "set_both": "Will set maintenance and test to machines below:",
    "unset_maint": "Will unset maintenance on machines below:",
    "unset_test": "Will unset test on machines below:",
    "unset_both": "Will unset maintenance and test on machines below:",
}

# SET PROD MACHINE only uses these four toolbar buttons (ignore BatchKick Out, Sync DB Config, …).
EGM_PROD_BATCH_BUTTONS: tuple[str, ...] = (
    "BatchMaintenance",
    "BatchTest",
    "BatchStart Using",
    "BatchTestCancel",
)

# Dry-run probe uses the same set as :data:`ACTION_BUTTONS`.
EGM_TOOLBAR_BATCH_BUTTONS: tuple[str, ...] = EGM_PROD_BATCH_BUTTONS

_SITE_ALIAS_BELONGS: dict[str, str] = {
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

_BELONGS_SITE = {
    "NP": "nwr",
    "NWR": "nwr",
    "NCH": "nch",
    "NC": "nch",
    "TBR": "tbr",
    "TBP": "tbp",
    "MDR": "mdr",
    "DHS": "dhs",
    "CP": "cp",
    "OSM": "osm",
    "WF": "wf",
    "WINFORD": "wf",
}


def _belongs_for_machine(belongs: str) -> str:
    b = (belongs or "").strip().upper()
    if b in ("NWR", "NP"):
        return "NP"
    return b


def _belongs_site_key(belongs: str) -> str:
    b = _belongs_for_machine(belongs)
    return _BELONGS_SITE.get(b, b.lower())


def _machine_display_name(machine: dict) -> str:
    return (machine.get("name") or machine.get("machine") or "").strip()


def _default_timeout_ms() -> int:
    return int(os.environ.get("PROD_SET_TIMEOUT_MS", "600000"))


def prod_batch_screenshots_enabled() -> bool:
    """After set/unset maintenance/test, capture EGM status-row PNGs (default on)."""
    return (os.environ.get("PROD_BATCH_SCREENSHOTS", "1").strip().lower() not in ("0", "false", "no", "off"))


def _screenshot_egm_row_locator(page, row, *, timeout_ms: int) -> str:
    """PNG of one EGM table row only — no View / cog / operation dialog."""
    try:
        row.scroll_into_view_if_needed(timeout=min(15_000, timeout_ms))
    except Exception:
        pass
    _page_pause(page, 200)
    fd, out_path = tempfile.mkstemp(suffix=".png", prefix="prod_batch_egm_row_")
    os.close(fd)
    row.screenshot(path=out_path, animations="disabled", scale="css")
    return out_path


def capture_machine_row_screenshot_on_page(
    page,
    machine_name: str,
    *,
    timeout_ms: int,
    max_pages: int,
) -> str:
    """Locate one machine row on the live EGM table and screenshot that row only."""
    row = _find_machine_row_live(page, machine_name, timeout_ms=timeout_ms, max_pages=max_pages)
    if row is None:
        raise RuntimeError(f"machine not found on EGM page: {machine_name!r}")
    return _screenshot_egm_row_locator(page, row, timeout_ms=timeout_ms)


def capture_machine_operation_screenshot_on_page(
    page,
    machine_name: str,
    *,
    timeout_ms: int,
    max_pages: int,
) -> str:
    """Alias — prod batch uses table-row capture (no operation dialog)."""
    return capture_machine_row_screenshot_on_page(
        page, machine_name, timeout_ms=timeout_ms, max_pages=max_pages
    )


def _capture_prod_batch_screenshots_on_page(
    page,
    machines: list[dict],
    *,
    timeout_ms: int,
    max_pages: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    shots: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    by_env: dict[str, list[dict]] = {}
    for m in machines:
        b = _belongs_for_machine(m.get("belongs", ""))
        by_env.setdefault(b, []).append(m)

    for belongs, env_machines in by_env.items():
        if not _ensure_env_egm_page(page, belongs, timeout_ms=timeout_ms, max_pages=max_pages):
            for m in env_machines:
                name = _machine_display_name(m)
                errors.append(
                    {
                        "belongs": m.get("belongs", belongs),
                        "machine": name,
                        "error": "login failed",
                    }
                )
            continue

        specs = _machine_lookup_specs(env_machines)
        pending = list(specs)
        limit = _resolve_collect_page_limit(max_pages)
        _go_first_page(page, timeout_ms=timeout_ms, max_steps=limit)
        _wait_table_idle(page, timeout_ms)
        steps = 0
        safety = 0
        shot_names: set[tuple[str, str]] = set()

        while pending:
            safety += 1
            if safety > limit * max(len(specs), 1) + 50:
                break

            resolved: list[tuple[str, dict, str, str]] = []
            for spec in list(pending):
                name, m, kind, key = spec
                dedupe = (str(m.get("belongs") or belongs).upper(), name)
                if dedupe in shot_names:
                    if kind == "full":
                        resolved.append(spec)
                    continue
                rows = _find_all_rows_for_target(page, kind, key, timeout_ms=timeout_ms)
                if not rows:
                    continue
                try:
                    path = _screenshot_egm_row_locator(page, rows[0], timeout_ms=timeout_ms)
                    shots.append({"belongs": m.get("belongs", belongs), "machine": name, "path": path})
                    shot_names.add(dedupe)
                except Exception as exc:
                    logger.warning("prod-batch row screenshot failed for %s: %s", name, exc)
                    errors.append(
                        {
                            "belongs": m.get("belongs", belongs),
                            "machine": name,
                            "error": str(exc),
                        }
                    )
                if kind == "full":
                    resolved.append(spec)

            for spec in resolved:
                pending.remove(spec)

            if not pending:
                break
            if not _can_pagination_next(page) or steps >= limit:
                break
            _click_pagination_next(page, timeout_ms=timeout_ms)
            steps += 1
            _wait_table_idle(page, timeout_ms)

        for name, m, _kind, _key in pending:
            dedupe = (str(m.get("belongs") or belongs).upper(), name)
            if dedupe in shot_names:
                continue
            errors.append(
                {
                    "belongs": m.get("belongs", belongs),
                    "machine": name,
                    "error": "machine not found on EGM page",
                }
            )
    return shots, errors


def capture_prod_machine_screenshots(
    machines: list[dict],
    *,
    headless: bool | None = None,
    timeout_ms: int | None = None,
    max_pages: int | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Dedicated browser session — EGM status-table row PNG for each machine."""
    from playwright.sync_api import sync_playwright

    if not machines:
        return [], []

    timeout_ms = timeout_ms or _default_timeout_ms()
    if max_pages is None:
        max_pages = int(os.environ.get("SM_MACHINE_MAX_PAGES") or 0) or None
    hl = _smachine_resolve_headless(headless)
    if headless is None:
        hl = _smachine_resolve_headless(
            os.environ.get("SMACHINE_HEADLESS", "1").strip().lower() not in ("0", "false", "no")
        )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=hl)
        context = browser.new_context(
            viewport={"width": 1600, "height": 900},
            ignore_https_errors=True,
        )
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        try:
            return _capture_prod_batch_screenshots_on_page(
                page, machines, timeout_ms=timeout_ms, max_pages=max_pages
            )
        finally:
            try:
                context.close()
            except Exception:
                pass
            browser.close()


def _max_phase_retries() -> int:
    try:
        return max(1, int((os.environ.get("PROD_SET_MAX_PHASE_RETRIES") or "10").strip()))
    except ValueError:
        return 10


def _prod_batch_fast_mode() -> bool:
    """Shorter UI settles and batch pagination (default on). Set ``PROD_BATCH_FAST=0`` to disable."""
    return (os.environ.get("PROD_BATCH_FAST", "1").strip().lower() not in ("0", "false", "no", "off"))


def _fast_ms(ms: int) -> int:
    if not _prod_batch_fast_mode():
        return ms
    return max(40, int(ms * 0.35))


def _page_pause(page, ms: int) -> None:
    page.wait_for_timeout(_fast_ms(ms))


def _list_path() -> str:
    path = (os.environ.get("SM_MACHINE_PATH") or "/egm/egmStatusList").strip() or "/egm/egmStatusList"
    if not path.startswith("/"):
        path = "/" + path
    return path


def _machine_target(machine_name: str) -> tuple[str, str]:
    return _parse_target_line(machine_name)


def _egm_table_ready(page, timeout_ms: int) -> bool:
    try:
        page.wait_for_selector(".app-container .el-table, .filter-container .el-table", timeout=min(20_000, timeout_ms))
        _wait_table_idle(page, timeout_ms)
        return True
    except Exception:
        return False


def _login_egm_backend(page, base: str, user: str, pw: str, *, timeout_ms: int) -> None:
    path = _list_path()
    login = "/login"
    login_url = f"{base.rstrip('/')}{login}?redirect={quote(path, safe='')}"
    list_url = f"{base.rstrip('/')}{path}"

    page.goto(login_url, wait_until="domcontentloaded")
    _page_pause(page, 900)

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

    page.wait_for_timeout(_fast_ms(1800))
    if path not in (page.url or ""):
        page.goto(list_url, wait_until="domcontentloaded")
    if not _egm_table_ready(page, timeout_ms):
        raise RuntimeError("EGM status table did not load after login")


def _ensure_env_egm_page(page, belongs: str, *, timeout_ms: int, max_pages: int) -> bool:
    """Open the correct PROD backend EGM list once per environment (reuse same page)."""
    from checkcredit import _np_resolve_backend  # noqa: WPS433

    norm = _belongs_for_machine(belongs)
    if getattr(page, "_prod_set_belongs", None) == norm and _egm_table_ready(page, timeout_ms):
        return True

    site = _belongs_site_key(norm)
    try:
        synth = _site_synthetic_machine(site)
    except (SystemExit, ValueError) as e:
        logger.warning("prod-set: unknown site for belongs %r: %s", belongs, e)
        return False

    base, user, pw = _np_resolve_backend(synth)
    if not (base and user and pw):
        logger.warning("prod-set: missing credentials for belongs %r (site %r)", belongs, site)
        return False

    try:
        _login_egm_backend(page, base, user, pw, timeout_ms=timeout_ms)
        limit = _resolve_collect_page_limit(max_pages)
        _go_first_page(page, timeout_ms=timeout_ms, max_steps=limit)
        _wait_table_idle(page, timeout_ms)
        page._prod_set_belongs = norm  # type: ignore[attr-defined]
        return True
    except Exception as e:
        logger.warning("prod-set: login/navigation failed for %r: %s", belongs, e)
        return False


def _find_all_rows_for_target(page, kind: str, key: str, *, timeout_ms: int):
    rows = _table_body_rows(page)
    try:
        rows.first.wait_for(state="visible", timeout=min(15_000, timeout_ms))
    except Exception:
        pass
    matched = []
    n = rows.count()
    for i in range(n):
        row = rows.nth(i)
        txt = _row_match_text_for_target(row, timeout_ms=timeout_ms)
        if not txt:
            continue
        if _row_text_matches(kind, key, txt):
            matched.append(row)
    return matched


def _machine_lookup_specs(machines: list[dict]) -> list[tuple[str, dict, str, str]]:
    specs: list[tuple[str, dict, str, str]] = []
    for m in machines:
        name = _machine_display_name(m)
        if not name:
            continue
        kind, key = _machine_target(name)
        specs.append((name, m, kind, key))
    return specs


def _batch_select_machines_on_live_page(
    page,
    machines: list[dict],
    *,
    timeout_ms: int,
    max_pages: int,
) -> tuple[list[dict], list[dict]]:
    specs = _machine_lookup_specs(machines)
    if not specs:
        return [], []

    pending = list(specs)
    limit = _resolve_collect_page_limit(max_pages)
    _go_first_page(page, timeout_ms=timeout_ms, max_steps=limit)
    _wait_table_idle(page, timeout_ms)

    selected: list[dict] = []
    fail_list: list[dict] = []
    seen: set[tuple[str, str]] = set()
    steps = 0
    safety = 0

    while pending:
        safety += 1
        if safety > limit * max(len(specs), 1) + 50:
            break

        resolved: list[tuple[str, dict, str, str]] = []
        for spec in list(pending):
            name, m, kind, key = spec
            rows = _find_all_rows_for_target(page, kind, key, timeout_ms=timeout_ms)
            if not rows:
                continue
            dedupe = (str(m.get("belongs") or "").upper(), name)
            if dedupe not in seen:
                try:
                    _ensure_row_checkbox_checked(page, rows[0], timeout_ms=timeout_ms)
                    seen.add(dedupe)
                    selected.append(m)
                except Exception as exc:
                    fail_list.append(
                        {"belongs": m.get("belongs", ""), "machine": name, "error": str(exc)}
                    )
            if kind == "full":
                resolved.append(spec)

        for spec in resolved:
            pending.remove(spec)

        if not pending:
            break
        if not _can_pagination_next(page) or steps >= limit:
            break
        _click_pagination_next(page, timeout_ms=timeout_ms)
        steps += 1
        _wait_table_idle(page, timeout_ms)

    for name, m, _kind, _key in pending:
        if any(_machine_display_name(x) == name for x in selected):
            continue
        fail_list.append(
            {
                "belongs": m.get("belongs", ""),
                "machine": name,
                "error": "machine not found on EGM page",
            }
        )
    return selected, fail_list


def _batch_read_live_states(
    page,
    machines: list[dict],
    *,
    timeout_ms: int,
    max_pages: int,
) -> dict[str, dict[str, Any] | None]:
    specs = _machine_lookup_specs(machines)
    if not specs:
        return {}

    pending = list(specs)
    limit = _resolve_collect_page_limit(max_pages)
    _go_first_page(page, timeout_ms=timeout_ms, max_steps=limit)
    _wait_table_idle(page, timeout_ms)

    out: dict[str, dict[str, Any] | None] = {name: None for name, _m, _k, _key in specs}
    steps = 0
    safety = 0

    while pending:
        safety += 1
        if safety > limit * max(len(specs), 1) + 50:
            break

        resolved: list[tuple[str, dict, str, str]] = []
        for spec in list(pending):
            name, _m, kind, key = spec
            rows = _find_all_rows_for_target(page, kind, key, timeout_ms=timeout_ms)
            if not rows:
                continue
            try:
                mn, is_test, _gt, status, online = _row_report_fields(rows[0], timeout_ms=timeout_ms)
                out[name] = {
                    "name": mn,
                    "test": bool(is_test),
                    "status": (status or "").strip(),
                    "online": (online or "").strip(),
                }
            except Exception:
                out[name] = None
            if kind == "full":
                resolved.append(spec)

        for spec in resolved:
            pending.remove(spec)

        if not pending:
            break
        if not _can_pagination_next(page) or steps >= limit:
            break
        _click_pagination_next(page, timeout_ms=timeout_ms)
        steps += 1
        _wait_table_idle(page, timeout_ms)

    return out


def _find_machine_row_live(page, machine_name: str, *, timeout_ms: int, max_pages: int):
    """Locate a machine row by scanning the **current** EGM table (paginate if needed)."""
    kind, key = _machine_target(machine_name)
    limit = _resolve_collect_page_limit(max_pages)
    _go_first_page(page, timeout_ms=timeout_ms, max_steps=limit)
    _wait_table_idle(page, timeout_ms)

    steps = 0
    while True:
        rows = _find_all_rows_for_target(page, kind, key, timeout_ms=timeout_ms)
        if rows:
            return rows[0]
        if not _can_pagination_next(page) or steps >= limit:
            return None
        _click_pagination_next(page, timeout_ms=timeout_ms)
        steps += 1
        _wait_table_idle(page, timeout_ms)


def _select_machine_on_live_page(page, machine_name: str, *, timeout_ms: int, max_pages: int) -> bool:
    row = _find_machine_row_live(page, machine_name, timeout_ms=timeout_ms, max_pages=max_pages)
    if row is None:
        return False
    _ensure_row_checkbox_checked(page, row, timeout_ms=timeout_ms)
    return True


def _read_live_row_state(page, machine_name: str, *, timeout_ms: int, max_pages: int) -> dict[str, Any] | None:
    """Status / test mode from the headless page DOM only (never from webapp cache)."""
    row = _find_machine_row_live(page, machine_name, timeout_ms=timeout_ms, max_pages=max_pages)
    if row is None:
        return None
    mn, is_test, _game_type, status, online = _row_report_fields(row, timeout_ms=timeout_ms)
    return {
        "name": mn,
        "test": bool(is_test),
        "status": (status or "").strip(),
        "online": (online or "").strip(),
    }


def _status_is_maintenance(status_text: str) -> bool:
    su = (status_text or "").upper()
    return "MAINTAIN" in su or "METERCHECK" in su


def _verify_live_state(state: dict[str, Any], action: str) -> bool:
    is_maint = _status_is_maintenance(state.get("status", ""))
    is_test = bool(state.get("test"))
    if action == "set_maint":
        return is_maint
    if action == "set_test":
        return is_test
    if action == "set_both":
        return is_maint and is_test
    if action == "unset_maint":
        return not is_maint
    if action == "unset_test":
        return not is_test
    if action == "unset_both":
        return not is_maint and not is_test
    return True


def _verify_machine_live(
    page, machine_name: str, action: str, *, timeout_ms: int, max_pages: int
) -> bool:
    state = _read_live_row_state(page, machine_name, timeout_ms=timeout_ms, max_pages=max_pages)
    if state is None:
        return False
    return _verify_live_state(state, action)


def _refresh_egm_table(page, *, timeout_ms: int, max_pages: int) -> None:
    """Reload table data on the page we already have (stay on EGM list, do not use webapp API)."""
    scope = page.locator(".filter-container, .app-container").first
    refresh = scope.get_by_role("button", name=re.compile(r"^refresh$", re.I))
    if refresh.count():
        try:
            refresh.first.click(timeout=min(30_000, timeout_ms))
            _wait_table_idle(page, timeout_ms)
        except Exception:
            pass
    limit = _resolve_collect_page_limit(max_pages)
    _go_first_page(page, timeout_ms=timeout_ms, max_steps=limit)
    _wait_table_idle(page, timeout_ms)


def _batch_button_match_pattern(label: str) -> re.Pattern[str]:
    """Match toolbar labels with flexible whitespace (``BatchKick Out`` vs ``BatchKickOut``)."""
    parts = [p for p in re.split(r"\s+", (label or "").strip()) if p]
    if not parts:
        return re.compile(r"^$")
    return re.compile(r"\s+".join(re.escape(p) for p in parts), re.I)


def _locate_batch_toolbar_button(page, label: str):
    pat = _batch_button_match_pattern(label)
    for scope in (
        page.locator(".filter-container"),
        page.locator(".app-container"),
        page,
    ):
        btn = scope.locator("button").filter(has_text=pat).first
        if btn.count():
            return btn
    role_btn = page.get_by_role("button", name=pat).first
    if role_btn.count():
        return role_btn
    return page.locator("button").filter(has_text=pat).first


def _batch_toolbar_button_actionable(btn) -> bool:
    """True when the batch toolbar button can receive a click (not ``is-disabled``)."""
    try:
        if btn.count() == 0:
            return False
        cls = btn.get_attribute("class") or ""
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


def _wait_batch_toolbar_ready(page, label: str, *, timeout_ms: int, wait_ms: int = 12_000) -> bool:
    """After row checkbox selection, EGM may enable toolbar buttons after a short delay."""
    deadline = time.monotonic() + min(wait_ms / 1000.0, timeout_ms / 1000.0)
    while time.monotonic() < deadline:
        btn = _locate_batch_toolbar_button(page, label)
        if btn.count() and _batch_toolbar_button_actionable(btn):
            return True
        page.wait_for_timeout(_fast_ms(250))
    btn = _locate_batch_toolbar_button(page, label)
    return btn.count() > 0 and _batch_toolbar_button_actionable(btn)


def _click_batch_button(page, label: str, *, timeout_ms: int, force: bool = False) -> tuple[bool, str]:
    """
    Click a toolbar batch button. Tries a real click first (Occupy/Timeout rows are
    often clickable once selected). Only reports disabled if click fails and button
    still looks non-actionable; optional force click as last resort.
    """
    btn = _locate_batch_toolbar_button(page, label)
    if btn.count() == 0:
        return False, "not found"
    try:
        btn.click(timeout=min(30_000, timeout_ms))
        _page_pause(page, 400)
        return True, ""
    except Exception:
        if force:
            try:
                btn.click(force=True, timeout=min(30_000, timeout_ms))
                _page_pause(page, 400)
                return True, ""
            except Exception:
                try:
                    btn.evaluate("el => el.click()")
                    _page_pause(page, 400)
                    return True, ""
                except Exception:
                    pass
        if not _batch_toolbar_button_actionable(btn):
            return False, "disabled"
        return False, "click failed"


def _submit_batch_action(page, label: str, remark: str, *, timeout_ms: int) -> tuple[bool, str]:
    """Click batch toolbar button and Save when confirm dialog opens."""
    _wait_batch_toolbar_ready(page, label, timeout_ms=timeout_ms)
    clicked, why = _click_batch_button(page, label, timeout_ms=timeout_ms)
    if not clicked and why in ("disabled", "click failed"):
        clicked, why = _click_batch_button(page, label, timeout_ms=timeout_ms, force=True)
    if not clicked:
        return False, why

    _page_pause(page, 500)
    if _visible_confirm_layer(page) is not None:
        try:
            _click_save_confirm(page, remark, timeout_ms=timeout_ms)
            _wait_batch_done(page, timeout_ms=timeout_ms)
        except Exception as e:
            return False, f"confirm/save failed: {e}"
    else:
        _page_pause(page, 800)
        if _visible_confirm_layer(page) is not None:
            try:
                _click_save_confirm(page, remark, timeout_ms=timeout_ms)
                _wait_batch_done(page, timeout_ms=timeout_ms)
            except Exception as e:
                return False, f"confirm/save failed: {e}"
        else:
            _wait_batch_done(page, timeout_ms=timeout_ms)
    return True, ""


def _visible_confirm_layer(page):
    for sel in (
        ".el-dialog__wrapper:not([style*='display: none'])",
        ".el-message-box__wrapper:not([style*='display: none'])",
    ):
        loc = page.locator(sel).filter(has=page.locator(".el-dialog, .el-message-box")).last
        if loc.count():
            try:
                if loc.is_visible():
                    return loc
            except Exception:
                continue
    dlg = page.locator(".el-dialog__wrapper").filter(has=page.locator(".el-dialog")).last
    if dlg.count():
        try:
            if dlg.is_visible():
                return dlg
        except Exception:
            pass
    mbox = page.locator(".el-message-box__wrapper").last
    if mbox.count():
        try:
            if mbox.is_visible():
                return mbox
        except Exception:
            pass
    return None


def _dismiss_batch_confirm_cancel(page, *, timeout_ms: int) -> bool:
    """Close Warning / confirm UI with **Cancel** — never Save."""
    _page_pause(page, 350)
    layer = _visible_confirm_layer(page)
    if layer is None:
        try:
            page.wait_for_function(
                """() => {
                  const dlg = document.querySelector('.el-dialog__wrapper:not([style*="display: none"]) .el-dialog');
                  const mb = document.querySelector('.el-message-box__wrapper:not([style*="display: none"]) .el-message-box');
                  return !!(dlg && dlg.offsetParent) || !!(mb && mb.offsetParent);
                }""",
                timeout=min(8_000, timeout_ms),
            )
            layer = _visible_confirm_layer(page)
        except Exception:
            layer = None
    if layer is None:
        return False

    inner = layer.locator(".el-dialog, .el-message-box").first
    if inner.count() == 0:
        inner = layer

    for name_pat in (r"^cancel$", r"^close$", r"^no$"):
        btn = inner.get_by_role("button", name=re.compile(name_pat, re.I))
        if btn.count():
            try:
                btn.first.click(timeout=min(15_000, timeout_ms))
                _wait_table_idle(page, timeout_ms)
                return True
            except Exception:
                continue

    cancel = inner.locator("button").filter(has_text=re.compile(r"cancel|关闭", re.I)).first
    if cancel.count():
        try:
            cancel.click(timeout=min(15_000, timeout_ms))
            _wait_table_idle(page, timeout_ms)
            return True
        except Exception:
            pass

    # Never click Save / Confirm / OK / primary.
    return False


def _clear_table_row_selection(page, *, timeout_ms: int) -> None:
    """Uncheck every visible row checkbox on the current page."""
    rows = _table_body_rows(page)
    n = rows.count()
    for i in range(n):
        try:
            _ensure_row_checkbox_unchecked(page, rows.nth(i), timeout_ms=timeout_ms)
        except Exception:
            continue


def _expect_toolbar_enabled_with_selection(label: str, *, is_maint: bool, is_test: bool) -> bool:
    """
    After one row is selected, which toolbar buttons should be clickable?

    - BatchStart Using → only when row already in maintenance.
    - BatchTestCancel → only when row in test mode.
    - BatchMaintenance / BatchTest → only when not already in that state.
    """
    if label == "BatchStart Using":
        return is_maint
    if label == "BatchTestCancel":
        return is_test
    if label == "BatchMaintenance":
        return not is_maint
    if label == "BatchTest":
        return not is_test
    return False


def _pick_probe_row(page, *, timeout_ms: int) -> tuple[Any, dict[str, Any]]:
    """Prefer a normal/online row so BatchMaintenance/BatchTest are enabled."""
    rows = _table_body_rows(page)
    n = rows.count()
    fallback = None
    fallback_ctx: dict[str, Any] = {}
    for i in range(n):
        row = rows.nth(i)
        try:
            mn, is_test, _gt, status, online = _row_report_fields(row, timeout_ms=timeout_ms)
        except Exception:
            continue
        su = (status or "").upper()
        ou = (online or "").upper()
        is_maint = "MAINTAIN" in su or "METERCHECK" in su
        is_online = "ONLINE" in ou and "OFFLINE" not in ou
        is_normalish = "NORMAL" in su or "OCCUPY" in su
        ctx = {
            "machine": mn,
            "maintenance": is_maint,
            "test": bool(is_test),
            "status": status,
            "online": online,
        }
        if fallback is None:
            fallback, fallback_ctx = row, ctx
        if is_online and is_normalish and not is_maint:
            return row, ctx
    if fallback is not None:
        return fallback, fallback_ctx
    raise RuntimeError("no EGM rows")


def _probe_one_toolbar_button(
    page,
    label: str,
    *,
    timeout_ms: int,
    expect_disabled: bool,
    expect_enabled_with_selection: bool | None = None,
) -> dict[str, Any]:
    """Check disabled/enabled state; if enabled, click and Cancel (no Save)."""
    btn = _locate_batch_toolbar_button(page, label)
    found = btn.count() > 0
    actionable = _batch_toolbar_button_actionable(btn) if found else False
    disabled = not actionable if found else None

    out: dict[str, Any] = {
        "found": found,
        "disabled": disabled,
        "ok": False,
        "detail": "",
    }

    if not found:
        out["detail"] = "button not found in toolbar"
        return out

    if expect_disabled:
        out["ok"] = not actionable
        if actionable:
            out["detail"] = "expected disabled with no row selected, but button is clickable"
        else:
            out["detail"] = "disabled without selection (expected)"
        return out

    should_enable = True if expect_enabled_with_selection is None else expect_enabled_with_selection
    if not should_enable:
        out["ok"] = not actionable
        if actionable:
            out["detail"] = "enabled but not expected for this row state (UI may differ)"
        else:
            out["detail"] = "disabled (expected for this row — e.g. not in maintenance/test)"
        return out

    if not actionable:
        out["detail"] = "expected enabled after row selected, but button is disabled"
        return out

    if not _click_batch_button(page, label, timeout_ms=timeout_ms)[0]:
        out["detail"] = "click failed"
        return out

    cancelled = _dismiss_batch_confirm_cancel(page, timeout_ms=timeout_ms)
    if cancelled:
        out["ok"] = True
        out["detail"] = "clicked; Warning/confirm dismissed with Cancel (Save not used)"
        return out

    # Some actions may not open a dialog (harmless no-op UI); still count as click OK.
    out["ok"] = True
    out["detail"] = "clicked; no confirm dialog (Save not used)"
    return out


def probe_egm_batch_toolbar_buttons(
    page,
    belongs: str,
    *,
    timeout_ms: int = 120_000,
    max_pages: int | None = None,
) -> dict[str, Any]:
    """
    Dry-run every EGM toolbar batch button: no selection → must be disabled;
    one row selected → click → Cancel only (never Save).
    """
    result: dict[str, Any] = {
        "belongs": belongs,
        "sample_machine": "",
        "buttons": {},
        "error": None,
    }

    if not _ensure_env_egm_page(page, belongs, timeout_ms=timeout_ms, max_pages=max_pages):
        result["error"] = "login failed"
        return result

    rows = _table_body_rows(page)
    try:
        rows.first.wait_for(state="visible", timeout=min(20_000, timeout_ms))
    except Exception:
        pass
    if rows.count() == 0:
        result["error"] = "no EGM rows on first page"
        return result

    _clear_table_row_selection(page, timeout_ms=timeout_ms)

    for label in EGM_TOOLBAR_BATCH_BUTTONS:
        result["buttons"][label] = {
            "without_selection": _probe_one_toolbar_button(
                page, label, timeout_ms=timeout_ms, expect_disabled=True
            )
        }

    row, row_ctx = _pick_probe_row(page, timeout_ms=timeout_ms)
    result["sample_machine"] = row_ctx.get("machine") or "(probe row)"
    is_maint = bool(row_ctx.get("maintenance"))
    is_test = bool(row_ctx.get("test"))

    _ensure_row_checkbox_checked(page, row, timeout_ms=timeout_ms)

    for label in EGM_TOOLBAR_BATCH_BUTTONS:
        _ensure_row_checkbox_checked(page, row, timeout_ms=timeout_ms)
        expect_on = _expect_toolbar_enabled_with_selection(
            label, is_maint=is_maint, is_test=is_test
        )
        probe = _probe_one_toolbar_button(
            page,
            label,
            timeout_ms=timeout_ms,
            expect_disabled=False,
            expect_enabled_with_selection=expect_on,
        )
        result["buttons"][label]["with_selection"] = probe
        result["buttons"][label]["row_context"] = {**row_ctx, "expects_enabled": expect_on}
        _dismiss_batch_confirm_cancel(page, timeout_ms=timeout_ms)
        _wait_table_idle(page, timeout_ms)

    _clear_table_row_selection(page, timeout_ms=timeout_ms)
    return result


def run_egm_batch_button_probe(
    site_aliases: list[str] | None = None,
    *,
    headless: bool | None = None,
    timeout_ms: int = 120_000,
    max_pages: int | None = None,
) -> dict[str, Any]:
    """Login to each PROD backend and dry-run toolbar batch buttons (Cancel only, never Save)."""
    from playwright.sync_api import sync_playwright

    from smmachine import DEFAULT_WEBMACHINE_SITES, _dedupe_site_keys_by_resolved_backend

    if site_aliases:
        use = [s.strip().lower() for s in site_aliases if (s or "").strip()]
    else:
        raw_env = (os.environ.get("WEBMACHINE_SITES") or "").strip()
        if raw_env:
            use = [s.strip().lower() for s in raw_env.split(",") if s.strip()]
        else:
            use = list(DEFAULT_WEBMACHINE_SITES)

    use, skipped = _dedupe_site_keys_by_resolved_backend(use)
    hl = _smachine_resolve_headless(headless)
    if headless is None and _truthy_env("SM_MACHINE_HEADED"):
        hl = False

    report: dict[str, Any] = {"sites": {}, "skipped": skipped, "headless": hl}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=hl)
        context = browser.new_context(
            viewport={"width": 1600, "height": 900},
            ignore_https_errors=True,
        )
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        try:
            for sk in use:
                belongs = _SITE_ALIAS_BELONGS.get(sk, sk.upper())
                try:
                    synth = _site_synthetic_machine(sk)
                    from checkcredit import _np_resolve_backend  # noqa: WPS433

                    base, user, pw = _np_resolve_backend(synth)
                    if not (user and pw):
                        report["sites"][sk] = {
                            "belongs": belongs,
                            "error": f"missing credentials for site {sk!r}",
                        }
                        continue
                    page._prod_set_belongs = None  # type: ignore[attr-defined]
                    report["sites"][sk] = probe_egm_batch_toolbar_buttons(
                        page,
                        belongs,
                        timeout_ms=timeout_ms,
                        max_pages=max_pages,
                    )
                except Exception as e:
                    report["sites"][sk] = {"belongs": belongs, "error": str(e)}
        finally:
            try:
                context.close()
            except Exception:
                pass
            browser.close()

    return report


def _truthy_env(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def _click_save_confirm(page, remark: str, *, timeout_ms: int) -> None:
    _page_pause(page, 500)
    dlg = page.locator(".el-dialog__wrapper").filter(has=page.locator(".el-dialog")).last
    if dlg.count() == 0:
        dlg = page.locator(".el-dialog").last
    dlg.wait_for(state="visible", timeout=min(30_000, timeout_ms))
    inner = dlg.locator(".el-dialog").first if dlg.locator(".el-dialog").count() else dlg
    if remark:
        for sel in ("textarea", "input[type='text']"):
            ta = inner.locator(sel).first
            if ta.count() and ta.is_visible():
                ta.fill(remark)
                break
    for name_pat in (r"^save$", r"confirm", r"^ok$"):
        btn = inner.get_by_role("button", name=re.compile(name_pat, re.I))
        if btn.count():
            btn.first.click(timeout=min(30_000, timeout_ms))
            _wait_table_idle(page, timeout_ms)
            return
    inner.locator("button.el-button--primary").first.click(timeout=min(30_000, timeout_ms))
    _wait_table_idle(page, timeout_ms)


def _wait_batch_done(page, *, timeout_ms: int) -> None:
    deadline = time.monotonic() + min(timeout_ms / 1000.0, 120.0)
    while time.monotonic() < deadline:
        _wait_table_idle(page, min(15_000, timeout_ms))
        _page_pause(page, 350)
        try:
            busy = page.locator(".el-loading-mask").filter(has=page.locator(":visible")).count()
            if busy == 0:
                break
        except Exception:
            break
    _page_pause(page, 600)


def _process_env(
    page,
    belongs: str,
    machines: list[dict],
    action: str,
    remark: str,
    cancel_check: Callable[[], bool],
    manual_stop_check: Callable[[], bool],
    *,
    timeout_ms: int,
    max_pages: int,
) -> tuple[list[dict], list[dict]]:
    ok_list: list[dict] = []
    fail_list: list[dict] = []

    if cancel_check():
        return ok_list, fail_list

    if not _ensure_env_egm_page(page, belongs, timeout_ms=timeout_ms, max_pages=max_pages):
        for m in machines:
            fail_list.append(
                {
                    "belongs": m.get("belongs", belongs),
                    "machine": _machine_display_name(m),
                    "error": "login failed",
                }
            )
        return ok_list, fail_list

    buttons = ACTION_BUTTONS.get(action, [])
    if not buttons:
        return ok_list, fail_list

    return _process_env_batch(
        page,
        belongs,
        machines,
        action,
        remark,
        buttons,
        action,
        cancel_check,
        manual_stop_check,
        timeout_ms=timeout_ms,
        max_pages=max_pages,
        ok_list=ok_list,
        fail_list=fail_list,
    )


def _batch_disabled_error(
    page,
    btn: str,
    machines: list[dict],
    belongs: str,
    *,
    timeout_ms: int,
    max_pages: int,
) -> str:
    """Human-readable reason when a toolbar batch button stays disabled."""
    hints: list[str] = []
    try:
        live_states = _batch_read_live_states(
            page, machines, timeout_ms=timeout_ms, max_pages=max_pages
        )
    except Exception:
        live_states = {}
    for m in machines:
        name = _machine_display_name(m)
        if not name:
            continue
        live = live_states.get(name)
        if live is None:
            hints.append(f"{name}: not on current EGM table (or lost selection)")
            continue
        st = (live.get("status") or "").strip()
        ol = (live.get("online") or "").strip()
        su = st.upper()
        ou = ol.upper()
        is_maint = _status_is_maintenance(st)
        is_test = bool(live.get("test"))
        if btn == "BatchMaintenance" and is_maint:
            hints.append(f"{name}: already in maintenance (status={st!r})")
        elif btn == "BatchTest" and is_test:
            hints.append(f"{name}: already in test mode")
        elif "OCCUPY" in su or "TIMEOUT" in su:
            hints.append(
                f"{name}: status {st!r} — BatchMaintenance should be clickable when row is "
                f"selected; check checkbox selection (online={ol!r})"
            )
        elif "OFFLINE" in ou:
            hints.append(f"{name}: offline ({ol!r}) — batch maintenance usually blocked")
        elif btn == "BatchMaintenance":
            hints.append(f"{name}: BatchMaintenance disabled (status={st!r}, online={ol!r})")
        else:
            hints.append(f"{name}: {btn} disabled (status={st!r}, online={ol!r})")
    if hints:
        return f"button {btn} disabled — " + "; ".join(hints)
    return (
        f"button {btn} disabled (row state may already match, game running, offline, "
        "or no row selected)"
    )


def _process_env_batch(
    page,
    belongs: str,
    machines: list[dict],
    action: str,
    remark: str,
    buttons: list[str],
    verify_action: str,
    cancel_check: Callable[[], bool],
    manual_stop_check: Callable[[], bool],
    *,
    timeout_ms: int,
    max_pages: int,
    ok_list: list[dict],
    fail_list: list[dict],
) -> tuple[list[dict], list[dict]]:
    if cancel_check() or manual_stop_check():
        return ok_list, fail_list

    try:
        selected, select_fail = _batch_select_machines_on_live_page(
            page, machines, timeout_ms=timeout_ms, max_pages=max_pages
        )
        fail_list.extend(select_fail)
    except Exception as exc:
        for m in machines:
            fail_list.append(
                {
                    "belongs": m.get("belongs", belongs),
                    "machine": _machine_display_name(m),
                    "error": str(exc),
                }
            )
        return ok_list, fail_list

    if cancel_check() or manual_stop_check() or not selected:
        return ok_list, fail_list

    # Retry path: if live EGM already matches this phase, skip batch clicks.
    still_need: list[dict] = []
    try:
        live_states = _batch_read_live_states(
            page, selected, timeout_ms=timeout_ms, max_pages=max_pages
        )
    except Exception as exc:
        for m in selected:
            fail_list.append(
                {
                    "belongs": m.get("belongs", belongs),
                    "machine": _machine_display_name(m),
                    "error": str(exc),
                }
            )
        return ok_list, fail_list

    for m in selected:
        if cancel_check() or manual_stop_check():
            break
        name = _machine_display_name(m)
        if not name:
            continue
        live = live_states.get(name)
        if live and _verify_live_state(live, verify_action):
            ok_list.append({"belongs": m.get("belongs", belongs), "machine": name})
        elif live is None:
            fail_list.append(
                {
                    "belongs": m.get("belongs", belongs),
                    "machine": name,
                    "error": "machine not found on EGM page",
                }
            )
        else:
            still_need.append(m)

    if cancel_check() or manual_stop_check() or not still_need:
        return ok_list, fail_list

    _clear_table_row_selection(page, timeout_ms=timeout_ms)
    try:
        reselected, reselect_fail = _batch_select_machines_on_live_page(
            page, still_need, timeout_ms=timeout_ms, max_pages=max_pages
        )
        fail_list.extend(reselect_fail)
    except Exception as exc:
        for m in still_need:
            fail_list.append(
                {
                    "belongs": m.get("belongs", belongs),
                    "machine": _machine_display_name(m),
                    "error": str(exc),
                }
            )
        return ok_list, fail_list

    if cancel_check() or manual_stop_check() or not reselected:
        return ok_list, fail_list
    selected = reselected

    for btn in buttons:
        _wait_batch_toolbar_ready(page, btn, timeout_ms=timeout_ms)

    for btn in buttons:
        if cancel_check() or manual_stop_check():
            break
        clicked, why = _submit_batch_action(page, btn, remark, timeout_ms=timeout_ms)
        if not clicked:
            if why == "disabled":
                err = _batch_disabled_error(
                    page, btn, selected, belongs, timeout_ms=timeout_ms, max_pages=max_pages
                )
                err += " (click failed — ensure row checkbox is selected on EGM page)"
            elif why == "not found":
                err = f"button {btn} not found"
            else:
                err = why or f"button {btn} failed"
            for m in selected:
                fail_list.append(
                    {
                        "belongs": m.get("belongs", belongs),
                        "machine": _machine_display_name(m),
                        "error": err,
                    }
                )
            return ok_list, fail_list

    _refresh_egm_table(page, timeout_ms=timeout_ms, max_pages=max_pages)

    try:
        post_states = _batch_read_live_states(
            page, selected, timeout_ms=timeout_ms, max_pages=max_pages
        )
    except Exception as exc:
        for m in selected:
            fail_list.append(
                {
                    "belongs": m.get("belongs", belongs),
                    "machine": _machine_display_name(m),
                    "error": str(exc),
                }
            )
        return ok_list, fail_list

    for m in selected:
        if cancel_check() or manual_stop_check():
            break
        name = _machine_display_name(m)
        if not name:
            continue
        live = post_states.get(name)
        if live and _verify_live_state(live, verify_action):
            ok_list.append({"belongs": m.get("belongs", belongs), "machine": name})
        else:
            detail = ""
            if live:
                detail = f" (live status={live.get('status')!r}, test={live.get('test')})"
            fail_list.append(
                {
                    "belongs": m.get("belongs", belongs),
                    "machine": name,
                    "error": f"status not as expected on EGM page{detail}",
                }
            )

    return ok_list, fail_list


def _run_step_with_retries(
    page,
    belongs: str,
    targets: list[dict],
    parent_action: str,
    remark: str,
    step_verify: str,
    step_buttons: list[str],
    cancel_check: Callable[[], bool],
    manual_stop_check: Callable[[], bool],
    *,
    timeout_ms: int,
    max_pages: int,
    on_phase_retry: Optional[Callable[[str, int, list[dict]], None]] = None,
) -> tuple[bool, list[dict]]:
    """
    Run one batch step, recheck live EGM, retry only failures until all pass or max attempts.
    Returns (passed, remaining_failures).
    """
    pending = list(targets)
    max_r = _max_phase_retries()

    if not _ensure_env_egm_page(page, belongs, timeout_ms=timeout_ms, max_pages=max_pages):
        return False, [
            {
                "belongs": m.get("belongs", belongs),
                "machine": _machine_display_name(m),
                "error": "login failed",
            }
            for m in pending
        ]

    for attempt in range(1, max_r + 1):
        if cancel_check() or manual_stop_check():
            return False, pending

        ok_part: list[dict] = []
        fail_part: list[dict] = []
        ok_part, fail_part = _process_env_batch(
            page,
            belongs,
            pending,
            parent_action,
            remark,
            step_buttons,
            step_verify,
            cancel_check,
            manual_stop_check,
            timeout_ms=timeout_ms,
            max_pages=max_pages,
            ok_list=ok_part,
            fail_list=fail_part,
        )
        if not fail_part:
            return True, []
        if on_phase_retry:
            on_phase_retry(step_verify, attempt, fail_part)
        pending = fail_part

    return False, pending


def _run_single_action_env(
    page,
    belongs: str,
    machines: list[dict],
    action: str,
    remark: str,
    cancel_check: Callable[[], bool],
    manual_stop_check: Callable[[], bool],
    *,
    timeout_ms: int,
    max_pages: int,
    on_phase_retry: Optional[Callable[[str, int, list[dict]], None]] = None,
) -> tuple[list[dict], list[dict]]:
    """set_maint / set_test / unset_* — retry until live EGM confirms, then return ok/fail lists."""
    buttons = ACTION_BUTTONS.get(action, [])
    if not buttons:
        return [], list(machines)

    targets = list(machines)
    passed, still_fail = _run_step_with_retries(
        page,
        belongs,
        targets,
        action,
        remark,
        action,
        buttons,
        cancel_check,
        manual_stop_check,
        timeout_ms=timeout_ms,
        max_pages=max_pages,
        on_phase_retry=on_phase_retry,
    )
    if not passed:
        return [], still_fail

    return [
        {"belongs": m.get("belongs", belongs), "machine": _machine_display_name(m)}
        for m in targets
        if _machine_display_name(m)
    ], []


def _run_phased_env(
    page,
    belongs: str,
    machines: list[dict],
    parent_action: str,
    remark: str,
    cancel_check: Callable[[], bool],
    manual_stop_check: Callable[[], bool],
    *,
    timeout_ms: int,
    max_pages: int,
    on_phase_retry: Optional[Callable[[str, int, list[dict]], None]] = None,
) -> tuple[list[dict], list[dict]]:
    """
    For ``set_both`` / ``unset_both``: phase 1 (e.g. maintenance) → live recheck → retry failures
    until all pass; then phase 2 (test) the same way; then final combined verify on EGM page.
    """
    steps = PHASED_STEPS.get(parent_action, [])
    if not steps:
        return [], list(machines)

    targets = list(machines)
    all_ok: list[dict] = []
    all_fail: list[dict] = []

    for step_verify, step_buttons in steps:
        if cancel_check() or manual_stop_check():
            return all_ok, all_fail

        phase_passed, pending = _run_step_with_retries(
            page,
            belongs,
            targets,
            parent_action,
            remark,
            step_verify,
            step_buttons,
            cancel_check,
            manual_stop_check,
            timeout_ms=timeout_ms,
            max_pages=max_pages,
            on_phase_retry=on_phase_retry,
        )
        if not phase_passed:
            all_fail.extend(pending)
            return all_ok, all_fail

    _refresh_egm_table(page, timeout_ms=timeout_ms, max_pages=max_pages)
    try:
        final_states = _batch_read_live_states(
            page, targets, timeout_ms=timeout_ms, max_pages=max_pages
        )
    except Exception as exc:
        return all_ok, [
            {
                "belongs": m.get("belongs", belongs),
                "machine": _machine_display_name(m),
                "error": str(exc),
            }
            for m in targets
            if _machine_display_name(m)
        ]

    for m in targets:
        if cancel_check() or manual_stop_check():
            break
        name = _machine_display_name(m)
        if not name:
            continue
        live = final_states.get(name)
        if live and _verify_live_state(live, parent_action):
            all_ok.append({"belongs": m.get("belongs", belongs), "machine": name})
        else:
            detail = ""
            if live:
                detail = f" (live status={live.get('status')!r}, test={live.get('test')})"
            all_fail.append(
                {
                    "belongs": m.get("belongs", belongs),
                    "machine": name,
                    "error": f"final EGM check failed{detail}",
                }
            )

    return all_ok, all_fail


def run_prod_batch_job(
    action: str,
    machines: list[dict],
    remark: str = "",
    *,
    cancel_check: Optional[Callable[[], bool]] = None,
    manual_stop_check: Optional[Callable[[], bool]] = None,
    on_manual_stop: Optional[Callable[[dict], None]] = None,
    on_phase_retry: Optional[Callable[[str, int, list[dict]], None]] = None,
    capture_screenshots: bool | None = None,
) -> dict[str, Any]:
    """
    Run batch job grouped by environment. Returns summary with success/failed lists
    of {belongs, machine} dicts for Lark/UI.

    Verification always re-reads the Playwright EGM table (never webapp machine JSON).
    """
    from playwright.sync_api import sync_playwright

    if cancel_check is None:
        cancel_check = lambda: False
    if manual_stop_check is None:
        manual_stop_check = lambda: False

    by_env: dict[str, list[dict]] = {}
    for m in machines:
        b = _belongs_for_machine(m.get("belongs", ""))
        by_env.setdefault(b, []).append(m)

    all_ok: list[dict] = []
    all_fail: list[dict] = []
    shot_list: list[dict[str, Any]] = []
    shot_errs: list[dict[str, Any]] = []
    want_shots = capture_screenshots if capture_screenshots is not None else prod_batch_screenshots_enabled()

    timeout_ms = _default_timeout_ms()
    max_pages = int(os.environ.get("SM_MACHINE_MAX_PAGES") or 0) or None
    headless = _smachine_resolve_headless(
        os.environ.get("SMACHINE_HEADLESS", "1").strip().lower() not in ("0", "false", "no")
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={"width": 1600, "height": 900},
            ignore_https_errors=True,
        )
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        try:
            use_retry = action in AUTO_RETRY_ACTIONS

            for belongs, env_machines in by_env.items():
                if cancel_check():
                    break
                if action in PHASED_STEPS:
                    ok, fail = _run_phased_env(
                        page,
                        belongs,
                        env_machines,
                        action,
                        remark,
                        cancel_check,
                        manual_stop_check,
                        timeout_ms=timeout_ms,
                        max_pages=max_pages,
                        on_phase_retry=on_phase_retry,
                    )
                elif use_retry:
                    ok, fail = _run_single_action_env(
                        page,
                        belongs,
                        env_machines,
                        action,
                        remark,
                        cancel_check,
                        manual_stop_check,
                        timeout_ms=timeout_ms,
                        max_pages=max_pages,
                        on_phase_retry=on_phase_retry,
                    )
                else:
                    ok, fail = _process_env(
                        page,
                        belongs,
                        env_machines,
                        action,
                        remark,
                        cancel_check,
                        manual_stop_check,
                        timeout_ms=timeout_ms,
                        max_pages=max_pages,
                    )
                all_ok.extend(ok)
                all_fail.extend(fail)

            if want_shots and machines and not cancel_check() and not _prod_batch_fast_mode():
                try:
                    shot_list, shot_errs = _capture_prod_batch_screenshots_on_page(
                        page, machines, timeout_ms=timeout_ms, max_pages=max_pages
                    )
                except Exception as exc:
                    logger.exception("prod-batch screenshot batch failed")
                    shot_errs.append({"belongs": "", "machine": "", "error": str(exc)})
        finally:
            try:
                context.close()
            except Exception:
                pass
            browser.close()

    return {
        "action": action,
        "success": all_ok,
        "failed": all_fail,
        "ok": [f"{x['belongs']}::{x['machine']}" for x in all_ok],
        "failed_keys": [f"{x['belongs']}::{x['machine']}" for x in all_fail],
        "screenshots": shot_list,
        "screenshot_errors": shot_errs,
    }


def live_verify_prod_machines(
    action: str,
    machines: list[dict],
    *,
    headless: bool | None = None,
    timeout_ms: int | None = None,
    max_pages: int | None = None,
) -> dict[str, Any]:
    """
    Read-only live EGM check for each requested machine (no batch UI clicks).
    Returns the same ``success`` / ``failed`` shape as :func:`run_prod_batch_job`.
    """
    from playwright.sync_api import sync_playwright

    if max_pages is None:
        max_pages = int(os.environ.get("SM_MACHINE_MAX_PAGES") or 0) or None
    timeout_ms = timeout_ms or _default_timeout_ms()
    hl = _smachine_resolve_headless(headless)
    if headless is None:
        hl = _smachine_resolve_headless(
            os.environ.get("SMACHINE_HEADLESS", "1").strip().lower() not in ("0", "false", "no")
        )

    by_env: dict[str, list[dict]] = {}
    for m in machines:
        b = _belongs_for_machine(m.get("belongs", ""))
        by_env.setdefault(b, []).append(m)

    success: list[dict] = []
    failed: list[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=hl)
        context = browser.new_context(
            viewport={"width": 1600, "height": 900},
            ignore_https_errors=True,
        )
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        try:
            for belongs, env_machines in by_env.items():
                if not _ensure_env_egm_page(
                    page, belongs, timeout_ms=timeout_ms, max_pages=max_pages
                ):
                    for m in env_machines:
                        failed.append(
                            {
                                "belongs": m.get("belongs", belongs),
                                "machine": _machine_display_name(m),
                                "error": "login failed",
                            }
                        )
                    continue
                for m in env_machines:
                    name = _machine_display_name(m)
                    if not name:
                        continue
                    state = _read_live_row_state(
                        page, name, timeout_ms=timeout_ms, max_pages=max_pages
                    )
                    if state is None:
                        failed.append(
                            {
                                "belongs": m.get("belongs", belongs),
                                "machine": name,
                                "error": "machine not found on EGM page",
                            }
                        )
                        continue
                    if _verify_live_state(state, action):
                        success.append(
                            {
                                "belongs": m.get("belongs", belongs),
                                "machine": name,
                                "live": state,
                            }
                        )
                    else:
                        detail = (
                            f"live status={state.get('status')!r}, test={state.get('test')}"
                        )
                        failed.append(
                            {
                                "belongs": m.get("belongs", belongs),
                                "machine": name,
                                "error": detail,
                                "live": state,
                            }
                        )
        finally:
            try:
                context.close()
            except Exception:
                pass
            browser.close()

    return {
        "action": action,
        "success": success,
        "failed": failed,
        "ok": [f"{x['belongs']}::{x['machine']}" for x in success],
        "failed_keys": [f"{x['belongs']}::{x['machine']}" for x in failed],
        "live_check": True,
    }


run_prod_set_job = run_prod_batch_job
