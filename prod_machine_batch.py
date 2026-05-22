"""
Playwright automation for PROD machine batch maintenance/test on EGM status pages.

Recheck / verification always reads the **live** EGM table in the headless browser
(``smmachine`` row parsers). It never uses webapp JSON or ``webmachine_data.json``.
"""
from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Callable, Optional
from urllib.parse import quote

from smmachine import (
    _can_pagination_next,
    _click_pagination_next,
    _ensure_row_checkbox_checked,
    _find_row_for_target,
    _go_first_page,
    _parse_target_line,
    _resolve_collect_page_limit,
    _row_report_fields,
    _site_synthetic_machine,
    _smachine_resolve_headless,
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


def _max_phase_retries() -> int:
    try:
        return max(1, int((os.environ.get("PROD_SET_MAX_PHASE_RETRIES") or "10").strip()))
    except ValueError:
        return 10


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


def _find_machine_row_live(page, machine_name: str, *, timeout_ms: int, max_pages: int):
    """Locate a machine row by scanning the **current** EGM table (paginate if needed)."""
    kind, key = _machine_target(machine_name)
    limit = _resolve_collect_page_limit(max_pages)
    _go_first_page(page, timeout_ms=timeout_ms, max_steps=limit)
    _wait_table_idle(page, timeout_ms)

    steps = 0
    while True:
        row = _find_row_for_target(page, kind, key, timeout_ms)
        if row is not None:
            return row
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


def _click_batch_button(page, label: str, *, timeout_ms: int) -> bool:
    compact = re.sub(r"\s+", "", label or "")
    pat = re.compile(re.escape(compact), re.I)
    for scope in (
        page.locator(".filter-container"),
        page.locator(".app-container"),
        page,
    ):
        btn = scope.locator("button").filter(has_text=pat).first
        if btn.count():
            try:
                btn.click(timeout=min(30_000, timeout_ms))
                page.wait_for_timeout(400)
                return True
            except Exception:
                continue
    role_btn = page.get_by_role("button", name=re.compile(re.escape(label), re.I)).first
    if role_btn.count():
        try:
            role_btn.click(timeout=min(30_000, timeout_ms))
            page.wait_for_timeout(400)
            return True
        except Exception:
            pass
    return False


def _click_save_confirm(page, remark: str, *, timeout_ms: int) -> None:
    page.wait_for_timeout(500)
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
        page.wait_for_timeout(350)
        try:
            busy = page.locator(".el-loading-mask").filter(has=page.locator(":visible")).count()
            if busy == 0:
                break
        except Exception:
            break
    page.wait_for_timeout(600)


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
    selected: list[dict] = []
    for m in machines:
        if cancel_check() or manual_stop_check():
            break
        name = _machine_display_name(m)
        if not name:
            continue
        try:
            if not _select_machine_on_live_page(
                page, name, timeout_ms=timeout_ms, max_pages=max_pages
            ):
                fail_list.append(
                    {
                        "belongs": m.get("belongs", belongs),
                        "machine": name,
                        "error": "machine not found on EGM page",
                    }
                )
                continue
            selected.append(m)
        except Exception as e:
            fail_list.append(
                {"belongs": m.get("belongs", belongs), "machine": name, "error": str(e)}
            )

    if cancel_check() or manual_stop_check() or not selected:
        return ok_list, fail_list

    for btn in buttons:
        if cancel_check() or manual_stop_check():
            break
        if not _click_batch_button(page, btn, timeout_ms=timeout_ms):
            for m in selected:
                fail_list.append(
                    {
                        "belongs": m.get("belongs", belongs),
                        "machine": _machine_display_name(m),
                        "error": f"button {btn} not found",
                    }
                )
            return ok_list, fail_list
        _click_save_confirm(page, remark, timeout_ms=timeout_ms)
        _wait_batch_done(page, timeout_ms=timeout_ms)

    if cancel_check() or manual_stop_check():
        return ok_list, fail_list

    _refresh_egm_table(page, timeout_ms=timeout_ms, max_pages=max_pages)

    for m in selected:
        if cancel_check() or manual_stop_check():
            break
        name = _machine_display_name(m)
        if not name:
            continue
        try:
            if _verify_machine_live(
                page, name, verify_action, timeout_ms=timeout_ms, max_pages=max_pages
            ):
                ok_list.append({"belongs": m.get("belongs", belongs), "machine": name})
            else:
                live = _read_live_row_state(
                    page, name, timeout_ms=timeout_ms, max_pages=max_pages
                )
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
        except Exception as e:
            fail_list.append(
                {"belongs": m.get("belongs", belongs), "machine": name, "error": str(e)}
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

    all_ok: list[dict] = []
    all_fail: list[dict] = []
    _refresh_egm_table(page, timeout_ms=timeout_ms, max_pages=max_pages)
    for m in targets:
        if cancel_check() or manual_stop_check():
            break
        name = _machine_display_name(m)
        if not name:
            continue
        try:
            if _verify_machine_live(
                page, name, action, timeout_ms=timeout_ms, max_pages=max_pages
            ):
                all_ok.append({"belongs": m.get("belongs", belongs), "machine": name})
            else:
                live = _read_live_row_state(page, name, timeout_ms=timeout_ms, max_pages=max_pages)
                detail = ""
                if live:
                    detail = f" (live status={live.get('status')!r}, test={live.get('test')})"
                all_fail.append(
                    {
                        "belongs": m.get("belongs", belongs),
                        "machine": name,
                        "error": f"status not as expected on EGM page{detail}",
                    }
                )
        except Exception as e:
            all_fail.append(
                {"belongs": m.get("belongs", belongs), "machine": name, "error": str(e)}
            )
    return all_ok, all_fail


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
    for m in targets:
        if cancel_check() or manual_stop_check():
            break
        name = _machine_display_name(m)
        if not name:
            continue
        try:
            if _verify_machine_live(
                page, name, parent_action, timeout_ms=timeout_ms, max_pages=max_pages
            ):
                all_ok.append({"belongs": m.get("belongs", belongs), "machine": name})
            else:
                live = _read_live_row_state(page, name, timeout_ms=timeout_ms, max_pages=max_pages)
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
        except Exception as e:
            all_fail.append(
                {"belongs": m.get("belongs", belongs), "machine": name, "error": str(e)}
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
    }


run_prod_set_job = run_prod_batch_job
