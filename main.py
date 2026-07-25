import base64
import contextvars
import http
import json
import re
import sys
import threading
import uuid
from typing import Any, Optional
import requests
import time
import os
import mimetypes
from dotenv import load_dotenv

# Resolve imports from this repo regardless of process cwd (systemd, gunicorn, etc.)
_CHBOX_DIR = os.path.dirname(os.path.abspath(__file__))
if _CHBOX_DIR not in sys.path:
    sys.path.insert(0, _CHBOX_DIR)

# ``python main.py`` loads this file as ``__main__``. Lazy ``import main`` (e.g. jenkinsupdate)
# would otherwise execute module-level code again and start a second APScheduler → duplicate cron.
if __name__ == "__main__":
    sys.modules.setdefault("main", sys.modules["__main__"])

# Load .env from the project directory (works under systemd when CWD is not the app folder)
_ENV_PATH = os.path.join(_CHBOX_DIR, ".env")
load_dotenv(_ENV_PATH)


def _apply_warm_pool_env_from_dotenv() -> None:
    """Repo ``.env`` wins over systemd ``EnvironmentFile`` for Jenkins warm-pool keys."""
    if not os.path.isfile(_ENV_PATH):
        return
    keys = (
        "JU_WARM_POOL",
        "JU_WARM_ALLOW_COLD_FALLBACK",
        "JU_WARM_PREWARM_ON_STARTUP",
        "JENKINS_WARM_STARTUP_WAIT_SEC",
        "JENKINS_WARM_STARTUP_BLOCK",
    )
    try:
        from dotenv import dotenv_values

        vals = dotenv_values(_ENV_PATH)
    except Exception:
        return
    for key in keys:
        raw = vals.get(key)
        if raw is not None and str(raw).strip() != "":
            os.environ[key] = str(raw).strip()


_apply_warm_pool_env_from_dotenv()

from flask import Flask, request, jsonify, Response
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from collections import OrderedDict
import random

# Import command handlers from separate modules
from duty_list import search_duty
from holiday import get_today_date, format_holidays, holidays_this_month
from funny import get_miao, lucifer, dog, get_picture1_path, get_manchung_path
import fe_duty 
import bi_duty
import game 
import reminder 
import fpms_duty 
import ose_Duty
import pms_duty
import sre_Duty
import cpms_duty
import db_duty
import liveslot_duty
import ote_duty
import ft

import providerid

import nwr
import winford
import nch
import cp
import tbp
import tbr
import dhs
import mdr
import smmachine
import maintenancemachineagent

import maintenance
import emergency
import ecsre

import bot_help
import list_range

# amountloss / jenkinsupdate pull playwright — avoid top-level import so startup survives flaky browsers.

_jenkins_mod = None  # None = not loaded yet; False = import failed


def _get_jenkinsupdate():
    """Return jenkinsupdate module or None if import failed (logged once)."""
    global _jenkins_mod
    if _jenkins_mod is False:
        return None
    if _jenkins_mod is not None:
        return _jenkins_mod
    try:
        import jenkinsupdate as ju

        _jenkins_mod = ju
        return ju
    except Exception as e:
        print(f"[jenkinsupdate] lazy import failed (FPMS /jenkins flows disabled): {e!r}")
        _jenkins_mod = False
        return None


_DUTY_LEAVE_WFH_FOOTER_COMMANDS: dict[str, str] = {
    "/fpms": "fpms",
    "/ote": "ote",
    "/bi": "bi",
    "/fe": "fe",
    "/sre": "sre",
    "/db": "db",
    "/dba": "db",
    "/cpms": "cpms",
    "/pms": "pms",
    "/ft": "ft",
}


def _append_department_leave_wfh_footer(reply: str, command: str) -> str:
    """Append today's leave/WFH for department duty commands."""
    dept_key = _DUTY_LEAVE_WFH_FOOTER_COMMANDS.get((command or "").strip().lower())
    if not dept_key or not reply:
        return reply
    try:
        import leavewfh as lw

        footer = lw.format_department_leave_wfh_footer(
            dept_key,
            use_html="<b>" in reply,
        )
    except Exception as exc:
        print(f"⚠️ leave/WFH footer failed for {command}: {exc}")
        return reply
    if footer:
        return f"{reply}\n\n{footer}"
    return reply


def _send_month_attendance_card(chat_id: str, clean_text: str, mode: str) -> None:
    """Send /leave, /wfh, or /leavewfh month card; optional department arg e.g. ``/leave fpms``."""
    try:
        import leavewfh as _leavewfh
    except ImportError:
        import leave as _leavewfh  # type: ignore[no-redef]

    dept_key, dept_err = _leavewfh.parse_month_attendance_department(clean_text)
    if dept_err:
        send_message(chat_id, dept_err)
        return
    if mode == "leave":
        payload = _leavewfh.get_leave_month_payload(department_key=dept_key)
    elif mode == "wfh":
        payload = _leavewfh.get_wfh_month_payload(department_key=dept_key)
    else:
        payload = _leavewfh.get_leave_wfh_month_payload(department_key=dept_key)
    card = payload.get("lark_card")
    if isinstance(card, dict):
        resp = send_message(chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive")
        if resp.get("code") != 0:
            send_message(chat_id, payload.get("text") or "❌ Leave/WFH card failed.")
    else:
        send_message(chat_id, payload.get("text") or "❌ Could not load leave/WFH data.")


# ================= CONFIGURATION =================
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET") 
VERIFICATION_TOKEN = (os.getenv("VERIFICATION_TOKEN") or "").strip()
DUTY_CHAT_ID = os.getenv("DUTY_CHAT_ID")
app = Flask(__name__)
app.config.setdefault(
    "SECRET_KEY",
    (os.environ.get("WEBAPP_SECRET_KEY") or os.environ.get("APP_SECRET") or "change-me").strip() or "change-me",
)

if not VERIFICATION_TOKEN:
    print(
        "[lark] WARNING: VERIFICATION_TOKEN is unset/empty — Feishu/Lark POSTs return 403 "
        "(copy Verification Token from 开发者后台 → 事件与回调).",
        flush=True,
    )

RANDOM_EMOJI_CODES = [
    "GRINNING", "JOY", "WINK", "BLUSH", "YUM", "HEART_EYES", "KISSING_HEART", "SUNGLASSES",
    "THINKING_FACE", "HUGGING_FACE", "MONKEY_FACE", "DOG", "CAT", "FOX_FACE", "LION_FACE",
    "UNICORN_FACE", "EARTH_ASIA", "VOLCANO", "APPLE", "PIZZA", "BEER", "COFFEE", "BALLOON",
    "GIFT", "TICKET", "TROPHY"
]

ALL_EMOJI_CODES = [
    "GRINNING", "JOY", "WINK", "BLUSH", "YUM", "HEART_EYES", "KISSING_HEART", "SUNGLASSES",
    "THINKING_FACE", "HUGGING_FACE", "MONKEY_FACE", "DOG", "CAT", "FOX_FACE", "LION_FACE",
    "UNICORN_FACE", "EARTH_ASIA", "VOLCANO", "APPLE", "PIZZA", "BEER", "COFFEE", "BALLOON",
    "GIFT", "TICKET", "TROPHY"
]

# ================= ALL-DUTY SUMMARY AND CHECK =================
def get_all_duty_summary():
    lines = []
    lines.append("📋 **ALL DUTY SUMMARY FOR TODAY** 📋\n")
    lines.append("**【FPMS】**")
    lines.append(fpms_duty.get_fpms_today_duty())
    lines.append("")
    lines.append("**【PMS】**")
    lines.append(pms_duty.dutyNextDay())
    lines.append("")
    lines.append("**【BI】**")
    lines.append(bi_duty.get_bi_today_duty())
    lines.append("")
    lines.append("**【FE】**")
    lines.append(fe_duty.get_fe_next_three_duty())
    lines.append("")
    lines.append("**【CPMS】**")
    lines.append(cpms_duty.get_cpms_three_days_text())
    lines.append("")
    lines.append("**【SRE】**")
    lines.append(sre_Duty.get_sre_week_duty())
    lines.append("")
    lines.append("**【DB】**")
    lines.append(db_duty.get_three_weeks_summary())
    lines.append("")
    lines.append("**【Liveslot】**")
    lines.append(liveslot_duty.get_three_weeks_summary())
    lines.append("")
    lines.append("**【OTE】**")
    lines.append(ote_duty.get_three_weeks_summary())
    lines.append("")
    lines.append("**【OSE】**")
    lines.append(ose_Duty.get_ose_today_duty())
    lines.append("")
    return "\n".join(lines).strip()

def get_all_duty_check(month=None, year=None):
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month
    lines = []
    lines.append(f"🔍 **DUTY MISSING REPORT – {datetime(year, month, 1).strftime('%B %Y')}** 🔍\n")
    lines.append("**【FPMS】**")
    lines.append(fpms_duty.fpms_check(month=month, year=year))
    lines.append("")
    lines.append("**【PMS】**")
    lines.append(pms_duty.pmsCheck(month=month, year=year))
    lines.append("")
    lines.append("**【BI】**")
    lines.append(bi_duty.bi_check(month=month, year=year))
    lines.append("")
    lines.append("**【FE】**")
    lines.append(fe_duty.fe_check(month=month, year=year))
    lines.append("")
    lines.append("**【CPMS】**")
    lines.append(cpms_duty.cpms_check(month=month, year=year))
    lines.append("")
    lines.append("**【SRE】**")
    lines.append(sre_Duty.sre_check(month=month, year=year))
    lines.append("")
    lines.append("**【DB】**")
    lines.append(db_duty.db_check(month=month, year=year))
    lines.append("")
    lines.append("**【Liveslot】**")
    lines.append(liveslot_duty.liveslot_check(month=month, year=year))
    lines.append("")
    lines.append("**【OTE】**")
    lines.append(ote_duty.ote_check(month=month, year=year))
    lines.append("")
    return "\n".join(lines).strip()


def _dispatch_daily_duty_reply(cmd: str) -> Optional[str]:
    """Return today's duty text for a single department slash command, or ``None``."""
    c = (cmd or "").strip().lower()
    if c == "/ose":
        return ose_Duty.get_ose_today_duty()
    if c == "/fpms":
        return fpms_duty.get_fpms_today_duty()
    if c == "/pms":
        return pms_duty.dutyNextDay()
    if c == "/bi":
        return bi_duty.get_bi_today_duty()
    if c == "/fe":
        return fe_duty.get_fe_next_three_duty()
    if c == "/cpms":
        return cpms_duty.get_cpms_three_days_text()
    if c == "/sre":
        return sre_Duty.get_sre_week_duty()
    if c in ("/db", "/dba"):
        return db_duty.get_three_weeks_summary()
    if c == "/liveslot":
        return liveslot_duty.get_three_weeks_summary()
    if c == "/ote":
        return ote_duty.get_three_weeks_summary()
    if c == "/ft":
        return ft.get_ft_three_days()
    return None


def _build_multi_duty_reply(commands: list[str]) -> Optional[str]:
    """Combine several department duty blocks into one message."""
    blocks: list[str] = []
    for cmd in commands:
        body = _dispatch_daily_duty_reply(cmd)
        if not body:
            continue
        label = cmd.strip().upper().lstrip("/")
        blocks.append(f"**【{label}】**\n{body}")
    if not blocks:
        return None
    return "\n\n".join(blocks).strip()


def _send_duty_card(dept: str, body_text: str, chat_id: str) -> bool:
    """Send a department's duty text as a styled Lark message card.

    Falls back to a plain-text message if the card can't be built or the
    interactive send is rejected. Returns ``True`` when a card was delivered.
    """
    try:
        import dutyai as _dutyai

        payload = _dutyai.build_text_card(dept, body_text)
        card = payload.get("lark_card") if isinstance(payload, dict) else None
        if isinstance(card, dict):
            resp = send_message(
                chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive"
            )
            if not (isinstance(resp, dict) and resp.get("code") not in (0, None)):
                return True
    except Exception as exc:
        print(f"⚠️ duty card send failed for {dept}: {exc!r}", flush=True)
    send_message(chat_id, body_text)
    return False


def _send_payload_card(payload: dict, chat_id: str) -> bool:
    """Send a pre-built ``{"text", "lark_card"}`` payload as an interactive card.

    Falls back to the plain-text version if the card is missing or the
    interactive send is rejected. Returns ``True`` when a card was delivered.
    """
    try:
        card = payload.get("lark_card") if isinstance(payload, dict) else None
        if isinstance(card, dict):
            resp = send_message(
                chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive"
            )
            if not (isinstance(resp, dict) and resp.get("code") not in (0, None)):
                return True
    except Exception as exc:
        print(f"⚠️ payload card send failed: {exc!r}", flush=True)
    if isinstance(payload, dict) and payload.get("text"):
        send_message(chat_id, payload["text"])
    return False


def display_all_duty():
    summary = get_all_duty_summary()
    send_message(DUTY_CHAT_ID, summary)
    print("✅ Sent all-duty summary to", DUTY_CHAT_ID)

def monthly_duty_check():
    now = datetime.now()
    month = now.month
    year = now.year
    report = get_all_duty_check(month=month, year=year)
    send_message(DUTY_CHAT_ID, report)
    print(f"✅ Sent monthly duty check for {year}-{month:02d} to {DUTY_CHAT_ID}")

# ================= Amount Loss =================

AMOUNT_LOSS_MAX_ATTEMPTS = 2
AMOUNT_LOSS_RETRY_NOTICE = "Error occurred... Auto retry Please wait..."


def run_amountloss_check(chat_id, date_str=None, *, scheduled_9am=False):
    """在后台线程中执行 amount loss 检查，并将结果发送到指定 chat_id（失败自动重跑一轮）"""
    try:
        from amountloss import amount_loss_9am_enabled, fetch_fpms_data
    except ImportError as e:
        send_message(
            chat_id,
            "❌ 无法加载 FPMS 抓取模块（fetch_fpms_data）。"
            f" 请把与开发环境一致的 fpms_fetcher.py 部署到服务器，并安装 playwright。\n{str(e)}",
        )
        return

    if scheduled_9am and not amount_loss_9am_enabled():
        print(
            "[Amount Loss] 9:00 display/sheet fill skipped (temporarily disabled; AMOUNT_LOSS_9AM_ENABLED=1 to restore)",
            flush=True,
        )
        return

    for attempt in range(1, AMOUNT_LOSS_MAX_ATTEMPTS + 1):
        try:
            result = fetch_fpms_data(
                headless=True,
                target_date_str=date_str,
                filterdata=True,
                checklog=True,
                scheduled_9am=scheduled_9am,
            )
            if isinstance(result, dict) and result.get("lark_card"):
                sync_note = str(result.get("sync_note") or "").strip()
                if sync_note:
                    send_message(chat_id, sync_note)
                card_json = json.dumps(result["lark_card"])
                resp = send_message(chat_id, card_json, msg_type="interactive")
                if resp.get("code") != 0:
                    send_message(chat_id, result.get("text") or str(result))
                tsv_all = (result.get("sheet_tsv_all") or "").strip()
                tsv_game = (result.get("sheet_tsv_game") or "").strip()
                if tsv_all:
                    send_message(
                        chat_id,
                        "📋 Copy for Sheet — python3 amountloss.py --getdata\n```text\n" + tsv_all + "\n```",
                    )
                if tsv_game:
                    send_message(
                        chat_id,
                        "📋 Copy for Sheet — By Game\n```text\n" + tsv_game + "\n```",
                    )
            else:
                send_message(chat_id, result if isinstance(result, str) else str(result))
            return
        except Exception as e:
            if attempt < AMOUNT_LOSS_MAX_ATTEMPTS:
                send_message(chat_id, AMOUNT_LOSS_RETRY_NOTICE)
                print(f"[Amount Loss] attempt {attempt} failed: {e!r}, auto-retrying...")
            else:
                send_message(chat_id, f"❌ Amount Loss 检查失败: {str(e)}")
                print(f"[Amount Loss] failed after {AMOUNT_LOSS_MAX_ATTEMPTS} attempts: {e!r}")


def run_smsfail_check(chat_id):
    """Background: SMS gateway OTP log scrape (otpsmslog.run_otp_login), send English summary to chat."""
    try:
        from otpsmslog import run_otp_login
    except ImportError as e:
        send_message(
            chat_id,
            f"❌ Cannot load otpsmslog (install playwright, etc.): {e}",
        )
        return
    try:
        result = run_otp_login(headless=True)
        send_message(chat_id, result)
    except Exception as e:
        send_message(chat_id, f"❌ SMS OTP log check failed: {str(e)}")
        print(f"[SMS fail] error: {e!r}")


def run_smscheckplayer_check(chat_id, player_id: str):
    """Background: OTP log for one or more players (Status/Provider left blank). Date range: today 00:00—now. At most the 3 newest rows per player with a summary of Status/Provider mix. Same browser session; only Player ID changes between searches."""
    try:
        from otpsmslog import parse_player_ids, run_otp_login
    except ImportError as e:
        send_message(
            chat_id,
            f"❌ Cannot load otpsmslog (install playwright, etc.): {e}",
        )
        return
    raw = (player_id or "").strip()
    if not raw:
        send_message(
            chat_id,
            "❌ Usage: `/smscheckplayer <player_id(s)>` — one or many (today 00:00—now, up to 3 newest logs each), e.g. `/smscheckplayer 127317237` or `/smscheckplayer 7052472, 1069954565, 1040662396`",
        )
        return
    if not parse_player_ids(raw):
        send_message(
            chat_id,
            "❌ No valid player IDs after parsing. Use commas, spaces, or newlines between IDs.",
        )
        return
    try:
        result = run_otp_login(headless=True, player_id=raw)
        if isinstance(result, dict) and result.get("lark_card"):
            card_json = json.dumps(result["lark_card"])
            resp = send_message(chat_id, card_json, msg_type="interactive")
            if resp.get("code") != 0:
                send_message(chat_id, result.get("text") or str(result))
        else:
            send_message(
                chat_id,
                result if isinstance(result, str) else (result.get("text") or str(result)),
            )
    except Exception as e:
        send_message(chat_id, f"❌ SMS player OTP log check failed: {str(e)}")
        print(f"[SMS check player] error: {e!r}")


def run_checkcredit_finderror(
    chat_id,
    machine_query: str,
    date_str: str,
    mode: str = "default",
    navigator_logic_log_basename: Optional[str] = None,
    thread_root_message_id: Optional[str] = None,
):
    """Background: same as checkcredit + `--date`. Uses OSS HTTP by default (see checkcredit_use_oss_source)."""
    thread_root = (thread_root_message_id or _get_checkcredit_thread_root(chat_id) or "").strip() or None
    if thread_root:
        _set_checkcredit_thread_root(chat_id, thread_root)

    # R1: warm the Third-Http browser login for this machine's backend now, so it overlaps
    # the log read below — the later Detail screenshot (checkcredit/machineerror) then reuses
    # the ready page instead of launching+logging-in on the critical path.
    _prewarm_third_http_for_machine(machine_query)

    def _cc_send(text, **kwargs):
        return _checkcredit_send(chat_id, text, thread_root=thread_root, **kwargs)

    try:
        import checkcredit
    except ImportError as e:
        _cc_send(f"❌ Cannot load checkcredit module: {e}")
        return
    try:
        td = datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
        use_oss = checkcredit.checkcredit_use_oss_source()
        out = checkcredit.run_finderror(
            str(machine_query).strip(),
            target_date=td,
            timeout_ms=max(15_000, 90_000),
            base=checkcredit.DEFAULT_BASE,
            user=checkcredit.DEFAULT_USER,
            pw=checkcredit.DEFAULT_PASS,
            source="oss" if use_oss else "navigator",
            navigator_logic_log_basename=navigator_logic_log_basename,
        )
        text = (out.get("text") or "").strip()
        np = out.get("np_followup")
        preview_img_path = None
        preview_img_key = ""
        preview_img_err = ""
        preview_img_attempted = False
        error_ctx_paths: list[str] = []
        machineerror_fb: list[str] = []
        if isinstance(np, dict):
            try:
                md = str(np.get("machine_display") or "").strip() or None
                ms = str(np.get("machine_match_substr") or "").strip() or None
                cap = getattr(checkcredit, "screenshot_egm_status_window", None)
                if callable(cap) and md:
                    preview_img_attempted = True
                    preview_img_path = cap(
                        machine_display=md,
                        machine_substr=ms,
                        timeout_ms=120_000,
                        headed=False,
                    )
                    preview_img_key = upload_image_lark(preview_img_path) or ""
                    if not preview_img_key:
                        preview_img_err = "upload image failed"
                        print("[checkcredit] EGM preview screenshot upload failed", flush=True)
                if callable(getattr(checkcredit, "build_np_choice_lark_card", None)):
                    np_choices = np.get("np_choices") or []
                    intro_line = ""
                    extra_md = ""
                    extra_error_images: list[dict[str, str]] = []
                    if str(mode or "").strip().lower() == "error_only":
                        np_choices = np.get("np_choices_error_only") or []
                        intro_line = "Found players error"
                        extra_md = str(np.get("machineerror_context_md") or "")
                        merged_rows = out.get("merged_players") or []
                        pick_err = getattr(checkcredit, "select_top2_error_players", None)
                        build_ctx = getattr(checkcredit, "build_error_context_screenshots", None)
                        fb_ctx = getattr(checkcredit, "format_error_context_text_fallback", None)
                        if callable(pick_err) and callable(build_ctx):
                            err_rows = pick_err(merged_rows) or []
                            for rr in err_rows[:2]:
                                if not (rr.get("errors") or []):
                                    continue
                                ctx_items = build_ctx(rr, max_errors=6, lines_before_after=4) or []
                                row_got_img = False
                                for ci in ctx_items:
                                    pth = str(ci.get("path") or "").strip()
                                    if not pth:
                                        continue
                                    error_ctx_paths.append(pth)
                                    ik = upload_image_lark(pth) or ""
                                    if not ik:
                                        try:
                                            sz = os.path.getsize(pth)
                                        except OSError:
                                            sz = -1
                                        print(
                                            f"[checkcredit] error-context upload failed: path={pth} size={sz}",
                                            flush=True,
                                        )
                                        continue
                                    row_got_img = True
                                    extra_error_images.append(
                                        {
                                            "img_key": ik,
                                            "title": str(ci.get("title") or "Error context screenshot"),
                                        }
                                    )
                                if not row_got_img and callable(fb_ctx):
                                    chunk = fb_ctx(rr, max_errors=6)
                                    if chunk:
                                        machineerror_fb.append(chunk)
                    else:
                        intro_line = str(np.get("np_choice_intro") or "").strip()
                    same_last_line = ""
                    if str(mode or "").strip().lower() != "error_only":
                        same_last_line = str(np.get("same_last_line") or "")
                    np["np_choices"] = np_choices
                    out["lark_card_candidates"] = checkcredit.build_np_choice_lark_card(
                        np_choices,
                        target_date_iso=str(np.get("target_date") or ""),
                        machine_display=str(np.get("machine_display") or ""),
                        third_http_backend=str(np.get("third_http_backend") or "NP"),
                        image_key=preview_img_key,
                        intro_line=intro_line,
                        same_last_line=same_last_line,
                        extra_md=extra_md,
                        extra_error_images=extra_error_images,
                        navigator_same_day_multi_log=bool(np.get("navigator_same_day_multi_log")),
                    )
            except Exception as e:
                preview_img_err = str(e)
                print(f"[checkcredit] EGM preview screenshot failed: {e!r}", flush=True)
            finally:
                if preview_img_path and os.path.isfile(preview_img_path):
                    try:
                        os.unlink(preview_img_path)
                    except OSError:
                        pass
                for pth in error_ctx_paths:
                    if pth and os.path.isfile(pth):
                        try:
                            os.unlink(pth)
                        except OSError:
                            pass
            if preview_img_attempted and not preview_img_key:
                msg = (
                    f"⚠️ EGM preview screenshot unavailable: {preview_img_err}"
                    if preview_img_err
                    else "⚠️ EGM preview screenshot unavailable."
                )
                _cc_send(msg)
        card = out.get("lark_card_candidates")
        if isinstance(card, dict):
            card_json = json.dumps(card)
            resp = _cc_send(card_json, msg_type="interactive")
            if resp.get("code") != 0:
                _cc_send(text if text else "(no output)")
            if machineerror_fb and str(mode or "").strip().lower() == "error_only":
                _cc_send(
                    "⚠️ Error log images unavailable (PNG render or Lark upload failed). Text context:\n\n"
                    + "\n\n".join(machineerror_fb),
                )
        else:
            _cc_send(text if text else "(no output)")

        if isinstance(np, dict):
            _set_checkcredit_np_pending(chat_id, np, thread_root=thread_root)
    except Exception as e:
        cmd = "machineerror" if str(mode or "").strip().lower() == "error_only" else "checkcredit"
        _cc_send(f"❌ {cmd} failed: {e}")
        print(f"[{cmd}] error: {e!r}")


def _prewarm_third_http_for_machine(machine_query: str) -> None:
    """R1: start the Third-Http warm browser login for this machine's backend NOW, so it
    overlaps phase A (the OSS log read) instead of paying launch+login later on the critical
    path. Non-blocking (queues a prewarm task on the per-tag worker); safe no-op when the pool
    is disabled, credentials are missing, or the browser is already warm (login fast-paths)."""
    try:
        from third_http_warm_pool import third_http_warm_pool, third_http_warm_pool_enabled

        if not third_http_warm_pool_enabled():
            return
        import checkcredit as _cc

        tag = _cc._np_log_backend_tag(str(machine_query).strip())
        if not tag or not _cc._np_backend_has_credentials(tag):
            return
        third_http_warm_pool().prewarm([tag])
        print(
            f"[third-http-warm] prewarm {tag} submitted (overlaps log read for {machine_query!r})",
            flush=True,
        )
    except Exception as ex:
        print(f"[third-http-warm] prewarm skip for {machine_query!r}: {ex!r}", flush=True)


def _send_machine_lookup_card(chat_id: str, text: str, *, title: str) -> None:
    """Send a machine-lookup result as a TRTC-parsed Lark card; fall back to raw text
    when there's nothing card-worthy (error/usage) or the interactive send is rejected."""
    card = None
    try:
        import machine_card

        card = machine_card.build_card_from_text(text, title=title)
    except Exception as ex:
        print(f"[machine-card] build failed: {ex!r}", flush=True)
    if card:
        try:
            resp = send_message(chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive")
            if isinstance(resp, dict) and resp.get("code") in (0, None):
                return
            print(f"[machine-card] interactive rejected: {resp!r}", flush=True)
        except Exception as ex:
            print(f"[machine-card] send failed: {ex!r}", flush=True)
    send_message(chat_id, text)


def _machine_query_after_prefix(clean_text: str, prefix: str) -> str:
    """Text after a machine command prefix, accepting both '/nwr 2005' and '/nwr2005'."""
    return clean_text[len(prefix):].strip()


def run_check_machine_log_job(
    chat_id: str,
    machine_query: str,
    date_str: str,
    thread_root_message_id: Optional[str] = None,
    *,
    stuck_credit: bool = False,
) -> None:
    """OSS/LogNavigator logic log → threaded Lark card + AI summary (+ Third Http when applicable)."""
    thread_root = (thread_root_message_id or _get_checkcredit_thread_root(chat_id) or "").strip() or None
    if thread_root:
        _set_checkcredit_thread_root(chat_id, thread_root)

    # R1: kick the Third-Http browser login off concurrently with the log read below, so a
    # cold/slept browser finishes authenticating while OSS fetch runs — the screenshot step
    # then reuses the ready page instead of launching+logging-in on the critical path.
    _prewarm_third_http_for_machine(machine_query)

    def _cml_send(text, **kwargs):
        return _checkcredit_send(chat_id, text, thread_root=thread_root, **kwargs)

    try:
        import checkmachinelog
    except ImportError as e:
        _cml_send(f"❌ Cannot load checkmachinelog module: {e}")
        return
    try:
        td = datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
        out = checkmachinelog.run_check_machine_log(
            str(machine_query).strip(),
            target_date=td,
            stuck_credit=stuck_credit,
        )
        card = out.get("lark_card")
        if isinstance(card, dict):
            resp = _cml_send(json.dumps(card, ensure_ascii=False), msg_type="interactive")
            if resp.get("code") != 0:
                text = (out.get("text") or "").strip()
                if text:
                    _cml_send(text)
                else:
                    _cml_send(f"❌ {'stuck credit' if stuck_credit else 'checkmachinelog'} card failed: {resp}")
        else:
            text = (out.get("text") or "").strip()
            if text:
                _cml_send(text)
            else:
                _cml_send(f"✅ {'stuck credit' if stuck_credit else 'checkmachinelog'} finished (no output).")

        pick = out.get("third_http_followup")
        if isinstance(pick, dict) and (pick.get("user_id") or "").strip():
            be = str(pick.get("third_http_backend") or "NP").strip().upper()
            uid = str(pick["user_id"]).strip()
            cr = pick.get("credit_value")
            cr_s = str(cr) if cr is not None else "n/a"
            ts = str(pick.get("time_short") or "").strip()
            md = str(pick.get("machine_display") or machine_query).strip()
            if stuck_credit:
                _cml_send(
                    f"📋 **Stuck credit** on `{md}` — last player **`{uid}`** (credit `{cr_s}` @ `{ts}`).\n"
                    f"**Checking Third Http ({be})** — did the player **transfer out credit**?\n\n"
                    f"卡机额度 — 正在查 **Third Http ({be})** 玩家 **`{uid}`** 是否已成功转出…"
                )
                success_caption = (
                    f"✅ **Third Http ({be})** — player `{uid}` **transferred out credit** successfully "
                    f"(Detail matches log amount `{cr_s}` @ `{ts}`).\n"
                    f"✅ Third Http 有匹配记录 — 玩家 **`{uid}`** 额度应已成功转出（卡机可清）。"
                )
            else:
                err_p = str(pick.get("error_player_id") or "").strip()
                kind = str(pick.get("verify_kind") or "").strip()
                if kind == "transfer_out" and err_p and err_p != uid:
                    _cml_send(
                        f"📋 Log **error** player `{err_p}` · card **transfer-out** "
                        f"`{cr_s}` @ `{ts}` → player **`{uid}`**.\n"
                        f"**Checking Third Http ({be})** for **`{uid}`** (not error player).\n\n"
                        f"日志 error 玩家 `{err_p}` ≠ 转出玩家 **`{uid}`** — 查 Third Http 转出记录…"
                    )
                    success_caption = (
                        f"✅ **Third Http ({be})** — player **`{uid}`** **transferred out credit** "
                        f"(Detail `{cr_s}` @ `{ts}`, machine `{md}`).\n"
                        f"✅ Third Http — 玩家 **`{uid}`** 转出成功（非 error 玩家 `{err_p}`）。"
                    )
                else:
                    _cml_send(
                        f"📋 Log shows an **error** for player `{uid}` (credit `{cr_s}` @ `{ts}`).\n"
                        f"**Now checking Third Http ({be})** — did the player **transfer out credit** successfully?\n\n"
                        f"日志有 error — 正在查 **Third Http ({be})** 是否已成功转出额度…"
                    )
                    success_caption = (
                        f"✅ **Third Http ({be})** — player `{uid}` **transferred out credit** successfully "
                        f"(Detail matches log amount `{cr_s}` @ `{ts}`).\n"
                        f"✅ Third Http 有匹配记录 — 玩家 **`{uid}`** 额度应已成功转出。"
                    )
            _np_run_screenshot_worker(
                chat_id,
                uid,
                str(pick.get("target_date_iso") or date_str).strip(),
                ts,
                machine_substr=pick.get("machine_match_substr"),
                expected_credit=cr if isinstance(cr, (int, float)) else None,
                machine_display=str(pick.get("machine_display") or "").strip() or None,
                thread_root=thread_root,
                success_caption=success_caption,
                time_short_candidates=pick.get("time_short_candidates"),
            )
    except Exception as e:
        label = "stuck credit" if stuck_credit else "checkmachinelog"
        _cml_send(f"❌ {label} failed: {e}")
        print(f"[{label}] error: {e!r}")


def run_checkcredit_navigator_next_log(chat_id: str) -> None:
    """Open the next same-day logic log (card **check another logs**) — OSS or LogNavigator."""
    pend = _get_checkcredit_np_pending(chat_id)
    files = (pend or {}).get("navigator_logic_log_files") or []
    opened = str((pend or {}).get("navigator_opened_logic_log_basename") or "").strip()
    if not pend or len(files) < 2:
        _checkcredit_send(
            chat_id,
            "❌ No alternate logic logs in context — run `/checkcredit …` again.",
        )
        return
    try:
        idx = files.index(opened) if opened in files else 0
    except ValueError:
        idx = 0
    next_idx = (idx + 1) % len(files)
    next_fn = str(files[next_idx] or "").strip()
    if not next_fn:
        _checkcredit_send(chat_id, "❌ Could not resolve next log filename.")
        return
    mq = str((pend.get("machine_display") or "")).strip()
    date_iso = str((pend.get("target_date") or "")).strip()
    if not mq or not date_iso:
        _checkcredit_send(chat_id, "❌ Pending machine/date missing — run `/checkcreditdate …` again.")
        return
    thread_root = _get_checkcredit_thread_root(chat_id)
    _checkcredit_send(chat_id, f"⏳ Opening next logic log `{next_fn}` …", thread_root=thread_root)
    run_checkcredit_finderror(
        chat_id,
        mq,
        date_iso,
        mode="default",
        navigator_logic_log_basename=next_fn,
        thread_root_message_id=thread_root,
    )


def run_checkcredit_player_job(chat_id: str, machine: str, player_id: str, date_iso: str) -> None:
    """OSS log → player credit row → Third Http Detail screenshot (same path as ``/npthirdhttp``)."""
    try:
        import checkcredit
        from datetime import datetime as _dt

        td = _dt.strptime(date_iso.strip(), "%Y-%m-%d").date()
    except ValueError:
        _checkcredit_send(chat_id, "❌ Invalid date — use YYYY-MM-DD.")
        return
    except ImportError as e:
        _checkcredit_send(chat_id, f"❌ Cannot load checkcredit: {e}")
        return
    md, lc, err = checkcredit.resolve_player_log_credit_snapshot(
        machine.strip(), player_id.strip(), td
    )
    if err:
        _checkcredit_send(chat_id, f"❌ {err}")
        return
    assert lc is not None
    ts = str(lc.get("time_short") or "").strip()
    if not ts:
        _checkcredit_send(chat_id, "❌ No credit time in log for this player.")
        return
    exp: Optional[float] = None
    v = lc.get("value")
    if v is not None:
        try:
            exp = float(v)
        except (TypeError, ValueError):
            exp = None
    display_md = (md or "").strip() or None
    ms = checkcredit.machine_match_substr_from_display((md or "").strip()) or None
    _np_run_screenshot_worker(
        chat_id,
        player_id.strip(),
        date_iso.strip(),
        ts,
        machine_substr=ms,
        expected_credit=exp,
        machine_display=display_md,
    )


def run_cctv_screenshot_job(chat_id: str, machine_query: str) -> None:
    """EGM Status: click **CCTV**, screenshot dialog only (no credit / log checks)."""
    try:
        import checkcredit
    except ImportError as e:
        send_message(chat_id, f"❌ Cannot load checkcredit module: {e}")
        return
    cap = getattr(checkcredit, "screenshot_egm_cctv_window", None)
    resolve_route = getattr(checkcredit, "resolve_machine_display_for_egm_route", None)
    if not callable(cap):
        send_message(
            chat_id,
            "❌ `checkcredit.screenshot_egm_cctv_window` missing — deploy the latest `checkcredit.py`.",
        )
        return
    mq = (machine_query or "").strip()
    if not mq:
        send_message(
            chat_id,
            "❌ Usage: `/cctv <machine>` — same machine label as checkcredit (e.g. `OSMCP181`, `Dragons-0181`).",
        )
        return
    md_resolved = mq
    ms_resolved: Optional[str] = None
    if callable(resolve_route):
        send_message(chat_id, "⏳ LogNavigator / OSS — resolving machine → correct backend (NCH, CP, …)…")
        try:
            md_resolved, ms_resolved = resolve_route(mq, timeout_ms=120_000)
        except Exception as e:
            send_message(chat_id, f"❌ Could not resolve machine / environment: {e}")
            return
        tag_fn = getattr(checkcredit, "_np_log_backend_tag", lambda _: "?")
        send_message(
            chat_id,
            f"→ **{md_resolved}** · backend **{tag_fn(md_resolved)}** — EGM **CCTV**…",
        )
    else:
        send_message(chat_id, "⏳ EGM **CCTV** — login → click **CCTV** → screenshot…")
    path = None
    try:
        path = cap(
            machine_display=md_resolved,
            machine_substr=(ms_resolved or "").strip() or None,
            timeout_ms=120_000,
            headed=False,
        )
        key = upload_image_lark(path)
        if not key:
            send_message(chat_id, "❌ CCTV screenshot upload failed.")
            return
        r = send_image_message(chat_id, key)
        if r.get("code") != 0:
            send_message(chat_id, f"❌ Failed to send image: {r}")
    except Exception as e:
        send_message(chat_id, f"❌ CCTV screenshot failed: {e}")
        print(f"[cctv] error: {e!r}", flush=True)
    finally:
        if path and os.path.isfile(path):
            try:
                os.unlink(path)
            except OSError:
                pass


def _third_http_warm_enabled_for_bot() -> bool:
    try:
        from third_http_warm_pool import third_http_warm_pool_enabled

        return bool(third_http_warm_pool_enabled())
    except Exception:
        return False


def _np_run_screenshot_worker(
    chat_id: str,
    uid: str,
    date_iso: str,
    time_short: str,
    *,
    machine_substr: Optional[str] = None,
    expected_credit: Optional[float] = None,
    machine_display: Optional[str] = None,
    thread_root: Optional[str] = None,
    success_caption: Optional[str] = None,
    time_short_candidates: Optional[list[str]] = None,
) -> None:
    """NP / WF / DHS / NCH / CP / OSM / MDR / TBP Log Third Http → `recharge` Detail screenshot. Always **headless** on server."""
    root = (thread_root or _get_checkcredit_thread_root(chat_id) or "").strip() or None

    def _np_send(text, **kwargs):
        return _checkcredit_send(chat_id, text, thread_root=root, **kwargs)

    try:
        import checkcredit

        screenshot_np_recharge_detail = checkcredit.screenshot_np_recharge_detail
    except ImportError as e:
        _np_send(f"❌ Cannot load checkcredit module: {e}")
        return
    except AttributeError:
        _np_send(
            "❌ checkcredit.screenshot_np_recharge_detail missing — deploy the latest `checkcredit.py`.",
        )
        return
    backend_tag = getattr(checkcredit, "_np_log_backend_tag", lambda _: "NP")(
        (machine_display or "").strip() or None
    )
    _np_send(
        f"⏳ {backend_tag} backend (Playwright): Log Third Http → recharge Detail"
        f"{' (warm browser)' if _third_http_warm_enabled_for_bot() else ''}…",
    )
    path = None
    try:
        path = screenshot_np_recharge_detail(
            uid,
            date_iso,
            time_short,
            timeout_ms=120_000,
            machine_substr=machine_substr,
            expected_credit=expected_credit,
            machine_display=machine_display,
            headed=False,
            time_short_candidates=time_short_candidates,
        )
        key = upload_image_lark(path)
        if not key:
            _np_send("❌ Failed to upload screenshot to Lark.")
            return
        if (success_caption or "").strip():
            _np_send(success_caption.strip())
        if root:
            r = reply_message_in_thread(root, key, msg_type="image")
        else:
            r = send_image_message(chat_id, key)
        if r.get("code") != 0:
            _np_send(f"❌ Failed to send image: {r}")
    except Exception as e:
        err_s = str(e)
        if "No Log Third Http rows" in err_s or "empty table after Search" in err_s:
            tip = (
                "\n💡 **No Third Http rows** for this UserId/time window — transfer likely **did not complete** "
                f"(log error player `{uid}` @ `{time_short}`). "
                "Widen `NP_BACKEND_WINDOW_MINUTES` if the log time is near the window edge."
                "\n💡 Third Http **无记录** — 转出可能**未成功**（日志 error 玩家与时间见上）。"
                " 可调大 `NP_BACKEND_WINDOW_MINUTES`。"
            )
        elif "did not load after Search" in err_s or "tbody stayed hidden" in err_s:
            tip = (
                "\n💡 Search results never became ready (empty/hidden table or slow UI). "
                "Retry, or run locally: `python3 checkcredit.py --checkuser ... --pause`."
                "\n💡 搜索结果未就绪（空表/隐藏 tbody 或页面慢），请重试或用 `--checkuser --pause` 本地查看。"
            )
        elif "No " in err_s and " Detail on pages" in err_s:
            tip = (
                "\n💡 Rows exist but **no matching recharge Detail** — bot already retries **machineId-only** "
                "when log `amount` ≠ Detail `amount`. Try `NP_BACKEND_MAX_PAGES` / `NP_BACKEND_WINDOW_MINUTES`. "
                "Disable machine-only pass: `NP_THIRD_HTTP_NO_MACHINE_ONLY_FALLBACK=1`."
                "\n💡 有 recharge 行但 Detail 不匹配 — 已自动尝试仅匹配机台；可调页数/时间窗。"
            )
        elif "No usable temporary directory" in err_s or "writable temporary directory" in err_s:
            tip = (
                "\n💡 **Server has no writable `/tmp`** — screenshot never started (not an NP search miss). "
                "On the bot host: `mkdir -p /root/osedutybot/.tmp && chmod 700 /root/osedutybot/.tmp`, "
                "add `TMPDIR=/root/osedutybot/.tmp` to `.env`, restart Duty Bot, then retry stuck credit."
                "\n💡 服务器 **没有可写临时目录**，截图步骤未执行（不是 Third Http 搜不到）。"
                " 在主机创建 `.tmp` 并设置 `TMPDIR`，重启后再试。"
            )
        else:
            tip = (
                "\n💡 Duty Bot runs this screenshot **headless**. Try raising `NP_BACKEND_MAX_PAGES` / "
                "`NP_BACKEND_WINDOW_MINUTES`, or widen `NP_BACKEND_AMOUNT_EPS` (default `0.05`) in `.env`. "
                "For **TBP**, try `TBP_THIRD_HTTP_AMOUNT_SCALE` (e.g. `100` for cents) or "
                "`TBP_THIRD_HTTP_NO_MACHINE_ONLY_FALLBACK=1` to disable the extra machine-only pass. "
                "For a **visible** Chromium window, run locally: `python3 checkcredit.py --checkuser ... --pause`."
            )
        print(
            "[npthirdhttp] screenshot context "
            f"uid={uid!r} date={date_iso!r} time={time_short!r} "
            f"machine_substr={machine_substr!r} credit={expected_credit!r} "
            f"machine_display={machine_display!r}",
            flush=True,
        )
        _np_send(f"❌ {backend_tag} third-http screenshot failed: {e}{tip}")
        print(f"[npthirdhttp] error: {e!r}")
    finally:
        if path and os.path.isfile(path):
            try:
                os.unlink(path)
            except OSError:
                pass


def run_np_third_http_by_choice(chat_id: str, choice_idx: int) -> None:
    """choice_idx: 1–4 matching `np_choices` from last `/checkcreditdate` NP prompt."""
    pend = _get_checkcredit_np_pending(chat_id)
    choices = (pend or {}).get("np_choices") or []
    if not pend or choice_idx < 1 or choice_idx > len(choices):
        _checkcredit_send(
            chat_id,
            "❌ No active NP choice list — run `/checkcreditdate …` again, then reply **1**–**4**.",
        )
        return
    ch = choices[choice_idx - 1]
    uid = str(ch.get("user_id") or "").strip()
    date_iso = (pend.get("target_date") or "").strip()
    time_short = (ch.get("time_short") or "").strip()
    if not uid or not date_iso or not time_short:
        _checkcredit_send(chat_id, "❌ Pending NP choice is incomplete — use `/npthirdhttp …` with full date/time.")
        return
    ms = (pend.get("machine_match_substr") or "").strip() or None
    exp = ch.get("credit_value")
    if exp is not None:
        try:
            exp = float(exp)
        except (TypeError, ValueError):
            exp = None
    if exp is None and ch.get("credit") not in (None, "", "n/a"):
        try:
            exp = float(str(ch.get("credit")).strip())
        except ValueError:
            exp = None
    md = (pend.get("machine_display") or "").strip() or None
    # If EGM small window currently shows the same member as selected player,
    # short-circuit and prompt that the player has not left machine yet.
    if md:
        try:
            import checkcredit

            get_member = getattr(checkcredit, "get_egm_member_user_id", None)
            if callable(get_member):
                cur_member = str(
                    get_member(
                        machine_display=md,
                        machine_substr=ms,
                        timeout_ms=120_000,
                        headed=False,
                    )
                    or ""
                ).strip()
                if cur_member and cur_member == uid:
                    _checkcredit_send(chat_id, "Player haven't out the machine")
                    return
        except Exception as e:
            print(f"[npthirdhttp] EGM member pre-check skipped: {e!r}", flush=True)
    _np_run_screenshot_worker(
        chat_id,
        uid,
        date_iso,
        time_short,
        machine_substr=ms,
        expected_credit=exp,
        machine_display=md,
    )


def run_np_third_http_job(chat_id: str, argv: list[str]):
    """Background: NP Log Third Http Req → first `recharge` row → Detail dialog screenshot."""
    try:
        import checkcredit

        _ = checkcredit.screenshot_np_recharge_detail
    except ImportError as e:
        _checkcredit_send(chat_id, f"❌ Cannot load checkcredit module: {e}")
        return
    except AttributeError:
        _checkcredit_send(
            chat_id,
            "❌ checkcredit.screenshot_np_recharge_detail missing — deploy the latest `checkcredit.py`.",
        )
        return
    if not argv:
        _checkcredit_send(
            chat_id,
            "❌ Usage: `/npthirdhttp <player_id>` — or `/npthirdhttp <player_id> YYYY-MM-DD HH:MM:SS.mmm`",
        )
        return
    uid = argv[0].strip()
    date_iso: Optional[str] = None
    time_short: Optional[str] = None
    pend = None
    if len(argv) == 2:
        _checkcredit_send(
            chat_id,
            "❌ Use `/npthirdhttp <player_id>` after checkcredit, "
            "or full `/npthirdhttp <player_id> YYYY-MM-DD HH:MM:SS.mmm` (three parts).",
        )
        return
    if len(argv) >= 3:
        date_iso = argv[1].strip()
        time_short = argv[2].strip()
        try:
            datetime.strptime(date_iso, "%Y-%m-%d")
        except ValueError:
            _checkcredit_send(chat_id, "❌ Date must be `YYYY-MM-DD`.")
            return
        if not time_short:
            _checkcredit_send(chat_id, "❌ Missing time (HH:MM:SS or HH:MM:SS.mmm).")
            return
    else:
        pend = _get_checkcredit_np_pending(chat_id)
        if not pend:
            _checkcredit_send(
                chat_id,
                "❌ No pending `/checkcreditdate` context in this chat. "
                "Run checkcredit first, or use `/npthirdhttp <player_id> YYYY-MM-DD HH:MM:SS.mmm`.",
            )
            return
        date_iso = pend["target_date"]
        time_short = ""
        for ch in pend.get("np_choices") or []:
            if str(ch.get("user_id")) == str(uid):
                time_short = (ch.get("time_short") or "").strip()
                break
        if not time_short:
            for p in pend.get("latest_two_players", []):
                if str(p.get("user_id")) == str(uid):
                    time_short = (p.get("time_short") or "").strip()
                    break
        if not time_short:
            _checkcredit_send(
                chat_id,
                f"❌ User ID `{uid}` not in the last checkcredit NP list (choices 1–4). "
                f"Use: `/npthirdhttp {uid} YYYY-MM-DD HH:MM:SS.mmm`",
            )
            return

    assert date_iso is not None and time_short is not None
    ms = None
    exp = None
    md: Optional[str] = None
    if pend:
        md = (pend.get("machine_display") or "").strip() or None
        ms = (pend.get("machine_match_substr") or "").strip() or None
        for ch in pend.get("np_choices") or []:
            if str(ch.get("user_id")) == str(uid):
                exp = ch.get("credit_value")
                if exp is not None:
                    try:
                        exp = float(exp)
                    except (TypeError, ValueError):
                        exp = None
                if exp is None and ch.get("credit") not in (None, "", "n/a"):
                    try:
                        exp = float(str(ch.get("credit")).strip())
                    except ValueError:
                        exp = None
                break
    _np_run_screenshot_worker(
        chat_id,
        uid,
        date_iso,
        time_short,
        machine_substr=ms,
        expected_credit=exp,
        machine_display=md,
    )


def scheduled_amountloss_check():
    """
    每日 9:00：与手动 `/al` 相同（filterdata + CHECKLOG + Lark sheet 同步 + 卡片 + TSV）。
    目标日期为昨天（与 fetch_fpms_data 默认一致）。
    TEMPORARY: skipped unless AMOUNT_LOSS_9AM_ENABLED=1.
    """
    try:
        from amountloss import amount_loss_9am_enabled
    except ImportError:
        print("[Amount Loss] 9:00 job skipped (amountloss unavailable)", flush=True)
        return
    if not amount_loss_9am_enabled():
        print(
            "[Amount Loss] 9:00 display/sheet fill skipped (temporarily disabled; AMOUNT_LOSS_9AM_ENABLED=1 to restore)",
            flush=True,
        )
        return
    target_chat_id = DUTY_CHAT_ID
    mention = f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>'
    send_message(
        target_chat_id,
        f"{mention}\n⏳ Amount Loss (9:00 — same as /al), please wait...",
    )
    threading.Thread(
        target=run_amountloss_check,
        args=(target_chat_id, None),
        kwargs={"scheduled_9am": True},
        daemon=True,
    ).start()
    print(f"✅ Scheduled Amount Loss (9:00) started (run_amountloss_check) → {target_chat_id}")

# ================= LARK API HELPERS =================
def add_all_reactions(message_id):
    token = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    success_count = 0
    for emoji in ALL_EMOJI_CODES:
        url = f"https://open.larksuite.com/open-apis/im/v1/messages/{message_id}/reactions"
        payload = {"reaction_type": {"emoji_type": emoji}}
        for attempt in range(3):
            resp = requests.post(url, headers=headers, json=payload)
            if resp.status_code == 200:
                print(f"✅ Added {emoji}")
                success_count += 1
                break
            elif resp.status_code == 429:
                wait = (2 ** attempt) + random.uniform(0, 0.5)
                print(f"⚠️ Rate limited on {emoji}, retrying after {wait:.1f}s")
                time.sleep(wait)
                continue
            else:
                print(f"⚠️ {emoji} failed: {resp.status_code} {resp.text}")
                break
        time.sleep(1.0)
    print(f"Added {success_count} of {len(ALL_EMOJI_CODES)} reactions")

def add_random_reaction(message_id):
    token = get_tenant_access_token()
    url = f"https://open.larksuite.com/open-apis/im/v1/messages/{message_id}/reactions"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    random_emoji = random.choice(RANDOM_EMOJI_CODES)
    payload = {"reaction_type": {"emoji_type": random_emoji}}
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"✅ Added {random_emoji} reaction to message {message_id}")
    else:
        print(f"❌ Failed to add reaction: {response.text}")
    return response.json()

def add_heart_reaction(message_id):
    return add_message_reaction(message_id, "HEART")


def add_message_reaction(message_id, emoji_type, *, fallbacks: tuple[str, ...] = ()):
    mid = (message_id or "").strip()
    if not mid:
        print("[lark] reaction skipped: missing message_id", flush=True)
        return None
    token = get_tenant_access_token()
    url = f"https://open.larksuite.com/open-apis/im/v1/messages/{mid}/reactions"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    for code in (emoji_type, *fallbacks):
        et = (code or "").strip()
        if not et:
            continue
        payload = {"reaction_type": {"emoji_type": et}}
        response = requests.post(url, headers=headers, json=payload, timeout=20)
        try:
            body = response.json()
        except Exception:
            body = {}
        if response.status_code == 200 and int(body.get("code", -1)) == 0:
            print(f"✅ Added {et} reaction to message {mid}", flush=True)
            return body
        print(
            f"⚠️ {et} reaction failed: status={response.status_code} body={body!r}",
            flush=True,
        )
    return None


def _reaction_id_from_response(add_reaction_body: Any) -> Optional[str]:
    """Pull ``data.reaction_id`` out of a successful add-reaction response."""
    if isinstance(add_reaction_body, dict):
        rid = ((add_reaction_body.get("data") or {}).get("reaction_id") or "").strip()
        return rid or None
    return None


def remove_message_reaction(message_id, reaction_id) -> bool:
    """DELETE a reaction previously added to a message (by its reaction_id)."""
    mid = (message_id or "").strip()
    rid = (reaction_id or "").strip()
    if not mid or not rid:
        return False
    token = get_tenant_access_token()
    url = f"https://open.larksuite.com/open-apis/im/v1/messages/{mid}/reactions/{rid}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.delete(url, headers=headers, timeout=20)
        body = response.json()
    except Exception as exc:  # noqa: BLE001 — network hiccups shouldn't break the reply
        print(f"⚠️ remove reaction {rid} error: {exc!r}", flush=True)
        return False
    if response.status_code == 200 and int(body.get("code", -1)) == 0:
        print(f"✅ Removed reaction {rid} from message {mid}", flush=True)
        return True
    print(
        f"⚠️ remove reaction {rid} failed: status={response.status_code} body={body!r}",
        flush=True,
    )
    return False


# Lark UI tooltip may say "GotIt"; official emoji_type is **Get** (see im message-reaction emojis doc).
_GOT_IT_REACTION_FALLBACKS = ("GotIt", "GOTIT", "LGTM", "OnIt", "CheckMark")


def add_gotit_reaction(message_id):
    override = (os.getenv("OFFSET_ACK_EMOJI") or "").strip()
    if override and override not in ("GotIt", "GOTIT", "OK"):
        return add_message_reaction(
            message_id, override, fallbacks=("Get", *_GOT_IT_REACTION_FALLBACKS)
        )
    return add_message_reaction(message_id, "Get", fallbacks=_GOT_IT_REACTION_FALLBACKS)


_DONE_REACTION_FALLBACKS = ("Done", "CheckMark", "JIAYI")


def add_done_reaction(message_id):
    return add_message_reaction(message_id, "DONE", fallbacks=_DONE_REACTION_FALLBACKS)


_lark_user_message_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "_lark_user_message_id", default=None
)
_lark_defer_done_reaction: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "_lark_defer_done_reaction", default=False
)
# reaction_id of the "got it" ack, so it can be removed when processing finishes.
_lark_gotit_reaction_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "_lark_gotit_reaction_id", default=None
)
# Chat the inbound message came from. ``send_message`` only auto-quote-replies when the
# target chat IS this chat — otherwise a cross-group send would be turned into a reply
# in the INBOUND chat and its ``chat_id`` argument silently discarded (see send_message).
_lark_user_chat_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "_lark_user_chat_id", default=None
)


def set_lark_incoming_message(
    message_id: Optional[str] = None, chat_id: Optional[str] = None
) -> None:
    mid = (message_id or "").strip() or None
    _lark_user_message_id.set(mid)
    _lark_user_chat_id.set((chat_id or "").strip() or None)
    _lark_defer_done_reaction.set(False)
    _lark_gotit_reaction_id.set(None)


def remember_gotit_reaction(add_reaction_body: Any) -> None:
    """Record the just-added GotIt reaction's id so :func:`mark_lark_process_done`
    can remove it and replace it with DONE."""
    _lark_gotit_reaction_id.set(_reaction_id_from_response(add_reaction_body))


def defer_lark_done_reaction() -> None:
    """Background work will call :func:`mark_lark_process_done` when finished."""
    _lark_defer_done_reaction.set(True)


def mark_lark_process_done(message_id: Optional[str] = None) -> None:
    mid = (message_id or _lark_user_message_id.get() or "").strip()
    if not mid:
        return
    # Swap the "got it" ack for "done": remove the ack reaction first, then add DONE.
    rid = (_lark_gotit_reaction_id.get() or "").strip()
    if rid:
        remove_message_reaction(mid, rid)
        _lark_gotit_reaction_id.set(None)
    add_done_reaction(mid)


def finish_lark_incoming_message_if_sync() -> None:
    if _lark_defer_done_reaction.get():
        return
    if not (_lark_user_message_id.get() or "").strip():
        return
    mark_lark_process_done()


def lark_background_task(fn, *args, **kwargs):
    """Run ``fn`` in a thread; add **DONE** on the triggering user message when it returns."""
    defer_lark_done_reaction()
    try:
        return fn(*args, **kwargs)
    finally:
        mark_lark_process_done()


def start_lark_background_thread(fn, *args, **kwargs) -> None:
    """Spawn a daemon thread that preserves Lark incoming-message context for quoted replies."""
    ctx = contextvars.copy_context()

    def _target() -> None:
        ctx.run(lark_background_task, fn, *args, **kwargs)

    threading.Thread(target=_target, daemon=True).start()


def _lark_im_ack():
    """HTTP 200 for Lark without GotIt/Done reactions (ignored messages)."""
    return jsonify({"success": True})


def _lark_im_done():
    finish_lark_incoming_message_if_sync()
    return jsonify({"success": True})


def recall_message(message_id):
    token = get_tenant_access_token()
    url = f"https://open.larksuite.com/open-apis/im/v1/messages/{message_id}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.delete(url, headers=headers)
    if resp.status_code == 200:
        print(f"✅ Message {message_id} recalled")
    else:
        print(f"❌ Failed to recall message {message_id}: {resp.text}")
        
def _lark_build_message_content(text, msg_type: str = "text") -> str:
    if msg_type == "interactive":
        return text if isinstance(text, str) else json.dumps(text)
    if msg_type == "image":
        return json.dumps({"image_key": text})
    return json.dumps({"text": text})


def _lark_post_message_reply(
    parent_message_id: str,
    text,
    *,
    msg_type: str = "text",
    mentions=None,
    reply_in_thread: bool = False,
) -> dict:
    """POST ``/im/v1/messages/{message_id}/reply`` — quoted reply or thread-only reply."""
    mid = (parent_message_id or "").strip()
    if not mid:
        return {"code": -1, "msg": "no message_id"}
    token = get_tenant_access_token()
    if not token:
        print("[lark] message reply skipped: no tenant_access_token", flush=True)
        return {"code": -1, "msg": "no tenant_access_token"}
    url = f"https://open.larksuite.com/open-apis/im/v1/messages/{mid}/reply"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body: dict[str, Any] = {
        "msg_type": msg_type,
        "content": _lark_build_message_content(text, msg_type),
    }
    if reply_in_thread:
        body["reply_in_thread"] = True
    if mentions:
        body["mentions"] = mentions
    return requests.post(url, headers=headers, json=body).json()


def send_message(
    chat_id,
    text,
    msg_type="text",
    mentions=None,
    receive_id_type="chat_id",
    reply_to_message_id=None,
):
    """Send to chat, or quote-reply to ``reply_to_message_id`` (defaults to inbound user message).

    The implicit quote-reply applies ONLY when ``chat_id`` is the chat the inbound
    message came from. A reply is posted via ``/messages/{id}/reply``, which puts it in
    the PARENT message's chat and ignores ``chat_id`` entirely — so without this check a
    cross-group send (e.g. ``/m`` posting the forward + check-email cards to the QA/CS
    group while the user typed ``/m`` in the command group) silently landed back in the
    inbound chat. Pass ``reply_to_message_id=""`` to force a direct post explicitly.
    """
    if reply_to_message_id is not None:
        reply_mid = (reply_to_message_id or "").strip() or None
    else:
        _inbound_chat = (_lark_user_chat_id.get() or "").strip()
        _target_chat = str(chat_id or "").strip()
        if _inbound_chat and _target_chat and _target_chat != _inbound_chat:
            reply_mid = None  # cross-chat send → direct post, never a reply
        else:
            reply_mid = (_lark_user_message_id.get() or "").strip() or None
    if reply_mid:
        return _lark_post_message_reply(
            reply_mid, text, msg_type=msg_type, mentions=mentions, reply_in_thread=False
        )
    token = get_tenant_access_token()
    if not token:
        print("[lark] send_message skipped: no tenant_access_token", flush=True)
        return {"code": -1, "msg": "no tenant_access_token"}
    url = "https://open.larksuite.com/open-apis/im/v1/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    content = _lark_build_message_content(text, msg_type)
    # Lark `POST /im/v1/messages` request body is only receive_id + msg_type + content (+ optional uuid).
    # Do not send undocumented fields — stray keys have caused odd interactive-card behavior in the wild.
    body = {
        "receive_id": chat_id,
        "msg_type": msg_type,
        "content": content,
    }
    if mentions:
        body["mentions"] = mentions
    rid_type = (receive_id_type or "chat_id").strip() or "chat_id"
    params = {"receive_id_type": rid_type}
    response = requests.post(url, headers=headers, params=params, json=body)
    return response.json()


def _extract_lark_message_id(resp: Any) -> str:
    if not isinstance(resp, dict):
        return ""
    data = resp.get("data") or {}
    if not isinstance(data, dict):
        return ""
    mid = str(data.get("message_id") or "").strip()
    if mid:
        return mid
    nested = data.get("message") or {}
    if isinstance(nested, dict):
        return str(nested.get("message_id") or "").strip()
    return ""


def reply_message_in_thread(
    parent_message_id: str,
    text: str,
    msg_type: str = "text",
    mentions=None,
) -> dict:
    """Reply inside a thread only (``reply_in_thread=true`` — not main chat stream)."""
    return _lark_post_message_reply(
        parent_message_id,
        text,
        msg_type=msg_type,
        mentions=mentions,
        reply_in_thread=True,
    )

def send_file(chat_id, file_token):
    token = get_tenant_access_token()
    url = "https://open.larksuite.com/open-apis/im/v1/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "receive_id": chat_id,
        "msg_type": "file",
        "content": json.dumps({"file_key": file_token}),
    }
    params = {"receive_id_type": "chat_id"}
    response = requests.post(url, headers=headers, params=params, json=payload)
    return response.json()
    
def upload_file_to_drive(file_path):
    token = get_tenant_access_token()
    url = "https://open.larksuite.com/open-apis/drive/v1/files/upload_all"
    headers = {"Authorization": f"Bearer {token}"}
    with open(file_path, 'rb') as f:
        files = {'file': f}
        data = {'file_name': os.path.basename(file_path)}
        resp = requests.post(url, headers=headers, files=files, data=data)
    result = resp.json()
    if result.get('code') == 0:
        return result['data']['file_token']
    else:
        print(f"❌ Drive upload failed: {result}")
        return None


def upload_image_lark(image_path: str):
    """Upload PNG/JPEG for im/v1/messages msg_type=image; returns image_key or None."""
    token = get_tenant_access_token()
    url = "https://open.larksuite.com/open-apis/im/v1/images"
    headers = {"Authorization": f"Bearer {token}"}
    ext = os.path.splitext(image_path)[1].lower()
    mime, _ = mimetypes.guess_type(image_path)
    if not mime or mime not in ("image/png", "image/jpeg"):
        mime = "image/jpeg" if ext in (".jpg", ".jpeg") else "image/png"
    with open(image_path, "rb") as f:
        files = {"image": (os.path.basename(image_path), f, mime)}
        data = {"image_type": "message"}
        resp = requests.post(url, headers=headers, files=files, data=data)
    result = resp.json()
    if result.get("code") == 0:
        return result.get("data", {}).get("image_key")
    print(f"❌ Lark image upload failed: {result}")
    return None


def send_image_message(chat_id, image_key: str):
    token = get_tenant_access_token()
    url = "https://open.larksuite.com/open-apis/im/v1/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "receive_id": chat_id,
        "msg_type": "image",
        "content": json.dumps({"image_key": image_key}),
    }
    params = {"receive_id_type": "chat_id"}
    return requests.post(url, headers=headers, params=params, json=payload).json()


# Last `/checkcreditdate` result in this chat — used by `/npthirdhttp` for date + credit time window.
CHECKCREDIT_NP_PENDING = {}
CHECKCREDIT_THREAD_ROOT: dict[str, dict] = {}


def _set_checkcredit_thread_root(chat_id: str, message_id: str) -> None:
    mid = (message_id or "").strip()
    if not mid:
        return
    CHECKCREDIT_THREAD_ROOT[chat_id] = {"message_id": mid, "ts": time.time()}


def _get_checkcredit_thread_root(chat_id: str, max_age_sec: float = 3600.0) -> Optional[str]:
    ent = CHECKCREDIT_THREAD_ROOT.get(chat_id)
    if not ent:
        return None
    if time.time() - ent["ts"] > max_age_sec:
        del CHECKCREDIT_THREAD_ROOT[chat_id]
        return None
    return str(ent.get("message_id") or "").strip() or None


def _checkcredit_begin_thread(
    chat_id: str,
    parent_message_id: Optional[str] = None,
) -> Optional[str]:
    """Thread under the user's ``/checkcredit`` message (``reply_in_thread`` — not main chat)."""
    parent = (parent_message_id or "").strip() or None
    if parent:
        _set_checkcredit_thread_root(chat_id, parent)
    return parent


def _checkcredit_send(
    chat_id: str,
    text: str,
    *,
    thread_root: Optional[str] = None,
    msg_type: str = "text",
    mentions=None,
) -> dict:
    root = (thread_root or _get_checkcredit_thread_root(chat_id) or "").strip()
    if root:
        return reply_message_in_thread(root, text, msg_type=msg_type, mentions=mentions)
    return send_message(chat_id, text, msg_type=msg_type, mentions=mentions)


def _checkcredit_send_image(chat_id: str, image_key: str, *, thread_root: Optional[str] = None) -> dict:
    root = (thread_root or _get_checkcredit_thread_root(chat_id) or "").strip()
    if root:
        return reply_message_in_thread(root, image_key, msg_type="image")
    return send_image_message(chat_id, image_key)


def _set_checkcredit_np_pending(
    chat_id: str,
    payload: dict,
    thread_root: Optional[str] = None,
) -> None:
    root = (thread_root or _get_checkcredit_thread_root(chat_id) or "").strip() or None
    CHECKCREDIT_NP_PENDING[chat_id] = {"payload": payload, "ts": time.time(), "thread_root": root}
    if root:
        _set_checkcredit_thread_root(chat_id, root)


def _get_checkcredit_np_pending(chat_id: str, max_age_sec: float = 3600.0):
    ent = CHECKCREDIT_NP_PENDING.get(chat_id)
    if not ent:
        return None
    if time.time() - ent["ts"] > max_age_sec:
        del CHECKCREDIT_NP_PENDING[chat_id]
        return None
    return ent["payload"]

# ``/update`` / ``/jenkinsupdate`` — thread replies under the user's command message.
UPDATE_THREAD_ROOT: dict[str, dict] = {}


def _set_update_thread_root(session_key: str, message_id: str) -> None:
    sk = (session_key or "").strip()
    mid = (message_id or "").strip()
    if not sk or not mid:
        return
    UPDATE_THREAD_ROOT[sk] = {"message_id": mid, "ts": time.time()}


def _get_update_thread_root(session_key: str, max_age_sec: float = 7200.0) -> Optional[str]:
    ent = UPDATE_THREAD_ROOT.get((session_key or "").strip())
    if not ent:
        return None
    if time.time() - ent["ts"] > max_age_sec:
        del UPDATE_THREAD_ROOT[(session_key or "").strip()]
        return None
    return str(ent.get("message_id") or "").strip() or None


def update_thread_summary(body: str) -> str:
    for line in (body or "").replace("\r\n", "\n").split("\n"):
        s = line.strip()
        if not s or s.lower().startswith("email:"):
            continue
        s = re.sub(
            r"^/?(?:update|jenkinsupdate|updatejenkins|updatemore)\b\s*",
            "",
            s,
            count=1,
            flags=re.I,
        ).strip()
        return s[:200] if s else "/update"
    return "/update"


def _build_update_thread_starter_card(summary: str) -> dict:
    title = "🔧 /update"
    body = (summary or "").strip()[:500] or "Jenkins update"
    return {
        "schema": "2.0",
        "config": {"width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": title},
        },
        "body": {
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": body}},
            ]
        },
    }


def update_begin_thread(
    chat_id: str,
    session_key: str,
    summary: str,
    *,
    fallback_parent_id: Optional[str] = None,
    force_new: bool = False,
) -> Optional[str]:
    """Bind ``/update`` replies to the user's command message (thread only, no starter card)."""
    sk = (session_key or "").strip()
    if not force_new:
        existing = _get_update_thread_root(sk)
        if existing:
            return existing
    parent = (fallback_parent_id or "").strip() or None
    if parent and sk:
        _set_update_thread_root(sk, parent)
        return parent
    # Already bound on an earlier step (e.g. CPMS pick → multi-env split) — never replace
    # with a main-chat ``🔧 /update`` starter card when ``message_id`` was missing later.
    existing = _get_update_thread_root(sk)
    if existing:
        return existing
    # No incoming message_id (e.g. tests) — post a starter card as last resort.
    card = _build_update_thread_starter_card(summary)
    resp = send_message(chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive")
    if isinstance(resp, dict) and resp.get("code") not in (None, 0):
        print(f"[update] starter card failed chat={chat_id}: {resp}", flush=True)
    else:
        parent = _extract_lark_message_id(resp) or None
    if parent and sk:
        _set_update_thread_root(sk, parent)
    return parent


def make_update_thread_send(chat_id: str, session_key: str, base_send=None):
    if base_send is None:
        base_send = send_message

    def _send(cid, text, msg_type="text", mentions=None, **kwargs):
        root = _get_update_thread_root((session_key or "").strip())
        if root and cid == chat_id:
            return reply_message_in_thread(root, text, msg_type=msg_type, mentions=mentions)
        try:
            return base_send(cid, text, msg_type=msg_type, mentions=mentions, **kwargs)
        except TypeError:
            try:
                return base_send(cid, text, msg_type=msg_type)
            except TypeError:
                return base_send(cid, text)

    return _send


def make_update_thread_send_image(chat_id: str, session_key: str, base_send=None):
    if base_send is None:
        base_send = send_image_message

    def _send_img(cid, image_key):
        root = _get_update_thread_root((session_key or "").strip())
        if root and cid == chat_id:
            return reply_message_in_thread(root, image_key, msg_type="image")
        return base_send(cid, image_key)

    return _send_img


# ``/nchsetmaintenance`` etc. — thread replies under the user's command message only.
PROD_BATCH_THREAD_ROOT: dict[str, dict] = {}


def _set_prod_batch_thread_root(chat_id: str, message_id: str) -> None:
    cid = (chat_id or "").strip()
    mid = (message_id or "").strip()
    if not cid or not mid:
        return
    PROD_BATCH_THREAD_ROOT[cid] = {"message_id": mid, "ts": time.time()}


def _prod_batch_thread_root_from_incoming_message(message: dict, *, message_id: Optional[str] = None) -> Optional[str]:
    """Prefer ``root_id`` when the command was sent inside an existing thread."""
    root = str((message or {}).get("root_id") or "").strip()
    if root:
        return root
    mid = (message_id or (message or {}).get("message_id") or "").strip()
    return mid or None


def _get_prod_batch_thread_root(chat_id: str, max_age_sec: float = 7200.0) -> Optional[str]:
    ent = PROD_BATCH_THREAD_ROOT.get((chat_id or "").strip())
    if not ent:
        return None
    if time.time() - ent["ts"] > max_age_sec:
        del PROD_BATCH_THREAD_ROOT[(chat_id or "").strip()]
        return None
    return str(ent.get("message_id") or "").strip() or None


def make_prod_batch_thread_send(
    chat_id: str,
    *,
    thread_root: Optional[str] = None,
    base_send=None,
):
    if base_send is None:
        base_send = send_message
    cid = (chat_id or "").strip()
    bound_root = (thread_root or "").strip() or None

    def _send(target_chat_id, text, msg_type="text", mentions=None, **kwargs):
        root = (bound_root or _get_prod_batch_thread_root(cid) or "").strip()
        if root and (target_chat_id or "").strip() == cid:
            return reply_message_in_thread(root, text, msg_type=msg_type, mentions=mentions)
        try:
            return base_send(target_chat_id, text, msg_type=msg_type, mentions=mentions, **kwargs)
        except TypeError:
            try:
                return base_send(target_chat_id, text, msg_type=msg_type)
            except TypeError:
                return base_send(target_chat_id, text)

    return _send


def prod_batch_send_image_message(chat_id: str, image_key: str) -> dict:
    cid = (chat_id or "").strip()
    root = (_get_prod_batch_thread_root(cid) or "").strip()
    if root:
        return reply_message_in_thread(root, image_key, msg_type="image")
    return send_image_message(chat_id, image_key)


_CAT_FILE_TOKEN = None
def get_cat_file_token():
    global _CAT_FILE_TOKEN
    if _CAT_FILE_TOKEN is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cat_path = os.path.join(script_dir, "cat.jpg")
        if not os.path.exists(cat_path):
            print("❌ cat.jpg not found")
            return None
        _CAT_FILE_TOKEN = upload_file_to_drive(cat_path)
    return _CAT_FILE_TOKEN

_tenant_token_cache: dict[str, object] = {"token": None, "expires_at": 0.0}
_tenant_token_lock = threading.Lock()
_TENANT_TOKEN_REFRESH_SEC = 120  # refresh before Lark expiry (typically 7200s)


def get_tenant_access_token():
    """Return tenant_access_token; cached ~2h with early refresh."""
    now = time.time()
    with _tenant_token_lock:
        tok = _tenant_token_cache.get("token")
        exp = float(_tenant_token_cache.get("expires_at") or 0.0)
        if tok and now < exp:
            return tok  # type: ignore[return-value]

    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    try:
        response = requests.post(url, headers=headers, json=data, timeout=15)
        response.raise_for_status()
        body = response.json()
    except Exception as ex:
        print(f"[lark] tenant_access_token request failed: {ex!r}", flush=True)
        with _tenant_token_lock:
            stale = _tenant_token_cache.get("token")
            if stale:
                return stale  # type: ignore[return-value]
        return None

    if body.get("code") not in (0, None):
        print(f"[lark] tenant_access_token API error: {body}", flush=True)
        return None

    token = body.get("tenant_access_token")
    if not token:
        print(f"[lark] tenant_access_token missing in response: {body}", flush=True)
        return None

    try:
        expire_sec = int(body.get("expire") or 7200)
    except (TypeError, ValueError):
        expire_sec = 7200
    ttl = max(60, expire_sec - _TENANT_TOKEN_REFRESH_SEC)
    with _tenant_token_lock:
        _tenant_token_cache["token"] = token
        _tenant_token_cache["expires_at"] = time.time() + ttl
    return token

# ================= SCHEDULED REMINDERS =================
# Lark open_id for @mentions (same as reminder.py ``omduty`` / ``OMDUTY``).
TARGET_USER_OPEN_ID = (
    os.getenv("omduty", "").strip()
    or os.getenv("OMDUTY", "").strip()
    or "ou_d7bc33724e2d6ced4050c944c2ca5650"
)
# Sheet-based daily reminders are always delivered to this group.
REMINDER_TARGET_CHAT_ID = os.getenv(
    "REMINDER_TARGET_CHAT_ID",
    "oc_9de3d63fc589df6feeb9b0bee9c45b72",
).strip() or "oc_9de3d63fc589df6feeb9b0bee9c45b72"

# OSE offset approvers (Lark open_id) — each receives pending approval message cards.
OFFSET_APPROVER_OPEN_IDS: frozenset[str] = frozenset(
    {
        "ou_540944d83349cda961ec6124425cdfb4",
        "ou_c4346ace5927c14f51a89b2394b55338",
        "ou_5f660c0fb0769d184aca635d02209272",  # Jun Chen
    }
)

# ---- PLDT prefix rotation (changePrefix.py) ----
# Weekly job changes the CPPLDT trunk's CallerID Prefix (+1) and posts before/after
# screenshots + prefix.png + a @CS(Team) message to this group.
# PLDT rotation posts to TWO groups with DIFFERENT content:
#   - screenshots group: before/after shots + prefix.png (the visual proof)
#   - card group: only the "Change PLDT CallerID Prefix" announcement card
PLDT_SCREENSHOT_GROUP_CHAT_ID = (
    os.getenv("PLDT_SCREENSHOT_GROUP_CHAT_ID", "").strip()
    or os.getenv("PLDT_PREFIX_GROUP_CHAT_ID", "").strip()  # legacy env name
    or "oc_51b6fbf2636525acfb4ead3afa3c93ce"
)
PLDT_CARD_GROUP_CHAT_ID = (
    os.getenv("PLDT_CARD_GROUP_CHAT_ID", "").strip()
    or "oc_6a2f477c2a5a36aba633afab466f3166"
)
# Announce-only groups: the bot POSTS here but IGNORES every inbound message
# (no commands, chat, or @mention handling). The PLDT card group is announce-only;
# add more via ANNOUNCE_ONLY_CHAT_IDS (comma-separated).
ANNOUNCE_ONLY_CHAT_IDS = {PLDT_CARD_GROUP_CHAT_ID} | {
    c.strip() for c in os.getenv("ANNOUNCE_ONLY_CHAT_IDS", "").split(",") if c.strip()
}


def _is_announce_only_chat(chat_id: Optional[str]) -> bool:
    """Groups where the bot only announces — inbound messages are ignored."""
    return (chat_id or "").strip() in ANNOUNCE_ONLY_CHAT_IDS


PLDT_CS_OPEN_ID = (
    os.getenv("PLDT_CS_OPEN_ID", "").strip()
    or "ou_c927a378e9b464741c67b61c1641577b"  # @CS (Team)
)


def run_pldt_prefix_rotation(dry_run: bool = False, notify_chat: Optional[str] = None) -> None:
    """Rotate the PLDT prefix and post the result to the two groups.

    On a successful real run:
      - SCREENSHOT group (``PLDT_SCREENSHOT_GROUP_CHAT_ID``): before shot → after shot → prefix.png
      - CARD group (``PLDT_CARD_GROUP_CHAT_ID``): the "Change PLDT CallerID Prefix" card only

    Run this in a background thread / scheduler job (never inline in the webhook), so
    ``send_message`` posts to the group instead of quote-replying an inbound message.
    ``notify_chat`` set = manual run: status/errors go there. ``notify_chat`` None =
    scheduled run: status/errors are logged (never spam the groups with bot chatter).
    """
    shot_group = PLDT_SCREENSHOT_GROUP_CHAT_ID
    card_group = PLDT_CARD_GROUP_CHAT_ID

    def _status(text: str) -> None:
        if notify_chat:
            send_message(notify_chat, text)
        else:
            print(f"[pldtprefix] {text}", flush=True)

    def _alert_group(text: str) -> None:
        # Real-run failures must reach the SCREENSHOT group even on scheduled runs
        # (dry-run failures stay with the invoker; skip the dup if invoked from there).
        if dry_run or not shot_group or shot_group == notify_chat:
            return
        try:
            send_message(shot_group, text)
        except Exception as alert_exc:  # noqa: BLE001
            print(f"[pldtprefix] failure alert to {shot_group} failed: {alert_exc!r}", flush=True)

    try:
        import changePrefix as _cp
        res = _cp.rotate_prefix(dry_run=dry_run)
    except Exception as exc:  # noqa: BLE001
        print(f"[pldtprefix] rotation error: {exc!r}", flush=True)
        _status(f"❌ PLDT prefix rotation error: {exc}")
        _alert_group(
            f"❌ PLDT prefix rotation error: {exc}\n"
            "The prefix was NOT changed. Please check the bot and rerun with `/pldtrun`."
        )
        return

    if not res.get("ok"):
        _status(
            f"❌ PLDT prefix rotation failed (attempts={res.get('attempts')}): "
            f"{res.get('message')}"
        )
        _alert_group(
            f"❌ PLDT prefix login failed after {res.get('attempts')} attempt(s): "
            f"{res.get('message')}\n"
            "The prefix was NOT changed. Please check the bot and rerun with `/pldtrun`."
        )
        return

    if dry_run:
        _status(
            f"🧪 [dry-run] PLDT prefix would change "
            f"`{res.get('old_prefix')}` → `{res.get('new_prefix')}`; "
            f"CS number `{res.get('message_number')}`. No change applied, nothing posted to the group."
        )
        return

    if res.get("already_applied"):
        # This ISO week's rotation already happened (or was reconciled) — do NOT post
        # a duplicate announcement to the CS group.
        _status(
            f"ℹ️ PLDT prefix already rotated this week "
            f"(`{res.get('old_prefix')}` → `{res.get('new_prefix')}`, "
            f"number `{res.get('message_number')}`). Skipping the group announcement."
        )
        return

    def _post_image(path: Optional[str]) -> None:
        """Upload + send an image to the SCREENSHOT group only."""
        if not (path and os.path.isfile(path)):
            return
        key = upload_image_lark(path)
        if key:
            send_image_message(shot_group, key)
        else:
            send_message(shot_group, "⚠️ (screenshot upload failed)")

    # SCREENSHOT group: before shot → after shot → prefix.png (+ their labels).
    send_message(shot_group, "Before changed the value")
    _post_image(res.get("before_image"))
    send_message(shot_group, "After Changed the value")
    _post_image(res.get("after_image"))
    prefix_png = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prefix.png")
    _post_image(prefix_png)

    number = res.get("message_number")
    # The announcement card goes to BOTH groups (screenshot group gets it AFTER the
    # shots; card group gets only this). Card @mentions use `<at id=ou_…></at>`.
    card = {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "Change PLDT CallerID Prefix"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f'<at id="{PLDT_CS_OPEN_ID}"></at> Please be informed that we have '
                            f"already changed the PLDT prefix number to {number}. Thank you."
                        ),
                    },
                }
            ]
        },
    }
    card_json = json.dumps(card, ensure_ascii=False)
    text_fallback = (
        f'<at user_id="{PLDT_CS_OPEN_ID}">CS (Team)</at> Please be informed that we have '
        f"already changed the PLDT prefix number to {number}. Thank you."
    )
    for g in dict.fromkeys([card_group, shot_group]):  # both groups, deduped
        resp = send_message(g, card_json, msg_type="interactive")
        if isinstance(resp, dict) and resp.get("code") not in (0, None):
            # Card failed for this group → fall back to plain text so CS still gets notified.
            print(f"[pldtprefix] announcement card failed for {g} ({resp}); sending text", flush=True)
            send_message(g, text_fallback)
    print(
        f"[pldtprefix] rotation done: {res.get('old_prefix')} → {res.get('new_prefix')} "
        f"(shots→{shot_group}, card→{card_group})",
        flush=True,
    )


def pldt_prefix_weekly_rotate() -> None:
    """Scheduler entrypoint — Tuesday 05:55 Asia/Manila."""
    run_pldt_prefix_rotation(dry_run=False)


def send_shift_reminder(chat_id, message):
    send_message(chat_id, message)
    print(f"⏰ Shift reminder sent to {chat_id}: {message}")


def _send_ose_payload(chat_id: str, payload: dict, *, mention_user_id: Optional[str] = None) -> None:
    text = str((payload or {}).get("text") or "").strip()
    card = (payload or {}).get("lark_card")
    if mention_user_id:
        send_message(chat_id, f'<at user_id="{mention_user_id}">User</at>')
    if isinstance(card, dict):
        resp = send_message(chat_id, json.dumps(card), msg_type="interactive")
        if resp.get("code") != 0 and text:
            send_message(chat_id, text)
        return
    if text:
        send_message(chat_id, text)


def morning_reminder():
    today = datetime.now().date()
    payload = ose_Duty.get_ose_payload_for_date(today, mode="morning", include_tag=True)
    _send_ose_payload(DUTY_CHAT_ID, payload)
    print(f"⏰ OSE morning card sent to {DUTY_CHAT_ID}")

def evening_reminder():
    today = datetime.now().date()
    payload = ose_Duty.get_ose_payload_for_date(today, mode="evening", include_tag=True)
    _send_ose_payload(DUTY_CHAT_ID, payload)
    print(f"⏰ OSE evening card sent to {DUTY_CHAT_ID}")


def reminder_sheet_daily_sync():
    """Purge expired reminder rows and reload cron jobs from Bitable."""
    try:
        cnt, total = reminder.sync_sheet_daily_reminders(
            scheduler=scheduler,
            send_func=send_message,
            get_token_func=get_tenant_access_token,
            chat_id=REMINDER_TARGET_CHAT_ID,
            target_user_id=TARGET_USER_OPEN_ID,
        )
        print(f"[Reminder sheet] daily sync: {cnt}/{total} job(s)", flush=True)
    except Exception as exc:
        print(f"[Reminder sheet] daily sync failed: {exc!r}", flush=True)


_ose_bitable_sync_lock = threading.Lock()
_leave_wfh_sync_lock = threading.Lock()
_holiday_sync_lock = threading.Lock()


def poll_offset_approver_notifications_from_bitable():
    """Notify approvers + mirror manual Base offset edits to wiki Offset2026."""
    try:
        import offsetleave as ol

        stats = ol.scan_bitable_pending_offsets_for_approver_notify()
        n = int((stats or {}).get("notified") or 0)
        if n:
            print(f"[offsetleave] bitable poll: notified approvers for {n} pending offset(s)", flush=True)
        wiki = ol.scan_bitable_offsets_for_duty_wiki_sync()
        ws = int((wiki or {}).get("synced") or 0)
        wd = int((wiki or {}).get("deleted") or 0)
        if ws or wd:
            print(
                f"[offsetleave] duty wiki poll: synced {ws} row(s), deleted {wd} row(s) "
                f"(scanned {(wiki or {}).get('scanned')})",
                flush=True,
            )
        dele = ol.scan_bitable_offsets_for_deletion_notify()
        dn = int((dele or {}).get("notified") or 0)
        if dn:
            print(
                f"[offsetleave] deletion poll: notified approvers for {dn} deleted offset(s)",
                flush=True,
            )
        req = ol.scan_bitable_offsets_for_requester_approval_notify()
        rn = int((req or {}).get("notified") or 0)
        pn = int((req or {}).get("peer_notified") or 0)
        if rn or pn:
            print(
                f"[offsetleave] approval poll: notified {rn} requester(s), {pn} peer approver(s)",
                flush=True,
            )
        import ose_Duty as od

        sh = od.scan_bitable_approved_offsets_for_shift_sheet()
        sa = int((sh or {}).get("applied") or 0)
        if sa:
            print(f"[ose_Duty] shift sheet poll: applied {sa} approved offset(s)", flush=True)
        rv = od.scan_revert_deleted_offsets_from_shift_sheet()
        sr = int((rv or {}).get("reverted") or 0)
        if sr:
            print(f"[ose_Duty] shift sheet poll: reverted {sr} deleted offset(s)", flush=True)
        lv = od.scan_bitable_approved_leave_for_shift_sheet()
        la = int((lv or {}).get("applied") or 0)
        lr = int((lv or {}).get("restyled") or 0)
        if la or lr:
            print(
                f"[ose_Duty] leave shift sheet poll: applied {la} leave row(s), restyled {lr}",
                flush=True,
            )
        lrv = od.scan_revert_deleted_leave_from_shift_sheet()
        lsr = int((lrv or {}).get("reverted") or 0)
        if lsr:
            print(f"[ose_Duty] leave shift sheet poll: reverted {lsr} deleted/unapproved leave(s)", flush=True)
    except Exception as exc:
        print(f"[offsetleave] bitable approver poll failed: {exc!r}", flush=True)


def ose_leave_offset_daily_sync():
    """Refresh OSE Lark Bitable leave/offset cache once per day (same host TZ as morning OSE)."""
    if not _ose_bitable_sync_lock.acquire(blocking=False):
        print("[OSE Bitable] sync skipped (already running)", flush=True)
        return
    try:
        line = ose_Duty.sync_ose_leave_offset_bitable()
        print(f"[OSE Bitable] {line}", flush=True)
        try:
            pur = ose_Duty.purge_stale_ose_offset_bitable_rows()
            if pur.get("deleted"):
                print(
                    f"[OSE Bitable] stale offset purge: deleted {pur['deleted']} row(s) "
                    f"(scanned {pur.get('scanned')}, ref {pur.get('ref_date')})",
                    flush=True,
                )
            if pur.get("errors"):
                print(f"[OSE Bitable] stale offset purge errors: {pur['errors']!r}", flush=True)
        except Exception as exc:
            print(f"[OSE Bitable] stale offset purge failed: {exc!r}", flush=True)
        try:
            import offsetleave as ol

            ol.schedule_offset_duty_wiki_sync(full=True)
        except Exception as exc:
            print(f"[OSE Bitable] duty wiki offset full sync schedule failed: {exc!r}", flush=True)
    finally:
        _ose_bitable_sync_lock.release()


def ose_leave_wfh_calendar_sync():
    """
    Sync HRMS company Leave + WFH calendars into tracking Bitables (leavewfh.py).

    Keeps **current month and all future months** in each tracking table (past months pruned).
    Per-month refresh only touches rows overlapping that month (other months stay).
    """
    if not _leave_wfh_sync_lock.acquire(blocking=False):
        print("[Leave/WFH sync] skipped (already running)", flush=True)
        return
    try:
        try:
            import leavewfh as lw
        except ImportError:
            import leave as lw  # type: ignore[no-redef]

        bundle = lw.sync_hrms_to_tracking_bitables()
        y, m = bundle["year"], bundle["month"]
        sync_months = bundle.get("sync_months") or [(y, m)]
        leave_res = bundle["leaveose"]
        leave_all_res = bundle["leave_all"]
        wfh_res = bundle["wfh"]
        months_label = ", ".join(f"{yy}-{mm:02d}" for yy, mm in sync_months)
        if len(sync_months) > 6:
            months_label = (
                f"{sync_months[0][0]}-{sync_months[0][1]:02d}"
                f" … {sync_months[-1][0]}-{sync_months[-1][1]:02d} ({len(sync_months)} months)"
            )
        print(
            f"[Leave/WFH sync] months={months_label} leaveose: "
            f"deleted={leave_res.get('deleted', 0)} added={leave_res.get('added', leave_res.get('created', 0))} "
            f"pruned_past={leave_res.get('pruned_past', 0)} "
            f"| leave全员: deleted={leave_all_res.get('deleted', 0)} "
            f"added={leave_all_res.get('added', leave_all_res.get('created', 0))} "
            f"| WFH: deleted={wfh_res.get('deleted', 0)} added={wfh_res.get('added', 0)}",
            flush=True,
        )
        for label, res in (
            ("leaveose", leave_res),
            ("leave_all", leave_all_res),
            ("wfh", wfh_res),
        ):
            if res.get("fetch_failed"):
                print(
                    f"[Leave/WFH sync] ⚠️ {label} FETCH FAILED — table left unchanged, "
                    f"will retry next run: {res.get('message') or res.get('warnings')}",
                    flush=True,
                )
            elif res.get("skipped"):
                print(f"[Leave/WFH sync] {label} skipped: {res.get('message')}", flush=True)
            for w in res.get("warnings") or []:
                print(f"[Leave/WFH sync] {label} warning: {w}", flush=True)
            for err in res.get("create_errors") or []:
                print(f"[Leave/WFH sync] {label} create error: {err}", flush=True)
    except Exception as exc:
        print(f"[Leave/WFH sync] failed: {exc!r}", flush=True)
    finally:
        _leave_wfh_sync_lock.release()


def public_holiday_csv_sync():
    """Sync SNSoft Public Holiday Listing (Lark calendar) → ``holiday.csv``."""
    if not _holiday_sync_lock.acquire(blocking=False):
        print("[Holiday sync] skipped (already running)", flush=True)
        return
    try:
        from holiday_sync import sync_public_holidays_from_calendar

        result = sync_public_holidays_from_calendar()
        if result.get("ok"):
            print(
                f"[Holiday sync] {result['count']} row(s) from {result.get('calendar_title')!r} "
                f"via {result.get('auth') or '?'} "
                f"({', '.join(str(y) for y in result.get('years') or [])}) → {result.get('csv_path')}",
                flush=True,
            )
        else:
            print(f"[Holiday sync] failed: {result.get('error')}", flush=True)
            lst = result.get("list")
            if isinstance(lst, dict) and lst.get("search_hits"):
                print(f"[Holiday sync] search hits: {lst.get('search_hits')}", flush=True)
    except Exception as exc:
        print(f"[Holiday sync] error: {exc!r}", flush=True)
    finally:
        _holiday_sync_lock.release()


# def amountloss():
#     mention_line = f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>'
#     msg = mention_line + "\n" + "Hi Morning Shift kindly reminder to do Amount Loss~"
#     send_shift_reminder(DUTY_CHAT_ID, msg)
    
def myoseweeklymeeting():
    mention_line = f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>'
    msg = mention_line + "\n" + "MY OSE WEEKLY MEETING"
    send_shift_reminder(DUTY_CHAT_ID, msg)

scheduler = BackgroundScheduler()


def _add_scheduler_job(job_id: str, func, trigger: str, **trigger_kwargs) -> None:
    scheduler.add_job(
        func=func,
        trigger=trigger,
        id=job_id,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        **trigger_kwargs,
    )


# HRMS → leaveose / leave全员 / WFH Bitables — always on (no .env toggles).
# Full sync can take a few minutes; 06:40 finishes before the 07:00 morning OSE card.
_LEAVE_WFH_SYNC_INTERVAL_MIN = 60
_LEAVE_WFH_PRE_MORNING_HOUR = 6
_LEAVE_WFH_PRE_MORNING_MINUTE = 40


def _register_leave_wfh_sync_jobs() -> None:
    _add_scheduler_job(
        "ose_leave_wfh_calendar_sync_pre_morning",
        ose_leave_wfh_calendar_sync,
        "cron",
        hour=_LEAVE_WFH_PRE_MORNING_HOUR,
        minute=_LEAVE_WFH_PRE_MORNING_MINUTE,
    )
    _add_scheduler_job(
        "ose_leave_wfh_calendar_sync_interval",
        ose_leave_wfh_calendar_sync,
        "interval",
        minutes=_LEAVE_WFH_SYNC_INTERVAL_MIN,
        next_run_time=datetime.now(),
    )
    print(
        f"[Leave/WFH sync] always on: pre-morning "
        f"{_LEAVE_WFH_PRE_MORNING_HOUR:02d}:{_LEAVE_WFH_PRE_MORNING_MINUTE:02d} "
        f"+ every {_LEAVE_WFH_SYNC_INTERVAL_MIN} min (first run on startup); "
        f"syncs current month + all future HRMS months",
        flush=True,
    )


def _register_holiday_sync_jobs() -> None:
    _add_scheduler_job(
        "public_holiday_csv_sync_daily",
        public_holiday_csv_sync,
        "cron",
        hour=6,
        minute=35,
    )
    _add_scheduler_job(
        "public_holiday_csv_sync_startup",
        public_holiday_csv_sync,
        "interval",
        hours=24,
        next_run_time=datetime.now(),
    )
    print("[Holiday sync] daily 06:35 + every 24h (first run on startup)", flush=True)


# Lark leave/offset: clear in-process cache + prefetch before morning OSE card (same TZ as hour=7 job).
_add_scheduler_job("ose_leave_offset_daily_sync", ose_leave_offset_daily_sync, "cron", hour=6, minute=50)
_register_leave_wfh_sync_jobs()
_register_holiday_sync_jobs()
_add_scheduler_job("morning_reminder", morning_reminder, "cron", hour=7, minute=0)
_add_scheduler_job("evening_reminder", evening_reminder, "cron", hour=19, minute=0)
_add_scheduler_job("reminder_sheet_daily_sync", reminder_sheet_daily_sync, "cron", hour=0, minute=5)


def _jenkinsupdate_history_midnight_reset() -> None:
    """Clear jenkinsupdate.json (today's rebuild history) at 00:00."""
    try:
        ju = _get_jenkinsupdate()
        if ju and hasattr(ju, "clear_run_history_file"):
            ju.clear_run_history_file()
    except Exception as e:
        print(f"[jenkinsupdate] midnight history reset failed: {e!r}", flush=True)


_add_scheduler_job(
    "jenkinsupdate_history_midnight_reset",
    _jenkinsupdate_history_midnight_reset,
    "cron",
    hour=0,
    minute=0,
)
_offset_approver_poll_min = int(os.getenv("OSE_OFFSET_APPROVER_POLL_MIN", "3"))
_add_scheduler_job(
    "poll_offset_approver_notifications",
    poll_offset_approver_notifications_from_bitable,
    "interval",
    minutes=max(1, _offset_approver_poll_min),
)
try:
    from amountloss import amount_loss_9am_enabled as _amount_loss_9am_enabled

    if _amount_loss_9am_enabled():
        _add_scheduler_job("scheduled_amountloss_check", scheduled_amountloss_check, "cron", hour=9, minute=0)
    else:
        print(
            "[Amount Loss] 9:00 cron not registered (temporarily disabled; AMOUNT_LOSS_9AM_ENABLED=1 to restore)",
            flush=True,
        )
except ImportError:
    print("[Amount Loss] 9:00 cron not registered (amountloss unavailable)", flush=True)
_add_scheduler_job("myoseweeklymeeting", myoseweeklymeeting, "cron", day_of_week="tue", hour=17, minute=0)
# PLDT prefix rotation — every Tuesday 05:55 Asia/Manila (UTC+8, no DST).
_add_scheduler_job(
    "pldt_prefix_weekly_rotate",
    pldt_prefix_weekly_rotate,
    "cron",
    day_of_week="tue",
    hour=5,
    minute=55,
    timezone="Asia/Manila",
)
_add_scheduler_job("monthly_duty_check", monthly_duty_check, "cron", day=1, hour=0, minute=0)
# Weekly clear of /egs + /egstest sent logs (egs.json / egstest.json) — Monday 00:00 (GMT+8).
try:
    import maintenance_mail as _egs_mail_reset
    _add_scheduler_job(
        "egs_weekly_reset", _egs_mail_reset.egs_reset_stores, "cron",
        day_of_week="mon", hour=0, minute=0,
    )
except Exception as _egs_reset_err:
    print(f"[egs] weekly reset cron not registered: {_egs_reset_err!r}", flush=True)
PENDING_RESTART_FILE = "restart_pending.json"

def send_restart_ready():
    if not os.path.exists(PENDING_RESTART_FILE):
        return
    try:
        with open(PENDING_RESTART_FILE, "r") as f:
            data = json.load(f)
        chat_id = data.get("chat_id")
        timestamp_str = data.get("timestamp")
        ready_msg = (data.get("message") or "✅ Bot is ready.").strip() or "✅ Bot is ready."
        if chat_id and timestamp_str:
            timestamp = datetime.fromisoformat(timestamp_str)
            if (datetime.now() - timestamp).total_seconds() < 60:
                send_message(chat_id, ready_msg)
        os.remove(PENDING_RESTART_FILE)
    except Exception as e:
        print(f"❌ Failed to send restart ready: {e}")
        try:
            os.remove(PENDING_RESTART_FILE)
        except:
            pass


def write_restart_pending(chat_id, *, message: str | None = None):
    data = {
        "chat_id": chat_id,
        "timestamp": datetime.now().isoformat(),
    }
    if message:
        data["message"] = message
    try:
        with open(PENDING_RESTART_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"❌ Failed to write restart pending file: {e}")


def _restart_standalone_webapp() -> tuple[bool, str]:
    """Restart ``python3 webapp.py`` on the public dashboard port (default 8765)."""
    import subprocess

    root = _CHBOX_DIR
    port = int((os.getenv("WEBAPP_STANDALONE_PORT") or "8765").strip() or "8765")
    py = (os.getenv("WEBAPP_PYTHON") or "/root/miniconda3/bin/python3").strip()
    tmux_session = (os.getenv("WEBAPP_TMUX_SESSION") or "webapp").strip() or "webapp"
    log_path = os.path.join(root, "webapp.run.log")

    subprocess.run(
        ["fuser", "-k", f"{port}/tcp"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1)
    subprocess.run(
        ["tmux", "kill-session", "-t", tmux_session],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    shell_cmd = f"cd {root} && {py} webapp.py >> {log_path} 2>&1"
    proc = subprocess.run(
        ["tmux", "new", "-d", "-s", tmux_session, shell_cmd],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        return False, f"webapp tmux start failed ({err or proc.returncode})"

    time.sleep(5)
    listen = subprocess.run(["ss", "-lntp"], capture_output=True, text=True)
    if f":{port}" not in (listen.stdout or ""):
        return False, f"webapp not listening on :{port} (see {log_path})"
    return True, f"webapp restarted on :{port}"


def _schedule_larkbot_systemctl_restart(*, delay_sec: float = 2.0) -> None:
    """Restart ``larkbot.service`` after a short delay (current process will exit)."""
    import subprocess

    unit = (os.getenv("LARKBOT_SYSTEMD_UNIT") or "larkbot.service").strip() or "larkbot.service"
    delay = max(0.5, float(delay_sec))
    subprocess.Popen(
        ["bash", "-c", f"sleep {delay} && systemctl restart {unit}"],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _restart_larkbot_service(chat_id: str) -> None:
    """Restart ``larkbot.service`` only (no webapp)."""
    send_message(chat_id, "🔄 Restarting larkbot.service…")
    write_restart_pending(
        chat_id,
        message="✅ larkbot is ready.",
    )
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    _schedule_larkbot_systemctl_restart()


def _handle_restart_services(chat_id: str) -> None:
    ok, webapp_msg = _restart_standalone_webapp()
    lines = ["🔄 Restarting duty services…"]
    lines.append(f"✅ {webapp_msg}" if ok else f"⚠️ {webapp_msg}")
    lines.append("🔄 Restarting larkbot.service…")
    send_message(chat_id, "\n".join(lines))
    write_restart_pending(
        chat_id,
        message="✅ larkbot + webapp are ready.",
    )
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    _schedule_larkbot_systemctl_restart()

def get_bot_open_id():
    """Duty/Lark **bot** open_id via ``GET /open-apis/bot/v3/info`` (not ``users/me``)."""
    token = get_tenant_access_token()
    if not token:
        print("❌ Failed to get bot open_id: no tenant_access_token", flush=True)
        return None
    host = (os.getenv("LARK_HOST") or "https://open.larksuite.com").rstrip("/")
    url = f"{host}/open-apis/bot/v3/info"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=15).json()
    except Exception as ex:
        print(f"❌ Failed to get bot open_id: {ex!r}", flush=True)
        return None
    if resp.get("code") == 0:
        oid = ((resp.get("bot") or {}).get("open_id") or "").strip()
        if oid:
            return oid
    print("❌ Failed to get bot open_id:", resp, flush=True)
    return None

BOT_OPEN_ID = (
    os.getenv("BOT_OPEN_ID") or os.getenv("DUTY_BOT_OPEN_ID") or "ou_1f6596a9923a2a835918e7e2513595d5"
).strip()

_JENKINS_BOT_OPEN_ID_DEFAULT = "ou_45cc096780a23354f0719c9635765985"


def _jenkins_bot_open_id() -> str:
    return (os.getenv("JENKINS_BOT_OPEN_ID") or _JENKINS_BOT_OPEN_ID_DEFAULT).strip()


if not (os.getenv("BOT_OPEN_ID") or os.getenv("DUTY_BOT_OPEN_ID")):
    print(
        f"[lark] WARNING: BOT_OPEN_ID / DUTY_BOT_OPEN_ID unset — using built-in default {BOT_OPEN_ID!r} "
        "for self-message skip and @mention detection.",
        flush=True,
    )
if not (os.getenv("JENKINS_BOT_OPEN_ID") or "").strip():
    print(
        f"[lark] WARNING: JENKINS_BOT_OPEN_ID unset — using default {_JENKINS_BOT_OPEN_ID_DEFAULT!r}. "
        "Jenkinsbot → duty `/replyupdateemail` will fail if this open_id is wrong.",
        flush=True,
    )


def _mention_includes_duty_bot(mentions: list) -> bool:
    for mention in mentions or []:
        mention_id_obj = mention.get("id")
        if isinstance(mention_id_obj, dict):
            mention_id = mention_id_obj.get("open_id", "")
        else:
            mention_id = mention_id_obj
        if mention_id == BOT_OPEN_ID:
            return True
    return False


SECRET_COMMAND_ALLOWED_OPEN_ID = (
    os.getenv("SECRET_COMMAND_ALLOWED_OPEN_ID", "").strip()
    or "ou_5f660c0fb0769d184aca635d02209272"
)


def _secret_command_allowed(sender_open_id: Optional[str]) -> bool:
    return (sender_open_id or "").strip() == SECRET_COMMAND_ALLOWED_OPEN_ID


def _extract_tagged_users_from_message(
    original_text: str, mentions: list, *, exclude_bot: bool = True
) -> list[tuple[str, str]]:
    """Return ``[(display_name, open_id), ...]`` for @-tagged users in a Lark IM message."""
    results: list[tuple[str, str]] = []
    seen: set[str] = set()
    bot_oid = BOT_OPEN_ID if exclude_bot else ""

    for match in re.finditer(
        r'<at (?:open_id|user_id)="([^"]+)"[^>]*>([^<]*)</at>',
        original_text or "",
        re.I,
    ):
        open_id = match.group(1).strip()
        name = (match.group(2) or "user").strip() or "user"
        if not open_id or open_id in seen:
            continue
        if exclude_bot and open_id == bot_oid:
            continue
        seen.add(open_id)
        results.append((name, open_id))

    for m in mentions or []:
        mention_id = m.get("id", {})
        if isinstance(mention_id, dict):
            open_id = (
                mention_id.get("open_id") or mention_id.get("user_id") or ""
            ).strip()
        else:
            open_id = str(mention_id or "").strip()
        if not open_id or open_id in seen:
            continue
        if exclude_bot and open_id == bot_oid:
            continue
        key = m.get("key", "")
        km = re.search(r"<at[^>]*>(.*?)</at>", key, re.I)
        name = (km.group(1) if km else m.get("name") or "user").strip() or "user"
        seen.add(open_id)
        results.append((name, open_id))

    return results


def _format_open_id_lookup_reply(tagged: list[tuple[str, str]]) -> str:
    lines: list[str] = []
    for name, open_id in tagged:
        lines.append(f"Tagged {name} with open_id: {open_id}")
        lines.append(f'Mention: <at user_id="{open_id}">{name}</at>')
    return "\n".join(lines)


def _handle_secret_open_id_lookup(original_text: str, mentions: list) -> str:
    tagged = _extract_tagged_users_from_message(original_text, mentions)
    if not tagged:
        return "❌ No user mentioned correctly. Use `/secret1 @user` (mention the user)."
    return _format_open_id_lookup_reply(tagged)


_GIT_PULL_RESTART_RE = re.compile(
    r"(?i)(?:"
    r"git\s+pull(?:\s+and|\s*[,，]?\s*then)?\s+(?:restart|reboot)\s+(?:the\s+)?(?:service|services|bot|larkbot|duty\s+bot|webapp)"
    r"|git\s+pull\s+(?:and\s+)?(?:restart|reboot)\b"
    r"|(?:pull|update)\s+(?:code|repo|git)\s+(?:and\s+)?(?:restart|reboot)\s+(?:the\s+)?(?:service|services|bot|larkbot|webapp)"
    r"|(?:deploy|update)\s+(?:the\s+)?(?:bot|code|server|osedutybot)\b"
    r"|拉代码.*重启|部署.*重启"
    r")"
)


def looks_like_git_pull_restart(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    if t.lower() in ("/deploy", "/gitpullrestart"):
        return True
    try:
        import commandagent as _ca

        return _ca.looks_like_git_pull_restart(t)
    except Exception:
        return bool(_GIT_PULL_RESTART_RE.search(t))


def _git_pull_deploy_script() -> str:
    root = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root, "deploy", "git-pull-keep-local-json.sh")


def _run_git_pull_and_restart(chat_id: str) -> None:
    import subprocess

    root = os.path.dirname(os.path.abspath(__file__))
    script = _git_pull_deploy_script()
    if not os.path.isfile(script):
        send_message(chat_id, f"❌ Deploy script not found: `{script}`")
        return
    try:
        proc = subprocess.run(
            ["bash", script, root],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=600,
        )
    except subprocess.TimeoutExpired:
        send_message(chat_id, "❌ git pull timed out after 10 minutes.")
        return
    except Exception as exc:
        send_message(chat_id, f"❌ git pull failed: {exc!r}")
        return

    combined = "\n".join(x for x in (proc.stdout, proc.stderr) if x).strip()
    tail = combined[-1200:] if len(combined) > 1200 else combined
    if proc.returncode != 0:
        send_message(
            chat_id,
            f"❌ git pull failed (exit {proc.returncode}).\n```\n{tail}\n```",
        )
        return

    summary = tail[-600:] if tail else "done"
    send_message(chat_id, f"✅ git pull OK.\n```\n{summary}\n```")
    _restart_larkbot_service(chat_id)


def _handle_git_pull_restart_deploy(chat_id: str) -> None:
    send_message(chat_id, "⏳ git pull origin main + restart larkbot…")
    start_lark_background_thread(_run_git_pull_and_restart, chat_id)


def _run_jenkins_warm_status_check(chat_id: str) -> None:
    try:
        import jenkinsupdate as _ju_status

        report = _ju_status.jenkins_warm_pool_status_report()
    except Exception as exc:
        send_message(chat_id, f"❌ warm status check failed: {exc!r}")
        return
    send_message(chat_id, f"🌡️ **Jenkins warm browser status**\n{report}")


def _handle_jenkins_warm_status(chat_id: str) -> None:
    start_lark_background_thread(_run_jenkins_warm_status_check, chat_id)


def _dispatch_jenkins_duty_command(
    chat_id: str,
    sender_id: str,
    duty_clean: str,
    duty_orig: str,
    send,
    *,
    message_content_raw: str = "",
) -> bool:
    """Handle jenkinsbot → duty bot (``/replyupdateemail``, etc.) with or without jenkinsupdate."""
    ju = _get_jenkinsupdate()
    if ju is None:
        try:
            import updatemore as um

            if um.is_jenkinsbot_duty_command(duty_orig or duty_clean or message_content_raw):
                send(
                    chat_id,
                    "⚠️ **jenkinsupdate** is not loaded (e.g. Playwright missing). "
                    "`/replyupdateemail` can still send a **single** email if parsed, but "
                    "**`/updatemore` batching** needs jenkinsupdate. Fix the server import error "
                    "and set `JENKINS_BOT_OPEN_ID` in `.env`.",
                )
        except Exception:
            pass
    if ju:
        try:
            import updatemore as um

            return um.handle_jenkinsbot_callback(
                chat_id,
                sender_id,
                duty_clean,
                duty_orig,
                send,
                sessions=ju._fpms_lark_sessions,
                sessions_lock=ju._fpms_lark_sessions_lock,
                session_key_fn=ju._fpms_lark_session_key,
                dispatch_update_body=lambda cid, sk, body, snd, **kw: ju._dispatch_lark_update_command_body(
                    cid, sk, body, snd, **kw
                ),
                message_content_raw=message_content_raw,
            )
        except Exception as ex:
            print(f"[lark] jenkins duty via jenkinsupdate failed: {ex!r}", flush=True)
    try:
        import updatemore as um

        empty_lock = threading.Lock()
        return um.handle_jenkinsbot_callback(
            chat_id,
            sender_id,
            duty_clean,
            duty_orig,
            send,
            sessions={},
            sessions_lock=empty_lock,
            session_key_fn=lambda cid, sid: f"{(cid or '').strip()}:{(sid or '').strip()}",
            dispatch_update_body=lambda *a, **kw: False,
            message_content_raw=message_content_raw,
        )
    except Exception as ex:
        print(f"[lark] jenkins duty fallback failed: {ex!r}", flush=True)
        return False


def _lark_flatten_rich_content(obj) -> str:
    """Collect plain text from Lark post / rich ``content`` JSON."""
    parts: list[str] = []
    if isinstance(obj, str):
        s = obj.strip()
        if s:
            parts.append(s)
    elif isinstance(obj, dict):
        if str(obj.get("tag") or "").lower() == "text":
            t = obj.get("text")
            if isinstance(t, str) and t.strip():
                parts.append(t.strip())
        else:
            for key in ("text", "title", "content"):
                if key in obj:
                    sub = _lark_flatten_rich_content(obj[key])
                    if sub:
                        parts.append(sub)
    elif isinstance(obj, list):
        for item in obj:
            sub = _lark_flatten_rich_content(item)
            if sub:
                parts.append(sub)
    return " ".join(parts)


def _lark_extract_message_text(content_str: str) -> str:
    """Parse ``im.message`` ``content`` JSON — text, post, and rich variants."""
    raw = (content_str or "").strip()
    if not raw:
        return ""
    try:
        content = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if not isinstance(content, dict):
        return str(content)
    plain = content.get("text")
    if isinstance(plain, str) and plain.strip():
        return plain.strip()
    for locale in ("zh_cn", "en_us", "ja_jp", "zh_hk", "en", "zh"):
        block = content.get(locale)
        if isinstance(block, dict):
            flat = _lark_flatten_rich_content(block.get("content"))
            if flat.strip():
                return flat.strip()
    flat_all = _lark_flatten_rich_content(content)
    return flat_all.strip()


def _lark_extract_image_keys(
    content_str: str, *, message_type: Optional[str] = None
) -> list[str]:
    """Collect Lark ``image_key`` values from image or post/rich messages."""
    keys: list[str] = []
    raw = (content_str or "").strip()
    if not raw:
        return keys
    try:
        content = json.loads(raw)
    except json.JSONDecodeError:
        return keys
    if not isinstance(content, dict):
        return keys

    def _add_key(value) -> None:
        key = str(value or "").strip()
        if key and key not in keys:
            keys.append(key)

    if (message_type or "").strip().lower() == "image":
        _add_key(content.get("image_key"))
        return keys

    _add_key(content.get("image_key"))

    def _walk(obj) -> None:
        if isinstance(obj, dict):
            if str(obj.get("tag") or "").lower() == "img":
                _add_key(obj.get("image_key"))
            for value in obj.values():
                _walk(value)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(content)
    return keys


def _lark_full_message_body(
    original_text: str, clean_text: str, message_content_raw: str
) -> str:
    """Best-effort full user text (multi-line post / Branch: blocks), not mention-stripped one-liner."""
    for candidate in (original_text, clean_text):
        c = (candidate or "").replace("\r\n", "\n").strip()
        if not c:
            continue
        low = c.casefold()
        if "branch:" in low or "services:" in low or "missing credit" in low:
            return c
        if len(c.splitlines()) >= 2:
            return c
    flat = _lark_extract_message_text(message_content_raw or "")
    if flat.strip():
        return flat.strip()
    return (clean_text or original_text or "").strip()


def _parse_missing_credit_alert(text: str) -> Optional[dict]:
    raw = (text or "").replace("\r\n", "\n")
    if not re.search(r"(?i)(?:type\s*:\s*)?missing\s+credit", raw):
        return None
    out: dict[str, str] = {}
    m = re.search(r"(?im)^\s*account\s*:\s*(\d+)", raw)
    if m:
        out["account"] = m.group(1)
    m = re.search(r"(?im)^\s*amount\s+missing\s*:\s*([\d.]+)", raw)
    if m:
        out["amount"] = m.group(1)
    m = re.search(
        r"(?im)withdrawal\s+time\s*:\s*(\d{4})[/-](\d{2})[/-](\d{2})(?:\s+\d{2}:\d{2}:\d{2})?",
        raw,
    )
    if m:
        out["date_iso"] = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"(?im)proposal\s+withdrawal\s*:\s*(\S+)", raw)
    if m:
        out["proposal"] = m.group(1)
    return out or None


def _looks_like_jenkins_nl_update(text: str) -> bool:
    try:
        ju = _get_jenkinsupdate()
        if ju is not None:
            return bool(ju.looks_like_natural_jenkins_update(text))
        import jenkinsupdate as _ju

        return bool(_ju.looks_like_natural_jenkins_update(text))
    except Exception:
        raw = (text or "").replace("\r\n", "\n")
        low = raw.casefold()
        return bool(
            re.search(r"(?im)^\s*branch\s*:", raw)
            and re.search(r"(?im)^\s*services?\s*:", raw)
            and re.search(r"(?i)update|uat|jenkins|rc[\s-]*uat|部署|更新", raw)
        )


def _try_missing_credit_inquiry(
    chat_id: str,
    body: str,
    *,
    bot_mentioned: bool,
    message_id: Optional[str],
    send_func,
) -> bool:
    if not bot_mentioned:
        return False
    parsed = _parse_missing_credit_alert(body)
    if not parsed:
        return False
    lines = [
        "📋 **Missing Credit alert parsed**",
        f"• Account: `{parsed.get('account', '?')}`",
        f"• Amount missing: `{parsed.get('amount', '?')}`",
        f"• Withdrawal date: `{parsed.get('date_iso', '?')}`",
    ]
    if parsed.get("proposal"):
        lines.append(f"• Proposal: `{parsed['proposal']}`")
    lines.append(
        "\n⏳ I need the **machine type** (e.g. `NWR2074`) to scan logs. "
        "Fill the form below — player/date are pre-filled when possible."
    )
    send_func(chat_id, "\n".join(lines))
    try:
        import checkcredit

        card = checkcredit.build_checkcredit_player_form_card()
        send_func(chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive")
    except Exception as ex:
        acct = parsed.get("account") or ""
        dt = parsed.get("date_iso") or ""
        send_func(
            chat_id,
            f"Use `@Duty Bot /checkcreditdate <machine>` with player `{acct}` date `{dt}`.",
        )
        print(f"[missing-credit] card failed: {ex!r}", flush=True)
    print(f"[missing-credit] parsed {parsed!r}", flush=True)
    return True


def download_lark_message_image(
    message_id: str, file_key: str
) -> Optional[tuple[str, bytes]]:
    """Download an image resource from a Lark message. Returns (mime_type, bytes)."""
    mid = (message_id or "").strip()
    fk = (file_key or "").strip()
    if not mid or not fk:
        return None
    token = get_tenant_access_token()
    if not token:
        print("[lark] image download skipped: no tenant_access_token", flush=True)
        return None
    url = (
        "https://open.larksuite.com/open-apis/im/v1/messages/"
        f"{mid}/resources/{fk}"
    )
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers, params={"type": "image"}, timeout=90)
    except Exception as ex:
        print(f"[lark] image download request failed: {ex!r}", flush=True)
        return None
    if resp.status_code != 200:
        preview = (resp.text or "")[:300]
        print(
            f"[lark] image download HTTP {resp.status_code} message_id={mid!r} "
            f"file_key={fk!r} body={preview!r}",
            flush=True,
        )
        return None
    ctype = (resp.headers.get("Content-Type") or "image/jpeg").split(";")[0].strip().lower()
    if not ctype.startswith("image/"):
        ctype = "image/jpeg"
    data = resp.content
    if not data:
        return None
    return ctype, data


def _try_lark_vision_reply(
    chat_id: str,
    message_id: Optional[str],
    message_content_raw: str,
    *,
    message_type: Optional[str],
    user_text: str,
    bot_mentioned: bool,
    send_func,
    session_key: Optional[str] = None,
) -> bool:
    """Download image(s) from Lark and reply via vision LLM. Returns True if handled."""
    if not bot_mentioned:
        return False
    if (user_text or "").lstrip().startswith("/"):
        return False
    image_keys = _lark_extract_image_keys(
        message_content_raw, message_type=message_type
    )
    if not image_keys:
        return False
    mid = (message_id or "").strip()
    if not mid:
        return False
    try:
        import chatagent as _chatagent

        if not (
            _chatagent.is_enabled()
            and _chatagent.vision_enabled()
            and _chatagent.llm_available()
        ):
            send_func(
                chat_id,
                "ℹ️ Image recognition needs `BOT_USE_CHATAGENT=1` and a vision-capable model "
                "(e.g. `qwen3.5:9b` on Ollama).",
            )
            return True
    except Exception as ex:
        print(f"[lark] vision precheck failed: {ex!r}", flush=True)
        return False

    images: list[tuple[str, bytes]] = []
    for key in image_keys[: int(os.getenv("BOT_CHAT_VISION_MAX_IMAGES", "3") or "3")]:
        got = download_lark_message_image(mid, key)
        if got:
            images.append(got)
    if not images:
        send_func(
            chat_id,
            "⚠️ Could not download the image from Lark.\n"
            "Check the bot app has permission to read message resources "
            "(im:message / download message images in Lark developer console).",
        )
        return True

    print(
        f"[lark] vision: {len(images)} image(s) keys={image_keys[:3]!r} "
        f"prompt={user_text!r}",
        flush=True,
    )
    try:
        import chatagent as _chatagent

        vis_reply = _chatagent.reply_with_images(
            user_text or "", images, session_key=session_key
        )
    except Exception as ex:
        print(f"[lark] vision LLM failed: {ex!r}", flush=True)
        send_func(chat_id, f"⚠️ Image recognition failed: {ex}")
        return True
    if vis_reply:
        send_func(chat_id, vis_reply)
        print(f"🖼️ Vision reply to chat {chat_id}", flush=True)
    else:
        send_func(
            chat_id,
            "⚠️ Could not analyze the image (model returned empty). "
            "Try a smaller screenshot or ask again.",
        )
    return True


processed_messages = set()
processed_lock = threading.Lock()
_MAX_PROCESSED_MESSAGE_IDS = 50_000
_PROCESSED_PRUNE_CHUNK = 10_000
# Lark WebSocket may redeliver recent events after reconnect; in-memory dedup is cleared on restart.
_BOT_STARTED_AT_MS = int(time.time() * 1000)


def _lark_event_create_time_ms(data: dict) -> Optional[int]:
    """Best-effort event/message timestamp (ms) from Lark schema 2.0 or legacy callback."""
    if not isinstance(data, dict):
        return None
    hdr = data.get("header")
    if isinstance(hdr, dict):
        ct = hdr.get("create_time")
        if ct is not None:
            try:
                return int(ct)
            except (TypeError, ValueError):
                pass
    ev = data.get("event")
    if isinstance(ev, dict):
        msg = ev.get("message")
        if isinstance(msg, dict):
            ct = msg.get("create_time")
            if ct is not None:
                try:
                    return int(ct)
                except (TypeError, ValueError):
                    pass
        ct = ev.get("create_time")
        if ct is not None:
            try:
                return int(ct)
            except (TypeError, ValueError):
                pass
    return None


def _lark_skip_stale_event_on_startup(data: dict) -> bool:
    """Ignore events that happened before this process started (replay on WS reconnect)."""
    skip = (os.getenv("LARK_SKIP_STALE_ON_STARTUP") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )
    if not skip:
        return False
    try:
        grace_ms = int(os.getenv("LARK_STARTUP_STALE_GRACE_MS", "10000"))
    except ValueError:
        grace_ms = 10_000
    created_ms = _lark_event_create_time_ms(data)
    if created_ms is None:
        return False
    return created_ms < _BOT_STARTED_AT_MS - grace_ms


def _remember_processed_message_id(message_id: str) -> bool:
    """Record ``message_id``; return True if it was already seen (duplicate)."""
    if not message_id:
        return False
    with processed_lock:
        if message_id in processed_messages:
            return True
        if len(processed_messages) >= _MAX_PROCESSED_MESSAGE_IDS:
            for _ in range(_PROCESSED_PRUNE_CHUNK):
                try:
                    processed_messages.pop()
                except KeyError:
                    break
        processed_messages.add(message_id)
        return False


def _feishu_decrypt_encrypt_field(ciphertext_b64: str, encrypt_key: str) -> str:
    """Decrypt Feishu ``encrypt`` field (AES-256-CBC + PKCS7), same algorithm as open-platform samples."""
    import base64
    import hashlib

    try:
        from Crypto.Cipher import AES
    except ImportError as e:
        raise ImportError("pip install pycryptodome") from e

    bs = AES.block_size
    key = hashlib.sha256(encrypt_key.encode("utf-8")).digest()
    enc = base64.b64decode(ciphertext_b64)
    iv = enc[:bs]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    raw = cipher.decrypt(enc[bs:])
    pad_len = raw[-1]
    if pad_len < 1 or pad_len > bs:
        raise ValueError("invalid PKCS7 padding")
    raw = raw[:-pad_len]
    return raw.decode("utf-8")


def _feishu_maybe_decrypt_webhook_payload(raw):
    # type: (Optional[Dict]) -> Optional[Dict]
    """
    **Optional.** Only needed when you turn **on** 「Encrypt Key / 加密」 under Feishu/Lark
    **开发者后台 → 事件与回调**. Then POST bodies look like ``{\"encrypt\": \"...\"}`` and must be
    decrypted before ``header.token`` is readable.

    If encryption is **off** (default), requests are plain JSON — do **not** set ``LARK_ENCRYPT_KEY``
    (you can omit ``ENCRYPT_KEY`` / ``FEISHU_ENCRYPT_KEY`` entirely).

    When encryption **is** on, set ``LARK_ENCRYPT_KEY`` (or ``ENCRYPT_KEY``) to the same Encrypt Key
    shown in the console, and ``pip install pycryptodome``.
    """
    if not isinstance(raw, dict) or "encrypt" not in raw:
        return raw
    ek = (
        os.getenv("LARK_ENCRYPT_KEY")
        or os.getenv("ENCRYPT_KEY")
        or os.getenv("FEISHU_ENCRYPT_KEY")
        or ""
    ).strip()
    if not ek:
        print(
            "[lark] POST body has `encrypt` but LARK_ENCRYPT_KEY is unset — "
            "either set it to match 事件与回调 → Encrypt Key, **or** turn off encryption there "
            "if you did not intend to use it.",
            flush=True,
        )
        return raw
    try:
        plain = _feishu_decrypt_encrypt_field(str(raw["encrypt"]), ek)
        if plain.startswith("\ufeff"):
            plain = plain.lstrip("\ufeff")
        return json.loads(plain)
    except ImportError as ex:
        print(f"[lark] {ex} — encrypted webhooks disabled until installed.", flush=True)
        return raw
    except Exception as ex:
        print(f"[lark] decrypt webhook failed: {ex!r}", flush=True)
        return raw


def _lark_is_schema_v2(data):
    # type: (Optional[dict]) -> bool
    """Schema may arrive as str ``2.0`` or occasionally non-string — avoid wrong token branch."""
    if not isinstance(data, dict):
        return False
    s = data.get("schema")
    return s == "2.0" or str(s).strip() == "2.0"


def _lark_looks_like_lark_card_update_credential(token_str):
    # type: (str) -> bool
    """
    Legacy ``card.action.trigger_v1`` flat payloads use top-level ``token`` for **updating the card**
    (credential like ``c-xxxxx``), **not** the app Verification Token — see Lark docs
    "Message Card Callback Interaction" (trigger_v1). Do not use that field for verification compare.
    """
    s = (token_str or "").strip()
    if not s:
        return False
    return s.startswith("c-") or s.startswith("d-")


def _lark_extract_verification_token(data):
    # type: (Optional[dict]) -> Optional[str]
    """
    App **Verification Token**: schema 2.0 uses ``header.token``; some payloads use ``verification_token``.

    **Do not** treat top-level ``token`` as verification when it is the **card update credential**
    (see :func:`_lark_looks_like_lark_card_update_credential`).
    """
    if not isinstance(data, dict):
        return None
    h = data.get("header")
    if isinstance(h, dict):
        for key in ("token", "Token", "verification_token"):
            t = h.get(key)
            if t is not None:
                ts = str(t).strip()
                # header.token in schema 2.0 envelope is the app verification token
                return ts
    vt = data.get("verification_token")
    if vt is not None:
        return str(vt).strip()
    t2 = data.get("token")
    if t2 is None:
        return None
    ts = str(t2).strip()
    if _lark_looks_like_lark_card_update_credential(ts):
        return None
    return ts


def _lark_is_legacy_card_trigger_v1_flat(data):
    # type: (object) -> bool
    """
    Earlier **card.action.trigger_v1** body shape (flat JSON, no ``schema`` / ``event`` envelope):
    ``open_id``, ``open_message_id``, ``action``, top-level ``token`` = card credential — Lark docs 2024.
    """
    if not isinstance(data, dict):
        return False
    if data.get("encrypt") is not None:
        return False
    # Already normalized or schema 2 envelope with header.event_type
    het = _lark_header_event_type(data)
    if het.startswith("card.action"):
        return False
    if isinstance(data.get("header"), dict) and data["header"].get("event_type"):
        return False
    if not isinstance(data.get("action"), dict):
        return False
    return bool(data.get("open_message_id") or data.get("open_id"))


def _lark_normalize_legacy_card_trigger_v1_flat(data):
    # type: (object) -> object
    """
    Map flat ``trigger_v1`` POST body into the same shape as schema-2 ``event`` + ``header.event_type``
    so :func:`_lark_resolve_card_action` and Jenkins handlers work.
    """
    if not isinstance(data, dict) or not _lark_is_legacy_card_trigger_v1_flat(data):
        return data
    ev = {
        "operator": {},
        "action": data.get("action"),
        "context": {},
    }
    oid = data.get("open_id")
    if oid:
        ev["operator"]["open_id"] = str(oid).strip()
    uid = data.get("union_id")
    if uid:
        ev["operator"]["union_id"] = str(uid).strip()
    ocid = data.get("open_chat_id") or data.get("chat_id")
    if ocid:
        ev["open_chat_id"] = str(ocid).strip()
        ev["context"]["open_chat_id"] = str(ocid).strip()
    omid = data.get("open_message_id")
    if omid:
        ev["context"]["open_message_id"] = str(omid).strip()
    data["event"] = ev
    hdr = data.get("header") if isinstance(data.get("header"), dict) else {}
    hdr["event_type"] = "card.action.trigger_v1"
    hdr["event_id"] = hdr.get("event_id") or str(omid or "")[:80]
    data["header"] = hdr
    data["schema"] = "2.0"
    print(
        "[lark] normalized legacy flat card.action.trigger_v1 body → schema-shaped event "
        "(open_message_id=%r)"
        % (omid,),
        flush=True,
    )
    return data


def _lark_http_empty_json_ok():
    # type: () -> Response
    """Feishu card callbacks: HTTP 200 + JSON ``{}`` (see handle-card-callbacks doc)."""
    return jsonify({})


def _lark_http_card_callback_ok():
    # type: () -> Response
    """
    Feishu ``card.action.trigger``: HTTP **200** + JSON body within ~**3s** (doc: empty ``{}`` or ``toast``/``card``).

    Default: literal ``{}`` — smallest surface for **200672** (wrong body format).
    Set ``LARK_CARD_ACK_TOAST=1`` for a minimal success ``toast`` (ASCII-only ``content``).
    """
    print("[lark] HTTP 200 card ACK (instant)", flush=True)
    if (os.getenv("LARK_CARD_ACK_TOAST") or "").strip() == "1":
        body = json.dumps(
            {
                "toast": {
                    "type": "success",
                    "content": "OK",
                    "i18n": {"en_us": "OK", "zh_cn": "OK"},
                }
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )
        return Response(body, status=200, mimetype="application/json")
    return Response(b"{}", status=200, mimetype="application/json")


def _lark_http_card_callback_response(body: dict) -> Response:
    """Return card.callback body (toast and/or in-place card update) within the 3s window."""
    print(f"[lark] HTTP 200 card callback response keys={list(body.keys())!r}", flush=True)
    return Response(
        json.dumps(body, ensure_ascii=False),
        status=200,
        mimetype="application/json",
    )


def _lark_parse_card_action_value(val):
    # type: (object) -> Optional[dict]
    """Decode ``event.action.value`` (object or JSON string)."""
    if val is None:
        return None
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            o = json.loads(s)
            return o if isinstance(o, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _lark_form_field_text(v):
    # type: (object) -> str
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (int, float)):
        return str(v).strip()
    if isinstance(v, list):
        parts = []
        for x in v:
            t = _lark_form_field_text(x)
            if t:
                parts.append(t)
        return " ".join(parts).strip()
    if isinstance(v, dict):
        if "hour" in v and "minute" in v:
            try:
                hh = int(v.get("hour"))
                mm = int(v.get("minute"))
                if 0 <= hh <= 23 and 0 <= mm <= 59:
                    return f"{hh:02d}:{mm:02d}"
            except Exception:
                pass
        for k in ("value", "text", "content", "date", "time", "datetime"):
            t = _lark_form_field_text(v.get(k))
            if t:
                return t
        # Fallback: first non-empty field value
        for vv in v.values():
            t = _lark_form_field_text(vv)
            if t:
                return t
    return ""


def _lark_get_card_form_field(action_obj, name):
    # type: (object, str) -> str
    if not isinstance(action_obj, dict):
        return ""
    fv = action_obj.get("form_value")
    if not isinstance(fv, dict):
        return ""
    return _lark_form_field_text(fv.get(name))


def _lark_find_field_deep(obj, name):
    # type: (object, str) -> str
    if isinstance(obj, dict):
        if name in obj:
            t = _lark_form_field_text(obj.get(name))
            if t:
                return t
        for vv in obj.values():
            t = _lark_find_field_deep(vv, name)
            if t:
                return t
    elif isinstance(obj, list):
        for it in obj:
            t = _lark_find_field_deep(it, name)
            if t:
                return t
    return ""


def _normalize_rem_add_date_field(raw: str) -> str:
    s = str(raw or "").strip()
    if re.match(r"^\d{10,13}$", s):
        try:
            ts = int(s)
            if ts > 10**12:
                ts = ts // 1000
            return datetime.fromtimestamp(ts).strftime("%Y/%m/%d")
        except Exception:
            return s
    m = re.match(r"^\s*(\d{4})-(\d{2})-(\d{2})(?:\s+.*)?$", s)
    if m:
        return f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
    return s


def _parse_rem_add_form_fields(ev_ca: dict, parsed_ca: dict) -> dict[str, Any]:
    """Extract add-reminder form values from a card callback payload."""
    act_ca = ev_ca.get("action") if isinstance(ev_ca.get("action"), dict) else {}
    start_raw = _lark_get_card_form_field(act_ca, "start_date")
    end_raw = _lark_get_card_form_field(act_ca, "end_date")
    time_raw = _lark_get_card_form_field(act_ca, "time")
    reason = _lark_get_card_form_field(act_ca, "reason")
    if isinstance(parsed_ca, dict):
        fv_rem = parsed_ca.get("form_value")
        if isinstance(fv_rem, dict):
            start_raw = start_raw or _lark_form_field_text(fv_rem.get("start_date"))
            end_raw = end_raw or _lark_form_field_text(fv_rem.get("end_date"))
            time_raw = time_raw or _lark_form_field_text(fv_rem.get("time"))
            reason = reason or _lark_form_field_text(fv_rem.get("reason"))
        start_raw = start_raw or _lark_form_field_text(parsed_ca.get("start_date"))
        end_raw = end_raw or _lark_form_field_text(parsed_ca.get("end_date"))
        time_raw = time_raw or _lark_form_field_text(parsed_ca.get("time"))
        reason = reason or _lark_form_field_text(parsed_ca.get("reason"))
    start_raw = start_raw or _lark_find_field_deep(ev_ca, "start_date")
    end_raw = end_raw or _lark_find_field_deep(ev_ca, "end_date")
    time_raw = time_raw or _lark_find_field_deep(ev_ca, "time")
    reason = reason or _lark_find_field_deep(ev_ca, "reason")
    time_raw = (time_raw or "").strip() or _lark_find_field_deep(ev_ca, "time_preset").strip()
    fv_ca = act_ca.get("form_value") if isinstance(act_ca, dict) else {}
    when_labels_cb: list[str] = []
    if isinstance(fv_ca, dict):
        when_labels_cb = reminder.parse_when_form_value(fv_ca.get("when"))
    if isinstance(parsed_ca, dict) and isinstance(parsed_ca.get("form_value"), dict) and not when_labels_cb:
        when_labels_cb = reminder.parse_when_form_value(parsed_ca["form_value"].get("when"))
    if not when_labels_cb:
        when_labels_cb = reminder.parse_when_form_value(_lark_find_field_deep(ev_ca, "when"))
    start_raw = _normalize_rem_add_date_field(start_raw)
    end_raw = _normalize_rem_add_date_field(end_raw)
    m24 = re.match(r"^\s*(\d{1,2}):(\d{2})(?::\d{2})?\s*$", time_raw or "")
    if m24:
        hh = int(m24.group(1))
        mm = int(m24.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            ap = "AM" if hh < 12 else "PM"
            hh12 = hh % 12
            if hh12 == 0:
                hh12 = 12
            time_raw = f"{hh12}:{mm:02d}{ap}"
    return {
        "start_raw": (start_raw or "").strip(),
        "end_raw": (end_raw or "").strip(),
        "time_raw": (time_raw or "").strip(),
        "reason": (reason or "").strip(),
        "when_labels": when_labels_cb,
    }


def _try_rem_add_submit_card_response(parsed_ca: dict, ev_ca: dict, chat_id_ca: str) -> Optional[dict]:
    """Synchronous card.callback body for add-reminder form submit (keeps card in place)."""
    if str(parsed_ca.get("k") or "").strip().lower() != "rem_add_submit":
        return None
    fields = _parse_rem_add_form_fields(ev_ca, parsed_ca)
    if not (
        fields["start_raw"]
        and fields["end_raw"]
        and fields["time_raw"]
        and fields["reason"]
    ):
        return {
            "toast": {
                "type": "error",
                "content": "Please fill all fields: Start Date, End Date, Time, Reason.",
            }
        }
    when_labels = fields["when_labels"] or None
    result = reminder.add_sheet_reminder(
        start_raw=str(fields["start_raw"]),
        end_raw=str(fields["end_raw"]),
        time_raw=str(fields["time_raw"]),
        reason=str(fields["reason"]),
        get_token_func=get_tenant_access_token,
        scheduler=scheduler,
        send_func=send_message,
        chat_id=chat_id_ca,
        target_user_id=TARGET_USER_OPEN_ID,
        schedule_chat_id=REMINDER_TARGET_CHAT_ID,
        when_labels=when_labels if isinstance(when_labels, list) and when_labels else None,
        emit_chat_card=False,
    )
    if isinstance(result, str):
        err = (result or "").strip() or "Failed to add reminder."
        if err.startswith("❌"):
            err = err[1:].strip()
        return {"toast": {"type": "error", "content": err}}
    return {
        "card": {"type": "raw", "data": reminder.build_reminder_added_card_v2(result)},
        "toast": {"type": "success", "content": "Reminder added"},
    }


def _lark_test_card_json() -> str:
    """Minimal interactive card for ``/test`` — button triggers ``k=test_hi`` card callback."""
    card = {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "Message card test"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": "Tap **Say hi** — the bot will **@ you** with **hi**.",
                    },
                },
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "Say hi"},
                    "type": "primary",
                    "behaviors": [{"type": "callback", "value": {"k": "test_hi"}}],
                },
            ],
        },
    }
    return json.dumps(card, ensure_ascii=False)


def _lark_safe_parse_json_body(req):
    # type: (object) -> Optional[dict]
    """Prefer ``get_json``; fallback to raw body (some proxies strip / alter Content-Type)."""
    raw = req.get_json(silent=True)
    if isinstance(raw, dict):
        return raw
    b = req.get_data(cache=False)
    if not b:
        return None
    if b.startswith(b"\xef\xbb\xbf"):
        b = b[3:]
    try:
        parsed = json.loads(b.decode("utf-8"))
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _lark_coerce_event_dict(data):
    # type: (object) -> object
    """Some gateways deliver ``event`` as a JSON string — normalize to a dict."""
    if not isinstance(data, dict):
        return data
    ev = data.get("event")
    if isinstance(ev, str):
        try:
            parsed = json.loads(ev)
            data["event"] = parsed if isinstance(parsed, dict) else {}
        except Exception:
            data["event"] = {}
    elif ev is None and isinstance(data, dict):
        het = _lark_header_event_type(data)
        if het.startswith("card.action"):
            data["event"] = {}
        elif _lark_is_schema_v2(data) and isinstance(data.get("action"), dict):
            # SDK-flat card callback (no ``event`` yet) — same trigger as :func:`_lark_should_merge_flat_card_callback`
            data["event"] = {}
    return data


def _lark_should_merge_flat_card_callback(data):
    # type: (dict) -> bool
    """True when payload is (or looks like) ``card.action.trigger`` including SDK-flat shapes."""
    if not isinstance(data, dict):
        return False
    et = _lark_header_event_type(data)
    if et.startswith("card.action"):
        return True
    # Schema 2.0 + top-level ``action`` (gateway stripped ``event`` / ``header.event_type``).
    if _lark_is_schema_v2(data) and isinstance(data.get("action"), dict):
        return True
    return False


def _lark_normalize_card_callback_envelope(data):
    # type: (object) -> object
    """
    Merge flattened ``card.action.trigger`` fields into ``event`` when proxies strip nesting.
    Feishu Schema 2.0 puts **chat** in ``event.context.open_chat_id`` (not ``context.chat_id``);
    SDK-flat payloads use top-level ``open_chat_id`` / ``open_message_id`` — mirror OpenClaw #71670.
    """
    if not isinstance(data, dict):
        return data
    if not _lark_should_merge_flat_card_callback(data):
        return data
    ev = data.get("event")
    if not isinstance(ev, dict):
        ev = {}
    for k in (
        "action",
        "operator",
        "open_chat_id",
        "chat_id",
        "context",
        "host",
        "delivery_type",
        "token",
    ):
        if k in data and data[k] is not None and k not in ev:
            ev[k] = data[k]
    # Flat IDs → ``context`` (canonical IM shape per open.feishu.cn card-callback-communication).
    ctx = ev.get("context")
    if not isinstance(ctx, dict):
        ctx = {}
        ev["context"] = ctx
    if isinstance(data.get("open_chat_id"), str) and data["open_chat_id"].strip() and not ctx.get(
        "open_chat_id"
    ):
        ctx["open_chat_id"] = data["open_chat_id"].strip()
    if isinstance(data.get("open_message_id"), str) and data["open_message_id"].strip() and not ctx.get(
        "open_message_id"
    ):
        ctx["open_message_id"] = data["open_message_id"].strip()
    top_uid = data.get("open_id") or data.get("user_id")
    top_union = data.get("union_id")
    op = ev.get("operator")
    if top_uid or top_union:
        if not isinstance(op, dict):
            ev["operator"] = {}
            op = ev["operator"]
        if isinstance(op, dict):
            op = dict(op)
            if top_uid and not op.get("open_id"):
                op["open_id"] = top_uid
            if top_union and not op.get("union_id"):
                op["union_id"] = top_union
            ev["operator"] = op
    # Lark troubleshooting: groups often expose chat on ``event.open_chat_id``; only ``context.open_chat_id``
    # may exist — mirror onto event top-level so downstream always sees a stable target id.
    ctx_merge = ev.get("context") if isinstance(ev.get("context"), dict) else {}
    if not ev.get("open_chat_id") and ctx_merge.get("open_chat_id"):
        ev["open_chat_id"] = ctx_merge["open_chat_id"]
    data["event"] = ev
    return data


def _lark_extract_card_event_fields(ev):
    # type: (dict) -> tuple
    """
    Resolve chat / sender / button ``value`` from ``event`` for ``card.action.trigger`` payloads.

    **Chat ID priority** (avoids client ``code: undefined`` when the synthetic reply has no target):
    **event.open_chat_id** first (often top-level on the event object in groups), then ``event.chat_id``,
    then ``context.open_chat_id``, then ``context.chat_id``. Do **not** rely on ``context.chat_id`` alone
    when ``open_chat_id`` exists elsewhere — wrong field mapping is a common cause of ``code: undefined``.
    """
    ctx = ev.get("context") if isinstance(ev.get("context"), dict) else {}
    act = ev.get("action") or {}
    val = act.get("value")
    chat_id = ev.get("open_chat_id") or ev.get("chat_id")
    if not chat_id:
        chat_id = ctx.get("open_chat_id") or ctx.get("chat_id")
    op = ev.get("operator") or {}
    sender_id = op.get("open_id")
    if not sender_id:
        sender_id = op.get("union_id")
    if not sender_id:
        sender_id = (
            ev.get("open_id")
            or ev.get("user_id")
            or op.get("user_id")
        )
    return chat_id, sender_id, val


def _lark_event_body_looks_like_card_interaction(ev):
    # type: (object) -> bool
    """When ``header.event_type`` is missing or wrong, still recognize card callbacks by shape."""
    if not isinstance(ev, dict):
        return False
    act = ev.get("action")
    if not isinstance(act, dict):
        return False
    if ev.get("message"):
        return False
    if act.get("tag") == "button":
        return True
    # JSON 2.0 / some builds omit ``tag``; ``name`` + ``value`` + operator/context is enough.
    if act.get("name") and act.get("value") is not None:
        return bool(ev.get("operator") or ev.get("context"))
    if act.get("value") is not None and (ev.get("operator") or ev.get("context")):
        return True
    return bool(ev.get("operator") or ev.get("context"))


def _lark_resolve_card_action(data):
    # type: (dict) -> Optional[tuple]
    """
    Returns ``(chat_id, sender_id, value, event_id)`` for card button callbacks, or ``None``.
    Matches by ``header.event_type`` **or** schema-2.0 payload shape (operator + action + context).
    """
    if not isinstance(data, dict):
        return None
    hdr = data.get("header") if isinstance(data.get("header"), dict) else {}
    et = _lark_header_event_type(data)
    eid = hdr.get("event_id") if isinstance(hdr, dict) else None
    if eid is None:
        eid = data.get("event_id")
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}

    named = et in ("card.action.trigger", "card.action.trigger_v1")
    heuristic = et != "im.message.receive_v1" and (
        (
            _lark_is_schema_v2(data)
            and _lark_event_body_looks_like_card_interaction(ev)
        )
        or (
            isinstance(ev.get("action"), dict)
            and len(ev.get("action") or {}) > 0
            and (ev.get("operator") or ev.get("context"))
        )
    )
    ctx0 = ev.get("context") if isinstance(ev.get("context"), dict) else {}
    legacy_shape = (
        et != "im.message.receive_v1"
        and isinstance(ev.get("action"), dict)
        and len(ev.get("action") or {}) > 0
        and (ev.get("operator") or ev.get("context"))
        and bool(
            ev.get("open_chat_id")
            or ev.get("chat_id")
            or ctx0.get("open_chat_id")
            or ctx0.get("chat_id")
        )
    )
    if not (named or heuristic or legacy_shape):
        return None
    if (heuristic or legacy_shape) and not named:
        print(
            "[lark] card.action matched by payload shape (event.operator/context + event.action); "
            f"header.event_type was {et!r}",
            flush=True,
        )
    chat_id, sender_id, val = _lark_extract_card_event_fields(ev)
    return (chat_id, sender_id, val, eid)


def _lark_payload_has_card_action(data):
    # type: (object) -> bool
    """
    True when ``event.action`` **or** SDK-flat top-level ``action`` is present (any interactive tag).
    """
    if not isinstance(data, dict):
        return False
    ev = data.get("event")
    if isinstance(ev, dict):
        act = ev.get("action")
        if isinstance(act, dict) and len(act) > 0:
            return True
    act_top = data.get("action")
    return isinstance(act_top, dict) and len(act_top) > 0


def _lark_header_event_type(data):
    # type: (object) -> str
    """``header.event_type``, or rare top-level ``event_type`` (some gateway proxies strip nested keys)."""
    if isinstance(data, dict):
        h = data.get("header")
        if isinstance(h, dict):
            et = h.get("event_type")
            if et is not None:
                return str(et).strip()
        et2 = data.get("event_type")
        if et2 is not None:
            return str(et2).strip()
    return ""


def _lark_ack_only_event_type(het: str) -> bool:
    """
    Subscribed in the Lark console but not implemented in this bot — still **must** HTTP 200.

    Without this branch, schema-2 payloads hit the generic **Unknown webhook** path and spam logs
    (e.g. ``meeting_room.meeting_room.status_changed_v1`` shares the same Request URL as IM events).
    """
    if not het:
        return False
    h = het.lower()
    if h.startswith("meeting_room."):
        return True
    return False


def _process_evo_sd_batch_paste(chat_id: str, email_text: str) -> None:
    """Run the EVO Service Desk batch pipeline (same as ``/m``) and post the cards.

    Shared by the explicit ``/m`` command and the no-command auto-detection so both
    paths behave identically. Only allowed in the OSE BOT - Ops & Maintenance group.
    """
    if not maintenance.is_evo_batch_command_chat(chat_id):
        send_message(chat_id, maintenance.EVO_BATCH_WRONG_GROUP_MESSAGE)
        return
    try:
        token = get_tenant_access_token()
        batch = maintenance.process_evo_sd_batch_maintenance(email_text, token)
        import maintenance_mail as _maint_mail

        if batch.get("email_sent"):
            _maint_mail.send_evo_batch_maintenance_email(
                subject=batch["email_subject"],
                body=batch["email_body"],
            )
            fwd_chat = maintenance.evo_batch_forward_chat_id()
            fwd_card = batch.get("forward_card")
            if fwd_chat and fwd_card:
                send_message(
                    fwd_chat,
                    json.dumps(fwd_card, ensure_ascii=False),
                    msg_type="interactive",
                    reply_to_message_id="",  # direct post to the forward group, never a reply
                )
            # @tag QA Support Team + CS to check the email that was just sent. Pinned to the
            # forward group (oc_9ffa9a…) via its own resolver, INDEPENDENT of fwd_chat — so a
            # misconfigured EVO_BATCH_FORWARD_CHAT_ID can't divert this ping into the /m
            # command group (oc_51b6fbf…).
            check_chat = maintenance.evo_batch_check_email_chat_id()
            if check_chat:
                send_message(
                    check_chat,
                    json.dumps(
                        maintenance.build_evo_batch_check_email_card(
                            batch.get("email_subject") or ""
                        ),
                        ensure_ascii=False,
                    ),
                    msg_type="interactive",
                    reply_to_message_id="",  # direct post to the QA/CS group, never a reply
                )
        send_message(
            chat_id,
            json.dumps(batch["result_card"], ensure_ascii=False),
            msg_type="interactive",
        )
    except Exception as ex:
        send_message(chat_id, f"❌ EVO 批量 `/m` 处理失败: `{ex}`")


def _egs_reply(chat_id: str, reply_mid: str, payload, *, msg_type: str = "text") -> dict:
    """Post an ``/egs`` message as a **quote-reply** to the user's original message.

    Quote-reply (not ``reply_in_thread``): interactive cards accepted as thread replies
    render invisibly in Feishu — the user then sees only a reaction. A quote-reply keeps
    the message tied to the user's ``/egs`` message AND visible in the chat.
    """
    mid = (reply_mid or "").strip() or None
    return send_message(chat_id, payload, msg_type=msg_type, reply_to_message_id=mid)


def _process_egs_paste(chat_id: str, body_text: str, *, test: bool = False) -> None:
    """``/egs`` — show an **editable preview card** for a pasted maintenance notice.

    The LLM-derived subject (``{Vendor} {Type} Maintenance - DD/MM/YYYY``) and the body
    (+ signature) are pre-filled into editable fields; nothing is sent until the user taps
    **Send** on the card. Posted DIRECTLY to the chat (interactive cards render invisibly
    as replies). Gated to the same OSE BOT - Ops & Maintenance group as ``/m``.

    ``test=True`` (``/egstest``) marks the card TEST — Send delivers ONLY to junchen@
    (``EGS_TEST_REPLY_TO``), no Cc, no QA/CS tag, not recorded in egs.json.
    """
    _cmd = "/egstest" if test else "/egs"
    if not maintenance.is_evo_batch_command_chat(chat_id):
        send_message(chat_id, maintenance.EVO_BATCH_WRONG_GROUP_MESSAGE)
        return
    reply_mid = (_lark_user_message_id.get() or "").strip()
    body = (body_text or "").strip()
    if not body:
        _egs_reply(
            chat_id,
            reply_mid,
            f"请在 `{_cmd}` 后粘贴维护通知内容。\n"
            "标题会自动生成：`{维护内容} - {今天日期 DD/MM/YYYY}`，"
            + (
                "点发送后**只**发到 junchen@snsoft.my（测试）。"
                if test
                else "邮件发送到 egs.maintenance@om.hotelstotsenberg.com（抄送 om@hotelstotsenberg.com）。"
            ),
        )
        return
    try:
        subject = maintenance.build_egs_email_subject(body)
        import maintenance_mail as _maint_mail

        full_body = body
        if _maint_mail.EGS_MAIL_SIGNATURE:
            full_body = f"{body}\n\n{_maint_mail.EGS_MAIL_SIGNATURE}"

        kwargs: dict = {}
        if test:
            _cc_note = (
                f"（抄送 {_maint_mail.EGS_TEST_REPLY_CC}）"
                if _maint_mail.EGS_TEST_REPLY_CC
                else "（无抄送）"
            )
            kwargs = dict(
                header_title="🧪 EGS 测试邮件预览 / Test — sends to junchen@",
                info_md=(
                    f"🧪 测试模式：点 **发送** 后邮件发送到 **{_maint_mail.EGS_TEST_REPLY_TO}**"
                    f"{_cc_note}，不 @QA/CS、不记录。可编辑标题/正文。"
                ),
                send_label="🧪 发送测试 / Send test",
                extra_send_val={"t": "1"},
            )
        card = maintenance.build_egs_preview_card(
            subject, full_body, reply_to_message_id=reply_mid, **kwargs
        )
        _resp = send_message(
            chat_id,
            json.dumps(card, ensure_ascii=False),
            msg_type="interactive",
            reply_to_message_id="",  # force a direct post, not a reply
        )
        print(
            f"[egs] preview card sent test={test} code={(_resp or {}).get('code')!r} "
            f"msg={(_resp or {}).get('msg')!r}",
            flush=True,
        )
    except Exception as ex:
        print(f"[egs] preview card failed: {ex!r}", flush=True)
        _egs_reply(chat_id, reply_mid, f"❌ `{_cmd}` 处理失败: `{ex}`")


def _post_egsreply_preview_card(
    chat_id: str, email_title: str, content: str, reply_mid: str, *, test: bool
) -> None:
    """Post the editable ``/egsreply`` reply preview card. ``email_title`` is the PICKED
    email's subject — it rides in the Send button value (``s``) and locks the reply target,
    so the reply always threads off that email (editing the shown title can't change it)."""
    import maintenance_mail as _maint_mail

    full_content = content
    if _maint_mail.EGS_MAIL_SIGNATURE:
        full_content = (
            f"{content}\n\n{_maint_mail.EGS_MAIL_SIGNATURE}"
            if content
            else _maint_mail.EGS_MAIL_SIGNATURE
        )
    if test:
        header = "🧪 EGS 回复预览(测试) / Reply preview (TEST)"
        info = (
            f"🧪 测试模式：回复只发送到 **{_maint_mail.EGS_TEST_REPLY_TO}**（不发给原收件人）。\n"
            "回复的**邮件已选定**（上方标题即所选邮件，勿改）；填写下方**正文**后点 **发送**。"
        )
        send_label = "🧪 回复(测试) / Send Reply (test)"
    else:
        header = "📧 EGS 回复邮件预览 / Reply — review before sending"
        info = (
            "将在**所选邮件**的会话内**回复**（收件/抄送同原邮件，保持在原会话内）。\n"
            "回复的**邮件已选定**（上方标题即所选邮件，勿改）；填写**正文**后点 **回复 / Send Reply**。"
        )
        send_label = "✅ 回复 / Send Reply"
    card = maintenance.build_egs_preview_card(
        email_title,
        full_content,
        reply_to_message_id=reply_mid,
        header_title=header,
        title_label="回复邮件 Replying to (已选定)",
        title_placeholder="Subject of the email to reply to",
        send_key="egsreply_send",
        send_label=send_label,
        info_md=info,
        # Lock the reply subject to the PICKED email (rides in the Send button value as `s`),
        # so the stored-Message-ID lookup always matches even if the title field is edited.
        extra_send_val={"t": "1", "s": email_title} if test else {"s": email_title},
    )
    _resp = send_message(
        chat_id,
        json.dumps(card, ensure_ascii=False),
        msg_type="interactive",
        reply_to_message_id="",  # direct post (interactive cards render invisibly as replies)
    )
    print(
        f"[egsreply] preview card sent test={test} code={(_resp or {}).get('code')!r} "
        f"msg={(_resp or {}).get('msg')!r}",
        flush=True,
    )


# Reply body pasted WITH ``/egsreply`` (``/egsreply\n{body}``), held until the user taps an
# email in the picker so it can pre-fill the reply preview. Keyed by the user's original
# message id (which rides on the picker buttons as ``m``), falling back to chat_id. The Lark
# server is single-process threaded (``app.run(threaded=True)``) so a lock-guarded dict is
# shared across the picker→callback threads; entries self-expire so a never-picked paste leaks
# nothing.
_EGSREPLY_PENDING_BODY: dict[str, tuple[float, str]] = {}
_EGSREPLY_PENDING_LOCK = threading.Lock()
_EGSREPLY_PENDING_TTL = 1800.0  # 30 min
_EGSREPLY_PENDING_MAX = 50


def _egsreply_stash_body(key: str, body: str) -> None:
    """Remember the pasted reply ``body`` under ``key`` for the pending picker (no-op if
    either is empty). Prunes stale/overflow entries so the map stays small."""
    key = (key or "").strip()
    if not key or not body:
        return
    now = time.time()
    with _EGSREPLY_PENDING_LOCK:
        for _k in [
            _k for _k, (_ts, _) in _EGSREPLY_PENDING_BODY.items()
            if now - _ts > _EGSREPLY_PENDING_TTL
        ]:
            _EGSREPLY_PENDING_BODY.pop(_k, None)
        while len(_EGSREPLY_PENDING_BODY) >= _EGSREPLY_PENDING_MAX:
            _oldest = min(_EGSREPLY_PENDING_BODY, key=lambda k: _EGSREPLY_PENDING_BODY[k][0])
            _EGSREPLY_PENDING_BODY.pop(_oldest, None)
        _EGSREPLY_PENDING_BODY[key] = (now, body)


def _egsreply_pop_body(key: str) -> str:
    """Remove and return the pasted reply body stashed under ``key`` (``""`` if none/expired)."""
    key = (key or "").strip()
    if not key:
        return ""
    now = time.time()
    with _EGSREPLY_PENDING_LOCK:
        item = _EGSREPLY_PENDING_BODY.pop(key, None)
    if not item:
        return ""
    _ts, body = item
    return body if now - _ts <= _EGSREPLY_PENDING_TTL else ""


def _process_egsreply_paste(chat_id: str, content: str = "", *, test: bool = False) -> None:
    """``/egsreply`` / ``/egsreplytest`` — ALWAYS show the PICKER card so the user picks
    which previously sent email to reply to.

    The picker lists recent ``/egs`` sends (``egs.json``) — or ``/egstest`` sends
    (``egstest.json``) for ``/egsreplytest``. Tapping an email opens the editable reply
    preview card, where the user writes (or reviews) the reply and sends it; the reply threads
    off the stored Message-ID (no fuzzy search, no "email not found"). ``content`` is the body
    pasted with the command — stashed here and pre-filled into the preview once the user picks
    an email (so it isn't retyped). ``test=True`` replies only to the test address (junchen@).
    """
    _cmd = "/egsreplytest" if test else "/egsreply"
    if not maintenance.is_evo_batch_command_chat(chat_id):
        send_message(chat_id, maintenance.EVO_BATCH_WRONG_GROUP_MESSAGE)
        return
    reply_mid = (_lark_user_message_id.get() or "").strip()
    # Hold the pasted body against the same key the picker buttons carry (``m`` → reply_mid,
    # else chat_id), so the ``egsreply_pick`` callback can pre-fill the reply preview.
    _egsreply_stash_body(reply_mid or chat_id, (content or "").strip())
    try:
        # Picker card: /egsreply lists real /egs sends (egs.json),
        # /egsreplytest lists /egstest sends (egstest.json).
        import maintenance_mail as _maint_mail

        entries = _maint_mail.egs_recent_sent_emails(test=test)
        if not entries:
            _store = "egstest.json" if test else "egs.json"
            _src = "/egstest" if test else "/egs"
            _egs_reply(
                chat_id,
                reply_mid,
                f"没有 `{_src}` 发送记录（{_store} 为空）。先用 `{_src}` 发送一封再回复。",
            )
            return
        picker = maintenance.build_egsreply_picker_card(
            entries, test=test, reply_to_message_id=reply_mid
        )
        _resp = send_message(
            chat_id,
            json.dumps(picker, ensure_ascii=False),
            msg_type="interactive",
            reply_to_message_id="",  # direct post
        )
        print(
            f"[egsreply] picker sent test={test} n={len(entries)} "
            f"code={(_resp or {}).get('code')!r}",
            flush=True,
        )
    except Exception as ex:  # noqa: BLE001
        print(f"[egsreply] picker failed: {ex!r}", flush=True)
        _egs_reply(chat_id, reply_mid, f"❌ `{_cmd}` 处理失败: `{ex}`")


def _try_egs_card_response(parsed_ca: dict, ev_ca: dict, chat_id_ca: str) -> Optional[dict]:
    """Synchronous card.callback for the ``/egs`` **Send Email** / **Cancel** buttons.

    Reads the (possibly edited) title + content from the form, updates the card in place
    (buttons removed → no double-send), and sends the email in a background thread so we
    stay within Lark's ~3s callback window.
    """
    k = str(parsed_ca.get("k") or "").strip().lower()
    if k not in ("egs_send", "egs_cancel", "egsreply_send", "egsreply_pick"):
        return None

    # The card's own message_id — so we can DELETE the card (make it disappear) after the tap.
    ctx = ev_ca.get("context") if isinstance(ev_ca.get("context"), dict) else {}
    card_msg_id = (
        (ctx.get("open_message_id") or ev_ca.get("open_message_id") or "").strip()
    )
    # The user's original /egs message id (rode in on the button) — so the confirmation
    # threads under it, same as the card.
    orig_mid = str(parsed_ca.get("m") or "").strip()

    def _recall_card() -> None:
        if card_msg_id:
            try:
                recall_message(card_msg_id)
            except Exception as _rc_err:  # noqa: BLE001
                print(f"[egs] card recall failed: {_rc_err!r}", flush=True)

    if k == "egs_cancel":
        threading.Thread(target=_recall_card, daemon=True).start()  # card disappears
        return {"toast": {"type": "info", "content": "Cancelled"}}

    if k == "egsreply_pick":
        # Picker button tapped — swap the picker for the editable reply preview card
        # pre-filled with the chosen email's subject and any body pasted with the command.
        pick_subject = str(parsed_ca.get("s") or "").strip()
        pick_test = str(parsed_ca.get("t") or "").strip() == "1"
        if not pick_subject:
            return {"toast": {"type": "error", "content": "No email subject on this button."}}
        # Reply body the user pasted with `/egsreply` (same key the picker was stashed under).
        pick_body = _egsreply_pop_body(orig_mid or chat_id_ca)

        def _pick_job() -> None:
            _recall_card()  # remove the picker
            try:
                _post_egsreply_preview_card(
                    chat_id_ca, pick_subject, pick_body, orig_mid, test=pick_test
                )
            except Exception as ex:  # noqa: BLE001
                _egs_reply(chat_id_ca, orig_mid, f"❌ `/egsreply` 打开编辑卡失败: `{ex}`")

        threading.Thread(target=_pick_job, daemon=True).start()
        return {"toast": {"type": "info", "content": "Opening reply editor"}}

    # Send tapped — pull the edited Title + Content out of the submitted form.
    act = ev_ca.get("action") if isinstance(ev_ca.get("action"), dict) else {}
    title = _lark_get_card_form_field(act, "egs_title")
    body = _lark_get_card_form_field(act, "egs_body")
    fv = parsed_ca.get("form_value")
    if isinstance(fv, dict):
        title = title or _lark_form_field_text(fv.get("egs_title"))
        body = body or _lark_form_field_text(fv.get("egs_body"))
    title = title or _lark_find_field_deep(ev_ca, "egs_title")
    body = body or _lark_find_field_deep(ev_ca, "egs_body")
    title = (title or "").strip()
    body = (body or "").strip()

    is_reply = k == "egsreply_send"
    is_test = str(parsed_ca.get("t") or "").strip() == "1"
    if is_reply:
        # Reply subject is LOCKED to the picked email (carried in the Send button value as
        # `s`), so the stored-Message-ID lookup always matches — no "未找到原邮件" fallback,
        # even if the (informational) title field was edited.
        _picked = str(parsed_ca.get("s") or "").strip()
        if _picked:
            title = _picked
    if not title or not body:
        # Keep the card so the user can fix it — just warn.
        return {"toast": {"type": "error", "content": "Title and content cannot be empty."}}

    def _send_job() -> None:
        _recall_card()  # remove the form card first so it disappears on click
        try:
            import maintenance_mail as _maint_mail

            if is_reply:
                # `title` here is the subject to FIND; reply-all inside that email's thread.
                info = _maint_mail.reply_egs_email(
                    email_title=title, body=body, test=is_test
                )
                _to = ", ".join(info.get("to") or [])
                _cc = ", ".join(info.get("cc") or [])
                _lbl = "/egsreplytest" if is_test else "/egsreply"
                if info.get("threaded"):
                    _note = "（已在原邮件会话内回复）"
                elif info.get("found"):
                    _note = ""
                else:
                    _note = "（⚠️ 未找到原邮件，已发送纯测试邮件）"
                _egs_reply(
                    chat_id_ca,
                    orig_mid,
                    f"✅ `{_lbl}` 已发送{_note}\n📌 {info.get('subject') or ''}\n📧 收件: {_to}"
                    + (f"\n📄 抄送: {_cc}" if _cc else ""),
                )
            elif is_test:
                # /egstest — throwaway send to junchen@ (Cc om@), no QA/CS tag, not stored.
                _maint_mail.send_egs_maintenance_email(
                    subject=title,
                    body=body,
                    append_signature=False,
                    to_override=_maint_mail.EGS_TEST_REPLY_TO,
                )
                _cc = _maint_mail.EGS_TEST_REPLY_CC
                _egs_reply(
                    chat_id_ca,
                    orig_mid,
                    f"✅ `/egstest` 测试邮件已发送\n📌 主题: {title}\n"
                    f"📧 收件: {_maint_mail.EGS_TEST_REPLY_TO}"
                    + (f"\n📄 抄送: {_cc}" if _cc else ""),
                )
            else:
                # Body already includes the signature (shown/edited in the card) → don't re-append.
                _maint_mail.send_egs_maintenance_email(
                    subject=title, body=body, append_signature=False
                )
                _egs_reply(chat_id_ca, orig_mid, f"✅ `/egs` 邮件已发送\n📌 主题: {title}")
                # @tag QA Support Team + CS to check the sent email — same pinned forward group
                # (oc_9ffa9a…) as /m, via evo_batch_check_email_chat_id() (env-independent).
                check_chat = maintenance.evo_batch_check_email_chat_id()
                if check_chat:
                    send_message(
                        check_chat,
                        json.dumps(
                            maintenance.build_evo_batch_check_email_card(title),
                            ensure_ascii=False,
                        ),
                        msg_type="interactive",
                        reply_to_message_id="",  # direct post to the QA/CS group, never a reply
                    )
        except Exception as ex:  # noqa: BLE001
            if is_reply:
                _lbl = "/egsreplytest" if is_test else "/egsreply"
            else:
                _lbl = "/egstest" if is_test else "/egs"
            _egs_reply(chat_id_ca, orig_mid, f"❌ `{_lbl}` 失败: `{ex}`")

    threading.Thread(target=_send_job, daemon=True).start()
    return {"toast": {"type": "success", "content": "Sending reply" if is_reply else "Sending email"}}


def _reinject_synthetic_command_message(
    chat_id: str, sender_open_id: str, command_text: str
) -> bool:
    """Card button (``k=cmd_run``) → run a command through the FULL normal message pipeline.

    Mirrors the WS-mode synthetic dispatch (:func:`_lark_ws_dispatch_payload`): an
    in-process POST of an ``im.message.receive_v1`` event, so tapping a button behaves
    exactly like typing that command. ``message_id`` stays EMPTY on purpose — that skips
    duplicate-dedupe, the GotIt reaction and the quote-reply contextvar (a fake id would
    make ``send_message``'s reply POST fail with no fallback). ``chat_type: p2p`` bypasses
    the group @mention gate while replies still target the real ``chat_id``.
    """
    cmd = (command_text or "").strip()
    if not cmd or len(cmd) > 400:
        return False
    # Admin/destructive commands are never run from a button — defense in depth
    # (our suggestion cards never offer them, but card values are client data).
    _denied = {
        "/restart", "/restarta", "/deploy", "/gitpullrestart",
        "/restartservices", "/restservices", "/secret1", "/secret2", "/cashout",
    }
    if cmd.split()[0].lower() in _denied:
        print(f"[cmdbtn] blocked admin command from button: {cmd!r}", flush=True)
        return False
    now_ms = str(int(time.time() * 1000))
    payload = {
        "schema": "2.0",
        "header": {
            "event_id": f"cmdbtn-{uuid.uuid4().hex}",
            "event_type": "im.message.receive_v1",
            "create_time": now_ms,
            "token": VERIFICATION_TOKEN,
            "app_id": APP_ID or "",
            "tenant_key": "",
        },
        "event": {
            "sender": {
                "sender_id": {"open_id": (sender_open_id or "").strip() or "ou_cmdbtn"},
                "sender_type": "user",
            },
            "message": {
                "message_id": "",
                "chat_id": chat_id,
                # Keep group semantics (some flows post differently in p2p);
                # a synthetic @bot mention passes the group gate instead.
                "chat_type": "group",
                "message_type": "text",
                "create_time": now_ms,
                "content": json.dumps({"text": cmd}, ensure_ascii=False),
                "mentions": [
                    {
                        "key": "@_user_1",
                        "id": {"open_id": BOT_OPEN_ID},
                        "name": "Duty Bot",
                        "tenant_key": "",
                    }
                ],
            },
        },
    }
    try:
        with app.test_client() as client:
            rv = client.post("/webhook/event", json=payload)
        print(
            f"[cmdbtn] reinjected {cmd!r} for chat {chat_id} → HTTP {rv.status_code}",
            flush=True,
        )
        return int(rv.status_code) < 400
    except Exception as ex:
        print(f"[cmdbtn] reinject failed for {cmd!r}: {ex!r}", flush=True)
        return False


@app.route("/webhook/event", methods=["POST", "GET", "OPTIONS"])
def lark_webhook():
    # Some proxies send OPTIONS; **405** breaks Feishu card interaction (expects HTTP 200 family on callback URL).
    if request.method == "OPTIONS":
        return Response(status=204)

    if request.method == "GET":
        payload = {
            "ok": True,
            "service": "lark_webhook",
            "detail": "Feishu/Lark must POST JSON to this URL for events and card callbacks.",
        }
        if (request.args.get("diag") or "").strip().lower() in ("1", "true", "yes"):
            ek = (
                (os.getenv("LARK_ENCRYPT_KEY") or os.getenv("ENCRYPT_KEY") or os.getenv("FEISHU_ENCRYPT_KEY") or "")
                .strip()
            )
            ju = _get_jenkinsupdate()
            payload["diag"] = {
                "verification_token_configured": bool(VERIFICATION_TOKEN),
                "encrypt_key_configured": bool(ek),
                "jenkinsupdate_import_ok": ju is not None,
                "extra_webhook_paths": [
                    p.strip()
                    for p in (os.getenv("LARK_WEBHOOK_EXTRA_PATHS") or "").split(",")
                    if p.strip()
                ],
            }
            payload["checklist_cn"] = [
                "开发者后台 → 事件与回调：请求地址必须是公网 HTTPS，路径与本服务一致（含 nginx 转发）。",
                "若使用「长连接 / persistent connection」：设 LARK_EVENT_MODE=websocket 并运行 python main.py（已注册 card.action.trigger 并修补 SDK CARD 帧；否则按钮会 code:undefined）。",
                "优先订阅新版「卡片回传交互」card.action.trigger（请求体含 header.token = Verification Token）。",
                "若仍订阅旧版 card.action.trigger_v1（扁平 JSON，token 为卡片凭证 c-…）：误把该 token 当 Verification Token 会 403；本仓库已忽略 c- 前缀。若 JSON 内仍无 Verification Token，可设 LARK_LEGACY_CARD_V1_ALLOW_MISSING_VERIFICATION_TOKEN=1（仅信任链路时使用）。",
                "环境变量 VERIFICATION_TOKEN 与后台「Verification Token」完全一致（无多空格）。",
                "若开启了加密：设 LARK_ENCRYPT_KEY；未开启加密：后台关掉加密或勿配密钥。",
                "点按钮时 journalctl 应出现 [lark] webhook POST；若没有，请求没到本进程（DNS/防火墙/URL 错误）。",
                "若日志有 ❌ Token mismatch → 修正 VERIFICATION_TOKEN；403 会导致客户端报错/code undefined。",
                "群组卡片交互：会话 ID 优先读 event.open_chat_id（或 context.open_chat_id）；只用 context.chat_id 易导致客户端 code:undefined — 本仓库已按该优先级解析并在仅有 context 时回填 event.open_chat_id。",
                "核对开发者后台：事件订阅 Request URL；若仍配置「消息卡片请求网址」，须指向同一可访问端点或使用 LARK_WEBHOOK_EXTRA_PATHS。",
            ]
        return jsonify(payload)

    raw_in = _lark_safe_parse_json_body(request)
    if raw_in is None:
        return jsonify({"error": "invalid json"}), 400
    data = _feishu_maybe_decrypt_webhook_payload(raw_in)
    data = _lark_coerce_event_dict(data)
    if isinstance(data, dict):
        data = _lark_normalize_legacy_card_trigger_v1_flat(data)
    if isinstance(data, dict):
        data = _lark_normalize_card_callback_envelope(data)

    if isinstance(raw_in, dict) and raw_in.get("encrypt") is not None and data is raw_in:
        print(
            "[lark] POST body is still encrypted — set LARK_ENCRYPT_KEY (and pycryptodome), "
            "or disable 「加密」 in 事件与回调 so Feishu sends plain JSON.",
            flush=True,
        )
        return jsonify({"error": "Invalid token"}), 403

    if data.get("type") == "url_verification":
        return jsonify({"challenge": data["challenge"]})

    token = _lark_extract_verification_token(data)
    token_ok = token == VERIFICATION_TOKEN
    if not token_ok and token is None and VERIFICATION_TOKEN:
        # Legacy flat ``trigger_v1`` often has **no** Verification Token in JSON (only ``c-`` card credential).
        # Opt-in only — prefer subscribing to **Card Callback Interaction** (``card.action.trigger``) which includes ``header.token``.
        if (
            (os.getenv("LARK_LEGACY_CARD_V1_ALLOW_MISSING_VERIFICATION_TOKEN") or "").strip() == "1"
            and _lark_header_event_type(data) == "card.action.trigger_v1"
        ):
            print(
                "[lark] accepting card.action.trigger_v1 without body Verification Token "
                "(LARK_LEGACY_CARD_V1_ALLOW_MISSING_VERIFICATION_TOKEN=1); "
                "prefer migrating subscription to card.action.trigger schema 2.0.",
                flush=True,
            )
            token_ok = True
    if not token_ok:
        sch = data.get("schema") if isinstance(data, dict) else None
        print(
            f"❌ Token mismatch: expected {VERIFICATION_TOKEN}, got {token!r} schema={sch!r}",
            flush=True,
        )
        return jsonify({"error": "Invalid token"}), 403

    hdr_et = _lark_header_event_type(data)
    # ``meeting_room.*`` and similar — subscribed but unhandled; ACK without logging (high volume).
    if _lark_ack_only_event_type(hdr_et):
        return jsonify({"success": True})

    # One line per POST — if this never appears when you tap a card button, Feishu is not reaching this process
    # (wrong public URL/port, nginx not proxy_pass to here, or firewall). Fix infra before debugging Python.
    print(
        "[lark] webhook POST len=%s path=%s ct=%s"
        % (
            request.content_length,
            request.path,
            (request.headers.get("Content-Type") or "")[:80],
        ),
        flush=True,
    )
    # Text messages → ``im.message.receive_v1``; button taps → ``card.action.trigger`` / ``card.action.trigger_v1``.
    # If you only see the former when testing, the app is not receiving card events (subscribe + publish).
    print(
        "[lark] event_type=%r schema=%r top_keys=%r"
        % (
            hdr_et,
            data.get("schema") if isinstance(data, dict) else None,
            list(data.keys())[:14] if isinstance(data, dict) else [],
        ),
        flush=True,
    )

    # Bot footer menu « Push event » — ``application.bot.menu_v6`` (Lark docs). **Not** ``card.action.trigger``;
    # client expects ``{"success": true}``, **not** empty ``{}`` (card ACK).
    if hdr_et in ("application.bot.menu_v6", "application.bot.menu"):
        menu_payload = dict(data) if isinstance(data, dict) else {}

        def _bot_menu_worker() -> None:
            try:
                import offsetleave as _ol

                _ol.handle_bot_menu_event(
                    menu_payload,
                    send_message=send_message,
                    get_token_func=get_tenant_access_token,
                )
            except Exception as exc:
                print(f"[lark] bot menu worker failed: {exc!r}", flush=True)

        threading.Thread(
            target=_bot_menu_worker,
            daemon=True,
            name="lark-bot-menu",
        ).start()
        print("[lark] bot menu event — ACK success=True (worker started)", flush=True)
        return jsonify({"success": True})

    # ``card.action.trigger``: MUST respond within **3s** with HTTP 200 and body ``{}`` or ``toast``/``card``.
    # Never return ``{"success": true}`` here — Feishu treats that as an invalid card callback and shows
    # "Something went wrong … code: undefined". Also match by payload shape if ``event_type`` is missing.
    card_resolved = _lark_resolve_card_action(data)
    # Any ``card.action*`` must ACK — if resolver misses a new payload shape, still return ``{}``.
    if hdr_et.startswith("card.action") and card_resolved is None:
        print(
            "[lark] card.callback event_type=%r but resolver returned None — still returning 200 {}. "
            "Payload keys: %r"
            % (hdr_et, list(data.keys()) if isinstance(data, dict) else type(data)),
            flush=True,
        )
        return _lark_http_card_callback_ok()

    if card_resolved is not None:
        chat_id_ca, sender_id_ca, val_ca, eid_ca = card_resolved
        if sender_id_ca and sender_id_ca == BOT_OPEN_ID:
            return _lark_http_card_callback_ok()
        parsed_sync = _lark_parse_card_action_value(val_ca)
        if isinstance(parsed_sync, dict):
            thread_r = str(parsed_sync.get("r") or "").strip()
            if thread_r and chat_id_ca:
                _set_prod_batch_thread_root(chat_id_ca, thread_r)
            sm_sync = smmachine.try_prod_batch_sm_env_card_response(
                parsed_sync,
                chat_id=chat_id_ca or "",
            )
            if sm_sync is not None:
                if eid_ca:
                    _remember_processed_message_id(eid_ca)
                return _lark_http_card_callback_response(sm_sync)
            ev_sync = data.get("event") if isinstance(data.get("event"), dict) else {}
            rem_sync = _try_rem_add_submit_card_response(
                parsed_sync,
                ev_sync,
                chat_id_ca or "",
            )
            if rem_sync is not None:
                if eid_ca:
                    _remember_processed_message_id(eid_ca)
                return _lark_http_card_callback_response(rem_sync)
            egs_sync = _try_egs_card_response(
                parsed_sync,
                ev_sync,
                chat_id_ca or "",
            )
            if egs_sync is not None:
                if eid_ca:
                    _remember_processed_message_id(eid_ca)
                return _lark_http_card_callback_response(egs_sync)
        # Never wait on ``processed_lock`` in this thread — Lark times out ~3s; lock contention → ``code: undefined``.
        def _run_card_callback_worker() -> None:
            if eid_ca and _remember_processed_message_id(eid_ca):
                print(f"⏭️ Duplicate card callback {eid_ca} ignored ({hdr_et!r})", flush=True)
                return
            if maintenance.is_evo_batch_forward_only_chat(chat_id_ca) or _is_announce_only_chat(chat_id_ca):
                print(
                    f"⏭️ announce-only / forward-only group — ignoring card callback ({chat_id_ca})",
                    flush=True,
                )
                return
            if not chat_id_ca:
                parsed_pref = _lark_parse_card_action_value(val_ca)
                try:
                    import offsetleave as _offsetleave_pref

                    ok_pref = isinstance(parsed_pref, dict) and str(
                        parsed_pref.get("k") or ""
                    ).strip().lower() in getattr(
                        _offsetleave_pref,
                        "OFFSETLEAVE_CARD_CALLBACK_KEYS",
                        frozenset(),
                    )
                except Exception:
                    ok_pref = False
                if not ok_pref:
                    print(
                        f"⚠️ card action skipped: missing chat_id event_type={hdr_et!r}",
                        flush=True,
                    )
                    return
            try:
                ev_ca = data.get("event") if isinstance(data.get("event"), dict) else {}
                op_ca = ev_ca.get("operator") if isinstance(ev_ca.get("operator"), dict) else {}
                parsed_ca = _lark_parse_card_action_value(val_ca)
                if isinstance(parsed_ca, dict):
                    thread_r = str(parsed_ca.get("r") or "").strip()
                    if thread_r and chat_id_ca:
                        _set_prod_batch_thread_root(chat_id_ca, thread_r)
                if isinstance(parsed_ca, dict) and str(parsed_ca.get("k") or "").strip().lower() == "test_hi":
                    at_id = (
                        (op_ca.get("open_id") or "").strip()
                        or (sender_id_ca or "").strip()
                        or (op_ca.get("union_id") or "").strip()
                    )
                    if not at_id:
                        print("⚠️ test_hi card: missing operator open_id", flush=True)
                        return
                    send_message(chat_id_ca, f'<at user_id="{at_id}"></at> hi')
                    return
                if isinstance(parsed_ca, dict) and str(parsed_ca.get("k") or "").strip().lower() == "np_check_alt_logs":
                    threading.Thread(
                        target=run_checkcredit_navigator_next_log,
                        args=(chat_id_ca,),
                        daemon=True,
                    ).start()
                    return
                if isinstance(parsed_ca, dict) and str(parsed_ca.get("k") or "").strip().lower() == "np_pick":
                    try:
                        idx_np = int(parsed_ca.get("i"))
                    except (TypeError, ValueError):
                        return
                    pend_np = _get_checkcredit_np_pending(chat_id_ca)
                    choices_np = (pend_np or {}).get("np_choices") or []
                    if pend_np and 1 <= idx_np <= len(choices_np):
                        threading.Thread(
                            target=run_np_third_http_by_choice,
                            args=(chat_id_ca, idx_np),
                            daemon=True,
                        ).start()
                    return
                if isinstance(parsed_ca, dict) and str(parsed_ca.get("k") or "").strip().lower() == "mem_pick":
                    try:
                        idx_mem = int(parsed_ca.get("i"))
                    except (TypeError, ValueError):
                        return
                    try:
                        import chatagent as _chatagent_pick

                        _sender_cands = [
                            (op_ca.get("open_id") or "").strip(),
                            (sender_id_ca or "").strip(),
                            (op_ca.get("union_id") or "").strip(),
                        ]
                        _mem_reply = _chatagent_pick.resolve_recall_pick(
                            chat_id_ca, _sender_cands, idx_mem
                        )
                    except Exception as _mem_err:
                        print(f"⚠️ mem_pick failed: {_mem_err!r}", flush=True)
                        _mem_reply = None
                    if _mem_reply:
                        send_message(chat_id_ca, _mem_reply)
                    else:
                        send_message(
                            chat_id_ca,
                            "ℹ️ That memory list has expired — ask me again "
                            "(e.g. “what did I ask today?”).",
                        )
                    return
                if isinstance(parsed_ca, dict) and str(parsed_ca.get("k") or "").strip().lower() == "cmd_run":
                    cmd_btn = str(parsed_ca.get("c") or "").strip()
                    op_open_cs = (
                        (op_ca.get("open_id") or "").strip()
                        or (sender_id_ca or "").strip()
                        or (op_ca.get("union_id") or "").strip()
                    )
                    if cmd_btn:
                        threading.Thread(
                            target=_reinject_synthetic_command_message,
                            args=(chat_id_ca, op_open_cs, cmd_btn),
                            daemon=True,
                        ).start()
                    return
                if isinstance(parsed_ca, dict) and str(parsed_ca.get("k") or "").strip().lower() == "cmd_chat":
                    def _run_cmd_chat_fallback() -> None:
                        try:
                            import commandsuggest as _cmdsuggest_cb
                            import chatagent as _chatagent_cb

                            _cs_open = (op_ca.get("open_id") or "").strip() or (
                                sender_id_ca or ""
                            ).strip()
                            pend_text = _cmdsuggest_cb.pop_pending_chat(
                                chat_id_ca,
                                [
                                    _cs_open,
                                    (op_ca.get("union_id") or "").strip(),
                                    (sender_id_ca or "").strip(),
                                ],
                            )
                            if not pend_text:
                                send_message(
                                    chat_id_ca,
                                    "ℹ️ That suggestion card expired — please resend your message.",
                                )
                                return
                            _cs_key = _chatagent_cb.memory_session_key(chat_id_ca, _cs_open)
                            chat_reply = _chatagent_cb.reply_if_enabled(
                                pend_text, session_key=_cs_key
                            )
                            if chat_reply:
                                chat_reply = (
                                    _chatagent_cb.sanitize_outbound_chat_reply(chat_reply)
                                    or chat_reply
                                )
                            send_message(
                                chat_id_ca,
                                chat_reply
                                or "Hi! 👋 I'm Duty Bot — ask me about duty/leave/machines, or `/help`.",
                            )
                        except Exception as _cs_err:
                            print(f"⚠️ cmd_chat fallback failed: {_cs_err!r}", flush=True)

                    threading.Thread(target=_run_cmd_chat_fallback, daemon=True).start()
                    return
                if isinstance(parsed_ca, dict) and smmachine.handle_prod_batch_card_callback(
                    parsed_ca,
                    chat_id=chat_id_ca,
                    send_message=make_prod_batch_thread_send(chat_id_ca),
                    action_obj=(
                        ev_ca.get("action") if isinstance(ev_ca.get("action"), dict) else None
                    ),
                ):
                    return
                try:
                    import offsetleave as _offsetleave

                    if isinstance(parsed_ca, dict) and _offsetleave.handle_card_callback(
                        parsed_ca,
                        ev_ca,
                        sender_open_id=sender_id_ca or "",
                        chat_id=chat_id_ca,
                        send_message=send_message,
                        webhook_data=data if isinstance(data, dict) else None,
                    ):
                        return
                except Exception as e:
                    if chat_id_ca:
                        send_message(chat_id_ca, f"❌ Offset/leave submit failed: {e}")
                    else:
                        print(f"❌ Offset/leave submit failed (no chat_id): {e!r}", flush=True)
                    return
                if isinstance(parsed_ca, dict) and str(parsed_ca.get("k") or "").strip().lower() == reminder.MAINT_REMINDER_CONFIRM_KEY:
                    rid_m = str(parsed_ca.get("id") or "").strip()
                    at_id_m = (
                        (op_ca.get("open_id") or "").strip()
                        or (sender_id_ca or "").strip()
                        or (op_ca.get("union_id") or "").strip()
                    )
                    at_prefix = f'<at user_id="{at_id_m}"></at> ' if at_id_m else ""
                    send_message(
                        chat_id_ca,
                        f"{at_prefix}✅ Confirmed: maintenance & test have been set"
                        + (f" (reminder ID `{rid_m}`)." if rid_m else "."),
                    )
                    return
                if isinstance(parsed_ca, dict) and str(parsed_ca.get("k") or "").strip().lower() == "rem_del":
                    rid = str(parsed_ca.get("id") or "").strip()
                    if not rid:
                        send_message(chat_id_ca, "❌ Reminder delete failed: missing ID.")
                        return
                    try:
                        result = reminder.delete_sheet_reminders(
                            ids=[rid],
                            get_token_func=get_tenant_access_token,
                            scheduler=scheduler,
                            send_func=send_message,
                            chat_id=chat_id_ca,
                            target_user_id=TARGET_USER_OPEN_ID,
                            schedule_chat_id=REMINDER_TARGET_CHAT_ID,
                        )
                        send_message(chat_id_ca, result)
                    except Exception as e:
                        send_message(chat_id_ca, f"❌ Reminder delete failed: {e}")
                    return
                if isinstance(parsed_ca, dict) and str(parsed_ca.get("k") or "").strip().lower() == "rem_add_submit":
                    # Handled synchronously in _try_rem_add_submit_card_response (in-place card update).
                    return
                if (
                    isinstance(parsed_ca, dict)
                    and str(parsed_ca.get("k") or "").strip().lower() == "checkcredit_player_submit"
                ):
                    act_ca = ev_ca.get("action") if isinstance(ev_ca.get("action"), dict) else {}
                    machine_raw = _lark_get_card_form_field(act_ca, "machine_type")
                    player_raw = _lark_get_card_form_field(act_ca, "player_id")
                    date_raw = _lark_get_card_form_field(act_ca, "log_date")
                    if isinstance(parsed_ca, dict):
                        fv_cb = parsed_ca.get("form_value")
                        if isinstance(fv_cb, dict):
                            machine_raw = machine_raw or _lark_form_field_text(fv_cb.get("machine_type"))
                            player_raw = player_raw or _lark_form_field_text(fv_cb.get("player_id"))
                            date_raw = date_raw or _lark_form_field_text(fv_cb.get("log_date"))
                        machine_raw = machine_raw or _lark_form_field_text(parsed_ca.get("machine_type"))
                        player_raw = player_raw or _lark_form_field_text(parsed_ca.get("player_id"))
                        date_raw = date_raw or _lark_form_field_text(parsed_ca.get("log_date"))
                    machine_raw = machine_raw or _lark_find_field_deep(ev_ca, "machine_type")
                    player_raw = player_raw or _lark_find_field_deep(ev_ca, "player_id")
                    date_raw = date_raw or _lark_find_field_deep(ev_ca, "log_date")

                    def _normalize_checkcredit_date_iso(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
                        s = str(raw or "").strip()
                        if not s:
                            return None, "Date is empty."
                        if re.match(r"^\d{10,13}$", s):
                            try:
                                ts = int(s)
                                if ts > 10**12:
                                    ts = ts // 1000
                                return datetime.fromtimestamp(ts).strftime("%Y-%m-%d"), None
                            except Exception:
                                return None, "Invalid date timestamp."
                        m = re.match(r"^\s*(\d{4})-(\d{2})-(\d{2})", s)
                        if m:
                            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}", None
                        m2 = re.match(r"^\s*(\d{4})/(\d{2})/(\d{2})", s)
                        if m2:
                            return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}", None
                        return None, "Date must be YYYY-MM-DD (or use the date picker)."

                    date_iso_cb, derr = _normalize_checkcredit_date_iso(date_raw)
                    if derr:
                        send_message(chat_id_ca, f"❌ {derr}")
                        return
                    machine_cb = str(machine_raw or "").strip()
                    player_cb = str(player_raw or "").strip()
                    if not machine_cb or not player_cb:
                        send_message(chat_id_ca, "❌ Please fill Machine type and Player ID.")
                        return
                    assert date_iso_cb is not None
                    threading.Thread(
                        target=run_checkcredit_player_job,
                        args=(chat_id_ca, machine_cb, player_cb, date_iso_cb),
                        daemon=True,
                    ).start()
                    return
                if (
                    isinstance(parsed_ca, dict)
                    and str(parsed_ca.get("k") or "").strip().lower() == "findmachine_submit"
                ):
                    act_ca = ev_ca.get("action") if isinstance(ev_ca.get("action"), dict) else {}
                    fm_env_raw = _lark_get_card_form_field(act_ca, "fm_env")
                    fm_game_raw = _lark_get_card_form_field(act_ca, "fm_game")
                    fm_online_raw = _lark_get_card_form_field(act_ca, "fm_online")
                    fv_fm = parsed_ca.get("form_value")
                    if isinstance(fv_fm, dict):
                        fm_env_raw = fm_env_raw or _lark_form_field_text(fv_fm.get("fm_env"))
                        fm_game_raw = fm_game_raw or _lark_form_field_text(fv_fm.get("fm_game"))
                        fm_online_raw = fm_online_raw or _lark_form_field_text(fv_fm.get("fm_online"))
                    fm_env_raw = fm_env_raw or _lark_find_field_deep(ev_ca, "fm_env")
                    fm_game_raw = fm_game_raw or _lark_find_field_deep(ev_ca, "fm_game")
                    fm_online_raw = fm_online_raw or _lark_find_field_deep(ev_ca, "fm_online")

                    def _run_findmachine_job():
                        try:
                            import findmachine as _fm_mod

                            for _fm_msg in _fm_mod.run_findmachine_query(
                                fm_env_raw, fm_game_raw, fm_online_raw
                            ):
                                send_message(chat_id_ca, _fm_msg)
                        except Exception as _fm_err:
                            print(f"❌ findmachine job: {_fm_err!r}", flush=True)
                            try:
                                send_message(chat_id_ca, f"❌ findmachine failed: {_fm_err}")
                            except Exception:
                                pass

                    threading.Thread(target=_run_findmachine_job, daemon=True).start()
                    return
                if (
                    isinstance(parsed_ca, dict)
                    and str(parsed_ca.get("k") or "").strip().lower() == "vpn_create_submit"
                ):
                    act_ca = ev_ca.get("action") if isinstance(ev_ca.get("action"), dict) else {}
                    vpn_users_raw = _lark_get_card_form_field(act_ca, "vpn_users")
                    vpn_location_raw = _lark_get_card_form_field(act_ca, "vpn_location")
                    fv_vpn = parsed_ca.get("form_value")
                    if isinstance(fv_vpn, dict):
                        vpn_users_raw = vpn_users_raw or _lark_form_field_text(fv_vpn.get("vpn_users"))
                        vpn_location_raw = vpn_location_raw or _lark_form_field_text(
                            fv_vpn.get("vpn_location")
                        )
                    vpn_users_raw = vpn_users_raw or _lark_form_field_text(parsed_ca.get("vpn_users"))
                    vpn_location_raw = vpn_location_raw or _lark_form_field_text(
                        parsed_ca.get("vpn_location")
                    )
                    vpn_users_raw = vpn_users_raw or _lark_find_field_deep(ev_ca, "vpn_users")
                    vpn_location_raw = vpn_location_raw or _lark_find_field_deep(ev_ca, "vpn_location")
                    ju = _get_jenkinsupdate()
                    if not ju:
                        send_message(chat_id_ca, "❌ jenkinsupdate unavailable.")
                        return
                    sender_use = ju.resolve_lark_jenkins_card_sender(
                        chat_id_ca, sender_id_ca or "", op_ca
                    )
                    ju.begin_vpn_run_from_card(
                        chat_id_ca,
                        sender_use or sender_id_ca or "",
                        vpn_users_raw,
                        vpn_location_raw,
                        send_message,
                        lark_message_id=(
                            str(
                                (
                                    (ev_ca.get("context") or {})
                                    if isinstance(ev_ca.get("context"), dict)
                                    else {}
                                ).get("open_message_id")
                                or ev_ca.get("open_message_id")
                                or ""
                            ).strip()
                            or None
                        ),
                    )
                    return
                ju = _get_jenkinsupdate()
                if not ju:
                    print(
                        f"⚠️ card action skipped: jenkinsupdate unavailable chat_id={chat_id_ca!r} "
                        f"event_type={hdr_et!r}",
                        flush=True,
                    )
                    return
                sender_use = ju.resolve_lark_jenkins_card_sender(
                    chat_id_ca, sender_id_ca or "", op_ca
                )
                if not sender_use:
                    print(
                        f"⚠️ card action skipped: could not resolve sender "
                        f"chat_id={chat_id_ca!r} raw_sender={sender_id_ca!r} event_type={hdr_et!r}",
                        flush=True,
                    )
                    return
                ctx_ca = ev_ca.get("context") if isinstance(ev_ca.get("context"), dict) else {}
                card_omid = str(ctx_ca.get("open_message_id") or ev_ca.get("open_message_id") or "").strip() or None
                ju.handle_lark_jenkins_card_action(
                    chat_id_ca, sender_use, val_ca, send_message, operator=op_ca,
                    lark_message_id=card_omid,
                )
            except Exception as ex:
                print(f"❌ card callback worker: {ex!r}", flush=True)
                try:
                    send_message(chat_id_ca, f"❌ Card action failed: {ex}")
                except Exception:
                    pass

        threading.Thread(target=_run_card_callback_worker, daemon=True).start()
        return _lark_http_card_callback_ok()

    # Resolver missed but body has ``event.action`` — never fall through to ``success: true``.
    if card_resolved is None and _lark_payload_has_card_action(data):
        print(
            "[lark] card-like payload (event.action present) but resolver returned None — ACK 200 {}",
            flush=True,
        )
        return _lark_http_card_callback_ok()

    sender_id = None
    sender_union_id = None
    if _lark_is_schema_v2(data):
        event = data.get("event", {})
        sender = event.get("sender", {})
        sid_obj = sender.get("sender_id") or {}
        if isinstance(sid_obj, dict):
            sender_id = sid_obj.get("open_id")
            sender_union_id = sid_obj.get("union_id")
        else:
            sender_id = None
            sender_union_id = None
    else:
        event = data.get("event", {})
        sender_id = event.get("open_id") or event.get("user_id")

    if sender_id and sender_id == BOT_OPEN_ID:
        print("⏭️ Ignoring own message")
        return jsonify({"success": True})

    chat_id = None
    text = None
    message_content_raw = ""
    chat_type = None
    mentions = []
    message_id = None
    is_mention_old = False
    lark_message_type = None

    if data.get("header", {}).get("event_type") == "im.message.receive_v1":
        event = data.get("event", {})
        message = event.get("message", {})
        chat_id = message.get("chat_id")
        message_id = message.get("message_id")
        chat_type = message.get("chat_type")
        lark_message_type = (message.get("message_type") or "").strip() or None
        mentions = message.get("mentions", [])
        message_content_raw = message.get("content") or "{}"
        try:
            text = _lark_extract_message_text(message_content_raw)
        except Exception as ex:
            print(f"[lark] content parse failed: {ex!r}", flush=True)
            text = ""
    elif data.get("type") == "event_callback":
        event = data.get("event", {})
        chat_id = event.get("open_chat_id") or event.get("chat_id")
        message_id = event.get("open_message_id") or event.get("message_id")
        chat_type = event.get("chat_type")
        lark_message_type = (
            event.get("message_type") or event.get("msg_type") or ""
        ).strip() or None
        mentions = event.get("mentions", [])
        is_mention_old = event.get("is_mention", False)
        message_content_raw = event.get("content") or "{}"
        text = event.get("text_without_at_bot") or event.get("text", "")
        if not text:
            try:
                text = _lark_extract_message_text(message_content_raw)
            except Exception:
                text = ""
        elif not message_content_raw or message_content_raw == "{}":
            message_content_raw = json.dumps({"text": text})
    else:
        het = _lark_header_event_type(data)
        if _lark_ack_only_event_type(het):
            return _lark_im_done()
        print("⚠️ Unknown webhook branch hdr_et=%r (not im.message / event_callback)" % (het,), flush=True)
        # Card callbacks need HTTP 200 + ``{}`` (or toast). **Do not** use that ACK for every schema-2.0
        # event — bot menu / approvals etc. expect ``{"success": true}`` or they show ``code: undefined``.
        if _lark_payload_has_card_action(data) or (
            het and het.lower().startswith("card.action")
        ):
            return _lark_http_card_callback_ok()
        return _lark_im_done()

    if _lark_skip_stale_event_on_startup(data):
        print(
            f"⏭️ Stale event ignored (before bot start) message_id={message_id!r}",
            flush=True,
        )
        return _lark_im_done()

    if message_id and _remember_processed_message_id(message_id):
        print(f"⏭️ Duplicate message {message_id} ignored")
        return _lark_im_done()

    if not chat_id or text is None:
        # Card callbacks can be mis-parsed as ``im.message`` shape but lack chat/text — **400 breaks the client**.
        if hdr_et.startswith("card.action") or _lark_payload_has_card_action(data):
            print(
                "[lark] Missing chat_id/text on card-shaped POST — ACK {} (avoid 400 on interaction)",
                flush=True,
            )
            return _lark_http_card_callback_ok()
        print("❌ Could not extract chat_id or text")
        return jsonify({"error": "Missing data"}), 400

    if maintenance.is_evo_batch_forward_only_chat(chat_id) or _is_announce_only_chat(chat_id):
        print(
            f"⏭️ announce-only / forward-only group — ignoring inbound message ({chat_id})",
            flush=True,
        )
        return _lark_im_ack()

    original_text = text
    print(
        f"📝 Original text: {repr(original_text)} sender={sender_id!r} "
        f"content_len={len(message_content_raw or '')}",
        flush=True,
    )

        # 清理提及占位符（先做清理，便于后续命令处理）
    mention_keys = [m.get("key", "") for m in mentions if m.get("key")]
    for key in mention_keys:
        text = text.replace(key, "")
    text = re.sub(r'@_user_\d+', '', text)
    text = re.sub(r'<[^>]+>', '', text)
    clean_text_multiline = re.sub(r'[ \t]+\n', '\n', text).strip()
    clean_text_multiline = re.sub(r'\n[ \t]+', '\n', clean_text_multiline)
    clean_text = re.sub(r'\s+', ' ', clean_text_multiline).strip()
    print(f"🧹 Cleaned text (repr): {repr(clean_text)}")

    _pipeline_t0 = time.perf_counter()

    def _pipeline_mark(stage: str) -> None:
        print(
            f"[pipeline] +{(time.perf_counter() - _pipeline_t0) * 1000:.0f}ms {stage}",
            flush=True,
        )

    _pipeline_mark(f"msg {clean_text[:60]!r}")

    jenkins_bot_oid = _jenkins_bot_open_id()
    is_jenkins_bot_sender = bool(
        sender_id and jenkins_bot_oid and sender_id == jenkins_bot_oid
    )
    try:
        import updatemore as _updatemore

        duty_blob = _updatemore.resolve_duty_command_body(
            original_text, clean_text, message_content_raw
        )
    except Exception:
        duty_blob = (clean_text or original_text or message_content_raw or "").strip()

    # Group (and unknown chat_type): require @Duty Bot; p2p: always respond.
    bot_mentioned = chat_type == "p2p"
    if chat_type != "p2p":
        if chat_type is None:
            print("[lark] chat_type missing — treating as group (require @mention)", flush=True)
        bot_mentioned = False
        for mention in mentions:
            mention_id_obj = mention.get("id")
            if isinstance(mention_id_obj, dict):
                mention_id = mention_id_obj.get("open_id", "")
            else:
                mention_id = mention_id_obj
            print(f"🔍 Mention open_id: {mention_id}")
            if mention_id == BOT_OPEN_ID:
                bot_mentioned = True
                print(f"✅ Bot mentioned by open_id: {mention_id}")
                break
        if not bot_mentioned and is_mention_old:
            bot_mentioned = True
            print("✅ Bot mentioned (old schema via is_mention flag)")

    # Duty commands from any sender (human or bot) — e.g. ``/replyupdateemail`` without strict @ parsing.
    if chat_type == "group" and not bot_mentioned:
        try:
            import updatemore as _updatemore

            if _updatemore.is_jenkinsbot_duty_command(duty_blob):
                bot_mentioned = True
                print("✅ Jenkins/duty email command — treat as mentioned (any sender)")
            elif is_jenkins_bot_sender and (
                _updatemore.is_reply_update_email_text(message_content_raw or "")
                or _updatemore.is_jenkinsbot_duty_command(message_content_raw or "")
            ):
                bot_mentioned = True
                print("✅ Jenkinsbot sender duty command — treat as mentioned")
        except Exception:
            if re.search(r"/?replyupdateemail\b", duty_blob or "", re.I):
                bot_mentioned = True

    # Reply **1**–**4** after `/checkcreditdate` NP prompt — works in group **without** @bot
    stripped_choice = clean_text.strip()
    if stripped_choice in ("1", "2", "3", "4"):
        pend_np = _get_checkcredit_np_pending(chat_id)
        choices_np = (pend_np or {}).get("np_choices") or []
        idx_np = int(stripped_choice)
        if pend_np and 1 <= idx_np <= len(choices_np):
            start_lark_background_thread(run_np_third_http_by_choice, chat_id, idx_np)
            return _lark_im_done()

    # jenkinsbot / any sender → duty email callbacks — no @duty required when command is present.
    try:
        import updatemore as _updatemore

        _jb_duty_cmd = _updatemore.is_jenkinsbot_duty_command(duty_blob)
        if not _jb_duty_cmd:
            _jb_duty_cmd = _updatemore.is_reply_update_email_text(duty_blob or "")
        if not _jb_duty_cmd:
            for _scan in (clean_text, original_text, message_content_raw):
                if _scan and (
                    _updatemore.is_jenkinsbot_duty_command(_scan)
                    or _updatemore.is_reply_update_email_text(_scan)
                ):
                    _jb_duty_cmd = True
                    break
        if not _jb_duty_cmd and is_jenkins_bot_sender:
            _jb_duty_cmd = (
                _updatemore.is_jenkinsbot_duty_command(message_content_raw or "")
                or _updatemore.is_reply_update_email_text(message_content_raw or "")
            )
        if not _jb_duty_cmd and is_jenkins_bot_sender and _mention_includes_duty_bot(mentions):
            _jb_duty_cmd = _updatemore.is_reply_update_email_text(message_content_raw or "")
    except Exception:
        _jb_duty_cmd = bool(re.search(r"/?replyupdateemail\b", duty_blob or "", re.I))

    if _jb_duty_cmd:
        _duty_orig = duty_blob or original_text
        _duty_clean = duty_blob or clean_text
        print(
            f"[lark] jenkins duty cmd sender={sender_id!r} jenkinsbot={is_jenkins_bot_sender} "
            f"body={_duty_orig!r}",
            flush=True,
        )
        if _dispatch_jenkins_duty_command(
            chat_id,
            sender_id or "",
            _duty_clean,
            _duty_orig,
            send_message,
            message_content_raw=message_content_raw,
        ):
            return _lark_im_done()
        if is_jenkins_bot_sender or _jb_duty_cmd:
            send_message(
                chat_id,
                "❌ **Duty bot** saw a jenkinsbot command but could not handle it.\n"
                f"Body: `{(_duty_orig or '')[:200]}`",
            )
            return _lark_im_done()

    ju = _get_jenkinsupdate()
    jenkins_sess_active = (
        ju.jenkins_update_has_active_lark_session(chat_id, sender_id) if ju else False
    )

    if chat_type != "p2p" and not bot_mentioned and not jenkins_sess_active:
        if is_jenkins_bot_sender:
            print(
                f"⏭️ Jenkinsbot message ignored (no duty command) text={original_text!r} "
                f"content={message_content_raw[:240]!r}",
                flush=True,
            )
        else:
            print("⏭️ Bot not mentioned in group chat – ignoring further commands")
        return _lark_im_ack()

    set_lark_incoming_message(message_id, chat_id)
    if message_id and (chat_type == "p2p" or bot_mentioned):
        remember_gotit_reaction(add_gotit_reaction(message_id))

    if text == "我要验牌":
        reply = f'<at user_id="{sender_id}"></at> 给我擦皮鞋'
        send_message(chat_id, reply)
        return _lark_im_done()

    if text == "good luck" or text == "Good luck":
        add_heart_reaction(message_id)
        return _lark_im_done()

    if text == "random":
        add_random_reaction(message_id)
        return _lark_im_done()

    if text == "spamreact":
        add_all_reactions(message_id)
        return _lark_im_done()

    _full_body = _lark_full_message_body(
        original_text, clean_text_multiline, message_content_raw
    )
    _chat_memory_key = None
    try:
        import chatagent as _chatagent_mem

        if chat_id and sender_id:
            _chat_memory_key = _chatagent_mem.memory_session_key(chat_id, sender_id)
    except Exception:
        pass

    # "Show your thinking" — when the user asks a question AND wants to see the
    # AI's reasoning (e.g. "... also i want to know what ai thinking"), reply with
    # a two-part "What im thinking / What is my answer" message. Guarded so any
    # failure falls through to normal handling.
    try:
        import aithinking as _aithinking

        _ait_src = (clean_text_multiline or clean_text or "").strip()
        if (
            (chat_type == "p2p" or bot_mentioned)
            and _ait_src
            and _aithinking.wants_ai_thinking(_ait_src)
        ):
            _ait_reply = _aithinking.answer_with_thinking(
                _ait_src, session_key=_chat_memory_key
            )
            if _ait_reply:
                send_message(chat_id, _ait_reply)
                print(f"🧠 AI-thinking reply to chat {chat_id}", flush=True)
                return _lark_im_done()
    except Exception as _ait_err:
        print(f"⚠️ AI-thinking skipped: {_ait_err!r}", flush=True)

    if _try_missing_credit_inquiry(
        chat_id,
        _full_body,
        bot_mentioned=bot_mentioned,
        message_id=message_id,
        send_func=send_message,
    ):
        return _lark_im_done()

    if _try_lark_vision_reply(
        chat_id,
        message_id,
        message_content_raw,
        message_type=lark_message_type,
        user_text=clean_text_multiline or clean_text,
        bot_mentioned=bot_mentioned,
        send_func=send_message,
        session_key=_chat_memory_key,
    ):
        return _lark_im_done()

    update_thread_root = None
    if data.get("header", {}).get("event_type") == "im.message.receive_v1":
        msg_obj = (data.get("event") or {}).get("message") or {}
        update_thread_root = _prod_batch_thread_root_from_incoming_message(
            msg_obj, message_id=message_id
        )
    else:
        update_thread_root = (message_id or "").strip() or None
    if ju and ju.handle_lark_jenkins_update_message(
        chat_id,
        sender_id,
        _full_body,
        _full_body,
        send_message,
        allow_start=bot_mentioned,
        lark_sender_union_id=sender_union_id,
        lark_message_id=(message_id or "").strip() or None,
        lark_thread_root_id=update_thread_root,
    ):
        return _lark_im_done()

    if ju is None and _looks_like_jenkins_nl_update(_full_body):
        send_message(
            chat_id,
            "⚠️ **Jenkins `/update` is not available on this PC.**\n"
            "Install Playwright so the bot can fill the form and show **Confirm / Cancel** "
            "(it will **not** auto-build without your click):\n"
            "```\npip install playwright\nplaywright install chromium\n```\n"
            "Then restart `python main.py` (with LARK_EVENT_MODE=websocket if using long connection).\n"
            "Or paste: `@Duty Bot /jenkinsupdate rc uat master` + Branch/Version/Services block.",
        )
        return _lark_im_done()

    if bot_help.handle_help_command(
        clean_text,
        chat_id=chat_id,
        send_message=send_message,
        jenkins_available=_get_jenkinsupdate() is not None,
    ):
        return _lark_im_done()

    # Admin deploy/restart — rule match only, before router/LLM/chat.
    if (bot_mentioned or chat_type == "p2p") and _secret_command_allowed(sender_id):
        try:
            import commandagent as _ops_ca

            if _ops_ca.detect_git_pull_restart_command(clean_text):
                _handle_git_pull_restart_deploy(chat_id)
                return _lark_im_done()
            if _ops_ca.detect_restart_services_command(clean_text):
                _handle_restart_services(chat_id)
                return _lark_im_done()
        except Exception as _ops_err:
            print(f"⚠️ Admin ops (early) skipped: {_ops_err!r}", flush=True)

    # 初始化回复变量
    reply = ""
    if game.has_active_game(sender_id):
        reply, should_clear, job_id = game.check_answer(sender_id, clean_text)
        if reply:
            if job_id:
                try:
                    scheduler.remove_job(job_id)
                except Exception as e:
                    print(f"⚠️ Could not cancel job {job_id}: {e}")
            send_message(chat_id, reply)
        return _lark_im_done()
    
    

    # Offset/leave slash + rule commands run BEFORE any LLM (offsetai / chatagent).
    try:
        import offsetleave as _offsetleave

        if _offsetleave.handle_offset_slash_commands(
            clean_text,
            sender_open_id=sender_id or "",
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_tenant_access_token,
        ):
            return _lark_im_done()

        if _offsetleave.handle_showoffset(
            clean_text,
            chat_id=chat_id,
            send_message=send_message,
            sender_open_id=sender_id or "",
            get_token_func=get_tenant_access_token,
        ):
            return _lark_im_done()

        if _offsetleave.handle_offset_query(
            clean_text,
            sender_open_id=sender_id or "",
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_tenant_access_token,
        ):
            return _lark_im_done()

        if _offsetleave.handle_editoffset_command(
            clean_text,
            sender_open_id=sender_id or "",
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_tenant_access_token,
        ):
            return _lark_im_done()

        if _offsetleave.handle_deleteoffset_command(
            clean_text,
            sender_open_id=sender_id or "",
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_tenant_access_token,
        ):
            return _lark_im_done()

        if _offsetleave.handle_pendingoffset_command(
            clean_text,
            sender_open_id=sender_id or "",
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_tenant_access_token,
        ):
            return _lark_im_done()

        if _offsetleave.handle_mention(
            clean_text,
            sender_open_id=sender_id or "",
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_tenant_access_token,
        ):
            return _lark_im_done()
    except Exception as e:
        send_message(chat_id, f"❌ Offset/leave form failed: {e}")
        return _lark_im_done()

    _pipeline_mark("offset block done")

    # Natural English → slash command (rules first, LLM only if rules abstain).
    _skip_commandagent = False
    _router_decision = None
    try:
        import chathandleagent as _router

        _rt0 = time.perf_counter()
        _router_decision = _router.route(
            _full_body or clean_text_multiline or clean_text,
            bot_mentioned=bot_mentioned,
        )
        _pipeline_mark(
            f"router done ({(time.perf_counter() - _rt0) * 1000:.0f}ms, "
            f"reason={getattr(_router_decision, 'reason', '?')})"
        )
        print(
            f"🧭 Router: {clean_text!r} → {_router_decision.kind} "
            f"(reason={_router_decision.reason}, cmd_conf={_router_decision.command_conf:.2f}, "
            f"chat_conf={_router_decision.chat_conf:.2f})",
            flush=True,
        )
        if _router_decision.is_chat:
            _skip_commandagent = True
    except Exception as _router_err:
        print(f"⚠️ Router skipped (fallback to chitchat gate): {_router_err!r}", flush=True)
        try:
            import chitchat as _chitchat_gate

            if _chitchat_gate.looks_like_chitchat(clean_text):
                _skip_commandagent = True
                print(f"💬 Chitchat gate: skip commandagent for {clean_text!r}", flush=True)
        except Exception:
            pass

    _is_routed_command = bool(
        _router_decision is not None and getattr(_router_decision, "is_command", False)
    )

    if not _skip_commandagent:
        try:
            import commandagent as _commandagent

            _multi_cmds = _commandagent.detect_multi_duty_commands(clean_text)
            if _multi_cmds:
                _multi_reply = _build_multi_duty_reply(_multi_cmds)
                if _multi_reply:
                    send_message(chat_id, _multi_reply)
                    print(
                        f"✅ Multi-duty reply ({len(_multi_cmds)} depts): {_multi_cmds}",
                        flush=True,
                    )
                    return _lark_im_done()
                print(
                    f"⚠️ Multi-duty detected {_multi_cmds} but no handler output — fallback",
                    flush=True,
                )

            ai_command = None
            if (
                _commandagent.is_enabled()
                and _router_decision is not None
                and getattr(_router_decision, "command", None)
                # An explicit slash command is never rewritten — in groups the router sees
                # the body WITH the "@_user_1" placeholder, misses its slash fast-path, and
                # its content heuristics can misroute pastes like "/stresstest <notice>".
                and not (clean_text or "").lstrip().startswith("/")
            ):
                cand = _router_decision.command
                if cand and cand != clean_text:
                    ai_command = cand
            if not ai_command:
                ai_command = _commandagent.translate_if_enabled(clean_text)
            if ai_command:
                print(f"🤖 Command agent map: {clean_text!r} → {ai_command.splitlines()[0]!r}", flush=True)
                clean_text = ai_command
        except Exception as _commandagent_err:
            print(f"⚠️ Command agent skipped (bot continues without AI): {_commandagent_err!r}", flush=True)

    _pipeline_mark("commandagent done")

    # Offset LLM agent — only after slash/rule command handlers above.
    try:
        import offsetai as _offsetai

        if _offsetai.handle(
            clean_text,
            sender_open_id=sender_id or "",
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_tenant_access_token,
            session_key=_chat_memory_key,
        ):
            return _lark_im_done()
    except Exception as _offsetai_err:
        print(f"⚠️ offsetai skipped: {_offsetai_err!r}", flush=True)

    _pipeline_mark("offsetai done")

    # Auto-detect EVO Service Desk batch paste even WITHOUT the `/m` command.
    # Only fires on the distinctive ``※SD-xxxxx※`` multi-block format, so normal
    # messages and other commands are never intercepted. We rebuild the source from
    # ``original_text`` (preserving newlines, which ``clean_text`` collapses) and strip
    # an optional leading ``/m`` so an explicit command still works here too.
    try:
        _evo_src = original_text or ""
        for _mk in mention_keys:
            _evo_src = _evo_src.replace(_mk, "")
        _evo_src = re.sub(r"@_user_\d+", "", _evo_src)
        _evo_src = re.sub(r"<[^>]+>", "", _evo_src)
        _evo_cmd = re.search(r"(?:^|\s)/m\s+", _evo_src, re.IGNORECASE)
        if _evo_cmd:
            _evo_src = _evo_src[_evo_cmd.end():]
        _evo_src = _evo_src.strip()
        if _evo_src.startswith('"') and _evo_src.endswith('"'):
            _evo_src = _evo_src[1:-1].strip()
        if _evo_src and maintenance.is_evo_sd_batch_paste(_evo_src):
            _process_evo_sd_batch_paste(chat_id, _evo_src)
            return _lark_im_done()
    except Exception as _evo_auto_err:
        print(f"⚠️ EVO batch auto-detect skipped: {_evo_auto_err!r}", flush=True)

    # Respect the router: if it already decided this is a COMMAND (e.g. /checkperson,
    # a maintenance paste, a credit check), do NOT let the math/chat shortcut below
    # hijack it. A pasted report can contain dates/amounts that superficially look like
    # arithmetic ("2026/01/01 00:00:00 - 2026/06/03"), which previously produced a
    # bogus "couldn't parse that calculation" reply instead of running the command.
    try:
        import chatagent as _math_agent

        _math_src = (clean_text_multiline or clean_text or "").strip()
        if _math_src and not _is_routed_command:
            if _math_agent.looks_like_math_question(_math_src):
                _math_reply = _math_agent.resolve_math_from_context(
                    _math_src, session_key=_chat_memory_key
                )
                if not _math_reply:
                    _math_reply = _math_agent.math_parse_failure_message(_math_src)
                if _math_reply:
                    _math_reply = (
                        _math_agent.sanitize_outbound_chat_reply(_math_reply)
                        or _math_reply
                    )
                    _math_agent.remember_chat_turn(
                        _chat_memory_key, _math_src, _math_reply
                    )
                    send_message(chat_id, _math_reply)
                    print(f"💬 Math reply to chat {chat_id}", flush=True)
                    return _lark_im_done()
            elif _math_agent.looks_like_math_followup(_math_src):
                _math_reply = _math_agent.resolve_math_from_context(
                    _math_src, session_key=_chat_memory_key
                )
                if _math_reply:
                    _math_reply = (
                        _math_agent.sanitize_outbound_chat_reply(_math_reply)
                        or _math_reply
                    )
                    _math_agent.remember_chat_turn(
                        _chat_memory_key, _math_src, _math_reply
                    )
                    send_message(chat_id, _math_reply)
                    print(f"💬 Math follow-up reply to chat {chat_id}", flush=True)
                    return _lark_im_done()
            elif (
                (bot_mentioned or chat_type == "p2p")
                and (
                    _math_agent.has_pending_recall(_chat_memory_key)
                    or _math_agent.looks_like_memory_recall(_math_src)
                    or _math_agent.looks_like_math_memory_recall(_math_src)
                    or _math_agent.looks_like_today_memory_recall(_math_src)
                    or _math_agent.looks_like_week_memory_recall(_math_src)
                    or _math_agent.looks_like_memory_capability_question(_math_src)
                    or _math_agent.looks_like_vague_memory_recall(_math_src)
                )
            ):
                _recall_reply = _math_agent.try_memory_recall_reply(
                    _math_src, session_key=_chat_memory_key
                )
                if _recall_reply:
                    _math_agent.remember_chat_turn(
                        _chat_memory_key, _math_src, _recall_reply
                    )
                    # Ambiguous recall → interactive card with numbered buttons
                    # (fallback: the plain numbered-list text).
                    _recall_card = _math_agent.build_recall_choice_card(
                        _chat_memory_key, _recall_reply
                    )
                    _card_sent = False
                    if isinstance(_recall_card, dict):
                        try:
                            _resp_rc = send_message(
                                chat_id,
                                json.dumps(_recall_card, ensure_ascii=False),
                                msg_type="interactive",
                            )
                            _card_sent = not (
                                isinstance(_resp_rc, dict)
                                and _resp_rc.get("code") not in (0, None)
                            )
                        except Exception as _rc_err:
                            print(
                                f"⚠️ recall choice card failed: {_rc_err!r}", flush=True
                            )
                    if not _card_sent:
                        send_message(chat_id, _recall_reply)
                    print(f"💬 Memory recall reply to chat {chat_id}", flush=True)
                    return _lark_im_done()
    except Exception as _math_err:
        print(f"⚠️ Deterministic chat shortcut skipped: {_math_err!r}", flush=True)

    # AI duty/leave assistant (read-only): free-form requests like "i want ose duty
    # tmmr", "fpms after two days", "fpms and cpms today", "ose next month 16", or
    # "this week who on leave" → emoji message card(s). The handler has its own strict
    # keyword/date gate and returns None for anything that isn't a duty/leave query,
    # so normal command/chat routing below is untouched. Never raises into the hot path.
    try:
        import dutyai as _dutyai

        _dutyai_src = (clean_text_multiline or clean_text or "").strip()
        _run_dutyai = bool(_dutyai_src)
        if _run_dutyai and (clean_text or "").strip().startswith("/") and _is_routed_command:
            if not _dutyai.message_needs_dutyai_cards(_dutyai_src):
                _run_dutyai = False
                print(
                    f"⏭️ dutyai skipped — already mapped to {clean_text.splitlines()[0]!r}",
                    flush=True,
                )
        _dutyai_payloads = _dutyai.handle(_dutyai_src, session_key=_chat_memory_key) if _run_dutyai else None
        if _dutyai_payloads:
            for _p in _dutyai_payloads:
                _card = _p.get("lark_card") if isinstance(_p, dict) else None
                if isinstance(_card, dict):
                    _resp = send_message(
                        chat_id, json.dumps(_card, ensure_ascii=False), msg_type="interactive"
                    )
                    if isinstance(_resp, dict) and _resp.get("code") not in (0, None) and _p.get("text"):
                        send_message(chat_id, _p["text"])
                elif isinstance(_p, dict) and _p.get("text"):
                    send_message(chat_id, _p["text"])
            print(
                f"🗂️ dutyai handled {clean_text!r} → {len(_dutyai_payloads)} card(s)",
                flush=True,
            )
            return _lark_im_done()
    except Exception as _dutyai_err:
        print(f"⚠️ dutyai skipped (bot continues normally): {_dutyai_err!r}", flush=True)

    _pipeline_mark("dutyai done")

    # 命令处理
    if clean_text.lower() == "/test":
        send_message(chat_id, _lark_test_card_json(), msg_type="interactive")
        return _lark_im_done()

    elif re.match(r"^(?:/)?offset\s*$", clean_text, re.I):
        try:
            import offsetleave as _offsetleave_cmd

            if _offsetleave_cmd.handle_offset_slash_commands(
                clean_text,
                sender_open_id=sender_id or "",
                chat_id=chat_id,
                chat_type=chat_type,
                send_message=send_message,
                get_token_func=get_tenant_access_token,
            ):
                return _lark_im_done()
        except Exception as _offset_cmd_err:
            send_message(chat_id, f"❌ Offset command failed: {_offset_cmd_err}")
            return _lark_im_done()

    elif re.match(r"^(?:/)?(?:deleteoffset|editoffset|pendingoffset|showoffset)\b", clean_text, re.I):
        try:
            import offsetleave as _offsetleave_cmd

            if _offsetleave_cmd.handle_offset_slash_commands(
                clean_text,
                sender_open_id=sender_id or "",
                chat_id=chat_id,
                chat_type=chat_type,
                send_message=send_message,
                get_token_func=get_tenant_access_token,
            ):
                return _lark_im_done()
        except Exception as _offset_cmd_err:
            send_message(chat_id, f"❌ Offset command failed: {_offset_cmd_err}")
            return _lark_im_done()

    elif len(clean_text) >= 3 and clean_text[:3].lower() == '/s ':
        query = clean_text[3:].strip()
        print(f"🔍 Duty query extracted: '{query}'")
        reply = search_duty(query)
        try:
            import chitchat as _chitchat_s

            if reply.startswith("No matching duty personnel found") and _chitchat_s.looks_like_chitchat(query):
                reply = ""
        except Exception:
            pass
    elif clean_text.lower() == '/date':
        today = get_today_date()
        reply = f"Today's date is {today}."
    elif re.match(r"^/leavewfh\b", clean_text, re.I) or re.match(r"^/wfhleave\b", clean_text, re.I):
        _send_month_attendance_card(chat_id, clean_text, "both")
        return _lark_im_done()
    elif re.match(r"^/leave\b", clean_text, re.I):
        _send_month_attendance_card(chat_id, clean_text, "leave")
        return _lark_im_done()
    elif re.match(r"^/wfh\b", clean_text, re.I):
        _send_month_attendance_card(chat_id, clean_text, "wfh")
        return _lark_im_done()
    elif re.match(r"^/wholeave\b", clean_text, re.I):
        try:
            import leavewfh as _leavewfh
        except ImportError:
            import leave as _leavewfh  # type: ignore[no-redef]

        payload = _leavewfh.get_wholeave_today_payload()
        card = payload.get("lark_card")
        if isinstance(card, dict):
            resp = send_message(chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive")
            if resp.get("code") != 0:
                send_message(chat_id, payload.get("text") or "❌ Wholeave card failed.")
        else:
                send_message(chat_id, payload.get("text") or "❌ Could not load OSE leave Bitable.")
        return _lark_im_done()
    elif clean_text.lower() == '/holiday':
        reply = format_holidays()
    elif clean_text.lower() == '/holidaythismonth':
        reply = holidays_this_month()
    elif clean_text.lower() == '/miao':
        reply = get_miao()
    elif clean_text.lower() == '/lucifer':
        reply = lucifer()
    elif clean_text.lower() == '/dog':
        reply = dog()
    elif clean_text.lower() == '/freewifi':
        picture_path = get_picture1_path()
        if not os.path.isfile(picture_path):
            send_message(chat_id, "❌ picture1 not found.")
            return _lark_im_done()
        key = upload_image_lark(picture_path)
        if not key:
            send_message(chat_id, "❌ Failed to upload picture1.")
            return _lark_im_done()
        result = send_image_message(chat_id, key)
        if result.get("code") != 0:
            send_message(chat_id, f"❌ Failed to send picture1: {result}")
        return _lark_im_done()
    elif clean_text.lower() == '/manchung':
        picture_path = get_manchung_path()
        if not os.path.isfile(picture_path):
            send_message(chat_id, "❌ manchung picture not found.")
            return _lark_im_done()
        key = upload_image_lark(picture_path)
        if not key:
            send_message(chat_id, "❌ Failed to upload manchung picture.")
            return _lark_im_done()
        result = send_image_message(chat_id, key)
        if result.get("code") != 0:
            send_message(chat_id, f"❌ Failed to send manchung picture: {result}")
        return _lark_im_done()
    elif clean_text.lower() == '/picture cat':
        file_token = get_cat_file_token()
        if file_token:
            result = send_file(chat_id, file_token)
            if result.get("code") != 0:
                send_message(chat_id, f"❌ Failed to send cat picture: {result}")
        else:
            send_message(chat_id, "❌ Failed to upload cat picture.")
        return _lark_im_done()
    elif re.match(r"(?i)^/(?:checkperson|check_person|whochecks|findcheckperson)\b", clean_text):
        try:
            import checkperson as _checkperson

            _cp_src = (clean_text_multiline or _full_body or clean_text or "").strip()
            _cp_body = _checkperson.strip_command(_cp_src)
            if not _cp_body:
                send_message(chat_id, _checkperson.USAGE)
            else:
                _cp_card, _cp_text = _checkperson.build_card(_cp_body)
                if _cp_card:
                    _cp_resp = send_message(
                        chat_id, json.dumps(_cp_card, ensure_ascii=False), msg_type="interactive"
                    )
                    if _cp_resp.get("code") != 0:
                        send_message(chat_id, _cp_text)
                else:
                    send_message(chat_id, _cp_text)
        except Exception as _cp_err:
            print(f"⚠️ checkperson failed: {_cp_err!r}", flush=True)
            send_message(chat_id, "❌ Could not find the check person right now. Please try again.")
        return _lark_im_done()
    elif clean_text.lower() == '/fpms':
        _send_duty_card("fpms", fpms_duty.get_fpms_today_duty(), chat_id)
        return _lark_im_done()
    elif clean_text.lower().startswith('/fpmscheck'):
        parts = clean_text.split()
        if len(parts) > 1:
            try:
                date_str = parts[1]
                if '/' in date_str:
                    month, year = map(int, date_str.split('/'))
                elif '-' in date_str:
                    year, month = map(int, date_str.split('-'))
                else:
                    raise ValueError
                reply = fpms_duty.fpms_check(month=month, year=year)
            except ValueError:
                reply = "❌ 格式错误。请使用 `/fpmscheck MM/YYYY` 或 `/fpmscheck YYYY-MM`"
        else:
            reply = fpms_duty.fpms_check()
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower() == '/pms':
        _send_duty_card("pms", pms_duty.dutyNextDay(), chat_id)
        return _lark_im_done()
    elif clean_text.lower().startswith('/pmscheck'):
        parts = clean_text.split()
        if len(parts) > 1:
            try:
                date_str = parts[1]
                if '/' in date_str:
                    month, year = map(int, date_str.split('/'))
                elif '-' in date_str:
                    year, month = map(int, date_str.split('-'))
                else:
                    raise ValueError
                reply = pms_duty.pmsCheck(month=month, year=year)
            except ValueError:
                reply = "❌ Invalid format. Use `/pmscheck MM/YYYY` or `/pmscheck YYYY-MM`"
        else:
            reply = pms_duty.pmsCheck()
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower() == '/bi':
        _send_duty_card("bi", bi_duty.get_bi_today_duty(), chat_id)
        return _lark_im_done()
    elif clean_text.lower().startswith('/bicheck'):
        parts = clean_text.split()
        if len(parts) > 1:
            try:
                date_str = parts[1]
                if '/' in date_str:
                    month, year = map(int, date_str.split('/'))
                elif '-' in date_str:
                    year, month = map(int, date_str.split('-'))
                else:
                    raise ValueError
                reply = bi_duty.bi_check(month=month, year=year)
            except ValueError:
                reply = "❌ 格式错误。请使用 `/bicheck MM/YYYY` 或 `/bicheck YYYY-MM`"
        else:
            reply = bi_duty.bi_check()
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower() == '/fe':
        _send_duty_card("fe", fe_duty.get_fe_next_three_duty(), chat_id)
        return _lark_im_done()
    elif clean_text.lower().startswith('/fecheck'):
        parts = clean_text.split()
        if len(parts) > 1:
            try:
                date_str = parts[1]
                if '/' in date_str:
                    month, year = map(int, date_str.split('/'))
                elif '-' in date_str:
                    year, month = map(int, date_str.split('-'))
                else:
                    raise ValueError
                reply = fe_duty.fe_check(month=month, year=year)
            except ValueError:
                reply = "❌ 格式错误。请使用 `/fecheck MM/YYYY` 或 `/fecheck YYYY-MM`"
        else:
            reply = fe_duty.fe_check()
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower() == '/cpms':
        _send_payload_card(cpms_duty.get_cpms_payload(), chat_id)
        return _lark_im_done()
    elif clean_text.lower().startswith('/cpmscheck'):
        parts = clean_text.split()
        if len(parts) > 1:
            try:
                date_str = parts[1]
                if '/' in date_str:
                    month, year = map(int, date_str.split('/'))
                elif '-' in date_str:
                    year, month = map(int, date_str.split('-'))
                else:
                    raise ValueError
                reply = cpms_duty.cpms_check(month=month, year=year)
            except ValueError:
                reply = "❌ 格式错误。请使用 `/cpmscheck MM/YYYY` 或 `/cpmscheck YYYY-MM`"
        else:
            reply = cpms_duty.cpms_check()
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower() == '/sre':
        _send_duty_card("sre", sre_Duty.get_sre_week_duty(), chat_id)
        return _lark_im_done()
    elif clean_text.lower().startswith('/srecheck'):
        parts = clean_text.split()
        if len(parts) > 1:
            try:
                date_str = parts[1]
                if '/' in date_str:
                    month, year = map(int, date_str.split('/'))
                elif '-' in date_str:
                    year, month = map(int, date_str.split('-'))
                else:
                    raise ValueError
                reply = sre_Duty.sre_check(month=month, year=year)
            except ValueError:
                reply = "❌ 格式错误。请使用 `/srecheck MM/YYYY` 或 `/srecheck YYYY-MM`"
        else:
            reply = sre_Duty.sre_check()
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower() in ('/db', '/dba'):
        _send_duty_card("db", db_duty.get_three_weeks_summary(), chat_id)
        return _lark_im_done()
    elif clean_text.lower().startswith('/dbcheck'):
        parts = clean_text.split()
        if len(parts) > 1:
            try:
                date_str = parts[1]
                if '/' in date_str:
                    month, year = map(int, date_str.split('/'))
                elif '-' in date_str:
                    year, month = map(int, date_str.split('-'))
                else:
                    raise ValueError
                reply = db_duty.db_check(month=month, year=year)
            except ValueError:
                reply = "❌ 格式错误。请使用 `/dbcheck MM/YYYY` 或 `/dbcheck YYYY-MM`"
        else:
            reply = db_duty.db_check()
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower() == '/liveslot':
        _send_duty_card("liveslot", liveslot_duty.get_three_weeks_summary(), chat_id)
        return _lark_im_done()
    elif clean_text.lower().startswith('/liveslotcheck'):
        parts = clean_text.split()
        if len(parts) > 1:
            try:
                date_str = parts[1]
                if '/' in date_str:
                    month, year = map(int, date_str.split('/'))
                elif '-' in date_str:
                    year, month = map(int, date_str.split('-'))
                else:
                    raise ValueError
                reply = liveslot_duty.liveslot_check(month=month, year=year)
            except ValueError:
                reply = "❌ 格式错误。请使用 `/liveslotcheck MM/YYYY` 或 `/liveslotcheck YYYY-MM`"
        else:
            reply = liveslot_duty.liveslot_check()
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower() == '/ote':
        _send_duty_card("ote", ote_duty.get_three_weeks_summary(), chat_id)
        return _lark_im_done()
    elif clean_text.lower().startswith('/otecheck'):
        parts = clean_text.split()
        if len(parts) > 1:
            try:
                date_str = parts[1]
                if '/' in date_str:
                    month, year = map(int, date_str.split('/'))
                elif '-' in date_str:
                    year, month = map(int, date_str.split('-'))
                else:
                    raise ValueError
                reply = ote_duty.ote_check(month=month, year=year)
            except ValueError:
                reply = "❌ 格式错误。请使用 `/otecheck MM/YYYY` 或 `/otecheck YYYY-MM`"
        else:
            reply = ote_duty.ote_check()
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower() == '/ft':
        duty_schedule = _append_department_leave_wfh_footer(
            ft.get_ft_three_days(), "/ft"
        )
        _send_duty_card("ft", duty_schedule, chat_id)
        fyi_message = """FYI
        Phan Qi Xiang - Try whatsapp first, else use phone line 
        Kevin Lim       - Call phone number , not whatapps call
        Pin Quan        - Try whatsapp first, else use phone line
        Winson Hong   - Try to spam 
        """
        send_message(chat_id, fyi_message)
        return _lark_im_done()
    elif clean_text.lower().startswith('/ftcheck'):
        parts = clean_text.split()
        if len(parts) > 1:
            try:
                date_str = parts[1]
                if '/' in date_str:
                    month, year = map(int, date_str.split('/'))
                elif '-' in date_str:
                    year, month = map(int, date_str.split('-'))
                else:
                    raise ValueError
                reply = ft.ft_check(month=month, year=year)
            except ValueError:
                reply = "❌ 格式错误。请使用 `/ftcheck MM/YYYY` 或 `/ftcheck YYYY-MM`"
        else:
            reply = ft.ft_check()
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text == '/ose':
        payload = ose_Duty.get_ose_payload_for_now(include_tag=False)
        _send_ose_payload(chat_id, payload)
        return _lark_im_done()
    elif clean_text.startswith('/osedate'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            payload = ose_Duty.get_ose_payload_for_now(include_tag=False)
            _send_ose_payload(chat_id, payload)
            return _lark_im_done()
        else:
            date_str = parts[1].strip()
            try:
                target_date = datetime.strptime(date_str, "%d/%m/%Y").date()
                payload = ose_Duty.get_ose_payload_for_date(target_date, mode="date", include_tag=False)
                _send_ose_payload(chat_id, payload)
                return _lark_im_done()
            except ValueError:
                reply = "❌ Invalid date format. Please use DD/MM/YYYY (e.g., 12/12/2026)"
    elif clean_text.lower().startswith('/dutycheckall'):
        parts = clean_text.split()
        if len(parts) > 1:
            try:
                date_str = parts[1]
                if '/' in date_str:
                    month, year = map(int, date_str.split('/'))
                elif '-' in date_str:
                    year, month = map(int, date_str.split('-'))
                else:
                    raise ValueError
                reply = get_all_duty_check(month=month, year=year)
            except ValueError:
                reply = "❌ 格式错误。请使用 `/dutycheckall MM/YYYY` 或 `/dutycheckall YYYY-MM`"
        else:
            reply = get_all_duty_check()
        send_message(chat_id, reply)
        return _lark_im_done()
    
    cmd_parts = clean_text.split()
    if not cmd_parts:
        # Whitespace-only / stripped-empty body — must still return a valid response for Lark.
        return _lark_im_done()
    cmd = cmd_parts[0].lower()
    if cmd == '/ecsre':
        game_name = cmd_parts[1] if len(cmd_parts) > 1 else None
        reply = ecsre.get_responsible_games(game_name)
        send_message(chat_id, reply)
        return _lark_im_done()
    elif cmd == '/pldtprefix':
        # Log in to Polylink UC (IPBX), read the captcha via vision LLM (retry up
        # to PLDT_PREFIX_MAX_ATTEMPTS, default 20, on `验证码不正确`), then
        # screenshot the Provider edit page.
        _pp_pid = None
        for _tok in cmd_parts[1:]:
            if _tok.isdigit():
                _pp_pid = _tok
                break

        def _run_pldtprefix_job(chat_id_pp=chat_id, provider_id_pp=_pp_pid):
            try:
                import changePrefix as _cp_mod

                send_message(
                    chat_id_pp,
                    f"🔐 Logging in to Polylink UC and opening Provider "
                    f"`id={provider_id_pp or _cp_mod._provider_id()}` … "
                    f"(reading captcha, up to {_cp_mod._max_attempts()} tries)",
                )
                res = _cp_mod.run_change_prefix(provider_id=provider_id_pp)
                img_path = res.get("result_image")
                if res.get("ok"):
                    head = (
                        f"✅ Provider `id={res.get('provider_id')}` opened "
                        f"({res.get('attempts')} attempt(s))."
                    )
                else:
                    head = (
                        f"❌ Could not open Provider `id={res.get('provider_id')}` "
                        f"after {res.get('attempts')} attempt(s).\n{res.get('message')}"
                    )
                send_message(chat_id_pp, head)
                if img_path and os.path.isfile(img_path):
                    key = upload_image_lark(img_path)
                    if key:
                        r = send_image_message(chat_id_pp, key)
                        if r.get("code") != 0:
                            send_message(chat_id_pp, f"❌ Failed to send screenshot: {r}")
                    else:
                        send_message(chat_id_pp, "❌ Failed to upload screenshot.")
            except Exception as _pp_err:
                print(f"❌ pldtprefix job: {_pp_err!r}", flush=True)
                try:
                    send_message(chat_id_pp, f"❌ /pldtprefix failed: {_pp_err}")
                except Exception:
                    pass

        threading.Thread(target=_run_pldtprefix_job, daemon=True).start()
        return _lark_im_done()
    elif cmd in ('/pldtrun', '/pldtrotate'):
        # /pldtrun — really change the PLDT prefix to the next value + announce to
        # the CS group (what `/pldtrotate apply` used to do).
        # /pldtrotate — SAFE dry-run preview only (no change, no group post).
        _pr_mode = (cmd_parts[1].lower() if len(cmd_parts) > 1 else "")
        if cmd == '/pldtrotate' and _pr_mode in ("apply", "confirm", "real", "run", "go", "yes"):
            send_message(
                chat_id,
                "ℹ️ The real rotation moved to `/pldtrun` — `/pldtrotate` is now always "
                "a dry-run preview. Send `/pldtrun` to actually change the prefix.",
            )
            return _lark_im_done()
        _pr_dry = cmd == '/pldtrotate'

        def _run_pldtrotate_job(chat_pr=chat_id, dry_pr=_pr_dry, cmd_pr=cmd):
            try:
                if dry_pr:
                    send_message(chat_pr, "🧪 Running PLDT prefix rotation (dry-run — no change, no group post)…")
                    # Dry-run summary goes to the invoker, not the CS group.
                    run_pldt_prefix_rotation(dry_run=True, notify_chat=chat_pr)
                else:
                    send_message(chat_pr, "🔄 Changing PLDT prefix and posting to the CS group…")
                    run_pldt_prefix_rotation(dry_run=False)
                    send_message(chat_pr, "✅ PLDT prefix rotation finished.")
            except Exception as _pr_err:
                print(f"❌ pldtrotate job: {_pr_err!r}", flush=True)
                try:
                    send_message(chat_pr, f"❌ {cmd_pr} failed: {_pr_err}")
                except Exception:
                    pass

        threading.Thread(target=_run_pldtrotate_job, daemon=True).start()
        return _lark_im_done()
    elif cmd == '/loginosmwatch':
        # Force a fresh Lark login QR for the OSM-Watch dashboard, posted to the
        # lab group. Used when the session expired and the auto-QR already timed out.
        try:
            import osmwatch as _ow_mod

            _ow_mod.request_login(chat_id)
            send_message(
                chat_id,
                "🔐 OSM-Watch: login requested — a fresh QR will be posted to the lab group "
                "shortly. Scan it with your Lark app to sign the bot in.",
            )
        except Exception as _ow_err:
            print(f"❌ loginosmwatch: {_ow_err!r}", flush=True)
            try:
                send_message(chat_id, f"❌ /loginosmwatch failed: {_ow_err}")
            except Exception:
                pass
        return _lark_im_done()
    elif cmd == '/osmwatch':
        # Screenshot the OSM-Watch dashboard (warm browser) and send it to this chat.
        _ow_url = None
        for _tok in cmd_parts[1:]:
            if _tok.startswith("http"):
                _ow_url = _tok
                break

        def _run_osmwatch_shot(chat_id_ow=chat_id, url_ow=_ow_url):
            try:
                import osmwatch as _ow_mod

                send_message(chat_id_ow, "📸 OSM-Watch: capturing the dashboard…")
                box = _ow_mod.capture_and_send(chat_id_ow, url=url_ow)
                err = box.get("error")
                # 'blocked' / 'not_authenticated' already notify the chat themselves;
                # a screenshot on success is sent from inside capture_and_send.
                if err and err not in ("blocked", "not_authenticated"):
                    send_message(chat_id_ow, f"❌ OSM-Watch capture failed: {err}")
            except Exception as _ow_err:
                print(f"❌ osmwatch: {_ow_err!r}", flush=True)
                try:
                    send_message(chat_id_ow, f"❌ /osmwatch failed: {_ow_err}")
                except Exception:
                    pass

        threading.Thread(target=_run_osmwatch_shot, daemon=True).start()
        return _lark_im_done()
    elif cmd == '/encoder':
        # Look up encoder/TRTC info (MAIN/POOL/CCTV IPs) for one or more machines
        # from latestencoder.json (kept fresh by the osmwatch warm browser).
        # Tokens split on space / comma / '&' — e.g. `/encoder nwr2205 & nwr2206`.
        _enc_arg = " ".join(cmd_parts[1:]).strip()

        def _run_encoder(chat_id_enc=chat_id, arg_enc=_enc_arg):
            try:
                import osmwatch as _ow_mod

                if arg_enc.lower() == "refresh":
                    send_message(chat_id_enc, "🔄 OSM-Watch: refreshing encoder data…")
                    _ow_mod.refresh_encoder(chat_id_enc)
                    return
                # Prefer an interactive emoji card; fall back to plain text when the
                # card can't render (no data / no match / usage) or the send fails.
                _enc_card = _ow_mod.build_encoder_card(arg_enc)
                if _enc_card:
                    _enc_resp = send_message(chat_id_enc, json.dumps(_enc_card), msg_type="interactive")
                    if isinstance(_enc_resp, dict) and _enc_resp.get("code") == 0:
                        return
                for _msg in _ow_mod.query_encoder(arg_enc):
                    send_message(chat_id_enc, _msg)
            except Exception as _enc_err:
                print(f"❌ encoder: {_enc_err!r}", flush=True)
                try:
                    send_message(chat_id_enc, f"❌ /encoder failed: {_enc_err}")
                except Exception:
                    pass

        threading.Thread(target=_run_encoder, daemon=True).start()
        return _lark_im_done()
    elif cmd == '/ec':
        game_name = cmd_parts[1] if len(cmd_parts) > 1 else None
        result = emergency.get_emergency_contacts_payload(game_name)
        if isinstance(result, dict) and result.get("lark_card"):
            card_json = json.dumps(result["lark_card"])
            resp = send_message(chat_id, card_json, msg_type="interactive")
            if resp.get("code") != 0:
                send_message(chat_id, result.get("text") or str(result))
        else:
            send_message(chat_id, result.get("text") if isinstance(result, dict) else str(result))
        return _lark_im_done()
    elif clean_text == '/cashout':
        reply = f'the player has been get back his credit. @On-Duty-OSM-Lavie(Podium1) kindly manual cashout the credit and reboot the machine. After that, @Xavier (CS OSM) kindly unset and test the machine thanks'
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text == '/restartA':
        reply = f'cd /home/pi/osm && ./stopallserver.sh && ./startserver.sh'
        send_message(chat_id, reply)
        return _lark_im_done()
    elif re.search(
        r"(?:^|\s)/m\s+",
        clean_text,
        re.I,
    ):
        # EVO multi-ticket batch paste — CP filter, outbound email, result card.
        cmd_m = re.search(r"/m\s+", original_text, re.IGNORECASE | re.DOTALL)
        email_text = original_text[cmd_m.end() :].strip() if cmd_m else ""
        if email_text.startswith('"') and email_text.endswith('"'):
            email_text = email_text[1:-1]
        if not maintenance.is_evo_batch_command_chat(chat_id):
            send_message(chat_id, maintenance.EVO_BATCH_WRONG_GROUP_MESSAGE)
            return _lark_im_done()
        if not email_text:
            send_message(
                chat_id,
                "请粘贴 EVO 批量维护通知（`※SD-xxxxx※` + `====` 分隔）。\n"
                "单封邮件解析请用 `/ms` 或 `/maintenance`。",
            )
            return _lark_im_done()
        if not maintenance.is_evo_sd_batch_paste(email_text):
            send_message(
                chat_id,
                "⚠️ 未识别为 EVO 批量维护格式。请粘贴含 `※SD-xxxxx※` 的多条通知，"
                "或使用 `/ms` 解析单封 Service Desk 邮件。",
            )
            return _lark_im_done()
        _process_evo_sd_batch_paste(chat_id, email_text)
        return _lark_im_done()
    elif cmd in ("/egs", "/egstest"):
        # Pasted maintenance notice → editable card → Send. `/egs` → egs.maintenance@
        # (Cc om@) + @QA/CS tag; `/egstest` → test send to junchen@ only.
        # Rebuild the body from ``original_text`` (keeps newlines, which ``clean_text``
        # collapses) and strip the leading command token + mentions.
        _egs_test = cmd == "/egstest"
        _egs_src = original_text or ""
        for _mk in mention_keys:
            _egs_src = _egs_src.replace(_mk, "")
        _egs_src = re.sub(r"@_user_\d+", "", _egs_src)
        _egs_src = re.sub(r"<[^>]+>", "", _egs_src)
        _egs_cmd = re.search(rf"(?:^|\s){re.escape(cmd)}\b\s*", _egs_src, re.IGNORECASE)
        _egs_src = _egs_src[_egs_cmd.end():] if _egs_cmd else ""
        _egs_src = _egs_src.strip()
        if _egs_src.startswith('"') and _egs_src.endswith('"'):
            _egs_src = _egs_src[1:-1].strip()
        _process_egs_paste(chat_id, _egs_src, test=_egs_test)
        return _lark_im_done()
    elif cmd in ("/egsreply", "/egsreplytest"):
        # Show the picker so the user chooses which sent email to reply to; any text pasted
        # WITH the command becomes the reply body, pre-filled into the preview after picking.
        # Rebuild from ``original_text`` (keeps newlines, which ``clean_text`` collapses) and
        # strip the leading command token + mentions. `/egsreplytest` → test address only.
        _egr_src = original_text or ""
        for _mk in mention_keys:
            _egr_src = _egr_src.replace(_mk, "")
        _egr_src = re.sub(r"@_user_\d+", "", _egr_src)
        # Strip only Lark mention markup (``<at ...>name</at>``), NOT every ``<...>`` — the
        # reply body is free prose that may legitimately contain angle brackets (e.g. an email
        # ``<admin@x.com>`` or a ``<placeholder>``), which must survive into the sent reply.
        _egr_src = re.sub(r"</?at\b[^>]*>", "", _egr_src)
        _egr_cmd = re.search(rf"(?:^|\s){re.escape(cmd)}\b\s*", _egr_src, re.IGNORECASE)
        _egr_src = _egr_src[_egr_cmd.end():] if _egr_cmd else ""
        _egr_src = _egr_src.strip()
        if _egr_src.startswith('"') and _egr_src.endswith('"'):
            _egr_src = _egr_src[1:-1].strip()
        _process_egsreply_paste(chat_id, _egr_src, test=cmd == "/egsreplytest")
        return _lark_im_done()
    elif re.search(
        r"(?:^|\s)/(maintenance|maintenanceshort|ms)\s+",
        clean_text,
        re.I,
    ):
        # Single pasted email — preview card only (no outbound SMTP).
        cmd_m = re.search(
            r"/(maintenance|maintenanceshort|ms)\s+",
            original_text,
            re.IGNORECASE | re.DOTALL,
        )
        if cmd_m:
            email_text = original_text[cmd_m.end() :].strip()
            if email_text.startswith('"') and email_text.endswith('"'):
                email_text = email_text[1:-1]
        else:
            email_text = ""
        if email_text:
            token = get_tenant_access_token()
            subj = maintenance.parse_subject_from_pasted_email(email_text) or "Maintenance (/ms)"
            resolved_subj = maintenance.resolve_maintenance_subject(subj, email_text)
            if maintenance.subject_should_ignore(resolved_subj):
                skip_reason = (
                    "Service Desk `IMPORTANT!` stream/issue alert (not scheduled maintenance)"
                    if maintenance.is_service_desk_important_alert(resolved_subj)
                    else "subject contains ignored marker (e.g. `C88live_ow.ph`)"
                )
                send_message(
                    chat_id,
                    f"⏭️ Skipped — {skip_reason}.\n\n`{resolved_subj}`",
                )
                return _lark_im_done()
            from_line = ""
            for line in email_text.splitlines()[:40]:
                if re.match(r"^From:\s*", line, re.I):
                    from_line = line
                    break
            if maintenance.from_should_ignore(from_line):
                send_message(
                    chat_id,
                    "⏭️ Skipped — email is from OM-PH / om@hotelstotsenberg.com (outbound copy).",
                )
                return _lark_im_done()
            if from_line and not maintenance.from_is_allowed_sender(from_line):
                send_message(
                    chat_id,
                    "⏭️ Skipped — sender must be "
                    "`no-reply-evolution@evolution.com` (Jira) or "
                    "`servicedesk@evolution.com` (Service Desk).",
                )
                return _lark_im_done()
            if maintenance.is_maintenance_cancelled_email(email_text):
                prior = maintenance.lookup_prior_maintenance_schedule(
                    resolved_subj, email_text
                )
                table_game = maintenance.table_display_from_prior(prior) or None
                cancel_card = maintenance.build_cancelled_maintenance_card(
                    email_subject=resolved_subj,
                    email_body=email_text,
                    table_game=table_game,
                    prior=prior,
                )
                send_message(
                    chat_id,
                    json.dumps(cancel_card, ensure_ascii=False),
                    msg_type="interactive",
                )
                return _lark_im_done()
            if maintenance.is_maintenance_uncancel_clarification_email(email_text):
                prior = maintenance.lookup_prior_maintenance_schedule(
                    resolved_subj, email_text
                )
                launched = (
                    maintenance.table_names_from_prior_entry(prior) if prior else []
                )
                hdr_title, hdr_tpl, _body_md, card_el = (
                    maintenance.build_maintenance_notice(
                        email_text,
                        email_subject=resolved_subj,
                        launched_tables=launched or None,
                        prior=prior,
                    )
                )
                card = maintenance.build_maintenance_card(
                    email_subject=resolved_subj,
                    gamelist_section="",
                    summary_section="",
                    body_elements=card_el,
                    email_body=email_text,
                    show_meta=False,
                    header_title=hdr_title or None,
                    header_template=hdr_tpl or "green",
                )
                send_message(
                    chat_id,
                    json.dumps(card, ensure_ascii=False),
                    msg_type="interactive",
                )
                return _lark_im_done()
            if maintenance.gamelist_configured() and token:
                to_cp, _launched = maintenance.gamelist_has_launched(
                    email_text, token
                )
                if not to_cp:
                    send_message(
                        chat_id,
                        "⛔ **没有 CP 上线游戏**（NOT IN CP WEBSITE）。",
                    )
                    return _lark_im_done()
            if (
                "[service desk]" not in resolved_subj.lower()
                and "tinc-" not in resolved_subj.lower()
            ):
                send_message(
                    chat_id,
                    "⚠️ 未能识别邮件主题。请在粘贴内容末尾保留一行 "
                    "``[Service Desk] … / … / (SD-xxxxx)``，或开头加 ``Subject: …``。",
                )
                return _lark_im_done()
            first_reply, hdr_title, hdr_tpl, card_body, card_el = (
                maintenance.process_maintenance_pipeline(
                    email_text,
                    token,
                    email_subject=resolved_subj,
                )
            )
            card = maintenance.build_maintenance_card(
                email_subject=resolved_subj,
                gamelist_section=(first_reply or "").strip(),
                summary_section=card_body or "",
                body_elements=card_el,
                email_body=email_text,
                show_meta=False,
                header_title=hdr_title or None,
                header_template=hdr_tpl or "orange",
            )
            send_message(chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive")
        else:
            send_message(
                chat_id,
                "Please paste email content after the command.\n"
                "Examples: `/ms …`, `/maintenance …`, `/maintenanceshort …` "
                "(EVO batch + email: `/m …`)",
            )
        return _lark_im_done()
    elif cmd == "/checkemail":
        title = " ".join(cmd_parts[1:]).strip()
        if not title:
            send_message(
                chat_id,
                "❌ Usage: `/checkemail <email title or SD-xxxxx>`\n"
                "Example:\n"
                "`/checkemail SD-7066787`\n"
                "`/checkemail [Service Desk] Equipment maintenance / 01/Jun/26 … (SD-7066787)`\n\n"
                "Finds mail in om@ IMAP (prefers **Evolution original**, not `Re:`/`Fw:`). "
                "Parsed fields only — **no email sent**.",
            )
            return _lark_im_done()
        try:
            import maintenance_mail as _maint_mail
            import maintenance as _maint_mod

            token = get_tenant_access_token()
            card = _maint_mail.check_maintenance_email_by_title(
                title, tenant_access_token=token
            )
        except Exception as ex:
            card = _maint_mod.build_checkemail_error_card(
                f"❌ `/checkemail` failed: `{ex}`",
                title="Check email — error",
            )
        send_message(
            chat_id,
            json.dumps(card, ensure_ascii=False),
            msg_type="interactive",
        )
        return _lark_im_done()
    elif cmd == '/checkerror':
        # AI review of the larkbot.service journal — ERRORS ONLY: the module
        # pre-filters the journal so the LLM reports what error + what time.
        _ce_args = " ".join(cmd_parts[1:]).strip()

        def _run_checkerror_job(chat_id_ce=chat_id, args_ce=_ce_args):
            try:
                import checkerror as _ce_mod

                _ce_mod.handle_checkerror_command(
                    args_ce, chat_id=chat_id_ce, send_message=send_message
                )
            except Exception as _ce_err:
                print(f"❌ checkerror job: {_ce_err!r}", flush=True)
                try:
                    send_message(chat_id_ce, f"❌ /checkerror failed: {_ce_err}")
                except Exception:
                    pass

        threading.Thread(target=_run_checkerror_job, daemon=True).start()
        return _lark_im_done()
    elif cmd == '/checkevo':
        # Look up one game by 游戏名称/Games Name in the EVO gamelist sheet and
        # show its row. If the bot lacks sheet permission → explicit 91403 notice.
        _ev_name = " ".join(cmd_parts[1:]).strip()
        try:
            import maintenance as _maint_ev

            _ev_result = _maint_ev.lookup_evo_gamelist_row(
                _ev_name, get_tenant_access_token()
            )
            _ev_reply = _maint_ev.build_checkevo_reply(_ev_name, _ev_result)
        except Exception as _ev_err:
            print(f"❌ /checkevo failed: {_ev_err!r}", flush=True)
            _ev_reply = f"❌ /checkevo failed: {_ev_err}"
        send_message(chat_id, _ev_reply)
        return _lark_im_done()
    elif clean_text.lower().startswith('/stresstest'):
        # Explicit stress-test announcement paste: the AI reads the machine list + the
        # set-maintenance time and schedules a one-time reminder 10 min before it.
        _stress_body = re.sub(
            r'(?is)^\s*/stresstest\b[ \t]*', '', clean_text_multiline or clean_text, count=1
        ).strip()
        try:
            _stress_reply = maintenancemachineagent.handle_stresstest_command(
                _stress_body,
                chat_id=chat_id,
                send_message=send_message,
                get_token_func=get_tenant_access_token,
                scheduler=scheduler,
                target_user_id=TARGET_USER_OPEN_ID,
                schedule_chat_id=REMINDER_TARGET_CHAT_ID,
            )
        except Exception as _stress_err:
            _stress_reply = f"❌ /stresstest failed: {_stress_err}"
        if _stress_reply:
            send_message(chat_id, _stress_reply)
        return _lark_im_done()
    elif maintenancemachineagent.is_maintenance_schedule_message(original_text, mention_keys):
        # Scheduled stress-test announcement (has an action + a future date/time + machine list /
        # "ALL <ENV> MACHINES <Venue>"). Schedule a one-time reminder 10 min before the action.
        if chat_type == "group" and not bot_mentioned:
            print("⏭️ maintenance schedule ignored (bot not @mentioned in group)", flush=True)
            return _lark_im_done()
        try:
            handled_maint, maint_reply = maintenancemachineagent.handle_maintenance_schedule_message(
                original_text,
                mention_keys,
                chat_id=chat_id,
                send_message=send_message,
                get_token_func=get_tenant_access_token,
                scheduler=scheduler,
                target_user_id=TARGET_USER_OPEN_ID,
                schedule_chat_id=REMINDER_TARGET_CHAT_ID,
            )
        except Exception as _maint_err:
            handled_maint, maint_reply = True, f"❌ Maintenance schedule failed: {_maint_err}"
        if handled_maint:
            if maint_reply:
                send_message(chat_id, maint_reply)
            return _lark_im_done()
    elif maintenancemachineagent.is_machine_status_check_message(original_text, mention_keys):
        # Read-only machine status from webmachine_data.json (same targeting as set/unset maintenance).
        if chat_type == "group" and not bot_mentioned:
            print("⏭️ status check ignored (bot not @mentioned in group)", flush=True)
            return _lark_im_done()
        try:
            handled_st, st_reply = maintenancemachineagent.handle_machine_status_check_message(
                original_text,
                mention_keys,
                chat_id=chat_id,
                send_message=send_message,
            )
        except Exception as _st_err:
            handled_st, st_reply = True, f"❌ Machine status check failed: {_st_err}"
        if handled_st:
            if st_reply:
                send_message(chat_id, st_reply)
            return _lark_im_done()
    elif maintenancemachineagent.is_direct_set_unset_message(original_text, mention_keys):
        # Short ``set/unset NWR2008`` — execute immediately; LLM asks only when ambiguous/not found.
        if chat_type == "group" and not bot_mentioned:
            print("⏭️ direct set/unset ignored (bot not @mentioned in group)", flush=True)
            return _lark_im_done()
        if data.get("header", {}).get("event_type") == "im.message.receive_v1":
            msg_obj = (data.get("event") or {}).get("message") or {}
            thread_root = _prod_batch_thread_root_from_incoming_message(
                msg_obj, message_id=message_id
            )
        else:
            thread_root = (message_id or "").strip() or None
        if thread_root:
            _set_prod_batch_thread_root(chat_id, thread_root)
        pb_send = make_prod_batch_thread_send(chat_id, thread_root=thread_root)
        try:
            handled_direct, direct_reply = maintenancemachineagent.handle_direct_set_unset_message(
                original_text,
                mention_keys,
                chat_id=chat_id,
                send_message=pb_send,
                thread_root_message_id=thread_root,
            )
        except Exception as _direct_err:
            handled_direct, direct_reply = True, f"❌ Direct set/unset failed: {_direct_err}"
        if handled_direct:
            if direct_reply:
                pb_send(chat_id, direct_reply)
            return _lark_im_done()
    elif maintenancemachineagent.is_short_set_unset_only_message(original_text, mention_keys):
        if chat_type == "group" and not bot_mentioned:
            print("⏭️ set/unset shorthand ignored (bot not @mentioned in group)", flush=True)
            return _lark_im_done()
        if data.get("header", {}).get("event_type") == "im.message.receive_v1":
            msg_obj = (data.get("event") or {}).get("message") or {}
            thread_root = _prod_batch_thread_root_from_incoming_message(
                msg_obj, message_id=message_id
            )
        else:
            thread_root = (message_id or "").strip() or None
        pb_send = make_prod_batch_thread_send(chat_id, thread_root=thread_root)
        pb_send(
            chat_id,
            maintenancemachineagent.short_set_unset_usage_text(original_text, mention_keys),
        )
        return _lark_im_done()
    elif maintenancemachineagent.is_maintenance_now_message(original_text, mention_keys):
        # Immediate (no time) set/unset maintenance/test — either "ALL <ENV> MACHINES <Venue>"
        # (expanded from webmachine_data.json, PROD only) or an explicit machine list where the env
        # is inferred from the names (e.g. "unset maintenance TBP8609" — no site word needed).
        # Then run the existing prod-batch confirm/execute flow.
        if chat_type == "group" and not bot_mentioned:
            print("⏭️ maintenance group command ignored (bot not @mentioned in group)", flush=True)
            return _lark_im_done()
        if data.get("header", {}).get("event_type") == "im.message.receive_v1":
            msg_obj = (data.get("event") or {}).get("message") or {}
            thread_root = _prod_batch_thread_root_from_incoming_message(
                msg_obj, message_id=message_id
            )
        else:
            thread_root = (message_id or "").strip() or None
        if thread_root:
            _set_prod_batch_thread_root(chat_id, thread_root)
        pb_send = make_prod_batch_thread_send(chat_id, thread_root=thread_root)
        try:
            handled_now, now_reply = maintenancemachineagent.handle_maintenance_now_message(
                original_text,
                mention_keys,
                chat_id=chat_id,
                send_message=pb_send,
                thread_root_message_id=thread_root,
            )
        except Exception as _maint_now_err:
            handled_now, now_reply = True, f"❌ Maintenance group command failed: {_maint_now_err}"
        if handled_now:
            if now_reply:
                pb_send(chat_id, now_reply)
            return _lark_im_done()
    elif smmachine.is_prod_batch_sm_command(original_text, mention_keys):
        if chat_type == "group" and not bot_mentioned:
            print("⏭️ /sm command ignored (bot not @mentioned in group)", flush=True)
            return _lark_im_done()
        if data.get("header", {}).get("event_type") == "im.message.receive_v1":
            msg_obj = (data.get("event") or {}).get("message") or {}
            thread_root = _prod_batch_thread_root_from_incoming_message(
                msg_obj, message_id=message_id
            )
        else:
            thread_root = (message_id or "").strip() or None
        if thread_root:
            _set_prod_batch_thread_root(chat_id, thread_root)
        pb_send = make_prod_batch_thread_send(chat_id, thread_root=thread_root)
        handled_sm, sm_reply = smmachine.handle_prod_batch_sm_command(
            chat_id=chat_id,
            send_message=pb_send,
            thread_root_message_id=thread_root,
        )
        if handled_sm:
            if sm_reply:
                pb_send(chat_id, sm_reply)
            return _lark_im_done()
    elif smmachine.is_prod_batch_bot_message(original_text, mention_keys) or smmachine.is_prod_batch_bot_message(clean_text, []):
        # ``clean_text`` may be the AI-mapped form (e.g. "i want nwr set maintenance …"
        # → "/nwrsetmaintenance …"). Use whichever text actually parses, so natural
        # language reaches the real prod-batch pipeline instead of falling to /s or /nwr.
        if smmachine.is_prod_batch_bot_message(original_text, mention_keys):
            _pb_text, _pb_mentions = original_text, mention_keys
        else:
            _pb_text, _pb_mentions = clean_text, []
        if chat_type == "group" and not bot_mentioned:
            print("⏭️ prod-batch command ignored (bot not @mentioned in group)", flush=True)
            return _lark_im_done()
        if data.get("header", {}).get("event_type") == "im.message.receive_v1":
            msg_obj = (data.get("event") or {}).get("message") or {}
            thread_root = _prod_batch_thread_root_from_incoming_message(
                msg_obj, message_id=message_id
            )
        else:
            thread_root = (message_id or "").strip() or None
        if thread_root:
            _set_prod_batch_thread_root(chat_id, thread_root)
        pb_send = make_prod_batch_thread_send(chat_id, thread_root=thread_root)
        handled_pb, pb_reply = smmachine.handle_prod_batch_bot_command(
            _pb_text,
            _pb_mentions,
            chat_id=chat_id,
            send_message=pb_send,
            thread_root_message_id=thread_root,
        )
        if handled_pb:
            if pb_reply:
                pb_send(chat_id, pb_reply)
            return _lark_im_done()
    elif clean_text.lower().startswith("/list"):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ " + list_range.USAGE_EXAMPLES
        else:
            reply = list_range.format_list_range(parts[1])
        send_message(chat_id, reply)
        return _lark_im_done()
    elif re.match(r"^/(?:findmachine|fm)\b", clean_text, re.I) or re.match(
        r"(?i)^find\s*machines?\s*$", clean_text
    ):
        # Interactive card: environment + game type + online/offline → machine names.
        try:
            import findmachine as _findmachine

            card_fm = _findmachine.build_findmachine_form_card()
            resp_fm = send_message(chat_id, json.dumps(card_fm, ensure_ascii=False), msg_type="interactive")
            if isinstance(resp_fm, dict) and resp_fm.get("code") not in (0, None):
                send_message(chat_id, f"❌ Find-machine card rejected: {resp_fm}")
        except Exception as e:
            send_message(chat_id, f"❌ findmachine card failed: {e}")
        return _lark_im_done()
    elif clean_text.lower().startswith('/nch'):
        # Accept both "/nch 1900" and "/nch1900" (no space); multiple ids ok.
        query = _machine_query_after_prefix(clean_text, '/nch')
        if not query:
            send_message(chat_id, "❌ Usage: `/nch <asset_id(s)>`\nExamples: `/nch 1900`, `/nch1900`, `/nch nch2839 nch2378`, `/nch nch2839,nch2378`")
        else:
            _send_machine_lookup_card(chat_id, nch.get_nch_info(query), title="NCH machine")
        return _lark_im_done()
    elif clean_text.lower().startswith('/nwr'):
        query = _machine_query_after_prefix(clean_text, '/nwr')
        if not query:
            send_message(chat_id, "❌ Usage: `/nwr <nwr_number(s)>`\nExamples: `/nwr 2005`, `/nwr2005`, `/nwr 2005,2006`, `/nwr nwr2005 nwr2006`")
        else:
            _send_machine_lookup_card(chat_id, nwr.get_nwr_info(query), title="NWR machine")
        return _lark_im_done()
    elif clean_text.lower().startswith('/wf'):
        query = _machine_query_after_prefix(clean_text, '/wf')
        if not query:
            send_message(chat_id, "❌ Usage: `/wf <asset_id(s)>`\nExamples: `/wf 8092`, `/wf8092`, `/wf 8092,8093`, `/wf win8092 win8093`")
        else:
            _send_machine_lookup_card(chat_id, winford.get_winford_info(query), title="Winford asset")
        return _lark_im_done()
    elif clean_text.lower().startswith('/tbr'):
        query = _machine_query_after_prefix(clean_text, '/tbr')
        if not query:
            send_message(chat_id, "❌ Usage: `/tbr <machine_id(s)>`\nExamples: `/tbr 2099`, `/tbr2099`, `/tbr tbr2099 tbr2100`, `/tbr 2099,2100`")
        else:
            _send_machine_lookup_card(chat_id, tbr.get_tbr_info(query), title="TBR machine")
        return _lark_im_done()
    elif clean_text.lower().startswith('/tbp'):
        query = _machine_query_after_prefix(clean_text, '/tbp')
        if not query:
            send_message(chat_id, "❌ Usage: `/tbp <machine_id(s)>`\nExamples: `/tbp 1234`, `/tbp1234`, `/tbp tbp1234 tbp5678`, `/tbp 1234,5678`")
        else:
            _send_machine_lookup_card(chat_id, tbp.get_tbp_info(query), title="TBP machine")
        return _lark_im_done()
    elif clean_text.lower().startswith('/cp') and not clean_text.lower().startswith('/cpms'):
        query = _machine_query_after_prefix(clean_text, '/cp')
        if not query:
            send_message(chat_id, "❌ Usage: `/cp <asset_number(s)>`\nExamples: `/cp 1234`, `/cp1234`, `/cp cp2839 cp2378`, `/cp cp2839,cp2378`")
        else:
            _send_machine_lookup_card(chat_id, cp.get_cp_info(query), title="CP asset")
        return _lark_im_done()
    elif clean_text.lower().startswith('/dhs'):
        query = _machine_query_after_prefix(clean_text, '/dhs')
        if not query:
            send_message(chat_id, "❌ Usage: `/dhs <asset_id(s)>`\nExamples: `/dhs 1234`, `/dhs1234`, `/dhs dhs1234 dhs5678`, `/dhs 1234,5678`")
        else:
            _send_machine_lookup_card(chat_id, dhs.get_dhs_info(query), title="DHS asset")
        return _lark_im_done()
    elif clean_text.lower().startswith('/mdr'):
        query = _machine_query_after_prefix(clean_text, '/mdr')
        if not query:
            send_message(chat_id, "❌ Usage: `/mdr <asset_id(s)>`\nExamples: `/mdr 1234`, `/mdr1234`, `/mdr mdr1234 mdr5678`, `/mdr 1234,5678`")
        else:
            _send_machine_lookup_card(chat_id, mdr.get_mdr_info(query), title="MDR asset")
        return _lark_im_done()
    elif clean_text.lower().startswith('/secret1'):
        if not _secret_command_allowed(sender_id):
            send_message(chat_id, "❌ You are not allowed to use this command.")
            return _lark_im_done()
        reply = _handle_secret_open_id_lookup(original_text, mentions)
        send_message(chat_id, reply)
        return _lark_im_done()
    elif (
        _secret_command_allowed(sender_id)
        and re.search(r'open\s*_?\s*id', clean_text, re.I)
        and not clean_text.lower().startswith('/secret')
    ):
        tagged = _extract_tagged_users_from_message(original_text, mentions)
        if tagged:
            send_message(chat_id, _format_open_id_lookup_reply(tagged))
            return _lark_im_done()
    elif clean_text.lower() in ("/deploy", "/gitpullrestart"):
        if not _secret_command_allowed(sender_id):
            send_message(chat_id, "❌ You are not allowed to use this command.")
            return _lark_im_done()
        _handle_git_pull_restart_deploy(chat_id)
        return _lark_im_done()
    elif clean_text.lower() in ("/warmstatus", "/jenkinswarmstatus"):
        _handle_jenkins_warm_status(chat_id)
        return _lark_im_done()
    elif (
        (bot_mentioned or chat_type == "p2p")
        and _secret_command_allowed(sender_id)
        and looks_like_git_pull_restart(clean_text)
    ):
        _handle_git_pull_restart_deploy(chat_id)
        return _lark_im_done()
    elif re.match(r"^/al(?:\s+\d{1,2}/\d{1,2})?\s*$", clean_text.lower()):
            # /al or /al DD/MM: run Amount Loss checklog flow in background, return interactive card + TSV.
            parts = clean_text.split()
            date_param = parts[1].strip() if len(parts) > 1 else None
            send_message(chat_id, "⏳ Checking Amount Loss (CHECKLOG), please wait...")
            start_lark_background_thread(run_amountloss_check, chat_id, date_param)
            return _lark_im_done()
    elif re.match(r"^/cctv\b", clean_text, re.I):
        m_cv = re.match(r"^/cctv\s+(\S+)", clean_text.strip(), re.I)
        if not m_cv:
            send_message(
                chat_id,
                "❌ Usage: `/cctv <machine>` — EGM **CCTV** only (no credit check).\n"
                "Example: `@Duty Bot /cctv OSMCP181` · `/cctv Dragons-0181`",
            )
            return _lark_im_done()
        start_lark_background_thread(run_cctv_screenshot_job, chat_id, m_cv.group(1))
        return _lark_im_done()
    elif clean_text.lower().startswith("/npthirdhttp"):
        parts = clean_text.split()
        start_lark_background_thread(run_np_third_http_job, chat_id, parts[1:])
        return _lark_im_done()
    elif re.match(r"^/checkcreditdate\s*$", clean_text, re.I):
        # Bare `/checkcreditdate` — interactive card (machine + player + date). With a machine token, use the branch below.
        try:
            import checkcredit

            card_cp = checkcredit.build_checkcredit_player_form_card()
            send_message(chat_id, json.dumps(card_cp), msg_type="interactive")
        except Exception as e:
            send_message(chat_id, f"❌ checkcredit date card failed: {e}")
        return _lark_im_done()
    elif re.search(r"/checkmachinelog\b", clean_text, re.I):
        m_cml = re.search(
            r"/checkmachinelog\b\s+(\S+)(?:\s+(\d{4}-\d{2}-\d{2}))?",
            clean_text,
            re.I,
        )
        if not m_cml:
            send_message(
                chat_id,
                "❌ Usage:\n"
                "• `/checkmachinelog <machine> [YYYY-MM-DD]` — last player, transfer-out credit, error ±10 lines (or success log)\n"
                "• Natural language: `check machine log DHS3077` · `check machine error DHS3077`\n"
                "Examples: `@Duty Bot /checkmachinelog DHS3077` · `check machine log DHS3077 2026-06-26`",
            )
            return _lark_im_done()
        machine_q = m_cml.group(1).strip()
        date_arg = (m_cml.group(2) or "").strip() or datetime.now().strftime("%Y-%m-%d")
        try:
            datetime.strptime(date_arg, "%Y-%m-%d")
        except ValueError:
            send_message(chat_id, "❌ Date must be `YYYY-MM-DD`.")
            return _lark_im_done()
        try:
            import checkcredit

            use_oss = checkcredit.checkcredit_use_oss_source()
        except Exception:
            use_oss = True
        thread_root = _checkcredit_begin_thread(chat_id, message_id)
        wait_msg = (
            "⏳ Running checkmachinelog via OSS HTTP, please wait..."
            if use_oss
            else "⏳ Running checkmachinelog (LogNavigator), please wait..."
        )
        _checkcredit_send(chat_id, wait_msg, thread_root=thread_root)
        start_lark_background_thread(
            run_check_machine_log_job,
            chat_id,
            machine_q,
            date_arg,
            thread_root,
        )
        return _lark_im_done()
    elif re.search(r"/stuckcredit\b", clean_text, re.I):
        m_sc = re.search(
            r"/stuckcredit\b\s+(\S+)(?:\s+(\d{4}-\d{2}-\d{2}))?",
            clean_text,
            re.I,
        )
        if not m_sc:
            send_message(
                chat_id,
                "❌ Usage:\n"
                "• `/stuckcredit <machine> [YYYY-MM-DD]` — stuck credit: log + Third Http transfer-out check\n"
                "• Natural language: `NWR2938 stuck credit` · `stuck credit DHS3077`\n"
                "Examples: `@Duty Bot /stuckcredit NWR2938` · `NWR2938 stuck credit 2026-06-26`",
            )
            return _lark_im_done()
        machine_q = m_sc.group(1).strip()
        date_arg = (m_sc.group(2) or "").strip() or datetime.now().strftime("%Y-%m-%d")
        try:
            datetime.strptime(date_arg, "%Y-%m-%d")
        except ValueError:
            send_message(chat_id, "❌ Date must be `YYYY-MM-DD`.")
            return _lark_im_done()
        try:
            import checkcredit

            use_oss = checkcredit.checkcredit_use_oss_source()
        except Exception:
            use_oss = True
        thread_root = _checkcredit_begin_thread(chat_id, message_id)
        wait_msg = (
            "⏳ Stuck credit — reading machine log via OSS HTTP, then Third Http…"
            if use_oss
            else "⏳ Stuck credit — reading machine log (LogNavigator), then Third Http…"
        )
        _checkcredit_send(chat_id, wait_msg, thread_root=thread_root)
        start_lark_background_thread(
            run_check_machine_log_job,
            chat_id,
            machine_q,
            date_arg,
            thread_root,
            stuck_credit=True,
        )
        return _lark_im_done()
    elif re.search(r"/(?:checkcreditdate|checkcredit|machineerror)\b", clean_text, re.I):
        # Longer token first in alternation so `/checkcreditdate` is not parsed as `/checkcredit` + `date`.
        # Optional date defaults to **today** (server local) when omitted — e.g. `@Duty Bot /checkcredit 1171`.
        m_cc = re.search(
            r"/(checkcreditdate|checkcredit|machineerror)\b\s+(\S+)(?:\s+(\d{4}-\d{2}-\d{2}))?",
            clean_text,
            re.I,
        )
        if not m_cc:
            send_message(
                chat_id,
                "❌ Usage:\n"
                "• `/checkcreditdate` — **interactive card**: machine + player + date → Third Http Detail\n"
                "• `/checkcredit <machine>` — **today** (same as `--date` omitted in CLI)\n"
                "• `/checkcreditdate <machine> [YYYY-MM-DD]` — optional date; omit for today\n"
                "• `/machineerror <machine> [YYYY-MM-DD]` — latest two players with error only\n"
                "Examples: `@Duty Bot /checkcredit 1171` · `/checkcreditdate 2074 2026-04-27`\n"
                "(same as `python3 checkcredit.py --finderror … --date YYYY-MM-DD`)",
            )
            return _lark_im_done()
        cmd_cc = (m_cc.group(1) or "").strip().lower()
        machine_q = m_cc.group(2).strip()
        date_arg = (m_cc.group(3) or "").strip()
        if not date_arg:
            date_arg = datetime.now().strftime("%Y-%m-%d")
        try:
            datetime.strptime(date_arg, "%Y-%m-%d")
        except ValueError:
            send_message(
                chat_id,
                "❌ Date must be `YYYY-MM-DD` (e.g. `2026-04-27`).",
            )
            return _lark_im_done()
        import checkcredit

        use_oss_wait = checkcredit.checkcredit_use_oss_source()
        thread_root = _checkcredit_begin_thread(chat_id, message_id)
        wait_msg = (
            "⏳ Running machineerror via OSS HTTP, please wait..."
            if cmd_cc == "machineerror" and use_oss_wait
            else "⏳ Running machineerror, browser may take a while — please wait..."
            if cmd_cc == "machineerror"
            else "⏳ Running checkcredit via OSS HTTP , please wait..."
            if use_oss_wait
            else "⏳ Running LogNavigator checkcredit, browser may take a while — please wait..."
        )
        _checkcredit_send(chat_id, wait_msg, thread_root=thread_root)
        start_lark_background_thread(
            run_checkcredit_finderror,
            chat_id,
            machine_q,
            date_arg,
            "error_only" if cmd_cc == "machineerror" else "default",
            None,
            thread_root,
        )
        return _lark_im_done()
    elif clean_text.lower().startswith("/smsfail"):
        send_message(chat_id, "⏳ Running SMS gateway OTP log check, please wait...")
        start_lark_background_thread(run_smsfail_check, chat_id)
        return _lark_im_done()
    elif clean_text.lower().startswith("/smscheckplayer"):
        parts = clean_text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            send_message(
                chat_id,
                "❌ Usage: `/smscheckplayer <player_id(s)>` — today 00:00—now, up to 3 newest logs per player; e.g. `/smscheckplayer 127317237` or `/smscheckplayer 7052472, 1069954565` (commas / spaces / newlines OK)",
            )
            return _lark_im_done()
        payload = parts[1].strip()
        send_message(
            chat_id,
            "⏳ Running SMS OTP log check for player(s) (today 00:00—now, up to 3 newest rows each), please wait...",
        )
        start_lark_background_thread(run_smscheckplayer_check, chat_id, payload)
        return _lark_im_done()
    elif clean_text.lower().startswith('/pid'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/pid <provider_id>`\nExamples: `/pid 30`, `/pid 30 31 32`"
        else:
            query = parts[1]
            reply = providerid.get_provider_info(query)
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower() == '/secret2':
        if not _secret_command_allowed(sender_id):
            send_message(chat_id, "❌ You are not allowed to use this command.")
            return _lark_im_done()
        reply = f"当前群组的 ID 是：{chat_id}"
        result = send_message(chat_id, reply)
        message_id = result.get('data', {}).get('message_id')
        if message_id:
            run_date = datetime.now() + timedelta(seconds=8)
            scheduler.add_job(func=recall_message, trigger='date', run_date=run_date, args=[message_id])
        return _lark_im_done()
    elif clean_text.lower() in ['/memorytest']:
        number = game.start_game(sender_id)
        send_result = send_message(chat_id, f"🧠 **Memory Game**\nRemember this number: **{number}**\nYou have 5 seconds to type it back.")
        message_id = send_result.get("data", {}).get("message_id")
        if message_id:
            run_date = datetime.now() + timedelta(seconds=2)
            job = scheduler.add_job(func=recall_message, trigger='date', run_date=run_date, args=[message_id])
            game.set_game_job(sender_id, job.id)
        return _lark_im_done()
    elif (
        bot_mentioned or chat_type == "p2p"
    ) and reminder.looks_like_timer_request(clean_text):
        reply = reminder.schedule_natural_timer(
            chat_id=chat_id,
            user_id=sender_id,
            text=clean_text,
            scheduler=scheduler,
            send_func=send_message,
        )
        if reply:
            send_message(chat_id, reply)
            return _lark_im_done()
    elif clean_text.lower().startswith('/reminder'):
        parts = clean_text.split()
        if len(parts) < 3:
            reply = "❌ Usage: `/reminder [at] <time|duration> <message>`\nExamples:\n  `/reminder 1h30m Team meeting`\n  `/reminder 8:39PM Lunch`\n  `/reminder at 2039 Break`\nNatural language: `@Duty Bot add timer 5mins` or `add timer 1h30m lunch`"
        else:
            time_or_duration = None
            msg_start_idx = None
            for i in range(1, len(parts)):
                token = parts[i]
                if token.lower() == 'at':
                    continue
                if (':' in token or token.lower().endswith(('am', 'pm')) or
                    (token.isdigit() and len(token) == 4)):
                    time_or_duration = token
                    msg_start_idx = i + 1
                    break
            if time_or_duration is None:
                if len(parts) > 1:
                    time_or_duration = parts[1]
                    msg_start_idx = 2
                else:
                    reply = "❌ Missing time/duration and message."
            if time_or_duration and msg_start_idx and msg_start_idx < len(parts):
                message = ' '.join(parts[msg_start_idx:])
                if (':' in time_or_duration or
                    time_or_duration.lower().endswith(('am', 'pm')) or
                    (time_or_duration.isdigit() and len(time_or_duration) == 4)):
                    result = reminder.schedule_reminder_absolute(
                        chat_id=chat_id,
                        user_id=sender_id,
                        time_str=time_or_duration,
                        message=message,
                        scheduler=scheduler,
                        send_func=send_message
                    )
                else:
                    result = reminder.schedule_reminder(
                        chat_id=chat_id,
                        user_id=sender_id,
                        duration_str=time_or_duration,
                        message=message,
                        scheduler=scheduler,
                        send_func=send_message
                    )
                reply = result
            else:
                reply = "❌ Invalid format. Please specify a time/duration and a message."
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower().startswith('/addreminder'):
        parts = clean_text.split(maxsplit=4)
        if len(parts) < 5:
            reminder.send_add_reminder_form_card(
                send_func=send_message,
                chat_id=chat_id,
            )
            return _lark_im_done()
        start_raw = parts[1].strip()
        end_raw = parts[2].strip()
        time_raw = parts[3].strip()
        reason = parts[4].strip()
        result = reminder.add_sheet_reminder(
            start_raw=start_raw,
            end_raw=end_raw,
            time_raw=time_raw,
            reason=reason,
            get_token_func=get_tenant_access_token,
            scheduler=scheduler,
            send_func=send_message,
            chat_id=chat_id,
            target_user_id=TARGET_USER_OPEN_ID,
            schedule_chat_id=REMINDER_TARGET_CHAT_ID,
        )
        if (result or "").strip():
            send_message(chat_id, result)
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/deletereminder'):
        parts = clean_text.split()
        ids = [p.strip() for p in parts[1:] if p.strip()]
        if not ids:
            try:
                reminder.send_sheet_reminder_list_card(
                    send_func=send_message,
                    chat_id=chat_id,
                    get_token_func=get_tenant_access_token,
                )
            except Exception as e:
                send_message(chat_id, f"❌ Failed to load reminder list: {e}")
            return jsonify({"success": True})
        result = reminder.delete_sheet_reminders(
            ids=ids,
            get_token_func=get_tenant_access_token,
            scheduler=scheduler,
            send_func=send_message,
            chat_id=chat_id,
            target_user_id=TARGET_USER_OPEN_ID,
            schedule_chat_id=REMINDER_TARGET_CHAT_ID,
        )
        send_message(chat_id, result)
        return jsonify({"success": True})
    elif clean_text.lower() == '/restart':
        send_message(chat_id, "🔄 Restarting bot...")
        write_restart_pending(chat_id)
        scheduler.shutdown(wait=False)
        def delayed_exit():
            time.sleep(1)
            os._exit(0)
        threading.Thread(target=delayed_exit).start()
        return _lark_im_done()
    elif clean_text.lower() in ("/restartservices", "/restservices"):
        _handle_restart_services(chat_id)
        return _lark_im_done()

    # 如果前面没有任何命令匹配，并且 reply 为空，则忽略
    if reply:
        cmd_token = (clean_text.split()[0] if clean_text else "").lower()
        if cmd_token in _DUTY_LEAVE_WFH_FOOTER_COMMANDS:
            reply = _append_department_leave_wfh_footer(reply, cmd_token)
        send_message(chat_id, reply)
        print(f"✅ Replied to chat {chat_id}: {reply}")
    else:
        print(f"⚠️ No command matched and no reply generated for chat {chat_id}")
        # Use the mention-stripped text so chitchat's anchored ^…$ rules match
        # (original_text still has "@_user_1 …" which broke greeting matching).
        _chat_source = (
            clean_text_multiline or clean_text or _full_body or original_text or ""
        ).strip()
        # Is this a casual/chat message (router said chat, or it looks like small talk)?
        _is_chatty = False
        try:
            if _router_decision is not None and getattr(_router_decision, "is_chat", False):
                _is_chatty = True
            elif _router_decision is not None and getattr(_router_decision, "reason", "") == "missing_credit":
                _is_chatty = False
            else:
                import chitchat as _chitchat_probe

                _is_chatty = _chitchat_probe.looks_like_chitchat(_chat_source)
        except Exception:
            pass
        if _looks_like_jenkins_nl_update(_full_body):
            _jenkins_nl_reply = None
            try:
                import chatagent as _notice_ca

                _jenkins_nl_reply = _notice_ca.llm_notice_reply(
                    "The user pasted a Jenkins update request, but the update flow did NOT "
                    "start — nothing was filled or built. The most common cause: a previous "
                    "update in this chat is still waiting on its Confirm/Cancel card. What "
                    "the user should do: reply `cancel` first, then resend the same full "
                    "request (environment + Branch/Version/Services); or start it explicitly "
                    "with `/jenkinsupdate` plus the same block. The bot only fills the "
                    "Jenkins form and shows a screenshot with Confirm/Cancel buttons — it "
                    "never builds without the user's click.",
                    user_text=_chat_source,
                    must_contain=("cancel",),
                )
            except Exception as _notice_err:
                print(f"[jenkins-fallback] notice LLM skipped: {_notice_err!r}", flush=True)
            send_message(
                chat_id,
                _jenkins_nl_reply
                or (
                    "⚠️ Jenkins **update** was not started from that message.\n"
                    "A previous update may still be waiting on **Confirm / Cancel** — say "
                    "**cancel**, then send the full block again, or use "
                    "`/jenkinsupdate <environment>` + Branch/Version/Services.\n"
                    "The bot fills the form + screenshot — you tap **Confirm** or **Cancel** "
                    "(no auto-build)."
                ),
            )
            print(f"💬 Jenkins NL fallback hint for chat {chat_id}", flush=True)
        elif (mc := _parse_missing_credit_alert(_full_body)) and bot_mentioned:
            send_message(
                chat_id,
                "Use the **checkcredit** card above, or `@Duty Bot /checkcreditdate <machine>` "
                f"with player `{mc.get('account', '?')}` and date `{mc.get('date_iso', '?')}`.",
            )
        elif _chat_source.lstrip().startswith("/") and bot_mentioned:
            # Unknown/typo'd slash command → "did you mean" buttons instead of silence.
            _slash_sug_sent = False
            try:
                import commandsuggest as _cmdsuggest_slash

                _slash_sug = _cmdsuggest_slash.suggest_for_slash_typo(_chat_source)
                if _slash_sug:
                    _slash_sug_sent = _cmdsuggest_slash.send_suggestion_card(
                        chat_id,
                        _chat_source,
                        _slash_sug,
                        send_message=send_message,
                        sender_id=sender_id,
                    )
                    if _slash_sug_sent:
                        print(
                            f"🧭 Slash-typo suggestions ({len(_slash_sug)}) for chat {chat_id}",
                            flush=True,
                        )
            except Exception as _slash_sug_err:
                print(f"⚠️ Slash suggest skipped: {_slash_sug_err!r}", flush=True)
            if not _slash_sug_sent:
                _slash_usage = None
                try:
                    import commandsuggest as _cmdsuggest_usage

                    _slash_usage = _cmdsuggest_usage.usage_hint(_chat_source)
                except Exception:
                    _slash_usage = None
                send_message(
                    chat_id,
                    _slash_usage
                    or (
                        "❓ Unknown command "
                        f"`{_chat_source.split()[0][:40]}` — say `/help` for the full list."
                    ),
                )
        elif (
            _chat_source
            and not _chat_source.lstrip().startswith("/")
            and (
                bot_mentioned
                or (
                    _router_decision is not None
                    and getattr(_router_decision, "reason", "")
                    in ("math", "math_followup")
                )
            )
        ):
            # Command-first: before the chat LLM, offer "did you mean …" command
            # buttons when the text scores against the command registry. Strong
            # matches (score ≥ 2.0, e.g. a machine token) even override a router
            # "chat" verdict — the tiny prod LLM misroutes work requests to chat.
            if bot_mentioned:
                try:
                    import commandsuggest as _cmdsuggest_nl

                    _nl_sug = _cmdsuggest_nl.suggest_commands(_chat_source)
                    _nl_top = float(_nl_sug[0]["score"]) if _nl_sug else 0.0
                    if (_nl_top >= 2.0 or (not _is_chatty and _nl_top >= 1.0)) and (
                        _cmdsuggest_nl.send_suggestion_card(
                            chat_id,
                            _chat_source,
                            _nl_sug,
                            send_message=send_message,
                            sender_id=sender_id,
                        )
                    ):
                        print(
                            f"🧭 Command suggestions ({len(_nl_sug)}, top={_nl_top:.2f}) "
                            f"for chat {chat_id}",
                            flush=True,
                        )
                        return _lark_im_done()
                except Exception as _nl_sug_err:
                    print(f"⚠️ Command suggest skipped: {_nl_sug_err!r}", flush=True)
            chat_reply = None
            try:
                import chitchat as _chitchat

                chat_reply = _chitchat.try_reply(_chat_source)
            except Exception as _chat_err:
                print(f"⚠️ Chitchat skipped: {_chat_err!r}", flush=True)
            if not chat_reply:
                try:
                    import chatagent as _chatagent

                    chat_reply = _chatagent.reply_if_enabled(
                        _chat_source, session_key=_chat_memory_key
                    )
                    if not chat_reply:
                        print(
                            f"[chat] chatagent returned no reply for {_chat_source!r} "
                            f"(enabled={_chatagent.is_enabled()}, backend={_chatagent.backend_mode()}, "
                            f"llm={_chatagent.llm_available()})",
                            flush=True,
                        )
                except Exception as _chatagent_err:
                    print(f"⚠️ Chat agent skipped: {_chatagent_err!r}", flush=True)
            if chat_reply:
                try:
                    import chatagent as _chatagent_sanitize

                    chat_reply = _chatagent_sanitize.sanitize_outbound_chat_reply(chat_reply)
                except Exception:
                    pass
            if chat_reply:
                send_message(chat_id, chat_reply)
                print(f"💬 Chat reply to chat {chat_id}", flush=True)
            elif _is_chatty or (
                _router_decision is not None
                and getattr(_router_decision, "reason", "") in ("math", "math_followup")
            ):
                _math_fb = None
                try:
                    import chatagent as _math_agent_fb

                    _math_fb = _math_agent_fb.resolve_math_from_context(
                        _chat_source, session_key=_chat_memory_key
                    )
                    if _math_fb:
                        _math_fb = (
                            _math_agent_fb.sanitize_outbound_chat_reply(_math_fb)
                            or _math_fb
                        )
                except Exception:
                    pass
                if _math_fb:
                    send_message(chat_id, _math_fb)
                    print(f"💬 Math fallback reply to chat {chat_id}", flush=True)
                else:
                    send_message(
                        chat_id,
                        "Hi! 👋 I'm Duty Bot. I'm here for duty/leave/machine stuff — "
                        "ask me e.g. “who is on fpms duty” or say `/help`. 😊",
                    )
                    print(f"💬 Friendly chat fallback to chat {chat_id}", flush=True)
            else:
                try:
                    import commandagent as _commandagent

                    if _commandagent.is_enabled():
                        send_message(
                            chat_id,
                            "🤖 I got your message but command agent could not run or map it to a command.\n"
                            "Try `@Duty Bot /fpms` for now.\n"
                            "Admin: check `journalctl -u larkbot -n 50 | grep commandagent` for load errors.",
                        )
                    else:
                        send_message(
                            chat_id,
                            "ℹ️ Natural language needs `BOT_USE_AI=1` in `.env` + trained model.\n"
                            "For now use slash commands, e.g. `@Duty Bot /fpms`.",
                        )
                except ModuleNotFoundError as _nl_mod_err:
                    send_message(
                        chat_id,
                        "🤖 AI deps missing on this server (`"
                        + str(_nl_mod_err)
                        + "`).\n"
                        "Admin: `python -m pip install -r requirements-ai.txt` then restart larkbot.\n"
                        "For now: `@Duty Bot /fpms`",
                    )
                except Exception as _nl_hint_err:
                    print(f"⚠️ NL hint failed: {_nl_hint_err!r}", flush=True)

    return _lark_im_done()

def _handle_reply_update_email_internal(payload: dict) -> tuple[bool, str, int]:
    """
    Shared handler for ``POST /internal/reply-update-email`` (jenkinsbot → duty bot).
    Returns ``(ok, message, http_status)``.
    """
    chat_id = (payload.get("chat_id") or DUTY_CHAT_ID or "").strip()
    email_title = (payload.get("email_title") or "").strip()
    environment = (payload.get("environment") or "").strip()
    when = (payload.get("when") or "").strip()
    if not chat_id:
        return False, "missing chat_id", 400
    if not email_title or not environment or not when:
        return False, "missing email_title, environment, or when", 400
    ju = _get_jenkinsupdate()
    if ju is None:
        return False, "jenkinsupdate module unavailable", 503
    try:
        import updatemore as um
    except Exception as ex:
        return False, f"updatemore import failed: {ex}", 503
    um.process_reply_update_email(
        chat_id,
        email_title,
        environment,
        when,
        send_message,
        sessions=ju._fpms_lark_sessions,
        sessions_lock=ju._fpms_lark_sessions_lock,
        session_key_fn=ju._fpms_lark_session_key,
        dispatch_update_body=lambda cid, sk, body, snd, **kw: ju._dispatch_lark_update_command_body(
            cid, sk, body, snd, **kw
        ),
    )
    return True, "processed", 200


def _run_reply_update_email_background(payload: dict) -> None:
    """Run IMAP reply off the HTTP thread so jenkinsbot does not hit its POST timeout."""
    try:
        ok, msg, code = _handle_reply_update_email_internal(payload)
        if not ok:
            chat_id = (payload.get("chat_id") or DUTY_CHAT_ID or "").strip()
            if chat_id:
                send_message(
                    chat_id,
                    f"❌ Jenkins email callback failed ({code}): {msg}",
                )
    except Exception as ex:
        chat_id = (payload.get("chat_id") or DUTY_CHAT_ID or "").strip()
        print(f"❌ reply-update-email background error: {ex}")
        if chat_id:
            send_message(chat_id, f"❌ Jenkins email callback error: {ex}")


@app.route("/internal/reply-update-email", methods=["POST"])
def internal_reply_update_email():
    """
    jenkinsbot calls this when Lark bot→bot @mention does not reach duty bot.
    Optional header ``X-Duty-Internal-Token`` must match ``DUTY_INTERNAL_TOKEN``.
    """
    token_need = (os.getenv("DUTY_INTERNAL_TOKEN") or "").strip()
    if token_need:
        got = (
            (request.headers.get("X-Duty-Internal-Token") or "").strip()
            or (request.headers.get("Authorization") or "").replace("Bearer", "").strip()
        )
        if got != token_need:
            return jsonify({"ok": False, "error": "unauthorized"}), 403
    payload = request.get_json(silent=True) or {}
    chat_id = (payload.get("chat_id") or DUTY_CHAT_ID or "").strip()
    email_title = (payload.get("email_title") or "").strip()
    environment = (payload.get("environment") or "").strip()
    when = (payload.get("when") or "").strip()
    if not chat_id:
        return jsonify({"ok": False, "error": "missing chat_id"}), 400
    if not email_title or not environment or not when:
        return jsonify({"ok": False, "error": "missing email_title, environment, or when"}), 400
    ju = _get_jenkinsupdate()
    if ju is None:
        return jsonify({"ok": False, "error": "jenkinsupdate module unavailable"}), 503
    try:
        import updatemore  # noqa: F401
    except Exception as ex:
        return jsonify({"ok": False, "error": f"updatemore import failed: {ex}"}), 503
    threading.Thread(
        target=_run_reply_update_email_background,
        args=(payload,),
        daemon=True,
    ).start()
    return jsonify({"ok": True, "message": "accepted", "accepted": True}), 202


def _handle_updatemore_jenkins_callback_internal(payload: dict) -> tuple[bool, str, int]:
    """Shared handler for ``POST /internal/updatemore-jenkins-callback``."""
    chat_id = (payload.get("chat_id") or DUTY_CHAT_ID or "").strip()
    command = (payload.get("command") or "").strip()
    if not chat_id:
        return False, "missing chat_id", 400
    if not command:
        return False, "missing command", 400
    ju = _get_jenkinsupdate()
    if ju is None:
        return False, "jenkinsupdate module unavailable", 503
    try:
        import updatemore as um
    except Exception as ex:
        return False, f"updatemore import failed: {ex}", 503
    if not (
        um.is_failed_stop_message(command) or um.is_success_proceed_message(command)
    ):
        return False, "command must be /FailedStop or /SuccessProceedNext", 400
    um.process_updatemore_jenkins_command(
        chat_id,
        command,
        send_message,
        sessions=ju._fpms_lark_sessions,
        sessions_lock=ju._fpms_lark_sessions_lock,
        session_key_fn=ju._fpms_lark_session_key,
        dispatch_update_body=lambda cid, sk, body, snd, **kw: ju._dispatch_lark_update_command_body(
            cid, sk, body, snd, **kw
        ),
    )
    return True, "processed", 200


def _run_updatemore_jenkins_callback_background(payload: dict) -> None:
    try:
        ok, msg, code = _handle_updatemore_jenkins_callback_internal(payload)
        if not ok:
            chat_id = (payload.get("chat_id") or DUTY_CHAT_ID or "").strip()
            if chat_id:
                send_message(
                    chat_id,
                    f"❌ Jenkins updatemore callback failed ({code}): {msg}",
                )
    except Exception as ex:
        chat_id = (payload.get("chat_id") or DUTY_CHAT_ID or "").strip()
        print(f"❌ updatemore-jenkins-callback background error: {ex}")
        if chat_id:
            send_message(chat_id, f"❌ Jenkins updatemore callback error: {ex}")


@app.route("/internal/updatemore-jenkins-callback", methods=["POST"])
def internal_updatemore_jenkins_callback():
    """
    jenkinsbot calls this when Lark bot→bot delivery for ``/FailedStop`` or
    ``/SuccessProceedNext`` is unreliable (same pattern as ``/internal/reply-update-email``).
    """
    token_need = (os.getenv("DUTY_INTERNAL_TOKEN") or "").strip()
    if token_need:
        got = (
            (request.headers.get("X-Duty-Internal-Token") or "").strip()
            or (request.headers.get("Authorization") or "").replace("Bearer", "").strip()
        )
        if got != token_need:
            return jsonify({"ok": False, "error": "unauthorized"}), 403
    payload = request.get_json(silent=True) or {}
    chat_id = (payload.get("chat_id") or DUTY_CHAT_ID or "").strip()
    command = (payload.get("command") or "").strip()
    if not chat_id:
        return jsonify({"ok": False, "error": "missing chat_id"}), 400
    if not command:
        return jsonify({"ok": False, "error": "missing command"}), 400
    ju = _get_jenkinsupdate()
    if ju is None:
        return jsonify({"ok": False, "error": "jenkinsupdate module unavailable"}), 503
    try:
        import updatemore  # noqa: F401
    except Exception as ex:
        return jsonify({"ok": False, "error": f"updatemore import failed: {ex}"}), 503
    threading.Thread(
        target=_run_updatemore_jenkins_callback_background,
        args=(payload,),
        daemon=True,
    ).start()
    return jsonify({"ok": True, "message": "accepted", "accepted": True}), 202


def _register_lark_webhook_duplicate_paths():
    """
    If the developer console still points at a legacy path (e.g. old 「消息卡片请求网址」),
    set ``LARK_WEBHOOK_EXTRA_PATHS=/callback,/open/event`` — comma-separated POST paths on this app.
    Each path will invoke the same handler as ``POST /webhook/event``.
    """
    extra = (os.getenv("LARK_WEBHOOK_EXTRA_PATHS") or "").strip()
    if not extra:
        return
    for i, raw in enumerate(extra.split(",")):
        path = raw.strip()
        if not path or path.rstrip("/") == "/webhook/event":
            continue
        app.add_url_rule(path, "lark_webhook_extra_%d" % i, lark_webhook, methods=["POST", "GET", "OPTIONS"])
        print("[lark] Extra webhook POST route registered: %s" % path, flush=True)


_register_lark_webhook_duplicate_paths()


def _try_mount_webapp_blueprint() -> None:
    # Default: mount dashboard on this Flask app. Opt-out with WEBMACHINE_MOUNT_IN_MAIN=0|false|no|off
    # (avoids 404 on /wm/ when operators forget to set the env on the server).
    _v = (os.environ.get("WEBMACHINE_MOUNT_IN_MAIN") or "").strip().lower()
    if _v in ("0", "false", "no", "off"):
        return
    try:
        import webapp as _wm
    except Exception as e:
        print("[webapp] optional mount skipped (import failed): %r" % (e,), flush=True)
        return
    prefix = (os.environ.get("WEBMACHINE_URL_PREFIX") or "/wm").strip()
    if prefix and not prefix.startswith("/"):
        prefix = "/" + prefix
    try:
        _wm.register_webapp(app, url_prefix=prefix, mounted_in_main=True)
        _wm.start_background_scrape_loop()
        print(
            "[webapp] dashboard registered at prefix %r (live scrape on by default; WEBMACHINE_SCRAPE=0 to disable)"
            % prefix,
            flush=True,
        )
        threading.Thread(
            target=ose_leave_wfh_calendar_sync,
            name="hrms-bitable-startup-sync",
            daemon=True,
        ).start()
        print("[webapp] HRMS→Bitable startup sync queued (main.py scheduler handles ongoing sync)", flush=True)
    except Exception as e:
        print("[webapp] optional mount failed: %r" % (e,), flush=True)


_try_mount_webapp_blueprint()

if not scheduler.running:
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))
    threading.Thread(target=poll_offset_approver_notifications_from_bitable, daemon=True).start()
# Load daily reminder jobs from Lark Sheet at startup.
try:
    _cnt, _total = reminder.sync_sheet_daily_reminders(
        scheduler=scheduler,
        send_func=send_message,
        get_token_func=get_tenant_access_token,
        chat_id=REMINDER_TARGET_CHAT_ID,
        target_user_id=TARGET_USER_OPEN_ID,
    )
    print(f"✅ Reminder sheet sync loaded: {_cnt}/{_total} job(s)")
except Exception as _e:
    print(f"⚠️ Reminder sheet sync failed on startup: {_e!r}")
send_restart_ready()

try:
    import maintenance_mail as _maint_mail

    _maint_mail.start_maintenance_mail_watcher(
        send_message_func=send_message,
        get_token_func=get_tenant_access_token,
    )
except Exception as _mail_e:
    print(f"⚠️ Maintenance mail watcher failed to start: {_mail_e!r}", flush=True)

try:
    import maintenance_mail as _allemail_mod

    _allemail_mod.start_allemail_cache_scanner()
except Exception as _allemail_e:
    print(f"⚠️ allemail.json cache scanner failed to start: {_allemail_e!r}", flush=True)

def _lark_event_mode() -> str:
    """``http`` (default) = public Request URL only; ``websocket`` = persistent connection + local Flask."""
    return (os.getenv("LARK_EVENT_MODE") or "http").strip().lower()


def _lark_ws_uses_persistent_connection() -> bool:
    return _lark_event_mode() in ("websocket", "ws", "longconn", "persistent", "long_connection")


def _lark_ws_ensure_inbound_message_id(payload: dict) -> dict:
    if not isinstance(payload, dict):
        return payload
    ev = payload.get("event")
    if not isinstance(ev, dict):
        return payload
    msg = ev.get("message")
    if not isinstance(msg, dict):
        return payload
    if (msg.get("message_id") or "").strip():
        return payload
    for alt in (
        ev.get("message_id"),
        (ev.get("message") or {}).get("message_id") if isinstance(ev.get("message"), dict) else None,
    ):
        mid = str(alt or "").strip()
        if mid:
            msg["message_id"] = mid
            break
    return payload


def _lark_ws_ensure_card_webhook_payload(payload: dict) -> dict:
    out = dict(payload)
    out.setdefault("schema", "2.0")
    hdr = dict(out.get("header") or {})
    hdr.setdefault("event_type", "card.action.trigger")
    hdr.setdefault("event_id", hdr.get("event_id") or str(uuid.uuid4()))
    if VERIFICATION_TOKEN and not str(hdr.get("token") or "").strip():
        hdr["token"] = VERIFICATION_TOKEN
    out["header"] = hdr
    ev = out.get("event")
    if isinstance(ev, dict):
        ctx = ev.get("context") if isinstance(ev.get("context"), dict) else {}
        if not ev.get("open_chat_id") and ctx.get("open_chat_id"):
            ev["open_chat_id"] = str(ctx["open_chat_id"]).strip()
        if not ev.get("chat_id") and ctx.get("chat_id"):
            ev["chat_id"] = str(ctx["chat_id"]).strip()
        out["event"] = ev
    return out


def _lark_ws_to_webhook_payload(data) -> dict:
    import lark_oapi as lark

    raw = json.loads(lark.JSON.marshal(data))
    if isinstance(raw, dict) and "header" in raw and "event" in raw:
        payload = dict(raw)
        hdr = dict(payload.get("header") or {})
        payload["header"] = hdr
    else:
        inner = raw.get("event", raw) if isinstance(raw, dict) else raw
        payload = {
            "schema": "2.0",
            "header": {
                "event_id": str(uuid.uuid4()),
                "event_type": "im.message.receive_v1",
                "create_time": str(int(time.time() * 1000)),
            },
            "event": inner,
        }
    if VERIFICATION_TOKEN:
        hdr = payload.setdefault("header", {})
        if not str(hdr.get("token") or "").strip():
            hdr["token"] = VERIFICATION_TOKEN
    payload = _lark_ws_ensure_inbound_message_id(payload)
    mid = (
        ((payload.get("event") or {}).get("message") or {}).get("message_id")
        if isinstance(payload.get("event"), dict)
        else None
    )
    if not str(mid or "").strip():
        print("[lark-ws] warning: payload missing event.message.message_id", flush=True)
    return payload


def _lark_ws_dispatch_payload(payload: dict) -> tuple[int, dict]:
    """In-process POST to ``lark_webhook`` (same handlers as HTTPS Request URL mode)."""
    with app.test_client() as client:
        rv = client.post("/webhook/event", json=payload)
    body: dict = {}
    if rv.data:
        try:
            parsed = json.loads(rv.get_data(as_text=True))
            if isinstance(parsed, dict):
                body = parsed
        except (ValueError, TypeError):
            body = {}
    return int(rv.status_code), body


def _lark_ws_to_menu_webhook_payload(data) -> dict:
    import lark_oapi as lark

    raw = json.loads(lark.JSON.marshal(data))
    if isinstance(raw, dict) and "header" in raw and "event" in raw:
        payload = dict(raw)
    else:
        inner = raw.get("event", raw) if isinstance(raw, dict) else raw
        payload = {
            "schema": "2.0",
            "header": {
                "event_id": str(uuid.uuid4()),
                "event_type": "application.bot.menu_v6",
                "create_time": str(int(time.time() * 1000)),
            },
            "event": inner,
        }
    if VERIFICATION_TOKEN:
        hdr = payload.setdefault("header", {})
        if not str(hdr.get("token") or "").strip():
            hdr["token"] = VERIFICATION_TOKEN
    hdr = payload.setdefault("header", {})
    hdr.setdefault("event_type", "application.bot.menu_v6")
    return payload


def _lark_ws_on_bot_menu(data) -> None:
    try:
        payload = _lark_ws_to_menu_webhook_payload(data)
        status, _ = _lark_ws_dispatch_payload(payload)
        print(
            f"[lark-ws] application.bot.menu_v6 dispatched status={status}",
            flush=True,
        )
    except Exception as exc:
        print(f"[lark-ws] bot menu dispatch failed: {exc!r}", flush=True)


def _lark_ws_on_message(data) -> None:
    try:
        payload = _lark_ws_to_webhook_payload(data)
        status, _ = _lark_ws_dispatch_payload(payload)
        print(f"[lark-ws] im.message.receive_v1 dispatched status={status}", flush=True)
    except Exception as exc:
        print(f"[lark-ws] im.message dispatch failed: {exc!r}", flush=True)


def _lark_ws_on_card_action(data):
    from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTriggerResponse

    import lark_oapi as lark

    try:
        payload = _lark_ws_ensure_card_webhook_payload(json.loads(lark.JSON.marshal(data)))
        status, body = _lark_ws_dispatch_payload(payload)
        print(
            f"[lark-ws] card.action.trigger dispatched status={status} resp_keys={list(body.keys())!r}",
            flush=True,
        )
        if status == 200 and isinstance(body, dict):
            return P2CardActionTriggerResponse(body)
        if status == 403:
            print(
                "[lark-ws] card callback 403 — check VERIFICATION_TOKEN matches developer console",
                flush=True,
            )
    except Exception as exc:
        print(f"[lark-ws] card callback failed: {exc!r}", flush=True)
    return P2CardActionTriggerResponse({})


def _lark_ws_handler_dispatch(handler, payload: bytes) -> Any:
    """
    Dispatch a WebSocket frame through ``EventDispatcherHandler``.

    Older ``lark-oapi`` on some servers only expose ``do_without_validation`` (no leading underscore).
    """
    for name in ("_do_without_validation", "do_without_validation"):
        fn = getattr(handler, name, None)
        if callable(fn):
            return fn(payload)
    return _lark_ws_handler_dispatch_manual(handler, payload)


def _lark_ws_handler_dispatch_manual(handler, payload: bytes) -> Any:
    """Last resort when installed lark-oapi predates ``do_without_validation``."""
    from lark_oapi.core.const import UTF_8
    from lark_oapi.core.json import JSON
    from lark_oapi.core.utils import Strings
    from lark_oapi.event.context import EventContext
    from lark_oapi.core.exception import EventException

    pl = payload.decode(UTF_8)
    context = JSON.unmarshal(pl, EventContext)
    if Strings.is_not_empty(context.schema):
        context.schema = "p2"
        context.type = context.header.event_type
    elif Strings.is_not_empty(context.uuid):
        context.schema = "p1"
        context.type = context.event.get("type")

    event_key = f"{context.schema}.{context.type}"
    cb_map = getattr(handler, "_callback_processor_map", None) or {}
    if event_key in cb_map:
        processor = cb_map.get(event_key)
        if processor is None:
            raise EventException(f"callback processor not found, type: {context.type}")
        data = JSON.unmarshal(pl, processor.type())
        return processor.do(data)

    proc_map = getattr(handler, "_processorMap", None) or {}
    processor = proc_map.get(event_key)
    if processor is None:
        raise EventException(f"processor not found, type: {context.type}")
    data = JSON.unmarshal(pl, processor.type())
    processor.do(data)
    return None


# The Lark app is subscribed (in the developer console) to event types the bot
# never registered a processor for — vc.meeting.*, task.task.*,
# meeting_room.* — so the WS SDK pushes them, ``_do_without_validation`` raises
# ``EventException("processor not found, type: …")``, and the patched handler
# below used to log every one as ``handle message failed`` (tens of thousands a
# day; see checkerror.py's noise filter). These are NOT failures: an event with
# no handler is simply ignored. We ACK 200 and stay silent, logging each
# distinct unhandled event type ONCE for visibility. The permanent fix is to
# unsubscribe those events in the console; this keeps the journal clean meanwhile.
_lark_ws_unhandled_seen: set[str] = set()
_lark_ws_unhandled_lock = threading.Lock()
_LARK_WS_UNHANDLED_TYPE_RE = re.compile(r"type:\s*(\S+)")


def _lark_ws_unhandled_event_type(exc: Exception) -> Optional[str]:
    """If ``exc`` is the SDK's missing-processor error, return the event type.

    Matches both ``processor not found`` and ``callback processor not found``.
    Any other exception (a real handler crash) returns ``None`` so it still logs.
    """
    msg = str(exc or "")
    if "processor not found" not in msg:
        return None
    m = _LARK_WS_UNHANDLED_TYPE_RE.search(msg)
    return m.group(1) if m else "<unknown>"


def _lark_ws_note_unhandled_event(event_type: str) -> None:
    """Log a given unhandled event type at most once."""
    with _lark_ws_unhandled_lock:
        if event_type in _lark_ws_unhandled_seen:
            return
        _lark_ws_unhandled_seen.add(event_type)
    print(
        f"[lark-ws] ignoring unsubscribed event type {event_type!r} "
        "(no processor registered → ACK 200, silenced). "
        "Unsubscribe it in the Lark developer console to stop delivery.",
        flush=True,
    )


def _lark_ws_apply_card_frame_patch() -> None:
    """lark-oapi ws client drops MessageType.CARD without ACK → Lark shows code: undefined."""
    try:
        from lark_oapi.core.const import UTF_8
        from lark_oapi.core.json import JSON
        from lark_oapi.ws.client import Client, _get_by_key
        from lark_oapi.ws.const import (
            HEADER_BIZ_RT,
            HEADER_MESSAGE_ID,
            HEADER_SEQ,
            HEADER_SUM,
            HEADER_TRACE_ID,
            HEADER_TYPE,
        )
        from lark_oapi.ws.enum import MessageType
        from lark_oapi.ws.model import Response
    except ImportError:
        print("[lark-ws] pip install lark-oapi for persistent connection mode", flush=True)
        raise

    if getattr(Client, "_osedutybot_card_patch", False):
        return

    async def _handle_data_frame_patched(self, frame):
        hs = frame.headers
        msg_id = _get_by_key(hs, HEADER_MESSAGE_ID)
        trace_id = _get_by_key(hs, HEADER_TRACE_ID)
        sum_ = _get_by_key(hs, HEADER_SUM)
        seq = _get_by_key(hs, HEADER_SEQ)
        type_ = _get_by_key(hs, HEADER_TYPE)

        pl = frame.payload
        if int(sum_) > 1:
            pl = self._combine(msg_id, int(sum_), int(seq), pl)
            if pl is None:
                return

        message_type = MessageType(type_)
        resp = Response(code=http.HTTPStatus.OK)
        try:
            start = int(round(time.time() * 1000))
            if message_type in (MessageType.EVENT, MessageType.CARD):
                result = _lark_ws_handler_dispatch(self._event_handler, pl)
            else:
                return
            end = int(round(time.time() * 1000))
            header = hs.add()
            header.key = HEADER_BIZ_RT
            header.value = str(end - start)
            if result is not None:
                resp.data = base64.b64encode(JSON.marshal(result).encode(UTF_8))
        except Exception as e:
            _unhandled_type = _lark_ws_unhandled_event_type(e)
            if _unhandled_type is not None:
                # Subscribed-but-unhandled event (VC/task/meeting-room/…): not a
                # failure. Keep the OK response created above, log the type once.
                _lark_ws_note_unhandled_event(_unhandled_type)
            else:
                from lark_oapi.core.log import logger

                logger.error(
                    self._fmt_log(
                        "handle message failed, message_type: {}, message_id: {}, trace_id: {}, err: {}",
                        message_type.value,
                        msg_id,
                        trace_id,
                        e,
                    )
                )
                resp = Response(code=http.HTTPStatus.INTERNAL_SERVER_ERROR)

        frame.payload = JSON.marshal(resp).encode(UTF_8)
        await self._write_message(frame.SerializeToString())

    Client._handle_data_frame = _handle_data_frame_patched
    Client._osedutybot_card_patch = True
    print("[lark-ws] patched lark-oapi ws Client for CARD callbacks", flush=True)


def _run_lark_ws_forever() -> None:
    """Block on Lark persistent connection (im.message + card.action.trigger)."""
    import lark_oapi as lark

    if not (APP_ID and APP_SECRET):
        raise RuntimeError("Set APP_ID and APP_SECRET in .env for LARK_EVENT_MODE=websocket")

    _lark_ws_apply_card_frame_patch()
    builder = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(_lark_ws_on_message)
        .register_p2_card_action_trigger(_lark_ws_on_card_action)
    )
    if hasattr(builder, "register_p2_application_bot_menu_v6"):
        builder = builder.register_p2_application_bot_menu_v6(_lark_ws_on_bot_menu)
    else:
        print(
            "[lark-ws] lark-oapi has no register_p2_application_bot_menu_v6 — "
            "bot menu needs HTTP webhook or upgrade lark-oapi",
            flush=True,
        )
    handler = builder.build()
    _probe = getattr(handler, "_do_without_validation", None) or getattr(handler, "do_without_validation", None)
    print(
        "[lark-ws] EventDispatcherHandler dispatch="
        + (getattr(_probe, "__name__", "manual_fallback") if callable(_probe) else "manual_fallback"),
        flush=True,
    )
    domain_name = (os.getenv("LARK_DOMAIN") or "lark").strip().lower()
    domain = lark.FEISHU_DOMAIN if domain_name == "feishu" else lark.LARK_DOMAIN
    cli = lark.ws.Client(
        str(APP_ID).strip(),
        str(APP_SECRET).strip(),
        event_handler=handler,
        log_level=lark.LogLevel.INFO,
        domain=domain,
    )
    print(
        "[lark-ws] Persistent connection active (im.message + card.action.trigger + bot menu). "
        "Developer console: Subscription mode → Receive callbacks through persistent connection.",
        flush=True,
    )
    cli.start()


def _run_main_entry() -> int:
    """
    Same startup guard style as legacy ``run_larkbot.py``:
    - force cwd to project root
    - ensure root is on sys.path
    - print full traceback to stderr on any startup/runtime crash

    ``LARK_EVENT_MODE=websocket`` — Flask in background + Lark persistent connection (main thread).
    Default ``http`` — Flask only (public HTTPS Request URL forwards to /webhook/event).
    """
    import traceback

    root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(root)
    if root not in sys.path:
        sys.path.insert(0, root)

    try:
        port_str = os.getenv("PORT") or os.getenv("LARKBOT_PORT") or "5000"
        port = int(port_str)
        try:
            import commandagent as _boot_commandagent

            _boot_commandagent.startup_status()
        except Exception as _boot_commandagent_err:
            print(f"[commandagent] startup check skipped: {_boot_commandagent_err!r}", flush=True)
        try:
            import chatagent as _boot_chatagent

            _boot_chatagent.startup_status()
        except Exception as _boot_chatagent_err:
            print(f"[chatagent] startup check skipped: {_boot_chatagent_err!r}", flush=True)
        try:
            import chathandleagent as _boot_router

            _boot_router.startup_status()
        except Exception as _boot_router_err:
            print(f"[chathandleagent] startup check skipped: {_boot_router_err!r}", flush=True)
        try:
            import commandsuggest as _boot_cmdsuggest

            _boot_cmdsuggest.startup_status()
        except Exception as _boot_cmdsuggest_err:
            print(f"[commandsuggest] startup check skipped: {_boot_cmdsuggest_err!r}", flush=True)
        try:
            import aithinking as _boot_aithinking

            _boot_aithinking.startup_status()
        except Exception as _boot_aithinking_err:
            print(f"[aithinking] startup check skipped: {_boot_aithinking_err!r}", flush=True)
        try:
            import codeassist as _boot_codeassist

            _boot_codeassist.startup_status()
        except Exception as _boot_code_err:
            print(f"[codeassist] startup check skipped: {_boot_code_err!r}", flush=True)
        try:
            import dutyai as _boot_dutyai

            _boot_dutyai.startup_status()
        except Exception as _boot_dutyai_err:
            print(f"[dutyai] startup check skipped: {_boot_dutyai_err!r}", flush=True)
        try:
            import offsetai as _boot_offsetai

            _boot_offsetai.startup_status()
        except Exception as _boot_offsetai_err:
            print(f"[offsetai] startup check skipped: {_boot_offsetai_err!r}", flush=True)
        try:
            import jenkinsupdate as _boot_ju

            _boot_ju.prewarm_all_jenkins_browsers_on_startup()
        except Exception as _boot_ju_err:
            print(f"[warm] startup pre-warm skipped: {_boot_ju_err!r}", flush=True)
        try:
            import prod_machine_batch as _boot_pmb

            _boot_pmb.prewarm_prod_env_pool_on_startup()
        except Exception as _boot_pmb_err:
            print(f"[prod-warm] startup pre-warm skipped: {_boot_pmb_err!r}", flush=True)
        try:
            import smmachine as _boot_wm

            _boot_wm.prewarm_webmachine_scrape_pool_on_startup()
        except Exception as _boot_wm_err:
            print(f"[wm-warm] startup pre-warm skipped: {_boot_wm_err!r}", flush=True)
        try:
            import checkcredit as _boot_cc

            _boot_cc._ensure_writable_temp_dir()
        except Exception as _boot_tmp_err:
            print(f"[checkcredit] temp dir init failed: {_boot_tmp_err!r}", flush=True)
        try:
            from third_http_warm_pool import prewarm_third_http_pool_on_startup

            prewarm_third_http_pool_on_startup()
        except Exception as _boot_th_err:
            print(f"[third-http-warm] startup pre-warm skipped: {_boot_th_err!r}", flush=True)
        try:
            import osmwatch as _boot_ow

            _boot_ow.prewarm_osmwatch_on_startup()
        except Exception as _boot_ow_err:
            print(f"[osmwatch-warm] startup pre-warm skipped: {_boot_ow_err!r}", flush=True)
        if _lark_ws_uses_persistent_connection():
            def _flask_bg() -> None:
                app.run(host="127.0.0.1", port=port, debug=False, threaded=True, use_reloader=False)

            threading.Thread(target=_flask_bg, daemon=True, name="larkbot-flask").start()
            print(
                "[lark] LARK_EVENT_MODE=websocket — Flask on http://127.0.0.1:%d (diag); "
                "events via persistent connection." % port,
                flush=True,
            )
            time.sleep(1.0)
            _run_lark_ws_forever()
            return 0

        print(
            "[lark] Listening http://0.0.0.0:%d (threaded=True). "
            "Feishu Request URL must be HTTPS and reachable from the internet; reverse-proxy to this port."
            % port,
            flush=True,
        )
        app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
        return 0
    except OSError as e:
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        print(
            "Flask bind failed (port %s in use or permission?): %s"
            % (os.getenv("PORT") or os.getenv("LARKBOT_PORT") or "5000", e),
            file=sys.stderr,
            flush=True,
        )
        return 1
    except BaseException:
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        return 1


if __name__ == "__main__":
    raise SystemExit(_run_main_entry())