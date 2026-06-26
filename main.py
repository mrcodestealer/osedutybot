import contextvars
import json
import re
import sys
import threading
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
load_dotenv(os.path.join(_CHBOX_DIR, ".env"))

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
import dhs
import mdr
import smmachine
import maintenancemachineagent

import p0
import p1
import maintenance
import emergency
import ecsre

import otpp1
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
LABORATORY_GROUP = os.getenv("LABORATORY_GROUP")
OSE_BOT_GROUP = os.getenv("OSE_BOT_GROUP")
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
    lines.append(cpms_duty.get_cpms_three_days())
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
    if c == "/fpms":
        return fpms_duty.get_fpms_today_duty()
    if c == "/pms":
        return pms_duty.dutyNextDay()
    if c == "/bi":
        return bi_duty.get_bi_today_duty()
    if c == "/fe":
        return fe_duty.get_fe_next_three_duty()
    if c == "/cpms":
        return cpms_duty.format_output(cpms_duty.get_cpms_three_days())
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


def run_check_machine_log_job(chat_id: str, machine_query: str, date_str: str) -> None:
    """OSS/LogNavigator logic log → last player, transfer-out credit, error or success context."""
    try:
        import checkmachinelog
    except ImportError as e:
        send_message(chat_id, f"❌ Cannot load checkmachinelog module: {e}")
        return
    try:
        td = datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
        out = checkmachinelog.run_check_machine_log(
            str(machine_query).strip(),
            target_date=td,
        )
        text = (out.get("text") or "").strip()
        if text:
            send_message(chat_id, text)
        else:
            send_message(chat_id, "✅ checkmachinelog finished (no text output).")
    except Exception as e:
        send_message(chat_id, f"❌ checkmachinelog failed: {e}")
        print(f"[checkmachinelog] error: {e!r}")


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


def _np_run_screenshot_worker(
    chat_id: str,
    uid: str,
    date_iso: str,
    time_short: str,
    *,
    machine_substr: Optional[str] = None,
    expected_credit: Optional[float] = None,
    machine_display: Optional[str] = None,
) -> None:
    """NP / WF / DHS / NCH / CP / OSM / MDR / TBP Log Third Http → `recharge` Detail screenshot. Always **headless** on server."""
    try:
        import checkcredit

        screenshot_np_recharge_detail = checkcredit.screenshot_np_recharge_detail
    except ImportError as e:
        _checkcredit_send(chat_id, f"❌ Cannot load checkcredit module: {e}")
        return
    except AttributeError:
        _checkcredit_send(
            chat_id,
            "❌ checkcredit.screenshot_np_recharge_detail missing — deploy the latest `checkcredit.py`.",
        )
        return
    backend_tag = getattr(checkcredit, "_np_log_backend_tag", lambda _: "NP")(
        (machine_display or "").strip() or None
    )
    _checkcredit_send(
        chat_id,
        f"⏳ {backend_tag} backend (Playwright): login → Log Third Http Req → recharge → Detail screenshot…",
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
        )
        key = upload_image_lark(path)
        if not key:
            _checkcredit_send(chat_id, "❌ Failed to upload screenshot to Lark.")
            return
        r = _checkcredit_send_image(chat_id, key)
        if r.get("code") != 0:
            _checkcredit_send(chat_id, f"❌ Failed to send image: {r}")
    except Exception as e:
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
        _checkcredit_send(chat_id, f"❌ {backend_tag} third-http screenshot failed: {e}{tip}")
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

# ================= P0 交互确认相关 =================
pending_p0_confirmation = {}  # key: sender_id -> {"timestamp": datetime, "original_text": str}
_pending_p0_lock = threading.Lock()
P0_CONFIRMATION_TIMEOUT = 60  # 秒

def handle_p0_confirmation(chat_id, sender_id, clean_text, original_text, send_func):
    now = datetime.now()

    # 情况1：用户在 OSE_BOT_GROUP 回复确认
    if chat_id == OSE_BOT_GROUP:
        with _pending_p0_lock:
            entry = pending_p0_confirmation.get(sender_id)
        if entry is not None:
            if (now - entry["timestamp"]).total_seconds() > P0_CONFIRMATION_TIMEOUT:
                with _pending_p0_lock:
                    pending_p0_confirmation.pop(sender_id, None)
                return False, None

            reply_lower = clean_text.strip().lower()
            if reply_lower in ('yes', 'y'):
                with _pending_p0_lock:
                    pending_p0_confirmation.pop(sender_id, None)
                alert_msg = p0.format_p0_alert(chat_id, sender_id, entry["original_text"])
                send_func(OSE_BOT_GROUP, alert_msg)
                return True, None
            if reply_lower in ('no', 'n'):
                with _pending_p0_lock:
                    pending_p0_confirmation.pop(sender_id, None)
                return True, "👌 Understood, not a P0."
            return True, "❓ Please confirm: is this a P0? Reply 'yes' or 'no'."

    # 情况2：在 LABORATORY_GROUP 中检测到 P0 关键字（此分支之前缺失）
    if chat_id == LABORATORY_GROUP and p0.should_broadcast(original_text):
        with _pending_p0_lock:
            pending_p0_confirmation[sender_id] = {
                "timestamp": now,
                "original_text": original_text
            }
        send_func(OSE_BOT_GROUP, f'⚠️ <at user_id="{sender_id}">User</at> This is P0? (Reply "yes" or "no" without mentioning me)')
        return True, None

    return False, None

def clean_pending_p0_confirmations():
    now = datetime.now()
    with _pending_p0_lock:
        expired = [
            k
            for k, v in pending_p0_confirmation.items()
            if (now - v["timestamp"]).total_seconds() > P0_CONFIRMATION_TIMEOUT
        ]
        for k in expired:
            pending_p0_confirmation.pop(k, None)
    if expired:
        print(f"🧹 Cleaned {len(expired)} expired P0 confirmations")

# ================= P1 交互确认相关 =================
pending_p1_confirmation = {}  # key: sender_id -> {"timestamp": datetime, "original_text": str}
_pending_p1_lock = threading.Lock()
P1_CONFIRMATION_TIMEOUT = 60  # 秒
active_p1_reminders = {}      # key: sender_id -> job_id (用于取消提醒)
_active_p1_reminders_lock = threading.Lock()

def handle_p1_confirmation(chat_id, sender_id, clean_text, original_text, send_func):
    now = datetime.now()

    with _pending_p1_lock:
        pending_keys = list(pending_p1_confirmation.keys())
    print(f"[P1] ENV OSE_BOT_GROUP={OSE_BOT_GROUP}, chat_id={chat_id}, sender_id={sender_id}, clean_text='{clean_text}'")
    print(f"[P1] pending keys: {pending_keys}")

    # 情况1：用户在 OSE_BOT_GROUP 回复确认
    if chat_id == OSE_BOT_GROUP:
        with _pending_p1_lock:
            entry = pending_p1_confirmation.get(sender_id)
        if entry is not None:
            if (now - entry["timestamp"]).total_seconds() > P1_CONFIRMATION_TIMEOUT:
                with _pending_p1_lock:
                    pending_p1_confirmation.pop(sender_id, None)
                return False, None

            reply_lower = clean_text.strip().lower()
            if reply_lower in ('yes', 'y'):
                with _pending_p1_lock:
                    pending_p1_confirmation.pop(sender_id, None)
                _, confirm_reply = send_p1_alert_and_reminder(OSE_BOT_GROUP, sender_id, entry["original_text"], send_func)
                return True, confirm_reply
            if reply_lower in ('no', 'n'):
                with _pending_p1_lock:
                    pending_p1_confirmation.pop(sender_id, None)
                return True, "👌 Understood, not a P1."
            return True, "❓ Please confirm: is this a P1? Reply 'yes' or 'no'."

        print(f"[P1] sender_id {sender_id} not in pending list, ignoring.")
        return False, None

    # 情况2：在 LABORATORY_GROUP 中检测到 P1 关键字
    if chat_id == LABORATORY_GROUP and p1.should_broadcast(original_text):
        with _pending_p1_lock:
            pending_p1_confirmation[sender_id] = {
                "timestamp": now,
                "original_text": original_text
            }
        send_func(OSE_BOT_GROUP, f'⚠️ <at user_id="{sender_id}">User</at> This is P1? (Reply "yes" or "no" without mentioning me)')
        return True, None

    return False, None

def send_p1_alert_and_reminder(source_chat_id, sender_id, original_text, send_func):
    alert_msg = p1.format_p1_alert(source_chat_id, sender_id, original_text)
    send_func(OSE_BOT_GROUP, alert_msg)
    print(f"[P1] Alert sent to {OSE_BOT_GROUP}")

    reminder_text = f'<at user_id="{TARGET_USER_OPEN_ID}">User</at> ⏰ Already 15mins it might escalate to p0'
    try:
        job = reminder.schedule_reminder(
            chat_id=OSE_BOT_GROUP,
            user_id=sender_id,
            duration_str="15m",
            message=reminder_text,
            scheduler=scheduler,
            send_func=send_func
        )
        if job:
            with _active_p1_reminders_lock:
                active_p1_reminders[sender_id] = job.id
            print(f"[P1] Reminder job {job.id} saved for sender {sender_id}")
        return True, "✅ Will remind after 15minutes escalate to P0. If problem solved kindly tag me and use command /cancelp1"
    except Exception as e:
        print(f"[P1] Failed to schedule reminder: {e}")
        return True, "✅ P1 alert sent but failed to set reminder."

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


def set_lark_incoming_message(message_id: Optional[str] = None) -> None:
    mid = (message_id or "").strip() or None
    _lark_user_message_id.set(mid)
    _lark_defer_done_reaction.set(False)


def defer_lark_done_reaction() -> None:
    """Background work will call :func:`mark_lark_process_done` when finished."""
    _lark_defer_done_reaction.set(True)


def mark_lark_process_done(message_id: Optional[str] = None) -> None:
    mid = (message_id or _lark_user_message_id.get() or "").strip()
    if mid:
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
    """Send to chat, or quote-reply to ``reply_to_message_id`` (defaults to inbound user message)."""
    if reply_to_message_id is not None:
        reply_mid = (reply_to_message_id or "").strip() or None
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

    Same month: add new rows when someone is approved on Lark calendars.
    New calendar month: each table is cleared and refilled for that month only.
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
        leave_res = bundle["leaveose"]
        leave_all_res = bundle["leave_all"]
        wfh_res = bundle["wfh"]
        print(
            f"[Leave/WFH sync] {y}-{m:02d} leaveose: "
            f"deleted={leave_res.get('deleted', 0)} added={leave_res.get('added', leave_res.get('created', 0))} "
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

def clean_pending_p1_confirmations():
    now = datetime.now()
    with _pending_p1_lock:
        expired = [
            k
            for k, v in pending_p1_confirmation.items()
            if (now - v["timestamp"]).total_seconds() > P1_CONFIRMATION_TIMEOUT
        ]
        for k in expired:
            pending_p1_confirmation.pop(k, None)
    if expired:
        print(f"🧹 Cleaned {len(expired)} expired P1 confirmations")

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
        f"+ every {_LEAVE_WFH_SYNC_INTERVAL_MIN} min (first run on startup)",
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
_add_scheduler_job("monthly_duty_check", monthly_duty_check, "cron", day=1, hour=0, minute=0)
_add_scheduler_job("clean_pending_p0_confirmations", clean_pending_p0_confirmations, "interval", minutes=5)
_add_scheduler_job("clean_pending_p1_confirmations", clean_pending_p1_confirmations, "interval", minutes=5)

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
                )
        send_message(
            chat_id,
            json.dumps(batch["result_card"], ensure_ascii=False),
            msg_type="interactive",
        )
    except Exception as ex:
        send_message(chat_id, f"❌ EVO 批量 `/m` 处理失败: `{ex}`")


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
        print("[lark] bot menu event — ACK success=True", flush=True)
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
        # Never wait on ``processed_lock`` in this thread — Lark times out ~3s; lock contention → ``code: undefined``.
        def _run_card_callback_worker() -> None:
            if eid_ca and _remember_processed_message_id(eid_ca):
                print(f"⏭️ Duplicate card callback {eid_ca} ignored ({hdr_et!r})", flush=True)
                return
            if maintenance.is_evo_batch_forward_only_chat(chat_id_ca):
                print(
                    f"⏭️ EVO forward-only group — ignoring card callback ({chat_id_ca})",
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

    if maintenance.is_evo_batch_forward_only_chat(chat_id):
        print(
            f"⏭️ EVO forward-only group — ignoring inbound message ({chat_id})",
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

    # ================= 跨群组 P0 交互确认 =================
    handled_p0, p0_reply = handle_p0_confirmation(chat_id, sender_id, clean_text, original_text, send_message)
    if handled_p0:
        if p0_reply:
            send_message(chat_id, p0_reply)
        return _lark_im_done()

    # ================= 跨群组 P1 交互确认 =================
    handled_p1, p1_reply = handle_p1_confirmation(chat_id, sender_id, clean_text, original_text, send_message)
    if handled_p1:
        if p1_reply:
            send_message(chat_id, p1_reply)
        return _lark_im_done()

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

    set_lark_incoming_message(message_id)
    if message_id and (chat_type == "p2p" or bot_mentioned):
        add_gotit_reaction(message_id)

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
            "Then restart `python run_local_bot.py`.\n"
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
    
    

    try:
        import offsetleave as _offsetleave

        if _offsetleave.handle_showoffset(
            clean_text,
            chat_id=chat_id,
            send_message=send_message,
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

    # Natural English → slash command (optional; BOT_USE_AI=1). Failures keep hardcoded-only behavior.
    # The router (chathandleagent) decides COMMAND vs CHAT so a real work request is never
    # silently answered as small talk (and vice versa). Everything is guarded — on any
    # error we fall back to the previous chitchat-gate behaviour.
    _skip_commandagent = False
    _router_decision = None
    try:
        import chathandleagent as _router

        _router_decision = _router.route(
            _full_body or clean_text_multiline or clean_text,
            bot_mentioned=bot_mentioned,
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

    # Respect the router: if it already decided this is a COMMAND (e.g. /identifyissue,
    # a maintenance paste, a credit check), do NOT let the math/chat shortcut below
    # hijack it. A pasted report can contain dates/amounts that superficially look like
    # arithmetic ("2026/01/01 00:00:00 - 2026/06/03"), which previously produced a
    # bogus "couldn't parse that calculation" reply instead of running the command.
    _is_routed_command = bool(
        _router_decision is not None and getattr(_router_decision, "is_command", False)
    )
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
                bot_mentioned
                and (
                    _math_agent.has_pending_recall(_chat_memory_key)
                    or _math_agent.looks_like_memory_recall(_math_src)
                    or _math_agent.looks_like_math_memory_recall(_math_src)
                    or _math_agent.looks_like_today_memory_recall(_math_src)
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
        _dutyai_payloads = _dutyai.handle(_dutyai_src, session_key=_chat_memory_key)
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
            # Router may have already produced the mapped command (e.g. prod-batch).
            # Only adopt it when AI is enabled and it differs from the original text.
            if (
                _commandagent.is_enabled()
                and _router_decision is not None
                and getattr(_router_decision, "command", None)
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

    # 命令处理
    if clean_text.lower() == "/test":
        send_message(chat_id, _lark_test_card_json(), msg_type="interactive")
        return _lark_im_done()

    if clean_text.lower() == '/cancelp1':
        with _active_p1_reminders_lock:
            job_id = active_p1_reminders.get(sender_id)
        if job_id:
            try:
                scheduler.remove_job(job_id)
                with _active_p1_reminders_lock:
                    active_p1_reminders.pop(sender_id, None)
                reply = "✅ P1 reminder has been cancelled."
            except Exception as e:
                reply = f"❌ Failed to cancel reminder: {e}"
        else:
            reply = "ℹ️ No active P1 reminder to cancel."
        send_message(chat_id, reply)
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
    elif clean_text.lower() == '/fpmsp0':
        reply = fpms_duty.fpmsp0()
    elif clean_text.lower() == '/otpp0':
        reply = otpp1.get_otp_p0_guide()
    elif re.match(r"(?i)^/(?:identifyissue|identify_issue|checkissue|check_issue|issue|whatissue)\b", clean_text):
        try:
            import identifyissue as _identifyissue

            _issue_src = (clean_text_multiline or _full_body or clean_text or "").strip()
            _issue_body = _identifyissue.strip_command(_issue_src)
            if not _issue_body:
                send_message(chat_id, _identifyissue.USAGE)
            else:
                _issue_card, _issue_text = _identifyissue.build_card(_issue_body)
                if _issue_card:
                    _resp = send_message(
                        chat_id, json.dumps(_issue_card, ensure_ascii=False), msg_type="interactive"
                    )
                    if _resp.get("code") != 0:
                        send_message(chat_id, _issue_text)
                else:
                    send_message(chat_id, _issue_text)
        except Exception as _issue_err:
            print(f"⚠️ identifyissue failed: {_issue_err!r}", flush=True)
            send_message(chat_id, "❌ Could not analyze the issue right now. Please try again.")
        return _lark_im_done()
    elif clean_text.lower() == '/fpms':
        reply = fpms_duty.get_fpms_today_duty()
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
        reply = pms_duty.dutyNextDay()
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
        reply = bi_duty.get_bi_today_duty()
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
        reply = fe_duty.get_fe_next_three_duty()
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
        results = cpms_duty.get_cpms_three_days()
        reply = cpms_duty.format_output(results)
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
        reply = sre_Duty.get_sre_week_duty()
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
        reply = db_duty.get_three_weeks_summary()
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
        reply = liveslot_duty.get_three_weeks_summary()
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
        reply = ote_duty.get_three_weeks_summary()
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
        send_message(chat_id, duty_schedule)
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
    elif clean_text.lower().startswith('/nch'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/nch <asset_id(s)>`\nExamples: `/nch 1900`, `/nch nch2839 nch2378`, `/nch nch2839,nch2378`"
        else:
            query = parts[1]
            reply = nch.get_nch_info(query)
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower().startswith('/nwr'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/nwr <nwr_number(s)>`\nExamples: `/nwr 2005`, `/nwr 2005,2006`, `/nwr nwr2005 nwr2006`"
        else:
            query = parts[1]
            reply = nwr.get_nwr_info(query)
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower().startswith('/wf'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/wf <asset_id(s)>`\nExamples: `/wf 8092`, `/wf 8092,8093`, `/wf win8092 win8093`"
        else:
            query = parts[1]
            reply = winford.get_winford_info(query)
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower().startswith('/tbp'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/tbp <machine_id(s)>`\nExamples: `/tbp 1234`, `/tbp tbp1234 tbp5678`, `/tbp 1234,5678`"
        else:
            query = parts[1]
            reply = tbp.get_tbp_info(query)
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower().startswith('/cp') and not clean_text.lower().startswith('/cpms'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/cp <asset_number(s)>`\nExamples: `/cp 1234`, `/cp cp2839 cp2378`, `/cp cp2839,cp2378`"
        else:
            query = parts[1]
            reply = cp.get_cp_info(query)
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower().startswith('/dhs'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/dhs <asset_id(s)>`\nExamples: `/dhs 1234`, `/dhs dhs1234 dhs5678`, `/dhs 1234,5678`"
        else:
            query = parts[1]
            reply = dhs.get_dhs_info(query)
        send_message(chat_id, reply)
        return _lark_im_done()
    elif clean_text.lower().startswith('/mdr'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/mdr <asset_id(s)>`\nExamples: `/mdr 1234`, `/mdr mdr1234 mdr5678`, `/mdr 1234,5678`"
        else:
            query = parts[1]
            reply = mdr.get_mdr_info(query)
        send_message(chat_id, reply)
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
        send_message(
            chat_id,
            "⏳ Running checkmachinelog via OSS HTTP, please wait..."
            if use_oss
            else "⏳ Running checkmachinelog (LogNavigator), please wait...",
        )
        start_lark_background_thread(run_check_machine_log_job, chat_id, machine_q, date_arg)
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
            send_message(
                chat_id,
                "⚠️ Jenkins **update** was not started from that message.\n"
                "Try `@Duty Bot /jenkinsupdate rc uat master` then paste Branch/Version/Services, "
                "or say **cancel** and send the full block again.\n"
                "The bot fills the form + screenshot — you tap **Confirm** or **Cancel** (no auto-build).",
            )
            print(f"💬 Jenkins NL fallback hint for chat {chat_id}", flush=True)
        elif (mc := _parse_missing_credit_alert(_full_body)) and bot_mentioned:
            send_message(
                chat_id,
                "Use the **checkcredit** card above, or `@Duty Bot /checkcreditdate <machine>` "
                f"with player `{mc.get('account', '?')}` and date `{mc.get('date_iso', '?')}`.",
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
                # Last-resort: if the message reads like a player issue/incident
                # report, analyze it with the AI issue identifier instead of the
                # generic "could not map" message.
                _issue_handled = False
                try:
                    import identifyissue as _identifyissue_fb

                    _issue_src = (
                        clean_text_multiline or _full_body or _chat_source or clean_text or ""
                    ).strip()
                    # Fast regex first, then let the AI decide anything it missed.
                    if _identifyissue_fb.is_issue_report(_issue_src):
                        _issue_card, _issue_text = _identifyissue_fb.build_card(
                            _identifyissue_fb.strip_command(_issue_src)
                        )
                        if _issue_card:
                            _resp = send_message(
                                chat_id,
                                json.dumps(_issue_card, ensure_ascii=False),
                                msg_type="interactive",
                            )
                            if _resp.get("code") != 0:
                                send_message(chat_id, _issue_text)
                        elif _issue_text:
                            send_message(chat_id, _issue_text)
                        print(f"🔎 Issue identifier (fallback) card to chat {chat_id}", flush=True)
                        _issue_handled = True
                except Exception as _issue_fb_err:
                    print(f"⚠️ Issue identifier fallback skipped: {_issue_fb_err!r}", flush=True)
                if _issue_handled:
                    return _lark_im_done()
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

def _run_main_entry() -> int:
    """
    Same startup guard style as legacy ``run_larkbot.py``:
    - force cwd to project root
    - ensure root is on sys.path
    - print full traceback to stderr on any startup/runtime crash
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
            import jenkinsupdate as _boot_ju

            _boot_ju.prewarm_all_jenkins_browsers_on_startup()
        except Exception as _boot_ju_err:
            print(f"[warm] startup pre-warm skipped: {_boot_ju_err!r}", flush=True)
        try:
            import prod_machine_batch as _boot_pmb

            _boot_pmb.prewarm_prod_env_pool_on_startup()
        except Exception as _boot_pmb_err:
            print(f"[prod-warm] startup pre-warm skipped: {_boot_pmb_err!r}", flush=True)
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