import json
import re
import sys
import threading
from typing import Optional
import requests
import time
import os
from dotenv import load_dotenv

# Resolve imports from this repo regardless of process cwd (systemd, gunicorn, etc.)
_CHBOX_DIR = os.path.dirname(os.path.abspath(__file__))
if _CHBOX_DIR not in sys.path:
    sys.path.insert(0, _CHBOX_DIR)

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
from funny import get_miao, lucifer, dog
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

import p0
import p1
import maintenance
import emergency
import ecsre

import update
import otpp1

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


# ================= CONFIGURATION =================
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET") 
VERIFICATION_TOKEN = (os.getenv("VERIFICATION_TOKEN") or "").strip()
DUTY_CHAT_ID = os.getenv("DUTY_CHAT_ID")
LABORATORY_GROUP = os.getenv("LABORATORY_GROUP")
OSE_BOT_GROUP = os.getenv("OSE_BOT_GROUP")

app = Flask(__name__)

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


def run_amountloss_check(chat_id, date_str=None):
    """在后台线程中执行 amount loss 检查，并将结果发送到指定 chat_id（失败自动重跑一轮）"""
    try:
        from amountloss import fetch_fpms_data
    except ImportError as e:
        send_message(
            chat_id,
            "❌ 无法加载 FPMS 抓取模块（fetch_fpms_data）。"
            f" 请把与开发环境一致的 fpms_fetcher.py 部署到服务器，并安装 playwright。\n{str(e)}",
        )
        return

    for attempt in range(1, AMOUNT_LOSS_MAX_ATTEMPTS + 1):
        try:
            result = fetch_fpms_data(
                headless=True,
                target_date_str=date_str,
                filterdata=True,
                checklog=True,
            )
            if isinstance(result, dict) and result.get("lark_card"):
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


def run_checkcredit_finderror(chat_id, machine_query: str, date_str: str):
    """Background: same as checkcredit + `--date`. Uses OSS HTTP if CHECKCREDIT_USE_OSS is set."""
    try:
        import checkcredit
    except ImportError as e:
        send_message(chat_id, f"❌ Cannot load checkcredit module: {e}")
        return
    try:
        td = datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
        use_oss = os.getenv("CHECKCREDIT_USE_OSS", "").strip().lower() in ("1", "true", "yes", "on")
        out = checkcredit.run_finderror(
            str(machine_query).strip(),
            target_date=td,
            timeout_ms=max(15_000, 90_000),
            base=checkcredit.DEFAULT_BASE,
            user=checkcredit.DEFAULT_USER,
            pw=checkcredit.DEFAULT_PASS,
            source="oss" if use_oss else "navigator",
        )
        text = (out.get("text") or "").strip()
        cards: list[dict] = []
        for key in ("lark_card", "lark_card_summary", "lark_card_candidates"):
            c = out.get(key)
            if isinstance(c, dict):
                cards.append(c)
        if cards:
            card_failed = False
            for card in cards:
                card_json = json.dumps(card)
                resp = send_message(chat_id, card_json, msg_type="interactive")
                if resp.get("code") != 0:
                    card_failed = True
                    break
            if card_failed:
                send_message(chat_id, text if text else "(no output)")
        else:
            send_message(chat_id, text if text else "(no output)")

        np = out.get("np_followup")
        if isinstance(np, dict):
            _set_checkcredit_np_pending(chat_id, np)
            choices = np.get("np_choices") or []
            try:
                np_card = checkcredit.build_np_choice_lark_card(
                    choices,
                    target_date_iso=str(np.get("target_date") or ""),
                    machine_display=str(np.get("machine_display") or ""),
                    third_http_backend=str(np.get("third_http_backend") or "NP"),
                )
                card_json = json.dumps(np_card)
                resp_np = send_message(chat_id, card_json, msg_type="interactive")
                if resp_np.get("code") != 0:
                    lines = []
                    _td = str(np.get("target_date") or "").strip()
                    _md = str(np.get("machine_display") or "").strip()
                    _be = str(np.get("third_http_backend") or "NP").strip().upper()
                    if _be not in ("NP", "WF", "DHS", "NCH", "CP", "OSM", "MDR", "TBP"):
                        _be = "NP"
                    if _td or _md:
                        lines.append(
                            f"Log date ({_be} window): `{_td or '?'}` · Machine: `{_md or '?'}`"
                        )
                    for i, ch in enumerate(choices):
                        uid = ch.get("user_id", "")
                        cr = ch.get("credit", "n/a")
                        ts = ch.get("time_short") or "n/a"
                        lines.append(f"{i + 1}) User ID `{uid}` — last credit `{cr}` @ `{ts}`")
                    send_message(chat_id, "\n".join(lines) if lines else "(no NP choices)")
            except Exception as e:
                print(f"[checkcredit] NP choice card failed: {e!r}")
                lines = []
                _td = str(np.get("target_date") or "").strip()
                _md = str(np.get("machine_display") or "").strip()
                _be = str(np.get("third_http_backend") or "NP").strip().upper()
                if _be not in ("NP", "WF", "DHS", "NCH", "CP", "OSM", "MDR", "TBP"):
                    _be = "NP"
                if _td or _md:
                    lines.append(f"Log date ({_be} window): `{_td or '?'}` · Machine: `{_md or '?'}`")
                for i, ch in enumerate(choices):
                    uid = ch.get("user_id", "")
                    cr = ch.get("credit", "n/a")
                    ts = ch.get("time_short") or "n/a"
                    lines.append(f"{i + 1}) User ID `{uid}` — last credit `{cr}` @ `{ts}`")
                send_message(chat_id, "\n".join(lines) if lines else "(no NP choices)")
    except Exception as e:
        send_message(chat_id, f"❌ checkcredit failed: {e}")
        print(f"[checkcredit] error: {e!r}")


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
        send_message(chat_id, f"❌ Cannot load checkcredit module: {e}")
        return
    except AttributeError:
        send_message(
            chat_id,
            "❌ checkcredit.screenshot_np_recharge_detail missing — deploy the latest `checkcredit.py`.",
        )
        return
    backend_tag = getattr(checkcredit, "_np_log_backend_tag", lambda _: "NP")(
        (machine_display or "").strip() or None
    )
    send_message(
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
            send_message(chat_id, "❌ Failed to upload screenshot to Lark.")
            return
        r = send_image_message(chat_id, key)
        if r.get("code") != 0:
            send_message(chat_id, f"❌ Failed to send image: {r}")
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
        send_message(chat_id, f"❌ {backend_tag} third-http screenshot failed: {e}{tip}")
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
        send_message(
            chat_id,
            "❌ No active NP choice list — run `/checkcreditdate …` again, then reply **1**–**4**.",
        )
        return
    ch = choices[choice_idx - 1]
    uid = str(ch.get("user_id") or "").strip()
    date_iso = (pend.get("target_date") or "").strip()
    time_short = (ch.get("time_short") or "").strip()
    if not uid or not date_iso or not time_short:
        send_message(chat_id, "❌ Pending NP choice is incomplete — use `/npthirdhttp …` with full date/time.")
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
        send_message(chat_id, f"❌ Cannot load checkcredit module: {e}")
        return
    except AttributeError:
        send_message(
            chat_id,
            "❌ checkcredit.screenshot_np_recharge_detail missing — deploy the latest `checkcredit.py`.",
        )
        return
    if not argv:
        send_message(
            chat_id,
            "❌ Usage: `/npthirdhttp <player_id>` — or `/npthirdhttp <player_id> YYYY-MM-DD HH:MM:SS.mmm`",
        )
        return
    uid = argv[0].strip()
    date_iso: Optional[str] = None
    time_short: Optional[str] = None
    pend = None
    if len(argv) == 2:
        send_message(
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
            send_message(chat_id, "❌ Date must be `YYYY-MM-DD`.")
            return
        if not time_short:
            send_message(chat_id, "❌ Missing time (HH:MM:SS or HH:MM:SS.mmm).")
            return
    else:
        pend = _get_checkcredit_np_pending(chat_id)
        if not pend:
            send_message(
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
            send_message(
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
    每日 9:00：在 DUTY_CHAT_ID 群 @TARGET_USER_OPEN_ID，
    文案格式：As checked today amount loss show 0 records / Search time: 0.029 seconds
    （数字与耗时来自 amountloss.fetch_fpms_data 实时结果）
    """
    target_chat_id = DUTY_CHAT_ID
    try:
        from amountloss import fetch_fpms_data
    except ImportError as e:
        send_message(
            target_chat_id,
            f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>\n'
            f"❌ 无法加载 amountloss 模块: {str(e)}",
        )
        return

    result = None
    for attempt in range(1, AMOUNT_LOSS_MAX_ATTEMPTS + 1):
        try:
            result = fetch_fpms_data(headless=True, target_date_str=None)
            break
        except Exception as e:
            if attempt < AMOUNT_LOSS_MAX_ATTEMPTS:
                send_message(
                    target_chat_id,
                    f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>\n{AMOUNT_LOSS_RETRY_NOTICE}',
                )
                print(f"[scheduled_amountloss_check] attempt {attempt} failed: {e!r}, auto-retrying...")
            else:
                send_message(
                    target_chat_id,
                    f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>\n'
                    f"❌ Amount Loss 检查失败: {str(e)}",
                )
                print(f"scheduled_amountloss_check failed after {AMOUNT_LOSS_MAX_ATTEMPTS} attempts: {e}")
                return

    if result is None:
        return

    line = (result or "").strip()
    # 页面原文多为 "Total 0 records / Search time: ..."，去掉开头 Total 以贴合口头文案
    line = re.sub(r"^Total\s+", "", line, count=1, flags=re.IGNORECASE)
    mention = f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>'
    text = f"{mention}\nAs checked amount loss show {line}"
    send_message(target_chat_id, text)
    print(f"✅ Scheduled Amount Loss (9:00) sent to {target_chat_id}")

# ================= P0 交互确认相关 =================
pending_p0_confirmation = {}  # key: sender_id -> {"timestamp": datetime, "original_text": str}
P0_CONFIRMATION_TIMEOUT = 60  # 秒

def handle_p0_confirmation(chat_id, sender_id, clean_text, original_text, send_func):
    global pending_p0_confirmation
    now = datetime.now()

    # 情况1：用户在 OSE_BOT_GROUP 回复确认
    if chat_id == OSE_BOT_GROUP and sender_id in pending_p0_confirmation:
        entry = pending_p0_confirmation[sender_id]
        if (now - entry["timestamp"]).total_seconds() > P0_CONFIRMATION_TIMEOUT:
            del pending_p0_confirmation[sender_id]
            return False, None

        reply_lower = clean_text.strip().lower()
        if reply_lower in ('yes', 'y'):
            del pending_p0_confirmation[sender_id]
            alert_msg = p0.format_p0_alert(chat_id, sender_id, entry["original_text"])
            send_func(OSE_BOT_GROUP, alert_msg)
            return True, None
        elif reply_lower in ('no', 'n'):
            del pending_p0_confirmation[sender_id]
            return True, "👌 Understood, not a P0."
        else:
            return True, "❓ Please confirm: is this a P0? Reply 'yes' or 'no'."

    # 情况2：在 LABORATORY_GROUP 中检测到 P0 关键字（此分支之前缺失）
    if chat_id == LABORATORY_GROUP and p0.should_broadcast(original_text):
        pending_p0_confirmation[sender_id] = {
            "timestamp": now,
            "original_text": original_text
        }
        send_func(OSE_BOT_GROUP, f'⚠️ <at user_id="{sender_id}">User</at> This is P0? (Reply "yes" or "no" without mentioning me)')
        return True, None

    return False, None

def clean_pending_p0_confirmations():
    now = datetime.now()
    expired = [k for k, v in pending_p0_confirmation.items()
               if (now - v["timestamp"]).total_seconds() > P0_CONFIRMATION_TIMEOUT]
    for k in expired:
        del pending_p0_confirmation[k]
    if expired:
        print(f"🧹 Cleaned {len(expired)} expired P0 confirmations")

# ================= P1 交互确认相关 =================
pending_p1_confirmation = {}  # key: sender_id -> {"timestamp": datetime, "original_text": str}
P1_CONFIRMATION_TIMEOUT = 60  # 秒
active_p1_reminders = {}      # key: sender_id -> job_id (用于取消提醒)

def handle_p1_confirmation(chat_id, sender_id, clean_text, original_text, send_func):
    global pending_p1_confirmation
    now = datetime.now()

    print(f"[P1] ENV OSE_BOT_GROUP={OSE_BOT_GROUP}, chat_id={chat_id}, sender_id={sender_id}, clean_text='{clean_text}'")
    print(f"[P1] pending keys: {list(pending_p1_confirmation.keys())}")

    # 情况1：用户在 OSE_BOT_GROUP 回复确认
    if chat_id == OSE_BOT_GROUP:
        if sender_id in pending_p1_confirmation:
            entry = pending_p1_confirmation[sender_id]
            if (now - entry["timestamp"]).total_seconds() > P1_CONFIRMATION_TIMEOUT:
                del pending_p1_confirmation[sender_id]
                return False, None

            reply_lower = clean_text.strip().lower()
            if reply_lower in ('yes', 'y'):
                del pending_p1_confirmation[sender_id]
                _, confirm_reply = send_p1_alert_and_reminder(OSE_BOT_GROUP, sender_id, entry["original_text"], send_func)
                return True, confirm_reply
            elif reply_lower in ('no', 'n'):
                del pending_p1_confirmation[sender_id]
                return True, "👌 Understood, not a P1."
            else:
                # 回复了其他内容，提示确认
                return True, "❓ Please confirm: is this a P1? Reply 'yes' or 'no'."
        else:
            # 在 OSE_BOT_GROUP 发言，但 sender_id 不在待确认列表中（可能是其他人或超时）
            # 不做任何处理，让消息继续走其他命令（但会被提及检查拦截，除非@机器人）
            print(f"[P1] sender_id {sender_id} not in pending list, ignoring.")
            return False, None

    # 情况2：在 LABORATORY_GROUP 中检测到 P1 关键字
    if chat_id == LABORATORY_GROUP and p1.should_broadcast(original_text):
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
    token = get_tenant_access_token()
    url = f"https://open.larksuite.com/open-apis/im/v1/messages/{message_id}/reactions"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"reaction_type": {"emoji_type": "HEART"}}
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"✅ Added heart reaction to message {message_id}")
    else:
        print(f"❌ Failed to add reaction: {response.text}")
    return response.json()
    
def recall_message(message_id):
    token = get_tenant_access_token()
    url = f"https://open.larksuite.com/open-apis/im/v1/messages/{message_id}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.delete(url, headers=headers)
    if resp.status_code == 200:
        print(f"✅ Message {message_id} recalled")
    else:
        print(f"❌ Failed to recall message {message_id}: {resp.text}")
        
def send_message(chat_id, text, msg_type="text", mentions=None):
    token = get_tenant_access_token()
    url = "https://open.larksuite.com/open-apis/im/v1/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if msg_type == "interactive":
        # Lark: content is the interactive card JSON string (not wrapped in {"text": ...}).
        content = text if isinstance(text, str) else json.dumps(text)
    else:
        content = json.dumps({"text": text})
    # Lark `POST /im/v1/messages` request body is only receive_id + msg_type + content (+ optional uuid).
    # Do not send undocumented fields — stray keys have caused odd interactive-card behavior in the wild.
    body = {
        "receive_id": chat_id,
        "msg_type": msg_type,
        "content": content,
    }
    if mentions:
        body["mentions"] = mentions
    params = {"receive_id_type": "chat_id"}
    response = requests.post(url, headers=headers, params=params, json=body)
    return response.json()

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
    with open(image_path, "rb") as f:
        files = {"image": (os.path.basename(image_path), f, "image/png")}
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


def _set_checkcredit_np_pending(chat_id: str, payload: dict) -> None:
    CHECKCREDIT_NP_PENDING[chat_id] = {"payload": payload, "ts": time.time()}


def _get_checkcredit_np_pending(chat_id: str, max_age_sec: float = 3600.0):
    ent = CHECKCREDIT_NP_PENDING.get(chat_id)
    if not ent:
        return None
    if time.time() - ent["ts"] > max_age_sec:
        del CHECKCREDIT_NP_PENDING[chat_id]
        return None
    return ent["payload"]

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

def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    response = requests.post(url, headers=headers, json=data)
    return response.json().get("tenant_access_token")

# ================= SCHEDULED REMINDERS =================
TARGET_USER_OPEN_ID = "ou_d7bc33724e2d6ced4050c944c2ca5650"
# Sheet-based daily reminders are always delivered to this group.
REMINDER_TARGET_CHAT_ID = os.getenv(
    "REMINDER_TARGET_CHAT_ID",
    "oc_9de3d63fc589df6feeb9b0bee9c45b72",
).strip() or "oc_9de3d63fc589df6feeb9b0bee9c45b72"

def send_shift_reminder(chat_id, message):
    send_message(chat_id, message)
    print(f"⏰ Shift reminder sent to {chat_id}: {message}")

def morning_reminder():
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    _, night_yesterday = ose_Duty.get_shift_names_for_date(yesterday)
    morning_today, _ = ose_Duty.get_shift_names_for_date(today)
    lines = []
    if night_yesterday:
        lines.append("(～￣▽￣)～ Rest Well")
        for name in night_yesterday:
            lines.append(f"• {name}")
        lines.append("")
    if morning_today:
        lines.append("Good Luckヾ(≧▽≦*)o")
        for name in morning_today:
            lines.append(f"• {name}")
    if not night_yesterday and not morning_today:
        lines.append("(～￣▽￣)～ Rest Well Night Shift\nGood Luck Morning Shift ヾ(≧▽≦*)o")
    msg = "\n".join(lines)
    mention_line = f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>'
    msg = mention_line + "\n" + msg
    send_shift_reminder(DUTY_CHAT_ID, msg)

def evening_reminder():
    today = datetime.now().date()
    morning_today, _ = ose_Duty.get_shift_names_for_date(today)
    _, night_today = ose_Duty.get_shift_names_for_date(today)
    lines = []
    if morning_today:
        lines.append("(～￣▽￣)～ Rest Well")
        for name in morning_today:
            lines.append(f"• {name}")
        lines.append("")
    if night_today:
        lines.append("Good Luckヾ(≧▽≦*)o")
        for name in night_today:
            lines.append(f"• {name}")
    if not morning_today and not night_today:
        lines.append("(～￣▽￣)～ Rest Well Morning Shift\nGood Luck Night Shift ヾ(≧▽≦*)o")
    msg = "\n".join(lines)
    mention_line = f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>'
    msg = mention_line + "\n" + msg
    send_shift_reminder(DUTY_CHAT_ID, msg)

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
    expired = [k for k, v in pending_p1_confirmation.items()
               if (now - v["timestamp"]).total_seconds() > P1_CONFIRMATION_TIMEOUT]
    for k in expired:
        del pending_p1_confirmation[k]
    if expired:
        print(f"🧹 Cleaned {len(expired)} expired P1 confirmations")

scheduler = BackgroundScheduler()
scheduler.add_job(func=morning_reminder, trigger="cron", hour=7, minute=00)
scheduler.add_job(func=evening_reminder, trigger="cron", hour=19, minute=0)
scheduler.add_job(func=scheduled_amountloss_check,trigger="cron",hour=9,minute=0)
scheduler.add_job(func=myoseweeklymeeting, trigger="cron", day_of_week='tue', hour=17, minute=0)
scheduler.add_job(func=monthly_duty_check, trigger="cron", day=1, hour=0, minute=0)
scheduler.add_job(func=clean_pending_p0_confirmations, trigger="interval", minutes=5)
scheduler.add_job(func=clean_pending_p1_confirmations, trigger="interval", minutes=5)

PENDING_RESTART_FILE = "restart_pending.json"

def write_restart_pending(chat_id):
    data = {
        "chat_id": chat_id,
        "timestamp": datetime.now().isoformat()
    }
    try:
        with open(PENDING_RESTART_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"❌ Failed to write restart pending file: {e}")

def send_restart_ready():
    if not os.path.exists(PENDING_RESTART_FILE):
        return
    try:
        with open(PENDING_RESTART_FILE, "r") as f:
            data = json.load(f)
        chat_id = data.get("chat_id")
        timestamp_str = data.get("timestamp")
        if chat_id and timestamp_str:
            timestamp = datetime.fromisoformat(timestamp_str)
            if (datetime.now() - timestamp).total_seconds() < 60:
                send_message(chat_id, "✅ Bot is ready.")
        os.remove(PENDING_RESTART_FILE)
    except Exception as e:
        print(f"❌ Failed to send restart ready: {e}")
        try:
            os.remove(PENDING_RESTART_FILE)
        except:
            pass

def get_bot_open_id():
    token = get_tenant_access_token()
    url = "https://open.larksuite.com/open-apis/contact/v3/users/me?user_id_type=open_id"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers).json()
    if resp.get("code") == 0:
        return resp["data"]["user"]["open_id"]
    print("❌ Failed to get bot open_id:", resp)
    return None

BOT_OPEN_ID = "ou_1f6596a9923a2a835918e7e2513595d5"

processed_messages = set()
processed_lock = threading.Lock()


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


def _lark_extract_verification_token(data):
    # type: (Optional[dict]) -> Optional[str]
    """v2 events put token under ``header.token``; legacy uses top-level ``token``. Always try both."""
    if not isinstance(data, dict):
        return None
    h = data.get("header")
    if isinstance(h, dict):
        for key in ("token", "Token", "verification_token"):
            t = h.get(key)
            if t is not None:
                return str(t).strip()
    for key in ("token", "verification_token"):
        t2 = data.get(key)
        if t2 is not None:
            return str(t2).strip()
    return None


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
    data["event"] = ev
    return data


def _lark_extract_card_event_fields(ev):
    # type: (dict) -> tuple
    """
    Best-effort chat / sender / button value from ``event`` (see Lark ``card.action.trigger`` doc).
    In **group chats**, ``open_chat_id`` may appear on ``event`` top-level before ``context``; try that first
    (avoids ``code: undefined`` when chat_id was only read from ``context``).
    """
    ctx = ev.get("context") or {}
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
                "同一页「订阅事件」里勾选 card.action.trigger（卡片回传交互），保存后创建版本并发布应用。",
                "环境变量 VERIFICATION_TOKEN 与后台「Verification Token」完全一致（无多空格）。",
                "若开启了加密：设 LARK_ENCRYPT_KEY；未开启加密：后台关掉加密或勿配密钥。",
                "点按钮时 journalctl 应出现 [lark] webhook POST；若没有，请求没到本进程（DNS/防火墙/URL 错误）。",
                "若日志有 ❌ Token mismatch → 修正 VERIFICATION_TOKEN；403 会导致客户端报错/code undefined。",
            ]
        return jsonify(payload)

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

    raw_in = _lark_safe_parse_json_body(request)
    if raw_in is None:
        return jsonify({"error": "invalid json"}), 400
    data = _feishu_maybe_decrypt_webhook_payload(raw_in)
    data = _lark_coerce_event_dict(data)
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
    if token != VERIFICATION_TOKEN:
        sch = data.get("schema") if isinstance(data, dict) else None
        print(
            f"❌ Token mismatch: expected {VERIFICATION_TOKEN}, got {token!r} schema={sch!r}",
            flush=True,
        )
        return jsonify({"error": "Invalid token"}), 403

    hdr_et = _lark_header_event_type(data)

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
        # Never wait on ``processed_lock`` in this thread — Lark times out ~3s; lock contention → ``code: undefined``.
        def _run_card_callback_worker() -> None:
            with processed_lock:
                if eid_ca and eid_ca in processed_messages:
                    print(f"⏭️ Duplicate card callback {eid_ca} ignored ({hdr_et!r})", flush=True)
                    return
                if eid_ca:
                    processed_messages.add(eid_ca)
            ju = _get_jenkinsupdate()
            if not ju or not chat_id_ca:
                print(
                    f"⚠️ card action skipped: ju={bool(ju)} chat_id={chat_id_ca!r} "
                    f"event_type={hdr_et!r}",
                    flush=True,
                )
                return
            try:
                ev_ca = data.get("event") if isinstance(data.get("event"), dict) else {}
                op_ca = ev_ca.get("operator") if isinstance(ev_ca.get("operator"), dict) else {}
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
                ju.handle_lark_jenkins_card_action(chat_id_ca, sender_use, val_ca, send_message)
            except Exception as ex:
                print(f"❌ handle_lark_jenkins_card_action: {ex!r}", flush=True)
                try:
                    send_message(chat_id_ca, f"❌ Jenkins card action failed: {ex}")
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
    chat_type = None
    mentions = []
    message_id = None
    is_mention_old = False

    if data.get("header", {}).get("event_type") == "im.message.receive_v1":
        event = data.get("event", {})
        message = event.get("message", {})
        chat_id = message.get("chat_id")
        message_id = message.get("message_id")
        chat_type = message.get("chat_type")
        mentions = message.get("mentions", [])
        content_str = message.get("content", "{}")
        try:
            content = json.loads(content_str)
            text = content.get("text", "")
        except json.JSONDecodeError:
            text = ""
    elif data.get("type") == "event_callback":
        event = data.get("event", {})
        chat_id = event.get("open_chat_id") or event.get("chat_id")
        message_id = event.get("open_message_id") or event.get("message_id")
        chat_type = event.get("chat_type")
        mentions = event.get("mentions", [])
        is_mention_old = event.get("is_mention", False)
        text = event.get("text_without_at_bot") or event.get("text", "")
        if not text:
            content_str = event.get("content")
            if content_str:
                try:
                    content = json.loads(content_str)
                    text = content.get("text", "")
                except:
                    pass
    else:
        het = _lark_header_event_type(data)
        print("⚠️ Unknown webhook branch hdr_et=%r (not im.message / event_callback)" % (het,), flush=True)
        # Never return ``success:true`` for card/interaction — include legacy payloads without ``schema:2.0``.
        if (
            _lark_is_schema_v2(data)
            or _lark_payload_has_card_action(data)
            or (het and het.lower().startswith("card.action"))
        ):
            return _lark_http_card_callback_ok()
        return jsonify({"success": True})

    with processed_lock:
        if message_id and message_id in processed_messages:
            print(f"⏭️ Duplicate message {message_id} ignored")
            return jsonify({"success": True})
        if message_id:
            processed_messages.add(message_id)

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
    
    if text == "我要验牌":
        reply = f'<at user_id="{sender_id}"></at> 给我擦皮鞋'
        send_message(chat_id, reply)
        return jsonify({"success": True})
    
    if text == "good luck" or text == "Good luck":
        add_heart_reaction(message_id)
        
    if text == "random":
        add_random_reaction(message_id)
        
    if text == "spamreact":
        add_all_reactions(message_id)
        return jsonify({"success": True})

    original_text = text
    print(f"📝 Original text: {repr(original_text)}")

        # 清理提及占位符（先做清理，便于后续命令处理）
    mention_keys = [m.get("key", "") for m in mentions if m.get("key")]
    for key in mention_keys:
        text = text.replace(key, "")
    text = re.sub(r'@_user_\d+', '', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    clean_text = text
    print(f"🧹 Cleaned text (repr): {repr(clean_text)}")

        # 清理提及占位符（先做清理，便于后续命令处理）
    mention_keys = [m.get("key", "") for m in mentions if m.get("key")]
    for key in mention_keys:
        text = text.replace(key, "")
    text = re.sub(r'@_user_\d+', '', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    clean_text = text
    print(f"🧹 Cleaned text (repr): {repr(clean_text)}")

        # ================= 跨群组 P0 交互确认 =================
    handled_p0, p0_reply = handle_p0_confirmation(chat_id, sender_id, clean_text, original_text, send_message)
    if handled_p0:
        if p0_reply:
            send_message(chat_id, p0_reply)
        return jsonify({"success": True})

    # ================= 跨群组 P1 交互确认 =================
    handled_p1, p1_reply = handle_p1_confirmation(chat_id, sender_id, clean_text, original_text, send_message)
    if handled_p1:
        if p1_reply:
            send_message(chat_id, p1_reply)
        return jsonify({"success": True})

    # Reply **1**–**4** after `/checkcreditdate` NP prompt — works in group **without** @bot
    stripped_choice = clean_text.strip()
    if stripped_choice in ("1", "2", "3", "4"):
        pend_np = _get_checkcredit_np_pending(chat_id)
        choices_np = (pend_np or {}).get("np_choices") or []
        idx_np = int(stripped_choice)
        if pend_np and 1 <= idx_np <= len(choices_np):
            threading.Thread(
                target=run_np_third_http_by_choice,
                args=(chat_id, idx_np),
                daemon=True,
            ).start()
            return jsonify({"success": True})

    # ================= 群组 @bot（后续命令需要提及；FPMS 多步会话中跟帖可不 @） =================
    bot_mentioned = chat_type != "group"
    if chat_type == "group":
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

    ju = _get_jenkinsupdate()
    jenkins_sess_active = (
        ju.jenkins_update_has_active_lark_session(chat_id, sender_id) if ju else False
    )
    if ju and ju.handle_lark_jenkins_update_message(
        chat_id,
        sender_id,
        clean_text,
        original_text,
        send_message,
        allow_start=bot_mentioned,
        lark_sender_union_id=sender_union_id,
    ):
        return jsonify({"success": True})

    if chat_type == "group" and not bot_mentioned and not jenkins_sess_active:
        print("⏭️ Bot not mentioned in group chat – ignoring further commands")
        return jsonify({"success": True})

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
        return jsonify({"success": True})
    
    

    # 命令处理
    if clean_text.lower() == '/cancelp1':
        if sender_id in active_p1_reminders:
            job_id = active_p1_reminders[sender_id]
            try:
                scheduler.remove_job(job_id)
                del active_p1_reminders[sender_id]
                reply = "✅ P1 reminder has been cancelled."
            except Exception as e:
                reply = f"❌ Failed to cancel reminder: {e}"
        else:
            reply = "ℹ️ No active P1 reminder to cancel."
        send_message(chat_id, reply)
        return jsonify({"success": True})
    
    elif len(clean_text) >= 3 and clean_text[:3].lower() == '/s ':
        query = clean_text[3:].strip()
        print(f"🔍 Duty query extracted: '{query}'")
        reply = search_duty(query)
    elif clean_text.lower() == '/date':
        today = get_today_date()
        reply = f"Today's date is {today}."
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
    elif clean_text.lower() == '/picture cat':
        file_token = get_cat_file_token()
        if file_token:
            result = send_file(chat_id, file_token)
            if result.get("code") != 0:
                send_message(chat_id, f"❌ Failed to send cat picture: {result}")
        else:
            send_message(chat_id, "❌ Failed to upload cat picture.")
        return jsonify({"success": True})
    elif clean_text.lower() == '/fpmsp0':
        reply = fpms_duty.fpmsp0()
    elif clean_text.lower() == '/otpp0':
        reply = otpp1.get_otp_p0_guide()
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
        return jsonify({"success": True})
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
        return jsonify({"success": True})
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
        return jsonify({"success": True})
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
        return jsonify({"success": True})
    elif clean_text.lower() == '/cpms':
        results = cpms_duty.get_cpms_three_days()
        formatted = cpms_duty.format_output(results)
        send_message(chat_id, formatted)
        return jsonify({"success": True})
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
        return jsonify({"success": True})
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
        return jsonify({"success": True})
    elif clean_text.lower() == '/db':
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
        return jsonify({"success": True})
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
        return jsonify({"success": True})
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
        return jsonify({"success": True})
    elif clean_text.lower() == '/ft':
        duty_schedule = ft.get_ft_three_days()
        send_message(chat_id, duty_schedule)   
        fyi_message = """FYI
        Phan Qi Xiang - Try whatsapp first, else use phone line 
        Kevin Lim       - Call phone number , not whatapps call
        Pin Quan        - Try whatsapp first, else use phone line
        Winson Hong   - Try to spam 
        """
        send_message(chat_id, fyi_message)
        return jsonify({"success": True})  
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
        return jsonify({"success": True})
    elif clean_text == '/ose':
        reply = ose_Duty.get_ose_today_duty()
    elif clean_text.startswith('/osedate'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = ose_Duty.get_ose_today_duty()
        else:
            date_str = parts[1].strip()
            try:
                target_date = datetime.strptime(date_str, "%d/%m/%Y").date()
                reply = ose_Duty.get_ose_duty_for_date(target_date)
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
        return jsonify({"success": True})
    
    cmd_parts = clean_text.split()
    if not cmd_parts:
        return
    cmd = cmd_parts[0].lower()
    if cmd == '/ecsre':
        game_name = cmd_parts[1] if len(cmd_parts) > 1 else None
        reply = ecsre.get_responsible_games(game_name)
        send_message(chat_id, reply)
        return jsonify({"success": True})
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
        return jsonify({"success": True})
    elif clean_text == '/cashout':
        reply = f'the player has been get back his credit. @On-Duty-OSM-Lavie(Podium1) kindly manual cashout the credit and reboot the machine. After that, @Xavier (CS OSM) kindly unset and test the machine thanks'
        send_message(chat_id, reply)
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/update'):
        parts = clean_text.split(maxsplit=1)
        args = parts[1].strip() if len(parts) > 1 else ""
        reply = update.handle_update(args)
        send_message(chat_id, reply)
        return jsonify({"success": True})
    elif clean_text == '/restartA':
        reply = f'cd /home/pi/osm && ./stopallserver.sh && ./startserver.sh'
        send_message(chat_id, reply)
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/maintenance') or clean_text.lower().startswith('/maintenanceshort'):
        cmd = 'maintenance' if clean_text.lower().startswith('/maintenance') else 'maintenanceshort'
        pattern = rf'/{cmd}\s+(.*)'
        match = re.search(pattern, original_text, re.IGNORECASE | re.DOTALL)
        if match:
            email_text = match.group(1).strip()
            if email_text.startswith('"') and email_text.endswith('"'):
                email_text = email_text[1:-1]
        else:
            email_text = ''
        if email_text:
            game_name = maintenance.get_table_name(email_text)
            if game_name == "Unknown":
                game_name = "Unknown table"
            second_reply = maintenance.process_email(email_text)
            send_message(chat_id, second_reply)
        else:
            send_message(chat_id, "Please provide the email text after the command.")
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/nch'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/nch <asset_id(s)>`\nExamples: `/nch 1900`, `/nch nch2839 nch2378`, `/nch nch2839,nch2378`"
        else:
            query = parts[1]
            reply = nch.get_nch_info(query)
        send_message(chat_id, reply)
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/nwr'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/nwr <nwr_number(s)>`\nExamples: `/nwr 2005`, `/nwr 2005,2006`, `/nwr nwr2005 nwr2006`"
        else:
            query = parts[1]
            reply = nwr.get_nwr_info(query)
        send_message(chat_id, reply)
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/wf'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/wf <asset_id(s)>`\nExamples: `/wf 8092`, `/wf 8092,8093`, `/wf win8092 win8093`"
        else:
            query = parts[1]
            reply = winford.get_winford_info(query)
        send_message(chat_id, reply)
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/tbp'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/tbp <machine_id(s)>`\nExamples: `/tbp 1234`, `/tbp tbp1234 tbp5678`, `/tbp 1234,5678`"
        else:
            query = parts[1]
            reply = tbp.get_tbp_info(query)
        send_message(chat_id, reply)
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/cp'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/cp <asset_number(s)>`\nExamples: `/cp 1234`, `/cp cp2839 cp2378`, `/cp cp2839,cp2378`"
        else:
            query = parts[1]
            reply = cp.get_cp_info(query)
        send_message(chat_id, reply)
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/dhs'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/dhs <asset_id(s)>`\nExamples: `/dhs 1234`, `/dhs dhs1234 dhs5678`, `/dhs 1234,5678`"
        else:
            query = parts[1]
            reply = dhs.get_dhs_info(query)
        send_message(chat_id, reply)
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/mdr'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/mdr <asset_id(s)>`\nExamples: `/mdr 1234`, `/mdr mdr1234 mdr5678`, `/mdr 1234,5678`"
        else:
            query = parts[1]
            reply = mdr.get_mdr_info(query)
        send_message(chat_id, reply)
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/secret1'):
        match = re.search(r'<at open_id="([^"]+)"[^>]*>([^<]+)</at>', original_text)
        if match:
            open_id = match.group(1)
            name = match.group(2).strip()
            reply = f"Tagged {name} with open_id: {open_id}\nMention: <at user_id=\"{open_id}\">{name}</at>"
        else:
            target_mention = None
            for m in mentions:
                mention_id = m.get("id", {})
                if isinstance(mention_id, dict):
                    open_id = mention_id.get("open_id")
                else:
                    open_id = mention_id
                if open_id and open_id != BOT_OPEN_ID:
                    target_mention = m
                    break
            if target_mention:
                key = target_mention.get("key", "")
                match = re.search(r'<at[^>]*>(.*?)</at>', key)
                name = match.group(1) if match else "user"
                open_id = target_mention.get("id", {}).get("open_id") if isinstance(target_mention.get("id"), dict) else target_mention.get("id")
                reply = f"Tagged {name} with open_id: {open_id}\nMention: <at user_id=\"{open_id}\">{name}</at>"
            else:
                reply = "❌ No user mentioned correctly. Use `/secret1 @user` (mention the user)."
        send_message(chat_id, reply)
        return jsonify({"success": True})
    elif re.match(r"^/al(?:\s+\d{1,2}/\d{1,2})?\s*$", clean_text.lower()):
            # /al or /al DD/MM: run Amount Loss checklog flow in background, return interactive card + TSV.
            parts = clean_text.split()
            date_param = parts[1].strip() if len(parts) > 1 else None
            send_message(chat_id, "⏳ Checking Amount Loss (CHECKLOG), please wait...")
            threading.Thread(target=run_amountloss_check, args=(chat_id, date_param), daemon=True).start()
            return jsonify({"success": True})
    elif clean_text.lower().startswith("/npthirdhttp"):
        parts = clean_text.split()
        threading.Thread(
            target=run_np_third_http_job,
            args=(chat_id, parts[1:]),
            daemon=True,
        ).start()
        return jsonify({"success": True})
    elif re.search(r"/(?:checkcreditdate|checkcredit)\b", clean_text, re.I):
        # Longer token first in alternation so `/checkcreditdate` is not parsed as `/checkcredit` + `date`.
        # Optional date defaults to **today** (server local) when omitted — e.g. `@Duty Bot /checkcredit 1171`.
        m_cc = re.search(
            r"/(checkcreditdate|checkcredit)\b\s+(\S+)(?:\s+(\d{4}-\d{2}-\d{2}))?",
            clean_text,
            re.I,
        )
        if not m_cc:
            send_message(
                chat_id,
                "❌ Usage:\n"
                "• `/checkcredit <machine>` — **today** (same as `--date` omitted in CLI)\n"
                "• `/checkcreditdate <machine> [YYYY-MM-DD]` — optional date; omit for today\n"
                "Examples: `@Duty Bot /checkcredit 1171` · `/checkcreditdate 2074 2026-04-27`\n"
                "(same as `python3 checkcredit.py --finderror … --date YYYY-MM-DD`)",
            )
            return jsonify({"success": True})
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
            return jsonify({"success": True})
        use_oss_wait = os.getenv("CHECKCREDIT_USE_OSS", "").strip().lower() in ("1", "true", "yes", "on")
        send_message(
            chat_id,
            "⏳ Running checkcredit via OSS HTTP , please wait..."
            if use_oss_wait
            else "⏳ Running LogNavigator checkcredit, browser may take a while — please wait...",
        )
        threading.Thread(
            target=run_checkcredit_finderror,
            args=(chat_id, machine_q, date_arg),
            daemon=True,
        ).start()
        return jsonify({"success": True})
    elif clean_text.lower().startswith("/smsfail"):
        send_message(chat_id, "⏳ Running SMS gateway OTP log check, please wait...")
        threading.Thread(target=run_smsfail_check, args=(chat_id,), daemon=True).start()
        return jsonify({"success": True})
    elif clean_text.lower().startswith("/smscheckplayer"):
        parts = clean_text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            send_message(
                chat_id,
                "❌ Usage: `/smscheckplayer <player_id(s)>` — today 00:00—now, up to 3 newest logs per player; e.g. `/smscheckplayer 127317237` or `/smscheckplayer 7052472, 1069954565` (commas / spaces / newlines OK)",
            )
            return jsonify({"success": True})
        payload = parts[1].strip()
        send_message(
            chat_id,
            "⏳ Running SMS OTP log check for player(s) (today 00:00—now, up to 3 newest rows each), please wait...",
        )
        threading.Thread(
            target=run_smscheckplayer_check,
            args=(chat_id, payload),
            daemon=True,
        ).start()
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/pid'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            reply = "❌ Usage: `/pid <provider_id>`\nExamples: `/pid 30`, `/pid 30 31 32`"
        else:
            query = parts[1]
            reply = providerid.get_provider_info(query)
        send_message(chat_id, reply)
        return jsonify({"success": True})
    elif clean_text.lower() == '/secret2':
        reply = f"当前群组的 ID 是：{chat_id}"
        result = send_message(chat_id, reply)
        message_id = result.get('data', {}).get('message_id')
        if message_id:
            run_date = datetime.now() + timedelta(seconds=8)
            scheduler.add_job(func=recall_message, trigger='date', run_date=run_date, args=[message_id])
        return jsonify({"success": True})
    elif clean_text.lower() in ['/memorytest']:
        number = game.start_game(sender_id)
        send_result = send_message(chat_id, f"🧠 **Memory Game**\nRemember this number: **{number}**\nYou have 5 seconds to type it back.")
        message_id = send_result.get("data", {}).get("message_id")
        if message_id:
            run_date = datetime.now() + timedelta(seconds=2)
            job = scheduler.add_job(func=recall_message, trigger='date', run_date=run_date, args=[message_id])
            game.set_game_job(sender_id, job.id)
        return jsonify({"success": True}) 
    elif clean_text.lower().startswith('/reminder'):
        parts = clean_text.split()
        if len(parts) < 3:
            reply = "❌ Usage: `/reminder [at] <time|duration> <message>`\nExamples:\n  `/reminder 1h30m Team meeting`\n  `/reminder 8:39PM Lunch`\n  `/reminder at 2039 Break`"
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
        return jsonify({"success": True})
    elif clean_text.lower().startswith('/addreminder'):
        parts = clean_text.split(maxsplit=4)
        if len(parts) < 5:
            send_message(
                chat_id,
                "❌ Usage: `/addreminder <start_date> <end_date> <time> <reason>`\n"
                "Date: `YYYY/MM/DD` (or `MM/DD` for current year)\n"
                "Time: `HH:MMPM/AM` (e.g. `6:30PM`)\n"
                "Example: `/addreminder 2026/04/29 2026/06/06 6:30PM Just for testing`",
            )
            return jsonify({"success": True})
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
                send_message(
                    chat_id,
                    "Reply with `/deletereminder <ID> [ID] [ID]` to delete multiple reminders.",
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
        return jsonify({"success": True})

    # 如果前面没有任何命令匹配，并且 reply 为空，则忽略
    if reply:
        send_message(chat_id, reply)
        print(f"✅ Replied to chat {chat_id}: {reply}")
    else:
        print(f"⚠️ No command matched and no reply generated for chat {chat_id}")

    return jsonify({"success": True})

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

scheduler.start()
atexit.register(lambda: scheduler.shutdown())
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