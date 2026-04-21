import json
import re
import threading
import requests
import time
import os
from dotenv import load_dotenv

# Load .env from the project directory (works under systemd when CWD is not the app folder)
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from flask import Flask, request, jsonify
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
import amountloss

# ================= CONFIGURATION =================
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET") 
VERIFICATION_TOKEN = os.getenv("VERIFICATION_TOKEN")
DUTY_CHAT_ID = os.getenv("DUTY_CHAT_ID")
LABORATORY_GROUP = os.getenv("LABORATORY_GROUP")
OSE_BOT_GROUP = os.getenv("OSE_BOT_GROUP")

app = Flask(__name__)

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

def run_amountloss_check(chat_id, date_str=None):
    """在后台线程中执行 amount loss 检查，并将结果发送到指定 chat_id"""
    try:
        from amountloss import fetch_fpms_data
    except ImportError as e:
        send_message(
            chat_id,
            "❌ 无法加载 FPMS 抓取模块（fetch_fpms_data）。"
            f" 请把与开发环境一致的 fpms_fetcher.py 部署到服务器，并安装 playwright。\n{str(e)}",
        )
        return
    try:
        result = fetch_fpms_data(headless=True, target_date_str=date_str)
        if result == "no amount loss record found for today":
            reply = "✅ Today no Amount Loss records found"
        else:
            reply = "⚠️ As checked Amount Loss records found, kindly check!!"
        send_message(chat_id, reply)
    except Exception as e:
        error_msg = f"❌ Amount Loss 检查失败: {str(e)}"
        send_message(chat_id, error_msg)
        
def scheduled_amountloss_check():
    # 目标群聊 ID（例如你的 DUTY_CHAT_ID 或 OSE_BOT_GROUP）
    target_chat_id = DUTY_CHAT_ID   # 或直接写死群 ID
    # 默认查询昨天至今天的数据（相当于不带参数的 /al）
    run_amountloss_check(target_chat_id, date_str=None)

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
    body = {
        "receive_id": chat_id,
        "msg_type": msg_type,
        "content": json.dumps({"text": text}),
        "user_id": BOT_OPEN_ID
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
        "user_id": BOT_OPEN_ID
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

@app.route("/webhook/event", methods=["POST"])
def lark_webhook():
    data = request.json

    if data.get("type") == "url_verification":
        return jsonify({"challenge": data["challenge"]})

    token = None
    if data.get("schema") == "2.0":
        token = data.get("header", {}).get("token")
    else:
        token = data.get("token")
    if token != VERIFICATION_TOKEN:
        print(f"❌ Token mismatch: expected {VERIFICATION_TOKEN}, got {token}")
        return jsonify({"error": "Invalid token"}), 403

    sender_id = None
    if data.get("schema") == "2.0":
        event = data.get("event", {})
        sender = event.get("sender", {})
        sender_id = sender.get("sender_id", {}).get("open_id")
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
        print(f"⚠️ Ignoring unknown event type")
        return jsonify({"success": True})

    with processed_lock:
        if message_id and message_id in processed_messages:
            print(f"⏭️ Duplicate message {message_id} ignored")
            return jsonify({"success": True})
        if message_id:
            processed_messages.add(message_id)

    if not chat_id or text is None:
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

    # ================= 群组提及检查（仅用于后续命令） =================
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
        if not bot_mentioned:
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
        send_message(chat_id, "FYI Wailoon - onleave 2026-04-04 to 2026-04-17")
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
        reply = emergency.get_emergency_contacts(game_name)
        send_message(chat_id, reply)
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
    elif clean_text.lower().startswith('/al'):
            parts = clean_text.split()
            date_param = None
            if len(parts) > 1:  
                date_param = parts[1]   # 如 "16/04"
            send_message(chat_id, "⏳ 正在查询 Amount Loss，请稍候...")
            threading.Thread(target=run_amountloss_check, args=(chat_id, date_param), daemon=True).start()
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

scheduler.start()
atexit.register(lambda: scheduler.shutdown())
send_restart_ready()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)