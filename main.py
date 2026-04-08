import json
import re
import threading
import requests
import time
import os
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

import nwr
import winford
import nch
import cp
import tbp
import dhs
import mdr

import p0
import maintenance
import emergency
import ecsre

import update

from dotenv import load_dotenv
load_dotenv()
# ================= CONFIGURATION =================

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET") 
VERIFICATION_TOKEN = os.getenv("VERIFICATION_TOKEN")
DUTY_CHAT_ID = os.getenv("DUTY_CHAT_ID")

app = Flask(__name__)

RANDOM_EMOJI_CODES = [
    "GRINNING", "JOY", "WINK", "BLUSH", "YUM", "HEART_EYES", "KISSING_HEART", "SUNGLASSES",
    "THINKING_FACE", "HUGGING_FACE", "MONKEY_FACE", "DOG", "CAT", "FOX_FACE", "LION_FACE",
    "UNICORN_FACE", "EARTH_ASIA", "VOLCANO", "APPLE", "PIZZA", "BEER", "COFFEE", "BALLOON",
    "GIFT", "TICKET", "TROPHY"
]

# List of all emoji codes from your provided list
ALL_EMOJI_CODES = [
    "GRINNING", "JOY", "WINK", "BLUSH", "YUM", "HEART_EYES", "KISSING_HEART", "SUNGLASSES",
    "THINKING_FACE", "HUGGING_FACE", "MONKEY_FACE", "DOG", "CAT", "FOX_FACE", "LION_FACE",
    "UNICORN_FACE", "EARTH_ASIA", "VOLCANO", "APPLE", "PIZZA", "BEER", "COFFEE", "BALLOON",
    "GIFT", "TICKET", "TROPHY"
]

# ================= ALL-DUTY SUMMARY AND CHECK =================

def get_all_duty_summary():
    """Collect today's duty information from all teams and return a combined message."""
    lines = []
    lines.append("📋 **ALL DUTY SUMMARY FOR TODAY** 📋\n")

    # FPMS
    lines.append("**【FPMS】**")
    lines.append(fpms_duty.get_fpms_today_duty())
    lines.append("")

    # PMS
    lines.append("**【PMS】**")
    lines.append(pms_duty.dutyNextDay())   # shows next day (which includes today if before midnight? better use dutyForDate)
    lines.append("")

    # BI
    lines.append("**【BI】**")
    lines.append(bi_duty.get_bi_today_duty())
    lines.append("")

    # FE
    lines.append("**【FE】**")
    lines.append(fe_duty.get_fe_next_three_duty())
    lines.append("")

    # CPMS
    lines.append("**【CPMS】**")
    lines.append(cpms_duty.get_cpms_three_days())
    lines.append("")

    # SRE
    lines.append("**【SRE】**")
    lines.append(sre_Duty.get_sre_week_duty())
    lines.append("")

    # DB
    lines.append("**【DB】**")
    lines.append(db_duty.get_three_weeks_summary())
    lines.append("")

    # Liveslot
    lines.append("**【Liveslot】**")
    lines.append(liveslot_duty.get_three_weeks_summary())
    lines.append("")

    # OTE
    lines.append("**【OTE】**")
    lines.append(ote_duty.get_three_weeks_summary())
    lines.append("")

    # OSE
    lines.append("**【OSE】**")
    lines.append(ose_Duty.get_ose_today_duty())
    lines.append("")

    return "\n".join(lines).strip()

def get_all_duty_check(month=None, year=None):
    """
    Run all `*_check` functions and return combined report.
    If month/year not provided, uses current month.
    """
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month

    lines = []
    lines.append(f"🔍 **DUTY MISSING REPORT – {datetime(year, month, 1).strftime('%B %Y')}** 🔍\n")

    # FPMS
    lines.append("**【FPMS】**")
    lines.append(fpms_duty.fpms_check(month=month, year=year))
    lines.append("")

    # PMS
    lines.append("**【PMS】**")
    lines.append(pms_duty.pmsCheck(month=month, year=year))
    lines.append("")

    # BI
    lines.append("**【BI】**")
    lines.append(bi_duty.bi_check(month=month, year=year))
    lines.append("")

    # FE
    lines.append("**【FE】**")
    lines.append(fe_duty.fe_check(month=month, year=year))
    lines.append("")

    # CPMS
    lines.append("**【CPMS】**")
    lines.append(cpms_duty.cpms_check(month=month, year=year))
    lines.append("")

    # SRE
    lines.append("**【SRE】**")
    lines.append(sre_Duty.sre_check(month=month, year=year))
    lines.append("")

    # DB
    lines.append("**【DB】**")
    lines.append(db_duty.db_check(month=month, year=year))
    lines.append("")

    # Liveslot
    lines.append("**【Liveslot】**")
    lines.append(liveslot_duty.liveslot_check(month=month, year=year))
    lines.append("")

    # OTE
    lines.append("**【OTE】**")
    lines.append(ote_duty.ote_check(month=month, year=year))
    lines.append("")

    return "\n".join(lines).strip()

def display_all_duty():
    """Send the all-duty summary to the designated duty chat."""
    summary = get_all_duty_summary()
    send_message(DUTY_CHAT_ID, summary)
    print("✅ Sent all-duty summary to", DUTY_CHAT_ID)

def monthly_duty_check():
    """Run all checks for the new month on the 1st day at midnight."""
    now = datetime.now()
    # Ensure we are checking the month that just started (e.g., if run at 00:00 on 1st, it's the new month)
    month = now.month
    year = now.year
    report = get_all_duty_check(month=month, year=year)
    send_message(DUTY_CHAT_ID, report)
    print(f"✅ Sent monthly duty check for {year}-{month:02d} to {DUTY_CHAT_ID}")

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
        time.sleep(1.0)   # base delay between emojis
    print(f"Added {success_count} of {len(ALL_EMOJI_CODES)} reactions")

def add_random_reaction(message_id):
    """Add a random reaction from the predefined list to a message."""
    token = get_tenant_access_token()
    url = f"https://open.larksuite.com/open-apis/im/v1/messages/{message_id}/reactions"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    random_emoji = random.choice(RANDOM_EMOJI_CODES)
    payload = {
        "reaction_type": {
            "emoji_type": random_emoji
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"✅ Added {random_emoji} reaction to message {message_id}")
    else:
        print(f"❌ Failed to add reaction: {response.text}")
    return response.json()

def add_heart_reaction(message_id):
    """Add a heart reaction to a message."""
    token = get_tenant_access_token()
    url = f"https://open.larksuite.com/open-apis/im/v1/messages/{message_id}/reactions"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "reaction_type": {
            "emoji_type": "HEART"
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"✅ Added heart reaction to message {message_id}")
    else:
        print(f"❌ Failed to add reaction: {response.text}")
    return response.json()
    
def recall_message(message_id):
    """Delete a message using Lark's recall API."""
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
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    body = {
        "receive_id": chat_id,
        "msg_type": msg_type,
        "content": json.dumps({"text": text}),
        "user_id": BOT_OPEN_ID   # <-- Add the bot's open_id
    }
    if mentions:
        body["mentions"] = mentions
    params = {"receive_id_type": "chat_id"}
    response = requests.post(url, headers=headers, params=params, json=body)
    return response.json()

def send_file(chat_id, file_token):
    """Send a file message using a file_token."""
    token = get_tenant_access_token()
    url = "https://open.larksuite.com/open-apis/im/v1/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
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
    """Upload a file to Lark Drive and return the file_token."""
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
    """Send a shift reminder message to the given chat."""
    send_message(chat_id, message)
    print(f"⏰ Shift reminder sent to {chat_id}: {message}")

def morning_reminder():
    """7am message: Rest Well yesterday night shift / Good Luck today morning shift"""
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    # Get yesterday's night shift
    _, night_yesterday = ose_Duty.get_shift_names_for_date(yesterday)
    # Get today's morning shift
    morning_today, _ = ose_Duty.get_shift_names_for_date(today)

    lines = []

    # Rest Well section (night shift of yesterday)
    if night_yesterday:
        lines.append("(～￣▽￣)～ Rest Well")
        for name in night_yesterday:
            lines.append(f"• {name}")
        lines.append("")  # blank line

    # Good Luck section (morning shift of today)
    if morning_today:
        lines.append("Good Luckヾ(≧▽≦*)o")
        for name in morning_today:
            lines.append(f"• {name}")

    # If both are empty, send a generic message
    if not night_yesterday and not morning_today:
        lines.append("(～￣▽￣)～ Rest Well Night Shift\nGood Luck Morning Shift ヾ(≧▽≦*)o")

    msg = "\n".join(lines)
    # Prepend a mention for the target user
    mention_line = f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>'
    msg = mention_line + "\n" + msg
    send_shift_reminder(DUTY_CHAT_ID, msg)

def evening_reminder():
    """7pm message: Rest Well today morning shift / Good Luck tonight night shift"""
    today = datetime.now().date()

    # Get today's morning shift
    morning_today, _ = ose_Duty.get_shift_names_for_date(today)
    # Get today's night shift
    _, night_today = ose_Duty.get_shift_names_for_date(today)

    lines = []

    # Rest Well section (morning shift of today)
    if morning_today:
        lines.append("(～￣▽￣)～ Rest Well")
        for name in morning_today:
            lines.append(f"• {name}")
        lines.append("")  # blank line

    # Good Luck section (night shift of today)
    if night_today:
        lines.append("Good Luckヾ(≧▽≦*)o")
        for name in night_today:
            lines.append(f"• {name}")

    if not morning_today and not night_today:
        lines.append("(～￣▽￣)～ Rest Well Morning Shift\nGood Luck Night Shift ヾ(≧▽≦*)o")

    msg = "\n".join(lines)
    # Prepend a mention for the target user
    mention_line = f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>'
    msg = mention_line + "\n" + msg
    send_shift_reminder(DUTY_CHAT_ID, msg)

def amountloss():
    # Prepend a mention for the target user
    mention_line = f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>'
    msg = mention_line + "\n" + "Hi Morning Shift kindly reminder to do Amount Loss~"
    send_shift_reminder(DUTY_CHAT_ID, msg)
    
def myoseweeklymeeting():
    # Prepend a mention for the target user
    mention_line = f'<at user_id="{TARGET_USER_OPEN_ID}">User</at>'
    msg = mention_line + "\n" + "MY OSE WEEKLY MEETING"
    send_shift_reminder(DUTY_CHAT_ID, msg)

scheduler = BackgroundScheduler()

# Shift 
scheduler.add_job(func=morning_reminder, trigger="cron", hour=7, minute=00)
scheduler.add_job(func=evening_reminder, trigger="cron", hour=19, minute=0)
# Amount Loss
scheduler.add_job(func=amountloss, trigger="cron", hour=9, minute=0)
# MY OSE WEEKLY MEETING
scheduler.add_job(func=myoseweeklymeeting, trigger="cron", hour=17, minute=0)


# New: daily all-duty summary at midnight
#scheduler.add_job(func=display_all_duty, trigger="cron", hour=0, minute=0)

# New: monthly duty check on 1st day at midnight
scheduler.add_job(func=monthly_duty_check, trigger="cron", day=1, hour=0, minute=0)

PENDING_RESTART_FILE = "restart_pending.json"

def write_restart_pending(chat_id):
    """Save the chat_id where the restart command was issued."""
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
    """If a restart was pending, send a ready message and clean up."""
    if not os.path.exists(PENDING_RESTART_FILE):
        return
    try:
        with open(PENDING_RESTART_FILE, "r") as f:
            data = json.load(f)
        chat_id = data.get("chat_id")
        timestamp_str = data.get("timestamp")
        if chat_id and timestamp_str:
            timestamp = datetime.fromisoformat(timestamp_str)
            # Only send if the restart was requested recently (within last minute)
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

    # 1. URL verification (优先处理，不依赖 message_id)
    if data.get("type") == "url_verification":
        return jsonify({"challenge": data["challenge"]})

    # 2. Token verification (同样不依赖 message_id)
    token = None
    if data.get("schema") == "2.0":
        token = data.get("header", {}).get("token")
    else:
        token = data.get("token")
    if token != VERIFICATION_TOKEN:
        print(f"❌ Token mismatch: expected {VERIFICATION_TOKEN}, got {token}")
        return jsonify({"error": "Invalid token"}), 403

    # 3. 提取 sender_id（用于后续判断）
    sender_id = None
    if data.get("schema") == "2.0":
        event = data.get("event", {})
        sender = event.get("sender", {})
        sender_id = sender.get("sender_id", {}).get("open_id")
    else:
        event = data.get("event", {})
        sender_id = event.get("open_id") or event.get("user_id")

    # 忽略 Bot 自己发送的消息
    if sender_id and sender_id == BOT_OPEN_ID:
        print("⏭️ Ignoring own message")
        return jsonify({"success": True})

        # 4. Initialize variables
    chat_id = None
    text = None
    chat_type = None
    mentions = []
    message_id = None
    is_mention_old = False   # <-- for old schema

    # 5. Extract message details (two schema branches)
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
        mentions = event.get("mentions", [])          # usually empty
        is_mention_old = event.get("is_mention", False)   # <-- capture flag
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

    # 7. Check required fields
    if not chat_id or text is None:
        print("❌ Could not extract chat_id or text")
        return jsonify({"error": "Missing data"}), 400
    
    if text == "我要验牌":
        reply = f'<at user_id="{sender_id}"></at> 给我擦皮鞋'
        send_message(chat_id, reply)
        return jsonify({"success": True})
    
    if text == "good luck" or text == "Good luck":
        add_heart_reaction(message_id)
        #send_message(chat_id, "Good luck to you too! 🍀")
        
    if text ==  "random":
        add_random_reaction(message_id)
        
    if text == "spamreact":
        add_all_reactions(message_id)
        return jsonify({"success": True})  # Optional: stop further processing

    # 8. Group mention check (supports both schemas)
    if chat_type == "group":
        bot_mentioned = False
        for mention in mentions:
            # Get the id field; it could be a dict (new schema) or a string (old schema)
            mention_id_obj = mention.get("id")
            if isinstance(mention_id_obj, dict):
                mention_id = mention_id_obj.get("open_id", "")
            else:
                mention_id = mention_id_obj  # old schema: direct string

            # Debug: print the extracted open_id
            print(f"🔍 Mention open_id: {mention_id}")

            if mention_id == BOT_OPEN_ID:
                bot_mentioned = True
                print(f"✅ Bot mentioned by open_id: {mention_id}")
                break

        # Check old schema via is_mention flag
        if not bot_mentioned and is_mention_old:
            bot_mentioned = True
            print("✅ Bot mentioned (old schema via is_mention flag)")

        if not bot_mentioned:
            print("⏭️ Bot not mentioned in group chat – ignoring")
            # Optionally log full event for ignored messages
            # print("Ignored event full data:", json.dumps(data, indent=2))
            return jsonify({"success": True})
        
    # 9. 清理文本中的提及占位符
    original_text = text
    print(f"📝 Original text: {repr(original_text)}")

    mention_keys = [m.get("key", "") for m in mentions if m.get("key")]
    print(f"🔑 Mention keys: {mention_keys}")
    for key in mention_keys:
        text = text.replace(key, "")

    # Additional cleanup
    text = re.sub(r'@_user_\d+', '', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()

    clean_text = text
    print(f"🧹 Cleaned text (repr): {repr(clean_text)}")
    
    if game.has_active_game(sender_id):
        reply, should_clear, job_id = game.check_answer(sender_id, clean_text)
        if reply:
            # Cancel the scheduled recall (if any)
            if job_id:
                try:
                    scheduler.remove_job(job_id)
                except Exception as e:
                    print(f"⚠️ Could not cancel job {job_id}: {e}")
            send_message(chat_id, reply)
        return jsonify({"success": True})

    # 10. COMMAND HANDLING
    if len(clean_text) >= 3 and clean_text[:3].lower() == '/s ':
        query = clean_text[3:].strip()
        print(f"🔍 Duty query extracted: '{query}'")
        reply = search_duty(query)
    elif clean_text.lower() == '/date':
        today = get_today_date()
        reply = f"Today's date is {today}."
        print(f"📅 Date reply: {reply}")
    elif clean_text.lower() == '/holiday':
        reply = format_holidays()
    elif clean_text.lower() == '/holidaythismonth':
        reply = holidays_this_month()
        print(f"📅 Holiday reply: {reply}")
        
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
        
    elif clean_text.lower().startswith('/p0test'):
        junchen = "ou_5f660c0fb0769d184aca635d02209272"
        # Split command from arguments
        parts = clean_text.split(maxsplit=1)
        args = parts[1].strip() if len(parts) > 1 else ""
        p0_details = p0.handle_p0(args)
        reply = (
            "🧪 **P0 Test**\n\n"
            f'<at user_id="{junchen}">Jun Chen</at>\n\n'
            f"{p0_details}"
        )
        send_message(chat_id, reply)
        return jsonify({"success": True})

    elif clean_text.lower().startswith('/p0'):
        yuxuan = "ou_c4346ace5927c14f51a89b2394b55338"
        junchen = "ou_5f660c0fb0769d184aca635d02209272"
        shen = "ou_060a405a6f3fd35456b719ef9ba62a86"
        parts = clean_text.split(maxsplit=1)
        args = parts[1].strip() if len(parts) > 1 else ""
        p0_details = p0.handle_p0(args)
        reply = (
            "📍 !!!P0 Incident Alert!!!\n\n"
            #f'<at user_id="{junchen}">Jun Chen</at>\n'
            f'<at user_id="{yuxuan}">Yuxuan</at>\n'
            f'<at user_id="{shen}">Shen</at>\n\n'
            f"{p0_details}"
        )
        send_message(chat_id, reply)
        return jsonify({"success": True})
    
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
                # Expect format: MM/YYYY or YYYY-MM
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
                # Expect format: MM/YYYY or YYYY-MM
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
        reply = "yes?"
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
        # Format the results properly
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
        reply = ose_Duty.get_ose_today_duty()          # uses today's date
    elif clean_text.startswith('/osedate'):
        parts = clean_text.split(maxsplit=1)
        if len(parts) == 1:
            # no date argument → show today's duty
            reply = ose_Duty.get_ose_today_duty()
        else:
            date_str = parts[1].strip()
            try:
                target_date = datetime.strptime(date_str, "%d/%m/%Y").date()
                reply = ose_Duty.get_ose_duty_for_date(target_date)   # function from ose_Duty
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
        # 处理 /ecsre
        game_name = cmd_parts[1] if len(cmd_parts) > 1 else None
        reply = ecsre.get_responsible_games(game_name)
        send_message(chat_id, reply)
        return jsonify({"success": True})

    elif cmd == '/ec':
        # 处理 /ec
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
        # Determine which command was used
        cmd = 'maintenance' if clean_text.lower().startswith('/maintenance') else 'maintenanceshort'
        # Use original_text (the raw message) to capture everything after the command
        pattern = rf'/{cmd}\s+(.*)'
        match = re.search(pattern, original_text, re.IGNORECASE | re.DOTALL)
        if match:
            email_text = match.group(1).strip()
            # Remove surrounding quotes if present
            if email_text.startswith('"') and email_text.endswith('"'):
                email_text = email_text[1:-1]
        else:
            email_text = ''

        if email_text:
            # First message: tag the user with the table name
            game_name = maintenance.get_table_name(email_text)
            if game_name == "Unknown":
                game_name = "Unknown table"
            #first_reply = f'<at user_id="ou_8faac9cb9f7bf3ee69dc09f8e1f147bc">User</at> {game_name}'
            #send_message(chat_id, first_reply)

            # Second message: full summary
            second_reply = maintenance.process_email(email_text)
            send_message(chat_id, second_reply)
        else:
            send_message(chat_id, "Please provide the email text after the command.")
        return jsonify({"success": True})

        
    ################################################################################
    ##################        Machine List       ###################################
    
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
        # Extract the part after /nwr (if any)
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
            reply = "❌ Usage: `/winford <asset_id(s)>`\nExamples: `/winford 8092`, `/winford 8092,8093`, `/winford win8092 win8093`"
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
    
    ################################################################################
    
    elif clean_text.lower().startswith('/secret1'):
        # Use the original text (before cleanup) to extract the mentioned user
        match = re.search(r'<at open_id="([^"]+)"[^>]*>([^<]+)</at>', original_text)
        if match:
            open_id = match.group(1)
            name = match.group(2).strip()
            reply = f"Tagged {name} with open_id: {open_id}\nMention: <at user_id=\"{open_id}\">{name}</at>"
        else:
            # Fallback for new schema (mentions array)
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
    
    elif clean_text.lower() == '/secret2':
        
        reply = f"This group ID is {chat_id}"
        send_message(chat_id, reply)
        return jsonify({"success": True})
        
    elif clean_text.lower() in ['/memorytest']:
        # Start game
        number = game.start_game(sender_id)
        # Send the number message
        send_result = send_message(chat_id, f"🧠 **Memory Game**\nRemember this number: **{number}**\nYou have 5 seconds to type it back.")
        message_id = send_result.get("data", {}).get("message_id")
        if message_id:
            # Schedule recall after 2 seconds
            run_date = datetime.now() + timedelta(seconds=2)
            job = scheduler.add_job(func=recall_message, trigger='date', run_date=run_date, args=[message_id])
            game.set_game_job(sender_id, job.id)
        return jsonify({"success": True}) 
    
    elif clean_text.lower().startswith('/reminder'):
        parts = clean_text.split()
        if len(parts) < 3:
            reply = "❌ Usage: `/reminder [at] <time|duration> <message>`\nExamples:\n  `/reminder 1h30m Team meeting`\n  `/reminder 8:39PM Lunch`\n  `/reminder at 2039 Break`"
        else:
            # Skip optional 'at' and find the first token that looks like a time/duration
            time_or_duration = None
            msg_start_idx = None
            for i in range(1, len(parts)):
                token = parts[i]
                if token.lower() == 'at':
                    continue
                # Detect absolute time patterns
                if (':' in token or token.lower().endswith(('am', 'pm')) or
                    (token.isdigit() and len(token) == 4)):
                    time_or_duration = token
                    msg_start_idx = i + 1
                    break
            if time_or_duration is None:
                # No absolute time found, assume first non-at token is a duration
                if len(parts) > 1:
                    time_or_duration = parts[1]
                    msg_start_idx = 2
                else:
                    reply = "❌ Missing time/duration and message."

            if time_or_duration and msg_start_idx and msg_start_idx < len(parts):
                message = ' '.join(parts[msg_start_idx:])
                # Decide which scheduler to use
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
        # Send initial notification
        send_message(chat_id, "🔄 Restarting bot...")
        
        # Write pending restart info so the new instance can announce readiness
        write_restart_pending(chat_id)
        
        # Shut down the scheduler
        scheduler.shutdown(wait=False)
        
        # Delay exit to allow messages to be sent
        def delayed_exit():
            time.sleep(1)
            os._exit(0)
        
        threading.Thread(target=delayed_exit).start()
        return jsonify({"success": True})

    send_message(chat_id, reply)
    print(f"✅ Replied to chat {chat_id}: {reply}")

    return jsonify({"success": True})

scheduler.start()
atexit.register(lambda: scheduler.shutdown())

# Send ready message if this is a restart
send_restart_ready()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
