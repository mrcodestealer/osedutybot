import json
import re
import requests
from flask import Flask, request, jsonify
from datetime import datetime, timedelta

# Import command handlers from separate modules
from duty_utils import search_duty
from date_utils import get_today_date, format_holidays
from duty_schedule_utils import get_today_duty, get_department_duty, format_table

# ================= CONFIGURATION =================
APP_ID = "cli_a9ca652b89b85ed1"
APP_SECRET = "VQJh0oFKfsyCHr5tQDMVNbr4o4kmjbFr"
VERIFICATION_TOKEN = "17CBLymsuOVyXaRv6Ig4lhDBXH3n1giI"

app = Flask(__name__)

# ================= LARK API HELPERS =================
def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    response = requests.post(url, headers=headers, json=data)
    return response.json().get("tenant_access_token")

def send_message(chat_id, text, msg_type="text"):
    token = get_tenant_access_token()
    url = "https://open.larksuite.com/open-apis/im/v1/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    body = {
        "receive_id": chat_id,
        "msg_type": msg_type,
        "content": json.dumps({"text": text})
    }
    params = {"receive_id_type": "chat_id"}
    response = requests.post(url, headers=headers, params=params, json=body)
    return response.json()

# ================= WEBHOOK HANDLER =================
@app.route("/webhook/event", methods=["POST"])
def lark_webhook():
    data = request.json
    print("📥 Received raw data:", json.dumps(data, indent=2))

    # 1. URL verification
    if data.get("type") == "url_verification":
        print("✅ Handling url_verification")
        return jsonify({"challenge": data["challenge"]})

    # 2. Token verification
    token = None
    if data.get("schema") == "2.0":
        token = data.get("header", {}).get("token")
    else:
        token = data.get("token")

    if token != VERIFICATION_TOKEN:
        print(f"❌ Token mismatch: expected {VERIFICATION_TOKEN}, got {token}")
        return jsonify({"error": "Invalid token"}), 403

    # 3. Extract message details
    chat_id = None
    text = None
    chat_type = None
    mentions = []

    if data.get("header", {}).get("event_type") == "im.message.receive_v1":
        event = data.get("event", {})
        message = event.get("message", {})
        chat_id = message.get("chat_id")
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
        chat_type = event.get("chat_type")
        mentions = event.get("mentions", [])
        text = event.get("text", "")
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

    if not chat_id or text is None:
        print("❌ Could not extract chat_id or text")
        return jsonify({"error": "Missing data"}), 400

    # 4. MENTION CHECK (only respond if bot is mentioned in group chats)
    BOT_NAME = "TagResponderBot"  # ⬅️ Your bot's exact display name

    if chat_type == "group":
        bot_mentioned = False
        for mention in mentions:
            mention_name = mention.get("name", "")
            if mention_name == BOT_NAME:
                bot_mentioned = True
                print(f"✅ Bot mentioned by name: {mention_name}")
                break
        if not bot_mentioned:
            print("⏭️ Bot not mentioned in group chat – ignoring")
            return jsonify({"success": True})
    # Private chat: always reply

    # 5. REMOVE MENTION PLACEHOLDERS
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

    # 6. COMMAND HANDLING
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
        print(f"📅 Holiday reply: {reply}")
    elif clean_text.lower() == '/duty':
        reply = get_today_duty()
        print(f"📋 Duty reply: {reply}")
    elif clean_text.lower() in ['/fpms', '/bi', '/sre', '/db', '/fe', '/cpms', '/pms']:
        dept_map = {
            '/fpms': 'fpms',
            '/bi': 'bi',
            '/sre': 'sre',
            '/db': 'db',
            '/fe': 'fe',
            '/cpms': 'cpms',
            '/pms': 'pms'
        }
        dept_key = dept_map[clean_text.lower()]
        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)
        next_day = today + timedelta(days=2)

        def get_table(date):
            rows = get_department_duty(dept_key, date)
            if not rows:
                return f"No duty for {date.strftime('%Y-%m-%d')}."
            table_rows = [[name, phone] for name, phone in rows]
            return format_table(["Duty name", "Phone number"], table_rows)

        reply_parts = []
        reply_parts.append(f"Duty Today : {today.strftime('%Y-%m-%d')}")
        reply_parts.append(get_table(today))
        reply_parts.append("")
        reply_parts.append(f"Duty Next day : {tomorrow.strftime('%Y-%m-%d')}")
        reply_parts.append(get_table(tomorrow))
        reply_parts.append("")
        reply_parts.append(f"Duty Next next day : {next_day.strftime('%Y-%m-%d')}")
        reply_parts.append(get_table(next_day))
        reply = "\n".join(reply_parts)
        print(f"📋 Department duty reply: {reply}")
    else:
        reply = "I only respond to `/s <name>`, `/date`, `/holiday`, `/duty`, or department commands like `/fpms`, `/bi`, `/sre`, `/db`, `/fe`, `/cpms`, `/pms`."

    send_message(chat_id, reply)
    print(f"✅ Replied to chat {chat_id}: {reply}")

    return jsonify({"success": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)