#!/usr/bin/env python3
"""
P1 跨群组广播模块：
监听 LABORATORY_GROUP 中的消息，若包含 "p1" 或 "P1"，则转发到 OSE_BOT_GROUP。
"""

import re
from datetime import datetime
import sre_Duty

# 群组 ID 从环境变量读取（在 bot 主脚本中传入，或在此读取）
# 为了模块独立，我们设计为函数接收群组 ID 和消息内容

def format_p1_alert(group_id, sender_name, text):
    """
    格式化要发送的告警消息。
    group_id: 来源群组 ID
    sender_name: 发送者名称（可选）
    text: 原始消息内容
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f'🚨 <b>P1 detected IN EMERGENCY GROUP(TESTING ONLY)</b> 🚨\n\n'
    msg += f"<b>Senior kindly send P1 Overview</b>\n\n"
    return msg

def should_broadcast(text):
    if not text:
        return False
    result = re.search(r'\bp1\b', text, re.IGNORECASE) is not None
    print(f"[P1] should_broadcast check: text='{text[:50]}', result={result}")
    return result

# 如果需要更精确的独立单词匹配，使用 \b 边界
# 示例：re.search(r'\bp1\b', text, re.IGNORECASE)

def broadcast_p1(source_chat_id, target_chat_id, sender_name, message_text, send_func):
    print(f"[P1] broadcast_p0 called: source={source_chat_id}, target={target_chat_id}")
    print(f"[P1] message_text: {message_text[:100]}")
    if source_chat_id == target_chat_id:
        print("[P1] source == target, skipping")
        return False
    if should_broadcast(message_text):
        print("[P1] should_broadcast returned True")
        alert = format_p1_alert(source_chat_id, sender_name, message_text)
        print(f"[P1] alert content: {alert[:200]}")
        try:
            send_func(target_chat_id, alert)
            print(f"[P1] alert sent to {target_chat_id}")
        except Exception as e:
            print(f"[P1] Failed to send alert: {e}")
        return True
    else:
        print("[P1] should_broadcast returned False")
    return False

