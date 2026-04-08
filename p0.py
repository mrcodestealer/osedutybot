#!/usr/bin/env python3
"""
P0 跨群组广播模块：
监听 LABORATORY_GROUP 中的消息，若包含 "p0" 或 "P0"，则转发到 OSE_BOT_GROUP。
"""

import re
from datetime import datetime

# 群组 ID 从环境变量读取（在 bot 主脚本中传入，或在此读取）
# 为了模块独立，我们设计为函数接收群组 ID 和消息内容

def format_p0_alert(group_id, sender_name, text):
    """
    格式化要发送的告警消息。
    group_id: 来源群组 ID
    sender_name: 发送者名称（可选）
    text: 原始消息内容
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"🚨 **P0 关键词告警** 🚨\n"
    msg += f"来源群组: `{group_id}`\n"
    if sender_name:
        msg += f"发送者: {sender_name}\n"
    msg += f"时间: {timestamp}\n"
    msg += f"内容: {text}"
    return msg

def should_broadcast(text):
    """
    判断消息是否包含 P0 关键词（不区分大小写）。
    可扩展为更复杂的模式，比如正则匹配 'p0' 作为独立单词。
    """
    if not text:
        return False
    # 简单匹配：包含 "p0" 或 "P0"
    return re.search(r'\bp0\b', text, re.IGNORECASE) is not None

# 如果需要更精确的独立单词匹配，使用 \b 边界
# 示例：re.search(r'\bp0\b', text, re.IGNORECASE)

def broadcast_p0(source_chat_id, target_chat_id, sender_name, message_text, send_func):
    """
    检测并广播 P0 消息。
    source_chat_id: 当前消息所在群组 ID
    target_chat_id: 目标广播群组 ID（OSE_BOT_GROUP）
    sender_name: 发送者显示名称（可选）
    message_text: 消息文本
    send_func: 发送消息的函数，例如 send_message(chat_id, text)
    返回是否执行了广播。
    """
    # 避免广播自己（如果目标群组与来源相同，则忽略）
    if source_chat_id == target_chat_id:
        return False
    # 检查关键词
    if should_broadcast(message_text):
        alert = format_p0_alert(source_chat_id, sender_name, message_text)
        send_func(target_chat_id, alert)
        return True
    return False