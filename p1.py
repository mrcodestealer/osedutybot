#!/usr/bin/env python3
"""
P1 跨群组广播模块（交互确认版）：
监听 LABORATORY_GROUP 中的消息，若包含 "p1" 或 "P1"，则进入确认流程。
"""

import re
from datetime import datetime

def format_p1_alert(group_id, sender_name, text):
    """
    格式化要发送的告警消息。
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f'🚨 <b>P1 detected IN EMERGENCY GROUP(TESTING ONLY)</b> 🚨\n\n'
    msg += f"<b>Senior kindly send P1 Overview</b>\n\n"
    return msg

def should_broadcast(text):
    """
    检查文本是否包含独立的 p1 单词（不区分大小写）。
    """
    if not text:
        return False
    return re.search(r'\bp1\b', text, re.IGNORECASE) is not None

# 原 broadcast_p1 函数已移除，逻辑移至主程序中进行交互确认。