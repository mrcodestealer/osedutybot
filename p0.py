#!/usr/bin/env python3
"""
P0 跨群组广播模块（交互确认版）：
监听 LABORATORY_GROUP 中的消息，若包含 "p0" 或 "P0"，则触发确认流程。
"""

import re
from datetime import datetime
import sre_Duty

def format_p0_alert(group_id, sender_name, text):
    """
    格式化要发送的告警消息。
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f'🚨 <b>P0 detected IN EMERGENCY GROUP(TESTING ONLY)</b> 🚨\n\n'
    msg += f"<b>Senior kindly send P0 Overview</b>\n\n"
    msg += f"<b>PH MEMBER Kindly call</b>\nAldan 9660695459\nMiyu 9158346719\nSheng K 9993978561\n\n"
    msg += f'<b>MY MEMBER CALL SRE DUTY</b> \n' + sre_Duty.p0sre() 
    msg += f"\n\n<b>Identify P0 Issue</b>\nKindly provide what P0 issue now: /p0major /p0fpms /p0cpms /p0otp\n"
    return msg

def should_broadcast(text):
    if not text:
        return False
    result = re.search(r'\bp0\b', text, re.IGNORECASE) is not None
    print(f"[P0] should_broadcast check: text='{text[:50]}', result={result}")
    return result