#!/usr/bin/env python3
"""
Update command module – provides Jenkins links for different projects.
"""

import difflib

# 预定义的有效命令和对应的 (显示名称, 链接)
UPDATE_MAP = {
    "fpms prod": ("FPMS PROD SCRIPT", "https://jenkins.client8.me/job/FPMS/job/FPMS_PROD_SCRIPT_RUN/"),
    "frontend uat1 h5": ("FRONTEND UAT1 H5", "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-1/job/h5-uat/"),

}

# 所有有效命令的列表（用于模糊匹配）
VALID_COMMANDS = list(UPDATE_MAP.keys())

def handle_update(args):
    """
    处理 /update 命令的参数，返回要发送的消息字符串。
    args: 用户输入的参数字符串（例如 "fpms prod" 或 "fpms prd"）
    """
    if not args:
        return "Usage: /update <project> <environment>\nExample: /update fpms prod"

    args_lower = args.strip().lower()
    # 模糊匹配，找到最相似的命令（相似度阈值 0.6）
    matches = difflib.get_close_matches(args_lower, VALID_COMMANDS, n=1, cutoff=0.6)
    if matches:
        matched = matches[0]
        display, url = UPDATE_MAP[matched]
        return f"{display}\nLINK : {url}"
    else:
        available = ", ".join(VALID_COMMANDS)
        return f"Unknown update target '{args}'. Available: {available}"