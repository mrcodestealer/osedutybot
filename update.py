#!/usr/bin/env python3
"""
Update command module – provides Jenkins links for different projects.
"""

import difflib

# 预定义的有效命令和对应的 (显示名称, 链接)
UPDATE_MAP = {
    "fpms prod script": ("FPMS PROD SCRIPT", "https://jenkins.client8.me/job/FPMS/job/FPMS_PROD_SCRIPT_RUN/"),
    "frontend uat1 h5": ("FRONTEND UAT1 H5", "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-1/job/h5-uat/build?delay=0sec"),
    "frontend uat2 h5": ("FRONTEND UAT2 H5", "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-2/job/h5-uat/build?delay=0sec"),
    "frontend uat3 h5": ("FRONTEND UAT3 H5", "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-3/job/h5-uat/build?delay=0sec"),
    "frontend uat4 h5": ("FRONTEND UAT4 H5", "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-4/job/h5-uat/build?delay=0sec"),
    "frontend uat1 web": ("FRONTEND UAT1 WEB", "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-1/job/web-uat/build?delay=0sec"),
    "frontend uat2 web": ("FRONTEND UAT2 WEB", "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-2/job/web-uat/build?delay=0sec"),
    "frontend uat3 web": ("FRONTEND UAT3 WEB", "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-3/job/web-uat/build?delay=0sec"),
    "frontend uat4 web": ("FRONTEND UAT4 WEB", "https://jenkins.client8.me/job/FRONTEND/job/UAT/job/projects/job/uat-4/job/web-uat/build?delay=0sec"),
    "fpms uat fgs": ("FPMS FGS", "https://jenkins.client8.me/job/FGS_CLIENT/job/FGS-UAT-UPDATE/build?delay=0sec"),
    "ccms uat fe bo": ("FPMS_NT_UAT_BO_UPDATE", "https://jenkins.client8.me/job/FPMS_NT/view/all/job/FPMS_NT_UAT_BO_UPDATE/build?delay=0sec"),
    "fpms uat fnt": ("FPMS FNT","https://jenkins.client8.me/job/FNT/job/FNT_UAT_SCRIPT_RUN/build?delay=0sec"),
    "cpms uat update": ("CPMS-UAT-UPDATE","\nhttps://jenkins.client8.me/job/IGO/job/UAT/job/IGO-UAT-UPDATE/build?delay=0sec\nhttps://jenkins.client8.me/job/CPMS/job/UAT/job/CPMS-UAT-UPDATE/build?delay=0sec"),
    "igo uat script run": ("IGO UAT SCRIPT RUN","https://jenkins.client8.me/job/IGO/job/UAT/job/IGO-UAT-SCRIPT-RUN/")
}

# 所有有效命令的列表（用于模糊匹配）
VALID_COMMANDS = list(UPDATE_MAP.keys())

def handle_update(args):
    """
    处理 /update 命令的参数，返回要发送的消息字符串。
    args: 用户输入的参数字符串（例如 "fpms prod" 或 "fpms prd"）
    
    行为：
      - 如果 args 精确或部分匹配多个命令（子串匹配），列出所有匹配项。
      - 如果只匹配一个，直接返回该命令的链接。
      - 如果没有匹配，尝试模糊匹配（类似拼写纠错）并返回最佳建议。
      - 如果仍无结果，显示可用命令列表。
    """
    if not args:
        return "Usage: /update <project> <environment>\nExample: /update fpms prod"

    args_lower = args.strip().lower()
    
    # 1. 子串匹配（找出所有包含 args_lower 的命令）
    substring_matches = [cmd for cmd in VALID_COMMANDS if args_lower in cmd]
    
    if len(substring_matches) == 1:
        # 唯一匹配，直接返回
        matched = substring_matches[0]
        display, url = UPDATE_MAP[matched]
        return f"{display}\nLINK : {url}"
    
    elif len(substring_matches) > 1:
        # 多个匹配，列出所有
        lines = [f"Multiple matches for '{args}':"]
        for cmd in substring_matches:
            display, url = UPDATE_MAP[cmd]
            # 将多行 URL 中的换行符替换为空格，避免格式错乱
            url_display = url.replace('\n', ' / ')
            lines.append(f"• {display} : {url_display}")
        return "\n".join(lines)
    
    # 2. 没有子串匹配，尝试模糊匹配
    fuzzy_matches = difflib.get_close_matches(args_lower, VALID_COMMANDS, n=3, cutoff=0.6)
    if fuzzy_matches:
        lines = [f"Did you mean one of these?"]
        for cmd in fuzzy_matches:
            display, url = UPDATE_MAP[cmd]
            url_display = url.replace('\n', ' / ')
            lines.append(f"• {display} : {url_display}")
        return "\n".join(lines)
    
    # 3. 完全没找到，显示帮助
    available = ", ".join(VALID_COMMANDS)
    return f"Unknown update target '{args}'. Available: {available}"