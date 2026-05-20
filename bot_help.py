"""
Bot command catalogue for Lark /help.

Keep in sync with handlers in main.py (and offsetleave / jenkinsupdate flows).
"""

from __future__ import annotations

from typing import Callable, Optional

# (command, description_en, description_zh)
CommandRow = tuple[str, str, str]

HELP_HEADER = (
    "📖 <b>Duty Bot — command list</b>\n"
    "In <b>group chats</b>, @mention the bot first, then type the command.\n"
    "群聊请先 <b>@ 机器人</b>，再输入命令。\n"
    "Use <code>/help jenkins</code> for Jenkins job keywords.\n"
)


def _rows(*items: CommandRow) -> list[str]:
    lines: list[str] = []
    for cmd, en, zh in items:
        lines.append(f"• <code>{cmd}</code> — {en} / {zh}")
    return lines


def _section(title: str, rows: list[CommandRow]) -> str:
    return f"\n<b>{title}</b>\n" + "\n".join(_rows(*rows))


def _jenkins_keyword_lines() -> list[str]:
    try:
        from jenkinsupdate import JENKINS_UPDATE_JOB_REGISTRY

        keys = sorted({k.strip() for k in JENKINS_UPDATE_JOB_REGISTRY if k.strip()})
    except Exception:
        keys = []
    if not keys:
        return ["(Jenkins module not loaded on this server)"]
    lines = ["Keywords (use after <code>/jenkinsupdate</code>):"]
    chunk: list[str] = []
    for i, k in enumerate(keys, 1):
        chunk.append(k)
        if i % 8 == 0 or i == len(keys):
            lines.append("  " + " · ".join(chunk))
            chunk = []
    return lines


def build_help_sections(
    *,
    jenkins_available: bool = True,
) -> list[str]:
    """Return help body chunks (each under ~4000 chars for Lark)."""
    sections: list[str] = []

    sections.append(
        _section(
            "General | 通用",
            [
                ("/help", "Show this command list", "显示命令列表"),
                ("/help jenkins", "List /jenkinsupdate job keywords", "列出 Jenkins 任务关键字"),
                ("/s &lt;name&gt;", "Search duty roster by name", "按姓名查值班表"),
                ("/date", "Today's date", "今天日期"),
                ("/holiday", "Upcoming public holidays", "即将到来的公共假期"),
                ("/holidaythismonth", "Holidays this month", "本月假期"),
                ("/test", "Send test interactive card", "发送测试卡片"),
                ("/restart", "Restart the bot process", "重启机器人进程"),
            ],
        )
    )

    sections.append(
        _section(
            "Department duty | 部门值班",
            [
                ("/fpms", "FPMS duty (today)", "FPMS 今日值班"),
                ("/fpmscheck [MM/YYYY]", "FPMS missing-duty report", "FPMS 缺勤检查"),
                ("/fpmsp0", "FPMS P0 duty contacts", "FPMS P0 值班联系"),
                ("/pms", "PMS duty (next days)", "PMS 值班"),
                ("/pmscheck [MM/YYYY]", "PMS missing-duty report", "PMS 缺勤检查"),
                ("/bi", "BI duty (today)", "BI 今日值班"),
                ("/bicheck [MM/YYYY]", "BI missing-duty report", "BI 缺勤检查"),
                ("/fe", "FE duty (next 3 days)", "FE 近三天值班"),
                ("/fecheck [MM/YYYY]", "FE missing-duty report", "FE 缺勤检查"),
                ("/cpms", "CPMS duty (3 days)", "CPMS 三天值班"),
                ("/cpmscheck [MM/YYYY]", "CPMS missing-duty report", "CPMS 缺勤检查"),
                ("/sre", "SRE duty (this & next week)", "SRE 本周与下周值班"),
                ("/srecheck [MM/YYYY]", "SRE missing-duty report", "SRE 缺勤检查"),
                ("/db", "DB duty (3 weeks)", "DB 三周值班"),
                ("/dbcheck [MM/YYYY]", "DB missing-duty report", "DB 缺勤检查"),
                ("/liveslot", "Liveslot duty (3 weeks)", "Liveslot 三周值班"),
                ("/liveslotcheck [MM/YYYY]", "Liveslot missing-duty report", "Liveslot 缺勤检查"),
                ("/ote", "OTE duty (3 weeks)", "OTE 三周值班"),
                ("/otecheck [MM/YYYY]", "OTE missing-duty report", "OTE 缺勤检查"),
                ("/ft", "FT duty (3 days) + contact FYI", "FT 三天值班与联系提示"),
                ("/ftcheck [MM/YYYY]", "FT missing-duty report", "FT 缺勤检查"),
                ("/ose", "OSE duty card (now)", "OSE 当前值班卡片"),
                ("/osedate DD/MM/YYYY", "OSE duty for a date", "指定日期 OSE 值班"),
                ("/dutycheckall [MM/YYYY]", "All departments missing-duty", "全部部门缺勤检查"),
                ("/ecsre [&lt;game&gt;]", "EC SRE game ownership", "EC SRE 游戏负责人"),
                ("/ec [&lt;game&gt;]", "Emergency contacts card", "紧急联系人卡片"),
                ("/otpp0", "OTP P0 guide", "OTP P0 指引"),
            ],
        )
    )

    sections.append(
        _section(
            "OSE offset / leave | OSE 调休请假",
            [
                (
                    "showoffset [month]",
                    "Monthly offset calendar card (no slash)",
                    "月度调休日历卡片（无斜杠）",
                ),
                (
                    "@bot offset",
                    "Open offset request form (private card)",
                    "打开调休申请表（私聊卡片）",
                ),
                (
                    "@bot leave",
                    "Open leave request form (private card)",
                    "打开请假申请表（私聊卡片）",
                ),
                (
                    "editoffset",
                    "Edit your pending offset (private)",
                    "编辑本人待审批调休",
                ),
                (
                    "deleteoffset",
                    "Delete offset records (private)",
                    "删除调休记录",
                ),
                (
                    "pendingoffset",
                    "Approver: pending offset queue",
                    "审批人：待审调休列表",
                ),
            ],
        )
    )

    sections.append(
        _section(
            "Machines & assets | 机台与资产",
            [
                ("/nch &lt;id&gt;", "NCH machine info", "NCH 机台信息"),
                ("/nwr &lt;id&gt;", "NWR machine info", "NWR 机台信息"),
                ("/wf &lt;id&gt;", "Winford asset info", "Winford 资产信息"),
                ("/tbp &lt;id&gt;", "TBP machine info", "TBP 机台信息"),
                ("/cp &lt;id&gt;", "CP asset info", "CP 资产信息"),
                ("/dhs &lt;id&gt;", "DHS asset info", "DHS 资产信息"),
                ("/mdr &lt;id&gt;", "MDR asset info", "MDR 资产信息"),
                ("/pid &lt;id&gt;", "Provider ID lookup", "Provider ID 查询"),
                ("/cctv &lt;machine&gt;", "EGM CCTV screenshot (async)", "EGM 监控截图（异步）"),
            ],
        )
    )

    sections.append(
        _section(
            "Logs & credit | 日志与额度",
            [
                (
                    "/checkcreditdate",
                    "Interactive card: machine + player + date",
                    "交互卡片：机台+玩家+日期",
                ),
                (
                    "/checkcredit &lt;machine&gt; [YYYY-MM-DD]",
                    "Credit / log check (today if no date)",
                    "额度/日志检查（默认今天）",
                ),
                (
                    "/machineerror &lt;machine&gt; [YYYY-MM-DD]",
                    "Latest players with errors only",
                    "仅显示有错误的玩家",
                ),
                (
                    "/npthirdhttp …",
                    "NP Third HTTP detail (async)",
                    "NP Third HTTP 详情（异步）",
                ),
                ("/al [DD/MM]", "Amount Loss CHECKLOG (async)", "Amount Loss 检查（异步）"),
                (
                    "/smsfail",
                    "SMS gateway OTP failure check (async)",
                    "短信网关 OTP 失败检查（异步）",
                ),
                (
                    "/smscheckplayer &lt;id&gt;",
                    "SMS OTP logs for player(s) today (async)",
                    "玩家今日 SMS OTP 日志（异步）",
                ),
            ],
        )
    )

    if jenkins_available:
        sections.append(
            _section(
                "Jenkins | 构建",
                [
                    (
                        "/jenkinsupdate &lt;keyword&gt; …",
                        "Match job → confirm card → trigger build",
                        "匹配任务 → 确认卡片 → 触发构建",
                    ),
                    (
                        "/updatejenkins …",
                        "Alias of /jenkinsupdate",
                        "/jenkinsupdate 别名",
                    ),
                ],
            )
        )
    else:
        sections.append(
            "\n<b>Jenkins | 构建</b>\n"
            "• <code>/jenkinsupdate</code> — unavailable (module not loaded) / 不可用\n"
        )

    sections.append(
        _section(
            "Maintenance & ops | 运维",
            [
                (
                    "/m &lt;email&gt; | /maintenance …",
                    "Parse maintenance email → replies",
                    "解析维护邮件并回复",
                ),
                ("/update &lt;args&gt;", "Server update helper", "服务器更新助手"),
                ("/cashout", "Cashout reminder template", "出款提醒模板"),
                ("/restartA", "Show Pi restart shell one-liner", "显示 Pi 重启命令"),
            ],
        )
    )

    sections.append(
        _section(
            "Reminders | 提醒",
            [
                ("/reminder &lt;time&gt; &lt;msg&gt;", "Schedule a one-off reminder", "设置一次性提醒"),
                (
                    "/addreminder …",
                    "Add sheet reminder (or open form card)",
                    "添加表格提醒（或打开表单）",
                ),
                (
                    "/deletereminder [id…]",
                    "Delete reminders (or list card)",
                    "删除提醒（或列表卡片）",
                ),
                ("/cancelp1", "Cancel active P1 escalation reminder", "取消 P1 升级提醒"),
            ],
        )
    )

    sections.append(
        _section(
            "Fun & admin | 趣味与管理",
            [
                ("/miao", "Random miao", "随机喵"),
                ("/lucifer", "Lucifer quote", "Lucifer"),
                ("/dog", "Dog meme", "狗狗"),
                ("/picture cat", "Send cat picture", "发送猫咪图片"),
                ("/memorytest", "Number memory mini-game", "数字记忆小游戏"),
                ("/secret1 @user", "Resolve mention open_id", "解析 @ 用户 open_id"),
                ("/secret2", "Show chat_id (auto-recall)", "显示 chat_id（自动撤回）"),
            ],
        )
    )

    sections.append(
        "\n<b>Special (no slash) | 特殊</b>\n"
        "• P0/P1: reply <b>yes</b> / <b>no</b> when bot asks in lab/OSE groups / "
        "实验室或 OSE 群确认 P0/P1 时回复 yes/no\n"
        "• After <code>/checkcreditdate</code> NP prompt, reply <b>1</b>–<b>4</b> "
        "(no @ needed) / NP 选项回复 1–4（无需 @）\n"
        "• Active <code>/jenkinsupdate</code> session: follow bot prompts / "
        "Jenkins 会话中按机器人提示操作\n"
    )

    return sections


def _split_messages(header: str, sections: list[str], max_len: int = 3800) -> list[str]:
    """Pack sections into one or more Lark messages."""
    parts: list[str] = []
    current = header
    for block in sections:
        candidate = current + block
        if len(candidate) <= max_len:
            current = candidate
            continue
        if current.strip():
            parts.append(current.rstrip())
        current = block
        if len(current) > max_len:
            # Very long single block — hard split by lines
            lines = block.split("\n")
            buf = ""
            for line in lines:
                if len(buf) + len(line) + 1 > max_len:
                    if buf.strip():
                        parts.append(buf.rstrip())
                    buf = line + "\n"
                else:
                    buf += line + "\n"
            current = ""
            if buf.strip():
                current = buf
    if current.strip():
        parts.append(current.rstrip())
    return parts or [header.rstrip()]


def format_help(
    topic: Optional[str] = None,
    *,
    jenkins_available: bool = True,
) -> list[str]:
    """
    Return one or more message bodies for Lark.
    topic: None | 'jenkins'
    """
    t = (topic or "").strip().lower()
    if t in ("jenkins", "jenkinsupdate", "ju"):
        lines = [
            "📖 <b>/jenkinsupdate keywords</b>\n",
            "Usage: <code>@Bot /jenkinsupdate &lt;keyword&gt;</code> "
            "(then confirm on the card).\n",
            "用法：<code>@机器人 /jenkinsupdate 关键字</code>（再在卡片上确认）。\n",
        ]
        lines.extend(_jenkins_keyword_lines())
        body = "\n".join(lines)
        if len(body) > 3800:
            return _split_messages("", [body])
        return [body]

    sections = build_help_sections(jenkins_available=jenkins_available)
    return _split_messages(HELP_HEADER, sections)


def handle_help_command(
    clean_text: str,
    *,
    chat_id: str,
    send_message: Callable[..., dict],
    jenkins_available: bool = True,
) -> bool:
    """Handle /help and /commands. Returns True if handled."""
    raw = (clean_text or "").strip()
    low = raw.lower()
    if low not in ("/help", "/commands", "/command") and not low.startswith("/help "):
        return False
    topic = None
    if low.startswith("/help "):
        topic = raw.split(maxsplit=1)[1].strip()
    messages = format_help(topic, jenkins_available=jenkins_available)
    for i, body in enumerate(messages):
        if i:
            body = f"📖 <b>(continued {i + 1}/{len(messages)})</b>\n" + body
        send_message(chat_id, body)
    return True
