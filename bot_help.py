"""
Bot command catalogue for Lark /help (interactive message cards).

Keep in sync with handlers in main.py (and offsetleave / jenkinsupdate flows).
"""

from __future__ import annotations

import json
from typing import Any, Callable, Optional

# (command, description_en, description_zh)
CommandRow = tuple[str, str, str]

# (section_key, title, emoji, header_template, rows)
HelpSection = tuple[str, str, str, str, list[CommandRow]]

_SECTION_ICONS = {
    "general": "🧭",
    "duty": "👥",
    "ose": "📅",
    "machines": "🎰",
    "logs": "📊",
    "jenkins": "🔧",
    "ops": "⚙️",
    "reminders": "⏰",
}


def _help_sections(*, jenkins_available: bool) -> list[HelpSection]:
    sections: list[HelpSection] = [
        (
            "general",
            "General",
            _SECTION_ICONS["general"],
            "blue",
            [
                ("/help", "Command list (this card)", "命令列表（本卡片）"),
                ("/help jenkins", "Jenkins job keywords card", "Jenkins 关键字卡片"),
                ("/s <name>", "Search duty roster", "按姓名查值班表"),
                ("/date", "Today's date", "今天日期"),
                ("/holiday", "Upcoming public holidays", "即将到来的公共假期"),
                ("/holidaythismonth", "Holidays this month", "本月假期"),
                ("/leave", "@bot — who is on leave today (HRMS calendar)", "@机器人 — 今日谁请假（HRMS 日历）"),
                (
                    "/wholeave",
                    "@bot — who is on leave today (leave sheet)",
                    "@机器人 — 今日谁请假（请假表）",
                ),
                ("/restart", "Restart bot process", "重启机器人"),
            ],
        ),
        (
            "duty",
            "Department duty",
            _SECTION_ICONS["duty"],
            "wathet",
            [
                ("/fpms", "FPMS duty (today)", "FPMS 今日值班"),
                ("/fpmscheck [MM/YYYY]", "FPMS missing-duty report", "FPMS 缺勤检查"),
                ("/fpmsp0", "FPMS P0 contacts", "FPMS P0 联系"),
                ("/pms", "PMS duty (next days)", "PMS 值班"),
                ("/pmscheck [MM/YYYY]", "PMS missing-duty report", "PMS 缺勤检查"),
                ("/bi", "BI duty (today)", "BI 今日值班"),
                ("/bicheck [MM/YYYY]", "BI missing-duty report", "BI 缺勤检查"),
                ("/fe", "FE duty (next 3 days)", "FE 近三天值班"),
                ("/fecheck [MM/YYYY]", "FE missing-duty report", "FE 缺勤检查"),
                ("/cpms", "CPMS duty (3 days)", "CPMS 三天值班"),
                ("/cpmscheck [MM/YYYY]", "CPMS missing-duty report", "CPMS 缺勤检查"),
                ("/sre", "SRE this & next week", "SRE 本周与下周"),
                ("/srecheck [MM/YYYY]", "SRE missing-duty report", "SRE 缺勤检查"),
                ("/db", "DB duty (3 weeks); alias /dba", "DB 三周值班（别名 /dba）"),
                ("/dbcheck [MM/YYYY]", "DB missing-duty report", "DB 缺勤检查"),
                ("/liveslot", "Liveslot (3 weeks)", "Liveslot 三周值班"),
                ("/liveslotcheck [MM/YYYY]", "Liveslot missing-duty", "Liveslot 缺勤检查"),
                ("/ote", "OTE (3 weeks)", "OTE 三周值班"),
                ("/otecheck [MM/YYYY]", "OTE missing-duty", "OTE 缺勤检查"),
                ("/ft", "FT 3 days + contact FYI", "FT 三天值班与联系提示"),
                ("/ftcheck [MM/YYYY]", "FT missing-duty", "FT 缺勤检查"),
                ("/ose", "OSE duty card (now)", "OSE 当前值班"),
                ("/osedate DD/MM/YYYY", "OSE duty on date", "指定日期 OSE 值班"),
                ("/dutycheckall [MM/YYYY]", "All depts missing-duty", "全部部门缺勤检查"),
                ("/ecsre [<game>]", "EC SRE game owner", "EC SRE 游戏负责人"),
                ("/ec [<game>]", "Emergency contacts", "紧急联系人"),
                ("/otpp0", "OTP P0 guide", "OTP P0 指引"),
            ],
        ),
        (
            "ose",
            "OSE offset & leave",
            _SECTION_ICONS["ose"],
            "green",
            [
                ("showoffset [month]", "Monthly offset calendar", "月度调休日历"),
                ("@bot offset", "Offset request form", "调休申请表"),
                ("@bot leave", "Leave request form", "请假申请表"),
                ("editoffset", "Edit pending offset", "编辑待审调休"),
                ("deleteoffset", "Delete offset records", "删除调休记录"),
                ("pendingoffset", "Approver pending queue", "审批人待审列表"),
            ],
        ),
        (
            "machines",
            "Machines & assets",
            _SECTION_ICONS["machines"],
            "orange",
            [
                ("/nch <id>", "NCH machine info", "NCH 机台"),
                ("/nwr <id>", "NWR machine info", "NWR 机台"),
                ("/wf <id>", "Winford asset", "Winford 资产"),
                ("/tbp <id>", "TBP machine", "TBP 机台"),
                ("/cp <id>", "CP asset", "CP 资产"),
                ("/dhs <id>", "DHS asset", "DHS 资产"),
                ("/mdr <id>", "MDR asset", "MDR 资产"),
                ("/pid <id>", "Provider ID lookup", "Provider ID"),
                ("/cctv <machine>", "EGM CCTV screenshot", "EGM 监控截图"),
            ],
        ),
        (
            "logs",
            "Logs & credit",
            _SECTION_ICONS["logs"],
            "purple",
            [
                ("/checkcreditdate", "Card: machine + player + date", "卡片：机台+玩家+日期"),
                ("/checkcredit <machine> [date]", "Credit / log check", "额度/日志检查"),
                ("/machineerror <machine> [date]", "Players with errors only", "仅有错误的玩家"),
                ("/npthirdhttp …", "NP Third HTTP (async)", "NP Third HTTP"),
                ("/al [DD/MM]", "Amount Loss CHECKLOG", "Amount Loss 检查"),
                ("/smsfail", "SMS OTP failure check", "短信 OTP 失败检查"),
                ("/smscheckplayer <id>", "SMS OTP logs today", "玩家 SMS OTP 日志"),
            ],
        ),
    ]

    if jenkins_available:
        sections.append(
            (
                "jenkins",
                "Jenkins",
                _SECTION_ICONS["jenkins"],
                "indigo",
                [
                    ("/update <keyword>", "Match job → confirm → build", "匹配任务→确认→构建"),
                    ("/updatemore …", "Multiple updates (same / not same env)", "批量更新"),
                    ("/jenkinsupdate …", "Alias of /update", "同上（别名）"),
                    ("/updatejenkins …", "Alias of /update", "同上（别名）"),
                ],
            )
        )

    sections.extend(
        [
            (
                "ops",
                "Maintenance & ops",
                _SECTION_ICONS["ops"],
                "grey",
                [
                    ("/m <email> | /maintenance …", "Parse maintenance email", "解析维护邮件"),
                    ("/cashout", "Cashout reminder template", "出款提醒模板"),
                    ("/restartA", "Pi restart one-liner", "Pi 重启命令"),
                ],
            ),
            (
                "reminders",
                "Reminders",
                _SECTION_ICONS["reminders"],
                "yellow",
                [
                    ("/reminder <time> <msg>", "One-off reminder", "一次性提醒"),
                    ("/addreminder …", "Sheet reminder / form", "表格提醒/表单"),
                    ("/deletereminder [id]", "Delete / list reminders", "删除/列表提醒"),
                    ("/cancelp1", "Cancel P1 escalation timer", "取消 P1 升级提醒"),
                ],
            ),
        ]
    )
    return sections


def _section_markdown(title: str, emoji: str, rows: list[CommandRow]) -> str:
    lines = [f"{emoji} **{title}**"]
    for cmd, en, zh in rows:
        lines.append(f"▸ `{cmd}` — {en} · {zh}")
    return "\n".join(lines)


def _card_shell(
    *,
    title: str,
    template: str,
    elements: list[dict[str, Any]],
    subtitle: str = "",
) -> dict[str, Any]:
    body_els: list[dict[str, Any]] = []
    if subtitle:
        body_els.append(
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": subtitle},
            }
        )
        body_els.append({"tag": "hr"})
    body_els.extend(elements)
    while body_els and body_els[-1].get("tag") == "hr":
        body_els.pop()
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": title},
        },
        "body": {"elements": body_els},
    }


def _pack_sections_into_cards(sections: list[HelpSection]) -> list[dict[str, Any]]:
    """
    One help card with every section.

    Sending multiple interactive cards in a row often leaves only the last message
    visible in Lark, so we keep the full list on a single card (~6KB JSON).
    """
    intro = (
        "**Group chats:** @mention the bot, then type a command.\n"
        "**群聊：** 先 **@ 机器人**，再输入命令。\n"
        "Tip: `/help jenkins` for build keywords · `/help jenkins` 查看构建关键字"
    )

    elements: list[dict[str, Any]] = []
    for _key, title, emoji, _tpl, rows in sections:
        elements.append(
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": _section_markdown(title, emoji, rows),
                },
            }
        )
        elements.append({"tag": "hr"})

    return [
        _card_shell(
            title="Duty Bot — Commands",
            template="blue",
            subtitle=intro,
            elements=elements,
        )
    ]


def _jenkins_keywords() -> list[str]:
    try:
        from jenkinsupdate import JENKINS_UPDATE_JOB_REGISTRY

        return sorted({k.strip() for k in JENKINS_UPDATE_JOB_REGISTRY if k.strip()})
    except Exception:
        return []


def build_jenkins_help_card() -> dict[str, Any]:
    keys = _jenkins_keywords()
    if keys:
        rows_md: list[str] = []
        for i in range(0, len(keys), 4):
            row = keys[i : i + 4]
            rows_md.append(" · ".join(f"`{k}`" for k in row))
        body = (
            "**Usage** · `@Bot /update <keyword>` → confirm on card\n"
            "**用法** · `@机器人 /update 关键字` → 卡片确认\n\n"
            + "\n".join(rows_md)
        )
    else:
        body = "_Jenkins module not loaded on this server._"

    return _card_shell(
        title="Jenkins — job keywords",
        template="blue",
        subtitle="",
        elements=[
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": body},
            }
        ],
    )


def build_help_cards(*, jenkins_available: bool = True) -> list[dict[str, Any]]:
    sections = _help_sections(jenkins_available=jenkins_available)
    return _pack_sections_into_cards(sections)


def build_help_plain_text(*, jenkins_available: bool = True) -> str:
    """Fallback when the interactive card API rejects the payload."""
    lines = [
        "📖 Duty Bot — commands",
        "Group: @mention bot first · 群聊请先 @ 机器人",
        "",
    ]
    for _key, title, emoji, _tpl, rows in _help_sections(jenkins_available=jenkins_available):
        lines.append(_section_markdown(title, emoji, rows))
        lines.append("")
    return "\n".join(lines).strip()


def _send_help_card(
    chat_id: str,
    card: dict[str, Any],
    send_message: Callable[..., dict],
    *,
    plain_fallback: str,
) -> None:
    payload = json.dumps(card, ensure_ascii=False)
    try:
        resp = send_message(chat_id, payload, msg_type="interactive")
    except Exception as exc:
        print(f"[help] send_message raised: {exc!r}", flush=True)
        send_message(chat_id, plain_fallback)
        return
    if not isinstance(resp, dict) or resp.get("code") != 0:
        print(f"[help] interactive card rejected: {resp!r}", flush=True)
        send_message(chat_id, plain_fallback)
        return
    print(f"[help] interactive card sent chat_id={chat_id}", flush=True)


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

    topic = ""
    if low.startswith("/help "):
        topic = raw.split(maxsplit=1)[1].strip().lower()

    fallback = build_help_plain_text(jenkins_available=jenkins_available)

    if topic in ("jenkins", "jenkinsupdate", "ju"):
        _send_help_card(chat_id, build_jenkins_help_card(), send_message, plain_fallback=fallback)
        return True

    _send_help_card(
        chat_id,
        build_help_cards(jenkins_available=jenkins_available)[0],
        send_message,
        plain_fallback=fallback,
    )
    return True
