"""
Casual small-talk replies for @Duty Bot (闲聊).

Only fires when no slash command matched and the message looks like chitchat,
not a duty/machine/Jenkins request. Toggle: ``BOT_USE_CHITCHAT=0`` to disable.
"""

from __future__ import annotations

import os
import random
import re
from typing import Optional

# Skip when the user probably wanted a bot command, not chitchat.
_COMMANDISH_RE = re.compile(
    r"(?i)\b("
    r"duty|fpms|bi|sre|db|fe|cpms|pms|ote|leave|wfh|holiday|jenkins|"
    r"machine|asset|nch|nwr|winford|checkcredit|offset|reminder|help|"
    r"fpms|wholeave|cctv|sms|credit"
    r")\b|/"
)

_CJK_RE = re.compile(r"[\u4e00-\u9fff]")

_CHITCHAT_RULES: list[tuple[re.Pattern[str], list[str]]] = [
    (
        re.compile(
            r"^(?:"
            r"hi+|hello+|hey+|hiya+|yo+|"
            r"good\s+(?:morning|afternoon|evening|night)|"
            r"gm|morning|"
            r"你好|您好|嗨|哈啰|哈喽"
            r")\s*[!.?~]*$",
            re.I,
        ),
        [
            "Hi! 👋 How can I help? Try `/help` or ask in English like “who is on fpms duty today”.",
            "Hello! I'm Duty Bot — duty roster, leave, machines, and more. Say `/help` for commands.",
            "Hey there! 👋 Need duty info? e.g. `/fpms` or “show me bi duty”.",
        ],
    ),
    (
        re.compile(
            r"^(?:"
            r"how\s+are\s+you|how\s+r\s+u|how\s+are\s+u|"
            r"how\s+is\s+it\s+going|how\s+are\s+things|"
            r"what(?:'s|\s+is)\s+up|whats\s+up|sup|"
            r"wassup|you\s+good|"
            r"你好吗|怎么样|还好吗"
            r")(?:\s+\w+){0,4}\s*[!.?~]*$",
            re.I,
        ),
        [
            "I'm good, thanks! Ready to help with duty / leave / machines. What do you need?",
            "All good here 🤖 Ask me anything work-related — or `/help` for the command list.",
            "Doing well! Tell me what you need: duty roster, holidays, machine lookup, etc.",
        ],
    ),
    (
        re.compile(
            r"^(?:"
            r"thanks?(?:\s+a\s+lot|\s+you|\s+so\s+much)?|thank\s+you|thx|ty|cheers|"
            r"谢谢|多谢|感谢"
            r")\s*[!.?~]*$",
            re.I,
        ),
        [
            "You're welcome! 😊",
            "Anytime — ping me again if you need anything.",
            "Happy to help!",
        ],
    ),
    (
        re.compile(
            r"^(?:"
            r"bye+|goodbye|see\s+you|see\s+ya|cya|later|good\s+night|gn|"
            r"再见|拜拜|晚安"
            r")\s*[!.?~]*$",
            re.I,
        ),
        [
            "Bye! 👋 Have a good one.",
            "See you later!",
            "Goodbye — I'll be here when you need duty info.",
        ],
    ),
    (
        re.compile(
            r"^(?:"
            r"who\s+are\s+you|what\s+are\s+you|what\s+can\s+you\s+do|"
            r"are\s+you\s+a\s+bot|"
            r"你是谁|你能做什么|你是什么"
            r")\s*[!.?~]*$",
            re.I,
        ),
        [
            "I'm **Duty Bot** — FPMS/BI/SRE duty, leave/WFH, holidays, machine lookup, Jenkins helpers, and more. Try `/help`.",
            "Duty Bot at your service 🤖 Slash commands like `/fpms`, `/bi`, or natural English when AI is on. `/help` lists everything.",
        ],
    ),
    (
        re.compile(
            r"^(?:"
            r"nice|cool|awesome|great\s+job|well\s+done|good\s+bot|"
            r"厉害|不错|棒"
            r")\s*[!.?~]*$",
            re.I,
        ),
        [
            "Thanks! 😄 Let me know if you need anything else.",
            "Glad I could help!",
        ],
    ),
    (
        re.compile(
            r"^(?:"
            r"(?:hi+|hello+|hey+|hiya+|yo+|good\s+(?:morning|afternoon|evening))\s+)?"
            r"(?:how\s+are\s+you|how\s+r\s+u|how\s+are\s+u|how\s+is\s+it\s+going|"
            r"how\s+are\s+things|what(?:'s|\s+is)\s+up|whats\s+up|sup|wassup|you\s+good)"
            r"(?:\s+\w+){0,4}\s*[!.?~]*$",
            re.I,
        ),
        [
            "I'm doing great, thanks for asking! 🤖 What can I help you with?",
            "All good here — ready when you need duty info or machine lookup.",
            "Pretty good! Ask me anything work-related or just say hi anytime.",
        ],
    ),
    (
        re.compile(
            r"^(?:"
            r"i'?m\s+bored|im\s+bored|i'?m\s+boring|im\s+boring|"
            r"so\s+bored|feel(?:ing)?\s+bored|bored(?:\s+today)?|"
            r"nothing\s+to\s+do|kill\s+time|"
            r"好无聊|无聊"
            r")\s*[!.?~]*$",
            re.I,
        ),
        [
            "Bored? 😄 I can't stream Netflix — but try `/fpms` or ask who's on duty today.",
            "Hang in there! Want a distraction? Ask me about holidays (`/holiday`) or who's on call.",
            "I feel you — slow day? I'm here if you want to chat or need a quick duty lookup.",
        ],
    ),
]


def is_enabled() -> bool:
    return (os.getenv("BOT_USE_CHITCHAT") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _normalize(text: str) -> str:
    t = (text or "").strip()
    t = re.sub(r"\s+", " ", t)
    return t


def pick_localized_reply(replies: list[str], user_text: str) -> str:
    """Pick an English-only reply (Duty Bot does not reply in Chinese)."""
    english = [r for r in replies if not _CJK_RE.search(r)]
    pool = english or replies
    return random.choice(pool)


def looks_like_chitchat(text: str) -> bool:
    """True when the message is casual chat, not a duty/command request."""
    raw = _normalize(text)
    if not raw or raw.startswith("/"):
        return False
    if _COMMANDISH_RE.search(raw):
        return False
    if len(raw.split()) > 12:
        return False
    for pattern, _replies in _CHITCHAT_RULES:
        if pattern.search(raw):
            return True
    return False


def try_reply(text: str) -> Optional[str]:
    """
    Return a casual reply for small talk, or ``None`` if not chitchat / disabled.
    """
    if not is_enabled():
        return None
    raw = _normalize(text)
    if not raw or raw.startswith("/"):
        return None
    if _COMMANDISH_RE.search(raw):
        return None
    # Long messages are usually requests, not pure chitchat.
    if len(raw.split()) > 8:
        return None
    for pattern, replies in _CHITCHAT_RULES:
        if pattern.search(raw):
            return pick_localized_reply(replies, raw)
    return None
