"""
chathandleagent — the *router* that decides how to handle an incoming message.

Problem it solves
-----------------
Before this agent, the bot ran ``commandagent`` first and, if it returned
``None`` (low confidence / missing intent), the message silently fell through to
``chatagent`` — so a real work request (e.g. "nwr set maintenance") got a chatty
non-answer instead of running the command. There was no single place that
*decided* "is this a COMMAND or just CHAT?".

This module is that single place. It looks like a human triage desk:

    route(text) -> RouteDecision(kind = "command" | "chat" | "unknown", ...)

Decision strategy (high precision first, AI last)
-------------------------------------------------
1. Already a ``/slash`` command            -> command (let main.py handle it)
2. Deterministic prod-batch maintenance    -> command (e.g. set maintenance)
3. Cheap chat fast-paths (math / pure small-talk) -> chat (skip an LLM call)
4. **AI decides** — ``commandagent.command_signal`` resolves the message through
   pattern rules → a single direct LLM call that answers "is this a COMMAND or
   CHAT?" (and, for commands, which one). We simply trust that decision.
5. Fallback (only when the LLM is unavailable): strong work keywords / machine
   ids → command, otherwise chat.

There is no longer a local DistilBERT classifier or a chat-vs-command
confidence comparison — the language model makes the call directly.

Everything is wrapped so it can never raise into the bot's hot path; on any
error it returns ``unknown`` and the caller keeps its previous behaviour.

Toggle: ``BOT_USE_CHATHANDLE=0`` to disable (router returns ``unknown`` always,
so the bot behaves like before).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Optional

# Hard "this is work" signals — if any appears, it's almost never small talk.
_COMMAND_KEYWORDS_RE = re.compile(
    r"(?i)\b("
    r"duty|roster|on[\s-]?call|oncall|"
    r"fpms|pms|bi|fe|cpms|sre|dba|db|liveslot|ote|ft|"
    r"leave|wfh|work from home|holiday|offset|"
    r"jenkins|deploy|build|update|"
    r"machine|asset|egm|nch|nwr|winford|tbr|tbp|mdr|dhs|"
    r"maintenance|maint|"
    r"checkcredit|credit|cctv|sms|otp|reminder|provider id|pid|"
    r"cashout|p0|p1|emergency contact"
    r")\b"
)
# A machine id like NWR2113 / NCH1422 / a bare 3+ digit asset.
_MACHINE_ID_RE = re.compile(
    r"(?i)\b(?:nch|nwr|wf|win|winford|tbr|tbp|cp|osm|dhs|mdr)\s*-?\s*\d{2,}\b|\b\d{4,}\b"
)
# Search-ish phrasing ("who is David", "find Henry", "phone number for ...").
_SEARCH_RE = re.compile(
    r"(?i)\b(who is|find|look ?up|search|phone (?:number )?(?:for|of)|contact (?:for|of))\b"
)

_CMD = "command"
_CHAT = "chat"
_UNKNOWN = "unknown"


@dataclass
class RouteDecision:
    kind: str  # "command" | "chat" | "unknown"
    reason: str = ""
    command: Optional[str] = None  # pre-mapped slash command when known
    command_conf: float = 0.0
    chat_conf: float = 0.0

    @property
    def is_command(self) -> bool:
        return self.kind == _CMD

    @property
    def is_chat(self) -> bool:
        return self.kind == _CHAT


def is_enabled() -> bool:
    return (os.getenv("BOT_USE_CHATHANDLE") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _word_count(text: str) -> int:
    return len((text or "").split())


def _looks_like_pure_chitchat(text: str) -> bool:
    try:
        import chitchat

        return chitchat.looks_like_chitchat(text)
    except Exception:
        return False


def _command_signal(text: str) -> dict:
    try:
        import commandagent

        return commandagent.command_signal(text)
    except Exception:
        return {"tag": None, "confidence": 0.0, "margin": 0.0, "command": None, "deterministic": False}


def _looks_like_math(text: str) -> bool:
    try:
        import chatagent

        return chatagent.looks_like_math_question(text)
    except Exception:
        return False


def _looks_like_math_followup(text: str) -> bool:
    try:
        import chatagent

        return chatagent.looks_like_math_followup(text)
    except Exception:
        return False


def route(text: str, *, bot_mentioned: bool = True) -> RouteDecision:
    """Decide how to handle ``text``. Never raises."""
    raw = (text or "").strip()
    if not raw:
        return RouteDecision(_UNKNOWN, reason="empty")
    if not is_enabled():
        return RouteDecision(_UNKNOWN, reason="disabled")

    try:
        # 1) Explicit slash command — main.py handles it directly.
        if raw.lstrip().startswith("/"):
            return RouteDecision(_CMD, reason="slash", command=raw)

        # 1b) Missing Credit ops paste → checkcredit flow (not casual chat).
        if re.search(r"(?i)(?:type\s*:\s*)?missing\s+credit", raw):
            return RouteDecision(_CMD, reason="missing_credit", command="/checkcreditdate")

        # 2) Deterministic prod-batch maintenance (highest precision).
        cmd_sig = _command_signal(raw)
        if cmd_sig.get("deterministic") and cmd_sig.get("command"):
            return RouteDecision(
                _CMD, reason="prod_batch", command=cmd_sig["command"], command_conf=1.0
            )

        cmd_conf = float(cmd_sig.get("confidence") or 0.0)
        cmd_route = cmd_sig.get("route")

        has_kw = bool(_COMMAND_KEYWORDS_RE.search(raw))
        has_machine = bool(_MACHINE_ID_RE.search(raw))
        has_search = bool(_SEARCH_RE.search(raw))

        # 3) Cheap chat fast-paths — handle obvious math / small-talk without an
        #    LLM round-trip (and never hijack a real work request).
        if _looks_like_math(raw) and not has_kw and not has_search:
            return RouteDecision(_CHAT, reason="math", chat_conf=0.85)
        if _looks_like_math_followup(raw) and not has_kw and not has_search:
            return RouteDecision(_CHAT, reason="math_followup", chat_conf=0.8)

        # 4) AI decides. ``command_signal`` runs pattern rules → a direct LLM call
        #    that answers "COMMAND or CHAT?" (and which command). Trust it.
        if cmd_route == "command" and cmd_sig.get("command"):
            return RouteDecision(
                _CMD,
                reason=f"ai_{cmd_sig.get('source') or 'signal'}",
                command=cmd_sig.get("command"),
                command_conf=cmd_conf,
            )
        if cmd_route == "chat":
            return RouteDecision(
                _CHAT,
                reason=f"ai_{cmd_sig.get('source') or 'signal'}",
                chat_conf=cmd_conf,
            )

        # 5) Fallback (LLM unavailable / abstained): use cheap keyword heuristics.
        if _looks_like_pure_chitchat(raw) and _word_count(raw) <= 12:
            return RouteDecision(_CHAT, reason="chitchat")
        if (has_kw or has_machine or has_search) and not _looks_like_pure_chitchat(raw):
            resolved_cmd = cmd_sig.get("command")
            if not resolved_cmd:
                try:
                    import commandagent as _ca

                    resolved_cmd = _ca._resolve_command_for_route(raw, cmd_sig)
                except Exception:
                    resolved_cmd = None
            return RouteDecision(
                _CMD,
                reason="keyword" if has_kw else ("machine_id" if has_machine else "search"),
                command=resolved_cmd,
                command_conf=cmd_conf,
            )

        # No work signal and no command — treat as chat (not unknown).
        return RouteDecision(_CHAT, reason="no_work_signal", chat_conf=0.4)
    except Exception as exc:  # pragma: no cover - safety net
        print(f"⚠️ chathandleagent route error: {exc!r}", flush=True)
        return RouteDecision(_UNKNOWN, reason="error")


def classify(text: str, *, bot_mentioned: bool = True) -> str:
    """Convenience wrapper returning just the kind string."""
    return route(text, bot_mentioned=bot_mentioned).kind


def startup_status() -> None:
    print(
        f"[chathandleagent] enabled={is_enabled()} decision=direct-LLM (commandagent)",
        flush=True,
    )


def _cli(text: str) -> None:
    d = route(text)
    print(f"Input:    {text!r}")
    print(f"Route:    {d.kind}   (reason={d.reason})")
    print(f"Command:  {d.command!r}")
    print(f"cmd_conf={d.command_conf:.3f}  chat_conf={d.chat_conf:.3f}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        _cli(" ".join(sys.argv[1:]))
    else:
        print('Usage: python chathandleagent.py "your message here"')
