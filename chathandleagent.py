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
2. Deterministic + pattern + offset rules  -> command or chat (no LLM)
3. Cheap chat fast-paths (math / pure small-talk) -> chat (skip an LLM call)
4. **LLM only when rules abstain** — ``commandagent.command_signal(allow_llm=True)``
5. Fallback (LLM unavailable): keyword heuristics → command, else chat

Everything is wrapped so it can never raise into the bot's hot path; on any
error it returns ``unknown`` and the caller keeps its previous behaviour.

Toggle: ``BOT_USE_CHATHANDLE=0`` to disable (router returns ``unknown`` always,
so the bot behaves like before).
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Optional

# Hard "this is work" signals — if any appears, it's almost never small talk.
_COMMAND_KEYWORDS_RE = re.compile(
    r"(?i)\b("
    r"duty|roster|on[\s-]?call|oncall|"
    r"fpms|pms|bi|fe|cpms|sre|dba|db|liveslot|ote|ft|"
    r"leave|wfh|work from home|holiday|offset|"
    r"deploy|build|update|git|pull|"
    r"machine|asset|egm|encoder|nch|nwr|winford|tbr|tbp|mdr|dhs|osm|"
    r"maintenance|maint|"
    r"checkcredit|credit|cctv|sms|otp|reminder|provider id|pid|"
    r"cashout|emergency contact"
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


def _command_signal(text: str, *, allow_llm: bool = False) -> dict:
    try:
        import commandagent

        return commandagent.command_signal(text, allow_llm=allow_llm)
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

        # 2) Rule-based command signal (deterministic + pattern + offset — no LLM).
        cmd_sig = _command_signal(raw, allow_llm=False)
        if cmd_sig.get("deterministic") and cmd_sig.get("command"):
            return RouteDecision(
                _CMD,
                reason=str(cmd_sig.get("source") or "deterministic"),
                command=cmd_sig["command"],
                command_conf=1.0,
            )
        if cmd_sig.get("route") == "command" and cmd_sig.get("command"):
            print(
                f"[chathandleagent] rules only → {cmd_sig.get('command')!r} "
                f"(tag={cmd_sig.get('tag')}, conf={float(cmd_sig.get('confidence') or 0):.2f}, no LLM)",
                flush=True,
            )
            return RouteDecision(
                _CMD,
                reason=f"rule_{cmd_sig.get('source') or 'signal'}",
                command=cmd_sig.get("command"),
                command_conf=float(cmd_sig.get("confidence") or 1.0),
            )
        if cmd_sig.get("route") == "chat":
            return RouteDecision(
                _CHAT,
                reason=f"rule_{cmd_sig.get('source') or 'signal'}",
                chat_conf=float(cmd_sig.get("confidence") or 0.9),
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

        # 3b) A resolvable machine token beats the LLM — the tiny prod model
        #     (qwen2.5:0.5b) misroutes bare ids like "TBR2099" to chat.
        if has_machine and not _looks_like_pure_chitchat(raw):
            resolved_pre = None
            try:
                import commandagent as _ca

                resolved_pre = _ca._resolve_command_for_route(raw, cmd_sig)
            except Exception:
                resolved_pre = None
            if resolved_pre:
                print(
                    f"[chathandleagent] machine token → {resolved_pre!r} (pre-LLM)",
                    flush=True,
                )
                return RouteDecision(
                    _CMD,
                    reason="machine_id",
                    command=resolved_pre,
                    command_conf=0.95,
                )

        # 4) LLM only when rules did not decide.
        llm_sig = _command_signal(raw, allow_llm=True)
        cmd_conf = float(llm_sig.get("confidence") or cmd_conf)
        cmd_route = llm_sig.get("route") or cmd_route
        if llm_sig.get("route") == "command" and llm_sig.get("command"):
            print(
                f"[chathandleagent] command LLM → {llm_sig.get('command')!r} "
                f"(tag={llm_sig.get('tag')}, conf={cmd_conf:.2f})",
                flush=True,
            )
            return RouteDecision(
                _CMD,
                reason=f"ai_{llm_sig.get('source') or 'signal'}",
                command=llm_sig.get("command"),
                command_conf=cmd_conf,
            )
        if llm_sig.get("route") == "chat":
            print(
                f"[chathandleagent] chat LLM (conf={cmd_conf:.2f}) — "
                f"downstream uses BOT_CHAT_MODEL",
                flush=True,
            )
            return RouteDecision(
                _CHAT,
                reason=f"ai_{llm_sig.get('source') or 'signal'}",
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
        f"[chathandleagent] enabled={is_enabled()} decision=rules-first-then-LLM",
        flush=True,
    )
    if is_enabled():
        try:
            t0 = time.perf_counter()
            route("who is on fpms duty today")
            ms = (time.perf_counter() - t0) * 1000
            print(f"[chathandleagent] route warmup done in {ms:.0f}ms", flush=True)
        except Exception as exc:
            print(f"[chathandleagent] route warmup skipped: {exc!r}", flush=True)


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
