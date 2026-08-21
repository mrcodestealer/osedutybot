"""
commandsuggest — command-first "did you mean" suggestions for Duty Bot.

Problem it solves
-----------------
When the routing pipeline (chathandleagent → commandagent → the main.py elif
chain) fails to map a message, the bot used to fall straight to the chat LLM,
which gives useless "could you clarify?" replies (prod model is tiny). Users
want the bot to *think in commands first* and, when unsure, ASK BACK with
tappable command buttons instead of chatting.

What it provides
----------------
  suggest_commands(text)        ranked [{command, usage, desc_en, desc_zh, score}]
  suggest_for_slash_typo(text)  difflib over known slash tokens ("/fpm" → "/fpms")
  send_suggestion_card(...)     builds + sends the Lark 卡片 2.0 with buttons

Buttons carry ``{"k": "cmd_run", "c": "<command>"}`` — main.py's card worker
re-injects ``c`` through the FULL normal message pipeline
(:func:`main._reinject_synthetic_command_message`), so tapping a button behaves
exactly like typing that command. A "💬 chat" button carries ``{"k": "cmd_chat"}``;
the original message is held in a pending store so the chat LLM can still
answer it on tap.

The registry is derived live from ``bot_help``'s catalogue (single source of
display truth) plus curated zh/en trigger aliases below.

Toggle: ``BOT_COMMAND_SUGGEST=0`` disables (callers keep the old behaviour).
"""

from __future__ import annotations

import difflib
import json
import os
import re
import threading
import time
from typing import Any, Callable, Optional

_CJK_RE = re.compile(r"[一-鿿]")

# Admin/dangerous commands are never suggested (secret-gated or destructive).
_NEVER_SUGGEST = {
    "/restart",
    "/restarta",
    "/deploy",
    "/gitpullrestart",
    "/restartservices",
    "/restservices",
    "/secret1",
    "/secret2",
    "/cashout",
    "/test",
    "/memorytest",
}

# Curated NL trigger aliases per command base (lowercase; CJK matched as
# substring, latin as whole word). Keep aliases SPECIFIC — generic words like
# "check" alone would fire on everything.
_EXTRA_ALIASES: dict[str, tuple[str, ...]] = {
    "/help": ("help", "commands", "指令", "帮助", "怎么用"),
    "/s": ("search duty", "查人", "找人", "duty search"),
    "/date": ("today date", "日期",),
    "/holiday": ("holiday", "假期", "节日", "公共假期"),
    "/leave": ("leave", "请假"),
    "/wfh": ("wfh", "work from home", "在家办公", "居家"),
    "/leavewfh": ("leave wfh", "请假 wfh"),
    "/wholeave": ("who leave", "谁请假", "on leave today", "今天请假"),
    "/fpms": ("fpms", "fpms duty", "fpms 值班"),
    "/pms": ("pms duty",),
    "/bi": ("bi duty",),
    "/fe": ("fe duty",),
    "/cpms": ("cpms",),
    "/sre": ("sre",),
    "/db": ("db duty", "dba"),
    "/liveslot": ("liveslot", "live slot"),
    "/ote": ("ote",),
    "/ft": ("ft duty",),
    "/ose": ("ose duty", "ose 值班"),
    "/osedate": ("ose date",),
    "/offset": ("offset", "调休", "换班", "补班", "apply leave", "申请请假"),
    "/checkcredit": ("credit", "额度", "信用", "check credit", "分数"),
    "/checkcreditdate": ("credit date", "missing credit"),
    "/stuckcredit": ("stuck credit", "卡分", "卡额度"),
    "/checkmachinelog": ("machine log", "机台日志", "日志"),
    "/machineerror": ("machine error", "机台错误", "报错"),
    "/cctv": ("cctv", "监控", "摄像头", "camera"),
    "/findmachine": ("find machine", "找机台", "哪些机台", "which machine"),
    "/nch": ("nch", "encoder", "编码器"),
    "/nwr": ("nwr", "encoder", "编码器"),
    "/wf": ("winford", "encoder", "编码器"),
    "/tbr": ("tbr", "encoder", "编码器"),
    "/tbp": ("tbp", "encoder", "编码器"),
    "/cp": ("encoder", "编码器"),
    "/dhs": ("dhs", "encoder", "编码器"),
    "/mdr": ("mdr", "encoder", "编码器"),
    "/list": ("machine list", "机台列表"),
    "/sm": ("set maintenance", "unset maintenance", "维护", "prod maintenance"),
    "/stresstest": ("stress test", "stress test reminder", "压测", "压力测试", "meter roll over"),
    "/m": ("evo batch", "evo 批量"),
    "/egs": ("egs", "egs maintenance", "simpleplay maintenance", "维护通知发信"),
    "/egstest": ("egs test", "egstest", "preview egs", "测试维护标题"),
    "/egsreply": ("egs reply", "reply email", "回复邮件"),
    "/egsreplytest": ("egs reply test", "test reply email", "测试回复邮件"),
    "/al": ("amount loss", "损失", "输赢"),
    "/pid": ("provider id", "provider", "供应商"),
    # Deliberately no bare "ip" alias — it would fire on unrelated traffic.
    "/isp": ("isp", "asn", "ip lookup", "ip owner", "whose ip", "运营商", "ip 归属"),
    "/smsfail": ("sms fail", "短信失败", "otp fail"),
    "/smscheckplayer": ("sms player", "短信玩家", "otp player"),
    "/npthirdhttp": ("np third", "third http"),
    "/checkperson": ("check person", "who checks", "检查人", "负责人"),
    "/ec": ("emergency contact", "紧急联系"),
    "/ecsre": ("sre emergency", "sre 紧急"),
    "/reminder": ("remind", "提醒", "定时", "timer"),
    "/addreminder": ("add reminder", "加提醒"),
    "/deletereminder": ("delete reminder", "删提醒", "show reminder"),
    "/checkemail": ("check email", "邮件"),
    "/checkerror": ("check error", "service error", "bot error", "服务错误", "查错误"),
    "/log": ("log", "logs", "journal", "journalctl", "日志", "查日志", "grep log"),
    "/checkevo": ("check evo", "evo game", "gamelist", "game list", "查游戏", "游戏上线", "check game"),
    "/dutycheckall": ("duty check all", "值班检查"),
}


def is_enabled() -> bool:
    return (os.getenv("BOT_COMMAND_SUGGEST") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


# ---------------------------------------------------------------------------
# Registry (built lazily from bot_help + aliases above)
# ---------------------------------------------------------------------------

_REGISTRY_LOCK = threading.Lock()
_REGISTRY: list[dict[str, Any]] | None = None


def _build_registry() -> list[dict[str, Any]]:
    entries: dict[str, dict[str, Any]] = {}
    try:
        import bot_help

        for _key, _title, _emoji, _tpl, rows in bot_help._help_sections():
            for usage, desc_en, desc_zh in rows:
                base = (usage or "").split()[0].strip().lower()
                if not base.startswith("/") or base in entries:
                    continue
                entries[base] = {
                    "command": base,
                    "usage": usage,
                    "desc_en": desc_en,
                    "desc_zh": desc_zh,
                    "aliases": set(),
                }
    except Exception as ex:
        print(f"[commandsuggest] bot_help catalog unavailable: {ex!r}", flush=True)
    for base, aliases in _EXTRA_ALIASES.items():
        e = entries.setdefault(
            base,
            {"command": base, "usage": base, "desc_en": "", "desc_zh": "", "aliases": set()},
        )
        e["aliases"].update(a.lower() for a in aliases)
    for base, e in entries.items():
        e["aliases"].add(base.lstrip("/"))
    return [e for b, e in entries.items() if b not in _NEVER_SUGGEST]


def _registry() -> list[dict[str, Any]]:
    global _REGISTRY
    with _REGISTRY_LOCK:
        if _REGISTRY is None:
            _REGISTRY = _build_registry()
        return _REGISTRY


def known_slash_commands() -> list[str]:
    return sorted({e["command"] for e in _registry()})


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def _alias_score(text: str, text_low: str, aliases: set[str]) -> float:
    score = 0.0
    hits = 0
    for a in aliases:
        if not a or len(a) < 2:
            continue
        if _CJK_RE.search(a):
            # lowercase both sides — CJK unaffected, latin parts of mixed
            # aliases (e.g. "ose 值班") must match case-insensitively
            hit = a in text_low
        else:
            hit = bool(re.search(rf"(?i)(?<![a-z0-9]){re.escape(a)}(?![a-z0-9])", text_low))
        if hit:
            hits += 1
            score = max(score, 1.0 + min(len(a), 12) / 24.0)
    if hits > 1:
        score += 0.25 * (hits - 1)
    return score


def suggest_commands(text: str, *, limit: int = 5) -> list[dict[str, Any]]:
    """Rank commands for a natural-language message that nothing else matched."""
    raw = (text or "").strip()
    if not raw or not is_enabled() or len(raw) > 600:
        return []
    if raw.lstrip().startswith("/"):
        return suggest_for_slash_typo(raw, limit=limit)
    low = raw.lower()
    out: list[dict[str, Any]] = []

    # Machine-token suggestions (deterministic beats keyword scores).
    try:
        import commandagent as _ca

        ml = _ca.detect_machine_lookup_command(raw)
        if ml:
            out.append(
                {
                    "command": ml,
                    "usage": ml,
                    "desc_en": "Machine info / encoder lookup",
                    "desc_zh": "机台信息 / 编码器查询",
                    "score": 3.0,
                }
            )
        elif len(raw) <= 80:
            # Short message with a machine token but extra words ("TBR2099 坏了")
            # — offer the lookup as a suggestion. Long pastes (incident reports)
            # are excluded so plain chat still handles them.
            toks = _ca._MACHINE_LOOKUP_TOKEN_RE.findall(raw)
            if toks:
                bases = {_ca._MACHINE_LOOKUP_SITE_CMDS[t[0].lower()] for t in toks}
                if len(bases) == 1:
                    cmd = f"{bases.pop()} {' '.join(t[1] for t in toks[:5])}"
                    out.append(
                        {
                            "command": cmd,
                            "usage": cmd,
                            "desc_en": "Machine info / encoder lookup",
                            "desc_zh": "机台信息 / 编码器查询",
                            "score": 2.2,
                        }
                    )
    except Exception:
        pass

    seen = {s["command"].split()[0] for s in out}
    # A concrete machine-token suggestion supersedes ALL site-lookup commands —
    # they share the "encoder/编码器" aliases and would only add noise.
    _site_bases = {"/nch", "/nwr", "/wf", "/tbr", "/tbp", "/cp", "/dhs", "/mdr"}
    skip_sites = bool(out)
    for e in _registry():
        if e["command"] in seen:
            continue
        if skip_sites and e["command"] in _site_bases:
            continue
        sc = _alias_score(raw, low, e["aliases"])
        if sc >= 0.9:
            out.append({**{k: e[k] for k in ("command", "usage", "desc_en", "desc_zh")}, "score": sc})
    out.sort(key=lambda s: s["score"], reverse=True)
    return out[:limit]


def suggest_for_slash_typo(text: str, *, limit: int = 4) -> list[dict[str, Any]]:
    """"/fpm" → "/fpms" etc. via difflib + prefix match over known slash tokens."""
    raw = (text or "").strip()
    if not raw.startswith("/") or not is_enabled():
        return []
    token = raw.split()[0].lower()
    if len(token) < 3:  # lone "/" or "/x" would prefix-match half the registry
        return []
    bases = known_slash_commands()
    if token in bases:
        return []  # valid command that failed for another reason — don't second-guess
    scored: dict[str, float] = {}
    for b in bases:
        if b.startswith(token) or token.startswith(b):
            scored[b] = max(scored.get(b, 0.0), 2.0 + len(token) / 40.0)
    for b in difflib.get_close_matches(token, bases, n=limit * 2, cutoff=0.6):
        ratio = difflib.SequenceMatcher(None, token, b).ratio()
        scored[b] = max(scored.get(b, 0.0), 1.0 + ratio)
    by_base = {e["command"]: e for e in _registry()}
    out = []
    for b, sc in sorted(scored.items(), key=lambda kv: kv[1], reverse=True)[:limit]:
        e = by_base.get(b) or {"usage": b, "desc_en": "", "desc_zh": ""}
        # Button runs the BARE base — appending the typo's args would break
        # exact-match commands ("/fpms 123" never fires the /fpms handler).
        out.append(
            {
                "command": b,
                "usage": e.get("usage", b),
                "desc_en": e.get("desc_en", ""),
                "desc_zh": e.get("desc_zh", ""),
                "score": sc,
            }
        )
    return out


def usage_hint(text: str) -> Optional[str]:
    """For a VALID slash token that produced no reply, return its usage line."""
    token = ((text or "").strip().split() or [""])[0].lower()
    if not token.startswith("/"):
        return None
    for e in _registry():
        if e["command"] == token:
            desc = e.get("desc_en") or e.get("desc_zh") or ""
            return f"Usage: `{e['usage']}`" + (f" — {desc}" if desc else "")
    return None


# ---------------------------------------------------------------------------
# Pending "just chat" store (for the 💬 button)
# ---------------------------------------------------------------------------

_PENDING_LOCK = threading.Lock()
_PENDING: dict[str, tuple[float, str]] = {}
_PENDING_TTL_SEC = 900.0
_PENDING_MAX = 200


def remember_pending_chat(chat_id: str, sender_id: str, text: str) -> None:
    key = f"{(chat_id or '').strip()}:{(sender_id or '').strip()}"
    if not key.strip(":") or not (text or "").strip():
        return
    now = time.time()
    with _PENDING_LOCK:
        for k in [k for k, (ts, _) in _PENDING.items() if now - ts > _PENDING_TTL_SEC]:
            _PENDING.pop(k, None)
        _PENDING.pop(key, None)  # re-insert so refreshed entries move to the tail
        if len(_PENDING) >= _PENDING_MAX:
            _PENDING.pop(next(iter(_PENDING)), None)
        _PENDING[key] = (now, (text or "").strip()[:2000])


def pop_pending_chat(chat_id: str, sender_ids: list[str]) -> Optional[str]:
    now = time.time()
    with _PENDING_LOCK:
        for sid in sender_ids:
            sid = (sid or "").strip()
            if not sid:
                continue
            key = f"{(chat_id or '').strip()}:{sid}"
            item = _PENDING.pop(key, None)
            if item and now - item[0] <= _PENDING_TTL_SEC:
                return item[1]
    return None


# ---------------------------------------------------------------------------
# Card
# ---------------------------------------------------------------------------


def _v2_callback_button(label: str, btn_type: str, payload: dict, element_id: str) -> dict:
    return {
        "tag": "button",
        "text": {"tag": "plain_text", "content": label[:60]},
        "type": btn_type,
        "behaviors": [
            {"type": "callback", "value": {k: str(v) for k, v in payload.items()}}
        ],
        "element_id": element_id[:20],
    }


def _v2_button_row(buttons: list[dict]) -> dict:
    return {
        "tag": "column_set",
        "flex_mode": "flow",
        "background_style": "default",
        "horizontal_spacing": "8px",
        "columns": [
            {
                "tag": "column",
                "width": "auto",
                "weight": 1,
                "vertical_align": "top",
                "elements": [b],
            }
            for b in buttons
        ],
    }


def build_suggestion_card(
    text: str, suggestions: list[dict[str, Any]], *, offer_chat: bool = True
) -> Optional[dict]:
    sugs = [s for s in (suggestions or []) if len(str(s.get("command") or "")) <= 200][:6]
    if not sugs:
        return None
    cjk = bool(_CJK_RE.search(text or ""))
    intro = (
        "🤔 我不太确定你的意思 — 你是不是想用这些指令？**点按钮直接执行**："
        if cjk
        else "🤔 I'm not sure what you meant — did you mean one of these commands? **Tap a button to run it:**"
    )
    lines = [intro, ""]
    buttons: list[dict] = []
    for i, s in enumerate(sugs, 1):
        desc = (s.get("desc_zh") if cjk else s.get("desc_en")) or s.get("desc_en") or ""
        lines.append(f"{i}. **`{s['command']}`**" + (f" — {desc}" if desc else ""))
        buttons.append(
            _v2_callback_button(
                s["command"],
                "primary" if i == 1 else "default",
                {"k": "cmd_run", "c": s["command"]},
                f"cmdsug_{i}",
            )
        )
    elements: list[dict] = [
        {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}}
    ]
    for i in range(0, len(buttons), 3):
        elements.append(_v2_button_row(buttons[i : i + 3]))
    if offer_chat:
        chat_label = "💬 都不是，聊天回答" if cjk else "💬 None of these — just chat"
        elements.append(
            _v2_button_row(
                [_v2_callback_button(chat_label, "default", {"k": "cmd_chat"}, "cmdsug_chat")]
            )
        )
    note = (
        "也可以直接把指令打出来，或说 `/help` 看全部指令。"
        if cjk
        else "You can also type the command yourself, or say `/help` for the full list."
    )
    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": note}})
    title = "指令建议 / Did you mean…" if cjk else "Did you mean… / 指令建议"
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "orange", "title": {"tag": "plain_text", "content": title}},
        "body": {"elements": elements},
    }


def send_suggestion_card(
    chat_id: str,
    text: str,
    suggestions: list[dict[str, Any]],
    *,
    send_message: Callable,
    sender_id: str = "",
) -> bool:
    """Build + send the suggestion card (text-list fallback). Returns True when sent."""
    card = build_suggestion_card(text, suggestions)
    if not card:
        return False
    remember_pending_chat(chat_id, sender_id, text)
    try:
        resp = send_message(chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive")
        if not (isinstance(resp, dict) and resp.get("code") not in (0, None)):
            return True
    except Exception as ex:
        print(f"[commandsuggest] card send failed: {ex!r}", flush=True)
    # Plain-text fallback (still useful: user can type the command).
    try:
        cjk = bool(_CJK_RE.search(text or ""))
        head = "你是不是想用：" if cjk else "Did you mean:"
        body = "\n".join(f"{i}. `{s['command']}`" for i, s in enumerate(suggestions[:6], 1))
        send_message(chat_id, f"{head}\n{body}")
        return True
    except Exception as ex:
        print(f"[commandsuggest] text fallback failed: {ex!r}", flush=True)
        return False


def startup_status() -> None:
    print(
        f"[commandsuggest] enabled={is_enabled()} registry={len(_registry())} commands",
        flush=True,
    )
