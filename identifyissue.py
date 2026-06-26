"""
identifyissue — "what is this issue?" analyzer for the Duty Bot (CP OM Duty).

Goal
----
When a CS / QA / duty member pastes a player report (e.g. "players can't login,
Account ID: ...") the bot should *explain the issue*: classify it as **P0 Major /
P0 Minor / P1 / Other**, say which systems are likely involved (FPMS / CPMS /
PMS / FE / SRE), who to call, the handling steps, and produce a ready-to-send
**P0 Incident Overview in English + 中文**.

All of the OSE / CP P0 SOP knowledge (P0 quick guide, escalation rules, P0 flow,
withdrawal/deposit special cases, OTP cases, and the FPMS/CPMS/PMS/FE system
architecture) is baked into ``SOP_KNOWLEDGE`` below so the AI always reasons with
the real runbook — not guesses.

Two modes
---------
1. **AI mode** — if an OpenAI-compatible LLM is configured (same env vars as
   ``chatagent``: ``BOT_CHAT_API_KEY`` / ``OPENAI_API_KEY`` etc.), the model gets
   ``SOP_KNOWLEDGE`` as a system prompt and writes a detailed bilingual analysis.
2. **Deterministic fallback** — if no LLM (or it errors), a rule-based classifier
   still produces a classification, the right support departments, and a bilingual
   P0 overview, so the bot is never silent.

CLI
---
    python identifyissue.py "players cannot login ... Account ID: 1, 2, 3"
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

try:  # so BOT_CHAT_* / BOT_ISSUE_* resolve when run standalone (main.py already loads .env)
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

# ---------------------------------------------------------------------------
# SOP / runbook knowledge base (the AI reasons strictly from this).
# Transcribed from the CP OM Duty P0 SOP guide + system architecture diagrams.
# ---------------------------------------------------------------------------

SOP_KNOWLEDGE = """You are **CP OM Duty Bot — Issue Identifier**. A CS/QA/duty member pastes a player
problem report and you explain *what the issue is* and *how to handle it*, using ONLY the
CP / OSE P0 SOP runbook below. Be precise, structured and actionable. Never invent phone
numbers or names that are not in the runbook.

================ P0 SOP QUICK GUIDE — ESCALATION CATEGORY ================
MAJOR ISSUES (P0 major): Login, games/events entering, withdrawal, deposit,
promotion/voucher, rebate, LuckyCoin problems.
  - Any issue which caused COMPANY loss.
  - MUST send the P0 overview to the WhatsApp group as well.
MINOR ISSUES (P0 minor): All other issues, including cases where it is unclear whether the
problem is on our side or limited to a specific provider (especially if only ONE provider
is affected).
  - Any issue which caused PLAYER loss.
  - No need to send the P0 overview to the WhatsApp group.
NOTE: If MORE THAN 4 players are experiencing the issue, verify with Aldan Chan & Miyu —
this is considered P0. Every time you call, give them a brief update on what is happening.

================ WITHDRAWAL / DEPOSIT SPECIAL CASES ================
CASE A — All channels affected (all banks, e-wallets AND providers):
  -> Treat as P0 MAJOR issue -> Call P0-OM -> Follow the full P0 process.
CASE B — Specific provider only / unsure if the issue is ours:
  -> Follow P1 process -> Call ONLY the on-duty SRE.
  -> Once confirmed it is a provider issue -> End the meeting -> Proceed with the provider
     escalation process.

================ P0 FLOW ================
1. Start a Lark meeting immediately.
2. Call based on the category:
   - MAJOR issue -> Call P0-OM via WhatsApp, and also: Bk, Yang, Koo, YC, Wennie, Eden,
     Jun Meng. (Working or non-working hours, the P0-OM Group must still be contacted.)
   - MINOR issue -> Call the on-duty SRE (pick the SRE on duty for the project/category of
     the issue, per the OSE & SRE Duty Shift Document). No response -> escalate to Wei Siong
     & Adrian Chong. At the same time also call Aldan Chan & Miyu.
   - At the same time call the Emergency Group P0 -> Aldan Chan, Miyu (this is the GENERAL
     emergency group, used for ANY P0).
   - The Game Urgent Group P0 (Yui Yang + Product Manager + Game Operation, the "Game Issue
     Emergency Contact") is a SEPARATE group — call it ONLY for GAME issues (cannot enter game,
     game launch/transfer, a specific game provider). Do NOT call Game Urgent for login,
     withdrawal, deposit, OTP, promotion or other non-game issues.
   - If the issue is clearly confirmed under a specific team (e.g. CPMS), you may call the
     P0-DEV WhatsApp group immediately (P0-CPMS DEV, P0-FPMS DEV, P0-Frontend DEV, P0-PMS DEV).
3. Coordinate in the meeting and confirm if further support is needed.
   - Do NOT call WhatsApp groups at this step. OM Duty contacts members individually in order:
     FE:   On-duty -> Eden -> Jun Meng -> Wennie
     FPMS: On-duty -> Bk -> Yang -> Eason -> Greg
     PMS:  On-duty -> Manuel Lorenzo Pereira -> Darren -> Alviss
     CPMS: On-duty -> YC -> Koo
4. Need to call the OS team (Aldan Chan, Miyu).

================ ESCALATION RULE (BOTH SCENARIOS) ================
CP OM Duty must call Greg, Eason, Rock Lim if the ROOT CAUSE is still NOT identified:
  - Major Issue  -> after 5 mins
  - Minor Issue  -> after 10 mins
  - Other Issue  -> after 15 mins (no need to call)
Incident Commander priority: Adrian Chong -> Wei Siong -> SRE Duty -> Dev TL -> Dev Duty.

================ MINOR-ISSUE TRIAGE FLOW (Backend vs Frontend) ================
Issue -> identify Minor or Major P0.
  Major issues (withdrawal, deposit, or login problems) -> CALL P0-OM -> P0 SOP.
  Minor issues -> identify Backend or Frontend issue:
    Frontend -> Call Frontend SRE on duty -> escalate to Wei Siong & Adrian if no response
                -> Call FE DEV on duty -> P0 SOP.
    Backend  -> Call Backend SRE on duty -> escalate to Wei Siong & Adrian if no response
                -> identify PMS / CPMS / FPMS -> Call that DEV on duty -> P0 SOP.

================ OTP / SMS LOGIN CASES ================
1. Start a Lark meeting immediately.
2. Call Jacob C. (09681199077) and ask him to join the P0 meeting. Contact SRE Backend Duty
   to check the SMS server. If you cannot reach Jacob, contact Zora (09616987232).
   Check the SMS backend logs, then proceed by scenario:
   - SCENARIO 1 — Success on OUR side, FAILED on provider side: say "OTP sent out success,
     ask players to try and login again." Wait for player confirmation, then report the issue
     to the provider (NEW OTP GUIDELINE).
   - SCENARIO 2 — FAILED on BOTH our side and the provider: call Lim Lian Cheng. If you can't
     reach him, contact Qi Xiang or Ho Ching.
OTP note: If 4+ players hit the issue, first confirm it is currently ongoing. If yes, check
the logs and follow the standard flow: if logs show the request was SUCCESSFUL, ask the
player to try again; if MULTIPLE FAILURES, disable the line causing the issue and have CS ask
the player to retry; if it still fails, proceed with the P0 SOP. QA initially classifies OTP
as P1 unless there are confirmed failures in the logs. For P1: no meeting needed unless there
are multiple failures and all troubleshooting is done but the issue persists. For P0: follow
the P0 SOP and inform Jacob about what was checked and done.

================ SYSTEM ARCHITECTURE (which DEV/SRE owns what) ================
User -> Casinoplus Website -> FPMS. FPMS routes Deposit/Withdrawal -> PMS, and Enter-Game -> CPMS.
- FPMS (Fantasy Player Management System): MAIN system. Holds ALL player data & activities.
  If FPMS is unhealthy, it AFFECTS PLAYER LOGIN to the CP website. Owner: FPMS Dev.
  Promotion / voucher / rebate / LuckyCoin / player balance also live here.
- CPMS (Content Provider Management System): the BRIDGE from our servers to the provider
  servers. When a player opens games (BACCARAT, COLORGAME, GCILIVE/Roulette, EGS slots, OSM),
  FPMS sends game info + transfer in/out to CPMS, which talks to the third-party providers so
  the player can enter the game. Owner: CPMS Dev. "Can't enter game" / game-launch issues.
- PMS (Payment Management System): Deposit/Withdrawal. On Deposit the player is redirected to
  PMS-CP (port 8182, GCASH). After the top-up is confirmed, PMS sends the order to the FPMS
  backend (port 7100) for record-keeping. Channels: GCASH, Maya, Banks. Owner: PMS Dev.
- Frontend (FE) Dev: client-side / website UI & UX. Anything wrong with the page itself
  (rendering, buttons, display, web error pages) -> DEV-FRONTEND / Frontend SRE.
- SRE (Site Reliability Engineer; SRE-platform & SRE-game): systems, automation, monitoring,
  incident response. First responder for MINOR issues (Backend SRE vs Frontend SRE).
- DBA: Database Administrator.

Deposit flow (for reference): User clicks Deposit (Website) -> FPMS Create TopUp Proposal ->
PMS Get Available Deposit Provider -> Create Merchant Proposal -> Provider Validate User Top Up
-> Confirm TopUp -> FPMS Save Record & Top Up User Balance -> Website Display TopUp amount.

================ HOW TO PICK THE "SUPPORT REQUEST" DEPARTMENTS ================
Map the symptom to the likely owners (request support from these teams):
- Login / cannot log in / login error / OTP login        -> FPMS, FE, SRE (OTP also: SRE Backend + Jacob/SMS)
- Cannot enter game / game won't open / transfer in-out   -> CPMS, FPMS, SRE (+ the game provider)
- Deposit / top-up problems                               -> PMS, FPMS, SRE
- Withdrawal / cash-out problems                          -> PMS, FPMS, SRE
- Promotion / voucher / rebate / LuckyCoin / balance      -> FPMS, SRE
- Website not loading / UI / display / page error         -> FE, SRE
- OTP / SMS not received                                  -> SRE (Backend), Jacob (SMS server), provider

================ OUTPUT FORMAT (always follow) ================
Reply in this exact structure, English first then 中文. Be concise but complete:

🔎 Issue Identified / 问题识别
- Summary: <one line of what is happening>
- Category: <P0 Major | P0 Minor | P1 | Other> — <why, citing the SOP rule>
- Likely system(s): <FPMS / CPMS / PMS / FE / SRE / provider, with a one-line reason from the architecture>
- Special case: <only if withdrawal/deposit Case A/B or OTP applies; else "none">

🛠️ How to handle / 处理步骤
- <numbered steps from the P0 flow / triage flow / OTP flow relevant to THIS issue>
- Escalation timer: <5/10/15 min rule based on category>

📞 Who to call / 需要联系
- <the specific people/groups from the SOP for this category>

🚨 P0 Incident Overview (copy-paste)
P0 Incident Overview
🕒 Time: <YYYY-MM-DD HH:MM> - Incident Start
🔥 Issue: <short English description>
🎯 Impact scope: <N players, plus QA/CS if relevant>
👥 Support request: <the support departments you picked>

P0 事故概览
🕒 时间: <YYYY-MM-DD HH:MM> (事故开始)
🔥 问题: <中文描述>
🎯 影响范围: <N名玩家，QA，CS>
👥 支援请求: <支援部门>

Rules: Use the player count you are given for "Impact scope / 影响范围". If withdrawal/deposit
hits ALL channels -> P0 Major (Case A); if only one provider/unsure -> P1/minor (Case B). If
4+ players and not clearly major, still treat as P0 and verify with Aldan Chan & Miyu. Keep the
overview short and ready to paste into the WhatsApp / Lark group. ONLY mention the Game Urgent
Group when the issue is a GAME issue (cannot enter game / game provider) — never for login,
withdrawal, deposit or OTP.
"""

# ---------------------------------------------------------------------------
# Deterministic classification helpers (used for the fallback AND to ground the
# AI's "Impact scope" with a real player count).
# ---------------------------------------------------------------------------

# Symptom -> (category_hint, [support departments], chinese label, english label)
_CATEGORY_RULES: list[tuple[str, re.Pattern, list[str], str, str]] = [
    (
        "login",
        re.compile(r"(?i)\b(log\s*in|login|log\s*on|sign\s*in|cannot\s+login|can't\s+login|unable\s+to\s+login)\b"),
        ["FPMS", "FE", "SRE"],
        "登录",
        "Login",
    ),
    (
        "otp",
        re.compile(r"(?i)\b(otp|one[\s-]*time\s*password|sms|verification\s*code|verify\s*code|did\s*not\s*receive\s*code)\b"),
        ["SRE (Backend)", "FPMS", "SMS (Jacob)"],
        "OTP/短信",
        "OTP / SMS",
    ),
    (
        "enter_game",
        re.compile(r"(?i)\b(enter\s*game|enter\s*the\s*game|open\s*game|can'?t\s*enter|cannot\s*enter|launch\s*game|game\s*not\s*loading|baccarat|colorgame|roulette|slot|gcilive|osm|egs)\b"),
        ["CPMS", "FPMS", "SRE", "Provider"],
        "进入游戏",
        "Enter game",
    ),
    (
        "deposit",
        re.compile(r"(?i)\b(deposit|top\s*up|topup|recharge|gcash|maya)\b"),
        ["PMS", "FPMS", "SRE"],
        "存款",
        "Deposit",
    ),
    (
        "withdrawal",
        re.compile(r"(?i)\b(withdraw|withdrawal|cash\s*out|cashout|payout)\b"),
        ["PMS", "FPMS", "SRE"],
        "提款",
        "Withdrawal",
    ),
    (
        "promo",
        re.compile(r"(?i)\b(promotion|promo|voucher|rebate|lucky\s*coin|luckycoin|bonus|reward|cashback)\b"),
        ["FPMS", "SRE"],
        "优惠/返水",
        "Promotion / voucher / rebate / LuckyCoin",
    ),
    (
        "frontend",
        re.compile(r"(?i)\b(website\s*(?:not|down|error|blank|white)|page\s*(?:error|not\s*load|blank)|ui|display|button|cannot\s*open\s*(?:the\s*)?(?:web|site|page)|404|500|loading\s*forever)\b"),
        ["FE", "SRE"],
        "前端/网页",
        "Website / frontend",
    ),
]

# Symptoms that are MAJOR P0 categories per the quick guide.
_MAJOR_CATEGORIES = {"login", "enter_game", "deposit", "withdrawal", "promo"}

_ALL_CHANNELS_RE = re.compile(
    r"(?i)\b(all\s+(?:channels|banks|e-?wallets|providers)|every\s+(?:channel|bank|provider)|"
    r"all\s+(?:payment\s+)?methods|全部(?:渠道|通道)|所有(?:渠道|通道|银行|provider))\b"
)
_ONE_PROVIDER_RE = re.compile(
    r"(?i)\b(only\s+one\s+provider|single\s+provider|specific\s+provider|one\s+bank\s+only|"
    r"just\s+one\s+(?:provider|bank|channel))\b"
)


def _tz() -> ZoneInfo:
    name = (os.getenv("BOT_ISSUE_TZ") or "Asia/Manila").strip()
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("Asia/Manila")


def _now_str() -> str:
    return datetime.now(_tz()).strftime("%Y-%m-%d %H:%M")


def extract_account_ids(text: str) -> list[str]:
    """Pull player account IDs from a pasted report.

    Priority: numbers that follow an "Account ID" / "ID" / "UID" marker, then any
    standalone 6+ digit runs (account IDs in these reports are long numbers).
    """
    raw = text or ""
    ids: list[str] = []
    seen: set[str] = set()

    def _add(token: str) -> None:
        t = token.strip()
        if t and t not in seen:
            seen.add(t)
            ids.append(t)

    # 1) explicit markers, including a marker followed by several IDs on next lines
    marker = re.search(
        r"(?i)(?:account\s*id|player\s*id|user\s*id|account|玩家\s*id|账号|帳號|uid|id)\s*[:：#]?\s*(.+)$",
        raw,
        re.S,
    )
    if marker:
        tail = marker.group(1)
        for tok in re.findall(r"\b\d{6,}\b", tail):
            _add(tok)
    if ids:
        return ids

    # 2) any long digit runs anywhere (skip dates like 2026-06-26)
    no_date = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", " ", raw)
    for tok in re.findall(r"\b\d{6,}\b", no_date):
        _add(tok)
    return ids


def count_players(text: str) -> int:
    """Best-effort player-impact count: explicit "N players" wins, else # of account IDs."""
    raw = text or ""
    m = re.search(r"(?i)\b(\d{1,4})\s*(?:players?|users?|accounts?|玩家|名玩家|位玩家|个玩家)\b", raw)
    if m:
        try:
            n = int(m.group(1))
            if n > 0:
                return n
        except ValueError:
            pass
    ids = extract_account_ids(raw)
    if ids:
        return len(ids)
    # "a player" / "the player" singular mention
    if re.search(r"(?i)\b(a|one|the)\s+player\b|有(?:个|位|名)?玩家", raw):
        return 1
    return 0


def classify(text: str) -> dict:
    """Rule-based classification. Returns category, severity, departments, flags."""
    raw = text or ""
    matched: list[tuple[str, list[str], str, str]] = []
    for key, pat, depts, zh, en in _CATEGORY_RULES:
        if pat.search(raw):
            matched.append((key, depts, zh, en))

    players = count_players(raw)
    all_channels = bool(_ALL_CHANNELS_RE.search(raw))
    one_provider = bool(_ONE_PROVIDER_RE.search(raw))

    # Aggregate departments (preserve order, dedupe)
    depts: list[str] = []
    for _key, dl, _zh, _en in matched:
        for d in dl:
            if d not in depts:
                depts.append(d)

    cat_keys = [m[0] for m in matched]
    en_labels = [m[3] for m in matched]
    zh_labels = [m[2] for m in matched]

    is_payment = any(k in ("deposit", "withdrawal") for k in cat_keys)
    is_major_cat = any(k in _MAJOR_CATEGORIES for k in cat_keys)

    # Severity logic per SOP.
    severity = "Other"
    reason_en = "No clear P0 category matched."
    reason_zh = "未匹配到明确的 P0 类别。"

    if is_payment and one_provider and not all_channels:
        severity = "P1 / Minor"
        reason_en = "Withdrawal/Deposit limited to one provider / unsure if ours (Case B) → P1, call on-duty SRE only."
        reason_zh = "提款/存款仅影响单一 provider 或不确定是否我方问题（Case B）→ P1，仅联系值班 SRE。"
    elif is_payment and all_channels:
        severity = "P0 Major"
        reason_en = "Withdrawal/Deposit affects ALL channels (Case A) → P0 Major, call P0-OM and follow full P0 process."
        reason_zh = "提款/存款影响所有渠道（Case A）→ P0 重大，呼叫 P0-OM 并走完整 P0 流程。"
    elif is_major_cat:
        severity = "P0 Major"
        reason_en = "Symptom is in the Major list (login / enter game / deposit / withdrawal / promotion-voucher-rebate-LuckyCoin)."
        reason_zh = "症状属于重大类别（登录 / 进入游戏 / 存款 / 提款 / 优惠-代金券-返水-LuckyCoin）。"
    elif "otp" in cat_keys:
        severity = "P1 (verify logs)"
        reason_en = "OTP is initially P1 unless logs show confirmed failures; 4+ ongoing failures → P0."
        reason_zh = "OTP 默认 P1，除非日志确认失败；4+ 持续失败 → P0。"
    elif matched:
        severity = "P0 Minor"
        reason_en = "Matched a non-major category → treat as Minor (player loss); call on-duty SRE."
        reason_zh = "匹配到非重大类别 → 视为次要（玩家损失）；联系值班 SRE。"

    # 4+ players rule: bump toward P0 and verify.
    four_plus = players >= 4
    if four_plus and severity in ("Other", "P0 Minor", "P1 (verify logs)", "P1 / Minor"):
        reason_en += f" {players} players affected (>4) → verify with Aldan Chan & Miyu, treat as P0."
        reason_zh += f" 受影响玩家 {players} 名（>4）→ 与 Aldan Chan & Miyu 确认，按 P0 处理。"

    return {
        "categories": cat_keys,
        "category_en": en_labels,
        "category_zh": zh_labels,
        "departments": depts,
        "severity": severity,
        "reason_en": reason_en,
        "reason_zh": reason_zh,
        "players": players,
        "all_channels": all_channels,
        "one_provider": one_provider,
        "is_otp": "otp" in cat_keys,
        "four_plus": four_plus,
    }


# ---------------------------------------------------------------------------
# LLM call (OpenAI-compatible; same env vars as chatagent).
# ---------------------------------------------------------------------------

def _llm_api_key() -> str:
    return (
        os.getenv("BOT_CHAT_API_KEY")
        or os.getenv("ANTHROPIC_API_KEY")
        or os.getenv("OPENAI_API_KEY")
        or ""
    ).strip()


def _llm_base_url() -> str:
    return (os.getenv("BOT_CHAT_API_BASE") or "https://api.openai.com/v1").strip().rstrip("/")


def _llm_model() -> str:
    return (os.getenv("BOT_ISSUE_MODEL") or os.getenv("BOT_CHAT_MODEL") or "gpt-4o-mini").strip()


def llm_available() -> bool:
    return bool(_llm_api_key())


def is_enabled() -> bool:
    """Issue identifier is on by default; set BOT_USE_IDENTIFYISSUE=0 to disable."""
    return (os.getenv("BOT_USE_IDENTIFYISSUE") or "1").strip().lower() in ("1", "true", "yes", "on")


def show_thinking() -> bool:
    """Set BOT_ISSUE_SHOW_THINKING=1 to enable the model's reasoning and show it in the card."""
    return (os.getenv("BOT_ISSUE_SHOW_THINKING") or "0").strip().lower() in ("1", "true", "yes", "on")


def _llm_complete_full(
    system_prompt: str,
    user_text: str,
    *,
    max_tokens: Optional[int] = None,
    timeout: Optional[float] = None,
    think: Optional[bool] = None,
) -> tuple[Optional[str], str]:
    """Call the LLM; return ``(answer, reasoning)``. ``reasoning`` is the model's
    thinking trace when the backend exposes it (Ollama/qwen ``reasoning``)."""
    api_key = _llm_api_key()
    if not api_key:
        return None, ""
    url = f"{_llm_base_url()}/chat/completions"
    payload = {
        "model": _llm_model(),
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        "max_tokens": int(max_tokens if max_tokens is not None else int(os.getenv("BOT_ISSUE_MAX_TOKENS", "3000"))),
        "temperature": float(os.getenv("BOT_ISSUE_TEMPERATURE", "0.3")),
    }
    base = _llm_base_url().lower()
    if "11434" in base or "ollama" in base:
        if think is None:
            think = (os.getenv("BOT_CHAT_LLM_THINK") or "false").strip().lower() in ("1", "true", "yes", "on")
        payload["think"] = bool(think)
        # Keep the model resident so Ollama doesn't unload/reload between calls
        # (reload churn is what causes the intermittent 500 "connection closed").
        keep_alive = (os.getenv("BOT_CHAT_OLLAMA_KEEP_ALIVE") or "-1").strip()
        if keep_alive.lower() not in ("0", "off", "false", "no"):
            try:
                payload["keep_alive"] = int(keep_alive)
            except ValueError:
                payload["keep_alive"] = keep_alive
    _timeout = timeout if timeout is not None else float(os.getenv("BOT_ISSUE_LLM_TIMEOUT", "120"))
    attempts = max(1, int(os.getenv("BOT_ISSUE_LLM_RETRIES", "2")) + 1)
    last_err = ""
    for attempt in range(1, attempts + 1):
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=_timeout) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            choices = body.get("choices") or []
            if not choices:
                return None, ""
            message = choices[0].get("message") or {}
            content = (message.get("content") or "").strip()
            reasoning = (message.get("reasoning") or "").strip()
            if not content and reasoning:
                content = reasoning
            return (content or None), reasoning
        except urllib.error.HTTPError as exc:
            try:
                detail = exc.read().decode("utf-8", errors="replace")[:300]
            except Exception:
                detail = exc.reason
            last_err = f"HTTP {exc.code}: {detail}"
            # Only 5xx are worth retrying (model reload / transient); 4xx won't fix itself.
            if exc.code < 500:
                print(f"⚠️ identifyissue LLM {last_err}", flush=True)
                return None, ""
        except Exception as exc:
            last_err = repr(exc)
        if attempt < attempts:
            print(f"⚠️ identifyissue LLM transient error (attempt {attempt}/{attempts}): {last_err} — retrying", flush=True)
            import time as _time

            _time.sleep(float(os.getenv("BOT_ISSUE_LLM_RETRY_DELAY", "1.5")))
    print(f"⚠️ identifyissue LLM failed after {attempts} attempts: {last_err}", flush=True)
    return None, ""


def _llm_complete(
    system_prompt: str,
    user_text: str,
    *,
    max_tokens: Optional[int] = None,
    timeout: Optional[float] = None,
) -> Optional[str]:
    answer, _ = _llm_complete_full(
        system_prompt, user_text, max_tokens=max_tokens, timeout=timeout
    )
    return answer


# ---------------------------------------------------------------------------
# Bilingual P0 overview + deterministic full analysis (fallback).
# ---------------------------------------------------------------------------

def _impact_scope_text(info: dict, *, zh: bool) -> str:
    players = info.get("players", 0)
    if players > 0:
        if zh:
            return f"{players}名玩家，QA，CS"
        return f"{players} player{'s' if players != 1 else ''}, QA, CS"
    return ("玩家数待确认，QA，CS" if zh else "players TBC, QA, CS")


def build_p0_overview(text: str, info: Optional[dict] = None, *, issue_en: str = "", issue_zh: str = "") -> str:
    """Ready-to-paste bilingual P0 Incident Overview."""
    info = info or classify(text)
    now = _now_str()
    depts = ", ".join(info.get("departments") or ["FPMS", "FE", "SRE"])
    if not issue_en:
        labels = info.get("category_en") or []
        issue_en = (
            f"Players encounter a problem ({', '.join(labels)})." if labels
            else "Players encounter an issue (details in report)."
        )
    if not issue_zh:
        labels_zh = info.get("category_zh") or []
        issue_zh = (
            f"玩家遇到问题（{('、'.join(labels_zh))}）。" if labels_zh
            else "玩家遇到问题（详见报告）。"
        )
    lines = [
        "P0 Incident Overview",
        f"🕒 Time: {now} - Incident Start",
        f"🔥 Issue: {issue_en}",
        f"🎯 Impact scope: {_impact_scope_text(info, zh=False)}",
        f"👥 Support request: {depts}",
        "",
        "P0 事故概览",
        f"🕒 时间: {now} (事故开始)",
        f"🔥 问题: {issue_zh}",
        f"🎯 影响范围: {_impact_scope_text(info, zh=True)}",
        f"👥 支援请求: {depts}",
    ]
    return "\n".join(lines)


def _who_to_call(info: dict) -> list[str]:
    sev = info.get("severity", "")
    is_game = "enter_game" in (info.get("categories") or [])
    out: list[str] = []
    if info.get("is_otp"):
        out.append("Start a Lark meeting; call Jacob C. 📞 09681199077 (or Zora 📞 09616987232) + SRE Backend Duty to check the SMS server.")
        out.append("Scenario 2 (failed both sides): call Lim Lian Cheng → else Qi Xiang / Ho Ching.")
        return out
    if sev.startswith("P0 Major"):
        out.append("Call P0-OM via WhatsApp (also Bk, Yang, Koo, YC, Wennie, Eden, Jun Meng).")
        out.append("Emergency Group P0 (general): Aldan Chan, Miyu.")
        if is_game:
            out.append("Game Urgent Group (game issues only): Yui Yang + PM + Game Operation.")
        out.append("If root cause not found in 5 min: call Greg, Eason, Rock Lim.")
    elif "Minor" in sev or sev.startswith("P1"):
        out.append("Call the on-duty SRE for this category (per OSE & SRE Duty Shift Doc).")
        out.append("No response → escalate to Wei Siong & Adrian Chong; also call Aldan Chan & Miyu.")
        out.append("If root cause not found in 10 min: call Greg, Eason, Rock Lim.")
    else:
        out.append("Confirm severity; for 'Other' issues escalate after 15 min (no call needed).")
    if info.get("four_plus"):
        out.append("4+ players affected → verify with Aldan Chan & Miyu and treat as P0.")
    return out


def _deterministic_analysis(text: str, info: dict) -> str:
    sev = info["severity"]
    cats_en = ", ".join(info.get("category_en") or []) or "Unclassified"
    depts = ", ".join(info.get("departments") or ["FPMS", "FE", "SRE"])

    # Likely system explanation grounded in the architecture.
    sys_bits: list[str] = []
    ck = set(info.get("categories") or [])
    if "login" in ck:
        sys_bits.append("Login depends on FPMS (holds all player data); the page is served by FE — check FPMS health + frontend, with SRE.")
    if "otp" in ck:
        sys_bits.append("OTP/SMS login goes through the SMS server (SRE Backend + Jacob) before FPMS validates the login.")
    if "enter_game" in ck:
        sys_bits.append("Entering a game flows FPMS → CPMS → provider; a launch/transfer failure usually points to CPMS or the provider.")
    if "deposit" in ck:
        sys_bits.append("Deposit redirects to PMS (port 8182, GCASH) then records back to FPMS (port 7100) — check PMS first.")
    if "withdrawal" in ck:
        sys_bits.append("Withdrawal/cash-out is handled by PMS to banks/e-wallets and recorded in FPMS — check PMS + channels.")
    if "promo" in ck:
        sys_bits.append("Promotion/voucher/rebate/LuckyCoin/balance live in FPMS.")
    if "frontend" in ck:
        sys_bits.append("Website/UI/display problems are Frontend (FE) — loop in Frontend SRE.")
    if not sys_bits:
        sys_bits.append("Could not map the symptom to a system automatically — confirm whether it is Backend (SRE → FPMS/CPMS/PMS) or Frontend (FE).")

    special = "none"
    if info.get("all_channels") and ("deposit" in ck or "withdrawal" in ck):
        special = "Withdrawal/Deposit Case A — ALL channels affected → P0 Major, call P0-OM, full P0 process."
    elif info.get("one_provider") and ("deposit" in ck or "withdrawal" in ck):
        special = "Withdrawal/Deposit Case B — one provider / unsure → P1, call on-duty SRE only; if provider issue, end meeting & provider escalation."
    elif info.get("is_otp"):
        special = "OTP case — start meeting, call Jacob + SRE Backend, check SMS logs (Scenario 1 success-our-side / Scenario 2 failed-both)."

    timer = "15 min (no call needed)" if sev == "Other" else ("5 min" if sev.startswith("P0 Major") else "10 min")

    lines = [
        "🔎 Issue Identified / 问题识别",
        f"- Summary: {_one_line(text)}",
        f"- Category: {sev} — {info['reason_en']}",
        f"- 类别: {sev} — {info['reason_zh']}",
        f"- Matched: {cats_en}",
        "- Likely system(s):",
    ]
    lines += [f"  • {b}" for b in sys_bits]
    lines.append(f"- Special case: {special}")
    lines.append("")
    lines.append("🛠️ How to handle / 处理步骤")
    lines.append("1. Start a Lark meeting immediately.")
    lines.append("2. Classify (above) and call per category; coordinate in the meeting.")
    lines.append("3. Contact DEV/SRE on duty individually if support is needed (FE/FPMS/PMS/CPMS order).")
    lines.append(f"- Escalation timer (root cause not found): {timer} → call Greg, Eason, Rock Lim.")
    lines.append("")
    lines.append("📞 Who to call / 需要联系")
    lines += [f"- {w}" for w in _who_to_call(info)]
    lines.append("")
    lines.append("🚨 P0 Incident Overview (copy-paste)")
    lines.append(build_p0_overview(text, info))
    return "\n".join(lines)


def _one_line(text: str, *, max_len: int = 160) -> str:
    one = re.sub(r"\s+", " ", (text or "").strip())
    return one if len(one) <= max_len else one[: max_len - 1] + "…"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

USAGE = (
    "🔎 Identify Issue / 识别问题\n"
    "Paste the player report after the command and I'll classify it (P0 Major / Minor / "
    "P1 / Other), say which systems & teams are involved, how to handle it, and build a "
    "bilingual P0 Incident Overview.\n\n"
    "把玩家反馈贴在命令后面，我会判断严重级别、涉及的系统/部门、处理步骤，并生成中英文 P0 事故概览。\n\n"
    "Example / 示例:\n"
    "/identifyissue Players cannot login to the PC version via password, error prompt.\n"
    "Account ID: 1043930439\n1037672785\n1049901322"
)


# Problem / report-phrasing signals used to auto-detect an issue report even when
# the user never says the words "identify issue".
_PROBLEM_RE = re.compile(
    r"(?i)\b("
    r"unable\s+to|can'?t|cannot|could\s*n'?t|not\s+able\s+to|"
    r"error|errors|failed|failing|fail|stuck|"
    r"not\s+working|doesn'?t\s+work|don'?t\s+work|no\s+response|"
    r"prompt(?:ing|ed)?|pending\s+transaction|missing|"
    r"issue|problem|incident|bug|abnormal|"
    r"无法|不能|不可以|失败|报错|错误|卡住|异常|问题|故障"
    r")\b"
)
_REPORT_PHRASE_RE = re.compile(
    r"(?i)("
    r"please\s+(?:help\s+)?check|kindly\s+(?:help\s+)?check|help\s+(?:us|me)\s+check|"
    r"help\s+check|pls\s+check|reach(?:ed)?\s+out|reported|reporting|complain(?:ed|ing|t)?|"
    r"player\s+(?:said|reported|reached|cannot|can'?t|is\s+unable)|"
    r"encountered|experienc(?:e|ed|ing)|"
    r"玩家(?:反馈|反映|表示|无法|不能|说)|反馈|反映|协助查|帮.{0,4}查"
    r")"
)
_PLAYER_CTX_RE = re.compile(
    r"(?i)\b(player|players|account|accounts|user|users|uid|account\s*id)\b|玩家|账号|帳號|用户"
)


def looks_like_issue_report(text: str) -> bool:
    """Heuristic: is this a player issue/incident report we should auto-analyze?

    Designed for @bot messages. High precision so normal duty/chat is not hijacked:
    a strong report needs a player-account context PLUS a symptom or problem phrasing.
    """
    raw = (text or "").strip()
    if not raw or raw.lstrip().startswith("/"):
        return False
    if not is_enabled():
        return False
    # Don't steal the structured maintenance / credit-check flows.
    try:
        import commandagent as _ca

        if _ca.detect_prod_batch_command(raw) or _ca.detect_checkcredit_command(raw):
            return False
    except Exception:
        pass

    info = classify(raw)
    has_symptom = bool(info.get("categories"))
    has_account_id = bool(extract_account_ids(raw))
    has_player_ctx = has_account_id or bool(_PLAYER_CTX_RE.search(raw))
    has_problem = bool(_PROBLEM_RE.search(raw))
    has_report_phrase = bool(_REPORT_PHRASE_RE.search(raw))

    # 1) Account id + (a symptom or any problem/report wording) → almost certainly a report.
    if has_account_id and (has_symptom or has_problem or has_report_phrase):
        return True
    # 2) No id, but clear player context + a category symptom + problem/report wording.
    if has_player_ctx and has_symptom and (has_problem or has_report_phrase):
        return True
    return False


# --- AI-driven routing (let the model decide, not just regex) --------------

_AI_ROUTER_SYSTEM = (
    "You are the intent router for the CP OM Duty operations bot. Decide whether the user's "
    "message is a PLAYER ISSUE / INCIDENT REPORT that operations should analyze — for example a "
    "player who cannot log in, cannot withdraw or deposit, cannot enter a game, an OTP/SMS "
    "problem, a stuck/pending transaction, an error prompt, or any service problem affecting "
    "players (often with an account id). "
    "Answer NO for casual chat/greetings, thanks, questions about the bot, and for duty/leave/"
    "holiday/machine/jenkins lookups or other slash-command requests. "
    "Reply with ONLY one word: YES or NO."
)


def ai_router_enabled() -> bool:
    """Toggle the LLM intent router (set BOT_ISSUE_AI_ROUTER=0 to use regex only)."""
    return (os.getenv("BOT_ISSUE_AI_ROUTER") or "1").strip().lower() in ("1", "true", "yes", "on")


def ai_is_issue_report(text: str) -> bool:
    """Ask the LLM to decide if this is a player issue report (used for cases regex misses).

    Returns False if AI is unavailable/disabled or on any error — the regex
    ``looks_like_issue_report`` remains the fast, deterministic path.
    """
    raw = (text or "").strip()
    if not raw or raw.lstrip().startswith("/"):
        return False
    if not (is_enabled() and ai_router_enabled() and llm_available()):
        return False
    try:
        import commandagent as _ca

        if _ca.detect_prod_batch_command(raw) or _ca.detect_checkcredit_command(raw):
            return False
    except Exception:
        pass
    reply = _llm_complete(
        _AI_ROUTER_SYSTEM,
        f'Message:\n"""\n{raw}\n"""\nIs this a player issue/incident report? Answer YES or NO.',
        max_tokens=int(os.getenv("BOT_ISSUE_ROUTER_MAX_TOKENS", "5")),
        timeout=float(os.getenv("BOT_ISSUE_ROUTER_TIMEOUT", "15")),
    )
    if not reply:
        return False
    decision = bool(re.search(r"(?i)\byes\b", reply.strip()[:16]))
    print(f"[identifyissue] AI router decision={decision} for {raw[:80]!r} (raw={reply.strip()[:20]!r})", flush=True)
    return decision


def is_issue_report(text: str) -> bool:
    """Combined decision: fast regex first, then let the AI decide the rest."""
    return looks_like_issue_report(text) or ai_is_issue_report(text)


def strip_command(text: str) -> str:
    """Remove a leading /identifyissue (or alias) token, keep the rest of the report."""
    raw = (text or "").strip()
    raw = re.sub(
        r"(?i)^/(?:identifyissue|identify_issue|checkissue|check_issue|issue|whatissue)\b[ \t]*",
        "",
        raw,
    )
    return raw.strip()


def identify_issue(text: str) -> str:
    """Main: return a detailed bilingual issue analysis + P0 overview."""
    body = (text or "").strip()
    if not body:
        return USAGE
    info = classify(body)
    answer, _reasoning, _engine = _analyze(body, info)
    return answer


def _build_user_prompt(body: str, info: dict) -> str:
    players = info.get("players", 0)
    return (
        f"Player report to analyze:\n\"\"\"\n{body}\n\"\"\"\n\n"
        f"Detected player count (use this for Impact scope / 影响范围): "
        f"{players if players else 'unknown — say players TBC'}.\n"
        f"Current incident start time to use: {_now_str()}.\n"
        "Follow the OUTPUT FORMAT exactly (English then 中文, including the copy-paste P0 overview).\n"
        "IMPORTANT: Do NOT include any reasoning, planning, analysis notes, or 'Thinking "
        "Process' text. Start your reply DIRECTLY with the line '🔎 Issue Identified / 问题识别'."
    )


def _clean_ai_answer(content: str) -> str:
    """qwen 'thinking' models sometimes prepend a reasoning trace to the answer.
    Trim everything before the first real output marker (🔎 / Issue Identified)."""
    text = (content or "").strip()
    if not text:
        return text
    idx = text.find("🔎")
    if idx > 0:
        return text[idx:].strip()
    m = re.search(r"(?im)^\s*(?:#+\s*)?(?:\*\*)?\s*Issue\s+Identified\b", text)
    if m and m.start() > 0:
        return text[m.start():].strip()
    # Drop a leading "Thinking Process:" / "Reasoning:" block if the real answer follows.
    m2 = re.search(r"(?is)\b(thinking process|reasoning|analysis)\s*:.*?\n\s*\n(.+)$", text)
    if m2 and len(m2.group(2).strip()) > 80:
        return m2.group(2).strip()
    return text


def _analyze(body: str, info: dict) -> tuple[str, str, str]:
    """Return ``(analysis_text, reasoning, engine)``.

    engine is the model name when the AI produced it, else "rule-based".
    """
    if llm_available():
        raw_answer, reasoning = _llm_complete_full(
            SOP_KNOWLEDGE,
            _build_user_prompt(body, info),
            think=True if show_thinking() else None,
        )
        # qwen 'thinking' models may put the final formatted answer in either the
        # content OR the reasoning field — salvage whichever actually contains it.
        for candidate in (raw_answer, reasoning):
            cleaned = _clean_ai_answer(candidate) if candidate else ""
            if cleaned and ("🔎" in cleaned or "Issue Identified" in cleaned or "P0 Incident" in cleaned):
                return cleaned, reasoning, _llm_model()
    return _deterministic_analysis(body, info), "", "rule-based"


# ---------------------------------------------------------------------------
# Lark interactive card
# ---------------------------------------------------------------------------

def _severity_template(severity: str) -> str:
    s = (severity or "").lower()
    if "major" in s:
        return "red"
    if "minor" in s or s.startswith("p1"):
        return "orange"
    return "blue"


def _div(content: str) -> dict:
    return {"tag": "div", "text": {"tag": "lark_md", "content": content}}


def _note(content: str) -> dict:
    return {"tag": "note", "elements": [{"tag": "lark_md", "content": content}]}


def build_card(text: str) -> tuple[Optional[dict], str]:
    """Build a Lark interactive card for an issue report.

    Returns ``(card_dict, fallback_text)``. ``card_dict`` is None only if the
    input is empty. ``fallback_text`` is the plain-text analysis (sent if the
    card POST fails or for non-card clients).
    """
    body = (text or "").strip()
    if not body:
        return None, USAGE

    info = classify(body)
    analysis, reasoning, engine = _analyze(body, info)
    severity = info.get("severity", "Other")
    used_ai = engine != "rule-based"

    elements: list[dict] = []

    if used_ai:
        # The AI already returns the full structured EN+CN analysis incl. the P0 overview.
        elements.append(_div(analysis))
    else:
        # Render reliable deterministic sections.
        cats_en = ", ".join(info.get("category_en") or []) or "Unclassified"
        sys_lines = "\n".join(f"• {b}" for b in _likely_systems(info))
        special = _special_case_text(info)
        timer = (
            "15 min (no call needed)" if severity == "Other"
            else ("5 min" if severity.startswith("P0 Major") else "10 min")
        )
        elements.append(
            _div(
                f"**🔎 Issue Identified / 问题识别**\n"
                f"- **Summary:** {_one_line(body)}\n"
                f"- **Category / 类别:** {severity}\n"
                f"  - EN: {info['reason_en']}\n"
                f"  - 中: {info['reason_zh']}\n"
                f"- **Matched:** {cats_en}\n"
                f"- **Likely system(s):**\n{sys_lines}\n"
                f"- **Special case:** {special}"
            )
        )
        elements.append({"tag": "hr"})
        elements.append(
            _div(
                "**🛠️ How to handle / 处理步骤**\n"
                "1. Start a Lark meeting immediately. / 立即开 Lark 会议。\n"
                "2. Classify (above) and call per category. / 按类别判级并联系。\n"
                "3. Contact DEV/SRE on duty individually if support is needed "
                "(FE/FPMS/PMS/CPMS order).\n"
                f"- **Escalation timer:** {timer} → call Greg, Eason, Rock Lim."
            )
        )
        elements.append({"tag": "hr"})
        who = "\n".join(f"- {w}" for w in _who_to_call(info))
        elements.append(_div(f"**📞 Who to call / 需要联系**\n{who}"))
        elements.append({"tag": "hr"})
        elements.append(
            _div("**🚨 P0 Incident Overview (copy-paste)**\n" + build_p0_overview(body, info))
        )

    if reasoning and show_thinking():
        think = reasoning.strip()
        if len(think) > 1500:
            think = think[:1500].rstrip() + "…"
        elements.append({"tag": "hr"})
        elements.append(_div(f"**🧠 AI thinking / AI 思考**\n{think}"))

    footer = (
        f"🤖 AI: {engine} · {_now_str()}"
        if used_ai
        else f"⚙️ rule-based (AI offline) · {_now_str()} — admin: check Ollama at {_llm_base_url()}"
    )
    elements.append(_note(footer))

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": _severity_template(severity),
            "title": {"tag": "plain_text", "content": f"🔎 Issue Identified — {severity}"},
        },
        "elements": elements,
    }
    return card, analysis


def _likely_systems(info: dict) -> list[str]:
    ck = set(info.get("categories") or [])
    bits: list[str] = []
    if "login" in ck:
        bits.append("Login depends on FPMS (player data); page served by FE — check FPMS + frontend with SRE.")
    if "otp" in ck:
        bits.append("OTP/SMS goes through the SMS server (SRE Backend + Jacob) before FPMS validates login.")
    if "enter_game" in ck:
        bits.append("Enter game flows FPMS → CPMS → provider; launch/transfer failure points to CPMS or provider.")
    if "deposit" in ck:
        bits.append("Deposit redirects to PMS (port 8182, GCASH) then records back to FPMS (port 7100).")
    if "withdrawal" in ck:
        bits.append("Withdrawal/cash-out handled by PMS to banks/e-wallets, recorded in FPMS — check PMS + channels.")
    if "promo" in ck:
        bits.append("Promotion/voucher/rebate/LuckyCoin/balance live in FPMS.")
    if "frontend" in ck:
        bits.append("Website/UI/display problems are Frontend (FE) — loop in Frontend SRE.")
    if not bits:
        bits.append("Symptom not auto-mapped — confirm Backend (SRE → FPMS/CPMS/PMS) vs Frontend (FE).")
    return bits


def _special_case_text(info: dict) -> str:
    ck = set(info.get("categories") or [])
    if info.get("all_channels") and ("deposit" in ck or "withdrawal" in ck):
        return "Withdrawal/Deposit Case A — ALL channels affected → P0 Major, call P0-OM, full P0 process."
    if info.get("one_provider") and ("deposit" in ck or "withdrawal" in ck):
        return "Withdrawal/Deposit Case B — one provider / unsure → P1, call on-duty SRE only."
    if info.get("is_otp"):
        return "OTP case — start meeting, call Jacob + SRE Backend, check SMS logs (Scenario 1/2)."
    return "none"


def _cli(text: str) -> None:
    print("=" * 70)
    print(f"AI available: {llm_available()}  model={_llm_model()!r}  show_thinking={show_thinking()}")
    print("=" * 70)
    print(identify_issue(text))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        _cli(" ".join(sys.argv[1:]))
    else:
        print(USAGE)
