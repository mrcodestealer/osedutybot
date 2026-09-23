#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ask qwen3.6:35b-a3b the one question regex cannot: is this a reply to US?

A provider group carries ticket acknowledgements, API support answers and
ordinary chatter alongside the one message that answers our weekly
"any maintenance plans for this week?". Keyword matching cannot tell them apart —
the Pragmatic Play transcript has two long, fluent, entirely irrelevant support
answers between our question and Natalia's actual reply.

DIVISION OF LABOUR, and it is deliberate:
  * the LLM decides RELEVANCE      — is this addressed to us, and what does it say
  * ``noticeparse`` decides the TIMES — parsed from the message text by regex

The model never supplies a date. A hallucinated timestamp written into a shared
operational sheet is the worst failure this feature could have, and this split
makes it structurally impossible: whatever the model says, the window still has
to be found in the provider's own words.

FAILS CLOSED. ``newportwatch.is_new_activity`` deliberately fails OPEN so a
swapped model cannot mute its alerts; here the consequence of a wrong "yes" is a
wrong write to the sheet, so an unavailable or undecided model yields
"unclear" and the caller leaves the row alone and reports it for a human.

Production notes (from deploy/ollama.service.d/override.conf): the model is
local Ollama on the bot's own box, CPU backend, OLLAMA_NUM_PARALLEL=1 (so these
calls serialise) and OLLAMA_CONTEXT_LENGTH=8192 (so transcripts are trimmed).
A cold load is 2-4 minutes, warm inference ~4s — hence ``warmup()``.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Optional

import requests

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

def _int_env(name: str, default: int) -> int:
    """Never raise at import. A typo in one env value would otherwise make
    `import providerllm` throw - inside judge(), inside the sweep, on the
    Telegram worker - and take the whole collection down."""
    try:
        return max(200, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        print(f"[providerllm] {name} is not a number; using {default}", flush=True)
        return default


# Keep well inside OLLAMA_CONTEXT_LENGTH=8192 once the prompt is added.
_MAX_MSG_CHARS = _int_env("PROVIDERASK_MAX_MSG_CHARS", 2500)
_MAX_QUOTE_CHARS = _int_env("PROVIDERASK_MAX_QUOTE_CHARS", 2500)


def model() -> str:
    """The classifier model. Pinned separately so it can be changed alone."""
    name = (os.getenv("PROVIDERASK_MODEL") or "").strip()
    if name:
        return name
    try:
        import chatagent as ca

        return ca.shared_llm_model()
    except Exception:
        return "qwen3.6:35b-a3b"


def _base_url() -> str:
    try:
        import chatagent as ca

        return ca._llm_base_url()
    except Exception:
        return (os.getenv("BOT_CHAT_API_BASE") or "http://127.0.0.1:11434/v1").rstrip("/")


def _api_key() -> str:
    try:
        import chatagent as ca

        return ca._llm_api_key()
    except Exception:
        return os.getenv("BOT_CHAT_API_KEY") or "ollama"


def _timeout() -> float:
    """Its own timeout: the shared BOT_CHAT_LLM_TIMEOUT default of 30s is far
    below a cold 35B load (2-4 minutes) and would fail every first call."""
    try:
        return float(os.getenv("PROVIDERASK_TIMEOUT", "600"))
    except ValueError:
        return 600.0


_SYSTEM = """You triage messages in a chat group shared with a game provider.

Our team posted this question in the group:
    "Hi team, are there any maintenance plans for this week?"

For ONE later message, decide whether it ANSWERS that question.

Reply with ONLY a JSON object, no prose, no markdown fence:
{"answers_us": true|false, "verdict": "<v>", "confidence": 0.0-1.0, "why": "<12 words>"}

verdict must be exactly one of:
  "no_maintenance"  the message says there is no maintenance planned
  "maintenance"     the message announces or points to a maintenance window
  "unrelated"       the message is about something else entirely
  "unclear"         it may be about maintenance but you cannot tell what it says

Rules:
- Ticket acknowledgements ("ticket 12345 was created"), API/error support answers,
  documentation links and ordinary chatter are "unrelated", answers_us false —
  even when they are long, polite and well written.
- A message that merely points at another message ("please refer to this
  notification") IS answering us: verdict "maintenance", answers_us true.
- A notice saying a maintenance has been COMPLETED or CANCELLED is "unrelated".
- A message announcing a RESCHEDULED maintenance is "maintenance".
- Do NOT extract dates or times. Never invent them. Something else reads those.
- Do NOT judge whether a date is in the past or the future, and do not let the
  date change your answer. A notice that announces a maintenance window is
  "maintenance" whenever it is dated. Staleness is decided elsewhere.
- If you are unsure, use "unclear" with a low confidence. Guessing is worse."""


def _extract_json(text: str) -> dict:
    """First JSON object in the reply. Thinking models like to add prose."""
    if not text:
        return {}
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.S)
    if fence:
        text = fence.group(1)
    start = text.find("{")
    while start != -1:
        depth, i = 0, start
        while i < len(text):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        obj = json.loads(text[start:i + 1])
                        if isinstance(obj, dict):
                            return obj
                    except Exception:
                        break
            i += 1
        start = text.find("{", start + 1)
    return {}


def _chat(messages: list, *, max_tokens: int = 220,
          timeout: Optional[float] = None) -> str:
    payload: dict[str, Any] = {
        "model": model(),
        "messages": messages,
        "temperature": 0,
        "max_tokens": max_tokens,
    }
    try:
        import chatagent as ca

        payload = ca.enrich_ollama_chat_payload(payload, think=False)
    except Exception:
        # Without this the /v1 endpoint ignores "think" and a qwen3.x model
        # reasons at full effort on every call - hundreds of hidden tokens.
        payload["reasoning_effort"] = "none"
        payload["keep_alive"] = -1
    resp = requests.post(
        f"{_base_url().rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {_api_key()}",
                 "Content-Type": "application/json"},
        json=payload, timeout=timeout or _timeout(),
    )
    data = resp.json()
    return (((data.get("choices") or [{}])[0].get("message") or {})
            .get("content") or "")


def warmup() -> bool:
    """Force the model resident before the first real classification.

    A cold 35B load is 2-4 minutes on this box. Without this the first provider
    in the sweep pays for it and can time out while the rest run in ~4s each.
    Non-fatal: a failure here only means the first real call is slow.
    """
    try:
        _chat([{"role": "user", "content": "reply with OK"}],
              max_tokens=5, timeout=_timeout())
        print(f"[providerllm] {model()} warm", flush=True)
        return True
    except Exception as err:  # noqa: BLE001
        print(f"[providerllm] warmup failed (first call will be slow): {err!r}",
              flush=True)
        return False


def available() -> bool:
    try:
        import chatagent as ca

        return bool(ca.llm_available())
    except Exception:
        return bool(_base_url())


def _answers_us(value) -> Optional[bool]:
    """The model's answers_us field -> True / False, or None when unreadable.

    ``bool(obj.get("answers_us"))`` read the STRING "false" as True - any
    non-empty string is truthy - so a model that quoted its boolean turned
    "@BrandX_ops No maintenance for BrandX this week" into an answer to US, and
    judge() blanked our row on another operator's say-so. Only a real JSON
    boolean or the exact word "true" counts as yes; "false" / "no" / "0" are
    no; anything else (missing, "yes", 1, "maybe") is None, which the caller
    turns into ok=False - fail closed, the row is left alone for a human.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        word = value.strip().lower()
        if word == "true":
            return True
        if word in ("false", "no", "0"):
            return False
        return None
    if isinstance(value, int) and value == 0:
        return False
    return None


def classify_reply(text: str, *, group: str = "", quoted_text: str = "",
                   asked_at: str = "", sender: str = "") -> dict:
    """Is this message an answer to our weekly ask?

    Returns ``{answers_us, verdict, confidence, why, ok}``. ``ok`` is False when
    the model could not be reached or its reply could not be parsed — the caller
    must treat that as "do not touch the row", never as "no maintenance".
    """
    out = {"answers_us": False, "verdict": "unclear", "confidence": 0.0,
           "why": "", "ok": False}
    body = (text or "").strip()
    if not body:
        out["why"] = "empty message"
        return out

    parts = [f"Group: {group}" if group else "",
             f"We asked at: {asked_at}" if asked_at else "",
             f"Message from: {sender}" if sender else "",
             "", "MESSAGE:", body[:_MAX_MSG_CHARS]]
    if quoted_text.strip():
        parts += ["", "THIS MESSAGE QUOTES:", quoted_text.strip()[:_MAX_QUOTE_CHARS]]
    user = "\n".join(p for p in parts if p is not None)

    try:
        raw = _chat([{"role": "system", "content": _SYSTEM},
                     {"role": "user", "content": user}])
    except Exception as err:  # noqa: BLE001
        out["why"] = f"model unreachable: {err!r}"
        print(f"[providerllm] {out['why']}", flush=True)
        return out

    obj = _extract_json(raw)
    if not obj:
        out["why"] = "model reply was not JSON"
        print(f"[providerllm] unparseable reply: {raw[:200]!r}", flush=True)
        return out

    verdict = str(obj.get("verdict") or "").strip().lower()
    if verdict not in ("no_maintenance", "maintenance", "unrelated", "unclear"):
        out["why"] = f"unknown verdict {verdict!r}"
        return out
    answers = _answers_us(obj.get("answers_us"))
    if answers is None:
        out["why"] = f"unreadable answers_us {obj.get('answers_us')!r}"
        print(f"[providerllm] {out['why']}", flush=True)
        return out
    try:
        conf = float(obj.get("confidence", 0.0))
    except (TypeError, ValueError):
        conf = 0.0
    out.update(ok=True, verdict=verdict,
               answers_us=answers,
               confidence=max(0.0, min(1.0, conf)),
               why=str(obj.get("why") or "")[:120])
    return out


def min_confidence() -> float:
    try:
        return float(os.getenv("PROVIDERASK_MIN_CONFIDENCE", "0.6"))
    except ValueError:
        return 0.6


def main(argv: Optional[list] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Provider reply triage")
    ap.add_argument("--warmup", action="store_true")
    ap.add_argument("--file", help="classify the text in this file")
    ap.add_argument("--group", default="")
    args = ap.parse_args(argv)

    print(f"model   : {model()}")
    print(f"base    : {_base_url()}")
    print(f"timeout : {_timeout()}s")
    if args.warmup:
        return 0 if warmup() else 1
    if args.file:
        text = open(args.file, encoding="utf-8").read()
        print(json.dumps(classify_reply(text, group=args.group),
                         ensure_ascii=False, indent=2))
        return 0
    ap.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
