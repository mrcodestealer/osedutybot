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
    if answers is None and verdict == "unrelated":
        # "unrelated" already says "not an answer to us", and judge() ignores
        # such a bubble whatever answers_us holds. Failing it closed instead
        # made it a retry that is re-asked every sweep and gets the same reply
        # at temperature 0: "@OtherBrand_ops ticket 5521 is being handled" with
        # answers_us omitted held the provider's real window, posted right
        # after it, unfiled for the whole hour.
        answers = False
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


# ---------------------------------------------------------------------------
# Confirm-before-write (VAWATCH_LLM_CONFIRM / PROVIDERASK_LLM_CONFIRM)
# ---------------------------------------------------------------------------
#
# The September 2026 audit measured the regex parser on 1390 fresh, realistic
# messages written to look like notices while meaning something else - a status
# line saying "Ended", 【失效】, "will not be executed", an aggregator's or a
# wallet interface's maintenance, an operator asking the provider to "put our
# games under maintenance" - and about one in five still wrote a window. Each
# was a wording no rule had seen, and adding rules one wording at a time does not
# converge. So a window the parser wants to WRITE is first put to the model as a
# yes/no question. The division of labour above still holds: the model never
# supplies a date or a time - it can only confirm the parser's reading or refuse
# it - and it fails CLOSED: anything but three clear yeses means a person reads
# the notice instead, and nothing is written.

_CONFIRM_SYSTEM = """You check ONE message from a chat group shared between an online casino operator and a game provider. The OPERATOR is us: CasinoPlus (also written CP, Casinoplus, IGO). The PROVIDER is named below, and the message was posted in the PROVIDER's own group with us - so a maintenance notice that does not say whose maintenance it is, is the PROVIDER's. The provider does not need to name itself.

A parser read a maintenance window from the message. Your job is to CONFIRM or REJECT the parser. You never supply dates or times yourself.

Reply with ONLY a JSON object, no prose, no markdown fence:
{"kind": "<k>", "whose": "<w>", "window": "<m>", "confidence": 0.0-1.0, "why": "<15 words>"}

kind - what the message DOES, exactly one of:
  "announcement"           it announces an upcoming or ongoing maintenance or outage as a fact
  "question"               it asks whether there is maintenance, or asks to confirm a time
  "request"                it asks or proposes that a maintenance be done, moved or held at some time
  "cancelled_or_finished"  it cancels, postpones, suspends, voids, withdraws or retracts a maintenance, says it is not needed or not executed, or reports it finished, ended or completed
  "no_impact"              it says players are not affected, zero downtime, games stay available
  "other"                  an acknowledgement, a quote of someone else's notice, a test, a wrong-group message, or anything else

whose - whose maintenance it is, exactly one of:
  "provider"          the PROVIDER's games or platform go down for players (the default for a notice that names no one)
  "operator"          CasinoPlus / CP / IGO's own maintenance
  "third_party"       a bank, payment, wallet, telco, cloud, aggregator or another game studio
  "test_environment"  UAT, staging, test or sandbox only
  "partial"           only a back office, report, API interface, single feature, bonus, customer-service desk, or only some games, lobbies or regions
  "unclear"

window - is the message's window for that maintenance, in GMT+8, the parser's window? exactly one of:
  "same"       the same day, the same start and the same end (check AM/PM, 上午/下午/晚上/凌晨, next-day markers like 次日 or +1, and time zones)
  "different"  a different day or time, or the parser's window is the OLD one of a move
  "unclear"

If you are not sure, choose "unclear" and give a low confidence. A wrong confirmation writes a wrong window into a shared operations sheet; a rejection only asks a person to check."""


def confirm_timeout() -> float:
    try:
        return max(5.0, float(os.getenv("VAWATCH_CONFIRM_TIMEOUT", "60")))
    except ValueError:
        return 60.0


def confirm_min_confidence() -> float:
    try:
        return float(os.getenv("VAWATCH_CONFIRM_MIN_CONFIDENCE", "0.7"))
    except ValueError:
        return 0.7


def confirm_endpoint_problem() -> str:
    """Why the confirm model must not be called, or "".

    The confirm check sends provider messages to the model. The deployment runs
    it on the bot's own box (BOT_CHAT_API_BASE=http://127.0.0.1:11434/v1), but
    with that variable unset chatagent falls back to api.openai.com - so the
    endpoint is checked, not assumed. Only a loopback, private-network or
    single-label host (a docker service name) is used unless
    VAWATCH_CONFIRM_ALLOW_REMOTE=1; otherwise nothing is sent and the notice
    goes to a person.
    """
    if os.getenv("VAWATCH_CONFIRM_ALLOW_REMOTE", "0").strip().lower() in ("1", "true", "yes", "on"):
        return ""
    import ipaddress
    from urllib.parse import urlparse
    host = (urlparse(_base_url()).hostname or "").strip().lower()
    if not host:
        return "no model endpoint is configured"
    if host == "localhost" or "." not in host or host.endswith((".local", ".lan", ".internal")):
        return ""
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_loopback or ip.is_private:
            return ""
    except ValueError:
        pass
    return (f"the model endpoint {host} is not on this server - set BOT_CHAT_API_BASE "
            f"to the local Ollama, or VAWATCH_CONFIRM_ALLOW_REMOTE=1 to allow it")


def confirm_window(text: str, *, provider: str, start, end, names=(),
                   reschedule: bool = False, group: str = "") -> dict:
    """Does ``text`` announce exactly [start, end) as ``provider``'s outage?

    -> {"verdict": "yes" | "no" | "unclear", "transient": bool, "why": str,
        "confidence": float}. "yes" only when the model answers true to all
    three questions with confidence >= VAWATCH_CONFIRM_MIN_CONFIDENCE. A model
    that cannot be reached, times out or answers something unreadable gives
    "unclear" with transient=True - the caller retries it later rather than
    carding it, since a cold model (2-4 minutes to load) is not a verdict.
    """
    out = {"verdict": "unclear", "transient": False, "why": "", "confidence": 0.0}
    body = (text or "").strip()
    if not body or start is None or end is None:
        out["why"] = "nothing to confirm"
        return out
    problem = confirm_endpoint_problem()
    if problem:
        # A configuration fault, not a transient one: carded, never retried
        # into a remote call.
        out["why"] = problem
        return out
    also =[n for n in names if n and n.lower() != (provider or "").lower()]
    user = "\n".join([
        f"Provider: {provider}" + (f" (also written: {', '.join(also[:6])})" if also else ""),
        f"Group: {group}" if group else "",
        "Parser's window: {:%Y-%m-%d %H:%M} to {:%Y-%m-%d %H:%M} (GMT+8){}".format(
            start, end, " - read as a RESCHEDULE: this is the NEW window" if reschedule else ""),
        "", "MESSAGE:", body[:_MAX_MSG_CHARS]])
    try:
        raw = _chat([{"role": "system", "content": _CONFIRM_SYSTEM},
                     {"role": "user", "content": user}],
                    max_tokens=160, timeout=confirm_timeout())
    except Exception as err:  # noqa: BLE001
        out.update(transient=True, why=f"model unreachable: {err!r}"[:200])
        print(f"[providerllm] confirm: {out['why']}", flush=True)
        return out
    obj = _extract_json(raw)
    if not obj:
        out.update(transient=True, why="model reply was not JSON")
        print(f"[providerllm] confirm: unparseable reply {raw[:200]!r}", flush=True)
        return out
    kinds = ("announcement", "question", "request", "cancelled_or_finished", "no_impact", "other")
    whoses = ("provider", "operator", "third_party", "test_environment", "partial", "unclear")
    windows = ("same", "different", "unclear")
    got = {k: str(obj.get(k) or "").strip().lower().replace(" ", "_")
           for k in ("kind", "whose", "window")}
    try:
        conf = max(0.0, min(1.0, float(obj.get("confidence", 0.0))))
    except (TypeError, ValueError):
        conf = 0.0
    why = str(obj.get("why") or "")[:160]
    out.update(confidence=conf, why=why)
    if got["kind"] not in kinds or got["whose"] not in whoses or got["window"] not in windows:
        out.update(transient=True, why=f"unreadable answer {obj!r}"[:200])
        return out
    ok = got["kind"] == "announcement" and got["whose"] == "provider" and got["window"] == "same"
    if ok and conf >= confirm_min_confidence():
        out["verdict"] = "yes"
        return out
    reasons = []
    if got["kind"] != "announcement":
        reasons.append("the message is a {} , not an announcement".format(
            got["kind"].replace("_", " ")).replace(" ,", ","))
    if got["whose"] != "provider":
        reasons.append("it is not this provider's outage ({})".format(got["whose"].replace("_", " ")))
    if got["window"] != "same":
        reasons.append("the window is not the one the parser read ({})".format(got["window"]))
    out["verdict"] = "no" if (got["kind"] != "announcement" or got["whose"] not in ("provider", "unclear")
                              or got["window"] == "different") else "unclear"
    out["why"] = "; ".join(filter(None, [
        ("the model says: " + ", ".join(reasons)) if reasons else
        "the model is not confident ({:.2f})".format(conf), why]))
    return out


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
