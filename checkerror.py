"""/checkerror — AI review of larkbot.service journal ERRORS (what error + what time).

Flow:
  1. ``journalctl -u larkbot.service --since -<window>`` (short-iso timestamps).
  2. Python pre-filter keeps ONLY error entries — ❌ lines, Python tracebacks,
     ``ERROR``/``CRITICAL``/``FATAL``, ``SomethingError:``/``Exception``, ``failed``,
     and systemd crash lines (``Main process exited`` / ``Failed with result``).
     Normal info/debug logs NEVER reach the LLM, so the report can only ever
     talk about service errors.
  3. qwen3.6:35b-a3b (same OpenAI-compatible Ollama endpoint as the rest of the
     bot) turns the excerpt into a short report: one line per distinct error,
     with the log timestamp(s) and a plain-language explanation.

qwen3.6:35b-a3b is a *thinking* model — same handling as changePrefix.py:
``reasoning_effort="none"`` + ``think=False`` on Ollama, else it burns every
token reasoning and returns empty content. Cold-loading 35B on the CPU box can
take minutes, hence the long default timeout.

If the LLM call fails the command still answers with the raw extracted error
lines (truncated), so /checkerror is useful even when Ollama is down.
"""

from __future__ import annotations

import os
import re
import subprocess

import requests

# ---------------------------------------------------------------------------
# Config (env-overridable; defaults match the prod box)
# ---------------------------------------------------------------------------


def _unit() -> str:
    return (
        os.getenv("CHECKERROR_UNIT")
        or os.getenv("LARKBOT_SYSTEMD_UNIT")
        or "larkbot.service"
    ).strip() or "larkbot.service"


def _model() -> str:
    # Pinned to qwen3.6:35b-a3b per requirement — override only via the explicit
    # CHECKERROR_MODEL env, never falling back to another chat model.
    return (os.getenv("CHECKERROR_MODEL") or "qwen3.6:35b-a3b").strip()


def _api_base() -> str:
    return (
        os.getenv("CHECKERROR_API_BASE")
        or os.getenv("BOT_CHAT_API_BASE")
        or "http://127.0.0.1:11434/v1"
    ).strip().rstrip("/")


def _api_key() -> str:
    return (
        os.getenv("CHECKERROR_API_KEY")
        or os.getenv("BOT_CHAT_API_KEY")
        or os.getenv("OPENAI_API_KEY")
        or "ollama"
    ).strip()


def _is_ollama(base: str) -> bool:
    low = (base or "").lower()
    return "11434" in low or "ollama" in low


def _ollama_keep_alive():
    ka = (
        os.getenv("CHECKERROR_KEEP_ALIVE")
        or os.getenv("BOT_CHAT_OLLAMA_KEEP_ALIVE")
        or "-1"
    ).strip()
    try:
        return int(ka)
    except ValueError:
        return ka


def _llm_timeout() -> int:
    # Cold-loading the 35B model on the CPU box (often deep into swap) can take
    # minutes — same lesson as PLDT_CAPTCHA_WARMUP_TIMEOUT.
    try:
        return max(30, int(os.getenv("CHECKERROR_TIMEOUT", "600")))
    except ValueError:
        return 600


def _max_excerpt_chars() -> int:
    try:
        return max(1000, int(os.getenv("CHECKERROR_MAX_CHARS", "9000")))
    except ValueError:
        return 9000


_DEFAULT_WINDOW = "24h"
_WINDOW_RE = re.compile(r"^(\d{1,4})\s*(m|min|h|hr|d|day)s?$", re.I)


def _parse_window(raw: str) -> tuple[str, str] | None:
    """``24h``/``90m``/``3d`` → (journalctl --since value, human label)."""
    m = _WINDOW_RE.match((raw or "").strip().lower())
    if not m:
        return None
    n = int(m.group(1))
    if n <= 0:
        return None
    suffix = {"m": "m", "min": "m", "h": "h", "hr": "h", "d": "d", "day": "d"}[
        m.group(2)
    ]
    return f"-{n}{suffix}", f"{n}{suffix}"


# ---------------------------------------------------------------------------
# Journal read + error-only extraction
# ---------------------------------------------------------------------------

# A journal line is an error entry when its MESSAGE matches one of these.
# ⚠️-only warnings deliberately do not match: the command reports errors only.
# The Error/Exception branch has an OPTIONAL module/class prefix so bare
# ``Exception: msg`` / ``Error: …`` / ``Exception in thread T:`` also match.
_ERROR_LINE_RE = re.compile(
    r"(❌"
    r"|Traceback \(most recent call last\)"
    r"|\b(?:ERROR|CRITICAL|FATAL)\b"
    r"|\b(?:[A-Za-z_][A-Za-z_0-9.]*)?(?:Error|Exception)\b"
    r"|(?i:\bfailed\b|\bfailure\b)"
    r"|Main process exited"
    r")"
)

# Continuation lines of a Python traceback (each journal line carries its own
# timestamp prefix; the message part is the traceback text). The first line
# after the frames that is NOT a continuation is the terminal exception line —
# captured unconditionally, so ANY exception class is kept (ReadTimeout,
# RemoteDisconnected, StopIteration, bare Exception, …).
_TB_CONT_RE = re.compile(r'^(?:\s+|File "|\.\.\.)')
# Chained-traceback connectors arrive AFTER the terminal line ended the block —
# keep them so ``raise X from Y`` shows as ONE crash, not two unrelated errors.
_TB_CHAIN_RE = re.compile(
    r"^(?:During handling of the above exception|The above exception was the direct cause)"
)
_TB_MAX_LINES = 40

# short-iso: ``2026-07-17T09:15:32+0800 host proc[pid]: message``
_JOURNAL_LINE_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[+\-]\d{2}:?\d{2})?)\s+\S+\s+([^:]*):\s?(.*)$"
)


def _read_journal(since: str) -> tuple[bool, str]:
    """Run journalctl; returns (ok, output-or-error-message)."""
    unit = _unit()
    cmd = [
        "journalctl",
        "-u",
        unit,
        "--since",
        since,
        "--no-pager",
        "-o",
        "short-iso",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except FileNotFoundError:
        return False, (
            "`journalctl` not found — /checkerror reads the systemd journal, "
            "so it only works on the server (not on a Windows/dev machine)."
        )
    except Exception as exc:  # noqa: BLE001
        return False, f"journalctl failed to run: {exc!r}"
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        return False, f"journalctl exited {proc.returncode}: {err[:400]}"
    return True, proc.stdout or ""


def extract_error_entries(journal_text: str) -> list[str]:
    """Keep only error lines (with their timestamps) + full traceback blocks."""
    entries: list[str] = []
    in_traceback = False
    tb_lines = 0
    for raw in (journal_text or "").splitlines():
        line = raw.rstrip()
        if not line or line.startswith("--"):  # "-- No entries --", boot markers
            in_traceback = False
            continue
        m = _JOURNAL_LINE_RE.match(line)
        if not m:
            continue
        ts, _proc, msg = m.group(1), m.group(2), m.group(3)
        if in_traceback:
            if _TB_CONT_RE.match(msg):
                entries.append(f"{ts} {msg}")
                tb_lines += 1
                if tb_lines >= _TB_MAX_LINES:
                    in_traceback = False
                continue
            # First non-continuation line = the terminal exception line.
            in_traceback = False
            if msg.strip():
                entries.append(f"{ts} {msg}")
                continue
        if _TB_CHAIN_RE.match(msg):
            entries.append(f"{ts} {msg}")
            continue
        # ⚠️ lines are "warning, bot continues" in this codebase — even when they
        # quote an exception (e.g. "⚠️ Command agent skipped … TimeoutError()"),
        # the service handled it. /checkerror reports errors only.
        if msg.lstrip().startswith("⚠"):
            continue
        if _ERROR_LINE_RE.search(msg):
            entries.append(f"{ts} {msg}")
            if "Traceback (most recent call last)" in msg:
                in_traceback = True
                tb_lines = 0
    return entries


def _cap_excerpt(entries: list[str]) -> tuple[str, int]:
    """Most recent entries first-priority: keep the tail under the char cap."""
    cap = _max_excerpt_chars()
    kept: list[str] = []
    total = 0
    for line in reversed(entries):
        total += len(line) + 1
        if total > cap and kept:
            break
        kept.append(line)
    kept.reverse()
    return "\n".join(kept), len(entries) - len(kept)


# ---------------------------------------------------------------------------
# LLM review
# ---------------------------------------------------------------------------

_PROMPT = """You are reviewing the systemd journal of `{unit}` (a Python Lark-bot service).
The excerpt below was pre-filtered: it contains ONLY error lines and tracebacks from the last {label} — all normal logs were removed.

Write a short error report in plain English:
- One bullet per DISTINCT error: `<time from the log>` — what went wrong, in one sentence (name the root cause if the traceback shows it).
- Merge repeats of the same error into one bullet: `xN, first <time>, last <time>`.
- Use timestamps exactly as they appear in the log; never invent times.
- No preamble, no advice section, at most 20 bullets.

ERROR LOG EXCERPT:
{excerpt}"""


def _strip_think(text: str) -> str:
    return re.sub(r"(?is)<think>.*?</think>", "", text or "").strip()


def review_errors_with_llm(excerpt: str, *, label: str) -> str:
    """Ask the model for the when+what report. Raises on any failure."""
    base = _api_base()
    payload = {
        "model": _model(),
        "messages": [
            {
                "role": "user",
                "content": _PROMPT.format(unit=_unit(), label=label, excerpt=excerpt),
            }
        ],
        "max_tokens": 900,
        "temperature": 0,
    }
    if _is_ollama(base):
        # Thinking model — see module docstring / changePrefix.py.
        payload["reasoning_effort"] = "none"
        payload["think"] = False
        payload["keep_alive"] = _ollama_keep_alive()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_api_key()}",
    }
    resp = requests.post(
        f"{base}/chat/completions", headers=headers, json=payload,
        timeout=_llm_timeout(),
    )
    resp.raise_for_status()
    body = resp.json()
    choices = body.get("choices") or []
    if not choices:
        raise RuntimeError(f"no choices in LLM response: {str(body)[:300]}")
    content = (choices[0].get("message") or {}).get("content") or ""
    if isinstance(content, list):  # some backends return content parts
        content = "".join(
            part.get("text", "") for part in content if isinstance(part, dict)
        )
    text = _strip_think(str(content))
    if not text:
        raise RuntimeError("LLM returned empty content")
    return text


# ---------------------------------------------------------------------------
# Command entry point (called from main.py in a daemon thread)
# ---------------------------------------------------------------------------


def handle_checkerror_command(args_text: str, *, chat_id: str, send_message) -> None:
    arg = (args_text or "").strip()
    parsed = _parse_window(arg) if arg else _parse_window(_DEFAULT_WINDOW)
    if parsed is None:
        send_message(
            chat_id,
            "❌ Usage: `/checkerror [window]` — window like `6h`, `24h`, `3d` "
            f"(default {_DEFAULT_WINDOW}).\n"
            f"Reviews **errors only** from the `{_unit()}` journal with "
            f"`{_model()}`: what error happened and at what time.",
        )
        return
    since, label = parsed

    send_message(
        chat_id,
        f"🔍 Reading `{_unit()}` journal (last {label}) — errors only — then asking "
        f"`{_model()}` to review… (first run may take minutes while the model loads)",
    )

    ok, journal = _read_journal(since)
    if not ok:
        send_message(chat_id, f"❌ /checkerror: {journal}")
        return

    entries = extract_error_entries(journal)
    if not entries:
        send_message(
            chat_id,
            f"✅ No errors in `{_unit()}` journal in the last {label}.",
        )
        return

    excerpt, dropped = _cap_excerpt(entries)
    head = f"🧾 **{_unit()} errors — last {label}** ({len(entries)} error line(s)"
    head += f", oldest {dropped} not shown to AI)" if dropped else ")"

    try:
        report = review_errors_with_llm(excerpt, label=label)
    except Exception as exc:  # noqa: BLE001 — LLM down ≠ command useless
        print(f"[checkerror] LLM review failed: {exc!r}", flush=True)
        raw_tail = excerpt[-3500:]
        send_message(
            chat_id,
            f"{head}\n⚠️ AI review failed (`{exc}`) — raw error lines instead:\n"
            f"```\n{raw_tail}\n```",
        )
        return

    send_message(chat_id, f"{head}\n{report}")
