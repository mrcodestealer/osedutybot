"""/checkerror — AI review of larkbot.service journal ERRORS (what error + what time).

Flow:
  1. ``journalctl -u larkbot.service --since -<window>`` (short-iso timestamps).
  2. Python pre-filter keeps ONLY error entries — ❌ lines, Python tracebacks,
     ``ERROR``/``CRITICAL``/``FATAL``, ``SomethingError:``/``Exception``, ``failed``,
     and systemd crash lines (``Main process exited`` / ``Failed with result``).
     Normal info/debug logs NEVER reach the LLM, so the report can only ever
     talk about service errors.
  3. Known SDK noise is dropped (see ``_noise_patterns`` — chiefly the Lark
     long-connection ``processor not found`` chatter for unsubscribed event
     types, which logs at ERROR tens of thousands of times a day), then
     IDENTICAL errors are grouped into one block with a count + first/last time
     (``group_error_blocks``). Without this a single recurring error floods the
     excerpt and the char cap throws away every other (rarer, real) error.
  4. qwen3.6:35b-a3b (same OpenAI-compatible Ollama endpoint as the rest of the
     bot) turns the grouped excerpt into a short report: one bullet per distinct
     error, with its time(s) and a plain-language explanation.

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
from typing import Any, Optional
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

_TB_HEADER = "Traceback (most recent call last)"
# Continuation lines of a Python traceback (each journal line carries its own
# timestamp prefix; the message part is the traceback text). The first line
# after the frames that is NOT a continuation is the terminal exception line —
# captured unconditionally, so ANY exception class is kept (ReadTimeout,
# RemoteDisconnected, StopIteration, bare Exception, …).
_TB_CONT_RE = re.compile(r'^(?:\s+|File "|\.\.\.)')
# Chained-traceback connectors arrive AFTER the terminal line — keep them so
# ``raise X from Y`` shows as ONE crash, not two unrelated errors.
_TB_CHAIN_RE = re.compile(
    r"^(?:During handling of the above exception|The above exception was the direct cause)"
)
_TB_MAX_LINES = 40

# short-iso: ``2026-07-17T09:15:32+0800 host proc[pid]: message``
_JOURNAL_LINE_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[+\-]\d{2}:?\d{2})?)\s+\S+\s+([^:]*):\s?(.*)$"
)

# Signature normalisation — collapse the volatile parts of a message so that two
# occurrences of the SAME error (differing only by ids/timestamps/counts) share
# one signature and get grouped. Order matters: timestamps → uuids → long hex
# (trace_id/conn_id) → any remaining digit run.
_SIG_TS_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:[+\-]\d{2}:?\d{2})?"
)
_SIG_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_SIG_HEX_RE = re.compile(r"\b[0-9a-fA-F]{8,}\b")
_SIG_NUM_RE = re.compile(r"\d+")
_SIG_WS_RE = re.compile(r"\s+")


def _signature(text: str) -> str:
    s = _SIG_TS_RE.sub("<T>", text or "")
    s = _SIG_UUID_RE.sub("<U>", s)
    s = _SIG_HEX_RE.sub("<H>", s)
    s = _SIG_NUM_RE.sub("N", s)
    return _SIG_WS_RE.sub(" ", s).strip().lower()


def _noise_patterns() -> list[re.Pattern]:
    """Journal lines that log at ERROR level but are NOT the bot failing.

    The Lark long-connection SDK logs every event type the bot never subscribed
    to (VC meetings, task updates, meeting-room status) as
    ``handle message failed … err: processor not found, type: <event>`` — at
    ``[ERROR]``, every few seconds, tens of thousands per day. That is SDK
    chatter, not a service error, so it is hidden from the report. Extend the
    list with ``CHECKERROR_IGNORE`` (one substring/regex per comma or newline).
    """
    pats = [r"processor not found"]
    extra = (os.getenv("CHECKERROR_IGNORE") or "").strip()
    if extra:
        pats.extend(tok.strip() for tok in re.split(r"[,\n]", extra) if tok.strip())
    out: list[re.Pattern] = []
    for p in pats:
        try:
            out.append(re.compile(p, re.I))
        except re.error:
            out.append(re.compile(re.escape(p), re.I))
    return out


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


def extract_error_blocks(journal_text: str) -> tuple[list[dict], int]:
    """Parse the journal into error *blocks* + a count of hidden noise lines.

    A block is one error occurrence: either a single error line, or a whole
    (possibly chained) Python traceback. Each block is
    ``{"first": ts, "last": ts, "lines": [(ts, msg), ...]}``. Known SDK-noise
    lines are dropped and counted separately, never appearing as errors.
    """
    noise_res = _noise_patterns()

    def _is_noise(msg: str) -> bool:
        return any(p.search(msg) for p in noise_res)

    blocks: list[dict] = []
    cur: list[tuple[str, str]] | None = None  # open traceback block
    tb_state: str | None = None  # None | "frames" | "after_terminal"
    tb_lines = 0
    saw_chain = False  # last non-blank line was a "During handling …" connector

    def _flush() -> None:
        nonlocal cur, tb_state, tb_lines, saw_chain
        if cur:
            blocks.append({"first": cur[0][0], "last": cur[-1][0], "lines": cur})
        cur = None
        tb_state = None
        tb_lines = 0
        saw_chain = False

    noise_count = 0
    for raw in (journal_text or "").splitlines():
        line = raw.rstrip()
        m = _JOURNAL_LINE_RE.match(line)
        if not m:  # "-- No entries --", boot markers, unparseable → ends any block
            _flush()
            continue
        ts, _proc, msg = m.group(1), m.group(2), m.group(3)
        stripped = msg.strip()

        if tb_state == "frames":
            if _TB_CONT_RE.match(msg):
                cur.append((ts, msg))
                tb_lines += 1
                if tb_lines >= _TB_MAX_LINES:
                    _flush()
                continue
            # First non-continuation line = the terminal exception line.
            if stripped:
                cur.append((ts, msg))
            tb_state = "after_terminal"
            saw_chain = False
            continue

        if tb_state == "after_terminal":
            if not stripped:
                continue  # blank line between chained tracebacks — keep open
            if _TB_CHAIN_RE.match(stripped):
                cur.append((ts, msg))
                saw_chain = True
                continue  # the following Traceback header extends THIS block
            # A new Traceback header extends the block ONLY as the second half of
            # a chained crash (right after a "During handling …" connector).
            # Otherwise it is a separate error → flush and let the fresh-line
            # handler below start a new block for it.
            if saw_chain and stripped.startswith(_TB_HEADER):
                cur.append((ts, msg))
                tb_state = "frames"
                tb_lines = 0
                saw_chain = False
                continue
            _flush()  # block complete — reprocess this line as a fresh line below

        if _is_noise(msg):
            noise_count += 1
            continue
        if stripped.startswith(_TB_HEADER):
            cur = [(ts, msg)]
            tb_state = "frames"
            tb_lines = 0
            continue
        # ⚠️ lines are "warning, bot continues" in this codebase — even when they
        # quote an exception (e.g. "⚠️ Command agent skipped … TimeoutError()"),
        # the service handled it. /checkerror reports errors only.
        if stripped.startswith("⚠"):
            continue
        if _ERROR_LINE_RE.search(msg):
            blocks.append({"first": ts, "last": ts, "lines": [(ts, msg)]})
    _flush()
    return blocks, noise_count


def group_error_blocks(blocks: list[dict]) -> list[dict]:
    """Collapse identical error blocks → one group with count + first/last time.

    Grouping is by normalised signature (ids/timestamps/numbers stripped), so a
    502 that recurs 400× becomes a single group ``count=400`` instead of 400
    lines flooding the excerpt. Groups are returned in first-seen order.
    """
    groups: dict[str, dict] = {}
    order: list[str] = []
    for b in blocks:
        text = "\n".join(msg for _ts, msg in b["lines"])
        sig = _signature(text)
        g = groups.get(sig)
        if g is None:
            groups[sig] = {
                "count": 1,
                "first": b["first"],
                "last": b["last"],
                "lines": b["lines"],
            }
            order.append(sig)
            continue
        g["count"] += 1
        if b["first"] < g["first"]:
            g["first"] = b["first"]
        if b["last"] > g["last"]:
            g["last"] = b["last"]
    return [groups[s] for s in order]


def format_groups_for_llm(groups: list[dict]) -> tuple[str, int]:
    """Chronological, ``---``-separated blocks under the char cap.

    Each block gets a header the model can copy verbatim:
    ``[×N | first <t> | last <t>]`` for repeats, ``[<t>]`` for a one-off.
    Returns (excerpt, groups_dropped_for_size).
    """
    ordered = sorted(groups, key=lambda g: g["last"])
    cap = _max_excerpt_chars()
    kept: list[str] = []
    dropped = 0
    total = 0
    for g in reversed(ordered):  # budget most-recent groups first
        if g["count"] > 1:
            hdr = f"[×{g['count']} | first {g['first']} | last {g['last']}]"
        else:
            hdr = f"[{g['first']}]"
        body = "\n".join(msg for _ts, msg in g["lines"])
        chunk = f"{hdr}\n{body}"
        total += len(chunk) + 5
        if total > cap and kept:
            dropped += 1
            continue
        kept.append(chunk)
    kept.reverse()
    return "\n---\n".join(kept), dropped


# ---------------------------------------------------------------------------
# ``/log`` — raw journal tail + grep (no LLM)
# ---------------------------------------------------------------------------

_LOG_DEFAULT_WINDOW = "2h"
_LOG_DEFAULT_LINES = 40
_LOG_MAX_LINES = 300
_LOG_MAX_CHARS = 3200  # Lark message budget for the code block

# Credentials that must never be echoed into a chat, even from our own journal.
_SECRET_PATTERNS: tuple[tuple[re.Pattern, str], ...] = (
    (re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._\-]{8,}"), r"\1 ***"),
    (
        re.compile(
            r"(?i)\b(password|passwd|pwd|secret|token|app_secret|api[_-]?key|"
            r"authorization)\b(\s*[=:]\s*|\"\s*:\s*\"?)([^\s,;\"'}\]]{4,})"
        ),
        r"\1\2***",
    ),
    (re.compile(r"\b([ut]_[A-Za-z0-9]{6})[A-Za-z0-9]{10,}"), r"\1***"),  # Lark tokens
)


def _redact_secrets(text: str) -> str:
    out = text or ""
    for pat, repl in _SECRET_PATTERNS:
        out = pat.sub(repl, out)
    return out


def parse_log_args(args_text: str) -> dict[str, Any]:
    """``/log`` arguments → {window, label, lines, pattern}.

    Accepts, in any order before the pattern:
      * a window token — ``30m`` / ``2h`` / ``3d``
      * a line count — ``-n 100``, ``n=100`` or a bare integer
    Everything left over is the grep pattern (regex; falls back to a literal
    substring when it does not compile).
    """
    toks = (args_text or "").split()
    window, label = _parse_window(_LOG_DEFAULT_WINDOW)  # type: ignore[misc]
    lines = _LOG_DEFAULT_LINES
    whole_word = False
    rest: list[str] = []
    i = 0
    while i < len(toks):
        t = toks[i]
        if t in ("-w", "--word") and not rest:
            whole_word = True
            i += 1
            continue
        if t in ("-n", "--lines") and i + 1 < len(toks) and toks[i + 1].isdigit():
            lines = int(toks[i + 1])
            i += 2
            continue
        m_n = re.match(r"^(?:-n|n=|lines=)(\d{1,4})$", t, re.I)
        if m_n:
            lines = int(m_n.group(1))
            i += 1
            continue
        parsed = _parse_window(t)
        if parsed:
            window, label = parsed
            i += 1
            continue
        if t.isdigit() and not rest:  # bare count only before the pattern
            lines = int(t)
            i += 1
            continue
        rest.append(t)
        i += 1
    return {
        "window": window,
        "label": label,
        "lines": max(1, min(_LOG_MAX_LINES, lines)),
        "whole_word": whole_word,
        "pattern": " ".join(rest).strip(),
    }


def _compile_pattern(
    pattern: str, *, whole_word: bool = False
) -> tuple[Optional[re.Pattern], bool]:
    """(compiled, is_regex). Invalid regex degrades to a literal search.

    ``whole_word`` wraps the pattern in ``\\b`` like ``grep -w`` — so ``moved``
    stops matching inside ``Removed``.
    """
    pat = (pattern or "").strip()
    if not pat:
        return None, False
    try:
        return re.compile(rf"\b(?:{pat})\b" if whole_word else pat, re.I), True
    except re.error:
        lit = re.escape(pat)
        return re.compile(rf"\b{lit}\b" if whole_word else lit, re.I), False


# The bot logs every incoming message verbatim ("📝 Original text: '/log …'",
# router/pipeline lines, …). Searching for a term therefore matches the /log
# command that contains it, burying the real hits under echoes of the question.
# Those lines are dropped unless the user is explicitly grepping for "/log".
_SELF_ECHO_RE = re.compile(r"['\"]/log\b")


def filter_log_lines(
    journal_text: str, pattern: str, *, lines: int, whole_word: bool = False
) -> tuple[list[str], int, bool]:
    """(kept_lines_newest_last, total_matched, pattern_was_regex)."""
    rx, is_regex = _compile_pattern(pattern, whole_word=whole_word)
    want_self = "/log" in (pattern or "")
    matched = [
        ln.rstrip()
        for ln in (journal_text or "").splitlines()
        if ln.strip()
        and not ln.startswith("--")
        and (want_self or not _SELF_ECHO_RE.search(ln))
        and (rx is None or rx.search(ln))
    ]
    return matched[-lines:], len(matched), is_regex


def handle_log_command(args_text: str, *, chat_id: str, send_message) -> None:
    """``/log [window] [-n N] [pattern]`` — raw journal tail, optionally grepped."""
    raw = (args_text or "").strip()
    if raw in ("-h", "--help", "help", "?"):
        send_message(
            chat_id,
            "📜 `/log [window] [-n N] [-w] [pattern]`\n"
            f"• `/log` — last {_LOG_DEFAULT_LINES} lines ({_LOG_DEFAULT_WINDOW})\n"
            "• `/log timeout` — lines containing `timeout`\n"
            "• `/log -w moved` — whole word only (skips `Removed`)\n"
            "• `/log 6h -n 100 evo` — last 6h, up to 100 matching lines\n"
            "• pattern is a regex (falls back to plain text if invalid)\n"
            f"Reads the `{_unit()}` journal. Credentials are masked, and the "
            "bot's own `/log` command lines are hidden.",
        )
        return
    opts = parse_log_args(raw)
    ok, journal = _read_journal(opts["window"])
    if not ok:
        send_message(chat_id, f"❌ /log: {journal}")
        return

    kept, total, is_regex = filter_log_lines(
        journal,
        opts["pattern"],
        lines=opts["lines"],
        whole_word=opts["whole_word"],
    )
    pat = opts["pattern"]
    head = f"📜 **{_unit()}** — last {opts['label']}"
    if pat:
        kind = "regex" if is_regex else "text"
        if opts["whole_word"]:
            kind += ", whole word"
        head += f", matching `{pat}` ({kind})"
    if not kept:
        send_message(chat_id, f"{head}\n_No matching lines._")
        return

    body = _redact_secrets("\n".join(kept))
    if len(body) > _LOG_MAX_CHARS:
        body = "…\n" + body[-_LOG_MAX_CHARS:]
    shown = len(kept)
    head += f"\n{total} line(s) matched"
    if shown < total:
        head += f", showing the last {shown}"
    send_message(chat_id, f"{head}\n```\n{body}\n```")


# ---------------------------------------------------------------------------
# LLM review
# ---------------------------------------------------------------------------

_PROMPT = """You are reviewing the systemd journal of `{unit}` (a Python Lark-bot service).
The excerpt below was pre-filtered to ERROR entries only from the last {label}, then IDENTICAL errors were grouped. Each block is ONE distinct error, separated by a line of `---`. The block's first line is a header:
- `[×N | first <t> | last <t>]` — this error occurred N times, between those two times.
- `[<t>]` — it happened once, at that time.

Write a short error report in plain English, one bullet per block, in time order:
- Start the bullet with the time: for a repeated error write `×N, first <t>, last <t>`; for a one-off write `<t>`.
- Then one sentence on what went wrong — name the root cause from the traceback's last line if present.
- Copy times EXACTLY from the block header; never invent or reformat times.
- No preamble, no advice section. At most 25 bullets.

ERROR LOG EXCERPT:
{excerpt}"""


def _strip_think(text: str) -> str:
    text = re.sub(r"(?is)<think>.*?</think>", "", text or "").strip()
    # Drop a leading preamble paragraph ("Based on the log, …") before the first
    # bullet/timestamp, which the model sometimes emits despite the instruction.
    lines = text.splitlines()
    for i, ln in enumerate(lines):
        s = ln.strip().lstrip("`").lstrip()
        if s.startswith(("-", "•", "*", "×")) or re.match(r"^\d{4}-\d{2}-\d{2}", s):
            return "\n".join(lines[i:]).strip() or text
    return text


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

    blocks, noise = extract_error_blocks(journal)
    if not blocks:
        note = f" ({noise} SDK-noise line(s) hidden)" if noise else ""
        send_message(
            chat_id,
            f"✅ No service errors in `{_unit()}` journal in the last {label}.{note}",
        )
        return

    groups = group_error_blocks(blocks)
    excerpt, dropped_groups = format_groups_for_llm(groups)
    occurrences = sum(g["count"] for g in groups)

    head = (
        f"🧾 **{_unit()} errors — last {label}**\n"
        f"{occurrences} error occurrence(s) → {len(groups)} distinct"
    )
    if noise:
        head += f"; {noise} SDK-noise line(s) hidden"
    if dropped_groups:
        head += f"; {dropped_groups} rarer distinct error(s) not shown to AI"

    try:
        report = review_errors_with_llm(excerpt, label=label)
    except Exception as exc:  # noqa: BLE001 — LLM down ≠ command useless
        print(f"[checkerror] LLM review failed: {exc!r}", flush=True)
        raw = excerpt if len(excerpt) <= 3500 else excerpt[-3500:]
        send_message(
            chat_id,
            f"{head}\n⚠️ AI review failed (`{exc}`) — grouped error blocks instead:\n"
            f"```\n{raw}\n```",
        )
        return

    send_message(chat_id, f"{head}\n{report}")
