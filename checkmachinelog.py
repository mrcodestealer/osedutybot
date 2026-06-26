#!/usr/bin/env python3
"""
Machine logic log — detect errors, last player, last error (±10 line context).

Log source matches **checkcredit** (OSS HTTP by default; LogNavigator when
``CHECKCREDIT_USE_NAVIGATOR=1`` / ``CHECKCREDIT_USE_OSS=0``).

  python3 checkmachinelog.py 2074 --date 2026-06-26
  python3 checkmachinelog.py NCH1422 --date 2026-04-27 --no-ai
  python3 checkmachinelog.py CP0231 --navigator

Env (optional):
  CHECKMACHINELOG_AI_TAIL_LINES — lines from log end for AI (default 200)
  CHECKMACHINELOG_CTX_BEFORE / CHECKMACHINELOG_CTX_AFTER — error context (default 10 each)
  CHECKMACHINELOG_USE_AI — 0/false to skip LLM even when API key is set
  Same OSS / LogNavigator vars as checkcredit (``.env`` auto-loaded).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import date, datetime
from typing import Any, Optional

_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(_ROOT_DIR, ".env"))
except ImportError:
    pass

import checkcredit as cc


def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(name, str(default)).strip() or str(default)))
    except ValueError:
        return default


def _env_truthy(name: str, *, default: bool = True) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def ai_tail_line_count() -> int:
    return max(1, _env_int("CHECKMACHINELOG_AI_TAIL_LINES", 200))


def ctx_before_lines() -> int:
    return _env_int("CHECKMACHINELOG_CTX_BEFORE", 10)


def ctx_after_lines() -> int:
    return _env_int("CHECKMACHINELOG_CTX_AFTER", 10)


def use_ai_summary() -> bool:
    return _env_truthy("CHECKMACHINELOG_USE_AI", default=True)


def tail_log_lines(log_text: str, *, max_lines: int | None = None) -> list[str]:
    lines = (log_text or "").splitlines()
    n = max_lines if max_lines is not None else ai_tail_line_count()
    if len(lines) <= n:
        return lines
    return lines[-n:]


def enrich_error_context(
    raw_lines: list[str],
    errors: list[dict[str, Any]],
    *,
    before: int | None = None,
    after: int | None = None,
) -> None:
    """In-place: set context_lines to ±before/after around each error line."""
    b = ctx_before_lines() if before is None else max(0, int(before))
    a = ctx_after_lines() if after is None else max(0, int(after))
    for e in errors:
        li = int(e.get("line_idx", -1))
        if li < 0:
            continue
        start = max(0, li - b)
        end = min(len(raw_lines), li + a + 1)
        ctx: list[str] = []
        for gi in range(start, end):
            marker = ">>" if gi == li else "  "
            ctx.append(f"{marker} {raw_lines[gi]}")
        e["context_lines"] = ctx


def enrich_merged_context(raw_lines: list[str], merged: list[dict[str, Any]]) -> None:
    for row in merged:
        enrich_error_context(raw_lines, row.get("errors") or [])


def _split_player_blocks(raw_lines: list[str]) -> list[tuple[str, list[tuple[int, str]]]]:
    """Same block boundaries as ``checkcredit.parse_user_blocks_full``."""
    blocks: list[tuple[str, list[tuple[int, str]]]] = []
    cur_uid: str | None = None
    cur_lines: list[tuple[int, str]] = []
    for i, line in enumerate(raw_lines):
        um = cc._USERID_START.search(line)
        if um:
            if cur_uid is not None:
                blocks.append((cur_uid, cur_lines))
            cur_uid = um.group(1)
            cur_lines = [(i, line)]
        elif cur_uid is not None:
            cur_lines.append((i, line))
    if cur_uid is not None:
        blocks.append((cur_uid, cur_lines))
    return blocks


def _last_block_lines_for_uid(
    raw_lines: list[str], uid: str | None
) -> list[tuple[int, str]]:
    if not uid:
        return []
    want = str(uid).strip()
    last: list[tuple[int, str]] = []
    for block_uid, blines in _split_player_blocks(raw_lines):
        if block_uid == want:
            last = blines
    return last


def _line_context(
    raw_lines: list[str],
    line_idx: int,
    *,
    before: int | None = None,
    after: int | None = None,
    highlight: bool = True,
) -> list[str]:
    b = ctx_before_lines() if before is None else max(0, int(before))
    a = ctx_after_lines() if after is None else max(0, int(after))
    start = max(0, line_idx - b)
    end = min(len(raw_lines), line_idx + a + 1)
    out: list[str] = []
    for gi in range(start, end):
        if highlight:
            marker = ">>" if gi == line_idx else "  "
            out.append(f"{marker} {raw_lines[gi]}")
        else:
            out.append(raw_lines[gi])
    return out


def extract_transfer_out(row: dict[str, Any] | None) -> dict[str, Any]:
    """Last resolved credit for a player (cur_coin / reduce_num / enter_game / aft)."""
    if not row:
        return {"amount": None, "time": None, "source": None, "line_idx": -1}
    lc = row.get("latest_credit")
    if not isinstance(lc, dict):
        return {"amount": None, "time": None, "source": None, "line_idx": -1}
    return {
        "amount": lc.get("value"),
        "time": (lc.get("time_short") or "").strip() or None,
        "source": (lc.get("source") or "").strip() or None,
        "line_idx": int(lc.get("line_idx", -1)),
    }


def find_last_success_line(
    raw_lines: list[str], uid: str | None
) -> dict[str, Any] | None:
    """Last ``successJson`` line with ``error: 0`` in the player's final log block."""
    blines = _last_block_lines_for_uid(raw_lines, uid)
    if not blines:
        return None
    best: dict[str, Any] | None = None
    for line_idx, line in blines:
        if "successJson" not in line:
            continue
        if not cc._ERR_ZERO.search(line):
            continue
        coin = cc._CUR_COIN.search(line)
        amount = None
        if coin:
            try:
                amount = float(coin.group(1))
            except ValueError:
                amount = None
        best = {
            "line_idx": line_idx,
            "full_line": line.rstrip(),
            "time": cc._line_time_prefix(line) or None,
            "cur_coin": amount,
        }
    if not best:
        return None
    best["context_lines"] = _line_context(raw_lines, int(best["line_idx"]))
    return best


def _row_for_uid(merged: list[dict[str, Any]], uid: str | None) -> dict[str, Any] | None:
    if not uid:
        return None
    want = str(uid).strip()
    for row in merged:
        if str(row.get("user_id", "")).strip() == want:
            return row
    return None


def _format_transfer_out_line(transfer: dict[str, Any], *, uid: str | None) -> str:
    amt = transfer.get("amount")
    ts = transfer.get("time") or "n/a"
    src = transfer.get("source") or "n/a"
    if amt is None:
        return f"**Last player transfer-out credit:** `n/a` (User `{uid or 'n/a'}` — no credit line parsed)"
    src_note = {
        "cur_coin": "successJson cur_coin",
        "reduce_num": "reduce_num",
        "enter_game_target": "enter_game target/add_num",
        "aft_interrogation_faild_amount": "aft interrogation amount",
    }.get(str(src), str(src))
    return (
        f"**Last player transfer-out credit:** `{amt}` @ `{ts}` "
        f"(User `{uid or 'n/a'}`, from `{src_note}`)"
    )


def format_success_context_block(success: dict[str, Any] | None, *, uid: str | None) -> str:
    if not success:
        return (
            f"**Success log:** (none parsed for User `{uid or 'n/a'}` — no successJson with error 0)\n"
        )
    t = (success.get("time") or "").strip() or "n/a"
    coin = success.get("cur_coin")
    coin_s = "n/a" if coin is None else str(coin)
    ctx = success.get("context_lines") or []
    body = "\n".join(ctx) if ctx else (success.get("full_line") or "").strip() or "(no line)"
    b, a = ctx_before_lines(), ctx_after_lines()
    return (
        f"**Last success log** (no errors in log)\n"
        f"- User ID: `{uid or 'n/a'}`\n"
        f"- Time: `{t}`\n"
        f"- cur_coin: `{coin_s}`\n"
        f"- Context ({b} lines above + line + {a} below):\n"
        f"```\n{body}\n```\n"
    )


def _pick_latest_error(merged: list[dict[str, Any]]) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    best_li = -1
    for row in merged:
        for e in row.get("errors") or []:
            li = int(e.get("line_idx", -1))
            if li > best_li:
                best_li = li
                best = {**e, "user_id": row.get("user_id")}
    return best


_LOG_AI_SYSTEM = (
    "You analyze the tail of a casino EGM logic log file.\n"
    "User blocks start with extra1/extra2/extra3 ... userid: <digits> or enter_game userid: <digits>.\n"
    "Error lines contain JSON with 'error': N where N is an integer > 0.\n"
    "Reply with ONE JSON object only, no markdown, keys:\n"
    '  "last_player_user_id": string or null — userid of the player block that ends last in this tail\n'
    '  "last_error_user_id": string or null — userid owning the last error>0 line in this tail\n'
    '  "last_error_line": string or null — short quote of that error line (max 200 chars)\n'
    '  "last_error_code": string or null — the error field value\n'
    '  "summary": one English sentence explaining what happened\n'
)


def _llm_available() -> bool:
    try:
        import chatagent as ca

        return bool(ca.llm_available())
    except Exception:
        return False


def ai_summarize_log_tail(
    tail_text: str,
    *,
    machine_display: str,
    target_date: date,
) -> dict[str, Any] | None:
    """Ask LLM to read tail lines; return parsed JSON dict or None."""
    if not use_ai_summary() or not _llm_available():
        return None
    try:
        import chatagent as ca
    except Exception:
        return None

    api_key = ca._llm_api_key()
    if not api_key:
        return None

    user = (
        f"machine={machine_display!r} date={target_date.isoformat()}\n"
        f"--- log tail ({ai_tail_line_count()} lines max) ---\n"
        f"{tail_text.strip()}"
    )
    payload: dict[str, Any] = {
        "model": ca._llm_model_for_request(images=False),
        "messages": [
            {"role": "system", "content": _LOG_AI_SYSTEM},
            {"role": "user", "content": user},
        ],
        "max_tokens": 500,
        "temperature": 0.0,
    }
    try:
        if ca._is_ollama_base():
            payload["think"] = False
    except Exception:
        pass

    url = f"{ca._llm_base_url()}/chat/completions"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=ca._llm_timeout_sec()) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="replace"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as e:
        return {"_error": str(e)}

    try:
        content = (body["choices"][0]["message"]["content"] or "").strip()
    except (KeyError, IndexError, TypeError):
        return {"_error": "invalid LLM response shape"}

    content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.I | re.M).strip()
    try:
        obj = json.loads(content)
        return obj if isinstance(obj, dict) else {"_error": "LLM JSON not an object"}
    except json.JSONDecodeError:
        return {"summary": content[:800], "_raw": True}


def _fetch_logic_log_body(
    machine_query: str,
    td: date,
    *,
    timeout_ms: int,
    source: str,
    logic_log_basename: str | None = None,
) -> tuple[str, str, list[str], str]:
    """
    Load logic log text (OSS or LogNavigator). Returns
    (log_body, machine_display, header_lines, opened_basename).
    """
    text_parts: list[str] = []
    machine_display = (machine_query or "").strip()
    date_str = td.isoformat()
    timeout_sec = max(30.0, timeout_ms / 1000.0)
    chosen = ""

    if source == "oss":
        same_day = cc.list_oss_logic_log_basenames_for_date(
            machine_query, td, timeout_sec=min(30.0, timeout_sec)
        )
        want = (logic_log_basename or "").strip()

        def _oss_fetch(basename: str) -> str:
            body, oss_parts = cc.fetch_log_via_oss(
                machine_query,
                td,
                timeout_sec=timeout_sec,
                logic_log_basename=basename,
            )
            text_parts.extend(oss_parts)
            return body

        log_body = ""
        if same_day:
            if want and want in same_day:
                chosen = want
                log_body = _oss_fetch(chosen)
            elif len(same_day) >= 2:
                best_fn, best_body, best_ts = "", "", ""
                for fn in same_day:
                    try:
                        body = _oss_fetch(fn)
                    except Exception as e:
                        text_parts.append(f"⚠ Could not fetch logic log {fn}: {e}")
                        continue
                    ts = cc._latest_log_ts_in_body(body)
                    text_parts.append(f"→ scanned {fn}: last activity {ts or 'n/a'}")
                    if best_fn == "" or ts > best_ts:
                        best_fn, best_body, best_ts = fn, body, ts
                if best_fn:
                    chosen, log_body = best_fn, best_body
                else:
                    chosen = f"{date_str}.log" if f"{date_str}.log" in same_day else same_day[0]
                    log_body = _oss_fetch(chosen)
            else:
                chosen = f"{date_str}.log" if f"{date_str}.log" in same_day else same_day[0]
                log_body = _oss_fetch(chosen)
        else:
            chosen = f"{date_str}.log"
            log_body = _oss_fetch(chosen)
        machine_display = cc.resolve_oss_machine_folder(machine_query)
    else:
        log_body, machine_display, nav_parts, nav_meta = cc.fetch_log_via_navigator(
            machine_query,
            td,
            timeout_ms=timeout_ms,
            base=cc.DEFAULT_BASE,
            user=cc.DEFAULT_USER,
            pw=cc.DEFAULT_PASS,
            debug_headed=False,
            logic_log_basename=logic_log_basename,
        )
        text_parts.extend(nav_parts)
        chosen = str(nav_meta.get("opened_logic_log_basename") or f"{date_str}.log")

    return log_body, machine_display, text_parts, chosen


def format_error_context_block(err: dict[str, Any] | None, *, title: str = "Last error") -> str:
    if not err:
        return f"**{title}:** (none)\n"
    uid = str(err.get("user_id") or "n/a")
    code = err.get("error_count", "?")
    t = (err.get("time") or "").strip() or "n/a"
    ctx = err.get("context_lines") or []
    if ctx:
        body = "\n".join(ctx)
    else:
        body = (err.get("full_line") or err.get("snippet") or "").strip() or "(no line)"
    b, a = ctx_before_lines(), ctx_after_lines()
    head = (
        f"**{title}**\n"
        f"- User ID: `{uid}`\n"
        f"- Time: `{t}`\n"
        f"- Error code: `{code}`\n"
        f"- Context ({b} lines above + error + {a} below):\n"
    )
    return f"{head}```\n{body}\n```\n"


def format_report(
    *,
    machine_display: str,
    target_date: date,
    opened_basename: str,
    latest_any_uid: str | None,
    latest_err_uid: str | None,
    last_error: dict[str, Any] | None,
    transfer_out: dict[str, Any] | None,
    last_success: dict[str, Any] | None,
    ai_summary: dict[str, Any] | None,
    header_lines: list[str],
) -> str:
    dstr = target_date.isoformat()
    parts: list[str] = []
    if header_lines:
        parts.append("\n".join(header_lines))
        parts.append("")
    parts.append(f"**Machine:** `{machine_display}`  **Date:** `{dstr}`  **File:** `{opened_basename}`")
    parts.append("")
    parts.append(
        f"**Last player in log (any):** `{latest_any_uid or 'n/a'}`\n"
        f"**Last player with error:** `{latest_err_uid or 'n/a'}`"
    )
    if latest_any_uid and latest_err_uid:
        same = latest_any_uid == latest_err_uid
        parts.append(
            "**Same player?** "
            + ("Yes — last activity and last error refer to the same user." if same else "No — different users.")
        )
    parts.append("")
    xfer = transfer_out or {}
    parts.append(_format_transfer_out_line(xfer, uid=latest_any_uid))
    parts.append("")
    if last_error:
        parts.append(format_error_context_block(last_error))
    else:
        parts.append(format_success_context_block(last_success, uid=latest_any_uid))
    if ai_summary:
        parts.append("**AI summary (tail read):**")
        if ai_summary.get("_error"):
            parts.append(f"(LLM failed: {ai_summary['_error']})")
        elif ai_summary.get("_raw"):
            parts.append(str(ai_summary.get("summary") or ""))
        else:
            for k in (
                "last_player_user_id",
                "last_error_user_id",
                "last_error_code",
                "last_error_line",
                "summary",
            ):
                v = ai_summary.get(k)
                if v is not None and str(v).strip():
                    parts.append(f"- {k}: {v}")
    return "\n".join(parts).strip() + "\n"


def run_check_machine_log(
    machine_query: str,
    *,
    target_date: date | None = None,
    timeout_ms: int = 90_000,
    source: str | None = None,
    logic_log_basename: str | None = None,
    skip_ai: bool = False,
) -> dict[str, Any]:
    """
    Fetch logic log (checkcredit path), parse errors, enrich ±10 context, optional AI on tail.

    Returns dict with keys: text, machine_display, target_date, opened_basename,
    latest_any_uid, latest_err_uid, last_error, ai_summary, merged_players, log_tail_lines.
    """
    td = target_date or date.today()
    if source is None:
        source = "oss" if cc.checkcredit_use_oss_source() else "navigator"

    log_body, machine_display, header_lines, opened_basename = _fetch_logic_log_body(
        machine_query,
        td,
        timeout_ms=timeout_ms,
        source=source,
        logic_log_basename=logic_log_basename,
    )

    raw_lines = log_body.splitlines()
    parsed = cc.parse_user_blocks_full(log_body)
    merged = cc.merge_players_full(parsed)
    enrich_merged_context(raw_lines, merged)

    le_uid, _ = cc.pick_latest_error_uid(merged)
    la_uid, _ = cc.pick_latest_any_uid(merged)
    last_error = _pick_latest_error(merged)
    last_player_row = _row_for_uid(merged, la_uid)
    transfer_out = extract_transfer_out(last_player_row)
    last_success = find_last_success_line(raw_lines, la_uid) if not last_error else None

    tail_lines = tail_log_lines(log_body)
    tail_text = "\n".join(tail_lines)

    ai_summary: dict[str, Any] | None = None
    if not skip_ai:
        ai_summary = ai_summarize_log_tail(
            tail_text,
            machine_display=machine_display,
            target_date=td,
        )

    text = format_report(
        machine_display=machine_display,
        target_date=td,
        opened_basename=opened_basename,
        latest_any_uid=la_uid,
        latest_err_uid=le_uid,
        last_error=last_error,
        transfer_out=transfer_out,
        last_success=last_success,
        ai_summary=ai_summary,
        header_lines=header_lines,
    )

    return {
        "text": text,
        "machine_display": machine_display,
        "target_date": td,
        "opened_basename": opened_basename,
        "latest_any_uid": la_uid,
        "latest_err_uid": le_uid,
        "last_error": last_error,
        "transfer_out": transfer_out,
        "last_success": last_success,
        "ai_summary": ai_summary,
        "merged_players": merged,
        "log_tail_lines": tail_lines,
        "source": source,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="checkmachinelog — machine error log (same fetch as checkcredit)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 checkmachinelog.py 2074 --date 2026-06-26\n"
            "  python3 checkmachinelog.py NCH1422 --no-ai\n"
            "  python3 checkmachinelog.py CP0231 --navigator --date 2026-02-05\n"
        ),
    )
    ap.add_argument("machine", help="Machine query (e.g. 2074, NWR2074, NCH1422)")
    ap.add_argument("--date", metavar="YYYY-MM-DD", help="Log date (default: today)")
    ap.add_argument("--timeout-ms", type=int, default=90_000)
    ap.add_argument(
        "--oss",
        action="store_true",
        help="OSS HTTP (default when CHECKCREDIT_USE_OSS not disabled)",
    )
    ap.add_argument("--navigator", action="store_true", help="Force LogNavigator browser")
    ap.add_argument("--logic-file", metavar="BASENAME", help="Specific logic log basename")
    ap.add_argument("--no-ai", action="store_true", help="Skip LLM tail summary")
    args = ap.parse_args(argv)

    td = date.today()
    if args.date:
        try:
            td = datetime.strptime(args.date.strip(), "%Y-%m-%d").date()
        except ValueError:
            print("❌ Invalid --date; use YYYY-MM-DD", file=sys.stderr)
            return 2

    if args.navigator:
        source = "navigator"
    elif args.oss:
        source = "oss"
    else:
        source = "oss" if cc.checkcredit_use_oss_source() else "navigator"

    try:
        out = run_check_machine_log(
            str(args.machine).strip(),
            target_date=td,
            timeout_ms=args.timeout_ms,
            source=source,
            logic_log_basename=(args.logic_file or "").strip() or None,
            skip_ai=bool(args.no_ai),
        )
    except Exception as e:
        print(f"❌ {e}", file=sys.stderr)
        return 1

    print(out.get("text") or "")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
