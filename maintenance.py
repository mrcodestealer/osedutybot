#!/usr/bin/env python3
"""
Generate a simplified maintenance summary from a full email.
Supports multiple email formats, including ongoing maintenance.

Env (optional):
  gamelist / GAMELIST — Lark **spreadsheet token** for the game list workbook.
  Each **sheet tab title** is treated as a game name; sheets must include columns
  ``游戏名称 / Games Name`` and ``游戏状态 / Game Status``.
"""

from __future__ import annotations

import sys
import re
import os
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()

GAMELIST_SPREADSHEET_TOKEN = (
    os.getenv("gamelist", "").strip() or os.getenv("GAMELIST", "").strip()
)


def _cell_norm(c: Any) -> str:
    if c is None:
        return ""
    return str(c).replace("\r", " ").replace("\n", " ").strip().lower()


def _names_match(a: Any, b: Any) -> bool:
    na = _cell_norm(a).replace(" ", "")
    nb = _cell_norm(b).replace(" ", "")
    if not na or not nb:
        return False
    return na == nb or na in nb or nb in na


def _is_launched_status(cell: Any) -> bool:
    raw = str(cell or "")
    low = raw.lower()
    return ("上线" in raw) and ("launched" in low)


def _fetch_spreadsheet_sheets(tenant_token: str, spreadsheet_token: str) -> list[dict[str, Any]]:
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
    headers = {"Authorization": f"Bearer {tenant_token}"}
    resp = requests.get(url, headers=headers, timeout=60)
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(str(data.get("msg", data)))
    sheets = data.get("data", {}).get("sheets", [])
    out: list[dict[str, Any]] = []
    for sh in sheets:
        sid = sh.get("sheet_id") or sh.get("sheetId")
        title = sh.get("title") or sh.get("sheet_title") or ""
        if sid:
            out.append({"sheet_id": str(sid), "title": str(title)})
    return out


def _fetch_sheet_values(
    tenant_token: str, spreadsheet_token: str, sheet_id: str, *, max_row: int = 500
) -> list[list[Any]]:
    rng = f"{sheet_id}!A1:ZZ{max_row}"
    enc = quote(rng, safe="")
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{enc}"
    headers = {"Authorization": f"Bearer {tenant_token}"}
    params = {"valueRenderOption": "ToString", "dateTimeRenderOption": "FormattedString"}
    resp = requests.get(url, headers=headers, params=params, timeout=90)
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(str(data.get("msg", data)))
    vr = data.get("data", {}).get("valueRange") or data.get("data", {}).get("value_range") or {}
    return vr.get("values") or []


def _find_header_row_and_cols(grid: list[list[Any]]) -> tuple[int, int, int] | None:
    for ri, row in enumerate(grid[:60]):
        if not row:
            continue
        name_ci: int | None = None
        status_ci: int | None = None
        for ci, cell in enumerate(row):
            cn = _cell_norm(cell)
            if "游戏名称" in cn or ("games" in cn and "name" in cn):
                name_ci = ci
            if "游戏状态" in cn or ("game" in cn and "status" in cn):
                status_ci = ci
        if name_ci is not None and status_ci is not None:
            return ri, name_ci, status_ci
    return None


def _row_launched_for_game(
    grid: list[list[Any]], game_name: str, sheet_title: str
) -> bool | None:
    parsed = _find_header_row_and_cols(grid)
    if not parsed:
        return None
    hi, ci_name, ci_status = parsed
    last: bool | None = None
    for row in grid[hi + 1 :]:
        if not row:
            continue
        name_cell = row[ci_name] if len(row) > ci_name else ""
        status_cell = row[ci_status] if len(row) > ci_status else ""
        if not (_names_match(name_cell, game_name) or _names_match(name_cell, sheet_title)):
            continue
        last = _is_launched_status(status_cell)

    data_rows = [r for r in grid[hi + 1 :] if r]
    if last is None and len(data_rows) == 1 and _names_match(sheet_title, game_name):
        r = data_rows[0]
        st = r[ci_status] if len(r) > ci_status else ""
        return _is_launched_status(st)
    return last


def _best_sheet_match(sheets: list[dict[str, Any]], game_name: str) -> dict[str, Any] | None:
    gn = _cell_norm(game_name).replace(" ", "")
    if not gn:
        return None
    best: dict[str, Any] | None = None
    best_score = -1
    for sh in sheets:
        title = str(sh.get("title") or "")
        tn = _cell_norm(title).replace(" ", "")
        if not tn:
            continue
        if gn == tn:
            return sh
        if gn in tn or tn in gn:
            score = min(len(gn), len(tn))
            if score > best_score:
                best_score = score
                best = sh
    return best


def _table_block_stop_line(line: str) -> bool:
    """True if this line ends the “list of tables” block (not a table name row)."""
    if not line or not line.strip():
        return False
    ln = line.strip()
    if re.match(
        r"^(?:You may find summary|Start time:|End time:|Reason:|Table availability:|\[Service Desk\])",
        ln,
        re.I,
    ):
        return True
    if re.match(r"^(?:TINC-|SD-\d)", ln, re.I):
        return True
    if re.match(
        r"^(?:This is to inform|During which|Please |Kindly |Note:)",
        ln,
        re.I,
    ):
        return True
    return False


def _parse_table_block_after_heading(
    lines: list[str], heading_i: int
) -> tuple[list[str], int]:
    """
    Collect consecutive table-name lines after ``Affected table`` /
    ``following tables will be unavailable`` headings.

    Skips blank lines after the heading, then reads non-empty lines until a
    blank line or a known section header.
    """
    j = heading_i + 1
    n = len(lines)
    while j < n and not lines[j].strip():
        j += 1
    names: list[str] = []
    while j < n:
        chunk = lines[j].strip()
        if not chunk:
            break
        if _table_block_stop_line(chunk):
            break
        names.append(chunk)
        j += 1
    return names, j


def extract_info(text):
    """Parse email text line by line to extract fields."""
    info = {
        'table': 'Unknown',
        'reason': 'Unknown',
        'status': 'Unknown',
        'start_time': 'Unknown',
        'end_time': 'Unknown',
        'reference': 'Unknown'
    }

    lines = [line.strip() for line in text.splitlines()]
    table_availability_affected = False

    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        if not line:
            i += 1
            continue

        # ---- Table detection ----
        if re.search(r'^table\s+', line, re.IGNORECASE):
            match = re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE)
            if match:
                info['table'] = match.group(1).strip()
            else:
                match = re.search(r'table\s+(.*?)\s+was', line, re.IGNORECASE)
                if match:
                    info['table'] = match.group(1).strip()
        elif re.search(r'^Affected table/-s:', line, re.IGNORECASE):
            block_names, j = _parse_table_block_after_heading(lines, i)
            if block_names:
                info["table"] = ", ".join(block_names)
            i = j
            continue
        elif re.search(r'following tables will be unavailable:', line, re.IGNORECASE):
            block_names, j = _parse_table_block_after_heading(lines, i)
            if block_names:
                info["table"] = ", ".join(block_names)
            i = j
            continue

        # ---- Reason ----
        elif re.search(r'^Reason:', line, re.IGNORECASE):
            match = re.search(r'^Reason:\s*(.*)$', line, re.IGNORECASE)
            if match:
                reason = match.group(1).strip()
                reason = re.sub(r'\s*\([^)]*\)', '', reason)
                info['reason'] = reason

        # ---- Status (explicit) ----
        elif re.search(r'^Status:', line, re.IGNORECASE):
            match = re.search(r'^Status:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['status'] = match.group(1).strip()

        # ---- Table availability (new format status) ----
        elif re.search(r'^Table availability:', line, re.IGNORECASE):
            match = re.search(r'^Table availability:\s*(.*)$', line, re.IGNORECASE)
            if match and match.group(1).strip().lower() == 'affected':
                table_availability_affected = True

        # ---- Start time ----
        elif re.search(r'^Start time:', line, re.IGNORECASE):
            match = re.search(r'^Start time:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['start_time'] = match.group(1).strip()

        # ---- End time ----
        elif re.search(r'^End time:', line, re.IGNORECASE):
            match = re.search(r'^End time:\s*(.*)$', line, re.IGNORECASE)
            if match:
                info['end_time'] = match.group(1).strip()

        # ---- Time of resolution (old format) ----
        elif re.search(r'^Time of resolution:', line, re.IGNORECASE):
            match = re.search(r'from\s+(.*?)\s+till\s+(.*?)(?:\s*\(|$)', line, re.IGNORECASE)
            if match:
                info['start_time'] = match.group(1).strip()
                info['end_time'] = match.group(2).strip()
            else:
                # Check for "We will inform you as soon..." pattern
                if re.search(r'We will inform you as soon', line, re.IGNORECASE):
                    info['end_time'] = "TBA"

        # ---- Reference lines (only if they match expected patterns) ----
        elif re.search(r'^(TINC-\d+|SD-\d+|\[Service Desk\])', line, re.IGNORECASE):
            info['reference'] = line.strip()

        i += 1

    # --- Fallbacks ---
    # If start time still unknown, try to find it in the first paragraph
    if info['start_time'] == 'Unknown':
        # Look for "from ... UTC" pattern
        from_match = re.search(r'from\s+(.*?)\s+UTC', text, re.IGNORECASE)
        if from_match:
            info['start_time'] = from_match.group(1).strip() + " UTC"
    # If end time still unknown and "We will inform you" appears, set TBA
    if info['end_time'] == 'Unknown' and re.search(r'We will inform you as soon', text, re.IGNORECASE):
        info['end_time'] = "TBA"
    # If table name still unknown, try to extract from "table X in Y" in the first paragraph
    if info['table'] == 'Unknown':
        table_match = re.search(r'table\s+([^\.]+?)\s+in', text, re.IGNORECASE)
        if table_match:
            info['table'] = table_match.group(1).strip()
    # Do NOT fallback to first line for reference; leave as "Unknown" if not found

    # Set status based on table availability (only if not already set)
    if info['status'] == 'Unknown' and table_availability_affected:
        info['status'] = 'Affected'
    elif info['status'] == 'Unknown' and re.search(r'successfully accomplished', text, re.IGNORECASE):
        info['status'] = 'Fixed'

    return info

def generate_output(info):
    """Format the extracted info into the desired output with user mentions."""
    # Use the provided open IDs for the two roles
    qa_os_local_id = "ou_0342007237c6c1aa262acae839acb7c6"
    cs_team_id = "ou_c927a378e9b464741c67b61c1641577b"

    output = [
        f'Hi <at user_id="{qa_os_local_id}">QA OS Local</at> <at user_id="{cs_team_id}">CS (Team)</at> , kindly check this email. Thank you.',
        "",
        f"Affected table : {info['table']}",
        f"Reason : {info['reason']}",
        f"Status: {info['status']}",
        f"Start time: {info['start_time']}",
        f"End time : {info['end_time']}",
        "",
        f"REF EMAIL:{info['reference']}"
    ]
    return "\n".join(output)

def get_table_name(text):
    """Extract just the affected table name for the first tag message."""
    lines = [line.strip() for line in text.splitlines()]
    for i, line in enumerate(lines):
        if re.search(r'^table\s+', line, re.IGNORECASE):
            match = re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE)
            if match:
                return match.group(1).strip()
            match = re.search(r'table\s+(.*?)\s+was', line, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        elif re.search(r'^Affected table/-s:', line, re.IGNORECASE):
            block_names, _ = _parse_table_block_after_heading(lines, i)
            if block_names:
                return block_names[0]
        elif re.search(r'following tables will be unavailable:', line, re.IGNORECASE):
            block_names, _ = _parse_table_block_after_heading(lines, i)
            if block_names:
                return block_names[0]
        elif re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE):
            match = re.search(r'table\s+([^\.]+?)\s+in', line, re.IGNORECASE)
            if match:
                return match.group(1).strip()
    # Fallback: if none of the above, try to extract from the first line containing "table"
    first_table = re.search(r'table\s+([^\.]+?)\s+in', text, re.IGNORECASE)
    if first_table:
        return first_table.group(1).strip()
    return "Unknown"


def extract_candidate_game_names(text: str) -> list[str]:
    """Collect possible game/table names from maintenance email text."""
    seen: set[str] = set()
    out: list[str] = []

    def add(raw: str) -> None:
        for part in re.split(r"[,;，、]", raw):
            t = part.strip()
            if not t or t.lower() == "unknown":
                continue
            key = _cell_norm(t).replace(" ", "")
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(t)

    primary = get_table_name(text)
    if primary and primary.strip().lower() != "unknown":
        add(primary)

    for m in re.finditer(r"table\s+([^\.]+?)\s+in", text, re.IGNORECASE):
        add(m.group(1).strip())

    lines = [line.strip() for line in text.splitlines()]
    for i, line in enumerate(lines):
        if re.search(r"^Affected table/-s:", line, re.IGNORECASE):
            block_names, _ = _parse_table_block_after_heading(lines, i)
            for nm in block_names:
                add(nm)
            break
        if re.search(r"following tables will be unavailable:", line, re.IGNORECASE):
            block_names, _ = _parse_table_block_after_heading(lines, i)
            for nm in block_names:
                add(nm)
            break

    return out


def process_email(text):
    """Process email text and return the formatted summary."""
    info = extract_info(text)
    return generate_output(info)


def process_maintenance_pipeline(
    email_text: str, tenant_access_token: str | None
) -> tuple[str, str]:
    """
    Two-part reply for bot:
    1) Launched vs not launched vs unknown (from gamelist spreadsheet).
    2) Full QA/CS summary only if at least one candidate is 「上线 Launched」.

    If ``gamelist`` env or token is missing, returns ("", process_email(...)).
    """
    tok = (tenant_access_token or "").strip()
    ss = GAMELIST_SPREADSHEET_TOKEN
    if not ss or not tok:
        return "", process_email(email_text)

    try:
        sheets = _fetch_spreadsheet_sheets(tok, ss)
    except Exception as e:
        return (
            f"⚠️ **Gamelist 表格**读取失败（仍发送下方原始摘要）: `{e}`",
            process_email(email_text),
        )

    candidates = extract_candidate_game_names(email_text)
    if not candidates:
        return (
            "⚠️ 未能从邮件中识别游戏/表名（仍发送下方原始摘要）。",
            process_email(email_text),
        )

    launched_list: list[str] = []
    not_launched_list: list[str] = []
    unknown_list: list[str] = []

    for g in candidates:
        sh = _best_sheet_match(sheets, g)
        if not sh:
            unknown_list.append(f"{g}（gamelist 中无匹配子表标题）")
            continue
        try:
            grid = _fetch_sheet_values(tok, ss, sh["sheet_id"])
        except Exception:
            unknown_list.append(f"{g}（读表失败）")
            continue
        verdict = _row_launched_for_game(grid, g, sh.get("title") or "")
        if verdict is True:
            launched_list.append(g)
        elif verdict is False:
            not_launched_list.append(g)
        else:
            unknown_list.append(f"{g}（未找到列或无匹配行）")

    lines1 = ["📋 **游戏上线状态（对照 gamelist 表格）**"]
    lines1.append(
        "✅ **上线 Launched：** "
        + (", ".join(launched_list) if launched_list else "（无）")
    )
    lines1.append(
        "⛔ **非上线 Launched：** "
        + (", ".join(not_launched_list) if not_launched_list else "（无）")
    )
    if unknown_list:
        lines1.append("❓ **未匹配子表或状态未知：** " + ", ".join(unknown_list))

    msg1 = "\n".join(lines1)

    if launched_list:
        msg2 = process_email(email_text)
    else:
        msg2 = (
            "📭 **第二段（已过滤）：** 邮件中识别的游戏均未处于「上线 Launched」，"
            "已跳过 QA/CS 摘要。"
        )

    return msg1, msg2


def main():
    """Command‑line interface."""
    if len(sys.argv) > 1:
        text = " ".join(sys.argv[1:])
        if '\n' not in text:
            print("⚠️ Hint: For multiline input, please enclose the email in quotes or use a pipe (python3 maintenance.py < email.txt).", file=sys.stderr)
    else:
        text = sys.stdin.read()
    if not text.strip():
        print("No input provided.", file=sys.stderr)
        sys.exit(1)
    print(process_email(text))

if __name__ == "__main__":
    main()