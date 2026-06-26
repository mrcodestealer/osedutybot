#!/usr/bin/env python3
"""
checkperson — "who is the check person for this issue?" finder for the Duty Bot.

Goal
----
A CS / QA / duty member describes an issue, and the bot decides — using the
real historical issue data from the Lark Base — who should check it:

    Issue       : <concise title of the issue>
    Priority    : <P0 / P1 / P2 / P3>
    Department   : <FE / FPMS / CPMS / PMS / AI / SRE / ... >
    Check Person : <the person (Assignee) most likely to own this issue>

How the AI "learns" the data
----------------------------
The knowledge comes from a Lark Base (bitable) with several tables:

  * ``Chat`` / ``GameChat`` / ``All`` — large corpus of reported issues, each with
    a Department + Priority (and a written description / summary). Good for
    learning *which department + priority* a symptom maps to.
  * ``BugMeegle`` / ``MainMeegle`` — engineering tickets that ALSO carry an
    **Assignee** (= the real "check person"), a Related Department, Priority,
    Root Cause and Solution. Good for learning *who* owns which kind of issue.

``build_snapshot()`` pulls every table once and caches a compact, normalized
snapshot to ``checkperson_data.json`` (refreshed on a TTL). From that snapshot we
build a **department → check-person ranking** (from the Meegle assignees) and a
**retrieval corpus** of past issues.

For a new issue we:
  1. retrieve the most similar past issues (token/IDF overlap — no extra deps),
  2. summarise the department / priority / assignee distribution of the matches,
  3. hand all of that to qwen3.5:35b-a3b (or whatever ``BOT_CHAT_MODEL`` is) as
     grounded context and ask it to *reason* and output the 4 fields,
  4. fall back to a deterministic pick (most-similar record + department mapping)
     when no LLM is configured or it errors — so the bot is never silent.

CLI
---
    python checkperson.py --refresh                       # rebuild the snapshot
    python checkperson.py "player 12345 cannot login on PC web, error prompt"
    python checkperson.py "colorland says already joined this session"
"""

from __future__ import annotations

import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Optional

try:  # so BOT_CHAT_* / APP_ID etc. resolve when run standalone
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Lark Base (bitable) configuration
# ---------------------------------------------------------------------------

# The "Issue / Check Person" knowledge base. Override with env if it moves.
APP_TOKEN = (os.getenv("CHECKPERSON_APP_TOKEN") or "RKxMbfZl9ao1b0syHvslrjLTgIX").strip()
SNAPSHOT_PATH = os.getenv("CHECKPERSON_SNAPSHOT") or os.path.join(_ROOT_DIR, "checkperson_data.json")

# Field aliases (different tables name the same thing differently).
_TITLE_FIELDS = ("Issue Title", "Issue Name", "Title")
_DESC_FIELDS = ("Source Message", "Issue Description", "Description", "Text")
_SUMMARY_FIELDS = ("Summary",)
_DEPT_FIELDS = ("Related Department", "Department")
_PRIORITY_FIELDS = ("Priority",)
_PERSON_FIELDS = ("Assignee", "Check Person", "Owner")
_ROOTCAUSE_FIELDS = ("Root Cause",)
_SOLUTION_FIELDS = ("Solution",)

# Assignee values that are not a real person to recommend on their own.
_NON_PERSON_ASSIGNEES = {"-", "--", "n/a", "na", "none", "tbd", "unassigned", ""}

_STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "to", "of", "in", "on", "at", "for",
    "is", "are", "was", "were", "be", "been", "being", "this", "that", "these",
    "those", "it", "its", "we", "i", "you", "he", "she", "they", "as", "by",
    "with", "from", "please", "help", "hi", "team", "kindly", "check", "issue",
    "thank", "thanks", "po", "om", "duty", "cp", "us", "me", "our", "your",
    "when", "while", "upon", "also", "still", "not", "no", "can", "cannot",
    "cant", "could", "would", "should", "have", "has", "had", "do", "does",
    "did", "will", "may", "if", "so", "up", "out", "about", "regarding",
    "encountered", "reported", "player", "players", "account", "accounts",
    "id", "ref", "reference", "thru", "via", "due", "after", "before",
}


# ---------------------------------------------------------------------------
# Lark API helpers
# ---------------------------------------------------------------------------

def _lark_base() -> str:
    return (os.getenv("LARK_OPEN_BASE") or "https://open.larksuite.com").strip().rstrip("/")


def get_tenant_access_token() -> str:
    import requests

    app_id = os.getenv("APP_ID")
    app_secret = os.getenv("APP_SECRET")
    if not app_id or not app_secret:
        raise RuntimeError("APP_ID / APP_SECRET not set in environment (.env)")
    url = f"{_lark_base()}/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": app_id, "app_secret": app_secret}, timeout=30)
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"Failed to get tenant token: {data}")
    return data["tenant_access_token"]


def _list_tables(token: str) -> list[dict[str, Any]]:
    import requests

    url = f"{_lark_base()}/open-apis/bitable/v1/apps/{APP_TOKEN}/tables"
    headers = {"Authorization": f"Bearer {token}"}
    out: list[dict[str, Any]] = []
    page_token = None
    while True:
        params: dict[str, Any] = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        data = requests.get(url, headers=headers, params=params, timeout=30).json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to list tables: {data}")
        d = data.get("data", {})
        out.extend(d.get("items") or [])
        if not d.get("has_more"):
            break
        page_token = d.get("page_token")
    return out


def _get_records(token: str, table_id: str) -> list[dict[str, Any]]:
    import requests

    url = f"{_lark_base()}/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {token}"}
    out: list[dict[str, Any]] = []
    page_token = None
    while True:
        params: dict[str, Any] = {"page_size": 200}
        if page_token:
            params["page_token"] = page_token
        data = requests.get(url, headers=headers, params=params, timeout=60).json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to fetch records for {table_id}: {data}")
        d = data.get("data", {})
        out.extend(d.get("items") or [])
        if not d.get("has_more"):
            break
        page_token = d.get("page_token")
    return out


# ---------------------------------------------------------------------------
# Field normalization
# ---------------------------------------------------------------------------

def _field_text(value: Any) -> str:
    """Flatten a Lark bitable field value to plain text."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in ("text", "label", "name", "value", "en_name"):
            if value.get(key):
                return str(value[key]).strip()
        return ""
    if isinstance(value, list):
        parts = [_field_text(v) for v in value]
        return ", ".join(p for p in parts if p)
    return str(value).strip()


def _first_field(fields: dict[str, Any], names: tuple[str, ...]) -> str:
    for n in names:
        if n in fields:
            t = _field_text(fields.get(n))
            if t:
                return t
    return ""


def _split_persons(assignee_text: str) -> list[str]:
    """A Meegle Assignee may be a comma-separated list of names."""
    out: list[str] = []
    for raw in re.split(r"[,/、;]+", assignee_text or ""):
        name = raw.strip()
        if not name:
            continue
        if name.lower() in _NON_PERSON_ASSIGNEES:
            continue
        out.append(name)
    return out


def _normalize_record(table_name: str, rec: dict[str, Any]) -> Optional[dict[str, Any]]:
    fields = rec.get("fields", {}) or {}
    title = _first_field(fields, _TITLE_FIELDS)
    desc = _first_field(fields, _DESC_FIELDS)
    summary = _first_field(fields, _SUMMARY_FIELDS)
    department = _first_field(fields, _DEPT_FIELDS)
    priority = _first_field(fields, _PRIORITY_FIELDS).upper().replace(" ", "")
    person = _first_field(fields, _PERSON_FIELDS)
    root_cause = _first_field(fields, _ROOTCAUSE_FIELDS)
    solution = _first_field(fields, _SOLUTION_FIELDS)

    blob = " ".join(p for p in (title, summary, desc) if p).strip()
    if not blob:
        return None
    # Keep the snapshot small: trim long descriptions.
    if len(desc) > 600:
        desc = desc[:600] + "…"
    return {
        "source": table_name,
        "title": title,
        "summary": summary,
        "description": desc,
        "department": department,
        "priority": priority,
        "check_person": person,
        "root_cause": (root_cause[:300] + "…") if len(root_cause) > 300 else root_cause,
        "solution": (solution[:300] + "…") if len(solution) > 300 else solution,
    }


# ---------------------------------------------------------------------------
# Snapshot build / load
# ---------------------------------------------------------------------------

def build_snapshot() -> dict[str, Any]:
    """Fetch every table from the Lark Base and write a normalized snapshot."""
    token = get_tenant_access_token()
    tables = _list_tables(token)
    records: list[dict[str, Any]] = []
    table_summary: list[dict[str, Any]] = []
    for tb in tables:
        tid = tb.get("table_id")
        name = tb.get("name") or tid
        if not tid:
            continue
        raw = _get_records(token, tid)
        kept = 0
        for r in raw:
            norm = _normalize_record(name, r)
            if norm:
                records.append(norm)
                kept += 1
        table_summary.append({"name": name, "table_id": tid, "records": len(raw), "kept": kept})

    snapshot = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "app_token": APP_TOKEN,
        "tables": table_summary,
        "records": records,
    }
    snapshot["department_to_persons"] = _build_department_persons(records)
    snapshot["priority_by_department"] = _build_priority_by_department(records)
    with open(SNAPSHOT_PATH, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=1)
    return snapshot


def _build_department_persons(records: list[dict[str, Any]]) -> dict[str, list[list[Any]]]:
    """department -> ranked [person, count] from records that carry an Assignee."""
    counts: dict[str, Counter] = defaultdict(Counter)
    for r in records:
        dept = (r.get("department") or "").strip()
        person_text = r.get("check_person") or ""
        if not dept:
            continue
        for p in _split_persons(person_text):
            counts[dept][p] += 1
    out: dict[str, list[list[Any]]] = {}
    for dept, ctr in counts.items():
        ranked = [[p, c] for p, c in ctr.most_common()]
        if ranked:
            out[dept] = ranked
    return out


def _build_priority_by_department(records: list[dict[str, Any]]) -> dict[str, list[list[Any]]]:
    counts: dict[str, Counter] = defaultdict(Counter)
    for r in records:
        dept = (r.get("department") or "").strip()
        pr = (r.get("priority") or "").strip()
        if dept and pr:
            counts[dept][pr] += 1
    return {d: [[p, c] for p, c in ctr.most_common()] for d, ctr in counts.items()}


def _snapshot_ttl_seconds() -> float:
    try:
        return max(0.0, float(os.getenv("CHECKPERSON_SNAPSHOT_TTL_HOURS", "24"))) * 3600.0
    except ValueError:
        return 24 * 3600.0


_SNAPSHOT_CACHE: dict[str, Any] | None = None


def load_snapshot(*, refresh: bool = False, allow_build: bool = True) -> dict[str, Any]:
    """Return the snapshot dict; rebuild it if missing/stale (or ``refresh``)."""
    global _SNAPSHOT_CACHE
    if _SNAPSHOT_CACHE is not None and not refresh:
        return _SNAPSHOT_CACHE

    disk: dict[str, Any] | None = None
    if os.path.exists(SNAPSHOT_PATH):
        try:
            with open(SNAPSHOT_PATH, encoding="utf-8") as f:
                disk = json.load(f)
        except Exception:
            disk = None

    stale = True
    if disk and disk.get("generated_at"):
        try:
            age = time.time() - datetime.fromisoformat(disk["generated_at"]).timestamp()
            stale = age > _snapshot_ttl_seconds()
        except Exception:
            stale = True

    if refresh or disk is None or stale:
        if allow_build:
            try:
                _SNAPSHOT_CACHE = build_snapshot()
                return _SNAPSHOT_CACHE
            except Exception as exc:
                print(f"⚠️ checkperson: snapshot rebuild failed ({exc!r}); using cached copy if any", flush=True)
        if disk is not None:
            _SNAPSHOT_CACHE = disk
            return _SNAPSHOT_CACHE
        # Nothing on disk and we couldn't build → empty KB.
        _SNAPSHOT_CACHE = {"records": [], "department_to_persons": {}, "priority_by_department": {}}
        return _SNAPSHOT_CACHE

    _SNAPSHOT_CACHE = disk
    return _SNAPSHOT_CACHE


# ---------------------------------------------------------------------------
# Retrieval (token / IDF overlap — dependency free)
# ---------------------------------------------------------------------------

def _tokenize(text: str) -> list[str]:
    toks = re.findall(r"[a-z0-9]+", (text or "").lower())
    return [t for t in toks if len(t) > 2 and t not in _STOPWORDS and not t.isdigit()]


_IDF_CACHE: dict[int, dict[str, float]] = {}


def _idf(records: list[dict[str, Any]]) -> dict[str, float]:
    key = id(records)
    cached = _IDF_CACHE.get(key)
    if cached is not None:
        return cached
    n = len(records) or 1
    df: Counter = Counter()
    for r in records:
        toks = set(_tokenize(f"{r.get('title','')} {r.get('summary','')} {r.get('description','')}"))
        for t in toks:
            df[t] += 1
    idf = {t: math.log((n + 1) / (c + 1)) + 1.0 for t, c in df.items()}
    _IDF_CACHE[key] = idf
    return idf


def retrieve_similar(query: str, snapshot: dict[str, Any], *, k: int = 10) -> list[tuple[float, dict[str, Any]]]:
    records = snapshot.get("records") or []
    if not records:
        return []
    idf = _idf(records)
    q_tokens = _tokenize(query)
    if not q_tokens:
        return []
    q_set = set(q_tokens)
    scored: list[tuple[float, dict[str, Any]]] = []
    for r in records:
        r_tokens = set(_tokenize(f"{r.get('title','')} {r.get('summary','')} {r.get('description','')}"))
        if not r_tokens:
            continue
        overlap = q_set & r_tokens
        if not overlap:
            continue
        score = sum(idf.get(t, 1.0) for t in overlap)
        # Length-normalize a little so very long records don't dominate.
        score /= math.sqrt(len(r_tokens)) or 1.0
        scored.append((score, r))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[:k]


# ---------------------------------------------------------------------------
# Deterministic suggestion (fallback + grounding)
# ---------------------------------------------------------------------------

def _person_for_department(snapshot: dict[str, Any], department: str) -> str:
    dept_map = snapshot.get("department_to_persons") or {}
    ranked = dept_map.get(department) or []
    for person, _count in ranked:
        if person and person.lower() not in _NON_PERSON_ASSIGNEES:
            return person
    return ""


def deterministic_suggestion(query: str, snapshot: dict[str, Any]) -> dict[str, str]:
    """Rule/retrieval based pick used as fallback and to ground the LLM."""
    matches = retrieve_similar(query, snapshot, k=12)

    # Department: majority vote among matches.
    dept_votes: Counter = Counter()
    prio_votes: Counter = Counter()
    person_votes: Counter = Counter()
    best_with_person: dict[str, Any] | None = None
    for score, r in matches:
        if r.get("department"):
            dept_votes[r["department"]] += score
        if r.get("priority"):
            prio_votes[r["priority"]] += score
        for p in _split_persons(r.get("check_person") or ""):
            person_votes[p] += score
            if best_with_person is None:
                best_with_person = r

    department = dept_votes.most_common(1)[0][0] if dept_votes else ""
    # Prefer the most similar (top-1) match's priority; fall back to a weighted vote.
    priority = ""
    if matches and matches[0][1].get("priority"):
        priority = matches[0][1]["priority"]
    elif prio_votes:
        priority = prio_votes.most_common(1)[0][0]

    # Check person: prefer the person ranked highest for the chosen department,
    # else the most-voted person across matches.
    person = _person_for_department(snapshot, department)
    if not person and person_votes:
        person = person_votes.most_common(1)[0][0]

    title = ""
    if matches:
        title = matches[0][1].get("title") or matches[0][1].get("summary") or ""
    if not title:
        title = _one_line(query, max_len=80)

    return {
        "issue": title,
        "priority": priority or "P2",
        "department": department or "Unknown",
        "check_person": person or "CP OM Duty (on-duty)",
    }


# ---------------------------------------------------------------------------
# LLM (OpenAI-compatible; same env as chatagent / identifyissue)
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
    return (os.getenv("BOT_CHECKPERSON_MODEL") or os.getenv("BOT_CHAT_MODEL") or "gpt-4o-mini").strip()


def llm_available() -> bool:
    return bool(_llm_api_key())


def is_enabled() -> bool:
    return (os.getenv("BOT_USE_CHECKPERSON") or "1").strip().lower() in ("1", "true", "yes", "on")


def _is_ollama_base() -> bool:
    base = _llm_base_url().lower()
    return "11434" in base or "ollama" in base


_SYSTEM_PROMPT = """You are **CP OM Duty Bot — Check Person Finder**.

A CS / QA / duty member describes a player or system issue. Your job is to decide,
based ONLY on the historical issue knowledge base provided to you, four things:

  • Issue        — a short, clear one-line title of the problem.
  • Priority     — one of P0 / P1 / P2 / P3 (use the priorities seen on similar
                   past issues; P0 = major / company loss / login-deposit-withdrawal
                   widespread / many players, P1 = significant single system,
                   P2 = limited or display, P3 = minor / cosmetic).
  • Department   — the team that owns it (e.g. FE, FPMS, CPMS, PMS, AI, SRE,
                   Game PO/Provider, PO, SEO, Main Site Operation, ...). Use the
                   department that similar past issues were routed to.
  • Check Person — the specific person who should check it. Pick from the
                   "Department → check persons" mapping and the assignees seen on
                   the most similar past issues. If the department has known
                   assignees, choose the most appropriate / most frequent one.

You are given:
  1) A "Department → check persons (by frequency)" mapping learned from real tickets.
  2) The most similar past issues (with their Department, Priority and Check Person).

Reason about which past issues are most like the new one, then choose. Do NOT invent
people who are not in the provided data — if you are unsure of the person, pick the
top person for the chosen department from the mapping.

OUTPUT FORMAT — reply EXACTLY in this shape and nothing before it:

Issue : <one line>
Priority : <P0|P1|P2|P3>
Department : <department>
Check Person : <name>

Reason / 理由: <1 short line in English, then 1 short line in 中文 explaining why —
cite the similar issue or the department mapping you used.>
"""


def _llm_complete(system_prompt: str, user_text: str, *, max_tokens: int = 700, timeout: float = 120.0) -> Optional[str]:
    api_key = _llm_api_key()
    if not api_key:
        return None
    url = f"{_llm_base_url()}/chat/completions"
    payload: dict[str, Any] = {
        "model": _llm_model(),
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        "max_tokens": int(max_tokens),
        "temperature": float(os.getenv("BOT_CHECKPERSON_TEMPERATURE", "0.2")),
    }
    if _is_ollama_base():
        payload["think"] = (os.getenv("BOT_CHAT_LLM_THINK") or "false").strip().lower() in ("1", "true", "yes", "on")
        keep_alive = (os.getenv("BOT_CHAT_OLLAMA_KEEP_ALIVE") or "-1").strip()
        try:
            payload["keep_alive"] = int(keep_alive)
        except ValueError:
            payload["keep_alive"] = keep_alive
    attempts = max(1, int(os.getenv("BOT_CHECKPERSON_LLM_RETRIES", "2")) + 1)
    last_err = ""
    for attempt in range(1, attempts + 1):
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = json.loads(resp.read().decode("utf-8", errors="replace"))
            choices = body.get("choices") or []
            if not choices:
                return None
            message = choices[0].get("message") or {}
            content = (message.get("content") or "").strip()
            reasoning = (message.get("reasoning") or "").strip()
            return content or (reasoning or None)
        except urllib.error.HTTPError as exc:
            try:
                detail = exc.read().decode("utf-8", errors="replace")[:200]
            except Exception:
                detail = exc.reason
            last_err = f"HTTP {exc.code}: {detail}"
            if exc.code < 500:
                print(f"⚠️ checkperson LLM {last_err}", flush=True)
                return None
        except Exception as exc:
            last_err = repr(exc)
        if attempt < attempts:
            time.sleep(float(os.getenv("BOT_CHECKPERSON_LLM_RETRY_DELAY", "1.5")))
    print(f"⚠️ checkperson LLM failed after {attempts} attempts: {last_err}", flush=True)
    return None


# ---------------------------------------------------------------------------
# Context building + analysis
# ---------------------------------------------------------------------------

def _one_line(text: str, *, max_len: int = 120) -> str:
    one = re.sub(r"\s+", " ", (text or "").strip())
    return one if len(one) <= max_len else one[: max_len - 1] + "…"


def _build_llm_context(query: str, snapshot: dict[str, Any], *, k: int = 8) -> str:
    matches = retrieve_similar(query, snapshot, k=k)

    # Department mapping limited to departments that appear in the matches (plus
    # always include the global top few) so the model sees relevant owners.
    dept_map = snapshot.get("department_to_persons") or {}
    relevant_depts: list[str] = []
    for _s, r in matches:
        d = r.get("department")
        if d and d not in relevant_depts:
            relevant_depts.append(d)
    lines: list[str] = []
    lines.append("=== Department → check persons (by past assignment frequency) ===")
    shown = 0
    for dept in relevant_depts:
        ranked = dept_map.get(dept) or []
        if not ranked:
            continue
        persons = ", ".join(f"{p} ({c})" for p, c in ranked[:6])
        lines.append(f"- {dept}: {persons}")
        shown += 1
    if shown == 0:
        # No assignee info for matched depts → show the globally most common owners.
        for dept, ranked in list(dept_map.items())[:8]:
            persons = ", ".join(f"{p} ({c})" for p, c in ranked[:5])
            lines.append(f"- {dept}: {persons}")

    lines.append("")
    lines.append("=== Most similar past issues ===")
    if not matches:
        lines.append("(no similar past issues found)")
    for i, (score, r) in enumerate(matches, 1):
        person = r.get("check_person") or "—"
        title = r.get("title") or r.get("summary") or _one_line(r.get("description", ""), max_len=80)
        detail = r.get("summary") or _one_line(r.get("description", ""), max_len=160)
        lines.append(
            f"{i}. [{r.get('source','?')}] {title}\n"
            f"   Department: {r.get('department') or '—'} | Priority: {r.get('priority') or '—'} | Check Person: {person}\n"
            f"   About: {detail}"
        )
    return "\n".join(lines)


_OUT_RE = re.compile(
    r"(?is)Issue\s*[:：]\s*(?P<issue>.*?)\s*"
    r"Priority\s*[:：]\s*(?P<priority>.*?)\s*"
    r"Department\s*[:：]\s*(?P<department>.*?)\s*"
    r"Check\s*Person\s*[:：]\s*(?P<person>.*?)\s*(?:\n\s*\n|Reason|理由|$)"
)


def _parse_llm_output(text: str) -> Optional[dict[str, str]]:
    if not text:
        return None
    m = _OUT_RE.search(text)
    if not m:
        return None
    issue = _one_line(m.group("issue"))
    priority = (m.group("priority") or "").strip().splitlines()[0].strip()
    department = (m.group("department") or "").strip().splitlines()[0].strip()
    person = (m.group("person") or "").strip().splitlines()[0].strip()
    if not (issue or department or person):
        return None
    pm = re.search(r"(?i)p\s*([0-3])", priority)
    if pm:
        priority = f"P{pm.group(1)}"
    return {
        "issue": issue,
        "priority": priority or "P2",
        "department": department or "Unknown",
        "check_person": person or "CP OM Duty (on-duty)",
    }


def analyze(query: str, *, refresh: bool = False) -> dict[str, Any]:
    """Return a dict: issue, priority, department, check_person, reason, engine, raw."""
    snapshot = load_snapshot(refresh=refresh)
    fallback = deterministic_suggestion(query, snapshot)

    if llm_available():
        context = _build_llm_context(query, snapshot)
        user_text = (
            f"{context}\n\n"
            f"=== New issue to route ===\n\"\"\"\n{query.strip()}\n\"\"\"\n\n"
            "Decide the Issue, Priority, Department and Check Person using the data above. "
            "Reply EXACTLY in the required output format."
        )
        raw = _llm_complete(_SYSTEM_PROMPT, user_text)
        parsed = _parse_llm_output(raw or "")
        if parsed:
            reason = ""
            rm = re.search(r"(?is)(?:Reason|理由)\s*[:：]?\s*(.+)$", raw or "")
            if rm:
                reason = rm.group(1).strip()
            # If the model left a field blank, backfill from the deterministic pick.
            for kf in ("issue", "priority", "department", "check_person"):
                if not parsed.get(kf) or parsed[kf].lower() in ("", "unknown", "n/a"):
                    parsed[kf] = fallback[kf]
            parsed.update({"reason": reason, "engine": _llm_model(), "raw": raw})
            return parsed

    fallback.update({
        "reason": "Matched against the most similar historical issues and the department→assignee mapping.",
        "engine": "rule-based",
        "raw": "",
    })
    return fallback


def format_text(result: dict[str, Any]) -> str:
    lines = [
        f"Issue : {result.get('issue','')}",
        f"Priority : {result.get('priority','')}",
        f"Department : {result.get('department','')}",
        f"Check Person : {result.get('check_person','')}",
    ]
    if result.get("reason"):
        lines.append("")
        lines.append(f"Reason / 理由: {result['reason']}")
    return "\n".join(lines)


def find_check_person(text: str, *, refresh: bool = False) -> str:
    body = strip_command(text)
    if not body:
        return USAGE
    return format_text(analyze(body, refresh=refresh))


# ---------------------------------------------------------------------------
# Command parsing helpers (for bot routing)
# ---------------------------------------------------------------------------

USAGE = (
    "🔎 Find Check Person / 查找跟进人\n"
    "Describe the issue and I'll tell you who should check it, based on past tickets.\n"
    "描述问题，我会根据历史工单判断由谁跟进。\n\n"
    "I will reply with:\n"
    "Issue : ...\nPriority : ...\nDepartment : ...\nCheck Person : ...\n\n"
    "Example / 示例:\n"
    "/checkperson player 12345 cannot login on PC web, error prompt"
)

_CHECKPERSON_CMD_RE = re.compile(r"(?i)^/(?:checkperson|check_person|whochecks|findcheckperson)\b[ \t]*")

# Natural-language trigger: "help me find check person ..." / "who should check ..."
_CHECKPERSON_NL_RE = re.compile(
    r"(?i)("
    r"(?:help\s+(?:me|us)\s+)?(?:find|get|tell\s+me)\s+(?:the\s+)?check\s*person"
    r"|who\s+(?:is|should|will)\s+(?:be\s+)?(?:the\s+)?check\s*person"
    r"|who\s+(?:should|will|can)\s+check\s+this"
    r"|assign\s+(?:a\s+)?check\s*person"
    r"|查找?(?:跟进人|检查人|负责人)"
    r"|谁(?:来)?(?:跟进|检查|负责)"
    r")"
)


def strip_command(text: str) -> str:
    raw = (text or "").strip()
    raw = _CHECKPERSON_CMD_RE.sub("", raw)
    # Also drop a leading natural-language trigger phrase so only the issue remains.
    raw = re.sub(
        r"(?i)^(?:please\s+)?(?:help\s+(?:me|us)\s+)?(?:find|get|tell\s+me)\s+(?:the\s+)?check\s*person\b"
        r"(?:\s+(?:for|of|on|about))?\s*[:：]?\s*",
        "",
        raw,
    )
    return raw.strip()


def looks_like_checkperson_request(text: str) -> bool:
    raw = (text or "").strip()
    if not raw or not is_enabled():
        return False
    if _CHECKPERSON_CMD_RE.match(raw):
        return True
    return bool(_CHECKPERSON_NL_RE.search(raw))


# ---------------------------------------------------------------------------
# Lark interactive card
# ---------------------------------------------------------------------------

def _priority_template(priority: str) -> str:
    p = (priority or "").upper()
    if p == "P0":
        return "red"
    if p == "P1":
        return "orange"
    if p == "P2":
        return "yellow"
    return "blue"


def _div(content: str) -> dict:
    return {"tag": "div", "text": {"tag": "lark_md", "content": content}}


def _note(content: str) -> dict:
    return {"tag": "note", "elements": [{"tag": "lark_md", "content": content}]}


def build_card(text: str, *, refresh: bool = False) -> tuple[Optional[dict], str]:
    body = strip_command(text)
    if not body:
        return None, USAGE
    result = analyze(body, refresh=refresh)
    plain = format_text(result)

    elements: list[dict] = [
        _div(
            f"**Issue:** {result.get('issue','')}\n"
            f"**Priority:** `{result.get('priority','')}`\n"
            f"**Department:** {result.get('department','')}\n"
            f"**Check Person:** **{result.get('check_person','')}**"
        )
    ]
    if result.get("reason"):
        elements.append({"tag": "hr"})
        elements.append(_div(f"**🧠 Reason / 理由**\n{result['reason']}"))

    engine = result.get("engine", "rule-based")
    footer = (
        f"🤖 {engine} · learned from Lark Base"
        if engine != "rule-based"
        else f"⚙️ rule-based (AI offline) · learned from Lark Base"
    )
    elements.append(_note(footer))

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": _priority_template(result.get("priority", "")),
            "title": {"tag": "plain_text", "content": f"🔎 Check Person — {result.get('department','')}"},
        },
        "elements": elements,
    }
    return card, plain


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli(argv: list[str]) -> int:
    refresh = False
    args = []
    for a in argv:
        if a in ("--refresh", "-r"):
            refresh = True
        else:
            args.append(a)

    if refresh and not args:
        print("Rebuilding snapshot from Lark Base…")
        snap = build_snapshot()
        print(f"✅ Snapshot written to {SNAPSHOT_PATH}")
        print(f"   Tables: {snap.get('tables')}")
        print(f"   Records: {len(snap.get('records') or [])}")
        print(f"   Departments with known check persons: {len(snap.get('department_to_persons') or {})}")
        return 0

    if not args:
        print(USAGE)
        return 0

    query = " ".join(args)
    print("=" * 70)
    print(f"AI available: {llm_available()}  model={_llm_model()!r}")
    print("=" * 70)
    print(find_check_person(query, refresh=refresh))
    return 0


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass
    raise SystemExit(_cli(sys.argv[1:]))
