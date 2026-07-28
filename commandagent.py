"""
Natural-language → slash-command router for the Duty Bot.

Resolution order (when ``BOT_USE_AI=1``)
---------------------------------------
1. **Deterministic rules** — prod-batch maintenance, credit checks, identify-issue, etc.
2. **Pattern rules** — catalogue paraphrases from ``build_intent_catalog()`` (fast, high precision).
3. **LLM intent** — OpenAI-compatible API classifies *command vs chat* directly and picks the
   intent tag (needs ``BOT_CHAT_API_KEY`` / ``OPENAI_API_KEY``).

The local DistilBERT classifier has been removed — the language model makes the
command-vs-chat decision directly.

**With AI / without AI**
- Default (without AI): ``BOT_USE_AI`` unset or ``0`` — bot behaves exactly as before (hardcoded ``/`` commands only).
- With AI: set ``BOT_USE_AI=1`` in ``.env`` — English messages are mapped to slash commands before normal handlers run.
- If the LLM is unavailable, confidence is low, or anything throws, the layer returns ``None`` and the bot continues unchanged.

**Inspect**
    python commandagent.py route "who is on fpms duty today"   # show rule/LLM resolution path
    python commandagent.py bench "who is covering fpms shift tonight" -n 5   # LLM latency (ms)
    python commandagent.py bench-all -n 3   # compare all Ollama models (command classify)
    python commandagent.py patterns   # print pattern counts per intent

LLM env (optional, reuses ``chatagent`` API config):
- ``BOT_COMMANDAGENT_LLM`` — set ``0`` to skip LLM even when an API key exists.
- ``BOT_COMMANDAGENT_LLM_MODEL`` — command-routing model (default: ``qwen2.5:0.5b``).
- ``BOT_COMMANDAGENT_LLM_TIMEOUT`` — seconds (default ``15``).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

_CHBOX_DIR = Path(__file__).resolve().parent
# Tag used to say "this is not a command" (casual chat / unknown).
NONE_TAG = "cmd_none"


def startup_status() -> None:
    """Log AI mode at bot boot (check ``journalctl -u larkbot`` after restart)."""
    enabled = is_enabled()
    llm_on = _cmd_llm_enabled()
    print(
        f"[commandagent] BOT_USE_AI={os.getenv('BOT_USE_AI')!r} enabled={enabled} llm={llm_on}",
        flush=True,
    )
    if not enabled:
        print("[commandagent] Natural language OFF — only `/` commands work.", flush=True)
        return
    if llm_on:
        print("[commandagent] ✅ Ready — pattern rules + direct LLM intent.", flush=True)
    else:
        print(
            "[commandagent] ⚠️ No LLM available — pattern rules only. "
            "Set BOT_CHAT_API_KEY / OPENAI_API_KEY to enable AI command/chat routing.",
            flush=True,
        )
    if enabled:
        try:
            t0 = time.perf_counter()
            rows = _get_pattern_index()
            ms = (time.perf_counter() - t0) * 1000
            print(
                f"[commandagent] pattern index warmed: {len(rows)} rows in {ms:.0f}ms",
                flush=True,
            )
            for phrase in (
                "who is on fpms duty today",
                "show me sre duty next week",
                "who is on cpms duty",
                "check credit NCH1422",
            ):
                _match_intent_by_patterns(phrase)
            print("[commandagent] common phrase pattern cache warmed", flush=True)
        except Exception as exc:
            print(f"[commandagent] pattern index warmup skipped: {exc!r}", flush=True)


def is_enabled() -> bool:
    """True when ``BOT_USE_AI=1`` (or ``true`` / ``yes``)."""
    return (os.getenv("BOT_USE_AI") or "").strip().lower() in ("1", "true", "yes", "on")


@dataclass
class IntentSpec:
    tag: str
    command: str  # e.g. "/fpms" or "/s"
    patterns: list[str] = field(default_factory=list)
    arg_kind: Optional[str] = None  # search_name | machine_id | department | rest | date_dmy


# ---------------------------------------------------------------------------
# Training pattern catalogue (English paraphrases → bot commands)
# ---------------------------------------------------------------------------

_DEPTS = (
    "fpms",
    "pms",
    "bi",
    "fe",
    "cpms",
    "sre",
    "db",
    "dba",
    "liveslot",
    "ote",
    "ft",
)

_DEPT_DUTY_TEMPLATES = (
    "who is on {d} duty today",
    "who is on {d} duty",
    "who is {d} today",
    "i want {d} today",
    "show me {d} duty",
    "show {d} duty roster",
    "what is {d} duty today",
    "today {d} on call",
    "today {d} on-call",
    "{d} on call",
    "{d} on call now",
    "on call {d}",
    "{d} duty today",
    "{d} on duty",
    "{d} roster today",
    "list {d} duty",
    "tell me {d} duty",
    "who covers {d} today",
    "who is covering {d} shift tonight",
    "who is covering {d} shift",
    "who covers {d} shift",
    "{d} shift today",
    "current {d} duty",
)

_DEPT_CHECK_TEMPLATES = (
    "check {d} missing duty",
    "{d} duty check this month",
    "who missed {d} duty",
    "{d} attendance check",
    "report missing {d} duty",
)

_MACHINE_PREFIXES = ("nch", "nwr", "wf", "winford", "tbp", "tbr", "cp", "dhs", "mdr")

_MACHINE_TEMPLATES = (
    "lookup {p} {id}",
    "show {p} {id}",
    "check machine {p}{id}",
    "what is {p} {id}",
    "info for {p} {id}",
    "{p} machine {id}",
    "get {p} {id} details",
    "i want machine {p} {id}",
    "machine {p} {id}",
)

_SEARCH_TEMPLATES = (
    "search duty for {name}",
    "find {name} in duty list",
    "who is {name}",
    "look up {name} duty",
    "duty roster {name}",
    "phone number for {name}",
    "contact for {name}",
    "where is {name} on duty",
    "{name} duty info",
)

_LEAVE_TEMPLATES = (
    "who is on leave in {d} this month",
    "{d} leave this month",
    "show {d} leave",
    "list {d} department leave",
    "monthly leave for {d}",
)

_WFH_TEMPLATES = (
    "who is wfh in {d}",
    "{d} work from home this month",
    "show {d} wfh",
    "{d} remote work this month",
)

_SAMPLE_NAMES = ("David", "Henry", "Ryan", "Monlong", "Adrian", "Darren", "Wennie", "BK")
_SAMPLE_MACHINE_IDS = ("1422", "7183", "8092", "8900", "2133", "NCH1422", "NWR2140")

# ---------------------------------------------------------------------------
# Prod-batch maintenance (e.g. ``/nwrsetmaintenance``) — site + op + what
# Mirrors smmachine._PROD_BATCH_BOT_CMD_RE so NL maps to a real command.
# ---------------------------------------------------------------------------
_PB_SITE_WORDS: dict[str, str] = {
    # natural word -> canonical site token used in the slash command
    "nwr": "nwr",
    "np": "nwr",
    "nch": "nch",
    "nc": "nch",
    "tbr": "tbr",
    "tbp": "tbp",
    "mdr": "mdr",
    "dhs": "dhs",
    "cp": "cp",
    "osm": "cp",
    "wf": "wf",
    "winford": "wf",
}
_PB_SITE_DISPLAY = ("nwr", "nch", "winford", "mdr", "tbr", "tbp", "dhs", "cp")
# Verbs that mean "turn on/apply" vs "turn off/remove"
_PB_SET_WORDS = ("set", "enable", "activate", "turn on", "switch on", "put", "apply", "mark", "flag")
_PB_UNSET_WORDS = ("unset", "disable", "deactivate", "turn off", "switch off", "remove", "clear", "cancel", "lift", "take off", "unmark")
_PB_WHAT_BOTH = ("maintenance and test", "maintenance test", "maintenance+test", "test and maintenance", "both maintenance and test", "maintenancetest", "testmaintenance")
_PB_WHAT_MAINT = ("maintenance", "maint", "mtn", "under maintenance", "in maintenance")
_PB_WHAT_TEST = ("test mode", "test")

# Casual human prefixes applied to base patterns so the model sees real phrasing,
# not just clean templates. Keep this list modest — it multiplies dataset size.
_HUMAN_PREFIXES = (
    "",
    "i want ",
    "i wanna ",
    "i need ",
    "can you ",
    "can u ",
    "could you ",
    "pls ",
    "please ",
    "help me ",
    "hey ",
    "hi ",
    "bot ",
    "hey bot ",
    "ok now ",
)
_HUMAN_SUFFIXES = ("", " pls", " please", " thanks", " now", " today", " asap", "?")


def _augment_human(patterns: list[str], *, max_prefixes: int = 6, max_suffixes: int = 3) -> list[str]:
    """Expand clean templates with a few casual human prefixes/suffixes."""
    out: list[str] = []
    seen: set[str] = set()
    prefixes = _HUMAN_PREFIXES[:max_prefixes]
    suffixes = _HUMAN_SUFFIXES[:max_suffixes]
    for pat in patterns:
        base = pat.strip()
        if not base:
            continue
        for pre in prefixes:
            for suf in suffixes:
                variant = f"{pre}{base}{suf}".strip()
                if variant and variant not in seen:
                    seen.add(variant)
                    out.append(variant)
    return out


# Out-of-scope / casual-chat negatives. The model learns to map these to ``cmd_none``
# so casual chat is NOT forced into a slash command (then chatagent handles it).
_NONE_PATTERNS = (
    "hi", "hello", "hey", "yo", "good morning", "good afternoon", "good evening",
    "how are you", "how are you doing", "what's up", "hows it going", "you there",
    "thanks", "thank you", "thx", "ty", "cheers", "appreciate it",
    "bye", "goodbye", "see you", "see ya", "good night", "talk later",
    "lol", "haha", "nice", "cool", "awesome", "great", "ok", "okay", "got it", "sure",
    "i'm bored", "i'm tired", "happy friday", "have a good weekend", "lunch time",
    "who are you", "are you a bot", "tell me a joke",
    "what's the weather", "is it going to rain", "i love you bot", "you're the best",
    "let's chat", "just saying hi", "random question", "nothing much",
    "what do you think", "do you sleep", "are you human", "good job",
    "i'm hungry", "coffee time", "long day today", "so sleepy", "morning everyone",
    "happy new year", "merry christmas", "congrats", "well done team",
    "what time is it", "where are you from", "do you like music", "sing me a song",
    "tell me something funny", "play a game", "rock paper scissors", "flip a coin",
)


def _simple_intent(tag: str, command: str, *pattern_groups: str) -> IntentSpec:
    pats: list[str] = []
    for g in pattern_groups:
        pats.extend(s.strip() for s in g.split("|") if s.strip())
    # Always include the slash form so mixed input still classifies correctly.
    pats.append(command)
    return IntentSpec(tag=tag, command=command, patterns=list(dict.fromkeys(pats)))


def build_intent_catalog() -> list[IntentSpec]:
    intents: list[IntentSpec] = []

    intents.append(
        _simple_intent(
            "cmd_help",
            "/help",
            "help|what can you do|list commands|show commands|command list|how do i use this bot|what are your commands",
        )
    )
    intents.append(
        _simple_intent(
            "cmd_date",
            "/date",
            "what is today|today's date|what date is it|what date is it today|current date|tell me the date|give me today's date",
        )
    )
    intents.append(
        _simple_intent(
            "cmd_holiday",
            "/holiday",
            "upcoming holidays|next public holiday|holiday list|when is the next holiday|public holidays",
        )
    )
    intents.append(
        _simple_intent(
            "cmd_holiday_month",
            "/holidaythismonth",
            "holidays this month|any holiday this month|public holidays this month",
        )
    )
    intents.append(
        _simple_intent(
            "cmd_wholeave",
            "/wholeave",
            "who is on leave today|who is leave today|today leave list|anyone on leave today",
        )
    )
    intents.append(
        _simple_intent(
            "cmd_restart",
            "/restart",
            "restart the bot|reboot bot|restart duty bot",
        )
    )
    intents.append(
        _simple_intent(
            "cmd_restart_services",
            "/restartservices",
            "restart services|restart all services|restart webapp|restart larkbot and webapp|reboot services",
        )
    )
    intents.append(
        _simple_intent(
            "cmd_ose",
            "/ose",
            "ose duty now|who is ose on duty|ose on call|current ose duty",
        )
    )
    intents.append(
        _simple_intent(
            "cmd_cashout",
            "/cashout",
            "cashout reminder|manual cashout template|cash out message",
        )
    )
    intents.append(
        _simple_intent(
            "cmd_smsfail",
            "/smsfail",
            "sms otp failure|check sms fail|sms otp failed today",
        )
    )
    intents.append(
        _simple_intent(
            "cmd_checkcreditdate",
            "/checkcreditdate",
            "check credit by date form|credit check card|open credit check",
        )
    )

    for dept in _DEPTS:
        if dept in ("dba", "db"):
            continue
        cmd = f"/{dept}"
        pats = [t.format(d=dept) for t in _DEPT_DUTY_TEMPLATES]
        pats.extend([f"/{dept}", f"{dept} duty"])
        intents.append(IntentSpec(tag=f"cmd_{dept}", command=cmd, patterns=pats))

    intents.append(
        IntentSpec(
            tag="cmd_db",
            command="/db",
            patterns=[t.format(d="db") for t in _DEPT_DUTY_TEMPLATES]
            + [t.format(d="dba") for t in _DEPT_DUTY_TEMPLATES]
            + ["dba duty today", "database duty today", "/db", "/dba"],
        )
    )

    for dept in _DEPTS:
        if dept in ("dba", "liveslot"):
            cmd = f"/{dept}check" if dept == "liveslot" else f"/{dept}check"
        cmd = f"/{dept}check"
        if dept == "dba":
            continue
        pats = [t.format(d=dept) for t in _DEPT_CHECK_TEMPLATES]
        intents.append(IntentSpec(tag=f"cmd_{dept}check", command=cmd, patterns=pats + [cmd]))

    intents.append(
        _simple_intent(
            "cmd_dutycheckall",
            "/dutycheckall",
            "check all departments duty|all dept missing duty|duty check all teams",
        )
    )

    # Parameterized intents
    search_pats: list[str] = []
    for name in _SAMPLE_NAMES:
        for t in _SEARCH_TEMPLATES:
            search_pats.append(t.format(name=name))
        search_pats.extend([name, f"find {name}", f"/s {name}"])
    intents.append(
        IntentSpec(tag="cmd_search", command="/s", patterns=search_pats, arg_kind="search_name")
    )

    for prefix in _MACHINE_PREFIXES:
        cmd = f"/{prefix}" if prefix != "winford" else "/wf"
        pats: list[str] = [cmd]
        for mid in _SAMPLE_MACHINE_IDS:
            bare = re.sub(r"^[A-Za-z]+", "", mid) or mid
            for t in _MACHINE_TEMPLATES:
                pats.append(t.format(p=prefix, id=bare))
                pats.append(t.format(p=prefix.upper(), id=mid))
        intents.append(
            IntentSpec(tag=f"cmd_{prefix}", command=cmd, patterns=pats, arg_kind="machine_id")
        )

    intents.append(
        IntentSpec(
            tag="cmd_list",
            command="/list",
            patterns=[
                "expand range 8900-8911",
                "list machines 8900 to 8911",
                "expand id range",
                "generate list from range",
                "/list 8900-8911",
            ],
            arg_kind="rest",
        )
    )

    # Credit check — many human phrasings. The actual command + machine are rebuilt
    # deterministically by detect_checkcredit_command (preserves site prefix); these
    # patterns are the fuzzy safety net so the intent classifies with high confidence.
    _cc_machines = ("NWR2065", "NCH1422", "WF8092", "2074", "nwr2113", "DHS3178")
    _cc_pats: list[str] = [
        "check credit", "checkcredit", "credit check", "check the credit",
        "check credit for machine", "credit log check", "credit log",
        "player credit on machine", "player credit", "check player credit",
        "check credit by date", "credit history", "check credit balance",
        "look up credit", "show credit", "view credit log",
    ]
    for _m in _cc_machines:
        _cc_pats.extend([
            f"check credit {_m}", f"check credit machine {_m}",
            f"check credit for machine {_m}", f"credit check {_m}",
            f"check the credit of {_m}", f"player credit on {_m}",
            f"credit log {_m}", f"checkcredit {_m}", f"show credit {_m}",
            f"check credit {_m} 2026-04-27",
        ])
    intents.append(
        IntentSpec(
            tag="cmd_checkcredit",
            command="/checkcredit",
            patterns=_augment_human(_cc_pats, max_prefixes=4, max_suffixes=2),
            arg_kind="machine_id",
        )
    )

    # Machine error — latest two players with error only (/machineerror).
    _me_pats: list[str] = ["machine error", "machineerror", "error only", "machine error log"]
    for _m in _cc_machines:
        _me_pats.extend([
            f"machine error {_m}", f"machineerror {_m}",
            f"machine error for {_m}",
            f"error only {_m}", f"machine error {_m} 2026-04-27",
        ])
    intents.append(
        IntentSpec(
            tag="cmd_machineerror",
            command="/machineerror",
            patterns=_augment_human(_me_pats, max_prefixes=4, max_suffixes=1),
            arg_kind="machine_id",
        )
    )

    _cml_pats: list[str] = [
        "check machine log", "checkmachinelog", "machine log check",
        "check machine error", "machine log error",
    ]
    for _m in _cc_machines:
        _cml_pats.extend([
            f"check machine log {_m}", f"check machine error {_m}",
            f"checkmachinelog {_m}", f"machine log check {_m}",
            f"check machine log {_m} 2026-04-27",
        ])
    intents.append(
        IntentSpec(
            tag="cmd_checkmachinelog",
            command="/checkmachinelog",
            patterns=_augment_human(_cml_pats, max_prefixes=4, max_suffixes=1),
            arg_kind="machine_id",
        )
    )

    _sc_pats: list[str] = ["stuck credit", "credit stuck"]
    for _m in _cc_machines:
        _sc_pats.extend([
            f"{_m} stuck credit", f"stuck credit {_m}",
            f"{_m} credit stuck", f"credit stuck {_m}",
            f"stuck credit {_m} 2026-04-27",
        ])
    intents.append(
        IntentSpec(
            tag="cmd_stuckcredit",
            command="/stuckcredit",
            patterns=_augment_human(_sc_pats, max_prefixes=4, max_suffixes=1),
            arg_kind="machine_id",
        )
    )

    intents.append(
        IntentSpec(
            tag="cmd_cctv",
            command="/cctv",
            patterns=["cctv screenshot", "egm cctv", "show cctv for machine", "camera shot machine"],
            arg_kind="machine_id",
        )
    )

    intents.append(
        IntentSpec(
            tag="cmd_pid",
            command="/pid",
            patterns=["provider id lookup", "find provider id", "pid lookup"],
            arg_kind="rest",
        )
    )

    for dept in ("fpms", "bi", "sre", "db", "fe", "cpms", "pms", "ote"):
        pats = [t.format(d=dept) for t in _LEAVE_TEMPLATES]
        intents.append(
            IntentSpec(
                tag=f"cmd_leave_{dept}",
                command="/leave",
                patterns=pats,
                arg_kind="department",
            )
        )

    intents.append(
        IntentSpec(
            tag="cmd_leave_all",
            command="/leave",
            patterns=["who is on leave this month", "monthly leave list", "leave this month", "/leave"],
            arg_kind="optional_department",
        )
    )

    for dept in ("fpms", "bi", "sre", "fe", "cpms"):
        pats = [t.format(d=dept) for t in _WFH_TEMPLATES]
        intents.append(
            IntentSpec(tag=f"cmd_wfh_{dept}", command="/wfh", patterns=pats, arg_kind="department")
        )

    intents.append(
        IntentSpec(
            tag="cmd_leavewfh",
            command="/leavewfh",
            patterns=["leave and wfh this month", "leave wfh summary", "who is away this month", "/leavewfh"],
        )
    )

    # OSE offset NL is handled by offsetai (reads sheet + LLM) — not commandagent.

    intents.append(
        IntentSpec(
            tag="cmd_ecsre",
            command="/ecsre",
            patterns=["ec sre game owner", "who owns game sre", "game owner ecsre"],
            arg_kind="rest",
        )
    )

    intents.append(
        IntentSpec(
            tag="cmd_ec",
            command="/ec",
            patterns=["emergency contact", "ec contact for game", "who to call emergency"],
            arg_kind="rest",
        )
    )

    # Timed/message reminder: "remind me in 30 minutes ...". Requires an argument
    # (a time/duration + message); build_slash_command returns None without one.
    intents.append(
        IntentSpec(
            tag="cmd_reminder",
            command="/reminder",
            patterns=[
                "remind me in 30 minutes",
                "set a reminder",
                "schedule reminder",
                "remind me",
                "set reminder",
                "schedule a reminder",
                "remind me to call the team",
                "remind me to check the machines",
                "remind me at 8pm",
                "remind me in 1 hour",
                "remind me in 15 minutes",
                "set a reminder for 30 minutes",
                "set a reminder to restart the server",
                "ping me in 20 minutes",
                "remind me tomorrow morning",
            ],
            arg_kind="rest",
        )
    )

    # Bare "add reminder" → opens the reminder form card (main.py /addreminder with
    # no args). arg_kind=None so a plain "/addreminder" is produced (no argument).
    intents.append(
        _simple_intent(
            "cmd_addreminder",
            "/addreminder",
            "add reminder|add a reminder|create reminder|create a reminder|new reminder|"
            "make a reminder|i want to add a reminder|set up a reminder|add reminder for me|"
            "open reminder form|add new reminder|i want to create a reminder|"
            "can you add a reminder|add reminder please|reminder form",
        )
    )

    # Bare "delete reminder" → opens the reminder list card (main.py /deletereminder
    # with no args). arg_kind=None so a plain "/deletereminder" is produced.
    intents.append(
        _simple_intent(
            "cmd_deletereminder",
            "/deletereminder",
            "delete reminder|remove reminder|delete a reminder|remove a reminder|"
            "cancel reminder|delete reminders|remove reminders|list reminders to delete|"
            "i want to delete a reminder|delete my reminder|remove my reminder|"
            "show reminder|show reminders to delete|clear reminder",
        )
    )

    # -- Prod-batch maintenance (the /nwrsetmaintenance family) -------------
    # One intent per (site, action) so the classifier can learn to recognise
    # "nwr set maintenance" etc. The real command + machines are reconstructed
    # deterministically by ``detect_prod_batch_command`` (more reliable than the
    # classifier for structured input); these patterns are the safety net.
    _PB_ACTION_TEMPLATES = {
        "setmaintenance": (
            "{s} set maintenance", "set maintenance for {s}", "set {s} to maintenance",
            "put {s} in maintenance", "enable maintenance on {s}", "mark {s} maintenance",
            "{s} machines set maintenance", "set maintenance mode {s}",
            "turn on maintenance for {s}", "{s} under maintenance",
        ),
        "settest": (
            "{s} set test", "set test for {s}", "set {s} to test",
            "put {s} in test", "enable test on {s}", "{s} test mode",
            "turn on test for {s}", "set test mode {s}",
        ),
        "setmaintenancetest": (
            "{s} set maintenance and test", "set both maintenance and test for {s}",
            "set {s} maintenance test", "{s} set both", "enable maintenance and test on {s}",
        ),
        "unsetmaintenance": (
            "{s} unset maintenance", "remove maintenance from {s}", "disable maintenance on {s}",
            "clear maintenance {s}", "lift maintenance for {s}", "turn off maintenance {s}",
            "take {s} out of maintenance", "cancel maintenance {s}",
        ),
        "unsettest": (
            "{s} unset test", "remove test from {s}", "disable test on {s}",
            "clear test {s}", "turn off test {s}",
        ),
        "unsetmaintenancetest": (
            "{s} unset both", "remove maintenance and test from {s}",
            "disable maintenance and test on {s}", "clear both {s}",
        ),
    }
    # Only a few representative sites are needed for *pattern shape*: the actual
    # site/op/what is re-derived from the message by detect_prod_batch_command,
    # so we keep the dataset small (and CPU training fast).
    _pb_pattern_sites = ("nwr", "nch", "winford")
    for _action, _tmpls in _PB_ACTION_TEMPLATES.items():
        pats: list[str] = []
        for site in _pb_pattern_sites:
            for t in _tmpls:
                pats.append(t.format(s=site))
            pats.append(f"/{('wf' if site == 'winford' else site)}{_action}")
        # canonical command stored on the spec is just a placeholder; the real
        # slash text is rebuilt per-site by detect_prod_batch_command.
        intents.append(
            IntentSpec(
                tag=f"cmd_pb_{_action}",
                command="/SETMAINTENANCE",  # sentinel, replaced at resolve time
                patterns=_augment_human(pats, max_prefixes=4, max_suffixes=1),
                arg_kind="prod_batch",
            )
        )

    # -- Out-of-scope / casual chat: teach the model to abstain --------------
    intents.append(
        IntentSpec(
            tag=NONE_TAG,
            command="",
            patterns=_augment_human(list(_NONE_PATTERNS), max_prefixes=4, max_suffixes=2),
            arg_kind=None,
        )
    )

    return intents


def _looks_like_slash_command(text: str) -> bool:
    s = (text or "").lstrip()
    return s.startswith("/")


# ---------------------------------------------------------------------------
# Deterministic prod-batch (set/unset maintenance/test) reconstruction.
# This is intentionally NOT left to the fuzzy classifier: the command is highly
# structured (site + op + what + machine list) so a rule-based rebuild is far
# more reliable. Returns canonical text the bot's prod-batch handler understands.
# ---------------------------------------------------------------------------

_PB_SITE_RE = re.compile(
    r"(?i)\b(nwr|np|nch|nc|tbr|tbp|mdr|dhs|winford|wf|osm|cp)\b"
)
_PB_MAINT_RE = re.compile(r"(?i)\b(maintenance|maint|mtn)\b|维护|維護")
_PB_TEST_RE = re.compile(r"(?i)\btest\b|测试|測試")
_PB_UNSET_RE = re.compile(
    r"(?i)\b(unset|disable|deactivate|remove|clear|cancel|lift|unmark|"
    r"turn\s+off|switch\s+off|take\s+off|take\s+out)\b"
    r"|取消|解除|关闭|關閉|撤销|撤銷|移除|结束|結束|退出"
)
_PB_SET_RE = re.compile(
    r"(?i)\b(set|enable|activate|put|apply|mark|flag|turn\s+on|switch\s+on)\b"
    r"|设置|設置|设定|設定|开启|開啟|启用|啟用|进入|進入|加入"
)
# A machine token: optional site prefix + 3+ digits, or a display name containing one.
_PB_MACHINE_TOKEN_RE = re.compile(
    r"(?i)\b(?:nwr|nch|mdr|tbr|tbp|dhs|cp|osm|wf|win|winford)\s*-?\s*\d{2,}\b|\b\d{3,}\b"
)


def _pb_extract_machines(text: str) -> list[str]:
    """Pull machine names/ids from a free-form message.

    Priority:
      1. Text after a ``machines:`` / ``machine:`` / ``:`` marker (keeps full names).
      2. Each non-empty line after the first (paste style).
      3. Regex-matched machine tokens anywhere in the text.
    """
    raw = (text or "").strip()
    if not raw:
        return []
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    # 2) multi-line paste: lines after the command line that look like machines
    if len(lines) > 1:
        machines = [ln for ln in lines[1:] if _PB_MACHINE_TOKEN_RE.search(ln)]
        if machines:
            return machines
    # 1) "... machines: A, B, C" / "... : A B C"
    m = re.search(r"(?i)(?:machines?|assets?|egms?)\s*[:\-]\s*(.+)$", raw, re.S)
    if not m:
        m = re.search(r":\s*(.+)$", raw, re.S)
    if m:
        tail = m.group(1).strip()
        if _PB_MACHINE_TOKEN_RE.search(tail):
            parts = re.split(r"[,;&\n]+", tail)
            cleaned = [p.strip() for p in parts if p.strip() and _PB_MACHINE_TOKEN_RE.search(p)]
            if cleaned:
                return cleaned
    # 3) any machine tokens found inline
    found = _PB_MACHINE_TOKEN_RE.findall(raw)
    return [re.sub(r"\s+", "", f) for f in found] if found else []


def detect_prod_batch_command(text: str) -> Optional[str]:
    """Map a natural-language maintenance request to canonical prod-batch text.

    e.g. "i want nwr set maintenance, machines: NWR2113, NWR2114"
         -> "/nwrsetmaintenance\nNWR2113\nNWR2114"

    Returns ``None`` if the message is not a prod-batch maintenance request.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    site_m = _PB_SITE_RE.search(raw)
    if not site_m:
        return None
    has_maint = bool(_PB_MAINT_RE.search(raw))
    has_test = bool(_PB_TEST_RE.search(raw))
    if not (has_maint or has_test):
        return None

    site = _PB_SITE_WORDS.get(site_m.group(1).lower())
    if not site:
        return None

    op = "unset" if _PB_UNSET_RE.search(raw) else ("set" if _PB_SET_RE.search(raw) else None)
    if op is None:
        # No explicit verb but "X maintenance" almost always means set.
        op = "set"

    if has_maint and has_test:
        what = "maintenancetest"
    elif has_maint:
        what = "maintenance"
    else:
        what = "test"

    command = f"/{site}{op}{what}"
    machines = _pb_extract_machines(raw)
    if machines:
        return command + "\n" + "\n".join(machines)
    return command


# ---------------------------------------------------------------------------
# Deterministic check-credit / machine-error reconstruction.
# Like prod-batch, a credit check is structured (intent + machine [+ date]) so a
# rule-based rebuild is far more reliable than the fuzzy classifier — and it works
# even when the model fails to load. CRUCIALLY this preserves the *site prefix*
# (NWR2065, NCH1422, WF8092 …) instead of bare digits: checkcredit.resolve_oss_
# machine_folder() defaults bare digits to NWR{n}, so "check credit NCH1422" must
# keep "NCH1422" or it would wrongly hit NWR1422.
# ---------------------------------------------------------------------------

# Site prefixes accepted in a machine token (mirrors main.py machine handlers).
_CC_SITE_ALT = r"nch|nwr|wf|winford|win|tbp|tbr|cp|osm|dhs|mdr"
# Explicit credit-check / machine-error intent phrases (high precision; avoids
# false positives like "my credit is 500").
_CC_INTENT_RE = re.compile(
    r"(?i)\b(?:"
    r"check\s*credit|credit\s*check|checkcredit|"
    r"credit\s*log|player\s*credit|credit\s*for\s*machine|credit\s*on\s*machine|"
    r"machine\s*error|machineerror|error\s*log"
    r")\b"
    r"|查信用|信用查询|信用查詢|检查信用|檢查信用|查额度|查額度|额度查询|額度查詢|"
    r"查机台信用|查機台信用|信用记录|信用記錄|信用日志|信用日誌"
)
_CC_CREDIT_RE = re.compile(r"(?i)\bcredit\b|信用|额度|額度")
# "machine error" / "machineerror" / "error only" -> /machineerror (latest 2 error players)
_CC_ERROR_ONLY_RE = re.compile(
    r"(?i)\bmachine\s*error\b|\bmachineerror\b|\berror\s*only\b"
    r"|机台错误|機台錯誤|机器错误|機器錯誤|机台报错|機台報錯|机器报错|機器報錯"
)
_CC_DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
# A machine token WITH a site prefix (kept verbatim, spaces/dashes removed).
_CC_PREFIXED_MACHINE_RE = re.compile(
    rf"(?i)\b((?:{_CC_SITE_ALT})\s*-?\s*\d{{2,}})\b"
)
# Bare numeric asset (3+ digits) — only used when no prefixed token is present.
_CC_BARE_MACHINE_RE = re.compile(r"\b(\d{3,})\b")


def _cc_extract_machine(text: str) -> Optional[str]:
    """Return the machine label for a credit check, preserving any site prefix.

    Priority: prefixed token (``NWR2065``) > bare digits (``2065``). Dates are
    stripped first so ``2026-04-27`` is never mistaken for an asset id.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    m = _CC_PREFIXED_MACHINE_RE.search(raw)
    if m:
        return re.sub(r"[\s-]+", "", m.group(1)).upper()
    no_date = _CC_DATE_RE.sub(" ", raw)
    m2 = _CC_BARE_MACHINE_RE.search(no_date)
    return m2.group(1) if m2 else None


def detect_checkcredit_command(text: str) -> Optional[str]:
    """Map a natural-language credit/error request to canonical bot text.

    e.g. "check credit machine NWR2065"           -> "/checkcredit NWR2065"
         "credit check nch1422 2026-04-27"          -> "/checkcredit NCH1422 2026-04-27"
         "machine error for WF8092"                 -> "/machineerror WF8092"

    Returns ``None`` when the message is not a credit/machine-error request.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    if _CML_INTENT_RE.search(raw):
        return None
    if _STUCK_CREDIT_RE.search(raw):
        return None
    is_error = bool(_CC_ERROR_ONLY_RE.search(raw))
    has_intent = bool(_CC_INTENT_RE.search(raw))
    has_credit = bool(_CC_CREDIT_RE.search(raw))
    if not (is_error or has_intent or has_credit):
        return None
    machine = _cc_extract_machine(raw)
    prefixed = bool(_CC_PREFIXED_MACHINE_RE.search(raw))
    # Require a clear credit/error intent phrase, OR a prefixed machine token so a
    # bare "credit 500" never fires. (machineerror already implies intent.)
    if not (is_error or has_intent or prefixed):
        return None
    if not machine:
        return None
    base = "/machineerror" if is_error else "/checkcredit"
    dm = _CC_DATE_RE.search(raw)
    return f"{base} {machine} {dm.group(1)}" if dm else f"{base} {machine}"


# ---------------------------------------------------------------------------
# checkmachinelog — machine log / check machine error (±10 context, transfer-out)
# Runs BEFORE detect_checkcredit_command so "check machine error" maps here,
# not to /machineerror. Bare "machine error" still → /machineerror.
# ---------------------------------------------------------------------------

_CML_INTENT_RE = re.compile(
    r"(?i)\b(?:"
    r"check\s*machine\s*log|checkmachinelog|machine\s*log\s*check|"
    r"check\s*machine\s*error|machine\s*log\s*error"
    r")\b"
    r"|查机台日志|查機台日誌|机台日志|機台日誌|机器日志|機器日誌|"
    r"检查机台日志|檢查機台日誌|机台错误日志|機台錯誤日誌|机台日志检查|機台日誌檢查"
)


def detect_checkmachinelog_command(text: str) -> Optional[str]:
    """e.g. "check machine log DHS3077" -> "/checkmachinelog DHS3077" """
    raw = (text or "").strip()
    if not raw or not _CML_INTENT_RE.search(raw):
        return None
    machine = _cc_extract_machine(raw)
    if not machine:
        return None
    dm = _CC_DATE_RE.search(raw)
    return f"/checkmachinelog {machine} {dm.group(1)}" if dm else f"/checkmachinelog {machine}"


# ---------------------------------------------------------------------------
# stuck credit — last player transfer-out via Third Http (always screenshot)
# Runs BEFORE detect_checkcredit_command ("credit" would false-positive).
# ---------------------------------------------------------------------------

_STUCK_CREDIT_RE = re.compile(
    r"(?i)\b(?:stuck\s+credit|credit\s+stuck)\b"
    r"|卡额度|卡額度|额度卡住|額度卡住|卡信用|信用卡住|卡币|卡幣|卡分"
)


def detect_stuck_credit_command(text: str) -> Optional[str]:
    """e.g. "NWR2938 stuck credit" -> "/stuckcredit NWR2938" """
    raw = (text or "").strip()
    if not raw or not _STUCK_CREDIT_RE.search(raw):
        return None
    machine = _cc_extract_machine(raw)
    if not machine:
        return None
    dm = _CC_DATE_RE.search(raw)
    return f"/stuckcredit {machine} {dm.group(1)}" if dm else f"/stuckcredit {machine}"


_SHOW_REMINDER_RE = re.compile(r"(?i)^show reminder\s*[?.!]*$")


def detect_show_reminder_command(text: str) -> Optional[str]:
    """Map exactly ``show reminder`` → ``/deletereminder`` (reminder list card)."""
    raw = (text or "").strip()
    if _SHOW_REMINDER_RE.match(raw):
        return "/deletereminder"
    return None


_RESTART_SERVICES_RE = re.compile(
    r"(?i)(?:^|\s)(?:restart|reboot)\s+(?:all\s+)?services?(?:\s|$|[.!?])"
    r"|(?:^|\s)restart\s+(?:the\s+)?(?:webapp|web\s+app)(?:\s+and\s+(?:larkbot|bot|duty\s+bot))?(?:\s|$|[.!?])"
    r"|重启(?:所有|全部)?服务|重啟(?:所有|全部)?服務|重新启动(?:所有|全部)?服务|重新啟動(?:所有|全部)?服務"
    r"|重启\s*(?:webapp|web\s*app|网页|網頁|机器人|機器人|bot|duty\s*bot)|重啟\s*(?:webapp|web\s*app|网页|網頁|机器人|機器人|bot|duty\s*bot)"
)

_GIT_PULL_RESTART_RE = re.compile(
    r"(?i)(?:"
    r"git\s+pull(?:\s+and|\s*[,，]?\s*then)?\s+(?:restart|reboot)\s+(?:the\s+)?(?:service|services|bot|larkbot|duty\s+bot|webapp)"
    r"|git\s+pull\s+(?:and\s+)?(?:restart|reboot)\b"
    r"|git\s+pull\b"
    r"|(?:pull|update)\s+(?:code|repo|git)\s+(?:and\s+)?(?:restart|reboot)\s+(?:the\s+)?(?:service|services|bot|larkbot|webapp)"
    r"|(?:deploy|update)\s+(?:the\s+)?(?:bot|code|server|osedutybot)\b"
    r"|拉代码.*重启|部署.*重启"
    r")"
)


def looks_like_git_pull_restart(text: str) -> bool:
    """True for ``/deploy``, ``git pull and restart service``, etc."""
    t = (text or "").strip()
    if not t:
        return False
    if t.lower() in ("/deploy", "/gitpullrestart"):
        return True
    return bool(_GIT_PULL_RESTART_RE.search(t))


def detect_git_pull_restart_command(text: str) -> Optional[str]:
    """Map deploy phrases → ``/gitpullrestart`` (no LLM)."""
    if looks_like_git_pull_restart(text):
        t = (text or "").strip().lower()
        if t in ("/deploy", "/gitpullrestart"):
            return t if t.startswith("/") else "/gitpullrestart"
        return "/gitpullrestart"
    return None


def detect_restart_services_command(text: str) -> Optional[str]:
    """Map natural language → ``/restartservices`` (webapp :8765 + larkbot systemd)."""
    raw = (text or "").strip()
    if not raw or _looks_like_slash_command(raw):
        return None
    if looks_like_git_pull_restart(raw):
        return None
    if _RESTART_SERVICES_RE.search(raw):
        return "/restartservices"
    return None


def detect_checkperson_command(text: str) -> Optional[str]:
    """Map natural language → ``/checkperson`` (AI decides who should check an issue)."""
    raw = (text or "").strip()
    if not raw or _looks_like_slash_command(raw):
        return None
    # Don't collide with the maintenance / credit structured flows.
    if detect_prod_batch_command(raw) or detect_stuck_credit_command(raw) or detect_checkmachinelog_command(raw) or detect_checkcredit_command(raw):
        return None
    try:
        import checkperson as _cp

        if _cp.looks_like_checkperson_request(raw):
            return "/checkperson"
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Argument extraction
# ---------------------------------------------------------------------------

_SEARCH_PREFIX_RE = re.compile(
    r"(?i)^(?:who is|search(?:\s+duty)?(?:\s+for)?|find|look up|lookup|show me|tell me about|"
    r"duty roster for|search duty for|contact for|phone number for|where is|get info on)\s+"
)
_DEPT_IN_TEXT_RE = re.compile(
    r"(?i)\b(fpms|pms|bi|fe|cpms|sre|db|dba|liveslot|ote|ft)\b"
)
# Stable order when one message asks for several department duty rosters.
_MULTI_DUTY_DEPT_ORDER = (
    "ose",
    "fpms",
    "pms",
    "bi",
    "fe",
    "cpms",
    "sre",
    "db",
    "liveslot",
    "ote",
    "ft",
)
_MULTI_DUTY_CONTEXT_RE = re.compile(
    r"(?i)\b("
    r"duty|on[\s-]?call|roster|schedule|shift|today|cover|"
    r"who\s+is\s+on|provide|show|tell|list|give\s+me"
    r")\b"
)
_MULTI_DUTY_SKIP_RE = re.compile(
    r"(?i)\b("
    r"leave|wfh|holiday|maintenance|machine|checkcredit|machineerror|"
    r"update|deploy|reminder|offset|cctv|credit"
    r")\b|/"
)
_MACHINE_ID_RE = re.compile(
    r"(?i)\b(?:(?:nch|nwr|wf|winford|tbr|tbp|cp|osm|dhs|mdr)\s*)?(\d{3,}|[A-Z]{2,4}\d+)\b"
)
_MACHINE_NL_PREFIX_RE = re.compile(
    r"(?i)^(?:i want|show|lookup|check|get|find|info for|machine|asset)\s+"
)
_RANGE_RE = re.compile(r"\d{3,}\s*-\s*\d{3,}")


def _machine_digits(text: str) -> Optional[str]:
    """Return bare machine/asset digits from natural language or slash-style text."""
    text = _MACHINE_NL_PREFIX_RE.sub("", (text or "").strip())
    m = _MACHINE_ID_RE.search(text)
    if not m:
        return None
    raw = re.sub(r"\s+", "", m.group(1))
    dm = re.search(r"(\d{3,})", raw)
    return dm.group(1) if dm else None


# ---------------------------------------------------------------------------
# Bare machine-ID lookup: "TBR2099" / "tbr 2099 encoder" / "NCH1001 的信息" →
# the per-site sheet command ("/tbr 2099"). High precision by construction:
# after stripping machine tokens and lookup-noise words, NOTHING may remain —
# so "set maintenance TBR2099", "TBR2099 stuck credit" etc. never match here
# and keep flowing to the prod-batch / checkcredit / maintenance detectors.
# This runs BEFORE any LLM, fixing the "@Duty Bot TBR2099 → chat" misroute.
# ---------------------------------------------------------------------------
_MACHINE_LOOKUP_SITE_CMDS = {
    "nch": "/nch",
    "nwr": "/nwr",
    "wf": "/wf",
    "win": "/wf",
    "winford": "/wf",
    "tbr": "/tbr",
    "tbp": "/tbp",
    "cp": "/cp",
    "osm": "/cp",
    "dhs": "/dhs",
    "mdr": "/mdr",
}
# Lookbehind/lookahead instead of \b: CJK chars count as \w in Python re, so
# "看tbr2099" / "tbr2099的" would never match with plain word boundaries.
_MACHINE_LOOKUP_TOKEN_RE = re.compile(
    r"(?i)(?<![a-z0-9_])(nch|nwr|winford|win|wf|tbr|tbp|cp|osm|dhs|mdr)\s*-?\s*(\d{1,6})(?!\d)"
)
_MACHINE_LOOKUP_NOISE_RE = re.compile(
    r"(?i)\b(?:encoders?|machines?|assets?|info|information|details?|"
    r"check|checking|lookup|look|up|show|me|find|get|status|stream(?:ing)?|url|"
    r"cctv|mini\s*pc|pc|ip|please|pls|plz|help|hi|hello|the|for|of|is|what|whats|"
    r"about|can|you|u|give|tell|number|id|query|search|need|want|i"
    r")\b"
    r"|[?？!！。，,.:：;；、~～()（）\[\]【】\"'`&＆+＋/／|｜]"
    r"|(?:编码器|机台|机器|资产|信息|资料|详情|状态|查询|查一下|查下|查|帮我|帮忙|麻烦|请问|请|"
    r"看一下|看下|看看|是什么|什么|哪个|你好|我要|我想|给我|你能|你可以|能不能|可不可以|"
    r"一下|你|能|想|要|看|的|了|吗|呢|啊|哦)"
)


def detect_machine_lookup_command(text: str) -> Optional[str]:
    """「TBR2099」「tbr 2099 encoder」「NCH1001 的信息」→ ``/tbr 2099`` 等 site 查机台指令。"""
    raw = (text or "").strip()
    # Group messages reach the router as the FULL body incl. "@_user_1" mention
    # placeholders — strip them or the leftover check below always abstains.
    raw = re.sub(r"@_user_\d+|<at[^>]*>.*?</at>", " ", raw, flags=re.I | re.S).strip()
    if not raw or raw.startswith("/") or len(raw) > 100:
        return None
    toks = _MACHINE_LOOKUP_TOKEN_RE.findall(raw)
    if not toks or len(toks) > 6:
        return None
    bases = {_MACHINE_LOOKUP_SITE_CMDS[t[0].lower()] for t in toks}
    if len(bases) != 1:
        return None
    leftover = _MACHINE_LOOKUP_TOKEN_RE.sub(" ", raw)
    leftover = _MACHINE_LOOKUP_NOISE_RE.sub(" ", leftover)
    if leftover.strip():
        return None
    ids = " ".join(t[1] for t in toks)
    return f"{bases.pop()} {ids}"


def extract_argument(arg_kind: Optional[str], user_text: str, spec: IntentSpec) -> Optional[str]:
    if not arg_kind:
        return None
    text = (user_text or "").strip()
    # Group messages arrive with "@_user_1" mention placeholders / <at> tags. Strip
    # them here so they never leak into the extracted argument (e.g. the router
    # building "/s @_user_1 ryan" from a mention-laden body).
    text = re.sub(r"@_user_\d+|<at[^>]*>.*?</at>", " ", text, flags=re.I | re.S)
    text = re.sub(r"\s+", " ", text).strip()
    if arg_kind == "search_name":
        # A leading slash command ("/s ryan", "/search ryan") must not become part
        # of the name — otherwise the query doubles up to "/s ryan".
        text = re.sub(r"^/(?:s|search)\b\s*", "", text, flags=re.I).strip()
        m = re.search(r"(?i)search duty for\s+(.+)$", text)
        if m:
            q = m.group(1).strip(" ?!.,")
        else:
            q = _SEARCH_PREFIX_RE.sub("", text).strip(" ?!.,")
            q = re.sub(r"(?i)\s+(?:in duty|on duty|duty info|phone|number)\s*$", "", q).strip()
        if not q:
            return None
        # Don't treat a department/site word (or maintenance verbs) as a person name —
        # this is what caused "nwr set maintenance" to fire `/s set maintenance`.
        low = q.lower()
        if _DEPT_IN_TEXT_RE.fullmatch(low) or _PB_SITE_RE.fullmatch(low):
            return None
        if _PB_MAINT_RE.search(low) or _PB_UNSET_RE.search(low) or low in ("set", "unset", "test"):
            return None
        return q or None
    if arg_kind == "prod_batch":
        # Reconstruct the real maintenance command from the message.
        return detect_prod_batch_command(text)
    if arg_kind == "machine_id":
        return _machine_digits(text)
    if arg_kind == "department":
        m = _DEPT_IN_TEXT_RE.search(text)
        return m.group(1).lower() if m else None
    if arg_kind == "optional_department":
        m = _DEPT_IN_TEXT_RE.search(text)
        return m.group(1).lower() if m else None
    if arg_kind == "rest":
        # Drop leading natural-language fluff; keep ids / remainder.
        m = _RANGE_RE.search(text)
        if m:
            return m.group(0).replace(" ", "")
        m = _MACHINE_ID_RE.search(text)
        if m:
            return re.sub(r"\s+", "", m.group(0))
        cleaned = _SEARCH_PREFIX_RE.sub("", text).strip()
        return cleaned or None
    if arg_kind == "date_dmy":
        m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", text)
        return m.group(1) if m else None
    return None


def build_slash_command(spec: IntentSpec, user_text: str) -> Optional[str]:
    base = spec.command
    # Out-of-scope class never maps to a command.
    if spec.tag == NONE_TAG or not base:
        return None
    if spec.arg_kind == "prod_batch":
        # The full canonical command (site+op+what + machines) is rebuilt here.
        return detect_prod_batch_command(user_text)
    # Credit check / machine error: rebuild deterministically so the site prefix
    # (NWR/NCH/WF…) is preserved rather than reduced to bare digits.
    if spec.tag in ("cmd_checkcredit", "cmd_machineerror"):
        cc = detect_checkcredit_command(user_text)
        if cc:
            return cc
        machine = _cc_extract_machine(user_text)
        return f"{base} {machine}" if machine else None
    if spec.tag == "cmd_checkmachinelog":
        cml = detect_checkmachinelog_command(user_text)
        if cml:
            return cml
        machine = _cc_extract_machine(user_text)
        return f"{base} {machine}" if machine else None
    if spec.tag == "cmd_stuckcredit":
        sc = detect_stuck_credit_command(user_text)
        if sc:
            return sc
        machine = _cc_extract_machine(user_text)
        return f"{base} {machine}" if machine else None
    arg = extract_argument(spec.arg_kind, user_text, spec)
    if spec.arg_kind in ("search_name", "machine_id", "rest", "date_dmy") and not arg:
        return None
    if spec.arg_kind == "department" and arg:
        return f"{base} {arg}"
    if spec.arg_kind == "optional_department":
        return f"{base} {arg}" if arg else base
    if arg:
        return f"{base} {arg}"
    return base


# ---------------------------------------------------------------------------
# Pattern-rule matching (runtime, uses the intent catalogue from build_intent_catalog)
# ---------------------------------------------------------------------------

_PATTERN_INDEX_LOCK = threading.Lock()
_PATTERN_INDEX: list[tuple[int, str, IntentSpec]] | None = None
_PATTERN_MATCH_CACHE: dict[str, tuple[str, float, IntentSpec] | None] = {}
_PATTERN_MATCH_CACHE_TTL = 300.0
_PATTERN_MATCH_CACHE_TS: dict[str, float] = {}

# Cheap gate — call the LLM only when the message could plausibly be a command.
_COMMAND_LLM_SIGNAL_RE = re.compile(
    r"(?i)\b("
    r"duty|roster|on[\s-]?call|leave|wfh|holiday|offset|"
    r"fpms|pms|bi|fe|cpms|sre|dba|db|liveslot|ote|ft|ose|"
    r"machine|asset|egm|encoders?|nch|nwr|winford|tbr|tbp|mdr|dhs|osm|"
    r"maintenance|maint|test|credit|cctv|deploy|update|"
    r"reminder|help|restart|cashout|sms|pid|provider|"
    r"who is|find|look ?up|search|phone|contact|check"
    r")\b|/"
)


def _normalize_for_match(text: str) -> str:
    s = re.sub(r"\s+", " ", (text or "").strip().lower())
    return s.strip(" ?!.,")


def _pattern_matches(normalized_text: str, pattern: str) -> bool:
    p = _normalize_for_match(pattern)
    if not p:
        return False
    if normalized_text == p:
        return True
    if p.startswith("/") and (normalized_text == p or normalized_text.startswith(p + " ")):
        return True
    if len(p) >= 4 and p in normalized_text:
        return True
    # Short duty phrases: "fpms duty" inside longer text.
    if len(p) >= 6 and re.search(rf"(?<!\w){re.escape(p)}(?!\w)", normalized_text):
        return True
    return False


def _get_pattern_index() -> list[tuple[int, str, IntentSpec]]:
    """Longest patterns first so specific intents beat generic ones."""
    global _PATTERN_INDEX
    with _PATTERN_INDEX_LOCK:
        if _PATTERN_INDEX is not None:
            return _PATTERN_INDEX
        rows: list[tuple[int, str, IntentSpec]] = []
        seen: set[tuple[str, str]] = set()
        for spec in build_intent_catalog():
            if spec.tag == NONE_TAG:
                continue
            for pat in spec.patterns:
                norm = _normalize_for_match(pat)
                if not norm or (spec.tag, norm) in seen:
                    continue
                seen.add((spec.tag, norm))
                rows.append((len(norm), norm, spec))
        rows.sort(key=lambda r: r[0], reverse=True)
        _PATTERN_INDEX = rows
        return rows


def _match_dept_duty_fast(norm: str) -> tuple[IntentSpec, float] | None:
    """Fast path: ``show me sre duty next week`` → ``cmd_sre`` without scanning the full catalogue."""
    if not norm or "duty" not in norm:
        return None
    by_tag = _intents_by_tag()
    for dept in _DEPTS:
        if dept == "dba":
            continue
        tag = "cmd_db" if dept == "db" else f"cmd_{dept}"
        spec = by_tag.get(tag)
        if not spec:
            continue
        if re.search(rf"(?<!\w){re.escape(dept)}(?!\w)", norm):
            return spec, 0.98
    return None


def _match_intent_by_patterns(text: str) -> tuple[IntentSpec, float] | None:
    """Return ``(spec, confidence)`` when a catalogue pattern matches."""
    raw = (text or "").strip()
    if not raw:
        return None
    now = time.time()
    cached = _PATTERN_MATCH_CACHE.get(raw)
    if cached is not None or raw in _PATTERN_MATCH_CACHE:
        ts = _PATTERN_MATCH_CACHE_TS.get(raw, 0.0)
        if now - ts < _PATTERN_MATCH_CACHE_TTL:
            if cached is None:
                return None
            spec, conf = cached
            return spec, conf

    t0 = time.perf_counter()
    norm = _normalize_for_match(raw)
    fast = _match_dept_duty_fast(norm)
    if fast:
        best = fast
    else:
        best: tuple[IntentSpec, float] | None = None
        for plen, pnorm, spec in _get_pattern_index():
            if _pattern_matches(norm, pnorm):
                conf = 1.0 if pnorm == norm else min(0.98, 0.85 + plen / 200.0)
                if best is None or conf > best[1]:
                    best = (spec, conf)
                if conf >= 0.99:
                    break

    elapsed_ms = (time.perf_counter() - t0) * 1000
    if elapsed_ms > 200:
        print(
            f"[commandagent] pattern match {elapsed_ms:.0f}ms "
            f"hit={best[0].tag if best else None} cache_miss text={raw[:60]!r}",
            flush=True,
        )

    with _PATTERN_INDEX_LOCK:
        _PATTERN_MATCH_CACHE[raw] = best
        _PATTERN_MATCH_CACHE_TS[raw] = now
        if len(_PATTERN_MATCH_CACHE) > 512:
            _PATTERN_MATCH_CACHE.clear()
            _PATTERN_MATCH_CACHE_TS.clear()
    return best


def _intents_by_tag() -> dict[str, IntentSpec]:
    return {spec.tag: spec for spec in build_intent_catalog()}


# ---------------------------------------------------------------------------
# LLM intent classification (command vs chat + intent tag)
# ---------------------------------------------------------------------------

_CMD_LLM_TIMEOUT = float(os.getenv("BOT_COMMANDAGENT_LLM_TIMEOUT", "8"))
_cmd_llm_failed_logged = False
_CMD_LLM_CACHE: dict[str, tuple[float, dict[str, Any] | None]] = {}
_CMD_LLM_CACHE_LOCK = threading.Lock()
_CMD_LLM_CACHE_TTL = 180.0


def _cmd_llm_enabled() -> bool:
    if not is_enabled():
        return False
    if (os.getenv("BOT_COMMANDAGENT_LLM") or "").strip().lower() in ("0", "false", "no", "off"):
        return False
    try:
        import chatagent as ca

        return bool(ca.llm_available())
    except Exception:
        return False


def _cmd_llm_model() -> str:
    return (os.getenv("BOT_COMMANDAGENT_LLM_MODEL") or "qwen2.5:0.5b").strip()


def _cmd_llm_base() -> str:
    try:
        import chatagent as ca

        return ca._llm_base_url()
    except Exception:
        return (os.getenv("BOT_CHAT_API_BASE") or "https://api.openai.com/v1").strip().rstrip("/")


def _cmd_llm_api_key() -> str:
    try:
        import chatagent as ca

        return ca._llm_api_key()
    except Exception:
        return (
            os.getenv("BOT_CHAT_API_KEY")
            or os.getenv("OPENAI_API_KEY")
            or ""
        ).strip()


def _build_llm_intent_catalog() -> str:
    lines: list[str] = []
    for spec in build_intent_catalog():
        if spec.tag == NONE_TAG:
            continue
        extra = f" (arg: {spec.arg_kind})" if spec.arg_kind else ""
        lines.append(f"- {spec.tag}: {spec.command}{extra}")
    return "\n".join(lines)


_CMD_LLM_SYSTEM = (
    "You classify messages for an OSE duty bot on Lark/Feishu.\n"
    "CRITICAL: Do NOT think out loud. Output ONLY one JSON object — no markdown, no explanation.\n"
    "Decide: is this a bot COMMAND (work request) or casual CHAT (greeting, thanks, small talk, "
    "general conversation, math, jokes)?\n"
    "JSON schema:\n"
    '{"route":"command"|"chat","intent_tag":string|null,"confidence":0.0-1.0}\n'
    "- route=chat → intent_tag must be null, confidence = how sure it is casual chat.\n"
    "- route=command → pick the best intent_tag from the catalog below; confidence = how sure.\n"
    "- If unsure, prefer route=chat with lower confidence rather than guessing a command.\n"
    "Examples:\n"
    '"hi" → {"route":"chat","intent_tag":null,"confidence":0.99}\n'
    '"who is on fpms duty today" → {"route":"command","intent_tag":"cmd_fpms","confidence":0.95}\n'
    '"who is covering fpms shift tonight" → {"route":"command","intent_tag":"cmd_fpms","confidence":0.95}\n'
    '"check credit NCH1422" → {"route":"command","intent_tag":"cmd_checkcredit","confidence":0.95}\n'
    '"how are you doing" → {"route":"chat","intent_tag":null,"confidence":0.98}\n'
    '"nwr set maintenance NWR2113" → {"route":"command","intent_tag":"cmd_pb_setmaintenance","confidence":0.9}\n'
    "Intent catalog:\n"
)


def _resolve_llm_intent_tag(tag_s: str) -> Optional[str]:
    """Map LLM tag variants (``fpms``, ``/fpms``) to catalogue tags (``cmd_fpms``)."""
    raw = (tag_s or "").strip()
    if not raw or raw.lower() in ("null", "none"):
        return None
    by_tag = _intents_by_tag()
    if raw in by_tag:
        return raw
    low = raw.lower()
    if low in by_tag:
        return low
    if not low.startswith("cmd_"):
        cand = f"cmd_{low.lstrip('/')}"
        if cand in by_tag:
            return cand
    slash = raw if raw.startswith("/") else f"/{raw.lstrip('/')}"
    for spec in by_tag.values():
        if spec.command.lower() == slash.lower():
            return spec.tag
    return None


def _cmd_llm_is_ollama() -> bool:
    base = _cmd_llm_base().lower()
    return "11434" in base or "ollama" in base


def _ollama_native_base() -> str:
    base = _cmd_llm_base().rstrip("/")
    if base.endswith("/v1"):
        base = base[:-3]
    return base.rstrip("/")


def _extract_route_json_from_llm_body(body: dict[str, Any]) -> dict[str, Any] | None:
    """Parse route JSON from OpenAI-style or Ollama native chat responses."""
    message: dict[str, Any] = {}
    if "choices" in body:
        message = ((body.get("choices") or [{}])[0].get("message") or {})
    elif isinstance(body.get("message"), dict):
        message = body["message"]

    chunks: list[str] = []
    for key in ("content", "reasoning"):
        val = (message.get(key) or "").strip()
        if val:
            chunks.append(val)
    if not chunks:
        return None

    seen: set[str] = set()
    candidates = chunks + ["\n".join(chunks)]
    for text in candidates:
        if text in seen:
            continue
        seen.add(text)
        obj = _parse_llm_json(text)
        if obj and str(obj.get("route") or "").lower() in ("command", "chat"):
            return obj

    combined = candidates[-1]
    best: dict[str, Any] | None = None
    pos = 0
    while True:
        a = combined.find("{", pos)
        if a == -1:
            break
        obj = _parse_llm_json(combined[a:])
        if obj and str(obj.get("route") or "").lower() in ("command", "chat"):
            best = obj
        pos = a + 1
    return best


def _apply_ollama_cmd_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not _cmd_llm_is_ollama():
        return payload
    payload["think"] = False
    payload["format"] = "json"
    try:
        import chatagent as ca

        payload = ca.enrich_ollama_chat_payload(payload, think=False)
    except Exception:
        pass
    payload["think"] = False
    payload["format"] = "json"
    return payload


def _llm_route_obj_to_result(obj: dict[str, Any]) -> dict[str, Any] | None:
    route = str(obj.get("route") or "").strip().lower()
    tag = obj.get("intent_tag")
    tag_s = (
        str(tag).strip()
        if tag is not None and str(tag).strip().lower() not in ("null", "none")
        else ""
    )
    try:
        conf = float(obj.get("confidence") or 0.0)
    except (TypeError, ValueError):
        conf = 0.0
    conf = max(0.0, min(1.0, conf))
    if route not in ("command", "chat"):
        print(f"[commandagent] LLM bad route={route!r} obj={obj!r}", flush=True)
        return None
    if route == "chat":
        return {
            "route": "chat",
            "intent_tag": NONE_TAG,
            "confidence": conf,
            "source": "llm",
        }
    resolved_tag = _resolve_llm_intent_tag(tag_s)
    spec = _intents_by_tag().get(resolved_tag or "")
    if not spec or resolved_tag == NONE_TAG:
        print(
            f"[commandagent] LLM unknown intent_tag={tag_s!r} "
            f"resolved={resolved_tag!r} obj={obj!r}",
            flush=True,
        )
        return None
    return {
        "route": "command",
        "intent_tag": resolved_tag,
        "confidence": conf,
        "source": "llm",
        "spec": spec,
    }


def _correct_llm_duty_misroute(raw: str, result: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Fix common small-model swaps (check) on plain duty roster questions."""
    if not result or result.get("route") != "command":
        return result
    m = re.search(r"(?i)\b(fpms|pms|bi|fe|cpms|sre|db|dba|liveslot|ote|ft)\b", raw or "")
    dept = (m.group(1).lower() if m else "") or ""
    if dept == "dba":
        dept = "db"
    duty_q = bool(
        re.search(
            r"(?i)\b(who is on|who covers|who is covering|on duty|on call|"
            r"shift tonight|shift today|duty today|roster)\b",
            raw or "",
        )
    )
    check_q = bool(re.search(r"(?i)\b(check|missing|attendance|verify)\b", raw or ""))
    tag = str(result.get("intent_tag") or "")
    if not duty_q or check_q:
        return result
    if tag.endswith("check"):
        fix_tag = f"cmd_{dept}" if dept else tag.replace("check", "").rstrip("_") or tag
        if not _intents_by_tag().get(fix_tag):
            fix_tag = f"cmd_{dept}" if dept else "cmd_fpms"
    else:
        return result
    spec = _intents_by_tag().get(fix_tag)
    if not spec:
        return result
    return {**result, "intent_tag": fix_tag, "spec": spec}


def _classify_intent_llm_ollama_native(
    raw: str, system: str, model: str, *, timeout: Optional[float] = None
) -> dict[str, Any] | None:
    """Ollama ``/api/chat`` with ``think=false`` + ``format=json`` (fast, reliable JSON)."""
    url = f"{_ollama_native_base()}/api/chat"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": raw},
        ],
        "stream": False,
        "think": False,
        "format": "json",
        "options": {"temperature": 0, "num_predict": 64},
        "keep_alive": -1,
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout or _CMD_LLM_TIMEOUT) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    obj = _extract_route_json_from_llm_body(body)
    if not obj:
        msg = body.get("message") or {}
        print(
            f"[commandagent] Ollama native parse failed content={(msg.get('content') or '')[:160]!r}",
            flush=True,
        )
        return None
    return _correct_llm_duty_misroute(raw, _llm_route_obj_to_result(obj))


def _classify_intent_llm_openai_compat(
    raw: str, system: str, model_name: str, api_key: str
) -> dict[str, Any] | None:
    """OpenAI-compatible API (non-Ollama backends)."""
    payload: dict[str, Any] = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": raw},
        ],
        "max_tokens": 256,
        "temperature": 0,
    }
    payload = _apply_ollama_cmd_payload(payload)
    req = urllib.request.Request(
        f"{_cmd_llm_base()}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=_CMD_LLM_TIMEOUT) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    obj = _extract_route_json_from_llm_body(body)
    if not obj:
        msg = ((body.get("choices") or [{}])[0].get("message") or {})
        print(
            f"[commandagent] OpenAI-compat parse failed content={(msg.get('content') or '')[:160]!r} "
            f"reasoning={(msg.get('reasoning') or '')[:160]!r}",
            flush=True,
        )
        return None
    return _correct_llm_duty_misroute(raw, _llm_route_obj_to_result(obj))


def _llm_message_text(body: dict[str, Any]) -> str:
    """Extract assistant text; qwen thinking models may leave ``content`` empty."""
    message = ((body.get("choices") or [{}])[0].get("message") or {})
    try:
        import chatagent as ca

        return ca._text_from_llm_message(message)
    except Exception:
        content = (message.get("content") or "").strip()
        reasoning = (message.get("reasoning") or "").strip()
        return content or reasoning


def _parse_llm_json(content: str) -> dict[str, Any] | None:
    s = (content or "").strip()
    if not s:
        return None
    s = re.sub(r"^```(?:json)?|```$", "", s.strip(), flags=re.I | re.M).strip()
    a, b = s.find("{"), s.rfind("}")
    if a == -1 or b == -1 or b < a:
        return None
    try:
        obj = json.loads(s[a : b + 1])
    except ValueError:
        return None
    return obj if isinstance(obj, dict) else None


def _classify_intent_llm(text: str) -> dict[str, Any] | None:
    """
    LLM routing classification. Returns dict with keys:
    ``route`` (command|chat), ``intent_tag``, ``confidence``, ``source``=llm.
    """
    global _cmd_llm_failed_logged
    raw = (text or "").strip()
    if not raw or not _cmd_llm_enabled():
        return None

    with _CMD_LLM_CACHE_LOCK:
        hit = _CMD_LLM_CACHE.get(raw)
        if hit and (time.time() - hit[0]) < _CMD_LLM_CACHE_TTL:
            return hit[1]

    api_key = _cmd_llm_api_key()
    if not api_key:
        return None

    system = _CMD_LLM_SYSTEM + _build_llm_intent_catalog()
    model_name = _cmd_llm_model()
    result: dict[str, Any] | None = None
    t0 = time.perf_counter()
    api_used = "ollama_native" if _cmd_llm_is_ollama() else "openai_compat"
    try:
        print(
            f"[commandagent] LLM classify: model={model_name!r} api={api_used} text={raw[:80]!r}",
            flush=True,
        )
        if _cmd_llm_is_ollama():
            result = _classify_intent_llm_ollama_native(raw, system, model_name)
        else:
            result = _classify_intent_llm_openai_compat(raw, system, model_name, api_key)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        route_s = (result or {}).get("route") or "none"
        print(
            f"[commandagent] LLM classify done: {elapsed_ms:.0f}ms api={api_used} "
            f"model={model_name!r} route={route_s}",
            flush=True,
        )
    except urllib.error.HTTPError as exc:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(f"[commandagent] LLM classify failed after {elapsed_ms:.0f}ms", flush=True)
        if not _cmd_llm_failed_logged:
            try:
                detail = exc.read().decode("utf-8", errors="replace")[:300]
            except Exception:
                detail = exc.reason
            print(f"⚠️ commandagent LLM HTTP {exc.code}: {detail}", flush=True)
            _cmd_llm_failed_logged = True
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(f"[commandagent] LLM classify failed after {elapsed_ms:.0f}ms", flush=True)
        if not _cmd_llm_failed_logged:
            print(f"⚠️ commandagent LLM request failed: {exc!r}", flush=True)
            _cmd_llm_failed_logged = True

    with _CMD_LLM_CACHE_LOCK:
        _CMD_LLM_CACHE[raw] = (time.time(), result)
        if len(_CMD_LLM_CACHE) > 256:
            _CMD_LLM_CACHE.clear()
    return result


_NONE_PATTERN_INDEX: list[str] | None = None


def _get_none_pattern_norms() -> list[str]:
    global _NONE_PATTERN_INDEX
    if _NONE_PATTERN_INDEX is not None:
        return _NONE_PATTERN_INDEX
    norms: list[str] = []
    seen: set[str] = set()
    for spec in build_intent_catalog():
        if spec.tag != NONE_TAG:
            continue
        for pat in spec.patterns:
            norm = _normalize_for_match(pat)
            if norm and norm not in seen:
                seen.add(norm)
                norms.append(norm)
    norms.sort(key=len, reverse=True)
    _NONE_PATTERN_INDEX = norms
    return norms


def _match_none_by_patterns(text: str) -> bool:
    """True when ``text`` matches a ``cmd_none`` catalogue pattern (casual chat)."""
    norm = _normalize_for_match(text)
    if not norm:
        return False
    for pnorm in _get_none_pattern_norms():
        if _pattern_matches(norm, pnorm):
            return True
    return False


def _resolve_command_for_route(text: str, cmd_sig: dict[str, Any]) -> Optional[str]:
    """Best-effort slash command for routing (pattern fill-in when keyword hit lacks one)."""
    cmd = cmd_sig.get("command")
    if cmd:
        return cmd
    pat_hit = _match_intent_by_patterns(text)
    if pat_hit:
        spec, _ = pat_hit
        built = build_slash_command(spec, text)
        if built:
            return built
        if spec.arg_kind is None and spec.command:
            return spec.command
    # Bare machine-ID lookup ("TBR2099", "tbr 2099 encoder") — no verb needed.
    ml = detect_machine_lookup_command(text)
    if ml:
        return ml
    # Machine prefix + digits (e.g. "lookup nwr 2005") when not in training samples.
    m = re.search(
        r"(?i)\b(?:lookup|show|check|get|find|info for|machine)\s+"
        r"(nch|nwr|wf|winford|tbp|tbr|cp|dhs|mdr)\s*-?\s*(\d{2,})\b",
        text,
    )
    if m:
        prefix = m.group(1).lower()
        cmd_base = "/wf" if prefix == "winford" else f"/{prefix}"
        return f"{cmd_base} {m.group(2)}"
    m2 = re.search(
        r"(?i)\b(nch|nwr|wf|winford|tbp|tbr|cp|dhs|mdr)\s*-?\s*(\d{2,})\b",
        text,
    )
    if m2 and (
        re.search(
            r"(?i)\b(lookup|show|check|machine|asset|info|encoders?|status|streaming|url|ip)\b",
            text,
        )
        or re.search(r"编码器|机台|状态|信息|资料", text)
    ):
        prefix = m2.group(1).lower()
        cmd_base = "/wf" if prefix == "winford" else f"/{prefix}"
        return f"{cmd_base} {m2.group(2)}"
    return None


def _looks_like_pure_chitchat(text: str) -> bool:
    if _match_none_by_patterns(text):
        return True
    try:
        import chitchat

        return chitchat.looks_like_chitchat(text)
    except Exception:
        return False


def _resolve_fuzzy_intent(text: str, *, skip_patterns: bool = False) -> dict[str, Any]:
    """
    Pattern rules → direct LLM. Returns partial signal dict:
    tag, confidence, margin, command, source, route.

    Set ``skip_patterns=True`` when the caller already ran ``_match_intent_by_patterns``.
    """
    out: dict[str, Any] = {
        "tag": None,
        "confidence": 0.0,
        "margin": 0.0,
        "command": None,
        "source": None,
        "route": None,
    }
    raw = (text or "").strip()
    if not raw:
        return out

    # 1) Pattern catalogue
    if not skip_patterns:
        pat_hit = _match_intent_by_patterns(raw)
        if pat_hit:
            spec, conf = pat_hit
            cmd = build_slash_command(spec, raw)
            if cmd or spec.arg_kind is None:
                out.update(
                    tag=spec.tag,
                    confidence=conf,
                    margin=conf,
                    command=cmd,
                    source="pattern",
                    route="command" if spec.tag != NONE_TAG else "chat",
                )
                if out["route"] == "command" and cmd:
                    return out
                if out["route"] == "command" and spec.arg_kind is None:
                    out["command"] = spec.command
                    return out

    # 2) LLM (skip obvious chitchat without work signals)
    llm_ok = _cmd_llm_enabled() and (
        _COMMAND_LLM_SIGNAL_RE.search(raw) or not _looks_like_pure_chitchat(raw)
    )
    if llm_ok:
        llm = _classify_intent_llm(raw)
        if llm:
            route = llm.get("route")
            conf = float(llm.get("confidence") or 0.0)
            if route == "chat" and conf >= 0.55:
                out.update(
                    tag=NONE_TAG,
                    confidence=conf,
                    margin=conf,
                    command=None,
                    source="llm",
                    route="chat",
                )
                return out
            if route == "command" and conf >= 0.50:
                spec = llm.get("spec") or _intents_by_tag().get(str(llm.get("intent_tag") or ""))
                if spec:
                    cmd = build_slash_command(spec, raw)
                    if cmd or spec.arg_kind is None:
                        out.update(
                            tag=spec.tag,
                            confidence=conf,
                            margin=conf,
                            command=cmd or spec.command,
                            source="llm",
                            route="command",
                        )
                        return out

    return out


def _detect_offset_leave_rule_command(text: str) -> dict[str, Any] | None:
    """Map offset/leave rule phrases to slash-style commands (no LLM)."""
    raw = (text or "").strip()
    if not raw:
        return None
    slash_map = {
        "offset": "offset_form",
        "deleteoffset": "delete_offset",
        "editoffset": "edit_offset",
        "pendingoffset": "pending_offset",
        "showoffset": "show_offset",
    }
    token = raw.lower().split()[0]
    if token.startswith("/"):
        token = token[1:]
    if token in slash_map:
        action = slash_map[token]
        return dict(
            tag=f"cmd_{action}",
            confidence=1.0,
            margin=1.0,
            command=f"/{token}",
            deterministic=True,
            source="offset_rule",
            route="command",
        )
    if _looks_like_slash_command(raw):
        return None
    try:
        import offsetleave as ol

        action = ol._parse_offset_leave_action_rules(raw)
    except Exception:
        return None
    cmd_by_action = {
        "offset_form": "/offset",
        "leave_form": "/oseleave",
        "edit_offset": "/editoffset",
        "delete_offset": "/deleteoffset",
        "pending_offset": "/pendingoffset",
        "show_offset": "/showoffset",
    }
    cmd = cmd_by_action.get(action or "")
    if not cmd:
        return None
    return dict(
        tag=f"cmd_{action}",
        confidence=1.0,
        margin=1.0,
        command=cmd,
        deterministic=True,
        source="offset_rule",
        route="command",
    )


def _run_deterministic_detectors(text: str) -> dict[str, Any] | None:
    """High-precision structured command detectors. Returns signal dict or None."""
    raw = (text or "").strip()
    if not raw:
        return None
    ol_cmd = _detect_offset_leave_rule_command(raw)
    if ol_cmd:
        return ol_cmd
    pb = detect_prod_batch_command(raw)
    if pb:
        return dict(tag="cmd_pb", confidence=1.0, margin=1.0, command=pb, deterministic=True, source="deterministic", route="command")
    cml = detect_checkmachinelog_command(raw)
    if cml:
        return dict(tag="cmd_checkmachinelog", confidence=1.0, margin=1.0, command=cml, deterministic=True, source="deterministic", route="command")
    sc = detect_stuck_credit_command(raw)
    if sc:
        return dict(tag="cmd_stuckcredit", confidence=1.0, margin=1.0, command=sc, deterministic=True, source="deterministic", route="command")
    cc = detect_checkcredit_command(raw)
    if cc:
        tag = "cmd_machineerror" if cc.startswith("/machineerror") else "cmd_checkcredit"
        return dict(tag=tag, confidence=1.0, margin=1.0, command=cc, deterministic=True, source="deterministic", route="command")
    sr = detect_show_reminder_command(raw)
    if sr:
        return dict(tag="cmd_deletereminder", confidence=1.0, margin=1.0, command=sr, deterministic=True, source="deterministic", route="command")
    gp = detect_git_pull_restart_command(raw)
    if gp:
        return dict(tag="cmd_gitpullrestart", confidence=1.0, margin=1.0, command=gp, deterministic=True, source="deterministic", route="command")
    rs = detect_restart_services_command(raw)
    if rs:
        return dict(tag="cmd_restart_services", confidence=1.0, margin=1.0, command=rs, deterministic=True, source="deterministic", route="command")
    cp = detect_checkperson_command(raw)
    if cp:
        return dict(tag="cmd_checkperson", confidence=1.0, margin=1.0, command=cp, deterministic=True, source="deterministic", route="command")
    du = detect_single_duty_command(raw)
    if du:
        return dict(tag="cmd_duty", confidence=1.0, margin=1.0, command=du, deterministic=True, source="deterministic", route="command")
    ml = detect_machine_lookup_command(raw)
    if ml:
        return dict(tag="cmd_machine_lookup", confidence=1.0, margin=1.0, command=ml, deterministic=True, source="deterministic", route="command")
    return None


def multi_duty_enabled() -> bool:
    """When on (default), detect 2+ department names in one duty request."""
    return (os.getenv("BOT_MULTI_DUTY") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def detect_multi_duty_commands(text: str) -> Optional[list[str]]:
    """
    If ``text`` asks for duty/roster for **two or more** departments, return
    slash commands in stable order (e.g. ``['/fpms', '/cpms']``).

    Deterministic (no LLM). Runs before single-intent ``translate_if_enabled``.
    """
    if not multi_duty_enabled():
        return None
    raw = (text or "").strip()
    if not raw or _looks_like_slash_command(raw):
        return None
    if not _MULTI_DUTY_CONTEXT_RE.search(raw):
        return None
    if _MULTI_DUTY_SKIP_RE.search(raw):
        return None
    if detect_prod_batch_command(raw) or detect_stuck_credit_command(raw) or detect_checkmachinelog_command(raw) or detect_checkcredit_command(raw):
        return None
    found_depts: list[str] = []
    for dept in _MULTI_DUTY_DEPT_ORDER:
        if re.search(rf"(?i)\b{re.escape(dept)}\b", raw):
            found_depts.append(dept)
    if re.search(r"(?i)\bdba\b", raw) and "db" not in found_depts:
        found_depts.append("db")

    if len(found_depts) < 2:
        return None

    order_idx = {d: i for i, d in enumerate(_MULTI_DUTY_DEPT_ORDER)}
    found_depts.sort(key=lambda d: order_idx.get(d, 999))
    return [f"/{d}" for d in found_depts]


# Filler words that don't change a single-department duty ask ("sre duty today",
# "who is on fpms", "db pls"). Anything left over after removing the department +
# these = a real sentence, so we DON'T hijack it (let patterns / the LLM decide).
_SINGLE_DUTY_FILLER_RE = re.compile(
    r"(?i)\b("
    r"duty|roster|on[\s-]?call|oncall|schedule|shift|"
    r"today|now|tonight|tomorrow|tmr|tmrw|tmmr|tmw|tomo|"
    r"who|whos|is|are|on|of|the|a|for|me|us|my|our|"
    r"please|pls|kindly|thanks|thx|ty|"
    r"show|tell|give|list|provide|check|see|want|need|"
    r"what|whats|current|currently|any"
    r")\b"
)


def detect_single_duty_command(text: str) -> Optional[str]:
    """Map a bare single-department ask to its slash command, deterministically (no LLM).

    Fires only when the message is *essentially just one department name* (plus
    filler / a near-date word) — e.g. ``sre``, ``fpms duty today``, ``who is on db``.
    Returns ``None`` for real sentences ("fe is broken"), zero departments, or 2+
    departments (``detect_multi_duty_commands`` owns those).
    """
    raw = (text or "").strip()
    # Group messages arrive as "@_user_1 cpms" — strip the mention placeholder so the
    # residue check below doesn't see "user 1" and bail out (which forced a slow LLM
    # fallback and meant bare dept names only worked in DMs, not group chats).
    raw = re.sub(r"@_user_\d+|<at[^>]*>.*?</at>", " ", raw, flags=re.I | re.S).strip()
    if not raw or _looks_like_slash_command(raw):
        return None
    if _MULTI_DUTY_SKIP_RE.search(raw):
        return None

    found: list[str] = []
    for dept in _MULTI_DUTY_DEPT_ORDER:
        if re.search(rf"(?i)\b{re.escape(dept)}\b", raw):
            found.append(dept)
    if re.search(r"(?i)\bdba\b", raw) and "db" not in found:
        found.append("db")
    if len(found) != 1:
        return None

    dept = found[0]
    residue = re.sub(rf"(?i)\b{re.escape(dept)}\b", " ", raw)
    if dept == "db":
        residue = re.sub(r"(?i)\bdba\b", " ", residue)
    residue = _SINGLE_DUTY_FILLER_RE.sub(" ", residue)
    residue = re.sub(r"[^0-9A-Za-z一-鿿]+", " ", residue).strip()
    if residue:
        return None  # leftover meaningful words → not a bare duty ask
    return f"/{dept}"


def command_signal(text: str, *, allow_llm: bool = True) -> dict[str, Any]:
    """Diagnostic signal for the router (``chathandleagent``).

    Returns ``tag``, ``confidence``, ``margin``, ``command``, ``deterministic``,
    ``source`` (deterministic|pattern|offset_rule|llm), and ``route`` (command|chat).

    When ``allow_llm=False``, stops after deterministic + pattern rules (no LLM).
    Never raises.
    """
    out: dict[str, Any] = {
        "tag": None,
        "confidence": 0.0,
        "margin": 0.0,
        "command": None,
        "deterministic": False,
        "source": None,
        "route": None,
    }
    raw = (text or "").strip()
    if not raw:
        return out
    det = _run_deterministic_detectors(raw)
    if det:
        out.update(det)
        out["deterministic"] = True
        return out

    # Pattern catalogue is cheap — always run for routing diagnostics.
    pat_hit = _match_intent_by_patterns(raw)
    if pat_hit:
        spec, conf = pat_hit
        cmd = build_slash_command(spec, raw)
        if cmd or spec.arg_kind is None:
            out.update(
                tag=spec.tag,
                confidence=conf,
                margin=conf,
                command=cmd or spec.command,
                source="pattern",
                route="command" if spec.tag != NONE_TAG else "chat",
            )
            return out

    if not is_enabled() or not allow_llm:
        return out
    fuzzy = _resolve_fuzzy_intent(raw, skip_patterns=True)
    out.update(fuzzy)
    out["deterministic"] = False
    return out


def translate_if_enabled(text: str) -> Optional[str]:
    """
    Map natural English to a slash command when AI is enabled.

    Order: deterministic rules → pattern catalogue → direct LLM intent.
    Returns ``None`` when disabled, already ``/…``, classified as chat, low confidence, or on error.
    """
    if not is_enabled():
        return None
    raw = (text or "").strip()
    if not raw or _looks_like_slash_command(raw):
        return None

    rules_cmd = resolve_command_rules_only(raw)
    if rules_cmd:
        print(f"[commandagent] rules map: {raw[:80]!r} -> {str(rules_cmd).splitlines()[0]!r}", flush=True)
        return rules_cmd

    det = _run_deterministic_detectors(raw)
    if det and det.get("command"):
        src = det.get("source") or "deterministic"
        cmd = det["command"]
        print(f"[commandagent] {src} map: {raw[:80]!r} -> {str(cmd).splitlines()[0]!r}", flush=True)
        return cmd

    fuzzy = _resolve_fuzzy_intent(raw)
    if fuzzy.get("route") == "chat":
        return None
    cmd = fuzzy.get("command")
    if not cmd:
        if not _cmd_llm_enabled():
            print(f"⚠️ AI enabled but no LLM available for {raw!r} — pattern rules only", flush=True)
        return None
    src = fuzzy.get("source") or "fuzzy"
    print(f"[commandagent] {src} map: {raw[:80]!r} -> {str(cmd).splitlines()[0]!r}", flush=True)
    return cmd


def resolve_command_rules_only(text: str) -> Optional[str]:
    """Map natural language to a slash command using rules only (no LLM)."""
    if not is_enabled():
        return None
    raw = (text or "").strip()
    if not raw or _looks_like_slash_command(raw):
        return None
    sig = command_signal(raw, allow_llm=False)
    if sig.get("route") == "command" and sig.get("command"):
        return sig["command"]
    return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_CLI_ENV_KEYS = (
    "BOT_USE_AI",
    "BOT_CHAT_API_BASE",
    "BOT_CHAT_API_KEY",
    "BOT_CHAT_MODEL",
    "BOT_COMMANDAGENT_LLM_MODEL",
    "BOT_COMMANDAGENT_LLM",
    "OPENAI_API_KEY",
)


def _read_dotenv_key(key: str, env_file: Path) -> Optional[str]:
    if not env_file.is_file():
        return None
    try:
        lines = env_file.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    prefix = f"{key}="
    for line in reversed(lines):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s.startswith("export "):
            s = s[7:].strip()
        if not s.startswith(prefix):
            continue
        val = s[len(prefix) :].strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        return val
    return None


def _load_cli_env() -> None:
    """Load LLM-related keys from repo ``.env`` without ``source`` (safe on broken files)."""
    env_file = _CHBOX_DIR / ".env"
    for key in _CLI_ENV_KEYS:
        if os.environ.get(key):
            continue
        val = _read_dotenv_key(key, env_file)
        if val:
            os.environ[key] = val


def _cli_route(phrase: str) -> None:
    """Show full resolution path: deterministic → pattern → direct LLM."""
    _load_cli_env()
    os.environ.setdefault("BOT_USE_AI", "1")
    os.environ.setdefault("BOT_CHAT_API_BASE", "http://127.0.0.1:11434/v1")
    os.environ.setdefault("BOT_CHAT_API_KEY", "ollama")

    llm_on = _cmd_llm_enabled()
    cmd_model = _cmd_llm_model()
    api_base = _cmd_llm_base()
    print(f"Config:      BOT_USE_AI={os.getenv('BOT_USE_AI')!r} llm_enabled={llm_on}")
    print(f"Command LLM: model={cmd_model!r} base={api_base!r}")
    if not llm_on:
        print(
            "Hint:        set BOT_CHAT_API_KEY=ollama and run Ollama, "
            "or fix .env / export vars before testing LLM routing.",
            flush=True,
        )

    det = _run_deterministic_detectors(phrase)
    if det:
        print(f"Input:       {phrase!r}")
        print(f"Source:      deterministic")
        print(f"Tag:         {det.get('tag')}")
        print(f"Route:       {det.get('route')}")
        print(f"Command:     {det.get('command')!r}")
        return
    pat = _match_intent_by_patterns(phrase)
    fuzzy = _resolve_fuzzy_intent(phrase)
    sig = command_signal(phrase)
    print(f"Input:       {phrase!r}")
    if pat:
        print(f"Pattern:     {pat[0].tag} ({pat[1]:.3f})")
    else:
        print("Pattern:     —")
    print(f"Resolved:    source={sig.get('source')} route={sig.get('route')} tag={sig.get('tag')}")
    print(f"Confidence:  {sig.get('confidence', 0):.3f}  margin={sig.get('margin', 0):.3f}")
    print(f"Command:     {sig.get('command')!r}")
    if sig.get("source") == "llm":
        print(f"LLM model:   {cmd_model!r} (command routing only)")
    elif not sig.get("source") and llm_on:
        print("LLM:         called but returned no match (check Ollama: ollama ps / curl API)")
    elif not sig.get("source") and not llm_on:
        print("LLM:         skipped (not enabled / no API key)")
    print(f"Translate:   {translate_if_enabled(phrase)!r}")


def _ollama_ps_models() -> list[dict[str, Any]]:
    if not _cmd_llm_is_ollama():
        return []
    try:
        with urllib.request.urlopen(f"{_ollama_native_base()}/api/ps", timeout=10) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        return list(body.get("models") or [])
    except Exception:
        return []


def _cli_diagnose(phrase: str = "") -> None:
    """Explain command latency: rules vs LLM, loaded models, warm classify timing."""
    _load_cli_env()
    os.environ.setdefault("BOT_USE_AI", "1")
    os.environ.setdefault("BOT_CHAT_API_BASE", "http://127.0.0.1:11434/v1")
    os.environ.setdefault("BOT_CHAT_API_KEY", "ollama")

    cmd_model = _cmd_llm_model()
    chat_model = (os.getenv("BOT_CHAT_MODEL") or "gpt-4o-mini").strip()
    samples = [
        phrase.strip() if phrase.strip() else "",
        "who is on fpms duty today",
        "next week sre duty",
        "hello",
    ]
    samples = [s for s in samples if s]

    print("=== Command LLM diagnose ===")
    print(f"Command model:  {cmd_model!r}  (BOT_COMMANDAGENT_LLM_MODEL)")
    print(f"Chat model:     {chat_model!r}  (BOT_CHAT_MODEL — dutyai/offsetai/chat)")
    print(f"Ollama base:    {_cmd_llm_base()!r}")
    print(f"LLM enabled:    {_cmd_llm_enabled()}")
    print(f"Timeout:        {_CMD_LLM_TIMEOUT}s")
    print(f"System prompt:  ~{len(_CMD_LLM_SYSTEM + _build_llm_intent_catalog()) // 4} tokens (catalog slows cold starts)")
    print("")

    loaded = _ollama_ps_models()
    if loaded:
        print("Loaded Ollama models (ollama ps):")
        for m in loaded:
            name = m.get("name") or m.get("model") or "?"
            size_gb = (m.get("size_vram") or m.get("size") or 0) / (1024**3)
            expires = m.get("expires_at") or m.get("expires") or "—"
            print(f"  - {name}  ({size_gb:.1f} GB)  expires={expires}")
        names = {str(m.get("name") or m.get("model") or "") for m in loaded}
        if cmd_model not in names and not any(cmd_model in n for n in names):
            print(f"  WARN: {cmd_model!r} NOT loaded — first command LLM call pays reload cost (often 10-30s on CPU)")
        if chat_model != cmd_model and chat_model not in names and not any(chat_model in n for n in names):
            print(f"  WARN: {chat_model!r} NOT loaded — dutyai/offsetai may reload 35b before you see a reply")
    else:
        print("Loaded models:  (Ollama /api/ps unavailable — is Ollama running?)")
    print("")

    print("Rule coverage (pattern match = 0ms command LLM):")
    for s in samples:
        pat = _match_intent_by_patterns(s)
        tag = pat[0].tag if pat else None
        needs = "NO — instant rules" if pat else "YES — needs 2b LLM"
        print(f"  {s!r}")
        print(f"    pattern={tag}  command_llm={needs}")
    print("")

    print("Common slow paths (NOT the 2b command model):")
    print("  - dutyai used to call 35b BEFORE slash handler (fixed: regex-first + skip when mapped)")
    print("  - offsetleave + offsetai LLM use routing_llm_model() (default: BOT_COMMANDAGENT_LLM_MODEL / 0.5b)")
    print("  - Swapping 2b <-> 35b in RAM unloads the other model (keep both warm: bash deploy/warmup-ollama.sh)")
    print("")

    if not _cmd_llm_enabled():
        print("Skip live LLM timing — command LLM not enabled.")
        return
    test = phrase.strip() or "hello"
    print(f"Live classify timing for {test!r} (warm, cache cleared once):")
    with _CMD_LLM_CACHE_LOCK:
        _CMD_LLM_CACHE.pop(test, None)
    t0 = time.perf_counter()
    result = _classify_intent_llm(test)
    ms = (time.perf_counter() - t0) * 1000
    print(f"  {ms:.0f} ms  route={(result or {}).get('route')} tag={(result or {}).get('intent_tag')}")
    if ms > 3000:
        print("  SLOW (>3s): likely model cold start or CPU overload — run: ollama ps")
    elif ms > 800:
        print("  OK-ish for CPU; GPU usually <500ms when model stays loaded")
    else:
        print("  Fast — model was warm")


def _cli_bench(phrase: str, *, runs: int = 3, model_override: str = "") -> None:
    """Benchmark command LLM latency (bypasses pattern rules; hits model every run)."""
    _load_cli_env()
    os.environ.setdefault("BOT_USE_AI", "1")
    os.environ.setdefault("BOT_CHAT_API_BASE", "http://127.0.0.1:11434/v1")
    os.environ.setdefault("BOT_CHAT_API_KEY", "ollama")
    if (model_override or "").strip():
        os.environ["BOT_COMMANDAGENT_LLM_MODEL"] = model_override.strip()

    cmd_model = _cmd_llm_model()
    if not _cmd_llm_enabled():
        print("ERROR: LLM not enabled — set BOT_CHAT_API_KEY=ollama and start Ollama.", flush=True)
        return

    raw = (phrase or "").strip()
    if not raw:
        print("ERROR: empty phrase", flush=True)
        return

    print(f"Benchmark:   {raw!r}")
    print(f"Model:       {cmd_model!r}")
    print(f"Runs:        {runs} (cache cleared each run)")
    print("Note:        bypasses pattern rules — measures LLM classify only.")
    print("")

    times_ms: list[float] = []
    routes: list[str] = []
    for i in range(max(1, runs)):
        with _CMD_LLM_CACHE_LOCK:
            _CMD_LLM_CACHE.pop(raw, None)
        t0 = time.perf_counter()
        result = _classify_intent_llm(raw)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        times_ms.append(elapsed_ms)
        route_s = (result or {}).get("route") or "none"
        tag_s = (result or {}).get("intent_tag") or "—"
        routes.append(route_s)
        print(f"  run {i + 1}/{runs}: {elapsed_ms:6.0f} ms  route={route_s} tag={tag_s}")

    if not times_ms:
        return
    times_ms.sort()
    avg = sum(times_ms) / len(times_ms)
    mid = times_ms[len(times_ms) // 2]
    print("")
    print(f"Summary ({len(times_ms)} runs):")
    print(f"  min:     {times_ms[0]:.0f} ms")
    print(f"  median:  {mid:.0f} ms")
    print(f"  max:     {times_ms[-1]:.0f} ms")
    print(f"  avg:     {avg:.0f} ms")
    print(f"  routes:  {routes}")


_DEFAULT_BENCH_MODELS: tuple[str, ...] = (
    "qwen2.5:0.5b",
    "qwen3.5:2b",
    "qwen3.5:4b",
    "qwen3.5:9b",
    "qwen2.5:14b-instruct",
    "qwen3.6:35b-a3b",
)


def _ollama_installed_models() -> list[str]:
    try:
        with urllib.request.urlopen(f"{_ollama_native_base()}/api/tags", timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        names = [str(m.get("name") or "").strip() for m in (data.get("models") or [])]
        return [n for n in names if n]
    except Exception as exc:
        print(f"WARN: could not list Ollama models: {exc!r}", flush=True)
        return list(_DEFAULT_BENCH_MODELS)


def _preload_ollama_model(model: str) -> None:
    """Load one model into RAM before benchmarking (avoids 8s swap timeouts)."""
    if not _cmd_llm_is_ollama():
        return
    url = f"{_ollama_native_base()}/api/generate"
    payload = {
        "model": model,
        "prompt": "hi",
        "stream": False,
        "keep_alive": -1,
        "options": {"num_predict": 1},
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    bench_timeout = float(os.getenv("BOT_COMMANDAGENT_BENCH_TIMEOUT", "120"))
    with urllib.request.urlopen(req, timeout=bench_timeout) as resp:
        resp.read()


def _bench_classify_once(
    raw: str, model: str, system: str, *, timeout: Optional[float] = None
) -> tuple[float, Optional[dict[str, Any]]]:
    t0 = time.perf_counter()
    result = _classify_intent_llm_ollama_native(raw, system, model, timeout=timeout)
    return (time.perf_counter() - t0) * 1000, result


def _bench_stats(times_ms: list[float]) -> dict[str, float]:
    if not times_ms:
        return {}
    ordered = sorted(times_ms)
    return {
        "min": ordered[0],
        "median": ordered[len(ordered) // 2],
        "max": ordered[-1],
        "avg": sum(ordered) / len(ordered),
    }


def _cli_bench_all(
    phrase: str,
    *,
    runs: int = 3,
    models: Optional[list[str]] = None,
    from_ollama: bool = False,
    warmup: bool = True,
) -> None:
    """Compare command-classify latency across multiple Ollama models."""
    _load_cli_env()
    os.environ.setdefault("BOT_USE_AI", "1")
    os.environ.setdefault("BOT_CHAT_API_BASE", "http://127.0.0.1:11434/v1")
    os.environ.setdefault("BOT_CHAT_API_KEY", "ollama")

    if not _cmd_llm_is_ollama():
        print("ERROR: bench-all requires Ollama (BOT_CHAT_API_BASE with port 11434).", flush=True)
        return

    raw = (phrase or "").strip()
    if not raw:
        print("ERROR: empty phrase", flush=True)
        return

    if from_ollama:
        model_list = _ollama_installed_models()
    elif models:
        model_list = models
    else:
        model_list = list(_DEFAULT_BENCH_MODELS)

    if not model_list:
        print("ERROR: no models to benchmark.", flush=True)
        return

    system = _CMD_LLM_SYSTEM + _build_llm_intent_catalog()
    runs = max(1, runs)
    bench_timeout = float(os.getenv("BOT_COMMANDAGENT_BENCH_TIMEOUT", "120"))

    print(f"Benchmark-all: {raw!r}")
    print(f"API:           Ollama /api/chat (think=false, format=json)")
    print(f"Runs/model:    {runs}" + (" + 1 warmup (discarded)" if warmup else ""))
    print(f"Timeout/run:   {bench_timeout:.0f}s")
    print(f"Models:        {len(model_list)}")
    print("")

    rows: list[dict[str, Any]] = []
    for model in model_list:
        print(f"=== {model} ===", flush=True)
        try:
            t_pre = time.perf_counter()
            _preload_ollama_model(model)
            print(f"  preload: {(time.perf_counter() - t_pre) * 1000:.0f} ms", flush=True)
        except Exception as exc:
            print(f"  preload warn: {exc!r}", flush=True)
        if warmup:
            try:
                w_ms, _ = _bench_classify_once(raw, model, system, timeout=bench_timeout)
                print(f"  warmup: {w_ms:.0f} ms (discarded)", flush=True)
            except Exception as exc:
                print(f"  warmup failed: {exc!r}", flush=True)

        times_ms: list[float] = []
        route_s = "none"
        tag_s = "—"
        err = ""
        for i in range(runs):
            try:
                elapsed_ms, result = _bench_classify_once(
                    raw, model, system, timeout=bench_timeout
                )
                times_ms.append(elapsed_ms)
                route_s = (result or {}).get("route") or "none"
                tag_s = (result or {}).get("intent_tag") or "—"
                print(
                    f"  run {i + 1}/{runs}: {elapsed_ms:6.0f} ms  route={route_s} tag={tag_s}",
                    flush=True,
                )
            except Exception as exc:
                err = str(exc)
                print(f"  run {i + 1}/{runs}: ERROR {exc!r}", flush=True)

        stats = _bench_stats(times_ms)
        rows.append(
            {
                "model": model,
                "stats": stats,
                "route": route_s,
                "tag": tag_s,
                "error": err,
                "ok": bool(stats),
            }
        )
        print("", flush=True)

    ok_rows = [r for r in rows if r.get("ok")]
    ok_rows.sort(key=lambda r: float((r.get("stats") or {}).get("median") or 1e18))

    print("=" * 72)
    print(f"{'Model':<24} {'min':>7} {'median':>7} {'max':>7} {'avg':>7}  route/tag")
    print("-" * 72)
    for r in rows:
        st = r.get("stats") or {}
        if st:
            print(
                f"{r['model']:<24} {st['min']:7.0f} {st['median']:7.0f} "
                f"{st['max']:7.0f} {st['avg']:7.0f}  {r.get('route')}/{r.get('tag')}"
            )
        else:
            print(f"{r['model']:<24} {'—':>7} {'—':>7} {'—':>7} {'—':>7}  ERROR {r.get('error')}")
    print("-" * 72)
    if ok_rows:
        best = ok_rows[0]
        st = best["stats"]
        print(
            f"Fastest (median): {best['model']} — {st['median']:.0f} ms "
            f"(recommended for BOT_COMMANDAGENT_LLM_MODEL)"
        )
    print("")
    print("Note: large models (35b) are for BOT_CHAT_MODEL, not command routing.")
    print("      Unload other models (ollama ps) for fairest CPU comparison.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Duty Bot command agent (natural language → slash commands)")
    sub = parser.add_subparsers(dest="cmd")

    p_route = sub.add_parser("route", help="Show rule/LLM resolution path for a phrase")
    p_route.add_argument("phrase", type=str)

    p_diag = sub.add_parser("diagnose", help="Explain command LLM slowness (rules vs LLM, ollama ps)")
    p_diag.add_argument("phrase", nargs="?", default="", type=str)

    p_bench = sub.add_parser("bench", help="Benchmark command LLM latency (ms)")
    p_bench.add_argument("phrase", type=str)
    p_bench.add_argument("-n", "--runs", type=int, default=3, help="number of runs (default 3)")
    p_bench.add_argument(
        "-m",
        "--model",
        type=str,
        default="",
        help="override BOT_COMMANDAGENT_LLM_MODEL (e.g. qwen2.5:0.5b)",
    )

    p_bench_all = sub.add_parser("bench-all", help="Compare command LLM latency across Ollama models")
    p_bench_all.add_argument(
        "phrase",
        nargs="?",
        default="who is covering fpms shift tonight",
        help="test phrase (default: who is covering fpms shift tonight)",
    )
    p_bench_all.add_argument("-n", "--runs", type=int, default=3, help="timed runs per model (default 3)")
    p_bench_all.add_argument(
        "-m",
        "--models",
        type=str,
        default="",
        help="comma-separated model names (default: built-in list)",
    )
    p_bench_all.add_argument(
        "--from-ollama",
        action="store_true",
        help="benchmark every model returned by ollama list",
    )
    p_bench_all.add_argument(
        "--no-warmup",
        action="store_true",
        help="skip discarded warmup run per model",
    )

    sub.add_parser("patterns", help="Show pattern counts per intent")

    args = parser.parse_args()
    if args.cmd == "route":
        _cli_route(args.phrase)
    elif args.cmd == "diagnose":
        _cli_diagnose(str(getattr(args, "phrase", "") or ""))
    elif args.cmd == "bench":
        _cli_bench(
            args.phrase,
            runs=max(1, int(args.runs or 3)),
            model_override=str(getattr(args, "model", "") or ""),
        )
    elif args.cmd == "bench-all":
        models_arg = str(getattr(args, "models", "") or "").strip()
        model_list = [m.strip() for m in models_arg.split(",") if m.strip()] if models_arg else None
        _cli_bench_all(
            str(getattr(args, "phrase", "") or ""),
            runs=max(1, int(getattr(args, "runs", 3) or 3)),
            models=model_list,
            from_ollama=bool(getattr(args, "from_ollama", False)),
            warmup=not bool(getattr(args, "no_warmup", False)),
        )
    elif args.cmd == "patterns":
        intents = build_intent_catalog()
        total = 0
        for spec in intents:
            n = len(spec.patterns)
            total += n
            print(f"{spec.tag:24} {spec.command:16} {n:4} patterns  arg={spec.arg_kind}")
        print(f"Total patterns: {total}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
