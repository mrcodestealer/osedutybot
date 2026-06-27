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
    python commandagent.py patterns   # print pattern counts per intent

LLM env (optional, reuses ``chatagent`` API config):
- ``BOT_COMMANDAGENT_LLM`` — set ``0`` to skip LLM even when an API key exists.
- ``BOT_COMMANDAGENT_LLM_MODEL`` — override model (default: ``BOT_CHAT_MODEL``).
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

_MACHINE_PREFIXES = ("nch", "nwr", "wf", "winford", "tbp", "cp", "dhs", "mdr")

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


def build_intent_catalog(*, jenkins_available: bool = True) -> list[IntentSpec]:
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
            "cmd_cancelp1",
            "/cancelp1",
            "cancel p1 reminder|stop p1 escalation|cancel p1 alert",
        )
    )
    intents.append(
        _simple_intent(
            "cmd_fpmsp0",
            "/fpmsp0",
            "fpms p0 contacts|fpms p0 phone|fpms emergency contact",
        )
    )
    intents.append(
        _simple_intent(
            "cmd_otpp0",
            "/otpp0",
            "otp p0 guide|otp p0 contacts|otp emergency",
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

    if jenkins_available:
        intents.append(
            _simple_intent(
                "cmd_update",
                "/update",
                "run jenkins update|trigger jenkins job|start jenkins build|deploy via jenkins|"
                "i want update jenkins|update jenkins pms|update jenkins fpms|"
                "want to update pms|deploy pms uat|jenkins pms update",
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
    r"(?i)(?:^|\s)(?:restart|reboot)\s+(?:all\s+)?services(?:\s|$|[.!?])"
    r"|(?:^|\s)restart\s+(?:the\s+)?(?:webapp|web\s+app)(?:\s+and\s+(?:larkbot|bot|duty\s+bot))?(?:\s|$|[.!?])"
    r"|重启(?:所有|全部)?服务|重啟(?:所有|全部)?服務|重新启动(?:所有|全部)?服务|重新啟動(?:所有|全部)?服務"
    r"|重启\s*(?:webapp|web\s*app|网页|網頁|机器人|機器人|bot|duty\s*bot)|重啟\s*(?:webapp|web\s*app|网页|網頁|机器人|機器人|bot|duty\s*bot)"
)


def detect_restart_services_command(text: str) -> Optional[str]:
    """Map natural language → ``/restartservices`` (webapp :8765 + larkbot systemd)."""
    raw = (text or "").strip()
    if not raw or _looks_like_slash_command(raw):
        return None
    if _RESTART_SERVICES_RE.search(raw):
        return "/restartservices"
    return None


# "check this issue" / "identify this issue" / "help us check this" / "what is this
# issue" → /identifyissue. The full report (incl. account ids) is read separately by
# main.py from the multiline body, so we only need to detect the *intent* here.
_IDENTIFY_ISSUE_RE = re.compile(
    r"(?i)("
    r"(?:identify|check|explain|analy[sz]e|classify|diagnose|what\s+is|what's|whats|"
    r"tell\s+me\s+about|help\s+(?:us|me)\s+(?:to\s+)?(?:check|identify|analy[sz]e))\s+"
    r"(?:this|the|that|out|us\s+with)?\s*(?:issue|problem|incident|error|case|bug|report)"
    r"|help\s+(?:us|me)\s+check\s+this(?:\s+(?:one|out))?"
    r"|check\s+this\s+(?:one|out|issue|problem|incident)"
    r"|identify\s+(?:this\s+)?(?:issue|problem|incident)"
    r"|(?:这是?|帮.{0,6}看看?|分析|判断).{0,6}(?:什么)?(?:问题|事故|issue)"
    r"|是什么问题"
    r")"
)


def detect_identify_issue_command(text: str) -> Optional[str]:
    """Map natural language → ``/identifyissue`` (AI explains/classifies the issue)."""
    raw = (text or "").strip()
    if not raw or _looks_like_slash_command(raw):
        return None
    # Avoid colliding with the maintenance / credit / jenkins structured flows.
    if detect_prod_batch_command(raw) or detect_stuck_credit_command(raw) or detect_checkmachinelog_command(raw) or detect_checkcredit_command(raw):
        return None
    if _IDENTIFY_ISSUE_RE.search(raw):
        return "/identifyissue"
    # Auto-detect a player issue/incident report (e.g. "please help check this
    # player account 12345, unable to withdraw ...") even without the literal
    # phrase "identify issue" — let the AI explain it.
    try:
        import identifyissue as _ii

        if _ii.looks_like_issue_report(raw):
            return "/identifyissue"
    except Exception:
        pass
    return None


def detect_checkperson_command(text: str) -> Optional[str]:
    """Map natural language → ``/checkperson`` (AI decides who should check an issue)."""
    raw = (text or "").strip()
    if not raw or _looks_like_slash_command(raw):
        return None
    # Don't collide with the maintenance / credit / jenkins structured flows.
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
    r"leave|wfh|holiday|jenkins|maintenance|machine|checkcredit|machineerror|"
    r"update|deploy|reminder|offset|cctv|credit"
    r")\b|/"
)
_MACHINE_ID_RE = re.compile(
    r"(?i)\b(?:(?:nch|nwr|wf|winford|tbp|cp|dhs|mdr)\s*)?(\d{3,}|[A-Z]{2,4}\d+)\b"
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


def extract_argument(arg_kind: Optional[str], user_text: str, spec: IntentSpec) -> Optional[str]:
    if not arg_kind:
        return None
    text = (user_text or "").strip()
    if arg_kind == "search_name":
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
    r"machine|asset|egm|nch|nwr|winford|tbr|tbp|mdr|dhs|"
    r"maintenance|maint|test|credit|cctv|jenkins|deploy|update|"
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

    norm = _normalize_for_match(raw)
    best: tuple[IntentSpec, float] | None = None
    for plen, _pnorm, spec in _get_pattern_index():
        for pat in spec.patterns:
            if _pattern_matches(norm, pat):
                conf = 1.0 if _normalize_for_match(pat) == norm else min(0.98, 0.85 + plen / 200.0)
                if best is None or conf > best[1]:
                    best = (spec, conf)
                break
        if best and best[1] >= 0.99:
            break

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

_CMD_LLM_TIMEOUT = float(os.getenv("BOT_COMMANDAGENT_LLM_TIMEOUT", "15"))
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
    explicit = (os.getenv("BOT_COMMANDAGENT_LLM_MODEL") or "").strip()
    if explicit:
        return explicit
    try:
        import chatagent as ca

        return ca._llm_model_for_request(images=False)
    except Exception:
        return (os.getenv("BOT_CHAT_MODEL") or "gpt-4o-mini").strip()


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
    "Decide: is this a bot COMMAND (work request) or casual CHAT (greeting, thanks, small talk, "
    "general conversation, math, jokes)?\n"
    "Reply with ONLY one JSON object:\n"
    '{"route":"command"|"chat","intent_tag":string|null,"confidence":0.0-1.0}\n'
    "- route=chat → intent_tag must be null, confidence = how sure it is casual chat.\n"
    "- route=command → pick the best intent_tag from the catalog below; confidence = how sure.\n"
    "- If unsure, prefer route=chat with lower confidence rather than guessing a command.\n"
    "Examples:\n"
    '"hi" → {"route":"chat","intent_tag":null,"confidence":0.99}\n'
    '"who is on fpms duty today" → {"route":"command","intent_tag":"cmd_fpms","confidence":0.95}\n'
    '"check credit NCH1422" → {"route":"command","intent_tag":"cmd_checkcredit","confidence":0.95}\n'
    '"how are you doing" → {"route":"chat","intent_tag":null,"confidence":0.98}\n'
    '"nwr set maintenance NWR2113" → {"route":"command","intent_tag":"cmd_pb_setmaintenance","confidence":0.9}\n'
    "Intent catalog:\n"
)


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
    payload = {
        "model": _cmd_llm_model(),
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": raw},
        ],
        "max_tokens": 120,
        "temperature": 0,
    }
    req = urllib.request.Request(
        f"{_cmd_llm_base()}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    result: dict[str, Any] | None = None
    try:
        with urllib.request.urlopen(req, timeout=_CMD_LLM_TIMEOUT) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        content = ((body.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        obj = _parse_llm_json(content)
        if not obj:
            result = None
        else:
            route = str(obj.get("route") or "").strip().lower()
            tag = obj.get("intent_tag")
            tag_s = str(tag).strip() if tag else ""
            try:
                conf = float(obj.get("confidence") or 0.0)
            except (TypeError, ValueError):
                conf = 0.0
            conf = max(0.0, min(1.0, conf))
            if route not in ("command", "chat"):
                result = None
            elif route == "chat":
                result = {
                    "route": "chat",
                    "intent_tag": NONE_TAG,
                    "confidence": conf,
                    "source": "llm",
                }
            else:
                spec = _intents_by_tag().get(tag_s)
                if not spec or tag_s == NONE_TAG:
                    result = None
                else:
                    result = {
                        "route": "command",
                        "intent_tag": tag_s,
                        "confidence": conf,
                        "source": "llm",
                        "spec": spec,
                    }
    except urllib.error.HTTPError as exc:
        if not _cmd_llm_failed_logged:
            try:
                detail = exc.read().decode("utf-8", errors="replace")[:300]
            except Exception:
                detail = exc.reason
            print(f"⚠️ commandagent LLM HTTP {exc.code}: {detail}", flush=True)
            _cmd_llm_failed_logged = True
    except Exception as exc:
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
    if m2 and re.search(r"(?i)\b(lookup|show|check|machine|asset|info)\b", text):
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


def _run_deterministic_detectors(text: str) -> dict[str, Any] | None:
    """High-precision structured command detectors. Returns signal dict or None."""
    raw = (text or "").strip()
    if not raw:
        return None
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
    rs = detect_restart_services_command(raw)
    if rs:
        return dict(tag="cmd_restart_services", confidence=1.0, margin=1.0, command=rs, deterministic=True, source="deterministic", route="command")
    cp = detect_checkperson_command(raw)
    if cp:
        return dict(tag="cmd_checkperson", confidence=1.0, margin=1.0, command=cp, deterministic=True, source="deterministic", route="command")
    ii = detect_identify_issue_command(raw)
    if ii:
        return dict(tag="cmd_identifyissue", confidence=1.0, margin=1.0, command=ii, deterministic=True, source="deterministic", route="command")
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
    try:
        import jenkinsupdate as _jenkins_gate

        if _jenkins_gate.looks_like_natural_jenkins_update(raw):
            return None
    except Exception:
        pass

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


def command_signal(text: str) -> dict[str, Any]:
    """Diagnostic signal for the router (``chathandleagent``).

    Returns ``tag``, ``confidence``, ``margin``, ``command``, ``deterministic``,
    ``source`` (deterministic|pattern|llm), and ``route`` (command|chat).
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

    if not is_enabled():
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

    det = _run_deterministic_detectors(raw)
    if det and det.get("command"):
        src = det.get("source") or "deterministic"
        cmd = det["command"]
        print(f"[commandagent] {src} map: {raw[:80]!r} -> {str(cmd).splitlines()[0]!r}", flush=True)
        return cmd

    try:
        import jenkinsupdate as _jenkins_gate

        if _jenkins_gate.looks_like_natural_jenkins_update(raw):
            print(f"[commandagent] Skip NL map — Jenkins update request: {raw[:120]!r}", flush=True)
            return None
    except Exception:
        if re.search(
            r"(?i)(?:update|deploy|trigger|run).*(?:jenkins|\bfpms\b|\bpms\b|\bbi\b)"
            r"|\bbranch\s*:.*\bservices?\s*:|\bservices?\s*:.*\bbranch\s*:",
            raw,
        ):
            return None

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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli_route(phrase: str) -> None:
    """Show full resolution path: deterministic → pattern → direct LLM."""
    os.environ.setdefault("BOT_USE_AI", "1")
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
    print(f"Translate:   {translate_if_enabled(phrase)!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Duty Bot command agent (natural language → slash commands)")
    sub = parser.add_subparsers(dest="cmd")

    p_route = sub.add_parser("route", help="Show rule/LLM resolution path for a phrase")
    p_route.add_argument("phrase", type=str)

    sub.add_parser("patterns", help="Show pattern counts per intent")

    args = parser.parse_args()
    if args.cmd == "route":
        _cli_route(args.phrase)
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
