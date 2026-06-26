"""
Natural-language → slash-command router for the Duty Bot (DistilBERT intent classifier).

**With AI / without AI**
- Default (without AI): ``BOT_USE_AI`` unset or ``0`` — bot behaves exactly as before (hardcoded ``/`` commands only).
- With AI: set ``BOT_USE_AI=1`` in ``.env`` — English messages are mapped to slash commands before normal handlers run.
- If the model is missing, confidence is low, or anything throws, the layer returns ``None`` and the bot continues unchanged.

**Train / test**
    python commandagent.py train [--epochs 8] [--output commandagent_pt]
    python commandagent.py test "who is on fpms duty today"
    python commandagent.py eval
    python commandagent.py patterns   # print training pattern counts

Model folder default: ``commandagent_pt/`` (legacy ``command_intent_pt/`` still auto-detected).
Env override: ``BOT_COMMANDAGENT_MODEL_DIR`` or ``BOT_AI_MODEL_DIR``.
"""

from __future__ import annotations

import argparse
import os
import pickle
import re
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

_CHBOX_DIR = Path(__file__).resolve().parent
DEFAULT_MODEL_DIR = _CHBOX_DIR / "commandagent_pt"
LEGACY_MODEL_DIR = _CHBOX_DIR / "command_intent_pt"
# Raised from the old 0.12/0.03: with a trained ``cmd_none`` (out-of-scope) class the
# model now *learns* to abstain, so we can demand real confidence before firing a command.
CONFIDENCE_THRESHOLD = float(os.getenv("BOT_AI_CONFIDENCE", "0.35"))
CONFIDENCE_MARGIN = float(os.getenv("BOT_AI_MARGIN", "0.08"))
# Tag the model uses to say "this is not a command" (casual chat / unknown).
NONE_TAG = "cmd_none"
MAX_SEQ_LEN = 64

# Lazy imports for inference (avoid heavy load when AI disabled)
_torch = None
_DistilBertTokenizer = None
_DistilBertForSequenceClassification = None
_classifier_singleton: Optional["CommandClassifier"] = None
_classifier_failed: bool = False


def startup_status() -> None:
    """Log AI mode at bot boot (check ``journalctl -u larkbot`` after restart)."""
    enabled = is_enabled()
    path = model_dir()
    has_model = (path / "config.json").is_file()
    print(
        f"[commandagent] BOT_USE_AI={os.getenv('BOT_USE_AI')!r} enabled={enabled} "
        f"model_dir={path} model_exists={has_model}",
        flush=True,
    )
    if not enabled:
        print("[commandagent] Natural language OFF — only `/` commands work.", flush=True)
        return
    if not has_model:
        print(f"[commandagent] ⚠️ Model missing at {path} — run: python commandagent.py train", flush=True)
        return
    clf = _get_classifier()
    if clf is None:
        print("[commandagent] ⚠️ Model present but failed to load — check torch/transformers in service Python.", flush=True)
    else:
        print(f"[commandagent] ✅ Ready — natural English → slash commands (threshold={CONFIDENCE_THRESHOLD}).", flush=True)


def _lazy_torch():
    global _torch, _DistilBertTokenizer, _DistilBertForSequenceClassification
    if _torch is None:
        import torch
        from transformers import DistilBertForSequenceClassification, DistilBertTokenizer

        _torch = torch
        _DistilBertTokenizer = DistilBertTokenizer
        _DistilBertForSequenceClassification = DistilBertForSequenceClassification
    return _torch, _DistilBertTokenizer, _DistilBertForSequenceClassification


def is_enabled() -> bool:
    """True when ``BOT_USE_AI=1`` (or ``true`` / ``yes``)."""
    return (os.getenv("BOT_USE_AI") or "").strip().lower() in ("1", "true", "yes", "on")


def model_dir() -> Path:
    explicit = (
        os.getenv("BOT_COMMANDAGENT_MODEL_DIR")
        or os.getenv("BOT_AI_MODEL_DIR")
        or ""
    ).strip()
    if explicit:
        return Path(explicit)
    if (DEFAULT_MODEL_DIR / "config.json").is_file():
        return DEFAULT_MODEL_DIR
    if (LEGACY_MODEL_DIR / "config.json").is_file():
        print(
            f"[commandagent] Using legacy model dir {LEGACY_MODEL_DIR} — "
            f"run: mv command_intent_pt commandagent_pt",
            flush=True,
        )
        return LEGACY_MODEL_DIR
    return DEFAULT_MODEL_DIR


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

    intents.append(
        IntentSpec(
            tag="cmd_showoffset",
            command="showoffset",
            patterns=["show offset calendar", "offset schedule", "monthly offset", "showoffset"],
        )
    )

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
_PB_MAINT_RE = re.compile(r"(?i)\b(maintenance|maint|mtn)\b")
_PB_TEST_RE = re.compile(r"(?i)\btest\b")
_PB_UNSET_RE = re.compile(
    r"(?i)\b(unset|disable|deactivate|remove|clear|cancel|lift|unmark|"
    r"turn\s+off|switch\s+off|take\s+off|take\s+out)\b"
)
_PB_SET_RE = re.compile(
    r"(?i)\b(set|enable|activate|put|apply|mark|flag|turn\s+on|switch\s+on)\b"
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
)
_CC_CREDIT_RE = re.compile(r"(?i)\bcredit\b")
# "machine error" / "machineerror" / "error only" -> /machineerror (latest 2 error players)
_CC_ERROR_ONLY_RE = re.compile(r"(?i)\bmachine\s*error\b|\bmachineerror\b|\berror\s*only\b")
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
    if detect_prod_batch_command(raw) or detect_checkmachinelog_command(raw) or detect_checkcredit_command(raw):
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
# Classifier
# ---------------------------------------------------------------------------


def _sanitize_hf_env() -> None:
    """Blank HF cache env vars break local model load with stat(None)."""
    for key in ("HF_HOME", "HUGGINGFACE_HUB_CACHE", "TRANSFORMERS_CACHE", "HF_HUB_CACHE"):
        val = os.environ.get(key)
        if val is not None and str(val).strip().lower() in ("", "none", "null"):
            os.environ.pop(key, None)


def _save_model_compat(model, tokenizer, output_dir: Path) -> None:
    """Save weights for both newer (safetensors) and older (pytorch_model.bin) transformers."""
    out = str(output_dir)
    try:
        model.save_pretrained(out, safe_serialization=False)
    except TypeError:
        model.save_pretrained(out)
    tokenizer.save_pretrained(out)


def _load_pretrained_compat(model_path: Path):
    """Load local DistilBERT classifier (fast tokenizer.json + safetensors or pytorch bin)."""
    torch, _, _ = _lazy_torch()
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    _sanitize_hf_env()
    path = str(model_path)
    local = {"local_files_only": True}
    # transformers 4.x slow DistilBertTokenizer requires vocab.txt; our models only have tokenizer.json.
    tokenizer = AutoTokenizer.from_pretrained(path, use_fast=True, **local)
    bin_file = model_path / "pytorch_model.bin"
    safe_file = model_path / "model.safetensors"
    if bin_file.is_file():
        model = AutoModelForSequenceClassification.from_pretrained(
            path, use_safetensors=False, **local
        )
    elif safe_file.is_file():
        try:
            model = AutoModelForSequenceClassification.from_pretrained(path, **local)
        except Exception:
            model = AutoModelForSequenceClassification.from_pretrained(
                path, use_safetensors=True, **local
            )
    else:
        raise FileNotFoundError(
            f"No model weights in {model_path} (need model.safetensors or pytorch_model.bin)"
        )
    return torch, tokenizer, model


class CommandClassifier:
    def __init__(self, model_path: Path):
        torch, self.tokenizer, self.model = _load_pretrained_compat(model_path)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model_path = model_path
        self.model = self.model.to(self.device)
        self.model.eval()
        meta_path = model_path / "metadata.pkl"
        if not meta_path.is_file():
            raise FileNotFoundError(f"Missing metadata.pkl in {model_path}")
        with meta_path.open("rb") as f:
            meta = pickle.load(f)
        self.tag_to_id: dict[str, int] = meta["tag_to_id"]
        self.id_to_tag: dict[int, str] = meta["id_to_tag"]
        raw_intents = meta.get("intents", [])
        self.intents_by_tag: dict[str, IntentSpec] = {}
        for item in raw_intents:
            if isinstance(item, IntentSpec):
                spec = item
            else:
                spec = IntentSpec(
                    tag=item["tag"],
                    command=item["command"],
                    patterns=item.get("patterns", []),
                    arg_kind=item.get("arg_kind"),
                )
            self.intents_by_tag[spec.tag] = spec

    def predict(self, text: str) -> tuple[str, float, float]:
        """Return ``(tag, top_confidence, margin_over_second)``."""
        torch, _, _ = _lazy_torch()
        inputs = self.tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            padding="max_length",
            max_length=MAX_SEQ_LEN,
        )
        inputs = {k: v.to(self.device) for k, v in inputs.items()}
        with torch.no_grad():
            logits = self.model(**inputs).logits
        probs = torch.softmax(logits, dim=-1).cpu()[0]
        k = min(2, int(probs.numel()))
        topk = torch.topk(probs, k=k)
        idx = int(topk.indices[0].item())
        second = float(topk.values[1].item()) if k > 1 else 0.0
        margin = float(topk.values[0].item() - second)
        return self.id_to_tag[idx], float(topk.values[0].item()), margin

    def resolve(self, text: str, *, threshold: float = CONFIDENCE_THRESHOLD) -> Optional[str]:
        tag, conf, margin = self.predict(text)
        if conf < threshold or margin < CONFIDENCE_MARGIN:
            return None
        spec = self.intents_by_tag.get(tag)
        if not spec:
            return None
        cmd = build_slash_command(spec, text)
        return cmd


def _get_classifier() -> Optional[CommandClassifier]:
    global _classifier_singleton, _classifier_failed
    if _classifier_singleton is not None:
        return _classifier_singleton
    if _classifier_failed:
        return None
    path = model_dir()
    if not (path / "config.json").is_file():
        print(f"⚠️ AI model not found at {path} — natural-language routing disabled", flush=True)
        _classifier_failed = True
        return None
    try:
        _classifier_singleton = CommandClassifier(path)
        print(f"✅ AI command classifier loaded from {path}", flush=True)
        return _classifier_singleton
    except Exception as exc:
        print(f"⚠️ AI classifier load failed: {exc!r}", flush=True)
        traceback.print_exc()
        _classifier_failed = True
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
    if detect_prod_batch_command(raw) or detect_checkmachinelog_command(raw) or detect_checkcredit_command(raw):
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

    Returns ``{"tag", "confidence", "margin", "command", "deterministic"}``.
    ``command`` is the mapped slash command (or ``None``). ``deterministic`` is
    True when a hard rule (prod-batch) produced it. Never raises.
    """
    out: dict[str, Any] = {
        "tag": None,
        "confidence": 0.0,
        "margin": 0.0,
        "command": None,
        "deterministic": False,
    }
    raw = (text or "").strip()
    if not raw:
        return out
    pb = detect_prod_batch_command(raw)
    if pb:
        out.update(tag="cmd_pb", confidence=1.0, margin=1.0, command=pb, deterministic=True)
        return out
    cml = detect_checkmachinelog_command(raw)
    if cml:
        out.update(tag="cmd_checkmachinelog", confidence=1.0, margin=1.0, command=cml, deterministic=True)
        return out
    cc = detect_checkcredit_command(raw)
    if cc:
        tag = "cmd_machineerror" if cc.startswith("/machineerror") else "cmd_checkcredit"
        out.update(tag=tag, confidence=1.0, margin=1.0, command=cc, deterministic=True)
        return out
    sr = detect_show_reminder_command(raw)
    if sr:
        out.update(tag="cmd_deletereminder", confidence=1.0, margin=1.0, command=sr, deterministic=True)
        return out
    rs = detect_restart_services_command(raw)
    if rs:
        out.update(tag="cmd_restart_services", confidence=1.0, margin=1.0, command=rs, deterministic=True)
        return out
    ii = detect_identify_issue_command(raw)
    if ii:
        out.update(tag="cmd_identifyissue", confidence=1.0, margin=1.0, command=ii, deterministic=True)
        return out
    clf = _get_classifier()
    if clf is None:
        return out
    try:
        tag, conf, margin = clf.predict(raw)
        out["tag"] = tag
        out["confidence"] = conf
        out["margin"] = margin
        if tag != NONE_TAG and conf >= CONFIDENCE_THRESHOLD and margin >= CONFIDENCE_MARGIN:
            spec = clf.intents_by_tag.get(tag)
            if spec:
                out["command"] = build_slash_command(spec, raw)
    except Exception as exc:
        print(f"⚠️ command_signal error: {exc!r}", flush=True)
    return out


def translate_if_enabled(text: str) -> Optional[str]:
    """
    Map natural English to a slash command when AI is enabled.
    Returns ``None`` when disabled, input is already ``/…``, model missing, low confidence, or on error.
    """
    if not is_enabled():
        return None
    raw = (text or "").strip()
    if not raw or _looks_like_slash_command(raw):
        return None
    # Deterministic prod-batch maintenance mapping runs BEFORE the fuzzy model —
    # "i want nwr set maintenance ..." -> "/nwrsetmaintenance ...".
    pb = detect_prod_batch_command(raw)
    if pb:
        print(f"[commandagent] Prod-batch map: {raw[:80]!r} → {pb.splitlines()[0]!r}", flush=True)
        return pb
    cml = detect_checkmachinelog_command(raw)
    if cml:
        print(f"[commandagent] Check-machine-log map: {raw[:80]!r} → {cml!r}", flush=True)
        return cml
    # Deterministic credit-check / machine-error mapping (also runs BEFORE the
    cc = detect_checkcredit_command(raw)
    if cc:
        print(f"[commandagent] Check-credit map: {raw[:80]!r} → {cc!r}", flush=True)
        return cc
    sr = detect_show_reminder_command(raw)
    if sr:
        print(f"[commandagent] Show-reminder map: {raw[:80]!r} → {sr!r}", flush=True)
        return sr
    rs = detect_restart_services_command(raw)
    if rs:
        print(f"[commandagent] Restart-services map: {raw[:80]!r} → {rs!r}", flush=True)
        return rs
    ii = detect_identify_issue_command(raw)
    if ii:
        print(f"[commandagent] Identify-issue map: {raw[:80]!r} → {ii!r}", flush=True)
        return ii
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
    clf = _get_classifier()
    if clf is None:
        print(f"⚠️ AI enabled but classifier unavailable for {raw!r}", flush=True)
        return None
    try:
        return clf.resolve(raw)
    except Exception as exc:
        print(f"⚠️ AI resolve error: {exc!r}", flush=True)
        return None


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------


def prepare_training_examples(intents: list[IntentSpec]) -> tuple[list[str], list[int], dict[str, int]]:
    texts: list[str] = []
    labels: list[int] = []
    tag_to_id: dict[str, int] = {}
    for idx, spec in enumerate(intents):
        tag_to_id[spec.tag] = idx
        seen: set[str] = set()
        for pat in spec.patterns:
            for variant in (pat, pat.lower(), pat.upper() if pat.islower() else pat):
                if variant not in seen:
                    seen.add(variant)
                    texts.append(variant)
                    labels.append(idx)
    return texts, labels, tag_to_id


def train_model(
    output_dir: Path,
    *,
    epochs: int = 8,
    jenkins_available: bool = True,
    batch_size: int = 32,
    lr: float = 2e-5,
) -> dict[str, Any]:
    import random

    from torch.utils.data import DataLoader, TensorDataset
    from transformers import DistilBertForSequenceClassification, DistilBertTokenizer

    torch, _, _ = _lazy_torch()

    intents = build_intent_catalog(jenkins_available=jenkins_available)
    texts, labels, tag_to_id = prepare_training_examples(intents)
    print(f"Training samples: {len(texts)} intents: {len(intents)}")

    pairs = list(zip(texts, labels))
    random.seed(42)
    random.shuffle(pairs)
    split = int(len(pairs) * 0.85)
    train_pairs, val_pairs = pairs[:split], pairs[split:]

    tok = DistilBertTokenizer.from_pretrained("distilbert-base-uncased")
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def _encode(batch_texts: list[str], batch_labels: list[int]):
        enc = tok(
            batch_texts,
            truncation=True,
            padding="max_length",
            max_length=MAX_SEQ_LEN,
            return_tensors="pt",
        )
        return enc["input_ids"], enc["attention_mask"], torch.tensor(batch_labels, dtype=torch.long)

    def _make_loader(items: list[tuple[str, int]], shuffle: bool) -> DataLoader:
        ids_list, mask_list, label_list = [], [], []
        for t, lb in items:
            i, m, y = _encode([t], [lb])
            ids_list.append(i.squeeze(0))
            mask_list.append(m.squeeze(0))
            label_list.append(y.squeeze(0))
        ds = TensorDataset(
            torch.stack(ids_list),
            torch.stack(mask_list),
            torch.stack(label_list),
        )
        return DataLoader(ds, batch_size=batch_size, shuffle=shuffle)

    train_loader = _make_loader(train_pairs, shuffle=True)
    val_loader = _make_loader(val_pairs, shuffle=False)

    model = DistilBertForSequenceClassification.from_pretrained(
        "distilbert-base-uncased", num_labels=len(tag_to_id)
    ).to(device)
    optim = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=0.01)

    best_acc = 0.0
    output_dir.mkdir(parents=True, exist_ok=True)

    for epoch in range(1, epochs + 1):
        model.train()
        train_loss = 0.0
        for input_ids, attention_mask, y in train_loader:
            input_ids = input_ids.to(device)
            attention_mask = attention_mask.to(device)
            y = y.to(device)
            optim.zero_grad()
            out = model(input_ids=input_ids, attention_mask=attention_mask, labels=y)
            out.loss.backward()
            optim.step()
            train_loss += float(out.loss.item())

        model.eval()
        correct = total = 0
        with torch.no_grad():
            for input_ids, attention_mask, y in val_loader:
                input_ids = input_ids.to(device)
                attention_mask = attention_mask.to(device)
                y = y.to(device)
                logits = model(input_ids=input_ids, attention_mask=attention_mask).logits
                pred = logits.argmax(dim=-1)
                correct += int((pred == y).sum().item())
                total += int(y.size(0))
        val_acc = correct / max(total, 1)
        print(
            f"Epoch {epoch}/{epochs}  loss={train_loss / max(len(train_loader), 1):.4f}  "
            f"val_acc={val_acc:.1%} ({correct}/{total})"
        )
        if val_acc >= best_acc:
            best_acc = val_acc
            _save_model_compat(model, tok, output_dir)

    id_to_tag = {v: k for k, v in tag_to_id.items()}
    meta = {
        "tag_to_id": tag_to_id,
        "id_to_tag": id_to_tag,
        "intents": [
            {
                "tag": i.tag,
                "command": i.command,
                "patterns": i.patterns,
                "arg_kind": i.arg_kind,
            }
            for i in intents
        ],
    }
    with (output_dir / "metadata.pkl").open("wb") as f:
        pickle.dump(meta, f)

    print(f"✅ Model saved to {output_dir} best_val_acc={best_acc:.1%}")
    return {"val_accuracy": best_acc, "samples": len(texts), "intents": len(intents)}


def evaluate_model(model_path: Path) -> None:
    intents = build_intent_catalog()
    texts, labels, _ = prepare_training_examples(intents)
    clf = CommandClassifier(model_path)
    correct = 0
    for text, label in zip(texts, labels):
        tag, conf, _margin = clf.predict(text)
        pred = clf.tag_to_id.get(tag, -1)
        if pred == label:
            correct += 1
    acc = correct / max(len(texts), 1)
    print(f"Train-set accuracy (sanity): {acc:.1%} ({correct}/{len(texts)})")


def _cli_test(phrase: str, model_path: Path) -> None:
    if not (model_path / "config.json").is_file():
        print(f"Model not found at {model_path}. Run: python commandagent.py train")
        sys.exit(1)
    clf = CommandClassifier(model_path)
    tag, conf, margin = clf.predict(phrase)
    spec = clf.intents_by_tag.get(tag)
    cmd = build_slash_command(spec, phrase) if spec else None
    resolved = clf.resolve(phrase)
    print(f"Input:      {phrase!r}")
    print(f"Intent:     {tag} ({conf:.3f}, margin={margin:.3f})")
    print(f"Command:    {cmd!r}")
    print(f"Resolved:   {resolved!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Duty Bot command agent (natural language → slash commands)")
    sub = parser.add_subparsers(dest="cmd")

    p_train = sub.add_parser("train", help="Train DistilBERT intent classifier")
    p_train.add_argument("--epochs", type=int, default=8)
    p_train.add_argument("--output", type=str, default=str(DEFAULT_MODEL_DIR))
    p_train.add_argument("--no-jenkins", action="store_true")

    p_test = sub.add_parser("test", help="Test a phrase")
    p_test.add_argument("phrase", type=str)
    p_test.add_argument("--model", type=str, default=str(DEFAULT_MODEL_DIR))

    sub.add_parser("eval", help="Evaluate model on training patterns")
    sub.add_parser("patterns", help="Show pattern counts per intent")

    args = parser.parse_args()
    if args.cmd == "train":
        train_model(
            Path(args.output),
            epochs=args.epochs,
            jenkins_available=not args.no_jenkins,
        )
    elif args.cmd == "test":
        _cli_test(args.phrase, Path(args.model))
    elif args.cmd == "eval":
        evaluate_model(model_dir())
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
