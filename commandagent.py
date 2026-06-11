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
CONFIDENCE_THRESHOLD = float(os.getenv("BOT_AI_CONFIDENCE", "0.12"))
CONFIDENCE_MARGIN = float(os.getenv("BOT_AI_MARGIN", "0.03"))
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

    intents.append(
        IntentSpec(
            tag="cmd_checkcredit",
            command="/checkcredit",
            patterns=[
                "check credit for machine",
                "credit log check",
                "player credit on machine",
                "check credit NCH1422",
            ],
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
                "run jenkins update|trigger jenkins job|start jenkins build|deploy via jenkins",
            )
        )

    intents.append(
        IntentSpec(
            tag="cmd_reminder",
            command="/reminder",
            patterns=["remind me in 30 minutes", "set a reminder", "schedule reminder"],
            arg_kind="rest",
        )
    )

    return intents


def _looks_like_slash_command(text: str) -> bool:
    s = (text or "").lstrip()
    return s.startswith("/")


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
            return m.group(1).strip(" ?!.,")
        q = _SEARCH_PREFIX_RE.sub("", text).strip(" ?!.,")
        q = re.sub(r"(?i)\s+(?:in duty|on duty|duty info|phone|number)\s*$", "", q).strip()
        return q or None
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
