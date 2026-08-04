"""Silent watcher for the Newport activities group.

Reads messages from ONE source group, asks qwen3.6:35b-a3b whether the message is
new **stress-test** or **machine pull-out** information, and — only when it is —
posts a card to a separate target group tagging the two CC people.

Hard rule: **nothing is ever sent to the source group.** Every send in this module
goes to :func:`target_chat_id`, and :func:`is_silent_source_chat` lets the message
handler bail out before it can react, reply or forward anything there.

qwen3.6:35b-a3b is a *thinking* model — same handling as changePrefix.py /
checkerror.py: ``reasoning_effort="none"`` + ``think=False`` on Ollama, otherwise
it spends every token reasoning and returns empty content.
"""

from __future__ import annotations

import json
import os
import re
import threading
from typing import Any, Callable, Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Group we LISTEN to. Never written to.
SOURCE_CHAT_ID_DEFAULT = "oc_1dd135e6e3dedab6fd356406d07e683b"
# Group the detected notice is posted to.
TARGET_CHAT_ID_DEFAULT = "oc_ad9b5bdbb2826ba2ee9730920ef25432"
# People @-tagged on the posted card.
CC_OPEN_IDS_DEFAULT = (
    "ou_dadb58c6d52bd92f92fc3b74301137d7",
    "ou_9927c4dfc0e063a22473a068f0579aed",
)


def source_chat_id() -> str:
    return (os.getenv("NEWPORT_SOURCE_CHAT_ID") or SOURCE_CHAT_ID_DEFAULT).strip()


def target_chat_id() -> str:
    return (os.getenv("NEWPORT_TARGET_CHAT_ID") or TARGET_CHAT_ID_DEFAULT).strip()


def cc_open_ids() -> list[str]:
    raw = (os.getenv("NEWPORT_CC_OPEN_IDS") or "").strip()
    if raw:
        return [x.strip() for x in re.split(r"[,\s]+", raw) if x.strip()]
    return list(CC_OPEN_IDS_DEFAULT)


def is_enabled() -> bool:
    return (os.getenv("NEWPORT_WATCH") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def is_silent_source_chat(chat_id: str | None) -> bool:
    """True for the watched group — the bot must stay completely silent there."""
    cid = (chat_id or "").strip()
    src = source_chat_id()
    return bool(cid) and bool(src) and cid == src


def _model() -> str:
    return (os.getenv("NEWPORT_MODEL") or "qwen3.6:35b-a3b").strip()


def _api_base() -> str:
    return (
        os.getenv("NEWPORT_API_BASE")
        or os.getenv("BOT_CHAT_API_BASE")
        or "http://127.0.0.1:11434/v1"
    ).strip().rstrip("/")


def _api_key() -> str:
    return (
        os.getenv("NEWPORT_API_KEY")
        or os.getenv("BOT_CHAT_API_KEY")
        or os.getenv("OPENAI_API_KEY")
        or "ollama"
    ).strip()


def _is_ollama(base: str) -> bool:
    low = (base or "").lower()
    return "11434" in low or "ollama" in low


def _timeout() -> int:
    try:
        return max(30, int(os.getenv("NEWPORT_TIMEOUT", "600")))
    except ValueError:
        return 600


# ---------------------------------------------------------------------------
# Duplicate guard — the same notice must not be posted twice
# ---------------------------------------------------------------------------

_STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "newportwatch.json")
_STATE_LOCK = threading.Lock()
_STATE_MAX = 400


def _load_state() -> dict[str, Any]:
    try:
        with open(_STATE_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {"handled": []}
    if not isinstance(data, dict):
        return {"handled": []}
    return {"handled": [str(x) for x in (data.get("handled") or [])]}


def _save_state(state: dict[str, Any]) -> None:
    tmp = _STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    os.replace(tmp, _STATE_PATH)


def already_handled(message_id: str) -> bool:
    mid = (message_id or "").strip()
    if not mid:
        return False
    with _STATE_LOCK:
        return mid in set(_load_state().get("handled") or [])


def mark_handled(message_id: str) -> None:
    mid = (message_id or "").strip()
    if not mid:
        return
    with _STATE_LOCK:
        state = _load_state()
        handled = [x for x in (state.get("handled") or []) if x != mid]
        handled.append(mid)
        _save_state({"handled": handled[-_STATE_MAX:]})


# ---------------------------------------------------------------------------
# LLM classification
# ---------------------------------------------------------------------------

_SYSTEM = (
    "You classify messages posted in a casino operations group.\n"
    "Decide whether the message announces NEW work of one of these kinds:\n"
    "  stress_test    — a stress test / launching / QA testing schedule for machines,\n"
    "                   ram clear, parameter or denom changes, maintenance+test mode\n"
    "                   timelines, launching dates.\n"
    "  machine_pullout— machines being pulled out / removed / uninstalled, with a\n"
    "                   date and usually a machine count or list.\n"
    "Anything else — chit-chat, thanks, acknowledgements, questions, status updates\n"
    "about work already announced, greetings, reminders with no new schedule — is\n"
    "  other.\n\n"
    "First decide the SPEECH ACT, then the kind. A message can name the right\n"
    "machines, counts, dates and times and still be other — repeating a notice is\n"
    "not making one.\n"
    "  announces_new    — the sender is telling the group about work not yet announced.\n"
    "  asks             — the sender wants to be told: a question, or a request for\n"
    '                     advice, confirmation or instructions ("kindly advise",\n'
    '                     "please confirm", "do we still need", "what time").\n'
    '  reports_done     — the work is finished or already under way ("completed",\n'
    '                     "were pulled out", "has been done", "yesterday", "already").\n'
    "  amends_announced — changes a detail of something already announced.\n"
    "  other_talk       — anything else.\n"
    "If the speech act is asks or reports_done, kind MUST be other, no matter how\n"
    "many machines, counts or dates the message repeats.\n\n"
    "Reply with ONLY a compact JSON object, no prose and no code fences, with the\n"
    "keys in exactly this order:\n"
    '{"speech_act": "announces_new" | "asks" | "reports_done" | "amends_announced" | "other_talk", '
    '"kind": "stress_test" | "machine_pullout" | "other", '
    '"confidence": 0.0-1.0, '
    '"title": "<short headline, max 60 chars>", '
    '"summary": "<1-3 short bullet lines of the key facts: machines, counts, dates, times>"}\n\n'
    "Be strict: if the message does not clearly announce new stress-test or "
    "pull-out work, answer other. A message that only says thank you, asks a "
    "question, or confirms something already done is other."
)


def _strip_think(text: str) -> str:
    return re.sub(r"(?is)<think>.*?</think>", "", text or "").strip()


def _extract_json(text: str) -> dict[str, Any]:
    raw = _strip_think(text)
    raw = re.sub(r"^```(?:json)?|```$", "", raw.strip(), flags=re.I | re.M).strip()
    m = re.search(r"\{.*\}", raw, re.S)
    if not m:
        return {}
    try:
        out = json.loads(m.group(0))
        return out if isinstance(out, dict) else {}
    except json.JSONDecodeError:
        return {}


def classify_message(text: str) -> dict[str, Any]:
    """Ask the LLM what this message is. Returns {} when it cannot decide."""
    body = (text or "").strip()
    if not body:
        return {}
    base = _api_base()
    payload: dict[str, Any] = {
        "model": _model(),
        "messages": [
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": body[:6000]},
        ],
        "max_tokens": 400,
        "temperature": 0,
    }
    if _is_ollama(base):
        payload["reasoning_effort"] = "none"
        payload["think"] = False
        payload["keep_alive"] = (
            os.getenv("BOT_CHAT_OLLAMA_KEEP_ALIVE") or "-1"
        ).strip()
    try:
        resp = requests.post(
            f"{base}/chat/completions",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {_api_key()}",
            },
            json=payload,
            timeout=_timeout(),
        )
        resp.raise_for_status()
        content = (
            ((resp.json().get("choices") or [{}])[0].get("message") or {}).get("content")
            or ""
        )
    except Exception as exc:  # noqa: BLE001 — never break the silent watcher
        print(f"[newport] classify failed: {exc!r}", flush=True)
        return {}
    if isinstance(content, list):
        content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
    out = _extract_json(str(content))
    if out:
        print(
            f"[newport] classify → kind={out.get('kind')!r} conf={out.get('confidence')!r}",
            flush=True,
        )
    return out


def _min_confidence() -> float:
    try:
        return float(os.getenv("NEWPORT_MIN_CONFIDENCE", "0.6"))
    except ValueError:
        return 0.6


# Only a genuine announcement is posted. The model reliably identifies the speech
# act — its summaries say "Completed yesterday", "Inquiry regarding…", "previously
# 8am-10am" — but used to still label those machine_pullout/stress_test, because
# `kind` was emitted BEFORE that observation existed. Asking for speech_act first,
# then gating on it here, is what makes the distinction stick.
#
# ``amends_announced`` (a retime, a postponement, a CANCELLATION) is excluded on
# purpose: those restate work the group already knows about, and a cancellation
# posted as "New stress test information" would be actively misleading. Set
# NEWPORT_POST_AMENDMENTS=1 to have amendments posted as well.
_ANNOUNCING = ("announces_new",)


def _post_amendments() -> bool:
    return (os.getenv("NEWPORT_POST_AMENDMENTS") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def is_new_activity(result: dict[str, Any]) -> bool:
    kind = str((result or {}).get("kind") or "").strip().lower()
    if kind not in ("stress_test", "machine_pullout"):
        return False
    act = str((result or {}).get("speech_act") or "").strip().lower()
    if act:  # absent → fail OPEN: a swapped NEWPORT_MODEL must not mute everything
        allowed = _ANNOUNCING + (("amends_announced",) if _post_amendments() else ())
        if act not in allowed:
            return False
    try:
        conf = float((result or {}).get("confidence") or 0)
    except (TypeError, ValueError):
        conf = 0.0
    return conf >= _min_confidence()


# ---------------------------------------------------------------------------
# Card
# ---------------------------------------------------------------------------

_KIND_LABEL = {
    "stress_test": ("🧪 New stress test information", "orange"),
    "machine_pullout": ("📤 New machine pull-out information", "red"),
}


def build_activity_card(
    *,
    result: dict[str, Any],
    message_text: str,
    image_keys: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Card for the target group: heading, LLM summary, the original text, CC tags."""
    kind = str(result.get("kind") or "").strip().lower()
    header, template = _KIND_LABEL.get(kind, ("📌 New activity information", "blue"))
    title = str(result.get("title") or "").strip()

    elements: list[dict[str, Any]] = []
    summary = str(result.get("summary") or "").strip()
    if title or summary:
        top = f"**{title}**" if title else ""
        if summary:
            top = (top + "\n" if top else "") + summary
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": top}})
        elements.append({"tag": "hr"})

    body = (message_text or "").strip()
    if body:
        if len(body) > 3500:
            body = body[:3500] + "\n…"
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": body}})

    for key in image_keys or []:
        if key:
            elements.append({"tag": "img", "img_key": key, "alt": {"tag": "plain_text", "content": ""}})

    ats = " ".join(f"<at id={oid}></at>" for oid in cc_open_ids() if oid)
    if ats:
        elements.append({"tag": "hr"})
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"cc: {ats}"}})

    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": template, "title": {"tag": "plain_text", "content": header}},
        "body": {"elements": elements or [{"tag": "div", "text": {"tag": "lark_md", "content": "(empty)"}}]},
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


_CLASS_HINT = re.compile(
    r"(?is)pull\s*-?\s*out|pullout|uninstall|stress\s*test|launch|ram\s*clear|denom"
)


def _worth_classifying(body: str) -> bool:
    """Cheap pre-filter before spending an LLM call.

    Long messages always pass. A short one must carry a class word AND a number:
    a terse real notice has counts or dates ("Pull out 19 JJBX Gold Aug 5", 27
    chars), while a bare mention ("Pullout tomorrow.") does not. The old flat
    ``len < 40`` floor dropped the former silently.
    """
    if len(body) >= 40:
        return True
    return bool(_CLASS_HINT.search(body)) and bool(re.search(r"\d", body))


def handle_source_message(
    *,
    chat_id: str,
    message_id: str,
    text: str,
    send_message: Callable[..., Any],
    image_keys: Optional[list[str]] = None,
) -> bool:
    """Classify a message from the watched group; post to the target when it is new.

    Returns True when a card was sent. NEVER sends to ``chat_id`` (the source).
    """
    if not is_enabled() or not is_silent_source_chat(chat_id):
        return False
    if already_handled(message_id):
        return False
    body = (text or "").strip()
    if not _worth_classifying(body):
        print(
            f"[newport] skipped msg {message_id} — no signal ({len(body)} chars)",
            flush=True,
        )
        mark_handled(message_id)
        return False

    result = classify_message(body)
    if not is_new_activity(result):
        mark_handled(message_id)
        return False

    dest = target_chat_id()
    if not dest or dest == source_chat_id():
        # Refuse to post if the target is missing or would be the watched group.
        print("[newport] target chat invalid — not sending", flush=True)
        mark_handled(message_id)
        return False

    card = build_activity_card(result=result, message_text=body, image_keys=image_keys)
    send_message(
        dest,
        json.dumps(card, ensure_ascii=False),
        msg_type="interactive",
        reply_to_message_id="",  # direct post to the target group, never a reply
    )
    mark_handled(message_id)
    print(
        f"[newport] posted {result.get('kind')!r} notice to {dest} "
        f"(msg {message_id})",
        flush=True,
    )
    return True
