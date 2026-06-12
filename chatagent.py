"""
Casual chat agent for Duty Bot — three backends:

1. **API LLM** — OpenAI-compatible (GPT, Ollama, etc.), no local training
2. **Local generative** — fine-tune DistilGPT-2 on your chat pairs (``train-llm``)
3. **Local classifier** — DistilBERT intent + templates (``train``)

**Backend** (``BOT_CHATAGENT_BACKEND``):
    ``auto`` — API LLM if key set → else local generative if trained → else classifier
    ``llm`` — API only
    ``local-llm`` — self-trained generative model only (``chatagent_llm_pt/``)
    ``local`` — classifier + templates only (``chatagent_pt/``)

**Self-train generative (自己练 LLM):**
    python chatagent.py train-llm [--epochs 5]
    Saves to ``chatagent_llm_pt/`` (DistilGPT-2 fine-tuned on chat Q→A pairs).

**Train classifier fallback:**
    python chatagent.py train [--epochs 10]
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import random
import re
import sys
import traceback
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

_CHBOX_DIR = Path(__file__).resolve().parent
DEFAULT_MODEL_DIR = _CHBOX_DIR / "chatagent_pt"
DEFAULT_GEN_MODEL_DIR = _CHBOX_DIR / "chatagent_llm_pt"
GEN_BASE_MODEL = (os.getenv("BOT_CHAT_GEN_BASE_MODEL") or "distilgpt2").strip()
GEN_MAX_SEQ_LEN = int(os.getenv("BOT_CHAT_GEN_MAX_SEQ_LEN", "256"))
GEN_MAX_NEW_TOKENS = int(os.getenv("BOT_CHAT_GEN_MAX_NEW_TOKENS", "100"))
CONFIDENCE_THRESHOLD = float(os.getenv("BOT_CHAT_CONFIDENCE", "0.10"))
CONFIDENCE_MARGIN = float(os.getenv("BOT_CHAT_MARGIN", "0.02"))
MAX_SEQ_LEN = 128
MAX_CHAT_WORDS = int(os.getenv("BOT_CHAT_MAX_WORDS", "80"))
LLM_TIMEOUT_SEC = float(os.getenv("BOT_CHAT_LLM_TIMEOUT", "30"))
LLM_MAX_TOKENS = int(os.getenv("BOT_CHAT_LLM_MAX_TOKENS", "220"))
DEFAULT_LLM_MODEL = (os.getenv("BOT_CHAT_MODEL") or "gpt-4o-mini").strip()
DEFAULT_LLM_BASE = (os.getenv("BOT_CHAT_API_BASE") or "https://api.openai.com/v1").strip().rstrip("/")

_SYSTEM_PROMPT = """You are Duty Bot, a friendly workplace assistant on Lark/Feishu for the OSE team.
Users may casually chat or ask about duty rosters, leave/WFH, holidays, machines, and Jenkins helpers.

For casual conversation: reply naturally, warm, and concise (1–3 short sentences). Light emoji is fine.
Always reply in English only, even if the user writes Chinese.
If they ask for work data you cannot look up in chat, gently suggest `/help` or examples like "who is on fpms duty".
Never invent duty names, phone numbers, machine IDs, or confidential information.
Stay professional; avoid politics, religion, and inappropriate topics."""

_torch = None
_classifier_singleton: Optional["ChatClassifier"] = None
_classifier_failed: bool = False
_generative_singleton: Optional["LocalGenerativeChat"] = None
_generative_failed: bool = False
_llm_failed_logged: bool = False

_COMMANDISH_RE = re.compile(
    r"(?i)\b("
    r"duty|fpms|bi|sre|db|fe|cpms|pms|ote|leave|wfh|holiday|jenkins|"
    r"machine|asset|nch|nwr|winford|checkcredit|offset|reminder|"
    r"wholeave|cctv|sms|credit|deploy|build|ticket|incident|oncall|on-call"
    r")\b|/"
)

_CJK_RE = re.compile(r"[\u4e00-\u9fff]")


@dataclass
class ChatIntentSpec:
    tag: str
    patterns: list[str] = field(default_factory=list)
    responses: list[str] = field(default_factory=list)


def _lazy_torch():
    global _torch
    if _torch is None:
        import torch

        _torch = torch
    return _torch


def is_enabled() -> bool:
    return (os.getenv("BOT_USE_CHATAGENT") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def backend_mode() -> str:
    mode = (os.getenv("BOT_CHATAGENT_BACKEND") or "auto").strip().lower()
    if mode in ("llm", "local-llm", "local", "auto"):
        return mode
    return "auto"


def _llm_api_key() -> str:
    return (os.getenv("BOT_CHAT_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()


def _llm_model() -> str:
    return (os.getenv("BOT_CHAT_MODEL") or DEFAULT_LLM_MODEL).strip()


def _llm_base_url() -> str:
    return (os.getenv("BOT_CHAT_API_BASE") or DEFAULT_LLM_BASE).strip().rstrip("/")


def llm_available() -> bool:
    return bool(_llm_api_key())


def _sanitize_llm_reply(text: str) -> str:
    out = (text or "").strip()
    out = re.sub(r"\n{3,}", "\n\n", out)
    if len(out) > 1200:
        out = out[:1197].rstrip() + "..."
    if _CJK_RE.search(out):
        return ""
    return out


def _llm_chat(user_text: str) -> Optional[str]:
    """Call OpenAI-compatible chat/completions. Returns None on failure."""
    global _llm_failed_logged
    api_key = _llm_api_key()
    if not api_key:
        return None
    url = f"{_llm_base_url()}/chat/completions"
    payload = {
        "model": _llm_model(),
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ],
        "max_tokens": LLM_MAX_TOKENS,
        "temperature": float(os.getenv("BOT_CHAT_LLM_TEMPERATURE", "0.75")),
    }
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
        with urllib.request.urlopen(req, timeout=LLM_TIMEOUT_SEC) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        choices = body.get("choices") or []
        if not choices:
            return None
        content = (choices[0].get("message") or {}).get("content")
        reply = _sanitize_llm_reply(content or "")
        return reply or None
    except urllib.error.HTTPError as exc:
        err_body = ""
        try:
            err_body = exc.read().decode("utf-8", errors="replace")[:400]
        except Exception:
            pass
        if not _llm_failed_logged:
            print(f"⚠️ Chat LLM HTTP {exc.code}: {err_body or exc.reason}", flush=True)
            _llm_failed_logged = True
        return None
    except Exception as exc:
        if not _llm_failed_logged:
            print(f"⚠️ Chat LLM request failed: {exc!r}", flush=True)
            _llm_failed_logged = True
        return None


def _local_reply(text: str) -> Optional[str]:
    clf = _get_classifier()
    if clf is None:
        return None
    try:
        return clf.reply(text)
    except Exception as exc:
        print(f"⚠️ Chat local reply error: {exc!r}", flush=True)
        return None


def gen_model_dir() -> Path:
    explicit = (os.getenv("BOT_CHATAGENT_LLM_DIR") or "").strip()
    if explicit:
        return Path(explicit)
    return DEFAULT_GEN_MODEL_DIR


def generative_model_ready() -> bool:
    path = gen_model_dir()
    return (path / "config.json").is_file()


def _gen_training_prompt(user: str, bot: str) -> str:
    return f"User: {user.strip()}\nBot: {bot.strip()}\n"


def _gen_inference_prompt(user: str) -> str:
    return f"User: {user.strip()}\nBot: "


def build_generative_training_texts(intents: list[ChatIntentSpec]) -> list[str]:
    texts: list[str] = []
    seen: set[str] = set()
    for spec in intents:
        for pat in spec.patterns:
            for resp in spec.responses:
                for user in (pat, pat.lower()):
                    line = _gen_training_prompt(user, resp)
                    if line not in seen:
                        seen.add(line)
                        texts.append(line)
    return texts


class LocalGenerativeChat:
    """Small locally fine-tuned causal LM (e.g. DistilGPT-2)."""

    def __init__(self, model_path: Path):
        from transformers import AutoModelForCausalLM, AutoTokenizer

        torch = _lazy_torch()
        path = str(model_path)
        local = {"local_files_only": True}
        self.tokenizer = AutoTokenizer.from_pretrained(path, **local)
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        self.model = AutoModelForCausalLM.from_pretrained(path, **local)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = self.model.to(self.device)
        self.model.eval()

    def reply(self, user_text: str) -> Optional[str]:
        torch = _lazy_torch()
        prompt = _gen_inference_prompt(user_text)
        inputs = self.tokenizer(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=GEN_MAX_SEQ_LEN,
        )
        inputs = {k: v.to(self.device) for k, v in inputs.items()}
        with torch.no_grad():
            out = self.model.generate(
                **inputs,
                max_new_tokens=GEN_MAX_NEW_TOKENS,
                do_sample=True,
                temperature=float(os.getenv("BOT_CHAT_GEN_TEMPERATURE", "0.85")),
                top_p=float(os.getenv("BOT_CHAT_GEN_TOP_P", "0.92")),
                pad_token_id=self.tokenizer.pad_token_id,
                eos_token_id=self.tokenizer.eos_token_id,
            )
        full = self.tokenizer.decode(out[0], skip_special_tokens=True)
        if "Bot: " not in full:
            return None
        reply = full.split("Bot: ", 1)[1]
        if "User:" in reply:
            reply = reply.split("User:", 1)[0]
        reply = _sanitize_llm_reply(reply.strip())
        return reply or None


def _get_generative() -> Optional[LocalGenerativeChat]:
    global _generative_singleton, _generative_failed
    if _generative_singleton is not None:
        return _generative_singleton
    if _generative_failed:
        return None
    path = gen_model_dir()
    if not (path / "config.json").is_file():
        return None
    try:
        _generative_singleton = LocalGenerativeChat(path)
        print(f"✅ Local generative chat loaded from {path}", flush=True)
        return _generative_singleton
    except Exception as exc:
        print(f"⚠️ Local generative chat load failed: {exc!r}", flush=True)
        traceback.print_exc()
        _generative_failed = True
        return None


def _local_generative_reply(text: str) -> Optional[str]:
    gen = _get_generative()
    if gen is None:
        return None
    try:
        return gen.reply(text)
    except Exception as exc:
        print(f"⚠️ Local generative reply error: {exc!r}", flush=True)
        return None


def model_dir() -> Path:
    explicit = (os.getenv("BOT_CHATAGENT_MODEL_DIR") or "").strip()
    if explicit:
        return Path(explicit)
    return DEFAULT_MODEL_DIR


def _chat_intent(tag: str, responses: list[str], *pattern_groups: str) -> ChatIntentSpec:
    pats: list[str] = []
    for g in pattern_groups:
        pats.extend(s.strip() for s in g.split("|") if s.strip())
    return ChatIntentSpec(tag=tag, patterns=list(dict.fromkeys(pats)), responses=responses)


def build_chat_catalog() -> list[ChatIntentSpec]:
    intents: list[ChatIntentSpec] = []

    intents.append(
        _chat_intent(
            "chat_greeting",
            [
                "Hi! 👋 I'm Duty Bot — ask me about duty, leave, machines, or say `/help`.",
                "Hello! How can I help you today?",
                "Hey there! 👋 Work questions welcome — e.g. “who is on fpms duty”.",
            ],
            "hi|hello|hey|hiya|yo|howdy|greetings|good day|"
            "你好|您好|嗨|哈喽|哈啰|早上好|下午好|晚上好",
        )
    )

    intents.append(
        _chat_intent(
            "chat_how_are_you",
            [
                "I'm doing great, thanks for asking! 🤖 What can I help you with?",
                "All good here — ready when you need duty info or machine lookup.",
                "Pretty good! Ask me anything work-related or just say hi anytime.",
            ],
            "how are you|how r u|how are u|how is it going|how are things|"
            "how you doing|how are you doing|how are you doing today|"
            "hey how are you|hey how are you doing|what's up|whats up|sup|wassup|you good|"
            "你好吗|怎么样|还好吗|最近怎样",
        )
    )

    intents.append(
        _chat_intent(
            "chat_thanks",
            [
                "You're welcome! 😊",
                "Anytime — happy to help!",
                "No problem at all!",
            ],
            "thanks|thank you|thanks so much|thank you so much|thx|ty|cheers|"
            "much appreciated|appreciate it|"
            "谢谢|多谢|感谢|辛苦了",
        )
    )

    intents.append(
        _chat_intent(
            "chat_goodbye",
            [
                "Bye! 👋 Ping me anytime you need duty info.",
                "See you later!",
                "Take care — I'll be here when you need me.",
            ],
            "bye|goodbye|see you|see ya|cya|later|talk later|catch you later|"
            "good night|gn|night night|再见|拜拜|晚安",
        )
    )

    intents.append(
        _chat_intent(
            "chat_who_are_you",
            [
                "I'm **Duty Bot** 🤖 — department duty, leave/WFH, holidays, machines, Jenkins helpers. Try `/help`.",
                "Duty Bot at your service! I understand English for both chat and work commands (with AI on).",
            ],
            "who are you|what are you|what can you do|are you a bot|are you real|"
            "tell me about yourself|introduce yourself|"
            "你是谁|你是什么|你能做什么",
        )
    )

    intents.append(
        _chat_intent(
            "chat_compliment",
            [
                "Thanks! 😄 That's kind of you.",
                "Aw, thank you — glad I could help!",
                "You're too kind! Let me know if you need anything else.",
            ],
            "nice|cool|awesome|great job|well done|good bot|you rock|love you bot|"
            "you're the best|amazing|fantastic|厉害|不错|棒极了",
        )
    )

    intents.append(
        _chat_intent(
            "chat_laugh",
            [
                "😄 Glad something's funny!",
                "Haha — need anything else?",
                "LOL — I'm here if you need duty stuff too.",
            ],
            "haha|hahaha|lol|lmao|rofl|hehe|funny|that's funny|so funny",
        )
    )

    intents.append(
        _chat_intent(
            "chat_sorry",
            [
                "No worries at all!",
                "It's okay — how can I help?",
                "All good! Don't worry about it.",
            ],
            "sorry|my bad|apologies|didn't mean to|oops|excuse me|对不起|抱歉",
        )
    )

    intents.append(
        _chat_intent(
            "chat_ack",
            [
                "Got it 👍",
                "Okay!",
                "Sure thing.",
                "Alright — shout if you need me.",
            ],
            "ok|okay|k|sure|alright|all right|got it|understood|roger|noted|fine|"
            "好的|明白|收到|嗯|行",
        )
    )

    intents.append(
        _chat_intent(
            "chat_morning",
            [
                "Good morning! ☀️ Hope you have a smooth day — I'm here if you need duty info.",
                "Morning! Coffee time? ☕ I'm ready when you need `/fpms` or anything else.",
            ],
            "good morning|morning|gm|top of the morning|早|早安",
        )
    )

    intents.append(
        _chat_intent(
            "chat_tired",
            [
                "Hang in there! 💪 Take a break if you can — I'll handle the bot stuff when you're back.",
                "Long day? Rest up — I'm always here for quick duty lookups.",
            ],
            "i'm tired|so tired|exhausted|long day|need a break|burned out|burnout|"
            "好累|太累了|累死了",
        )
    )

    intents.append(
        _chat_intent(
            "chat_weather",
            [
                "I don't have a window 🌤️ — but I can fetch duty rosters! Try `/fpms` or “who is on bi duty”.",
                "No weather radar here — only spreadsheets and duty lists 😄",
            ],
            "weather|rain|sunny|hot today|cold today|going to rain|temperature|"
            "天气|下雨|好热",
        )
    )

    intents.append(
        _chat_intent(
            "chat_weekend",
            [
                "Hope you get a good rest! 🎉 I'll be here Monday for duty questions.",
                "Enjoy the weekend! Ping me anytime for on-call / duty info.",
            ],
            "weekend|friday|happy friday|tgif|saturday plans|sunday|long weekend|"
            "周末|星期五",
        )
    )

    intents.append(
        _chat_intent(
            "chat_confused",
            [
                "No problem — try `/help` for commands, or ask in plain English like “show fpms duty”.",
                "I'm not sure what you mean — duty question? Try `@Duty Bot /help`.",
            ],
            "i don't understand|don't get it|what do you mean|confused|huh|"
            "听不懂|不明白|什么意思",
        )
    )

    intents.append(
        _chat_intent(
            "chat_bored",
            [
                "Maybe check who's on duty? `/fpms` `/bi` `/sre` — or just chat, I'm listening 😄",
                "Bored? I can't stream Netflix — but I can tell you today's holidays with `/holiday`.",
            ],
            "i'm bored|so bored|nothing to do|kill time|boring|im bored|im boring|i am bored|"
            "i am boring|feeling bored|好无聊",
        )
    )

    # Catch-all casual English (trained heavily so most off-topic chat gets a friendly reply)
    general_patterns = [
        "just chatting",
        "wanted to say hi",
        "random thought",
        "having a coffee",
        "taking a break",
        "chilling at desk",
        "just chilling at my desk",
        "slow day today",
        "busy day today",
        "almost lunch time",
        "feeling good today",
        "not bad today",
        "you there",
        "anyone there",
        "talk to me",
        "let's chat",
        "nice to meet you",
        "what a day",
        "crazy day",
        "stressed out",
        "happy today",
        "just saying",
        "never mind",
        "fair enough",
        "makes sense",
        "sounds good",
        "interesting",
        "tell me more",
        "really",
        "wow",
        "oh nice",
        "that's cool",
        "good to know",
        "what's new",
        "how's your day",
        "you busy",
        "are you free",
        "just wondering",
        "no reason",
        "forget it",
        "carry on",
        "as you were",
        "good talk",
        "hmm",
        "i see",
        "right",
        "yeah",
        "yep",
        "nope",
        "maybe later",
        "we'll see",
        "let me think",
        "one sec",
        "hold on",
        "brb",
        "back now",
        "still here",
        "you awake",
        "anybody home",
        "knock knock",
        "tell me something",
        "surprise me",
        "cheer me up",
        "i need a break",
        "monday again",
        "almost friday",
        "weekend soon",
        "coffee break",
        "lunch soon",
    ]

    intents.append(
        _chat_intent(
            "chat_general",
            [
                "I'm mostly built for work stuff (duty, leave, machines) — but happy to chat briefly! "
                "Need anything? Try `/help` or ask naturally.",
                "Got you 😊 I'm Duty Bot — casual chat is fine; for tasks say things like “who is on fpms duty”.",
                "I hear you! For work I can help with rosters and lookups — otherwise I'm glad to keep you company.",
            ],
            "|".join(general_patterns),
        )
    )

    return intents


def _looks_like_command(text: str) -> bool:
    return (text or "").lstrip().startswith("/") or bool(_COMMANDISH_RE.search(text or ""))


def startup_status() -> None:
    enabled = is_enabled()
    mode = backend_mode()
    llm_ok = llm_available()
    path = model_dir()
    gen_path = gen_model_dir()
    has_classifier = (path / "config.json").is_file()
    has_generative = generative_model_ready()
    print(
        f"[chatagent] BOT_USE_CHATAGENT={os.getenv('BOT_USE_CHATAGENT')!r} enabled={enabled} "
        f"backend={mode} api_key={'yes' if llm_ok else 'no'} api_model={_llm_model()!r} "
        f"classifier_dir={path} classifier_exists={has_classifier} "
        f"generative_dir={gen_path} generative_exists={has_generative}",
        flush=True,
    )
    if not enabled:
        print("[chatagent] Casual chat OFF.", flush=True)
        return
    if mode in ("llm", "auto") and llm_ok:
        print(f"[chatagent] ✅ API LLM ready ({_llm_model()} @ {_llm_base_url()})", flush=True)
    elif mode == "llm" and not llm_ok:
        print("[chatagent] ⚠️ backend=llm but no OPENAI_API_KEY / BOT_CHAT_API_KEY.", flush=True)
    if mode in ("local-llm", "auto") and has_generative:
        gen = _get_generative()
        if gen is None:
            print("[chatagent] ⚠️ Generative model present but failed to load.", flush=True)
        else:
            print(f"[chatagent] ✅ Self-trained generative chat ready ({gen_path})", flush=True)
    elif mode == "local-llm" and not has_generative:
        print("[chatagent] ⚠️ Run: python chatagent.py train-llm", flush=True)
    if mode in ("local", "auto") and has_classifier:
        if mode == "local" or (not llm_ok and not has_generative):
            clf = _get_classifier()
            if clf is None:
                print("[chatagent] ⚠️ Classifier present but failed to load.", flush=True)
            else:
                print(f"[chatagent] ✅ Classifier chat ready (threshold={CONFIDENCE_THRESHOLD}).", flush=True)
    elif mode == "local" and not has_classifier:
        print("[chatagent] ⚠️ Run: python chatagent.py train", flush=True)


class ChatClassifier:
    def __init__(self, model_path: Path):
        from commandagent import _load_pretrained_compat

        torch, self.tokenizer, self.model = _load_pretrained_compat(model_path)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = self.model.to(self.device)
        self.model.eval()
        with (model_path / "metadata.pkl").open("rb") as f:
            meta = pickle.load(f)
        self.tag_to_id: dict[str, int] = meta["tag_to_id"]
        self.id_to_tag: dict[int, str] = meta["id_to_tag"]
        self.intents_by_tag: dict[str, ChatIntentSpec] = {}
        for item in meta.get("intents", []):
            if isinstance(item, ChatIntentSpec):
                spec = item
            else:
                spec = ChatIntentSpec(
                    tag=item["tag"],
                    patterns=item.get("patterns", []),
                    responses=item.get("responses", []),
                )
            self.intents_by_tag[spec.tag] = spec

    def predict(self, text: str) -> tuple[str, float, float]:
        torch = _lazy_torch()
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

    def reply(self, text: str) -> Optional[str]:
        from chitchat import pick_localized_reply

        tag, conf, margin = self.predict(text)
        threshold = CONFIDENCE_THRESHOLD
        margin_min = CONFIDENCE_MARGIN
        if tag == "chat_general":
            threshold = max(threshold, 0.22)
            margin_min = max(margin_min, 0.06)
        if conf < threshold or margin < margin_min:
            return None
        spec = self.intents_by_tag.get(tag)
        if not spec or not spec.responses:
            return None
        return pick_localized_reply(spec.responses, text)


def chat_signal(text: str) -> dict:
    """Diagnostic signal for the router (``chathandleagent``).

    Returns ``{"tag", "confidence", "margin"}`` from the local chat classifier.
    Falls back to a confident signal when the rule-based ``chitchat`` matches,
    so the router can still detect small talk even before the model is trained.
    Never raises.
    """
    out = {"tag": None, "confidence": 0.0, "margin": 0.0}
    raw = (text or "").strip()
    if not raw:
        return out
    try:
        import chitchat

        if chitchat.looks_like_chitchat(raw):
            out.update(tag="chitchat", confidence=0.9, margin=0.9)
            return out
    except Exception:
        pass
    clf = _get_classifier()
    if clf is None:
        return out
    try:
        tag, conf, margin = clf.predict(raw)
        out.update(tag=tag, confidence=float(conf), margin=float(margin))
    except Exception as exc:
        print(f"⚠️ chat_signal error: {exc!r}", flush=True)
    return out


def _get_classifier() -> Optional[ChatClassifier]:
    global _classifier_singleton, _classifier_failed
    if _classifier_singleton is not None:
        return _classifier_singleton
    if _classifier_failed:
        return None
    path = model_dir()
    if not (path / "config.json").is_file():
        print(f"⚠️ Chat model not found at {path}", flush=True)
        _classifier_failed = True
        return None
    try:
        _classifier_singleton = ChatClassifier(path)
        print(f"✅ Chat agent loaded from {path}", flush=True)
        return _classifier_singleton
    except Exception as exc:
        print(f"⚠️ Chat agent load failed: {exc!r}", flush=True)
        traceback.print_exc()
        _classifier_failed = True
        return None


def reply_if_enabled(text: str) -> Optional[str]:
    """Return a casual chat reply: API LLM → local generative → classifier."""
    if not is_enabled():
        return None
    raw = (text or "").strip()
    if not raw or _looks_like_command(raw):
        return None
    if len(raw.split()) > MAX_CHAT_WORDS:
        return None

    mode = backend_mode()
    if mode in ("llm", "auto") and llm_available():
        reply = _llm_chat(raw)
        if reply:
            print(f"[chatagent] API LLM reply ({len(reply)} chars)", flush=True)
            return reply
        if mode == "llm":
            return None

    if mode in ("local-llm", "auto"):
        reply = _local_generative_reply(raw)
        if reply:
            print(f"[chatagent] Local generative reply ({len(reply)} chars)", flush=True)
            return reply
        if mode == "local-llm":
            return None

    if mode in ("local", "auto"):
        return _local_reply(raw)
    return None


def prepare_training_examples(
    intents: list[ChatIntentSpec],
) -> tuple[list[str], list[int], dict[str, int]]:
    texts: list[str] = []
    labels: list[int] = []
    tag_to_id: dict[str, int] = {}
    for idx, spec in enumerate(intents):
        tag_to_id[spec.tag] = idx
        seen: set[str] = set()
        for pat in spec.patterns:
            for variant in (pat, pat.lower()):
                if variant not in seen:
                    seen.add(variant)
                    texts.append(variant)
                    labels.append(idx)
    return texts, labels, tag_to_id


def train_model(
    output_dir: Path,
    *,
    epochs: int = 10,
    batch_size: int = 32,
    lr: float = 2e-5,
) -> dict[str, Any]:
    import random

    from torch.utils.data import DataLoader, TensorDataset
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    from commandagent import _save_model_compat

    torch = _lazy_torch()
    intents = build_chat_catalog()
    texts, labels, tag_to_id = prepare_training_examples(intents)
    print(f"[chatagent] Training samples: {len(texts)} intents: {len(intents)}")

    base_pairs = list(zip(texts, labels))
    random.seed(42)
    random.shuffle(base_pairs)
    split = int(len(base_pairs) * 0.85)
    val_pairs = base_pairs[split:]
    train_base = base_pairs[:split]

    # Oversample minority intents on train only so chat_general does not dominate.
    by_label: dict[int, list[tuple[str, int]]] = {}
    for t, lb in train_base:
        by_label.setdefault(lb, []).append((t, lb))
    max_n = max(len(v) for v in by_label.values())
    train_pairs: list[tuple[str, int]] = []
    for lb, items in by_label.items():
        reps = max(1, max_n // max(len(items), 1))
        train_pairs.extend(items * reps)
    random.shuffle(train_pairs)
    print(f"[chatagent] Train pairs (balanced): {len(train_pairs)}  val: {len(val_pairs)}")

    tok = AutoTokenizer.from_pretrained("distilbert-base-uncased", use_fast=True)
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

    model = AutoModelForSequenceClassification.from_pretrained(
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
            {"tag": i.tag, "patterns": i.patterns, "responses": i.responses}
            for i in intents
        ],
    }
    with (output_dir / "metadata.pkl").open("wb") as f:
        pickle.dump(meta, f)

    print(f"✅ Chat model saved to {output_dir} best_val_acc={best_acc:.1%}")
    return {"val_accuracy": best_acc, "samples": len(texts), "intents": len(intents)}


def train_generative_model(
    output_dir: Path,
    *,
    epochs: int = 5,
    batch_size: int = 4,
    lr: float = 5e-5,
) -> dict[str, Any]:
    """Fine-tune a small causal LM (DistilGPT-2) on chat Q→A pairs — train your own LLM locally."""
    from torch.utils.data import DataLoader, TensorDataset
    from transformers import AutoModelForCausalLM, AutoTokenizer

    torch = _lazy_torch()
    intents = build_chat_catalog()
    samples = build_generative_training_texts(intents)
    random.seed(42)
    random.shuffle(samples)
    split = int(len(samples) * 0.9)
    train_samples, val_samples = samples[:split], samples[split:]
    print(
        f"[chatagent] Generative train: {len(train_samples)} samples, "
        f"val={len(val_samples)}, base={GEN_BASE_MODEL!r}"
    )

    tok = AutoTokenizer.from_pretrained(GEN_BASE_MODEL)
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = AutoModelForCausalLM.from_pretrained(GEN_BASE_MODEL).to(device)

    def _encode_batch(texts: list[str]):
        enc = tok(
            texts,
            truncation=True,
            padding="max_length",
            max_length=GEN_MAX_SEQ_LEN,
            return_tensors="pt",
        )
        return enc["input_ids"], enc["attention_mask"]

    def _loader(items: list[str], shuffle: bool) -> DataLoader:
        ids_list, mask_list = [], []
        for text in items:
            i, m = _encode_batch([text])
            ids_list.append(i.squeeze(0))
            mask_list.append(m.squeeze(0))
        ds = TensorDataset(torch.stack(ids_list), torch.stack(mask_list))
        return DataLoader(ds, batch_size=batch_size, shuffle=shuffle)

    train_loader = _loader(train_samples, shuffle=True)
    val_loader = _loader(val_samples, shuffle=False)
    optim = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=0.01)

    best_loss = float("inf")
    output_dir.mkdir(parents=True, exist_ok=True)

    for epoch in range(1, epochs + 1):
        model.train()
        train_loss = 0.0
        for input_ids, attention_mask in train_loader:
            input_ids = input_ids.to(device)
            attention_mask = attention_mask.to(device)
            optim.zero_grad()
            out = model(input_ids=input_ids, attention_mask=attention_mask, labels=input_ids)
            out.loss.backward()
            optim.step()
            train_loss += float(out.loss.item())

        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for input_ids, attention_mask in val_loader:
                input_ids = input_ids.to(device)
                attention_mask = attention_mask.to(device)
                out = model(input_ids=input_ids, attention_mask=attention_mask, labels=input_ids)
                val_loss += float(out.loss.item())

        avg_train = train_loss / max(len(train_loader), 1)
        avg_val = val_loss / max(len(val_loader), 1)
        print(f"Epoch {epoch}/{epochs}  train_loss={avg_train:.4f}  val_loss={avg_val:.4f}")
        if avg_val <= best_loss:
            best_loss = avg_val
            model.save_pretrained(str(output_dir))
            tok.save_pretrained(str(output_dir))

    meta = {"base_model": GEN_BASE_MODEL, "samples": len(samples), "format": "User: ...\\nBot: ...\\n"}
    with (output_dir / "metadata.pkl").open("wb") as f:
        pickle.dump(meta, f)

    print(f"✅ Generative chat model saved to {output_dir} best_val_loss={best_loss:.4f}")
    print("   Set BOT_CHATAGENT_BACKEND=local-llm (or auto) and restart larkbot.")
    return {"val_loss": best_loss, "samples": len(samples)}


def evaluate_model(model_path: Path) -> None:
    intents = build_chat_catalog()
    texts, labels, _ = prepare_training_examples(intents)
    clf = ChatClassifier(model_path)
    correct = 0
    for text, label in zip(texts, labels):
        tag, _, _ = clf.predict(text)
        if clf.tag_to_id.get(tag, -1) == label:
            correct += 1
    acc = correct / max(len(texts), 1)
    print(f"Train-set accuracy (sanity): {acc:.1%} ({correct}/{len(texts)})")


def _cli_test(phrase: str, model_path: Path) -> None:
    print(f"Input:       {phrase!r}")
    print(f"Backend:     {backend_mode()}  api_key={'yes' if llm_available() else 'no'}")
    if backend_mode() in ("llm", "auto") and llm_available():
        print(f"API reply:   {_llm_chat(phrase)!r}")
        if backend_mode() == "llm":
            return
    gen_path = gen_model_dir()
    if backend_mode() in ("local-llm", "auto") and (gen_path / "config.json").is_file():
        global _generative_singleton, _generative_failed
        _generative_singleton = None
        _generative_failed = False
        print(f"Gen reply:   {_local_generative_reply(phrase)!r}")
        if backend_mode() == "local-llm":
            return
    if not (model_path / "config.json").is_file():
        print(f"Classifier not found at {model_path}. Run: python chatagent.py train")
        return
    clf = ChatClassifier(model_path)
    tag, conf, margin = clf.predict(phrase)
    print(f"Classifier:  {tag} ({conf:.3f}, margin={margin:.3f})")
    print(f"Template:    {clf.reply(phrase)!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Duty Bot chat agent (casual conversation)")
    sub = parser.add_subparsers(dest="cmd")

    p_train = sub.add_parser("train", help="Train DistilBERT classifier + templates")
    p_train.add_argument("--epochs", type=int, default=10)
    p_train.add_argument("--output", type=str, default=str(DEFAULT_MODEL_DIR))

    p_train_llm = sub.add_parser("train-llm", help="Fine-tune local generative LLM (DistilGPT-2)")
    p_train_llm.add_argument("--epochs", type=int, default=5)
    p_train_llm.add_argument("--output", type=str, default=str(DEFAULT_GEN_MODEL_DIR))

    p_test = sub.add_parser("test", help="Test a phrase")
    p_test.add_argument("phrase", type=str)
    p_test.add_argument("--model", type=str, default=str(DEFAULT_MODEL_DIR))

    sub.add_parser("eval", help="Evaluate classifier on training patterns")
    sub.add_parser("patterns", help="Show pattern counts per intent")

    args = parser.parse_args()
    if args.cmd == "train":
        train_model(Path(args.output), epochs=args.epochs)
    elif args.cmd == "train-llm":
        train_generative_model(Path(args.output), epochs=args.epochs)
    elif args.cmd == "test":
        _cli_test(args.phrase, Path(args.model))
    elif args.cmd == "eval":
        evaluate_model(model_dir())
    elif args.cmd == "patterns":
        intents = build_chat_catalog()
        total = 0
        for spec in intents:
            n = len(spec.patterns)
            total += n
            print(f"{spec.tag:22} {n:4} patterns  {len(spec.responses)} responses")
        print(f"Total patterns: {total}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
