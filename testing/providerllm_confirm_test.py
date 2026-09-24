#!/usr/bin/env python3
"""providerllm.confirm_window - the confirm-before-write question, offline.

  python3 testing/providerllm_confirm_test.py      exit 1 on any failure

The model call (_chat) is replaced by a fake that records what it was asked and
returns a canned reply; a real network call raises. Nothing leaves this process.
"""
import datetime as d
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import providerllm as pl  # noqa: E402

NET = {"n": 0}


def _blocked(*_a, **_k):
    NET["n"] += 1
    raise RuntimeError("network blocked in test")


pl.requests.post = _blocked
TZ = d.timezone(d.timedelta(hours=8))
S, E = d.datetime(2026, 9, 26, 14, 30, tzinfo=TZ), d.datetime(2026, 9, 26, 16, 30, tzinfo=TZ)
TEXT = "Scheduled maintenance on 26/09/2026 from 2.30 - 4.30 pm (GMT+8)."
ASKED, FAILS = [], []


def model(reply=None, raises=None):
    def fake(messages, **_k):
        ASKED.append(messages)
        if raises:
            raise raises
        return reply
    pl._chat = fake


def ask(**kw):
    ASKED.clear()
    return pl.confirm_window(TEXT, provider="Pragmatic Play", start=S, end=E,
                             names=["Pragmatic Play", "PP"], **kw)


def check(name, ok, got=""):
    print(("PASS  " if ok else "FAIL  ") + name + ("" if ok else f"   -> {got}"))
    if not ok:
        FAILS.append(name)


os.environ["BOT_CHAT_API_BASE"] = "http://127.0.0.1:11434/v1"
YES = '{"kind": "announcement", "whose": "provider", "window": "same", "confidence": 0.9, "why": "ok"}'

model(YES)
r = ask()
check("three yeses with confidence >= 0.7 confirm", r["verdict"] == "yes", r)
user = ASKED[0][1]["content"] if ASKED else ""
check("the prompt carries the provider, its alias and the parser's GMT+8 window",
      "Pragmatic Play" in user and "PP" in user
      and "2026-09-26 14:30 to 2026-09-26 16:30 (GMT+8)" in user, user)
sysmsg = ASKED[0][0]["content"] if ASKED else ""
check("the model is told it never supplies a time", "never supply dates or times" in sysmsg, sysmsg[:200])

model('{"kind": "announcement", "whose": "provider", "window": "different", "confidence": 0.95, "why": "pm"}')
r = ask()
check("a window mismatch is a NO, not a retry", r["verdict"] == "no" and not r["transient"]
      and "window is not the one the parser read" in r["why"], r)
model('{"kind": "cancelled_or_finished", "whose": "provider", "window": "same", "confidence": 0.9, "why": "cancelled"}')
check("a cancelled / finished notice is a NO", ask()["verdict"] == "no")
model('{"kind": "question", "whose": "provider", "window": "same", "confidence": 0.9, "why": "asks"}')
check("a question is a NO", ask()["verdict"] == "no")
model('{"kind": "announcement", "whose": "operator", "window": "same", "confidence": 0.9, "why": "operator"}')
check("the operator's own maintenance is a NO", ask()["verdict"] == "no")
model('{"kind": "announcement", "whose": "unclear", "window": "same", "confidence": 0.9, "why": "?"}')
r = ask()
check("'whose: unclear' is carded (unclear), not written", r["verdict"] == "unclear" and not r["transient"], r)
model('{"kind": "announcement", "whose": "provider", "window": "same", "confidence": 0.4, "why": "unsure"}')
r = ask()
check("low confidence is unclear and carded, not retried", r["verdict"] == "unclear" and not r["transient"], r)
model('{"kind": "Announcement", "whose": "Provider", "window": "Same", "confidence": "0.8"}')
check("choices are read case-insensitively, confidence as a string", ask()["verdict"] == "yes")
model('{"kind": "yes", "whose": "provider", "window": "same", "confidence": 0.9}')
r = ask()
check("an unreadable field fails closed and is retried", r["verdict"] == "unclear" and r["transient"], r)
model("I think this is probably fine.")
r = ask()
check("a reply with no JSON fails closed and is retried", r["verdict"] == "unclear" and r["transient"], r)
model(raises=TimeoutError("read timed out"))
r = ask()
check("a timeout (cold model) fails closed and is retried", r["verdict"] == "unclear" and r["transient"], r)

model(YES)
os.environ["BOT_CHAT_API_BASE"] = "https://api.openai.com/v1"
r = ask()
check("a REMOTE endpoint is never called - carded, not retried",
      r["verdict"] == "unclear" and not r["transient"] and not ASKED and "not on this server" in r["why"], r)
os.environ["VAWATCH_CONFIRM_ALLOW_REMOTE"] = "1"
check("...unless VAWATCH_CONFIRM_ALLOW_REMOTE=1", ask()["verdict"] == "yes")
os.environ.pop("VAWATCH_CONFIRM_ALLOW_REMOTE")
for base in ("http://ollama:11434/v1", "http://10.0.0.5:11434/v1", "http://localhost:11434/v1"):
    os.environ["BOT_CHAT_API_BASE"] = base
    check(f"a local endpoint is allowed: {base}", ask()["verdict"] == "yes")
os.environ["BOT_CHAT_API_BASE"] = "http://127.0.0.1:11434/v1"
r = pl.confirm_window("", provider="PP", start=S, end=E)
check("an empty message is never sent", r["verdict"] == "unclear" and not r["transient"])

print("-" * 78)
print(f"{'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILED'}   | real network attempts: {NET['n']}")
sys.exit(1 if FAILS or NET["n"] else 0)
