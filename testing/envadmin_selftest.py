#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""envadmin (/showenv, /addenv, /editenv) - offline self-test.

  python testing/envadmin_selftest.py        exit 1 on any failure

Never touches the real .env (ENV_PATH points at a temp dir) and never talks to
Lark: send_message is a recorder, and requests is blocked outright.
"""
import io
import json
import os
import pathlib
import re
import stat
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import requests  # noqa: E402


def _no_net(*_a, **_k):
    raise RuntimeError("envadmin_selftest: network blocked")


requests.get = requests.post = requests.put = requests.patch = _no_net

import envadmin as ea  # noqa: E402

ADMIN = "ou_5f660c0fb0769d184aca635d02209272"
OTHER = "ou_someoneelse000000000000000000000"
GROUP = "oc_group0000000000000000000000000"
P2P = "oc_p2p00000000000000000000000000000"

FAILS = []


def check(name, cond, detail=""):
    print(("PASS  " if cond else "FAIL  ") + name + ("" if cond else f"   -> {detail}"))
    if not cond:
        FAILS.append(name)


SAMPLE = (
    "# ---- Lark ----\n"
    "APP_ID=cli_abc\n"
    "APP_SECRET = s3cr3t-value   # inline note\n"
    "export SMTP_PASS=\"p@ss word\"\n"
    "\n"
    "#COMMENTED=out\n"
    "EMPTY=\n"
    "VAWATCH_CHAT=🆕VA 公告｜VA announcements\n"
    "URL=https://x.example/a?b=c&d=e\n"
    "APP_ID=second_copy\n"
)


class Box:
    def __init__(self, fail_open_id=False):
        self.sent = []          # (target, rtype, msg_type, text)
        self.fail_open_id = fail_open_id

    def __call__(self, target, text, msg_type="text", mentions=None,
                 receive_id_type="chat_id", reply_to_message_id=None):
        self.sent.append((target, receive_id_type, msg_type, text))
        if receive_id_type == "open_id" and self.fail_open_id:
            return {"code": 230013, "msg": "bot has no availability to this user"}
        return {"code": 0, "data": {"message_id": "om_x"}}


def fresh(content=SAMPLE, raw=None):
    d = pathlib.Path(tempfile.mkdtemp())
    ea.ENV_PATH = d / ".env"
    ea.ENV_PATH.write_bytes(raw if raw is not None else content.encode("utf-8"))
    return d


def env_bytes():
    return ea.ENV_PATH.read_bytes()


def run(text, sender=ADMIN, chat=P2P, chat_type="p2p", box=None):
    box = box or Box()
    handled = ea.handle_command(text, sender_id=sender, chat_id=chat,
                                chat_type=chat_type, send_message=box)
    return handled, box


def edit_cards(match=""):
    cards, _ = ea.build_edit_cards(ea._read(), match)
    return cards


def card_button(card):
    form = [e for e in card["body"]["elements"] if e.get("tag") == "form"][0]
    btn = [e for e in form["elements"] if e.get("tag") == "button"][0]
    return form, btn["behaviors"][0]["value"]


def click(card, changes, operator=ADMIN, via_parsed=False):
    """Simulate Save: form_value = default values with ``changes`` applied."""
    form, value = card_button(card)
    refs = value["r"].split(",")
    inputs = [e for e in form["elements"] if e.get("tag") == "input"]
    fv = {}
    for i, box in enumerate(inputs):
        key = refs[i].split("#")[0]
        v = box.get("default_value", "")
        if refs[i] in changes:
            v = changes[refs[i]]
        elif key in changes:
            v = changes[key]
        fv[box["name"]] = v
    ev = {"operator": {"open_id": operator}, "action": {"value": value}}
    parsed = dict(value)
    if via_parsed:
        parsed["form_value"] = fv
    else:
        ev["action"]["form_value"] = fv
    return ea.handle_card_callback(parsed, ev, P2P)


def secret_leaked_to(box, target):
    for tgt, _rt, _mt, text in box.sent:
        if tgt == target and ("s3cr3t" in text or "p@ss" in text or "cli_abc" in text):
            return True
    return False


# ---------------------------------------------------------------- matching
check("match: plain", ea.match_command("/showenv") == ("/showenv", ""))
check("match: mention key stripped", ea.match_command("@_user_1 /addenv A=1") == ("/addenv", " A=1"))
check("match: case-insensitive", (ea.match_command("/EditEnv VA") or ("",))[0] == "/editenv")
check("match: longer word is not ours", ea.match_command("/editenvx") is None)
check("match: other command", ea.match_command("/log 6h") is None)
MAIN_RE = r"(?i)^/(?:showenv|addenv|editenv)(?![a-z0-9_])"
src = (ROOT / "main.py").read_text(encoding="utf-8")
check("main.py routes with the same pattern", MAIN_RE in src)
for t in ("/showenv", "/addenv A=1", "/editenv VA", "/EDITENV"):
    check(f"main regex accepts {t!r}", bool(re.match(MAIN_RE, t)) and ea.match_command(t) is not None)
for t in ("/showenvx", "/addenvironment", "showenv"):
    check(f"main regex rejects {t!r}", not re.match(MAIN_RE, t) and ea.match_command(t) is None)
check("main.py denies the three from buttons",
      all(f'"{c}"' in src.split("_denied = {", 1)[1].split("}", 1)[0] for c in ea.COMMANDS))
check("main.py wires the card callback", "handle_card_callback(" in src and "import envadmin as _envadmin_cb" in src)
check("main.py redacts /addenv in the logs", src.count("<redacted /addenv>") == 3)
check("env branch sits before the AI-thinking handler",
      src.index("import envadmin as _envadmin\n") < src.index("import aithinking as _aithinking"))
check("env branch sits after the group @mention gate",
      src.index('print("⏭️ Bot not mentioned in group chat') < src.index("import envadmin as _envadmin\n"))

# ---------------------------------------------------------------- gating
fresh()
before = env_bytes()
for cmd in ("/showenv", "/addenv NEW=1", "/editenv"):
    h, box = run(cmd, sender=OTHER, chat=GROUP, chat_type="group")
    check(f"non-admin {cmd}: handled", h)
    check(f"non-admin {cmd}: one refusal, no card",
          len(box.sent) == 1 and box.sent[0][2] == "text" and "restricted" in box.sent[0][3], box.sent)
    h, box = run(cmd, sender="", chat=P2P)
    check(f"no sender {cmd}: refused", "restricted" in box.sent[0][3])
check("non-admin: file untouched", env_bytes() == before)
check("admin set is exactly one id", ea.ADMIN_OPEN_IDS == frozenset({ADMIN}))
check("admin cannot be widened from the env",
      "getenv" not in pathlib.Path(ea.__file__).read_text(encoding="utf-8"))

# ---------------------------------------------------------------- /showenv
fresh()
h, box = run("/showenv", chat=P2P, chat_type="p2p")
check("showenv p2p: one interactive card to the p2p chat",
      len(box.sent) == 1 and box.sent[0][0] == P2P and box.sent[0][2] == "interactive", box.sent)
card = json.loads(box.sent[0][3])
md = card["body"]["elements"][0]["content"]
check("showenv: whole file shown verbatim", SAMPLE.rstrip("\n") in md, md[:200])
check("showenv: schema 2.0", card.get("schema") == "2.0")

h, box = run("/showenv", chat=GROUP, chat_type="group")
check("showenv group: card goes to the admin by open_id",
      box.sent[0][0] == ADMIN and box.sent[0][1] == "open_id" and box.sent[0][2] == "interactive", box.sent[:1])
check("showenv group: group only gets a pointer", box.sent[-1][0] == GROUP and "private chat" in box.sent[-1][3])
check("showenv group: nothing secret posted to the group", not secret_leaked_to(box, GROUP))

h, box = run("/showenv", chat=GROUP, chat_type="group", box=Box(fail_open_id=True))
check("showenv group, DM fails: warns in group", "Could not DM" in box.sent[-1][3], box.sent)
check("showenv group, DM fails: still nothing secret in the group", not secret_leaked_to(box, GROUP))
check("showenv group, DM fails: no card posted to the group",
      not any(t == GROUP and mt == "interactive" for t, _r, mt, _x in box.sent))

big = "".join(f"KEY_{i:04d}={'v' * 180}\n" for i in range(400))
fresh(big)
h, box = run("/showenv")
cards = [json.loads(t) for _g, _r, mt, t in box.sent if mt == "interactive"]
check("showenv big file: split over several cards", len(cards) > 1, len(cards))
check("showenv big file: every card under the size limit",
      all(len(json.dumps(c, ensure_ascii=False).encode()) <= ea._CARD_BYTES + 400 for c in cards))
joined = "".join(re.sub(r"^```\n|\n```$", "", c["body"]["elements"][0]["content"]) + "\n" for c in cards)
check("showenv big file: nothing lost across cards", joined == big, (len(joined), len(big)))

fresh(raw=b"")
h, box = run("/showenv")
check("showenv empty file: says so", "empty" in box.sent[0][3])
ea.ENV_PATH = pathlib.Path(tempfile.mkdtemp()) / ".env"
h, box = run("/showenv")
check("showenv missing file: error text, no card", box.sent[0][2] == "text" and "no .env" in box.sent[0][3])
h, box = run("/addenv A=1")
check("addenv missing file: refuses, creates nothing", not ea.ENV_PATH.exists() and "no .env" in box.sent[0][3])

# ---------------------------------------------------------------- /addenv
d = fresh()
h, box = run("/addenv NEW_KEY=hello=world  ")
after = env_bytes().decode()
check("addenv: appended as the last line", after == SAMPLE + "NEW_KEY=hello=world\n", repr(after[-60:]))
check("addenv: reply names the key", "NEW_KEY" in box.sent[0][3] and "hello" not in box.sent[0][3])
check("addenv: backup of the previous file", len(list(d.glob(".env.bak-*"))) == 1
      and next(d.glob(".env.bak-*")).read_bytes() == SAMPLE.encode())

h, box = run("/addenv APP_SECRET=x")
check("addenv: existing key refused", "Already in .env: APP_SECRET" in box.sent[0][3] and env_bytes().decode() == after)
h, box = run("/addenv 9BAD=x")
check("addenv: bad name refused", "not a valid variable name" in box.sent[0][3])
h, box = run("/addenv NOEQUALS")
check("addenv: missing = refused", "not KEY=value" in box.sent[0][3])
h, box = run("/addenv")
check("addenv: empty refused", "Usage" in box.sent[0][3])
h, box = run("/addenv A1=1\nA2=2\nA1=3")
check("addenv: duplicate within the message refused", "Given twice" in box.sent[0][3] and env_bytes().decode() == after)
h, box = run("/addenv OK1=1\n9BAD=2")
check("addenv: all-or-nothing", "OK1" not in env_bytes().decode())
h, box = run("/addenv M1=one\nexport M2 = two")
check("addenv: several lines at once", env_bytes().decode().endswith("M1=one\nM2=two\n"))
h, box = run("@_user_1 /addenv G1=grp", chat=GROUP, chat_type="group")
check("addenv from a group works (mention stripped)", env_bytes().decode().endswith("G1=grp\n"))

fresh("A=1")                                 # no trailing newline
run("/addenv B=2")
check("addenv: file without final newline", env_bytes() == b"A=1\nB=2\n", env_bytes())
fresh(raw=b"A=1\r\nB=2\r\n")                 # CRLF file
run("/addenv C=3")
check("addenv: keeps CRLF", env_bytes() == b"A=1\r\nB=2\r\nC=3\r\n", env_bytes())
fresh(raw=b"\xef\xbb\xbfA=1\n")              # BOM
run("/addenv C=3")
check("addenv: keeps a BOM", env_bytes() == b"\xef\xbb\xbfA=1\nC=3\n", env_bytes())
check("addenv: BOM does not hide the first key", [e.key for e in ea._read().entries] == ["A", "C"])
fresh(raw=b"A=\xff\xfe\n")
h, box = run("/addenv C=3")
check("addenv: non-UTF-8 file refused untouched", "not UTF-8" in box.sent[0][3] and env_bytes() == b"A=\xff\xfe\n")

if os.name != "nt":
    fresh()
    os.chmod(ea.ENV_PATH, 0o600)
    run("/addenv Z=1")
    check("addenv: file mode kept", stat.S_IMODE(os.stat(ea.ENV_PATH).st_mode) == 0o600)

d = fresh()
(d / ".env.bak-handmade").write_text("keep me", encoding="utf-8")
for i in range(ea._BACKUPS_KEPT + 15):
    run(f"/addenv BK{i}=1")
ours = sorted(p.name for p in d.glob(".env.bak-*") if ea._BACKUP_NAME_RE.match(p.name))
check("backups pruned to the newest 20", len(ours) == ea._BACKUPS_KEPT, len(ours))
# 35 adds (BK0..BK34) -> 35 backups; the one taken before add i holds BK0..BK(i-1).
newest, oldest = (d / ours[-1]).read_bytes(), (d / ours[0]).read_bytes()
check("the newest backup is the file just before the last add",
      b"BK33=1" in newest and b"BK34=1" not in newest)
check("the oldest kept backup is the 20th newest (older ones pruned)",
      b"BK14=1" in oldest and b"BK15=1" not in oldest)
check("a hand-made .env.bak-* is never pruned", (d / ".env.bak-handmade").exists())
check("no temp file left behind", not (d / ".env.tmp-envadmin").exists())

# ---------------------------------------------------------------- /editenv
fresh()
h, box = run("/editenv", chat=GROUP, chat_type="group")
check("editenv group: card to the admin by open_id", box.sent[0][1] == "open_id" and box.sent[0][0] == ADMIN)
check("editenv group: nothing secret in the group", not secret_leaked_to(box, GROUP))
card = json.loads(box.sent[0][3])
form, value = card_button(card)
inputs = [e for e in form["elements"] if e.get("tag") == "input"]
refs = value["r"].split(",")
check("editenv: one box per variable, incl. the repeat",
      refs == ["APP_ID#0", "APP_SECRET#0", "SMTP_PASS#0", "EMPTY#0", "VAWATCH_CHAT#0", "URL#0", "APP_ID#1"], refs)
check("editenv: commented line is not a variable", "COMMENTED#0" not in refs)
check("editenv: boxes hold the raw values",
      [b.get("default_value", "") for b in inputs] ==
      ["cli_abc", "s3cr3t-value   # inline note", "\"p@ss word\"", "", "🆕VA 公告｜VA announcements",
       "https://x.example/a?b=c&d=e", "second_copy"], [b.get("default_value") for b in inputs])
check("editenv: empty value has no default_value key", "default_value" not in inputs[3])
check("editenv: button value is all strings", all(isinstance(v, str) for v in value.values()))
check("editenv: inputs capped at Lark's 1000", all(b["max_length"] == 1000 for b in inputs))
check("editenv: submit button", [e for e in form["elements"] if e.get("tag") == "button"][0]
      .get("form_action_type") == "submit")

# save: two changes, everything else byte-identical
fresh()
card = edit_cards()[0]
r = click(card, {"APP_SECRET#0": "new-secret", "APP_ID#1": "third"})
expect = SAMPLE.replace("APP_SECRET = s3cr3t-value   # inline note", "APP_SECRET = new-secret") \
               .replace("APP_ID=second_copy", "APP_ID=third")
check("save: status saved", r.get("toast", {}).get("type") == "success", r)
check("save: only the edited values changed, spacing kept", env_bytes().decode() == expect,
      env_bytes().decode())
check("save: card replaced by a summary without values",
      r.get("card", {}).get("type") == "raw" and "new-secret" not in json.dumps(r["card"])
      and "APP_SECRET" in json.dumps(r["card"]))
check("save: backup kept", len(list(ea.ENV_PATH.parent.glob(".env.bak-*"))) == 1)

r2 = click(card, {"APP_SECRET#0": "new-secret", "APP_ID#1": "third"})
check("save redelivered: 'Already saved', no second write",
      r2.get("toast", {}).get("content") == "Already saved." and env_bytes().decode() == expect, r2)
r3 = click(card, {"APP_SECRET#0": "other"})
check("save from a stale card: refused", "changed after this card" in r3["toast"]["content"]
      and env_bytes().decode() == expect, r3)

fresh()
card = edit_cards()[0]
r = click(card, {})
check("save with nothing changed: no write", r["toast"]["content"] == "Nothing changed."
      and env_bytes() == SAMPLE.encode() and not list(ea.ENV_PATH.parent.glob(".env.bak-*")))
r = click(card, {"URL": "bad\nINJECT=1"})
check("save with a line break: refused", r["toast"]["type"] == "error" and env_bytes() == SAMPLE.encode(), r)
r = click(card, {"URL": "x"}, operator=OTHER)
check("save by someone else: refused", "Only the bot admin" in r["toast"]["content"] and env_bytes() == SAMPLE.encode())
r = click(card, {"URL": "x"}, operator="")
check("save with no operator: refused", "Only the bot admin" in r["toast"]["content"])
r = click(card, {"URL": "https://y.example"}, via_parsed=True)
check("save: form_value read from the parsed value too",
      r["toast"]["type"] == "success" and "URL=https://y.example\n" in env_bytes().decode(), r)

fresh()
card = edit_cards()[0]
run("/addenv LATER=1")          # appending does not touch the card's values
r = click(card, {"EMPTY": "filled"})
check("save after an /addenv elsewhere: still allowed",
      r["toast"]["type"] == "success" and "EMPTY=filled\n" in env_bytes().decode() and "LATER=1" in env_bytes().decode(), r)

fresh()
card = edit_cards()[0]
ea.ENV_PATH.write_bytes(SAMPLE.replace("EMPTY=\n", "").encode())
r = click(card, {"URL": "x"})
check("save after a key was deleted by hand: refused", "No longer in .env: EMPTY" in r["toast"]["content"], r)

ev = {"operator": {"open_id": ADMIN}, "action": {}}
check("callback not ours -> None", ea.handle_card_callback({"k": "evom_gen"}, ev, P2P) is None)
r = ea.handle_card_callback({"k": ea.CARD_KEY}, ev, P2P)
check("callback without data: error toast", r["toast"]["type"] == "error")

# filter, too-long values, paging
fresh(SAMPLE + "LONG=" + "x" * 1500 + "\n")
cards, long_keys = ea.build_edit_cards(ea._read(), "")
check("value over 1000 chars: not editable, named in the note",
      long_keys == ["LONG"] and "LONG" in cards[0]["body"]["elements"][0]["text"]["content"]
      and "LONG#0" not in card_button(cards[0])[1]["r"])
cards = edit_cards("app_")
check("filter: only matching keys", card_button(cards[0])[1]["r"] == "APP_ID#0,APP_SECRET#0,APP_ID#1",
      card_button(cards[0])[1]["r"])
h, box = run("/editenv NOPE_NOTHING")
check("filter with no match: says so", "No variable" in box.sent[0][3])

fresh("".join(f"K{i:03d}={'v' * 120}\n" for i in range(130)))
cards = edit_cards()
refs_all = sum((card_button(c)[1]["r"].split(",") for c in cards), [])
check("paging: 130 variables over several cards", len(cards) >= 4, len(cards))
check("paging: at most 40 boxes per card", all(len(card_button(c)[1]["r"].split(",")) <= 40 for c in cards))
check("paging: each card under the size limit",
      all(len(json.dumps(c, ensure_ascii=False).encode()) <= ea._CARD_BYTES for c in cards))
check("paging: every variable exactly once", refs_all == [f"K{i:03d}#0" for i in range(130)])
r = click(cards[1], {"K045": "changed"})
r2 = click(cards[0], {"K001": "also"})
check("paging: saving one page does not block another",
      r["toast"]["type"] == "success" and r2["toast"]["type"] == "success"
      and "K045=changed\n" in env_bytes().decode() and "K001=also\n" in env_bytes().decode(), (r, r2))

print("-" * 70)
print(f"{'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILED'}   | Lark sends: recorder only, network blocked")
sys.exit(1 if FAILS else 0)
