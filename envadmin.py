#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""/showenv, /addenv, /editenv - read and edit the bot's own .env from Lark.

  /showenv                 the whole .env file, as it is on disk
  /addenv KEY=value        append one new line (several lines = several keys)
  /editenv [text]          a card with one box per variable; Save overwrites
                           the file (optionally only keys containing <text>)

WHO: exactly one open_id, hard-coded on purpose (Jun Chen). There is no env
override: /editenv can rewrite the .env, so an allow-list read from the .env
would let the command widen its own gate. The Save button is gated again on the
operator of the click, since a card can be forwarded to anyone.

WHERE: the file content is a set of secrets (app secret, SMTP and Base tokens),
so /showenv and /editenv never post it into a group. In a group the cards go to
the admin's private chat with the bot and the group only hears "sent to your
private chat"; if that DM fails, nothing is posted anywhere.

SAFETY of a write:
  * only the VALUE part of existing KEY=... lines is edited; comments, blank
    lines, `export`, spacing and line endings are kept byte for byte;
  * each edit card carries a fingerprint of the values it showed; if the file
    changed since (another card saved, /addenv, a hand edit on the server) the
    save is refused instead of silently undoing that change;
  * the current file is copied to .env.bak-YYYYmmdd-HHMMSS first (newest 20
    kept; `.env.*` is git-ignored), then written atomically with its mode.

Nothing here reloads os.environ: every module reads its settings once at
start-up (and systemd reads the same file via EnvironmentFile), so a change
takes effect on the next /restart - the cards say so.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

ENV_PATH = Path(__file__).resolve().parent / ".env"

# The one person who may use these commands. Deliberately not configurable.
ADMIN_OPEN_IDS = frozenset({"ou_5f660c0fb0769d184aca635d02209272"})  # Jun Chen

COMMANDS = ("/showenv", "/addenv", "/editenv")
CARD_KEY = "envedit_save"

_BACKUP_PREFIX = ".env.bak-"
_BACKUPS_KEPT = 20
# Lark rejects a card over ~30 KB; stay well inside it.
_CARD_BYTES = 20000
_EDIT_PER_PAGE = 40
# Lark's input box holds at most 1000 characters.
_INPUT_MAX = 1000

_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# prefix = indent + optional `export ` + KEY + `=` + spaces; value = the rest.
_KV_RE = re.compile(r"^(\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_.\-]*)\s*=[ \t]*)(.*)$")
_CMD_RE = re.compile(r"^\s*(/showenv|/addenv|/editenv)(?![A-Za-z0-9_])(.*)$", re.I | re.S)
_BAD_CHARS = ("\r", "\n", "\x00")

_LOCK = threading.Lock()


class EnvFileError(RuntimeError):
    """The .env is missing or unreadable. Nothing is written after this."""


def is_admin(open_id: Optional[str]) -> bool:
    return (open_id or "").strip() in ADMIN_OPEN_IDS


# ---------------------------------------------------------------------------
# The file
# ---------------------------------------------------------------------------

@dataclass
class _Entry:
    idx: int          # line number (0-based) in _Doc.lines
    key: str
    occ: int          # 0 for the first KEY=, 1 for a second one, ...
    prefix: str
    value: str
    ending: str

    @property
    def ref(self) -> str:
        return f"{self.key}#{self.occ}"


@dataclass
class _Doc:
    bom: bytes
    lines: list = field(default_factory=list)      # each WITH its line ending
    entries: list = field(default_factory=list)

    def text(self) -> str:
        return "".join(self.lines)

    def newline(self) -> str:
        return "\r\n" if any(l.endswith("\r\n") for l in self.lines) else "\n"


def _split_ending(line: str) -> tuple:
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n"):
        return line[:-1], "\n"
    return line, ""


def _parse(text: str, bom: bytes = b"") -> _Doc:
    # Split on \n only: str.splitlines() also breaks on \x0b,   ... and
    # would cut a value that happens to contain one.
    lines = re.split(r"(?<=\n)", text)
    if lines and lines[-1] == "":
        lines.pop()
    doc = _Doc(bom=bom, lines=lines)
    seen: dict = {}
    for i, line in enumerate(lines):
        body, ending = _split_ending(line)
        m = _KV_RE.match(body)
        if not m or body.lstrip().startswith("#"):
            continue
        key = m.group(2)
        occ = seen.get(key, 0)
        seen[key] = occ + 1
        doc.entries.append(_Entry(i, key, occ, m.group(1), m.group(3), ending))
    return doc


def _read() -> _Doc:
    try:
        raw = ENV_PATH.read_bytes()
    except FileNotFoundError:
        raise EnvFileError(f"there is no .env at {ENV_PATH}")
    except Exception as err:  # noqa: BLE001
        raise EnvFileError(f"could not read {ENV_PATH}: {err!r}")
    bom = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
    try:
        text = raw[len(bom):].decode("utf-8")
    except UnicodeDecodeError as err:
        raise EnvFileError(f"{ENV_PATH.name} is not UTF-8 ({err}) - not touching it")
    return _parse(text, bom)


# Only names this module made are ever pruned - never a hand-made .env.bak-old.
# Fixed width, so name order is age order.
_BACKUP_NAME_RE = re.compile(r"^\.env\.bak-\d{8}-\d{6}-\d{3}(?:-\d{2})?$")


def _prune_backups() -> None:
    ours = sorted(p for p in ENV_PATH.parent.glob(_BACKUP_PREFIX + "*")
                  if _BACKUP_NAME_RE.match(p.name))
    for p in ours[:-_BACKUPS_KEPT]:
        try:
            p.unlink()
        except Exception:  # noqa: BLE001
            pass


def _write(doc: _Doc) -> str:
    """Back up the current file, then replace it atomically. -> backup name."""
    now = time.time()
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(now)) + f"-{int(now * 1000) % 1000:03d}"
    backup = ENV_PATH.with_name(_BACKUP_PREFIX + stamp)
    n = 1
    while backup.exists() and n < 100:
        backup = ENV_PATH.with_name(f"{_BACKUP_PREFIX}{stamp}-{n:02d}")
        n += 1
    if backup.exists():
        raise EnvFileError("could not pick a backup name - not writing")
    shutil.copy2(ENV_PATH, backup)
    tmp = ENV_PATH.with_name(".env.tmp-envadmin")
    tmp.write_bytes(doc.bom + doc.text().encode("utf-8"))
    try:
        shutil.copymode(ENV_PATH, tmp)
    except Exception:  # noqa: BLE001
        pass
    os.replace(tmp, ENV_PATH)
    _prune_backups()
    return backup.name


def _fingerprint(pairs: list) -> str:
    h = hashlib.sha256()
    for ref, value in pairs:
        h.update(ref.encode("utf-8") + b"\x1e" + value.encode("utf-8") + b"\x1f")
    return h.hexdigest()[:20]


def _has_bad_chars(s: str) -> bool:
    return any(c in s for c in _BAD_CHARS)


# ---------------------------------------------------------------------------
# /addenv
# ---------------------------------------------------------------------------

def add_lines(block: str) -> dict:
    """Append KEY=value lines. All-or-nothing: one bad line and none are added.

    -> {"ok": bool, "added": [keys], "error": str}
    """
    lines = [l.strip() for l in re.split(r"\r?\n", block or "") if l.strip()]
    if not lines:
        return {"ok": False, "added": [],
                "error": "Nothing to add. Usage: `/addenv KEY=value`"}
    pairs = []
    for line in lines:
        if "=" not in line:
            return {"ok": False, "added": [],
                    "error": f"`{line[:60]}` is not KEY=value."}
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if key.lower().startswith("export "):
            key = key[7:].strip()
        if not _KEY_RE.match(key):
            return {"ok": False, "added": [],
                    "error": f"`{key[:60]}` is not a valid variable name "
                             f"(letters, digits and _, not starting with a digit)."}
        if _has_bad_chars(value):
            return {"ok": False, "added": [], "error": f"{key}: the value has a line break."}
        pairs.append((key, value))
    names = [k for k, _ in pairs]
    dup_in_msg = sorted({k for k in names if names.count(k) > 1})
    if dup_in_msg:
        return {"ok": False, "added": [],
                "error": "Given twice in this message: " + ", ".join(dup_in_msg)}
    with _LOCK:
        doc = _read()
        existing = {e.key for e in doc.entries}
        already = [k for k in names if k in existing]
        if already:
            return {"ok": False, "added": [],
                    "error": "Already in .env: " + ", ".join(already)
                             + " - change it with `/editenv " + already[0] + "`."}
        nl = doc.newline()
        if doc.lines and not doc.lines[-1].endswith("\n"):
            doc.lines[-1] += nl
        for k, v in pairs:
            doc.lines.append(f"{k}={v}{nl}")
        backup = _write(doc)
    return {"ok": True, "added": names, "backup": backup, "error": ""}


# ---------------------------------------------------------------------------
# /editenv save
# ---------------------------------------------------------------------------

def apply_edits(refs: list, fingerprint: str, submitted: dict) -> dict:
    """Write the submitted values of one edit card.

    refs        the "KEY#occ" list the card was built from, in order
    fingerprint what those values were when the card was built
    submitted   {ref: new value} for the boxes Lark sent back

    -> {"status": saved|nochange|already|conflict|invalid, "changed": [keys],
        "backup": name, "error": str}
    """
    out = {"status": "conflict", "changed": [], "backup": "", "error": ""}
    bad = sorted(r.split("#")[0] for r, v in submitted.items() if _has_bad_chars(v))
    if bad:
        out.update(status="invalid", error="Line break in: " + ", ".join(bad))
        return out
    with _LOCK:
        doc = _read()
        by_ref = {e.ref: e for e in doc.entries}
        gone = [r.split("#")[0] for r in refs if r not in by_ref]
        if gone:
            out["error"] = "No longer in .env: " + ", ".join(gone)
            return out
        now = _fingerprint([(r, by_ref[r].value) for r in refs])
        if now != fingerprint:
            # A redelivered or double-tapped Save finds its own values already on
            # disk: that is success, not a conflict.
            if submitted and all(by_ref[r].value == v for r, v in submitted.items()
                                 if r in by_ref):
                out["status"] = "already"
                return out
            out["error"] = "The .env changed after this card was made."
            return out
        changed = [r for r in refs if r in submitted and by_ref[r].value != submitted[r]]
        if not changed:
            out["status"] = "nochange"
            return out
        for r in changed:
            e = by_ref[r]
            doc.lines[e.idx] = e.prefix + submitted[r] + e.ending
        out["backup"] = _write(doc)
    out["status"] = "saved"
    out["changed"] = [r.split("#")[0] for r in changed]
    return out


# ---------------------------------------------------------------------------
# Cards
# ---------------------------------------------------------------------------

def _card(title: str, template: str, elements: list) -> dict:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": template,
                   "title": {"tag": "plain_text", "content": title[:100]}},
        "body": {"elements": elements},
    }


def _plain(text: str) -> dict:
    return {"tag": "div", "text": {"tag": "plain_text", "content": text}}


def _md(text: str) -> dict:
    return {"tag": "markdown", "content": text}


def _size(card: dict) -> int:
    return len(json.dumps(card, ensure_ascii=False).encode("utf-8"))


def _code(text: str) -> str:
    # A literal ``` inside the file would close the block early.
    return "```\n" + text.replace("```", "ˋˋˋ") + "\n```"


def build_show_cards(doc: _Doc) -> list:
    """The whole file, split over as many cards as the size limit needs."""
    text = doc.text()
    n_vars = len(doc.entries)
    foot = (f"{n_vars} variables · {ENV_PATH} · changes apply after /restart")
    if not text.strip():
        return [_card("📄 .env", "grey", [_plain("The .env file is empty."), _plain(foot)])]
    chunks, cur = [], ""
    for line in doc.lines:
        if cur and _size(_card("x", "blue", [_md(_code(cur + line)), _plain(foot)])) > _CARD_BYTES:
            chunks.append(cur)
            cur = ""
        cur += line
    if cur:
        chunks.append(cur)
    total = len(chunks)
    cards = []
    for i, chunk in enumerate(chunks, 1):
        title = "📄 .env" + (f" ({i}/{total})" if total > 1 else "")
        cards.append(_card(title, "blue", [_md(_code(chunk.rstrip("\r\n"))), _plain(foot)]))
    return cards


def _edit_card(page: list, i: int, total: int, note: str) -> dict:
    title = "✏️ Edit .env" + (f" ({i}/{total})" if total > 1 else "")
    form_elems: list = []
    refs = []
    for e in page:
        label = e.key if e.occ == 0 else f"{e.key}  (repeat #{e.occ + 1})"
        form_elems.append(_plain(label))
        box = {"tag": "input", "name": f"e{len(refs)}", "width": "fill",
               "max_length": _INPUT_MAX,
               "placeholder": {"tag": "plain_text", "content": "(empty)"}}
        if e.value:
            box["default_value"] = e.value
        form_elems.append(box)
        refs.append(e.ref)
    form_elems.append({
        "tag": "button",
        "name": "envedit_save_btn",
        "text": {"tag": "plain_text", "content": "Save to .env"},
        "type": "primary",
        "form_action_type": "submit",
        "behaviors": [{"type": "callback", "value": {
            "k": CARD_KEY,
            "h": _fingerprint([(e.ref, e.value) for e in page]),
            "r": ",".join(refs),
        }}],
    })
    return _card(title, "orange", [
        _plain(note),
        {"tag": "form", "name": "envedit_form", "elements": form_elems},
    ])


def build_edit_cards(doc: _Doc, match: str = "") -> tuple:
    """-> (cards, skipped_too_long_keys). Each card saves only its own boxes."""
    needle = (match or "").strip().lower()
    wanted = [e for e in doc.entries if not needle or needle in e.key.lower()]
    long_keys = [e.key for e in wanted if len(e.value) > _INPUT_MAX]
    editable = [e for e in wanted if len(e.value) <= _INPUT_MAX]
    note = ("Change a value and press Save. Comments and blank lines are kept "
            "as they are; the old file is backed up first. To add a new variable "
            "use /addenv KEY=value. Changes apply after /restart.")
    if long_keys:
        note += " Too long to edit here, left as they are: " + ", ".join(long_keys) + "."
    pages, cur = [], []
    for e in editable:
        if cur and (len(cur) >= _EDIT_PER_PAGE
                    or _size(_edit_card(cur + [e], 1, 1, note)) > _CARD_BYTES):
            pages.append(cur)
            cur = []
        cur.append(e)
    if cur:
        pages.append(cur)
    total = len(pages)
    return [_edit_card(p, i, total, note) for i, p in enumerate(pages, 1)], long_keys


def _saved_card(res: dict) -> dict:
    keys = res.get("changed") or []
    return _card("✅ .env saved", "green", [
        _plain(f"{len(keys)} changed: " + ", ".join(keys)),
        _plain(f"Backup of the previous file: {res.get('backup')}"),
        _plain("Send /restart to apply."),
    ])


# ---------------------------------------------------------------------------
# Lark entry points
# ---------------------------------------------------------------------------

def match_command(text: str) -> Optional[tuple]:
    """-> (command, rest) when ``text`` is one of ours, after mention keys."""
    s = re.sub(r"@_(?:user_\d+|all)\b", "", text or "")
    m = _CMD_RE.match(s)
    if not m:
        return None
    return m.group(1).lower(), m.group(2)


def _ok(resp) -> bool:
    return isinstance(resp, dict) and resp.get("code") == 0


def _deliver_private(cards: list, *, chat_id: str, chat_type: str, sender_id: str,
                     send_message: Callable, what: str) -> None:
    """Send secret-bearing cards only to the admin: in place in a p2p chat, by
    open_id DM from a group. Never into a group."""
    private = (chat_type or "") == "p2p"
    target = chat_id if private else sender_id
    rtype = "chat_id" if private else "open_id"
    fails = []
    for c in cards:
        body = json.dumps(c, ensure_ascii=False)
        r = (send_message(target, body, msg_type="interactive")
             if private else
             send_message(target, body, msg_type="interactive",
                          receive_id_type=rtype, reply_to_message_id=""))
        if not _ok(r):
            code = r.get("code") if isinstance(r, dict) else "?"
            fails.append(f"{code} {str((r or {}).get('msg') or '')[:80]}".strip())
            print(f"[envadmin] {what} card send failed: {r!r}", flush=True)
            break
    if private:
        if fails:
            send_message(chat_id, f"❌ Could not send the {what} card (Lark {fails[0]}).")
        return
    if fails:
        send_message(chat_id, f"⚠️ Could not DM you the {what} card (Lark {fails[0]}). "
                              f"Open a private chat with Duty Bot and send the command there.")
    else:
        n = len(cards)
        send_message(chat_id, f"📬 Sent the {what} to your private chat with the bot"
                              + (f" ({n} cards)." if n > 1 else "."))


def handle_command(text: str, *, sender_id: str, chat_id: str, chat_type: str,
                   send_message: Callable) -> bool:
    """Run /showenv, /addenv or /editenv. -> True when ``text`` was one of them
    (whatever the outcome), so the caller stops routing it."""
    got = match_command(text)
    if not got:
        return False
    cmd, rest = got
    if not is_admin(sender_id):
        print(f"⛔ {cmd} denied for sender={sender_id!r} chat={chat_id!r}", flush=True)
        send_message(chat_id, f"🚫 `{cmd}` is restricted to the bot admin.")
        return True
    try:
        if cmd == "/addenv":
            res = add_lines(rest)
            if res["ok"]:
                send_message(chat_id, "✅ Added to .env: " + ", ".join(res["added"])
                             + f"\nBackup: {res['backup']} · send /restart to apply.")
            else:
                send_message(chat_id, "❌ " + res["error"])
            return True
        doc = _read()
        if cmd == "/showenv":
            _deliver_private(build_show_cards(doc), chat_id=chat_id, chat_type=chat_type,
                             sender_id=sender_id, send_message=send_message, what=".env")
            return True
        cards, _long = build_edit_cards(doc, rest)
        if not cards:
            send_message(chat_id, "No variable in .env matches that."
                         if rest.strip() else "The .env has no variables to edit.")
            return True
        _deliver_private(cards, chat_id=chat_id, chat_type=chat_type,
                         sender_id=sender_id, send_message=send_message, what=".env editor")
    except EnvFileError as err:
        send_message(chat_id, f"❌ {err}")
    return True


def _form_values(parsed: dict, ev: dict) -> dict:
    act = ev.get("action") if isinstance(ev.get("action"), dict) else {}
    fv = act.get("form_value")
    if not isinstance(fv, dict):
        fv = parsed.get("form_value")
    return fv if isinstance(fv, dict) else {}


def handle_card_callback(parsed: dict, ev: dict, chat_id: str) -> Optional[dict]:
    """Save on an /editenv card. -> None when the click is not ours."""
    if str(parsed.get("k") or "").strip().lower() != CARD_KEY:
        return None
    op = ev.get("operator") if isinstance(ev.get("operator"), dict) else {}
    oid = str(op.get("open_id") or "").strip()
    if not is_admin(oid):
        print(f"⛔ envedit save denied for operator={oid!r} chat={chat_id!r}", flush=True)
        return {"toast": {"type": "error", "content": "Only the bot admin can save the .env."}}
    refs = [r for r in str(parsed.get("r") or "").split(",") if r]
    fp = str(parsed.get("h") or "")
    if not refs or not fp:
        return {"toast": {"type": "error", "content": "This card is missing its data - run /editenv again."}}
    fv = _form_values(parsed, ev)
    submitted = {}
    for i, ref in enumerate(refs):
        v = fv.get(f"e{i}")
        if isinstance(v, str):
            submitted[ref] = v
    try:
        res = apply_edits(refs, fp, submitted)
    except EnvFileError as err:
        return {"toast": {"type": "error", "content": str(err)[:200]}}
    except Exception as err:  # noqa: BLE001
        print(f"[envadmin] save failed: {err!r}", flush=True)
        return {"toast": {"type": "error", "content": f"Save failed: {err!r}"[:200]}}
    st = res["status"]
    print(f"[envadmin] save by {oid}: {st} {res.get('changed')} {res.get('error')}", flush=True)
    if st == "saved":
        return {"toast": {"type": "success", "content": "Saved - send /restart to apply."},
                "card": {"type": "raw", "data": _saved_card(res)}}
    if st == "already":
        return {"toast": {"type": "info", "content": "Already saved."}}
    if st == "nochange":
        return {"toast": {"type": "info", "content": "Nothing changed."}}
    if st == "invalid":
        return {"toast": {"type": "error", "content": res["error"][:200]}}
    return {"toast": {"type": "error",
                      "content": (res["error"] + " Run /editenv again.")[:200]}}
