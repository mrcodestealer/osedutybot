#!/usr/bin/env python3
"""Parse and queue ``/updatemore`` multi-segment Jenkins update flows."""

from __future__ import annotations

import re
import threading
from typing import Any, Callable

UPDATEMORE_CMD_RE = re.compile(r"/updatemore\b", re.I)
_SAME_MARKER = "same"
_NOT_SAME_MARKERS = frozenset({"not same", "notsame"})
_SEGMENT_MARKERS = frozenset({_SAME_MARKER, *_NOT_SAME_MARKERS})


def parse_email_subject_from_line(line: str) -> str | None:
    """
    ``Email: (reply email): Livechat v1.0.27 …`` or ``Email:Livechat …``
    Uses the substring after the **rightmost** ``:`` on the line.
    """
    raw = (line or "").strip()
    if not re.match(r"email\b", raw, re.I):
        return None
    if ":" not in raw:
        return None
    subject = raw.rsplit(":", 1)[-1].strip()
    return subject or None


def parse_email_from_update_body(body: str) -> str | None:
    """Extract the first ``Email:`` subject from any ``/update`` message body."""
    for line in (body or "").replace("\r\n", "\n").split("\n"):
        em = parse_email_subject_from_line(line)
        if em:
            return em
    return None


def normalize_email_key(title: str) -> str:
    return re.sub(r"\s+", " ", (title or "").strip().casefold())


def assign_email_batches(segments: list[dict[str, Any]]) -> None:
    """
    Consecutive ``same`` segments that **each** have the same explicit ``Email:`` title
    share one batch → one combined reply after all complete.

    No inheritance: ``same`` without ``Email:`` breaks the batch chain.
    """
    batch_counter = 0
    i = 0
    while i < len(segments):
        email = (segments[i].get("email_subject") or "").strip()
        if not email:
            i += 1
            continue
        indices = [i]
        j = i + 1
        while j < len(segments) and segments[j].get("same_as_prev"):
            nxt_email = (segments[j].get("email_subject") or "").strip()
            if not nxt_email:
                break
            if normalize_email_key(nxt_email) != normalize_email_key(email):
                break
            indices.append(j)
            j += 1
        bid = batch_counter
        batch_counter += 1
        for idx in indices:
            segments[idx]["email_batch_id"] = bid
            segments[idx]["email_batch_indices"] = list(indices)
            segments[idx]["email_batch_title"] = email
        i = j if len(indices) > 1 else i + 1


def build_email_batch_state(segments: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    """Runtime batch tracker (only batches with 2+ segments)."""
    batches: dict[int, dict[str, Any]] = {}
    seen: set[int] = set()
    for seg in segments:
        if not isinstance(seg, dict):
            continue
        bid = seg.get("email_batch_id")
        indices = seg.get("email_batch_indices")
        if bid is None or bid in seen or not isinstance(indices, list) or len(indices) < 2:
            continue
        seen.add(int(bid))
        batches[int(bid)] = {
            "title": (seg.get("email_batch_title") or seg.get("email_subject") or "").strip(),
            "indices": list(indices),
            "done_by_idx": {},
        }
    return batches


def _normalize_lines(body: str) -> list[str]:
    return [ln.rstrip() for ln in (body or "").replace("\r\n", "\n").split("\n")]


def _is_segment_marker(line: str) -> bool:
    return (line or "").strip().casefold() in _SEGMENT_MARKERS


def parse_updatemore_body(body: str) -> list[dict[str, Any]]:
    """
    Parse ``/updatemore`` message into ordered segments.

    Each segment dict:
      - ``env_line`` — keyword line (e.g. ``update fpms uat``)
      - ``lines`` — branch/version/services config lines
      - ``email_subject`` — only when this segment has an explicit ``Email:`` line
      - ``same_as_prev`` — True when preceded by a ``same`` marker
    """
    lines = _normalize_lines(body)
    while lines and not lines[0].strip():
        lines.pop(0)
    if not lines or not UPDATEMORE_CMD_RE.search(lines[0]):
        raise ValueError("First line must include `/updatemore`.")
    lines = lines[1:]
    while lines and not lines[0].strip():
        lines.pop(0)
    if not lines:
        raise ValueError("No update block after `/updatemore`.")

    segments: list[dict[str, Any]] = []

    def consume_config(start: int, env: str) -> tuple[list[str], str | None, int]:
        cfg: list[str] = []
        email_subject: str | None = None
        j = start
        while j < len(lines):
            ln = lines[j].strip()
            if _is_segment_marker(ln):
                break
            em = parse_email_subject_from_line(lines[j])
            if em:
                email_subject = em
            else:
                cfg.append(lines[j])
            j += 1
        return cfg, email_subject, j

    env_line = lines[0].strip()
    if not env_line:
        raise ValueError("First segment needs an environment keyword line.")
    i = 1
    cfg, email, i = consume_config(i, env_line)
    segments.append(
        {
            "env_line": env_line,
            "lines": cfg,
            "email_subject": email,
            "same_as_prev": False,
        }
    )

    while i < len(lines):
        marker = lines[i].strip().casefold()
        i += 1
        if marker == _SAME_MARKER:
            if not segments:
                raise ValueError("`same` before any segment.")
            env = segments[-1]["env_line"]
            cfg, email, i = consume_config(i, env)
            segments.append(
                {
                    "env_line": env,
                    "lines": cfg,
                    "email_subject": email,
                    "same_as_prev": True,
                }
            )
        elif marker in _NOT_SAME_MARKERS:
            if i >= len(lines):
                raise ValueError("`not same` must be followed by an environment line.")
            env_line = lines[i].strip()
            if not env_line:
                raise ValueError("Environment line after `not same` is empty.")
            i += 1
            cfg, email, i = consume_config(i, env_line)
            segments.append(
                {
                    "env_line": env_line,
                    "lines": cfg,
                    "email_subject": email,
                    "same_as_prev": False,
                }
            )
        else:
            raise ValueError(f"Expected `same` or `not same`, got: {lines[i - 1]!r}")

    assign_email_batches(segments)
    return segments


def segment_to_update_body(segment: dict[str, Any]) -> str:
    """Build a single ``/update`` message body for one queue segment."""
    parts = [f"/update {segment['env_line']}"]
    parts.extend(segment.get("lines") or [])
    email = (segment.get("email_subject") or "").strip()
    if email:
        parts.append(f"Email: {email}")
    return "\n".join(parts)


def normalize_env_key(env_line: str) -> str:
    return re.sub(r"\s+", " ", (env_line or "").strip().casefold())


def queue_summary(segments: list[dict[str, Any]]) -> str:
    lines = [f"📋 **/updatemore** — {len(segments)} segment(s):"]
    for n, seg in enumerate(segments, 1):
        same = " (same env)" if seg.get("same_as_prev") else ""
        em = ""
        if seg.get("email_subject"):
            batch_ix = seg.get("email_batch_indices") or []
            em = " 📧 (batched reply)" if len(batch_ix) > 1 else " 📧"
        lines.append(f"  {n}. `{seg['env_line']}`{same}{em}")
    return "\n".join(lines)


def get_queue(sess: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(sess, dict):
        return None
    q = sess.get("updatemore_queue")
    return q if isinstance(q, dict) else None


def init_queue(
    segments: list[dict[str, Any]],
    *,
    chat_id: str,
    sender_id: str,
) -> dict[str, Any]:
    return {
        "segments": segments,
        "index": 0,
        "waiting_jenkins": False,
        "chat_id": chat_id,
        "sender_id": sender_id,
        "stopped": False,
        "email_batches": build_email_batch_state(segments),
        "email_watches": [],
    }


def current_segment(q: dict[str, Any]) -> dict[str, Any] | None:
    segs = q.get("segments") or []
    idx = int(q.get("index") or 0)
    if 0 <= idx < len(segs):
        return segs[idx]
    return None


def has_next_segment(q: dict[str, Any]) -> bool:
    segs = q.get("segments") or []
    return int(q.get("index") or 0) + 1 < len(segs)


def next_segment_same_env(q: dict[str, Any]) -> bool:
    segs = q.get("segments") or []
    idx = int(q.get("index") or 0)
    if idx + 1 >= len(segs):
        return False
    return bool(segs[idx + 1].get("same_as_prev"))


def segment_has_email(q: dict[str, Any]) -> bool:
    seg = current_segment(q)
    if not seg:
        return False
    return bool((seg.get("email_subject") or "").strip())


def clear_queue_from_session(sess: dict[str, Any]) -> None:
    sess.pop("updatemore_queue", None)


def register_email_build_watch(
    q: dict[str, Any],
    *,
    seg_idx: int,
    email_title: str,
) -> None:
    watches = list(q.get("email_watches") or [])
    watches.append({"seg_idx": int(seg_idx), "title": (email_title or "").strip()})
    q["email_watches"] = watches


def record_email_build_success(
    q: dict[str, Any],
    *,
    email_title: str,
    environment: str,
    when: str,
) -> tuple[str, list[tuple[str, str]] | None, str]:
    """
    Returns ``(status, rows, canonical_title)`` — ``status`` is ``sent`` or ``pending``.
    """
    title = (email_title or "").strip()
    key = normalize_email_key(title)
    seg_idx: int | None = None
    watches = list(q.get("email_watches") or [])
    for i, watch in enumerate(watches):
        if normalize_email_key(str(watch.get("title") or "")) == key:
            seg_idx = int(watch.get("seg_idx", -1))
            watches.pop(i)
            break
    q["email_watches"] = watches

    segs = q.get("segments") or []
    seg = segs[seg_idx] if seg_idx is not None and 0 <= seg_idx < len(segs) else None
    indices = list((seg or {}).get("email_batch_indices") or [])
    canonical = str((seg or {}).get("email_batch_title") or title)

    if len(indices) < 2:
        return "sent", [(environment.strip(), when.strip())], canonical

    bid = int((seg or {}).get("email_batch_id", -1))
    batches = q.get("email_batches") or {}
    batch = batches.get(bid)
    if not isinstance(batch, dict):
        batch = {"title": canonical, "indices": indices, "done_by_idx": {}}
        batches[bid] = batch
        q["email_batches"] = batches

    done_by_idx: dict[int, dict[str, str]] = dict(batch.get("done_by_idx") or {})
    if seg_idx is not None and seg_idx >= 0:
        done_by_idx[seg_idx] = {
            "environment": environment.strip(),
            "time": when.strip(),
        }
    else:
        spare = [ix for ix in indices if ix not in done_by_idx]
        if spare:
            done_by_idx[spare[0]] = {
                "environment": environment.strip(),
                "time": when.strip(),
            }
    batch["done_by_idx"] = done_by_idx

    if len(done_by_idx) < len(indices):
        return "pending", None, canonical

    rows = [
        (done_by_idx[ix]["environment"], done_by_idx[ix]["time"])
        for ix in sorted(indices)
        if ix in done_by_idx
    ]
    batch["done_by_idx"] = {}
    return "sent", rows, canonical


# ----- jenkinsbot → duty bot callbacks -----

_SUCCESS_PROCEED_RE = re.compile(r"/SuccessProceedNext\b", re.I)
_FAILED_STOP_RE = re.compile(r"/FailedStop\b", re.I)
_EMAIL_DONE_RE = re.compile(
    r"^(?P<title>.+?)\s+(?P<env>\S+)\s+(?P<time>\d{1,2}:\d{2}\s*[AP]M)\s*$",
    re.I,
)


def is_success_proceed_message(text: str) -> bool:
    return bool(_SUCCESS_PROCEED_RE.search(text or ""))


def is_failed_stop_message(text: str) -> bool:
    return bool(_FAILED_STOP_RE.search(text or ""))


def parse_email_done_message(text: str) -> tuple[str, str, str] | None:
    """Parse ``{email title} {ENVIRONMENT} {time}`` from jenkinsbot."""
    raw = (text or "").strip()
    for pat in (r"@_user_\d+", r"<[^>]+>"):
        raw = re.sub(pat, "", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    m = _EMAIL_DONE_RE.match(raw)
    if not m:
        return None
    return m.group("title").strip(), m.group("env").strip(), m.group("time").strip()


def find_waiting_queue_for_chat(
    chat_id: str,
    sessions: dict,
    sessions_lock: threading.Lock,
) -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None]:
    """Find ``updatemore_queue`` waiting for jenkins in this chat (any user session)."""
    prefix = f"{(chat_id or '').strip()}:"
    with sessions_lock:
        for sk, sess in list(sessions.items()):
            if not str(sk).startswith(prefix):
                continue
            if not isinstance(sess, dict):
                continue
            q = get_queue(sess)
            if q and q.get("waiting_jenkins") and not q.get("stopped"):
                return str(sk), q, sess
    return None, None, None


def find_active_queue_for_chat(
    chat_id: str,
    sessions: dict,
    sessions_lock: threading.Lock,
) -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None]:
    """Find any active (non-stopped) ``updatemore_queue`` in this chat."""
    prefix = f"{(chat_id or '').strip()}:"
    with sessions_lock:
        for sk, sess in list(sessions.items()):
            if not str(sk).startswith(prefix):
                continue
            if not isinstance(sess, dict):
                continue
            q = get_queue(sess)
            if q and not q.get("stopped"):
                return str(sk), q, sess
    return None, None, None


def _send_jenkins_email_reply(
    send: Callable[..., Any],
    chat_id: str,
    *,
    email_title: str,
    completions: list[tuple[str, str]],
) -> None:
    import maintenance_mail as mm

    mm.reply_jenkins_update_done_email(
        email_title=email_title,
        completions=completions,
    )
    envs = ", ".join(c[0] for c in completions)
    send(
        chat_id,
        f"📧 Auto-replied email ({len(completions)} done block(s)) → junchen@snsoft.my\n"
        f"**Subject:** `{email_title}`\n**Environments:** {envs}",
    )


def handle_jenkins_email_done(
    chat_id: str,
    sender_id: str,
    email_title: str,
    environment: str,
    when: str,
    send: Callable[..., Any],
    *,
    sessions: dict,
    sessions_lock: threading.Lock,
    session_key_fn: Callable[[str, str], str],
    dispatch_update_body: Callable[..., bool],
) -> bool:
    """Process jenkinsbot email-done notification (with or without ``/updatemore`` queue)."""
    key, q, sess = find_active_queue_for_chat(chat_id, sessions, sessions_lock)

    if q and not q.get("stopped"):
        status, rows, canonical = record_email_build_success(
            q,
            email_title=email_title,
            environment=environment,
            when=when,
        )
        if status == "pending":
            send(
                chat_id,
                f"📧 Jenkins **{environment}** done at **{when}** — waiting for other `same` "
                f"segment(s) with the same `Email:` subject before replying…",
            )
        elif status == "sent" and rows:
            try:
                _send_jenkins_email_reply(
                    send,
                    chat_id,
                    email_title=canonical or email_title,
                    completions=rows,
                )
            except Exception as ex:
                send(chat_id, f"❌ Jenkins email auto-reply failed: {ex}")
                return True

        if q.get("waiting_jenkins") and not q.get("stopped"):
            with sessions_lock:
                q["waiting_jenkins"] = False
                next_idx = int(q.get("index") or 0) + 1
                q["index"] = next_idx
                segs = q.get("segments") or []
                if next_idx >= len(segs):
                    if sess:
                        clear_queue_from_session(sess)
                    send(chat_id, "✅ All `/updatemore` segments finished.")
                    return True
                next_body = segment_to_update_body(segs[next_idx])
            send(chat_id, f"▶️ Next `/updatemore` segment ({next_idx + 1})…")
            if key:
                dispatch_update_body(
                    chat_id,
                    key,
                    next_body,
                    send,
                    from_updatemore=True,
                )
        return True

    # Single ``/update`` with Email (no queue)
    try:
        _send_jenkins_email_reply(
            send,
            chat_id,
            email_title=email_title,
            completions=[(environment, when)],
        )
    except Exception as ex:
        send(chat_id, f"❌ Jenkins email auto-reply failed: {ex}")
    return True


def handle_jenkinsbot_callback(
    chat_id: str,
    sender_id: str,
    clean_text: str,
    original_text: str,
    send: Callable[..., Any],
    *,
    sessions: dict,
    sessions_lock: threading.Lock,
    session_key_fn: Callable[[str, str], str],
    dispatch_update_body: Callable[..., bool],
) -> bool:
    """
    Handle ``/SuccessProceedNext``, ``/FailedStop``, or email-done lines from jenkinsbot.
    Returns True if consumed.
    """
    body = (original_text or clean_text or "").replace("\r\n", "\n")

    if is_failed_stop_message(body):
        key, q, sess = find_waiting_queue_for_chat(chat_id, sessions, sessions_lock)
        if not q:
            key, q, sess = find_active_queue_for_chat(chat_id, sessions, sessions_lock)
        if not q:
            return False
        with sessions_lock:
            q["stopped"] = True
            q["waiting_jenkins"] = False
            if sess:
                clear_queue_from_session(sess)
        send(
            chat_id,
            "⛔ **/updatemore** stopped — Jenkins build failed or was aborted.",
        )
        return True

    email_done = parse_email_done_message(body)
    if email_done:
        title, environment, when = email_done
        return handle_jenkins_email_done(
            chat_id,
            sender_id,
            title,
            environment,
            when,
            send,
            sessions=sessions,
            sessions_lock=sessions_lock,
            session_key_fn=session_key_fn,
            dispatch_update_body=dispatch_update_body,
        )

    if is_success_proceed_message(body):
        key, q, sess = find_waiting_queue_for_chat(chat_id, sessions, sessions_lock)
        if not q or q.get("stopped"):
            return False
        with sessions_lock:
            q["waiting_jenkins"] = False
            idx = int(q.get("index") or 0) + 1
            q["index"] = idx
            segs = q.get("segments") or []
            if idx >= len(segs):
                if sess:
                    clear_queue_from_session(sess)
                send(chat_id, "✅ All `/updatemore` segments finished.")
                return True
            next_body = segment_to_update_body(segs[idx])
        send(chat_id, f"▶️ Next `/updatemore` segment ({idx + 1})…")
        dispatch_update_body(
            chat_id,
            key or session_key_fn(chat_id, sender_id),
            next_body,
            send,
            from_updatemore=True,
        )
        return True

    return False
