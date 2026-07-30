#!/usr/bin/env python3
"""Lark ephemeral group forms for OSE leave / offset (visible only to the requester)."""

from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from datetime import date, datetime
from typing import Any, Callable, Literal, Optional

import requests

import ose_Duty as od

_CHBOX_DIR = os.path.dirname(os.path.abspath(__file__))
_OFFSET_APPROVER_NOTIFIED_PATH = os.path.join(_CHBOX_DIR, "offset_approver_notified.json")
_OFFSET_APPROVER_NOTIFIED_LOCK = threading.Lock()
_OFFSET_REQUESTER_OPEN_ID_PATH = os.path.join(_CHBOX_DIR, "offset_requester_open_id.json")
_OFFSET_REQUESTER_OPEN_ID_LOCK = threading.Lock()
_OFFSET_REQUESTER_APPROVAL_NOTIFIED_PATH = os.path.join(
    _CHBOX_DIR,
    "offset_requester_approval_notified.json",
)
_OFFSET_REQUESTER_APPROVAL_NOTIFIED_LOCK = threading.Lock()
_OFFSET_PEER_APPROVER_APPROVAL_NOTIFIED_PATH = os.path.join(
    _CHBOX_DIR,
    "offset_peer_approver_approval_notified.json",
)
_OFFSET_PEER_APPROVER_APPROVAL_NOTIFIED_LOCK = threading.Lock()

# Snapshot of every known offset row (record_id -> row dict), refreshed each poll. Used to
# detect deletions by ANY method (manual Base delete, bot delete, API) and notify approvers.
_OFFSET_ROWS_SNAPSHOT_PATH = os.path.join(_CHBOX_DIR, "offset_rows_snapshot.json")
_OFFSET_ROWS_SNAPSHOT_LOCK = threading.Lock()
# Who deleted a row via the bot (record_id -> {open_id, name, ts}); best-effort attribution.
_OFFSET_DELETE_ACTOR_PATH = os.path.join(_CHBOX_DIR, "offset_delete_actors.json")
_OFFSET_DELETE_ACTOR_LOCK = threading.Lock()
# Dedupe so the deletion poll DMs approvers only once per deleted row (record_id -> ts).
_OFFSET_DELETION_NOTIFIED_PATH = os.path.join(_CHBOX_DIR, "offset_deletion_notified.json")
_OFFSET_DELETION_NOTIFIED_LOCK = threading.Lock()
# Drop attribution / dedupe entries older than this (seconds) to keep the files small.
_OFFSET_DELETE_STATE_TTL_SEC = int(os.getenv("OSE_OFFSET_DELETE_STATE_TTL_SEC", str(30 * 24 * 3600)))

# Prevent opening multiple edit forms for the same pending record (double-tap Edit).
_OFFSET_EDIT_OPEN_LOCK = threading.Lock()
_OFFSET_EDIT_OPEN: dict[str, str] = {}

# Prevent double-tap Delete on the same row (duplicate callbacks / impatient clicks).
_OFFSET_DELETE_LOCK = threading.Lock()
_OFFSET_DELETE_IN_FLIGHT: set[str] = set()

# Prevent double-tap Submit creating duplicate Bitable rows (Lark + web).
_OFFSET_SUBMIT_LOCK = threading.Lock()
_OFFSET_SUBMIT_IN_FLIGHT: set[str] = set()
_OFFSET_SUBMIT_RECENT: dict[str, float] = {}
_OFFSET_SUBMIT_DEDUPE_SEC = int(os.getenv("OSE_OFFSET_SUBMIT_DEDUPE_SEC", "60"))

_OFFSET_SUBMIT_KEY = "offsetleave_offset_submit"
_LEAVE_SUBMIT_KEY = "offsetleave_leave_submit"
_OFFSET_APPR_PICK_KEY = "offsetleave_offset_appr_pick"
_OFFSET_APPR_CONFIRM_KEY = "offsetleave_offset_appr_confirm"

# Lark open_ids — each receives the interactive approval card for new offset submissions.
# Keep in sync with ``main.OFFSET_APPROVER_OPEN_IDS``.
OFFSET_APPROVER_OPEN_IDS: frozenset[str] = frozenset(
    {
        "ou_540944d83349cda961ec6124425cdfb4",
        "ou_c4346ace5927c14f51a89b2394b55338",
        "ou_5f660c0fb0769d184aca635d02209272",  # Jun Chen
    }
)

# Lark bot custom menu — Push event ``event_key`` (Developer Console → Bot → Custom menu).
BOT_MENU_EVENT_KEY_OFFSET_FORM = "187236871623876123"
BOT_MENU_EVENT_KEYS_OFFSET_FORM: frozenset[str] = frozenset({BOT_MENU_EVENT_KEY_OFFSET_FORM})
BOT_MENU_EVENT_KEY_OFFSET_DELETE = "1786248761248618414124"
BOT_MENU_EVENT_KEYS_OFFSET_DELETE: frozenset[str] = frozenset({BOT_MENU_EVENT_KEY_OFFSET_DELETE})
BOT_MENU_EVENT_KEY_SHOW_OFFSET = "6543761547615237517625312"
BOT_MENU_EVENT_KEYS_SHOW_OFFSET: frozenset[str] = frozenset({BOT_MENU_EVENT_KEY_SHOW_OFFSET})
BOT_MENU_EVENT_KEY_OFFSET_EDIT = "16246751235716253765123123"
BOT_MENU_EVENT_KEYS_OFFSET_EDIT: frozenset[str] = frozenset({BOT_MENU_EVENT_KEY_OFFSET_EDIT})
BOT_MENU_EVENT_KEY_PENDING_OFFSET = "741626437812648126123"
BOT_MENU_EVENT_KEYS_PENDING_OFFSET: frozenset[str] = frozenset({BOT_MENU_EVENT_KEY_PENDING_OFFSET})
BOT_MENU_EVENT_KEY_APPROVER_OFFSET_EDIT = "1726387126481826481"
BOT_MENU_EVENT_KEYS_APPROVER_OFFSET_EDIT: frozenset[str] = frozenset({BOT_MENU_EVENT_KEY_APPROVER_OFFSET_EDIT})
BOT_MENU_EVENT_KEY_APPROVER_OFFSET_DELETE = "512346512435816238"
BOT_MENU_EVENT_KEYS_APPROVER_OFFSET_DELETE: frozenset[str] = frozenset({BOT_MENU_EVENT_KEY_APPROVER_OFFSET_DELETE})
BOT_MENU_EVENT_KEY_APPROVER_SHOW_OFFSET = "7632487287468163123"
BOT_MENU_EVENT_KEYS_APPROVER_SHOW_OFFSET: frozenset[str] = frozenset({BOT_MENU_EVENT_KEY_APPROVER_SHOW_OFFSET})

OFFSET_APPROVAL_CALLBACK_KEYS = frozenset({_OFFSET_APPR_PICK_KEY, _OFFSET_APPR_CONFIRM_KEY})

_OFFSET_EDIT_PICK_KEY = "offsetleave_offset_edit_pick"
_OFFSET_EDIT_SUBMIT_KEY = "offsetleave_offset_edit_submit"
_OFFSET_DELETE_KEY = "offsetleave_offset_delete"
# Approver deleteoffset step 1: pick a month, then see only that month's rows.
_OFFSET_DELETE_MONTH_KEY = "offsetleave_offset_delete_month"

OFFSETLEAVE_CARD_CALLBACK_KEYS = frozenset(
    set(OFFSET_APPROVAL_CALLBACK_KEYS)
    | {
        _OFFSET_EDIT_PICK_KEY,
        _OFFSET_EDIT_SUBMIT_KEY,
        _OFFSET_DELETE_KEY,
        _OFFSET_DELETE_MONTH_KEY,
    }
)

# Mirror OSE offset rows from the bot Base table into the wiki duty-shift Offset2026 bitable.
# Source: https://casinoplus.sg.larksuite.com/base/CpdEbEofwaYyyEsSjlElKNxzgec?table=tblC5T2MAydwT42j&view=vewHEvu7K8
# Dest:   https://casinoplus.sg.larksuite.com/wiki/O4Dfw4DVTiPpFukn801l5z3WgMd?sheet=02eZI8&table=tblL4rrbJHJSosDX&view=vewFF82Q2p
OFFSET_SOURCE_BASE_TOKEN = (
    os.getenv("OFFSET_SOURCE_BASE_TOKEN") or os.getenv("OSE_BASE_TOKEN") or "CpdEbEofwaYyyEsSjlElKNxzgec"
).strip()
OFFSET_SOURCE_TABLE_ID = (os.getenv("OFFSET_SOURCE_TABLE_ID") or os.getenv("OSE_OFFSET_TABLE_ID") or "tblC5T2MAydwT42j").strip()
OFFSET_DUTY_WIKI_SPREADSHEET_TOKEN = (
    os.getenv("OFFSET_DUTY_WIKI_SPREADSHEET_TOKEN") or "UjF0saOVuhJSWLtBv9GlaQOkgbe"
).strip()
OFFSET_DUTY_SHEET_ID = (os.getenv("OFFSET_DUTY_SHEET_ID") or "02eZI8").strip()
OFFSET_DUTY_TABLE_ID = (os.getenv("OFFSET_DUTY_TABLE_ID") or "tblL4rrbJHJSosDX").strip()
OFFSET_DUTY_BITABLE_BASE = (os.getenv("OFFSET_DUTY_BITABLE_BASE") or "I97gbnViZaqSdNs8U8AliyWtgDz").strip()
_OFFSET_DUTY_SYNC_STATE_PATH = os.path.join(
    _CHBOX_DIR,
    os.getenv("OFFSET_DUTY_SYNC_STATE", ".offset_duty_sync_state.json"),
)
_OFFSET_DUTY_SYNC_LOCK = threading.Lock()
_OFFSET_DUTY_SYNC_THREAD_LOCK = threading.Lock()
_OFFSET_DUTY_SYNC_FIELDS = (
    "Request Date",
    "Request Person",
    "Exchange Person",
    "Shift Type",
    "Original Date",
    "Exchange Date",
    "Reason",
    "Approval Status",
    "Approver",
    "Approval Date",
    "Remarks",
)


def _load_offset_duty_sync_state_unlocked() -> dict[str, Any]:
    try:
        with open(_OFFSET_DUTY_SYNC_STATE_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return {"by_src": {}, "src_fp": {}}
    except Exception:
        return {"by_src": {}, "src_fp": {}}
    if not isinstance(data, dict):
        return {"by_src": {}, "src_fp": {}}
    by_src = data.get("by_src")
    if not isinstance(by_src, dict):
        by_src = {}
    src_fp = data.get("src_fp")
    if not isinstance(src_fp, dict):
        src_fp = {}
    return {
        "by_src": {str(k): str(v) for k, v in by_src.items() if k and v},
        "src_fp": {str(k): str(v) for k, v in src_fp.items() if k and v},
    }


def _save_offset_duty_sync_state_unlocked(state: dict[str, Any]) -> None:
    payload = {
        "by_src": dict(state.get("by_src") or {}),
        "src_fp": dict(state.get("src_fp") or {}),
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    tmp = _OFFSET_DUTY_SYNC_STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    os.replace(tmp, _OFFSET_DUTY_SYNC_STATE_PATH)


def _read_json_file(path: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _write_json_file(path: str, payload: dict[str, Any]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    os.replace(tmp, path)


def _load_offset_rows_snapshot() -> dict[str, dict[str, Any]]:
    with _OFFSET_ROWS_SNAPSHOT_LOCK:
        data = _read_json_file(_OFFSET_ROWS_SNAPSHOT_PATH)
    rows = data.get("rows") if isinstance(data.get("rows"), dict) else {}
    return {str(k): dict(v) for k, v in rows.items() if k and isinstance(v, dict)}


def _save_offset_rows_snapshot(rows: dict[str, dict[str, Any]]) -> None:
    with _OFFSET_ROWS_SNAPSHOT_LOCK:
        _write_json_file(
            _OFFSET_ROWS_SNAPSHOT_PATH,
            {
                "rows": {str(k): dict(v) for k, v in rows.items() if k},
                "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            },
        )


def _prune_ts_map(entries: dict[str, Any]) -> dict[str, Any]:
    """Drop entries whose ``ts`` is older than the TTL (entries without ts are kept)."""
    now = time.time()
    out: dict[str, Any] = {}
    for k, v in entries.items():
        if not k:
            continue
        ts = 0.0
        if isinstance(v, dict):
            try:
                ts = float(v.get("ts") or 0)
            except Exception:
                ts = 0.0
        else:
            try:
                ts = float(v)
            except Exception:
                ts = 0.0
        if ts and (now - ts) > _OFFSET_DELETE_STATE_TTL_SEC:
            continue
        out[str(k)] = v
    return out


def _record_offset_delete_actor(record_id: str, open_id: str, name: str) -> None:
    """Remember who deleted ``record_id`` via the bot, so the poll can attribute the deleter."""
    rid = (record_id or "").strip()
    if not rid:
        return
    with _OFFSET_DELETE_ACTOR_LOCK:
        data = _read_json_file(_OFFSET_DELETE_ACTOR_PATH)
        actors = data.get("actors") if isinstance(data.get("actors"), dict) else {}
        actors = _prune_ts_map({str(k): v for k, v in actors.items()})
        actors[rid] = {
            "open_id": (open_id or "").strip(),
            "name": (name or "").strip(),
            "ts": time.time(),
        }
        _write_json_file(_OFFSET_DELETE_ACTOR_PATH, {"actors": actors})


def _pop_offset_delete_actor(record_id: str) -> dict[str, str]:
    """Return + remove the recorded bot deleter for ``record_id`` (empty dict if none)."""
    rid = (record_id or "").strip()
    if not rid:
        return {}
    with _OFFSET_DELETE_ACTOR_LOCK:
        data = _read_json_file(_OFFSET_DELETE_ACTOR_PATH)
        actors = data.get("actors") if isinstance(data.get("actors"), dict) else {}
        actors = _prune_ts_map({str(k): v for k, v in actors.items()})
        entry = actors.pop(rid, None)
        _write_json_file(_OFFSET_DELETE_ACTOR_PATH, {"actors": actors})
    if isinstance(entry, dict):
        return {"open_id": str(entry.get("open_id") or ""), "name": str(entry.get("name") or "")}
    return {}


def _offset_deletion_already_notified(record_id: str) -> bool:
    rid = (record_id or "").strip()
    if not rid:
        return False
    with _OFFSET_DELETION_NOTIFIED_LOCK:
        data = _read_json_file(_OFFSET_DELETION_NOTIFIED_PATH)
        notified = data.get("notified") if isinstance(data.get("notified"), dict) else {}
        return rid in notified


def _mark_offset_deletion_notified(record_id: str) -> None:
    rid = (record_id or "").strip()
    if not rid:
        return
    with _OFFSET_DELETION_NOTIFIED_LOCK:
        data = _read_json_file(_OFFSET_DELETION_NOTIFIED_PATH)
        notified = data.get("notified") if isinstance(data.get("notified"), dict) else {}
        notified = _prune_ts_map({str(k): v for k, v in notified.items()})
        notified[rid] = {"ts": time.time()}
        _write_json_file(_OFFSET_DELETION_NOTIFIED_PATH, {"notified": notified})


def _offset_duty_sync_map_get(src_record_id: str) -> str:
    rid = (src_record_id or "").strip()
    if not rid:
        return ""
    with _OFFSET_DUTY_SYNC_LOCK:
        return str(_load_offset_duty_sync_state_unlocked().get("by_src", {}).get(rid) or "").strip()


def _offset_duty_sync_map_set(
    src_record_id: str,
    dest_record_id: str,
    *,
    fingerprint: str = "",
) -> None:
    src = (src_record_id or "").strip()
    dest = (dest_record_id or "").strip()
    if not src or not dest:
        return
    fp = (fingerprint or "").strip()
    with _OFFSET_DUTY_SYNC_LOCK:
        state = _load_offset_duty_sync_state_unlocked()
        by_src = dict(state.get("by_src") or {})
        src_fp = dict(state.get("src_fp") or {})
        by_src[src] = dest
        if fp:
            src_fp[src] = fp
        _save_offset_duty_sync_state_unlocked({"by_src": by_src, "src_fp": src_fp})


def _offset_duty_sync_map_pop(src_record_id: str) -> str:
    src = (src_record_id or "").strip()
    if not src:
        return ""
    with _OFFSET_DUTY_SYNC_LOCK:
        state = _load_offset_duty_sync_state_unlocked()
        by_src = dict(state.get("by_src") or {})
        src_fp = dict(state.get("src_fp") or {})
        dest = str(by_src.pop(src, "") or "").strip()
        src_fp.pop(src, None)
        _save_offset_duty_sync_state_unlocked({"by_src": by_src, "src_fp": src_fp})
        return dest


def _offset_sync_field_key(v: Any) -> str:
    if isinstance(v, (int, float)):
        return str(int(v))
    return str(od._field_text(v)).strip()


def _offset_sync_fingerprint(fields: dict[str, Any]) -> str:
    f = fields or {}
    parts = [
        _offset_sync_field_key(f.get("Request Date")),
        od._title_name(od._field_text(f.get("Request Person"))),
        od._title_name(od._field_text(f.get("Exchange Person"))),
        od._field_text(f.get("Shift Type")).strip().upper(),
        _offset_sync_field_key(f.get("Original Date")),
        _offset_sync_field_key(f.get("Exchange Date")),
        od._field_text(f.get("Reason")).strip().lower(),
    ]
    return "|".join(parts)


def _offset_fields_for_duty_mirror(src_fields: dict[str, Any]) -> dict[str, Any]:
    src = src_fields or {}
    out: dict[str, Any] = {}
    for name in _OFFSET_DUTY_SYNC_FIELDS:
        if name not in src:
            continue
        val = src.get(name)
        if val is None:
            continue
        if isinstance(val, str) and not val.strip():
            continue
        out[name] = val
    return out


def _bitable_dest_request(
    token: str,
    method: str,
    *,
    record_id: str = "",
    fields: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    base = OFFSET_DUTY_BITABLE_BASE
    table = OFFSET_DUTY_TABLE_ID
    headers = {"Authorization": f"Bearer {token}"}
    if fields is not None:
        headers["Content-Type"] = "application/json"
    rid = (record_id or "").strip()
    if method.upper() == "GET" and rid:
        url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base}/tables/{table}/records/{rid}"
        res = requests.get(url, headers=headers, params={"user_id_type": "open_id"}, timeout=30).json()
    elif method.upper() == "POST":
        url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base}/tables/{table}/records"
        res = requests.post(
            url,
            headers=headers,
            params={"user_id_type": "open_id"},
            json={"fields": fields or {}},
            timeout=30,
        ).json()
    elif method.upper() == "PUT" and rid:
        url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base}/tables/{table}/records/{rid}"
        res = requests.put(
            url,
            headers=headers,
            params={"user_id_type": "open_id"},
            json={"fields": fields or {}},
            timeout=30,
        ).json()
    elif method.upper() == "DELETE" and rid:
        url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base}/tables/{table}/records/{rid}"
        res = requests.delete(url, headers=headers, params={"user_id_type": "open_id"}, timeout=30).json()
    else:
        raise ValueError(f"unsupported bitable dest request {method!r}")
    return res


def _fetch_source_offset_record(token: str, src_record_id: str) -> Optional[dict[str, Any]]:
    rid = (src_record_id or "").strip()
    if not rid:
        return None
    url = (
        f"https://open.larksuite.com/open-apis/bitable/v1/apps/"
        f"{OFFSET_SOURCE_BASE_TOKEN}/tables/{OFFSET_SOURCE_TABLE_ID}/records/{rid}"
    )
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers, params={"user_id_type": "open_id"}, timeout=30).json()
    if res.get("code") != 0:
        return None
    rec = (res.get("data") or {}).get("record")
    return rec if isinstance(rec, dict) else None


def _fetch_source_offset_record_with_retry(
    token: str,
    src_record_id: str,
    *,
    attempts: int = 5,
    delay_sec: float = 0.6,
) -> Optional[dict[str, Any]]:
    """Bitable create → GET can lag briefly; retry before treating the row as missing."""
    rid = (src_record_id or "").strip()
    if not rid:
        return None
    last: Optional[dict[str, Any]] = None
    tries = max(1, int(attempts))
    for i in range(tries):
        last = _fetch_source_offset_record(token, rid)
        if last is not None:
            return last
        if i + 1 < tries:
            time.sleep(delay_sec)
    return last


def _find_dest_record_id_by_fingerprint(token: str, fingerprint: str) -> str:
    fp = (fingerprint or "").strip()
    if not fp:
        return ""
    items = od._bitable_get_all_records(token, OFFSET_DUTY_BITABLE_BASE, OFFSET_DUTY_TABLE_ID)
    for it in items:
        fields = it.get("fields") or {}
        if _offset_sync_fingerprint(fields) == fp:
            return str(it.get("record_id") or "").strip()
    return ""


def _upsert_offset_duty_mirror(token: str, src_record_id: str, src_fields: dict[str, Any]) -> str:
    src = (src_record_id or "").strip()
    if not src:
        raise ValueError("src_record_id is required")
    payload = _offset_fields_for_duty_mirror(src_fields)
    if not payload:
        raise ValueError("empty mirror payload")
    dest_id = _offset_duty_sync_map_get(src)
    if dest_id:
        upd = _bitable_dest_request(token, "PUT", record_id=dest_id, fields=payload)
        if upd.get("code") == 0:
            return dest_id
        if int(upd.get("code") or 0) not in (1254043, 1254044):
            raise RuntimeError(f"duty offset update failed: {upd}")
        dest_id = ""
    if not dest_id:
        fp = _offset_sync_fingerprint(src_fields)
        dest_id = _find_dest_record_id_by_fingerprint(token, fp)
    if dest_id:
        upd = _bitable_dest_request(token, "PUT", record_id=dest_id, fields=payload)
        if upd.get("code") != 0:
            raise RuntimeError(f"duty offset update failed: {upd}")
    else:
        created = _bitable_dest_request(token, "POST", fields=payload)
        if created.get("code") != 0:
            raise RuntimeError(f"duty offset create failed: {created}")
        dest_id = str(((created.get("data") or {}).get("record") or {}).get("record_id") or "").strip()
        if not dest_id:
            raise RuntimeError(f"duty offset create missing record_id: {created}")
    _offset_duty_sync_map_set(src, dest_id, fingerprint=_offset_sync_fingerprint(src_fields))
    return dest_id


def _delete_offset_duty_mirror(token: str, src_record_id: str) -> bool:
    src = (src_record_id or "").strip()
    if not src:
        return False
    dest_id = _offset_duty_sync_map_pop(src)
    if not dest_id:
        return False
    res = _bitable_dest_request(token, "DELETE", record_id=dest_id)
    code = int(res.get("code") or 0)
    if code == 0 or code in (1254043, 1254044):
        return True
    raise RuntimeError(f"duty offset delete failed: {res}")


def _prune_duty_wiki_orphans(token: str, live_src_fingerprints: set[str]) -> int:
    """
    Delete wiki rows with no matching Base row (manual Base delete, or pre-sync wiki-only rows).
    """
    fps = {fp for fp in live_src_fingerprints if fp}
    items = od._bitable_get_all_records(token, OFFSET_DUTY_BITABLE_BASE, OFFSET_DUTY_TABLE_ID)
    pruned = 0
    with _OFFSET_DUTY_SYNC_LOCK:
        state = _load_offset_duty_sync_state_unlocked()
        by_src = dict(state.get("by_src") or {})
        src_fp = dict(state.get("src_fp") or {})
    dest_to_src = {dest: src for src, dest in by_src.items()}
    for it in items:
        dest_id = str(it.get("record_id") or "").strip()
        if not dest_id:
            continue
        fp = _offset_sync_fingerprint(it.get("fields") or {})
        if fp in fps:
            continue
        res = _bitable_dest_request(token, "DELETE", record_id=dest_id)
        code = int(res.get("code") or 0)
        if code not in (0, 1254043, 1254044):
            raise RuntimeError(f"duty offset orphan prune failed: {res}")
        src_id = dest_to_src.get(dest_id)
        if src_id:
            by_src.pop(src_id, None)
            src_fp.pop(src_id, None)
        pruned += 1
    if pruned:
        with _OFFSET_DUTY_SYNC_LOCK:
            _save_offset_duty_sync_state_unlocked({"by_src": by_src, "src_fp": src_fp})
    return pruned


def sync_offset_to_duty_wiki(*, record_id: str = "", delete: bool = False) -> dict[str, Any]:
    """
    Mirror one source offset row into the wiki Offset2026 bitable (best-effort, raises on API error).
    """
    rid = (record_id or "").strip()
    if not rid:
        return {"ok": False, "error": "missing record_id"}
    token = od.get_tenant_access_token()
    if delete:
        deleted = _delete_offset_duty_mirror(token, rid)
        return {"ok": True, "record_id": rid, "deleted": deleted}
    had_mapping = bool(_offset_duty_sync_map_get(rid))
    src = _fetch_source_offset_record_with_retry(token, rid)
    if src is None:
        if had_mapping:
            deleted = _delete_offset_duty_mirror(token, rid)
            return {"ok": True, "record_id": rid, "deleted": deleted, "reason": "source_missing"}
        raise RuntimeError(f"source offset {rid!r} not readable yet (will retry on next poll)")
    dest_id = _upsert_offset_duty_mirror(token, rid, src.get("fields") or {})
    return {"ok": True, "record_id": rid, "dest_record_id": dest_id}


def sync_all_offsets_to_duty_wiki() -> dict[str, Any]:
    """Full reconcile: every source offset row is upserted into wiki Offset2026."""
    token = od.get_tenant_access_token()
    src_items = od._bitable_get_all_records(token, OFFSET_SOURCE_BASE_TOKEN, OFFSET_SOURCE_TABLE_ID)
    upserted = 0
    deleted = 0
    errors: list[str] = []
    live_src: set[str] = set()
    live_fps: set[str] = set()
    for it in src_items:
        src_id = str(it.get("record_id") or "").strip()
        if not src_id:
            continue
        live_src.add(src_id)
        live_fps.add(_offset_sync_fingerprint(it.get("fields") or {}))
        try:
            _upsert_offset_duty_mirror(token, src_id, it.get("fields") or {})
            upserted += 1
        except Exception as exc:
            errors.append(f"{src_id}: {exc}")
    with _OFFSET_DUTY_SYNC_LOCK:
        state = _load_offset_duty_sync_state_unlocked()
        by_src = dict(state.get("by_src") or {})
        stale_src = [sid for sid in by_src if sid not in live_src]
    for sid in stale_src:
        try:
            if _delete_offset_duty_mirror(token, sid):
                deleted += 1
        except Exception as exc:
            errors.append(f"delete {sid}: {exc}")
    try:
        deleted += _prune_duty_wiki_orphans(token, live_fps)
    except Exception as exc:
        errors.append(f"orphan prune: {exc}")
    return {
        "ok": not errors,
        "source_rows": len(live_src),
        "upserted": upserted,
        "deleted": deleted,
        "errors": errors,
    }


def schedule_offset_duty_wiki_sync(
    *,
    record_id: str = "",
    delete: bool = False,
    full: bool = False,
) -> None:
    """Background mirror to wiki Offset2026; never raises to callers."""

    def _run() -> None:
        try:
            if full:
                result = sync_all_offsets_to_duty_wiki()
                print(
                    f"[offsetleave] duty wiki offset sync: upserted={result.get('upserted')} "
                    f"deleted={result.get('deleted')} errors={len(result.get('errors') or [])}",
                    flush=True,
                )
                if result.get("errors"):
                    print(f"[offsetleave] duty wiki offset sync errors: {result['errors']!r}", flush=True)
                return
            result = sync_offset_to_duty_wiki(record_id=record_id, delete=delete)
            if result.get("dest_record_id"):
                print(
                    f"[offsetleave] duty wiki offset upserted {record_id!r} -> {result['dest_record_id']!r}",
                    flush=True,
                )
        except Exception as exc:
            action = "delete" if delete else "upsert"
            target = record_id or ("ALL" if full else "?")
            print(f"[offsetleave] duty wiki offset {action} failed for {target!r}: {exc!r}", flush=True)

    with _OFFSET_DUTY_SYNC_THREAD_LOCK:
        threading.Thread(target=_run, daemon=True, name="offset-duty-wiki-sync").start()


OffsetLeaveAction = Literal[
    "offset_form",
    "leave_form",
    "edit_offset",
    "delete_offset",
    "pending_offset",
    "show_offset",
]

_OFFSET_LEAVE_ACTIONS: frozenset[str] = frozenset(
    {
        "offset_form",
        "leave_form",
        "edit_offset",
        "delete_offset",
        "pending_offset",
        "show_offset",
    }
)

# Cheap signals — only call the LLM when these appear and rules did not match.
_OFFSET_LEAVE_SIGNAL_RE = re.compile(
    r"(?i)\b("
    r"offset|leave|swap|shift|duty|roster|调休|换班|请假|休假|"
    r"annual\s+leave|sick\s+leave|compassionate|hospitalisation"
    r")\b"
)

# Read-only leave queries (dutyai / ``/leave`` month cards) — not the leave request form.
_LEAVE_QUERY_RE = re.compile(
    r"(?i)\b("
    r"who(?:'s|\s+is|\s+are).{0,40}\bleave\b|"
    r"\bleave\b.{0,30}\b(?:today|this\s+month|next\s+month|monthly|list)\b|"
    r"(?:show|list|check|view).{0,20}\bleave\b|"
    r"anyone\s+on\s+leave|on\s+leave\s+today|today(?:'s)?\s+leave"
    r")\b"
)

# Department duty roster lookups — commandagent / dutyai / ``/sre`` etc., not offset.
_DUTY_DEPT_RE = (
    r"fpms|pms|bi|fe|cpms|sre|db|dba|liveslot|ote|ft|ose|cpa|fpms0"
)
_DUTY_ROSTER_QUERY_RE = re.compile(
    r"(?i)\b("
    rf"(?:{_DUTY_DEPT_RE})\b.{{0,40}}\bduty\b|"
    rf"\bduty\b.{{0,40}}\b(?:{_DUTY_DEPT_RE})\b|"
    r"(?:next|this|last)\s+week.{0,40}\bduty\b|"
    r"\bduty\b.{0,30}\b(?:next|this|last)\s+week\b|"
    r"who(?:'s|\s+is|\s+are).{0,40}\bduty\b|"
    r"(?:show|list|check|view).{0,24}\bduty\b|"
    r"on[\s-]?call"
    r")\b"
)


def looks_like_duty_roster_query(text: str) -> bool:
    """Roster/on-call lookup — must not be classified as offset/leave."""
    s = normalize_offset_command_text(text)
    if not s:
        return False
    if od._text_mentions_offset(s):
        return False
    if _LEAVE_QUERY_RE.search(s):
        return False
    return bool(_DUTY_ROSTER_QUERY_RE.search(s))

_OFFSET_FORM_RULE_RE = re.compile(
    r"(?i)\b("
    r"调休|换班|"
    r"swap\s+(?:my\s+)?(?:duty|shift|roster)|"
    r"exchange\s+(?:my\s+)?(?:duty|shift|roster)|"
    r"(?:duty|shift|roster)\s+swap|swap\s+(?:duty|shift)|"
    r"change\s+my\s+(?:duty|shift)\s+day|"
    r"request\s+(?:a\s+)?(?:duty\s+)?offset|"
    r"submit\s+(?:an?\s+)?offset|offset\s+request|offset\s+form|"
    r"need\s+to\s+swap\s+my\s+(?:duty|shift)"
    r")\b"
)

_LEAVE_FORM_RULE_RE = re.compile(
    r"(?i)\b("
    r"请假|休假申请|我要请假|申请请假|"
    r"apply\s+(?:for\s+)?(?:annual|sick|compassionate|hospitalisation|marriage|"
    r"maternity|replacement|non[\s-]?pay)?\s*leave|"
    r"request\s+(?:annual|sick|compassionate|hospitalisation|marriage|maternity|"
    r"replacement|non[\s-]?pay)?\s*leave|"
    r"submit\s+(?:a\s+)?leave|leave\s+request|leave\s+form|"
    r"take\s+(?:annual|sick)\s+leave|"
    r"i\s+need\s+(?:annual|sick\s+)?leave|"
    r"file\s+(?:a\s+)?leave"
    r")\b"
)

_EDIT_OFFSET_RULE_RE = re.compile(
    r"(?i)^(?:/)?editoffset\s*$"
    r"|^(?:edit|change|update|modify|amend)\s+(?:my\s+|the\s+|our\s+)?(?:pending\s+)?offsets?(?:\s+request)?\s*$"
)

_DELETE_OFFSET_RULE_RE = re.compile(
    r"(?i)^(?:/)?deleteoffset\s*$"
    r"|^(?:delete|remove|cancel|drop)\s+(?:my\s+|the\s+|our\s+)?(?:pending\s+)?offsets?(?:\s+request)?\s*$"
)

_OFFSET_COMMAND_WORDS = frozenset(
    {"offset", "deleteoffset", "editoffset", "pendingoffset", "showoffset"}
)


def normalize_offset_command_text(text: str) -> str:
    """Accept ``/showoffset`` or ``showoffset`` — return unprefixed command text."""
    s = (text or "").strip()
    if not s.startswith("/"):
        return s
    rest = s[1:].lstrip()
    word = (rest.split()[0] if rest else "").lower()
    if word in _OFFSET_COMMAND_WORDS:
        return rest
    return s


def is_offset_command_text(text: str) -> bool:
    s = normalize_offset_command_text(text)
    word = (s.split()[0] if s else "").lower()
    return word in _OFFSET_COMMAND_WORDS


_OFFSET_FORM_SLASH_RE = re.compile(r"^(?:/)?offset\s*$", re.I)
_EDIT_OFFSET_SLASH_RE = re.compile(r"^(?:/)?editoffset\b", re.I)
_DELETE_OFFSET_SLASH_RE = re.compile(r"^(?:/)?deleteoffset\b", re.I)
_PENDING_OFFSET_SLASH_RE = re.compile(r"^(?:/)?pendingoffset\b", re.I)
_SHOW_OFFSET_SLASH_RE = re.compile(r"^(?:/)?showoffset\b", re.I)

_PENDING_OFFSET_RULE_RE = re.compile(
    r"(?i)^(?:/)?pendingoffset\s*$"
    r"|^(?:pending|pendingoffset)\s*(?:offset\s+)?(?:requests?|approvals?|queue)?\s*$"
)

_CLASSIFY_ACTION_CACHE: dict[str, Optional[str]] = {}

_OFFSET_LEAVE_LLM_SYSTEM = (
    "You classify messages for an OSE workplace bot. The user wants to open a form, "
    "run an admin queue, or view a calendar — NOT read-only 'who is on leave' queries.\n"
    "Reply with ONE JSON object only: {\"action\": \"<action>\"}\n"
    "Valid actions:\n"
    "- offset_form: submit a NEW duty shift swap / offset request (open offset form)\n"
    "- leave_form: submit a NEW leave application (open leave form)\n"
    "- edit_offset: edit or change their pending offset request\n"
    "- delete_offset: cancel / delete their offset request\n"
    "- pending_offset: approver views pending offset approvals queue\n"
    "- show_offset: view the offset calendar / schedule (read-only)\n"
    "- none: not about OSE offset/leave forms (e.g. who is on leave, monthly leave lists, "
    "other departments)\n"
    "Examples:\n"
    '\"I want to swap my duty with someone\" -> offset_form\n'
    '\"can I apply for annual leave\" -> leave_form\n'
    '\"change my offset request\" -> edit_offset\n'
    '\"cancel my offset\" -> delete_offset\n'
    '\"show pending offset approvals\" -> pending_offset\n'
    '\"offset calendar for June\" -> show_offset\n'
    '\"who is on leave today\" -> none\n'
    '\"fpms leave this month\" -> none'
)


def _offset_leave_ai_enabled() -> bool:
    explicit = (os.getenv("BOT_USE_OFFSET_LEAVE_AI") or "").strip().lower()
    if explicit in ("0", "false", "no", "off"):
        return False
    if explicit in ("1", "true", "yes", "on"):
        return True
    inherited = (os.getenv("BOT_USE_AI") or "").strip().lower()
    if inherited in ("0", "false", "no", "off"):
        return False
    return True


def _offset_leave_llm_available() -> bool:
    try:
        import chatagent as ca

        return bool(ca.llm_available())
    except Exception:
        return False


def _parse_offset_leave_action_rules(text: str) -> Optional[str]:
    s = normalize_offset_command_text(text)
    if not s:
        return None
    if re.fullmatch(r"offset\s*", s, re.I):
        return "offset_form"
    if _PENDING_OFFSET_RULE_RE.match(s):
        return "pending_offset"
    if _DELETE_OFFSET_RULE_RE.match(s):
        return "delete_offset"
    if _EDIT_OFFSET_RULE_RE.match(s):
        return "edit_offset"
    if od.parse_showoffset_command(s) is not None:
        return "show_offset"
    if _LEAVE_QUERY_RE.search(s):
        return None
    if _OFFSET_FORM_RULE_RE.search(s):
        return "offset_form"
    if _LEAVE_FORM_RULE_RE.search(s):
        return "leave_form"
    if od._text_mentions_offset(s):
        if re.search(r"(?i)\b(delete|remove|cancel|drop|withdraw|取消|删除)\b", s):
            return "delete_offset"
        if re.search(r"(?i)\b(edit|change|update|modify|amend|修改|更改)\b", s):
            return "edit_offset"
        if re.search(r"(?i)\b(pending|approvals?)\b", s):
            return "pending_offset"
        # Lookups (show / who / 谁 / month) — not a new request
        if re.search(
            r"(?i)\b(show\s+me|show|who|which|what|list|check|view|see|approved|rejected)\b",
            s,
        ) or re.search(r"谁|有谁|哪位", s):
            return "query_offset"
        if od.parse_offset_month_from_text(s) and not re.search(
            r"(?i)\b(apply|submit|request|swap|delete|edit|cancel|申请|删除|修改|取消)\b", s
        ):
            return "query_offset"
        if _OFFSET_FORM_RULE_RE.search(s):
            return "offset_form"
        return "offset_form"
    if re.search(r"\bleave\b", s, re.I) and not _is_month_attendance_slash_command(s):
        return "leave_form"
    return None


def _shift_calendar_month(year: int, month: int, delta: int) -> tuple[int, int]:
    idx = year * 12 + (month - 1) + delta
    return idx // 12, (idx % 12) + 1


def _parse_offset_month_filter(text: str) -> Optional[tuple[int, int]]:
    """Extract ``(year, month)`` from phrases like ``for this month`` / ``in June``."""
    s = (text or "").strip()
    if not s:
        return None
    low = s.lower()
    today = date.today()

    if re.search(r"(?i)\b(?:for|in)\s+(?:this|current)\s+month\b", low) or re.search(
        r"(?i)\bthis\s+month\b", low
    ):
        return today.year, today.month
    if re.search(r"(?i)\b(?:for|in)\s+next\s+month\b", low) or re.search(
        r"(?i)\bnext\s+month\b", low
    ):
        return _shift_calendar_month(today.year, today.month, 1)
    if re.search(r"(?i)\b(?:for|in)\s+last\s+month\b", low) or re.search(
        r"(?i)\blast\s+month\b", low
    ):
        return _shift_calendar_month(today.year, today.month, -1)

    m = re.search(r"(?i)\b(?:for|in)\s+(\d{1,2})\b", s)
    if m:
        month = int(m.group(1))
        if 1 <= month <= 12:
            return today.year, month

    m = re.search(
        r"(?i)\b(?:for|in)\s+(january|february|march|april|may|june|july|august|"
        r"september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\b",
        s,
    )
    if m:
        token = m.group(1)
        for name, num in od.MONTH_MAP.items():
            if name.lower() == token.lower():
                return today.year, num
    return None


def _month_filter_label(year: int, month: int) -> str:
    try:
        return date(year, month, 1).strftime("%B %Y")
    except ValueError:
        return f"{month}/{year}"


def _offset_row_touches_month(row: dict[str, Any], year: int, month: int) -> bool:
    for key in ("original_date", "exchange_date", "request_date"):
        d = od._parse_date_value(row.get(key))
        if d and d.year == year and d.month == month:
            return True
    return False


def _filter_offsets_by_month(
    rows: list[dict[str, Any]], year: int, month: int
) -> list[dict[str, Any]]:
    return [r for r in rows if _offset_row_touches_month(r, year, month)]


def _parse_offset_leave_action_llm(text: str) -> Optional[str]:
    if not _offset_leave_ai_enabled() or not _offset_leave_llm_available():
        return None
    try:
        import chatagent as ca
    except Exception:
        return None
    api_key = ca._llm_api_key()
    if not api_key:
        return None
    model = ca.routing_llm_model()
    print(
        f"[offsetleave] LLM classify start model={model!r} text={(text or '')[:80]!r}",
        flush=True,
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": _OFFSET_LEAVE_LLM_SYSTEM},
            {"role": "user", "content": (text or "").strip()},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    req = urllib.request.Request(
        f"{ca._llm_base_url()}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=ca._llm_timeout_sec()) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        print(f"[offsetleave] offset/leave LLM classify failed: {exc!r}", flush=True)
        return None
    try:
        content = body["choices"][0]["message"]["content"]
        obj = json.loads(content) if isinstance(content, str) else {}
    except (KeyError, IndexError, TypeError, json.JSONDecodeError):
        return None
    action = str((obj or {}).get("action") or "").strip().lower()
    if action == "none" or action not in _OFFSET_LEAVE_ACTIONS:
        return None
    return action


def parse_offset_leave_action(text: str) -> Optional[str]:
    """Map natural language → offset/leave bot action (rules first, then optional LLM)."""
    s = normalize_offset_command_text(text)
    if not s:
        return None
    if looks_like_duty_roster_query(s):
        return None
    if (text or "").strip().startswith("/") and not is_offset_command_text(text):
        return None
    cached = _CLASSIFY_ACTION_CACHE.get(s)
    if s in _CLASSIFY_ACTION_CACHE:
        return cached
    action = _parse_offset_leave_action_rules(s)
    if not action and _OFFSET_LEAVE_SIGNAL_RE.search(s):
        action = _parse_offset_leave_action_llm(s)
    _CLASSIFY_ACTION_CACHE[s] = action
    if len(_CLASSIFY_ACTION_CACHE) > 128:
        _CLASSIFY_ACTION_CACHE.clear()
    if action:
        print(f"[offsetleave] NL action: {s[:100]!r} -> {action}", flush=True)
    return action


def _wants_offset(text: str) -> bool:
    return parse_offset_leave_action(text) == "offset_form"


def _is_month_attendance_slash_command(text: str) -> bool:
    """Bot queries like ``/leave fpms`` — not the OSE leave request form."""
    s = (text or "").strip()
    return bool(
        re.match(
            r"^/(?:leave(?:wfh)?|wfhleave|wholeave|wfh)(?:\s+\S+)?$",
            s,
            re.I,
        )
    )


def _wants_leave(text: str) -> bool:
    return parse_offset_leave_action(text) == "leave_form"


def _fetch_user_display_name(open_id: str, token: str) -> str:
    oid = (open_id or "").strip()
    if not oid:
        return ""
    url = f"https://open.larksuite.com/open-apis/contact/v3/users/{oid}"
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers, params={"user_id_type": "open_id"}, timeout=20).json()
    if res.get("code") != 0:
        return ""
    user = (res.get("data") or {}).get("user") or {}
    for key in ("name", "en_name", "nickname"):
        val = str(user.get(key) or "").strip()
        if val:
            return val
    return ""


def _match_roster_name(display: str) -> Optional[str]:
    raw = (display or "").strip()
    if not raw:
        return None
    titled = od._title_name(raw)
    for roster in od.OSE_LEAVE_FORM_NAMES:
        if titled == od._title_name(roster) or od._names_same_person(roster, raw):
            return od._title_name(roster)
    # e.g. Lark "John Kenneth Chua" → roster "Kenneth"
    display_tokens = {t.lower() for t in od._word_tokens(raw) if len(t) >= 3}
    if not display_tokens:
        return None
    matches: list[str] = []
    for roster in od.OSE_LEAVE_FORM_NAMES:
        rtokens = od._word_tokens(roster)
        if len(rtokens) == 1 and rtokens[0].lower() in display_tokens:
            matches.append(od._title_name(roster))
    if len(matches) == 1:
        return matches[0]
    return None


def _is_roster_name(name: str) -> bool:
    nm = od._title_name(name)
    if not nm:
        return False
    return any(nm == od._title_name(r) for r in od.OSE_LEAVE_FORM_NAMES)


def resolve_request_person(open_id: str, token: str) -> str:
    """Resolve OSE roster name; prefer Bitable/sheet person index over Lark display name."""
    from_index = od.lookup_roster_name_for_open_id(open_id, token)
    if from_index:
        return from_index
    display = _fetch_user_display_name(open_id, token)
    roster = _match_roster_name(display)
    if roster:
        return roster
    raise ValueError(
        f"Could not match your Lark account to an OSE roster name"
        + (f" (Lark name: {display!r})." if display else ".")
        + " Ask an admin to add your open_id under OSE_PERSON_OPEN_IDS."
    )


def try_resolve_request_person(open_id: str, token: str) -> Optional[str]:
    try:
        return resolve_request_person(open_id, token)
    except ValueError:
        return None


def wants_editoffset(text: str) -> bool:
    """``editoffset`` plus natural talk (rules + optional LLM)."""
    return parse_offset_leave_action(text) == "edit_offset"


def _edit_form_session_key(owner_open_id: str, record_id: str) -> str:
    return f"{(owner_open_id or '').strip()}:{(record_id or '').strip()}"


def _clear_edit_forms_for_owner(owner_open_id: str) -> None:
    oid = (owner_open_id or "").strip()
    if not oid:
        return
    prefix = f"{oid}:"
    with _OFFSET_EDIT_OPEN_LOCK:
        for key in list(_OFFSET_EDIT_OPEN.keys()):
            if key.startswith(prefix):
                _OFFSET_EDIT_OPEN.pop(key, None)


def _is_edit_form_open(owner_open_id: str, record_id: str) -> bool:
    return _edit_form_session_key(owner_open_id, record_id) in _OFFSET_EDIT_OPEN


def _mark_edit_form_open(owner_open_id: str, record_id: str, message_id: str = "") -> None:
    key = _edit_form_session_key(owner_open_id, record_id)
    if not key or key == ":":
        return
    with _OFFSET_EDIT_OPEN_LOCK:
        _OFFSET_EDIT_OPEN[key] = (message_id or "open").strip() or "open"


def _clear_edit_form_open(owner_open_id: str, record_id: str) -> None:
    key = _edit_form_session_key(owner_open_id, record_id)
    with _OFFSET_EDIT_OPEN_LOCK:
        _OFFSET_EDIT_OPEN.pop(key, None)


def _delete_session_key(owner_open_id: str, record_id: str) -> str:
    return f"{(owner_open_id or '').strip()}:{(record_id or '').strip()}"


def _try_begin_offset_delete(owner_open_id: str, record_id: str) -> Optional[str]:
    key = _delete_session_key(owner_open_id, record_id)
    if not key or key == ":":
        return "missing record"
    with _OFFSET_DELETE_LOCK:
        if key in _OFFSET_DELETE_IN_FLIGHT:
            return "Delete already in progress for this request — please wait."
        _OFFSET_DELETE_IN_FLIGHT.add(key)
    return None


def _end_offset_delete(owner_open_id: str, record_id: str) -> None:
    key = _delete_session_key(owner_open_id, record_id)
    with _OFFSET_DELETE_LOCK:
        _OFFSET_DELETE_IN_FLIGHT.discard(key)


def _deliver_requester_offset_edit_menu(
    *,
    owner_open_id: str,
    request_person: str,
    group_chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    token: str,
) -> None:
    """editoffset in group/DM: only this user's pending rows (never approver admin list)."""
    rows = _pending_offsets_for_request_person(request_person)
    if not rows:
        send_message(
            group_chat_id,
            f"No pending offset found for **{request_person}**. "
            "Approved or rejected requests cannot be edited with editoffset.",
        )
        return
    if len(rows) == 1:
        card = build_offset_edit_form_card(
            owner_open_id=owner_open_id,
            request_person=request_person,
            row=rows[0],
            is_admin=False,
        )
        _deliver_private_card(
            owner_open_id=owner_open_id,
            group_chat_id=group_chat_id,
            chat_type=chat_type,
            card=card,
            send_message=send_message,
            token=token,
        )
        rid = str(rows[0].get("record_id") or "").strip()
        if rid:
            _mark_edit_form_open(owner_open_id, rid)
        return
    card = build_offset_edit_list_card(owner_open_id, request_person, rows, is_admin=False)
    _deliver_private_card(
        owner_open_id=owner_open_id,
        group_chat_id=group_chat_id,
        chat_type=chat_type,
        card=card,
        send_message=send_message,
        token=token,
    )


def wants_deleteoffset(text: str) -> bool:
    """``deleteoffset`` plus natural talk (rules + optional LLM)."""
    return parse_offset_leave_action(text) == "delete_offset"


def wants_pendingoffset(text: str) -> bool:
    """``pendingoffset`` plus natural talk (rules + optional LLM)."""
    return parse_offset_leave_action(text) == "pending_offset"


def _pending_offsets_for_request_person(request_person: str) -> list[dict[str, Any]]:
    rp = od._title_name(request_person)
    if not rp:
        return []
    data = od.get_ose_offset_records_admin()
    out: list[dict[str, Any]] = []
    for it in (data or {}).get("items") or []:
        if not bool(it.get("pending")):
            continue
        if od._title_name(str(it.get("request_person") or "")) != rp:
            continue
        out.append(dict(it))
    return out


def _non_pending_offsets_all() -> list[dict[str, Any]]:
    """Approved / rejected rows (for approver edit lists)."""
    data = od.get_ose_offset_records_admin()
    out: list[dict[str, Any]] = []
    for it in (data or {}).get("items") or []:
        if bool(it.get("pending")):
            continue
        out.append(dict(it))
    out.sort(
        key=lambda r: (r.get("request_date") or "", r.get("record_id") or ""),
        reverse=True,
    )
    return out


def _all_offsets_for_approver_delete() -> list[dict[str, Any]]:
    """All offset rows approvers may delete (pending + approved + rejected)."""
    od.invalidate_ose_bitable_cache()
    data = od.get_ose_offset_records_admin()
    out: list[dict[str, Any]] = [dict(it) for it in (data or {}).get("items") or []]
    out.sort(
        key=lambda r: (
            0 if bool(r.get("pending")) else 1,
            str(r.get("request_date") or ""),
            str(r.get("record_id") or ""),
        ),
    )
    out.sort(
        key=lambda r: str(r.get("request_date") or ""),
        reverse=True,
    )
    out.sort(key=lambda r: 0 if bool(r.get("pending")) else 1)
    return out


def _all_pending_offsets() -> list[dict[str, Any]]:
    """All pending offset rows (for approver pendingoffset command)."""
    od.invalidate_ose_bitable_cache()
    data = od.get_ose_offset_records_admin()
    out: list[dict[str, Any]] = []
    for it in (data or {}).get("items") or []:
        if bool(it.get("pending")):
            out.append(dict(it))
    out.sort(
        key=lambda r: (
            r.get("request_date") or "",
            r.get("original_date") or "",
            r.get("request_person") or "",
        ),
        reverse=True,
    )
    return out


def _parsed_admin_flag(parsed: dict[str, Any]) -> bool:
    a = parsed.get("admin")
    if a is True:
        return True
    if isinstance(a, (int, float)) and int(a) == 1:
        return True
    return str(a or "").strip().lower() in ("1", "true", "yes")


def _select_options(values: tuple[str, ...] | list[str]) -> list[dict[str, Any]]:
    return [
        {"text": {"tag": "plain_text", "content": str(v)}, "value": str(v)}
        for v in values
    ]


def _callback_payload(
    kind: str,
    *,
    owner_open_id: str,
    request_person: str,
    pick_roster_name: bool = False,
) -> dict[str, str]:
    d = {
        "k": kind,
        "owner": (owner_open_id or "").strip(),
        "request_person": (request_person or "").strip(),
    }
    if pick_roster_name:
        d["pick_roster"] = "1"
    return d


def _roster_name_picker_elements() -> list[dict[str, Any]]:
    return [
        {
            "tag": "div",
            "text": {"tag": "plain_text", "content": "Your name (OSE roster)"},
        },
        {
            "tag": "select_static",
            "name": "request_person",
            "placeholder": {"tag": "plain_text", "content": "Select your roster name"},
            "options": _select_options(od.OSE_LEAVE_FORM_NAMES),
            "required": True,
        },
    ]


def build_offset_form_card(*, owner_open_id: str, request_person: str) -> dict[str, Any]:
    req = od._canonical_roster_form_name(request_person) or od._title_name(request_person)
    if not req:
        raise ValueError("Request person is required for the offset form.")
    exchange_names = list(od.ose_offset_form_exchange_names(exclude_person=req))
    intro = (
        f"**Request person:** {req}\n"
        "Pick **Exchange person** and the other fields below, then tap **Submit**.\n"
        "_**Myself**: Original date = your duty day (D/N); Exchange date = your rest day._\n"
        "_**Shift** must match your duty on **Original date** (N night / D day); "
        "two-person swaps must also match exchange person's duty on **Exchange date**._"
    )
    form_elements: list[dict[str, Any]] = []
    form_elements.extend(
        [
            {
                "tag": "div",
                "text": {"tag": "plain_text", "content": "Exchange person"},
            },
            {
                "tag": "select_static",
                "name": "exchange_person",
                "placeholder": {"tag": "plain_text", "content": "Select exchange person"},
                "options": _select_options(exchange_names),
                "required": True,
            },
            {
                "tag": "div",
                "text": {"tag": "plain_text", "content": "Shift"},
            },
            {
                "tag": "select_static",
                "name": "shift_type",
                "placeholder": {"tag": "plain_text", "content": "N or D"},
                "options": _select_options(od.OSE_SHIFT_TYPES),
                "required": True,
            },
            {
                "tag": "div",
                "text": {"tag": "plain_text", "content": "Original date"},
            },
            {
                "tag": "date_picker",
                "name": "original_date",
                "placeholder": {"tag": "plain_text", "content": "Pick original date"},
                "required": True,
            },
            {
                "tag": "div",
                "text": {"tag": "plain_text", "content": "Exchange date"},
            },
            {
                "tag": "date_picker",
                "name": "exchange_date",
                "placeholder": {"tag": "plain_text", "content": "Pick exchange date"},
                "required": True,
            },
            {
                "tag": "input",
                "name": "reason",
                "input_type": "multiline_text",
                "rows": 4,
                "auto_resize": True,
                "width": "fill",
                "label": {"tag": "plain_text", "content": "Reason"},
                "label_position": "top",
                "placeholder": {"tag": "plain_text", "content": "Reason for offset"},
                "required": True,
                "max_length": 1000,
            },
            {
                "tag": "button",
                "name": "submit_ose_offset",
                "text": {"tag": "plain_text", "content": "Submit"},
                "type": "primary",
                "form_action_type": "submit",
                "behaviors": [
                    {
                        "type": "callback",
                        "value": _callback_payload(
                            _OFFSET_SUBMIT_KEY,
                            owner_open_id=owner_open_id,
                            request_person=req,
                        ),
                    }
                ],
            },
        ]
    )
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "OSE offset request"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": intro},
                },
                {
                    "tag": "form",
                    "name": "ose_offset_form",
                    "elements": form_elements,
                },
            ]
        },
    }


def offset_submit_fingerprint(
    *,
    request_person: str,
    exchange_person: str,
    shift_type: str,
    original_date: date,
    exchange_date: date,
    reason: str,
) -> str:
    parts = [
        od._title_name(request_person),
        od._title_name(exchange_person),
        (shift_type or "").strip().upper(),
        original_date.isoformat(),
        exchange_date.isoformat(),
        (reason or "").strip().lower(),
    ]
    return "|".join(parts)


def try_begin_offset_submit(actor_key: str, fingerprint: str) -> Optional[str]:
    """
    Reserve a submit slot for ``actor_key`` (Lark open_id or ``web:{name}``).
    Returns an error message when duplicate / in-flight; ``None`` if OK to proceed.
    """
    actor = (actor_key or "").strip()
    if not actor:
        actor = "unknown"
    fp = (fingerprint or "").strip()
    dedupe_key = f"{actor}:{fp}"
    now = time.monotonic()
    with _OFFSET_SUBMIT_LOCK:
        if actor in _OFFSET_SUBMIT_IN_FLIGHT:
            return "Your offset submit is already processing. Please wait."
        last = _OFFSET_SUBMIT_RECENT.get(dedupe_key, 0.0)
        if fp and now - last < _OFFSET_SUBMIT_DEDUPE_SEC:
            return (
                "Duplicate submit ignored — the same offset was just saved. "
                "Check your pending list or wait a minute before submitting again."
            )
        _OFFSET_SUBMIT_IN_FLIGHT.add(actor)
    return None


def release_offset_submit(actor_key: str, fingerprint: str, *, success: bool) -> None:
    actor = (actor_key or "").strip() or "unknown"
    fp = (fingerprint or "").strip()
    dedupe_key = f"{actor}:{fp}"
    with _OFFSET_SUBMIT_LOCK:
        _OFFSET_SUBMIT_IN_FLIGHT.discard(actor)
        if success and fp:
            _OFFSET_SUBMIT_RECENT[dedupe_key] = time.monotonic()
            stale_before = time.monotonic() - max(_OFFSET_SUBMIT_DEDUPE_SEC * 4, 120)
            for k, ts in list(_OFFSET_SUBMIT_RECENT.items()):
                if ts < stale_before:
                    _OFFSET_SUBMIT_RECENT.pop(k, None)


def build_offset_edit_saved_card(
    *,
    request_person: str,
    row: dict[str, Any],
) -> dict[str, Any]:
    md = _offset_approval_table_md(
        row,
        status=str(row.get("approval_status") or "Pending"),
        intro=f"**{_lark_md_cell(request_person)}** — your pending offset was **saved**.",
    )
    return {
        "schema": "2.0",
        "config": {"width_mode": "fill"},
        "header": {
            "template": "green",
            "title": {"tag": "plain_text", "content": "OSE offset — saved"},
        },
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}]},
    }


def _finish_ephemeral_card_ui(
    *,
    owner_open_id: str,
    chat_id: str,
    card: dict[str, Any],
    message_id: str,
    send_message: Callable[..., Any],
    token: str,
    fallback_text: str,
) -> None:
    """Show result after save/submit — group ephemeral cards often cannot be PATCHed."""
    mid = (message_id or "").strip()
    if _try_patch_interactive_card_message(mid, card):
        return
    _dismiss_ephemeral_form(mid)
    cid = (chat_id or "").strip()
    oid = (owner_open_id or "").strip()
    if cid and oid:
        try:
            _send_ephemeral_card(cid, oid, card, token)
            return
        except Exception as exc:
            print(f"[offsetleave] ephemeral result card failed: {exc!r}", flush=True)
    if cid and fallback_text:
        send_message(chat_id, fallback_text)


def _finish_offset_edit_saved_ui(
    *,
    owner_open_id: str,
    request_person: str,
    row: dict[str, Any],
    message_id: str,
    chat_id: str,
    send_message: Callable[..., Any],
    token: str,
) -> None:
    card = build_offset_edit_saved_card(request_person=request_person, row=row)
    _finish_ephemeral_card_ui(
        owner_open_id=owner_open_id,
        chat_id=chat_id,
        card=card,
        message_id=message_id,
        send_message=send_message,
        token=token,
        fallback_text=f"✅ Offset updated for {request_person}.",
    )


def build_offset_submit_done_card(
    *,
    request_person: str,
    record_id: str,
    message: str,
) -> dict[str, Any]:
    rid = (record_id or "").strip() or "—"
    body = (message or "Offset submitted.").strip()
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "green",
            "title": {"tag": "plain_text", "content": "OSE offset — submitted"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**Request person:** {_lark_md_cell(request_person)}\n\n{body}\n\n**Record:** `{_lark_md_cell(rid)}`",
                    },
                }
            ]
        },
    }


def build_leave_form_card(
    *, owner_open_id: str, request_person: str, pick_roster_name: bool = False
) -> dict[str, Any]:
    intro = (
        "Select your roster name, then fill the fields below and tap **Submit**."
        if pick_roster_name
        else (f"**Name:** {request_person}\n" "Fill the fields below, then tap **Submit**.")
    )
    form_elements: list[dict[str, Any]] = []
    if pick_roster_name:
        form_elements.extend(_roster_name_picker_elements())
    form_elements.extend(
        [
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Leave type"},
                        },
                        {
                            "tag": "select_static",
                            "name": "leave_type",
                            "placeholder": {"tag": "plain_text", "content": "Select leave type"},
                            "options": _select_options(od.OSE_LEAVE_TYPES),
                            "required": True,
                        },
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Start date"},
                        },
                        {
                            "tag": "date_picker",
                            "name": "start_date",
                            "placeholder": {"tag": "plain_text", "content": "Pick start date"},
                            "required": True,
                        },
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "End date"},
                        },
                        {
                            "tag": "date_picker",
                            "name": "end_date",
                            "placeholder": {"tag": "plain_text", "content": "Pick end date"},
                            "required": True,
                        },
                        {
                            "tag": "input",
                            "name": "reason",
                            "input_type": "multiline_text",
                            "rows": 4,
                            "auto_resize": True,
                            "width": "fill",
                            "label": {"tag": "plain_text", "content": "Reason"},
                            "label_position": "top",
                            "placeholder": {"tag": "plain_text", "content": "Reason for leave"},
                            "required": True,
                            "max_length": 1000,
                        },
                        {
                            "tag": "button",
                            "name": "submit_ose_leave",
                            "text": {"tag": "plain_text", "content": "Submit"},
                            "type": "primary",
                            "form_action_type": "submit",
                            "behaviors": [
                                {
                                    "type": "callback",
                                    "value": _callback_payload(
                                        _LEAVE_SUBMIT_KEY,
                                        owner_open_id=owner_open_id,
                                        request_person=request_person,
                                        pick_roster_name=pick_roster_name,
                                    ),
                                }
                            ],
                        },
        ]
    )
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "OSE leave request"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": intro},
                },
                {
                    "tag": "form",
                    "name": "ose_leave_form",
                    "elements": form_elements,
                },
            ]
        },
    }


def _short_cell(s: Any) -> str:
    t = str(s or "").replace("\n", " ").replace("|", "/").strip()
    if len(t) > 240:
        return t[:240] + "…"
    return t or "—"


def _offset_shift_labels(shift_type: Any) -> tuple[str, str]:
    """Return (short D/N, long Day Shift / Night Shift)."""
    code = str(shift_type or "").strip().upper()
    if code == "D":
        return "D", "Day Shift"
    if code == "N":
        return "N", "Night Shift"
    return code or "—", code or "—"


def _format_offset_delete_row_summary(
    index: int,
    row: dict[str, Any],
    *,
    is_admin: bool,
) -> str:
    """Human-readable swap line for the delete picker card."""
    req = _short_cell(row.get("request_person"))
    exc = _short_cell(row.get("exchange_person"))
    orig = _short_cell(row.get("original_date"))
    exch = _short_cell(row.get("exchange_date"))
    shift_short, shift_long = _offset_shift_labels(row.get("shift_type"))
    reason = _short_cell(row.get("reason"))
    self_swap = od._names_same_person(
        str(row.get("request_person") or ""),
        str(row.get("exchange_person") or ""),
    )
    if self_swap:
        headline = f"**{index}.** {req} changed himself · {shift_short} · {orig} → {exch}"
        lines = [headline, f"**Reason:** {reason}"]
    else:
        headline = f"**{index}.** {req} changed his {orig} → {exc} {exch}"
        lines = [headline, shift_long, f"**Reason:** {reason}"]
    if is_admin:
        st = _short_cell(row.get("approval_status"))
        if bool(row.get("pending")) and not st:
            st = "Pending"
        lines.append(f"**Requester:** {req} · **Status:** {st}")
    return "\n".join(lines)


def _callback_payload_edit_submit(
    *,
    owner_open_id: str,
    record_id: str,
    request_person: str = "",
    admin: bool = False,
) -> dict[str, Any]:
    d: dict[str, Any] = {
        "k": _OFFSET_EDIT_SUBMIT_KEY,
        "owner": (owner_open_id or "").strip(),
        "record_id": (record_id or "").strip(),
    }
    rp = (request_person or "").strip()
    if rp:
        d["request_person"] = rp
    if admin:
        d["admin"] = 1
    return d


def _callback_payload_row_action(
    kind: str,
    *,
    owner_open_id: str,
    request_person: str,
    record_id: str,
    admin: bool = False,
    month_ym: Optional[tuple[int, int]] = None,
    start: int = 0,
) -> dict[str, Any]:
    d: dict[str, Any] = {
        "k": kind,
        "owner": (owner_open_id or "").strip(),
        "request_person": (request_person or "").strip(),
        "record_id": (record_id or "").strip(),
    }
    if admin:
        d["admin"] = 1
    # Carry the month/page so the card rebuilt AFTER a delete stays on the same
    # month and page instead of falling back to the unfiltered list.
    if month_ym:
        d["y"], d["m"] = int(month_ym[0]), int(month_ym[1])
    if start:
        d["off"] = int(start)
    return d


def _row_datepicker_initial(cell: Any) -> str:
    raw = str(cell or "").strip()
    if not raw:
        return ""
    try:
        return _parse_date_iso(raw).isoformat()
    except Exception:
        return ""


def build_offset_edit_list_card(
    owner_open_id: str,
    request_person: str,
    rows: list[dict[str, Any]],
    *,
    is_admin: bool = False,
    month_label: Optional[str] = None,
    filter_label: Optional[str] = None,
) -> dict[str, Any]:
    cap = 15
    sliced = rows[:cap]
    month_note = f" ({month_label})" if month_label else ""
    filter_note = f"\n_Filter: **{filter_label}**_" if filter_label else ""
    if is_admin:
        intro = (
            f"**Approver** — edit **approved** or **rejected** offsets (all requesters)"
            f"{month_note}{filter_note}."
        )
        cap_note = "non-pending"
    else:
        intro = (
            f"**{request_person}** — pending offset requests you can **edit**"
            f"{month_note}{filter_note}.\n"
            "Approved / rejected rows are not listed."
        )
        cap_note = "pending"
    elements: list[dict[str, Any]] = [{"tag": "div", "text": {"tag": "lark_md", "content": intro}}]
    if len(rows) > cap:
        elements.append(
            {
                "tag": "div",
                "text": {"tag": "plain_text", "content": f"(Showing first {cap} of {len(rows)} {cap_note}.)"},
            }
        )
    for i, r in enumerate(sliced, start=1):
        rid = str(r.get("record_id") or "").strip()
        if not rid:
            continue
        extra = ""
        if is_admin:
            extra = f"\n**Requester:** {_short_cell(r.get('request_person'))} · **Status:** {_short_cell(r.get('approval_status'))}"
        summary = (
            f"**{i}.** {_short_cell(r.get('exchange_person'))} · **{_short_cell(r.get('shift_type'))}** · "
            f"{_short_cell(r.get('original_date'))} → {_short_cell(r.get('exchange_date'))}\n"
            f"**Reason:** {_short_cell(r.get('reason'))}{extra}"
        )
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": summary}})
        elements.append(
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "Edit"},
                "type": "primary",
                "behaviors": [
                    {
                        "type": "callback",
                        "value": _callback_payload_row_action(
                            _OFFSET_EDIT_PICK_KEY,
                            owner_open_id=owner_open_id,
                            request_person=request_person,
                            record_id=rid,
                            admin=is_admin,
                        ),
                    }
                ],
            }
        )
    title = "OSE offset — edit (approver)" if is_admin else "OSE offset — edit"
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "blue", "title": {"tag": "plain_text", "content": title}},
        "body": {"elements": elements},
    }


# Rows per delete card. A month with more than this gets "Show next" paging, so
# nothing is hidden; keeping it bounded keeps the card payload well inside Lark's
# size limit (each row is a summary div + a Delete button).
_OFFSET_DELETE_PAGE = 25


def _offset_months_present(rows: list[dict[str, Any]]) -> list[tuple[int, int, int]]:
    """Months that actually have offsets → ``[(year, month, row_count)]``, newest first.

    A row is counted under EVERY month it touches (original / exchange / request
    date), matching :func:`_offset_row_touches_month`, so clicking any of those
    months finds the row. Months with no offsets never appear.
    """
    counts: dict[tuple[int, int], int] = {}
    for r in rows:
        seen: set[tuple[int, int]] = set()
        for key in ("original_date", "exchange_date", "request_date"):
            d = od._parse_date_value(r.get(key))
            if d:
                seen.add((d.year, d.month))
        for ym in seen:
            counts[ym] = counts.get(ym, 0) + 1
    return [(y, m, c) for (y, m), c in sorted(counts.items(), reverse=True)]


def build_offset_delete_month_picker_card(
    owner_open_id: str, rows: list[dict[str, Any]]
) -> dict[str, Any]:
    """Approver deleteoffset step 1 — one button per month that has offsets.

    Replaces the old flat list, which was capped at 15 rows and silently hid the
    rest. Picking a month shows only that month, so the cap stops mattering.
    """
    months = _offset_months_present(rows)
    total = len(rows)
    elements: list[dict[str, Any]] = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**Approver** — pick a **month** to delete offsets from.\n"
                    f"{total} offset record(s) across {len(months)} month(s)."
                ),
            },
        }
    ]
    if not months:
        elements.append(
            {"tag": "div", "text": {"tag": "lark_md", "content": "_No offset records._"}}
        )
    for y, m, c in months:
        elements.append(
            {
                "tag": "button",
                "text": {
                    "tag": "plain_text",
                    "content": f"{_month_filter_label(y, m)}  ({c})",
                },
                "type": "primary",
                "behaviors": [
                    {
                        "type": "callback",
                        "value": {
                            "k": _OFFSET_DELETE_MONTH_KEY,
                            "owner": (owner_open_id or "").strip(),
                            "y": y,
                            "m": m,
                        },
                    }
                ],
            }
        )
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "OSE offset — delete (approver)"},
        },
        "body": {"elements": elements},
    }


def build_offset_delete_list_card(
    owner_open_id: str,
    request_person: str,
    rows: list[dict[str, Any]],
    *,
    is_admin: bool = False,
    month_label: Optional[str] = None,
    filter_label: Optional[str] = None,
    month_ym: Optional[tuple[int, int]] = None,
    start: int = 0,
) -> dict[str, Any]:
    # One card holds a page of rows; anything beyond gets a "Show next" button, so
    # a month with more offsets than fit is never silently truncated.
    page = _OFFSET_DELETE_PAGE
    total = len(rows)
    start = max(0, min(int(start or 0), max(0, total - 1)))
    sliced = rows[start : start + page]
    month_note = f" ({month_label})" if month_label else ""
    filter_note = f"\n_Filter: **{filter_label}**_" if filter_label else ""
    if is_admin:
        intro = (
            f"**Approver** — pick an offset to **delete**{month_note}{filter_note}\n"
            "(pending, approved, or rejected — removes the Bitable row)."
        )
        cap_note = "record(s)"
    else:
        intro = (
            f"**{request_person}** — pending offset requests{month_note} you can **delete**.\n"
            "Approved / rejected rows are not listed."
        )
        cap_note = "pending"
    elements: list[dict[str, Any]] = [{"tag": "div", "text": {"tag": "lark_md", "content": intro}}]
    if total > len(sliced):
        first, last = start + 1, start + len(sliced)
        elements.append(
            {
                "tag": "div",
                "text": {
                    "tag": "plain_text",
                    "content": f"Showing {first}–{last} of {total} {cap_note}.",
                },
            }
        )
    for i, r in enumerate(sliced, start=start + 1):
        rid = str(r.get("record_id") or "").strip()
        if not rid:
            continue
        summary = _format_offset_delete_row_summary(i, r, is_admin=is_admin)
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": summary}})
        elements.append(
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "Delete"},
                "type": "danger",
                "behaviors": [
                    {
                        "type": "callback",
                        "value": _callback_payload_row_action(
                            _OFFSET_DELETE_KEY,
                            owner_open_id=owner_open_id,
                            request_person=request_person,
                            record_id=rid,
                            admin=is_admin,
                            month_ym=month_ym,
                            start=start,
                        ),
                    }
                ],
            }
        )
    # Paging / back controls need a month to re-query, so they only apply to the
    # approver month flow.
    if month_ym:
        nav: list[dict[str, Any]] = []
        if start + page < total:
            remaining = total - (start + page)
            nav.append(
                {
                    "tag": "button",
                    "text": {
                        "tag": "plain_text",
                        "content": f"Show next {min(page, remaining)} ▶",
                    },
                    "type": "primary",
                    "behaviors": [
                        {
                            "type": "callback",
                            "value": {
                                "k": _OFFSET_DELETE_MONTH_KEY,
                                "owner": (owner_open_id or "").strip(),
                                "y": int(month_ym[0]),
                                "m": int(month_ym[1]),
                                "off": start + page,
                            },
                        }
                    ],
                }
            )
        if start > 0:
            nav.append(
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "◀ Previous"},
                    "type": "default",
                    "behaviors": [
                        {
                            "type": "callback",
                            "value": {
                                "k": _OFFSET_DELETE_MONTH_KEY,
                                "owner": (owner_open_id or "").strip(),
                                "y": int(month_ym[0]),
                                "m": int(month_ym[1]),
                                "off": max(0, start - page),
                            },
                        }
                    ],
                }
            )
        nav.append(
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "◀ All months"},
                "type": "default",
                "behaviors": [
                    {
                        "type": "callback",
                        "value": {
                            "k": _OFFSET_DELETE_MONTH_KEY,
                            "owner": (owner_open_id or "").strip(),
                            "all": 1,
                        },
                    }
                ],
            }
        )
        elements.extend(nav)
    title = "OSE offset — delete (approver)" if is_admin else "OSE offset — delete"
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "orange", "title": {"tag": "plain_text", "content": title}},
        "body": {"elements": elements},
    }


def build_offset_pending_list_card(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Approver view of all pending offsets (read + Approve/Reject per row)."""
    cap = 12
    total = len(rows)
    sliced = rows[:cap]
    intro = (
        f"**{total} pending offset request(s)** awaiting approval.\n"
        "Tap **Approve** or **Reject** on a row, add optional **Remarks**, then **Confirm**."
    )
    elements: list[dict[str, Any]] = [{"tag": "div", "text": {"tag": "lark_md", "content": intro}}]
    if total > cap:
        elements.append(
            {
                "tag": "motion",
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "plain_text",
                            "content": f"(Showing first {cap} of {total} pending — run pendingoffset again after clearing some.)",
                        },
                    }
                ],
            }
        )
    if not sliced:
        elements.append(
            {
                "tag": "div",
                "text": {"tag": "plain_text", "content": "No pending offset requests right now."},
            }
        )
    for i, r in enumerate(sliced, start=1):
        rid = str(r.get("record_id") or "").strip()
        if not rid:
            continue
        rp = _short_cell(r.get("request_person"))
        summary = (
            f"**{i}. {rp}** · {_short_cell(r.get('exchange_person'))} · "
            f"**{_short_cell(r.get('shift_type'))}** · "
            f"{_short_cell(r.get('original_date'))} → {_short_cell(r.get('exchange_date'))}\n"
            f"**Req. date:** {_short_cell(r.get('request_date'))} · **Reason:** {_short_cell(r.get('reason'))}"
        )
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": summary}})
        approve_val = {"k": _OFFSET_APPR_PICK_KEY, "record_id": rid, "decision": "Approved"}
        reject_val = {"k": _OFFSET_APPR_PICK_KEY, "record_id": rid, "decision": "Rejected"}
        elements.append(_approval_pick_button_row(approve_val, reject_val))
        elements.append({"tag": "hr"})
    if elements and elements[-1].get("tag") == "hr":
        elements.pop()
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "wathet",
            "title": {"tag": "plain_text", "content": "OSE offset — pending queue"},
        },
        "body": {"elements": elements},
    }


def build_offset_edit_form_card(
    *,
    owner_open_id: str,
    request_person: str,
    row: dict[str, Any],
    is_admin: bool = False,
) -> dict[str, Any]:
    rid = str(row.get("record_id") or "").strip()
    req_on_row = str(row.get("request_person") or request_person or "").strip()
    exchange_names = list(
        od.ose_offset_form_exchange_names(exclude_person=req_on_row)
    )
    exc_on_row = str(row.get("exchange_person") or "").strip()
    if od._title_name(exc_on_row) and od._title_name(exc_on_row) == od._title_name(req_on_row):
        exc_on_row = od.OFFSET_EXCHANGE_MYSELF_LABEL
    o_ini = _row_datepicker_initial(row.get("original_date"))
    x_ini = _row_datepicker_initial(row.get("exchange_date"))
    shift_on_row = str(row.get("shift_type") or "").strip().upper()
    reason_on_row = str(row.get("reason") or "").strip()
    original_dp: dict[str, Any] = {
        "tag": "date_picker",
        "name": "original_date",
        "placeholder": {"tag": "plain_text", "content": "Pick original date"},
        "required": True,
    }
    if o_ini:
        original_dp["initial_date"] = o_ini
    exchange_dp: dict[str, Any] = {
        "tag": "date_picker",
        "name": "exchange_date",
        "placeholder": {"tag": "plain_text", "content": "Pick exchange date"},
        "required": True,
    }
    if x_ini:
        exchange_dp["initial_date"] = x_ini
    status_line = ""
    if is_admin:
        status_line = f"\n**Status:** {_short_cell(row.get('approval_status'))}"
    date_lines = ""
    if row.get("original_date") or row.get("exchange_date"):
        date_lines = (
            f"\n**Original date:** {_short_cell(row.get('original_date'))}"
            f"\n**Exchange date:** {_short_cell(row.get('exchange_date'))}"
        )
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "OSE offset — edit request (approver)" if is_admin else "OSE offset — edit request"},
        },
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**Request person:** {req_on_row}\n"
                            f"**Record:** `{_short_cell(rid)}`{status_line}{date_lines}\n"
                            "Update the fields below, then tap **Save**."
                        ),
                    },
                },
                {
                    "tag": "form",
                    "name": "ose_offset_edit_form",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Exchange person"},
                        },
                        {
                            "tag": "select_static",
                            "name": "exchange_person",
                            "placeholder": {"tag": "plain_text", "content": "Select exchange person"},
                            "options": _select_options(exchange_names),
                            "required": True,
                            **({"initial_option": exc_on_row} if exc_on_row in exchange_names else {}),
                        },
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Shift"},
                        },
                        {
                            "tag": "select_static",
                            "name": "shift_type",
                            "placeholder": {"tag": "plain_text", "content": "N or D"},
                            "options": _select_options(od.OSE_SHIFT_TYPES),
                            "required": True,
                            **(
                                {"initial_option": shift_on_row}
                                if shift_on_row in od.OSE_SHIFT_TYPES
                                else {}
                            ),
                        },
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Original date"},
                        },
                        original_dp,
                        {
                            "tag": "div",
                            "text": {"tag": "plain_text", "content": "Exchange date"},
                        },
                        exchange_dp,
                        {
                            "tag": "input",
                            "name": "reason",
                            "input_type": "multiline_text",
                            "rows": 4,
                            "auto_resize": True,
                            "width": "fill",
                            "label": {"tag": "plain_text", "content": "Reason"},
                            "label_position": "top",
                            "placeholder": {"tag": "plain_text", "content": "Reason for offset"},
                            "required": True,
                            "max_length": 1000,
                            **({"value": reason_on_row} if reason_on_row else {}),
                        },
                        {
                            "tag": "button",
                            "name": "submit_ose_offset_edit",
                            "text": {"tag": "plain_text", "content": "Save"},
                            "type": "primary",
                            "form_action_type": "submit",
                            "behaviors": [
                                {
                                    "type": "callback",
                                    "value": _callback_payload_edit_submit(
                                        owner_open_id=owner_open_id,
                                        record_id=rid,
                                        request_person=req_on_row,
                                        admin=is_admin,
                                    ),
                                }
                            ],
                        },
                    ],
                },
            ]
        },
    }


def _send_ephemeral_card(
    chat_id: str,
    open_id: str,
    card: dict[str, Any],
    token: str,
) -> None:
    cid = (chat_id or "").strip()
    oid = (open_id or "").strip()
    if not cid or not oid:
        raise ValueError("chat_id and open_id are required for a group-only form")
    url = "https://open.larksuite.com/open-apis/ephemeral/v1/send"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    body = {
        "chat_id": cid,
        "open_id": oid,
        "msg_type": "interactive",
        "card": card,
    }
    res = requests.post(url, headers=headers, json=body, timeout=20).json()
    if res.get("code") != 0:
        raise RuntimeError(f"Failed to send group-only form: {res}")


def _deliver_private_card(
    *,
    owner_open_id: str,
    group_chat_id: str,
    chat_type: Optional[str],
    card: dict[str, Any],
    send_message: Callable[..., dict[str, Any]],
    token: str,
) -> None:
    if (chat_type or "").strip().lower() == "group":
        _send_ephemeral_card(group_chat_id, owner_open_id, card, token)
        return
    card_json = json.dumps(card, ensure_ascii=False)
    resp = send_message(group_chat_id, card_json, msg_type="interactive")
    if isinstance(resp, dict) and int(resp.get("code", -1)) != 0:
        raise RuntimeError(f"Failed to send form: {resp}")


def wants_offset_request(text: str) -> bool:
    return _wants_offset(text)


def _bot_menu_operator_open_id(data: dict[str, Any]) -> str:
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    op = ev.get("operator") if isinstance(ev.get("operator"), dict) else {}
    oid_wrap = op.get("operator_id") if isinstance(op.get("operator_id"), dict) else {}
    return str(oid_wrap.get("open_id") or "").strip()


def _bot_menu_event_key(data: dict[str, Any]) -> str:
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    return str(ev.get("event_key") or "").strip()


def handle_bot_menu_event(
    data: dict[str, Any],
    *,
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """
    Handle ``application.bot.menu_v6`` push events.

    Returns True when the payload was a bot-menu event (handled or logged); False if not menu-shaped.
    """
    if not isinstance(data, dict) or not isinstance(data.get("event"), dict):
        return False
    event_key = _bot_menu_event_key(data)
    if not event_key:
        print("[offsetleave] bot menu: missing event_key", flush=True)
        return True
    oid = _bot_menu_operator_open_id(data)
    if not oid:
        print(f"[offsetleave] bot menu {event_key!r}: missing operator open_id", flush=True)
        return True

    def _send_open_id(_chat_id: str, text: str, **kwargs: Any) -> dict[str, Any]:
        return send_message(
            oid,
            text,
            receive_id_type="open_id",
            msg_type=str(kwargs.get("msg_type") or "text"),
            mentions=kwargs.get("mentions"),
        )

    if event_key in BOT_MENU_EVENT_KEYS_OFFSET_FORM:
        print(f"[offsetleave] bot menu {event_key!r} → offset form for {oid}", flush=True)
        _open_offset_form(
            sender_open_id=oid,
            chat_id=oid,
            chat_type="p2p",
            send_message=_send_open_id,
            get_token_func=get_token_func,
        )
        return True

    if event_key in BOT_MENU_EVENT_KEYS_OFFSET_DELETE:
        print(f"[offsetleave] bot menu {event_key!r} → delete offset for {oid}", flush=True)
        _open_offset_delete_picker(
            sender_open_id=oid,
            chat_id=oid,
            chat_type="p2p",
            send_message=_send_open_id,
            get_token_func=get_token_func,
        )
        return True

    if event_key in BOT_MENU_EVENT_KEYS_SHOW_OFFSET:
        print(f"[offsetleave] bot menu {event_key!r} → show offset for {oid}", flush=True)
        _open_show_offset_calendar(
            sender_open_id=oid,
            chat_id=oid,
            send_message=_send_open_id,
            get_token_func=get_token_func,
        )
        return True

    if event_key in BOT_MENU_EVENT_KEYS_OFFSET_EDIT:
        print(f"[offsetleave] bot menu {event_key!r} → edit offset for {oid}", flush=True)
        _open_offset_edit_picker(
            sender_open_id=oid,
            chat_id=oid,
            chat_type="p2p",
            send_message=_send_open_id,
            get_token_func=get_token_func,
        )
        return True

    if event_key in BOT_MENU_EVENT_KEYS_PENDING_OFFSET:
        print(f"[offsetleave] bot menu {event_key!r} → pending offset queue for {oid}", flush=True)
        _open_pending_offset_queue(
            sender_open_id=oid,
            chat_id=oid,
            chat_type="p2p",
            send_message=_send_open_id,
            get_token_func=get_token_func,
        )
        return True

    if event_key in BOT_MENU_EVENT_KEYS_APPROVER_OFFSET_EDIT:
        print(f"[offsetleave] bot menu {event_key!r} → approver edit offset for {oid}", flush=True)
        _open_approver_offset_edit_picker(
            sender_open_id=oid,
            chat_id=oid,
            chat_type="p2p",
            send_message=_send_open_id,
            get_token_func=get_token_func,
        )
        return True

    if event_key in BOT_MENU_EVENT_KEYS_APPROVER_OFFSET_DELETE:
        print(f"[offsetleave] bot menu {event_key!r} → approver delete offset for {oid}", flush=True)
        _open_approver_offset_delete_picker(
            sender_open_id=oid,
            chat_id=oid,
            chat_type="p2p",
            send_message=_send_open_id,
            get_token_func=get_token_func,
        )
        return True

    if event_key in BOT_MENU_EVENT_KEYS_APPROVER_SHOW_OFFSET:
        print(f"[offsetleave] bot menu {event_key!r} → approver show offset for {oid}", flush=True)
        _open_approver_show_offset(
            sender_open_id=oid,
            chat_id=oid,
            send_message=_send_open_id,
            get_token_func=get_token_func,
        )
        return True

    print(f"[offsetleave] bot menu: unhandled event_key={event_key!r}", flush=True)
    return True


def _open_pending_offset_queue(
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """Approver menu: pending offset approval queue (same as ``pendingoffset``)."""
    oid = (sender_open_id or "").strip()
    if not _is_offset_approver_open_id(oid):
        send_message(chat_id, "As checked you are not Approver")
        return True
    return handle_pendingoffset_command(
        "pendingoffset",
        sender_open_id=oid,
        chat_id=chat_id,
        chat_type=chat_type,
        send_message=send_message,
        get_token_func=get_token_func,
        force=True,
    )


def _open_approver_offset_edit_picker(
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """Approver menu: edit approved/rejected offset rows (skips requester pending menu)."""
    oid = (sender_open_id or "").strip()
    if not _is_offset_approver_open_id(oid):
        send_message(chat_id, "As checked you are not Approver")
        return True
    return handle_editoffset_command(
        "editoffset",
        sender_open_id=oid,
        chat_id=chat_id,
        chat_type=chat_type,
        send_message=send_message,
        get_token_func=get_token_func,
        force=True,
        person_filter="",
        status_filter="",
        approver_admin_only=True,
    )


def _open_approver_offset_delete_picker(
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """Approver menu: delete any offset row (approver-only delete list)."""
    oid = (sender_open_id or "").strip()
    if not _is_offset_approver_open_id(oid):
        send_message(chat_id, "As checked you are not Approver")
        return True
    return handle_deleteoffset_command(
        "deleteoffset",
        sender_open_id=oid,
        chat_id=chat_id,
        chat_type=chat_type,
        send_message=send_message,
        get_token_func=get_token_func,
        force=True,
        person_filter="",
        status_filter="",
    )


def _open_approver_show_offset(
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """Approver menu: show the full-team offset calendar (approver view of ``showoffset``)."""
    oid = (sender_open_id or "").strip()
    if not _is_offset_approver_open_id(oid):
        send_message(chat_id, "As checked you are not Approver")
        return True
    return _open_show_offset_calendar(
        sender_open_id=oid,
        chat_id=chat_id,
        send_message=send_message,
        get_token_func=get_token_func,
    )


def _open_offset_delete_picker(
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """Open the offset delete list card (same as ``deleteoffset`` without NL filters)."""
    return handle_deleteoffset_command(
        "deleteoffset",
        sender_open_id=sender_open_id,
        chat_id=chat_id,
        chat_type=chat_type,
        send_message=send_message,
        get_token_func=get_token_func,
        force=True,
        person_filter="",
        status_filter="",
    )


def _open_offset_edit_picker(
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """Open the offset edit list card (same as ``editoffset`` without NL filters)."""
    return handle_editoffset_command(
        "editoffset",
        sender_open_id=sender_open_id,
        chat_id=chat_id,
        chat_type=chat_type,
        send_message=send_message,
        get_token_func=get_token_func,
        force=True,
        person_filter="",
        status_filter="",
    )


def _open_show_offset_calendar(
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """Show offset calendar for the current month (same as bare ``showoffset``)."""
    return handle_showoffset(
        "showoffset",
        chat_id=chat_id,
        send_message=send_message,
        sender_open_id=sender_open_id,
        get_token_func=get_token_func,
    )


def _open_offset_form(
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """Open the OSE duty offset (shift swap) form card for the sender."""
    oid = (sender_open_id or "").strip()
    if not oid:
        send_message(chat_id, "❌ Could not identify your Lark user for a private form.")
        return True
    try:
        token = get_token_func()
    except Exception as e:
        send_message(chat_id, f"❌ {e}")
        return True
    try:
        offset_request_person = resolve_request_person(oid, token)
    except ValueError as e:
        send_message(chat_id, f"❌ {e}")
        return True
    _deliver_private_card(
        owner_open_id=oid,
        group_chat_id=chat_id,
        chat_type=chat_type,
        card=build_offset_form_card(
            owner_open_id=oid,
            request_person=offset_request_person,
        ),
        send_message=send_message,
        token=token,
    )
    return True


def handle_offset_form_command(
    clean_text: str,
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """Handle ``/offset`` — open the shift-swap form (not chat, not dept ``/leave``)."""
    text = normalize_offset_command_text(clean_text)
    if not _OFFSET_FORM_SLASH_RE.match(text):
        return False
    return _open_offset_form(
        sender_open_id=sender_open_id,
        chat_id=chat_id,
        chat_type=chat_type,
        send_message=send_message,
        get_token_func=get_token_func,
    )


def handle_offset_slash_commands(
    clean_text: str,
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """Handle ``/offset``, ``/deleteoffset``, ``/editoffset``, etc. — rules only, never LLM."""
    text = normalize_offset_command_text(clean_text)
    if not is_offset_command_text(clean_text):
        return False
    if _OFFSET_FORM_SLASH_RE.match(text):
        return handle_offset_form_command(
            text,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_token_func,
        )
    if _DELETE_OFFSET_SLASH_RE.match(text):
        return handle_deleteoffset_command(
            text,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_token_func,
            force=True,
        )
    if _EDIT_OFFSET_SLASH_RE.match(text):
        return handle_editoffset_command(
            text,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_token_func,
            force=True,
        )
    if _PENDING_OFFSET_SLASH_RE.match(text):
        return handle_pendingoffset_command(
            text,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_token_func,
            force=True,
        )
    if _SHOW_OFFSET_SLASH_RE.match(text):
        return handle_showoffset(
            text,
            chat_id=chat_id,
            send_message=send_message,
            sender_open_id=sender_open_id,
            get_token_func=get_token_func,
        )
    return False


def _showoffset_view_for_sender(
    sender_open_id: str,
    get_token_func: Optional[Callable[[], str]],
) -> tuple[Optional[str], bool]:
    """
    Return ``(involved_person, include_all_team)`` for showoffset display.

    - Requester only → your rows (as requester or exchange person)
    - Approver only → full team
    - Both → your section + full team section
    """
    oid = (sender_open_id or "").strip()
    if not oid:
        return None, False
    is_approver = _is_offset_approver_open_id(oid)
    request_person: Optional[str] = None
    if get_token_func:
        try:
            request_person = try_resolve_request_person(oid, get_token_func())
        except Exception:
            request_person = None
    if is_approver and request_person:
        return request_person, True
    if is_approver:
        return None, False
    if request_person:
        return request_person, False
    return None, False


def handle_showoffset(
    clean_text: str,
    *,
    chat_id: str,
    send_message: Callable[..., dict[str, Any]],
    sender_open_id: str = "",
    get_token_func: Optional[Callable[[], str]] = None,
) -> bool:
    text = normalize_offset_command_text(clean_text)
    try:
        target = od.parse_showoffset_command(text)
    except ValueError as exc:
        send_message(chat_id, f"❌ {exc}")
        return True
    if target is None and parse_offset_leave_action(clean_text) == "show_offset":
        target = od.parse_offset_month_from_text(clean_text) or (
            date.today().year,
            date.today().month,
        )
    if target is None:
        return False
    year, month = target
    try:
        involved, show_all = _showoffset_view_for_sender(sender_open_id, get_token_func)
        card = od.build_ose_showoffset_card(
            year,
            month,
            involved_person=involved,
            include_all_team=show_all,
        )
        send_message(chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive")
    except Exception as exc:
        send_message(chat_id, f"❌ showoffset failed: {exc}")
    return True


def handle_offset_query(
    clean_text: str,
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """Natural-language offset lookups, e.g. 五月有谁offset / who has offset in May."""
    act = parse_offset_leave_action(clean_text)
    if act in ("delete_offset", "edit_offset", "pending_offset", "offset_form", "leave_form", "show_offset"):
        return False
    if act not in ("query_offset",) and not od.looks_like_offset_lookup_query(clean_text):
        return False
    month = od.parse_offset_month_from_text(clean_text)
    if not month:
        month = (date.today().year, date.today().month)
    person = od.match_roster_name_in_text(clean_text)
    if not person:
        person, _, inferred_month = _resolve_nl_offset_filters(clean_text, month_target=month)
        if inferred_month:
            month = inferred_month
    return execute_offset_action(
        "show_calendar",
        clean_text=clean_text,
        month_target=month,
        person_filter=person,
        sender_open_id=sender_open_id,
        chat_id=chat_id,
        chat_type=chat_type,
        send_message=send_message,
        get_token_func=get_token_func,
    )


def handle_editoffset_command(
    clean_text: str,
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
    force: bool = False,
    month_target: Optional[tuple[int, int]] = None,
    person_filter: Optional[str] = None,
    status_filter: Optional[str] = None,
    approver_admin_only: bool = False,
) -> bool:
    if not force and not wants_editoffset(clean_text):
        return False
    oid = (sender_open_id or "").strip()
    if not oid:
        send_message(chat_id, "❌ Could not identify your Lark user.")
        return True
    if person_filter is None and status_filter is None:
        person_filter, status_filter, inferred_month = _resolve_nl_offset_filters(
            clean_text, month_target=month_target
        )
        if month_target is None:
            month_target = inferred_month
    if month_target is None:
        month_target = _parse_offset_month_filter(clean_text)
    month_label = (
        _month_filter_label(month_target[0], month_target[1]) if month_target else None
    )
    filter_note = ""
    if person_filter or status_filter:
        bits = []
        if person_filter:
            bits.append(str(person_filter))
        if status_filter:
            bits.append(str(status_filter))
        filter_note = " · ".join(bits)
    has_nl_filter = bool(person_filter or status_filter or month_target)
    try:
        token = get_token_func()
        _clear_edit_forms_for_owner(oid)
        if _is_offset_approver_open_id(oid):
            request_person = try_resolve_request_person(oid, token)
            if status_filter == "pending":
                send_message(
                    chat_id,
                    "Pending offset requests: use **pendingoffset** to review, or **editoffset** "
                    "as the requester to change your own pending row.\n"
                    "Approver **editoffset** lists **approved** or **rejected** rows only.",
                )
                return True
            if (
                not approver_admin_only
                and not has_nl_filter
                and request_person
                and _pending_offsets_for_request_person(request_person)
            ):
                _deliver_requester_offset_edit_menu(
                    owner_open_id=oid,
                    request_person=request_person,
                    group_chat_id=chat_id,
                    chat_type=chat_type,
                    send_message=send_message,
                    token=token,
                )
                return True
            rows = _non_pending_offsets_all()
            if month_target:
                rows = _filter_offsets_by_month(rows, *month_target)
            if person_filter or status_filter:
                import offsetai as oai

                rows = oai.filter_offset_rows(
                    rows,
                    clean_text,
                    person_filter=person_filter,
                    status_filter=status_filter,
                    month_target=month_target,
                )
            if not rows:
                hint = f" for **{month_label}**" if month_label else ""
                if filter_note:
                    hint = f" matching **{filter_note}**" + hint
                send_message(chat_id, f"No offset records found to edit{hint}.")
                return True
            card = build_offset_edit_list_card(
                oid,
                "",
                rows,
                is_admin=True,
                month_label=month_label,
                filter_label=filter_note or None,
            )
            _deliver_private_card(
                owner_open_id=oid,
                group_chat_id=chat_id,
                chat_type=chat_type,
                card=card,
                send_message=send_message,
                token=token,
            )
        else:
            request_person = resolve_request_person(oid, token)
            rows = _pending_offsets_for_request_person(request_person)
            if month_target:
                rows = _filter_offsets_by_month(rows, *month_target)
            if person_filter or status_filter:
                import offsetai as oai

                rows = oai.filter_offset_rows(
                    rows,
                    clean_text,
                    person_filter=person_filter,
                    status_filter=status_filter,
                    month_target=month_target,
                )
            if not rows:
                if month_label or filter_note:
                    hint = f" for **{month_label}**" if month_label else ""
                    if filter_note:
                        hint = f" matching **{filter_note}**" + hint
                    send_message(
                        chat_id,
                        f"No **pending** offset requests found to edit{hint}.",
                    )
                else:
                    send_message(
                        chat_id,
                        f"No pending offset found for **{request_person}**. "
                        "Approved or rejected requests cannot be edited with editoffset.",
                    )
                return True
            if len(rows) == 1 and not has_nl_filter:
                card = build_offset_edit_form_card(
                    owner_open_id=oid,
                    request_person=request_person,
                    row=rows[0],
                    is_admin=False,
                )
                _deliver_private_card(
                    owner_open_id=oid,
                    group_chat_id=chat_id,
                    chat_type=chat_type,
                    card=card,
                    send_message=send_message,
                    token=token,
                )
                rid = str(rows[0].get("record_id") or "").strip()
                if rid:
                    _mark_edit_form_open(oid, rid)
            else:
                card = build_offset_edit_list_card(
                    oid,
                    request_person,
                    rows,
                    is_admin=False,
                    month_label=month_label,
                    filter_label=filter_note or None,
                )
                _deliver_private_card(
                    owner_open_id=oid,
                    group_chat_id=chat_id,
                    chat_type=chat_type,
                    card=card,
                    send_message=send_message,
                    token=token,
                )
    except Exception as e:
        send_message(chat_id, f"❌ editoffset: {e}")
    return True


def _text_explicitly_mentions_offset_status(text: str) -> bool:
    return bool(
        re.search(
            r"(?i)\b(pending|approved|rejected|待审|已批|已拒)\b",
            text or "",
        )
    )


def _text_explicitly_mentions_offset_month(text: str) -> bool:
    t = text or ""
    return (
        _parse_offset_month_filter(t) is not None
        or od.parse_offset_month_from_text(t) is not None
    )


def _resolve_nl_offset_filters(
    clean_text: str,
    *,
    month_target: Optional[tuple[int, int]] = None,
) -> tuple[Optional[str], Optional[str], Optional[tuple[int, int]]]:
    """Person / status / month from NL (roster token match, then optional 0.5b)."""
    person: Optional[str] = od.match_roster_name_in_text(clean_text)
    status: Optional[str] = None
    month = month_target
    try:
        import offsetai as oai

        inferred = oai.infer_offset_filters(clean_text)
        if not person:
            person = (inferred.get("person") or "").strip() or None
        st = str(inferred.get("status") or "").strip().lower()
        if st in ("approved", "pending", "rejected"):
            status = st
        if month is None:
            try:
                y, m = inferred.get("year"), inferred.get("month")
                if y is not None and m is not None:
                    month = (int(y), int(m))
            except (TypeError, ValueError):
                pass
    except Exception:
        pass
    if not _text_explicitly_mentions_offset_status(clean_text):
        status = None
    if not _text_explicitly_mentions_offset_month(clean_text):
        month = None
    if person:
        print(
            f"[offsetleave] NL person filter: {person!r} from {clean_text[:80]!r}",
            flush=True,
        )
    return person, status, month


def handle_deleteoffset_command(
    clean_text: str,
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
    force: bool = False,
    month_target: Optional[tuple[int, int]] = None,
    person_filter: Optional[str] = None,
    status_filter: Optional[str] = None,
) -> bool:
    if not force and not wants_deleteoffset(clean_text):
        return False
    oid = (sender_open_id or "").strip()
    if not oid:
        send_message(chat_id, "❌ Could not identify your Lark user.")
        return True
    if person_filter is None and status_filter is None:
        person_filter, status_filter, inferred_month = _resolve_nl_offset_filters(
            clean_text, month_target=month_target
        )
        if month_target is None:
            month_target = inferred_month
    if month_target is None:
        month_target = _parse_offset_month_filter(clean_text)
    month_label = (
        _month_filter_label(month_target[0], month_target[1]) if month_target else None
    )
    filter_note = ""
    if person_filter or status_filter:
        bits = []
        if person_filter:
            bits.append(str(person_filter))
        if status_filter:
            bits.append(str(status_filter))
        filter_note = " · ".join(bits)
    try:
        token = get_token_func()
        if _is_offset_approver_open_id(oid):
            rows = _all_offsets_for_approver_delete()
            if month_target:
                rows = _filter_offsets_by_month(rows, *month_target)
            if person_filter or status_filter:
                import offsetai as oai

                rows = oai.filter_offset_rows(
                    rows,
                    clean_text,
                    person_filter=person_filter,
                    status_filter=status_filter,
                    month_target=month_target,
                )
            if not rows:
                hint = f" for **{month_label}**" if month_label else ""
                if filter_note:
                    hint = f" matching **{filter_note}**" + hint
                send_message(chat_id, f"No offset records found to delete{hint}.")
                return True
            if not month_target and not filter_note:
                # Plain `deleteoffset` → pick a month first, then that month's rows.
                # Avoids the 15-row cap hiding records. An explicit month/person/status
                # filter still jumps straight to the matching list.
                card = build_offset_delete_month_picker_card(oid, rows)
            else:
                intro_filter = f"\n_Filter: {filter_note}_" if filter_note else ""
                card = build_offset_delete_list_card(
                    oid, "", rows, is_admin=True, month_label=month_label
                )
                if intro_filter and card.get("body", {}).get("elements"):
                    card["body"]["elements"][0]["text"]["content"] += intro_filter
        else:
            request_person = resolve_request_person(oid, token)
            rows = _pending_offsets_for_request_person(request_person)
            if month_target:
                rows = _filter_offsets_by_month(rows, *month_target)
            if person_filter or status_filter:
                import offsetai as oai

                rows = oai.filter_offset_rows(
                    rows,
                    clean_text,
                    person_filter=person_filter,
                    status_filter=status_filter,
                    month_target=month_target,
                )
            if not rows:
                if month_label:
                    send_message(
                        chat_id,
                        f"No **pending** offset requests for **{month_label}** that you can delete. "
                        "Approved / rejected rows must be removed by an approver.",
                    )
                else:
                    send_message(
                        chat_id,
                        "No offset found that you requested (no pending rows). "
                        "Ask an approver if an approved/rejected row must be removed.",
                    )
                return True
            card = build_offset_delete_list_card(
                oid, request_person, rows, is_admin=False, month_label=month_label
            )
        _deliver_private_card(
            owner_open_id=oid,
            group_chat_id=chat_id,
            chat_type=chat_type,
            card=card,
            send_message=send_message,
            token=token,
        )
    except Exception as e:
        send_message(chat_id, f"❌ deleteoffset: {e}")
    return True


def handle_pendingoffset_command(
    clean_text: str,
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
    force: bool = False,
) -> bool:
    if not force and not wants_pendingoffset(clean_text):
        return False
    oid = (sender_open_id or "").strip()
    if not oid:
        send_message(chat_id, "❌ Could not identify your Lark user.")
        return True
    if not _is_offset_approver_open_id(oid):
        send_message(chat_id, "❌ **pendingoffset** is for configured offset approvers only.")
        return True
    try:
        token = get_token_func()
        rows = _all_pending_offsets()
        if not rows:
            send_message(chat_id, "✅ No pending offset requests — the queue is empty.")
            return True
        card = build_offset_pending_list_card(rows)
        _deliver_private_card(
            owner_open_id=oid,
            group_chat_id=chat_id,
            chat_type=chat_type,
            card=card,
            send_message=send_message,
            token=token,
        )
    except Exception as e:
        send_message(chat_id, f"❌ pendingoffset: {e}")
    return True


def execute_offset_action(
    action: str,
    *,
    clean_text: str = "",
    month_target: Optional[tuple[int, int]] = None,
    llm_reply: Optional[str] = None,
    person_filter: Optional[str] = None,
    status_filter: Optional[str] = None,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    """Run an offset action decided by ``offsetai`` (no command / NL classifier)."""
    act = (action or "").strip().lower()
    oid = (sender_open_id or "").strip()

    if act == "query":
        reply = ""
        try:
            import offsetai as oai

            reply = oai.build_query_reply(clean_text)
        except Exception:
            reply = ""
        if not reply:
            reply = (llm_reply or "").strip()
        if not reply:
            today = date.today()
            y, m = month_target or (today.year, today.month)
            involved, show_all = _showoffset_view_for_sender(sender_open_id, get_token_func)
            label = _month_filter_label(y, m)
            if show_all and involved:
                mine = od._collect_offset_month_pair_lines(y, m, involved_person=involved)
                all_lines = od._collect_offset_month_pair_lines(y, m)
                lines = [f"**OSE offset — {label}**\n", "**Your offsets**"]
                if mine:
                    lines.extend(f"• {line}" for line in mine)
                else:
                    lines.append("_None_")
                lines.extend(["", "**All offsets**"])
                if all_lines:
                    lines.extend(f"• {line}" for line in all_lines)
                else:
                    lines.append("_None_")
                reply = "\n".join(lines)
            else:
                pair_lines = od._collect_offset_month_pair_lines(
                    y, m, involved_person=involved
                )
                if not pair_lines:
                    if involved:
                        reply = f"No offset requests for **{label}** involving you."
                    else:
                        reply = f"No offset requests for **{label}**."
                else:
                    lines = [f"**OSE offset — {label}**\n"]
                    lines.extend(f"• {line}" for line in pair_lines)
                    reply = "\n".join(lines)
        send_message(chat_id, reply)
        return True

    if act == "show_calendar":
        today = date.today()
        y, m = month_target or (today.year, today.month)
        try:
            query_person = (person_filter or "").strip() or od.match_roster_name_in_text(
                clean_text
            )
            if query_person:
                print(
                    f"[offsetleave] show_calendar person={query_person!r} "
                    f"month={y}-{m:02d} text={clean_text[:80]!r}",
                    flush=True,
                )
                card = od.build_ose_showoffset_card(
                    y,
                    m,
                    involved_person=query_person,
                    include_all_team=False,
                )
            else:
                involved, show_all = _showoffset_view_for_sender(
                    sender_open_id, get_token_func
                )
                card = od.build_ose_showoffset_card(
                    y, m, involved_person=involved, include_all_team=show_all
                )
            send_message(chat_id, json.dumps(card, ensure_ascii=False), msg_type="interactive")
        except Exception as exc:
            send_message(chat_id, f"❌ show offset failed: {exc}")
        return True

    if act == "add":
        if not oid:
            send_message(chat_id, "❌ Could not identify your Lark user for a private form.")
            return True
        try:
            token = get_token_func()
            request_person = resolve_request_person(oid, token)
            _deliver_private_card(
                owner_open_id=oid,
                group_chat_id=chat_id,
                chat_type=chat_type,
                card=build_offset_form_card(
                    owner_open_id=oid,
                    request_person=request_person,
                ),
                send_message=send_message,
                token=token,
            )
        except Exception as e:
            send_message(chat_id, f"❌ Could not open offset form: {e}")
        return True

    if act in ("delete_pick",):
        return handle_deleteoffset_command(
            clean_text,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_token_func,
            force=True,
            month_target=month_target,
            person_filter=person_filter,
            status_filter=status_filter,
        )

    if act == "delete":
        return handle_deleteoffset_command(
            clean_text,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_token_func,
            force=True,
            month_target=month_target,
        )

    if act == "edit":
        return handle_editoffset_command(
            clean_text,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_token_func,
            force=True,
            month_target=month_target,
            person_filter=person_filter,
            status_filter=status_filter,
        )

    if act == "pending":
        return handle_pendingoffset_command(
            clean_text,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            chat_type=chat_type,
            send_message=send_message,
            get_token_func=get_token_func,
            force=True,
        )

    return False


def handle_mention(
    clean_text: str,
    *,
    sender_open_id: str,
    chat_id: str,
    chat_type: Optional[str],
    send_message: Callable[..., dict[str, Any]],
    get_token_func: Callable[[], str],
) -> bool:
    text = (clean_text or "").strip()
    want_offset = _wants_offset(text)
    want_leave = _wants_leave(text)
    try:
        import offsetai

        if offsetai.is_enabled():
            if offsetai.looks_like_offset_topic(text):
                return False
            if want_offset:
                want_offset = False
    except Exception:
        pass
    if not want_offset and not want_leave:
        return False
    oid = (sender_open_id or "").strip()
    if not oid:
        send_message(chat_id, "❌ Could not identify your Lark user for a private form.")
        return True
    try:
        token = get_token_func()
    except Exception as e:
        send_message(chat_id, f"❌ {e}")
        return True
    try:
        if want_offset:
            _open_offset_form(
                sender_open_id=oid,
                chat_id=chat_id,
                chat_type=chat_type,
                send_message=send_message,
                get_token_func=get_token_func,
            )
        if want_leave:
            request_person = try_resolve_request_person(oid, token)
            pick_roster = request_person is None
            if pick_roster:
                request_person = ""
            _deliver_private_card(
                owner_open_id=oid,
                group_chat_id=chat_id,
                chat_type=chat_type,
                card=build_leave_form_card(
                    owner_open_id=oid,
                    request_person=request_person,
                    pick_roster_name=pick_roster,
                ),
                send_message=send_message,
                token=token,
            )
    except Exception as e:
        send_message(chat_id, f"❌ Could not open form: {e}")
    return True


def _form_field_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (int, float)):
        return str(v).strip()
    if isinstance(v, list):
        parts = [_form_field_text(x) for x in v]
        return " ".join(p for p in parts if p).strip()
    if isinstance(v, dict):
        for key in ("value", "text", "content", "date"):
            t = _form_field_text(v.get(key))
            if t:
                return t
        for vv in v.values():
            t = _form_field_text(vv)
            if t:
                return t
    return ""


def _get_form_field(action_obj: Any, parsed: Any, event_obj: Any, name: str) -> str:
    if isinstance(action_obj, dict):
        fv = action_obj.get("form_value")
        if isinstance(fv, dict):
            t = _form_field_text(fv.get(name))
            if t:
                return t
    if isinstance(parsed, dict):
        fv = parsed.get("form_value")
        if isinstance(fv, dict):
            t = _form_field_text(fv.get(name))
            if t:
                return t
        t = _form_field_text(parsed.get(name))
        if t:
            return t
    return _find_field_deep(event_obj, name)


def _find_field_deep(obj: Any, name: str) -> str:
    if isinstance(obj, dict):
        if name in obj:
            t = _form_field_text(obj.get(name))
            if t:
                return t
        for vv in obj.values():
            t = _find_field_deep(vv, name)
            if t:
                return t
    elif isinstance(obj, list):
        for it in obj:
            t = _find_field_deep(it, name)
            if t:
                return t
    return ""


def _parse_date_iso(raw: Optional[str]) -> date:
    s = str(raw or "").strip()
    if not s:
        raise ValueError("date is required")
    if re.match(r"^\d{10,13}$", s):
        try:
            ts = int(s)
            if ts > 10**12:
                ts //= 1000
            return datetime.fromtimestamp(ts).date()
        except Exception as e:
            raise ValueError(f"invalid date timestamp {raw!r}") from e
    m = re.match(r"^\s*(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m2 = re.match(r"^\s*(\d{4})/(\d{2})/(\d{2})", s)
    if m2:
        return date(int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
    try:
        return date.fromisoformat(s[:10])
    except ValueError as e:
        raise ValueError(f"invalid date {raw!r} (use YYYY-MM-DD)") from e


def _resolve_submit_request_person(
    parsed: dict[str, Any],
    *,
    action: dict[str, Any],
    event_obj: Any,
) -> str:
    request_person = str(parsed.get("request_person") or "").strip()
    if str(parsed.get("pick_roster") or "") == "1":
        picked = _get_form_field(action, parsed, event_obj, "request_person")
        if not picked:
            raise ValueError("Please select your roster name from the OSE list.")
        request_person = od._title_name(picked)
        if not _is_roster_name(request_person):
            raise ValueError(f"{picked!r} is not on the OSE roster.")
    if not request_person:
        raise ValueError("Request person is missing from the form session.")
    return request_person


def _assert_owner(
    parsed: dict[str, Any],
    sender_open_id: str,
    *,
    action: Optional[dict[str, Any]] = None,
    event_obj: Any = None,
) -> tuple[str, str]:
    owner = str(parsed.get("owner") or "").strip()
    sender = (sender_open_id or "").strip()
    if not owner or owner != sender:
        raise ValueError("This form can only be submitted by the user who opened it.")
    act = action if isinstance(action, dict) else {}
    request_person = _resolve_submit_request_person(parsed, action=act, event_obj=event_obj)
    return owner, request_person


def _event_message_id(event_obj: Any, webhook_data: Any = None) -> str:
    for source in (event_obj, webhook_data):
        if not isinstance(source, dict):
            continue
        ctx = source.get("context")
        if isinstance(ctx, dict):
            for key in ("open_message_id", "message_id"):
                mid = str(ctx.get(key) or "").strip()
                if mid:
                    return mid
        act = source.get("action")
        if isinstance(act, dict):
            for key in ("open_message_id", "message_id"):
                mid = str(act.get(key) or "").strip()
                if mid:
                    return mid
            act_ctx = act.get("context")
            if isinstance(act_ctx, dict):
                for key in ("open_message_id", "message_id"):
                    mid = str(act_ctx.get(key) or "").strip()
                    if mid:
                        return mid
        for key in ("open_message_id", "message_id"):
            mid = str(source.get(key) or "").strip()
            if mid:
                return mid
    if isinstance(webhook_data, dict):
        ev = webhook_data.get("event")
        if isinstance(ev, dict) and ev is not event_obj:
            return _event_message_id(ev)
    return ""


def _dismiss_ephemeral_form(message_id: str) -> None:
    mid = (message_id or "").strip()
    if not mid:
        print("[offsetleave] dismiss skipped: missing ephemeral message_id", flush=True)
        return
    try:
        token = od.get_tenant_access_token()
    except Exception as exc:
        print(f"[offsetleave] dismiss ephemeral form token error: {exc!r}", flush=True)
        return
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    try:
        res = requests.post(
            "https://open.larksuite.com/open-apis/ephemeral/v1/delete",
            headers=headers,
            json={"message_id": mid},
            timeout=20,
        ).json()
        code = int(res.get("code", -1))
        if code == 0:
            print(f"[offsetleave] dismissed ephemeral form {mid}", flush=True)
            return
        if code == 18051:
            return
        print(f"[offsetleave] ephemeral delete failed: {res!r}", flush=True)
        fallback = requests.delete(
            f"https://open.larksuite.com/open-apis/im/v1/messages/{mid}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=20,
        ).json()
        if int(fallback.get("code", -1)) != 0:
            print(f"[offsetleave] im delete fallback failed: {fallback!r}", flush=True)
    except Exception as exc:
        print(f"[offsetleave] dismiss ephemeral form error: {exc!r}", flush=True)


def _operator_open_id(event_obj: dict[str, Any], fallback: str) -> str:
    op = event_obj.get("operator") if isinstance(event_obj.get("operator"), dict) else {}
    oid = (op.get("open_id") or "").strip()
    if oid:
        return oid
    return (fallback or "").strip()


def _lark_md_cell(s: Any) -> str:
    t = str(s if s is not None else "").replace("\n", " ").replace("|", "/").strip()
    if len(t) > 900:
        return t[:900] + "…"
    return t or "—"


def _offset_approval_table_md(
    row: dict[str, Any],
    *,
    status: str,
    intro: Optional[str] = None,
) -> str:
    rd = _lark_md_cell(row.get("request_date"))
    rp = _lark_md_cell(row.get("request_person"))
    ex = _lark_md_cell(row.get("exchange_person"))
    sh = _lark_md_cell(row.get("shift_type"))
    od_ = _lark_md_cell(row.get("original_date"))
    xd = _lark_md_cell(row.get("exchange_date"))
    rs = _lark_md_cell(row.get("reason"))
    st = _lark_md_cell(status)
    intro_line = intro or (
        "**Someone submitted an offset record.** Review the table, tap **Approve** or **Reject**, "
        "then optional **Remarks**, then **Confirm**."
    )
    lines = [
        intro_line,
        "",
        "| REQ. DATE | REQUEST PERSON | EXCHANGE PERSON | SHIFT | ORIGINAL DATE | EXCHANGE DATE | REASON | STATUS |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
        f"| {rd} | {rp} | {ex} | {sh} | {od_} | {xd} | {rs} | {st} |",
        "",
    ]
    return "\n".join(lines)


def _lookup_offset_row(record_id: str, *, bust_cache: bool = False) -> Optional[dict[str, Any]]:
    """Find offset row by Bitable record_id; optionally force a fresh Bitable read."""
    rid = (record_id or "").strip()
    if not rid:
        return None

    def _scan() -> Optional[dict[str, Any]]:
        for it in (od.get_ose_offset_records_admin().get("items") or []):
            if str(it.get("record_id") or "").strip() == rid:
                return dict(it)
        return None

    if bust_cache:
        od.invalidate_ose_bitable_cache()
    row = _scan()
    if row is not None:
        return row
    if not bust_cache:
        od.invalidate_ose_bitable_cache()
        return _scan()
    return None


def _offset_admin_row_by_id(record_id: str) -> dict[str, Any]:
    rid = (record_id or "").strip()
    if not rid:
        raise ValueError("missing record_id")
    row = _lookup_offset_row(rid, bust_cache=True)
    if row is None:
        raise KeyError(f"offset record {rid!r}")
    return row


def _local_pending_offset_row(
    *,
    record_id: str,
    request_person: str,
    exchange_person: str,
    shift_type: str,
    original_date: date,
    exchange_date: date,
    reason: str,
) -> dict[str, Any]:
    today = date.today()
    return {
        "record_id": record_id,
        "request_id": "",
        "request_date": od._format_yyyymmdd(today),
        "request_person": od._title_name(request_person),
        "exchange_person": od._title_name(exchange_person),
        "shift_type": (shift_type or "").strip().upper(),
        "original_date": od._format_yyyymmdd(original_date),
        "exchange_date": od._format_yyyymmdd(exchange_date),
        "reason": (reason or "").strip(),
        "approval_status": "Pending",
    }


def _try_patch_interactive_card_message(message_id: str, card: dict[str, Any]) -> bool:
    """PATCH card in place. Ephemeral group cards often cannot be patched (returns False)."""
    mid = (message_id or "").strip()
    if not mid:
        return False
    token = od.get_tenant_access_token()
    url = f"https://open.larksuite.com/open-apis/im/v1/messages/{mid}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    payload = {"msg_type": "interactive", "content": json.dumps(card, ensure_ascii=False)}
    last_res: dict[str, Any] = {}
    for id_type in ("open_message_id", ""):
        params = {"message_id_type": id_type} if id_type else {}
        last_res = requests.patch(
            url,
            headers=headers,
            params=params,
            json=payload,
            timeout=25,
        ).json()
        if int(last_res.get("code", -1)) == 0:
            return True
    print(f"[offsetleave] patch card skipped for {mid!r}: {last_res!r}", flush=True)
    return False


def _patch_interactive_card_message(message_id: str, card: dict[str, Any]) -> None:
    if not _try_patch_interactive_card_message(message_id, card):
        raise RuntimeError(f"patch card failed for message_id={message_id!r}")


def _approval_pick_button_row(approve_val: dict[str, Any], reject_val: dict[str, Any]) -> dict[str, Any]:
    def _btn(label: str, typ: str, val: dict[str, Any]) -> dict[str, Any]:
        return {
            "tag": "button",
            "text": {"tag": "plain_text", "content": label},
            "type": typ,
            "behaviors": [{"type": "callback", "value": val}],
        }

    return {
        "tag": "column_set",
        "flex_mode": "flow",
        "background_style": "default",
        "horizontal_spacing": "8px",
        "columns": [
            {
                "tag": "column",
                "width": "auto",
                "weight": 1,
                "vertical_align": "top",
                "elements": [_btn("Approve", "primary", approve_val)],
            },
            {
                "tag": "column",
                "width": "auto",
                "weight": 1,
                "vertical_align": "top",
                "elements": [_btn("Reject", "danger", reject_val)],
            },
        ],
    }


def build_offset_approver_initial_card(row: dict[str, Any]) -> dict[str, Any]:
    rid = str(row.get("record_id") or "").strip()
    st = (str(row.get("approval_status") or "").strip() or "Pending")
    md = _offset_approval_table_md(row, status=st)
    approve_val = {"k": _OFFSET_APPR_PICK_KEY, "record_id": rid, "decision": "Approved"}
    reject_val = {"k": _OFFSET_APPR_PICK_KEY, "record_id": rid, "decision": "Rejected"}
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "wathet",
            "title": {"tag": "plain_text", "content": "OSE offset — pending approval"},
        },
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}, _approval_pick_button_row(approve_val, reject_val)]},
    }


def build_offset_approver_confirm_card(row: dict[str, Any], decision: str) -> dict[str, Any]:
    rid = str(row.get("record_id") or "").strip()
    dec = (decision or "").strip().title()
    st_show = f"{dec} (pending Confirm)"
    md = _offset_approval_table_md(row, status=st_show)
    confirm_val = {"k": _OFFSET_APPR_CONFIRM_KEY, "record_id": rid, "decision": dec}
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "wathet",
            "title": {"tag": "plain_text", "content": "OSE offset — confirm approval"},
        },
        "body": {
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": md}},
                {
                    "tag": "form",
                    "name": "ose_offset_approval_confirm",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": (
                                    f"You selected **{dec}**. Add optional **Remarks** below, then tap **Confirm**."
                                ),
                            },
                        },
                        {
                            "tag": "input",
                            "name": "approval_remarks",
                            "input_type": "multiline_text",
                            "rows": 2,
                            "auto_resize": True,
                            "width": "fill",
                            "label": {"tag": "plain_text", "content": "Remarks (optional)"},
                            "label_position": "top",
                            "placeholder": {"tag": "plain_text", "content": "Optional remarks"},
                            "required": False,
                            "max_length": 1000,
                        },
                        {
                            "tag": "button",
                            "name": "confirm_offset_approval",
                            "text": {"tag": "plain_text", "content": "Confirm"},
                            "type": "primary",
                            "form_action_type": "submit",
                            "behaviors": [{"type": "callback", "value": confirm_val}],
                        },
                    ],
                },
            ]
        },
    }


def build_offset_approver_done_card(row: dict[str, Any], decision: str, remarks: str) -> dict[str, Any]:
    dec = (decision or "").strip().title()
    rr = (remarks or "").strip()
    extra = f"\n**Remarks:** {_lark_md_cell(rr) if rr else '—'}"
    md = _offset_approval_table_md(row, status=dec) + extra
    tpl = "green" if dec == "Approved" else "red"
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": tpl, "title": {"tag": "plain_text", "content": f"OSE offset — {dec}"}},
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}]},
    }


def build_offset_requester_responded_card(
    row: dict[str, Any],
    *,
    approver_name: str,
    decision: str,
    remarks: str,
) -> dict[str, Any]:
    """Read-only message card for the requester (same layout style as approver done card, picture 2)."""
    dec = (decision or "").strip().title()
    an = _lark_md_cell(approver_name)
    rd = _lark_md_cell(row.get("request_date"))
    rp = _lark_md_cell(row.get("request_person"))
    ex = _lark_md_cell(row.get("exchange_person"))
    sh = _lark_md_cell(row.get("shift_type"))
    od_ = _lark_md_cell(row.get("original_date"))
    xd = _lark_md_cell(row.get("exchange_date"))
    rs = _lark_md_cell(row.get("reason"))
    st = _lark_md_cell((row.get("approval_status") or dec or "").strip() or dec)
    rr = (remarks or "").strip()
    remark_line = f"**Remarks:** {_lark_md_cell(rr) if rr else '—'}"
    md = "\n".join(
        [
            f"**{an}** already responded to your offset request (**{dec}**).",
            "",
            "| REQ. DATE | REQUEST PERSON | EXCHANGE PERSON | SHIFT | ORIGINAL DATE | EXCHANGE DATE | REASON | STATUS |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
            f"| {rd} | {rp} | {ex} | {sh} | {od_} | {xd} | {rs} | {st} |",
            "",
            remark_line,
        ]
    )
    tpl = "green" if dec == "Approved" else "red"
    return {
        "schema": "2.0",
        "config": {"width_mode": "fill"},
        "header": {"template": tpl, "title": {"tag": "plain_text", "content": f"OSE offset — {dec}"}},
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}]},
    }


def build_offset_requester_approver_deleted_card(
    row: dict[str, Any],
    *,
    approver_name: str,
) -> dict[str, Any]:
    """Read-only card DM'd to the requester when an approver deletes their pending offset."""
    an = _lark_md_cell(approver_name)
    intro = (
        f"**{an}** deleted your **pending** offset request. "
        "The row has been removed — submit again with **offset** if you still need a swap."
    )
    md = _offset_approval_table_md(row, status="Deleted", intro=intro)
    return {
        "schema": "2.0",
        "config": {"width_mode": "fill"},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "OSE offset — request deleted"},
        },
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}]},
    }


def build_offset_requester_deleted_notify_card(
    row: dict[str, Any],
    *,
    requester_name: str,
) -> dict[str, Any]:
    """Read-only card for approvers when a requester deletes a pending offset."""
    rn = _lark_md_cell(requester_name)
    intro = (
        f"**{rn}** **deleted** a **pending** offset request. "
        "No approval action is needed — the request was withdrawn and removed from the table."
    )
    md = _offset_approval_table_md(row, status="Withdrawn", intro=intro)
    return {
        "schema": "2.0",
        "config": {"width_mode": "fill"},
        "header": {
            "template": "grey",
            "title": {"tag": "plain_text", "content": "OSE offset — pending request deleted"},
        },
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}]},
    }


def build_offset_deleted_actor_confirm_card(row: dict[str, Any]) -> dict[str, Any]:
    """Confirmation DM for the approver who deleted the row (same record details as peer alert)."""
    status_was = str(row.get("approval_status") or "").strip().title()
    if bool(row.get("pending")) and not status_was:
        status_was = "Pending"
    intro = (
        "✅ **You deleted** this offset record successfully. "
        "The row has been removed from the table and other approvers have been notified."
    )
    status_cell = f"Deleted (was {status_was})" if status_was else "Deleted"
    md = _offset_approval_table_md(row, status=status_cell, intro=intro)
    return {
        "schema": "2.0",
        "config": {"width_mode": "fill"},
        "header": {
            "template": "green",
            "title": {"tag": "plain_text", "content": "OSE offset — record deleted"},
        },
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}]},
    }


def build_offset_deleted_notify_card(
    row: dict[str, Any],
    *,
    deleter_label: str,
    deleter_known: bool,
) -> dict[str, Any]:
    """
    Read-only alert for approvers when an offset row is removed from the table by ANY
    method (manual Base delete, bot delete, API). ``deleter_label`` names the operator
    when known, otherwise explains it could not be determined.
    """
    status_was = str(row.get("approval_status") or "").strip().title()
    if bool(row.get("pending")) and not status_was:
        status_was = "Pending"
    who = _lark_md_cell(deleter_label)
    if deleter_known:
        intro = (
            f"⚠️ An offset record was **deleted** by **{who}**. "
            "No approval action is needed — it has been removed from the table."
        )
    else:
        intro = (
            "⚠️ An offset record was **deleted directly in the Base table** "
            f"({who}). No approval action is needed — it has been removed from the table."
        )
    status_cell = f"Deleted (was {status_was})" if status_was else "Deleted"
    md = _offset_approval_table_md(row, status=status_cell, intro=intro)
    return {
        "schema": "2.0",
        "config": {"width_mode": "fill"},
        "header": {
            "template": "red",
            "title": {"tag": "plain_text", "content": "OSE offset — record deleted"},
        },
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}]},
    }


def build_offset_requester_edited_notify_card(
    row: dict[str, Any],
    *,
    requester_name: str,
) -> dict[str, Any]:
    """Interactive card for approvers when a requester updates a pending offset (re-approve/reject)."""
    rid = str(row.get("record_id") or "").strip()
    rn = _lark_md_cell(requester_name)
    intro = (
        f"**{rn}** updated a **pending** offset request. Please **review again** — "
        "tap **Approve** or **Reject**, then optional **Remarks**, then **Confirm**."
    )
    md = _offset_approval_table_md(row, status="Pending", intro=intro)
    approve_val = {"k": _OFFSET_APPR_PICK_KEY, "record_id": rid, "decision": "Approved"}
    reject_val = {"k": _OFFSET_APPR_PICK_KEY, "record_id": rid, "decision": "Rejected"}
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "wathet",
            "title": {"tag": "plain_text", "content": "OSE offset — request updated (re-review)"},
        },
        "body": {
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": md}},
                _approval_pick_button_row(approve_val, reject_val),
            ]
        },
    }


def build_offset_other_approver_responded_card(
    row: dict[str, Any],
    *,
    approver_name: str,
    decision: str,
    remarks: str,
) -> dict[str, Any]:
    """Read-only card for the other approver(s) after one peer has confirmed."""
    dec = (decision or "").strip().title()
    verb = "approved" if dec == "Approved" else "rejected"
    an = _lark_md_cell(approver_name)
    rd = _lark_md_cell(row.get("request_date"))
    rp = _lark_md_cell(row.get("request_person"))
    ex = _lark_md_cell(row.get("exchange_person"))
    sh = _lark_md_cell(row.get("shift_type"))
    od_ = _lark_md_cell(row.get("original_date"))
    xd = _lark_md_cell(row.get("exchange_date"))
    rs = _lark_md_cell(row.get("reason"))
    st = _lark_md_cell((row.get("approval_status") or dec or "").strip() or dec)
    rr = (remarks or "").strip()
    remark_line = f"**Remarks:** {_lark_md_cell(rr) if rr else '—'}"
    md = "\n".join(
        [
            f"**{an}** already **{verb}** this offset request. No further action is needed on your pending card.",
            "",
            "| REQ. DATE | REQUEST PERSON | EXCHANGE PERSON | SHIFT | ORIGINAL DATE | EXCHANGE DATE | REASON | STATUS |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
            f"| {rd} | {rp} | {ex} | {sh} | {od_} | {xd} | {rs} | {st} |",
            "",
            remark_line,
        ]
    )
    tpl = "green" if dec == "Approved" else "red"
    return {
        "schema": "2.0",
        "config": {"width_mode": "fill"},
        "header": {"template": tpl, "title": {"tag": "plain_text", "content": f"OSE offset — {dec} (by {approver_name})"}},
        "body": {"elements": [{"tag": "div", "text": {"tag": "lark_md", "content": md}}]},
    }


def _build_offset_approval_denied_card() -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "red", "title": {"tag": "plain_text", "content": "Offset approval"}},
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {"tag": "plain_text", "content": "Not authorized — only the assigned approver can use this card."},
                }
            ]
        },
    }


def _build_offset_approval_error_card(message: str) -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "red", "title": {"tag": "plain_text", "content": "Offset approval error"}},
        "body": {"elements": [{"tag": "div", "text": {"tag": "plain_text", "content": _lark_md_cell(message)}}]},
    }


def _norm_offset_decision(raw: Any) -> str:
    t = str(raw or "").strip().lower()
    if t in ("approved", "approve"):
        return "Approved"
    if t in ("rejected", "reject"):
        return "Rejected"
    raise ValueError(f"invalid decision {raw!r}")


def _approver_display_for_bitable(operator_open_id: str) -> str:
    token = od.get_tenant_access_token()
    display = _fetch_user_display_name(operator_open_id, token)
    roster = _match_roster_name(display) if display else None
    if roster:
        return roster
    if display:
        return od._title_name(display)
    return "Approver"


def _is_offset_approver_open_id(open_id: str) -> bool:
    return (open_id or "").strip() in OFFSET_APPROVER_OPEN_IDS


def _lark_im_send_message(
    receive_id: str,
    text: str,
    msg_type: str = "text",
    mentions: Any = None,
    receive_id_type: str = "chat_id",
) -> dict[str, Any]:
    """Send Lark IM when ``main.send_message`` is unavailable (e.g. web offset submit)."""
    token = od.get_tenant_access_token()
    url = "https://open.larksuite.com/open-apis/im/v1/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if msg_type == "interactive":
        content = text if isinstance(text, str) else json.dumps(text)
    else:
        content = json.dumps({"text": text})
    body: dict[str, Any] = {
        "receive_id": receive_id,
        "msg_type": msg_type,
        "content": content,
    }
    if mentions:
        body["mentions"] = mentions
    rid_type = (receive_id_type or "chat_id").strip() or "chat_id"
    response = requests.post(
        url,
        headers=headers,
        params={"receive_id_type": rid_type},
        json=body,
        timeout=30,
    )
    return response.json()


def _notify_offset_approver_pending(send_message: Callable[..., Any], row: dict[str, Any]) -> None:
    if not OFFSET_APPROVER_OPEN_IDS:
        return
    card = build_offset_approver_initial_card(row)
    body = json.dumps(card, ensure_ascii=False)
    for oid in OFFSET_APPROVER_OPEN_IDS:
        aid = (oid or "").strip()
        if not aid:
            continue
        r = send_message(aid, body, msg_type="interactive", receive_id_type="open_id")
        if isinstance(r, dict) and int(r.get("code", -1)) != 0:
            print(f"[offsetleave] approver DM failed for {aid!r}: {r!r}", flush=True)


def notify_offset_approvers_for_record(
    record_id: str,
    *,
    send_message: Optional[Callable[..., Any]] = None,
    row: Optional[dict[str, Any]] = None,
    fallback_row: Optional[dict[str, Any]] = None,
) -> None:
    """DM pending-approval cards to all configured approvers (Lark bot or web submit)."""
    rid = (record_id or "").strip()
    if not rid:
        return
    send = send_message or _lark_im_send_message
    resolved = row
    if resolved is None:
        try:
            resolved = _offset_admin_row_by_id(rid)
        except Exception:
            if fallback_row:
                resolved = fallback_row
            else:
                try:
                    resolved = od.get_ose_offset_record_admin_row(rid)
                except Exception as exc:
                    print(f"[offsetleave] approver notify: no row for {rid!r}: {exc!r}", flush=True)
                    return
    _notify_offset_approver_pending(send, resolved)
    _mark_offset_record_notified(rid)


def _load_notified_offset_record_ids_unlocked() -> set[str]:
    try:
        with open(_OFFSET_APPROVER_NOTIFIED_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return set()
    except Exception:
        return set()
    ids = data.get("record_ids") if isinstance(data, dict) else data
    if not isinstance(ids, list):
        return set()
    return {str(x).strip() for x in ids if str(x).strip()}


def _load_notified_offset_record_ids() -> set[str]:
    with _OFFSET_APPROVER_NOTIFIED_LOCK:
        return _load_notified_offset_record_ids_unlocked()


def _mark_offset_record_notified(record_id: str) -> None:
    rid = (record_id or "").strip()
    if not rid:
        return
    with _OFFSET_APPROVER_NOTIFIED_LOCK:
        known = _load_notified_offset_record_ids_unlocked()
        if rid in known:
            return
        known.add(rid)
        tmp = f"{_OFFSET_APPROVER_NOTIFIED_PATH}.tmp"
        payload = {"record_ids": sorted(known)}
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, _OFFSET_APPROVER_NOTIFIED_PATH)


def _unmark_offset_record_notified(record_id: str) -> None:
    """Drop record from poll dedupe file after requester deletes a pending row."""
    rid = (record_id or "").strip()
    if not rid:
        return
    with _OFFSET_APPROVER_NOTIFIED_LOCK:
        known = _load_notified_offset_record_ids_unlocked()
        if rid not in known:
            return
        known.discard(rid)
        tmp = f"{_OFFSET_APPROVER_NOTIFIED_PATH}.tmp"
        payload = {"record_ids": sorted(known)}
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, _OFFSET_APPROVER_NOTIFIED_PATH)


def _offset_row_ready_for_approver_notify(row: dict[str, Any]) -> bool:
    if not bool(row.get("pending")):
        return False
    if not str(row.get("request_person") or "").strip():
        return False
    if not str(row.get("exchange_person") or "").strip():
        return False
    return True


def scan_bitable_pending_offsets_for_approver_notify() -> dict[str, int]:
    """
    Find pending offset rows added directly in Bitable (not via submit_ose_offset)
    and DM approvers. Uses ``offset_approver_notified.json`` to avoid duplicate cards.
    """
    od.invalidate_ose_bitable_cache()
    items = (od.get_ose_offset_records_admin() or {}).get("items") or []
    notified_ids = _load_notified_offset_record_ids()
    sent = 0
    for row in items:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("record_id") or "").strip()
        if not rid or rid in notified_ids:
            continue
        if not _offset_row_ready_for_approver_notify(row):
            continue
        try:
            notify_offset_approvers_for_record(rid, row=dict(row))
            sent += 1
            notified_ids.add(rid)
        except Exception as exc:
            print(f"[offsetleave] bitable scan notify failed for {rid!r}: {exc!r}", flush=True)
    return {"scanned": len(items), "notified": sent}


def scan_bitable_offsets_for_duty_wiki_sync() -> dict[str, int]:
    """
    Mirror Base offset rows edited directly in Bitable (manual add/change/delete).

    Bot submit/update/delete already schedule sync; this poll catches manual Base edits.
    """
    od.invalidate_ose_bitable_cache()
    token = od.get_tenant_access_token()
    items = od._bitable_get_all_records(token, OFFSET_SOURCE_BASE_TOKEN, OFFSET_SOURCE_TABLE_ID)
    with _OFFSET_DUTY_SYNC_LOCK:
        state = _load_offset_duty_sync_state_unlocked()
        by_src = dict(state.get("by_src") or {})
        src_fp = dict(state.get("src_fp") or {})
    live_src: set[str] = set()
    live_fps: set[str] = set()
    synced = 0
    for it in items:
        src_id = str(it.get("record_id") or "").strip()
        if not src_id:
            continue
        live_src.add(src_id)
        fields = it.get("fields") or {}
        fp = _offset_sync_fingerprint(fields)
        live_fps.add(fp)
        if by_src.get(src_id) and src_fp.get(src_id) == fp:
            continue
        try:
            sync_offset_to_duty_wiki(record_id=src_id)
            synced += 1
        except Exception as exc:
            print(f"[offsetleave] duty wiki poll sync failed for {src_id!r}: {exc!r}", flush=True)
    deleted = 0
    for src_id in list(by_src.keys()):
        if src_id in live_src:
            continue
        try:
            sync_offset_to_duty_wiki(record_id=src_id, delete=True)
            deleted += 1
        except Exception as exc:
            print(f"[offsetleave] duty wiki poll delete failed for {src_id!r}: {exc!r}", flush=True)
    try:
        deleted += _prune_duty_wiki_orphans(token, live_fps)
    except Exception as exc:
        print(f"[offsetleave] duty wiki orphan prune failed: {exc!r}", flush=True)
    return {"scanned": len(items), "synced": synced, "deleted": deleted}


def scan_bitable_offsets_for_deletion_notify() -> dict[str, int]:
    """
    Detect offset rows removed from the Base table by **any** method (manual Base delete,
    bot delete, API) and DM all approvers exactly once per deleted row.

    Compares a persisted snapshot of known rows against the live table. Bot deletes notify
    immediately and are marked done, so this poll mainly catches manual/API deletions where
    the Base API does not expose who deleted the row.
    """
    od.invalidate_ose_bitable_cache()
    items = (od.get_ose_offset_records_admin() or {}).get("items") or []
    live: dict[str, dict[str, Any]] = {}
    for r in items:
        if not isinstance(r, dict):
            continue
        rid = str(r.get("record_id") or "").strip()
        if rid:
            live[rid] = dict(r)

    prev = _load_offset_rows_snapshot()
    new_snapshot = dict(live)
    notified = 0
    for rid, row in prev.items():
        if rid in live or _offset_deletion_already_notified(rid):
            continue
        if not str(row.get("request_person") or "").strip():
            continue  # empty/junk row — nothing meaningful to report
        actor = _pop_offset_delete_actor(rid)
        actor_open = (actor.get("open_id") or "").strip()
        actor_name = (actor.get("name") or "").strip()
        if actor_open or actor_name:
            deleter_known = True
            deleter_label = actor_name or "a bot action"
            exclude = actor_open
        else:
            deleter_known = False
            deleter_label = "operator could not be determined from the Base API"
            exclude = ""
        try:
            ok = _notify_offset_approvers_deleted(
                row,
                deleter_label=deleter_label,
                deleter_known=deleter_known,
                exclude_open_id=exclude,
            )
        except Exception as exc:
            ok = False
            print(f"[offsetleave] deletion notify error for {rid!r}: {exc!r}", flush=True)
        if ok:
            _mark_offset_deletion_notified(rid)
            notified += 1
        else:
            # Keep the row in the snapshot and restore attribution so the next poll retries.
            new_snapshot[rid] = row
            if actor_open or actor_name:
                _record_offset_delete_actor(rid, actor_open, actor_name)

    _save_offset_rows_snapshot(new_snapshot)
    return {"scanned": len(items), "notified": notified}


def _toast_approval_problem(send_message: Callable[..., Any], chat_id: str, text: str) -> None:
    cid = (chat_id or "").strip()
    if cid:
        try:
            send_message(cid, text)
        except Exception:
            print(f"[offsetleave] approval notify failed: {text}", flush=True)
    else:
        print(f"[offsetleave] approval: {text}", flush=True)


def _requester_open_id_for_offset_row(request_person: str, record_id: str = "") -> str:
    nm = (request_person or "").strip()
    rid = (record_id or "").strip()
    if rid:
        stored = _lookup_stored_offset_requester_open_id(rid)
        if stored:
            return stored
    if not nm:
        return ""
    token = od.get_tenant_access_token()
    idx = od._get_ose_person_open_id_index(token)
    return (od._lookup_person_open_id(nm, idx) or "").strip()


def _load_offset_requester_open_id_map_unlocked() -> dict[str, str]:
    try:
        with open(_OFFSET_REQUESTER_OPEN_ID_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    by_record = data.get("by_record")
    if not isinstance(by_record, dict):
        by_record = data
    return {str(k): str(v) for k, v in by_record.items() if k and v}


def _save_offset_requester_open_id_map_unlocked(by_record: dict[str, str]) -> None:
    tmp = _OFFSET_REQUESTER_OPEN_ID_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"by_record": dict(by_record)}, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    os.replace(tmp, _OFFSET_REQUESTER_OPEN_ID_PATH)


def remember_offset_requester_open_id(record_id: str, requester_open_id: str) -> None:
    """Remember who submitted an offset (bot/web) so approval DMs can find them later."""
    rid = (record_id or "").strip()
    oid = (requester_open_id or "").strip()
    if not rid or not oid:
        return
    with _OFFSET_REQUESTER_OPEN_ID_LOCK:
        by_record = _load_offset_requester_open_id_map_unlocked()
        by_record[rid] = oid
        _save_offset_requester_open_id_map_unlocked(by_record)


def _lookup_stored_offset_requester_open_id(record_id: str) -> str:
    rid = (record_id or "").strip()
    if not rid:
        return ""
    with _OFFSET_REQUESTER_OPEN_ID_LOCK:
        return str(_load_offset_requester_open_id_map_unlocked().get(rid) or "").strip()


def _load_requester_approval_notified_unlocked() -> dict[str, str]:
    try:
        with open(_OFFSET_REQUESTER_APPROVAL_NOTIFIED_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    by_record = data.get("by_record")
    if not isinstance(by_record, dict):
        by_record = data
    return {str(k): str(v) for k, v in by_record.items() if k and v}


def _save_requester_approval_notified_unlocked(by_record: dict[str, str]) -> None:
    tmp = _OFFSET_REQUESTER_APPROVAL_NOTIFIED_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"by_record": dict(by_record)}, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    os.replace(tmp, _OFFSET_REQUESTER_APPROVAL_NOTIFIED_PATH)


def _requester_approval_already_notified(record_id: str, decision: str) -> bool:
    rid = (record_id or "").strip()
    dec = (decision or "").strip().title()
    if not rid or dec not in ("Approved", "Rejected"):
        return False
    with _OFFSET_REQUESTER_APPROVAL_NOTIFIED_LOCK:
        return _load_requester_approval_notified_unlocked().get(rid) == dec


def _mark_requester_approval_notified(record_id: str, decision: str) -> None:
    rid = (record_id or "").strip()
    dec = (decision or "").strip().title()
    if not rid or dec not in ("Approved", "Rejected"):
        return
    with _OFFSET_REQUESTER_APPROVAL_NOTIFIED_LOCK:
        by_record = _load_requester_approval_notified_unlocked()
        by_record[rid] = dec
        _save_requester_approval_notified_unlocked(by_record)


def _load_peer_approver_approval_notified_unlocked() -> dict[str, str]:
    try:
        with open(_OFFSET_PEER_APPROVER_APPROVAL_NOTIFIED_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    by_record = data.get("by_record")
    if not isinstance(by_record, dict):
        by_record = data
    return {str(k): str(v) for k, v in by_record.items() if k and v}


def _save_peer_approver_approval_notified_unlocked(by_record: dict[str, str]) -> None:
    tmp = _OFFSET_PEER_APPROVER_APPROVAL_NOTIFIED_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"by_record": dict(by_record)}, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    os.replace(tmp, _OFFSET_PEER_APPROVER_APPROVAL_NOTIFIED_PATH)


def _peer_approver_approval_already_notified(record_id: str, decision: str) -> bool:
    rid = (record_id or "").strip()
    dec = (decision or "").strip().title()
    if not rid or dec not in ("Approved", "Rejected"):
        return False
    with _OFFSET_PEER_APPROVER_APPROVAL_NOTIFIED_LOCK:
        return _load_peer_approver_approval_notified_unlocked().get(rid) == dec


def _mark_peer_approver_approval_notified(record_id: str, decision: str) -> None:
    rid = (record_id or "").strip()
    dec = (decision or "").strip().title()
    if not rid or dec not in ("Approved", "Rejected"):
        return
    with _OFFSET_PEER_APPROVER_APPROVAL_NOTIFIED_LOCK:
        by_record = _load_peer_approver_approval_notified_unlocked()
        by_record[rid] = dec
        _save_peer_approver_approval_notified_unlocked(by_record)


def _acting_approver_open_id_from_name(approver_name: str) -> str:
    """Best-effort open_id for the approver who acted (to skip their peer DM)."""
    an = (approver_name or "").strip()
    if not an:
        return ""
    token = od.get_tenant_access_token()
    idx = od._get_ose_person_open_id_index(token)
    oid = (od._lookup_person_open_id(an, idx) or "").strip()
    if oid in OFFSET_APPROVER_OPEN_IDS:
        return oid
    titled = od._title_name(an)
    for aid in OFFSET_APPROVER_OPEN_IDS:
        candidate = (aid or "").strip()
        if not candidate:
            continue
        roster = od.lookup_roster_name_for_open_id(candidate, token)
        if roster and od._names_same_person(roster, an):
            return candidate
        display = _fetch_user_display_name(candidate, token)
        if display and (
            od._names_same_person(display, an)
            or od._title_name(display) == titled
        ):
            return candidate
    return ""


def _notify_requester_offset_responded(
    send_message: Callable[..., Any],
    row: dict[str, Any],
    *,
    approver_name: str,
    decision: str,
    remarks: str,
) -> None:
    rid = str(row.get("record_id") or "").strip()
    dec = (decision or "").strip().title()
    if rid and _requester_approval_already_notified(rid, dec):
        return
    request_person = str(row.get("request_person") or "").strip()
    if not request_person:
        return
    oid = _requester_open_id_for_offset_row(request_person, record_id=rid)
    if not oid:
        print(
            f"[offsetleave] could not resolve Lark open_id for requester {request_person!r} "
            f"(record {rid or '?'})",
            flush=True,
        )
        return
    card = build_offset_requester_responded_card(
        row,
        approver_name=approver_name,
        decision=decision,
        remarks=remarks,
    )
    body = json.dumps(card, ensure_ascii=False)
    r = send_message(oid, body, msg_type="interactive", receive_id_type="open_id")
    if isinstance(r, dict) and int(r.get("code", -1)) != 0:
        print(f"[offsetleave] requester DM failed: {r!r}", flush=True)
        return
    if rid:
        _mark_requester_approval_notified(rid, dec)


def notify_offset_approval_decision(
    record_id: str,
    *,
    send_message: Optional[Callable[..., Any]] = None,
    approver_name: str = "",
    decision: str = "",
    remarks: str = "",
    acting_approver_open_id: str = "",
) -> dict[str, bool]:
    """
    DM requester and other approvers when an offset is approved/rejected.
    Idempotent per record_id + decision (bot card, web admin, or Base poll).
    """
    rid = (record_id or "").strip()
    out = {"requester": False, "peer_approvers": False}
    if not rid:
        return out
    send = send_message or _lark_im_send_message
    try:
        row = _offset_admin_row_by_id(rid)
    except Exception as exc:
        print(f"[offsetleave] approval notify: no row {rid!r}: {exc!r}", flush=True)
        return out
    dec = (decision or str(row.get("approval_status") or "")).strip().title()
    if dec not in ("Approved", "Rejected"):
        return out
    an = (approver_name or str(row.get("approver") or "")).strip() or "Approver"
    rr = remarks if remarks else str(row.get("remarks") or "")
    actor = (acting_approver_open_id or "").strip() or _acting_approver_open_id_from_name(an)
    if not _requester_approval_already_notified(rid, dec):
        try:
            _notify_requester_offset_responded(
                send,
                row,
                approver_name=an,
                decision=dec,
                remarks=rr,
            )
        except Exception as exc:
            print(f"[offsetleave] requester approval notify failed for {rid!r}: {exc!r}", flush=True)
    out["requester"] = _requester_approval_already_notified(rid, dec)
    if not _peer_approver_approval_already_notified(rid, dec):
        try:
            _notify_other_offset_approvers_responded(
                send,
                row,
                acting_approver_open_id=actor,
                approver_name=an,
                decision=dec,
                remarks=rr,
            )
        except Exception as exc:
            print(f"[offsetleave] peer approver notify failed for {rid!r}: {exc!r}", flush=True)
    out["peer_approvers"] = _peer_approver_approval_already_notified(rid, dec)
    return out


def notify_requester_offset_approval_result(
    record_id: str,
    *,
    send_message: Optional[Callable[..., Any]] = None,
    approver_name: str = "",
    decision: str = "",
    remarks: str = "",
) -> bool:
    """DM the requester that their offset was approved/rejected (card with full row details)."""
    result = notify_offset_approval_decision(
        record_id,
        send_message=send_message,
        approver_name=approver_name,
        decision=decision,
        remarks=remarks,
    )
    return bool(result.get("requester"))


def scan_bitable_offsets_for_requester_approval_notify() -> dict[str, int]:
    """Notify requesters and peer approvers when rows were approved/rejected in Base."""
    od.invalidate_ose_bitable_cache()
    items = (od.get_ose_offset_records_admin() or {}).get("items") or []
    requester_sent = 0
    peer_sent = 0
    for row in items:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("record_id") or "").strip()
        if not rid or bool(row.get("pending")):
            continue
        dec = str(row.get("approval_status") or "").strip().title()
        if dec not in ("Approved", "Rejected"):
            continue
        need_requester = not _requester_approval_already_notified(rid, dec)
        need_peer = not _peer_approver_approval_already_notified(rid, dec)
        if not need_requester and not need_peer:
            continue
        result = notify_offset_approval_decision(
            rid,
            approver_name=str(row.get("approver") or ""),
            decision=dec,
            remarks=str(row.get("remarks") or ""),
            acting_approver_open_id=_acting_approver_open_id_from_name(str(row.get("approver") or "")),
        )
        if need_requester and result.get("requester"):
            requester_sent += 1
        if need_peer and result.get("peer_approvers"):
            peer_sent += 1
    return {
        "scanned": len(items),
        "notified": requester_sent,
        "peer_notified": peer_sent,
    }


def _notify_offset_approvers_requester_edited(
    send_message: Callable[..., Any],
    row: dict[str, Any],
    *,
    requester_name: str,
) -> None:
    card = build_offset_requester_edited_notify_card(row, requester_name=requester_name)
    body = json.dumps(card, ensure_ascii=False)
    for oid in OFFSET_APPROVER_OPEN_IDS:
        aid = (oid or "").strip()
        if not aid:
            continue
        r = send_message(aid, body, msg_type="interactive", receive_id_type="open_id")
        if isinstance(r, dict) and int(r.get("code", -1)) != 0:
            print(f"[offsetleave] requester-edit notify failed for {aid!r}: {r!r}", flush=True)


def _notify_requester_offset_deleted_by_approver(
    send_message: Callable[..., Any],
    row: dict[str, Any],
    *,
    approver_name: str,
) -> None:
    request_person = str(row.get("request_person") or "").strip()
    if not request_person:
        return
    rid = str(row.get("record_id") or "").strip()
    oid = _requester_open_id_for_offset_row(request_person, record_id=rid)
    if not oid:
        print(
            f"[offsetleave] could not DM requester {request_person!r} about approver delete "
            f"(record {rid or '?'})",
            flush=True,
        )
        return
    card = build_offset_requester_approver_deleted_card(row, approver_name=approver_name)
    body = json.dumps(card, ensure_ascii=False)
    r = send_message(oid, body, msg_type="interactive", receive_id_type="open_id")
    if isinstance(r, dict) and int(r.get("code", -1)) != 0:
        print(f"[offsetleave] requester approver-delete DM failed: {r!r}", flush=True)


def _notify_offset_approvers_requester_deleted(
    send_message: Callable[..., Any],
    row: dict[str, Any],
    *,
    requester_name: str,
) -> None:
    if not OFFSET_APPROVER_OPEN_IDS:
        return
    card = build_offset_requester_deleted_notify_card(row, requester_name=requester_name)
    body = json.dumps(card, ensure_ascii=False)
    for oid in OFFSET_APPROVER_OPEN_IDS:
        aid = (oid or "").strip()
        if not aid:
            continue
        r = send_message(aid, body, msg_type="interactive", receive_id_type="open_id")
        if isinstance(r, dict) and int(r.get("code", -1)) != 0:
            print(f"[offsetleave] requester-delete notify failed for {aid!r}: {r!r}", flush=True)


def build_offset_direct_delete_card(restored: list[dict[str, Any]]) -> dict[str, Any]:
    """Red notice: offsets were deleted straight from the Base and put back."""
    n = len(restored)
    lines = [
        f"**{n} offset row(s) were deleted directly from the Base** and have been "
        "**restored** automatically.",
        "",
        "Offsets must be removed with the bot menu (`deleteoffset`), not by deleting "
        "the row in the sheet — a direct delete skips the approval trail and leaves "
        "the duty roster out of sync.",
        "",
        "**Kindly ask whoever removed these to use `deleteoffset` instead.**",
        "",
        "**Restored:**",
    ]
    for r in restored[:12]:
        summary = str(r.get("summary") or "").strip() or "offset row"
        lines.append(f"• {summary}")
    if n > 12:
        lines.append(f"• …and {n - 12} more")
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "red",
            "title": {"tag": "plain_text", "content": "⚠️ Offset deleted directly from the sheet"},
        },
        "body": {
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}}
            ]
        },
    }


def notify_offset_direct_delete_restored(restored: list[dict[str, Any]]) -> None:
    """Tell every offset approver that a direct delete was undone."""
    if not restored:
        return
    card = build_offset_direct_delete_card(restored)
    payload = json.dumps(card, ensure_ascii=False)
    for oid in sorted(OFFSET_APPROVER_OPEN_IDS):
        if not oid:
            continue
        try:
            _lark_im_send_message(
                oid, payload, msg_type="interactive", receive_id_type="open_id"
            )
        except Exception as exc:  # noqa: BLE001 — one bad id must not stop the rest
            print(
                f"[offsetleave] direct-delete notice failed for {oid}: {exc!r}",
                flush=True,
            )


def _notify_offset_approvers_deleted(
    row: dict[str, Any],
    *,
    deleter_label: str,
    deleter_known: bool,
    exclude_open_id: str = "",
    send_message: Optional[Callable[..., Any]] = None,
) -> bool:
    """
    DM every approver that an offset row was deleted (any method). Skips ``exclude_open_id``
    (the approver who performed the delete themselves). Returns True if every DM succeeded.
    """
    if not OFFSET_APPROVER_OPEN_IDS:
        return True
    send = send_message or _lark_im_send_message
    card = build_offset_deleted_notify_card(
        row, deleter_label=deleter_label, deleter_known=deleter_known
    )
    body = json.dumps(card, ensure_ascii=False)
    skip = (exclude_open_id or "").strip()
    all_ok = True
    for oid in OFFSET_APPROVER_OPEN_IDS:
        aid = (oid or "").strip()
        if not aid or aid == skip:
            continue
        r = send(aid, body, msg_type="interactive", receive_id_type="open_id")
        if isinstance(r, dict) and int(r.get("code", -1)) != 0:
            all_ok = False
            print(f"[offsetleave] deletion notify failed for {aid!r}: {r!r}", flush=True)
    return all_ok


def _notify_offset_deleter_confirm(
    send_message: Callable[..., Any],
    row: dict[str, Any],
    *,
    deleter_open_id: str,
) -> None:
    """DM the approver who deleted the row — peers get the alert card; actor gets this confirm."""
    oid = (deleter_open_id or "").strip()
    if not oid:
        return
    card = build_offset_deleted_actor_confirm_card(row)
    body = json.dumps(card, ensure_ascii=False)
    r = send_message(oid, body, msg_type="interactive", receive_id_type="open_id")
    if isinstance(r, dict) and int(r.get("code", -1)) != 0:
        print(f"[offsetleave] deleter confirm DM failed for {oid!r}: {r!r}", flush=True)


def _notify_other_offset_approvers_responded(
    send_message: Callable[..., Any],
    row: dict[str, Any],
    *,
    acting_approver_open_id: str,
    approver_name: str,
    decision: str,
    remarks: str,
) -> None:
    rid = str(row.get("record_id") or "").strip()
    dec = (decision or "").strip().title()
    if rid and _peer_approver_approval_already_notified(rid, dec):
        return
    actor = (acting_approver_open_id or "").strip() or _acting_approver_open_id_from_name(approver_name)
    card = build_offset_other_approver_responded_card(
        row,
        approver_name=approver_name,
        decision=decision,
        remarks=remarks,
    )
    body = json.dumps(card, ensure_ascii=False)
    targets = [
        (oid or "").strip()
        for oid in OFFSET_APPROVER_OPEN_IDS
        if (oid or "").strip() and (oid or "").strip() != actor
    ]
    if not targets:
        if rid:
            _mark_peer_approver_approval_notified(rid, dec)
        return
    all_ok = True
    for aid in targets:
        r = send_message(aid, body, msg_type="interactive", receive_id_type="open_id")
        if isinstance(r, dict) and int(r.get("code", -1)) != 0:
            all_ok = False
            print(f"[offsetleave] peer approver DM failed for {aid!r}: {r!r}", flush=True)
    if rid and all_ok:
        _mark_peer_approver_approval_notified(rid, dec)


def _assert_offset_card_actor(
    parsed: dict[str, Any],
    sender_open_id: str,
    token: str,
) -> tuple[str, Optional[str], bool]:
    """Return (owner_open_id, request_person_roster_or_None, is_approver_admin_flow)."""
    owner = str(parsed.get("owner") or "").strip()
    sender = (sender_open_id or "").strip()
    if not owner or owner != sender:
        raise ValueError("This action is only for the user who opened the menu.")
    if _parsed_admin_flag(parsed):
        if not _is_offset_approver_open_id(sender):
            raise ValueError("Only configured offset approvers can use this admin action.")
        return owner, None, True
    rp_val = str(parsed.get("request_person") or "").strip()
    if not rp_val:
        raise ValueError("Missing session.")
    rp_live = resolve_request_person(sender, token)
    if od._title_name(rp_live) != od._title_name(rp_val):
        raise ValueError("Identity mismatch for this action.")
    return owner, rp_live, False


def _build_offset_edit_approver_empty_patch_card() -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "grey", "title": {"tag": "plain_text", "content": "OSE offset — edit (approver)"}},
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "plain_text",
                        "content": "No approved or rejected offset records left to edit.",
                    },
                }
            ]
        },
    }


def _build_offset_delete_approver_empty_patch_card() -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "grey", "title": {"tag": "plain_text", "content": "OSE offset — delete (approver)"}},
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "plain_text",
                        "content": "No offset records left to delete.",
                    },
                }
            ]
        },
    }


def _build_offset_edit_empty_patch_card(request_person: str) -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "grey", "title": {"tag": "plain_text", "content": "OSE offset — edit"}},
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "plain_text",
                        "content": f"No pending offset found that you requested ({request_person}).",
                    },
                }
            ]
        },
    }


def _build_offset_delete_empty_patch_card(request_person: str) -> dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": "grey", "title": {"tag": "plain_text", "content": "OSE offset — delete"}},
        "body": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "plain_text",
                        "content": f"No pending offset requests left to delete ({request_person}).",
                    },
                }
            ]
        },
    }


def _patch_my_offset_list_after_change(
    *,
    message_id: str,
    mode: str,
    owner_open_id: str,
    request_person: str,
) -> None:
    mid = (message_id or "").strip()
    if not mid:
        return
    m = (mode or "").strip().lower()
    if m == "edit_admin":
        rows = _non_pending_offsets_all()
        card = (
            build_offset_edit_list_card(owner_open_id, "", rows, is_admin=True)
            if rows
            else _build_offset_edit_approver_empty_patch_card()
        )
    elif m == "delete_admin":
        rows = _all_offsets_for_approver_delete()
        card = (
            build_offset_delete_list_card(owner_open_id, "", rows, is_admin=True)
            if rows
            else _build_offset_delete_approver_empty_patch_card()
        )
    elif m == "edit":
        rows = _pending_offsets_for_request_person(request_person)
        card = (
            build_offset_edit_list_card(owner_open_id, request_person, rows, is_admin=False)
            if rows
            else _build_offset_edit_empty_patch_card(request_person)
        )
    elif m == "delete":
        rows = _pending_offsets_for_request_person(request_person)
        card = (
            build_offset_delete_list_card(owner_open_id, request_person, rows, is_admin=False)
            if rows
            else _build_offset_delete_empty_patch_card(request_person)
        )
    else:
        return
    if not _try_patch_interactive_card_message(mid, card):
        print(f"[offsetleave] list refresh patch skipped ({m})", flush=True)


def _handle_offset_delete_month(
    parsed: dict[str, Any],
    event_obj: dict[str, Any],
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., Any],
    webhook_data: Optional[dict[str, Any]],
) -> bool:
    """Approver picked a month → replace the picker with that month's delete list."""
    cid = (chat_id or "").strip()
    mid = _event_message_id(event_obj, webhook_data)
    try:
        oid = (sender_open_id or "").strip()
        if not _is_offset_approver_open_id(oid):
            _toast_approval_problem(
                send_message, cid, "Only an offset approver can use this menu."
            )
            return True
        # Re-read live rather than trusting the card: rows may have been deleted or
        # approved since the picker was built.
        all_rows = _all_offsets_for_approver_delete()
        if parsed.get("all"):  # "◀ All months" → back to the month picker
            if not all_rows:
                _toast_approval_problem(send_message, cid, "No offset records left.")
                return True
            card = build_offset_delete_month_picker_card(oid, all_rows)
        else:
            try:
                year = int(parsed.get("y"))
                month = int(parsed.get("m"))
            except (TypeError, ValueError):
                raise ValueError("missing month")
            try:
                start = max(0, int(parsed.get("off") or 0))
            except (TypeError, ValueError):
                start = 0
            rows = _filter_offsets_by_month(all_rows, year, month)
            label = _month_filter_label(year, month)
            if not rows:
                _toast_approval_problem(
                    send_message, cid, f"No offset records left for {label}."
                )
                return True
            card = build_offset_delete_list_card(
                oid,
                "",
                rows,
                is_admin=True,
                month_label=label,
                month_ym=(year, month),
                start=start,
            )
        if not (mid and _try_patch_interactive_card_message(mid, card)):
            token = od.get_tenant_access_token()
            if cid:
                _send_ephemeral_card(cid, oid, card, token)
            else:
                raise ValueError("Could not open the month list — run deleteoffset again.")
    except Exception as exc:  # noqa: BLE001 — surface as a toast, never 500 the callback
        print(f"[offsetleave] delete month pick failed: {exc!r}", flush=True)
        _toast_approval_problem(send_message, cid, f"deleteoffset: {exc}")
    return True


def _handle_offset_edit_pick(
    parsed: dict[str, Any],
    event_obj: dict[str, Any],
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., Any],
    webhook_data: Optional[dict[str, Any]],
) -> bool:
    cid = (chat_id or "").strip()
    mid = _event_message_id(event_obj, webhook_data)
    try:
        token = od.get_tenant_access_token()
        owner, rp_live, is_admin = _assert_offset_card_actor(parsed, sender_open_id, token)
        rid = str(parsed.get("record_id") or "").strip()
        if not rid:
            raise ValueError("missing record id")
        if not is_admin and _is_edit_form_open(owner, rid):
            _toast_approval_problem(
                send_message,
                cid,
                "You already opened the edit form for this request. "
                "Finish **Save** on that form, or run **editoffset** again to start over.",
            )
            return True
        row = _offset_admin_row_by_id(rid)
        if is_admin:
            if not mid:
                raise ValueError("missing message id")
            if bool(row.get("pending")):
                raise ValueError(
                    "This record is still pending. Approver editoffset is for approved/rejected rows only; "
                    "use your normal editoffset as the requester for pending items."
                )
            req_disp = str(row.get("request_person") or "").strip()
            form_card = build_offset_edit_form_card(
                owner_open_id=owner,
                request_person=req_disp,
                row=row,
                is_admin=True,
            )
            if not _try_patch_interactive_card_message(mid, form_card):
                if cid:
                    _send_ephemeral_card(cid, owner, form_card, token)
                else:
                    raise ValueError("Could not open edit form — run editoffset again.")
        else:
            if not bool(row.get("pending")):
                raise ValueError("That request is no longer pending (already approved or rejected).")
            if od._title_name(str(row.get("request_person") or "")) != od._title_name(rp_live or ""):
                raise ValueError("That offset is not yours to edit.")
            form_card = build_offset_edit_form_card(
                owner_open_id=owner,
                request_person=rp_live or "",
                row=row,
                is_admin=False,
            )
            opened = False
            if mid and _try_patch_interactive_card_message(mid, form_card):
                _mark_edit_form_open(owner, rid, mid)
                opened = True
            elif cid:
                _send_ephemeral_card(cid, owner, form_card, token)
                _mark_edit_form_open(owner, rid)
                opened = True
            if not opened:
                raise ValueError("Could not open edit form — run editoffset again.")
    except Exception as e:
        if cid:
            send_message(chat_id, f"❌ {e}")
        else:
            print(f"[offsetleave] edit pick: {e!r}", flush=True)
    return True


def _handle_offset_edit_submit(
    parsed: dict[str, Any],
    event_obj: dict[str, Any],
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., Any],
    webhook_data: Optional[dict[str, Any]],
) -> bool:
    cid = (chat_id or "").strip()
    mid = _event_message_id(event_obj, webhook_data)
    try:
        token = od.get_tenant_access_token()
        owner, rp_live, is_admin = _assert_offset_card_actor(parsed, sender_open_id, token)
        rid = str(parsed.get("record_id") or "").strip()
        if not rid:
            raise ValueError("missing record id")
        action = event_obj.get("action") if isinstance(event_obj.get("action"), dict) else {}
        exchange_person = _get_form_field(action, parsed, event_obj, "exchange_person")
        shift_type = _get_form_field(action, parsed, event_obj, "shift_type")
        original_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "original_date"))
        exchange_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "exchange_date"))
        reason = _get_form_field(action, parsed, event_obj, "reason")
        if not reason or not exchange_person or not shift_type:
            raise ValueError("Please fill Exchange person, Shift, dates, and Reason.")
        if is_admin:
            row_chk = _offset_admin_row_by_id(rid)
            if bool(row_chk.get("pending")):
                raise ValueError("Cannot use approver save on a pending row.")
            od.update_ose_offset_record_fields(
                record_id=rid,
                exchange_person=exchange_person,
                shift_type=shift_type,
                original_date=original_date,
                exchange_date=exchange_date,
                reason=reason,
            )
            _clear_edit_form_open(owner, rid)
            fresh_admin = _offset_admin_row_by_id(rid)
            _finish_offset_edit_saved_ui(
                owner_open_id=owner,
                request_person=str(fresh_admin.get("request_person") or ""),
                row=fresh_admin,
                message_id=mid,
                chat_id=cid,
                send_message=send_message,
                token=token,
            )
        else:
            row_chk = _offset_admin_row_by_id(rid)
            if not bool(row_chk.get("pending")):
                raise ValueError(
                    "This request is no longer pending (already approved or rejected). "
                    "Run editoffset again to refresh the list."
                )
            if od._title_name(str(row_chk.get("request_person") or "")) != od._title_name(rp_live or ""):
                raise ValueError("Not your request to edit.")
            od.update_ose_offset_request(
                record_id=rid,
                request_person=rp_live or "",
                exchange_person=exchange_person,
                shift_type=shift_type,
                original_date=original_date,
                exchange_date=exchange_date,
                reason=reason,
            )
            _clear_edit_form_open(owner, rid)
            fresh = _offset_admin_row_by_id(rid)
            try:
                _notify_offset_approvers_requester_edited(
                    send_message,
                    fresh,
                    requester_name=rp_live or str(fresh.get("request_person") or ""),
                )
            except Exception as exc:
                print(f"[offsetleave] requester-edit approver notify failed: {exc!r}", flush=True)
            _finish_offset_edit_saved_ui(
                owner_open_id=owner,
                request_person=rp_live or str(fresh.get("request_person") or ""),
                row=fresh,
                message_id=mid,
                chat_id=cid,
                send_message=send_message,
                token=token,
            )
    except Exception as e:
        if cid:
            send_message(chat_id, f"❌ Save failed: {e}")
        else:
            print(f"[offsetleave] edit submit: {e!r}", flush=True)
    return True


def _handle_offset_delete_row(
    parsed: dict[str, Any],
    event_obj: dict[str, Any],
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., Any],
    webhook_data: Optional[dict[str, Any]],
) -> bool:
    cid = (chat_id or "").strip()
    mid = _event_message_id(event_obj, webhook_data)
    owner = ""
    rid = ""
    try:
        token = od.get_tenant_access_token()
        owner, rp_live, is_admin = _assert_offset_card_actor(parsed, sender_open_id, token)
        rid = str(parsed.get("record_id") or "").strip()
        if not rid:
            raise ValueError("missing record id")
        busy = _try_begin_offset_delete(owner, rid)
        if busy:
            _toast_approval_problem(send_message, cid, f"⏳ {busy}")
            return True
        row_chk = _lookup_offset_row(rid, bust_cache=True)
        if row_chk is None:
            raise ValueError(
                "This record was already deleted or is no longer in the table. "
                "Run **deleteoffset** to refresh the list."
            )
        if is_admin:
            was_pending = bool(row_chk.get("pending"))
        else:
            was_pending = False
            if not bool(row_chk.get("pending")):
                raise ValueError(
                    "This request is no longer pending (already approved or rejected). "
                    "Run deleteoffset again to refresh the list."
                )
            if od._title_name(str(row_chk.get("request_person") or "")) != od._title_name(rp_live or ""):
                raise ValueError("Not your request to delete.")
        deleted_snapshot = dict(row_chk)
        if is_admin:
            try:
                actor_name = _approver_display_for_bitable(owner)
            except Exception:
                actor_name = ""
        else:
            actor_name = rp_live or str(row_chk.get("request_person") or "")
        # Remember who deleted it so the deletion poll can attribute the operator if the
        # immediate approver DM below fails (best-effort; manual Base deletes stay unknown).
        _record_offset_delete_actor(rid, owner, actor_name)
        try:
            od.delete_ose_offset_record(record_id=rid)
        except RuntimeError as exc:
            err = str(exc).lower()
            if "not found" in err or "record not exist" in err or "125404" in err:
                od.invalidate_ose_bitable_cache()
            else:
                raise
        od.invalidate_ose_bitable_cache()
        if is_admin:
            # Approver deleted it — alert OTHER approvers (not the actor), then confirm to actor.
            try:
                _notify_offset_approvers_deleted(
                    deleted_snapshot,
                    deleter_label=actor_name or "an approver",
                    deleter_known=True,
                    exclude_open_id=owner,
                    send_message=send_message,
                )
                _mark_offset_deletion_notified(rid)
            except Exception as exc:
                print(f"[offsetleave] approver-delete peer notify failed: {exc!r}", flush=True)
            try:
                _notify_offset_deleter_confirm(
                    send_message,
                    deleted_snapshot,
                    deleter_open_id=owner,
                )
            except Exception as exc:
                print(f"[offsetleave] approver-delete deleter confirm failed: {exc!r}", flush=True)
            if was_pending:
                try:
                    _unmark_offset_record_notified(rid)
                    _notify_requester_offset_deleted_by_approver(
                        send_message,
                        deleted_snapshot,
                        approver_name=actor_name or "Approver",
                    )
                except Exception as exc:
                    print(f"[offsetleave] approver-delete requester notify failed: {exc!r}", flush=True)
            rows = _all_offsets_for_approver_delete()
            # Stay on the month/page the approver was working in (the delete button
            # carries y/m/off); without this the rebuild fell back to the unfiltered
            # list and appeared to "lose" the month.
            _ym: Optional[tuple[int, int]] = None
            _label = None
            _start = 0
            try:
                if parsed.get("y") and parsed.get("m"):
                    _ym = (int(parsed["y"]), int(parsed["m"]))
                    _label = _month_filter_label(*_ym)
                    rows = _filter_offsets_by_month(rows, *_ym)
                    _start = max(0, int(parsed.get("off") or 0))
                    if _start >= len(rows):  # last row on the page was deleted
                        _start = max(0, ((len(rows) - 1) // _OFFSET_DELETE_PAGE) * _OFFSET_DELETE_PAGE)
            except (TypeError, ValueError):
                _ym, _label, _start = None, None, 0
            card = (
                build_offset_delete_list_card(
                    owner,
                    "",
                    rows,
                    is_admin=True,
                    month_label=_label,
                    month_ym=_ym,
                    start=_start,
                )
                if rows
                else _build_offset_delete_approver_empty_patch_card()
            )
            fallback = "✅ Offset record deleted."
        else:
            rp = rp_live or str(row_chk.get("request_person") or "")
            try:
                _notify_offset_approvers_requester_deleted(
                    send_message,
                    deleted_snapshot,
                    requester_name=rp,
                )
                _unmark_offset_record_notified(rid)
                _mark_offset_deletion_notified(rid)
            except Exception as exc:
                print(f"[offsetleave] requester-delete approver notify failed: {exc!r}", flush=True)
            rows = _pending_offsets_for_request_person(rp)
            card = (
                build_offset_delete_list_card(owner, rp, rows, is_admin=False)
                if rows
                else _build_offset_delete_empty_patch_card(rp)
            )
            fallback = f"✅ Deleted pending offset for {rp}."
        _finish_ephemeral_card_ui(
            owner_open_id=owner,
            chat_id=cid,
            card=card,
            message_id=mid,
            send_message=send_message,
            token=token,
            fallback_text=fallback,
        )
    except KeyError:
        _toast_approval_problem(
            send_message,
            cid,
            "❌ This record is no longer available (may already be deleted). Run **deleteoffset** again.",
        )
    except Exception as e:
        if cid:
            send_message(chat_id, f"❌ Delete failed: {e}")
        else:
            print(f"[offsetleave] delete: {e!r}", flush=True)
    finally:
        if owner and rid:
            _end_offset_delete(owner, rid)
    return True


def _handle_offset_approval_callback(
    parsed: dict[str, Any],
    event_obj: dict[str, Any],
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., Any],
    webhook_data: Optional[dict[str, Any]],
) -> bool:
    key = str(parsed.get("k") or "").strip().lower()
    operator = _operator_open_id(event_obj, sender_open_id)
    mid = _event_message_id(event_obj, webhook_data)
    if not OFFSET_APPROVER_OPEN_IDS or not _is_offset_approver_open_id(operator):
        try:
            if mid:
                _patch_interactive_card_message(mid, _build_offset_approval_denied_card())
        except Exception:
            pass
        _toast_approval_problem(send_message, chat_id, "❌ Only the assigned approver can act on this card.")
        return True
    try:
        if key == _OFFSET_APPR_PICK_KEY:
            rid = str(parsed.get("record_id") or "").strip()
            dec = _norm_offset_decision(parsed.get("decision"))
            if not rid:
                raise ValueError("missing record_id")
            row = _offset_admin_row_by_id(rid)
            if not mid:
                raise ValueError("missing message id for card update")
            _patch_interactive_card_message(mid, build_offset_approver_confirm_card(row, dec))
            return True
        if key == _OFFSET_APPR_CONFIRM_KEY:
            rid = str(parsed.get("record_id") or "").strip()
            dec = _norm_offset_decision(parsed.get("decision"))
            action = event_obj.get("action") if isinstance(event_obj.get("action"), dict) else {}
            remarks = _get_form_field(action, parsed, event_obj, "approval_remarks")
            if not rid:
                raise ValueError("missing record_id")
            _offset_admin_row_by_id(rid)
            approver_name = _approver_display_for_bitable(operator)
            od.update_ose_offset_approval(
                record_id=rid,
                status=dec,
                approver=approver_name,
                remarks=remarks,
                approver_open_id=operator,
            )
            fresh = _offset_admin_row_by_id(rid)
            if mid:
                _patch_interactive_card_message(mid, build_offset_approver_done_card(fresh, dec, remarks))
            try:
                notify_offset_approval_decision(
                    rid,
                    send_message=send_message,
                    approver_name=approver_name,
                    decision=dec,
                    remarks=remarks,
                    acting_approver_open_id=operator,
                )
            except Exception as exc:
                print(f"[offsetleave] approval notify failed: {exc!r}", flush=True)
            return True
    except Exception as exc:
        try:
            if mid:
                _patch_interactive_card_message(mid, _build_offset_approval_error_card(str(exc)))
        except Exception:
            pass
        _toast_approval_problem(send_message, chat_id, f"❌ Offset approval failed: {exc}")
    return True


def handle_card_callback(
    parsed: dict[str, Any],
    event_obj: dict[str, Any],
    *,
    sender_open_id: str,
    chat_id: str,
    send_message: Callable[..., dict[str, Any]],
    webhook_data: Optional[dict[str, Any]] = None,
) -> bool:
    key = str(parsed.get("k") or "").strip().lower()
    if key in OFFSET_APPROVAL_CALLBACK_KEYS:
        return _handle_offset_approval_callback(
            parsed,
            event_obj,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            send_message=send_message,
            webhook_data=webhook_data,
        )
    if key == _OFFSET_EDIT_PICK_KEY:
        return _handle_offset_edit_pick(
            parsed,
            event_obj,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            send_message=send_message,
            webhook_data=webhook_data,
        )
    if key == _OFFSET_EDIT_SUBMIT_KEY:
        return _handle_offset_edit_submit(
            parsed,
            event_obj,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            send_message=send_message,
            webhook_data=webhook_data,
        )
    if key == _OFFSET_DELETE_MONTH_KEY:
        return _handle_offset_delete_month(
            parsed,
            event_obj,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            send_message=send_message,
            webhook_data=webhook_data,
        )
    if key == _OFFSET_DELETE_KEY:
        return _handle_offset_delete_row(
            parsed,
            event_obj,
            sender_open_id=sender_open_id,
            chat_id=chat_id,
            send_message=send_message,
            webhook_data=webhook_data,
        )
    if key not in (_OFFSET_SUBMIT_KEY, _LEAVE_SUBMIT_KEY):
        return False
    cid = (chat_id or "").strip()
    try:
        action = event_obj.get("action") if isinstance(event_obj.get("action"), dict) else {}
        owner, request_person = _assert_owner(
            parsed, sender_open_id, action=action, event_obj=event_obj
        )
        reason = _get_form_field(action, parsed, event_obj, "reason")
        if not reason:
            if cid:
                send_message(chat_id, "❌ Reason is required.")
            return True
        if key == _OFFSET_SUBMIT_KEY:
            try:
                request_person = resolve_request_person(
                    owner, od.get_tenant_access_token()
                )
            except ValueError as exc:
                if cid:
                    send_message(chat_id, f"❌ {exc}")
                return True
            exchange_person = _get_form_field(action, parsed, event_obj, "exchange_person")
            shift_type = _get_form_field(action, parsed, event_obj, "shift_type")
            original_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "original_date"))
            exchange_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "exchange_date"))
            if not exchange_person or not shift_type:
                if cid:
                    send_message(chat_id, "❌ Please fill Exchange person and Shift.")
                return True
            fp = offset_submit_fingerprint(
                request_person=request_person,
                exchange_person=exchange_person,
                shift_type=shift_type,
                original_date=original_date,
                exchange_date=exchange_date,
                reason=reason,
            )
            dup_err = try_begin_offset_submit(owner, fp)
            mid = _event_message_id(event_obj, webhook_data)
            if dup_err:
                _toast_approval_problem(send_message, cid, f"❌ {dup_err}")
                if mid:
                    _try_patch_interactive_card_message(
                        mid,
                        build_offset_submit_done_card(
                            request_person=request_person,
                            record_id="",
                            message=dup_err,
                        ),
                    )
                return True
            _dismiss_ephemeral_form(mid)
            try:
                out = od.submit_ose_offset(
                    request_person=request_person,
                    exchange_person=exchange_person,
                    shift_type=shift_type,
                    original_date=original_date,
                    exchange_date=exchange_date,
                    reason=reason,
                )
                rid = str((out or {}).get("record_id") or "").strip()
                if rid:
                    remember_offset_requester_open_id(rid, owner)
                release_offset_submit(owner, fp, success=True)
                done_msg = f"✅ Offset submitted for {request_person} (record {rid or 'saved'})."
                done_card = build_offset_submit_done_card(
                    request_person=request_person,
                    record_id=rid,
                    message=done_msg,
                )
                _finish_ephemeral_card_ui(
                    owner_open_id=owner,
                    chat_id=cid,
                    card=done_card,
                    message_id=mid,
                    send_message=send_message,
                    token=od.get_tenant_access_token(),
                    fallback_text=done_msg,
                )
            except Exception as submit_exc:
                release_offset_submit(owner, fp, success=False)
                raise submit_exc
            return True
        leave_type = _get_form_field(action, parsed, event_obj, "leave_type")
        start_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "start_date"))
        end_date = _parse_date_iso(_get_form_field(action, parsed, event_obj, "end_date"))
        if not leave_type:
            if cid:
                send_message(chat_id, "❌ Please choose a leave type.")
            return True
        out = od.submit_ose_leave(
            name=request_person,
            leave_type=leave_type,
            start_date=start_date,
            end_date=end_date,
            reason=reason,
        )
        rid = str((out or {}).get("record_id") or "").strip()
        _dismiss_ephemeral_form(_event_message_id(event_obj, webhook_data))
        if cid:
            send_message(
                chat_id,
                f"✅ Leave submitted for {request_person} (record {rid or 'saved'}).",
            )
    except Exception as e:
        if cid:
            send_message(chat_id, f"❌ Submit failed: {e}")
        else:
            print(f"[offsetleave] submit failed (no chat_id): {e!r}", flush=True)
    return True
