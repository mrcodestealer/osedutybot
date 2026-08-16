#!/usr/bin/env python3
"""Read-only diagnostic: why does /egsreply(test) log ``quoted=False``?

Answers, in order:
  1. What Message-ID did we STORE at send time (egstest.json / egs.json)?
  2. What folders does this mailbox actually have? (a Lark ``Sent`` may not be named "Sent")
  3. Is the subject in the allemail index at all?
  4. Does a LIVE IMAP subject search find the mail — and what is its REAL Message-ID?

Nothing is sent and every SELECT is readonly. Run from the repo root:

    python diag_egsreply_quote.py "NT auth/player v2.0.6 UPDATE PRODUCTION - 12/08/2026"
"""
import sys

import maintenance_mail as mm

TITLE = (sys.argv[1] if len(sys.argv) > 1 else "").strip()
TEST = "--real" not in sys.argv
if not TITLE:
    print("usage: python diag_egsreply_quote.py '<exact email title>' [--real]")
    raise SystemExit(2)


def hr(t):
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}")


hr(f"1. STORED SEND  ({'egstest.json' if TEST else 'egs.json'})")
stored = mm.egs_store_lookup(TITLE, test=TEST)
stored_mid = ""
if not stored:
    print(f"!! no stored entry for {TITLE!r} — picker/store mismatch")
else:
    stored_mid = str(stored.get("message_id") or "")
    print(f"subject    : {stored.get('subject')!r}")
    print(f"message_id : {stored_mid!r}   <-- what find_message_by_message_id searches for")
    print(f"to/cc      : {stored.get('to')} / {stored.get('cc')}")
    print(f"sent at    : {stored.get('at')}")

hr("2. FOLDERS THIS MAILBOX ACTUALLY HAS")
print(f"configured EGS_REPLY_IMAP_FOLDERS : {mm.EGS_REPLY_IMAP_FOLDERS}")
print(f"configured allemail folders       : {mm._allemail_folders()}")
real_folders = []
try:
    m = mm._connect_imap_simple(timeout=30)
    try:
        typ, data = m.list()
        for raw in data or []:
            line = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)
            name = line.split(' "/" ')[-1].strip().strip('"') if ' "/" ' in line else line
            real_folders.append(name)
        print("server folders:")
        for f in real_folders:
            print(f"   - {f}")
        missing = [
            f for f in mm.EGS_REPLY_IMAP_FOLDERS
            if f.casefold() not in {r.casefold() for r in real_folders}
        ]
        if missing:
            print(f"\n!! CONFIGURED BUT NOT ON SERVER: {missing}")
            print("   -> those folders are silently skipped, so nothing there can be quoted")
    finally:
        try:
            m.logout()
        except Exception:
            pass
except Exception as ex:
    print(f"!! folder list failed: {ex!r}")

hr("3. ALLEMAIL INDEX")
try:
    with mm._allemail_lock:
        entries = mm._allemail_load().get("emails", [])
    print(f"cache enabled : {mm._allemail_enabled()}")
    print(f"indexed mails : {len(entries)}")
    key = mm._thread_subject_key(TITLE)
    print(f"subject key   : {key!r}")
    hits = [e for e in entries if mm._thread_subject_key(e.get("subject") or "") == key]
    print(f"same-thread   : {len(hits)}")
    for e in sorted(hits, key=lambda e: float(e.get("date_ts") or 0), reverse=True):
        print(
            f"   {(e.get('date') or '?')[:16]}  {e.get('folder')!r}/{e.get('uid')!r}  "
            f"mid={e.get('message_id')!r}  from={e.get('from_raw')!r}"
        )
    if not hits:
        print("   -> nothing indexed for this thread; _thread_newest_quote can only return None")
except Exception as ex:
    print(f"!! allemail read failed: {ex!r}")

hr("4. LIVE IMAP — does the mail exist, and what is its REAL Message-ID?")
needle = TITLE.split(" - ")[0][:48]
print(f"searching SUBJECT ~ {needle!r} in {mm.EGS_REPLY_IMAP_FOLDERS}\n")
found_any = False
try:
    m = mm._connect_imap_simple(timeout=30)
    try:
        for folder in mm.EGS_REPLY_IMAP_FOLDERS:
            if not mm._select_mail_folder(m, folder, readonly=True):
                print(f"   {folder!r}: SELECT failed (folder missing?)")
                continue
            safe = needle.replace('"', "").replace("\\", "")
            uids = mm._uid_search(m, f'(SUBJECT "{safe}")')
            print(f"   {folder!r}: {len(uids or [])} match(es)")
            for uid in (uids or [])[-5:]:
                msg = mm._fetch_uid_message(m, uid)
                if msg is None:
                    continue
                found_any = True
                real_mid = (msg.get("Message-ID") or "").strip()
                same = (
                    mm._normalize_message_id(real_mid)
                    == mm._normalize_message_id(stored_mid)
                )
                print(f"      uid={uid!r} date={msg.get('Date')!r}")
                print(f"        real Message-ID : {real_mid!r}")
                print(f"        matches stored  : {same}"
                      + ("" if same else "   <-- REWRITTEN BY LARK ON SEND"))
    finally:
        try:
            m.logout()
        except Exception:
            pass
except Exception as ex:
    print(f"!! live search failed: {ex!r}")

hr("VERDICT")
if not found_any:
    print("The mail is NOT in any searched folder of this mailbox.")
    print("-> Lark is not keeping a copy we can read (no Sent copy / no self-delivery of Cc).")
    print("-> Nothing to quote: the only copy lives in the recipient's mailbox.")
elif stored_mid and not TEST:
    print("Mail found. Compare 'matches stored' above.")
else:
    print("Mail found — if 'matches stored' is False, the stored Message-ID is stale:")
    print("   find_message_by_message_id() can never hit, and threading headers point at")
    print("   an id no client will recognise. Fix = store the id the SERVER assigned,")
    print("   or resolve the quote source by subject instead of by Message-ID.")
