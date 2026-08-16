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

hr("4. DOES IMAP SEARCH WORK ON THIS SERVER?")
# Lark implements only the date criteria the indexer uses. If SUBJECT/HEADER searches come
# back empty on a folder the index says holds these mails, SEARCH is the broken part — not
# the mailbox — and every search-based lookup is dead on arrival.
needle = TITLE.split(" - ")[0][:48]
try:
    m = mm._connect_imap_simple(timeout=30)
    try:
        for folder in mm.EGS_REPLY_IMAP_FOLDERS:
            if not mm._select_mail_folder(m, folder, readonly=True):
                print(f"   {folder!r}: SELECT failed (folder missing?)")
                continue
            safe = needle.replace('"', "").replace("\\", "")
            n_subj = len(mm._uid_search(m, f'(SUBJECT "{safe}")') or [])
            n_mid = 0
            if stored_mid:
                s = stored_mid.replace('"', "").replace("\\", "")
                n_mid = len(mm._uid_search(m, f'(HEADER Message-ID "{s}")') or [])
            n_since = len(mm._uid_search(m, "(SINCE 01-Aug-2026)") or [])
            print(f"   {folder!r}: SUBJECT={n_subj}  HEADER-Message-ID={n_mid}  SINCE={n_since}")
    finally:
        try:
            m.logout()
        except Exception:
            pass
except Exception as ex:
    print(f"!! search probe failed: {ex!r}")

hr("5. THE ROUTE THAT MATTERS — direct (folder, uid) fetch")
got = mm._quote_source_by_message_id(stored_mid, TITLE) if stored_mid else None
print(f"_quote_source_by_message_id -> {'MESSAGE' if got is not None else 'None'}")
if got is not None:
    print(f"   subject : {mm._decode_msg_subject(got)!r}")
    print(f"   date    : {got.get('Date')!r}")
newest = mm._thread_newest_quote(
    message_id=stored_mid, subject=TITLE, references="",
    not_older_than=mm._message_date_ts(got) if got is not None else 0.0,
)
print(f"_thread_newest_quote        -> {'MESSAGE' if newest is not None else 'None'}")
if newest is not None:
    print(f"   subject : {mm._decode_msg_subject(newest)!r}")
    print(f"   date    : {newest.get('Date')!r}")
    print(f"   from    : {newest.get('From')!r}")

hr("VERDICT")
final = newest or got
if final is None:
    print("No quote source resolvable -> replies will still send unquoted.")
else:
    html = mm.build_reply_message_html("TESTING", final)
    ok = "adit-html-block--collapsed" in html and "history-quote-wrapper" in html
    print(f"Quote source resolved; reply HTML carries the Lark reply shape: {ok}")
    print(f"Quoting: {mm._decode_msg_subject(final)!r} ({final.get('Date')})")
    print("-> /egsreply(test) should now render Show/Hide email thread.")
