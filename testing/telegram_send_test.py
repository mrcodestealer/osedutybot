"""Regression test for /telegramsendjctest against a synthetic Telegram-Web DOM.

The fixture now models what the audit found the live client actually does:
  * rows carry data-peer-id and opening one sets location.hash to that id
  * emoji in titles render as <img class="emoji" alt="...">
  * pressing Enter in the composer POSTS an outgoing bubble and empties the box
    (or, when SEND_WITH_ENTER is off, inserts a newline and posts nothing)
  * the sidebar search box exists and returns "global" rows for strangers

Every refusal path is asserted to leave the composer empty and post nothing.

No network, no Telegram, no login. Run: python testing/telegram_send_test.py
"""
import os, sys, json
from pathlib import Path

proj = str(Path(__file__).resolve().parent.parent)
os.chdir(proj)
sys.path.insert(0, proj)
import telegramwarm as tw
from playwright.sync_api import sync_playwright


def row(title, peer, *, emoji=None, active=False, username=None):
    """peer=None omits data-peer-id (exercises the title-based fallbacks).
    username sets the hash the fixture will emit on open ('#@name'), as Web K does
    for any peer that has a username."""
    em = f'<img class="emoji" alt="{emoji}">' if emoji else ""
    pid = f' data-peer-id="{peer}"' if peer is not None else ""
    un = f' data-username="{username}"' if username else ""
    return (f'<li class="chatlist-chat{" active" if active else ""}"{pid}{un}>'
            f'<span class="user-title">{title}{em}'
            f'<span class="dialog-time">02:31 PM</span></span></li>')


def page_html(rows_html, *, header_for_click=True, composer=True,
              send_with_enter=True, with_search=False, global_rows_html="",
              header_text=None, post_bubble=True, extra_editable=False):
    comp = ('<div class="input-message-container">'
            '<div class="input-message-input" contenteditable="true"></div></div>'
            if composer else "")
    search = ('<input class="input-search-input" placeholder=" ">' if with_search else "")
    # Clicking a row: mark it active, set the hash to its peer id, and (optionally)
    # write its title into the header — the way the real client opens a conversation.
    # The isTrusted guard is essential: Web K ignores synthetic DOM clicks.
    click_js = f"""
      const openRow = (li) => {{
        document.querySelectorAll('#column-left li').forEach(o => o.classList.remove('active'));
        li.classList.add('active');
        // Web K: '#@username' when the peer has one, else '#<peer id>'; no hash change
        // at all when the row carries neither (fallback-verification fixtures).
        const un = li.getAttribute('data-username');
        const pid = li.getAttribute('data-peer-id');
        if (un) location.hash = '#@' + un; else if (pid) location.hash = '#' + pid;
        document.querySelector('#hdr').innerText =
          {json.dumps(header_text) if header_text is not None else
           ("li.querySelector('.user-title').childNodes[0].textContent.trim()" if header_for_click else "''")};
        // Deliberately NOT clearing #sent here. The real client keeps a chat's history
        // when it is re-opened; wiping it on every click hid the idempotency and force
        // behaviour (the first bubble vanished before the second call could see it).
        // Each case gets a fresh page, so nothing leaks between cases anyway.
      }};
      const wire = (li) => li.addEventListener('click', (e) => {{ if (!e.isTrusted) return; openRow(li); }});
      document.querySelectorAll('#column-left li').forEach(wire);
      // Search: typing a title INJECTS "global" stranger rows (users/channels this
      // account never spoke to). They are not in the DOM until a search runs, exactly
      // as on the live client — so a send that never searches can never see them.
      const s = document.querySelector('.input-search-input');
      if (s) s.addEventListener('input', () => {{
        const g = document.getElementById('global');
        if (!g) return;
        g.innerHTML = s.value.trim() ? {json.dumps(global_rows_html)} : '';
        g.querySelectorAll('li').forEach(wire);
      }});
      // Composer: Enter posts (when send_with_enter) and empties the box.
      const box = document.querySelector('.input-message-input');
      if (box) box.addEventListener('keydown', (e) => {{
        if (e.key !== 'Enter' || e.shiftKey) return;
        window.__enters = (window.__enters || 0) + 1;
        if (!{str(send_with_enter).lower()}) return;   // Ctrl+Enter mode: newline only
        e.preventDefault();
        const text = box.innerText;
        if (!text.trim()) return;
        if (!{str(post_bubble).lower()}) {{ box.innerText = ''; return; }}  // clears, posts nothing
        const b = document.createElement('div');
        b.className = 'bubble is-out';
        b.innerHTML = '<div class="message">' + text.replace(/\\n/g, '<br>') +
                      '<span class="time-inner">02:40 PM</span></div>';
        document.getElementById('sent').appendChild(b);
        box.innerText = '';
      }});
    """
    return f"""
    <style>
      .dialog-time{{display:block}}
      #column-left{{width:380px}}
      /* #global is empty until a search injects rows, so it needs no display:none —
         hiding it gave the injected stranger rows a zero box and made the read
         route's search look broken when it was the fixture. */
    </style>
    <div id="column-left">
      {search}
      <ul class="chatlist">{rows_html}</ul>
      <ul class="chatlist" id="global"></ul>
    </div>
    <div id="column-center">
      <div class="chat-info"><span class="peer-title" id="hdr"></span></div>
      <div class="bubbles" id="sent"></div>
      {comp}
      {'<div id="decoy" contenteditable="true"></div>' if extra_editable else ''}
    </div>
    <script>{click_js}</script>
    """


fails = 0


def check(label, got, want):
    global fails
    ok = got == want
    fails += not ok
    print(("  ok  " if ok else " FAIL "), f"{label}: {got!r}")


def composer(page):
    return (page.eval_on_selector(".input-message-input", "e => e.innerText") or "").strip()


def outgoing(page):
    return page.eval_on_selector_all(".bubble.is-out .message",
                                     "els => els.map(e => e.childNodes[0].textContent.trim())")


MSG = "Hi team, are there any maintenance plans for this week?"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    _pages = []

    def fresh(html):
        """A brand-new page per case. set_content replaces the DOM but NOT
        location.hash or accumulated listeners, so cases sharing one page leak
        state into each other — the exact flakiness that made pinned/multi-line
        fail in sequence while passing in isolation."""
        pg = browser.new_page(viewport={"width": 1400, "height": 900})
        pg.set_content(html)
        _pages.append(pg)
        return pg

    page = browser.new_page(viewport={"width": 1400, "height": 900})
    os.environ["TELEGRAM_TEST_CHAT"] = "jc"
    os.environ["TELEGRAM_TEST_MESSAGE"] = MSG
    os.environ.pop("TELEGRAM_TEST_CHAT_PEER", None)

    print("=== happy path: exact match, identity via peer id, send CONFIRMED by bubble ===")
    page = fresh(page_html(row("JC Team", 11) + row("jc", 22) + row("jcsia", 33) + row("Ops", 44)))
    res = tw._send_test_message(page)
    check("ok", res.get("ok"), True)
    check("status", res.get("status"), "sent")
    check("verified by peer id", res.get("verifiedBy"), "peer-id")
    check("peer reported", str(res.get("peerId")), "22")
    check("outgoing bubble holds the text", outgoing(page), [MSG])
    check("composer emptied", composer(page), "")
    check("exactly one Enter pressed", page.evaluate("() => window.__enters"), 1)

    print("\n=== case-SENSITIVE for sends: 'jc' must not resolve to 'JC' ===")
    page = fresh(page_html(row("JC", 55) + row("Ops", 44)))
    res = tw._send_test_message(page)
    check("refused", res.get("ok"), False)
    check("stage", res.get("stage"), "open")
    check("nothing posted", outgoing(page), [])

    print("\n=== emoji in title: 'jc' must not resolve to 'jc<fire>' ===")
    page = fresh(page_html(row("jc", 66, emoji="🔥") + row("Ops", 44)))
    r = page.evaluate(tw._FIND_CHAT_JS, {"wanted": "jc", "allowSubstring": False,
                                        "caseSensitive": True})
    check("emoji title read back with alt text", r["candidates"][0], "jc🔥")
    check("no exact match", r["matches"], 0)
    res = tw._send_test_message(page)
    check("refused", res.get("ok"), False)
    check("nothing posted", outgoing(page), [])

    print("\n=== both 'jc' and 'jc<fire>' rendered: only the plain one matches ===")
    page = fresh(page_html(row("jc", 22) + row("jc", 66, emoji="🔥")))
    r = page.evaluate(tw._FIND_CHAT_JS, {"wanted": "jc", "allowSubstring": False,
                                        "caseSensitive": True})
    check("exactly one match", r["matches"], 1)
    check("it is the plain one", str(r["peerId"]), "22")

    print("\n=== send route NEVER searches: a same-titled stranger in search results is unreachable ===")
    page = fresh(page_html(row("Ops", 44), with_search=True,
                               global_rows_html=row("jc", 999)))
    res = tw._send_test_message(page)
    check("refused (0 matches, no search)", res.get("ok"), False)
    check("stage", res.get("stage"), "open")
    check("search box untouched",
          page.eval_on_selector(".input-search-input", "e => e.value"), "")
    check("nothing posted", outgoing(page), [])
    # The READ route may search, and then finds the stranger — by design, read-only.
    r = tw._open_chat_by_title(page, "jc", allow_substring=True, allow_search=True,
                               accept_hash_change=True)
    check("read route reaches it via search", r.get("ok"), True)

    print("\n=== pinned target: same title, wrong peer -> refuse before clicking ===")
    os.environ["TELEGRAM_TEST_CHAT_PEER"] = "22"
    page = fresh(page_html(row("jc", 777) + row("Ops", 44)))
    res = tw._send_test_message(page)
    check("refused", res.get("ok"), False)
    check("reason names the pin", "pinned" in (res.get("reason") or ""), True)
    check("nothing posted", outgoing(page), [])
    page = fresh(page_html(row("jc", 22) + row("Ops", 44)))
    res = tw._send_test_message(page)
    check("correct peer sends", res.get("status"), "sent")
    check("verified by pin", res.get("verifiedBy"), "pinned-peer")
    os.environ.pop("TELEGRAM_TEST_CHAT_PEER", None)

    print("\n=== 'Send with Enter' OFF: Enter inserts a newline, nothing posts -> not_sent, draft cleared ===")
    page = fresh(page_html(row("jc", 22), send_with_enter=False))
    res = tw._send_test_message(page)
    check("not ok", res.get("ok"), False)
    check("status", res.get("status"), "not_sent")
    check("nothing posted", outgoing(page), [])
    check("draft cleared, not left behind", composer(page), "")

    print("\n=== multi-line message stays ONE message (Shift+Enter between lines) ===")
    os.environ["TELEGRAM_TEST_MESSAGE"] = "line one\nline two\nline three"
    page = fresh(page_html(row("jc", 22)))
    res = tw._send_test_message(page)
    check("sent", res.get("status"), "sent")
    check("exactly one Enter", page.evaluate("() => window.__enters"), 1)
    check("one bubble, not three", len(outgoing(page)), 1)
    os.environ["TELEGRAM_TEST_MESSAGE"] = MSG

    print("\n=== idempotency: text already the newest outgoing message -> skipped ===")
    page = fresh(page_html(row("jc", 22)))
    # Seed an existing outgoing bubble with our exact text. The fixture keeps history
    # across the row click, as the real client does, so no re-append trick is needed.
    page.evaluate("""() => {
      const b = document.createElement('div'); b.className = 'bubble is-out';
      b.innerHTML = '<div class="message">%s<span class="time-inner">01:00 PM</span></div>';
      document.getElementById('sent').appendChild(b);
    }""" % MSG)
    res = tw._send_test_message(page)
    check("ok (nothing to do)", res.get("ok"), True)
    check("status", res.get("status"), "already_sent")
    check("still exactly one bubble", len(outgoing(page)), 1)

    print("\n=== pre-Enter re-verify: chat switches after typing -> abort, composer cleared ===")
    page = fresh(page_html(row("jc", 22) + row("Ops", 44)))
    # Simulate the conversation changing under us the moment text is in the box.
    page.evaluate("""() => {
      const box = document.querySelector('.input-message-input');
      box.addEventListener('input', () => { location.hash = '#44'; }, { once: true });
    }""")
    res = tw._send_test_message(page)
    check("aborted", res.get("status"), "aborted")
    check("reason explains", "conversation changed" in (res.get("reason") or ""), True)
    check("nothing posted", outgoing(page), [])
    check("composer cleared", composer(page), "")

    print("\n=== ambiguous / missing / header-contradiction still refuse ===")
    page = fresh(page_html(row("jc", 1) + row("jc", 2)))
    res = tw._send_test_message(page)
    check("two exact rows -> refused", res.get("ok"), False)
    page = fresh(page_html(row("Ops", 44) + row("Alerts", 45)))
    res = tw._send_test_message(page)
    check("missing -> refused", res.get("ok"), False)
    page = fresh(page_html(row("jc", 22), header_for_click=False))
    # Header stays blank; identity still proven by peer id -> allowed.
    res = tw._send_test_message(page)
    check("blank header but peer id proven -> sends", res.get("status"), "sent")

    print("\n=== composer missing -> no_composer, nothing typed anywhere ===")
    page = fresh(page_html(row("jc", 22), composer=False))
    res = tw._send_test_message(page)
    check("status", res.get("status"), "no_composer")
    check("nothing posted", outgoing(page), [])

    print("\n=== _titles_match: case sensitivity is a parameter ===")
    check("case-sensitive rejects JC", tw._titles_match("JC", "jc", allow_substring=False,
                                                        case_sensitive=True), False)
    check("case-insensitive accepts JC", tw._titles_match("JC", "jc", allow_substring=False), True)
    check("whitespace normalised", tw._titles_match("  jc \n ", "jc", allow_substring=False,
                                                    case_sensitive=True), True)

    print("\n=== LIVE SHAPE: peer has a @username -> hash is '#@name', send must still work ===")
    # Web K puts the username in the hash for any peer that has one. The first patch
    # compared hash to data-peer-id and aborted EVERY such send after typing.
    os.environ.pop("TELEGRAM_TEST_CHAT_PEER", None)
    page = fresh(page_html(row("jc", 22, username="jcname") + row("Ops", 44)))
    res = tw._send_test_message(page)
    check("sent despite username hash", res.get("status"), "sent")
    check("identity via the clicked row, not the hash", res.get("verifiedBy"), "peer-row-active")
    check("hash reported as username", res.get("hash"), "#@jcname")
    check("bubble posted once", outgoing(page), [MSG])
    check("numeric peer for pinning, never '@name'", tw._display_peer(res), "22")

    print("\n=== username hash + numeric pin -> pinned-peer via the selected row ===")
    os.environ["TELEGRAM_TEST_CHAT_PEER"] = "22"
    page = fresh(page_html(row("jc", 22, username="jcname") + row("Ops", 44)))
    res = tw._send_test_message(page)
    check("sent", res.get("status"), "sent")
    check("verified by pin", res.get("verifiedBy"), "pinned-peer")
    os.environ.pop("TELEGRAM_TEST_CHAT_PEER", None)

    print("\n=== card never prints '@name' as the peer id ===")
    c = tw._build_check_card("jc", {"ok": True, "chat": "jc", "total": 1, "messages": [],
                                    "hash": "#@jcname", "peerId": "22", "verifiedBy": "peer-row-active"}, None)
    blob = json.dumps(c, ensure_ascii=False)
    check("numeric id printed", "Peer id: `22`" in blob, True)
    check("username not offered as a pin", "@jcname" in blob, False)

    print("\n=== Enter clears the composer but NO bubble appears -> 'unknown', never 'sent' ===")
    page = fresh(page_html(row("jc", 22), post_bubble=False))
    res = tw._send_test_message(page)
    check("not reported as sent", res.get("status"), "unknown")
    check("ok is False", res.get("ok"), False)

    print("\n=== header CONTRADICTS the row -> refuse at verify, nothing typed ===")
    page = fresh(page_html(row("jc", 22) + row("Ops", 44), header_text="Ops"))
    res = tw._send_test_message(page)
    check("refused", res.get("ok"), False)
    check("stage", res.get("stage"), "verify")
    check("reason names both titles", "expected 'jc'" in (res.get("reason") or ""), True)
    check("composer untouched", composer(page), "")
    check("nothing posted", outgoing(page), [])

    print("\n=== pinned refusal happens BEFORE any click ===")
    os.environ["TELEGRAM_TEST_CHAT_PEER"] = "999"
    page = fresh(page_html(row("jc", 777) + row("Ops", 44)))
    res = tw._send_test_message(page)
    check("refused", res.get("ok"), False)
    check("no row was opened (hash untouched)", page.evaluate("() => location.hash"), "")
    check("no row active", page.evaluate("() => document.querySelectorAll('.active').length"), 0)
    os.environ.pop("TELEGRAM_TEST_CHAT_PEER", None)

    print("\n=== already_sent leaves the composer empty ===")
    page = fresh(page_html(row("jc", 22)))
    tw._send_test_message(page)                       # first: sends
    res = tw._send_test_message(page)                 # second: newest bubble == text
    check("skipped", res.get("status"), "already_sent")
    check("composer empty", composer(page), "")
    check("still one bubble", len(outgoing(page)), 1)

    print("\n=== force repeats the fixed message despite idempotency ===")
    res = tw._send_test_message(page, force=True)
    check("sent again on force", res.get("status"), "sent")
    check("now two bubbles", len(outgoing(page)), 2)

    print("\n=== CRLF in the message -> still ONE Enter, one bubble ===")
    os.environ["TELEGRAM_TEST_MESSAGE"] = "line one\r\nline two\rline three"
    page = fresh(page_html(row("jc", 22)))
    res = tw._send_test_message(page)
    check("sent", res.get("status"), "sent")
    check("exactly one Enter", page.evaluate("() => window.__enters"), 1)
    check("one bubble", len(outgoing(page)), 1)
    os.environ["TELEGRAM_TEST_MESSAGE"] = MSG

    print("\n=== fallbacks with NO data-peer-id: header, then row-active ===")
    page = fresh(page_html(row("jc", None) + row("Ops", None)))        # header set on click
    res = tw._send_test_message(page)
    check("sent via header", res.get("status"), "sent")
    check("verifiedBy header", res.get("verifiedBy"), "header")
    page = fresh(page_html(row("jc", None) + row("Ops", None), header_for_click=False))
    res = tw._send_test_message(page)
    check("sent via row-active (blank header)", res.get("status"), "sent")
    check("verifiedBy row-active", res.get("verifiedBy"), "row-active")

    print("\n=== no composer, but ANOTHER editable exists -> nothing typed into it ===")
    page = fresh(page_html(row("jc", 22), composer=False, extra_editable=True))
    res = tw._send_test_message(page)
    check("refused", res.get("ok"), False)
    check("stage", res.get("stage"), "send")
    check("decoy editable untouched", page.eval_on_selector("#decoy", "e => e.innerText").strip(), "")

    print("\n=== handler driven: ledger refuses a repeat, force bypasses, pending flag resets ===")
    # Drive _TelegramWarm._handle_send_test with the browser pieces stubbed out, so the
    # ledger + coalescing logic (previously untested) is exercised for real.
    page = fresh(page_html(row("jc", 22)))
    sent_msgs = []
    w = tw._TelegramWarm()
    w._page = page
    w._healthy = lambda: True
    w._check_auth = lambda *a, **k: "authenticated"
    _orig_send_text = tw.send_text
    tw.send_text = lambda cid, txt: (sent_msgs.append(txt), {"code": 0})[1]
    tw._send_shot = lambda *a, **k: True
    try:
        w._send_pending = True
        w._handle_send_test({"chat_id": "oc_x"})
        check("first run sent", any(t.startswith("✅") for t in sent_msgs), True)
        check("pending flag reset by finally", w._send_pending, False)
        check("ledger recorded", len(w._send_ledger), 1)
        sent_msgs.clear()
        w._handle_send_test({"chat_id": "oc_x"})
        check("second run refused by ledger", any("not sending again" in t for t in sent_msgs), True)
        check("still one bubble", len(outgoing(page)), 1)
        sent_msgs.clear()
        w._handle_send_test({"chat_id": "oc_x", "force": True})
        check("force bypasses the ledger", any(t.startswith("✅") for t in sent_msgs), True)
        check("two bubbles after force", len(outgoing(page)), 2)
    finally:
        tw.send_text = _orig_send_text

    print("\n=== stale _send_pending expires instead of refusing forever ===")
    w2 = tw._TelegramWarm()
    w2._send_pending = True
    w2._send_pending_since = 1.0                        # long ago
    ok = w2.send_test(None)
    check("stale flag released, send queued", ok, True)
    check("queue holds the send", w2._tasks.qsize(), 1)

    browser.close()

print("\n=== worker coalescing: a second send while one is pending is refused (no browser) ===")
w = tw._TelegramWarm()
sent_texts = []
tw.send_text = lambda cid, txt: (sent_texts.append(txt), {"code": 0})[1]
check("first send queued", w.send_test("oc_x"), True)
check("second refused while pending", w.send_test("oc_x"), False)
check("queue holds exactly one send",
      sum(1 for t in list(w._tasks.queue) if t.get("kind") == "send_test"), 1)
check("caller told it was queued", any("queued" in t for t in sent_texts), True)
check("caller told the second was refused", any("already queued" in t for t in sent_texts), True)

print("\n=== code-request latch expires instead of sticking forever ===")
w2 = tw._TelegramWarm()
w2._awaiting_code = True
w2._awaiting_code_since = 1.0            # long ago
os.environ["TELEGRAM_CODE_LATCH_MAX_SEC"] = "60"
check("expired latch released", w2._code_wait_active(), False)
check("flag cleared", w2._awaiting_code, False)
w2._awaiting_code = True
import time as _t
w2._awaiting_code_since = _t.time()
check("fresh latch holds", w2._code_wait_active(), True)

print(f"\nTOTAL FAILURES: {fails}")
sys.exit(1 if fails else 0)
