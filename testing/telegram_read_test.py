"""Regression test for /checktelegramgroup against a synthetic Telegram-Web DOM.

Checks the things that decide whether the right messages come back, and — just as
importantly — that the READ path never touches the composer.

No network, no Telegram, no login. Run: python testing/telegram_read_test.py
"""
import os, sys, json
from pathlib import Path

proj = str(Path(__file__).resolve().parent.parent)
os.chdir(proj)
sys.path.insert(0, proj)
import telegramwarm as tw
from playwright.sync_api import sync_playwright

GROUP = "CP x 5G Integration_new"


def page_html(titles, bubbles, *, composer=True):
    items = "".join(
        f'<li class="chatlist-chat" data-t="{t}"><span class="user-title">{t}'
        f'<span class="dialog-time">02:31 PM</span></span></li>'
        for t in titles
    )
    # Timestamp sits INSIDE .message, exactly as Web K renders it, so the tail-strip
    # logic is genuinely exercised.
    bub = "".join(
        f'<div class="bubble{" is-out" if out else ""}" data-mid="{i}">'
        f'{f"<span class=peer-title>{who}</span>" if who else ""}'
        f'<div class="message">{txt}<span class="time-inner">{tm}</span></div></div>'
        for i, (who, txt, tm, out) in enumerate(bubbles)
    )
    comp = ('<div class="input-message-input" contenteditable="true"></div>'
            if composer else "")
    return f"""
    <style>.dialog-time{{display:block}}</style>
    <div id="column-left"><ul class="chatlist">{items}</ul></div>
    <div id="column-center">
      <div class="chat-info"><span class="peer-title" id="hdr"></span></div>
      <div class="bubbles">{bub}</div>
      {comp}
    </div>
    <script>
      document.querySelectorAll('#column-left li').forEach(li => {{
        li.addEventListener('click', (e) => {{
          if (!e.isTrusted) return;   // Web K ignores synthetic clicks
          document.querySelector('#hdr').innerText = li.getAttribute('data-t');
        }});
      }});
    </script>
    """


BUBBLES = [
    ("Ada", "Routine maintenance is scheduled", "10:01", False),
    ("Vica", "好的稍等", "10:05", False),
    ("", "noted, thanks", "10:06", True),
    ("Ada", "CPQA updated", "10:10", False),
    ("Vica", "已添加完成", "10:12", False),
    ("Ada", "please confirm", "10:15", False),
    ("", "confirmed", "10:20", True),
    ("Vica", "測試環境原回調網址", "10:22", False),
]

fails = 0


def check(label, got, want):
    global fails
    ok = got == want
    fails += not ok
    print(("  ok  " if ok else " FAIL "), f"{label}: {got!r}")


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1400, "height": 900})
    os.environ["TELEGRAM_CHECK_CHAT"] = GROUP
    os.environ["TELEGRAM_CHECK_COUNT"] = "5"

    print("=== reads the LAST 5 of 8, newest last ===")
    page.set_content(page_html([GROUP, "jc", "Ops"], BUBBLES))
    res = tw._check_group(page, GROUP, 5)
    check("ok", res.get("ok"), True)
    check("chat", res.get("chat"), GROUP)
    msgs = res.get("messages", [])
    check("count", len(msgs), 5)
    check("first of the five", msgs[0]["text"], "CPQA updated")
    check("last (newest)", msgs[-1]["text"], "測試環境原回調網址")
    check("timestamp stripped from body", "10:22" in msgs[-1]["text"], False)
    check("time captured separately", msgs[-1]["time"], "10:22")
    check("sender captured", msgs[-1]["sender"], "Vica")
    # Last 5 of 8 = source indices 3..7, so the outgoing "confirmed" bubble
    # (source index 6) lands at position 3 of the returned slice.
    check("outgoing flagged", msgs[3]["out"], True)
    check("outgoing body", msgs[3]["text"], "confirmed")
    check("outgoing sender defaults to me", msgs[3]["sender"], "me")

    print("\n=== READ-ONLY: composer never touched ===")
    body = page.eval_on_selector(".input-message-input", "e => e.innerText")
    check("composer empty", body.strip(), "")

    print("\n=== substring match (reading only) ===")
    page.set_content(page_html([GROUP, "Ops"], BUBBLES))
    res = tw._check_group(page, "CP x 5G", 3)
    check("ok", res.get("ok"), True)
    check("matched by substring", res.get("matchKind"), "substring")
    check("count", len(res.get("messages", [])), 3)

    print("\n=== exact still wins over substring ===")
    page.set_content(page_html(["CP x 5G Integration_new", "CP x 5G Integration_new_OLD"], BUBBLES))
    res = tw._check_group(page, GROUP, 2)
    check("ok", res.get("ok"), True)
    check("matchKind", res.get("matchKind"), "exact")

    print("\n=== no such chat -> refuse, report candidates ===")
    page.set_content(page_html(["Ops", "Alerts"], BUBBLES))
    res = tw._check_group(page, GROUP, 5)
    check("refused", res.get("ok"), False)
    check("stage", res.get("stage"), "open")
    check("candidates listed", bool(res.get("candidates")), True)

    print("\n=== regression: sidebar titles carry a timestamp ===")
    # The live sidebar returns titles like "(OG) IGO / YB\nTue" because the time is a
    # nested block element inside .user-title. Exact matching must still work, or
    # /telegramsendjctest (exact-only) can never find any normal sidebar row.
    live_titles = ["(OG) IGO / YB", "CP x 5G Integration_new",
                   "Casino Plus | VA Gaming integration group", "jc"]
    for want in live_titles:
        page.set_content(page_html(live_titles, BUBBLES))
        r = page.evaluate(tw._FIND_CHAT_JS, {"wanted": want, "allowSubstring": False})
        check(f"exact match {want!r}", r["matches"], 1)
    page.set_content(page_html(live_titles, BUBBLES))
    r = page.evaluate(tw._FIND_CHAT_JS, {"wanted": "not present", "allowSubstring": False})
    check("absent title still reports 0", r["matches"], 0)

    print("\n=== target row scrolled far below the viewport ===")
    # The live failure: '(OG) IGO / YB' was row 14 of 15, in the DOM but below the
    # fold, so elementFromPoint returned null and topDesc was None — reported as
    # "something is on top of it" when nothing was. It must be scrolled into view.
    filler = "".join(
        f'<li class="chatlist-chat"><span class="user-title">Filler {i}'
        f'<span class="dialog-time">10:0{i % 10}</span></span></li>'
        for i in range(40)
    )
    deep = f"""
    <style>
      .dialog-time{{display:block}}
      #column-left{{width:380px;height:900px;overflow-y:scroll}}
      .chatlist-chat{{height:64px}}
    </style>
    <div id="column-left"><ul class="chatlist">
      {filler}
      <li class="chatlist-chat"><span class="user-title">{GROUP}
        <span class="dialog-time">Tue</span></span></li>
    </ul></div>
    <div id="column-center">
      <div class="chat-info"><span class="peer-title" id="hdr"></span></div>
      <div class="bubbles">
        <div class="bubble" data-mid="1"><span class="peer-title">Ada</span>
          <div class="message">deep row<span class="time-inner">10:01</span></div></div>
      </div>
      <div class="input-message-input" contenteditable="true"></div>
    </div>
    <script>
      document.querySelectorAll('#column-left li').forEach(li => {{
        li.addEventListener('click', (e) => {{
          if (!e.isTrusted) return;
          li.classList.add('active');
        }});
      }});
    </script>
    """
    page.set_content(deep)
    # Without scrollIntoView the row is off-screen and unhittable...
    r = page.evaluate(tw._FIND_CHAT_JS,
                      {"wanted": GROUP, "allowSubstring": False, "scrollIntoView": False})
    check("found in the DOM", r["matches"], 1)
    check("reported off-screen", r["offscreen"], True)
    check("not hittable off-screen", r["hitOk"], False)
    # ...and with it, the row becomes clickable.
    r = page.evaluate(tw._FIND_CHAT_JS,
                      {"wanted": GROUP, "allowSubstring": False, "scrollIntoView": True})
    check("scrolled into view", r["offscreen"], False)
    check("now hittable", r["hitOk"], True)

    page.set_content(deep)
    res = tw._check_group(page, GROUP, 5)
    check("opens a deep row end to end", res.get("ok"), True)
    check("messages read", len(res.get("messages", [])), 1)

    print("\n=== rows covered by a search overlay (the live failure) ===")
    # Exactly what the live client showed: 15 laid-out rows with valid boxes, an open
    # search overlay on top, and every coordinate click landing on the overlay. A
    # rect alone proves layout, not hittability — elementFromPoint must be consulted.
    covered = f"""
    <style>
      .dialog-time{{display:block}}
      /* The real sidebar is ~380px wide, so a row's CENTRE — the point that gets
         clicked and hit-tested — falls under the overlay. Without constraining the
         width the row spans the whole page and its centre sits beside the overlay,
         which made this fixture pass while the live client failed. */
      #column-left{{width:380px}}
      #overlay{{position:fixed;left:0;top:0;width:380px;height:900px;background:#fff;z-index:99}}
      #overlay.gone{{display:none}}
    </style>
    <div id="column-left">
      <ul class="chatlist">
        <li class="chatlist-chat"><span class="user-title">{GROUP}
          <span class="dialog-time">02:31 PM</span></span></li>
      </ul>
      <!-- The close control belongs INSIDE the overlay, as Telegram's search panel
           header is. Putting it underneath meant the overlay covered the very
           button that dismisses it, which no real client does. -->
      <div id="overlay">
        <div class="sidebar-header"><button class="sidebar-close-button">back</button></div>
        <input class="input-search-input" value="">
      </div>
    </div>
    <div id="column-center">
      <div class="chat-info"><span class="peer-title" id="hdr"></span></div>
      <div class="bubbles">
        <div class="bubble" data-mid="1"><span class="peer-title">Ada</span>
          <div class="message">covered case<span class="time-inner">10:01</span></div></div>
      </div>
      <div class="input-message-input" contenteditable="true"></div>
    </div>
    <script>
      // The back button dismisses the overlay, like closing Telegram's search panel.
      document.querySelector('.sidebar-close-button')
        .addEventListener('click', () => document.getElementById('overlay').classList.add('gone'));
      document.querySelectorAll('#column-left li').forEach(li => {{
        li.addEventListener('click', (e) => {{
          if (!e.isTrusted) return;
          li.classList.add('active');
        }});
      }});
    </script>
    """
    page.set_content(covered)
    r = page.evaluate(tw._FIND_CHAT_JS, {"wanted": GROUP, "allowSubstring": False})
    check("row found despite the overlay", r["matches"], 1)
    check("rect exists (layout is fine)", bool(r["rect"]), True)
    check("hit test says NOT clickable", r["hitOk"], False)
    check("names what is on top", "overlay" in (r.get("topDesc") or ""), True)

    page.set_content(covered)
    res = tw._check_group(page, GROUP, 5)
    check("recovers by dismissing the overlay", res.get("ok"), True)
    check("messages read after recovery", len(res.get("messages", [])), 1)

    print("\n=== header unreadable, but selected row confirms it ===")
    # The live client never exposes a readable header, so verification must also
    # accept the sidebar's selected-row state. Here #hdr stays empty forever and the
    # clicked row gains .active instead.
    rowonly = f"""
    <style>.dialog-time{{display:block}}</style>
    <div id="column-left"><ul class="chatlist">
      <li class="chatlist-chat"><span class="user-title">{GROUP}
        <span class="dialog-time">02:31 PM</span></span></li>
      <li class="chatlist-chat"><span class="user-title">Ops
        <span class="dialog-time">Tue</span></span></li>
    </ul></div>
    <div id="column-center">
      <div class="chat-info"><span class="peer-title" id="hdr"></span></div>
      <div class="bubbles">
        <div class="bubble" data-mid="1"><span class="peer-title">Ada</span>
          <div class="message">hello there<span class="time-inner">10:01</span></div></div>
      </div>
      <div class="input-message-input" contenteditable="true"></div>
    </div>
    <script>
      document.querySelectorAll('#column-left li').forEach(li => {{
        li.addEventListener('click', (e) => {{
          if (!e.isTrusted) return;
          document.querySelectorAll('#column-left li').forEach(o => o.classList.remove('active'));
          li.classList.add('active');       // header deliberately left blank
        }});
      }});
    </script>
    """
    page.set_content(rowonly)
    res = tw._check_group(page, GROUP, 5)
    check("opened via row-active", res.get("ok"), True)
    check("messages still read", len(res.get("messages", [])), 1)
    body = page.eval_on_selector(".input-message-input", "e => e.innerText")
    check("still read-only", body.strip(), "")

    print("\n=== wrong row active -> refuse ===")
    # Clicking marks a DIFFERENT row active, so neither signal confirms the target.
    wrongrow = rowonly.replace("li.classList.add('active');",
                               "document.querySelectorAll('#column-left li')[1].classList.add('active');")
    page.set_content(wrongrow)
    res = tw._check_group(page, GROUP, 5)
    check("refused", res.get("ok"), False)
    check("stage", res.get("stage"), "open")
    check("inventory attached", isinstance(res.get("ui"), dict), True)

    print("\n=== retry: first click does not take (list re-rendered) ===")
    # The live failure was "Element is not attached to the DOM": the chat list
    # re-renders between find and click. Clicking coordinates avoids holding a
    # handle, but a coordinate can still miss, so the header is verified and the
    # cycle retried. Here the first trusted click is deliberately swallowed.
    flaky = f"""
    <style>.dialog-time{{display:block}}</style>
    <div id="column-left"><ul class="chatlist">
      <li class="chatlist-chat"><span class="user-title">{GROUP}
        <span class="dialog-time">02:31 PM</span></span></li>
    </ul></div>
    <div id="column-center">
      <div class="chat-info"><span class="peer-title" id="hdr"></span></div>
      <div class="bubbles">
        <div class="bubble" data-mid="1"><span class="peer-title">Ada</span>
          <div class="message">hello<span class="time-inner">10:01</span></div></div>
      </div>
      <div class="input-message-input" contenteditable="true"></div>
    </div>
    <script>
      window.__clicks = 0;
      document.querySelectorAll('#column-left li').forEach(li => {{
        li.addEventListener('click', (e) => {{
          if (!e.isTrusted) return;
          window.__clicks++;
          if (window.__clicks >= 2) {{
            document.querySelector('#hdr').innerText = {GROUP!r};
          }}
        }});
      }});
    </script>
    """
    page.set_content(flaky)
    res = tw._check_group(page, GROUP, 5)
    check("recovered on retry", res.get("ok"), True)
    check("trusted clicks needed", page.evaluate("() => window.__clicks"), 2)

    print("\n=== give up cleanly when the header never matches ===")
    never = flaky.replace("window.__clicks >= 2", "false")
    page.set_content(never)
    res = tw._check_group(page, GROUP, 5)
    check("refused", res.get("ok"), False)
    check("stage", res.get("stage"), "open")
    check("reason explains what could not be confirmed",
          "could not confirm" in (res.get("reason") or ""), True)
    body = page.eval_on_selector(".input-message-input", "e => e.innerText")
    check("composer untouched", body.strip(), "")

    print("\n=== messy real-world bubbles (from live output) ===")
    # Reproduces exactly what the live group returned: an "edited" marker fused to the
    # timestamp, the time rendered INSIDE the message text, a media-only bubble with
    # no text at all, and an over-long body.
    long_body = "X" * 900
    messy = f"""
    <div id="column-left"><ul class="chatlist">
      <li class="chatlist-chat" data-t="{GROUP}"><span class="user-title">{GROUP}</span></li>
    </ul></div>
    <div id="column-center">
      <div class="chat-info"><span class="peer-title" id="hdr"></span></div>
      <div class="bubbles">
        <div class="bubble" data-mid="1"><span class="peer-title">king</span>
          <div class="message">Hello Team,<span class="time-inner">edited

09:59 AM</span></div></div>
        <div class="bubble is-out" data-mid="2">
          <div class="message">Hi team, any maintenance?<span class="time-inner">

02:42 PM</span></div></div>
        <div class="bubble" data-mid="3"><span class="peer-title">VP Support</span>
          <div class="document">GameList.xlsx</div><span class="time-inner">11:10 AM</span></div>
        <div class="bubble" data-mid="4"><span class="peer-title">Ada</span>
          <div class="message">{long_body}<span class="time-inner">11:11 AM</span></div></div>
      </div>
      <div class="input-message-input" contenteditable="true"></div>
    </div>
    <script>
      document.querySelectorAll('#column-left li').forEach(li => {{
        li.addEventListener('click', (e) => {{
          if (!e.isTrusted) return;
          document.querySelector('#hdr').innerText = li.getAttribute('data-t');
        }});
      }});
    </script>
    """
    page.set_content(messy)
    res = tw._check_group(page, GROUP, 4)
    check("ok", res.get("ok"), True)
    ms = res.get("messages", [])
    check("edited flagged", ms[0]["edited"], True)
    check("clock extracted from 'edited\\n\\n09:59 AM'", ms[0]["time"], "09:59 AM")
    check("body free of the time block", ms[0]["text"], "Hello Team,")
    check("blank-prefixed time cleaned", ms[1]["time"], "02:42 PM")
    check("outgoing body intact", ms[1]["text"], "Hi team, any maintenance?")
    check("media reports a kind", ms[2]["kind"], "document")
    # The filename is worth keeping; only the sender-name chrome is stripped.
    check("media body is the filename, not bubble chrome", ms[2]["text"], "GameList.xlsx")
    check("long body truncated", ms[3]["truncated"], True)
    check("truncated to 600", len(ms[3]["text"]), 600)
    body = page.eval_on_selector(".input-message-input", "e => e.innerText")
    check("still read-only", body.strip(), "")

    print("\n=== chat opens but no bubbles -> read stage + inventory ===")
    page.set_content(page_html([GROUP], []))
    res = tw._check_group(page, GROUP, 5)
    check("refused", res.get("ok"), False)
    check("stage", res.get("stage"), "read")
    check("inventory present", isinstance(res.get("ui"), dict), True)

    browser.close()

print("\n=== card builder (one card per chat, screenshot inside) ===")
ok_result = {
    "ok": True, "chat": GROUP, "total": 23, "matchKind": "exact",
    "messages": [
        {"sender": "Ada", "time": "10:15", "text": "please confirm",
         "out": False, "edited": False, "truncated": False, "kind": None},
        {"sender": "me", "time": "10:20", "text": "confirmed",
         "out": True, "edited": True, "truncated": False, "kind": None},
        {"sender": "VP Support", "time": "11:10", "text": "GameList.xlsx",
         "out": False, "edited": False, "truncated": False, "kind": "document"},
    ],
}
card = tw._build_check_card(GROUP, ok_result, "img_abc123")
check("schema 2.0", card.get("schema"), "2.0")
check("green header on success", card["header"]["template"], "green")
check("chat name in header", GROUP in card["header"]["title"]["content"], True)
imgs = [e for e in card["body"]["elements"] if e.get("tag") == "img"]
check("screenshot embedded in the card", len(imgs), 1)
check("img_key wired", imgs[0]["img_key"], "img_abc123")
blob = json.dumps(card, ensure_ascii=False)
check("messages present in body", "please confirm" in blob, True)
check("edited marker shown", "edited" in blob, True)
check("media kind shown", "(document) GameList.xlsx" in blob, True)
check("card is JSON-serialisable", isinstance(blob, str), True)

no_img = tw._build_check_card(GROUP, ok_result, None)
check("no img element without a key",
      [e for e in no_img["body"]["elements"] if e.get("tag") == "img"], [])

fail_result = {"ok": False, "stage": "open", "reason": "0 chats matching",
               "candidates": ["Ops", "Alerts"], "ui": {"editables": []}}
fcard = tw._build_check_card("Nope", fail_result, None)
check("red header on failure", fcard["header"]["template"], "red")
fblob = json.dumps(fcard, ensure_ascii=False)
check("failure reason shown", "0 chats matching" in fblob, True)
check("candidates shown", "Alerts" in fblob, True)
check("inventory shown", "editables" in fblob, True)

print("\n=== per-chat screenshot paths are distinct ===")
check("distinct paths", tw._shot_path_for(0) != tw._shot_path_for(1), True)

print("\n=== plain-text fallback when a card cannot be sent ===")
fb = tw._TelegramWarm._plain_fallback(GROUP, ok_result)
check("fallback names the chat", GROUP in fb, True)
check("fallback carries a message", "please confirm" in fb, True)
fb2 = tw._TelegramWarm._plain_fallback("Nope", fail_result)
check("fallback reports failure", "failed at the" in fb2, True)

print(f"\nTOTAL FAILURES: {fails}")
sys.exit(1 if fails else 0)
