"""Regression test for /telegramsendjctest against a synthetic Telegram-Web DOM.

Covers the parts that decide whether a message goes to the RIGHT chat:
  * exact (not fuzzy) title matching, so "jc" never resolves to "JC Team"
  * refusal when the title is ambiguous or absent
  * refusal to type when the opened chat header disagrees with the request
  * the composer actually receiving the text

No network, no Telegram, no login. Run: python testing/telegram_send_test.py
"""
import os, sys, json
from pathlib import Path

proj = str(Path(__file__).resolve().parent.parent)
os.chdir(proj)
sys.path.insert(0, proj)
import telegramwarm as tw
from playwright.sync_api import sync_playwright


def page_html(titles, *, header_for_click=True, composer=True):
    items = "".join(
        f'<li class="chatlist-chat" data-t="{t}"><span class="user-title">{t}'
        f'<span class="dialog-time">02:31 PM</span></span></li>'
        for t in titles
    )
    comp = ('<div class="input-message-input" contenteditable="true"></div>'
            if composer else "")
    # Clicking a sidebar row writes that row's title into the header, the way the
    # real client does when it opens a conversation.
    #
    # The isTrusted guard is the important part: Telegram Web K ignores synthetic DOM
    # clicks and only reacts to real pointer events. An earlier version clicked via
    # JS (el.click()), which passes a naive listener but never opens the chat against
    # the real client. Requiring a trusted event makes this test fail if anyone
    # reintroduces a JS click.
    click_js = """
      document.querySelectorAll('#column-left li').forEach(li => {
        li.addEventListener('click', (e) => {
          if (!e.isTrusted) return;
          document.querySelector('#hdr').innerText = li.getAttribute('data-t');
        });
      });
    """ if header_for_click else ""
    return f"""
    <style>.dialog-time{{display:block}}</style>
    <div id="column-left"><ul class="chatlist">{items}</ul></div>
    <div id="column-center">
      <div class="chat-info"><span class="peer-title" id="hdr"></span></div>
      {comp}
    </div>
    <script>{click_js}</script>
    """


fails = 0


def check(label, got, want):
    global fails
    ok = got == want
    fails += not ok
    print(("  ok  " if ok else " FAIL "), f"{label}: {got!r}")


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1400, "height": 900})
    os.environ["TELEGRAM_TEST_CHAT"] = "jc"
    os.environ["TELEGRAM_TEST_MESSAGE"] = "Hi team, are there any maintenance plans for this week?"

    print("=== exact match wins over similar names ===")
    page.set_content(page_html(["JC Team", "jc", "jcsia", "Ops"]))
    res = tw._send_test_message(page)
    check("send ok", res.get("ok"), True)
    check("chat opened", res.get("chat"), "jc")
    body = page.eval_on_selector(".input-message-input", "e => e.innerText")
    # Enter clears nothing in a static page, so the text should still be in the box.
    check("composer received text",
          "maintenance plans" in body, True)

    print("\n=== ambiguous title -> refuse, send nothing ===")
    page.set_content(page_html(["jc", "JC"]))   # two exact case-insensitive matches
    res = tw._send_test_message(page)
    check("refused", res.get("ok"), False)
    check("stage", res.get("stage"), "open")
    body = page.eval_on_selector(".input-message-input", "e => e.innerText")
    check("composer untouched", body.strip(), "")

    print("\n=== missing chat -> refuse ===")
    page.set_content(page_html(["Ops", "Alerts"]))
    res = tw._send_test_message(page)
    check("refused", res.get("ok"), False)
    check("stage", res.get("stage"), "open")

    print("\n=== header disagrees -> refuse BEFORE typing ===")
    # Row click does not update the header, so verification must catch the mismatch.
    page.set_content(page_html(["jc", "Ops"], header_for_click=False))
    res = tw._send_test_message(page)
    check("refused", res.get("ok"), False)
    check("stage", res.get("stage"), "verify")
    body = page.eval_on_selector(".input-message-input", "e => e.innerText")
    check("composer untouched", body.strip(), "")

    print("\n=== composer missing -> reports UI inventory ===")
    page.set_content(page_html(["jc"], composer=False))
    res = tw._send_test_message(page)
    check("refused", res.get("ok"), False)
    check("stage", res.get("stage"), "compose")
    check("inventory present", isinstance(res.get("ui"), dict), True)

    browser.close()

print(f"\nTOTAL FAILURES: {fails}")
sys.exit(1 if fails else 0)
