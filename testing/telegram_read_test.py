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
        f'<li class="chatlist-chat" data-t="{t}"><span class="user-title">{t}</span></li>'
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

    print("\n=== chat opens but no bubbles -> read stage + inventory ===")
    page.set_content(page_html([GROUP], []))
    res = tw._check_group(page, GROUP, 5)
    check("refused", res.get("ok"), False)
    check("stage", res.get("stage"), "read")
    check("inventory present", isinstance(res.get("ui"), dict), True)

    browser.close()

print(f"\nTOTAL FAILURES: {fails}")
sys.exit(1 if fails else 0)
