"""Regression test for the .stealthy honeypot bug.

Rebuilds the exact field inventory reported by the live 2FA screen and asserts the
password lands in the REAL input, not a decoy. Also checks the phone form
(contenteditable) still fills. No network, no Telegram, no login required.
"""
import os, sys
from pathlib import Path

# Project root is this file's parent directory, so the test runs on the server too.
proj = str(Path(__file__).resolve().parent.parent)
os.chdir(proj)
sys.path.insert(0, proj)
import telegramwarm as tw
from playwright.sync_api import sync_playwright

# Mirrors the reported inventory, decoys first in document order.
PWD_PAGE = """
<div id="page-chats"><div class="chatlist"></div></div>
<div id="auth-pages">
  <div class="input-field input-field-password">
    <input type="password" class="stealthy">
    <input type="password" class="input-field-input is-empty error">
    <input type="password" class="stealthy">
    <div class="input-field-border"></div>
  </div>
  <button class="btn-primary btn-color-primary">NEXT</button>
</div>
"""

PHONE_PAGE = """
<div id="page-chats"></div>
<div id="auth-pages">
  <div class="input-field input-select"><div class="input-field-input" contenteditable="true"></div></div>
  <div class="input-field input-field-phone"><div class="input-field-input" contenteditable="true" inputmode="decimal"></div></div>
  <button class="btn-primary btn-color-primary">NEXT</button>
</div>
"""

CODE_PAGE = """
<div id="page-chats"></div>
<div id="auth-pages">
  <input type="password" class="stealthy">
  <div class="input-field"><div class="input-field-input" contenteditable="true"></div></div>
</div>
"""

fails = 0
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1200, "height": 800})

    # --- 2FA password screen -------------------------------------------------
    page.set_content(PWD_PAGE)
    ok = tw._fill_auth_field(page, "S3cret!Value")
    real = page.eval_on_selector(
        "#auth-pages input.input-field-input", "e => e.value")
    decoys = page.eval_on_selector_all(
        "#auth-pages input.stealthy", "els => els.map(e => e.value)")
    print("=== 2FA password screen ===")
    print("  fill returned      :", ok)
    print("  real field value   :", repr(real))
    print("  decoy field values :", decoys)
    good = ok and real == "S3cret!Value" and all(v == "" for v in decoys)
    print("  RESULT:", "OK — password in the real field, decoys untouched" if good else "FAIL")
    fails += not good

    # --- phone form (contenteditable) ---------------------------------------
    page.set_content(PHONE_PAGE)
    ok2 = tw._fill_auth_field(page, "+60102693549")
    phone = page.eval_on_selector(
        "#auth-pages .input-field-phone .input-field-input", "e => e.innerText")
    print("\n=== phone form (regression) ===")
    print("  fill returned :", ok2, "| phone field:", repr(phone))
    good2 = ok2 and "102693549" in phone.replace(" ", "")
    print("  RESULT:", "OK" if good2 else "FAIL")
    fails += not good2

    # --- code screen with a decoy present ------------------------------------
    page.set_content(CODE_PAGE)
    ok3 = tw._fill_code(page, "14632")
    code_val = page.eval_on_selector(
        "#auth-pages .input-field-input", "e => e.innerText")
    decoy3 = page.eval_on_selector("#auth-pages input.stealthy", "e => e.value")
    print("\n=== code screen (decoy present) ===")
    print("  fill returned :", ok3, "| code field:", repr(code_val), "| decoy:", repr(decoy3))
    good3 = ok3 and code_val.strip() == "14632" and decoy3 == ""
    print("  RESULT:", "OK" if good3 else "FAIL")
    fails += not good3

    browser.close()

print(f"\nTOTAL FAILURES: {fails}")
sys.exit(1 if fails else 0)
