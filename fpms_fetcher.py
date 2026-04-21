"""
FPMS 自动登录与表格抓取模块（支持保存/加载浏览器状态）
用法：
    python3 fpms_fetcher.py --save-state   # 有头模式手动登录一次，保存状态
    python3 fpms_fetcher.py                # 无头模式使用已保存状态自动查询
    python3 fpms_fetcher.py DD/MM          # 指定日期查询
"""
import json
import pyotp
import sys
import os
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

LOGIN_URL = "https://mgnt-webserver.casinoplus.top/"
USERNAME = "CPOM01"
PASSWORD = "8c0fa1"
TOTP_SECRET = "MNYG63JQGEYTMOJTHE4DMMBTGQYDIOI"
TABLE_SELECTOR = "#creditLostFixSummaryTable tbody tr"
REPORT_URL = "https://mgnt-webserver.casinoplus.top/report"
STATE_FILE = "browser_state.json"
COOKIES_FILE = "cookies.json"

NAV_TIMEOUT_MS = 90_000
LOGIN_FIELD_TIMEOUT_MS = 90_000
MENU_TIMEOUT_MS = 60_000

CHROMIUM_ARGS = ["--disable-blink-features=AutomationControlled"]


def _normalize_cookies(cookies):
    valid = {"Strict", "Lax", "None"}
    for c in cookies:
        if c.get("sameSite") not in valid:
            c["sameSite"] = "Lax"
    return cookies


def _is_login_page(page):
    if "login" in page.url.lower():
        return True
    return page.locator("#username").count() > 0


def _goto_report(page):
    page.goto(REPORT_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    page.wait_for_timeout(1500)


def _do_full_login(page, context, save_state_only=False):
    """完整账号+TOTP 登录；成功后写入 browser_state.json。"""
    print("🔐 正在登录 FPMS...")
    page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    try:
        page.wait_for_load_state("networkidle", timeout=45000)
    except PlaywrightTimeout:
        pass

    user_loc = page.locator("#username, input[name='username'], input[formcontrolname='username']").first
    user_loc.wait_for(state="visible", timeout=LOGIN_FIELD_TIMEOUT_MS)
    user_loc.fill(USERNAME)
    page.locator("#password, input[name='password'], input[type='password']").first.fill(PASSWORD)
    page.locator('input[type="submit"], button[type="submit"]').first.click()
    page.wait_for_selector("#OTP", state="visible", timeout=LOGIN_FIELD_TIMEOUT_MS)
    code = pyotp.TOTP(TOTP_SECRET).now()
    print(f"🔢 当前验证码: {code}")
    page.fill("#OTP", code)
    page.keyboard.press("Enter")
    try:
        page.wait_for_url("**/report", timeout=15000)
        print("✅ 页面已自动跳转到报表页")
    except PlaywrightTimeout:
        print("⚠️ 未自动跳转，正在手动导航到 /report...")
        _goto_report(page)

    print("✅ 登录成功，正在保存浏览器状态...")
    context.storage_state(path=STATE_FILE)
    print(f"✅ 状态已保存到 {STATE_FILE}")
    if save_state_only:
        return


def fetch_fpms_data(headless=False, target_date_str=None, save_state=False):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            slow_mo=100 if not headless else 0,
            args=CHROMIUM_ARGS,
        )

        ctx_opts = {}
        if os.path.exists(STATE_FILE) and not save_state:
            ctx_opts["storage_state"] = STATE_FILE

        context = browser.new_context(**ctx_opts)
        page = context.new_page()

        try:
            if save_state:
                _do_full_login(page, context, save_state_only=True)
                return "state_saved"

            # 会话恢复顺序：browser_state.json → cookies.json → 完整登录
            if ctx_opts:
                print("🚀 使用 browser_state.json 访问报表页…")
                _goto_report(page)
            elif os.path.exists(COOKIES_FILE):
                print("🍪 使用 cookies.json 访问报表页…")
                with open(COOKIES_FILE, "r", encoding="utf-8") as f:
                    context.add_cookies(_normalize_cookies(json.load(f)))
                _goto_report(page)
            else:
                print("📭 无本地会话文件，执行完整登录…")
                _do_full_login(page, context, save_state_only=False)

            if _is_login_page(page) and os.path.exists(COOKIES_FILE) and ctx_opts:
                print("🍪 browser_state 失效，尝试 cookies.json …")
                with open(COOKIES_FILE, "r", encoding="utf-8") as f:
                    context.add_cookies(_normalize_cookies(json.load(f)))
                _goto_report(page)

            if _is_login_page(page):
                print("⚠️ 会话无效或已过期，执行完整登录…")
                _do_full_login(page, context, save_state_only=False)

            if _is_login_page(page):
                raise RuntimeError(
                    "仍停留在登录页：请检查网络、账号权限，或在本机执行 "
                    "`python fpms_fetcher.py --save-state` 生成有效的 browser_state.json 后上传到服务器。"
                )

            page.keyboard.press("Escape")
            page.wait_for_timeout(500)

            # ---------- 以下为通用查询流程 ----------
            try:
                page.wait_for_load_state("networkidle", timeout=45000)
            except PlaywrightTimeout:
                pass
            page.wait_for_timeout(2000)

            # 文本可能在 panel-heading 内的 label/h4 上，:has-text 对 div 可能匹配不到
            print("📂 展开 Miscellaneous Report 菜单...")
            misc_xpath = (
                '//div[contains(@class, "panel-heading")]//label[contains(text(), "Miscellaneous Report")]'
                ' | //h4[contains(text(), "Miscellaneous Report")]'
                ' | //div[contains(@class, "panel-heading")]//*[contains(normalize-space(.), "Miscellaneous Report")]'
            )
            misc_heading = page.locator(f"xpath={misc_xpath}").first
            misc_heading.wait_for(state="visible", timeout=MENU_TIMEOUT_MS)
            misc_heading.scroll_into_view_if_needed()
            misc_heading.click()
            page.wait_for_timeout(1000)

            print("🖱️ 点击 CREDIT_LOST_FIX_PROPOSAL_REPORT...")
            report_xpath = (
                '//li[contains(text(), "CREDIT_LOST_FIX_PROPOSAL_REPORT")]'
                ' | //a[contains(text(), "CREDIT_LOST_FIX_PROPOSAL_REPORT")]'
            )
            report_link = page.locator(f"xpath={report_xpath}").first
            report_link.wait_for(state="visible", timeout=MENU_TIMEOUT_MS)
            report_link.scroll_into_view_if_needed()
            report_link.click()

            page.wait_for_selector("#creditLostFixProposalReportQuery", timeout=MENU_TIMEOUT_MS)
            print("✅ 进入报表查询界面")

            def set_select_value(selector, value, multiple=False):
                page.wait_for_selector(selector, timeout=10000)
                js = """
                    ([selector, value, multiple]) => {
                        const select = document.querySelector(selector);
                        if (!select) throw new Error('Select not found');
                        if (multiple) {
                            for (let i = 0; i < select.options.length; i++) {
                                select.options[i].selected = value.includes(select.options[i].value);
                            }
                        } else {
                            select.value = value;
                        }
                        select.dispatchEvent(new Event('input', { bubbles: true }));
                        select.dispatchEvent(new Event('change', { bubbles: true }));
                        const scope = angular.element(select).scope();
                        if (scope) scope.$apply();
                        return select.value;
                    }
                """
                return page.evaluate(js, [selector, value, multiple])

            all_product_values = page.evaluate("""
                () => {
                    const select = document.querySelector('select[ng-model="vm.creditLostFixProposalReportQuery.platformList"]');
                    if (!select) return [];
                    return Array.from(select.options).map(opt => opt.value).filter(v => v && v !== '?');
                }
            """)
            if all_product_values:
                set_select_value(
                    'select[ng-model="vm.creditLostFixProposalReportQuery.platformList"]',
                    all_product_values,
                    multiple=True,
                )

            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            if target_date_str:
                day, month = map(int, target_date_str.split("/"))
                target_date = datetime(today.year, month, day)
            else:
                target_date = today
            end_date = target_date
            start_date = end_date - timedelta(days=1)
            start_str = start_date.strftime("%Y/%m/%d 00:00:00")
            end_str = end_date.strftime("%Y/%m/%d 00:00:00")

            page.locator('label:has-text("Start date")').locator("..").locator("input").fill(start_str)
            page.locator('label:has-text("End date")').locator("..").locator("input").fill(end_str)

            all_type_values = page.evaluate("""
                () => {
                    const select = document.querySelector('select[ng-model="vm.creditLostFixProposalReportQuery.proposalTypeNames"]');
                    if (!select) return [];
                    return Array.from(select.options).map(opt => opt.value).filter(v => v && v !== '?');
                }
            """)
            if all_type_values:
                set_select_value(
                    'select[ng-model="vm.creditLostFixProposalReportQuery.proposalTypeNames"]',
                    all_type_values,
                    multiple=True,
                )

            set_select_value('select[ng-model="vm.creditLostFixProposalReportQuery.providerId"]', "all")

            for _ in range(3):
                set_select_value('select[ng-model="vm.creditLostFixProposalReportQuery.proposalStatus"]', "Success")
                current_val = page.evaluate(
                    'document.querySelector(\'select[ng-model="vm.creditLostFixProposalReportQuery.proposalStatus"]\').value'
                )
                if current_val == "Success":
                    break
                page.wait_for_timeout(500)

            page.locator('button:has-text("Search")').first.click()
            page.wait_for_timeout(5000)

            total_label = page.locator('label.ng-binding:has-text("Total")').first
            total_label.wait_for(state="visible", timeout=30000)
            total_text = total_label.text_content().strip()

            if "Total 0 records" in total_text:
                return "no amount loss record found for today"
            return "as checked amount loss have record today"

        except Exception as e:
            print(f"❌ 错误: {e}")
            page.screenshot(path="error_screenshot.png")
            raise
        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    headless = "--headless" in sys.argv
    save_state = "--save-state" in sys.argv
    target_date = None
    for arg in sys.argv[1:]:
        if not arg.startswith("--"):
            target_date = arg
            break
    result = fetch_fpms_data(headless=headless, target_date_str=target_date, save_state=save_state)
    print(result)
