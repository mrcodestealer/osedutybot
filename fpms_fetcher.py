"""
FPMS 自动登录与表格抓取模块（支持保存/加载浏览器状态）
用法：
    python3 fpms_fetcher.py --save-state   # 有头模式手动登录一次，保存状态
    python3 fpms_fetcher.py                # 无头模式使用已保存状态自动查询
    python3 fpms_fetcher.py DD/MM          # 指定日期查询
"""
import json
import re
import pyotp
import sys
import os
import platform
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

# 侧栏报表名（页面可能把文案放在子节点，勿用 XPath 的 text() 只匹配直接文本）
REPORT_MENU_LABEL_FULL = "CREDIT_LOST_FIX_PROPOSAL_REPORT"
REPORT_MENU_LABEL_RE = re.compile(r"CREDIT_LOST_FIX_PROPOSAL", re.IGNORECASE)
# 菜单标题可能换行 / 多空格；勿用纯字符串 get_by_text 在服务器上易超时
MISC_MENU_TEXT_RE = re.compile(r"Miscellaneous\s*Report", re.IGNORECASE)

# 与桌面 Chrome 一致，避免 headless 默认小视口触发「移动端」隐藏侧栏
DESKTOP_VIEWPORT = {"width": 1920, "height": 1080}
DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

CHROMIUM_ARGS = ["--disable-blink-features=AutomationControlled"]
if platform.system() == "Linux":
    # 服务器 / Docker 常见：避免共享内存不足导致页面不完整
    CHROMIUM_ARGS.append("--disable-dev-shm-usage")


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


def _dismiss_report_password_popup(page):
    """进入 /report 后常见「强制改密」弹窗：先 Esc → 等 2 秒 → 再 Esc，再操作左侧菜单。"""
    page.keyboard.press("Escape")
    page.wait_for_timeout(2000)
    page.keyboard.press("Escape")


def _ensure_report_table_visible(page, max_extra_esc_rounds=5):
    """
    等待 #creditLostFixSummaryTable 出现（说明弹层已关）；超时则再按 ESC，与手工调试一致。
    """
    for _ in range(max_extra_esc_rounds):
        try:
            page.wait_for_selector("#creditLostFixSummaryTable", timeout=1000)
            return
        except PlaywrightTimeout:
            print("⚠️ 表格未出现，再次按 ESC...")
            page.keyboard.press("Escape")
            page.wait_for_timeout(1000)


def _misc_report_heading_locator(page):
    misc_xpath = (
        '//div[contains(@class, "panel-heading")]//label[contains(., "Miscellaneous Report")]'
        ' | //h4[contains(., "Miscellaneous Report")]'
        ' | //div[contains(@class, "panel-heading")]//*[contains(normalize-space(.), "Miscellaneous Report")]'
    )
    return page.locator(f"xpath={misc_xpath}").first


def _prepare_report_page_layout(page):
    """Headless 小视口下侧栏常被 CSS 隐藏，先固定桌面布局再操作菜单。"""
    page.evaluate("() => { window.scrollTo(0, 0); }")
    page.wait_for_timeout(300)


def _wait_body_contains_misc_menu(page, timeout=90000):
    """等 Angular 把侧栏文案渲染进 DOM（innerText 含 miscellaneous + report）。"""
    page.wait_for_function(
        """() => {
            const b = document.body;
            if (!b) return false;
            const t = (b.innerText || '').toLowerCase();
            return t.includes('miscellaneous') && t.includes('report');
        }""",
        timeout=timeout,
    )


def _try_click_misc_in_frame(frame):
    """在单个 frame 内尝试点击 Miscellaneous Report 标题。"""
    xp = (
        '//div[contains(@class, "panel-heading")]//label[contains(., "Miscellaneous")]'
        '[contains(., "Report")]'
        ' | //h4[contains(., "Miscellaneous")]'
        ' | //*[contains(@class, "panel-heading")]//*[contains(., "Miscellaneous")]'
    )
    loc = frame.locator(f"xpath={xp}").first
    try:
        loc.wait_for(state="attached", timeout=8000)
        loc.scroll_into_view_if_needed()
        loc.click(timeout=8000)
        return True
    except PlaywrightTimeout:
        pass
    except Exception:
        pass
    try:
        loc2 = frame.get_by_text(MISC_MENU_TEXT_RE).first
        loc2.wait_for(state="attached", timeout=8000)
        loc2.scroll_into_view_if_needed()
        loc2.click(timeout=8000, force=True)
        return True
    except PlaywrightTimeout:
        pass
    except Exception:
        pass
    return False


def _click_misc_menu_via_js(page):
    """不依赖 Playwright visible，直接在 DOM 中找含文案的 panel 并派发自定义 click。"""
    return page.evaluate(
        """() => {
            const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
            const nodes = document.querySelectorAll(
                '.panel-heading, .panel-heading *, label, h4, [class*="panel"]'
            );
            for (const el of nodes) {
                const raw = norm(el.innerText || el.textContent || '');
                if (!/miscellaneous/i.test(raw) || !/report/i.test(raw)) continue;
                let head = el.closest('.panel-heading');
                if (!head) head = el.closest('[class*="panel"]') || el;
                try {
                    head.scrollIntoView({ block: 'center', inline: 'nearest' });
                    head.click();
                    return true;
                } catch (e1) {
                    try {
                        head.dispatchEvent(
                            new MouseEvent('click', { bubbles: true, cancelable: true, view: window })
                        );
                        return true;
                    } catch (e2) {}
                }
            }
            return false;
        }"""
    )


def _click_miscellaneous_report_section(page):
    """展开 Miscellaneous Report（主 frame / 子 frame / XPath / 正则 / JS 兜底）。"""
    print("📂 展开 Miscellaneous Report 菜单...")
    _prepare_report_page_layout(page)

    try:
        _wait_body_contains_misc_menu(page, timeout=90000)
    except PlaywrightTimeout:
        print("⚠️ 90s 内页面 innerText 仍未出现 Miscellaneous+Report，继续尝试定位…")

    primary = _misc_report_heading_locator(page)
    try:
        primary.wait_for(state="attached", timeout=20000)
        primary.scroll_into_view_if_needed()
        primary.click(timeout=12000, force=True)
        page.wait_for_timeout(1500)
        return
    except PlaywrightTimeout:
        pass
    except Exception as e:
        print(f"⚠️ 主 frame XPath 点击失败: {e}")

    # Playwright 文本引擎（正则，兼容多空格）
    try:
        fb = page.get_by_text(MISC_MENU_TEXT_RE).first
        fb.wait_for(state="attached", timeout=20000)
        fb.scroll_into_view_if_needed()
        fb.click(timeout=12000, force=True)
        page.wait_for_timeout(1500)
        return
    except PlaywrightTimeout:
        pass
    except Exception as e:
        print(f"⚠️ get_by_text(正则) 失败: {e}")

    # 子 frame（少数部署把侧栏放在 iframe）
    for frame in page.frames:
        if frame.is_detached():
            continue
        u = ""
        try:
            u = frame.url or ""
        except Exception:
            pass
        if u.startswith("about:"):
            continue
        if _try_click_misc_in_frame(frame):
            page.wait_for_timeout(1500)
            return

    print("⚠️ 尝试用浏览器脚本点击 Miscellaneous Report …")
    if _click_misc_menu_via_js(page):
        page.wait_for_timeout(1500)
        return

    page.screenshot(path="error_misc_menu.png")
    raise RuntimeError(
        "无法展开 Miscellaneous Report：主页面与子 frame、JS 兜底均失败。"
        "已保存 error_misc_menu.png；请确认服务器上 /report 与本地一致、无额外登录页。"
    )


def _open_credit_lost_proposal_report(page):
    """展开 Miscellaneous Report 并点击 CREDIT_LOST_FIX_PROPOSAL_REPORT（兼容子节点文案）。"""
    _click_miscellaneous_report_section(page)

    print("🖱️ 点击 CREDIT_LOST_FIX_PROPOSAL_REPORT...")
    # contains(., ...) 包含子节点文本；text() 仅直接文本，易导致 li 匹配不到
    report_xpath = (
        f'//a[contains(., "{REPORT_MENU_LABEL_FULL}")]'
        f' | //li[contains(., "{REPORT_MENU_LABEL_FULL}")]'
        f' | //span[contains(., "{REPORT_MENU_LABEL_FULL}")]'
        ' | //a[contains(., "CREDIT_LOST_FIX")]'
        ' | //li[contains(., "CREDIT_LOST_FIX")]'
    )
    report_link = page.locator(f"xpath={report_xpath}").first

    try:
        report_link.wait_for(state="visible", timeout=20000)
    except PlaywrightTimeout:
        print("⚠️ XPath 未命中，改用 Playwright has_text（正则）…")
        report_link = (
            page.locator('a, li, span, [role="link"], .list-group-item, [ng-click]')
            .filter(has_text=REPORT_MENU_LABEL_RE)
            .first
        )
        try:
            report_link.wait_for(state="visible", timeout=20000)
        except PlaywrightTimeout:
            print("⚠️ 仍未可见，再次点击 Miscellaneous Report 折叠后再试…")
            _click_miscellaneous_report_section(page)
            report_link = (
                page.locator('a, li, span, [role="link"], .list-group-item')
                .filter(has_text=REPORT_MENU_LABEL_RE)
                .first
            )
            try:
                report_link.wait_for(state="visible", timeout=MENU_TIMEOUT_MS)
            except PlaywrightTimeout:
                report_link.wait_for(state="attached", timeout=15000)
                report_link.scroll_into_view_if_needed()

    report_link.scroll_into_view_if_needed()
    try:
        report_link.click(timeout=15000)
    except PlaywrightTimeout:
        report_link.click(force=True, timeout=15000)
    page.wait_for_timeout(500)


def _goto_report(page):
    page.goto(REPORT_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    page.wait_for_timeout(500)
    _dismiss_report_password_popup(page)
    _ensure_report_table_visible(page)


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
    navigated_via_goto = False
    try:
        page.wait_for_url("**/report", timeout=15000)
        print("✅ 页面已自动跳转到报表页")
    except PlaywrightTimeout:
        print("⚠️ 未自动跳转，正在手动导航到 /report...")
        _goto_report(page)
        navigated_via_goto = True
    if not navigated_via_goto:
        _dismiss_report_password_popup(page)
        _ensure_report_table_visible(page)

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

        ctx_opts = {
            "viewport": DESKTOP_VIEWPORT,
            "user_agent": DESKTOP_USER_AGENT,
            "device_scale_factor": 1,
            "is_mobile": False,
            "has_touch": False,
        }
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

            # /report 弹窗已在 _goto_report / _do_full_login 内用 Esc 处理

            # ---------- 以下为通用查询流程 ----------
            try:
                page.wait_for_load_state("networkidle", timeout=45000)
            except PlaywrightTimeout:
                pass
            page.wait_for_timeout(1000)

            _open_credit_lost_proposal_report(page)

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
