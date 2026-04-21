"""
FPMS 自动登录与表格抓取模块（支持保存/加载浏览器状态）
用法：
    python3 fpms_fetcher.py --save-state   # 有头模式手动登录一次，保存状态
    python3 fpms_fetcher.py                # 无头模式使用已保存状态自动查询
    python3 fpms_fetcher.py DD/MM          # 指定日期查询
"""
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

def fetch_fpms_data(headless=False, target_date_str=None, save_state=False):
    with sync_playwright() as p:
        # 启动持久化上下文（如果状态文件存在则加载）
        if os.path.exists(STATE_FILE) and not save_state:
            context = p.chromium.launch_persistent_context(
                user_data_dir="./browser_data",
                headless=headless,
                args=['--disable-blink-features=AutomationControlled']
            )
        else:
            # 首次或保存状态时：无状态文件时用普通 launch；必须尊重 headless（服务器无 X11 时需 True）
            browser = p.chromium.launch(
                headless=headless,
                slow_mo=100 if not headless else 0,
                args=['--disable-blink-features=AutomationControlled'],
            )
            context = browser.new_context()
        
        page = context.new_page()

        try:
            # 如果处于保存状态模式，手动登录
            if save_state or not os.path.exists(STATE_FILE):
                print("🔐 需要进行手动登录（有头模式）...")
                page.goto(LOGIN_URL, wait_until="networkidle")
                page.fill("#username", USERNAME)
                page.fill("#password", PASSWORD)
                page.click('input[type="submit"]')
                page.wait_for_selector("#OTP", state="visible", timeout=30000)
                code = pyotp.TOTP(TOTP_SECRET).now()
                print(f"🔢 当前验证码: {code}")
                page.fill("#OTP", code)
                page.keyboard.press("Enter")
                # 等待登录完成
                try:
                    page.wait_for_url("**/report", timeout=5000)
                    print("✅ 页面已自动跳转到报表页")
                except PlaywrightTimeout:
                    print("⚠️ 未自动跳转，正在手动导航到 /report...")
                    page.goto(REPORT_URL, wait_until="networkidle")
                print("✅ 登录成功，正在保存浏览器状态...")
                context.storage_state(path=STATE_FILE)
                print(f"✅ 状态已保存到 {STATE_FILE}")
                # 如果只是保存状态，则退出
                if save_state:
                    return "state_saved"
                # 否则继续执行后续查询
            else:
                # 正常查询流程：直接访问报表页
                print("🚀 使用已保存状态直接访问报表页")
                page.goto(REPORT_URL, wait_until="networkidle")
                page.wait_for_timeout(2000)
                page.keyboard.press("Escape")
                page.wait_for_timeout(500)

            # ---------- 以下为通用查询流程 ----------
            # 点击菜单进入目标报表
            print("📂 展开 Miscellaneous Report 菜单...")
            misc_heading = page.locator('div.panel-heading:has-text("Miscellaneous Report")')
            misc_heading.wait_for(state="visible", timeout=15000)
            misc_heading.click()

            print("🖱️ 点击 CREDIT_LOST_FIX_PROPOSAL_REPORT...")
            report_link = page.locator('li:has-text("CREDIT_LOST_FIX_PROPOSAL_REPORT")')
            report_link.wait_for(state="visible", timeout=10000)
            report_link.click()

            page.wait_for_selector("#creditLostFixProposalReportQuery", timeout=15000)
            print("✅ 进入报表查询界面")

            # 通用JS设置函数
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

            # 1. Product 全选
            all_product_values = page.evaluate("""
                () => {
                    const select = document.querySelector('select[ng-model="vm.creditLostFixProposalReportQuery.platformList"]');
                    if (!select) return [];
                    return Array.from(select.options).map(opt => opt.value).filter(v => v && v !== '?');
                }
            """)
            if all_product_values:
                set_select_value('select[ng-model="vm.creditLostFixProposalReportQuery.platformList"]', all_product_values, multiple=True)

            # 2. 日期
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            if target_date_str:
                day, month = map(int, target_date_str.split('/'))
                target_date = datetime(today.year, month, day)
            else:
                target_date = today
            end_date = target_date
            start_date = end_date - timedelta(days=1)
            start_str = start_date.strftime("%Y/%m/%d 00:00:00")
            end_str = end_date.strftime("%Y/%m/%d 00:00:00")

            page.locator('label:has-text("Start date")').locator('..').locator('input').fill(start_str)
            page.locator('label:has-text("End date")').locator('..').locator('input').fill(end_str)

            # 3. Proposal Type 全选
            all_type_values = page.evaluate("""
                () => {
                    const select = document.querySelector('select[ng-model="vm.creditLostFixProposalReportQuery.proposalTypeNames"]');
                    if (!select) return [];
                    return Array.from(select.options).map(opt => opt.value).filter(v => v && v !== '?');
                }
            """)
            if all_type_values:
                set_select_value('select[ng-model="vm.creditLostFixProposalReportQuery.proposalTypeNames"]', all_type_values, multiple=True)

            # 4. Provider -> "all"
            set_select_value('select[ng-model="vm.creditLostFixProposalReportQuery.providerId"]', "all")

            # 5. Proposal Status -> "Success"
            for attempt in range(3):
                set_select_value('select[ng-model="vm.creditLostFixProposalReportQuery.proposalStatus"]', "Success")
                current_val = page.evaluate('document.querySelector(\'select[ng-model="vm.creditLostFixProposalReportQuery.proposalStatus"]\').value')
                if current_val == "Success":
                    break
                page.wait_for_timeout(500)

            # 6. Search
            page.locator('button:has-text("Search")').first.click()
            page.wait_for_timeout(5000)

            total_label = page.locator('label.ng-binding:has-text("Total")').first
            total_label.wait_for(state="visible", timeout=30000)
            total_text = total_label.text_content().strip()

            if "Total 0 records" in total_text:
                return "no amount loss record found for today"
            else:
                return "as checked amount loss have record today"

        except Exception as e:
            print(f"❌ 错误: {e}")
            page.screenshot(path="error_screenshot.png")
            raise
        finally:
            context.close()
            if 'browser' in locals():
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