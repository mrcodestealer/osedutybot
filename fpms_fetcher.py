"""
FPMS 自动登录与表格抓取模块（最终稳定版 - 增强菜单点击）
"""
import pyotp
import sys
import json
import os
import time
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

LOGIN_URL = "https://mgnt-webserver.casinoplus.top/"
USERNAME = "CPOM01"
PASSWORD = "8c0fa1"
TOTP_SECRET = "MNYG63JQGEYTMOJTHE4DMMBTGQYDIOI"
TABLE_SELECTOR = "#creditLostFixSummaryTable tbody tr"
REPORT_URL = "https://mgnt-webserver.casinoplus.top/report"
COOKIES_FILE = "cookies.json"

def fetch_fpms_data(headless=False, target_date_str=None):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            slow_mo=100 if not headless else 0,
            args=['--disable-blink-features=AutomationControlled']
        )
        context = browser.new_context()
        page = context.new_page()

        try:
            # ---------- Cookies 注入 ----------
            if os.path.exists(COOKIES_FILE):
                print("🍪 加载 cookies.json ...")
                with open(COOKIES_FILE, 'r') as f:
                    cookies = json.load(f)
                
                valid_same_site = {"Strict", "Lax", "None"}
                for cookie in cookies:
                    if "sameSite" in cookie and cookie["sameSite"] not in valid_same_site:
                        cookie["sameSite"] = "Lax"
                
                context.add_cookies(cookies)
                print("🚀 直接访问报表页")
                page.goto(REPORT_URL, wait_until="networkidle")
                page.wait_for_timeout(3000)

                if "login" in page.url.lower() or page.locator('#username').count() > 0:
                    print("⚠️ Cookies 已失效，请更新 cookies.json")
                    return "cookies_expired"
                else:
                    print("✅ 已进入报表页")
                    page.wait_for_timeout(2000)
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(500)
            else:
                print("❌ cookies.json 不存在")
                return "no_cookies"

            # ---------- 使用更稳定的方式点击菜单 ----------
            print("📂 等待左侧菜单加载...")
            # 等待页面完全渲染
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(2000)
            
            # 尝试直接通过文本点击 Miscellaneous Report（使用 XPath）
            misc_xpath = '//div[contains(@class, "panel-heading")]//label[contains(text(), "Miscellaneous Report")] | //h4[contains(text(), "Miscellaneous Report")]'
            misc_heading = page.locator(f'xpath={misc_xpath}').first
            misc_heading.wait_for(state="visible", timeout=15000)
            # 检查是否已展开，若未展开则点击
            parent = misc_heading.evaluate("el => el.closest('.panel-heading')")
            if parent:
                is_expanded = page.evaluate("el => el.getAttribute('aria-expanded')", parent)
                if is_expanded != "true":
                    misc_heading.click()
                    page.wait_for_timeout(1000)
            else:
                misc_heading.click()
                page.wait_for_timeout(1000)
            
            print("🖱️ 点击 CREDIT_LOST_FIX_PROPOSAL_REPORT...")
            report_xpath = '//li[contains(text(), "CREDIT_LOST_FIX_PROPOSAL_REPORT")]'
            report_link = page.locator(f'xpath={report_xpath}').first
            report_link.wait_for(state="visible", timeout=10000)
            report_link.click()

            page.wait_for_selector("#creditLostFixProposalReportQuery", timeout=15000)
            print("✅ 进入报表查询界面")

            # ---------- 通用 JS 设置 ----------
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
            print("📌 Product 全选")
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
            print("📅 设置日期")
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
            print("🔍 点击 Search")
            page.locator('button:has-text("Search")').first.click()
            page.wait_for_timeout(5000)

            # 等待结果
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
            browser.close()

if __name__ == "__main__":
    headless = "--headless" in sys.argv
    target_date = None
    for arg in sys.argv[1:]:
        if not arg.startswith("--"):
            target_date = arg
            break
    result = fetch_fpms_data(headless=headless, target_date_str=target_date)
    print(result)