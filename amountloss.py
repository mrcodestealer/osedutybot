"""
FPMS 自动登录与表格抓取模块（最终版：包含 Provider 选择）

用法:
  python3 amountloss_backup.py           # 有界面
  python3 amountloss_backup.py -headless # 无头（亦支持 --headless）
"""
import pyotp
import sys
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

LOGIN_URL = "https://mgnt-webserver.casinoplus.top/"
USERNAME = "CPOM01"
PASSWORD = "8c0fa1"
TOTP_SECRET = "MNYG63JQGEYTMOJTHE4DMMBTGQYDIOI"
TABLE_SELECTOR = "#creditLostFixSummaryTable tbody tr"
REPORT_URL = "https://mgnt-webserver.casinoplus.top/report"

def fetch_fpms_data(headless=False):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=100 if not headless else 0)
        page = browser.new_page()

        try:
            # ---------- 登录部分 ----------
            print("🌐 访问登录页...")
            page.goto(LOGIN_URL, wait_until="networkidle")

            print("🔐 填写用户名密码并登录...")
            page.fill("#username", USERNAME)
            page.fill("#password", PASSWORD)
            page.click('input[type="submit"]')

            print("⏳ 等待 TOTP 输入框出现...")
            page.wait_for_selector("#OTP", state="visible", timeout=10000)

            code = pyotp.TOTP(TOTP_SECRET).now()
            print(f"🔢 当前验证码: {code}")

            totp_input = page.locator("#OTP")
            totp_input.click()
            totp_input.fill(code)
            print("⏳ 按 Enter 提交验证...")
            totp_input.press("Enter")

            # ---------- 导航到报表页并处理弹窗 ----------
            print("⏳ 等待登录完成...")
            try:
                page.wait_for_url("**/report", timeout=10000)
                print("✅ 已到达报表页面")
            except PlaywrightTimeout:
                print("⚠️ 未自动跳转，手动导航到 /report")
                page.goto(REPORT_URL, wait_until="networkidle")

            if "/report" not in page.url:
                print("⚠️ 再次导航到报表页...")
                page.goto(REPORT_URL, wait_until="networkidle")

            print("⏳ 等待约6秒让弹窗出现...")
            page.wait_for_timeout(6000)
            print("🛡️ 按 ESC 关闭弹窗...")
            page.keyboard.press("Escape")
            page.wait_for_timeout(1000)

            # 二次尝试按 ESC（如果弹窗仍在）
            try:
                page.wait_for_selector("#creditLostFixSummaryTable", timeout=3000)
            except PlaywrightTimeout:
                print("⚠️ 表格未出现，再次按 ESC...")
                page.keyboard.press("Escape")
                page.wait_for_timeout(1000)

            # ---------- 点击菜单进入目标报表 ----------
            print("📂 展开 Miscellaneous Report 菜单...")
            misc_heading = page.locator('div.panel-heading:has-text("Miscellaneous Report")')
            misc_heading.wait_for(state="visible", timeout=10000)
            misc_heading.click()

            print("🖱️ 点击 CREDIT_LOST_FIX_PROPOSAL_REPORT...")
            report_link = page.locator('li:has-text("CREDIT_LOST_FIX_PROPOSAL_REPORT")')
            report_link.wait_for(state="visible", timeout=10000)
            report_link.click()

            # 等待报表查询区域加载
            page.wait_for_selector("#creditLostFixProposalReportQuery", timeout=15000)
            print("✅ 进入报表查询界面")

            # ---------- 设置筛选条件 ----------
            # 1. Product (Multiple) -> 全选
            print("📌 选择 Product: 全选")
            product_label = page.locator('label:has-text("Product (Multiple)")')
            product_dropdown = product_label.locator('..').locator('button.dropdown-toggle')
            product_dropdown.click()
            page.wait_for_selector('.bootstrap-select.open .bs-actionsbox .bs-select-all', timeout=5000)
            page.locator('.bootstrap-select.open .bs-actionsbox .bs-select-all').click()
            page.keyboard.press("Escape")

            # 2. 设置日期：昨天 00:00:00 至 今天 00:00:00
            print("📅 设置日期范围：昨天 00:00:00 至 今天 00:00:00")
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday = today - timedelta(days=1)
            start_str = yesterday.strftime("%Y/%m/%d 00:00:00")
            end_str = today.strftime("%Y/%m/%d 00:00:00")

            start_label = page.locator('label:has-text("Start date")')
            start_input = start_label.locator('..').locator('input')
            start_input.click()
            start_input.fill(start_str)

            end_label = page.locator('label:has-text("End date")')
            end_input = end_label.locator('..').locator('input')
            end_input.click()
            end_input.fill(end_str)

            # 3. Proposal Type -> 全选（点击三个选项）
            print("📌 选择 Proposal Type: 全选 (点击三个选项)")
            proposal_label = page.locator('label:has-text("Proposal Type")')
            proposal_dropdown = proposal_label.locator('..').locator('button.dropdown-toggle')
            proposal_dropdown.click()
            page.wait_for_selector('.bootstrap-select.open .dropdown-menu.inner', timeout=5000)

            options = [
                "Auto-fix Credit Lost",
                "Fix Platform Credit Lost",
                "manual credit lost"
            ]
            for opt in options:
                option_locator = page.locator(f'.bootstrap-select.open .dropdown-menu.inner li a:has-text("{opt}")')
                option_locator.wait_for(state="visible", timeout=3000)
                option_locator.click()
                page.wait_for_timeout(200)

            page.keyboard.press("Escape")

            # 4 & 5. Provider = all、Proposal Status = Success（option 的 value 区分大小写；需触发 Angular digest）
            def set_native_select(selector, value):
                page.wait_for_selector(selector, timeout=10000)
                page.evaluate(
                    """
                    ([selector, value]) => {
                        const select = document.querySelector(selector);
                        if (!select) throw new Error('Select not found: ' + selector);
                        select.value = value;
                        select.dispatchEvent(new Event('input', { bubbles: true }));
                        select.dispatchEvent(new Event('change', { bubbles: true }));
                        const scope = angular.element(select).scope();
                        if (scope) scope.$apply();
                        return select.value;
                    }
                    """,
                    [selector, value],
                )

            print("📌 选择 Provider: All（ng-model，value=all）")
            set_native_select(
                'select[ng-model="vm.creditLostFixProposalReportQuery.providerId"]',
                "all",
            )

            print("📌 选择 Proposal Status: Success（value 必须为 Success，不是 success）")
            page.wait_for_timeout(300)
            for attempt in range(5):
                set_native_select(
                    'select[ng-model="vm.creditLostFixProposalReportQuery.proposalStatus"]',
                    "Success",
                )
                current = page.evaluate(
                    """() => document.querySelector(
                        'select[ng-model="vm.creditLostFixProposalReportQuery.proposalStatus"]'
                    ).value"""
                )
                if current == "Success":
                    break
                page.wait_for_timeout(400)
            print("✅ Provider / Proposal Status 已设置")

            # 6. 点击 Search 按钮
            print("🔍 点击 Search 按钮...")
            search_btn = page.locator('button:has-text("Search")').first
            search_btn.click()

            # 临时等待10秒观察页面状态
            print("⏳ 等待10秒观察页面状态...")
            page.wait_for_timeout(10000)

            # ---------- 等待表格数据 ----------
            print("⏳ 等待表格数据加载...")
            page.wait_for_selector(TABLE_SELECTOR, timeout=30000)
            print("✅ 表格已加载")

            # 抓取表格
            rows = page.query_selector_all(TABLE_SELECTOR)
            data = []
            for row in rows:
                cells = row.query_selector_all("td")
                row_data = [c.inner_text().strip() for c in cells]
                if row_data:
                    data.append(row_data)

            print(f"📊 成功抓取 {len(data)} 行数据")
            return data

        except Exception as e:
            print(f"❌ 脚本执行出错: {e}")
            try:
                page.screenshot(path="error_screenshot.png")
                print("📸 已保存错误截图 error_screenshot.png")
            except:
                pass
            raise
        finally:
            browser.close()

if __name__ == "__main__":
    headless = "--headless" in sys.argv or "-headless" in sys.argv
    data = fetch_fpms_data(headless=headless)
    print("\n===== 抓取结果 =====")
    for i, row in enumerate(data):
        print(f"第{i+1}行: {row}")

