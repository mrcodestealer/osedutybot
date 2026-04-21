"""
FPMS 自动登录与表格抓取模块（支持指定日期查询）
用法：python3 fpms_fetcher.py [DD/MM]
示例：python3 fpms_fetcher.py 14/03   # 查询3月14日的数据
      python3 fpms_fetcher.py         # 默认查询昨天的数据
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

def fetch_fpms_data(headless=False, target_date_str=None):
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
                page.wait_for_url("**/report", timeout=2000)
                print("✅ 已到达报表页面")
            except PlaywrightTimeout:
                print("⚠️ 未自动跳转，手动导航到 /report")
                page.goto(REPORT_URL, wait_until="networkidle")

            if "/report" not in page.url:
                print("⚠️ 再次导航到报表页...")
                page.goto(REPORT_URL, wait_until="networkidle")

            print("⏳ 等待约2秒让弹窗出现...")
            page.wait_for_timeout(2000)
            print("🛡️ 按 ESC 关闭弹窗...")
            page.keyboard.press("Escape")
            page.wait_for_timeout(500)

            try:
                page.wait_for_selector("#creditLostFixSummaryTable", timeout=1000)
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

            page.wait_for_selector("#creditLostFixProposalReportQuery", timeout=15000)
            print("✅ 进入报表查询界面")

            # ---------- 通用函数：纯 JS 设置 select 值 ----------
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

            # 1. Product (Multiple) -> 全选
            print("📌 选择 Product: 全选")
            all_product_values = page.evaluate("""
                () => {
                    const select = document.querySelector('select[ng-model="vm.creditLostFixProposalReportQuery.platformList"]');
                    if (!select) return [];
                    return Array.from(select.options).map(opt => opt.value).filter(v => v && v !== '?');
                }
            """)
            if all_product_values:
                set_select_value('select[ng-model="vm.creditLostFixProposalReportQuery.platformList"]', all_product_values, multiple=True)
                print("✅ Product 全选完成")
            else:
                print("⚠️ 未能获取 Product 选项，跳过")

            # 2. 设置日期（支持命令行参数）
            print("📅 计算日期范围...")
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            if target_date_str:
                try:
                    day, month = map(int, target_date_str.split('/'))
                    target_date = datetime(today.year, month, day)
                except ValueError:
                    raise ValueError("日期格式错误，应为 DD/MM，例如 14/03")
            else:
                target_date = today  # 默认结束日期为今天（查询昨天到今天的数据）
            end_date = target_date
            start_date = end_date - timedelta(days=1)
            start_str = start_date.strftime("%Y/%m/%d 00:00:00")
            end_str = end_date.strftime("%Y/%m/%d 00:00:00")
            print(f"📅 查询日期范围：{start_str} 至 {end_str}")

            start_label = page.locator('label:has-text("Start date")')
            start_input = start_label.locator('..').locator('input')
            start_input.click()
            start_input.fill(start_str)

            end_label = page.locator('label:has-text("End date")')
            end_input = end_label.locator('..').locator('input')
            end_input.click()
            end_input.fill(end_str)

            # 3. Proposal Type -> 全选
            print("📌 选择 Proposal Type: 全选")
            all_type_values = page.evaluate("""
                () => {
                    const select = document.querySelector('select[ng-model="vm.creditLostFixProposalReportQuery.proposalTypeNames"]');
                    if (!select) return [];
                    return Array.from(select.options).map(opt => opt.value).filter(v => v && v !== '?');
                }
            """)
            if all_type_values:
                set_select_value('select[ng-model="vm.creditLostFixProposalReportQuery.proposalTypeNames"]', all_type_values, multiple=True)
                print("✅ Proposal Type 全选完成")
            else:
                print("⚠️ 未能获取 Proposal Type 选项，跳过")

            # 4. Provider -> "all"
            print("📌 选择 Provider: All")
            set_select_value('select[ng-model="vm.creditLostFixProposalReportQuery.providerId"]', "all")
            print("✅ Provider 已设置为 All")

            # 5. Proposal Status -> "Success"
            print("📌 选择 Proposal Status: Success")
            success_set = False
            for attempt in range(3):
                set_select_value('select[ng-model="vm.creditLostFixProposalReportQuery.proposalStatus"]', "Success")
                current_val = page.evaluate('document.querySelector(\'select[ng-model="vm.creditLostFixProposalReportQuery.proposalStatus"]\').value')
                print(f"   尝试 {attempt+1}: select.value = {current_val}")
                if current_val == "Success":
                    success_set = True
                    break
                page.wait_for_timeout(500)
            if not success_set:
                raise Exception("Proposal Status 无法设置为 Success")
            print("✅ Proposal Status 已设置为 Success")

            # 6. 点击 Search
            print("🔍 点击 Search 按钮...")
            search_btn = page.locator('button:has-text("Search")').first
            search_btn.click()

            # 等待5秒让数据加载
            print("⏳ 等待8秒让数据加载...")
            page.wait_for_timeout(8000)

            # 等待 Total 统计标签出现
            print("⏳ 等待查询结果统计...")
            total_label = page.locator('label.ng-binding:has-text("Total")').first
            total_label.wait_for(state="visible", timeout=30000)
            total_text = total_label.text_content().strip()
            print(f"📊 统计信息: {total_text}")

            # 根据记录数返回消息
            if "Total 0 records" in total_text:
                print("✅ 今日无 Amount Loss 记录")
                return "no amount loss record found for today"
            else:
                print("⏳ 等待表格数据加载...")
                page.wait_for_selector(TABLE_SELECTOR, timeout=10000)
                rows = page.query_selector_all(TABLE_SELECTOR)
                data = []
                for row in rows:
                    cells = row.query_selector_all("td")
                    row_data = [c.inner_text().strip() for c in cells]
                    if row_data:
                        data.append(row_data)
                print(f"📊 成功抓取 {len(data)} 行数据")
                return "as checked amount loss have record today"

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
    headless = "--headless" in sys.argv
    target_date = None
    # 解析命令行参数，忽略以 "--" 开头的选项
    for arg in sys.argv[1:]:
        if not arg.startswith("--"):
            target_date = arg
            break
    result = fetch_fpms_data(headless=headless, target_date_str=target_date)
    print("\n===== 最终结果 =====")
    print(result)