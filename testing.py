"""
FPMS 自动登录与表格抓取模块（智能定位版）
"""
import pyotp
import sys
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# 配置
LOGIN_URL = "https://mgnt-webserver.casinoplus.top/"
USERNAME = "CPOM01"
PASSWORD = "8c0fa1"
TOTP_SECRET = "MNYG63JQGEYTMOJTHE4DMMBTGQYDIOI"
REPORT_URL_PATTERN = "**/report**"
TABLE_SELECTOR = "#creditLostFixSummaryTable tbody tr"

def fetch_fpms_data(headless=False):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=100 if not headless else 0)
        page = browser.new_page()

        try:
            print("🌐 访问登录页...")
            page.goto(LOGIN_URL, wait_until="networkidle")

            # 1. 登录
            print("🔐 填写用户名密码...")
            page.fill("#username", USERNAME)
            page.fill("#password", PASSWORD)
            page.click('input[type="submit"]')

            # 2. 等待 TOTP 页面特征文本出现（确保页面已切换）
            print("⏳ 等待 TOTP 验证页面加载...")
            # 根据常见 TOTP 页面文本调整
            totp_text_locator = page.get_by_text(
                "Google Authenticator", exact=False
            ).or_(page.get_by_text("验证码", exact=False)).or_(
                page.get_by_text("Verification Code", exact=False)
            ).or_(page.get_by_text("6位数字", exact=False))
            totp_text_locator.wait_for(timeout=10000)
            print("✅ TOTP 页面已加载")

            # 3. 定位 TOTP 输入框 —— 使用语义化方法
            # 方法 A：通过 placeholder（最可靠）
            totp_input = None
            placeholder_candidates = [
                "验证码", "Authenticator", "6-digit", "6位", "Code", "OTP", "TOTP"
            ]
            for placeholder in placeholder_candidates:
                loc = page.get_by_placeholder(placeholder, exact=False).first
                if loc.count() > 0 and loc.is_visible():
                    totp_input = loc
                    print(f"✅ 通过 placeholder 定位输入框: '{placeholder}'")
                    break

            # 方法 B：如果 placeholder 失败，通过 role 和邻近文本定位
            if not totp_input:
                # 查找包含 "验证码" 或 "Code" 的 label 关联的 input
                label_loc = page.get_by_text("验证码", exact=False).or_(
                    page.get_by_text("Code", exact=False)
                ).first
                if label_loc.count() > 0:
                    # 尝试通过 label 的 for 属性或父级寻找输入框
                    label_for = label_loc.get_attribute("for")
                    if label_for:
                        totp_input = page.locator(f"#{label_for}")
                    else:
                        # 在 label 附近查找 input
                        totp_input = label_loc.locator("..").get_by_role("textbox").first
                    if totp_input and totp_input.is_visible():
                        print("✅ 通过关联标签定位输入框")

            # 方法 C：直接找页面上唯一的数字输入框
            if not totp_input:
                inputs = page.get_by_role("textbox").all()
                for inp in inputs:
                    # 排除用户名/密码类型
                    if inp.get_attribute("type") == "password":
                        continue
                    if inp.get_attribute("name") in ["username", "user"]:
                        continue
                    if inp.get_attribute("id") in ["username", "password"]:
                        continue
                    if inp.is_visible():
                        totp_input = inp
                        print("✅ 使用第一个可见文本框作为输入框")
                        break

            if not totp_input:
                page.screenshot(path="totp_not_found.png")
                raise Exception("❌ 无法定位 TOTP 输入框，截图已保存")

            # 4. 填入验证码
            code = pyotp.TOTP(TOTP_SECRET).now()
            print(f"🔢 填入验证码: {code}")
            totp_input.fill(code)

            # 5. 提交验证
            print("⏳ 提交验证...")
            # 优先找“验证”按钮
            try:
                verify_btn = page.get_by_role("button", name="验证").or_(
                    page.get_by_role("button", name="Verify")
                ).or_(page.get_by_role("button", name="Submit"))
                verify_btn.click(timeout=3000)
                print("✅ 点击验证按钮")
            except:
                page.keyboard.press("Enter")
                print("⚠️ 按 Enter 提交")

            # 6. 等待登录成功跳转
            print("⏳ 等待跳转到报表页面...")
            page.wait_for_url(REPORT_URL_PATTERN, timeout=20000)
            print("✅ 登录成功")

            # 7. 抓取表格
            print("⏳ 等待表格数据...")
            page.wait_for_selector(TABLE_SELECTOR, timeout=30000)
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
    headless = "--headless" in sys.argv
    data = fetch_fpms_data(headless=headless)
    print("\n===== 抓取结果 =====")
    for i, row in enumerate(data):
        print(f"第{i+1}行: {row}")