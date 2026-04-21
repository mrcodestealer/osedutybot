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

# 主流程总步数（用于 [FPMS x/y] 进度）
TOTAL_FLOW_STEPS = 16


def _fpms_log(step, message):
    """统一进度日志，便于服务器 / systemd 里对照卡在哪一步。"""
    print(f"[FPMS {step}/{TOTAL_FLOW_STEPS}] {message}", flush=True)


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


def _raise_if_cloudflare_blocked(page):
    """
    数据中心/服务器出口 IP 常被 Cloudflare WAF 直接拦截，返回与业务站完全不同的 HTML。
    无法靠 Playwright 改选择器解决，需换出口 IP、白名单或站点侧放行。
    """
    try:
        title = (page.title() or "").lower()
    except Exception:
        title = ""
    try:
        html = (page.content() or "")[:100000].lower()
    except Exception:
        html = ""
    if (
        "sorry, you have been blocked" in html
        or "cf-error-details" in html
        or ("cloudflare" in title and "attention required" in title)
    ):
        raise RuntimeError(
            "【Cloudflare 拦截】当前出口 IP 被站点 WAF 拒绝，页面为 “Sorry, you have been blocked”，"
            "不是 FPMS 业务页。请在 Cloudflare / 防火墙将本机出口 IP 加入允许列表，或改用未被拦的网络（如办公网）运行；"
            "仅靠改自动化脚本无法绕过。\n"
            "[EN] Egress IP blocked by Cloudflare WAF; whitelist this server IP or use an allowed network. "
            "Not a Playwright/locator issue."
        )
    if (
        "just a moment" in title
        or "checking your browser" in html
        or "cf-browser-verification" in html
        or "challenge-platform" in html
    ):
        raise RuntimeError(
            "【Cloudflare 挑战页】出现 “Just a moment” / 浏览器检测，无头环境通常无法自动通过。\n"
            "[EN] Cloudflare challenge page; use manual session export from a trusted browser or ask site to allow your IP."
        )


def _dismiss_report_password_popup(page):
    """进入 /report 后常见「强制改密」弹窗：先 Esc → 等 2 秒 → 再 Esc，再操作左侧菜单。"""
    page.keyboard.press("Escape")
    page.wait_for_timeout(2000)
    page.keyboard.press("Escape")


def _ensure_report_table_visible(page, max_extra_esc_rounds=5):
    """
    等待 #creditLostFixSummaryTable 出现（说明弹层已关）；超时则再按 ESC，与手工调试一致。
    """
    for i in range(max_extra_esc_rounds):
        try:
            page.wait_for_selector("#creditLostFixSummaryTable", timeout=1000)
            return
        except PlaywrightTimeout:
            print(
                f"  · [4/{TOTAL_FLOW_STEPS}] 主内容表格未在 1s 内出现，"
                f"第 {i + 1}/{max_extra_esc_rounds} 次 ESC 关弹层…",
                flush=True,
            )
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
    """等侧栏可用：innerText 含 miscellaneous+report，或已出现 panel / sidebar 壳子。"""
    page.wait_for_function(
        """() => {
            const b = document.body;
            if (!b) return false;
            const t = (b.innerText || '').toLowerCase();
            if (t.includes('miscellaneous') && t.includes('report')) return true;
            if (document.querySelector('.panel-group, .panel-heading, [class*="sidebar"], nav')) return true;
            return false;
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
    """不依赖 Playwright visible：先窄选 panel，再全文档小节点深度匹配文案并点击。"""
    ok = page.evaluate(
        """() => {
            const tryClick = (head) => {
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
                return false;
            };
            const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
            const narrow = document.querySelectorAll(
                '.panel-heading, .panel-heading *, label, h4, [class*="panel"]'
            );
            for (const el of narrow) {
                const raw = norm(el.innerText || el.textContent || '');
                if (!/miscellaneous/i.test(raw) || !/report/i.test(raw)) continue;
                let head = el.closest('.panel-heading') || el.closest('[class*="panel"]') || el;
                if (tryClick(head)) return true;
            }
            const all = document.querySelectorAll('body *');
            for (const el of all) {
                if (!el.getBoundingClientRect || el.getBoundingClientRect().width < 2) continue;
                const raw = norm(el.innerText || el.textContent || '');
                if (raw.length > 240) continue;
                if (!/miscellaneous/i.test(raw) || !/report/i.test(raw)) continue;
                let head = el.closest('.panel-heading') || el.closest('a, button, [ng-click]') || el;
                if (tryClick(head)) return true;
            }
            return false;
        }"""
    )
    return bool(ok)


def _try_expand_sidebar_collapsed(page):
    """部分主题在 headless 下侧栏折叠，尝试点汉堡 / data-toggle。"""
    selectors = (
        '[data-toggle="collapse"]',
        '.navbar-toggle',
        'button[aria-label*="menu" i]',
        'button[title*="Menu" i]',
        '[class*="sidebar-toggle"]',
        '[class*="menu-toggle"]',
    )
    for sel in selectors:
        loc = page.locator(sel).first
        try:
            if loc.count() == 0:
                continue
            loc.click(timeout=2000)
            page.wait_for_timeout(400)
        except Exception:
            continue


def _dump_report_page_debug(page, path_html="error_misc_menu.html"):
    """失败时写出截图 + 部分 HTML + URL，便于与本地对比。"""
    try:
        url = page.url
        title = page.title()
    except Exception:
        url, title = "(url?)", "(title?)"
    try:
        snippet = page.evaluate(
            """() => {
                const b = document.body;
                if (!b) return '';
                const t = b.innerText || '';
                return t.length > 1200 ? t.slice(0, 1200) + '…' : t;
            }"""
        )
    except Exception:
        snippet = "(innerText?)"
    print(f"[FPMS debug] url={url!r} title={title!r}", flush=True)
    print(f"[FPMS debug] body.innerText 前 1200 字:\n{snippet}", flush=True)
    try:
        html = page.content()
        if len(html) > 500_000:
            html = html[:500_000] + "\n<!-- truncated -->\n"
        with open(path_html, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[FPMS debug] 已写入页面 HTML: {path_html}", flush=True)
    except Exception as e:
        print(f"[FPMS debug] 无法写入 HTML: {e}", flush=True)


def _click_miscellaneous_report_section(page):
    """展开 Miscellaneous Report（主 frame / 子 frame / XPath / 正则 / JS 兜底）。"""
    print(f"  · [7/{TOTAL_FLOW_STEPS}] 展开左侧「Miscellaneous Report」…", flush=True)
    _prepare_report_page_layout(page)
    _try_expand_sidebar_collapsed(page)

    try:
        print(f"  · [7/{TOTAL_FLOW_STEPS}] 等待侧栏 DOM（文案或 panel 壳）…", flush=True)
        _wait_body_contains_misc_menu(page, timeout=90000)
    except PlaywrightTimeout:
        print(f"  · [7/{TOTAL_FLOW_STEPS}] 90s 内未检测到侧栏特征，仍继续尝试点击…", flush=True)

    primary = _misc_report_heading_locator(page)
    try:
        print(f"  · [7/{TOTAL_FLOW_STEPS}] 尝试主 frame XPath…", flush=True)
        primary.wait_for(state="attached", timeout=20000)
        primary.scroll_into_view_if_needed()
        primary.click(timeout=12000, force=True)
        page.wait_for_timeout(1500)
        print(f"  · [7/{TOTAL_FLOW_STEPS}] XPath 点击成功", flush=True)
        return
    except PlaywrightTimeout:
        pass
    except Exception as e:
        print(f"  · [7/{TOTAL_FLOW_STEPS}] XPath 失败: {e}", flush=True)

    try:
        print(f"  · [7/{TOTAL_FLOW_STEPS}] 尝试 get_by_text 正则…", flush=True)
        fb = page.get_by_text(MISC_MENU_TEXT_RE).first
        fb.wait_for(state="attached", timeout=20000)
        fb.scroll_into_view_if_needed()
        fb.click(timeout=12000, force=True)
        page.wait_for_timeout(1500)
        print(f"  · [7/{TOTAL_FLOW_STEPS}] get_by_text 点击成功", flush=True)
        return
    except PlaywrightTimeout:
        pass
    except Exception as e:
        print(f"  · [7/{TOTAL_FLOW_STEPS}] get_by_text 失败: {e}", flush=True)

    print(f"  · [7/{TOTAL_FLOW_STEPS}] 遍历 {len(page.frames)} 个 frame …", flush=True)
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
            print(f"  · [7/{TOTAL_FLOW_STEPS}] 子 frame 点击成功", flush=True)
            return

    print(f"  · [7/{TOTAL_FLOW_STEPS}] JS 深度匹配点击…", flush=True)
    if _click_misc_menu_via_js(page):
        page.wait_for_timeout(1500)
        print(f"  · [7/{TOTAL_FLOW_STEPS}] JS 兜底已执行", flush=True)
        return

    page.screenshot(path="error_misc_menu.png")
    _dump_report_page_debug(page)
    raise RuntimeError(
        "无法展开 Miscellaneous Report：主页面、子 frame、JS 兜底均失败。"
        "已保存 error_misc_menu.png、error_misc_menu.html；日志中有 url 与 body 摘要。"
    )


def _open_credit_lost_proposal_report(page):
    """展开 Miscellaneous Report 并点击 CREDIT_LOST_FIX_PROPOSAL_REPORT（兼容子节点文案）。"""
    _click_miscellaneous_report_section(page)

    print(f"  · [7/{TOTAL_FLOW_STEPS}] 点击子项 CREDIT_LOST_FIX_PROPOSAL_REPORT…", flush=True)
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
        print(f"  · [7/{TOTAL_FLOW_STEPS}] 子报表 XPath 未命中，改用 has_text 正则…", flush=True)
        report_link = (
            page.locator('a, li, span, [role="link"], .list-group-item, [ng-click]')
            .filter(has_text=REPORT_MENU_LABEL_RE)
            .first
        )
        try:
            report_link.wait_for(state="visible", timeout=20000)
        except PlaywrightTimeout:
            print(f"  · [7/{TOTAL_FLOW_STEPS}] 仍未可见，再次展开 Miscellaneous Report…", flush=True)
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
    _fpms_log(4, "打开 /report（domcontentloaded）…")
    page.goto(REPORT_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    page.wait_for_timeout(500)
    _raise_if_cloudflare_blocked(page)
    _fpms_log(4, "关闭改密相关弹层（Esc → 2s → Esc）…")
    _dismiss_report_password_popup(page)
    _fpms_log(4, "等待 #creditLostFixSummaryTable 或循环 ESC…")
    _ensure_report_table_visible(page)
    _fpms_log(4, "/report 页面准备阶段结束")


def _do_full_login(page, context, save_state_only=False):
    """完整账号+TOTP 登录；成功后写入 browser_state.json。"""
    _fpms_log(3, "完整登录：打开登录页并填写账号/密码/TOTP…")
    page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    _raise_if_cloudflare_blocked(page)
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
        _fpms_log(3, "登录后已跳转到 /report")
    except PlaywrightTimeout:
        _fpms_log(3, "未自动跳转 /report，改为 goto /report…")
        _goto_report(page)
        navigated_via_goto = True
    if not navigated_via_goto:
        _fpms_log(4, "已在 /report：关弹窗并等主表格…")
        _dismiss_report_password_popup(page)
        _ensure_report_table_visible(page)

    _raise_if_cloudflare_blocked(page)
    _fpms_log(3, "写入 browser_state.json …")
    context.storage_state(path=STATE_FILE)
    _fpms_log(3, f"状态已保存到 {STATE_FILE}")
    if save_state_only:
        return


def fetch_fpms_data(headless=False, target_date_str=None, save_state=False):
    with sync_playwright() as p:
        _fpms_log(1, f"启动 Playwright Chromium（headless={headless}）…")
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

        _fpms_log(2, "创建浏览器上下文（1920×1080 桌面 UA）…")
        context = browser.new_context(**ctx_opts)
        page = context.new_page()

        try:
            if save_state:
                _do_full_login(page, context, save_state_only=True)
                return "state_saved"

            # 会话恢复顺序：browser_state.json → cookies.json → 完整登录
            if ctx_opts.get("storage_state"):
                _fpms_log(3, "会话：加载 browser_state.json 并进入 /report …")
                _goto_report(page)
            elif os.path.exists(COOKIES_FILE):
                _fpms_log(3, "会话：注入 cookies.json 并进入 /report …")
                with open(COOKIES_FILE, "r", encoding="utf-8") as f:
                    context.add_cookies(_normalize_cookies(json.load(f)))
                _goto_report(page)
            else:
                _fpms_log(3, "会话：无本地 state/cookies，执行完整登录 …")
                _do_full_login(page, context, save_state_only=False)

            if _is_login_page(page) and os.path.exists(COOKIES_FILE) and ctx_opts.get("storage_state"):
                _fpms_log(3, "browser_state 可能失效，改试 cookies.json …")
                with open(COOKIES_FILE, "r", encoding="utf-8") as f:
                    context.add_cookies(_normalize_cookies(json.load(f)))
                _goto_report(page)

            if _is_login_page(page):
                _fpms_log(3, "仍像登录页，执行完整账号登录 …")
                _do_full_login(page, context, save_state_only=False)

            if _is_login_page(page):
                raise RuntimeError(
                    "仍停留在登录页：请检查网络、账号权限，或在本机执行 "
                    "`python fpms_fetcher.py --save-state` 生成有效的 browser_state.json 后上传到服务器。"
                )

            _raise_if_cloudflare_blocked(page)
            _fpms_log(5, "会话就绪：当前不在登录页，准备查询 Amount Loss …")

            # ---------- 以下为通用查询流程 ----------
            _fpms_log(6, "等待 networkidle（最多 45s）…")
            try:
                page.wait_for_load_state("networkidle", timeout=45000)
            except PlaywrightTimeout:
                _fpms_log(6, "networkidle 超时，继续下一步 …")
            page.wait_for_timeout(1000)

            _fpms_log(7, "导航到 CREDIT_LOST_FIX_PROPOSAL 查询界面 …")
            _open_credit_lost_proposal_report(page)

            _fpms_log(8, "等待查询表单 #creditLostFixProposalReportQuery …")
            page.wait_for_selector("#creditLostFixProposalReportQuery", timeout=MENU_TIMEOUT_MS)
            _fpms_log(8, "已进入报表查询界面")

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
            _fpms_log(9, "全选 Product …")
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

            _fpms_log(10, f"填写日期范围：{start_str} ~ {end_str} …")
            page.locator('label:has-text("Start date")').locator("..").locator("input").fill(start_str)
            page.locator('label:has-text("End date")').locator("..").locator("input").fill(end_str)

            all_type_values = page.evaluate("""
                () => {
                    const select = document.querySelector('select[ng-model="vm.creditLostFixProposalReportQuery.proposalTypeNames"]');
                    if (!select) return [];
                    return Array.from(select.options).map(opt => opt.value).filter(v => v && v !== '?');
                }
            """)
            _fpms_log(11, "全选 Proposal Type …")
            if all_type_values:
                set_select_value(
                    'select[ng-model="vm.creditLostFixProposalReportQuery.proposalTypeNames"]',
                    all_type_values,
                    multiple=True,
                )

            _fpms_log(12, "Provider=all，Proposal Status=Success …")
            set_select_value('select[ng-model="vm.creditLostFixProposalReportQuery.providerId"]', "all")

            for _ in range(3):
                set_select_value('select[ng-model="vm.creditLostFixProposalReportQuery.proposalStatus"]', "Success")
                current_val = page.evaluate(
                    'document.querySelector(\'select[ng-model="vm.creditLostFixProposalReportQuery.proposalStatus"]\').value'
                )
                if current_val == "Success":
                    break
                page.wait_for_timeout(500)

            _fpms_log(13, "点击 Search …")
            page.locator('button:has-text("Search")').first.click()
            page.wait_for_timeout(5000)

            _fpms_log(14, "等待结果区 Total 标签 …")
            total_label = page.locator('label.ng-binding:has-text("Total")').first
            total_label.wait_for(state="visible", timeout=30000)
            total_text = total_label.text_content().strip()

            _fpms_log(15, f"解析结果：{total_text[:120]!r} …")
            if "Total 0 records" in total_text:
                _fpms_log(16, "完成：今日无 Amount Loss 记录")
                return "no amount loss record found for today"
            _fpms_log(16, "完成：今日存在 Amount Loss 记录")
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