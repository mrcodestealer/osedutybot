"""
OTP / SMS 相关：自动登录 SMS Gateway CP，进入 Messages，筛选 OTP 并统计 SUCCESS/FAILED

站点: https://sms-web.platform10.me/

用法：
  python otpsmslog.py              # 有界面
  python otpsmslog.py --headless   # 无头
"""
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta

import pyotp
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

LOGIN_URL = "https://sms-web.platform10.me/"
USERNAME = "cpomduty"
PASSWORD = "123456"
TOTP_SECRET = "GYZDEYRSMY4DEZDBMJTGIMRYMQZTAMDEGIZDMNRVHA3TIZBQGBTDSNDFHE4TMODFGAYTENLDGIYDC"

DEFAULT_PLATFORM = "Casino Plus"
DEFAULT_MESSAGE_FILTER = "OTP"
# Message 页多选下拉：Status / Provider Status（与页面选项文案一致）
FILTER_STATUS_OPTION = "All"
FILTER_PROVIDER_STATUS_OPTION = "Failed"
DATE_DISPLAY_FMT = "%m-%d-%Y %H:%M"

NAV_TIMEOUT_MS = 90_000
FIELD_TIMEOUT_MS = 60_000
# 登录后主界面侧栏 React 渲染缓冲（Home / Sms / Messages）
POST_LOGIN_SETTLE_MS = 1_200
MESSAGES_NAV_TIMEOUT_MS = 25_000

CHROMIUM_ARGS = ["--disable-blink-features=AutomationControlled"]


def _status_or_provider_is_failed(st: str, pv: str) -> bool:
    s = (st or "").strip().upper()
    p = (pv or "").strip().upper()
    return s == "FAILED" or p == "FAILED"


def _fill_otp_field(page, code: str):
    """智能填充 OTP 输入框（支持单框、6格、React 受控组件）"""
    assert len(code) == 6 and code.isdigit(), "TOTP 应为 6 位数字"
    page.wait_for_timeout(400)

    # 六个单格
    one_char = page.locator("input[maxlength='1']")
    if one_char.count() >= 6:
        print("→ 检测到 6 个单格输入框，逐个填充")
        for i, ch in enumerate(code):
            cell = one_char.nth(i)
            cell.click()
            cell.fill(ch)
            cell.evaluate(
                """(el, val) => {
                    el.value = val;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                ch
            )
        print("→ 已写入 6 格 OTP")
        return

    # 单框
    otp_selectors = [
        "#OTP", "#otp", "#code", "#verificationCode",
        "input[name='otp']", "input[name='code']", "input[name='OTP']",
        "input[formcontrolname='otp']", "input[formcontrolname='code']",
        "input[autocomplete='one-time-code']",
        "input[placeholder*='OTP' i]", "input[placeholder*='Code' i]",
        "input[type='tel']"
    ]
    otp_loc = None
    for sel in otp_selectors:
        cand = page.locator(sel)
        if cand.count() < 1:
            continue
        first = cand.first
        try:
            if first.is_visible():
                otp_loc = first
                break
        except Exception:
            continue

    if otp_loc is None:
        raise RuntimeError("未找到 OTP 输入框，请检查页面元素")

    otp_loc.wait_for(state="visible", timeout=FIELD_TIMEOUT_MS)
    otp_loc.scroll_into_view_if_needed()
    otp_loc.click()
    otp_loc.fill("")
    otp_loc.fill(code, force=True)

    otp_loc.evaluate(
        """(el, val) => {
            const proto = window.HTMLInputElement.prototype;
            const desc = Object.getOwnPropertyDescriptor(proto, 'value');
            if (desc && desc.set) desc.set.call(el, val);
            else el.value = val;
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
            try { el.dispatchEvent(new InputEvent('input', { bubbles: true, data: val })); } catch (e) {}
        }""",
        code
    )
    print("→ 已填充单框 OTP 并触发事件")


def _raise_if_cloudflare_blocked(page):
    """检测 Cloudflare 拦截页"""
    try:
        title = (page.title() or "").lower()
    except:
        title = ""
    try:
        html = (page.content() or "")[:100000].lower()
    except:
        html = ""
    if (
        "sorry, you have been blocked" in html
        or "cf-error-details" in html
        or ("cloudflare" in title and "attention required" in title)
    ):
        raise RuntimeError(
            "【Cloudflare 拦截】当前出口 IP 被拒绝。"
            "请将本机公网 IP 加入 Cloudflare 白名单后重试。"
        )


def _submit_otp(page):
    """提交 OTP：优先 OTP 所在表单的按钮；再 Enter / requestSubmit。"""
    error_selectors = ".alert, .error, .text-danger, [class*=error], .invalid-feedback"
    for el in page.locator(error_selectors).all():
        try:
            if el.is_visible():
                print(f"⚠️ 页面提示: {el.text_content()}")
        except Exception:
            pass

    # 1) 焦点在 OTP 框上按 Enter（很多站点第二步靠回车提交）
    print("→ 在 OTP 输入框按 Enter 尝试提交")
    for sel in ("#OTP", "#otp", "input[name='otp']", "input[name='code']", "input[autocomplete='one-time-code']"):
        if page.locator(sel).count() < 1:
            continue
        try:
            loc = page.locator(sel).first
            loc.focus()
            page.keyboard.press("Enter")
            page.wait_for_timeout(800)
            break
        except Exception:
            pass

    # 2) OTP 所在 form 内提交；若无 form（SPA）则在祖先节点里找按钮
    clicked = page.evaluate(
        """() => {
            const otp = document.querySelector(
                '#OTP, #otp, input[name="otp"], input[name="code"], input[autocomplete="one-time-code"]'
            );
            if (!otp) return false;
            const tryClick = (el) => {
                if (el && !el.disabled && el.offsetParent !== null) {
                    el.click();
                    return true;
                }
                return false;
            };
            let form = otp.closest('form');
            if (form) {
                let btn = form.querySelector('button[type="submit"], input[type="submit"]');
                if (tryClick(btn)) return true;
                const buttons = form.querySelectorAll('button');
                for (const b of buttons) {
                    const t = (b.textContent || '').trim();
                    if (/login|verify|submit|confirm|登录|验证/i.test(t) && tryClick(b)) return true;
                }
                if (typeof form.requestSubmit === 'function') {
                    try { form.requestSubmit(); return true; } catch (e) {}
                }
            }
            let node = otp.parentElement;
            for (let d = 0; d < 12 && node; d++) {
                const btn = node.querySelector(
                    'button[type="submit"], button.btn-primary, button.btn-success, input[type="submit"]'
                );
                if (tryClick(btn)) return true;
                node = node.parentElement;
            }
            return false;
        }"""
    )
    if clicked:
        print("→ 已通过 OTP 所在表单提交（click / requestSubmit）")
        return

    # 3) 全局可见的提交按钮（遍历多枚，避免点到隐藏层）
    submit_selectors = [
        'button:has-text("Verify")',
        'button:has-text("Submit")',
        'button:has-text("Confirm")',
        'button:has-text("Login")',
        'button:has-text("登录")',
        'input[type="submit"]',
        'button[type="submit"]',
    ]
    for sel in submit_selectors:
        loc = page.locator(sel)
        try:
            n = loc.count()
        except Exception:
            continue
        for i in range(n):
            btn = loc.nth(i)
            try:
                if not btn.is_visible():
                    continue
                btn.scroll_into_view_if_needed()
                btn.click(timeout=5000)
                print(f"→ 已点击: {sel} (#{i})")
                return
            except Exception:
                continue

    print("→ 再次按 Enter（兜底）")
    page.keyboard.press("Enter")


def _messages_sidebar_link(page):
    loc = page.locator('a:has(span.side-bar-nav-name:has-text("Messages"))')
    if loc.count() < 1:
        loc = page.locator('a:has(i[title="Messages"])')
    return loc


def _home_sidebar_link(page):
    loc = page.locator('a:has(span.side-bar-nav-name:has-text("Home"))')
    if loc.count() < 1:
        loc = page.locator('a:has(i[title="Home"])')
    return loc


def _sms_sidebar_toggle(page):
    return page.locator("div.nav-title-toggle").filter(
        has=page.locator("span.side-bar-nav-name", has_text="Sms")
    )


def _sms_submenu_is_expanded(page) -> bool:
    """Sms 下折叠区是否已展开（Bootstrap collapse 常见 class: show / in）。已展开则不要再点 Sms，否则会收起。"""
    row = page.locator("div.nav-by-role").filter(
        has=page.locator("span.side-bar-nav-name", has_text="Sms")
    ).first
    try:
        if row.count() < 1:
            return False
        collapse = row.locator("div.collapse").first
        if collapse.count() < 1:
            return False
        cls = (collapse.get_attribute("class") or "").lower()
        parts = cls.split()
        return "show" in parts or "in" in parts
    except Exception:
        return False


def _scroll_into_view_dom(locator, timeout_ms: int = 5_000) -> None:
    """
    用 DOM scrollIntoView，不要求元素 Playwright 意义下 visible（避免 scroll_into_view_if_needed 超时）。
    """
    try:
        locator.evaluate(
            """el => {
                try {
                    el.scrollIntoView({ block: 'nearest', inline: 'nearest' });
                } catch (_) {}
            }""",
            timeout=timeout_ms,
        )
    except Exception:
        pass


def _goto_messages_page(page):
    """
    侧栏顺序（与产品一致）：先点 Home，再点 Sms 展开子菜单，才会出现 Messages，最后点 Messages。
    不在 Home 后检查 Messages 是否 visible（折叠里 is_visible 常为 False）。
    """
    page.wait_for_selector(".side-bar", state="visible", timeout=MESSAGES_NAV_TIMEOUT_MS)
    page.wait_for_timeout(POST_LOGIN_SETTLE_MS)

    home = _home_sidebar_link(page)
    if home.count() > 0:
        print("→ 点击 Home")
        h = home.first
        h.wait_for(state="visible", timeout=12_000)
        h.scroll_into_view_if_needed()
        try:
            h.click(timeout=10_000)
        except Exception:
            h.click(force=True, timeout=10_000)
        page.wait_for_timeout(max(POST_LOGIN_SETTLE_MS, 700))
    else:
        print("⚠️ 未找到 Home，直接 Sms → Messages")

    sms = _sms_sidebar_toggle(page)
    if sms.count() < 1:
        raise RuntimeError("未找到侧栏 Sms（nav-title-toggle）")

    if _sms_submenu_is_expanded(page):
        print("→ Sms 子菜单已是展开状态，不再点击 Sms（避免再点一次被收起）")
    else:
        print("→ 点击 Sms 展开子菜单（其后才会出现 Messages）")
        _scroll_into_view_dom(sms.first)
        try:
            sms.first.click(timeout=8000)
        except Exception:
            sms.first.click(force=True, timeout=8000)
        page.wait_for_timeout(800)

    msg_link = _messages_sidebar_link(page)
    if msg_link.count() < 1:
        print("⚠️ 点过一次 Sms 后仍无 Messages 节点，再点一次 Sms（若上次未展开成功）")
        _scroll_into_view_dom(sms.first)
        try:
            sms.first.click(timeout=8000)
        except Exception:
            sms.first.click(force=True, timeout=8000)
        page.wait_for_timeout(800)
        msg_link = _messages_sidebar_link(page)
    if msg_link.count() < 1:
        raise RuntimeError("未找到 Messages 链接（请确认 Sms 已展开）")
    print("→ 点击 Messages")
    msg_link.first.wait_for(state="attached", timeout=MESSAGES_NAV_TIMEOUT_MS)
    _scroll_into_view_dom(msg_link.first)
    try:
        msg_link.first.click(timeout=12_000, force=True)
    except Exception:
        msg_link.first.click(force=True, timeout=12_000)

    page.wait_for_selector('h1.page-title:has-text("Message")', timeout=MESSAGES_NAV_TIMEOUT_MS)


def _multiselect_pick_option(page, want: str):
    """
    在已打开的多选下拉中，勾选与 want 文案匹配的一项（不区分大小写，整行文案 norm 后相等）。
    返回非空字符串表示成功方式；失败返回 None。
    """
    want = (want or "").strip()
    if not want:
        return None
    wi = want.lower()

    picked = page.evaluate(
        """(want) => {
            const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
            const w = norm(want);
            const wi = w.toLowerCase();
            if (!w) return null;

            const visible = (el) => {
                if (!el || el.disabled) return false;
                const r = el.getBoundingClientRect();
                const st = window.getComputedStyle(el);
                return r.width >= 1 && r.height >= 1 && st.visibility !== 'hidden' && st.display !== 'none';
            };

            const boxes = document.querySelectorAll('input[type="checkbox"]');
            for (const cb of boxes) {
                if (!visible(cb)) continue;
                let row = cb.closest('label');
                if (!row) row = cb.closest('li, [role="option"], div');
                if (!row || !visible(row)) continue;
                const t = norm(row.textContent);
                if (t.toLowerCase() === wi) {
                    cb.click();
                    return 'checkbox-label';
                }
            }

            const nodes = document.querySelectorAll('label, li, [role="option"], div, span');
            for (const el of nodes) {
                if (!visible(el)) continue;
                const t = norm(el.textContent);
                if (t.toLowerCase() !== wi) continue;
                const r = el.getBoundingClientRect();
                if (r.width < 2 || r.height < 2) continue;
                const cb = el.querySelector('input[type="checkbox"]');
                if (cb && visible(cb)) {
                    cb.click();
                    return 'checkbox-inner';
                }
                el.click();
                return 'click-text';
            }
            return null;
        }""",
        want,
    )

    if picked is None:
        try:
            page.get_by_text(want, exact=True).last.click(timeout=6000)
            return "get_by_text"
        except Exception:
            pass
        try:
            page.get_by_text(re.compile("^" + re.escape(want) + "$", re.I)).last.click(timeout=4000)
            return "get_by_text_re"
        except Exception:
            pass
    return picked


def _find_form_creatable_trigger(page, field_key: str):
    """
    在 table-filter 表单内找 Status / Provider Status 的 creatable 输入框。
    field_key: 'status' | 'provider_status'
    """
    form = page.locator("form.table-filter-container")
    items = form.locator("div.table-filter-item")
    try:
        n = items.count()
    except Exception:
        n = 0
    for i in range(n):
        item = items.nth(i)
        lab = item.locator("span.label").first
        if lab.count() == 0:
            continue
        raw = (lab.inner_text() or "").strip()
        norm = " ".join(raw.split()).lower()
        if field_key == "status":
            if norm == "status":
                inp = item.locator(
                    "input[type='text'], input.custom-form-input, input.parent-input"
                ).first
                if inp.count():
                    return inp
        elif field_key == "provider_status":
            if "provider" in norm and "status" in norm:
                inp = item.locator(
                    "input[type='text'], input.custom-form-input, input.parent-input"
                ).first
                if inp.count():
                    return inp

    placeholders = {
        "status": ["Status"],
        "provider_status": ["Provider Status", "Provider status"],
    }.get(field_key, [])
    for ph in placeholders:
        loc = form.locator(f'input[placeholder="{ph}"]')
        if loc.count() > 0:
            return loc.first
    return None


def _fill_multiselect_filter(page, field_key: str, option_text: str, label_for_log: str):
    trigger = _find_form_creatable_trigger(page, field_key)
    if trigger is None:
        print(f"⚠️ 未找到「{label_for_log}」筛选输入，跳过")
        return
    opt = (option_text or "").strip()
    trigger.wait_for(state="visible", timeout=15_000)
    trigger.scroll_into_view_if_needed()
    trigger.click()
    page.wait_for_timeout(450)
    picked = _multiselect_pick_option(page, opt)
    if picked is None:
        raise RuntimeError(f'未在下拉中选中「{label_for_log}」= {opt!r}，请核对页面选项文案。')
    print(f"→ {label_for_log} 已选: {opt!r}（{picked}）")
    page.wait_for_timeout(200)
    page.keyboard.press("Escape")
    page.wait_for_timeout(200)


def _fill_platform(page, text: str):
    """
    Platform 为多选下拉（复选框列表），不能只往 input 里填字。
    点击输入打开菜单 → 勾选与文案完全一致的项（如 Casino Plus）→ ESC 收起。
    """
    form = page.locator("form.table-filter-container")
    candidates = [
        form.locator('input[placeholder="Platform"]'),
        page.locator(".table-filter").locator('input[placeholder="Platform"]'),
        page.locator(
            'div.creatable-input:has(span.label:has-text("Platform")) input[placeholder="Platform"]'
        ),
    ]
    plat = None
    for loc in candidates:
        try:
            if loc.count() > 0:
                plat = loc.first
                break
        except Exception:
            continue
    if plat is None:
        raise RuntimeError("未找到 Platform 输入框（placeholder=Platform）")

    want = (text or "").strip()
    plat.wait_for(state="visible", timeout=15_000)
    plat.scroll_into_view_if_needed()
    plat.click()
    page.wait_for_timeout(500)

    picked = _multiselect_pick_option(page, want)
    if picked is None:
        raise RuntimeError(
            f'未在下拉列表中选中 Platform「{want}」。请确认 DEFAULT_PLATFORM 与页面选项完全一致（含空格）。'
        )

    print(f"→ Platform 已勾选: {want!r}（{picked}）")
    page.wait_for_timeout(250)
    page.keyboard.press("Escape")
    page.wait_for_timeout(250)


def _fill_date_range_mmddyyyy_hhmm(page, start_dt: datetime, end_dt: datetime):
    """Date from / Date to：MM-DD-YYYY HH:MM（与站点展示一致）。"""
    from_str = start_dt.strftime(DATE_DISPLAY_FMT)
    to_str = end_dt.strftime(DATE_DISPLAY_FMT)

    for label, val in (("Date from", from_str), ("Date to", to_str)):
        group = page.locator("div.k2-form-group").filter(
            has=page.locator("span.label", has_text=label)
        )
        inp = group.locator(".custom-datepicker input, .react-datepicker-wrapper input, input").first
        inp.wait_for(state="visible", timeout=15_000)
        inp.click()
        inp.fill("")
        inp.fill(val)
        inp.press("Tab")
        page.wait_for_timeout(200)


def _fill_message_filter(page, text: str):
    """Message 输入框（#textmessage）。"""
    msg = page.locator("#textmessage, input[name='message'][placeholder='Message']").first
    msg.wait_for(state="visible", timeout=15_000)
    msg.click()
    msg.fill("")
    msg.fill(text)


def _set_rows_per_page_max(page):
    """分页下拉选最大条数（如 1000/page），便于一页看清。"""
    pag = None
    for sel in (
        page.locator(".pagination-main-wrapper select"),
        page.locator(".pagination select"),
    ):
        try:
            if sel.count() > 0:
                pag = sel.first
                break
        except Exception:
            continue
    if pag is None:
        return
    try:
        opts = pag.locator("option")
        n_opt = opts.count()
        best_val = None
        best_n = -1
        for i in range(n_opt):
            v = opts.nth(i).get_attribute("value") or ""
            try:
                n = int(v)
            except ValueError:
                continue
            if n > best_n:
                best_n = n
                best_val = v
        if best_val:
            pag.select_option(value=best_val)
            print(f"→ 分页设为 {best_val} / page")
    except Exception as e:
        print(f"⚠️ 设置分页条数跳过: {e}")


def _click_search_messages(page):
    btn = page.locator('button[type="submit"].k2button.primary').filter(has_text="Search")
    if btn.count() < 1:
        btn = page.locator('form.table-filter-container button[type="submit"]').filter(
            has_text="Search"
        )
    if btn.count() < 1:
        btn = page.get_by_role("button", name="Search")
    btn.first.wait_for(state="visible", timeout=15_000)
    btn.first.click()


def _parse_otp_table(page):
    """
    解析 Message 表格：Player ID、Status、Provider Status、Time。

    n>=9（含 10 列）：MessageId(0), Platform(1), Provider(2), **Player ID(3)**, Message(4),
    Status(5), Provider Status(6), Length(7), Time(8)…
    **Player ID 只读第 4 列（下标 3）**；空或占位符 @ / - 则为 **N/A**，绝不把 MessageId 当 Player ID。
    n==8 旧表无 Player 列时 Player ID 一律 N/A。虚拟滚动仍合并多屏行。
    """
    rows = page.evaluate(
        r"""() => {
            const table = document.querySelector('.k2table-group .k2table');
            if (!table) return [];

            const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
            const NA = 'N/A';
            const cellPlayerRaw = (txt) => {
                const t = norm(txt);
                if (!t) return '';
                const low = t.toLowerCase();
                if (t === '@' || t === '-' || t === '—' || low === 'n/a') return '';
                return t;
            };

            const layoutFor = (n) => {
                if (n >= 9) return { msg: 0, st: 5, pv: 6, tm: 8, ply: 3 };
                if (n >= 8) return { msg: 0, st: 4, pv: 5, tm: 7, ply: -1 };
                return {
                    msg: 0,
                    st: Math.min(5, Math.max(0, n - 3)),
                    pv: Math.min(6, Math.max(0, n - 2)),
                    tm: Math.max(0, n - 1),
                    ply: -1,
                };
            };

            const tbody = table.querySelector('.tbody');
            const scroller = (tbody && tbody.parentElement) || table;
            const byKey = new Map();

            const parseVisible = () => {
                const dataRows = table.querySelectorAll('.tbody .tr');
                for (const row of dataRows) {
                    const cells = row.querySelectorAll('.td[role="cell"]');
                    const n = cells.length;
                    if (n < 6) continue;
                    const L = layoutFor(n);
                    const need = Math.max(L.msg, L.st, L.pv, L.tm, L.ply >= 0 ? L.ply : 0);
                    if (n <= need) continue;

                    const msgId = norm(cells[L.msg].textContent);
                    const st = norm(cells[L.st].textContent);
                    const pv = norm(cells[L.pv].textContent);
                    const tm = n > L.tm ? norm(cells[L.tm].textContent) : '';

                    let displayPlayer = NA;
                    if (n >= 9 && n > 3) {
                        const fromCol3 = cellPlayerRaw(cells[3].textContent);
                        displayPlayer = fromCol3 || NA;
                    } else if (L.ply >= 0 && n > L.ply) {
                        const v = cellPlayerRaw(cells[L.ply].textContent);
                        displayPlayer = v || NA;
                    }

                    if (!st && !pv && !msgId) continue;
                    const key = msgId || (displayPlayer + '|' + tm + '|' + st + '|' + pv);
                    byKey.set(key, [displayPlayer, st, pv, tm]);
                }
            };

            let prevTop = -1;
            let stall = 0;
            for (let iter = 0; iter < 200; iter++) {
                parseVisible();
                const sh = scroller.scrollHeight;
                const ch = scroller.clientHeight || 1;
                const stp = scroller.scrollTop;
                const atBottom = stp + ch >= sh - 3;
                const nextTop = atBottom ? 0 : Math.min(stp + Math.max(80, Math.floor(ch * 0.88)), sh);
                scroller.scrollTop = nextTop;
                if (scroller.scrollTop === prevTop) {
                    stall++;
                    if (stall >= 6) break;
                } else {
                    stall = 0;
                    prevTop = scroller.scrollTop;
                }
                if (atBottom && scroller.scrollTop === 0) {
                    parseVisible();
                    break;
                }
            }
            parseVisible();
            try {
                scroller.scrollTop = 0;
            } catch (e) {}

            return Array.from(byKey.values());
        }"""
    )

    rows = rows or []
    rows.sort(key=lambda x: (x[3] if len(x) > 3 else ""), reverse=True)

    counter: Counter = Counter()
    for item in rows:
        if len(item) < 3:
            continue
        pid, st, pv = item[0], item[1], item[2]
        counter[(st, pv)] += 1
    return counter, rows


def format_otp_log_summary(counter: Counter, detail_rows=None) -> str:
    """
    Lark / bot output: English only. FAILED rows grouped by Player ID with time range and count.
    """
    lines = ["As checked OTP logs:"]
    if not counter:
        lines.append("Status: (no rows), Provider Status: (no rows), Counts: 0")
    else:
        for (st, pv), n in sorted(counter.items(), key=lambda x: (x[0][0], x[0][1])):
            lines.append(f"Status: {st}, Provider Status: {pv}, Counts: {n}")

    failed_rows = []
    if detail_rows:
        for item in detail_rows:
            if len(item) < 3:
                continue
            pid, st, pv = item[0], item[1], item[2]
            tm = item[3] if len(item) > 3 else ""
            if _status_or_provider_is_failed(st, pv):
                failed_rows.append((pid, st, pv, tm))

    lines.append("")
    lines.append("Player ID (FAILED only):")
    if not failed_rows:
        lines.append("(No FAILED records)")
    else:
        by_player = defaultdict(list)
        for pid, st, pv, tm in failed_rows:
            key = (pid or "").strip() or "(no player id)"
            by_player[key].append((st, pv, tm))

        for pid_key in sorted(
            by_player.keys(),
            key=lambda k: (k == "(no player id)", k.upper() == "N/A", k),
        ):
            entries = by_player[pid_key]
            st0, pv0, _ = entries[0]
            times_all = [(e[2] or "").strip() for e in entries if (e[2] or "").strip()]
            cnt = len(entries)
            disp = "N/A" if pid_key in ("(no player id)", "N/A") else pid_key
            if not times_all:
                time_str = "Time range: —"
            elif len(times_all) == 1:
                t0 = times_all[0]
                time_str = f"Time range: from {t0} to {t0}"
            else:
                t_min = min(times_all)
                t_max = max(times_all)
                time_str = f"Time range: from {t_min} to {t_max}"
            lines.append(
                f"Player ID: {disp}, Status: {st0}, Provider Status: {pv0}, "
                f"{time_str}, Count: {cnt}"
            )
    return "\n".join(lines)


def run_otp_login(headless=False):
    """登录 SMS 网关，进入 Messages，按条件查询 OTP 并返回统计文案。"""
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            slow_mo=80 if not headless else 0,
            args=CHROMIUM_ARGS,
        )
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        # 监听网络请求（用于调试 OTP 验证接口）
        def log_request(request):
            if "api" in request.url or "auth" in request.url or "otp" in request.url.lower():
                print(f"🌐 请求: {request.method} {request.url}")
        def log_response(response):
            if "api" in response.url or "auth" in response.url or "otp" in response.url.lower():
                print(f"🌐 响应: {response.status} {response.url}")
        page.on("request", log_request)
        page.on("response", log_response)

        try:
            # 1. 打开登录页
            print(f"→ 打开登录页 {LOGIN_URL}")
            page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            _raise_if_cloudflare_blocked(page)

            # 先等用户名框出现再填表；networkidle 可能拖很久，只作短超时以免迟迟不填 username
            print("→ 等待登录表单（用户名框）出现…")
            page.wait_for_selector(
                "#username, input[name='username'], input[formcontrolname='username']",
                state="visible",
                timeout=FIELD_TIMEOUT_MS,
            )
            try:
                page.wait_for_load_state("networkidle", timeout=8_000)
            except PlaywrightTimeout:
                pass

            # 2. 填写用户名密码
            print("→ 填写用户名 / 密码")
            user_loc = page.locator(
                "#username, input[name='username'], input[formcontrolname='username']"
            ).first
            user_loc.wait_for(state="visible", timeout=min(15_000, FIELD_TIMEOUT_MS))
            user_loc.fill(USERNAME)
            page.locator("#password, input[name='password'], input[type='password']").first.fill(
                PASSWORD
            )
            page.locator('input[type="submit"], button[type="submit"]').first.click()

            # 3. 等待 OTP 输入框出现
            print("→ 等待 OTP 输入框出现")
            page.wait_for_selector(
                "#OTP, #otp, #code, input[name='otp'], input[name='code'], input[maxlength='1'], "
                "input[autocomplete='one-time-code']",
                state="visible",
                timeout=FIELD_TIMEOUT_MS,
            )

            # 4. 填入 TOTP
            code = pyotp.TOTP(TOTP_SECRET).now()
            print(f"→ 填入 Google Authenticator 动态码: {code}")
            _fill_otp_field(page, code)

            # 等待页面反应
            page.wait_for_timeout(1000)

            # 检查是否自动跳转
            if page.locator(".side-bar").count() > 0:
                print("→ 已自动跳转至主界面，跳过 OTP 提交步骤")
            else:
                print("→ 提交 OTP 验证")
                _submit_otp(page)

                # 等待登录成功
                print("→ 等待主界面加载...")
                try:
                    page.wait_for_selector(".side-bar", state="visible", timeout=MESSAGES_NAV_TIMEOUT_MS)
                except PlaywrightTimeout:
                    # 保存诊断信息
                    print("\n--- 登录失败诊断 ---")
                    print(f"当前 URL: {page.url}")
                    # 查找错误提示
                    error_msg = ""
                    for sel in [".alert", ".error", ".text-danger", "[class*=error]"]:
                        el = page.locator(sel).first
                        if el.count() > 0 and el.is_visible():
                            error_msg = el.text_content()
                            print(f"页面错误提示: {error_msg}")
                            break
                    # 保存截图和 HTML
                    page.screenshot(path="login_failed.png")
                    with open("login_failed.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    print("已保存截图: login_failed.png 和页面源码: login_failed.html")
                    raise RuntimeError(
                        f"登录失败：未进入主界面。请检查截图和 HTML 文件。\n"
                        f"当前 URL: {page.url}\n错误信息: {error_msg}"
                    )

            print("✅ 已进入主界面")

            print("→ 导航：Home → Sms → Messages")
            _goto_messages_page(page)
            page.wait_for_selector(".main-content-wrap", state="visible", timeout=15_000)

            now = datetime.now()
            date_from = now - timedelta(hours=1)
            print(
                f"→ 筛选 Platform={DEFAULT_PLATFORM!r}，Status={FILTER_STATUS_OPTION!r}，"
                f"Provider Status={FILTER_PROVIDER_STATUS_OPTION!r}，"
                f"Date from={date_from.strftime(DATE_DISPLAY_FMT)}，"
                f"Date to={now.strftime(DATE_DISPLAY_FMT)}，Message={DEFAULT_MESSAGE_FILTER!r}"
            )
            _fill_platform(page, DEFAULT_PLATFORM)
            _fill_multiselect_filter(page, "status", FILTER_STATUS_OPTION, "Status")
            _fill_multiselect_filter(
                page, "provider_status", FILTER_PROVIDER_STATUS_OPTION, "Provider Status"
            )
            _fill_date_range_mmddyyyy_hhmm(page, date_from, now)
            _fill_message_filter(page, DEFAULT_MESSAGE_FILTER)

            print("→ 点击 Search")
            _click_search_messages(page)

            page.wait_for_timeout(2000)
            _set_rows_per_page_max(page)
            print("→ 等待 3 秒加载表格…")
            page.wait_for_timeout(3000)

            try:
                page.wait_for_selector(".k2table-group .k2table .tbody .tr", timeout=20_000)
            except PlaywrightTimeout:
                print("⚠️ 未检测到数据行，仍尝试统计…")

            counter, detail_rows = _parse_otp_table(page)
            summary = format_otp_log_summary(counter, detail_rows=detail_rows)
            print(summary)

            final_url = page.url
            print(f"✅ 完成，当前 URL: {final_url}")
            return summary

        except Exception as e:
            print(f"❌ 错误: {e}")
            try:
                page.screenshot(path="error_otpsmslog.png")
                print("已保存截图: error_otpsmslog.png")
            except:
                pass
            raise
        finally:
            context.close()
            browser.close()

if __name__ == "__main__":
    headless = "--headless" in sys.argv
    out = run_otp_login(headless=headless)
    print("\n===== 输出 =====\n")
    print(out)