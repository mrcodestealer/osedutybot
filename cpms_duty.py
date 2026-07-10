#!/usr/bin/env python3
"""
CPMS Duty Schedule – Next Three Days Duty (calendar-grid format)

Usage:
    ./cpms_duty.py                # shows today, tomorrow, next day duty
    ./cpms_duty.py YYYY-MM-DD     # shows duty for the three days starting from specified date

This script reads a specific Lark sheet containing CPMS duty schedules.
Sheet names are like '08-2026' (MM-YYYY) or '7-2025' (M-YYYY); each sheet is
one month laid out as a **calendar grid**:

    Row 1 : month banner, e.g. "AUGUST 2026"
    Row 2 : day-of-week header  Mon | Tue | Wed | Thu | Fri | Sat | Sun
    Grid  : one cell per calendar day, each cell holding three lines:
                <day-number>
                <main duty name>
                bk: <backup duty name>
            e.g.  "9\\nwailoon\\nbk: kingsley"
    Below : a "Contact List" section mapping name -> phone number, e.g.
                wailoon | phone & whatapp: 60182247838

Duty is therefore keyed by **day-of-month** (not weekday); phone numbers are
resolved by looking each name up in the Contact List.
"""

import re
import sys
import time
import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
load_dotenv()
# ================= Configuration =================

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
SPREADSHEET_TOKEN = os.getenv("CPMS_SPREADSHEET_TOKEN")

# value 可为解析好的 day_map，或缓存的 CpmsSheetNotPublished（负缓存，避免重复请求缺失月份）
_month_duty_cache: dict[tuple[int, int], tuple[float, object]] = {}
try:
    _MONTH_CACHE_SEC = max(0, int((os.getenv("CPMS_DUTY_CACHE_SEC") or "300").strip() or "300"))
except ValueError:
    _MONTH_CACHE_SEC = 300

# 工作表名称形如 "07-2026" 或 "7-2025"（月份可能不补零），匹配 (month, year)。
_SHEET_TITLE_RE = re.compile(r"^\s*(\d{1,2})-(\d{4})\s*$")

# 日历解析用的正则 / 常量
_DOW_TOKENS = {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
_DAYNUM_RE = re.compile(r"^\s*(\d{1,2})\s*$")
# 备份行： "bk: kingsley" / "backup - kh" / "b/u：yc" 等
_BK_RE = re.compile(r"^\s*(?:bk|backup|b/?u|back\s*up)\b\s*[:：\-–—]?\s*(.*)$", re.IGNORECASE)
# 电话号码检测（宽松，用于「这格是否像电话」的判断，可跨空白）
_PHONE_RE = re.compile(r"(\+?\d[\d\s\-]{6,}\d)")
# 电话号码抽取（严格，单个号码 token —— 不跨空格/换行，避免把两个号码粘在一起）
_PHONE_TOKEN_RE = re.compile(r"\+?\d[\d\-]{6,}\d")


class CpmsSheetNotPublished(Exception):
    """当目标月份的工作表尚未创建时抛出，携带最新可用月份用于友好提示。"""

    def __init__(self, year: int, month: int, latest: tuple[int, int] | None = None):
        self.year = year
        self.month = month
        self.latest = latest  # (year, month) 或 None
        super().__init__(
            f"CPMS duty for {datetime(year, month, 1).strftime('%B %Y')} has not been published yet."
        )

    @property
    def friendly_message(self) -> str:
        target_human = datetime(self.year, self.month, 1).strftime("%B %Y")
        target_sheet = f"{self.month:02d}-{self.year}"
        text = (
            f"⚠️ CPMS duty for **{target_human}** hasn't been published yet "
            f"(sheet `{target_sheet}` not found)."
        )
        if self.latest:
            ly, lm = self.latest
            latest_human = datetime(ly, lm, 1).strftime("%B %Y")
            text += f"\nLatest available roster: **{latest_human}** (`{lm:02d}-{ly}`)."
        return text


class CpmsNoPermission(Exception):
    """当 Bot 无权限读取 CPMS 电子表格时抛出，携带友好提示（请 Koo 授权）。"""

    def __init__(self, year: int | None = None, month: int | None = None):
        self.year = year
        self.month = month
        super().__init__("Bot has no permission to view the CPMS sheet.")

    @property
    def friendly_message(self) -> str:
        where = ""
        if self.year and self.month:
            where = f" for **{datetime(self.year, self.month, 1).strftime('%B %Y')}**"
        return (
            f"🔒 Bot has no permission to view the CPMS sheet{where}.\n"
            "Kindly ask **Koo** to provide permission to Bot. "
            "Can request Koo to share to the OSE duty group."
        )


# Lark 权限/禁止访问相关的 error code（尽力枚举）+ 关键词兜底
_PERMISSION_CODES = {403, 91403, 1061045, 1062010, 1069902, 1310213, 1310214, 1310235}
_PERMISSION_KEYWORDS = (
    "permission", "forbidden", "not have access", "no access", "access denied",
    "unauthorized", "无权限", "没有权限", "权限不足", "无权",
)


def _looks_like_permission_error(status_code, result) -> bool:
    """判断一个 Lark API 响应是否为「无权限 / 禁止访问」类错误。"""
    try:
        if status_code == 403:
            return True
    except Exception:
        pass
    if not isinstance(result, dict):
        return False
    if result.get("code") in _PERMISSION_CODES:
        return True
    msg = str(result.get("msg") or result.get("error") or "").lower()
    return any(k in msg for k in _PERMISSION_KEYWORDS)


def _parse_sheet_month(title: str) -> tuple[int, int] | None:
    """把工作表标题解析为 (year, month)；非 ``M-YYYY`` / ``MM-YYYY`` 格式返回 None。"""
    m = _SHEET_TITLE_RE.match(title or "")
    if not m:
        return None
    month = int(m.group(1))
    year = int(m.group(2))
    if 1 <= month <= 12:
        return (year, month)
    return None


def _latest_available_month(sheets) -> tuple[int, int] | None:
    """返回工作表中最新的 (year, month)，用于提示尚未发布时的最近可用月份。"""
    months = []
    for sheet in sheets or []:
        parsed = _parse_sheet_month(sheet.get("title", ""))
        if parsed:
            months.append(parsed)
    return max(months) if months else None

# ================= Helper Functions =================
def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"Failed to get token: {result}")
    return result["tenant_access_token"]

def get_sheet_list(token):
    """获取电子表格下所有工作表的列表"""
    url = f"https://open.larksuite.com/open-apis/sheets/v3/spreadsheets/{SPREADSHEET_TOKEN}/sheets/query"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        if _looks_like_permission_error(resp.status_code, result):
            raise CpmsNoPermission()
        print(f"获取工作表列表失败: {result}")
        return []
    return result.get("data", {}).get("sheets", [])

def get_sheet_id_for_month(token, target_year, target_month, sheets=None):
    """根据年月查找对应的工作表ID。

    工作表名称格式为 ``MM-YYYY`` 或 ``M-YYYY``（月份可能不补零，如 08-2026 / 7-2025），
    两种写法都能匹配。``sheets`` 可传入已获取的工作表列表以避免重复请求。
    """
    if sheets is None:
        sheets = get_sheet_list(token)
    for sheet in sheets:
        if _parse_sheet_month(sheet.get("title", "")) == (target_year, target_month):
            return sheet.get("sheet_id")
    return None

def get_range_values(token, sheet_id, range_str):
    """读取指定范围的值"""
    url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{sheet_id}!{range_str}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    result = resp.json()
    if result.get("code") != 0:
        if _looks_like_permission_error(resp.status_code, result):
            raise CpmsNoPermission()
        print(f"读取范围 {range_str} 失败: {result}")
        return None
    return result.get("data", {}).get("valueRange", {}).get("values", [])

def extract_text_from_cell(cell):
    """将单元格内容（可能为富文本列表）转换为纯文本字符串，保留换行以便解析姓名/电话。"""
    if cell is None:
        return ""
    if isinstance(cell, str):
        return cell.strip()
    if isinstance(cell, list):
        parts = []
        for item in cell:
            if isinstance(item, dict) and "text" in item:
                parts.append(item["text"])
            elif isinstance(item, str):
                parts.append(item)
        return "\n".join(parts).strip()
    return str(cell).strip()


# ================= Calendar-grid parsing =================
def _clean_name(raw: str) -> str:
    """把一格里的姓名清洗成可显示 / 可查电话的形式。

    去除电话尾巴（"wailoon phone & whatapp: ..."）、括号注释（"(leave)"）、
    " - onleave" 之类的破折号后缀，并压缩空白。
    """
    if not raw:
        return ""
    s = str(raw)
    # 砍掉 "phone"/"whatapp"/"whatsapp" 之后的所有内容（姓名不含这些词）
    s = re.split(r"(?i)\b(?:phone|what['\s]?ap+s?|whatsapp)\b", s, maxsplit=1)[0]
    # 去掉括号注释
    s = re.sub(r"\([^)]*\)", "", s)
    # 去掉 " - onleave" / " – off" 等破折号后缀（两侧带空格的破折号）
    s = re.split(r"\s+[-–—]\s+", s, maxsplit=1)[0]
    return re.sub(r"\s+", " ", s).strip()


def _norm_key(name: str) -> str:
    """姓名归一化查找键：小写、压缩空白、去标点（保留字母数字与空格）。"""
    s = _clean_name(name).lower()
    s = re.sub(r"[^\w\s]", "", s)
    return re.sub(r"\s+", " ", s).strip()


def _extract_phone(text: str) -> str:
    """从文本中抽取「第一个」电话号码（返回仅含数字/前导 + 的字符串），无则空串。

    使用不跨空格/换行的 token 正则：若一格里堆了两个号码（如 phone 与 whatsapp 各一行），
    只取第一个，避免被拼接成一个无效的超长号码。
    """
    if not text:
        return ""
    s = str(text)
    first = ""
    for m in _PHONE_TOKEN_RE.finditer(s):
        digits = re.sub(r"[^\d+]", "", m.group(0))
        if not first:
            first = digits
        if len(re.sub(r"\D", "", digits)) >= 8:  # 优先返回位数足够的号码
            return digits
    return first


def _is_day_number(line: str) -> int | None:
    m = _DAYNUM_RE.match(line or "")
    if not m:
        return None
    v = int(m.group(1))
    return v if 1 <= v <= 31 else None


def _plausible_name(s: str) -> bool:
    """粗略判断文本是否像人名——用于堆叠布局兜底，过滤网格下方的杂项注释行。

    人名：非空、不太长（<=24）、不含数字、至少有一个字母（含中文由 isalpha 覆盖）。
    """
    s = (s or "").strip()
    if not s or len(s) > 24:
        return False
    if any(ch.isdigit() for ch in s):
        return False
    return any(ch.isalpha() for ch in s)


def _parse_calendar_sheet(values) -> tuple[dict[int, tuple[str, str]], dict[str, tuple[str, str]]]:
    """解析日历版工作表。

    返回 ``(day_map, contact_map)``：
      * ``day_map``     : {day:int -> (main_name, backup_name)}
      * ``contact_map`` : {norm_key -> (display_name, phone)}
    """
    day_map: dict[int, tuple[str, str]] = {}
    contact_map: dict[str, tuple[str, str]] = {}
    if not values:
        return day_map, contact_map

    n_rows = len(values)

    def cell(r: int, c: int) -> str:
        if 0 <= r < n_rows and 0 <= c < len(values[r]):
            return extract_text_from_cell(values[r][c])
        return ""

    # ---- 1. 定位 "Contact List" 区域 ----
    label_row: int | None = None
    for r in range(n_rows):
        row = values[r] or []
        for c in range(min(len(row), 3)):
            if cell(r, c).strip().lower() == "contact list":
                label_row = r
                break
        if label_row is not None:
            break

    contact_data_start: int | None = (label_row + 1) if label_row is not None else None
    if contact_data_start is None:
        # 没有显式标签：找第一行「B 列像电话且 A 列像姓名」的行
        for r in range(n_rows):
            b = cell(r, 1)
            if _PHONE_RE.search(b) and re.search(r"(?i)phone|what", b) and _clean_name(cell(r, 0)):
                contact_data_start = r
                break

    # 日历区域的下界（不含）：优先 Contact List 标签行，其次联系人数据起始行
    if label_row is not None:
        cal_end = label_row
    elif contact_data_start is not None:
        cal_end = contact_data_start
    else:
        cal_end = n_rows

    # ---- 2. 定位星期表头行，确定日历列 ----
    dow_row: int | None = None
    dow_cols: list[int] = []
    for r in range(cal_end):
        cols = [c for c in range(len(values[r] or [])) if cell(r, c).strip().lower()[:3] in _DOW_TOKENS]
        if len(cols) >= 3:
            dow_row = r
            dow_cols = cols
            break
    cal_cols = dow_cols if dow_cols else list(range(7))
    cal_start = (dow_row + 1) if dow_row is not None else 0

    # ---- 3. 解析日历日期格 ----
    for r in range(cal_start, cal_end):
        for c in cal_cols:
            text = cell(r, c)
            if not text:
                continue
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            if not lines:
                continue
            day: int | None = None
            main = ""
            backup = ""
            leftover: list[str] = []
            for ln in lines:
                dv = _is_day_number(ln)
                if dv is not None and day is None:
                    day = dv
                    continue
                bm = _BK_RE.match(ln)
                if bm and not backup:
                    backup = _clean_name(bm.group(1))
                    continue
                leftover.append(ln)
            if leftover:
                main = _clean_name(leftover[0])

            # 堆叠式布局兜底：本格只有日期数字，主/备在同列紧邻下方的格子里。
            # 遇到空行即停（避免扫到网格下方的杂项/注释），最多看 3 行，且只接受像人名的文本。
            if day is not None and not main and not backup:
                got_main = ""
                got_backup = ""
                rr = r + 1
                steps = 0
                while rr < cal_end and steps < 3:
                    lines2 = [x.strip() for x in cell(rr, c).splitlines() if x.strip()]
                    if not lines2:
                        break  # 空行 = 该日期格结束
                    if _is_day_number(lines2[0]) is not None:
                        break  # 撞到下一个日期格
                    for ln in lines2:
                        bm = _BK_RE.match(ln)
                        if bm and not got_backup:
                            cand = _clean_name(bm.group(1))
                            if _plausible_name(cand):
                                got_backup = cand
                        elif not got_main:
                            cand = _clean_name(ln)
                            if _plausible_name(cand):
                                got_main = cand
                    if got_main and got_backup:
                        break
                    rr += 1
                    steps += 1
                main = main or got_main
                backup = backup or got_backup

            if day is not None and (main or backup):
                day_map[day] = (main, backup)

    # ---- 4. 解析联系人列表 ----
    if contact_data_start is not None:
        for r in range(contact_data_start, n_rows):
            name_raw = cell(r, 0)
            phone_raw = cell(r, 1)
            if not name_raw.strip() and not phone_raw.strip():
                continue
            name = _clean_name(name_raw)
            if not name:
                continue
            phone = _extract_phone(phone_raw) or _extract_phone(name_raw)
            key = _norm_key(name)
            if key and key not in contact_map:
                contact_map[key] = (name, phone)

    return day_map, contact_map


def get_cpms_month_duty(year: int, month: int) -> dict[int, tuple[str, str, str, str]]:
    """
    读取 ``MM-YYYY`` 工作表一次，返回 ``{day -> (main, main_phone, backup, backup_phone)}``。
    结果按 ``CPMS_DUTY_CACHE_SEC``（默认 300 秒）缓存，避免整月/多日查询重复打 Lark API。
    目标月份工作表不存在时抛出 ``CpmsSheetNotPublished``。
    """
    key = (year, month)
    now = time.time()
    if _MONTH_CACHE_SEC > 0:
        cached = _month_duty_cache.get(key)
        if cached and now - cached[0] < _MONTH_CACHE_SEC:
            payload = cached[1]
            if isinstance(payload, CpmsSheetNotPublished):
                raise payload
            return payload

    try:
        token = get_tenant_access_token()
        sheets = get_sheet_list(token)
        sheet_id = get_sheet_id_for_month(token, year, month, sheets=sheets)
        if not sheet_id:
            exc = CpmsSheetNotPublished(year, month, _latest_available_month(sheets))
            if _MONTH_CACHE_SEC > 0:
                _month_duty_cache[key] = (now, exc)
            raise exc

        values = get_range_values(token, sheet_id, "A1:I80")
    except CpmsNoPermission:
        # 无权限：附上月份上下文再抛出；不做负缓存，Koo 授权后下次立即恢复
        raise CpmsNoPermission(year, month)
    if values is None:
        raise Exception("无法读取工作表数据")

    day_map, contact_map = _parse_calendar_sheet(values)
    result: dict[int, tuple[str, str, str, str]] = {}
    for day, (main, backup) in day_map.items():
        main_phone = contact_map.get(_norm_key(main), ("", ""))[1] if main else ""
        backup_phone = contact_map.get(_norm_key(backup), ("", ""))[1] if backup else ""
        result[day] = (main, main_phone, backup, backup_phone)

    if _MONTH_CACHE_SEC > 0:
        _month_duty_cache[key] = (now, result)
    return result


def _cpms_month_human(year: int, month: int) -> str:
    return datetime(year, month, 1).strftime("%B %Y")


def cpms_fallback_notice(target_year: int, target_month: int, src_year: int, src_month: int) -> str:
    """回退提示（保留以兼容调用方；日历版通常不做跨月回退）。"""
    prev_year, prev_month = (target_year, target_month - 1) if target_month > 1 else (target_year - 1, 12)
    rel = "previous month" if (src_year, src_month) == (prev_year, prev_month) else "most recent available"
    return (
        f"⚠️ {_cpms_month_human(target_year, target_month)} CPMS roster hasn't been published yet — "
        f"showing **{_cpms_month_human(src_year, src_month)}** duty ({rel})."
    )


def invalidate_cpms_duty_cache() -> None:
    """清空按月值班缓存（测试或强制刷新时调用）。"""
    _month_duty_cache.clear()


def cpms_check(month=None, year=None):
    """
    检查指定月份（默认为当前月份）中是否有任何日期缺少值班。
    返回字符串，格式与 fpms_check 一致。
    """
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month

    # 计算该月的总天数
    first_day = datetime(year, month, 1).date()
    if month == 12:
        next_month_first = datetime(year + 1, 1, 1).date()
    else:
        next_month_first = datetime(year, month + 1, 1).date()
    days_in_month = (next_month_first - first_day).days

    try:
        day_map = get_cpms_month_duty(year, month)
    except (CpmsSheetNotPublished, CpmsNoPermission) as e:
        return e.friendly_message
    except Exception:
        day_map = {}

    missing = []
    for day in range(1, days_in_month + 1):
        main_name, _, backup_name, _ = day_map.get(day, ("", "", "", ""))
        if not main_name and not backup_name:
            missing.append(day)

    month_name = datetime(year, month, 1).strftime("%B %Y")
    if not missing:
        return f"✅ All days in {month_name} have duty assigned."
    else:
        missing_str = ", ".join(str(d) for d in missing)
        return f"⚠️ {month_name} 缺少值班的日期：{missing_str}"

# ================= Core Logic =================
def get_cpms_duty_for_date(target_date):
    """
    返回指定日期 target_date 的 CPMS 值班信息。
    返回格式: (date_obj, main_name, main_phone, backup_name, backup_phone, fallback)
    ``fallback`` 始终为 None（日历版按日查询，不做跨月回退）；保留该字段以兼容调用方。
    目标月份工作表不存在时抛出 ``CpmsSheetNotPublished``。
    """
    day_map = get_cpms_month_duty(target_date.year, target_date.month)
    main_name, main_phone, backup_name, backup_phone = day_map.get(target_date.day, ("", "", "", ""))
    return target_date, main_name, main_phone, backup_name, backup_phone, None

def get_cpms_three_days(start_date=None):
    """获取连续三天的值班信息"""
    if start_date is None:
        start_date = datetime.now().date()

    results = []
    for i in range(3):
        target = start_date + timedelta(days=i)
        try:
            result = get_cpms_duty_for_date(target)
            results.append(result)
        except (CpmsSheetNotPublished, CpmsNoPermission) as e:
            # 该月未发布 / Bot 无权限：保留异常以便渲染友好提示
            results.append((target, e))
        except Exception as e:
            # 如果某天出错（如临时 API 错误），添加错误信息
            results.append((target, f"Error: {e}"))
    return results


def get_cpms_three_days_text(start_date=None):
    """返回连续三天值班信息的格式化文本。

    当整段日期都落在同一个「未发布 / 无权限」提示时，合并为一条，避免重复三遍。
    """
    results = get_cpms_three_days(start_date)
    notes = [
        it for it in results
        if len(it) == 2 and isinstance(it[1], (CpmsSheetNotPublished, CpmsNoPermission))
    ]
    if notes and len(notes) == len(results):
        msgs = {n[1].friendly_message for n in notes}
        if len(msgs) == 1:
            return notes[0][1].friendly_message
    return format_output(results)


def format_output(results):
    """格式化输出三天的值班信息（纯文本）。"""
    lines = []
    for item in results:
        date_obj = item[0]
        date_str = date_obj.strftime("%B %d, %Y %A")

        # 该月未发布 / Bot 无权限：渲染友好提示后跳过主/备解析
        if len(item) == 2 and isinstance(item[1], (CpmsSheetNotPublished, CpmsNoPermission)):
            lines.append(f"📅 CPMS Schedule - {date_str}")
            lines.append(item[1].friendly_message)
            lines.append("")
            continue

        # 临时错误
        if len(item) == 2 and isinstance(item[1], str):
            lines.append(f"📅 CPMS Schedule - {date_str}")
            lines.append(f"• {item[1]}")
            lines.append("")
            continue

        _, main_name, main_phone, backup_name, backup_phone, _ = item

        lines.append(f"📅 CPMS Schedule - {date_str}")
        if main_name:
            lines.append(f"• {main_name}  📞 {main_phone}")
        else:
            lines.append("• No main duty assigned")

        lines.append("Backup :")
        if backup_name:
            lines.append(f"• {backup_name}  📞 {backup_phone}")
        else:
            lines.append("• No backup duty assigned")

        lines.append("")  # 空行分隔

    return "\n".join(lines)


# ================= Lark card (phone shown as inline chip) =================
def _md_div(content: str) -> dict:
    return {"tag": "div", "text": {"tag": "lark_md", "content": content}}


def _duty_role_line(marker: str, role: str, name: str, phone: str) -> str:
    """一行主/备值班：``🟢 **Main** · wailoon   `📞 60182247838` ``。

    电话号码用行内代码块（inline code）渲染成一个「号码 chip」——展示用、不可点击、无跳转，
    符合用户选择的 “plain number, no button（display only）”。
    """
    if not name:
        return f"{marker} **{role}** · —"
    chip = f"   `📞 {phone}`" if phone else "   `📞 —`"
    return f"{marker} **{role}** · {name}{chip}"


def build_cpms_card(dates) -> dict:
    """为给定日期列表构建 CPMS 值班消息卡片（电话号码以按钮呈现）。

    返回 ``{"text": <纯文本兜底>, "lark_card": <卡片 dict>}``。
    """
    dates = list(dates) or [datetime.now().date()]

    # 三天全落在同一条「未发布 / 无权限」提示时，合并为一条
    resolved: list[tuple] = []
    notices: list[str] = []  # 每天的 friendly_message（仅 not-published / no-permission）
    for d in dates:
        try:
            item = get_cpms_duty_for_date(d)  # (date, main, mph, backup, bph, fallback)
            resolved.append(item)
        except (CpmsSheetNotPublished, CpmsNoPermission) as e:
            resolved.append((d, e))
            notices.append(e.friendly_message)
        except Exception as e:
            resolved.append((d, f"Error: {e}"))

    elements: list[dict] = []
    text_lines: list[str] = []

    if resolved and len(notices) == len(resolved) and len(set(notices)) == 1:
        note = notices[0]
        elements.append(_md_div(note))
        text_lines.append(note)
    else:
        for i, item in enumerate(resolved):
            if i > 0:
                elements.append({"tag": "hr"})
            d = item[0]
            label = d.strftime("%A, %d %b %Y")
            elements.append(_md_div(f"📅 **{label}**"))

            if len(item) == 2 and isinstance(item[1], (CpmsSheetNotPublished, CpmsNoPermission)):
                elements.append(_md_div(item[1].friendly_message))
                text_lines.append(f"{label}\n{item[1].friendly_message}")
                continue
            if len(item) == 2 and isinstance(item[1], str):
                elements.append(_md_div(f"• {item[1]}"))
                text_lines.append(f"{label}\n{item[1]}")
                continue

            _, main_name, main_phone, backup_name, backup_phone, _ = item
            elements.append(_md_div(_duty_role_line("🟢", "Main", main_name, main_phone)))
            elements.append(_md_div(_duty_role_line("🔵", "Backup", backup_name, backup_phone)))

            text_lines.append(
                f"{label}\n🟢 Main: {main_name or '—'}  📞 {main_phone or '—'}\n"
                f"🔵 Backup: {backup_name or '—'}  📞 {backup_phone or '—'}"
            )

    if len(dates) == 1:
        title = f"🟧 CPMS DUTY · {dates[0].strftime('%d/%m/%Y')}"
    else:
        title = (
            f"🟧 CPMS DUTY · {dates[0].strftime('%d/%m')}"
            f"–{dates[-1].strftime('%d/%m/%Y')}"
        )

    card = {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": title},
        },
        "body": {"elements": elements},
    }
    return {"text": "CPMS DUTY\n" + "\n\n".join(text_lines), "lark_card": card}


def get_cpms_payload(start_date=None) -> dict:
    """今天/明天/后天三天的 CPMS 值班卡片（电话号码以按钮呈现）。"""
    if start_date is None:
        start_date = datetime.now().date()
    dates = [start_date + timedelta(days=i) for i in range(3)]
    return build_cpms_card(dates)

# ================= Main =================
if __name__ == "__main__":
    start_date = None
    if len(sys.argv) > 1:
        try:
            start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        except ValueError:
            print("❌ 日期格式错误，请使用 YYYY-MM-DD 格式 (例如 2026-03-20)")
            sys.exit(1)

    try:
        print(get_cpms_three_days_text(start_date))
    except Exception as e:
        print(f"❌ 执行出错: {e}")
        sys.exit(1)
