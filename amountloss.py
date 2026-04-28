"""
FPMS Amount Loss 查询（供 main.py /al 调用）

用法:
  python3 amountloss.py
  python3 amountloss.py -headless
  python3 amountloss.py --getdata          # 查询后拉满每页 1000 条并打印明细表
  python3 amountloss.py --getdata 16/04
  python3 amountloss.py --filterdata       # 同上拉表后：只保留 In/Out Live 备注，30 分钟窗口去重，拆 Transfer ID，In 时间升序再接 Out 时间升序
  python3 amountloss.py --checklog          # 同上 + SLS Error log 列；凭证：控制台 STS 三件套，或 RAM 用户 AK + AssumeRole（见下方常量区注释）；需 pip install aliyun-log-python-sdk（AssumeRole 另需 aliyun-python-sdk-sts）
"""
import json
import os
import platform
import pyotp
import re
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

LOGIN_URL = "https://mgnt-webserver.casinoplus.top/"
USERNAME = "CPOM01"
PASSWORD = "8c0fa1"
TOTP_SECRET = "MNYG63JQGEYTMOJTHE4DMMBTGQYDIOI"
TABLE_SELECTOR = "#creditLostFixSummaryTable tbody tr"
REPORT_URL = "https://mgnt-webserver.casinoplus.top/report"

MENU_TIMEOUT_MS = 60_000
CHROMIUM_ARGS = ["--disable-blink-features=AutomationControlled"]
if platform.system() == "Linux":
    CHROMIUM_ARGS.append("--disable-dev-shm-usage")

DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# SLS（与控制台 URL 中 project/logstore/region 一致；凭证只用环境变量）
#
# 方式 A — 控制台「获取临时凭证」三件套：ALIYUN_SLS_ACCESS_KEY_ID / _SECRET / _SECURITY_TOKEN
#
# 方式 B — AssumeRole（ECS 常见）：实例 RAM 角色 → STS AssumeRole → 目标账号下有 SLS 读权限的角色。
#   目标角色（读日志）：acs:ram::5754739415144793:role/sls-platform-readonly
#     写 ALIYUN_SLS_ASSUME_ROLE_ARN=上述 ARN
#     或 ALIYUN_SLS_ASSUME_TARGET_ACCOUNT_ID=5754739415144793 + ALIYUN_SLS_ASSUME_TARGET_ROLE_NAME=sls-platform-readonly
#   调用方（在 ECS 上）：OSE-ECS-Read-Monitor-sls 的临时 AK（走元数据，无需长期密钥）
#     ALIYUN_ECS_RAM_ROLE_NAME=OSE-ECS-Read-Monitor-sls
#   调用方（本机调试）：RAM 用户长期 AK
#     ALIYUN_ASSUME_ACCESS_KEY_ID / ALIYUN_ASSUME_ACCESS_KEY_SECRET（或 ALIYUN_ACCESS_KEY_*）
#   可选：ALIYUN_STS_REGION（默认 cn-hangzhou）、ALIYUN_ASSUME_ROLE_SESSION_NAME、ALIYUN_ASSUME_ROLE_EXTERNAL_ID
SLS_DEFAULT_ENDPOINT = "ap-southeast-1.log.aliyuncs.com"
SLS_DEFAULT_PROJECT = "platform-prod-aliyun-logs"
SLS_DEFAULT_LOGSTORE = "platform-fpms-prod"
# Start Time 前后各 4 分钟，与控制台示例 Apr 15 05:14:01 ~ 05:22:01（中心 05:18:01）一致
SLS_WINDOW_MINUTES = 4


def _load_plain_env_file(path: str) -> None:
    """无 python-dotenv 时解析 KEY=value（支持引号），仅当环境变量尚未设置时写入。"""
    if not path or not os.path.isfile(path):
        return
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip()
                if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
                    val = val[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = val
    except OSError:
        pass


def _load_sls_env_files() -> None:
    """
    依次加载（后者不覆盖已有环境变量）：
    amountloss 同目录 amountloss.sls.env、.env；当前工作目录 .env、amountloss.sls.env。
    若已安装 python-dotenv，用其解析；否则对上述路径尝试简易 KEY=value 解析。
    """
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, "amountloss.sls.env"),
        os.path.join(here, ".env"),
        os.path.join(os.getcwd(), ".env"),
        os.path.join(os.getcwd(), "amountloss.sls.env"),
    ]
    try:
        from dotenv import load_dotenv

        for p in candidates:
            if os.path.isfile(p):
                load_dotenv(p, override=False)
    except ImportError:
        for p in candidates:
            _load_plain_env_file(p)


def _env_first(*names: str) -> str:
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return ""


def _sls_assume_role_arn() -> str:
    """若配置了 AssumeRole，返回要扮演的 RAM Role ARN（一般为跨账号 sls-platform-readonly）；否则空字符串。"""
    arn = _env_first("ALIYUN_SLS_ASSUME_ROLE_ARN", "ALIYUN_ASSUME_ROLE_ARN")
    if arn:
        return arn
    account = _env_first(
        "ALIYUN_SLS_ASSUME_TARGET_ACCOUNT_ID",
        "ALIYUN_RAM_ACCOUNT_ID",
        "ALIYUN_ACCOUNT_ID",
    )
    role = _env_first(
        "ALIYUN_SLS_ASSUME_TARGET_ROLE_NAME",
        "ALIYUN_RAM_ROLE_NAME",
    )
    if account and role:
        return f"acs:ram::{account}:role/{role}"
    return ""


def _sls_assume_caller_ak_sk():
    # type: () -> Tuple[str, str]
    """调用 STS AssumeRole 的 RAM 用户长期 AK（本机调试；ECS 上优先用 ALIYUN_ECS_RAM_ROLE_NAME）。"""
    ak = _env_first("ALIYUN_ASSUME_ACCESS_KEY_ID", "ALIYUN_ACCESS_KEY_ID")
    sk = _env_first("ALIYUN_ASSUME_ACCESS_KEY_SECRET", "ALIYUN_ACCESS_KEY_SECRET")
    return ak, sk


def _ecs_ram_security_credentials_or_raise(role_name):
    # type: (str) -> Tuple[str, str, str]
    """从 ECS 元数据拉取绑定在该实例上的 RAM 角色临时凭证（仅 ECS 内网可用）。"""
    import urllib.error
    import urllib.request

    url = (
        "http://100.100.100.200/latest/meta-data/ram/security-credentials/"
        + urllib.request.quote(role_name, safe="")
    )
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.URLError as e:
        raise ValueError(
            "configured ALIYUN_ECS_RAM_ROLE_NAME but cannot fetch RAM credentials from ECS metadata "
            "(http://100.100.100.200). Only works inside Alibaba Cloud ECS/VPC.\n"
            "本地调试请改用 ALIYUN_ASSUME_ACCESS_KEY_ID/SECRET，或使用控制台 STS 三件套。\n"
            f"原因: {e}"
        ) from e
    body = json.loads(raw)
    ak = body.get("AccessKeyId")
    sk = body.get("AccessKeySecret")
    token = body.get("SecurityToken")
    if not ak or not sk or not token:
        raise ValueError("ECS metadata RAM JSON missing AccessKeyId/Secret/SecurityToken: " + raw[:400])
    return ak, sk, token


def _sls_assume_caller_triple_or_raise():
    # type: () -> Tuple[str, str, Optional[str]]
    """AssumeRole 调用方：(ak, sk, token)；token 非空时表示用 STS 临时身份（ECS 实例角色）。"""
    ecs_role = _env_first("ALIYUN_ECS_RAM_ROLE_NAME")
    if ecs_role:
        ak, sk, tok = _ecs_ram_security_credentials_or_raise(ecs_role)
        return ak, sk, tok
    ak, sk = _sls_assume_caller_ak_sk()
    return ak, sk, None


def _sls_credentials_via_assume_role(
    role_arn,
    caller_ak,
    caller_sk,
    caller_token=None,
):
    # type: (str, str, str, Optional[str]) -> Tuple[str, str, str]
    try:
        from aliyunsdkcore.auth.credentials import StsTokenCredential
        from aliyunsdkcore.client import AcsClient
        from aliyunsdksts.request.v20150401 import AssumeRoleRequest
    except ImportError as e:
        raise ImportError(
            "已配置 AssumeRole 查 SLS，请安装: pip install aliyun-python-sdk-core aliyun-python-sdk-sts"
        ) from e

    sts_region = _env_first("ALIYUN_STS_REGION") or "cn-hangzhou"
    session_name = (_env_first("ALIYUN_ASSUME_ROLE_SESSION_NAME") or "amountloss-sls").strip() or "amountloss-sls"
    if len(session_name) > 64:
        session_name = session_name[:64]
    dur_raw = _env_first("ALIYUN_ASSUME_ROLE_DURATION_SECONDS")
    try:
        duration = int(dur_raw) if dur_raw else 3600
    except ValueError:
        duration = 3600

    if caller_token:
        credential = StsTokenCredential(caller_ak, caller_sk, caller_token)
        client = AcsClient(region_id=sts_region, credential=credential)
    else:
        client = AcsClient(caller_ak, caller_sk, sts_region)
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_accept_format("json")
    req.set_RoleArn(role_arn)
    req.set_RoleSessionName(session_name)
    req.set_DurationSeconds(duration)
    ext = _env_first("ALIYUN_ASSUME_ROLE_EXTERNAL_ID")
    if ext:
        req.set_ExternalId(ext)

    raw = client.do_action_with_exception(req)
    payload = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
    body = json.loads(payload)
    cred = body.get("Credentials") or {}
    ak = cred.get("AccessKeyId")
    secret = cred.get("AccessKeySecret")
    token = cred.get("SecurityToken")
    if not ak or not secret or not token:
        raise ValueError(
            "AssumeRole 响应未包含完整 Credentials。片段：" + payload[:800]
        )
    return ak, secret, token


def _sls_credentials_or_raise():
    """优先 AssumeRole（若配置了 Role）；否则使用控制台 STS 三件套或长期 AK（勿写入代码仓库）。"""
    role_arn = _sls_assume_role_arn()
    if role_arn:
        caller_ak, caller_sk, caller_tok = _sls_assume_caller_triple_or_raise()
        if not _env_first("ALIYUN_ECS_RAM_ROLE_NAME") and (not caller_ak or not caller_sk):
            raise ValueError(
                "checklog：已配置 AssumeRole 目标角色（ALIYUN_SLS_ASSUME_ROLE_ARN 或 "
                "ALIYUN_SLS_ASSUME_TARGET_ACCOUNT_ID + ALIYUN_SLS_ASSUME_TARGET_ROLE_NAME），"
                "但未配置调用方凭证。\n"
                "在 ECS 上：设置 ALIYUN_ECS_RAM_ROLE_NAME=OSE-ECS-Read-Monitor-sls（从元数据取 STS）。\n"
                "在本机：设置 ALIYUN_ASSUME_ACCESS_KEY_ID / ALIYUN_ASSUME_ACCESS_KEY_SECRET（RAM 用户长期密钥）。"
            )
        return _sls_credentials_via_assume_role(
            role_arn, caller_ak, caller_sk, caller_tok
        )

    ak = _env_first(
        "ALIYUN_SLS_ACCESS_KEY_ID",
        "ALIBABA_CLOUD_ACCESS_KEY_ID",
        "ALIYUN_ACCESS_KEY_ID",
    )
    secret = _env_first(
        "ALIYUN_SLS_ACCESS_KEY_SECRET",
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
        "ALIYUN_ACCESS_KEY_SECRET",
    )
    token = _env_first(
        "ALIYUN_SLS_SECURITY_TOKEN",
        "ALIBABA_CLOUD_SECURITY_TOKEN",
    )
    if not ak or not secret:
        raise ValueError(
            "checklog 需要阿里云凭证，任选其一：\n"
            "  A) ECS AssumeRole：目标 ALIYUN_SLS_ASSUME_ROLE_ARN=acs:ram::5754739415144793:role/sls-platform-readonly"
            "（或 TARGET_ACCOUNT_ID + TARGET_ROLE_NAME）"
            " + 调用方 ALIYUN_ECS_RAM_ROLE_NAME=OSE-ECS-Read-Monitor-sls；需 pip install aliyun-python-sdk-sts\n"
            "  B) 本机 AssumeRole：同上目标 ARN + ALIYUN_ASSUME_ACCESS_KEY_ID/SECRET（RAM 用户可 Assume 该角色）\n"
            "  C) 控制台「获取临时凭证」：ALIYUN_SLS_ACCESS_KEY_ID / _SECRET / _SECURITY_TOKEN\n"
            "  或在 amountloss.py 同目录 amountloss.sls.env / .env 中配置（勿提交 Git）。"
        )
    return ak, secret, token or None


def _sls_msg_is_error_candidate(msg: str) -> bool:
    """与控制台一致：msg 中同时含 platformCreditLostFix 与 null。"""
    if not msg:
        return False
    return ("platformCreditLostFix" in msg) and ("null" in msg.lower())


def _sls_fetch_error_msgs_for_row(transfer_id, center_dt):
    # type: (str, datetime) -> List[str]
    """按控制台查询语法与时间窗拉取，再筛 platformCreditLostFix + null。"""
    try:
        from aliyun.log import GetLogsRequest, LogClient
    except ImportError as e:
        raise ImportError(
            "请安装: pip install aliyun-log-python-sdk"
        ) from e

    ak, secret, token = _sls_credentials_or_raise()
    endpoint = _env_first("ALIYUN_SLS_ENDPOINT") or SLS_DEFAULT_ENDPOINT
    project = _env_first("ALIYUN_SLS_PROJECT") or SLS_DEFAULT_PROJECT
    logstore = _env_first("ALIYUN_SLS_LOGSTORE") or SLS_DEFAULT_LOGSTORE

    start = center_dt - timedelta(minutes=SLS_WINDOW_MINUTES)
    end = center_dt + timedelta(minutes=SLS_WINDOW_MINUTES)
    from_ts = int(start.timestamp())
    to_ts = int(end.timestamp())
    # 与 Monaco 示例一致：7046635540 and "null null"
    query = f'{transfer_id} and "null null"'

    client = LogClient(endpoint, ak, secret, securityToken=token)
    req = GetLogsRequest(
        project=project,
        logstore=logstore,
        fromTime=from_ts,
        toTime=to_ts,
        topic="",
        query=query,
        line=500,
        offset=0,
        reverse=False,
    )
    resp = client.get_logs(req)
    out = []  # type: List[str]
    for log in resp.get_logs():
        contents = log.get_contents() or {}
        msg = contents.get("msg")
        if msg is None:
            continue
        if isinstance(msg, str) and _sls_msg_is_error_candidate(msg):
            out.append(msg.strip())
    return out


def _attach_sls_error_logs(filter_headers, filter_rows):
    # type: (list, list) -> Tuple[list, list]
    """在 FILTERED 表后追加列 Error log。"""
    try:
        idx_tid = filter_headers.index("Transfer ID")
        idx_st = filter_headers.index("Start Time")
    except ValueError as e:
        raise ValueError("checklog 需要列 Transfer ID 与 Start Time") from e

    new_headers = list(filter_headers) + ["Error log"]
    new_rows = []
    print(
        f"📡 SLS：每行 Transfer ID + Start Time ±{SLS_WINDOW_MINUTES} 分钟；"
        '检索式与控制台一致："<id> and \\"null null\\""；'
        "保留 msg 中含 platformCreditLostFix 且含 null 的记录。"
    )
    for row in filter_rows:
        tid = row[idx_tid].strip() if idx_tid < len(row) else ""
        st = row[idx_st] if idx_st < len(row) else ""
        dt = _parse_report_datetime(st)
        if not tid or dt is None:
            new_rows.append(list(row) + [""])
            continue
        try:
            msgs = _sls_fetch_error_msgs_for_row(tid, dt)
            cell = "\n---\n".join(msgs) if msgs else ""
        except Exception as ex:
            cell = f"(SLS 查询失败: {ex})"
        new_rows.append(list(row) + [cell])
    return new_headers, new_rows


def _amount_loss_result_summary(page, total_label_text: str) -> str:
    """组合 Total … records 与 Search time …，供飞书展示。"""
    merged = page.evaluate(
        r"""() => {
            let totalLine = '';
            let searchLine = '';
            const nodes = document.querySelectorAll('label, span, div, p, td, li');
            for (const el of nodes) {
                const raw = (el.textContent || '').replace(/\s+/g, ' ').trim();
                if (!raw || raw.length > 220) continue;
                if (/^Total\s+\d+\s+records?\.?$/i.test(raw)) totalLine = raw;
                if (/search\s*time/i.test(raw) && /second/i.test(raw)) searchLine = raw;
            }
            if (totalLine && searchLine) return totalLine + ' / ' + searchLine;
            if (totalLine) return totalLine;
            if (searchLine) return searchLine;
            return '';
        }"""
    )
    if merged and str(merged).strip():
        return str(merged).strip()
    return " ".join((total_label_text or "").split())


def _set_page_size_and_blur(page, size: int = 1000) -> None:
    """Length Per Page：FPMS 把 input.pageSize 放在 #creditLostFixProposalReportTablePage，不在 table_wrapper 里。"""
    page.wait_for_selector("#creditLostFixProposalReportTable", state="visible", timeout=15000)
    page.wait_for_selector(
        "#creditLostFixProposalReportTablePage input.pageSize",
        state="visible",
        timeout=15000,
    )
    page.evaluate(
        """([n]) => {
            const el = document.querySelector(
                '#creditLostFixProposalReportTablePage input.pageSize'
            );
            if (!el) throw new Error('creditLostFixProposalReportTablePage input.pageSize not found');
            el.focus();
            el.value = String(n);
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
            el.blur();
        }""",
        [size],
    )
    page.mouse.click(5, 5)


def _scrape_credit_lost_proposal_table(page):
    """表头来自提案表 wrapper 的 scrollHead（避免命中上方 summary 表）；tbody 用 #creditLostFixProposalReportTable。"""
    return page.evaluate(
        r"""() => {
            const wrap = document.getElementById('creditLostFixProposalReportTable_wrapper');
            const headRow = wrap && wrap.querySelector('.dataTables_scrollHead thead tr');
            const tbody = document.querySelector('#creditLostFixProposalReportTable tbody');
            if (!headRow) return { error: 'no proposal scroll head', headers: [], rows: [] };
            if (!tbody) return { error: 'no tbody', headers: [], rows: [] };
            const headers = Array.from(headRow.querySelectorAll('th')).map((th) => {
                let t = (th.innerText || th.textContent || '').replace(/\s+/g, ' ').trim();
                t = t.replace(/\s*:\s*activate to sort column.*$/i, '').trim();
                return t;
            });
            const rows = [];
            for (const tr of tbody.querySelectorAll('tr')) {
                const cells = Array.from(tr.querySelectorAll('td')).map((td) => {
                    const a = td.querySelector('a');
                    if (a) return (a.textContent || '').replace(/\s+/g, ' ').trim();
                    return (td.innerText || td.textContent || '').replace(/\s+/g, ' ').trim();
                });
                if (cells.length) rows.push(cells);
            }
            return { headers, rows, error: '' };
        }"""
    )


def _print_table(headers, rows):
    if not headers:
        print("（无表头）")
        return
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            if i < len(widths):
                widths[i] = max(widths[i], len(c))
    sep = " | "

    def fmt_row(cells):
        parts = []
        for i, h in enumerate(headers):
            cell = cells[i] if i < len(cells) else ""
            parts.append(cell.ljust(widths[i]))
        return sep.join(parts)

    print(fmt_row(headers))
    print(sep.join("-" * w for w in widths))
    for r in rows:
        print(fmt_row(r))
    print(f"共 {len(rows)} 行，{len(headers)} 列。")


def _header_col_index(headers, *needles):
    """按规范化子串匹配列下标，needles 按优先级尝试。"""
    norm = [(h or "").replace("\n", " ").strip().lower() for h in headers]
    for needle in needles:
        n = (needle or "").strip().lower()
        for i, h in enumerate(norm):
            if n == h or n in h or h in n:
                return i
    return None


def _rows_as_dicts(headers, rows):
    out = []
    for cells in rows:
        d = {}
        for j, h in enumerate(headers):
            key = (h or "").replace("\n", " ").strip()
            d[key] = cells[j].strip() if j < len(cells) else ""
        out.append(d)
    return out


def _parse_report_datetime(s: str):
    """解析 FPMS 如 2026/04/27  21:42:40（多空格亦可）。"""
    if not s or not str(s).strip():
        return None
    t = re.sub(r"\s+", " ", str(s).strip())
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
        try:
            return datetime.strptime(t, fmt)
        except ValueError:
            continue
    return None


def _filter_30min_non_overlapping(sorted_rows: list, time_key: str) -> list:
    """
    按时间升序；保留每条「锚点」行，删除其后 30 分钟内（含同刻多条则只保留锚点）的其余行；
    下一锚点为第一个时间 > 当前锚点 + 30 分钟的记录，重复。
    """
    if not sorted_rows:
        return []
    out = []
    i = 0
    n = len(sorted_rows)
    while i < n:
        row = sorted_rows[i]
        out.append(row)
        t0 = _parse_report_datetime(row.get(time_key, ""))
        i += 1
        if t0 is None:
            continue
        cutoff = t0 + timedelta(minutes=30)
        while i < n:
            t1 = _parse_report_datetime(sorted_rows[i].get(time_key, ""))
            if t1 is None:
                i += 1
                continue
            if t1 <= cutoff:
                i += 1
            else:
                break
    return out


def _split_remarks_transfer_id(remarks):
    # type: (str) -> Tuple[str, str]
    """按 ' Transfer ID' 拆成名称与 ID（ID 可为数字串）。"""
    r = (remarks or "").strip()
    marker = " Transfer ID"
    if marker in r:
        i = r.index(marker)
        name = r[:i].strip()
        tid = r[i + len(marker) :].strip()
        return name, tid
    m = re.search(r"(?i)\s+transfer\s*id\s*(\d+)\s*$", r)
    if m:
        return r[: m.start()].strip(), m.group(1).strip()
    return r, ""


def _filter_credit_lost_table(headers, rows):
    # type: (list, list) -> Tuple[list, list]
    """
    仅 Account / Amount (PHP) / Start Time / Remarks；
    备注以 Transfer-InLive / Transfer-OutLive 分两路，各自按天时间内升序后做 30 分钟窗口去重；
    备注拆 Transfer Name + Transfer ID；最终行序：全部 In（时间升序）再接全部 Out（时间升序）。
    """
    h_acc = _header_col_index(headers, "Account", "account")
    h_amt = _header_col_index(headers, "Amount (PHP)", "amount (php)", "amount")
    h_st = _header_col_index(headers, "Start Time", "start time")
    h_rm = _header_col_index(headers, "Remarks", "remark")
    if None in (h_acc, h_amt, h_st, h_rm):
        missing = []
        if h_acc is None:
            missing.append("Account")
        if h_amt is None:
            missing.append("Amount (PHP)")
        if h_st is None:
            missing.append("Start Time")
        if h_rm is None:
            missing.append("Remarks")
        raise ValueError("表头缺少列: " + ", ".join(missing) + f"；实际表头={headers!r}")

    slim = []
    for cells in rows:
        if h_rm >= len(cells):
            continue
        acc = cells[h_acc].strip() if h_acc < len(cells) else ""
        amt = cells[h_amt].strip() if h_amt < len(cells) else ""
        st = cells[h_st].strip() if h_st < len(cells) else ""
        rm = cells[h_rm].strip() if h_rm < len(cells) else ""
        slim.append(
            {
                "Account": acc,
                "Amount (PHP)": amt,
                "Start Time": st,
                "Remarks": rm,
            }
        )

    in_prefix = "Transfer-InLive SlotsAmount Lost"
    out_prefix = "Transfer-OutLive SlotsAmount Lost"

    def is_in_live(r):
        return r["Remarks"].startswith(in_prefix)

    def is_out_live(r):
        return r["Remarks"].startswith(out_prefix)

    in_part = [r for r in slim if is_in_live(r)]
    out_part = [r for r in slim if is_out_live(r)]

    in_part.sort(key=lambda r: (_parse_report_datetime(r["Start Time"]) or datetime.min,))
    out_part.sort(key=lambda r: (_parse_report_datetime(r["Start Time"]) or datetime.min,))

    in_f = _filter_30min_non_overlapping(in_part, "Start Time")
    out_f = _filter_30min_non_overlapping(out_part, "Start Time")

    out_headers = [
        "Account",
        "Amount (PHP)",
        "Start Time",
        "Transfer Name",
        "Transfer ID",
    ]
    combined = []
    for r in in_f:
        name, tid = _split_remarks_transfer_id(r["Remarks"])
        combined.append(
            [r["Account"], r["Amount (PHP)"], r["Start Time"], name, tid]
        )
    for r in out_f:
        name, tid = _split_remarks_transfer_id(r["Remarks"])
        combined.append(
            [r["Account"], r["Amount (PHP)"], r["Start Time"], name, tid]
        )
    return out_headers, combined


def fetch_fpms_data(
    headless=False,
    target_date_str=None,
    save_state=False,
    getdata=False,
    filterdata=False,
    checklog=False,
):
    """
    main.py /al 调用: fetch_fpms_data(headless=True, target_date_str=date_str, filterdata=True, checklog=True)
    target_date_str: 可选，格式 DD/MM。
      - 未传: Start = 昨天 00:00:00，End = 今天 00:00:00
      - 例如 16/04: Start = 当年 16/04 00:00:00，End = 17/04 00:00:00
    save_state: 兼容参数，当前脚本未使用。
    getdata: 为 True 时，在读到 Total … records 后将每页条数设为 1000、等待 8 秒并打印明细表。
    filterdata: 为 True 时同样拉表，再按 In/Out Live 备注过滤、30 分钟窗口去重、拆 Transfer ID 后打印精简表。
    checklog: 为 True 时同样拉表并做 filter，再调用阿里云 SLS API（非浏览器）按每行查询 Error log；
      凭证：控制台 STS 三件套，或 RAM 用户 AK + ALIYUN_SLS_ASSUME_ROLE_ARN（AssumeRole，见文件顶部注释）。
    返回: 一行摘要字符串，例如 Total 0 records / Search time: 0.205 seconds
    """
    _ = save_state  # 保留与旧版 fpms_fetcher 相同的调用约定
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            slow_mo=100 if not headless else 0,
            args=CHROMIUM_ARGS,
        )
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=DESKTOP_UA,
            device_scale_factor=1,
            is_mobile=False,
            has_touch=False,
        )
        page = context.new_page()

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

            # 2. 日期：默认 昨天 00:00 ~ 今天 00:00；DD/MM 则为 该日 00:00 ~ 次日 00:00
            now = datetime.now()
            today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            if target_date_str:
                part = target_date_str.strip()
                day, month = map(int, part.split("/"))
                start_date = datetime(now.year, month, day)
                end_date = start_date + timedelta(days=1)
            else:
                end_date = today_midnight
                start_date = end_date - timedelta(days=1)
            start_str = start_date.strftime("%Y/%m/%d 00:00:00")
            end_str = end_date.strftime("%Y/%m/%d 00:00:00")
            print(f"📅 设置日期范围：{start_str} ~ {end_str}")

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

            print("⏳ 等待查询结果…")
            page.wait_for_timeout(5000)

            total_label = page.locator('label.ng-binding:has-text("Total")').first
            total_label.wait_for(state="visible", timeout=30000)
            total_text = total_label.text_content().strip()
            summary = _amount_loss_result_summary(page, total_text)
            print(f"📊 {summary}")

            if getdata or filterdata or checklog:
                print("📄 设置 Length Per Page = 1000 并移开焦点…")
                _set_page_size_and_blur(page, 1000)
                print("⏳ 等待 8 秒让表格按每页 1000 条加载…")
                page.wait_for_timeout(8000)
                data = _scrape_credit_lost_proposal_table(page)
                if data.get("error"):
                    print(f"⚠️ 抓表: {data['error']}")
                headers = data.get("headers") or []
                rows = data.get("rows") or []
                if getdata:
                    print(
                        "\n===== CREDIT_LOST_FIX_PROPOSAL 明细（scrollHead 列名 / scrollBody 数据）====="
                    )
                    _print_table(headers, rows)
                if filterdata or checklog:
                    try:
                        fh, fr = _filter_credit_lost_table(headers, rows)
                        if filterdata:
                            print(
                                "\n===== FILTERED（Account / Amount / Start Time / Transfer Name / Transfer ID）====="
                            )
                            _print_table(fh, fr)
                        if checklog:
                            _load_sls_env_files()
                            print(
                                "\n===== CHECKLOG（SLS Error log；project="
                                f"{_env_first('ALIYUN_SLS_PROJECT') or SLS_DEFAULT_PROJECT}）====="
                            )
                            eh, er = _attach_sls_error_logs(fh, fr)
                            _print_table(eh, er)
                    except ValueError as ve:
                        print(f"⚠️ filterdata/checklog: {ve}")

            return summary

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
    argv = sys.argv[1:]
    headless = "--headless" in argv or "-headless" in argv
    getdata = "--getdata" in argv
    filterdata = "--filterdata" in argv
    checklog = "--checklog" in argv
    skip_flags = {"--headless", "-headless", "--getdata", "--filterdata", "--checklog"}
    date_arg = None
    for a in argv:
        if a in skip_flags or a.startswith("-"):
            continue
        date_arg = a
        break
    out = fetch_fpms_data(
        headless=headless,
        target_date_str=date_arg,
        getdata=getdata,
        filterdata=filterdata,
        checklog=checklog,
    )
    print("\n===== 结果 =====")
    print(out)

