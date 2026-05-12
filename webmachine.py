#!/usr/bin/env python3
"""
Lark **Web app** — lightweight machine status dashboard (standalone or mounted on Duty ``main.py``).

Run standalone::

    python3 webmachine.py
    # or: WEBMACHINE_PORT=8765 python3 webmachine.py

Mount inside Duty bot (same process, no second ``main.py``) — **on by default**; to disable set ``WEBMACHINE_MOUNT_IN_MAIN=0`` (or ``false`` / ``no`` / ``off``)::

    # optional: WEBMACHINE_MOUNT_IN_MAIN=0   disable dashboard on this app
    # optional: WEBMACHINE_URL_PREFIX=/wm   (default /wm)
    # optional: WEBMACHINE_SCRAPE=0       disable live EGM scrape (default: **on** in code; no .env required)
    # optional: WEBMACHINE_SCRAPE_INTERVAL_SEC=900
    # optional: WEBMACHINE_SITES=nwr,nch,...  (default: ``smmachine.DEFAULT_WEBMACHINE_SITES`` — all backends; CP/OSM share one URL and are deduped)
    # optional: SM_MACHINE_COLLECT_MAX_PAGES=500  (read-only scrape page cap when SM_MACHINE_MAX_PAGES unset)

Point Feishu **Desktop / Mobile homepage** to your public HTTPS URL (reverse-proxy to this port).

**Feishu + free ngrok (``*.ngrok-free.app``) — interstitial / 内置页打不开**

Free ngrok shows a **warning HTML page** before traffic reaches this app. Normal browsers can click **Visit site**; Lark’s **in-app WebView** often cannot, so the same URL works in Chrome but **fails inside Lark**.

- **Practical fix:** append Feishu’s **open in system browser** flag to the homepage URL in the developer console, e.g.  
  ``https://<your-tunnel>.ngrok-free.app/wm/?lk_jump_to_browser=true``  
  (Feishu / Lark docs: open webpage in external browser.)

- **Other options:** paid ngrok (disable interstitial in dashboard), or a **real HTTPS domain** / Cloudflare Tunnel without that page.

Adding ``ngrok-skip-browser-warning`` in **Flask** does **not** fix the **first** HTML load: ngrok serves the warning **before** the request hits this app, and Lark’s WebView does not send that header on normal navigation. (Custom headers only help clients you control, e.g. ``curl`` / ``fetch``.)

Data sources:

1. **Live scrape** (default **on**) — background thread calls ``smmachine.smachine_collect_machines_multi_sites``; disable with ``WEBMACHINE_SCRAPE=0``.
2. **JSON file** — default ``webmachine_data.json`` next to this module (or ``WEBMACHINE_DATA_PATH``). If missing, an empty ``[]`` file is **created**; after each successful scrape, results are **written back** to that file (skipped when ``WEBMACHINE_JSON`` inline is set).
3. **Inline JSON** — ``WEBMACHINE_JSON='[...]'`` overrides file for fallback display only (no auto-create / no persist).

Row keys (first match wins): **environment** / ``env`` / ``site``; **name** / ``machine``; **status**;
**online** / ``online_offline`` / ``state``. Optional boolean **is_test** (or ``(TEST)`` in the name) for dashboard counts.

The HTML dashboard shows **global and per-environment** totals (online / offline / conn unknown / test / maintain) and a **search** box; ``GET /api/machines`` includes a **stats** object with the same numbers.

Optional: ``WEBMACHINE_API_TOKEN`` — ``GET /api/machines`` requires ``Authorization: Bearer <token>``.

Env: ``WEBMACHINE_PORT``, ``WEBMACHINE_HOST``, ``WEBMACHINE_TITLE``, ``WEBMACHINE_REFRESH_SEC``, ``WEBMACHINE_DEBUG``.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

from flask import Blueprint, Flask, jsonify, render_template_string, request, url_for

wm_bp = Blueprint("wm", __name__)

_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{{ title }}</title>
  {% if refresh_sec > 0 %}
  <meta http-equiv="refresh" content="{{ refresh_sec }}"/>
  {% endif %}
  <style>
    :root {
      --bg: #0b0f14;
      --card: #151b26;
      --elev: #1c2533;
      --text: #e8edf4;
      --muted: #8b9cb3;
      --accent: #3b82f6;
      --ok: #22c55e;
      --warn: #eab308;
      --bad: #ef4444;
      --line: #2a3544;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, rgba(59,130,246,.12), transparent), var(--bg);
      color: var(--text); min-height: 100vh;
    }
    header {
      padding: 1.1rem 1.35rem; border-bottom: 1px solid var(--line);
      display: flex; flex-wrap: wrap; align-items: flex-start; justify-content: space-between; gap: 0.85rem;
    }
    h1 { margin: 0; font-size: 1.25rem; font-weight: 650; letter-spacing: -0.02em; }
    .sub { color: var(--muted); font-size: 0.8rem; line-height: 1.45; max-width: 42rem; }
    .count { font-weight: 700; color: var(--text); }
    main { padding: 1rem 1.35rem 2.5rem; max-width: 1280px; margin: 0 auto; width: 100%; }
    .panel {
      background: var(--card); border: 1px solid var(--line); border-radius: 12px;
      padding: 1rem 1.1rem; margin-bottom: 1rem; box-shadow: 0 8px 32px rgba(0,0,0,.2);
    }
    .panel-title { margin: 0 0 0.75rem; font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: .08em; color: var(--muted); }
    .summary-grid {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(108px, 1fr)); gap: 0.65rem;
    }
    .stat-card {
      background: var(--elev); border: 1px solid var(--line); border-radius: 10px; padding: 0.7rem 0.85rem;
    }
    .stat-card .num { font-size: 1.35rem; font-weight: 750; line-height: 1.15; letter-spacing: -0.03em; }
    .stat-card .lbl { font-size: 0.65rem; color: var(--muted); text-transform: uppercase; letter-spacing: .06em; margin-top: 0.2rem; }
    .stat-card.total .num { color: var(--accent); }
    .stat-card.ok .num { color: var(--ok); }
    .stat-card.bad .num { color: var(--bad); }
    .stat-card.warn .num { color: var(--warn); }
    .env-scroll {
      display: flex; flex-wrap: wrap; gap: 0.65rem;
    }
    .env-card {
      flex: 1 1 160px; min-width: 148px; max-width: 220px;
      background: var(--elev); border: 1px solid var(--line); border-radius: 10px; padding: 0.65rem 0.75rem;
    }
    .env-card h3 { margin: 0 0 0.45rem; font-size: 0.95rem; font-weight: 650; color: var(--accent); }
    .env-mini { display: grid; grid-template-columns: 1fr 1fr; gap: 0.25rem 0.5rem; font-size: 0.72rem; color: var(--muted); }
    .env-mini b { color: var(--text); font-weight: 600; }
    .toolbar {
      display: flex; flex-wrap: wrap; align-items: center; gap: 0.65rem; margin-bottom: 0.85rem;
    }
    .toolbar input[type="search"] {
      flex: 1 1 220px; max-width: 420px; min-width: 180px;
      padding: 0.55rem 0.85rem; border-radius: 10px; border: 1px solid var(--line);
      background: #0f141c; color: var(--text); font-size: 0.92rem; outline: none;
    }
    .toolbar input[type="search"]:focus { border-color: var(--accent); box-shadow: 0 0 0 3px rgba(59,130,246,.2); }
    .filter-hint { font-size: 0.78rem; color: var(--muted); }
    .env-filter-bar {
      display: flex; flex-wrap: wrap; align-items: center; gap: 0.45rem; margin-bottom: 0.75rem;
    }
    .env-filter-btn {
      font: inherit; font-size: 0.8rem; font-weight: 600;
      padding: 0.4rem 0.8rem; border-radius: 999px; cursor: pointer; border: 1px solid var(--line);
      background: var(--elev); color: var(--muted);
      transition: background .15s, color .15s, border-color .15s;
    }
    .env-filter-btn:hover { border-color: var(--accent); color: var(--text); }
    .env-filter-btn.active {
      background: rgba(59,130,246,.25); border-color: var(--accent); color: var(--text);
    }
    .table-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; border-radius: 12px; border: 1px solid var(--line); }
    table { width: 100%; border-collapse: collapse; background: var(--elev); }
    th, td { padding: 0.6rem 0.8rem; text-align: left; border-bottom: 1px solid var(--line); font-size: 0.88rem; }
    th {
      background: #222b3a; font-size: 0.68rem; text-transform: uppercase; letter-spacing: .06em; color: var(--muted);
      position: sticky; top: 0; z-index: 1;
    }
    tr:last-child td { border-bottom: none; }
    tbody tr:hover td { background: rgba(59,130,246,.07); }
    .pill {
      display: inline-block; padding: 0.18rem 0.5rem; border-radius: 999px; font-size: 0.72rem; font-weight: 600;
    }
    .pill-online { background: rgba(34,197,94,.15); color: var(--ok); }
    .pill-offline { background: rgba(239,68,68,.15); color: var(--bad); }
    .pill-unknown { background: rgba(139,156,179,.2); color: var(--muted); }
    .pill-test { background: rgba(234,179,8,.14); color: var(--warn); }
    .pill-maint { background: rgba(248,113,113,.12); color: #fca5a5; }
    .pill-occupy { background: rgba(234,179,8,.14); color: var(--warn); }
    .empty { color: var(--muted); padding: 2.5rem 1rem; text-align: center; line-height: 1.6; }
    footer { padding: 1rem; text-align: center; color: var(--muted); font-size: 0.75rem; border-top: 1px solid var(--line); }
    a { color: var(--accent); text-decoration: none; } a:hover { text-decoration: underline; }
    .muted { color: var(--muted); font-size: 0.85rem; }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>{{ title }}</h1>
      <div class="sub">Live machine list · provenance: {{ loaded_from }}{% if row_total %} · <span class="count">{{ row_total }} rows</span>{% endif %}</div>
    </div>
  </header>
  <main>
    {% if rows %}
    <section class="panel" aria-label="All environments summary">
      <h2 class="panel-title">All environments</h2>
      <div class="summary-grid">
        <div class="stat-card total"><div class="num">{{ stats.total }}</div><div class="lbl">Total</div></div>
        <div class="stat-card ok"><div class="num">{{ stats.online }}</div><div class="lbl">Online</div></div>
        <div class="stat-card bad"><div class="num">{{ stats.offline }}</div><div class="lbl">Offline</div></div>
        <div class="stat-card"><div class="num">{{ stats.conn_unknown }}</div><div class="lbl">Conn unknown</div></div>
        <div class="stat-card warn"><div class="num">{{ stats.test }}</div><div class="lbl">Test mode</div></div>
        <div class="stat-card"><div class="num">{{ stats.maintain }}</div><div class="lbl">Maintain</div></div>
      </div>
    </section>
    <section class="panel" aria-label="Per environment summary">
      <h2 class="panel-title">By environment</h2>
      <div class="env-scroll">
        {% for e in stats_env %}
        <div class="env-card">
          <h3>{{ e.environment }}</h3>
          <div class="env-mini">
            <span>Total</span><b>{{ e.total }}</b>
            <span>Online</span><b>{{ e.online }}</b>
            <span>Offline</span><b>{{ e.offline }}</b>
            <span>Unknown</span><b>{{ e.conn_unknown }}</b>
            <span>Test</span><b>{{ e.test }}</b>
            <span>Maintain</span><b>{{ e.maintain }}</b>
          </div>
        </div>
        {% endfor %}
      </div>
    </section>
    <div class="env-filter-bar" id="wm-env-filters" role="toolbar" aria-label="Filter by environment">
      <button type="button" class="env-filter-btn active" data-wm-env="" id="wm-env-all">Show all</button>
      {% for e in stats_env %}
      <button type="button" class="env-filter-btn" data-wm-env="{{ e.environment|e }}">{{ e.environment }}</button>
      {% endfor %}
    </div>
    <div class="toolbar">
      <input type="search" id="wm-search" placeholder="Search environment, machine, status, online…" autocomplete="off" aria-label="Filter machines"/>
      <span class="filter-hint" id="wm-filter-hint"></span>
    </div>
    <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Environment</th>
          <th>Machine name</th>
          <th>Test</th>
          <th>Status</th>
          <th>Online / Offline</th>
        </tr>
      </thead>
      <tbody id="wm-tbody">
        {% for r in rows %}
        <tr data-env="{{ r.environment|e }}" data-name="{{ r.name|e }}" data-status="{{ r.status|e }}" data-online="{{ r.online_label|e }}">
          <td>{{ r.environment }}</td>
          <td><strong>{{ r.name }}</strong></td>
          <td>{% if r.is_test %}<span class="pill pill-test">TEST</span>{% else %}<span class="muted">—</span>{% endif %}</td>
          <td>
            {% set st = (r.status or '')|lower %}
            {% if 'maintain' in st %}
            <span class="pill pill-maint">{{ r.status }}</span>
            {% elif 'normal' in st %}
            <span class="pill pill-online">{{ r.status }}</span>
            {% elif 'occupy' in st %}
            <span class="pill pill-occupy">{{ r.status }}</span>
            {% else %}
            {{ r.status }}
            {% endif %}
          </td>
          <td><span class="pill {{ r.pill_class }}">{{ r.online_label }}</span></td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    </div>
    <script>
    (function () {
      var inp = document.getElementById("wm-search");
      var hint = document.getElementById("wm-filter-hint");
      var tbody = document.getElementById("wm-tbody");
      var bar = document.getElementById("wm-env-filters");
      if (!tbody) return;
      var envSel = "";

      function matchesEnv(tr) {
        if (!envSel) return true;
        return (tr.getAttribute("data-env") || "") === envSel;
      }

      function apply() {
        var term = (inp && inp.value) ? inp.value.trim().toLowerCase() : "";
        var rows = tbody.querySelectorAll("tr");
        var total = rows.length;
        var n = 0;
        for (var i = 0; i < rows.length; i++) {
          var tr = rows[i];
          var hay = (tr.getAttribute("data-env") || "") + " " + (tr.getAttribute("data-name") || "") + " " +
            (tr.getAttribute("data-status") || "") + " " + (tr.getAttribute("data-online") || "");
          hay = hay.toLowerCase();
          var textOk = !term || hay.indexOf(term) !== -1;
          var envOk = matchesEnv(tr);
          var show = textOk && envOk;
          tr.style.display = show ? "" : "none";
          if (show) n++;
        }
        if (hint) {
          if (!envSel && !term) {
            hint.textContent = "";
          } else {
            var label = [];
            if (envSel) label.push("env: " + envSel);
            if (term) label.push("search");
            hint.textContent = "Showing " + n + " of " + total + " (" + label.join(", ") + ")";
          }
        }
      }

      if (bar) {
        bar.addEventListener("click", function (ev) {
          var btn = ev.target.closest("[data-wm-env]");
          if (!btn || !bar.contains(btn)) return;
          envSel = btn.getAttribute("data-wm-env") || "";
          bar.querySelectorAll(".env-filter-btn").forEach(function (b) {
            b.classList.toggle("active", b === btn);
          });
          apply();
        });
      }
      if (inp) {
        inp.addEventListener("input", apply);
        inp.addEventListener("search", apply);
      }
    })();
    </script>
    {% else %}
    <div class="empty">
      No rows yet. If live scrape is on, wait for the first run to finish or check <code>/api/machines</code> for <code>scrape.errors</code>.
      Otherwise add rows to <code>webmachine_data.json</code> or set <code>WEBMACHINE_JSON</code>.
    </div>
    {% endif %}
  </main>
  <footer>
    {% if refresh_sec > 0 %}Auto-refresh every {{ refresh_sec }}s · {% endif %}
    API: <a href="{{ api_href }}">api/machines</a>
  </footer>
</body>
</html>
"""

_scrape_lock = threading.Lock()
_scrape_rows: list[dict] = []
_scrape_errs: dict[str, str] = {}
_scrape_ts: float = 0.0
_bg_started = False
_bg_lock = threading.Lock()


def _truthy_env(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def _scrape_enabled() -> bool:
    """Live EGM scrape is **on** unless ``WEBMACHINE_SCRAPE`` is explicitly ``0`` / ``false`` / ``no`` / ``off``."""
    return (os.environ.get("WEBMACHINE_SCRAPE") or "").strip().lower() not in ("0", "false", "no", "off")


def _data_json_path() -> Path:
    custom = (os.environ.get("WEBMACHINE_DATA_PATH") or "").strip()
    return Path(custom) if custom else (_ROOT / "webmachine_data.json")


def _ensure_machine_data_file() -> None:
    """Create ``webmachine_data.json`` (empty array) when missing — skipped if ``WEBMACHINE_JSON`` is set."""
    if (os.environ.get("WEBMACHINE_JSON") or "").strip():
        return
    p = _data_json_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    if p.is_file():
        return
    try:
        p.write_text("[\n]\n", encoding="utf-8")
    except OSError:
        pass


def _persist_scrape_to_data_file(rows: list[dict]) -> None:
    """Write latest scrape snapshot for reload / backup; skipped when ``WEBMACHINE_JSON`` inline is used."""
    if (os.environ.get("WEBMACHINE_JSON") or "").strip():
        return
    p = _data_json_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = [
            {
                "environment": r.get("environment"),
                "name": r.get("name"),
                "status": r.get("status"),
                "online": r.get("online_raw") or r.get("online_label"),
                "is_test": bool(r.get("is_test")),
            }
            for r in rows
        ]
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except OSError:
        pass


def _cell(row: dict, *keys: str) -> str:
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return ""


def _online_pill(raw: str) -> tuple[str, str]:
    s = " ".join((raw or "").lower().split())
    if "offline" in s:
        return "Offline", "pill-offline"
    if "online" in s:
        return "Online", "pill-online"
    t = (raw or "").strip() or "—"
    return t, "pill-unknown"


def _infer_is_test(raw: dict, name: str) -> bool:
    v = raw.get("is_test")
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if str(v or "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    n = (name or "").lower()
    return "(test)" in n


def _status_is_maintain(status: str) -> bool:
    return "maintain" in " ".join((status or "").lower().split())


def _normalize_rows(raw: object) -> list[dict]:
    if raw is None:
        return []
    if isinstance(raw, dict):
        inner = raw.get("machines") or raw.get("rows") or raw.get("data")
        if isinstance(inner, list):
            raw = inner
        else:
            return []
    if not isinstance(raw, list):
        return []
    out: list[dict] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        env = _cell(row, "environment", "env", "site", "backend")
        name = _cell(row, "name", "machine", "machine_name", "id")
        status = _cell(row, "status", "machine_status", "state_detail")
        online_raw = _cell(row, "online", "online_offline", "conn", "reachability")
        if not online_raw:
            st = _cell(row, "state")
            if st and st.lower() in ("online", "offline"):
                online_raw = st
        label, pill = _online_pill(online_raw)
        is_test = _infer_is_test(row, name)
        out.append(
            {
                "environment": env or "—",
                "name": name or "—",
                "status": status or "—",
                "online_label": label,
                "online_raw": online_raw or "—",
                "pill_class": pill,
                "is_test": is_test,
            }
        )
    return out


def _compute_stats(rows: list[dict]) -> tuple[dict, list[dict]]:
    """Global counts and per-environment counts (same keys)."""
    g = {"total": 0, "online": 0, "offline": 0, "conn_unknown": 0, "test": 0, "maintain": 0}
    if not rows:
        return g, []
    by_env: dict[str, dict] = {}
    for r in rows:
        env = str(r.get("environment") or "—")
        if env not in by_env:
            by_env[env] = {
                "environment": env,
                "total": 0,
                "online": 0,
                "offline": 0,
                "conn_unknown": 0,
                "test": 0,
                "maintain": 0,
            }
        be = by_env[env]
        g["total"] += 1
        be["total"] += 1
        pc = str(r.get("pill_class") or "")
        if pc == "pill-online":
            g["online"] += 1
            be["online"] += 1
        elif pc == "pill-offline":
            g["offline"] += 1
            be["offline"] += 1
        else:
            g["conn_unknown"] += 1
            be["conn_unknown"] += 1
        if r.get("is_test"):
            g["test"] += 1
            be["test"] += 1
        if _status_is_maintain(str(r.get("status") or "")):
            g["maintain"] += 1
            be["maintain"] += 1
    env_list = sorted(by_env.values(), key=lambda x: str(x["environment"]).lower())
    return g, env_list


def _load_raw_json() -> tuple[list[dict], str]:
    inline = (os.environ.get("WEBMACHINE_JSON") or "").strip()
    if inline:
        try:
            data = json.loads(inline)
        except json.JSONDecodeError as e:
            raise ValueError(f"WEBMACHINE_JSON is not valid JSON: {e}") from e
        return _normalize_rows(data), "WEBMACHINE_JSON"

    _ensure_machine_data_file()
    p = _data_json_path()
    if not p.is_file():
        return [], f"(no file {p.name})"

    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError) as e:
        raise ValueError(f"Cannot read {p}: {e}") from e
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {p}: {e}") from e
    return _normalize_rows(data), str(p)


def _api_auth_ok() -> bool:
    tok = (os.environ.get("WEBMACHINE_API_TOKEN") or "").strip()
    if not tok:
        return True
    auth = (request.headers.get("Authorization") or "").strip()
    return auth == f"Bearer {tok}"


def _run_scrape_once() -> None:
    global _scrape_rows, _scrape_errs, _scrape_ts
    try:
        from smmachine import smachine_collect_machines_multi_sites
    except Exception as e:
        with _scrape_lock:
            _scrape_errs = {"_import": repr(e)}
            _scrape_ts = time.time()
            _scrape_rows = []
        return
    try:
        raw_rows, errs = smachine_collect_machines_multi_sites()
    except Exception as e:
        raw_rows, errs = [], {"_fatal": repr(e)}
    norm = _normalize_rows(raw_rows)
    norm.sort(key=lambda r: ((r.get("environment") or "").lower(), (r.get("name") or "").lower()))
    with _scrape_lock:
        _scrape_rows = norm
        _scrape_errs = errs
        _scrape_ts = time.time()
    _persist_scrape_to_data_file(norm)


def _background_worker() -> None:
    while True:
        if _scrape_enabled():
            try:
                _run_scrape_once()
            except Exception as e:
                with _scrape_lock:
                    _scrape_errs["_worker"] = repr(e)
                    _scrape_ts = time.time()
        try:
            interval = int((os.environ.get("WEBMACHINE_SCRAPE_INTERVAL_SEC") or "900").strip() or "900")
        except ValueError:
            interval = 900
        time.sleep(max(60, interval))


def start_background_scrape_loop() -> None:
    """Start daemon thread that refreshes scrape cache (runs when scrape is not explicitly disabled)."""
    global _bg_started
    if not _scrape_enabled():
        return
    with _bg_lock:
        if _bg_started:
            return
        _bg_started = True
    th = threading.Thread(target=_background_worker, name="webmachine-scrape", daemon=True)
    th.start()


def _display_rows_and_provenance() -> tuple[list[dict], str]:
    """Normalized display rows + short provenance string for the HTML header (only when ``WEBMACHINE_SCRAPE``)."""
    json_rows: list[dict] = []
    json_src = ""
    try:
        json_rows, json_src = _load_raw_json()
    except ValueError:
        json_rows, json_src = [], ""

    if not _scrape_enabled():
        return json_rows, json_src

    with _scrape_lock:
        ts = _scrape_ts
        srows = list(_scrape_rows)
        serrs = dict(_scrape_errs)

    if ts > 0:
        stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
        err_note = ""
        if serrs:
            err_note = f" · site errors: {serrs}"
        if srows:
            return srows, f"EGM scrape @ {stamp}{err_note}"
        return [], f"EGM scrape @ {stamp} (0 rows){err_note}"

    if json_rows:
        return json_rows, f"{json_src} (stale until first scrape)"
    return [], "Waiting for first EGM scrape to finish…"


@wm_bp.get("/healthz")
def healthz():
    with _scrape_lock:
        meta = {"scrape_ts": _scrape_ts, "scrape_row_count": len(_scrape_rows), "scrape_errors": dict(_scrape_errs)}
    return jsonify(ok=True, service="webmachine", **meta)


@wm_bp.get("/api/machines")
def api_machines():
    if not _api_auth_ok():
        return jsonify(error="unauthorized"), 401
    with _scrape_lock:
        scrape_meta = {"ts": _scrape_ts, "errors": dict(_scrape_errs)}
    if not _scrape_enabled():
        try:
            rows, src = _load_raw_json()
        except ValueError as e:
            return jsonify(error=str(e)), 400
        stats, stats_env = _compute_stats(rows)
        return jsonify(
            source=src,
            count=len(rows),
            machines=rows,
            scrape=scrape_meta,
            stats={"global": stats, "by_environment": stats_env},
        )
    rows, src = _display_rows_and_provenance()
    stats, stats_env = _compute_stats(rows)
    return jsonify(
        source=src,
        count=len(rows),
        machines=rows,
        scrape=scrape_meta,
        stats={"global": stats, "by_environment": stats_env},
    )


@wm_bp.get("/")
def index():
    title = (os.environ.get("WEBMACHINE_TITLE") or "Machine status").strip() or "Machine status"
    try:
        refresh_sec = int((os.environ.get("WEBMACHINE_REFRESH_SEC") or "30").strip() or "30")
    except ValueError:
        refresh_sec = 30
    if refresh_sec < 0:
        refresh_sec = 0
    if not _scrape_enabled():
        try:
            rows, src = _load_raw_json()
        except ValueError as e:
            rows, src = [], f"Error: {e}"
    else:
        rows, src = _display_rows_and_provenance()
    stats, stats_env = _compute_stats(rows)
    return render_template_string(
        _PAGE,
        title=title,
        rows=rows,
        row_total=len(rows),
        stats=stats,
        stats_env=stats_env,
        loaded_from=src,
        refresh_sec=refresh_sec,
        api_href=url_for("wm.api_machines"),
    )


def register_webmachine(flask_app: Flask, *, url_prefix: str | None = None) -> None:
    """
    Register routes on an existing Flask app (e.g. Duty ``main.app``).

    ``url_prefix`` ``None`` / ``""`` / ``"/"`` → mount at application root.
    Otherwise use e.g. ``"/wm"`` so the dashboard lives under that prefix.

    Background scraping starts when :func:`start_background_scrape_loop` is called (see ``main.py`` / ``webmachine.main``); scrape is **on by default** unless ``WEBMACHINE_SCRAPE=0``.
    """
    pref = (url_prefix or "").strip()
    if pref in ("", "/"):
        flask_app.register_blueprint(wm_bp)
    else:
        if not pref.startswith("/"):
            pref = "/" + pref
        flask_app.register_blueprint(wm_bp, url_prefix=pref.rstrip("/") or "/")
    _ensure_machine_data_file()


app = Flask(__name__)
register_webmachine(app, url_prefix=None)


def main() -> int:
    try:
        port = int((os.environ.get("WEBMACHINE_PORT") or "8765").strip() or "8765")
    except ValueError:
        print("WEBMACHINE_PORT must be an integer.", file=sys.stderr)
        return 1
    host = (os.environ.get("WEBMACHINE_HOST") or "0.0.0.0").strip() or "0.0.0.0"
    print(
        f"[webmachine] http://{host}:{port}/  (Lark Web app homepage → public https URL that proxies here)\n"
        f"[webmachine] Data: live scrape (default on) + {_ROOT / 'webmachine_data.json'}; WEBMACHINE_SCRAPE=0 to disable",
        flush=True,
    )
    start_background_scrape_loop()
    app.run(host=host, port=port, debug=_truthy_env("WEBMACHINE_DEBUG"), threaded=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
