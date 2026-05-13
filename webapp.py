#!/usr/bin/env python3
"""
Lark **Web app** — lightweight machine status dashboard (standalone or mounted on Duty ``main.py``).

Run standalone::

    python3 webapp.py
    # or: WEBMACHINE_PORT=8765 python3 webapp.py

Mount inside Duty bot (same process, no second ``main.py``) — **on by default**; to disable set ``WEBMACHINE_MOUNT_IN_MAIN=0`` (or ``false`` / ``no`` / ``off``)::

    # optional: WEBMACHINE_MOUNT_IN_MAIN=0   disable dashboard on this app
    # optional: WEBMACHINE_URL_PREFIX=/wm   (default /wm)
    # optional: WEBMACHINE_SCRAPE=0       disable live EGM scrape (default: **on** in code; no .env required)
    # optional: WEBMACHINE_SCRAPE_INTERVAL_SEC=900
    # optional: WEBMACHINE_SITES=nwr,nch,...  (default: ``smmachine.DEFAULT_WEBMACHINE_SITES`` — all backends; CP/OSM share one URL and are deduped)
    # optional: WEBMACHINE_DEPLOYMENTS=prod,qat,uat  (default: all three; QAT/UAT use ``*.osmslot.org`` hosts in ``smmachine``)
    # optional: WEBMACHINE_OSMSLOT_USER / WEBMACHINE_OSMSLOT_PASSWORD  (QAT/UAT login; default admin / 123456)
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

Row keys (first match wins): **environment** (``PROD`` / ``QAT`` / ``UAT``); **belongs** (venue, e.g. ``CP``, ``MDR``);
**name** / ``machine``; **game_type** / ``game``; **status**; **online** / ``online_offline`` / ``state``. Optional boolean **is_test** (or ``(TEST)`` in the name) for dashboard counts.

The HTML dashboard shows **deployment** tabs (PROD / QAT / UAT), **global and per-belongs** totals, and a **search** box; ``GET /api/machines`` includes a **stats** object with the same numbers.

Optional: ``WEBMACHINE_API_TOKEN`` — ``GET /api/machines`` requires ``Authorization: Bearer <token>``.

Env: ``WEBMACHINE_PORT``, ``WEBMACHINE_HOST``, ``WEBMACHINE_TITLE``, ``WEBMACHINE_REFRESH_SEC`` (default **0** = no auto page reload), ``WEBMACHINE_DEBUG``.
"""

from __future__ import annotations

import calendar
import importlib
import json
import os
import requests
import re
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

from flask import Blueprint, Flask, jsonify, redirect, render_template_string, request, session, url_for

wm_bp = Blueprint("wm", __name__)

_ADMIN_LOGIN_FAIL_LOCK = threading.Lock()
_ADMIN_LOGIN_FAIL_BY_IP: dict[str, int] = {}
_ADMIN_ALERT_CHAT_ID = "oc_9de3d63fc589df6feeb9b0bee9c45b72"
_ADMIN_ALERT_AT_USER = "ou_5f660c0fb0769d184aca635d02209272"


def _webapp_client_ip() -> str:
    xff = (request.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return (request.remote_addr or "").strip() or "unknown"


def _track_admin_login_failure() -> int:
    ip = _webapp_client_ip()
    with _ADMIN_LOGIN_FAIL_LOCK:
        _ADMIN_LOGIN_FAIL_BY_IP[ip] = int(_ADMIN_LOGIN_FAIL_BY_IP.get(ip, 0)) + 1
        n = _ADMIN_LOGIN_FAIL_BY_IP[ip]
    if n > 0 and n % 5 == 0:
        _send_admin_login_bruteforce_lark(ip, n)
    return n


def _clear_admin_login_failures_for_current_ip() -> None:
    ip = _webapp_client_ip()
    with _ADMIN_LOGIN_FAIL_LOCK:
        _ADMIN_LOGIN_FAIL_BY_IP.pop(ip, None)


def _send_admin_login_bruteforce_lark(ip: str, total_failures: int) -> None:
    try:
        import ose_Duty as od

        token = od.get_tenant_access_token()
        text = (
            f'<at user_id="{_ADMIN_ALERT_AT_USER}"></at> '
            f"Webapp admin login: {total_failures} failed attempt(s) from IP {ip}."
        )
        url = "https://open.larksuite.com/open-apis/im/v1/messages"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {
            "receive_id": _ADMIN_ALERT_CHAT_ID,
            "msg_type": "text",
            "content": json.dumps({"text": text}),
        }
        requests.post(url, headers=headers, params={"receive_id_type": "chat_id"}, json=body, timeout=20)
    except Exception:
        pass


def _admin_session_who() -> str:
    return (session.get("admin_whologin") or "").strip()


def _ensure_admin_session_redirect():
    if not _admin_session_who():
        return redirect(url_for("wm.admin_login"))
    return None


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
    }
    .wm-header-bar {
      display: flex; flex-wrap: wrap; align-items: flex-start; justify-content: space-between; gap: 1rem;
    }
    a.wm-head-nav-btn {
      text-decoration: none; display: inline-block; flex-shrink: 0; align-self: flex-start;
    }
    a.wm-head-nav-btn:hover { text-decoration: none; }
    .wm-head-nav-group { display: flex; flex-wrap: wrap; align-items: center; justify-content: flex-end; gap: 0.5rem; flex-shrink: 0; }
    a.wm-head-nav-btn.active { border-color: var(--accent); background: rgba(59,130,246,.18); color: var(--text); }
    a.wm-admin-btn {
      border-color: #b91c1c !important; background: #dc2626 !important; color: #fff !important;
    }
    a.wm-admin-btn:hover {
      border-color: #991b1b !important; background: #b91c1c !important; color: #fff !important;
      text-decoration: none;
    }
    .wm-head-top { display: flex; flex-direction: column; align-items: flex-start; gap: 0.55rem; }
    .wm-head-title { margin: 0; font-size: 1.35rem; font-weight: 700; letter-spacing: -0.02em; }
    .wm-head-title-btn {
      margin: 0; cursor: pointer; font: inherit;
      font-size: 1.2rem; font-weight: 650; letter-spacing: -0.02em; color: var(--text);
      padding: 0.5rem 1rem; border-radius: 10px; border: 1px solid var(--line);
      background: var(--elev);
      transition: border-color .15s, background .15s, box-shadow .15s;
    }
    .wm-head-title-btn:hover {
      border-color: var(--accent); background: rgba(59,130,246,.12);
      box-shadow: 0 0 0 3px rgba(59,130,246,.15);
    }
    .wm-head-title-btn:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
    .wm-head-total { margin: 0; font-size: 0.92rem; color: var(--muted); line-height: 1.4; }
    .wm-head-total .count { font-weight: 700; color: var(--text); }
    .wm-head-updated { margin: 0.25rem 0 0; font-size: 0.82rem; color: var(--muted); line-height: 1.35; }
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
    .deployment-filter-bar {
      display: flex; flex-wrap: wrap; align-items: center; gap: 0.45rem;
    }
    .env-filter-bar {
      display: flex; flex-wrap: wrap; align-items: center; gap: 0.45rem; margin-bottom: 0.75rem;
    }
    .row-filter-bar { margin-bottom: 0.65rem; }
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
  <header class="wm-header-bar">
    <div class="wm-head-top">
      <h1 class="wm-head-title">{{ title }}</h1>
      <p class="wm-head-total">Total <span class="count">{{ row_total }}</span> of machines detected</p>
      <p class="wm-head-updated">Last Updated : {{ last_updated }}</p>
    </div>
    <div class="wm-head-nav-group">
      <a class="wm-head-title-btn wm-head-nav-btn active" href="{{ machine_status_href }}">Machine status</a>
      <a class="wm-head-title-btn wm-head-nav-btn" href="{{ all_duty_href }}">All Duty</a>
      <a class="wm-head-title-btn wm-head-nav-btn" href="{{ machine_encoder_href }}">Machine encoder</a>
      <a class="wm-head-title-btn wm-admin-btn" href="{{ admin_login_href }}">Admin Page</a>
    </div>
  </header>
  <main id="wm-main">
    {% if rows %}
    <section class="panel" aria-label="Deployment tier">
      <div class="deployment-filter-bar" id="wm-dep-filters" role="toolbar" aria-label="Deployment tier">
        <button type="button" class="env-filter-btn" data-wm-deployment="ALL">ALL</button>
        <button type="button" class="env-filter-btn active" data-wm-deployment="PROD">PROD</button>
        <button type="button" class="env-filter-btn" data-wm-deployment="QAT">QAT</button>
        <button type="button" class="env-filter-btn" data-wm-deployment="UAT">UAT</button>
      </div>
    </section>
    <section class="panel" aria-label="All environments summary">
      <h2 class="panel-title">All environments</h2>
      <div class="summary-grid" id="wm-summary-global">
        <div class="stat-card total"><div class="num" data-wm-stat="total">{{ stats.total }}</div><div class="lbl">Total</div></div>
        <div class="stat-card ok"><div class="num" data-wm-stat="online">{{ stats.online }}</div><div class="lbl">Online</div></div>
        <div class="stat-card bad"><div class="num" data-wm-stat="offline">{{ stats.offline }}</div><div class="lbl">Offline</div></div>
        <div class="stat-card"><div class="num" data-wm-stat="conn_unknown">{{ stats.conn_unknown }}</div><div class="lbl">Conn unknown</div></div>
        <div class="stat-card warn"><div class="num" data-wm-stat="test">{{ stats.test }}</div><div class="lbl">Test mode</div></div>
        <div class="stat-card"><div class="num" data-wm-stat="maintain">{{ stats.maintain }}</div><div class="lbl">Maintain</div></div>
      </div>
    </section>
    <section class="panel" aria-label="Per belongs summary">
      <h2 class="panel-title">By belongs</h2>
      <div class="env-scroll" id="wm-belongs-cards">
        {% for e in stats_env %}
        <div class="env-card" data-wm-belongs-card="{{ e.belongs|e }}">
          <h3>{{ e.belongs }}</h3>
          <div class="env-mini">
            <span>Total</span><b data-wm-belongs-stat="total">{{ e.total }}</b>
            <span>Online</span><b data-wm-belongs-stat="online">{{ e.online }}</b>
            <span>Offline</span><b data-wm-belongs-stat="offline">{{ e.offline }}</b>
            <span>Unknown</span><b data-wm-belongs-stat="conn_unknown">{{ e.conn_unknown }}</b>
            <span>Test</span><b data-wm-belongs-stat="test">{{ e.test }}</b>
            <span>Maintain</span><b data-wm-belongs-stat="maintain">{{ e.maintain }}</b>
          </div>
        </div>
        {% endfor %}
      </div>
    </section>
    <div class="env-filter-bar" id="wm-env-filters" role="toolbar" aria-label="Filter by belongs">
      <button type="button" class="env-filter-btn active" data-wm-belongs="" id="wm-env-all">Show all</button>
      {% for e in stats_env %}
      <button type="button" class="env-filter-btn" data-wm-belongs="{{ e.belongs|e }}">{{ e.belongs }}</button>
      {% endfor %}
    </div>
    <div class="env-filter-bar row-filter-bar" id="wm-row-filters" role="toolbar" aria-label="Optional row filters (multi-select)">
      <button type="button" class="env-filter-btn active" data-wm-clear="1" id="wm-row-all">Show all</button>
      <button type="button" class="env-filter-btn" data-wm-toggle="test">Test</button>
      <button type="button" class="env-filter-btn" data-wm-toggle="maintain">Maintain</button>
      <button type="button" class="env-filter-btn" data-wm-toggle="offline">Offline</button>
      <button type="button" class="env-filter-btn" data-wm-toggle="online">Online</button>
    </div>
    <div class="toolbar">
      <input type="search" id="wm-search" placeholder="Search belongs, machine, game type, status, online…" autocomplete="off" aria-label="Filter machines"/>
      <span class="filter-hint" id="wm-filter-hint"></span>
    </div>
    <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Environment</th>
          <th>Belongs</th>
          <th>Machine name</th>
          <th>Game Type</th>
          <th>Test</th>
          <th>Status</th>
          <th>Online / Offline</th>
        </tr>
      </thead>
      <tbody id="wm-tbody">
        {% for r in rows %}
        <tr data-deployment="{{ r.environment|e }}" data-belongs="{{ r.belongs|e }}" data-name="{{ r.name|e }}" data-game-type="{{ r.game_type|e }}" data-status="{{ r.status|e }}" data-online="{{ r.online_label|e }}"
            data-test="{% if r.is_test %}1{% else %}0{% endif %}"
            data-maint="{% if 'maintain' in ((r.status or '')|lower) %}1{% else %}0{% endif %}"
            data-offline="{% if r.pill_class == 'pill-offline' %}1{% else %}0{% endif %}"
            data-online1="{% if r.pill_class == 'pill-online' %}1{% else %}0{% endif %}">
          <td>{{ r.environment }}</td>
          <td>{{ r.belongs }}</td>
          <td><strong>{{ r.name }}</strong></td>
          <td>{{ r.game_type }}</td>
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
      var depBar = document.getElementById("wm-dep-filters");
      var rowBar = document.getElementById("wm-row-filters");
      var summary = document.getElementById("wm-summary-global");
      var belongsCards = document.getElementById("wm-belongs-cards");
      if (!tbody) return;
      var depSel = "PROD";
      var belongsSel = "";
      var filt = { test: false, maintain: false, offline: false, online: false };

      function anyRowFilt() {
        return filt.test || filt.maintain || filt.offline || filt.online;
      }

      function matchesDeployment(tr) {
        if (!depSel || depSel === "ALL") return true;
        return (tr.getAttribute("data-deployment") || "") === depSel;
      }

      function matchesBelongs(tr) {
        if (!belongsSel) return true;
        return (tr.getAttribute("data-belongs") || "") === belongsSel;
      }

      function matchesRowKind(tr) {
        if (!anyRowFilt()) return true;
        var t = tr.getAttribute("data-test") || "0";
        var m = tr.getAttribute("data-maint") || "0";
        var off = tr.getAttribute("data-offline") || "0";
        var on1 = tr.getAttribute("data-online1") || "0";
        if (filt.test && t !== "1") return false;
        if (filt.maintain && m !== "1") return false;
        if (filt.offline && off !== "1") return false;
        if (filt.online && on1 !== "1") return false;
        return true;
      }

      function syncRowBarActive() {
        if (!rowBar) return;
        var clearBtn = rowBar.querySelector("#wm-row-all");
        rowBar.querySelectorAll("[data-wm-toggle]").forEach(function (b) {
          var k = b.getAttribute("data-wm-toggle");
          if (k && Object.prototype.hasOwnProperty.call(filt, k)) {
            b.classList.toggle("active", !!filt[k]);
          }
        });
        if (clearBtn) clearBtn.classList.toggle("active", !anyRowFilt());
      }

      function rowVisible(tr, term) {
        var hay = (tr.getAttribute("data-belongs") || "") + " " + (tr.getAttribute("data-name") || "") + " " +
          (tr.getAttribute("data-game-type") || "") + " " +
          (tr.getAttribute("data-status") || "") + " " + (tr.getAttribute("data-online") || "");
        hay = hay.toLowerCase();
        var textOk = !term || hay.indexOf(term) !== -1;
        return textOk && matchesDeployment(tr) && matchesBelongs(tr) && matchesRowKind(tr);
      }

      function updateSummary() {
        var rows = tbody.querySelectorAll("tr");
        var g = { total: 0, online: 0, offline: 0, conn_unknown: 0, test: 0, maintain: 0 };
        var byBelongs = {};
        for (var i = 0; i < rows.length; i++) {
          var tr = rows[i];
          if (!matchesDeployment(tr)) continue;
          var belongs = tr.getAttribute("data-belongs") || "—";
          if (!byBelongs[belongs]) {
            byBelongs[belongs] = { total: 0, online: 0, offline: 0, conn_unknown: 0, test: 0, maintain: 0 };
          }
          var bb = byBelongs[belongs];
          g.total += 1;
          bb.total += 1;
          var off = tr.getAttribute("data-offline") === "1";
          var on1 = tr.getAttribute("data-online1") === "1";
          if (on1) { g.online += 1; bb.online += 1; }
          else if (off) { g.offline += 1; bb.offline += 1; }
          else { g.conn_unknown += 1; bb.conn_unknown += 1; }
          if (tr.getAttribute("data-test") === "1") { g.test += 1; bb.test += 1; }
          if (tr.getAttribute("data-maint") === "1") { g.maintain += 1; bb.maintain += 1; }
        }
        if (summary) {
          summary.querySelectorAll("[data-wm-stat]").forEach(function (el) {
            var k = el.getAttribute("data-wm-stat");
            if (k && Object.prototype.hasOwnProperty.call(g, k)) el.textContent = String(g[k]);
          });
        }
        if (belongsCards) {
          belongsCards.querySelectorAll("[data-wm-belongs-card]").forEach(function (card) {
            var key = card.getAttribute("data-wm-belongs-card") || "";
            var stats = byBelongs[key] || { total: 0, online: 0, offline: 0, conn_unknown: 0, test: 0, maintain: 0 };
            card.querySelectorAll("[data-wm-belongs-stat]").forEach(function (el) {
              var k = el.getAttribute("data-wm-belongs-stat");
              if (k && Object.prototype.hasOwnProperty.call(stats, k)) el.textContent = String(stats[k]);
            });
            card.style.display = stats.total ? "" : "none";
          });
        }
        if (bar) {
          bar.querySelectorAll("[data-wm-belongs]").forEach(function (btn) {
            var key = btn.getAttribute("data-wm-belongs") || "";
            if (!key) return;
            var stats = byBelongs[key];
            btn.style.display = stats && stats.total ? "" : "none";
          });
        }
      }

      function apply() {
        var term = (inp && inp.value) ? inp.value.trim().toLowerCase() : "";
        var rows = tbody.querySelectorAll("tr");
        var total = rows.length;
        var n = 0;
        for (var i = 0; i < rows.length; i++) {
          var tr = rows[i];
          var show = rowVisible(tr, term);
          tr.style.display = show ? "" : "none";
          if (show) n++;
        }
        updateSummary();
        if (hint) {
          if (!belongsSel && !term && !anyRowFilt()) {
            hint.textContent = "";
          } else {
            var label = [];
            if (depSel) label.push(depSel);
            if (belongsSel) label.push("belongs: " + belongsSel);
            if (filt.test) label.push("test");
            if (filt.maintain) label.push("maintain");
            if (filt.offline) label.push("offline");
            if (filt.online) label.push("online");
            if (term) label.push("search");
            hint.textContent = "Showing " + n + " of " + total + " (" + label.join(", ") + ")";
          }
        }
      }

      if (depBar) {
        depBar.addEventListener("click", function (ev) {
          var btn = ev.target.closest("[data-wm-deployment]");
          if (!btn || !depBar.contains(btn)) return;
          depSel = btn.getAttribute("data-wm-deployment") || "PROD";
          belongsSel = "";
          depBar.querySelectorAll(".env-filter-btn").forEach(function (b) {
            b.classList.toggle("active", b === btn);
          });
          if (bar) {
            var allBtn = bar.querySelector("#wm-env-all");
            bar.querySelectorAll(".env-filter-btn").forEach(function (b) {
              b.classList.toggle("active", b === allBtn);
            });
          }
          apply();
        });
      }
      if (bar) {
        bar.addEventListener("click", function (ev) {
          var btn = ev.target.closest("[data-wm-belongs]");
          if (!btn || !bar.contains(btn)) return;
          belongsSel = btn.getAttribute("data-wm-belongs") || "";
          bar.querySelectorAll(".env-filter-btn").forEach(function (b) {
            b.classList.toggle("active", b === btn);
          });
          apply();
        });
      }
      if (rowBar) {
        syncRowBarActive();
        rowBar.addEventListener("click", function (ev) {
          var btn = ev.target.closest("button");
          if (!btn || !rowBar.contains(btn)) return;
          if (btn.getAttribute("data-wm-clear") === "1") {
            filt.test = filt.maintain = filt.offline = filt.online = false;
            syncRowBarActive();
            apply();
            return;
          }
          var k = btn.getAttribute("data-wm-toggle");
          if (!k || !Object.prototype.hasOwnProperty.call(filt, k)) return;
          filt[k] = !filt[k];
          syncRowBarActive();
          apply();
        });
      }
      if (inp) {
        inp.addEventListener("input", apply);
        inp.addEventListener("search", apply);
      }
      apply();
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

_VALID_DUTY_KINDS = frozenset({"ote", "ose", "fpms", "ft", "fe", "bi", "cpms", "db", "sre"})


def _duty_month_label(year: int, month: int) -> str:
    names = (
        "",
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    )
    return f"{names[month]} {year}"


def _duty_weeks_from_cells(year: int, month: int, day_cells: dict[int, dict]) -> list[list[dict | None]]:
    out: list[list[dict | None]] = []
    for row in calendar.monthcalendar(year, month):
        line: list[dict | None] = []
        for d in row:
            line.append(None if d == 0 else day_cells.get(d))
        out.append(line)
    return out


def _duty_calendar_fetch_live(kind: str, year: int, month: int) -> dict[str, object]:
    """Load one duty calendar month directly from roster sources (no ``allduty.json``)."""
    kind = (kind or "ose").strip().lower()
    base: dict[str, object] = {"ok": False, "kind": kind, "year": year, "month": month, "weeks": []}
    if kind not in _VALID_DUTY_KINDS:
        base["error"] = "Unknown kind (valid: " + ", ".join(sorted(_VALID_DUTY_KINDS)) + ")"
        return base
    if month < 1 or month > 12:
        base["error"] = "month must be 1–12"
        return base
    month_label = _duty_month_label(year, month)
    _, last_day = calendar.monthrange(year, month)

    try:
        if kind == "ose":
            import ose_Duty as od

            out = od.get_ose_month_calendar(year, month)
            if isinstance(out, dict):
                out = dict(out)
                out.setdefault("kind", "ose")
                out["layout"] = "ose"
            return out
        if kind == "ote":
            import ote_duty as od

            vals, rows, err = od.get_values_and_targets_for_year(year)
            if err:
                return {**base, "error": err, "month_label": month_label}
            if not rows:
                return {**base, "error": "No OTE section in sheet", "month_label": month_label}
            cells: dict[int, dict] = {}
            for d in range(1, last_day + 1):
                dt = date(year, month, d)
                names = sorted(od.get_duty_for_date(dt, vals, rows))
                people: list[dict[str, str]] = []
                for name in names:
                    lookup = next((t["lookup"] for t in od.TARGET_DUTY if t["display"] == name), name)
                    people.append({"name": name, "phone": od.get_phone_from_dutylist(lookup)})
                cells[d] = {"day": d, "duty_people": people}
            return {
                "ok": True,
                "error": None,
                "kind": "ote",
                "layout": "single",
                "year": year,
                "month": month,
                "month_label": month_label,
                "weeks": _duty_weeks_from_cells(year, month, cells),
            }
        if kind == "db":
            import db_duty as od

            vals, rows, err = od.get_values_and_targets_for_year(year)
            if err:
                return {**base, "error": err, "month_label": month_label}
            if not rows:
                return {**base, "error": "No DB duty section in sheet", "month_label": month_label}
            cells = {}
            for d in range(1, last_day + 1):
                dt = date(year, month, d)
                names = sorted(od.get_duty_for_date(dt, vals, rows))
                people = []
                for name in names:
                    ph = next((t["phone"] for t in od.TARGET_DUTY if t["name"] == name), "未找到电话号码")
                    people.append({"name": name, "phone": ph})
                cells[d] = {"day": d, "duty_people": people}
            return {
                "ok": True,
                "error": None,
                "kind": "db",
                "layout": "single",
                "year": year,
                "month": month,
                "month_label": month_label,
                "weeks": _duty_weeks_from_cells(year, month, cells),
            }
        if kind == "fpms":
            import fpms_duty as fd

            dm = fd.get_month_duty_map(year, month)
            if dm is None:
                return {**base, "error": "Could not load FPMS sheet for this month", "month_label": month_label}
            cells = {}
            for d in range(1, last_day + 1):
                names = sorted(dm.get(d, []))
                people = [{"name": n, "phone": fd.get_phone(n)} for n in names]
                cells[d] = {"day": d, "duty_people": people}
            return {
                "ok": True,
                "error": None,
                "kind": "fpms",
                "layout": "single",
                "year": year,
                "month": month,
                "month_label": month_label,
                "weeks": _duty_weeks_from_cells(year, month, cells),
            }
        if kind == "ft":
            import ft as ftmod

            token = ftmod.get_tenant_access_token()
            records = ftmod.get_table_records(token, ftmod.BASE_ID, ftmod.TABLE_ID)
            phone_map = ftmod.load_phone_map(ftmod.DUTY_LIST_PATH)
            cells = {}
            for d in range(1, last_day + 1):
                dt = date(year, month, d)
                names: list[str] = []
                for rec in records:
                    if ftmod.extract_date_from_record(rec) == dt:
                        names.extend(ftmod.extract_names_from_record(rec))
                seen: set[str] = set()
                uniq: list[str] = []
                for n in names:
                    if n not in seen:
                        seen.add(n)
                        uniq.append(n)
                people = [{"name": n, "phone": phone_map.get(n, "Not found")} for n in sorted(uniq)]
                cells[d] = {"day": d, "duty_people": people}
            return {
                "ok": True,
                "error": None,
                "kind": "ft",
                "layout": "single",
                "year": year,
                "month": month,
                "month_label": month_label,
                "weeks": _duty_weeks_from_cells(year, month, cells),
            }
        if kind == "fe":
            import fe_duty as fe

            token = fe.get_tenant_access_token()
            values = fe.get_sheet_values(token)
            if not values or len(values) < 31:
                return {
                    **base,
                    "error": "Insufficient FE sheet data (expected 31 day cells)",
                    "month_label": month_label,
                }
            cells = {}
            for d in range(1, last_day + 1):
                raw = values[d - 1] if d - 1 < len(values) else ""
                raw_s = str(raw).strip() if raw is not None else ""
                pairs = fe.parse_names_and_get_phones(raw_s) if raw_s else []
                people = [{"name": p[0], "phone": p[1]} for p in pairs]
                cells[d] = {"day": d, "duty_people": people}
            return {
                "ok": True,
                "error": None,
                "kind": "fe",
                "layout": "single",
                "year": year,
                "month": month,
                "month_label": month_label,
                "weeks": _duty_weeks_from_cells(year, month, cells),
            }
        if kind == "bi":
            import bi_duty as bi

            token = bi.get_tenant_access_token()
            rows = bi._get_bi_duty_rows(month, year, token)
            cells = {}
            for d in range(1, last_day + 1):
                dt = date(year, month, d)
                names = sorted({name for (sd, ed, name) in rows if sd <= dt <= ed})
                people = [{"name": n, "phone": bi.get_phone(n)} for n in names]
                cells[d] = {"day": d, "duty_people": people}
            return {
                "ok": True,
                "error": None,
                "kind": "bi",
                "layout": "single",
                "year": year,
                "month": month,
                "month_label": month_label,
                "weeks": _duty_weeks_from_cells(year, month, cells),
            }
        if kind == "cpms":
            import cpms_duty as cp

            try:
                weekday_map = cp.get_cpms_weekday_duty_map(year, month)
            except Exception as e:
                return {**base, "error": str(e), "month_label": month_label}
            cells = {}
            for d in range(1, last_day + 1):
                dt = date(year, month, d)
                weekday = dt.strftime("%A")
                main, mph, backup, bph = weekday_map.get(weekday, ("", "", "", ""))
                cells[d] = {
                    "day": d,
                    "main": main or "",
                    "main_phone": mph or "",
                    "backup": backup or "",
                    "backup_phone": bph or "",
                }
            return {
                "ok": True,
                "error": None,
                "kind": "cpms",
                "layout": "cpms",
                "year": year,
                "month": month,
                "month_label": month_label,
                "weeks": _duty_weeks_from_cells(year, month, cells),
            }
        if kind == "sre":
            import sre_Duty as sre

            token = sre.get_tenant_access_token()
            props = sre.get_sheet_metadata(token, sre.SPREADSHEET_TOKEN, sre.SHEET_ID)
            if not props:
                return {**base, "error": "Cannot retrieve SRE sheet metadata", "month_label": month_label}
            max_row = props.get("rowCount", 200)
            scan_range = f"A1:ZZ{max_row}"
            values = sre.get_range_values(token, sre.SPREADSHEET_TOKEN, sre.SHEET_ID, scan_range)
            if values is None:
                return {**base, "error": "Failed to read SRE sheet data", "month_label": month_label}
            if len(values) < 2:
                return {**base, "error": "SRE sheet has fewer than 2 rows", "month_label": month_label}
            cells = {}
            for d in range(1, last_day + 1):
                dt = date(year, month, d)
                raw_names = sre._get_duty_names_for_date(dt, values)
                people = []
                for name in sorted(raw_names):
                    csv_name = sre.TABLE_TO_CSV.get(name, name)
                    phone = sre.get_phone_from_dutylist(csv_name)
                    proj = (sre.NAME_TO_PROJECT.get(name) or "").strip()
                    people.append({"name": name, "phone": phone, "subtitle": proj})
                cells[d] = {"day": d, "duty_people": people}
            return {
                "ok": True,
                "error": None,
                "kind": "sre",
                "layout": "single",
                "year": year,
                "month": month,
                "month_label": month_label,
                "weeks": _duty_weeks_from_cells(year, month, cells),
            }
    except Exception as e:
        return {**base, "error": str(e), "month_label": month_label}
    return {**base, "error": f"Unhandled kind: {kind!r}", "month_label": month_label}


_DUTY_CACHE_SEC = int(os.getenv("WEBMACHINE_DUTY_CACHE_SEC", "3600"))
_duty_lock = threading.Lock()
_DUTY_MEMORY: dict[str, object] = {"entries": {}}


def _duty_json_path() -> Path:
    custom = (os.environ.get("WEBMACHINE_DUTY_DATA_PATH") or "").strip()
    return Path(custom) if custom else (_ROOT / "allduty.json")


def _duty_cache_key(kind: str, year: int, month: int) -> str:
    return f"{(kind or 'ose').strip().lower()}:{year}:{month}"


def _duty_entry_fingerprint(payload: dict[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)


def _duty_entry_updated_wall(entry: dict[str, object], fallback: float) -> float:
    raw = entry.get("updated_at")
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    return float(fallback)


def _load_duty_snapshot() -> dict[str, object] | None:
    path = _duty_json_path()
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    calendars = raw.get("calendars")
    if not isinstance(calendars, dict):
        raw["calendars"] = {}
    return raw


def _duty_snapshot_entry(
    snapshot: dict[str, object], key: str
) -> tuple[dict[str, object] | None, float | None]:
    calendars = snapshot.get("calendars")
    if not isinstance(calendars, dict):
        return None, None
    entry = calendars.get(key)
    if isinstance(entry, dict) and isinstance(entry.get("data"), dict):
        path = _duty_json_path()
        fallback = path.stat().st_mtime if path.is_file() else time.time()
        return dict(entry["data"]), _duty_entry_updated_wall(entry, fallback)
    if isinstance(entry, dict) and ("weeks" in entry or "ok" in entry):
        path = _duty_json_path()
        fallback = path.stat().st_mtime if path.is_file() else time.time()
        return dict(entry), _duty_entry_updated_wall(entry, fallback)
    return None, None


def _persist_duty_snapshot(snapshot: dict[str, object]) -> None:
    path = _duty_json_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        snapshot["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except OSError:
        pass


def _duty_memory_set(key: str, payload: dict[str, object]) -> None:
    with _duty_lock:
        entries = _DUTY_MEMORY.get("entries")
        if not isinstance(entries, dict):
            entries = {}
            _DUTY_MEMORY["entries"] = entries
        entries[key] = {"mono": time.monotonic(), "data": dict(payload)}


def _duty_memory_get(key: str) -> dict[str, object] | None:
    with _duty_lock:
        entries = _DUTY_MEMORY.get("entries")
        if not isinstance(entries, dict):
            return None
        entry = entries.get(key)
        if not isinstance(entry, dict):
            return None
        if time.monotonic() - float(entry.get("mono") or 0.0) >= _DUTY_CACHE_SEC:
            return None
        data = entry.get("data")
        return dict(data) if isinstance(data, dict) else None


def duty_calendar_payload(
    kind: str, year: int, month: int, *, force_refresh: bool = False
) -> dict[str, object]:
    """JSON for All Duty calendar; served from ``allduty.json`` when fresh."""
    kind = (kind or "ose").strip().lower()
    key = _duty_cache_key(kind, year, month)
    now_wall = time.time()

    if not force_refresh:
        cached = _duty_memory_get(key)
        if cached is not None:
            return cached
        snapshot = _load_duty_snapshot()
        if snapshot is not None:
            payload, updated_at = _duty_snapshot_entry(snapshot, key)
            if payload is not None and updated_at is not None and now_wall - updated_at < _DUTY_CACHE_SEC:
                _duty_memory_set(key, payload)
                return payload

    snapshot = _load_duty_snapshot()
    previous_payload: dict[str, object] | None = None
    if snapshot is not None:
        previous_payload, _ = _duty_snapshot_entry(snapshot, key)

    if force_refresh and kind == "ose":
        import ose_Duty as od

        od.invalidate_ose_bitable_cache()

    try:
        live = _duty_calendar_fetch_live(kind, year, month)
    except Exception as e:
        if previous_payload is not None:
            _duty_memory_set(key, previous_payload)
            return previous_payload
        return {
            "ok": False,
            "kind": kind,
            "year": year,
            "month": month,
            "weeks": [],
            "error": str(e),
            "month_label": _duty_month_label(year, month),
        }

    if not live.get("ok") and previous_payload is not None:
        _duty_memory_set(key, previous_payload)
        return previous_payload

    _duty_memory_set(key, live)
    previous_fp = _duty_entry_fingerprint(previous_payload) if previous_payload is not None else None
    current_fp = _duty_entry_fingerprint(live)
    if previous_fp != current_fp:
        if snapshot is None:
            snapshot = {"calendars": {}}
        calendars = snapshot.setdefault("calendars", {})
        if not isinstance(calendars, dict):
            calendars = {}
            snapshot["calendars"] = calendars
        calendars[key] = {
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "data": live,
        }
        _persist_duty_snapshot(snapshot)
    return live


_ENCODER_SITE_DEFS: tuple[dict[str, str], ...] = (
    {"key": "cp", "label": "CP", "module": "cp", "prefix": "OSM", "id_keywords": "assets number"},
    {"key": "dhs", "label": "DHS", "module": "dhs", "prefix": "DHS", "id_keywords": "asset id"},
    {"key": "mdr", "label": "MDR", "module": "mdr", "prefix": "MDR", "id_keywords": "asset id"},
    {"key": "nch", "label": "NCH", "module": "nch", "prefix": "NCH", "id_keywords": "asset id"},
    {"key": "nwr", "label": "NP", "module": "nwr", "prefix": "NWR", "id_keywords": "asset"},
    {"key": "tbp", "label": "TBP", "module": "tbp", "prefix": "TBP", "id_keywords": "machine id"},
    {"key": "wf", "label": "WF", "module": "winford", "prefix": "WF", "id_keywords": "asset"},
)

_ENCODER_CACHE: dict[str, object] = {"mono": 0.0, "items": None, "errors": {}}
_ENCODER_CACHE_SEC = int(os.getenv("WEBMACHINE_ENCODER_CACHE_SEC", "3600"))
_encoder_lock = threading.Lock()


def _encoder_json_path() -> Path:
    custom = (os.environ.get("WEBMACHINE_ENCODER_DATA_PATH") or "").strip()
    return Path(custom) if custom else (_ROOT / "machineencoder.json")


def _encoder_snapshot_fingerprint(items: list[dict[str, object]], errors: dict[str, str]) -> str:
    return json.dumps({"items": items, "errors": errors}, sort_keys=True, ensure_ascii=False)


def _load_encoder_snapshot() -> tuple[list[dict[str, object]], dict[str, str], float] | None:
    path = _encoder_json_path()
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if isinstance(raw, list):
        items = raw
        errors: dict[str, str] = {}
        updated_at = path.stat().st_mtime
    elif isinstance(raw, dict):
        items = raw.get("items")
        errors = raw.get("errors") or {}
        if not isinstance(items, list):
            return None
        if not isinstance(errors, dict):
            errors = {}
        updated_raw = raw.get("updated_at") or raw.get("updated")
        updated_at = path.stat().st_mtime
        if isinstance(updated_raw, (int, float)):
            updated_at = float(updated_raw)
        elif isinstance(updated_raw, str) and updated_raw.strip():
            try:
                updated_at = datetime.fromisoformat(updated_raw.replace("Z", "+00:00")).timestamp()
            except ValueError:
                pass
    else:
        return None
    return list(items), {str(k): str(v) for k, v in errors.items()}, float(updated_at)


def _persist_encoder_json(items: list[dict[str, object]], errors: dict[str, str]) -> None:
    path = _encoder_json_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "items": items,
            "errors": errors,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except OSError:
        pass


def _encoder_fetch_from_sheets(
    previous_items: list[dict[str, object]] | None = None,
) -> tuple[list[dict[str, object]], dict[str, str]]:
    items: list[dict[str, object]] = []
    errors: dict[str, str] = {}
    previous_by_site: dict[str, list[dict[str, object]]] = {}
    if previous_items:
        for item in previous_items:
            if not isinstance(item, dict):
                continue
            site_key = str(item.get("site") or "").strip()
            if not site_key:
                continue
            previous_by_site.setdefault(site_key, []).append(item)
    for spec in _ENCODER_SITE_DEFS:
        key = spec["key"]
        try:
            mod = importlib.import_module(spec["module"])
            if key == "nwr":
                part = _encoder_rows_nwr(mod)
            elif key == "wf":
                part = _encoder_rows_winford(mod)
            else:
                part = _encoder_rows_from_columns_module(mod, spec)
            if not part and previous_by_site.get(key):
                items.extend(previous_by_site[key])
                errors[key] = "Refresh returned no encoder rows for this site; kept the previous snapshot."
            else:
                items.extend(part)
        except Exception as e:
            errors[key] = str(e)
            if previous_by_site.get(key):
                items.extend(previous_by_site[key])
    items.sort(key=lambda r: (str(r.get("site_label") or ""), str(r.get("machine") or "")))
    return items, errors


def _encoder_header_rows(
    data: list[list[object]], keywords: tuple[str, ...]
) -> list[tuple[int, list[object]]]:
    blocks: list[tuple[int, list[object]]] = []
    for i, row in enumerate(data or []):
        if not row:
            continue
        for cell in row:
            s = str(cell or "").strip().lower()
            if any(k in s for k in keywords):
                blocks.append((i, row))
                break
    return blocks


def _encoder_locate_header_row(data: list[list[object]], keywords: tuple[str, ...]) -> tuple[int | None, list[object]]:
    blocks = _encoder_header_rows(data, keywords)
    if not blocks:
        return None, []
    return blocks[0]


def _encoder_map_columns(headers: list[object], columns: dict[str, str], id_keywords: tuple[str, ...]) -> tuple[dict[str, int], int | None]:
    col_indexes: dict[str, int] = {}
    for col_name in columns.values():
        col_indexes[col_name] = -1
        for idx, cell in enumerate(headers):
            if cell and isinstance(cell, str) and col_name.lower() in cell.lower():
                col_indexes[col_name] = idx
                break
    asset_col = None
    for idx, cell in enumerate(headers):
        if not cell or not isinstance(cell, str):
            continue
        s = cell.strip().lower()
        if any(k in s for k in id_keywords):
            asset_col = idx
            break
    return col_indexes, asset_col


def _encoder_item_from_row(
    *,
    site_key: str,
    site_label: str,
    machine_prefix: str,
    row: list[object],
    col_indexes: dict[str, int],
    asset_col: int,
    extract_cell_value,
) -> dict[str, object] | None:
    if asset_col is None or asset_col < 0 or len(row) <= asset_col:
        return None
    cell_val = extract_cell_value(row[asset_col])
    match = re.search(r"\d+", cell_val or "")
    if not match:
        return None
    asset_id = match.group()
    machine = f"{machine_prefix}{asset_id}"
    fields: list[dict[str, str]] = []
    for label, idx in col_indexes.items():
        if idx is None or idx < 0:
            continue
        value = extract_cell_value(row[idx]) if len(row) > idx else ""
        fields.append({"label": label, "value": value})
    return {
        "site": site_key,
        "site_label": site_label,
        "machine": machine,
        "asset_id": asset_id,
        "fields": fields,
    }


def _encoder_rows_from_columns_module(mod, spec: dict[str, str]) -> list[dict[str, object]]:
    columns = getattr(mod, "COLUMNS", {})
    if not columns:
        return []
    token = mod.get_tenant_access_token()
    data = mod.get_all_sheet_data(token, mod.SPREADSHEET_TOKEN, mod.SHEET_ID)
    if not data:
        return []
    id_keywords = tuple(k.strip() for k in spec["id_keywords"].split(",") if k.strip())
    header_row, headers = _encoder_locate_header_row(data, id_keywords)
    if header_row is None:
        return []
    col_indexes, asset_col = _encoder_map_columns(headers, columns, id_keywords)
    if asset_col is None:
        return []
    out: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in data[header_row + 1 :]:
        item = _encoder_item_from_row(
            site_key=spec["key"],
            site_label=spec["label"],
            machine_prefix=spec["prefix"],
            row=row,
            col_indexes=col_indexes,
            asset_col=asset_col,
            extract_cell_value=mod.extract_cell_value,
        )
        if not item:
            continue
        dedupe = f"{spec['key']}:{item['asset_id']}"
        if dedupe in seen:
            continue
        seen.add(dedupe)
        out.append(item)
    return out


def _encoder_rows_nwr(mod) -> list[dict[str, object]]:
    token = mod.get_tenant_access_token()
    sheets = [
        {
            "spreadsheet_token": mod.FIRST_SPREADSHEET_TOKEN,
            "sheet_id": mod.FIRST_SHEET_ID,
            "required_fields": [
                "Top Encoder", "Main Encoder", "Mini PC", "CCTV",
                "TOP Streaming URL", "Main Streaming URL", "CCTV URL",
            ],
            "id_column_name": "Asset id",
            "top_url_field": "TOP Streaming URL",
        },
        {
            "spreadsheet_token": mod.SECOND_SPREADSHEET_TOKEN,
            "sheet_id": mod.SECOND_SHEET_ID,
            "required_fields": [
                "Top Encoder", "Main Encoder", "Mini PC", "CCTV",
                "Pool Streaming URL", "Main Streaming URL", "CCTV URL",
            ],
            "id_column_name": "Asset ID",
            "top_url_field": "Pool Streaming URL",
        },
    ]
    out: list[dict[str, object]] = []
    seen: set[str] = set()
    for sheet in sheets:
        data = mod.get_all_sheet_data(token, sheet["spreadsheet_token"], sheet["sheet_id"])
        if not data:
            continue
        header_keywords = tuple(f.lower() for f in sheet["required_fields"]) + ("asset",)
        header_row, headers = _encoder_locate_header_row(data, header_keywords)
        if header_row is None:
            continue
        col_map: dict[str, int] = {}
        for field in sheet["required_fields"]:
            col_map[field] = -1
            for idx, col_name in enumerate(headers):
                if col_name and field.lower() == str(col_name).strip().lower():
                    col_map[field] = idx
                    break
        id_col = None
        for idx, col_name in enumerate(headers):
            if col_name and sheet["id_column_name"].lower() in str(col_name).strip().lower():
                id_col = idx
                break
        if id_col is None:
            continue
        for row in data[header_row + 1 :]:
            cell_val = mod.extract_cell_value(row[id_col]) if len(row) > id_col else ""
            match = re.search(r"\d+", cell_val or "")
            if not match:
                continue
            asset_id = match.group()
            dedupe = f"nwr:{asset_id}"
            if dedupe in seen:
                continue
            seen.add(dedupe)
            fields: list[dict[str, str]] = []
            for field, idx in col_map.items():
                if idx < 0:
                    continue
                value = mod.extract_cell_value(row[idx]) if len(row) > idx else ""
                fields.append({"label": field, "value": value})
            out.append(
                {
                    "site": "nwr",
                    "site_label": "NP",
                    "machine": f"NWR{asset_id}",
                    "asset_id": asset_id,
                    "fields": fields,
                }
            )
    return out


def _encoder_rows_winford(mod) -> list[dict[str, object]]:
    required_fields = [
        "Top Encoder", "Main Encoder", "Mini PC", "CCTV",
        "TOP video URL", "Main Video URL", "CCTV Link",
    ]
    token = mod.get_tenant_access_token()
    data = mod.get_all_sheet_data(token)
    if not data:
        return []
    header_keywords = tuple(f.lower() for f in required_fields) + ("asset",)
    header_row, headers = _encoder_locate_header_row(data, header_keywords)
    if header_row is None:
        return []
    col_map: dict[str, int] = {}
    for field in required_fields:
        col_map[field] = -1
        for idx, col_name in enumerate(headers):
            if col_name and field.lower() == str(col_name).strip().lower():
                col_map[field] = idx
                break
    asset_col = None
    for idx, col_name in enumerate(headers):
        if col_name and "asset" in str(col_name).strip().lower() and "id" in str(col_name).strip().lower():
            asset_col = idx
            break
    if asset_col is None:
        return []
    out: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in data[header_row + 1 :]:
        cell_val = mod.extract_cell_value(row[asset_col]) if len(row) > asset_col else ""
        match = re.search(r"\d+", cell_val or "")
        if not match:
            continue
        asset_id = match.group()
        if asset_id in seen:
            continue
        seen.add(asset_id)
        fields: list[dict[str, str]] = []
        for field, idx in col_map.items():
            if idx < 0:
                continue
            value = mod.extract_cell_value(row[idx]) if len(row) > idx else ""
            fields.append({"label": field, "value": value})
        out.append(
            {
                "site": "wf",
                "site_label": "WF",
                "machine": f"WF{asset_id}",
                "asset_id": asset_id,
                "fields": fields,
            }
        )
    return out


def _encoder_collect_all(*, force_refresh: bool = False) -> tuple[list[dict[str, object]], dict[str, str]]:
    now_mono = time.monotonic()
    now_wall = time.time()
    with _encoder_lock:
        cached_items = _ENCODER_CACHE.get("items")
        cached_errors = _ENCODER_CACHE.get("errors")
        if (
            not force_refresh
            and isinstance(cached_items, list)
            and isinstance(cached_errors, dict)
            and now_mono - float(_ENCODER_CACHE.get("mono") or 0.0) < _ENCODER_CACHE_SEC
        ):
            return list(cached_items), dict(cached_errors)

    snapshot = _load_encoder_snapshot()
    if not force_refresh and snapshot is not None:
        items, errors, updated_at = snapshot
        if now_wall - updated_at < _ENCODER_CACHE_SEC:
            with _encoder_lock:
                _ENCODER_CACHE["items"] = list(items)
                _ENCODER_CACHE["errors"] = dict(errors)
                _ENCODER_CACHE["mono"] = now_mono
            return items, errors

    items, errors = _encoder_fetch_from_sheets(snapshot[0] if snapshot is not None else None)
    previous_fp = (
        _encoder_snapshot_fingerprint(snapshot[0], snapshot[1])
        if snapshot is not None
        else None
    )
    current_fp = _encoder_snapshot_fingerprint(items, errors)
    if previous_fp != current_fp:
        _persist_encoder_json(items, errors)
    with _encoder_lock:
        _ENCODER_CACHE["items"] = list(items)
        _ENCODER_CACHE["errors"] = dict(errors)
        _ENCODER_CACHE["mono"] = now_mono
    return items, errors


def _encoder_filter_items(
    items: list[dict[str, object]],
    *,
    site: str = "",
    query: str = "",
) -> list[dict[str, object]]:
    site_key = (site or "").strip().lower()
    q = (query or "").strip().lower()
    out: list[dict[str, object]] = []
    for item in items:
        if site_key and str(item.get("site") or "").lower() != site_key:
            continue
        if q:
            hay = " ".join(
                [
                    str(item.get("machine") or ""),
                    str(item.get("site_label") or ""),
                    " ".join(
                        f"{f.get('label', '')} {f.get('value', '')}"
                        for f in (item.get("fields") or [])
                        if isinstance(f, dict)
                    ),
                ]
            ).lower()
            if q not in hay:
                continue
        out.append(item)
    return out


def _encoder_site_counts(items: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        site_key = str(item.get("site") or "").strip().lower()
        if not site_key:
            continue
        counts[site_key] = counts.get(site_key, 0) + 1
    return counts


def machine_encoder_payload(*, site: str = "", query: str = "", force_refresh: bool = False) -> dict[str, object]:
    items, errors = _encoder_collect_all(force_refresh=force_refresh)
    filtered = _encoder_filter_items(items, site=site, query=query)
    return {
        "ok": True,
        "count": len(filtered),
        "total": len(items),
        "items": filtered,
        "errors": errors,
        "site_counts": _encoder_site_counts(items),
        "sites": [{"key": s["key"], "label": s["label"]} for s in _ENCODER_SITE_DEFS],
    }



_ALL_DUTY_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>All Duty</title>
  <style>
    :root {
      --bg: #0b0f14; --card: #151b26; --elev: #1c2533; --text: #e8edf4; --muted: #8b9cb3;
      --accent: #3b82f6; --ok: #22c55e; --line: #2a3544; --morn: #e8c468; --night: #9ec5f7;
      --ose-purple: #c4b5fd;
      --ose-purple-deep: #7c3aed;
      --ose-purple-bg: rgba(124, 58, 237, 0.12);
      --ose-glow: 0 0 0 1px rgba(167, 139, 250, 0.45), 0 0 20px rgba(124, 58, 237, 0.35), 0 0 40px rgba(124, 58, 237, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, rgba(59,130,246,.12), transparent), var(--bg);
      color: var(--text); min-height: 100vh;
    }
    header.wm-header-bar {
      padding: 1.1rem 1.35rem; border-bottom: 1px solid var(--line);
      display: flex; flex-wrap: wrap; align-items: flex-start; justify-content: space-between; gap: 1rem;
    }
    .ose-hero h1 { margin: 0 0 0.35rem; font-size: 1.35rem; font-weight: 700; letter-spacing: -0.02em; }
    .ose-hero p { margin: 0; font-size: 0.82rem; color: var(--muted); max-width: 36rem; line-height: 1.45; }
    a.wm-head-title-btn {
      text-decoration: none; display: inline-block; flex-shrink: 0;
      margin: 0; cursor: pointer; font: inherit; font-size: 0.95rem; font-weight: 650;
      padding: 0.5rem 1rem; border-radius: 10px; border: 1px solid var(--line); background: var(--elev); color: var(--text);
      transition: border-color .15s, background .15s, box-shadow .15s;
    }
    a.wm-head-title-btn:hover { border-color: var(--accent); background: rgba(59,130,246,.12); box-shadow: 0 0 0 3px rgba(59,130,246,.15); text-decoration: none; }
    .wm-head-nav-group { display: flex; flex-wrap: wrap; align-items: center; justify-content: flex-end; gap: 0.5rem; flex-shrink: 0; }
    a.wm-head-nav-btn.active { border-color: var(--accent); background: rgba(59,130,246,.18); color: var(--text); }
    a.wm-admin-btn {
      border-color: #b91c1c !important; background: #dc2626 !important; color: #fff !important;
    }
    a.wm-admin-btn:hover {
      border-color: #991b1b !important; background: #b91c1c !important; color: #fff !important;
      text-decoration: none;
    }
    .duty-kind-bar {
      display: flex; flex-wrap: wrap; align-items: center; gap: 0.4rem; margin-bottom: 1rem;
      padding: 0.5rem 0.55rem; border-radius: 12px; border: 1px solid var(--line); background: rgba(21,27,38,.65);
    }
    .duty-kind-bar .bar-label {
      font-size: 0.65rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted);
      width: 100%; margin-bottom: 0.15rem;
    }
    @media (min-width: 520px) {
      .duty-kind-bar .bar-label { width: auto; margin-bottom: 0; margin-right: 0.35rem; }
    }
    .duty-kind-btn {
      font: inherit; font-size: 0.72rem; font-weight: 650; padding: 0.38rem 0.55rem; border-radius: 8px;
      border: 1px solid var(--line); background: var(--elev); color: var(--muted); cursor: pointer;
      transition: background .2s, border-color .2s, color .2s, box-shadow .2s;
    }
    .duty-kind-btn:hover { border-color: rgba(167, 139, 250, 0.5); color: var(--text); }
    .duty-kind-btn.active {
      background: var(--ose-purple-bg);
      border-color: rgba(196, 181, 253, 0.85);
      color: var(--ose-purple);
      box-shadow: var(--ose-glow);
    }
    main.ose-main { padding: 1.1rem 1.35rem 2.5rem; max-width: min(1480px, 98vw); margin: 0 auto; width: 100%; }
    .ose-year-row {
      display: flex; align-items: center; justify-content: center; gap: 0.75rem; margin-bottom: 1.1rem;
    }
    .ose-year-label { font-size: 1.15rem; font-weight: 700; min-width: 5rem; text-align: center; }
    .ose-icon-btn {
      font: inherit; width: 2.25rem; height: 2.25rem; border-radius: 10px; border: 1px solid var(--line);
      background: var(--elev); color: var(--text); cursor: pointer; font-size: 1.1rem; line-height: 1;
    }
    .ose-icon-btn:hover { border-color: rgba(167, 139, 250, 0.55); box-shadow: 0 0 14px rgba(124, 58, 237, 0.22); }
    .ose-month-grid {
      display: grid; grid-template-columns: repeat(auto-fill, minmax(104px, 1fr)); gap: 0.45rem; margin-bottom: 1.25rem;
    }
    .ose-month-btn {
      font: inherit; font-size: 0.74rem; font-weight: 600; padding: 0.48rem 0.3rem; border-radius: 10px;
      border: 1px solid var(--line); background: var(--card); color: var(--muted); cursor: pointer;
      transition: background .2s, border-color .2s, color .2s, box-shadow .2s;
    }
    .ose-month-btn:hover { border-color: rgba(167, 139, 250, 0.5); color: var(--text); }
    .ose-month-btn.active {
      background: var(--ose-purple-bg);
      border-color: rgba(196, 181, 253, 0.85);
      color: var(--ose-purple);
      box-shadow: var(--ose-glow);
    }
    .ose-cal-panel {
      background: var(--card); border: 1px solid var(--line); border-radius: 14px; padding: 1.15rem 1.15rem 1.25rem;
      box-shadow: 0 8px 32px rgba(0,0,0,.22);
    }
    .ose-cal-title {
      margin: 0 0 0.75rem; font-size: 1.22rem; font-weight: 650;
      padding-bottom: 0.5rem; border-bottom: 2px solid rgba(124, 58, 237, 0.35);
    }
    .ose-cal-warn { margin: 0 0 0.75rem; font-size: 0.9rem; color: #f87171; }
    .ose-cal-dow {
      display: grid; grid-template-columns: repeat(7, 1fr); gap: 0.5rem; margin-bottom: 0.45rem;
    }
    .ose-cal-dow span {
      text-align: center; font-size: 0.74rem; font-weight: 600; text-transform: uppercase; letter-spacing: .06em; color: var(--muted);
    }
    .ose-cal-weeks { display: flex; flex-direction: column; gap: 0.5rem; }
    .ose-cal-row { display: grid; grid-template-columns: repeat(7, minmax(0, 1fr)); gap: 0.55rem; align-items: stretch; }
    .ose-cal-cell {
      min-height: 9.5rem; border-radius: 12px; border: 1px solid var(--line); background: linear-gradient(165deg, rgba(28,37,51,.98), rgba(20,26,36,.99));
      padding: 0.62rem 0.58rem 0.62rem; display: flex; flex-direction: column; gap: 0.38rem; align-items: stretch;
      transition: border-color .2s, box-shadow .2s, background .2s;
    }
    .ose-cal-cell.empty { background: transparent; border: none; min-height: 0; }
    .ose-cal-cell.is-today {
      border-color: rgba(196, 181, 253, 0.75);
      box-shadow: var(--ose-glow);
      background: linear-gradient(165deg, rgba(124, 58, 237, 0.14), rgba(20,26,36,.98));
    }
    .ose-day-head { display: flex; align-items: center; justify-content: space-between; gap: 0.4rem; margin-bottom: 0.2rem; }
    .ose-cal-cell .d { font-weight: 800; font-size: 1.08rem; color: var(--text); letter-spacing: -0.02em; line-height: 1; }
    .ose-today-badge {
      flex-shrink: 0; font-size: 0.58rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em;
      padding: 0.15rem 0.4rem; border-radius: 999px;
      background: linear-gradient(135deg, rgba(124, 58, 237, 0.55), rgba(167, 139, 250, 0.35));
      color: #f5f3ff; border: 1px solid rgba(196, 181, 253, 0.5);
      box-shadow: 0 0 12px rgba(124, 58, 237, 0.45);
    }
    .ose-blocks { display: flex; flex-direction: column; gap: 0.36rem; }
    .ose-block {
      border-radius: 9px; padding: 0.38rem 0.4rem;
      border: 1px solid rgba(42, 53, 68, 0.9);
    }
    .ose-block-m { background: rgba(232, 196, 104, 0.06); border-color: rgba(232, 196, 104, 0.12); }
    .ose-block-n { background: rgba(158, 197, 247, 0.06); border-color: rgba(158, 197, 247, 0.12); }
    .ose-block-label { font-size: 0.68rem; font-weight: 750; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.22rem; }
    .ose-block-m .ose-block-label { color: var(--morn); }
    .ose-block-n .ose-block-label { color: var(--night); }
    .ose-chip-wrap { display: flex; flex-wrap: wrap; gap: 0.2rem 0.25rem; align-items: flex-start; }
    .ose-chip {
      font-size: 0.72rem; font-weight: 500; line-height: 1.28; padding: 0.16rem 0.4rem; border-radius: 7px;
      background: rgba(15, 20, 28, 0.65); color: #cbd5e1; border: 1px solid rgba(42, 53, 68, 0.85); max-width: 100%;
    }
    .ose-dash { font-size: 0.74rem; color: var(--muted); opacity: 0.7; }
    .ose-duty-people { display: flex; flex-direction: column; gap: 0.34rem; max-height: 11rem; overflow-y: auto; }
    .ose-duty-people::-webkit-scrollbar { width: 4px; }
    .ose-duty-people::-webkit-scrollbar-thumb { background: rgba(124, 58, 237, 0.35); border-radius: 4px; }
    .ose-person-line { border-bottom: 1px solid rgba(42,53,68,.6); padding-bottom: 0.22rem; margin-bottom: 0.08rem; }
    .ose-person-line:last-child { border-bottom: none; margin-bottom: 0; padding-bottom: 0; }
    .ose-person-name { font-size: 0.78rem; font-weight: 600; color: #e2e8f0; line-height: 1.32; word-break: break-word; }
    .ose-person-phone { font-size: 0.7rem; color: #94a3b8; margin-top: 0.08rem; }
    .ose-extra-wrap { margin-top: 0.2rem; display: flex; flex-direction: column; gap: 0.4rem; max-height: 8.5rem; overflow-y: auto; }
    .ose-extra-wrap::-webkit-scrollbar { width: 4px; }
    .ose-extra-wrap::-webkit-scrollbar-thumb { background: rgba(124, 58, 237, 0.35); border-radius: 4px; }
    .ose-extra {
      padding: 0.36rem 0.4rem; border-radius: 8px; font-size: 0.72rem; line-height: 1.45;
      border: 1px solid rgba(42, 53, 68, 0.75); background: rgba(11, 15, 20, 0.35);
    }
    .ose-extra-title { font-weight: 750; text-transform: uppercase; letter-spacing: 0.07em; margin-bottom: 0.24rem; font-size: 0.66rem; }
    .ose-extra-leave { border-color: rgba(232, 196, 104, 0.15); }
    .ose-extra-leave .ose-extra-title { color: var(--morn); }
    .ose-extra-offset { border-color: rgba(158, 197, 247, 0.15); }
    .ose-extra-offset .ose-extra-title { color: var(--night); }
    .ose-extra-line { color: #9fb0c9; margin-bottom: 0.16rem; word-break: break-word; }
    .ose-extra-line:last-child { margin-bottom: 0; }
    .ose-loading { text-align: center; color: var(--muted); padding: 2rem; font-size: 0.95rem; }
    .ose-submit-bar {
      display: flex; flex-wrap: wrap; gap: 0.55rem; margin-bottom: 1rem;
    }
    .ose-submit-bar[hidden] { display: none !important; }
    .ose-submit-btn {
      font: inherit; font-size: 0.82rem; font-weight: 650; padding: 0.48rem 0.95rem; border-radius: 10px;
      border: 1px solid rgba(196, 181, 253, 0.55); background: var(--ose-purple-bg); color: var(--ose-purple);
      cursor: pointer; transition: border-color .15s, box-shadow .15s, background .15s;
      text-decoration: none; display: inline-block;
    }
    .ose-submit-btn:hover { border-color: var(--ose-purple); box-shadow: var(--ose-glow); }
    footer { padding: 1rem; text-align: center; color: var(--muted); font-size: 0.75rem; border-top: 1px solid var(--line); }
    a { color: var(--accent); }
    @media (min-width: 1200px) {
      .ose-cal-cell { min-height: 10.5rem; padding: 0.7rem 0.65rem 0.7rem; }
      .ose-duty-people { max-height: 12.5rem; }
      .ose-extra-wrap { max-height: 9.5rem; }
    }
  </style>
</head>
<body>
  <header class="wm-header-bar">
    <div class="ose-hero">
      <h1>All Duty</h1>
    </div>
    <div class="wm-head-nav-group">
      <a class="wm-head-title-btn wm-head-nav-btn" href="{{ machine_status_href }}">Machine status</a>
      <a class="wm-head-title-btn wm-head-nav-btn active" href="{{ all_duty_href }}">All Duty</a>
      <a class="wm-head-title-btn wm-head-nav-btn" href="{{ machine_encoder_href }}">Machine encoder</a>
      <a class="wm-head-title-btn wm-admin-btn" href="{{ admin_login_href }}">Admin Page</a>
    </div>
  </header>
  <main class="ose-main" id="ose-main">
    <nav class="duty-kind-bar" id="duty-kind-bar" aria-label="Duty roster">
      <span class="bar-label">Roster</span>
      <button type="button" class="duty-kind-btn" data-kind="ote">OTE</button>
      <button type="button" class="duty-kind-btn" data-kind="ose">OSE</button>
      <button type="button" class="duty-kind-btn" data-kind="fpms">FPMS</button>
      <button type="button" class="duty-kind-btn" data-kind="ft">FT</button>
      <button type="button" class="duty-kind-btn" data-kind="fe">FE</button>
      <button type="button" class="duty-kind-btn" data-kind="bi">BI</button>
      <button type="button" class="duty-kind-btn" data-kind="cpms">CPMS</button>
      <button type="button" class="duty-kind-btn" data-kind="db">DB</button>
      <button type="button" class="duty-kind-btn" data-kind="sre">SRE</button>
    </nav>
    <div class="ose-submit-bar" id="ose-submit-bar" hidden>
      <a class="ose-submit-btn" id="ose-open-leave" href="{{ ose_submit_leave_href }}">Submit Leave</a>
      <a class="ose-submit-btn" id="ose-open-offset" href="{{ ose_submit_offset_href }}">Submit Offset</a>
    </div>
    <div class="ose-year-row">
      <button type="button" class="ose-icon-btn" id="ose-y-prev" aria-label="Previous year">‹</button>
      <span class="ose-year-label" id="ose-year-label"></span>
      <button type="button" class="ose-icon-btn" id="ose-y-next" aria-label="Next year">›</button>
    </div>
    <div class="ose-month-grid" id="ose-month-btns" role="tablist" aria-label="Month"></div>
    <div class="ose-cal-panel" id="ose-cal-panel">
      <h3 class="ose-cal-title" id="ose-cal-title">Select a month</h3>
      <p class="ose-cal-warn" id="ose-cal-warn" hidden></p>
      <div class="ose-cal-dow" aria-hidden="true">
        <span>Mon</span><span>Tue</span><span>Wed</span><span>Thu</span><span>Fri</span><span>Sat</span><span>Sun</span>
      </div>
      <div class="ose-cal-weeks" id="ose-cal-root"><div class="ose-loading" id="ose-cal-loading">Loading calendar…</div></div>
    </div>
  </main>
  <footer>API: <a id="duty-api-foot" href="#">api/duty-calendar</a> · <a href="{{ back_href }}">Machine list</a></footer>
  <script>
  (function () {
    var API = {{ api_calendar_href | tojson }};
    var dutyKind = {{ initial_kind | tojson }};
    var validKinds = ["ote", "ose", "fpms", "ft", "fe", "bi", "cpms", "db", "sre"];
    if (validKinds.indexOf(dutyKind) < 0) dutyKind = "ose";
    var monthNames = ["January","February","March","April","May","June","July","August","September","October","November","December"];
    var now = new Date();
    var year = now.getFullYear();
    var selectedMonth = now.getMonth() + 1;
    var monthBar = document.getElementById("ose-month-btns");
    var yearLabel = document.getElementById("ose-year-label");
    var calTitle = document.getElementById("ose-cal-title");
    var calWarn = document.getElementById("ose-cal-warn");
    var calRoot = document.getElementById("ose-cal-root");
    var loadingEl = document.getElementById("ose-cal-loading");
    var kindBar = document.getElementById("duty-kind-bar");
    var oseSubmitBar = document.getElementById("ose-submit-bar");
    function setYearLabel() { yearLabel.textContent = String(year); }

    function syncOseSubmitBar() {
      if (!oseSubmitBar) return;
      oseSubmitBar.hidden = dutyKind !== "ose";
    }

    function syncKindBar() {
      if (!kindBar) return;
      kindBar.querySelectorAll(".duty-kind-btn").forEach(function (btn) {
        var k = btn.getAttribute("data-kind");
        var on = k === dutyKind;
        btn.classList.toggle("active", on);
        btn.setAttribute("aria-pressed", on ? "true" : "false");
      });
      var foot = document.getElementById("duty-api-foot");
      if (foot) {
        foot.href = API + "?kind=" + encodeURIComponent(dutyKind) + "&year=" + encodeURIComponent(String(year)) + "&month=" + encodeURIComponent(String(selectedMonth));
      }
      syncOseSubmitBar();
    }

    if (kindBar) {
      kindBar.addEventListener("click", function (ev) {
        var btn = ev.target.closest("[data-kind]");
        if (!btn || !kindBar.contains(btn)) return;
        var k = btn.getAttribute("data-kind");
        if (!k || k === dutyKind) return;
        dutyKind = k;
        syncKindBar();
        loadMonth();
      });
    }

    function buildMonthButtons() {
      monthBar.innerHTML = "";
      for (var m = 1; m <= 12; m++) {
        var b = document.createElement("button");
        b.type = "button";
        b.className = "ose-month-btn" + (m === selectedMonth ? " active" : "");
        b.textContent = monthNames[m - 1];
        b.setAttribute("role", "tab");
        b.setAttribute("aria-selected", m === selectedMonth ? "true" : "false");
        b.setAttribute("data-month", String(m));
        b.addEventListener("click", function (ev) {
          var t = ev.target;
          selectedMonth = parseInt(t.getAttribute("data-month"), 10);
          monthBar.querySelectorAll(".ose-month-btn").forEach(function (x) {
            var on = x === t;
            x.classList.toggle("active", on);
            x.setAttribute("aria-selected", on ? "true" : "false");
          });
          loadMonth();
        });
        monthBar.appendChild(b);
      }
    }

    function fillNameChips(wrapEl, arr) {
      wrapEl.innerHTML = "";
      if (!arr || !arr.length) {
        var dash = document.createElement("span");
        dash.className = "ose-dash";
        dash.textContent = "—";
        wrapEl.appendChild(dash);
        return;
      }
      for (var i = 0; i < arr.length; i++) {
        var ch = document.createElement("span");
        ch.className = "ose-chip";
        ch.textContent = arr[i];
        wrapEl.appendChild(ch);
      }
    }

    function appendLeaveAndOffset(box, cell) {
      var leaves = cell.leave || [];
      var offs = cell.offset || [];
      if (!leaves.length && !offs.length) return;
      var wrap = document.createElement("div");
      wrap.className = "ose-extra-wrap";
      if (leaves.length) {
        var sec = document.createElement("div");
        sec.className = "ose-extra ose-extra-leave";
        var h = document.createElement("div");
        h.className = "ose-extra-title";
        h.textContent = "Leave";
        sec.appendChild(h);
        for (var i = 0; i < leaves.length; i++) {
          var r = leaves[i];
          var line = document.createElement("div");
          line.className = "ose-extra-line";
          if (r.start === r.end) {
            line.textContent = r.name + " (" + r.leave_type + ") — " + r.start;
          } else {
            line.textContent = r.name + " (" + r.leave_type + ") — " + r.start + " → " + r.end;
          }
          sec.appendChild(line);
        }
        wrap.appendChild(sec);
      }
      if (offs.length) {
        var sec2 = document.createElement("div");
        sec2.className = "ose-extra ose-extra-offset";
        var h2 = document.createElement("div");
        h2.className = "ose-extra-title";
        h2.textContent = "Offset";
        sec2.appendChild(h2);
        for (var j = 0; j < offs.length; j++) {
          var line2 = document.createElement("div");
          line2.className = "ose-extra-line";
          line2.textContent = offs[j];
          sec2.appendChild(line2);
        }
        wrap.appendChild(sec2);
      }
      box.appendChild(wrap);
    }

    function appendOseShifts(box, cell) {
      var blocks = document.createElement("div");
      blocks.className = "ose-blocks";
      var bm = document.createElement("div");
      bm.className = "ose-block ose-block-m";
      var lm = document.createElement("div");
      lm.className = "ose-block-label";
      lm.textContent = "Morning";
      bm.appendChild(lm);
      var wm = document.createElement("div");
      wm.className = "ose-chip-wrap";
      fillNameChips(wm, cell.morning || []);
      bm.appendChild(wm);
      blocks.appendChild(bm);
      var bn = document.createElement("div");
      bn.className = "ose-block ose-block-n";
      var ln = document.createElement("div");
      ln.className = "ose-block-label";
      ln.textContent = "Night";
      bn.appendChild(ln);
      var wn = document.createElement("div");
      wn.className = "ose-chip-wrap";
      fillNameChips(wn, cell.night || []);
      bn.appendChild(wn);
      blocks.appendChild(bn);
      box.appendChild(blocks);
      appendLeaveAndOffset(box, cell);
    }

    function appendCpmsShifts(box, cell) {
      var blocks = document.createElement("div");
      blocks.className = "ose-blocks";
      if (cell.dutyNote) {
        var er = document.createElement("div");
        er.className = "ose-extra-line";
        er.style.marginBottom = "0.35rem";
        er.style.color = "#f87171";
        er.textContent = cell.dutyNote;
        blocks.appendChild(er);
      }
      var bm = document.createElement("div");
      bm.className = "ose-block ose-block-m";
      var lm = document.createElement("div");
      lm.className = "ose-block-label";
      lm.textContent = "Main";
      bm.appendChild(lm);
      var wm = document.createElement("div");
      wm.className = "ose-chip-wrap";
      fillNameChips(wm, cell.main ? [cell.main] : []);
      bm.appendChild(wm);
      if (cell.main_phone) {
        var pm = document.createElement("div");
        pm.className = "ose-person-phone";
        pm.style.marginTop = "0.2rem";
        pm.textContent = "📞 " + cell.main_phone;
        bm.appendChild(pm);
      }
      blocks.appendChild(bm);
      var bn = document.createElement("div");
      bn.className = "ose-block ose-block-n";
      var ln = document.createElement("div");
      ln.className = "ose-block-label";
      ln.textContent = "Backup";
      bn.appendChild(ln);
      var wn = document.createElement("div");
      wn.className = "ose-chip-wrap";
      fillNameChips(wn, cell.backup ? [cell.backup] : []);
      bn.appendChild(wn);
      if (cell.backup_phone) {
        var pb = document.createElement("div");
        pb.className = "ose-person-phone";
        pb.style.marginTop = "0.2rem";
        pb.textContent = "📞 " + cell.backup_phone;
        bn.appendChild(pb);
      }
      blocks.appendChild(bn);
      box.appendChild(blocks);
    }

    function appendSingleDuty(box, cell) {
      var blk = document.createElement("div");
      blk.className = "ose-block ose-block-m";
      var lb = document.createElement("div");
      lb.className = "ose-block-label";
      lb.textContent = "Duty";
      blk.appendChild(lb);
      var people = cell.duty_people;
      if (people && people.length) {
        var wrap = document.createElement("div");
        wrap.className = "ose-duty-people";
        for (var i = 0; i < people.length; i++) {
          var p = people[i];
          var row = document.createElement("div");
          row.className = "ose-person-line";
          var nm = document.createElement("div");
          nm.className = "ose-person-name";
          nm.textContent = p.name + (p.subtitle ? " · " + p.subtitle : "");
          row.appendChild(nm);
          var ph = document.createElement("div");
          ph.className = "ose-person-phone";
          ph.textContent = "📞 " + (p.phone != null && p.phone !== "" ? p.phone : "—");
          row.appendChild(ph);
          wrap.appendChild(row);
        }
        blk.appendChild(wrap);
      } else if (cell.duty && cell.duty.length) {
        var w = document.createElement("div");
        w.className = "ose-chip-wrap";
        fillNameChips(w, cell.duty);
        blk.appendChild(w);
      } else {
        var dash = document.createElement("span");
        dash.className = "ose-dash";
        dash.textContent = "—";
        blk.appendChild(dash);
      }
      box.appendChild(blk);
    }

    function renderCalendar(data) {
      if (loadingEl && loadingEl.parentNode) loadingEl.parentNode.removeChild(loadingEl);
      calRoot.innerHTML = "";
      calWarn.hidden = true;
      calWarn.textContent = "";
      if (!data || !data.ok) {
        calTitle.textContent = data && data.month_label ? data.month_label : "Calendar";
        var err = (data && data.error) ? data.error : "Failed to load";
        calWarn.textContent = err;
        calWarn.hidden = false;
        return;
      }
      calTitle.textContent = data.month_label || ("Month " + data.month);
      if (data.bitable_warning) {
        calWarn.textContent = data.bitable_warning;
        calWarn.hidden = false;
      }
      var layout = data.layout || (data.kind === "ose" ? "ose" : "single");
      var todayRef = new Date();
      var tY = todayRef.getFullYear();
      var tM = todayRef.getMonth() + 1;
      var tD = todayRef.getDate();
      var cy = data.year;
      var cm = data.month;
      var weeks = data.weeks || [];
      for (var wi = 0; wi < weeks.length; wi++) {
        var row = document.createElement("div");
        row.className = "ose-cal-row";
        var wk = weeks[wi];
        for (var di = 0; di < wk.length; di++) {
          var cell = wk[di];
          var box = document.createElement("div");
          if (!cell) {
            box.className = "ose-cal-cell empty";
          } else {
            var isToday = (cell.day === tD && cm === tM && cy === tY);
            box.className = "ose-cal-cell" + (isToday ? " is-today" : "");
            var head = document.createElement("div");
            head.className = "ose-day-head";
            var dayn = document.createElement("span");
            dayn.className = "d";
            dayn.textContent = String(cell.day);
            head.appendChild(dayn);
            if (isToday) {
              var badge = document.createElement("span");
              badge.className = "ose-today-badge";
              badge.textContent = "Today";
              head.appendChild(badge);
            }
            box.appendChild(head);
            if (layout === "ose") appendOseShifts(box, cell);
            else if (layout === "cpms") appendCpmsShifts(box, cell);
            else appendSingleDuty(box, cell);
          }
          row.appendChild(box);
        }
        calRoot.appendChild(row);
      }
      syncKindBar();
    }

    function loadMonth() {
      calRoot.innerHTML = "";
      var ld = document.createElement("div");
      ld.className = "ose-loading";
      ld.id = "ose-cal-loading";
      ld.textContent = "Loading calendar…";
      calRoot.appendChild(ld);
      loadingEl = ld;
      var url = API + "?kind=" + encodeURIComponent(dutyKind) + "&year=" + encodeURIComponent(String(year)) + "&month=" + encodeURIComponent(String(selectedMonth));
      fetch(url).then(function (r) { return r.json(); }).then(renderCalendar).catch(function (e) {
        renderCalendar({ ok: false, error: String(e) });
      });
    }

    document.getElementById("ose-y-prev").addEventListener("click", function () {
      year--;
      setYearLabel();
      syncKindBar();
      buildMonthButtons();
      loadMonth();
    });
    document.getElementById("ose-y-next").addEventListener("click", function () {
      year++;
      setYearLabel();
      syncKindBar();
      buildMonthButtons();
      loadMonth();
    });


    syncKindBar();
    setYearLabel();
    buildMonthButtons();
    loadMonth();
  })();
  </script>
</body>
</html>
"""


_OSE_FORM_PAGE_CSS = """
    :root {
      --bg: #0b0f14; --card: #151b26; --elev: #1c2533; --text: #e8edf4; --muted: #8b9cb3;
      --accent: #3b82f6; --ok: #22c55e; --line: #2a3544;
      --ose-purple: #c4b5fd; --ose-purple-bg: rgba(124, 58, 237, 0.12); --ose-glow: 0 0 0 1px rgba(167, 139, 250, 0.45), 0 0 20px rgba(124, 58, 237, 0.35);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, rgba(59,130,246,.12), transparent), var(--bg);
      color: var(--text); min-height: 100vh;
    }
    .ose-form-page-header {
      padding: 1rem 1.35rem; border-bottom: 1px solid var(--line);
      display: flex; align-items: center; gap: 0.85rem;
    }
    .ose-back-btn {
      text-decoration: none; display: inline-flex; align-items: center; gap: 0.35rem;
      font-size: 0.9rem; font-weight: 650; padding: 0.45rem 0.85rem; border-radius: 10px;
      border: 1px solid var(--line); background: var(--elev); color: var(--text);
    }
    .ose-back-btn:hover { border-color: var(--accent); background: rgba(59,130,246,.12); text-decoration: none; }
    .ose-form-page-header h1 { margin: 0; font-size: 1.2rem; font-weight: 700; }
    main.ose-form-page { padding: 1.1rem 1.35rem 2.5rem; max-width: min(1400px, 98vw); margin: 0 auto; width: 100%; }
    .ose-form-card, .ose-records-card {
      background: var(--card); border: 1px solid var(--line); border-radius: 14px; padding: 1rem 1.1rem; margin-bottom: 1rem;
    }
    .ose-records-card h2 { margin: 0; font-size: 1rem; font-weight: 700; letter-spacing: -0.01em; }
    .ose-records-head {
      display: flex; align-items: center; justify-content: space-between; gap: 0.75rem;
      margin-bottom: 0.35rem;
    }
    .ose-records-refresh-btn {
      font: inherit; font-size: 0.78rem; font-weight: 650; padding: 0.38rem 0.8rem; border-radius: 9px;
      border: 1px solid rgba(196, 181, 253, 0.55); background: var(--ose-purple-bg); color: var(--ose-purple);
      cursor: pointer; flex-shrink: 0;
    }
    .ose-records-refresh-btn:hover { border-color: var(--ose-purple); box-shadow: var(--ose-glow); }
    .ose-records-refresh-btn:disabled { opacity: 0.65; cursor: wait; }
    .ose-records-hint { margin: 0 0 0.85rem; font-size: 0.75rem; color: var(--muted); line-height: 1.4; }
    .ose-records-wrap {
      overflow: auto; max-height: min(70vh, 760px);
      border: 1px solid var(--line); border-radius: 12px;
      background: rgba(10, 14, 20, 0.45);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
    }
    .ose-records-table {
      width: 100%; min-width: 980px; border-collapse: separate; border-spacing: 0;
      background: transparent; font-size: 0.8rem;
    }
    .ose-records-table th, .ose-records-table td {
      padding: 0.62rem 0.75rem; border-bottom: 1px solid rgba(42, 53, 68, 0.9);
      text-align: left; vertical-align: middle;
    }
    .ose-records-table thead th {
      position: sticky; top: 0; z-index: 2;
      background: #1a2331; font-size: 0.72rem; font-weight: 650;
      letter-spacing: 0.01em; color: #a8b7cc; white-space: nowrap;
    }
    .ose-records-table tbody tr:nth-child(odd) td { background: rgba(21, 27, 38, 0.55); }
    .ose-records-table tbody tr:nth-child(even) td { background: rgba(26, 34, 46, 0.72); }
    .ose-records-table tbody tr:hover td { background: rgba(59,130,246,0.1); }
    .ose-records-table tbody tr:last-child td { border-bottom: none; }
    .ose-records-table .col-name { min-width: 8.5rem; }
    .ose-records-table .col-pill { min-width: 7.5rem; }
    .ose-records-table .col-date { min-width: 6.5rem; }
    .ose-records-table .col-note { min-width: 9rem; width: 18%; }
    .ose-records-table .col-person { min-width: 7rem; }
    .ose-records-table .col-shift { width: 4.5rem; text-align: center; }
    .ose-records-table td.ose-cell-name { font-weight: 650; color: #f1f5f9; white-space: nowrap; }
    .ose-records-table td.ose-cell-date {
      white-space: nowrap; font-variant-numeric: tabular-nums; color: #c7d2e3; font-size: 0.78rem;
    }
    .ose-records-table td.ose-cell-person { white-space: nowrap; }
    .ose-records-table td.ose-cell-note { max-width: 16rem; line-height: 1.45; word-break: break-word; }
    .ose-records-table td.ose-cell-empty { color: rgba(139, 156, 179, 0.55); }
    .ose-records-table td.ose-cell-pill { white-space: nowrap; }
    .ose-records-table td.ose-cell-shift { text-align: center; }
    .ose-pill {
      display: inline-flex; align-items: center; justify-content: center;
      padding: 0.2rem 0.55rem; border-radius: 999px; font-size: 0.72rem; font-weight: 650;
      line-height: 1.2; border: 1px solid transparent; white-space: nowrap;
    }
    .ose-pill-leave { background: rgba(59,130,246,0.16); color: #bfdbfe; border-color: rgba(59,130,246,0.28); }
    .ose-pill-status.is-approved { background: rgba(34,197,94,0.14); color: #86efac; border-color: rgba(34,197,94,0.28); }
    .ose-pill-status.is-pending { background: rgba(234,179,8,0.14); color: #fde68a; border-color: rgba(234,179,8,0.28); }
    .ose-pill-status.is-rejected { background: rgba(239,68,68,0.14); color: #fca5a5; border-color: rgba(239,68,68,0.28); }
    .ose-pill-status { background: rgba(139,156,179,0.16); color: #cbd5e1; border-color: rgba(139,156,179,0.28); }
    .ose-pill-shift { min-width: 1.75rem; background: rgba(124,58,237,0.16); color: #ddd6fe; border-color: rgba(124,58,237,0.28); }
    .ose-records-empty { padding: 1.25rem; color: var(--muted); text-align: center; font-size: 0.84rem; }
    .ose-form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.65rem 0.75rem; }
    .ose-form-grid .full { grid-column: 1 / -1; }
    .ose-form-grid label { display: flex; flex-direction: column; gap: 0.28rem; font-size: 0.72rem; color: var(--muted); }
    .ose-form-grid input, .ose-form-grid select, .ose-form-grid textarea {
      font: inherit; font-size: 0.86rem; padding: 0.48rem 0.55rem; border-radius: 8px;
      border: 1px solid var(--line); background: #0f141c; color: var(--text);
    }
    .ose-form-grid textarea { min-height: 4.5rem; resize: vertical; }
    .ose-date-pair { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }
    .ose-form-actions { display: flex; justify-content: flex-end; gap: 0.5rem; margin-top: 0.85rem; }
    .ose-submit-btn {
      font: inherit; font-size: 0.82rem; font-weight: 650; padding: 0.48rem 0.95rem; border-radius: 10px;
      border: 1px solid rgba(196, 181, 253, 0.55); background: var(--ose-purple-bg); color: var(--ose-purple);
      cursor: pointer;
    }
    .ose-submit-btn:hover { border-color: var(--ose-purple); box-shadow: var(--ose-glow); }
    .ose-form-msg { margin: 0.55rem 0 0; font-size: 0.78rem; color: #f87171; min-height: 1.1rem; }
    .ose-form-msg.ok { color: var(--ok); }
    .ose-records-wrap { overflow: auto; border: 1px solid var(--line); border-radius: 10px; }
    .ose-records-table { width: 100%; border-collapse: collapse; background: var(--elev); font-size: 0.82rem; }
    .ose-records-table th, .ose-records-table td { padding: 0.55rem 0.65rem; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
    .ose-records-table th { background: #222b3a; font-size: 0.68rem; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); }
    .ose-records-table tbody tr:hover td { background: rgba(59,130,246,.07); }
    .ose-records-empty { padding: 1rem; color: var(--muted); text-align: center; }
    @media (max-width: 720px) { .ose-form-grid { grid-template-columns: 1fr; } }
"""

_OSE_SUBMIT_LEAVE_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Submit Leave</title>
  <style>""" + _OSE_FORM_PAGE_CSS + """</style>
</head>
<body>
  <header class="ose-form-page-header">
    <a class="ose-back-btn" href="{{ back_href }}">← Back</a>
    <h1>Submit Leave</h1>
  </header>
  <main class="ose-form-page">
    <section class="ose-form-card">
      <form id="ose-leave-form" class="ose-form-grid">
        <label class="full">Name
          <select name="name" id="ose-leave-name" required></select>
        </label>
        <label class="full">Leave Type
          <select name="leave_type" id="ose-leave-type" required></select>
        </label>
        <label>Start Date
          <div class="ose-date-pair">
            <select id="ose-leave-start-month" data-ose-month required aria-label="Start month"></select>
            <select id="ose-leave-start-day" data-ose-day required aria-label="Start day"></select>
          </div>
        </label>
        <label>End Date
          <div class="ose-date-pair">
            <select id="ose-leave-end-month" data-ose-month required aria-label="End month"></select>
            <select id="ose-leave-end-day" data-ose-day required aria-label="End day"></select>
          </div>
        </label>
        <label class="full">Reason
          <textarea name="reason" id="ose-leave-reason" required></textarea>
        </label>
        <div class="ose-form-actions full">
          <button type="submit" class="ose-submit-btn">Submit</button>
        </div>
      </form>
      <p class="ose-form-msg" id="ose-leave-msg"></p>
    </section>
    <section class="ose-records-card">
      <div class="ose-records-head">
        <h2>All leave records</h2>
        <button type="button" class="ose-records-refresh-btn" id="ose-leave-refresh">Refresh</button>
      </div>
      <p class="ose-records-hint">Scroll horizontally if needed. Empty fields show as —.</p>
      <div class="ose-records-wrap">
        <table class="ose-records-table">
          <thead>
            <tr>
              <th scope="col" class="col-name">Name</th>
              <th scope="col" class="col-pill">Leave type</th>
              <th scope="col" class="col-date">Start date</th>
              <th scope="col" class="col-date">End date</th>
              <th scope="col" class="col-note">Reason</th>
              <th scope="col" class="col-pill">Status</th>
              <th scope="col" class="col-person">Approver</th>
              <th scope="col" class="col-date">Approval date</th>
              <th scope="col" class="col-note">Remarks</th>
            </tr>
          </thead>
          <tbody id="ose-leave-list-body"><tr><td class="ose-records-empty" colspan="9">Loading…</td></tr></tbody>
        </table>
      </div>
    </section>
  </main>
  <script>
  (function () {
    var API_META = {{ api_ose_meta_href | tojson }};
    var API_LIST = {{ api_ose_leave_list_href | tojson }};
    var API_SUBMIT = {{ api_ose_submit_leave_href | tojson }};
    function fillSelectOptions(sel, items, placeholder) {
      if (!sel) return;
      sel.innerHTML = "";
      var ph = document.createElement("option");
      ph.value = "";
      ph.textContent = placeholder || "Select…";
      ph.disabled = true;
      ph.selected = true;
      sel.appendChild(ph);
      for (var i = 0; i < items.length; i++) {
        var opt = document.createElement("option");
        opt.value = items[i];
        opt.textContent = items[i];
        sel.appendChild(opt);
      }
    }
    function initMonthDaySelects() {
      var monthNames = ["January","February","March","April","May","June","July","August","September","October","November","December"];
      document.querySelectorAll("[data-ose-month]").forEach(function (sel) {
        sel.innerHTML = "";
        var mph = document.createElement("option");
        mph.value = ""; mph.textContent = "Month"; mph.disabled = true; mph.selected = true;
        sel.appendChild(mph);
        for (var m = 1; m <= 12; m++) {
          var mopt = document.createElement("option");
          mopt.value = String(m);
          mopt.textContent = monthNames[m - 1];
          sel.appendChild(mopt);
        }
      });
      document.querySelectorAll("[data-ose-day]").forEach(function (sel) {
        sel.innerHTML = "";
        var dph = document.createElement("option");
        dph.value = ""; dph.textContent = "Day"; dph.disabled = true; dph.selected = true;
        sel.appendChild(dph);
        for (var d = 1; d <= 31; d++) {
          var dopt = document.createElement("option");
          dopt.value = String(d);
          dopt.textContent = String(d);
          sel.appendChild(dopt);
        }
      });
    }
    initMonthDaySelects();
    function appendTextCell(tr, val, extraClass) {
      var td = document.createElement("td");
      if (extraClass) td.className = extraClass;
      var s = String(val || "").trim();
      if (!s) {
        td.classList.add("ose-cell-empty");
        td.textContent = "—";
      } else {
        td.textContent = s;
      }
      tr.appendChild(td);
    }
    function appendPillCell(tr, val, pillClass) {
      var td = document.createElement("td");
      var s = String(val || "").trim();
      if (!s) {
        td.className = "ose-cell-empty";
        td.textContent = "—";
      } else {
        td.className = "ose-cell-pill";
        var span = document.createElement("span");
        span.className = "ose-pill " + (pillClass || "");
        span.textContent = s;
        td.appendChild(span);
      }
      tr.appendChild(td);
    }
    function appendStatusCell(tr, val) {
      var td = document.createElement("td");
      var s = String(val || "").trim();
      if (!s) {
        td.className = "ose-cell-empty";
        td.textContent = "—";
      } else {
        td.className = "ose-cell-pill";
        var span = document.createElement("span");
        var cls = "ose-pill ose-pill-status";
        var k = s.toLowerCase();
        if (k.indexOf("approv") >= 0) cls += " is-approved";
        else if (k.indexOf("pend") >= 0) cls += " is-pending";
        else if (k.indexOf("reject") >= 0) cls += " is-rejected";
        span.className = cls;
        span.textContent = s;
        td.appendChild(span);
      }
      tr.appendChild(td);
    }
    function renderLeaveList(data) {
      var body = document.getElementById("ose-leave-list-body");
      if (!body) return;
      body.innerHTML = "";
      var items = (data && data.items) || [];
      if (!items.length) {
        var row = document.createElement("tr");
        var cell = document.createElement("td");
        cell.className = "ose-records-empty"; cell.colSpan = 9; cell.textContent = "No leave records.";
        row.appendChild(cell); body.appendChild(row); return;
      }
      for (var i = 0; i < items.length; i++) {
        var it = items[i];
        var tr = document.createElement("tr");
        appendTextCell(tr, it.name, "ose-cell-name");
        appendPillCell(tr, it.leave_type, "ose-pill-leave");
        appendTextCell(tr, it.start_date, "ose-cell-date");
        appendTextCell(tr, it.end_date, "ose-cell-date");
        appendTextCell(tr, it.reason, "ose-cell-note");
        appendStatusCell(tr, it.status);
        appendTextCell(tr, it.approver, "ose-cell-person");
        appendTextCell(tr, it.approval_date, "ose-cell-date");
        appendTextCell(tr, it.remarks, "ose-cell-note");
        body.appendChild(tr);
      }
    }
    function loadLeaveList(forceRefresh) {
      var body = document.getElementById("ose-leave-list-body");
      var refreshBtn = document.getElementById("ose-leave-refresh");
      var url = API_LIST;
      if (forceRefresh) url += (url.indexOf("?") >= 0 ? "&" : "?") + "refresh=1";
      if (body) body.innerHTML = '<tr><td class="ose-records-empty" colspan="9">Loading…</td></tr>';
      if (refreshBtn) refreshBtn.disabled = true;
      fetch(url).then(function (r) { return r.json(); }).then(renderLeaveList).catch(function () {
        renderLeaveList({ items: [] });
      }).finally(function () {
        if (refreshBtn) refreshBtn.disabled = false;
      });
    }
    fetch(API_META).then(function (r) { return r.json(); }).then(function (meta) {
      if (!meta || !meta.ok) throw new Error((meta && meta.error) || "Failed to load options");
      fillSelectOptions(document.getElementById("ose-leave-name"), meta.leave_names || [], "Select name");
      fillSelectOptions(document.getElementById("ose-leave-type"), meta.leave_types || [], "Select leave type");
    }).catch(function (e) {
      var msg = document.getElementById("ose-leave-msg");
      if (msg) msg.textContent = String(e);
    });
    loadLeaveList();
    var leaveRefreshBtn = document.getElementById("ose-leave-refresh");
    if (leaveRefreshBtn) leaveRefreshBtn.addEventListener("click", function () { loadLeaveList(true); });
    var form = document.getElementById("ose-leave-form");
    if (form) {
      form.addEventListener("submit", function (ev) {
        ev.preventDefault();
        var msg = document.getElementById("ose-leave-msg");
        var payload = {
          name: document.getElementById("ose-leave-name").value,
          leave_type: document.getElementById("ose-leave-type").value,
          start_month: document.getElementById("ose-leave-start-month").value,
          start_day: document.getElementById("ose-leave-start-day").value,
          end_month: document.getElementById("ose-leave-end-month").value,
          end_day: document.getElementById("ose-leave-end-day").value,
          reason: document.getElementById("ose-leave-reason").value
        };
        fetch(API_SUBMIT, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        }).then(function (r) { return r.json(); }).then(function (data) {
          if (!data || !data.ok) throw new Error((data && data.error) || "Submit failed");
          if (msg) { msg.textContent = "Leave submitted."; msg.className = "ose-form-msg ok"; }
          form.reset();
          initMonthDaySelects();
          loadLeaveList(true);
        }).catch(function (e) {
          if (msg) { msg.textContent = String(e); msg.className = "ose-form-msg"; }
        });
      });
    }
  })();
  </script>
</body>
</html>
"""

_OSE_SUBMIT_OFFSET_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Submit Offset</title>
  <style>""" + _OSE_FORM_PAGE_CSS + """</style>
</head>
<body>
  <header class="ose-form-page-header">
    <a class="ose-back-btn" href="{{ back_href }}">← Back</a>
    <h1>Submit Offset</h1>
  </header>
  <main class="ose-form-page">
    <section class="ose-form-card">
      <form id="ose-offset-form" class="ose-form-grid">
        <label>Request Person
          <select name="request_person" id="ose-offset-req" required></select>
        </label>
        <label>Exchange Person
          <select name="exchange_person" id="ose-offset-exc" required></select>
        </label>
        <label>Shift Type
          <select name="shift_type" id="ose-offset-shift" required></select>
        </label>
        <label>Original Date
          <div class="ose-date-pair">
            <select id="ose-offset-orig-month" data-ose-month required aria-label="Original month"></select>
            <select id="ose-offset-orig-day" data-ose-day required aria-label="Original day"></select>
          </div>
        </label>
        <label>Exchange Date
          <div class="ose-date-pair">
            <select id="ose-offset-xchg-month" data-ose-month required aria-label="Exchange month"></select>
            <select id="ose-offset-xchg-day" data-ose-day required aria-label="Exchange day"></select>
          </div>
        </label>
        <label class="full">Reason
          <textarea name="reason" id="ose-offset-reason" required></textarea>
        </label>
        <div class="ose-form-actions full">
          <button type="submit" class="ose-submit-btn">Submit</button>
        </div>
      </form>
      <p class="ose-form-msg" id="ose-offset-msg"></p>
    </section>
    <section class="ose-records-card">
      <div class="ose-records-head">
        <h2>All offset records</h2>
        <button type="button" class="ose-records-refresh-btn" id="ose-offset-refresh">Refresh</button>
      </div>
      <p class="ose-records-hint">Scroll horizontally if needed. Empty fields show as —.</p>
      <div class="ose-records-wrap">
        <table class="ose-records-table">
          <thead>
            <tr>
              <th scope="col" class="col-date">Request date</th>
              <th scope="col" class="col-person">Request person</th>
              <th scope="col" class="col-person">Exchange person</th>
              <th scope="col" class="col-shift">Shift</th>
              <th scope="col" class="col-date">Original date</th>
              <th scope="col" class="col-date">Exchange date</th>
              <th scope="col" class="col-note">Reason</th>
              <th scope="col" class="col-pill">Approval status</th>
              <th scope="col" class="col-person">Approver</th>
              <th scope="col" class="col-date">Approval date</th>
              <th scope="col" class="col-note">Remarks</th>
            </tr>
          </thead>
          <tbody id="ose-offset-list-body"><tr><td class="ose-records-empty" colspan="11">Loading…</td></tr></tbody>
        </table>
      </div>
    </section>
  </main>
  <script>
  (function () {
    var API_META = {{ api_ose_meta_href | tojson }};
    var API_LIST = {{ api_ose_offset_list_href | tojson }};
    var API_SUBMIT = {{ api_ose_submit_offset_href | tojson }};
    function fillSelectOptions(sel, items, placeholder) {
      if (!sel) return;
      sel.innerHTML = "";
      var ph = document.createElement("option");
      ph.value = "";
      ph.textContent = placeholder || "Select…";
      ph.disabled = true;
      ph.selected = true;
      sel.appendChild(ph);
      for (var i = 0; i < items.length; i++) {
        var opt = document.createElement("option");
        opt.value = items[i];
        opt.textContent = items[i];
        sel.appendChild(opt);
      }
    }
    function initMonthDaySelects() {
      var monthNames = ["January","February","March","April","May","June","July","August","September","October","November","December"];
      document.querySelectorAll("[data-ose-month]").forEach(function (sel) {
        sel.innerHTML = "";
        var mph = document.createElement("option");
        mph.value = ""; mph.textContent = "Month"; mph.disabled = true; mph.selected = true;
        sel.appendChild(mph);
        for (var m = 1; m <= 12; m++) {
          var mopt = document.createElement("option");
          mopt.value = String(m);
          mopt.textContent = monthNames[m - 1];
          sel.appendChild(mopt);
        }
      });
      document.querySelectorAll("[data-ose-day]").forEach(function (sel) {
        sel.innerHTML = "";
        var dph = document.createElement("option");
        dph.value = ""; dph.textContent = "Day"; dph.disabled = true; dph.selected = true;
        sel.appendChild(dph);
        for (var d = 1; d <= 31; d++) {
          var dopt = document.createElement("option");
          dopt.value = String(d);
          dopt.textContent = String(d);
          sel.appendChild(dopt);
        }
      });
    }
    initMonthDaySelects();
    function appendTextCell(tr, val, extraClass) {
      var td = document.createElement("td");
      if (extraClass) td.className = extraClass;
      var s = String(val || "").trim();
      if (!s) {
        td.classList.add("ose-cell-empty");
        td.textContent = "—";
      } else {
        td.textContent = s;
      }
      tr.appendChild(td);
    }
    function appendPillCell(tr, val, pillClass, extraClass) {
      var td = document.createElement("td");
      if (extraClass) td.className = extraClass;
      var s = String(val || "").trim();
      if (!s) {
        td.className = (extraClass ? extraClass + " " : "") + "ose-cell-empty";
        td.textContent = "—";
      } else {
        td.className = ((extraClass ? extraClass + " " : "") + "ose-cell-pill").trim();
        var span = document.createElement("span");
        span.className = "ose-pill " + (pillClass || "");
        span.textContent = s;
        td.appendChild(span);
      }
      tr.appendChild(td);
    }
    function appendStatusCell(tr, val) {
      var td = document.createElement("td");
      var s = String(val || "").trim();
      if (!s) {
        td.className = "ose-cell-empty";
        td.textContent = "—";
      } else {
        td.className = "ose-cell-pill";
        var span = document.createElement("span");
        var cls = "ose-pill ose-pill-status";
        var k = s.toLowerCase();
        if (k.indexOf("approv") >= 0) cls += " is-approved";
        else if (k.indexOf("pend") >= 0) cls += " is-pending";
        else if (k.indexOf("reject") >= 0) cls += " is-rejected";
        span.className = cls;
        span.textContent = s;
        td.appendChild(span);
      }
      tr.appendChild(td);
    }
    function renderOffsetList(data) {
      var body = document.getElementById("ose-offset-list-body");
      if (!body) return;
      body.innerHTML = "";
      var items = (data && data.items) || [];
      if (!items.length) {
        var row = document.createElement("tr");
        var cell = document.createElement("td");
        cell.className = "ose-records-empty"; cell.colSpan = 11; cell.textContent = "No offset records.";
        row.appendChild(cell); body.appendChild(row); return;
      }
      for (var i = 0; i < items.length; i++) {
        var it = items[i];
        var tr = document.createElement("tr");
        appendTextCell(tr, it.request_date, "ose-cell-date");
        appendTextCell(tr, it.request_person, "ose-cell-person");
        appendTextCell(tr, it.exchange_person, "ose-cell-person");
        appendPillCell(tr, it.shift_type, "ose-pill-shift", "ose-cell-shift");
        appendTextCell(tr, it.original_date, "ose-cell-date");
        appendTextCell(tr, it.exchange_date, "ose-cell-date");
        appendTextCell(tr, it.reason, "ose-cell-note");
        appendStatusCell(tr, it.approval_status);
        appendTextCell(tr, it.approver, "ose-cell-person");
        appendTextCell(tr, it.approval_date, "ose-cell-date");
        appendTextCell(tr, it.remarks, "ose-cell-note");
        body.appendChild(tr);
      }
    }
    function loadOffsetList(forceRefresh) {
      var body = document.getElementById("ose-offset-list-body");
      var refreshBtn = document.getElementById("ose-offset-refresh");
      var url = API_LIST;
      if (forceRefresh) url += (url.indexOf("?") >= 0 ? "&" : "?") + "refresh=1";
      if (body) body.innerHTML = '<tr><td class="ose-records-empty" colspan="11">Loading…</td></tr>';
      if (refreshBtn) refreshBtn.disabled = true;
      fetch(url).then(function (r) { return r.json(); }).then(renderOffsetList).catch(function () {
        renderOffsetList({ items: [] });
      }).finally(function () {
        if (refreshBtn) refreshBtn.disabled = false;
      });
    }
    fetch(API_META).then(function (r) { return r.json(); }).then(function (meta) {
      if (!meta || !meta.ok) throw new Error((meta && meta.error) || "Failed to load options");
      fillSelectOptions(document.getElementById("ose-offset-req"), meta.offset_names || [], "Select request person");
      fillSelectOptions(document.getElementById("ose-offset-exc"), meta.offset_names || [], "Select exchange person");
      fillSelectOptions(document.getElementById("ose-offset-shift"), meta.shift_types || [], "Select shift");
    }).catch(function (e) {
      var msg = document.getElementById("ose-offset-msg");
      if (msg) msg.textContent = String(e);
    });
    loadOffsetList();
    var offsetRefreshBtn = document.getElementById("ose-offset-refresh");
    if (offsetRefreshBtn) offsetRefreshBtn.addEventListener("click", function () { loadOffsetList(true); });
    var form = document.getElementById("ose-offset-form");
    if (form) {
      form.addEventListener("submit", function (ev) {
        ev.preventDefault();
        var msg = document.getElementById("ose-offset-msg");
        var payload = {
          request_person: document.getElementById("ose-offset-req").value,
          exchange_person: document.getElementById("ose-offset-exc").value,
          shift_type: document.getElementById("ose-offset-shift").value,
          original_month: document.getElementById("ose-offset-orig-month").value,
          original_day: document.getElementById("ose-offset-orig-day").value,
          exchange_month: document.getElementById("ose-offset-xchg-month").value,
          exchange_day: document.getElementById("ose-offset-xchg-day").value,
          reason: document.getElementById("ose-offset-reason").value
        };
        fetch(API_SUBMIT, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        }).then(function (r) { return r.json(); }).then(function (data) {
          if (!data || !data.ok) throw new Error((data && data.error) || "Submit failed");
          if (msg) { msg.textContent = "Offset submitted."; msg.className = "ose-form-msg ok"; }
          form.reset();
          initMonthDaySelects();
          loadOffsetList(true);
        }).catch(function (e) {
          if (msg) { msg.textContent = String(e); msg.className = "ose-form-msg"; }
        });
      });
    }
  })();
  </script>
</body>
</html>
"""


_ADMIN_LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin login</title>
  <style>
    :root {
      --bg: #0b0f14; --card: #151b26; --elev: #1c2533; --text: #e8edf4; --muted: #8b9cb3;
      --accent: #3b82f6; --line: #2a3544; --bad: #ef4444;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; min-height: 100vh; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, rgba(59,130,246,.12), transparent), var(--bg);
      color: var(--text); display: flex; align-items: center; justify-content: center; padding: 1.25rem;
    }
    .adm-login-card {
      width: 100%; max-width: 380px; background: var(--card); border: 1px solid var(--line); border-radius: 12px;
      padding: 1.35rem 1.25rem; box-shadow: 0 8px 32px rgba(0,0,0,.2);
    }
    .adm-login-card h1 { margin: 0 0 1rem; font-size: 1.2rem; font-weight: 700; letter-spacing: -0.02em; }
    .adm-err {
      margin: 0 0 1rem; padding: 0.55rem 0.7rem; border-radius: 8px; background: rgba(239,68,68,.12);
      border: 1px solid rgba(239,68,68,.35); color: #fecaca; font-size: 0.88rem;
    }
    label { display: block; font-size: 0.78rem; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: .06em; margin-bottom: 0.35rem; }
    input[type="text"], input[type="password"] {
      width: 100%; padding: 0.55rem 0.75rem; border-radius: 10px; border: 1px solid var(--line);
      background: #0f141c; color: var(--text); font-size: 0.95rem; outline: none; margin-bottom: 0.85rem;
    }
    input:focus { border-color: var(--accent); box-shadow: 0 0 0 3px rgba(59,130,246,.2); }
    button[type="submit"] {
      width: 100%; margin-top: 0.35rem; font: inherit; font-weight: 650; font-size: 0.92rem;
      padding: 0.58rem 1rem; border-radius: 10px; border: 1px solid var(--accent); background: rgba(59,130,246,.22);
      color: var(--text); cursor: pointer;
    }
    button[type="submit"]:hover { background: rgba(59,130,246,.32); }
  </style>
</head>
<body>
  <div class="adm-login-card">
    <h1>Admin login</h1>
    {% if error %}
    <p class="adm-err">{{ error }}</p>
    {% endif %}
    <form method="post" action="{{ admin_login_action }}" autocomplete="on">
      <label for="adm-login-id">ID</label>
      <input id="adm-login-id" name="login_id" type="text" required autocomplete="username"/>
      <label for="adm-login-pw">Password</label>
      <input id="adm-login-pw" name="password" type="password" required autocomplete="current-password"/>
      <button type="submit">Sign in</button>
    </form>
  </div>
</body>
</html>
"""


_ADMIN_LEAVE_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin · Leave</title>
  <style>
    :root {
      --bg: #0b0f14; --card: #151b26; --elev: #1c2533; --text: #e8edf4; --muted: #8b9cb3;
      --accent: #3b82f6; --line: #2a3544; --ok: #22c55e; --bad: #ef4444; --warn: #eab308;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, rgba(59,130,246,.12), transparent), var(--bg);
      color: var(--text); min-height: 100vh;
    }
    header.adm-header {
      padding: 1rem 1.35rem; border-bottom: 1px solid var(--line);
      display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 0.75rem;
    }
    .adm-header h1 { margin: 0; font-size: 1.15rem; font-weight: 700; }
    .adm-nav { display: flex; flex-wrap: wrap; align-items: center; gap: 0.45rem; }
    a.adm-nav-btn, button.adm-nav-btn {
      font: inherit; font-size: 0.85rem; font-weight: 650; padding: 0.45rem 0.85rem; border-radius: 10px;
      border: 1px solid var(--line); background: var(--elev); color: var(--text); cursor: pointer;
      text-decoration: none; display: inline-block;
    }
    a.adm-nav-btn:hover, button.adm-nav-btn:hover { border-color: var(--accent); background: rgba(59,130,246,.12); text-decoration: none; }
    a.adm-nav-btn.active { border-color: var(--accent); background: rgba(59,130,246,.18); }
    button.adm-nav-btn.ghost { background: transparent; color: var(--muted); }
    main { padding: 1rem 1.35rem 2rem; max-width: 1400px; margin: 0 auto; width: 100%; }
    .adm-toolbar {
      display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 0.65rem; margin-bottom: 0.85rem;
    }
    .adm-filters { display: flex; flex-wrap: wrap; gap: 0.45rem; align-items: center; }
    .env-filter-btn {
      font: inherit; font-size: 0.8rem; font-weight: 600;
      padding: 0.4rem 0.8rem; border-radius: 999px; cursor: pointer; border: 1px solid var(--line);
      background: var(--elev); color: var(--muted);
    }
    .env-filter-btn:hover { border-color: var(--accent); color: var(--text); }
    .env-filter-btn.active { background: rgba(59,130,246,.25); border-color: var(--accent); color: var(--text); }
    .adm-table-wrap { overflow-x: auto; border-radius: 12px; border: 1px solid var(--line); }
    table { width: 100%; border-collapse: collapse; background: var(--elev); }
    th, td { padding: 0.55rem 0.65rem; text-align: left; border-bottom: 1px solid var(--line); font-size: 0.84rem; vertical-align: middle; }
    th {
      background: #222b3a; font-size: 0.66rem; text-transform: uppercase; letter-spacing: .06em; color: var(--muted);
      position: sticky; top: 0; z-index: 1;
    }
    tr:last-child td { border-bottom: none; }
    tbody tr:hover td { background: rgba(59,130,246,.06); }
    .ose-pill {
      display: inline-block; padding: 0.16rem 0.48rem; border-radius: 999px; font-size: 0.72rem; font-weight: 600;
      border: 1px solid transparent;
    }
    .ose-pill-status.is-approved { background: rgba(34,197,94,0.14); color: #86efac; border-color: rgba(34,197,94,0.28); }
    .ose-pill-status.is-pending { background: rgba(234,179,8,0.14); color: #fde68a; border-color: rgba(234,179,8,0.28); }
    .ose-pill-status.is-rejected { background: rgba(239,68,68,0.14); color: #fca5a5; border-color: rgba(239,68,68,0.28); }
    .ose-pill-status { background: rgba(139,156,179,0.16); color: #cbd5e1; border-color: rgba(139,156,179,0.28); }
    .ose-pill-leave { background: rgba(167,139,250,.12); color: #ddd6fe; border-color: rgba(167,139,250,.28); }
    .ose-cell-empty { color: var(--muted); opacity: 0.65; }
    button.adm-act {
      font: inherit; font-size: 0.72rem; font-weight: 650; padding: 0.28rem 0.55rem; border-radius: 8px;
      border: 1px solid var(--line); background: var(--card); color: var(--muted); cursor: pointer; margin-right: 0.25rem;
    }
    button.adm-act-appr.selected { background: rgba(34,197,94,.18); border-color: rgba(34,197,94,.45); color: #86efac; }
    button.adm-act-rej.selected { background: rgba(239,68,68,.18); border-color: rgba(239,68,68,.45); color: #fca5a5; }
    button.adm-act-rmk { border-color: rgba(59,130,246,.4); color: var(--accent); }
    .adm-modal-backdrop {
      position: fixed; inset: 0; background: rgba(0,0,0,.55); display: none; align-items: center; justify-content: center;
      z-index: 200; padding: 1rem;
    }
    .adm-modal-backdrop.show { display: flex; }
    .adm-modal {
      background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 1rem 1.1rem; max-width: 440px; width: 100%;
      box-shadow: 0 16px 48px rgba(0,0,0,.35);
    }
    .adm-modal h2 { margin: 0 0 0.65rem; font-size: 1rem; }
    .adm-modal textarea {
      width: 100%; min-height: 6rem; padding: 0.55rem 0.65rem; border-radius: 10px; border: 1px solid var(--line);
      background: #0f141c; color: var(--text); font-size: 0.88rem; resize: vertical;
    }
    .adm-modal-actions { display: flex; justify-content: flex-end; gap: 0.45rem; margin-top: 0.75rem; }
    .adm-modal-actions button {
      font: inherit; font-weight: 650; font-size: 0.85rem; padding: 0.45rem 0.85rem; border-radius: 10px; cursor: pointer; border: 1px solid var(--line);
      background: var(--elev); color: var(--text);
    }
    .adm-modal-actions button.primary { border-color: var(--accent); background: rgba(59,130,246,.22); }
    .adm-records-empty { text-align: center; color: var(--muted); padding: 2rem; }
  </style>
</head>
<body>
  <header class="adm-header">
    <h1>Admin · Leave approvals</h1>
    <nav class="adm-nav">
      <a class="adm-nav-btn active" href="{{ admin_leave_href }}">Leave</a>
      <a class="adm-nav-btn" href="{{ admin_offset_href }}">Offset</a>
      <form method="post" action="{{ admin_logout_action }}" style="display:inline;margin:0;">
        <button type="submit" class="adm-nav-btn ghost">Logout</button>
      </form>
    </nav>
  </header>
  <main>
    <div class="adm-toolbar">
      <div class="adm-filters">
        <button type="button" class="env-filter-btn active" id="adm-filter-all" data-mode="all">ALL</button>
        <button type="button" class="env-filter-btn" id="adm-filter-todo" data-mode="todo">TO DO LIST</button>
      </div>
      <button type="button" class="env-filter-btn" id="adm-refresh">Refresh</button>
    </div>
    <div class="adm-table-wrap">
      <table>
        <thead id="adm-thead"></thead>
        <tbody id="adm-body"><tr><td class="adm-records-empty" colspan="20">Loading…</td></tr></tbody>
      </table>
    </div>
  </main>
  <div class="adm-modal-backdrop" id="adm-modal-backdrop" role="dialog" aria-modal="true">
    <div class="adm-modal">
      <h2>Remarks</h2>
      <textarea id="adm-modal-ta" placeholder="Optional remarks"></textarea>
      <div class="adm-modal-actions">
        <button type="button" id="adm-modal-cancel">Cancel</button>
        <button type="button" class="primary" id="adm-modal-confirm">Confirm</button>
      </div>
    </div>
  </div>
  <script>
  (function () {
    var API_LIST = {{ api_admin_leave_list_href | tojson }};
    var API_APPROVE = {{ api_admin_leave_approve_href | tojson }};
    var filterMode = "all";
    var backdrop = document.getElementById("adm-modal-backdrop");
    var modalTa = document.getElementById("adm-modal-ta");
    var modalRecordId = "";
    var modalStatus = "";
    var tbody = document.getElementById("adm-body");
    var thead = document.getElementById("adm-thead");

    function statusCell(status) {
      var td = document.createElement("td");
      var s = String(status || "").trim();
      if (!s) { td.className = "ose-cell-empty"; td.textContent = "—"; return td; }
      var span = document.createElement("span");
      span.className = "ose-pill ose-pill-status";
      var k = s.toLowerCase();
      if (k.indexOf("approv") >= 0) span.classList.add("is-approved");
      else if (k.indexOf("pend") >= 0) span.classList.add("is-pending");
      else if (k.indexOf("reject") >= 0) span.classList.add("is-rejected");
      span.textContent = s;
      td.appendChild(span);
      return td;
    }

    function textCell(val, extra) {
      var td = document.createElement("td");
      if (extra) td.className = extra;
      var s = String(val || "").trim();
      if (!s) { td.classList.add("ose-cell-empty"); td.textContent = "—"; }
      else { td.textContent = s; }
      return td;
    }

    function typeCell(val) {
      var td = document.createElement("td");
      var s = String(val || "").trim();
      if (!s) { td.className = "ose-cell-empty"; td.textContent = "—"; return td; }
      var span = document.createElement("span");
      span.className = "ose-pill ose-pill-leave";
      span.textContent = s;
      td.appendChild(span);
      return td;
    }

    function setRowDecision(tr, st) {
      tr.dataset.decision = st;
      var a = tr.querySelector(".adm-act-appr");
      var r = tr.querySelector(".adm-act-rej");
      if (a) a.classList.toggle("selected", st === "Approved");
      if (r) r.classList.toggle("selected", st === "Rejected");
    }

    function submitApprove(recordId, status, remarks, done) {
      fetch(API_APPROVE, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ record_id: recordId, status: status, remarks: remarks || "" })
      }).then(function (r) { return r.json(); }).then(function (data) {
        if (!data || !data.ok) throw new Error((data && data.error) || "Approve failed");
        done(null);
      }).catch(function (e) { done(e); });
    }

    function render(data) {
      var items = (data && data.items) || [];
      tbody.innerHTML = "";
      thead.innerHTML = "";
      var hr = document.createElement("tr");
      if (filterMode === "all") {
        ["Name", "Leave type", "Start date", "End date", "Reason", "Status", "Approver", "Approval date", "Remarks"].forEach(function (h) {
          var th = document.createElement("th"); th.textContent = h; hr.appendChild(th);
        });
        thead.appendChild(hr);
        if (!items.length) {
          var er = document.createElement("tr");
          var ec = document.createElement("td"); ec.colSpan = 9; ec.className = "adm-records-empty"; ec.textContent = "No leave records.";
          er.appendChild(ec); tbody.appendChild(er); return;
        }
        for (var i = 0; i < items.length; i++) {
          var it = items[i];
          var tr = document.createElement("tr");
          tr.appendChild(textCell(it.name, "ose-cell-name"));
          tr.appendChild(typeCell(it.leave_type));
          tr.appendChild(textCell(it.start_date, "ose-cell-date"));
          tr.appendChild(textCell(it.end_date, "ose-cell-date"));
          tr.appendChild(textCell(it.reason, "ose-cell-note"));
          tr.appendChild(statusCell(it.status));
          tr.appendChild(textCell(it.approver, "ose-cell-name"));
          tr.appendChild(textCell(it.approval_date, "ose-cell-date"));
          tr.appendChild(textCell(it.remarks, "ose-cell-note"));
          tbody.appendChild(tr);
        }
        return;
      }
      ["Name", "Leave type", "Start date", "End date", "Reason", "Status", "Approved", "Rejected", "Remarks"].forEach(function (h) {
        var th = document.createElement("th"); th.textContent = h; hr.appendChild(th);
      });
      thead.appendChild(hr);
      var pend = [];
      for (var j = 0; j < items.length; j++) {
        if (items[j].pending) pend.push(items[j]);
      }
      if (!pend.length) {
        var er2 = document.createElement("tr");
        var ec2 = document.createElement("td"); ec2.colSpan = 9; ec2.className = "adm-records-empty"; ec2.textContent = "No pending leave.";
        er2.appendChild(ec2); tbody.appendChild(er2); return;
      }
      for (var k = 0; k < pend.length; k++) {
        var p = pend[k];
        var tr2 = document.createElement("tr");
        tr2.dataset.recordId = String(p.record_id || "");
        tr2.appendChild(textCell(p.name, "ose-cell-name"));
        tr2.appendChild(typeCell(p.leave_type));
        tr2.appendChild(textCell(p.start_date, "ose-cell-date"));
        tr2.appendChild(textCell(p.end_date, "ose-cell-date"));
        tr2.appendChild(textCell(p.reason, "ose-cell-note"));
        tr2.appendChild(statusCell(p.status));
        var tdA = document.createElement("td");
        var bA = document.createElement("button"); bA.type = "button"; bA.className = "adm-act adm-act-appr"; bA.textContent = "Approved";
        bA.addEventListener("click", function () { setRowDecision(tr2, "Approved"); });
        tdA.appendChild(bA); tr2.appendChild(tdA);
        var tdR = document.createElement("td");
        var bR = document.createElement("button"); bR.type = "button"; bR.className = "adm-act adm-act-rej"; bR.textContent = "Rejected";
        bR.addEventListener("click", function () { setRowDecision(tr2, "Rejected"); });
        tdR.appendChild(bR); tr2.appendChild(tdR);
        var tdM = document.createElement("td");
        var bM = document.createElement("button"); bM.type = "button"; bM.className = "adm-act adm-act-rmk"; bM.textContent = "Remarks";
        bM.addEventListener("click", function () {
          var rid = tr2.dataset.recordId;
          var dec = tr2.dataset.decision || "";
          if (!dec) { window.alert("Choose Approved or Rejected first."); return; }
          modalRecordId = rid; modalStatus = dec; modalTa.value = "";
          backdrop.classList.add("show");
        });
        tdM.appendChild(bM); tr2.appendChild(tdM);
        tbody.appendChild(tr2);
      }
    }

    function loadList(forceRefresh) {
      var url = API_LIST;
      if (forceRefresh) url += (url.indexOf("?") >= 0 ? "&" : "?") + "refresh=1";
      tbody.innerHTML = '<tr><td class="adm-records-empty" colspan="20">Loading…</td></tr>';
      fetch(url).then(function (r) { return r.json(); }).then(function (data) {
        if (!data || data.ok === false) throw new Error((data && data.error) || "Load failed");
        render(data);
      }).catch(function () {
        render({ items: [] });
      });
    }

    document.getElementById("adm-filter-all").addEventListener("click", function () {
      filterMode = "all";
      document.getElementById("adm-filter-all").classList.add("active");
      document.getElementById("adm-filter-todo").classList.remove("active");
      loadList(false);
    });
    document.getElementById("adm-filter-todo").addEventListener("click", function () {
      filterMode = "todo";
      document.getElementById("adm-filter-todo").classList.add("active");
      document.getElementById("adm-filter-all").classList.remove("active");
      loadList(false);
    });
    document.getElementById("adm-refresh").addEventListener("click", function () { loadList(true); });

    document.getElementById("adm-modal-cancel").addEventListener("click", function () {
      backdrop.classList.remove("show");
    });
    backdrop.addEventListener("click", function (ev) {
      if (ev.target === backdrop) backdrop.classList.remove("show");
    });
    document.getElementById("adm-modal-confirm").addEventListener("click", function () {
      var rid = modalRecordId;
      var st = modalStatus;
      if (!rid || !st) { backdrop.classList.remove("show"); return; }
      var rm = modalTa.value || "";
      var btn = document.getElementById("adm-modal-confirm");
      btn.disabled = true;
      submitApprove(rid, st, rm, function (err) {
        btn.disabled = false;
        backdrop.classList.remove("show");
        if (err) window.alert(String(err));
        else loadList(true);
      });
    });

    loadList(false);
  })();
  </script>
</body>
</html>
"""


_ADMIN_OFFSET_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin · Offset</title>
  <style>
    :root {
      --bg: #0b0f14; --card: #151b26; --elev: #1c2533; --text: #e8edf4; --muted: #8b9cb3;
      --accent: #3b82f6; --line: #2a3544;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, rgba(59,130,246,.12), transparent), var(--bg);
      color: var(--text); min-height: 100vh;
    }
    header.adm-header {
      padding: 1rem 1.35rem; border-bottom: 1px solid var(--line);
      display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 0.75rem;
    }
    .adm-header h1 { margin: 0; font-size: 1.15rem; font-weight: 700; }
    .adm-nav { display: flex; flex-wrap: wrap; align-items: center; gap: 0.45rem; }
    a.adm-nav-btn, button.adm-nav-btn {
      font: inherit; font-size: 0.85rem; font-weight: 650; padding: 0.45rem 0.85rem; border-radius: 10px;
      border: 1px solid var(--line); background: var(--elev); color: var(--text); cursor: pointer;
      text-decoration: none; display: inline-block;
    }
    a.adm-nav-btn:hover, button.adm-nav-btn:hover { border-color: var(--accent); background: rgba(59,130,246,.12); text-decoration: none; }
    a.adm-nav-btn.active { border-color: var(--accent); background: rgba(59,130,246,.18); }
    button.adm-nav-btn.ghost { background: transparent; color: var(--muted); }
    main { padding: 1rem 1.35rem 2rem; max-width: 1400px; margin: 0 auto; width: 100%; }
    .adm-toolbar {
      display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 0.65rem; margin-bottom: 0.85rem;
    }
    .adm-filters { display: flex; flex-wrap: wrap; gap: 0.45rem; align-items: center; }
    .env-filter-btn {
      font: inherit; font-size: 0.8rem; font-weight: 600;
      padding: 0.4rem 0.8rem; border-radius: 999px; cursor: pointer; border: 1px solid var(--line);
      background: var(--elev); color: var(--muted);
    }
    .env-filter-btn:hover { border-color: var(--accent); color: var(--text); }
    .env-filter-btn.active { background: rgba(59,130,246,.25); border-color: var(--accent); color: var(--text); }
    .adm-table-wrap { overflow-x: auto; border-radius: 12px; border: 1px solid var(--line); }
    table { width: 100%; border-collapse: collapse; background: var(--elev); }
    th, td { padding: 0.55rem 0.65rem; text-align: left; border-bottom: 1px solid var(--line); font-size: 0.84rem; vertical-align: middle; }
    th {
      background: #222b3a; font-size: 0.66rem; text-transform: uppercase; letter-spacing: .06em; color: var(--muted);
      position: sticky; top: 0; z-index: 1;
    }
    tr:last-child td { border-bottom: none; }
    tbody tr:hover td { background: rgba(59,130,246,.06); }
    .ose-pill {
      display: inline-block; padding: 0.16rem 0.48rem; border-radius: 999px; font-size: 0.72rem; font-weight: 600;
      border: 1px solid transparent;
    }
    .ose-pill-status.is-approved { background: rgba(34,197,94,0.14); color: #86efac; border-color: rgba(34,197,94,0.28); }
    .ose-pill-status.is-pending { background: rgba(234,179,8,0.14); color: #fde68a; border-color: rgba(234,179,8,0.28); }
    .ose-pill-status.is-rejected { background: rgba(239,68,68,0.14); color: #fca5a5; border-color: rgba(239,68,68,0.28); }
    .ose-pill-status { background: rgba(139,156,179,0.16); color: #cbd5e1; border-color: rgba(139,156,179,0.28); }
    .ose-pill-shift { background: rgba(96,165,250,.12); color: #93c5fd; border-color: rgba(96,165,250,.28); }
    .ose-cell-empty { color: var(--muted); opacity: 0.65; }
    button.adm-act {
      font: inherit; font-size: 0.72rem; font-weight: 650; padding: 0.28rem 0.55rem; border-radius: 8px;
      border: 1px solid var(--line); background: var(--card); color: var(--muted); cursor: pointer; margin-right: 0.25rem;
    }
    button.adm-act-appr.selected { background: rgba(34,197,94,.18); border-color: rgba(34,197,94,.45); color: #86efac; }
    button.adm-act-rej.selected { background: rgba(239,68,68,.18); border-color: rgba(239,68,68,.45); color: #fca5a5; }
    button.adm-act-rmk { border-color: rgba(59,130,246,.4); color: var(--accent); }
    .adm-modal-backdrop {
      position: fixed; inset: 0; background: rgba(0,0,0,.55); display: none; align-items: center; justify-content: center;
      z-index: 200; padding: 1rem;
    }
    .adm-modal-backdrop.show { display: flex; }
    .adm-modal {
      background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 1rem 1.1rem; max-width: 440px; width: 100%;
      box-shadow: 0 16px 48px rgba(0,0,0,.35);
    }
    .adm-modal h2 { margin: 0 0 0.65rem; font-size: 1rem; }
    .adm-modal textarea {
      width: 100%; min-height: 6rem; padding: 0.55rem 0.65rem; border-radius: 10px; border: 1px solid var(--line);
      background: #0f141c; color: var(--text); font-size: 0.88rem; resize: vertical;
    }
    .adm-modal-actions { display: flex; justify-content: flex-end; gap: 0.45rem; margin-top: 0.75rem; }
    .adm-modal-actions button {
      font: inherit; font-weight: 650; font-size: 0.85rem; padding: 0.45rem 0.85rem; border-radius: 10px; cursor: pointer; border: 1px solid var(--line);
      background: var(--elev); color: var(--text);
    }
    .adm-modal-actions button.primary { border-color: var(--accent); background: rgba(59,130,246,.22); }
    .adm-records-empty { text-align: center; color: var(--muted); padding: 2rem; }
  </style>
</head>
<body>
  <header class="adm-header">
    <h1>Admin · Offset approvals</h1>
    <nav class="adm-nav">
      <a class="adm-nav-btn" href="{{ admin_leave_href }}">Leave</a>
      <a class="adm-nav-btn active" href="{{ admin_offset_href }}">Offset</a>
      <form method="post" action="{{ admin_logout_action }}" style="display:inline;margin:0;">
        <button type="submit" class="adm-nav-btn ghost">Logout</button>
      </form>
    </nav>
  </header>
  <main>
    <div class="adm-toolbar">
      <div class="adm-filters">
        <button type="button" class="env-filter-btn active" id="adm-filter-all" data-mode="all">ALL</button>
        <button type="button" class="env-filter-btn" id="adm-filter-todo" data-mode="todo">TO DO LIST</button>
      </div>
      <button type="button" class="env-filter-btn" id="adm-refresh">Refresh</button>
    </div>
    <div class="adm-table-wrap">
      <table>
        <thead id="adm-thead"></thead>
        <tbody id="adm-body"><tr><td class="adm-records-empty" colspan="20">Loading…</td></tr></tbody>
      </table>
    </div>
  </main>
  <div class="adm-modal-backdrop" id="adm-modal-backdrop" role="dialog" aria-modal="true">
    <div class="adm-modal">
      <h2>Remarks</h2>
      <textarea id="adm-modal-ta" placeholder="Optional remarks"></textarea>
      <div class="adm-modal-actions">
        <button type="button" id="adm-modal-cancel">Cancel</button>
        <button type="button" class="primary" id="adm-modal-confirm">Confirm</button>
      </div>
    </div>
  </div>
  <script>
  (function () {
    var API_LIST = {{ api_admin_offset_list_href | tojson }};
    var API_APPROVE = {{ api_admin_offset_approve_href | tojson }};
    var filterMode = "all";
    var backdrop = document.getElementById("adm-modal-backdrop");
    var modalTa = document.getElementById("adm-modal-ta");
    var modalRecordId = "";
    var modalStatus = "";
    var tbody = document.getElementById("adm-body");
    var thead = document.getElementById("adm-thead");

    function offStatus(it) {
      return String(it.approval_status || it.status || "").trim();
    }

    function statusCell(it) {
      var td = document.createElement("td");
      var s = offStatus(it);
      if (!s) { td.className = "ose-cell-empty"; td.textContent = "—"; return td; }
      var span = document.createElement("span");
      span.className = "ose-pill ose-pill-status";
      var k = s.toLowerCase();
      if (k.indexOf("approv") >= 0) span.classList.add("is-approved");
      else if (k.indexOf("pend") >= 0) span.classList.add("is-pending");
      else if (k.indexOf("reject") >= 0) span.classList.add("is-rejected");
      span.textContent = s;
      td.appendChild(span);
      return td;
    }

    function textCell(val, extra) {
      var td = document.createElement("td");
      if (extra) td.className = extra;
      var s = String(val || "").trim();
      if (!s) { td.classList.add("ose-cell-empty"); td.textContent = "—"; }
      else { td.textContent = s; }
      return td;
    }

    function shiftCell(val) {
      var td = document.createElement("td");
      var s = String(val || "").trim();
      if (!s) { td.className = "ose-cell-empty"; td.textContent = "—"; return td; }
      var span = document.createElement("span");
      span.className = "ose-pill ose-pill-shift";
      span.textContent = s;
      td.appendChild(span);
      return td;
    }

    function setRowDecision(tr, st) {
      tr.dataset.decision = st;
      var a = tr.querySelector(".adm-act-appr");
      var r = tr.querySelector(".adm-act-rej");
      if (a) a.classList.toggle("selected", st === "Approved");
      if (r) r.classList.toggle("selected", st === "Rejected");
    }

    function submitApprove(recordId, status, remarks, done) {
      fetch(API_APPROVE, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ record_id: recordId, status: status, remarks: remarks || "" })
      }).then(function (r) { return r.json(); }).then(function (data) {
        if (!data || !data.ok) throw new Error((data && data.error) || "Approve failed");
        done(null);
      }).catch(function (e) { done(e); });
    }

    function render(data) {
      var items = (data && data.items) || [];
      tbody.innerHTML = "";
      thead.innerHTML = "";
      var hr = document.createElement("tr");
      if (filterMode === "all") {
        ["Req. date", "Request person", "Exchange person", "Shift", "Original date", "Exchange date", "Reason", "Status", "Approver", "Approval date", "Remarks"].forEach(function (h) {
          var th = document.createElement("th"); th.textContent = h; hr.appendChild(th);
        });
        thead.appendChild(hr);
        if (!items.length) {
          var er = document.createElement("tr");
          var ec = document.createElement("td"); ec.colSpan = 11; ec.className = "adm-records-empty"; ec.textContent = "No offset records.";
          er.appendChild(ec); tbody.appendChild(er); return;
        }
        for (var i = 0; i < items.length; i++) {
          var it = items[i];
          var tr = document.createElement("tr");
          tr.appendChild(textCell(it.request_date, "ose-cell-date"));
          tr.appendChild(textCell(it.request_person, "ose-cell-name"));
          tr.appendChild(textCell(it.exchange_person, "ose-cell-name"));
          tr.appendChild(shiftCell(it.shift_type));
          tr.appendChild(textCell(it.original_date, "ose-cell-date"));
          tr.appendChild(textCell(it.exchange_date, "ose-cell-date"));
          tr.appendChild(textCell(it.reason, "ose-cell-note"));
          tr.appendChild(statusCell(it));
          tr.appendChild(textCell(it.approver, "ose-cell-name"));
          tr.appendChild(textCell(it.approval_date, "ose-cell-date"));
          tr.appendChild(textCell(it.remarks, "ose-cell-note"));
          tbody.appendChild(tr);
        }
        return;
      }
      ["Request person", "Exchange person", "Shift", "Original date", "Exchange date", "Reason", "Status", "Approved", "Rejected", "Remarks"].forEach(function (h) {
        var th = document.createElement("th"); th.textContent = h; hr.appendChild(th);
      });
      thead.appendChild(hr);
      var pend = [];
      for (var j = 0; j < items.length; j++) {
        if (items[j].pending) pend.push(items[j]);
      }
      if (!pend.length) {
        var er2 = document.createElement("tr");
        var ec2 = document.createElement("td"); ec2.colSpan = 10; ec2.className = "adm-records-empty"; ec2.textContent = "No pending offset.";
        er2.appendChild(ec2); tbody.appendChild(er2); return;
      }
      for (var k = 0; k < pend.length; k++) {
        var p = pend[k];
        var tr2 = document.createElement("tr");
        tr2.dataset.recordId = String(p.record_id || "");
        tr2.appendChild(textCell(p.request_person, "ose-cell-name"));
        tr2.appendChild(textCell(p.exchange_person, "ose-cell-name"));
        tr2.appendChild(shiftCell(p.shift_type));
        tr2.appendChild(textCell(p.original_date, "ose-cell-date"));
        tr2.appendChild(textCell(p.exchange_date, "ose-cell-date"));
        tr2.appendChild(textCell(p.reason, "ose-cell-note"));
        tr2.appendChild(statusCell(p));
        var tdA = document.createElement("td");
        var bA = document.createElement("button"); bA.type = "button"; bA.className = "adm-act adm-act-appr"; bA.textContent = "Approved";
        bA.addEventListener("click", function () { setRowDecision(tr2, "Approved"); });
        tdA.appendChild(bA); tr2.appendChild(tdA);
        var tdR = document.createElement("td");
        var bR = document.createElement("button"); bR.type = "button"; bR.className = "adm-act adm-act-rej"; bR.textContent = "Rejected";
        bR.addEventListener("click", function () { setRowDecision(tr2, "Rejected"); });
        tdR.appendChild(bR); tr2.appendChild(tdR);
        var tdM = document.createElement("td");
        var bM = document.createElement("button"); bM.type = "button"; bM.className = "adm-act adm-act-rmk"; bM.textContent = "Remarks";
        bM.addEventListener("click", function () {
          var rid = tr2.dataset.recordId;
          var dec = tr2.dataset.decision || "";
          if (!dec) { window.alert("Choose Approved or Rejected first."); return; }
          modalRecordId = rid; modalStatus = dec; modalTa.value = "";
          backdrop.classList.add("show");
        });
        tdM.appendChild(bM); tr2.appendChild(tdM);
        tbody.appendChild(tr2);
      }
    }

    function loadList(forceRefresh) {
      var url = API_LIST;
      if (forceRefresh) url += (url.indexOf("?") >= 0 ? "&" : "?") + "refresh=1";
      tbody.innerHTML = '<tr><td class="adm-records-empty" colspan="20">Loading…</td></tr>';
      fetch(url).then(function (r) { return r.json(); }).then(function (data) {
        if (!data || data.ok === false) throw new Error((data && data.error) || "Load failed");
        render(data);
      }).catch(function () {
        render({ items: [] });
      });
    }

    document.getElementById("adm-filter-all").addEventListener("click", function () {
      filterMode = "all";
      document.getElementById("adm-filter-all").classList.add("active");
      document.getElementById("adm-filter-todo").classList.remove("active");
      loadList(false);
    });
    document.getElementById("adm-filter-todo").addEventListener("click", function () {
      filterMode = "todo";
      document.getElementById("adm-filter-todo").classList.add("active");
      document.getElementById("adm-filter-all").classList.remove("active");
      loadList(false);
    });
    document.getElementById("adm-refresh").addEventListener("click", function () { loadList(true); });

    document.getElementById("adm-modal-cancel").addEventListener("click", function () {
      backdrop.classList.remove("show");
    });
    backdrop.addEventListener("click", function (ev) {
      if (ev.target === backdrop) backdrop.classList.remove("show");
    });
    document.getElementById("adm-modal-confirm").addEventListener("click", function () {
      var rid = modalRecordId;
      var st = modalStatus;
      if (!rid || !st) { backdrop.classList.remove("show"); return; }
      var rm = modalTa.value || "";
      var btn = document.getElementById("adm-modal-confirm");
      btn.disabled = true;
      submitApprove(rid, st, rm, function (err) {
        btn.disabled = false;
        backdrop.classList.remove("show");
        if (err) window.alert(String(err));
        else loadList(true);
      });
    });

    loadList(false);
  })();
  </script>
</body>
</html>
"""


_ENCODER_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Machine encoder</title>
  <style>
    :root {
      --bg: #0b0f14; --card: #151b26; --elev: #1c2533; --text: #e8edf4; --muted: #8b9cb3;
      --accent: #3b82f6; --line: #2a3544;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, rgba(59,130,246,.12), transparent), var(--bg);
      color: var(--text); min-height: 100vh;
    }
    header.wm-header-bar {
      padding: 1.1rem 1.35rem; border-bottom: 1px solid var(--line);
      display: flex; flex-wrap: wrap; align-items: flex-start; justify-content: space-between; gap: 1rem;
    }
    .wm-encoder-hero h1 { margin: 0; font-size: 1.35rem; font-weight: 700; letter-spacing: -0.02em; }
    a.wm-head-title-btn {
      text-decoration: none; display: inline-block; flex-shrink: 0;
      margin: 0; cursor: pointer; font: inherit; font-size: 0.95rem; font-weight: 650;
      padding: 0.5rem 1rem; border-radius: 10px; border: 1px solid var(--line); background: var(--elev); color: var(--text);
    }
    a.wm-head-title-btn:hover { border-color: var(--accent); background: rgba(59,130,246,.12); text-decoration: none; }
    .wm-head-nav-group { display: flex; flex-wrap: wrap; align-items: center; justify-content: flex-end; gap: 0.5rem; flex-shrink: 0; }
    a.wm-head-nav-btn.active { border-color: var(--accent); background: rgba(59,130,246,.18); color: var(--text); }
    a.wm-admin-btn {
      border-color: #b91c1c !important; background: #dc2626 !important; color: #fff !important;
    }
    a.wm-admin-btn:hover {
      border-color: #991b1b !important; background: #b91c1c !important; color: #fff !important;
      text-decoration: none;
    }
    main { padding: 1rem 1.35rem 2.5rem; max-width: 1280px; margin: 0 auto; width: 100%; }
    .env-filter-bar { display: flex; flex-wrap: wrap; align-items: center; gap: 0.45rem; margin-bottom: 0.75rem; }
    .env-filter-btn {
      font: inherit; font-size: 0.8rem; font-weight: 600;
      padding: 0.4rem 0.8rem; border-radius: 999px; cursor: pointer; border: 1px solid var(--line);
      background: var(--elev); color: var(--muted);
    }
    .env-filter-btn:hover { border-color: var(--accent); color: var(--text); }
    .env-filter-btn.active { background: rgba(59,130,246,.25); border-color: var(--accent); color: var(--text); }
    .toolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 0.65rem; margin-bottom: 0.85rem; }
    .toolbar input[type="search"] {
      flex: 1 1 220px; max-width: 420px; min-width: 180px;
      padding: 0.55rem 0.85rem; border-radius: 10px; border: 1px solid var(--line);
      background: #0f141c; color: var(--text); font-size: 0.92rem; outline: none;
    }
    .toolbar input[type="search"]:focus { border-color: var(--accent); box-shadow: 0 0 0 3px rgba(59,130,246,.2); }
    .filter-hint { font-size: 0.78rem; color: var(--muted); }
    .encoder-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 0.75rem; }
    .encoder-card {
      background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 0.85rem 0.95rem;
    }
    .encoder-card h3 { margin: 0 0 0.35rem; font-size: 0.98rem; }
    .encoder-card .site { font-size: 0.72rem; color: var(--muted); margin-bottom: 0.55rem; }
    .encoder-card dl { margin: 0; display: grid; grid-template-columns: minmax(0, 1fr); gap: 0.35rem; }
    .encoder-card dt { font-size: 0.68rem; text-transform: uppercase; letter-spacing: .05em; color: var(--muted); }
    .encoder-card dd { margin: 0 0 0.15rem; font-size: 0.82rem; word-break: break-word; }
    .encoder-empty { color: var(--muted); padding: 2rem 1rem; text-align: center; }
    .encoder-warn { color: #fbbf24; font-size: 0.78rem; margin: 0 0 0.75rem; }
    footer { padding: 1rem; text-align: center; color: var(--muted); font-size: 0.75rem; border-top: 1px solid var(--line); }
  </style>
</head>
<body>
  <header class="wm-header-bar">
    <div class="wm-encoder-hero"><h1>Machine encoder</h1></div>
    <div class="wm-head-nav-group">
      <a class="wm-head-title-btn wm-head-nav-btn" href="{{ machine_status_href }}">Machine status</a>
      <a class="wm-head-title-btn wm-head-nav-btn" href="{{ all_duty_href }}">All Duty</a>
      <a class="wm-head-title-btn wm-head-nav-btn active" href="{{ machine_encoder_href }}">Machine encoder</a>
      <a class="wm-head-title-btn wm-admin-btn" href="{{ admin_login_href }}">Admin Page</a>
    </div>
  </header>
  <main>
    <div class="env-filter-bar" id="wm-encoder-sites" role="toolbar" aria-label="Encoder site"></div>
    <div class="toolbar">
      <input type="search" id="wm-encoder-search" placeholder="Search machine, encoder IP, streaming URL…" autocomplete="off" aria-label="Search encoders"/>
      <button type="button" class="env-filter-btn" id="wm-encoder-refresh">Refresh</button>
      <span class="filter-hint" id="wm-encoder-hint"></span>
    </div>
    <p class="encoder-warn" id="wm-encoder-warn" hidden></p>
    <div class="encoder-grid" id="wm-encoder-grid"><div class="encoder-empty">Loading encoder data…</div></div>
  </main>
  <footer>API: <a href="{{ api_encoder_href }}">api/encoders</a></footer>
  <script>
  (function () {
    var API = {{ api_encoder_href | tojson }};
    var siteBar = document.getElementById("wm-encoder-sites");
    var grid = document.getElementById("wm-encoder-grid");
    var searchEl = document.getElementById("wm-encoder-search");
    var hintEl = document.getElementById("wm-encoder-hint");
    var warnEl = document.getElementById("wm-encoder-warn");
    var refreshBtn = document.getElementById("wm-encoder-refresh");
    var allItems = [];
    var siteKey = "";
    var sites = [];
    var siteCounts = {};
    function renderSites() {
      if (!siteBar) return;
      siteBar.innerHTML = "";
      var allBtn = document.createElement("button");
      allBtn.type = "button";
      allBtn.className = "env-filter-btn" + (siteKey ? "" : " active");
      allBtn.textContent = "Show all";
      allBtn.setAttribute("data-site", "");
      siteBar.appendChild(allBtn);
      for (var i = 0; i < sites.length; i++) {
        var s = sites[i];
        var btn = document.createElement("button");
        btn.type = "button";
        btn.className = "env-filter-btn" + (siteKey === s.key ? " active" : "");
        btn.textContent = s.label;
        btn.setAttribute("data-site", s.key);
        siteBar.appendChild(btn);
      }
    }
    function renderGrid() {
      if (!grid) return;
      var q = (searchEl && searchEl.value || "").trim().toLowerCase();
      var shown = [];
      for (var i = 0; i < allItems.length; i++) {
        var it = allItems[i];
        if (siteKey && String(it.site || "") !== siteKey) continue;
        if (q) {
          var hay = (String(it.machine || "") + " " + String(it.site_label || "")).toLowerCase();
          var fields = it.fields || [];
          for (var j = 0; j < fields.length; j++) {
            hay += " " + String(fields[j].label || "") + " " + String(fields[j].value || "");
          }
          if (hay.toLowerCase().indexOf(q) < 0) continue;
        }
        shown.push(it);
      }
      grid.innerHTML = "";
      if (!shown.length) {
        var empty = document.createElement("div");
        empty.className = "encoder-empty";
        empty.textContent = "No encoder records match the current filters.";
        grid.appendChild(empty);
      } else {
        for (var k = 0; k < shown.length; k++) {
          var item = shown[k];
          var card = document.createElement("article");
          card.className = "encoder-card";
          var h = document.createElement("h3");
          h.textContent = item.machine || "";
          card.appendChild(h);
          var site = document.createElement("div");
          site.className = "site";
          site.textContent = item.site_label || item.site || "";
          card.appendChild(site);
          var dl = document.createElement("dl");
          var fields2 = item.fields || [];
          for (var f = 0; f < fields2.length; f++) {
            var row = fields2[f];
            var value = String(row.value || "").trim();
            if (!value) continue;
            var dt = document.createElement("dt");
            dt.textContent = row.label || "";
            var dd = document.createElement("dd");
            dd.textContent = value;
            dl.appendChild(dt);
            dl.appendChild(dd);
          }
          card.appendChild(dl);
          grid.appendChild(card);
        }
      }
      if (hintEl) {
        var hint = "Showing " + shown.length + " of " + allItems.length;
        if (sites.length) {
          var bits = [];
          for (var si = 0; si < sites.length; si++) {
            var siteMeta = sites[si];
            bits.push(siteMeta.label + " " + String(siteCounts[siteMeta.key] || 0));
          }
          hint += " · " + bits.join(" · ");
        }
        hintEl.textContent = hint;
      }
    }
    function applyPayload(data) {
      allItems = (data && data.items) || [];
      sites = (data && data.sites) || [];
      siteCounts = (data && data.site_counts) || {};
      var errs = data && data.errors;
      if (warnEl) {
        var keys = errs ? Object.keys(errs) : [];
        if (keys.length) {
          warnEl.hidden = false;
          warnEl.textContent = "Some sites failed to load: " + keys.map(function (k) { return k + ": " + errs[k]; }).join("; ");
        } else {
          warnEl.hidden = true;
          warnEl.textContent = "";
        }
      }
      renderSites();
      renderGrid();
    }
    function loadEncoders(forceRefresh) {
      var url = API;
      if (forceRefresh) url += (url.indexOf("?") >= 0 ? "&" : "?") + "refresh=1";
      if (grid) grid.innerHTML = '<div class="encoder-empty">Loading encoder data…</div>';
      if (refreshBtn) refreshBtn.disabled = true;
      fetch(url).then(function (r) { return r.json(); }).then(applyPayload).catch(function (e) {
        if (grid) grid.innerHTML = '<div class="encoder-empty">' + String(e) + '</div>';
      }).finally(function () {
        if (refreshBtn) refreshBtn.disabled = false;
      });
    }
    if (siteBar) {
      siteBar.addEventListener("click", function (ev) {
        var btn = ev.target.closest("[data-site]");
        if (!btn || !siteBar.contains(btn)) return;
        siteKey = btn.getAttribute("data-site") || "";
        renderSites();
        renderGrid();
      });
    }
    if (searchEl) searchEl.addEventListener("input", renderGrid);
    if (refreshBtn) refreshBtn.addEventListener("click", function () { loadEncoders(true); });
    loadEncoders(false);
  })();
  </script>
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
                "belongs": r.get("belongs"),
                "name": r.get("name"),
                "game_type": r.get("game_type"),
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


_KNOWN_DEPLOYMENTS = frozenset({"PROD", "QAT", "UAT"})


def _filter_rows_by_deployment(rows: list[dict], deployment: str) -> list[dict]:
    dep = (deployment or "PROD").strip().upper() or "PROD"
    return [r for r in rows if str(r.get("environment") or "").upper() == dep]


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
        deployment = _cell(row, "deployment", "tier", "env_tier").upper()
        belongs = _cell(row, "belongs", "venue", "site_belong", "site_code")
        if deployment not in _KNOWN_DEPLOYMENTS:
            if env.upper() in _KNOWN_DEPLOYMENTS:
                deployment = env.upper()
            else:
                deployment = "PROD"
        if not belongs:
            if env and env.upper() not in _KNOWN_DEPLOYMENTS:
                belongs = env
            else:
                belongs = _cell(row, "site", "backend") or "—"
        name = _cell(row, "name", "machine", "machine_name", "id")
        game_type = _cell(row, "game_type", "game", "game_name", "gameType")
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
                "environment": deployment,
                "belongs": belongs or "—",
                "name": name or "—",
                "game_type": game_type or "—",
                "status": status or "—",
                "online_label": label,
                "online_raw": online_raw or "—",
                "pill_class": pill,
                "is_test": is_test,
            }
        )
    return out


def _compute_stats(rows: list[dict]) -> tuple[dict, list[dict]]:
    """Global counts and per-belongs counts (same keys)."""
    g = {"total": 0, "online": 0, "offline": 0, "conn_unknown": 0, "test": 0, "maintain": 0}
    if not rows:
        return g, []
    by_belongs: dict[str, dict] = {}
    for r in rows:
        label = str(r.get("belongs") or "—")
        if label not in by_belongs:
            by_belongs[label] = {
                "belongs": label,
                "total": 0,
                "online": 0,
                "offline": 0,
                "conn_unknown": 0,
                "test": 0,
                "maintain": 0,
            }
        be = by_belongs[label]
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
    belongs_list = sorted(by_belongs.values(), key=lambda x: str(x["belongs"]).lower())
    return g, belongs_list


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
        from smmachine import smachine_collect_machines_all_deployments
    except Exception as e:
        with _scrape_lock:
            _scrape_errs = {"_import": repr(e)}
            _scrape_ts = time.time()
            _scrape_rows = []
        return
    try:
        raw_rows, errs = smachine_collect_machines_all_deployments()
    except Exception as e:
        raw_rows, errs = [], {"_fatal": repr(e)}
    norm = _normalize_rows(raw_rows)
    norm.sort(
        key=lambda r: (
            (r.get("environment") or "").lower(),
            (r.get("belongs") or "").lower(),
            (r.get("name") or "").lower(),
        )
    )
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


def _machines_last_updated_str() -> str:
    """Local ``YYYY-mm-dd HH:MM:SS`` for last machine list refresh (live scrape time, else data file mtime)."""
    with _scrape_lock:
        ts = _scrape_ts
    if ts > 0:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    try:
        p = _data_json_path()
        if p.is_file():
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(p.stat().st_mtime))
    except OSError:
        pass
    return "—"


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
    return jsonify(ok=True, service="webapp", **meta)


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
        refresh_sec = int((os.environ.get("WEBMACHINE_REFRESH_SEC") or "0").strip() or "0")
    except ValueError:
        refresh_sec = 0
    if refresh_sec < 0:
        refresh_sec = 0
    if not _scrape_enabled():
        try:
            rows, _ = _load_raw_json()
        except ValueError as e:
            rows, _ = [], f"Error: {e}"
    else:
        rows, _ = _display_rows_and_provenance()
    prod_rows = _filter_rows_by_deployment(rows, "PROD")
    stats, stats_env = _compute_stats(prod_rows)
    return render_template_string(
        _PAGE,
        title=title,
        rows=rows,
        row_total=len(prod_rows),
        last_updated=_machines_last_updated_str(),
        stats=stats,
        stats_env=stats_env,
        refresh_sec=refresh_sec,
        api_href=url_for("wm.api_machines"),
        machine_status_href=url_for("wm.index"),
        all_duty_href=url_for("wm.all_duty"),
        machine_encoder_href=url_for("wm.machine_encoders"),
        admin_login_href=url_for("wm.admin_login"),
    )


@wm_bp.get("/machine-encoders")
def machine_encoders():
    return render_template_string(
        _ENCODER_PAGE,
        machine_status_href=url_for("wm.index"),
        all_duty_href=url_for("wm.all_duty"),
        machine_encoder_href=url_for("wm.machine_encoders"),
        api_encoder_href=url_for("wm.api_encoders"),
        admin_login_href=url_for("wm.admin_login"),
    )


@wm_bp.get("/api/encoders")
def api_encoders():
    site = (request.args.get("site") or "").strip()
    query = (request.args.get("q") or request.args.get("query") or "").strip()
    force_refresh = (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes")
    return jsonify(
        machine_encoder_payload(site=site, query=query, force_refresh=force_refresh)
    )


@wm_bp.get("/all-duty")
def all_duty():
    raw = (request.args.get("kind") or "ose").strip().lower()
    if raw not in _VALID_DUTY_KINDS:
        raw = "ose"
    return render_template_string(
        _ALL_DUTY_PAGE,
        back_href=url_for("wm.index"),
        machine_status_href=url_for("wm.index"),
        all_duty_href=url_for("wm.all_duty", kind=raw),
        machine_encoder_href=url_for("wm.machine_encoders"),
        api_calendar_href=url_for("wm.api_duty_calendar"),
        ose_submit_leave_href=url_for("wm.ose_submit_leave_page"),
        ose_submit_offset_href=url_for("wm.ose_submit_offset_page"),
        initial_kind=raw,
        admin_login_href=url_for("wm.admin_login"),
    )


@wm_bp.get("/admin")
def admin_login():
    return render_template_string(
        _ADMIN_LOGIN_PAGE,
        error="",
        admin_login_action=url_for("wm.admin_login_submit"),
    )


@wm_bp.post("/admin/login")
def admin_login_submit():
    login_id = (request.form.get("login_id") or "").strip()
    password = request.form.get("password") or ""
    try:
        import ose_Duty as od

        who = od.verify_webapp_admin_login(login_id, password)
    except ValueError:
        _track_admin_login_failure()
        return render_template_string(
            _ADMIN_LOGIN_PAGE,
            error="Invalid ID or password.",
            admin_login_action=url_for("wm.admin_login_submit"),
        )
    except RuntimeError as e:
        return render_template_string(
            _ADMIN_LOGIN_PAGE,
            error=str(e),
            admin_login_action=url_for("wm.admin_login_submit"),
        )
    session["admin_whologin"] = who
    _clear_admin_login_failures_for_current_ip()
    return redirect(url_for("wm.admin_leave"))


@wm_bp.get("/admin/leave")
def admin_leave():
    redir = _ensure_admin_session_redirect()
    if redir:
        return redir
    return render_template_string(
        _ADMIN_LEAVE_PAGE,
        admin_leave_href=url_for("wm.admin_leave"),
        admin_offset_href=url_for("wm.admin_offset"),
        admin_logout_action=url_for("wm.admin_logout"),
        api_admin_leave_list_href=url_for("wm.api_admin_leave_list"),
        api_admin_leave_approve_href=url_for("wm.api_admin_leave_approve"),
    )


@wm_bp.get("/admin/offset")
def admin_offset():
    redir = _ensure_admin_session_redirect()
    if redir:
        return redir
    return render_template_string(
        _ADMIN_OFFSET_PAGE,
        admin_leave_href=url_for("wm.admin_leave"),
        admin_offset_href=url_for("wm.admin_offset"),
        admin_logout_action=url_for("wm.admin_logout"),
        api_admin_offset_list_href=url_for("wm.api_admin_offset_list"),
        api_admin_offset_approve_href=url_for("wm.api_admin_offset_approve"),
    )


@wm_bp.post("/admin/logout")
def admin_logout():
    session.pop("admin_whologin", None)
    return redirect(url_for("wm.admin_login"))


@wm_bp.get("/api/admin/leave-list")
def api_admin_leave_list():
    if not _admin_session_who():
        return jsonify(ok=False, error="unauthorized"), 401
    try:
        import ose_Duty as od

        if (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes"):
            od.invalidate_ose_bitable_cache()
        return jsonify(od.get_ose_leave_records_admin())
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400


@wm_bp.get("/api/admin/offset-list")
def api_admin_offset_list():
    if not _admin_session_who():
        return jsonify(ok=False, error="unauthorized"), 401
    try:
        import ose_Duty as od

        if (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes"):
            od.invalidate_ose_bitable_cache()
        return jsonify(od.get_ose_offset_records_admin())
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400


@wm_bp.post("/api/admin/leave/approve")
def api_admin_leave_approve():
    who = _admin_session_who()
    if not who:
        return jsonify(ok=False, error="unauthorized"), 401
    body = request.get_json(silent=True) or {}
    record_id = str(body.get("record_id") or "").strip()
    status = str(body.get("status") or "").strip()
    remarks = str(body.get("remarks") or "")
    if not record_id:
        return jsonify(ok=False, error="record_id is required"), 400
    try:
        import ose_Duty as od

        out = od.update_ose_leave_approval(record_id=record_id, status=status, approver=who, remarks=remarks)
        return jsonify(out)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400


@wm_bp.post("/api/admin/offset/approve")
def api_admin_offset_approve():
    who = _admin_session_who()
    if not who:
        return jsonify(ok=False, error="unauthorized"), 401
    body = request.get_json(silent=True) or {}
    record_id = str(body.get("record_id") or "").strip()
    status = str(body.get("status") or "").strip()
    remarks = str(body.get("remarks") or "")
    if not record_id:
        return jsonify(ok=False, error="record_id is required"), 400
    try:
        import ose_Duty as od

        out = od.update_ose_offset_approval(record_id=record_id, status=status, approver=who, remarks=remarks)
        return jsonify(out)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400


@wm_bp.get("/all-duty/ose/submit-leave")
def ose_submit_leave_page():
    return render_template_string(
        _OSE_SUBMIT_LEAVE_PAGE,
        back_href=url_for("wm.all_duty", kind="ose"),
        api_ose_meta_href=url_for("wm.api_ose_submit_meta"),
        api_ose_leave_list_href=url_for("wm.api_ose_leave_list"),
        api_ose_submit_leave_href=url_for("wm.api_ose_submit_leave"),
    )


@wm_bp.get("/all-duty/ose/submit-offset")
def ose_submit_offset_page():
    return render_template_string(
        _OSE_SUBMIT_OFFSET_PAGE,
        back_href=url_for("wm.all_duty", kind="ose"),
        api_ose_meta_href=url_for("wm.api_ose_submit_meta"),
        api_ose_offset_list_href=url_for("wm.api_ose_offset_list"),
        api_ose_submit_offset_href=url_for("wm.api_ose_submit_offset"),
    )


@wm_bp.get("/ose-duty")
def ose_duty():
    return redirect(url_for("wm.all_duty", kind="ose"), code=302)


@wm_bp.get("/api/duty-calendar")
def api_duty_calendar():
    try:
        y = int((request.args.get("year") or str(date.today().year)).strip())
        m = int((request.args.get("month") or str(date.today().month)).strip())
    except ValueError:
        return jsonify(ok=False, error="Invalid year or month"), 400
    if m < 1 or m > 12:
        return jsonify(ok=False, error="month must be 1–12"), 400
    kind = (request.args.get("kind") or "ose").strip().lower()
    force_refresh = (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes")
    return jsonify(duty_calendar_payload(kind, y, m, force_refresh=force_refresh))


@wm_bp.get("/api/ose-duty-calendar")
def api_ose_duty_calendar():
    """Backward-compatible alias: same as ``/api/duty-calendar?kind=ose``."""
    try:
        y = int((request.args.get("year") or str(date.today().year)).strip())
        m = int((request.args.get("month") or str(date.today().month)).strip())
    except ValueError:
        return jsonify(ok=False, error="Invalid year or month"), 400
    if m < 1 or m > 12:
        return jsonify(ok=False, error="month must be 1–12"), 400
    force_refresh = (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes")
    return jsonify(duty_calendar_payload("ose", y, m, force_refresh=force_refresh))


@wm_bp.get("/api/ose/submit-meta")
def api_ose_submit_meta():
    import ose_Duty as od

    return jsonify(ok=True, **od.get_ose_submit_form_options())


@wm_bp.get("/api/ose/leave-list")
def api_ose_leave_list():
    try:
        import ose_Duty as od

        if (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes"):
            od.invalidate_ose_bitable_cache()
        return jsonify(od.get_ose_leave_records_list())
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400


@wm_bp.get("/api/ose/offset-list")
def api_ose_offset_list():
    try:
        import ose_Duty as od

        if (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes"):
            od.invalidate_ose_bitable_cache()
        return jsonify(od.get_ose_offset_records_list())
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400


@wm_bp.get("/api/ose/leave-calendar")
def api_ose_leave_calendar():
    try:
        y = int((request.args.get("year") or str(date.today().year)).strip())
        m = int((request.args.get("month") or str(date.today().month)).strip())
    except ValueError:
        return jsonify(ok=False, error="Invalid year or month"), 400
    try:
        import ose_Duty as od

        return jsonify(od.get_ose_leave_names_calendar(y, m))
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400


@wm_bp.post("/api/ose/submit-leave")
def api_ose_submit_leave():
    body = request.get_json(silent=True) or {}
    try:
        import ose_Duty as od

        out = od.submit_ose_leave(
            name=str(body.get("name") or ""),
            leave_type=str(body.get("leave_type") or ""),
            start_date=od._resolve_submit_date(
                month=body.get("start_month"),
                day=body.get("start_day"),
                raw_date=str(body.get("start_date") or ""),
            ),
            end_date=od._resolve_submit_date(
                month=body.get("end_month"),
                day=body.get("end_day"),
                raw_date=str(body.get("end_date") or ""),
            ),
            reason=str(body.get("reason") or ""),
        )
        return jsonify(out)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400


@wm_bp.post("/api/ose/submit-offset")
def api_ose_submit_offset():
    body = request.get_json(silent=True) or {}
    try:
        import ose_Duty as od

        out = od.submit_ose_offset(
            request_person=str(body.get("request_person") or ""),
            exchange_person=str(body.get("exchange_person") or ""),
            shift_type=str(body.get("shift_type") or ""),
            original_date=od._resolve_submit_date(
                month=body.get("original_month"),
                day=body.get("original_day"),
                raw_date=str(body.get("original_date") or ""),
            ),
            exchange_date=od._resolve_submit_date(
                month=body.get("exchange_month"),
                day=body.get("exchange_day"),
                raw_date=str(body.get("exchange_date") or ""),
            ),
            reason=str(body.get("reason") or ""),
        )
        return jsonify(out)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400


def register_webapp(flask_app: Flask, *, url_prefix: str | None = None) -> None:
    """
    Register routes on an existing Flask app (e.g. Duty ``main.app``).

    ``url_prefix`` ``None`` / ``""`` / ``"/"`` → mount at application root.
    Otherwise use e.g. ``"/wm"`` so the dashboard lives under that prefix.

    Background scraping starts when :func:`start_background_scrape_loop` is called (see ``main.py`` / ``webapp.main``); scrape is **on by default** unless ``WEBMACHINE_SCRAPE=0``.
    """
    flask_app.config.setdefault(
        "SECRET_KEY",
        os.environ.get("WEBAPP_SECRET_KEY") or os.environ.get("APP_SECRET") or "change-me",
    )
    pref = (url_prefix or "").strip()
    if pref in ("", "/"):
        flask_app.register_blueprint(wm_bp)
    else:
        if not pref.startswith("/"):
            pref = "/" + pref
        flask_app.register_blueprint(wm_bp, url_prefix=pref.rstrip("/") or "/")
    _ensure_machine_data_file()


register_webmachine = register_webapp


app = Flask(__name__)
app.config.setdefault(
    "SECRET_KEY",
    os.environ.get("WEBAPP_SECRET_KEY") or os.environ.get("APP_SECRET") or "change-me",
)
register_webapp(app, url_prefix=None)


def main() -> int:
    try:
        port = int((os.environ.get("WEBMACHINE_PORT") or "8765").strip() or "8765")
    except ValueError:
        print("WEBMACHINE_PORT must be an integer.", file=sys.stderr)
        return 1
    host = (os.environ.get("WEBMACHINE_HOST") or "0.0.0.0").strip() or "0.0.0.0"
    print(
        f"[webapp] http://{host}:{port}/  (Lark Web app homepage → public https URL that proxies here)\n"
        f"[webapp] Data: live scrape (default on) + {_ROOT / 'webmachine_data.json'}; WEBMACHINE_SCRAPE=0 to disable",
        flush=True,
    )
    start_background_scrape_loop()
    app.run(host=host, port=port, debug=_truthy_env("WEBMACHINE_DEBUG"), threaded=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
