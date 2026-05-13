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
**name** / ``machine``; **status**; **online** / ``online_offline`` / ``state``. Optional boolean **is_test** (or ``(TEST)`` in the name) for dashboard counts.

The HTML dashboard shows **deployment** tabs (PROD / QAT / UAT), **global and per-belongs** totals, and a **search** box; ``GET /api/machines`` includes a **stats** object with the same numbers.

Optional: ``WEBMACHINE_API_TOKEN`` — ``GET /api/machines`` requires ``Authorization: Bearer <token>``.

Env: ``WEBMACHINE_PORT``, ``WEBMACHINE_HOST``, ``WEBMACHINE_TITLE``, ``WEBMACHINE_REFRESH_SEC`` (default **0** = no auto page reload), ``WEBMACHINE_DEBUG``.
"""

from __future__ import annotations

import calendar
import json
import os
import sys
import threading
import time
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

from flask import Blueprint, Flask, jsonify, redirect, render_template_string, request, url_for

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
    }
    .wm-header-bar {
      display: flex; flex-wrap: wrap; align-items: flex-start; justify-content: space-between; gap: 1rem;
    }
    a.wm-head-nav-btn {
      text-decoration: none; display: inline-block; flex-shrink: 0; align-self: flex-start;
    }
    a.wm-head-nav-btn:hover { text-decoration: none; }
    .wm-head-top { display: flex; flex-direction: column; align-items: flex-start; gap: 0.55rem; }
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
      <button type="button" class="wm-head-title-btn" id="wm-title-btn" aria-label="Scroll to dashboard content">{{ title }}</button>
      <p class="wm-head-total">Total <span class="count">{{ row_total }}</span> of machines detected</p>
      <p class="wm-head-updated">Last Updated : {{ last_updated }}</p>
    </div>
    <a class="wm-head-title-btn wm-head-nav-btn" href="{{ all_duty_href }}">All Duty</a>
  </header>
  <script>
  (function () {
    var btn = document.getElementById("wm-title-btn");
    var main = document.getElementById("wm-main");
    if (btn && main) {
      btn.addEventListener("click", function () {
        main.scrollIntoView({ behavior: "smooth", block: "start" });
      });
    }
  })();
  </script>
  <main id="wm-main">
    {% if rows %}
    <section class="panel" aria-label="Deployment tier">
      <div class="deployment-filter-bar" id="wm-dep-filters" role="toolbar" aria-label="Deployment tier">
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
      <input type="search" id="wm-search" placeholder="Search belongs, machine, status, online…" autocomplete="off" aria-label="Filter machines"/>
      <span class="filter-hint" id="wm-filter-hint"></span>
    </div>
    <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Environment</th>
          <th>Belongs</th>
          <th>Machine name</th>
          <th>Test</th>
          <th>Status</th>
          <th>Online / Offline</th>
        </tr>
      </thead>
      <tbody id="wm-tbody">
        {% for r in rows %}
        <tr data-deployment="{{ r.environment|e }}" data-belongs="{{ r.belongs|e }}" data-name="{{ r.name|e }}" data-status="{{ r.status|e }}" data-online="{{ r.online_label|e }}"
            data-test="{% if r.is_test %}1{% else %}0{% endif %}"
            data-maint="{% if 'maintain' in ((r.status or '')|lower) %}1{% else %}0{% endif %}"
            data-offline="{% if r.pill_class == 'pill-offline' %}1{% else %}0{% endif %}"
            data-online1="{% if r.pill_class == 'pill-online' %}1{% else %}0{% endif %}">
          <td>{{ r.environment }}</td>
          <td>{{ r.belongs }}</td>
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
            var label = [depSel];
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


def duty_calendar_payload(kind: str, year: int, month: int) -> dict[str, object]:
    """JSON for All Duty calendar (duty modules + phones for all kinds except OSE)."""
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
      <p>Calendars use the same modules as the bot (see filenames below). <strong>Phone numbers</strong> show for every roster <em>except OSE</em>. OSE keeps morning/night + leave/offset only.</p>
      <p style="margin-top:0.45rem"><code>ote_duty.py</code>, <code>ose_Duty.py</code>, <code>fpms_duty.py</code>, <code>ft.py</code>, <code>fe_duty.py</code>, <code>bi_duty.py</code>, <code>cpms_duty.py</code>, <code>db_duty.py</code>, <code>sre_Duty.py</code></p>
    </div>
    <a class="wm-head-title-btn" href="{{ back_href }}">Machine status</a>
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

    function setYearLabel() { yearLabel.textContent = String(year); }

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
        all_duty_href=url_for("wm.all_duty"),
    )


@wm_bp.get("/all-duty")
def all_duty():
    raw = (request.args.get("kind") or "ose").strip().lower()
    if raw not in _VALID_DUTY_KINDS:
        raw = "ose"
    return render_template_string(
        _ALL_DUTY_PAGE,
        back_href=url_for("wm.index"),
        api_calendar_href=url_for("wm.api_duty_calendar"),
        initial_kind=raw,
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
    return jsonify(duty_calendar_payload(kind, y, m))


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
    return jsonify(duty_calendar_payload("ose", y, m))


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
