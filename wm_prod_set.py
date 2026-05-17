"""HTML template for SET PROD MACHINE page (imported by webapp)."""

PROD_SET_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{{ title }}</title>
  <style>
    :root {
      --bg: #0b0f14; --card: #151b26; --elev: #1c2533; --line: #2a3544;
      --text: #e8edf4; --muted: #8b9cb3; --accent: #3b82f6; --ok: #22c55e; --bad: #ef4444; --warn: #eab308;
    }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }
    .wm-header-bar {
      display: flex; flex-wrap: wrap; align-items: flex-start; justify-content: space-between; gap: 1rem 1.25rem;
      padding: 1rem 1.35rem; border-bottom: 1px solid var(--line); background: var(--card);
    }
    .wm-head-title { margin: 0; font-size: 1.35rem; font-weight: 700; }
    .wm-head-nav-group { display: flex; flex-wrap: wrap; gap: 0.5rem; }
    .wm-head-title-btn {
      display: inline-flex; align-items: center; padding: 0.45rem 0.85rem; border-radius: 8px;
      border: 1px solid var(--line); background: var(--elev); color: var(--muted); font-size: 0.82rem; font-weight: 600; text-decoration: none;
    }
    .wm-head-title-btn.active, .wm-head-title-btn:hover { border-color: var(--accent); color: var(--text); }
    main { padding: 1rem 1.35rem 2rem; max-width: 1400px; margin: 0 auto; }
    .panel { background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 1rem; margin-bottom: 1rem; }
    .panel-title { margin: 0 0 0.75rem; font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: .08em; color: var(--muted); }
    .env-filter-row { display: flex; flex-wrap: wrap; align-items: center; gap: 0.5rem 0.65rem; margin-bottom: 0.75rem; }
    .env-filter-btn {
      font: inherit; font-size: 0.8rem; font-weight: 600; padding: 0.4rem 0.85rem; border-radius: 999px;
      border: 1px solid var(--line); background: var(--elev); color: var(--muted); cursor: pointer;
    }
    .env-filter-btn.active { background: rgba(59,130,246,.25); border-color: var(--accent); color: var(--text); }
    .env-refresh-row { display: flex; flex-wrap: wrap; align-items: center; gap: 0.4rem 0.5rem; margin-top: 0.65rem; padding-top: 0.65rem; border-top: 1px solid var(--line); }
    .env-refresh-btn {
      font: inherit; font-size: 0.72rem; font-weight: 600; padding: 0.32rem 0.65rem; border-radius: 8px;
      border: 1px solid var(--line); background: #0f141c; color: var(--muted); cursor: pointer;
    }
    .env-refresh-btn:hover:not(:disabled) { border-color: var(--ok); color: var(--ok); }
    .env-refresh-btn:disabled { opacity: 0.55; cursor: wait; }
    .env-refresh-btn.refresh-all { border-color: rgba(34,197,94,.45); color: var(--ok); }
    #ps-load-status { margin: 0 0 0.75rem; font-size: 0.85rem; color: var(--muted); min-height: 1.25rem; }
    #ps-load-status.err { color: #fca5a5; }
    .toolbar-row { display: flex; flex-wrap: wrap; align-items: flex-start; gap: 0.75rem; margin-bottom: 0.75rem; }
    .toolbar-row input[type="search"] {
      flex: 1 1 280px; min-width: 200px; padding: 0.55rem 0.85rem; border-radius: 10px;
      border: 1px solid var(--line); background: #0f141c; color: var(--text); font-size: 0.92rem;
    }
    .action-col { display: flex; flex-direction: column; gap: 0.4rem; }
    .action-row { display: flex; flex-wrap: wrap; gap: 0.4rem; }
    .btn-action {
      font: inherit; font-size: 0.78rem; font-weight: 600; padding: 0.45rem 0.75rem; border-radius: 8px;
      border: 1px solid var(--accent); background: rgba(59,130,246,.15); color: var(--text); cursor: pointer;
    }
    .btn-action:hover { background: rgba(59,130,246,.3); }
    .btn-action:disabled { opacity: 0.45; cursor: not-allowed; }
    .table-wrap { overflow-x: auto; border: 1px solid var(--line); border-radius: 10px; }
    table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
    th, td { padding: 0.5rem 0.65rem; border-bottom: 1px solid var(--line); text-align: left; }
    th { background: #222b3a; font-size: 0.68rem; text-transform: uppercase; color: var(--muted); position: sticky; top: 0; }
    tr:hover td { background: rgba(59,130,246,.06); }
    tr.row-hidden { display: none; }
    .pill { display: inline-block; padding: 0.15rem 0.45rem; border-radius: 999px; font-size: 0.7rem; font-weight: 600; }
    .pill-online { background: rgba(34,197,94,.15); color: var(--ok); }
    .pill-offline { background: rgba(239,68,68,.15); color: var(--bad); }
    .pill-maintain { background: rgba(248,113,113,.12); color: #fca5a5; }
    .pill-test { background: rgba(234,179,8,.14); color: var(--warn); }
    .pill-normal { background: rgba(34,197,94,.12); color: var(--ok); }
    .pill-occupy { background: rgba(234,179,8,.14); color: var(--warn); }
    .modal-backdrop {
      display: none; position: fixed; inset: 0; background: rgba(0,0,0,.55); z-index: 1000;
      align-items: center; justify-content: center;
    }
    .modal-backdrop.open { display: flex; }
    .modal-box {
      background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 1.25rem;
      width: min(420px, 92vw); box-shadow: 0 20px 50px rgba(0,0,0,.45);
    }
    .modal-box h3 { margin: 0 0 0.5rem; font-size: 1.1rem; }
    .modal-box p { margin: 0 0 0.75rem; color: var(--muted); font-size: 0.88rem; }
    .modal-box textarea {
      width: 100%; min-height: 80px; padding: 0.6rem; border-radius: 8px; border: 1px solid var(--line);
      background: #0f141c; color: var(--text); font-family: inherit; resize: vertical;
    }
    .modal-actions { display: flex; justify-content: flex-end; gap: 0.5rem; margin-top: 0.75rem; }
    .modal-actions button {
      font: inherit; padding: 0.5rem 1rem; border-radius: 8px; cursor: pointer; font-weight: 600;
    }
    .btn-secondary { border: 1px solid var(--line); background: var(--elev); color: var(--text); }
    .btn-primary { border: none; background: var(--accent); color: #fff; }
    .job-banner {
      display: none; margin-bottom: 1rem; padding: 0.85rem 1rem; border-radius: 10px;
      border: 1px solid var(--line); background: var(--elev);
    }
    .job-banner.visible { display: block; }
    .job-banner.fail { border-color: var(--bad); }
    .fail-list { margin: 0.5rem 0; padding-left: 1.2rem; color: #fecaca; font-size: 0.88rem; }
    .empty { color: var(--muted); text-align: center; padding: 2rem; }
  </style>
</head>
<body>
  <header class="wm-header-bar">
    <div class="wm-head-top">
      <h1 class="wm-head-title">{{ title }}</h1>
      <p style="margin:0.35rem 0 0;color:var(--muted);font-size:0.88rem;">Batch set maintenance / test on backend EGM status (PROD only)</p>
    </div>
    <div class="wm-head-nav-group">
      <a class="wm-head-title-btn active" href="{{ prod_set_href }}">SET PROD MACHINE</a>
      <a class="wm-head-title-btn" href="{{ machine_status_href }}">Machine status</a>
      <a class="wm-head-title-btn" href="{{ all_duty_href }}">All Duty</a>
      <a class="wm-head-title-btn" href="{{ machine_encoder_href }}">Machine encoder</a>
      <a class="wm-head-title-btn" href="{{ admin_login_href }}">Admin Page</a>
    </div>
  </header>
  <main>
    <div id="job-banner" class="job-banner" role="status"></div>
    <section class="panel">
      <h2 class="panel-title">Environment</h2>
      <div class="env-filter-row" id="ps-env-filters">
        <button type="button" class="env-filter-btn active" data-ps-env="ALL">Show all</button>
        {% for code in env_codes %}
        <button type="button" class="env-filter-btn" data-ps-env="{{ code }}">{{ code }}</button>
        {% endfor %}
      </div>
      <div class="env-refresh-row" id="ps-refresh-row">
        <button type="button" class="env-refresh-btn refresh-all" data-refresh-belongs="ALL">Refresh all PROD</button>
        {% for code in env_codes %}
        <button type="button" class="env-refresh-btn" data-refresh-belongs="{{ code }}">Refresh {{ code }}</button>
        {% endfor %}
      </div>
    </section>
    <section class="panel">
      <h2 class="panel-title">Machines</h2>
      <p id="ps-load-status" role="status"></p>
      <div class="toolbar-row">
        <input type="search" id="ps-search" placeholder="Search belongs, machine, game type, status, online…" autocomplete="off"/>
        <div class="action-col">
          <div class="action-row">
            <button type="button" class="btn-action" data-action="set_maint">Set maintenance</button>
            <button type="button" class="btn-action" data-action="set_test">Set test</button>
            <button type="button" class="btn-action" data-action="set_both">Set maintenance and Set test</button>
          </div>
          <div class="action-row">
            <button type="button" class="btn-action" data-action="unset_maint">Unset maintenance</button>
            <button type="button" class="btn-action" data-action="unset_test">Unset test</button>
            <button type="button" class="btn-action" data-action="unset_both">Unset maintenance and unset test</button>
          </div>
        </div>
      </div>
      <div class="table-wrap">
        <table id="ps-table">
          <thead>
            <tr>
              <th style="width:36px"><input type="checkbox" id="ps-select-all-visible" title="Select all visible rows"/></th>
              <th>Belongs</th>
              <th>Machine</th>
              <th>Game type</th>
              <th>Status</th>
              <th>Online</th>
            </tr>
          </thead>
          <tbody id="ps-tbody"></tbody>
        </table>
      </div>
      <p id="ps-empty" class="empty" style="display:none;">No machines match filters.</p>
    </section>
  </main>

  <div id="remark-modal" class="modal-backdrop" aria-hidden="true">
    <div class="modal-box" role="dialog" aria-labelledby="remark-title">
      <h3 id="remark-title">Confirm batch operation</h3>
      <p id="remark-hint">Optional remark (max 100 characters). Leave blank if not needed.</p>
      <textarea id="remark-input" maxlength="100" placeholder="Please enter remark"></textarea>
      <div class="modal-actions">
        <button type="button" class="btn-secondary" id="remark-cancel">Cancel</button>
        <button type="button" class="btn-primary" id="remark-confirm">Confirm</button>
      </div>
    </div>
  </div>

  <script>
    const API_MACHINES = {{ api_machines_json|safe }};
    const API_REFRESH = {{ api_refresh_json|safe }};
    const API_JOB = "{{ api_job_url }}";
    const API_CANCEL = "{{ api_cancel_url }}";
    const PS_ENVS = {{ env_codes_json|safe }};
    let allRows = [];
    let activeEnv = "ALL";
    let searchQ = "";
    let pendingAction = null;
    let pollTimer = null;
    let currentJobId = null;

    function esc(s) {
      const d = document.createElement("div");
      d.textContent = s == null ? "" : String(s);
      return d.innerHTML;
    }

    function statusPill(s) {
      const t = String(s || "").toLowerCase();
      let cls = "pill-normal";
      if (t.includes("maintain")) cls = "pill-maintain";
      else if (t.includes("test")) cls = "pill-test";
      else if (t.includes("occupy")) cls = "pill-occupy";
      else if (t === "normal") cls = "pill-normal";
      return `<span class="pill ${cls}">${esc(s || "—")}</span>`;
    }

    function onlinePill(s) {
      const t = String(s || "").toLowerCase();
      const cls = t === "online" ? "pill-online" : (t === "offline" ? "pill-offline" : "pill-normal");
      return `<span class="pill ${cls}">${esc(s || "—")}</span>`;
    }

    /**
     * Classify machine by display name (not belongs column).
     * WF = WF* or winford only — never NWR.
     * NWR = NWR* / NWR+digits in name.
     */
    function machineEnvFromName(machineName) {
      const raw = String(machineName || "").trim();
      if (!raw) return null;
      const seg = raw.replace(/\\/g, "/").split("/").pop().trim();
      const alnum = seg.replace(/[^A-Za-z0-9]/g, "").toUpperCase();
      if (/^DHS/i.test(seg) || alnum.startsWith("DHS")) return "DHS";
      if (/^NCH/i.test(seg) || alnum.startsWith("NCH")) return "NCH";
      if (/^OSM/i.test(seg) || alnum.startsWith("OSM")) return "CP";
      if (/^CP/i.test(seg) || alnum.startsWith("CP")) return "CP";
      if (/^MDR/i.test(seg) || alnum.startsWith("MDR")) return "MDR";
      if (/^TBR/i.test(seg) || alnum.startsWith("TBR")) return "TBR";
      if (/^TBP/i.test(seg) || alnum.startsWith("TBP")) return "TBP";
      if (/^NWR/i.test(seg) || alnum.startsWith("NWR") || /NWR\\d/.test(alnum)) return "NWR";
      if (/winford/i.test(raw)) return "WF";
      if (/^WF/i.test(seg) || alnum.startsWith("WF")) return "WF";
      return null;
    }

    function rowMatchesEnv(r, envCode) {
      if (envCode === "ALL") return true;
      return machineEnvFromName(r.machine) === envCode;
    }

    function rowMatches(r) {
      if (!rowMatchesEnv(r, activeEnv)) return false;
      if (!searchQ) return true;
      const hay = [r.belongs, r.machine, r.game_type, r.status, r.online, r.maintain, r.test].join(" ").toLowerCase();
      return hay.includes(searchQ);
    }

    function visibleRows() {
      return allRows.filter(rowMatches);
    }

    function setLoadStatus(msg, isErr) {
      const el = document.getElementById("ps-load-status");
      if (!el) return;
      el.textContent = msg || "";
      el.classList.toggle("err", !!isErr);
    }

    function renderTable() {
      const tbody = document.getElementById("ps-tbody");
      const vis = visibleRows();
      const emptyEl = document.getElementById("ps-empty");
      emptyEl.style.display = vis.length ? "none" : "block";
      if (!vis.length && !allRows.length) {
        emptyEl.textContent = "No PROD machines loaded. Use Refresh CP / WF / … above to pull live data from EGM.";
      } else if (!vis.length) {
        emptyEl.textContent = "No machines match filters.";
      }
      tbody.innerHTML = vis.map(r => {
        const key = `${r.belongs}::${r.machine}`;
        return `<tr data-key="${esc(key)}" data-belongs="${esc(r.belongs)}" data-machine="${esc(r.machine)}">
          <td><input type="checkbox" class="ps-row-cb" data-key="${esc(key)}"/></td>
          <td>${esc(r.belongs)}</td>
          <td>${esc(r.machine)}</td>
          <td>${esc(r.game_type || "—")}</td>
          <td>${statusPill(r.status)}</td>
          <td>${onlinePill(r.online)}</td>
        </tr>`;
      }).join("");
    }

    function setEnvActive(code) {
      activeEnv = code;
      document.querySelectorAll(".env-filter-btn[data-ps-env]").forEach(b => {
        b.classList.toggle("active", b.getAttribute("data-ps-env") === code);
      });
      renderTable();
    }

    function selectedMachines() {
      const keys = new Set();
      document.querySelectorAll(".ps-row-cb:checked").forEach(cb => {
        const tr = cb.closest("tr");
        if (!tr || tr.classList.contains("row-hidden")) return;
        keys.add(cb.getAttribute("data-key"));
      });
      return allRows.filter(r => keys.has(`${r.belongs}::${r.machine}`));
    }

    async function loadMachines() {
      const res = await fetch(API_MACHINES);
      const data = await res.json();
      if (data.error) throw new Error(data.error || "load failed");
      const raw = data.machines || data.rows || [];
      allRows = raw
        .filter(r => (r.environment || "PROD").toUpperCase() === "PROD")
        .map(r => ({
          belongs: r.belongs || "",
          machine: r.name || r.machine || "",
          game_type: r.game_type || "",
          status: r.status || "",
          online: r.online_label || r.online || "",
          maintain: r.maintain || "",
          test: r.is_test || r.test || "",
        }));
      const src = data.source || "";
      const n = allRows.length;
      if (n) {
        setLoadStatus(`${n} PROD machine(s)${src ? " · " + src : ""}`);
      } else {
        setLoadStatus(
          (src && !src.includes("Waiting"))
            ? `0 PROD machines (${src}). Click Refresh … to scrape live EGM.`
            : "No data yet — click Refresh CP / WF / … to load from live EGM backends."
        );
      }
      renderTable();
    }

    async function refreshBelongs(belongs, btn) {
      const label = belongs === "ALL" ? "all PROD sites" : belongs;
      if (btn) btn.disabled = true;
      document.querySelectorAll("[data-refresh-belongs]").forEach(b => { b.disabled = true; });
      setLoadStatus(`Refreshing ${label} from live EGM (Playwright)… this may take a few minutes.`);
      try {
        const res = await fetch(API_REFRESH, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ belongs }),
        });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "refresh failed");
        let msg = `Loaded ${data.count} machine(s) for ${belongs} · ${data.source || "live scrape"}`;
        if (data.warning) msg += ` · ${data.warning}`;
        setLoadStatus(msg);
        await loadMachines();
      } catch (e) {
        setLoadStatus("Refresh failed: " + e.message, true);
      } finally {
        document.querySelectorAll("[data-refresh-belongs]").forEach(b => { b.disabled = false; });
      }
    }

    function openRemarkModal(action) {
      pendingAction = action;
      const labels = {
        set_maint: "Set maintenance",
        set_test: "Set test",
        set_both: "Set maintenance and Set test",
        unset_maint: "Unset maintenance",
        unset_test: "Unset test",
        unset_both: "Unset maintenance and unset test",
      };
      document.getElementById("remark-hint").textContent =
        `Action: ${labels[action] || action}. Machines selected: ${selectedMachines().length}`;
      document.getElementById("remark-input").value = "";
      document.getElementById("remark-modal").classList.add("open");
    }

    function closeRemarkModal() {
      document.getElementById("remark-modal").classList.remove("open");
      pendingAction = null;
    }

    async function startJob(action, remark) {
      const machines = selectedMachines();
      if (!machines.length) {
        alert("Select at least one machine in the list.");
        return;
      }
      const res = await fetch(API_JOB, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action, remark, machines }),
      });
      const data = await res.json();
      if (data.error) {
        alert(data.error || "Failed to start job");
        return;
      }
      currentJobId = data.job_id;
      showJobBanner(data.message || "Job started. Processing…", false);
      if (pollTimer) clearInterval(pollTimer);
      pollTimer = setInterval(pollJob, 2500);
      pollJob();
    }

    async function pollJob() {
      if (!currentJobId) return;
      const res = await fetch(API_JOB.replace("/jobs", "/jobs/" + encodeURIComponent(currentJobId)));
      const data = await res.json();
      if (!data.ok) return;
      if (data.status === "running" || data.status === "pending") {
        showJobBanner(data.message || "Processing…", false);
        return;
      }
      clearInterval(pollTimer);
      pollTimer = null;
      if (data.status === "awaiting_manual") {
        let html = `<strong>${esc(data.message || "Some machines failed")}</strong>`;
        if (data.failed && data.failed.length) {
          html += "<ul class=\"fail-list\">" + data.failed.map(f =>
            `<li>${esc(f.belongs)} — ${esc(f.machine)} (${esc(f.reason || "failed")})</li>`
          ).join("") + "</ul>";
        }
        html += `<div class="modal-actions" style="margin-top:0.75rem"><button type="button" class="btn-primary" id="ps-do-manual">Do manually</button></div>`;
        const banner = document.getElementById("job-banner");
        banner.className = "job-banner visible fail";
        banner.innerHTML = html;
        document.getElementById("ps-do-manual").onclick = async () => {
          await fetch(API_JOB.replace("/jobs/", "/jobs/" + encodeURIComponent(currentJobId) + "/manual"), { method: "POST" });
          pollTimer = setInterval(pollJob, 2500);
        };
        return;
      }
      if (data.status === "cancelled") {
        showJobBanner(data.message || "Cancelled.", false);
        currentJobId = null;
        return;
      }
      if (data.status === "done") {
        let html = `<strong>${esc(data.message || "Done")}</strong>`;
        if (data.summary) {
          const s = data.summary;
          html += `<p style="color:var(--muted);font-size:0.88rem;">Success: ${(s.success||[]).length}, Failed: ${(s.failed||[]).length}</p>`;
        }
        showJobBanner(html, false);
        currentJobId = null;
        loadMachines();
      }
    }

    function showJobBanner(html, isFail) {
      const b = document.getElementById("job-banner");
      b.className = "job-banner visible" + (isFail ? " fail" : "");
      b.innerHTML = html;
    }

    async function cancelJob() {
      if (!currentJobId) return;
      await fetch(API_CANCEL.replace("JOB_ID", encodeURIComponent(currentJobId)), { method: "POST" });
    }

    document.querySelectorAll(".env-filter-btn[data-ps-env]").forEach(btn => {
      btn.addEventListener("click", () => setEnvActive(btn.getAttribute("data-ps-env")));
    });
    document.getElementById("ps-search").addEventListener("input", e => {
      searchQ = e.target.value.trim().toLowerCase();
      renderTable();
    });
    document.getElementById("ps-select-all-visible").addEventListener("change", e => {
      document.querySelectorAll("#ps-tbody tr").forEach(tr => {
        if (!tr.classList.contains("row-hidden")) {
          const cb = tr.querySelector(".ps-row-cb");
          if (cb) cb.checked = e.target.checked;
        }
      });
    });
    document.querySelectorAll(".btn-action").forEach(btn => {
      btn.addEventListener("click", () => {
        if (!selectedMachines().length) {
          alert("Select at least one machine.");
          return;
        }
        openRemarkModal(btn.getAttribute("data-action"));
      });
    });
    document.getElementById("remark-cancel").addEventListener("click", closeRemarkModal);
    document.getElementById("remark-confirm").addEventListener("click", () => {
      const action = pendingAction;
      const remark = document.getElementById("remark-input").value.trim();
      closeRemarkModal();
      if (action) startJob(action, remark);
    });

    document.querySelectorAll("[data-refresh-belongs]").forEach(btn => {
      btn.addEventListener("click", () => {
        refreshBelongs(btn.getAttribute("data-refresh-belongs"), btn);
      });
    });

    loadMachines().catch(e => {
      document.getElementById("ps-empty").style.display = "block";
      document.getElementById("ps-empty").textContent = "Failed to load machines: " + e.message;
    });
  </script>
</body>
</html>
"""
