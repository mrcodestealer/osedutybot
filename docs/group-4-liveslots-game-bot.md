# Group 4 — Liveslots & Game Bot

**Lark group:** Liveslots & Game Bot  
**Purpose:** Machine lookups on duty — asset info, Provider ID, CCTV, encoder data, set/unset maintenance, stress-test workflows, and prod-batch screenshots.

[← All groups](../README.md) · [中文](group-4-liveslots-game-bot.zh.md) · **[Full walkthroughs →](USER_GUIDE.md#7-group-4--machines-walkthroughs)**

---

## How to use

1. Open the **Liveslots & Game Bot** Lark group.
2. **@mention the bot**, then send your command.
3. For bulk ID expansion, use **`/list`** first, then site-specific commands.

> Credit / player logs → **Group 2**.  
> Jenkins / VPN / maintenance **email** → **Group 3**.  
> **Duty roster** `/liveslot` (who is on duty) → **Group 1**.

---

## Machine lookup

| Command | Description |
|---------|-------------|
| `/list <range>` | Expand IDs — e.g. `8900-8911`, `8905,8910`, `NWR2133-NWR2142` |
| `/nch <id>` | NCH machine — e.g. `/nch 1900`, `/nch nch2839 nch2378` |
| `/nwr <id>` | NWR machine — e.g. `/nwr 2005`, `/nwr nwr2005 nwr2006` |
| `/wf <id>` | Winford asset |
| `/tbp <id>` | TBP machine |
| `/cp <id>` | CP asset *(not `/cpms`)* |
| `/dhs <id>` | DHS asset |
| `/mdr <id>` | MDR asset |

Winford (`/wf`) responses include **Top Encoder** and **Main Encoder** fields from the asset sheet.

### Multi-machine syntax

All site commands accept several IDs in one message:

```
/nwr 2005,2006
/nwr nwr2005 nwr2006
/nch nch2839,nch2378
/cp cp2839 cp2378
```

Use `/list` when you have a **range** (e.g. `NWR2133-NWR2142`) and need each ID on its own line for maintenance.

---

## Provider, CCTV, IP

| Command | Description |
|---------|-------------|
| `/pid <id>` | Provider ID lookup |
| `/cctv <machine>` | EGM CCTV screenshot only (no credit check) |
| `/isp <ip> [ip ...]` | ISP / Organization, Country and ASN for one or more IPs |

**Examples:**

```
@Liveslots & Game Bot /pid 12345
@Liveslots & Game Bot /cctv OSMCP181
@Liveslots & Game Bot /cctv Dragons-0181
@Liveslots & Game Bot /isp 112.198.1.1
@Liveslots & Game Bot /isp 112.198.1.1 203.177.42.1 180.190.1.1
@Liveslots & Game Bot /isp 8.8.8.8,1.1.1.1
```

`/isp` merges six public IP-intel sources (IPinfo, ip-api, ipwho.is, ipapi.is, RIPEstat,
iplocation.net) and answers with an **IP Details** card. Values are merged across every IP
in the command, so three Globe IPs on two autonomous systems read `AS4775 / AS132199`. The
chip on each line names the leading source plus how many others agreed (`IPinfo +2`).
Private, loopback and reserved addresses are reported as such without a network call.

---

## Machine encoder (web UI)

Encoder IPs and streaming URLs are available on the bot's **web dashboard** (same server as `webapp.py`):

- Path: **`/machine-encoders`**
- Filter by site (NWR, WF, NCH, …) and search by machine name or encoder IP

Ask your admin for the base URL (typically `http://<server>:<PORT>/machine-encoders`). Data is refreshed from Google Sheets / `machineencoder.json`.

---

## Set / unset maintenance

### Wizard: `/sm`

```
@Liveslots & Game Bot /sm
```

Pick environment → action (set/unset maintenance and/or test) → confirm card → prod-batch job runs with optional **screenshots**.

**Important:** `/sm` is a **multi-step card wizard**. Do not close the card mid-flow; if it times out, send `/sm` again.

### Prod-batch command list (examples)

| Action | NWR example |
|--------|-------------|
| Set maintenance | `/nwrsetmaintenance` + one machine per line |
| Unset maintenance | `/nwrunsetmaintenance` |
| Set maint + test | `/nwrsetmaintenancetest` |
| Unset both | `/nwrunsetmaintenancetest` |

Replace `nwr` with `nch`, `wf`, `cp`, `tbp`, `dhs`, etc.

### Slash commands (prod-batch family)

Natural language or explicit slash — site prefix + action:

| Pattern | Example |
|---------|---------|
| `/<site>setmaintenance` | `/nwrsetmaintenance` + machine lines |
| `/<site>unsetmaintenance` | `/nwrunsetmaintenance` |
| `/<site>setmaintenancetest` | set maintenance **and** test |
| `/<site>unsetmaintenancetest` | unset both |

Sites: `nwr`, `nch`, `wf`, `cp`, `tbp`, `dhs`, …

**Natural language examples:**

```
@Liveslots & Game Bot nwr set maintenance NWR2113 NWR2114
@Liveslots & Game Bot unset maintenance for nch NCH1422
```

### Machine status (read-only)

Ask for status without changing anything — bot reads `webmachine_data.json`:

```
@Liveslots & Game Bot check status of NWR2113
@Liveslots & Game Bot is WF8145 in maintenance?
```

---

## Stress test

### Schedule reminder (announcement parsing)

@mention with a **scheduled** stress-test / maintenance announcement. The bot parses action time and machine list, then schedules a reminder **10 minutes before** action time:

```
@Liveslots & Game Bot Please set maintenance and test ALL WF MACHINES Good Fortune
later JUNE 09, 2026 09:45 pm, due to Stress Test.

5 Dragons-WF8145
Dragon of the Eastern Ocean-WF8146
...
```

At action time, staff run the actual set-maintenance flow (`/sm` or prod-batch), then confirm on the reminder card.

### `/stresstest` (explicit command)

Same parsing, but explicit — no @mention wording heuristics. Type `/stresstest` and paste the announcement on the next line(s). The bot reads the **set-maintenance date & time** (not the stress-test event time) and the **machine list**, schedules the reminder **10 minutes before**, and always replies — either echoing the parsed machines/times, or saying exactly what it could not find:

```
/stresstest
We have 4 DFDC machines subject for Stress Test July 15, 2026 at 11:00 AM.
Please set to maintain status and test mode July 14, 2026 at 2145H
- WF8109 ( 5 Treasures )
- WF8112 ( 5 Treasures )
```

### Stress-test screenshots

Prod-batch jobs can attach **machine screenshots** after set/unset (enabled via server env). Use `/sm` or prod-batch commands; screenshots appear in the thread reply.

---

## Natural language examples

| You say | Routed to |
|---------|-----------|
| `nwr 2133` | `/nwr 2133` |
| `provider id for …` | `/pid` |
| `isp lookup 8.8.8.8` / `who owns this ip address …` | `/isp` |
| `cctv Dragons-0181` | `/cctv` |
| `nwr set maintenance NWR2113` | prod-batch set maintenance |
| `all wf machines good fortune set maintenance` | `/sm` or announcement parser |

---

## FAQ

**`/cp` vs `/cpms`:** `/cp` is CP **asset** lookup (Group 4). `/cpms` is **department duty** (Group 1).

**CCTV vs credit:** `/cctv` is screenshot only. For credit/logs use **Group 2** (`/checkcredit`, `/stuckcredit`).

**Liveslot duty roster:** `/liveslot` (who is on Liveslot duty) lives in **Group 1** — this group is for **machine operations**, not the duty schedule.

**More help:** [User guide §7](USER_GUIDE.md#7-group-4--machines-walkthroughs) · [Encoder web UI](USER_GUIDE.md#78-machine-encoder-browser)
