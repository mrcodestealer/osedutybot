# Duty Bot — Complete User Guide

Step-by-step guide for using the OSE Lark bots. For command lists by group, see the [group docs](README.md).

[中文完整指南](USER_GUIDE.zh.md) · [← Index](../README.md)

---

## 1. Before you start

### Join the right Lark group

| What you need | Group | Doc |
|---------------|-------|-----|
| Who is on duty, leave, holidays, offset | **OSE Duty Bot** | [Group 1](group-1-ose-duty-bot.md) |
| Credit, logs, stuck credit, SMS OTP | **Log & Credit Bot** | [Group 2](group-2-log-credit-bot.md) |
| Jenkins, VPN, maintenance email | **Ops & Maintenance Bot** | [Group 3](group-3-ops-maintenance-bot.md) |
| Machine info, CCTV, maintenance on floor | **Liveslots & Game Bot** | [Group 4](group-4-liveslots-game-bot.md) |

### @mention rule (most important)

```
┌─────────────────────────────────────────────────────────┐
│  GROUP CHAT          │  PRIVATE CHAT (DM)                │
├──────────────────────┼───────────────────────────────────┤
│  MUST @ bot first    │  No @ needed                      │
│  @Bot /fpms          │  /fpms                            │
│  @Bot who is on duty │  who is on duty                   │
└──────────────────────┴───────────────────────────────────┘
```

If the bot **does not reply** in a group, you probably forgot to **@mention** it.

### Slash commands vs natural language

| Style | When to use | Example |
|-------|-------------|---------|
| **Slash** `/command` | Exact, fast, always works | `@Bot /fpms` |
| **Natural language** | Casual phrasing (needs AI on server) | `@Bot who is on fpms duty today` |

Both work in group (with @) or DM. When unsure, use **`/help`** or a slash command.

---

## 2. How replies work

### Text vs interactive cards

| Reply type | What it looks like | What you do |
|------------|-------------------|-------------|
| **Plain text** | Normal message | Read the answer |
| **Interactive card** | Buttons, forms, dropdowns | Fill fields → tap **Submit** / **Confirm** |
| **Thread reply** | Reply under your message | Long jobs (credit, Jenkins) often reply **in a thread** — open the thread to see progress |

### “Please wait” messages

Many commands start a **background job** (browser, Jenkins, log scrape). You will see:

```
⏳ Running checkcredit, browser may take a while — please wait...
```

- **Do not** send the same command again immediately.
- Check the **thread** under your message for the final result.
- First request after server idle can take **30s–2min**.

### Buttons on cards

| Button | Meaning |
|--------|---------|
| **Confirm** / **Submit** | Proceed with the action |
| **Cancel** | Abort; you may need to start over |
| **I have set maintenance** | On stress-test reminder — tap after you finished on floor |

---

## 3. Date & time formats (cheat sheet)

| Use case | Format | Example |
|----------|--------|---------|
| OSE duty on a date | `DD/MM/YYYY` | `/osedate 30/06/2026` |
| Missing-duty month check | `MM/YYYY` or `YYYY-MM` | `/fpmscheck 06/2026` |
| Credit / machine log | `YYYY-MM-DD` | `/checkcredit OSMCP181 2026-06-28` |
| Amount Loss | `DD/MM` | `/al 28/06` |
| Reminder duration | `1h30m`, `5mins`, `45m` | `/reminder 1h30m Lunch` |
| Reminder clock time | `8:39PM`, `at 2039` | `/reminder 8:39PM Standup` |

---

## 4. Group 1 — Duty (walkthroughs)

### 4.1 Check who is on duty today

**Fastest (slash):**

```
@OSE Duty Bot /fpms
@OSE Duty Bot /bi
@OSE Duty Bot /ose
```

**Natural language:**

```
@OSE Duty Bot who is on fpms duty today?
@OSE Duty Bot 今天谁值班 bi？
```

**All departments missing-duty this month:**

```
@OSE Duty Bot /fpmscheck
@OSE Duty Bot /fpmscheck 06/2026
@OSE Duty Bot /dutycheckall 06/2026
```

The reply often includes **today's leave/WFH** for that department at the bottom.

### 4.2 Search roster by name

```
@OSE Duty Bot /s john
@OSE Duty Bot /s 王明
```

### 4.3 Leave & WFH this month

Opens an **interactive month card** (optional department filter):

```
@OSE Duty Bot /leave
@OSE Duty Bot /leave fpms
@OSE Duty Bot /wfh sre
@OSE Duty Bot /leavewfh cpms
```

Valid department keys: `fpms`, `ote`, `bi`, `fe`, `sre`, `db`, `dba`, `cpms`, `pms`, `ft`.

**Who is on leave today (OSE Bitable):**

```
@OSE Duty Bot /wholeave
```

### 4.4 Apply for leave (form)

1. In **Group 1**, send: `@OSE Duty Bot leave` or `@OSE Duty Bot offset` (for 调休).
2. Bot sends a **form card visible only to you** (ephemeral in group).
3. Fill: dates, type, reason, exchange person (offset), etc.
4. Tap **Submit**.
5. Approvers get a notification card; you get status updates when approved/rejected.

**Other offset keywords:**

| Step | Command |
|------|---------|
| View monthly offset calendar | `showoffset` or `五月有谁offset` |
| Edit your pending request | `editoffset` |
| Approver: pending queue | `pendingoffset` |

### 4.5 Holidays

```
@OSE Duty Bot /holiday
@OSE Duty Bot /holidaythismonth
@OSE Duty Bot /date
```

### 4.6 Incident helpers

Paste the player report after the command:

```
@OSE Duty Bot /checkperson player stuck at loading screen after top up
```

### 4.7 Reminders

```
@OSE Duty Bot /reminder 30m Check ticket
@OSE Duty Bot /reminder 8:39PM Team sync
@OSE Duty Bot add timer 5mins coffee
```

---

## 5. Group 2 — Log & credit (walkthroughs)

### 5.1 Full credit check (recommended for new users)

**Step 1** — Start the card wizard:

```
@Log & Credit Bot /checkcreditdate
```

**Step 2** — On the card, enter:
- Machine name (e.g. `OSMCP181`, `Dragons-0181`, `DHS3077`)
- Player ID (if known)
- Date (default today)

**Step 3** — Submit. Wait for thread reply with log screenshots / Third Http detail.

### 5.2 Quick credit (you know machine + date)

```
@Log & Credit Bot /checkcredit OSMCP181
@Log & Credit Bot /checkcredit 1171 2026-06-28
@Log & Credit Bot /checkcredit Dragons-0181
```

Date omitted = **today**.

### 5.3 Stuck credit on floor

```
@Log & Credit Bot /stuckcredit NWR2938
@Log & Credit Bot /stuckcredit DHS3077 2026-06-26
```

Returns log summary + Third Http transfer-out info.

### 5.4 Machine log (last player, errors)

```
@Log & Credit Bot /checkmachinelog DHS3077
@Log & Credit Bot /checkmachinelog NWR2938 2026-06-26
```

### 5.5 Errors only

```
@Log & Credit Bot /machineerror OSMCP181
```

### 5.6 SMS OTP

```
@Log & Credit Bot /smsfail
@Log & Credit Bot /smscheckplayer 127317237
@Log & Credit Bot /smscheckplayer 7052472, 1069954565
```

Multiple player IDs: commas, spaces, or newlines.

### 5.7 Amount Loss

```
@Log & Credit Bot /al
@Log & Credit Bot /al 28/06
```

---

## 6. Group 3 — Ops (walkthroughs)

### 6.1 Jenkins build

**Step 1** — List keywords (once, or when unsure):

```
@Ops & Maintenance Bot /help jenkins
```

**Step 2** — Start build:

```
@Ops & Maintenance Bot /update np
```

**Step 3** — Read the **confirmation card** — verify job name and parameters.

**Step 4** — Tap **Confirm** to start build. Follow thread for result.

**Batch updates:**

```
@Ops & Maintenance Bot /updatemore
UPDATE np
UPDATE fpms
...
```

### 6.2 Create VPN

**Step 1:**

```
@Ops & Maintenance Bot /createvpn
```

**Step 2** — Fill the card (users, location, etc.) → Submit.

**Step 3** — Wait for Jenkins/build flow in thread.

If session expired: send `/createvpn` again.

### 6.3 Find old VPN config

```
@Ops & Maintenance Bot /findvpn alex
```

### 6.4 Maintenance email

Paste email body after command:

```
@Ops & Maintenance Bot /ms
<paste email content here>
```

Or EVO batch:

```
@Ops & Maintenance Bot /m <EVO batch details>
```

**Cashout template:**

```
@Ops & Maintenance Bot /cashout
```

---

## 7. Group 4 — Machines (walkthroughs)

### 7.1 Look up one machine

```
@Liveslots & Game Bot /nwr 2133
@Liveslots & Game Bot /nwr nwr2005 nwr2006
@Liveslots & Game Bot /nch 1900
@Liveslots & Game Bot /wf 8092
@Liveslots & Game Bot /dhs 3050
```

IDs flexible: `2133`, `nwr2133`, comma or space separated.

### 7.2 Expand a range first

```
@Liveslots & Game Bot /list 8900-8911
@Liveslots & Game Bot /list NWR2133-NWR2142
```

Then copy IDs into `/nwr` or maintenance commands.

### 7.3 Provider ID · IP / ISP lookup

```
@Liveslots & Game Bot /pid 30
@Liveslots & Game Bot /pid 30 31 32
```

```
@Liveslots & Game Bot /isp 112.198.1.1
@Liveslots & Game Bot /isp 112.198.1.1 203.177.42.1 180.190.1.1
@Liveslots & Game Bot /isp 8.8.8.8,1.1.1.1
```

`/isp` answers with an **IP Details** card — ISP / Organization, Country and ASN — merged
across six public sources and across every IP you pass, so several IPs on different
networks read as `AS4775 / AS132199`. IPv6 works. Private and reserved addresses are
named as such instead of being looked up.

### 7.4 CCTV screenshot (no credit)

```
@Liveslots & Game Bot /cctv OSMCP181
@Liveslots & Game Bot /cctv Dragons-0181
```

Use **same machine label** as credit commands.

### 7.5 Set maintenance — wizard `/sm`

**Step 1:**

```
@Liveslots & Game Bot /sm
```

**Step 2** — Pick environment (NWR, NCH, WF, …).

**Step 3** — Pick action: set/unset maintenance and/or test.

**Step 4** — Enter or confirm machine list on card.

**Step 5** — Confirm. Job runs; screenshots may appear in thread.

### 7.6 Set maintenance — direct command

```
@Liveslots & Game Bot /nwrsetmaintenance
NWR2113
NWR2114
```

Or natural language:

```
@Liveslots & Game Bot nwr set maintenance NWR2113 NWR2114
```

Other patterns: `/nwrunsetmaintenance`, `/nchsetmaintenancetest`, etc.

### 7.7 Stress-test reminder

Paste full announcement with **date/time** and machine list:

```
@Liveslots & Game Bot Please set maintenance and test ALL WF MACHINES Good Fortune
later JUNE 09, 2026 09:45 pm, due to Stress Test.

5 Dragons-WF8145
Dragon of the Eastern Ocean-WF8146
```

Bot schedules reminder **10 minutes before** action time. At action time: run `/sm` or prod-batch, then tap **I have set maintenance** on the reminder card.

Or use the explicit command — `/stresstest` + paste the announcement on the next line (always replies with the parsed machines and times, or with what is missing):

```
/stresstest
We have 4 DFDC machines subject for Stress Test July 15, 2026 at 11:00 AM.
Please set to maintain status and test mode July 14, 2026 at 2145H
- WF8109 ( 5 Treasures )
- WF8112 ( 5 Treasures )
```

### 7.8 Machine encoder (browser)

Open (ask admin for host): `http://<server>:<PORT>/machine-encoders`

Search by machine name or encoder IP; filter by site (NWR, WF, NCH, …).

---

## 8. Decision tree — “which group?”

```
Need to know WHO is on duty?          → Group 1  /fpms, /ose, …
Need PLAYER credit or LOG?            → Group 2  /checkcredit, /stuckcredit
Need JENKINS or VPN?                  → Group 3  /update, /createvpn
Need MACHINE asset info or CCTV?      → Group 4  /nwr, /cctv, /pid
Need the ISP / country / ASN of an IP? → Group 4  /isp
Need to SET maintenance on machines?  → Group 4  /sm, /nwrsetmaintenance
Leave / offset application?           → Group 1  @bot leave, @bot offset
```

---

## 9. Troubleshooting

| Problem | Fix |
|---------|-----|
| No reply in group | **@mention the bot** |
| `❌ Usage:` error | Copy the format from the error message; check date format table above |
| Stuck on “please wait” | Open **thread**; wait 1–2 min; don’t spam same command |
| Card button does nothing | Session expired — run the command again (`/createvpn`, `/sm`, `/checkcreditdate`) |
| Natural language ignored | Use slash command; or ask admin if `BOT_USE_AI=1` |
| Wrong data / empty | Sheet or credentials issue — tell bot admin |
| Used wrong group | Command may still work but team prefers correct group — see decision tree |

---

## 10. Quick reference card

Print or pin in your group:

```
@Bot /help              Full command list
@Bot /help jenkins      Jenkins keywords (Group 3)

Group 1: /fpms /leave /wholeave /osedate /offset
Group 2: /checkcreditdate /stuckcredit /smsfail
Group 3: /update /createvpn /findvpn /ms
Group 4: /nwr /cctv /pid /isp /sm
```

More detail: [Group 1](group-1-ose-duty-bot.md) · [Group 2](group-2-log-credit-bot.md) · [Group 3](group-3-ops-maintenance-bot.md) · [Group 4](group-4-liveslots-game-bot.md)
