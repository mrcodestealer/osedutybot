# Group 1 — OSE Duty Bot

**Lark group:** OSE Duty Bot (main group)  
**Purpose:** Highest-frequency duty lookups — rosters, leave/WFH, holidays, offset/leave workflows, and incident routing helpers.

[← All groups](../README.md) · [中文](group-1-ose-duty-bot.zh.md) · **[Full walkthroughs →](USER_GUIDE.md#4-group-1--duty-walkthroughs)**

---

## How to use

1. Open the **OSE Duty Bot** Lark group (or DM the bot).
2. In a **group chat**, type **`@OSE Duty Bot`** then your command or question.
3. Send **`/help`** for the full command card (all groups share the same backend).

### First-time checklist

- [ ] You are in the **OSE Duty Bot** group (not Log/Ops/Machine groups).
- [ ] In group chat you typed **`@`** and selected the bot before the command.
- [ ] For leave/offset forms you used keywords like `leave` or `offset` after the @mention.

---

## General commands

| Command | Description |
|---------|-------------|
| `/help` | Command list |
| `/s <name>` | Search duty roster by name |
| `/date` | Today's date |
| `/holiday` | Upcoming public holidays |
| `/holidaythismonth` | Holidays this month |
| `/leave [dept]` | Leave this month — e.g. `/leave fpms`, `/leave ote`, `/leave bi` |
| `/wfh [dept]` | WFH this month — e.g. `/wfh fpms`, `/wfh sre` |
| `/leavewfh [dept]` | Leave + WFH (alias `/wfhleave`) — e.g. `/leavewfh cpms` |
| `/wholeave` | Who is on leave today (OSE leave Bitable) |

---

## Department duty

| Command | Description |
|---------|-------------|
| `/fpms` | FPMS duty (today) |
| `/fpmscheck [MM/YYYY]` | FPMS missing-duty report |
| `/pms` | PMS duty (next days) |
| `/pmscheck [MM/YYYY]` | PMS missing-duty |
| `/bi` | BI duty (today) |
| `/bicheck [MM/YYYY]` | BI missing-duty |
| `/fe` | FE duty (next 3 days) |
| `/fecheck [MM/YYYY]` | FE missing-duty |
| `/cpms` | CPMS duty (3 days) |
| `/cpmscheck [MM/YYYY]` | CPMS missing-duty |
| `/sre` | SRE this & next week |
| `/srecheck [MM/YYYY]` | SRE missing-duty |
| `/db` or `/dba` | DB duty (3 weeks) |
| `/dbcheck [MM/YYYY]` | DB missing-duty |
| `/liveslot` | Liveslot duty roster (3 weeks) |
| `/liveslotcheck [MM/YYYY]` | Liveslot missing-duty |
| `/ote` | OTE (3 weeks) |
| `/otecheck [MM/YYYY]` | OTE missing-duty |
| `/ft` | FT 3 days + contact FYI |
| `/ftcheck [MM/YYYY]` | FT missing-duty |
| `/ose` | OSE duty card (now) |
| `/osedate DD/MM/YYYY` | OSE duty on a date |
| `/dutycheckall [MM/YYYY]` | All departments missing-duty |
| `/ecsre [<game>]` | EC SRE game owner |
| `/ec [<game>]` | Emergency contacts |

Most department commands append **today's leave/WFH** for that dept at the bottom of the reply.

> **Machine lookups** (`/nwr`, `/cctv`, etc.) → use **Group 4**.  
> **Log / credit checks** → use **Group 2**.

---

## OSE offset & leave

Use **@mention + phrase** (no slash required for forms):

| Phrase / keyword | Description |
|------------------|-------------|
| `showoffset [month]` | Monthly offset calendar — e.g. 五月有谁offset |
| `@bot offset` | Offset (调休) application form |
| `@bot leave` | Leave application form |
| `editoffset` | Edit pending offset |
| `deleteoffset` | Delete offset (approvers can delete pending) |
| `pendingoffset` | Approver queue |

### Offset / leave form — step by step

1. Send `@OSE Duty Bot offset` or `@OSE Duty Bot leave`.
2. A **form card** appears — in groups it is usually **only visible to you**.
3. Fill required fields (dates, shift type, exchange person for offset, reason).
4. Tap **Submit**.
5. **Pending:** use `editoffset` or `deleteoffset` to change your request.
6. **Approvers:** use `pendingoffset` to see the queue and approve on the card.

Slash optional: `/editoffset`, `/pendingoffset`, `/showoffset` also work.

---

## Incident helpers (AI)

| Command | Description |
|---------|-------------|
| `/checkperson <issue>` | Suggest Issue / Priority / Department / Check Person from past tickets |

---

## Reminders

| Command | Description |
|---------|-------------|
| `/reminder <time> <msg>` | One-off reminder — e.g. `/reminder 1h30m Team meeting` |
| `/addreminder …` | Sheet reminder / form |
| `/deletereminder [id]` | Delete or list reminders |

Natural language: `@bot add timer 5mins lunch`

---

## Natural language examples

| You say | Result |
|---------|--------|
| `who is on fpms duty` / `今天谁值班 fpms` | FPMS today |
| `show holidays this month` | Monthly holidays |
| `apply for annual leave` / `@bot leave` | Leave form |
| `swap my duty shift` / `@bot offset` | Offset form |
| `pending offset approvals` | Approver queue |

---

## FAQ

**Use this group for** daily “who is on duty?”, leave/WFH, holidays, and offset/leave — not for logs or machine maintenance.

**Date format:** `/osedate DD/MM/YYYY`; month checks use `MM/YYYY` or `YYYY-MM` (e.g. `/fpmscheck 06/2026`).

**`/leave fpms` unknown department?** Valid keys: `fpms`, `ote`, `bi`, `fe`, `sre`, `db`, `dba`, `cpms`, `pms`, `ft`.

**More help:** [User guide §4](USER_GUIDE.md#4-group-1--duty-walkthroughs) · [Troubleshooting](USER_GUIDE.md#9-troubleshooting)
