# Group 3 — Ops & Maintenance Bot

**Lark group:** Ops & Maintenance Bot  
**Purpose:** Jenkins builds, VPN, maintenance email parsing, deploy/restart, cashout templates, and other ops — kept away from daily duty chat.

[← All groups](../README.md) · [中文](group-3-ops-maintenance-bot.zh.md) · **[Full walkthroughs →](USER_GUIDE.md#6-group-3--ops-walkthroughs)**

---

## How to use

1. Open the **Ops & Maintenance Bot** Lark group.
2. **@mention the bot**, then send your command.
3. Jenkins: **`/help jenkins`** lists all job keywords for `/update`.

> Machine set/unset maintenance and CCTV → **Group 4**.  
> Credit / logs → **Group 2**.

---

## Jenkins

| Command | Description |
|---------|-------------|
| `/update <keyword>` | Match Jenkins job → confirm on card → build |
| `/updatemore …` | Multiple updates (one `UPDATE …` block per job) |
| `/jenkinsupdate …` | Alias of `/update` |
| `/updatejenkins …` | Alias of `/update` |
| `/help jenkins` | List all registered job keywords |

**Example:**

```
@Ops & Maintenance Bot /update np
@Ops & Maintenance Bot /help jenkins
```

Natural language: `update np backend`, `jenkins build fpms`

### Jenkins — step by step

1. `@Ops & Maintenance Bot /help jenkins` — find the keyword for your service.
2. `@Ops & Maintenance Bot /update <keyword>` — e.g. `/update np`.
3. Read the **confirmation card** (job name, branch, parameters).
4. Tap **Confirm** only if correct.
5. Watch the **thread** for build progress / success / failure.

**Never** confirm a card if you did not intend that Jenkins job.

---

## VPN

| Command / phrase | Description |
|------------------|-------------|
| `/createvpn` | Start VPN creation wizard (card form) |
| `/findvpn <name>` | Find old VPN config files — e.g. `/findvpn alex` |
| `create vpn` / `make vpn for …` | Natural-language trigger for `/createvpn` |
| `find vpn config` / `找 vpn 配置` | Natural-language trigger for `/findvpn` |

### VPN create — step by step

1. `@Ops & Maintenance Bot /createvpn` (or say `create vpn`).
2. Fill **vpn_users** and **vpn_location** (or fields shown on card).
3. Submit → Jenkins flow runs in thread.
4. Session expired? Start again with `/createvpn`.

---

## Maintenance email

| Command | Description |
|---------|-------------|
| `/m <EVO batch>` | EVO SD batch → CP filter + send email |
| `/ms <email>` | Parse a single maintenance email (paste or forward content) |
| `/maintenance …` | Alias for maintenance email parsing |
| `/cashout` | Cashout reminder template |

---

## Ops & admin *(restricted)*

| Command | Description |
|---------|-------------|
| `/restartA` | Pi restart one-liner |
| `/restart` | Restart bot process |
| `/deploy` or `/gitpullrestart` | Pull code and restart bot |
| `/restartservices` or `/restservices` | Restart related services |
| `/secret1 @user` | Look up tagged user's open_id |
| `/secret2` | Admin utility |

These may be limited to specific chats or senders on production.

---

## Monitoring (Grafana)

The ops stack includes Grafana dashboards for core metrics (used by on-call / SRE). Browser reachability is verified via `checkaccess.py` on the server.

If your team shares dashboard links in this group, pin the canonical Grafana URL for your environment. There is no dedicated `/grafana` slash command in the bot today — use Jenkins or browser bookmarks for dashboard access.

---

## Natural language examples

| You say | Routed to |
|---------|-----------|
| `update np` | `/update np` |
| `create vpn for new user` | `/createvpn` |
| `find old vpn config for alex` | `/findvpn` |
| `parse this maintenance email` | `/ms` flow |

---

## FAQ

**Confirm before build:** Jenkins `/update` always shows a confirmation card — do not dismiss until you verify the job name.

**VPN wizard expired?** Send `/createvpn` again if the session times out.

**Maintenance on machines** (set/unset maintenance, stress-test scheduling, `/sm`) → **Group 4**.

**Daily duty** (`/fpms`, leave) → **Group 1**.

**More help:** [User guide §6](USER_GUIDE.md#6-group-3--ops-walkthroughs)
