# OSE Lark Bots — Documentation

Workplace bots for the OSE team on Lark (Feishu). Commands are split across **four Lark groups** so daily duty lookups stay separate from logs, ops, and machine work.

**中文索引：** [README.zh.md](README.zh.md)

### New here?

Read the **[Complete User Guide](docs/USER_GUIDE.md)** — step-by-step workflows, @mention rules, date formats, card buttons, and troubleshooting.  
中文：**[完整使用指南](docs/USER_GUIDE.zh.md)**

---

## Which group should I use?

| Group | Bot name | Use for |
|-------|----------|---------|
| **1** | [OSE Duty Bot](docs/group-1-ose-duty-bot.md) | Department duty, leave/WFH, holidays, offset/leave forms, P0/P1 helpers |
| **2** | [Log & Credit Bot](docs/group-2-log-credit-bot.md) | Credit checks, machine logs, stuck credit, Amount Loss, SMS OTP |
| **3** | [Ops & Maintenance Bot](docs/group-3-ops-maintenance-bot.md) | Jenkins builds, VPN, maintenance emails, deploy/restart, ops templates |
| **4** | [Liveslots & Game Bot](docs/group-4-liveslots-game-bot.md) | Machine lookup, Provider ID, CCTV, encoder, set/unset maintenance, stress-test |

> **Rule for all groups:** In a **group chat**, **@mention the bot** first, then type your command. In **DM**, no @mention is needed.

---

## Quick examples

```
# Group 1 — duty
@OSE Duty Bot /fpms
@OSE Duty Bot 今天谁值班 bi？

# Group 2 — logs
@Log & Credit Bot /checkcredit OSMCP181
@Log & Credit Bot /checkcreditdate

# Group 3 — ops
@Ops & Maintenance Bot /update np
@Ops & Maintenance Bot /createvpn

# Group 4 — machines
@Liveslots & Game Bot /nwr 2133
@Liveslots & Game Bot /cctv Dragons-0181
```

Send **`/help`** in any group for the command card (full catalogue). Topic-specific help: **`/help jenkins`** (Group 3).

---

## Documentation

| English | 中文 |
|---------|------|
| **[Complete user guide](docs/USER_GUIDE.md)** | **[完整使用指南](docs/USER_GUIDE.zh.md)** |
| [Group 1 — OSE Duty Bot](docs/group-1-ose-duty-bot.md) | [群组 1 — 值班机器人](docs/group-1-ose-duty-bot.zh.md) |
| [Group 2 — Log & Credit Bot](docs/group-2-log-credit-bot.md) | [群组 2 — 日志与额度](docs/group-2-log-credit-bot.zh.md) |
| [Group 3 — Ops & Maintenance Bot](docs/group-3-ops-maintenance-bot.md) | [群组 3 — 运维与维护](docs/group-3-ops-maintenance-bot.zh.md) |
| [Group 4 — Liveslots & Game Bot](docs/group-4-liveslots-game-bot.md) | [群组 4 — 机台与游戏](docs/group-4-liveslots-game-bot.zh.md) |

---

## Natural language (AI)

When AI is enabled on the server, you can ask in English or Chinese in any group. The bot routes intent to the right handler. Examples:

- Group 1: `who is on fpms duty`, `@bot leave`, `pending offset approvals`
- Group 2: `check credit for machine OSMCP181`, `sms otp fail today`
- Group 3: `create vpn for alex`, `update np backend`
- Group 4: `nwr set maintenance NWR2113`, `show encoder for WF8145`

If AI is off, use slash commands from the group guide above.

---

## For administrators

This repo runs the bot backend (`main.py`). Configuration: copy [`.env.example`](.env.example) → `.env`.

| Task | Command |
|------|---------|
| Local run (Windows) | `.\run_local.ps1` |
| Server run | `python main.py` |
| Production systemd | `deploy/larkbot-longconn.service.example` |

Keep `/help` in sync: command cards are defined in `bot_help.py`; user docs live under `docs/`.

Internal OSE tool — contact your bot admin for Lark app access and `.env` secrets.
