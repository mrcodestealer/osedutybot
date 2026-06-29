# OSE Lark 机器人 — 文档索引

OSE 团队在 Lark（飞书）上的工作机器人。功能按 **四个群组** 拆分，避免日常值班查询与日志、运维、机台操作混在一起。

**English index:** [README.md](README.md)

### 第一次使用？

请读 **[完整使用指南](docs/USER_GUIDE.zh.md)** — 含 @ 规则、分步流程、日期格式、卡片操作与故障排除。  
English: **[Complete User Guide](docs/USER_GUIDE.md)**

---

## 我应该用哪个群？

| 群组 | 机器人名称 | 用途 |
|------|-----------|------|
| **1** | [OSE Duty Bot](docs/group-1-ose-duty-bot.zh.md) | 部门值班、请假/WFH、假期、调休/请假、P0/P1 辅助 |
| **2** | [Log & Credit Bot](docs/group-2-log-credit-bot.zh.md) | 额度检查、机台日志、卡机、Amount Loss、SMS OTP |
| **3** | [Ops & Maintenance Bot](docs/group-3-ops-maintenance-bot.zh.md) | Jenkins 构建、VPN、维护邮件、部署/重启、运维模板 |
| **4** | [Liveslots & Game Bot](docs/group-4-liveslots-game-bot.zh.md) | 机台查询、Provider ID、CCTV、Encoder、维护开关、压测 |

> **所有群组通用规则：** **群聊**必须先 **@ 机器人**再输入命令；**私聊**无需 @。

---

## 快速示例

```
# 群组 1 — 值班
@OSE Duty Bot /fpms
@OSE Duty Bot 今天谁值班 bi？

# 群组 2 — 日志
@Log & Credit Bot /checkcredit OSMCP181
@Log & Credit Bot /checkcreditdate

# 群组 3 — 运维
@Ops & Maintenance Bot /update np
@Ops & Maintenance Bot /createvpn

# 群组 4 — 机台
@Liveslots & Game Bot /nwr 2133
@Liveslots & Game Bot /cctv Dragons-0181
```

任意群组发送 **`/help`** 可查看命令卡片；**`/help jenkins`** 查看 Jenkins 关键字（群组 3）。

---

## 分群文档

| 中文 | English |
|------|---------|
| **[完整使用指南](docs/USER_GUIDE.zh.md)** | **[Complete user guide](docs/USER_GUIDE.md)** |
| [群组 1 — 值班机器人](docs/group-1-ose-duty-bot.zh.md) | [Group 1 — OSE Duty Bot](docs/group-1-ose-duty-bot.md) |
| [群组 2 — 日志与额度](docs/group-2-log-credit-bot.zh.md) | [Group 2 — Log & Credit Bot](docs/group-2-log-credit-bot.md) |
| [群组 3 — 运维与维护](docs/group-3-ops-maintenance-bot.zh.md) | [Group 3 — Ops & Maintenance Bot](docs/group-3-ops-maintenance-bot.md) |
| [群组 4 — 机台与游戏](docs/group-4-liveslots-game-bot.zh.md) | [Group 4 — Liveslots & Game Bot](docs/group-4-liveslots-game-bot.md) |

---

## 自然语言（AI）

服务器开启 AI 后，可在各群用中英文提问，机器人会识别意图。示例：

- 群组 1：`今天谁值班 fpms`、`@bot leave`、`pendingoffset`
- 群组 2：`check credit OSMCP181`、`今天 sms otp 失败`
- 群组 3：`create vpn`、`update np`
- 群组 4：`nwr set maintenance NWR2113`

AI 未开启时请使用各群文档中的斜杠命令。

---

## 管理员

后端代码在本仓库（`main.py`）。配置：复制 [`.env.example`](.env.example) 为 `.env`。

| 操作 | 命令 |
|------|------|
| 本地运行（Windows） | `.\run_local.ps1` |
| 服务器运行 | `python main.py` |
| 生产 systemd | `deploy/larkbot-longconn.service.example` |

`/help` 卡片内容在 `bot_help.py`；用户文档在 `docs/` 目录。

内部 OSE 工具 — Lark 权限与 `.env` 请联系机器人管理员。
