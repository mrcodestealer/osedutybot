# 群组 3 — Ops & Maintenance Bot（运维与维护）

**Lark 群组：** Ops & Maintenance Bot  
**用途：** Jenkins 构建、VPN、维护邮件解析、部署/重启、出款模板等运维操作 — 与日常值班分开。

[← 返回索引](../README.zh.md) · [English](group-3-ops-maintenance-bot.md) · **[完整流程 →](USER_GUIDE.zh.md#6-群组-3--运维逐步操作)**

---

## 使用方法

1. 打开 **Ops & Maintenance Bot** Lark 群。
2. **@ 机器人**后发送命令。
3. Jenkins：**`/help jenkins`** 列出 `/update` 可用关键字。

> 机台维护开关、CCTV → **群组 4**。  
> 日志/额度 → **群组 2**。

---

## Jenkins

| 命令 | 说明 |
|------|------|
| `/update <关键字>` | 匹配任务 → 卡片确认 → 构建 |
| `/updatemore …` | 批量更新（每段 `UPDATE …` 一行） |
| `/jenkinsupdate …` | 同上（别名） |
| `/updatejenkins …` | 同上（别名） |
| `/help jenkins` | 所有已注册关键字 |

**示例：**

```
@Ops & Maintenance Bot /update np
@Ops & Maintenance Bot /help jenkins
```

自然语言：`update np backend`、`jenkins build fpms`

### Jenkins — 逐步操作

1. `@Ops & Maintenance Bot /help jenkins` — 查关键字。
2. `@Ops & Maintenance Bot /update <关键字>` — 如 `/update np`。
3. 阅读**确认卡片**（任务名、分支、参数）。
4. 核对无误再点 **Confirm**。
5. 在**话题**里看构建结果。

非本人发起的构建，**不要**点确认。

---

## VPN

| 命令 / 说法 | 说明 |
|------------|------|
| `/createvpn` | 创建 VPN 向导（卡片表单） |
| `/findvpn <名字>` | 查找旧 VPN 配置 — 如 `/findvpn alex` |
| `create vpn` / `新建 vpn` | 自然语言触发 `/createvpn` |
| `find vpn config` / `找 vpn 配置` | 自然语言触发 `/findvpn` |

---

## 维护邮件

| 命令 | 说明 |
|------|------|
| `/m <EVO batch>` | EVO 批量维护 + CP 筛选 + 发信 |
| `/ms <邮件>` | 解析单封维护邮件 |
| `/maintenance …` | 维护邮件解析（别名） |
| `/cashout` | 出款提醒模板 |

---

## 运维与管理员 *(可能受限)*

| 命令 | 说明 |
|------|------|
| `/restartA` | Pi 重启命令 |
| `/restart` | 重启机器人进程 |
| `/deploy` 或 `/gitpullrestart` | 拉代码并重启 |
| `/restartservices` 或 `/restservices` | 重启相关服务 |
| `/secret1 @用户` | 查询被 @ 用户的 open_id |
| `/secret2` | 管理员工具 |

生产环境可能仅限特定群或发送者。

---

## 监控（Grafana）

运维侧使用 Grafana 查看核心指标。服务器上 `checkaccess.py` 会校验浏览器能否访问 Grafana 等 URL。

团队可在本群固定分享环境对应的 Grafana 链接。目前机器人**没有** `/grafana` 斜杠命令 — 请用书签或 Jenkins 相关流程配合使用。

---

## 自然语言示例

| 你说 | 路由到 |
|------|--------|
| `update np` | `/update np` |
| `create vpn` | `/createvpn` |
| `找 alex 的 vpn 配置` | `/findvpn` |
| `解析这封维护邮件` | `/ms` 流程 |

---

## 常见问题

**构建前确认：** `/update` 一定会弹出确认卡片，请核对任务名后再点确认。

**VPN 会话过期？** 重新发送 `/createvpn`。

**机台维护**（开关维护、`/sm`、压测排程）→ **群组 4**。

**日常值班**（`/fpms`、请假）→ **群组 1**。

**更多：** [指南 §6](USER_GUIDE.zh.md#6-群组-3--运维逐步操作)
