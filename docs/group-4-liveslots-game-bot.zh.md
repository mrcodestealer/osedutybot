# 群组 4 — Liveslots & Game Bot（机台与游戏）

**Lark 群组：** Liveslots & Game Bot  
**用途：** 值班时查机台 — 资产信息、Provider ID、CCTV、Encoder、开关维护、压测流程、prod-batch 截图。

[← 返回索引](../README.zh.md) · [English](group-4-liveslots-game-bot.md) · **[完整流程 →](USER_GUIDE.zh.md#7-群组-4--机台逐步操作)**

---

## 使用方法

1. 打开 **Liveslots & Game Bot** Lark 群。
2. **@ 机器人**后发送命令。
3. 批量编号先用 **`/list`** 展开，再用各站点命令查询。

> 额度/玩家日志 → **群组 2**。  
> Jenkins / VPN / 维护**邮件** → **群组 3**。  
> **值班表** `/liveslot`（谁值班）→ **群组 1**。

---

## 机台查询

| 命令 | 说明 |
|------|------|
| `/list <范围>` | 展开编号 — 如 `8900-8911`、`NWR2133-NWR2142` |
| `/nch <id>` | NCH — 如 `/nch 1900`、`/nch nch2839` |
| `/nwr <id>` | NWR — 如 `/nwr 2005`、`/nwr nwr2005` |
| `/wf <id>` | Winford 资产 |
| `/tbp <id>` | TBP 机台 |
| `/cp <id>` | CP 资产（不是 `/cpms`） |
| `/dhs <id>` | DHS 资产 |
| `/mdr <id>` | MDR 资产 |

`/wf` 回复含表格中的 **Top Encoder**、**Main Encoder** 字段。

### 多台机台写法

```
/nwr 2005,2006
/nwr nwr2005 nwr2006
/nch nch2839,nch2378
```

有**区间**时先用 `/list NWR2133-NWR2142` 展开，再用于维护命令。

---

## Provider、CCTV

| 命令 | 说明 |
|------|------|
| `/pid <id>` | Provider ID 查询 |
| `/cctv <机台>` | 仅 EGM 监控截图（不查额度） |

**示例：**

```
@Liveslots & Game Bot /pid 12345
@Liveslots & Game Bot /cctv OSMCP181
@Liveslots & Game Bot /cctv Dragons-0181
```

---

## Machine Encoder（网页）

Encoder IP、推流地址在机器人 **Web 面板**上查看（`webapp.py`）：

- 路径：**`/machine-encoders`**
- 按站点筛选（NWR、WF、NCH…），可按机台名或 Encoder IP 搜索

基址请咨询管理员（一般为 `http://<服务器>:<PORT>/machine-encoders`）。数据来自 Google Sheets / `machineencoder.json`。

---

## 开关维护

### 向导：`/sm`

```
@Liveslots & Game Bot /sm
```

选择环境 → 操作（开/关 maintenance 和/或 test）→ 确认卡片 → 执行 prod-batch，可选 **截图**。

**注意：** `/sm` 是多步卡片向导。中途不要关掉卡片；超时就重新发 `/sm`。

### Prod-batch 命令示例

| 操作 | NWR 示例 |
|------|----------|
| 开 maintenance | `/nwrsetmaintenance` + 每行一台 |
| 关 maintenance | `/nwrunsetmaintenance` |
| 开 maint + test | `/nwrsetmaintenancetest` |
| 关两者 | `/nwrunsetmaintenancetest` |

将 `nwr` 换成 `nch`、`wf`、`cp` 等。

### 斜杠命令（prod-batch 系列）

自然语言或显式斜杠 — 站点前缀 + 动作：

| 模式 | 示例 |
|------|------|
| `/<站点>setmaintenance` | `/nwrsetmaintenance` + 机台列表 |
| `/<站点>unsetmaintenance` | `/nwrunsetmaintenance` |
| `/<站点>setmaintenancetest` | 同时 set maintenance 与 test |
| `/<站点>unsetmaintenancetest` | 同时 unset |

站点：`nwr`、`nch`、`wf`、`cp`、`tbp`、`dhs` 等。

**自然语言示例：**

```
@Liveslots & Game Bot nwr set maintenance NWR2113 NWR2114
@Liveslots & Game Bot unset maintenance nch NCH1422
```

### 机台状态（只读）

不修改状态，从 `webmachine_data.json` 读取：

```
@Liveslots & Game Bot check status NWR2113
@Liveslots & Game Bot WF8145 是否在 maintenance？
```

---

## 压测（Stress Test）

### 排程提醒（解析公告）

@机器人 发送**带时间**的压测/维护公告。机器人解析动作时间与机台列表，在动作时间 **前 10 分钟** 发提醒：

```
@Liveslots & Game Bot Please set maintenance and test ALL WF MACHINES Good Fortune
later JUNE 09, 2026 09:45 pm, due to Stress Test.

5 Dragons-WF8145
Dragon of the Eastern Ocean-WF8146
...
```

到点后人工执行实际维护（`/sm` 或 prod-batch），再在提醒卡片上确认。

### `/stresstest`（显式命令）

同样的解析，但显式触发（不依赖 @mention 措辞判断）。输入 `/stresstest`，下一行粘贴公告。机器人读取**设维护的日期时间**（不是压测本身的时间）和**机台列表**，在其**前 10 分钟**排提醒，并必定回复（解析结果或缺失项）：

```
/stresstest
We have 4 DFDC machines subject for Stress Test July 15, 2026 at 11:00 AM.
Please set to maintain status and test mode July 14, 2026 at 2145H
- WF8109 ( 5 Treasures )
- WF8112 ( 5 Treasures )
```

### 压测截图

Prod-batch 可在开关维护后附带**机台截图**（由服务器环境变量控制）。使用 `/sm` 或 prod-batch 命令，截图会出现在话题回复中。

---

## 自然语言示例

| 你说 | 路由到 |
|------|--------|
| `nwr 2133` | `/nwr 2133` |
| `provider id …` | `/pid` |
| `cctv Dragons-0181` | `/cctv` |
| `nwr set maintenance NWR2113` | prod-batch |
| `good fortune 全部 wf 开维护` | `/sm` 或公告解析 |

---

## 常见问题

**`/cp` 与 `/cpms`：** `/cp` 是 CP **资产**（本群）；`/cpms` 是 **部门值班**（群组 1）。

**CCTV 与额度：** `/cctv` 只截图。查额度/日志请用 **群组 2**。

**Liveslot 值班表：** `/liveslot` 在 **群组 1** — 本群是**机台操作**。

**更多：** [指南 §7](USER_GUIDE.zh.md#7-群组-4--机台逐步操作) · [Encoder 网页](USER_GUIDE.zh.md#78-machine-encoder浏览器)
