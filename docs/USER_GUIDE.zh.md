# Duty Bot — 完整使用指南

OSE Lark 机器人逐步操作说明。分群命令列表见 [各群文档](README.md)。

[English guide](USER_GUIDE.md) · [← 返回索引](../README.zh.md)

---

## 1. 开始之前

### 加入正确的 Lark 群

| 需求 | 群组 | 文档 |
|------|------|------|
| 谁值班、请假、假期、调休 | **OSE Duty Bot** | [群组 1](group-1-ose-duty-bot.zh.md) |
| 额度、日志、卡机、SMS OTP | **Log & Credit Bot** | [群组 2](group-2-log-credit-bot.zh.md) |
| Jenkins、VPN、维护邮件 | **Ops & Maintenance Bot** | [群组 3](group-3-ops-maintenance-bot.zh.md) |
| 机台信息、CCTV、现场维护 | **Liveslots & Game Bot** | [群组 4](group-4-liveslots-game-bot.zh.md) |

### @ 机器人规则（最重要）

```
┌─────────────────────────────────────────────────────────┐
│  群聊                 │  私聊（DM）                      │
├──────────────────────┼───────────────────────────────────┤
│  必须先 @ 机器人      │  无需 @                           │
│  @Bot /fpms          │  /fpms                            │
│  @Bot 今天谁值班      │  今天谁值班                       │
└──────────────────────┴───────────────────────────────────┘
```

群聊里**没 @ 机器人**是最常见的「机器人不理我」原因。

### 斜杠命令 vs 自然语言

| 方式 | 何时使用 | 示例 |
|------|----------|------|
| **斜杠** `/命令` | 准确、快速、一定可用 | `@Bot /fpms` |
| **自然语言** | 口语提问（需服务器开 AI） | `@Bot 今天谁值班 fpms` |

不确定时用 **`/help`** 或斜杠命令。

---

## 2. 回复形式说明

### 纯文字 vs 交互卡片

| 类型 | 样子 | 你要做什么 |
|------|------|-----------|
| **纯文字** | 普通消息 | 直接阅读 |
| **交互卡片** | 按钮、表单、下拉框 | 填字段 → 点 **提交** / **确认** |
| **话题回复** | 在你消息下方的话题里 | 额度、Jenkins 等长任务常在**话题**里更新 — 请点开话题看结果 |

### 「请稍候」消息

很多命令会启动**后台任务**（浏览器、Jenkins、抓日志）：

```
⏳ Running checkcredit, browser may take a while — please wait...
```

- **不要**马上重复发同一命令。
- 到消息下方的**话题**里看最终结果。
- 服务器空闲后首次请求可能要 **30 秒～2 分钟**。

### 卡片上的按钮

| 按钮 | 含义 |
|------|------|
| **Confirm** / **Submit** | 确认执行 |
| **Cancel** | 取消；通常需重新发命令 |
| **I have set maintenance** | 压测提醒 — 现场做完维护后点 |

---

## 3. 日期与时间格式速查

| 场景 | 格式 | 示例 |
|------|------|------|
| OSE 指定日期值班 | `DD/MM/YYYY` | `/osedate 30/06/2026` |
| 缺勤检查（按月） | `MM/YYYY` 或 `YYYY-MM` | `/fpmscheck 06/2026` |
| 额度 / 机台日志 | `YYYY-MM-DD` | `/checkcredit OSMCP181 2026-06-28` |
| Amount Loss | `DD/MM` | `/al 28/06` |
| 提醒（时长） | `1h30m`、`5mins` | `/reminder 1h30m 开会` |
| 提醒（钟点） | `8:39PM`、`at 2039` | `/reminder 8:39PM standup` |

---

## 4. 群组 1 — 值班（逐步操作）

### 4.1 查今天谁值班

**斜杠（最快）：**

```
@OSE Duty Bot /fpms
@OSE Duty Bot /bi
@OSE Duty Bot /ose
```

**自然语言：**

```
@OSE Duty Bot 今天谁值班 fpms？
@OSE Duty Bot who is on bi duty?
```

**本月缺勤检查：**

```
@OSE Duty Bot /fpmscheck
@OSE Duty Bot /fpmscheck 06/2026
@OSE Duty Bot /dutycheckall 06/2026
```

回复底部常会附带该部门**今日请假/WFH**。

### 4.2 按姓名搜值班表

```
@OSE Duty Bot /s john
@OSE Duty Bot /s 王明
```

### 4.3 本月请假与 WFH

发送后会打开**本月交互卡片**（可选部门）：

```
@OSE Duty Bot /leave
@OSE Duty Bot /leave fpms
@OSE Duty Bot /wfh sre
@OSE Duty Bot /leavewfh cpms
```

可用部门：`fpms`, `ote`, `bi`, `fe`, `sre`, `db`, `dba`, `cpms`, `pms`, `ft`。

**今天谁请假（OSE 表）：**

```
@OSE Duty Bot /wholeave
```

### 4.4 申请请假 / 调休（表单）

1. 在**群组 1** 发送：`@OSE Duty Bot leave`（请假）或 `@OSE Duty Bot offset`（调休）。
2. 机器人发**仅你可见**的表单卡片（群内 ephemeral）。
3. 填写：日期、类型、原因、调休交换对象等。
4. 点 **Submit**。
5. 审批人会收到卡片；通过/拒绝后你会收到通知。

**其他调休关键词：**

| 操作 | 说法 |
|------|------|
| 看月度调休 | `showoffset` 或 `五月有谁offset` |
| 改待审申请 | `editoffset` |
| 审批人看待审 | `pendingoffset` |

### 4.5 假期

```
@OSE Duty Bot /holiday
@OSE Duty Bot /holidaythismonth
@OSE Duty Bot /date
```

### 4.6 事件辅助

在命令后粘贴玩家问题描述：

```
@OSE Duty Bot /checkperson 充值后卡在 loading
```

### 4.7 提醒

```
@OSE Duty Bot /reminder 30m 检查工单
@OSE Duty Bot /reminder 8:39PM 站会
@OSE Duty Bot add timer 5mins 咖啡
```

---

## 5. 群组 2 — 日志与额度（逐步操作）

### 5.1 完整额度检查（新手推荐）

**第 1 步** — 打开卡片向导：

```
@Log & Credit Bot /checkcreditdate
```

**第 2 步** — 在卡片填写：
- 机台名（如 `OSMCP181`、`Dragons-0181`、`DHS3077`）
- 玩家 ID（若已知）
- 日期（默认今天）

**第 3 步** — 提交，在**话题**里等日志截图 / Third Http 详情。

### 5.2 快速查额度（已知机台）

```
@Log & Credit Bot /checkcredit OSMCP181
@Log & Credit Bot /checkcredit 1171 2026-06-28
```

不写日期 = **今天**。

### 5.3 现场卡机

```
@Log & Credit Bot /stuckcredit NWR2938
@Log & Credit Bot /stuckcredit DHS3077 2026-06-26
```

### 5.4 机台日志

```
@Log & Credit Bot /checkmachinelog DHS3077
@Log & Credit Bot /checkmachinelog NWR2938 2026-06-26
```

### 5.5 仅看有错误的玩家

```
@Log & Credit Bot /machineerror OSMCP181
```

### 5.6 SMS OTP

```
@Log & Credit Bot /smsfail
@Log & Credit Bot /smscheckplayer 127317237
@Log & Credit Bot /smscheckplayer 7052472, 1069954565
```

多个玩家 ID：逗号、空格或换行均可。

### 5.7 Amount Loss

```
@Log & Credit Bot /al
@Log & Credit Bot /al 28/06
```

---

## 6. 群组 3 — 运维（逐步操作）

### 6.1 Jenkins 构建

**第 1 步** — 查关键字：

```
@Ops & Maintenance Bot /help jenkins
```

**第 2 步** — 发起构建：

```
@Ops & Maintenance Bot /update np
```

**第 3 步** — 看**确认卡片**，核对任务名与参数。

**第 4 步** — 点 **Confirm**，在话题里跟结果。

**批量：**

```
@Ops & Maintenance Bot /updatemore
UPDATE np
UPDATE fpms
```

### 6.2 创建 VPN

**第 1 步：** `@Ops & Maintenance Bot /createvpn`

**第 2 步：** 填卡片 → 提交。

**第 3 步：** 在话题里等 Jenkins 流程。

会话过期就重新发 `/createvpn`。

### 6.3 找旧 VPN 配置

```
@Ops & Maintenance Bot /findvpn alex
```

### 6.4 维护邮件

```
@Ops & Maintenance Bot /ms
<粘贴邮件正文>
```

**出款模板：**

```
@Ops & Maintenance Bot /cashout
```

---

## 7. 群组 4 — 机台（逐步操作）

### 7.1 查单机信息

```
@Liveslots & Game Bot /nwr 2133
@Liveslots & Game Bot /nwr nwr2005 nwr2006
@Liveslots & Game Bot /nch 1900
@Liveslots & Game Bot /wf 8092
```

编号可写 `2133`、`nwr2133`，多个用逗号或空格。

### 7.2 先展开编号区间

```
@Liveslots & Game Bot /list 8900-8911
@Liveslots & Game Bot /list NWR2133-NWR2142
```

### 7.3 Provider ID · IP / ISP 查询

```
@Liveslots & Game Bot /pid 30
@Liveslots & Game Bot /pid 30 31 32
```

```
@Liveslots & Game Bot /isp 112.198.1.1
@Liveslots & Game Bot /isp 112.198.1.1 203.177.42.1 180.190.1.1
@Liveslots & Game Bot /isp 8.8.8.8,1.1.1.1
```

`/isp` 回复 **IP Details** 卡片：ISP / 组织、国家、ASN，来自 6 个公开来源。多个地址按
**网络分组**，一眼看出哪些地址属于同一运营商或同一网段。支持 IPv6。内网与保留地址
直接说明，不发起查询。

从日志粘贴时，地址旁的**玩家 ID** 会自动配对，显示为 `138.84.76.76 👤1081561491`。
每组一行或用空行分隔，顺序不限：

```
@Liveslots & Game Bot /isp
138.84.76.76
1081561491

1075487320
103.40.2.142
```

### 7.4 CCTV（不查额度）

```
@Liveslots & Game Bot /cctv OSMCP181
@Liveslots & Game Bot /cctv Dragons-0181
```

机台名与额度命令**同一套写法**。

### 7.5 开关维护 — 向导 `/sm`

1. `@Liveslots & Game Bot /sm`
2. 选环境（NWR、NCH、WF…）
3. 选操作：开/关 maintenance 和/或 test
4. 在卡片确认机台列表
5. 确认执行；话题里可能有截图

### 7.6 直接命令

```
@Liveslots & Game Bot /nwrsetmaintenance
NWR2113
NWR2114
```

或自然语言：`nwr set maintenance NWR2113 NWR2114`

### 7.7 压测排程提醒

粘贴带**日期时间**和机台列表的公告：

```
@Liveslots & Game Bot Please set maintenance and test ALL WF MACHINES Good Fortune
later JUNE 09, 2026 09:45 pm, due to Stress Test.

5 Dragons-WF8145
...
```

到点前 10 分钟会提醒；到点后执行 `/sm`，再在卡片点 **I have set maintenance**。

也可用显式命令 `/stresstest` + 下一行粘贴公告（必定回复：解析到的机台与时间，或缺什么）：

```
/stresstest
We have 4 DFDC machines subject for Stress Test July 15, 2026 at 11:00 AM.
Please set to maintain status and test mode July 14, 2026 at 2145H
- WF8109 ( 5 Treasures )
- WF8112 ( 5 Treasures )
```

### 7.8 Machine Encoder（浏览器）

打开：`http://<服务器>:<PORT>/machine-encoders`（问管理员要地址）

---

## 8. 该用哪个群？

```
查谁值班？              → 群组 1   /fpms、/ose
查玩家额度/日志？        → 群组 2   /checkcredit、/stuckcredit
Jenkins / VPN？         → 群组 3   /update、/createvpn
机台资产 / CCTV？       → 群组 4   /nwr、/cctv、/pid
查 IP 的 ISP/国家/ASN？ → 群组 4   /isp
现场开关维护？          → 群组 4   /sm、/nwrsetmaintenance
请假 / 调休？           → 群组 1   @bot leave、@bot offset
```

---

## 9. 故障排除

| 现象 | 处理 |
|------|------|
| 群聊没反应 | **@ 机器人** |
| `❌ Usage:` | 按提示改格式；对照上文日期表 |
| 一直 please wait | 开**话题**等 1～2 分钟；勿刷屏 |
| 卡片按钮无效 | 会话过期 — 重新发命令 |
| 自然语言无效 | 改用斜杠；或问管理员 AI 是否开启 |
| 数据空/错 | 表格或配置问题 — 找管理员 |
| 用错群 | 命令可能仍能用，但请按上表换群 |

---

## 10. 速查（可 pin 到群公告）

```
@Bot /help              完整命令
@Bot /help jenkins      Jenkins 关键字（群 3）

群 1: /fpms /leave /wholeave /osedate /offset
群 2: /checkcreditdate /stuckcredit /smsfail
群 3: /update /createvpn /findvpn /ms
群 4: /nwr /cctv /pid /isp /sm
```

详细：[群 1](group-1-ose-duty-bot.zh.md) · [群 2](group-2-log-credit-bot.zh.md) · [群 3](group-3-ops-maintenance-bot.zh.md) · [群 4](group-4-liveslots-game-bot.zh.md)
