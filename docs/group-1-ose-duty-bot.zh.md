# 群组 1 — OSE Duty Bot（值班机器人）

**Lark 群组：** OSE Duty Bot（主群）  
**用途：** 最高频的值班查询 — 各部门 roster、请假/WFH、假期、调休/请假流程，以及事件分级辅助。

[← 返回索引](../README.zh.md) · [English](group-1-ose-duty-bot.md) · **[完整流程 →](USER_GUIDE.zh.md#4-群组-1--值班逐步操作)**

---

## 使用方法

1. 打开 **OSE Duty Bot** Lark 群（或与机器人私聊）。
2. **群聊**中先输入 **`@OSE Duty Bot`**，再发命令或问题。
3. 发送 **`/help`** 可查看完整命令卡片。

### 首次使用检查

- [ ] 你在 **OSE Duty Bot** 群（不是日志/运维/机台群）。
- [ ] 群聊里 **@** 了机器人再发命令。
- [ ] 请假/调休用 `leave` 或 `offset` 等关键词。

---

## 通用命令

| 命令 | 说明 |
|------|------|
| `/help` | 命令列表 |
| `/s <姓名>` | 按姓名搜索值班表 |
| `/date` | 今天日期 |
| `/holiday` | 即将到来的公共假期 |
| `/holidaythismonth` | 本月假期 |
| `/leave [部门]` | 本月请假 — 如 `/leave fpms`、`/leave ote` |
| `/wfh [部门]` | 本月 WFH — 如 `/wfh fpms`、`/wfh sre` |
| `/leavewfh [部门]` | 请假 + WFH（别名 `/wfhleave`） |
| `/wholeave` | 今日谁请假（OSE 请假多维表） |

---

## 部门值班

| 命令 | 说明 |
|------|------|
| `/fpms` | FPMS 今日值班 |
| `/fpmscheck [MM/YYYY]` | FPMS 缺勤检查 |
| `/pms` | PMS 值班 |
| `/pmscheck [MM/YYYY]` | PMS 缺勤检查 |
| `/bi` | BI 今日值班 |
| `/bicheck [MM/YYYY]` | BI 缺勤检查 |
| `/fe` | FE 近三天值班 |
| `/fecheck [MM/YYYY]` | FE 缺勤检查 |
| `/cpms` | CPMS 三天值班 |
| `/cpmscheck [MM/YYYY]` | CPMS 缺勤检查 |
| `/sre` | SRE 本周与下周 |
| `/srecheck [MM/YYYY]` | SRE 缺勤检查 |
| `/db` 或 `/dba` | DB 三周值班 |
| `/dbcheck [MM/YYYY]` | DB 缺勤检查 |
| `/liveslot` | Liveslot 三周值班表 |
| `/liveslotcheck [MM/YYYY]` | Liveslot 缺勤检查 |
| `/ote` | OTE 三周值班 |
| `/otecheck [MM/YYYY]` | OTE 缺勤检查 |
| `/ft` | FT 三天值班与联系提示 |
| `/ftcheck [MM/YYYY]` | FT 缺勤检查 |
| `/ose` | OSE 当前值班 |
| `/osedate DD/MM/YYYY` | 指定日期 OSE 值班 |
| `/dutycheckall [MM/YYYY]` | 全部部门缺勤检查 |
| `/ecsre [<游戏>]` | EC SRE 游戏负责人 |
| `/ec [<游戏>]` | 紧急联系人 |

多数部门命令会在回复底部附带该部门**今日请假/WFH**。

> **机台查询**（`/nwr`、`/cctv` 等）→ 请用 **群组 4**。  
> **日志/额度** → 请用 **群组 2**。

---

## OSE 调休与请假

**@机器人 + 自然语言**（表单无需斜杠）：

| 说法 / 关键词 | 说明 |
|--------------|------|
| `showoffset [月份]` | 月度调休 — 如「五月有谁offset」 |
| `@bot offset` | 调休申请表 |
| `@bot leave` | 请假申请表 |
| `editoffset` | 编辑待审调休 |
| `pendingoffset` | 审批人待审列表 |

### 调休 / 请假表单 — 逐步操作

1. 发送 `@OSE Duty Bot offset`（调休）或 `@OSE Duty Bot leave`（请假）。
2. 出现**表单卡片** — 群聊里通常**仅你可见**。
3. 填写日期、班次、调休交换对象、原因等。
4. 点 **Submit**。
5. **待审中：** 用 `editoffset` 修改（调休记录不再删除）。
6. **审批人：** 用 `pendingoffset` 看待审并在卡片上审批。

也支持斜杠：`/editoffset`、`/pendingoffset`、`/showoffset`。

---

## 事件辅助（AI）

| 命令 | 说明 |
|------|------|
| `/checkperson <问题>` | 根据历史工单建议跟进信息 |

---

## 提醒

| 命令 | 说明 |
|------|------|
| `/reminder <时间> <内容>` | 一次性提醒 |
| `/addreminder …` | 表格提醒/表单 |
| `/deletereminder [id]` | 删除/列出提醒 |

自然语言：`@bot add timer 5mins 午饭`

---

## 自然语言示例

| 你说 | 结果 |
|------|------|
| `今天谁值班 fpms` | FPMS 今日值班 |
| `本月假期` | 本月公共假期 |
| `@bot leave` | 请假申请表 |
| `@bot offset` | 调休申请表 |
| `pendingoffset` | 待审列表 |

---

## 常见问题

**本群适合：** 日常「谁值班」、请假/WFH、假期、调休 — 不要在此查日志或操作机台维护。

**日期：** `/osedate DD/MM/YYYY`；缺勤检查 `MM/YYYY` 或 `YYYY-MM`。

**`/leave fpms` 部门不认识？** 可用：`fpms`, `ote`, `bi`, `fe`, `sre`, `db`, `dba`, `cpms`, `pms`, `ft`。

**更多：** [指南 §4](USER_GUIDE.zh.md#4-群组-1--值班逐步操作) · [故障排除](USER_GUIDE.zh.md#9-故障排除)
