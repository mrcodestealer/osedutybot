# 群组 2 — Log & Credit Bot（日志与额度）

**Lark 群组：** Log & Credit Bot  
**用途：** 集中所有日志与额度相关查询 — 玩家额度、机台日志、卡机、Amount Loss、SMS OTP。

[← 返回索引](../README.zh.md) · [English](group-2-log-credit-bot.md) · **[完整流程 →](USER_GUIDE.zh.md#5-群组-2--日志与额度逐步操作)**

---

## 使用方法

1. 打开 **Log & Credit Bot** Lark 群。
2. **@ 机器人**后发送命令。
3. 需要选机台/玩家/日期时，优先用 **`/checkcreditdate`** 交互卡片。

> 值班查询 → **群组 1**。机台资产 / CCTV → **群组 4**。

---

## 命令一览

| 命令 | 说明 |
|------|------|
| `/checkcreditdate` | 交互卡片：机台 + 玩家 + 日期 |
| `/checkcredit <机台> [日期]` | 额度/日志检查 |
| `/checkmachinelog <机台> [日期]` | 末位玩家 + 错误/成功摘要 |
| `/stuckcredit <机台> [日期]` | 卡机额度：日志 + Third Http 转出 |
| `/machineerror <机台> [日期]` | 仅有错误的玩家列表 |
| `/npthirdhttp …` | NP Third HTTP（异步） |
| `/al [DD/MM]` | Amount Loss CHECKLOG |
| `/smsfail` | 今日 SMS OTP 失败检查 |
| `/smscheckplayer <id>` | 指定玩家今日 SMS OTP 日志 |

---

## 日期格式

| 命令 | 日期格式 |
|------|----------|
| `/checkcredit`、`/checkmachinelog`、`/stuckcredit`、`/machineerror` | `YYYY-MM-DD`（可选，默认今天） |
| `/al` | `DD/MM`（可选，默认今天） |

**机台名称：** 与其他流程一致 — 如 `OSMCP181`、`Dragons-0181`、`DHS3050`、`NWR2938`，或数字 `1171`。

### 机台名写法提示

| 类型 | 示例 | 常用于 |
|------|------|--------|
| EGM 显示名 | `Dragons-0181`、`OSMCP181` | `/checkcredit`、`/cctv`、`/stuckcredit` |
| 站点+编号 | `DHS3077`、`NWR2938` | `/checkmachinelog` |
| 短数字 | `1171`、`2074` | `/checkcredit` |

出现 `❌ Usage:` 时，按提示里的示例格式重发。

### 话题回复

额度/日志类命令多在消息下方的**话题**里返回结果：

1. 你发 `@Log & Credit Bot /checkcredit OSMCP181`
2. 话题里先出现 `⏳ please wait...`
3. 截图和日志在**同一话题** — 点开消息查看

---

## 常见流程

### 现场卡机

```
@Log & Credit Bot /stuckcredit OSMCP181
@Log & Credit Bot /stuckcredit Dragons-0181 2026-06-28
```

### 不知道玩家 ID 时查额度

```
@Log & Credit Bot /checkcreditdate
```
在卡片里填写 → 机器人跑完整额度流程。

### SMS OTP 问题

```
@Log & Credit Bot /smsfail
@Log & Credit Bot /smscheckplayer 12345678
```

### Amount Loss

```
@Log & Credit Bot /al
@Log & Credit Bot /al 28/06
```

---

## 自然语言示例

| 你说 | 路由到 |
|------|--------|
| `check credit OSMCP181` | `/checkcredit` |
| `机台日志 Dragons-0181` | `/checkmachinelog` |
| `NCH1900 卡机` | `/stuckcredit` |
| `今天 sms otp 失败` | `/smsfail` |

---

## 常见问题

**为何单独成群？** 日志/额度常带截图和长回复，与主值班群分开更清晰。

**响应慢？** 服务器可能需启动浏览器；空闲后首次请求会更久。

**别用错群：** 机台**资产**（`/nwr`、`/nch`）在 **群组 4**。

**更多：** [指南 §5](USER_GUIDE.zh.md#5-群组-2--日志与额度逐步操作) · [`/checkcreditdate` 向导](USER_GUIDE.zh.md#51-完整额度检查新手推荐)
