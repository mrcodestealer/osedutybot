# Group 2 — Log & Credit Bot

**Lark group:** Log & Credit Bot  
**Purpose:** All log and credit investigation — player credit, machine logs, stuck credit, Amount Loss, and SMS OTP.

[← All groups](../README.md) · [中文](group-2-log-credit-bot.zh.md) · **[Full walkthroughs →](USER_GUIDE.md#5-group-2--log--credit-walkthroughs)**

---

## How to use

1. Open the **Log & Credit Bot** Lark group.
2. **@mention the bot**, then send your command.
3. Prefer **`/checkcreditdate`** when you need the interactive card (machine + player + date picker).

> Duty rosters → **Group 1**. Machine asset info / CCTV → **Group 4**.

---

## Commands

| Command | Description |
|---------|-------------|
| `/checkcreditdate` | Interactive card: pick machine, player, and date |
| `/checkcredit <machine> [date]` | Credit / log check for a machine |
| `/checkmachinelog <machine> [date]` | Last player on machine + error/success summary |
| `/stuckcredit <machine> [date]` | Stuck credit: log + Third Http transfer-out |
| `/machineerror <machine> [date]` | List players with errors only |
| `/npthirdhttp …` | NP Third HTTP lookup (async) |
| `/al [DD/MM]` | Amount Loss CHECKLOG |
| `/smsfail` | SMS OTP failure check (today) |
| `/smscheckplayer <id>` | SMS OTP logs for a player today |

---

## Date formats

| Command family | Date format |
|----------------|-------------|
| `/checkcredit`, `/checkmachinelog`, `/stuckcredit`, `/machineerror` | `YYYY-MM-DD` (optional; default today) |
| `/al` | `DD/MM` (optional; default today) |

**Machine label:** Same as other bot flows — e.g. `OSMCP181`, `Dragons-0181`, `DHS3050`, `NWR2938`, or numeric asset `1171`.

### Machine name tips

| Style | Example | Used by |
|-------|---------|---------|
| EGM display name | `Dragons-0181`, `OSMCP181` | `/checkcredit`, `/cctv`, `/stuckcredit` |
| Site + number | `DHS3077`, `NWR2938` | `/checkmachinelog` |
| Short numeric | `1171`, `2074` | `/checkcredit` (LogNavigator style) |

When the bot shows `❌ Usage:`, copy the example format from that message.

### Thread replies

Credit and log commands almost always reply in a **thread** under your message:

1. You send `@Log & Credit Bot /checkcredit OSMCP181`
2. Bot posts `⏳ please wait...` in the thread
3. Final screenshots / log text appear in the **same thread** — tap the message to expand it

---

## Typical workflows

### Stuck credit on floor

```
@Log & Credit Bot /stuckcredit OSMCP181
@Log & Credit Bot /stuckcredit Dragons-0181 2026-06-28
```

### Quick credit without knowing the player

```
@Log & Credit Bot /checkcreditdate
```
Fill in the card → bot runs the full credit pipeline.

### SMS OTP issues

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

## Natural language examples

| You say | Routed to |
|---------|-----------|
| `check credit for OSMCP181` | `/checkcredit` |
| `machine log for Dragons-0181` | `/checkmachinelog` |
| `stuck credit on NCH1900` | `/stuckcredit` |
| `sms otp fail today` | `/smsfail` |

---

## FAQ

**Why a separate group?** Log/credit commands often need screenshots and long replies — keeping them here avoids cluttering the main duty chat.

**Slow responses?** Credit and log flows may launch a browser on the server; first request after idle can take longer.

**Wrong group?** Machine **asset** lookup (`/nwr`, `/nch`) is **Group 4**, not this group.

**More help:** [User guide §5](USER_GUIDE.md#5-group-2--log--credit-walkthroughs) · [`/checkcreditdate` wizard](USER_GUIDE.md#51-full-credit-check-recommended-for-new-users)
