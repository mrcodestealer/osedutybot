"""
gold.py — LIVE / PAPER trading of the PORTFOLIO strategy (matches backtest.py).

Combines 4 OOS-validated sleeves as independent positions, each gated by a daily
SMA(50) trend filter and sized by volatility-parity risk (1% equity/trade):

    consensus_trend, allweather, keltner_pullback, donchian_break

Signals are imported directly from backtest.py (build_tf_bars + build_signals) so
live behaviour is identical to the backtest. Each sleeve trades at most one position
at a time, tagged with its own MT5 magic number. Exits: 50% partial @ 2R, trailing
stop, and a 72-hour time stop — same as the backtest engine.

Account is an OANDA DEMO (paper). Set ENABLE_TRADING=False for signal-only dry runs.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime

import pandas as pd
import MetaTrader5 as mt5

import backtest as bt  # reuse the exact signal/feature code

STRATEGY_NAME = "portfolio"
STRATEGY_VERSION = "3.0"

# ----------------------------- MT5 connection -----------------------------
def _default_mt5_path() -> str | None:
    env = os.environ.get("MT5_TERMINAL_PATH")
    if env and os.path.isfile(env):
        return env
    if sys.platform == "win32":
        for p in (
            r"C:\Program Files\MetaTrader 5\terminal64.exe",
            r"C:\Program Files\OANDA MetaTrader Second\terminal64.exe",
            r"C:\Program Files\OANDA - MetaTrader 5\terminal64.exe",
        ):
            if os.path.isfile(p):
                return p
        return None
    return (
        r"/Users/junchen/Library/Application Support/net.metaquotes.wine.metatrader5"
        r"/drive_c/Program Files/MetaTrader 5/terminal64.exe"
    )

MT5_TERMINAL_PATH = _default_mt5_path()

server = "OANDA_Global-Demo-1"
login = 1715532098
password = "Jcsiah0318--=="

SYMBOL = "XAUUSD.sml"

# ----------------------------- Portfolio config (== backtest winner) -----------------------------
RISK_FRAC = 0.01          # 1% equity risk per trade (vol parity)
TREND_SMA = 50            # daily SMA trend gate (highest-Sharpe preset)
CONTRACT_SIZE_FALLBACK = 100.0
MAX_LOT_CAP = bt.MAX_LOT  # 0.05 hard cap (also clamped to broker max)

# Shared exit params (all 4 sleeves use backtest defaults)
ATR_MULTIPLIER_SL = 2.0
PARTIAL_R = 2.0
TRAIL_ATR_FRAC = 0.10
TIME_STOP_BARS = 72       # close after 72 x 1H bars (== backtest)

# Session gate is applied inside build_signals (server-time bar hour 7..21).
TRADE_START_HOUR = bt.SESSION_START_H
TRADE_END_HOUR = bt.SESSION_END_H

BARS_5M = 30000           # ~104 calendar days of 5m (enough for SMA50 + atr_pct warmup)

# The 4 selected sleeves (high-Sharpe preset). Defaults => atr_sl=2.0, cooldown=6, partial=2R.
SLEEVE_NAMES = ["consensus_trend", "allweather", "keltner_pullback", "donchian_break"]
SLEEVES: dict[str, bt.StrategyParams] = {
    name: bt.StrategyParams(
        strategy=name, long_only=False, risk_frac=RISK_FRAC, trend_sma_days=TREND_SMA,
        atr_mult_sl=ATR_MULTIPLIER_SL, partial_r=PARTIAL_R, trail_atr_frac=TRAIL_ATR_FRAC,
    )
    for name in SLEEVE_NAMES
}
BASE_MAGIC = 540000
MAGIC = {name: BASE_MAGIC + i for i, name in enumerate(SLEEVE_NAMES)}
MAGIC_TO_SLEEVE = {v: k for k, v in MAGIC.items()}

ENABLE_TRADING = True     # False => signal-only dry run (no orders sent)
STATE_FILE = "portfolio_state.json"

logging.basicConfig(filename="strategy_logGold.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


def log(msg: str) -> None:
    print(msg, flush=True)
    logging.info(msg)


# ----------------------------- MT5 helpers -----------------------------
def initialize_mt5() -> bool:
    ok = mt5.initialize(MT5_TERMINAL_PATH) if MT5_TERMINAL_PATH else mt5.initialize()
    if not ok:
        print("Failed to initialize MetaTrader 5")
        if MT5_TERMINAL_PATH:
            print(f"  Tried path: {MT5_TERMINAL_PATH}")
        print("  Set env MT5_TERMINAL_PATH to terminal64.exe or install MT5")
        mt5.shutdown()
        return False
    if not mt5.login(login, password, server):
        print(f"Failed to login: {mt5.last_error()}")
        mt5.shutdown()
        return False
    print("Login successfully")
    return True


def get_rates(symbol: str, timeframe, bars: int) -> pd.DataFrame | None:
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, bars)
    if rates is None or len(rates) == 0:
        print(f"Failed to get rates for {symbol}")
        return None
    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    df.set_index("time", inplace=True)
    return df


def contract_size() -> float:
    info = mt5.symbol_info(SYMBOL)
    cs = getattr(info, "trade_contract_size", None) if info else None
    return float(cs) if cs else CONTRACT_SIZE_FALLBACK


def account_equity() -> float:
    acct = mt5.account_info()
    return float(acct.equity) if acct else bt.INITIAL_CAPITAL


def compute_lots(sl_dist: float) -> float:
    """Volatility-parity sizing clamped to the broker's lot limits."""
    info = mt5.symbol_info(SYMBOL)
    equity = account_equity()
    cs = contract_size()
    raw = (RISK_FRAC * equity) / (sl_dist * cs) if sl_dist > 0 else 0.0
    vmin = getattr(info, "volume_min", 0.01) if info else 0.01
    vmax = min(getattr(info, "volume_max", MAX_LOT_CAP) if info else MAX_LOT_CAP, MAX_LOT_CAP)
    vstep = getattr(info, "volume_step", 0.01) if info else 0.01
    lots = max(vmin, min(vmax, raw))
    lots = round(round(lots / vstep) * vstep, 3)
    implied_risk = lots * sl_dist * cs
    if raw < vmin:
        log(f"  ⚠ risk-target lots {raw:.5f} < broker min {vmin}. Using {lots} "
            f"=> trade risks ~${implied_risk:.2f} ({implied_risk/equity*100:.1f}% of equity). "
            f"Account too small for true 1% sizing.")
    return lots


# ----------------------------- Signals (reuse backtest) -----------------------------
def build_closed_bars() -> pd.DataFrame | None:
    """5m -> 1H feature frame (identical to backtest), dropping the forming bar."""
    df5 = get_rates(SYMBOL, mt5.TIMEFRAME_M5, BARS_5M)
    if df5 is None or len(df5) < 5000:
        return None
    for c in ("open", "high", "low", "close", "tick_volume"):
        if c not in df5.columns:
            return None
    bars = bt.build_tf_bars(df5)["1h"]
    if len(bars) < 60:
        return None
    return bars.iloc[:-1]  # drop the still-forming 1H bar


def eval_signal(bars_closed: pd.DataFrame, p: bt.StrategyParams) -> str | None:
    sig = bt.build_signals(bars_closed, p)
    s = int(sig[-1])
    return "buy" if s > 0 else "sell" if s < 0 else None


def session_ok(bars_closed: pd.DataFrame) -> bool:
    h = int(bars_closed.index[-1].hour)
    return TRADE_START_HOUR <= h < TRADE_END_HOUR


# ----------------------------- State -----------------------------
def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {"orders": {}, "last_bar": {}}
    try:
        with open(STATE_FILE) as f:
            st = json.load(f)
            st.setdefault("orders", {})
            st.setdefault("last_bar", {})
            return st
    except Exception:
        return {"orders": {}, "last_bar": {}}


def save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2, default=str)
    except Exception as e:
        logging.error(f"save_state failed: {e}")


# ----------------------------- Orders -----------------------------
def place_order(direction: str, lots: float, sl: float, tp: float, magic: int, comment: str):
    tick = mt5.symbol_info_tick(SYMBOL)
    if tick is None:
        return None
    price = tick.ask if direction == "buy" else tick.bid
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": SYMBOL,
        "volume": lots,
        "type": mt5.ORDER_TYPE_BUY if direction == "buy" else mt5.ORDER_TYPE_SELL,
        "price": price,
        "sl": sl,
        "tp": tp,
        "deviation": 20,
        "magic": magic,
        "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }
    result = mt5.order_send(request)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        msg = result.comment if result else mt5.last_error()
        log(f"  Order FAILED [{comment}]: {msg}")
        return None
    log(f"  ORDER {direction.upper()} {lots} {SYMBOL} @ {price:.2f} SL={sl:.2f} magic={magic} [{comment}]")
    return result.order


def modify_sl(ticket: int, new_sl: float) -> bool:
    pos = mt5.positions_get(ticket=ticket)
    if not pos:
        return False
    p = pos[0]
    result = mt5.order_send({
        "action": mt5.TRADE_ACTION_SLTP, "position": ticket,
        "sl": new_sl, "tp": p.tp, "symbol": p.symbol,
        "deviation": 20, "magic": p.magic,
    })
    return result is not None and result.retcode == mt5.TRADE_RETCODE_DONE


def close_volume(pos, lots: float, comment: str) -> bool:
    tick = mt5.symbol_info_tick(pos.symbol)
    if tick is None:
        return False
    is_buy = pos.type == mt5.ORDER_TYPE_BUY
    result = mt5.order_send({
        "action": mt5.TRADE_ACTION_DEAL, "symbol": pos.symbol, "volume": lots,
        "type": mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY,
        "position": pos.ticket, "price": tick.bid if is_buy else tick.ask,
        "deviation": 20, "magic": pos.magic, "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC, "type_filling": mt5.ORDER_FILLING_FOK,
    })
    return result is not None and result.retcode == mt5.TRADE_RETCODE_DONE


# ----------------------------- Position management (== backtest exits) -----------------------------
def manage_position(name: str, od: dict, pos, df_5m: pd.DataFrame, bars_closed: pd.DataFrame) -> None:
    tick = mt5.symbol_info_tick(SYMBOL)
    if tick is None:
        return
    is_buy = od["direction"] == "buy"
    entry = od["entry_price"]
    atr_fixed = od["atr_fixed"]
    sl_dist = od["initial_sl_points"]
    cur = tick.bid if is_buy else tick.ask
    profit = (cur - entry) if is_buy else (entry - cur)
    r = profit / sl_dist if sl_dist else 0.0

    # Time stop: 72 x 1H bars since entry.
    try:
        elapsed_h = (bars_closed.index[-1] - pd.Timestamp(od["entry_bar"])).total_seconds() / 3600
        if elapsed_h >= TIME_STOP_BARS:
            if close_volume(pos, pos.volume, f"{name} time_stop"):
                log(f"  [{name}] TIME STOP after {elapsed_h:.0f}h -> closed")
            return
    except Exception:
        pass

    # 50% partial @ 2R, then move stop to breakeven+trail.
    if not od.get("partial_closed") and profit >= PARTIAL_R * sl_dist:
        half = round(pos.volume / 2, 3)
        if half > 0 and close_volume(pos, half, f"{name} partial_2R"):
            od["partial_closed"] = True
            be = entry + (TRAIL_ATR_FRAC * atr_fixed if is_buy else -TRAIL_ATR_FRAC * atr_fixed)
            modify_sl(pos.ticket, be)
            log(f"  [{name}] PARTIAL 50% @ 2R, SL -> {be:.2f}")
            return

    if od.get("partial_closed"):
        # Post-partial: trail with recent 5m extremes once >= 3R.
        if profit >= 3 * sl_dist:
            if is_buy:
                new_sl = float(df_5m["low"].iloc[-20:].min()) - 0.2 * atr_fixed
                if new_sl > entry and new_sl > pos.sl:
                    modify_sl(pos.ticket, new_sl)
            else:
                new_sl = float(df_5m["high"].iloc[-20:].max()) + 0.2 * atr_fixed
                if new_sl < entry and new_sl < pos.sl:
                    modify_sl(pos.ticket, new_sl)
    else:
        # Pre-partial step trailing (== backtest).
        new_sl = None
        if r >= 4:
            new_sl = entry + (2 * sl_dist if is_buy else -2 * sl_dist)
        elif r >= 3:
            new_sl = entry + (sl_dist if is_buy else -sl_dist)
        elif r >= 2:
            new_sl = entry + (TRAIL_ATR_FRAC * atr_fixed if is_buy else -TRAIL_ATR_FRAC * atr_fixed)
        if new_sl is not None:
            if (is_buy and new_sl > pos.sl) or (not is_buy and new_sl < pos.sl):
                modify_sl(pos.ticket, new_sl)


# ----------------------------- Main loop -----------------------------
def main() -> None:
    if not initialize_mt5():
        return
    if mt5.symbol_info(SYMBOL) is None:
        print(f"Symbol {SYMBOL} not found")
        mt5.shutdown()
        return
    mt5.symbol_select(SYMBOL, True)

    state = load_state()

    print("=" * 72)
    print(f"gold.py  PORTFOLIO  v{STRATEGY_VERSION}   (paper / demo)")
    print(f"Sleeves : {', '.join(SLEEVE_NAMES)}")
    print(f"Config  : risk={RISK_FRAC:.1%}/trade | trend_sma={TREND_SMA} | "
          f"atr_sl={ATR_MULTIPLIER_SL} | partial={PARTIAL_R}R | timestop={TIME_STOP_BARS}h")
    print(f"Trading : {'LIVE (demo)' if ENABLE_TRADING else 'DRY RUN (signals only)'} | session {TRADE_START_HOUR}-{TRADE_END_HOUR}h")
    print("=" * 72)

    while True:
        try:
            bars_closed = build_closed_bars()
            df_5m = get_rates(SYMBOL, mt5.TIMEFRAME_M5, 200)
            if bars_closed is None or df_5m is None:
                time.sleep(10)
                continue

            bar_time = str(bars_closed.index[-1])
            equity = account_equity()
            positions = mt5.positions_get(symbol=SYMBOL) or ()
            open_by_magic = {p.magic: p for p in positions if p.magic in MAGIC_TO_SLEEVE}

            print("\n" + "=" * 72)
            print(f"bar={bar_time} | equity=${equity:.2f} | open={len(open_by_magic)}/{len(SLEEVE_NAMES)}")

            for name in SLEEVE_NAMES:
                p = SLEEVES[name]
                magic = MAGIC[name]
                sig = eval_signal(bars_closed, p)
                pos = open_by_magic.get(magic)
                orders = state["orders"]

                # Reconcile: position closed externally / by SL.
                if pos is None and name in orders:
                    log(f"  [{name}] position closed -> clearing state")
                    orders.pop(name, None)
                    save_state(state)

                if pos is not None:
                    od = orders.get(name)
                    if od:
                        manage_position(name, od, pos, df_5m, bars_closed)
                        save_state(state)
                    print(f"  [{name:16s}] OPEN {od['direction'] if od else '?'} "
                          f"vol={pos.volume} entry={pos.price_open:.2f} sl={pos.sl:.2f} P/L={pos.profit:+.2f} | sig={sig or 'none'}")
                    continue

                # Flat: consider a new entry (once per closed bar).
                last_bar = state["last_bar"].get(name)
                print(f"  [{name:16s}] FLAT | sig={sig or 'none'}")
                if not sig:
                    continue
                if not session_ok(bars_closed):
                    print(f"      outside session -> skip")
                    continue
                if last_bar == bar_time:
                    continue  # already acted on this bar

                state["last_bar"][name] = bar_time
                atr_v = float(bars_closed["atr"].iloc[-1])
                if atr_v <= 0:
                    save_state(state)
                    continue
                sl_dist = atr_v * ATR_MULTIPLIER_SL
                tick = mt5.symbol_info_tick(SYMBOL)
                px = (tick.ask if sig == "buy" else tick.bid) if tick else float(bars_closed["close"].iloc[-1])
                sl_price = px - sl_dist if sig == "buy" else px + sl_dist
                tp_price = px + sl_dist * 20 if sig == "buy" else px - sl_dist * 20
                lots = compute_lots(sl_dist)

                print(f"      *** {sig.upper()} SIGNAL *** atr={atr_v:.2f} sl_dist={sl_dist:.2f} lots={lots}")
                if not ENABLE_TRADING:
                    log(f"  [{name}] DRY RUN {sig} lots={lots} (no order sent)")
                    save_state(state)
                    continue

                ticket = place_order(sig, lots, sl_price, tp_price, magic, name)
                if ticket:
                    orders[name] = {
                        "ticket": ticket, "direction": sig, "entry_price": px,
                        "atr_fixed": atr_v, "initial_sl_points": sl_dist,
                        "entry_bar": bar_time, "entry_time": datetime.now().isoformat(),
                        "partial_closed": False,
                    }
                save_state(state)

            time.sleep(30)

        except Exception as e:
            print(f"Error in main loop: {e}")
            logging.exception("Main loop exception")
            time.sleep(10)

    mt5.shutdown()


if __name__ == "__main__":
    main()
