import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime
import time
import logging
import json
import os
import sys

STRATEGY_NAME = "allweather"
STRATEGY_VERSION = "2.0"

# ----------------------------- 配置 (allweather — 与 backtest.py 一致) -----------------------------
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
        return None  # Windows: auto-detect installed terminal
    return (
        r"/Users/junchen/Library/Application Support/net.metaquotes.wine.metatrader5"
        r"/drive_c/Program Files/MetaTrader 5/terminal64.exe"
    )

MT5_TERMINAL_PATH = _default_mt5_path()

server = "OANDA_Global-Demo-1"
login = 1715532098
password = "Jcsiah0318--=="

SYMBOL = "XAUUSD.sml"
VOLUME = 0.002

# allweather 冠军参数
ATR_PERIOD = 14
ATR_MULTIPLIER_SL = 2.5
PARTIAL_R = 2.0
TRAIL_ATR_FRAC = 0.10
COOLDOWN_BARS = 2          # 1H K 线根数
DONCHIAN = 15
RSI_ENTRY = 35
ADX_THRESH = 18
EMA_FAST = 20
EMA_SLOW = 50

TRADE_START_HOUR = 7       # 与 backtest SESSION 一致
TRADE_END_HOUR = 21

ORDER_DETAILS_FILE = "order_detailsGold.json"
STATE_FILE = "allweather_state.json"

logging.basicConfig(filename='strategy_logGold.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


# ----------------------------- MT5 基础 -----------------------------
def initialize_mt5():
    ok = mt5.initialize(MT5_TERMINAL_PATH) if MT5_TERMINAL_PATH else mt5.initialize()
    if not ok:
        print("Failed to initialize MetaTrader 5")
        if MT5_TERMINAL_PATH:
            print(f"  Tried path: {MT5_TERMINAL_PATH}")
        print("  Set env MT5_TERMINAL_PATH to terminal64.exe or install MT5")
        mt5.shutdown()
        return False
    authorized = mt5.login(login, password, server)
    if not authorized:
        print(f"Failed to login: {mt5.last_error()}")
        mt5.shutdown()
        return False
    print("Login successfully")
    return True


def get_rates(symbol, timeframe, bars=300):
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, bars)
    if rates is None or len(rates) == 0:
        print(f"Failed to get rates for {symbol} {timeframe}")
        return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.set_index('time', inplace=True)
    return df


# ----------------------------- 指标 -----------------------------
def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def calculate_atr(df, period=ATR_PERIOD):
    high, low, close = df['high'], df['low'], df['close']
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def calculate_rsi(close, period=14):
    d = close.diff()
    g = d.clip(lower=0).rolling(period).mean()
    l = (-d.clip(upper=0)).rolling(period).mean()
    return 100 - (100 / (1 + g / l.replace(0, pd.NA)))


def calculate_adx(df, period=14):
    h, l = df['high'], df['low']
    c_prev = df['close'].shift()
    tr = pd.concat([(h - l), (h - c_prev).abs(), (l - c_prev).abs()], axis=1).max(axis=1)
    up, dn = h - h.shift(), l.shift() - l
    plus_dm = pd.Series(
        [u if u > d and u > 0 else 0.0 for u, d in zip(up, dn)], index=df.index
    )
    minus_dm = pd.Series(
        [d if d > u and d > 0 else 0.0 for u, d in zip(up, dn)], index=df.index
    )
    atr_v = tr.rolling(period).mean().replace(0, pd.NA)
    plus_di = 100 * plus_dm.rolling(period).mean() / atr_v
    minus_di = 100 * minus_dm.rolling(period).mean() / atr_v
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA)
    return dx.rolling(period).mean()


def add_allweather_indicators(df):
    out = df.copy()
    out['atr'] = calculate_atr(out)
    out['ema_f'] = calculate_ema(out['close'], EMA_FAST)
    out['ema_s'] = calculate_ema(out['close'], EMA_SLOW)
    out['rsi'] = calculate_rsi(out['close'])
    out['adx'] = calculate_adx(out)
    bb_m = out['close'].rolling(20).mean()
    bb_s = out['close'].rolling(20).std()
    out['bb_upper'] = bb_m + 2 * bb_s
    out['bb_lower'] = bb_m - 2 * bb_s
    out['don_hi'] = out['high'].rolling(DONCHIAN).max().shift(1)
    out['don_lo'] = out['low'].rolling(DONCHIAN).min().shift(1)
    return out


def eval_allweather_signal(df_1h, df_4h):
    """
    在最后一根已收盘 1H K 线上评估 allweather 信号。
    返回: 'buy' | 'sell' | None
    """
    h1 = df_1h.iloc[:-1].copy()
    h4 = df_4h.iloc[:-1].copy()
    if len(h1) < max(DONCHIAN + 5, 50) or len(h4) < EMA_SLOW + 5:
        return None, {}

    h1 = add_allweather_indicators(h1)
    row = h1.iloc[-1]
    prev = h1.iloc[-2]

    ema_f_4h = calculate_ema(h4['close'], EMA_FAST).iloc[-1]
    ema_s_4h = calculate_ema(h4['close'], EMA_SLOW).iloc[-1]
    bull4 = ema_f_4h > ema_s_4h
    bear4 = ema_f_4h < ema_s_4h

    trending = row['adx'] >= ADX_THRESH
    ranging = not trending

    brk_up = row['close'] > row['don_hi']
    brk_dn = row['close'] < row['don_lo']
    long_trend = brk_up and (bull4 or trending)
    short_trend = brk_dn and (bear4 or trending)

    rsi_x_up = (prev['rsi'] < RSI_ENTRY) and (row['rsi'] > RSI_ENTRY)
    rsi_x_dn = (prev['rsi'] > 100 - RSI_ENTRY) and (row['rsi'] < 100 - RSI_ENTRY)
    long_range = ranging and (row['close'] <= row['bb_lower']) and rsi_x_up
    short_range = ranging and (row['close'] >= row['bb_upper']) and rsi_x_dn

    long_sig = bool(long_trend or long_range)
    short_sig = bool(short_trend or short_range)

    info = {
        'bar_time': h1.index[-1],
        'close': float(row['close']),
        'adx': float(row['adx']),
        'rsi': float(row['rsi']),
        'mode': 'trend' if trending else 'range',
        'long_trend': long_trend,
        'short_trend': short_trend,
        'long_range': long_range,
        'short_range': short_range,
        'bull4': bull4,
        'bear4': bear4,
    }

    if long_sig and not short_sig:
        return 'buy', info
    if short_sig and not long_sig:
        return 'sell', info
    return None, info


def check_trading_time():
    hour = datetime.now().hour
    return TRADE_START_HOUR <= hour < TRADE_END_HOUR


# ----------------------------- 状态持久化 -----------------------------
def load_order_details():
    if not os.path.exists(ORDER_DETAILS_FILE):
        return None
    try:
        with open(ORDER_DETAILS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load order details: {e}")
        return None


def save_order_details(details):
    try:
        with open(ORDER_DETAILS_FILE, 'w') as f:
            json.dump(details, f, indent=4)
    except Exception as e:
        logging.error(f"Failed to save order details: {e}")


def remove_order_details():
    try:
        if os.path.exists(ORDER_DETAILS_FILE):
            os.remove(ORDER_DETAILS_FILE)
    except Exception as e:
        logging.error(f"Failed to remove order details: {e}")


def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=4)
    except Exception as e:
        logging.error(f"Failed to save state: {e}")


def cooldown_ok(last_trade_bar_time, current_bar_time):
    if not last_trade_bar_time:
        return True
    try:
        last = pd.Timestamp(last_trade_bar_time)
        cur = pd.Timestamp(current_bar_time)
        hours = (cur - last).total_seconds() / 3600
        return hours > COOLDOWN_BARS
    except Exception:
        return True


# ----------------------------- 下单与风控 -----------------------------
def place_order(direction, sl_price, tp_price):
    tick = mt5.symbol_info_tick(SYMBOL)
    if tick is None:
        print("Failed to get tick")
        return None
    order_type = mt5.ORDER_TYPE_BUY if direction == 'buy' else mt5.ORDER_TYPE_SELL
    price = tick.ask if direction == 'buy' else tick.bid
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": SYMBOL,
        "volume": VOLUME,
        "type": order_type,
        "price": price,
        "sl": sl_price,
        "tp": tp_price,
        "deviation": 10,
        "magic": 123456,
        "comment": "AllWeather",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }
    result = mt5.order_send(request)
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Order failed: {result.comment}")
        logging.error(f"Order failed: {result.comment}")
        return None
    print(f"Order placed: {direction} {VOLUME} at {price}, SL={sl_price}")
    logging.info(f"Order placed: {direction} {VOLUME} at {price}, SL={sl_price}")
    return result.order


def modify_order(ticket, new_sl):
    position = mt5.positions_get(ticket=ticket)
    if not position:
        return False
    pos = position[0]
    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "position": ticket,
        "sl": new_sl,
        "tp": pos.tp,
        "symbol": pos.symbol,
        "deviation": 10,
        "magic": 123456,
        "comment": "AllWeather trailing",
    }
    result = mt5.order_send(request)
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Failed to modify SL: {result.comment}")
        return False
    print(f"Trailing stop updated to {new_sl} for ticket {ticket}")
    return True


def close_partial_and_move_stop(ticket, entry_price, atr_fixed, initial_sl_points,
                                partial_closed, direction, order_details_path):
    if partial_closed:
        return partial_closed, True

    position = mt5.positions_get(ticket=ticket)
    if not position:
        return partial_closed, False
    pos = position[0]
    tick = mt5.symbol_info_tick(pos.symbol)
    if tick is None:
        return partial_closed, False

    is_buy = direction == 'buy'
    current_price = tick.bid if is_buy else tick.ask
    profit_points = (current_price - entry_price) if is_buy else (entry_price - current_price)

    if profit_points < PARTIAL_R * initial_sl_points:
        return partial_closed, False

    half_vol = round(pos.volume / 2, 3)
    if half_vol <= 0:
        return partial_closed, False

    order_type = mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY
    close_price = tick.bid if is_buy else tick.ask
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": pos.symbol,
        "volume": half_vol,
        "type": order_type,
        "position": ticket,
        "price": close_price,
        "deviation": 10,
        "magic": 123456,
        "comment": "AllWeather partial 2R",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }
    result = mt5.order_send(request)
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Partial close failed: {result.comment}")
        return partial_closed, False

    print(f"Partially closed {half_vol} at {close_price}")
    logging.info(f"Partially closed {half_vol} at {close_price} for ticket {ticket}")

    try:
        with open(order_details_path, 'r') as f:
            details = json.load(f)
        details['partial_closed'] = True
        with open(order_details_path, 'w') as f:
            json.dump(details, f, indent=4)
    except Exception as e:
        print(f"Could not update order_details: {e}")

    if is_buy:
        new_sl = entry_price + TRAIL_ATR_FRAC * atr_fixed
    else:
        new_sl = entry_price - TRAIL_ATR_FRAC * atr_fixed
    modify_order(ticket, new_sl)
    return True, True


def update_trailing_stop(ticket, entry_price, current_price, atr_fixed, initial_sl_points, direction):
    order = mt5.positions_get(ticket=ticket)
    if not order:
        return False
    pos = order[0]
    is_buy = direction == 'buy'
    profit_points = (current_price - entry_price) if is_buy else (entry_price - current_price)
    r = profit_points / initial_sl_points if initial_sl_points else 0

    new_sl = None
    if r >= 4:
        new_sl = entry_price + (2 * initial_sl_points if is_buy else -2 * initial_sl_points)
    elif r >= 3:
        new_sl = entry_price + (initial_sl_points if is_buy else -initial_sl_points)
    elif r >= 2:
        new_sl = entry_price + (TRAIL_ATR_FRAC * atr_fixed if is_buy else -TRAIL_ATR_FRAC * atr_fixed)

    if new_sl is None:
        return False
    if is_buy and new_sl > pos.sl:
        return modify_order(ticket, new_sl)
    if not is_buy and new_sl < pos.sl:
        return modify_order(ticket, new_sl)
    return False


def update_stop_after_partial(ticket, entry_price, atr_fixed, initial_sl_points, current_price,
                              direction, df_5m):
    """部分平仓后：盈利 >= 3R 时用 5M 高低点移动止损（与 backtest 类似）。"""
    order = mt5.positions_get(ticket=ticket)
    if not order:
        return False
    pos = order[0]
    is_buy = direction == 'buy'
    profit_points = (current_price - entry_price) if is_buy else (entry_price - current_price)
    if profit_points < 3 * initial_sl_points:
        return False

    if is_buy:
        recent_low = df_5m['low'].iloc[-20:].min()
        new_sl = recent_low - 0.2 * atr_fixed
        if new_sl <= entry_price or new_sl <= pos.sl + 0.5 * atr_fixed:
            return False
        return modify_order(ticket, new_sl)
    else:
        recent_high = df_5m['high'].iloc[-20:].max()
        new_sl = recent_high + 0.2 * atr_fixed
        if new_sl >= entry_price or new_sl >= pos.sl - 0.5 * atr_fixed:
            return False
        return modify_order(ticket, new_sl)


# ----------------------------- 主循环 -----------------------------
def main():
    if not initialize_mt5():
        return

    if mt5.symbol_info(SYMBOL) is None:
        print(f"Symbol {SYMBOL} not found")
        mt5.shutdown()
        return

    persisted = load_order_details()
    state = load_state()
    ticket = entry_price = entry_atr_fixed = initial_sl_points = direction = entry_time = None
    partial_closed = False
    last_signal_bar = state.get('last_signal_bar')
    last_processed_bar = state.get('last_processed_bar')

    if persisted:
        ticket = persisted.get('ticket')
        entry_price = persisted.get('entry_price')
        entry_atr_fixed = persisted.get('atr_fixed')
        initial_sl_points = persisted.get('initial_sl_points')
        direction = persisted.get('direction')
        partial_closed = persisted.get('partial_closed', False)
        entry_time = persisted.get('entry_time')
        pos = mt5.positions_get(ticket=ticket) if ticket else None
        if not pos:
            remove_order_details()
            ticket = entry_price = entry_atr_fixed = initial_sl_points = direction = entry_time = None
            partial_closed = False
        else:
            print(f"Restored active order: {ticket} {direction} @ {entry_price}")

    print("=" * 70)
    print(f"gold.py  {STRATEGY_NAME.upper()} ONLY  v{STRATEGY_VERSION}")
    print("Expected log: 'AllWeather | bar=... ADX=... RSI=... signal=...'")
    print("If you see GoldenCross / Market Structure → you are running OLD gold.py")
    print("=" * 70)
    print(f"Strategy: {STRATEGY_NAME} | 1H signals | long+short | {VOLUME} lots")
    print(f"Params: donchian={DONCHIAN} adx={ADX_THRESH} rsi={RSI_ENTRY} "
          f"atr_sl={ATR_MULTIPLIER_SL} cooldown={COOLDOWN_BARS}h")

    while True:
        try:
            df_1h = get_rates(SYMBOL, mt5.TIMEFRAME_H1, 300)
            df_4h = get_rates(SYMBOL, mt5.TIMEFRAME_H4, 200)
            df_5m = get_rates(SYMBOL, mt5.TIMEFRAME_M5, 200)
            if df_1h is None or df_4h is None or df_5m is None:
                time.sleep(5)
                continue

            signal, info = eval_allweather_signal(df_1h, df_4h)
            bar_time = str(info.get('bar_time', ''))
            h1_closed = df_1h.iloc[:-1]
            atr_1h = calculate_atr(h1_closed).iloc[-1]
            tick = mt5.symbol_info_tick(SYMBOL)
            current_price = tick.bid if tick else df_5m['close'].iloc[-1]

            print("\n" + "=" * 70)
            print(f"AllWeather | bar={bar_time} | mode={info.get('mode')} | "
                  f"ADX={info.get('adx', 0):.1f} RSI={info.get('rsi', 0):.1f}")
            print(f"  trend L/S={info.get('long_trend')}/{info.get('short_trend')} | "
                  f"range L/S={info.get('long_range')}/{info.get('short_range')}")
            print(f"  signal={signal or 'none'} | 1H ATR={atr_1h:.2f} | price={current_price:.2f}")
            print("=" * 70)

            if not check_trading_time():
                print("Outside session (7–21). Waiting...")
                time.sleep(30)
                continue

            positions = mt5.positions_get(magic=123456)

            if positions:
                pos = positions[0]
                if ticket and pos.ticket == ticket:
                    is_buy = direction == 'buy'
                    px = tick.bid if is_buy else tick.ask if tick else current_price
                    new_partial, _ = close_partial_and_move_stop(
                        ticket, entry_price, entry_atr_fixed, initial_sl_points,
                        partial_closed, direction, ORDER_DETAILS_FILE,
                    )
                    if new_partial != partial_closed:
                        partial_closed = new_partial
                    elif partial_closed:
                        update_stop_after_partial(
                            ticket, entry_price, entry_atr_fixed, initial_sl_points,
                            px, direction, df_5m,
                        )
                    else:
                        update_trailing_stop(
                            ticket, entry_price, px, entry_atr_fixed,
                            initial_sl_points, direction,
                        )
            else:
                if ticket is not None:
                    remove_order_details()
                    ticket = entry_price = entry_atr_fixed = initial_sl_points = direction = None
                    partial_closed = False
                    entry_time = None

            # 每根新 1H K 线最多评估一次开仓
            if not positions and signal and bar_time and bar_time != last_processed_bar:
                last_processed_bar = bar_time
                state['last_processed_bar'] = bar_time
                save_state(state)

                if cooldown_ok(last_signal_bar, bar_time):
                    sl_dist = atr_1h * ATR_MULTIPLIER_SL
                    if signal == 'buy':
                        sl_price = current_price - sl_dist
                        tp_price = current_price + sl_dist * 20
                    else:
                        sl_price = current_price + sl_dist
                        tp_price = current_price - sl_dist * 20

                    print(f"*** {signal.upper()} SIGNAL (allweather) ***")
                    new_ticket = place_order(signal, sl_price, tp_price)
                    if new_ticket:
                        ticket = new_ticket
                        entry_price = current_price
                        entry_atr_fixed = atr_1h
                        initial_sl_points = sl_dist
                        direction = signal
                        partial_closed = False
                        entry_time = datetime.now().isoformat()
                        last_signal_bar = bar_time
                        state['last_signal_bar'] = bar_time
                        save_state(state)
                        save_order_details({
                            'ticket': ticket,
                            'direction': direction,
                            'entry_price': entry_price,
                            'atr_fixed': entry_atr_fixed,
                            'initial_sl_points': initial_sl_points,
                            'entry_time': entry_time,
                            'partial_closed': False,
                        })
                else:
                    print(f"Cooldown active ({COOLDOWN_BARS}h bars since last entry)")

            time.sleep(30)

        except Exception as e:
            print(f"Error in main loop: {e}")
            logging.exception("Main loop exception")
            time.sleep(10)

    mt5.shutdown()


if __name__ == "__main__":
    main()
