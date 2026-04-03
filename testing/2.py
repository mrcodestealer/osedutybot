import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
import json
import os
import re

# ----------------------------- 配置 -----------------------------
oanda_mt5_path = r"C:\Program Files\OANDA MetaTrader third\terminal64.exe"
server = "OANDA_Global-Demo-1"
login = 1715532141
password = "Jcsiah0318--=="

# 选择要交易的品种（支持任何 MT5 货币对，例如 AUDUSD, EURUSD, XAUUSD 等）
SYMBOL = "AUDUSD.sml"          # 可改为任何品种，如 "EURUSD", "GBPUSD", "XAUUSD"

# 根据品种生成独立的日志和订单详情文件名
safe_symbol = re.sub(r'[\\/*?:"<>|.]', '_', SYMBOL)   # 将非法字符替换为下划线
LOG_FILE = f'strategy_log_{safe_symbol}.log'
ORDER_DETAILS_FILE = f'order_details_{safe_symbol}.json'

VOLUME = 0.002                 # 固定手数
ATR_TIMEFRAME = mt5.TIMEFRAME_M15
ATR_PERIOD = 14
ATR_MULTIPLIER_SL = 2

# 多周期均线参数
EMA_FAST = 20
EMA_SLOW = 50

# MACD参数
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# 交易时间（UTC+8）
TRADE_START_HOUR = 16
TRADE_END_HOUR = 4

# 日志配置
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# ----------------------------- 辅助函数 -----------------------------
def initialize_mt5():
    if not mt5.initialize(oanda_mt5_path):
        print("Failed to initialize MetaTrader 5")
        mt5.shutdown()
        return False
    authorized = mt5.login(login, password, server)
    if not authorized:
        print(f"Failed to login: {mt5.last_error()}")
        mt5.shutdown()
        return False
    print("Login successfully")
    return True

def get_rates(symbol, timeframe, bars=200):
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, bars)
    if rates is None or len(rates) == 0:
        print(f"Failed to get rates for {symbol} {timeframe}")
        return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.set_index('time', inplace=True)
    return df

def calculate_ema(df, period):
    return df['close'].ewm(span=period, adjust=False).mean()

def calculate_macd(df, fast=12, slow=26, signal=9):
    exp1 = df['close'].ewm(span=fast, adjust=False).mean()
    exp2 = df['close'].ewm(span=slow, adjust=False).mean()
    macd_line = exp1 - exp2
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line

def close_partial_and_move_stop(ticket, entry_price, atr_fixed, initial_sl_points, partial_closed, order_details_path):
    if partial_closed:
        return partial_closed, True

    position = mt5.positions_get(ticket=ticket)
    if not position:
        return partial_closed, False
    pos = position[0]

    tick = mt5.symbol_info_tick(pos.symbol)
    if tick is None:
        return partial_closed, False
    current_price = tick.bid
    profit_points = current_price - entry_price

    if profit_points < 2 * initial_sl_points:
        return partial_closed, False

    half_vol = round(pos.volume / 2, 3)
    if half_vol <= 0:
        print("Half volume too small, cannot partial close")
        return partial_closed, False

    order_type = mt5.ORDER_TYPE_SELL
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": pos.symbol,
        "volume": half_vol,
        "type": order_type,
        "position": ticket,
        "price": current_price,
        "deviation": 10,
        "magic": 123456,
        "comment": "Partial close at 2R",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }
    result = mt5.order_send(request)
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Partial close failed: {result.comment}")
        return partial_closed, False

    print(f"✅ Partially closed {half_vol} at {current_price}")
    logging.info(f"Partially closed {half_vol} at {current_price} for ticket {ticket}")

    try:
        with open(order_details_path, 'r') as f:
            details = json.load(f)
        details['partial_closed'] = True
        with open(order_details_path, 'w') as f:
            json.dump(details, f, indent=4)
    except Exception as e:
        print(f"⚠️ Could not update order_details: {e}")

    new_sl = entry_price + 0.1 * atr_fixed
    modify_order(ticket, new_sl)

    return True, True

def calculate_atr(df, period=ATR_PERIOD):
    high = df['high']
    low = df['low']
    close = df['close']
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean().iloc[-1]
    return atr

def check_trading_time():
    now = datetime.now()
    hour = now.hour
    if TRADE_START_HOUR <= hour < 24 or 0 <= hour < TRADE_END_HOUR:
        return True
    return False

def place_order(direction, price, sl_price, tp_price):
    symbol = SYMBOL
    lot_size = VOLUME
    order_type = mt5.ORDER_TYPE_BUY if direction == 'buy' else mt5.ORDER_TYPE_SELL
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        print("Failed to get tick")
        return None
    request_price = tick.ask if direction == 'buy' else tick.bid
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": lot_size,
        "type": order_type,
        "price": request_price,
        "sl": sl_price,
        "tp": tp_price,
        "deviation": 10,
        "magic": 123456,
        "comment": "EMA_MACD_Strategy",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }
    result = mt5.order_send(request)
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Order failed: {result.comment}")
        logging.error(f"Order failed: {result.comment}")
        return None
    print(f"Order placed: {direction} {lot_size} at {request_price}, SL={sl_price}")
    logging.info(f"Order placed: {direction} {lot_size} at {request_price}, SL={sl_price}")
    return result.order

def modify_order(ticket, new_sl):
    position = mt5.positions_get(ticket=ticket)
    if not position:
        return False
    position = position[0]
    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "position": ticket,
        "sl": new_sl,
        "tp": position.tp,
        "symbol": position.symbol,
        "deviation": 10,
        "magic": 123456,
        "comment": "Trailing stop update",
    }
    result = mt5.order_send(request)
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Failed to modify SL: {result.comment}")
        return False
    print(f"Trailing stop updated to {new_sl} for ticket {ticket}")
    return True

def update_trailing_stop(ticket, entry_price, current_price, atr_fixed, initial_sl_points):
    order = mt5.positions_get(ticket=ticket)
    if not order:
        return False
    order = order[0]
    direction = 'buy' if order.type == mt5.ORDER_TYPE_BUY else 'sell'
    if direction == 'buy':
        profit_points = current_price - entry_price
    else:
        profit_points = entry_price - current_price

    if profit_points >= 2 * initial_sl_points:
        if direction == 'buy':
            new_sl = entry_price + 0.1 * atr_fixed
        else:
            new_sl = entry_price - 0.1 * atr_fixed
        if (direction == 'buy' and new_sl > order.sl) or (direction == 'sell' and new_sl < order.sl):
            modify_order(ticket, new_sl)
            return True

    if profit_points >= 3 * initial_sl_points:
        if direction == 'buy':
            new_sl = entry_price + initial_sl_points
        else:
            new_sl = entry_price - initial_sl_points
        if (direction == 'buy' and new_sl > order.sl) or (direction == 'sell' and new_sl < order.sl):
            modify_order(ticket, new_sl)
            return True

    if profit_points >= 4 * initial_sl_points:
        if direction == 'buy':
            new_sl = entry_price + 2 * initial_sl_points
        else:
            new_sl = entry_price - 2 * initial_sl_points
        if (direction == 'buy' and new_sl > order.sl) or (direction == 'sell' and new_sl < order.sl):
            modify_order(ticket, new_sl)
            return True

    return False

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
            logging.info("Order details file removed")
    except Exception as e:
        logging.error(f"Failed to remove order details: {e}")

def main():
    if not initialize_mt5():
        return

    symbol_info = mt5.symbol_info(SYMBOL)
    if symbol_info is None:
        print(f"Symbol {SYMBOL} not found")
        mt5.shutdown()
        return

    persisted = load_order_details()
    ticket = None
    entry_price = None
    entry_atr_fixed = None
    initial_sl_points = None
    direction = None
    partial_closed = False
    entry_time = None

    if persisted:
        ticket = persisted.get('ticket')
        entry_price = persisted.get('entry_price')
        entry_atr_fixed = persisted.get('atr_fixed')
        initial_sl_points = persisted.get('initial_sl_points')
        direction = persisted.get('direction')
        partial_closed = persisted.get('partial_closed', False)
        entry_time = persisted.get('entry_time')

        pos = mt5.positions_get(ticket=ticket) if ticket else None
        if pos:
            print(f"Restored active order: {ticket} at {entry_price}")
            logging.info(f"Restored active order: {ticket} at {entry_price}")
        else:
            print(f"Order {ticket} not found, removing details")
            logging.info(f"Order {ticket} not found, removing details")
            remove_order_details()
            ticket = None
            entry_price = None
            entry_atr_fixed = None
            initial_sl_points = None
            direction = None
            partial_closed = False
            entry_time = None

    conditions_met_previously = False

    while True:
        try:
            now = datetime.now()

            df_1h = get_rates(SYMBOL, mt5.TIMEFRAME_H1, 200)
            df_15m = get_rates(SYMBOL, mt5.TIMEFRAME_M15, 200)
            df_5m = get_rates(SYMBOL, mt5.TIMEFRAME_M5, 200)
            if df_1h is None or df_15m is None or df_5m is None:
                print("Failed to get data. Retrying...")
                time.sleep(5)
                continue

            ema20_1h = calculate_ema(df_1h, EMA_FAST).iloc[-1]
            ema50_1h = calculate_ema(df_1h, EMA_SLOW).iloc[-1]
            diff_1h = ema20_1h - ema50_1h
            ema20_15m = calculate_ema(df_15m, EMA_FAST).iloc[-1]
            ema50_15m = calculate_ema(df_15m, EMA_SLOW).iloc[-1]
            macd_line, signal_line = calculate_macd(df_5m, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
            macd_current = macd_line.iloc[-1]
            signal_current = signal_line.iloc[-1]
            macd_prev = macd_line.iloc[-2]
            signal_prev = signal_line.iloc[-2]
            atr_value = calculate_atr(df_15m)
            current_price = df_5m['close'].iloc[-1]

            cond_1h_trend = ema20_1h > ema50_1h
            cond_15m_trend = ema20_15m > ema50_15m
            golden_cross = (macd_current > signal_current) and (macd_prev <= signal_prev) and (macd_current > 0)

            # 显示信息（格式略，因篇幅保持原样，但小数点可保持5位）
            print("\n" + "="*70)
            h1_signal = "(Buy signal)" if cond_1h_trend else ""
            print(f"1H {h1_signal:<12} EMA20={ema20_1h:.5f}  EMA50={ema50_1h:.5f}  Diff={diff_1h:.5f}")
            m15_signal = "(Buy signal)" if cond_15m_trend else ""
            print(f"15M {m15_signal:<12} EMA20={ema20_15m:.5f}  EMA50={ema50_15m:.5f}  Diff={ema20_15m - ema50_15m:.5f}")
            m5_signal = "(Buy signal)" if golden_cross else ""
            macd_diff = macd_current - signal_current
            print(f"5M  {m5_signal:<12} MACD={macd_current:.5f}  Signal={signal_current:.5f}  Prev MACD={macd_prev:.5f}  Prev Signal={signal_prev:.5f}  Diff={macd_diff:.5f}")
            print(f"ATR (15M) = {atr_value:.5f}")
            print(f"Current Price = {current_price:.5f}")
            trend_1h_str = "Bullish" if cond_1h_trend else "Neutral/Bearish"
            trend_15m_str = "Bullish" if cond_15m_trend else "Neutral/Bearish"
            print(f"Conditions: 1H Trend={trend_1h_str} 15M Trend={trend_15m_str} GoldenCross={golden_cross}")
            if cond_1h_trend and cond_15m_trend:
                potential_sl = current_price - atr_value * ATR_MULTIPLIER_SL
                print(f"If place order, SL will be {potential_sl:.5f}")
            else:
                print("If place order, SL will be ---")
            print("="*70)

            # 显示活跃订单状态（与之前相同，略，可保留）
            # ... （因篇幅省略，但实际代码中应保留）

            all_conditions_met = cond_1h_trend and cond_15m_trend and golden_cross
            if all_conditions_met:
                if not conditions_met_previously:
                    potential_sl = current_price - atr_value * ATR_MULTIPLIER_SL
                    logging.info(f"ALL CONDITIONS MET at {now.strftime('%Y-%m-%d %H:%M:%S')} | "
                                 f"Price={current_price:.5f} | SL would be {potential_sl:.5f} | "
                                 f"1H Diff={diff_1h:.5f} | 15M Diff={ema20_15m - ema50_15m:.5f} | "
                                 f"MACD={macd_current:.5f} Signal={signal_current:.5f} | ATR={atr_value:.5f}")
                    conditions_met_previously = True
            else:
                conditions_met_previously = False

            if not check_trading_time():
                print("Outside trading hours. No trading actions will be taken.")
                time.sleep(30)
                continue

            positions = mt5.positions_get(magic=123456)
            if positions:
                pos = positions[0]
                if ticket is None or pos.ticket != ticket:
                    print("Position exists but no matching persisted order. Skipping trailing stop.")
                else:
                    new_partial, success = close_partial_and_move_stop(
                        ticket, entry_price, entry_atr_fixed, initial_sl_points,
                        partial_closed, ORDER_DETAILS_FILE
                    )
                    if success and new_partial != partial_closed:
                        partial_closed = new_partial
                    else:
                        tick = mt5.symbol_info_tick(SYMBOL)
                        if tick is not None:
                            current_price_pos = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask
                            update_trailing_stop(ticket, entry_price, current_price_pos, entry_atr_fixed, initial_sl_points)
            else:
                if ticket is not None:
                    print(f"Position closed, removing details")
                    logging.info("Position closed, removing details")
                    remove_order_details()
                    ticket = None
                    entry_price = None
                    entry_atr_fixed = None
                    initial_sl_points = None
                    direction = None
                    partial_closed = False
                    entry_time = None

            if not positions and all_conditions_met:
                print("*** BUY SIGNAL DETECTED ***")
                sl_price = current_price - atr_value * ATR_MULTIPLIER_SL
                tp_price = current_price + atr_value * 20
                new_ticket = place_order('buy', current_price, sl_price, tp_price)
                if new_ticket:
                    order_details = {
                        'ticket': new_ticket,
                        'direction': 'buy',
                        'entry_price': current_price,
                        'atr_fixed': atr_value,
                        'initial_sl_points': atr_value * ATR_MULTIPLIER_SL,
                        'entry_time': datetime.now().isoformat(),
                        'partial_closed': False
                    }
                    save_order_details(order_details)
                    ticket = new_ticket
                    entry_price = current_price
                    entry_atr_fixed = atr_value
                    initial_sl_points = atr_value * ATR_MULTIPLIER_SL
                    direction = 'buy'
                    partial_closed = False
                    entry_time = datetime.now().isoformat()
                    print("Order executed and details saved")
                    logging.info(f"Order placed with values: 1H EMA20={ema20_1h:.5f} EMA50={ema50_1h:.5f} Diff={diff_1h:.5f} | "
                                 f"15M EMA20={ema20_15m:.5f} EMA50={ema50_15m:.5f} | "
                                 f"MACD={macd_current:.5f} Signal={signal_current:.5f} | ATR={atr_value:.5f} | Price={current_price:.5f}")
                else:
                    print("Order execution failed")

            time.sleep(30)

        except Exception as e:
            print(f"Error in main loop: {e}")
            logging.exception("Main loop exception")
            time.sleep(10)

    mt5.shutdown()

if __name__ == "__main__":
    main()