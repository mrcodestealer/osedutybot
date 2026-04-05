import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
import json
import os

# ----------------------------- 配置 -----------------------------
#oanda_mt5_path = r"C:\Program Files\OANDA MetaTrader Second\terminal64.exe"
oanda_mt5_path = r'/Users/junchen/Library/Application Support/net.metaquotes.wine.metatrader5/drive_c/Program Files/MetaTrader 5/terminal64.exe'

server = "OANDA_Global-Demo-1"
login = 1715532098
password = "Jcsiah0318--=="

SYMBOL = "XAUUSD.sml"
VOLUME = 0.002
ATR_TIMEFRAME = mt5.TIMEFRAME_M15
ATR_PERIOD = 14
ATR_MULTIPLIER_SL = 2

EMA_FAST = 20
EMA_SLOW = 50

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

TRADE_START_HOUR = 16
TRADE_END_HOUR = 4

ORDER_DETAILS_FILE = "order_detailsGold.json"

STRUCTURE_LOOKBACK_HOURS = 12

logging.basicConfig(filename='strategy_logGold.log', level=logging.INFO,
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

def detect_market_structure(df, recent_bars=5, overall_bars=50):
    """
    返回：
    - curr_high, curr_low: 最近 recent_bars 根K线的高/低点
    - overall_high: 最近 overall_bars 根K线的最高点
    - prev_high: 最近 overall_bars 根K线中，排除最近 recent_bars 根后的最高点
    - prev_low: 最近 overall_bars 根K线中，排除最近 recent_bars 根后的最低点
    """
    high_series = df['high']
    low_series = df['low']
    if len(high_series) < overall_bars:
        return {
            'curr_high': None, 'curr_low': None,
            'overall_high': None,
            'prev_high': None, 'prev_low': None,
        }
    
    # 近期窗口（最近 recent_bars 根）
    curr_high = high_series.iloc[-recent_bars:].max()
    curr_low = low_series.iloc[-recent_bars:].min()
    
    # 整体窗口（最近 overall_bars 根）
    overall_high = high_series.iloc[-overall_bars:].max()
    
    # 前窗口（整体窗口中排除近期窗口的部分）
    if overall_bars > recent_bars:
        prev_high = high_series.iloc[-overall_bars:-recent_bars].max()
        prev_low = low_series.iloc[-overall_bars:-recent_bars].min()
    else:
        prev_high = None
        prev_low = None
    
    return {
        'curr_high': curr_high,
        'curr_low': curr_low,
        'overall_high': overall_high,
        'prev_high': prev_high,
        'prev_low': prev_low,
    }

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

# ---------- 新的基于 Higher Low 的移动止损函数 ----------
def update_stop_by_higher_low(ticket, entry_price, atr_fixed, initial_sl_points, current_price, df_5m):
    """
    在部分平仓后使用：当价格创出新高（盈利>=3R）并回调形成 higher low 时，
    将止损上移到最近20根5分钟K线最低价下方一个缓冲，且移动幅度至少0.5倍ATR。
    """
    order = mt5.positions_get(ticket=ticket)
    if not order:
        return False
    pos = order[0]
    current_sl = pos.sl
    profit_points = current_price - entry_price

    # 必须已经达到3R盈利，才考虑 higher low 移动（避免刚保本就频繁移动）
    if profit_points < 3 * initial_sl_points:
        return False

    # 计算最近20根5分钟K线的最低价
    recent_low = df_5m['low'].iloc[-20:].min()
    # 建议新止损 = 最近低点 - 0.2倍ATR（缓冲）
    new_sl = recent_low - 0.2 * atr_fixed
    # 新止损不能低于入场价
    if new_sl <= entry_price:
        return False
    # 只有当新止损比当前止损高出至少 0.5倍ATR 时才移动
    if new_sl > current_sl + 0.5 * atr_fixed:
        modify_order(ticket, new_sl)
        return True
    return False

def update_trailing_stop(ticket, entry_price, current_price, atr_fixed, initial_sl_points):
    """根据当前盈利更新移动止损（基于ATR阶梯）"""
    order = mt5.positions_get(ticket=ticket)
    if not order:
        return False
    order = order[0]
    direction = 'buy' if order.type == mt5.ORDER_TYPE_BUY else 'sell'
    if direction == 'buy':
        profit_points = current_price - entry_price
    else:
        profit_points = entry_price - current_price

    # 2R: 移到保本+缓冲
    if profit_points >= 2 * initial_sl_points:
        if direction == 'buy':
            new_sl = entry_price + 0.1 * atr_fixed
        else:
            new_sl = entry_price - 0.1 * atr_fixed
        if (direction == 'buy' and new_sl > order.sl) or (direction == 'sell' and new_sl < order.sl):
            modify_order(ticket, new_sl)
            return True

    # 3R: 移到1R
    if profit_points >= 3 * initial_sl_points:
        if direction == 'buy':
            new_sl = entry_price + initial_sl_points
        else:
            new_sl = entry_price - initial_sl_points
        if (direction == 'buy' and new_sl > order.sl) or (direction == 'sell' and new_sl < order.sl):
            modify_order(ticket, new_sl)
            return True

    # 4R: 移到2R
    if profit_points >= 4 * initial_sl_points:
        if direction == 'buy':
            new_sl = entry_price + 2 * initial_sl_points
        else:
            new_sl = entry_price - 2 * initial_sl_points
        if (direction == 'buy' and new_sl > order.sl) or (direction == 'sell' and new_sl < order.sl):
            modify_order(ticket, new_sl)
            return True

    return False

# ----------------------------- 持久化函数 -----------------------------
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

def find_peaks(series, order=1):
    """找到局部峰值（高点），返回按价格降序排序的列表"""
    peaks = []
    for i in range(order, len(series) - order):
        if all(series.iloc[i] >= series.iloc[i-j] for j in range(1, order+1)) and \
           all(series.iloc[i] >= series.iloc[i+j] for j in range(1, order+1)):
            peaks.append(series.iloc[i])
    return sorted(peaks, reverse=True)  # 从高到低排序

def find_troughs(series, order=2):
    """找到局部谷底（低点），返回按价格升序排序的列表"""
    troughs = []
    for i in range(order, len(series) - order):
        if all(series.iloc[i] <= series.iloc[i-j] for j in range(1, order+1)) and \
           all(series.iloc[i] <= series.iloc[i+j] for j in range(1, order+1)):
            troughs.append(series.iloc[i])
    return sorted(troughs)  # 从低到高排序

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

            # 显示信息（保持不变）
            print("\n" + "="*70)
            h1_signal = "(Buy signal)" if cond_1h_trend else ""
            print(f"1H {h1_signal:<12} EMA20={ema20_1h:.2f}  EMA50={ema50_1h:.2f}  Diff={diff_1h:.2f}")
            m15_signal = "(Buy signal)" if cond_15m_trend else ""
            print(f"15M {m15_signal:<12} EMA20={ema20_15m:.2f}  EMA50={ema50_15m:.2f}  Diff={ema20_15m - ema50_15m:.2f}")
            m5_signal = "(Buy signal)" if golden_cross else ""
            macd_diff = macd_current - signal_current
            print(f"5M  {m5_signal:<12} MACD={macd_current:.2f}  Signal={signal_current:.2f}  Prev MACD={macd_prev:.2f}  Prev Signal={signal_prev:.2f}  Diff={macd_diff:.2f}")
            print(f"ATR (15M) = {atr_value:.2f}")
            print(f"Current Price = {current_price:.2f}")
            trend_1h_str = "Bullish" if cond_1h_trend else "Neutral/Bearish"
            trend_15m_str = "Bullish" if cond_15m_trend else "Neutral/Bearish"
            print(f"Conditions: 1H Trend={trend_1h_str} 15M Trend={trend_15m_str} GoldenCross={golden_cross}")
            if cond_1h_trend and cond_15m_trend:
                potential_sl = current_price - atr_value * ATR_MULTIPLIER_SL
                print(f"If place order, SL will be {potential_sl:.2f}")
            else:
                print("If place order, SL will be ---")
            print("="*70)
            
                        # 设置回溯窗口（1小时图K线根数）
            lookback_hours = 15   # 可根据需要调整，确保不包含 4800.80
            high_series = df_1h['high'].iloc[-lookback_hours:]
            low_series = df_1h['low'].iloc[-lookback_hours:]
            
            # 使用局部峰值检测高点
            peaks = find_peaks(high_series, order=1)
            higher_high = peaks[0] if peaks else None
            previous_higher_high = None
            if higher_high is not None and len(peaks) > 1:
                # 设置最小差距（例如 0.5 倍 ATR 或固定点数 15）
                min_diff = max(atr_value * 0.8, 20)   # 将 0.5 提高到 0.8，固定点数提高到 20
                for p in peaks[1:]:
                    if higher_high - p >= min_diff:
                        previous_higher_high = p
                        break
                # 如果没找到，可以降级取第二高峰值
                if previous_higher_high is None and len(peaks) > 1:
                    previous_higher_high = peaks[1]
            
            # 当前低点（最近5根K线的最低价）
            curr_low = low_series.iloc[-5:].min()
            
            # 使用局部谷底检测低点（可选，保持原有逻辑或改用 troughs）
            troughs = find_troughs(low_series, order=1)
            # 取最低的谷底作为 Previous Higher low（您也可以保留原来的排序取最低）
            previous_higher_low = troughs[0] if len(troughs) > 0 else None
            
            print("Market Structure")
            print(f"Higher high : {higher_high:.2f}" if higher_high is not None else "Higher high : N/A")
            print(f"Higher low : {curr_low:.2f}")
            print(f"Previous Higher high : {previous_higher_high:.2f}" if previous_higher_high is not None else "Previous Higher high : N/A")
            print(f"Previous Higher low : {previous_higher_low:.2f}" if previous_higher_low is not None else "Previous Higher low : N/A")
            print("="*70)

            if ticket is not None and entry_price is not None:
                positions = mt5.positions_get(magic=123456)
                if positions:
                    pos = positions[0]
                    current_price_pos = pos.price_current
                    sl_display = f"{pos.sl:.2f}"
                else:
                    current_price_pos = current_price
                    sl_display = "N/A"
                profit_points = current_price_pos - entry_price
                profit_r = profit_points / initial_sl_points if initial_sl_points != 0 else 0
                print("\n" + "="*70)
                print("📊 **ACTIVE ORDER STATUS**")
                print(f"Ticket: {ticket}  |  Entry Time: {entry_time if entry_time else 'N/A'}")
                print(f"Entry Price: {entry_price:.2f}  |  Current Price: {current_price_pos:.2f}")
                print(f"ATR Fixed: {entry_atr_fixed:.2f}  |  Current SL: {sl_display}")
                print(f"Profit: {profit_points:.2f} points  |  R Multiple: {profit_r:.2f}R")
                if not partial_closed:
                    target_2r = entry_price + 2 * initial_sl_points
                    print(f"\n🔹 2R Target: {target_2r:.2f} (Profit = {2*initial_sl_points:.2f} points)")
                    print(f"   → When price reaches {target_2r:.2f}: close 50% position and move SL to {entry_price + 0.1 * entry_atr_fixed:.2f}")
                else:
                    print("\n✅ Already closed 50% at 2R. Remaining position now follows Higher-Low trailing stop.")
                print("="*70)

            all_conditions_met = cond_1h_trend and cond_15m_trend and golden_cross
            if all_conditions_met:
                if not conditions_met_previously:
                    potential_sl = current_price - atr_value * ATR_MULTIPLIER_SL
                    logging.info(f"ALL CONDITIONS MET at {now.strftime('%Y-%m-%d %H:%M:%S')} | "
                                 f"Price={current_price:.2f} | SL would be {potential_sl:.2f} | "
                                 f"1H Diff={diff_1h:.2f} | 15M Diff={ema20_15m - ema50_15m:.2f} | "
                                 f"MACD={macd_current:.2f} Signal={signal_current:.2f} | ATR={atr_value:.2f}")
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
                    # 先检查是否需要部分平仓（2R）
                    new_partial, success = close_partial_and_move_stop(
                        ticket, entry_price, entry_atr_fixed, initial_sl_points,
                        partial_closed, ORDER_DETAILS_FILE
                    )
                    if success and new_partial != partial_closed:
                        partial_closed = new_partial
                        # 部分平仓后，止损已经移动到保本+缓冲，跳过本次更新
                    else:
                        # 根据是否已经部分平仓选择不同的止损更新逻辑
                        tick = mt5.symbol_info_tick(SYMBOL)
                        if tick is not None:
                            current_price_pos = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask
                            if partial_closed:
                                # 已部分平仓 → 使用 Higher Low 移动止损
                                update_stop_by_higher_low(ticket, entry_price, entry_atr_fixed,
                                                          initial_sl_points, current_price_pos, df_5m)
                            else:
                                # 尚未部分平仓 → 使用原有的 ATR 阶梯止损
                                update_trailing_stop(ticket, entry_price, current_price_pos,
                                                     entry_atr_fixed, initial_sl_points)
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
                    logging.info(f"Order placed with values: 1H EMA20={ema20_1h:.2f} EMA50={ema50_1h:.2f} Diff={diff_1h:.2f} | "
                                 f"15M EMA20={ema20_15m:.2f} EMA50={ema50_15m:.2f} | "
                                 f"MACD={macd_current:.2f} Signal={signal_current:.2f} | ATR={atr_value:.2f} | Price={current_price:.2f}")
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