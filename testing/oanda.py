import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
import json
import os

# ----------------------------- 配置 -----------------------------
oanda_mt5_path = r"C:\Program Files\OANDA MetaTrader Second\terminal64.exe"
server = "OANDA_Global-Demo-1"
login = 1715532098
password = "Jcsiah0318--=="

SYMBOL = "XAUUSD.sml"          # 交易品种
VOLUME = 0.002                 # 固定手数
ATR_TIMEFRAME = mt5.TIMEFRAME_M15   # ATR使用15分钟图
ATR_PERIOD = 14                # ATR周期
ATR_MULTIPLIER_SL = 2          # 初始止损倍数

# 多周期均线参数
EMA_FAST = 20
EMA_SLOW = 50
EMA_DIFF_THRESHOLD = 18        # 1H均线差阈值（点数，黄金1点=0.01美元）

# MACD参数
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# 交易时间（UTC+8）
TRADE_START_HOUR = 16          # 下午4点
TRADE_END_HOUR = 4             # 凌晨4点（次日）

# 持久化文件
ORDER_DETAILS_FILE = "order_details.json"

# 日志 - 只记录订单和错误
logging.basicConfig(filename='strategy_log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# ----------------------------- 辅助函数 -----------------------------
def initialize_mt5():
    """初始化MT5连接"""
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
    """获取最近bars根K线数据"""
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, bars)
    if rates is None or len(rates) == 0:
        print(f"Failed to get rates for {symbol} {timeframe}")
        return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.set_index('time', inplace=True)
    return df

def calculate_ema(df, period):
    """计算EMA"""
    return df['close'].ewm(span=period, adjust=False).mean()

def calculate_macd(df, fast=12, slow=26, signal=9):
    """计算MACD线、信号线"""
    exp1 = df['close'].ewm(span=fast, adjust=False).mean()
    exp2 = df['close'].ewm(span=slow, adjust=False).mean()
    macd_line = exp1 - exp2
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line

def calculate_atr(df, period=ATR_PERIOD):
    """计算ATR"""
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
    """检查当前时间是否在允许交易时段（UTC+8）"""
    now = datetime.now()
    hour = now.hour
    if TRADE_START_HOUR <= hour < 24 or 0 <= hour < TRADE_END_HOUR:
        return True
    return False

def place_order(direction, price, sl_price, tp_price):
    """发送交易订单，返回订单号或None"""
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
    """修改订单止损"""
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
    """根据当前盈利更新移动止损"""
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
    """从文件加载订单详情，返回字典或None"""
    if not os.path.exists(ORDER_DETAILS_FILE):
        return None
    try:
        with open(ORDER_DETAILS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load order details: {e}")
        return None

def save_order_details(details):
    """保存订单详情到文件"""
    try:
        with open(ORDER_DETAILS_FILE, 'w') as f:
            json.dump(details, f, indent=4)
    except Exception as e:
        logging.error(f"Failed to save order details: {e}")

def remove_order_details():
    """删除订单详情文件"""
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

    # 持久化数据（如果有）
    persisted = load_order_details()
    ticket = None
    entry_price = None
    entry_atr_fixed = None
    initial_sl_points = None
    direction = None

    if persisted:
        pos = mt5.positions_get(ticket=persisted['ticket'])
        if pos:
            ticket = persisted['ticket']
            entry_price = persisted['entry_price']
            entry_atr_fixed = persisted['atr_fixed']
            initial_sl_points = persisted['initial_sl_points']
            direction = persisted['direction']
            print(f"Restored active order: {ticket} at {entry_price}")
            logging.info(f"Restored active order: {ticket} at {entry_price}")
        else:
            print(f"Order {persisted['ticket']} not found, removing details")
            logging.info(f"Order {persisted['ticket']} not found, removing details")
            remove_order_details()

    # --- Flag to track whether all conditions were already logged ---
    conditions_met_previously = False

    # 主循环
    while True:
        try:
            now = datetime.now()

            # 获取数据
            df_1h = get_rates(SYMBOL, mt5.TIMEFRAME_H1, 200)
            df_15m = get_rates(SYMBOL, mt5.TIMEFRAME_M15, 200)
            df_5m = get_rates(SYMBOL, mt5.TIMEFRAME_M5, 200)
            if df_1h is None or df_15m is None or df_5m is None:
                print("Failed to get data. Retrying...")
                time.sleep(5)
                continue

            # --- 计算所有指标 ---
            # 1H
            ema20_1h = calculate_ema(df_1h, EMA_FAST).iloc[-1]
            ema50_1h = calculate_ema(df_1h, EMA_SLOW).iloc[-1]
            diff_1h = ema20_1h - ema50_1h
            # 15M
            ema20_15m = calculate_ema(df_15m, EMA_FAST).iloc[-1]
            ema50_15m = calculate_ema(df_15m, EMA_SLOW).iloc[-1]
            # 5M MACD
            macd_line, signal_line = calculate_macd(df_5m, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
            macd_current = macd_line.iloc[-1]
            signal_current = signal_line.iloc[-1]
            macd_prev = macd_line.iloc[-2]
            signal_prev = signal_line.iloc[-2]
            # ATR (15M)
            atr_value = calculate_atr(df_15m)
            # 当前价格 (5M close)
            current_price = df_5m['close'].iloc[-1]

            # --- 条件判断 ---
            cond_1h_trend = ema20_1h > ema50_1h
            cond_1h_diff = diff_1h > EMA_DIFF_THRESHOLD
            cond_15m_trend = ema20_15m > ema50_15m
            golden_cross = (macd_current > signal_current) and (macd_prev <= signal_prev) and (macd_current > 0)

            # --- 显示信息到终端（网格视图）---
            print("\n" + "="*70)

            # 1H line
            if cond_1h_trend and cond_1h_diff:
                h1_signal = "(Buy signal)"
            else:
                h1_signal = ""
            print(f"1H {h1_signal:<12} EMA20={ema20_1h:.2f}  EMA50={ema50_1h:.2f}  Diff={diff_1h:.2f}  (Threshold=18)")

            # 15M line
            if cond_15m_trend:
                m15_signal = "(Buy signal)"
            else:
                m15_signal = ""
            print(f"15M {m15_signal:<12} EMA20={ema20_15m:.2f}  EMA50={ema50_15m:.2f}  Diff={ema20_15m - ema50_15m:.2f}")

            # 5M line (MACD)
            if golden_cross:
                m5_signal = "(Buy signal)"
            else:
                m5_signal = ""
            macd_diff = macd_current - signal_current
            print(f"5M  {m5_signal:<12} MACD={macd_current:.2f}  Signal={signal_current:.2f}  Prev MACD={macd_prev:.2f}  Prev Signal={signal_prev:.2f}  Diff={macd_diff:.2f}")

            # ATR and price
            print(f"ATR (15M) = {atr_value:.2f}")
            print(f"Current Price = {current_price:.2f}")

            # Conditions line
            trend_1h_str = "Bullish" if cond_1h_trend else "Neutral/Bearish"
            trend_15m_str = "Bullish" if cond_15m_trend else "Neutral/Bearish"
            print(f"Conditions: 1H Trend={trend_1h_str} 1H Diff>18={cond_1h_diff} 15M Trend={trend_15m_str} GoldenCross={golden_cross}")

            # If all trend conditions are met, show where SL would be placed
            if cond_1h_trend and cond_1h_diff and cond_15m_trend:
                potential_sl = current_price - atr_value * ATR_MULTIPLIER_SL
                print(f"If place order, SL will be {potential_sl:.2f}")
            else:
                print("If place order, SL will be ---")

            print("="*70)

            # --- Log when all conditions become true (runs every iteration, regardless of trading hours) ---
            all_conditions_met = cond_1h_trend and cond_1h_diff and cond_15m_trend and golden_cross
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

            # --- Trading hours check ---
            if not check_trading_time():
                print("Outside trading hours. No trading actions will be taken.")
                time.sleep(30)
                continue

            # --- Trading actions (only when in trading hours) ---
            # 检查当前持仓
            positions = mt5.positions_get(magic=123456)
            if positions:
                pos = positions[0]
                if ticket is None or pos.ticket != ticket:
                    print("Position exists but no matching persisted order. Skipping trailing stop.")
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

            # 如果没有持仓，尝试开新仓
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
                        'entry_time': datetime.now().isoformat()
                    }
                    save_order_details(order_details)
                    ticket = new_ticket
                    entry_price = current_price
                    entry_atr_fixed = atr_value
                    initial_sl_points = atr_value * ATR_MULTIPLIER_SL
                    direction = 'buy'
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