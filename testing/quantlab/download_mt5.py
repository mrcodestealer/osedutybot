"""Download all available M5 history for crypto symbols from the OANDA TMS
MT5 terminal and cache as engine-compatible .npz (plus per-bar spread)."""
import os
from datetime import datetime, timedelta, timezone

import MetaTrader5 as mt5
import numpy as np

DIR = os.path.dirname(os.path.abspath(__file__))
TERMINAL = r"C:\Program Files\OANDA TMS MT5 Terminal\terminal64.exe"

SYMBOLS = {"btc": "BTCUSD", "eth": "ETHUSD", "sol": "SOLUSD"}
START = datetime(2025, 1, 1, tzinfo=timezone.utc)


def fetch(symbol):
    assert mt5.symbol_select(symbol, True), f"cannot select {symbol}"
    chunks = []
    t = START
    now = datetime.now(timezone.utc)
    while t < now:
        t2 = min(t + timedelta(days=45), now)
        r = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_M5, t, t2)
        if r is not None and len(r):
            chunks.append(np.array(r))
        t = t2
    assert chunks, f"no data for {symbol}"
    allr = np.concatenate(chunks)
    _, idx = np.unique(allr["time"], return_index=True)
    allr = allr[np.sort(idx)]
    return allr


def main():
    assert mt5.initialize(TERMINAL), mt5.last_error()
    for key, symbol in SYMBOLS.items():
        r = fetch(symbol)
        ts = (r["time"].astype("int64") * 10**9)  # epoch s -> ns
        mid = (r["high"] + r["low"] + r["close"]) / 3
        info = mt5.symbol_info(symbol)
        point = info.point
        spread_bp = r["spread"].astype(np.float64) * point / r["close"] * 1e4
        out = os.path.join(DIR, f"data_{key}.npz")
        np.savez_compressed(
            out, ts=ts,
            open=r["open"].astype(np.float64), high=r["high"].astype(np.float64),
            low=r["low"].astype(np.float64), close=r["close"].astype(np.float64),
            vol=r["tick_volume"].astype(np.float64), spread_bp=spread_bp,
        )
        t0 = datetime.fromtimestamp(int(r["time"][0]), tz=timezone.utc)
        t1 = datetime.fromtimestamp(int(r["time"][-1]), tz=timezone.utc)
        print(f"{symbol}: {len(r)} bars  {t0:%Y-%m-%d} .. {t1:%Y-%m-%d}  "
              f"last close {r['close'][-1]:.2f}  "
              f"spread median {np.median(spread_bp):.2f}bp p75 {np.percentile(spread_bp,75):.2f}bp "
              f"-> {out}")
    mt5.shutdown()


if __name__ == "__main__":
    main()
