#!/usr/bin/env python3
"""
Download XAUUSD 5-minute OHLCV from MetaTrader 5.

Uses the same MT5 login / symbol as gold.py.
Output CSV matches 5mGoldDataSet.csv format (semicolon-separated).

Usage:
  python dataset.py
  python dataset.py --year 2026 --output 5mGoldDataSet_2026.csv
  python dataset.py --from 2026-01-01 --to 2026-06-02
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import MetaTrader5 as mt5
import pandas as pd

# Same credentials / symbol as gold.py
from gold import MT5_TERMINAL_PATH, SYMBOL, login, password, server

TIMEFRAME = mt5.TIMEFRAME_M5
DEFAULT_OUT_DIR = Path(__file__).resolve().parent


def initialize_mt5() -> bool:
    ok = mt5.initialize(MT5_TERMINAL_PATH) if MT5_TERMINAL_PATH else mt5.initialize()
    if not ok:
        print("Failed to initialize MetaTrader 5")
        if MT5_TERMINAL_PATH:
            print(f"  Path: {MT5_TERMINAL_PATH}")
        print("  Set MT5_TERMINAL_PATH or install MT5")
        return False
    if not mt5.login(login, password, server):
        print(f"Failed to login: {mt5.last_error()}")
        mt5.shutdown()
        return False
    print(f"Logged in — server={server} login={login}")
    return True


def fetch_rates(symbol: str, date_from: datetime, date_to: datetime) -> pd.DataFrame | None:
    if not mt5.symbol_select(symbol, True):
        print(f"Symbol {symbol} not available")
        return None

    rates = mt5.copy_rates_range(symbol, TIMEFRAME, date_from, date_to)
    if rates is None or len(rates) == 0:
        print(f"No data returned: {mt5.last_error()}")
        return None

    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    df = df.sort_values("time").drop_duplicates("time")
    return df


def to_dataset_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Match 5mGoldDataSet.csv: Date;Open;High;Low;Close;tick_volume"""
    out = pd.DataFrame({
        "Date": df["time"].dt.strftime("%Y.%m.%d %H:%M"),
        "Open": df["open"],
        "High": df["high"],
        "Low": df["low"],
        "Close": df["close"],
        "tick_volume": df["tick_volume"].astype(int),
    })
    return out


def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep=";", index=False)
    print(f"Saved {len(df):,} bars -> {path}")


def parse_args() -> argparse.Namespace:
    now = datetime.now()
    p = argparse.ArgumentParser(description="Download XAUUSD M5 data from MT5 (gold.py login)")
    p.add_argument("--symbol", default=SYMBOL, help=f"MT5 symbol (default: {SYMBOL})")
    p.add_argument("--year", type=int, default=now.year, help="Calendar year to download (default: this year)")
    p.add_argument("--from", dest="date_from", default=None, help="Start date YYYY-MM-DD (overrides --year)")
    p.add_argument("--to", dest="date_to", default=None, help="End date YYYY-MM-DD (default: now)")
    p.add_argument(
        "--output", "-o", default=None,
        help="Output CSV path (default: 5mGoldDataSet_<year>.csv in testing/)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    now = datetime.now()

    if args.date_from:
        date_from = datetime.strptime(args.date_from, "%Y-%m-%d")
    else:
        date_from = datetime(args.year, 1, 1)

    if args.date_to:
        date_to = datetime.strptime(args.date_to, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    else:
        date_to = now

    if date_from >= date_to:
        print("Invalid range: start must be before end")
        return 1

    out_path = Path(args.output) if args.output else DEFAULT_OUT_DIR / f"5mGoldDataSet_{args.year}.csv"

    print(f"Downloading {args.symbol} M5: {date_from.date()} -> {date_to.date()}")

    if not initialize_mt5():
        return 1

    try:
        raw = fetch_rates(args.symbol, date_from, date_to)
        if raw is None:
            return 1

        csv_df = to_dataset_csv(raw)
        save_csv(csv_df, out_path)

        print(f"Range: {csv_df['Date'].iloc[0]} -> {csv_df['Date'].iloc[-1]}")
        print(f"Rows: {len(csv_df):,}")
        return 0
    finally:
        mt5.shutdown()


if __name__ == "__main__":
    sys.exit(main())
