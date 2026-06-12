#!/usr/bin/env python3
"""
XAUUSD backtest — resample 5m CSV → 1H/15m (CSV unchanged).

Split (strict anti-overfit):
  • Development + walk-forward OOS: before UNSEEN_START
  • Unseen (evaluate once): UNSEEN_START → end of data

Default: all-market OOS selection (yearly stability + WF robustness + ≥10 tpm).
Strategies: donchian (trend), regime_adaptive / allweather (trend+range combo).
Exit: trailing stop + 50% partial @ 2R | 0.002 lots | $50
"""

from __future__ import annotations

import argparse
import itertools
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Literal

import numpy as np
import pandas as pd

DEFAULT_CSV = Path("/Users/junchen/Downloads/tradings/AI1/5mGoldDataSet.csv")
FALLBACK_CSV = Path(__file__).resolve().parent / "5mGoldDataSet.csv"

INITIAL_CAPITAL = 50.0
BASE_LOT = 0.002
MAX_LOT = 0.05
CONTRACT_SIZE = 100.0
SPREAD = 0.30

MIN_TRADES_PER_MONTH = 10
UNSEEN_START = "2025-01-01"  # true holdout — never used for selection

WF_TRAIN_MONTHS = 24
WF_TEST_MONTHS = 6
WF_STEP_MONTHS = 6
PURGE_DAYS = 5

SESSION_START_H = 7
SESSION_END_H = 21


@dataclass(frozen=True)
class StrategyParams:
    strategy: str = "gold_ema_macd"
    atr_mult_sl: float = 2.0
    partial_r: float = 2.0
    trail_atr_frac: float = 0.10
    cooldown_bars: int = 6
    ema_fast: int = 20
    ema_slow: int = 50
    vol_z: float = 1.0
    donchian: int = 20
    rsi_entry: float = 40.0
    adx_thresh: float = 22.0
    atr_pct_lo: float = 0.25
    atr_pct_hi: float = 0.80
    long_only: bool = True
    vote_k: int = 2  # ensemble: min number of agreeing base strategies
    risk_frac: float = 0.0  # >0 => size each trade to risk this fraction of equity (vol parity)
    trend_sma_days: int = 0  # >0 => only trade in direction of the daily N-day SMA trend (regime gate)


# Set after all-market OOS selection
DEFAULT_STRATEGY = StrategyParams(
    strategy="allweather",
    atr_mult_sl=2.5,
    partial_r=2.0,
    trail_atr_frac=0.10,
    cooldown_bars=2,
    donchian=15,
    rsi_entry=35,
    adx_thresh=18,
    long_only=False,
)


@dataclass
class Trade:
    direction: Literal["long", "short"]
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    entry_price: float
    exit_price: float
    lots: float
    pnl_usd: float
    r_multiple: float
    partial_closed: bool
    exit_reason: str


@dataclass
class BacktestResult:
    params: StrategyParams
    trades: list[Trade] = field(default_factory=list)
    equity_curve: pd.Series = field(default_factory=pd.Series)
    sharpe: float = 0.0
    total_return_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    trades_per_month: float = 0.0
    final_equity: float = INITIAL_CAPITAL


# ---------------------------------------------------------------------------
# Data (5m load → resample; never write CSV)
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    print(msg, flush=True)


def resolve_csv(path: str | None) -> Path:
    if path:
        p = Path(path)
        if p.exists():
            return p
        raise FileNotFoundError(p)
    return DEFAULT_CSV if DEFAULT_CSV.exists() else FALLBACK_CSV


def load_ohlcv_5m(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        csv_path, sep=";",
        names=["datetime", "open", "high", "low", "close", "tick_volume"],
        header=0,
    )
    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y.%m.%d %H:%M")
    df = df.sort_values("datetime").drop_duplicates("datetime").set_index("datetime")
    for c in ("open", "high", "low", "close", "tick_volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna()


def resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    return (
        df.resample(rule)
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "tick_volume": "sum"})
        .dropna()
    )


def ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([(h - l), (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def rsi(close: pd.Series, n: int = 14) -> pd.Series:
    d = close.diff()
    g = d.clip(lower=0).rolling(n).mean()
    l = (-d.clip(upper=0)).rolling(n).mean()
    return 100 - (100 / (1 + g / l.replace(0, np.nan)))


def macd(close: pd.Series, f: int = 12, s: int = 26, sig: int = 9) -> tuple[pd.Series, pd.Series]:
    m = ema(close, f) - ema(close, s)
    return m, m.ewm(span=sig, adjust=False).mean()


def adx(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l = df["high"], df["low"]
    c_prev = df["close"].shift()
    tr = pd.concat([(h - l), (h - c_prev).abs(), (l - c_prev).abs()], axis=1).max(axis=1)
    up, dn = h - h.shift(), l.shift() - l
    plus_dm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=df.index)
    atr_v = tr.rolling(n).mean().replace(0, np.nan)
    plus_di = 100 * plus_dm.rolling(n).mean() / atr_v
    minus_di = 100 * minus_dm.rolling(n).mean() / atr_v
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.rolling(n).mean()


def build_tf_bars(df5: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """1H signal/execution bars + 15m filter from 5m source."""
    h1 = resample_ohlcv(df5, "1h")
    m15 = resample_ohlcv(df5, "15min")
    h4 = resample_ohlcv(df5, "4h")

    for df in (h1, m15, h4):
        df["atr"] = atr(df)
        df["ema_f"] = ema(df["close"], 20)
        df["ema_s"] = ema(df["close"], 50)

    m, s = macd(h1["close"])
    h1["macd"], h1["macd_sig"] = m, s
    h1["macd_up"] = (m > s) & (m.shift(1) <= s.shift(1))
    h1["macd_dn"] = (m < s) & (m.shift(1) >= s.shift(1))
    h1["rsi"] = rsi(h1["close"])
    h1["adx"] = adx(h1)
    bb_m = h1["close"].rolling(20).mean()
    bb_s = h1["close"].rolling(20).std()
    h1["bb_upper"] = bb_m + 2 * bb_s
    h1["bb_lower"] = bb_m - 2 * bb_s

    vm = h1["tick_volume"].rolling(48).mean()
    vs = h1["tick_volume"].rolling(48).std()
    h1["vol_z"] = (h1["tick_volume"] - vm) / vs.replace(0, np.nan)

    atr_roll = h1["atr"].rolling(480, min_periods=80)
    h1["atr_pct"] = atr_roll.apply(lambda x: float((x[-1] >= x).mean()), raw=True)

    kc_mid = ema(h1["close"], 20)
    kc_w = 1.5 * h1["atr"]
    h1["kc_upper"] = kc_mid + kc_w
    h1["kc_lower"] = kc_mid - kc_w
    h1["ret_z"] = (
        h1["close"].pct_change() - h1["close"].pct_change().rolling(48).mean()
    ) / h1["close"].pct_change().rolling(48).std().replace(0, np.nan)

    h1["ema_f_15"] = m15["ema_f"].reindex(h1.index, method="ffill").shift(1)
    h1["ema_s_15"] = m15["ema_s"].reindex(h1.index, method="ffill").shift(1)
    h1["ema_f_4h"] = h4["ema_f"].reindex(h1.index, method="ffill").shift(1)
    h1["ema_s_4h"] = h4["ema_s"].reindex(h1.index, method="ffill").shift(1)

    return {"1h": h1.dropna(), "15m": m15, "4h": h4}


# ---------------------------------------------------------------------------
# Strategy signal generators (on 1H index)
# ---------------------------------------------------------------------------
def _session_ok(idx: pd.DatetimeIndex) -> pd.Series:
    return pd.Series((idx.hour >= SESSION_START_H) & (idx.hour < SESSION_END_H), index=idx)


def sig_gold_ema_macd(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    bull = (df["ema_f_4h"] > df["ema_s_4h"]) & (df["ema_f_15"] > df["ema_s_15"])
    bear = (df["ema_f_4h"] < df["ema_s_4h"]) & (df["ema_f_15"] < df["ema_s_15"])
    vol = df["vol_z"] >= p.vol_z
    long_s = bull & vol & df["macd_up"] & (df["macd"] > 0)
    short_s = bear & vol & df["macd_dn"] & (df["macd"] < 0)
    return long_s, short_s


def sig_ema_cross(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    ef = ema(df["close"], p.ema_fast)
    es = ema(df["close"], p.ema_slow)
    up = (ef > es) & (ef.shift(1) <= es.shift(1))
    dn = (ef < es) & (ef.shift(1) >= es.shift(1))
    trend_up = df["ema_f_4h"] > df["ema_s_4h"]
    trend_dn = df["ema_f_4h"] < df["ema_s_4h"]
    return up & trend_up, dn & trend_dn


def sig_donchian(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    n = p.donchian
    hi = df["high"].rolling(n).max().shift(1)
    lo = df["low"].rolling(n).min().shift(1)
    up = df["close"] > hi
    dn = df["close"] < lo
    trend_up = df["ema_f"] > df["ema_s"]
    trend_dn = df["ema_f"] < df["ema_s"]
    return up & trend_up, dn & trend_dn


def sig_rsi_pullback(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    up = df["ema_f"] > df["ema_s"]
    dn = df["ema_f"] < df["ema_s"]
    long_s = up & (df["rsi"].shift(1) < p.rsi_entry) & (df["rsi"] > p.rsi_entry + 5)
    short_s = dn & (df["rsi"].shift(1) > 100 - p.rsi_entry) & (df["rsi"] < 100 - p.rsi_entry - 5)
    return long_s, short_s


def sig_macd_trend(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    bull = df["ema_f_4h"] > df["ema_s_4h"]
    bear = df["ema_f_4h"] < df["ema_s_4h"]
    long_s = bull & df["macd_up"] & (df["macd"] > 0)
    short_s = bear & df["macd_dn"] & (df["macd"] < 0)
    return long_s, short_s


def sig_breakout_vol(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    vol = df["vol_z"] >= p.vol_z
    brk_up = df["close"] >= df["high"].rolling(8).max().shift(1)
    brk_dn = df["close"] <= df["low"].rolling(8).min().shift(1)
    up = df["ema_f_15"] > df["ema_s_15"]
    dn = df["ema_f_15"] < df["ema_s_15"]
    return vol & brk_up & up, vol & brk_dn & dn


def sig_regime_adaptive(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    """
    All-weather: trend mode (ADX high) → breakout/MACD with 4H bias;
    range mode (ADX low) → BB + RSI mean reversion. Long & short.
    """
    trending = df["adx"] >= p.adx_thresh
    ranging = ~trending
    bull4 = df["ema_f_4h"] > df["ema_s_4h"]
    bear4 = df["ema_f_4h"] < df["ema_s_4h"]
    bull1 = df["ema_f"] > df["ema_s"]
    bear1 = df["ema_f"] < df["ema_s"]

    hi = df["high"].rolling(p.donchian).max().shift(1)
    lo = df["low"].rolling(p.donchian).min().shift(1)

    long_trend = trending & bull4 & bull1 & (
        (df["close"] > hi) | (df["macd_up"] & (df["macd"] > 0))
    )
    short_trend = trending & bear4 & bear1 & (
        (df["close"] < lo) | (df["macd_dn"] & (df["macd"] < 0))
    )

    rsi_x_up = (df["rsi"].shift(1) < p.rsi_entry) & (df["rsi"] > p.rsi_entry)
    rsi_x_dn = (df["rsi"].shift(1) > 100 - p.rsi_entry) & (df["rsi"] < 100 - p.rsi_entry)
    long_range = ranging & (df["close"] <= df["bb_lower"]) & rsi_x_up
    short_range = ranging & (df["close"] >= df["bb_upper"]) & rsi_x_dn

    long_s = long_trend | long_range
    short_s = short_trend | short_range
    return long_s, short_s


def sig_allweather(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    """
    All-weather combo: Donchian breakout (trend) + BB/RSI mean reversion (range).
    Trend legs use 4H bias when ADX high; range legs when ADX low. Long & short.
    """
    trending = df["adx"] >= p.adx_thresh
    ranging = ~trending
    bull4 = df["ema_f_4h"] > df["ema_s_4h"]
    bear4 = df["ema_f_4h"] < df["ema_s_4h"]

    hi = df["high"].rolling(p.donchian).max().shift(1)
    lo = df["low"].rolling(p.donchian).min().shift(1)
    brk_up = df["close"] > hi
    brk_dn = df["close"] < lo

    long_trend = brk_up & (bull4 | trending)
    short_trend = brk_dn & (bear4 | trending)

    rsi_x_up = (df["rsi"].shift(1) < p.rsi_entry) & (df["rsi"] > p.rsi_entry)
    rsi_x_dn = (df["rsi"].shift(1) > 100 - p.rsi_entry) & (df["rsi"] < 100 - p.rsi_entry)
    long_range = ranging & (df["close"] <= df["bb_lower"]) & rsi_x_up
    short_range = ranging & (df["close"] >= df["bb_upper"]) & rsi_x_dn

    long_s = long_trend | long_range
    short_s = short_trend | short_range
    return long_s, short_s


def _vol_band(df: pd.DataFrame, p: StrategyParams) -> pd.Series:
    return (df["atr_pct"] >= p.atr_pct_lo) & (df["atr_pct"] <= p.atr_pct_hi)


def sig_vol_regime_momo(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    """4H trend + 1H momentum burst only in tradeable ATR percentile band."""
    vb = _vol_band(df, p)
    bull4 = df["ema_f_4h"] > df["ema_s_4h"]
    bear4 = df["ema_f_4h"] < df["ema_s_4h"]
    n = max(p.donchian, 8)
    mom_up = df["close"] > df["close"].shift(n)
    mom_dn = df["close"] < df["close"].shift(n)
    vol = df["vol_z"] >= p.vol_z
    long_s = vb & bull4 & mom_up & vol & (df["ema_f"] > df["ema_s"]) & (df["macd"] > df["macd_sig"])
    short_s = vb & bear4 & mom_dn & vol & (df["ema_f"] < df["ema_s"]) & (df["macd"] < df["macd_sig"])
    return long_s, short_s


def sig_keltner_pullback(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    """4H trend + prior bar touched Keltner band then closed back with 1H EMA."""
    bull4 = df["ema_f_4h"] > df["ema_s_4h"]
    bear4 = df["ema_f_4h"] < df["ema_s_4h"]
    touch_lo = df["low"].shift(1) <= df["kc_lower"].shift(1)
    touch_hi = df["high"].shift(1) >= df["kc_upper"].shift(1)
    long_s = bull4 & touch_lo & (df["close"] > df["ema_f"]) & (df["rsi"] > p.rsi_entry)
    short_s = bear4 & touch_hi & (df["close"] < df["ema_f"]) & (df["rsi"] < 100 - p.rsi_entry)
    return long_s, short_s


def sig_session_breakout(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    """London / NY active hours: Donchian break with 4H + vol band filter."""
    hour = df.index.hour
    active = ((hour >= 8) & (hour < 12)) | ((hour >= 13) & (hour < 17))
    vb = _vol_band(df, p)
    hi = df["high"].rolling(p.donchian).max().shift(1)
    lo = df["low"].rolling(p.donchian).min().shift(1)
    bull4 = df["ema_f_4h"] > df["ema_s_4h"]
    bear4 = df["ema_f_4h"] < df["ema_s_4h"]
    long_s = active & vb & bull4 & (df["close"] > hi) & (df["vol_z"] >= p.vol_z - 0.3)
    short_s = active & vb & bear4 & (df["close"] < lo) & (df["vol_z"] >= p.vol_z - 0.3)
    return long_s, short_s


def sig_consensus_trend(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    """Multi-filter agreement: 4H/1H/15m EMA stack + MACD + mild momentum."""
    bull = (
        (df["ema_f_4h"] > df["ema_s_4h"])
        & (df["ema_f"] > df["ema_s"])
        & (df["ema_f_15"] > df["ema_s_15"])
        & (df["macd"] > df["macd_sig"])
        & (df["ret_z"] > 0.3)
        & (df["rsi"] > p.rsi_entry)
        & (df["rsi"] < 72)
    )
    bear = (
        (df["ema_f_4h"] < df["ema_s_4h"])
        & (df["ema_f"] < df["ema_s"])
        & (df["ema_f_15"] < df["ema_s_15"])
        & (df["macd"] < df["macd_sig"])
        & (df["ret_z"] < -0.3)
        & (df["rsi"] < 100 - p.rsi_entry)
        & (df["rsi"] > 28)
    )
    vb = _vol_band(df, p)
    return bull & vb, bear & vb


def sig_mr_extreme(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    """Low ADX: fade ret_z extremes at BB; high ADX: skip (handled by low ADX only)."""
    calm = df["adx"] < p.adx_thresh
    long_s = calm & (df["ret_z"] < -p.vol_z) & (df["close"] <= df["bb_lower"]) & (df["rsi"] < p.rsi_entry)
    short_s = calm & (df["ret_z"] > p.vol_z) & (df["close"] >= df["bb_upper"]) & (df["rsi"] > 100 - p.rsi_entry)
    return long_s, short_s


STRATEGIES: dict[str, Callable[[pd.DataFrame, StrategyParams], tuple[pd.Series, pd.Series]]] = {
    "gold_ema_macd": sig_gold_ema_macd,
    "ema_cross_1h": sig_ema_cross,
    "donchian_break": sig_donchian,
    "rsi_pullback": sig_rsi_pullback,
    "macd_trend": sig_macd_trend,
    "breakout_vol": sig_breakout_vol,
    "regime_adaptive": sig_regime_adaptive,
    "allweather": sig_allweather,
    "vol_regime_momo": sig_vol_regime_momo,
    "keltner_pullback": sig_keltner_pullback,
    "session_breakout": sig_session_breakout,
    "consensus_trend": sig_consensus_trend,
    "mr_extreme": sig_mr_extreme,
}

# Base strategies that vote in the ensemble (everything except the ensemble itself).
ENSEMBLE_BASE = tuple(STRATEGIES.keys())


def sig_ensemble(df: pd.DataFrame, p: StrategyParams) -> tuple[pd.Series, pd.Series]:
    """
    Combine ALL base strategies via directional voting.

    Each base strategy casts a long and/or short vote per bar. We enter long when
    at least `vote_k` strategies vote long AND longs strictly outnumber shorts
    (and vice-versa for shorts). Higher vote_k => fewer, higher-conviction trades
    (higher Sharpe); lower vote_k => more trades. Only ~3 tuned params overall,
    so overfitting risk stays low while combining every edge in the book.
    """
    n = len(df)
    long_votes = np.zeros(n, dtype=np.int16)
    short_votes = np.zeros(n, dtype=np.int16)
    for name in ENSEMBLE_BASE:
        ls, ss = STRATEGIES[name](df, p)
        long_votes += ls.fillna(False).to_numpy().astype(np.int16)
        short_votes += ss.fillna(False).to_numpy().astype(np.int16)

    k = max(1, int(p.vote_k))
    long_arr = (long_votes >= k) & (long_votes > short_votes)
    short_arr = (short_votes >= k) & (short_votes > long_votes)
    return (
        pd.Series(long_arr, index=df.index),
        pd.Series(short_arr, index=df.index),
    )


STRATEGIES["ensemble"] = sig_ensemble


def build_signals(df: pd.DataFrame, p: StrategyParams) -> np.ndarray:
    fn = STRATEGIES[p.strategy]
    long_s, short_s = fn(df, p)

    # Daily-trend regime gate: only allow longs above / shorts below the daily
    # N-day SMA. Uses yesterday's daily close (shift 1 day) => no look-ahead.
    # Single robust parameter; kills counter-trend dip-buying in crashes (e.g. 2013).
    if p.trend_sma_days and p.trend_sma_days > 0:
        dc = df["close"].resample("1D").last().dropna()  # drop weekend/holiday gaps
        sma = dc.rolling(p.trend_sma_days).mean()
        up = (dc > sma).shift(1)
        dn = (dc < sma).shift(1)
        up_1h = up.reindex(df.index, method="ffill").fillna(False).to_numpy().astype(bool)
        dn_1h = dn.reindex(df.index, method="ffill").fillna(False).to_numpy().astype(bool)
        long_s = long_s & pd.Series(up_1h, index=df.index)
        short_s = short_s & pd.Series(dn_1h, index=df.index)

    sess = _session_ok(df.index)
    long_s = long_s & sess
    short_s = short_s & sess

    raw = np.zeros(len(df), dtype=int)
    raw[long_s.fillna(False).values] = 1
    if not p.long_only:
        raw[short_s.fillna(False).values & (raw == 0)] = -1

    out = np.zeros_like(raw)
    last = -p.cooldown_bars - 1
    for i, s in enumerate(raw):
        if s and i - last > p.cooldown_bars:
            out[i] = s
            last = i
    return out


# ---------------------------------------------------------------------------
# Execution on 1H bars
# ---------------------------------------------------------------------------
def pnl_usd(delta: float, lots: float, sign: int) -> float:
    return sign * delta * lots * CONTRACT_SIZE


def simulate_trade(
    bars: pd.DataFrame,
    direction: Literal["long", "short"],
    i0: int,
    entry: float,
    lots: float,
    atr_v: float,
    p: StrategyParams,
) -> tuple[list[tuple[pd.Timestamp, float, float, str]], int]:
    sign = 1 if direction == "long" else -1
    sl_dist = atr_v * p.atr_mult_sl
    if sl_dist <= 0:
        return [], i0

    sl = entry - sign * sl_dist
    rem, partial = lots, False
    chunks: list[tuple[pd.Timestamp, float, float, str]] = []
    peak_r = 0.0

    for i in range(i0 + 1, len(bars)):
        row = bars.iloc[i]
        t, hi, lo = bars.index[i], float(row["high"]), float(row["low"])
        profit = (hi - entry) if direction == "long" else (entry - lo)
        peak_r = max(peak_r, profit / sl_dist)

        if (lo <= sl) if direction == "long" else (hi >= sl):
            px = sl - SPREAD / 2 if direction == "long" else sl + SPREAD / 2
            d = (px - entry) if direction == "long" else (entry - px)
            chunks.append((t, px, pnl_usd(d, rem, sign), "trailing_sl"))
            return chunks, i

        if not partial and profit >= p.partial_r * sl_dist:
            half = round(rem / 2, 4)
            if half > 0:
                px = (
                    entry + p.partial_r * sl_dist - SPREAD / 2
                    if direction == "long"
                    else entry - p.partial_r * sl_dist + SPREAD / 2
                )
                d = (px - entry) if direction == "long" else (entry - px)
                chunks.append((t, px, pnl_usd(d, half, sign), "partial_50pct"))
                rem -= half
                partial = True
                sl = entry + sign * p.trail_atr_frac * atr_v

        if not partial:
            r = profit / sl_dist
            ns = None
            if r >= 4:
                ns = entry + sign * 2 * sl_dist
            elif r >= 3:
                ns = entry + sign * sl_dist
            elif r >= 2:
                ns = entry + sign * p.trail_atr_frac * atr_v
            if ns is not None:
                if direction == "long" and ns > sl:
                    sl = ns
                elif direction == "short" and ns < sl:
                    sl = ns
        elif peak_r >= 3:
            w = bars.iloc[max(0, i - 5) : i + 1]
            if direction == "long":
                nl = float(w["low"].min()) - 0.2 * atr_v
                if nl > entry and nl > sl:
                    sl = nl
            else:
                nh = float(w["high"].max()) + 0.2 * atr_v
                if nh < entry and nh < sl:
                    sl = nh

        if i - i0 >= 72:
            px = float(row["close"])
            px = px - SPREAD / 2 if direction == "long" else px + SPREAD / 2
            d = (px - entry) if direction == "long" else (entry - px)
            chunks.append((t, px, pnl_usd(d, rem, sign), "time_stop"))
            return chunks, i

    last = bars.iloc[-1]
    px = float(last["close"])
    px = px - SPREAD / 2 if direction == "long" else px + SPREAD / 2
    d = (px - entry) if direction == "long" else (entry - px)
    chunks.append((bars.index[-1], px, pnl_usd(d, rem, sign), "eod"))
    return chunks, len(bars) - 1


def run_backtest(
    bars: pd.DataFrame,
    p: StrategyParams,
    start: pd.Timestamp,
    end: pd.Timestamp,
    sigs: np.ndarray | None = None,
) -> BacktestResult:
    data = bars[(bars.index >= start) & (bars.index <= end)]
    if len(data) < 50:
        return BacktestResult(params=p)

    if sigs is None:
        full = build_signals(bars, p)
        sigs = full[(bars.index >= start) & (bars.index <= end)]

    trades: list[Trade] = []
    equity = INITIAL_CAPITAL
    eq_pts: list[tuple[pd.Timestamp, float]] = [(data.index[0], equity)]
    idx = list(data.index)
    i, n = 0, len(data)

    while i < n:
        s = int(sigs[i])
        if s == 0:
            i += 1
            continue
        row = data.iloc[i]
        atr_v = float(row["atr"])
        if not np.isfinite(atr_v) or atr_v <= 0:
            i += 1
            continue

        # Risk-based sizing: keep $ risk ≈ constant fraction of equity across all
        # vol regimes (critical for Sharpe on a 20yr span where gold ATR grows ~20x).
        if p.risk_frac and p.risk_frac > 0.0:
            sl_dist = atr_v * p.atr_mult_sl
            raw = (p.risk_frac * equity) / (sl_dist * CONTRACT_SIZE) if sl_dist > 0 else 0.0
            lots = float(min(MAX_LOT, max(1e-4, raw)))
        else:
            lots = BASE_LOT
        dirc: Literal["long", "short"] = "long" if s > 0 else "short"
        entry = float(row["close"]) + (SPREAD / 2 if dirc == "long" else -SPREAD / 2)
        chunks, ei = simulate_trade(data, dirc, i, entry, lots, atr_v, p)
        if not chunks:
            i += 1
            continue

        total = sum(c[2] for c in chunks)
        sl_d = atr_v * p.atr_mult_sl
        trades.append(
            Trade(
                direction=dirc,
                entry_time=idx[i],
                exit_time=chunks[-1][0],
                entry_price=entry,
                exit_price=chunks[-1][1],
                lots=lots,
                pnl_usd=total,
                r_multiple=total / (sl_d * lots * CONTRACT_SIZE) if sl_d else 0,
                partial_closed=any(c[3] == "partial_50pct" for c in chunks),
                exit_reason=chunks[-1][3],
            )
        )
        equity = max(5.0, equity + total)
        eq_pts.append((chunks[-1][0], equity))
        i = ei + 1

    months = max((end - start).days / 30.44, 1 / 30.44)
    tpm = len(trades) / months
    if not eq_pts:
        return BacktestResult(params=p, trades_per_month=tpm)

    eq = pd.Series([e for _, e in eq_pts], index=pd.DatetimeIndex([t for t, _ in eq_pts])).sort_index()
    daily = eq.resample("1D").last().ffill().dropna()
    dr = daily.pct_change().dropna()
    sharpe = float(dr.mean() / dr.std() * np.sqrt(252)) if len(dr) > 1 and dr.std() > 0 else 0.0
    peak = daily.cummax()
    max_dd = float(((daily - peak) / peak.replace(0, np.nan)).min() * 100) if len(daily) else 0.0
    ret = (daily.iloc[-1] - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
    wins = [t.pnl_usd for t in trades if t.pnl_usd > 0]
    loss = [abs(t.pnl_usd) for t in trades if t.pnl_usd < 0]
    pf = sum(wins) / sum(loss) if loss and sum(loss) > 0 else (999.0 if wins else 0.0)

    return BacktestResult(
        params=p,
        trades=trades,
        equity_curve=daily,
        sharpe=sharpe,
        total_return_pct=ret,
        max_drawdown_pct=max_dd,
        win_rate=sum(1 for t in trades if t.pnl_usd > 0) / len(trades) if trades else 0,
        profit_factor=pf,
        trades_per_month=tpm,
        final_equity=float(daily.iloc[-1]),
    )


def param_combos(only: frozenset[str] | None = None) -> list[StrategyParams]:
    combos: list[StrategyParams] = []
    grids = {
        "gold_ema_macd": {
            "atr_mult_sl": [1.5, 2.0, 2.5],
            "vol_z": [0.8, 1.0, 1.2],
            "cooldown_bars": [4, 6, 8],
            "long_only": [True, False],
        },
        "ema_cross_1h": {
            "atr_mult_sl": [1.5, 2.0, 2.5],
            "ema_fast": [12, 20],
            "ema_slow": [26, 50],
            "cooldown_bars": [4, 6, 8],
            "long_only": [True, False],
        },
        "donchian_break": {
            "atr_mult_sl": [1.5, 2.0],
            "donchian": [15, 20, 30],
            "cooldown_bars": [4, 6],
            "long_only": [True, False],
        },
        "rsi_pullback": {
            "atr_mult_sl": [1.5, 2.0, 2.5],
            "rsi_entry": [35, 40, 45],
            "cooldown_bars": [3, 5, 7],
            "long_only": [True],
        },
        "macd_trend": {
            "atr_mult_sl": [1.5, 2.0, 2.5],
            "cooldown_bars": [4, 6, 8],
            "long_only": [True, False],
        },
        "breakout_vol": {
            "atr_mult_sl": [1.5, 2.0],
            "vol_z": [0.8, 1.0, 1.2],
            "cooldown_bars": [3, 5],
            "long_only": [True, False],
        },
        "regime_adaptive": {
            "atr_mult_sl": [1.5, 2.0, 2.5],
            "adx_thresh": [18, 22, 26],
            "donchian": [12, 15, 20],
            "rsi_entry": [35, 40],
            "cooldown_bars": [3, 4, 6],
            "long_only": [False],
        },
        "allweather": {
            "atr_mult_sl": [1.5, 2.0, 2.5],
            "adx_thresh": [18, 22, 26],
            "donchian": [12, 15, 20],
            "rsi_entry": [35, 40],
            "cooldown_bars": [2, 3, 4],
            "long_only": [False],
        },
        "vol_regime_momo": {
            "atr_mult_sl": [1.5, 2.0, 2.5],
            "vol_z": [0.5, 0.8, 1.0],
            "donchian": [6, 10, 14],
            "atr_pct_lo": [0.20, 0.30],
            "atr_pct_hi": [0.75, 0.85],
            "cooldown_bars": [2, 4],
            "long_only": [False],
        },
        "keltner_pullback": {
            "atr_mult_sl": [1.5, 2.0, 2.5],
            "rsi_entry": [38, 42, 46],
            "cooldown_bars": [3, 5, 7],
            "long_only": [False],
        },
        "session_breakout": {
            "atr_mult_sl": [1.5, 2.0],
            "donchian": [10, 15, 20],
            "vol_z": [0.3, 0.6],
            "atr_pct_lo": [0.20, 0.30],
            "atr_pct_hi": [0.80, 0.90],
            "cooldown_bars": [2, 4],
            "long_only": [False],
        },
        "consensus_trend": {
            "atr_mult_sl": [1.5, 2.0, 2.5],
            "rsi_entry": [40, 45],
            "atr_pct_lo": [0.15, 0.25],
            "atr_pct_hi": [0.80, 0.90],
            "cooldown_bars": [4, 6, 8],
            "long_only": [False],
        },
        "mr_extreme": {
            "atr_mult_sl": [1.5, 2.0],
            "adx_thresh": [18, 22, 26],
            "vol_z": [1.0, 1.5, 2.0],
            "rsi_entry": [32, 38],
            "cooldown_bars": [2, 3, 4],
            "long_only": [False],
        },
        "ensemble": {
            "vote_k": [2, 3, 4, 5],
            "atr_mult_sl": [1.5, 2.0, 2.5],
            "cooldown_bars": [1, 2, 3],
            "long_only": [False],
        },
    }
    for strat, g in grids.items():
        if only is not None and strat not in only:
            continue
        keys = list(g.keys())
        for vals in itertools.product(*g.values()):
            kw = dict(zip(keys, vals))
            combos.append(StrategyParams(strategy=strat, partial_r=2.0, trail_atr_frac=0.10, **kw))
    return combos


STABLE_STRATEGIES = frozenset({
    "vol_regime_momo",
    "keltner_pullback",
    "session_breakout",
    "consensus_trend",
    "mr_extreme",
    "allweather",
})


def evaluate_stable_oos(folds: list[tuple], bars: pd.DataFrame, p: StrategyParams) -> OOSMetrics | None:
    """Stable profit: OOS chained > 0, robust Sharpe > 0, ≥10 tpm, ≥48% profitable folds."""
    m = evaluate_oos_loose(folds, bars, p)
    if m is None:
        return None
    if m.mean_tpm < MIN_TRADES_PER_MONTH:
        return None
    if m.chained_oos_return_pct <= 0 or m.robust_sharpe <= 0 or m.mean_sharpe <= 0:
        return None
    if m.pct_profitable_folds < 0.48:
        return None
    m.allmarket_score = (
        0.45 * m.robust_sharpe
        + 0.35 * min(m.chained_oos_return_pct / 50.0, 2.0)
        + 0.20 * m.pct_profitable_folds
    )
    return m


def search_stable_profit(bars: pd.DataFrame, folds: list[tuple], dev_end: pd.Timestamp) -> list[OOSMetrics]:
    combos = param_combos(only=STABLE_STRATEGIES)
    log(f"Stable-profit search (custom strategies): {len(combos)} configs")
    log("  Filter: OOS chained>0, robust Sharpe>0, ≥10 tpm, ≥48% profitable WF folds")
    results: list[OOSMetrics] = []
    for n, p in enumerate(combos, 1):
        m = evaluate_stable_oos(folds, bars, p)
        if m:
            ysh, yret = yearly_performance(bars, p, dev_end)
            if len(ysh) >= 5:
                m.min_yearly_sharpe = float(np.min(ysh))
                m.pct_positive_years = float(np.mean([x > 0 for x in yret]))
            results.append(m)
        if n % 60 == 0:
            log(f"  ... {n}/{len(combos)} | stable qualified: {len(results)}")
    results.sort(key=lambda x: x.allmarket_score, reverse=True)
    log(f"Stable-profit qualified: {len(results)}/{len(combos)}")
    if results:
        b = results[0]
        log(
            f"Best stable: {b.params.strategy} score={b.allmarket_score:.3f} | "
            f"OOS chained={b.chained_oos_return_pct:+.1f}% robustSh={b.robust_sharpe:+.2f} | "
            f"tpm={b.mean_tpm:.1f} posFolds={b.pct_profitable_folds*100:.0f}%"
        )
    return results


def wf_folds(t0: pd.Timestamp, t1: pd.Timestamp, holdout: pd.Timestamp) -> list[tuple]:
    folds, cur, fid = [], t0, 0
    while True:
        tr_end = cur + pd.DateOffset(months=WF_TRAIN_MONTHS)
        te0 = tr_end + pd.Timedelta(days=PURGE_DAYS)
        te1 = te0 + pd.DateOffset(months=WF_TEST_MONTHS)
        if te1 > holdout or te0 >= t1:
            break
        te1 = min(te1, holdout - pd.Timedelta(days=1))
        folds.append((fid, cur, tr_end, te0, te1))
        fid += 1
        cur += pd.DateOffset(months=WF_STEP_MONTHS)
    return folds


@dataclass
class OOSMetrics:
    params: StrategyParams
    mean_sharpe: float
    std_sharpe: float
    robust_sharpe: float
    mean_tpm: float
    min_tpm: float
    mean_fold_return_pct: float
    chained_oos_return_pct: float
    mean_max_dd_pct: float
    worst_max_dd_pct: float
    pct_profitable_folds: float
    n_folds: int
    min_yearly_sharpe: float = 0.0
    pct_positive_years: float = 0.0
    allmarket_score: float = 0.0


def yearly_performance(bars: pd.DataFrame, p: StrategyParams, dev_end: pd.Timestamp) -> tuple[list[float], list[float]]:
    sig = build_signals(bars, p)
    sharpes, rets = [], []
    for year in range(bars.index[0].year, dev_end.year + 1):
        ys = pd.Timestamp(f"{year}-01-01")
        ye = min(pd.Timestamp(f"{year}-12-31 23:59:59"), dev_end)
        if ys > dev_end or ye < bars.index[0]:
            continue
        ys = max(ys, bars.index[0])
        mask = (bars.index >= ys) & (bars.index <= ye)
        if mask.sum() < 80:
            continue
        r = run_backtest(bars, p, ys, ye, sigs=sig[mask])
        if len(r.trades) < 8:
            continue
        sharpes.append(r.sharpe)
        rets.append(r.total_return_pct)
    return sharpes, rets


def evaluate_allmarket(
    folds: list[tuple], bars: pd.DataFrame, p: StrategyParams, dev_end: pd.Timestamp
) -> OOSMetrics | None:
    m = evaluate_oos_loose(folds, bars, p)
    if m is None:
        return None
    ysh, yret = yearly_performance(bars, p, dev_end)
    if len(ysh) < 8:
        return None
    min_ysh = float(np.min(ysh))
    pct_pos_y = float(np.mean([x > 0 for x in yret]))
    if pct_pos_y < 0.42:
        return None

    score = (
        0.30 * min_ysh
        + 0.30 * m.robust_sharpe
        + 0.25 * pct_pos_y
        + 0.10 * (1.0 if m.chained_oos_return_pct > 0 else -1.0)
        + 0.05 * (m.worst_max_dd_pct / 100.0)
    )
    m.min_yearly_sharpe = min_ysh
    m.pct_positive_years = pct_pos_y
    m.allmarket_score = score
    return m


def search_allmarket(bars: pd.DataFrame, folds: list[tuple], dev_end: pd.Timestamp) -> list[OOSMetrics]:
    combos = param_combos()
    log(f"All-market search: {len(combos)} configs | ≥{MIN_TRADES_PER_MONTH} tpm | ≥42% positive years")
    results: list[OOSMetrics] = []
    for n, p in enumerate(combos, 1):
        m = evaluate_allmarket(folds, bars, p, dev_end)
        if m:
            results.append(m)
        if n % 100 == 0:
            log(f"  ... {n}/{len(combos)} | qualified: {len(results)}")
    results.sort(key=lambda x: x.allmarket_score, reverse=True)
    log(f"All-market qualified: {len(results)}/{len(combos)}")
    if results:
        b = results[0]
        log(
            f"Best all-market: {b.params.strategy} score={b.allmarket_score:.3f} | "
            f"minYearSh={b.min_yearly_sharpe:+.2f} robustSh={b.robust_sharpe:+.2f} | "
            f"posYears={b.pct_positive_years*100:.0f}% OOS chained={b.chained_oos_return_pct:+.1f}%"
        )
    return results


def evaluate_ensemble(
    folds: list[tuple], bars: pd.DataFrame, p: StrategyParams, dev_end: pd.Timestamp, min_tpm: float
) -> OOSMetrics | None:
    """Score ensemble configs for high *robust* Sharpe with time-stability (anti-overfit)."""
    m = evaluate_oos_loose(folds, bars, p)
    if m is None or m.mean_tpm < min_tpm:
        return None
    ysh, yret = yearly_performance(bars, p, dev_end)
    if len(ysh) < 8:
        return None
    min_ysh = float(np.min(ysh))
    pct_pos_y = float(np.mean([x > 0 for x in yret]))
    # Sharpe-first score; reward time stability, penalise frequency starvation lightly.
    m.min_yearly_sharpe = min_ysh
    m.pct_positive_years = pct_pos_y
    m.allmarket_score = (
        0.50 * m.robust_sharpe
        + 0.25 * min_ysh
        + 0.20 * pct_pos_y
        + 0.05 * (1.0 if m.chained_oos_return_pct > 0 else -1.0)
    )
    return m


def search_ensemble(
    bars: pd.DataFrame, folds: list[tuple], dev_end: pd.Timestamp, min_tpm: float
) -> list[OOSMetrics]:
    combos = param_combos(only=frozenset({"ensemble"}))
    log(f"Ensemble search: {len(combos)} vote/SL/cooldown configs combining {len(ENSEMBLE_BASE)} strategies")
    log(f"  Filter: ≥{min_tpm:.0f} tpm | ≥8 evaluable years | maximise robust OOS Sharpe + time stability")
    results: list[OOSMetrics] = []
    for n, p in enumerate(combos, 1):
        m = evaluate_ensemble(folds, bars, p, dev_end, min_tpm)
        if m:
            results.append(m)
        log(
            f"  [{n:>2}/{len(combos)}] vote_k={p.vote_k} sl={p.atr_mult_sl} cd={p.cooldown_bars} "
            + (
                f"-> robustSh={m.robust_sharpe:+.2f} tpm={m.mean_tpm:.1f} minYSh={m.min_yearly_sharpe:+.2f} "
                f"posY={m.pct_positive_years*100:.0f}% score={m.allmarket_score:.3f}"
                if m
                else "-> rejected (tpm/years filter)"
            )
        )
    results.sort(key=lambda x: x.allmarket_score, reverse=True)
    log(f"Ensemble qualified: {len(results)}/{len(combos)}")
    return results


# ---------------------------------------------------------------------------
# Portfolio combine: run each strategy as a sleeve, equal-weight the return
# streams. Diversification across low-correlation sleeves raises Sharpe and
# smooths drawdown; equal weighting (no fitted weights) keeps it anti-overfit.
# ---------------------------------------------------------------------------
TRADING_DAYS = 252.0


def _sharpe(rets: pd.Series) -> float:
    if rets is None or len(rets) < 2 or rets.std() == 0:
        return 0.0
    return float(rets.mean() / rets.std() * np.sqrt(TRADING_DAYS))


def sleeve_daily_returns(bars: pd.DataFrame, p: StrategyParams, start, end) -> pd.Series | None:
    """Daily return series of a single strategy sleeve over [start, end]."""
    r = run_backtest(bars, p, start, end)
    if len(r.equity_curve) < 3 or len(r.trades) == 0:
        return None
    return r.equity_curve.pct_change().fillna(0.0)


def portfolio_daily_returns(bars: pd.DataFrame, sleeves: list[StrategyParams], start, end) -> pd.Series | None:
    """Equal-weight (daily-rebalanced) portfolio of sleeve return streams."""
    cols = []
    for p in sleeves:
        dr = sleeve_daily_returns(bars, p, start, end)
        if dr is not None:
            cols.append(dr)
    if not cols:
        return None
    mat = pd.concat(cols, axis=1).fillna(0.0)
    return mat.mean(axis=1)


def portfolio_metrics(bars: pd.DataFrame, sleeves: list[StrategyParams], start, end) -> dict:
    rets = portfolio_daily_returns(bars, sleeves, start, end)
    if rets is None or len(rets) < 2:
        return {"sharpe": 0.0, "return_pct": 0.0, "max_dd_pct": 0.0, "trades": 0, "tpm": 0.0}
    eq = (1 + rets).cumprod()
    peak = eq.cummax()
    max_dd = float(((eq - peak) / peak).min() * 100)
    total_trades = 0
    for p in sleeves:
        r = run_backtest(bars, p, start, end)
        total_trades += len(r.trades)
    months = max((pd.Timestamp(end) - pd.Timestamp(start)).days / 30.44, 1e-9)
    return {
        "sharpe": _sharpe(rets),
        "return_pct": float((eq.iloc[-1] - 1) * 100),
        "max_dd_pct": max_dd,
        "trades": total_trades,
        "tpm": total_trades / months,
    }


@dataclass
class SleeveOOS:
    params: StrategyParams
    fold_sharpes: list[float]
    mean_sharpe: float
    robust_sharpe: float
    mean_tpm: float
    pct_pos_years: float
    min_year_sharpe: float


def evaluate_sleeve_oos(folds: list[tuple], bars: pd.DataFrame, p: StrategyParams, dev_end: pd.Timestamp) -> SleeveOOS | None:
    """Robustness of one sleeve across WF OOS folds + yearly stability (no unseen)."""
    sig = build_signals(bars, p)
    fold_sh, tpms = [], []
    for _, _, _, te0, te1 in folds:
        mask = (bars.index >= te0) & (bars.index <= te1)
        r = run_backtest(bars, p, te0, te1, sigs=sig[mask])
        fold_sh.append(r.sharpe)
        tpms.append(r.trades_per_month)
    if not fold_sh:
        return None
    ysh, yret = yearly_performance(bars, p, dev_end)
    if len(ysh) < 6:
        return None
    return SleeveOOS(
        params=p,
        fold_sharpes=fold_sh,
        mean_sharpe=float(np.mean(fold_sh)),
        robust_sharpe=float(np.mean(fold_sh) - 0.5 * np.std(fold_sh)),
        mean_tpm=float(np.mean(tpms)),
        pct_pos_years=float(np.mean([x > 0 for x in yret])),
        min_year_sharpe=float(np.min(ysh)),
    )


def best_config_per_strategy(
    bars: pd.DataFrame, folds: list[tuple], dev_end: pd.Timestamp, risk_frac: float, tune: bool,
    trend_sma: int = 0,
) -> dict[str, SleeveOOS]:
    """For each base strategy, pick the most robust (WF-OOS) config as its sleeve."""
    best: dict[str, SleeveOOS] = {}
    if tune:
        combos = [p for p in param_combos() if p.strategy != "ensemble"]
        groups: dict[str, list[StrategyParams]] = {}
        for p in combos:
            groups.setdefault(p.strategy, []).append(p)
    else:
        groups = {
            s: [StrategyParams(strategy=s, long_only=False)]
            for s in STRATEGIES
            if s != "ensemble"
        }
    for strat, plist in groups.items():
        cand: SleeveOOS | None = None
        for p in plist:
            p = StrategyParams(**{**vars(p), "risk_frac": risk_frac, "trend_sma_days": trend_sma})
            m = evaluate_sleeve_oos(folds, bars, p, dev_end)
            if m is None:
                continue
            if cand is None or m.robust_sharpe > cand.robust_sharpe:
                cand = m
        if cand is not None:
            best[strat] = cand
            log(
                f"  sleeve {strat:16s} robustSh={cand.robust_sharpe:+.2f} meanSh={cand.mean_sharpe:+.2f} "
                f"tpm={cand.mean_tpm:5.1f} posY={cand.pct_pos_years*100:3.0f}% minYSh={cand.min_year_sharpe:+.2f}"
            )
    return best


def chain_oos_return(folds: list[tuple], bars: pd.DataFrame, p: StrategyParams) -> float:
    sig_full = build_signals(bars, p)
    equity = INITIAL_CAPITAL
    for _, _, _, te0, te1 in folds:
        mask = (bars.index >= te0) & (bars.index <= te1)
        r = run_backtest(bars, p, te0, te1, sigs=sig_full[mask])
        equity *= 1 + r.total_return_pct / 100
    return (equity / INITIAL_CAPITAL - 1) * 100


def evaluate_oos(folds: list[tuple], bars: pd.DataFrame, p: StrategyParams) -> OOSMetrics | None:
    sig_full = build_signals(bars, p)
    sharpes, tpms, rets, dds = [], [], [], []
    for _, _, _, te0, te1 in folds:
        mask = (bars.index >= te0) & (bars.index <= te1)
        r = run_backtest(bars, p, te0, te1, sigs=sig_full[mask])
        sharpes.append(r.sharpe)
        tpms.append(r.trades_per_month)
        rets.append(r.total_return_pct)
        dds.append(r.max_drawdown_pct)

    if not sharpes:
        return None

    mean_tpm = float(np.mean(tpms))
    if mean_tpm < MIN_TRADES_PER_MONTH:
        return None

    mean_sh = float(np.mean(sharpes))
    std_sh = float(np.std(sharpes))
    pct_pos = float(np.mean([x > 0 for x in rets]))
    if pct_pos < 0.45:
        return None

    chained = chain_oos_return(folds, bars, p)
    if chained <= 0 or mean_sh < 0:
        return None

    return OOSMetrics(
        params=p,
        mean_sharpe=mean_sh,
        std_sharpe=std_sh,
        robust_sharpe=mean_sh - 0.5 * std_sh,
        mean_tpm=mean_tpm,
        min_tpm=float(np.min(tpms)),
        mean_fold_return_pct=float(np.mean(rets)),
        chained_oos_return_pct=chained,
        mean_max_dd_pct=float(np.mean(dds)),
        worst_max_dd_pct=float(np.min(dds)),
        pct_profitable_folds=pct_pos,
        n_folds=len(folds),
    )


def evaluate_oos_loose(folds: list[tuple], bars: pd.DataFrame, p: StrategyParams) -> OOSMetrics | None:
    """Only tpm filter — for reporting when strict filter empty."""
    sig_full = build_signals(bars, p)
    sharpes, tpms, rets, dds = [], [], [], []
    for _, _, _, te0, te1 in folds:
        mask = (bars.index >= te0) & (bars.index <= te1)
        r = run_backtest(bars, p, te0, te1, sigs=sig_full[mask])
        sharpes.append(r.sharpe)
        tpms.append(r.trades_per_month)
        rets.append(r.total_return_pct)
        dds.append(r.max_drawdown_pct)
    if not sharpes or float(np.mean(tpms)) < MIN_TRADES_PER_MONTH:
        return None
    mean_sh = float(np.mean(sharpes))
    std_sh = float(np.std(sharpes))
    chained = chain_oos_return(folds, bars, p)
    return OOSMetrics(
        params=p,
        mean_sharpe=mean_sh,
        std_sharpe=std_sh,
        robust_sharpe=mean_sh - 0.5 * std_sh,
        mean_tpm=float(np.mean(tpms)),
        min_tpm=float(np.min(tpms)),
        mean_fold_return_pct=float(np.mean(rets)),
        chained_oos_return_pct=chained,
        mean_max_dd_pct=float(np.mean(dds)),
        worst_max_dd_pct=float(np.min(dds)),
        pct_profitable_folds=float(np.mean([x > 0 for x in rets])),
        n_folds=len(folds),
    )


def search_all_oos(bars: pd.DataFrame, folds: list[tuple]) -> tuple[list[OOSMetrics], list[OOSMetrics]]:
    combos = param_combos()
    log(f"OOS selection: {len(combos)} configs × {len(STRATEGIES)} strategies (never touches unseen)")
    strict_l: list[OOSMetrics] = []
    loose_l: list[OOSMetrics] = []
    for n, p in enumerate(combos, 1):
        m_loose = evaluate_oos_loose(folds, bars, p)
        if m_loose:
            loose_l.append(m_loose)
        m = evaluate_oos(folds, bars, p)
        if m:
            strict_l.append(m)
        if n % 80 == 0:
            log(f"  ... {n}/{len(combos)} | strict: {len(strict_l)} loose: {len(loose_l)}")
    log(f"Strict qualified: {len(strict_l)}/{len(combos)} | loose (tpm only): {len(loose_l)}")
    return strict_l, loose_l


def pick_winners(strict: list[OOSMetrics], loose: list[OOSMetrics]) -> tuple[dict[str, OOSMetrics], bool]:
    pool = strict if strict else loose
    used_strict = bool(strict)
    if not pool:
        raise RuntimeError("No strategy met ≥10 trades/month on walk-forward OOS.")
    return {
        "profit": max(pool, key=lambda m: m.chained_oos_return_pct),
        "sharpe": max(pool, key=lambda m: m.robust_sharpe),
        "drawdown": max(pool, key=lambda m: m.worst_max_dd_pct),
    }, used_strict


def print_block(title: str, dev: BacktestResult, unseen: BacktestResult, m: OOSMetrics) -> None:
    log("\n" + "=" * 76)
    log(title)
    log("=" * 76)
    log(f"Strategy / params : {m.params.strategy} | {m.params}")
    extra = ""
    if m.allmarket_score != 0.0:
        extra = (
            f" | all-market score {m.allmarket_score:.3f} | minYearSh {m.min_yearly_sharpe:+.2f} | "
            f"posYears {m.pct_positive_years*100:.0f}%"
        )
    log(
        f"OOS (WF, pre-2025): Sharpe {m.mean_sharpe:+.2f} (robust {m.robust_sharpe:+.2f}) | "
        f"chained ret {m.chained_oos_return_pct:+.1f}% | worst fold DD {m.worst_max_dd_pct:.1f}% | "
        f"tpm {m.mean_tpm:.1f} | profitable folds {m.pct_profitable_folds*100:.0f}%{extra}"
    )
    log(
        f"Development backtest (<{UNSEEN_START}): ret {dev.total_return_pct:+.1f}% | "
        f"Sharpe {dev.sharpe:.2f} | DD {dev.max_drawdown_pct:.1f}% | tpm {dev.trades_per_month:.1f}"
    )
    ue = unseen.equity_curve.index[-1].date() if len(unseen.equity_curve) else "?"
    log(
        f"UNSEEN ({UNSEEN_START} → {ue}): ret {unseen.total_return_pct:+.1f}% | "
        f"Sharpe {unseen.sharpe:.2f} | DD {unseen.max_drawdown_pct:.1f}% | "
        f"tpm {unseen.trades_per_month:.1f} | ${unseen.final_equity:.2f}"
    )
    if unseen.trades:
        monthly = pd.Series(
            1, index=pd.DatetimeIndex([t.entry_time for t in unseen.trades])
        ).resample("ME").sum()
        log("Unseen monthly trades:")
        for ts, cnt in monthly.items():
            mark = "✓" if cnt >= MIN_TRADES_PER_MONTH else " "
            log(f"  {ts.strftime('%Y-%m')}  {int(cnt):3d}  {mark}")


def result_row(period: str, start: pd.Timestamp, end: pd.Timestamp, r: BacktestResult) -> dict:
    months = max((end - start).days / 30.44, 1 / 30.44)
    return {
        "period": period,
        "start": start.date().isoformat(),
        "end": end.date().isoformat(),
        "sharpe": round(r.sharpe, 4),
        "return_pct": round(r.total_return_pct, 2),
        "max_dd_pct": round(r.max_drawdown_pct, 2),
        "trades": len(r.trades),
        "trades_per_month": round(len(r.trades) / months, 2),
        "win_rate_pct": round(r.win_rate * 100, 1),
        "profit_factor": round(r.profit_factor, 3),
        "final_equity": round(r.final_equity, 2),
    }


def print_result_table(rows: list[dict], title: str) -> None:
    log("\n" + "=" * 100)
    log(title)
    log("=" * 100)
    log(f"{'Period':<22} {'Start':<12} {'End':<12} {'Sharpe':>8} {'Return%':>10} {'MaxDD%':>8} {'Trades':>7} {'TPM':>6} {'WR%':>6} {'PF':>6} {'Equity':>8}")
    log("-" * 100)
    for row in rows:
        log(
            f"{row['period']:<22} {row['start']:<12} {row['end']:<12} "
            f"{row['sharpe']:>8.3f} {row['return_pct']:>+9.1f}% {row['max_dd_pct']:>7.1f}% "
            f"{row['trades']:>7} {row['trades_per_month']:>6.1f} {row['win_rate_pct']:>5.1f}% "
            f"{row['profit_factor']:>6.2f} ${row['final_equity']:>7.2f}"
        )
    log("=" * 100)


def run_full_dataset_report(bars: pd.DataFrame, p: StrategyParams, unseen_start: pd.Timestamp) -> None:
    log("\n" + "#" * 100)
    log(f"FULL DATASET REPORT — {p.strategy} | 1H bars resampled from 5m CSV")
    log(f"Params: {p}")
    log("#" * 100)

    dev_end = unseen_start - pd.Timedelta(days=1)
    end = bars.index[-1]
    sig = build_signals(bars, p)
    rows: list[dict] = []

    full = run_backtest(bars, p, bars.index[0], end)
    rows.append(result_row("FULL DATASET", bars.index[0], end, full))
    dev = run_backtest(bars, p, bars.index[0], dev_end)
    rows.append(result_row("Development (<unseen)", bars.index[0], dev_end, dev))
    unseen = run_backtest(bars, p, unseen_start, end)
    rows.append(result_row("UNSEEN (holdout)", unseen_start, end, unseen))
    print_result_table(rows, "SUMMARY BY PERIOD")

    folds = wf_folds(bars.index[0], dev_end, unseen_start)
    fold_rows = []
    log("\nWALK-FORWARD OOS FOLDS:")
    log(f"{'Fold':>4} {'Test start':<12} {'Test end':<12} {'Sharpe':>8} {'Return%':>10} {'MaxDD%':>8} {'Trades':>7} {'TPM':>6}")
    log("-" * 80)
    for fid, _, _, te0, te1 in folds:
        mask = (bars.index >= te0) & (bars.index <= te1)
        r = run_backtest(bars, p, te0, te1, sigs=sig[mask])
        fold_rows.append({
            "fold": fid,
            "test_start": te0.date().isoformat(),
            "test_end": te1.date().isoformat(),
            "sharpe": round(r.sharpe, 4),
            "return_pct": round(r.total_return_pct, 2),
            "max_dd_pct": round(r.max_drawdown_pct, 2),
            "trades": len(r.trades),
            "trades_per_month": round(r.trades_per_month, 2),
            "win_rate_pct": round(r.win_rate * 100, 1),
            "profit_factor": round(r.profit_factor, 3),
        })
        log(
            f"{fid:>4} {str(te0.date()):<12} {str(te1.date()):<12} {r.sharpe:>8.3f} "
            f"{r.total_return_pct:>+9.1f}% {r.max_drawdown_pct:>7.1f}% {len(r.trades):>7} {r.trades_per_month:>6.1f}"
        )
    oos_sh = [x["sharpe"] for x in fold_rows]
    log("-" * 80)
    log(
        f"OOS mean Sharpe: {np.mean(oos_sh):+.3f} | median: {np.median(oos_sh):+.3f} | "
        f"Sharpe>0: {sum(1 for s in oos_sh if s > 0)}/{len(oos_sh)}"
    )

    year_rows = []
    log("\nYEARLY BREAKDOWN:")
    log(f"{'Year':<6} {'Sharpe':>8} {'Return%':>10} {'MaxDD%':>8} {'Trades':>7} {'TPM':>6} {'WR%':>6} {'PF':>6}")
    log("-" * 70)
    for year in range(bars.index[0].year, end.year + 1):
        ys = pd.Timestamp(f"{year}-01-01")
        ye = pd.Timestamp(f"{year}-12-31 23:59:59")
        if ys > end:
            break
        ye = min(ye, end)
        if ys < bars.index[0]:
            ys = bars.index[0]
        mask = (bars.index >= ys) & (bars.index <= ye)
        if mask.sum() < 50:
            continue
        r = run_backtest(bars, p, ys, ye, sigs=sig[mask])
        year_rows.append(result_row(str(year), ys, ye, r))
        log(
            f"{year:<6} {r.sharpe:>8.3f} {r.total_return_pct:>+9.1f}% {r.max_drawdown_pct:>7.1f}% "
            f"{len(r.trades):>7} {r.trades_per_month:>6.1f} {r.win_rate*100:>5.1f}% {r.profit_factor:>6.2f}"
        )

    out = Path(__file__).resolve().parent
    pd.DataFrame(rows).to_csv(out / "full_report_periods.csv", index=False)
    pd.DataFrame(fold_rows).to_csv(out / "full_report_wf_folds.csv", index=False)
    pd.DataFrame(year_rows).to_csv(out / "full_report_yearly.csv", index=False)
    pd.DataFrame([vars(t) for t in full.trades]).to_csv(out / "full_report_all_trades.csv", index=False)
    log("\nSaved: full_report_periods.csv, full_report_wf_folds.csv, full_report_yearly.csv, full_report_all_trades.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description="Strict WF + unseen holdout")
    parser.add_argument("--csv", default=None)
    parser.add_argument("--unseen-start", default=UNSEEN_START)
    parser.add_argument(
        "--full-report",
        action="store_true",
        help="Run full dataset report on winner params",
    )
    parser.add_argument(
        "--legacy-winners",
        action="store_true",
        help="Old flow: pick profit/sharpe/drawdown winners (not all-market)",
    )
    parser.add_argument(
        "--stable-search",
        action="store_true",
        help="Search custom quant strategies for OOS stable profit (default if no other mode)",
    )
    parser.add_argument(
        "--ensemble",
        action="store_true",
        help="Combine ALL strategies via voting; tune vote_k/SL/cooldown for high robust Sharpe",
    )
    parser.add_argument(
        "--portfolio",
        action="store_true",
        help="Combine strategies as an equal-weight PORTFOLIO of OOS-validated sleeves (recommended)",
    )
    parser.add_argument(
        "--tune-sleeves",
        action="store_true",
        help="Portfolio: tune each sleeve's params on WF-OOS (slower) instead of defaults",
    )
    parser.add_argument(
        "--risk-frac",
        type=float,
        default=0.01,
        help="Per-trade risk as fraction of equity (volatility-parity sizing). Default 0.01",
    )
    parser.add_argument(
        "--max-sleeves",
        type=int,
        default=8,
        help="Portfolio: max number of sleeves to combine (more = more trades, less concentration)",
    )
    parser.add_argument(
        "--min-pos-years",
        type=float,
        default=0.50,
        help="Portfolio: min fraction of profitable calendar years to include a sleeve "
             "(lower => more sleeves => more trades/week, slightly lower Sharpe). Default 0.50",
    )
    parser.add_argument(
        "--trend-sma",
        type=int,
        default=100,
        help="Portfolio: daily N-day SMA regime gate (only trade with the trend). "
             "0=off. ~50 = highest Sharpe, ~100 = lowest drawdown. Default 100",
    )
    parser.add_argument(
        "--min-tpm",
        type=float,
        default=float(MIN_TRADES_PER_MONTH),
        help="Minimum trades/month target for ensemble selection (more trades/week)",
    )
    args = parser.parse_args()
    stable_mode = args.stable_search or (
        not args.legacy_winners and not args.full_report and not args.ensemble and not args.portfolio
    )

    csv_path = resolve_csv(args.csv)
    unseen_start = pd.Timestamp(args.unseen_start)
    log(f"Loading 5m CSV: {csv_path}")
    df5 = load_ohlcv_5m(csv_path)
    log(f"5m bars: {len(df5):,} | {df5.index[0].date()} → {df5.index[-1].date()}")

    bars = build_tf_bars(df5)["1h"]
    log(f"Resampled 1H bars: {len(bars):,}")

    dev_end = unseen_start - pd.Timedelta(days=1)
    folds = wf_folds(bars.index[0], dev_end, unseen_start)
    unseen_end = bars.index[-1]

    if args.full_report:
        run_full_dataset_report(bars, DEFAULT_STRATEGY, unseen_start)
        return

    log(f"\nDevelopment + WF: {bars.index[0].date()} → {dev_end.date()} ({len(folds)} OOS folds)")
    log(f"UNSEEN (eval once): {unseen_start.date()} → {unseen_end.date()}")
    log(f"Requirements: ≥{MIN_TRADES_PER_MONTH} tpm | trailing SL + 50% @ 2R | 0.002 lots | ${INITIAL_CAPITAL:.0f}")

    if args.portfolio:
        log("\n" + "#" * 76)
        log("PORTFOLIO COMBINE — every strategy as a sleeve, equal-weight the survivors")
        log("#" * 76)
        log(f"  Risk sizing: {args.risk_frac:.1%}/trade (vol parity) | sleeve tuning: {args.tune_sleeves} | "
            f"max sleeves: {args.max_sleeves} | daily trend gate SMA: {args.trend_sma or 'off'}")
        log("  Step 1 — score each sleeve on WF-OOS folds + yearly stability (no unseen used):")
        sleeves = best_config_per_strategy(bars, folds, dev_end, args.risk_frac, args.tune_sleeves, args.trend_sma)
        if not sleeves:
            raise RuntimeError("No sleeves evaluable.")

        # Selection (anti-overfit): positive MEAN OOS fold Sharpe + edge that holds
        # across the majority of calendar YEARS (years are far less noisy than the
        # 6-month WF folds). Rank by a stability-weighted score, equal-weight winners.
        def sleeve_score(s: SleeveOOS) -> float:
            return s.mean_sharpe + 0.6 * (s.pct_pos_years - 0.5)

        qualified = [
            s for s in sleeves.values()
            if s.mean_sharpe > 0 and s.pct_pos_years >= args.min_pos_years and s.mean_tpm >= 1.0
        ]
        qualified.sort(key=sleeve_score, reverse=True)
        if not qualified:
            log("  ⚠ No sleeve passed mean-Sharpe+yearly filter; falling back to positive-mean-Sharpe sleeves.")
            qualified = sorted(
                [s for s in sleeves.values() if s.mean_sharpe > 0],
                key=sleeve_score, reverse=True,
            )
        if not qualified:
            raise RuntimeError("No sleeve had positive mean OOS Sharpe — no tradeable edge found.")

        selected = qualified[: args.max_sleeves]
        sel_params = [s.params for s in selected]
        log(f"\n  Selected {len(selected)} sleeves (by mean OOS Sharpe + yearly stability):")
        for s in selected:
            log(f"    • {s.params.strategy:16s} meanOOSsh={s.mean_sharpe:+.2f} tpm={s.mean_tpm:.1f} "
                f"posY={s.pct_pos_years*100:.0f}% sl/cd={s.params.atr_mult_sl}/{s.params.cooldown_bars}")

        # Portfolio metrics: per-fold OOS (robust), dev (in-sample), unseen (true test).
        log("\n  Step 2 — combined portfolio performance:")
        fold_sh = []
        for _, _, _, te0, te1 in folds:
            rets = portfolio_daily_returns(bars, sel_params, te0, te1)
            if rets is not None:
                fold_sh.append(_sharpe(rets))
        oos_mean = float(np.mean(fold_sh)) if fold_sh else 0.0
        oos_robust = float(np.mean(fold_sh) - 0.5 * np.std(fold_sh)) if fold_sh else 0.0
        oos_pos = float(np.mean([s > 0 for s in fold_sh])) if fold_sh else 0.0

        dev_m = portfolio_metrics(bars, sel_params, bars.index[0], dev_end)
        uns_m = portfolio_metrics(bars, sel_params, unseen_start, unseen_end)

        log("\n" + "=" * 76)
        log("PORTFOLIO RESULTS")
        log("=" * 76)
        log(f"WF-OOS folds   : mean Sharpe {oos_mean:+.2f} | robust {oos_robust:+.2f} | "
            f"Sharpe>0 {sum(s>0 for s in fold_sh)}/{len(fold_sh)} folds")
        log(f"Development    : Sharpe {dev_m['sharpe']:+.2f} | ret {dev_m['return_pct']:+.1f}% | "
            f"DD {dev_m['max_dd_pct']:.1f}% | {dev_m['trades']} trades | tpm {dev_m['tpm']:.1f}")
        log(f"UNSEEN 2025    : Sharpe {uns_m['sharpe']:+.2f} | ret {uns_m['return_pct']:+.1f}% | "
            f"DD {uns_m['max_dd_pct']:.1f}% | {uns_m['trades']} trades | tpm {uns_m['tpm']:.1f}")
        log(f"Trades/week    : dev {dev_m['tpm']/4.345:.1f} | unseen {uns_m['tpm']/4.345:.1f}")
        log("\nAnti-overfit read:")
        log(f"  • dev−OOS Sharpe gap = {dev_m['sharpe']-oos_mean:+.2f} (small = robust, not curve-fit)")
        log(f"  • {oos_pos*100:.0f}% of OOS folds positive | sleeves equal-weighted (no fitted weights)")
        log(f"  • unseen 2025 never touched during selection")
        log("\nDial trades/week vs Sharpe:")
        log("  • More trades   : --min-pos-years 0.45 --max-sleeves 10  (more sleeves)")
        log("  • Higher Sharpe : --min-pos-years 0.55 --max-sleeves 4 --trend-sma 50")
        log("  • Lower drawdown: --risk-frac 0.005 --trend-sma 100      (smaller positions, trend gate)")
        log("  • Trend gate off: --trend-sma 0                          (trade all regimes)")

        out = Path(__file__).resolve().parent
        pd.DataFrame([
            {
                "strategy": s.params.strategy,
                "params": str(s.params),
                "oos_robust_sharpe": s.robust_sharpe,
                "oos_mean_sharpe": s.mean_sharpe,
                "mean_tpm": s.mean_tpm,
                "pct_pos_years": s.pct_pos_years,
                "min_year_sharpe": s.min_year_sharpe,
                "selected": s in selected,
            }
            for s in sorted(sleeves.values(), key=lambda x: x.robust_sharpe, reverse=True)
        ]).to_csv(out / "portfolio_sleeves.csv", index=False)
        pd.DataFrame([{
            "n_sleeves": len(selected),
            "sleeves": ",".join(s.params.strategy for s in selected),
            "risk_frac": args.risk_frac,
            "oos_mean_sharpe": oos_mean,
            "oos_robust_sharpe": oos_robust,
            "oos_pct_pos_folds": oos_pos,
            "dev_sharpe": dev_m["sharpe"],
            "dev_return_pct": dev_m["return_pct"],
            "dev_max_dd_pct": dev_m["max_dd_pct"],
            "dev_tpm": dev_m["tpm"],
            "unseen_sharpe": uns_m["sharpe"],
            "unseen_return_pct": uns_m["return_pct"],
            "unseen_max_dd_pct": uns_m["max_dd_pct"],
            "unseen_tpm": uns_m["tpm"],
            "unseen_trades": uns_m["trades"],
        }]).to_csv(out / "portfolio_summary.csv", index=False)
        log("\nSaved: portfolio_sleeves.csv, portfolio_summary.csv")
        return

    if args.ensemble:
        ranked = search_ensemble(bars, folds, dev_end, args.min_tpm)
        if not ranked:
            log(f"\n⚠ No ensemble config met ≥{args.min_tpm:.0f} tpm. Relaxing tpm to 5 and re-scoring...")
            ranked = search_ensemble(bars, folds, dev_end, 5.0)
        if not ranked:
            raise RuntimeError("Ensemble produced no evaluable configs.")

        winner = ranked[0]
        p = winner.params
        log("\n" + "#" * 76)
        log(f"ENSEMBLE WINNER — {len(ENSEMBLE_BASE)} strategies combined by majority vote (OOS-selected)")
        log("#" * 76)
        log(f"  vote_k={p.vote_k} | atr_mult_sl={p.atr_mult_sl} | cooldown_bars={p.cooldown_bars} | long_only={p.long_only}")
        log(f"  score={winner.allmarket_score:.3f} | robustSh={winner.robust_sharpe:+.2f} | "
            f"minYearSh={winner.min_yearly_sharpe:+.2f} | posYears={winner.pct_positive_years*100:.0f}% | tpm={winner.mean_tpm:.1f}")
        if len(ranked) > 1:
            log("\nTop ensemble configs (OOS):")
            for i, m in enumerate(ranked[:8], 1):
                log(
                    f"  {i}. vote_k={m.params.vote_k} sl={m.params.atr_mult_sl} cd={m.params.cooldown_bars} | "
                    f"robustSh={m.robust_sharpe:+.2f} minYSh={m.min_yearly_sharpe:+.2f} "
                    f"posY={m.pct_positive_years*100:.0f}% tpm={m.mean_tpm:.1f} chained={m.chained_oos_return_pct:+.1f}%"
                )

        dev = run_backtest(bars, p, bars.index[0], dev_end)
        unseen = run_backtest(bars, p, unseen_start, unseen_end)
        print_block("ENSEMBLE WINNER", dev, unseen, winner)

        gap = dev.sharpe - winner.mean_sharpe
        log("\nAnti-overfit checks (selection used OOS only; unseen untouched until now):")
        log(f"  • OOS mean Sharpe (WF folds)     : {winner.mean_sharpe:+.2f}")
        log(f"  • Dev (in-sample) Sharpe         : {dev.sharpe:+.2f}  (dev−OOS gap {gap:+.2f}; small gap = robust)")
        log(f"  • UNSEEN 2025 Sharpe (true test) : {unseen.sharpe:+.2f} | ret {unseen.total_return_pct:+.1f}% | tpm {unseen.trades_per_month:.1f}")
        log(f"  • Trades/week (unseen)           : {unseen.trades_per_month/4.345:.1f}")

        out = Path(__file__).resolve().parent
        row = _summary_row("ensemble", winner, dev, unseen)
        row["ensemble_score"] = winner.allmarket_score
        row["vote_k"] = p.vote_k
        pd.DataFrame([row]).to_csv(out / "strategy_comparison.csv", index=False)
        pd.DataFrame([
            {**{"rank": i + 1, "params": str(m.params)}, **{k: getattr(m, k) for k in vars(m) if k != "params"}}
            for i, m in enumerate(ranked[:30])
        ]).to_csv(out / "oos_all_qualified.csv", index=False)
        pd.DataFrame([vars(t) for t in unseen.trades]).to_csv(out / "backtest_trades.csv", index=False)
        log("\nSaved: strategy_comparison.csv, oos_all_qualified.csv, backtest_trades.csv")
        log(f"\nTo run a full per-year/fold report on this winner:")
        log(f"  set DEFAULT_STRATEGY vote_k/sl/cooldown, then: python {Path(__file__).name} --full-report")
        return

    if stable_mode and not args.legacy_winners:
        stable = search_stable_profit(bars, folds, dev_end)
        if not stable:
            log("\n⚠ No config passed stable OOS filters. Showing best OOS profit among custom strategies...")
            loose_l: list[OOSMetrics] = []
            for p in param_combos(only=STABLE_STRATEGIES):
                m = evaluate_oos_loose(folds, bars, p)
                if m and m.mean_tpm >= MIN_TRADES_PER_MONTH:
                    loose_l.append(m)
            stable = sorted(loose_l, key=lambda x: x.chained_oos_return_pct, reverse=True)

        if not stable:
            raise RuntimeError("No custom strategy met ≥10 trades/month on OOS.")

        winner = stable[0]
        p = winner.params
        log("\n" + "#" * 76)
        log("STABLE-PROFIT WINNER (custom strategies, OOS only)")
        log("#" * 76)
        passed = winner.chained_oos_return_pct > 0 and winner.robust_sharpe > 0
        if not passed:
            log("  ⚠ Did not pass strict stable filters — best available by OOS chained return")
        log(f"  {p}")
        if len(stable) > 1:
            log("\nTop 5:")
            for i, m in enumerate(stable[:5], 1):
                log(
                    f"  {i}. {m.params.strategy:18s} chained={m.chained_oos_return_pct:+6.1f}% "
                    f"robust={m.robust_sharpe:+.2f} tpm={m.mean_tpm:.1f}"
                )

        dev = run_backtest(bars, p, bars.index[0], dev_end)
        unseen = run_backtest(bars, p, unseen_start, unseen_end)
        print_block("STABLE WINNER", dev, unseen, winner)

        log("\nStable-profit checklist:")
        log(f"  • OOS chained return > 0: {'✓' if winner.chained_oos_return_pct > 0 else '✗'} ({winner.chained_oos_return_pct:+.1f}%)")
        log(f"  • OOS robust Sharpe > 0: {'✓' if winner.robust_sharpe > 0 else '✗'} ({winner.robust_sharpe:+.2f})")
        log(f"  • ≥{MIN_TRADES_PER_MONTH} tpm: {'✓' if winner.mean_tpm >= MIN_TRADES_PER_MONTH else '✗'}")
        log(f"  • Unseen return (reference): {unseen.total_return_pct:+.1f}% | Sharpe {unseen.sharpe:.2f}")

        out = Path(__file__).resolve().parent
        row = _summary_row("stable", winner, dev, unseen)
        row["stable_score"] = winner.allmarket_score
        row["passed_stable_filter"] = passed
        pd.DataFrame([row]).to_csv(out / "strategy_comparison.csv", index=False)
        pd.DataFrame([
            {**{"rank": i + 1, "params": str(m.params)}, **{k: getattr(m, k) for k in vars(m) if k != "params"}}
            for i, m in enumerate(stable[:30])
        ]).to_csv(out / "oos_all_qualified.csv", index=False)
        pd.DataFrame([vars(t) for t in unseen.trades]).to_csv(out / "backtest_trades.csv", index=False)
        log("\nSaved: strategy_comparison.csv, oos_all_qualified.csv, backtest_trades.csv")
        return

    if args.legacy_winners:
        strict, loose = search_all_oos(bars, folds)
        winners, used_strict = pick_winners(strict, loose)
        log("\n" + "#" * 76)
        if used_strict:
            log("OOS WINNERS (strict) — no unseen data used")
        else:
            log("⚠ OOS WINNERS (loose fallback: tpm only)")
        log("#" * 76)
        for label, m in winners.items():
            log(
                f"  [{label:9s}] {m.params.strategy:16s} | chained OOS {m.chained_oos_return_pct:+7.1f}% | "
                f"robust Sharpe {m.robust_sharpe:+.2f} | worst fold DD {m.worst_max_dd_pct:6.1f}%"
            )
        summary_rows = []
        for label, m in winners.items():
            p = m.params
            dev = run_backtest(bars, p, bars.index[0], dev_end)
            unseen = run_backtest(bars, p, unseen_start, unseen_end)
            print_block(f"WINNER: {label.upper()}", dev, unseen, m)
            summary_rows.append(_summary_row(label, m, dev, unseen))
        out = Path(__file__).resolve().parent
        pd.DataFrame(summary_rows).to_csv(out / "strategy_comparison.csv", index=False)
        pd.DataFrame([{**{"params": str(m.params)}, **{k: getattr(m, k) for k in vars(m) if k != "params"}} for m in loose]).to_csv(
            out / "oos_all_qualified.csv", index=False
        )
        ref = winners["sharpe"].params
        unseen_ref = run_backtest(bars, ref, unseen_start, unseen_end)
        pd.DataFrame([vars(t) for t in unseen_ref.trades]).to_csv(out / "backtest_trades.csv", index=False)
        log("\nSaved: strategy_comparison.csv, oos_all_qualified.csv, backtest_trades.csv")
        return

    # --- All-market (bull / bear / range) selection ---
    allmarket = search_allmarket(bars, folds, dev_end)
    if not allmarket:
        log("\n⚠ No config passed all-market filters (≥10 tpm, ≥42% positive years). Relaxing to tpm-only + yearly stats...")
        loose_l: list[OOSMetrics] = []
        for p in param_combos():
            m = evaluate_oos_loose(folds, bars, p)
            if not m:
                continue
            ysh, yret = yearly_performance(bars, p, dev_end)
            if len(ysh) < 5:
                continue
            m.min_yearly_sharpe = float(np.min(ysh))
            m.pct_positive_years = float(np.mean([x > 0 for x in yret]))
            m.allmarket_score = 0.4 * m.robust_sharpe + 0.3 * m.min_yearly_sharpe + 0.3 * m.pct_positive_years
            loose_l.append(m)
        allmarket = sorted(loose_l, key=lambda x: x.allmarket_score, reverse=True)

    if not allmarket:
        raise RuntimeError("No strategy met minimum trade frequency on OOS.")

    winner = allmarket[0]
    p = winner.params

    log("\n" + "#" * 76)
    log("ALL-MARKET WINNER (OOS only — trend + range via regime_adaptive or best scorer)")
    log("#" * 76)
    log(f"  {p.strategy} | score={winner.allmarket_score:.3f} | minYearSh={winner.min_yearly_sharpe:+.2f} | "
        f"robustSh={winner.robust_sharpe:+.2f} | posYears={winner.pct_positive_years*100:.0f}%")
    if len(allmarket) > 1:
        log("\nTop 5 all-market (OOS):")
        for i, m in enumerate(allmarket[:5], 1):
            log(
                f"  {i}. {m.params.strategy:16s} score={m.allmarket_score:.3f} | "
                f"minYSh={m.min_yearly_sharpe:+.2f} robust={m.robust_sharpe:+.2f} | "
                f"OOS chained={m.chained_oos_return_pct:+.1f}% tpm={m.mean_tpm:.1f}"
            )

    dev = run_backtest(bars, p, bars.index[0], dev_end)
    unseen = run_backtest(bars, p, unseen_start, unseen_end)
    print_block("ALL-MARKET WINNER", dev, unseen, winner)

    targets = []
    if winner.robust_sharpe >= 1.0:
        targets.append("robust OOS Sharpe ≥1.0 ✓")
    else:
        targets.append(f"robust OOS Sharpe ≥1.0 ✗ ({winner.robust_sharpe:.2f})")
    if winner.mean_tpm >= MIN_TRADES_PER_MONTH:
        targets.append(f"≥{MIN_TRADES_PER_MONTH} tpm ✓")
    if winner.pct_positive_years >= 0.5:
        targets.append("≥50% positive years ✓")
    else:
        targets.append(f"≥50% positive years ✗ ({winner.pct_positive_years*100:.0f}%)")
    if unseen.sharpe >= 1.0 and unseen.trades_per_month >= MIN_TRADES_PER_MONTH:
        targets.append(f"unseen Sharpe≥1 ({unseen.sharpe:.2f}) ✓")
    else:
        targets.append(f"unseen Sharpe≥1 ({unseen.sharpe:.2f}) — reference only")

    log("\nTarget checklist (OOS vs unseen):")
    for t in targets:
        log(f"  • {t}")

    out = Path(__file__).resolve().parent
    row = _summary_row("allmarket", winner, dev, unseen)
    row["allmarket_score"] = winner.allmarket_score
    row["min_yearly_sharpe"] = winner.min_yearly_sharpe
    row["pct_positive_years"] = winner.pct_positive_years
    pd.DataFrame([row]).to_csv(out / "strategy_comparison.csv", index=False)
    pd.DataFrame([
        {**{"rank": i + 1, "params": str(m.params)}, **{k: getattr(m, k) for k in vars(m) if k != "params"}}
        for i, m in enumerate(allmarket[:30])
    ]).to_csv(out / "oos_all_qualified.csv", index=False)
    pd.DataFrame([vars(t) for t in unseen.trades]).to_csv(out / "backtest_trades.csv", index=False)
    log("\nSaved: strategy_comparison.csv, oos_all_qualified.csv, backtest_trades.csv (all-market winner, unseen)")
    log(f"\nFull report: python {Path(__file__).name} --full-report --csv {csv_path}")
    log("(Update DEFAULT_STRATEGY in backtest.py to winner params for --full-report)")


def _summary_row(label: str, m: OOSMetrics, dev: BacktestResult, unseen: BacktestResult) -> dict:
    return {
        "winner_type": label,
        "strategy": m.params.strategy,
        "params": str(m.params),
        "oos_mean_sharpe": m.mean_sharpe,
        "oos_robust_sharpe": m.robust_sharpe,
        "oos_chained_return_pct": m.chained_oos_return_pct,
        "oos_worst_dd_pct": m.worst_max_dd_pct,
        "oos_mean_tpm": m.mean_tpm,
        "dev_return_pct": dev.total_return_pct,
        "dev_sharpe": dev.sharpe,
        "dev_max_dd_pct": dev.max_drawdown_pct,
        "unseen_return_pct": unseen.total_return_pct,
        "unseen_sharpe": unseen.sharpe,
        "unseen_max_dd_pct": unseen.max_drawdown_pct,
        "unseen_tpm": unseen.trades_per_month,
        "unseen_trades": len(unseen.trades),
        "unseen_final_equity": unseen.final_equity,
    }


if __name__ == "__main__":
    main()
