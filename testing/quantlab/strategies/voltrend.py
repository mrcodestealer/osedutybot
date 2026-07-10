"""Volatility-aware trend strategies for XAUUSD 5m (family: voltrend).

Variants
--------
supertrend      : classic ATR-band flip trend follower (per-bar band logic).
keltner_trend   : hold long while price rides above Keltner upper band,
                  exit on close back through the EMA midline (short mirrored).
ema_atr_regime  : EMA trend direction, gated by the percentile of ATR(96)
                  over the trailing ~90 days (16000 bars). Low-vol band vs
                  high-vol band regimes.
atr_momentum    : continuous position clip(k * (close - sma(n)) / atr(96)).

All functions follow the engine contract: pos[t] decided at close of bar t,
engine applies it to the t -> t+1 return. long_only=1 clips to [0, 1].
"""
import os
import sys

import numpy as np

_PARENT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)
import engine


# ------------------------------------------------------------- (a) SuperTrend

def _supertrend_dir(d, period, mult):
    """Running SuperTrend direction (+1 up / -1 down / 0 warmup).

    Classic final-band recursion; inherently sequential, so this is the one
    per-bar python loop in the family (runs over plain lists for speed).
    """
    a = engine.atr(d, period)
    hl2 = (d["high"] + d["low"]) / 2.0
    ub = (hl2 + mult * a).tolist()
    lb = (hl2 - mult * a).tolist()
    c = d["close"].tolist()
    n = len(c)
    out = [0.0] * n
    if n == 0:
        return np.asarray(out)
    fub = ub[0]
    flb = lb[0]
    trend = 0.0
    prev_c = c[0]
    for t in range(1, n):
        u = ub[t]
        l = lb[t]
        ct = c[t]
        # final band updates use close[t-1] -> causal
        if u < fub or prev_c > fub:
            fub = u
        if l > flb or prev_c < flb:
            flb = l
        # flip logic uses close[t] vs the current final bands -> causal
        if ct > fub:
            trend = 1.0
        elif ct < flb:
            trend = -1.0
        out[t] = trend
        prev_c = ct
    return np.asarray(out)


def supertrend(d, period=14, mult=3.0, long_only=0):
    dirv = _supertrend_dir(d, period, mult)
    if long_only:
        return np.maximum(dirv, 0.0)
    return dirv


# --------------------------------------------------------- (b) Keltner trend

def keltner_trend(d, n=100, mult=2.0, long_only=0):
    """Enter long when close breaks above EMA(n) + mult*ATR(n); stay long
    until close falls back through the EMA midline. Short leg mirrored."""
    close = d["close"]
    mid = engine.ema(close, n)
    a = engine.atr(d, n)
    upper = mid + mult * a
    lower = mid - mult * a
    zeros = np.zeros(len(close), dtype=bool)
    pos_l = engine.hold_signal(close > upper, zeros, close < mid)
    if long_only:
        return pos_l
    pos_s = engine.hold_signal(zeros, close < lower, close > mid)
    return pos_l + pos_s


# -------------------------------------------- (c) EMA trend + ATR-percentile

def ema_atr_regime(d, n=400, pct_lo=0.0, pct_hi=0.35, atr_n=96,
                   rank_win=16500, long_only=0):
    """sign(close - EMA(n)) but only while the percentile rank of ATR(atr_n)
    within the trailing rank_win bars (~90 days) lies in [pct_lo, pct_hi]."""
    import pandas as pd
    close = d["close"]
    trend = np.sign(close - engine.ema(close, n))
    a = engine.atr(d, atr_n)
    p = pd.Series(a).rolling(rank_win, min_periods=rank_win).rank(pct=True).to_numpy()
    gate = (p >= pct_lo) & (p <= pct_hi)          # NaN warmup -> False -> flat
    pos = np.where(gate, trend, 0.0)
    if long_only:
        pos = np.maximum(pos, 0.0)
    return pos


# --------------------------------------------- (d) ATR-normalized momentum

def atr_momentum(d, n=96, k=0.5, atr_n=96, long_only=0):
    """Continuous position: clip(k * (close - SMA(n)) / ATR(atr_n), -1, 1)."""
    close = d["close"]
    m = engine.sma(close, n)
    a = engine.atr(d, atr_n)
    with np.errstate(divide="ignore", invalid="ignore"):
        z = (close - m) / a
    pos = np.clip(k * z, -1.0, 1.0)
    if long_only:
        pos = np.clip(pos, 0.0, 1.0)
    return np.nan_to_num(pos)


# Best train-selected config per variant (selected by TRAIN sharpe only;
# see results/voltrend_grid.csv for the full 168-row grid).
BEST = [
    {"fn": "supertrend", "variant": "supertrend",
     "params": {"period": 56, "mult": 5.0, "long_only": 1}},
    {"fn": "keltner_trend", "variant": "keltner_trend",
     "params": {"n": 400, "mult": 3.0, "long_only": 1}},
    {"fn": "ema_atr_regime", "variant": "ema_atr_regime",
     "params": {"n": 3200, "pct_lo": 0.0, "pct_hi": 0.3, "atr_n": 96,
                "rank_win": 16500, "long_only": 1}},
    {"fn": "atr_momentum", "variant": "atr_momentum",
     "params": {"n": 768, "k": 1.2, "atr_n": 96, "long_only": 1}},
]
