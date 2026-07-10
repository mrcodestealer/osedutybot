"""Moving-average trend-following family for XAUUSD 5m.

All functions follow the engine contract:
    fn(d, **params) -> np.ndarray of positions in [-1, +1], same length as d['close'].
pos[t] is decided at the close of bar t (engine applies it to the t -> t+1 return).
All indicators are causal (pandas rolling / recursive ewm).

Variants:
    sma_cross  - SMA fast/slow cross (long_flat=False -> long-short, True -> long-flat)
    ema_cross  - EMA fast/slow cross (same modes)
    macd_hist  - sign of MACD histogram (fast/slow/signal EMAs)
    triple_ma  - triple-MA alignment: long iff fast>mid>slow, short iff fast<mid<slow, else flat
"""
import os
import sys

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_HERE)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

import engine


def sma_cross(d, fast=20, slow=120, long_flat=False):
    c = d["close"]
    f = engine.sma(c, fast)
    s = engine.sma(c, slow)
    pos = np.where(f > s, 1.0, -1.0)
    pos[np.isnan(f) | np.isnan(s)] = 0.0
    if long_flat:
        pos = np.maximum(pos, 0.0)
    return pos


def ema_cross(d, fast=20, slow=120, long_flat=False):
    c = d["close"]
    f = engine.ema(c, fast)
    s = engine.ema(c, slow)
    pos = np.where(f > s, 1.0, -1.0)
    pos[np.isnan(f) | np.isnan(s)] = 0.0
    if long_flat:
        pos = np.maximum(pos, 0.0)
    return pos


def macd_hist(d, fast=12, slow=26, signal=9, long_flat=False):
    c = d["close"]
    macd = engine.ema(c, fast) - engine.ema(c, slow)
    sig = engine.ema(macd, signal)
    hist = macd - sig
    pos = np.sign(hist).astype(np.float64)
    pos[np.isnan(hist)] = 0.0
    if long_flat:
        pos = np.maximum(pos, 0.0)
    return pos


def triple_ma(d, fast=20, mid=120, slow=480, long_flat=False):
    c = d["close"]
    f = engine.sma(c, fast)
    m = engine.sma(c, mid)
    s = engine.sma(c, slow)
    up = (f > m) & (m > s)
    dn = (f < m) & (m < s)
    pos = np.where(up, 1.0, np.where(dn, -1.0, 0.0))
    pos[np.isnan(f) | np.isnan(m) | np.isnan(s)] = 0.0
    if long_flat:
        pos = np.maximum(pos, 0.0)
    return pos


# Best train-sharpe config per variant (filled by the grid search; see
# results/ma_cross_grid.csv for the full sweep).
BEST = [
    {"fn": "sma_cross", "params": {"fast": 80, "slow": 240, "long_flat": False}, "variant": "sma_cross_ls"},
    {"fn": "sma_cross", "params": {"fast": 80, "slow": 240, "long_flat": True}, "variant": "sma_cross_lf"},
    {"fn": "ema_cross", "params": {"fast": 40, "slow": 480, "long_flat": False}, "variant": "ema_cross_ls"},
    {"fn": "ema_cross", "params": {"fast": 40, "slow": 480, "long_flat": True}, "variant": "ema_cross_lf"},
    {"fn": "macd_hist", "params": {"fast": 48, "slow": 104, "signal": 36, "long_flat": True}, "variant": "macd_hist"},
    {"fn": "triple_ma", "params": {"fast": 40, "mid": 240, "slow": 2880, "long_flat": True}, "variant": "triple_ma"},
]
