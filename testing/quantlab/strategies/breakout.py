"""Channel/level breakout family for XAUUSD 5m.

All functions follow the engine contract:
    fn(d, **params) -> np.ndarray of positions in [-1, +1], same length as d['close'].
pos[t] is decided at the close of bar t (engine applies it to the t -> t+1 return).
All indicators are causal (trailing rolling windows, prior-bar channel levels,
prior-day extremes).

Variants:
    donchian  - close breaks the previous bar's rolling max(high,n)/min(low,n)
                channel. exit_mode='opposite' (stop-and-reverse / flat on the
                opposite break) or 'mid' (exit at mid-channel touch).
    bollinger - close breaks SMA(n) +/- k*std(n) band. exit_mode='band' (hold
                only while outside the band) or 'mean' (hold until mean touch).
    prevday   - close breaks the previous calendar day's high/low (via day_id),
                optional buffer in bp. exit_mode='newday' (flat at the first
                bar of the next day) or 'opposite'.
    squeeze   - volatility squeeze: Bollinger bandwidth percentile-rank over a
                trailing lookback is low (squeeze on prior bar), then close
                exits the band. exit_mode='mean' (until mean touch) or
                'reenter' (hold while outside the band).

mode='ls' -> long-short, mode='lf' -> long-flat (gold drifts up long-term).
"""
import os
import sys

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_HERE)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

import engine


def _shift1(x):
    """Previous-bar value (causal shift by one), NaN for bar 0."""
    out = np.empty(len(x), dtype=np.float64)
    out[0] = np.nan
    out[1:] = x[:-1]
    return out


def _two_sided(el, es, xl, xs, mode):
    """Combine independent long/short legs built with engine.hold_signal.

    el/es: long/short entry flags.  xl/xs: long/short exit flags (must already
    include the opposite entry so a new opposite entry kills the open leg).
    """
    z = np.zeros(len(el), dtype=bool)
    long_pos = engine.hold_signal(el, z, xl)
    if mode == "lf":
        return long_pos
    short_pos = engine.hold_signal(z, es, xs)
    return long_pos + short_pos


def donchian(d, n=96, exit_mode="opposite", mode="ls"):
    """Donchian channel breakout of the previous bar's n-bar high/low channel."""
    close = d["close"]
    up = _shift1(engine.rolling_max(d["high"], n))
    lo = _shift1(engine.rolling_min(d["low"], n))
    el = close > up          # NaN warmup compares False
    es = close < lo
    z = np.zeros(len(close), dtype=bool)
    if exit_mode == "opposite":
        if mode == "lf":
            return engine.hold_signal(el, z, es)
        return engine.hold_signal(el, es, z)
    # mid-channel exit
    midc = 0.5 * (up + lo)
    xl = (close < midc) | es
    xs = (close > midc) | el
    return _two_sided(el, es, xl, xs, mode)


def bollinger(d, n=96, k=2.0, exit_mode="band", mode="ls"):
    """Bollinger band breakout: close outside SMA(n) +/- k*std(n)."""
    close = d["close"]
    m = engine.sma(close, n)
    s = engine.rolling_std(close, n)
    upper = m + k * s
    lower = m - k * s
    el = close > upper
    es = close < lower
    if exit_mode == "band":
        # hold only while outside the band
        pos = np.where(el, 1.0, np.where(es, -1.0, 0.0))
        if mode == "lf":
            pos = np.maximum(pos, 0.0)
        return pos
    # 'mean': hold until the mean is touched (or the opposite band breaks)
    xl = (close <= m) | es
    xs = (close >= m) | el
    return _two_sided(el, es, xl, xs, mode)


def prevday(d, buffer_bp=0.0, exit_mode="newday", mode="ls"):
    """Previous-day high/low breakout using day_id (prior day extremes are
    fully known once the new day starts, so levels are causal)."""
    close = d["close"]
    day_id = d["day_id"]
    n = len(close)
    new_day = np.concatenate([[True], day_id[1:] != day_id[:-1]])
    starts = np.flatnonzero(new_day)
    day_high = np.maximum.reduceat(d["high"], starts)
    day_low = np.minimum.reduceat(d["low"], starts)
    ph = np.full(n, np.nan)
    pl = np.full(n, np.nan)
    m1 = day_id >= 1
    ph[m1] = day_high[day_id[m1] - 1]
    pl[m1] = day_low[day_id[m1] - 1]
    b = buffer_bp * 1e-4
    el = close > ph * (1.0 + b)
    es = close < pl * (1.0 - b)
    z = np.zeros(n, dtype=bool)
    if exit_mode == "newday":
        if mode == "lf":
            return engine.hold_signal(el, z, new_day | es)
        return engine.hold_signal(el, es, new_day)
    # 'opposite'
    if mode == "lf":
        return engine.hold_signal(el, z, es)
    return engine.hold_signal(el, es, z)


def squeeze(d, n=96, pct=20.0, lookback=2880, k=2.0, exit_mode="mean", mode="ls"):
    """Volatility squeeze breakout: bandwidth percentile-rank (trailing
    `lookback` bars) was <= pct on the prior bar, then close exits the band."""
    import pandas as pd
    close = d["close"]
    m = engine.sma(close, n)
    s = engine.rolling_std(close, n)
    upper = m + k * s
    lower = m - k * s
    with np.errstate(divide="ignore", invalid="ignore"):
        bw = s / m
    rank = pd.Series(bw).rolling(lookback).rank(pct=True).to_numpy()
    sq_prev = _shift1(rank) <= pct / 100.0      # NaN -> False
    el = sq_prev & (close > upper)
    es = sq_prev & (close < lower)
    if exit_mode == "mean":
        xl = (close <= m) | es
        xs = (close >= m) | el
    else:  # 'reenter': hold only while price stays outside the band
        xl = (close < upper) | es
        xs = (close > lower) | el
    return _two_sided(el, es, xl, xs, mode)


# Best train-sharpe config per variant (filled by the grid search; see
# results/breakout_grid.csv for the full sweep).
BEST = [
    {"fn": "donchian", "params": {"n": 576, "exit_mode": "mid", "mode": "lf"}, "variant": "donchian"},
    {"fn": "bollinger", "params": {"n": 576, "k": 1.5, "exit_mode": "mean", "mode": "lf"}, "variant": "bollinger"},
    {"fn": "prevday", "params": {"buffer_bp": 0.0, "exit_mode": "opposite", "mode": "lf"}, "variant": "prevday"},
    {"fn": "squeeze", "params": {"n": 288, "pct": 20.0, "lookback": 2880, "k": 2.0, "exit_mode": "mean", "mode": "lf"}, "variant": "squeeze"},
]
