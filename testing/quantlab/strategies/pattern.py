"""Price-action pattern strategies for XAUUSD 5m.

Engine contract: fn(d, **params) -> np.ndarray, positions in [-1, +1],
same length as d['close']. pos[t] is decided at the close of bar t and the
engine applies it to the t -> t+1 return. All computations here are causal.

Variants
--------
ha_trend    : Heikin-Ashi trend following. Long after S consecutive green HA
              candles, short after S red (mode='ls') or long/flat (mode='lf'),
              exit on HA color flip.
streak_rev  : Streak reversal. After S consecutive down closes go long for H
              bars (mode='long'); mode='ls' adds the symmetric short after S
              consecutive up closes.
gap_open    : Overnight gap across day boundaries. When the first bar of a new
              day opens > k*ATR away from the prior day's last close, fade
              (mode='fade') or follow (mode='follow') the gap until the close
              crosses back through the prior close (gap fill) or the day ends
              (causal end-of-day time rule; sessions here end ~23:50).
range_exp   : Range expansion. When a bar's high-low range exceeds k*ATR of
              the previous bar, follow the bar's direction for H bars.
              mode='ls' both directions, mode='lf' long-only.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine


# ------------------------------------------------------------------ helpers

def _runlen(b):
    """Length of the consecutive True run ending at each bar (0 where False)."""
    n = len(b)
    b = np.asarray(b, dtype=bool)
    last_false = np.maximum.accumulate(np.where(~b, np.arange(n), -1))
    return np.where(b, np.arange(n) - last_false, 0)


def _heikin_ashi(d):
    """Causal Heikin-Ashi open/close.

    ha_close[t] = (o+h+l+c)/4 of bar t
    ha_open[t]  = (ha_open[t-1] + ha_close[t-1]) / 2, computed via an
    alpha=0.5 recursive EWM of ha_close shifted one bar (identical recursion).
    """
    o, h, l, c = d["open"], d["high"], d["low"], d["close"]
    ha_c = (o + h + l + c) / 4.0
    ha_o = pd.Series(ha_c).shift(1).ewm(alpha=0.5, adjust=False).mean().to_numpy()
    return ha_o, ha_c


# ---------------------------------------------------------------- strategies

def ha_trend(d, S=3, mode="ls"):
    """Long after S consecutive green HA candles, short after S red,
    exit (flat) on HA color flip. mode='lf' disables shorts."""
    ha_o, ha_c = _heikin_ashi(d)
    with np.errstate(invalid="ignore"):
        green = ha_c > ha_o  # NaN warmup -> False (treated as red; harmless)
    n = len(green)
    flip = np.concatenate([[False], green[1:] != green[:-1]])
    last_flip = np.maximum.accumulate(np.where(flip, np.arange(n), 0))
    run = np.arange(n) - last_flip + 1  # length of current same-color run
    ent_l = green & (run >= S)
    ent_s = (~green) & (run >= S)
    if mode == "lf":
        ent_s = np.zeros(n, dtype=bool)
    return engine.hold_signal(ent_l, ent_s, flip)


def streak_rev(d, S=4, H=12, mode="long"):
    """After S consecutive down closes go long for H bars.
    mode='ls' adds symmetric shorts after S consecutive up closes."""
    c = d["close"]
    dc = np.diff(c, prepend=c[0])
    trig_l = _runlen(dc < 0) >= S
    pos = pd.Series(trig_l.astype(float)).rolling(H, min_periods=1).max().to_numpy()
    if mode == "ls":
        trig_s = _runlen(dc > 0) >= S
        pos = pos - pd.Series(trig_s.astype(float)).rolling(H, min_periods=1).max().to_numpy()
    return pos


def gap_open(d, k=1.0, mode="fade", atr_n=14):
    """Overnight-gap trade. On the first bar of each new day, if
    |open - prior day last close| > k * ATR(atr_n) (ATR as of the prior bar),
    fade (-sign(gap)) or follow (+sign(gap)) the gap. Hold until the day's
    close crosses back through the prior close (gap filled) or until the
    end-of-day time window (hour>=23 or 22:45+; sessions end ~23:50)."""
    c, o = d["close"], d["open"]
    day = pd.Series(d["day_id"])
    new_day = np.concatenate([[False], d["day_id"][1:] != d["day_id"][:-1]])
    a = engine.atr(d, atr_n)
    prev_c = np.concatenate([[np.nan], c[:-1]])
    prev_a = np.concatenate([[np.nan], a[:-1]])
    gap_b = pd.Series(np.where(new_day, o - prev_c, np.nan)).groupby(day).transform("first").to_numpy()
    thr_b = pd.Series(np.where(new_day, k * prev_a, np.nan)).groupby(day).transform("first").to_numpy()
    ref_b = pd.Series(np.where(new_day, prev_c, np.nan)).groupby(day).transform("first").to_numpy()
    with np.errstate(invalid="ignore"):
        event = np.abs(gap_b) > thr_b  # NaN -> False (e.g. very first day)
        gdir = np.sign(gap_b)
        cmax = pd.Series(c).groupby(day).cummax().to_numpy()
        cmin = pd.Series(c).groupby(day).cummin().to_numpy()
        filled = np.where(gap_b > 0, cmin <= ref_b, cmax >= ref_b)
    eod = (d["hour"] >= 23) | ((d["hour"] == 22) & (d["minute"] >= 45))
    sgn = -1.0 if mode == "fade" else 1.0
    return np.where(event & ~filled & ~eod, sgn * np.nan_to_num(gdir), 0.0)


def range_exp(d, k=2.0, H=12, mode="ls", atr_n=14):
    """When bar range (high-low) exceeds k * ATR(atr_n) of the previous bar,
    follow the bar's close-vs-open direction for H bars.
    mode='lf' takes only the long side."""
    h, l, c, o = d["high"], d["low"], d["close"], d["open"]
    a = engine.atr(d, atr_n)
    prev_a = np.concatenate([[np.nan], a[:-1]])
    with np.errstate(invalid="ignore"):
        expand = (h - l) > k * prev_a
    trig_l = expand & (c > o)
    pos = pd.Series(trig_l.astype(float)).rolling(H, min_periods=1).max().to_numpy()
    if mode == "ls":
        trig_s = expand & (c < o)
        pos = pos - pd.Series(trig_s.astype(float)).rolling(H, min_periods=1).max().to_numpy()
    return pos


# Best train-selected config per variant (train sharpe, 1bp/side; grid in
# results/pattern_grid.csv). Note: streak_rev's train-best (S=3, H=144)
# degenerates to ~100% long exposure (a 3-down-close streak nearly always
# occurs within any 144-bar window), i.e. it mostly captures gold's drift.
BEST = [
    {"fn": "ha_trend", "params": {"S": 8, "mode": "lf"}, "variant": "ha_trend"},
    {"fn": "streak_rev", "params": {"S": 3, "H": 144, "mode": "long"}, "variant": "streak_rev"},
    {"fn": "gap_open", "params": {"k": 1.0, "mode": "fade", "atr_n": 14}, "variant": "gap_open"},
    {"fn": "range_exp", "params": {"k": 6.0, "H": 48, "mode": "lf", "atr_n": 14}, "variant": "range_exp"},
]
