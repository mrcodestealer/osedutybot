"""Mean-reversion strategy family for XAUUSD 5m bars.

Variants
--------
boll_fade        : Bollinger-band fade. Long when close < SMA(n) - k*std(n),
                   short when close > SMA(n) + k*std(n); each side exits when
                   close crosses back through the SMA(n).
zscore_fade      : z-score reversion. Long when z(n) < -zin, exit when
                   z > -zout; short when z > +zin, exit when z < +zout.
rsi_fade         : RSI fade. Long when RSI(p) < lo, exit at RSI >= 50;
                   short when RSI(p) > hi (default 100-lo), exit at RSI <= 50.
rsi_fade_maxhold : rsi_fade entries taken at condition ONSET (condition goes
                   false -> true) with an extra time stop: force exit
                   max_hold bars after the (latest) onset, in addition to
                   the 50-cross exit.

All variants accept long_only=True (long-flat: short side disabled).
Engine contract: pos[t] is decided at the close of bar t and is applied by
the engine to the t -> t+1 return. All functions below are causal (rolling /
cumulative operators only).
"""
import os
import sys

import numpy as np

_QL = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _QL not in sys.path:
    sys.path.insert(0, _QL)
import engine


def _prev(flag):
    """Previous bar's value of a boolean flag (False for the first bar)."""
    return np.concatenate([[False], flag[:-1]])


def boll_fade(d, n=96, k=2.0, long_only=False):
    c = d["close"]
    m = engine.sma(c, n)
    s = engine.rolling_std(c, n)
    z0 = np.zeros(len(c), dtype=bool)
    # long side: enter below lower band, exit on cross back up through mean
    pos = engine.hold_signal(c < m - k * s, z0, c >= m)
    if not long_only:
        # short side: enter above upper band, exit on cross back down
        # (a short entry implies c >= m, which force-exits any long, so the
        #  two one-sided state machines can never overlap)
        pos = pos - engine.hold_signal(c > m + k * s, z0, c <= m)
    return pos


def zscore_fade(d, n=96, zin=2.0, zout=0.5, long_only=False):
    z = engine.zscore(d["close"], n)
    z = np.where(np.isfinite(z), z, np.nan)
    z0 = np.zeros(len(z), dtype=bool)
    pos = engine.hold_signal(z < -zin, z0, z > -zout)
    if not long_only:
        pos = pos - engine.hold_signal(z > zin, z0, z < zout)
    return pos


def rsi_fade(d, p=2, lo=10.0, hi=None, long_only=False):
    if hi is None:
        hi = 100.0 - lo
    r = engine.rsi(d["close"], p)
    z0 = np.zeros(len(r), dtype=bool)
    pos = engine.hold_signal(r < lo, z0, r >= 50.0)
    if not long_only:
        pos = pos - engine.hold_signal(r > hi, z0, r <= 50.0)
    return pos


def _time_stop_state(entries_onset, exits_base, max_hold):
    """One-sided (+1/flat) state machine with a max_hold-bar time stop
    counted from the latest entry onset. Fully vectorized and causal."""
    n = len(entries_onset)
    ar = np.arange(n)
    idx = np.where(entries_onset, ar, -1)
    idx = np.maximum.accumulate(idx)
    bars_since = np.where(idx >= 0, ar - idx, 0)
    time_ex = (idx >= 0) & (bars_since >= max_hold)
    z0 = np.zeros(n, dtype=bool)
    return engine.hold_signal(entries_onset, z0, exits_base | time_ex)


def rsi_fade_maxhold(d, p=2, lo=10.0, hi=None, max_hold=12, long_only=False):
    if hi is None:
        hi = 100.0 - lo
    r = engine.rsi(d["close"], p)
    cond_l = r < lo
    pos = _time_stop_state(cond_l & ~_prev(cond_l), r >= 50.0, max_hold)
    if not long_only:
        cond_s = r > hi
        pos = pos - _time_stop_state(cond_s & ~_prev(cond_s), r <= 50.0, max_hold)
    return pos


# Train-sharpe-selected best config per variant (grid: results/meanrev_grid.csv).
# NOTE: the family is weak overall on this data/cost model - only 7/248 grid
# configs had positive train sharpe, all of them low-trade-count RSI configs.
BEST = [
    {"fn": "boll_fade", "variant": "boll_fade",
     "params": {"n": 48, "k": 2.5, "long_only": True}},
    {"fn": "zscore_fade", "variant": "zscore_fade",
     "params": {"n": 96, "zin": 2.5, "zout": 0.25, "long_only": True}},
    {"fn": "rsi_fade", "variant": "rsi_fade",
     "params": {"p": 14, "lo": 5.0, "long_only": True}},
    {"fn": "rsi_fade_maxhold", "variant": "rsi_fade_maxhold",
     "params": {"p": 14, "lo": 10.0, "max_hold": 12, "long_only": True}},
]
