"""Classic oscillator strategies for XAUUSD 5m (engine contract).

Variants
--------
stoch_cross     : Stochastic %K(n)/%D(d_n) cross while %D is inside the
                  OS/OB zone; mean-reversion exit at the 50 midline.
cci_osc         : CCI(n) crossing +/-thr, either momentum (trade with the
                  break, exit on zero-cross) or fade (trade against the
                  extreme, exit on zero-cross).
willr_thresh    : Williams %R(n) level thresholds, fade or momentum, exit
                  at the -50 midline.
rsi_hysteresis  : RSI(n) momentum with hysteresis band: long > ub,
                  short < lb (= 100 - ub), hold previous in between.

All variants accept mode="ls" (long-short) or "lf" (long-flat: short
signals are flattened to 0).  Every function is causal: pos[t] depends
only on bars <= t; the engine applies the signal to the t -> t+1 return.
"""
import os
import sys

import numpy as np
import pandas as pd

_PARENT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)
import engine  # noqa: E402


# ------------------------------------------------------------------ helpers

def _cross_up(a, b):
    """True at t when a crossed above b between t-1 and t (causal)."""
    above = a > b
    below_eq = a <= b
    out = np.zeros(len(np.asarray(a)), dtype=bool)
    out[1:] = above[1:] & below_eq[:-1]
    return out


def _cross_dn(a, b):
    below = a < b
    above_eq = a >= b
    out = np.zeros(len(np.asarray(a)), dtype=bool)
    out[1:] = below[1:] & above_eq[:-1]
    return out


def _two_sided(e_long, x_long, e_short, x_short, mode):
    """Combine independent long / short legs built with engine.hold_signal."""
    z = np.zeros(len(e_long))
    lp = engine.hold_signal(e_long, z, x_long)          # in {0, +1}
    sp = engine.hold_signal(z, e_short, x_short)        # in {0, -1}
    pos = np.clip(lp + sp, -1.0, 1.0)
    if mode == "lf":
        pos = np.clip(pos, 0.0, 1.0)
    return pos


def _stoch_k(d, n):
    hh = engine.rolling_max(d["high"], n)
    ll = engine.rolling_min(d["low"], n)
    rng = hh - ll
    with np.errstate(divide="ignore", invalid="ignore"):
        return 100.0 * (d["close"] - ll) / np.where(rng == 0, np.nan, rng)


# ---------------------------------------------------------------- variants

def stoch_cross(d, n=14, d_n=3, lo=20.0, hi=80.0, mode="ls"):
    """Stochastic %K/%D cross inside OB/OS zones, exit at the 50 midline.

    Long : %K crosses above %D while %D < lo, exit when %K > 50.
    Short: %K crosses below %D while %D > hi, exit when %K < 50.
    """
    k = _stoch_k(d, n)
    kd = engine.sma(k, d_n)
    e_long = _cross_up(k, kd) & (kd < lo)
    e_short = _cross_dn(k, kd) & (kd > hi)
    x_long = k > 50.0
    x_short = k < 50.0
    return _two_sided(e_long, x_long, e_short, x_short, mode)


def cci_osc(d, n=20, thr=100.0, style="momentum", mode="ls"):
    """CCI(n) vs +/-thr.  Mean deviation is approximated by the rolling SMA
    of |TP - SMA(TP, n)| (vectorized, causal).

    momentum: long on cross above +thr (exit CCI < 0),
              short on cross below -thr (exit CCI > 0).
    fade    : long on cross below -thr (exit CCI > 0),
              short on cross above +thr (exit CCI < 0).
    """
    tp = (d["high"] + d["low"] + d["close"]) / 3.0
    m = engine.sma(tp, n)
    md = engine.sma(np.abs(tp - m), n)
    with np.errstate(divide="ignore", invalid="ignore"):
        cci = (tp - m) / (0.015 * np.where(md == 0, np.nan, md))
    if style == "momentum":
        e_long, x_long = _cross_up(cci, thr), cci < 0.0
        e_short, x_short = _cross_dn(cci, -thr), cci > 0.0
    else:  # fade
        e_long, x_long = _cross_dn(cci, -thr), cci > 0.0
        e_short, x_short = _cross_up(cci, thr), cci < 0.0
    return _two_sided(e_long, x_long, e_short, x_short, mode)


def willr_thresh(d, n=14, lo=-80.0, hi=-20.0, style="fade", mode="ls"):
    """Williams %R(n) level thresholds with -50 midline exit.

    fade    : long while %R < lo (oversold), exit %R > -50;
              short while %R > hi (overbought), exit %R < -50.
    momentum: long while %R > hi (near highs), exit %R < -50;
              short while %R < lo (near lows), exit %R > -50.
    """
    r = _stoch_k(d, n) - 100.0  # Williams %R = stoch %K - 100
    mid = -50.0
    if style == "fade":
        e_long, x_long = r < lo, r > mid
        e_short, x_short = r > hi, r < mid
    else:  # momentum
        e_long, x_long = r > hi, r < mid
        e_short, x_short = r < lo, r > mid
    return _two_sided(e_long, x_long, e_short, x_short, mode)


def rsi_hysteresis(d, n=14, ub=55.0, mode="ls"):
    """RSI momentum with hysteresis: long when RSI(n) > ub, short when
    RSI(n) < lb = 100 - ub, hold previous position in between."""
    r = engine.rsi(d["close"], n)
    lb = 100.0 - ub
    raw = np.where(r > ub, 1.0, np.where(r < lb, -1.0, np.nan))
    pos = pd.Series(raw).ffill().fillna(0.0).to_numpy()
    if mode == "lf":
        pos = np.clip(pos, 0.0, 1.0)
    return pos


# Best train-Sharpe config per variant (selected on TRAIN sharpe only,
# 1bp/side; see results/oscillator_grid.csv for the full grid).
BEST = [
    {"fn": "stoch_cross", "variant": "stochastic",
     "params": {"n": 56, "d_n": 5, "lo": 10.0, "hi": 90.0, "mode": "lf"}},
    {"fn": "cci_osc", "variant": "cci",
     "params": {"n": 96, "thr": 200.0, "style": "momentum", "mode": "lf"}},
    {"fn": "willr_thresh", "variant": "williams_r",
     "params": {"n": 192, "lo": -90.0, "hi": -10.0, "style": "momentum", "mode": "lf"}},
    {"fn": "rsi_hysteresis", "variant": "rsi_hysteresis",
     "params": {"n": 56, "ub": 60.0, "mode": "lf"}},
]
