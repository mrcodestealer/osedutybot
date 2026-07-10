"""Time-series momentum family for the XAUUSD 5m strategy lab.

Variants
--------
a) roc_sign          : pos = sign(close - close[n bars ago]); long-short or long-flat.
b) multi_horizon_vote: vote of ROC signs across a subset of horizons
                       {48, 288, 1440, 4320}; 'hard' = majority sign,
                       'prop' = mean of signs (fractional position).
c) vol_scaled_momentum: pos = clip(roc_n / (rolling_std(ret,n)*sqrt(n)*k), -1, 1),
                       continuous position sizing.
d) deadzone_momentum : sign(roc_n) only when |close-close[-n]| > k*ATR(atr_n)*sqrt(n);
                       inside the dead-zone either flat (hold=False) or keep
                       the previous position (hold=True) - filters chop.

All functions follow the engine contract: causal, return float array in
[-1, +1] aligned with d['close']; NaN warmup is treated as flat by the engine.
"""
import numpy as np
import pandas as pd

import engine


def _roc(c, n):
    """Causal n-bar rate of change (fractional). NaN during warmup."""
    out = np.full(len(c), np.nan)
    out[n:] = c[n:] / c[:-n] - 1.0
    return out


# ------------------------------------------------------------------ variant a
def roc_sign(d, n=288, long_flat=False):
    pos = np.sign(_roc(d["close"], n))
    if long_flat:
        pos = np.clip(pos, 0.0, 1.0)
    return pos


# ------------------------------------------------------------------ variant b
def multi_horizon_vote(d, horizons=(48, 288, 1440, 4320), mode="hard",
                       long_flat=False):
    c = d["close"]
    votes = np.zeros(len(c))
    for n in horizons:
        votes += np.sign(np.nan_to_num(_roc(c, n)))  # warmup votes 0 (causal)
    if mode == "hard":
        pos = np.sign(votes)
    else:  # 'prop': fractional position = mean vote
        pos = votes / float(len(horizons))
    if long_flat:
        pos = np.clip(pos, 0.0, 1.0)
    return pos


# ------------------------------------------------------------------ variant c
def vol_scaled_momentum(d, n=288, k=1.0, long_flat=False):
    c = d["close"]
    m = len(c)
    ret = np.zeros(m)
    ret[1:] = c[1:] / c[:-1] - 1.0
    sd = engine.rolling_std(ret, n)
    with np.errstate(divide="ignore", invalid="ignore"):
        raw = _roc(c, n) / (sd * np.sqrt(n) * k)
    pos = np.clip(raw, -1.0, 1.0)
    if long_flat:
        pos = np.clip(pos, 0.0, 1.0)
    return pos


# ------------------------------------------------------------------ variant d
def deadzone_momentum(d, n=96, k=1.0, atr_n=96, hold=False, long_flat=False):
    c = d["close"]
    m = len(c)
    diff = np.full(m, np.nan)
    diff[n:] = c[n:] - c[:-n]                      # price-unit n-bar move
    thresh = k * engine.atr(d, atr_n) * np.sqrt(n)  # sqrt-scaled ATR band
    active = np.abs(diff) > thresh                  # NaN diff -> False
    raw = np.sign(diff)
    if hold:
        sig = np.where(active, raw, np.nan)
        pos = pd.Series(sig).ffill().fillna(0.0).to_numpy()
    else:
        pos = np.where(active, raw, 0.0)
    pos = np.nan_to_num(pos)
    if long_flat:
        pos = np.clip(pos, 0.0, 1.0)
    return pos


# Best train-Sharpe config per variant (filled in by the grid search runner).
BEST = [
    {"fn": "roc_sign", "params": {"n": 4320, "long_flat": False}, "variant": "roc_sign_ls"},
    {"fn": "roc_sign", "params": {"n": 4320, "long_flat": True}, "variant": "roc_sign_lf"},
    {"fn": "multi_horizon_vote", "params": {"horizons": [1440, 4320], "mode": "prop", "long_flat": True}, "variant": "multi_horizon_vote"},
    {"fn": "vol_scaled_momentum", "params": {"n": 4320, "k": 0.5, "long_flat": True}, "variant": "vol_scaled"},
    {"fn": "deadzone_momentum", "params": {"n": 12, "k": 2.0, "atr_n": 96, "hold": True, "long_flat": True}, "variant": "deadzone"},
]
