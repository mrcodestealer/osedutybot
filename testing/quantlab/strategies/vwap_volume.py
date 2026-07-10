"""VWAP and volume strategy family for the XAUUSD 5m lab.

All functions follow the engine contract:
    fn(d, **params) -> float array in [-1, +1], same length as d['close'],
    pos[t] decided at close of bar t (engine applies it to the t->t+1 return).

Variants
--------
vwap_fade     : fade deviation from the daily anchored VWAP, exit at VWAP touch
vwap_follow   : trend-follow the daily anchored VWAP (hysteresis band in ATR)
vol_spike_mom : volume-spike momentum, hold H bars after a spike bar
obv_cross     : EMA crossover on On-Balance-Volume
vw_momentum   : sign of volume-weighted rolling momentum
"""
import numpy as np
import pandas as pd

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine


# ---------------------------------------------------------------- helpers

def _daily_vwap(d):
    """Daily anchored VWAP: cum(vol * typical_price) / cum(vol), reset per day_id."""
    tp = (d["high"] + d["low"] + d["close"]) / 3.0
    pv = pd.Series(tp * d["vol"])
    v = pd.Series(d["vol"])
    gid = d["day_id"]
    cum_pv = pv.groupby(gid).cumsum().to_numpy()
    cum_v = v.groupby(gid).cumsum().to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(cum_v > 0, cum_pv / cum_v, np.nan)


def _dev_atr(d, atr_n):
    """(close - daily VWAP) in ATR units. NaN-safe (NaN -> no signal)."""
    vwap = _daily_vwap(d)
    a = engine.atr(d, atr_n)
    with np.errstate(divide="ignore", invalid="ignore"):
        return (d["close"] - vwap) / a


def _last_entry_hold(sig, hold):
    """Position = value of most recent nonzero sig within the last `hold` bars.
    Causal: only looks backwards. New entries refresh the clock."""
    n = len(sig)
    ar = np.arange(n)
    idx = np.where(sig != 0, ar, -1)
    idx = np.maximum.accumulate(idx)
    idx_c = np.maximum(idx, 0)
    return np.where((idx >= 0) & (ar - idx < hold), sig[idx_c], 0.0)


# ---------------------------------------------------------------- variants

def vwap_fade(d, k=1.5, atr_n=96, long_only=False):
    """Fade deviation from daily VWAP: long when close < vwap - k*ATR,
    exit when close touches (crosses back through) VWAP; short symmetric."""
    dev = _dev_atr(d, atr_n)
    dev = np.nan_to_num(dev, nan=0.0)
    zeros = np.zeros(len(dev))
    long_leg = engine.hold_signal(dev < -k, zeros, dev >= 0)
    if long_only:
        return long_leg
    short_leg = engine.hold_signal(zeros, dev > k, dev <= 0)
    return np.clip(long_leg + short_leg, -1.0, 1.0)


def vwap_follow(d, band=0.0, atr_n=96, long_only=False):
    """Follow the daily VWAP with a hysteresis band: go long when close rises
    band*ATR above VWAP, drop the long when close falls back below VWAP.
    band=0 degenerates to sign(close - vwap)."""
    dev = _dev_atr(d, atr_n)
    dev = np.nan_to_num(dev, nan=0.0)
    zeros = np.zeros(len(dev))
    long_leg = engine.hold_signal(dev > band, zeros, dev < 0)
    if long_only:
        return long_leg
    short_leg = engine.hold_signal(zeros, dev < -band, dev > 0)
    return np.clip(long_leg + short_leg, -1.0, 1.0)


def vol_spike_mom(d, vol_n=96, k=3.0, hold=12, long_only=False):
    """Volume-spike momentum: when vol > k * sma(vol, vol_n), take the sign of
    the bar's close-to-close return and hold it for `hold` bars."""
    vma = engine.sma(d["vol"], vol_n)
    c = d["close"]
    ret = np.zeros(len(c))
    ret[1:] = c[1:] - c[:-1]
    spike = np.nan_to_num(d["vol"] > k * vma) & (ret != 0)
    sig = np.where(spike, np.sign(ret), 0.0)
    if long_only:
        sig = np.where(sig > 0, sig, 0.0)
    return _last_entry_hold(sig, hold)


def obv_cross(d, fast=48, slow=288, long_only=False):
    """On-Balance-Volume EMA crossover: OBV = cumsum(sign(dclose) * vol),
    long when ema(OBV, fast) > ema(OBV, slow), short (or flat) otherwise."""
    c = d["close"]
    dc = np.diff(c, prepend=c[0])
    obv = np.cumsum(np.sign(dc) * d["vol"])
    diff = engine.ema(obv, fast) - engine.ema(obv, slow)
    pos = np.sign(np.nan_to_num(diff))
    if long_only:
        pos = np.where(pos > 0, pos, 0.0)
    return pos


def vw_momentum(d, n=48, norm=True, long_only=False):
    """Volume-weighted momentum: sign of sma(ret * vol, n). With norm=True the
    average is divided by sma(vol, n) (a true volume-weighted mean return);
    the sign is identical, but norm=False keeps the raw sum spec."""
    c = d["close"]
    ret = np.zeros(len(c))
    ret[1:] = c[1:] / c[:-1] - 1.0
    num = engine.sma(ret * d["vol"], n)
    if norm:
        den = engine.sma(d["vol"], n)
        with np.errstate(divide="ignore", invalid="ignore"):
            m = num / den
    else:
        m = num
    pos = np.sign(np.nan_to_num(m))
    if long_only:
        pos = np.where(pos > 0, pos, 0.0)
    return pos


# Best configs per variant (selected by TRAIN sharpe only); filled by the
# grid-search run. See results/vwap_volume_grid.csv for the full grid.
BEST = [
    {"fn": "vwap_fade", "params": {"k": 3.0, "atr_n": 288, "long_only": True}, "variant": "vwap_fade"},
    {"fn": "vwap_follow", "params": {"band": 1.0, "atr_n": 288, "long_only": True}, "variant": "vwap_follow"},
    {"fn": "vol_spike_mom", "params": {"vol_n": 288, "k": 5.0, "hold": 48, "long_only": True}, "variant": "vol_spike_mom"},
    {"fn": "obv_cross", "params": {"fast": 96, "slow": 1152, "long_only": True}, "variant": "obv_cross"},
    {"fn": "vw_momentum", "params": {"n": 576, "norm": True, "long_only": True}, "variant": "vw_momentum"},
]
