"""Regime-switching and ensemble strategies for XAUUSD 5m.

Family: "ensemble". Four variants:
  (a) regime_switch     - efficiency-ratio gate: trend (EMA cross) in trending
                          regimes, z-score mean reversion in choppy regimes.
  (b) vol_target_trend  - EMA-cross trend sign scaled by min(1, target/realized vol).
  (c) vote5             - 5-signal vote (EMA cross, Donchian side, RSI-mom, ROC,
                          daily-VWAP side); act only when |mean sign| >= thresh.
  (d) drift_bear_filter - always long (gold drift) except a bear filter
                          (close < SMA(n) and ROC < -k*ATR) sends it flat/short,
                          with hysteresis re-entry when price recovers above SMA.

All functions follow the engine contract: fn(d, **params) -> pos array in
[-1, +1], causal (pos[t] uses only bars <= t). Engine applies next-bar execution.
"""
import os
import sys

import numpy as np
import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_HERE)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)
import engine  # noqa: E402

BARS_PER_YEAR = 66000.0  # ~1.41M bars / 21.4 years of XAUUSD 5m


# ------------------------------------------------------------------ helpers

def _shift(x, n):
    out = np.full(len(x), np.nan)
    if n < len(x):
        out[n:] = x[:-n] if n > 0 else x
    return out


def _efficiency_ratio(close, n):
    """Kaufman efficiency ratio: |close - close[-n]| / sum(|dclose|) over n."""
    s = pd.Series(close)
    num = s.diff(n).abs().to_numpy()
    denom = s.diff().abs().rolling(n).sum().to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        return num / denom


def _trend_sign(close, fast, slow):
    ef = engine.ema(close, fast)
    es = engine.ema(close, slow)
    return np.where(ef > es, 1.0, -1.0)


def _mr_pos(close, z_n, z_entry):
    """Z-score mean reversion with zero-cross exit (stateful, vectorized)."""
    z = engine.zscore(close, z_n)
    zp = np.concatenate([[np.nan], z[:-1]])
    entries_long = z <= -z_entry
    entries_short = z >= z_entry
    valid = ~np.isnan(z) & ~np.isnan(zp)
    exits = (zp * z <= 0) & valid
    return engine.hold_signal(entries_long, entries_short, exits)


def _daily_vwap(d):
    """Session (per-day) anchored VWAP, causal (cumulative within day)."""
    pv = d["close"] * d["vol"]
    day = np.asarray(d["day_id"])
    cum_pv = pd.Series(pv).groupby(day).cumsum().to_numpy()
    cum_v = pd.Series(d["vol"]).groupby(day).cumsum().to_numpy()
    return np.where(cum_v > 0, cum_pv / np.maximum(cum_v, 1e-12), d["close"])


# ------------------------------------------------------------------ variants

def regime_switch(d, er_n=48, er_thr=0.35, fast=12, slow=96,
                  z_n=48, z_entry=1.5, long_flat=False):
    """(a) ER-gated regime switch: trend signal when ER >= thr, else z-score MR."""
    close = d["close"]
    er = _efficiency_ratio(close, er_n)
    trend = _trend_sign(close, fast, slow)
    mr = _mr_pos(close, z_n, z_entry)
    pos = np.where(er >= er_thr, trend, mr)
    pos[np.isnan(er)] = 0.0
    if long_flat:
        pos = np.clip(pos, 0.0, 1.0)
    return pos


def vol_target_trend(d, fast=24, slow=192, vol_n=288, target_ann=0.10,
                     long_flat=False):
    """(b) EMA-cross trend sign scaled by min(1, target_vol / realized_vol)."""
    close = d["close"]
    ret = pd.Series(close).pct_change().to_numpy()
    realized = engine.rolling_std(ret, vol_n) * np.sqrt(BARS_PER_YEAR)
    with np.errstate(divide="ignore", invalid="ignore"):
        scale = np.minimum(1.0, target_ann / np.maximum(realized, 1e-12))
    scale = np.round(scale * 20.0) / 20.0  # 0.05 steps to limit micro-turnover
    sign = _trend_sign(close, fast, slow)
    if long_flat:
        sign = np.clip(sign, 0.0, 1.0)
    pos = sign * scale
    pos[np.isnan(realized)] = 0.0
    return pos


def vote5(d, scale=1.0, thresh=0.6, rsi_band=10.0, long_flat=False):
    """(c) 5-signal vote: EMA cross + Donchian side + RSI-mom + ROC + VWAP side.

    Lookbacks are base values multiplied by `scale`. Position = sign of the
    mean vote, taken only when |mean| >= thresh, else flat.
    """
    close = d["close"]
    f = lambda n: max(2, int(round(n * scale)))  # noqa: E731

    s1 = np.sign(np.nan_to_num(
        engine.ema(close, f(12)) - engine.ema(close, f(96))))
    n_don = f(96)
    mid = (engine.rolling_max(d["high"], n_don)
           + engine.rolling_min(d["low"], n_don)) / 2.0
    s2 = np.sign(np.nan_to_num(close - mid))
    r = engine.rsi(close, f(14))
    s3 = np.where(r > 50.0 + rsi_band, 1.0,
                  np.where(r < 50.0 - rsi_band, -1.0, 0.0))
    s3 = np.nan_to_num(s3)
    s4 = np.sign(np.nan_to_num(close - _shift(close, f(48))))
    s5 = np.sign(np.nan_to_num(close - _daily_vwap(d)))

    vote = (s1 + s2 + s3 + s4 + s5) / 5.0
    pos = np.where(vote >= thresh, 1.0, np.where(vote <= -thresh, -1.0, 0.0))
    if long_flat:
        pos = np.clip(pos, 0.0, 1.0)
    return pos


def drift_bear_filter(d, sma_n=1152, roc_n=96, k=2.0, bear_short=False):
    """(d) Buy-and-hold with crash protection. Long by default; when the bear
    filter trips (close < SMA(sma_n) AND roc_n-bar change < -k*ATR14) go flat
    (or short). Re-enter long only when close recovers above the SMA."""
    close = d["close"]
    ma = engine.sma(close, sma_n)
    a = engine.atr(d, 14)
    roc = close - _shift(close, roc_n)
    bear = (close < ma) & (roc < -k * a)          # NaN warmup -> False
    entries_long = np.nan_to_num(close > ma)
    n = len(close)
    if bear_short:
        entries_short = bear
        exits = np.zeros(n, dtype=bool)
    else:
        entries_short = np.zeros(n, dtype=bool)
        exits = bear
    return engine.hold_signal(entries_long, entries_short, exits)


# ------------------------------------------------------------------ best configs
# Train-sharpe-selected grid winners (see results/ensemble_grid.csv).
# All pass engine.causality_check. Val sharpe @1bp: regime_switch -0.511,
# vol_target_trend 0.947, vote5 -1.153, drift_bear_filter 0.385.
BEST = [
    {"fn": "regime_switch", "variant": "regime_switch",
     "params": {"er_n": 96, "er_thr": 0.45, "fast": 12, "slow": 96,
                "z_n": 96, "z_entry": 2.0, "long_flat": True}},
    {"fn": "vol_target_trend", "variant": "vol_target_trend",
     "params": {"fast": 48, "slow": 384, "vol_n": 288, "target_ann": 0.18,
                "long_flat": True}},
    {"fn": "vote5", "variant": "vote5",
     "params": {"scale": 4.0, "thresh": 1.0, "rsi_band": 15.0,
                "long_flat": True}},
    {"fn": "drift_bear_filter", "variant": "drift_bear_filter",
     "params": {"sma_n": 1152, "roc_n": 288, "k": 3.0, "bear_short": False}},
    # High-frequency eligible config (val avg_hold 19.4 bars, ~1960 trades/yr,
    # val sharpe 0.769 @1bp): best train sharpe among HF-eligible configs.
    {"fn": "vol_target_trend", "variant": "vol_target_trend_hf",
     "params": {"fast": 48, "slow": 384, "vol_n": 96, "target_ann": 0.12,
                "long_flat": True}},
]
