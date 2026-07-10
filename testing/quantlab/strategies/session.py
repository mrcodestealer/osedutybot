"""Intraday session / time-of-day strategies for XAUUSD 5m.

All functions follow the engine contract:
    fn(d, **params) -> np.ndarray of positions in [-1, +1], same length as d['close'].
pos[t] is decided at the close of bar t and applied by the engine to the
t -> t+1 return. Everything here is clock/past-data based (hour of bar t,
bar index within the current day, running intraday extremes), so signals
are strictly causal. Day-end exits use the wall clock (hour >= exit_hour)
rather than "last bar of day", because the latter is not knowable in
real time (early closes / missing bars).

Server time is MT5 broker time (GMT+2/+3): days span ~01:00-23:55.
"""
import numpy as np
import pandas as pd


# ------------------------------------------------------------------ helpers

def _bar_in_day(d):
    """0-based bar index within each calendar day (causal)."""
    day_id = np.asarray(d["day_id"])
    n = len(day_id)
    starts = np.flatnonzero(np.concatenate([[True], day_id[1:] != day_id[:-1]]))
    dref = day_id - day_id[0]  # consecutive 0..D-1 for any prefix slice
    return np.arange(n) - starts[dref], starts, dref


def _day_open(d):
    """Open of the first bar of the current day (causal)."""
    _, starts, dref = _bar_in_day(d)
    return d["open"][starts[dref]]


def _ffill_within_day(vals, day_id):
    """Forward-fill vals within each day; leading NaNs of a day -> 0."""
    s = pd.Series(vals).groupby(pd.Series(day_id)).ffill()
    return np.nan_to_num(s.to_numpy(), nan=0.0)


# ---------------------------------------------------------------- (a) hour band

def hour_band(d, h1=8, band_len=8, short_opposite=False):
    """Long while hour in [h1, h1+band_len) mod 24; optionally short the
    opposite band [h1+12, h1+12+band_len) mod 24, else flat outside."""
    hour = d["hour"]
    in_long = ((hour - h1) % 24) < band_len
    pos = in_long.astype(np.float64)
    if short_opposite:
        in_short = ((hour - (h1 + 12)) % 24) < band_len
        pos = pos - (in_short & ~in_long).astype(np.float64)
    return pos


# ------------------------------------------------- (b) opening-range breakout

def orb(d, k_bars=6, long_short=True, exit_hour=23):
    """Opening-range breakout: the first k_bars of each day define a high/low
    range. After the range is set, go with the latest close beyond the range
    (long above the OR-high, short below the OR-low). Flat from exit_hour."""
    bid, _, dref = _bar_in_day(d)
    high, low, close = d["high"], d["low"], d["close"]
    day = pd.Series(dref)

    in_or = bid < k_bars
    h_masked = pd.Series(np.where(in_or, high, -np.inf))
    l_masked = pd.Series(np.where(in_or, low, np.inf))
    or_high = h_masked.groupby(day).cummax().to_numpy()
    or_low = l_masked.groupby(day).cummin().to_numpy()

    ready = bid >= k_bars
    up = ready & (close > or_high)
    dn = ready & (close < or_low)
    raw = np.full(len(close), np.nan)
    raw[dn] = -1.0
    raw[up] = 1.0
    pos = _ffill_within_day(raw, dref)
    if not long_short:
        pos = np.clip(pos, 0.0, 1.0)
    pos[d["hour"] >= exit_hour] = 0.0
    return pos


# ---------------------------------------------------- (c) session momentum

def session_momentum(d, m_bars=24, long_only=False, exit_hour=23):
    """At bar m_bars of the day, take the sign of the day-so-far return
    (close vs day open) and hold that position until exit_hour."""
    bid, _, dref = _bar_in_day(d)
    day_open = _day_open(d)
    sig_at_m = np.sign(d["close"] - day_open)
    raw = np.where(bid == m_bars, sig_at_m, np.nan)
    pos = _ffill_within_day(raw, dref)
    if long_only:
        pos = np.clip(pos, 0.0, 1.0)
    pos[d["hour"] >= exit_hour] = 0.0
    return pos


# ------------------------------------------- (d) overnight vs intraday drift

def overnight_intraday(d, side="overnight", evening_hour=23, morning_bars=0,
                       skip_first=0, exit_hour=23):
    """Which side carries gold's drift?

    side='overnight': long from evening_hour to day close (this captures the
        overnight/weekend gap via the last bar's position) plus the first
        morning_bars bars of the new day.
    side='intraday': long during the day only - from bar skip_first of the day
        until exit_hour, so the position is dropped before the daily gap.
    """
    bid, _, _ = _bar_in_day(d)
    hour = d["hour"]
    if side == "overnight":
        pos = ((hour >= evening_hour) | (bid < morning_bars)).astype(np.float64)
    elif side == "intraday":
        pos = ((bid >= skip_first) & (hour < exit_hour)).astype(np.float64)
    else:
        raise ValueError(f"unknown side {side!r}")
    return pos


# ---------------------------------------------------------------- best configs

# Best config per variant, selected by TRAIN sharpe only (1bp/side costs).
BEST = [
    {"fn": "hour_band", "variant": "hour_band",
     "params": {"h1": 20, "band_len": 12, "short_opposite": False}},
    {"fn": "orb", "variant": "orb",
     "params": {"k_bars": 12, "long_short": False, "exit_hour": 23}},
    {"fn": "session_momentum", "variant": "session_momentum",
     "params": {"m_bars": 24, "long_only": True, "exit_hour": 23}},
    {"fn": "overnight_intraday", "variant": "overnight_intraday",
     "params": {"side": "overnight", "evening_hour": 21, "morning_bars": 6}},
]
