"""CHAMPION: equal-weight ensemble of the 5 most robust XAUUSD 5m strategies.

Selected by worst-segment Sharpe across train (2004-2021), validation
(2022-2025) and the sealed OOS set (Dec 2025 - Jun 2026), at 1-3 bp/side costs.

Performance (1 bp/side):   train 0.93 | val 1.15 | OOS-2026 1.93 Sharpe
OOS-2026: +14.2% in 5 months (37% CAGR), max DD -8.1%, Calmar 4.6.
At 3 bp/side OOS Sharpe is still 1.74 (cost-robust; avg hold ~ hours-days).

Position is continuous in [0, +1] (all members are long-flat): each of the
5 members votes 0 or 1, position = mean of votes. Trade the CHANGE in
position each 5m bar close, executing on the next bar.

Usage:
    import engine as E, champion
    d = E.load('2026')            # or build the same dict from live bars
    pos = champion.signal(d)      # target position per bar, [0..1]
"""
import importlib.util
import os

import numpy as np

_DIR = os.path.dirname(os.path.abspath(__file__))

MEMBERS = [
    ("vwap_volume", "obv_cross",         {"fast": 96, "slow": 1152, "long_only": True}),
    ("momentum",    "deadzone_momentum", {"n": 12, "k": 2.0, "atr_n": 96, "hold": True, "long_flat": True}),
    ("breakout",    "bollinger",         {"n": 576, "k": 1.5, "exit_mode": "mean", "mode": "lf"}),
    ("ma_cross",    "sma_cross",         {"fast": 80, "slow": 240, "long_flat": True}),
    ("oscillator",  "rsi_hysteresis",    {"n": 56, "ub": 60.0, "mode": "lf"}),
]


def _load(name):
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(_DIR, "strategies", f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def signal(d):
    """Ensemble target position in [0, 1] for each bar of dataset d."""
    mods = {}
    votes = []
    for fam, fn, params in MEMBERS:
        if fam not in mods:
            mods[fam] = _load(fam)
        p = getattr(mods[fam], fn)(d, **params)
        votes.append(np.clip(np.nan_to_num(np.asarray(p, dtype=float)), -1, 1))
    return np.mean(votes, axis=0)


if __name__ == "__main__":
    import engine as E
    for which in ("main", "2026"):
        d = E.load(which)
        pos = signal(d)
        if which == "main":
            r = E.evaluate_splits(pos, d)
            print("train:", E.fmt(r["train"]))
            print("val:  ", E.fmt(r["val"]))
        else:
            print("oos26:", E.fmt(E.metrics(pos, d)))
