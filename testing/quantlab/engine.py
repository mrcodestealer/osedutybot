"""Shared vectorized backtest engine for the XAUUSD 5m strategy lab.

Contract for strategy functions
-------------------------------
    def my_strategy(d, **params) -> np.ndarray
        d: dict with keys ts (datetime64[ns]), open, high, low, close, vol,
           hour, minute, dow (0=Mon), day_id (int, increments each new date)
        returns: float array, same length as d['close'], position in [-1, +1].
        pos[t] is the signal DECIDED at the close of bar t; the engine holds it
        during bar t+1 (i.e. it is applied to the t -> t+1 close-to-close return).
        MUST be causal: pos[t] may only use data up to and including bar t.
        NaNs during indicator warmup are fine (treated as flat).

Costs: cost_bp is charged PER SIDE on turnover (|change in position|),
so a full round trip of a unit position costs 2 * cost_bp. Default 1.0 bp
per side (~2 bp round trip ~= $0.65/oz at $3300 gold: spread + commission).

Splits: train = main data before 2022-01-01, val = 2022-01-01 onward.
The 2026 file is a held-out OOS set - do not touch it during development.
"""
import json
import os

import numpy as np

_DIR = os.path.dirname(os.path.abspath(__file__))

TRAIN_END = np.datetime64("2022-01-01")
DEFAULT_COST_BP = 1.0  # per side


def load(which="main"):
    """Load cached dataset ('main' or '2026') as a dict of aligned arrays."""
    z = np.load(os.path.join(_DIR, f"data_{which}.npz"))
    ts = z["ts"].astype("datetime64[ns]")
    d = {
        "ts": ts,
        "open": z["open"],
        "high": z["high"],
        "low": z["low"],
        "close": z["close"],
        "vol": z["vol"],
    }
    ts_s = ts.astype("datetime64[s]").astype("int64")
    d["hour"] = ((ts_s // 3600) % 24).astype(np.int64)
    d["minute"] = ((ts_s // 60) % 60).astype(np.int64)
    days = ts.astype("datetime64[D]")
    d["dow"] = ((days.astype("int64") + 3) % 7).astype(np.int64)  # 0=Mon
    new_day = np.concatenate([[True], days[1:] != days[:-1]])
    d["day_id"] = np.cumsum(new_day) - 1
    return d


def slice_d(d, mask_or_idx):
    return {k: v[mask_or_idx] for k, v in d.items()}


# ---------------------------------------------------------------- indicators
# All causal. Strategies should prefer these over hand-rolled versions.

def sma(x, n):
    import pandas as pd
    return pd.Series(x).rolling(n).mean().to_numpy()


def ema(x, n):
    import pandas as pd
    return pd.Series(x).ewm(span=n, adjust=False).mean().to_numpy()


def rolling_std(x, n):
    import pandas as pd
    return pd.Series(x).rolling(n).std().to_numpy()


def rolling_max(x, n):
    import pandas as pd
    return pd.Series(x).rolling(n).max().to_numpy()


def rolling_min(x, n):
    import pandas as pd
    return pd.Series(x).rolling(n).min().to_numpy()


def zscore(x, n):
    m = sma(x, n)
    s = rolling_std(x, n)
    with np.errstate(divide="ignore", invalid="ignore"):
        return (x - m) / s


def true_range(d):
    h, l, c = d["high"], d["low"], d["close"]
    pc = np.concatenate([[c[0]], c[:-1]])
    return np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))


def atr(d, n=14):
    import pandas as pd
    return pd.Series(true_range(d)).ewm(alpha=1.0 / n, adjust=False).mean().to_numpy()


def rsi(x, n=14):
    import pandas as pd
    diff = np.diff(x, prepend=x[0])
    up = pd.Series(np.where(diff > 0, diff, 0.0)).ewm(alpha=1.0 / n, adjust=False).mean()
    dn = pd.Series(np.where(diff < 0, -diff, 0.0)).ewm(alpha=1.0 / n, adjust=False).mean()
    rs = up / dn.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).to_numpy()


def hold_signal(entries_long, entries_short, exits):
    """Stateful position builder: enter on entry flags, flat on exit flags.
    entries win over exits on the same bar. Vector-friendly forward fill."""
    n = len(exits)
    sig = np.zeros(n)
    sig[entries_short.astype(bool)] = -1.0
    sig[entries_long.astype(bool)] = 1.0
    ev = sig.copy()
    ev[(~entries_long.astype(bool)) & (~entries_short.astype(bool)) & exits.astype(bool)] = 0.0
    have = entries_long.astype(bool) | entries_short.astype(bool) | exits.astype(bool)
    idx = np.where(have, np.arange(n), -1)
    idx = np.maximum.accumulate(idx)
    out = np.where(idx >= 0, ev[np.maximum(idx, 0)], 0.0)
    return out


# ---------------------------------------------------------------- backtest

def _max_drawdown(eq):
    peak = np.maximum.accumulate(eq)
    dd = eq / peak - 1.0
    return dd.min() if len(dd) else 0.0


def metrics(pos, d, cost_bp=DEFAULT_COST_BP):
    """Backtest a position series over dataset d. Returns a metrics dict."""
    close, ts = d["close"], d["ts"]
    n = len(close)
    pos = np.clip(np.nan_to_num(np.asarray(pos, dtype=np.float64)), -1.0, 1.0)
    assert len(pos) == n, f"pos length {len(pos)} != data length {n}"

    ret = np.zeros(n)
    ret[1:] = close[1:] / close[:-1] - 1.0
    held = np.concatenate([[0.0], pos[:-1]])          # held during bar t
    dpos = np.diff(np.concatenate([[0.0], held]))      # trade executed at bar t
    cost = np.abs(dpos) * cost_bp * 1e-4
    sret = held * ret - cost

    eq = np.cumprod(1.0 + sret)
    years = max((ts[-1] - ts[0]) / np.timedelta64(1, "s") / (365.25 * 86400), 1e-9)
    bpy = n / years
    mu, sd = sret.mean(), sret.std()
    downside = sret[sret < 0].std() if (sret < 0).any() else 0.0
    sharpe = mu / sd * np.sqrt(bpy) if sd > 0 else 0.0
    sortino = mu / downside * np.sqrt(bpy) if downside > 0 else 0.0
    max_dd = _max_drawdown(eq)
    total_ret = eq[-1] - 1.0
    cagr = eq[-1] ** (1.0 / years) - 1.0 if eq[-1] > 0 else -1.0

    # per-trade stats (segments of constant nonzero held position)
    change = np.flatnonzero(np.diff(held) != 0) + 1
    starts = np.concatenate([[0], change])
    seg_pos = held[starts]
    seg_pnl = np.add.reduceat(held * ret, starts) if n else np.array([])
    live = seg_pos != 0
    trade_pnl = seg_pnl[live] - 2.0 * np.abs(seg_pos[live]) * cost_bp * 1e-4
    n_trades = int(live.sum())
    ends = np.concatenate([starts[1:], [n]])
    hold_bars = (ends - starts)[live]
    gross_win = trade_pnl[trade_pnl > 0].sum()
    gross_loss = -trade_pnl[trade_pnl < 0].sum()

    return {
        "n_bars": int(n),
        "years": round(float(years), 2),
        "total_return_pct": round(float(total_ret * 100), 2),
        "cagr_pct": round(float(cagr * 100), 3),
        "sharpe": round(float(sharpe), 3),
        "sortino": round(float(sortino), 3),
        "max_dd_pct": round(float(max_dd * 100), 2),
        "calmar": round(float(cagr / abs(max_dd)), 3) if max_dd < 0 else 0.0,
        "profit_factor": round(float(gross_win / gross_loss), 3) if gross_loss > 0 else float("inf"),
        "win_rate_pct": round(float((trade_pnl > 0).mean() * 100), 2) if n_trades else 0.0,
        "n_trades": n_trades,
        "trades_per_year": round(n_trades / years, 1),
        "avg_hold_bars": round(float(hold_bars.mean()), 1) if n_trades else 0.0,
        "avg_trade_bp": round(float(trade_pnl.mean() * 1e4), 2) if n_trades else 0.0,
        "exposure_pct": round(float((held != 0).mean() * 100), 1),
        "cost_bp_per_side": cost_bp,
    }


def evaluate_splits(pos, d, cost_bp=DEFAULT_COST_BP):
    """Evaluate a full-length position series on the train/val split of `d`."""
    k = int(np.searchsorted(d["ts"], TRAIN_END))
    return {
        "train": metrics(pos[:k], slice_d(d, slice(0, k)), cost_bp),
        "val": metrics(pos[k:], slice_d(d, slice(k, len(pos))), cost_bp),
    }


def causality_check(fn, d, params, cut=0.7, warmup=1000):
    """True if fn's signals are unchanged when future data is removed."""
    n = len(d["close"])
    k = int(n * cut)
    p_full = np.nan_to_num(np.asarray(fn(d, **params), dtype=np.float64)[:k])
    p_cut = np.nan_to_num(np.asarray(fn(slice_d(d, slice(0, k)), **params), dtype=np.float64))
    return bool(np.allclose(p_full[warmup:], p_cut[warmup:], atol=1e-9))


def fmt(m):
    return json.dumps(m, indent=2, default=str)
