import time, itertools, numpy as np, pandas as pd
import backtest as bt

t0 = time.time()
df5 = bt.load_ohlcv_5m(bt.resolve_csv(None))
bars = bt.build_tf_bars(df5)["1h"]
dev_end = pd.Timestamp("2024-12-31")
unseen_start = pd.Timestamp("2025-01-01")
start, end = bars.index[0], bars.index[-1]
print(f"loaded in {time.time()-t0:.1f}s", flush=True)

base = [s for s in bt.STRATEGIES if s != "ensemble"]

def cache(rf):
    dev_ret, uns_ret, dev_tr, uns_tr = {}, {}, {}, {}
    for s in base:
        p = bt.StrategyParams(strategy=s, long_only=False, risk_frac=rf)
        rd = bt.run_backtest(bars, p, start, dev_end)
        ru = bt.run_backtest(bars, p, unseen_start, end)
        if len(rd.trades) >= 30:
            dev_ret[s] = rd.equity_curve.pct_change().fillna(0.0); dev_tr[s] = len(rd.trades)
        if len(ru.equity_curve) >= 3:
            uns_ret[s] = ru.equity_curve.pct_change().fillna(0.0); uns_tr[s] = len(ru.trades)
    return dev_ret, uns_ret, dev_tr, uns_tr

def sh(r):
    return float(r.mean()/r.std()*np.sqrt(252)) if r is not None and len(r)>1 and r.std()>0 else 0.0

def port(retmap, cols):
    cols = [c for c in cols if c in retmap]
    if not cols: return None
    M = pd.concat([retmap[c] for c in cols], axis=1).fillna(0.0)
    return M.mean(axis=1)

def dd(r):
    eq=(1+r).cumprod(); return float(((eq-eq.cummax())/eq.cummax()).min()*100)

for rf in (0.01, 0.02):
    dev_ret, uns_ret, dev_tr, uns_tr = cache(rf)
    months_dev = (dev_end-start).days/30.44
    months_uns = (end-unseen_start).days/30.44
    sets = {
        "4 stable (posY>=0.5)": ["consensus_trend","keltner_pullback","regime_adaptive","vol_regime_momo"],
        "+session+rsi (6)": ["consensus_trend","keltner_pullback","regime_adaptive","vol_regime_momo","session_breakout","rsi_pullback"],
        "+donchian+allweather (freq, 8)": ["consensus_trend","keltner_pullback","regime_adaptive","vol_regime_momo","session_breakout","rsi_pullback","donchian_break","allweather"],
        "all 13": base,
        "top3 sharpe": ["consensus_trend","session_breakout","keltner_pullback"],
    }
    print(f"\n===== risk_frac={rf} =====", flush=True)
    for name, cols in sets.items():
        dr = port(dev_ret, cols); ur = port(uns_ret, cols)
        dtr = sum(dev_tr.get(c,0) for c in cols); utr = sum(uns_tr.get(c,0) for c in cols)
        print(f"  {name:32s} devSh={sh(dr):+.2f} devDD={dd(dr):6.1f}% devTPM={dtr/months_dev:5.1f} | "
              f"unsSh={sh(ur):+.2f} unsDD={dd(ur):6.1f}% unsTPM={utr/months_uns:5.1f}", flush=True)
