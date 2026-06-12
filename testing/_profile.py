import time, numpy as np, pandas as pd
import backtest as bt

t0 = time.time()
df5 = bt.load_ohlcv_5m(bt.resolve_csv(None))
bars = bt.build_tf_bars(df5)["1h"]
print(f"loaded+built in {time.time()-t0:.1f}s | 1H bars={len(bars):,}", flush=True)

dev_end = pd.Timestamp("2024-12-31")
start = bars.index[0]
RF = 0.02

base = [s for s in bt.STRATEGIES if s != "ensemble"]
daily_rets, indiv = {}, {}
print("--- per-strategy sleeves (default params, risk_frac=0.02, long+short) ---", flush=True)
for strat in base:
    p = bt.StrategyParams(strategy=strat, long_only=False, risk_frac=RF)
    r = bt.run_backtest(bars, p, start, dev_end)
    if len(r.trades) < 30:
        print(f"  {strat:16s} too few trades ({len(r.trades)})", flush=True)
        continue
    dr = r.equity_curve.pct_change().dropna()
    daily_rets[strat] = dr
    indiv[strat] = r.sharpe
    print(f"  {strat:16s} sharpe={r.sharpe:+.2f} ret={r.total_return_pct:+9.1f}% dd={r.max_drawdown_pct:6.1f}% "
          f"tpm={r.trades_per_month:5.1f} pf={r.profit_factor:.2f}", flush=True)

M = pd.concat(daily_rets, axis=1).fillna(0.0)
M.columns = list(daily_rets.keys())
corr = M.corr()
print("\nmean pairwise corr:", round(float(corr.values[np.triu_indices(len(M.columns),1)].mean()), 3), flush=True)

def psharpe(cols):
    port = M[cols].mean(axis=1)
    return port.mean()/port.std()*np.sqrt(252) if port.std() > 0 else 0.0

print(f"\nEQUAL-WEIGHT all {len(M.columns)} sleeves: Sharpe={psharpe(list(M.columns)):+.2f}", flush=True)
pos = [c for c in M.columns if indiv[c] > 0]
print(f"EQUAL-WEIGHT {len(pos)} positive-Sharpe sleeves {pos}: Sharpe={psharpe(pos):+.2f}", flush=True)
top = sorted(M.columns, key=lambda c: indiv[c], reverse=True)[:5]
print(f"EQUAL-WEIGHT top-5 sleeves {top}: Sharpe={psharpe(top):+.2f}", flush=True)
