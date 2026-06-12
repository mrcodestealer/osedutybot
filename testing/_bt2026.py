import sys
import numpy as np, pandas as pd
import backtest as bt

bt.INITIAL_CAPITAL = float(sys.argv[1]) if len(sys.argv) > 1 else 20.0  # start capital

CSV = "5mGoldDataSet_2026.csv"
RF, TREND = 0.01, 50
names = ["consensus_trend", "allweather", "keltner_pullback", "donchian_break"]
sleeves = [bt.StrategyParams(strategy=s, long_only=False, risk_frac=RF, trend_sma_days=TREND) for s in names]

df5 = bt.load_ohlcv_5m(CSV)
bars = bt.build_tf_bars(df5)["1h"]
start, end = bars.index[0], bars.index[-1]
months = (end - start).days / 30.44
print("="*78)
print(f"2026 FORWARD TEST | start capital ${bt.INITIAL_CAPITAL:.0f} | risk {RF:.0%}/trade | trend SMA {TREND}")
print(f"Data: {start} -> {end}  ({len(bars):,} 1H bars, {months:.1f} months, gold ${df5['close'].iloc[-1]:,.0f})")
print("="*78)

def stats(r):
    wins=[t.pnl_usd for t in r.trades if t.pnl_usd>0]; loss=[abs(t.pnl_usd) for t in r.trades if t.pnl_usd<0]
    pf = sum(wins)/sum(loss) if loss else (999 if wins else 0)
    return len(r.trades), (sum(1 for t in r.trades if t.pnl_usd>0)/len(r.trades)*100 if r.trades else 0), pf

# ---- per-sleeve (each its own $20 account) ----
print(f"\nPER-SLEEVE (each a standalone ${bt.INITIAL_CAPITAL:.0f} account):")
print(f"{'sleeve':16s}{'trades':>7}{'win%':>7}{'PF':>6}{'ret%':>9}{'Sharpe':>8}{'maxDD%':>8}{'final$':>9}")
all_trades=[]
for s in sleeves:
    r = bt.run_backtest(bars, s, start, end)
    n, wr, pf = stats(r); all_trades += r.trades
    print(f"{s.strategy:16s}{n:>7}{wr:>6.1f}%{pf:>6.2f}{r.total_return_pct:>+8.1f}%{r.sharpe:>+8.2f}{r.max_drawdown_pct:>7.1f}%{r.final_equity:>8.2f}")

# ---- combined portfolio (equal-weight daily-rebalanced) ----
rets = bt.portfolio_daily_returns(bars, sleeves, start, end)
eq = (1+rets).cumprod()*bt.INITIAL_CAPITAL
sh = float(rets.mean()/rets.std()*np.sqrt(252)) if rets.std()>0 else 0
dd = float(((eq-eq.cummax())/eq.cummax()).min()*100)
tot_n = len(all_trades); wins=sum(1 for t in all_trades if t.pnl_usd>0)
tpm = tot_n/months

print("\n" + "="*78)
print(f"COMBINED PORTFOLIO (equal-weight, ${bt.INITIAL_CAPITAL:.0f} start)")
print("="*78)
print(f"  Final equity   : ${eq.iloc[-1]:.2f}   (from ${bt.INITIAL_CAPITAL:.2f})")
print(f"  Total return   : {(eq.iloc[-1]/bt.INITIAL_CAPITAL-1)*100:+.1f}%")
print(f"  Sharpe (ann.)  : {sh:+.2f}")
print(f"  Max drawdown   : {dd:.1f}%")
print(f"  Total trades   : {tot_n}  ({tpm:.1f}/month, {tpm/4.345:.1f}/week)")
print(f"  Win rate       : {wins/tot_n*100:.1f}%" if tot_n else "  Win rate: n/a")
print(f"  Peak equity    : ${eq.max():.2f} | trough ${eq.min():.2f}")

# ---- monthly breakdown ----
print("\nMONTHLY (portfolio return% + trades):")
m_ret = eq.resample("ME").last().pct_change()
m_ret.iloc[0] = eq.resample("ME").last().iloc[0]/bt.INITIAL_CAPITAL - 1
tr_month = pd.Series(1, index=pd.DatetimeIndex([t.entry_time for t in all_trades])).resample("ME").sum() if all_trades else pd.Series(dtype=int)
print(f"  {'month':<9}{'ret%':>9}{'trades':>8}{'equity$':>10}")
eqm = eq.resample("ME").last()
for ts in eqm.index:
    r = m_ret.get(ts, float('nan'))*100
    c = int(tr_month.get(ts, 0))
    print(f"  {ts.strftime('%Y-%m'):<9}{r:>+8.1f}%{c:>8}{eqm[ts]:>10.2f}")

# save
pd.DataFrame([{
    "start_capital": bt.INITIAL_CAPITAL, "final_equity": round(float(eq.iloc[-1]),2),
    "return_pct": round((eq.iloc[-1]/bt.INITIAL_CAPITAL-1)*100,1), "sharpe": round(sh,2),
    "max_dd_pct": round(dd,1), "total_trades": tot_n, "tpm": round(tpm,1),
    "win_rate_pct": round(wins/tot_n*100,1) if tot_n else 0,
    "data_start": str(start), "data_end": str(end), "months": round(months,1),
}]).to_csv("backtest_2026_result.csv", index=False)
print("\nSaved: backtest_2026_result.csv")
