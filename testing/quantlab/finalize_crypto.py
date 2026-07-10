"""Open the sealed OOS window (2026-04-01 ..) for a crypto symbol:
evaluate each variant's train-selected best config + a top-5 robust ensemble
+ buy & hold, at base and stress costs. Run only after sweep_crypto.py.

Usage: python finalize_crypto.py <btc|eth|sol>
"""
import importlib.util
import json
import os
import sys

import numpy as np

import engine as E
from sweep_crypto import COSTS, OOS_START, VAL_START, load_mod

DIR = os.path.dirname(os.path.abspath(__file__))


def main(sym):
    base, stress = COSTS[sym]
    d = E.load(sym)
    n = len(d["close"])
    k_oos = int(np.searchsorted(d["ts"], OOS_START))
    k_val = int(np.searchsorted(d["ts"], VAL_START))
    d_oos = E.slice_d(d, slice(k_oos, n))
    sel = json.load(open(os.path.join(DIR, "results", f"{sym}_selected.json")))

    # eligibility: positive on BOTH train and val at stress cost, enough trades,
    # not degenerate (holding through most of val = disguised buy & hold)
    val_bars = k_oos - k_val
    elig = [r for r in sel
            if min(r["train_sharpe_stress"], r["val_sharpe_stress"]) > 0.3
            and r["val_trades"] >= 15 and r["val_hold"] < 0.5 * val_bars]
    members = elig[:5]

    mods, positions = {}, {}
    v2f = {}
    for fam in {r["family"] for r in sel}:
        mods[fam] = load_mod(fam)
        v2f.update({b["variant"]: (fam, b["fn"]) for b in mods[fam].BEST})

    def build(r):
        fam, fn_name = v2f[r["variant"]]
        fn = getattr(mods[fam], fn_name)
        params = json.loads(r["params"])
        ok = E.causality_check(fn, d, params, cut=0.7, warmup=500)
        pos = np.clip(np.nan_to_num(np.asarray(fn(d, **params), dtype=np.float64)), -1, 1)
        return pos, ok

    print(f"=== {sym.upper()} OOS {str(d['ts'][k_oos])[:10]} .. {str(d['ts'][-1])[:10]} "
          f"({n - k_oos} bars) | costs base {base} / stress {stress} bp/side ===")
    print(f"eligible robust variants: {len(elig)} of {len(sel)}; ensemble members: "
          f"{[m['variant'] for m in members]}")

    out = {"symbol": sym, "cost_base": base, "cost_stress": stress,
           "members": [], "rows": []}
    curves = {}

    def report(label, pos, extra=None):
        row = {"name": label}
        for cn, c in (("base", base), ("stress", stress)):
            m = E.metrics(pos[k_oos:], d_oos, cost_bp=c)
            row[f"oos_sharpe_{cn}"] = m["sharpe"]
            if cn == "base":
                row.update(oos_cagr=m["cagr_pct"], oos_dd=m["max_dd_pct"],
                           oos_trades=m["n_trades"], oos_ret=m["total_return_pct"])
        if extra:
            row.update(extra)
        out["rows"].append(row)
        held = np.concatenate([[0.0], pos[k_oos:][:-1]])
        ret = np.zeros(n - k_oos)
        ret[1:] = d_oos["close"][1:] / d_oos["close"][:-1] - 1
        sret = held * ret - np.abs(np.diff(np.concatenate([[0.0], held]))) * base * 1e-4
        curves[label] = list(np.round(np.cumprod(1 + sret), 5)[::24])
        print(f"  {label:32s} oos_sharpe {row['oos_sharpe_base']:6.2f} @base "
              f"{row['oos_sharpe_stress']:6.2f} @stress | ret {row['oos_ret']:7.2f}% "
              f"dd {row['oos_dd']:6.1f}% trades {row['oos_trades']}")

    report("buy_and_hold", np.ones(n), {"val_sharpe": None})
    ens = []
    for r in sel[:12]:  # report top-12 robust variants
        pos, causal_ok = build(r)
        if not causal_ok:
            print(f"  !! causality FAIL {r['variant']} - excluded")
            continue
        report(f"{r['family']}/{r['variant']}", pos,
               {"train_sharpe": r["train_sharpe"], "val_sharpe": r["val_sharpe"],
                "params": r["params"]})
        if r in members:
            ens.append(pos)
            out["members"].append({"variant": r["variant"], "params": r["params"]})
    if len(ens) >= 2:
        report("ENSEMBLE", np.mean(ens, axis=0))

    ts_ds = [str(t)[:16] for t in d["ts"][k_oos:][::24]]
    curves["ts"] = ts_ds
    json.dump(curves, open(os.path.join(DIR, "results", f"{sym}_curves.json"), "w"))
    json.dump(out, open(os.path.join(DIR, "results", f"{sym}_final.json"), "w"), indent=1)
    print(f"wrote results/{sym}_final.json")


if __name__ == "__main__":
    main(sys.argv[1])
