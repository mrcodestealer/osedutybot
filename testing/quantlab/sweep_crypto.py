"""Re-run the full audited strategy grid (results/<family>_grid.csv) on a
crypto dataset. Selection on train (< 2026-01-01), validation Jan-Mar 2026;
the Apr-Jul 2026 OOS slice is NOT touched here (finalize_crypto.py opens it).

Usage: python sweep_crypto.py <btc|eth|sol>
"""
import glob
import importlib.util
import json
import os
import sys

import numpy as np
import pandas as pd

import engine as E

DIR = os.path.dirname(os.path.abspath(__file__))
VAL_START = np.datetime64("2026-01-01")
OOS_START = np.datetime64("2026-04-01")

COSTS = {  # per-side bp: (base, stress) from observed median spreads
    "btc": (2.5, 4.0),
    "eth": (6.0, 9.0),
    "sol": (34.0, 50.0),
}


def load_mod(name):
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(DIR, "strategies", f"{name}.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def main(sym):
    base, stress = COSTS[sym]
    d = E.load(sym)
    k_oos = int(np.searchsorted(d["ts"], OOS_START))
    dev = E.slice_d(d, slice(0, k_oos))          # development data only
    k_val = int(np.searchsorted(dev["ts"], VAL_START))
    d_tr = E.slice_d(dev, slice(0, k_val))
    d_va = E.slice_d(dev, slice(k_val, len(dev["close"])))
    print(f"{sym}: dev {len(dev['close'])} bars (train {k_val}, val {len(d_va['close'])}), "
          f"oos sealed {len(d['close']) - k_oos} bars, cost {base}/{stress} bp/side")

    rows = []
    for path in sorted(glob.glob(os.path.join(DIR, "results", "*_grid.csv"))):
        fam = os.path.basename(path).replace("_grid.csv", "")
        if fam in COSTS or fam.startswith(("btc", "eth", "sol")):
            continue  # skip our own outputs
        mod = load_mod(fam)
        v2f = {b["variant"]: b["fn"] for b in mod.BEST}
        grid = pd.read_csv(path)
        for _, g in grid.iterrows():
            var = g["variant"]
            fn = getattr(mod, v2f.get(var, ""), None)
            if fn is None:
                continue
            try:
                params = json.loads(g["params"])
                pos = np.clip(np.nan_to_num(np.asarray(
                    fn(dev, **params), dtype=np.float64)), -1, 1)
                r = {"family": fam, "variant": var, "params": g["params"]}
                for tag, dd, sl in (("train", d_tr, slice(0, k_val)),
                                    ("val", d_va, slice(k_val, len(pos)))):
                    for cn, c in (("base", base), ("stress", stress)):
                        m = E.metrics(pos[sl], dd, cost_bp=c)
                        r[f"{tag}_sharpe_{cn}"] = m["sharpe"]
                        if cn == "base":
                            r[f"{tag}_cagr"] = m["cagr_pct"]
                            r[f"{tag}_dd"] = m["max_dd_pct"]
                            r[f"{tag}_trades"] = m["n_trades"]
                            r[f"{tag}_hold"] = m["avg_hold_bars"]
                rows.append(r)
            except Exception as exc:
                rows.append({"family": fam, "variant": var, "params": g["params"],
                             "error": str(exc)[:120]})
        print(f"  {fam}: {len(grid)} configs done")

    df = pd.DataFrame(rows)
    out = os.path.join(DIR, "results", f"{sym}_grid.csv")
    df.to_csv(out, index=False)

    okd = df[df.get("error").isna()] if "error" in df else df
    sel = []
    for var, grp in okd.groupby("variant"):
        b = grp.loc[grp["train_sharpe_base"].idxmax()]
        sel.append({
            "family": str(b["family"]), "variant": str(var), "params": str(b["params"]),
            "train_sharpe": float(b["train_sharpe_base"]),
            "val_sharpe": float(b["val_sharpe_base"]),
            "train_sharpe_stress": float(b["train_sharpe_stress"]),
            "val_sharpe_stress": float(b["val_sharpe_stress"]),
            "val_cagr": float(b["val_cagr"]), "val_dd": float(b["val_dd"]),
            "val_trades": int(b["val_trades"]), "val_hold": float(b["val_hold"]),
        })
    sel.sort(key=lambda r: min(r["train_sharpe_stress"], r["val_sharpe_stress"]),
             reverse=True)
    with open(os.path.join(DIR, "results", f"{sym}_selected.json"), "w") as f:
        json.dump(sel, f, indent=1)
    print(f"{sym}: wrote {out} ({len(df)} rows) and {sym}_selected.json")
    print(f"{sym} top by robustness (min(train,val) @stress cost):")
    for r in sel[:8]:
        print(f"  {min(r['train_sharpe_stress'], r['val_sharpe_stress']):6.2f} | "
              f"train {r['train_sharpe']:5.2f} val {r['val_sharpe']:5.2f} @base | "
              f"{r['family']}/{r['variant']} {r['params'][:60]}")


if __name__ == "__main__":
    main(sys.argv[1])
