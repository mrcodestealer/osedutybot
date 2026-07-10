"""Final evaluation: run every family's BEST configs on train/val and the
sealed 2026 out-of-sample set, at 1/2/3 bp per-side costs. Emits
results/final_summary.csv and results/equity_curves.json.

Only run this AFTER strategy development is frozen - it opens the holdout.
"""
import glob
import importlib.util
import json
import os

import numpy as np

import engine as E

DIR = os.path.dirname(os.path.abspath(__file__))
COSTS = [1.0, 2.0, 3.0]


def load_modules():
    mods = {}
    for path in sorted(glob.glob(os.path.join(DIR, "strategies", "*.py"))):
        name = os.path.splitext(os.path.basename(path))[0]
        if name.startswith("_"):
            continue
        spec = importlib.util.spec_from_file_location(name, path)
        mod = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(mod)
        except Exception as exc:  # broken module: skip but report
            print(f"!! failed to import {name}: {exc}")
            continue
        if hasattr(mod, "BEST"):
            mods[name] = mod
    return mods


def main():
    d = E.load("main")
    d26 = E.load("2026")
    k = int(np.searchsorted(d["ts"], E.TRAIN_END))

    rows = []
    curves = {}

    # benchmark
    for label, dd in (("val", E.slice_d(d, slice(k, len(d["close"])))), ("oos2026", d26)):
        m = E.metrics(np.ones(len(dd["close"])), dd, cost_bp=0.0)
        rows.append({"family": "benchmark", "variant": "buy_and_hold", "params": "{}",
                     "segment": label, "cost_bp": 0.0, **m})
    curves["benchmark/buy_and_hold"] = {
        "oos_ts": [str(t)[:16] for t in d26["ts"][::48]],
        "oos_eq": list(np.round(np.cumprod(1 + np.diff(d26["close"], prepend=d26["close"][0]) / np.concatenate([[d26["close"][0]], d26["close"][:-1]])), 5)[::48]),
    }

    mods = load_modules()
    print(f"loaded {len(mods)} strategy modules: {list(mods)}")
    for name, mod in mods.items():
        for cfg in mod.BEST:
            fn = getattr(mod, cfg["fn"], None)
            if fn is None:
                print(f"!! {name}.{cfg['fn']} missing")
                continue
            key = f"{name}/{cfg['variant']}"
            try:
                if not E.causality_check(fn, d26, cfg["params"], cut=0.7, warmup=200):
                    print(f"!! {key}: causality FAIL on 2026 data - skipping")
                    continue
                pos = fn(d, **cfg["params"])
                pos26 = fn(d26, **cfg["params"])
            except Exception as exc:
                print(f"!! {key}: {exc}")
                continue
            for cost in COSTS:
                sp = E.evaluate_splits(pos, d, cost_bp=cost)
                m26 = E.metrics(pos26, d26, cost_bp=cost)
                for seg, m in (("train", sp["train"]), ("val", sp["val"]), ("oos2026", m26)):
                    rows.append({"family": name, "variant": cfg["variant"],
                                 "params": json.dumps(cfg["params"]),
                                 "segment": seg, "cost_bp": cost, **m})
            # equity curves at base cost
            held26 = np.concatenate([[0.0], np.clip(np.nan_to_num(pos26), -1, 1)[:-1]])
            ret26 = np.zeros(len(d26["close"]))
            ret26[1:] = d26["close"][1:] / d26["close"][:-1] - 1
            sret26 = held26 * ret26 - np.abs(np.diff(np.concatenate([[0.0], held26]))) * 1e-4
            curves[key] = {"oos_eq": list(np.round(np.cumprod(1 + sret26), 5)[::48])}
            print(f"ok {key}")

    import pandas as pd
    df = pd.DataFrame(rows)
    out_csv = os.path.join(DIR, "results", "final_summary.csv")
    df.to_csv(out_csv, index=False)
    with open(os.path.join(DIR, "results", "equity_curves.json"), "w") as f:
        json.dump(curves, f)
    print(f"\nwrote {out_csv} ({len(df)} rows)")

    # quick leaderboard: val sharpe at 1bp with OOS confirmation
    base = df[(df.cost_bp == 1.0)]
    piv = base.pivot_table(index=["family", "variant"], columns="segment",
                           values="sharpe", aggfunc="first")
    if not piv.empty:
        piv = piv.sort_values("val", ascending=False)
        print("\n=== Sharpe leaderboard (1bp/side) ===")
        print(piv.head(25).to_string())


if __name__ == "__main__":
    main()
