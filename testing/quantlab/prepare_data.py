"""Parse the raw XAUUSD 5m CSVs once and cache them as .npz for fast loading."""
import os
import numpy as np
import pandas as pd

DIR = os.path.dirname(os.path.abspath(__file__))
TESTING = os.path.dirname(DIR)

SOURCES = {
    "main": os.path.join(TESTING, "5mGoldDataSet.csv"),
    "2026": os.path.join(TESTING, "5mGoldDataSet_2026.csv"),
}


def prepare(name, path):
    df = pd.read_csv(path, sep=";")
    df["Date"] = pd.to_datetime(df["Date"], format="%Y.%m.%d %H:%M")
    n_raw = len(df)
    df = df.sort_values("Date").drop_duplicates(subset="Date", keep="last")
    bad = (
        (df[["Open", "High", "Low", "Close"]] <= 0).any(axis=1)
        | (df["High"] < df["Low"])
    )
    df = df[~bad]
    out = os.path.join(DIR, f"data_{name}.npz")
    np.savez_compressed(
        out,
        ts=df["Date"].to_numpy().astype("datetime64[ns]").astype("int64"),
        open=df["Open"].to_numpy(dtype=np.float64),
        high=df["High"].to_numpy(dtype=np.float64),
        low=df["Low"].to_numpy(dtype=np.float64),
        close=df["Close"].to_numpy(dtype=np.float64),
        vol=df["tick_volume"].to_numpy(dtype=np.float64),
    )
    print(
        f"{name}: {n_raw} raw -> {len(df)} clean bars "
        f"({df['Date'].iloc[0]} .. {df['Date'].iloc[-1]}), "
        f"dupes/bad dropped: {n_raw - len(df)} -> {out}"
    )


if __name__ == "__main__":
    for name, path in SOURCES.items():
        prepare(name, path)
