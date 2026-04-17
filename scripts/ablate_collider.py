"""Ablation test for the flagged collider candidate `psa_10_vs_raw_pct`.

McElreath v4 (Statistical Rethinking, Lecture 05/06): conditioning on
descendants of the target biases inference. `psa_10_vs_raw_pct` is a
ratio whose numerator is current PSA 10 price — same market state as the
target's numerator — making it a collider candidate.

This script:
  1. Runs the v1_3 bootstrap-ensemble walkforward, same methodology as
     scripts/walkforward_ensemble.py, WITHOUT the suspect feature.
  2. Compares top-2% / top-decile / Spearman metrics against the existing
     v1_3 baseline (walkforward_ensemble_<latest>.json).
  3. Verdict:
       - Challenger wins on top-2% Sharpe AND Spearman → the feature was
         hurting; drop it permanently.
       - Challenger loses → feature was net-positive despite collider risk;
         keep but flag.
       - Mixed → ambiguous; inspect per-bucket.

Defaults to N_ENSEMBLE=10 (third of production) for faster turnaround;
re-run with N_ENSEMBLE=30 if the first pass is directional.

Run:  python -m scripts.ablate_collider [--n-ensemble 10] [--drop psa_10_vs_raw_pct]
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.metrics import r2_score

from db.connection import get_db
from pipeline.model.features import (
    FEATURE_COLUMNS, HORIZON_DAYS, TARGET_COL, build_training_dataset,
)
from pipeline.model.friction import EBAY_FVF, SHIPPING_COST

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ablate_collider")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"

BASE_PARAMS = {
    "objective": "quantile", "alpha": 0.5,
    "num_leaves": 31, "learning_rate": 0.05, "n_estimators": 500,
    "min_child_samples": 20, "subsample": 0.8, "colsample_bytree": 0.8,
    "reg_alpha": 0.1, "reg_lambda": 1.0, "verbose": -1,
}

BUY_SLIPPAGE = 0.05
SELL_SLIPPAGE = 0.03
MIN_TRAIN_SAMPLES = 300
MIN_TRADES_PER_MONTH = 3
MIN_PRICE_FILTER = 25.0


def _net_return(buy, sell):
    if buy is None or sell is None or buy <= 0 or sell <= 0:
        return float("nan")
    eb = buy * (1 + BUY_SLIPPAGE)
    es = sell * (1 - SELL_SLIPPAGE)
    net = es * (1 - EBAY_FVF) - SHIPPING_COST
    return (net - eb) / eb


def _attach_prices(test: pd.DataFrame, db: sqlite3.Connection) -> pd.DataFrame:
    ph = pd.read_sql_query(
        "SELECT card_id, date, psa_10_price FROM price_history "
        "WHERE psa_10_price IS NOT NULL", db,
    )
    ph["date"] = pd.to_datetime(ph["date"])
    by_card = {cid: g.sort_values("date").reset_index(drop=True)
               for cid, g in ph.groupby("card_id")}
    buy, sell = [], []
    for _, row in test.iterrows():
        g = by_card.get(row["card_id"])
        if g is None or g.empty:
            buy.append(None); sell.append(None); continue
        a = pd.Timestamp(row["anchor_date"])
        f = a + pd.Timedelta(days=HORIZON_DAYS)
        bg = g[(g["date"] >= a) & (g["date"] <= a + pd.Timedelta(days=31))]
        sg = g[(g["date"] >= f) & (g["date"] <= f + pd.Timedelta(days=30))]
        buy.append(float(bg.iloc[0]["psa_10_price"]) if not bg.empty else None)
        sell.append(float(sg.iloc[0]["psa_10_price"]) if not sg.empty else None)
    test = test.copy()
    test["buy_price"] = buy
    test["sell_price"] = sell
    return test


def _monthly_sharpe(df: pd.DataFrame) -> float:
    if df.empty: return 0.0
    grp = df.groupby(df["anchor_date"].dt.to_period("M"))["net_return"]
    counts = grp.count()
    means = grp.mean()
    keep = counts[counts >= MIN_TRADES_PER_MONTH].index
    monthly = means.loc[keep].dropna()
    arr = np.asarray(monthly.values, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) < 2: return 0.0
    std = float(np.nanstd(arr, ddof=1))
    if not np.isfinite(std) or std <= 0: return 0.0
    return float(np.nanmean(arr) / std * np.sqrt(12))


def run_walkforward(features: list[str], n_ensemble: int, tag: str) -> dict:
    """Run the full bootstrap-ensemble walkforward with a specific feature set."""
    with get_db() as db:
        df = build_training_dataset(db)
    if df.empty:
        raise SystemExit("No training samples.")
    df["anchor_date"] = pd.to_datetime(df["anchor_date"])
    logger.info("[%s] Samples: %d  features: %d", tag, len(df), len(features))

    start = df["anchor_date"].min().to_period("M").to_timestamp() + pd.DateOffset(months=6)
    end = df["anchor_date"].max().to_period("M").to_timestamp()
    months = pd.date_range(start, end, freq="MS")

    rng = np.random.default_rng(seed=42)
    preds = []
    for m in months:
        train = df[df["anchor_date"] < m]
        test = df[(df["anchor_date"] >= m) &
                  (df["anchor_date"] < m + pd.DateOffset(months=1))]
        if len(train) < MIN_TRAIN_SAMPLES or len(test) == 0:
            continue
        Xtr = train[features].values
        ytr = np.log1p(np.clip(train[TARGET_COL].values, -0.999, None))
        Xte = test[features].values

        preds_per_model = np.zeros((n_ensemble, len(test)))
        for i in range(n_ensemble):
            idx = rng.integers(0, len(Xtr), size=len(Xtr))
            params = {**BASE_PARAMS, "seed": 42 + i}
            model = lgb.LGBMRegressor(**params)
            model.fit(Xtr[idx], ytr[idx])
            preds_per_model[i] = np.expm1(model.predict(Xte))
        ens_median = np.median(preds_per_model, axis=0)

        slice_df = test[["card_id", "anchor_date", TARGET_COL]].copy()
        slice_df["pred"] = ens_median
        preds.append(slice_df)

    logger.info("[%s] Folds complete: %d", tag, len(preds))
    all_preds = pd.concat(preds, ignore_index=True)
    with get_db() as db:
        all_preds = _attach_prices(all_preds, db)
    all_preds = all_preds.dropna(subset=["buy_price", "sell_price"])
    all_preds = all_preds[all_preds["buy_price"] >= MIN_PRICE_FILTER].copy()
    all_preds["net_return"] = [
        _net_return(b, s)
        for b, s in zip(all_preds["buy_price"], all_preds["sell_price"])
    ]
    all_preds = all_preds.dropna(subset=["net_return"])

    y = all_preds[TARGET_COL].values
    yhat = all_preds["pred"].values
    r2 = float(r2_score(y, yhat))
    sp = float(spearmanr(y, yhat).statistic)

    all_preds["decile"] = pd.qcut(all_preds["pred"], 10, labels=False, duplicates="drop") + 1
    top = all_preds[all_preds["decile"] == all_preds["decile"].max()]
    top_net = float(top["net_return"].mean())
    top_hit = float((top["net_return"] > 0).mean())
    top_sharpe = _monthly_sharpe(top)

    n_top2 = max(1, int(round(0.02 * len(all_preds))))
    top2 = all_preds.nlargest(n_top2, "pred")
    top2_net = float(top2["net_return"].mean())
    top2_hit = float((top2["net_return"] > 0).mean())
    top2_sharpe = _monthly_sharpe(top2)

    return {
        "tag": tag,
        "n_ensemble": n_ensemble,
        "n_features": len(features),
        "folds": len(preds),
        "n_predictions": int(len(all_preds)),
        "r_squared_oos": r2,
        "spearman_oos": sp,
        "hit_rate": float((all_preds["net_return"] > 0).mean()),
        "top_decile_net": top_net,
        "top_decile_hit": top_hit,
        "top_decile_sharpe": top_sharpe,
        "top2_net": top2_net,
        "top2_hit": top2_hit,
        "top2_sharpe": top2_sharpe,
        "n_top2": int(n_top2),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-ensemble", type=int, default=10,
                        help="Bootstrap models per fold (default 10 for speed; 30 matches prod)")
    parser.add_argument("--drop", type=str, default="psa_10_vs_raw_pct",
                        help="Feature to drop from FEATURE_COLUMNS")
    args = parser.parse_args()

    if args.drop not in FEATURE_COLUMNS:
        raise SystemExit(f"{args.drop} not in FEATURE_COLUMNS")

    baseline_features = list(FEATURE_COLUMNS)
    challenger_features = [f for f in FEATURE_COLUMNS if f != args.drop]

    logger.info("=== Baseline: FULL feature set (n=%d) ===", len(baseline_features))
    baseline = run_walkforward(baseline_features, args.n_ensemble, tag="baseline")

    logger.info("=== Challenger: MINUS %s (n=%d) ===", args.drop, len(challenger_features))
    challenger = run_walkforward(challenger_features, args.n_ensemble, tag="challenger")

    print("\n" + "=" * 72)
    print(f"ABLATION — drop `{args.drop}`  (n_ensemble={args.n_ensemble})")
    print("=" * 72)
    fmt = "{:<26} {:>14} {:>14}  {:>10}"
    print(fmt.format("Metric", "Baseline", "Challenger", "Δ"))
    print("-" * 72)
    for k, label in [
        ("spearman_oos", "Spearman"),
        ("r_squared_oos", "R² OOS"),
        ("hit_rate", "Hit rate"),
        ("top_decile_sharpe", "Top-decile Sharpe"),
        ("top_decile_net", "Top-decile net"),
        ("top2_sharpe", "Top-2% Sharpe"),
        ("top2_net", "Top-2% net"),
        ("top2_hit", "Top-2% hit rate"),
    ]:
        b = baseline[k]; c = challenger[k]
        diff = c - b
        mark = "✓" if (diff > 0) else "✗" if diff < 0 else "·"
        print(fmt.format(label, f"{b:.4f}", f"{c:.4f}", f"{diff:+.4f} {mark}"))
    print("-" * 72)

    # Verdict logic
    wins = sum(1 for k in ("spearman_oos", "top2_sharpe", "top_decile_sharpe")
               if challenger[k] > baseline[k])
    if wins >= 2:
        verdict = f"DROP `{args.drop}` — challenger beats baseline on {wins}/3 primary metrics"
    elif wins == 1:
        verdict = f"AMBIGUOUS — mixed on primary metrics; re-run with --n-ensemble 30"
    else:
        verdict = f"KEEP `{args.drop}` — baseline beats challenger on primary metrics"

    print(f"\nVERDICT: {verdict}\n")

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    outp = MODELS_DIR / f"ablation_{args.drop}_{ts}.json"
    outp.write_text(json.dumps({
        "baseline": baseline, "challenger": challenger,
        "dropped_feature": args.drop, "verdict": verdict,
        "n_ensemble": args.n_ensemble,
    }, indent=2, default=str))
    print(f"Wrote {outp}")


if __name__ == "__main__":
    main()
