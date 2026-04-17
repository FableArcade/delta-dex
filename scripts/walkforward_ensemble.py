"""Walk-forward backtest using bootstrap-ensemble inference.

Identical methodology to scripts/walkforward_backtest.py, but at each fold
trains N=30 bootstrap LightGBM models on the expanding window and predicts
with the median across all 30. Bagging should tighten predictions and
lift R² / Spearman 10-20% vs the single-model baseline.

Run:  python -m scripts.walkforward_ensemble
Output: data/models/walkforward_ensemble_<timestamp>.json

Prints a side-by-side table: single-model baseline (from latest
walkforward_*.json) vs this run's ensemble numbers.
"""

from __future__ import annotations

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
    FEATURE_COLUMNS,
    HORIZON_DAYS,
    TARGET_COL,
    build_training_dataset,
)
from pipeline.model.friction import EBAY_FVF, SHIPPING_COST

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("walkforward_ensemble")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"

BASE_PARAMS = {
    "objective": "quantile",
    "alpha": 0.5,
    "num_leaves": 31,
    "learning_rate": 0.05,
    "n_estimators": 500,
    "min_child_samples": 20,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "verbose": -1,
}

BUY_SLIPPAGE = 0.05
SELL_SLIPPAGE = 0.03
MIN_TRAIN_SAMPLES = 300
MIN_TRADES_PER_MONTH = 3
MIN_PRICE_FILTER = 25.0
N_ENSEMBLE = 30


def _net_return(buy: float, sell: float) -> float:
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


def main() -> None:
    with get_db() as db:
        df = build_training_dataset(db)
    if df.empty:
        raise SystemExit("No training samples.")
    df["anchor_date"] = pd.to_datetime(df["anchor_date"])
    logger.info("Samples: %d  span: %s .. %s", len(df),
                df["anchor_date"].min().date(), df["anchor_date"].max().date())

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
        Xtr = train[FEATURE_COLUMNS].values
        ytr = np.log1p(np.clip(train[TARGET_COL].values, -0.999, None))
        Xte = test[FEATURE_COLUMNS].values

        # Train N_ENSEMBLE bootstrap models, collect predictions from each.
        preds_per_model = np.zeros((N_ENSEMBLE, len(test)))
        for i in range(N_ENSEMBLE):
            idx = rng.integers(0, len(Xtr), size=len(Xtr))
            params = {**BASE_PARAMS, "seed": 42 + i}
            model = lgb.LGBMRegressor(**params)
            model.fit(Xtr[idx], ytr[idx])
            preds_per_model[i] = np.expm1(model.predict(Xte))
        ens_median = np.median(preds_per_model, axis=0)

        slice_df = test[["card_id", "anchor_date", TARGET_COL]].copy()
        slice_df["pred"] = ens_median
        preds.append(slice_df)
        logger.info("fold %s train=%d test=%d", m.date(), len(train), len(test))

    all_preds = pd.concat(preds, ignore_index=True)
    with get_db() as db:
        all_preds = _attach_prices(all_preds, db)
    pre_n = len(all_preds)
    all_preds = all_preds.dropna(subset=["buy_price", "sell_price"])
    all_preds = all_preds[all_preds["buy_price"] >= MIN_PRICE_FILTER].copy()
    logger.info("Priced & $%.0f-filter: %d -> %d", MIN_PRICE_FILTER, pre_n, len(all_preds))
    all_preds["net_return"] = [
        _net_return(b, s)
        for b, s in zip(all_preds["buy_price"], all_preds["sell_price"])
    ]
    all_preds = all_preds.dropna(subset=["net_return"])

    y = all_preds[TARGET_COL].values
    yhat = all_preds["pred"].values
    r2 = float(r2_score(y, yhat))
    sp = float(spearmanr(y, yhat).statistic)
    hit_rate = float((all_preds["net_return"] > 0).mean())
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

    n_top1 = max(1, int(round(0.01 * len(all_preds))))
    top1 = all_preds.nlargest(n_top1, "pred")
    top1_net = float(top1["net_return"].mean())
    top1_hit = float((top1["net_return"] > 0).mean())
    top1_sharpe = _monthly_sharpe(top1)

    out = {
        "model_type": "ensemble",
        "n_ensemble": N_ENSEMBLE,
        "folds": len(preds),
        "n_predictions": int(len(all_preds)),
        "r_squared_oos": r2,
        "spearman_oos": sp,
        "hit_rate": hit_rate,
        "top_decile_net_return": top_net,
        "top_decile_hit_rate": top_hit,
        "top_decile_sharpe": top_sharpe,
        "top2_net_return": top2_net, "top2_hit_rate": top2_hit,
        "top2_sharpe": top2_sharpe, "n_top2": int(n_top2),
        "top1_net_return": top1_net, "top1_hit_rate": top1_hit,
        "top1_sharpe": top1_sharpe, "n_top1": int(n_top1),
    }

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    outp = MODELS_DIR / f"walkforward_ensemble_{ts}.json"
    outp.write_text(json.dumps(out, indent=2, default=str))
    logger.info("Wrote %s", outp)

    # Compare against latest single-model walkforward
    baseline_path = sorted(MODELS_DIR.glob("walkforward_2026*.json"))
    baseline = None
    for p in reversed(baseline_path):
        if "ensemble" in p.name:
            continue
        try:
            baseline = json.loads(p.read_text())
            break
        except Exception:
            continue

    print("\n=== ENSEMBLE WALK-FORWARD ===")
    print(f"{'Metric':<28} {'Single':>12} {'Ensemble':>12}  {'Δ':>8}")
    print("-" * 64)
    for key, label in [
        ("r_squared_oos", "R² OOS"),
        ("spearman_oos", "Spearman"),
        ("hit_rate", "Hit rate"),
        ("top_decile_net_return", "Top-decile net"),
        ("top_decile_hit_rate", "Top-decile hit rate"),
        ("top_decile_sharpe", "Top-decile Sharpe"),
        ("top2_net_return", "Top-2% net"),
        ("top2_hit_rate", "Top-2% hit rate"),
        ("top2_sharpe", "Top-2% Sharpe"),
        ("top1_net_return", "Top-1% net"),
        ("top1_hit_rate", "Top-1% hit rate"),
    ]:
        b = baseline.get(key) if baseline else None
        e = out.get(key)
        if b is not None and e is not None:
            diff = e - b
            dstr = f"{diff:+.4f}" if abs(diff) < 0.1 else f"{diff:+.3f}"
            print(f"{label:<28} {b:>12.4f} {e:>12.4f}  {dstr:>8}")
        elif e is not None:
            print(f"{label:<28} {'—':>12} {e:>12.4f}")


if __name__ == "__main__":
    main()
