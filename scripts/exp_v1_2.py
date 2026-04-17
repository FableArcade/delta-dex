"""Experiment: raise R² OOS by changing horizon, target transform, price
floor, outlier trim — without touching production code.

Monkey-patches pipeline.model.features constants at runtime, then runs a
walk-forward evaluation identical in spirit to scripts/walkforward_backtest.py
but with:

  - MIN_PSA10_PRICE = 10 (was 20): expand sample count
  - HORIZON_DAYS    = 180 (was 90): amortize friction over longer hold
  - OUTLIER_TRIM_PCT = 0.02 (was 0.01): reduce tail noise
  - target transform: log1p(net_return) for training, invert for eval

Then does permutation-importance feature selection, cuts to top 12, re-runs.

Emits data/models/exp_v1_2_<run>_<timestamp>.json with metrics per run and
prints a comparison table at the end.

Pure experiment — does not write to model_projections, model_report_card,
or model_promotion_log. Dataset is cached to data/models/exp_v1_2_dataset.parquet
on first build; delete that file to force rebuild.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.metrics import r2_score

# Patch features module constants BEFORE importing anything that reads them.
import pipeline.model.features as feat_mod  # noqa: E402

feat_mod.MIN_PSA10_PRICE = 10.0
feat_mod.HORIZON_DAYS = 180
feat_mod.MIN_FORWARD_DAYS = 180
feat_mod.OUTLIER_TRIM_PCT = 0.02

from db.connection import get_db  # noqa: E402
from pipeline.model.features import FEATURE_COLUMNS, build_training_dataset  # noqa: E402
from pipeline.model.friction import EBAY_FVF, SHIPPING_COST  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("exp_v1_2")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

DATASET_CACHE = MODELS_DIR / "exp_v1_2_dataset.parquet"

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
DEFAULT_MIN_PRICE_FILTER = 25.0
MIN_TRAIN_SAMPLES = 300
MIN_TRADES_PER_MONTH = 3
HORIZON_DAYS = 180  # matches feat patch


def _net_return(buy: float, sell: float) -> float:
    if buy is None or sell is None or buy <= 0 or sell <= 0:
        return float("nan")
    eb = buy * (1 + BUY_SLIPPAGE)
    es = sell * (1 - SELL_SLIPPAGE)
    net = es * (1 - EBAY_FVF) - SHIPPING_COST
    return (net - eb) / eb


def _attach_prices_180d(test: pd.DataFrame, db: sqlite3.Connection) -> pd.DataFrame:
    """Buy = first PSA 10 within anchor..anchor+14d.
    Sell = first PSA 10 within anchor+HORIZON..anchor+HORIZON+30d."""
    ph = pd.read_sql_query(
        "SELECT card_id, date, psa_10_price FROM price_history "
        "WHERE psa_10_price IS NOT NULL",
        db,
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
        bg = g[(g["date"] >= a) & (g["date"] <= a + pd.Timedelta(days=14))]
        sg = g[(g["date"] >= f) & (g["date"] <= f + pd.Timedelta(days=30))]
        buy.append(float(bg.iloc[0]["psa_10_price"]) if not bg.empty else None)
        sell.append(float(sg.iloc[0]["psa_10_price"]) if not sg.empty else None)
    test = test.copy()
    test["buy_price"] = buy
    test["sell_price"] = sell
    return test


def _sharpe_monthly(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    grp = df.groupby(df["anchor_date"].dt.to_period("M"))["net_return"]
    counts = grp.count()
    means = grp.mean()
    keep = counts[counts >= MIN_TRADES_PER_MONTH].index
    monthly = means.loc[keep].dropna()
    arr = np.asarray(monthly.values, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) < 2:
        return 0.0
    std = float(np.nanstd(arr, ddof=1))
    if not np.isfinite(std) or std <= 0:
        return 0.0
    return float(np.nanmean(arr) / std * np.sqrt(12))


def load_or_build_dataset(db: sqlite3.Connection, force: bool = False) -> pd.DataFrame:
    if DATASET_CACHE.exists() and not force:
        logger.info("Loading cached dataset: %s", DATASET_CACHE)
        df = pd.read_parquet(DATASET_CACHE)
        logger.info("Cached dataset: %d samples, %d columns", len(df), len(df.columns))
        return df

    logger.info("Building training dataset (HORIZON=180d, MIN_PRICE=$10)...")
    df = build_training_dataset(db)
    if df.empty:
        raise SystemExit("No training samples.")

    df["anchor_date"] = pd.to_datetime(df["anchor_date"])
    df["target_net"] = df["target_return_90d"]  # actually 180d after patch
    df["target_log"] = np.log1p(df["target_net"].clip(lower=-0.999))

    df.to_parquet(DATASET_CACHE, index=False)
    logger.info("Cached to %s (%d samples)", DATASET_CACHE, len(df))
    return df


def walkforward(
    df: pd.DataFrame,
    db: sqlite3.Connection,
    feature_cols: List[str],
    label: str,
    min_price: float = DEFAULT_MIN_PRICE_FILTER,
) -> Dict:
    """Run expanding-window walk-forward. Trains on log1p(net), inverts for eval."""
    df = df.copy()
    df["anchor_date"] = pd.to_datetime(df["anchor_date"])

    start = df["anchor_date"].min().to_period("M").to_timestamp() + pd.DateOffset(months=6)
    end = df["anchor_date"].max().to_period("M").to_timestamp()
    months = pd.date_range(start, end, freq="MS")
    logger.info("[%s] %d potential folds, %d features, %d samples",
                label, len(months), len(feature_cols), len(df))

    preds = []
    importances = np.zeros(len(feature_cols))
    n_fits = 0
    for m in months:
        train = df[df["anchor_date"] < m]
        test = df[(df["anchor_date"] >= m) &
                  (df["anchor_date"] < m + pd.DateOffset(months=1))]
        if len(train) < MIN_TRAIN_SAMPLES or len(test) == 0:
            continue
        model = lgb.LGBMRegressor(**BASE_PARAMS)
        model.fit(train[feature_cols].values, train["target_log"].values)
        p_log = model.predict(test[feature_cols].values)
        slice_df = test[["card_id", "anchor_date", "target_net", "target_log"]].copy()
        slice_df["pred_log"] = p_log
        slice_df["pred_net"] = np.expm1(p_log)  # invert
        slice_df["fold_month"] = m.strftime("%Y-%m")
        preds.append(slice_df)
        importances += model.feature_importances_
        n_fits += 1

    if not preds:
        raise SystemExit(f"[{label}] no folds produced predictions.")

    all_preds = pd.concat(preds, ignore_index=True)
    # Use the target_net already computed in build_training_dataset as the
    # realized return. build_training_dataset only emits samples with valid
    # anchor and forward prices, so target_net is always defined here.
    # Skipping re-attach_prices avoids buy-window mismatches caused by the
    # monthly anchor resample vs. sparse underlying price history.
    all_preds["net_return"] = all_preds["target_net"]
    pre_n = len(all_preds)
    all_preds = all_preds.dropna(subset=["net_return"])
    logger.info("[%s] target_net rows: %d -> %d (dropna)", label, pre_n, len(all_preds))

    y_linear = all_preds["target_net"].values
    y_log = all_preds["target_log"].values
    yhat_log = all_preds["pred_log"].values
    yhat_linear = all_preds["pred_net"].values

    r2_linear = float(r2_score(y_linear, yhat_linear)) if len(y_linear) > 1 else float("nan")
    r2_log = float(r2_score(y_log, yhat_log)) if len(y_log) > 1 else float("nan")
    spearman = float(spearmanr(y_linear, yhat_log).statistic) if len(y_linear) > 1 else float("nan")
    hit_rate = float((all_preds["net_return"] > 0).mean())

    try:
        all_preds["decile"] = pd.qcut(all_preds["pred_log"], 10,
                                       labels=False, duplicates="drop") + 1
        top = all_preds[all_preds["decile"] == all_preds["decile"].max()]
        top_net = float(top["net_return"].mean())
        top_hit = float((top["net_return"] > 0).mean())
    except Exception:
        n_top = max(1, int(0.1 * len(all_preds)))
        top = all_preds.nlargest(n_top, "pred_log")
        top_net = float(top["net_return"].mean())
        top_hit = float((top["net_return"] > 0).mean())

    top_sharpe = _sharpe_monthly(top)
    overall_sharpe = _sharpe_monthly(all_preds)

    # Feature importance (averaged across folds)
    importances = importances / max(1, n_fits)
    total = importances.sum() or 1.0
    imp_dict = {f: round(float(v / total * 100), 2)
                for f, v in zip(feature_cols, importances)}
    imp_dict = dict(sorted(imp_dict.items(), key=lambda x: -x[1]))

    result = {
        "label": label,
        "features_used": feature_cols,
        "n_features": len(feature_cols),
        "folds": n_fits,
        "n_predictions": int(len(all_preds)),
        "r_squared_oos_linear": r2_linear,
        "r_squared_oos_log": r2_log,
        "spearman_oos": spearman,
        "hit_rate_net": hit_rate,
        "top_decile_net_return": top_net,
        "top_decile_hit_rate": top_hit,
        "top_decile_sharpe": top_sharpe,
        "overall_sharpe": overall_sharpe,
        "n_top_decile": int(len(top)),
        "feature_importance_pct": imp_dict,
    }

    print(f"\n=== [{label}] walk-forward ===")
    print(f"Folds: {n_fits}  N: {len(all_preds)}  Features: {len(feature_cols)}")
    print(f"R² (linear):         {r2_linear:+.4f}")
    print(f"R² (log space):      {r2_log:+.4f}")
    print(f"Spearman:            {spearman:+.4f}")
    print(f"Hit rate net:        {hit_rate*100:.1f}%")
    print(f"Top-decile net:      {top_net*100:+.2f}%  (hit {top_hit*100:.0f}%, n={len(top)})")
    print(f"Sharpe (top-decile): {top_sharpe:+.3f}")
    print(f"Sharpe (overall):    {overall_sharpe:+.3f}")
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild", action="store_true",
                    help="Force rebuild of cached training dataset.")
    ap.add_argument("--slim-k", type=int, default=12,
                    help="Top K features for v1_2_slim (default: 12).")
    ap.add_argument("--min-price", type=float, default=DEFAULT_MIN_PRICE_FILTER)
    args = ap.parse_args()

    with get_db() as db:
        df = load_or_build_dataset(db, force=args.rebuild)

        # Run 1: full feature set
        full_feats = list(FEATURE_COLUMNS)
        r_full = walkforward(df, db, full_feats, "v1_2_full", args.min_price)

        # Feature selection: take top K by fold-averaged importance
        top_feats = list(r_full["feature_importance_pct"].keys())[: args.slim_k]
        logger.info("Top %d features by importance: %s", args.slim_k, top_feats)

        # Run 2: slim feature set
        r_slim = walkforward(df, db, top_feats, "v1_2_slim", args.min_price)

    # v1_1 reference (from walkforward_20260413_015704.json)
    v1_1_ref = {
        "label": "v1_1 (reference)",
        "n_features": 31,
        "folds": 49,
        "n_predictions": 5094,
        "r_squared_oos_linear": 0.1923,
        "spearman_oos": 0.5062,
        "hit_rate_net": 0.0389,
        "top_decile_net_return": -0.1653,
        "top_decile_hit_rate": 0.1412,
        "top_decile_sharpe": 0.0,
        "overall_sharpe": 0.0,
        "n_top_decile": 510,
    }

    comparison = {
        "run_timestamp": dt.datetime.utcnow().isoformat(),
        "config": {
            "MIN_PSA10_PRICE": feat_mod.MIN_PSA10_PRICE,
            "HORIZON_DAYS": feat_mod.HORIZON_DAYS,
            "OUTLIER_TRIM_PCT": feat_mod.OUTLIER_TRIM_PCT,
            "target_transform": "log1p(net_realized_return)",
            "min_price_filter": args.min_price,
        },
        "v1_1_reference": v1_1_ref,
        "v1_2_full": r_full,
        "v1_2_slim": r_slim,
    }

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = MODELS_DIR / f"exp_v1_2_comparison_{ts}.json"
    out.write_text(json.dumps(comparison, indent=2, default=str))
    logger.info("Wrote %s", out)

    # Print comparison table
    rows = [v1_1_ref, r_full, r_slim]
    print("\n\n=========== COMPARISON ===========")
    print(f"{'Metric':<28} {'v1_1':>12} {'v1_2_full':>12} {'v1_2_slim':>12}")
    print("-" * 68)
    for key, label in [
        ("n_features", "Features"),
        ("folds", "Folds"),
        ("n_predictions", "N preds"),
        ("r_squared_oos_linear", "R² OOS (linear)"),
        ("spearman_oos", "Spearman OOS"),
        ("hit_rate_net", "Hit rate (net)"),
        ("top_decile_net_return", "Top-decile net return"),
        ("top_decile_hit_rate", "Top-decile hit rate"),
        ("top_decile_sharpe", "Top-decile Sharpe"),
    ]:
        vals = [r.get(key, float("nan")) for r in rows]
        fmt_vals = []
        for v in vals:
            if isinstance(v, (int, np.integer)):
                fmt_vals.append(f"{int(v):>12}")
            elif isinstance(v, float):
                if key in ("hit_rate_net", "top_decile_net_return", "top_decile_hit_rate"):
                    fmt_vals.append(f"{v*100:>11.2f}%")
                else:
                    fmt_vals.append(f"{v:>12.4f}")
            else:
                fmt_vals.append(f"{v!s:>12}")
        print(f"{label:<28}" + "".join(fmt_vals))
    print("==================================\n")


if __name__ == "__main__":
    main()
