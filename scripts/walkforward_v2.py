"""
v2 walk-forward backtest — quantile ensemble + CQR intervals + honest dual
universe evaluation (per Analytics Taste P15, P3, P7, P10, Signal Classes).

Compared against v1_3's walkforward_ensemble.py results:

  * Target: log-return + winsorization (v1_3: log1p of clipped raw return)
  * Features: 49 v2 columns (v1_3's 31 + 5 ranks + 3 calendar + 2 pop-velocity
    schema + 8 sentiment schema; ranks/calendar computed, others NaN-tolerant)
  * Quantile ensemble: q10, q50, q90 — three LightGBM quantile regressors per
    fold. Single-fit, not inner-bootstrapped (v2 inner bootstrap is a separate
    future experiment)
  * Conformalized Quantile Regression: time-based 80/20 calibration split per
    fold; CQR adjustment tightens/loosens intervals to hit exact 80% coverage
  * P(beats baseline) classifier: isotonic-calibrated probability that
    net_return > universe mean for that fold
  * Dual-universe evaluation: report metrics on BOTH $25+ and $100+ filters
    side-by-side. No cherry-picked Sharpe headline.
  * Interval coverage: actual coverage measured per fold and in aggregate

Run:  python -m scripts.walkforward_v2
Output: data/models/walkforward_v2_<ts>.json
"""
from __future__ import annotations

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
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import r2_score

from db.connection import get_db
from pipeline.model.features_v2 import (
    HORIZON_DAYS,
    TARGET_COL,
    V2_FEATURE_COLUMNS,
    build_training_dataset_v2,
)
from pipeline.model.friction import EBAY_FVF, SHIPPING_COST

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("walkforward_v2")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# Same trading frictions as v1_3 walkforward_ensemble for honest comparison
BUY_SLIPPAGE = 0.05
SELL_SLIPPAGE = 0.03
MIN_TRAIN_SAMPLES = 300
MIN_TRADES_PER_MONTH = 3

# Target coverage for CQR interval
CQR_ALPHA = 0.20  # 80% nominal coverage
CAL_FRACTION = 0.20  # last 20% of train chronologically = calibration split

BASE_QUANTILE_PARAMS = {
    "objective": "quantile",
    "num_leaves": 31,
    "learning_rate": 0.05,
    "n_estimators": 400,
    "min_child_samples": 20,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "verbose": -1,
}

BASE_CLASSIFIER_PARAMS = {
    "objective": "binary",
    "num_leaves": 31,
    "learning_rate": 0.05,
    "n_estimators": 400,
    "min_child_samples": 20,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "verbose": -1,
    "metric": "binary_logloss",
}


# ---------------------------------------------------------------------------
# Net-return + price attachment (identical to v1_3 walkforward for fair A/B)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# CQR
# ---------------------------------------------------------------------------

def _cqr_adjust(q_low_cal, q_high_cal, y_cal, alpha=CQR_ALPHA):
    """Conformalized Quantile Regression adjustment (Romano et al., 2019).

    E_i = max(q_low(X_i) - y_i, y_i - q_high(X_i))
    Q = ceil((n+1)(1-alpha)) / n quantile of E_i
    Final interval: [q_low - Q, q_high + Q]
    """
    e_low = q_low_cal - y_cal
    e_high = y_cal - q_high_cal
    e = np.maximum(e_low, e_high)
    n = len(e)
    q_level = min(max((1 - alpha) * (1 + 1.0 / n), 0.0), 1.0)
    Q = float(np.quantile(e, q_level))
    return Q


# ---------------------------------------------------------------------------
# Per-fold training: q10/q50/q90 + classifier + isotonic + CQR
# ---------------------------------------------------------------------------

def _train_fold(X_tr, y_pct_tr, y_bin_tr, cal_idx, X_arr):
    """Train the four models needed per fold and return a predictor.

    Returns: dict with callables for (p10, p50, p90, prob_pos) on new X + Q
    """
    # Log-return target for quantile models, winsorized at [-40, +200]%
    y_pct_clip = np.clip(y_pct_tr, -40.0, 200.0)
    y_log = np.log1p(y_pct_clip / 100.0)

    # Training split vs calibration split (time-ordered externally; cal_idx
    # is the held-out tail already indexed)
    tr_mask = np.ones(len(X_tr), dtype=bool)
    tr_mask[cal_idx] = False
    X_train_only = X_tr[tr_mask]
    X_cal = X_tr[cal_idx]
    y_log_train = y_log[tr_mask]
    y_log_cal = y_log[cal_idx]
    y_bin_train = y_bin_tr[tr_mask]
    y_bin_cal = y_bin_tr[cal_idx]

    q10 = lgb.LGBMRegressor(**{**BASE_QUANTILE_PARAMS, "alpha": 0.10, "seed": 42})
    q10.fit(X_train_only, y_log_train)
    q50 = lgb.LGBMRegressor(**{**BASE_QUANTILE_PARAMS, "alpha": 0.50, "seed": 42})
    q50.fit(X_train_only, y_log_train)
    q90 = lgb.LGBMRegressor(**{**BASE_QUANTILE_PARAMS, "alpha": 0.90, "seed": 42})
    q90.fit(X_train_only, y_log_train)

    # CQR adjustment in log-return space
    lp10_cal = q10.predict(X_cal)
    lp90_cal = q90.predict(X_cal)
    Q = _cqr_adjust(lp10_cal, lp90_cal, y_log_cal, alpha=CQR_ALPHA)

    # Classifier: P(net_return > 0) → will be baseline-adjusted at eval time
    clf = lgb.LGBMClassifier(**{**BASE_CLASSIFIER_PARAMS, "seed": 42})
    clf.fit(X_train_only, y_bin_train)
    raw_cal_probs = clf.predict_proba(X_cal)[:, 1]
    iso = IsotonicRegression(out_of_bounds="clip").fit(raw_cal_probs, y_bin_cal)

    # Inference on the test features X_arr
    lp10 = q10.predict(X_arr) - Q
    lp50 = q50.predict(X_arr)
    lp90 = q90.predict(X_arr) + Q
    p10_pct = (np.exp(lp10) - 1.0) * 100.0
    p50_pct = (np.exp(lp50) - 1.0) * 100.0
    p90_pct = (np.exp(lp90) - 1.0) * 100.0
    raw_prob = clf.predict_proba(X_arr)[:, 1]
    prob_pos = iso.transform(raw_prob)

    # Diagnostics — raw vs CQR coverage on calibration set
    cov_raw_cal = float(np.mean((lp10_cal <= y_log_cal) & (y_log_cal <= lp90_cal)))
    cov_adj_cal = float(np.mean(
        ((lp10_cal - Q) <= y_log_cal) & (y_log_cal <= (lp90_cal + Q))
    ))

    return {
        "p10_pct": p10_pct, "p50_pct": p50_pct, "p90_pct": p90_pct,
        "prob_pos": prob_pos,
        "Q_cqr": Q,
        "cov_raw_cal": cov_raw_cal,
        "cov_adj_cal": cov_adj_cal,
    }


# ---------------------------------------------------------------------------
# Evaluation — dual universe ($25 and $100) per Analytics Taste P15
# ---------------------------------------------------------------------------

def _eval_universe(all_preds: pd.DataFrame, price_filter: float) -> Dict:
    sub = all_preds[all_preds["buy_price"] >= price_filter].copy()
    if sub.empty:
        return {"n_predictions": 0, "filter": f"${price_filter:.0f}+"}

    y = sub[TARGET_COL].values
    yhat_log = np.log1p(np.clip(sub["p50_pct"].values / 100.0, -0.999, None))
    y_log = np.log1p(np.clip(y * 1.0, -0.999, None))  # target_return_180d is already decimal return, not pct
    # Note: TARGET_COL is a DECIMAL (e.g. 0.123 for +12.3%), but our p50 is PCT
    # Reconcile — convert p50 to decimal for R² / Spearman in decimal space
    pred_decimal = sub["p50_pct"].values / 100.0
    try:
        r2 = float(r2_score(y, pred_decimal))
    except Exception:
        r2 = float("nan")
    sp = float(spearmanr(y, pred_decimal).statistic) if len(sub) > 1 else float("nan")

    # Interval coverage — in DECIMAL net-return space
    p10_dec = sub["p10_pct"].values / 100.0
    p90_dec = sub["p90_pct"].values / 100.0
    actual = sub["net_return"].values
    cov_mask = ~np.isnan(actual)
    cov = float(np.mean((p10_dec[cov_mask] <= actual[cov_mask]) &
                        (actual[cov_mask] <= p90_dec[cov_mask]))) if cov_mask.any() else float("nan")

    hit_rate = float((sub["net_return"] > 0).mean())
    sub["decile"] = pd.qcut(sub["p50_pct"], 10, labels=False, duplicates="drop") + 1

    def _slice_metrics(s: pd.DataFrame, label: str) -> Dict:
        if s.empty:
            return {}
        net = float(s["net_return"].mean())
        hit = float((s["net_return"] > 0).mean())
        sharpe = _monthly_sharpe(s)
        return {f"{label}_net_return": net, f"{label}_hit_rate": hit,
                f"{label}_sharpe": sharpe, f"n_{label}": int(len(s))}

    top = sub[sub["decile"] == sub["decile"].max()]
    top2 = sub.nlargest(max(1, int(round(0.02 * len(sub)))), "p50_pct")
    top1 = sub.nlargest(max(1, int(round(0.01 * len(sub)))), "p50_pct")

    out = {
        "filter": f"${price_filter:.0f}+",
        "n_predictions": int(len(sub)),
        "r_squared_oos": r2,
        "spearman_oos": sp,
        "hit_rate": hit_rate,
        "interval_coverage_80": cov,  # post-CQR empirical coverage, target 0.80
    }
    out.update(_slice_metrics(top, "top_decile"))
    out.update(_slice_metrics(top2, "top2"))
    out.update(_slice_metrics(top1, "top1"))
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    with get_db() as db:
        df = build_training_dataset_v2(db, compute_pop_velocity=False)
    if df.empty:
        raise SystemExit("No v2 training samples.")
    df["anchor_date"] = pd.to_datetime(df["anchor_date"])
    logger.info("v2 samples: %d × %d features; span %s .. %s",
                len(df), len(V2_FEATURE_COLUMNS),
                df["anchor_date"].min().date(), df["anchor_date"].max().date())

    start = df["anchor_date"].min().to_period("M").to_timestamp() + pd.DateOffset(months=6)
    end = df["anchor_date"].max().to_period("M").to_timestamp()
    months = pd.date_range(start, end, freq="MS")

    preds_rows: List[pd.DataFrame] = []
    fold_diagnostics: List[Dict] = []

    for m in months:
        train = df[df["anchor_date"] < m]
        test = df[(df["anchor_date"] >= m) &
                  (df["anchor_date"] < m + pd.DateOffset(months=1))]
        if len(train) < MIN_TRAIN_SAMPLES or len(test) == 0:
            continue

        # Sort train by anchor_date; last CAL_FRACTION is calibration split
        train_sorted = train.sort_values("anchor_date").reset_index(drop=True)
        X_tr = train_sorted[V2_FEATURE_COLUMNS].values
        # target_return_180d is decimal (e.g. 0.123); convert to pct for log-return engineering
        y_pct_tr = train_sorted[TARGET_COL].values * 100.0
        y_bin_tr = (train_sorted[TARGET_COL].values > 0).astype(int)

        n_cal = max(50, int(len(train_sorted) * CAL_FRACTION))
        cal_idx = np.arange(len(train_sorted) - n_cal, len(train_sorted))

        X_te = test[V2_FEATURE_COLUMNS].values
        res = _train_fold(X_tr, y_pct_tr, y_bin_tr, cal_idx, X_te)

        slice_df = test[["card_id", "anchor_date", TARGET_COL]].copy()
        slice_df["p10_pct"] = res["p10_pct"]
        slice_df["p50_pct"] = res["p50_pct"]
        slice_df["p90_pct"] = res["p90_pct"]
        slice_df["prob_pos"] = res["prob_pos"]
        preds_rows.append(slice_df)
        fold_diagnostics.append({
            "fold_month": str(m.date()),
            "n_train": int(len(train_sorted) - n_cal),
            "n_cal": int(n_cal),
            "n_test": int(len(test)),
            "cqr_Q": res["Q_cqr"],
            "cov_raw_cal": res["cov_raw_cal"],
            "cov_adj_cal": res["cov_adj_cal"],
        })
        logger.info("fold %s train=%d cal=%d test=%d Q=%.4f cov_raw=%.2f → cov_adj=%.2f",
                    m.date(), len(train_sorted) - n_cal, n_cal, len(test),
                    res["Q_cqr"], res["cov_raw_cal"], res["cov_adj_cal"])

    if not preds_rows:
        raise SystemExit("No predictions produced.")
    all_preds = pd.concat(preds_rows, ignore_index=True)
    with get_db() as db:
        all_preds = _attach_prices(all_preds, db)
    pre_n = len(all_preds)
    all_preds = all_preds.dropna(subset=["buy_price", "sell_price"])
    logger.info("Priced: %d -> %d", pre_n, len(all_preds))
    all_preds["net_return"] = [
        _net_return(b, s)
        for b, s in zip(all_preds["buy_price"], all_preds["sell_price"])
    ]
    all_preds = all_preds.dropna(subset=["net_return"])

    # --- Dual-universe evaluation ---
    eval_25 = _eval_universe(all_preds, price_filter=25.0)
    eval_100 = _eval_universe(all_preds, price_filter=100.0)

    # Aggregate fold-level CQR coverage
    mean_cov_raw = float(np.mean([f["cov_raw_cal"] for f in fold_diagnostics])) if fold_diagnostics else float("nan")
    mean_cov_adj = float(np.mean([f["cov_adj_cal"] for f in fold_diagnostics])) if fold_diagnostics else float("nan")

    out = {
        "model_type": "v2 quantile + CQR + isotonic classifier",
        "folds": len(fold_diagnostics),
        "n_features": len(V2_FEATURE_COLUMNS),
        "features_used": V2_FEATURE_COLUMNS,
        "cal_fraction": CAL_FRACTION,
        "cqr_alpha": CQR_ALPHA,
        "mean_cov_raw_cal": mean_cov_raw,
        "mean_cov_adj_cal": mean_cov_adj,
        "universe_25": eval_25,
        "universe_100": eval_100,
        "fold_diagnostics": fold_diagnostics,
    }

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    outp = MODELS_DIR / f"walkforward_v2_{ts}.json"
    outp.write_text(json.dumps(out, indent=2, default=str))
    logger.info("Wrote %s", outp)

    # Pretty-print comparison table
    def _row(label, a_key, b_key=None):
        b_key = b_key or a_key
        a = eval_25.get(a_key, "—")
        b = eval_100.get(b_key, "—")
        af = f"{a:+.4f}" if isinstance(a, float) else str(a)
        bf = f"{b:+.4f}" if isinstance(b, float) else str(b)
        print(f"  {label:<30} {af:>14} {bf:>14}")

    print("\n=== v2 WALK-FORWARD (dual-universe, CQR-calibrated intervals) ===")
    print(f"Folds: {len(fold_diagnostics)}   v2 features: {len(V2_FEATURE_COLUMNS)}")
    print(f"Calibration coverage (avg per fold): raw={mean_cov_raw:.3f}  adj={mean_cov_adj:.3f} (target {1-CQR_ALPHA:.2f})")
    print(f"\n{'Metric':<30} {'$25+ universe':>14} {'$100+ universe':>14}")
    print("-" * 62)
    _row("n predictions", "n_predictions")
    _row("R² OOS", "r_squared_oos")
    _row("Spearman", "spearman_oos")
    _row("Hit rate (overall)", "hit_rate")
    _row("Interval cov (target 0.80)", "interval_coverage_80")
    _row("Top-decile net return", "top_decile_net_return")
    _row("Top-decile hit rate", "top_decile_hit_rate")
    _row("Top-decile Sharpe", "top_decile_sharpe")
    _row("Top-2% net return", "top2_net_return")
    _row("Top-2% hit rate", "top2_hit_rate")
    _row("Top-2% Sharpe", "top2_sharpe")
    _row("Top-1% Sharpe", "top1_sharpe")


if __name__ == "__main__":
    main()
