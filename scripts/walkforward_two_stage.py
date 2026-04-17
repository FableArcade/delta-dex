"""Walk-forward backtest for the two-stage filter+rank architecture.

Same methodology as scripts/walkforward_ensemble.py (expanding-window,
monthly folds, friction-realistic returns) but trains both stages at
each fold and compares against the existing v1_3 ensemble baseline.

Run:  python -m scripts.walkforward_two_stage [--n-ensemble 10]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import r2_score

from db.connection import get_db
from pipeline.model.features import (
    FEATURE_COLUMNS, HORIZON_DAYS, TARGET_COL, build_training_dataset,
)
from pipeline.model.friction import EBAY_FVF, SHIPPING_COST
from pipeline.model.two_stage import (
    FEATURES_V2, CLF_PARAMS, REG_PARAMS,
    WINNER_WEIGHT, LOSER_WEIGHT, REVERSAL_PREMIUM,
    DEEP_DIP_THRESHOLD, SURVIVAL_THRESHOLD, CAL_FRACTION,
    _build_weights,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("walkforward_two_stage")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"

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


def _attach_prices(test: pd.DataFrame, db) -> pd.DataFrame:
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-ensemble", type=int, default=10)
    args = parser.parse_args()
    n_ens = args.n_ensemble

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

    peak_disc_idx = FEATURES_V2.index("peak_discount")
    rng = np.random.default_rng(seed=42)
    preds_list = []

    for m in months:
        train_full = df[df["anchor_date"] < m]
        test = df[(df["anchor_date"] >= m) &
                  (df["anchor_date"] < m + pd.DateOffset(months=1))]
        if len(train_full) < MIN_TRAIN_SAMPLES or len(test) == 0:
            continue

        # Split calibration from train
        train_full = train_full.sort_values("anchor_date")
        cal_n = max(20, int(len(train_full) * CAL_FRACTION))
        cal = train_full.tail(cal_n)
        train = train_full.head(len(train_full) - cal_n)

        X_train = train[FEATURES_V2].values
        y_train = train[TARGET_COL].values
        X_cal = cal[FEATURES_V2].values
        y_cal = cal[TARGET_COL].values
        X_test = test[FEATURES_V2].values

        y_train_log = np.log1p(np.clip(y_train, -0.999, None))

        # Stage 1: classifier
        clf = lgb.LGBMClassifier(**CLF_PARAMS)
        clf.fit(X_train, (y_train > 0).astype(int))
        proba_cal_raw = clf.predict_proba(X_cal)[:, 1]
        iso = IsotonicRegression(out_of_bounds="clip")
        iso.fit(proba_cal_raw, (y_cal > 0).astype(int))
        proba_test = iso.predict(clf.predict_proba(X_test)[:, 1])

        # Stage 2: asymmetrically-weighted bootstrap ensemble
        peak_disc_train = X_train[:, peak_disc_idx]
        weights = _build_weights(y_train, peak_disc_train)

        preds_per_model = np.zeros((n_ens, len(test)))
        for i in range(n_ens):
            idx = rng.integers(0, len(X_train), size=len(X_train))
            params = {**REG_PARAMS, "seed": 42 + i}
            model = lgb.LGBMRegressor(**params)
            model.fit(X_train[idx], y_train_log[idx], sample_weight=weights[idx])
            preds_per_model[i] = np.expm1(model.predict(X_test))
        ens_median = np.median(preds_per_model, axis=0)

        # Combined score: regression prediction × survival probability
        combined = ens_median * proba_test

        slice_df = test[["card_id", "anchor_date", TARGET_COL]].copy()
        slice_df["pred"] = ens_median
        slice_df["combined"] = combined
        slice_df["survival_prob"] = proba_test
        slice_df["peak_discount"] = X_test[:, peak_disc_idx]
        preds_list.append(slice_df)
        logger.info("fold %s train=%d cal=%d test=%d",
                     m.date(), len(train), len(cal), len(test))

    all_preds = pd.concat(preds_list, ignore_index=True)
    with get_db() as db:
        all_preds = _attach_prices(all_preds, db)
    all_preds = all_preds.dropna(subset=["buy_price", "sell_price"])
    all_preds = all_preds[all_preds["buy_price"] >= MIN_PRICE_FILTER].copy()
    all_preds["net_return"] = [
        _net_return(b, s)
        for b, s in zip(all_preds["buy_price"], all_preds["sell_price"])
    ]
    all_preds = all_preds.dropna(subset=["net_return"])
    logger.info("Total priced predictions: %d", len(all_preds))

    y = all_preds[TARGET_COL].values
    yhat = all_preds["pred"].values
    combined = all_preds["combined"].values

    # --- Unfiltered metrics (apples-to-apples vs v1_3) ---
    sp = float(spearmanr(y, yhat).statistic)
    r2 = float(r2_score(y, yhat))
    hit = float((all_preds["net_return"] > 0).mean())

    # Top cohorts ranked by COMBINED score (filter × rank)
    n_top2 = max(1, int(round(0.02 * len(all_preds))))
    n_top1 = max(1, int(round(0.01 * len(all_preds))))
    top2 = all_preds.nlargest(n_top2, "combined")
    top1 = all_preds.nlargest(n_top1, "combined")

    top2_net = float(top2["net_return"].mean())
    top2_hit = float((top2["net_return"] > 0).mean())
    top2_sharpe = _monthly_sharpe(top2)
    top1_net = float(top1["net_return"].mean())
    top1_hit = float((top1["net_return"] > 0).mean())
    top1_sharpe = _monthly_sharpe(top1)

    # --- Filtered metrics (after survival filter) ---
    survived = all_preds[all_preds["survival_prob"] >= SURVIVAL_THRESHOLD]
    filt_hit = float((survived["net_return"] > 0).mean()) if len(survived) > 0 else 0

    # --- Reversal-specific: deep-dip survivors ---
    dip = all_preds[all_preds["peak_discount"] > DEEP_DIP_THRESHOLD]
    dip_survived = dip[dip["survival_prob"] >= SURVIVAL_THRESHOLD]
    dip_surv_hit = float((dip_survived["net_return"] > 0).mean()) if len(dip_survived) > 0 else 0

    # Top-decile for comparison
    all_preds["decile"] = pd.qcut(all_preds["combined"], 10, labels=False, duplicates="drop") + 1
    top_dec = all_preds[all_preds["decile"] == all_preds["decile"].max()]
    top_dec_net = float(top_dec["net_return"].mean())
    top_dec_hit = float((top_dec["net_return"] > 0).mean())
    top_dec_sharpe = _monthly_sharpe(top_dec)

    out = {
        "architecture": "two_stage_filter_rank",
        "n_ensemble": n_ens,
        "folds": len(preds_list),
        "n_predictions": int(len(all_preds)),
        "collider_dropped": "psa_10_vs_raw_pct",
        "spearman_oos": sp,
        "r_squared_oos": r2,
        "hit_rate": hit,
        "top_decile_net": top_dec_net,
        "top_decile_hit": top_dec_hit,
        "top_decile_sharpe": top_dec_sharpe,
        "top2_net": top2_net,
        "top2_hit": top2_hit,
        "top2_sharpe": top2_sharpe,
        "n_top2": int(n_top2),
        "top1_net": top1_net,
        "top1_hit": top1_hit,
        "top1_sharpe": top1_sharpe,
        "n_top1": int(n_top1),
        "filtered_hit_rate": filt_hit,
        "n_survived": int(len(survived)),
        "reversal_dip_hit_rate": dip_surv_hit,
        "n_dip_survived": int(len(dip_survived)),
    }

    # Load v1_3 baseline for comparison
    baseline_path = sorted(MODELS_DIR.glob("walkforward_ensemble_2026*.json"))
    baseline = None
    for p in reversed(baseline_path):
        try:
            baseline = json.loads(p.read_text())
            break
        except Exception:
            continue

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    outp = MODELS_DIR / f"walkforward_two_stage_{ts}.json"
    outp.write_text(json.dumps(out, indent=2, default=str))
    logger.info("Wrote %s", outp)

    # Print comparison
    print("\n" + "=" * 80)
    print(f"TWO-STAGE FILTER+RANK vs V1_3 ENSEMBLE  (n_ens={n_ens})")
    print("=" * 80)
    fmt = "{:<30} {:>14} {:>14}  {:>10}"
    print(fmt.format("Metric", "V1_3 Baseline", "Two-Stage", "Δ"))
    print("-" * 80)
    for key, label in [
        ("spearman_oos", "Spearman"),
        ("r_squared_oos", "R² OOS"),
        ("hit_rate", "Hit rate"),
        ("top_decile_sharpe", "Top-decile Sharpe"),
        ("top_decile_net", "Top-decile net"),
        ("top2_sharpe", "Top-2% Sharpe"),
        ("top2_net", "Top-2% net"),
        ("top2_hit", "Top-2% hit rate"),
        ("top1_net", "Top-1% net"),
        ("top1_hit", "Top-1% hit rate"),
    ]:
        b_key = key.replace("_net", "_net_return").replace("_hit", "_hit_rate") if baseline else key
        b = baseline.get(b_key, baseline.get(key)) if baseline else None
        e = out.get(key)
        if b is not None and e is not None:
            diff = e - b
            mark = "✓" if diff > 0 else "✗" if diff < 0 else "·"
            print(fmt.format(label, f"{b:.4f}", f"{e:.4f}", f"{diff:+.4f} {mark}"))
        elif e is not None:
            print(fmt.format(label, "—", f"{e:.4f}", ""))

    print("-" * 80)
    print(f"{'Filtered hit rate':<30} {'—':>14} {filt_hit:>14.4f}")
    print(f"{'Survived cards':<30} {'—':>14} {len(survived):>14}")
    print(f"{'Reversal (dip) hit rate':<30} {'—':>14} {dip_surv_hit:>14.4f}")
    print(f"{'Deep-dip survived cards':<30} {'—':>14} {len(dip_survived):>14}")
    print()


if __name__ == "__main__":
    main()
