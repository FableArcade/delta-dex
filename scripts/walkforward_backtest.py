"""Expanded walk-forward backtest over the full price history.

For each month M in the evaluation window:
  1. Take every training sample with anchor_date < M (expanding window).
  2. Fit a median LightGBM quantile model on those.
  3. Predict for every training sample whose anchor_date falls in [M, M+1).
  4. Record predictions + realized 90d net-of-cost return.

Aggregate across months -> R^2, Sharpe, hit rate, top-decile net return.
Feeds the promotion gate (pipeline/model/promotion_gate.py).

Usage:
  python scripts/walkforward_backtest.py           # full window
  python scripts/walkforward_backtest.py --quick   # 6-month smoke slice

Output JSON: data/models/walkforward_<timestamp>.json

We reuse pipeline.model.features.build_training_dataset as-is; walk-
forward only changes the split, not the features. Because the training
set already contains (card, anchor_date, features..., target), per-month
slicing is cheap — we don't rebuild features per fold.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import lightgbm as lgb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy.stats import spearmanr  # noqa: E402
from sklearn.metrics import r2_score  # noqa: E402

from db.connection import get_db  # noqa: E402
from pipeline.model.features import (  # noqa: E402
    FEATURE_COLUMNS,
    HORIZON_DAYS,
    TARGET_COL,
    build_training_dataset,
)
from pipeline.model.friction import EBAY_FVF, SHIPPING_COST  # noqa: E402
from pipeline.model.promotion_gate import evaluate_and_record  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("walkforward")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

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


def _net_return(buy: float, sell: float) -> float:
    if buy is None or sell is None or buy <= 0 or sell <= 0:
        return float("nan")
    eb = buy * (1 + BUY_SLIPPAGE)
    es = sell * (1 - SELL_SLIPPAGE)
    net = es * (1 - EBAY_FVF) - SHIPPING_COST
    return (net - eb) / eb


def _attach_prices(test: pd.DataFrame, db: sqlite3.Connection) -> pd.DataFrame:
    """Same logic as realistic_backtest._attach_prices, inlined to avoid
    circular script imports. buy price = first PSA 10 <= anchor+14d;
    sell price = first PSA 10 >= anchor+90d (within 30d grace)."""
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
        # Buy window widened to 31d to match the monthly anchor resample
        # in features.build_training_dataset — with $10 price floor many
        # cards have sparse monthly-cadence PSA 10 prints.
        bg = g[(g["date"] >= a) & (g["date"] <= a + pd.Timedelta(days=31))]
        sg = g[(g["date"] >= f) & (g["date"] <= f + pd.Timedelta(days=30))]
        buy.append(float(bg.iloc[0]["psa_10_price"]) if not bg.empty else None)
        sell.append(float(sg.iloc[0]["psa_10_price"]) if not sg.empty else None)
    test = test.copy()
    test["buy_price"] = buy
    test["sell_price"] = sell
    return test


def run(quick: bool = False, record_gate: bool = True,
        model_version: str | None = None,
        min_price: float = DEFAULT_MIN_PRICE_FILTER) -> dict:
    with get_db() as db:
        return _run(db, quick=quick, record_gate=record_gate,
                    model_version=model_version, min_price=min_price)


def _sharpe(returns: np.ndarray | pd.Series,
            min_trades_per_month: int = MIN_TRADES_PER_MONTH) -> float:
    """Nan-safe annualized Sharpe from a per-observation series of net returns.

    Expects a pandas Series indexed by anchor_date (or equivalent) or a
    DataFrame already aggregated per-month. Returns 0.0 when there is
    insufficient data rather than NaN.
    """
    if isinstance(returns, pd.Series) and len(returns) == 0:
        return 0.0
    arr = np.asarray(returns, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) < 2:
        return 0.0
    std = float(np.nanstd(arr, ddof=1))
    if not np.isfinite(std) or std <= 0:
        return 0.0
    mean = float(np.nanmean(arr))
    return float(mean / std * np.sqrt(12))


def _monthly_sharpe(df: pd.DataFrame) -> float:
    """Compute annualized Sharpe from monthly-mean net returns, requiring
    >= MIN_TRADES_PER_MONTH in each included month."""
    if df.empty:
        return 0.0
    grp = df.groupby(df["anchor_date"].dt.to_period("M"))["net_return"]
    counts = grp.count()
    means = grp.mean()
    keep = counts[counts >= MIN_TRADES_PER_MONTH].index
    monthly = means.loc[keep].dropna()
    return _sharpe(monthly.values)


def _run(db: sqlite3.Connection, quick: bool, record_gate: bool,
         model_version: str | None, min_price: float) -> dict:
    logger.info("Building training dataset...")
    df = build_training_dataset(db)
    if df.empty:
        raise SystemExit("No training samples — aborting.")
    df["anchor_date"] = pd.to_datetime(df["anchor_date"])
    logger.info("Samples: %d  span: %s .. %s",
                len(df), df["anchor_date"].min().date(),
                df["anchor_date"].max().date())

    # Build month boundaries.
    start = df["anchor_date"].min().to_period("M").to_timestamp() + pd.DateOffset(months=6)
    end = df["anchor_date"].max().to_period("M").to_timestamp()
    months = pd.date_range(start, end, freq="MS")

    if quick:
        # Smoke test: last 6 months only.
        months = months[-6:] if len(months) > 6 else months
        logger.info("--quick mode: %d months (%s .. %s)",
                    len(months), months[0].date(), months[-1].date())

    preds = []
    for m in months:
        train = df[df["anchor_date"] < m]
        test = df[(df["anchor_date"] >= m)
                  & (df["anchor_date"] < m + pd.DateOffset(months=1))]
        if len(train) < MIN_TRAIN_SAMPLES or len(test) == 0:
            logger.info("skip %s  train=%d test=%d (below min)",
                        m.date(), len(train), len(test))
            continue
        model = lgb.LGBMRegressor(**BASE_PARAMS)
        y_train_log = np.log1p(np.clip(train[TARGET_COL].values, -0.999, None))
        model.fit(train[FEATURE_COLUMNS].values, y_train_log)
        p = np.expm1(model.predict(test[FEATURE_COLUMNS].values))
        slice_df = test[["card_id", "anchor_date", TARGET_COL]].copy()
        slice_df["pred"] = p
        slice_df["fold_month"] = m.strftime("%Y-%m")
        preds.append(slice_df)
        logger.info("fold %s  train=%d test=%d", m.date(), len(train), len(test))

    if not preds:
        raise SystemExit("No walk-forward folds produced predictions.")

    all_preds = pd.concat(preds, ignore_index=True)
    all_preds = _attach_prices(all_preds, db)
    pre_n = len(all_preds)
    all_preds = all_preds.dropna(subset=["buy_price", "sell_price"])
    all_preds = all_preds[all_preds["buy_price"] >= min_price].copy()
    logger.info("Priced & $%.0f-filter: %d -> %d", min_price,
                pre_n, len(all_preds))

    all_preds["net_return"] = [
        _net_return(b, s)
        for b, s in zip(all_preds["buy_price"], all_preds["sell_price"])
    ]
    all_preds = all_preds.dropna(subset=["net_return"])

    # Metrics
    y = all_preds[TARGET_COL].values
    yhat = all_preds["pred"].values
    r2 = float(r2_score(y, yhat)) if len(y) > 1 else float("nan")
    sp = float(spearmanr(y, yhat).statistic) if len(y) > 1 else float("nan")

    hit_rate = float((all_preds["net_return"] > 0).mean())

    # Top decile by prediction
    try:
        all_preds["decile"] = pd.qcut(all_preds["pred"], 10,
                                      labels=False, duplicates="drop") + 1
        top = all_preds[all_preds["decile"] == all_preds["decile"].max()]
        top_decile_net = float(top["net_return"].mean())
        top_hit = float((top["net_return"] > 0).mean())
    except Exception as e:
        logger.warning("decile bucketing failed (%s); falling back to top 10%%", e)
        n_top = max(1, int(0.1 * len(all_preds)))
        top = all_preds.nlargest(n_top, "pred")
        top_decile_net = float(top["net_return"].mean())
        top_hit = float((top["net_return"] > 0).mean())

    # Annualized Sharpe: compute both overall and top-decile using nan-safe
    # stats, requiring >= MIN_TRADES_PER_MONTH per month for inclusion.
    # Returns 0.0 (not NaN) when there's insufficient data.
    top_decile_sharpe = _monthly_sharpe(top)
    overall_sharpe = _monthly_sharpe(all_preds)
    n_top_decile = int(len(top))

    # Top 2% cohort — the actual pick list size we'd trade.
    n_top2 = max(1, int(round(0.02 * len(all_preds))))
    top2 = all_preds.nlargest(n_top2, "pred")
    top2_net = float(top2["net_return"].mean())
    top2_hit = float((top2["net_return"] > 0).mean())
    top2_sharpe = _monthly_sharpe(top2)

    # Top 1% — tightest conviction slice.
    n_top1 = max(1, int(round(0.01 * len(all_preds))))
    top1 = all_preds.nlargest(n_top1, "pred")
    top1_net = float(top1["net_return"].mean())
    top1_hit = float((top1["net_return"] > 0).mean())
    top1_sharpe = _monthly_sharpe(top1)

    version = model_version or f"walkforward_{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    metrics = {
        "model_version": version,
        "quick": quick,
        "folds": len(preds),
        "n_predictions": int(len(all_preds)),
        "r_squared_oos": r2,
        "spearman_oos": sp,
        "hit_rate": hit_rate,
        "top_decile_net_return": top_decile_net,
        "top_decile_hit_rate": top_hit,
        "top_decile_sharpe": top_decile_sharpe,
        "overall_sharpe": overall_sharpe,
        # Back-compat: legacy "sharpe" key mirrors top-decile Sharpe
        # (the strategy we actually trade).
        "sharpe": top_decile_sharpe,
        "n_top_decile": n_top_decile,
        "top2_net_return": top2_net,
        "top2_hit_rate": top2_hit,
        "top2_sharpe": top2_sharpe,
        "n_top2": int(n_top2),
        "top1_net_return": top1_net,
        "top1_hit_rate": top1_hit,
        "top1_sharpe": top1_sharpe,
        "n_top1": int(n_top1),
        "friction": {
            "ebay_fvf": EBAY_FVF, "shipping": SHIPPING_COST,
            "buy_slippage": BUY_SLIPPAGE, "sell_slippage": SELL_SLIPPAGE,
        },
        "min_price_filter": min_price,
    }

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = MODELS_DIR / f"walkforward_{ts}.json"
    out.write_text(json.dumps(metrics, indent=2, default=str))
    logger.info("Wrote %s", out)

    print("\n=== WALK-FORWARD RESULTS ===")
    print(f"Folds: {metrics['folds']}  N: {metrics['n_predictions']}")
    print(f"R^2 (oos):          {r2:+.4f}")
    print(f"Spearman (oos):     {sp:+.4f}")
    print(f"Hit rate (net):     {hit_rate*100:.1f}%")
    print(f"Top-decile net:     {top_decile_net*100:+.2f}%  (hit {top_hit*100:.0f}%, n={n_top_decile})")
    print(f"Top-2% net:         {top2_net*100:+.2f}%  (hit {top2_hit*100:.0f}%, n={n_top2})")
    print(f"Top-1% net:         {top1_net*100:+.2f}%  (hit {top1_hit*100:.0f}%, n={n_top1})")
    print(f"Sharpe (top-decile): {top_decile_sharpe:+.3f}")
    print(f"Sharpe (top-2%):     {top2_sharpe:+.3f}")
    print(f"Sharpe (top-1%):     {top1_sharpe:+.3f}")
    print(f"Sharpe (overall):    {overall_sharpe:+.3f}")

    if record_gate and not quick:
        with get_db() as gdb:
            decision = evaluate_and_record(gdb, metrics, version)
        metrics["gate_decision"] = decision.as_dict()
        print(f"Gate: {decision.decision} — {decision.reason}")
    elif quick:
        print("Gate: skipped (--quick)")

    return metrics


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--quick", action="store_true",
                   help="Run last 6 months only; skip gate write.")
    p.add_argument("--no-gate", action="store_true",
                   help="Skip writing a gate decision.")
    p.add_argument("--model-version", default=None)
    p.add_argument("--min-price", type=float, default=DEFAULT_MIN_PRICE_FILTER,
                   help="Minimum buy price filter in USD (default: 25).")
    args = p.parse_args()
    run(quick=args.quick, record_gate=not args.no_gate,
        model_version=args.model_version, min_price=args.min_price)


if __name__ == "__main__":
    main()
