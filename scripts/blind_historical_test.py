"""Blind historical portfolio simulation.

For each month M in the evaluation window:
  1. Train the v1_3 model on samples with anchor_date strictly < M.
  2. Predict returns for samples anchored in [M, M+1).
  3. Pick the top 2% by projected return, subject to a $100 min price
     filter (the same universe the live /model/picks endpoint uses).
  4. "Buy" equal-weight at the anchor-month PSA 10 price, hold 180 days,
     "sell" at the anchor+180d price. Net of eBay FVF 13% + $5 shipping
     + 5% buy slippage + 3% sell slippage.
  5. Compound monthly returns into a portfolio curve; compare vs a
     random-pick baseline drawn from the same $100+ universe.

This is what an investor following the model would have actually earned
if they'd deployed v1_3 at the start of the evaluation window with only
data available at that moment — the model never sees the future.

Reports monthly P&L, cumulative return, hit rate, max drawdown, and
Sharpe. Writes a JSON artefact for later inspection.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import lightgbm as lgb
import numpy as np
import pandas as pd

from db.connection import get_db
from pipeline.model.features import (
    FEATURE_COLUMNS, HORIZON_DAYS, TARGET_COL, build_training_dataset,
)
from pipeline.model.friction import EBAY_FVF, SHIPPING_COST

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("blind_historical")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"

BASE_PARAMS = {
    "objective": "quantile", "alpha": 0.5,
    "num_leaves": 31, "learning_rate": 0.05,
    "n_estimators": 500, "min_child_samples": 20,
    "subsample": 0.8, "colsample_bytree": 0.8,
    "reg_alpha": 0.1, "reg_lambda": 1.0,
    "verbose": -1,
}

BUY_SLIPPAGE = 0.05
SELL_SLIPPAGE = 0.03
MIN_TRAIN_SAMPLES = 300
MIN_PRICE = 100.0       # same filter the live /picks uses
TOP_FRACTION = 0.02     # top-2% cohort
N_RANDOM_BASELINE = 50  # draws per month for random baseline variance


def _net_return(buy: float, sell: float) -> float:
    if buy is None or sell is None or buy <= 0 or sell <= 0:
        return float("nan")
    eb = buy * (1 + BUY_SLIPPAGE)
    es = sell * (1 - SELL_SLIPPAGE)
    net = es * (1 - EBAY_FVF) - SHIPPING_COST
    return (net - eb) / eb


def _attach_prices(test, db):
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


def _max_drawdown(curve: list[float]) -> float:
    """Max peak-to-trough drawdown (negative %)."""
    if not curve: return 0.0
    peak = curve[0]
    mdd = 0.0
    for v in curve:
        if v > peak: peak = v
        dd = (v / peak) - 1.0
        if dd < mdd: mdd = dd
    return mdd


def main() -> None:
    with get_db() as db:
        df = build_training_dataset(db)
    if df.empty:
        raise SystemExit("No training data.")
    df["anchor_date"] = pd.to_datetime(df["anchor_date"])
    logger.info("dataset: %d samples, span %s .. %s",
                len(df), df["anchor_date"].min().date(), df["anchor_date"].max().date())

    # Build per-month rolling: train on everything before M, test on [M, M+1).
    start = df["anchor_date"].min().to_period("M").to_timestamp() + pd.DateOffset(months=12)
    end   = df["anchor_date"].max().to_period("M").to_timestamp()
    months = pd.date_range(start, end, freq="MS")
    rng = np.random.default_rng(42)

    monthly: list[dict] = []
    for m in months:
        train = df[df["anchor_date"] < m]
        test  = df[(df["anchor_date"] >= m) & (df["anchor_date"] < m + pd.DateOffset(months=1))]
        if len(train) < MIN_TRAIN_SAMPLES or len(test) == 0:
            continue

        Xtr = train[FEATURE_COLUMNS].values
        ytr = np.log1p(np.clip(train[TARGET_COL].values, -0.999, None))
        model = lgb.LGBMRegressor(**BASE_PARAMS)
        model.fit(Xtr, ytr)

        Xte = test[FEATURE_COLUMNS].values
        pred = np.expm1(model.predict(Xte))

        # Attach realistic buy/sell prices to every test sample.
        scored = test[["card_id", "anchor_date", TARGET_COL]].copy()
        scored["pred"] = pred
        with get_db() as db:
            scored = _attach_prices(scored, db)
        scored = scored.dropna(subset=["buy_price", "sell_price"])
        scored = scored[scored["buy_price"] >= MIN_PRICE].copy()
        if len(scored) == 0:
            continue
        scored["net_return"] = [
            _net_return(b, s)
            for b, s in zip(scored["buy_price"], scored["sell_price"])
        ]
        scored = scored.dropna(subset=["net_return"])
        if len(scored) == 0:
            continue

        # Top-2% model cohort
        n_top = max(1, int(round(TOP_FRACTION * len(scored))))
        top = scored.nlargest(n_top, "pred")
        model_ret = float(top["net_return"].mean())
        model_hit = float((top["net_return"] > 0).mean())

        # Random-baseline: draw same N cards at random from the same
        # $100+ universe and average over N_RANDOM_BASELINE replicates.
        random_returns = []
        for _ in range(N_RANDOM_BASELINE):
            idx = rng.choice(len(scored), size=n_top, replace=False)
            random_returns.append(float(scored.iloc[idx]["net_return"].mean()))
        random_ret = float(np.mean(random_returns))
        random_std = float(np.std(random_returns))

        # Also compute the whole-universe (equal-weight everything) return
        # as a "just buy every $100+ card" baseline.
        ew_ret = float(scored["net_return"].mean())

        monthly.append({
            "month": m.strftime("%Y-%m"),
            "universe_n": int(len(scored)),
            "top_n": int(n_top),
            "model_net_return": model_ret,
            "model_hit_rate": model_hit,
            "random_net_return": random_ret,
            "random_std": random_std,
            "equal_weight_net_return": ew_ret,
        })
        logger.info(
            "%s  n=%4d top=%3d  model=%+6.2f%%  random=%+6.2f%%±%.1f%%  ew=%+6.2f%%",
            m.strftime("%Y-%m"), len(scored), n_top,
            model_ret * 100, random_ret * 100, random_std * 100, ew_ret * 100,
        )

    if not monthly:
        raise SystemExit("no months produced results")

    # Compound portfolio curves.
    months_labels = [r["month"] for r in monthly]
    model_returns = np.array([r["model_net_return"] for r in monthly])
    random_returns = np.array([r["random_net_return"] for r in monthly])
    ew_returns = np.array([r["equal_weight_net_return"] for r in monthly])

    model_curve = np.cumprod(1 + model_returns) - 1
    random_curve = np.cumprod(1 + random_returns) - 1
    ew_curve = np.cumprod(1 + ew_returns) - 1

    # Sharpe (annualized, monthly returns × sqrt(12/6=2) since each pick
    # has 180d holding window ≈ 6 months, so 2 "cycles" per year).
    def sharpe(r):
        r = r[~np.isnan(r)]
        if len(r) < 2: return 0.0
        s = np.std(r, ddof=1)
        if s <= 0: return 0.0
        return float(np.mean(r) / s * np.sqrt(2))

    summary = {
        "months_tested": len(monthly),
        "model": {
            "avg_monthly_net_return": float(np.mean(model_returns)),
            "cumulative_return": float(model_curve[-1]),
            "hit_rate_monthly": float((model_returns > 0).mean()),
            "annualized_sharpe": sharpe(model_returns),
            "max_drawdown": _max_drawdown(list(model_curve)),
            "best_month": months_labels[int(np.argmax(model_returns))],
            "worst_month": months_labels[int(np.argmin(model_returns))],
        },
        "random_baseline": {
            "avg_monthly_net_return": float(np.mean(random_returns)),
            "cumulative_return": float(random_curve[-1]),
            "hit_rate_monthly": float((random_returns > 0).mean()),
        },
        "equal_weight_universe": {
            "avg_monthly_net_return": float(np.mean(ew_returns)),
            "cumulative_return": float(ew_curve[-1]),
            "hit_rate_monthly": float((ew_returns > 0).mean()),
        },
        "monthly": monthly,
    }

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = MODELS_DIR / f"blind_historical_{ts}.json"
    out.write_text(json.dumps(summary, indent=2, default=str))
    logger.info("wrote %s", out)

    # Pretty report
    m = summary["model"]
    r = summary["random_baseline"]
    e = summary["equal_weight_universe"]
    print("\n" + "=" * 68)
    print("  BLIND HISTORICAL PORTFOLIO TEST")
    print("=" * 68)
    print(f"  Months tested:           {summary['months_tested']}")
    print(f"  Universe filter:         ≥${MIN_PRICE:.0f} PSA 10, 180d hold, realistic friction")
    print(f"  Cohort:                  top {int(TOP_FRACTION*100)}% of each month's predictions")
    print()
    print(f"  {'':<30} {'Model':>12} {'Random':>12} {'Buy-all':>12}")
    print(f"  {'-'*30} {'-'*12} {'-'*12} {'-'*12}")
    print(f"  {'avg monthly net return':<30} {m['avg_monthly_net_return']*100:>+11.2f}% "
          f"{r['avg_monthly_net_return']*100:>+11.2f}% {e['avg_monthly_net_return']*100:>+11.2f}%")
    print(f"  {'cumulative return':<30} {m['cumulative_return']*100:>+11.2f}% "
          f"{r['cumulative_return']*100:>+11.2f}% {e['cumulative_return']*100:>+11.2f}%")
    print(f"  {'monthly hit rate (>0)':<30} {m['hit_rate_monthly']*100:>11.1f}% "
          f"{r['hit_rate_monthly']*100:>11.1f}% {e['hit_rate_monthly']*100:>11.1f}%")
    print(f"  {'annualized Sharpe':<30} {m['annualized_sharpe']:>12.3f}")
    print(f"  {'max drawdown':<30} {m['max_drawdown']*100:>+11.2f}%")
    print(f"  best month:  {m['best_month']}   worst month: {m['worst_month']}")
    print()
    print(f"  Monthly results (model cohort):")
    print(f"  {'month':<10} {'n_pick':>6}  {'net return':>10}")
    for row in monthly:
        mark = "✓" if row["model_net_return"] > 0 else "·"
        print(f"  {row['month']:<10} {row['top_n']:>6}  {row['model_net_return']*100:>+9.2f}%  {mark}")
    print("=" * 68)


if __name__ == "__main__":
    main()
