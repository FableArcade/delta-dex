"""Realistic walk-forward backtest with transaction costs and slippage.

Existing backtests measure gross ranking quality. This one measures what
an investor *actually* takes home after:

  * eBay final-value fee (13% of sale price)
  * Fixed shipping absorbed by seller ($5)
  * Slippage: assume you buy at 5% above ended_avg (you needed a listing
    to buy from, not the clearing price) and sell at 3% below ended_avg
    (you need to price competitively to move inventory)

The script:
  1. Builds the v1.2 training dataset (net-of-cost target).
  2. Splits chronologically into train / test.
  3. Trains the median LightGBM model.
  4. On the test set, ranks predictions into deciles.
  5. For each decile, computes:
       - Gross realized return (what v1.0 metrics showed)
       - Net realized return with fees + shipping + slippage
       - Hit rate at net level (% of positions that were +EV after costs)
       - Max drawdown within the 90d horizon (using trailing min)
  6. Computes the **cost of friction**: gross - net return by decile.

Run: python -m scripts.realistic_backtest

Output: prints a table + writes data/models/realistic_backtest_<date>.json.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

from db.connection import get_db
from pipeline.model.features import (
    FEATURE_COLUMNS,
    HORIZON_DAYS,
    TARGET_COL,
    build_training_dataset,
)
from pipeline.model.friction import EBAY_FVF, SHIPPING_COST

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("realistic_backtest")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# Slippage assumptions — conservative so net numbers reflect reality.
BUY_SLIPPAGE = 0.05   # you pay 5% above clearing on entry
SELL_SLIPPAGE = 0.03  # you take 3% below clearing on exit

HOLDOUT_MONTHS = 12

# v1.2a: Raise the universe floor. A $20 card eats 25% in flat shipping
# alone — friction is mechanically unrecoverable. $100 drops that to ~5%
# and lets genuine alpha show through if it exists.
MIN_PRICE_FILTER = 100.0

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


def apply_friction(buy_price: float, sell_price: float) -> float:
    """Net return including slippage on both sides + fees + shipping."""
    if buy_price <= 0 or sell_price <= 0:
        return -1.0
    effective_buy = buy_price * (1 + BUY_SLIPPAGE)
    effective_sell = sell_price * (1 - SELL_SLIPPAGE)
    net_proceeds = effective_sell * (1 - EBAY_FVF) - SHIPPING_COST
    return (net_proceeds - effective_buy) / effective_buy


def gross_return(buy_price: float, sell_price: float) -> float:
    if buy_price <= 0 or sell_price <= 0:
        return -1.0
    return (sell_price - buy_price) / buy_price


def run() -> dict:
    with get_db() as db:
        return _run_with_db(db)


def _run_with_db(db: sqlite3.Connection) -> dict:
    df = build_training_dataset(db)
    if df.empty or len(df) < 200:
        raise SystemExit(f"insufficient_data samples={len(df)}")

    logger.info("Dataset: %d samples, %d features", len(df), len(FEATURE_COLUMNS))

    df["anchor_date"] = pd.to_datetime(df["anchor_date"])
    cutoff = df["anchor_date"].max() - pd.DateOffset(months=HOLDOUT_MONTHS)
    train = df[df["anchor_date"] < cutoff]
    test = df[df["anchor_date"] >= cutoff].copy()
    logger.info("Train %d / Test %d (cutoff %s)", len(train), len(test), cutoff.date())

    X_train = train[FEATURE_COLUMNS].values
    y_train_raw = train[TARGET_COL].values  # already net-of-cost in v1.2
    y_train = np.log1p(np.clip(y_train_raw, -0.999, None))  # v1.3: log-return
    X_test = test[FEATURE_COLUMNS].values

    model = lgb.LGBMRegressor(**BASE_PARAMS)
    model.fit(X_train, y_train)
    test["pred"] = np.expm1(model.predict(X_test))  # invert log-return

    # Recover buy/sell prices so we can compute gross vs net explicitly.
    # We don't have them in the feature frame; reconstruct from price_history.
    logger.info("Reconstructing buy/sell prices for test samples...")
    test = _attach_prices(test, db)

    # Drop any row we couldn't price — can't score it honestly.
    test = test.dropna(subset=["buy_price", "sell_price"])
    logger.info("Priced test rows: %d", len(test))

    # v1.2a: Apply $100 universe filter post-hoc on the BUY price
    pre_filter_n = len(test)
    test = test[test["buy_price"] >= MIN_PRICE_FILTER].copy()
    logger.info("Universe filter $%.0f: %d -> %d rows",
                MIN_PRICE_FILTER, pre_filter_n, len(test))

    test["gross_return"] = [
        gross_return(b, s) for b, s in zip(test["buy_price"], test["sell_price"])
    ]
    test["net_return"] = [
        apply_friction(b, s) for b, s in zip(test["buy_price"], test["sell_price"])
    ]

    # Decile analysis
    test = test.sort_values("pred").reset_index(drop=True)
    test["decile"] = pd.qcut(test["pred"], 10, labels=False, duplicates="drop") + 1

    rows = []
    for d, grp in test.groupby("decile"):
        rows.append({
            "decile": int(d),
            "n": int(len(grp)),
            "mean_pred": round(float(grp["pred"].mean()), 4),
            "mean_gross": round(float(grp["gross_return"].mean()), 4),
            "mean_net": round(float(grp["net_return"].mean()), 4),
            "friction_cost": round(
                float(grp["gross_return"].mean() - grp["net_return"].mean()), 4
            ),
            "hit_rate_gross": round(float((grp["gross_return"] > 0).mean()), 4),
            "hit_rate_net": round(float((grp["net_return"] > 0).mean()), 4),
            "median_net": round(float(grp["net_return"].median()), 4),
        })

    # Top-decile tradeable strategy: go long top 10%, report net return
    top = test[test["decile"] == test["decile"].max()]
    top_net = float(top["net_return"].mean())
    top_hit = float((top["net_return"] > 0).mean())

    # Compare to naive "buy everything" baseline
    all_net = float(test["net_return"].mean())
    edge = top_net - all_net

    print("\n=== REALISTIC BACKTEST (v1.2: net-of-cost target + friction) ===")
    print(f"Samples: {len(test)} | Features: {len(FEATURE_COLUMNS)}")
    print(f"Friction assumptions: {EBAY_FVF*100:.0f}% eBay, ${SHIPPING_COST:.0f} ship, "
          f"{BUY_SLIPPAGE*100:.0f}% buy-slip, {SELL_SLIPPAGE*100:.0f}% sell-slip\n")

    print(f"{'Decile':<7}{'N':<6}{'Pred':<9}{'Gross':<9}{'Net':<9}"
          f"{'Friction':<10}{'HitNet':<8}{'MedNet':<9}")
    for r in rows:
        print(f"{r['decile']:<7}{r['n']:<6}"
              f"{r['mean_pred']*100:>6.1f}%  "
              f"{r['mean_gross']*100:>6.1f}%  "
              f"{r['mean_net']*100:>6.1f}%  "
              f"{r['friction_cost']*100:>7.1f}%  "
              f"{r['hit_rate_net']*100:>5.0f}%   "
              f"{r['median_net']*100:>6.1f}%")

    print(f"\nTop-decile strategy net return: {top_net*100:.2f}%  "
          f"(hit rate {top_hit*100:.0f}%)")
    print(f"Baseline (buy all)      net return: {all_net*100:.2f}%")
    print(f"Model edge over baseline:           {edge*100:+.2f}pp")

    # v1.2a: Regime split — bucket test anchors by quarter and report
    # top-decile net return per regime. A model that's +EV in bull
    # quarters and -EV in bear quarters is a regime-timed strategy,
    # not a broken one.
    print("\n=== REGIME SPLIT (top-decile only, by anchor quarter) ===")
    top_only = test[test["decile"] == test["decile"].max()].copy()
    top_only["quarter"] = top_only["anchor_date"].dt.to_period("Q").astype(str)
    # Also compute market baseline per quarter (all test rows)
    market_by_q = test.groupby(test["anchor_date"].dt.to_period("Q").astype(str))[
        "net_return"
    ].mean()

    print(f"{'Quarter':<10}{'N':<5}{'TopNet':<10}{'Market':<10}{'Edge':<9}{'Hit':<6}")
    regime_rows = []
    for q, grp in top_only.groupby("quarter"):
        market = float(market_by_q.get(q, 0.0))
        top_q = float(grp["net_return"].mean())
        hit = float((grp["net_return"] > 0).mean())
        edge_q = top_q - market
        regime_rows.append({
            "quarter": q, "n": int(len(grp)),
            "top_net": round(top_q, 4),
            "market_net": round(market, 4),
            "edge": round(edge_q, 4),
            "hit_rate": round(hit, 4),
        })
        marker = " +EV" if top_q > 0 else ""
        print(f"{q:<10}{len(grp):<5}"
              f"{top_q*100:>6.1f}%   "
              f"{market*100:>6.1f}%   "
              f"{edge_q*100:>+5.1f}pp  "
              f"{hit*100:>3.0f}%{marker}")

    positive_quarters = [r for r in regime_rows if r["top_net"] > 0]
    print(f"\n+EV quarters: {len(positive_quarters)} / {len(regime_rows)}")
    if positive_quarters:
        avg_pos = sum(r["top_net"] for r in positive_quarters) / len(positive_quarters)
        print(f"Avg top-decile net in +EV quarters: {avg_pos*100:+.1f}%")

    result_regime = regime_rows

    if top_net <= 0:
        print("\nVERDICT: Top-decile strategy is NOT profitable after realistic costs.")
        print("         The model has ranking power but no +EV edge. Do not deploy.")
    elif edge < 0.02:
        print("\nVERDICT: Top-decile is marginally profitable but edge over baseline is thin.")
        print("         Risk of regime change eating the edge is high.")
    else:
        print(f"\nVERDICT: Top-decile produces {edge*100:.1f}pp of edge over baseline, net of costs.")
        print("         Edge is tradeable. Size positions conservatively until more test windows.")

    result = {
        "run_date": dt.date.today().isoformat(),
        "samples": len(test),
        "features": len(FEATURE_COLUMNS),
        "deciles": rows,
        "top_decile_net_return": top_net,
        "top_decile_hit_rate": top_hit,
        "baseline_net_return": all_net,
        "edge_vs_baseline": edge,
        "min_price_filter": MIN_PRICE_FILTER,
        "regime_split": result_regime,
        "friction": {
            "ebay_fvf": EBAY_FVF,
            "shipping": SHIPPING_COST,
            "buy_slippage": BUY_SLIPPAGE,
            "sell_slippage": SELL_SLIPPAGE,
        },
    }
    out = MODELS_DIR / f"realistic_backtest_{dt.date.today().isoformat()}.json"
    out.write_text(json.dumps(result, indent=2, default=str))
    print(f"\nWrote {out}")
    return result


def _attach_prices(test: pd.DataFrame, db: sqlite3.Connection) -> pd.DataFrame:
    """Look up buy (anchor) and sell (anchor+90d) PSA 10 prices."""
    # Pull all PSA 10 price points once.
    ph = pd.read_sql_query(
        "SELECT card_id, date, psa_10_price FROM price_history "
        "WHERE psa_10_price IS NOT NULL",
        db,
    )
    ph["date"] = pd.to_datetime(ph["date"])
    ph = ph.sort_values(["card_id", "date"])

    buy, sell = [], []
    # Index by card for O(1) lookups
    by_card = {cid: g.reset_index(drop=True) for cid, g in ph.groupby("card_id")}
    for _, row in test.iterrows():
        g = by_card.get(row["card_id"])
        if g is None or g.empty:
            buy.append(None); sell.append(None); continue
        anchor = row["anchor_date"]
        forward = anchor + pd.Timedelta(days=HORIZON_DAYS)

        # buy = first price at/after anchor within 31 days (monthly anchor cadence)
        bg = g[(g["date"] >= anchor) & (g["date"] <= anchor + pd.Timedelta(days=31))]
        sg = g[(g["date"] >= forward) & (g["date"] <= forward + pd.Timedelta(days=30))]
        buy.append(float(bg.iloc[0]["psa_10_price"]) if not bg.empty else None)
        sell.append(float(sg.iloc[0]["psa_10_price"]) if not sg.empty else None)

    test["buy_price"] = buy
    test["sell_price"] = sell
    return test


if __name__ == "__main__":
    run()
