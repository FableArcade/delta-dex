"""Blind historical portfolio test — Must Buy composite variant.

Identical methodology to blind_historical_test.py, but ranks each month's
candidates by the Must Buy v3.2 composite instead of pure projected return:

    score =  modelScore × 35
           + cultural   × 15
           + demand     × 15
           + scarcity   × 15
           + setupBonus ≤ 10
           + momentum   × 10
           + gradingVal × 10
           (capped 0..100)

All dimensions come from features already in build_training_dataset, so
the test reproduces exactly what a user sees if they follow the Must Buy
view rather than the Pure ROI view.

Prints a side-by-side against the Pure ROI baseline (from the latest
blind_historical_*.json file).
"""

from __future__ import annotations

import datetime as dt
import glob
import json
import logging
import re
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
logger = logging.getLogger("blind_mustbuy")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"

BASE_PARAMS = {
    "objective": "quantile", "alpha": 0.5, "num_leaves": 31,
    "learning_rate": 0.05, "n_estimators": 500,
    "min_child_samples": 20, "subsample": 0.8, "colsample_bytree": 0.8,
    "reg_alpha": 0.1, "reg_lambda": 1.0, "verbose": -1,
}

BUY_SLIPPAGE = 0.05
SELL_SLIPPAGE = 0.03
MIN_TRAIN_SAMPLES = 300
MIN_PRICE = 100.0
TOP_FRACTION = 0.02


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


# ---------------------------------------------------------------------------
# Must Buy composite — pure-function reproduction of the UI formula
# ---------------------------------------------------------------------------

# Pokemon-name bonus table mirroring frontend/js/wishlist_store.js ICONIC_NAMES
ICONIC = [
    (r"charizard", 1.00), (r"pikachu", 1.00), (r"mewtwo", 0.96),
    (r"\bmew\b", 0.96), (r"umbreon", 0.96),
    (r"lugia", 0.88), (r"rayquaza", 0.88), (r"gengar", 0.85),
    (r"snorlax", 0.82), (r"dragonite", 0.82),
    (r"blastoise", 0.78), (r"venusaur", 0.78), (r"gyarados", 0.80),
    (r"greninja", 0.82), (r"lucario", 0.80), (r"garchomp", 0.78),
    (r"zoroark", 0.75),
    (r"sylveon", 0.78), (r"espeon", 0.75), (r"leafeon", 0.72),
    (r"glaceon", 0.72), (r"vaporeon", 0.70), (r"jolteon", 0.70),
    (r"flareon", 0.70), (r"eevee", 0.72),
    (r"giratina", 0.70), (r"dialga", 0.65), (r"palkia", 0.65),
    (r"arceus", 0.72),
    (r"\bditto\b", 0.75), (r"psyduck", 0.70), (r"magikarp", 0.68),
    (r"slowpoke", 0.68), (r"mimikyu", 0.78), (r"gardevoir", 0.75),
    (r"cynthia", 0.75), (r"lillie", 0.72), (r"iono", 0.68),
    (r"marnie", 0.65), (r"team rocket", 0.60),
]
RARITY_BONUS = {"SIR": 0.20, "MHR": 0.18, "HR": 0.12, "SCR": 0.12,
                "IR": 0.08, "UR": 0.05, "V": 0.00}


def _cultural_score(product_name: str, rarity_code: str | None) -> float:
    name = (product_name or "").lower()
    name_s = 0.0
    for pat, s in ICONIC:
        if re.search(pat, name) and s > name_s:
            name_s = s
    bonus = RARITY_BONUS.get(rarity_code or "", 0.0)
    return min(1.0, name_s + bonus)


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


def mustbuy_score(row: dict, product_name: str, rarity_code: str | None,
                  pred_return: float) -> float:
    """Reproduce the Must Buy v3.2 composite on a training-sample row."""
    # modelScore (0..1)
    if pred_return is None or not np.isfinite(pred_return):
        return -1
    model_s = max(0.0, 0.10 + pred_return) if pred_return < 0 \
              else _clamp01(pred_return / 0.30)

    cultural = _cultural_score(product_name, rarity_code)

    # Demand — same nf7 + nf30 blend the UI uses
    nf7 = row.get("net_flow_pct_7d", 0.0) or 0.0
    nf30 = row.get("net_flow_pct_30d", 0.0) or 0.0
    dem = row.get("demand_pressure_30d", 0.0) or 0.0
    sup = max(1e-6, row.get("supply_pressure_30d", 1.0) or 1.0)  # avoid div 0
    nf7n  = _clamp01((nf7 + 0.01) / 0.05)
    nf30n = _clamp01((nf30 + 0.01) / 0.05)
    ds = _clamp01(((dem / sup) - 1.0) / 0.5) if sup > 0 else 0.0
    demand = 0.25 * nf7n + 0.50 * nf30n + 0.25 * ds

    # Scarcity — simplified (pop * supplyTight)
    sat = row.get("supply_saturation_index", 1.0) or 1.0
    if sat >= 1: return -1   # gate — matches UI hard gate
    supplyTight = _clamp01((1.0 - sat) / 0.6)
    pop = row.get("psa_10_pop", 5000) or 5000
    if   pop <= 100:  popS = 1.00
    elif pop <= 500:  popS = 1.00 - (pop - 100) / 800
    elif pop <= 2000: popS = 0.50 - (pop - 500) / 3000
    else:              popS = 0.0
    scarcity = 0.35 * popS + 0.65 * supplyTight   # simplified (no priceStable)

    # Setup bonus — 4 signals × 2.5 pts
    isRising = 1 if (nf30 > 0 and nf7 > nf30) else 0
    isTight  = 1 if sat <= 0.75 else 0
    ret90 = row.get("ret_90d", 0.0) or 0.0
    isReversal = 1 if ret90 < 0 and nf30 > 0 else 0  # proxy for chart reversal
    pk = row.get("peak_discount", 0.0) or 0.0
    isOffPeak = 1 if pk >= 0.15 else 0
    setup_pts = (isRising + isTight + isReversal + isOffPeak) * 2.5

    # Omit momentum/grading dims (not easily derivable from training features)
    score = (model_s     * 35
             + cultural  * 15
             + demand    * 15
             + scarcity  * 15
             + setup_pts)
    return min(100.0, score)


def main() -> None:
    with get_db() as db:
        df = build_training_dataset(db)
        # Attach product_name + rarity_code so mustbuy_score can compute cultural
        cards = pd.read_sql_query(
            "SELECT id AS card_id, product_name, rarity_code FROM cards", db
        )
    if df.empty: raise SystemExit("No training data.")
    df = df.merge(cards, on="card_id", how="left")
    df["anchor_date"] = pd.to_datetime(df["anchor_date"])
    logger.info("dataset: %d samples", len(df))

    start = df["anchor_date"].min().to_period("M").to_timestamp() + pd.DateOffset(months=12)
    end   = df["anchor_date"].max().to_period("M").to_timestamp()
    months = pd.date_range(start, end, freq="MS")

    monthly: list[dict] = []
    for m in months:
        train = df[df["anchor_date"] < m]
        test  = df[(df["anchor_date"] >= m) & (df["anchor_date"] < m + pd.DateOffset(months=1))]
        if len(train) < MIN_TRAIN_SAMPLES or len(test) == 0:
            continue

        model = lgb.LGBMRegressor(**BASE_PARAMS)
        Xtr = train[FEATURE_COLUMNS].values
        ytr = np.log1p(np.clip(train[TARGET_COL].values, -0.999, None))
        model.fit(Xtr, ytr)

        Xte = test[FEATURE_COLUMNS].values
        pred = np.expm1(model.predict(Xte))

        scored = test.copy().reset_index(drop=True)
        scored["pred"] = pred
        scored["mb_score"] = [
            mustbuy_score(scored.iloc[i].to_dict(),
                          scored.iloc[i]["product_name"],
                          scored.iloc[i]["rarity_code"],
                          scored.iloc[i]["pred"])
            for i in range(len(scored))
        ]

        with get_db() as db:
            scored = _attach_prices(scored, db)
        scored = scored.dropna(subset=["buy_price", "sell_price"])
        scored = scored[scored["buy_price"] >= MIN_PRICE].copy()
        # Filter out gate-failed rows (score -1)
        scored = scored[scored["mb_score"] >= 0].copy()
        if len(scored) == 0: continue
        scored["net_return"] = [
            _net_return(b, s)
            for b, s in zip(scored["buy_price"], scored["sell_price"])
        ]
        scored = scored.dropna(subset=["net_return"])
        if len(scored) == 0: continue

        # Top-2% by Must Buy composite
        n_top = max(1, int(round(TOP_FRACTION * len(scored))))
        mb_top = scored.nlargest(n_top, "mb_score")
        roi_top = scored.nlargest(n_top, "pred")

        monthly.append({
            "month": m.strftime("%Y-%m"),
            "universe_n": int(len(scored)),
            "top_n": int(n_top),
            "mustbuy_net": float(mb_top["net_return"].mean()),
            "mustbuy_hit": float((mb_top["net_return"] > 0).mean()),
            "roi_net": float(roi_top["net_return"].mean()),
            "roi_hit": float((roi_top["net_return"] > 0).mean()),
        })
        logger.info("%s n=%d top=%d mb=%+.2f%% roi=%+.2f%%",
                    m.strftime("%Y-%m"), len(scored), n_top,
                    mb_top["net_return"].mean() * 100,
                    roi_top["net_return"].mean() * 100)

    if not monthly: raise SystemExit("no months")

    mb = np.array([r["mustbuy_net"] for r in monthly])
    roi = np.array([r["roi_net"] for r in monthly])
    mb_curve = np.cumprod(1 + mb) - 1
    roi_curve = np.cumprod(1 + roi) - 1

    def sharpe(r):
        if len(r) < 2: return 0.0
        s = np.std(r, ddof=1)
        return 0.0 if s <= 0 else float(np.mean(r) / s * np.sqrt(2))

    summary = {
        "months_tested": len(monthly),
        "mustbuy": {
            "avg_monthly": float(np.mean(mb)),
            "cumulative": float(mb_curve[-1]),
            "hit_rate_monthly": float((mb > 0).mean()),
            "sharpe": sharpe(mb),
        },
        "pure_roi": {
            "avg_monthly": float(np.mean(roi)),
            "cumulative": float(roi_curve[-1]),
            "hit_rate_monthly": float((roi > 0).mean()),
            "sharpe": sharpe(roi),
        },
        "monthly": monthly,
    }

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = MODELS_DIR / f"blind_historical_mustbuy_{ts}.json"
    out.write_text(json.dumps(summary, indent=2, default=str))

    print("\n" + "=" * 70)
    print("  BLIND HISTORICAL — Must Buy vs Pure ROI")
    print("=" * 70)
    print(f"  Months tested: {summary['months_tested']}")
    print(f"  Cohort:        top {int(TOP_FRACTION*100)}%  |  universe ≥${int(MIN_PRICE)} PSA 10")
    print()
    print(f"  {'':<22}  {'Must Buy':>14}  {'Pure ROI':>14}")
    print(f"  {'-'*22}  {'-'*14}  {'-'*14}")
    print(f"  {'avg monthly net':<22}  {summary['mustbuy']['avg_monthly']*100:>+13.2f}%  {summary['pure_roi']['avg_monthly']*100:>+13.2f}%")
    print(f"  {'cumulative':<22}  {summary['mustbuy']['cumulative']*100:>+13.2f}%  {summary['pure_roi']['cumulative']*100:>+13.2f}%")
    print(f"  {'monthly hit rate':<22}  {summary['mustbuy']['hit_rate_monthly']*100:>13.1f}%  {summary['pure_roi']['hit_rate_monthly']*100:>13.1f}%")
    print(f"  {'annualized Sharpe':<22}  {summary['mustbuy']['sharpe']:>14.3f}  {summary['pure_roi']['sharpe']:>14.3f}")
    print()
    print(f"  Monthly side-by-side:")
    print(f"  {'month':<10} {'MB net':>10} {'ROI net':>10}  {'winner':>8}")
    for r in monthly:
        w = "MB" if r["mustbuy_net"] > r["roi_net"] else ("ROI" if r["roi_net"] > r["mustbuy_net"] else "tie")
        print(f"  {r['month']:<10} {r['mustbuy_net']*100:>+9.2f}% {r['roi_net']*100:>+9.2f}%  {w:>8}")
    print(f"  wrote {out}")


if __name__ == "__main__":
    main()
