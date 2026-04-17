"""Walk-forward on the LIQUID universe only.

Rationale (Analytics Taste P15 — eval corpus matches deployment corpus):
Investors don't trade thin-liquidity cards. A "prediction" on a card that
sells once a quarter is noise you can't execute on. Training on those cards
teaches the model patterns that don't generalize to the investable universe,
and wastes model capacity on distributions that aren't actionable.

This script:
  1. Builds the training dataset as usual
  2. Ranks cards by mean `sales_per_day_30d` across their sample history
  3. Keeps only the top N (default 1000) most-liquid cards
  4. Runs the same 46-fold × 30-bootstrap walkforward methodology
  5. Compares against the full-universe v1_3 baseline

Expected effects:
  - ~8x speedup (1000 vs ~8000 cards)
  - Higher Spearman on investable cohort (less noise from thin trading)
  - Potentially HIGHER top-2% Sharpe if thin-card noise was degrading ranking
    at the conviction tip

Run:  python -m scripts.walkforward_liquid [--top-n 1000] [--n-ensemble 30]
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
from sklearn.metrics import r2_score

from db.connection import get_db
from pipeline.model.features import (
    FEATURE_COLUMNS, HORIZON_DAYS, TARGET_COL, build_training_dataset,
)
from pipeline.model.friction import EBAY_FVF, SHIPPING_COST

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("walkforward_liquid")

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


def select_liquid_cards(
    df: pd.DataFrame,
    top_n: int,
    min_psa10_price: float = 100.0,
) -> tuple[set, dict]:
    """Investable-universe filter using PRICE as the liquidity proxy.

    Delta's `ebay_history`-derived liquidity features (sales_per_day_30d,
    new_listings_per_day_30d, thin_market_flag) are mostly empty across
    the training dataset — underlying table isn't populated for most
    cards at feature-computation time. So we fall back to the one signal
    that actually has data: recent PSA 10 price.

    Rationale: higher-priced cards self-select as the investable universe.
    A $100+ card is worth the $5 shipping + 13% FVF friction. A $30 card
    might trade often but is noise in dollar terms.

    Filter: cards whose MEDIAN PSA 10 price across their sample history
    is ≥ min_psa10_price. Take top N by median price if more qualify.
    """
    if "log_price" not in df.columns:
        raise ValueError("log_price not in training dataset")

    agg = df.groupby("card_id").agg(
        median_log_price=("log_price", "median"),
        max_log_price=("log_price", "max"),
        n_samples=("log_price", "size"),
    )
    agg["median_psa10_price"] = 10 ** agg["median_log_price"]
    agg["max_psa10_price"] = 10 ** agg["max_log_price"]

    # Threshold on median price (more stable than max, avoids single-spike cards)
    qualifying = agg[agg["median_psa10_price"] >= min_psa10_price].copy()

    if len(qualifying) == 0:
        raise SystemExit(
            f"No cards with median PSA 10 ≥ ${min_psa10_price} — "
            f"max median in dataset is ${agg['median_psa10_price'].max():.0f}"
        )

    if len(qualifying) <= top_n:
        logger.warning("Only %d cards pass $%.0f threshold; taking all",
                       len(qualifying), min_psa10_price)
        selected = set(qualifying.index)
    else:
        selected = set(qualifying.nlargest(top_n, "median_psa10_price").index)

    sel_df = qualifying.loc[list(selected)]
    diagnostics = {
        "total_cards_with_samples": int(df["card_id"].nunique()),
        "passed_price_filter": int(len(qualifying)),
        "selected_n": len(selected),
        "top_n_requested": top_n,
        "filter": {
            "method": "price_proxy_only",
            "min_median_psa10_price": min_psa10_price,
            "note": "liquidity features empty in training data; using price as proxy",
        },
        "selected_stats": {
            "median_psa10_price": float(sel_df["median_psa10_price"].median()),
            "min_psa10_price": float(sel_df["median_psa10_price"].min()),
            "max_psa10_price": float(sel_df["median_psa10_price"].max()),
            "mean_n_samples_per_card": float(sel_df["n_samples"].mean()),
        },
    }
    return selected, diagnostics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top-n", type=int, default=1000,
                        help="Keep the top N cards by mean sales_per_day_30d (default 1000)")
    parser.add_argument("--n-ensemble", type=int, default=30,
                        help="Bootstrap models per fold (default 30, matches prod)")
    args = parser.parse_args()

    n_ens = args.n_ensemble
    top_n = args.top_n

    with get_db() as db:
        df_full = build_training_dataset(db)
    if df_full.empty:
        raise SystemExit("No training samples.")
    df_full["anchor_date"] = pd.to_datetime(df_full["anchor_date"])

    logger.info("Full universe: %d samples, %d unique cards",
                len(df_full), df_full["card_id"].nunique())

    # Select investable universe via price proxy
    # (Delta's ebay_history-derived liquidity features are empty in
    # the training dataset; price is the only signal with data.)
    selected_cards, liq_diag = select_liquid_cards(df_full, top_n, min_psa10_price=100.0)
    df = df_full[df_full["card_id"].isin(selected_cards)].reset_index(drop=True)
    logger.info("Investable universe: %d samples, %d cards",
                len(df), df["card_id"].nunique())
    logger.info("Price filter: %d cards passed $%.0f threshold, %d selected after top-N cap",
                liq_diag["passed_price_filter"],
                liq_diag["filter"]["min_median_psa10_price"],
                liq_diag["selected_n"])
    stats = liq_diag["selected_stats"]
    logger.info("Selected stats: median PSA10=$%.0f (range $%.0f-$%.0f), ~%.0f samples/card",
                stats["median_psa10_price"], stats["min_psa10_price"],
                stats["max_psa10_price"], stats["mean_n_samples_per_card"])

    logger.info("Samples span: %s .. %s",
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

        preds_per_model = np.zeros((n_ens, len(test)))
        for i in range(n_ens):
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
        "model_type": "ensemble_liquid",
        "universe": f"top_{top_n}_liquid_cards",
        "liquidity_diagnostics": liq_diag,
        "collider_dropped": "psa_10_vs_raw_pct",
        "n_ensemble": n_ens,
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
    outp = MODELS_DIR / f"walkforward_liquid_{top_n}_{ts}.json"
    outp.write_text(json.dumps(out, indent=2, default=str))
    logger.info("Wrote %s", outp)

    # Compare against the full-universe v1_3 baseline
    baseline = None
    for p in sorted(MODELS_DIR.glob("walkforward_ensemble_2026*.json"), reverse=True):
        try:
            baseline = json.loads(p.read_text())
            break
        except Exception:
            continue

    print("\n" + "=" * 80)
    print(f"LIQUID-{top_n} vs FULL UNIVERSE V1_3  (n_ens={n_ens}, collider dropped)")
    print("=" * 80)
    fmt = "{:<30} {:>14} {:>14}  {:>10}"
    print(fmt.format("Metric", "V1_3 Full", "Liquid", "Δ"))
    print("-" * 80)
    for key, label in [
        ("spearman_oos", "Spearman"),
        ("r_squared_oos", "R² OOS"),
        ("hit_rate", "Hit rate"),
        ("top_decile_sharpe", "Top-decile Sharpe"),
        ("top_decile_net_return", "Top-decile net"),
        ("top2_sharpe", "Top-2% Sharpe"),
        ("top2_net_return", "Top-2% net"),
        ("top2_hit_rate", "Top-2% hit rate"),
        ("top1_net_return", "Top-1% net"),
        ("top1_hit_rate", "Top-1% hit rate"),
    ]:
        b = baseline.get(key) if baseline else None
        e = out.get(key)
        if b is not None and e is not None:
            diff = e - b
            mark = "✓" if diff > 0 else "✗" if diff < 0 else "·"
            print(fmt.format(label, f"{b:.4f}", f"{e:.4f}", f"{diff:+.4f} {mark}"))
        elif e is not None:
            print(fmt.format(label, "—", f"{e:.4f}", ""))
    print("-" * 80)
    print(f"Universe size:            {liq_diag['selected_n']} cards")
    print(f"Samples per fold (avg):   {len(df) // max(len(preds), 1)}")
    print()


if __name__ == "__main__":
    main()
