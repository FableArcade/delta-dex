"""Investable universe filter — shared between training, walkforward, and inference.

Delta's training distribution must match its deployment distribution
(Analytics Taste P15). This module defines the single source of truth
for "which cards does the model see."

Filter: cards whose MEDIAN PSA 10 price across their sample history is
≥ MIN_PSA10_PRICE. Rationale: higher-priced cards self-select as the
investable universe (clear friction costs + actionable stakes). Delta's
ebay_history-derived liquidity features are empty in the training data
(known pipeline bug, see MODEL_DAG.md), so price is the only signal
with actual variance.

Why a single module:
  - Training: subsets training data to this universe
  - Walkforward: same filter for apples-to-apples evaluation
  - Inference: only scores cards in this universe (no out-of-distribution
    predictions leaking into the UI)

Change the threshold in ONE place; everything downstream rebuilds consistently.
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger("pipeline.model.liquid_universe")

# ---- Tunable threshold ----
# $100 chosen because:
#   - Empirically produces ~500-600 cards in current training data
#   - Clears friction comfortably ($5 shipping = 5% of $100; 13% FVF predictable)
#   - Matches the realistic_backtest.py default filter
#   - Walkforward at this level achieved top-1% hit rate 82%, top-2% hit rate 76%
MIN_PSA10_PRICE = 100.0

# Target cap on universe size — if more cards pass the price threshold,
# take the highest-priced N. Set high enough that most qualifying cards
# pass; acts as a safety rail rather than a hard constraint.
DEFAULT_TOP_N = 1000


def select_liquid_universe(
    df: pd.DataFrame,
    top_n: int = DEFAULT_TOP_N,
    min_psa10_price: float = MIN_PSA10_PRICE,
) -> tuple[set, dict]:
    """Return the set of investable card_ids, plus diagnostics.

    Accepts either the training dataset (with `card_id` column) OR a live
    features DataFrame (with card_id as index). Handles both.
    """
    if "log_price" not in df.columns:
        raise ValueError("log_price not in dataset — check features pipeline")

    # Training dataset has card_id as column; live features has it as index.
    if "card_id" in df.columns:
        grouped = df.groupby("card_id")
    else:
        # Live features — one row per card, no grouping needed
        grouped = df.reset_index().groupby("card_id" if "card_id" in df.reset_index().columns else df.index.name or "index")

    agg = grouped.agg(
        median_log_price=("log_price", "median"),
        max_log_price=("log_price", "max"),
        n_samples=("log_price", "size"),
    )
    agg["median_psa10_price"] = 10 ** agg["median_log_price"]
    agg["max_psa10_price"] = 10 ** agg["max_log_price"]

    qualifying = agg[agg["median_psa10_price"] >= min_psa10_price].copy()

    if len(qualifying) == 0:
        raise SystemExit(
            f"No cards with median PSA 10 ≥ ${min_psa10_price} — "
            f"dataset max median is ${agg['median_psa10_price'].max():.0f}"
        )

    if len(qualifying) <= top_n:
        selected = set(qualifying.index)
    else:
        selected = set(qualifying.nlargest(top_n, "median_psa10_price").index)

    sel_df = qualifying.loc[list(selected)]
    diagnostics = {
        "total_cards_with_samples": int(len(agg)),
        "passed_price_filter": int(len(qualifying)),
        "selected_n": len(selected),
        "top_n_cap": top_n,
        "min_psa10_price": min_psa10_price,
        "selected_stats": {
            "median_psa10_price": float(sel_df["median_psa10_price"].median()),
            "min_psa10_price": float(sel_df["median_psa10_price"].min()),
            "max_psa10_price": float(sel_df["median_psa10_price"].max()),
            "mean_n_samples_per_card": float(sel_df["n_samples"].mean()),
        },
    }
    return selected, diagnostics


def filter_to_liquid_universe(
    df: pd.DataFrame,
    top_n: int = DEFAULT_TOP_N,
    min_psa10_price: float = MIN_PSA10_PRICE,
) -> pd.DataFrame:
    """Convenience: return the filtered DataFrame (not just the set).

    Works for both training dataset (card_id column) and live features
    (card_id as index).
    """
    selected, diag = select_liquid_universe(df, top_n, min_psa10_price)
    logger.info(
        "Liquid universe: %d cards (from %d total, %d passed $%.0f filter)",
        diag["selected_n"], diag["total_cards_with_samples"],
        diag["passed_price_filter"], min_psa10_price,
    )

    if "card_id" in df.columns:
        return df[df["card_id"].isin(selected)].reset_index(drop=True)
    else:
        # Index-based (live features)
        return df[df.index.isin(selected)]
