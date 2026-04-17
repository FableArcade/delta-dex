"""Liquidity features derived from eBay listing snapshots.

Why this matters: the base model projects a +45% return on a card that
sells twice a quarter. That return is unrealizable — you can't exit a
thin market at the quoted price without cutting 15-30% to find a buyer.
The model must learn that illiquid projections are less trustworthy.

Signals extracted from ebay_history (30-day window ending at anchor):

- sales_per_day_30d: raw velocity of clears (ended/30).
- sell_through_30d: ended / active_from. Fraction of standing inventory
  that cleared. High = healthy demand, low = inventory accumulating.
- ask_bid_spread_proxy: active_from / avg_ended_count. When listings
  pile up relative to sales, ask prices drift above bid. Pure count-
  based proxy; we don't have active prices in schema.
- thin_market_flag: 1 if sales_per_day < 0.1 (<3 sales in 30d). Binary
  signal for "projections are untrustworthy here."
"""

from __future__ import annotations

import sqlite3
from typing import Dict, Optional

import pandas as pd

THIN_MARKET_THRESHOLD = 0.1  # sales/day; <3/month == thin


def compute_liquidity_at_date(
    ebay_hist: pd.DataFrame,
    anchor_date: pd.Timestamp,
    window_days: int = 30,
) -> Dict[str, float]:
    """Compute liquidity features from a card's ebay_history for anchor.

    ebay_hist: DataFrame with datetime index, columns including
               'ended', 'active_from', 'new'.
    """
    start = anchor_date - pd.Timedelta(days=window_days)
    window = ebay_hist[(ebay_hist.index > start) & (ebay_hist.index <= anchor_date)]
    if window.empty:
        return _zeros()

    ended_total = float(window["ended"].fillna(0).sum())
    new_total = float(window["new"].fillna(0).sum())
    avg_active = float(window["active_from"].fillna(0).mean())
    sales_per_day = ended_total / max(window_days, 1)
    sell_through = ended_total / avg_active if avg_active > 0 else 0.0
    ask_bid_proxy = avg_active / max(ended_total, 1.0) if ended_total > 0 else 10.0
    thin_flag = 1.0 if sales_per_day < THIN_MARKET_THRESHOLD else 0.0

    return {
        "sales_per_day_30d": sales_per_day,
        "sell_through_30d": min(sell_through, 5.0),  # cap, avoid outliers
        "ask_bid_proxy_30d": min(ask_bid_proxy, 20.0),
        "new_listings_per_day_30d": new_total / max(window_days, 1),
        "thin_market_flag": thin_flag,
    }


def compute_live_liquidity(db: sqlite3.Connection) -> pd.DataFrame:
    """Compute latest 30d liquidity features for all cards.

    Returns DataFrame indexed by card_id with the five liquidity columns.
    """
    rows = db.execute(
        """
        WITH win AS (
            SELECT card_id,
                   SUM(COALESCE(ended, 0))       AS ended_total,
                   SUM(COALESCE(new, 0))         AS new_total,
                   AVG(COALESCE(active_from, 0)) AS avg_active,
                   COUNT(*)                      AS sample_days
            FROM ebay_history
            WHERE date >= date('now', '-30 days')
            GROUP BY card_id
        )
        SELECT * FROM win
        """
    ).fetchall()

    recs = []
    for r in rows:
        days = max(r["sample_days"] or 30, 1)
        ended = float(r["ended_total"] or 0)
        new = float(r["new_total"] or 0)
        avg_active = float(r["avg_active"] or 0)
        sales_per_day = ended / days
        sell_through = ended / avg_active if avg_active > 0 else 0.0
        ask_bid_proxy = avg_active / max(ended, 1.0) if ended > 0 else 10.0
        recs.append({
            "card_id": r["card_id"],
            "sales_per_day_30d": sales_per_day,
            "sell_through_30d": min(sell_through, 5.0),
            "ask_bid_proxy_30d": min(ask_bid_proxy, 20.0),
            "new_listings_per_day_30d": new / days,
            "thin_market_flag": 1.0 if sales_per_day < THIN_MARKET_THRESHOLD else 0.0,
        })
    df = pd.DataFrame(recs)
    if not df.empty:
        df = df.set_index("card_id")
    return df


def _zeros() -> Dict[str, float]:
    return {
        "sales_per_day_30d": 0.0,
        "sell_through_30d": 0.0,
        "ask_bid_proxy_30d": 10.0,  # neutral-high: unknown is assumed thin
        "new_listings_per_day_30d": 0.0,
        "thin_market_flag": 1.0,
    }


LIQUIDITY_COLUMNS = [
    "sales_per_day_30d",
    "sell_through_30d",
    "ask_bid_proxy_30d",
    "new_listings_per_day_30d",
    "thin_market_flag",
]
