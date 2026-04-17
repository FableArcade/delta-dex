"""Set-release catalyst features — Signal Class 4 (cultural/meta catalyst).

Captures the "new set release drives TCG attention" cycle using data
already in the DB (sets.release_date). Three features:

  1. days_since_set_release — age of the card's own set at anchor date.
     Fresh sets (<90d) carry speculation premium; old sets are price-cycled.
  2. days_since_any_set_release — how recent is the TCG's last hype wave?
     All cards benefit when the category is in the spotlight.
  3. is_fresh_set — binary: is the card's set ≤ 90 days old at anchor?

All three are derived from historical release dates; no new data source,
no leakage risk (we only look at sets released on or before anchor_date).

NOTE: Unlike ebay_history liquidity features, these features DO have
valid historical values — set release dates are static historical facts.
So they work for backfilled training samples, not just forward data.
"""

from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional, Tuple

import pandas as pd


def load_set_release_calendar(
    db: sqlite3.Connection,
) -> Tuple[Dict[str, pd.Timestamp], List[pd.Timestamp]]:
    """Returns ({set_code -> release_date}, sorted list of all release dates).

    The sorted list is used for "most-recent set release on or before
    anchor_date" lookups during feature computation.
    """
    rows = db.execute(
        "SELECT set_code, release_date FROM sets WHERE release_date IS NOT NULL"
    ).fetchall()
    by_code: Dict[str, pd.Timestamp] = {}
    all_dates: List[pd.Timestamp] = []
    for r in rows:
        try:
            d = pd.to_datetime(r["release_date"])
        except Exception:
            continue
        by_code[r["set_code"]] = d
        all_dates.append(d)
    return by_code, sorted(all_dates)


def catalyst_features_at_date(
    own_set_code: str,
    anchor_date: pd.Timestamp,
    set_dates: Dict[str, pd.Timestamp],
    all_release_dates_sorted: List[pd.Timestamp],
) -> Dict[str, float]:
    """Compute catalyst features for a (card, anchor_date) sample.

    Returns:
      days_since_set_release     - age of card's own set at anchor, in days
      days_since_any_set_release - days since MOST RECENT set release on/before anchor
      is_fresh_set               - 1.0 if own set released within 90 days, else 0.0

    Missing / unknown values are filled with sensible defaults:
      days_since_set_release: 9999 (treat as "very old" if unknown)
      days_since_any_set_release: 9999 (if no releases ever found)
      is_fresh_set: 0.0
    """
    # Days since this card's own set release
    own_release = set_dates.get(own_set_code)
    if own_release is None or pd.isna(own_release):
        own_days = 9999.0
    else:
        own_days = max(0.0, (anchor_date - own_release).days)

    # Days since the globally most-recent set release (on or before anchor)
    # Binary-search for the largest date <= anchor
    any_days = 9999.0
    if all_release_dates_sorted:
        lo, hi = 0, len(all_release_dates_sorted) - 1
        best = None
        while lo <= hi:
            mid = (lo + hi) // 2
            if all_release_dates_sorted[mid] <= anchor_date:
                best = all_release_dates_sorted[mid]
                lo = mid + 1
            else:
                hi = mid - 1
        if best is not None:
            any_days = max(0.0, (anchor_date - best).days)

    return {
        "days_since_set_release": float(own_days),
        "days_since_any_set_release": float(any_days),
        "is_fresh_set": 1.0 if own_days <= 90 else 0.0,
    }


CATALYST_COLUMNS = [
    "days_since_set_release",
    "days_since_any_set_release",
    "is_fresh_set",
]
