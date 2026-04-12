"""
eBay-derived pricing: outlier-adjusted + 7-day EMA smoothing.

Takes raw eBay ended-auction average prices, applies a 0.88 outlier
adjustment factor (calibrated from Collectrics data), then smooths
with a 7-day exponential moving average.
"""

import logging

logger = logging.getLogger(__name__)

# Outlier adjustment factor — eBay ended averages overstate true market
# price by ~12% due to outlier high sales.  Calibrated against Collectrics.
OUTLIER_FACTOR = 0.88

# EMA span (days).  Alpha = 2 / (span + 1)
EMA_SPAN = 7
EMA_ALPHA = 2.0 / (EMA_SPAN + 1)


def _ema(prev, cur):
    """Single-step EMA update.  Returns cur if prev is None."""
    if cur is None:
        return prev
    if prev is None:
        return cur
    return EMA_ALPHA * cur + (1 - EMA_ALPHA) * prev


def compute_ebay_derived(db, card_id):
    """Compute smoothed daily price estimates from ebay_history.

    For each date row in ebay_history:
      1. Multiply ended_avg_*_price by OUTLIER_FACTOR to get adjusted price.
      2. Apply 7-day EMA across the date-sorted series.
      3. Insert/replace into ebay_derived_history.

    Parameters
    ----------
    db : sqlite3.Connection
    card_id : str

    Returns
    -------
    dict  {"rows_written": int}
    """
    rows = db.execute(
        """SELECT date,
                  ended_avg_raw_price,
                  ended_avg_psa_9_price,
                  ended_avg_psa_10_price
             FROM ebay_history
            WHERE card_id = ?
            ORDER BY date""",
        (card_id,),
    ).fetchall()

    if not rows:
        return {"rows_written": 0}

    # --- pass 1: outlier adjustment ---
    adjusted = []
    for r in rows:
        adjusted.append({
            "date": r["date"],
            "raw": r["ended_avg_raw_price"] * OUTLIER_FACTOR
                   if r["ended_avg_raw_price"] is not None else None,
            "psa_9": r["ended_avg_psa_9_price"] * OUTLIER_FACTOR
                     if r["ended_avg_psa_9_price"] is not None else None,
            "psa_10": r["ended_avg_psa_10_price"] * OUTLIER_FACTOR
                      if r["ended_avg_psa_10_price"] is not None else None,
        })

    # --- pass 2: 7-day EMA ---
    ema_raw = None
    ema_psa_9 = None
    ema_psa_10 = None
    smoothed = []

    for a in adjusted:
        ema_raw = _ema(ema_raw, a["raw"])
        ema_psa_9 = _ema(ema_psa_9, a["psa_9"])
        ema_psa_10 = _ema(ema_psa_10, a["psa_10"])

        smoothed.append((
            card_id,
            a["date"],
            round(ema_raw, 4) if ema_raw is not None else None,
            round(ema_psa_9, 4) if ema_psa_9 is not None else None,
            round(ema_psa_10, 4) if ema_psa_10 is not None else None,
        ))

    # --- write ---
    db.executemany(
        """INSERT OR REPLACE INTO ebay_derived_history
           (card_id, date, d_raw_price, d_psa_9_price, d_psa_10_price)
           VALUES (?, ?, ?, ?, ?)""",
        smoothed,
    )

    logger.info("ebay_derived card=%s  rows_written=%d", card_id, len(smoothed))
    return {"rows_written": len(smoothed)}
