"""
Composite price blending: PriceCharting + eBay-derived + JustTCG.

Weights for raw prices (3 sources available):
    PC 0.35, eBay 0.30, JustTCG 0.35

For PSA graded prices (no JustTCG source):
    PC 0.55, eBay 0.45

When a source is missing on a given date the weights are redistributed
proportionally among the available sources.
"""

from __future__ import annotations

import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

# --- Weight configuration ---
RAW_WEIGHTS = {"pc": 0.35, "ebay": 0.30, "jtcg": 0.35}
PSA_WEIGHTS = {"pc": 0.55, "ebay": 0.45}


def _blend(values_and_weights):
    """Weighted average with dynamic redistribution for missing sources.

    Parameters
    ----------
    values_and_weights : list[tuple[float | None, float]]
        Each element is (value, base_weight).

    Returns
    -------
    float | None
    """
    available = [(v, w) for v, w in values_and_weights if v is not None]
    if not available:
        return None
    total_w = sum(w for _, w in available)
    return round(sum(v * (w / total_w) for v, w in available), 4)


def compute_composite(db, card_id):
    """Blend PriceCharting, eBay-derived, and JustTCG into composite prices.

    Reads the latest data from price_history, ebay_derived_history, and
    justtcg_history, then writes date-aligned composite rows into
    composite_history.

    Parameters
    ----------
    db : sqlite3.Connection
    card_id : str

    Returns
    -------
    dict  {"rows_written": int}
    """
    # Gather all dates across sources into a unified timeline
    dates = defaultdict(lambda: {
        "pc_raw": None, "pc_psa9": None, "pc_psa10": None,
        "eb_raw": None, "eb_psa9": None, "eb_psa10": None,
        "jt_raw": None,
    })

    # PriceCharting
    for r in db.execute(
        "SELECT date, raw_price, psa_9_price, psa_10_price "
        "FROM price_history WHERE card_id = ? ORDER BY date",
        (card_id,),
    ):
        d = dates[r["date"]]
        d["pc_raw"] = r["raw_price"]
        d["pc_psa9"] = r["psa_9_price"]
        d["pc_psa10"] = r["psa_10_price"]

    # eBay-derived
    for r in db.execute(
        "SELECT date, d_raw_price, d_psa_9_price, d_psa_10_price "
        "FROM ebay_derived_history WHERE card_id = ? ORDER BY date",
        (card_id,),
    ):
        d = dates[r["date"]]
        d["eb_raw"] = r["d_raw_price"]
        d["eb_psa9"] = r["d_psa_9_price"]
        d["eb_psa10"] = r["d_psa_10_price"]

    # JustTCG (raw only)
    for r in db.execute(
        "SELECT date, j_raw_price "
        "FROM justtcg_history WHERE card_id = ? ORDER BY date",
        (card_id,),
    ):
        dates[r["date"]]["jt_raw"] = r["j_raw_price"]

    if not dates:
        return {"rows_written": 0}

    # Compute composites
    inserts = []
    for dt in sorted(dates):
        s = dates[dt]

        c_raw = _blend([
            (s["pc_raw"],  RAW_WEIGHTS["pc"]),
            (s["eb_raw"],  RAW_WEIGHTS["ebay"]),
            (s["jt_raw"],  RAW_WEIGHTS["jtcg"]),
        ])

        c_psa9 = _blend([
            (s["pc_psa9"],  PSA_WEIGHTS["pc"]),
            (s["eb_psa9"],  PSA_WEIGHTS["ebay"]),
        ])

        c_psa10 = _blend([
            (s["pc_psa10"],  PSA_WEIGHTS["pc"]),
            (s["eb_psa10"],  PSA_WEIGHTS["ebay"]),
        ])

        inserts.append((card_id, dt, c_raw, c_psa9, c_psa10))

    db.executemany(
        """INSERT OR REPLACE INTO composite_history
           (card_id, date, c_raw_price, c_psa_9_price, c_psa_10_price)
           VALUES (?, ?, ?, ?, ?)""",
        inserts,
    )

    logger.info("composite card=%s  rows_written=%d", card_id, len(inserts))
    return {"rows_written": len(inserts)}
