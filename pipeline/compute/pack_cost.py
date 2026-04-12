"""
Pack cost derivation from sealed product prices.

Finds sealed products ('Booster Pack', 'Sleeved Booster Pack',
'Booster Bundle') for a set, computes per-pack cost for each type,
then averages available components into avg_pack_cost.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Number of packs inside a Booster Bundle
BUNDLE_PACK_COUNT = 6


def compute_pack_cost(db, set_code, date):
    """Derive average pack cost from sealed product prices.

    Parameters
    ----------
    db : sqlite3.Connection
    set_code : str
    date : str  (YYYY-MM-DD)

    Returns
    -------
    dict  {
        "avg_pack_cost": float | None,
        "avg_booster_pack": float | None,
        "avg_sleeved_booster_pack": float | None,
        "avg_booster_bundle_per_pack": float | None,
    }
    """
    # Find sealed products for this set
    sealed = db.execute(
        """SELECT id, sealed_type
             FROM cards
            WHERE set_code = ?
              AND sealed_product = 'Y'
              AND sealed_type IN ('Booster Pack',
                                  'Sleeved Booster Pack',
                                  'Booster Bundle')""",
        (set_code,),
    ).fetchall()

    if not sealed:
        logger.info("pack_cost: no sealed products for set=%s", set_code)
        return {
            "avg_pack_cost": None,
            "avg_booster_pack": None,
            "avg_sleeved_booster_pack": None,
            "avg_booster_bundle_per_pack": None,
        }

    # Group by type and get latest prices
    type_prices = {
        "Booster Pack": [],
        "Sleeved Booster Pack": [],
        "Booster Bundle": [],
    }

    for s in sealed:
        price_row = db.execute(
            """SELECT raw_price
                 FROM price_history
                WHERE card_id = ?
                  AND date <= ?
                  AND raw_price IS NOT NULL
                ORDER BY date DESC
                LIMIT 1""",
            (s["id"], date),
        ).fetchone()

        if price_row and price_row["raw_price"] is not None:
            type_prices[s["sealed_type"]].append(price_row["raw_price"])

    # Compute per-type averages
    avg_booster = None
    avg_sleeved = None
    avg_bundle_per_pack = None

    if type_prices["Booster Pack"]:
        avg_booster = sum(type_prices["Booster Pack"]) / len(type_prices["Booster Pack"])

    if type_prices["Sleeved Booster Pack"]:
        avg_sleeved = (sum(type_prices["Sleeved Booster Pack"])
                       / len(type_prices["Sleeved Booster Pack"]))

    if type_prices["Booster Bundle"]:
        avg_bundle = (sum(type_prices["Booster Bundle"])
                      / len(type_prices["Booster Bundle"]))
        avg_bundle_per_pack = avg_bundle / BUNDLE_PACK_COUNT

    # Average available components
    components = [v for v in [avg_booster, avg_sleeved, avg_bundle_per_pack]
                  if v is not None]
    avg_pack_cost = round(sum(components) / len(components), 4) if components else None

    # Counts for metadata
    bp_count = len(type_prices["Booster Pack"])
    sl_count = len(type_prices["Sleeved Booster Pack"])
    bb_count = len(type_prices["Booster Bundle"])

    # Insert into pack_cost table
    db.execute(
        """INSERT OR REPLACE INTO pack_cost
           (set_code, date,
            avg_booster_pack, avg_sleeved_booster_pack,
            avg_booster_bundle_per_pack, avg_pack_cost,
            booster_pack_count, sleeved_booster_count, booster_bundle_count)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (set_code, date,
         round(avg_booster, 4) if avg_booster is not None else None,
         round(avg_sleeved, 4) if avg_sleeved is not None else None,
         round(avg_bundle_per_pack, 4) if avg_bundle_per_pack is not None else None,
         avg_pack_cost, bp_count, sl_count, bb_count),
    )

    # Also update set_daily with the pack cost if a row exists
    existing = db.execute(
        "SELECT ev_raw_per_pack FROM set_daily WHERE set_code = ? AND date = ?",
        (set_code, date),
    ).fetchone()

    if existing and avg_pack_cost is not None:
        ev_raw = existing["ev_raw_per_pack"]
        avg_gain_loss = round(ev_raw - avg_pack_cost, 4) if ev_raw is not None else None
        db.execute(
            """UPDATE set_daily
                  SET avg_pack_cost = ?, avg_gain_loss = ?
                WHERE set_code = ? AND date = ?""",
            (avg_pack_cost, avg_gain_loss, set_code, date),
        )

    logger.info(
        "pack_cost set=%s date=%s  avg_pack_cost=%s  "
        "bp=%d sl=%d bb=%d",
        set_code, date, avg_pack_cost, bp_count, sl_count, bb_count,
    )

    return {
        "avg_pack_cost": avg_pack_cost,
        "avg_booster_pack": round(avg_booster, 4) if avg_booster else None,
        "avg_sleeved_booster_pack": round(avg_sleeved, 4) if avg_sleeved else None,
        "avg_booster_bundle_per_pack": (round(avg_bundle_per_pack, 4)
                                        if avg_bundle_per_pack else None),
    }
