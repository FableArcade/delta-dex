"""
Expected Value (EV) calculator per pack for a given set.

EV = SUM across rarity buckets of (pull_rate * avg_price_in_rarity).

Only cards with set_value_include='Y' are counted.  For each rarity
bucket the average raw (and PSA 10) price is the mean of the latest
prices for all included cards in that rarity.
"""

import logging

logger = logging.getLogger(__name__)


def _latest_prices_for_set(db, set_code, date):
    """Return the most recent price on or before `date` for each card.

    Returns
    -------
    list[sqlite3.Row]  with columns: card_id, rarity_code, raw_price, psa_10_price
    """
    return db.execute(
        """
        SELECT c.id AS card_id,
               c.rarity_code,
               ph.raw_price,
               ph.psa_10_price
          FROM cards c
          JOIN price_history ph
            ON ph.card_id = c.id
           AND ph.date = (
               SELECT MAX(ph2.date)
                 FROM price_history ph2
                WHERE ph2.card_id = c.id
                  AND ph2.date <= ?
           )
         WHERE c.set_code = ?
           AND c.set_value_include = 'Y'
        """,
        (date, set_code),
    ).fetchall()


def compute_ev_for_set(db, set_code, date):
    """Calculate EV raw and EV PSA 10 per pack for a set on a given date.

    Steps
    -----
    1. For each rarity bucket in the set, compute average raw price and
       average PSA 10 price across included cards.
    2. EV per pack = SUM(pull_rate * avg_price) over all rarity buckets.
    3. Compute total_set_raw_value = sum of latest raw prices for all
       included cards.
    4. Write per-rarity results to set_rarity_snapshot.
    5. Write set-level totals to set_daily.

    Parameters
    ----------
    db : sqlite3.Connection
    set_code : str
    date : str  (YYYY-MM-DD)

    Returns
    -------
    dict  {
        "ev_raw_per_pack": float,
        "ev_psa_10_per_pack": float,
        "total_set_raw_value": float,
        "rarity_buckets": int,
        "cards_counted": int,
    }
    """
    # Load rarity buckets for this set
    rarities = db.execute(
        "SELECT set_rarity, rarity_code, pull_rate FROM rarities "
        "WHERE set_code = ?",
        (set_code,),
    ).fetchall()

    rarity_map = {r["rarity_code"]: r for r in rarities}

    # Get latest prices for included cards
    card_prices = _latest_prices_for_set(db, set_code, date)

    if not card_prices:
        logger.warning("ev_calculator: no card prices for set=%s date=%s",
                       set_code, date)
        return {
            "ev_raw_per_pack": 0.0,
            "ev_psa_10_per_pack": 0.0,
            "total_set_raw_value": 0.0,
            "rarity_buckets": 0,
            "cards_counted": 0,
        }

    # Group prices by rarity_code
    from collections import defaultdict
    rarity_prices = defaultdict(lambda: {"raw": [], "psa10": []})

    total_set_raw_value = 0.0
    cards_counted = 0

    for cp in card_prices:
        rc = cp["rarity_code"]
        if cp["raw_price"] is not None:
            rarity_prices[rc]["raw"].append(cp["raw_price"])
            total_set_raw_value += cp["raw_price"]
        if cp["psa_10_price"] is not None:
            rarity_prices[rc]["psa10"].append(cp["psa_10_price"])
        cards_counted += 1

    # Compute EV per pack and write rarity snapshots
    ev_raw_total = 0.0
    ev_psa10_total = 0.0
    buckets_used = 0

    # Also collect PSA pop data for rarity snapshot (aggregate from psa_pop_history)
    psa_pop_agg = db.execute(
        """
        SELECT c.rarity_code,
               SUM(pp.psa_10_base) AS psa_pop_10_base,
               SUM(pp.total_base)  AS psa_pop_total_base,
               AVG(pp.gem_pct)     AS psa_avg_gem_pct
          FROM cards c
          JOIN psa_pop_history pp
            ON pp.card_id = c.id
           AND pp.date = (
               SELECT MAX(pp2.date)
                 FROM psa_pop_history pp2
                WHERE pp2.card_id = c.id
                  AND pp2.date <= ?
           )
         WHERE c.set_code = ?
           AND c.set_value_include = 'Y'
         GROUP BY c.rarity_code
        """,
        (date, set_code),
    ).fetchall()
    psa_by_rarity = {r["rarity_code"]: r for r in psa_pop_agg}

    for rc, rar in rarity_map.items():
        prices = rarity_prices.get(rc)
        if not prices or not prices["raw"]:
            continue

        avg_raw = sum(prices["raw"]) / len(prices["raw"])
        avg_psa10 = (sum(prices["psa10"]) / len(prices["psa10"])
                     if prices["psa10"] else None)

        pull_rate = rar["pull_rate"]
        ev_raw = pull_rate * avg_raw
        ev_psa10 = pull_rate * avg_psa10 if avg_psa10 is not None else None

        ev_raw_total += ev_raw
        if ev_psa10 is not None:
            ev_psa10_total += ev_psa10
        buckets_used += 1

        psa = psa_by_rarity.get(rc)
        psa_10_base = psa["psa_pop_10_base"] if psa else None
        psa_total_base = psa["psa_pop_total_base"] if psa else None
        psa_gem = psa["psa_avg_gem_pct"] if psa else None

        db.execute(
            """INSERT OR REPLACE INTO set_rarity_snapshot
               (set_rarity, date, avg_raw_price, avg_psa_10_price,
                ev_raw_per_pack, ev_psa_10_per_pack,
                psa_pop_10_base, psa_pop_total_base, psa_avg_gem_pct)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (rar["set_rarity"], date,
             round(avg_raw, 4),
             round(avg_psa10, 4) if avg_psa10 is not None else None,
             round(ev_raw, 4),
             round(ev_psa10, 4) if ev_psa10 is not None else None,
             psa_10_base, psa_total_base, psa_gem),
        )

    # Write set_daily (pack_cost filled separately)
    ev_raw_total = round(ev_raw_total, 4)
    ev_psa10_total = round(ev_psa10_total, 4)
    total_set_raw_value = round(total_set_raw_value, 4)

    # Fetch existing pack cost if already computed
    existing = db.execute(
        "SELECT avg_pack_cost FROM set_daily WHERE set_code = ? AND date = ?",
        (set_code, date),
    ).fetchone()
    avg_pack_cost = existing["avg_pack_cost"] if existing else None
    avg_gain_loss = (round(ev_raw_total - avg_pack_cost, 4)
                     if avg_pack_cost is not None else None)

    db.execute(
        """INSERT OR REPLACE INTO set_daily
           (set_code, date, ev_raw_per_pack, ev_psa_10_per_pack,
            avg_pack_cost, avg_gain_loss, total_set_raw_value)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (set_code, date, ev_raw_total, ev_psa10_total,
         avg_pack_cost, avg_gain_loss, total_set_raw_value),
    )

    logger.info(
        "ev_calculator set=%s date=%s  ev_raw=%.4f  ev_psa10=%.4f  "
        "total_raw=%.4f  buckets=%d  cards=%d",
        set_code, date, ev_raw_total, ev_psa10_total,
        total_set_raw_value, buckets_used, cards_counted,
    )

    return {
        "ev_raw_per_pack": ev_raw_total,
        "ev_psa_10_per_pack": ev_psa10_total,
        "total_set_raw_value": total_set_raw_value,
        "rarity_buckets": buckets_used,
        "cards_counted": cards_counted,
    }
