"""
Leaderboard: rank all sets across multiple dimensions.

Reads the latest set_daily and set_rarity_snapshot data, then ranks
all 20 sets on 4 dimensions.  Lower rank number = better.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _rank_desc(values):
    """Rank values highest-first (highest value gets rank 1).

    Parameters
    ----------
    values : list[tuple[str, float | None]]
        [(set_code, value), ...]

    Returns
    -------
    dict  {set_code: rank}
    """
    sortable = [(sc, v if v is not None else float("-inf"))
                for sc, v in values]
    sortable.sort(key=lambda x: x[1], reverse=True)
    return {sc: i + 1 for i, (sc, _) in enumerate(sortable)}


def _rank_asc(values):
    """Rank values with least-negative first (closest to zero or highest).

    For avg_gain_loss where -0.50 is better than -2.00.

    Parameters
    ----------
    values : list[tuple[str, float | None]]

    Returns
    -------
    dict  {set_code: rank}
    """
    sortable = [(sc, v if v is not None else float("-inf"))
                for sc, v in values]
    sortable.sort(key=lambda x: x[1], reverse=True)
    return {sc: i + 1 for i, (sc, _) in enumerate(sortable)}


def compute_leaderboard(db, date):
    """Rank all sets and write to leaderboard table.

    Parameters
    ----------
    db : sqlite3.Connection
    date : str  (YYYY-MM-DD)

    Returns
    -------
    dict  {"sets_ranked": int}
    """
    sets = db.execute("SELECT set_code FROM sets").fetchall()
    if not sets:
        return {"sets_ranked": 0}

    set_codes = [s["set_code"] for s in sets]

    # Gather data for each set
    set_data = {}
    for sc in set_codes:
        # set_daily
        daily = db.execute(
            """SELECT ev_raw_per_pack, ev_psa_10_per_pack,
                      avg_pack_cost, avg_gain_loss, total_set_raw_value
                 FROM set_daily
                WHERE set_code = ? AND date <= ?
                ORDER BY date DESC LIMIT 1""",
            (sc, date),
        ).fetchone()

        # Rarity bucket count and PSA gem %
        rarity_stats = db.execute(
            """SELECT COUNT(*) AS rarity_buckets,
                      SUM(psa_pop_10_base) AS psa_pop_10_base,
                      SUM(psa_pop_total_base) AS psa_pop_total_base,
                      AVG(psa_avg_gem_pct) AS psa_avg_gem_pct
                 FROM set_rarity_snapshot sr
                 JOIN rarities r ON r.set_rarity = sr.set_rarity
                WHERE r.set_code = ? AND sr.date <= ?
                  AND sr.date = (
                      SELECT MAX(sr2.date)
                        FROM set_rarity_snapshot sr2
                       WHERE sr2.set_rarity = sr.set_rarity
                         AND sr2.date <= ?
                  )""",
            (sc, date, date),
        ).fetchone()

        # Cards counted
        cards_counted_row = db.execute(
            "SELECT COUNT(*) AS cnt FROM cards "
            "WHERE set_code = ? AND set_value_include = 'Y'",
            (sc,),
        ).fetchone()

        set_data[sc] = {
            "ev_raw_per_pack": daily["ev_raw_per_pack"] if daily else None,
            "ev_psa_10_per_pack": daily["ev_psa_10_per_pack"] if daily else None,
            "avg_pack_cost": daily["avg_pack_cost"] if daily else None,
            "avg_gain_loss": daily["avg_gain_loss"] if daily else None,
            "total_set_raw_value": daily["total_set_raw_value"] if daily else None,
            "rarity_buckets": rarity_stats["rarity_buckets"] if rarity_stats else 0,
            "cards_counted": cards_counted_row["cnt"] if cards_counted_row else 0,
            "psa_pop_10_base": rarity_stats["psa_pop_10_base"] if rarity_stats else None,
            "psa_pop_total_base": rarity_stats["psa_pop_total_base"] if rarity_stats else None,
            "psa_avg_gem_pct": rarity_stats["psa_avg_gem_pct"] if rarity_stats else None,
        }

    # --- Rank across 4 dimensions ---
    # rank_avg_gain_loss: least negative first (highest value = rank 1)
    ranks_gain = _rank_asc(
        [(sc, d["avg_gain_loss"]) for sc, d in set_data.items()]
    )
    # rank_ev_raw_per_pack: highest first
    ranks_ev = _rank_desc(
        [(sc, d["ev_raw_per_pack"]) for sc, d in set_data.items()]
    )
    # rank_total_set_raw_value: highest first
    ranks_value = _rank_desc(
        [(sc, d["total_set_raw_value"]) for sc, d in set_data.items()]
    )
    # rank_psa_avg_gem_pct: highest first
    ranks_gem = _rank_desc(
        [(sc, d["psa_avg_gem_pct"]) for sc, d in set_data.items()]
    )

    # --- Write leaderboard ---
    for sc in set_codes:
        d = set_data[sc]
        db.execute(
            """INSERT OR REPLACE INTO leaderboard
               (set_code, date, rarity_buckets, cards_counted,
                avg_pack_cost, ev_raw_per_pack, ev_psa_10_per_pack,
                avg_gain_loss, total_set_raw_value,
                psa_pop_10_base, psa_pop_total_base, psa_avg_gem_pct,
                rank_avg_gain_loss, rank_ev_raw_per_pack,
                rank_total_set_raw_value, rank_psa_avg_gem_pct)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (sc, date,
             d["rarity_buckets"], d["cards_counted"],
             d["avg_pack_cost"], d["ev_raw_per_pack"],
             d["ev_psa_10_per_pack"], d["avg_gain_loss"],
             d["total_set_raw_value"],
             d["psa_pop_10_base"], d["psa_pop_total_base"],
             d["psa_avg_gem_pct"],
             ranks_gain[sc], ranks_ev[sc],
             ranks_value[sc], ranks_gem[sc]),
        )

    logger.info("leaderboard date=%s  sets_ranked=%d", date, len(set_codes))
    return {"sets_ranked": len(set_codes)}
