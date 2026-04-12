"""Leaderboard endpoint."""

from fastapi import APIRouter, Depends
from api.deps import get_db_conn

router = APIRouter()


@router.get("/leaderboard")
def leaderboard(db=Depends(get_db_conn)):
    """Return the latest leaderboard snapshot with all sets ranked."""

    # Get the most recent date
    max_date_row = db.execute(
        "SELECT MAX(date) AS d FROM leaderboard"
    ).fetchone()
    generated_at = max_date_row["d"] if max_date_row else None

    rows = db.execute("""
        SELECT
            l.set_code, s.set_name, s.logo_url, l.date,
            l.rarity_buckets, l.cards_counted, l.avg_pack_cost,
            l.ev_raw_per_pack, l.ev_psa_10_per_pack, l.avg_gain_loss,
            l.total_set_raw_value,
            l.psa_pop_10_base, l.psa_pop_total_base, l.psa_avg_gem_pct,
            l.rank_avg_gain_loss, l.rank_ev_raw_per_pack,
            l.rank_total_set_raw_value, l.rank_psa_avg_gem_pct
        FROM leaderboard l
        JOIN sets s ON s.set_code = l.set_code
        WHERE l.date = (SELECT MAX(date) FROM leaderboard)
        ORDER BY l.rank_avg_gain_loss ASC
    """).fetchall()

    result_rows = []
    for r in rows:
        result_rows.append({
            "set-code": r["set_code"],
            "set-name": r["set_name"],
            "logo-url": r["logo_url"],
            "generated-at": r["date"],
            "rarity-buckets": r["rarity_buckets"],
            "cards-counted": r["cards_counted"],
            "avg-pack-cost": r["avg_pack_cost"],
            "ev-raw-per-pack": r["ev_raw_per_pack"],
            "ev-psa-10-per-pack": r["ev_psa_10_per_pack"],
            "avg-gain-loss": r["avg_gain_loss"],
            "total-set-raw-value": r["total_set_raw_value"],
            "psa-pop-10-base": r["psa_pop_10_base"],
            "psa-pop-total-base": r["psa_pop_total_base"],
            "psa-avg-gem-pct": r["psa_avg_gem_pct"],
            "rank-avg-gain-loss": r["rank_avg_gain_loss"],
            "rank-ev-raw-per-pack": r["rank_ev_raw_per_pack"],
            "rank-total-set-raw-value": r["rank_total_set_raw_value"],
            "rank-psa-avg-gem-pct": r["rank_psa_avg_gem_pct"],
        })

    return {
        "generated-at": generated_at,
        "default-ranking-metric": "avg-gain-loss",
        "rows": result_rows,
    }
