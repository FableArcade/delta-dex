"""Health check endpoint."""

from datetime import datetime
from fastapi import APIRouter, Depends
from api.deps import get_db_conn

router = APIRouter()


@router.get("/health")
def health(db=Depends(get_db_conn)):
    """Pipeline status, last run timestamp, and database stats."""

    # Last leaderboard run
    row = db.execute(
        "SELECT MAX(date) AS last_run FROM leaderboard"
    ).fetchone()
    last_run = row["last_run"] if row else None

    # Table row counts
    tables = [
        "sets", "rarities", "cards", "price_history", "set_daily",
        "leaderboard", "pack_cost", "set_rarity_snapshot",
        "ebay_history", "ebay_market_history", "ebay_derived_history",
        "justtcg_history", "composite_history",
        "market_pressure", "supply_saturation",
    ]
    table_counts = {}
    for t in tables:
        cnt = db.execute(f"SELECT COUNT(*) AS c FROM {t}").fetchone()["c"]
        table_counts[t] = cnt

    total_cards = table_counts.get("cards", 0)
    total_sets = table_counts.get("sets", 0)

    return {
        "status": "ok",
        "generated-at": datetime.utcnow().strftime("%Y-%m-%d"),
        "last-pipeline-run": last_run,
        "db-stats": {
            "total-sets": total_sets,
            "total-cards": total_cards,
            "table-row-counts": table_counts,
        },
    }
