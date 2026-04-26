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

    # Market pressure freshness
    mp_row = db.execute(
        "SELECT MAX(as_of) AS latest, COUNT(DISTINCT card_id) AS cards "
        "FROM market_pressure WHERE window_days = 30 AND mode = 'observed'"
    ).fetchone()
    mp_latest = mp_row["latest"] if mp_row else None
    mp_cards = mp_row["cards"] if mp_row else 0

    # eBay collection freshness
    ebay_row = db.execute(
        "SELECT MAX(date) AS latest, COUNT(DISTINCT card_id) AS cards "
        "FROM ebay_history"
    ).fetchone()
    ebay_latest = ebay_row["latest"] if ebay_row else None
    ebay_cards = ebay_row["cards"] if ebay_row else 0

    # Cron log check
    cron_row = db.execute(
        "SELECT MAX(date) AS latest FROM price_history"
    ).fetchone()
    price_latest = cron_row["latest"] if cron_row else None

    return {
        "status": "ok",
        "generated-at": datetime.utcnow().strftime("%Y-%m-%d"),
        "last-pipeline-run": last_run,
        "freshness": {
            "market-pressure-latest": mp_latest,
            "market-pressure-cards": mp_cards,
            "ebay-history-latest": ebay_latest,
            "ebay-history-cards": ebay_cards,
            "price-history-latest": price_latest,
        },
        "db-stats": {
            "total-sets": total_sets,
            "total-cards": total_cards,
            "table-row-counts": table_counts,
        },
    }

@router.get("/db_debug")
def db_debug():
    import os
    return {
        "DATABASE_URL_set": bool(os.environ.get("DATABASE_URL")),
        "DATABASE_URL_value": os.environ.get("DATABASE_URL", "NOT SET")[:30] + "...",
        "connection_module": str(type(get_db)),
    }

@router.get("/db_debug2")
def db_debug2():
    import os
    url = os.environ.get("DATABASE_URL", "")
    return {
        "DATABASE_URL_set": bool(url),
        "DATABASE_URL_starts_with": url[:40] if url else "EMPTY/NONE",
        "all_db_vars": {k: v[:20] for k, v in os.environ.items() if "DATABASE" in k or "PG" in k},
    }
