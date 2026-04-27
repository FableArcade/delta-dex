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

@router.get("/cron_status")
def cron_status():
    """Check if cron daemon is running and show recent log output."""
    import subprocess
    import os

    # Is cron running?
    try:
        ps = subprocess.run(
            ["ps", "aux"], capture_output=True, text=True, timeout=5
        )
        cron_lines = [l for l in ps.stdout.splitlines() if "cron" in l.lower() and "grep" not in l]
        cron_running = len(cron_lines) > 0
        cron_pid = cron_lines[0].split()[1] if cron_running else None
    except Exception as e:
        cron_running = None
        cron_pid = str(e)

    # Read recent cron logs
    logs = {}
    for name in ["cron_ebay.log", "cron_daily.log", "cron_heartbeat.log"]:
        path = f"/tmp/logs/{name}"
        try:
            with open(path) as f:
                lines = f.readlines()
                logs[name] = "".join(lines[-30:])  # last 30 lines
        except FileNotFoundError:
            logs[name] = "file not found"

    # Check crontab
    try:
        ct = subprocess.run(["cat", "/etc/cron.d/deltadex"], capture_output=True, text=True, timeout=5)
        crontab = ct.stdout
    except Exception as e:
        crontab = str(e)

    return {
        "cron_running": cron_running,
        "cron_pid": cron_pid,
        "crontab": crontab,
        "logs": logs,
    }


@router.post("/trigger_ebay")
def trigger_ebay():
    """Manually trigger eBay signal universe collection in background."""
    import subprocess
    import threading

    def run():
        subprocess.run(
            ["/usr/local/bin/python", "-m", "scripts.populate_ebay_signal_universe"],
            cwd="/app",
            capture_output=True,
            timeout=1800,
        )

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return {"status": "started", "message": "eBay collection running in background. Check /api/health freshness in ~10 min."}


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
