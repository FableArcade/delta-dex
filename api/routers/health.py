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

    # Is cron running? Try multiple detection methods.
    cron_running = None
    cron_pid = None
    for pid_path in ["/var/run/crond.pid", "/run/crond.pid", "/var/run/cron.pid"]:
        try:
            with open(pid_path) as f:
                cron_pid = f.read().strip()
                # Check if process exists
                os.kill(int(cron_pid), 0)
                cron_running = True
                break
        except (FileNotFoundError, ValueError, ProcessLookupError, PermissionError):
            continue
    if cron_running is None:
        # Fallback: check heartbeat recency
        try:
            with open("/tmp/logs/cron_heartbeat.log") as f:
                lines = f.readlines()
                if lines:
                    last = lines[-1].strip()
                    cron_running = True
                    cron_pid = f"heartbeat-alive (last: {last})"
        except FileNotFoundError:
            cron_running = False
            cron_pid = "no heartbeat, no PID file"

    # Read recent cron logs
    logs = {}
    for name in ["cron_ebay.log", "cron_daily.log", "cron_heartbeat.log", "trigger_ebay.log", "trigger_signals.log", "trigger_seed.log", "trigger_price_scrape.log", "trigger_daily_pipeline.log"]:
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


@router.post("/trigger_daily_pipeline")
def trigger_daily_pipeline(stage: str = "all"):
    """Run the daily pipeline. stage='all' (default), 'scrape', or 'compute'."""
    import threading

    def run():
        try:
            import subprocess
            cmd = ["/usr/local/bin/python", "-m", "pipeline.daily_pipeline"]
            if stage != "all":
                cmd.extend(["--stage", stage])
            result = subprocess.run(
                cmd,
                cwd="/app",
                capture_output=True,
                text=True,
                timeout=7200,
            )
            with open("/tmp/logs/trigger_daily_pipeline.log", "w") as f:
                f.write(f"returncode: {result.returncode}\n")
                f.write(f"--- stdout (last 5000) ---\n{result.stdout[-5000:]}\n")
                f.write(f"--- stderr (last 3000) ---\n{result.stderr[-3000:]}\n")
        except Exception as e:
            with open("/tmp/logs/trigger_daily_pipeline.log", "w") as f:
                f.write(f"ERROR: {e}\n")

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return {"status": "started", "message": "Daily pipeline running. Check /api/cron_status for trigger_daily_pipeline.log."}


@router.post("/trigger_price_scrape")
def trigger_price_scrape(set_code: str = ""):
    """Scrape PriceCharting prices for cards missing price data. Optionally filter by set_code."""
    import threading

    def run():
        try:
            import subprocess
            cmd = ["/usr/local/bin/python", "-m", "scripts.bootstrap_pc_history_and_images",
                   "--resume"]
            if set_code:
                cmd.extend(["--set-code", set_code])
            result = subprocess.run(
                cmd,
                cwd="/app",
                capture_output=True,
                text=True,
                timeout=3600,
            )
            with open("/tmp/logs/trigger_price_scrape.log", "w") as f:
                f.write(f"returncode: {result.returncode}\n")
                f.write(f"--- stdout (last 5000) ---\n{result.stdout[-5000:]}\n")
                f.write(f"--- stderr (last 3000) ---\n{result.stderr[-3000:]}\n")
        except Exception as e:
            with open("/tmp/logs/trigger_price_scrape.log", "w") as f:
                f.write(f"ERROR: {e}\n")

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return {"status": "started", "set_code": set_code or "all", "message": "Price scrape running with --resume. Check /api/cron_status logs for trigger_price_scrape.log."}


@router.post("/trigger_seed_sets")
def trigger_seed_sets(set_ids: str = ""):
    """Seed specific sets from pokemontcg.io. Pass comma-separated set IDs, or leave empty for all promo sets."""
    import threading

    target_ids = [s.strip() for s in set_ids.split(",") if s.strip()] if set_ids else [
        "basep", "np", "dpp", "hsp", "bwp", "xyp", "smp", "swshp", "svp"
    ]

    def run():
        try:
            import subprocess
            result = subprocess.run(
                ["/usr/local/bin/python", "-m", "scripts.seed_from_pokemontcg",
                 "--sets", ",".join(target_ids)],
                cwd="/app",
                capture_output=True,
                text=True,
                timeout=1800,
            )
            with open("/tmp/logs/trigger_seed.log", "w") as f:
                f.write(f"returncode: {result.returncode}\n")
                f.write(f"--- stdout (last 5000) ---\n{result.stdout[-5000:]}\n")
                f.write(f"--- stderr (last 2000) ---\n{result.stderr[-2000:]}\n")
        except Exception as e:
            with open("/tmp/logs/trigger_seed.log", "w") as f:
                f.write(f"ERROR: {e}\n")

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return {"status": "started", "sets": target_ids, "message": "Seeding in background. Check /api/cron_status logs for trigger_seed.log."}


@router.post("/trigger_ebay")
def trigger_ebay():
    """Manually trigger eBay signal universe collection in background."""
    import subprocess
    import threading

    def run():
        result = subprocess.run(
            ["/usr/local/bin/python", "-m", "scripts.populate_ebay_signal_universe"],
            cwd="/app",
            capture_output=True,
            text=True,
            timeout=1800,
        )
        # Write output for debugging
        with open("/tmp/logs/trigger_ebay.log", "w") as f:
            f.write(f"returncode: {result.returncode}\n")
            f.write(f"--- stdout ---\n{result.stdout[-3000:]}\n")
            f.write(f"--- stderr ---\n{result.stderr[-3000:]}\n")

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return {"status": "started", "message": "eBay collection running in background. Check /api/cron_status logs in ~10 min."}


@router.post("/trigger_signals")
def trigger_signals():
    """Recompute market_pressure + supply_saturation from existing eBay data. No API keys needed."""
    import threading

    def run():
        try:
            from db.connection import get_db
            from pipeline.compute.market_pressure import compute_market_pressure

            with get_db() as db:
                all_ids = [r["card_id"] for r in db.execute(
                    "SELECT DISTINCT card_id FROM ebay_history"
                ).fetchall()]

            ok, failed = 0, 0
            with get_db() as db:
                for i, cid in enumerate(all_ids):
                    try:
                        compute_market_pressure(db, cid)
                        ok += 1
                    except Exception:
                        failed += 1
                    if (i + 1) % 200 == 0:
                        with open("/tmp/logs/trigger_signals.log", "w") as f:
                            f.write(f"Progress: {i+1}/{len(all_ids)} ok={ok} failed={failed}\n")

            with open("/tmp/logs/trigger_signals.log", "w") as f:
                f.write(f"Done: {ok} ok, {failed} failed out of {len(all_ids)}\n")
        except Exception as e:
            with open("/tmp/logs/trigger_signals.log", "w") as f:
                f.write(f"ERROR: {e}\n")

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return {"status": "started", "cards": "all with ebay data", "message": "Signal recompute running. Check /api/cron_status logs."}


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
