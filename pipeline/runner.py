"""Pipeline orchestrator -- runs collectors in sequence with logging."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from config.settings import settings
from db.connection import get_db
from pipeline.collectors.pricecharting import PriceChartingCollector
from pipeline.collectors.ebay import EBayCollector
from pipeline.collectors.justtcg import JustTCGCollector
from pipeline.collectors.psa_pop import PSAPopCollector

logger = logging.getLogger("pipeline.runner")


# ------------------------------------------------------------------
# Pipeline-run bookkeeping
# ------------------------------------------------------------------

def _start_run(stage: str) -> int:
    """Insert a pipeline_runs row and return its id."""
    with get_db() as db:
        cur = db.execute(
            "INSERT INTO pipeline_runs (started_at, status, stage) VALUES (?, 'running', ?)",
            (datetime.now(timezone.utc).isoformat(), stage),
        )
        return cur.lastrowid


def _finish_run(run_id: int, status: str, processed: int, errors: int, notes: str = "") -> None:
    with get_db() as db:
        db.execute(
            """
            UPDATE pipeline_runs
            SET finished_at = ?, status = ?, cards_processed = ?, errors = ?, notes = ?
            WHERE id = ?
            """,
            (datetime.now(timezone.utc).isoformat(), status, processed, errors, notes, run_id),
        )


# ------------------------------------------------------------------
# Runner helpers
# ------------------------------------------------------------------

def _run_collector(collector_cls, date: str) -> dict:
    """Instantiate, run, and close a single collector. Returns summary dict."""
    with collector_cls() as collector:
        logger.info("=== Starting %s ===", collector.name)
        result = collector.collect(date)
        logger.info(
            "=== Finished %s: %d processed, %d errors ===",
            collector.name, result["processed"], result["errors"],
        )
        return result


def _run_sequence(collectors: list, date: str, stage: str) -> None:
    """Run a list of collector classes in order, logging to pipeline_runs."""
    run_id = _start_run(stage)
    total_processed = 0
    total_errors = 0
    notes_parts: list[str] = []

    try:
        for cls in collectors:
            try:
                result = _run_collector(cls, date)
                total_processed += result["processed"]
                total_errors += result["errors"]
                notes_parts.append(
                    f"{cls.name}: {result['processed']}ok/{result['errors']}err"
                )
            except Exception as exc:
                total_errors += 1
                notes_parts.append(f"{cls.name}: CRASHED ({exc})")
                logger.exception("Collector %s crashed", cls.name)

        status = "done" if total_errors == 0 else "done_with_errors"
        _finish_run(run_id, status, total_processed, total_errors, "; ".join(notes_parts))
        logger.info("Pipeline %s complete: %d processed, %d errors", stage, total_processed, total_errors)

    except Exception as exc:
        _finish_run(run_id, "failed", total_processed, total_errors, str(exc))
        logger.exception("Pipeline %s failed", stage)
        raise


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

DAILY_COLLECTORS = [PriceChartingCollector, EBayCollector, JustTCGCollector]
WEEKLY_COLLECTORS = DAILY_COLLECTORS + [PSAPopCollector]


def run_daily(date: str | None = None) -> None:
    """Run the daily collection pipeline: pricecharting -> ebay -> justtcg."""
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(">>> Daily pipeline for %s", date)
    _run_sequence(DAILY_COLLECTORS, date, "daily")


def run_weekly(date: str | None = None) -> None:
    """Run the weekly pipeline: daily collectors + PSA Pop."""
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(">>> Weekly pipeline for %s", date)
    _run_sequence(WEEKLY_COLLECTORS, date, "weekly")


def run_single(collector_name: str, date: str | None = None) -> None:
    """Run a single named collector."""
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    name_map = {
        "pricecharting": PriceChartingCollector,
        "ebay": EBayCollector,
        "justtcg": JustTCGCollector,
        "psa_pop": PSAPopCollector,
    }
    cls = name_map.get(collector_name)
    if cls is None:
        raise ValueError(f"Unknown collector: {collector_name}. Options: {list(name_map.keys())}")

    logger.info(">>> Single run: %s for %s", collector_name, date)
    _run_sequence([cls], date, f"single:{collector_name}")


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def _setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(name)-22s | %(levelname)-5s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        root.addHandler(handler)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pokemon Analytics data collection pipeline",
    )
    parser.add_argument(
        "mode",
        choices=["daily", "weekly", "single"],
        help="Pipeline mode to run",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Collection date (YYYY-MM-DD). Defaults to today UTC.",
    )
    parser.add_argument(
        "--collector",
        default=None,
        help="Collector name (required for 'single' mode). "
             "Options: pricecharting, ebay, justtcg, psa_pop",
    )
    parser.add_argument(
        "--log-level",
        default=settings.log_level,
        help="Log level (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=settings.pipeline_dry_run,
        help="Log what would happen without making requests",
    )

    args = parser.parse_args()
    _setup_logging(args.log_level)

    if args.dry_run:
        logger.info("DRY RUN mode -- no actual requests will be made")
        # In a full implementation, collectors would check settings.pipeline_dry_run
        settings.pipeline_dry_run = True

    if args.mode == "daily":
        run_daily(args.date)
    elif args.mode == "weekly":
        run_weekly(args.date)
    elif args.mode == "single":
        if not args.collector:
            parser.error("--collector is required for 'single' mode")
        run_single(args.collector, args.date)


if __name__ == "__main__":
    main()
