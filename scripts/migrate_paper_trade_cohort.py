"""Add `cohort` column to paper_trades and backfill existing rows.

Idempotent: safe to run repeatedly. If the column already exists, skips the
ALTER. Backfill computes predicted-return percentiles per (as_of) batch and
tags each trade as top_decile / top_quartile / middle / bottom_quartile.

Cohort definitions (by projected_return percentile within a day's batch):
    top_decile       -> rank >= P90   (top 10%)
    top_quartile     -> P75 <= rank < P90  (next 15%)
    middle           -> P25 <= rank < P75  (middle 50%)
    bottom_quartile  -> rank < P25   (bottom 25%)

Run:
    python scripts/migrate_paper_trade_cohort.py
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DB_PATH  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("migrate_paper_trade_cohort")


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def _cohort_for_rank(rank: int, n: int) -> str:
    """rank is 0-indexed ascending by projected_return. Higher rank = higher return."""
    if n <= 0:
        return "middle"
    # percentile in [0, 1)
    pct = rank / n
    if pct >= 0.90:
        return "top_decile"
    if pct >= 0.75:
        return "top_quartile"
    if pct >= 0.25:
        return "middle"
    return "bottom_quartile"


def backfill_cohorts(conn: sqlite3.Connection) -> dict:
    """Assign cohort to every row in paper_trades, grouped by as_of."""
    as_ofs = [r[0] for r in conn.execute(
        "SELECT DISTINCT as_of FROM paper_trades"
    ).fetchall()]

    totals = {"top_decile": 0, "top_quartile": 0, "middle": 0, "bottom_quartile": 0}
    for as_of in as_ofs:
        rows = conn.execute(
            """SELECT card_id, horizon_days, projected_return
                 FROM paper_trades
                WHERE as_of = ?
                ORDER BY projected_return ASC NULLS FIRST""",
            (as_of,),
        ).fetchall()
        # SQLite doesn't support NULLS FIRST in older versions; re-sort in py defensively.
        rows = sorted(rows, key=lambda r: (r[2] is None, r[2] if r[2] is not None else 0.0))
        n = len(rows)
        for idx, r in enumerate(rows):
            cohort = _cohort_for_rank(idx, n)
            conn.execute(
                """UPDATE paper_trades
                      SET cohort = ?
                    WHERE card_id = ? AND as_of = ? AND horizon_days = ?""",
                (cohort, r[0], as_of, r[1]),
            )
            totals[cohort] += 1
        logger.info("Backfilled %s: n=%d", as_of, n)
    conn.commit()
    return totals


def run(db_path: Path = DB_PATH) -> dict:
    if not db_path.exists():
        raise SystemExit(f"DB not found at {db_path}")
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.execute("PRAGMA foreign_keys = ON")
    result: dict = {"added_column": False, "backfill": {}}
    try:
        if _column_exists(conn, "paper_trades", "cohort"):
            logger.info("cohort column already present; skipping ALTER")
        else:
            conn.execute("ALTER TABLE paper_trades ADD COLUMN cohort TEXT")
            conn.commit()
            result["added_column"] = True
            logger.info("Added paper_trades.cohort TEXT")

        # Helpful index for cohort-filtered reports
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_paper_trades_cohort ON paper_trades(cohort, as_of)"
        )
        conn.commit()

        result["backfill"] = backfill_cohorts(conn)
    finally:
        conn.close()
    logger.info("Migration complete: %s", result)
    return result


if __name__ == "__main__":
    run()
