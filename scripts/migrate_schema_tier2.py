"""Tier 2 schema migration — idempotent.

Applies ONLY the Tier 2 schema changes listed in the Tier 2 spec:
  - ADD columns:
      model_report_card.promotion_status TEXT
      model_report_card.promotion_reason TEXT
      model_projections.model_version    TEXT  (already exists; skipped)
      model_projections.training_cutoff  DATE
      model_projections.feature_hash     TEXT
  - CREATE tables:
      paper_trades
      model_promotion_log
      narrow_target_predictions
  - CREATE supporting indexes

Does NOT touch `pipeline_runs` — the ops agent owns that column.

Back up the DB first:
  cp data/pokemon.db data/pokemon.db.bak.tier2

Run:
  python scripts/migrate_schema_tier2.py
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

# Allow running as a script from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DB_PATH  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("migrate_schema_tier2")


ADD_COLUMNS = [
    # (table, column, type)
    ("model_report_card", "promotion_status", "TEXT DEFAULT 'pending'"),
    ("model_report_card", "promotion_reason", "TEXT"),
    ("model_projections", "training_cutoff", "DATE"),
    ("model_projections", "feature_hash", "TEXT"),
]

CREATE_TABLES = [
    """CREATE TABLE IF NOT EXISTS paper_trades (
        card_id         TEXT NOT NULL REFERENCES cards(id),
        as_of           TEXT NOT NULL,
        horizon_days    INTEGER NOT NULL,
        model_version   TEXT NOT NULL,
        entry_price     REAL,
        projected_return REAL,
        confidence_low  REAL,
        confidence_high REAL,
        exit_date       TEXT,
        exit_price      REAL,
        realized_return_gross REAL,
        realized_return_net   REAL,
        hit             INTEGER,
        evaluated_at    TEXT,
        PRIMARY KEY (card_id, as_of, horizon_days)
    )""",
    """CREATE TABLE IF NOT EXISTS model_promotion_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        model_version   TEXT NOT NULL,
        evaluated_at    TEXT NOT NULL,
        decision        TEXT NOT NULL,
        walkforward_sharpe REAL,
        walkforward_hit_rate REAL,
        walkforward_top_decile_net REAL,
        walkforward_n   INTEGER,
        reason          TEXT,
        gate_version    TEXT,
        metrics_json    TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS narrow_target_predictions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        target_name     TEXT NOT NULL,
        card_id         TEXT NOT NULL REFERENCES cards(id),
        event_date      TEXT NOT NULL,
        horizon_days    INTEGER NOT NULL,
        predicted_return REAL,
        confidence      REAL,
        event_features_json TEXT,
        model_version   TEXT,
        created_at      TEXT DEFAULT (datetime('now')),
        UNIQUE (target_name, card_id, event_date, horizon_days)
    )""",
]

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_paper_trades_asof ON paper_trades(as_of)",
    "CREATE INDEX IF NOT EXISTS idx_paper_trades_exit ON paper_trades(exit_date)",
    "CREATE INDEX IF NOT EXISTS idx_paper_trades_unevaluated "
    "ON paper_trades(evaluated_at) WHERE evaluated_at IS NULL",
    "CREATE INDEX IF NOT EXISTS idx_promotion_log_version "
    "ON model_promotion_log(model_version, evaluated_at)",
    "CREATE INDEX IF NOT EXISTS idx_narrow_predictions_target "
    "ON narrow_target_predictions(target_name, event_date)",
]


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    for r in rows:
        # PRAGMA: (cid, name, type, notnull, dflt_value, pk)
        if r[1] == column:
            return True
    return False


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def run(db_path: Path = DB_PATH) -> dict:
    if not db_path.exists():
        raise SystemExit(f"DB not found at {db_path}")
    logger.info("Migrating %s", db_path)
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.execute("PRAGMA foreign_keys = ON")
    result = {"added_columns": [], "created_tables": [], "created_indexes": 0,
              "skipped_columns": [], "skipped_tables": []}
    try:
        # Add columns
        for table, col, ctype in ADD_COLUMNS:
            if not table_exists(conn, table):
                logger.warning("Table %s missing — skipping column %s", table, col)
                result["skipped_columns"].append(f"{table}.{col}(no table)")
                continue
            if column_exists(conn, table, col):
                result["skipped_columns"].append(f"{table}.{col}")
                logger.info("Column %s.%s already exists; skipping", table, col)
                continue
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ctype}")
            result["added_columns"].append(f"{table}.{col}")
            logger.info("Added column %s.%s %s", table, col, ctype)

        # Create tables
        for ddl in CREATE_TABLES:
            # Extract table name for reporting (cheap parse)
            name = ddl.split("EXISTS", 1)[1].split("(", 1)[0].strip()
            existed = table_exists(conn, name)
            conn.execute(ddl)
            if existed:
                result["skipped_tables"].append(name)
            else:
                result["created_tables"].append(name)
                logger.info("Created table %s", name)

        # Indexes
        for ddl in CREATE_INDEXES:
            conn.execute(ddl)
            result["created_indexes"] += 1

        conn.commit()
    finally:
        conn.close()
    logger.info("Migration complete: %s", result)
    return result


if __name__ == "__main__":
    run()
