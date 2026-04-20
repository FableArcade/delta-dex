"""Migrate SQLite database to PostgreSQL.

Reads all tables from the local SQLite DB and inserts into Postgres.
Creates tables first from schema_pg.sql, then bulk-inserts data.

Usage:
    DATABASE_URL="postgresql://..." python -m scripts.migrate_to_postgres
"""

import os
import sys
import sqlite3
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
import psycopg2.extras

SQLITE_PATH = Path(__file__).resolve().parent.parent / "data" / "pokemon.db"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "db" / "schema_pg.sql"

# Tables in dependency order (parents before children)
TABLES = [
    "sets",
    "rarities",
    "cards",
    "price_history",
    "psa_pop_history",
    "ebay_history",
    "ebay_market_history",
    "ebay_derived_history",
    "justtcg_history",
    "composite_history",
    "market_pressure",
    "supply_saturation",
    "set_daily",
    "leaderboard",
    "pack_cost",
    "set_rarity_snapshot",
    "set_alpha_linkage",
    "card_peer_correlation",
    "model_projections",
    "model_report_card",
    "model_promotion_log",
    "paper_trades",
    "narrow_target_predictions",
    "tournament_appearances",
    "pipeline_runs",
]


def migrate():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("Set DATABASE_URL env var")
        sys.exit(1)

    # Connect to both
    sqlite_conn = sqlite3.connect(str(SQLITE_PATH))
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg2.connect(db_url)

    # Create schema
    print("Creating Postgres schema...")
    with pg_conn.cursor() as cur:
        cur.execute(open(str(SCHEMA_PATH)).read())
    pg_conn.commit()
    print("Schema created.")

    # Migrate each table
    for table in TABLES:
        try:
            rows = sqlite_conn.execute(f"SELECT * FROM {table}").fetchall()
        except sqlite3.OperationalError:
            print(f"  SKIP {table} (not in SQLite)")
            continue

        if not rows:
            print(f"  SKIP {table} (empty)")
            continue

        cols = rows[0].keys()
        col_names = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))

        # Truncate existing Postgres data for this table
        with pg_conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table} CASCADE")

        # Bulk insert in batches of 1000
        batch_size = 1000
        total = len(rows)
        inserted = 0

        with pg_conn.cursor() as cur:
            for i in range(0, total, batch_size):
                batch = rows[i:i + batch_size]
                values = [tuple(row[c] for c in cols) for row in batch]
                try:
                    psycopg2.extras.execute_batch(
                        cur,
                        f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                        values,
                        page_size=batch_size,
                    )
                    inserted += len(batch)
                except Exception as exc:
                    pg_conn.rollback()
                    print(f"  ERROR {table} batch {i}: {exc}")
                    # Try row by row for this batch
                    for row in batch:
                        try:
                            cur.execute(
                                f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                                tuple(row[c] for c in cols),
                            )
                            inserted += 1
                        except Exception:
                            pass
                    pg_conn.commit()
                    continue

        pg_conn.commit()
        print(f"  {table}: {inserted}/{total} rows")

    sqlite_conn.close()
    pg_conn.close()
    print("\nMigration complete.")


if __name__ == "__main__":
    migrate()
