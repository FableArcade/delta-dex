"""Database connection layer — auto-detects SQLite vs Postgres.

If DATABASE_URL is set, uses Postgres (production on Railway).
Otherwise, uses local SQLite (development).
"""

import os

DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    # Postgres mode — import everything from the Postgres module
    try:
        from db.connection_pg import get_db, init_db
        import sys
        print("DB: Connected to Postgres", file=sys.stderr)
    except Exception as exc:
        import sys
        print(f"DB: Postgres import failed ({exc}), falling back to SQLite", file=sys.stderr)
        DATABASE_URL = None

if not DATABASE_URL:
    # SQLite mode — local development
    import sqlite3
    from contextlib import contextmanager
    from pathlib import Path
    from config.settings import DB_PATH

    def init_db():
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        schema_path = Path(__file__).parent / "schema.sql"
        with get_db() as db:
            db.executescript(schema_path.read_text())

    @contextmanager
    def get_db():
        conn = sqlite3.connect(str(DB_PATH), timeout=30.0)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 10000")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
