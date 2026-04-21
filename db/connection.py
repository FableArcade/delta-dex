"""Database connection layer — auto-detects SQLite vs Postgres."""

import os

_url = os.environ.get("DATABASE_URL", "")

# Write to a file so we can verify what Python sees
with open("/tmp/db_debug.txt", "w") as f:
    f.write(f"DATABASE_URL={_url[:20] if _url else 'NONE'}\n")

if _url and _url.startswith("postgres"):
    from db.connection_pg import get_db, init_db  # noqa
else:
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
# Force rebuild 1776729236
