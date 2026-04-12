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
    conn.execute("PRAGMA busy_timeout = 10000")  # 10s — tolerate concurrent writers
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
