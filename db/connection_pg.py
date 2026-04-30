"""PostgreSQL connection layer — drop-in replacement for connection.py.

Uses psycopg2 with a connection pool. The get_db() context manager returns
a cursor with dict-like row access (RealDictCursor) so existing code that
does row["column_name"] keeps working.

Set DATABASE_URL env var to the Railway Postgres connection string.
"""

import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
import psycopg2.pool

_pool = None


def _get_pool():
    global _pool
    if _pool is None:
        url = os.environ.get("DATABASE_URL")
        if not url:
            raise RuntimeError("DATABASE_URL not set")
        _pool = psycopg2.pool.ThreadedConnectionPool(1, 10, url, connect_timeout=5)
    return _pool


def init_db():
    """Create tables if they don't exist."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema_pg.sql")
    with get_db() as db:
        db.execute(open(schema_path).read())


class PgCursorWrapper:
    """Wraps a psycopg2 RealDictCursor to match sqlite3 API patterns.

    - execute() returns self (for chaining)
    - fetchone() returns a dict-like row
    - fetchall() returns list of dict-like rows
    - executescript() splits on ; and executes each statement
    """

    def __init__(self, cursor, conn):
        self._cursor = cursor
        self._conn = conn

    def execute(self, sql, params=None):
        # Convert SQLite ? placeholders to Postgres %s
        sql = sql.replace("?", "%s")
        # INSERT OR REPLACE → INSERT ... ON CONFLICT DO UPDATE
        import re as _re
        if "INSERT OR REPLACE INTO" in sql:
            # Extract table name and columns
            m = _re.match(r"INSERT OR REPLACE INTO\s+(\w+)\s*\(([^)]+)\)", sql, _re.I | _re.S)
            if m:
                table = m.group(1)
                cols = [c.strip() for c in m.group(2).split(",")]
                # Assume first column is the primary key for conflict
                # Map table names to their primary key columns for ON CONFLICT
                _pk_map = {
                    "market_pressure": "card_id, window_days, mode, as_of",
                    "supply_saturation": "card_id, mode, as_of",
                    "leaderboard": "set_code, date",
                    "pack_cost": "set_code, date",
                    "set_rarity_snapshot": "set_rarity, date",
                    "price_history": "card_id, date",
                    "ebay_history": "card_id, date",
                    "psa_pop_history": "card_id, date",
                    "set_daily": "set_code, date",
                }
                conflict_cols = _pk_map.get(table, cols[0])
                updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols[1:])
                sql = sql.replace("INSERT OR REPLACE INTO", "INSERT INTO")
                sql = sql.rstrip().rstrip(";")
                sql += f" ON CONFLICT ({conflict_cols}) DO UPDATE SET {updates}"
        # INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
        sql = sql.replace("INSERT OR IGNORE INTO", "INSERT INTO")
        if "ON CONFLICT" not in sql and "DO NOTHING" not in sql and sql.strip().upper().startswith("INSERT INTO"):
            pass  # leave as-is, conflict handled above
        # Convert SQLite datetime('now') to Postgres NOW()
        sql = sql.replace("datetime('now')", "NOW()")
        sql = sql.replace("date('now')", "CURRENT_DATE::text")
        sql = sql.replace("date('now', 'localtime')", "CURRENT_DATE::text")
        # Convert SQLite date('now', '-N days') to Postgres
        import re
        sql = re.sub(
            r"date\('now',\s*'(-?\d+)\s*days?'\s*(?:,\s*'localtime')?\)",
            r"(CURRENT_DATE + INTERVAL '\1 days')::date::text",
            sql
        )
        sql = re.sub(
            r"date\('now',\s*'-(\d+)\s*days?'\s*(?:,\s*'localtime')?\)",
            r"(CURRENT_DATE - INTERVAL '\1 days')::date::text",
            sql
        )
        # Convert SQLite date(col, '-N days') to Postgres
        sql = re.sub(
            r"date\((\w+),\s*'(-?\d+)\s*days?'\)",
            r"((\1::date + INTERVAL '\2 days')::date::text)",
            sql
        )
        sql = re.sub(
            r"date\((\w+\.?\w*),\s*'-(\d+)\s*days?'\)",
            r"((\1::date - INTERVAL '\2 days')::date::text)",
            sql
        )
        # IFNULL → COALESCE
        sql = sql.replace("IFNULL(", "COALESCE(")
        # excluded.col → EXCLUDED.col (case-sensitive in Postgres)
        # Actually both work in Postgres, no change needed

        try:
            self._cursor.execute(sql, params)
        except Exception:
            self._conn.rollback()
            raise
        return self

    def executescript(self, sql):
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                self.execute(stmt)

    def fetchone(self):
        row = self._cursor.fetchone()
        return dict(row) if row else None

    def fetchall(self):
        return [dict(r) for r in self._cursor.fetchall()]

    @property
    def lastrowid(self):
        return self._cursor.lastrowid

    @property
    def rowcount(self):
        return self._cursor.rowcount


@contextmanager
def get_db():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        wrapper = PgCursorWrapper(cursor, conn)
        yield wrapper
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        pool.putconn(conn)
