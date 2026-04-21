"""FastAPI dependency for request-scoped database connections.

Auto-detects Postgres (DATABASE_URL) vs SQLite (local dev).
"""

import os

DATABASE_URL = os.environ.get("DATABASE_URL", "")

if DATABASE_URL and DATABASE_URL.startswith("postgres"):
    import psycopg2
    import psycopg2.extras
    import psycopg2.pool
    import re

    _pool = psycopg2.pool.ThreadedConnectionPool(1, 10, DATABASE_URL, connect_timeout=10)

    class _PgRow(dict):
        """Dict that also supports index access like sqlite3.Row."""
        def keys(self):
            return list(super().keys())

    def get_db_conn():
        conn = _pool.getconn()
        try:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            class _Wrapper:
                def __init__(self):
                    self._cursor = cursor
                    self._conn = conn

                def execute(self, sql, params=None):
                    sql = sql.replace("?", "%s")
                    sql = sql.replace("datetime('now')", "NOW()::text")
                    sql = sql.replace("date('now')", "CURRENT_DATE::text")
                    sql = sql.replace("date('now', 'localtime')", "CURRENT_DATE::text")
                    # date('now', '-N days') → text for comparison with TEXT date columns
                    sql = re.sub(r"date\('now',\s*'-(\d+)\s*days?'\s*(?:,\s*'localtime')?\)", r"(CURRENT_DATE - INTERVAL '\1 days')::date::text", sql)
                    sql = re.sub(r"date\('now',\s*'(-?\d+)\s*days?'\s*(?:,\s*'localtime')?\)", r"(CURRENT_DATE + INTERVAL '\1 days')::date::text", sql)
                    # date(column, '-N days') → text
                    sql = re.sub(r"date\((\w+\.?\w*),\s*'-(\d+)\s*days?'\)", r"((\1::date - INTERVAL '\2 days')::date::text)", sql)
                    sql = sql.replace("IFNULL(", "COALESCE(")
                    self._cursor.execute(sql, params)
                    return self

                def fetchone(self):
                    row = self._cursor.fetchone()
                    return dict(row) if row else None

                def fetchall(self):
                    return [dict(r) for r in self._cursor.fetchall()]

            wrapper = _Wrapper()
            yield wrapper
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            _pool.putconn(conn)

else:
    import sqlite3
    from config.settings import DB_PATH

    def get_db_conn():
        conn = sqlite3.connect(str(DB_PATH), timeout=30.0, check_same_thread=False)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 10000")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
