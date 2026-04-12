"""FastAPI dependency for request-scoped database connections."""

import sqlite3
from config.settings import DB_PATH


def get_db_conn():
    """Yield a request-scoped SQLite connection with Row factory.

    `check_same_thread=False` is required because FastAPI dispatches sync
    dependencies and sync endpoints through a thread pool, and a single
    request can be handled across more than one pool thread (dependency
    setup, endpoint body, generator cleanup). The Python sqlite3 wrapper's
    default is to refuse any cross-thread access, which raises
    `ProgrammingError: SQLite objects created in a thread can only be used
    in that same thread`. We turn that safety off because each request gets
    its OWN connection here (never shared across requests), so there is no
    real concurrent access to a single connection — just sequential access
    from different worker threads within one request.

    `timeout=30.0` + `busy_timeout = 10000` then handles the OTHER kind of
    concurrency: the background bootstrap scraper holding a brief write
    lock when the API gets a request. Readers wait up to 10s for the writer
    to release instead of failing immediately.
    """
    conn = sqlite3.connect(
        str(DB_PATH),
        timeout=30.0,
        check_same_thread=False,
    )
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 10000")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()
