import sqlite3
from pathlib import Path

import pytest

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "db" / "schema.sql"


@pytest.fixture
def db():
    """In-memory SQLite database with the full schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())

    yield conn
    conn.close()
