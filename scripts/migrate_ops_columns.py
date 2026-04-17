"""One-shot migration: add ops columns to pipeline_runs.

Adds ONLY:
  * pipeline_runs.scraper_completion_json TEXT
    JSON blob tracking per-source scrape completion:
    {"pricecharting": {"expected": 8535, "processed": 915, "pct": 10.7}, ...}

Idempotent: checks existing columns before ALTER. Owned by the ops agent;
does NOT touch columns owned by model-infra.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection import get_db  # noqa: E402


ADDITIONS = [
    ("pipeline_runs", "scraper_completion_json", "TEXT"),
]


def _existing_columns(db, table: str) -> set[str]:
    rows = db.execute(f"PRAGMA table_info({table})").fetchall()
    return {r["name"] for r in rows}


def migrate() -> int:
    added = 0
    skipped = 0
    with get_db() as db:
        for table, col, coltype in ADDITIONS:
            cols = _existing_columns(db, table)
            if col in cols:
                print(f"  [skip] {table}.{col} already present")
                skipped += 1
                continue
            db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
            print(f"  [add]  {table}.{col} {coltype}")
            added += 1
    print(f"Migration complete: added={added} skipped={skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(migrate())
