"""Shared pytest fixtures.

Tests must run headless without network. Use the in-memory ``db`` fixture
and the ``seeded_db`` fixture for tests that need synthetic price history.
"""
from __future__ import annotations

import hashlib
import random
import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "db" / "schema.sql"

# Make `import pipeline...` and `import scripts...` work without installing.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture
def db():
    """In-memory SQLite database with the full schema loaded."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())
    yield conn
    conn.close()


@pytest.fixture
def seeded_db(db):
    """In-memory DB seeded with 2 sets, 4 cards, ~100 rows of daily price_history.

    Deterministic: seeded RNG so results are reproducible.
    """
    rng = random.Random(1337)

    sets = [
        ("TEST1", "Test Set 1", "2022-01-01"),
        ("TEST2", "Test Set 2", "2024-01-01"),
    ]
    for set_code, name, release in sets:
        db.execute(
            "INSERT INTO sets (set_code, set_name, release_date) VALUES (?,?,?)",
            (set_code, name, release),
        )

    cards = [
        ("c-charizard-1", "Charizard VSTAR #18", "TEST1", "SIR"),
        ("c-pikachu-1", "Pikachu V #25", "TEST1", "RR"),
        ("c-umbreon-1", "Umbreon VMAX #215", "TEST2", "SIR"),
        ("c-charizard-2", "Charizard ex #199", "TEST2", "SCR"),
    ]
    for cid, name, set_code, rarity in cards:
        db.execute(
            "INSERT INTO cards (id, product_name, set_code, rarity_code, rarity_name, sealed_product) "
            "VALUES (?,?,?,?,?,?)",
            (cid, name, set_code, rarity, rarity, "N"),
        )

    # ~100 daily rows per card; monotonic-ish price walk starting from base.
    import datetime as dt

    start = dt.date(2024, 1, 1)
    for cid, _name, _sc, _r in cards:
        base = rng.uniform(50, 300)
        psa10 = base * 3
        for i in range(120):  # 120 days -> 480 rows total
            day = start + dt.timedelta(days=i)
            # Random-walk drift
            base *= 1 + rng.uniform(-0.02, 0.025)
            psa10 *= 1 + rng.uniform(-0.02, 0.025)
            db.execute(
                "INSERT INTO price_history (card_id, date, raw_price, psa_10_price, "
                "psa_10_vs_raw_pct, sales_volume) VALUES (?,?,?,?,?,?)",
                (cid, day.isoformat(), round(base, 2), round(psa10, 2),
                 (psa10 / base - 1) * 100, rng.randint(0, 30)),
            )
        # Seed PSA pop history
        db.execute(
            "INSERT INTO psa_pop_history (card_id, date, psa_8_base, psa_9_base, "
            "psa_10_base, total_base, gem_pct) VALUES (?,?,?,?,?,?,?)",
            (cid, "2024-05-01", 100, 500, 1000, 2000, 0.50),
        )
        # Seed eBay listing history (30 days back from 2024-04-30)
        for i in range(40):
            day = dt.date(2024, 3, 20) + dt.timedelta(days=i)
            db.execute(
                "INSERT INTO ebay_history (card_id, date, ended, new, active_from) "
                "VALUES (?,?,?,?,?)",
                (cid, day.isoformat(), rng.randint(0, 5),
                 rng.randint(0, 4), rng.randint(5, 40)),
            )

    db.commit()
    return db


@pytest.fixture
def feature_row():
    """A single deterministic feature row suitable for predict/SHAP tests."""
    return {
        "ret_30d": 0.05, "ret_90d": 0.10, "ret_365d": 0.25,
        "peak_discount": 0.10, "trough_recovery": 0.15, "volatility": 0.3,
        "ma_distance": 0.02, "log_price": 2.0, "net_flow_pct_7d": 0.1,
        "net_flow_pct_30d": 0.05, "demand_pressure_7d": 0.2,
        "demand_pressure_30d": 0.18, "supply_saturation_index": 1.1,
        "ds_ratio": 1.2, "gem_pct": 0.35, "psa_10_pop": 500.0,
        "psa_10_vs_raw_pct": 200.0, "cultural_score": 0.9,
        "rarity_tier": 0.0, "history_days": 300.0,
        "cultural_tier": 3.0, "pokemon_peak_log": 3.5,
        "pokemon_peak_ratio": 0.6,
        "sales_per_day_30d": 1.5, "sell_through_30d": 0.8,
        "ask_bid_proxy_30d": 2.0, "new_listings_per_day_30d": 1.0,
        "thin_market_flag": 0.0,
        "reprint_count_365d": 0.0, "reprint_shock_decay": 0.0,
        "total_same_pokemon_cards": 1.0,
    }


def feature_hash(row: dict) -> str:
    """Deterministic hash of a feature dict for reproducibility tests."""
    items = sorted((k, float(v)) for k, v in row.items() if k != "card_id")
    s = ",".join(f"{k}={v:.6f}" for k, v in items)
    return hashlib.sha256(s.encode()).hexdigest()
