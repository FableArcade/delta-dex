"""Tests for pipeline.compute.ev_calculator."""
import pytest
from pipeline.compute.ev_calculator import compute_ev_for_set


def _seed_set(db, set_code="TEST"):
    """Insert a set, rarities, cards, and prices for testing."""
    db.execute("INSERT INTO sets (set_code, set_name) VALUES (?, ?)", (set_code, "Test Set"))
    return set_code


def _add_rarity(db, set_code, rarity_code, pull_rate, card_count=1):
    set_rarity = f"{set_code}_{rarity_code}"
    db.execute(
        "INSERT INTO rarities (set_rarity, set_code, rarity_code, rarity_name, card_count, pull_rate) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (set_rarity, set_code, rarity_code, rarity_code, card_count, pull_rate),
    )


def _add_card(db, card_id, set_code, rarity_code, raw_price, psa10_price=None, date="2026-04-11"):
    db.execute(
        "INSERT INTO cards (id, product_name, set_code, rarity_code, rarity_name, set_value_include) "
        "VALUES (?, ?, ?, ?, ?, 'Y')",
        (card_id, f"Card {card_id}", set_code, rarity_code, rarity_code),
    )
    db.execute(
        "INSERT INTO price_history (card_id, date, raw_price, psa_10_price) VALUES (?, ?, ?, ?)",
        (card_id, date, raw_price, psa10_price),
    )


class TestEVCalculator:
    def test_single_rarity_single_card(self, db):
        """One rarity bucket with one card — EV should be pull_rate * price."""
        sc = _seed_set(db)
        _add_rarity(db, sc, "SIR", 1 / 180)
        _add_card(db, "c1", sc, "SIR", 100.0, 500.0)

        result = compute_ev_for_set(db, sc, "2026-04-11")

        assert result["cards_counted"] == 1
        assert result["rarity_buckets"] == 1
        assert abs(result["ev_raw_per_pack"] - (1 / 180 * 100.0)) < 0.01
        assert abs(result["ev_psa_10_per_pack"] - (1 / 180 * 500.0)) < 0.01
        assert abs(result["total_set_raw_value"] - 100.0) < 0.01

    def test_multiple_cards_in_rarity_uses_mean(self, db):
        """Two cards in same rarity — EV uses arithmetic mean of prices."""
        sc = _seed_set(db)
        _add_rarity(db, sc, "UR", 1 / 36)
        _add_card(db, "c1", sc, "UR", 10.0, 50.0)
        _add_card(db, "c2", sc, "UR", 30.0, 150.0)

        result = compute_ev_for_set(db, sc, "2026-04-11")

        avg_raw = (10.0 + 30.0) / 2  # = 20.0
        avg_psa10 = (50.0 + 150.0) / 2  # = 100.0
        assert abs(result["ev_raw_per_pack"] - (1 / 36 * avg_raw)) < 0.01
        assert abs(result["ev_psa_10_per_pack"] - (1 / 36 * avg_psa10)) < 0.01
        assert result["total_set_raw_value"] == pytest.approx(40.0, abs=0.01)

    def test_multiple_rarities_sum(self, db):
        """Two rarity buckets — EV is sum of both."""
        sc = _seed_set(db)
        _add_rarity(db, sc, "SIR", 1 / 180)
        _add_rarity(db, sc, "HR", 1 / 36)
        _add_card(db, "c1", sc, "SIR", 200.0, 800.0)
        _add_card(db, "c2", sc, "HR", 50.0, 200.0)

        result = compute_ev_for_set(db, sc, "2026-04-11")

        expected_ev_raw = (1 / 180 * 200.0) + (1 / 36 * 50.0)
        expected_ev_psa10 = (1 / 180 * 800.0) + (1 / 36 * 200.0)
        assert result["ev_raw_per_pack"] == pytest.approx(expected_ev_raw, abs=0.01)
        assert result["ev_psa_10_per_pack"] == pytest.approx(expected_ev_psa10, abs=0.01)
        assert result["rarity_buckets"] == 2
        assert result["cards_counted"] == 2

    def test_no_prices_returns_zeros(self, db):
        """No price data — should return zero EV."""
        sc = _seed_set(db)
        _add_rarity(db, sc, "SIR", 1 / 180)
        # No cards or prices inserted

        result = compute_ev_for_set(db, sc, "2026-04-11")

        assert result["ev_raw_per_pack"] == 0.0
        assert result["ev_psa_10_per_pack"] == 0.0
        assert result["cards_counted"] == 0

    def test_missing_psa10_still_computes_raw(self, db):
        """Card has raw price but no PSA 10 — raw EV should still compute."""
        sc = _seed_set(db)
        _add_rarity(db, sc, "UR", 1 / 36)
        _add_card(db, "c1", sc, "UR", 25.0, None)

        result = compute_ev_for_set(db, sc, "2026-04-11")

        assert result["ev_raw_per_pack"] == pytest.approx(1 / 36 * 25.0, abs=0.01)
        # PSA 10 EV should be 0 (no PSA 10 prices contributed)
        assert result["ev_psa_10_per_pack"] == 0.0

    def test_excluded_cards_not_counted(self, db):
        """Cards with set_value_include='N' should be skipped."""
        sc = _seed_set(db)
        _add_rarity(db, sc, "C", 1.0)
        # Add an included card
        _add_card(db, "c1", sc, "C", 1.0)
        # Add an excluded card directly
        db.execute(
            "INSERT INTO cards (id, product_name, set_code, rarity_code, rarity_name, set_value_include) "
            "VALUES (?, ?, ?, ?, ?, 'N')",
            ("c_excluded", "Excluded Card", sc, "C", "C"),
        )
        db.execute(
            "INSERT INTO price_history (card_id, date, raw_price) VALUES (?, ?, ?)",
            ("c_excluded", "2026-04-11", 999.0),
        )

        result = compute_ev_for_set(db, sc, "2026-04-11")

        assert result["cards_counted"] == 1
        assert result["total_set_raw_value"] == pytest.approx(1.0, abs=0.01)

    def test_uses_latest_price_before_date(self, db):
        """Should use the most recent price on or before the target date."""
        sc = _seed_set(db)
        _add_rarity(db, sc, "UR", 1 / 36)
        db.execute(
            "INSERT INTO cards (id, product_name, set_code, rarity_code, rarity_name, set_value_include) "
            "VALUES (?, ?, ?, ?, ?, 'Y')",
            ("c1", "Card c1", sc, "UR", "UR"),
        )
        # Old price
        db.execute(
            "INSERT INTO price_history (card_id, date, raw_price) VALUES (?, ?, ?)",
            ("c1", "2026-04-01", 10.0),
        )
        # Newer price (but before target date)
        db.execute(
            "INSERT INTO price_history (card_id, date, raw_price) VALUES (?, ?, ?)",
            ("c1", "2026-04-10", 20.0),
        )
        # Future price (after target date — should be ignored)
        db.execute(
            "INSERT INTO price_history (card_id, date, raw_price) VALUES (?, ?, ?)",
            ("c1", "2026-04-15", 50.0),
        )

        result = compute_ev_for_set(db, sc, "2026-04-11")

        assert result["ev_raw_per_pack"] == pytest.approx(1 / 36 * 20.0, abs=0.01)
