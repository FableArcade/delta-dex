"""Tests for pipeline.compute.pack_cost."""
import pytest
from pipeline.compute.pack_cost import compute_pack_cost, BUNDLE_PACK_COUNT


def _seed_set(db, set_code="TEST"):
    db.execute("INSERT INTO sets (set_code, set_name) VALUES (?, ?)", (set_code, "Test Set"))
    return set_code


def _add_sealed(db, card_id, set_code, sealed_type, price, date="2026-04-11"):
    db.execute(
        "INSERT INTO cards (id, product_name, set_code, sealed_product, sealed_type, set_value_include) "
        "VALUES (?, ?, ?, 'Y', ?, 'Y')",
        (card_id, f"Sealed {card_id}", set_code, sealed_type),
    )
    db.execute(
        "INSERT INTO price_history (card_id, date, raw_price) VALUES (?, ?, ?)",
        (card_id, date, price),
    )


class TestPackCost:
    def test_booster_only(self, db):
        """Single booster pack — avg_pack_cost equals its price."""
        sc = _seed_set(db)
        _add_sealed(db, "bp1", sc, "Booster Pack", 4.50)

        result = compute_pack_cost(db, sc, "2026-04-11")

        assert result["avg_booster_pack"] == pytest.approx(4.50, abs=0.01)
        assert result["avg_pack_cost"] == pytest.approx(4.50, abs=0.01)

    def test_bundle_divides_by_pack_count(self, db):
        """Bundle price should be divided by BUNDLE_PACK_COUNT (6)."""
        sc = _seed_set(db)
        _add_sealed(db, "bb1", sc, "Booster Bundle", 30.00)

        result = compute_pack_cost(db, sc, "2026-04-11")

        expected = 30.00 / BUNDLE_PACK_COUNT  # 5.00
        assert result["avg_booster_bundle_per_pack"] == pytest.approx(expected, abs=0.01)
        assert result["avg_pack_cost"] == pytest.approx(expected, abs=0.01)

    def test_all_three_types_averaged(self, db):
        """All three types — avg_pack_cost is mean of per-pack costs."""
        sc = _seed_set(db)
        _add_sealed(db, "bp1", sc, "Booster Pack", 4.00)
        _add_sealed(db, "sl1", sc, "Sleeved Booster Pack", 6.00)
        _add_sealed(db, "bb1", sc, "Booster Bundle", 30.00)  # = 5.00/pack

        result = compute_pack_cost(db, sc, "2026-04-11")

        expected = (4.00 + 6.00 + 5.00) / 3
        assert result["avg_pack_cost"] == pytest.approx(expected, abs=0.01)

    def test_no_sealed_returns_none(self, db):
        """No sealed products — all values should be None."""
        sc = _seed_set(db)

        result = compute_pack_cost(db, sc, "2026-04-11")

        assert result["avg_pack_cost"] is None
        assert result["avg_booster_pack"] is None

    def test_multiple_boosters_averaged(self, db):
        """Multiple booster pack entries — should average their prices."""
        sc = _seed_set(db)
        _add_sealed(db, "bp1", sc, "Booster Pack", 4.00)
        _add_sealed(db, "bp2", sc, "Booster Pack", 5.00)

        result = compute_pack_cost(db, sc, "2026-04-11")

        assert result["avg_booster_pack"] == pytest.approx(4.50, abs=0.01)

    def test_gain_loss_computed_when_ev_exists(self, db):
        """When set_daily has EV, pack_cost should update avg_gain_loss."""
        sc = _seed_set(db)
        # Pre-seed set_daily with EV
        db.execute(
            "INSERT INTO set_daily (set_code, date, ev_raw_per_pack) VALUES (?, ?, ?)",
            (sc, "2026-04-11", 3.50),
        )
        _add_sealed(db, "bp1", sc, "Booster Pack", 4.00)

        compute_pack_cost(db, sc, "2026-04-11")

        row = db.execute(
            "SELECT avg_gain_loss FROM set_daily WHERE set_code = ? AND date = ?",
            (sc, "2026-04-11"),
        ).fetchone()

        assert row["avg_gain_loss"] == pytest.approx(3.50 - 4.00, abs=0.01)
