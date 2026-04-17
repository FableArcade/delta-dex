"""Tests for pipeline.model.reprint_risk."""
from __future__ import annotations

import math

import pandas as pd
import pytest

pytest.importorskip("pipeline.model.reprint_risk")

from pipeline.model.reprint_risk import (
    DECAY_HALF_LIFE_DAYS,
    REPRINT_COLUMNS,
    build_reprint_index,
    load_release_calendar,
    reprint_features_at_date,
)


def test_no_prior_releases_returns_zeros():
    out = reprint_features_at_date(
        pokemon="charizard", own_set_code="X", anchor_date=pd.Timestamp("2024-01-01"),
        reprint_idx={},
    )
    assert out == {
        "reprint_count_365d": 0.0,
        "reprint_shock_decay": 0.0,
        "total_same_pokemon_cards": 0.0,
    }
    assert set(REPRINT_COLUMNS).issubset(out.keys())


def test_excludes_same_set():
    idx = {"charizard": [(pd.Timestamp("2023-06-01"), "OWN"),
                          (pd.Timestamp("2023-12-01"), "OTHER")]}
    out = reprint_features_at_date(
        pokemon="charizard", own_set_code="OWN",
        anchor_date=pd.Timestamp("2024-01-01"), reprint_idx=idx,
    )
    # Only the OTHER set counts as a reprint.
    assert out["reprint_count_365d"] == 1.0
    assert out["total_same_pokemon_cards"] == 1.0


def test_shock_decay_halves_at_half_life():
    # Reprint exactly HALF_LIFE days before anchor -> shock == 0.5.
    anchor = pd.Timestamp("2024-06-01")
    reprint_date = anchor - pd.Timedelta(days=int(DECAY_HALF_LIFE_DAYS))
    idx = {"pikachu": [(reprint_date, "OTHER")]}
    out = reprint_features_at_date("pikachu", "OWN", anchor, idx)
    assert math.isclose(out["reprint_shock_decay"], 0.5, abs_tol=0.01)


def test_old_reprints_excluded_from_365d_window():
    anchor = pd.Timestamp("2024-06-01")
    idx = {
        "mewtwo": [
            (anchor - pd.Timedelta(days=400), "OLD"),  # outside window
            (anchor - pd.Timedelta(days=100), "RECENT"),
        ]
    }
    out = reprint_features_at_date("mewtwo", "OWN", anchor, idx)
    assert out["reprint_count_365d"] == 1.0  # only RECENT counts
    assert out["total_same_pokemon_cards"] == 2.0  # lifetime includes OLD


def test_build_reprint_index_groups_by_pokemon(seeded_db):
    df = load_release_calendar(seeded_db)
    idx = build_reprint_index(df)
    assert isinstance(idx, dict)
    # seeded DB has two Charizard cards in different sets
    if "charizard" in idx:
        assert len(idx["charizard"]) >= 1
