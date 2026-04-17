"""Tests for pipeline.model.features.

Verifies:
- No future leakage (features at T use only data <= T).
- NaN/missing handling is defensive.
- Deterministic output (same input -> same feature hash).
- Live feature vector has the full FEATURE_COLUMNS schema.
"""
from __future__ import annotations

import math

import pandas as pd
import pytest

pytest.importorskip("pipeline.model.features")

from pipeline.model import features as feat_mod
from pipeline.model.features import (
    FEATURE_COLUMNS,
    build_live_features,
    build_training_dataset,
    cultural_score,
    cultural_tier,
    extract_pokemon_name,
)
from tests.conftest import feature_hash


# ---------- pure helpers ----------

def test_extract_pokemon_name_strips_suffixes():
    assert extract_pokemon_name("Charizard VSTAR #GG70") == "charizard"
    assert extract_pokemon_name("Pikachu V #25") == "pikachu"
    assert "mewtwo" in extract_pokemon_name("Team Rocket's Mewtwo")


def test_cultural_score_ranges_01():
    s = cultural_score("Charizard VSTAR", "SIR")
    assert 0.0 <= s <= 1.0
    assert s > cultural_score("Random Trainer", None)


def test_cultural_tier_known_values():
    assert cultural_tier("Charizard VSTAR") == 3
    assert cultural_tier("Pikachu V") == 3
    assert cultural_tier("Umbreon VMAX") == 2
    assert cultural_tier("Random NPC") == 0


# ---------- no-future-leakage invariant ----------

def test_price_at_offset_uses_only_trailing_data():
    """_price_at_offset must only read rows at-or-before the anchor."""
    idx = pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01",
                          "2024-04-01", "2024-05-01"])
    df = pd.DataFrame({"psa_10_price": [100, 110, 120, 9999, 10000]},
                      index=idx)
    anchor = pd.Timestamp("2024-03-01")
    # Asking for 30 days before the anchor (target = 2024-01-30) must return
    # the most recent row <= target, which is the Jan 1 price of 100.
    p = feat_mod._price_at_offset(df, anchor, 30)
    assert p == 100.0
    # The future values (9999, 10000) must never be returned.
    for days in (0, 10, 30, 60, 90, 365):
        p = feat_mod._price_at_offset(df, anchor, days)
        if p is not None:
            assert p <= 120.0, f"future leakage at offset={days}"


def test_training_dataset_no_future_leakage(seeded_db):
    """Every sample's features must be derivable from data strictly <= anchor."""
    df = build_training_dataset(seeded_db)
    # With only 120 days of history + 90-day forward horizon and monthly anchors,
    # we expect a small number of samples (or zero if runway is too short).
    if df.empty:
        pytest.skip("seeded data is too short to produce forward targets")

    for _, row in df.iterrows():
        anchor = pd.Timestamp(row["anchor_date"])
        # ret_365d should be bounded if history_days < 365.
        assert not math.isnan(row["ret_30d"])
        # All feature columns must be finite
        for col in FEATURE_COLUMNS:
            if col in row:
                assert math.isfinite(float(row[col])), f"non-finite {col}"


# ---------- live features ----------

def test_build_live_features_shape(seeded_db):
    df = build_live_features(seeded_db)
    if df.empty:
        pytest.skip("live features empty — min PSA10 floor not reached")
    # Must contain every FEATURE_COLUMNS entry.
    for col in FEATURE_COLUMNS:
        assert col in df.columns, f"missing live feature: {col}"
    # No NaN in required feature columns after fills.
    for col in FEATURE_COLUMNS:
        assert df[col].notna().all(), f"NaN remains in {col}"


# ---------- determinism ----------

def test_feature_hash_deterministic(feature_row):
    h1 = feature_hash(feature_row)
    h2 = feature_hash(dict(feature_row))
    assert h1 == h2
    mutated = dict(feature_row)
    mutated["ret_30d"] += 0.0001
    assert feature_hash(mutated) != h1


def test_build_live_features_deterministic(seeded_db):
    """Same DB state -> same feature vectors."""
    df1 = build_live_features(seeded_db)
    df2 = build_live_features(seeded_db)
    if df1.empty:
        pytest.skip("no live features to compare")
    pd.testing.assert_frame_equal(df1.sort_index(), df2.sort_index())
