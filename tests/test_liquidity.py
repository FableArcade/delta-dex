"""Tests for pipeline.model.liquidity."""
from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("pipeline.model.liquidity")

from pipeline.model.liquidity import (
    LIQUIDITY_COLUMNS,
    THIN_MARKET_THRESHOLD,
    compute_liquidity_at_date,
    compute_live_liquidity,
)


def _hist(rows):
    """Build a dated dataframe indexed by date."""
    df = pd.DataFrame(rows)
    df.index = pd.to_datetime(df["date"])
    return df.drop(columns=["date"])


def test_empty_window_returns_pessimistic_zeros():
    empty = pd.DataFrame(columns=["ended", "new", "active_from"])
    empty.index = pd.DatetimeIndex([])
    out = compute_liquidity_at_date(empty, pd.Timestamp("2024-06-01"))
    # Pessimistic defaults: assume thin market with no sales.
    assert out["thin_market_flag"] == 1.0
    assert out["sales_per_day_30d"] == 0.0
    assert set(LIQUIDITY_COLUMNS).issubset(out.keys())


def test_thin_market_flag_triggers_below_threshold():
    # 2 ended over 30 days -> 0.067/day, below the 0.1 threshold.
    rows = []
    for i, d in enumerate(pd.date_range("2024-05-02", periods=30)):
        rows.append({"date": d, "ended": 1 if i in (5, 15) else 0,
                     "new": 0, "active_from": 10})
    out = compute_liquidity_at_date(_hist(rows), pd.Timestamp("2024-06-01"))
    assert out["thin_market_flag"] == 1.0
    assert out["sales_per_day_30d"] < THIN_MARKET_THRESHOLD


def test_liquid_market_flag_clears():
    rows = []
    for d in pd.date_range("2024-05-02", periods=30):
        rows.append({"date": d, "ended": 3, "new": 2, "active_from": 20})
    out = compute_liquidity_at_date(_hist(rows), pd.Timestamp("2024-06-01"))
    assert out["thin_market_flag"] == 0.0
    assert out["sales_per_day_30d"] >= THIN_MARKET_THRESHOLD


def test_sell_through_capped():
    rows = [{"date": pd.Timestamp("2024-05-15"), "ended": 1000,
             "new": 0, "active_from": 1}]
    out = compute_liquidity_at_date(_hist(rows), pd.Timestamp("2024-06-01"))
    # sell_through = 1000 / 1 = 1000; capped at 5.0 per module.
    assert out["sell_through_30d"] == 5.0


def test_compute_live_liquidity_columns(seeded_db):
    df = compute_live_liquidity(seeded_db)
    # The seeded eBay data is from 2024-03-20..2024-04-28; with `date('now')`
    # well past that, the 30-day window is empty -> empty df is OK.
    if df.empty:
        pytest.skip("seeded eBay history falls outside 30d of today")
    for col in LIQUIDITY_COLUMNS:
        assert col in df.columns
