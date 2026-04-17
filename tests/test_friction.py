"""Tests for pipeline.model.friction.

Friction is load-bearing: the entire training target is net_realized_return.
If this math breaks silently, every projection we show investors is wrong.
"""
from __future__ import annotations

import math

import pytest

from pipeline.model.friction import (
    EBAY_FVF,
    NET_SALE_FACTOR,
    SHIPPING_COST,
    net_realized_return,
)


def test_constants_sanity():
    assert 0.10 <= EBAY_FVF <= 0.15
    assert SHIPPING_COST > 0
    assert math.isclose(NET_SALE_FACTOR, 1.0 - EBAY_FVF)


def test_flat_price_is_negative_due_to_fees():
    """Buying and selling at the same price loses money to fees+shipping."""
    r = net_realized_return(100.0, 100.0)
    # Expected net: 100*0.87 - 5 - 100 = -18 -> -0.18
    assert r < 0
    assert math.isclose(r, (100 * NET_SALE_FACTOR - SHIPPING_COST - 100) / 100)


def test_break_even_requires_markup():
    """Break-even sell price on a $100 buy must exceed $100 by fees + ship."""
    # solve: sell * 0.87 - 5 = 100  => sell ~= 120.69
    break_even = (100.0 + SHIPPING_COST) / NET_SALE_FACTOR
    r = net_realized_return(100.0, break_even)
    assert abs(r) < 1e-9


def test_positive_return_on_big_gain():
    r = net_realized_return(100.0, 300.0)
    # 300*0.87 - 5 - 100 = 156 -> 1.56
    assert r > 1.0


def test_invalid_buy_returns_zero():
    assert net_realized_return(0.0, 100.0) == 0.0
    assert net_realized_return(-1.0, 100.0) == 0.0
    assert net_realized_return(None, 100.0) == 0.0  # type: ignore[arg-type]


def test_invalid_sell_returns_negative_one():
    assert net_realized_return(100.0, 0.0) == -1.0
    assert net_realized_return(100.0, None) == -1.0  # type: ignore[arg-type]


@pytest.mark.parametrize("buy", [20, 50, 100, 250, 1000])
def test_monotonic_in_sell_price(buy):
    """Return is strictly monotonic increasing in sell price."""
    prev = -1e9
    for sell in [buy * k for k in (0.5, 0.9, 1.0, 1.1, 1.5, 2.0, 3.0)]:
        r = net_realized_return(buy, sell)
        assert r > prev, f"not monotonic at buy={buy} sell={sell}"
        prev = r


@pytest.mark.parametrize("sell", [50, 150, 500, 2000])
def test_monotonic_decreasing_in_buy_price(sell):
    """For fixed sell price, higher buy price yields lower return."""
    prev = 1e9
    for buy in [10, 25, 50, 100, 250, 500]:
        r = net_realized_return(buy, sell)
        assert r < prev, f"not monotonic dec at buy={buy} sell={sell}"
        prev = r


def test_shipping_hurts_cheap_cards_more():
    """A $5 flat shipping cost is a larger % drag on cheap cards."""
    # Hold gross gain at 50%, compare net returns at different price levels.
    r_cheap = net_realized_return(20.0, 30.0)
    r_exp = net_realized_return(1000.0, 1500.0)
    assert r_exp > r_cheap
