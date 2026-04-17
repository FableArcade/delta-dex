"""Transaction-cost model for realistic investment returns.

The v1 model optimizes gross price change. Real investors eat fees on
exit: eBay final-value fee (~13%), payment processing (~0.3% baked into
eBay now), plus shipping they absorb if not charging buyer. An optimistic
gross prediction of +30% can easily be +13% net — enough to flip a
"buy" into a "skip."

Target transform: realized_return = (sell_price * NET_SALE_FACTOR - buy_price) / buy_price

Buy_price assumed to be the ended-avg price (what real sales clear at),
not ask price, since investor with patience buys at that clearing level.
"""

from __future__ import annotations

# eBay final value fee for Trading Cards category (2025): 13.25% base.
# Using 13% as a round, conservative number. Does NOT include the fixed
# $0.40 per-order fee — for PSA 10 cards at $100+ that's <0.5% and gets
# noisy; omitted for modeling simplicity.
EBAY_FVF = 0.13

# Flat shipping the seller eats when they offer free shipping (common
# for PSA 10 cards to remain competitive). $5 covers BMWT + sleeve/toploader.
# This is a fixed dollar amount, so it hurts cheap cards proportionally more.
SHIPPING_COST = 5.0

# Composite multiplier: of every $1 sold, seller keeps this fraction
# BEFORE fixed shipping deduction.
NET_SALE_FACTOR = 1.0 - EBAY_FVF  # 0.87


def net_realized_return(buy_price: float, sell_price: float) -> float:
    """Compute net-of-cost return an investor actually realizes.

    buy_price: price paid to acquire (ended-avg at anchor)
    sell_price: price received at exit (ended-avg at forward date)

    Returns fractional return (0.15 = +15%).
    """
    if buy_price is None or buy_price <= 0:
        return 0.0
    if sell_price is None or sell_price <= 0:
        return -1.0
    net_proceeds = sell_price * NET_SALE_FACTOR - SHIPPING_COST
    return (net_proceeds - buy_price) / buy_price
