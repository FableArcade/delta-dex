"""Card endpoints: index, detail, and search."""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from api.deps import get_db_conn

router = APIRouter()


def _neg(v):
    """Negate a value, preserving None.

    Pipeline convention: net_flow = avg_new - avg_ended, so positive means
    supply is BUILDING (bearish). The frontend uses the financial convention
    that positive = bullish, so we flip the sign at the API boundary. After
    this, positive net_flow means inventory is being absorbed (bullish) and
    negative means supply is accumulating (bearish).
    """
    return None if v is None else -v


def _card_summary(r) -> dict:
    """Convert a card + latest price row to kebab-case dict."""
    keys = set(r.keys())
    def g(k):
        return r[k] if k in keys else None
    return {
        "id": r["id"],
        "product-name": r["product_name"],
        "set-code": r["set_code"],
        "card-number": r["card_number"],
        "set-count": r["set_count"],
        "card-unique": r["card_unique"],
        "rarity-code": r["rarity_code"],
        "rarity-name": r["rarity_name"],
        "tcg-id": r["tcg_id"],
        "image-url": r["image_url"],
        # Product classification (sealed vs single)
        "is-sealed": (g("sealed_product") == "Y"),
        "sealed-type": g("sealed_type"),
        # Current prices
        "raw-price": g("raw_price"),
        "psa-10-price": g("psa_10_price"),
        "psa-9-price": g("psa_9_price"),
        "psa-8-price": g("psa_8_price"),
        "psa-7-price": g("psa_7_price"),
        "psa-10-vs-raw": g("psa_10_vs_raw"),
        "psa-10-vs-raw-pct": g("psa_10_vs_raw_pct"),
        # Opportunity-finder signals — PSA population + market pressure
        "gem-pct":                 g("gem_pct"),
        "psa-10-pop":              g("psa_10_pop"),
        "psa-total-pop":           g("psa_total_pop"),
        "supply-saturation-index":      g("supply_saturation_index"),
        "supply-saturation-label":      g("supply_saturation_label"),
        "supply-saturation-trend":      g("supply_saturation_trend"),
        "active-listings-delta-pct":    g("active_listings_delta_pct"),
        "net-flow":                     _neg(g("net_flow")),
        "net-flow-7d":                  _neg(g("net_flow_7d")),
        "net-flow-30d":                 _neg(g("net_flow_30d")),
        "net-flow-pct":                 _neg(g("net_flow_pct")),
        "net-flow-pct-7d":              _neg(g("net_flow_pct_7d")),
        "net-flow-pct-30d":             _neg(g("net_flow_pct_30d")),
        "demand-pressure":              g("demand_pressure"),
        "demand-pressure-7d":           g("demand_pressure_7d"),
        "supply-pressure":              g("supply_pressure"),
        # Long-term-hold history aggregates (from price_history over last 12 months).
        # Each "N days ago" field is the closest observation <= that threshold date.
        "raw-30d-ago":   g("raw_30d_ago"),
        "raw-90d-ago":   g("raw_90d_ago"),
        "raw-365d-ago":  g("raw_365d_ago"),
        "raw-max-1y":    g("raw_max_1y"),
        "raw-min-1y":    g("raw_min_1y"),
        "psa10-30d-ago":  g("psa10_30d_ago"),
        "psa10-90d-ago":  g("psa10_90d_ago"),
        "psa10-365d-ago": g("psa10_365d_ago"),
        "psa10-max-1y":   g("psa10_max_1y"),
        "psa10-min-1y":   g("psa10_min_1y"),
        "history-days":   g("history_days"),  # distinct dates over the full history
    }


@router.get("/card_index")
def card_index(db=Depends(get_db_conn)):
    """All cards (singles + sealed) with latest prices, opportunity-finder
    signals, and 1-year history aggregates for long-term-hold scoring.

    Joins in:
      * latest price_history row (raw / PSA prices)
      * latest psa_pop_history row (gem_pct, psa_10_base, total_base)
      * latest 30-day observed market_pressure row (net_flow)
      * latest supply_saturation row (saturation index + label)
      * 1-year price_history aggregates (30/90/365-day-ago snapshots,
        12-month max & min for both raw and PSA 10) — used to compute
        momentum × discount-from-peak for long-term hold picks.
    """
    # Use parametrised "today" so tests can freeze the date if needed.
    # (sqlite's `date('now')` is UTC — good enough for daily granularity.)
    rows = db.execute("""
        SELECT
            c.id, c.product_name, c.set_code, c.card_number,
            c.set_count, c.card_unique, c.rarity_code, c.rarity_name,
            c.tcg_id, c.image_url,
            c.sealed_product, c.sealed_type,

            ph.raw_price, ph.psa_10_price, ph.psa_9_price,
            ph.psa_8_price, ph.psa_7_price,
            ph.psa_10_vs_raw, ph.psa_10_vs_raw_pct,

            pp.gem_pct,
            pp.psa_10_base    AS psa_10_pop,
            pp.total_base     AS psa_total_pop,

            mp.net_flow,
            mp.net_flow_pct,
            mp.demand_pressure,
            mp.supply_pressure,
            mp.net_flow                     AS net_flow_30d,
            mp.net_flow_pct                 AS net_flow_pct_30d,
            mp7.net_flow                    AS net_flow_7d,
            mp7.net_flow_pct                AS net_flow_pct_7d,
            mp7.demand_pressure             AS demand_pressure_7d,
            ss.supply_saturation_index,
            ss.supply_saturation_label,
            ss.trend                        AS supply_saturation_trend,
            ss.active_listings_delta_pct,

            (SELECT raw_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-30 days')
                AND raw_price IS NOT NULL
              ORDER BY date DESC LIMIT 1)                       AS raw_30d_ago,
            (SELECT raw_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-90 days')
                AND raw_price IS NOT NULL
              ORDER BY date DESC LIMIT 1)                       AS raw_90d_ago,
            (SELECT raw_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-365 days')
                AND raw_price IS NOT NULL
              ORDER BY date DESC LIMIT 1)                       AS raw_365d_ago,
            (SELECT MAX(raw_price) FROM price_history
              WHERE card_id = c.id AND date >= date('now', '-365 days'))
                                                                 AS raw_max_1y,
            (SELECT MIN(raw_price) FROM price_history
              WHERE card_id = c.id AND date >= date('now', '-365 days')
                AND raw_price > 0)                               AS raw_min_1y,

            (SELECT psa_10_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-30 days')
                AND psa_10_price IS NOT NULL
              ORDER BY date DESC LIMIT 1)                       AS psa10_30d_ago,
            (SELECT psa_10_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-90 days')
                AND psa_10_price IS NOT NULL
              ORDER BY date DESC LIMIT 1)                       AS psa10_90d_ago,
            (SELECT psa_10_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-365 days')
                AND psa_10_price IS NOT NULL
              ORDER BY date DESC LIMIT 1)                       AS psa10_365d_ago,
            (SELECT MAX(psa_10_price) FROM price_history
              WHERE card_id = c.id AND date >= date('now', '-365 days'))
                                                                 AS psa10_max_1y,
            (SELECT MIN(psa_10_price) FROM price_history
              WHERE card_id = c.id AND date >= date('now', '-365 days')
                AND psa_10_price > 0)                            AS psa10_min_1y,

            (SELECT COUNT(DISTINCT date) FROM price_history
              WHERE card_id = c.id AND date >= date('now', '-365 days'))
                                                                 AS history_days

        FROM cards c
        LEFT JOIN price_history ph
            ON ph.card_id = c.id
            AND ph.date = (SELECT MAX(date) FROM price_history WHERE card_id = c.id)
        LEFT JOIN psa_pop_history pp
            ON pp.card_id = c.id
            AND pp.date = (SELECT MAX(date) FROM psa_pop_history WHERE card_id = c.id)
        LEFT JOIN market_pressure mp
            ON mp.card_id = c.id
            AND mp.window_days = 30
            AND mp.mode = 'observed'
            AND mp.as_of = (
                SELECT MAX(as_of) FROM market_pressure
                WHERE card_id = c.id AND window_days = 30 AND mode = 'observed'
            )
        LEFT JOIN market_pressure mp7
            ON mp7.card_id = c.id
            AND mp7.window_days = 7
            AND mp7.mode = 'observed'
            AND mp7.as_of = (
                SELECT MAX(as_of) FROM market_pressure
                WHERE card_id = c.id AND window_days = 7 AND mode = 'observed'
            )
        LEFT JOIN supply_saturation ss
            ON ss.card_id = c.id
            AND ss.mode = 'observed'
            AND ss.as_of = (
                SELECT MAX(as_of) FROM supply_saturation
                WHERE card_id = c.id AND mode = 'observed'
            )
        ORDER BY c.set_code, c.card_number
    """).fetchall()

    return {"cards": [_card_summary(r) for r in rows]}


@router.get("/card/{card_id}")
def card_detail(card_id: str, include: Optional[str] = None, db=Depends(get_db_conn)):
    """Full card detail with all history arrays."""

    card = db.execute("SELECT * FROM cards WHERE id = ?", (card_id,)).fetchone()
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")

    # Latest price
    latest_price = db.execute("""
        SELECT * FROM price_history
        WHERE card_id = ? ORDER BY date DESC LIMIT 1
    """, (card_id,)).fetchone()

    result = {
        "id": card["id"],
        "product-name": card["product_name"],
        "set-code": card["set_code"],
        "card-number": card["card_number"],
        "set-count": card["set_count"],
        "card-unique": card["card_unique"],
        "rarity-code": card["rarity_code"],
        "rarity-name": card["rarity_name"],
        "tcg-id": card["tcg_id"],
        "image-url": card["image_url"],
        "tcgplayer-image-url": card["tcgplayer_image_url"],
        "set-value-include": card["set_value_include"],
        "ebay-q-phrase": card["ebay_q_phrase"],
        "ebay-q-num": card["ebay_q_num"],
        "ebay-category-id": card["ebay_category_id"],
        # Sealed product flags so the frontend can swap UI accordingly.
        "sealed-product": card["sealed_product"],
        "sealed-type": card["sealed_type"],
        "is-sealed": (card["sealed_product"] == "Y"),
    }

    if latest_price:
        result.update({
            "raw-price": latest_price["raw_price"],
            "psa-7-price": latest_price["psa_7_price"],
            "psa-8-price": latest_price["psa_8_price"],
            "psa-9-price": latest_price["psa_9_price"],
            "psa-10-price": latest_price["psa_10_price"],
            "psa-10-vs-raw": latest_price["psa_10_vs_raw"],
            "psa-10-vs-raw-pct": latest_price["psa_10_vs_raw_pct"],
            "sales-volume": latest_price["sales_volume"],
        })

    # --- history: price_history ---
    ph_rows = db.execute("""
        SELECT date, raw_price, psa_7_price, psa_8_price, psa_9_price,
               psa_10_price, psa_10_vs_raw, psa_10_vs_raw_pct,
               sales_volume, interpolated
        FROM price_history WHERE card_id = ? ORDER BY date ASC
    """, (card_id,)).fetchall()
    result["history"] = [
        {
            "date": r["date"],
            "raw-price": r["raw_price"],
            "psa-7-price": r["psa_7_price"],
            "psa-8-price": r["psa_8_price"],
            "psa-9-price": r["psa_9_price"],
            "psa-10-price": r["psa_10_price"],
            "psa-10-vs-raw": r["psa_10_vs_raw"],
            "psa-10-vs-raw-pct": r["psa_10_vs_raw_pct"],
            "sales-volume": r["sales_volume"],
            "interpolated": r["interpolated"],
        }
        for r in ph_rows
    ]

    # --- history-psa: psa_pop_history ---
    psa_rows = db.execute("""
        SELECT date, psa_8_base, psa_9_base, psa_10_base,
               total_base, gem_pct
        FROM psa_pop_history WHERE card_id = ? ORDER BY date ASC
    """, (card_id,)).fetchall()
    result["history-psa"] = [
        {
            "date": r["date"],
            "psa-8-base": r["psa_8_base"],
            "psa-9-base": r["psa_9_base"],
            "psa-10-base": r["psa_10_base"],
            "total-base": r["total_base"],
            "gem-pct": r["gem_pct"],
        }
        for r in psa_rows
    ]

    # --- eBay-related histories (only when include=ebay) ---
    include_ebay = include and "ebay" in include.lower()

    if include_ebay:
        # history-ebay
        ebay_rows = db.execute("""
            SELECT date, from_date, active_from, active_to, ended, new,
                   ended_rate, ended_raw, new_raw, ended_graded, new_graded,
                   ended_psa_10, new_psa_10, ended_psa_9, new_psa_9,
                   ended_other_10, new_other_10,
                   ended_avg_raw_price, ended_avg_psa_10_price,
                   ended_avg_psa_9_price, ended_avg_other_10_price,
                   interpolated,
                   ended_adj, ended_raw_adj, ended_graded_adj,
                   new_adj, new_raw_adj, new_graded_adj,
                   ended_avg_raw_price_adj, ended_avg_psa_10_price_adj,
                   ended_avg_psa_9_price_adj
            FROM ebay_history WHERE card_id = ? ORDER BY date ASC
        """, (card_id,)).fetchall()
        result["history-ebay"] = [
            {
                "date": r["date"],
                "from-date": r["from_date"],
                "active-from": r["active_from"],
                "active-to": r["active_to"],
                "ended": r["ended"],
                "new": r["new"],
                "ended-rate": r["ended_rate"],
                "ended-raw": r["ended_raw"],
                "new-raw": r["new_raw"],
                "ended-graded": r["ended_graded"],
                "new-graded": r["new_graded"],
                "ended-psa-10": r["ended_psa_10"],
                "new-psa-10": r["new_psa_10"],
                "ended-psa-9": r["ended_psa_9"],
                "new-psa-9": r["new_psa_9"],
                "ended-other-10": r["ended_other_10"],
                "new-other-10": r["new_other_10"],
                "ended-avg-raw-price": r["ended_avg_raw_price"],
                "ended-avg-psa-10-price": r["ended_avg_psa_10_price"],
                "ended-avg-psa-9-price": r["ended_avg_psa_9_price"],
                "ended-avg-other-10-price": r["ended_avg_other_10_price"],
                "interpolated": r["interpolated"],
                "ended-adj": r["ended_adj"],
                "ended-raw-adj": r["ended_raw_adj"],
                "ended-graded-adj": r["ended_graded_adj"],
                "new-adj": r["new_adj"],
                "new-raw-adj": r["new_raw_adj"],
                "new-graded-adj": r["new_graded_adj"],
                "ended-avg-raw-price-adj": r["ended_avg_raw_price_adj"],
                "ended-avg-psa-10-price-adj": r["ended_avg_psa_10_price_adj"],
                "ended-avg-psa-9-price-adj": r["ended_avg_psa_9_price_adj"],
            }
            for r in ebay_rows
        ]

        # history-ebay-market
        emkt_rows = db.execute("""
            SELECT date, from_date, active_from, active_to, ended, new,
                   ended_raw, ended_psa_9, ended_psa_10, interpolated,
                   demand_pressure_observed, demand_pressure_est,
                   sold_rate_est, sold_est
            FROM ebay_market_history WHERE card_id = ? ORDER BY date ASC
        """, (card_id,)).fetchall()
        result["history-ebay-market"] = [
            {
                "date": r["date"],
                "from-date": r["from_date"],
                "active-from": r["active_from"],
                "active-to": r["active_to"],
                "ended": r["ended"],
                "new": r["new"],
                "ended-raw": r["ended_raw"],
                "ended-psa-9": r["ended_psa_9"],
                "ended-psa-10": r["ended_psa_10"],
                "interpolated": r["interpolated"],
                "demand-pressure-observed": r["demand_pressure_observed"],
                "demand-pressure-est": r["demand_pressure_est"],
                "sold-rate-est": r["sold_rate_est"],
                "sold-est": r["sold_est"],
            }
            for r in emkt_rows
        ]

        # history-ebay-derived
        eder_rows = db.execute("""
            SELECT date, d_raw_price, d_psa_9_price, d_psa_10_price
            FROM ebay_derived_history WHERE card_id = ? ORDER BY date ASC
        """, (card_id,)).fetchall()
        result["history-ebay-derived"] = [
            {
                "date": r["date"],
                "d-raw-price": r["d_raw_price"],
                "d-psa-9-price": r["d_psa_9_price"],
                "d-psa-10-price": r["d_psa_10_price"],
            }
            for r in eder_rows
        ]
    else:
        result["history-ebay"] = []
        result["history-ebay-market"] = []
        result["history-ebay-derived"] = []

    # --- history-justtcg ---
    jtcg_rows = db.execute("""
        SELECT date, j_raw_price
        FROM justtcg_history WHERE card_id = ? ORDER BY date ASC
    """, (card_id,)).fetchall()
    result["history-justtcg"] = [
        {"date": r["date"], "j-raw-price": r["j_raw_price"]}
        for r in jtcg_rows
    ]

    # --- history-collectrics (composite_history) ---
    comp_rows = db.execute("""
        SELECT date, c_raw_price, c_psa_9_price, c_psa_10_price
        FROM composite_history WHERE card_id = ? ORDER BY date ASC
    """, (card_id,)).fetchall()
    result["history-collectrics"] = [
        {
            "date": r["date"],
            "c-raw-price": r["c_raw_price"],
            "c-psa-9-price": r["c_psa_9_price"],
            "c-psa-10-price": r["c_psa_10_price"],
        }
        for r in comp_rows
    ]

    # --- collectrics.market-pressure ---
    mp_rows = db.execute("""
        SELECT window_days, mode, as_of, sample_days, interpolated_days,
               avg_active, avg_existing, avg_ended, avg_new,
               demand_pressure, supply_pressure, net_flow, net_flow_pct,
               state_label
        FROM market_pressure WHERE card_id = ? ORDER BY window_days, mode, as_of
    """, (card_id,)).fetchall()

    ss_rows = db.execute("""
        SELECT mode, as_of, supply_saturation_index, supply_saturation_label,
               trend, active_listings_delta_pct, demand_delta_pct, supply_delta_pct
        FROM supply_saturation WHERE card_id = ? ORDER BY mode, as_of
    """, (card_id,)).fetchall()

    # Build nested market-pressure matching Collectrics format:
    # {observed: {7d: {...}, 30d: {...}, baseline-comparison: {...}}, estimated: {...}}
    mp_nested = {}
    for r in mp_rows:
        mode = r["mode"]
        window_key = f"{r['window_days']}d"
        if mode not in mp_nested:
            mp_nested[mode] = {}

        raw_key = "avg-sold-est" if mode == "estimated" else "avg-ended"
        dp_key = "demand-pressure-est" if mode == "estimated" else "demand-pressure"

        mp_nested[mode][window_key] = {
            "window-days": r["window_days"],
            "as-of": r["as_of"],
            "mode": mode,
            "sample-days": r["sample_days"],
            "interpolated-days": r["interpolated_days"],
            "raw": {
                "avg-active": r["avg_active"],
                "avg-existing": r["avg_existing"],
                raw_key: r["avg_ended"],
                "avg-new": r["avg_new"],
            },
            "metrics": {
                dp_key: r["demand_pressure"],
                "supply-pressure": r["supply_pressure"],
                "net-flow": _neg(r["net_flow"]),
                "net-flow-pct": _neg(r["net_flow_pct"]),
            },
            "labels": {
                "state": r["state_label"],
            },
        }

    for r in ss_rows:
        mode = r["mode"]
        if mode not in mp_nested:
            mp_nested[mode] = {}
        mp_nested[mode]["baseline-comparison"] = {
            "supply-saturation-index": r["supply_saturation_index"],
            "supply-saturation-label": r["supply_saturation_label"],
            "trend": r["trend"],
            "active-listings-delta-pct": r["active_listings_delta_pct"],
            "demand-delta-pct": r["demand_delta_pct"],
            "supply-delta-pct": r["supply_delta_pct"],
        }

    result["collectrics"] = {"market-pressure": mp_nested}

    return result


@router.get("/search/cards")
def search_cards(
    q: Optional[str] = None,
    setCode: Optional[str] = None,
    rarity: Optional[str] = None,
    sort: Optional[str] = None,
    limit: int = Query(default=50, le=500),
    db=Depends(get_db_conn),
):
    """Search cards with optional filters and sorting."""
    conditions = ["c.sealed_product = 'N'"]
    params = []

    if q:
        conditions.append("c.search_text LIKE ?")
        params.append(f"%{q.lower()}%")
    if setCode:
        conditions.append("c.set_code = ?")
        params.append(setCode)
    if rarity:
        conditions.append("c.rarity_name = ?")
        params.append(rarity)

    where = " AND ".join(conditions)

    # Sort mapping
    sort_map = {
        "raw_desc": "ph.raw_price DESC",
        "raw_asc": "ph.raw_price ASC",
        "psa10_desc": "ph.psa_10_price DESC",
        "psa10_asc": "ph.psa_10_price ASC",
        "name_asc": "c.product_name ASC",
        "name_desc": "c.product_name DESC",
        "number_asc": "c.card_number ASC",
    }
    order_clause = sort_map.get(sort, "c.set_code, c.card_number")

    rows = db.execute(f"""
        SELECT
            c.id, c.product_name, c.set_code, c.card_number,
            c.set_count, c.card_unique, c.rarity_code, c.rarity_name,
            c.tcg_id, c.image_url,
            ph.raw_price, ph.psa_10_price, ph.psa_9_price,
            ph.psa_8_price, ph.psa_7_price,
            ph.psa_10_vs_raw, ph.psa_10_vs_raw_pct
        FROM cards c
        LEFT JOIN price_history ph
            ON ph.card_id = c.id
            AND ph.date = (SELECT MAX(date) FROM price_history WHERE card_id = c.id)
        WHERE {where}
        ORDER BY {order_clause}
        LIMIT ?
    """, params + [limit]).fetchall()

    return {
        "total": len(rows),
        "results": [_card_summary(r) for r in rows],
    }


@router.get("/search/rarities")
def search_rarities(
    setCode: Optional[str] = None,
    db=Depends(get_db_conn),
):
    """Return distinct rarities, optionally filtered by set."""
    if setCode:
        rows = db.execute("""
            SELECT DISTINCT rarity_code, rarity_name
            FROM rarities
            WHERE set_code = ?
            ORDER BY rarity_code
        """, (setCode,)).fetchall()
    else:
        rows = db.execute("""
            SELECT DISTINCT rarity_code, rarity_name
            FROM rarities
            ORDER BY rarity_code
        """).fetchall()

    return {
        "rarities": [
            {"rarity-code": r["rarity_code"], "rarity-name": r["rarity_name"]}
            for r in rows
        ]
    }
