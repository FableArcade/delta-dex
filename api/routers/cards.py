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
        "psa-pop-date":            g("psa_pop_date"),
        "psa-10-pop-prev":         g("psa_10_pop_prev"),
        "psa-pop-prev-date":       g("psa_pop_prev_date"),
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
        "psa10-ath":      g("psa10_ath"),
        "psa10-ath-date": g("psa10_ath_date"),
        "history-days":   g("history_days"),  # distinct dates over the full history
        # Set-alpha linkage — which card leads this one's moves, and how tightly
        "alpha-card-id":       g("alpha_card_id"),
        "alpha-name":          g("alpha_name"),
        "alpha-contemp-corr":  g("alpha_contemp_corr"),
        "alpha-lead-corr":     g("alpha_lead_corr"),
        "alpha-n-months":      g("alpha_n_months"),
        "alpha-psa10-current": g("alpha_psa10_current"),
        "alpha-psa10-30d-ago": g("alpha_psa10_30d_ago"),
        "alpha-psa10-90d-ago": g("alpha_psa10_90d_ago"),
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
            pp.date           AS psa_pop_date,
            (SELECT psa_10_base FROM psa_pop_history
              WHERE card_id = c.id
                AND date <= date(pp.date, '-21 days')
                AND psa_10_base IS NOT NULL
              ORDER BY date DESC LIMIT 1)                       AS psa_10_pop_prev,
            (SELECT date FROM psa_pop_history
              WHERE card_id = c.id
                AND date <= date(pp.date, '-21 days')
                AND psa_10_base IS NOT NULL
              ORDER BY date DESC LIMIT 1)                       AS psa_pop_prev_date,

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
                                                                 AS history_days,

            (SELECT MAX(psa_10_price) FROM price_history
              WHERE card_id = c.id AND psa_10_price IS NOT NULL)
                                                                 AS psa10_ath,
            (SELECT date FROM price_history
              WHERE card_id = c.id
                AND psa_10_price IS NOT NULL
              ORDER BY psa_10_price DESC, date DESC LIMIT 1)     AS psa10_ath_date,

            sal.alpha_card_id   AS alpha_card_id,
            sal.alpha_name      AS alpha_name,
            sal.contemp_corr    AS alpha_contemp_corr,
            sal.lead_corr       AS alpha_lead_corr,
            sal.n_months        AS alpha_n_months,
            (SELECT psa_10_price FROM price_history
              WHERE card_id = sal.alpha_card_id AND psa_10_price IS NOT NULL
              ORDER BY date DESC LIMIT 1)                        AS alpha_psa10_current,
            (SELECT psa_10_price FROM price_history
              WHERE card_id = sal.alpha_card_id
                AND date <= date('now', '-30 days')
                AND psa_10_price IS NOT NULL
              ORDER BY date DESC LIMIT 1)                        AS alpha_psa10_30d_ago,
            (SELECT psa_10_price FROM price_history
              WHERE card_id = sal.alpha_card_id
                AND date <= date('now', '-90 days')
                AND psa_10_price IS NOT NULL
              ORDER BY date DESC LIMIT 1)                        AS alpha_psa10_90d_ago

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
        LEFT JOIN set_alpha_linkage sal
            ON sal.card_id = c.id
        ORDER BY c.set_code, c.card_number
    """).fetchall()

    return {"cards": [_card_summary(r) for r in rows]}


@router.get("/card/{card_id}")
def card_detail(card_id: str, include: Optional[str] = None, db=Depends(get_db_conn)):
    """Full card detail with all history arrays."""

    card = db.execute("SELECT * FROM cards WHERE id = ?", (card_id,)).fetchone()
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")

    # Pull-rate / rarity grade — per-set rarity pull odds split across the
    # number of cards of that rarity in the set, combined with the gem rate
    # to produce a "PSA 10 odds per booster" grade.
    rarity_row = db.execute("""
        SELECT pull_rate, card_count, pull_rate_odds
          FROM rarities
         WHERE set_code = ? AND rarity_code = ?
    """, (card["set_code"], card["rarity_code"])).fetchone()
    gem_row = db.execute("""
        SELECT gem_pct, psa_10_base FROM psa_pop_history
         WHERE card_id = ?
      ORDER BY date DESC LIMIT 1
    """, (card_id,)).fetchone()

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

    # ---- Rarity grade block ----
    # Always emit the block so the UI can render consistently. Fields may be
    # null when coverage is missing; the UI degrades gracefully.
    if rarity_row and rarity_row["pull_rate"] and rarity_row["card_count"]:
        tier_pull = float(rarity_row["pull_rate"])
        tier_count = int(rarity_row["card_count"])
        specific_pull = tier_pull / tier_count if tier_count > 0 else None
    else:
        specific_pull = None
        tier_pull = None
        tier_count = None

    gem_pct = float(gem_row["gem_pct"]) if gem_row and gem_row["gem_pct"] is not None else None

    combined = (specific_pull * gem_pct) if (specific_pull and gem_pct) else None

    # Grade thresholds — tuned against modern SIR + HR distribution.
    # Preferred basis: combined PSA 10 odds (pull × gem). Fallback: specific
    # pull odds alone when gem rate is unknown (so newly-released chase cards
    # with no pop history yet still get a grade). Fallback basis uses a 4x
    # relaxation — i.e. the thresholds match what the card would land at
    # assuming a ~25% gem rate (typical for modern SIR).
    def grade_for_odds(odds):
        if odds is None:               return None
        if odds <= 1 / 10000:          return "S+"
        if odds <= 1 / 5000:           return "S"
        if odds <= 1 / 2500:           return "A"
        if odds <= 1 / 1000:           return "B"
        if odds <= 1 / 500:            return "C"
        return "D"

    def grade_for_pull_only(pull):
        # Pull-only: relax by 4x (assume a conservative 25% gem rate).
        if pull is None:               return None
        if pull <= 1 / 2500:           return "S+"
        if pull <= 1 / 1250:           return "S"
        if pull <= 1 / 625:            return "A"
        if pull <= 1 / 250:            return "B"
        if pull <= 1 / 125:            return "C"
        return "D"

    # Infer variant label from product name for cards with no rarity_code.
    # These are reverse holos, prize pack promos, stamped variants, etc. —
    # not chase cards, but we can still tell the user what they're looking at.
    variant_label = None
    if specific_pull is None:
        name = (card["product_name"] or "")
        lower = name.lower()
        # Match bracketed variants first — they're the most reliable signal
        import re as _re
        m = _re.search(r"\[([^\]]+)\]", name)
        if m:
            tag = m.group(1).strip().lower()
            # Map known tags to a user-friendly label
            tag_map = {
                "reverse holo": "Reverse Holo",
                "reverse":      "Reverse Holo",
                "cosmos holo":  "Cosmos Holo",
                "cosmos":       "Cosmos Holo",
                "master ball":  "Master Ball Promo",
                "poke ball":    "Poké Ball Promo",
                "stamped":      "Stamped Promo",
                "prize pack":   "Prize Pack Promo",
                "pokemon day 2025":            "Pokémon Day Promo",
                "cosmos professor program":    "Professor Program Promo",
                "ultra ball league":           "League Promo",
            }
            variant_label = tag_map.get(tag, m.group(1).strip())
        elif "holo" in lower:
            variant_label = "Holo"
        else:
            # Bare base card — no modifier, no rarity tier. Could be
            # Common / Uncommon / Rare; we don't have data to disambiguate
            # but "Base set" is accurate.
            variant_label = "Base Set"

    if combined is not None:
        grade = grade_for_odds(combined)
        grade_basis = "combined"
    elif specific_pull is not None:
        grade = grade_for_pull_only(specific_pull)
        grade_basis = "pull-only"
    else:
        # Non-chase card — no pull rate exists. Show the variant label in
        # place of a grade so the user knows what the card is.
        grade = "—"
        grade_basis = "variant"

    result["rarity-grade"] = {
        "tier-pull-rate":      tier_pull,
        "tier-pull-odds":      rarity_row["pull_rate_odds"] if rarity_row else None,
        "tier-card-count":     tier_count,
        "specific-pull-rate":  specific_pull,
        "gem-rate":            gem_pct,
        "combined-odds":       combined,
        "grade":               grade,
        "grade-basis":         grade_basis,  # combined | pull-only | variant
        "variant-label":       variant_label,
    }

    # Sales volume = eBay sold listings in the last 7 days (rolling). Computed
    # from ebay_history.ended (per-day ended-listing count); integer, null when
    # the card has no eBay history in the window (non-tracked or sealed).
    ath_row = db.execute(
        """
        SELECT MAX(psa_10_price) AS ath,
               (SELECT date FROM price_history
                WHERE card_id = ? AND psa_10_price = (
                  SELECT MAX(psa_10_price) FROM price_history
                  WHERE card_id = ? AND psa_10_price IS NOT NULL
                ) ORDER BY date DESC LIMIT 1) AS ath_date
        FROM price_history WHERE card_id = ? AND psa_10_price IS NOT NULL
        """,
        (card_id, card_id, card_id),
    ).fetchone()
    result["psa10-ath"] = ath_row["ath"] if ath_row else None
    result["psa10-ath-date"] = ath_row["ath_date"] if ath_row else None

    sv7_row = db.execute(
        """
        SELECT SUM(ended) AS s
        FROM ebay_history
        WHERE card_id = ? AND date >= date('now', '-7 days', 'localtime')
        """,
        (card_id,),
    ).fetchone()
    sales_volume_7d = int(sv7_row["s"]) if sv7_row and sv7_row["s"] is not None else None

    if latest_price:
        result.update({
            "raw-price": latest_price["raw_price"],
            "psa-7-price": latest_price["psa_7_price"],
            "psa-8-price": latest_price["psa_8_price"],
            "psa-9-price": latest_price["psa_9_price"],
            "psa-10-price": latest_price["psa_10_price"],
            "psa-10-vs-raw": latest_price["psa_10_vs_raw"],
            "psa-10-vs-raw-pct": latest_price["psa_10_vs_raw_pct"],
            "sales-volume": sales_volume_7d,
            "sales-volume-7d": sales_volume_7d,
            "sales-volume-pc": latest_price["sales_volume"],
        })
    else:
        result.update({
            "sales-volume": sales_volume_7d,
            "sales-volume-7d": sales_volume_7d,
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
            FROM ebay_history WHERE card_id = ? AND active_to IS NOT NULL AND active_to > 0 ORDER BY date ASC
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

    # --- Set-alpha linkage (and alpha's own recent returns for bearish check) ---
    sal = db.execute("""
        SELECT sal.alpha_card_id, sal.alpha_name, sal.contemp_corr,
               sal.lead_corr, sal.n_months,
               (SELECT psa_10_price FROM price_history
                  WHERE card_id = sal.alpha_card_id AND psa_10_price IS NOT NULL
                  ORDER BY date DESC LIMIT 1) AS alpha_psa10_current,
               (SELECT psa_10_price FROM price_history
                  WHERE card_id = sal.alpha_card_id
                    AND date <= date('now', '-30 days')
                    AND psa_10_price IS NOT NULL
                  ORDER BY date DESC LIMIT 1) AS alpha_psa10_30d_ago,
               (SELECT psa_10_price FROM price_history
                  WHERE card_id = sal.alpha_card_id
                    AND date <= date('now', '-90 days')
                    AND psa_10_price IS NOT NULL
                  ORDER BY date DESC LIMIT 1) AS alpha_psa10_90d_ago,
               (SELECT MAX(psa_10_price) FROM price_history
                  WHERE card_id = sal.alpha_card_id
                    AND date >= date('now', '-365 days')) AS alpha_psa10_max_1y
          FROM set_alpha_linkage sal
         WHERE sal.card_id = ?
    """, (card_id,)).fetchone()
    if sal:
        result["alpha-linkage"] = {
            "alpha-card-id":      sal["alpha_card_id"],
            "alpha-name":         sal["alpha_name"],
            "contemp-corr":       sal["contemp_corr"],
            "lead-corr":          sal["lead_corr"],
            "n-months":           sal["n_months"],
            "alpha-psa10-current":   sal["alpha_psa10_current"],
            "alpha-psa10-30d-ago":   sal["alpha_psa10_30d_ago"],
            "alpha-psa10-90d-ago":   sal["alpha_psa10_90d_ago"],
            "alpha-psa10-max-1y":    sal["alpha_psa10_max_1y"],
            "is-self":            (sal["alpha_card_id"] == card_id),
        }

    # --- tournament appearances (v2.2 competitive-play signal) ---
    # Pull from tournament_appearances table if it exists (populated by
    # pipeline.collectors.tournaments). Zero rows = card hasn't appeared
    # in any recent competitive tournament; the section is simply omitted
    # from the response in that case so the UI can show "no data."
    try:
        card_row = db.execute(
            "SELECT set_code, card_number FROM cards WHERE id = ?",
            (card_id,),
        ).fetchone()
        if card_row and card_row["set_code"] and card_row["card_number"]:
            tour_row = db.execute("""
                SELECT COUNT(*) AS total_90d,
                       SUM(CASE WHEN placing <= 8 THEN 1 ELSE 0 END) AS top8_90d,
                       SUM(CASE WHEN placing <= 16 THEN 1 ELSE 0 END) AS top16_90d,
                       COUNT(DISTINCT tournament_id) AS tournaments_90d,
                       MAX(tournament_date) AS last_seen
                  FROM tournament_appearances
                 WHERE set_code = ?
                   AND card_number = ?
                   AND tournament_date >= date('now', '-90 days')
            """, (card_row["set_code"], str(card_row["card_number"]))).fetchone()
            if tour_row and tour_row["total_90d"] and tour_row["total_90d"] > 0:
                result["tournament-play"] = {
                    "appearances-90d": tour_row["total_90d"],
                    "top8-appearances-90d": tour_row["top8_90d"] or 0,
                    "top16-appearances-90d": tour_row["top16_90d"] or 0,
                    "distinct-tournaments-90d": tour_row["tournaments_90d"] or 0,
                    "last-seen": tour_row["last_seen"],
                }
    except Exception:
        # tournament_appearances table may not exist yet on older DBs
        pass

    return result


@router.get("/card/{card_id}/peers")
def card_peers(card_id: str, min_corr: float = 0.60, db=Depends(get_db_conn)):
    """Cards that correlate with THIS card's PSA 10 returns (within its set),
    ranked by ρ. Uses the pairwise `card_peer_correlation` table.

    Surfaces organic clusters — e.g. for Glaceon, the other Eeveelutions
    correlate tighter with it (~0.92) than Umbreon the set alpha (0.76).
    """
    # Always surface the set alpha — even if its correlation with this card
    # is below the cutoff (e.g. Vaporeon ρ=0.39 to Umbreon still needs Umbreon
    # flagged as the alpha so the UI can show the set relationship).
    alpha_row = db.execute(
        "SELECT alpha_card_id FROM set_alpha_linkage WHERE card_id = ?",
        (card_id,),
    ).fetchone()
    alpha_id = alpha_row["alpha_card_id"] if alpha_row else None

    rows = db.execute("""
        SELECT cpc.card_b, cpc.corr, cpc.n_months,
               c.product_name, c.card_number, c.rarity_name,
               (SELECT psa_10_price FROM price_history
                  WHERE card_id = cpc.card_b AND psa_10_price IS NOT NULL
                  ORDER BY date DESC LIMIT 1)                       AS psa10_current,
               (SELECT psa_10_price FROM price_history
                  WHERE card_id = cpc.card_b
                    AND date <= date('now', '-30 days')
                    AND psa_10_price IS NOT NULL
                  ORDER BY date DESC LIMIT 1)                       AS psa10_30d_ago,
               (cpc.card_b = ?) AS is_alpha
          FROM card_peer_correlation cpc
          JOIN cards c ON c.id = cpc.card_b
         WHERE cpc.card_a = ?
           AND (cpc.corr >= ? OR cpc.card_b = ?)
      ORDER BY (cpc.card_b = ?) DESC, cpc.corr DESC
    """, (alpha_id, card_id, min_corr, alpha_id, alpha_id)).fetchall()

    # Self row (so the UI can highlight the card being viewed)
    self_row = db.execute("""
        SELECT c.id, c.product_name, c.card_number,
               (SELECT psa_10_price FROM price_history
                  WHERE card_id = c.id AND psa_10_price IS NOT NULL
                  ORDER BY date DESC LIMIT 1)                       AS cur,
               (SELECT psa_10_price FROM price_history
                  WHERE card_id = c.id
                    AND date <= date('now', '-30 days')
                    AND psa_10_price IS NOT NULL
                  ORDER BY date DESC LIMIT 1)                       AS d30
          FROM cards c WHERE c.id = ?
    """, (card_id,)).fetchone()

    out = []
    if self_row:
        cur, d30 = self_row["cur"], self_row["d30"]
        ret30 = (cur / d30 - 1) if (cur and d30 and d30 > 0) else None
        out.append({
            "id": self_row["id"],
            "product-name": self_row["product_name"],
            "card-number": self_row["card_number"],
            "corr": None,
            "psa-10-price": cur,
            "psa10-30d-return": ret30,
            "is-alpha": (alpha_id == card_id),
            "is-self": True,
        })

    for r in rows:
        cur, d30 = r["psa10_current"], r["psa10_30d_ago"]
        ret30 = (cur / d30 - 1) if (cur and d30 and d30 > 0) else None
        out.append({
            "id": r["card_b"],
            "product-name": r["product_name"],
            "card-number": r["card_number"],
            "corr": r["corr"],
            "psa-10-price": cur,
            "psa10-30d-return": ret30,
            "is-alpha": bool(r["is_alpha"]),
            "is-self": False,
        })

    # Guarantee the set alpha appears even when its correlation is below the
    # pairwise threshold OR was never persisted (e.g. Vaporeon↔Umbreon = 0.39,
    # below the 0.40 build floor). Look it up from set_alpha_linkage and splice
    # it in just after the self row.
    already_ids = {p["id"] for p in out}
    if alpha_id and alpha_id != card_id and alpha_id not in already_ids:
        a = db.execute("""
            SELECT c.id, c.product_name, c.card_number, sal.contemp_corr,
                   (SELECT psa_10_price FROM price_history
                      WHERE card_id = c.id AND psa_10_price IS NOT NULL
                      ORDER BY date DESC LIMIT 1)                      AS cur,
                   (SELECT psa_10_price FROM price_history
                      WHERE card_id = c.id
                        AND date <= date('now', '-30 days')
                        AND psa_10_price IS NOT NULL
                      ORDER BY date DESC LIMIT 1)                      AS d30
              FROM cards c
              LEFT JOIN set_alpha_linkage sal ON sal.card_id = ?
             WHERE c.id = ?
        """, (card_id, alpha_id)).fetchone()
        if a:
            cur, d30 = a["cur"], a["d30"]
            ret30 = (cur / d30 - 1) if (cur and d30 and d30 > 0) else None
            alpha_entry = {
                "id": a["id"],
                "product-name": a["product_name"],
                "card-number": a["card_number"],
                "corr": a["contemp_corr"],
                "psa-10-price": cur,
                "psa10-30d-return": ret30,
                "is-alpha": True,
                "is-self": False,
                "weak-alpha": True,  # flag so the UI can render a muted note
            }
            # Insert right after the self row (if present), else at the top
            insert_at = 1 if out and out[0].get("is-self") else 0
            out.insert(insert_at, alpha_entry)

    return {"card_id": card_id, "min_corr": min_corr, "peers": out}


@router.get("/card/{card_id}/alpha_peers")
def alpha_peers(card_id: str, db=Depends(get_db_conn)):
    """All cards linked to the same alpha as `card_id`, ranked by correlation."""
    row = db.execute(
        "SELECT alpha_card_id, set_code FROM set_alpha_linkage WHERE card_id = ?",
        (card_id,),
    ).fetchone()
    if not row:
        return {"alpha_card_id": None, "set_code": None, "peers": []}
    alpha_id = row["alpha_card_id"]
    set_code = row["set_code"]

    peers = db.execute("""
        SELECT sal.card_id, sal.contemp_corr, sal.lead_corr,
               c.product_name, c.card_number,
               (SELECT psa_10_price FROM price_history
                 WHERE card_id = sal.card_id AND psa_10_price IS NOT NULL
                 ORDER BY date DESC LIMIT 1)                      AS psa10_current,
               (SELECT psa_10_price FROM price_history
                 WHERE card_id = sal.card_id
                   AND date <= date('now', '-30 days')
                   AND psa_10_price IS NOT NULL
                 ORDER BY date DESC LIMIT 1)                      AS psa10_30d_ago
          FROM set_alpha_linkage sal
          JOIN cards c ON c.id = sal.card_id
         WHERE sal.alpha_card_id = ?
         ORDER BY (sal.card_id = sal.alpha_card_id) DESC,
                  sal.contemp_corr DESC
    """, (alpha_id,)).fetchall()

    out = []
    for p in peers:
        cur = p["psa10_current"]
        d30 = p["psa10_30d_ago"]
        ret30 = (cur / d30 - 1) if (cur and d30 and d30 > 0) else None
        out.append({
            "id": p["card_id"],
            "product-name": p["product_name"],
            "card-number": p["card_number"],
            "contemp-corr": p["contemp_corr"],
            "lead-corr": p["lead_corr"],
            "psa-10-price": cur,
            "psa10-30d-return": ret30,
            "is-alpha": (p["card_id"] == alpha_id),
            "is-self": (p["card_id"] == card_id),
        })

    return {"alpha_card_id": alpha_id, "set_code": set_code, "peers": out}


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
