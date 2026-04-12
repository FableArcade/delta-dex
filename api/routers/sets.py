"""Set endpoints: index and detail."""

from typing import Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException
from api.deps import get_db_conn

router = APIRouter()


def _set_summary(row) -> dict:
    """Convert a set + leaderboard joined row into kebab-case dict."""
    keys = set(row.keys())
    return {
        "set-code": row["set_code"],
        "set-name": row["set_name"],
        "release-date": row["release_date"],
        "logo-url": row["logo_url"] if "logo_url" in keys else None,
        "rarity-buckets": row["rarity_buckets"],
        "cards-counted": row["cards_counted"],
        "avg-pack-cost": row["avg_pack_cost"],
        "ev-raw-per-pack": row["ev_raw_per_pack"],
        "ev-psa-10-per-pack": row["ev_psa_10_per_pack"],
        "avg-gain-loss": row["avg_gain_loss"],
        "total-set-raw-value": row["total_set_raw_value"],
        "psa-pop-10-base": row["psa_pop_10_base"],
        "psa-pop-total-base": row["psa_pop_total_base"],
        "psa-avg-gem-pct": row["psa_avg_gem_pct"],
    }


@router.get("/set_returns")
def set_returns(db=Depends(get_db_conn)):
    """Per-set median forward returns at 30d / 90d / 365d horizons.

    Computes, for each set, the median trailing return across all
    non-sealed PSA-10-priced cards in that set:

        ret_Nd = (current_price − Nd_ago_price) / Nd_ago_price

    Used by the wishlist scorer to compute **card alpha** relative to its
    set's median — a card outperforming its set is a different signal than
    a card moving with the set-wide market beta.

    Response:
        {
            "as_of": "YYYY-MM-DD",
            "sets": {
                "SET_CODE": {
                    "n_cards": int,
                    "median_30d_return": float,
                    "median_90d_return": float,
                    "median_365d_return": float
                },
                ...
            }
        }
    """
    # Pull current + historical prices for all non-sealed singles. We
    # compute the returns in Python because SQLite doesn't have a native
    # MEDIAN aggregate.
    rows = db.execute("""
        SELECT
            c.set_code,
            c.id AS card_id,
            ph.psa_10_price AS current_price,
            (SELECT psa_10_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-30 days')
                AND psa_10_price IS NOT NULL
              ORDER BY date DESC LIMIT 1)  AS price_30d_ago,
            (SELECT psa_10_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-90 days')
                AND psa_10_price IS NOT NULL
              ORDER BY date DESC LIMIT 1)  AS price_90d_ago,
            (SELECT psa_10_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-365 days')
                AND psa_10_price IS NOT NULL
              ORDER BY date DESC LIMIT 1)  AS price_365d_ago
        FROM cards c
        LEFT JOIN price_history ph
            ON ph.card_id = c.id
            AND ph.date = (SELECT MAX(date) FROM price_history WHERE card_id = c.id)
        WHERE c.sealed_product = 'N'
          AND ph.psa_10_price IS NOT NULL
          AND ph.psa_10_price >= 20
    """).fetchall()

    # Bucket returns per set
    per_set_30: Dict[str, List[float]] = {}
    per_set_90: Dict[str, List[float]] = {}
    per_set_365: Dict[str, List[float]] = {}

    for r in rows:
        cur = r["current_price"]
        set_code = r["set_code"]
        if cur is None or cur <= 0:
            continue
        p30  = r["price_30d_ago"]
        p90  = r["price_90d_ago"]
        p365 = r["price_365d_ago"]
        if p30 and p30 > 0:
            per_set_30.setdefault(set_code, []).append((cur / p30) - 1)
        if p90 and p90 > 0:
            per_set_90.setdefault(set_code, []).append((cur / p90) - 1)
        if p365 and p365 > 0:
            per_set_365.setdefault(set_code, []).append((cur / p365) - 1)

    def _median(values: List[float]) -> Optional[float]:
        if not values:
            return None
        s = sorted(values)
        n = len(s)
        if n % 2 == 1:
            return s[n // 2]
        return 0.5 * (s[n // 2 - 1] + s[n // 2])

    # Combine into a single per-set dict. Use the largest coverage sample
    # for n_cards (30d has the most data).
    all_sets = set(per_set_30) | set(per_set_90) | set(per_set_365)
    sets_out = {}
    for code in all_sets:
        sets_out[code] = {
            "n-cards": max(
                len(per_set_30.get(code, [])),
                len(per_set_90.get(code, [])),
                len(per_set_365.get(code, [])),
            ),
            "median-30d-return":  _median(per_set_30.get(code, [])),
            "median-90d-return":  _median(per_set_90.get(code, [])),
            "median-365d-return": _median(per_set_365.get(code, [])),
        }

    as_of = db.execute("SELECT MAX(date) AS d FROM price_history").fetchone()
    return {
        "as-of": as_of["d"] if as_of else None,
        "sets": sets_out,
    }


@router.get("/sets_index")
def sets_index(db=Depends(get_db_conn)):
    """All sets with latest leaderboard stats."""
    rows = db.execute("""
        SELECT
            s.set_code, s.set_name, s.release_date, s.logo_url,
            l.rarity_buckets, l.cards_counted, l.avg_pack_cost,
            l.ev_raw_per_pack, l.ev_psa_10_per_pack, l.avg_gain_loss,
            l.total_set_raw_value,
            l.psa_pop_10_base, l.psa_pop_total_base, l.psa_avg_gem_pct
        FROM sets s
        LEFT JOIN leaderboard l
            ON l.set_code = s.set_code
            AND l.date = (SELECT MAX(date) FROM leaderboard)
        ORDER BY s.set_name
    """).fetchall()

    return {"sets": [_set_summary(r) for r in rows]}


@router.get("/set/{set_code}")
def set_detail(set_code: str, db=Depends(get_db_conn)):
    """Full set detail with history, pack cost breakdown, and rarity breakdown."""

    # --- basic set info ---
    s = db.execute(
        "SELECT * FROM sets WHERE set_code = ?", (set_code,)
    ).fetchone()
    if not s:
        raise HTTPException(status_code=404, detail="Set not found")

    # --- latest leaderboard row ---
    lb = db.execute("""
        SELECT * FROM leaderboard
        WHERE set_code = ? ORDER BY date DESC LIMIT 1
    """, (set_code,)).fetchone()

    # --- pack cost latest ---
    pc = db.execute("""
        SELECT * FROM pack_cost
        WHERE set_code = ? ORDER BY date DESC LIMIT 1
    """, (set_code,)).fetchone()

    pack_cost_components = None
    pack_cost_sample_counts = None
    if pc:
        pack_cost_components = {
            "avg-booster-pack": pc["avg_booster_pack"],
            "avg-sleeved-booster-pack": pc["avg_sleeved_booster_pack"],
            "avg-booster-bundle-per-pack": pc["avg_booster_bundle_per_pack"],
            "avg-pack-cost": pc["avg_pack_cost"],
        }
        pack_cost_sample_counts = {
            "booster-pack-count": pc["booster_pack_count"],
            "sleeved-booster-count": pc["sleeved_booster_count"],
            "booster-bundle-count": pc["booster_bundle_count"],
        }

    # --- history (set_daily) ---
    daily_rows = db.execute("""
        SELECT date, ev_raw_per_pack, ev_psa_10_per_pack,
               avg_pack_cost, avg_gain_loss, total_set_raw_value
        FROM set_daily
        WHERE set_code = ?
        ORDER BY date ASC
    """, (set_code,)).fetchall()

    history = []
    for d in daily_rows:
        history.append({
            "date": d["date"],
            "ev-raw-per-pack": d["ev_raw_per_pack"],
            "ev-psa-10-per-pack": d["ev_psa_10_per_pack"],
            "avg-pack-cost": d["avg_pack_cost"],
            "avg-gain-loss": d["avg_gain_loss"],
            "total-set-raw-value": d["total_set_raw_value"],
        })

    # --- rarity breakdown ---
    rarities = db.execute("""
        SELECT r.set_rarity, r.rarity_code, r.rarity_name,
               r.card_count, r.pull_rate, r.pull_rate_odds,
               snap.avg_raw_price, snap.avg_psa_10_price,
               snap.ev_raw_per_pack, snap.ev_psa_10_per_pack,
               snap.psa_pop_10_base, snap.psa_pop_total_base,
               snap.psa_avg_gem_pct
        FROM rarities r
        LEFT JOIN set_rarity_snapshot snap
            ON snap.set_rarity = r.set_rarity
            AND snap.date = (
                SELECT MAX(date) FROM set_rarity_snapshot
                WHERE set_rarity = r.set_rarity
            )
        WHERE r.set_code = ?
        ORDER BY r.rarity_code
    """, (set_code,)).fetchall()

    rarity_breakdown = {}
    for ra in rarities:
        rarity_breakdown[ra["rarity_code"]] = {
            "rarity-name": ra["rarity_name"],
            "card-count": ra["card_count"],
            "pull-rate": ra["pull_rate"],
            "pull-rate-odds": ra["pull_rate_odds"],
            "avg-raw-price": ra["avg_raw_price"],
            "avg-psa-10-price": ra["avg_psa_10_price"],
            "ev-raw-per-pack": ra["ev_raw_per_pack"],
            "ev-psa-10-per-pack": ra["ev_psa_10_per_pack"],
            "psa-pop-10-base": ra["psa_pop_10_base"],
            "psa-pop-total-base": ra["psa_pop_total_base"],
            "psa-avg-gem-pct": ra["psa_avg_gem_pct"],
        }

    # --- assemble response ---
    # logo_url may or may not exist depending on schema version
    s_keys = set(s.keys())
    result = {
        "set-code": s["set_code"],
        "set-name": s["set_name"],
        "release-date": s["release_date"],
        "logo-url": s["logo_url"] if "logo_url" in s_keys else None,
    }

    if lb:
        result.update({
            "rarity-buckets": lb["rarity_buckets"],
            "cards-counted": lb["cards_counted"],
            "avg-pack-cost": lb["avg_pack_cost"],
            "ev-raw-per-pack": lb["ev_raw_per_pack"],
            "ev-psa-10-per-pack": lb["ev_psa_10_per_pack"],
            "avg-gain-loss": lb["avg_gain_loss"],
            "total-set-raw-value": lb["total_set_raw_value"],
            "psa-pop-10-base": lb["psa_pop_10_base"],
            "psa-pop-total-base": lb["psa_pop_total_base"],
            "psa-avg-gem-pct": lb["psa_avg_gem_pct"],
        })

    result["pack-cost-components"] = pack_cost_components
    result["pack-cost-sample-counts"] = pack_cost_sample_counts
    result["history"] = history
    result["rarity-breakdown"] = rarity_breakdown

    return result
