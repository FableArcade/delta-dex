"""Market regime endpoint — a chase-basket bull/bear indicator.

Computes median 30d and 90d PSA 10 returns across a basket of blue-chip
cards (psa10 ≥ $200, iconic/strong cultural) and classifies the regime.

  BULL    — 30d ≥ +3% AND 90d ≥ +5%
  BEAR    — 30d ≤ -3% OR 90d ≤ -5%
  SIDEWAYS — in between

Blue chips lead the broad Pokemon market by weeks; their median return is
a cleaner sentiment signal than the whole-catalog average (which is
dragged down by friction on cheap cards).

Cached for 1 hour since the underlying price history only updates daily.
"""

from __future__ import annotations

import re
import time
from typing import Any, Dict

from fastapi import APIRouter, Depends

from api.deps import get_db_conn

router = APIRouter()


# ---------------------------------------------------------------------------
# Cultural score — lightweight Python copy of the JS rubric in
# frontend/js/wishlist_store.js. Needs to match closely; update in lockstep.
# ---------------------------------------------------------------------------

_ICONIC: list[tuple[re.Pattern, float]] = [
    (re.compile(r"charizard", re.I), 1.00),
    (re.compile(r"pikachu", re.I), 1.00),
    (re.compile(r"mewtwo", re.I), 0.96),
    (re.compile(r"\bmew\b", re.I), 0.96),
    (re.compile(r"umbreon", re.I), 0.96),
    (re.compile(r"lugia", re.I), 0.88),
    (re.compile(r"rayquaza", re.I), 0.88),
    (re.compile(r"gengar", re.I), 0.85),
    (re.compile(r"snorlax", re.I), 0.82),
    (re.compile(r"dragonite", re.I), 0.82),
    (re.compile(r"blastoise", re.I), 0.78),
    (re.compile(r"venusaur", re.I), 0.78),
    (re.compile(r"gyarados", re.I), 0.80),
    (re.compile(r"greninja", re.I), 0.82),
    (re.compile(r"lucario", re.I), 0.80),
    (re.compile(r"sylveon", re.I), 0.78),
    (re.compile(r"espeon", re.I), 0.75),
    (re.compile(r"eevee", re.I), 0.72),
    (re.compile(r"arceus", re.I), 0.72),
    (re.compile(r"giratina", re.I), 0.70),
    (re.compile(r"\bditto\b", re.I), 0.75),
    (re.compile(r"mimikyu", re.I), 0.78),
    (re.compile(r"gardevoir", re.I), 0.75),
    (re.compile(r"cynthia", re.I), 0.75),
    (re.compile(r"lillie", re.I), 0.72),
    (re.compile(r"iono", re.I), 0.68),
]
_RARITY_BONUS = {
    "SIR": 0.20, "MHR": 0.18, "HR": 0.12, "SCR": 0.12,
    "IR": 0.08, "UR": 0.05,
}


def _cultural_score(product_name: str, rarity_code: str | None) -> float:
    name = (product_name or "").lower()
    best = 0.0
    for pat, s in _ICONIC:
        if pat.search(name) and s > best:
            best = s
    return min(1.0, best + _RARITY_BONUS.get(rarity_code or "", 0.0))


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

_CACHE: dict[str, Any] = {"at": 0.0, "value": None}
_CACHE_TTL_SECONDS = 3600


def _compute_regime(db) -> Dict[str, Any]:
    # Pull all non-sealed cards with their latest PSA 10 price + 30d/90d lags.
    rows = db.execute(
        """
        WITH latest AS (
          SELECT card_id, MAX(date) AS d
          FROM price_history WHERE psa_10_price IS NOT NULL GROUP BY card_id
        )
        SELECT c.id, c.product_name, c.rarity_code,
               ph0.psa_10_price AS p0,
               (SELECT psa_10_price FROM price_history
                 WHERE card_id = c.id AND psa_10_price IS NOT NULL
                   AND date <= date(l.d, '-30 days')
                 ORDER BY date DESC LIMIT 1) AS p30,
               (SELECT psa_10_price FROM price_history
                 WHERE card_id = c.id AND psa_10_price IS NOT NULL
                   AND date <= date(l.d, '-90 days')
                 ORDER BY date DESC LIMIT 1) AS p90
          FROM cards c
          JOIN latest l ON l.card_id = c.id
          JOIN price_history ph0
            ON ph0.card_id = c.id AND ph0.date = l.d
         WHERE c.sealed_product = 'N' AND ph0.psa_10_price >= 200
        """
    ).fetchall()

    basket_30, basket_90 = [], []
    basket_size = 0
    for r in rows:
        cultural = _cultural_score(r["product_name"], r["rarity_code"])
        if cultural < 0.45:  # blue-chip gate: strong cultural or better
            continue
        p0 = r["p0"]
        basket_size += 1
        if r["p30"] and r["p30"] > 0:
            basket_30.append((p0 / r["p30"]) - 1.0)
        if r["p90"] and r["p90"] > 0:
            basket_90.append((p0 / r["p90"]) - 1.0)

    def _median(xs):
        if not xs:
            return None
        xs = sorted(xs)
        n = len(xs)
        return xs[n // 2] if n % 2 else 0.5 * (xs[n // 2 - 1] + xs[n // 2])

    m30 = _median(basket_30)
    m90 = _median(basket_90)

    # Classify. Conservative: bear if either horizon is clearly red; bull
    # requires both horizons to confirm so we don't call a bull off a
    # single noisy month.
    regime = "sideways"
    reason = ""
    if m30 is not None and m90 is not None:
        if m30 >= 0.03 and m90 >= 0.05:
            regime = "bull"
            reason = "both 30d and 90d chase-basket returns positive"
        elif (m30 is not None and m30 <= -0.03) or (m90 is not None and m90 <= -0.05):
            regime = "bear"
            reason = "30d or 90d chase-basket returns meaningfully negative"
        else:
            reason = "mixed or small moves in the chase basket"
    else:
        reason = "insufficient data to classify"

    return {
        "regime": regime,
        "reason": reason,
        "chase-30d-median-return": m30,
        "chase-90d-median-return": m90,
        "basket-size": basket_size,
        "samples-30d": len(basket_30),
        "samples-90d": len(basket_90),
        "description": {
            "bull":     "Blue-chip cards trending up. Model hit rates typically run high.",
            "bear":     "Blue-chip cards in drawdown. Model hit rates historically drop 15–25%.",
            "sideways": "Market is flat or noisy. Model works but returns are mixed.",
        }[regime],
    }


@router.get("/market/regime")
def market_regime(db=Depends(get_db_conn)):
    """Return the current chase-basket regime (bull / bear / sideways).

    Cached for 1 hour. The underlying price data refreshes daily, so a
    1-hour TTL is free and keeps this cheap.
    """
    now = time.time()
    if _CACHE["value"] is not None and (now - _CACHE["at"]) < _CACHE_TTL_SECONDS:
        return _CACHE["value"]
    value = _compute_regime(db)
    _CACHE["at"] = now
    _CACHE["value"] = value
    return value
