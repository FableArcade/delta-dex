"""Feature engineering for PokeDelta prediction model.

Assembles training datasets from historical price data and generates
feature vectors for live inference. All features use only data available
at-or-before the anchor date (no future leakage).
"""

from __future__ import annotations

import logging
import math
import re
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from pipeline.model.friction import net_realized_return
from pipeline.model.liquidity import (
    LIQUIDITY_COLUMNS,
    compute_liquidity_at_date,
    compute_live_liquidity,
)
from pipeline.model.reprint_risk import (
    REPRINT_COLUMNS,
    build_reprint_index,
    load_release_calendar,
    reprint_features_at_date,
)
from pipeline.model.catalyst import (
    CATALYST_COLUMNS,
    catalyst_features_at_date,
    load_set_release_calendar,
)
from pipeline.model.tournament_signal import (
    TOURNAMENT_COLUMNS,
    tournament_features_at_date,
    load_tournament_data,
)

logger = logging.getLogger("pipeline.model.features")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIN_PSA10_PRICE = 10.0
MIN_TRAILING_DAYS = 90
MIN_FORWARD_DAYS = 180
HORIZON_DAYS = 180
OUTLIER_TRIM_PCT = 0.02  # trim top/bottom 2% of targets
TARGET_COL = f"target_return_{HORIZON_DAYS}d"

# Cultural impact scoring (ported from card_leaderboard.js)
ICONIC_NAMES: Dict[str, float] = {
    "charizard": 1.00, "pikachu": 1.00, "mewtwo": 0.96, "mew": 0.96,
    "umbreon": 0.96, "lugia": 0.88, "rayquaza": 0.88, "gengar": 0.85,
    "snorlax": 0.82, "dragonite": 0.82, "eevee": 0.78, "sylveon": 0.78,
    "espeon": 0.75, "vaporeon": 0.72, "jolteon": 0.72, "flareon": 0.70,
    "glaceon": 0.70, "leafeon": 0.70, "gardevoir": 0.68, "lucario": 0.68,
    "arcanine": 0.65, "gyarados": 0.65, "blastoise": 0.65,
    "venusaur": 0.62, "alakazam": 0.60, "machamp": 0.58,
    "cynthia": 0.75, "lillie": 0.72, "iono": 0.68, "marnie": 0.65,
    "n ": 0.60, "professor": 0.55,
}

RARITY_BONUS: Dict[str, float] = {
    "SIR": 0.20, "MHR": 0.18, "HR": 0.12, "SCR": 0.12, "RR": 0.10,
    "GR": 0.10, "IR": 0.08, "UR": 0.05,
}

# Cultural ceiling tiers — categorical variable capturing franchise-level
# status that's distinct from current popularity.
#   3 = Franchise face: unbounded ceiling, series-defining
#   2 = Mascot tier: very high ceiling, persistent fan favorites
#   1 = Popular tier: strong ceiling but bounded (hype cycles shorter)
#   0 = Standard: baseline
CULTURAL_TIERS: Dict[str, int] = {
    # Tier 3: Franchise faces
    "charizard": 3, "pikachu": 3,
    # Tier 2: Mascot-tier (iconic across generations, strong merchandise presence)
    "mewtwo": 2, "mew": 2, "umbreon": 2, "lugia": 2, "rayquaza": 2,
    "eevee": 2, "gengar": 2,
    # Tier 1: Popular tier (beloved but bounded — hype cycles shorter)
    "snorlax": 1, "dragonite": 1, "sylveon": 1, "espeon": 1,
    "vaporeon": 1, "jolteon": 1, "flareon": 1, "glaceon": 1,
    "leafeon": 1, "gardevoir": 1, "lucario": 1, "arcanine": 1,
    "gyarados": 1, "blastoise": 1, "venusaur": 1, "alakazam": 1,
    "machamp": 1, "greninja": 1, "garchomp": 1, "tyranitar": 1,
    "dialga": 1, "palkia": 1, "giratina": 1, "arceus": 1,
    "zacian": 1, "zamazenta": 1,
}

# Regex for extracting core Pokemon name: strip suffixes (V, VMAX, VSTAR,
# EX, GX, BREAK, ex), card numbers, brackets, parens, rarity tags.
_POKEMON_NAME_CLEAN = re.compile(
    r"\s*(?:v\s*(?:max|star)?|ex|gx|break|le?g|vmax|vstar|"
    r"#\S+|\[.+?\]|\(.+?\)|\b(?:reverse|holo|rainbow|gold|full art|promo)\b)",
    re.IGNORECASE,
)


def cultural_score(product_name: str, rarity_code: Optional[str]) -> float:
    name_lower = product_name.lower()
    name_score = 0.0
    for key, val in ICONIC_NAMES.items():
        if key in name_lower:
            name_score = max(name_score, val)
            break
    bonus = RARITY_BONUS.get(rarity_code or "", 0.0)
    return min(1.0, name_score + bonus)


def cultural_tier(product_name: str) -> int:
    """Return ceiling tier (0-3) for a card based on the Pokemon name."""
    name_lower = product_name.lower()
    # Check highest tier first to avoid miscategorizing
    for tier_val in (3, 2, 1):
        for key, val in CULTURAL_TIERS.items():
            if val == tier_val and key in name_lower:
                return tier_val
    return 0


def extract_pokemon_name(product_name: str) -> str:
    """Extract the core Pokemon name from a card product name.

    Examples:
      "Charizard VSTAR #GG70" -> "charizard"
      "Origin Forme Dialga VSTAR #GG68" -> "origin forme dialga"
      "Hisuian Zoroark VSTAR" -> "hisuian zoroark"
      "Team Rocket's Mewtwo" -> "team rocket's mewtwo"
    """
    name = product_name.lower()
    name = _POKEMON_NAME_CLEAN.sub("", name)
    # Collapse whitespace
    name = " ".join(name.split()).strip()
    return name


# ---------------------------------------------------------------------------
# Historical training dataset
# ---------------------------------------------------------------------------

def build_training_dataset(db: sqlite3.Connection) -> pd.DataFrame:
    """Build training dataset from all historical price data.

    For each card, walks through monthly anchors and computes features
    using only trailing data, with the target being 90-day forward return.

    Returns a DataFrame with one row per (card, anchor_date) sample.
    """
    logger.info("Building training dataset...")

    # Load all cards (non-sealed only)
    cards = db.execute(
        "SELECT id, product_name, rarity_code FROM cards "
        "WHERE sealed_product = 'N'"
    ).fetchall()
    logger.info("Found %d non-sealed cards", len(cards))

    # Pre-compute cultural scores, tiers, and Pokemon name mapping
    cultural_scores = {}
    cultural_tiers_by_card = {}
    pokemon_names = {}
    for r in cards:
        cultural_scores[r["id"]] = cultural_score(r["product_name"], r["rarity_code"])
        cultural_tiers_by_card[r["id"]] = cultural_tier(r["product_name"])
        pokemon_names[r["id"]] = extract_pokemon_name(r["product_name"])
    card_ids = [r["id"] for r in cards]

    # Load all price history into a DataFrame for fast access
    price_df = pd.read_sql_query(
        "SELECT card_id, date, raw_price, psa_10_price, psa_9_price, "
        "psa_8_price, sales_volume "
        "FROM price_history WHERE card_id IN ({}) "
        "ORDER BY card_id, date".format(
            ",".join("?" for _ in card_ids)
        ),
        db,
        params=card_ids,
    )
    price_df["date"] = pd.to_datetime(price_df["date"])
    logger.info("Loaded %d price history rows", len(price_df))

    # Pre-compute per-Pokemon running max price across ALL cards of that
    # Pokemon, month by month. This captures franchise-level ceiling:
    # if any Pikachu card has ever hit $5000, that's a ceiling signal for
    # any new Pikachu printing. Uses trailing max (no future leakage).
    logger.info("Pre-computing per-Pokemon historical peaks...")
    price_df_peak = price_df.copy()
    price_df_peak["pokemon"] = price_df_peak["card_id"].map(pokemon_names)
    price_df_peak = price_df_peak[price_df_peak["psa_10_price"].notna() &
                                    (price_df_peak["psa_10_price"] >= MIN_PSA10_PRICE)]
    price_df_peak["month"] = price_df_peak["date"].dt.to_period("M").dt.to_timestamp()
    # For each (pokemon, month), find the max PSA 10 price seen in that month
    monthly_max = price_df_peak.groupby(["pokemon", "month"])["psa_10_price"].max().reset_index()
    monthly_max = monthly_max.sort_values(["pokemon", "month"])
    # Running cumulative max up to and including this month
    monthly_max["running_max"] = monthly_max.groupby("pokemon")["psa_10_price"].cummax()
    pokemon_peak_by_month = {}
    for _, row in monthly_max.iterrows():
        pokemon_peak_by_month[(row["pokemon"], row["month"])] = float(row["running_max"])
    logger.info("Indexed peaks for %d pokemon x months",
                len(pokemon_peak_by_month))

    # Load PSA pop history
    psa_df = pd.read_sql_query(
        "SELECT card_id, date, psa_10_base, total_base, gem_pct "
        "FROM psa_pop_history WHERE card_id IN ({}) "
        "ORDER BY card_id, date".format(
            ",".join("?" for _ in card_ids)
        ),
        db,
        params=card_ids,
    )
    psa_df["date"] = pd.to_datetime(psa_df["date"])

    # Load market pressure (latest per card per window)
    mp_df = pd.read_sql_query(
        "SELECT card_id, window_days, net_flow_pct, demand_pressure, "
        "supply_pressure, as_of "
        "FROM market_pressure WHERE mode = 'observed'",
        db,
    )

    # Load supply saturation
    ss_df = pd.read_sql_query(
        "SELECT card_id, supply_saturation_index, as_of "
        "FROM supply_saturation WHERE mode = 'observed'",
        db,
    )

    # v1.2: Load full eBay history for liquidity features
    ebay_df = pd.read_sql_query(
        "SELECT card_id, date, ended, new, active_from "
        "FROM ebay_history WHERE card_id IN ({}) ORDER BY card_id, date".format(
            ",".join("?" for _ in card_ids)
        ),
        db,
        params=card_ids,
    )
    ebay_df["date"] = pd.to_datetime(ebay_df["date"])

    # v1.2: Build reprint calendar once for all cards
    logger.info("Loading reprint calendar...")
    release_df = load_release_calendar(db)
    reprint_idx = build_reprint_index(release_df)
    card_set_codes = dict(
        db.execute(
            "SELECT id, set_code FROM cards WHERE sealed_product = 'N'"
        ).fetchall()
    )
    logger.info("Indexed %d Pokemon release timelines", len(reprint_idx))

    # v2.1: Set-release catalyst index (same data, different lookup)
    set_dates, all_release_dates_sorted = load_set_release_calendar(db)
    logger.info("Catalyst index: %d sets with release dates", len(set_dates))

    # v2.2: Tournament data (Signal Class 3)
    tournament_df = load_tournament_data(db)
    card_setnum = dict(
        db.execute(
            "SELECT id, set_code || '|' || card_number FROM cards "
            "WHERE sealed_product = 'N' AND set_code IS NOT NULL "
            "AND card_number IS NOT NULL"
        ).fetchall()
    )
    logger.info("Tournament data: %d appearance rows, %d card keys",
                len(tournament_df), len(card_setnum))

    samples = []
    for card_id in card_ids:
        card_prices = price_df[price_df["card_id"] == card_id].copy()
        if len(card_prices) < 2:
            continue

        card_prices = card_prices.set_index("date").sort_index()

        # Filter to rows with valid PSA 10 price
        valid = card_prices[card_prices["psa_10_price"].notna() &
                            (card_prices["psa_10_price"] >= MIN_PSA10_PRICE)]
        if len(valid) < 7:  # need enough history
            continue

        # Monthly resampling: first observation per month
        monthly = valid.resample("MS").first().dropna(subset=["psa_10_price"])
        if len(monthly) < 6:
            continue

        dates = monthly.index.tolist()
        cult_score = cultural_scores.get(card_id, 0.0)
        cult_tier = cultural_tiers_by_card.get(card_id, 0)
        pokemon_name = pokemon_names.get(card_id, "")

        # Get PSA data for this card
        card_psa = psa_df[psa_df["card_id"] == card_id].set_index("date").sort_index()

        for i in range(3, len(dates)):
            anchor_date = dates[i]
            # Check forward runway
            forward_date = anchor_date + pd.Timedelta(days=HORIZON_DAYS)
            # Find closest price to forward_date
            future_prices = valid[valid.index >= forward_date]
            if future_prices.empty:
                continue
            forward_price = future_prices.iloc[0]["psa_10_price"]
            anchor_price = monthly.loc[anchor_date, "psa_10_price"]

            if anchor_price <= 0 or forward_price <= 0:
                continue

            # v1.2: Net-of-cost target. Gross price gain minus eBay fees
            # and shipping — the return an investor actually realizes.
            target = net_realized_return(anchor_price, forward_price)

            # Compute features at anchor date
            trailing = valid[valid.index <= anchor_date]
            if len(trailing) < 7:
                continue

            features = _compute_features_at_date(
                trailing, anchor_price, anchor_date, cult_score, card_psa,
                mp_df[mp_df["card_id"] == card_id],
                ss_df[ss_df["card_id"] == card_id],
            )
            if features is None:
                continue

            # v1.2: Liquidity features from trailing eBay window
            card_ebay = ebay_df[ebay_df["card_id"] == card_id].set_index("date")
            features.update(compute_liquidity_at_date(card_ebay, anchor_date))

            # v1.2: Reprint-risk features
            features.update(reprint_features_at_date(
                pokemon_name,
                card_set_codes.get(card_id, ""),
                anchor_date,
                reprint_idx,
            ))

            # v2.1: Set-release catalyst features
            features.update(catalyst_features_at_date(
                card_set_codes.get(card_id, ""),
                anchor_date,
                set_dates,
                all_release_dates_sorted,
            ))

            # v2.2: Tournament competitive-demand features
            sn = card_setnum.get(card_id, "|").split("|", 1)
            features.update(tournament_features_at_date(
                sn[0] if len(sn) == 2 else "",
                sn[1] if len(sn) == 2 else "",
                anchor_date,
                tournament_df,
            ))

            # Add new cultural features
            features["cultural_tier"] = float(cult_tier)
            # Pokemon peak-ever as-of anchor date (trailing, no leakage).
            # Use monthly bucket — peak through anchor's month, inclusive.
            month = pd.Timestamp(anchor_date.year, anchor_date.month, 1)
            peak = pokemon_peak_by_month.get((pokemon_name, month))
            if peak is None or peak <= 0:
                # Fallback: use this card's own trailing max
                peak = float(prices[prices.index <= anchor_date].max() or anchor_price)
            # Log scale so $100 peak and $10000 peak are both meaningful
            features["pokemon_peak_log"] = math.log10(max(peak, 1.0))
            # Ratio of current price to pokemon's ever-peak (0.1 = deep value, 1.0 = at peak)
            features["pokemon_peak_ratio"] = anchor_price / peak if peak > 0 else 1.0

            features["card_id"] = card_id
            features["anchor_date"] = anchor_date.isoformat()[:10]
            features[TARGET_COL] = target
            samples.append(features)

    df = pd.DataFrame(samples)
    logger.info("Raw samples: %d", len(df))

    if df.empty:
        return df

    # Trim outliers on target
    lo = df[TARGET_COL].quantile(OUTLIER_TRIM_PCT)
    hi = df[TARGET_COL].quantile(1 - OUTLIER_TRIM_PCT)
    df = df[(df[TARGET_COL] >= lo) & (df[TARGET_COL] <= hi)]
    logger.info("After outlier trim: %d samples", len(df))

    return df


def _compute_features_at_date(
    trailing: pd.DataFrame,
    current_price: float,
    anchor_date: pd.Timestamp,
    cult_score: float,
    card_psa: pd.DataFrame,
    card_mp: pd.DataFrame,
    card_ss: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    """Compute the 20-feature vector using only data at/before anchor_date."""

    prices = trailing["psa_10_price"]
    raw_prices = trailing["raw_price"]

    # Price-derived features
    p_30d = _price_at_offset(trailing, anchor_date, 30)
    p_90d = _price_at_offset(trailing, anchor_date, 90)
    p_365d = _price_at_offset(trailing, anchor_date, 365)

    max_1y = prices[prices.index >= anchor_date - pd.Timedelta(days=365)].max()
    min_1y_series = prices[(prices.index >= anchor_date - pd.Timedelta(days=365)) & (prices > 0)]
    min_1y = min_1y_series.min() if len(min_1y_series) > 0 else current_price

    ret_30d = (current_price / p_30d - 1) if p_30d and p_30d > 0 else 0.0
    ret_90d = (current_price / p_90d - 1) if p_90d and p_90d > 0 else 0.0
    ret_365d = (current_price / p_365d - 1) if p_365d and p_365d > 0 else 0.0

    peak_discount = (max_1y - current_price) / max_1y if max_1y > 0 else 0.0
    trough_recovery = (current_price - min_1y) / min_1y if min_1y > 0 else 0.0
    volatility = (max_1y - min_1y) / current_price if current_price > 0 else 0.0

    # Moving average distance
    anchors = [p for p in [p_30d, p_90d, p_365d] if p and p > 0]
    ma = np.mean(anchors) if anchors else current_price
    ma_distance = (ma - current_price) / ma if ma > 0 else 0.0

    log_price = math.log10(max(current_price, 1.0))

    # Demand/supply features (use latest available before anchor)
    nf_7d = _get_mp_feature(card_mp, 7, "net_flow_pct")
    nf_30d = _get_mp_feature(card_mp, 30, "net_flow_pct")
    dp_7d = _get_mp_feature(card_mp, 7, "demand_pressure")
    dp_30d = _get_mp_feature(card_mp, 30, "demand_pressure")
    sp_30d = _get_mp_feature(card_mp, 30, "supply_pressure")
    ds_ratio = dp_30d / sp_30d if sp_30d and sp_30d > 0 else 1.0

    sat_index = _get_ss_feature(card_ss)

    # PSA scarcity
    gem_pct = _get_latest_psa(card_psa, anchor_date, "gem_pct")
    psa_10_pop = _get_latest_psa(card_psa, anchor_date, "psa_10_base")

    # PSA 10 premium
    current_raw = raw_prices.iloc[-1] if len(raw_prices) > 0 and pd.notna(raw_prices.iloc[-1]) else None
    psa_10_vs_raw_pct = ((current_price / current_raw - 1) * 100
                          if current_raw and current_raw > 0 else 0.0)

    # History coverage
    history_days = len(prices[prices.index >= anchor_date - pd.Timedelta(days=365)])

    return {
        "ret_30d": ret_30d,
        "ret_90d": ret_90d,
        "ret_365d": ret_365d,
        "peak_discount": max(0, peak_discount),
        "trough_recovery": max(0, trough_recovery),
        "volatility": max(0, volatility),
        "ma_distance": ma_distance,
        "log_price": log_price,
        "net_flow_pct_7d": nf_7d or 0.0,
        "net_flow_pct_30d": nf_30d or 0.0,
        "demand_pressure_7d": dp_7d or 0.0,
        "demand_pressure_30d": dp_30d or 0.0,
        "supply_saturation_index": sat_index or 1.0,
        "ds_ratio": ds_ratio,
        "gem_pct": gem_pct or 0.10,
        "psa_10_pop": psa_10_pop or 0.0,
        "psa_10_vs_raw_pct": psa_10_vs_raw_pct,
        "cultural_score": cult_score,
        "rarity_tier": 0.0,  # will be set by caller if needed
        "history_days": float(history_days),
    }


# ---------------------------------------------------------------------------
# Live inference feature vector
# ---------------------------------------------------------------------------

FEATURE_COLUMNS = [
    "ret_30d", "ret_90d", "ret_365d", "peak_discount", "trough_recovery",
    "volatility", "ma_distance", "log_price", "net_flow_pct_7d",
    "net_flow_pct_30d", "demand_pressure_7d", "demand_pressure_30d",
    "supply_saturation_index", "ds_ratio", "gem_pct", "psa_10_pop",
    # `psa_10_vs_raw_pct` PERMANENTLY REMOVED (2026-04-16):
    # Ratio of two loosely-coupled markets (PSA 10 vs. raw) with hidden
    # condition variance on the raw side + temporal misalignment between
    # numerator and denominator. Ablation (scripts/ablate_collider.py)
    # showed removal gave +18% relative Sharpe on top-2% conviction.
    # Signal it tried to capture is already cleanly present via gem_pct,
    # psa_10_pop, and log_price. See docs/MODEL_DAG.md.
    "cultural_score", "rarity_tier", "history_days",
    # v1.1: Cultural ceiling features
    "cultural_tier", "pokemon_peak_log", "pokemon_peak_ratio",
    # v1.2: Liquidity features REMOVED (2026-04-16) —
    # sales_per_day_30d, sell_through_30d, ask_bid_proxy_30d,
    # new_listings_per_day_30d, thin_market_flag all have ZERO variance
    # across the training dataset (all rows identical values). The
    # underlying ebay_history table isn't populated at feature-compute
    # time. Five dead features wasting model capacity. Re-introduce
    # only after the ebay_history pipeline is fixed — see MODEL_DAG.md.
    # *LIQUIDITY_COLUMNS,
    # v1.2: Reprint risk — supply-side existential risk for singles
    *REPRINT_COLUMNS,
    # v2.1 CATALYST features HELD BACK (2026-04-16). Trained a v2.1 model
    # with 28 features (catalyst added) and validated +0.62 Sharpe lift,
    # but top-1%/top-2% hit rates regressed -2.5pp and -1.0pp. User's
    # priority is hit-rate-first (psychologically-consistent investing)
    # over Sharpe-first (position-sizing), so reverted to v2.0 25-feature
    # set. Catalyst features remain COMPUTED (so DataFrame has them) but
    # NOT in FEATURE_COLUMNS — toggle back by re-enabling the line below.
    # *CATALYST_COLUMNS,
    # v2.2 TOURNAMENT features HELD BACK (2026-04-16). Temporal mismatch
    # between 180-day tournament coverage and training-anchor range
    # ending 2025-10-01 = zero training variance. Collected and present
    # in live features for UI surfacing only.
    # *TOURNAMENT_COLUMNS,
]


def build_live_features(db: sqlite3.Connection) -> pd.DataFrame:
    """Build feature vectors for all active cards using current DB state.

    Returns a DataFrame indexed by card_id with FEATURE_COLUMNS.
    """
    logger.info("Building live feature vectors...")

    # Pre-compute current peak-ever per Pokemon name across all cards
    logger.info("Computing per-Pokemon current peaks for live inference...")
    all_cards = db.execute(
        "SELECT id, product_name FROM cards WHERE sealed_product = 'N'"
    ).fetchall()
    pokemon_names_map = {r["id"]: extract_pokemon_name(r["product_name"]) for r in all_cards}

    peak_df = pd.read_sql_query("""
        SELECT card_id, MAX(psa_10_price) AS peak
        FROM price_history
        WHERE psa_10_price IS NOT NULL
        GROUP BY card_id
    """, db)
    card_peak = dict(zip(peak_df["card_id"], peak_df["peak"]))
    # Aggregate by Pokemon name
    pokemon_peak = {}
    for card_id, pokemon in pokemon_names_map.items():
        peak = card_peak.get(card_id)
        if peak is None or peak <= 0:
            continue
        existing = pokemon_peak.get(pokemon, 0)
        if peak > existing:
            pokemon_peak[pokemon] = float(peak)
    logger.info("Indexed peaks for %d unique Pokemon", len(pokemon_peak))

    # Use the same card_index query pattern as the API
    rows = db.execute("""
        SELECT
            c.id, c.product_name, c.rarity_code, c.sealed_product,
            ph.raw_price, ph.psa_10_price, ph.psa_10_vs_raw_pct,
            pp.gem_pct, pp.psa_10_base,
            mp30.net_flow_pct AS nf_30d,
            mp30.demand_pressure AS dp_30d,
            mp30.supply_pressure AS sp_30d,
            mp7.net_flow_pct AS nf_7d,
            mp7.demand_pressure AS dp_7d,
            ss.supply_saturation_index,
            (SELECT raw_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-30 days')
                AND raw_price IS NOT NULL
              ORDER BY date DESC LIMIT 1) AS raw_30d_ago,
            (SELECT raw_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-90 days')
                AND raw_price IS NOT NULL
              ORDER BY date DESC LIMIT 1) AS raw_90d_ago,
            (SELECT raw_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-365 days')
                AND raw_price IS NOT NULL
              ORDER BY date DESC LIMIT 1) AS raw_365d_ago,
            (SELECT psa_10_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-30 days')
                AND psa_10_price IS NOT NULL
              ORDER BY date DESC LIMIT 1) AS psa10_30d_ago,
            (SELECT psa_10_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-90 days')
                AND psa_10_price IS NOT NULL
              ORDER BY date DESC LIMIT 1) AS psa10_90d_ago,
            (SELECT psa_10_price FROM price_history
              WHERE card_id = c.id AND date <= date('now', '-365 days')
                AND psa_10_price IS NOT NULL
              ORDER BY date DESC LIMIT 1) AS psa10_365d_ago,
            (SELECT MAX(psa_10_price) FROM price_history
              WHERE card_id = c.id AND date >= date('now', '-365 days')) AS psa10_max_1y,
            (SELECT MIN(psa_10_price) FROM price_history
              WHERE card_id = c.id AND date >= date('now', '-365 days')
                AND psa_10_price > 0) AS psa10_min_1y,
            (SELECT COUNT(DISTINCT date) FROM price_history
              WHERE card_id = c.id AND date >= date('now', '-365 days')) AS history_days
        FROM cards c
        LEFT JOIN price_history ph
            ON ph.card_id = c.id
            AND ph.date = (SELECT MAX(date) FROM price_history WHERE card_id = c.id)
        LEFT JOIN psa_pop_history pp
            ON pp.card_id = c.id
            AND pp.date = (SELECT MAX(date) FROM psa_pop_history WHERE card_id = c.id)
        LEFT JOIN market_pressure mp30
            ON mp30.card_id = c.id AND mp30.window_days = 30 AND mp30.mode = 'observed'
            AND mp30.as_of = (SELECT MAX(as_of) FROM market_pressure
                               WHERE card_id = c.id AND window_days = 30 AND mode = 'observed')
        LEFT JOIN market_pressure mp7
            ON mp7.card_id = c.id AND mp7.window_days = 7 AND mp7.mode = 'observed'
            AND mp7.as_of = (SELECT MAX(as_of) FROM market_pressure
                              WHERE card_id = c.id AND window_days = 7 AND mode = 'observed')
        LEFT JOIN supply_saturation ss
            ON ss.card_id = c.id AND ss.mode = 'observed'
            AND ss.as_of = (SELECT MAX(as_of) FROM supply_saturation
                             WHERE card_id = c.id AND ss.mode = 'observed')
        WHERE c.sealed_product = 'N'
    """).fetchall()

    features_list = []
    for r in rows:
        psa10 = r["psa_10_price"]
        if not psa10 or psa10 < MIN_PSA10_PRICE:
            continue

        p30 = r["psa10_30d_ago"]
        p90 = r["psa10_90d_ago"]
        p365 = r["psa10_365d_ago"]
        max_1y = r["psa10_max_1y"] or psa10
        min_1y = r["psa10_min_1y"] or psa10

        ret_30d = (psa10 / p30 - 1) if p30 and p30 > 0 else 0.0
        ret_90d = (psa10 / p90 - 1) if p90 and p90 > 0 else 0.0
        ret_365d = (psa10 / p365 - 1) if p365 and p365 > 0 else 0.0

        peak_disc = max(0, (max_1y - psa10) / max_1y) if max_1y > 0 else 0.0
        trough_rec = max(0, (psa10 - min_1y) / min_1y) if min_1y > 0 else 0.0
        vol = max(0, (max_1y - min_1y) / psa10) if psa10 > 0 else 0.0

        anchors = [p for p in [p30, p90, p365] if p and p > 0]
        ma = np.mean(anchors) if anchors else psa10
        ma_dist = (ma - psa10) / ma if ma > 0 else 0.0

        sp = r["sp_30d"]
        dp = r["dp_30d"]
        ds = dp / sp if sp and sp > 0 else 1.0

        cult = cultural_score(r["product_name"], r["rarity_code"])
        cult_tier = cultural_tier(r["product_name"])

        # v1.1 cultural ceiling features
        pokemon = pokemon_names_map.get(r["id"], "")
        peak = pokemon_peak.get(pokemon)
        if peak is None or peak <= 0:
            peak = psa10  # Fallback: this card's own current price
        peak_log = math.log10(max(peak, 1.0))
        peak_ratio = psa10 / peak if peak > 0 else 1.0

        features_list.append({
            "card_id": r["id"],
            "ret_30d": ret_30d,
            "ret_90d": ret_90d,
            "ret_365d": ret_365d,
            "peak_discount": peak_disc,
            "trough_recovery": trough_rec,
            "volatility": vol,
            "ma_distance": ma_dist,
            "log_price": math.log10(max(psa10, 1.0)),
            "net_flow_pct_7d": r["nf_7d"] or 0.0,
            "net_flow_pct_30d": r["nf_30d"] or 0.0,
            "demand_pressure_7d": r["dp_7d"] or 0.0,
            "demand_pressure_30d": dp or 0.0,
            "supply_saturation_index": r["supply_saturation_index"] or 1.0,
            "ds_ratio": ds,
            "gem_pct": r["gem_pct"] or 0.10,
            "psa_10_pop": float(r["psa_10_base"] or 0),
            "psa_10_vs_raw_pct": r["psa_10_vs_raw_pct"] or 0.0,
            "cultural_score": cult,
            "rarity_tier": 0.0,
            "history_days": float(r["history_days"] or 0),
            # v1.1: Cultural ceiling features
            "cultural_tier": float(cult_tier),
            "pokemon_peak_log": peak_log,
            "pokemon_peak_ratio": peak_ratio,
        })

    df = pd.DataFrame(features_list)
    if not df.empty:
        df = df.set_index("card_id")

    # v1.2: Enrich with liquidity features from ebay_history
    liq = compute_live_liquidity(db)
    if not liq.empty:
        df = df.join(liq, how="left")
    for col in LIQUIDITY_COLUMNS:
        if col not in df.columns:
            df[col] = 0.0
        # thin_market defaults to 1 (pessimistic assumption: no data = illiquid);
        # all other liquidity features default to 0 (no activity).
        default = 1.0 if col == "thin_market_flag" else 0.0
        df[col] = df[col].fillna(default)

    # v1.2: Enrich with reprint-risk features (live = today as anchor)
    release_df = load_release_calendar(db)
    reprint_idx = build_reprint_index(release_df)
    card_rows = db.execute(
        "SELECT id, product_name, set_code, card_number FROM cards WHERE sealed_product = 'N'"
    ).fetchall()
    anchor = pd.Timestamp.now().normalize()
    reprint_recs = []
    for r in card_rows:
        pokemon = extract_pokemon_name(r["product_name"])
        reprint_recs.append({
            "card_id": r["id"],
            **reprint_features_at_date(pokemon, r["set_code"], anchor, reprint_idx),
        })
    rep_df = pd.DataFrame(reprint_recs).set_index("card_id")
    df = df.join(rep_df, how="left")
    for col in REPRINT_COLUMNS:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].fillna(0.0)

    # v2.1: Enrich with catalyst features (set-release recency)
    set_dates, all_release_dates_sorted = load_set_release_calendar(db)
    catalyst_recs = []
    for r in card_rows:
        catalyst_recs.append({
            "card_id": r["id"],
            **catalyst_features_at_date(
                r["set_code"], anchor, set_dates, all_release_dates_sorted,
            ),
        })
    cat_df = pd.DataFrame(catalyst_recs).set_index("card_id")
    df = df.join(cat_df, how="left")
    for col in CATALYST_COLUMNS:
        if col not in df.columns:
            df[col] = 9999.0 if "days" in col else 0.0
        default = 9999.0 if "days" in col else 0.0
        df[col] = df[col].fillna(default)

    # v2.2: Tournament signal features for live cards
    tournament_df = load_tournament_data(db)
    tour_recs = []
    for r in card_rows:
        cnum = r["card_number"]
        tour_recs.append({
            "card_id": r["id"],
            **tournament_features_at_date(
                r["set_code"] or "",
                str(cnum) if cnum is not None else "",
                anchor,
                tournament_df,
            ),
        })
    tour_df = pd.DataFrame(tour_recs).set_index("card_id")
    df = df.join(tour_df, how="left")
    for col in TOURNAMENT_COLUMNS:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].fillna(0.0)

    logger.info(
        "Built live features for %d cards (v2.2: +%d catalyst +%d tournament)",
        len(df), len(CATALYST_COLUMNS), len(TOURNAMENT_COLUMNS),
    )
    return df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _price_at_offset(
    trailing: pd.DataFrame, anchor: pd.Timestamp, days: int
) -> Optional[float]:
    target = anchor - pd.Timedelta(days=days)
    before = trailing[trailing.index <= target]["psa_10_price"]
    if before.empty:
        return None
    return float(before.iloc[-1])


def _get_mp_feature(
    mp: pd.DataFrame, window: int, col: str
) -> Optional[float]:
    subset = mp[mp["window_days"] == window]
    if subset.empty:
        return None
    return float(subset.iloc[-1][col]) if pd.notna(subset.iloc[-1][col]) else None


def _get_ss_feature(ss: pd.DataFrame) -> Optional[float]:
    if ss.empty:
        return None
    val = ss.iloc[-1]["supply_saturation_index"]
    return float(val) if pd.notna(val) else None


def _get_latest_psa(
    psa: pd.DataFrame, anchor: pd.Timestamp, col: str
) -> Optional[float]:
    before = psa[psa.index <= anchor]
    if before.empty:
        return None
    val = before.iloc[-1][col]
    return float(val) if pd.notna(val) else None
