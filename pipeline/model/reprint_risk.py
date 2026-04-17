"""Reprint-risk features.

Supply shock from a reprint is the #1 structural risk for singles.
A Charizard holding $400 can drop 30% overnight when a new set drops
with a "Charizard ex" variant. The base model has no awareness of this.

Signals derived from cards + sets (no scraper needed, uses existing data):

- reprint_count_trailing_365d: count of *other* cards featuring the same
  Pokemon whose set released in trailing 365 days before anchor. Direct
  measure of recent supply competition for the same collector demand.
- days_since_last_reprint: days between anchor and the most recent
  same-Pokemon release. Exponential decay — shock dissipates over time.
- total_same_pokemon_cards: lifetime count of same-Pokemon variants.
  Proxy for "is this character so oversaturated the ceiling is low?"

Reprint is scoped by Pokemon name (extracted via features.extract_pokemon_name)
so "Charizard ex 199/165" counts as a Charizard reprint relative to an
earlier Base Set Charizard. Cross-set match only — same-set variants
(holo vs reverse holo of same card) aren't counted as reprints.
"""

from __future__ import annotations

import math
import sqlite3
from typing import Dict, List, Tuple

import pandas as pd

REPRINT_WINDOW_DAYS = 365
DECAY_HALF_LIFE_DAYS = 180.0  # shock halves every 6 months


def load_release_calendar(db: sqlite3.Connection) -> pd.DataFrame:
    """Load (card_id, pokemon_name, set_release_date) for all cards.

    Returns one row per card, with the Pokemon name parsed and the
    set release date joined in. Sorted by release_date.
    """
    rows = db.execute(
        """
        SELECT c.id AS card_id,
               c.product_name,
               c.set_code,
               s.release_date
        FROM cards c
        JOIN sets s ON s.set_code = c.set_code
        WHERE c.sealed_product = 'N'
          AND s.release_date IS NOT NULL
        """
    ).fetchall()

    # Lazy import to avoid circular dependency with features.py
    from pipeline.model.features import extract_pokemon_name

    recs = []
    for r in rows:
        try:
            rel = pd.to_datetime(r["release_date"])
        except Exception:
            continue
        pokemon = extract_pokemon_name(r["product_name"])
        if not pokemon:
            continue
        recs.append({
            "card_id": r["card_id"],
            "set_code": r["set_code"],
            "pokemon": pokemon,
            "release_date": rel,
        })
    df = pd.DataFrame(recs)
    return df.sort_values("release_date").reset_index(drop=True)


def build_reprint_index(
    release_df: pd.DataFrame,
) -> Dict[str, List[Tuple[pd.Timestamp, str]]]:
    """Build {pokemon_name -> [(release_date, set_code), ...]} for fast lookup."""
    idx: Dict[str, List[Tuple[pd.Timestamp, str]]] = {}
    for _, row in release_df.iterrows():
        idx.setdefault(row["pokemon"], []).append(
            (row["release_date"], row["set_code"])
        )
    return idx


def reprint_features_at_date(
    pokemon: str,
    own_set_code: str,
    anchor_date: pd.Timestamp,
    reprint_idx: Dict[str, List[Tuple[pd.Timestamp, str]]],
) -> Dict[str, float]:
    """Compute reprint-risk features for a (card, anchor_date) sample.

    Only counts reprints from OTHER sets released <= anchor_date,
    and only within the trailing 365d window for count features.
    Lifetime count uses all trailing releases.
    """
    releases = reprint_idx.get(pokemon, [])
    # Filter: strictly before anchor, other sets
    past = [
        (d, code) for (d, code) in releases
        if d <= anchor_date and code != own_set_code
    ]
    if not past:
        return {
            "reprint_count_365d": 0.0,
            "reprint_shock_decay": 0.0,
            "total_same_pokemon_cards": 0.0,
        }

    cutoff = anchor_date - pd.Timedelta(days=REPRINT_WINDOW_DAYS)
    recent = [d for (d, _) in past if d >= cutoff]
    reprint_count_365d = float(len(recent))

    # Exponential decay: most recent reprint dominates.
    # shock = 2^(-days_since / half_life)
    most_recent = max(d for (d, _) in past)
    days_since = (anchor_date - most_recent).days
    shock = math.pow(0.5, max(days_since, 0) / DECAY_HALF_LIFE_DAYS)

    return {
        "reprint_count_365d": reprint_count_365d,
        "reprint_shock_decay": shock,
        "total_same_pokemon_cards": float(len(past)),
    }


REPRINT_COLUMNS = [
    "reprint_count_365d",
    "reprint_shock_decay",
    "total_same_pokemon_cards",
]
