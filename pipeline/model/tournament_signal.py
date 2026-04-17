"""Tournament-play signal features — Signal Class 3 (sentiment/competitive demand).

Converts tournament_appearances rows (one per (card, tournament, placing))
into per-(card, anchor_date) features.

Signals computed:
  tournament_apps_30d    — count of tournament appearances in trailing 30d
  tournament_apps_90d    — count in trailing 90d
  top8_apps_90d          — appearances at placing ≤ 8 (top-cut signal, higher weight)
  weighted_meta_share_90d — sum of copies × sqrt(player_count) / sqrt(max players)
                           — normalizes for tournament size so a 200-player major
                             counts more than a 50-player local

No leakage: all features use tournaments with date strictly ≤ anchor_date.

Integrates into features.py the same way catalyst.py does.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np

logger = logging.getLogger("pipeline.model.tournament_signal")

TOURNAMENT_COLUMNS = [
    "tournament_apps_30d",
    "tournament_apps_90d",
    "top8_apps_90d",
    "weighted_meta_share_90d",
]


def load_tournament_data(db: sqlite3.Connection) -> pd.DataFrame:
    """Returns a DataFrame of all tournament appearances with parsed dates.

    Columns: set_code, card_number, tournament_date (Timestamp), placing,
    copies, player_count.
    """
    try:
        rows = db.execute(
            """
            SELECT set_code, card_number, tournament_id, tournament_date,
                   placing, copies, player_count
            FROM tournament_appearances
            """
        ).fetchall()
    except sqlite3.OperationalError:
        # Table doesn't exist — tournament collector not run yet
        logger.warning("tournament_appearances table missing — all features will be 0")
        return pd.DataFrame(columns=[
            "set_code", "card_number", "tournament_date", "placing",
            "copies", "player_count",
        ])

    if not rows:
        return pd.DataFrame(columns=[
            "set_code", "card_number", "tournament_date", "placing",
            "copies", "player_count",
        ])

    df = pd.DataFrame([dict(r) for r in rows])
    df["tournament_date"] = pd.to_datetime(df["tournament_date"])
    return df


def tournament_features_at_date(
    card_set_code: str,
    card_number: str,
    anchor_date: pd.Timestamp,
    tournament_df: pd.DataFrame,
) -> Dict[str, float]:
    """Compute trailing-window tournament signal for a single (card, anchor)."""
    if tournament_df.empty or not card_set_code or not card_number:
        return {c: 0.0 for c in TOURNAMENT_COLUMNS}

    # Filter to this card's tournaments on or before anchor_date
    mask = (
        (tournament_df["set_code"] == card_set_code) &
        (tournament_df["card_number"].astype(str) == str(card_number)) &
        (tournament_df["tournament_date"] <= anchor_date)
    )
    sub = tournament_df[mask]
    if sub.empty:
        return {c: 0.0 for c in TOURNAMENT_COLUMNS}

    cutoff_30 = anchor_date - pd.Timedelta(days=30)
    cutoff_90 = anchor_date - pd.Timedelta(days=90)

    sub_30 = sub[sub["tournament_date"] >= cutoff_30]
    sub_90 = sub[sub["tournament_date"] >= cutoff_90]

    apps_30 = float(len(sub_30))
    apps_90 = float(len(sub_90))
    top8_90 = float(len(sub_90[sub_90["placing"] <= 8]))

    # Weighted meta share: larger tournaments weigh more (sqrt scaling)
    if len(sub_90) > 0:
        pc = sub_90["player_count"].fillna(50).clip(lower=1)
        copies = sub_90["copies"].fillna(1).clip(lower=0)
        weighted = float((copies * np.sqrt(pc) / np.sqrt(500.0)).sum())
    else:
        weighted = 0.0

    return {
        "tournament_apps_30d": apps_30,
        "tournament_apps_90d": apps_90,
        "top8_apps_90d": top8_90,
        "weighted_meta_share_90d": weighted,
    }


def build_card_tournament_lookup(
    db: sqlite3.Connection,
) -> pd.DataFrame:
    """Returns a DataFrame indexed by card_id with (set_code, card_number) columns
    for quick lookup during feature building.
    """
    rows = db.execute(
        "SELECT id AS card_id, set_code, card_number FROM cards WHERE sealed_product = 'N'"
    ).fetchall()
    df = pd.DataFrame([dict(r) for r in rows])
    return df.set_index("card_id")
