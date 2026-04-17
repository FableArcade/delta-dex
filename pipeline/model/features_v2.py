"""
v2 feature extension built on top of v1_3's FEATURE_COLUMNS.

New features added per Analytics Taste principles derived from Round 5 audit:

  P7 — Cross-sectional rank features (scale-invariant, cohort-relative signal):
    - price_rank_in_set / in_era / in_rarity
    - momentum_rank_in_era
    - scarcity_rank_in_set

  Signal Class 4 — Catalyst features (data already in DB):
    - days_since_set_release
    - days_to_next_set_release
    - set_release_window_30d

  Fundamental (Signal Class 2) — PSA population velocity placeholders:
    - psa_pop_growth_30d
    - psa_pop_growth_90d
    (psa_pop_history only covers 2026-02 → 2026-03 currently; schema-ready
     for the daily scrape backfill. All-NaN for historical training windows;
     LightGBM handles that fine.)

  Signal Class 3 — Attention/sentiment placeholders (NaN today, scrape-ready):
    - reddit_mentions_7d_z
    - reddit_velocity_30d
    - google_trends_30d_z
    - tournament_top8_30d
    - tournament_meta_share
    - anime_featured_30d
    - anime_upcoming_30d
    - youtube_videos_30d

  Total v1_3 FEATURE_COLUMNS (31) + v2 additions (18) = 49 features in v2.

Design choice: extend, don't replace. v1_3's build_training_dataset() is kept
as-is (it's in production via predict.py + the report-card endpoint). v2 calls
it first, then post-processes to attach the new columns. This preserves the
baseline for honest A/B.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from pipeline.model.features import (
    FEATURE_COLUMNS as V1_FEATURE_COLUMNS,
    HORIZON_DAYS,
    TARGET_COL,
    build_training_dataset,
)

logger = logging.getLogger("features_v2")


# ---------------------------------------------------------------------------
# v2 feature-name groups (for documentation + promotion-gate reporting)
# ---------------------------------------------------------------------------

V2_RANK_FEATURES = [
    "price_rank_in_set",
    "price_rank_in_era",
    "price_rank_in_rarity",
    "momentum_rank_in_era",
    "scarcity_rank_in_set",
]

V2_CALENDAR_FEATURES = [
    "days_since_set_release",
    "days_to_next_set_release",
    "set_release_window_30d",
]

V2_POP_VELOCITY_FEATURES = [
    "psa_pop_growth_30d",
    "psa_pop_growth_90d",
]

# Placeholders — NaN today, populated by daily scrapers when wired.
V2_SENTIMENT_FEATURES = [
    "reddit_mentions_7d_z",
    "reddit_velocity_30d",
    "google_trends_30d_z",
    "tournament_top8_30d",
    "tournament_meta_share",
    "anime_featured_30d",
    "anime_upcoming_30d",
    "youtube_videos_30d",
]

V2_FEATURE_COLUMNS = (
    list(V1_FEATURE_COLUMNS)
    + V2_RANK_FEATURES
    + V2_CALENDAR_FEATURES
    + V2_POP_VELOCITY_FEATURES
    + V2_SENTIMENT_FEATURES
)


# ---------------------------------------------------------------------------
# Feature-construction helpers
# ---------------------------------------------------------------------------

def _derive_era_from_set_code(set_code: Optional[str]) -> str:
    """Coarse era bucketing for cross-sectional ranks. Uses set_code prefix."""
    if not isinstance(set_code, str):
        return "unknown"
    s = set_code.upper()
    if s.startswith(("BS", "JU", "FO", "RO", "GY", "NG", "N1", "N2", "N3", "N4", "WOT")):
        return "wotc"
    if s.startswith(("EX",)):
        return "ex"
    if s.startswith(("DP", "PL", "HGSS", "HS")):
        return "dp_hgss"
    if s.startswith(("BW", "XY", "SM")):
        return "modern_classic"
    if s.startswith(("SWSH", "SV", "SCAR", "POR", "ASC", "PFL", "MEG", "WHT", "BLK", "PRE", "DES", "JTG", "TEF", "TWM", "SFA", "SCR", "PAF", "PAL", "OBF", "MEW")):
        return "modern"
    return "other"


def _load_card_metadata(db: sqlite3.Connection) -> pd.DataFrame:
    """One row per card with set_code, rarity_name, and derived era."""
    q = """
        SELECT c.id AS card_id,
               c.set_code,
               c.rarity_name,
               c.rarity_code,
               s.release_date AS set_release_date
        FROM cards c
        LEFT JOIN sets s ON s.set_code = c.set_code
    """
    df = pd.read_sql_query(q, db)
    df["set_release_date"] = pd.to_datetime(df["set_release_date"], errors="coerce")
    df["era"] = df["set_code"].apply(_derive_era_from_set_code)
    return df


def _compute_set_release_calendar(db: sqlite3.Connection) -> pd.DataFrame:
    """Return all set release dates for 'days_to_next_set_release' lookups."""
    s = pd.read_sql_query(
        "SELECT set_code, release_date FROM sets WHERE release_date IS NOT NULL "
        "ORDER BY release_date",
        db,
    )
    s["release_date"] = pd.to_datetime(s["release_date"], errors="coerce")
    s = s.dropna(subset=["release_date"]).sort_values("release_date").reset_index(drop=True)
    return s


def _days_to_next_release_after(anchor: pd.Timestamp, releases: np.ndarray) -> Optional[float]:
    """Days from anchor to the next set release strictly AFTER anchor.
    Returns None if anchor is beyond the last known release."""
    idx = int(np.searchsorted(releases, anchor.to_datetime64(), side="right"))
    if idx >= len(releases):
        return None
    return float((pd.Timestamp(releases[idx]) - anchor).days)


def _compute_psa_pop_velocity(db: sqlite3.Connection, anchor: pd.Timestamp,
                              card_ids: List[int],
                              days: int) -> Dict[int, float]:
    """For each card, compute (pop_now / pop_{days}_ago) - 1, using nearest
    snapshots on either side. Returns {card_id: growth} where data exists.

    psa_pop_history currently only spans 2026-02 to 2026-03, so this is
    schema-ready but will be mostly-NaN on historical training windows.
    """
    if not card_ids:
        return {}
    q = """
        SELECT card_id, date, psa_10_base AS psa_10_pop
        FROM psa_pop_history
        WHERE date <= ? AND psa_10_base IS NOT NULL
    """
    df = pd.read_sql_query(q, db, params=(anchor.date().isoformat(),))
    if df.empty:
        return {}
    df["date"] = pd.to_datetime(df["date"])
    cutoff = anchor - pd.Timedelta(days=days)
    out: Dict[int, float] = {}
    for cid, g in df.groupby("card_id"):
        g = g.sort_values("date")
        now = g[g["date"] <= anchor]
        past = g[g["date"] <= cutoff]
        if now.empty or past.empty:
            continue
        pop_now = float(now.iloc[-1]["psa_10_pop"])
        pop_past = float(past.iloc[-1]["psa_10_pop"])
        if pop_past <= 0:
            continue
        out[int(cid)] = pop_now / pop_past - 1.0
    return out


# ---------------------------------------------------------------------------
# v2 training dataset
# ---------------------------------------------------------------------------

def build_training_dataset_v2(db: sqlite3.Connection,
                              compute_pop_velocity: bool = True,
                              use_cache: bool = True) -> pd.DataFrame:
    """Return v1_3's training dataset extended with v2 feature columns.

    Cross-sectional ranks are computed per-anchor-date within cohorts
    (set / era / rarity_name). Calendar features use sets.release_date.
    Sentiment features are NaN (schema-ready). PSA pop velocity features
    are mostly NaN until the scrape backfills psa_pop_history.

    First run: builds from DB (~10-15 min) and writes a parquet cache keyed
    on the DB's last-modified timestamp. Subsequent runs with the same DB
    state read the parquet in seconds.
    """
    import os
    from pathlib import Path
    project_root = Path(__file__).resolve().parent.parent.parent
    cache_dir = project_root / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    db_path = project_root / "data" / "pokemon.db"
    db_mtime = int(os.path.getmtime(db_path)) if db_path.exists() else 0
    cache_key = f"v2_training_{db_mtime}_{int(compute_pop_velocity)}.parquet"
    cache_path = cache_dir / cache_key

    if use_cache and cache_path.exists():
        logger.info("v2: reading cached dataset %s", cache_path.name)
        try:
            return pd.read_parquet(cache_path)
        except Exception as e:
            logger.warning("cache read failed (%s); rebuilding", e)

    logger.info("v2: building base (v1_3) training dataset ...")
    df = build_training_dataset(db)
    if df.empty:
        return df

    logger.info("v2: loading card metadata + set calendar ...")
    meta = _load_card_metadata(db)
    df = df.merge(meta[["card_id", "set_code", "rarity_name", "era",
                        "set_release_date"]],
                  on="card_id", how="left")
    df["anchor_date"] = pd.to_datetime(df["anchor_date"])

    # --- Calendar features ---
    df["days_since_set_release"] = (
        df["anchor_date"] - df["set_release_date"]
    ).dt.days.astype("float64")

    sets_df = _compute_set_release_calendar(db)
    release_array = sets_df["release_date"].values if not sets_df.empty else np.array([], dtype="datetime64[ns]")
    df["days_to_next_set_release"] = [
        _days_to_next_release_after(a, release_array) for a in df["anchor_date"]
    ]
    df["set_release_window_30d"] = df["days_to_next_set_release"].apply(
        lambda d: 1.0 if (d is not None and 0 <= d <= 30) else 0.0
    )

    # --- Cross-sectional rank features per-anchor-date ---
    # Rank within cohort gives scale-invariant "is this card top-quartile in
    # its peer group?" signal — tree models love these.
    logger.info("v2: computing cross-sectional rank features ...")

    def _pct_rank_group(values: pd.Series) -> pd.Series:
        return values.rank(pct=True, method="average")

    # use log_price as the price feature (already engineered in v1_3)
    df["price_rank_in_set"] = df.groupby(["anchor_date", "set_code"])["log_price"].transform(_pct_rank_group)
    df["price_rank_in_era"] = df.groupby(["anchor_date", "era"])["log_price"].transform(_pct_rank_group)
    df["price_rank_in_rarity"] = df.groupby(["anchor_date", "rarity_name"])["log_price"].transform(_pct_rank_group)
    # Momentum rank uses ret_30d (v1_3 column)
    df["momentum_rank_in_era"] = df.groupby(["anchor_date", "era"])["ret_30d"].transform(_pct_rank_group)
    # Scarcity rank = 1 / psa_10_pop percentile within set (smaller pop → higher rank)
    inv_pop = 1.0 / df["psa_10_pop"].replace(0, np.nan)
    df["scarcity_rank_in_set"] = inv_pop.groupby([df["anchor_date"], df["set_code"]]).transform(_pct_rank_group)

    # --- PSA pop velocity (mostly NaN until backfill) ---
    if compute_pop_velocity:
        logger.info("v2: computing PSA pop velocity (sparse until backfill) ...")
        # Batch per anchor_date
        pop30: Dict[Tuple[pd.Timestamp, int], float] = {}
        pop90: Dict[Tuple[pd.Timestamp, int], float] = {}
        for anchor, grp in df.groupby("anchor_date"):
            cids = grp["card_id"].astype(int).tolist()
            d30 = _compute_psa_pop_velocity(db, anchor, cids, 30)
            d90 = _compute_psa_pop_velocity(db, anchor, cids, 90)
            for cid, v in d30.items():
                pop30[(anchor, cid)] = v
            for cid, v in d90.items():
                pop90[(anchor, cid)] = v
        df["psa_pop_growth_30d"] = [
            pop30.get((a, int(c)), np.nan)
            for a, c in zip(df["anchor_date"], df["card_id"])
        ]
        df["psa_pop_growth_90d"] = [
            pop90.get((a, int(c)), np.nan)
            for a, c in zip(df["anchor_date"], df["card_id"])
        ]
    else:
        df["psa_pop_growth_30d"] = np.nan
        df["psa_pop_growth_90d"] = np.nan

    # --- Sentiment placeholders (NaN until scrape pipeline lands) ---
    for col in V2_SENTIMENT_FEATURES:
        df[col] = np.nan

    # Ensure all v2 feature columns exist + are numeric-coerced
    for col in V2_FEATURE_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info("v2: dataset ready — %d rows × %d v2 features", len(df), len(V2_FEATURE_COLUMNS))

    if use_cache:
        try:
            df.to_parquet(cache_path, index=False)
            logger.info("v2: cached dataset → %s", cache_path.name)
        except Exception as e:
            logger.warning("cache write failed (%s); continuing without cache", e)

    return df


__all__ = [
    "V1_FEATURE_COLUMNS",
    "V2_FEATURE_COLUMNS",
    "V2_RANK_FEATURES",
    "V2_CALENDAR_FEATURES",
    "V2_POP_VELOCITY_FEATURES",
    "V2_SENTIMENT_FEATURES",
    "HORIZON_DAYS",
    "TARGET_COL",
    "build_training_dataset_v2",
]
