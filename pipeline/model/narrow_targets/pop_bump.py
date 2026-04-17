"""PSA pop-bump narrow target.

Hypothesis: a sudden jump in PSA 10 population (week-over-week >= 15%
or >= 25 absolute submissions on a card where base was <500) signals a
grading wave. Historically these waves compress PSA 10 premium 5-15%
over the next 30 days as newly-slabbed supply hits eBay.

Source: psa_pop_history (already populated by existing scraper).

Event: pop_bump_{card_id, detection_date}
Prediction target: 30d PSA 10 forward return.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

logger = logging.getLogger("pipeline.model.narrow_targets.pop_bump")

TARGET_NAME = "pop_bump"
HORIZON_DAYS = 30
MODEL_VERSION = "pop_bump_v0_heuristic"

# Bump thresholds — deliberately loose scaffold numbers. Calibrate once we
# have a labeled set.
MIN_PCT_JUMP = 0.15        # 15% week-over-week
MIN_ABS_JUMP = 25          # at least 25 new PSA 10s
SMALL_BASE_THRESHOLD = 500  # below this, abs_jump alone can qualify


@dataclass
class PopBumpEvent:
    card_id: str
    event_date: str
    prior_pop: int
    new_pop: int
    pct_jump: float
    abs_jump: int

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def detect(db: sqlite3.Connection, as_of: Optional[str] = None,
           lookback_days: int = 14) -> List[PopBumpEvent]:
    """Scan psa_pop_history for week-over-week bumps in the trailing window."""
    as_of = as_of or dt.date.today().isoformat()
    window_start = (dt.date.fromisoformat(as_of)
                    - dt.timedelta(days=lookback_days)).isoformat()

    rows = db.execute(
        """SELECT card_id, date, psa_10_base
             FROM psa_pop_history
            WHERE date >= date(?, '-30 days') AND date <= ?
            ORDER BY card_id, date""",
        (window_start, as_of),
    ).fetchall()

    events: List[PopBumpEvent] = []
    by_card: Dict[str, List[Any]] = {}
    for r in rows:
        cid = r["card_id"] if hasattr(r, "keys") else r[0]
        by_card.setdefault(cid, []).append(r)

    for card_id, hist in by_card.items():
        for i in range(1, len(hist)):
            prev = hist[i - 1]
            curr = hist[i]
            prev_date = prev["date"] if hasattr(prev, "keys") else prev[1]
            curr_date = curr["date"] if hasattr(curr, "keys") else curr[1]
            prev_pop = prev["psa_10_base"] if hasattr(prev, "keys") else prev[2]
            curr_pop = curr["psa_10_base"] if hasattr(curr, "keys") else curr[2]
            if curr_date < window_start or curr_date > as_of:
                continue
            if not prev_pop or not curr_pop or prev_pop <= 0:
                continue
            abs_jump = int(curr_pop) - int(prev_pop)
            if abs_jump <= 0:
                continue
            pct_jump = abs_jump / max(1, int(prev_pop))
            qualifies = (
                (pct_jump >= MIN_PCT_JUMP and abs_jump >= MIN_ABS_JUMP)
                or (prev_pop < SMALL_BASE_THRESHOLD and abs_jump >= MIN_ABS_JUMP)
            )
            if not qualifies:
                continue
            events.append(PopBumpEvent(
                card_id=card_id,
                event_date=curr_date,
                prior_pop=int(prev_pop),
                new_pop=int(curr_pop),
                pct_jump=float(pct_jump),
                abs_jump=abs_jump,
            ))
    logger.info("pop_bump.detect: %d events in window %s..%s",
                len(events), window_start, as_of)
    return events


def featurize(db: sqlite3.Connection, event: PopBumpEvent) -> Dict[str, Any]:
    """Minimal feature set. TODO(tier2): calibrate on historical bumps
    once labeled. Candidates: pre-bump 30d return, rolling pop velocity,
    gem_pct shift, eBay demand_pressure_30d at event date."""
    price_row = db.execute(
        """SELECT psa_10_price FROM price_history
            WHERE card_id = ? AND date <= ? AND psa_10_price IS NOT NULL
            ORDER BY date DESC LIMIT 1""",
        (event.card_id, event.event_date),
    ).fetchone()
    entry = None
    if price_row is not None:
        entry = price_row[0] if not hasattr(price_row, "keys") else price_row["psa_10_price"]

    return {
        "entry_price": entry,
        "prior_pop": event.prior_pop,
        "new_pop": event.new_pop,
        "pct_jump": event.pct_jump,
        "abs_jump": event.abs_jump,
    }


def predict(features: Dict[str, Any]) -> Optional[float]:
    """TODO(tier2): calibrate on historical pop bumps once labeled.
    Scaffold returns None so the prediction is stored but explicitly
    flagged as un-calibrated. Do NOT trade on this output today."""
    return None


def run(db: sqlite3.Connection, as_of: Optional[str] = None) -> Dict[str, Any]:
    as_of = as_of or dt.date.today().isoformat()
    events = detect(db, as_of)
    written = 0
    for ev in events:
        feats = featurize(db, ev)
        pred = predict(feats)
        db.execute(
            """INSERT OR IGNORE INTO narrow_target_predictions
               (target_name, card_id, event_date, horizon_days,
                predicted_return, confidence, event_features_json,
                model_version)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                TARGET_NAME, ev.card_id, ev.event_date,
                HORIZON_DAYS, pred, None,
                json.dumps({**feats, "event": ev.as_dict()}, default=str),
                MODEL_VERSION,
            ),
        )
        written += 1
    db.commit()
    return {"target": TARGET_NAME, "events": len(events),
            "rows_written": written, "as_of": as_of}
