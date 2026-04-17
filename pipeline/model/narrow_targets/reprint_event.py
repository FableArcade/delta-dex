"""Reprint-event narrow target.

Hypothesis: when a Pokemon card is reprinted (same character in a new set),
the original PSA 10 typically drops 10-25% over the following 30 days as
speculators front-run the supply shock, then mean-reverts partially as
collectors differentiate between the two prints.

Target: 30d forward return on the *original* print after a reprint event
on the same Pokemon name.

Data source: TBD.
  TODO(tier2): reprint announcements are surfaced via:
    1. set release calendar (pipeline.model.reprint_risk.load_release_calendar)
       — already available; triggers on set release date.
    2. TCG social-media scrape (Bulbapedia, PokeBeach, Serebii RSS) — not
       yet wired. When built, land a scraper in pipeline/scrapers/ and
       update `detect()` to read from it.
    3. Manual curation for known flagship reprints (151, Celebrations).

For now, detect() uses the set release calendar as a proxy: every release
triggers a reprint-event for each Pokemon name that appears in *both* the
new set and any older set.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from pipeline.model.features import extract_pokemon_name

logger = logging.getLogger("pipeline.model.narrow_targets.reprint_event")

TARGET_NAME = "reprint_event"
HORIZON_DAYS = 30
MODEL_VERSION = "reprint_event_v0_heuristic"


@dataclass
class ReprintEvent:
    original_card_id: str        # the older print whose price we predict
    new_card_id: str             # the reprint that caused the event
    pokemon: str
    event_date: str              # ISO date the new set released

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def detect(db: sqlite3.Connection, as_of: Optional[str] = None,
           lookback_days: int = 30) -> List[ReprintEvent]:
    """Find reprint events in the trailing `lookback_days` window.

    A reprint event = set released on date D where at least one Pokemon
    name in that set also appears in an older set. We emit one event per
    (old_card, new_card) pair.
    """
    as_of = as_of or dt.date.today().isoformat()
    window_start = (dt.date.fromisoformat(as_of)
                    - dt.timedelta(days=lookback_days)).isoformat()

    rows = db.execute(
        """SELECT c.id, c.product_name, c.set_code, s.release_date
             FROM cards c
             JOIN sets s ON s.set_code = c.set_code
            WHERE c.sealed_product = 'N'
              AND s.release_date IS NOT NULL""",
    ).fetchall()

    cards = []
    for r in rows:
        cards.append({
            "id": r["id"] if hasattr(r, "keys") else r[0],
            "pokemon": extract_pokemon_name(
                r["product_name"] if hasattr(r, "keys") else r[1]),
            "set_code": r["set_code"] if hasattr(r, "keys") else r[2],
            "release_date": r["release_date"] if hasattr(r, "keys") else r[3],
        })

    # Group cards by Pokemon, sort by release date asc.
    by_pokemon: Dict[str, List[Dict[str, Any]]] = {}
    for c in cards:
        by_pokemon.setdefault(c["pokemon"], []).append(c)

    events: List[ReprintEvent] = []
    for pokemon, plist in by_pokemon.items():
        if len(plist) < 2:
            continue
        plist.sort(key=lambda x: x["release_date"] or "")
        # New prints = released in the lookback window
        news = [p for p in plist if p["release_date"]
                and p["release_date"] >= window_start
                and p["release_date"] <= as_of]
        olds = [p for p in plist if p["release_date"]
                and p["release_date"] < window_start]
        for new_card in news:
            for old_card in olds:
                events.append(ReprintEvent(
                    original_card_id=old_card["id"],
                    new_card_id=new_card["id"],
                    pokemon=pokemon,
                    event_date=new_card["release_date"],
                ))
    logger.info("reprint_event.detect: %d events in window %s..%s",
                len(events), window_start, as_of)
    return events


def featurize(db: sqlite3.Connection, event: ReprintEvent) -> Dict[str, Any]:
    """Feature vector for a single reprint event.

    Today: minimal — entry price, pre-event 30d return, rarity rank.
    TODO(tier2): calibrate on historical events once labeled. Candidates:
      - original print's age (days since release)
      - price ratio new/old at event date
      - PSA 10 pop ratio old / (old+new)
      - event magnitude: set size, marketing push proxy
    """
    row = db.execute(
        """SELECT psa_10_price FROM price_history
            WHERE card_id = ? AND date <= ? AND psa_10_price IS NOT NULL
            ORDER BY date DESC LIMIT 1""",
        (event.original_card_id, event.event_date),
    ).fetchone()
    entry = None
    if row is not None:
        entry = row[0] if not hasattr(row, "keys") else row["psa_10_price"]

    pre_30d_row = db.execute(
        """SELECT psa_10_price FROM price_history
            WHERE card_id = ? AND date <= date(?, '-30 days')
              AND psa_10_price IS NOT NULL
            ORDER BY date DESC LIMIT 1""",
        (event.original_card_id, event.event_date),
    ).fetchone()
    pre_30 = None
    if pre_30d_row is not None:
        pre_30 = pre_30d_row[0] if not hasattr(pre_30d_row, "keys") else pre_30d_row["psa_10_price"]

    ret_30d_pre = None
    if entry and pre_30 and pre_30 > 0:
        ret_30d_pre = (entry / pre_30) - 1

    return {
        "entry_price": entry,
        "ret_30d_pre_event": ret_30d_pre,
        "pokemon": event.pokemon,
        "new_card_id": event.new_card_id,
    }


def predict(features: Dict[str, Any]) -> Optional[float]:
    """Predict 30d forward return.

    Today: returns None. TODO(tier2): calibrate on historical events once
    labeled. Placeholder heuristic kept in the body as a starting prior.
    """
    # Placeholder prior: modest negative reaction, but we don't trust it
    # enough to write it. Return None so the pipeline stores a null
    # predicted_return and the UI can flag "awaiting calibration".
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
                TARGET_NAME, ev.original_card_id, ev.event_date,
                HORIZON_DAYS, pred, None,
                json.dumps({**feats, "event": ev.as_dict()}, default=str),
                MODEL_VERSION,
            ),
        )
        written += 1
    db.commit()
    return {"target": TARGET_NAME, "events": len(events),
            "rows_written": written, "as_of": as_of}
