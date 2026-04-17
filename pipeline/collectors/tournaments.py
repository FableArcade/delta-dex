"""Tournament top-cut collector — limitlesstcg.com API.

Populates `tournament_appearances` table with per-card competitive-play
signal. The hypothesis: cards that appear frequently in top-cuts of recent
tournaments have active player demand, not just collector demand.

API shape:
  GET /api/tournaments?game=PTCG&limit=N       → list of recent tournaments
  GET /api/tournaments/{id}/standings          → full standings with decklists
      returns: [{placing, player, decklist: {pokemon: [{set, number, name, count}]}, ...}]

Our signal per (set_code, card_number):
  - total appearances across tournaments in last N days
  - appearances in top-8 / top-16 (weighted by competitive strength)
  - distinct tournaments where this card appeared

Matches to Delta's cards table via (set_code, card_number). Limitless
uses the same set codes as pokemontcg.io which should match Delta's
set codes for modern PTCG (MEW, SCR, SSP, TWM, etc.).

Run: python -m pipeline.collectors.tournaments
"""

from __future__ import annotations

import datetime as dt
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import httpx

from db.connection import get_db

logger = logging.getLogger("collector.tournaments")

API_BASE = "https://play.limitlesstcg.com/api"
USER_AGENT = "DeltaDex/2.1 (PokeDelta analytics)"
REQUEST_TIMEOUT = 20
RATE_LIMIT_DELAY = 0.3  # be nice, 3 req/sec


def _get(client: httpx.Client, path: str, **params) -> Optional[dict]:
    try:
        r = client.get(f"{API_BASE}{path}", params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        logger.warning("GET %s returned %d", path, r.status_code)
    except Exception as e:
        logger.warning("GET %s failed: %s", path, e)
    return None


def _ensure_table(db) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS tournament_appearances (
            set_code      TEXT NOT NULL,
            card_number   TEXT NOT NULL,
            tournament_id TEXT NOT NULL,
            tournament_date TEXT NOT NULL,
            player_count  INTEGER,
            placing       INTEGER,
            copies        INTEGER,
            PRIMARY KEY (set_code, card_number, tournament_id, placing)
        )
        """
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_tournament_apps_date ON tournament_appearances(tournament_date)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_tournament_apps_card ON tournament_appearances(set_code, card_number, tournament_date)"
    )
    db.commit()


def fetch_recent_tournaments(
    client: httpx.Client,
    days_back: int = 180,
    min_players: int = 50,
    limit_per_page: int = 100,
) -> List[dict]:
    """Fetch recent PTCG tournaments, filtered by size."""
    cutoff = (dt.datetime.utcnow() - dt.timedelta(days=days_back)).strftime("%Y-%m-%d")
    # API paginates via limit + time filter
    params = {"game": "PTCG", "limit": limit_per_page}
    data = _get(client, "/tournaments", **params)
    if not data:
        return []
    filtered = [
        t for t in data
        if t.get("players", 0) >= min_players
        and t.get("date", "") >= cutoff
    ]
    logger.info(
        "Found %d PTCG tournaments in last %dd with ≥%d players "
        "(from %d total returned)",
        len(filtered), days_back, min_players, len(data),
    )
    return filtered


def collect_tournament(
    client: httpx.Client,
    tournament: dict,
    db,
) -> int:
    """Fetch standings + decklists for a tournament, upsert card appearances."""
    tid = tournament["id"]
    tdate = tournament.get("date", "")[:10]  # YYYY-MM-DD
    players = tournament.get("players", 0)

    standings = _get(client, f"/tournaments/{tid}/standings")
    time.sleep(RATE_LIMIT_DELAY)
    if not isinstance(standings, list):
        return 0

    rows_inserted = 0
    for entry in standings:
        placing = entry.get("placing")
        if placing is None or placing > 32:
            continue  # top-32 cap — anything deeper isn't meta signal
        decklist = entry.get("decklist")
        if not isinstance(decklist, dict):
            continue
        pokemon = decklist.get("pokemon") or []

        for card in pokemon:
            set_code = card.get("set")
            number = card.get("number")
            count = card.get("count", 1)
            if not set_code or not number:
                continue
            try:
                db.execute(
                    """INSERT OR REPLACE INTO tournament_appearances
                       (set_code, card_number, tournament_id, tournament_date,
                        player_count, placing, copies)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (set_code, str(number), tid, tdate, players, placing, count),
                )
                rows_inserted += 1
            except Exception as e:
                logger.debug("Upsert failed for %s/%s: %s", set_code, number, e)

    db.commit()
    return rows_inserted


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    with httpx.Client(headers={"User-Agent": USER_AGENT}) as client:
        with get_db() as db:
            _ensure_table(db)
            tourns = fetch_recent_tournaments(client, days_back=180, min_players=50)
            if not tourns:
                logger.warning("No tournaments found")
                return 1

            total_inserted = 0
            for i, t in enumerate(tourns):
                inserted = collect_tournament(client, t, db)
                total_inserted += inserted
                if (i + 1) % 10 == 0:
                    logger.info(
                        "Progress %d/%d: %d appearances so far",
                        i + 1, len(tourns), total_inserted,
                    )

            # Diagnostics
            cnt = db.execute(
                "SELECT COUNT(*) AS c FROM tournament_appearances"
            ).fetchone()["c"]
            distinct_cards = db.execute(
                "SELECT COUNT(*) AS c FROM "
                "(SELECT DISTINCT set_code, card_number FROM tournament_appearances)"
            ).fetchone()["c"]

    logger.info("=== Tournament collection done ===")
    logger.info("Inserted: %d appearances this run", total_inserted)
    logger.info("Total rows in tournament_appearances: %d", cnt)
    logger.info("Distinct (set, number) cards tracked: %d", distinct_cards)
    return 0


if __name__ == "__main__":
    sys.exit(main())
