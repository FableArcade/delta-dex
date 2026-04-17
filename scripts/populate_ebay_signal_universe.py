"""Populate ebay_history for every card that appears in the Metrics Results
section of the app — the "signal universe."

Scope: any card with prior supply_saturation, market_pressure, or ebay_history
rows. That's the set the UI surfaces on the cards leaderboard / card detail,
and it's the set the model scores. Skips cards already collected today to
avoid double-spending API budget.

At 2 calls per card, the ~2,400-card signal universe fits comfortably inside
the 6,500/day Browse-API budget, leaving headroom for same-day re-runs.

Run: python -m scripts.populate_ebay_signal_universe
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import dotenv_values
env = dotenv_values(Path(__file__).resolve().parent.parent / ".env")
for k, v in env.items():
    if v:
        os.environ[k] = v

from db.connection import get_db
from pipeline.collectors.ebay_async import AsyncEBayCollector as EBayCollector
from pipeline.compute.market_pressure import compute_market_pressure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("populate_ebay_signal_universe")


def _signal_universe_ids(today: str) -> tuple[set[str], int]:
    """Return card IDs in the signal universe that still need today's data,
    along with the count already done today."""
    with get_db() as db:
        all_signal = {
            r["card_id"] for r in db.execute(
                """
                SELECT DISTINCT card_id FROM supply_saturation WHERE mode='observed'
                UNION
                SELECT DISTINCT card_id FROM market_pressure WHERE mode='observed'
                UNION
                SELECT DISTINCT card_id FROM ebay_history
                """
            ).fetchall()
        }
        done_today = {
            r["card_id"] for r in db.execute(
                "SELECT DISTINCT card_id FROM ebay_history WHERE date = ?",
                (today,),
            ).fetchall()
        }
    return all_signal - done_today, len(done_today)


class SignalUniverseEBayCollector(EBayCollector):
    """EBayCollector scoped to the signal universe, excluding today's already-done cards."""

    def __init__(self, target_card_ids: set[str]) -> None:
        super().__init__()
        self._target_card_ids = target_card_ids

    def get_cards(self) -> list[dict]:
        all_cards = super().get_cards()
        filtered = [c for c in all_cards if c["id"] in self._target_card_ids]
        self.logger.info(
            "Filtered %d total cards → %d signal-universe cards still needing today's data",
            len(all_cards), len(filtered),
        )
        return filtered


def main() -> int:
    today = dt.date.today().isoformat()
    target_ids, already_done = _signal_universe_ids(today)
    logger.info(
        "Signal universe to collect: %d cards (already done today: %d)",
        len(target_ids), already_done,
    )
    if not target_ids:
        logger.info("Nothing to do.")
        return 0

    coll = SignalUniverseEBayCollector(target_card_ids=target_ids)
    logger.info(
        "Collector mode: %s  (budget=%d/day)",
        "SANDBOX" if coll._is_sandbox else "PRODUCTION",
        coll.DAILY_BUDGET,
    )

    result = coll.collect(today)

    logger.info("=== Done ===")
    logger.info("Processed: %d", result.get("processed", 0))
    logger.info("Errors: %d", result.get("errors", 0))
    logger.info("API calls used: %d", coll._calls_today)

    with get_db() as db:
        n_today = db.execute(
            "SELECT COUNT(*) AS c FROM ebay_history WHERE date = ?",
            (today,),
        ).fetchone()["c"]
        n_cards = db.execute(
            "SELECT COUNT(DISTINCT card_id) AS c FROM ebay_history WHERE date = ?",
            (today,),
        ).fetchone()["c"]

    logger.info("ebay_history today: %d rows across %d distinct cards", n_today, n_cards)

    # Recompute market_pressure + supply_saturation for all cards with eBay data
    logger.info("Recomputing market_pressure + supply_saturation...")
    with get_db() as db:
        all_ebay_ids = [r["card_id"] for r in db.execute(
            "SELECT DISTINCT card_id FROM ebay_history"
        ).fetchall()]
    ok = 0
    failed = 0
    with get_db() as db:
        for cid in all_ebay_ids:
            try:
                compute_market_pressure(db, cid)
                ok += 1
            except Exception:
                failed += 1
    logger.info("Signals recomputed: %d ok, %d failed", ok, failed)

    return 0 if result.get("errors", 0) < 10 else 1


if __name__ == "__main__":
    sys.exit(main())
