"""Populate ebay_history for the liquid universe (~1,000 cards) only.

Wraps EBayCollector's collect() but scopes to cards in the investable
universe — avoids burning API budget on thin-liquidity cards the model
doesn't score anyway.

At 2 API calls per card (active + ended listings), this is ~2,000 calls
for the full liquid universe, ~30% of the 6,500/day budget.

Run: python -m scripts.populate_ebay_liquid
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Load .env before importing settings-dependent modules
from dotenv import dotenv_values
env = dotenv_values(Path(__file__).resolve().parent.parent / ".env")
for k, v in env.items():
    if v:
        os.environ[k] = v

from db.connection import get_db
from pipeline.collectors.ebay import EBayCollector
from pipeline.model.features import build_live_features
from pipeline.model.liquid_universe import filter_to_liquid_universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("populate_ebay_liquid")


class LiquidEBayCollector(EBayCollector):
    """EBayCollector scoped to the liquid universe only."""

    def __init__(self, liquid_card_ids: set[str]) -> None:
        super().__init__()
        self._liquid_card_ids = liquid_card_ids

    def get_cards(self) -> list[dict]:
        """Override: only return cards in the liquid universe."""
        all_cards = super().get_cards()
        filtered = [c for c in all_cards if c["id"] in self._liquid_card_ids]
        self.logger.info(
            "Filtered %d total cards → %d liquid cards",
            len(all_cards), len(filtered),
        )
        return filtered


def main() -> int:
    # Identify liquid universe from current live features
    logger.info("Building live features to identify liquid universe...")
    with get_db() as db:
        live_df = build_live_features(db)

    liquid_df = filter_to_liquid_universe(live_df)
    liquid_ids = set(str(cid) for cid in liquid_df.index.tolist())
    logger.info("Liquid universe: %d card IDs", len(liquid_ids))

    # Run the collector scoped to those cards
    coll = LiquidEBayCollector(liquid_card_ids=liquid_ids)
    logger.info(
        "Collector mode: %s  (budget=%d/day)",
        "SANDBOX" if coll._is_sandbox else "PRODUCTION",
        coll.DAILY_BUDGET,
    )

    today = dt.date.today().isoformat()
    result = coll.collect(today)

    logger.info("=== Done ===")
    logger.info("Processed: %d", result.get("processed", 0))
    logger.info("Errors: %d", result.get("errors", 0))
    logger.info("API calls used: %d", coll._calls_today)

    # Verify ebay_history has new rows
    with get_db() as db:
        n_today = db.execute(
            "SELECT COUNT(*) AS c FROM ebay_history WHERE date = ?",
            (today,),
        ).fetchone()["c"]
        n_total = db.execute(
            "SELECT COUNT(*) AS c FROM ebay_history"
        ).fetchone()["c"]
        n_cards = db.execute(
            "SELECT COUNT(DISTINCT card_id) AS c FROM ebay_history WHERE date = ?",
            (today,),
        ).fetchone()["c"]

    logger.info("ebay_history today: %d rows across %d cards", n_today, n_cards)
    logger.info("ebay_history total: %d rows (all time)", n_total)

    return 0 if result.get("errors", 0) < 10 else 1


if __name__ == "__main__":
    sys.exit(main())
