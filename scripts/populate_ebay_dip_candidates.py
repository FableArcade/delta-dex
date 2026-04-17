"""Populate ebay_history for Buy the Dip candidates not yet covered today.

Targets cards that are 20%+ off their ATH PSA 10 price and have PSA 10 > $10,
excluding cards already collected today. After collection, recomputes
market_pressure + supply_saturation for all cards with ebay_history.

Run: python -m scripts.populate_ebay_dip_candidates
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
logger = logging.getLogger("populate_ebay_dip")


def _dip_candidate_ids(today: str) -> tuple[set[str], int]:
    with get_db() as db:
        candidates = {
            r["card_id"] for r in db.execute(
                """
                SELECT c.id AS card_id
                FROM cards c
                JOIN price_history ph ON ph.card_id = c.id
                  AND ph.date = (SELECT MAX(date) FROM price_history WHERE card_id = c.id)
                WHERE c.sealed_product = 'N'
                  AND ph.psa_10_price > 10
                  AND (SELECT MAX(psa_10_price) FROM price_history
                       WHERE card_id = c.id AND psa_10_price IS NOT NULL)
                      > ph.psa_10_price * 1.25
                """
            ).fetchall()
        }
        done_today = {
            r["card_id"] for r in db.execute(
                "SELECT DISTINCT card_id FROM ebay_history WHERE date = ?",
                (today,),
            ).fetchall()
        }
    return candidates - done_today, len(done_today)


class DipCandidateEBayCollector(EBayCollector):
    def __init__(self, target_card_ids: set[str]) -> None:
        super().__init__()
        self._target_card_ids = target_card_ids

    def get_cards(self) -> list[dict]:
        all_cards = super().get_cards()
        filtered = [c for c in all_cards if c["id"] in self._target_card_ids]
        self.logger.info(
            "Filtered %d total → %d dip candidates needing today's data",
            len(all_cards), len(filtered),
        )
        return filtered


def _recompute_signals():
    logger.info("Recomputing market_pressure + supply_saturation for all ebay cards...")
    with get_db() as db:
        card_ids = [
            r["card_id"] for r in db.execute(
                "SELECT DISTINCT card_id FROM ebay_history"
            ).fetchall()
        ]
    logger.info("Computing signals for %d cards", len(card_ids))
    ok = 0
    failed = 0
    with get_db() as db:
        for cid in card_ids:
            try:
                compute_market_pressure(db, cid)
                ok += 1
            except Exception as exc:
                failed += 1
                if failed <= 5:
                    logger.warning("compute_market_pressure(%s) failed: %s", cid, exc)
    logger.info("Signals done: %d ok, %d failed", ok, failed)


def main() -> int:
    today = dt.date.today().isoformat()
    target_ids, already_done = _dip_candidate_ids(today)
    logger.info(
        "Dip candidates to collect: %d (already done today: %d)",
        len(target_ids), already_done,
    )

    if target_ids:
        coll = DipCandidateEBayCollector(target_card_ids=target_ids)
        logger.info(
            "Collector mode: %s  (budget=%d/day)",
            "SANDBOX" if coll._is_sandbox else "PRODUCTION",
            coll.DAILY_BUDGET,
        )
        result = coll.collect(today)
        logger.info("=== Collection Done ===")
        logger.info("Processed: %d  Errors: %d  API calls: %d",
                     result.get("processed", 0), result.get("errors", 0), coll._calls_today)
    else:
        logger.info("All dip candidates already collected today.")

    _recompute_signals()

    with get_db() as db:
        n_today = db.execute(
            "SELECT COUNT(DISTINCT card_id) AS c FROM ebay_history WHERE date = ?",
            (today,),
        ).fetchone()["c"]
        n_with_signals = db.execute(
            "SELECT COUNT(DISTINCT card_id) AS c FROM market_pressure WHERE mode = 'observed'"
        ).fetchone()["c"]
        sv_sample = db.execute(
            """
            SELECT card_id, SUM(ended) AS sales_7d
            FROM ebay_history
            WHERE date >= date('now', '-7 days')
            GROUP BY card_id
            ORDER BY sales_7d DESC
            LIMIT 5
            """
        ).fetchall()

    logger.info("ebay_history today: %d distinct cards", n_today)
    logger.info("market_pressure coverage: %d cards", n_with_signals)
    logger.info("Top 5 sales_7d: %s", [(r["card_id"], int(r["sales_7d"])) for r in sv_sample])
    return 0


if __name__ == "__main__":
    sys.exit(main())
