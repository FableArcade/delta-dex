"""Seed all Pokemon TCG sets and cards from pokemontcg.io API.

Adds missing sets and cards to the database with images.
Does NOT overwrite existing data — only fills gaps.

Run: python -m scripts.seed_from_pokemontcg
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import dotenv_values
env = dotenv_values(Path(__file__).resolve().parent.parent / ".env")
for k, v in env.items():
    if v:
        os.environ[k] = v

import httpx
from db.connection import get_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("seed_pokemontcg")

API_BASE = "https://api.pokemontcg.io/v2"
RATE_LIMIT = 0.15  # seconds between requests (free tier: ~20 req/sec)


def fetch_all_sets() -> list[dict]:
    """Fetch all Pokemon TCG sets from the API."""
    sets = []
    page = 1
    while True:
        resp = httpx.get(
            f"{API_BASE}/sets",
            params={"page": page, "pageSize": 250, "orderBy": "-releaseDate"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        sets.extend(data["data"])
        if len(sets) >= data["totalCount"]:
            break
        page += 1
        time.sleep(RATE_LIMIT)
    logger.info("Fetched %d sets from API", len(sets))
    return sets


def fetch_cards_for_set(set_id: str) -> list[dict]:
    """Fetch all cards in a set from the API."""
    cards = []
    page = 1
    while True:
        resp = httpx.get(
            f"{API_BASE}/cards",
            params={"q": f"set.id:{set_id}", "page": page, "pageSize": 250},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        cards.extend(data["data"])
        if len(cards) >= data["totalCount"]:
            break
        page += 1
        time.sleep(RATE_LIMIT)
    return cards


def map_set_code(api_set: dict) -> str:
    """Map pokemontcg.io set ID to our internal set_code."""
    # Use the ptcgoCode if available, else the API id uppercased
    return (api_set.get("ptcgoCode") or api_set["id"]).upper()


def upsert_set(db, api_set: dict) -> str:
    """Insert or update a set. Returns the set_code."""
    set_code = map_set_code(api_set)
    logo_url = api_set.get("images", {}).get("logo", "")
    symbol_url = api_set.get("images", {}).get("symbol", "")

    db.execute("""
        INSERT INTO sets (set_code, set_name, release_date, logo_url, total_cards)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(set_code) DO UPDATE SET
            set_name = COALESCE(excluded.set_name, sets.set_name),
            release_date = COALESCE(excluded.release_date, sets.release_date),
            logo_url = CASE WHEN excluded.logo_url != '' THEN excluded.logo_url ELSE sets.logo_url END,
            total_cards = COALESCE(excluded.total_cards, sets.total_cards)
    """, (
        set_code,
        api_set["name"],
        api_set.get("releaseDate", ""),
        logo_url,
        api_set.get("total", 0),
    ))
    return set_code


def upsert_card(db, api_card: dict, set_code: str) -> None:
    """Insert a card if it doesn't exist."""
    card_number = api_card.get("number", "")
    card_unique = f"{set_code}_{card_number}/{api_card.get('set', {}).get('printedTotal', '?')}"

    # Use tcgplayer ID if available
    tcg_id = str(api_card.get("tcgplayer", {}).get("url", "").split("/")[-1]) if api_card.get("tcgplayer") else ""

    # Image: prefer large, fall back to small
    images = api_card.get("images", {})
    image_url = images.get("large", images.get("small", ""))

    # Rarity
    rarity_name = api_card.get("rarity", "")
    rarity_code = rarity_name.replace(" ", "").upper()[:5] if rarity_name else ""

    # Product name
    product_name = api_card.get("name", "")
    number_str = f"#{card_number}" if card_number else ""
    full_name = f"{product_name} {number_str}".strip()

    # eBay search phrases
    ebay_q_phrase = product_name
    ebay_q_num = f"{card_number}/{api_card.get('set', {}).get('printedTotal', '')}" if card_number else ""

    db.execute("""
        INSERT OR IGNORE INTO cards (
            id, product_name, set_code, card_number, set_count,
            card_unique, rarity_code, rarity_name, tcg_id, image_url,
            sealed_product, ebay_q_phrase, ebay_q_num, ebay_category_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'N', ?, ?, '183454')
    """, (
        api_card["id"],
        full_name,
        set_code,
        card_number,
        api_card.get("set", {}).get("printedTotal", 0),
        card_unique,
        rarity_code,
        rarity_name,
        tcg_id,
        image_url,
        ebay_q_phrase,
        ebay_q_num,
    ))


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--sets", help="Comma-separated pokemontcg.io set IDs to seed (default: all)")
    args = parser.parse_args()

    target_set_ids = set(args.sets.split(",")) if args.sets else None

    logger.info("Fetching all sets from pokemontcg.io...")
    api_sets = fetch_all_sets()

    if target_set_ids:
        api_sets = [s for s in api_sets if s["id"] in target_set_ids]
        logger.info("Filtered to %d target sets: %s", len(api_sets), target_set_ids)

    with get_db() as db:
        existing_sets = {r["set_code"] for r in db.execute("SELECT set_code FROM sets").fetchall()}
        existing_cards = {r["id"] for r in db.execute("SELECT id FROM cards").fetchall()}
        logger.info("Existing: %d sets, %d cards", len(existing_sets), len(existing_cards))

    new_sets = 0
    new_cards = 0
    total_api_cards = 0

    with get_db() as db:
        for i, api_set in enumerate(api_sets):
            set_code = upsert_set(db, api_set)
            if set_code not in existing_sets:
                new_sets += 1

            # Fetch cards for this set
            logger.info(
                "[%d/%d] %s (%s) — fetching cards...",
                i + 1, len(api_sets), api_set["name"], set_code,
            )
            try:
                api_cards = fetch_cards_for_set(api_set["id"])
                total_api_cards += len(api_cards)
                for card in api_cards:
                    if card["id"] not in existing_cards:
                        upsert_card(db, card, set_code)
                        new_cards += 1
                time.sleep(RATE_LIMIT)
            except Exception as exc:
                logger.warning("Failed to fetch cards for %s: %s", set_code, exc)

            if (i + 1) % 10 == 0:
                logger.info("Progress: %d/%d sets, +%d new cards so far", i + 1, len(api_sets), new_cards)

    logger.info("=== Done ===")
    logger.info("Sets: %d total API, %d new", len(api_sets), new_sets)
    logger.info("Cards: %d total API, %d new", total_api_cards, new_cards)

    with get_db() as db:
        total_sets = db.execute("SELECT COUNT(*) AS c FROM sets").fetchone()["c"]
        total_cards = db.execute("SELECT COUNT(*) AS c FROM cards").fetchone()["c"]
    logger.info("DB now has: %d sets, %d cards", total_sets, total_cards)

    return 0


if __name__ == "__main__":
    sys.exit(main())
