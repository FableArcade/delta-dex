"""Ingest all English Black Star promo cards from pokemontcg.io.

pokemontcg.io has authoritative, complete metadata + image URLs for every
English promo set (basep through svp, 9 sets, ~1,241 cards total).
PriceCharting only surfaces ~50 cards per promo page, so this gets us
the full catalog.

Strategy:
  1. For each promo set id, page through /v2/cards
  2. Insert rows into cards table with set_code='PROMO' and id = ptcgio card id
  3. Image URL comes straight from pokemontcg.io (no PC fetch needed)
  4. pc_canonical_url left NULL — bootstrap_pc_history_and_images can fill
     in by name-search for the cards PC actually tracks.

Idempotent: INSERT OR IGNORE so re-runs don't clobber existing rows.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection import get_db, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("seed_promos")

PROMO_SET_IDS = [
    # (ptcgio set id, era label for logging)
    ("basep", "Wizards"),
    ("np",    "Nintendo"),
    ("dpp",   "Diamond & Pearl"),
    ("hsp",   "HGSS"),
    ("bwp",   "Black & White"),
    ("xyp",   "XY"),
    ("smp",   "Sun & Moon"),
    ("swshp", "Sword & Shield"),
    ("svp",   "Scarlet & Violet"),
]

API_BASE = "https://api.pokemontcg.io/v2"
PAGE_SIZE = 250
THROTTLE = 0.5  # API is generous; keep polite


def _fetch_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _fetch_all_cards(set_id: str) -> List[Dict[str, Any]]:
    cards: List[Dict[str, Any]] = []
    page = 1
    while True:
        url = f"{API_BASE}/cards?q=set.id:{set_id}&pageSize={PAGE_SIZE}&page={page}"
        data = _fetch_json(url)
        batch = data.get("data", [])
        cards.extend(batch)
        total = data.get("totalCount", 0)
        if len(cards) >= total or not batch:
            break
        page += 1
        time.sleep(THROTTLE)
    return cards


def _rarity_code(rarity: Optional[str], name: str) -> tuple[Optional[str], Optional[str]]:
    """Map ptcgio rarity + card name to our internal rarity codes."""
    name_lower = (name or "").lower()
    r = (rarity or "").lower()

    if "vmax" in name_lower or "vstar" in name_lower:
        return "V", "V / VSTAR / VMAX"
    if re.search(r"\bv\b", name_lower):
        return "V", "V / VSTAR / VMAX"
    if " ex " in f" {name_lower} " or name_lower.endswith(" ex"):
        return "UR", "Ultra Rare"
    if "gx" in name_lower or "tag team" in name_lower:
        return "UR", "Ultra Rare"
    if "rainbow" in r or "hyper" in r or "gold" in r:
        return "HR", "Hyper Rare"
    if "secret" in r:
        return "SCR", "Secret Rare"
    if "illustration" in r or "alt" in r:
        return "IR", "Illustration Rare"
    if "radiant" in r:
        return "RAD", "Radiant Rare"
    # Default for plain promo
    return "P", "Promo"


def _ebay_phrase(name: str, card_number: Optional[str]) -> str:
    base = re.sub(r"\s+", " ", name).strip()
    return f"{base} promo {card_number or ''}".strip()[:80]


def seed_promos(skip_existing: bool = True) -> Dict[str, int]:
    init_db()
    stats = {"fetched": 0, "inserted": 0, "skipped": 0, "per_set": {}}

    with get_db() as db:
        # Ensure PROMO set row exists
        db.execute(
            "INSERT OR IGNORE INTO sets (set_code, set_name, release_date) "
            "VALUES ('PROMO', 'Pokemon Black Star Promo', '2000-01-01')"
        )
        # Pull existing ids once to skip
        existing_ids = {
            r[0] for r in db.execute(
                "SELECT id FROM cards WHERE set_code = 'PROMO'"
            ).fetchall()
        }

        for set_id, era in PROMO_SET_IDS:
            logger.info("Fetching promos from %s (%s)...", set_id, era)
            try:
                cards = _fetch_all_cards(set_id)
            except Exception as exc:
                logger.error("%s fetch failed: %s", set_id, exc)
                continue
            logger.info("  %d cards fetched", len(cards))
            stats["fetched"] += len(cards)
            inserted = 0

            for c in cards:
                card_id = c.get("id")  # e.g., "svp-1"
                if not card_id:
                    continue
                if skip_existing and card_id in existing_ids:
                    stats["skipped"] += 1
                    continue

                name = c.get("name", "").strip()
                number_raw = str(c.get("number", "")).strip()
                number_int = int(number_raw) if number_raw.isdigit() else None

                rarity_code, rarity_name = _rarity_code(c.get("rarity"), name)
                images = c.get("images") or {}
                image_url = images.get("large") or images.get("small")

                product_name = f"{name} #{number_raw}" if number_raw else name

                db.execute(
                    """INSERT OR IGNORE INTO cards
                       (id, product_name, set_code, card_number, set_count,
                        rarity_code, rarity_name, set_value_include,
                        sealed_product, image_url,
                        ebay_q_phrase, ebay_category_id, search_text)
                       VALUES (?, ?, 'PROMO', ?, ?, ?, ?, 'Y', 'N', ?, ?, '183454', ?)""",
                    (
                        card_id, product_name, number_int, len(cards),
                        rarity_code, rarity_name, image_url,
                        _ebay_phrase(name, number_raw), name.lower(),
                    ),
                )
                existing_ids.add(card_id)
                inserted += 1

            db.commit()
            stats["per_set"][set_id] = inserted
            stats["inserted"] += inserted
            logger.info("  inserted %d new cards from %s", inserted, set_id)

    return stats


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--force", action="store_true",
                   help="Re-insert rows even if id already exists (no-op with INSERT OR IGNORE).")
    args = p.parse_args()
    s = seed_promos(skip_existing=not args.force)
    print(f"\nDONE: fetched={s['fetched']} inserted={s['inserted']} skipped={s['skipped']}")
    for k, v in s["per_set"].items():
        print(f"  {k}: {v}")
