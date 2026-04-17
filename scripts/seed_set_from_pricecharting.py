"""
Generalized PriceCharting set seeder.

Discovers cards for a set by scraping PriceCharting's console page sorted
by price, takes the top N by value, inserts them into the DB with pre-set
pc_canonical_urls so the bootstrap script picks them up directly.

Usage:
    python -m scripts.seed_set_from_pricecharting \\
        --set-code EVO \\
        --set-name "Pokemon Evolutions" \\
        --set-slug pokemon-evolutions \\
        --release-date 2016-11-02 \\
        --top-n 40

After seeding:
    python -m scripts.bootstrap_pc_history_and_images --resume
"""

from __future__ import annotations

import argparse
import re
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection import init_db, get_db

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
THROTTLE = 8.0  # Seconds between PriceCharting page requests — PC 403s fast

# ID range: 30000000+ for these bulk-seeded sets (avoids collision with
# Collectrics 4M-12M range and Crown Zenith 20M range)
ID_BASE = 30_000_000


def _slug_to_rarity(card_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Infer rarity_code and name from card name suffixes/patterns."""
    name_lower = card_name.lower()
    # Check in order of specificity
    if "vmax" in name_lower:
        return "V", "V / VSTAR / VMAX"
    if "vstar" in name_lower:
        return "V", "V / VSTAR / VMAX"
    if re.search(r'\bv\b', name_lower):
        return "V", "V / VSTAR / VMAX"
    if " ex " in f" {name_lower} " or name_lower.endswith(" ex") or "ex #" in name_lower:
        return "UR", "Ultra Rare"
    if "gx" in name_lower:
        return "UR", "Ultra Rare"
    if "tag team" in name_lower:
        return "UR", "Ultra Rare"
    if "hyper" in name_lower or "rainbow" in name_lower or "gold" in name_lower:
        return "HR", "Hyper Rare"
    if "illustration" in name_lower or "alt art" in name_lower:
        return "IR", "Illustration Rare"
    if "secret" in name_lower:
        return "SCR", "Secret Rare"
    if "break" in name_lower:
        return "UR", "Ultra Rare"
    if "radiant" in name_lower:
        return "RAD", "Radiant Rare"
    return "R", "Rare Holo"


def _extract_number(slug: str, card_name: str) -> Optional[str]:
    """Extract card number from slug or name.

    Slugs end with -<number> or -<NUM>, e.g. "charizard-vstar-19" -> "19"
    or "giratina-vstar-gg69" -> "GG69".
    """
    m = re.search(r'-([a-zA-Z]*\d+)$', slug)
    if m:
        return m.group(1).upper()
    m = re.search(r'#([a-zA-Z]*\d+)', card_name)
    if m:
        return m.group(1).upper()
    return None


def _clean_card_name(text: str) -> str:
    """Clean PriceCharting's displayed card name."""
    # Remove extra whitespace
    text = " ".join(text.split())
    # Remove leading "PC " or similar prefixes
    return text.strip()


def scrape_set_cards(
    set_slug: str,
    top_n: int = 40,
    max_pages: int = 5,
) -> List[Dict[str, Any]]:
    """Scrape PriceCharting's console page for a set, return top N cards by price.

    Iterates pages (PriceCharting paginates at 50 per page).
    Returns list of dicts with keys: name, slug, card_number.
    """
    client = httpx.Client(
        follow_redirects=True,
        timeout=15.0,
        headers={"User-Agent": USER_AGENT},
    )

    all_cards: Dict[str, Dict[str, Any]] = {}

    # Sort by price to get the valuable cards first
    page = 1
    while page <= max_pages:
        url = f"https://www.pricecharting.com/console/{set_slug}?sort=price&genre-name=All&page={page}"
        print(f"  Fetching page {page}: {url}")
        resp = None
        backoff = 30.0
        for attempt in range(1, 5):
            try:
                resp = client.get(url)
                if resp.status_code == 200:
                    break
                if resp.status_code in (403, 429):
                    print(f"  HTTP {resp.status_code} — rate-limited, sleep {backoff:.0f}s (attempt {attempt}/4)")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                print(f"  HTTP {resp.status_code}, stopping pagination")
                resp = None
                break
            except Exception as e:
                print(f"  Fetch failed: {e}; sleep {backoff:.0f}s")
                time.sleep(backoff)
                backoff *= 2
        if resp is None or resp.status_code != 200:
            print(f"  Giving up on page {page}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        count = 0
        for a in soup.select(f'a[href*="/game/{set_slug}/"]'):
            href = a.get("href", "")
            text = _clean_card_name(a.get_text())
            if not text:
                continue
            slug = urllib.parse.unquote(href.rstrip("/").split("/")[-1])
            if "reverse-holo" in slug or "[Reverse Holo]" in text:
                continue
            if slug in all_cards:
                continue
            all_cards[slug] = {
                "name": text,
                "slug": slug,
                "card_number": _extract_number(slug, text),
            }
            count += 1
        print(f"  Page {page}: {count} new cards (total: {len(all_cards)})")
        if count == 0:
            break
        page += 1
        time.sleep(THROTTLE)

    # PriceCharting sorts by price desc by default, so first N should be top value
    # top_n=0 means "no limit — return everything".
    if top_n and top_n > 0:
        result = list(all_cards.values())[:top_n]
    else:
        result = list(all_cards.values())
    print(f"  Selected {len(result)} cards")
    return result


def seed_set(
    set_code: str,
    set_name: str,
    set_slug: str,
    release_date: str,
    top_n: int = 40,
    id_offset: int = 0,
    psa_pop_url: Optional[str] = None,
) -> int:
    """Seed a set into the DB by scraping PriceCharting.

    Returns number of cards inserted.
    """
    init_db()

    print(f"\n=== Seeding {set_code} ({set_name}) ===")
    print(f"  PC slug: {set_slug}")
    print(f"  Release: {release_date}")
    print(f"  Target: top {top_n} cards")

    cards = scrape_set_cards(set_slug, top_n=top_n)
    if not cards:
        print("  No cards found, aborting")
        return 0

    pc_base = f"https://www.pricecharting.com/game/{set_slug}"

    with get_db() as db:
        # Insert set
        db.execute(
            """INSERT OR REPLACE INTO sets
               (set_code, set_name, release_date, psa_pop_url)
               VALUES (?, ?, ?, ?)""",
            (set_code, set_name, release_date, psa_pop_url),
        )

        # Dedupe against already-seeded canonical URLs so re-runs are safe.
        existing_urls = {
            row[0] for row in db.execute(
                "SELECT pc_canonical_url FROM cards "
                "WHERE set_code = ? AND pc_canonical_url IS NOT NULL",
                (set_code,),
            ).fetchall() if row[0]
        }
        # Next-available ID starting from (ID_BASE + id_offset), skipping any
        # IDs already used in that range so concurrent or repeated runs never
        # clobber existing rows.
        used_ids = {
            int(row[0]) for row in db.execute(
                "SELECT id FROM cards WHERE CAST(id AS INTEGER) >= ? "
                "AND CAST(id AS INTEGER) < ?",
                (ID_BASE + id_offset, ID_BASE + id_offset + 100_000),
            ).fetchall() if row[0] and str(row[0]).isdigit()
        }

        next_id = ID_BASE + id_offset

        def _alloc_id() -> str:
            nonlocal next_id
            while next_id in used_ids:
                next_id += 1
            chosen = next_id
            used_ids.add(chosen)
            next_id += 1
            return str(chosen)

        # Filter out cards we've already seeded.
        new_cards = []
        for card in cards:
            canonical = f"{pc_base}/{card['slug']}"
            if canonical in existing_urls:
                continue
            new_cards.append(card)
        skipped = len(cards) - len(new_cards)
        if skipped:
            print(f"  Skipping {skipped} already-seeded cards")
        cards = new_cards

        if not cards:
            print("  Nothing new to insert")
            return 0

        # Derive a rough set_count from cards scraped (not exact but usable)
        set_count = len(cards)

        # Insert cards
        rarities_seen: Dict[str, Dict[str, Any]] = {}
        for i, card in enumerate(cards):
            card_id = _alloc_id()
            rarity_code, rarity_name = _slug_to_rarity(card["name"])

            # Track rarities for the rarities table
            if rarity_code and rarity_code not in rarities_seen:
                rarities_seen[rarity_code] = {
                    "name": rarity_name,
                    "count": 0,
                }
            if rarity_code:
                rarities_seen[rarity_code]["count"] += 1

            # Extract a clean ebay phrase: just the Pokemon name
            ebay_q = _extract_ebay_phrase(card["name"], set_name)

            pc_url = f"{pc_base}/{card['slug']}"

            # Parse card_number; store as string; try integer if possible
            card_number_raw = card["card_number"]
            card_number_int = None
            if card_number_raw and card_number_raw.isdigit():
                card_number_int = int(card_number_raw)

            db.execute(
                """INSERT OR REPLACE INTO cards
                   (id, product_name, set_code, card_number, set_count,
                    rarity_code, rarity_name, set_value_include,
                    sealed_product, sealed_type,
                    ebay_q_phrase, ebay_category_id,
                    search_text, pc_canonical_url)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    card_id,
                    f"{card['name']}" + (f" #{card_number_raw}" if card_number_raw and f"#{card_number_raw}" not in card["name"] else ""),
                    set_code,
                    card_number_int,
                    set_count,
                    rarity_code,
                    rarity_name,
                    "Y",
                    "N",
                    None,
                    ebay_q,
                    "183454",
                    card["name"].lower(),
                    pc_url,
                ),
            )

        # Insert rarities (approximate pull rates — these are rough)
        rarity_pull_defaults = {
            "V": 0.111, "UR": 0.056, "HR": 0.008, "SCR": 0.012,
            "IR": 0.020, "RAD": 0.028, "R": 0.139, "MHR": 0.018,
            "SIR": 0.012, "GR": 0.008,
        }
        for rcode, info in rarities_seen.items():
            if rcode is None:
                continue
            pull_rate = rarity_pull_defaults.get(rcode, 0.05)
            odds = f"1/{int(1/pull_rate)}" if pull_rate > 0 else "unknown"
            db.execute(
                """INSERT OR REPLACE INTO rarities
                   (set_rarity, set_code, rarity_code, rarity_name,
                    card_count, pull_rate, pull_rate_odds)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (f"{set_code}_{rcode}", set_code, rcode, info["name"],
                 info["count"], pull_rate, odds),
            )

    print(f"  Inserted {len(cards)} cards, {len(rarities_seen)} rarity tiers")
    return len(cards)


def _extract_ebay_phrase(card_name: str, set_name: str) -> str:
    """Build a short eBay search phrase: Pokemon name + set hint."""
    # Strip suffixes
    name = re.sub(
        r'\s*(?:#\S+|\[.+?\]|\(.+?\))',
        "",
        card_name,
    ).strip()
    # Add short set identifier
    set_short = set_name.replace("Pokemon ", "").strip()
    return f"{name} {set_short}"[:80]


# ---------------------------------------------------------------------------
# Priority set list
# ---------------------------------------------------------------------------

PRIORITY_SETS = [
    # (set_code, set_name, pc_slug, release_date)
    # 2016 XY
    ("EVO", "Pokemon Evolutions", "pokemon-evolutions", "2016-11-02"),
    ("GEN", "Pokemon Generations", "pokemon-generations", "2016-02-22"),
    # 2018 SM
    ("BUS", "Pokemon Burning Shadows", "pokemon-burning-shadows", "2017-08-04"),
    ("HIF", "Pokemon Hidden Fates", "pokemon-hidden-fates", "2019-08-23"),
    ("SHL", "Pokemon Shining Legends", "pokemon-shining-legends", "2017-10-06"),
    # 2019 SM
    ("COE", "Pokemon Cosmic Eclipse", "pokemon-cosmic-eclipse", "2019-11-01"),
    ("DRM", "Pokemon Dragon Majesty", "pokemon-dragon-majesty", "2018-09-07"),
    # 2020 SWSH
    ("CPA", "Pokemon Champions Path", "pokemon-champion's-path", "2020-09-25"),
    ("VIV", "Pokemon Vivid Voltage", "pokemon-vivid-voltage", "2020-11-13"),
    ("SHF", "Pokemon Shining Fates", "pokemon-shining-fates", "2021-02-19"),
    # 2021 SWSH
    ("CRE", "Pokemon Chilling Reign", "pokemon-chilling-reign", "2021-06-18"),
    ("EVS", "Pokemon Evolving Skies", "pokemon-evolving-skies", "2021-08-27"),
    ("CEL", "Pokemon Celebrations", "pokemon-celebrations", "2021-10-08"),
    ("FST", "Pokemon Fusion Strike", "pokemon-fusion-strike", "2021-11-12"),
    # 2022 SWSH
    ("BRS", "Pokemon Brilliant Stars", "pokemon-brilliant-stars", "2022-02-25"),
    ("ASR", "Pokemon Astral Radiance", "pokemon-astral-radiance", "2022-05-27"),
    ("LOR", "Pokemon Lost Origin", "pokemon-lost-origin", "2022-09-09"),
    ("SIT", "Pokemon Silver Tempest", "pokemon-silver-tempest", "2022-11-11"),
]


def seed_all_priority_sets(top_n: int = 40, skip_existing: bool = True) -> None:
    """Seed all priority sets in order, with offset spacing between sets."""
    from db.connection import get_db

    for i, (code, name, slug, release) in enumerate(PRIORITY_SETS):
        with get_db() as db:
            existing = db.execute(
                "SELECT COUNT(*) FROM cards WHERE set_code = ?", (code,)
            ).fetchone()[0]
        if skip_existing and existing > 0:
            print(f"\n{code}: already has {existing} cards, skipping (use --force)")
            continue

        # Each set gets 100 ID slots to avoid collisions
        id_offset = i * 100
        try:
            seed_set(code, name, slug, release, top_n=top_n, id_offset=id_offset)
        except Exception as e:
            print(f"ERROR seeding {code}: {e}")
            continue


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--set-code", help="3-letter set code (e.g. EVO)")
    p.add_argument("--set-name", help="Full set name")
    p.add_argument("--set-slug", help="PriceCharting slug (e.g. pokemon-evolutions)")
    p.add_argument("--release-date", help="YYYY-MM-DD release date")
    p.add_argument("--top-n", type=int, default=40, help="Top N cards by price")
    p.add_argument("--all-priority", action="store_true",
                   help="Seed all priority sets (2016-2022)")
    p.add_argument("--force", action="store_true",
                   help="Re-seed even if set exists")
    args = p.parse_args()

    if args.all_priority:
        seed_all_priority_sets(top_n=args.top_n, skip_existing=not args.force)
    elif all([args.set_code, args.set_name, args.set_slug, args.release_date]):
        seed_set(args.set_code, args.set_name, args.set_slug,
                 args.release_date, top_n=args.top_n)
    else:
        p.error("Either --all-priority or all of --set-code, --set-name, "
                "--set-slug, --release-date are required")


if __name__ == "__main__":
    main()
