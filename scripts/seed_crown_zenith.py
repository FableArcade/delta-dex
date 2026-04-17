"""
Seed Crown Zenith set into the PokeDelta database.

Crown Zenith (SWSH12pt5) — released January 20, 2023.
Main set: 160 cards. Galarian Gallery subset: GG01-GG70.

This seeds the set, rarities, and the high-value chase cards that matter
for investment analysis. Bulk commons/uncommons are excluded since they
have no PSA 10 market and would just add noise.

After seeding, run the PriceCharting bootstrap to pull historical prices:
    python -m scripts.bootstrap_pc_history_and_images

Usage:
    cd /Users/yoson/pokemon-analytics-delta
    python -m scripts.seed_crown_zenith
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection import init_db, get_db

SET_CODE = "CRZ"
SET_NAME = "Pokemon Crown Zenith"
RELEASE_DATE = "2023-01-20"
PSA_POP_URL = "https://www.psacard.com/pop/tcg-cards/2023/pokemon-crown-zenith/183838"
SET_COUNT = 230  # 160 main + 70 Galarian Gallery

# PriceCharting set slug
PC_SET_SLUG = "pokemon-crown-zenith"

# ID range: use 20000000+ to avoid collision with existing Collectrics IDs
ID_BASE = 20000000

# Rarity breakdown (approximate pull rates for investor-grade cards)
RARITIES = [
    ("CRZ_SIR", "SIR", "Special Illustration Rare", 6, 0.012, "1/83"),
    ("CRZ_GG",  "GG",  "Galarian Gallery",         70, 0.167, "1/6"),
    ("CRZ_UR",  "UR",  "Ultra Rare",               14, 0.056, "1/18"),
    ("CRZ_HR",  "HR",  "Hyper Rare",                3, 0.008, "1/125"),
    ("CRZ_SCR", "SCR", "Secret Rare",               3, 0.008, "1/125"),
    ("CRZ_V",   "V",   "V / VSTAR / VMAX",         18, 0.111, "1/9"),
    ("CRZ_R",   "R",   "Rare Holo",                15, 0.139, "1/7"),
]

# Chase cards — the ones investors actually track.
# Format: (id_offset, product_name, card_number, rarity_code, rarity_name, ebay_q_phrase, pc_slug)
CHASE_CARDS = [
    # Galarian Gallery — the crown jewels of this set
    (1, "Pikachu VMAX #GG30", "GG30", "GG", "Galarian Gallery", "Pikachu VMAX", "pikachu-vmax-gg30"),
    (2, "Charizard VSTAR #GG70", "GG70", "GG", "Galarian Gallery", "Charizard VSTAR", "charizard-vstar-gg70"),
    (3, "Mewtwo VSTAR #GG44", "GG44", "GG", "Galarian Gallery", "Mewtwo VSTAR", "mewtwo-vstar-gg44"),
    (4, "Umbreon VMAX #GG58", "GG58", "GG", "Galarian Gallery", "Umbreon VMAX", "umbreon-vmax-gg58"),
    (5, "Sylveon VMAX #GG53", "GG53", "GG", "Galarian Gallery", "Sylveon VMAX", "sylveon-vmax-gg53"),
    (6, "Leafeon VSTAR #GG42", "GG42", "GG", "Galarian Gallery", "Leafeon VSTAR", "leafeon-vstar-gg42"),
    (7, "Glaceon VSTAR #GG40", "GG40", "GG", "Galarian Gallery", "Glaceon VSTAR", "glaceon-vstar-gg40"),
    (8, "Eevee #GG01", "GG01", "GG", "Galarian Gallery", "Eevee", "eevee-gg01"),
    (9, "Rayquaza VMAX #GG57", "GG57", "GG", "Galarian Gallery", "Rayquaza VMAX", "rayquaza-vmax-gg57"),
    (10, "Mew VMAX #GG47", "GG47", "GG", "Galarian Gallery", "Mew VMAX", "mew-vmax-gg47"),
    (11, "Pikachu V #GG29", "GG29", "GG", "Galarian Gallery", "Pikachu V", "pikachu-v-gg29"),
    (12, "Deoxys VMAX #GG34", "GG34", "GG", "Galarian Gallery", "Deoxys VMAX", "deoxys-vmax-gg34"),
    (13, "Deoxys VSTAR #GG33", "GG33", "GG", "Galarian Gallery", "Deoxys VSTAR", "deoxys-vstar-gg33"),
    (14, "Zeraora VMAX #GG43", "GG43", "GG", "Galarian Gallery", "Zeraora VMAX", "zeraora-vmax-gg43"),
    (15, "Zeraora VSTAR #GG42", "GG42", "GG", "Galarian Gallery", "Zeraora VSTAR", "zeraora-vstar-gg42"),
    (16, "Lugia VSTAR #GG59", "GG59", "GG", "Galarian Gallery", "Lugia VSTAR", "lugia-vstar-gg59"),
    (17, "Giratina VSTAR #GG69", "GG69", "GG", "Galarian Gallery", "Giratina VSTAR", "giratina-vstar-gg69"),
    (18, "Darkrai VSTAR #GG48", "GG48", "GG", "Galarian Gallery", "Darkrai VSTAR", "darkrai-vstar-gg48"),
    (19, "Absol #GG02", "GG02", "GG", "Galarian Gallery", "Absol", "absol-gg02"),
    (20, "Ditto #GG07", "GG07", "GG", "Galarian Gallery", "Ditto", "ditto-gg07"),

    # Main set — Ultra Rares / VSTAR / VMAX
    (50, "Mewtwo V #71", 71, "UR", "Ultra Rare", "Mewtwo V", "mewtwo-v-71"),
    (51, "Charizard V #SWSH260", "SWSH260", "V", "V / VSTAR / VMAX", "Charizard V", "charizard-v-swsh260"),
    (52, "Pikachu V #SWSH285", "SWSH285", "V", "V / VSTAR / VMAX", "Pikachu V", "pikachu-v-swsh285"),
    (53, "Regieleki VMAX #58", 58, "V", "V / VSTAR / VMAX", "Regieleki VMAX", "regieleki-vmax-58"),
    (54, "Regidrago VSTAR #60", 60, "V", "V / VSTAR / VMAX", "Regidrago VSTAR", "regidrago-vstar-60"),
    (55, "Heatran VMAX #26", 26, "V", "V / VSTAR / VMAX", "Heatran VMAX", "heatran-vmax-26"),

    # Secret Rares / Gold Cards
    (80, "Giratina VSTAR #131", 131, "SCR", "Secret Rare", "Giratina VSTAR Gold", "giratina-vstar-131"),
    (81, "Arceus VSTAR #132", 132, "SCR", "Secret Rare", "Arceus VSTAR Gold", "arceus-vstar-132"),

    # Sealed products
    (100, "Crown Zenith Booster Pack", None, None, None, "Crown Zenith Booster Pack", None),
    (101, "Crown Zenith Elite Trainer Box", None, None, None, "Crown Zenith ETB", None),
    (102, "Crown Zenith Booster Bundle", None, None, None, "Crown Zenith Booster Bundle", None),
]


def seed():
    init_db()

    with get_db() as db:
        # 1. Insert set
        db.execute(
            """INSERT OR REPLACE INTO sets (set_code, set_name, release_date, psa_pop_url)
               VALUES (?, ?, ?, ?)""",
            (SET_CODE, SET_NAME, RELEASE_DATE, PSA_POP_URL),
        )
        print(f"Set: {SET_CODE} ({SET_NAME})")

        # 2. Insert rarities
        for set_rarity, rcode, rname, count, pull_rate, odds in RARITIES:
            db.execute(
                """INSERT OR REPLACE INTO rarities
                   (set_rarity, set_code, rarity_code, rarity_name,
                    card_count, pull_rate, pull_rate_odds)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (set_rarity, SET_CODE, rcode, rname, count, pull_rate, odds),
            )
        print(f"Rarities: {len(RARITIES)} tiers")

        # 3. Insert cards
        singles = 0
        sealed = 0
        for offset, name, card_num, rcode, rname, ebay_q, pc_slug in CHASE_CARDS:
            card_id = str(ID_BASE + offset)

            # Determine if sealed
            is_sealed = rcode is None
            sealed_type = None
            if is_sealed:
                if "Booster Pack" in name:
                    sealed_type = "Booster Pack"
                elif "Elite Trainer" in name or "ETB" in name:
                    sealed_type = "Elite Trainer Box"
                elif "Bundle" in name:
                    sealed_type = "Booster Bundle"
                else:
                    sealed_type = "Sealed"

            # Build PriceCharting URL
            pc_url = None
            if pc_slug:
                pc_url = f"https://www.pricecharting.com/game/{PC_SET_SLUG}/{pc_slug}"

            db.execute(
                """INSERT OR REPLACE INTO cards
                   (id, product_name, set_code, card_number, set_count,
                    rarity_code, rarity_name, set_value_include,
                    sealed_product, sealed_type,
                    ebay_q_phrase, ebay_category_id,
                    search_text)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    card_id, name, SET_CODE, card_num, SET_COUNT,
                    rcode, rname, "Y",
                    "Y" if is_sealed else "N",
                    sealed_type,
                    ebay_q,
                    "183454",
                    name.lower(),
                ),
            )

            if is_sealed:
                sealed += 1
            else:
                singles += 1

        print(f"Cards: {singles} singles + {sealed} sealed = {singles + sealed} total")
        print(f"Card IDs: {ID_BASE + 1} to {ID_BASE + CHASE_CARDS[-1][0]}")
        print()
        print("Next steps:")
        print("  1. Run PriceCharting bootstrap to pull historical prices:")
        print("     python -m scripts.bootstrap_pc_history_and_images")
        print("  2. Run daily pipeline to compute metrics:")
        print("     python -m pipeline.daily_pipeline --stage compute")
        print("  3. Re-train model to include Crown Zenith:")
        print("     python -c \"from db.connection import get_db; from pipeline.model.train import train_model; db=get_db().__enter__(); train_model(db)\"")


if __name__ == "__main__":
    seed()
