"""
One-time import of scraped Collectrics data into the SQLite database.
Seeds: sets, rarities, cards, price_history, set_daily, leaderboard,
       and all 7 history arrays from card detail files.

Usage:
    cd /Users/yoson/pokemon-analytics
    python -m scripts.import_collectrics
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection import init_db, get_db

DATA_DIR = Path("/Users/yoson/collectrics-data")


def load_json(filename):
    with open(DATA_DIR / filename) as f:
        return json.load(f)


def import_sets_and_rarities(db):
    """Import sets, rarities, and set_daily history from all-sets data."""
    sets_data = load_json("collectrics-all-sets.json")

    for set_code, s in sets_data.items():
        db.execute(
            """INSERT OR REPLACE INTO sets (set_code, set_name, release_date, psa_pop_url)
               VALUES (?, ?, ?, ?)""",
            (set_code, s.get("set-name", ""), s.get("release-date"), s.get("psa-pop-url")),
        )

        # Rarity breakdown
        rb = s.get("rarity-breakdown", {})
        for rcode, r in rb.items():
            set_rarity = r.get("set-rarity", f"{set_code}_{rcode}")
            db.execute(
                """INSERT OR REPLACE INTO rarities
                   (set_rarity, set_code, rarity_code, rarity_name, card_count, pull_rate, pull_rate_odds)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    set_rarity,
                    set_code,
                    r.get("rarity-code", rcode),
                    r.get("rarity-name", ""),
                    r.get("card-count", 0),
                    r.get("pull-rate", 0),
                    r.get("pull-rate-odds"),
                ),
            )

            # Rarity snapshot (current values)
            today = s.get("generated-at", "")
            if today:
                db.execute(
                    """INSERT OR REPLACE INTO set_rarity_snapshot
                       (set_rarity, date, avg_raw_price, avg_psa_10_price,
                        ev_raw_per_pack, ev_psa_10_per_pack,
                        psa_pop_10_base, psa_pop_total_base, psa_avg_gem_pct)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        set_rarity,
                        today,
                        r.get("avg-raw-price"),
                        r.get("avg-psa-10-price"),
                        r.get("ev-raw-per-pack"),
                        r.get("ev-psa-10-per-pack"),
                        r.get("psa-pop-10-base"),
                        r.get("psa-pop-total-base"),
                        r.get("psa-avg-gem-pct"),
                    ),
                )

        # Set daily history
        history = s.get("history", [])
        for h in history:
            db.execute(
                """INSERT OR REPLACE INTO set_daily
                   (set_code, date, ev_raw_per_pack, ev_psa_10_per_pack,
                    avg_pack_cost, avg_gain_loss, total_set_raw_value)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    set_code,
                    h.get("date"),
                    h.get("ev-raw-per-pack"),
                    h.get("ev-psa-10-per-pack"),
                    h.get("avg-pack-cost"),
                    h.get("avg-gain-loss"),
                    h.get("total-set-raw-value"),
                ),
            )

        # Pack cost components
        pcc = s.get("pack-cost-components", {})
        pcs = s.get("pack-cost-sample-counts", {})
        today = s.get("generated-at", "")
        if pcc and today:
            db.execute(
                """INSERT OR REPLACE INTO pack_cost
                   (set_code, date, avg_booster_pack, avg_sleeved_booster_pack,
                    avg_booster_bundle_per_pack, avg_pack_cost,
                    booster_pack_count, sleeved_booster_count, booster_bundle_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    set_code,
                    today,
                    pcc.get("avg-booster-pack"),
                    pcc.get("avg-sleeved-booster-pack"),
                    pcc.get("avg-booster-bundle-per-pack"),
                    s.get("avg-pack-cost"),
                    pcs.get("booster-pack"),
                    pcs.get("sleeved-booster-pack"),
                    pcs.get("booster-bundle"),
                ),
            )

    print(f"  Imported {len(sets_data)} sets with rarities and history")


def import_cards_and_index(db):
    """Import card index (8600+ cards) from the all-data dump."""
    all_data = load_json("collectrics-all-data.json")
    card_index = all_data.get("cardIndex", {})
    cards = card_index.get("cards", [])

    for c in cards:
        set_code = c.get("set-code", "") or ""
        if not set_code:
            continue
        db.execute(
            """INSERT OR REPLACE INTO cards
               (id, product_name, set_code, card_number, set_count,
                rarity_code, rarity_name, image_url, set_value_include, search_text)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                c["id"],
                c.get("product-name", ""),
                set_code,
                c.get("card-number"),
                c.get("set-count"),
                c.get("rarity-code"),
                c.get("rarity-name"),
                c.get("image-url"),
                c.get("set-value-include", "Y"),
                c.get("searchText"),
            ),
        )

        # Store latest price as a price_history row
        latest_date = c.get("latest-date")
        if latest_date:
            db.execute(
                """INSERT OR REPLACE INTO price_history
                   (card_id, date, raw_price, psa_10_price, psa_10_vs_raw, psa_10_vs_raw_pct)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    c["id"],
                    latest_date,
                    c.get("raw-price"),
                    c.get("psa-10-price"),
                    c.get("psa-10-vs-raw"),
                    c.get("psa-10-vs-raw-pct"),
                ),
            )

    print(f"  Imported {len(cards)} cards from card index")

    # Build set_name -> set_code lookup
    set_name_to_code = {}
    for row in db.execute("SELECT set_code, set_name FROM sets").fetchall():
        set_name_to_code[row["set_name"]] = row["set_code"]

    # Import sealed products
    sealed = all_data.get("sealed", {})
    sealed_types = sealed.get("sealed-type", {})
    sealed_count = 0
    for stype, sdata in sealed_types.items():
        for row in sdata.get("rows", []):
            set_code = row.get("set-code") or set_name_to_code.get(row.get("set-name", ""), "")
            if not set_code:
                continue
            db.execute(
                """INSERT OR REPLACE INTO cards
                   (id, product_name, set_code, sealed_product, sealed_type,
                    set_value_include, image_url)
                   VALUES (?, ?, ?, 'Y', ?, 'N', ?)""",
                (
                    row["id"],
                    row.get("product-name", ""),
                    set_code,
                    stype,
                    row.get("image-url"),
                ),
            )
            # Store sealed price
            snap_date = row.get("snapshot-date")
            if snap_date:
                db.execute(
                    """INSERT OR REPLACE INTO price_history
                       (card_id, date, raw_price)
                       VALUES (?, ?, ?)""",
                    (row["id"], snap_date, row.get("raw-price")),
                )
            sealed_count += 1

    print(f"  Imported {sealed_count} sealed products")


def import_leaderboard(db):
    """Import leaderboard rankings."""
    lb = load_json("collectrics-api-leaderboard.json")
    gen_date = lb.get("generated-at", "")
    rows = lb.get("rows", [])

    for r in rows:
        db.execute(
            """INSERT OR REPLACE INTO leaderboard
               (set_code, date, rarity_buckets, cards_counted, avg_pack_cost,
                ev_raw_per_pack, ev_psa_10_per_pack, avg_gain_loss,
                total_set_raw_value, psa_pop_10_base, psa_pop_total_base,
                psa_avg_gem_pct, rank_avg_gain_loss, rank_ev_raw_per_pack,
                rank_total_set_raw_value, rank_psa_avg_gem_pct)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                r["set-code"],
                r.get("generated-at", gen_date),
                r.get("rarity-buckets"),
                r.get("cards-counted"),
                r.get("avg-pack-cost"),
                r.get("ev-raw-per-pack"),
                r.get("ev-psa-10-per-pack"),
                r.get("avg-gain-loss"),
                r.get("total-set-raw-value"),
                r.get("psa-pop-10-base"),
                r.get("psa-pop-total-base"),
                r.get("psa-avg-gem-pct"),
                r.get("rank-avg-gain-loss"),
                r.get("rank-ev-raw-per-pack"),
                r.get("rank-total-set-raw-value"),
                r.get("rank-psa-avg-gem-pct"),
            ),
        )

    print(f"  Imported leaderboard with {len(rows)} sets")


def import_card_details(db):
    """Import full card detail files with all 7 history arrays."""
    detail_files = [
        "collectrics-umbreon-full.json",
        "collectrics-card-detail-full.json",
        "collectrics-sample-cards.json",
    ]

    imported = 0
    for fname in detail_files:
        fpath = DATA_DIR / fname
        if not fpath.exists():
            continue

        data = load_json(fname)

        # Handle both single-card and multi-card files
        if "id" in data:
            cards_to_import = {data["id"]: data}
        else:
            cards_to_import = data

        for card_id, card in cards_to_import.items():
            if not isinstance(card, dict) or "error" in card:
                continue

            # Ensure card exists before updating
            sc = card.get("set-code") or "UNK"
            exists = db.execute("SELECT 1 FROM cards WHERE id = ?", (card_id,)).fetchone()
            if not exists:
                db.execute("INSERT OR IGNORE INTO sets (set_code, set_name) VALUES (?, ?)",
                           (sc, card.get("set-name") or sc))
                db.execute(
                    """INSERT INTO cards (id, product_name, set_code, set_value_include)
                       VALUES (?, ?, ?, ?)""",
                    (card_id, card.get("product-name") or "", sc, card.get("set-value-include") or "Y"),
                )

            # Update card metadata
            db.execute(
                """UPDATE cards SET
                   card_unique = ?, tcg_id = ?, image_url = COALESCE(?, image_url),
                   tcgplayer_image_url = ?, sealed_product = ?, sealed_type = ?,
                   ebay_q_phrase = ?, ebay_q_num = ?, ebay_category_id = ?
                   WHERE id = ?""",
                (
                    card.get("card-unique"),
                    card.get("tcg-id"),
                    card.get("image-url"),
                    card.get("tcgplayer-image-url"),
                    card.get("sealed-product", "N"),
                    card.get("sealed-type"),
                    card.get("ebay-q-phrase"),
                    card.get("ebay-q-num"),
                    card.get("ebay-category-id"),
                    card_id,
                ),
            )

            # 1. Price history
            for h in card.get("history", []):
                db.execute(
                    """INSERT OR REPLACE INTO price_history
                       (card_id, date, raw_price, psa_7_price, psa_8_price,
                        psa_9_price, psa_10_price, psa_10_vs_raw, psa_10_vs_raw_pct,
                        sales_volume, interpolated, interpolation_source)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        card_id,
                        h["date"],
                        h.get("raw-price"),
                        h.get("psa-7-price"),
                        h.get("psa-8-price"),
                        h.get("psa-9-price"),
                        h.get("psa-10-price"),
                        h.get("psa-10-vs-raw"),
                        h.get("psa-10-vs-raw-pct"),
                        h.get("sales-volume"),
                        1 if h.get("interpolated") else 0,
                        h.get("interpolation-source"),
                    ),
                )

            # 2. PSA Pop history
            for h in card.get("history-psa", []):
                db.execute(
                    """INSERT OR REPLACE INTO psa_pop_history
                       (card_id, date, psa_8_base, psa_9_base, psa_10_base, total_base, gem_pct)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        card_id, h["date"],
                        h.get("8-base"), h.get("9-base"), h.get("10-base"),
                        h.get("total-base"), h.get("gem-pct"),
                    ),
                )

            # 3. eBay history
            for h in card.get("history-ebay", []):
                db.execute(
                    """INSERT OR REPLACE INTO ebay_history
                       (card_id, date, from_date, active_from, active_to,
                        ended, new, ended_rate, ended_raw, new_raw,
                        ended_graded, new_graded, ended_psa_10, new_psa_10,
                        ended_psa_9, new_psa_9, ended_other_10, new_other_10,
                        ended_avg_raw_price, ended_avg_psa_10_price,
                        ended_avg_psa_9_price, ended_avg_other_10_price,
                        interpolated,
                        ended_adj, ended_raw_adj, ended_graded_adj,
                        new_adj, new_raw_adj, new_graded_adj,
                        ended_avg_raw_price_adj, ended_avg_psa_10_price_adj,
                        ended_avg_psa_9_price_adj)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        card_id, h["date"], h.get("from-date"),
                        h.get("active-from"), h.get("active-to"),
                        h.get("ended"), h.get("new"),
                        h.get("ended-rate"), h.get("ended-raw"), h.get("new-raw"),
                        h.get("ended-graded"), h.get("new-graded"),
                        h.get("ended-psa-10"), h.get("new-psa-10"),
                        h.get("ended-psa-9"), h.get("new-psa-9"),
                        h.get("ended-other-10"), h.get("new-other-10"),
                        h.get("ended-avg-raw-price"), h.get("ended-avg-psa-10-price"),
                        h.get("ended-avg-psa-9-price"), h.get("ended-avg-other-10-price"),
                        1 if h.get("interpolated") else 0,
                        h.get("ended-adj"), h.get("ended-raw-adj"), h.get("ended-graded-adj"),
                        h.get("new-adj"), h.get("new-raw-adj"), h.get("new-graded-adj"),
                        h.get("ended-avg-raw-price-adj"), h.get("ended-avg-psa-10-price-adj"),
                        h.get("ended-avg-psa-9-price-adj"),
                    ),
                )

            # 4. eBay market history
            for h in card.get("history-ebay-market", []):
                db.execute(
                    """INSERT OR REPLACE INTO ebay_market_history
                       (card_id, date, from_date, active_from, active_to,
                        ended, new, ended_raw, ended_psa_9, ended_psa_10,
                        interpolated, demand_pressure_observed, demand_pressure_est,
                        sold_rate_est, sold_est)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        card_id, h["date"], h.get("from-date"),
                        h.get("active-from"), h.get("active-to"),
                        h.get("ended"), h.get("new"),
                        h.get("ended-raw"), h.get("ended-psa-9"), h.get("ended-psa-10"),
                        1 if h.get("interpolated") else 0,
                        h.get("demand-pressure-observed"), h.get("demand-pressure-est"),
                        h.get("sold-rate-est"), h.get("sold-est"),
                    ),
                )

            # 5. eBay-derived pricing
            for h in card.get("history-ebay-derived", []):
                db.execute(
                    """INSERT OR REPLACE INTO ebay_derived_history
                       (card_id, date, d_raw_price, d_psa_9_price, d_psa_10_price)
                       VALUES (?, ?, ?, ?, ?)""",
                    (card_id, h["date"], h.get("d-raw-price"),
                     h.get("d-psa-9-price"), h.get("d-psa-10-price")),
                )

            # 6. JustTCG pricing
            for h in card.get("history-justtcg", []):
                db.execute(
                    """INSERT OR REPLACE INTO justtcg_history
                       (card_id, date, j_raw_price)
                       VALUES (?, ?, ?)""",
                    (card_id, h["date"], h.get("j-raw-price")),
                )

            # 7. Composite pricing
            for h in card.get("history-collectrics", []):
                db.execute(
                    """INSERT OR REPLACE INTO composite_history
                       (card_id, date, c_raw_price, c_psa_9_price, c_psa_10_price)
                       VALUES (?, ?, ?, ?, ?)""",
                    (card_id, h["date"], h.get("c-raw-price"),
                     h.get("c-psa-9-price"), h.get("c-psa-10-price")),
                )

            # Market pressure
            mp = card.get("collectrics", {}).get("market-pressure", {})
            for mode_key in ("observed", "estimated"):
                mode_data = mp.get(mode_key, {})
                for window_key in ("7d", "30d"):
                    w = mode_data.get(window_key, {})
                    if not w:
                        continue
                    raw = w.get("raw", {})
                    metrics = w.get("metrics", {})
                    labels = w.get("labels", {})

                    # Pick the right ended field
                    avg_ended = raw.get("avg-ended", raw.get("avg-sold-est"))
                    dp = metrics.get("demand-pressure", metrics.get("demand-pressure-est"))

                    db.execute(
                        """INSERT OR REPLACE INTO market_pressure
                           (card_id, window_days, mode, as_of, sample_days,
                            interpolated_days, avg_active, avg_existing, avg_ended,
                            avg_new, demand_pressure, supply_pressure, net_flow,
                            net_flow_pct, state_label)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            card_id, int(window_key.replace("d", "")), mode_key,
                            w.get("as-of"), w.get("sample-days"),
                            w.get("interpolated-days"), raw.get("avg-active"),
                            raw.get("avg-existing"), avg_ended, raw.get("avg-new"),
                            dp, metrics.get("supply-pressure"),
                            metrics.get("net-flow"), metrics.get("net-flow-pct"),
                            labels.get("state"),
                        ),
                    )

                # Baseline comparison
                bc = mode_data.get("baseline-comparison", {})
                if bc:
                    db.execute(
                        """INSERT OR REPLACE INTO supply_saturation
                           (card_id, mode, as_of, supply_saturation_index,
                            supply_saturation_label, trend,
                            active_listings_delta_pct, demand_delta_pct, supply_delta_pct)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            card_id, mode_key,
                            mode_data.get("7d", {}).get("as-of", ""),
                            bc.get("supply-saturation-index"),
                            bc.get("supply-saturation-label"),
                            bc.get("trend"),
                            bc.get("active-listings-delta-pct"),
                            bc.get("demand-delta-pct"),
                            bc.get("supply-delta-pct"),
                        ),
                    )

            imported += 1

    print(f"  Imported {imported} card details with full history arrays")


def main():
    print("Initializing database...")
    init_db()

    with get_db() as db:
        print("Importing sets, rarities, and set history...")
        import_sets_and_rarities(db)

        print("Importing card index and sealed products...")
        import_cards_and_index(db)

        print("Importing leaderboard...")
        import_leaderboard(db)

        print("Importing card details with full history...")
        import_card_details(db)

    # Verify
    with get_db() as db:
        counts = {}
        for table in [
            "sets", "rarities", "cards", "price_history", "psa_pop_history",
            "ebay_history", "ebay_market_history", "ebay_derived_history",
            "justtcg_history", "composite_history", "market_pressure",
            "supply_saturation", "set_daily", "set_rarity_snapshot",
            "pack_cost", "leaderboard",
        ]:
            row = db.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()
            counts[table] = row["c"]

        print("\n=== Database Summary ===")
        for table, count in counts.items():
            print(f"  {table:30s} {count:>8,}")

    from config.settings import DB_PATH
    db_size = DB_PATH.stat().st_size / (1024 * 1024)
    print(f"\n  Database size: {db_size:.1f} MB")
    print("  Done!")


if __name__ == "__main__":
    main()
