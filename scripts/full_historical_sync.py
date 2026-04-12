"""
Full historical data sync — fetches every card and sealed product
with complete price history from the Collectrics API and writes to
the local SQLite database.

This is a one-time bootstrap to ensure complete historical coverage
for every card across every tracked set. After this runs, the daily
production scrapers (pricecharting, ebay, tcgplayer, psa) take over
for ongoing updates.

Usage:
    cd /Users/yoson/pokemon-analytics
    python3 -m scripts.full_historical_sync
    python3 -m scripts.full_historical_sync --workers 8 --resume
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection import get_db, init_db

BASE_URL = "https://mycollectrics.com"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"


def fetch_json(path: str, retries: int = 3, backoff: float = 2.0):
    """Fetch a JSON endpoint with retries and exponential backoff."""
    url = f"{BASE_URL}{path}"
    last_err = None
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            with urlopen(req, timeout=30) as response:
                return json.loads(response.read())
        except (HTTPError, URLError, json.JSONDecodeError) as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(backoff ** attempt)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


# ============================================================
# Catalog discovery
# ============================================================

def discover_all_card_ids():
    """Get every card ID from the Collectrics card index + sealed leaderboard."""
    print("Discovering card catalog...")

    card_index = fetch_json("/api/card_index")
    cards = card_index.get("cards", [])
    print(f"  card_index: {len(cards)} cards")

    sealed = fetch_json("/api/sealed_leaderboard")
    sealed_count = 0
    sealed_items = []
    for stype, sdata in sealed.get("sealed-type", {}).items():
        for row in sdata.get("rows", []):
            sealed_items.append({
                "id": row["id"],
                "product-name": row.get("product-name", ""),
                "set-name": row.get("set-name", ""),
                "sealed-type": stype,
                "raw-price": row.get("raw-price"),
                "snapshot-date": row.get("snapshot-date"),
            })
            sealed_count += 1
    print(f"  sealed: {sealed_count} products")

    # Also pull sets_index for set metadata
    sets_index = fetch_json("/api/sets_index")
    sets_meta = {s["set-code"]: s for s in sets_index.get("sets", [])}
    print(f"  sets_index: {len(sets_meta)} sets")

    return {
        "cards": cards,
        "sealed": sealed_items,
        "sets_meta": sets_meta,
    }


# ============================================================
# Data writers (idempotent UPSERTs)
# ============================================================

def upsert_set(db, set_code: str, set_name: str, release_date=None, psa_pop_url=None):
    db.execute(
        """INSERT INTO sets (set_code, set_name, release_date, psa_pop_url)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(set_code) DO UPDATE SET
             set_name = excluded.set_name,
             release_date = COALESCE(excluded.release_date, sets.release_date),
             psa_pop_url = COALESCE(excluded.psa_pop_url, sets.psa_pop_url)""",
        (set_code, set_name, release_date, psa_pop_url),
    )


_set_name_cache = {}


def _resolve_set_code(db, card: dict) -> str:
    """Get the set_code for a card, resolving by set-name if set-code is missing."""
    sc = card.get("set-code")
    if sc:
        return sc
    set_name = card.get("set-name")
    if not set_name:
        return "UNK"
    if not _set_name_cache:
        for row in db.execute("SELECT set_code, set_name FROM sets").fetchall():
            _set_name_cache[row["set_name"]] = row["set_code"]
    return _set_name_cache.get(set_name, "UNK")


def upsert_card(db, card: dict):
    """Insert or update a card with metadata."""
    set_code = _resolve_set_code(db, card)
    upsert_set(db, set_code, card.get("set-name") or set_code)

    db.execute(
        """INSERT INTO cards
           (id, product_name, set_code, card_number, set_count, card_unique,
            rarity_code, rarity_name, tcg_id, image_url, tcgplayer_image_url,
            set_value_include, sealed_product, sealed_type,
            ebay_q_phrase, ebay_q_num, ebay_category_id, search_text)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
             product_name = excluded.product_name,
             set_code = excluded.set_code,
             card_number = COALESCE(excluded.card_number, cards.card_number),
             set_count = COALESCE(excluded.set_count, cards.set_count),
             card_unique = COALESCE(excluded.card_unique, cards.card_unique),
             rarity_code = COALESCE(excluded.rarity_code, cards.rarity_code),
             rarity_name = COALESCE(excluded.rarity_name, cards.rarity_name),
             tcg_id = COALESCE(excluded.tcg_id, cards.tcg_id),
             image_url = COALESCE(excluded.image_url, cards.image_url),
             tcgplayer_image_url = COALESCE(excluded.tcgplayer_image_url, cards.tcgplayer_image_url),
             set_value_include = excluded.set_value_include,
             sealed_product = excluded.sealed_product,
             sealed_type = COALESCE(excluded.sealed_type, cards.sealed_type),
             ebay_q_phrase = COALESCE(excluded.ebay_q_phrase, cards.ebay_q_phrase),
             ebay_q_num = COALESCE(excluded.ebay_q_num, cards.ebay_q_num),
             ebay_category_id = COALESCE(excluded.ebay_category_id, cards.ebay_category_id),
             search_text = COALESCE(excluded.search_text, cards.search_text)""",
        (
            card["id"],
            card.get("product-name", ""),
            set_code,
            card.get("card-number"),
            card.get("set-count"),
            card.get("card-unique"),
            card.get("rarity-code"),
            card.get("rarity-name"),
            card.get("tcg-id"),
            card.get("image-url"),
            card.get("tcgplayer-image-url"),
            card.get("set-value-include", "Y"),
            card.get("sealed-product", "N"),
            card.get("sealed-type"),
            card.get("ebay-q-phrase"),
            card.get("ebay-q-num"),
            card.get("ebay-category-id"),
            card.get("searchText"),
        ),
    )


def insert_history_arrays(db, card_id: str, card: dict):
    """Insert all 7 history arrays for a card."""
    # 1. price_history
    for h in card.get("history", []):
        db.execute(
            """INSERT OR REPLACE INTO price_history
               (card_id, date, raw_price, psa_7_price, psa_8_price,
                psa_9_price, psa_10_price, psa_10_vs_raw, psa_10_vs_raw_pct,
                sales_volume, interpolated, interpolation_source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                card_id, h["date"],
                h.get("raw-price"), h.get("psa-7-price"), h.get("psa-8-price"),
                h.get("psa-9-price"), h.get("psa-10-price"),
                h.get("psa-10-vs-raw"), h.get("psa-10-vs-raw-pct"),
                h.get("sales-volume"),
                1 if h.get("interpolated") else 0,
                h.get("interpolation-source"),
            ),
        )

    # 2. psa_pop_history
    for h in card.get("history-psa", []):
        db.execute(
            """INSERT OR REPLACE INTO psa_pop_history
               (card_id, date, psa_8_base, psa_9_base, psa_10_base, total_base, gem_pct)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (card_id, h["date"], h.get("8-base"), h.get("9-base"),
             h.get("10-base"), h.get("total-base"), h.get("gem-pct")),
        )

    # 3. ebay_history
    for h in card.get("history-ebay", []):
        db.execute(
            """INSERT OR REPLACE INTO ebay_history
               (card_id, date, from_date, active_from, active_to, ended, new,
                ended_rate, ended_raw, new_raw, ended_graded, new_graded,
                ended_psa_10, new_psa_10, ended_psa_9, new_psa_9,
                ended_other_10, new_other_10,
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

    # 4. ebay_market_history
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

    # 5. ebay_derived_history
    for h in card.get("history-ebay-derived", []):
        db.execute(
            """INSERT OR REPLACE INTO ebay_derived_history
               (card_id, date, d_raw_price, d_psa_9_price, d_psa_10_price)
               VALUES (?, ?, ?, ?, ?)""",
            (card_id, h["date"], h.get("d-raw-price"),
             h.get("d-psa-9-price"), h.get("d-psa-10-price")),
        )

    # 6. justtcg_history
    for h in card.get("history-justtcg", []):
        db.execute(
            """INSERT OR REPLACE INTO justtcg_history
               (card_id, date, j_raw_price)
               VALUES (?, ?, ?)""",
            (card_id, h["date"], h.get("j-raw-price")),
        )

    # 7. composite_history
    for h in card.get("history-collectrics", []):
        db.execute(
            """INSERT OR REPLACE INTO composite_history
               (card_id, date, c_raw_price, c_psa_9_price, c_psa_10_price)
               VALUES (?, ?, ?, ?, ?)""",
            (card_id, h["date"], h.get("c-raw-price"),
             h.get("c-psa-9-price"), h.get("c-psa-10-price")),
        )

    # Market pressure aggregates
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


# ============================================================
# Set-level data
# ============================================================

def sync_all_set_details(set_codes: list[str]):
    """Fetch full set details (rarity breakdown + history) for every set."""
    print(f"Syncing detailed data for {len(set_codes)} sets...")

    for i, set_code in enumerate(set_codes, 1):
        try:
            data = fetch_json(f"/api/set/{set_code}")
        except Exception as e:
            print(f"  [{i}/{len(set_codes)}] {set_code}: ERROR {e}")
            continue

        with get_db() as db:
            upsert_set(
                db, set_code,
                data.get("set-name", set_code),
                data.get("release-date"),
                data.get("psa-pop-url"),
            )

            # Rarity breakdown
            for rcode, r in data.get("rarity-breakdown", {}).items():
                set_rarity = r.get("set-rarity") or f"{set_code}_{rcode}"
                db.execute(
                    """INSERT OR REPLACE INTO rarities
                       (set_rarity, set_code, rarity_code, rarity_name,
                        card_count, pull_rate, pull_rate_odds)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        set_rarity, set_code,
                        r.get("rarity-code", rcode),
                        r.get("rarity-name", ""),
                        r.get("card-count", 0),
                        r.get("pull-rate", 0),
                        r.get("pull-rate-odds"),
                    ),
                )

                today = data.get("generated-at")
                if today:
                    db.execute(
                        """INSERT OR REPLACE INTO set_rarity_snapshot
                           (set_rarity, date, avg_raw_price, avg_psa_10_price,
                            ev_raw_per_pack, ev_psa_10_per_pack,
                            psa_pop_10_base, psa_pop_total_base, psa_avg_gem_pct)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            set_rarity, today,
                            r.get("avg-raw-price"), r.get("avg-psa-10-price"),
                            r.get("ev-raw-per-pack"), r.get("ev-psa-10-per-pack"),
                            r.get("psa-pop-10-base"), r.get("psa-pop-total-base"),
                            r.get("psa-avg-gem-pct"),
                        ),
                    )

            # Set history
            for h in data.get("history", []):
                db.execute(
                    """INSERT OR REPLACE INTO set_daily
                       (set_code, date, ev_raw_per_pack, ev_psa_10_per_pack,
                        avg_pack_cost, avg_gain_loss, total_set_raw_value)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        set_code, h["date"],
                        h.get("ev-raw-per-pack"), h.get("ev-psa-10-per-pack"),
                        h.get("avg-pack-cost"), h.get("avg-gain-loss"),
                        h.get("total-set-raw-value"),
                    ),
                )

            # Pack cost components
            pcc = data.get("pack-cost-components", {})
            pcs = data.get("pack-cost-sample-counts", {})
            today = data.get("generated-at")
            if pcc and today:
                db.execute(
                    """INSERT OR REPLACE INTO pack_cost
                       (set_code, date, avg_booster_pack, avg_sleeved_booster_pack,
                        avg_booster_bundle_per_pack, avg_pack_cost,
                        booster_pack_count, sleeved_booster_count, booster_bundle_count)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        set_code, today,
                        pcc.get("avg-booster-pack"),
                        pcc.get("avg-sleeved-booster-pack"),
                        pcc.get("avg-booster-bundle-per-pack"),
                        data.get("avg-pack-cost"),
                        pcs.get("booster-pack"),
                        pcs.get("sleeved-booster-pack"),
                        pcs.get("booster-bundle"),
                    ),
                )

        print(f"  [{i}/{len(set_codes)}] {set_code}: OK ({len(data.get('history', []))} history rows)")


# ============================================================
# Per-card detail sync
# ============================================================

def fetch_card_detail(card_id: str):
    """Fetch one card's full detail with all 7 history arrays."""
    return fetch_json(f"/api/card/{card_id}?include=ebay")


def sync_card_detail(card_id: str) -> tuple[str, bool, str]:
    """Worker: fetch + write one card. Returns (id, success, message)."""
    try:
        detail = fetch_card_detail(card_id)
        with get_db() as db:
            upsert_card(db, detail)
            insert_history_arrays(db, card_id, detail)
        return (card_id, True, "ok")
    except Exception as e:
        return (card_id, False, str(e)[:100])


def sync_all_card_details(card_ids: list[str], workers: int = 6, resume: bool = False):
    """Fetch and write full detail for every card in parallel."""
    if resume:
        # Skip cards that already have at least 50 history rows (full history)
        with get_db() as db:
            existing = {
                row["card_id"] for row in
                db.execute(
                    "SELECT card_id FROM price_history GROUP BY card_id HAVING COUNT(*) >= 50"
                ).fetchall()
            }
        before = len(card_ids)
        card_ids = [cid for cid in card_ids if cid not in existing]
        print(f"Resume mode: {before} total, skipping {before - len(card_ids)} already complete, {len(card_ids)} remaining")

    if not card_ids:
        print("Nothing to sync")
        return

    print(f"Syncing {len(card_ids)} cards with {workers} parallel workers...")
    start = time.time()
    succeeded = 0
    failed = 0
    failures = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(sync_card_detail, cid): cid for cid in card_ids}

        for i, future in enumerate(as_completed(futures), 1):
            card_id, ok, msg = future.result()
            if ok:
                succeeded += 1
            else:
                failed += 1
                failures.append((card_id, msg))

            if i % 100 == 0 or i == len(card_ids):
                elapsed = time.time() - start
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(card_ids) - i) / rate if rate > 0 else 0
                print(f"  [{i}/{len(card_ids)}] ok={succeeded} fail={failed} rate={rate:.1f}/s eta={eta/60:.1f}m")

    elapsed = time.time() - start
    print(f"\nDone in {elapsed/60:.1f}m: {succeeded} succeeded, {failed} failed")

    if failures:
        print(f"\nFirst 10 failures:")
        for cid, msg in failures[:10]:
            print(f"  {cid}: {msg}")


# ============================================================
# Main
# ============================================================

def verify_completeness():
    """Print database stats grouped by set."""
    print("\n" + "=" * 60)
    print("COMPLETENESS REPORT")
    print("=" * 60)

    with get_db() as db:
        per_set = db.execute("""
            SELECT
                s.set_code,
                s.set_name,
                COUNT(DISTINCT c.id) as cards,
                COUNT(DISTINCT CASE WHEN c.sealed_product = 'N' THEN c.id END) as singles,
                COUNT(DISTINCT CASE WHEN c.sealed_product = 'Y' THEN c.id END) as sealed,
                COUNT(DISTINCT ph.card_id) as cards_with_history
            FROM sets s
            LEFT JOIN cards c ON c.set_code = s.set_code
            LEFT JOIN price_history ph ON ph.card_id = c.id
            GROUP BY s.set_code
            ORDER BY s.set_code
        """).fetchall()

        print(f"\n{'SET':6s} {'NAME':35s} {'CARDS':>7s} {'SINGLES':>8s} {'SEALED':>7s} {'HIST':>6s}")
        print("-" * 80)
        total_cards = total_hist = 0
        for r in per_set:
            print(f"{r['set_code']:6s} {(r['set_name'] or '')[:35]:35s} "
                  f"{r['cards']:>7d} {r['singles']:>8d} {r['sealed']:>7d} "
                  f"{r['cards_with_history']:>6d}")
            total_cards += r["cards"]
            total_hist += r["cards_with_history"]

        print("-" * 80)
        print(f"{'TOTAL':6s} {'':35s} {total_cards:>7d} {'':>8s} {'':>7s} {total_hist:>6d}")

        # Coverage
        pct = (total_hist / total_cards * 100) if total_cards else 0
        print(f"\nHistory coverage: {pct:.1f}% ({total_hist}/{total_cards})")

        # Total history rows
        for table in ("price_history", "ebay_history", "composite_history",
                       "psa_pop_history", "ebay_market_history", "ebay_derived_history",
                       "justtcg_history", "market_pressure", "supply_saturation"):
            count = db.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()["c"]
            print(f"  {table:25s} {count:>10,}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=6, help="Parallel workers")
    parser.add_argument("--resume", action="store_true", help="Skip cards already in price_history")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of cards (for testing)")
    parser.add_argument("--sets-only", action="store_true", help="Only sync set-level data, skip cards")
    parser.add_argument("--verify-only", action="store_true", help="Only print completeness report")
    args = parser.parse_args()

    if args.verify_only:
        verify_completeness()
        return

    init_db()

    # Discover catalog
    catalog = discover_all_card_ids()

    # Insert all card metadata first
    print(f"\nInserting card metadata for {len(catalog['cards'])} cards + {len(catalog['sealed'])} sealed...")
    with get_db() as db:
        for c in catalog["cards"]:
            try:
                upsert_card(db, c)
            except Exception as e:
                print(f"  failed insert card {c.get('id')}: {e}")

        # Sealed: derive set_code from set_name
        set_name_to_code = {row["set_name"]: row["set_code"] for row in db.execute("SELECT set_code, set_name FROM sets")}
        for s in catalog["sealed"]:
            sc = set_name_to_code.get(s.get("set-name", ""))
            if not sc:
                continue
            s2 = {
                "id": s["id"],
                "product-name": s["product-name"],
                "set-code": sc,
                "set-name": s.get("set-name", ""),
                "sealed-product": "Y",
                "sealed-type": s["sealed-type"],
                "set-value-include": "N",
            }
            try:
                upsert_card(db, s2)
            except Exception as e:
                print(f"  failed insert sealed {s.get('id')}: {e}")

    # Sync set-level data
    set_codes = list(catalog["sets_meta"].keys())
    sync_all_set_details(set_codes)

    if args.sets_only:
        verify_completeness()
        return

    # Build complete card ID list (singles + sealed)
    all_ids = [c["id"] for c in catalog["cards"]]
    all_ids += [s["id"] for s in catalog["sealed"]]

    if args.limit > 0:
        all_ids = all_ids[: args.limit]

    sync_all_card_details(all_ids, workers=args.workers, resume=args.resume)

    verify_completeness()


if __name__ == "__main__":
    main()
