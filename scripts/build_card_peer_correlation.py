"""Pairwise PSA 10 return correlation between every pair of chase cards
within the same set. Persisted to card_peer_correlation so the card detail
page can surface a card's OWN peer cluster — not just its set alpha.

Only stores pairs with ρ ≥ 0.40 (noise floor) to keep the table small.
"""
from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from pathlib import Path

DB = Path(__file__).resolve().parents[1] / "data" / "pokemon.db"
TOP_TIERS = ("Special Illustration Rare", "Hyper Rare", "Illustration Rare")
MIN_MONTHS = 6
MIN_OVERLAP = 6
MIN_CORR = 0.40


def load_monthly_psa10(con):
    rows = con.execute("""
        SELECT p.card_id, c.product_name, c.set_code, p.date, p.psa_10_price
        FROM price_history p
        JOIN cards c ON c.id = p.card_id
        WHERE p.psa_10_price IS NOT NULL
          AND p.psa_10_price >= 20
          AND c.sealed_product = 'N'
          AND c.rarity_name IN (?, ?, ?)
        ORDER BY p.card_id, p.date
    """, TOP_TIERS).fetchall()

    cards: dict = {}
    for r in rows:
        cid = r["card_id"]
        month = r["date"][:7]
        if cid not in cards:
            cards[cid] = {
                "name": r["product_name"],
                "set": r["set_code"],
                "series": {},
            }
        cards[cid]["series"].setdefault(month, r["psa_10_price"])
    return cards


def monthly_returns(series):
    months = sorted(series.keys())
    out = {}
    for i in range(1, len(months)):
        p0, p1 = series[months[i - 1]], series[months[i]]
        if p0 and p0 > 0 and p1 and p1 > 0:
            out[months[i]] = p1 / p0 - 1
    return out


def pearson(xs, ys):
    n = len(xs)
    if n < 4:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    dx = math.sqrt(sum((xs[i] - mx) ** 2 for i in range(n)))
    dy = math.sqrt(sum((ys[i] - my) ** 2 for i in range(n)))
    return num / (dx * dy) if dx and dy else None


def main():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    con.execute("""
        CREATE TABLE IF NOT EXISTS card_peer_correlation (
            card_a     TEXT NOT NULL REFERENCES cards(id),
            card_b     TEXT NOT NULL REFERENCES cards(id),
            set_code   TEXT NOT NULL,
            corr       REAL NOT NULL,
            n_months   INTEGER NOT NULL,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (card_a, card_b)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_cpc_a ON card_peer_correlation(card_a, corr DESC)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_cpc_set ON card_peer_correlation(set_code)")

    cards = load_monthly_psa10(con)
    returns = {cid: monthly_returns(d["series"]) for cid, d in cards.items()}

    # Group by set
    by_set: dict = defaultdict(list)
    for cid, d in cards.items():
        if len(returns.get(cid, {})) >= MIN_MONTHS:
            by_set[d["set"]].append(cid)

    rows = []
    for set_code, cid_list in by_set.items():
        for i, a in enumerate(cid_list):
            ra = returns[a]
            for b in cid_list[i + 1:]:
                rb = returns[b]
                shared = [m for m in ra if m in rb]
                if len(shared) < MIN_OVERLAP:
                    continue
                xs = [ra[m] for m in shared]
                ys = [rb[m] for m in shared]
                c = pearson(xs, ys)
                if c is None or c < MIN_CORR:
                    continue
                rows.append((a, b, set_code, c, len(shared)))
                rows.append((b, a, set_code, c, len(shared)))

    con.execute("DELETE FROM card_peer_correlation")
    con.executemany("""
        INSERT INTO card_peer_correlation
            (card_a, card_b, set_code, corr, n_months)
        VALUES (?, ?, ?, ?, ?)
    """, rows)
    con.commit()

    n_unique = len(rows) // 2
    print(f"Wrote {len(rows)} rows ({n_unique} unique pairs, ρ ≥ {MIN_CORR}).")
    # PRE Glaceon sanity check — should show its cluster
    glaceon_peers = con.execute("""
        SELECT cpc.card_b, c.product_name, cpc.corr, cpc.n_months
          FROM card_peer_correlation cpc
          JOIN cards c ON c.id = cpc.card_b
         WHERE cpc.card_a = '8244593'
      ORDER BY cpc.corr DESC
    """).fetchall()
    print(f"\nGlaceon ex #150 peers ({len(glaceon_peers)} cards, ρ ≥ {MIN_CORR}):")
    for r in glaceon_peers:
        print(f"  {r['product_name'][:32]:<32} ρ={r['corr']:+.3f}  (n={r['n_months']})")


if __name__ == "__main__":
    main()
