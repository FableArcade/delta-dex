"""Precompute set-alpha linkage for every chase card and persist to the DB.

For each set, identifies the top-priced card of the top rarity tier (the
"alpha") and for every other chase card in that set computes:

  * contemp_corr  — Pearson correlation of monthly PSA 10 returns
  * lead_corr     — Pearson correlation of alpha[t] vs beta[t+1]
  * n_months      — observation count used

Results land in a new `set_alpha_linkage` table. Refresh monthly as prices
roll forward.

Run:  python scripts/build_set_alpha_linkage.py
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


def load_monthly_psa10(con):
    rows = con.execute("""
        SELECT p.card_id, c.product_name, c.set_code, c.rarity_name,
               p.date, p.psa_10_price
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
                "rarity": r["rarity_name"],
                "series": {},
            }
        cards[cid]["series"].setdefault(month, r["psa_10_price"])
    return cards


def monthly_returns(series):
    months = sorted(series.keys())
    out = []
    for i in range(1, len(months)):
        p0, p1 = series[months[i - 1]], series[months[i]]
        if p0 and p0 > 0 and p1 and p1 > 0:
            out.append((months[i], p1 / p0 - 1))
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


def align(ra, rb, lag=0):
    db = dict(ra)
    xs, ys = [], []
    for month, rb_val in rb:
        y, m = int(month[:4]), int(month[5:7])
        m -= lag
        while m < 1:
            m += 12; y -= 1
        a_month = f"{y:04d}-{m:02d}"
        if a_month in db:
            xs.append(db[a_month])
            ys.append(rb_val)
    return xs, ys


def main():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    con.execute("""
        CREATE TABLE IF NOT EXISTS set_alpha_linkage (
            card_id         TEXT PRIMARY KEY REFERENCES cards(id),
            set_code        TEXT NOT NULL,
            alpha_card_id   TEXT NOT NULL REFERENCES cards(id),
            alpha_name      TEXT NOT NULL,
            contemp_corr    REAL,
            lead_corr       REAL,
            n_months        INTEGER,
            updated_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_sal_alpha ON set_alpha_linkage(alpha_card_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_sal_set ON set_alpha_linkage(set_code)")

    cards = load_monthly_psa10(con)
    by_set: dict = defaultdict(list)
    for cid, data in cards.items():
        by_set[data["set"]].append((cid, data))

    rows_to_write = []
    for set_code, card_list in by_set.items():
        # Alpha = highest peak PSA 10 in the set among top-tier rarities
        ranked = sorted(
            ((cid, d, max(d["series"].values())) for cid, d in card_list),
            key=lambda x: x[2], reverse=True,
        )
        if len(ranked) < 2:
            continue
        alpha_cid, alpha_data, _ = ranked[0]
        alpha_ret = monthly_returns(alpha_data["series"])
        if len(alpha_ret) < MIN_MONTHS:
            continue

        # Alpha references itself with corr=1.0 so card detail on the alpha
        # itself returns something meaningful ("this IS the alpha").
        rows_to_write.append((
            alpha_cid, set_code, alpha_cid, alpha_data["name"],
            1.0, None, len(alpha_ret) + 1,
        ))

        for beta_cid, beta_data, _ in ranked[1:]:
            b_ret = monthly_returns(beta_data["series"])
            xs, ys = align(alpha_ret, b_ret, lag=0)
            if len(xs) < MIN_OVERLAP:
                continue
            c0 = pearson(xs, ys)
            xs1, ys1 = align(alpha_ret, b_ret, lag=1)
            c1 = pearson(xs1, ys1) if len(xs1) >= MIN_OVERLAP else None
            if c0 is None:
                continue
            rows_to_write.append((
                beta_cid, set_code, alpha_cid, alpha_data["name"],
                c0, c1, len(xs),
            ))

    con.execute("DELETE FROM set_alpha_linkage")
    con.executemany("""
        INSERT INTO set_alpha_linkage
            (card_id, set_code, alpha_card_id, alpha_name,
             contemp_corr, lead_corr, n_months)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows_to_write)
    con.commit()

    print(f"Wrote {len(rows_to_write)} linkage rows across {len(by_set)} sets.")
    # Show PRE as a sanity check
    cur = con.execute("""
        SELECT card_id, contemp_corr, lead_corr, alpha_name
          FROM set_alpha_linkage
         WHERE set_code = 'PRE'
      ORDER BY contemp_corr DESC LIMIT 10
    """).fetchall()
    print("\nPRE top-10 linkages:")
    for r in cur:
        name = con.execute("SELECT product_name FROM cards WHERE id=?", (r["card_id"],)).fetchone()[0]
        lead = f"{r['lead_corr']:+.3f}" if r['lead_corr'] is not None else "n/a"
        print(f"  {name[:32]:<32} ρ={r['contemp_corr']:+.3f}  lead={lead:>7}  (α: {r['alpha_name']})")


if __name__ == "__main__":
    main()
