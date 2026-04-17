"""Confirm today's top-decile paper trades are locked and print the watchlist.

Output:
    - Terminal table of the top-20 cards from today's top_decile cohort,
      sorted by projected_return desc, with name/set/entry price/predicted
      T+90 return and the cohort quantile bands.
    - CSV written to data/logs/top_decile_watchlist_<date>.csv
"""

from __future__ import annotations

import csv
import datetime as dt
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DB_PATH  # noqa: E402

HORIZON_DAYS = 180


def _quantile_bands(conn: sqlite3.Connection, as_of: str) -> Dict[str, float]:
    """Compute P25/P75/P90 bands for projected_return in the day's batch."""
    rows = [r[0] for r in conn.execute(
        """SELECT projected_return FROM paper_trades
            WHERE as_of = ? AND projected_return IS NOT NULL
            ORDER BY projected_return ASC""",
        (as_of,),
    ).fetchall()]
    if not rows:
        return {}
    n = len(rows)

    def q(p: float) -> float:
        idx = min(int(p * n), n - 1)
        return rows[idx]
    return {"p25": q(0.25), "p75": q(0.75), "p90": q(0.90),
            "min": rows[0], "max": rows[-1], "n": n}


def run(as_of: Optional[str] = None, top_n: int = 20) -> Dict[str, Any]:
    as_of = as_of or dt.date.today().isoformat()
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found at {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH), timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(paper_trades)").fetchall()}
        if "cohort" not in cols:
            raise SystemExit("paper_trades.cohort missing. "
                             "Run scripts/migrate_paper_trade_cohort.py first.")

        bands = _quantile_bands(conn, as_of)

        total_today = conn.execute(
            "SELECT COUNT(*) FROM paper_trades WHERE as_of = ?", (as_of,),
        ).fetchone()[0]
        decile_count = conn.execute(
            "SELECT COUNT(*) FROM paper_trades WHERE as_of = ? AND cohort = 'top_decile'",
            (as_of,),
        ).fetchone()[0]

        rows = conn.execute(
            """SELECT pt.card_id,
                      c.product_name,
                      c.set_code,
                      c.card_number,
                      pt.entry_price,
                      pt.projected_return,
                      pt.confidence_low,
                      pt.confidence_high,
                      pt.cohort,
                      pt.horizon_days
                 FROM paper_trades pt
                 LEFT JOIN cards c ON c.id = pt.card_id
                WHERE pt.as_of = ?
                  AND pt.cohort = 'top_decile'
                ORDER BY pt.projected_return DESC
                LIMIT ?""",
            (as_of, top_n),
        ).fetchall()
    finally:
        conn.close()

    maturity = (dt.date.fromisoformat(as_of) + dt.timedelta(days=HORIZON_DAYS)).isoformat()

    print("=" * 96)
    print(f"Top-decile watchlist  as_of={as_of}  T+{HORIZON_DAYS} maturity={maturity}")
    print(f"Locked today: {total_today} trades   top_decile cohort: {decile_count}")
    if bands:
        print(f"Quantile bands (projected_return): "
              f"P25={bands['p25']*100:+.2f}%  P75={bands['p75']*100:+.2f}%  "
              f"P90={bands['p90']*100:+.2f}%  max={bands['max']*100:+.2f}%")
    print("=" * 96)
    header = (f"{'#':>3} {'card_id':<14} {'set':<8} {'name':<40} "
              f"{'entry':>9} {'pred_T+90':>10}")
    print(header)
    print("-" * len(header))

    watchlist: List[Dict[str, Any]] = []
    for i, r in enumerate(rows, 1):
        name = (r["product_name"] or "?")[:40]
        print(
            f"{i:>3} {str(r['card_id'])[:14]:<14} "
            f"{str(r['set_code'] or ''):<8} "
            f"{name:<40} "
            f"${(r['entry_price'] or 0):>8.2f} "
            f"{(r['projected_return'] or 0) * 100:>+9.2f}%"
        )
        watchlist.append({
            "rank": i,
            "card_id": r["card_id"],
            "product_name": r["product_name"],
            "set_code": r["set_code"],
            "card_number": r["card_number"],
            "entry_price": r["entry_price"],
            "projected_return": r["projected_return"],
            "confidence_low": r["confidence_low"],
            "confidence_high": r["confidence_high"],
            "cohort": r["cohort"],
            "horizon_days": r["horizon_days"],
            "as_of": as_of,
            "maturity_date": maturity,
        })

    # CSV output
    log_dir = Path(__file__).resolve().parent.parent / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    csv_path = log_dir / f"top_decile_watchlist_{as_of}.csv"
    with csv_path.open("w", newline="") as fh:
        fieldnames = ["rank", "card_id", "product_name", "set_code", "card_number",
                      "entry_price", "projected_return", "confidence_low",
                      "confidence_high", "cohort", "horizon_days", "as_of",
                      "maturity_date", "quantile_p25", "quantile_p75", "quantile_p90"]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in watchlist:
            row = dict(row)
            row["quantile_p25"] = bands.get("p25") if bands else None
            row["quantile_p75"] = bands.get("p75") if bands else None
            row["quantile_p90"] = bands.get("p90") if bands else None
            writer.writerow(row)
    print(f"\nCSV written to {csv_path}")

    return {
        "as_of": as_of,
        "maturity_date": maturity,
        "total_locked": total_today,
        "top_decile_count": decile_count,
        "watchlist": watchlist,
        "bands": bands,
        "csv_path": str(csv_path),
    }


if __name__ == "__main__":
    run()
