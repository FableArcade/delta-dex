"""Production model audit.

Runs a battery of sanity checks and sanity backtests against the currently
promoted v1_3 model, without retraining anything. Human-readable output.

Checks:
  1. Model artifacts — version pointer, files exist, sizes reasonable
  2. Report card — what DB says the model scores
  3. Projections table — freshness, distribution, coverage
  4. Confidence calibration — does the 80% band actually cover 80% of realized?
  5. Top-picks spot check — the actual top-2% pick list with full context
  6. Feature importance — are drivers reasonable?
  7. Paper trade record — live track record if any
  8. Out-of-sample recency test — projections issued today that ALREADY
     have partial forward realization (anchors 60-180d old)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from db.connection import get_db

MODELS_DIR = Path(__file__).resolve().parent.parent / "data" / "models"


def h(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def sub(title: str) -> None:
    print(f"\n— {title} —")


def _fmt_pct(v: float) -> str:
    return f"{v * 100:+.2f}%" if np.isfinite(v) else "—"


def check_artifacts() -> str:
    h("1 · MODEL ARTIFACTS")
    ver_path = MODELS_DIR / "latest_version.txt"
    if not ver_path.exists():
        print("✗ latest_version.txt missing")
        return ""
    version = ver_path.read_text().strip()
    print(f"production version: {version}")

    needed = [f"{k}_{version}.lgb" for k in ("median", "lower", "upper")]
    for f in needed:
        p = MODELS_DIR / f
        status = "✓" if p.exists() else "✗ MISSING"
        size = p.stat().st_size // 1024 if p.exists() else 0
        print(f"  {status}  {f}  ({size}KB)")

    imp_path = MODELS_DIR / f"feature_importance_{version}.json"
    if imp_path.exists():
        imp = json.loads(imp_path.read_text())
        print(f"\nTop 5 features by importance:")
        for feat, pct in list(imp.items())[:5]:
            print(f"  {pct:>5.2f}%  {feat}")
    return version


def check_report_card(db, version: str) -> None:
    h("2 · REPORT CARD")
    row = db.execute(
        "SELECT * FROM model_report_card WHERE model_version = ?",
        (version,),
    ).fetchone()
    if not row:
        print("✗ no report card row")
        return

    r = dict(row)
    metrics = [
        ("R² OOS",                r["r_squared_oos"],          lambda v: v > 0.05),
        ("Spearman OOS",          r["spearman_oos"],           lambda v: v > 0.15),
        ("Hit rate (top-decile)", r["hit_rate_positive"],      lambda v: v > 0.55),
        ("Top-decile net return", r["mean_return_top_decile"], lambda v: v > 0),
        ("Bottom-decile return",  r["mean_return_bottom_decile"], lambda v: v < 0),
        ("Decile spread",         r["decile_spread"],          lambda v: v > 0.10),
    ]
    for name, val, ok in metrics:
        mark = "✓" if val is not None and ok(val) else "·"
        val_str = _fmt_pct(val) if "return" in name.lower() or "rate" in name.lower() or "spread" in name.lower() else f"{val:.4f}"
        print(f"  {mark}  {name:<24} {val_str}")
    print(f"\nstatus: {r.get('promotion_status', '?')}")


def check_projections(db) -> None:
    h("3 · PROJECTIONS TABLE")
    row = db.execute(
        """SELECT MAX(as_of) AS latest, COUNT(*) AS total,
                  SUM(CASE WHEN projected_return > 0 THEN 1 ELSE 0 END) AS positive,
                  SUM(CASE WHEN confidence_low > 0 THEN 1 ELSE 0 END) AS strong_conviction
             FROM model_projections WHERE horizon_days = 180"""
    ).fetchone()
    print(f"latest as_of:       {row['latest']}")
    print(f"total projections:  {row['total']}")
    print(f"  positive return:  {row['positive']} ({row['positive']/row['total']*100:.1f}%)")
    print(f"  confLow > 0:      {row['strong_conviction']} ({row['strong_conviction']/row['total']*100:.1f}%)")

    # Projection distribution
    rows = db.execute(
        """SELECT projected_return FROM model_projections
            WHERE horizon_days = 180 AND as_of = ?""",
        (row["latest"],),
    ).fetchall()
    preds = np.array([r[0] for r in rows])
    print(f"\ndistribution of predicted 180d net return:")
    for q in [0.01, 0.05, 0.50, 0.95, 0.99]:
        print(f"  p{int(q*100):>2}  {_fmt_pct(np.quantile(preds, q))}")


def check_calibration(db, version: str) -> None:
    h("4 · CONFIDENCE CALIBRATION")
    print("""Checks: do the model's 80% confidence bands actually contain the
realized return 80% of the time? We can only check this for projections
issued 180+ days ago with observable realized prices.""")

    # Pull projections old enough that forward realization exists.
    rows = db.execute(
        """SELECT mp.card_id, mp.as_of, mp.projected_return,
                  mp.confidence_low, mp.confidence_high
             FROM model_projections mp
            WHERE mp.horizon_days = 180
              AND mp.as_of <= date('now','-180 days')
              AND mp.model_version = ?""",
        (version,),
    ).fetchall()

    if not rows:
        print("  (no projections older than 180 days for this model version —")
        print("   calibration check not yet possible; needs another 6 months)")
        return

    hits, total, mae = 0, 0, []
    for r in rows:
        # Actual realized 180d return
        anchor = r["as_of"]
        sell_row = db.execute(
            """SELECT psa_10_price FROM price_history
                WHERE card_id = ? AND date >= date(?, '+180 days')
                  AND psa_10_price IS NOT NULL
                ORDER BY date ASC LIMIT 1""",
            (r["card_id"], anchor),
        ).fetchone()
        buy_row = db.execute(
            """SELECT psa_10_price FROM price_history
                WHERE card_id = ? AND date >= ? AND date <= date(?, '+31 days')
                  AND psa_10_price IS NOT NULL
                ORDER BY date ASC LIMIT 1""",
            (r["card_id"], anchor, anchor),
        ).fetchone()
        if not sell_row or not buy_row: continue
        buy, sell = buy_row[0], sell_row[0]
        if not buy or buy <= 0: continue
        realized = (sell * (1 - 0.13) - 5 - buy) / buy  # net of fees + shipping
        total += 1
        mae.append(abs(realized - r["projected_return"]))
        if r["confidence_low"] <= realized <= r["confidence_high"]:
            hits += 1

    if total == 0:
        print("  no priced pairs — skipping")
        return
    print(f"\nsamples:          {total}")
    print(f"band coverage:    {hits}/{total} = {hits/total*100:.1f}%")
    print(f"                  (target ~80% for conformal band, higher = bands too wide)")
    print(f"MAE:              {_fmt_pct(np.mean(mae))}")


def check_top_picks(db) -> None:
    h("5 · TOP-2% PICK SPOT CHECK ($100+ PSA 10 universe)")
    rows = db.execute(
        """
        WITH latest AS (
          SELECT card_id, MAX(as_of) AS max_date
          FROM model_projections WHERE horizon_days=180 GROUP BY card_id
        ),
        lp AS (
          SELECT card_id, MAX(date) AS max_date
          FROM price_history WHERE psa_10_price IS NOT NULL GROUP BY card_id
        )
        SELECT c.id, c.product_name, c.set_code, ph.psa_10_price AS psa10,
               mp.projected_return AS proj, mp.confidence_low AS cl, mp.confidence_high AS ch
        FROM model_projections mp
        JOIN latest l ON l.card_id=mp.card_id AND l.max_date=mp.as_of
        JOIN cards c ON c.id = mp.card_id
        JOIN lp ON lp.card_id=mp.card_id
        JOIN price_history ph ON ph.card_id=mp.card_id AND ph.date=lp.max_date
        WHERE mp.horizon_days=180 AND ph.psa_10_price >= 100
        ORDER BY mp.projected_return DESC
        """,
    ).fetchall()

    total = len(rows)
    n_top2 = max(1, int(round(0.02 * total)))
    print(f"universe: {total} cards")
    print(f"top 2% = {n_top2} picks\n")
    print(f"{'#':<3} {'card':<38} {'set':<5} {'psa10':>8} {'proj':>8} {'conf':>18}")
    print("-" * 88)
    for i, r in enumerate(rows[:n_top2], 1):
        conf = f"[{_fmt_pct(r['cl'])}, {_fmt_pct(r['ch'])}]"
        print(f"{i:<3} {r['product_name'][:38]:<38} {r['set_code']:<5} ${r['psa10']:>7.0f} {_fmt_pct(r['proj']):>8} {conf:>18}")


def check_paper_trade(db) -> None:
    h("6 · PAPER TRADE TRACK RECORD")
    try:
        n_rows = db.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0]
        if n_rows == 0:
            print("  no paper trades yet — live track record starts accruing")
            print("  after lock_top_decile_today.py runs for a few days")
            return
        closed = db.execute(
            """SELECT COUNT(*) AS n, AVG(realized_net_return) AS avg_ret,
                      SUM(CASE WHEN realized_net_return > 0 THEN 1 ELSE 0 END) AS wins
                 FROM paper_trades WHERE realized_net_return IS NOT NULL"""
        ).fetchone()
        print(f"  total locked:  {n_rows}")
        if closed["n"]:
            print(f"  evaluated:    {closed['n']}")
            print(f"  avg realized: {_fmt_pct(closed['avg_ret'] or 0)}")
            print(f"  hit rate:     {closed['wins']/closed['n']*100:.1f}% ({closed['wins']}/{closed['n']})")
        else:
            print(f"  maturing (none closed yet — T+180 not reached)")
    except Exception as exc:
        print(f"  paper_trades table not available: {exc}")


def main() -> None:
    version = check_artifacts()
    if not version:
        sys.exit(1)
    with get_db() as db:
        check_report_card(db, version)
        check_projections(db)
        check_calibration(db, version)
        check_top_picks(db)
        check_paper_trade(db)
    print("\n" + "=" * 72)
    print("  audit complete")
    print("=" * 72)


if __name__ == "__main__":
    main()
