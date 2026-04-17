#!/usr/bin/env python3
"""Empirical coverage report for model quantile bands.

Reads model_projections + realized outcomes (paper_trades or inferred from
price_history) and computes the empirical coverage of the [confidence_low,
confidence_high] band. Flags miscalibration if empirical coverage deviates
from the stated nominal level by more than MISCALIBRATION_THRESHOLD_PP
(percentage points).

Outputs:
  - data/models/calibration_<timestamp>.json (machine-readable)
  - One-page text summary to stdout

Runs without error on an empty `paper_trades` table; prints an
"insufficient data" notice and exits cleanly.

Usage:
    python3 scripts/calibration_report.py [--db <path>] [--nominal 0.80]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "data" / "pokemon_analytics.db"
OUTPUT_DIR = PROJECT_ROOT / "data" / "models"

NOMINAL_COVERAGE = 0.80
MISCALIBRATION_THRESHOLD_PP = 5.0  # flag if |empirical - nominal| > 5pp
MIN_SAMPLES_FOR_REPORT = 20


def _rows_from_paper_trades(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT card_id, as_of, horizon_days, projected_return,
               confidence_low, confidence_high, realized_return_net,
               realized_return_gross, model_version
        FROM paper_trades
        WHERE realized_return_net IS NOT NULL
           OR realized_return_gross IS NOT NULL
        """
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _rows_from_projections_and_history(
    conn: sqlite3.Connection,
) -> List[Dict[str, Any]]:
    """Fallback: infer realized return from price_history for historical
    projections whose horizon has elapsed."""
    today = dt.date.today().isoformat()
    cur = conn.execute(
        """
        SELECT p.card_id, p.as_of, p.horizon_days, p.projected_return,
               p.confidence_low, p.confidence_high, p.model_version,
               ph_entry.psa_10_price AS entry_price
        FROM model_projections p
        LEFT JOIN price_history ph_entry
          ON ph_entry.card_id = p.card_id AND ph_entry.date = p.as_of
        WHERE date(p.as_of, '+' || p.horizon_days || ' days') <= ?
        """,
        (today,),
    )
    cols = [c[0] for c in cur.description]
    out = []
    for r in cur.fetchall():
        rec = dict(zip(cols, r))
        if not rec["entry_price"] or rec["entry_price"] <= 0:
            continue
        exit_date_cutoff = (
            dt.date.fromisoformat(rec["as_of"])
            + dt.timedelta(days=int(rec["horizon_days"]))
        ).isoformat()
        exit_row = conn.execute(
            """SELECT psa_10_price FROM price_history
               WHERE card_id = ? AND date >= ? AND psa_10_price IS NOT NULL
               ORDER BY date ASC LIMIT 1""",
            (rec["card_id"], exit_date_cutoff),
        ).fetchone()
        if not exit_row or not exit_row[0]:
            continue
        exit_price = float(exit_row[0])
        rec["realized_return_gross"] = (exit_price - rec["entry_price"]) / rec["entry_price"]
        rec["realized_return_net"] = None  # gross-only when inferred
        out.append(rec)
    return out


def compute_coverage(
    rows: List[Dict[str, Any]], nominal: float = NOMINAL_COVERAGE
) -> Dict[str, Any]:
    """Compute empirical coverage + summary stats.

    A row is 'covered' if confidence_low <= realized <= confidence_high.
    Uses realized_return_net when available (reflects friction), else gross.
    """
    n = 0
    covered = 0
    below = 0
    above = 0
    for r in rows:
        lo, hi = r.get("confidence_low"), r.get("confidence_high")
        if lo is None or hi is None:
            continue
        realized = r.get("realized_return_net")
        if realized is None:
            realized = r.get("realized_return_gross")
        if realized is None:
            continue
        n += 1
        if realized < lo:
            below += 1
        elif realized > hi:
            above += 1
        else:
            covered += 1

    if n == 0:
        return {"n_samples": 0, "empirical_coverage": None,
                "nominal_coverage": nominal, "miscalibrated": None,
                "below_band": 0, "above_band": 0}

    empirical = covered / n
    deviation_pp = abs(empirical - nominal) * 100
    return {
        "n_samples": n,
        "empirical_coverage": round(empirical, 4),
        "nominal_coverage": nominal,
        "deviation_pp": round(deviation_pp, 2),
        "miscalibrated": deviation_pp > MISCALIBRATION_THRESHOLD_PP,
        "below_band": below,
        "above_band": above,
        "covered": covered,
    }


def format_text_report(result: Dict[str, Any]) -> str:
    lines = [
        "=" * 62,
        " Quantile Band Calibration Report",
        "=" * 62,
        f"  Nominal coverage:     {result['nominal_coverage']:.0%}",
    ]
    n = result.get("n_samples", 0)
    if not n:
        lines.append("  Samples available:    0")
        lines.append("")
        lines.append("  INSUFFICIENT DATA — no realized returns to compare.")
        lines.append("  Backfill paper_trades or wait for projection horizons")
        lines.append("  to elapse, then re-run.")
        lines.append("=" * 62)
        return "\n".join(lines)

    lines += [
        f"  Samples evaluated:    {n}",
        f"  Empirical coverage:   {result['empirical_coverage']:.2%}",
        f"  Deviation from nom.:  {result['deviation_pp']:.2f} pp",
        f"  Below band (too-low): {result['below_band']}",
        f"  Above band (too-hi):  {result['above_band']}",
        "",
    ]
    if n < MIN_SAMPLES_FOR_REPORT:
        lines.append(f"  WARNING: <{MIN_SAMPLES_FOR_REPORT} samples — result is noisy.")
    if result.get("miscalibrated"):
        lines.append(
            f"  FLAG: MISCALIBRATED — deviation exceeds "
            f"{MISCALIBRATION_THRESHOLD_PP}pp threshold."
        )
    else:
        lines.append("  Status: within tolerance.")
    lines.append("=" * 62)
    return "\n".join(lines)


def run(db_path: Path, nominal: float) -> int:
    if not db_path.exists():
        print(f"[calibration] DB not found at {db_path} — nothing to check.",
              file=sys.stderr)
        return 0

    conn = sqlite3.connect(str(db_path))
    try:
        try:
            rows = _rows_from_paper_trades(conn)
        except sqlite3.OperationalError:
            rows = []

        if not rows:
            try:
                rows = _rows_from_projections_and_history(conn)
            except sqlite3.OperationalError:
                rows = []
    finally:
        conn.close()

    result = compute_coverage(rows, nominal=nominal)
    result["generated_at"] = dt.datetime.utcnow().isoformat() + "Z"
    result["db_path"] = str(db_path)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = OUTPUT_DIR / f"calibration_{ts}.json"
    out_path.write_text(json.dumps(result, indent=2))

    print(format_text_report(result))
    print(f"\nReport written to: {out_path}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", type=Path, default=DEFAULT_DB,
                    help="Path to SQLite DB (default: data/pokemon_analytics.db)")
    ap.add_argument("--nominal", type=float, default=NOMINAL_COVERAGE,
                    help="Stated nominal band coverage (default: 0.80)")
    args = ap.parse_args()
    sys.exit(run(args.db, args.nominal))


if __name__ == "__main__":
    main()
