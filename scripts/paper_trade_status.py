"""Paper-trade status reporter.

Usage:
    python scripts/paper_trade_status.py
    python scripts/paper_trade_status.py --cohort top_decile

Reports, per cohort:
    - locked count
    - evaluated count (T+90 reached)
    - mean realized net return
    - hit rate
    - Sharpe (of net returns)
    - mean predicted return (calibration vs. realized)

For unevaluated trades, prints a "maturing on <date>" breakdown.

Also writes a JSON snapshot to data/logs/paper_trade_status_<ts>.json.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DB_PATH  # noqa: E402

HORIZON_DAYS = 180
COHORTS = ["top_decile", "top_quartile", "middle", "bottom_quartile"]


def _mean(xs: List[float]) -> Optional[float]:
    return sum(xs) / len(xs) if xs else None


def _stdev(xs: List[float]) -> Optional[float]:
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    var = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(var)


def _sharpe(xs: List[float]) -> Optional[float]:
    """Raw Sharpe on the sample of net returns (per trade, not annualized)."""
    m = _mean(xs)
    s = _stdev(xs)
    if m is None or s is None or s == 0:
        return None
    return m / s


def _fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "   n/a"
    return f"{v * 100:+6.2f}%"


def _fmt_num(v: Optional[float], width: int = 6, prec: int = 3) -> str:
    if v is None:
        return " " * (width - 3) + "n/a"
    return f"{v:{width}.{prec}f}"


def _cohort_stats(conn: sqlite3.Connection, cohort: Optional[str]) -> Dict[str, Any]:
    where = "cohort IS NOT NULL" if cohort is None else "cohort = ?"
    params: tuple = () if cohort is None else (cohort,)

    total = conn.execute(
        f"SELECT COUNT(*) FROM paper_trades WHERE {where}", params
    ).fetchone()[0]

    evaluated = conn.execute(
        f"SELECT COUNT(*) FROM paper_trades "
        f"WHERE {where} AND evaluated_at IS NOT NULL",
        params,
    ).fetchone()[0]

    eval_rows = conn.execute(
        f"""SELECT projected_return, realized_return_net, hit
              FROM paper_trades
             WHERE {where} AND evaluated_at IS NOT NULL""",
        params,
    ).fetchall()

    nets = [r[1] for r in eval_rows if r[1] is not None]
    preds_eval = [r[0] for r in eval_rows if r[0] is not None]
    hits = [r[2] for r in eval_rows if r[2] is not None]

    pending_rows = conn.execute(
        f"""SELECT as_of, horizon_days, COUNT(*)
              FROM paper_trades
             WHERE {where} AND evaluated_at IS NULL
             GROUP BY as_of, horizon_days
             ORDER BY as_of ASC""",
        params,
    ).fetchall()

    maturing: List[Dict[str, Any]] = []
    for as_of, horizon, n in pending_rows:
        try:
            maturity = (dt.date.fromisoformat(as_of)
                        + dt.timedelta(days=horizon)).isoformat()
        except Exception:
            maturity = None
        maturing.append({
            "as_of": as_of,
            "horizon_days": horizon,
            "count": n,
            "maturity_date": maturity,
        })

    mean_pred_all = conn.execute(
        f"SELECT AVG(projected_return) FROM paper_trades WHERE {where}",
        params,
    ).fetchone()[0]

    return {
        "locked": total,
        "evaluated": evaluated,
        "pending": total - evaluated,
        "mean_predicted_return_all": mean_pred_all,
        "mean_predicted_return_evaluated": _mean(preds_eval),
        "mean_realized_net": _mean(nets),
        "hit_rate": _mean([float(h) for h in hits]) if hits else None,
        "sharpe_net": _sharpe(nets),
        "maturing": maturing,
    }


def _print_table(per_cohort: Dict[str, Dict[str, Any]]) -> None:
    header = (f"{'cohort':<17} {'locked':>7} {'eval':>6} {'pred_mean':>10} "
              f"{'net_mean':>10} {'hit_rate':>9} {'sharpe':>8}")
    print(header)
    print("-" * len(header))
    for cohort, stats in per_cohort.items():
        print(
            f"{cohort:<17} "
            f"{stats['locked']:>7} "
            f"{stats['evaluated']:>6} "
            f"{_fmt_pct(stats['mean_predicted_return_all']):>10} "
            f"{_fmt_pct(stats['mean_realized_net']):>10} "
            f"{_fmt_pct(stats['hit_rate']):>9} "
            f"{_fmt_num(stats['sharpe_net'], width=8, prec=3):>8}"
        )


def _print_maturing(label: str, maturing: List[Dict[str, Any]]) -> None:
    if not maturing:
        return
    total = sum(m["count"] for m in maturing)
    print(f"\n  {label}: {total} trade(s) pending evaluation")
    # Condense to per-maturity-date counts.
    by_date: Dict[str, int] = {}
    for m in maturing:
        k = m["maturity_date"] or "unknown"
        by_date[k] = by_date.get(k, 0) + m["count"]
    for d, n in sorted(by_date.items()):
        print(f"    - {n} trades maturing on {d}")


def run(cohort_filter: Optional[str] = None,
        db_path: Path = DB_PATH) -> Dict[str, Any]:
    if not db_path.exists():
        raise SystemExit(f"DB not found at {db_path}")

    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        # Check cohort column
        cols = {r[1] for r in conn.execute("PRAGMA table_info(paper_trades)").fetchall()}
        if "cohort" not in cols:
            raise SystemExit("paper_trades.cohort column missing. "
                             "Run scripts/migrate_paper_trade_cohort.py first.")

        per_cohort: Dict[str, Dict[str, Any]] = {}
        targets = [cohort_filter] if cohort_filter else COHORTS
        for c in targets:
            per_cohort[c] = _cohort_stats(conn, c)

        overall = _cohort_stats(conn, None)
    finally:
        conn.close()

    # ---- Terminal output ----
    print("=" * 78)
    print(f"Paper-trade status  (as of {dt.date.today().isoformat()})")
    if cohort_filter:
        print(f"Filter: cohort = {cohort_filter}")
    print("=" * 78)
    _print_table(per_cohort)
    print(
        f"\n{'OVERALL':<17} "
        f"{overall['locked']:>7} "
        f"{overall['evaluated']:>6} "
        f"{_fmt_pct(overall['mean_predicted_return_all']):>10} "
        f"{_fmt_pct(overall['mean_realized_net']):>10} "
        f"{_fmt_pct(overall['hit_rate']):>9} "
        f"{_fmt_num(overall['sharpe_net'], width=8, prec=3):>8}"
    )

    for cohort, stats in per_cohort.items():
        _print_maturing(cohort, stats["maturing"])

    # ---- JSON snapshot ----
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    log_dir = Path(__file__).resolve().parent.parent / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    out_path = log_dir / f"paper_trade_status_{ts}.json"
    payload = {
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds"),
        "filter_cohort": cohort_filter,
        "overall": overall,
        "per_cohort": per_cohort,
    }
    out_path.write_text(json.dumps(payload, indent=2, default=str))
    print(f"\nSnapshot written to {out_path}")
    return payload


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="paper_trade_status")
    p.add_argument("--cohort", choices=COHORTS, default=None,
                   help="Filter to a single cohort")
    args = p.parse_args(argv)
    run(cohort_filter=args.cohort)
    return 0


if __name__ == "__main__":
    sys.exit(main())
