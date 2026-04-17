"""Paper-trading engine.

Daily pipeline step 1: `lock_trades(db, as_of)` copies today's projections
into `paper_trades` with the current PSA 10 entry price. Idempotent — re-
running the same day overwrites the same (card, as_of) row.

Daily pipeline step 2: `evaluate_due(db, today)` finds all open trades
whose as_of + 90d <= today and writes realized gross + net return.

Together these give us a ground-truth track record that, over ~6 months,
lets the promotion gate move from "trust the backtest" to "trust the live
record." The gate can then be reconfigured to prefer live-record metrics
over walk-forward metrics.
"""

from __future__ import annotations

import datetime as dt
import logging
import sqlite3
from typing import Any, Dict, Optional

from pipeline.model.friction import net_realized_return

logger = logging.getLogger("pipeline.model.paper_trade")

HORIZON_DAYS = 180


def _cohort_for_rank(rank: int, n: int) -> str:
    """Return cohort label for a 0-indexed ascending rank in a batch of n.

    Higher rank = higher projected_return.
        top_decile       P90+   (top 10%)
        top_quartile     P75-P90 (next 15%)
        middle           P25-P75
        bottom_quartile  <P25
    """
    if n <= 0:
        return "middle"
    pct = rank / n
    if pct >= 0.90:
        return "top_decile"
    if pct >= 0.75:
        return "top_quartile"
    if pct >= 0.25:
        return "middle"
    return "bottom_quartile"


def _latest_psa10_price(db: sqlite3.Connection, card_id: str,
                        on_or_before: str) -> Optional[float]:
    """Most recent PSA 10 price on or before `on_or_before` (ISO date)."""
    row = db.execute(
        """SELECT psa_10_price FROM price_history
            WHERE card_id = ? AND date <= ?
              AND psa_10_price IS NOT NULL
            ORDER BY date DESC LIMIT 1""",
        (card_id, on_or_before),
    ).fetchone()
    if not row:
        return None
    v = row[0] if not hasattr(row, "keys") else row["psa_10_price"]
    return float(v) if v is not None else None


def _first_psa10_price_on_or_after(db: sqlite3.Connection, card_id: str,
                                   on_or_after: str,
                                   window_days: int = 30) -> tuple[Optional[float], Optional[str]]:
    """First available PSA 10 price on or after the forward date, within
    a grace window. Returns (price, date_iso) or (None, None)."""
    end = (dt.date.fromisoformat(on_or_after) + dt.timedelta(days=window_days)).isoformat()
    row = db.execute(
        """SELECT date, psa_10_price FROM price_history
            WHERE card_id = ? AND date >= ? AND date <= ?
              AND psa_10_price IS NOT NULL
            ORDER BY date ASC LIMIT 1""",
        (card_id, on_or_after, end),
    ).fetchone()
    if not row:
        return None, None
    if hasattr(row, "keys"):
        return float(row["psa_10_price"]), row["date"]
    return float(row[1]), row[0]


def lock_trades(db: sqlite3.Connection, as_of: Optional[str] = None) -> Dict[str, Any]:
    """Copy today's model_projections into paper_trades with entry prices.
    Idempotent on (card_id, as_of, horizon_days)."""
    as_of = as_of or dt.date.today().isoformat()

    rows = db.execute(
        """SELECT card_id, projected_return, confidence_low, confidence_high,
                  model_version
             FROM model_projections
            WHERE as_of = ? AND horizon_days = ?""",
        (as_of, HORIZON_DAYS),
    ).fetchall()

    if not rows:
        logger.info("paper_trade.lock_trades: no projections for %s", as_of)
        return {"locked": 0, "as_of": as_of}

    locked = 0
    skipped = 0
    for r in rows:
        card_id = r["card_id"] if hasattr(r, "keys") else r[0]
        entry = _latest_psa10_price(db, card_id, as_of)
        if entry is None or entry <= 0:
            skipped += 1
            continue
        db.execute(
            """INSERT OR REPLACE INTO paper_trades
               (card_id, as_of, horizon_days, model_version,
                entry_price, projected_return,
                confidence_low, confidence_high)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                card_id, as_of, HORIZON_DAYS,
                r["model_version"] if hasattr(r, "keys") else r[4],
                entry,
                r["projected_return"] if hasattr(r, "keys") else r[1],
                r["confidence_low"] if hasattr(r, "keys") else r[2],
                r["confidence_high"] if hasattr(r, "keys") else r[3],
            ),
        )
        locked += 1
    db.commit()

    # Tag cohorts within this batch based on projected_return percentile.
    # Only tag if the `cohort` column exists (migration applied).
    cohort_counts = _assign_cohorts(db, as_of)

    logger.info("paper_trade.lock_trades: locked=%d skipped=%d as_of=%s cohorts=%s",
                locked, skipped, as_of, cohort_counts)
    return {"locked": locked, "skipped_no_price": skipped, "as_of": as_of,
            "cohorts": cohort_counts}


def _assign_cohorts(db: sqlite3.Connection, as_of: str) -> Dict[str, int]:
    """Assign cohort labels to all paper_trades for a given as_of date.
    No-op (returns empty dict) if the cohort column does not exist yet."""
    cols = {r[1] for r in db.execute("PRAGMA table_info(paper_trades)").fetchall()}
    if "cohort" not in cols:
        return {}

    rows = db.execute(
        """SELECT card_id, horizon_days, projected_return
             FROM paper_trades
            WHERE as_of = ?""",
        (as_of,),
    ).fetchall()
    # Sort ascending by projected_return; None treated as lowest.
    rows = sorted(rows,
                  key=lambda r: (r[2] is None, r[2] if r[2] is not None else 0.0))
    n = len(rows)
    counts: Dict[str, int] = {"top_decile": 0, "top_quartile": 0,
                              "middle": 0, "bottom_quartile": 0}
    for idx, r in enumerate(rows):
        cohort = _cohort_for_rank(idx, n)
        db.execute(
            """UPDATE paper_trades SET cohort = ?
                WHERE card_id = ? AND as_of = ? AND horizon_days = ?""",
            (cohort, r[0], as_of, r[1]),
        )
        counts[cohort] += 1
    db.commit()
    return counts


def evaluate_due(db: sqlite3.Connection, today: Optional[str] = None) -> Dict[str, Any]:
    """Score every unevaluated trade whose T+90 has come due."""
    today = today or dt.date.today().isoformat()
    cutoff = (dt.date.fromisoformat(today) - dt.timedelta(days=HORIZON_DAYS)).isoformat()

    rows = db.execute(
        """SELECT card_id, as_of, horizon_days, entry_price
             FROM paper_trades
            WHERE evaluated_at IS NULL
              AND as_of <= ?""",
        (cutoff,),
    ).fetchall()

    evaluated = 0
    unresolved = 0
    for r in rows:
        card_id = r["card_id"] if hasattr(r, "keys") else r[0]
        as_of = r["as_of"] if hasattr(r, "keys") else r[1]
        horizon = r["horizon_days"] if hasattr(r, "keys") else r[2]
        entry = r["entry_price"] if hasattr(r, "keys") else r[3]
        forward = (dt.date.fromisoformat(as_of) + dt.timedelta(days=horizon)).isoformat()
        exit_price, exit_date = _first_psa10_price_on_or_after(db, card_id, forward)
        if exit_price is None:
            unresolved += 1
            continue
        gross = (exit_price - entry) / entry if entry > 0 else None
        net = net_realized_return(entry, exit_price)
        hit = 1 if (net is not None and net > 0) else 0
        db.execute(
            """UPDATE paper_trades
                  SET exit_date = ?, exit_price = ?,
                      realized_return_gross = ?,
                      realized_return_net = ?,
                      hit = ?,
                      evaluated_at = ?
                WHERE card_id = ? AND as_of = ? AND horizon_days = ?""",
            (exit_date, exit_price, gross, net, hit,
             dt.datetime.utcnow().isoformat(timespec="seconds"),
             card_id, as_of, horizon),
        )
        evaluated += 1
    db.commit()
    logger.info("paper_trade.evaluate_due: evaluated=%d unresolved=%d today=%s",
                evaluated, unresolved, today)
    return {"evaluated": evaluated, "unresolved_no_exit_price": unresolved,
            "today": today}


def run_daily(db: sqlite3.Connection) -> Dict[str, Any]:
    """Convenience: lock today's trades, then evaluate anything due."""
    locked = lock_trades(db)
    scored = evaluate_due(db)
    return {"lock": locked, "evaluate": scored}
