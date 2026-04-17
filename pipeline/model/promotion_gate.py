"""Promotion gate: decides whether a trained model is allowed to write to
`model_projections` (and therefore reach the UI).

v3 — Long-only top-2% conviction strategy gate. The actual trading
strategy picks the top 2% of the daily projection batch (~100-200 cards),
not the full top decile. The gate is aligned to that cohort. Thresholds:

  top2_hit_rate   > 0.70   (conviction bar: 7 out of 10 picks profitable)
  top2_net_return >= 0.10  (crosses friction with meaningful margin)
  top2_sharpe     > 1.5    (strong risk-adjusted)
  spearman_oos    > 0.10   (ranking has real signal, not luck)
  n_top2          >= 30    (statistical power floor)

All five must pass for promotion. Top-decile metrics are retained as a
secondary `decile_gate` for audit continuity.

The gate reads walk-forward metrics (from scripts/walkforward_backtest.py
JSON output or an in-memory dict), writes an audit row to
`model_promotion_log`, and flips model_report_card.promotion_status.

Callers:
  - scripts/walkforward_backtest.py: runs evaluate_and_record() after bt
  - pipeline/model/predict.py:       calls is_promoted() before writing
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("pipeline.model.promotion_gate")

GATE_VERSION = "v3.0"

# --- Primary (top-2% conviction) gate ---
MIN_TOP2_HIT_RATE = 0.70
MIN_TOP2_NET = 0.10
MIN_TOP2_SHARPE = 1.5
MIN_SPEARMAN = 0.10
MIN_N_TOP2 = 30

# --- Secondary (top-decile) gate, retained for audit continuity ---
MIN_TOP_DECILE_HIT_RATE = 0.50
MIN_TOP_DECILE_NET = 0.02
MIN_TOP_DECILE_SHARPE = 0.5


@dataclass
class SubGate:
    name: str
    passed: bool
    reason: str
    checks: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class GateDecision:
    model_version: str
    decision: str  # 'promoted' | 'rejected'
    reason: str
    # Primary (top-2%) metrics
    top2_hit_rate: Optional[float]
    top2_net: Optional[float]
    top2_sharpe: Optional[float]
    spearman_oos: Optional[float]
    n_top2: Optional[int]
    # Secondary (top-decile) metrics, audit only
    top_decile_hit_rate: Optional[float]
    top_decile_net: Optional[float]
    top_decile_sharpe: Optional[float]
    n: Optional[int]
    # Sub-gate results
    top2_gate: Dict[str, Any] = field(default_factory=dict)
    decile_gate: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _check(cond: bool, label: str, reasons: List[str]) -> None:
    if not cond:
        reasons.append(label)


def _evaluate_top2(metrics: Dict[str, Any]) -> SubGate:
    t2h = metrics.get("top2_hit_rate")
    t2n = metrics.get("top2_net_return")
    t2s = metrics.get("top2_sharpe")
    sp = metrics.get("spearman_oos")
    n_t2 = metrics.get("n_top2")

    reasons: List[str] = []
    _check(t2h is not None and t2h > MIN_TOP2_HIT_RATE,
           f"top2_hit_rate={t2h} <= {MIN_TOP2_HIT_RATE}", reasons)
    _check(t2n is not None and t2n >= MIN_TOP2_NET,
           f"top2_net={t2n} < {MIN_TOP2_NET}", reasons)
    _check(t2s is not None and t2s > MIN_TOP2_SHARPE,
           f"top2_sharpe={t2s} <= {MIN_TOP2_SHARPE}", reasons)
    _check(sp is not None and sp > MIN_SPEARMAN,
           f"spearman_oos={sp} <= {MIN_SPEARMAN}", reasons)
    _check(n_t2 is not None and n_t2 >= MIN_N_TOP2,
           f"n_top2={n_t2} < {MIN_N_TOP2}", reasons)

    passed = not reasons
    reason = (
        f"PASS t2h={t2h} t2n={t2n} t2s={t2s} spearman={sp} n_t2={n_t2}"
        if passed else "; ".join(reasons)
    )
    return SubGate(
        name="top2",
        passed=passed,
        reason=reason,
        checks={
            "top2_hit_rate": t2h,
            "top2_net_return": t2n,
            "top2_sharpe": t2s,
            "spearman_oos": sp,
            "n_top2": n_t2,
        },
    )


def _evaluate_decile(metrics: Dict[str, Any]) -> SubGate:
    """Secondary gate on top-decile for audit continuity. Informational only."""
    tdh = metrics.get("top_decile_hit_rate")
    tdn = metrics.get("top_decile_net_return")
    tds = metrics.get("top_decile_sharpe")
    if tds is None:
        tds = metrics.get("sharpe")

    reasons: List[str] = []
    _check(tdh is not None and tdh > MIN_TOP_DECILE_HIT_RATE,
           f"top_decile_hit_rate={tdh} <= {MIN_TOP_DECILE_HIT_RATE}", reasons)
    _check(tdn is not None and tdn >= MIN_TOP_DECILE_NET,
           f"top_decile_net={tdn} < {MIN_TOP_DECILE_NET}", reasons)
    _check(tds is not None and tds > MIN_TOP_DECILE_SHARPE,
           f"top_decile_sharpe={tds} <= {MIN_TOP_DECILE_SHARPE}", reasons)

    passed = not reasons
    reason = (
        f"PASS tdh={tdh} tdn={tdn} tds={tds}"
        if passed else "; ".join(reasons)
    )
    return SubGate(
        name="decile",
        passed=passed,
        reason=reason,
        checks={
            "top_decile_hit_rate": tdh,
            "top_decile_net_return": tdn,
            "top_decile_sharpe": tds,
        },
    )


def evaluate(metrics: Dict[str, Any], model_version: str) -> GateDecision:
    """Pure decision function. Runs the primary (top-decile) gate and
    secondary (decile) gate. Promotion requires only the top-2% gate
    to pass; the decile result is recorded for audit."""
    top2 = _evaluate_top2(metrics)
    decile = _evaluate_decile(metrics)

    decision = "promoted" if top2.passed else "rejected"
    if top2.passed:
        reason = (
            f"TOP2_GATE:PASS [{top2.reason}] | "
            f"DECILE_GATE:{'PASS' if decile.passed else 'FAIL'} "
            f"[{decile.reason}]"
        )
    else:
        reason = f"TOP2_GATE:FAIL [{top2.reason}]"

    return GateDecision(
        model_version=model_version,
        decision=decision,
        reason=reason,
        top2_hit_rate=top2.checks.get("top2_hit_rate"),
        top2_net=top2.checks.get("top2_net_return"),
        top2_sharpe=top2.checks.get("top2_sharpe"),
        spearman_oos=top2.checks.get("spearman_oos"),
        n_top2=top2.checks.get("n_top2"),
        top_decile_hit_rate=decile.checks.get("top_decile_hit_rate"),
        top_decile_net=decile.checks.get("top_decile_net_return"),
        top_decile_sharpe=decile.checks.get("top_decile_sharpe"),
        n=metrics.get("n_predictions"),
        top2_gate=top2.as_dict(),
        decile_gate=decile.as_dict(),
    )


def record_decision(
    db: sqlite3.Connection,
    decision: GateDecision,
    metrics: Dict[str, Any],
) -> None:
    """Write an audit row to model_promotion_log and update the
    model_report_card for this version. Full sub-gate results are
    embedded in metrics_json."""
    now = dt.datetime.utcnow().isoformat(timespec="seconds")
    enriched_metrics = dict(metrics)
    enriched_metrics["top2_gate"] = decision.top2_gate
    enriched_metrics["decile_gate"] = decision.decile_gate
    enriched_metrics["gate_version"] = GATE_VERSION

    # Log schema keeps legacy column names (walkforward_*); we write the
    # primary (top-2%) metrics into them so downstream log readers still
    # reflect the strategy actually gating promotion.
    db.execute(
        """INSERT INTO model_promotion_log
           (model_version, evaluated_at, decision,
            walkforward_sharpe, walkforward_hit_rate,
            walkforward_top_decile_net, walkforward_n,
            reason, gate_version, metrics_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            decision.model_version, now, decision.decision,
            decision.top2_sharpe, decision.top2_hit_rate,
            decision.top2_net, decision.n_top2,
            decision.reason, GATE_VERSION,
            json.dumps(enriched_metrics, default=str),
        ),
    )
    # Best-effort: flip promotion_status on the latest report card row(s)
    db.execute(
        """UPDATE model_report_card
              SET promotion_status = ?, promotion_reason = ?
            WHERE model_version = ?""",
        (decision.decision, decision.reason, decision.model_version),
    )
    db.commit()


def evaluate_and_record(
    db: sqlite3.Connection,
    metrics: Dict[str, Any],
    model_version: str,
) -> GateDecision:
    decision = evaluate(metrics, model_version)
    record_decision(db, decision, metrics)
    logger.info(
        "Gate decision for %s: %s (%s)",
        model_version, decision.decision, decision.reason,
    )
    return decision


def is_promoted(db: sqlite3.Connection, model_version: str) -> bool:
    """Check whether a given model_version is currently promoted.

    If there is no report card row, default to NOT promoted — safer to
    suppress projections than to leak an unchecked model to the UI.
    """
    row = db.execute(
        """SELECT promotion_status
             FROM model_report_card
            WHERE model_version = ?
            ORDER BY as_of DESC
            LIMIT 1""",
        (model_version,),
    ).fetchone()
    if row is None:
        return False
    status = row[0] if not hasattr(row, "keys") else row["promotion_status"]
    return status == "promoted"
