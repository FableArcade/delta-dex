"""
Market pressure and supply saturation metrics from eBay listing data.

Computes demand/supply pressure, net flow, state labels, and supply
saturation index for 7-day and 30-day windows.  Includes estimated-mode
metrics using a sold_rate_est calibrated from Collectrics data.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Fraction of ended listings that actually sold (calibrated from Collectrics)
SOLD_RATE_EST = 0.76

# State label thresholds (fraction of avg_active)
NET_FLOW_THRESHOLD = 0.01

# Supply saturation thresholds
SAT_TIGHT = 0.9
SAT_SATURATED = 1.1


def _window_metrics(db, card_id, window_days, as_of):
    """Compute demand/supply metrics for a single time window.

    Returns
    -------
    dict | None  (None if insufficient data)
    """
    start_date = (datetime.strptime(as_of, "%Y-%m-%d")
                  - timedelta(days=window_days)).strftime("%Y-%m-%d")

    rows = db.execute(
        """SELECT date, active_to, ended, new, interpolated
             FROM ebay_history
            WHERE card_id = ?
              AND date > ? AND date <= ?
            ORDER BY date""",
        (card_id, start_date, as_of),
    ).fetchall()

    if not rows:
        return None

    sample_days = len(rows)
    interpolated_days = sum(1 for r in rows if r["interpolated"])

    # Averages
    active_vals = [r["active_to"] for r in rows if r["active_to"] is not None]
    ended_vals = [r["ended"] for r in rows if r["ended"] is not None]
    new_vals = [r["new"] for r in rows if r["new"] is not None]

    avg_active = sum(active_vals) / len(active_vals) if active_vals else 0
    avg_ended = sum(ended_vals) / len(ended_vals) if ended_vals else 0
    avg_new = sum(new_vals) / len(new_vals) if new_vals else 0

    # existing = active at start of window (use active_from of earliest row)
    first_active = rows[0]["active_to"]  # approximate
    avg_existing = first_active if first_active is not None else avg_active

    # Pressure metrics
    demand_pressure = avg_ended / avg_active if avg_active > 0 else None
    supply_pressure = avg_new / avg_active if avg_active > 0 else None
    net_flow = avg_new - avg_ended
    net_flow_pct = net_flow / avg_active if avg_active > 0 else None

    # State label
    if avg_active > 0:
        if net_flow > NET_FLOW_THRESHOLD * avg_active:
            state_label = "accumulating"
        elif net_flow < -NET_FLOW_THRESHOLD * avg_active:
            state_label = "draining"
        else:
            state_label = "balanced"
    else:
        state_label = "unknown"

    # --- Observed mode ---
    observed = {
        "card_id": card_id,
        "window_days": window_days,
        "mode": "observed",
        "as_of": as_of,
        "sample_days": sample_days,
        "interpolated_days": interpolated_days,
        "avg_active": round(avg_active, 4),
        "avg_existing": round(avg_existing, 4),
        "avg_ended": round(avg_ended, 4),
        "avg_new": round(avg_new, 4),
        "demand_pressure": round(demand_pressure, 6) if demand_pressure is not None else None,
        "supply_pressure": round(supply_pressure, 6) if supply_pressure is not None else None,
        "net_flow": round(net_flow, 4),
        "net_flow_pct": round(net_flow_pct, 6) if net_flow_pct is not None else None,
        "state_label": state_label,
    }

    # --- Estimated mode (using sold_rate_est) ---
    sold_est = avg_ended * SOLD_RATE_EST
    demand_pressure_est = sold_est / avg_active if avg_active > 0 else None

    estimated = dict(observed)
    estimated["mode"] = "estimated"
    estimated["demand_pressure"] = (round(demand_pressure_est, 6)
                                     if demand_pressure_est is not None else None)

    return observed, estimated, avg_active


def _insert_pressure(db, m):
    """Insert a single market_pressure row."""
    db.execute(
        """INSERT OR REPLACE INTO market_pressure
           (card_id, window_days, mode, as_of, sample_days,
            interpolated_days, avg_active, avg_existing, avg_ended,
            avg_new, demand_pressure, supply_pressure, net_flow,
            net_flow_pct, state_label)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (m["card_id"], m["window_days"], m["mode"], m["as_of"],
         m["sample_days"], m["interpolated_days"], m["avg_active"],
         m["avg_existing"], m["avg_ended"], m["avg_new"],
         m["demand_pressure"], m["supply_pressure"], m["net_flow"],
         m["net_flow_pct"], m["state_label"]),
    )


def compute_market_pressure(db, card_id):
    """Calculate supply/demand metrics for 7d and 30d windows.

    Parameters
    ----------
    db : sqlite3.Connection
    card_id : str

    Returns
    -------
    dict  {"windows_computed": int, "saturation_written": bool}
    """
    # Determine as_of from latest ebay_history date
    latest = db.execute(
        "SELECT MAX(date) AS d FROM ebay_history WHERE card_id = ?",
        (card_id,),
    ).fetchone()

    if not latest or not latest["d"]:
        return {"windows_computed": 0, "saturation_written": False}

    as_of = latest["d"]
    windows_computed = 0

    result_7d = _window_metrics(db, card_id, 7, as_of)
    result_30d = _window_metrics(db, card_id, 30, as_of)

    if result_7d:
        obs_7, est_7, avg_active_7 = result_7d
        _insert_pressure(db, obs_7)
        _insert_pressure(db, est_7)
        windows_computed += 1

    if result_30d:
        obs_30, est_30, avg_active_30 = result_30d
        _insert_pressure(db, obs_30)
        _insert_pressure(db, est_30)
        windows_computed += 1

    # --- "Supply saturation" index (30d baseline vs 7d) ---
    # Note on semantics: despite the name, this field is a LISTINGS MOMENTUM
    # RATIO — it tells you whether listings are trending up or down relative
    # to the 30-day baseline, NOT absolute market saturation.
    #
    #   ssi = avg_active_7d / avg_active_30d
    #
    # A card with 5 consistent listings and a card with 500 consistent
    # listings both score ssi ≈ 1.0 — the "saturation" framing is a legacy
    # label. Real absolute saturation would need a reference like graded
    # population or sales velocity.
    #
    # Downstream consumers (Must Buy Now, Top Chase, Demand Surge, Long-Term
    # Holds) treat this as a MOMENTUM signal (is listing volume contracting
    # this week vs last month), which is what it actually measures, and use
    # genuinely independent signals for absolute scarcity (PSA pop, price-
    # range stability). See frontend/js/card_leaderboard.js computeMustBuyScore.
    saturation_written = False
    if result_7d and result_30d:
        baseline_avg_active = avg_active_30
        current_avg_active = avg_active_7

        if baseline_avg_active > 0:
            ssi = current_avg_active / baseline_avg_active
        else:
            ssi = None

        # Label
        if ssi is not None:
            if ssi < SAT_TIGHT:
                sat_label = "tight"
            elif ssi > SAT_SATURATED:
                sat_label = "saturated"
            else:
                sat_label = "normal"
        else:
            sat_label = "unknown"

        # Trend: compare active listings, demand, supply between windows
        active_delta_pct = (
            (avg_active_7 - avg_active_30) / avg_active_30
            if avg_active_30 > 0 else None
        )
        demand_delta_pct = (
            (obs_7["avg_ended"] - obs_30["avg_ended"]) / obs_30["avg_ended"]
            if obs_30["avg_ended"] > 0 else None
        )
        supply_delta_pct = (
            (obs_7["avg_new"] - obs_30["avg_new"]) / obs_30["avg_new"]
            if obs_30["avg_new"] > 0 else None
        )

        # Trend label
        if active_delta_pct is not None and demand_delta_pct is not None:
            if demand_delta_pct > 0.05 and active_delta_pct < 0:
                trend = "tightening"
            elif demand_delta_pct < -0.05 and active_delta_pct > 0:
                trend = "loosening"
            elif active_delta_pct > 0.05:
                trend = "building"
            elif active_delta_pct < -0.05:
                trend = "contracting"
            else:
                trend = "stable"
        else:
            trend = "unknown"

        # Supply-saturation rows are byte-identical across observed/estimated
        # modes (only demand_pressure differs between modes, and that lives on
        # market_pressure, not here). We still write both to keep primary-key
        # compatibility with downstream joins that filter by mode, but it's
        # the same data — not a second independent estimate.
        for mode in ("observed", "estimated"):
            db.execute(
                """INSERT OR REPLACE INTO supply_saturation
                   (card_id, mode, as_of,
                    supply_saturation_index, supply_saturation_label,
                    trend, active_listings_delta_pct,
                    demand_delta_pct, supply_delta_pct)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (card_id, mode, as_of,
                 round(ssi, 6) if ssi is not None else None,
                 sat_label, trend,
                 round(active_delta_pct, 6) if active_delta_pct is not None else None,
                 round(demand_delta_pct, 6) if demand_delta_pct is not None else None,
                 round(supply_delta_pct, 6) if supply_delta_pct is not None else None),
            )
        saturation_written = True

    logger.info(
        "market_pressure card=%s as_of=%s  windows=%d  saturation=%s",
        card_id, as_of, windows_computed, saturation_written,
    )

    return {
        "windows_computed": windows_computed,
        "saturation_written": saturation_written,
    }
