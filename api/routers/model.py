"""Model projection endpoints for PokeDelta."""

import json
from fastapi import APIRouter, Depends
from api.deps import get_db_conn

router = APIRouter()


@router.get("/model/projections")
def model_projections(db=Depends(get_db_conn)):
    """All latest model projections keyed by card_id.

    Response includes `data_as_of` — the MAX(as_of) across returned rows —
    so the frontend can display a staleness indicator to users.
    """
    rows = db.execute("""
        SELECT mp.card_id, mp.projected_return, mp.confidence_low,
               mp.confidence_high, mp.confidence_width,
               mp.feature_contributions, mp.model_version, mp.as_of
        FROM model_projections mp
        INNER JOIN (
            SELECT card_id, MAX(as_of) AS max_date
            FROM model_projections
            WHERE horizon_days = 180
            GROUP BY card_id
        ) latest ON mp.card_id = latest.card_id AND mp.as_of = latest.max_date
        WHERE mp.horizon_days = 180
    """).fetchall()

    projections = {}
    max_as_of = None
    for r in rows:
        contribs = r["feature_contributions"]
        as_of = r["as_of"]
        if as_of and (max_as_of is None or as_of > max_as_of):
            max_as_of = as_of
        projections[r["card_id"]] = {
            "projected-return": r["projected_return"],
            "confidence-low": r["confidence_low"],
            "confidence-high": r["confidence_high"],
            "confidence-width": r["confidence_width"],
            "feature-contributions": json.loads(contribs) if contribs else {},
            "model-version": r["model_version"],
            "as-of": as_of,
        }

    # Also surface last pipeline run timestamp so users see data freshness
    # even when projections are sparse.
    last_run_at = None
    try:
        run_row = db.execute(
            "SELECT MAX(finished_at) AS finished_at FROM pipeline_runs "
            "WHERE status IN ('done', 'done_with_errors')"
        ).fetchone()
        if run_row and run_row["finished_at"]:
            last_run_at = run_row["finished_at"]
    except Exception:
        last_run_at = None

    return {
        "projections": projections,
        "count": len(projections),
        "data_as_of": max_as_of,
        "last_pipeline_run_at": last_run_at,
    }


@router.get("/model/picks")
def model_picks(
    cohort: str = "top2",
    min_price: float = 100.0,
    conf_low_positive: bool = False,
    db=Depends(get_db_conn),
):
    """Ranked investment pick list from today's promoted model.

    cohort:
      - top2    top 2% of projections by projected_return (primary strategy,
                82% hit rate / +17% net 180d / Sharpe 4.17 in walk-forward)
      - top1    top 1% (tighter conviction, ~86% hit / +19% net)
      - top10   top 10% (full decile, +3% net)

    Filters:
      - PSA 10 current price >= min_price (default $100, keeps friction
        tractable — $5 shipping is only 5% of a $100 card)
      - conf_low_positive=True optionally adds `confidence_low > 0`
        (downside band above zero). Off by default because it aggressively
        narrows the universe.
    """
    frac = {"top1": 0.01, "top2": 0.02, "top10": 0.10}.get(cohort, 0.02)

    # Pull latest projections joined with latest PSA 10 price so we can
    # price-filter on the server side instead of per-card round trips.
    rows = db.execute(
        """
        WITH latest_proj AS (
          SELECT card_id, MAX(as_of) AS max_date
          FROM model_projections WHERE horizon_days = 180 GROUP BY card_id
        ),
        latest_price AS (
          SELECT card_id, MAX(date) AS max_date
          FROM price_history WHERE psa_10_price IS NOT NULL GROUP BY card_id
        )
        SELECT c.id, c.product_name, c.set_code, c.card_number, c.image_url,
               c.rarity_code, c.rarity_name,
               mp.projected_return, mp.confidence_low, mp.confidence_high,
               mp.confidence_width, mp.feature_contributions,
               mp.model_version, mp.as_of,
               ph.psa_10_price AS psa10_current,
               ph.raw_price AS raw_current
        FROM model_projections mp
        INNER JOIN latest_proj lp ON lp.card_id = mp.card_id AND lp.max_date = mp.as_of
        INNER JOIN cards c ON c.id = mp.card_id
        LEFT JOIN latest_price lpp ON lpp.card_id = mp.card_id
        LEFT JOIN price_history ph ON ph.card_id = mp.card_id AND ph.date = lpp.max_date
        WHERE mp.horizon_days = 180
          AND ph.psa_10_price >= ?
          {conf_clause}
        ORDER BY mp.projected_return DESC
        """.replace("{conf_clause}",
                    "AND mp.confidence_low > 0" if conf_low_positive else ""),
        (min_price,),
    ).fetchall()

    total = len(rows)
    n_picks = max(1, int(round(frac * total))) if total else 0
    picks = rows[:n_picks]

    return {
        "cohort": cohort,
        "min_price": min_price,
        "universe_size": total,
        "pick_count": n_picks,
        "picks": [
            {
                "card-id": r["id"],
                "product-name": r["product_name"],
                "set-code": r["set_code"],
                "card-number": r["card_number"],
                "image-url": r["image_url"],
                "rarity-code": r["rarity_code"],
                "rarity-name": r["rarity_name"],
                "projected-return": r["projected_return"],
                "confidence-low": r["confidence_low"],
                "confidence-high": r["confidence_high"],
                "confidence-width": r["confidence_width"],
                "psa10-current": r["psa10_current"],
                "raw-current": r["raw_current"],
                "feature-contributions": json.loads(r["feature_contributions"]) if r["feature_contributions"] else {},
                "model-version": r["model_version"],
                "as-of": r["as_of"],
            }
            for r in picks
        ],
    }


@router.get("/model/report-card")
def model_report_card(db=Depends(get_db_conn)):
    """Latest model report card with accuracy metrics."""
    row = db.execute("""
        SELECT * FROM model_report_card
        ORDER BY as_of DESC, model_version DESC
        LIMIT 1
    """).fetchone()

    if not row:
        return {"available": False}

    return {
        "available": True,
        "model-version": row["model_version"],
        "as-of": row["as_of"],
        "horizon-days": row["horizon_days"],
        "total-samples": row["total_samples"],
        "r-squared-oos": row["r_squared_oos"],
        "spearman-oos": row["spearman_oos"],
        "mean-return-top-decile": row["mean_return_top_decile"],
        "mean-return-bottom-decile": row["mean_return_bottom_decile"],
        "decile-spread": row["decile_spread"],
        "hit-rate-positive": row["hit_rate_positive"],
        "calibration": json.loads(row["calibration_json"]) if row["calibration_json"] else [],
        "feature-importance": json.loads(row["feature_importance_json"]) if row["feature_importance_json"] else {},
    }
