"""Model projection endpoints for PokeDelta."""

import json
from fastapi import APIRouter, Depends
from api.deps import get_db_conn

router = APIRouter()


@router.get("/model/projections")
def model_projections(db=Depends(get_db_conn)):
    """All latest model projections keyed by card_id."""
    rows = db.execute("""
        SELECT mp.card_id, mp.projected_return, mp.confidence_low,
               mp.confidence_high, mp.confidence_width,
               mp.feature_contributions, mp.model_version, mp.as_of
        FROM model_projections mp
        INNER JOIN (
            SELECT card_id, MAX(as_of) AS max_date
            FROM model_projections
            WHERE horizon_days = 90
            GROUP BY card_id
        ) latest ON mp.card_id = latest.card_id AND mp.as_of = latest.max_date
        WHERE mp.horizon_days = 90
    """).fetchall()

    projections = {}
    for r in rows:
        contribs = r["feature_contributions"]
        projections[r["card_id"]] = {
            "projected-return": r["projected_return"],
            "confidence-low": r["confidence_low"],
            "confidence-high": r["confidence_high"],
            "confidence-width": r["confidence_width"],
            "feature-contributions": json.loads(contribs) if contribs else {},
            "model-version": r["model_version"],
            "as-of": r["as_of"],
        }

    return {"projections": projections, "count": len(projections)}


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
