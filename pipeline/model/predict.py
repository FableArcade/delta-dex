"""Inference module for PokeDelta prediction engine.

Loads trained models and generates projections for all active cards,
storing results in the model_projections table.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import lightgbm as lgb
import numpy as np

from pipeline.model.features import FEATURE_COLUMNS, build_live_features

logger = logging.getLogger("pipeline.model.predict")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"


def _load_models(version: str) -> Dict[str, lgb.Booster]:
    """Load the three quantile models for a given version."""
    models = {}
    for name in ("median", "lower", "upper"):
        path = MODELS_DIR / f"{name}_{version}.lgb"
        if not path.exists():
            raise FileNotFoundError(f"Model file not found: {path}")
        models[name] = lgb.Booster(model_file=str(path))
        logger.info("Loaded %s model from %s", name, path)
    return models


def _get_latest_version() -> str:
    """Read the latest model version from the pointer file."""
    path = MODELS_DIR / "latest_version.txt"
    if not path.exists():
        raise FileNotFoundError(
            "No trained model found. Run training first: "
            "python -m pipeline.model.train"
        )
    return path.read_text().strip()


def generate_projections(
    db,
    version: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate 90-day return projections for all active cards.

    Loads the model, builds live feature vectors, runs inference,
    computes feature contributions, and writes to model_projections.

    Returns summary stats.
    """
    version = version or _get_latest_version()
    date_str = dt.date.today().isoformat()

    logger.info("Generating projections with model %s for %s", version, date_str)

    # Load models
    models = _load_models(version)

    # Build features
    features_df = build_live_features(db)
    if features_df.empty:
        logger.warning("No cards with valid features for projection")
        return {"cards_projected": 0}

    X = features_df[FEATURE_COLUMNS].values
    card_ids = features_df.index.tolist()

    # Run inference
    pred_median = models["median"].predict(X)
    pred_lower = models["lower"].predict(X)
    pred_upper = models["upper"].predict(X)

    # Compute feature contributions using LightGBM's built-in SHAP
    try:
        shap_values = models["median"].predict(X, pred_contrib=True)
        # shap_values has shape (n_samples, n_features + 1) where last col is bias
        has_shap = True
    except Exception as e:
        logger.warning("SHAP computation failed, skipping contributions: %s", e)
        has_shap = False

    # Write projections
    inserted = 0
    for i, card_id in enumerate(card_ids):
        projected_return = float(pred_median[i])
        conf_low = float(pred_lower[i])
        conf_high = float(pred_upper[i])
        conf_width = conf_high - conf_low

        # Feature contributions
        if has_shap:
            contribs = {}
            for j, feat_name in enumerate(FEATURE_COLUMNS):
                val = float(shap_values[i][j])
                if abs(val) > 0.001:  # only include meaningful contributions
                    contribs[feat_name] = round(val, 4)
            # Sort by absolute contribution
            contribs = dict(sorted(contribs.items(), key=lambda x: -abs(x[1])))
            contribs_json = json.dumps(contribs)
        else:
            contribs_json = "{}"

        db.execute(
            """INSERT OR REPLACE INTO model_projections
               (card_id, as_of, horizon_days, projected_return,
                confidence_low, confidence_high, confidence_width,
                feature_contributions, model_version)
               VALUES (?, ?, 90, ?, ?, ?, ?, ?, ?)""",
            (
                card_id, date_str, projected_return,
                conf_low, conf_high, conf_width,
                contribs_json, version,
            ),
        )
        inserted += 1

    logger.info("Generated projections for %d cards", inserted)

    # Summary stats
    return {
        "cards_projected": inserted,
        "model_version": version,
        "date": date_str,
        "median_projection": float(np.median(pred_median)),
        "mean_projection": float(np.mean(pred_median)),
        "positive_count": int((pred_median > 0).sum()),
        "negative_count": int((pred_median <= 0).sum()),
        "avg_confidence_width": float(np.mean(pred_upper - pred_lower)),
    }
